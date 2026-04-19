import { useEffect, useMemo, useState } from 'react'
import { ensureManagedSourcePath } from '../../apis/managedSource'
import {
  refreshIntradayMonitorRealtime,
  type IntradayMonitorRankModeConfig,
  type IntradayMonitorRow,
  type IntradayMonitorTemplate,
} from '../../apis/reader'
import DetailsLink from '../../shared/DetailsLink'
import { normalizeTsCode } from '../../shared/stockCode'
import { readJsonStorage, writeJsonStorage } from '../../shared/storage'
import './css/IntradayMonitorCustomPage.css'

const TEMPLATE_STORAGE_KEY = 'lh_intraday_monitor_realtime_templates_v1'
const CUSTOM_MONITOR_STATE_KEY = 'lh_intraday_custom_monitor_state_v1'

type PersistedCustomMonitorState = {
  codeInput: string
  selectedTemplateId: string
  rows: IntradayMonitorRow[]
  refreshedAt: string
}

function createId() {
  return typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function'
    ? crypto.randomUUID()
    : `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`
}

function createTemplate(name = '', expression = ''): IntradayMonitorTemplate {
  return {
    id: createId(),
    name,
    expression,
  }
}

function normalizeTemplate(input: unknown): IntradayMonitorTemplate | null {
  if (!input || typeof input !== 'object') return null
  const item = input as Record<string, unknown>
  if (typeof item.id !== 'string') return null
  if (typeof item.name !== 'string') return null
  if (typeof item.expression !== 'string') return null
  return {
    id: item.id,
    name: item.name,
    expression: item.expression,
  }
}

function splitCodes(raw: string) {
  return raw
    .split(/[\s,;|，；、]+/)
    .map((item) => item.trim())
    .filter((item) => item !== '')
}

function formatNumber(value?: number | null, digits = 2) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return '--'
  }
  return Number.isInteger(value) ? String(value) : value.toFixed(digits)
}

function formatPercent(value?: number | null) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return '--'
  }
  return `${value.toFixed(2)}%`
}

export default function IntradayMonitorCustomPage() {
  const persisted = useMemo(() => {
    const parsed = readJsonStorage<Partial<PersistedCustomMonitorState>>(
      typeof window === 'undefined' ? null : window.sessionStorage,
      CUSTOM_MONITOR_STATE_KEY,
    )
    if (!parsed || typeof parsed !== 'object') {
      return null
    }
    return {
      codeInput: typeof parsed.codeInput === 'string' ? parsed.codeInput : '',
      selectedTemplateId:
        typeof parsed.selectedTemplateId === 'string' ? parsed.selectedTemplateId : '',
      rows: Array.isArray(parsed.rows) ? parsed.rows : [],
      refreshedAt: typeof parsed.refreshedAt === 'string' ? parsed.refreshedAt : '',
    } satisfies PersistedCustomMonitorState
  }, [])

  const [sourcePath, setSourcePath] = useState('')
  const [codeInput, setCodeInput] = useState(() => persisted?.codeInput ?? '')
  const [rows, setRows] = useState<IntradayMonitorRow[]>(() => persisted?.rows ?? [])
  const [refreshedAt, setRefreshedAt] = useState(() => persisted?.refreshedAt ?? '')
  const [selectedTemplateId, setSelectedTemplateId] = useState(
    () => persisted?.selectedTemplateId ?? '',
  )
  const [templates, setTemplates] = useState<IntradayMonitorTemplate[]>([])
  const [loading, setLoading] = useState(false)
  const [notice, setNotice] = useState('')
  const [error, setError] = useState('')
  const [templateModalOpen, setTemplateModalOpen] = useState(false)
  const [draftTemplate, setDraftTemplate] = useState<IntradayMonitorTemplate>(
    createTemplate(),
  )

  const sourcePathTrimmed = sourcePath.trim()

  useEffect(() => {
    void ensureManagedSourcePath()
      .then((value) => {
        setSourcePath(value)
      })
      .catch(() => {})
  }, [])

  useEffect(() => {
    const parsed = readJsonStorage<unknown>(
      typeof window === 'undefined' ? null : window.localStorage,
      TEMPLATE_STORAGE_KEY,
    )
    if (!Array.isArray(parsed)) {
      setTemplates([])
      return
    }
    const normalized = parsed
      .map(normalizeTemplate)
      .filter((item): item is IntradayMonitorTemplate => item !== null)
    setTemplates(normalized)
  }, [])

  useEffect(() => {
    writeJsonStorage(
      typeof window === 'undefined' ? null : window.localStorage,
      TEMPLATE_STORAGE_KEY,
      templates,
    )
  }, [templates])

  useEffect(() => {
    writeJsonStorage(
      typeof window === 'undefined' ? null : window.sessionStorage,
      CUSTOM_MONITOR_STATE_KEY,
      {
        codeInput,
        selectedTemplateId,
        rows,
        refreshedAt,
      } satisfies PersistedCustomMonitorState,
    )
  }, [codeInput, refreshedAt, rows, selectedTemplateId])

  useEffect(() => {
    if (selectedTemplateId !== '' && !templates.some((item) => item.id === selectedTemplateId)) {
      setSelectedTemplateId('')
    }
  }, [selectedTemplateId, templates])

  function onApplyCodeList() {
    const parts = splitCodes(codeInput)
    if (parts.length === 0) {
      setRows([])
      setNotice('名单为空，已清空当前监控列表。')
      setError('')
      return
    }

    const deduped = new Set<string>()
    const invalidInputs: string[] = []
    parts.forEach((part) => {
      const code = normalizeTsCode(part)
      if (!code) {
        invalidInputs.push(part)
        return
      }
      deduped.add(code)
    })

    const nextRows = Array.from(deduped).map((tsCode) => ({
      rank_mode: 'total',
      ts_code: tsCode,
      scene_name: '自定义',
      name: tsCode,
      board: '--',
      concept: '',
      trade_date: undefined,
      direction: null,
      total_score: null,
      scene_score: null,
      risk_score: null,
      confirm_strength: null,
      risk_intensity: null,
      scene_status: null,
      rank: null,
      total_mv_yi: null,
      realtime_price: null,
      realtime_change_pct: null,
      realtime_change_open_pct: null,
      realtime_vol_ratio: null,
      template_tag_text: null,
      template_tag_tone: null,
    } satisfies IntradayMonitorRow))

    setRows(nextRows)
    setRefreshedAt('')
    setError('')
    if (invalidInputs.length > 0) {
      setNotice(`已应用 ${nextRows.length} 个代码，忽略 ${invalidInputs.length} 个无效输入。`)
    } else {
      setNotice(`已应用 ${nextRows.length} 个代码。`)
    }
  }

  async function onRefreshRealtime() {
    if (sourcePathTrimmed === '') {
      setError('请先到“数据管理”页完成数据准备。')
      return
    }
    if (rows.length === 0) {
      setError('请先输入名单并应用。')
      return
    }

    setLoading(true)
    setError('')
    setNotice('')
    try {
      const rankModeConfigs: IntradayMonitorRankModeConfig[] = [
        {
          mode: 'total',
          sceneName: '全部',
          templateId: selectedTemplateId,
        },
      ]
      const result = await refreshIntradayMonitorRealtime({
        sourcePath: sourcePathTrimmed,
        rows,
        templates,
        rankModeConfigs,
      })
      setRows(result.rows ?? [])
      setRefreshedAt(result.refreshed_at ?? '')
      setNotice(`刷新完成，共 ${result.rows?.length ?? 0} 只。`)
    } catch (runError) {
      setError(`刷新失败: ${String(runError)}`)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="intraday-custom-page">
      <section className="intraday-custom-card">
        <h2 className="intraday-custom-title">自定义监控</h2>
        <p className="intraday-custom-tip">
          名单支持分隔符：逗号、分号、竖线、空格、换行（含中文符号）。模板检查与实时刷新复用实时监控链路。
        </p>

        <div className="intraday-custom-form-grid">
          <label className="intraday-custom-field intraday-custom-field-span-full">
            <span>自定义名单</span>
            <textarea
              value={codeInput}
              onChange={(event) => setCodeInput(event.target.value)}
              placeholder="示例：000001.SZ, 600000.SH；300750.SZ\n或 000001 600000 300750"
              rows={5}
            />
          </label>

          <label className="intraday-custom-field">
            <span>模板</span>
            <select
              value={selectedTemplateId}
              onChange={(event) => setSelectedTemplateId(event.target.value)}
            >
              <option value="">未选择</option>
              {templates.map((item) => (
                <option key={item.id} value={item.id}>
                  {item.name}
                </option>
              ))}
            </select>
          </label>
        </div>

        <div className="intraday-custom-actions">
          <button type="button" onClick={() => onApplyCodeList()} disabled={loading}>
            应用名单
          </button>
          <button type="button" onClick={() => setTemplateModalOpen(true)} disabled={loading}>
            模板管理
          </button>
          <button
            type="button"
            className="intraday-custom-primary-btn"
            onClick={() => void onRefreshRealtime()}
            disabled={loading || rows.length === 0}
          >
            {loading ? '刷新中...' : '刷新实时'}
          </button>
        </div>

        {notice ? <div className="intraday-custom-notice">{notice}</div> : null}
        {error ? <div className="intraday-custom-error">{error}</div> : null}

        {refreshedAt ? (
          <div className="intraday-custom-refreshed">最近刷新：{refreshedAt}</div>
        ) : null}

        <div className="intraday-custom-table-wrap">
          <table className="intraday-custom-table">
            <thead>
              <tr>
                <th>代码</th>
                <th>名称</th>
                <th>实时价</th>
                <th>实时涨幅</th>
                <th>实时量比</th>
                <th>模板标记</th>
              </tr>
            </thead>
            <tbody>
              {rows.length === 0 ? (
                <tr>
                  <td colSpan={6} className="intraday-custom-empty-cell">
                    暂无数据
                  </td>
                </tr>
              ) : (
                rows.map((row) => (
                  <tr key={row.ts_code}>
                    <td>{row.ts_code}</td>
                    <td>
                      <DetailsLink
                        className="intraday-custom-stock-link"
                        tsCode={row.ts_code}
                        tradeDate={typeof row.trade_date === 'string' ? row.trade_date : null}
                        sourcePath={sourcePathTrimmed || undefined}
                        title={`查看 ${row.name || row.ts_code} 详情`}
                      >
                        {row.name || row.ts_code}
                      </DetailsLink>
                    </td>
                    <td>{formatNumber(row.realtime_price)}</td>
                    <td>{formatPercent(row.realtime_change_pct)}</td>
                    <td>{formatNumber(row.realtime_vol_ratio)}</td>
                    <td>
                      {row.template_tag_text && row.template_tag_text.trim() !== ''
                        ? row.template_tag_text
                        : '--'}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>

      {templateModalOpen ? (
        <div
          className="intraday-custom-modal-mask"
          onClick={() => setTemplateModalOpen(false)}
        >
          <div
            className="intraday-custom-modal"
            onClick={(event) => event.stopPropagation()}
          >
            <div className="intraday-custom-modal-head">
              <h4>模板管理</h4>
              <button type="button" onClick={() => setTemplateModalOpen(false)}>
                关闭
              </button>
            </div>

            <div className="intraday-custom-modal-form">
              <input
                value={draftTemplate.name}
                onChange={(event) =>
                  setDraftTemplate((draft) => ({
                    ...draft,
                    name: event.target.value,
                  }))
                }
                placeholder="模板名，例如：放量突破"
              />
              <textarea
                value={draftTemplate.expression}
                onChange={(event) =>
                  setDraftTemplate((draft) => ({
                    ...draft,
                    expression: event.target.value,
                  }))
                }
                placeholder="示例：C > MA(C, 5) AND REALTIME_VOL_RATIO >= 2"
                rows={4}
              />
              <div className="intraday-custom-actions">
                <button
                  type="button"
                  onClick={() => {
                    const name = draftTemplate.name.trim()
                    const expression = draftTemplate.expression.trim()
                    if (!name || !expression) {
                      return
                    }
                    setTemplates((current) => [...current, createTemplate(name, expression)])
                    setDraftTemplate(createTemplate())
                  }}
                >
                  新增模板
                </button>
              </div>
            </div>

            <div className="intraday-custom-modal-list">
              {templates.length === 0 ? (
                <div className="intraday-custom-empty-cell">暂无模板</div>
              ) : (
                templates.map((item) => (
                  <div key={item.id} className="intraday-custom-modal-item">
                    <span>{item.name} · {item.expression}</span>
                    <button
                      type="button"
                      onClick={() => {
                        setTemplates((current) =>
                          current.filter((template) => template.id !== item.id),
                        )
                      }}
                    >
                      删除
                    </button>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>
      ) : null}
    </div>
  )
}
