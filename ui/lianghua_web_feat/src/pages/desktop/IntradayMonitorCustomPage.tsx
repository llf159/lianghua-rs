import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { ensureManagedSourcePath } from '../../apis/managedSource'
import {
  refreshIntradayMonitorRealtime,
  refreshIntradayMonitorTemplateTags,
  type IntradayMonitorRankModeConfig,
  type IntradayMonitorRow,
  type IntradayMonitorTemplate,
} from '../../apis/reader'
import IntradayTemplateManagerModal from './components/IntradayTemplateManagerModal'
import DetailsLink from '../../shared/DetailsLink'
import {
  formatConceptText,
  useConceptExclusions,
} from '../../shared/conceptExclusions'
import { normalizeTsCode } from '../../shared/stockCode'
import { readJsonStorage, writeJsonStorage } from '../../shared/storage'
import './css/IntradayMonitorCustomPage.css'

const TEMPLATE_STORAGE_KEY = 'lh_intraday_monitor_realtime_templates_v1'
const CUSTOM_MONITOR_STATE_KEY = 'lh_intraday_custom_monitor_state_v1'
const CONTINUOUS_MONITOR_INTERVAL_MS = 1000
const SPEED_HISTORY_KEEP_MS = 90_000
const SPEED_PERIOD_OPTIONS = [10, 30, 60] as const

type LoadingAction = 'refresh-realtime' | 'refresh-tags' | null
type SpeedPeriod = (typeof SPEED_PERIOD_OPTIONS)[number]

type PersistedCustomMonitorState = {
  codeInput: string
  selectedTemplateId: string
  rows: IntradayMonitorRow[]
  refreshedAt: string
}

type DeltaColumn =
  | 'realtime_price'
  | 'realtime_change_pct'
  | 'return_5d_pct'
  | 'realtime_vol_ratio'
type RowDeltaMap = Record<string, Partial<Record<DeltaColumn, number>>>
type PriceSnapshot = {
  capturedAt: number
  prices: Record<string, number>
}

const DELTA_COLUMNS = new Set<DeltaColumn>([
  'realtime_price',
  'realtime_change_pct',
  'return_5d_pct',
  'realtime_vol_ratio',
])

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

function formatClock(value: Date) {
  return value.toLocaleTimeString('zh-CN', {
    hour12: false,
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}

function formatRefreshTime(raw: string) {
  const value = raw.trim()
  if (!value) return '--'
  const withSeconds = value.match(/(\d{2}:\d{2}:\d{2})/)
  if (withSeconds) return withSeconds[1]
  const withMinutes = value.match(/(\d{2}:\d{2})/)
  return withMinutes ? withMinutes[1] : value
}

function formatDeltaValue(key: DeltaColumn, value?: number) {
  if (value === undefined || !Number.isFinite(value)) return null
  const sign = value > 0 ? '+' : ''
  if (key === 'realtime_change_pct' || key === 'return_5d_pct') {
    return `${sign}${value.toFixed(2)}%`
  }
  return `${sign}${value.toFixed(2)}`
}

function getPercentClassName(value?: number | null) {
  if (
    value === null ||
    value === undefined ||
    !Number.isFinite(value) ||
    value === 0
  ) {
    return 'intraday-custom-value-flat'
  }
  return value > 0 ? 'intraday-custom-value-up' : 'intraday-custom-value-down'
}

function getTagToneClassName(tone?: string | null) {
  if (tone === 'up') return 'intraday-custom-hit-badge-up'
  if (tone === 'down') return 'intraday-custom-hit-badge-down'
  return 'intraday-custom-hit-badge-neutral'
}

function isTemplateHit(row: IntradayMonitorRow) {
  return (
    row.template_tag_tone === 'up' &&
    typeof row.template_tag_text === 'string' &&
    row.template_tag_text.includes('命中')
  )
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value)
}

function getRowKey(row: IntradayMonitorRow) {
  return row.ts_code
}

function getNumericCellValue(row: IntradayMonitorRow, key: DeltaColumn) {
  const value = row[key]
  return isFiniteNumber(value) ? value : null
}

function buildRowDeltaMap(
  previousRows: IntradayMonitorRow[],
  nextRows: IntradayMonitorRow[],
) {
  const previousMap = new Map(previousRows.map((row) => [getRowKey(row), row]))
  const deltas: RowDeltaMap = {}

  for (const row of nextRows) {
    const previous = previousMap.get(getRowKey(row))
    if (!previous) continue

    const rowDeltas: Partial<Record<DeltaColumn, number>> = {}
    for (const key of DELTA_COLUMNS) {
      const previousValue = getNumericCellValue(previous, key)
      const nextValue = getNumericCellValue(row, key)
      if (previousValue === null || nextValue === null) continue

      const delta = nextValue - previousValue
      if (Math.abs(delta) > Number.EPSILON) {
        rowDeltas[key] = delta
      }
    }

    if (Object.keys(rowDeltas).length > 0) {
      deltas[getRowKey(row)] = rowDeltas
    }
  }

  return deltas
}

function buildPriceSnapshot(rows: IntradayMonitorRow[], capturedAt: number) {
  const prices: Record<string, number> = {}
  for (const row of rows) {
    if (isFiniteNumber(row.realtime_price) && row.realtime_price > 0) {
      prices[getRowKey(row)] = row.realtime_price
    }
  }
  return { capturedAt, prices }
}

function appendPriceSnapshot(
  history: PriceSnapshot[],
  rows: IntradayMonitorRow[],
  capturedAt: number,
) {
  const cutoff = capturedAt - SPEED_HISTORY_KEEP_MS
  return [
    ...history.filter((item) => item.capturedAt >= cutoff),
    buildPriceSnapshot(rows, capturedAt),
  ]
}

function buildSpeedMap(
  rows: IntradayMonitorRow[],
  history: PriceSnapshot[],
  periodSec: SpeedPeriod,
  now: number,
) {
  const target = now - periodSec * 1000
  let baseline: PriceSnapshot | null = null
  for (const snapshot of history) {
    if (snapshot.capturedAt <= target) {
      baseline = snapshot
    } else {
      break
    }
  }
  if (!baseline) return new Map<string, number>()

  const out = new Map<string, number>()
  for (const row of rows) {
    const currentPrice = row.realtime_price
    const previousPrice = baseline.prices[getRowKey(row)]
    if (
      isFiniteNumber(currentPrice) &&
      currentPrice > 0 &&
      isFiniteNumber(previousPrice) &&
      previousPrice > 0
    ) {
      out.set(getRowKey(row), (currentPrice / previousPrice - 1) * 100)
    }
  }
  return out
}

function waitForNextPaint() {
  if (typeof window === 'undefined') {
    return Promise.resolve()
  }
  return new Promise<void>((resolve) => {
    window.requestAnimationFrame(() => {
      window.requestAnimationFrame(() => resolve())
    })
  })
}

export default function IntradayMonitorCustomPage() {
  const { excludedConcepts } = useConceptExclusions()
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

  const persistedTemplates = useMemo(() => {
    const parsed = readJsonStorage<unknown>(
      typeof window === 'undefined' ? null : window.localStorage,
      TEMPLATE_STORAGE_KEY,
    )
    if (!Array.isArray(parsed)) {
      return []
    }
    return parsed
      .map(normalizeTemplate)
      .filter((item): item is IntradayMonitorTemplate => item !== null)
  }, [])

  const [sourcePath, setSourcePath] = useState('')
  const [codeInput, setCodeInput] = useState(() => persisted?.codeInput ?? '')
  const [rows, setRows] = useState<IntradayMonitorRow[]>(() => persisted?.rows ?? [])
  const [rowDeltas, setRowDeltas] = useState<RowDeltaMap>({})
  const [refreshedAt, setRefreshedAt] = useState(() => persisted?.refreshedAt ?? '')
  const [selectedTemplateId, setSelectedTemplateId] = useState(
    () => persisted?.selectedTemplateId ?? '',
  )
  const [templates, setTemplates] = useState<IntradayMonitorTemplate[]>(
    () => persistedTemplates,
  )
  const [loadingAction, setLoadingAction] = useState<LoadingAction>(null)
  const [notice, setNotice] = useState('')
  const [error, setError] = useState('')
  const [templateModalOpen, setTemplateModalOpen] = useState(false)
  const [continuousMonitorEnabled, setContinuousMonitorEnabled] = useState(false)
  const [speedPeriod, setSpeedPeriod] = useState<SpeedPeriod>(10)
  const [currentTime, setCurrentTime] = useState(() => new Date())
  const loadingRef = useRef(false)
  const rowsRef = useRef<IntradayMonitorRow[]>([])
  const refreshRealtimeRef = useRef<() => void>(() => {})
  const priceHistoryRef = useRef<PriceSnapshot[]>([])

  const sourcePathTrimmed = sourcePath.trim()
  const refreshingRealtime = loadingAction === 'refresh-realtime'
  const refreshingTags = loadingAction === 'refresh-tags'
  const isBusy = loadingAction !== null
  const speedMap = useMemo(
    () => buildSpeedMap(rows, priceHistoryRef.current, speedPeriod, Date.now()),
    [rows, speedPeriod],
  )
  const displayedRows = useMemo(() => {
    const hitRows: IntradayMonitorRow[] = []
    const otherRows: IntradayMonitorRow[] = []
    for (const row of rows) {
      if (isTemplateHit(row)) {
        hitRows.push(row)
      } else {
        otherRows.push(row)
      }
    }
    return [...hitRows, ...otherRows]
  }, [rows])
  const navigationItems = useMemo(
    () =>
      displayedRows.map((row) => ({
        tsCode: row.ts_code,
        tradeDate: typeof row.trade_date === 'string' ? row.trade_date : null,
        sourcePath: sourcePathTrimmed || undefined,
        name: typeof row.name === 'string' ? row.name : undefined,
      })),
    [displayedRows, sourcePathTrimmed],
  )

  useEffect(() => {
    void ensureManagedSourcePath()
      .then((value) => {
        setSourcePath(value)
      })
      .catch(() => {})
  }, [])

  useEffect(() => {
    const intervalId = window.setInterval(() => {
      setCurrentTime(new Date())
    }, 1000)
    return () => {
      window.clearInterval(intervalId)
    }
  }, [])

  useEffect(() => {
    loadingRef.current = isBusy
  }, [isBusy])

  useEffect(() => {
    rowsRef.current = rows
  }, [rows])

  const updateTemplates = useCallback((nextTemplates: IntradayMonitorTemplate[]) => {
    setTemplates(nextTemplates)
    writeJsonStorage(
      typeof window === 'undefined' ? null : window.localStorage,
      TEMPLATE_STORAGE_KEY,
      nextTemplates,
    )
  }, [])

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

  const topStatusText = useMemo(() => {
    if (refreshingRealtime) {
      return [`当前共 ${rows.length} 只`, '正在刷新实时行情，请稍候…']
        .filter(Boolean)
        .join(' | ')
    }

    return [
      `当前共 ${rows.length} 只`,
      selectedTemplateId !== '' ? '已启用模板标记' : '未启用模板标记',
    ]
      .filter(Boolean)
      .join(' | ')
  }, [refreshingRealtime, rows.length, selectedTemplateId])

  function onApplyCodeList() {
    const parts = splitCodes(codeInput)
    if (parts.length === 0) {
      setRows([])
      setRowDeltas({})
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
      realtime_trade_date: undefined,
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
    setRowDeltas({})
    priceHistoryRef.current = []
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

    setLoadingAction('refresh-realtime')
    setError('')
    setNotice('')
    await waitForNextPaint()
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
      const nextRows = result.rows ?? []
      setRowDeltas(buildRowDeltaMap(rows, nextRows))
      setRows(nextRows)
      priceHistoryRef.current = appendPriceSnapshot(
        priceHistoryRef.current,
        nextRows,
        Date.now(),
      )
      setRefreshedAt(result.refreshedAt || formatClock(new Date()))
      setError(result.warningMessage ?? '')
      setNotice(`刷新完成，共 ${result.rows?.length ?? 0} 只。`)
    } catch (runError) {
      setError(`刷新失败: ${String(runError)}`)
    } finally {
      setLoadingAction(null)
    }
  }

  refreshRealtimeRef.current = () => {
    void onRefreshRealtime()
  }

  useEffect(() => {
    if (!continuousMonitorEnabled) return undefined
    const intervalId = window.setInterval(() => {
      if (
        rowsRef.current.length > 0 &&
        !loadingRef.current &&
        sourcePathTrimmed !== ''
      ) {
        refreshRealtimeRef.current()
      }
    }, CONTINUOUS_MONITOR_INTERVAL_MS)

    return () => {
      window.clearInterval(intervalId)
    }
  }, [continuousMonitorEnabled, sourcePathTrimmed])

  async function onRefreshTemplateTagsOnly() {
    if (sourcePathTrimmed === '') {
      setError('请先到“数据管理”页完成数据准备。')
      return
    }
    if (rows.length === 0) {
      setError('请先输入名单并应用。')
      return
    }

    setLoadingAction('refresh-tags')
    setError('')
    setNotice('')
    await waitForNextPaint()
    try {
      const rankModeConfigs: IntradayMonitorRankModeConfig[] = [
        {
          mode: 'total',
          sceneName: '全部',
          templateId: selectedTemplateId,
        },
      ]
      const result = await refreshIntradayMonitorTemplateTags({
        sourcePath: sourcePathTrimmed,
        rows,
        templates,
        rankModeConfigs,
      })
      setRows(result.rows ?? [])
      setError(result.warningMessage ?? '')
      setNotice(`仅刷新标记完成，共 ${result.rows?.length ?? 0} 只。`)
    } catch (runError) {
      setError(`仅刷新标记失败: ${String(runError)}`)
    } finally {
      setLoadingAction(null)
    }
  }

  function onTemplateRemoved(templateId: string) {
    if (selectedTemplateId === templateId) {
      setSelectedTemplateId('')
    }
  }

  return (
    <div className="intraday-custom-page">
      <section className="intraday-custom-card">
        <div className="intraday-custom-section-head">
          <div>
            <h2 className="intraday-custom-title">自定义监控</h2>
            <div className="intraday-custom-status">{topStatusText}</div>
          </div>
        </div>
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
          <button type="button" onClick={() => onApplyCodeList()} disabled={isBusy}>
            应用名单
          </button>
          <button type="button" onClick={() => setTemplateModalOpen(true)} disabled={isBusy}>
            模板管理
          </button>
          <button
            type="button"
            className={[
              'intraday-custom-primary-btn',
              'intraday-custom-toolbar-btn',
              refreshingRealtime ? 'is-loading' : '',
            ]
              .filter(Boolean)
              .join(' ')}
            onClick={() => void onRefreshRealtime()}
            disabled={isBusy || rows.length === 0}
          >
            {refreshingRealtime ? '刷新实时中' : '刷新实时'}
          </button>
          <button
            type="button"
            className="intraday-custom-toolbar-btn"
            onClick={() => void onRefreshTemplateTagsOnly()}
            disabled={isBusy || rows.length === 0}
          >
            {refreshingTags ? '重算中...' : '仅刷新标记'}
          </button>
          <button
            type="button"
            className={[
              'intraday-custom-auto-toggle',
              continuousMonitorEnabled ? 'is-active' : '',
            ]
              .filter(Boolean)
              .join(' ')}
            role="switch"
            aria-checked={continuousMonitorEnabled}
            onClick={() => setContinuousMonitorEnabled((value) => !value)}
            disabled={sourcePathTrimmed === ''}
          >
            {continuousMonitorEnabled ? '持续监控中' : '持续监控'}
          </button>
          <label className="intraday-custom-inline-field">
            <span>涨速</span>
            <select
              value={speedPeriod}
              onChange={(event) =>
                setSpeedPeriod(Number(event.target.value) as SpeedPeriod)
              }
            >
              {SPEED_PERIOD_OPTIONS.map((value) => (
                <option key={value} value={value}>
                  {value}秒
                </option>
              ))}
            </select>
          </label>
        </div>

        {notice ? <div className="intraday-custom-notice">{notice}</div> : null}
        {error ? <div className="intraday-custom-error">{error}</div> : null}

        <div
          className={[
            'intraday-custom-scene-block',
            refreshingRealtime ? 'is-refreshing' : '',
          ]
            .filter(Boolean)
            .join(' ')}
          aria-busy={refreshingRealtime}
        >
          <div className="intraday-custom-scene-head">
            <h4>自定义名单</h4>
            <div className="intraday-custom-scene-head-actions">
              <span>{rows.length} 只</span>
              <span>{selectedTemplateId !== '' ? '模板标记开启' : '模板标记关闭'}</span>
              <div className="intraday-custom-time-strip" aria-live="polite">
                <span className="intraday-custom-time-pill">
                  <small>刷新</small>
                  <strong>{formatRefreshTime(refreshedAt)}</strong>
                </span>
                <span className="intraday-custom-time-pill is-current">
                  <small>当前</small>
                  <strong>{formatClock(currentTime)}</strong>
                </span>
              </div>
            </div>
          </div>
          <div className="intraday-custom-table-wrap">
            <table
              className="intraday-custom-table"
              style={{ minWidth: '1430px' }}
            >
              <colgroup>
                <col style={{ width: 72 }} />
                <col style={{ width: 112 }} />
                <col style={{ width: 110 }} />
                <col style={{ width: 96 }} />
                <col style={{ width: 108 }} />
                <col style={{ width: 100 }} />
                <col style={{ width: 92 }} />
                <col style={{ width: 160 }} />
                <col style={{ width: 108 }} />
                <col style={{ width: 96 }} />
                <col style={{ width: 116 }} />
                <col style={{ width: 260 }} />
              </colgroup>
              <thead>
                <tr>
                  <th>排名</th>
                  <th>代码</th>
                  <th>名称</th>
                  <th>实时价*</th>
                  <th>实时涨幅*</th>
                  <th>五日涨幅</th>
                  <th>涨速*</th>
                  <th>模板标记</th>
                  <th>实时量比*</th>
                  <th>板块</th>
                  <th>总市值(亿)</th>
                  <th>概念</th>
                </tr>
              </thead>
              <tbody>
                {rows.length === 0 ? (
                  <tr>
                    <td colSpan={12} className="intraday-custom-empty-cell">
                      暂无数据
                    </td>
                  </tr>
                ) : (
                  displayedRows.map((row) => {
                    const priceDelta = rowDeltas[getRowKey(row)]?.realtime_price
                    const priceDeltaText = formatDeltaValue(
                      'realtime_price',
                      priceDelta,
                    )
                    const pctDelta =
                      rowDeltas[getRowKey(row)]?.realtime_change_pct
                    const pctDeltaText = formatDeltaValue(
                      'realtime_change_pct',
                      pctDelta,
                    )
                    const return5dDelta =
                      rowDeltas[getRowKey(row)]?.return_5d_pct
                    const return5dDeltaText = formatDeltaValue(
                      'return_5d_pct',
                      return5dDelta,
                    )
                    const volRatioDelta =
                      rowDeltas[getRowKey(row)]?.realtime_vol_ratio
                    const volRatioDeltaText = formatDeltaValue(
                      'realtime_vol_ratio',
                      volRatioDelta,
                    )
                    const speedPct = speedMap.get(getRowKey(row))
                    const conceptText = formatConceptText(
                      row.concept ?? '',
                      excludedConcepts,
                    )

                    return (
                      <tr key={row.ts_code}>
                        <td>{formatNumber(row.rank, 0)}</td>
                        <td>{row.ts_code}</td>
                        <td>
                          <DetailsLink
                            className="intraday-custom-stock-link"
                            tsCode={row.ts_code}
                            tradeDate={typeof row.trade_date === 'string' ? row.trade_date : null}
                            sourcePath={sourcePathTrimmed || undefined}
                            title={`查看 ${row.name || row.ts_code} 详情`}
                            navigationItems={navigationItems}
                          >
                            {row.name || row.ts_code}
                          </DetailsLink>
                        </td>
                        <td>
                          <span className="intraday-custom-cell-value">
                            <span>{formatNumber(row.realtime_price)}</span>
                            {priceDeltaText ? (
                              <span
                                className={[
                                  'intraday-custom-delta',
                                  (priceDelta ?? 0) > 0
                                    ? 'intraday-custom-delta-up'
                                    : 'intraday-custom-delta-down',
                                ].join(' ')}
                              >
                                {priceDeltaText}
                              </span>
                            ) : null}
                          </span>
                        </td>
                        <td className={getPercentClassName(row.realtime_change_pct)}>
                          <span className="intraday-custom-cell-value">
                            <span>{formatPercent(row.realtime_change_pct)}</span>
                            {pctDeltaText ? (
                              <span
                                className={[
                                  'intraday-custom-delta',
                                  (pctDelta ?? 0) > 0
                                    ? 'intraday-custom-delta-up'
                                    : 'intraday-custom-delta-down',
                                ].join(' ')}
                              >
                                {pctDeltaText}
                              </span>
                            ) : null}
                          </span>
                        </td>
                        <td className={getPercentClassName(row.return_5d_pct)}>
                          <span className="intraday-custom-cell-value">
                            <span>{formatPercent(row.return_5d_pct)}</span>
                            {return5dDeltaText ? (
                              <span
                                className={[
                                  'intraday-custom-delta',
                                  (return5dDelta ?? 0) > 0
                                    ? 'intraday-custom-delta-up'
                                    : 'intraday-custom-delta-down',
                                ].join(' ')}
                              >
                                {return5dDeltaText}
                              </span>
                            ) : null}
                          </span>
                        </td>
                        <td className={getPercentClassName(speedPct)}>
                          {formatPercent(speedPct)}
                        </td>
                        <td>
                          <span
                            className={[
                              'intraday-custom-hit-badge',
                              getTagToneClassName(row.template_tag_tone),
                            ].join(' ')}
                          >
                            {row.template_tag_text && row.template_tag_text.trim() !== ''
                              ? row.template_tag_text
                              : '--'}
                          </span>
                        </td>
                        <td>
                          <span className="intraday-custom-cell-value">
                            <span>{formatNumber(row.realtime_vol_ratio)}</span>
                            {volRatioDeltaText ? (
                              <span
                                className={[
                                  'intraday-custom-delta',
                                  (volRatioDelta ?? 0) > 0
                                    ? 'intraday-custom-delta-up'
                                    : 'intraday-custom-delta-down',
                                ].join(' ')}
                              >
                                {volRatioDeltaText}
                              </span>
                            ) : null}
                          </span>
                        </td>
                        <td>{row.board || '--'}</td>
                        <td>{formatNumber(row.total_mv_yi)}</td>
                        <td
                          className="intraday-custom-cell-concept"
                          title={conceptText}
                        >
                          {conceptText}
                        </td>
                      </tr>
                    )
                  })
                )}
              </tbody>
            </table>
          </div>
          {refreshingRealtime ? (
            <div className="intraday-custom-refresh-overlay" role="status">
              <span className="intraday-custom-refresh-spinner" aria-hidden="true" />
              <span>正在刷新实时行情…</span>
            </div>
          ) : null}
        </div>
      </section>

      <IntradayTemplateManagerModal
        open={templateModalOpen}
        sourcePath={sourcePathTrimmed}
        templates={templates}
        onChangeTemplates={updateTemplates}
        onTemplateRemoved={onTemplateRemoved}
        onClose={() => setTemplateModalOpen(false)}
      />
    </div>
  )
}
