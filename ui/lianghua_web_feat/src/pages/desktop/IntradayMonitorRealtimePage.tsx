import { useEffect, useMemo, useState } from 'react'
import { ensureManagedSourcePath } from '../../apis/managedSource'
import {
  intradayMonitorPage,
  listSceneRankTradeDates,
  refreshIntradayMonitorRealtime,
  type IntradayMonitorRow,
} from '../../apis/reader'
import { getStrategyManagePage } from '../../apis/strategyManage'
import { formatConceptText, useConceptExclusions } from '../../share/conceptExclusions'
import { STOCK_PICK_BOARD_OPTIONS } from '../../share/stockPickShared'
import DetailsLink from '../../shared/DetailsLink'
import {
  TableSortButton,
  getAriaSort,
  type SortDefinition,
  type SortDirection,
  useTableSort,
} from '../../share/tableSort'
import { readJsonStorage } from '../../shared/storage'
import { DEFAULT_DATE_OPTION, normalizeTradeDates, pickDateValue } from '../../shared/tradeDate'
import './css/IntradayMonitorRealtimePage.css'

const INTRADAY_MONITOR_PAGE_STATE_KEY = 'lh_intraday_monitor_realtime_page'
const REFRESH_BATCH_SIZE = 50
const VISIBLE_COLUMNS = [
  'scene_name',
  'rank',
  'ts_code',
  'name',
  'realtime_price',
  'realtime_change_pct',
  'template_tag',
  'realtime_vol_ratio',
  'scene_score',
  'risk_score',
  'scene_status',
  'board',
  'total_mv_yi',
  'concept',
] as const

type VisibleColumn = (typeof VISIBLE_COLUMNS)[number]
type TemplateDirection = 'up' | 'down'
type TemplateBase = 'preclose' | 'open'

type SceneConfig = {
  id: string
  sceneName: string
  templateId: string
}

type MarkTemplate = {
  id: string
  name: string
  direction: TemplateDirection
  thresholdPct: number
  base: TemplateBase
}

type PersistedIntradayMonitorState = {
  sourcePath: string
  rankDateInput: string
  limitInput: string
  boardFilter: (typeof STOCK_PICK_BOARD_OPTIONS)[number]
  totalMvMinInput: string
  totalMvMaxInput: string
  sceneConfigs: SceneConfig[]
  templates: MarkTemplate[]
  rows: IntradayMonitorRow[]
  dateOptions: string[]
  sceneOptions: string[]
  refreshedAt: string
  sortKey: string | null
  sortDirection: SortDirection
}

type SceneRowsGroup = {
  sceneName: string
  rows: IntradayMonitorRow[]
}

const COLUMN_LABELS: Record<VisibleColumn, string> = {
  scene_name: '场景',
  rank: '排名',
  ts_code: '代码',
  name: '名称',
  realtime_price: '实时价*',
  realtime_change_pct: '实时涨幅*',
  template_tag: '模板标记',
  realtime_vol_ratio: '实时量比*',
  scene_score: '场景分',
  risk_score: '风险分',
  scene_status: '场景状态',
  board: '板块',
  total_mv_yi: '总市值(亿)',
  concept: '概念',
}

const COLUMN_WIDTHS: Record<VisibleColumn, number> = {
  scene_name: 128,
  rank: 72,
  ts_code: 112,
  name: 110,
  realtime_price: 96,
  realtime_change_pct: 108,
  template_tag: 160,
  realtime_vol_ratio: 108,
  scene_score: 98,
  risk_score: 98,
  scene_status: 104,
  board: 96,
  total_mv_yi: 116,
  concept: 260,
}

function createId() {
  return typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function'
    ? crypto.randomUUID()
    : `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`
}

function createSceneConfig(sceneName = '全部', templateId = ''): SceneConfig {
  return { id: createId(), sceneName, templateId }
}

function createTemplate(name = ''): MarkTemplate {
  return {
    id: createId(),
    name,
    direction: 'up',
    thresholdPct: 3,
    base: 'preclose',
  }
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

function getPercentClassName(value?: number | null) {
  if (value === null || value === undefined || !Number.isFinite(value) || value === 0) {
    return 'intraday-monitor-value-flat'
  }
  return value > 0 ? 'intraday-monitor-value-up' : 'intraday-monitor-value-down'
}

function isSortableColumn(key: VisibleColumn) {
  return !['ts_code', 'name', 'concept', 'template_tag'].includes(key)
}

function formatCell(key: VisibleColumn, row: IntradayMonitorRow, excludedConcepts: readonly string[]) {
  if (key === 'concept') return formatConceptText(row.concept ?? '', excludedConcepts)
  if (key === 'rank') return formatNumber(row.rank, 0)
  if (key === 'scene_score') return formatNumber(row.scene_score)
  if (key === 'risk_score') return formatNumber(row.risk_score)
  if (key === 'total_mv_yi') return formatNumber(row.total_mv_yi)
  if (key === 'realtime_price') return formatNumber(row.realtime_price)
  if (key === 'realtime_change_pct') return formatPercent(row.realtime_change_pct)
  if (key === 'realtime_vol_ratio') return formatNumber(row.realtime_vol_ratio)
  const value = row[key]
  if (value === null || value === undefined || value === '') return '--'
  return String(value)
}

export default function IntradayMonitorRealtimePage() {
  const { excludedConcepts } = useConceptExclusions()

  const persistedState = useMemo(() => {
    const parsed = readJsonStorage<Partial<PersistedIntradayMonitorState>>(
      typeof window === 'undefined' ? null : window.sessionStorage,
      INTRADAY_MONITOR_PAGE_STATE_KEY,
    )
    if (!parsed || typeof parsed !== 'object') return null

    const sceneConfigs = Array.isArray(parsed.sceneConfigs)
      ? parsed.sceneConfigs
          .map((item) =>
            item && typeof item.id === 'string'
              ? {
                  id: item.id,
                  sceneName: typeof item.sceneName === 'string' ? item.sceneName : '全部',
                  templateId: typeof item.templateId === 'string' ? item.templateId : '',
                }
              : null,
          )
          .filter((item): item is SceneConfig => item !== null)
      : []

    const templates = Array.isArray(parsed.templates)
      ? parsed.templates
          .map((item) => {
            if (!item || typeof item !== 'object' || typeof item.id !== 'string') return null
            const threshold = Number(item.thresholdPct)
            return {
              id: item.id,
              name: typeof item.name === 'string' ? item.name : '',
              direction: item.direction === 'down' ? 'down' : 'up',
              thresholdPct: Number.isFinite(threshold) ? threshold : 0,
              base: item.base === 'open' ? 'open' : 'preclose',
            } satisfies MarkTemplate
          })
          .filter((item): item is MarkTemplate => item !== null)
      : []

    return {
      sourcePath: typeof parsed.sourcePath === 'string' ? parsed.sourcePath : '',
      rankDateInput: typeof parsed.rankDateInput === 'string' ? parsed.rankDateInput : DEFAULT_DATE_OPTION,
      limitInput: typeof parsed.limitInput === 'string' ? parsed.limitInput : '100',
      boardFilter:
        parsed.boardFilter && STOCK_PICK_BOARD_OPTIONS.includes(parsed.boardFilter)
          ? parsed.boardFilter
          : '全部',
      totalMvMinInput: typeof parsed.totalMvMinInput === 'string' ? parsed.totalMvMinInput : '',
      totalMvMaxInput: typeof parsed.totalMvMaxInput === 'string' ? parsed.totalMvMaxInput : '',
      sceneConfigs: sceneConfigs.length > 0 ? sceneConfigs : [createSceneConfig()],
      templates,
      rows: Array.isArray(parsed.rows) ? parsed.rows : [],
      dateOptions: Array.isArray(parsed.dateOptions) ? parsed.dateOptions : [],
      sceneOptions: Array.isArray(parsed.sceneOptions) ? parsed.sceneOptions : [],
      refreshedAt: typeof parsed.refreshedAt === 'string' ? parsed.refreshedAt : '',
      sortKey: typeof parsed.sortKey === 'string' ? parsed.sortKey : null,
      sortDirection: parsed.sortDirection === 'asc' || parsed.sortDirection === 'desc' ? parsed.sortDirection : null,
    } satisfies PersistedIntradayMonitorState
  }, [])

  const [sourcePath, setSourcePath] = useState(() => persistedState?.sourcePath ?? '')
  const [rankDateInput, setRankDateInput] = useState(() => persistedState?.rankDateInput ?? DEFAULT_DATE_OPTION)
  const [limitInput, setLimitInput] = useState(() => persistedState?.limitInput ?? '100')
  const [boardFilter, setBoardFilter] = useState<(typeof STOCK_PICK_BOARD_OPTIONS)[number]>(() => persistedState?.boardFilter ?? '全部')
  const [totalMvMinInput, setTotalMvMinInput] = useState(() => persistedState?.totalMvMinInput ?? '')
  const [totalMvMaxInput, setTotalMvMaxInput] = useState(() => persistedState?.totalMvMaxInput ?? '')
  const [sceneConfigs, setSceneConfigs] = useState<SceneConfig[]>(() => persistedState?.sceneConfigs ?? [createSceneConfig()])
  const [templates, setTemplates] = useState<MarkTemplate[]>(() => persistedState?.templates ?? [])
  const [rows, setRows] = useState<IntradayMonitorRow[]>(() => persistedState?.rows ?? [])
  const [dateOptions, setDateOptions] = useState<string[]>(() => persistedState?.dateOptions ?? [])
  const [sceneOptions, setSceneOptions] = useState<string[]>(() => persistedState?.sceneOptions ?? [])
  const [refreshedAt, setRefreshedAt] = useState(() => persistedState?.refreshedAt ?? '')

  const [loading, setLoading] = useState(false)
  const [loadingAction, setLoadingAction] = useState<'读取' | '刷新实时' | null>(null)
  const [dateOptionsLoading, setDateOptionsLoading] = useState(false)
  const [error, setError] = useState('')

  const [templateModalOpen, setTemplateModalOpen] = useState(false)
  const [draftTemplate, setDraftTemplate] = useState<MarkTemplate>(createTemplate(''))

  const sourcePathTrimmed = sourcePath.trim()

  const sortDefinitions = useMemo(
    () =>
      Object.fromEntries(
        VISIBLE_COLUMNS.filter((key) => isSortableColumn(key)).map((key) => [
          key,
          { value: (row: IntradayMonitorRow) => row[key] } satisfies SortDefinition<IntradayMonitorRow>,
        ]),
      ) as Partial<Record<VisibleColumn, SortDefinition<IntradayMonitorRow>>>,
    [],
  )

  const { sortKey, sortDirection, sortedRows, toggleSort } = useTableSort(rows, sortDefinitions, {
    key: (persistedState?.sortKey as VisibleColumn | null) ?? null,
    direction: persistedState?.sortDirection ?? null,
  })

  const tableMinWidth = useMemo(() => VISIBLE_COLUMNS.reduce((total, key) => total + COLUMN_WIDTHS[key], 0), [])

  useEffect(() => {
    void ensureManagedSourcePath().then(setSourcePath).catch(() => {})
  }, [])

  useEffect(() => {
    try {
      window.sessionStorage.setItem(
        INTRADAY_MONITOR_PAGE_STATE_KEY,
        JSON.stringify({
          sourcePath,
          rankDateInput,
          limitInput,
          boardFilter,
          totalMvMinInput,
          totalMvMaxInput,
          sceneConfigs,
          templates,
          rows,
          dateOptions,
          sceneOptions,
          refreshedAt,
          sortKey,
          sortDirection,
        } satisfies PersistedIntradayMonitorState),
      )
    } catch {}
  }, [sourcePath, rankDateInput, limitInput, boardFilter, totalMvMinInput, totalMvMaxInput, sceneConfigs, templates, rows, dateOptions, sceneOptions, refreshedAt, sortKey, sortDirection])

  useEffect(() => {
    if (!sourcePathTrimmed) {
      setDateOptions([])
      setSceneOptions([])
      setRankDateInput(DEFAULT_DATE_OPTION)
      return
    }
    let cancelled = false
    const loadFilters = async () => {
      setDateOptionsLoading(true)
      try {
        const [rankDates, sceneData] = await Promise.all([
          listSceneRankTradeDates(sourcePathTrimmed),
          getStrategyManagePage(sourcePathTrimmed),
        ])
        if (cancelled) return
        const nextDateOptions = normalizeTradeDates(rankDates)
        const nextSceneOptions = (sceneData.scenes ?? []).map((item) => item.name)
        setDateOptions(nextDateOptions)
        setRankDateInput((current) => pickDateValue(current, nextDateOptions))
        setSceneOptions(nextSceneOptions)
        setSceneConfigs((current) =>
          (current.length > 0 ? current : [createSceneConfig()]).map((config) => ({
            ...config,
            sceneName:
              config.sceneName === '全部' || nextSceneOptions.includes(config.sceneName)
                ? config.sceneName
                : '全部',
          })),
        )
      } catch (loadError) {
        if (!cancelled) setError(`读取盘中监控筛选项失败: ${String(loadError)}`)
      } finally {
        if (!cancelled) setDateOptionsLoading(false)
      }
    }
    void loadFilters()
    return () => {
      cancelled = true
    }
  }, [sourcePathTrimmed])

  const templateMap = useMemo(() => new Map(templates.map((item) => [item.id, item])), [templates])

  function getSceneTemplate(sceneName: string) {
    const exact = sceneConfigs.find((item) => item.sceneName === sceneName && item.templateId)
    if (exact && templateMap.has(exact.templateId)) return templateMap.get(exact.templateId) ?? null
    const allScene = sceneConfigs.find((item) => item.sceneName === '全部' && item.templateId)
    if (allScene && templateMap.has(allScene.templateId)) return templateMap.get(allScene.templateId) ?? null
    return null
  }

  function getTemplateTag(row: IntradayMonitorRow) {
    const tpl = getSceneTemplate(row.scene_name)
    if (!tpl) return { text: '未配置', tone: 'neutral' as const }

    const rowAny = row as Record<string, unknown>
    const openBasedRaw =
      rowAny.realtime_open_change_pct ??
      rowAny.realtime_change_open_pct ??
      rowAny.open_change_pct ??
      rowAny.change_pct_open
    const openBasedPct = typeof openBasedRaw === 'number' && Number.isFinite(openBasedRaw) ? openBasedRaw : null
    const closeBasedPct =
      typeof row.realtime_change_pct === 'number' && Number.isFinite(row.realtime_change_pct)
        ? row.realtime_change_pct
        : null

    const pct = tpl.base === 'open' ? openBasedPct : closeBasedPct
    if (pct === null) {
      return { text: `${tpl.name} · ${tpl.base === 'open' ? '开盘基准' : '昨收基准'} · 无实时`, tone: 'neutral' as const }
    }

    const threshold = Math.abs(tpl.thresholdPct)
    const hit = tpl.direction === 'up' ? pct >= threshold : pct <= -threshold
    const baseText = tpl.base === 'open' ? '开盘基准' : '昨收基准'

    if (hit) {
      return {
        text: `${tpl.name} · ${baseText} · ${tpl.direction === 'up' ? '涨' : '跌'}${threshold}% 命中`,
        tone: tpl.direction === 'up' ? ('up' as const) : ('down' as const),
      }
    }

    return {
      text: `${tpl.name} · ${baseText} · 未命中`,
      tone: 'neutral' as const,
    }
  }

  function addSceneConfig() {
    setSceneConfigs((current) => [...current, createSceneConfig('全部', templates[0]?.id ?? '')])
  }

  function removeSceneConfig(id: string) {
    setSceneConfigs((current) => (current.length <= 1 ? current : current.filter((item) => item.id !== id)))
  }

  async function onRead(actionLabel: '读取' | '刷新实时') {
    if (!sourcePathTrimmed) {
      setError('请先到“数据管理”页完成数据准备')
      return
    }

    const limitRaw = limitInput.trim()
    const limit = limitRaw ? Number(limitRaw) : undefined
    if (limitRaw && (!Number.isInteger(limit) || (limit ?? 0) <= 0)) {
      setError('限制行数必须是正整数')
      return
    }

    const totalMvMin = totalMvMinInput.trim() ? Number(totalMvMinInput.trim()) : undefined
    const totalMvMax = totalMvMaxInput.trim() ? Number(totalMvMaxInput.trim()) : undefined
    if ((totalMvMinInput.trim() && !Number.isFinite(totalMvMin)) || (totalMvMaxInput.trim() && !Number.isFinite(totalMvMax))) {
      setError('总市值输入必须是数字')
      return
    }
    if (totalMvMin !== undefined && totalMvMax !== undefined && totalMvMin > totalMvMax) {
      setError('总市值最小值不能大于最大值')
      return
    }

    setLoading(true)
    setLoadingAction(actionLabel)
    setError('')

    const requestRankDate = actionLabel === '读取' ? rankDateInput : DEFAULT_DATE_OPTION

    try {
      if (actionLabel === '读取') {
        const requests = (sceneConfigs.length > 0 ? sceneConfigs : [createSceneConfig()]).map((config) =>
          intradayMonitorPage({
            sourcePath: sourcePathTrimmed,
            rankDate: requestRankDate,
            sceneName: config.sceneName === '全部' ? undefined : config.sceneName,
            limit,
            board: boardFilter === '全部' ? undefined : boardFilter,
            totalMvMin,
            totalMvMax,
          }),
        )
        const results = await Promise.all(requests)
        setRows(results.flatMap((item) => item.rows ?? []))
        const mergedDateOptions = normalizeTradeDates(results.flatMap((item) => item.rank_date_options ?? []))
        const resolvedDate = results.find((item) => item.resolved_rank_date)?.resolved_rank_date ?? rankDateInput
        if (mergedDateOptions.length > 0) {
          setDateOptions(mergedDateOptions)
          setRankDateInput(pickDateValue(resolvedDate, mergedDateOptions))
        }
        setRefreshedAt(results.find((item) => item.refreshed_at)?.refreshed_at ?? '')
      } else {
        const refreshedRows: IntradayMonitorRow[] = []
        let refreshed = ''
        for (let start = 0; start < rows.length; start += REFRESH_BATCH_SIZE) {
          const data = await refreshIntradayMonitorRealtime({
            sourcePath: sourcePathTrimmed,
            rows: rows.slice(start, start + REFRESH_BATCH_SIZE),
          })
          refreshedRows.push(...(data.rows ?? []))
          if (!refreshed && data.refreshed_at) refreshed = data.refreshed_at
        }
        setRows(refreshedRows)
        setRefreshedAt(refreshed)
      }
    } catch (readError) {
      setError(`读取失败: ${String(readError)}`)
      setRows([])
      setRefreshedAt('')
    } finally {
      setLoading(false)
      setLoadingAction(null)
    }
  }

  const groupedSceneRows = useMemo<SceneRowsGroup[]>(() => {
    const groups = new Map<string, IntradayMonitorRow[]>()
    for (const row of sortedRows) {
      const key = row.scene_name || '未命名场景'
      if (!groups.has(key)) groups.set(key, [])
      groups.get(key)?.push(row)
    }
    return Array.from(groups.entries()).map(([sceneName, rowsInScene]) => ({ sceneName, rows: rowsInScene }))
  }, [sortedRows])

  async function refreshSceneRows(sceneName: string) {
    const targetRows = rows.filter((row) => row.scene_name === sceneName)
    if (targetRows.length === 0 || !sourcePathTrimmed) return
    setLoading(true)
    setLoadingAction('刷新实时')
    setError('')
    try {
      const refreshedRows: IntradayMonitorRow[] = []
      let refreshed = ''
      for (let start = 0; start < targetRows.length; start += REFRESH_BATCH_SIZE) {
        const data = await refreshIntradayMonitorRealtime({
          sourcePath: sourcePathTrimmed,
          rows: targetRows.slice(start, start + REFRESH_BATCH_SIZE),
        })
        refreshedRows.push(...(data.rows ?? []))
        if (!refreshed && data.refreshed_at) refreshed = data.refreshed_at
      }
      const map = new Map(refreshedRows.map((item) => [item.ts_code, item]))
      setRows((currentRows) => currentRows.map((item) => (item.scene_name === sceneName ? map.get(item.ts_code) ?? item : item)))
      setRefreshedAt(refreshed)
    } catch (refreshError) {
      setError(`刷新失败: ${String(refreshError)}`)
    } finally {
      setLoading(false)
      setLoadingAction(null)
    }
  }

  function renderTable(sceneRows: IntradayMonitorRow[], sceneName: string) {
    const sceneNavigationItems = sceneRows.map((row) => ({
      tsCode: row.ts_code,
      tradeDate: typeof row.trade_date === 'string' ? row.trade_date : null,
      sourcePath: sourcePathTrimmed || undefined,
      name: typeof row.name === 'string' ? row.name : undefined,
    }))

    return (
      <div className="intraday-monitor-table-wrap">
        <table className="intraday-monitor-table" style={{ minWidth: `${tableMinWidth}px` }}>
          <colgroup>
            {VISIBLE_COLUMNS.map((key) => (
              <col key={key} style={{ width: `${COLUMN_WIDTHS[key]}px` }} />
            ))}
          </colgroup>
          <thead>
            <tr>
              {VISIBLE_COLUMNS.map((key) => {
                if (!isSortableColumn(key)) return <th key={key}>{COLUMN_LABELS[key]}</th>
                const isActive = sortKey === key && sortDirection !== null
                return (
                  <th key={key} aria-sort={getAriaSort(isActive, sortDirection)}>
                    <TableSortButton
                      label={COLUMN_LABELS[key]}
                      isActive={isActive}
                      direction={sortDirection}
                      onClick={() => toggleSort(key)}
                      title={`按${COLUMN_LABELS[key]}排序`}
                    />
                  </th>
                )
              })}
            </tr>
          </thead>
          <tbody>
            {sceneRows.map((row, index) => (
              <tr key={`${sceneName}-${row.ts_code}-${row.trade_date ?? index}`}>
                {VISIBLE_COLUMNS.map((key) => {
                  if (key === 'template_tag') {
                    const tag = getTemplateTag(row)
                    return (
                      <td key={`${sceneName}-${row.ts_code}-${key}`}>
                        <span className={`intraday-monitor-hit-badge intraday-monitor-hit-badge-${tag.tone}`}>{tag.text}</span>
                      </td>
                    )
                  }
                  const displayText = formatCell(key, row, excludedConcepts)
                  const isRealtimePct = key === 'realtime_change_pct'
                  return (
                    <td key={`${sceneName}-${row.ts_code}-${key}`} className={isRealtimePct ? getPercentClassName(row.realtime_change_pct) : undefined}>
                      {key === 'name' && displayText !== '--' ? (
                        <DetailsLink
                          className="intraday-monitor-stock-link"
                          tsCode={row.ts_code}
                          tradeDate={typeof row.trade_date === 'string' ? row.trade_date : null}
                          sourcePath={sourcePathTrimmed}
                          title={`查看 ${displayText} 详情`}
                          navigationItems={sceneNavigationItems}
                        >
                          {displayText}
                        </DetailsLink>
                      ) : (
                        displayText
                      )}
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    )
  }

  const canDeleteSceneConfig = sceneConfigs.length > 1

  return (
    <div className="intraday-monitor-page">
      <section className="intraday-monitor-card">
        <h2 className="intraday-monitor-title">实时监控</h2>

        <div className="intraday-monitor-form-grid">
          <label className="intraday-monitor-field">
            <span>排名日期</span>
            <select value={rankDateInput} onChange={(event) => setRankDateInput(event.target.value)} disabled={dateOptionsLoading}>
              {dateOptions.length === 0 ? (
                <option value={DEFAULT_DATE_OPTION}>{dateOptionsLoading ? '加载日期中...' : '最新'}</option>
              ) : (
                dateOptions.map((tradeDate) => (
                  <option key={tradeDate} value={tradeDate}>{tradeDate}</option>
                ))
              )}
            </select>
          </label>

          <label className="intraday-monitor-field"><span>限制行数</span><input type="number" min={1} step={1} value={limitInput} onChange={(e) => setLimitInput(e.target.value)} placeholder="100" /></label>
          <label className="intraday-monitor-field"><span>板块筛选</span><select value={boardFilter} onChange={(e) => setBoardFilter(e.target.value as (typeof STOCK_PICK_BOARD_OPTIONS)[number])}>{STOCK_PICK_BOARD_OPTIONS.map((board) => <option key={board} value={board}>{board}</option>)}</select></label>
          <label className="intraday-monitor-field"><span>总市值最小(亿)</span><input type="number" step={0.01} value={totalMvMinInput} onChange={(e) => setTotalMvMinInput(e.target.value)} placeholder="留空=不限" /></label>
          <label className="intraday-monitor-field"><span>总市值最大(亿)</span><input type="number" step={0.01} value={totalMvMaxInput} onChange={(e) => setTotalMvMaxInput(e.target.value)} placeholder="留空=不限" /></label>
        </div>

        <div className="intraday-monitor-actions">
          <button className="intraday-monitor-refresh-btn" type="button" onClick={() => setTemplateModalOpen(true)} disabled={loading || dateOptionsLoading}>模板管理</button>
          <button className="intraday-monitor-refresh-btn" type="button" onClick={addSceneConfig} disabled={loading || dateOptionsLoading}>新增场景</button>
          <button className="intraday-monitor-read-btn" type="button" onClick={() => void onRead('读取')} disabled={loading || dateOptionsLoading || sourcePathTrimmed === ''}>{loading && loadingAction === '读取' ? '读取中...' : '读取'}</button>
        </div>

        <div className="intraday-monitor-scene-configs">
          {sceneConfigs.map((config, index) => {
            const template = templateMap.get(config.templateId)
            return (
              <section key={config.id} className="intraday-monitor-scene-config-block">
                <div className="intraday-monitor-scene-config-title">场景配置 {index + 1}</div>
                <div className="intraday-monitor-scene-config-row">
                  <label className="intraday-monitor-field intraday-monitor-field-inline">
                    <span>场景筛选</span>
                    <select value={config.sceneName} onChange={(e) => setSceneConfigs((current) => current.map((item) => item.id === config.id ? { ...item, sceneName: e.target.value } : item))} disabled={sceneOptions.length === 0}>
                      <option value="全部">全部</option>
                      {sceneOptions.map((sceneName) => <option key={sceneName} value={sceneName}>{sceneName}</option>)}
                    </select>
                  </label>
                  <label className="intraday-monitor-field intraday-monitor-field-inline">
                    <span>模板</span>
                    <select value={config.templateId} onChange={(e) => setSceneConfigs((current) => current.map((item) => item.id === config.id ? { ...item, templateId: e.target.value } : item))}>
                      <option value="">未选择</option>
                      {templates.map((tpl) => <option key={tpl.id} value={tpl.id}>{tpl.name}</option>)}
                    </select>
                  </label>
                  <button className="intraday-monitor-delete-btn" type="button" onClick={() => removeSceneConfig(config.id)} disabled={!canDeleteSceneConfig || loading || dateOptionsLoading}>删除</button>
                </div>
                <div className="intraday-monitor-config-badge-row">
                  <span className="intraday-monitor-config-badge intraday-monitor-config-badge-scene">场景：{config.sceneName}</span>
                  <span className="intraday-monitor-config-badge intraday-monitor-config-badge-template">模板：{template ? `${template.name}(${template.direction === 'up' ? '涨' : '跌'}${Math.abs(template.thresholdPct)}%, ${template.base === 'open' ? '开盘价' : '昨收'})` : '未选择'}</span>
                </div>
              </section>
            )
          })}
        </div>

        {error ? <div className="intraday-monitor-error">{error}</div> : null}
      </section>

      <section className="intraday-monitor-card">
        <div className="intraday-monitor-table-head">
          <h3 className="intraday-monitor-subtitle">结果表格</h3>
          <span className="intraday-monitor-table-tip">模板标记：按场景模板与实时涨幅计算命中（支持开盘/昨收基准标注）</span>
        </div>
        {sortedRows.length === 0 ? <div className="intraday-monitor-empty">暂无数据</div> : (
          <div className="intraday-monitor-scene-groups">
            {groupedSceneRows.map((group) => (
              <section key={group.sceneName} className="intraday-monitor-scene-block">
                <header className="intraday-monitor-scene-head">
                  <h4>{group.sceneName}</h4>
                  <button className="intraday-monitor-refresh-btn" type="button" onClick={() => void refreshSceneRows(group.sceneName)} disabled={loading || dateOptionsLoading || sourcePathTrimmed === ''}>{loading && loadingAction === '刷新实时' ? '刷新中...' : '刷新实时'}</button>
                </header>
                {renderTable(group.rows, group.sceneName)}
              </section>
            ))}
          </div>
        )}
      </section>

      {templateModalOpen ? (
        <div className="intraday-monitor-modal-mask" onClick={() => setTemplateModalOpen(false)}>
          <div className="intraday-monitor-modal" onClick={(event) => event.stopPropagation()}>
            <div className="intraday-monitor-modal-head">
              <h4>模板管理（涨跌幅标记）</h4>
              <button type="button" className="intraday-monitor-modal-close" onClick={() => setTemplateModalOpen(false)}>关闭</button>
            </div>

            <div className="intraday-monitor-modal-form intraday-monitor-template-grid">
              <input value={draftTemplate.name} onChange={(e) => setDraftTemplate((d) => ({ ...d, name: e.target.value }))} placeholder="模板名，例如：强势突破" />
              <select value={draftTemplate.direction} onChange={(e) => setDraftTemplate((d) => ({ ...d, direction: e.target.value as TemplateDirection }))}>
                <option value="up">涨幅</option>
                <option value="down">跌幅</option>
              </select>
              <input type="number" step={0.1} value={draftTemplate.thresholdPct} onChange={(e) => setDraftTemplate((d) => ({ ...d, thresholdPct: Number(e.target.value) }))} placeholder="阈值%" />
              <select value={draftTemplate.base} onChange={(e) => setDraftTemplate((d) => ({ ...d, base: e.target.value as TemplateBase }))}>
                <option value="preclose">相对昨收</option>
                <option value="open">相对开盘价</option>
              </select>
              <button
                type="button"
                onClick={() => {
                  const name = draftTemplate.name.trim()
                  if (!name) return
                  setTemplates((current) => [...current, { ...draftTemplate, id: createId(), thresholdPct: Math.abs(draftTemplate.thresholdPct) }])
                  setDraftTemplate(createTemplate(''))
                }}
              >新增模板</button>
            </div>

            <div className="intraday-monitor-modal-list">
              {templates.length === 0 ? (
                <div className="intraday-monitor-empty">暂无模板</div>
              ) : (
                templates.map((tpl) => (
                  <div key={tpl.id} className="intraday-monitor-modal-item">
                    <span>{tpl.name} · {tpl.direction === 'up' ? '涨' : '跌'}{Math.abs(tpl.thresholdPct)}% · {tpl.base === 'open' ? '开盘价' : '昨收'}</span>
                    <button type="button" onClick={() => {
                      setTemplates((current) => current.filter((item) => item.id !== tpl.id))
                      setSceneConfigs((current) => current.map((item) => item.templateId === tpl.id ? { ...item, templateId: '' } : item))
                    }}>删除</button>
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
