import { useEffect, useMemo, useState } from 'react'
import { ensureManagedSourcePath } from '../../apis/managedSource'
import {
  intradayMonitorPage,
  refreshIntradayMonitorRealtime,
  type IntradayMonitorRow,
} from '../../apis/reader'
import {
  formatConceptText,
  isStBoard,
  useConceptExclusions,
} from '../../shared/conceptExclusions'
import { STOCK_PICK_BOARD_OPTIONS, buildBoardFilterOptions } from '../../share/stockPickShared'
import DetailsLink from '../../shared/DetailsLink'
import {
  TableSortButton,
  getAriaSort,
  type SortDefinition,
  type SortDirection,
  useTableSort,
} from '../../shared/tableSort'
import { readJsonStorage, writeJsonStorage } from '../../shared/storage'
import { DEFAULT_DATE_OPTION, normalizeTradeDates, pickDateValue } from '../../shared/tradeDate'
import './css/IntradayMonitorRealtimePage.css'

const INTRADAY_MONITOR_PAGE_STATE_KEY = 'lh_intraday_monitor_realtime_page_v2'
const REFRESH_BATCH_SIZE = 50

const TOTAL_MODE_COLUMNS = [
  'rank',
  'ts_code',
  'name',
  'realtime_price',
  'realtime_change_pct',
  'template_tag',
  'realtime_vol_ratio',
  'total_score',
  'board',
  'total_mv_yi',
  'concept',
] as const

const SCENE_MODE_COLUMNS = [
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
  'total_score',
  'scene_status',
  'board',
  'total_mv_yi',
  'concept',
] as const

type TotalModeColumn = (typeof TOTAL_MODE_COLUMNS)[number]
type SceneModeColumn = (typeof SCENE_MODE_COLUMNS)[number]
type VisibleColumn = TotalModeColumn | SceneModeColumn

type RankMode = 'total' | 'scene'
type TemplateDirection = 'up' | 'down'
type TemplateBase = 'preclose' | 'open'

type MarkTemplate = {
  id: string
  name: string
  direction: TemplateDirection
  thresholdPct: number
  base: TemplateBase
}

type RankModeConfig = {
  mode: RankMode
  sceneName: string
  templateId: string
}

type PersistedIntradayMonitorState = {
  sourcePath: string
  rankDateInput: string
  limitInput: string
  boardFilter: (typeof STOCK_PICK_BOARD_OPTIONS)[number]
  totalMvMinInput: string
  totalMvMaxInput: string
  templates: MarkTemplate[]
  rankModeConfigs: RankModeConfig[]
  rows: IntradayMonitorRow[]
  dateOptions: string[]
  sceneOptions: string[]
  refreshedAt: string
  sortKey: string | null
  sortDirection: SortDirection
}

type SceneRowsGroup = {
  key: string
  title: string
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
  total_score: '总分',
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
  total_score: 96,
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

function createTemplate(name = ''): MarkTemplate {
  return {
    id: createId(),
    name,
    direction: 'up',
    thresholdPct: 3,
    base: 'preclose',
  }
}

function createRankModeConfig(mode: RankMode, sceneName = '全部', templateId = ''): RankModeConfig {
  return {
    mode,
    sceneName,
    templateId,
  }
}

function getRankModeLabel(mode: RankMode) {
  return mode === 'total' ? '总榜' : 'Scene榜'
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
  if (key === 'total_score') return formatNumber(row.total_score)
  if (key === 'total_mv_yi') return formatNumber(row.total_mv_yi)
  if (key === 'realtime_price') return formatNumber(row.realtime_price)
  if (key === 'realtime_change_pct') return formatPercent(row.realtime_change_pct)
  if (key === 'realtime_vol_ratio') return formatNumber(row.realtime_vol_ratio)

  const value = row[key]
  if (value === null || value === undefined || value === '') return '--'
  return String(value)
}

function getRowMode(row: IntradayMonitorRow): RankMode {
  return row.rank_mode === 'scene' ? 'scene' : 'total'
}

export default function IntradayMonitorRealtimePage() {
  const { excludedConcepts, excludeStBoard } = useConceptExclusions()

  const persistedState = useMemo(() => {
    const parsed = readJsonStorage<Partial<PersistedIntradayMonitorState>>(
      typeof window === 'undefined' ? null : window.sessionStorage,
      INTRADAY_MONITOR_PAGE_STATE_KEY,
    )
    if (!parsed || typeof parsed !== 'object') return null

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

    const rankModeConfigs = Array.isArray(parsed.rankModeConfigs)
      ? parsed.rankModeConfigs
          .map((item) => {
            if (!item || typeof item !== 'object') return null
            const mode = item.mode === 'scene' ? 'scene' : item.mode === 'total' ? 'total' : null
            if (!mode) return null
            return {
              mode,
              sceneName: typeof item.sceneName === 'string' ? item.sceneName : '全部',
              templateId: typeof item.templateId === 'string' ? item.templateId : '',
            } satisfies RankModeConfig
          })
          .filter((item): item is RankModeConfig => item !== null)
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
      templates,
      rankModeConfigs:
        rankModeConfigs.length > 0
          ? rankModeConfigs
          : [createRankModeConfig('total'), createRankModeConfig('scene')],
      rows: Array.isArray(parsed.rows) ? parsed.rows : [],
      dateOptions: Array.isArray(parsed.dateOptions) ? parsed.dateOptions : [],
      sceneOptions: Array.isArray(parsed.sceneOptions) ? parsed.sceneOptions : [],
      refreshedAt: typeof parsed.refreshedAt === 'string' ? parsed.refreshedAt : '',
      sortKey: typeof parsed.sortKey === 'string' ? parsed.sortKey : null,
      sortDirection:
        parsed.sortDirection === 'asc' || parsed.sortDirection === 'desc' ? parsed.sortDirection : null,
    } satisfies PersistedIntradayMonitorState
  }, [])

  const [sourcePath, setSourcePath] = useState(() => persistedState?.sourcePath ?? '')
  const [rankDateInput, setRankDateInput] = useState(() => persistedState?.rankDateInput ?? DEFAULT_DATE_OPTION)
  const [limitInput, setLimitInput] = useState(() => persistedState?.limitInput ?? '100')
  const [boardFilter, setBoardFilter] = useState<(typeof STOCK_PICK_BOARD_OPTIONS)[number]>(() => persistedState?.boardFilter ?? '全部')
  const [totalMvMinInput, setTotalMvMinInput] = useState(() => persistedState?.totalMvMinInput ?? '')
  const [totalMvMaxInput, setTotalMvMaxInput] = useState(() => persistedState?.totalMvMaxInput ?? '')
  const [templates, setTemplates] = useState<MarkTemplate[]>(() => persistedState?.templates ?? [])
  const [rankModeConfigs, setRankModeConfigs] = useState<RankModeConfig[]>(
    () => persistedState?.rankModeConfigs ?? [createRankModeConfig('total'), createRankModeConfig('scene')],
  )
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
  const boardOptions = useMemo(
    () => buildBoardFilterOptions(STOCK_PICK_BOARD_OPTIONS, excludeStBoard),
    [excludeStBoard],
  )

  const sourcePathTrimmed = sourcePath.trim()

  const totalModeRows = useMemo(() => rows.filter((row) => getRowMode(row) === 'total'), [rows])

  const sortDefinitions = useMemo(
    () =>
      Object.fromEntries(
        Array.from(new Set([...TOTAL_MODE_COLUMNS, ...SCENE_MODE_COLUMNS]))
          .filter((key) => isSortableColumn(key))
          .map((key) => [
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

  const sortedTotalRows = useMemo(
    () => sortedRows.filter((row) => getRowMode(row) === 'total'),
    [sortedRows],
  )

  const groupedSceneRows = useMemo<SceneRowsGroup[]>(() => {
    const groups = new Map<string, IntradayMonitorRow[]>()
    for (const row of sortedRows) {
      if (getRowMode(row) !== 'scene') continue
      const key = row.scene_name || '未命名场景'
      if (!groups.has(key)) groups.set(key, [])
      groups.get(key)?.push(row)
    }
    return Array.from(groups.entries()).map(([sceneName, rowsInScene]) => ({
      key: sceneName,
      title: sceneName,
      rows: rowsInScene,
    }))
  }, [sortedRows])

  const hasTotalConfig = useMemo(
    () => rankModeConfigs.some((item) => item.mode === 'total'),
    [rankModeConfigs],
  )

  useEffect(() => {
    void ensureManagedSourcePath().then(setSourcePath).catch(() => {})
  }, [])

  useEffect(() => {
    if (excludeStBoard && isStBoard(boardFilter)) {
      setBoardFilter('全部')
    }
  }, [boardFilter, excludeStBoard])

  useEffect(() => {
    writeJsonStorage(
      typeof window === 'undefined' ? null : window.sessionStorage,
      INTRADAY_MONITOR_PAGE_STATE_KEY,
      {
        sourcePath,
        rankDateInput,
        limitInput,
        boardFilter,
        totalMvMinInput,
        totalMvMaxInput,
        templates,
        rankModeConfigs,
        rows,
        dateOptions,
        sceneOptions,
        refreshedAt,
        sortKey,
        sortDirection,
      } satisfies PersistedIntradayMonitorState,
    )
  }, [
    sourcePath,
    rankDateInput,
    limitInput,
    boardFilter,
    totalMvMinInput,
    totalMvMaxInput,
    templates,
    rankModeConfigs,
    rows,
    dateOptions,
    sceneOptions,
    refreshedAt,
    sortKey,
    sortDirection,
  ])

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
        const [totalResult, sceneResult] = await Promise.allSettled([
          intradayMonitorPage({ sourcePath: sourcePathTrimmed, rankMode: 'total', rankDate: DEFAULT_DATE_OPTION, limit: 1 }),
          intradayMonitorPage({ sourcePath: sourcePathTrimmed, rankMode: 'scene', rankDate: DEFAULT_DATE_OPTION, limit: 1 }),
        ])
        if (cancelled) return

        const totalData = totalResult.status === 'fulfilled' ? totalResult.value : null
        const sceneData = sceneResult.status === 'fulfilled' ? sceneResult.value : null

        if (!totalData && !sceneData) {
          throw new Error('总榜与Scene榜筛选项都读取失败')
        }

        const mergedDateOptions = normalizeTradeDates([
          ...(totalData?.rank_date_options ?? []),
          ...(sceneData?.rank_date_options ?? []),
        ])
        setDateOptions(mergedDateOptions)
        setRankDateInput((current) => pickDateValue(current, mergedDateOptions))

        const nextSceneOptions = Array.from(
          new Set((sceneData?.scene_options ?? []).map((item) => item.trim()).filter((item) => item !== '')),
        )
        setSceneOptions(nextSceneOptions)

        setRankModeConfigs((current) =>
          current.map((config) => {
            if (config.mode !== 'scene') return config
            return {
              ...config,
              sceneName:
                config.sceneName === '全部' || nextSceneOptions.includes(config.sceneName)
                  ? config.sceneName
                  : '全部',
            }
          }),
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

  function getAppliedTemplate(row: IntradayMonitorRow) {
    const mode = getRowMode(row)
    if (mode === 'total') {
      const totalConfig = rankModeConfigs.find((item) => item.mode === 'total' && item.templateId)
      return totalConfig ? templateMap.get(totalConfig.templateId) ?? null : null
    }

    const exact = rankModeConfigs.find(
      (item) => item.mode === 'scene' && item.sceneName === row.scene_name && item.templateId,
    )
    if (exact && templateMap.has(exact.templateId)) return templateMap.get(exact.templateId) ?? null

    const allScene = rankModeConfigs.find(
      (item) => item.mode === 'scene' && item.sceneName === '全部' && item.templateId,
    )
    if (allScene && templateMap.has(allScene.templateId)) return templateMap.get(allScene.templateId) ?? null

    return null
  }

  function getTemplateTag(row: IntradayMonitorRow) {
    const tpl = getAppliedTemplate(row)
    if (!tpl) return { text: '未配置', tone: 'neutral' as const }

    const openBasedPct =
      typeof row.realtime_change_open_pct === 'number' && Number.isFinite(row.realtime_change_open_pct)
        ? row.realtime_change_open_pct
        : null
    const closeBasedPct =
      typeof row.realtime_change_pct === 'number' && Number.isFinite(row.realtime_change_pct)
        ? row.realtime_change_pct
        : null

    const pct = tpl.base === 'open' ? openBasedPct : closeBasedPct
    if (pct === null) {
      return {
        text: `${tpl.name} · ${tpl.base === 'open' ? '开盘基准' : '昨收基准'} · 无实时`,
        tone: 'neutral' as const,
      }
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

  function addRankModeConfig(mode: RankMode) {
    if (mode === 'total') {
      if (hasTotalConfig) return
      setRankModeConfigs((current) => [...current, createRankModeConfig('total', '全部', templates[0]?.id ?? '')])
      return
    }
    setRankModeConfigs((current) => [...current, createRankModeConfig('scene', '全部', templates[0]?.id ?? '')])
  }

  function removeRankModeConfig(index: number) {
    setRankModeConfigs((current) => {
      if (current.length <= 1) return current
      return current.filter((_, idx) => idx !== index)
    })
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
    if (
      (totalMvMinInput.trim() && !Number.isFinite(totalMvMin)) ||
      (totalMvMaxInput.trim() && !Number.isFinite(totalMvMax))
    ) {
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
        const normalizedConfigs = rankModeConfigs.length > 0
          ? rankModeConfigs
          : [createRankModeConfig('total'), createRankModeConfig('scene')]

        const requests = normalizedConfigs.map((config) =>
          intradayMonitorPage({
            sourcePath: sourcePathTrimmed,
            rankMode: config.mode,
            rankDate: requestRankDate,
            sceneName: config.mode === 'scene' && config.sceneName !== '全部' ? config.sceneName : undefined,
            limit,
            board: boardFilter === '全部' ? undefined : boardFilter,
            excludeStBoard: excludeStBoard || undefined,
            totalMvMin,
            totalMvMax,
          }),
        )

        const settledResults = await Promise.allSettled(requests)
        const successResults = settledResults
          .filter((item) => item.status === 'fulfilled')
          .map((item) => item.value)

        if (successResults.length === 0) {
          const firstReason = settledResults.find((item) => item.status === 'rejected')
          throw (firstReason?.status === 'rejected' ? firstReason.reason : new Error('读取失败'))
        }

        const failedCount = settledResults.length - successResults.length
        if (failedCount > 0) {
          setError(`部分榜单读取失败：${failedCount} 个区块未返回，已展示其余数据`)
        }

        const mergedDateOptions = normalizeTradeDates(successResults.flatMap((item) => item.rank_date_options ?? []))
        const resolvedDate = successResults.find((item) => item.resolved_rank_date)?.resolved_rank_date ?? rankDateInput
        if (mergedDateOptions.length > 0) {
          setDateOptions(mergedDateOptions)
          setRankDateInput(pickDateValue(resolvedDate, mergedDateOptions))
        }

        const mergedSceneOptions = Array.from(
          new Set(successResults.flatMap((item) => item.scene_options ?? []).map((item) => item.trim()).filter((item) => item !== '')),
        )
        if (mergedSceneOptions.length > 0) {
          setSceneOptions(mergedSceneOptions)
        }

        const rowMap = new Map<string, IntradayMonitorRow>()
        for (const result of successResults) {
          for (const row of result.rows ?? []) {
            const mode = getRowMode(row)
            const unique = `${mode}|${row.scene_name}|${row.ts_code}|${row.trade_date ?? ''}`
            rowMap.set(unique, row)
          }
        }
        setRows(Array.from(rowMap.values()))
        setRefreshedAt(successResults.find((item) => item.refreshed_at)?.refreshed_at ?? '')
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

  async function refreshRowsByGroup(groupKey: string) {
    const targetRows =
      groupKey === 'total'
        ? rows.filter((row) => getRowMode(row) === 'total')
        : rows.filter((row) => getRowMode(row) === 'scene' && row.scene_name === groupKey)
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

      const refreshedMap = new Map(
        refreshedRows.map((item) => [
          `${getRowMode(item)}|${item.scene_name}|${item.ts_code}|${item.trade_date ?? ''}`,
          item,
        ]),
      )
      setRows((currentRows) =>
        currentRows.map((item) => {
          const key = `${getRowMode(item)}|${item.scene_name}|${item.ts_code}|${item.trade_date ?? ''}`
          return refreshedMap.get(key) ?? item
        }),
      )
      setRefreshedAt(refreshed)
    } catch (refreshError) {
      setError(`刷新失败: ${String(refreshError)}`)
    } finally {
      setLoading(false)
      setLoadingAction(null)
    }
  }

  function renderTable(displayedRows: IntradayMonitorRow[], columns: readonly VisibleColumn[]) {
    const tableMinWidth = columns.reduce((total, key) => total + COLUMN_WIDTHS[key], 0)
    const navigationItems = displayedRows.map((row) => ({
      tsCode: row.ts_code,
      tradeDate: typeof row.trade_date === 'string' ? row.trade_date : null,
      sourcePath: sourcePathTrimmed || undefined,
      name: typeof row.name === 'string' ? row.name : undefined,
    }))

    return (
      <div className="intraday-monitor-table-wrap">
        <table className="intraday-monitor-table" style={{ minWidth: `${tableMinWidth}px` }}>
          <colgroup>
            {columns.map((key) => (
              <col key={key} style={{ width: `${COLUMN_WIDTHS[key]}px` }} />
            ))}
          </colgroup>
          <thead>
            <tr>
              {columns.map((key) => {
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
            {displayedRows.map((row, index) => (
              <tr key={`${getRowMode(row)}-${row.scene_name}-${row.ts_code}-${row.trade_date ?? index}`}>
                {columns.map((key) => {
                  if (key === 'template_tag') {
                    const tag = getTemplateTag(row)
                    return (
                      <td key={`${getRowMode(row)}-${row.ts_code}-${key}`}>
                        <span className={`intraday-monitor-hit-badge intraday-monitor-hit-badge-${tag.tone}`}>{tag.text}</span>
                      </td>
                    )
                  }

                  const displayText = formatCell(key, row, excludedConcepts)
                  const isRealtimePct = key === 'realtime_change_pct'
                  return (
                    <td key={`${getRowMode(row)}-${row.ts_code}-${key}`} className={isRealtimePct ? getPercentClassName(row.realtime_change_pct) : undefined}>
                      {key === 'name' && displayText !== '--' ? (
                        <DetailsLink
                          className="intraday-monitor-stock-link"
                          tsCode={row.ts_code}
                          tradeDate={typeof row.trade_date === 'string' ? row.trade_date : null}
                          sourcePath={sourcePathTrimmed}
                          title={`查看 ${displayText} 详情`}
                          navigationItems={navigationItems}
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

  const rankModeConfigItems = rankModeConfigs.map((config, index) => ({
    index,
    key: `${config.mode}-${index}`,
    mode: config.mode,
    sceneName: config.sceneName,
    templateId: config.templateId,
    template: templateMap.get(config.templateId),
    canDelete: rankModeConfigs.length > 1,
  }))

  return (
    <div className="intraday-monitor-page">
      <section className="intraday-monitor-card">
        <h2 className="intraday-monitor-title">实时监控（总榜 + Scene榜）</h2>

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

          <label className="intraday-monitor-field">
            <span>限制行数</span>
            <input type="number" min={1} step={1} value={limitInput} onChange={(event) => setLimitInput(event.target.value)} placeholder="100" />
          </label>

          <label className="intraday-monitor-field">
            <span>板块筛选</span>
            <select value={boardFilter} onChange={(event) => setBoardFilter(event.target.value as (typeof STOCK_PICK_BOARD_OPTIONS)[number])}>
              {boardOptions.map((board) => (
                <option key={board} value={board}>{board}</option>
              ))}
            </select>
          </label>

          <label className="intraday-monitor-field">
            <span>总市值最小(亿)</span>
            <input type="number" step={0.01} value={totalMvMinInput} onChange={(event) => setTotalMvMinInput(event.target.value)} placeholder="留空=不限" />
          </label>

          <label className="intraday-monitor-field">
            <span>总市值最大(亿)</span>
            <input type="number" step={0.01} value={totalMvMaxInput} onChange={(event) => setTotalMvMaxInput(event.target.value)} placeholder="留空=不限" />
          </label>
        </div>

        <div className="intraday-monitor-actions">
          <button className="intraday-monitor-refresh-btn" type="button" onClick={() => setTemplateModalOpen(true)} disabled={loading || dateOptionsLoading}>模板管理</button>
          <button className="intraday-monitor-refresh-btn" type="button" onClick={() => addRankModeConfig('total')} disabled={loading || dateOptionsLoading || hasTotalConfig}>添加总榜区块</button>
          <button className="intraday-monitor-refresh-btn" type="button" onClick={() => addRankModeConfig('scene')} disabled={loading || dateOptionsLoading}>添加Scene榜区块</button>
          <button className="intraday-monitor-read-btn" type="button" onClick={() => void onRead('读取')} disabled={loading || dateOptionsLoading || sourcePathTrimmed === ''}>{loading && loadingAction === '读取' ? '读取中...' : '读取'}</button>
          <button className="intraday-monitor-refresh-btn" type="button" onClick={() => void onRead('刷新实时')} disabled={loading || dateOptionsLoading || sourcePathTrimmed === '' || rows.length === 0}>{loading && loadingAction === '刷新实时' ? '刷新中...' : '全部刷新实时'}</button>
        </div>

        <div className="intraday-monitor-config-list">
          {rankModeConfigItems.map((item) => (
            <section key={item.key} className="intraday-monitor-config-card">
              <div className="intraday-monitor-config-header">
                <h4>{getRankModeLabel(item.mode)}配置</h4>
                <button className="intraday-monitor-delete-btn" type="button" disabled={!item.canDelete || loading || dateOptionsLoading} onClick={() => removeRankModeConfig(item.index)}>删除</button>
              </div>

              <div className="intraday-monitor-config-grid">
                <label className="intraday-monitor-field intraday-monitor-field-inline">
                  <span>榜单类型</span>
                  <select
                    value={item.mode}
                    onChange={(event) => {
                      const nextMode = event.target.value === 'scene' ? 'scene' : 'total'
                      setRankModeConfigs((current) => {
                        const currentRow = current[item.index]
                        if (!currentRow) return current
                        if (nextMode === currentRow.mode) return current
                        if (nextMode === 'total' && current.some((cfg, idx) => cfg.mode === 'total' && idx !== item.index)) {
                          return current
                        }
                        return current.map((cfg, idx) =>
                          idx === item.index
                            ? { ...cfg, mode: nextMode, sceneName: nextMode === 'scene' ? cfg.sceneName : '全部' }
                            : cfg,
                        )
                      })
                    }}
                    disabled={loading || dateOptionsLoading}
                  >
                    <option value="total">总榜</option>
                    <option value="scene">Scene榜</option>
                  </select>
                </label>

                {item.mode === 'scene' ? (
                  <label className="intraday-monitor-field intraday-monitor-field-inline">
                    <span>场景筛选</span>
                    <select
                      value={item.sceneName}
                      onChange={(event) => {
                        const value = event.target.value
                        setRankModeConfigs((current) =>
                          current.map((cfg, idx) => (idx === item.index ? { ...cfg, sceneName: value } : cfg)),
                        )
                      }}
                      disabled={sceneOptions.length === 0}
                    >
                      <option value="全部">全部</option>
                      {sceneOptions.map((sceneName) => (
                        <option key={sceneName} value={sceneName}>{sceneName}</option>
                      ))}
                    </select>
                  </label>
                ) : (
                  <div className="intraday-monitor-placeholder">总榜不需要场景筛选</div>
                )}

                <label className="intraday-monitor-field intraday-monitor-field-inline">
                  <span>模板</span>
                  <select
                    value={item.templateId}
                    onChange={(event) => {
                      const value = event.target.value
                      setRankModeConfigs((current) =>
                        current.map((cfg, idx) => (idx === item.index ? { ...cfg, templateId: value } : cfg)),
                      )
                    }}
                  >
                    <option value="">未选择</option>
                    {templates.map((tpl) => (
                      <option key={tpl.id} value={tpl.id}>{tpl.name}</option>
                    ))}
                  </select>
                </label>
              </div>

              <div className="intraday-monitor-config-badge-row">
                <span className="intraday-monitor-config-badge intraday-monitor-config-badge-scene">类型：{getRankModeLabel(item.mode)}</span>
                {item.mode === 'scene' ? <span className="intraday-monitor-config-badge intraday-monitor-config-badge-scene">场景：{item.sceneName}</span> : null}
                <span className="intraday-monitor-config-badge intraday-monitor-config-badge-template">模板：{item.template ? `${item.template.name}(${item.template.direction === 'up' ? '涨' : '跌'}${Math.abs(item.template.thresholdPct)}%, ${item.template.base === 'open' ? '开盘价' : '昨收'})` : '未选择'}</span>
              </div>
            </section>
          ))}
        </div>

        {error ? <div className="intraday-monitor-error">{error}</div> : null}
      </section>

      <section className="intraday-monitor-card">
        <div className="intraday-monitor-table-head">
          <h3 className="intraday-monitor-subtitle">结果表格</h3>
          <span className="intraday-monitor-table-tip">* 实时数据来自行情接口，模板标记支持昨收/开盘双基准</span>
        </div>

        {refreshedAt ? <div className="intraday-monitor-refreshed">最近刷新：{refreshedAt}</div> : null}

        {rows.length === 0 ? (
          <div className="intraday-monitor-empty">暂无数据</div>
        ) : (
          <div className="intraday-monitor-result-sections">
            {rankModeConfigs.some((item) => item.mode === 'total') ? (
              <section className="intraday-monitor-result-block">
                <header className="intraday-monitor-scene-head">
                  <h4>总榜</h4>
                  <button className="intraday-monitor-refresh-btn" type="button" onClick={() => void refreshRowsByGroup('total')} disabled={loading || dateOptionsLoading || sourcePathTrimmed === '' || totalModeRows.length === 0}>{loading && loadingAction === '刷新实时' ? '刷新中...' : '刷新总榜实时'}</button>
                </header>
                {sortedTotalRows.length === 0 ? (
                  <div className="intraday-monitor-empty">总榜暂无数据</div>
                ) : (
                  renderTable(sortedTotalRows, TOTAL_MODE_COLUMNS)
                )}
              </section>
            ) : null}

            {rankModeConfigs.some((item) => item.mode === 'scene') ? (
              <section className="intraday-monitor-result-block">
                <header className="intraday-monitor-scene-head">
                  <h4>Scene榜</h4>
                </header>
                {groupedSceneRows.length === 0 ? (
                  <div className="intraday-monitor-empty">Scene榜暂无数据</div>
                ) : (
                  <div className="intraday-monitor-scene-groups">
                    {groupedSceneRows.map((group) => (
                      <section key={group.key} className="intraday-monitor-scene-block">
                        <header className="intraday-monitor-scene-head">
                          <h4>{group.title}</h4>
                          <button className="intraday-monitor-refresh-btn" type="button" onClick={() => void refreshRowsByGroup(group.key)} disabled={loading || dateOptionsLoading || sourcePathTrimmed === ''}>{loading && loadingAction === '刷新实时' ? '刷新中...' : '刷新该Scene实时'}</button>
                        </header>
                        {renderTable(group.rows, SCENE_MODE_COLUMNS)}
                      </section>
                    ))}
                  </div>
                )}
              </section>
            ) : null}
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
              <input value={draftTemplate.name} onChange={(event) => setDraftTemplate((draft) => ({ ...draft, name: event.target.value }))} placeholder="模板名，例如：强势突破" />
              <select value={draftTemplate.direction} onChange={(event) => setDraftTemplate((draft) => ({ ...draft, direction: event.target.value as TemplateDirection }))}>
                <option value="up">涨幅</option>
                <option value="down">跌幅</option>
              </select>
              <input type="number" step={0.1} value={draftTemplate.thresholdPct} onChange={(event) => setDraftTemplate((draft) => ({ ...draft, thresholdPct: Number(event.target.value) }))} placeholder="阈值%" />
              <select value={draftTemplate.base} onChange={(event) => setDraftTemplate((draft) => ({ ...draft, base: event.target.value as TemplateBase }))}>
                <option value="preclose">相对昨收</option>
                <option value="open">相对开盘价</option>
              </select>
              <button
                type="button"
                onClick={() => {
                  const name = draftTemplate.name.trim()
                  if (!name) return
                  setTemplates((current) => [
                    ...current,
                    {
                      ...draftTemplate,
                      id: createId(),
                      thresholdPct: Math.abs(draftTemplate.thresholdPct),
                    },
                  ])
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
                    <button
                      type="button"
                      onClick={() => {
                        setTemplates((current) => current.filter((item) => item.id !== tpl.id))
                        setRankModeConfigs((current) =>
                          current.map((item) => (item.templateId === tpl.id ? { ...item, templateId: '' } : item)),
                        )
                      }}
                    >删除</button>
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
