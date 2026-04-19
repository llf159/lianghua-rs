import { useEffect, useMemo, useState } from 'react'
import { ensureManagedSourcePath } from '../../apis/managedSource'
import {
  listSceneRankTradeDates,
  sceneRankOverviewPage,
  type SceneOverviewPageQuery,
  type SceneOverviewRow,
} from '../../apis/reader'
import {
  filterBoardItems,
  formatConceptText,
  isStBoard,
  useConceptExclusions,
} from '../../shared/conceptExclusions'
import { STOCK_PICK_BOARD_OPTIONS } from '../../shared/stockPickShared'
import DetailsLink from '../../shared/DetailsLink'
import {
  TableSortButton,
  getAriaSort,
  type SortDefinition,
  type SortDirection,
  useTableSort,
} from '../../shared/tableSort'
import { useRouteScrollRegion } from '../../shared/routeScroll'
import { readJsonStorage, readStoredSourcePath, writeJsonStorage } from '../../shared/storage'
import { DEFAULT_DATE_OPTION, normalizeTradeDates, pickDateValue } from '../../shared/tradeDate'
import './css/OverviewScenePage.css'

const OVERVIEW_PAGE_STATE_KEY = 'lh_scene_overview_page_state'
const OVERVIEW_PAGE_FILTER_STATE_KEY = 'lh_scene_overview_page_filter_state_v2'
const OVERVIEW_PAGE_RESULT_STATE_KEY = 'lh_scene_overview_page_result_state_v2'
const VISIBLE_COLUMNS = [
  'rank',
  'ts_code',
  'name',
  'total_mv_yi',
  'board',
  'scene_score',
  'risk_score',
  'scene_status',
  'concept',
] as const

type VisibleColumn = (typeof VISIBLE_COLUMNS)[number]

type AppliedConfig = {
  rankDate: string | null
  limit: number | null
  board: string | null
  totalMvMin: number | null
  totalMvMax: number | null
  rowCount: number
  sceneCount: number
}

type PersistedSceneOverviewFilterState = {
  sourcePath: string
  rankDateInput: string
  limitInput: string
  boardFilter: (typeof STOCK_PICK_BOARD_OPTIONS)[number]
  totalMvMinInput: string
  totalMvMaxInput: string
  selectedSceneName: string
  sortKey: string | null
  sortDirection: SortDirection
}

type PersistedSceneOverviewResultState = {
  rows: SceneOverviewRow[]
  dateOptions: string[]
  lastConfig: AppliedConfig | null
}

type PersistedSceneOverviewState = PersistedSceneOverviewFilterState &
  PersistedSceneOverviewResultState

const COLUMN_LABELS: Record<VisibleColumn, string> = {
  rank: '排名',
  ts_code: '代码',
  name: '名称',
  total_mv_yi: '总市值(亿)',
  board: '板块',
  scene_score: '场景分',
  risk_score: '风险分',
  scene_status: '场景状态',
  concept: '概念',
}

const COLUMN_WIDTHS: Record<VisibleColumn, number> = {
  rank: 64,
  ts_code: 120,
  name: 108,
  total_mv_yi: 110,
  board: 108,
  scene_score: 96,
  risk_score: 96,
  scene_status: 96,
  concept: 260,
}

function formatNumber(value?: number | null, digits = 2) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return '--'
  }
  return Number.isInteger(value) ? String(value) : value.toFixed(digits)
}

function formatCell(
  key: VisibleColumn,
  row: SceneOverviewRow,
  excludedConcepts: readonly string[],
) {
  if (key === 'concept') {
    return formatConceptText(row.concept, excludedConcepts)
  }
  if (key === 'rank') {
    return formatNumber(row.rank ?? null, 0)
  }
  if (key === 'scene_score') {
    return formatNumber(row.scene_score ?? null)
  }
  if (key === 'risk_score') {
    return formatNumber(row.risk_score ?? null)
  }
  if (key === 'total_mv_yi') {
    return formatNumber(row.total_mv_yi ?? null)
  }

  const value = row[key]
  if (value === null || value === undefined || value === '') {
    return '--'
  }
  return String(value)
}

function isSortableColumn(key: VisibleColumn) {
  return key !== 'ts_code' && key !== 'name' && key !== 'concept'
}

export default function OverviewScenePage() {
  const { excludedConcepts, excludeStBoard } = useConceptExclusions()
  const persistedState = useMemo(() => {
    const storage = typeof window === 'undefined' ? null : window.sessionStorage
    const parsed = readJsonStorage<Partial<PersistedSceneOverviewState>>(
      storage,
      OVERVIEW_PAGE_STATE_KEY,
    )
    const filterState = readJsonStorage<Partial<PersistedSceneOverviewFilterState>>(
      storage,
      OVERVIEW_PAGE_FILTER_STATE_KEY,
    )
    const resultState = readJsonStorage<Partial<PersistedSceneOverviewResultState>>(
      storage,
      OVERVIEW_PAGE_RESULT_STATE_KEY,
    )
    const merged = {
      ...parsed,
      ...filterState,
      ...resultState,
    }

    if (!merged || typeof merged !== 'object') {
      return null
    }

    return {
      sourcePath: typeof merged.sourcePath === 'string' ? merged.sourcePath : '',
      rankDateInput: typeof merged.rankDateInput === 'string' ? merged.rankDateInput : '',
      limitInput: typeof merged.limitInput === 'string' ? merged.limitInput : '100',
      boardFilter:
        merged.boardFilter && STOCK_PICK_BOARD_OPTIONS.includes(merged.boardFilter)
          ? merged.boardFilter
          : '全部',
      totalMvMinInput: typeof merged.totalMvMinInput === 'string' ? merged.totalMvMinInput : '',
      totalMvMaxInput: typeof merged.totalMvMaxInput === 'string' ? merged.totalMvMaxInput : '',
      rows: Array.isArray(merged.rows) ? merged.rows : [],
      dateOptions: Array.isArray(merged.dateOptions) ? merged.dateOptions : [],
      selectedSceneName:
        typeof merged.selectedSceneName === 'string' ? merged.selectedSceneName : '',
      lastConfig:
        merged.lastConfig && typeof merged.lastConfig === 'object'
          ? merged.lastConfig
          : null,
      sortKey: typeof merged.sortKey === 'string' ? merged.sortKey : null,
      sortDirection:
        merged.sortDirection === 'desc' || merged.sortDirection === 'asc'
          ? merged.sortDirection
          : null,
    } satisfies PersistedSceneOverviewState
  }, [])

  const [sourcePath, setSourcePath] = useState(
    () => persistedState?.sourcePath ?? readStoredSourcePath(),
  )
  const [rankDateInput, setRankDateInput] = useState(
    () => persistedState?.rankDateInput ?? '',
  )
  const [limitInput, setLimitInput] = useState(() => persistedState?.limitInput ?? '100')
  const [boardFilter, setBoardFilter] = useState<
    (typeof STOCK_PICK_BOARD_OPTIONS)[number]
  >(() => persistedState?.boardFilter ?? '全部')
  const [totalMvMinInput, setTotalMvMinInput] = useState(
    () => persistedState?.totalMvMinInput ?? '',
  )
  const [totalMvMaxInput, setTotalMvMaxInput] = useState(
    () => persistedState?.totalMvMaxInput ?? '',
  )
  const [rows, setRows] = useState<SceneOverviewRow[]>(() => persistedState?.rows ?? [])
  const [dateOptions, setDateOptions] = useState<string[]>(
    () => persistedState?.dateOptions ?? [],
  )
  const [selectedSceneName, setSelectedSceneName] = useState(
    () => persistedState?.selectedSceneName ?? '',
  )
  const [lastConfig, setLastConfig] = useState<AppliedConfig | null>(
    () => persistedState?.lastConfig ?? null,
  )
  const [loading, setLoading] = useState(false)
  const [dateOptionsLoading, setDateOptionsLoading] = useState(false)
  const [error, setError] = useState('')
  const boardOptions = useMemo(
    () => filterBoardItems(STOCK_PICK_BOARD_OPTIONS, excludeStBoard) as (typeof STOCK_PICK_BOARD_OPTIONS)[number][],
    [excludeStBoard],
  )

  const sourcePathTrimmed = sourcePath.trim()
  const sceneNames = useMemo(() => {
    const unique = new Set<string>()
    rows.forEach((row) => {
      const name = row.scene_name?.trim()
      if (name) {
        unique.add(name)
      }
    })
    return [...unique]
  }, [rows])

  const sceneRowCountMap = useMemo(() => {
    const map = new Map<string, number>()
    rows.forEach((row) => {
      const name = row.scene_name?.trim()
      if (!name) {
        return
      }
      map.set(name, (map.get(name) ?? 0) + 1)
    })
    return map
  }, [rows])

  useEffect(() => {
    if (!sceneNames.includes(selectedSceneName)) {
      setSelectedSceneName(sceneNames[0] ?? '')
    }
  }, [sceneNames, selectedSceneName])

  const displayedRows = useMemo(
    () => rows.filter((row) => row.scene_name === selectedSceneName),
    [rows, selectedSceneName],
  )

  const sortDefinitions = useMemo(
    () =>
      Object.fromEntries(
        VISIBLE_COLUMNS.filter((key) => isSortableColumn(key)).map((key) => [
          key,
          {
            value: (row: SceneOverviewRow) => row[key],
          } satisfies SortDefinition<SceneOverviewRow>,
        ]),
      ) as Partial<Record<VisibleColumn, SortDefinition<SceneOverviewRow>>>,
    [],
  )

  const { sortKey, sortDirection, sortedRows, toggleSort } = useTableSort(displayedRows, sortDefinitions, {
    key: (persistedState?.sortKey as VisibleColumn | null) ?? null,
    direction: persistedState?.sortDirection ?? null,
  })

  const tableMinWidth = useMemo(
    () => VISIBLE_COLUMNS.reduce((total, key) => total + COLUMN_WIDTHS[key], 0),
    [],
  )
  const tableWrapRef = useRouteScrollRegion<HTMLDivElement>('scene-overview-table', [
    sortedRows.length,
    tableMinWidth,
    selectedSceneName,
  ])
  const detailNavigationItems = sortedRows.map((row) => ({
    tsCode: row.ts_code,
    tradeDate: typeof row.trade_date === 'string' ? row.trade_date : null,
    sourcePath: sourcePathTrimmed || undefined,
    name: typeof row.name === 'string' ? row.name : undefined,
  }))

  useEffect(() => {
    let cancelled = false
    void ensureManagedSourcePath()
      .then((nextPath) => {
        if (!cancelled) {
          setSourcePath(nextPath)
        }
      })
      .catch(() => {})
    return () => {
      cancelled = true
    }
  }, [])

  useEffect(() => {
    if (excludeStBoard && isStBoard(boardFilter)) {
      setBoardFilter('全部')
    }
  }, [boardFilter, excludeStBoard])

  useEffect(() => {
    writeJsonStorage(
      typeof window === 'undefined' ? null : window.sessionStorage,
      OVERVIEW_PAGE_FILTER_STATE_KEY,
      {
        sourcePath,
        rankDateInput,
        limitInput,
        boardFilter,
        totalMvMinInput,
        totalMvMaxInput,
        selectedSceneName,
        sortKey,
        sortDirection,
      } satisfies PersistedSceneOverviewFilterState,
    )
  }, [
    sourcePath,
    rankDateInput,
    limitInput,
    boardFilter,
    totalMvMinInput,
    totalMvMaxInput,
    selectedSceneName,
    sortKey,
    sortDirection,
  ])

  useEffect(() => {
    writeJsonStorage(
      typeof window === 'undefined' ? null : window.sessionStorage,
      OVERVIEW_PAGE_RESULT_STATE_KEY,
      {
        rows,
        dateOptions,
        lastConfig,
      } satisfies PersistedSceneOverviewResultState,
    )
  }, [rows, dateOptions, lastConfig])

  useEffect(() => {
    if (!sourcePathTrimmed) {
      setDateOptions([])
      setRankDateInput(DEFAULT_DATE_OPTION)
      setDateOptionsLoading(false)
      return
    }

    let cancelled = false
    const loadDateOptions = async () => {
      setDateOptionsLoading(true)
      try {
        const values = normalizeTradeDates(await listSceneRankTradeDates(sourcePathTrimmed))
        if (cancelled) {
          return
        }
        setDateOptions(values)
        setRankDateInput((current) => pickDateValue(current, values))
        setError('')
      } catch (loadError) {
        if (cancelled) {
          return
        }
        setDateOptions([])
        setRankDateInput(DEFAULT_DATE_OPTION)
        setError(`读取日期列表失败: ${String(loadError)}`)
      } finally {
        if (!cancelled) {
          setDateOptionsLoading(false)
        }
      }
    }

    void loadDateOptions()
    return () => {
      cancelled = true
    }
  }, [sourcePathTrimmed])

  async function onRead() {
    if (!sourcePathTrimmed) {
      setError('请先到“数据管理”页完成数据准备')
      return
    }

    let limit: number | undefined
    const limitRaw = limitInput.trim()
    if (limitRaw) {
      const parsedLimit = Number(limitRaw)
      if (!Number.isInteger(parsedLimit) || parsedLimit <= 0) {
        setError('限制行数必须是正整数')
        return
      }
      limit = parsedLimit
    }

    let totalMvMin: number | undefined
    const minRaw = totalMvMinInput.trim()
    if (minRaw) {
      const parsedMin = Number(minRaw)
      if (!Number.isFinite(parsedMin)) {
        setError('总市值最小值必须是数字')
        return
      }
      totalMvMin = parsedMin
    }

    let totalMvMax: number | undefined
    const maxRaw = totalMvMaxInput.trim()
    if (maxRaw) {
      const parsedMax = Number(maxRaw)
      if (!Number.isFinite(parsedMax)) {
        setError('总市值最大值必须是数字')
        return
      }
      totalMvMax = parsedMax
    }

    if (
      totalMvMin !== undefined &&
      totalMvMax !== undefined &&
      totalMvMin > totalMvMax
    ) {
      setError('总市值最小值不能大于最大值')
      return
    }

    const query: SceneOverviewPageQuery = {
      sourcePath: sourcePathTrimmed,
      rankDate: rankDateInput.trim() || undefined,
      limit,
      board: boardFilter === '全部' ? undefined : boardFilter,
      excludeStBoard: excludeStBoard || undefined,
      totalMvMin,
      totalMvMax,
    }

    setLoading(true)
    setError('')
    try {
      const data = await sceneRankOverviewPage(query)
      const nextRows = data.rows ?? []
      const nextDateOptions = normalizeTradeDates(data.rank_date_options ?? [])
      const resolvedRankDate = data.resolved_rank_date?.trim() || query.rankDate || null

      if (nextDateOptions.length > 0) {
        setDateOptions(nextDateOptions)
        setRankDateInput(resolvedRankDate ?? pickDateValue(rankDateInput, nextDateOptions))
      } else if (resolvedRankDate) {
        setRankDateInput(resolvedRankDate)
      }

      const nextSceneNames = [...new Set(nextRows.map((row) => row.scene_name).filter(Boolean))]
      setRows(nextRows)
      setSelectedSceneName((current) =>
        nextSceneNames.includes(current) ? current : (nextSceneNames[0] ?? ''),
      )
      setLastConfig({
        rankDate: resolvedRankDate,
        limit: limit ?? null,
        board: query.board ?? null,
        totalMvMin: totalMvMin ?? null,
        totalMvMax: totalMvMax ?? null,
        rowCount: nextRows.length,
        sceneCount: nextSceneNames.length,
      })
    } catch (readError) {
      setError(`读取失败: ${String(readError)}`)
      setRows([])
      setSelectedSceneName('')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="overview-page">
      <section className="overview-card">
        <h2 className="overview-title">场景排名总览</h2>
        <div className="overview-source-note">当前数据目录：{sourcePathTrimmed || '读取中...'}</div>

        <div className="overview-form-grid">
          <label className="overview-field">
            <span>排名日期</span>
            <select
              value={rankDateInput}
              onChange={(event) => setRankDateInput(event.target.value)}
              disabled={dateOptionsLoading}
            >
              {dateOptions.length === 0 ? (
                <option value={DEFAULT_DATE_OPTION}>
                  {dateOptionsLoading ? '加载日期中...' : '最新'}
                </option>
              ) : (
                dateOptions.map((tradeDate) => (
                  <option key={tradeDate} value={tradeDate}>
                    {tradeDate}
                  </option>
                ))
              )}
            </select>
          </label>

          <label className="overview-field">
            <span>限制行数</span>
            <input
              type="number"
              min={1}
              step={1}
              value={limitInput}
              onChange={(event) => setLimitInput(event.target.value)}
              placeholder="100"
            />
          </label>

          <label className="overview-field">
            <span>板块筛选</span>
            <select
              value={boardFilter}
              onChange={(event) =>
                setBoardFilter(event.target.value as (typeof STOCK_PICK_BOARD_OPTIONS)[number])
              }
            >
              {boardOptions.map((board) => (
                <option key={board} value={board}>
                  {board}
                </option>
              ))}
            </select>
          </label>

          <label className="overview-field">
            <span>总市值最小(亿)</span>
            <input
              type="number"
              step={0.01}
              value={totalMvMinInput}
              onChange={(event) => setTotalMvMinInput(event.target.value)}
              placeholder="留空=不限"
            />
          </label>

          <label className="overview-field">
            <span>总市值最大(亿)</span>
            <input
              type="number"
              step={0.01}
              value={totalMvMaxInput}
              onChange={(event) => setTotalMvMaxInput(event.target.value)}
              placeholder="留空=不限"
            />
          </label>
        </div>

        <div className="overview-actions">
          <button
            className="overview-read-btn"
            type="button"
            onClick={() => void onRead()}
            disabled={loading || dateOptionsLoading || sourcePathTrimmed === ''}
          >
            {loading ? '读取中...' : '读取'}
          </button>
        </div>

        {error ? <div className="overview-error">{error}</div> : null}
      </section>

      {lastConfig ? (
        <section className="overview-card">
          <h3 className="overview-subtitle">本次读取配置</h3>
          <div className="overview-summary-grid">
            <div className="overview-summary-item">
              <span>排名日期</span>
              <strong>{lastConfig.rankDate ?? '最新'}</strong>
            </div>
            <div className="overview-summary-item">
              <span>限制行数</span>
              <strong>{lastConfig.limit ?? '不限'}</strong>
            </div>
            <div className="overview-summary-item">
              <span>板块筛选</span>
              <strong>{lastConfig.board ?? '不限'}</strong>
            </div>
            <div className="overview-summary-item">
              <span>总市值范围</span>
              <strong>
                {lastConfig.totalMvMin ?? '-'} ~ {lastConfig.totalMvMax ?? '-'} 亿
              </strong>
            </div>
            <div className="overview-summary-item">
              <span>场景数量</span>
              <strong>{lastConfig.sceneCount}</strong>
            </div>
            <div className="overview-summary-item">
              <span>返回行数</span>
              <strong>{lastConfig.rowCount}</strong>
            </div>
          </div>
        </section>
      ) : null}

      <section className="overview-card">
        <h3 className="overview-subtitle">场景菜单</h3>
        {sceneNames.length === 0 ? (
          <div className="overview-empty">暂无场景数据</div>
        ) : (
          <>
            <div className="overview-scene-headline">
              <div className="overview-scene-current">当前场景：{selectedSceneName || '--'}</div>
              <div className="overview-scene-count">共 {sceneNames.length} 个场景</div>
            </div>
            <div className="overview-scene-tabs" role="tablist" aria-label="场景选择菜单">
              {sceneNames.map((sceneName) => {
                const isActive = sceneName === selectedSceneName
                const rowCount = sceneRowCountMap.get(sceneName) ?? 0
                return (
                  <button
                    key={sceneName}
                    type="button"
                    role="tab"
                    aria-selected={isActive}
                    className={isActive ? 'overview-scene-tab is-active' : 'overview-scene-tab'}
                    onClick={() => setSelectedSceneName(sceneName)}
                  >
                    <span className="overview-scene-tab-name">{sceneName}</span>
                    <span className="overview-scene-tab-count">{rowCount}</span>
                  </button>
                )
              })}
            </div>
          </>
        )}
      </section>

      <section className="overview-card">
        <h3 className="overview-subtitle">结果表格</h3>
        {sortedRows.length === 0 ? (
          <div className="overview-empty">当前场景下暂无数据</div>
        ) : (
          <div className="overview-table-wrap" ref={tableWrapRef}>
            <table className="overview-table" style={{ minWidth: `${tableMinWidth}px` }}>
              <colgroup>
                {VISIBLE_COLUMNS.map((key) => (
                  <col key={key} style={{ width: `${COLUMN_WIDTHS[key]}px` }} />
                ))}
              </colgroup>
              <thead>
                <tr>
                  {VISIBLE_COLUMNS.map((key) => {
                    if (!isSortableColumn(key)) {
                      return <th key={key}>{COLUMN_LABELS[key]}</th>
                    }
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
                {sortedRows.map((row, index) => (
                  <tr key={`${row.scene_name}-${row.ts_code}-${row.trade_date ?? index}`}>
                    {VISIBLE_COLUMNS.map((key) => (
                      <td key={`${row.scene_name}-${row.ts_code}-${key}`}>
                        {key === 'name' && formatCell(key, row, excludedConcepts) !== '--' ? (
                          <DetailsLink
                            className="overview-stock-link"
                            tsCode={row.ts_code}
                            tradeDate={typeof row.trade_date === 'string' ? row.trade_date : null}
                            sourcePath={sourcePathTrimmed}
                            title={`查看 ${formatCell(key, row, excludedConcepts)} 详情`}
                            navigationItems={detailNavigationItems}
                          >
                            {formatCell(key, row, excludedConcepts)}
                          </DetailsLink>
                        ) : (
                          formatCell(key, row, excludedConcepts)
                        )}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  )
}
