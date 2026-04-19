import { useEffect, useMemo, useState } from 'react'
import { runConceptStockPick, type StockPickRow } from '../../apis/stockPick'
import {
  ConceptIncludeExcludePanels,
  buildAvailableConceptOptions,
  buildBoardFilterOptions,
  formatDateLabel,
  normalizeStringArray,
  STOCK_PICK_BOARD_OPTIONS,
  STOCK_PICK_MATCH_MODE_OPTIONS,
  StockPickResultTable,
  toggleStringSelection,
} from '../../shared/stockPickShared'
import { filterConceptItems, isStBoard, useConceptExclusions } from '../../shared/conceptExclusions'
import { useStockPickOutletContext } from './StockPickPage'
import { readJsonStorage, writeJsonStorage } from '../../shared/storage'

const CONCEPT_STOCK_PICK_STATE_KEY = 'concept-stock-pick-state-v1'
const CONCEPT_STOCK_PICK_FILTER_STATE_KEY = 'concept-stock-pick-filter-state-v2'
const CONCEPT_STOCK_PICK_RESULT_STATE_KEY = 'concept-stock-pick-result-state-v2'

type PersistedConceptStockPickFilterState = {
  board: (typeof STOCK_PICK_BOARD_OPTIONS)[number]
  tradeDate: string
  matchMode: (typeof STOCK_PICK_MATCH_MODE_OPTIONS)[number]
  conceptKeyword: string
  includeConcepts: string[]
  excludeConcepts: string[]
  selectedConcepts?: string[]
}

type PersistedConceptStockPickResultState = {
  rows: StockPickRow[]
  resolvedTradeDate: string
}

type PersistedConceptStockPickState = PersistedConceptStockPickFilterState &
  PersistedConceptStockPickResultState

export default function ConceptStockPickPage() {
  const { sourcePath, tradeDateOptions, latestTradeDate, conceptOptions, optionsLoading } = useStockPickOutletContext()
  const { excludedConcepts, excludeStBoard } = useConceptExclusions()
  const persistedState = useMemo(() => {
    const storage = typeof window === 'undefined' ? null : window.sessionStorage
    const parsed = readJsonStorage<Partial<PersistedConceptStockPickState>>(
      storage,
      CONCEPT_STOCK_PICK_STATE_KEY,
    )
    const filterState = readJsonStorage<Partial<PersistedConceptStockPickFilterState>>(
      storage,
      CONCEPT_STOCK_PICK_FILTER_STATE_KEY,
    )
    const resultState = readJsonStorage<Partial<PersistedConceptStockPickResultState>>(
      storage,
      CONCEPT_STOCK_PICK_RESULT_STATE_KEY,
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
      board:
        merged.board && STOCK_PICK_BOARD_OPTIONS.includes(merged.board)
          ? merged.board
          : '全部',
      tradeDate: typeof merged.tradeDate === 'string' ? merged.tradeDate : '',
      matchMode:
        merged.matchMode && STOCK_PICK_MATCH_MODE_OPTIONS.includes(merged.matchMode)
          ? merged.matchMode
          : 'OR',
      conceptKeyword: typeof merged.conceptKeyword === 'string' ? merged.conceptKeyword : '',
      includeConcepts: normalizeStringArray(
        Array.isArray(merged.includeConcepts)
          ? merged.includeConcepts.filter((item): item is string => typeof item === 'string')
          : Array.isArray(merged.selectedConcepts)
            ? merged.selectedConcepts.filter((item): item is string => typeof item === 'string')
            : [],
      ),
      excludeConcepts: normalizeStringArray(
        Array.isArray(merged.excludeConcepts)
          ? merged.excludeConcepts.filter((item): item is string => typeof item === 'string')
          : [],
      ),
      rows: Array.isArray(merged.rows) ? merged.rows : [],
      resolvedTradeDate: typeof merged.resolvedTradeDate === 'string' ? merged.resolvedTradeDate : '',
    } satisfies PersistedConceptStockPickState
  }, [])
  const [board, setBoard] = useState<(typeof STOCK_PICK_BOARD_OPTIONS)[number]>(() => persistedState?.board ?? '全部')
  const [tradeDate, setTradeDate] = useState(() => persistedState?.tradeDate ?? '')
  const [matchMode, setMatchMode] = useState<(typeof STOCK_PICK_MATCH_MODE_OPTIONS)[number]>(() => persistedState?.matchMode ?? 'OR')
  const [conceptKeyword, setConceptKeyword] = useState(() => persistedState?.conceptKeyword ?? '')
  const [includeConcepts, setIncludeConcepts] = useState<string[]>(() => persistedState?.includeConcepts ?? [])
  const [excludeConcepts, setExcludeConcepts] = useState<string[]>(() => persistedState?.excludeConcepts ?? [])
  const [rows, setRows] = useState<StockPickRow[]>(() => persistedState?.rows ?? [])
  const [resolvedTradeDate, setResolvedTradeDate] = useState(() => persistedState?.resolvedTradeDate ?? '')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const boardOptions = useMemo(
    () => buildBoardFilterOptions(STOCK_PICK_BOARD_OPTIONS, excludeStBoard),
    [excludeStBoard],
  )

  useEffect(() => {
    if (!latestTradeDate) {
      return
    }
    setTradeDate((current) => current || latestTradeDate)
  }, [latestTradeDate])

  useEffect(() => {
    if (excludeStBoard && isStBoard(board)) {
      setBoard('全部')
    }
  }, [board, excludeStBoard])

  useEffect(() => {
    setIncludeConcepts((current) => {
      const nextIncludeConcepts = filterConceptItems(current, excludedConcepts)
      return nextIncludeConcepts.length === current.length &&
        nextIncludeConcepts.every((item, index) => item === current[index])
        ? current
        : nextIncludeConcepts
    })
    setExcludeConcepts((current) => {
      const nextExcludeConcepts = filterConceptItems(current, excludedConcepts)
      return nextExcludeConcepts.length === current.length &&
        nextExcludeConcepts.every((item, index) => item === current[index])
        ? current
        : nextExcludeConcepts
    })
  }, [excludedConcepts])

  useEffect(() => {
    writeJsonStorage(
      typeof window === 'undefined' ? null : window.sessionStorage,
      CONCEPT_STOCK_PICK_FILTER_STATE_KEY,
      {
        board,
        tradeDate,
        matchMode,
        conceptKeyword,
        includeConcepts,
        excludeConcepts,
      } satisfies PersistedConceptStockPickFilterState,
    )
  }, [board, tradeDate, matchMode, conceptKeyword, includeConcepts, excludeConcepts])

  useEffect(() => {
    writeJsonStorage(
      typeof window === 'undefined' ? null : window.sessionStorage,
      CONCEPT_STOCK_PICK_RESULT_STATE_KEY,
      {
        rows,
        resolvedTradeDate,
      } satisfies PersistedConceptStockPickResultState,
    )
  }, [rows, resolvedTradeDate])

  const availableConceptOptions = useMemo(
    () => buildAvailableConceptOptions(conceptOptions, excludedConcepts),
    [conceptOptions, excludedConcepts],
  )

  function toggleIncludeConcept(value: string) {
    setIncludeConcepts((current) => toggleStringSelection(current, value))
    setExcludeConcepts((current) => current.filter((item) => item !== value))
  }

  function toggleExcludeConcept(value: string) {
    setExcludeConcepts((current) => toggleStringSelection(current, value))
    setIncludeConcepts((current) => current.filter((item) => item !== value))
  }

  async function onRun() {
    if (!sourcePath.trim()) {
      setError('当前数据目录为空。')
      return
    }

    setLoading(true)
    setError('')
    try {
      const result = await runConceptStockPick({
        sourcePath,
        board,
        excludeStBoard: excludeStBoard || undefined,
        tradeDate,
        includeConcepts,
        excludeConcepts,
        matchMode,
      })
      setRows(result.rows ?? [])
      setResolvedTradeDate(result.resolved_end_date ?? tradeDate)
    } catch (runError) {
      setRows([])
      setResolvedTradeDate('')
      setError(`概念选股失败: ${String(runError)}`)
    } finally {
      setLoading(false)
    }
  }

  return (
    <section className="stock-pick-card">
      <div className="stock-pick-section-head">
        <div>
          <h3 className="stock-pick-subtitle">概念选股</h3>
        </div>
      </div>

      <div className="stock-pick-form-grid">
        <label className="stock-pick-field">
          <span>选股范围</span>
          <select value={board} onChange={(event) => setBoard(event.target.value as typeof board)} disabled={optionsLoading}>
            {boardOptions.map((item) => (
              <option key={item} value={item}>
                {item}
              </option>
            ))}
          </select>
        </label>

        <label className="stock-pick-field">
          <span>统计日期</span>
          <select value={tradeDate} onChange={(event) => setTradeDate(event.target.value)} disabled={optionsLoading}>
            {tradeDateOptions.map((item) => (
              <option key={item} value={item}>
                {formatDateLabel(item)}
              </option>
            ))}
          </select>
        </label>

        <label className="stock-pick-field">
          <span>匹配模式</span>
          <select value={matchMode} onChange={(event) => setMatchMode(event.target.value as typeof matchMode)}>
            {STOCK_PICK_MATCH_MODE_OPTIONS.map((item) => (
              <option key={item} value={item}>
                {item}
              </option>
            ))}
          </select>
        </label>
      </div>

      <ConceptIncludeExcludePanels
        includeConcepts={includeConcepts}
        excludeConcepts={excludeConcepts}
        availableConceptOptions={availableConceptOptions}
        keyword={conceptKeyword}
        onKeywordChange={setConceptKeyword}
        onToggleInclude={toggleIncludeConcept}
        onToggleExclude={toggleExcludeConcept}
        onClearInclude={() => setIncludeConcepts([])}
        onClearExclude={() => setExcludeConcepts([])}
      />

      <div className="stock-pick-actions">
        <button type="button" className="stock-pick-primary-btn" onClick={() => void onRun()} disabled={loading || optionsLoading}>
          {loading ? '选股中...' : '执行选股'}
        </button>
      </div>

      {error ? <div className="stock-pick-message stock-pick-message-error">{error}</div> : null}

      <div className="stock-pick-result-head">
        <strong>结果列表</strong>
        <span>共 {rows.length} 只，统计日期：{formatDateLabel(resolvedTradeDate)}</span>
      </div>
      <StockPickResultTable
        rows={rows}
        tradeDate={resolvedTradeDate}
        sourcePath={sourcePath}
      />
    </section>
  )
}
