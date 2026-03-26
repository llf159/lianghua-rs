import { useEffect, useMemo, useState } from 'react'
import { runConceptStockPick, type StockPickRow } from '../../apis/stockPick'
import {
  STOCK_PICK_BOARD_OPTIONS,
  STOCK_PICK_MATCH_MODE_OPTIONS,
  StockPickResultTable,
  formatDateLabel,
} from './stockPickShared'
import { filterConceptItems, useConceptExclusions } from '../../shared/conceptExclusions'
import { useStockPickOutletContext } from './StockPickPage'
import { readJsonStorage } from '../../shared/storage'

const CONCEPT_STOCK_PICK_STATE_KEY = 'concept-stock-pick-state-v1'

type PersistedConceptStockPickState = {
  board: (typeof STOCK_PICK_BOARD_OPTIONS)[number]
  tradeDate: string
  matchMode: (typeof STOCK_PICK_MATCH_MODE_OPTIONS)[number]
  conceptKeyword: string
  selectedConcepts: string[]
  rows: StockPickRow[]
  resolvedTradeDate: string
}

export default function ConceptStockPickPage() {
  const { sourcePath, tradeDateOptions, latestTradeDate, conceptOptions, optionsLoading } = useStockPickOutletContext()
  const { excludedConcepts } = useConceptExclusions()
  const persistedState = useMemo(() => {
    const parsed = readJsonStorage<Partial<PersistedConceptStockPickState>>(
      typeof window === 'undefined' ? null : window.sessionStorage,
      CONCEPT_STOCK_PICK_STATE_KEY,
    )
    if (!parsed || typeof parsed !== 'object') {
      return null
    }

    return {
      board:
        parsed.board && STOCK_PICK_BOARD_OPTIONS.includes(parsed.board)
          ? parsed.board
          : '全部',
      tradeDate: typeof parsed.tradeDate === 'string' ? parsed.tradeDate : '',
      matchMode:
        parsed.matchMode && STOCK_PICK_MATCH_MODE_OPTIONS.includes(parsed.matchMode)
          ? parsed.matchMode
          : 'OR',
      conceptKeyword: typeof parsed.conceptKeyword === 'string' ? parsed.conceptKeyword : '',
      selectedConcepts: Array.isArray(parsed.selectedConcepts)
        ? parsed.selectedConcepts.filter((item): item is string => typeof item === 'string')
        : [],
      rows: Array.isArray(parsed.rows) ? parsed.rows : [],
      resolvedTradeDate: typeof parsed.resolvedTradeDate === 'string' ? parsed.resolvedTradeDate : '',
    } satisfies PersistedConceptStockPickState
  }, [])
  const [board, setBoard] = useState<(typeof STOCK_PICK_BOARD_OPTIONS)[number]>(() => persistedState?.board ?? '全部')
  const [tradeDate, setTradeDate] = useState(() => persistedState?.tradeDate ?? '')
  const [matchMode, setMatchMode] = useState<(typeof STOCK_PICK_MATCH_MODE_OPTIONS)[number]>(() => persistedState?.matchMode ?? 'OR')
  const [conceptKeyword, setConceptKeyword] = useState(() => persistedState?.conceptKeyword ?? '')
  const [selectedConcepts, setSelectedConcepts] = useState<string[]>(() => persistedState?.selectedConcepts ?? [])
  const [rows, setRows] = useState<StockPickRow[]>(() => persistedState?.rows ?? [])
  const [resolvedTradeDate, setResolvedTradeDate] = useState(() => persistedState?.resolvedTradeDate ?? '')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  useEffect(() => {
    if (!latestTradeDate) {
      return
    }
    setTradeDate((current) => current || latestTradeDate)
  }, [latestTradeDate])

  useEffect(() => {
    setSelectedConcepts((current) => {
      const nextSelectedConcepts = filterConceptItems(current, excludedConcepts)
      return nextSelectedConcepts.length === current.length &&
        nextSelectedConcepts.every((item, index) => item === current[index])
        ? current
        : nextSelectedConcepts
    })
  }, [excludedConcepts])

  useEffect(() => {
    try {
      window.sessionStorage.setItem(
        CONCEPT_STOCK_PICK_STATE_KEY,
        JSON.stringify({
          board,
          tradeDate,
          matchMode,
          conceptKeyword,
          selectedConcepts,
          rows,
          resolvedTradeDate,
        } satisfies PersistedConceptStockPickState),
      )
    } catch {
      // Ignore storage failures. The page still works without persistence.
    }
  }, [board, tradeDate, matchMode, conceptKeyword, selectedConcepts, rows, resolvedTradeDate])

  const availableConceptOptions = useMemo(
    () => filterConceptItems(conceptOptions, excludedConcepts),
    [conceptOptions, excludedConcepts],
  )

  const filteredConceptOptions = useMemo(() => {
    const keyword = conceptKeyword.trim().toLowerCase()
    if (!keyword) {
      return availableConceptOptions
    }
    return availableConceptOptions.filter((item) => item.toLowerCase().includes(keyword))
  }, [availableConceptOptions, conceptKeyword])

  function toggleConcept(value: string) {
    setSelectedConcepts((current) =>
      current.includes(value) ? current.filter((item) => item !== value) : [...current, value],
    )
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
        tradeDate,
        concepts: selectedConcepts,
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
            {STOCK_PICK_BOARD_OPTIONS.map((item) => (
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

      <div className="stock-pick-concept-panel">
        <div className="stock-pick-concept-head">
          <strong>概念选择</strong>
          <span>已选 {selectedConcepts.length} 项</span>
        </div>
        <div className="stock-pick-concept-toolbar">
          <input
            type="text"
            value={conceptKeyword}
            onChange={(event) => setConceptKeyword(event.target.value)}
            placeholder="搜索概念"
            className="stock-pick-concept-search"
          />
          <button type="button" className="stock-pick-chip-btn" onClick={() => setSelectedConcepts([])}>
            清空选择
          </button>
        </div>
        <div className="stock-pick-concept-list">
          {filteredConceptOptions.map((item) => {
            const active = selectedConcepts.includes(item)
            return (
              <button
                key={item}
                type="button"
                className={active ? 'stock-pick-chip-btn is-active' : 'stock-pick-chip-btn'}
                onClick={() => toggleConcept(item)}
              >
                {item}
              </button>
            )
          })}
        </div>
      </div>

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
      <StockPickResultTable rows={rows} tradeDate={resolvedTradeDate} />
    </section>
  )
}
