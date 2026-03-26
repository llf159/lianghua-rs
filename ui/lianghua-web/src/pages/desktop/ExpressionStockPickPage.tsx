import { useEffect, useMemo, useState } from 'react'
import { runExpressionStockPick, type StockPickRow } from '../../apis/stockPick'
import { STOCK_PICK_BOARD_OPTIONS, STOCK_PICK_SCOPE_OPTIONS, StockPickResultTable, formatDateLabel } from './stockPickShared'
import { useStockPickOutletContext } from './StockPickPage'
import { readJsonStorage } from '../../shared/storage'

const DEFAULT_EXPRESSION = ''
const EXPRESSION_STOCK_PICK_STATE_KEY = 'expression-stock-pick-state-v1'

type PersistedExpressionStockPickState = {
  board: (typeof STOCK_PICK_BOARD_OPTIONS)[number]
  startDate: string
  endDate: string
  scopeWay: (typeof STOCK_PICK_SCOPE_OPTIONS)[number]
  consecThreshold: string
  expression: string
  rows: StockPickRow[]
  resolvedEndDate: string
}

export default function ExpressionStockPickPage() {
  const { sourcePath, tradeDateOptions, latestTradeDate, optionsLoading } = useStockPickOutletContext()
  const persistedState = useMemo(() => {
    const parsed = readJsonStorage<Partial<PersistedExpressionStockPickState>>(
      typeof window === 'undefined' ? null : window.sessionStorage,
      EXPRESSION_STOCK_PICK_STATE_KEY,
    )
    if (!parsed || typeof parsed !== 'object') {
      return null
    }

    return {
      board:
        parsed.board && STOCK_PICK_BOARD_OPTIONS.includes(parsed.board)
          ? parsed.board
          : '全部',
      startDate: typeof parsed.startDate === 'string' ? parsed.startDate : '',
      endDate: typeof parsed.endDate === 'string' ? parsed.endDate : '',
      scopeWay:
        parsed.scopeWay && STOCK_PICK_SCOPE_OPTIONS.includes(parsed.scopeWay)
          ? parsed.scopeWay
          : 'LAST',
      consecThreshold: typeof parsed.consecThreshold === 'string' ? parsed.consecThreshold : '2',
      expression: typeof parsed.expression === 'string' ? parsed.expression : DEFAULT_EXPRESSION,
      rows: Array.isArray(parsed.rows) ? parsed.rows : [],
      resolvedEndDate: typeof parsed.resolvedEndDate === 'string' ? parsed.resolvedEndDate : '',
    } satisfies PersistedExpressionStockPickState
  }, [])
  const [board, setBoard] = useState<(typeof STOCK_PICK_BOARD_OPTIONS)[number]>(() => persistedState?.board ?? '全部')
  const [startDate, setStartDate] = useState(() => persistedState?.startDate ?? '')
  const [endDate, setEndDate] = useState(() => persistedState?.endDate ?? '')
  const [scopeWay, setScopeWay] = useState<(typeof STOCK_PICK_SCOPE_OPTIONS)[number]>(() => persistedState?.scopeWay ?? 'LAST')
  const [consecThreshold, setConsecThreshold] = useState(() => persistedState?.consecThreshold ?? '2')
  const [expression, setExpression] = useState(() => persistedState?.expression ?? DEFAULT_EXPRESSION)
  const [rows, setRows] = useState<StockPickRow[]>(() => persistedState?.rows ?? [])
  const [resolvedEndDate, setResolvedEndDate] = useState(() => persistedState?.resolvedEndDate ?? '')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  useEffect(() => {
    if (!latestTradeDate) {
      return
    }
    setStartDate((current) => current || latestTradeDate)
    setEndDate((current) => current || latestTradeDate)
  }, [latestTradeDate])

  useEffect(() => {
    try {
      window.sessionStorage.setItem(
        EXPRESSION_STOCK_PICK_STATE_KEY,
        JSON.stringify({
          board,
          startDate,
          endDate,
          scopeWay,
          consecThreshold,
          expression,
          rows,
          resolvedEndDate,
        } satisfies PersistedExpressionStockPickState),
      )
    } catch {
      // Ignore storage failures. The page still works without persistence.
    }
  }, [board, startDate, endDate, scopeWay, consecThreshold, expression, rows, resolvedEndDate])

  async function onRun() {
    if (!sourcePath.trim()) {
      setError('当前数据目录为空。')
      return
    }

    setLoading(true)
    setError('')
    try {
      const result = await runExpressionStockPick({
        sourcePath,
        board,
        startDate,
        endDate,
        scopeWay,
        expression,
        consecThreshold: scopeWay === 'CONSEC' ? Number(consecThreshold) : undefined,
      })
      setRows(result.rows ?? [])
      setResolvedEndDate(result.resolved_end_date ?? endDate)
    } catch (runError) {
      setRows([])
      setResolvedEndDate('')
      setError(`表达式选股失败: ${String(runError)}`)
    } finally {
      setLoading(false)
    }
  }

  return (
    <section className="stock-pick-card">
      <div className="stock-pick-section-head">
        <div>
          <h3 className="stock-pick-subtitle">表达式选股</h3>
        </div>
      </div>

      <div className="stock-pick-form-grid stock-pick-form-grid-expression">
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
          <span>起始日期</span>
          <select value={startDate} onChange={(event) => setStartDate(event.target.value)} disabled={optionsLoading}>
            {tradeDateOptions.map((item) => (
              <option key={item} value={item}>
                {formatDateLabel(item)}
              </option>
            ))}
          </select>
        </label>

        <label className="stock-pick-field">
          <span>结束日期</span>
          <select value={endDate} onChange={(event) => setEndDate(event.target.value)} disabled={optionsLoading}>
            {tradeDateOptions.map((item) => (
              <option key={item} value={item}>
                {formatDateLabel(item)}
              </option>
            ))}
          </select>
        </label>

        <label className="stock-pick-field">
          <span>选股方法</span>
          <select value={scopeWay} onChange={(event) => setScopeWay(event.target.value as typeof scopeWay)}>
            {STOCK_PICK_SCOPE_OPTIONS.map((item) => (
              <option key={item} value={item}>
                {item}
              </option>
            ))}
          </select>
        </label>

        {scopeWay === 'CONSEC' ? (
          <label className="stock-pick-field">
            <span>连续阈值</span>
            <input
              type="number"
              min={1}
              step={1}
              value={consecThreshold}
              onChange={(event) => setConsecThreshold(event.target.value)}
            />
          </label>
        ) : null}

        <label className="stock-pick-field stock-pick-field-span-full">
          <span>表达式</span>
          <textarea value={expression} onChange={(event) => setExpression(event.target.value)} rows={6} />
        </label>
      </div>

      <div className="stock-pick-actions">
        <button type="button" className="stock-pick-primary-btn" onClick={() => void onRun()} disabled={loading || optionsLoading}>
          {loading ? '选股中...' : '执行选股'}
        </button>
      </div>

      {error ? <div className="stock-pick-message stock-pick-message-error">{error}</div> : null}

      <div className="stock-pick-result-head">
        <strong>结果列表</strong>
        <span>共 {rows.length} 只，排序日期：{formatDateLabel(resolvedEndDate)}</span>
      </div>
      <StockPickResultTable rows={rows} tradeDate={resolvedEndDate} />
    </section>
  )
}
