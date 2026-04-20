import { useEffect, useMemo, useState } from 'react'
import { runExpressionStockPick, type StockPickRow } from '../../apis/stockPick'
import {
  buildBoardFilterOptions,
  STOCK_PICK_BOARD_OPTIONS,
  STOCK_PICK_SCOPE_OPTIONS,
  StockPickResultTable,
  formatDateLabel,
} from '../../shared/stockPickShared'
import { isStBoard, useConceptExclusions } from '../../shared/conceptExclusions'
import { useStockPickOutletContext } from './StockPickPage'
import { readJsonStorage, writeJsonStorage } from '../../shared/storage'

const DEFAULT_EXPRESSION = ''
const EXPRESSION_STOCK_PICK_STATE_KEY = 'expression-stock-pick-state'
const EXPRESSION_STOCK_PICK_FILTER_STATE_KEY = 'expression-stock-pick-filter-state-v2'
const EXPRESSION_STOCK_PICK_RESULT_STATE_KEY = 'expression-stock-pick-result-state-v2'
const COPY_SEPARATOR_OPTIONS = [
  { label: '逗号 (,)', value: ',' },
  { label: '分号 (;)', value: ';' },
  { label: '竖线 (|)', value: '|' },
  { label: '空格 ( )', value: ' ' },
  { label: '换行 (\\n)', value: '\n' },
] as const

type CopySeparatorOption = (typeof COPY_SEPARATOR_OPTIONS)[number]['value']

type PersistedExpressionStockPickFilterState = {
  board: (typeof STOCK_PICK_BOARD_OPTIONS)[number]
  referenceTradeDate: string
  lookbackPeriods: string
  scopeWay: (typeof STOCK_PICK_SCOPE_OPTIONS)[number]
  consecThreshold: string
  expression: string
}

type PersistedExpressionStockPickResultState = {
  rows: StockPickRow[]
  resolvedStartDate: string
  resolvedReferenceTradeDate: string
}

type PersistedExpressionStockPickState = PersistedExpressionStockPickFilterState &
  PersistedExpressionStockPickResultState

type LegacyPersistedExpressionStockPickState = Partial<PersistedExpressionStockPickState> & {
  startDate?: string
  endDate?: string
  resolvedEndDate?: string
}

function parsePositiveInt(value: string, fallback: number) {
  const parsed = Number(value)
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback
}

export default function ExpressionStockPickPage() {
  const { sourcePath, tradeDateOptions, latestTradeDate, optionsLoading } = useStockPickOutletContext()
  const { excludeStBoard } = useConceptExclusions()
  const persistedState = useMemo(() => {
    const storage = typeof window === 'undefined' ? null : window.sessionStorage
    const parsed = readJsonStorage<LegacyPersistedExpressionStockPickState>(
      storage,
      EXPRESSION_STOCK_PICK_STATE_KEY,
    )
    const filterState = readJsonStorage<Partial<PersistedExpressionStockPickFilterState>>(
      storage,
      EXPRESSION_STOCK_PICK_FILTER_STATE_KEY,
    )
    const resultState = readJsonStorage<Partial<PersistedExpressionStockPickResultState>>(
      storage,
      EXPRESSION_STOCK_PICK_RESULT_STATE_KEY,
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
      referenceTradeDate:
        typeof merged.referenceTradeDate === 'string'
          ? merged.referenceTradeDate
          : typeof merged.endDate === 'string'
            ? merged.endDate
            : '',
      lookbackPeriods:
        typeof merged.lookbackPeriods === 'string' ? merged.lookbackPeriods : '1',
      scopeWay:
        merged.scopeWay && STOCK_PICK_SCOPE_OPTIONS.includes(merged.scopeWay)
          ? merged.scopeWay
          : 'LAST',
      consecThreshold: typeof merged.consecThreshold === 'string' ? merged.consecThreshold : '2',
      expression: typeof merged.expression === 'string' ? merged.expression : DEFAULT_EXPRESSION,
      rows: Array.isArray(merged.rows) ? merged.rows : [],
      resolvedStartDate: typeof merged.resolvedStartDate === 'string' ? merged.resolvedStartDate : '',
      resolvedReferenceTradeDate:
        typeof merged.resolvedReferenceTradeDate === 'string'
          ? merged.resolvedReferenceTradeDate
          : typeof merged.resolvedEndDate === 'string'
            ? merged.resolvedEndDate
            : '',
    } satisfies PersistedExpressionStockPickState
  }, [])
  const [board, setBoard] = useState<(typeof STOCK_PICK_BOARD_OPTIONS)[number]>(() => persistedState?.board ?? '全部')
  const [referenceTradeDate, setReferenceTradeDate] = useState(() => persistedState?.referenceTradeDate ?? '')
  const [lookbackPeriods, setLookbackPeriods] = useState(() => persistedState?.lookbackPeriods ?? '1')
  const [scopeWay, setScopeWay] = useState<(typeof STOCK_PICK_SCOPE_OPTIONS)[number]>(() => persistedState?.scopeWay ?? 'LAST')
  const [consecThreshold, setConsecThreshold] = useState(() => persistedState?.consecThreshold ?? '2')
  const [expression, setExpression] = useState(() => persistedState?.expression ?? DEFAULT_EXPRESSION)
  const [rows, setRows] = useState<StockPickRow[]>(() => persistedState?.rows ?? [])
  const [resolvedStartDate, setResolvedStartDate] = useState(() => persistedState?.resolvedStartDate ?? '')
  const [resolvedReferenceTradeDate, setResolvedReferenceTradeDate] = useState(
    () => persistedState?.resolvedReferenceTradeDate ?? '',
  )
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [copyWithSuffix, setCopyWithSuffix] = useState(true)
  const [copySeparator, setCopySeparator] = useState<CopySeparatorOption>(',')
  const [copyNotice, setCopyNotice] = useState('')
  const [copySucceeded, setCopySucceeded] = useState(false)
  const boardOptions = useMemo(
    () => buildBoardFilterOptions(STOCK_PICK_BOARD_OPTIONS, excludeStBoard),
    [excludeStBoard],
  )

  useEffect(() => {
    if (!latestTradeDate) {
      return
    }
    setReferenceTradeDate((current) => current || latestTradeDate)
  }, [latestTradeDate])

  useEffect(() => {
    if (excludeStBoard && isStBoard(board)) {
      setBoard('全部')
    }
  }, [board, excludeStBoard])

  useEffect(() => {
    writeJsonStorage(
      typeof window === 'undefined' ? null : window.sessionStorage,
      EXPRESSION_STOCK_PICK_FILTER_STATE_KEY,
      {
        board,
        referenceTradeDate,
        lookbackPeriods,
        scopeWay,
        consecThreshold,
        expression,
      } satisfies PersistedExpressionStockPickFilterState,
    )
  }, [board, referenceTradeDate, lookbackPeriods, scopeWay, consecThreshold, expression])

  useEffect(() => {
    writeJsonStorage(
      typeof window === 'undefined' ? null : window.sessionStorage,
      EXPRESSION_STOCK_PICK_RESULT_STATE_KEY,
      {
        rows,
        resolvedStartDate,
        resolvedReferenceTradeDate,
      } satisfies PersistedExpressionStockPickResultState,
    )
  }, [rows, resolvedStartDate, resolvedReferenceTradeDate])

  useEffect(() => {
    if (!copySucceeded) {
      return
    }

    const timer = window.setTimeout(() => setCopySucceeded(false), 1400)
    return () => window.clearTimeout(timer)
  }, [copySucceeded])

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
        excludeStBoard: excludeStBoard || undefined,
        referenceTradeDate,
        lookbackPeriods: scopeWay === 'LAST' ? undefined : parsePositiveInt(lookbackPeriods, 1),
        scopeWay,
        expression,
        consecThreshold: scopeWay === 'CONSEC' ? Number(consecThreshold) : undefined,
      })
      setRows(result.rows ?? [])
      setResolvedStartDate(result.resolved_start_date ?? '')
      setResolvedReferenceTradeDate(result.resolved_end_date ?? referenceTradeDate)
    } catch (runError) {
      setRows([])
      setResolvedStartDate('')
      setResolvedReferenceTradeDate('')
      setError(`表达式选股失败: ${String(runError)}`)
    } finally {
      setLoading(false)
    }
  }

  async function onCopyStockCodes() {
    const normalizedCodes = rows
      .map((row) => row.ts_code.trim())
      .filter((value) => value.length > 0)
      .map((value) => {
        if (copyWithSuffix) {
          return value
        }
        const [codeOnly] = value.split('.')
        return codeOnly ?? value
      })

    if (normalizedCodes.length === 0) {
      setCopySucceeded(false)
      setCopyNotice('当前没有可复制的股票代码。')
      return
    }

    const text = normalizedCodes.join(copySeparator)
    try {
      if (typeof navigator !== 'undefined' && navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(text)
      } else if (typeof document !== 'undefined') {
        const textarea = document.createElement('textarea')
        textarea.value = text
        textarea.setAttribute('readonly', 'true')
        textarea.style.position = 'absolute'
        textarea.style.left = '-9999px'
        document.body.appendChild(textarea)
        textarea.select()
        document.execCommand('copy')
        document.body.removeChild(textarea)
      } else {
        throw new Error('当前环境不支持复制')
      }

      setCopySucceeded(true)
      setCopyNotice('')
    } catch (copyError) {
      setCopySucceeded(false)
      setCopyNotice(`复制失败: ${String(copyError)}`)
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
            {boardOptions.map((item) => (
              <option key={item} value={item}>
                {item}
              </option>
            ))}
          </select>
        </label>

        <label className="stock-pick-field">
          <span>参考日</span>
          <select
            value={referenceTradeDate}
            onChange={(event) => setReferenceTradeDate(event.target.value)}
            disabled={optionsLoading}
          >
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

        <label className="stock-pick-field">
          <span>前推周期数</span>
          <input
            type="number"
            min={1}
            step={1}
            value={lookbackPeriods}
            onChange={(event) => setLookbackPeriods(event.target.value)}
            disabled={scopeWay === 'LAST'}
          />
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
        <div className="stock-pick-result-tools">
          {excludeStBoard ? <span>已应用全局条件：排除 ST</span> : null}
          <span>
            共 {rows.length} 只，参考日：{formatDateLabel(resolvedReferenceTradeDate)}
            {resolvedStartDate && resolvedReferenceTradeDate && resolvedStartDate !== resolvedReferenceTradeDate
              ? `，窗口：${formatDateLabel(resolvedStartDate)} ~ ${formatDateLabel(resolvedReferenceTradeDate)}`
              : ''}
          </span>
          <div className="stock-pick-copy-config">
            <div className="stock-pick-copy-setting">
              <span>代码格式</span>
              <div className="stock-pick-copy-toggle" role="group" aria-label="代码格式">
                <button
                  type="button"
                  className={copyWithSuffix ? 'stock-pick-copy-toggle-btn is-active' : 'stock-pick-copy-toggle-btn'}
                  onClick={() => setCopyWithSuffix(true)}
                >
                  带后缀
                </button>
                <button
                  type="button"
                  className={!copyWithSuffix ? 'stock-pick-copy-toggle-btn is-active' : 'stock-pick-copy-toggle-btn'}
                  onClick={() => setCopyWithSuffix(false)}
                >
                  不带后缀
                </button>
              </div>
            </div>
            <label className="stock-pick-copy-setting">
              <span>分隔符</span>
              <select
                className="stock-pick-copy-select"
                value={copySeparator}
                onChange={(event) => setCopySeparator(event.target.value as CopySeparatorOption)}
              >
                {COPY_SEPARATOR_OPTIONS.map((item) => (
                  <option key={item.value} value={item.value}>
                    {item.label}
                  </option>
                ))}
              </select>
            </label>
          </div>
          <button
            type="button"
            className={copySucceeded ? 'stock-pick-chip-btn is-active' : 'stock-pick-chip-btn'}
            onClick={() => void onCopyStockCodes()}
          >
            {copySucceeded ? '已复制' : '复制股票'}
          </button>
        </div>
      </div>
      {copyNotice ? <div className="stock-pick-tip">{copyNotice}</div> : null}
      <StockPickResultTable
        rows={rows}
        tradeDate={resolvedReferenceTradeDate}
        sourcePath={sourcePath}
      />
    </section>
  )
}
