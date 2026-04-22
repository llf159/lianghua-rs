import { memo, useDeferredValue, useEffect, useMemo, useState } from 'react'
import { ensureManagedSourcePath } from '../../apis/managedSource'
import { listStockLookupRows, type StockLookupRow } from '../../apis/reader'
import {
  getStrategyPaperValidationDefaults,
  runStrategyPaperValidation,
  type StrategyPaperValidationData,
  type StrategyPaperValidationTradeRow,
} from '../../apis/strategyPaperValidation'
import {
  TableSortButton,
  getAriaSort,
  type SortDefinition,
  useTableSort,
} from '../../shared/tableSort'
import { readJsonStorage, writeJsonStorage } from '../../shared/storage'
import {
  buildStockLookupCandidates,
  findExactStockLookupMatch,
  getLookupDigits,
} from '../../shared/stockLookup'
import { normalizeTsCode } from '../../shared/stockCode'
import DetailsLink from '../../shared/DetailsLink'
import StrategyPaperValidationTemplateManagerModal, {
  type StrategyPaperValidationTemplate,
} from './components/StrategyPaperValidationTemplateManagerModal'
import './css/StrategyPaperValidationPage.css'

const MAX_STOCK_NAME_CANDIDATES = 8
const STRATEGY_PAPER_VALIDATION_TEMPLATE_STORAGE_KEY =
  'lh_strategy_paper_validation_templates_v1'
const EXTREME_TRADE_LIMIT = 20
const TRADE_PAGE_SIZE_OPTIONS = [
  { value: '100', label: '100 / 页' },
  { value: '200', label: '200 / 页' },
  { value: '500', label: '500 / 页' },
  { value: '1000', label: '1000 / 页' },
  { value: 'all', label: '全部' },
] as const

const INDEX_OPTIONS = [
  { value: '000001.SH', label: '上证指数' },
  { value: '399001.SZ', label: '深证成指' },
  { value: '399006.SZ', label: '创业板指' },
  { value: '000300.SH', label: '沪深300' },
  { value: '000905.SH', label: '中证500' },
  { value: '000852.SH', label: '中证1000' },
  { value: '000688.SH', label: '科创50' },
] as const

const BOARD_OPTIONS = [
  { value: '', label: '全部板块' },
  { value: '主板', label: '主板' },
  { value: '创业/科创', label: '创业/科创' },
  { value: '北交所', label: '北交所' },
  { value: 'ST', label: 'ST' },
  { value: '其他', label: '其他' },
] as const

type TradeStatusFilter = 'all' | 'closed' | 'open'
type TradeDetailModalStatus = Exclude<TradeStatusFilter, 'all'>
type TradePageSize = (typeof TRADE_PAGE_SIZE_OPTIONS)[number]['value']

type TradeSortKey =
  | 'status'
  | 'ts_code'
  | 'name'
  | 'buy_date'
  | 'sell_date'
  | 'buy_rank'
  | 'hold_days'
  | 'buy_cost_price'
  | 'sell_price'
  | 'open_return_pct'
  | 'high_return_pct'
  | 'close_return_pct'
  | 'realized_return_pct'

function normalizeTemplate(input: unknown): StrategyPaperValidationTemplate | null {
  if (!input || typeof input !== 'object') return null
  const item = input as Record<string, unknown>
  if (typeof item.id !== 'string') return null
  if (typeof item.name !== 'string') return null
  if (typeof item.buyExpression !== 'string') return null
  if (typeof item.sellExpression !== 'string') return null
  return {
    id: item.id,
    name: item.name,
    buyExpression: item.buyExpression,
    sellExpression: item.sellExpression,
  }
}

function compactDateToInput(value?: string | null) {
  if (!value || !/^\d{8}$/.test(value)) {
    return ''
  }
  return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`
}

function normalizeDateInput(value: string) {
  return value.replaceAll('-', '').trim()
}

function formatDateLabel(value?: string | null) {
  if (!value || value.length !== 8) {
    return '--'
  }
  return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`
}

function formatNumber(value?: number | null, digits = 2) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return '--'
  }
  return value.toFixed(digits)
}

function formatPercent(value?: number | null, digits = 2) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return '--'
  }
  return `${value.toFixed(digits)}%`
}

function statusLabel(row: StrategyPaperValidationTradeRow) {
  return row.status === 'closed' ? '已平仓' : '未平仓'
}

function getRealizedReturnValue(row: StrategyPaperValidationTradeRow) {
  const value = row.realized_return_pct
  return typeof value === 'number' && Number.isFinite(value) ? value : null
}

function getReturnToneClass(value?: number | null) {
  if (typeof value !== 'number' || !Number.isFinite(value) || value === 0) {
    return ''
  }
  return value > 0
    ? 'strategy-paper-validation-return-positive'
    : 'strategy-paper-validation-return-negative'
}

function renderReturnValue(value?: number | null) {
  const toneClass = getReturnToneClass(value)
  return (
    <span className={['strategy-paper-validation-return-value', toneClass].filter(Boolean).join(' ')}>
      {formatPercent(value)}
    </span>
  )
}

function resolveTradeBoard(tsCode: string, stockName?: string | null) {
  const normalizedName = stockName?.trim().toUpperCase() ?? ''
  if (normalizedName.startsWith('*ST') || normalizedName.startsWith('ST')) {
    return 'ST'
  }

  const normalizedCode = tsCode.trim().toUpperCase()
  if (normalizedCode.endsWith('.BJ')) {
    return '北交所'
  }
  if (
    (normalizedCode.endsWith('.SZ') && normalizedCode.startsWith('30')) ||
    (normalizedCode.endsWith('.SH') && normalizedCode.startsWith('688'))
  ) {
    return '创业/科创'
  }
  if (normalizedCode.endsWith('.SH') || normalizedCode.endsWith('.SZ')) {
    return '主板'
  }
  return '其他'
}

function parseOptionalNumber(value: string) {
  const trimmed = value.trim()
  if (!trimmed) {
    return null
  }

  const parsed = Number(trimmed)
  return Number.isFinite(parsed) ? parsed : null
}

function isWithinDateRange(value: string, start: string, end: string) {
  if (start && value < start) {
    return false
  }
  if (end && value > end) {
    return false
  }
  return true
}

type StrategyPaperValidationTradesTableProps = {
  trades: StrategyPaperValidationTradeRow[]
  sourcePath: string
}

const StrategyPaperValidationTradesTable = memo(
  function StrategyPaperValidationTradesTable({
    trades,
    sourcePath,
  }: StrategyPaperValidationTradesTableProps) {
    const [stockFilter, setStockFilter] = useState('')
    const [boardFilter, setBoardFilter] = useState('')
    const [tradeStatusFilter, setTradeStatusFilter] =
      useState<TradeStatusFilter>('all')
    const [buyDateStartInput, setBuyDateStartInput] = useState('')
    const [buyDateEndInput, setBuyDateEndInput] = useState('')
    const [returnMinInput, setReturnMinInput] = useState('')
    const [returnMaxInput, setReturnMaxInput] = useState('')
    const [buyRankMaxInput, setBuyRankMaxInput] = useState('')
    const [pageSize, setPageSize] = useState<TradePageSize>('100')
    const [currentPage, setCurrentPage] = useState(1)
    const deferredStockFilter = useDeferredValue(stockFilter)

    const tradeSortDefinitions = useMemo(
      () =>
        ({
          status: { value: statusLabel },
          ts_code: { value: (row) => row.ts_code },
          name: { value: (row) => row.name },
          buy_date: { value: (row) => row.buy_date },
          sell_date: { value: (row) => row.sell_date },
          buy_rank: { value: (row) => row.buy_rank },
          hold_days: { value: (row) => row.hold_days },
          buy_cost_price: { value: (row) => row.buy_cost_price },
          sell_price: { value: (row) => row.sell_price },
          open_return_pct: { value: (row) => row.open_return_pct },
          high_return_pct: { value: (row) => row.high_return_pct },
          close_return_pct: { value: (row) => row.close_return_pct },
          realized_return_pct: { value: (row) => row.realized_return_pct },
        }) satisfies Partial<Record<TradeSortKey, SortDefinition<StrategyPaperValidationTradeRow>>>,
      [],
    )

    const filteredTrades = useMemo(() => {
      const normalizedStockFilter = deferredStockFilter.trim().toLowerCase()
      const buyDateStart = normalizeDateInput(buyDateStartInput)
      const buyDateEnd = normalizeDateInput(buyDateEndInput)
      const returnMin = parseOptionalNumber(returnMinInput)
      const returnMax = parseOptionalNumber(returnMaxInput)
      const buyRankMax = parseOptionalNumber(buyRankMaxInput)

      return trades.filter((row) => {
        if (boardFilter && resolveTradeBoard(row.ts_code, row.name) !== boardFilter) {
          return false
        }

        if (tradeStatusFilter !== 'all' && row.status !== tradeStatusFilter) {
          return false
        }

        if (
          normalizedStockFilter &&
          !row.ts_code.toLowerCase().includes(normalizedStockFilter) &&
          !getLookupDigits(row.ts_code).includes(normalizedStockFilter) &&
          !(row.name ?? '').toLowerCase().includes(normalizedStockFilter)
        ) {
          return false
        }

        if (!isWithinDateRange(row.buy_date, buyDateStart, buyDateEnd)) {
          return false
        }

        const realizedReturn = getRealizedReturnValue(row)
        if (returnMin !== null && (realizedReturn === null || realizedReturn < returnMin)) {
          return false
        }
        if (returnMax !== null && (realizedReturn === null || realizedReturn > returnMax)) {
          return false
        }

        if (buyRankMax !== null) {
          const buyRank = row.buy_rank
          if (typeof buyRank !== 'number' || !Number.isFinite(buyRank) || buyRank > buyRankMax) {
            return false
          }
        }

        return true
      })
    }, [
      boardFilter,
      buyDateEndInput,
      buyDateStartInput,
      buyRankMaxInput,
      deferredStockFilter,
      returnMaxInput,
      returnMinInput,
      tradeStatusFilter,
      trades,
    ])

    const {
      sortKey,
      sortDirection,
      sortedRows: sortedTrades,
      toggleSort,
    } = useTableSort<StrategyPaperValidationTradeRow, TradeSortKey>(
      filteredTrades,
      tradeSortDefinitions,
      { key: 'buy_date', direction: 'desc' },
    )

    const rowsPerPage = pageSize === 'all' ? sortedTrades.length || 1 : Number(pageSize)
    const totalPages =
      pageSize === 'all' ? 1 : Math.max(1, Math.ceil(sortedTrades.length / rowsPerPage))
    const safeCurrentPage = Math.min(currentPage, totalPages)
    const visibleTrades = useMemo(() => {
      if (pageSize === 'all') {
        return sortedTrades
      }
      const start = (safeCurrentPage - 1) * rowsPerPage
      return sortedTrades.slice(start, start + rowsPerPage)
    }, [pageSize, rowsPerPage, safeCurrentPage, sortedTrades])

    const navigationItems = useMemo(
      () =>
        sortedTrades.map((row) => ({
          tsCode: row.ts_code,
          tradeDate: row.buy_date,
          sourcePath: sourcePath.trim() || undefined,
          name: row.name ?? undefined,
        })),
      [sortedTrades, sourcePath],
    )

    function resetToFirstPage() {
      setCurrentPage(1)
    }

    function renderSortableHeader(key: TradeSortKey, label: string) {
      const isActive = sortKey === key
      return (
        <th aria-sort={getAriaSort(isActive, sortDirection)}>
          <TableSortButton
            label={label}
            isActive={isActive}
            direction={sortDirection}
            onClick={() => {
              toggleSort(key)
              resetToFirstPage()
            }}
          />
        </th>
      )
    }

    return (
      <section className="strategy-paper-validation-card">
        <div className="strategy-paper-validation-table-head">
          <h3>交易明细</h3>
          <span>
            筛选 {filteredTrades.length} / 共 {trades.length} 笔
          </span>
        </div>

        <div className="strategy-paper-validation-table-filters">
          <label className="strategy-paper-validation-field">
            <span>股票</span>
            <input
              value={stockFilter}
              onChange={(event) => {
                setStockFilter(event.target.value)
                resetToFirstPage()
              }}
              placeholder="代码或名称"
            />
          </label>
          <label className="strategy-paper-validation-field">
            <span>状态</span>
            <select
              value={tradeStatusFilter}
              onChange={(event) => {
                setTradeStatusFilter(event.target.value as TradeStatusFilter)
                resetToFirstPage()
              }}
            >
              <option value="all">全部</option>
              <option value="closed">已平仓</option>
              <option value="open">未平仓</option>
            </select>
          </label>
          <label className="strategy-paper-validation-field">
            <span>板块</span>
            <select
              value={boardFilter}
              onChange={(event) => {
                setBoardFilter(event.target.value)
                resetToFirstPage()
              }}
            >
              {BOARD_OPTIONS.map((item) => (
                <option key={item.value || 'all'} value={item.value}>
                  {item.label}
                </option>
              ))}
            </select>
          </label>
          <label className="strategy-paper-validation-field">
            <span>买入日起</span>
            <input
              type="date"
              value={buyDateStartInput}
              onChange={(event) => {
                setBuyDateStartInput(event.target.value)
                resetToFirstPage()
              }}
            />
          </label>
          <label className="strategy-paper-validation-field">
            <span>买入日止</span>
            <input
              type="date"
              value={buyDateEndInput}
              onChange={(event) => {
                setBuyDateEndInput(event.target.value)
                resetToFirstPage()
              }}
            />
          </label>
          <label className="strategy-paper-validation-field">
            <span>记录收益下限(%)</span>
            <input
              type="number"
              step="0.01"
              value={returnMinInput}
              onChange={(event) => {
                setReturnMinInput(event.target.value)
                resetToFirstPage()
              }}
            />
          </label>
          <label className="strategy-paper-validation-field">
            <span>记录收益上限(%)</span>
            <input
              type="number"
              step="0.01"
              value={returnMaxInput}
              onChange={(event) => {
                setReturnMaxInput(event.target.value)
                resetToFirstPage()
              }}
            />
          </label>
          <label className="strategy-paper-validation-field">
            <span>买入排名不高于</span>
            <input
              type="number"
              min="1"
              step="1"
              value={buyRankMaxInput}
              onChange={(event) => {
                setBuyRankMaxInput(event.target.value)
                resetToFirstPage()
              }}
            />
          </label>
          <label className="strategy-paper-validation-field">
            <span>每页显示</span>
            <select
              value={pageSize}
              onChange={(event) => {
                setPageSize(event.target.value as TradePageSize)
                resetToFirstPage()
              }}
            >
              {TRADE_PAGE_SIZE_OPTIONS.map((item) => (
                <option key={item.value} value={item.value}>
                  {item.label}
                </option>
              ))}
            </select>
          </label>
        </div>

        <div className="strategy-paper-validation-table-wrap">
          <table className="strategy-paper-validation-table">
            <thead>
              <tr>
                {renderSortableHeader('status', '状态')}
                {renderSortableHeader('ts_code', '代码')}
                {renderSortableHeader('name', '名称')}
                {renderSortableHeader('buy_date', '买入日')}
                {renderSortableHeader('sell_date', '卖出日')}
                {renderSortableHeader('buy_rank', '买入排名')}
                {renderSortableHeader('hold_days', '持仓天数')}
                {renderSortableHeader('buy_cost_price', '买入成本')}
                {renderSortableHeader('sell_price', '卖出价')}
                {renderSortableHeader('realized_return_pct', '记录收益')}
              </tr>
            </thead>
            <tbody>
              {visibleTrades.length === 0 ? (
                <tr>
                  <td colSpan={10}>没有匹配当前筛选条件的交易。</td>
                </tr>
              ) : (
                visibleTrades.map((row, index) => (
                  <tr key={`${row.ts_code}-${row.buy_date}-${row.sell_date ?? 'open'}-${index}`}>
                    <td>{statusLabel(row)}</td>
                    <td>{row.ts_code}</td>
                    <td>
                      <DetailsLink
                        className="strategy-paper-validation-stock-link"
                        tsCode={row.ts_code}
                        tradeDate={row.buy_date}
                        sourcePath={sourcePath.trim() || undefined}
                        title={`查看 ${row.name ?? row.ts_code} 买入日详情`}
                        navigationItems={navigationItems}
                      >
                        {row.name ?? row.ts_code}
                      </DetailsLink>
                    </td>
                    <td>{formatDateLabel(row.buy_date)}</td>
                    <td>{formatDateLabel(row.sell_date)}</td>
                    <td>{row.buy_rank ?? '--'}</td>
                    <td>{row.hold_days}</td>
                    <td>{formatNumber(row.buy_cost_price)}</td>
                    <td>{formatNumber(row.sell_price)}</td>
                    <td>{renderReturnValue(row.realized_return_pct)}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        <div className="strategy-paper-validation-pagination">
          <span>
            第 {safeCurrentPage} / {totalPages} 页，当前显示 {visibleTrades.length} 笔
          </span>
          <div className="strategy-paper-validation-pagination-actions">
            <button
              type="button"
              className="strategy-paper-validation-secondary-btn"
              onClick={() => setCurrentPage(1)}
              disabled={safeCurrentPage <= 1}
            >
              首页
            </button>
            <button
              type="button"
              className="strategy-paper-validation-secondary-btn"
              onClick={() => setCurrentPage((page) => Math.max(1, page - 1))}
              disabled={safeCurrentPage <= 1}
            >
              上一页
            </button>
            <button
              type="button"
              className="strategy-paper-validation-secondary-btn"
              onClick={() => setCurrentPage((page) => Math.min(totalPages, page + 1))}
              disabled={safeCurrentPage >= totalPages}
            >
              下一页
            </button>
            <button
              type="button"
              className="strategy-paper-validation-secondary-btn"
              onClick={() => setCurrentPage(totalPages)}
              disabled={safeCurrentPage >= totalPages}
            >
              末页
            </button>
          </div>
        </div>
      </section>
    )
  },
)

type StrategyPaperValidationStatusTradeModalProps = {
  status: TradeDetailModalStatus
  rows: StrategyPaperValidationTradeRow[]
  sourcePath: string
  onClose: () => void
}

const StrategyPaperValidationStatusTradeModal = memo(
  function StrategyPaperValidationStatusTradeModal({
    status,
    rows,
    sourcePath,
    onClose,
  }: StrategyPaperValidationStatusTradeModalProps) {
    const title = status === 'closed' ? '已平仓交易明细' : '未平仓交易明细'
    const [boardFilter, setBoardFilter] = useState('')
    const tradeSortDefinitions = useMemo(
      () =>
        ({
          ts_code: { value: (row) => row.ts_code },
          name: { value: (row) => row.name },
          buy_date: { value: (row) => row.buy_date },
          sell_date: { value: (row) => row.sell_date },
          buy_rank: { value: (row) => row.buy_rank },
          hold_days: { value: (row) => row.hold_days },
          buy_cost_price: { value: (row) => row.buy_cost_price },
          sell_price: { value: (row) => row.sell_price },
          open_return_pct: { value: (row) => row.open_return_pct },
          high_return_pct: { value: (row) => row.high_return_pct },
          close_return_pct: { value: (row) => row.close_return_pct },
          realized_return_pct: { value: (row) => row.realized_return_pct },
        }) satisfies Partial<Record<TradeSortKey, SortDefinition<StrategyPaperValidationTradeRow>>>,
      [],
    )
    const filteredRows = useMemo(
      () =>
        rows.filter((row) => {
          if (!boardFilter) {
            return true
          }
          return resolveTradeBoard(row.ts_code, row.name) === boardFilter
        }),
      [boardFilter, rows],
    )
    const initialSortKey: TradeSortKey = status === 'closed' ? 'sell_date' : 'buy_date'
    const {
      sortKey,
      sortDirection,
      sortedRows,
      toggleSort,
    } = useTableSort<StrategyPaperValidationTradeRow, TradeSortKey>(
      filteredRows,
      tradeSortDefinitions,
      { key: initialSortKey, direction: 'desc' },
    )
    const navigationItems = useMemo(
      () =>
        sortedRows.map((row) => ({
          tsCode: row.ts_code,
          tradeDate: row.buy_date,
          sourcePath: sourcePath.trim() || undefined,
          name: row.name ?? undefined,
        })),
      [sortedRows, sourcePath],
    )

    function renderSortableHeader(key: TradeSortKey, label: string) {
      const isActive = sortKey === key
      return (
        <th aria-sort={getAriaSort(isActive, sortDirection)}>
          <TableSortButton
            label={label}
            isActive={isActive}
            direction={sortDirection}
            onClick={() => toggleSort(key)}
          />
        </th>
      )
    }

    return (
      <div
        className="strategy-paper-validation-modal-backdrop"
        role="presentation"
        onClick={(event) => {
          if (event.target === event.currentTarget) {
            onClose()
          }
        }}
      >
        <div className="strategy-paper-validation-modal" role="dialog" aria-modal="true">
          <div className="strategy-paper-validation-table-head">
            <div>
              <h3>{title}</h3>
              <span>
                筛选 {filteredRows.length} / 共 {rows.length} 笔
              </span>
            </div>
            <button
              type="button"
              className="strategy-paper-validation-secondary-btn"
              onClick={onClose}
            >
              关闭
            </button>
          </div>
          <div className="strategy-paper-validation-table-filters">
            <label className="strategy-paper-validation-field">
              <span>板块</span>
              <select value={boardFilter} onChange={(event) => setBoardFilter(event.target.value)}>
                {BOARD_OPTIONS.map((item) => (
                  <option key={item.value || 'all'} value={item.value}>
                    {item.label}
                  </option>
                ))}
              </select>
            </label>
          </div>
          <div className="strategy-paper-validation-table-wrap strategy-paper-validation-modal-table-wrap">
            <table className="strategy-paper-validation-table strategy-paper-validation-detail-table">
              <thead>
                <tr>
                  {renderSortableHeader('ts_code', '代码')}
                  {renderSortableHeader('name', '名称')}
                  {renderSortableHeader('buy_date', '买入日')}
                  {renderSortableHeader('sell_date', '卖出日')}
                  {renderSortableHeader('buy_rank', '买入排名')}
                  {renderSortableHeader('hold_days', '持仓天数')}
                  {renderSortableHeader('buy_cost_price', '买入成本')}
                  {renderSortableHeader('sell_price', '卖出价')}
                  {renderSortableHeader('open_return_pct', 'RATEO')}
                  {renderSortableHeader('high_return_pct', 'RATEH')}
                  {renderSortableHeader('close_return_pct', '收盘收益')}
                  {renderSortableHeader('realized_return_pct', '记录收益')}
                </tr>
              </thead>
              <tbody>
                {sortedRows.length === 0 ? (
                  <tr>
                    <td colSpan={12}>没有匹配当前筛选条件的交易。</td>
                  </tr>
                ) : (
                  sortedRows.map((row, index) => (
                    <tr key={`${status}-${row.ts_code}-${row.buy_date}-${row.sell_date ?? 'open'}-${index}`}>
                      <td>{row.ts_code}</td>
                      <td>
                        <DetailsLink
                          className="strategy-paper-validation-stock-link"
                          tsCode={row.ts_code}
                          tradeDate={row.buy_date}
                          sourcePath={sourcePath.trim() || undefined}
                          title={`查看 ${row.name ?? row.ts_code} 买入日详情`}
                          navigationItems={navigationItems}
                        >
                          {row.name ?? row.ts_code}
                        </DetailsLink>
                      </td>
                      <td>{formatDateLabel(row.buy_date)}</td>
                      <td>{formatDateLabel(row.sell_date)}</td>
                      <td>{row.buy_rank ?? '--'}</td>
                      <td>{row.hold_days}</td>
                      <td>{formatNumber(row.buy_cost_price)}</td>
                      <td>{formatNumber(row.sell_price)}</td>
                      <td>{renderReturnValue(row.open_return_pct)}</td>
                      <td>{renderReturnValue(row.high_return_pct)}</td>
                      <td>{renderReturnValue(row.close_return_pct)}</td>
                      <td>{renderReturnValue(row.realized_return_pct)}</td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    )
  },
)

export default function StrategyPaperValidationPage() {
  const persistedTemplates = useMemo(() => {
    const parsed = readJsonStorage<unknown>(
      typeof window === 'undefined' ? null : window.localStorage,
      STRATEGY_PAPER_VALIDATION_TEMPLATE_STORAGE_KEY,
    )
    if (!Array.isArray(parsed)) {
      return []
    }
    return parsed
      .map(normalizeTemplate)
      .filter((item): item is StrategyPaperValidationTemplate => item !== null)
  }, [])

  const [sourcePath, setSourcePath] = useState('')
  const [stockLookupRows, setStockLookupRows] = useState<StockLookupRow[]>([])
  const [initializing, setInitializing] = useState(false)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [notice, setNotice] = useState('')
  const [result, setResult] = useState<StrategyPaperValidationData | null>(null)

  const [startDateInput, setStartDateInput] = useState('')
  const [endDateInput, setEndDateInput] = useState('')
  const [minListedTradeDays, setMinListedTradeDays] = useState('60')
  const [indexTsCode, setIndexTsCode] = useState<string>(INDEX_OPTIONS[0].value)
  const [boardFilter, setBoardFilter] = useState('')
  const [buyPriceBasis, setBuyPriceBasis] = useState('open')
  const [slippagePct, setSlippagePct] = useState('0')
  const [testStockInput, setTestStockInput] = useState('')
  const [stockLookupFocused, setStockLookupFocused] = useState(false)
  const [buyExpression, setBuyExpression] = useState('RANK <= 100')
  const [sellExpression, setSellExpression] = useState('TIME >= 5 OR RATEH >= 8')
  const [templates, setTemplates] = useState<StrategyPaperValidationTemplate[]>(
    () => persistedTemplates,
  )
  const [selectedTemplateId, setSelectedTemplateId] = useState('')
  const [templateModalOpen, setTemplateModalOpen] = useState(false)
  const [extremeModalOpen, setExtremeModalOpen] = useState(false)
  const [tradeDetailModalStatus, setTradeDetailModalStatus] =
    useState<TradeDetailModalStatus | null>(null)
  const deferredTestStockInput = useDeferredValue(testStockInput)
  const stockNameCandidates = useMemo(
    () =>
      buildStockLookupCandidates(
        stockLookupRows,
        deferredTestStockInput,
        MAX_STOCK_NAME_CANDIDATES,
      ),
    [deferredTestStockInput, stockLookupRows],
  )
  const exactStockLookupMatch = useMemo(
    () => findExactStockLookupMatch(stockLookupRows, testStockInput),
    [stockLookupRows, testStockInput],
  )
  const resolvedTestTsCode =
    exactStockLookupMatch?.ts_code ?? normalizeTsCode(testStockInput) ?? ''
  const showStockNameCandidates =
    stockLookupFocused &&
    testStockInput.trim() !== '' &&
    stockNameCandidates.length > 0

  useEffect(() => {
    let cancelled = false

    const init = async () => {
      setInitializing(true)
      setError('')
      try {
        const resolved = await ensureManagedSourcePath()
        if (cancelled) {
          return
        }
        setSourcePath(resolved)

        const defaults = await getStrategyPaperValidationDefaults(resolved)
        if (cancelled) {
          return
        }

        setStartDateInput(compactDateToInput(defaults.start_date))
        setEndDateInput(compactDateToInput(defaults.end_date))
        setMinListedTradeDays(String(defaults.min_listed_trade_days))
        setIndexTsCode(defaults.index_ts_code || INDEX_OPTIONS[0].value)
        setBuyPriceBasis(defaults.buy_price_basis || 'open')
        setSlippagePct(String(defaults.slippage_pct ?? 0))
      } catch (initError) {
        if (!cancelled) {
          setError(`读取默认参数失败: ${String(initError)}`)
        }
      } finally {
        if (!cancelled) {
          setInitializing(false)
        }
      }
    }

    void init()
    return () => {
      cancelled = true
    }
  }, [])

  useEffect(() => {
    if (!sourcePath.trim()) {
      setStockLookupRows([])
      return
    }

    let cancelled = false
    const loadStockLookup = async () => {
      try {
        const rows = await listStockLookupRows(sourcePath)
        if (!cancelled) {
          setStockLookupRows(rows)
        }
      } catch {
        if (!cancelled) {
          setStockLookupRows([])
        }
      }
    }

    void loadStockLookup()
    return () => {
      cancelled = true
    }
  }, [sourcePath])

  useEffect(() => {
    writeJsonStorage(
      typeof window === 'undefined' ? null : window.localStorage,
      STRATEGY_PAPER_VALIDATION_TEMPLATE_STORAGE_KEY,
      templates,
    )
  }, [templates])

  useEffect(() => {
    if (
      selectedTemplateId !== '' &&
      !templates.some((item) => item.id === selectedTemplateId)
    ) {
      setSelectedTemplateId('')
    }
  }, [selectedTemplateId, templates])

  function onSelectStockCandidate(row: StockLookupRow) {
    setStockLookupFocused(false)
    setTestStockInput(row.name || getLookupDigits(row.ts_code))
  }

  function onApplyTemplate() {
    const template = templates.find((item) => item.id === selectedTemplateId)
    if (!template) {
      setError('请先选择一个表达式模板。')
      setNotice('')
      return
    }

    setBuyExpression(template.buyExpression)
    setSellExpression(template.sellExpression)
    setError('')
    setNotice(`已套用模板：${template.name}`)
  }

  function onTemplateRemoved(templateId: string) {
    if (selectedTemplateId === templateId) {
      setSelectedTemplateId('')
    }
  }

  async function onRun() {
    const normalizedStart = normalizeDateInput(startDateInput)
    const normalizedEnd = normalizeDateInput(endDateInput)

    if (!sourcePath.trim()) {
      setError('当前数据目录不可用。')
      return
    }
    if (!normalizedStart || !normalizedEnd) {
      setError('请填写开始和结束日期。')
      return
    }
    if (normalizedStart > normalizedEnd) {
      setError('开始日期不能晚于结束日期。')
      return
    }
    if (!buyExpression.trim()) {
      setError('买点方程不能为空。')
      return
    }
    if (!sellExpression.trim()) {
      setError('卖点方程不能为空。')
      return
    }
    if (testStockInput.trim() !== '' && !resolvedTestTsCode) {
      setError('测试股票请输入 6 位代码，或从候选中选择唯一股票。')
      return
    }

    setLoading(true)
    setError('')
    setNotice('')
    try {
      const data = await runStrategyPaperValidation({
        sourcePath,
        startDate: normalizedStart,
        endDate: normalizedEnd,
        minListedTradeDays: Math.max(0, Number(minListedTradeDays) || 0),
        indexTsCode: indexTsCode.trim() || undefined,
        testTsCode: resolvedTestTsCode || undefined,
        board: boardFilter || undefined,
        buyPriceBasis,
        slippagePct: Number(slippagePct) || 0,
        buyExpression,
        sellExpression,
      })
      setResult(data)
      setStartDateInput(compactDateToInput(data.start_date))
      setEndDateInput(compactDateToInput(data.end_date))
      setMinListedTradeDays(String(data.min_listed_trade_days))
      setIndexTsCode(data.index_ts_code)
      setBoardFilter(data.resolved_board ?? '')
      setBuyPriceBasis(data.buy_price_basis)
      setSlippagePct(String(data.slippage_pct))
    } catch (runError) {
      setResult(null)
      setError(`执行策略模拟盘验证失败: ${String(runError)}`)
    } finally {
      setLoading(false)
    }
  }

  const summary = result?.summary
  const trades = useMemo(() => result?.trades ?? [], [result])
  const selectedTemplate = templates.find((item) => item.id === selectedTemplateId)
  const closedTrades = useMemo(
    () => trades.filter((row) => row.status === 'closed'),
    [trades],
  )
  const openTrades = useMemo(
    () => trades.filter((row) => row.status === 'open'),
    [trades],
  )
  const tradeDetailModalRows =
    tradeDetailModalStatus === 'closed'
      ? closedTrades
      : tradeDetailModalStatus === 'open'
        ? openTrades
        : []

  const closedReturnTrades = useMemo(
    () =>
      closedTrades.filter((row) => getRealizedReturnValue(row) !== null),
    [closedTrades],
  )

  const bestTrades = useMemo(
    () =>
      [...closedReturnTrades]
        .sort(
          (left, right) =>
            (getRealizedReturnValue(right) ?? Number.NEGATIVE_INFINITY) -
            (getRealizedReturnValue(left) ?? Number.NEGATIVE_INFINITY),
        )
        .slice(0, EXTREME_TRADE_LIMIT),
    [closedReturnTrades],
  )

  const worstTrades = useMemo(
    () =>
      [...closedReturnTrades]
        .sort(
          (left, right) =>
            (getRealizedReturnValue(left) ?? Number.POSITIVE_INFINITY) -
            (getRealizedReturnValue(right) ?? Number.POSITIVE_INFINITY),
        )
        .slice(0, EXTREME_TRADE_LIMIT),
    [closedReturnTrades],
  )

  const hasExtremeTrades = bestTrades.length > 0 || worstTrades.length > 0

  function renderExtremeTradeTable(
    title: string,
    rows: StrategyPaperValidationTradeRow[],
  ) {
    const navigationItems = rows.map((row) => ({
      tsCode: row.ts_code,
      tradeDate: row.buy_date,
      sourcePath: sourcePath.trim() || undefined,
      name: row.name ?? undefined,
    }))

    return (
      <section className="strategy-paper-validation-extreme-panel">
        <div className="strategy-paper-validation-table-head">
          <h4>{title}</h4>
          <span>{rows.length} 条</span>
        </div>
        <div className="strategy-paper-validation-table-wrap strategy-paper-validation-extreme-table-wrap">
          <table className="strategy-paper-validation-table strategy-paper-validation-extreme-table">
            <thead>
              <tr>
                <th>代码</th>
                <th>名称</th>
                <th>买入日</th>
                <th>卖出日</th>
                <th>持仓天数</th>
                <th>买入成本</th>
                <th>卖出价</th>
                <th>记录收益</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row, index) => (
                <tr key={`${title}-${row.ts_code}-${row.buy_date}-${row.sell_date ?? 'open'}-${index}`}>
                  <td>{row.ts_code}</td>
                  <td>
                    <DetailsLink
                      className="strategy-paper-validation-stock-link"
                      tsCode={row.ts_code}
                      tradeDate={row.buy_date}
                      sourcePath={sourcePath.trim() || undefined}
                      title={`查看 ${row.name ?? row.ts_code} 买入日详情`}
                      navigationItems={navigationItems}
                    >
                      {row.name ?? row.ts_code}
                    </DetailsLink>
                  </td>
                  <td>{formatDateLabel(row.buy_date)}</td>
                  <td>{formatDateLabel(row.sell_date)}</td>
                  <td>{row.hold_days}</td>
                  <td>{formatNumber(row.buy_cost_price)}</td>
                  <td>{formatNumber(row.sell_price)}</td>
                  <td>{renderReturnValue(row.realized_return_pct)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    )
  }

  return (
    <div className="strategy-paper-validation-page">
      <section className="strategy-paper-validation-card">
        <div className="strategy-paper-validation-head">
          <div>
            <h2 className="strategy-paper-validation-title">策略模拟盘验证</h2>
            <p className="strategy-paper-validation-note">
              买点先按截面扫描并入持仓，再对当前持仓扫描卖点。当前版本卖出表现按当日收盘价记账，`RATEO / RATEH`
              作为卖点方程辅助字段一起回传。测试股票留空时按默认模式扫描全市场，填写后只验证这一只股票。
            </p>
          </div>
        </div>

        <div className="strategy-paper-validation-form-grid">
          <label className="strategy-paper-validation-field strategy-paper-validation-field-span-full">
            <span>测试股票，预览代码：{resolvedTestTsCode || '--'}</span>
            <div className="details-autocomplete">
              <input
                type="text"
                value={testStockInput}
                onChange={(event) => setTestStockInput(event.target.value)}
                onFocus={() => setStockLookupFocused(true)}
                onBlur={() => setStockLookupFocused(false)}
                onKeyDown={(event) => {
                  if (event.key === 'Enter' && stockNameCandidates.length > 0) {
                    event.preventDefault()
                    onSelectStockCandidate(stockNameCandidates[0])
                  }
                }}
                placeholder="留空为默认模式；输入股票名称、代码或拼音首字母可切到单票验证"
              />
              {showStockNameCandidates ? (
                <div className="details-autocomplete-menu">
                  {stockNameCandidates.map((row) => {
                    const code = getLookupDigits(row.ts_code)
                    return (
                      <button
                        className="details-autocomplete-option"
                        key={row.ts_code}
                        type="button"
                        onMouseDown={(event) => {
                          event.preventDefault()
                          onSelectStockCandidate(row)
                        }}
                      >
                        <strong>{row.name}</strong>
                        <span>{code || row.ts_code}</span>
                      </button>
                    )
                  })}
                </div>
              ) : null}
            </div>
          </label>
          <label className="strategy-paper-validation-field">
            <span>开始日期</span>
            <input type="date" value={startDateInput} onChange={(event) => setStartDateInput(event.target.value)} />
          </label>
          <label className="strategy-paper-validation-field">
            <span>结束日期</span>
            <input type="date" value={endDateInput} onChange={(event) => setEndDateInput(event.target.value)} />
          </label>
          <label className="strategy-paper-validation-field">
            <span>最少上市交易日</span>
            <input
              type="number"
              min="0"
              step="1"
              value={minListedTradeDays}
              onChange={(event) => setMinListedTradeDays(event.target.value)}
            />
          </label>
          <label className="strategy-paper-validation-field">
            <span>对比指数</span>
            <select value={indexTsCode} onChange={(event) => setIndexTsCode(event.target.value)}>
              {INDEX_OPTIONS.map((item) => (
                <option key={item.value} value={item.value}>
                  {item.label}
                </option>
              ))}
            </select>
          </label>
          <label className="strategy-paper-validation-field">
            <span>板块筛选</span>
            <select value={boardFilter} onChange={(event) => setBoardFilter(event.target.value)}>
              {BOARD_OPTIONS.map((item) => (
                <option key={item.value || 'all'} value={item.value}>
                  {item.label}
                </option>
              ))}
            </select>
          </label>
          <label className="strategy-paper-validation-field">
            <span>买点基准</span>
            <select value={buyPriceBasis} onChange={(event) => setBuyPriceBasis(event.target.value)}>
              <option value="open">开盘价</option>
              <option value="close">收盘价</option>
            </select>
          </label>
          <label className="strategy-paper-validation-field">
            <span>滑点系数(%)</span>
            <input type="number" step="0.01" value={slippagePct} onChange={(event) => setSlippagePct(event.target.value)} />
          </label>
          <label className="strategy-paper-validation-field strategy-paper-validation-field-span-full">
            <span>买点方程</span>
            <textarea value={buyExpression} onChange={(event) => setBuyExpression(event.target.value)} rows={5} />
          </label>
          <label className="strategy-paper-validation-field strategy-paper-validation-field-span-full">
            <span>卖点方程</span>
            <textarea value={sellExpression} onChange={(event) => setSellExpression(event.target.value)} rows={5} />
          </label>
        </div>

        <div className="strategy-paper-validation-actions">
          <button
            type="button"
            className="strategy-paper-validation-secondary-btn"
            onClick={() => setTemplateModalOpen(true)}
            disabled={loading || initializing}
          >
            模板管理
          </button>
          <label className="strategy-paper-validation-template-select">
            <span>表达式模板</span>
            <select
              value={selectedTemplateId}
              onChange={(event) => setSelectedTemplateId(event.target.value)}
              disabled={loading || initializing || templates.length === 0}
            >
              <option value="">未选择</option>
              {templates.map((template) => (
                <option key={template.id} value={template.id}>
                  {template.name}
                </option>
              ))}
            </select>
          </label>
          <button
            type="button"
            className="strategy-paper-validation-secondary-btn"
            onClick={onApplyTemplate}
            disabled={loading || initializing || !selectedTemplate}
          >
            套用模板
          </button>
          <button type="button" className="strategy-paper-validation-primary-btn" onClick={() => void onRun()} disabled={loading || initializing}>
            {loading ? '回放中...' : '开始验证'}
          </button>
        </div>

        {initializing ? <div className="strategy-paper-validation-placeholder">默认参数加载中...</div> : null}
        {notice ? <div className="strategy-paper-validation-notice">{notice}</div> : null}
        {error ? <div className="strategy-paper-validation-error">{error}</div> : null}
      </section>

      {summary ? (
        <section className="strategy-paper-validation-card">
          <div className="strategy-paper-validation-summary-grid">
            <div className="strategy-paper-validation-summary-item">
              <span>买入信号数</span>
              <strong>{summary.buy_signal_count}</strong>
            </div>
            <div className="strategy-paper-validation-summary-item strategy-paper-validation-summary-trade-actions">
              <span>交易状态明细</span>
              <div className="strategy-paper-validation-summary-action-row">
                <button
                  type="button"
                  className="strategy-paper-validation-summary-mini-btn"
                  onClick={() => setTradeDetailModalStatus('closed')}
                  disabled={summary.closed_trade_count === 0}
                >
                  <span>已平仓</span>
                  <strong>{summary.closed_trade_count}</strong>
                </button>
                <button
                  type="button"
                  className="strategy-paper-validation-summary-mini-btn"
                  onClick={() => setTradeDetailModalStatus('open')}
                  disabled={summary.open_trade_count === 0}
                >
                  <span>未平仓</span>
                  <strong>{summary.open_trade_count}</strong>
                </button>
              </div>
            </div>
            <div className="strategy-paper-validation-summary-item">
              <span>胜率</span>
              <strong>
                {formatPercent(
                  summary.win_rate === null || summary.win_rate === undefined
                    ? null
                    : summary.win_rate * 100,
                )}
              </strong>
            </div>
            <div className="strategy-paper-validation-summary-item">
              <span>平均收益</span>
              <strong>{formatPercent(summary.avg_return_pct)}</strong>
            </div>
            <div className="strategy-paper-validation-summary-item">
              <span>平均持仓天数</span>
              <strong>{formatNumber(summary.avg_hold_days, 1)}</strong>
            </div>
            <button
              type="button"
              className="strategy-paper-validation-summary-item strategy-paper-validation-summary-button"
              onClick={() => setExtremeModalOpen(true)}
              disabled={!hasExtremeTrades}
            >
              <span>最好 / 最差</span>
              <strong>
                {formatPercent(summary.best_return_pct)} / {formatPercent(summary.worst_return_pct)}
              </strong>
            </button>
          </div>

          <div className="strategy-paper-validation-run-meta">
            <span>
              区间 {formatDateLabel(result?.start_date)} ~ {formatDateLabel(result?.end_date)}
            </span>
            <span>
              模式{' '}
              {result?.test_ts_code
                ? `单票验证 ${result.test_stock_name || result.test_ts_code}`
                : '默认全市场'}
            </span>
            <span>买点基准 {result?.buy_price_basis === 'open' ? '开盘价' : '收盘价'}</span>
            <span>滑点 {formatNumber(result?.slippage_pct, 2)}%</span>
            <span>对比指数 {result?.index_ts_code || '--'}</span>
            <span>板块 {result?.resolved_board || '全部板块'}</span>
          </div>
        </section>
      ) : null}

      {result ? (
        <StrategyPaperValidationTradesTable
          trades={trades}
          sourcePath={sourcePath}
        />
      ) : null}

      {extremeModalOpen ? (
        <div
          className="strategy-paper-validation-modal-backdrop"
          role="presentation"
          onClick={(event) => {
            if (event.target === event.currentTarget) {
              setExtremeModalOpen(false)
            }
          }}
        >
          <div className="strategy-paper-validation-modal" role="dialog" aria-modal="true">
            <div className="strategy-paper-validation-table-head">
              <h3>最好 / 最差交易</h3>
              <button
                type="button"
                className="strategy-paper-validation-secondary-btn"
                onClick={() => setExtremeModalOpen(false)}
              >
                关闭
              </button>
            </div>
            <div className="strategy-paper-validation-extreme-grid">
              {renderExtremeTradeTable('最好 20 条', bestTrades)}
              {renderExtremeTradeTable('最差 20 条', worstTrades)}
            </div>
          </div>
        </div>
      ) : null}

      {tradeDetailModalStatus ? (
        <StrategyPaperValidationStatusTradeModal
          status={tradeDetailModalStatus}
          rows={tradeDetailModalRows}
          sourcePath={sourcePath}
          onClose={() => setTradeDetailModalStatus(null)}
        />
      ) : null}

      <StrategyPaperValidationTemplateManagerModal
        open={templateModalOpen}
        templates={templates}
        onChangeTemplates={setTemplates}
        onTemplateRemoved={onTemplateRemoved}
        onClose={() => setTemplateModalOpen(false)}
      />
    </div>
  )
}
