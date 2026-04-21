import { useDeferredValue, useEffect, useMemo, useState } from 'react'
import { ensureManagedSourcePath } from '../../apis/managedSource'
import { listStockLookupRows, type StockLookupRow } from '../../apis/reader'
import {
  getStrategyPaperValidationDefaults,
  runStrategyPaperValidation,
  type StrategyPaperValidationData,
  type StrategyPaperValidationTradeRow,
} from '../../apis/strategyPaperValidation'
import {
  buildStockLookupCandidates,
  findExactStockLookupMatch,
  getLookupDigits,
} from '../../shared/stockLookup'
import { normalizeTsCode } from '../../shared/stockCode'
import './css/StrategyPaperValidationPage.css'

const MAX_STOCK_NAME_CANDIDATES = 8

const INDEX_OPTIONS = [
  { value: '000001.SH', label: '上证指数' },
  { value: '399001.SZ', label: '深证成指' },
  { value: '399006.SZ', label: '创业板指' },
  { value: '000300.SH', label: '沪深300' },
  { value: '000905.SH', label: '中证500' },
  { value: '000852.SH', label: '中证1000' },
  { value: '000688.SH', label: '科创50' },
] as const

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
  return row.status === 'closed' ? '已平仓' : '持仓中'
}

export default function StrategyPaperValidationPage() {
  const [sourcePath, setSourcePath] = useState('')
  const [stockLookupRows, setStockLookupRows] = useState<StockLookupRow[]>([])
  const [initializing, setInitializing] = useState(false)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [result, setResult] = useState<StrategyPaperValidationData | null>(null)

  const [startDateInput, setStartDateInput] = useState('')
  const [endDateInput, setEndDateInput] = useState('')
  const [minListedTradeDays, setMinListedTradeDays] = useState('60')
  const [indexTsCode, setIndexTsCode] = useState<string>(INDEX_OPTIONS[0].value)
  const [buyPriceBasis, setBuyPriceBasis] = useState('open')
  const [slippagePct, setSlippagePct] = useState('0')
  const [testStockInput, setTestStockInput] = useState('')
  const [stockLookupFocused, setStockLookupFocused] = useState(false)
  const [buyExpression, setBuyExpression] = useState('rank <= 100')
  const [sellExpression, setSellExpression] = useState('TIME >= 5 OR RATEH >= 8')
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

  function onSelectStockCandidate(row: StockLookupRow) {
    setStockLookupFocused(false)
    setTestStockInput(row.name || getLookupDigits(row.ts_code))
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
    try {
      const data = await runStrategyPaperValidation({
        sourcePath,
        startDate: normalizedStart,
        endDate: normalizedEnd,
        minListedTradeDays: Math.max(0, Number(minListedTradeDays) || 0),
        indexTsCode: indexTsCode.trim() || undefined,
        testTsCode: resolvedTestTsCode || undefined,
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
  const trades = result?.trades ?? []

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
          <button type="button" className="strategy-paper-validation-primary-btn" onClick={() => void onRun()} disabled={loading || initializing}>
            {loading ? '回放中...' : '开始验证'}
          </button>
        </div>

        {initializing ? <div className="strategy-paper-validation-placeholder">默认参数加载中...</div> : null}
        {error ? <div className="strategy-paper-validation-error">{error}</div> : null}
      </section>

      {summary ? (
        <section className="strategy-paper-validation-card">
          <div className="strategy-paper-validation-summary-grid">
            <div className="strategy-paper-validation-summary-item">
              <span>买入信号数</span>
              <strong>{summary.buy_signal_count}</strong>
            </div>
            <div className="strategy-paper-validation-summary-item">
              <span>已平仓 / 持仓中</span>
              <strong>
                {summary.closed_trade_count} / {summary.open_trade_count}
              </strong>
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
            <div className="strategy-paper-validation-summary-item">
              <span>最好 / 最差</span>
              <strong>
                {formatPercent(summary.best_return_pct)} / {formatPercent(summary.worst_return_pct)}
              </strong>
            </div>
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
          </div>
        </section>
      ) : null}

      {result ? (
        <section className="strategy-paper-validation-card">
          <div className="strategy-paper-validation-table-head">
            <h3>交易明细</h3>
            <span>共 {trades.length} 笔</span>
          </div>

          <div className="strategy-paper-validation-table-wrap">
            <table className="strategy-paper-validation-table">
              <thead>
                <tr>
                  <th>状态</th>
                  <th>代码</th>
                  <th>名称</th>
                  <th>买入日</th>
                  <th>卖出日</th>
                  <th>买入排名</th>
                  <th>持仓天数</th>
                  <th>买入成本</th>
                  <th>卖出价</th>
                  <th>RATEO</th>
                  <th>RATEH</th>
                  <th>收盘收益</th>
                  <th>记录收益</th>
                </tr>
              </thead>
              <tbody>
                {trades.map((row, index) => (
                  <tr key={`${row.ts_code}-${row.buy_date}-${row.sell_date ?? 'open'}-${index}`}>
                    <td>{statusLabel(row)}</td>
                    <td>{row.ts_code}</td>
                    <td>{row.name ?? '--'}</td>
                    <td>{formatDateLabel(row.buy_date)}</td>
                    <td>{formatDateLabel(row.sell_date)}</td>
                    <td>{row.buy_rank ?? '--'}</td>
                    <td>{row.hold_days}</td>
                    <td>{formatNumber(row.buy_cost_price)}</td>
                    <td>{formatNumber(row.sell_price)}</td>
                    <td>{formatPercent(row.open_return_pct)}</td>
                    <td>{formatPercent(row.high_return_pct)}</td>
                    <td>{formatPercent(row.close_return_pct)}</td>
                    <td>{formatPercent(row.realized_return_pct)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}
    </div>
  )
}
