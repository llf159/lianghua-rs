import { useDeferredValue, useEffect, useMemo, useState } from 'react'
import { ensureManagedSourcePath } from '../../apis/managedSource'
import { listRankTradeDates, listStockLookupRows, type StockLookupRow } from '../../apis/reader'
import {
  getStrategyTriggerSimilarityPage,
  type StrategyTriggerSimilarityPageData,
} from '../../apis/strategyTriggerSimilarity'
import {
  buildStockLookupCandidates,
  findExactStockLookupMatch,
  getLookupDigits,
} from '../../shared/stockLookup'
import DetailsLink from '../../shared/DetailsLink'
import type { DetailsNavigationItem } from '../../shared/detailsLinkState'
import {
  formatConceptText,
  useConceptExclusions,
} from '../../shared/conceptExclusions'
import { sanitizeCodeInput, splitTsCode, stdTsCode } from '../../shared/stockCode'
import { normalizeTradeDates, pickDateValue } from '../../shared/tradeDate'
import './css/StrategyTriggerSimilarityPage.css'

const MAX_STOCK_NAME_CANDIDATES = 12
const DEFAULT_WINDOW_TRADE_DAYS = '20'
const DEFAULT_MAX_GAP_TRADE_DAYS = '5'
const DEFAULT_LIMIT = '30'

function parsePositiveInt(value: string, fallback: number) {
  const parsed = Number.parseInt(value.trim(), 10)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback
}

function parseNonNegativeInt(value: string, fallback: number) {
  const parsed = Number.parseInt(value.trim(), 10)
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback
}

function formatNumber(value: number | null | undefined, digits = 1) {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return '--'
  }
  return value.toFixed(digits)
}

function displayStockName(row: { tsCode: string; name?: string | null }) {
  return row.name?.trim() || splitTsCode(row.tsCode)
}

function displayText(value: string | null | undefined) {
  const text = value?.trim()
  return text ? text : '--'
}

export default function StrategyTriggerSimilarityPage() {
  const { excludedConcepts } = useConceptExclusions()
  const [sourcePath, setSourcePath] = useState('')
  const [tradeDateOptions, setTradeDateOptions] = useState<string[]>([])
  const [tradeDateInput, setTradeDateInput] = useState('')
  const [lookupRows, setLookupRows] = useState<StockLookupRow[]>([])
  const [lookupInput, setLookupInput] = useState('')
  const [lookupFocused, setLookupFocused] = useState(false)
  const [windowTradeDaysInput, setWindowTradeDaysInput] = useState(DEFAULT_WINDOW_TRADE_DAYS)
  const [maxGapTradeDaysInput, setMaxGapTradeDaysInput] = useState(DEFAULT_MAX_GAP_TRADE_DAYS)
  const [limitInput, setLimitInput] = useState(DEFAULT_LIMIT)
  const [loading, setLoading] = useState(false)
  const [initLoading, setInitLoading] = useState(true)
  const [error, setError] = useState('')
  const [data, setData] = useState<StrategyTriggerSimilarityPageData | null>(null)

  const deferredLookupInput = useDeferredValue(lookupInput)
  const inputCodeDigits = sanitizeCodeInput(lookupInput)
  const normalizedCode = inputCodeDigits.length === 6 ? stdTsCode(inputCodeDigits) : ''
  const stockNameCandidates = useMemo(
    () => buildStockLookupCandidates(lookupRows, deferredLookupInput, MAX_STOCK_NAME_CANDIDATES),
    [deferredLookupInput, lookupRows],
  )
  const exactStockLookupMatch = useMemo(
    () => findExactStockLookupMatch(lookupRows, lookupInput),
    [lookupInput, lookupRows],
  )
  const readTargetCode =
    normalizedCode ||
    (exactStockLookupMatch ? stdTsCode(getLookupDigits(exactStockLookupMatch.ts_code)) : '')
  const showStockNameCandidates =
    lookupFocused && lookupInput.trim() !== '' && stockNameCandidates.length > 0
  const detailNavigationItems = useMemo<DetailsNavigationItem[]>(
    () =>
      (data?.items ?? []).map((row) => ({
        tsCode: row.tsCode,
        tradeDate: row.candidateEndTradeDate,
        intervalStartTradeDate: row.candidateStartTradeDate,
        intervalEndTradeDate: row.candidateEndTradeDate,
        sourcePath: sourcePath.trim() || undefined,
        name: displayStockName(row),
      })),
    [data, sourcePath],
  )

  useEffect(() => {
    let cancelled = false
    async function loadInitialData() {
      setInitLoading(true)
      setError('')
      try {
        const resolvedSourcePath = await ensureManagedSourcePath()
        if (cancelled) {
          return
        }
        setSourcePath(resolvedSourcePath)
        const [dates, stocks] = await Promise.all([
          listRankTradeDates(resolvedSourcePath),
          listStockLookupRows(resolvedSourcePath),
        ])
        if (cancelled) {
          return
        }
        const normalizedDates = normalizeTradeDates(dates)
        setTradeDateOptions(normalizedDates)
        setTradeDateInput((current) => pickDateValue(current, normalizedDates))
        setLookupRows(stocks)
      } catch (loadError) {
        if (!cancelled) {
          setError(`初始化失败: ${String(loadError)}`)
        }
      } finally {
        if (!cancelled) {
          setInitLoading(false)
        }
      }
    }

    void loadInitialData()
    return () => {
      cancelled = true
    }
  }, [])

  function onSelectStockCandidate(row: StockLookupRow) {
    setLookupInput(row.name || getLookupDigits(row.ts_code) || row.ts_code)
    setLookupFocused(false)
  }

  async function onQuery() {
    const sourcePathTrimmed = sourcePath.trim()
    if (sourcePathTrimmed === '') {
      setError('数据源路径为空')
      return
    }
    if (readTargetCode === '') {
      setError('请输入有效股票代码或选择股票')
      return
    }

    setLoading(true)
    setError('')
    try {
      const result = await getStrategyTriggerSimilarityPage({
        sourcePath: sourcePathTrimmed,
        tradeDate: tradeDateInput || undefined,
        tsCode: readTargetCode,
        windowTradeDays: parsePositiveInt(windowTradeDaysInput, Number(DEFAULT_WINDOW_TRADE_DAYS)),
        maxGapTradeDays: parseNonNegativeInt(
          maxGapTradeDaysInput,
          Number(DEFAULT_MAX_GAP_TRADE_DAYS),
        ),
        limit: parsePositiveInt(limitInput, Number(DEFAULT_LIMIT)),
      })
      setData(result)
    } catch (queryError) {
      setData(null)
      setError(`查询失败: ${String(queryError)}`)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="trigger-sim-page">
      <section className="trigger-sim-card trigger-sim-query-card">
        <div className="trigger-sim-head">
          <h2>策略触发相似</h2>
          <span>{initLoading ? '初始化中...' : sourcePath || '--'}</span>
        </div>

        <div className="trigger-sim-form-grid">
          <label className="trigger-sim-field">
            <span>目标参考日</span>
            <select
              value={tradeDateInput}
              onChange={(event) => setTradeDateInput(event.target.value)}
              disabled={initLoading || tradeDateOptions.length === 0}
            >
              {tradeDateOptions.length === 0 ? (
                <option value="">{initLoading ? '读取日期中...' : '暂无可选日期'}</option>
              ) : null}
              {tradeDateOptions.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </label>

          <label className="trigger-sim-field trigger-sim-field-stock">
            <span>代码/名称输入，预览代码：{readTargetCode || '--'}</span>
            <div className="trigger-sim-autocomplete">
              <input
                type="text"
                value={lookupInput}
                onChange={(event) => setLookupInput(event.target.value)}
                onFocus={() => setLookupFocused(true)}
                onBlur={() => setLookupFocused(false)}
                onKeyDown={(event) => {
                  if (event.key === 'Enter') {
                    event.preventDefault()
                    if (stockNameCandidates.length > 0) {
                      onSelectStockCandidate(stockNameCandidates[0])
                    } else {
                      void onQuery()
                    }
                  }
                }}
                placeholder="输入股票名称、代码或拼音首字母"
              />
              {showStockNameCandidates ? (
                <div className="trigger-sim-autocomplete-menu">
                  {stockNameCandidates.map((row) => {
                    const code = getLookupDigits(row.ts_code)
                    return (
                      <button
                        className="trigger-sim-autocomplete-option"
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

          <label className="trigger-sim-field">
            <span>窗口交易日</span>
            <input
              type="number"
              min={1}
              step={1}
              value={windowTradeDaysInput}
              onChange={(event) => setWindowTradeDaysInput(event.target.value)}
            />
          </label>

          <label className="trigger-sim-field">
            <span>最大错位交易日</span>
            <input
              type="number"
              min={0}
              step={1}
              value={maxGapTradeDaysInput}
              onChange={(event) => setMaxGapTradeDaysInput(event.target.value)}
            />
          </label>

          <label className="trigger-sim-field">
            <span>结果数量</span>
            <input
              type="number"
              min={1}
              step={1}
              value={limitInput}
              onChange={(event) => setLimitInput(event.target.value)}
            />
          </label>

          <div className="trigger-sim-actions">
            <button
              className="trigger-sim-primary-btn"
              type="button"
              disabled={loading || initLoading || sourcePath.trim() === '' || readTargetCode === ''}
              onClick={onQuery}
            >
              {loading ? '查询中...' : '查询相似'}
            </button>
          </div>
        </div>

        {error ? <div className="trigger-sim-error">{error}</div> : null}
      </section>

      <section className="trigger-sim-card">
        <div className="trigger-sim-result-head">
          <div>
            <h3>相似列表</h3>
            <p>
              {data
                ? `${data.target.name || data.resolvedTsCode} · ${data.target.triggerCount} 次触发 · ${data.target.startTradeDate} 至 ${data.target.endTradeDate}`
                : '输入参考日和股票后查询'}
            </p>
          </div>
          {data ? (
            <span>
              {data.items.length} 条 · 窗口 {data.windowTradeDays} · 错位 {data.maxGapTradeDays}
            </span>
          ) : null}
        </div>

        {!data ? (
          <div className="trigger-sim-empty">暂无查询结果</div>
        ) : data.items.length === 0 ? (
          <div className="trigger-sim-empty">没有找到策略触发时间相似的股票</div>
        ) : (
          <div className="trigger-sim-table-wrap">
            <table className="trigger-sim-table">
              <thead>
                <tr>
                  <th>股票</th>
                  <th>匹配区间</th>
                  <th>行业</th>
                  <th>概念</th>
                  <th>双向相似度</th>
                  <th>匹配触发</th>
                  <th>平均错位</th>
                  <th>候选触发</th>
                  <th>总分</th>
                  <th>总榜</th>
                </tr>
              </thead>
              <tbody>
                {data.items.map((row) => {
                  const conceptText = formatConceptText(row.concept, excludedConcepts)
                  return (
                    <tr key={`${row.tsCode}-${row.candidateStartTradeDate}-${row.candidateEndTradeDate}`}>
                      <td>
                        <DetailsLink
                          className="trigger-sim-stock-link"
                          tsCode={row.tsCode}
                          tradeDate={row.candidateEndTradeDate}
                          intervalStartTradeDate={row.candidateStartTradeDate}
                          intervalEndTradeDate={row.candidateEndTradeDate}
                          sourcePath={sourcePath}
                          navigationItems={detailNavigationItems}
                          title={`查看 ${displayStockName(row)} 详情`}
                        >
                          <strong>{displayStockName(row)}</strong>
                          <span>{row.tsCode}</span>
                        </DetailsLink>
                      </td>
                      <td>
                        {row.candidateStartTradeDate}
                        <span className="trigger-sim-date-separator">至</span>
                        {row.candidateEndTradeDate}
                      </td>
                      <td>{displayText(row.industry)}</td>
                      <td className="trigger-sim-concept-cell" title={conceptText}>
                        {conceptText}
                      </td>
                      <td>{formatNumber(row.similarityScore, 1)}</td>
                      <td>{row.matchedEventCount}</td>
                      <td>{formatNumber(row.avgDateGapTradeDays, 2)}</td>
                      <td>{row.candidateTriggerCount}</td>
                      <td>{formatNumber(row.totalScore, 1)}</td>
                      <td>{row.rank === null || row.rank === undefined ? '--' : `#${row.rank}`}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  )
}
