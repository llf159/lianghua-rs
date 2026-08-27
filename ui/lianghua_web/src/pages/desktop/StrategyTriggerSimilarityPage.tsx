import { useDeferredValue, useEffect, useMemo, useState } from 'react'
import { ensureManagedSourcePath } from '../../apis/managedSource'
import { listRankTradeDates, listStockLookupRows, type StockLookupRow } from '../../apis/reader'
import {
  getStrategyTriggerSimilarityPage,
  getStrategyTriggerSimilarityRankingPage,
  listStrategyTriggerSimilarityBenchmarkIndexCodes,
  runStrategyTriggerSimilarityRanking,
  type StrategyTriggerRankingPageData,
  type StrategyTriggerSimilarityPageData,
  type StrategyTriggerSimilarityOutcomeSummary,
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
const DEFAULT_POOL_SEGMENTS = '5'
const DEFAULT_OUTCOME_TRADE_DAYS = '5'
const DEFAULT_LIMIT = '30'
const DEFAULT_BENCHMARK_INDEX_CODE = '000001.SH'
const SHOW_FULL_MARKET_RANKING = false
const BENCHMARK_INDEX_LABELS: Record<string, string> = {
  '000001.SH': '上证指数',
  '399001.SZ': '深证成指',
  '399300.SZ': '沪深300',
  '399905.SZ': '中证500',
  '399006.SZ': '创业板指',
  '000016.SH': '上证50',
  '000852.SH': '中证1000',
}

function benchmarkIndexLabel(code: string) {
  return BENCHMARK_INDEX_LABELS[code] || code
}

function parsePositiveInt(value: string, fallback: number) {
  const parsed = Number.parseInt(value.trim(), 10)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback
}

function formatNumber(value: number | null | undefined, digits = 1) {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return '--'
  }
  return value.toFixed(digits)
}

function formatPercent(value: number | null | undefined, digits = 2) {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return '--'
  }
  return `${value > 0 ? '+' : ''}${value.toFixed(digits)}%`
}

function outcomeTone(value: number | null | undefined) {
  if (typeof value !== 'number' || !Number.isFinite(value) || value === 0) {
    return ''
  }
  return value > 0 ? ' is-positive' : ' is-negative'
}

function assessRobustDirection(summary: StrategyTriggerSimilarityOutcomeSummary) {
  const winsorized = summary.winsorizedExcessReturnPct
  const median = summary.weightedMedianExcessReturnPct
  const positiveRate = summary.weightedExcessPositiveRate
  if (
    typeof winsorized !== 'number' ||
    !Number.isFinite(winsorized) ||
    typeof median !== 'number' ||
    !Number.isFinite(median) ||
    typeof positiveRate !== 'number' ||
    !Number.isFinite(positiveRate)
  ) {
    return { label: '样本不足', tone: 0 }
  }
  if (winsorized > 0 && median > 0 && positiveRate > 50) {
    return { label: '偏正', tone: 1 }
  }
  if (winsorized < 0 && median < 0 && positiveRate < 50) {
    return { label: '偏负', tone: -1 }
  }
  return { label: '方向分歧', tone: 0 }
}

function displayStockName(row: { tsCode: string; name?: string | null }) {
  return row.name?.trim() || splitTsCode(row.tsCode)
}

function displayText(value: string | null | undefined) {
  const text = value?.trim()
  return text ? text : '--'
}

function formatElapsed(value: number | null | undefined) {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return '--'
  }
  if (value < 60_000) {
    return `${(value / 1000).toFixed(1)} 秒`
  }
  return `${Math.floor(value / 60_000)} 分 ${Math.round((value % 60_000) / 1000)} 秒`
}

function formatGeneratedAt(value: number | null | undefined) {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return '--'
  }
  return new Date(value * 1000).toLocaleString()
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
  const [poolSegmentsInput, setPoolSegmentsInput] = useState(DEFAULT_POOL_SEGMENTS)
  const [outcomeTradeDaysInput, setOutcomeTradeDaysInput] = useState(
    DEFAULT_OUTCOME_TRADE_DAYS,
  )
  const [benchmarkIndexCodes, setBenchmarkIndexCodes] = useState<string[]>([
    DEFAULT_BENCHMARK_INDEX_CODE,
  ])
  const [benchmarkIndexCode, setBenchmarkIndexCode] = useState(DEFAULT_BENCHMARK_INDEX_CODE)
  const [limitInput, setLimitInput] = useState(DEFAULT_LIMIT)
  const [loading, setLoading] = useState(false)
  const [initLoading, setInitLoading] = useState(true)
  const [error, setError] = useState('')
  const [data, setData] = useState<StrategyTriggerSimilarityPageData | null>(null)
  const [rankingData, setRankingData] = useState<StrategyTriggerRankingPageData | null>(null)
  const [rankingLoading, setRankingLoading] = useState(false)
  const [rankingError, setRankingError] = useState('')

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
  const robustDirection = data ? assessRobustDirection(data.outcomeSummary) : null
  const rankingNavigationItems = useMemo<DetailsNavigationItem[]>(
    () =>
      (rankingData?.items ?? []).map((row) => ({
        tsCode: row.tsCode,
        tradeDate: rankingData?.resolvedTradeDate,
        sourcePath: sourcePath.trim() || undefined,
        name: displayStockName(row),
      })),
    [rankingData, sourcePath],
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
        const [dates, stocks, indexCodes] = await Promise.all([
          listRankTradeDates(resolvedSourcePath),
          listStockLookupRows(resolvedSourcePath),
          listStrategyTriggerSimilarityBenchmarkIndexCodes(),
        ])
        if (cancelled) {
          return
        }
        const normalizedDates = normalizeTradeDates(dates)
        setTradeDateOptions(normalizedDates)
        setTradeDateInput((current) => pickDateValue(current, normalizedDates))
        setLookupRows(stocks)
        setBenchmarkIndexCodes(indexCodes)
        setBenchmarkIndexCode((current) =>
          indexCodes.includes(current) ? current : indexCodes[0] || DEFAULT_BENCHMARK_INDEX_CODE,
        )
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

  useEffect(() => {
    if (!SHOW_FULL_MARKET_RANKING) {
      return
    }
    if (sourcePath.trim() === '' || tradeDateInput === '') {
      return
    }
    let cancelled = false
    async function loadRanking() {
      setRankingLoading(true)
      setRankingError('')
      setRankingData(null)
      try {
        const result = await getStrategyTriggerSimilarityRankingPage({
          sourcePath: sourcePath.trim(),
          tradeDate: tradeDateInput,
          windowTradeDays: parsePositiveInt(windowTradeDaysInput, Number(DEFAULT_WINDOW_TRADE_DAYS)),
          poolSegments: parsePositiveInt(poolSegmentsInput, Number(DEFAULT_POOL_SEGMENTS)),
          outcomeTradeDays: parsePositiveInt(
            outcomeTradeDaysInput,
            Number(DEFAULT_OUTCOME_TRADE_DAYS),
          ),
          benchmarkIndexCode,
          limit: 100,
        })
        if (!cancelled) {
          setRankingData(result)
        }
      } catch (loadError) {
        if (!cancelled) {
          setRankingData(null)
          setRankingError(`读取排行榜失败: ${String(loadError)}`)
        }
      } finally {
        if (!cancelled) {
          setRankingLoading(false)
        }
      }
    }
    void loadRanking()
    return () => {
      cancelled = true
    }
  }, [
    benchmarkIndexCode,
    outcomeTradeDaysInput,
    poolSegmentsInput,
    sourcePath,
    tradeDateInput,
    windowTradeDaysInput,
  ])

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
    setData(null)
    try {
      const result = await getStrategyTriggerSimilarityPage({
        sourcePath: sourcePathTrimmed,
        tradeDate: tradeDateInput || undefined,
        tsCode: readTargetCode,
        windowTradeDays: parsePositiveInt(windowTradeDaysInput, Number(DEFAULT_WINDOW_TRADE_DAYS)),
        poolSegments: parsePositiveInt(poolSegmentsInput, Number(DEFAULT_POOL_SEGMENTS)),
        outcomeTradeDays: parsePositiveInt(
          outcomeTradeDaysInput,
          Number(DEFAULT_OUTCOME_TRADE_DAYS),
        ),
        benchmarkIndexCode,
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

  async function onRefreshRanking() {
    const sourcePathTrimmed = sourcePath.trim()
    if (sourcePathTrimmed === '' || tradeDateInput === '') {
      setRankingError('数据源路径或参考日为空')
      return
    }
    setRankingLoading(true)
    setRankingError('')
    setRankingData(null)
    try {
      const result = await runStrategyTriggerSimilarityRanking({
        sourcePath: sourcePathTrimmed,
        tradeDate: tradeDateInput,
        windowTradeDays: parsePositiveInt(windowTradeDaysInput, Number(DEFAULT_WINDOW_TRADE_DAYS)),
        poolSegments: parsePositiveInt(poolSegmentsInput, Number(DEFAULT_POOL_SEGMENTS)),
        outcomeTradeDays: parsePositiveInt(
          outcomeTradeDaysInput,
          Number(DEFAULT_OUTCOME_TRADE_DAYS),
        ),
        benchmarkIndexCode,
        limit: 100,
      })
      setRankingData(result)
    } catch (refreshError) {
      setRankingError(`计算排行榜失败: ${String(refreshError)}`)
    } finally {
      setRankingLoading(false)
    }
  }

  return (
    <div className="trigger-sim-page">
      <section className="trigger-sim-card trigger-sim-query-card">
        <div className="trigger-sim-head">
          <h2>综合相似走势分析</h2>
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
            <span>池化分段</span>
            <input
              type="number"
              min={1}
              max={12}
              step={1}
              value={poolSegmentsInput}
              onChange={(event) => setPoolSegmentsInput(event.target.value)}
            />
          </label>

          <label className="trigger-sim-field">
            <span>后验交易日</span>
            <input
              type="number"
              min={1}
              step={1}
              value={outcomeTradeDaysInput}
              onChange={(event) => setOutcomeTradeDaysInput(event.target.value)}
            />
          </label>

          <label className="trigger-sim-field">
            <span>评级基准指数</span>
            <select
              value={benchmarkIndexCode}
              onChange={(event) => setBenchmarkIndexCode(event.target.value)}
            >
              {benchmarkIndexCodes.map((code) => (
                <option key={code} value={code}>
                  {benchmarkIndexLabel(code)} · {code}
                </option>
              ))}
            </select>
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

      {SHOW_FULL_MARKET_RANKING ? <section className="trigger-sim-card trigger-sim-ranking-card">
        <div className="trigger-sim-result-head">
          <div>
            <h3>策略触发未来走势排行榜</h3>
            <p>
              先从全部历史评分走势的未来表现反推启动点，再读取启动窗口中的策略触发序列；预测周期{' '}
              {rankingData?.outcomeTradeDays ?? outcomeTradeDaysInput} 日
            </p>
          </div>
          <span>
            {rankingLoading
              ? '正在计算或核验数据水位...'
              : rankingData?.isFresh
                ? `已同步 · ${formatGeneratedAt(rankingData.generatedAtEpochSeconds)}`
                : '需要计算'}
          </span>
        </div>

        {rankingError ? <div className="trigger-sim-error">{rankingError}</div> : null}
        {rankingData && !rankingData.isFresh ? (
          <div className="trigger-sim-stale-banner">
            <strong>排行榜不可视为最新</strong>
            <span>{rankingData.staleReason}</span>
            <button type="button" disabled={rankingLoading} onClick={onRefreshRanking}>
              立即按当前数据重算
            </button>
          </div>
        ) : null}

        {rankingData?.isFresh ? (
          <>
            <div className="trigger-sim-engine-meta">
              <span>股票池 {rankingData.universeCount}</span>
              <span>有效排名 {rankingData.rankedCount}</span>
              <span>
                历史锚点 {rankingData.evaluatedAnchorCount} / {rankingData.candidateAnchorCount}
              </span>
              <span>历史截止 {rankingData.historicalCutoffDate}</span>
              <span>耗时 {formatElapsed(rankingData.elapsedMs)}</span>
              <span>实时水位核验</span>
            </div>
            <p className="trigger-sim-direction-note">
              历史质量由未来超额分位45%、MFE分位25%、MAE分位20%和上涨持续性分位10%组成；
              取首次进入每日横截面前20%的成功启动，以及首次进入后20%的失败对照。规则不决定历史样本身份，
              但启动窗口必须存在策略触发，当前股票也必须在观察窗口内有触发。预测分由相似成功/失败模板的
              连续质量加权并结合有效样本置信度后，在当日股票池内转为百分位。
            </p>
            {rankingData.timings.length > 0 ? (
              <details className="trigger-sim-indicator-details">
                <summary>查看批量计算各阶段耗时</summary>
                <p>
                  {rankingData.timings
                    .map((timing) => `${timing.label} ${formatElapsed(timing.elapsedMs)}`)
                    .join(' · ')}
                </p>
              </details>
            ) : null}
          </>
        ) : null}

        {!rankingData || rankingData.items.length === 0 ? (
          <div className="trigger-sim-empty">
            {rankingLoading ? '全市场批量计算中，请保持页面打开' : '当前参数尚无排行榜结果'}
          </div>
        ) : (
          <div className="trigger-sim-table-wrap">
            <table className="trigger-sim-table trigger-sim-ranking-table">
              <thead>
                <tr>
                  <th>排名</th>
                  <th>股票</th>
                  <th>预测分</th>
                  <th>模板收缩超额</th>
                  <th>模板超额胜率</th>
                  <th>模板收益 / 超额</th>
                  <th>模板 MFE / MAE</th>
                  <th>置信度</th>
                  <th>样本</th>
                  <th>相似度</th>
                  <th>策略触发</th>
                  <th>当前策略排名</th>
                  <th>代表历史事件</th>
                </tr>
              </thead>
              <tbody>
                {rankingData.items.map((row) => {
                  const matchTitle = row.topMatches
                    .map(
                      (match) =>
                        `${match.name || match.tsCode} ${match.candidateEndTradeDate} · 相似 ${formatNumber(match.similarityScore, 1)} · 超额 ${formatPercent(match.forwardExcessReturnPct)}`,
                    )
                    .join('\n')
                  return (
                    <tr key={row.tsCode}>
                      <td>{row.rank ?? '--'}</td>
                      <td>
                        <DetailsLink
                          className="trigger-sim-stock-link"
                          tsCode={row.tsCode}
                          tradeDate={rankingData.resolvedTradeDate}
                          sourcePath={sourcePath}
                          navigationItems={rankingNavigationItems}
                          title={`查看 ${displayStockName(row)} 详情`}
                        >
                          <strong>{displayStockName(row)}</strong>
                          <span>{row.tsCode}</span>
                        </DetailsLink>
                      </td>
                      <td>{formatNumber(row.rankingScore, 1)}</td>
                      <td className={outcomeTone(row.shrunkExcessReturnPct)}>
                        {formatPercent(row.shrunkExcessReturnPct)}
                      </td>
                      <td>{formatPercent(row.excessPositiveRate, 1)}</td>
                      <td>
                        <span className={outcomeTone(row.expectedReturnPct)}>
                          {formatPercent(row.expectedReturnPct)}
                        </span>
                        <span className={`trigger-sim-date-separator${outcomeTone(row.expectedExcessReturnPct)}`}>
                          {formatPercent(row.expectedExcessReturnPct)}
                        </span>
                      </td>
                      <td>
                        <span className="is-positive">{formatPercent(row.expectedMfePct)}</span>
                        <span className="trigger-sim-date-separator is-negative">
                          {formatPercent(row.expectedMaePct)}
                        </span>
                      </td>
                      <td>{formatPercent(row.confidence * 100, 1)}</td>
                      <td>
                        {row.sampleCount}
                        <span className="trigger-sim-date-separator">
                          有效 {formatNumber(row.effectiveSampleCount, 1)}
                        </span>
                      </td>
                      <td>
                        {formatNumber(row.averageSimilarity, 1)}
                        <span className="trigger-sim-date-separator">
                          最高 {formatNumber(row.bestSimilarity, 1)}
                        </span>
                      </td>
                      <td>{row.triggerCount}</td>
                      <td>{row.originalRank ?? '--'}</td>
                      <td className="trigger-sim-rules-cell" title={matchTitle}>
                        {row.topMatches.length > 0
                          ? `${row.topMatches[0].name || row.topMatches[0].tsCode} · ${row.topMatches[0].candidateEndTradeDate}`
                          : '--'}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </section> : null}

      {data ? (
        <section className="trigger-sim-card trigger-sim-experience-card">
          <div className="trigger-sim-result-head">
            <div>
              <h3>相似事件经验</h3>
              <p>
                仅使用 {data.historicalCutoffDate} 以前触发、且已完整走完 {data.outcomeTradeDays}{' '}
                个交易日的历史事件
              </p>
            </div>
            <span>
              评级样本 {data.outcomeSummary.sampleCount} / 有效{' '}
              {formatNumber(data.outcomeSummary.effectiveSampleCount, 1)}
            </span>
          </div>

          <div className="trigger-sim-metric-grid">
            <div className="trigger-sim-metric trigger-sim-metric-primary">
              <span>稳健方向</span>
              <strong className={outcomeTone(robustDirection?.tone)}>
                {robustDirection?.label || '--'}
              </strong>
            </div>
            <div className="trigger-sim-metric">
              <span>10% 去极值超额</span>
              <strong className={outcomeTone(data.outcomeSummary.winsorizedExcessReturnPct)}>
                {formatPercent(data.outcomeSummary.winsorizedExcessReturnPct)}
              </strong>
            </div>
            <div className="trigger-sim-metric">
              <span>加权超额中位数</span>
              <strong className={outcomeTone(data.outcomeSummary.weightedMedianExcessReturnPct)}>
                {formatPercent(data.outcomeSummary.weightedMedianExcessReturnPct)}
              </strong>
            </div>
            <div className="trigger-sim-metric">
              <span>超额胜率</span>
              <strong>{formatPercent(data.outcomeSummary.weightedExcessPositiveRate, 1)}</strong>
            </div>
            <div className="trigger-sim-metric">
              <span>加权收益</span>
              <strong className={outcomeTone(data.outcomeSummary.weightedReturnPct)}>
                {formatPercent(data.outcomeSummary.weightedReturnPct)}
              </strong>
            </div>
            <div className="trigger-sim-metric">
              <span>加权超额</span>
              <strong className={outcomeTone(data.outcomeSummary.weightedExcessReturnPct)}>
                {formatPercent(data.outcomeSummary.weightedExcessReturnPct)}
              </strong>
            </div>
            <div className="trigger-sim-metric">
              <span>收缩后超额</span>
              <strong className={outcomeTone(data.outcomeSummary.shrunkExcessReturnPct)}>
                {formatPercent(data.outcomeSummary.shrunkExcessReturnPct)}
              </strong>
            </div>
            <div className="trigger-sim-metric">
              <span>加权胜率</span>
              <strong>{formatPercent(data.outcomeSummary.weightedPositiveRate, 1)}</strong>
            </div>
            <div className="trigger-sim-metric">
              <span>平均 MFE</span>
              <strong className={outcomeTone(data.outcomeSummary.weightedMfePct)}>
                {formatPercent(data.outcomeSummary.weightedMfePct)}
              </strong>
            </div>
            <div className="trigger-sim-metric">
              <span>平均 MAE</span>
              <strong className={outcomeTone(data.outcomeSummary.weightedMaePct)}>
                {formatPercent(data.outcomeSummary.weightedMaePct)}
              </strong>
            </div>
          </div>
          <p className="trigger-sim-direction-note">
            收益区间为事件次日开盘至周期末收盘，超额基准为
            {benchmarkIndexLabel(data.benchmarkIndexCode)}；评级固定最多取 30 个去重叠样本，
            仅在去极值超额、加权超额中位数和超额胜率三项方向一致时给出方向。
          </p>

          <div className="trigger-sim-engine-meta">
            <span>指纹 {data.target.pooledFeatureDimension} 维</span>
            <span>池化 {data.poolSegments} 段</span>
            <span>{data.kernelNames.join(' · ')}</span>
            <span>数据库指标 {data.indicatorColumns.length} 个</span>
            <span>
              事件全集 {data.candidateUniverseCount} / 入选 {data.candidateAnchorCount} / 有效{' '}
              {data.evaluatedAnchorCount}
              {data.candidatePoolTruncated ? '（最近主体 + 全历史分散补样）' : ''}
            </span>
          </div>
          {data.indicatorColumns.length > 0 ? (
            <details className="trigger-sim-indicator-details">
              <summary>查看参与计算的数据库指标</summary>
              <p>{data.indicatorColumns.join('、')}</p>
            </details>
          ) : null}
        </section>
      ) : null}

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
              {data.items.length} 条 · 窗口 {data.windowTradeDays} · 后验 {data.outcomeTradeDays}
            </span>
          ) : null}
        </div>

        {!data ? (
          <div className="trigger-sim-empty">暂无查询结果</div>
        ) : data.items.length === 0 ? (
          <div className="trigger-sim-empty">没有找到具备完整量价窗口和后验的历史触发事件</div>
        ) : (
          <div className="trigger-sim-table-wrap">
            <table className="trigger-sim-table">
              <thead>
                <tr>
                  <th>股票</th>
                  <th>匹配区间</th>
                  <th>行业</th>
                  <th>概念</th>
                  <th>综合相似</th>
                  <th>触发</th>
                  <th>量价</th>
                  <th>指标</th>
                  <th>市场</th>
                  <th>后验收益</th>
                  <th>后验超额</th>
                  <th>MFE / MAE</th>
                  <th>规则</th>
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
                      <td>{formatNumber(row.triggerSimilarity, 1)}</td>
                      <td>{formatNumber(row.priceVolumeSimilarity, 1)}</td>
                      <td>{formatNumber(row.indicatorSimilarity, 1)}</td>
                      <td>{formatNumber(row.marketSimilarity, 1)}</td>
                      <td className={outcomeTone(row.forwardReturnPct)}>
                        {formatPercent(row.forwardReturnPct)}
                        <span className="trigger-sim-date-separator">
                          {row.outcomeStartTradeDate} 至 {row.outcomeEndTradeDate}
                        </span>
                      </td>
                      <td className={outcomeTone(row.forwardExcessReturnPct)}>
                        {formatPercent(row.forwardExcessReturnPct)}
                      </td>
                      <td>
                        <span className="is-positive">{formatPercent(row.mfePct)}</span>
                        <span className="trigger-sim-date-separator is-negative">
                          {formatPercent(row.maePct)}
                        </span>
                      </td>
                      <td
                        className="trigger-sim-rules-cell"
                        title={row.matchedRuleNames.join('、')}
                      >
                        {row.matchedRuleCount} / {row.candidateTriggerCount}
                      </td>
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
