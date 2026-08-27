import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { ensureManagedSourcePath } from '../../apis/managedSource'
import { listRankTradeDates } from '../../apis/reader'
import {
  getStrategyTriggerSimilarityRankingPage,
  type StrategyTriggerRankingPageData,
} from '../../apis/strategyTriggerSimilarity'
import DetailsLink from '../../shared/DetailsLink'
import {
  filterBoardItems,
  isStBoard,
  useConceptExclusions,
} from '../../shared/conceptExclusions'
import { STOCK_PICK_BOARD_OPTIONS } from '../../shared/stockPickShared'
import { normalizeTradeDates, pickDateValue } from '../../shared/tradeDate'
import './css/StrategyTriggerSimilarityPage.css'
import './css/OverviewScenePage.css'

function formatNumber(value: number | null | undefined, digits = 1) {
  return typeof value === 'number' && Number.isFinite(value) ? value.toFixed(digits) : '--'
}

function formatPercent(value: number | null | undefined, digits = 2) {
  if (typeof value !== 'number' || !Number.isFinite(value)) return '--'
  return `${value > 0 ? '+' : ''}${value.toFixed(digits)}%`
}

function formatElapsed(value: number | null | undefined) {
  if (typeof value !== 'number' || !Number.isFinite(value)) return '--'
  if (value < 60_000) return `${(value / 1000).toFixed(1)} 秒`
  return `${Math.floor(value / 60_000)} 分 ${Math.round((value % 60_000) / 1000)} 秒`
}

function formatGeneratedAt(value: number | null | undefined) {
  return typeof value === 'number' && Number.isFinite(value)
    ? new Date(value * 1000).toLocaleString()
    : '--'
}

function outcomeTone(value: number | null | undefined) {
  if (typeof value !== 'number' || !Number.isFinite(value) || value === 0) return ''
  return value > 0 ? ' is-positive' : ' is-negative'
}

export default function OverviewSimilarityRankingPage() {
  const [sourcePath, setSourcePath] = useState('')
  const [dateOptions, setDateOptions] = useState<string[]>([])
  const [tradeDate, setTradeDate] = useState('')
  const [data, setData] = useState<StrategyTriggerRankingPageData | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const { excludeStBoard } = useConceptExclusions()
  const [limitInput, setLimitInput] = useState('100')
  const [boardFilter, setBoardFilter] = useState('全部')
  const [totalMvMinInput, setTotalMvMinInput] = useState('')
  const [totalMvMaxInput, setTotalMvMaxInput] = useState('')

  const boardOptions = useMemo(
    () => filterBoardItems(STOCK_PICK_BOARD_OPTIONS, excludeStBoard),
    [excludeStBoard],
  )

  useEffect(() => {
    if (excludeStBoard && isStBoard(boardFilter)) {
      setBoardFilter('全部')
    }
  }, [boardFilter, excludeStBoard])

  const navigationItems = useMemo(
    () =>
      (data?.items ?? []).map((row) => ({
        tsCode: row.tsCode,
        tradeDate: data?.resolvedTradeDate,
        sourcePath: sourcePath || undefined,
        name: row.name || row.tsCode,
      })),
    [data, sourcePath],
  )

  const historicalNavigationItems = useMemo(
    () =>
      (data?.items ?? []).flatMap((row) =>
        row.topMatches.map((match) => ({
          tsCode: match.tsCode,
          tradeDate: match.candidateEndTradeDate,
          intervalStartTradeDate: match.candidateStartTradeDate,
          intervalEndTradeDate: match.candidateEndTradeDate,
          sourcePath: sourcePath || undefined,
          name: match.name || match.tsCode,
        })),
      ),
    [data, sourcePath],
  )

  useEffect(() => {
    let cancelled = false
    void ensureManagedSourcePath()
      .then(async (path) => {
        const dates = await listRankTradeDates(path)
        if (cancelled) return
        const normalizedDates = normalizeTradeDates(dates)
        const initialTradeDate = pickDateValue('', normalizedDates)
        setSourcePath(path)
        setDateOptions(normalizedDates)
        setTradeDate(initialTradeDate)
        if (initialTradeDate) {
          const initialData = await getStrategyTriggerSimilarityRankingPage({
            sourcePath: path,
            tradeDate: initialTradeDate,
            limit: 100,
          })
          if (!cancelled) setData(initialData)
        }
      })
      .catch((loadError) => {
        if (!cancelled) setError(`初始化失败: ${String(loadError)}`)
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [])

  async function onRead() {
    if (!sourcePath || !tradeDate) return
    setLoading(true)
    setError('')
    let limit: number | undefined
    const limitRaw = limitInput.trim()
    if (limitRaw) {
      const parsedLimit = Number(limitRaw)
      if (!Number.isInteger(parsedLimit) || parsedLimit <= 0) {
        setError('限制行数必须是正整数')
        setLoading(false)
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
        setLoading(false)
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
        setLoading(false)
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
      setLoading(false)
      return
    }

    try {
      const result = await getStrategyTriggerSimilarityRankingPage({
        sourcePath,
        tradeDate,
        limit,
        board: boardFilter === '全部' ? undefined : boardFilter,
        excludeStBoard: excludeStBoard || undefined,
        totalMvMin,
        totalMvMax,
      })
      setData(result)
    } catch (readError) {
      setData(null)
      setError(`读取走势相似排名失败: ${String(readError)}`)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="trigger-sim-page">
      <section className="trigger-sim-card trigger-sim-query-card">
        <div className="trigger-sim-head">
          <div>
            <h2>走势相似排名</h2>
          </div>
          <span>{sourcePath || '--'}</span>
        </div>
        <div className="overview-form-grid">
          <label className="overview-field">
            <span>排名日期</span>
            <select value={tradeDate} onChange={(event) => setTradeDate(event.target.value)}>
              {dateOptions.map((date) => <option key={date} value={date}>{date}</option>)}
            </select>
          </label>
          <label className="overview-field">
            <span>限制行数</span>
            <input
              type="number"
              min={1}
              step={1}
              value={limitInput}
              onChange={(e) => setLimitInput(e.target.value)}
              placeholder="100"
            />
          </label>
          <label className="overview-field">
            <span>板块筛选</span>
            <select
              value={boardFilter}
              onChange={(event) =>
                setBoardFilter(
                  event.target.value as (typeof STOCK_PICK_BOARD_OPTIONS)[number],
                )
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
              onChange={(e) => setTotalMvMinInput(e.target.value)}
              placeholder="留空=不限"
            />
          </label>
          <label className="overview-field">
            <span>总市值最大(亿)</span>
            <input
              type="number"
              step={0.01}
              value={totalMvMaxInput}
              onChange={(e) => setTotalMvMaxInput(e.target.value)}
              placeholder="留空=不限"
            />
          </label>
        </div>
        <div className="overview-actions">
          <button
            className="overview-read-btn"
            type="button"
            disabled={loading || !sourcePath || !tradeDate}
            onClick={() => void onRead()}
          >
            {loading ? '读取中...' : '读取排名'}
          </button>
        </div>
        {error ? <div className="trigger-sim-error">{error}</div> : null}
      </section>

      {data?.isFresh ? (
        <section className="trigger-sim-card trigger-sim-ranking-card">
          <div className="trigger-sim-result-head">
            <div><h3>{data.resolvedTradeDate} 全市场排名</h3><p>历史截止 {data.historicalCutoffDate} · 生成于 {formatGeneratedAt(data.generatedAtEpochSeconds)}</p></div>
            <span>耗时 {formatElapsed(data.elapsedMs)}</span>
          </div>
          <div className="trigger-sim-engine-meta">
            <span>股票池 {data.universeCount}</span><span>有效排名 {data.rankedCount}</span>
            <span>历史模板 {data.evaluatedAnchorCount} / {data.candidateAnchorCount}</span>
            <span>窗口 {data.windowTradeDays} · 池化 {data.poolSegments} · 后验 {data.outcomeTradeDays}</span>
            <span>基准 {data.benchmarkIndexCode}</span><span>实时水位核验通过</span>
          </div>
          <div className="trigger-sim-ranking-scroll">
            <table className="trigger-sim-table trigger-sim-ranking-table">
              <thead><tr><th>排名</th><th>股票</th><th>板块</th><th>总市值(亿)</th><th>预测分</th><th>收缩超额</th><th>超额胜率</th><th>收益 / 超额</th><th>MFE / MAE</th><th>置信度</th><th>样本</th><th>相似度</th><th>策略排名</th><th>代表历史事件</th></tr></thead>
              <tbody>{data.items.map((row) => (
                <tr key={row.tsCode}>
                  <td>{row.rank ?? '--'}</td>
                  <td><DetailsLink className="trigger-sim-stock-link" tsCode={row.tsCode} tradeDate={data.resolvedTradeDate} sourcePath={sourcePath} navigationItems={navigationItems}><strong>{row.name || row.tsCode}</strong><span>{row.tsCode}</span></DetailsLink></td>
                  <td>{row.board || '--'}</td>
                  <td>{formatNumber(row.totalMvYi, 2)}</td>
                  <td>{formatNumber(row.rankingScore)}</td>
                  <td className={outcomeTone(row.shrunkExcessReturnPct)}>{formatPercent(row.shrunkExcessReturnPct)}</td>
                  <td>{formatPercent(row.excessPositiveRate, 1)}</td>
                  <td>{formatPercent(row.expectedReturnPct)} / {formatPercent(row.expectedExcessReturnPct)}</td>
                  <td>{formatPercent(row.expectedMfePct)} / {formatPercent(row.expectedMaePct)}</td>
                  <td>{formatPercent(row.confidence * 100, 1)}</td>
                  <td>{row.sampleCount} / 有效 {formatNumber(row.effectiveSampleCount)}</td>
                  <td>{formatNumber(row.averageSimilarity)} / {formatNumber(row.bestSimilarity)}</td>
                  <td>{row.originalRank ?? '--'}</td>
                  <td>
                    {row.topMatches[0] ? (
                      <DetailsLink
                        className="trigger-sim-stock-link trigger-sim-history-link"
                        tsCode={row.topMatches[0].tsCode}
                        tradeDate={row.topMatches[0].candidateEndTradeDate}
                        intervalStartTradeDate={row.topMatches[0].candidateStartTradeDate}
                        intervalEndTradeDate={row.topMatches[0].candidateEndTradeDate}
                        sourcePath={sourcePath}
                        navigationItems={historicalNavigationItems}
                        title={`查看 ${row.topMatches[0].name || row.topMatches[0].tsCode} 历史事件详情`}
                      >
                        <strong>{row.topMatches[0].name || row.topMatches[0].tsCode}</strong>
                        <span>{row.topMatches[0].candidateEndTradeDate}</span>
                      </DetailsLink>
                    ) : (
                      '--'
                    )}
                  </td>
                </tr>
              ))}</tbody>
            </table>
          </div>
        </section>
      ) : (
        <section className="trigger-sim-card">
          <div className="trigger-sim-stale-banner">
            <strong>{loading ? '正在读取' : '当前参数没有可用的新鲜排名'}</strong>
            <span>{data?.staleReason || '请先到“下载/计算”页面生成走势相似排行榜。'}</span>
            <Link to="/raw-data/download-compute">前往计算页面</Link>
          </div>
        </section>
      )}
    </div>
  )
}
