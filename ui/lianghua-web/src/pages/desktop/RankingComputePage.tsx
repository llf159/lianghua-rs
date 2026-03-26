import { useEffect, useState } from 'react'
import { inspectManagedSourceStatus } from '../../apis/managedSource'
import {
  getRankingComputeStatus,
  runRankingScoreCalculation,
  runRankingTiebreakFill,
  type RankComputeDbRange,
  type RankComputeResultContinuity,
  type RankingComputeStatus,
} from '../../apis/rankingCompute'
import './css/RankingComputePage.css'

type BusyAction = 'idle' | 'loading' | 'computing'

function compactDateToInput(value: string | null | undefined) {
  if (!value || !/^\d{8}$/.test(value)) {
    return ''
  }

  return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`
}

function inputDateToCompact(value: string) {
  return value.replaceAll('-', '').trim()
}

function formatTradeDate(value: string | null | undefined) {
  if (!value) {
    return '--'
  }

  if (/^\d{8}$/.test(value)) {
    return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`
  }

  return value
}

function formatElapsedMs(value: number) {
  if (!Number.isFinite(value) || value < 0) {
    return '--'
  }

  if (value < 1000) {
    return `${Math.round(value)} ms`
  }

  return `${(value / 1000).toFixed(value >= 10_000 ? 1 : 2)} s`
}

function describeDbRange(range: RankComputeDbRange | null | undefined) {
  if (!range) {
    return '读取中...'
  }

  if (!range.exists) {
    return `${range.fileName} 不存在`
  }

  if (!range.minTradeDate || !range.maxTradeDate) {
    return `${range.fileName} 已存在，但当前表里没有交易日数据`
  }

  return `${formatTradeDate(range.minTradeDate)} 至 ${formatTradeDate(range.maxTradeDate)}`
}

function describeResultDbContinuity(check: RankComputeResultContinuity | null | undefined) {
  if (!check) {
    return '读取中...'
  }

  if (!check.checked) {
    return '当前结果库暂无可检查区间'
  }

  return check.isContinuous ? '交易日连续' : '存在交易日缺口'
}

function formatDateSample(values: string[]) {
  if (values.length === 0) {
    return '--'
  }

  return values.map((value) => formatTradeDate(value)).join('、')
}

export default function RankingComputePage() {
  const [status, setStatus] = useState<RankingComputeStatus | null>(null)
  const [busyAction, setBusyAction] = useState<BusyAction>('loading')
  const [startDateInput, setStartDateInput] = useState('')
  const [endDateInput, setEndDateInput] = useState('')
  const [notice, setNotice] = useState('')
  const [error, setError] = useState('')

  const sourcePath = status?.sourcePath?.trim() ?? ''
  const isBusy = busyAction !== 'idle'

  async function loadStatus() {
    setBusyAction('loading')
    setError('')

    try {
      const managedStatus = await inspectManagedSourceStatus()
      const nextStatus = await getRankingComputeStatus(managedStatus.sourcePath)
      setStatus(nextStatus)
      setNotice('')

      setStartDateInput((current) => current || compactDateToInput(nextStatus.suggestedStartDate))
      setEndDateInput((current) => current || compactDateToInput(nextStatus.suggestedEndDate))
    } catch (loadError) {
      setNotice('')
      setError(`读取排名计算状态失败: ${String(loadError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  useEffect(() => {
    void loadStatus()
  }, [])

  async function onRunCompute() {
    if (!sourcePath) {
      setError('当前数据目录为空，请先到数据管理页确认目录。')
      return
    }

    const startDate = inputDateToCompact(startDateInput)
    const endDate = inputDateToCompact(endDateInput)
    if (!startDate || !endDate) {
      setError('请先输入开始日期和结束日期。')
      return
    }

    setBusyAction('computing')
    setError('')

    try {
      const scoreResult = await runRankingScoreCalculation(sourcePath, startDate, endDate)
      const tiebreakResult = await runRankingTiebreakFill(sourcePath)
      setStatus(tiebreakResult.status)
      setNotice(
        `排名计算和补排名完成，区间 ${formatTradeDate(scoreResult.startDate ?? null)} 至 ${formatTradeDate(scoreResult.endDate ?? null)}，总耗时 ${formatElapsedMs(scoreResult.elapsedMs + tiebreakResult.elapsedMs)}。`,
      )
    } catch (actionError) {
      setNotice('')
      setError(`排名计算失败: ${String(actionError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  return (
    <div className="ranking-compute-page">
      <section className="ranking-compute-card">
        <div className="ranking-compute-head">
          <div>
            <h2>排名计算</h2>
          </div>

          <button className="ranking-compute-secondary-btn" type="button" onClick={() => void loadStatus()} disabled={isBusy}>
            {busyAction === 'loading' ? '刷新中...' : '刷新日期信息'}
          </button>
        </div>

        <div className="ranking-compute-summary">
          <div className="ranking-compute-summary-item ranking-compute-summary-item-wide">
            <span>当前数据目录</span>
            <strong title={sourcePath}>{sourcePath || '读取中...'}</strong>
          </div>
          <div className="ranking-compute-summary-item">
            <span>原始库日期范围</span>
            <strong>{describeDbRange(status?.sourceDb)}</strong>
            <small>
              {status?.sourceDb
                ? `${status.sourceDb.distinctTradeDates} 个交易日，${status.sourceDb.rowCount} 行`
                : '读取中...'}
            </small>
          </div>
          <div className="ranking-compute-summary-item">
            <span>结果库日期范围</span>
            <strong>{describeDbRange(status?.resultDb)}</strong>
            <small>
              {status?.resultDb
                ? `${status.resultDb.distinctTradeDates} 个交易日，${status.resultDb.rowCount} 行`
                : '读取中...'}
            </small>
          </div>
          <div className="ranking-compute-summary-item">
            <span>结果库区间检查</span>
            <strong>{describeResultDbContinuity(status?.resultDbContinuity)}</strong>
            <small>
              {status?.resultDbContinuity?.checked
                ? `检查区间 ${formatTradeDate(status.resultDbContinuity.rangeStart)} 至 ${formatTradeDate(status.resultDbContinuity.rangeEnd)}，应有 ${status.resultDbContinuity.expectedTradeDates} 个交易日，结果库里有 ${status.resultDbContinuity.actualTradeDates} 个。`
                : '结果库为空或没有交易日，暂不检查连续性。'}
            </small>
            {status?.resultDbContinuity?.checked && !status.resultDbContinuity.isContinuous ? (
              <small className="ranking-compute-summary-alert">
                缺失 {status.resultDbContinuity.missingTradeDatesCount} 个交易日
                {status.resultDbContinuity.missingTradeDatesCount > 0
                  ? `：${formatDateSample(status.resultDbContinuity.missingTradeDatesSample)}`
                  : ''}
                {status.resultDbContinuity.unexpectedTradeDatesCount > 0
                  ? `；另有 ${status.resultDbContinuity.unexpectedTradeDatesCount} 个不在交易日列表内的日期：${formatDateSample(status.resultDbContinuity.unexpectedTradeDatesSample)}`
                  : ''}
              </small>
            ) : null}
          </div>
        </div>

        <div className="ranking-compute-form">
          <label className="ranking-compute-field">
            <span>开始日期</span>
            <input type="date" value={startDateInput} onChange={(event) => setStartDateInput(event.target.value)} />
          </label>

          <label className="ranking-compute-field">
            <span>结束日期</span>
            <input type="date" value={endDateInput} onChange={(event) => setEndDateInput(event.target.value)} />
          </label>

          <div className="ranking-compute-actions">
            <button className="ranking-compute-primary-btn" type="button" onClick={() => void onRunCompute()} disabled={isBusy || sourcePath === ''}>
              {busyAction === 'computing' ? '计算中...' : '计算排名'}
            </button>
          </div>
        </div>

        {notice ? <div className="ranking-compute-notice">{notice}</div> : null}
        {error ? <div className="ranking-compute-error">{error}</div> : null}
      </section>
    </div>
  )
}
