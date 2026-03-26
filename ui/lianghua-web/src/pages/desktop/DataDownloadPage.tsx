import { useDeferredValue, useEffect, useMemo, useRef, useState } from 'react'
import { inspectManagedSourceStatus } from '../../apis/managedSource'
import {
  getDataDownloadStatus,
  listenDataDownloadProgress,
  runDataDownload,
  type DataDownloadProgress,
  type DataDownloadRunResult,
  type DataDownloadStatus,
} from '../../apis/dataDownload'
import { readJsonStorage, writeJsonStorage } from '../../shared/storage'
import './css/DataDownloadPage.css'

type BusyAction = 'idle' | 'loading' | 'running'

type DataDownloadDraft = {
  token: string
  startDate: string
  endDate: string
  useTodayEnd: boolean
  threads: number
  retryTimes: number
  limitCallsPerMin: number
  includeTurnover: boolean
}

const DATA_DOWNLOAD_DRAFT_KEY = 'lh_data_download_draft_v1'

function formatTradeDate(value: string | null | undefined) {
  if (!value) {
    return '--'
  }

  if (/^\d{8}$/.test(value)) {
    return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`
  }

  return value
}

function inputDateToCompact(value: string) {
  return value.replaceAll('-', '').trim()
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

function calcProgressPercent(progress: DataDownloadProgress | null) {
  if (!progress || progress.total <= 0) {
    return null
  }

  return Math.max(0, Math.min(100, Math.round((progress.finished / progress.total) * 100)))
}

function formatPhaseLabel(phase: string | null | undefined) {
  switch (phase) {
    case 'prepare_trade_calendar':
      return '刷新交易日历'
    case 'prepare_stock_list':
      return '刷新股票列表'
    case 'download_bars':
      return '下载行情'
    case 'retry_failed':
      return '失败重试'
    case 'download_pending_trade_dates':
      return '增量下载'
    case 'write_db':
      return '写入数据库'
    case 'done':
      return '下载完成'
    case 'started':
      return '已启动'
    case 'failed':
      return '下载失败'
    default:
      return '运行中'
  }
}

function getPhaseStep(phase: string | null | undefined) {
  switch (phase) {
    case 'prepare_trade_calendar':
      return { current: 1, total: 5 }
    case 'prepare_stock_list':
      return { current: 2, total: 5 }
    case 'download_bars':
    case 'download_pending_trade_dates':
      return { current: 3, total: 5 }
    case 'retry_failed':
      return { current: 4, total: 5 }
    case 'write_db':
      return { current: 5, total: 5 }
    case 'done':
      return { current: 5, total: 5 }
    default:
      return null
  }
}

function formatDbRange(status: DataDownloadStatus | null) {
  const sourceDb = status?.sourceDb
  if (!sourceDb) {
    return '读取中...'
  }

  if (!sourceDb.exists) {
    return 'stock_data.db 不存在'
  }

  if (!sourceDb.minTradeDate || !sourceDb.maxTradeDate) {
    return 'stock_data 已存在，但还没有可用交易日'
  }

  return `${formatTradeDate(sourceDb.minTradeDate)} 至 ${formatTradeDate(sourceDb.maxTradeDate)}`
}

function formatFileRange(
  fileStatus: DataDownloadStatus['tradeCalendar'] | DataDownloadStatus['stockList'] | null | undefined,
) {
  if (!fileStatus) {
    return '读取中...'
  }

  if (!fileStatus.exists) {
    return `${fileStatus.fileName} 不存在`
  }

  if (!fileStatus.minTradeDate && !fileStatus.maxTradeDate) {
    return `${fileStatus.fileName} 已存在`
  }

  if (fileStatus.minTradeDate && fileStatus.maxTradeDate) {
    return `${formatTradeDate(fileStatus.minTradeDate)} 至 ${formatTradeDate(fileStatus.maxTradeDate)}`
  }

  return formatTradeDate(fileStatus.maxTradeDate ?? fileStatus.minTradeDate)
}

function readDraft(): DataDownloadDraft {
  const fallback: DataDownloadDraft = {
    token: '',
    startDate: '2024-01-01',
    endDate: '',
    useTodayEnd: true,
    threads: 4,
    retryTimes: 3,
    limitCallsPerMin: 190,
    includeTurnover: true,
  }

  const parsed = readJsonStorage<Partial<DataDownloadDraft>>(typeof window === 'undefined' ? null : window.localStorage, DATA_DOWNLOAD_DRAFT_KEY)
  if (!parsed) {
    return fallback
  }

  return {
    token: typeof parsed.token === 'string' ? parsed.token : fallback.token,
    startDate: typeof parsed.startDate === 'string' ? parsed.startDate : fallback.startDate,
    endDate: typeof parsed.endDate === 'string' ? parsed.endDate : fallback.endDate,
    useTodayEnd: typeof parsed.useTodayEnd === 'boolean' ? parsed.useTodayEnd : fallback.useTodayEnd,
    threads: typeof parsed.threads === 'number' && Number.isFinite(parsed.threads) ? parsed.threads : fallback.threads,
    retryTimes:
      typeof parsed.retryTimes === 'number' && Number.isFinite(parsed.retryTimes)
        ? parsed.retryTimes
        : fallback.retryTimes,
    limitCallsPerMin:
      typeof parsed.limitCallsPerMin === 'number' && Number.isFinite(parsed.limitCallsPerMin)
        ? parsed.limitCallsPerMin
        : fallback.limitCallsPerMin,
    includeTurnover:
      typeof parsed.includeTurnover === 'boolean'
        ? parsed.includeTurnover
        : fallback.includeTurnover,
  }
}

export default function DataDownloadPage() {
  const draft = useMemo(() => readDraft(), [])
  const [status, setStatus] = useState<DataDownloadStatus | null>(null)
  const [busyAction, setBusyAction] = useState<BusyAction>('loading')
  const [token, setToken] = useState(draft.token)
  const [startDateInput, setStartDateInput] = useState(draft.startDate)
  const [endDateInput, setEndDateInput] = useState(draft.endDate)
  const [useTodayEnd, setUseTodayEnd] = useState(draft.useTodayEnd)
  const [threads, setThreads] = useState(String(draft.threads))
  const [retryTimes, setRetryTimes] = useState(String(draft.retryTimes))
  const [limitCallsPerMin, setLimitCallsPerMin] = useState(String(draft.limitCallsPerMin))
  const [includeTurnover, setIncludeTurnover] = useState(draft.includeTurnover)
  const [notice, setNotice] = useState('')
  const [error, setError] = useState('')
  const [progress, setProgress] = useState<DataDownloadProgress | null>(null)
  const [displayProgressPercent, setDisplayProgressPercent] = useState(0)
  const activeDownloadIdRef = useRef('')
  const progressUnlistenRef = useRef<null | (() => void)>(null)
  const displayProgressPercentRef = useRef(0)

  const sourcePath = status?.sourcePath?.trim() ?? ''
  const isBusy = busyAction !== 'idle'
  const isFirstDownload = status?.plannedAction === 'first-download'
  const latestDbTradeDate = formatTradeDate(status?.sourceDb.maxTradeDate)
  const earliestDbTradeDate = formatTradeDate(status?.sourceDb.minTradeDate)
  const deferredProgress = useDeferredValue(progress)
  const resolvedIncrementalStartDate =
    inputDateToCompact(startDateInput) || status?.sourceDb.minTradeDate || '20240101'
  const progressPercent = calcProgressPercent(deferredProgress)
  const shownProgressPercent = progressPercent === null ? 10 : Math.max(displayProgressPercent, Math.min(progressPercent, 100))
  const phaseStep = getPhaseStep(deferredProgress?.phase)
  const progressCounterText =
    progressPercent === null
      ? '等待后端返回分段进度'
      : `${deferredProgress?.finished ?? 0} / ${deferredProgress?.total ?? 0}`

  useEffect(() => {
    displayProgressPercentRef.current = displayProgressPercent
  }, [displayProgressPercent])

  useEffect(() => {
    if (busyAction !== 'running') {
      displayProgressPercentRef.current = 0
      setDisplayProgressPercent(0)
      return
    }

    if (progressPercent === null) {
      const fallback = Math.max(displayProgressPercentRef.current, 10)
      displayProgressPercentRef.current = fallback
      setDisplayProgressPercent(fallback)
      return
    }

    const from = displayProgressPercentRef.current
    const to = progressPercent
    if (Math.abs(to - from) < 0.5) {
      displayProgressPercentRef.current = to
      setDisplayProgressPercent(to)
      return
    }

    let frameId = 0
    const duration = Math.min(560, Math.max(180, Math.abs(to - from) * 16))
    const startAt = performance.now()

    const tick = (now: number) => {
      const elapsed = now - startAt
      const ratio = Math.min(1, elapsed / duration)
      const eased = 1 - (1 - ratio) * (1 - ratio)
      const nextValue = from + (to - from) * eased
      displayProgressPercentRef.current = nextValue
      setDisplayProgressPercent(nextValue)

      if (ratio < 1) {
        frameId = window.requestAnimationFrame(tick)
      }
    }

    frameId = window.requestAnimationFrame(tick)
    return () => window.cancelAnimationFrame(frameId)
  }, [busyAction, progressPercent])

  useEffect(() => {
    writeJsonStorage(typeof window === 'undefined' ? null : window.localStorage, DATA_DOWNLOAD_DRAFT_KEY, {
      token,
      startDate: startDateInput,
      endDate: endDateInput,
      useTodayEnd,
      threads: Number(threads),
      retryTimes: Number(retryTimes),
      limitCallsPerMin: Number(limitCallsPerMin),
      includeTurnover,
    })
  }, [
    endDateInput,
    includeTurnover,
    limitCallsPerMin,
    retryTimes,
    startDateInput,
    threads,
    token,
    useTodayEnd,
  ])

  async function loadStatus() {
    setBusyAction('loading')
    setError('')

    try {
      const managedStatus = await inspectManagedSourceStatus()
      const nextStatus = await getDataDownloadStatus(managedStatus.sourcePath)
      setStatus(nextStatus)
    } catch (loadError) {
      setNotice('')
      setError(`读取下载状态失败: ${String(loadError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  useEffect(() => {
    void loadStatus()
  }, [])

  useEffect(() => {
    return () => {
      progressUnlistenRef.current?.()
    }
  }, [])

  async function onRunDownload() {
    if (!sourcePath) {
      setError('当前数据目录为空，请先到数据管理页确认目录。')
      return
    }

    if (!token.trim()) {
      setError('请先填写 Tushare Token。')
      return
    }

    const startDate = isFirstDownload
      ? inputDateToCompact(startDateInput)
      : resolvedIncrementalStartDate
    if (!startDate) {
      setError(isFirstDownload ? '请先填写开始日期。' : '请先提供增量补救起点。')
      return
    }

    const endDate = isFirstDownload
      ? (useTodayEnd ? 'today' : inputDateToCompact(endDateInput))
      : 'today'
    if (isFirstDownload && !endDate) {
      setError('请先填写结束日期，或勾选自动到当前交易日。')
      return
    }

    if (endDate !== 'today' && startDate > endDate) {
      setError('开始日期不能晚于结束日期。')
      return
    }

    setBusyAction('running')
    setError('')
    setNotice('')
    setProgress(null)
    setDisplayProgressPercent(0)
    displayProgressPercentRef.current = 0

    const downloadId = `download-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    activeDownloadIdRef.current = downloadId
    progressUnlistenRef.current?.()
    progressUnlistenRef.current = await listenDataDownloadProgress(downloadId, (nextProgress) => {
      if (activeDownloadIdRef.current !== downloadId) {
        return
      }
      setProgress((prev) => {
        if (
          prev &&
          prev.phase === nextProgress.phase &&
          prev.elapsedMs === nextProgress.elapsedMs &&
          prev.finished === nextProgress.finished &&
          prev.total === nextProgress.total &&
          prev.currentLabel === nextProgress.currentLabel &&
          prev.message === nextProgress.message
        ) {
          return prev
        }
        return nextProgress
      })
    })

    try {
      const result: DataDownloadRunResult = await runDataDownload({
        downloadId,
        sourcePath,
        token: token.trim(),
        startDate,
        endDate,
        threads: Math.max(1, Number(threads) || 1),
        retryTimes: Math.max(0, Number(retryTimes) || 0),
        limitCallsPerMin: Math.max(1, Number(limitCallsPerMin) || 1),
        includeTurnover,
      })

      setStatus(result.status)

      const failedTail =
        result.summary.failedCount > 0
          ? ` 失败 ${result.summary.failedCount} 只，前几项: ${result.summary.failedItems.slice(0, 3).join('；')}`
          : ''
      setNotice(
        `${result.actionLabel}完成，用时 ${formatElapsedMs(result.elapsedMs)}；成功 ${result.summary.successCount} 只，写入 ${result.summary.savedRows} 行。${failedTail}`.trim(),
      )
    } catch (runError) {
      setNotice('')
      setError(`执行下载失败: ${String(runError)}`)
    } finally {
      progressUnlistenRef.current?.()
      progressUnlistenRef.current = null
      activeDownloadIdRef.current = ''
      setBusyAction('idle')
    }
  }

  return (
    <div className="data-download-page">
      <section className="data-download-card">
        <div className="data-download-head">
          <div>
            <h2>数据下载</h2>
            <p>
              在当前应用数据目录里直接执行原始行情下载。Token 和下载参数会缓存在当前浏览器，页面会根据当前原始库状态自动切换为首次下载或增量更新表单。
            </p>
          </div>

          <button
            className="data-download-secondary-btn"
            type="button"
            onClick={() => void loadStatus()}
            disabled={isBusy}
          >
            {busyAction === 'loading' ? '刷新中...' : '刷新状态'}
          </button>
        </div>

        <div className="data-download-top-grid">
          <div className="data-download-action-card data-download-action-card-hero">
            <div className="data-download-action-head">
              <div>
                <span>即将执行</span>
                <strong>{status?.plannedActionLabel ?? '读取中...'}</strong>
              </div>
              <span className={isFirstDownload ? 'data-download-mode-pill' : 'data-download-mode-pill is-incremental'}>
                {isFirstDownload ? '首次模式' : '增量模式'}
              </span>
            </div>
            <p>{status?.plannedActionDetail ?? '正在读取当前原始库状态...'}</p>

            {isFirstDownload ? (
              <div className="data-download-hero-metrics">
                <div className="data-download-hero-metric">
                  <span>下载区间</span>
                  <strong>自定义起止日期</strong>
                </div>
                <div className="data-download-hero-metric">
                  <span>结束方式</span>
                  <strong>{useTodayEnd ? '自动跟随 today' : '使用你填写的结束日期'}</strong>
                </div>
              </div>
            ) : (
              <div className="data-download-hero-metrics">
                <div className="data-download-hero-metric">
                  <span>当前原始库最新日期</span>
                  <strong>{latestDbTradeDate}</strong>
                </div>
                <div className="data-download-hero-metric">
                  <span>本次结束方式</span>
                  <strong>固定更新到当前有效交易日</strong>
                </div>
              </div>
            )}
          </div>

          <div className="data-download-status-stack">
            <div className="data-download-summary-item data-download-summary-item-path">
              <span>当前数据目录</span>
              <strong title={sourcePath}>{sourcePath || '读取中...'}</strong>
            </div>
            <div className="data-download-summary">
              <div className="data-download-summary-item">
                <span>原始库状态</span>
                <strong>{formatDbRange(status)}</strong>
                <small>
                  {status?.sourceDb
                    ? `${status.sourceDb.distinctTradeDates} 个交易日，${status.sourceDb.rowCount} 行`
                    : '读取中...'}
                </small>
              </div>
              <div className="data-download-summary-item">
                <span>交易日历</span>
                <strong>{formatFileRange(status?.tradeCalendar)}</strong>
                <small>
                  {status?.tradeCalendar
                    ? `${status.tradeCalendar.rowCount} 行`
                    : '读取中...'}
                </small>
              </div>
              <div className="data-download-summary-item">
                <span>股票列表</span>
                <strong>{formatFileRange(status?.stockList)}</strong>
                <small>
                  {status?.stockList
                    ? `${status.stockList.rowCount} 行`
                    : '读取中...'}
                </small>
              </div>
            </div>
          </div>
        </div>

        <div className="data-download-panel-grid">
          <section className="data-download-panel">
            <div className="data-download-panel-head">
              <h3>通用配置</h3>
              <p>所有模式都需要。Token 只存当前浏览器，不会自动写回配置文件。</p>
            </div>

            <div className="data-download-form-grid">
              <label className="data-download-field data-download-field-span-2">
                <span>Tushare Token</span>
                <input
                  type="password"
                  value={token}
                  onChange={(event) => setToken(event.target.value)}
                  placeholder="输入后缓存在当前浏览器 localStorage"
                />
                <small>如果切浏览器或清缓存，需要重新填写。</small>
              </label>

              <label className="data-download-check data-download-check-span-2">
                <input
                  type="checkbox"
                  checked={includeTurnover}
                  onChange={(event) => setIncludeTurnover(event.target.checked)}
                />
                <span>下载换手率与量比字段，并在写库时一并保存</span>
              </label>
            </div>
          </section>

          <section className="data-download-panel">
            <div className="data-download-panel-head">
              <h3>{isFirstDownload ? '首次下载参数' : '增量更新参数'}</h3>
              <p>
                {isFirstDownload
                  ? '首次下载需要明确历史区间。结束日期可以固定，也可以跟随当前有效交易日。'
                  : '增量更新默认更新到当前有效交易日，不再要求你填写结束日期。'}
              </p>
            </div>

            {isFirstDownload ? (
              <div className="data-download-form-grid">
                <label className="data-download-field">
                  <span>开始日期</span>
                  <input
                    type="date"
                    value={startDateInput}
                    onChange={(event) => setStartDateInput(event.target.value)}
                  />
                </label>

                <label className="data-download-field">
                  <span>结束日期</span>
                  <input
                    type="date"
                    value={endDateInput}
                    onChange={(event) => setEndDateInput(event.target.value)}
                    disabled={useTodayEnd}
                  />
                  <small>{useTodayEnd ? '已切换为 today' : '不勾选时会使用这里的自定义日期'}</small>
                </label>

                <label className="data-download-check data-download-check-span-2">
                  <input
                    type="checkbox"
                    checked={useTodayEnd}
                    onChange={(event) => setUseTodayEnd(event.target.checked)}
                  />
                  <span>结束日期使用当前有效交易日（today）</span>
                </label>
              </div>
            ) : (
              <div className="data-download-incremental-box">
                <div className="data-download-incremental-summary">
                  <div className="data-download-incremental-item">
                    <span>当前原始库区间</span>
                    <strong>
                      {earliestDbTradeDate} 至 {latestDbTradeDate}
                    </strong>
                  </div>
                  <div className="data-download-incremental-item">
                    <span>本次更新终点</span>
                    <strong>当前有效交易日（today）</strong>
                  </div>
                </div>

                <label className="data-download-field">
                  <span>补救回补起点</span>
                  <input
                    type="date"
                    value={startDateInput}
                    onChange={(event) => setStartDateInput(event.target.value)}
                  />
                  <small>
                    只在少量校验失败股票需要整段重下时使用。为空时，默认取当前原始库最早日期
                    {status?.sourceDb.minTradeDate ? ` ${formatTradeDate(status.sourceDb.minTradeDate)}` : ''}。
                  </small>
                </label>
              </div>
            )}
          </section>
        </div>

        <section className="data-download-panel">
          <div className="data-download-panel-head">
            <h3>执行性能</h3>
            <p>这些参数会影响下载速度与请求节奏，首次下载和增量更新共用这一组。</p>
          </div>

          <div className="data-download-form-row">
            <label className="data-download-field">
              <span>线程数</span>
              <input
                type="number"
                min="1"
                value={threads}
                onChange={(event) => setThreads(event.target.value)}
              />
            </label>

            <label className="data-download-field">
              <span>重试次数</span>
              <input
                type="number"
                min="0"
                value={retryTimes}
                onChange={(event) => setRetryTimes(event.target.value)}
              />
            </label>

            <label className="data-download-field">
              <span>每分钟限频</span>
              <input
                type="number"
                min="1"
                value={limitCallsPerMin}
                onChange={(event) => setLimitCallsPerMin(event.target.value)}
              />
            </label>
          </div>
        </section>

        <div className="data-download-runbar">
          <div className="data-download-runbar-copy">
            <strong>{status?.plannedActionLabel ?? '开始下载'}</strong>
            <span className="data-download-actions-note">
              当前下载流程固定使用前复权 qfq。
            </span>
          </div>
          <button
            className="data-download-primary-btn"
            type="button"
            onClick={() => void onRunDownload()}
            disabled={isBusy || !sourcePath}
          >
            {busyAction === 'running' ? '下载执行中...' : (status?.plannedActionLabel ?? '开始下载')}
          </button>
        </div>

        {busyAction === 'running' ? (
          <div className="data-download-progress">
            <div className="data-download-progress-head">
              <div className="data-download-progress-title">
                <span className="data-download-progress-phase-pill">
                  {formatPhaseLabel(deferredProgress?.phase)}
                  {phaseStep ? ` · ${phaseStep.current}/${phaseStep.total}` : ''}
                </span>
                <strong>{deferredProgress?.actionLabel ?? status?.plannedActionLabel ?? '下载执行中'}</strong>
              </div>
              <div className="data-download-progress-value">
                <strong>{progressPercent === null ? '--' : `${progressPercent}%`}</strong>
                <span>{formatElapsedMs(deferredProgress?.elapsedMs ?? 0)}</span>
              </div>
            </div>
            <div className="data-download-progress-bar">
              <div
                className={`data-download-progress-bar-fill ${progressPercent === null ? 'is-indeterminate' : ''}`}
                style={{ width: `${Math.max(shownProgressPercent, 10)}%` }}
              />
            </div>
            <div className="data-download-progress-stats">
              <div className="data-download-progress-stat">
                <span>阶段</span>
                <strong>
                  {formatPhaseLabel(deferredProgress?.phase)}
                  {phaseStep ? ` ${phaseStep.current}/${phaseStep.total}` : ''}
                </strong>
              </div>
              <div className="data-download-progress-stat">
                <span>进度</span>
                <strong>{progressCounterText}</strong>
              </div>
              <div className="data-download-progress-stat data-download-progress-stat-wide">
                <span>当前对象</span>
                <strong>{deferredProgress?.currentLabel ?? '等待后端分派任务'}</strong>
              </div>
            </div>
            <div className="data-download-progress-text">
              {deferredProgress?.message ?? '下载已经启动，正在等待后端返回当前状态。安卓端长时间无响应时，可以先看这里的阶段提示和耗时。'}
            </div>
          </div>
        ) : null}

        {notice ? <div className="data-download-notice">{notice}</div> : null}
        {error ? <div className="data-download-error">{error}</div> : null}
      </section>
    </div>
  )
}
