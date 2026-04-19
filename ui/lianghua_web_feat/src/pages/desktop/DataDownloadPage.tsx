import { useDeferredValue, useEffect, useMemo, useRef, useState } from 'react'
import { inspectManagedSourceStatus } from '../../apis/managedSource'
import {
  getDataDownloadStatus,
  listenDataDownloadProgress,
  runConceptMostRelatedRepair,

  runDataDownload,
  runMissingStockRepair,
  runThsConceptDownload,
  type DataDownloadProgress,
  type DataDownloadRunResult,
  type DataDownloadStatus,
} from '../../apis/dataDownload'
import DataTaskProgress from '../../shared/DataTaskProgress'
import {
  calcProgressPercent,
  getCurrentObjectText,
  getPhaseStep,
  getProgressCounterText,
  normalizeProgressPhase,
  useAnimatedProgressPercent,
} from '../../shared/dataTaskProgressUtils'
import { readJsonStorage, writeJsonStorage } from '../../shared/storage'
import './css/DataDownloadPage.css'

type BusyAction = 'idle' | 'loading' | 'running'
type TaskSection = 'main' | 'concept'

type DataDownloadDraft = {
  token: string
  startDate: string
  endDate: string
  useTodayEnd: boolean
  threads: number
  retryTimes: number
  limitCallsPerMin: number
  includeTurnover: boolean
  thsConceptRetryEnabled: boolean
  thsConceptRetryTimes: number
  thsConceptRetryIntervalSecs: number
  thsConceptConcurrentEnabled: boolean
  thsConceptWorkerThreads: number
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

function formatPhaseLabel(phase: string | null | undefined) {
  switch (normalizeProgressPhase(phase)) {
    case 'prepare_trade_calendar':
      return '刷新交易日历'
    case 'prepare_stock_list':
      return '刷新股票列表'
    case 'download_bars':
      return '拉取行情'
    case 'download_index_bars':
      return '拉取指数'
    case 'download_pending_trade_dates':
      return '拉取增量'
    case 'validate_pending_trade_dates':
      return '校验增量'
    case 'calc_incremental_indicators':
      return '计算指标'
    case 'recover_failed_stocks':
      return '补拉断点股票'
    case 'retry_failed':
      return '失败重试'
    case 'write_db':
      return '写入数据库'
    case 'prepare_ths_concepts':
      return '准备概念下载'
    case 'fetch_ths_concept':
      return '抓取概念'
    case 'retry_ths_concepts':
    case 'retry_ths_concept':
      return '概念重试'
    case 'write_ths_concepts':
      return '写入概念文件'
    case 'rebuild_concept_performance':
      return '维护概念表现'
    case 'repair_concept_most_related':
      return '补算最相关概念'
    case 'delete_stock_data_indicator_columns':
      return '清空指标列'
    case 'rebuild_stock_data_indicator_columns':
      return '补算指标列'
    case 'done_ths_concepts':
      return '概念下载完成'
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

function getProgressWorkflow(action: string | null | undefined) {
  switch (action) {
    case 'first-download':
      return [
        'prepare_trade_calendar',
        'prepare_stock_list',
        'download_bars',
        'retry_failed',
        'write_db',
        'download_index_bars',
        'rebuild_concept_performance',
        'done',
      ] as string[]
    case 'incremental-download':
      return [
        'prepare_trade_calendar',
        'prepare_stock_list',
        'download_pending_trade_dates',
        'validate_pending_trade_dates',
        'calc_incremental_indicators',
        'recover_failed_stocks',
        'write_db',
        'download_index_bars',
        'rebuild_concept_performance',
        'done',
      ] as string[]
    case 'repair-missing-stocks':
      return ['download_bars', 'retry_failed', 'write_db', 'rebuild_concept_performance', 'done'] as string[]
    case 'download-ths-concepts':
      return [
        'prepare_ths_concepts',
        'fetch_ths_concept',
        'retry_ths_concepts',
        'write_ths_concepts',
        'done_ths_concepts',
      ] as string[]
    case 'rebuild-concept-performance':
      return ['rebuild_concept_performance'] as string[]
    case 'repair-concept-most-related':
      return ['repair_concept_most_related'] as string[]
    default:
      return null
  }
}

function formatDbRange(range: DataDownloadStatus['sourceDb'] | DataDownloadStatus['conceptPerformanceDb'] | null | undefined) {
  if (!range) {
    return '读取中...'
  }

  if (!range.exists) {
    return `${range.fileName} 不存在`
  }

  if (!range.minTradeDate || !range.maxTradeDate) {
    return `${range.tableName} 已存在，但还没有可用交易日`
  }

  return `${formatTradeDate(range.minTradeDate)} 至 ${formatTradeDate(range.maxTradeDate)}`
}

function formatFileRange(
  fileStatus:
    | DataDownloadStatus['tradeCalendar']
    | DataDownloadStatus['stockList']
    | DataDownloadStatus['thsConcepts']
    | null
    | undefined,
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

function formatMissingStockSummary(status: DataDownloadStatus | null) {
  const repair = status?.missingStockRepair
  if (!repair) {
    return '读取中...'
  }

  if (!repair.ready) {
    return repair.detail
  }

  if (repair.missingCount <= 0) {
    return '无缺失股票'
  }

  return `${repair.missingCount} 只待补全`
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
    thsConceptRetryEnabled: true,
    thsConceptRetryTimes: 4,
    thsConceptRetryIntervalSecs: 30,
    thsConceptConcurrentEnabled: false,
    thsConceptWorkerThreads: 4,
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
    thsConceptRetryEnabled:
      typeof parsed.thsConceptRetryEnabled === 'boolean'
        ? parsed.thsConceptRetryEnabled
        : fallback.thsConceptRetryEnabled,
    thsConceptRetryTimes:
      typeof parsed.thsConceptRetryTimes === 'number' && Number.isFinite(parsed.thsConceptRetryTimes)
        ? parsed.thsConceptRetryTimes
        : fallback.thsConceptRetryTimes,
    thsConceptRetryIntervalSecs:
      typeof parsed.thsConceptRetryIntervalSecs === 'number' && Number.isFinite(parsed.thsConceptRetryIntervalSecs)
        ? parsed.thsConceptRetryIntervalSecs
        : fallback.thsConceptRetryIntervalSecs,
    thsConceptConcurrentEnabled:
      typeof parsed.thsConceptConcurrentEnabled === 'boolean'
        ? parsed.thsConceptConcurrentEnabled
        : fallback.thsConceptConcurrentEnabled,
    thsConceptWorkerThreads:
      typeof parsed.thsConceptWorkerThreads === 'number' && Number.isFinite(parsed.thsConceptWorkerThreads)
        ? parsed.thsConceptWorkerThreads
        : fallback.thsConceptWorkerThreads,
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
  const [thsConceptRetryEnabled, setThsConceptRetryEnabled] = useState(draft.thsConceptRetryEnabled)
  const [thsConceptRetryTimes, setThsConceptRetryTimes] = useState(String(draft.thsConceptRetryTimes))
  const [thsConceptRetryIntervalSecs, setThsConceptRetryIntervalSecs] = useState(String(draft.thsConceptRetryIntervalSecs))
  const [thsConceptConcurrentEnabled, setThsConceptConcurrentEnabled] = useState(draft.thsConceptConcurrentEnabled)
  const [thsConceptWorkerThreads, setThsConceptWorkerThreads] = useState(String(draft.thsConceptWorkerThreads))
  const [notice, setNotice] = useState('')
  const [error, setError] = useState('')
  const [progress, setProgress] = useState<DataDownloadProgress | null>(null)
  const [activeTaskSection, setActiveTaskSection] = useState<TaskSection | null>(null)
  const [feedbackSection, setFeedbackSection] = useState<TaskSection>('main')
  const activeDownloadIdRef = useRef('')
  const progressUnlistenRef = useRef<null | (() => void)>(null)

  const sourcePath = status?.sourcePath?.trim() ?? ''
  const isBusy = busyAction !== 'idle'
  const isFirstDownload = status?.plannedAction === 'first-download'
  const latestDbTradeDate = formatTradeDate(status?.sourceDb.maxTradeDate)
  const deferredProgress = useDeferredValue(progress)
  const resolvedIncrementalStartDate =
    inputDateToCompact(startDateInput) || status?.sourceDb.minTradeDate || '20240101'
  const progressPercent = calcProgressPercent(deferredProgress, getProgressWorkflow, ['done', 'done_ths_concepts'])
  const shownProgressPercent = useAnimatedProgressPercent(busyAction === 'running', progressPercent)
  const phaseStep = getPhaseStep(deferredProgress?.action, deferredProgress?.phase, getProgressWorkflow)
  const progressCounterText = getProgressCounterText(deferredProgress, formatPhaseLabel)
  const missingStockRepair = status?.missingStockRepair ?? null
  const showMainProgress = busyAction === 'running' && activeTaskSection === 'main'
  const showConceptProgress = busyAction === 'running' && activeTaskSection === 'concept'
  const showMainNotice = Boolean(notice) && feedbackSection === 'main'
  const showConceptNotice = Boolean(notice) && feedbackSection === 'concept'
  const showMainError = Boolean(error) && feedbackSection === 'main'
  const showConceptError = Boolean(error) && feedbackSection === 'concept'

  useEffect(() => {
    if (busyAction !== 'running') {
      setActiveTaskSection(null)
    }
  }, [busyAction])

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
      thsConceptRetryEnabled,
      thsConceptRetryTimes: Number(thsConceptRetryTimes),
      thsConceptRetryIntervalSecs: Number(thsConceptRetryIntervalSecs),
      thsConceptConcurrentEnabled,
      thsConceptWorkerThreads: Number(thsConceptWorkerThreads),
    })
  }, [
    endDateInput,
    includeTurnover,
    limitCallsPerMin,
    retryTimes,
    startDateInput,
    thsConceptRetryEnabled,
    thsConceptRetryIntervalSecs,
    thsConceptRetryTimes,
    thsConceptConcurrentEnabled,
    thsConceptWorkerThreads,
    threads,
    token,
    useTodayEnd,
  ])

  async function loadStatus() {
    setBusyAction('loading')
    setFeedbackSection('main')
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

  async function runDataTask(
    section: TaskSection,
    executor: (downloadId: string) => Promise<DataDownloadRunResult>,
  ) {
    setBusyAction('running')
    setActiveTaskSection(section)
    setFeedbackSection(section)
    setError('')
    setNotice('')
    setProgress(null)

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
      const result = await executor(downloadId)
      setStatus(result.status)

      if (
        result.action === 'rebuild-concept-performance' ||
        result.action === 'repair-concept-most-related'
      ) {
        setNotice(
          `${result.actionLabel}完成，用时 ${formatElapsedMs(result.elapsedMs)}；写入 ${result.summary.savedRows} 行。`,
        )
      } else if (result.action === 'delete-stock-data-indicator-columns') {
        setNotice(
          `${result.actionLabel}完成，用时 ${formatElapsedMs(result.elapsedMs)}；清空 ${result.summary.successCount} 列，基础行情列已保留。`,
        )
      } else if (result.action === 'rebuild-stock-data-indicator-columns') {
        setNotice(
          `${result.actionLabel}完成，用时 ${formatElapsedMs(result.elapsedMs)}；补算 ${result.summary.successCount} 组，回写 ${result.summary.savedRows} 行。`,
        )
      } else {
        const failedTail =
          result.summary.failedCount > 0
            ? ` 失败 ${result.summary.failedCount} 只，前几项: ${result.summary.failedItems.slice(0, 3).join('；')}`
            : ''
        setNotice(
          `${result.actionLabel}完成，用时 ${formatElapsedMs(result.elapsedMs)}；成功 ${result.summary.successCount} 只，写入 ${result.summary.savedRows} 行。${failedTail}`.trim(),
        )
      }
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

  async function onRunDownload() {
    if (!sourcePath) {
      setFeedbackSection('main')
      setError('当前数据目录为空，请先到数据管理页确认目录。')
      return
    }

    if (!token.trim()) {
      setFeedbackSection('main')
      setError('请先填写 Tushare Token。')
      return
    }

    const startDate = isFirstDownload
      ? inputDateToCompact(startDateInput)
      : resolvedIncrementalStartDate
    if (!startDate) {
      setFeedbackSection('main')
      setError(isFirstDownload ? '请先填写开始日期。' : '请先提供增量补救起点。')
      return
    }

    const endDate = isFirstDownload
      ? (useTodayEnd ? 'today' : inputDateToCompact(endDateInput))
      : 'today'
    if (isFirstDownload && !endDate) {
      setFeedbackSection('main')
      setError('请先填写结束日期，或勾选自动到当前交易日。')
      return
    }

    if (endDate !== 'today' && startDate > endDate) {
      setFeedbackSection('main')
      setError('开始日期不能晚于结束日期。')
      return
    }

    await runDataTask('main', (downloadId) =>
      runDataDownload({
        downloadId,
        sourcePath,
        token: token.trim(),
        startDate,
        endDate,
        threads: Math.max(1, Number(threads) || 1),
        retryTimes: Math.max(0, Number(retryTimes) || 0),
        limitCallsPerMin: Math.max(1, Number(limitCallsPerMin) || 1),
        includeTurnover,
      }),
    )
  }

  async function onRunMissingStockRepair() {
    if (!sourcePath) {
      setFeedbackSection('main')
      setError('当前数据目录为空，请先到数据管理页确认目录。')
      return
    }
    if (!token.trim()) {
      setFeedbackSection('main')
      setError('请先填写 Tushare Token。')
      return
    }
    if (!missingStockRepair?.ready) {
      setFeedbackSection('main')
      setError(missingStockRepair?.detail ?? '当前缺失股票补全不可执行。')
      return
    }
    if ((missingStockRepair?.missingCount ?? 0) <= 0) {
      setFeedbackSection('main')
      setError('当前没有需要补全的缺失股票。')
      return
    }

    await runDataTask('main', (downloadId) =>
      runMissingStockRepair({
        downloadId,
        sourcePath,
        token: token.trim(),
        threads: Math.max(1, Number(threads) || 1),
        retryTimes: Math.max(0, Number(retryTimes) || 0),
        limitCallsPerMin: Math.max(1, Number(limitCallsPerMin) || 1),
        includeTurnover,
      }),
    )
  }

  async function onRunThsConceptDownload() {
    if (!sourcePath) {
      setFeedbackSection('concept')
      setError('当前数据目录为空，请先到数据管理页确认目录。')
      return
    }

    await runDataTask('concept', (downloadId) =>
      runThsConceptDownload({
        downloadId,
        sourcePath,
        retryEnabled: thsConceptRetryEnabled,
        retryTimes: Math.max(0, Number(thsConceptRetryTimes) || 0),
        retryIntervalSecs: Math.max(0, Number(thsConceptRetryIntervalSecs) || 0),
        concurrentEnabled: thsConceptConcurrentEnabled,
        workerThreads: Math.max(1, Number(thsConceptWorkerThreads) || 1),
      }),
    )
  }

  async function onRunConceptMostRelatedRepair() {
    if (!sourcePath) {
      setFeedbackSection('concept')
      setError('当前数据目录为空，请先到数据管理页确认目录。')
      return
    }

    await runDataTask('concept', (downloadId) =>
      runConceptMostRelatedRepair({
        downloadId,
        sourcePath,
      }),
    )
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

          <div className="data-download-head-actions">
            <button
              className="data-download-secondary-btn"
              type="button"
              onClick={() => void onRunMissingStockRepair()}
              disabled={isBusy || !missingStockRepair?.ready || (missingStockRepair?.missingCount ?? 0) <= 0}
              title={missingStockRepair?.detail || undefined}
            >
              {missingStockRepair?.missingCount ? `缺失股票补全 (${missingStockRepair.missingCount})` : '缺失股票补全'}
            </button>
            <button
              className="data-download-secondary-btn"
              type="button"
              onClick={() => void loadStatus()}
              disabled={isBusy}
            >
              {busyAction === 'loading' ? '刷新中...' : '刷新状态'}
            </button>
          </div>
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
              <>
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

                <div className="data-download-hero-config">
                  <div className="data-download-hero-config-head">
                    <strong>增量更新参数</strong>
                    <span>只在少量校验失败股票需要整段重下时使用</span>
                  </div>
                  <label className="data-download-field data-download-hero-field">
                    <span>补救回补起点</span>
                    <input
                      type="date"
                      value={startDateInput}
                      onChange={(event) => setStartDateInput(event.target.value)}
                    />
                    <small>
                      为空时，默认取当前原始库最早日期
                      {status?.sourceDb.minTradeDate ? ` ${formatTradeDate(status.sourceDb.minTradeDate)}` : ''}。
                    </small>
                  </label>
                </div>
              </>
            )}
          </div>

          <div className="data-download-status-stack">
            <div className="data-download-summary">
              <div className="data-download-summary-item">
                <span>原始库状态</span>
                <strong>{formatDbRange(status?.sourceDb)}</strong>
                <small>
                  {status?.sourceDb
                    ? `${status.sourceDb.distinctTradeDates} 个交易日，${status.sourceDb.rowCount} 行`
                    : '读取中...'}
                </small>
              </div>
              <div className="data-download-summary-item">
                <span>概念表现库</span>
                <strong>{formatDbRange(status?.conceptPerformanceDb)}</strong>
                <small>
                  {status?.conceptPerformanceDb
                    ? `${status.conceptPerformanceDb.distinctTradeDates} 个交易日，${status.conceptPerformanceDb.rowCount} 行`
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
              <div className="data-download-summary-item">
                <span>概念文件</span>
                <strong>{formatFileRange(status?.thsConcepts)}</strong>
                <small>
                  {status?.thsConcepts
                    ? `${status.thsConcepts.rowCount} 行`
                    : '读取中...'}
                </small>
              </div>
              <div className="data-download-summary-item">
                <span>缺失股票补全</span>
                <strong>{formatMissingStockSummary(status)}</strong>
                <small>
                  {status?.missingStockRepair
                    ? status.missingStockRepair.detail
                    : '读取中...'}
                </small>
              </div>
            </div>
          </div>
        </div>

        <div className="data-download-panel-grid">
          <section className="data-download-panel">
            <div className="data-download-panel-head">
              <h3>通用参数</h3>
              <p>所有模式共用。Token 只存当前浏览器，不会自动写回配置文件，执行性能参数也统一放在这里。</p>
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

              <div className="data-download-inline-grid data-download-field-span-2">
                <label className="data-download-check data-download-check-compact">
                  <input
                    type="checkbox"
                    checked={includeTurnover}
                    onChange={(event) => setIncludeTurnover(event.target.checked)}
                  />
                  <span>下载换手率</span>
                </label>

                <label className="data-download-field data-download-field-compact">
                  <span>每分钟限频</span>
                  <input
                    type="number"
                    min="1"
                    value={limitCallsPerMin}
                    onChange={(event) => setLimitCallsPerMin(event.target.value)}
                  />
                </label>

                <label className="data-download-field data-download-field-compact">
                  <span>线程数</span>
                  <input
                    type="number"
                    min="1"
                    value={threads}
                    onChange={(event) => setThreads(event.target.value)}
                  />
                </label>

                <label className="data-download-field data-download-field-compact">
                  <span>重试次数</span>
                  <input
                    type="number"
                    min="0"
                    value={retryTimes}
                    onChange={(event) => setRetryTimes(event.target.value)}
                  />
                </label>
              </div>
            </div>
          </section>

          {isFirstDownload ? (
            <section className="data-download-panel">
              <div className="data-download-panel-head">
                <h3>首次下载参数</h3>
                <p>首次下载需要明确历史区间。结束日期可以固定，也可以跟随当前有效交易日。</p>
              </div>

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
            </section>
          ) : null}
        </div>

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
            {showMainProgress ? '下载执行中...' : isBusy ? '任务执行中...' : (status?.plannedActionLabel ?? '开始下载')}
          </button>
        </div>

        {showMainProgress ? (
          <DataTaskProgress
            phaseLabel={formatPhaseLabel(deferredProgress?.phase)}
            phaseStepPillText={phaseStep ? ` · ${phaseStep.current}/${phaseStep.total}` : ''}
            phaseStepStatText={phaseStep ? ` ${phaseStep.current}/${phaseStep.total}` : ''}
            actionLabel={deferredProgress?.actionLabel ?? (status?.plannedActionLabel ?? '下载执行中')}
            progressPercent={progressPercent}
            elapsedText={formatElapsedMs(deferredProgress?.elapsedMs ?? 0)}
            shownProgressPercent={shownProgressPercent}
            progressCounterText={progressCounterText}
            currentObjectText={getCurrentObjectText(deferredProgress)}
            message={deferredProgress?.message}
            fallbackMessage="下载已经启动，正在等待后端返回当前状态。安卓端长时间无响应时，可以先看这里的阶段提示和耗时。"
          />
        ) : null}

        {showMainNotice ? <div className="data-download-notice">{notice}</div> : null}
        {showMainError ? <div className="data-download-error">{error}</div> : null}
      </section>

      <section className="data-download-card">
        <section className="data-download-panel">
          <div className="data-download-panel-head">
            <h3>概念数据下载</h3>
            <p>下载并补全概念相关数据。</p>
          </div>

          <div className="data-download-summary">
            <div className="data-download-summary-item">
              <span>概念文件</span>
              <strong>{formatFileRange(status?.thsConcepts)}</strong>
              <small>
                {status?.thsConcepts
                  ? `${status.thsConcepts.rowCount} 行`
                  : '读取中...'}
              </small>
            </div>
            <div className="data-download-summary-item">
              <span>概念表现库</span>
              <strong>{formatDbRange(status?.conceptPerformanceDb)}</strong>
              <small>
                {status?.conceptPerformanceDb
                  ? `${status.conceptPerformanceDb.distinctTradeDates} 个交易日，${status.conceptPerformanceDb.rowCount} 行`
                  : '读取中...'}
              </small>
            </div>
          </div>

          <div className="data-download-inline-grid">
                <label className="data-download-check data-download-check-compact">
                  <input
                    type="checkbox"
                    checked={thsConceptConcurrentEnabled}
                    onChange={(event) => setThsConceptConcurrentEnabled(event.target.checked)}
                  />
                  <span>并发模式</span>
                </label>

            <label className="data-download-field data-download-field-compact">
                  <span>并发线程数</span>
                  <input
                    type="number"
                    min="1"
                    value={thsConceptWorkerThreads}
                    onChange={(event) => setThsConceptWorkerThreads(event.target.value)}
                    disabled={!thsConceptConcurrentEnabled}
                  />
                </label>

            <label className="data-download-check data-download-check-compact">
              <input
                type="checkbox"
                checked={thsConceptRetryEnabled}
                onChange={(event) => setThsConceptRetryEnabled(event.target.checked)}
              />
              <span>失败后重试</span>
            </label>

            <label className="data-download-field data-download-field-compact">
              <span>重试次数</span>
              <input
                type="number"
                min="0"
                value={thsConceptRetryTimes}
                onChange={(event) => setThsConceptRetryTimes(event.target.value)}
                disabled={!thsConceptRetryEnabled}
              />
            </label>

            <label className="data-download-field data-download-field-compact">
              <span>重试间隔(秒)</span>
              <input
                type="number"
                min="0"
                value={thsConceptRetryIntervalSecs}
                onChange={(event) => setThsConceptRetryIntervalSecs(event.target.value)}
                disabled={!thsConceptRetryEnabled}
              />
            </label>

            <div className="data-download-panel-tip">
              {thsConceptConcurrentEnabled
                ? `当前使用并发抓取，线程数 ${Math.max(1, Number(thsConceptWorkerThreads) || 1)}。任一线程失败后会停止整轮，并按整轮重试逻辑处理。`
                : '当前使用串行抓取。失败后如果开启重试，会按整轮任务重新执行。'}
            </div>
          </div>

          <div className="data-download-panel-actions">
            <button
              className="data-download-secondary-btn"
              type="button"
              onClick={() => void onRunThsConceptDownload()}
              disabled={isBusy || !sourcePath}
            >
              {showConceptProgress ? '概念下载中...' : isBusy ? '任务执行中...' : '开始概念下载'}
            </button>

            <button
              className="data-download-secondary-btn"
              type="button"
              onClick={() => void onRunConceptMostRelatedRepair()}
              disabled={isBusy || !sourcePath}
            >
              {showConceptProgress ? '补算中...' : isBusy ? '任务执行中...' : '最相关概念补算'}
            </button>
          </div>

          {showConceptProgress ? (
            <DataTaskProgress
              phaseLabel={formatPhaseLabel(deferredProgress?.phase)}
              phaseStepPillText={phaseStep ? ` · ${phaseStep.current}/${phaseStep.total}` : ''}
              phaseStepStatText={phaseStep ? ` ${phaseStep.current}/${phaseStep.total}` : ''}
              actionLabel={deferredProgress?.actionLabel ?? '概念数据下载'}
              progressPercent={progressPercent}
              elapsedText={formatElapsedMs(deferredProgress?.elapsedMs ?? 0)}
              shownProgressPercent={shownProgressPercent}
              progressCounterText={progressCounterText}
              currentObjectText={getCurrentObjectText(deferredProgress)}
              message={deferredProgress?.message}
              fallbackMessage="下载已经启动，正在等待后端返回当前状态。安卓端长时间无响应时，可以先看这里的阶段提示和耗时。"
            />
          ) : null}
          {showConceptNotice ? <div className="data-download-notice">{notice}</div> : null}
          {showConceptError ? <div className="data-download-error">{error}</div> : null}
        </section>

      </section>

    </div>
  )
}
