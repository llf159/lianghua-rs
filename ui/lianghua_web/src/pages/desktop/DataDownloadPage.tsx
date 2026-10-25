import { forwardRef, useEffect, useImperativeHandle, useMemo, useRef, useState } from 'react'
import { inspectManagedSourceStatus } from '../../apis/managedSource'
import {
  DATA_DOWNLOAD_DRAFT_STORAGE_KEY,
  checkDatabases,
  repairDatabaseIssues,
  type DatabaseCheckReport,
  type DatabaseIssue,
  getDataDownloadStatus,
  listenDataDownloadProgress,
  runConceptMostRelatedRepair,
  runDataDownload,
  runDragonTigerDownload,
  runMissingStockRepair,
  runThsConceptDownload,
  type DataDownloadProgress,
  type DataDownloadRunResult,
  type DataDownloadStatus,
} from '../../apis/dataDownload'
import DataTaskProgress from '../../shared/DataTaskProgress'
import DatabaseIntegrityPanel from './components/DatabaseIntegrityPanel'
import { runRankingScoreCalculation } from '../../apis/rankingCompute'
import {
  calcProgressPercent,
  formatProgressDuration,
  getCurrentObjectText,
  getPhaseStep,
  getProgressCounterText,
  getProgressSegments,
  getProgressTaskLabel,
  normalizeProgressPhase,
  type ProgressWorkflowStep,
  useAnimatedProgressPercent,
  useProgressTiming,
} from '../../shared/dataTaskProgressUtils'
import ConfirmDialog from '../../shared/ConfirmDialog'
import { readStoredDetailCyqModel } from '../../shared/chartSettings'
import { readJsonStorage, writeJsonStorage } from '../../shared/storage'
import './css/DataDownloadPage.css'

type BusyAction = 'idle' | 'loading' | 'running' | 'checking'
type TaskSection = 'main' | 'dragonTiger' | 'concept'

type DataDownloadDraft = {
  token: string
  startDate: string
  endDate: string
  useTodayEnd: boolean
  threads: number
  retryTimes: number
  limitCallsPerMin: number
  includeTurnover: boolean
  dragonTigerStartDate: string
  dragonTigerEndDate: string
  dragonTigerUseTodayEnd: boolean
  thsConceptRetryEnabled: boolean
  thsConceptRetryTimes: number
  thsConceptRetryIntervalSecs: number
  thsConceptConcurrentEnabled: boolean
  thsConceptWorkerThreads: number
}

const STALE_STOCK_LIST_CONFIRM_REQUIRED_PREFIX = 'STALE_STOCK_LIST_CONFIRM_REQUIRED:'

function parseStaleStockListConfirmMessage(error: unknown) {
  const message = String(error)
  const markerIndex = message.indexOf(STALE_STOCK_LIST_CONFIRM_REQUIRED_PREFIX)
  if (markerIndex < 0) {
    return null
  }

  return message.slice(markerIndex + STALE_STOCK_LIST_CONFIRM_REQUIRED_PREFIX.length).trim()
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
    case 'prepare_index_download':
      return '准备指数'
    case 'write_index_db':
      return '写入指数'
    case 'index_download_done':
      return '指数阶段完成'
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
    case 'download_dragon_tiger':
      return '拉取龙虎榜'
    case 'write_dragon_tiger':
      return '写入龙虎榜库'
    case 'dragon_tiger_done':
      return '龙虎榜下载完成'
    case 'rebuild_incremental_concept_performance':
      return '维护本轮概念表现'
    case 'rebuild_concept_performance':
      return '维护概念表现'
    case 'stock_download_done':
      return '行情阶段完成'
    case 'panel_indicators':
      return '市场/横截面指标'
    case 'rebuild_cyq_history':
      return '重建筹码历史'
    case 'repair_cyq_history':
      return '修复筹码历史'
    case 'maintain_cyq_incremental':
      return '维护筹码增量'
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
    case 'repair_stock_history': return '修复股票历史'
    case 'repair_index_history': return '修复指数历史'
    case 'audit_recheck': return '重新检查修复计划'
    case 'score_prepare': return '准备评分'
    case 'score_compute': return '计算评分'
    case 'score_write': return '提交评分及排名'
    case 'prepare': return '准备相似度数据'
    case 'select-templates': return '筛选历史模板'
    case 'candidate-fingerprints': return '构建历史模板指纹'
    case 'target-fingerprints': return '构建当日股票指纹'
    case 'candidate-index': return '构建候选索引'
    case 'ranking': return '计算相似度排名'
    case 'validate': return '校验相似度结果'
    case 'write': return '提交相似度结果'
    case 'read-result': return '读取相似度结果'
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
    case 'repair-database':
      return [{ key: 'database_repair', label: '针对性修复', phases: [['audit_recheck', 'repair_stock_history', 'repair_index_history', 'rebuild_concept_performance']] }] as ProgressWorkflowStep[]
    case 'score':
      return ['score_prepare', 'score_compute', 'score_write'] as ProgressWorkflowStep[]
    case 'first-download':
      return [
        {
          key: 'stock_download',
          label: '股票行情',
          phases: [
            'prepare_trade_calendar',
            'prepare_stock_list',
            ['download_bars', 'retry_failed', 'write_db'],
            ['rebuild_incremental_concept_performance', 'stock_download_done'],
          ],
        },
        {
          key: 'index_download',
          label: '指数行情',
          phases: [
            'prepare_index_download',
            ['download_index_bars', 'write_index_db', 'index_download_done'],
          ],
        },
        {
          key: 'market_indicators',
          label: '市场指标',
          phases: ['panel_indicators'],
        },
      ] as ProgressWorkflowStep[]
    case 'incremental-download':
      return [
        {
          key: 'stock_incremental',
          label: '股票增量',
          phases: [
            'prepare_trade_calendar',
            'prepare_stock_list',
            'download_pending_trade_dates',
            'validate_pending_trade_dates',
            'calc_incremental_indicators',
            'write_db',
            'recover_failed_stocks',
            ['rebuild_incremental_concept_performance', 'stock_download_done'],
          ],
        },
        {
          key: 'index_incremental',
          label: '指数行情',
          phases: [
            'prepare_index_download',
            ['download_index_bars', 'write_index_db', 'index_download_done'],
          ],
        },
        {
          key: 'market_indicators',
          label: '市场指标',
          phases: ['panel_indicators'],
        },
        {
          key: 'chip_history',
          label: '筹码历史维护',
          phases: [['rebuild_cyq_history', 'repair_cyq_history']],
        },
        {
          key: 'chip_incremental',
          label: '筹码增量维护',
          phases: ['maintain_cyq_incremental'],
        },
      ] as ProgressWorkflowStep[]
    case 'repair-missing-stocks':
      return [
        { key: 'repair_prepare', label: '准备补救', phases: ['prepare_trade_calendar', 'prepare_stock_list'] },
        {
          key: 'stock_repair',
          label: '补全股票行情',
          phases: [['download_bars', 'retry_failed', 'write_db']],
        },
        {
          key: 'market_indicators',
          label: '市场指标',
          phases: ['panel_indicators'],
        },
        'rebuild_concept_performance',
      ] as ProgressWorkflowStep[]
    case 'download-ths-concepts':
      return [
        {
          key: 'concept_download',
          label: '概念同步',
          phases: [['prepare_ths_concepts', 'fetch_ths_concept', 'retry_ths_concepts', 'write_ths_concepts']],
        },
      ] as ProgressWorkflowStep[]
    case 'download-dragon-tiger':
      return [
        {
          key: 'dragon_tiger_download',
          label: '龙虎榜同步',
          phases: [['download_dragon_tiger', 'write_dragon_tiger']],
        },
      ] as ProgressWorkflowStep[]
    case 'rebuild-concept-performance':
      return ['rebuild_concept_performance'] as ProgressWorkflowStep[]
    case 'repair-concept-most-related':
      return ['repair_concept_most_related'] as ProgressWorkflowStep[]
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

function formatCompletionDetailTail(details: string[] | null | undefined) {
  const normalized = (details ?? []).map((item) => item.trim()).filter(Boolean)
  return normalized.length > 0 ? `；${normalized.join('；')}` : ''
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

function formatCyqChenMaintenanceSummary(status: DataDownloadStatus | null) {
  const maintenance = status?.cyqChenMaintenance
  if (!maintenance) {
    return '读取中...'
  }

  if (!maintenance.dbExists) {
    return '未发现新筹码库'
  }

  if (!maintenance.hasData) {
    return '新筹码库暂无数据'
  }

  return maintenance.strategyChanged ? '策略已变化' : '策略一致'
}

function formatDragonTigerSummary(status: DataDownloadStatus | null) {
  const dragonTiger = status?.dragonTigerDb
  if (!dragonTiger) {
    return '读取中...'
  }
  if (!dragonTiger.exists) {
    return 'dragon_tiger.db 不存在'
  }
  if (!dragonTiger.minTradeDate || !dragonTiger.maxTradeDate) {
    return '龙虎榜库已创建，暂无同步日期'
  }
  return `${formatTradeDate(dragonTiger.minTradeDate)} 至 ${formatTradeDate(dragonTiger.maxTradeDate)}`
}

function readDraft(): DataDownloadDraft {
  const fallback: DataDownloadDraft = {
    token: '',
    startDate: '2024-01-01',
    endDate: '',
    useTodayEnd: true,
    threads: 16,
    retryTimes: 3,
    limitCallsPerMin: 190,
    includeTurnover: true,
    dragonTigerStartDate: '2005-01-01',
    dragonTigerEndDate: '',
    dragonTigerUseTodayEnd: true,
    thsConceptRetryEnabled: true,
    thsConceptRetryTimes: 4,
    thsConceptRetryIntervalSecs: 30,
    thsConceptConcurrentEnabled: false,
    thsConceptWorkerThreads: 4,
  }

  const parsed = readJsonStorage<Partial<DataDownloadDraft>>(typeof window === 'undefined' ? null : window.localStorage, DATA_DOWNLOAD_DRAFT_STORAGE_KEY)
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
    dragonTigerStartDate:
      typeof parsed.dragonTigerStartDate === 'string'
        ? parsed.dragonTigerStartDate
        : fallback.dragonTigerStartDate,
    dragonTigerEndDate:
      typeof parsed.dragonTigerEndDate === 'string'
        ? parsed.dragonTigerEndDate
        : fallback.dragonTigerEndDate,
    dragonTigerUseTodayEnd:
      typeof parsed.dragonTigerUseTodayEnd === 'boolean'
        ? parsed.dragonTigerUseTodayEnd
        : fallback.dragonTigerUseTodayEnd,
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

type DataDownloadPageProps = {
  mergedMode?: boolean
  onMainTaskComplete?: () => void
  sharedToken?: string
  onSharedTokenChange?: (token: string) => void
  hideSharedTokenSection?: boolean
}

export type DailyDownloadOutcome = {
  completed: boolean
  latestTradeDate: string | null
  chipStrategySkipped: boolean
}

export type DataDownloadPageHandle = {
  runDailyIncrementalUpdate: () => Promise<DailyDownloadOutcome>
}

const DataDownloadPage = forwardRef<DataDownloadPageHandle, DataDownloadPageProps>(function DataDownloadPage(
  {
    mergedMode = false,
    onMainTaskComplete,
    sharedToken,
    onSharedTokenChange,
    hideSharedTokenSection = false,
  },
  ref,
) {
  const draft = useMemo(() => readDraft(), [])
  const [status, setStatus] = useState<DataDownloadStatus | null>(null)
  const [auditReport, setAuditReport] = useState<DatabaseCheckReport | null>(null)
  const [auditError, setAuditError] = useState('')
  const [auditRepairConfirm, setAuditRepairConfirm] = useState<DatabaseIssue[] | null>(null)
  const [busyAction, setBusyAction] = useState<BusyAction>('loading')
  const [localToken, setLocalToken] = useState(draft.token)
  const [startDateInput, setStartDateInput] = useState(draft.startDate)
  const [endDateInput, setEndDateInput] = useState(draft.endDate)
  const [useTodayEnd, setUseTodayEnd] = useState(draft.useTodayEnd)
  const [threads, setThreads] = useState(String(draft.threads))
  const [retryTimes, setRetryTimes] = useState(String(draft.retryTimes))
  const [limitCallsPerMin, setLimitCallsPerMin] = useState(String(draft.limitCallsPerMin))
  const [includeTurnover, setIncludeTurnover] = useState(draft.includeTurnover)
  const [dragonTigerStartDate, setDragonTigerStartDate] = useState(draft.dragonTigerStartDate)
  const [dragonTigerEndDate, setDragonTigerEndDate] = useState(draft.dragonTigerEndDate)
  const [dragonTigerUseTodayEnd, setDragonTigerUseTodayEnd] = useState(draft.dragonTigerUseTodayEnd)
  const [thsConceptRetryEnabled, setThsConceptRetryEnabled] = useState(draft.thsConceptRetryEnabled)
  const [thsConceptRetryTimes, setThsConceptRetryTimes] = useState(String(draft.thsConceptRetryTimes))
  const [thsConceptRetryIntervalSecs, setThsConceptRetryIntervalSecs] = useState(String(draft.thsConceptRetryIntervalSecs))
  const [thsConceptConcurrentEnabled, setThsConceptConcurrentEnabled] = useState(draft.thsConceptConcurrentEnabled)
  const [thsConceptWorkerThreads, setThsConceptWorkerThreads] = useState(String(draft.thsConceptWorkerThreads))
  const [notice, setNotice] = useState('')
  const [error, setError] = useState('')
  const [staleStockListConfirmMessage, setStaleStockListConfirmMessage] = useState('')
  const [cyqChenStrategyConfirm, setCyqChenStrategyConfirm] = useState<{
    allowStaleStockList: boolean
    message: string
  } | null>(null)
  const [progress, setProgress] = useState<DataDownloadProgress | null>(null)
  const [progressPercent, setProgressPercent] = useState<number | null>(null)
  const [activeTaskSection, setActiveTaskSection] = useState<TaskSection | null>(null)
  const [feedbackSection, setFeedbackSection] = useState<TaskSection>('main')
  const activeDownloadIdRef = useRef('')
  const progressUnlistenRef = useRef<null | (() => void)>(null)
  const busyActionRef = useRef(busyAction)
  busyActionRef.current = busyAction
  const token = sharedToken ?? localToken

  function setToken(nextToken: string) {
    setLocalToken(nextToken)
    onSharedTokenChange?.(nextToken)
  }

  const sourcePath = status?.sourcePath?.trim() ?? ''
  const isBusy = busyAction !== 'idle'
  const isFirstDownload = status?.plannedAction === 'first-download'
  const latestDbTradeDate = formatTradeDate(status?.sourceDb.maxTradeDate)
  const progressTiming = useProgressTiming(busyAction === 'running', progress, getProgressWorkflow)
  const resolvedIncrementalStartDate =
    status?.sourceDb.minTradeDate || status?.missingStockRepair.suggestedStartDate || ''
  const shownProgressPercent = useAnimatedProgressPercent(busyAction === 'running', progressPercent)
  const phaseStep = getPhaseStep(progress?.action, progress?.phase, getProgressWorkflow)
  const phaseLabel = getProgressTaskLabel(
    progress?.action,
    progress?.phase,
    getProgressWorkflow,
    formatPhaseLabel,
  )
  const progressCounterText = getProgressCounterText(progress, formatPhaseLabel)
  const progressSegments = getProgressSegments(
    progress?.action,
    progress?.phase,
    getProgressWorkflow,
    ['done', 'done_ths_concepts', 'dragon_tiger_done'],
    formatPhaseLabel,
  )
  const missingStockRepair = status?.missingStockRepair ?? null
  const showMainProgress = busyAction === 'running' && activeTaskSection === 'main'
  const showDragonTigerProgress = busyAction === 'running' && activeTaskSection === 'dragonTiger'
  const showConceptProgress = busyAction === 'running' && activeTaskSection === 'concept'
  const showMainNotice = Boolean(notice) && feedbackSection === 'main'
  const showDragonTigerNotice = Boolean(notice) && feedbackSection === 'dragonTiger'
  const showConceptNotice = Boolean(notice) && feedbackSection === 'concept'
  const showMainError = Boolean(error) && feedbackSection === 'main'
  const showDragonTigerError = Boolean(error) && feedbackSection === 'dragonTiger'
  const showConceptError = Boolean(error) && feedbackSection === 'concept'

  useEffect(() => {
    if (busyAction !== 'running') {
      setActiveTaskSection(null)
    }
  }, [busyAction])

  useEffect(() => {
    writeJsonStorage(typeof window === 'undefined' ? null : window.localStorage, DATA_DOWNLOAD_DRAFT_STORAGE_KEY, {
      token,
      startDate: startDateInput,
      endDate: endDateInput,
      useTodayEnd,
      threads: Number(threads),
      retryTimes: Number(retryTimes),
      limitCallsPerMin: Number(limitCallsPerMin),
      includeTurnover,
      dragonTigerStartDate,
      dragonTigerEndDate,
      dragonTigerUseTodayEnd,
      thsConceptRetryEnabled,
      thsConceptRetryTimes: Number(thsConceptRetryTimes),
      thsConceptRetryIntervalSecs: Number(thsConceptRetryIntervalSecs),
      thsConceptConcurrentEnabled,
      thsConceptWorkerThreads: Number(thsConceptWorkerThreads),
    })
  }, [
    endDateInput,
    dragonTigerEndDate,
    dragonTigerStartDate,
    dragonTigerUseTodayEnd,
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
    busyActionRef.current = 'running'
    setAuditReport(null)
    setActiveTaskSection(section)
    setFeedbackSection(section)
    setError('')
    setNotice('')
    setStaleStockListConfirmMessage('')
    setCyqChenStrategyConfirm(null)
    setProgress(null)
    setProgressPercent(null)

    const downloadId = `download-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    activeDownloadIdRef.current = downloadId
    progressUnlistenRef.current?.()
    progressUnlistenRef.current = await listenDataDownloadProgress(downloadId, (nextProgress) => {
      if (activeDownloadIdRef.current !== downloadId) {
        return
      }
      const nextPercent = calcProgressPercent(nextProgress, getProgressWorkflow, [
        'done',
        'done_ths_concepts',
        'dragon_tiger_done',
      ])
      // 重试准备、并行回调和未知总量不能把已确认的总进度倒退。
      setProgressPercent((previous) =>
        nextPercent === null ? previous : Math.max(previous ?? 0, nextPercent),
      )
      setProgress((prev) => {
        if (
          prev &&
          (nextProgress.elapsedMs < prev.elapsedMs ||
            (prev.phase === nextProgress.phase && prev.total === nextProgress.total &&
              nextProgress.finished < prev.finished))
        ) {
          return prev
        }
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
      if (section === 'main') {
        onMainTaskComplete?.()
      }

      if (
        result.action === 'rebuild-concept-performance' ||
        result.action === 'repair-concept-most-related'
      ) {
        setNotice(
          `${result.actionLabel}完成，用时 ${formatElapsedMs(result.elapsedMs)}；写入 ${result.summary.savedRows} 行${formatCompletionDetailTail(result.completionDetails)}。`,
        )
      } else if (result.action === 'repair-database') {
        setNotice(`${result.actionLabel}完成，用时 ${formatElapsedMs(result.elapsedMs)}${formatCompletionDetailTail(result.completionDetails)}。`)
      } else if (result.action === 'delete-stock-data-indicator-columns') {
        setNotice(
          `${result.actionLabel}完成，用时 ${formatElapsedMs(result.elapsedMs)}；清空 ${result.summary.successCount} 列，基础行情列已保留${formatCompletionDetailTail(result.completionDetails)}。`,
        )
      } else if (result.action === 'rebuild-stock-data-indicator-columns') {
        setNotice(
          `${result.actionLabel}完成，用时 ${formatElapsedMs(result.elapsedMs)}；补算 ${result.summary.successCount} 组，回写 ${result.summary.savedRows} 行${formatCompletionDetailTail(result.completionDetails)}。`,
        )
      } else if (result.action === 'download-dragon-tiger') {
        setNotice(
          `${result.actionLabel}完成，用时 ${formatElapsedMs(result.elapsedMs)}；同步 ${result.summary.successCount} 个交易日，写入 ${result.summary.savedRows} 行${formatCompletionDetailTail(result.completionDetails)}。`,
        )
      } else {
        const failedDetail =
          result.summary.failedCount > 0
            ? `失败 ${result.summary.failedCount} 只，前几项: ${result.summary.failedItems.slice(0, 3).join('；')}`
            : ''
        const detailTail = formatCompletionDetailTail([...result.completionDetails, failedDetail].filter(Boolean))
        setNotice(
          `${result.actionLabel}完成，用时 ${formatElapsedMs(result.elapsedMs)}；成功 ${result.summary.successCount} 只，写入 ${result.summary.savedRows} 行${detailTail}。`,
        )
      }
      return result
    } catch (runError) {
      const staleStockListMessage = section === 'main' ? parseStaleStockListConfirmMessage(runError) : null
      if (staleStockListMessage) {
        setNotice('')
        setError('')
        setStaleStockListConfirmMessage(staleStockListMessage)
        return null
      }

      setNotice('')
      setError(`执行下载失败: ${String(runError)}`)
      try {
        setStatus(await getDataDownloadStatus(sourcePath))
      } catch (statusError) {
        console.warn('下载失败后刷新待补全状态失败', statusError)
      }
      return null
    } finally {
      progressUnlistenRef.current?.()
      progressUnlistenRef.current = null
      activeDownloadIdRef.current = ''
      setBusyAction('idle')
    }
  }

  async function onRunDownload(
    allowStaleStockList = false,
    allowCyqChenStrategyRebuild?: boolean,
    skipCyqChenStrategyConfirm = false,
  ) {
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

    const chipModel = readStoredDetailCyqModel()
    if (
      status?.plannedAction === 'incremental-download' &&
      chipModel === 'chen' &&
      status.cyqChenMaintenance.strategyChanged &&
      allowCyqChenStrategyRebuild === undefined &&
      !skipCyqChenStrategyConfirm
    ) {
      setFeedbackSection('main')
      setError('')
      setNotice('')
      setCyqChenStrategyConfirm({
        allowStaleStockList,
        message:
          `${status.cyqChenMaintenance.detail}\n\n` +
          '继续后会在本轮下载完成前重建整个新筹码库；选择跳过则本轮只下载行情，不维护新筹码库。',
      })
      return
    }

    return runDataTask('main', (downloadId) =>
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
        allowStaleStockList,
        allowCyqChenStrategyRebuild: allowCyqChenStrategyRebuild ?? false,
        chipModel,
      }),
    )
  }

  useImperativeHandle(ref, () => ({
    async runDailyIncrementalUpdate() {
      if (busyActionRef.current !== 'idle') {
        throw new Error('下载页当前有任务正在执行。')
      }
      if (status?.plannedAction !== 'incremental-download') {
        throw new Error('每日工作流仅适用于已有行情库的增量更新；请先完成首次下载。')
      }

      const chipStrategySkipped =
        readStoredDetailCyqModel() === 'chen' && status.cyqChenMaintenance.strategyChanged
      const result = await onRunDownload(false, false, true)
      return {
        completed: Boolean(
          result &&
          result.summary.failedCount === 0 &&
          result.status.missingStockRepair.missingCount === 0,
        ),
        latestTradeDate: result?.status.sourceDb.maxTradeDate ?? null,
        chipStrategySkipped,
      }
    },
  }))

  async function onCheckDatabases() {
    if (!sourcePath || busyActionRef.current !== 'idle') return
    busyActionRef.current = 'checking'
    setBusyAction('checking')
    setAuditError('')
    setAuditReport(null)
    try {
      setAuditReport(await checkDatabases(sourcePath))
    } catch (checkError) {
      setAuditError(`数据库检查失败：${String(checkError)}`)
    } finally {
      busyActionRef.current = 'idle'
      setBusyAction('idle')
    }
  }

  async function onRepairDatabaseIssues(issues: DatabaseIssue[]) {
    setAuditRepairConfirm(null)
    if (busyActionRef.current !== 'idle' || !issues.length) return
    const action = issues[0].repairAction
    if (['stock', 'index'].includes(action ?? '') && !token.trim()) {
      setAuditError('请先填写上方共用的 Tushare Token。')
      return
    }
    const startDate = issues[0].startDate
    const endDate = issues[0].endDate
    await runDataTask('main', async (downloadId) => {
      // 重算前重新检查，避免使用已经过期的检查结果。
      if (action === 'ranking') {
        const current = await checkDatabases(sourcePath)
        if (!current.sourceReady) throw new Error('原始行情检查未通过，请先修复行情。')
        if (!startDate || !endDate) throw new Error('缺少需要重算的历史区间。')
        const currentIssues = issues.map((issue) => {
          const currentIssue = current.issues.find((item) => item.id === issue.id)
          if (!currentIssue) throw new Error('检查结果已变化，请重新检查。')
          return currentIssue
        })
        const rebuildStart = currentIssues.map((item) => item.startDate ?? startDate).sort()[0]
        const rebuildEnd = currentIssues.map((item) => item.endDate ?? endDate).sort().at(-1) ?? endDate
        const result = await runRankingScoreCalculation(sourcePath, rebuildStart, rebuildEnd, undefined, downloadId)
        return {
          action: 'repair-database', actionLabel: '评分／排名修复',
          elapsedMs: result.elapsedMs, summary: { successCount: 1, failedCount: 0, savedRows: 0, conceptPerformanceRows: 0, failedItems: [] },
          completionDetails: ['按当前生效策略重算', ...result.warnings], status: await getDataDownloadStatus(sourcePath),
        }
      }
      return repairDatabaseIssues(downloadId, {
        sourcePath, issueIds: issues.map((issue) => issue.id), token: token.trim(),
        threads: Math.max(1, Number(threads) || 1), retryTimes: Math.max(0, Number(retryTimes) || 0),
        limitCallsPerMin: Math.max(1, Number(limitCallsPerMin) || 1), includeTurnover,
      })
    })
    busyActionRef.current = 'idle'
    await onCheckDatabases()
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

  async function onRunDragonTigerDownload() {
    if (!sourcePath) {
      setFeedbackSection('dragonTiger')
      setError('当前数据目录为空，请先到数据管理页确认目录。')
      return
    }
    if (!token.trim()) {
      setFeedbackSection('dragonTiger')
      setError('请先在通用参数中填写 Tushare Token。')
      return
    }

    const startDate = inputDateToCompact(dragonTigerStartDate)
    const endDate = dragonTigerUseTodayEnd ? 'today' : inputDateToCompact(dragonTigerEndDate)
    if (!startDate) {
      setFeedbackSection('dragonTiger')
      setError('请填写龙虎榜下载开始日期。')
      return
    }
    if (!endDate) {
      setFeedbackSection('dragonTiger')
      setError('请填写龙虎榜下载结束日期，或勾选自动到当前交易日。')
      return
    }
    if (endDate !== 'today' && startDate > endDate) {
      setFeedbackSection('dragonTiger')
      setError('龙虎榜下载开始日期不能晚于结束日期。')
      return
    }

    await runDataTask('dragonTiger', (downloadId) =>
      runDragonTigerDownload({
        downloadId,
        sourcePath,
        token: token.trim(),
        startDate,
        endDate,
        retryTimes: Math.max(0, Number(retryTimes) || 0),
        limitCallsPerMin: Math.max(1, Number(limitCallsPerMin) || 1),
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
    <div className={mergedMode ? 'data-download-page is-merged-mode' : 'data-download-page'}>
      {!hideSharedTokenSection ? <section className="data-download-card data-download-shared-params">
        <div className="data-download-shared-params-copy">
          <span className="data-download-shared-params-label">公共参数</span>
          <div>
            <h2>下载任务 Token</h2>
            <p>行情下载、缺失股票补全和龙虎榜下载共用；概念下载不需要 Token。</p>
          </div>
        </div>

        <label className="data-download-field data-download-shared-token">
          <span>Tushare Token</span>
          <input
            type="password"
            value={token}
            onChange={(event) => setToken(event.target.value)}
            placeholder="输入后缓存在当前浏览器 localStorage"
          />
        </label>
      </section> : null}

      <section className="data-download-card">
        <div className="data-download-head">
          <div>
            <h2>{mergedMode ? '行情数据下载' : '数据下载'}</h2>
            <p>
              下载或增量更新行情数据，并按当前数据状态选择合适的参数表单。
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

            {!mergedMode ? (
              isFirstDownload ? (
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
              )
            ) : null}
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
              <div className="data-download-summary-item">
                <span>新筹码维护</span>
                <strong>{formatCyqChenMaintenanceSummary(status)}</strong>
                <small>
                  {status?.cyqChenMaintenance
                    ? status.cyqChenMaintenance.detail
                    : '读取中...'}
                </small>
              </div>
            </div>
          </div>
        </div>

        <div className="data-download-panel-grid">
          <section className="data-download-panel">
            <div className="data-download-panel-head">
              <h3>行情下载参数</h3>
            </div>

            <div className="data-download-form-grid">
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

                {!isFirstDownload ? (
                  <label className="data-download-field data-download-field-compact">
                    <span>补救回补起点</span>
                    <input
                      type="date"
                      value={resolvedIncrementalStartDate ? formatTradeDate(resolvedIncrementalStartDate) : ''}
                      readOnly
                    />
                    <small>
                      自动使用当前原始库最早日期
                      {status?.sourceDb.minTradeDate ? ` ${formatTradeDate(status.sourceDb.minTradeDate)}` : ''}。
                    </small>
                  </label>
                ) : null}
              </div>
            </div>
          </section>

          {isFirstDownload ? (
            <section className="data-download-panel">
              <div className="data-download-panel-head">
                <h3>首次下载参数</h3>
                <p>设置历史区间，结束日期可固定或跟随当前有效交易日。</p>
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
            phaseLabel={phaseLabel}
            phaseStepPillText={phaseStep ? ` · ${phaseStep.current}/${phaseStep.total}` : ''}
            phaseStepStatText={phaseStep ? ` ${phaseStep.current}/${phaseStep.total}` : ''}
            actionLabel={progress?.actionLabel ?? (status?.plannedActionLabel ?? '下载执行中')}
            progressPercent={progressPercent}
            progressSegments={progressSegments}
            elapsedText={formatElapsedMs(progressTiming.elapsedMs)}
            shownProgressPercent={shownProgressPercent}
            progressCounterText={progressCounterText}
            estimatedRemainingText={formatProgressDuration(progressTiming.estimatedRemainingMs)}
            estimatedTotalText={formatProgressDuration(progressTiming.estimatedTotalMs)}
            message={progress?.message}
            fallbackMessage="下载已经启动，正在等待后端返回当前状态。安卓端长时间无响应时，可以先看这里的阶段提示和耗时。"
          />
        ) : null}

        {showMainNotice ? <div className="data-download-notice">{notice}</div> : null}
        {showMainError ? <div className="data-download-error">{error}</div> : null}
      </section>

      <DatabaseIntegrityPanel
        report={auditReport?.sourcePath === sourcePath ? auditReport : null}
        busy={isBusy}
        checking={busyAction === 'checking'}
        error={auditError}
        onCheck={() => void onCheckDatabases()}
        onRepair={(issues) => setAuditRepairConfirm(issues)}
      />
      <ConfirmDialog
        open={Boolean(auditRepairConfirm)}
        title="执行针对性修复"
        message={auditRepairConfirm?.[0].repairAction === 'stock'
          ? `将按所选问题重拉 ${new Set(auditRepairConfirm?.map((issue) => issue.tsCode)).size} 只股票的完整历史并重算指标。异常旧行情会先删除并保存待补回记录，失败后可继续修复；行业、筹码和排名在行情检查通过后另行重算。`
          : '将按当前生效策略和检查给出的区间修复所选数据库，成功提交后自动复查。走势相似度请在计算页按日重算。'}
        confirmText="开始修复"
        busy={isBusy}
        onConfirm={() => auditRepairConfirm && void onRepairDatabaseIssues(auditRepairConfirm)}
        onCancel={() => setAuditRepairConfirm(null)}
      />

      <section className="data-download-card">
        <section className="data-download-panel">
          <div className="data-download-panel-head">
            <h3>龙虎榜数据下载</h3>
            <p>
              同步龙虎榜每日明细和营业部/机构席位明细，写入独立的 dragon_tiger.db。已经同步的交易日会自动跳过。
            </p>
          </div>

          <div className="data-download-dragon-status">
            <strong>{formatDragonTigerSummary(status)}</strong>
            <span>
              {status?.dragonTigerDb
                ? `${status.dragonTigerDb.syncedTradeDates} 个交易日 · 每日明细 ${status.dragonTigerDb.topListRows} 行 · 席位明细 ${status.dragonTigerDb.topInstRows} 行`
                : '正在读取龙虎榜库状态'}
            </span>
          </div>

          <div className="data-download-dragon-row">
            <label className="data-download-field data-download-field-compact">
              <span>开始日期</span>
              <input
                type="date"
                value={dragonTigerStartDate}
                onChange={(event) => setDragonTigerStartDate(event.target.value)}
              />
            </label>

            <label className="data-download-field data-download-field-compact">
              <span>结束日期</span>
              <input
                type="date"
                value={dragonTigerEndDate}
                onChange={(event) => setDragonTigerEndDate(event.target.value)}
                disabled={dragonTigerUseTodayEnd}
              />
            </label>

            <label className="data-download-check data-download-check-compact data-download-dragon-today">
              <input
                type="checkbox"
                checked={dragonTigerUseTodayEnd}
                onChange={(event) => setDragonTigerUseTodayEnd(event.target.checked)}
              />
              <span>自动到今天</span>
            </label>

            <button
              className="data-download-secondary-btn"
              type="button"
              onClick={() => void onRunDragonTigerDownload()}
              disabled={isBusy || !sourcePath}
            >
              {showDragonTigerProgress ? '龙虎榜下载中...' : isBusy ? '任务执行中...' : '开始龙虎榜下载'}
            </button>
          </div>

          {showDragonTigerProgress ? (
            <DataTaskProgress
              phaseLabel={phaseLabel}
              phaseStepPillText={phaseStep ? ` · ${phaseStep.current}/${phaseStep.total}` : ''}
              phaseStepStatText={phaseStep ? ` ${phaseStep.current}/${phaseStep.total}` : ''}
              actionLabel={progress?.actionLabel ?? '龙虎榜数据下载'}
              progressPercent={progressPercent}
              progressSegments={progressSegments}
              elapsedText={formatElapsedMs(progressTiming.elapsedMs)}
              shownProgressPercent={shownProgressPercent}
              progressCounterText={progressCounterText}
              currentObjectText={getCurrentObjectText(progress)}
              message={progress?.message}
              fallbackMessage="龙虎榜下载已经启动，正在等待后端返回当前交易日进度。"
            />
          ) : null}
          {showDragonTigerNotice ? <div className="data-download-notice">{notice}</div> : null}
          {showDragonTigerError ? <div className="data-download-error">{error}</div> : null}
        </section>
      </section>

      <section className="data-download-card">
        <section className="data-download-panel">
          <div className="data-download-panel-head">
            <h3>概念数据下载</h3>
          </div>

          {!mergedMode ? (
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
          ) : null}

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
                : '当前使用串行抓取。失败后如果开启重试，会从上次断点继续。'}
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
              phaseLabel={phaseLabel}
              phaseStepPillText={phaseStep ? ` · ${phaseStep.current}/${phaseStep.total}` : ''}
              phaseStepStatText={phaseStep ? ` ${phaseStep.current}/${phaseStep.total}` : ''}
              actionLabel={progress?.actionLabel ?? '概念数据下载'}
              progressPercent={progressPercent}
              progressSegments={progressSegments}
              elapsedText={formatElapsedMs(progressTiming.elapsedMs)}
              shownProgressPercent={shownProgressPercent}
              progressCounterText={progressCounterText}
              currentObjectText={getCurrentObjectText(progress)}
              message={progress?.message}
              fallbackMessage="下载已经启动，正在等待后端返回当前状态。安卓端长时间无响应时，可以先看这里的阶段提示和耗时。"
            />
          ) : null}
          {showConceptNotice ? <div className="data-download-notice">{notice}</div> : null}
          {showConceptError ? <div className="data-download-error">{error}</div> : null}
        </section>

      </section>

      <ConfirmDialog
        open={Boolean(staleStockListConfirmMessage)}
        title="沿用现有股票列表？"
        message={staleStockListConfirmMessage}
        confirmText="继续下载"
        cancelText="取消"
        busy={isBusy}
        onConfirm={() => {
          setStaleStockListConfirmMessage('')
          void onRunDownload(true)
        }}
        onCancel={() => {
          setStaleStockListConfirmMessage('')
        }}
      />

      <ConfirmDialog
        open={cyqChenStrategyConfirm !== null}
        title="筹码策略已变化"
        message={cyqChenStrategyConfirm?.message}
        confirmText="继续并重建"
        cancelText="跳过筹码维护"
        busy={isBusy}
        onConfirm={() => {
          const pending = cyqChenStrategyConfirm
          setCyqChenStrategyConfirm(null)
          if (pending) {
            void onRunDownload(pending.allowStaleStockList, true)
          }
        }}
        onCancel={() => {
          const pending = cyqChenStrategyConfirm
          setCyqChenStrategyConfirm(null)
          if (pending) {
            void onRunDownload(pending.allowStaleStockList, false)
          }
        }}
      />

    </div>
  )
})

export default DataDownloadPage
