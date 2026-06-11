import { useDeferredValue, useEffect, useRef, useState } from 'react'
import type { UnlistenFn } from '@tauri-apps/api/event'
import { inspectManagedSourceStatus, removeManagedSourceFile } from '../../apis/managedSource'
import {
  getDataDownloadStatus,
  getIndicatorManagePage,
  listenDataDownloadProgress,
  runStockDataIndicatorColumnsDelete,
  runStockDataIndicatorColumnsRebuild,
  saveIndicatorManagePage,
  type DataDownloadProgress,
  type DataDownloadRunResult,
  type DataDownloadStatus,
  type IndicatorManageDraft,
  type IndicatorManageItem,
  type IndicatorManagePageData,
} from '../../apis/dataDownload'
import {
  getRankingComputeStatus,
  previewRankingScoreCalculationWarnings,
  runConceptPerformanceCompute,
  runCyqChenCompute,
  runCyqCompute,
  runRankingScoreCalculation,
  type RankComputeDbRange,
  type RankComputeResultContinuity,
  type RankingComputeStatus,
} from '../../apis/rankingCompute'
import {
  getCyqChenStrategyBackupDiff,
  getCyqChenStrategyPage,
  type CyqChenStrategyBackupDiff,
} from '../../apis/cyqChen'
import {
  getManagedStrategyAssetsStatus,
  getManagedStrategyBackupDiff,
  type ManagedStrategyBackupDiff,
} from '../../apis/strategyAssets'
import DataTaskProgress from '../../shared/DataTaskProgress'
import {
  calcProgressPercent,
  getCurrentObjectText,
  getPhaseStep,
  getProgressCounterText,
  normalizeProgressPhase,
  useAnimatedProgressPercent,
} from '../../shared/dataTaskProgressUtils'
import ConfirmDialog from '../../shared/ConfirmDialog'
import './css/DataDownloadPage.css'
import './css/RankingComputePage.css'

type BusyAction =
  | 'idle'
  | 'loading'
  | 'computing'
  | 'cyq-computing'
  | 'cyq-chen-computing'
  | 'deleting-result-db'
  | 'deleting-cyq-chen-db'
  | 'indicator-running'
type IndicatorEditorMode = 'create' | 'edit'
type FeedbackSlot = 'status' | 'rank' | 'otherData' | 'cyq' | 'cyqChen' | 'indicatorTask'
type CardFeedback = Record<FeedbackSlot, { notice: string; error: string }>
type PendingConfirmState =
  | { kind: 'delete-indicator'; item: IndicatorManageItem }
  | { kind: 'delete-stock-indicator-columns' }
  | { kind: 'delete-result-db' }
  | { kind: 'delete-cyq-chen-db' }
  | null

function createEmptyCardFeedback(): CardFeedback {
  return {
    status: { notice: '', error: '' },
    rank: { notice: '', error: '' },
    otherData: { notice: '', error: '' },
    cyq: { notice: '', error: '' },
    cyqChen: { notice: '', error: '' },
    indicatorTask: { notice: '', error: '' },
  }
}

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

function describeDownloadFileRange(
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

function describeMissingStockSummary(status: DataDownloadStatus | null) {
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

function describeCyqChenMaintenanceSummary(status: DataDownloadStatus | null) {
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

function buildEmptyIndicatorDraft(): IndicatorManageDraft {
  return {
    name: '',
    expr: '',
    prec: 2,
  }
}

function normalizeIndicatorPrecInput(raw: string, fallback: number) {
  const trimmed = raw.trim()
  if (trimmed === '') {
    return fallback
  }

  if (!/^\d+$/.test(trimmed)) {
    return fallback
  }

  return Math.max(0, Number(trimmed))
}

function normalizeCyqFactorInput(raw: string) {
  const trimmed = raw.trim()
  if (trimmed === '') {
    return 50
  }
  if (!/^\d+$/.test(trimmed)) {
    return 50
  }
  return Math.max(2, Number(trimmed))
}

function formatPhaseLabel(phase: string | null | undefined) {
  switch (normalizeProgressPhase(phase)) {
    case 'delete_stock_data_indicator_columns':
      return '清空指标列'
    case 'rebuild_stock_data_indicator_columns':
      return '补算指标列'
    case 'compute_cyq':
      return '筹码计算'
    case 'compute_cyq_chen':
      return '新筹码计算'
    case 'done':
      return '任务完成'
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
    case 'delete-stock-data-indicator-columns':
      return ['delete_stock_data_indicator_columns'] as string[]
    case 'rebuild-stock-data-indicator-columns':
      return ['rebuild_stock_data_indicator_columns'] as string[]
    case 'cyq':
      return ['compute_cyq'] as string[]
    case 'cyq-chen':
      return ['compute_cyq_chen'] as string[]
    default:
      return null
  }
}

type RankingComputePageProps = {
  mergedMode?: boolean
}

export default function RankingComputePage({ mergedMode = false }: RankingComputePageProps) {
  const [status, setStatus] = useState<RankingComputeStatus | null>(null)
  const [downloadStatus, setDownloadStatus] = useState<DataDownloadStatus | null>(null)
  const [busyAction, setBusyAction] = useState<BusyAction>('loading')
  const [startDateInput, setStartDateInput] = useState('')
  const [endDateInput, setEndDateInput] = useState('')
  const [cyqFactorInput, setCyqFactorInput] = useState('50')
  const [cyqStartDateInput, setCyqStartDateInput] = useState('')
  const [cyqEndDateInput, setCyqEndDateInput] = useState('')
  const [cyqChenWarmupDaysInput, setCyqChenWarmupDaysInput] = useState('120')
  const [cyqChenBucketPctInput, setCyqChenBucketPctInput] = useState('1')
  const [cyqChenStartDateInput, setCyqChenStartDateInput] = useState('')
  const [cyqChenEndDateInput, setCyqChenEndDateInput] = useState('')
  const [cardFeedback, setCardFeedback] = useState<CardFeedback>(() => createEmptyCardFeedback())
  const [progress, setProgress] = useState<DataDownloadProgress | null>(null)
  const [strategyDiff, setStrategyDiff] = useState<ManagedStrategyBackupDiff | null>(null)
  const [strategyDiffLoading, setStrategyDiffLoading] = useState(false)
  const [cyqChenStrategyDiff, setCyqChenStrategyDiff] = useState<CyqChenStrategyBackupDiff | null>(null)
  const [cyqChenStrategyDiffLoading, setCyqChenStrategyDiffLoading] = useState(false)

  const [indicatorModalOpen, setIndicatorModalOpen] = useState(false)
  const [indicatorItems, setIndicatorItems] = useState<IndicatorManageItem[]>([])
  const [indicatorFilePath, setIndicatorFilePath] = useState('')
  const [indicatorExists, setIndicatorExists] = useState(false)
  const [indicatorLoading, setIndicatorLoading] = useState(false)
  const [indicatorSaving, setIndicatorSaving] = useState(false)
  const [indicatorError, setIndicatorError] = useState('')
  const [indicatorNotice, setIndicatorNotice] = useState('')
  const [indicatorEditorMode, setIndicatorEditorMode] = useState<IndicatorEditorMode | null>(null)
  const [indicatorDraft, setIndicatorDraft] = useState<IndicatorManageDraft | null>(null)
  const [indicatorEditingName, setIndicatorEditingName] = useState<string | null>(null)
  const [pendingConfirm, setPendingConfirm] = useState<PendingConfirmState>(null)

  const activeDownloadIdRef = useRef('')
  const progressUnlistenRef = useRef<UnlistenFn | null>(null)

  const sourcePath = status?.sourcePath?.trim() ?? ''
  const isBusy = busyAction !== 'idle'
  const showIndicatorProgress = busyAction === 'indicator-running'
  const showComputeProgress =
    busyAction === 'indicator-running' ||
    busyAction === 'cyq-computing' ||
    busyAction === 'cyq-chen-computing'
  const deferredProgress = useDeferredValue(progress)
  const progressPercent = calcProgressPercent(deferredProgress, getProgressWorkflow, ['done'])
  const shownProgressPercent = useAnimatedProgressPercent(showComputeProgress, progressPercent)
  const phaseStep = getPhaseStep(deferredProgress?.action, deferredProgress?.phase, getProgressWorkflow)
  const progressCounterText = getProgressCounterText(deferredProgress, formatPhaseLabel)

  function setFeedbackNotice(slot: FeedbackSlot, message: string) {
    setCardFeedback((current) => ({
      ...current,
      [slot]: { notice: message, error: '' },
    }))
  }

  function setFeedbackError(slot: FeedbackSlot, message: string) {
    setCardFeedback((current) => ({
      ...current,
      [slot]: { notice: '', error: message },
    }))
  }

  function clearFeedback(slot: FeedbackSlot) {
    setCardFeedback((current) => ({
      ...current,
      [slot]: { notice: '', error: '' },
    }))
  }

  function clearAllFeedback() {
    setCardFeedback(createEmptyCardFeedback())
  }

  function renderCardFeedback(slot: FeedbackSlot) {
    const feedback = cardFeedback[slot]
    return (
      <>
        {feedback.notice ? <div className="ranking-compute-notice">{feedback.notice}</div> : null}
        {feedback.error ? <div className="ranking-compute-error">{feedback.error}</div> : null}
      </>
    )
  }

  function applyIndicatorPage(page: IndicatorManagePageData) {
    setIndicatorItems(page.items)
    setIndicatorFilePath(page.filePath)
    setIndicatorExists(page.exists)
  }

  function clearIndicatorEditor() {
    setIndicatorEditorMode(null)
    setIndicatorDraft(null)
    setIndicatorEditingName(null)
  }

  async function startProgressListener(prefix: string) {
    const downloadId = `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
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
    return downloadId
  }

  function stopProgressListener() {
    progressUnlistenRef.current?.()
    progressUnlistenRef.current = null
    activeDownloadIdRef.current = ''
  }

  function updateIndicatorDraftPrec(rawValue: string) {
    setIndicatorDraft((current) =>
      current
        ? {
            ...current,
            prec: normalizeIndicatorPrecInput(rawValue, 2),
          }
        : current,
    )
  }

  function onCreateIndicator() {
    setIndicatorEditorMode('create')
    setIndicatorDraft(buildEmptyIndicatorDraft())
    setIndicatorEditingName(null)
    setIndicatorNotice('')
    setIndicatorError('')
  }

  function onEditIndicator(item: IndicatorManageItem) {
    setIndicatorEditorMode('edit')
    setIndicatorDraft({
      name: item.name,
      expr: item.expr,
      prec: item.prec,
    })
    setIndicatorEditingName(item.name)
    setIndicatorNotice('')
    setIndicatorError('')
  }

  async function persistIndicatorItems(nextItems: IndicatorManageDraft[], successMessage: string) {
    if (!sourcePath) {
      setIndicatorError('当前数据目录为空，无法保存指标配置。')
      return
    }

    setIndicatorSaving(true)
    setIndicatorError('')
    setIndicatorNotice('')
    try {
      const page = await saveIndicatorManagePage(sourcePath, nextItems)
      applyIndicatorPage(page)
      clearIndicatorEditor()
      setIndicatorNotice(successMessage)
    } catch (saveError) {
      setIndicatorError(`保存指标配置失败: ${String(saveError)}`)
    } finally {
      setIndicatorSaving(false)
    }
  }

  async function onSaveIndicatorDraft() {
    if (!indicatorDraft) {
      return
    }

    const nextDraft: IndicatorManageDraft = {
      name: indicatorDraft.name.trim(),
      expr: indicatorDraft.expr.trim(),
      prec: Math.max(0, Number(indicatorDraft.prec) || 0),
    }

    const currentItems = indicatorItems.map((item) => ({
      name: item.name,
      expr: item.expr,
      prec: item.prec,
    }))

    const nextItems =
      indicatorEditorMode === 'edit' && indicatorEditingName
        ? currentItems.map((item) =>
            item.name === indicatorEditingName ? nextDraft : item,
          )
        : [...currentItems, nextDraft]

    await persistIndicatorItems(
      nextItems,
      indicatorEditorMode === 'edit' ? `已保存指标：${nextDraft.name}` : `已新增指标：${nextDraft.name}`,
    )
  }

  async function onDeleteIndicator(item: IndicatorManageItem) {
    const nextItems = indicatorItems
      .filter((current) => current.name !== item.name)
      .map((current) => ({
        name: current.name,
        expr: current.expr,
        prec: current.prec,
      }))

    await persistIndicatorItems(nextItems, `已删除指标：${item.name}`)
  }

  function renderIndicatorEditorCard(title: string) {
    if (!indicatorDraft) {
      return null
    }

    return (
      <section className="data-download-indicator-editor-card">
        <div className="data-download-indicator-editor-head">
          <div>
            <h4>{title}</h4>
            <p>只保留名称、表达式和精度三个字段。</p>
          </div>
          <button
            className="data-download-secondary-btn"
            type="button"
            onClick={clearIndicatorEditor}
            disabled={indicatorSaving}
          >
            取消
          </button>
        </div>

        <div className="data-download-indicator-form-grid data-download-indicator-form-grid-stacked">
          <label className="data-download-field">
            <span>指标名</span>
            <input
              type="text"
              value={indicatorDraft.name}
              onChange={(event) =>
                setIndicatorDraft((current) =>
                  current ? { ...current, name: event.target.value } : current,
                )
              }
              placeholder="例如 MA10"
            />
          </label>

          <label className="data-download-field">
            <span>精度</span>
            <input
              type="number"
              min="0"
              step="1"
              value={indicatorDraft.prec}
              onChange={(event) => updateIndicatorDraftPrec(event.target.value)}
            />
          </label>

          <label className="data-download-field data-download-field-span-2">
            <span>表达式</span>
            <textarea
              className="data-download-indicator-editor"
              value={indicatorDraft.expr}
              onChange={(event) =>
                setIndicatorDraft((current) =>
                  current ? { ...current, expr: event.target.value } : current,
                )
              }
              spellCheck={false}
              placeholder={'REFV := REF(V, 1);\nDIV(V, REFV);'}
            />
          </label>
        </div>

        <div className="data-download-modal-actions">
          <button
            className="data-download-primary-btn"
            type="button"
            onClick={() => void onSaveIndicatorDraft()}
            disabled={indicatorSaving}
          >
            {indicatorSaving ? '保存中...' : indicatorEditorMode === 'edit' ? '保存指标' : '新增指标'}
          </button>
        </div>
      </section>
    )
  }

  function renderProgressCard(fallbackMessage: string) {
    return (
      <DataTaskProgress
        phaseLabel={formatPhaseLabel(deferredProgress?.phase)}
        phaseStepPillText={phaseStep ? ` · ${phaseStep.current}/${phaseStep.total}` : ''}
        phaseStepStatText={phaseStep ? ` ${phaseStep.current}/${phaseStep.total}` : ''}
        actionLabel={deferredProgress?.actionLabel ?? '数据任务'}
        progressPercent={progressPercent}
        elapsedText={formatElapsedMs(deferredProgress?.elapsedMs ?? 0)}
        shownProgressPercent={shownProgressPercent}
        progressCounterText={progressCounterText}
        currentObjectText={getCurrentObjectText(deferredProgress)}
        message={deferredProgress?.message}
        fallbackMessage={fallbackMessage}
      />
    )
  }

  async function loadStatus(options?: { preserveNotice?: boolean }) {
    const preserveNotice = options?.preserveNotice === true
    setBusyAction('loading')
    clearFeedback('status')

    try {
      const managedStatus = await inspectManagedSourceStatus()
      const nextStatus = await getRankingComputeStatus(managedStatus.sourcePath)
      let nextDownloadStatus: DataDownloadStatus | null = null
      let downloadStatusError = ''

      if (mergedMode) {
        try {
          nextDownloadStatus = await getDataDownloadStatus(managedStatus.sourcePath)
        } catch (loadError) {
          downloadStatusError = `读取下载检查失败: ${String(loadError)}`
        }
      }

      setStatus(nextStatus)
      setDownloadStatus(nextDownloadStatus)
      if (!preserveNotice) {
        clearAllFeedback()
      }
      if (downloadStatusError) {
        setFeedbackError('status', downloadStatusError)
      }

      setStartDateInput((current) => current || compactDateToInput(nextStatus.suggestedStartDate))
      setEndDateInput((current) => current || compactDateToInput(nextStatus.suggestedEndDate))
      setCyqFactorInput((current) =>
        current.trim() !== '' ? current : String(nextStatus.cyqFactor ?? 50),
      )
      setCyqEndDateInput((current) => current || compactDateToInput(nextStatus.sourceDb.maxTradeDate))
      setCyqChenWarmupDaysInput((current) =>
        current.trim() !== '' ? current : String(nextStatus.cyqChenWarmupDays ?? 120),
      )
      setCyqChenBucketPctInput((current) =>
        current.trim() !== '' ? current : String(nextStatus.cyqChenBucketPct ?? 1),
      )
      setCyqChenEndDateInput((current) => current || compactDateToInput(nextStatus.sourceDb.maxTradeDate))
    } catch (loadError) {
      setFeedbackError('status', `读取数据计算状态失败: ${String(loadError)}`)
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

  async function openIndicatorManager() {
    if (!sourcePath) {
      setFeedbackError('indicatorTask', '当前数据目录为空，请先到数据管理页确认目录。')
      return
    }

    setIndicatorModalOpen(true)
    setIndicatorLoading(true)
    setIndicatorError('')
    setIndicatorNotice('')
    try {
      const page = await getIndicatorManagePage(sourcePath)
      applyIndicatorPage(page)
      clearIndicatorEditor()
    } catch (loadError) {
      setIndicatorError(`读取指标配置失败: ${String(loadError)}`)
      setIndicatorItems([])
      setIndicatorFilePath('')
      setIndicatorExists(false)
      clearIndicatorEditor()
    } finally {
      setIndicatorLoading(false)
    }
  }

  async function runIndicatorTask(executor: (downloadId: string) => Promise<DataDownloadRunResult>) {
    setBusyAction('indicator-running')
    clearFeedback('indicatorTask')
    setProgress(null)

    const downloadId = await startProgressListener('download')

    try {
      const result = await executor(downloadId)
      if (result.action === 'delete-stock-data-indicator-columns') {
        setFeedbackNotice(
          'indicatorTask',
          `${result.actionLabel}完成，用时 ${formatElapsedMs(result.elapsedMs)}；清空 ${result.summary.successCount} 列，基础行情列已保留。`,
        )
      } else if (result.action === 'rebuild-stock-data-indicator-columns') {
        setFeedbackNotice(
          'indicatorTask',
          `${result.actionLabel}完成，用时 ${formatElapsedMs(result.elapsedMs)}；补算 ${result.summary.successCount} 组，回写 ${result.summary.savedRows} 行。`,
        )
      } else {
        setFeedbackNotice('indicatorTask', `${result.actionLabel}完成，用时 ${formatElapsedMs(result.elapsedMs)}。`)
      }
      await loadStatus({ preserveNotice: true })
    } catch (runError) {
      setFeedbackError('indicatorTask', `执行指标列维护失败: ${String(runError)}`)
    } finally {
      stopProgressListener()
      setBusyAction('idle')
    }
  }

  async function onRunStockDataIndicatorColumnsDelete() {
    if (!sourcePath) {
      setFeedbackError('indicatorTask', '当前数据目录为空，请先到数据管理页确认目录。')
      return
    }

    await runIndicatorTask((downloadId) =>
      runStockDataIndicatorColumnsDelete({
        downloadId,
        sourcePath,
      }),
    )
  }

  async function onRunStockDataIndicatorColumnsRebuild() {
    if (!sourcePath) {
      setFeedbackError('indicatorTask', '当前数据目录为空，请先到数据管理页确认目录。')
      return
    }

    await runIndicatorTask((downloadId) =>
      runStockDataIndicatorColumnsRebuild({
        downloadId,
        sourcePath,
      }),
    )
  }

  async function onDeleteResultDb() {
    if (!sourcePath) {
      setFeedbackError('rank', '当前数据目录为空，请先到数据管理页确认目录。')
      return
    }

    setBusyAction('deleting-result-db')
    clearFeedback('rank')

    try {
      await removeManagedSourceFile('result-db')
      const managedStatus = await inspectManagedSourceStatus()
      const nextStatus = await getRankingComputeStatus(managedStatus.sourcePath)
      setStatus(nextStatus)
      setFeedbackNotice('rank', '结果库已删除。下次计算排名会重新生成 score_summary / rule_details / scene_details。')
    } catch (actionError) {
      setFeedbackError('rank', `删除结果库失败: ${String(actionError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  async function onDeleteCyqChenDb() {
    if (!sourcePath) {
      setFeedbackError('cyqChen', '当前数据目录为空，请先到数据管理页确认目录。')
      return
    }

    setBusyAction('deleting-cyq-chen-db')
    clearFeedback('cyqChen')

    try {
      await removeManagedSourceFile('cyq-chen-db')
      const managedStatus = await inspectManagedSourceStatus()
      const nextStatus = await getRankingComputeStatus(managedStatus.sourcePath)
      setStatus(nextStatus)
      setCyqChenStrategyDiff(null)
      setFeedbackNotice('cyqChen', '新筹码库已删除。下次新筹码计算会重新生成 cyq_chen_snapshot / cyq_chen_bin。')
    } catch (actionError) {
      setFeedbackError('cyqChen', `删除新筹码库失败: ${String(actionError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  async function onRunCompute() {
    if (!sourcePath) {
      setFeedbackError('rank', '当前数据目录为空，请先到数据管理页确认目录。')
      return
    }

    const startDate = inputDateToCompact(startDateInput)
    const endDate = inputDateToCompact(endDateInput)
    if (!startDate || !endDate) {
      setFeedbackError('rank', '请先输入开始日期和结束日期。')
      return
    }

    setBusyAction('computing')
    clearFeedback('rank')
    setStrategyDiff(null)

    try {
      const previewWarnings = await previewRankingScoreCalculationWarnings(sourcePath, startDate, endDate)
      if (previewWarnings.length > 0) {
        setFeedbackNotice('rank', `提示：${previewWarnings.join('\n')}`)
      }
      const scoreResult = await runRankingScoreCalculation(sourcePath, startDate, endDate)
      setStatus(scoreResult.status)
      const warningText = scoreResult.warnings?.length
        ? `\n\n提示：${scoreResult.warnings.join('\n')}`
        : ''
      setFeedbackNotice(
        'rank',
        `排名计算完成（含J值同分排序），区间 ${formatTradeDate(scoreResult.startDate ?? null)} 至 ${formatTradeDate(scoreResult.endDate ?? null)}，耗时 ${formatElapsedMs(scoreResult.elapsedMs)}。${warningText}`,
      )
    } catch (actionError) {
      setFeedbackError('rank', `排名计算失败: ${String(actionError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  async function onViewStrategyDiff() {
    setStrategyDiffLoading(true)
    clearFeedback('rank')
    try {
      const assetsStatus = await getManagedStrategyAssetsStatus()
      const latestComputeSnapshot = assetsStatus.backups.find((item) => item.sourceKind === 'rank_compute')
      if (!latestComputeSnapshot) {
        setStrategyDiff(null)
        setFeedbackError('rank', '当前没有排名计算快照可对比。')
        return
      }
      const diff = await getManagedStrategyBackupDiff(latestComputeSnapshot.backupId)
      setStrategyDiff(diff)
    } catch (actionError) {
      setStrategyDiff(null)
      setFeedbackError('rank', `查看策略 diff 失败: ${String(actionError)}`)
    } finally {
      setStrategyDiffLoading(false)
    }
  }

  async function onViewCyqChenStrategyDiff() {
    if (!sourcePath) {
      setFeedbackError('cyqChen', '当前数据目录为空，请先到数据管理页确认目录。')
      return
    }

    setCyqChenStrategyDiffLoading(true)
    clearFeedback('cyqChen')
    try {
      const page = await getCyqChenStrategyPage(sourcePath)
      const latestBackup = page.backups[0]
      if (!latestBackup) {
        setCyqChenStrategyDiff(null)
        setFeedbackError('cyqChen', '当前没有筹码策略备份可对比。')
        return
      }
      const diff = await getCyqChenStrategyBackupDiff(sourcePath, latestBackup.backupId)
      setCyqChenStrategyDiff(diff)
    } catch (actionError) {
      setCyqChenStrategyDiff(null)
      setFeedbackError('cyqChen', `查看筹码策略 diff 失败: ${String(actionError)}`)
    } finally {
      setCyqChenStrategyDiffLoading(false)
    }
  }

  async function onRunOtherDataCompute() {
    if (!sourcePath) {
      setFeedbackError('otherData', '当前数据目录为空，请先到数据管理页确认目录。')
      return
    }

    setBusyAction('computing')
    clearFeedback('otherData')

    try {
      const result = await runConceptPerformanceCompute(sourcePath)
      setFeedbackNotice('otherData', `概念/行业表现计算完成，写入 ${result.savedRows} 行，耗时 ${formatElapsedMs(result.elapsedMs)}。`)
    } catch (actionError) {
      setFeedbackError('otherData', `其他数据计算失败: ${String(actionError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  async function onRunCyqCompute() {
    if (!sourcePath) {
      setFeedbackError('cyq', '当前数据目录为空，请先到数据管理页确认目录。')
      return
    }

    const factor = normalizeCyqFactorInput(cyqFactorInput)
    const startDate = cyqStartDateInput.trim()
    const endDate = cyqEndDateInput.trim()
    setCyqFactorInput(String(factor))
    setBusyAction('cyq-computing')
    clearFeedback('cyq')
    setProgress(null)

    try {
      const downloadId = await startProgressListener('cyq')
      const result = await runCyqCompute(
        sourcePath,
        factor,
        startDate || undefined,
        endDate || undefined,
        downloadId,
      )
      const nextStatus = await getRankingComputeStatus(sourcePath)
      setStatus(nextStatus)
      const rangeText =
        result.startDate && result.endDate
          ? `，区间 ${formatTradeDate(result.startDate)} 至 ${formatTradeDate(result.endDate)}`
          : ''
      setFeedbackNotice(
        'cyq',
        `筹码计算完成，分桶 ${result.factor}${rangeText}，写入 ${result.snapshotRows} 条摘要和 ${result.binRows} 条分桶，用时 ${formatElapsedMs(result.elapsedMs)}。`,
      )
    } catch (actionError) {
      setFeedbackError('cyq', `筹码计算失败: ${String(actionError)}`)
    } finally {
      stopProgressListener()
      setBusyAction('idle')
    }
  }

  async function onRunCyqChenCompute() {
    if (!sourcePath) {
      setFeedbackError('cyqChen', '当前数据目录为空，请先到数据管理页确认目录。')
      return
    }

    const warmupDays = normalizeIndicatorPrecInput(cyqChenWarmupDaysInput, 120)
    const bucketPct = Number(cyqChenBucketPctInput.trim() || '1')
    const startDate = cyqChenStartDateInput.trim()
    const endDate = cyqChenEndDateInput.trim()
    if (!Number.isFinite(bucketPct) || bucketPct <= 0) {
      setFeedbackError('cyqChen', '新筹码分桶百分比必须是正数。')
      return
    }

    setCyqChenWarmupDaysInput(String(warmupDays))
    setCyqChenBucketPctInput(String(bucketPct))
    setBusyAction('cyq-chen-computing')
    clearFeedback('cyqChen')
    setCyqChenStrategyDiff(null)
    setProgress(null)

    try {
      const downloadId = await startProgressListener('cyq-chen')
      const result = await runCyqChenCompute(
        sourcePath,
        warmupDays,
        bucketPct,
        startDate || undefined,
        endDate || undefined,
        downloadId,
      )
      const nextStatus = await getRankingComputeStatus(sourcePath)
      setStatus(nextStatus)
      const rangeText =
        result.startDate && result.endDate
          ? `，区间 ${formatTradeDate(result.startDate)} 至 ${formatTradeDate(result.endDate)}`
          : ''
      setFeedbackNotice(
        'cyqChen',
        `新筹码计算完成，预热 ${result.warmupDays} 天，分桶 ${result.bucketPct}%${rangeText}，写入 ${result.snapshotRows} 条摘要和 ${result.binRows} 条分桶，用时 ${formatElapsedMs(result.elapsedMs)}。`,
      )
    } catch (actionError) {
      setFeedbackError('cyqChen', `新筹码计算失败: ${String(actionError)}`)
    } finally {
      stopProgressListener()
      setBusyAction('idle')
    }
  }

  async function onConfirmPendingAction() {
    const current = pendingConfirm
    if (!current) {
      return
    }
    setPendingConfirm(null)

    if (current.kind === 'delete-indicator') {
      await onDeleteIndicator(current.item)
      return
    }

    if (current.kind === 'delete-stock-indicator-columns') {
      await onRunStockDataIndicatorColumnsDelete()
      return
    }

    if (current.kind === 'delete-cyq-chen-db') {
      await onDeleteCyqChenDb()
      return
    }

    await onDeleteResultDb()
  }

  return (
    <div className={mergedMode ? 'ranking-compute-page is-merged-mode' : 'ranking-compute-page'}>
      <section className="ranking-compute-card">
        <div className="ranking-compute-head">
          <div>
            <h2>数据检查</h2>
          </div>

          <div className="ranking-compute-actions">
            <button className="ranking-compute-secondary-btn" type="button" onClick={() => void loadStatus()} disabled={isBusy}>
              {busyAction === 'loading' ? '刷新中...' : mergedMode ? '刷新状态' : '刷新日期信息'}
            </button>
          </div>
        </div>

        <div className="ranking-compute-summary">
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
          <div className="ranking-compute-summary-item">
            <span>筹码库日期范围</span>
            <strong>{describeDbRange(status?.cyqDb)}</strong>
            <small>
              {status?.cyqDb
                ? `${status.cyqDb.distinctTradeDates} 个交易日，${status.cyqDb.rowCount} 条摘要，${status.cyqBinRowCount} 条分桶；当前分桶 ${status.cyqFactor ?? '--'}。`
                : '读取中...'}
            </small>
          </div>
          <div className="ranking-compute-summary-item">
            <span>新筹码库日期范围</span>
            <strong>{describeDbRange(status?.cyqChenDb)}</strong>
            <small>
              {status?.cyqChenDb
                ? `${status.cyqChenDb.distinctTradeDates} 个交易日，${status.cyqChenDb.rowCount} 条摘要，${status.cyqChenBinRowCount} 条分桶；预热 ${status.cyqChenWarmupDays ?? '--'} 天，分桶 ${status.cyqChenBucketPct ?? '--'}%。`
              : '读取中...'}
            </small>
          </div>
          {mergedMode ? (
            <>
              <div className="ranking-compute-summary-item">
                <span>概念表现库</span>
                <strong>{describeDbRange(downloadStatus?.conceptPerformanceDb)}</strong>
                <small>
                  {downloadStatus?.conceptPerformanceDb
                    ? `${downloadStatus.conceptPerformanceDb.distinctTradeDates} 个交易日，${downloadStatus.conceptPerformanceDb.rowCount} 行`
                    : '读取中...'}
                </small>
              </div>
              <div className="ranking-compute-summary-item">
                <span>交易日历</span>
                <strong>{describeDownloadFileRange(downloadStatus?.tradeCalendar)}</strong>
                <small>
                  {downloadStatus?.tradeCalendar
                    ? `${downloadStatus.tradeCalendar.rowCount} 行`
                    : '读取中...'}
                </small>
              </div>
              <div className="ranking-compute-summary-item">
                <span>股票列表</span>
                <strong>{describeDownloadFileRange(downloadStatus?.stockList)}</strong>
                <small>
                  {downloadStatus?.stockList
                    ? `${downloadStatus.stockList.rowCount} 行`
                    : '读取中...'}
                </small>
              </div>
              <div className="ranking-compute-summary-item">
                <span>概念文件</span>
                <strong>{describeDownloadFileRange(downloadStatus?.thsConcepts)}</strong>
                <small>
                  {downloadStatus?.thsConcepts
                    ? `${downloadStatus.thsConcepts.rowCount} 行`
                    : '读取中...'}
                </small>
              </div>
              <div className="ranking-compute-summary-item">
                <span>缺失股票补全</span>
                <strong>{describeMissingStockSummary(downloadStatus)}</strong>
                <small>
                  {downloadStatus?.missingStockRepair
                    ? downloadStatus.missingStockRepair.detail
                    : '读取中...'}
                </small>
              </div>
              <div className="ranking-compute-summary-item">
                <span>新筹码维护</span>
                <strong>{describeCyqChenMaintenanceSummary(downloadStatus)}</strong>
                <small>
                  {downloadStatus?.cyqChenMaintenance
                    ? downloadStatus.cyqChenMaintenance.detail
                    : '读取中...'}
                </small>
              </div>
            </>
          ) : null}
        </div>

        {renderCardFeedback('status')}
      </section>

      <section className="ranking-compute-card">
        <div className="ranking-compute-summary">
          <div className="ranking-compute-summary-item">
            <span>排名计算</span>
            <strong>score_summary / rule_details / scene_details + 排名</strong>
            <small>按区间重算总分、明细和排名。</small>
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
            <button
              className="ranking-compute-secondary-btn"
              type="button"
              onClick={() => void onViewStrategyDiff()}
              disabled={isBusy || strategyDiffLoading || sourcePath === ''}
            >
              {strategyDiffLoading ? '对比中...' : '显示计算快照 diff'}
            </button>
            <button className="ranking-compute-danger-btn" type="button" onClick={() => setPendingConfirm({ kind: 'delete-result-db' })} disabled={isBusy || sourcePath === ''}>
              {busyAction === 'deleting-result-db' ? '删除中...' : '删除结果库'}
            </button>
          </div>
        </div>

        {strategyDiff ? (
          <section className="ranking-compute-strategy-diff">
            <div className="ranking-compute-strategy-diff-headline">
              <div>
                <span>策略变化</span>
                <strong>
                  {strategyDiff.changedLineCount === 0
                    ? '当前生效策略与计算快照一致'
                    : `发现 ${strategyDiff.changedLineCount} 行变化`}
                </strong>
                <small>
                  计算快照 {strategyDiff.backupLabel} 对比当前生效 {strategyDiff.activeLabel}；变化条目完整显示，未变化条目折叠
                </small>
              </div>
              <button
                className="ranking-compute-secondary-btn"
                type="button"
                onClick={() => setStrategyDiff(null)}
                disabled={isBusy}
              >
                关闭 diff
              </button>
            </div>
            <div className="ranking-compute-strategy-diff-table-head">
              <span>快照</span>
              <span>当前</span>
              <span>策略内容</span>
            </div>
            <div className="ranking-compute-strategy-diff-body">
              {strategyDiff.lines.map((line, index) => (
                <div
                  key={`${line.kind}-${line.backupLine ?? 'n'}-${line.activeLine ?? 'n'}-${index}`}
                  className={`ranking-compute-strategy-diff-row is-${line.kind}`}
                >
                  <span>{line.backupLine ?? ''}</span>
                  <span>{line.activeLine ?? ''}</span>
                  <code>
                    {line.kind === 'backup'
                      ? '- '
                      : line.kind === 'active'
                        ? '+ '
                        : line.kind === 'omitted'
                          ? '... '
                          : '  '}
                    {line.text || ' '}
                  </code>
                </div>
              ))}
            </div>
          </section>
        ) : null}

        {renderCardFeedback('rank')}
      </section>

      <section className="ranking-compute-card">
        <div className="ranking-compute-summary">
          <div className="ranking-compute-summary-item">
            <span>其他数据计算</span>
            <strong>概念/行业表现</strong>
            <small>重建 concept_performance，包含 concept 和 industry 两类表现。</small>
          </div>
        </div>

        <div className="ranking-compute-actions">
          <button className="ranking-compute-secondary-btn" type="button" onClick={() => void onRunOtherDataCompute()} disabled={isBusy || sourcePath === ''}>
            {busyAction === 'computing' ? '计算中...' : '开始其他数据计算'}
          </button>
        </div>

        {renderCardFeedback('otherData')}
      </section>

      <section className="ranking-compute-card">
        <div className="ranking-compute-summary">
          <div className="ranking-compute-summary-item">
            <span>筹码计算</span>
            <strong>cyq.db / cyq_snapshot / cyq_bin</strong>
            <small>按日期范围重建筹码摘要和分桶明细。</small>
          </div>
        </div>

        <div className="ranking-compute-form ranking-compute-cyq-form">
          <label className="ranking-compute-field">
            <span>开始日期</span>
            <input
              type="date"
              value={cyqStartDateInput}
              onChange={(event) => setCyqStartDateInput(event.target.value)}
              placeholder={compactDateToInput(status?.sourceDb?.minTradeDate)}
            />
          </label>

          <label className="ranking-compute-field">
            <span>结束日期</span>
            <input
              type="date"
              value={cyqEndDateInput}
              onChange={(event) => setCyqEndDateInput(event.target.value)}
              placeholder={compactDateToInput(status?.sourceDb?.maxTradeDate)}
            />
          </label>

          <label className="ranking-compute-field">
            <span>分桶数</span>
            <input
              type="number"
              min="2"
              step="1"
              value={cyqFactorInput}
              onChange={(event) => setCyqFactorInput(event.target.value)}
            />
          </label>

          <small className="ranking-compute-cyq-range">
            起始日留空走默认：
            {status?.sourceDb?.minTradeDate || status?.sourceDb?.maxTradeDate
              ? `原始库 ${formatTradeDate(status?.sourceDb?.minTradeDate)} 至 ${formatTradeDate(status?.sourceDb?.maxTradeDate)}`
              : '暂无'}
          </small>

          <div className="ranking-compute-actions ranking-compute-cyq-actions">
            <button
              className="ranking-compute-secondary-btn"
              type="button"
              onClick={() => void onRunCyqCompute()}
              disabled={isBusy || sourcePath === ''}
            >
              {busyAction === 'cyq-computing' ? '计算中...' : '开始筹码计算'}
            </button>
          </div>
        </div>

        {busyAction === 'cyq-computing'
          ? renderProgressCard('筹码计算已经启动，正在等待后端返回当前股票进度。')
          : null}

        {renderCardFeedback('cyq')}
      </section>

      <section className="ranking-compute-card">
        <div className="ranking-compute-summary">
          <div className="ranking-compute-summary-item">
            <span>新筹码计算</span>
            <strong>cyq_chen.db / cyq_chen_snapshot / cyq_chen_bin</strong>
            <small>重建新筹码摘要、分桶明细和策略结果。</small>
          </div>
        </div>

        <div className="ranking-compute-form ranking-compute-cyq-form ranking-compute-cyq-chen-form">
          <label className="ranking-compute-field">
            <span>开始日期</span>
            <input
              type="date"
              value={cyqChenStartDateInput}
              onChange={(event) => setCyqChenStartDateInput(event.target.value)}
              placeholder={compactDateToInput(status?.sourceDb?.minTradeDate)}
            />
          </label>

          <label className="ranking-compute-field">
            <span>结束日期</span>
            <input
              type="date"
              value={cyqChenEndDateInput}
              onChange={(event) => setCyqChenEndDateInput(event.target.value)}
              placeholder={compactDateToInput(status?.sourceDb?.maxTradeDate)}
            />
          </label>

          <label className="ranking-compute-field">
            <span>预热天数</span>
            <input
              type="number"
              min="0"
              step="1"
              value={cyqChenWarmupDaysInput}
              onChange={(event) => setCyqChenWarmupDaysInput(event.target.value)}
            />
          </label>

          <label className="ranking-compute-field">
            <span>分桶百分比</span>
            <input
              type="number"
              min="0.01"
              step="0.1"
              value={cyqChenBucketPctInput}
              onChange={(event) => setCyqChenBucketPctInput(event.target.value)}
            />
          </label>

          <small className="ranking-compute-cyq-range">
            依赖文件：chip_change_rule.toml；当前库：
            {status?.cyqChenDb
              ? ` ${describeDbRange(status.cyqChenDb)}`
              : ' 读取中...'}
          </small>

          <div className="ranking-compute-actions ranking-compute-cyq-actions ranking-compute-cyq-chen-actions">
            <button
              className="ranking-compute-secondary-btn"
              type="button"
              onClick={() => void onRunCyqChenCompute()}
              disabled={isBusy || sourcePath === ''}
            >
              {busyAction === 'cyq-chen-computing' ? '计算中...' : '开始新筹码计算'}
            </button>
            <button
              className="ranking-compute-secondary-btn"
              type="button"
              onClick={() => void onViewCyqChenStrategyDiff()}
              disabled={isBusy || cyqChenStrategyDiffLoading || sourcePath === ''}
            >
              {cyqChenStrategyDiffLoading ? '对比中...' : '显示筹码策略 diff'}
            </button>
            <button
              className="ranking-compute-danger-btn"
              type="button"
              onClick={() => setPendingConfirm({ kind: 'delete-cyq-chen-db' })}
              disabled={isBusy || sourcePath === ''}
            >
              {busyAction === 'deleting-cyq-chen-db' ? '删除中...' : '删除新筹码库'}
            </button>
          </div>
        </div>

        {busyAction === 'cyq-chen-computing'
          ? renderProgressCard('新筹码计算已经启动，正在等待后端返回当前股票进度。')
          : null}

        {cyqChenStrategyDiff ? (
          <section className="ranking-compute-strategy-diff">
            <div className="ranking-compute-strategy-diff-headline">
              <div>
                <span>筹码策略变化</span>
                <strong>
                  {cyqChenStrategyDiff.changedLineCount === 0
                    ? '当前筹码策略与备份一致'
                    : `发现 ${cyqChenStrategyDiff.changedLineCount} 行变化`}
                </strong>
                <small>
                  备份 {cyqChenStrategyDiff.backupLabel} 对比当前生效 {cyqChenStrategyDiff.activeLabel}；变化策略完整显示，未变化策略折叠
                </small>
              </div>
              <button
                className="ranking-compute-secondary-btn"
                type="button"
                onClick={() => setCyqChenStrategyDiff(null)}
                disabled={isBusy}
              >
                关闭 diff
              </button>
            </div>
            <div className="ranking-compute-strategy-diff-table-head">
              <span>备份</span>
              <span>当前</span>
              <span>策略内容</span>
            </div>
            <div className="ranking-compute-strategy-diff-body">
              {cyqChenStrategyDiff.lines.map((line, index) => (
                <div
                  key={`${line.kind}-${line.backupLine ?? 'n'}-${line.activeLine ?? 'n'}-${index}`}
                  className={`ranking-compute-strategy-diff-row is-${line.kind}`}
                >
                  <span>{line.backupLine ?? ''}</span>
                  <span>{line.activeLine ?? ''}</span>
                  <code>
                    {line.kind === 'backup'
                      ? '- '
                      : line.kind === 'active'
                        ? '+ '
                        : line.kind === 'omitted'
                          ? '... '
                          : '  '}
                    {line.text || ' '}
                  </code>
                </div>
              ))}
            </div>
          </section>
        ) : null}

        {renderCardFeedback('cyqChen')}
      </section>

      <section className="ranking-compute-card">
        <div className="ranking-compute-summary">
          <div className="ranking-compute-summary-item">
            <span>行情数据指标列维护</span>
            <strong>ind.toml + stock_data 指标列</strong>
            <small>管理指标配置，并清空或补算行情指标列。</small>
          </div>
        </div>

        <div className="ranking-compute-actions">
          <button
            className="ranking-compute-secondary-btn"
            type="button"
            onClick={() => void openIndicatorManager()}
            disabled={isBusy}
          >
            指标管理
          </button>

          <button
            className="ranking-compute-danger-btn"
            type="button"
            onClick={() => setPendingConfirm({ kind: 'delete-stock-indicator-columns' })}
            disabled={isBusy || sourcePath === ''}
          >
            清空指标列
          </button>

          <button
            className="ranking-compute-secondary-btn"
            type="button"
            onClick={() => void onRunStockDataIndicatorColumnsRebuild()}
            disabled={isBusy || sourcePath === ''}
          >
            {busyAction === 'indicator-running' ? '补算中...' : '补算指标列'}
          </button>
        </div>

        {showIndicatorProgress
          ? renderProgressCard('任务已经启动，正在等待后端返回当前状态。')
          : null}

        {renderCardFeedback('indicatorTask')}
      </section>

      {indicatorModalOpen ? (
        <div
          className="data-download-modal-backdrop"
          role="presentation"
          onClick={() => {
            if (!indicatorSaving) {
              setIndicatorModalOpen(false)
            }
          }}
        >
          <div
            className="data-download-modal"
            role="dialog"
            aria-modal="true"
            aria-label="指标管理"
            onClick={(event) => event.stopPropagation()}
          >
            <div className="data-download-modal-head">
              <div>
                <h3>指标管理</h3>
                <p>{indicatorFilePath || 'ind.toml'}</p>
              </div>
              <button
                className="data-download-secondary-btn"
                type="button"
                onClick={() => setIndicatorModalOpen(false)}
                disabled={indicatorSaving}
              >
                关闭
              </button>
            </div>

            {indicatorNotice ? <div className="data-download-notice">{indicatorNotice}</div> : null}
            {indicatorError ? <div className="data-download-error">{indicatorError}</div> : null}

            {indicatorLoading ? (
              <div className="data-download-modal-loading">读取指标配置中...</div>
            ) : (
              <>
                <div className="data-download-modal-meta">
                  <span>{indicatorExists ? '当前文件已存在' : '当前文件不存在，将在保存时创建'}</span>
                  <span>当前共 {indicatorItems.length} 个指标，保存前会做语法和字段校验</span>
                  <span>J 用于排名同分排序，不能删。</span>
                </div>

                <div className="data-download-indicator-toolbar">
                  <button
                    className="data-download-secondary-btn"
                    type="button"
                    onClick={onCreateIndicator}
                    disabled={indicatorSaving}
                  >
                    新增指标
                  </button>
                  <button
                    className="data-download-secondary-btn"
                    type="button"
                    onClick={() => void openIndicatorManager()}
                    disabled={indicatorSaving}
                  >
                    重新读取
                  </button>
                </div>

                {indicatorDraft && indicatorEditorMode === 'create'
                  ? renderIndicatorEditorCard('新增指标')
                  : null}

                {indicatorItems.length === 0 ? (
                  <div className="data-download-modal-loading">当前没有指标，点“新增指标”开始添加。</div>
                ) : (
                  <div className="data-download-indicator-list">
                    {indicatorItems.map((item) => {
                      const isEditing =
                        indicatorEditorMode === 'edit' && indicatorEditingName === item.name

                      return isEditing ? (
                        <div key={item.name}>
                          {renderIndicatorEditorCard(`修改指标 · ${indicatorEditingName ?? '--'}`)}
                        </div>
                      ) : (
                        <article key={item.name} className="data-download-indicator-card">
                          <div className="data-download-indicator-card-head">
                            <div>
                              <div className="data-download-indicator-card-name">{item.name}</div>
                              <div className="data-download-indicator-card-meta">精度 {item.prec}</div>
                            </div>
                            <div className="data-download-indicator-card-actions">
                              <button
                                className="data-download-secondary-btn"
                                type="button"
                                onClick={() => onEditIndicator(item)}
                                disabled={indicatorSaving}
                              >
                                修改
                              </button>
                              <button
                                className="data-download-secondary-btn data-download-danger-btn"
                                type="button"
                                onClick={() => setPendingConfirm({ kind: 'delete-indicator', item })}
                                disabled={indicatorSaving}
                              >
                                删除
                              </button>
                            </div>
                          </div>

                          <pre className="data-download-indicator-card-expr">{item.expr}</pre>
                        </article>
                      )
                    })}
                  </div>
                )}

                <div className="data-download-modal-actions">
                  <button
                    className="data-download-primary-btn"
                    type="button"
                    onClick={() => setIndicatorModalOpen(false)}
                  >
                    关闭
                  </button>
                </div>
              </>
            )}
          </div>
        </div>
      ) : null}

      <ConfirmDialog
        open={pendingConfirm !== null}
        title={
          pendingConfirm?.kind === 'delete-indicator'
            ? '确认删除指标'
            : pendingConfirm?.kind === 'delete-stock-indicator-columns'
              ? '确认清空指标列'
              : pendingConfirm?.kind === 'delete-cyq-chen-db'
                ? '确认删除新筹码库'
                : '确认删除结果库'
        }
        message={
          pendingConfirm?.kind === 'delete-indicator'
            ? `确认删除指标 ${pendingConfirm.item.name} 吗？`
            : pendingConfirm?.kind === 'delete-stock-indicator-columns'
              ? '确认清空 stock_data 中的所有非基础指标列吗？\n\n该操作会重建 stock_data 表，只保留基础行情列和已有基础行情数据；数据量较大时耗时会更久。'
              : pendingConfirm?.kind === 'delete-cyq-chen-db'
                ? '确认删除当前新筹码库 cyq_chen.db 吗？将清空 cyq_chen_snapshot / cyq_chen_bin，删除后需要重新计算新筹码。'
                : '确认删除当前结果库 scoring_result.db 吗？将同时清空 score_summary / rule_details / scene_details，删除后需要重新计算排名。'
        }
        confirmText="确认"
        cancelText="取消"
        danger
        busy={isBusy}
        onCancel={() => setPendingConfirm(null)}
        onConfirm={() => void onConfirmPendingAction()}
      />
    </div>
  )
}
