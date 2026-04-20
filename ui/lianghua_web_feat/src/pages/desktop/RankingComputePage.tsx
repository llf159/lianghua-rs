import { useDeferredValue, useEffect, useRef, useState } from 'react'
import type { UnlistenFn } from '@tauri-apps/api/event'
import { inspectManagedSourceStatus, removeManagedSourceFile } from '../../apis/managedSource'
import {
  getIndicatorManagePage,
  listenDataDownloadProgress,
  runStockDataIndicatorColumnsDelete,
  runStockDataIndicatorColumnsRebuild,
  saveIndicatorManagePage,
  type DataDownloadProgress,
  type DataDownloadRunResult,
  type IndicatorManageDraft,
  type IndicatorManageItem,
  type IndicatorManagePageData,
} from '../../apis/dataDownload'
import {
  getRankingComputeStatus,
  runConceptPerformanceCompute,
  runRankingScoreCalculation,
  runRankingTiebreakFill,
  type RankComputeDbRange,
  type RankComputeResultContinuity,
  type RankComputeTimingItem,
  type RankingComputeStatus,
} from '../../apis/rankingCompute'
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

type BusyAction = 'idle' | 'loading' | 'computing' | 'deleting-result-db' | 'indicator-running'
type IndicatorEditorMode = 'create' | 'edit'
type PendingConfirmState =
  | { kind: 'delete-indicator'; item: IndicatorManageItem }
  | { kind: 'delete-stock-indicator-columns' }
  | { kind: 'delete-result-db' }
  | null

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

function formatTimingSummary(items: RankComputeTimingItem[]) {
  if (items.length === 0) {
    return '无分项'
  }

  return items
    .map((item) => `${item.label}${item.note ? `(${item.note})` : ''} ${formatElapsedMs(item.elapsedMs)}`)
    .join('；')
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

function formatPhaseLabel(phase: string | null | undefined) {
  switch (normalizeProgressPhase(phase)) {
    case 'delete_stock_data_indicator_columns':
      return '清空指标列'
    case 'rebuild_stock_data_indicator_columns':
      return '补算指标列'
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
    case 'delete-stock-data-indicator-columns':
      return ['delete_stock_data_indicator_columns'] as string[]
    case 'rebuild-stock-data-indicator-columns':
      return ['rebuild_stock_data_indicator_columns'] as string[]
    default:
      return null
  }
}

export default function RankingComputePage() {
  const [status, setStatus] = useState<RankingComputeStatus | null>(null)
  const [busyAction, setBusyAction] = useState<BusyAction>('loading')
  const [startDateInput, setStartDateInput] = useState('')
  const [endDateInput, setEndDateInput] = useState('')
  const [notice, setNotice] = useState('')
  const [error, setError] = useState('')
  const [progress, setProgress] = useState<DataDownloadProgress | null>(null)

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
  const deferredProgress = useDeferredValue(progress)
  const progressPercent = calcProgressPercent(deferredProgress, getProgressWorkflow, ['done'])
  const shownProgressPercent = useAnimatedProgressPercent(busyAction === 'indicator-running', progressPercent)
  const phaseStep = getPhaseStep(deferredProgress?.action, deferredProgress?.phase, getProgressWorkflow)
  const progressCounterText = getProgressCounterText(deferredProgress, formatPhaseLabel)

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

  async function loadStatus(options?: { preserveNotice?: boolean }) {
    const preserveNotice = options?.preserveNotice === true
    setBusyAction('loading')
    setError('')

    try {
      const managedStatus = await inspectManagedSourceStatus()
      const nextStatus = await getRankingComputeStatus(managedStatus.sourcePath)
      setStatus(nextStatus)
      if (!preserveNotice) {
        setNotice('')
      }

      setStartDateInput((current) => current || compactDateToInput(nextStatus.suggestedStartDate))
      setEndDateInput((current) => current || compactDateToInput(nextStatus.suggestedEndDate))
    } catch (loadError) {
      setNotice('')
      setError(`读取数据计算状态失败: ${String(loadError)}`)
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
      setError('当前数据目录为空，请先到数据管理页确认目录。')
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
      if (result.action === 'delete-stock-data-indicator-columns') {
        setNotice(
          `${result.actionLabel}完成，用时 ${formatElapsedMs(result.elapsedMs)}；清空 ${result.summary.successCount} 列，基础行情列已保留。`,
        )
      } else if (result.action === 'rebuild-stock-data-indicator-columns') {
        setNotice(
          `${result.actionLabel}完成，用时 ${formatElapsedMs(result.elapsedMs)}；补算 ${result.summary.successCount} 组，回写 ${result.summary.savedRows} 行。`,
        )
      } else {
        setNotice(`${result.actionLabel}完成，用时 ${formatElapsedMs(result.elapsedMs)}。`)
      }
      await loadStatus({ preserveNotice: true })
    } catch (runError) {
      setNotice('')
      setError(`执行指标列维护失败: ${String(runError)}`)
    } finally {
      progressUnlistenRef.current?.()
      progressUnlistenRef.current = null
      activeDownloadIdRef.current = ''
      setBusyAction('idle')
    }
  }

  async function onRunStockDataIndicatorColumnsDelete() {
    if (!sourcePath) {
      setError('当前数据目录为空，请先到数据管理页确认目录。')
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
      setError('当前数据目录为空，请先到数据管理页确认目录。')
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
      setError('当前数据目录为空，请先到数据管理页确认目录。')
      return
    }

    setBusyAction('deleting-result-db')
    setError('')

    try {
      await removeManagedSourceFile('result-db')
      const managedStatus = await inspectManagedSourceStatus()
      const nextStatus = await getRankingComputeStatus(managedStatus.sourcePath)
      setStatus(nextStatus)
      setNotice('结果库已删除。下次计算排名会重新生成 score_summary / rule_details / scene_details。')
    } catch (actionError) {
      setNotice('')
      setError(`删除结果库失败: ${String(actionError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

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
      const scoreStats = formatTimingSummary(scoreResult.timings)
      const tiebreakStats = formatTimingSummary(tiebreakResult.timings)
      setNotice(
        `排名计算和补排名完成，区间 ${formatTradeDate(scoreResult.startDate ?? null)} 至 ${formatTradeDate(scoreResult.endDate ?? null)}，总耗时 ${formatElapsedMs(scoreResult.elapsedMs + tiebreakResult.elapsedMs)}。统计：评分阶段 ${scoreStats}；补排名阶段 ${tiebreakStats}。`,
      )
    } catch (actionError) {
      setNotice('')
      setError(`排名计算失败: ${String(actionError)}`)
    } finally {
      setBusyAction('idle')
    }
  }

  async function onRunOtherDataCompute() {
    if (!sourcePath) {
      setError('当前数据目录为空，请先到数据管理页确认目录。')
      return
    }

    setBusyAction('computing')
    setError('')

    try {
      const result = await runConceptPerformanceCompute(sourcePath)
      setNotice(`概念/行业/板块表现计算完成，写入 ${result.savedRows} 行，耗时 ${formatElapsedMs(result.elapsedMs)}。`)
    } catch (actionError) {
      setNotice('')
      setError(`其他数据计算失败: ${String(actionError)}`)
    } finally {
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

    await onDeleteResultDb()
  }

  return (
    <div className="ranking-compute-page">
      <section className="ranking-compute-card">
        <div className="ranking-compute-head">
          <div>
            <h2>数据检查</h2>
          </div>

          <div className="ranking-compute-actions">
            <button className="ranking-compute-secondary-btn" type="button" onClick={() => void loadStatus()} disabled={isBusy}>
              {busyAction === 'loading' ? '刷新中...' : '刷新日期信息'}
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
        </div>
      </section>

      <section className="ranking-compute-card">
        <div className="ranking-compute-summary">
          <div className="ranking-compute-summary-item">
            <span>排名计算</span>
            <strong>score_summary / rule_details / scene_details + 补排名</strong>
            <small>按区间重算总分、规则明细、场景明细，并执行补排名。</small>
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
            <button className="ranking-compute-danger-btn" type="button" onClick={() => setPendingConfirm({ kind: 'delete-result-db' })} disabled={isBusy || sourcePath === ''}>
              {busyAction === 'deleting-result-db' ? '删除中...' : '删除结果库'}
            </button>
          </div>
        </div>
      </section>

      <section className="ranking-compute-card">
        <div className="ranking-compute-summary">
          <div className="ranking-compute-summary-item">
            <span>其他数据计算</span>
            <strong>概念/行业/板块表现</strong>
            <small>重建 concept_performance，包含 concept、industry 和 market 三类表现。</small>
          </div>
        </div>

        <div className="ranking-compute-actions">
          <button className="ranking-compute-secondary-btn" type="button" onClick={() => void onRunOtherDataCompute()} disabled={isBusy || sourcePath === ''}>
            {busyAction === 'computing' ? '计算中...' : '开始其他数据计算'}
          </button>
        </div>
      </section>

      <section className="ranking-compute-card">
        <div className="ranking-compute-summary">
          <div className="ranking-compute-summary-item">
            <span>行情数据指标列维护</span>
            <strong>ind.toml + stock_data 指标列</strong>
            <small>这里维护指标配置，并执行清空/补算指标列。</small>
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

        {showIndicatorProgress ? (
          <DataTaskProgress
            phaseLabel={formatPhaseLabel(deferredProgress?.phase)}
            phaseStepPillText={phaseStep ? ` · ${phaseStep.current}/${phaseStep.total}` : ''}
            phaseStepStatText={phaseStep ? ` ${phaseStep.current}/${phaseStep.total}` : ''}
            actionLabel={deferredProgress?.actionLabel ?? '行情数据指标列维护'}
            progressPercent={progressPercent}
            elapsedText={formatElapsedMs(deferredProgress?.elapsedMs ?? 0)}
            shownProgressPercent={shownProgressPercent}
            progressCounterText={progressCounterText}
            currentObjectText={getCurrentObjectText(deferredProgress)}
            message={deferredProgress?.message}
            fallbackMessage="任务已经启动，正在等待后端返回当前状态。"
          />
        ) : null}
      </section>

      {notice ? <div className="ranking-compute-notice">{notice}</div> : null}
      {error ? <div className="ranking-compute-error">{error}</div> : null}

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
              : '确认删除结果库'
        }
        message={
          pendingConfirm?.kind === 'delete-indicator'
            ? `确认删除指标 ${pendingConfirm.item.name} 吗？`
            : pendingConfirm?.kind === 'delete-stock-indicator-columns'
              ? '确认清空 stock_data 中的所有非基础指标列吗？\n\n该操作会重建 stock_data 表，只保留基础行情列和已有基础行情数据；数据量较大时耗时会更久。'
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
