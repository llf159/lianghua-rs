import { useEffect, useRef, useState } from 'react'

export type ProgressWorkflowStep =
  | string
  | {
      key: string
      label?: string
      // 同一数组内的事件共用计数，例如下载批次和该批次的事务写入。
      phases?: Array<string | string[]>
    }

type ProgressWorkflowResolver = (action: string | null | undefined) => ProgressWorkflowStep[] | null
type PhaseLabelFormatter = (phase: string | null | undefined) => string
type ProgressSegmentState = 'done' | 'active' | 'pending'

type TaskProgress = {
  phase: string
  action: string
  finished: number
  total: number
  currentLabel: string | null
  message: string
}

type ProgressTimingInput = {
  action: string
  phase: string
  finished: number
  total: number
  elapsedMs: number
}

type ProgressTimingSample = {
  taskPhase: string
  phaseStartElapsedMs: number
  sampledElapsedMs: number
  sampledAtMs: number
  finished: number
  total: number
  rateElapsedMs: number
  rateFinished: number
  rate: number | null
}

export type ProgressTiming = {
  elapsedMs: number
  phaseElapsedMs: number
  estimatedTotalMs: number | null
  estimatedRemainingMs: number | null
}

export function normalizeProgressPhase(phase: string | null | undefined) {
  switch (phase) {
    case 'retry_ths_concept':
      return 'retry_ths_concepts'
    case 'failed_ths_concept':
      return 'fetch_ths_concept'
    case 'completed':
      return 'done'
    default:
      return phase ?? null
  }
}

function normalizeWorkflowStep(step: ProgressWorkflowStep) {
  if (typeof step === 'string') {
    return {
      key: step,
      label: null,
      phases: [[step]],
    }
  }

  const phases = step.phases && step.phases.length > 0 ? step.phases : [step.key]
  return {
    key: step.key,
    label: step.label ?? null,
    phases: phases.map((phase) => typeof phase === 'string' ? [phase] : phase),
  }
}

function getWorkflowSteps(
  getProgressWorkflow: ProgressWorkflowResolver,
  action: string | null | undefined,
) {
  return getProgressWorkflow(action)?.map(normalizeWorkflowStep) ?? null
}

function findWorkflowStep(
  getProgressWorkflow: ProgressWorkflowResolver,
  action: string | null | undefined,
  phase: string | null | undefined,
) {
  const workflow = getWorkflowSteps(getProgressWorkflow, action)
  const normalizedPhase = normalizeProgressPhase(phase)
  if (!workflow || !normalizedPhase) {
    return null
  }

  for (let stepIndex = 0; stepIndex < workflow.length; stepIndex += 1) {
    const phaseIndex = workflow[stepIndex].phases.findIndex((phases) => phases.includes(normalizedPhase))
    if (phaseIndex >= 0) {
      return {
        workflow,
        step: workflow[stepIndex],
        stepIndex,
        phaseIndex,
        phaseTotal: workflow[stepIndex].phases.length,
      }
    }
  }

  return null
}

export function calcProgressPercent(
  progress: TaskProgress | null,
  getProgressWorkflow: ProgressWorkflowResolver,
  donePhases: string[],
) {
  if (!progress) {
    return null
  }

  const normalizedPhase = normalizeProgressPhase(progress.phase)
  const workflowMatch = findWorkflowStep(getProgressWorkflow, progress.action, progress.phase)
  const doneSet = new Set(donePhases)
  const isDone =
    doneSet.has(progress.phase) || (normalizedPhase ? doneSet.has(normalizedPhase) : false)
  if (isDone) {
    return 100
  }
  const localRatio = progress.total > 0
    ? Math.max(0, Math.min(1, progress.finished / progress.total))
    : null

  if (workflowMatch) {
    const stepRatio = (workflowMatch.phaseIndex + (localRatio ?? 0)) / workflowMatch.phaseTotal
    return Math.max(
      0,
      Math.min(99.9, ((workflowMatch.stepIndex + stepRatio) / workflowMatch.workflow.length) * 100),
    )
  }

  if (localRatio === null) {
    return null
  }

  return Math.max(0, Math.min(99.9, localRatio * 100))
}

export function getPhaseStep(
  action: string | null | undefined,
  phase: string | null | undefined,
  getProgressWorkflow: ProgressWorkflowResolver,
) {
  const workflowMatch = findWorkflowStep(getProgressWorkflow, action, phase)
  if (!workflowMatch) {
    return null
  }

  return { current: workflowMatch.stepIndex + 1, total: workflowMatch.workflow.length }
}

export function getProgressTaskLabel(
  action: string | null | undefined,
  phase: string | null | undefined,
  getProgressWorkflow: ProgressWorkflowResolver,
  formatPhaseLabel: PhaseLabelFormatter,
) {
  const workflowMatch = findWorkflowStep(getProgressWorkflow, action, phase)
  return workflowMatch?.step.label ?? formatPhaseLabel(phase)
}

export function getProgressSegments(
  action: string | null | undefined,
  phase: string | null | undefined,
  getProgressWorkflow: ProgressWorkflowResolver,
  donePhases: string[],
  formatPhaseLabel: PhaseLabelFormatter,
) {
  const workflowMatch = findWorkflowStep(getProgressWorkflow, action, phase)
  const workflow = workflowMatch?.workflow ?? getWorkflowSteps(getProgressWorkflow, action)
  if (!workflow) {
    return null
  }

  const normalizedPhase = normalizeProgressPhase(phase)
  const doneSet = new Set(donePhases)
  const isDone = Boolean(
    (phase && doneSet.has(phase)) || (normalizedPhase && doneSet.has(normalizedPhase)),
  )

  return workflow.map((step, index) => {
    const state: ProgressSegmentState = isDone || (workflowMatch && index < workflowMatch.stepIndex)
      ? 'done'
      : workflowMatch && index === workflowMatch.stepIndex
        ? 'active'
        : 'pending'

    return {
      key: step.key,
      label: step.label ?? formatPhaseLabel(step.key),
      state,
    }
  })
}

export function getProgressCounterText(
  progress: TaskProgress | null,
  formatPhaseLabel: PhaseLabelFormatter,
) {
  if (!progress) {
    return '等待后端返回分段进度'
  }

  const phaseLabel = formatPhaseLabel(progress.phase)
  if (progress.total > 0) {
    return `${phaseLabel} · ${progress.finished} / ${progress.total}`
  }

  return phaseLabel
}

export function getCurrentObjectText(progress: TaskProgress | null) {
  if (!progress) {
    return '等待后端分派任务'
  }

  return progress.currentLabel ?? progress.message ?? '等待后端分派任务'
}

export function formatProgressDuration(value: number | null) {
  if (value === null || !Number.isFinite(value)) return '估算中'
  const seconds = Math.ceil(Math.max(0, value) / 1000)
  const hours = Math.floor(seconds / 3600)
  const minutes = Math.floor((seconds % 3600) / 60)
  if (hours > 0) return `${hours}小时 ${minutes}分 ${seconds % 60}秒`
  if (minutes > 0) return `${minutes}分 ${seconds % 60}秒`
  return `${seconds}秒`
}

export function useProgressTiming(
  isRunning: boolean,
  progress: ProgressTimingInput | null | undefined,
  getProgressWorkflow?: ProgressWorkflowResolver,
): ProgressTiming {
  const [clockMs, setClockMs] = useState(0)
  const [sample, setSample] = useState<ProgressTimingSample | null>(null)

  useEffect(() => {
    if (!isRunning) {
      const frameId = window.requestAnimationFrame(() => setSample(null))
      return () => window.cancelAnimationFrame(frameId)
    }

    const timer = window.setInterval(() => setClockMs(Date.now()), 1000)
    return () => window.clearInterval(timer)
  }, [isRunning])

  useEffect(() => {
    if (!isRunning || !progress) {
      return
    }

    const frameId = window.requestAnimationFrame(() => {
      const sampledAtMs = Date.now()
      setSample((current) => {
        const workflowMatch = getProgressWorkflow
          ? findWorkflowStep(getProgressWorkflow, progress.action, progress.phase)
          : null
        const taskPhase = workflowMatch
          ? `${progress.action}::${workflowMatch.step.key}::${workflowMatch.phaseIndex}`
          : `${progress.action}::${progress.phase}`
        const phaseChanged = !current || current.taskPhase !== taskPhase
          || (current.total > 0 && progress.total > 0 && current.total !== progress.total)
          || progress.finished < current.finished || progress.elapsedMs < current.sampledElapsedMs
        const valuesChanged =
          phaseChanged || current.finished !== progress.finished || current.total !== progress.total
        if (!valuesChanged) {
          return current
        }

        const rateElapsed = phaseChanged ? 0 : progress.elapsedMs - current.rateElapsedMs
        const rateFinished = phaseChanged ? 0 : progress.finished - current.rateFinished
        const hasRateSample = rateElapsed > 0 && rateFinished > 0
        const intervalRate = hasRateSample ? rateFinished / rateElapsed : null
        const weight = 1 - Math.exp(-Math.LN2 * rateElapsed / 30000)

        return {
          taskPhase,
          phaseStartElapsedMs: phaseChanged
            ? progress.elapsedMs
            : (current as ProgressTimingSample).phaseStartElapsedMs,
          sampledElapsedMs: progress.elapsedMs,
          sampledAtMs,
          finished: progress.finished,
          total: progress.total,
          rateElapsedMs: phaseChanged || hasRateSample ? progress.elapsedMs : current.rateElapsedMs,
          rateFinished: phaseChanged || hasRateSample ? progress.finished : current.rateFinished,
          rate: phaseChanged ? null : intervalRate === null ? current.rate
            : current.rate === null ? intervalRate : current.rate * (1 - weight) + intervalRate * weight,
        }
      })
    })
    return () => window.cancelAnimationFrame(frameId)
  }, [isRunning, progress, getProgressWorkflow])

  if (!isRunning || !progress || !sample) {
    return {
      elapsedMs: 0,
      phaseElapsedMs: 0,
      estimatedTotalMs: null,
      estimatedRemainingMs: null,
    }
  }

  const elapsedMs =
    sample.sampledElapsedMs + Math.max(0, Math.max(clockMs, sample.sampledAtMs) - sample.sampledAtMs)
  const phaseElapsedMs = Math.max(0, elapsedMs - sample.phaseStartElapsedMs)
  const estimatedTotalMs = sample.rate !== null && sample.rate > 0 && sample.total > 0
    ? Math.max(0, sample.sampledElapsedMs - sample.phaseStartElapsedMs)
      + Math.max(0, sample.total - sample.finished) / sample.rate
    : null
  const estimatedRemainingMs =
    estimatedTotalMs === null ? null : Math.max(0, estimatedTotalMs - phaseElapsedMs)

  return { elapsedMs, phaseElapsedMs, estimatedTotalMs, estimatedRemainingMs }
}

export function useAnimatedProgressPercent(
  isRunning: boolean,
  progressPercent: number | null,
  minPercent = 0,
) {
  const [displayProgressPercent, setDisplayProgressPercent] = useState(0)
  const displayProgressPercentRef = useRef(0)

  useEffect(() => {
    if (!isRunning) {
      displayProgressPercentRef.current = 0
      const resetFrame = window.requestAnimationFrame(() => {
        setDisplayProgressPercent(0)
      })
      return () => window.cancelAnimationFrame(resetFrame)
    }

    let frameId = 0

    if (progressPercent === null) {
      const fallback = Math.max(displayProgressPercentRef.current, minPercent)
      displayProgressPercentRef.current = fallback
      frameId = window.requestAnimationFrame(() => {
        setDisplayProgressPercent(fallback)
      })
      return () => window.cancelAnimationFrame(frameId)
    }

    const from = displayProgressPercentRef.current
    const to = progressPercent
    if (Math.abs(to - from) < 0.01) {
      displayProgressPercentRef.current = to
      frameId = window.requestAnimationFrame(() => {
        setDisplayProgressPercent(to)
      })
      return () => window.cancelAnimationFrame(frameId)
    }

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
  }, [isRunning, minPercent, progressPercent])

  if (!isRunning) {
    return 0
  }

  if (progressPercent === null) {
    return Math.max(displayProgressPercent, minPercent)
  }

  return Math.max(0, Math.min(100, displayProgressPercent))
}
