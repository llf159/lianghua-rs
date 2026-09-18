import { useEffect, useRef, useState } from 'react'

export type ProgressWorkflowStep =
  | string
  | {
      key: string
      label?: string
      phases?: string[]
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
      phases: [step],
    }
  }

  const phases = step.phases && step.phases.length > 0 ? step.phases : [step.key]
  return {
    key: step.key,
    label: step.label ?? null,
    phases,
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
    const phaseIndex = workflow[stepIndex].phases.indexOf(normalizedPhase)
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
  const localRatio =
    isDone
      ? 1
      : progress.total > 0
        ? Math.max(0, Math.min(1, progress.finished / progress.total))
        : null

  if (workflowMatch) {
    if (localRatio !== null) {
      const stepRatio = (workflowMatch.phaseIndex + localRatio) / workflowMatch.phaseTotal
      return Math.max(
        0,
        Math.min(
          100,
          Math.round(((workflowMatch.stepIndex + stepRatio) / workflowMatch.workflow.length) * 100),
        ),
      )
    }

    if (isDone) {
      return 100
    }

    return null
  }

  if (localRatio === null) {
    return null
  }

  return Math.max(0, Math.min(100, Math.round(localRatio * 100)))
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

export function useAnimatedProgressPercent(
  isRunning: boolean,
  progressPercent: number | null,
  minPercent = 10,
) {
  const [displayProgressPercent, setDisplayProgressPercent] = useState(0)
  const displayProgressPercentRef = useRef(0)

  useEffect(() => {
    displayProgressPercentRef.current = displayProgressPercent
  }, [displayProgressPercent])

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
    if (Math.abs(to - from) < 0.5) {
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

  return Math.max(displayProgressPercent, Math.min(progressPercent, 100))
}
