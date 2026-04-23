import { useEffect, useRef, useState } from 'react'

type ProgressWorkflowResolver = (action: string | null | undefined) => string[] | null
type PhaseLabelFormatter = (phase: string | null | undefined) => string

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

export function calcProgressPercent(
  progress: TaskProgress | null,
  getProgressWorkflow: ProgressWorkflowResolver,
  donePhases: string[],
) {
  if (!progress) {
    return null
  }

  const normalizedPhase = normalizeProgressPhase(progress.phase)
  const workflow = getProgressWorkflow(progress.action)
  const doneSet = new Set(donePhases)
  const isDone = doneSet.has(progress.phase) || (normalizedPhase ? doneSet.has(normalizedPhase) : false)
  const localRatio =
    isDone
      ? 1
      : progress.total > 0
        ? Math.max(0, Math.min(1, progress.finished / progress.total))
        : null

  if (workflow && normalizedPhase) {
    const currentIndex = workflow.indexOf(normalizedPhase)
    if (currentIndex >= 0 && localRatio !== null) {
      return Math.max(
        0,
        Math.min(100, Math.round(((currentIndex + localRatio) / workflow.length) * 100)),
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
  const workflow = getProgressWorkflow(action)
  const normalizedPhase = normalizeProgressPhase(phase)
  if (!workflow || !normalizedPhase) {
    return null
  }

  const current = workflow.indexOf(normalizedPhase)
  if (current < 0) {
    return null
  }

  return { current: current + 1, total: workflow.length }
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
