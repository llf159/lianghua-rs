import type { TrackDefinition } from '../apis/strategyTracking'

export function prepareTrackingDefinitions(tracking: TrackDefinition[], captureTexts: Record<string, string>) {
  return tracking.map((track) => {
    if (!Number.isSafeInteger(track.ttl) || track.ttl <= 0) {
      throw new Error('跟踪观察期限必须为大于 0 的整数')
    }
    if (!Number.isSafeInteger(track.max_age ?? track.ttl) || (track.max_age ?? track.ttl) < track.ttl) {
      throw new Error('跟踪总期限必须为不小于观察期限的整数')
    }
    if (!Number.isFinite(track.points ?? 0) || track.steps?.some((step) => !Number.isFinite(step.points ?? 0))) {
      throw new Error('请填写完整、有效的跟踪分值；只观察时填 0')
    }
    let capture = { ...track.capture }
    if (Object.hasOwn(captureTexts, track.id)) {
      capture = {}
      for (const line of captureTexts[track.id].split('\n').filter((value) => value.trim())) {
        const match = line.match(/^\s*(A_[A-Z0-9_]+)\s*=\s*(.+)$/)
        if (!match || Object.hasOwn(capture, match[1])) {
          throw new Error('冻结值每行填写一个不重复的 A_变量 = 表达式')
        }
        capture[match[1]] = match[2]
      }
    }
    return {
      ...track,
      name: track.name.trim(),
      capture,
      start: track.start?.trim() || undefined,
      invalidate: track.invalidate?.trim() || undefined,
      eligible: track.eligible?.trim() || undefined,
      signal: track.signal?.trim() || undefined,
      score_until: track.score_until?.trim() || undefined,
    }
  })
}
