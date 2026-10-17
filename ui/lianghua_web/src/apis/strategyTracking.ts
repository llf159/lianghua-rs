import { invoke } from '@tauri-apps/api/core'

export type TrackDefinition = {
  id: string
  name: string
  enabled: boolean
  source?: string
  start?: string
  ttl: number
  capture?: Record<string, string>
  invalidate?: string
  eligible?: string
  steps?: { name: string; when: string; points?: number }[]
  signal?: string
  points?: number
  score_until?: string
}

export type TrackingRow = {
  track_id: string
  ts_code: string
  name: string
  started: string
  age: number
  observing: boolean
  captured: Record<string, number>
  source_score: number | null
  completed: string[]
  stopped: string | null
  reason: string | null
  first_signal: string | null
  signal_today: boolean
  eligible_today: boolean
  score: number
}

export type TrackingPage = {
  definitions: TrackDefinition[]
  sources: { id: string; name: string; enabled: boolean }[]
  as_of: string | null
  stale: boolean
  computed_symbols: number
  resumed_symbols: number
  warnings: string[]
  rows: TrackingRow[]
}

export const getStrategyTrackingPage = (sourcePath: string) =>
  invoke<TrackingPage>('get_strategy_tracking_page', { sourcePath })
export const saveStrategyTrack = (sourcePath: string, draft: TrackDefinition) =>
  invoke<TrackingPage>('save_strategy_track', { sourcePath, draft })
export const removeStrategyTrack = (sourcePath: string, id: string) =>
  invoke<TrackingPage>('remove_strategy_track', { sourcePath, id })
export const runStrategyTracking = (sourcePath: string, reference: string) =>
  invoke<TrackingPage>('run_strategy_tracking', {
    sourcePath,
    reference: reference || null,
  })
