export type TrackDefinition = {
  id: string
  name: string
  enabled: boolean
  source?: string
  start?: string
  ttl: number
  max_age?: number
  capture?: Record<string, string>
  invalidate?: string
  eligible?: string
  steps?: { name: string; when: string; points?: number }[]
  signal?: string
  points?: number
  score_until?: string
}
