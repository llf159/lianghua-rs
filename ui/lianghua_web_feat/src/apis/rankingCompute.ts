import { invoke } from '@tauri-apps/api/core'

export type RankComputeDbRange = {
  fileName: string
  tableName: string
  exists: boolean
  minTradeDate: string | null
  maxTradeDate: string | null
  distinctTradeDates: number
  rowCount: number
}

export type RankComputeResultContinuity = {
  checked: boolean
  isContinuous: boolean
  rangeStart: string | null
  rangeEnd: string | null
  expectedTradeDates: number
  actualTradeDates: number
  missingTradeDatesCount: number
  missingTradeDatesSample: string[]
  unexpectedTradeDatesCount: number
  unexpectedTradeDatesSample: string[]
}

export type RankingComputeStatus = {
  sourcePath: string
  sourceDb: RankComputeDbRange
  resultDb: RankComputeDbRange
  resultDbContinuity: RankComputeResultContinuity
  suggestedStartDate: string | null
  suggestedEndDate: string | null
}

export type RankComputeTimingItem = {
  key: string
  label: string
  elapsedMs: number
  note: string | null
}

export type RankingComputeRunResult = {
  action: 'score' | 'tiebreak' | string
  startDate?: string | null
  endDate?: string | null
  elapsedMs: number
  timings: RankComputeTimingItem[]
  status: RankingComputeStatus
}

export async function getRankingComputeStatus(sourcePath: string) {
  return invoke<RankingComputeStatus>('get_ranking_compute_status', { sourcePath })
}

export type ConceptPerformanceComputeResult = {
  action: string
  elapsedMs: number
  savedRows: number
}

export async function runRankingScoreCalculation(sourcePath: string, startDate: string, endDate: string) {
  return invoke<RankingComputeRunResult>('run_ranking_score_calculation', {
    sourcePath,
    startDate,
    endDate,
  })
}

export async function runConceptPerformanceCompute(sourcePath: string) {
  return invoke<ConceptPerformanceComputeResult>('run_concept_performance_compute', { sourcePath })
}

export async function runRankingTiebreakFill(sourcePath: string) {
  return invoke<RankingComputeRunResult>('run_ranking_tiebreak_fill', { sourcePath })
}
