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

export type StrategySimilarityActiveConfig = {
  algorithmVersion: string
  windowTradeDays: number
  poolSegments: number
  outcomeTradeDays: number
  benchmarkIndexCode: string
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
  strategyPath: string
  sourceDb: RankComputeDbRange
  resultDb: RankComputeDbRange
  similarityRankDb: RankComputeDbRange
  similarityActiveConfig?: StrategySimilarityActiveConfig | null
  conceptPerformanceDb: RankComputeDbRange
  resultDbContinuity: RankComputeResultContinuity
  cyqDb: RankComputeDbRange
  cyqBinRowCount: number
  cyqFactor: number | null
  cyqChenDb: RankComputeDbRange
  cyqChenBinRowCount: number
  cyqChenWarmupDays: number | null
  cyqChenBucketPct: number | null
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
  warnings: string[]
  status: RankingComputeStatus
}

export type ConvolutionRankComputeResult = {
  action: string
  kernelName: string
  windowSize: number
  startDate: string
  endDate: string
  elapsedMs: number
  savedRows: number
  tradeDates: number
}

export async function getRankingComputeStatus(sourcePath: string, strategyPath?: string) {
  return invoke<RankingComputeStatus>('get_ranking_compute_status', { sourcePath, strategyPath })
}

export async function previewRankingScoreCalculationWarnings(
  sourcePath: string,
  startDate: string,
  endDate: string,
  strategyPath?: string,
) {
  return invoke<string[]>('preview_ranking_score_calculation_warnings', {
    sourcePath,
    strategyPath,
    startDate,
    endDate,
  })
}

export type ConceptPerformanceComputeResult = {
  action: string
  elapsedMs: number
  savedRows: number
}

export type CyqComputeResult = {
  action: string
  startDate?: string | null
  endDate?: string | null
  elapsedMs: number
  snapshotRows: number
  binRows: number
  factor: number
  range: number
}

export type CyqChenComputeResult = {
  action: string
  startDate?: string | null
  endDate?: string | null
  elapsedMs: number
  snapshotRows: number
  binRows: number
  warmupDays: number
  bucketPct: number
}

export async function runRankingScoreCalculation(
  sourcePath: string,
  startDate: string,
  endDate: string,
  strategyPath?: string,
) {
  return invoke<RankingComputeRunResult>('run_ranking_score_calculation', {
    sourcePath,
    strategyPath,
    startDate,
    endDate,
  })
}

export async function runConvolutionRankCompute(
  sourcePath: string,
  startDate: string,
  endDate: string,
) {
  return invoke<ConvolutionRankComputeResult>('run_convolution_rank_compute', {
    sourcePath,
    startDate,
    endDate,
  })
}

export async function runConceptPerformanceCompute(sourcePath: string) {
  return invoke<ConceptPerformanceComputeResult>('run_concept_performance_compute', { sourcePath })
}

export async function runCyqCompute(
  sourcePath: string,
  factor: number,
  startDate?: string,
  endDate?: string,
  downloadId?: string,
) {
  return invoke<CyqComputeResult>('run_cyq_compute', {
    sourcePath,
    factor,
    startDate,
    endDate,
    downloadId,
  })
}

export async function runCyqChenCompute(
  sourcePath: string,
  warmupDays: number,
  bucketPct: number,
  startDate?: string,
  endDate?: string,
  downloadId?: string,
) {
  return invoke<CyqChenComputeResult>('run_cyq_chen_compute', {
    sourcePath,
    warmupDays,
    bucketPct,
    startDate,
    endDate,
    downloadId,
  })
}

export async function runRankingTiebreakFill(sourcePath: string, strategyPath?: string) {
  return invoke<RankingComputeRunResult>('run_ranking_tiebreak_fill', { sourcePath, strategyPath })
}
