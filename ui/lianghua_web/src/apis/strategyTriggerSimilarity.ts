import { invoke } from '@tauri-apps/api/core'

export type StrategyTriggerSimilarityTarget = {
  tsCode: string
  name?: string | null
  industry?: string | null
  concept?: string | null
  startTradeDate: string
  endTradeDate: string
  triggerCount: number
  ruleNames: string[]
  pooledFeatureDimension: number
}

export type StrategyTriggerSimilarityOutcomeSummary = {
  sampleCount: number
  effectiveSampleCount: number
  weightedReturnPct?: number | null
  weightedExcessReturnPct?: number | null
  shrunkExcessReturnPct?: number | null
  weightedPositiveRate?: number | null
  weightedMedianExcessReturnPct?: number | null
  winsorizedExcessReturnPct?: number | null
  weightedExcessPositiveRate?: number | null
  weightedMfePct?: number | null
  weightedMaePct?: number | null
}

export type StrategyTriggerSimilarityRow = {
  tsCode: string
  name?: string | null
  industry?: string | null
  concept?: string | null
  candidateStartTradeDate: string
  candidateEndTradeDate: string
  outcomeStartTradeDate: string
  outcomeEndTradeDate: string
  similarityScore: number
  triggerSimilarity: number
  priceVolumeSimilarity?: number | null
  indicatorSimilarity?: number | null
  marketSimilarity?: number | null
  candidateTriggerCount: number
  matchedRuleCount: number
  matchedRuleNames: string[]
  forwardReturnPct: number
  forwardExcessReturnPct?: number | null
  mfePct: number
  maePct: number
  totalScore?: number | null
  rank?: number | null
}

export type StrategyTriggerSimilarityPageData = {
  resolvedTradeDate: string
  resolvedTsCode: string
  windowTradeDays: number
  poolSegments: number
  outcomeTradeDays: number
  historicalCutoffDate: string
  benchmarkIndexCode: string
  kernelNames: string[]
  indicatorColumns: string[]
  candidateUniverseCount: number
  candidateAnchorCount: number
  evaluatedAnchorCount: number
  candidatePoolTruncated: boolean
  target: StrategyTriggerSimilarityTarget
  outcomeSummary: StrategyTriggerSimilarityOutcomeSummary
  items: StrategyTriggerSimilarityRow[]
}

export type StrategyTriggerSimilarityQuery = {
  sourcePath: string
  tradeDate?: string
  tsCode: string
  windowTradeDays?: number
  poolSegments?: number
  outcomeTradeDays?: number
  benchmarkIndexCode?: string
  limit?: number
}

export async function listStrategyTriggerSimilarityBenchmarkIndexCodes() {
  return invoke<string[]>('list_strategy_trigger_similarity_benchmark_index_codes')
}

export async function getStrategyTriggerSimilarityPage(query: StrategyTriggerSimilarityQuery) {
  return invoke<StrategyTriggerSimilarityPageData>('get_strategy_trigger_similarity_page', query)
}
