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

export type StrategyTriggerRankingMatch = {
  tsCode: string
  name?: string | null
  candidateStartTradeDate: string
  candidateEndTradeDate: string
  similarityScore: number
  forwardExcessReturnPct?: number | null
  mfePct: number
  maePct: number
}

export type StrategyTriggerRankingRow = {
  rank?: number | null
  tsCode: string
  name?: string | null
  industry?: string | null
  concept?: string | null
  board?: string | null
  totalMvYi?: number | null
  originalScore?: number | null
  originalRank?: number | null
  rankingScore?: number | null
  predictionSignal?: number | null
  confidence: number
  sampleCount: number
  effectiveSampleCount: number
  expectedReturnPct?: number | null
  expectedExcessReturnPct?: number | null
  shrunkExcessReturnPct?: number | null
  excessPositiveRate?: number | null
  expectedMfePct?: number | null
  expectedMaePct?: number | null
  averageSimilarity?: number | null
  bestSimilarity?: number | null
  triggerCount: number
  topMatches: StrategyTriggerRankingMatch[]
}

export type StrategyTriggerRankingTiming = {
  label: string
  elapsedMs: number
}

export type StrategyTriggerRankingPageData = {
  resolvedTradeDate: string
  historicalCutoffDate: string
  windowTradeDays: number
  poolSegments: number
  outcomeTradeDays: number
  benchmarkIndexCode: string
  algorithmVersion: string
  dataSignature: string
  generatedAtEpochSeconds?: number | null
  isFresh: boolean
  staleReason?: string | null
  universeCount: number
  rankedCount: number
  candidateUniverseCount: number
  candidateAnchorCount: number
  evaluatedAnchorCount: number
  elapsedMs?: number | null
  timings: StrategyTriggerRankingTiming[]
  items: StrategyTriggerRankingRow[]
}

export type StrategyTriggerRankingQuery = {
  sourcePath: string
  tradeDate?: string
  windowTradeDays?: number
  poolSegments?: number
  outcomeTradeDays?: number
  benchmarkIndexCode?: string
  limit?: number
  board?: string
  excludeStBoard?: boolean
  totalMvMin?: number
  totalMvMax?: number
}

export async function getStrategyTriggerSimilarityRankingPage(
  query: StrategyTriggerRankingQuery,
) {
  return invoke<StrategyTriggerRankingPageData>(
    'get_strategy_trigger_similarity_ranking_page',
    query,
  )
}

export async function runStrategyTriggerSimilarityRanking(
  query: StrategyTriggerRankingQuery,
) {
  return invoke<StrategyTriggerRankingPageData>(
    'run_strategy_trigger_similarity_ranking',
    query,
  )
}
