import { invoke } from '@tauri-apps/api/core'

export type StrategyTriggerSimilarityMatchedEvent = {
  ruleName: string
  targetTradeDate: string
  candidateTradeDate: string
  dateGapTradeDays: number
  eventScore: number
}

export type StrategyTriggerSimilarityTarget = {
  tsCode: string
  name?: string | null
  industry?: string | null
  concept?: string | null
  startTradeDate: string
  endTradeDate: string
  triggerCount: number
  ruleNames: string[]
}

export type StrategyTriggerSimilarityRow = {
  tsCode: string
  name?: string | null
  industry?: string | null
  concept?: string | null
  candidateStartTradeDate: string
  candidateEndTradeDate: string
  similarityScore: number
  matchedEventCount: number
  targetTriggerCount: number
  candidateTriggerCount: number
  matchedRuleCount: number
  avgDateGapTradeDays?: number | null
  matchedRuleNames: string[]
  matchedEvents: StrategyTriggerSimilarityMatchedEvent[]
  totalScore?: number | null
  rank?: number | null
}

export type StrategyTriggerSimilarityPageData = {
  resolvedTradeDate: string
  resolvedTsCode: string
  windowTradeDays: number
  maxGapTradeDays: number
  target: StrategyTriggerSimilarityTarget
  items: StrategyTriggerSimilarityRow[]
}

export type StrategyTriggerSimilarityQuery = {
  sourcePath: string
  tradeDate?: string
  tsCode: string
  windowTradeDays?: number
  maxGapTradeDays?: number
  limit?: number
}

export async function getStrategyTriggerSimilarityPage(query: StrategyTriggerSimilarityQuery) {
  return invoke<StrategyTriggerSimilarityPageData>('get_strategy_trigger_similarity_page', query)
}
