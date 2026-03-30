import { invoke } from '@tauri-apps/api/core'

export type StockPickOptionsData = {
  trade_date_options: string[]
  latest_trade_date?: string | null
  score_trade_date_options: string[]
  latest_score_trade_date?: string | null
  concept_options: string[]
  area_options: string[]
  industry_options: string[]
  strategy_options: string[]
}

export type StockPickRow = {
  ts_code: string
  name?: string | null
  board: string
  concept?: string | null
  rank?: number | null
  total_score?: number | null
  pick_note: string
}

export type StockPickResultData = {
  rows: StockPickRow[]
  resolved_start_date?: string | null
  resolved_end_date?: string | null
}

export type AdvancedStockPickRow = {
  ts_code: string
  name?: string | null
  board: string
  area?: string | null
  industry?: string | null
  concept?: string | null
  rank?: number | null
  total_score?: number | null
  adv_hit_cnt: number
  adv_score_sum: number
  pos_hit_cnt: number
  pos_score_sum: number
  all_hit_cnt: number
  all_score_sum: number
  noisy_companion_cnt: number
  advantage_hits: string
  companion_hits: string
  pick_note: string
}

export type AdvancedStockPickResultData = {
  rows: AdvancedStockPickRow[]
  resolved_trade_date?: string | null
  resolved_method_key: string
  resolved_method_label: string
  total_candidate_count: number
  eligible_candidate_count: number
  selected_count: number
  resolved_advantage_rule_names: string[]
  resolved_noisy_companion_rule_names: string[]
}

export type ExpressionStockPickQuery = {
  sourcePath: string
  board?: string
  referenceTradeDate?: string
  lookbackPeriods?: number
  scopeWay: string
  expression: string
  consecThreshold?: number
}

export type ConceptStockPickQuery = {
  sourcePath: string
  board?: string
  tradeDate?: string
  concepts: string[]
  matchMode: string
}

export type AdvancedStockPickQuery = {
  sourcePath: string
  tradeDate?: string
  board?: string
  area?: string
  industry?: string
  includeConcepts: string[]
  excludeConcepts: string[]
  conceptMatchMode?: string
  methodKey?: string
  selectedHorizon?: number
  strongQuantile?: number
  advantageRuleMode?: string
  manualRuleNames?: string[]
  autoMinSamples2?: number
  autoMinSamples3?: number
  autoMinSamples5?: number
  autoMinSamples10?: number
  requireWinRateAboveMarket?: boolean
  minPassHorizons?: number
  minAdvHits?: number
  topLimit?: number
  mixedSortKeys?: string[]
  noisyCompanionRuleNames?: string[]
  rankMax?: number
  totalScoreMin?: number
  totalScoreMax?: number
  totalMvMin?: number
  totalMvMax?: number
  circMvMin?: number
  circMvMax?: number
}

export async function getStockPickOptions(sourcePath: string) {
  return invoke<StockPickOptionsData>('get_stock_pick_options', { sourcePath })
}

export async function runExpressionStockPick(query: ExpressionStockPickQuery) {
  return invoke<StockPickResultData>('run_expression_stock_pick', query)
}

export async function runConceptStockPick(query: ConceptStockPickQuery) {
  return invoke<StockPickResultData>('run_concept_stock_pick', query)
}

export async function runAdvancedStockPick(query: AdvancedStockPickQuery) {
  return invoke<AdvancedStockPickResultData>('run_advanced_stock_pick', query)
}
