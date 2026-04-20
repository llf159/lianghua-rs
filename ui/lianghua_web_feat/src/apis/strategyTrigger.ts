import { invoke } from '@tauri-apps/api/core'

export type StrategyHeatmapCell = {
  trade_date: string
  day_level?: number | null
  avg_level?: number | null
  delta_level?: number | null
  above_avg?: boolean | null
}

export type StrategyOverviewPayload = {
  items?: StrategyHeatmapCell[]
  latest_trade_date?: string | null
  average_level?: number | null
}

export type StrategyDailyRow = {
  trade_date: string
  rule_name: string
  trigger_mode?: string | null
  sample_count?: number | null
  trigger_count?: number | null
  coverage?: number | null
  contribution_score?: number | null
  contribution_per_trigger?: number | null
  median_trigger_count?: number | null
  top100_trigger_count?: number | null
  best_rank?: number | null
}

export type StrategyChartPoint = {
  trade_date: string
  trigger_count?: number | null
  top100_trigger_count?: number | null
  coverage?: number | null
}

export type StrategyChartPayload = {
  items?: StrategyChartPoint[]
}

export type TriggeredStockRow = {
  rank?: number | null
  ts_code: string
  name?: string | null
  total_score?: number | null
  rule_score?: number | null
  concept?: string | null
}

export type StrategyStatisticsPageData = {
  overview?: StrategyOverviewPayload | null
  detail_rows?: StrategyDailyRow[]
  strategy_options?: string[]
  resolved_strategy_name?: string | null
  analysis_trade_date_options?: string[]
  resolved_analysis_trade_date?: string | null
  chart?: StrategyChartPayload | null
  triggered_stocks?: TriggeredStockRow[]
}

export type StrategyStatisticsDetailData = {
  strategy_name: string
  analysis_trade_date_options: string[]
  resolved_analysis_trade_date?: string | null
  selected_daily_row?: StrategyDailyRow | null
  chart?: StrategyChartPayload | null
  triggered_stocks: TriggeredStockRow[]
}

export type SceneStageRow = {
  stage: string
  sample_count: number
  stage_ratio_in_scene?: number | null
}

export type SceneContributionSummary = {
  scene_covered_count: number
  scene_total_sample_count: number
  scene_coverage_ratio?: number | null
  scene_rule_contribution_score?: number | null
  all_rule_contribution_score?: number | null
  scene_rule_contribution_ratio?: number | null
}

export type SceneStatisticsPageData = {
  scene_options?: string[]
  resolved_scene_name?: string | null
  analysis_trade_date_options?: string[]
  resolved_analysis_trade_date?: string | null
  stage_rows?: SceneStageRow[]
  summary?: SceneContributionSummary | null
}

export type StrategyStatisticsQuery = {
  sourcePath: string
  strategyName?: string
  analysisTradeDate?: string
}

export type StrategyTriggeredStocksQuery = {
  sourcePath: string
  strategyName: string
  analysisTradeDate: string
}

export type SceneStatisticsQuery = {
  sourcePath: string
  sceneName?: string
  analysisTradeDate?: string
}

export async function getStrategyStatisticsPage(query: StrategyStatisticsQuery) {
  return invoke<StrategyStatisticsPageData>('get_strategy_statistics_page', query)
}

export async function getStrategyStatisticsDetail(query: {
  sourcePath: string
  strategyName: string
  analysisTradeDate?: string
}) {
  return invoke<StrategyStatisticsDetailData>('get_strategy_statistics_detail', query)
}

export async function getStrategyTriggeredStocks(query: StrategyTriggeredStocksQuery) {
  return invoke<TriggeredStockRow[]>('get_strategy_triggered_stocks', query)
}

export async function getSceneStatisticsPage(query: SceneStatisticsQuery) {
  return invoke<SceneStatisticsPageData>('get_scene_statistics_page', query)
}

export type SceneLayerStateAvgResidualReturn = {
  scene_state: string
  avg_residual_return?: number | null
}

export type SceneLayerPoint = {
  trade_date: string
  state_avg_residual_returns: SceneLayerStateAvgResidualReturn[]
  top_bottom_spread?: number | null
  ic?: number | null
}

export type SceneLayerSceneSummary = {
  scene_name: string
  point_count: number
  spread_mean?: number | null
  ic_mean?: number | null
  ic_std?: number | null
  icir?: number | null
  ic_t_value?: number | null
}

export type SceneLayerBacktestData = {
  scene_name: string
  stock_adj_type: string
  index_ts_code: string
  index_beta: number
  concept_beta: number
  industry_beta: number
  start_date: string
  end_date: string
  min_samples_per_scene_day: number
  backtest_period: number
  points: SceneLayerPoint[]
  spread_mean?: number | null
  ic_mean?: number | null
  ic_std?: number | null
  icir?: number | null
  ic_t_value?: number | null
  is_all_scenes?: boolean
  all_scene_summaries?: SceneLayerSceneSummary[]
}

export type SceneLayerBacktestDefaultsData = {
  scene_options: string[]
  resolved_scene_name?: string | null
  start_date?: string | null
  end_date?: string | null
}

export type RuleLayerPoint = {
  trade_date: string
  sample_count: number
  avg_rule_score?: number | null
  avg_residual_return?: number | null
  top_bottom_spread?: number | null
  ic?: number | null
}

export type RuleLayerRuleSummary = {
  rule_name: string
  point_count: number
  avg_residual_mean?: number | null
  spread_mean?: number | null
  ic_mean?: number | null
  ic_std?: number | null
  icir?: number | null
  ic_t_value?: number | null
}

export type RuleLayerBacktestData = {
  rule_name: string
  stock_adj_type: string
  index_ts_code: string
  index_beta: number
  concept_beta: number
  industry_beta: number
  start_date: string
  end_date: string
  min_samples_per_rule_day: number
  backtest_period: number
  points: RuleLayerPoint[]
  avg_residual_mean?: number | null
  spread_mean?: number | null
  ic_mean?: number | null
  ic_std?: number | null
  icir?: number | null
  ic_t_value?: number | null
  is_all_rules?: boolean
  all_rule_summaries?: RuleLayerRuleSummary[]
}

export type RuleLayerBacktestDefaultsData = {
  rule_options: string[]
  resolved_rule_name?: string | null
  start_date?: string | null
  end_date?: string | null
}

export type RuleValidationUnknownConfig = {
  name: string
  start: number
  end: number
  step: number
}

export type RuleValidationUnknownValue = {
  name: string
  value: number
}

export type RuleValidationSimilarityRow = {
  rule_name: string
  explain?: string | null
  overlap_samples: number
  overlap_rate_vs_validation?: number | null
  overlap_rate_vs_existing?: number | null
  overlap_lift?: number | null
}

export type RuleValidationSampleStats = {
  positive_count: number
  negative_count: number
  random_count: number
  total_samples: number
}

export type RuleValidationSampleRow = {
  ts_code: string
  name?: string | null
  board: string
  volatility_group: string
  trade_date: string
  rule_score: number
  residual_return: number
}

export type RuleValidationSampleGroups = {
  positive: RuleValidationSampleRow[]
  negative: RuleValidationSampleRow[]
  random: RuleValidationSampleRow[]
}

export type RuleValidationComboResult = {
  combo_key: string
  combo_label: string
  formula: string
  unknown_values: RuleValidationUnknownValue[]
  trigger_samples: number
  triggered_days: number
  avg_daily_trigger: number
  sample_stats: RuleValidationSampleStats
  sample_groups: RuleValidationSampleGroups
  backtest: RuleLayerBacktestData
  similarity_rows: RuleValidationSimilarityRow[]
}

export type RuleExpressionValidationData = {
  import_rule_name: string
  import_rule_explain: string
  scope_way: string
  scope_windows: number
  sample_limit_per_group: number
  combo_results: RuleValidationComboResult[]
  best_combo_key?: string | null
}

export type MarketRankItem = {
  name: string
  value: number
}

export type MarketAnalysisSnapshot = {
  trade_date?: string | null
  concept_top: MarketRankItem[]
  industry_top: MarketRankItem[]
  gain_top: MarketRankItem[]
}

export type MarketAnalysisData = {
  lookback_period: number
  latest_trade_date?: string | null
  resolved_reference_trade_date?: string | null
  board_options: string[]
  resolved_board?: string | null
  interval: MarketAnalysisSnapshot
  daily: MarketAnalysisSnapshot
}

export type MarketContributorItem = {
  ts_code: string
  name?: string | null
  industry?: string | null
  contribution_pct: number
}

export type MarketContributionData = {
  scope: string
  kind: string
  name: string
  trade_date?: string | null
  start_date?: string | null
  end_date?: string | null
  lookback_period: number
  contributors: MarketContributorItem[]
}

export type SceneLayerBacktestQuery = {
  sourcePath: string
  stockAdjType?: string
  indexTsCode: string
  indexBeta?: number
  conceptBeta?: number
  industryBeta?: number
  startDate: string
  endDate: string
  minSamplesPerSceneDay?: number
  backtestPeriod?: number
}

export type RuleLayerBacktestQuery = {
  sourcePath: string
  stockAdjType?: string
  indexTsCode: string
  indexBeta?: number
  conceptBeta?: number
  industryBeta?: number
  startDate: string
  endDate: string
  minSamplesPerRuleDay?: number
  backtestPeriod?: number
}

export type RuleExpressionValidationQuery = {
  sourcePath: string
  importRuleName: string
  manualStrategy?: {
    name?: string
    sceneName?: string
    stage?: string
    scopeWay?: string
    scopeWindows?: number
    when?: string
    points?: number
    distPoints?: Array<{ min: number; max: number; points: number }> | null
    explain?: string
    tag?: string
  }
  when?: string
  scopeWay?: string
  scopeWindows?: number
  stockAdjType?: string
  indexTsCode: string
  indexBeta?: number
  conceptBeta?: number
  industryBeta?: number
  startDate: string
  endDate: string
  minSamplesPerRuleDay?: number
  backtestPeriod?: number
  unknownConfigs?: RuleValidationUnknownConfig[]
  sampleLimitPerGroup?: number
}

export async function getSceneLayerBacktestDefaults(sourcePath: string) {
  return invoke<SceneLayerBacktestDefaultsData>('get_scene_layer_backtest_defaults', { sourcePath })
}

export async function runSceneLayerBacktest(query: SceneLayerBacktestQuery) {
  return invoke<SceneLayerBacktestData>('run_scene_layer_backtest', query)
}

export async function getRuleLayerBacktestDefaults(sourcePath: string) {
  return invoke<RuleLayerBacktestDefaultsData>('get_rule_layer_backtest_defaults', { sourcePath })
}

export async function runRuleLayerBacktest(query: RuleLayerBacktestQuery) {
  return invoke<RuleLayerBacktestData>('run_rule_layer_backtest', query)
}

export async function runRuleExpressionValidation(query: RuleExpressionValidationQuery) {
  return invoke<RuleExpressionValidationData>('run_rule_expression_validation', query)
}

export async function getMarketAnalysis(query: {
  sourcePath: string
  lookbackPeriod?: number
  referenceTradeDate?: string
  board?: string
  excludeStBoard?: boolean
}) {
  return invoke<MarketAnalysisData>('get_market_analysis', query)
}

export async function getMarketContribution(query: {
  sourcePath: string
  scope: 'interval' | 'daily'
  kind: 'concept' | 'industry'
  name: string
  lookbackPeriod?: number
  referenceTradeDate?: string
}) {
  return invoke<MarketContributionData>('get_market_contribution', query)
}
