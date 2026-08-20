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
  resolved_board?: string | null
  exclude_st_board?: boolean
  total_mv_min?: number | null
  total_mv_max?: number | null
  min_samples_per_scene_day: number
  min_listed_trade_days: number
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
  avg_excess_residual_return?: number | null
  top_bottom_spread?: number | null
  ic?: number | null
}

export type RuleDecayValidation = {
  window_days: number
  recent_start_date?: string | null
  recent_end_date?: string | null
  recent_day_count: number
  prior_day_count: number
  recent_directional_excess_mean?: number | null
  prior_directional_excess_mean?: number | null
  decay_change?: number | null
  decay_t_value?: number | null
  status: 'significant_decay' | 'decay' | 'weakening' | 'weak' | 'stable' | 'improving' | 'insufficient' | string
  status_label: string
}

export type RuleLayerRuleSummary = {
  rule_name: string
  point_count: number
  avg_residual_mean?: number | null
  avg_excess_residual_mean?: number | null
  avg_er_change?: number | null
  profit_loss_ratio?: number | null
  spread_mean?: number | null
  avg_contribution_score?: number | null
  avg_contribution_per_trigger?: number | null
  ic_mean?: number | null
  ic_std?: number | null
  icir?: number | null
  ic_t_value?: number | null
  decay_validations?: RuleDecayValidation[]
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
  resolved_board?: string | null
  exclude_st_board?: boolean
  total_mv_min?: number | null
  total_mv_max?: number | null
  min_samples_per_rule_day: number
  min_listed_trade_days: number
  backtest_period: number
  points: RuleLayerPoint[]
  avg_residual_mean?: number | null
  avg_excess_residual_mean?: number | null
  decay_validations?: RuleDecayValidation[]
  avg_er_change?: number | null
  profit_loss_ratio?: number | null
  spread_mean?: number | null
  avg_contribution_score?: number | null
  avg_contribution_per_trigger?: number | null
  ic_mean?: number | null
  ic_std?: number | null
  icir?: number | null
  ic_t_value?: number | null
  layer_count?: number | null
  layer_method?: string | null
  layer_method_label?: string | null
  layer_summaries: RankLayerBucketSummary[]
  is_all_rules?: boolean
  all_rule_summaries?: RuleLayerRuleSummary[]
  rule_validation_details?: RuleValidationComboResult[]
}

export type RuleLayerBacktestDefaultsData = {
  rule_options: string[]
  resolved_rule_name?: string | null
  start_date?: string | null
  end_date?: string | null
}

export type RankLayerBucketSummary = {
  layer_index: number
  layer_label: string
  point_count: number
  sample_count: number
  avg_score?: number | null
  avg_residual_return?: number | null
  avg_er_change?: number | null
}

export type RankLayerSampleGroup = {
  layer_index: number
  layer_label: string
  total_samples: number
  triggered_days: number
  positive_count: number
  negative_count: number
  random_count: number
  positive: RuleValidationSampleRow[]
  negative: RuleValidationSampleRow[]
  random: RuleValidationSampleRow[]
}

export type RankLayerBacktestData = {
  stock_adj_type: string
  index_ts_code: string
  index_beta: number
  concept_beta: number
  industry_beta: number
  start_date: string
  end_date: string
  resolved_board?: string | null
  exclude_st_board?: boolean
  market_value_grouping?: boolean
  min_samples_per_rank_day: number
  min_listed_trade_days: number
  backtest_period: number
  layer_count: number
  layer_method: RankLayerMethod
  layer_method_label: string
  point_count: number
  sample_count: number
  avg_er_change?: number | null
  spread_mean?: number | null
  ic_mean?: number | null
  ic_std?: number | null
  icir?: number | null
  ic_t_value?: number | null
  top_k_summaries?: RankTopKSummary[]
  top_k_period_summaries?: RankTopKPeriodSummary[]
  layer_summaries: RankLayerBucketSummary[]
  layer_sample_groups?: RankLayerSampleGroup[]
  market_value_summaries?: RankLayerMarketValueSummary[]
}

export type RankTopKSummary = {
  top_k: number
  point_count: number
  sample_count: number
  avg_daily_residual_return?: number | null
  median_daily_residual_return?: number | null
  positive_day_ratio?: number | null
  daily_std?: number | null
  hac_t_value?: number | null
  hac_lag: number
}

export type RankTopKPeriodSummary = {
  period_label: string
  start_date: string
  end_date: string
  top_k: number
  point_count: number
  sample_count: number
  avg_daily_residual_return?: number | null
  median_daily_residual_return?: number | null
  positive_day_ratio?: number | null
  hac_t_value?: number | null
  hac_lag: number
}

export type RankLayerMarketValueSummary = {
  group_label: string
  total_mv_min?: number | null
  total_mv_max?: number | null
  point_count: number
  sample_count: number
  avg_er_change?: number | null
  spread_mean?: number | null
  ic_mean?: number | null
  ic_t_value?: number | null
  icir?: number | null
}

export type RankLayerMethod = "score" | "sample_count" | "rank"

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

export type RuleValidationTriggerCountStats = RuleValidationSampleStats & {
  trigger_count: number
}

export type RuleValidationSampleRow = {
  ts_code: string
  name?: string | null
  board: string
  volatility_group: string
  trade_date: string
  trigger_count: number
  rule_score: number
  residual_return: number
}

export type RuleValidationSampleGroups = {
  positive: RuleValidationSampleRow[]
  negative: RuleValidationSampleRow[]
  random: RuleValidationSampleRow[]
}

export type RuleValidationReturnDistributionBucket = {
  bucket_label: string
  sample_count: number
  sample_ratio?: number | null
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
  trigger_count_stats: RuleValidationTriggerCountStats[]
  sample_groups: RuleValidationSampleGroups
  return_distribution: RuleValidationReturnDistributionBucket[]
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
  continuation_id?: string | null
}

export type RuleExpressionCalibrationBucket = {
  score_multiplier: number
  sample_count: number
  avg_residual_return?: number | null
}

export type RuleExpressionCalibrationDistancePoint = {
  min: number
  max: number
  points: number
}

export type RuleExpressionCalibrationCandidate = {
  candidate_key: string
  scope_way: string
  scope_label: string
  scope_windows: number
  is_current: boolean
  trigger_samples: number
  triggered_days: number
  avg_daily_trigger: number
  avg_residual_mean?: number | null
  avg_excess_residual_mean?: number | null
  daily_std?: number | null
  standard_error?: number | null
  conservative_edge?: number | null
  early_excess_residual_mean?: number | null
  late_excess_residual_mean?: number | null
  ic_mean?: number | null
  ic_t_value?: number | null
  score_monotonicity?: number | null
  avg_score_multiplier?: number | null
  suggested_points: number
  suggested_total_points: number
  calibration_score: number
  status: string
  status_label: string
  score_buckets: RuleExpressionCalibrationBucket[]
  suggested_dist_points: RuleExpressionCalibrationDistancePoint[]
}

export type RuleExpressionCalibrationData = {
  continuation_id: string
  combo_key: string
  combo_label: string
  direction: string
  candidate_count: number
  point_scale_description: string
  recommended_candidate_key?: string | null
  candidates: RuleExpressionCalibrationCandidate[]
}

export type MarketRankItem = {
  name: string
  value: number
  ts_code?: string | null
  start_date?: string | null
  end_date?: string | null
  concepts?: string | null
  three_day_gain?: number | null
  five_day_gain?: number | null
}

export type MarketAnalysisSnapshot = {
  trade_date?: string | null
  concept_top: MarketRankItem[]
  industry_top: MarketRankItem[]
  concept_money_flow_top: MarketRankItem[]
  industry_money_flow_top: MarketRankItem[]
  concept_money_outflow_top: MarketRankItem[]
  industry_money_outflow_top: MarketRankItem[]
  gain_top: MarketRankItem[]
  sub_interval_gain_top: MarketRankItem[]
}

export type MarketAnalysisData = {
  lookback_period: number
  stock_rank_limit: number
  sub_interval_period: number
  min_board_stock_count: number
  latest_trade_date?: string | null
  resolved_reference_trade_date?: string | null
  board_options: string[]
  resolved_board?: string | null
  interval: MarketAnalysisSnapshot
  daily: MarketAnalysisSnapshot
}

export type DragonTigerMarketSummary = {
  top_list_rows: number
  stock_count: number
  top_inst_rows: number
  total_l_buy: number
  total_l_sell: number
  total_net_amount: number
}

export type DragonTigerTopListItem = {
  trade_date: string
  ts_code: string
  name: string
  close?: number | null
  pct_change?: number | null
  turnover_rate?: number | null
  amount?: number | null
  l_sell?: number | null
  l_buy?: number | null
  l_amount?: number | null
  net_amount?: number | null
  net_rate?: number | null
  amount_rate?: number | null
  float_values?: number | null
  reason: string
}

export type DragonTigerTopInstItem = {
  trade_date: string
  ts_code: string
  exalter: string
  buy?: number | null
  buy_rate?: number | null
  sell?: number | null
  sell_rate?: number | null
  net_buy?: number | null
  side: string
  reason: string
}

export type DragonTigerMarketData = {
  db_exists: boolean
  latest_sync_trade_date?: string | null
  resolved_trade_date?: string | null
  available_trade_dates: string[]
  summary: DragonTigerMarketSummary
  top_list: DragonTigerTopListItem[]
  top_inst: DragonTigerTopInstItem[]
}

export type DragonTigerStockDetailData = {
  ts_code: string
  name: string
  resolved_trade_date?: string | null
  current_list: DragonTigerTopListItem[]
  seats: DragonTigerTopInstItem[]
  history: DragonTigerTopListItem[]
  history_trade_count: number
  history_record_count: number
}

export type DragonTigerSeatStatisticsSummary = {
  appearance_count: number
  trade_date_count: number
  stock_count: number
  buy_count: number
  sell_count: number
  total_buy: number
  total_sell: number
  total_net_buy: number
}

export type DragonTigerSeatStatisticsRow = {
  trade_date: string
  ts_code: string
  name: string
  buy?: number | null
  sell?: number | null
  net_buy?: number | null
  side: string
  reason: string
}

export type DragonTigerSeatFavoriteStock = {
  ts_code: string
  name: string
  appearance_count: number
  total_buy: number
  total_sell: number
  total_net_buy: number
}

export type DragonTigerSeatStatisticsData = {
  exalter: string
  summary: DragonTigerSeatStatisticsSummary
  favorite_stocks: DragonTigerSeatFavoriteStock[]
  recent_records: DragonTigerSeatStatisticsRow[]
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
  minListedTradeDays?: number
  backtestPeriod?: number
  board?: string
  excludeStBoard?: boolean
  totalMvMin?: number
  totalMvMax?: number
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
  minListedTradeDays?: number
  backtestPeriod?: number
  parallelBatchSize?: number
  board?: string
  excludeStBoard?: boolean
  totalMvMin?: number
  totalMvMax?: number
}

export type RankLayerBacktestQuery = {
  sourcePath: string
  stockAdjType?: string
  indexTsCode: string
  indexBeta?: number
  conceptBeta?: number
  industryBeta?: number
  startDate: string
  endDate: string
  minSamplesPerRankDay?: number
  minListedTradeDays?: number
  backtestPeriod?: number
  layerCount?: number
  layerMethod?: RankLayerMethod
  board?: string
  excludeStBoard?: boolean
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
  minListedTradeDays?: number
  backtestPeriod?: number
  unknownConfigs?: RuleValidationUnknownConfig[]
  sampleLimitPerGroup?: number
  board?: string
  excludeStBoard?: boolean
  totalMvMin?: number
  totalMvMax?: number
}

export async function getSceneLayerBacktestDefaults(sourcePath: string) {
  return invoke<SceneLayerBacktestDefaultsData>('get_scene_layer_backtest_defaults', { sourcePath })
}

export async function runSceneLayerBacktest(query: SceneLayerBacktestQuery) {
  return invoke<SceneLayerBacktestData>('run_scene_layer_backtest', query)
}

export async function runTransientSceneLayerBacktest(query: SceneLayerBacktestQuery) {
  return invoke<SceneLayerBacktestData>('run_transient_scene_layer_backtest', query)
}

export async function getRuleLayerBacktestDefaults(sourcePath: string) {
  return invoke<RuleLayerBacktestDefaultsData>('get_rule_layer_backtest_defaults', { sourcePath })
}

export async function runRuleLayerBacktest(query: RuleLayerBacktestQuery) {
  return invoke<RuleLayerBacktestData>('run_rule_layer_backtest', query)
}

export async function runTransientRuleLayerBacktest(query: RuleLayerBacktestQuery) {
  return invoke<RuleLayerBacktestData>('run_transient_rule_layer_backtest', query)
}

export async function runRankLayerBacktest(query: RankLayerBacktestQuery) {
  return invoke<RankLayerBacktestData>('run_rank_layer_backtest', query)
}

export async function runTransientRankLayerBacktest(query: RankLayerBacktestQuery) {
  return invoke<RankLayerBacktestData>('run_transient_rank_layer_backtest', query)
}

export async function runRuleExpressionValidation(query: RuleExpressionValidationQuery) {
  return invoke<RuleExpressionValidationData>('run_rule_expression_validation', query)
}

export async function runRuleExpressionCalibration(continuationId: string, comboKey: string) {
  return invoke<RuleExpressionCalibrationData>('run_rule_expression_calibration', {
    continuationId,
    comboKey,
  })
}

export async function getMarketAnalysis(query: {
  sourcePath: string
  lookbackPeriod?: number
  referenceTradeDate?: string
  board?: string
  excludeStBoard?: boolean
  minListedTradeDays?: number
  stockRankLimit?: number
  subIntervalPeriod?: number
  minBoardStockCount?: number
}) {
  return invoke<MarketAnalysisData>('get_market_analysis', query)
}

export async function getDragonTigerMarketData(query: {
  sourcePath: string
  referenceTradeDate?: string
}) {
  return invoke<DragonTigerMarketData>('get_dragon_tiger_market_data', query)
}

export async function getDragonTigerStockDetail(query: {
  sourcePath: string
  tsCode: string
  tradeDate: string
}) {
  return invoke<DragonTigerStockDetailData>('get_dragon_tiger_stock_detail', query)
}

export async function getDragonTigerSeatStatistics(query: {
  sourcePath: string
  exalter: string
}) {
  return invoke<DragonTigerSeatStatisticsData>('get_dragon_tiger_seat_statistics', query)
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
