export type BacktestReturnMetric = "residual_return" | "rise_amount"

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
  return_metric: BacktestReturnMetric
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
  return_metric: BacktestReturnMetric
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
  return_metric: BacktestReturnMetric
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
  fixed_top_k_summaries?: FixedRankTopKSummary[]
  layer_summaries: RankLayerBucketSummary[]
  layer_sample_groups?: RankLayerSampleGroup[]
  market_value_summaries?: RankLayerMarketValueSummary[]
}

export type FixedRankTopKSummary = {
  period_label: string
  top_k: number
  ranking_days: number
  calendar_days?: number | null
  quarter_status?: string
  observed_days: number
  member_count: number
  ineligible_count: number
  unknown_eligibility_count: number
  label_count: number
  avg_daily_return?: number | null
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
  member_count: number
  label_count: number
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
  period?: string
  universe_samples?: number | null
  candidate_available_samples?: number | null
  baseline_available_samples?: number | null
  shared_score_samples?: number | null
  rule_name: string
  explain?: string | null
  overlap_samples: number
  overlap_rate_vs_validation?: number | null
  overlap_rate_vs_existing?: number | null
  overlap_lift?: number | null
  jaccard?: number | null
  phi?: number | null
  score_pearson?: number | null
  return_pearson?: number | null
  shared_return_days: number
}

export type RuleValidationWalkForwardFold = {
  fold_index: number
  status: string
  train_start_date: string
  train_end_date: string
  test_start_date: string
  test_end_date: string
  test_day_count: number
  test_sample_count: number
  ic_mean?: number | null
  ic_t_value?: number | null
  avg_residual_return?: number | null
  spread_mean?: number | null
}

export type RuleValidationWalkForwardData = {
  fold_count: number
  purge_days: number
  label_boundaries_checked?: boolean
  label_boundary_excluded_days?: number
  folds: RuleValidationWalkForwardFold[]
  ic_positive_folds: number
  residual_positive_folds: number
  spread_positive_folds: number
}

export type RuleValidationIncrementalFold = {
  fold_index: number
  status: string
  train_start_date: string
  train_end_date: string
  test_start_date: string
  test_end_date: string
  train_day_count: number
  test_day_count: number
  incremental_mean?: number | null
  incremental_hac_t?: number | null
  positive_day_ratio?: number | null
}

export type RuleValidationIncrementalData = {
  core_rule_names: string[]
  purge_days: number
  label_boundaries_checked?: boolean
  label_boundary_excluded_days?: number
  folds: RuleValidationIncrementalFold[]
  positive_folds: number
}

export type RuleValidationDailyMetric = {
  trade_date: string
  ic?: number | null
  avg_residual_return?: number | null
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
  daily_metrics?: RuleValidationDailyMetric[] | null
  walk_forward?: RuleValidationWalkForwardData | null
  incremental?: RuleValidationIncrementalData | null
  similarity_rows: RuleValidationSimilarityRow[]
}

export type RuleExpressionValidationData = {
  horizon?: number
  label_algorithm?: string
  archive_run_id?: string | null
  membership_saved?: boolean
  input_identity?: string
  input_manifest?: unknown
  baseline_origin?: string
  additional_periods?: RuleExpressionValidationData[]
  import_rule_name: string
  import_rule_explain: string
  scope_way: string
  scope_windows: number
  sample_limit_per_group: number
  random_sample_method?: string
  walk_forward_folds: number
  core_rule_names: string[]
  combo_results: RuleValidationComboResult[]
}

export type ValidationCoreRuleOption = {
  native_recompute?: boolean
  name: string
  trigger_count: number
  valid_trigger_count: number
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

export type MarketEnvironmentGroup = 'direction' | 'volume' | 'risk'

export type MarketEnvironmentPoint = {
  trade_date: string
  value: number | null
}

export type MarketEnvironmentDimension = {
  group: MarketEnvironmentGroup
  key: string
  label: string
  unit: string
  value: number | null
  change_5d: number | null
  interpretation: string
  sample_count: number
  sample_label: string
  history: MarketEnvironmentPoint[]
}

export type MarketEnvironmentCoverage = {
  expected_stocks: number
  quoted_stocks: number
  suspended_or_gapped_stocks: number
  missing_stocks: number
  unknown_listing_stocks: number
  ratio: number | null
  required_ratio: number
}

export type MarketEnvironmentFeedback = {
  key: string
  label: string
  rate_pct: number | null
  change_5d_pp: number | null
  events: number
  successes: number
  stocks: number
  description: string
}

export type MarketReversalEstimate = {
  status: 'estimated' | 'not_applicable' | 'unavailable'
  method: 'evidence' | 'price' | 'historical_rate' | null
  direction: 'up' | 'down' | null
  probability: number | null
  horizon_sessions: number
  reason: string
  version: string
  training_days: number
  training_successes: number
  training_last_date: string | null
  training_labels_through: string | null
  review_after: string | null
  evaluation_days: number
  evaluation_successes: number
  evidence: { label: string; contribution: number }[]
}

export type MarketEnvironmentData = {
  reference_trade_date: string | null
  analysis_trade_date: string | null
  latest_trade_date: string | null
  fallback_reason: string | null
  coverage: MarketEnvironmentCoverage | null
  return_5d_pct: number | null
  return_10d_pct: number | null
  breadth_10d_pct: number | null
  feedback: MarketEnvironmentFeedback[]
  reversal: MarketReversalEstimate | null
  phase: string | null
  headline: string
  summary: string
  status: 'ok' | 'insufficient'
  dimensions: MarketEnvironmentDimension[]
}

export async function getMarketEnvironment(query: {
  sourcePath: string
  referenceTradeDate?: string
}) {
  return invoke<MarketEnvironmentData>('get_market_environment', query)
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
  returnMetric?: BacktestReturnMetric
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
  returnMetric?: BacktestReturnMetric
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
  returnMetric?: BacktestReturnMetric
  layerCount?: number
  layerMethod?: RankLayerMethod
  board?: string
  excludeStBoard?: boolean
  marketValueBoundaries?: number[]
}

export type RuleExpressionValidationQuery = {
  additionalPeriods?: number[]
  researchGrouping?: ResearchGrouping
  researchStatistics?: ResearchStatisticsSettings
  researchQuestion?: string
  researchConfiguration?: unknown
  parentRunId?: string | null
  verifyInputs?: boolean
  taskId?: string
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
  returnMetric?: BacktestReturnMetric
  unknownConfigs?: RuleValidationUnknownConfig[]
  sampleLimitPerGroup?: number
  board?: string
  excludeStBoard?: boolean
  totalMvMin?: number
  totalMvMax?: number
  walkForwardFolds?: number
  coreRuleNames?: string[]
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

export async function getCachedRuleLayerBacktestDetail(query: {
  sourcePath: string
  ruleName: string
}) {
  return invoke<RuleValidationComboResult>('get_cached_rule_layer_backtest_detail', query)
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

export async function cancelRuleExpressionValidation(taskId: string) {
  return invoke<void>('cancel_rule_expression_validation', { taskId })
}

export async function getValidationCoreRuleOptions(sourcePath: string) {
  return invoke<ValidationCoreRuleOption[]>('get_validation_core_rule_options', { sourcePath })
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

export type ResearchArchiveHeader = {
  version: number
  run_id: string
  parent_run_id?: string | null
  created_ms: number
  question: string
  kind: string
  status?: string
  membership_saved?: boolean
  input_identity: string
}

export async function listResearchArchives(sourcePath: string) {
  return invoke<ResearchArchiveHeader[]>('list_research_archives', { sourcePath })
}

export async function saveResearchArchive(sourcePath: string, parentRunId: string | null, question: string, configuration: unknown, result: unknown, kind = "expression_validation", membershipSourceRunId: string | null = null, status = "completed") {
  return invoke<ResearchArchiveHeader>('save_research_archive', { sourcePath, parentRunId, question, configuration, result, kind, membershipSourceRunId, status })
}

export async function readResearchArchive<T = RuleExpressionValidationData>(sourcePath: string, runId: string) {
  return invoke<{ header: ResearchArchiveHeader; configuration: unknown; result: T }>('read_research_archive', { sourcePath, runId })
}

export async function deleteResearchArchive(sourcePath: string, runId: string) {
  return invoke<void>('delete_research_archive', { sourcePath, runId })
}

export type RankComparisonRequest = {
  grouping?: ResearchGrouping
  statistics?: ResearchStatisticsSettings
  source_path: string
  candidate_config: string
  stock_adj_type: string
  index_ts_code: string
  index_beta: number
  concept_beta: number
  industry_beta: number
  start_date: string
  end_date: string
  min_listed_trade_days: number
  backtest_period: number
  verify_inputs?: boolean
  task_id?: string
  research_question?: string
  research_notes?: unknown
  parent_run_id?: string | null
}

export type RankComparisonSummary = {
  dimension?: string
  bucket?: string
  statistics?: ResearchSeriesStatistics | null
  period: string
  pool_kind: string
  top_k: number
  common_ranking_days: number
  calendar_days: number | null
  quarter_status: string
  paired_return_days: number
  baseline_members: number
  baseline_scored_members: number
  baseline_labels: number
  candidate_members: number
  candidate_scored_members: number
  candidate_labels: number
  unknown_eligibility: number
  added: number
  removed: number
  added_labels: number
  removed_labels: number
  added_return_mean: number | null
  removed_return_mean: number | null
  baseline_paired_daily_mean: number | null
  candidate_paired_daily_mean: number | null
  paired_daily_difference: number | null
  common_score_label_members: number
  paired_ic_days: number
  baseline_ic_mean: number | null
  candidate_ic_mean: number | null
  ic_difference: number | null
  baseline_icir: number | null
  candidate_icir: number | null
  icir_difference?: number | null
  paired_spread_days: number
  baseline_spread_mean: number | null
  candidate_spread_mean: number | null
  spread_difference: number | null
  net_contribution_days: number
  net_contribution_mean: number | null
}

export type RankComparisonResult = {
  baseline_config: string
  candidate_config: string
  baseline_rule_names: string[]
  candidate_rule_names: string[]
  baseline_warnings: string[]
  candidate_warnings: string[]
  scoring_start_date: string
  baseline_ranking_days: number
  candidate_ranking_days: number
  input_identity: string
  label_algorithm: string
  ranking_algorithm: string
  input_digests: Array<{ file: string; bytes: number | null; sha256: string | null }>
  archive_run_id?: string | null
  archive_error?: string | null
  membership_saved?: boolean
  summaries: RankComparisonSummary[]
}

export async function readRankComparisonBaseline(sourcePath: string) {
  return invoke<string>('read_rank_comparison_baseline', { sourcePath })
}

export async function cancelRankComparison(taskId: string) {
  return invoke<void>('cancel_rank_comparison', { taskId })
}

export async function runRankComparison(request: RankComparisonRequest) {
  return invoke<RankComparisonResult>('run_rank_comparison', { request })
}

export type RankComparisonMember = {
  trade_date: string
  ts_code: string
  membership: "added" | "removed" | "shared"
  baseline_score: number | null
  baseline_rank: number | null
  candidate_score: number | null
  candidate_rank: number | null
  residual_return: number | null
  entry_date?: string | null
  label_end_date?: string | null
  missing_reason: string | null
}

export async function listRankComparisonMembers(query: {
  dimension?: string
  bucket?: string
  sourcePath: string
  runId: string
  startDate: string
  endDate: string
  poolKind: string
  topK: number
  membership: string
  offset: number
  limit: number
}) {
  return invoke<{ total: number; offset: number; items: RankComparisonMember[] }>('list_rank_comparison_members', query)
}

export type ResearchGrouping = { baseline_score_boundaries?: number[]; market_cap_boundaries: number[]; market_metric: string; market_boundaries: number[] }
export type ResearchEvidenceQuery = {run_id: string; combo_key: string; horizon: number; start_date: string; end_date: string; dimension: string; bucket: string | null; state: string; offset: number; limit: number}
export type ResearchEvidenceSample = {baseline_score: number | null; baseline_rank: number | null; ts_code: string; trade_date: string; board: string; market_cap: number | null; bucket: string; score: number | null; residual_return: number | null; entry_date: string | null; label_end_date: string | null; missing_reason: string | null}
export type ResearchEvidenceSummary = {market_status: string; statistics: ResearchSeriesStatistics;period: string; bucket: string; observations: number; triggered: number; labeled: number; different_stocks: number; unknown_eligibility: number; ineligible: number; unavailable: number; missing_history: number; missing_label: number; immature_label: number; insufficient_quote_label: number; missing_factor_label: number; matched_triggered: number; matched_days: number; raw_mean: number | null; stock_day_mean: number | null; control_mean: number | null; matched_difference: number | null; label_end_date: string | null}
export async function listResearchEvidence(sourcePath: string, query: ResearchEvidenceQuery) {
  return invoke<{total: number; items: ResearchEvidenceSample[]}>('list_research_evidence', {sourcePath, query})
}
export async function summarizeResearchEvidence(sourcePath: string, query: ResearchEvidenceQuery, frequency: string) {
  return invoke<ResearchEvidenceSummary[]>('summarize_research_evidence', {sourcePath, query, frequency})
}
export async function exportResearchArchive(sourcePath: string, runId: string, destinationFile?: string) {
  return invoke<string>('export_research_archive', {sourcePath, runId, destinationFile})
}

export type ResearchStatisticsSettings = {block_length: number; resamples: number; seed: number}
export type ResearchSeriesStatistics = {valid_days: number; calendar_days: number; mean: number | null; hac_t: number | null; normal_approx_p: number | null; family_adjusted_p: number | null; lower: number | null; upper: number | null; bootstrap_valid: number; status: string}

export async function importResearchArchive(sourcePath: string, packagePath: string) {
  return invoke<ResearchArchiveHeader>('import_research_archive', {sourcePath, packagePath})
}

export function researchRunStatusLabel(status?: string) {
  return (({draft: "草稿", running: "运行中", completed: "完成", failed: "失败", cancelled: "已取消", interrupted: "异常中断"} as Record<string,string>)[status ?? ""] ?? status) || "旧存档（未记录状态）"
}
