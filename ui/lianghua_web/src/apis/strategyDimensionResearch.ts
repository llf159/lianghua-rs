import { invoke } from '@tauri-apps/api/core'

export type StrategyDimensionRuleOption = {
  rule_name: string
  trigger_count: number
}

export type StrategyDimensionResearchDefaultsData = {
  start_date?: string | null
  end_date?: string | null
  rule_options: StrategyDimensionRuleOption[]
  max_strategy_count: number
  default_nonlinear_sample_limit: number
  max_nonlinear_sample_limit: number
  default_ridge_lambda: number
  default_holding_period: number
  max_holding_period: number
}

export type StrategyDimensionRuleSummary = {
  rule_name: string
  trigger_count: number
  coverage?: number | null
  score_mean_with_zeros?: number | null
  score_std_with_zeros?: number | null
}

export type StrategyDimensionPairMetrics = {
  left_rule_name: string
  right_rule_name: string
  joint_trigger_count: number
  union_trigger_count: number
  jaccard?: number | null
  phi?: number | null
  score_pearson_with_zeros?: number | null
  score_spearman_daily_mean?: number | null
  distance_correlation_daily_mean?: number | null
  nonlinear_sample_count: number
  return_pearson?: number | null
  return_shared_day_count: number
}

export type StrategyDimensionBasisCoefficient = {
  rule_name: string
  coefficient: number
}

export type StrategyDimensionOrthogonalDiagnostic = {
  rule_name: string
  basis_coefficients: StrategyDimensionBasisCoefficient[]
  explained_variance_ratio?: number | null
  residual_variance_ratio?: number | null
}

export type StrategyDimensionReturnSummary = {
  rule_name: string
  valid_day_count: number
  train_day_count: number
  test_day_count: number
  avg_residual_return?: number | null
  hac_t_value?: number | null
  train_avg_residual_return?: number | null
  test_avg_residual_return?: number | null
}

export type StrategyDimensionReturnIncrement = {
  rule_name: string
  basis_coefficients: StrategyDimensionBasisCoefficient[]
  train_sample_count: number
  test_sample_count: number
  test_incremental_mean?: number | null
  test_incremental_hac_t_value?: number | null
  test_incremental_positive_ratio?: number | null
}

export type StrategyDimensionResearchData = {
  start_date: string
  end_date: string
  universe_sample_count: number
  nonlinear_sample_limit: number
  ridge_lambda: number
  score_missing_value: number
  nonlinear_sample_scope: 'daily_cross_section_score_mean' | string
  orthogonal_order_sensitive: boolean
  strategies: StrategyDimensionRuleSummary[]
  pair_metrics: StrategyDimensionPairMetrics[]
  orthogonal_diagnostics: StrategyDimensionOrthogonalDiagnostic[]
  holding_period: number
  return_min_samples_per_day: number
  return_min_listed_trade_days: number
  return_stock_adj_type: string
  return_index_ts_code: string
  return_index_beta: number
  return_concept_beta: number
  return_industry_beta: number
  oos_train_ratio: number
  oos_test_start_date?: string | null
  return_summaries: StrategyDimensionReturnSummary[]
  return_increments: StrategyDimensionReturnIncrement[]
  pending_layers: string[]
}

export function getStrategyDimensionResearchDefaults(sourcePath: string) {
  return invoke<StrategyDimensionResearchDefaultsData>(
    'get_strategy_dimension_research_defaults',
    { sourcePath },
  )
}

export function runStrategyDimensionResearch(query: {
  sourcePath: string
  startDate: string
  endDate: string
  ruleNames: string[]
  nonlinearSampleLimit?: number
  ridgeLambda?: number
  holdingPeriod?: number
}) {
  return invoke<StrategyDimensionResearchData>(
    'run_strategy_dimension_research',
    query,
  )
}
