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
}) {
  return invoke<StrategyDimensionResearchData>('run_strategy_dimension_research', query)
}
