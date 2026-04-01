import { invoke } from "@tauri-apps/api/core";

export type StrategyPerformanceMethodNote = {
  key: string;
  title: string;
  description: string;
};

export type StrategyPerformanceFutureSummary = {
  horizon: number;
  sample_count: number;
  avg_future_return_pct?: number | null;
  p80_return_pct?: number | null;
  p90_return_pct?: number | null;
  p95_return_pct?: number | null;
  strong_quantile: number;
  strong_threshold_pct?: number | null;
  strong_base_rate?: number | null;
  win_rate?: number | null;
  max_future_return_pct?: number | null;
};

export type StrategyPerformanceHorizonMetric = {
  horizon: number;
  hit_n: number;
  avg_future_return_pct?: number | null;
  strong_hit_rate?: number | null;
  strong_lift?: number | null;
  win_rate?: number | null;
  avg_total_score?: number | null;
  avg_rank?: number | null;
  hit_vs_non_hit_delta_pct?: number | null;
  rank_ic_mean?: number | null;
  icir?: number | null;
  sharpe_ratio?: number | null;
  layer_return_spread_pct?: number | null;
  composite_score?: number | null;
  ic_passes_floor: boolean;
  low_confidence: boolean;
  passes_auto_filter: boolean;
  passes_negative_filter: boolean;
};

export type StrategyPerformanceRuleRow = {
  rule_name: string;
  explain?: string | null;
  tag?: string | null;
  scope_way?: string | null;
  scope_windows?: number | null;
  points?: number | null;
  has_dist_points: boolean;
  signal_direction: string;
  direction_label: string;
  auto_candidate: boolean;
  manually_selected: boolean;
  in_advantage_set: boolean;
  in_companion_set: boolean;
  negative_effective?: boolean | null;
  negative_effectiveness_label?: string | null;
  negative_review_notes: string[];
  base_composite_score?: number | null;
  combo_positive_score?: number | null;
  combo_negative_score?: number | null;
  confidence_adjustment?: number | null;
  final_strength_score?: number | null;
  overall_composite_score?: number | null;
  avg_rank_ic_mean?: number | null;
  metrics: StrategyPerformanceHorizonMetric[];
};

export type StrategyPerformanceCompanionRow = {
  rule_name: string;
  hit_n: number;
  avg_future_return_pct?: number | null;
  eligible_pool_avg_return_pct?: number | null;
  delta_return_pct?: number | null;
  win_rate?: number | null;
  eligible_pool_win_rate?: number | null;
  delta_win_rate?: number | null;
  low_confidence: boolean;
};

export type StrategyPerformancePortfolioWindow = {
  window_key: string;
  label: string;
  sample_days: number;
  sample_count: number;
  avg_portfolio_return_pct?: number | null;
  avg_market_return_pct?: number | null;
  avg_excess_return_pct?: number | null;
  excess_win_rate?: number | null;
  strong_hit_rate?: number | null;
  strong_lift?: number | null;
  avg_selected_count?: number | null;
  rank_ic_mean?: number | null;
  icir?: number | null;
  layer_return_spread_pct?: number | null;
  composite_score?: number | null;
  sharpe_ratio?: number | null;
};

export type StrategyPerformancePortfolioRow = {
  strategy_key: string;
  strategy_label: string;
  sort_description: string;
  factor_count?: number | null;
  windows: StrategyPerformancePortfolioWindow[];
};

export type StrategyPerformanceOverallScoreAnalysis = {
  horizon: number;
  sample_count: number;
  avg_future_return_pct?: number | null;
  strong_hit_rate?: number | null;
  win_rate?: number | null;
  spearman_corr?: number | null;
  rank_ic_mean?: number | null;
  icir?: number | null;
  layer_return_spread_pct?: number | null;
  bucket_mode: string;
  score_rows: StrategyPerformanceScoreBucketRow[];
};

export type StrategyPerformanceScoreBucketRow = {
  bucket_label: string;
  score_min?: number | null;
  score_max?: number | null;
  sample_count: number;
  avg_future_return_pct?: number | null;
  strong_hit_rate?: number | null;
  win_rate?: number | null;
};

export type StrategyPerformanceHitCountRow = {
  hit_count: number;
  sample_count: number;
  avg_future_return_pct?: number | null;
  strong_hit_rate?: number | null;
  win_rate?: number | null;
};

export type StrategyPerformanceRuleDirectionDetail = {
  signal_direction: string;
  direction_label: string;
  bucket_mode: string;
  sample_count: number;
  avg_future_return_pct?: number | null;
  strong_hit_rate?: number | null;
  win_rate?: number | null;
  spearman_corr?: number | null;
  abs_spearman_corr?: number | null;
  rank_ic_mean?: number | null;
  icir?: number | null;
  sharpe_ratio?: number | null;
  hit_vs_non_hit_delta_pct?: number | null;
  extreme_score_minus_mild_score_pct?: number | null;
  has_dist_points: boolean;
  score_rows: StrategyPerformanceScoreBucketRow[];
  hit_count_rows: StrategyPerformanceHitCountRow[];
};

export type StrategyPerformanceRuleDetail = {
  rule_name: string;
  horizon: number;
  explain?: string | null;
  tag?: string | null;
  scope_way?: string | null;
  scope_windows?: number | null;
  points?: number | null;
  has_dist_points: boolean;
  directions: StrategyPerformanceRuleDirectionDetail[];
};

export type StrategyPerformanceAutoFilterConfig = {
  min_samples_2: number;
  min_samples_3: number;
  min_samples_5: number;
  min_samples_10: number;
  require_win_rate_above_market: boolean;
  min_pass_horizons: number;
};

export type StrategyPerformancePageData = {
  horizons: number[];
  selected_horizon: number;
  strong_quantile: number;
  strategy_options: string[];
  future_summaries: StrategyPerformanceFutureSummary[];
  auto_filter: StrategyPerformanceAutoFilterConfig;
  resolved_advantage_mode: string;
  auto_candidate_rule_names: string[];
  manual_rule_names: string[];
  ignored_manual_rule_names: string[];
  resolved_advantage_rule_names: string[];
  resolved_companion_rule_names: string[];
  effective_negative_rule_names: string[];
  ineffective_negative_rule_names: string[];
  min_adv_hits: number;
  top_limit: number;
  max_combination_size: number;
  noisy_companion_rule_names: string[];
  rule_rows: StrategyPerformanceRuleRow[];
  companion_rows: StrategyPerformanceCompanionRow[];
  portfolio_rows: StrategyPerformancePortfolioRow[];
  overall_score_analysis?: StrategyPerformanceOverallScoreAnalysis | null;
  selected_rule_name?: string | null;
  rule_detail?: StrategyPerformanceRuleDetail | null;
  methods: StrategyPerformanceMethodNote[];
};

export type StrategyPerformanceHorizonViewData = {
  selected_horizon: number;
  noisy_companion_rule_names: string[];
  companion_rows: StrategyPerformanceCompanionRow[];
  portfolio_rows: StrategyPerformancePortfolioRow[];
  overall_score_analysis?: StrategyPerformanceOverallScoreAnalysis | null;
};

export type StrategyPerformancePickCacheCombination = {
  strategy_key: string;
  strategy_label: string;
  factor_count: number;
  rule_names: string[];
  rank_ic_mean: number;
  composite_score?: number | null;
};

export type StrategyPerformancePickCachePayload = {
  selected_horizon: number;
  strong_quantile: number;
  resolved_advantage_rule_names: string[];
  resolved_noisy_companion_rule_names: string[];
  resolved_advantage_combinations: StrategyPerformancePickCacheCombination[];
  resolved_noisy_combinations: StrategyPerformancePickCacheCombination[];
};

export type StrategyPerformanceQuery = {
  sourcePath: string;
  selectedHorizon?: number;
  strongQuantile?: number;
  advantageRuleMode?: string;
  manualRuleNames?: string[];
  autoMinSamples2?: number;
  autoMinSamples3?: number;
  autoMinSamples5?: number;
  autoMinSamples10?: number;
  requireWinRateAboveMarket?: boolean;
  minPassHorizons?: number;
  minAdvHits?: number;
  topLimit?: number;
  maxCombinationSize?: number;
  noisyCompanionRuleNames?: string[];
  selectedRuleName?: string;
};

export async function getStrategyPerformancePage(
  query: StrategyPerformanceQuery,
) {
  return invoke<StrategyPerformancePageData>(
    "get_strategy_performance_page",
    query,
  );
}

export async function getStrategyPickCache(query: {
  sourcePath: string;
  selectedHorizon?: number;
  strongQuantile?: number;
  advantageRuleMode?: string;
  manualRuleNames?: string[];
  autoMinSamples2?: number;
  autoMinSamples3?: number;
  autoMinSamples5?: number;
  autoMinSamples10?: number;
  requireWinRateAboveMarket?: boolean;
  minPassHorizons?: number;
  minAdvHits?: number;
  maxCombinationSize?: number;
}) {
  return invoke<StrategyPerformancePickCachePayload>("get_strategy_pick_cache", query);
}

export async function getLatestStrategyPickCache(sourcePath: string) {
  return invoke<StrategyPerformancePickCachePayload>("get_latest_strategy_pick_cache", {
    sourcePath,
  });
}

export async function getStrategyPerformanceHorizonView(query: {
  sourcePath: string;
  selectedHorizon?: number;
  strongQuantile?: number;
  resolvedAdvantageRuleNames: string[];
  autoMinSamples2?: number;
  autoMinSamples3?: number;
  autoMinSamples5?: number;
  autoMinSamples10?: number;
  requireWinRateAboveMarket?: boolean;
  minPassHorizons?: number;
  minAdvHits?: number;
  topLimit?: number;
  maxCombinationSize?: number;
  noisyCompanionRuleNames?: string[];
}) {
  return invoke<StrategyPerformanceHorizonViewData>(
    "get_strategy_performance_horizon_view",
    query,
  );
}

export async function getStrategyPerformanceRuleDetail(query: {
  sourcePath: string;
  selectedHorizon?: number;
  strongQuantile?: number;
  selectedRuleName: string;
}) {
  return invoke<StrategyPerformanceRuleDetail | null>(
    "get_strategy_performance_rule_detail",
    query,
  );
}
