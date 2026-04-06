import { invoke } from "@tauri-apps/api/core";
import type {
  StrategyPerformanceFutureSummary,
  StrategyPerformanceMethodNote,
  StrategyPerformanceRuleRow,
} from "./strategyPerformance";

export type StrategyDirection = "positive" | "negative";

export type StrategyValidationUnknownConfig = {
  name: string;
  start: number;
  end: number;
  step: number;
};

export type StrategyPerformanceValidationDraft = {
  strategy_direction: StrategyDirection;
  scope_way: string;
  scope_windows: number;
  when: string;
  import_name?: string | null;
  unknown_configs: StrategyValidationUnknownConfig[];
};

export type StrategyPerformanceValidationUnknownValue = {
  name: string;
  value: number;
};

export type StrategyPerformanceValidationComboSummary = {
  combo_key: string;
  combo_label: string;
  import_name?: string | null;
  formula: string;
  unknown_values: StrategyPerformanceValidationUnknownValue[];
  score_mode: string;
  trigger_samples: number;
  triggered_days: number;
  avg_daily_trigger: number;
  positive_overall_composite_score?: number | null;
  positive_avg_future_return_pct?: number | null;
  positive_primary_metric?: number | null;
  positive_secondary_metric?: number | null;
  positive_hit_n: number;
  negative_effective: boolean;
  negative_avg_future_return_pct?: number | null;
  negative_primary_metric?: number | null;
  negative_secondary_metric?: number | null;
  negative_hit_n: number;
};

export type StrategyPerformanceValidationLayerRow = {
  label: string;
  layer_value: number;
  sample_count: number;
  avg_future_return_pct?: number | null;
  strong_hit_rate?: number | null;
  win_rate?: number | null;
};

export type StrategyPerformanceValidationSimilarityRow = {
  rule_name: string;
  explain?: string | null;
  overlap_samples: number;
  overlap_rate_vs_validation?: number | null;
  overlap_rate_vs_existing?: number | null;
  overlap_lift?: number | null;
};

export type StrategyPerformanceValidationCaseData = {
  combo_summary: StrategyPerformanceValidationComboSummary;
  positive_row?: StrategyPerformanceRuleRow | null;
  negative_row?: StrategyPerformanceRuleRow | null;
  layer_mode: string;
  layer_rows: StrategyPerformanceValidationLayerRow[];
  similarity_rows: StrategyPerformanceValidationSimilarityRow[];
};

export type StrategyPerformanceValidationPageData = {
  strategy_direction: StrategyDirection;
  horizons: number[];
  selected_horizon: number;
  strong_quantile: number;
  future_summaries: StrategyPerformanceFutureSummary[];
  combo_summaries: StrategyPerformanceValidationComboSummary[];
  best_positive_case?: StrategyPerformanceValidationCaseData | null;
  best_negative_case?: StrategyPerformanceValidationCaseData | null;
  methods: StrategyPerformanceMethodNote[];
};

export async function getStrategyPerformanceValidationPage(query: {
  sourcePath: string;
  selectedHorizon?: number;
  strongQuantile?: number;
  draft: StrategyPerformanceValidationDraft;
}) {
  return invoke<StrategyPerformanceValidationPageData>(
    "get_strategy_performance_validation_page",
    query,
  );
}
