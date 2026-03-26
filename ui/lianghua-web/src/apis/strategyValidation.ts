import { invoke } from "@tauri-apps/api/core";
import type { StrategyManageRuleDraft } from "./strategyManage";
import type {
  StrategyPerformanceAutoFilterConfig,
  StrategyPerformanceFutureSummary,
  StrategyPerformanceMethodNote,
  StrategyPerformanceRuleDetail,
  StrategyPerformanceRuleRow,
} from "./strategyPerformance";

export type StrategyPerformanceValidationDraftSummary = {
  name: string;
  explain: string;
  tag?: string | null;
  scope_way: string;
  scope_windows: number;
  points: number;
  has_dist_points: boolean;
  score_mode: string;
};

export type StrategyPerformanceValidationPageData = {
  horizons: number[];
  selected_horizon: number;
  strong_quantile: number;
  future_summaries: StrategyPerformanceFutureSummary[];
  auto_filter: StrategyPerformanceAutoFilterConfig;
  draft_summary: StrategyPerformanceValidationDraftSummary;
  rule_rows: StrategyPerformanceRuleRow[];
  rule_detail?: StrategyPerformanceRuleDetail | null;
  methods: StrategyPerformanceMethodNote[];
};

export async function getStrategyPerformanceValidationPage(query: {
  sourcePath: string;
  selectedHorizon?: number;
  strongQuantile?: number;
  draft: StrategyManageRuleDraft;
}) {
  return invoke<StrategyPerformanceValidationPageData>(
    "get_strategy_performance_validation_page",
    query,
  );
}
