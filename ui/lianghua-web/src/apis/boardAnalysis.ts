import { invoke } from "@tauri-apps/api/core";

export type BoardAnalysisGroupRow = {
  name: string;
  sample_count: number;
  strength_score_pct?: number | null;
  avg_rank?: number | null;
  avg_return_pct?: number | null;
  up_ratio_pct?: number | null;
  top_rank?: number | null;
  leader_stock_name?: string | null;
  leader_stock_ts_code?: string | null;
  leader_stock_return_pct?: number | null;
};

export type BoardAnalysisSummary = {
  rank_sample_count: number;
  return_sample_count: number;
};

export type BoardAnalysisPageData = {
  resolved_ref_date?: string | null;
  resolved_backtest_start_date?: string | null;
  weighting_range_start: number;
  weighting_range_end: number;
  backtest_period_days: number;
  industry_strength_rows: BoardAnalysisGroupRow[];
  concept_strength_rows: BoardAnalysisGroupRow[];
  industry_return_rows: BoardAnalysisGroupRow[];
  concept_return_rows: BoardAnalysisGroupRow[];
  summary?: BoardAnalysisSummary | null;
};

export type BoardAnalysisStockRow = {
  ts_code: string;
  name?: string | null;
  market_board: string;
  industry?: string | null;
  concept?: string | null;
  rank?: number | null;
  total_score?: number | null;
  strength_weight?: number | null;
  return_pct?: number | null;
};

export type BoardAnalysisGroupDetail = {
  group_kind: string;
  metric_kind: string;
  group_name: string;
  resolved_ref_date?: string | null;
  resolved_backtest_start_date?: string | null;
  weighting_range_start: number;
  weighting_range_end: number;
  backtest_period_days: number;
  summary?: BoardAnalysisGroupRow | null;
  stocks: BoardAnalysisStockRow[];
};

export type BoardAnalysisQuery = {
  sourcePath: string;
  refDate?: string;
  weightingRangeStart?: number;
  weightingRangeEnd?: number;
  backtestPeriodDays?: number;
};

export type BoardAnalysisDetailQuery = BoardAnalysisQuery & {
  groupKind: "industry" | "concept";
  metricKind: "strength" | "return";
  groupName: string;
};

export async function getBoardAnalysisPage(query: BoardAnalysisQuery) {
  return invoke<BoardAnalysisPageData>("get_board_analysis_page", query);
}

export async function getBoardAnalysisGroupDetail(
  query: BoardAnalysisDetailQuery,
) {
  return invoke<BoardAnalysisGroupDetail>(
    "get_board_analysis_group_detail",
    query,
  );
}
