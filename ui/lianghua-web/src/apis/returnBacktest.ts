import { invoke } from "@tauri-apps/api/core";

export type ReturnBacktestBucket = {
  label: string;
  count: number;
};

export type ReturnBacktestRow = {
  ts_code: string;
  name?: string | null;
  board: string;
  rank?: number | null;
  best_rank?: number | null;
  total_score?: number | null;
  concept?: string | null;
  entry_trade_date?: string | null;
  entry_open?: number | null;
  exit_trade_date?: string | null;
  exit_close?: number | null;
  return_pct?: number | null;
  excess_return_pct?: number | null;
};

export type ReturnBacktestSummary = {
  selected_top_count: number;
  valid_top_count: number;
  benchmark_sample_count: number;
  benchmark_return_pct?: number | null;
  top_avg_return_pct?: number | null;
  top_avg_excess_return_pct?: number | null;
  top_strong_hit_rate?: number | null;
  top_weak_hit_rate?: number | null;
  benchmark_strong_hit_rate?: number | null;
  benchmark_weak_hit_rate?: number | null;
  strength_score?: number | null;
  strength_label?: string | null;
};

export type ReturnBacktestPageData = {
  resolved_rank_date?: string | null;
  resolved_ref_date?: string | null;
  board?: string | null;
  top_limit: number;
  benchmark_label?: string | null;
  rank_distribution?: ReturnBacktestBucket[] | null;
  benchmark_distribution?: ReturnBacktestBucket[] | null;
  rank_rows?: ReturnBacktestRow[] | null;
  benchmark_rows?: ReturnBacktestRow[] | null;
  summary?: ReturnBacktestSummary | null;
};

export type ReturnBacktestStrengthHeatmapItem = {
  rank_date: string;
  ref_date: string;
  strength_score?: number | null;
  strength_label?: string | null;
  top_avg_return_pct?: number | null;
  benchmark_return_pct?: number | null;
  top_strong_hit_rate?: number | null;
  top_weak_hit_rate?: number | null;
  benchmark_strong_hit_rate?: number | null;
  benchmark_weak_hit_rate?: number | null;
  valid_top_count: number;
  benchmark_sample_count: number;
};

export type ReturnBacktestStrengthOverviewData = {
  holding_days: number;
  top_limit: number;
  board?: string | null;
  latest_rank_date?: string | null;
  strong_days: number;
  weak_days: number;
  flat_days: number;
  items?: ReturnBacktestStrengthHeatmapItem[] | null;
};

export type ReturnBacktestQuery = {
  sourcePath: string;
  rankDate?: string;
  refDate?: string;
  topLimit?: number;
  board?: string;
};

export type ReturnBacktestStrengthOverviewQuery = {
  sourcePath: string;
  holdingDays?: number;
  topLimit?: number;
  board?: string;
};

export async function getReturnBacktestPage(query: ReturnBacktestQuery) {
  return invoke<ReturnBacktestPageData>("get_return_backtest_page", query);
}

export async function getReturnBacktestStrengthOverview(
  query: ReturnBacktestStrengthOverviewQuery,
) {
  return invoke<ReturnBacktestStrengthOverviewData>(
    "get_return_backtest_strength_overview",
    query,
  );
}
