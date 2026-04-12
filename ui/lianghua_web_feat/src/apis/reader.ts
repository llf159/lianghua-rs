import { invoke } from "@tauri-apps/api/core";

export type OverviewRow = {
  ts_code: string;
  trade_date?: string;
  ref_date?: string;
  resolved_rank_date?: string;
  resolved_ref_date?: string;
  total_score?: number;
  tiebreak_j?: number;
  rank?: number | null;
  ref_rank?: number | null;
  post_rank_return_pct?: number | null;
  name?: string;
  board?: string;
  board_category?: string;
  total_mv_yi?: number;
  concept?: string;
  [key: string]: string | number | null | undefined;
};

export type OverviewQuery = {
  sourcePath: string;
  tradeDate?: string;
  limit?: number;
  board?: string;
  totalMvMin?: number;
  totalMvMax?: number;
};

export type OverviewPageQuery = {
  sourcePath: string;
  rankDate?: string;
  refDate?: string;
  limit?: number;
  board?: string;
  totalMvMin?: number;
  totalMvMax?: number;
};

export type OverviewPageData = {
  rows: OverviewRow[];
  rank_date_options?: string[];
  resolved_rank_date?: string;
  resolved_ref_date?: string;
};

export type SceneOverviewRow = {
  ts_code: string;
  trade_date?: string;
  scene_name: string;
  scene_score?: number | null;
  risk_score?: number | null;
  scene_status?: string | null;
  rank?: number | null;
  name?: string;
  board?: string;
  total_mv_yi?: number | null;
  concept?: string;
  [key: string]: string | number | null | undefined;
};

export type SceneOverviewPageQuery = {
  sourcePath: string;
  rankDate?: string;
  limit?: number;
  board?: string;
  totalMvMin?: number;
  totalMvMax?: number;
};

export type SceneOverviewPageData = {
  rows: SceneOverviewRow[];
  rank_date_options?: string[];
  resolved_rank_date?: string;
};

export type StockLookupRow = {
  ts_code: string;
  name: string;
  cnspell?: string | null;
};

export async function rankOverview(query: OverviewQuery) {
  return invoke<OverviewRow[]>("get_rank_overview", query);
}

export async function rankOverviewPage(query: OverviewPageQuery) {
  return invoke<OverviewPageData>("get_rank_overview_page", query);
}

export async function listRankTradeDates(sourcePath: string) {
  return invoke<string[]>("get_rank_trade_date_options", { sourcePath });
}

export async function listSceneRankTradeDates(sourcePath: string) {
  return invoke<string[]>('get_scene_rank_trade_date_options', { sourcePath })
}

export async function sceneRankOverviewPage(query: SceneOverviewPageQuery) {
  return invoke<SceneOverviewPageData>('get_scene_rank_overview_page', query)
}

export async function listStockLookupRows(sourcePath: string) {
  return invoke<StockLookupRow[]>("list_stock_lookup_rows", { sourcePath });
}

export function isMissingOverviewExtension(error: unknown) {
  const message = String(error).toLowerCase();
  return (
    (message.includes("command") && message.includes("not found")) ||
    message.includes("unknown command") ||
    message.includes("get_rank_overview_page") ||
    message.includes("get_rank_trade_date_options")
  );
}
