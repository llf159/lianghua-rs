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
  excludeStBoard?: boolean;
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
  direction?: string | null;
  scene_score?: number | null;
  risk_score?: number | null;
  confirm_strength?: number | null;
  risk_intensity?: number | null;
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
  excludeStBoard?: boolean;
  totalMvMin?: number;
  totalMvMax?: number;
};

export type SceneOverviewPageData = {
  rows: SceneOverviewRow[];
  rank_date_options?: string[];
  resolved_rank_date?: string;
};

export type IntradayMonitorRow = {
  rank_mode: string;
  ts_code: string;
  trade_date?: string;
  scene_name: string;
  direction?: string | null;
  total_score?: number | null;
  scene_score?: number | null;
  risk_score?: number | null;
  confirm_strength?: number | null;
  risk_intensity?: number | null;
  scene_status?: string | null;
  rank?: number | null;
  name?: string;
  board?: string;
  total_mv_yi?: number | null;
  concept?: string;
  realtime_price?: number | null;
  realtime_open?: number | null;
  realtime_high?: number | null;
  realtime_low?: number | null;
  realtime_pre_close?: number | null;
  realtime_vol?: number | null;
  realtime_amount?: number | null;
  realtime_change_pct?: number | null;
  realtime_change_open_pct?: number | null;
  realtime_fall_from_high_pct?: number | null;
  realtime_vol_ratio?: number | null;
  template_tag_text?: string | null;
  template_tag_tone?: string | null;
  [key: string]: string | number | null | undefined;
};

export type IntradayMonitorTemplate = {
  id: string;
  name: string;
  expression: string;
};

export type IntradayMonitorRankModeConfig = {
  mode: "total" | "scene";
  sceneName: string;
  templateId: string;
};

export type IntradayMonitorPageQuery = {
  sourcePath: string;
  rankMode?: "total" | "scene";
  rankDate?: string;
  sceneName?: string;
  limit?: number;
  board?: string;
  excludeStBoard?: boolean;
  totalMvMin?: number;
  totalMvMax?: number;
};

export type IntradayMonitorPageData = {
  rows: IntradayMonitorRow[];
  rank_date_options?: string[];
  resolved_rank_date?: string;
  scene_options?: string[];
  refreshed_at?: string;
  warning_message?: string;
  warningMessage?: string;
};

export type IntradayRealtimeRefreshQuery = {
  sourcePath: string;
  rows: IntradayMonitorRow[];
  templates: IntradayMonitorTemplate[];
  rankModeConfigs: IntradayMonitorRankModeConfig[];
};

export type IntradayMonitorTemplateValidationData = {
  normalizedExpression: string;
  warmupNeed: number;
  message: string;
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
  return invoke<string[]>("get_scene_rank_trade_date_options", { sourcePath });
}

export async function sceneRankOverviewPage(query: SceneOverviewPageQuery) {
  return invoke<SceneOverviewPageData>("get_scene_rank_overview_page", query);
}

export async function intradayMonitorPage(query: IntradayMonitorPageQuery) {
  return invoke<IntradayMonitorPageData>("get_intraday_monitor_page", query);
}

export async function refreshIntradayMonitorRealtime(
  query: IntradayRealtimeRefreshQuery,
) {
  return invoke<IntradayMonitorPageData>(
    "refresh_intraday_monitor_realtime",
    query,
  );
}

export async function refreshIntradayMonitorTemplateTags(
  query: IntradayRealtimeRefreshQuery,
) {
  return invoke<IntradayMonitorPageData>(
    "refresh_intraday_monitor_template_tags",
    query,
  );
}

export async function validateIntradayMonitorTemplateExpression(
  sourcePath: string,
  expression: string,
) {
  return invoke<IntradayMonitorTemplateValidationData>(
    "validate_intraday_monitor_template_expression",
    { sourcePath, expression },
  );
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
