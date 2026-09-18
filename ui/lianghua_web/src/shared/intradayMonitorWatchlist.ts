import { normalizeTsCode } from "./stockCode";
import { readJsonStorage, writeJsonStorage } from "./storage";

export const INTRADAY_MONITOR_WATCHLIST_STORAGE_KEY =
  "lh_intraday_monitor_watchlist_v1";
export const INTRADAY_MONITOR_WATCHLIST_ENABLED_STORAGE_KEY =
  "lh_intraday_monitor_watchlist_enabled_v1";

export function normalizeIntradayMonitorWatchlistCodes(codes: string[]) {
  const seen = new Set<string>();
  const normalized: string[] = [];

  for (const rawCode of codes) {
    const code = normalizeTsCode(rawCode);
    if (!code || seen.has(code)) continue;
    seen.add(code);
    normalized.push(code);
  }

  return normalized;
}

export function readStoredIntradayMonitorWatchlist() {
  const parsed = readJsonStorage<unknown>(
    typeof window === "undefined" ? null : window.localStorage,
    INTRADAY_MONITOR_WATCHLIST_STORAGE_KEY,
  );
  if (!Array.isArray(parsed)) return [];
  return normalizeIntradayMonitorWatchlistCodes(
    parsed.filter((item): item is string => typeof item === "string"),
  );
}

export function writeStoredIntradayMonitorWatchlist(codes: string[]) {
  const normalized = normalizeIntradayMonitorWatchlistCodes(codes);
  writeJsonStorage(
    typeof window === "undefined" ? null : window.localStorage,
    INTRADAY_MONITOR_WATCHLIST_STORAGE_KEY,
    normalized,
  );
  return normalized;
}

export function readStoredIntradayMonitorWatchlistEnabled() {
  try {
    return (
      typeof window !== "undefined" &&
      window.localStorage.getItem(
        INTRADAY_MONITOR_WATCHLIST_ENABLED_STORAGE_KEY,
      ) === "1"
    );
  } catch {
    return false;
  }
}

export function writeStoredIntradayMonitorWatchlistEnabled(enabled: boolean) {
  try {
    if (typeof window !== "undefined") {
      window.localStorage.setItem(
        INTRADAY_MONITOR_WATCHLIST_ENABLED_STORAGE_KEY,
        enabled ? "1" : "0",
      );
    }
  } catch {
    // localStorage unavailable
  }
}
