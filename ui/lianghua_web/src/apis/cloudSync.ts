import { invoke } from "@tauri-apps/api/core";
import {
  readWatchObserveRowsFromCache,
  writeWatchObserveRowsToCache,
  type WatchObserveRow,
} from "../shared/watchObserve";

export type CloudConfig = {
  endpoint: string;
  folder: string;
  username: string;
  password: string;
  hasPassword: boolean;
};
export type CloudItem = {
  path: string;
  version: {
    id: string;
    hash: string;
    parents: string[];
    createdAt: string;
    size: number;
    compressedSize: number;
  };
  isHead: boolean;
  conflict: boolean;
  isLocalBase: boolean;
};
export type WatchlistTransferMode = "upload" | "download" | "merge";

export type CloudSyncResult = {
  lastModified?: string | null;
  watchRows?: Array<
    Pick<
      WatchObserveRow,
      "tsCode" | "name" | "watchDate" | "tag" | "concept" | "markedDate"
    >
  > | null;
  uploaded: number;
  downloaded: number;
  skipped: number;
  conflicts: string[];
  uploadedBytes: number;
  downloadedBytes: number;
  transferredBytes: number;
  settings: Record<string, string>;
  warnings: string[];
};
export function getCloudConfig() {
  return invoke<CloudConfig>("get_cloud_sync_config");
}
export function saveCloudConfig(config: CloudConfig) {
  return invoke<void>("save_cloud_sync_config", { config });
}
export function testCloudConnection() {
  return invoke<void>("test_cloud_sync_connection");
}
export function listCloudData() {
  return invoke<CloudItem[]>("list_cloud_sync_data");
}
export function deleteCloudVersion(item: CloudItem) {
  return invoke<void>("delete_cloud_sync_version", {
    path: item.path,
    versionId: item.version.id,
  });
}
export async function syncCloud(
  item?: CloudItem,
  downloadOnly = false,
  scope: "management" | "watchlists" = "management",
  watchlistMode?: WatchlistTransferMode,
) {
  const keys = await invoke<string[]>("get_cloud_sync_setting_keys");
  const settings: Record<string, string> = {};
  const before = new Map<string, string | null>();
  for (const key of keys) {
    const value = localStorage.getItem(key);
    before.set(key, value);
    if (value !== null) settings[key] = value;
  }
  const beforeWatchRows = watchlistMode
    ? localStorage.getItem("lh_watch_observe_list")
    : null;
  const result = await invoke<CloudSyncResult>(
    watchlistMode
      ? "transfer_watchlist_cloud"
      : item
        ? "restore_cloud_sync_version"
        : "run_cloud_sync",
    {
      settings,
      ...(watchlistMode
        ? { mode: watchlistMode, watchRows: readWatchObserveRowsFromCache() }
        : item
          ? { path: item.path, versionId: item.version.id }
          : { downloadOnly, scope }),
    },
  );
  // 不覆盖网络等待期间新增的本机编辑；下载版本仍保留在云端和已校验缓存。
  for (const [key, value] of Object.entries(result.settings)) {
    if (!before.has(key)) continue;
    if (localStorage.getItem(key) !== before.get(key)) {
      result.conflicts.push(`settings/${key}`);
      result.warnings.push(`设置在同步期间被修改，已保留本地内容：${key}`);
      continue;
    }
    try {
      const oldValue = localStorage.getItem(key);
      localStorage.setItem(key, value);
      window.dispatchEvent(
        new StorageEvent("storage", {
          key,
          oldValue,
          newValue: value,
          storageArea: localStorage,
        }),
      );
    } catch {
      result.warnings.push(`设置写入失败，请释放设备存储空间后重试：${key}`);
    }
  }
  if (watchlistMode && result.watchRows) {
    if (localStorage.getItem("lh_watch_observe_list") !== beforeWatchRows) {
      result.warnings.push(
        "同步期间自选已修改，已保留页面中的自选；可再次合并",
      );
    } else {
      try {
        writeWatchObserveRowsToCache(
          result.watchRows.map((row) => ({
            ...row,
            latestClose: null,
            latestChangePct: null,
            volumeRatio: null,
            return3dPct: null,
            postWatchReturnPct: null,
            todayRank: null,
            sceneMarker: null,
          })),
        );
      } catch (cacheError) {
        result.warnings.push(
          `同步完成，但页面自选写入失败：${String(cacheError)}`,
        );
      }
    }
  }
  return result;
}

export type UploadEstimate = {
  initialUploadBytes: number;
  uniqueObjects: number;
  uploadObjects: number;
  skippedDuplicates: number;
  configurationBytes: number;
  rankingDailyBytes: number;
  similarityDailyBytes: number;
  monthly22DaysBytes: number;
  monthly31DaysBytes: number;
  missingSimilarity: boolean;
};
export async function estimateCloudUpload() {
  const keys = await invoke<string[]>("get_cloud_sync_setting_keys");
  const settings: Record<string, string> = {};
  for (const key of keys) {
    const value = localStorage.getItem(key);
    if (value !== null) settings[key] = value;
  }
  return invoke<UploadEstimate>("estimate_cloud_sync_upload", { settings });
}

export function cloudPathLabel(path: string) {
  return (
    (
      {
        "bundles/strategies.zip": "策略与历史备份",
        "bundles/watchlists.zip": "自选与开关",
        "bundles/indicator_config.zip": "指标与详情图配置",
        "bundles/browser_settings.zip": "浏览习惯配置",
        "ranking/scoring_result.zip": "总榜数据库快照（近五个结果日期）",
        "similarity/strategy_similarity.zip": "相似榜数据库快照（最新一天）",
      } as Record<string, string>
    )[path] ?? path
  );
}

export function transferStrategyCloud(mode: WatchlistTransferMode) {
  return invoke<CloudSyncResult>("transfer_strategy_cloud", { mode });
}
