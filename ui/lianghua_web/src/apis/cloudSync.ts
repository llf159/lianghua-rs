import { invoke } from "@tauri-apps/api/core";
import {
  readJsonStorage,
  readStoredSourcePath,
  writeJsonStorage,
} from "../shared/storage";
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
  uploadedPaths: string[];
  downloadedPaths: string[];
  items: CloudItem[];
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
export function readWatchlistSyncFingerprint() {
  return JSON.stringify([
    readWatchObserveRowsFromCache()
      .map(({ tsCode, name, watchDate, tag, concept, markedDate }) => ({
        tsCode,
        name,
        watchDate,
        tag,
        concept,
        markedDate,
      }))
      .sort((left, right) => left.tsCode.localeCompare(right.tsCode)),
    localStorage.getItem("lh_intraday_monitor_watchlist_v1"),
    localStorage.getItem("am_board_filter"),
  ]);
}

let watchlistStartupTask: Promise<CloudSyncResult | null> | null = null;

export function checkWatchlistCloudAtStartup() {
  if (watchlistStartupTask) return watchlistStartupTask;
  watchlistStartupTask = (async () => {
    const config = await getCloudConfig();
    if (!config.endpoint.trim() || !navigator.onLine) return null;
    const items = await listCloudData();
    const remoteChanged = items.some(
      (item) =>
        item.isHead &&
        !item.isLocalBase &&
        (item.path === "bundles/watchlists.zip" ||
          item.path === "app/watch_observe.json" ||
          item.path === "settings/am_board_filter" ||
          item.path.startsWith("settings/lh_intraday_monitor_watchlist")),
    );
    const fingerprint = readWatchlistSyncFingerprint();
    const key = `lh_watchlist_auto_sync_v1:${readStoredSourcePath().trim()}`;
    const previous = readJsonStorage<{
      fingerprint: string;
      conflict: boolean;
    }>(localStorage, key);
    if (!remoteChanged && previous?.fingerprint === fingerprint) return null;
    const result = await syncCloud(undefined, false, "watchlists");
    writeJsonStorage(localStorage, key, {
      fingerprint:
        result.watchRows && result.conflicts.length === 0
          ? readWatchlistSyncFingerprint()
          : fingerprint,
      conflict: result.conflicts.length > 0,
    });
    return result;
  })().catch((error) => {
    console.error("启动时检查自选云端失败", error);
    return null;
  });
  return watchlistStartupTask;
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
  const beforeWatchRows =
    scope === "watchlists" || watchlistMode
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
          : {
              downloadOnly,
              scope,
              ...(scope === "watchlists"
                ? { watchRows: readWatchObserveRowsFromCache() }
                : {}),
            }),
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
  if (result.watchRows) {
    if (localStorage.getItem("lh_watch_observe_list") !== beforeWatchRows) {
      result.conflicts.push("bundles/watchlists.zip");
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
        "ranking/scoring_result.zip": "总榜排名与策略快照（同策略最多五日）",
        "similarity/strategy_similarity.zip": "相似榜数据库快照（最新一天）",
      } as Record<string, string>
    )[path] ?? path
  );
}

export function transferStrategyCloud(mode: WatchlistTransferMode) {
  return invoke<CloudSyncResult>("transfer_strategy_cloud", { mode });
}

export type CloudPreviewFile = { path: string; hash: string; text: string };
export type CloudPreviewSide = {
  id: string;
  createdAt: string | null;
  files: CloudPreviewFile[];
  dates: string[];
  overlapHash: string | null;
  replaceAll: boolean;
  receiveError: string | null;
};
export type CloudPreview = {
  path: string;
  localHash: string | null;
  heads: string[];
  baseAvailable: boolean;
  sides: CloudPreviewSide[];
  localDates: string[];
  localRevision: string | null;
};

export async function previewCloudData(path: string, versionId?: string) {
  const keys = await invoke<string[]>("get_cloud_sync_setting_keys");
  const settings: Record<string, string> = {};
  for (const key of keys) {
    const value = localStorage.getItem(key);
    if (value !== null) settings[key] = value;
  }
  return invoke<CloudPreview>("preview_cloud_sync_data", {
    path,
    versionId,
    settings,
  });
}

export function resolveCloudStrategies(
  preview: CloudPreview,
  choices: Record<string, string>,
) {
  return invoke<CloudSyncResult>("resolve_cloud_strategy_conflicts", {
    resolution: { localHash: preview.localHash, heads: preview.heads, choices },
  });
}

export function resolveCloudRanking(preview: CloudPreview, sideId: string) {
  const side = preview.sides.find((side) => side.id === sideId);
  if (!side) throw new Error("请选择榜单来源");
  return invoke<CloudSyncResult>(
    sideId === "local"
      ? "resolve_local_cloud_snapshot"
      : "restore_cloud_sync_version",
    {
      path: preview.path,
      versionId: sideId === "local" ? undefined : sideId,
      settings: {},
      decision: {
        localHash: preview.localHash,
        heads: preview.heads,
        dates: side.dates,
        overlapHash: side.overlapHash,
        replaceAll: side.replaceAll,
        localRevision: preview.localRevision,
      },
    },
  );
}
