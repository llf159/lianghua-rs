import { invoke } from "@tauri-apps/api/core";
import {
  isPermissionGranted,
  requestPermission,
} from "@tauri-apps/plugin-notification";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import {
  getAllMarketMonitorSnapshot,
  setAllMarketVolumeRatioPaused,
  openEastmoneyVerification,
  useEastmoneyVerifiedSession as connectEastmoneySession,
  stopEastmoneyVerifiedSession,
  type StockLookupRow,
  type AllMarketIndexRow,
  type AllMarketMonitorRow,
  type IntradayMonitorTemplate,
} from "../../apis/reader";
import IntradayTemplateManagerModal from "./components/IntradayTemplateManagerModal";
import WatchlistModal from "./components/WatchlistModal";
import IntradayMarketGroupTable from "./components/IntradayMarketGroupTable";
import {
  amount,
  percent,
  summarizeGroups,
  type MarketGroup,
} from "../../shared/intradayMarketGroups";
import DetailsLink from "../../shared/DetailsLink";
import GlobalStockSearch from "../../shared/GlobalStockSearch";
import {
  formatConceptText,
  filterConceptItems,
  splitConceptText,
  useConceptExclusions,
} from "../../shared/conceptExclusions";
import { readStoredRealtimeQuoteProvider } from "../../shared/realtimeSettings";
import {
  INTRADAY_MONITOR_BOARD_FILTER_STORAGE_KEY,
  readStoredIntradayMonitorWatchlist,
  writeStoredIntradayMonitorWatchlist,
} from "../../shared/intradayMonitorWatchlist";
import { STOCK_PICK_BOARD_OPTIONS } from "../../shared/stockPickShared";
import {
  TableSortButton,
  getAriaSort,
  type SortDefinition,
  type SortDirection,
  sortRows,
} from "../../shared/tableSort";
import { readJsonStorage, writeJsonStorage } from "../../shared/storage";
import "./css/AllMarketMonitorPage.css";
import "./css/DataImportPage.css";

const HISTORY_KEEP_MS = 90_000;
const DEFAULT_SPEED_PERIOD = 10;
const DEFAULT_SPEED_THRESHOLD = 2;
const DEFAULT_SPEED_MIN_TICKS = 3;
const PRICE_TICK_SIZE = 0.01;
const DEFAULT_VOLUME_RATIO_THRESHOLD = 10;
const DEFAULT_RANK_HIGHLIGHT_THRESHOLD = 300;
const DEFAULT_TOP_LIMIT = 50;
const SPEED_PERIOD_OPTIONS = [10, 30, 60] as const;
const SCENE_STAGE_THRESHOLD_OPTIONS = [
  { value: "observe", label: "观察" },
  { value: "trigger", label: "触发" },
  { value: "confirm", label: "确认" },
] as const;

const LS_KEY_SPEED_PERIOD = "am_speed_period";
const LS_KEY_SPEED_THRESHOLD = "am_speed_threshold";
const LS_KEY_SPEED_MIN_TICKS = "am_speed_min_ticks";
const LS_KEY_VOLUME_RATIO_THRESHOLD = "am_volume_ratio_threshold";
const LS_KEY_RANK_HIGHLIGHT_THRESHOLD = "am_rank_highlight_threshold";
const LS_KEY_SCENE_STAGE_THRESHOLD = "am_scene_stage_threshold";
const LS_KEY_TOP_LIMIT = "am_top_limit";
const LS_KEY_OTHER_SORT_EXPRESSION = "am_other_sort_expression";
const LS_KEY_OTHER_SORT_DIRECTION = "am_other_sort_direction";
const LS_KEY_COMPACT_MODE = "am_compact_mode";
const LS_KEY_SIDE_LIST_VISIBLE = "am_side_list_visibility";
const LS_KEY_TEMPLATE_DISPLAY_MODE = "am_template_display_mode";
const INTRADAY_MONITOR_TEMPLATE_STORAGE_KEY =
  "lh_intraday_monitor_realtime_templates_v1";

function readLocalStorageNumber<T extends number>(key: string, fallback: T): T {
  try {
    const raw = localStorage.getItem(key);
    if (raw != null && raw.trim() !== "") {
      const parsed = Number(raw);
      if (isFiniteNumber(parsed)) return parsed as T;
    }
  } catch {
    // localStorage unavailable
  }
  return fallback;
}

function buildLegacyTemplateExpression(
  direction: "up" | "down",
  thresholdPct: number,
  base: "preclose" | "open",
) {
  const threshold = Math.abs(thresholdPct);
  const field = base === "open" ? "RT_OP" : "PCT_CHG";
  return direction === "down"
    ? `${field} <= -${threshold}`
    : `${field} >= ${threshold}`;
}

function normalizeTemplate(input: unknown): IntradayMonitorTemplate | null {
  if (!input || typeof input !== "object") return null;
  const item = input as Record<string, unknown>;
  if (typeof item.id !== "string") return null;

  const directExpression =
    typeof item.expression === "string" ? item.expression.trim() : "";
  if (directExpression) {
    return {
      id: item.id,
      name: typeof item.name === "string" ? item.name : "",
      expression: directExpression,
      enabled: item.enabled !== false,
    };
  }

  const threshold = Number(item.thresholdPct);
  return {
    id: item.id,
    name: typeof item.name === "string" ? item.name : "",
    expression: buildLegacyTemplateExpression(
      item.direction === "down" ? "down" : "up",
      Number.isFinite(threshold) ? threshold : 0,
      item.base === "open" ? "open" : "preclose",
    ),
    enabled: item.enabled !== false,
  };
}

function readStoredTemplates() {
  const parsed = readJsonStorage<unknown>(
    typeof window === "undefined" ? null : window.localStorage,
    INTRADAY_MONITOR_TEMPLATE_STORAGE_KEY,
  );
  if (!Array.isArray(parsed)) return [];
  return parsed
    .map(normalizeTemplate)
    .filter((item): item is IntradayMonitorTemplate => item !== null);
}

type SceneStageThreshold =
  (typeof SCENE_STAGE_THRESHOLD_OPTIONS)[number]["value"];

function readLocalStorageSceneStageThreshold(): SceneStageThreshold {
  try {
    const raw = localStorage.getItem(LS_KEY_SCENE_STAGE_THRESHOLD);
    if (raw === "observe" || raw === "trigger" || raw === "confirm") {
      return raw;
    }
  } catch {
    // localStorage unavailable
  }
  return "trigger";
}

type PrimarySortKey =
  | "other_sort_value"
  | "realtime_change_pct"
  | "speed_pct"
  | "realtime_vol_ratio";
type TemplateDisplayMode = "off" | "mark" | "hits_only";
type HitPanelMode =
  | "all"
  | "realtime_change_pct"
  | "realtime_vol_ratio"
  | "speed_pct";
type SpeedPeriod = (typeof SPEED_PERIOD_OPTIONS)[number];
type BoardFilter = (typeof STOCK_PICK_BOARD_OPTIONS)[number] | "自选";
type SortKey =
  | "best_rank_3d"
  | "similarity_rank"
  | "other_sort_value"
  | "realtime_change_pct"
  | "return_5d_pct"
  | "speed_pct"
  | "realtime_vol_ratio"
  | "above_avg_price"
  | "realtime_change_open_pct"
  | "total_mv_yi";

type PriceSnapshot = {
  capturedAt: number;
  prices: Record<string, number>;
};

type MonitorRefreshConfig = {
  sceneStageThreshold: SceneStageThreshold;
  templateEnabled: boolean;
  templates: IntradayMonitorTemplate[];
  otherSortExpression: string;
};

type DisplayRow = AllMarketMonitorRow & {
  speed_pct?: number | null;
};

type SpeedHitRecord = DisplayRow & {
  hit_at: number;
  hit_speed_pct?: number | null;
  hit_speed_threshold_pct?: number | null;
};

type SpeedHitRecordsByPeriod = Record<SpeedPeriod, SpeedHitRecord[]>;

type MonitorEvent = {
  key: string;
  kind: Exclude<HitPanelMode, "all">;
  period?: SpeedPeriod;
  record: SpeedHitRecord;
  value: number;
  firstAt: number;
  latestAt: number;
  hits: number;
};

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function fingerprintMonitorRefreshConfig(
  config: MonitorRefreshConfig,
  realtimeProvider: "sina" | "tencent",
) {
  const enabledTemplates = config.templates.filter(
    (template) => template.enabled,
  );
  return JSON.stringify({
    realtimeProvider,
    sceneStageThreshold: config.sceneStageThreshold,
    templateEnabled: config.templateEnabled,
    templates: config.templateEnabled ? enabledTemplates : [],
    otherSortExpression: config.otherSortExpression.trim(),
  });
}

function parseNonNegativeIntegerInput(value: string, fallback: number) {
  const trimmed = value.trim();
  if (!trimmed) return fallback;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(0, Math.trunc(parsed));
}

function withDefaultText(value: string, fallback: number) {
  return value.trim() ? value : String(fallback);
}

function normalizeNonNegativeIntegerText(value: string, fallback: number) {
  return String(parseNonNegativeIntegerInput(value, fallback));
}

function readLocalStorageText(key: string, fallback = "") {
  try {
    return localStorage.getItem(key) ?? fallback;
  } catch {
    return fallback;
  }
}

function readLocalStorageSortDirection(
  key: string,
  fallback: Exclude<SortDirection, null>,
): Exclude<SortDirection, null> {
  try {
    const raw = localStorage.getItem(key);
    if (raw === "asc" || raw === "desc") return raw;
  } catch {
    // localStorage unavailable
  }
  return fallback;
}

function readLocalStorageBoolean(key: string, fallback: boolean) {
  try {
    const raw = localStorage.getItem(key);
    if (raw === "true") return true;
    if (raw === "false") return false;
  } catch {
    // localStorage unavailable
  }
  return fallback;
}

function readLocalStorageTemplateDisplayMode(): TemplateDisplayMode {
  try {
    const raw = localStorage.getItem(LS_KEY_TEMPLATE_DISPLAY_MODE);
    if (raw === "off" || raw === "mark" || raw === "hits_only") {
      return raw;
    }
  } catch {
    // localStorage unavailable
  }
  return "off";
}

function readLocalStorageSideListVisible() {
  const parsed = readJsonStorage<unknown>(
    typeof window === "undefined" ? null : window.localStorage,
    LS_KEY_SIDE_LIST_VISIBLE,
  );
  if (typeof parsed === "boolean") {
    return parsed;
  }
  if (parsed && typeof parsed === "object") {
    const item = parsed as Partial<Record<HitPanelMode, unknown>>;
    return (
      item.realtime_change_pct !== false ||
      item.realtime_vol_ratio !== false ||
      item.speed_pct !== false
    );
  }
  return true;
}

function formatNumber(value?: number | null, digits = 2) {
  if (!isFiniteNumber(value)) return "--";
  return Number.isInteger(value) ? String(value) : value.toFixed(digits);
}

function formatMarketValueYi(value?: number | null) {
  if (!isFiniteNumber(value)) return "--";
  const absValue = Math.abs(value);
  const digits = absValue >= 100 ? 0 : absValue >= 10 ? 1 : 2;
  return `${value.toFixed(digits)}亿`;
}

function formatPercent(value?: number | null) {
  if (!isFiniteNumber(value)) return "--";
  return `${value.toFixed(2)}%`;
}

function formatClock(value: Date) {
  return value.toLocaleTimeString("zh-CN", {
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function formatClockFromMs(value: number) {
  if (!Number.isFinite(value)) return "--";
  return formatClock(new Date(value));
}

function formatRefreshTime(raw: string) {
  const value = raw.trim();
  if (!value) return "--";
  const withSeconds = value.match(/(\d{2}:\d{2}:\d{2})/);
  if (withSeconds) return withSeconds[1];
  const withMinutes = value.match(/(\d{2}:\d{2})/);
  return withMinutes ? withMinutes[1] : value;
}

function getPercentClassName(value?: number | null) {
  if (!isFiniteNumber(value) || value === 0) {
    return "all-market-value-flat";
  }
  return value > 0 ? "all-market-value-up" : "all-market-value-down";
}

function getRealtimeChangeCellClassName(value?: number | null) {
  const classNames = [
    getPercentClassName(value),
    "all-market-realtime-group-start",
  ];
  if (!isFiniteNumber(value)) return classNames.join(" ");
  if (value > 7) {
    classNames.push("all-market-change-highlight-red");
  } else if (value >= 4) {
    classNames.push("all-market-change-highlight-yellow");
  } else if (value < -5) {
    classNames.push("all-market-change-highlight-green");
  } else if (value <= -2) {
    classNames.push("all-market-change-highlight-light-green");
  }
  return classNames.join(" ");
}

function isAboveAvgPrice(row: AllMarketMonitorRow) {
  return (
    isFiniteNumber(row.realtime_price) &&
    isFiniteNumber(row.realtime_avg_price) &&
    row.realtime_avg_price > 0 &&
    row.realtime_price > row.realtime_avg_price
  );
}

function formatAboveAvgPrice(row: AllMarketMonitorRow) {
  if (
    !isFiniteNumber(row.realtime_price) ||
    !isFiniteNumber(row.realtime_avg_price) ||
    row.realtime_avg_price <= 0
  ) {
    return "--";
  }
  return row.realtime_price > row.realtime_avg_price ? "是" : "否";
}

function getTemplateHits(row: AllMarketMonitorRow) {
  return Array.isArray(row.template_hits) ? row.template_hits : [];
}

function formatTemplateHitText(row: AllMarketMonitorRow) {
  const hits = getTemplateHits(row);
  if (hits.length === 0) return "--";
  if (hits.length === 1) return hits[0]?.template_name || "未命名模板";
  return `${hits.length}个模板`;
}

function isRankHighlight(
  rank: number | null | undefined,
  threshold: number | null,
) {
  if (!isFiniteNumber(threshold)) return false;
  return isFiniteNumber(rank) && rank <= threshold;
}

function buildPriceSnapshot(rows: AllMarketMonitorRow[], capturedAt: number) {
  const prices: Record<string, number> = {};
  for (const row of rows) {
    if (isFiniteNumber(row.realtime_price) && row.realtime_price > 0) {
      prices[row.ts_code] = row.realtime_price;
    }
  }
  return { capturedAt, prices };
}

function appendPriceSnapshot(
  history: PriceSnapshot[],
  rows: AllMarketMonitorRow[],
  capturedAt: number,
) {
  const cutoff = capturedAt - HISTORY_KEEP_MS;
  return [
    ...history.filter((item) => item.capturedAt >= cutoff),
    buildPriceSnapshot(rows, capturedAt),
  ];
}

function buildSpeedMap(
  rows: AllMarketMonitorRow[],
  history: PriceSnapshot[],
  periodSec: SpeedPeriod,
  now: number,
) {
  const target = now - periodSec * 1000;
  let boundarySnapshot: PriceSnapshot | null = null;
  const windowSnapshots: PriceSnapshot[] = [];
  for (const snapshot of history) {
    if (snapshot.capturedAt <= target) {
      boundarySnapshot = snapshot;
      continue;
    }
    windowSnapshots.push(snapshot);
  }
  if (boundarySnapshot) {
    windowSnapshots.unshift(boundarySnapshot);
  }

  const out = new Map<string, number>();
  for (const row of rows) {
    const currentPrice = row.realtime_price;
    if (!isFiniteNumber(currentPrice) || currentPrice <= 0) {
      continue;
    }

    let lowPrice = currentPrice;
    for (const snapshot of windowSnapshots) {
      const snapshotPrice = snapshot.prices[row.ts_code];
      if (
        isFiniteNumber(snapshotPrice) &&
        snapshotPrice > 0 &&
        snapshotPrice < lowPrice
      ) {
        lowPrice = snapshotPrice;
      }
    }

    if (lowPrice > 0) {
      out.set(row.ts_code, (currentPrice / lowPrice - 1) * 100);
    }
  }
  return out;
}

function getSpeedDynamicThresholdPct(
  price: number | null | undefined,
  minTicks: number | null,
) {
  if (
    !isFiniteNumber(price) ||
    price <= 0 ||
    !isFiniteNumber(minTicks) ||
    minTicks <= 0
  ) {
    return null;
  }
  return (minTicks * PRICE_TICK_SIZE * 100) / price;
}

function getEffectiveSpeedThresholdPct(
  row: Pick<AllMarketMonitorRow, "realtime_price">,
  baseThresholdPct: number | null,
  minTicks: number | null,
) {
  if (!isFiniteNumber(baseThresholdPct)) return null;
  const dynamicThresholdPct = getSpeedDynamicThresholdPct(
    row.realtime_price,
    minTicks,
  );
  return isFiniteNumber(dynamicThresholdPct)
    ? Math.max(baseThresholdPct, dynamicThresholdPct)
    : baseThresholdPct;
}

function createEmptyHitRecordsByPeriod(): SpeedHitRecordsByPeriod {
  return {
    10: [],
    30: [],
    60: [],
  };
}

export default function AllMarketMonitorPage() {
  const { excludedConcepts } = useConceptExclusions();
  const [sourcePath, setSourcePath] = useState("");
  const [enabled, setEnabled] = useState(false);
  const [templateDisplayMode, setTemplateDisplayMode] =
    useState<TemplateDisplayMode>(() => readLocalStorageTemplateDisplayMode());
  const [templates, setTemplates] = useState<IntradayMonitorTemplate[]>(() =>
    readStoredTemplates(),
  );
  const [rows, setRows] = useState<AllMarketMonitorRow[]>([]);
  const [indexRows, setIndexRows] = useState<AllMarketIndexRow[]>([]);
  const marketBreadth = useMemo(() => {
    const dateCounts = new Map<string, number>();
    for (const row of rows) {
      if (
        row.realtime_trade_date &&
        isFiniteNumber(row.realtime_price) &&
        row.realtime_price > 0
      ) {
        dateCounts.set(
          row.realtime_trade_date,
          (dateCounts.get(row.realtime_trade_date) ?? 0) + 1,
        );
      }
    }
    const tradeDate = [...dateCounts].sort(
      (a, b) => b[1] - a[1] || b[0].localeCompare(a[0]),
    )[0]?.[0];
    let rising = 0;
    let falling = 0;
    let flat = 0;
    for (const row of rows) {
      if (
        !tradeDate ||
        row.realtime_trade_date !== tradeDate ||
        !isFiniteNumber(row.realtime_price) ||
        row.realtime_price <= 0 ||
        !isFiniteNumber(row.realtime_pre_close) ||
        row.realtime_pre_close <= 0
      ) {
        continue;
      }
      if (row.realtime_price > row.realtime_pre_close) rising += 1;
      else if (row.realtime_price < row.realtime_pre_close) falling += 1;
      else flat += 1;
    }
    return { tradeDate, rising, falling, flat, total: rising + falling + flat };
  }, [rows]);
  const marketTurnover = useMemo(() => {
    const shanghai = indexRows.find((row) => row.ts_code === "000001.SH");
    const shenzhen = indexRows.find((row) => row.ts_code === "399001.SZ");
    const total =
      shanghai?.realtime_trade_date &&
      shanghai.realtime_trade_date === shenzhen?.realtime_trade_date &&
      isFiniteNumber(shanghai.realtime_amount) &&
      isFiniteNumber(shenzhen.realtime_amount)
        ? shanghai.realtime_amount + shenzhen.realtime_amount
        : null;
    return { total };
  }, [indexRows]);
  const [groupSelection, setGroupSelection] = useState<Pick<
    MarketGroup,
    "name" | "kind"
  > | null>(null);
  const [searchStock, setSearchStock] = useState<StockLookupRow | null>(null);
  const [hitPanelMode, setHitPanelMode] = useState<HitPanelMode>("all");
  const [monitorEvents, setMonitorEvents] = useState<MonitorEvent[]>([]);
  const [primarySortKey, setPrimarySortKey] = useState<PrimarySortKey>(
    "realtime_change_pct",
  );
  const [speedPeriod, setSpeedPeriod] = useState<SpeedPeriod>(() =>
    readLocalStorageNumber(LS_KEY_SPEED_PERIOD, DEFAULT_SPEED_PERIOD),
  );
  const [speedThresholdText, setSpeedThresholdText] = useState(() =>
    String(
      readLocalStorageNumber(LS_KEY_SPEED_THRESHOLD, DEFAULT_SPEED_THRESHOLD),
    ),
  );
  const [speedMinTicksText, setSpeedMinTicksText] = useState(() =>
    String(
      readLocalStorageNumber(LS_KEY_SPEED_MIN_TICKS, DEFAULT_SPEED_MIN_TICKS),
    ),
  );
  const [volumeRatioThresholdText, setVolumeRatioThresholdText] = useState(() =>
    String(
      readLocalStorageNumber(
        LS_KEY_VOLUME_RATIO_THRESHOLD,
        DEFAULT_VOLUME_RATIO_THRESHOLD,
      ),
    ),
  );
  const [rankHighlightThresholdText, setRankHighlightThresholdText] = useState(
    () =>
      String(
        readLocalStorageNumber(
          LS_KEY_RANK_HIGHLIGHT_THRESHOLD,
          DEFAULT_RANK_HIGHLIGHT_THRESHOLD,
        ),
      ),
  );
  const [sceneStageThreshold, setSceneStageThreshold] =
    useState<SceneStageThreshold>(() => readLocalStorageSceneStageThreshold());
  const [boardFilter, setBoardFilter] = useState<BoardFilter>(() => {
    try {
      const stored = localStorage.getItem(
        INTRADAY_MONITOR_BOARD_FILTER_STORAGE_KEY,
      );
      if (
        stored === "自选" ||
        STOCK_PICK_BOARD_OPTIONS.includes(
          stored as (typeof STOCK_PICK_BOARD_OPTIONS)[number],
        )
      ) {
        return stored as BoardFilter;
      }
    } catch {
      // localStorage unavailable
    }
    return "全部";
  });
  const [topLimitText, setTopLimitText] = useState(() =>
    String(readLocalStorageNumber(LS_KEY_TOP_LIMIT, DEFAULT_TOP_LIMIT)),
  );
  const [otherSortExpression, setOtherSortExpression] = useState(() =>
    readLocalStorageText(LS_KEY_OTHER_SORT_EXPRESSION),
  );
  const [otherSortDirection, setOtherSortDirection] = useState<
    Exclude<SortDirection, null>
  >(() => readLocalStorageSortDirection(LS_KEY_OTHER_SORT_DIRECTION, "asc"));
  const [compactMode, setCompactMode] = useState(() =>
    readLocalStorageBoolean(LS_KEY_COMPACT_MODE, false),
  );
  const [sideListVisible, setSideListVisible] = useState(() =>
    readLocalStorageSideListVisible(),
  );
  const [sortKey, setSortKey] = useState<SortKey | null>(null);
  const [sortDirection, setSortDirection] = useState<SortDirection>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [templateWarning, setTemplateWarning] = useState("");
  const [volumeRatioStatus, setVolumeRatioStatus] = useState("");
  const [volumeRatioEnabled, setVolumeRatioEnabled] = useState(false);
  const [eastmoneySessionOpen, setEastmoneySessionOpen] = useState(false);
  const eastmoneySessionMenuRef = useRef<HTMLDivElement>(null);
  const [eastmoneySessionBusy, setEastmoneySessionBusy] = useState(false);
  const [eastmoneySessionMessage, setEastmoneySessionMessage] = useState("");
  const [marketStatus, setMarketStatus] = useState("");
  const [refreshedAt, setRefreshedAt] = useState("");
  const [rankDate, setRankDate] = useState("");
  const [requestedCount, setRequestedCount] = useState(0);
  const [fetchedCount, setFetchedCount] = useState(0);
  const [currentTime, setCurrentTime] = useState(() => new Date());
  const [hitRecordsByPeriod, setHitRecordsByPeriod] =
    useState<SpeedHitRecordsByPeriod>(() => createEmptyHitRecordsByPeriod());
  const [volumeRatioNewHighTimes, setVolumeRatioNewHighTimes] = useState<
    Map<string, number>
  >(() => new Map());
  const [changePctNewHighTimes, setChangePctNewHighTimes] = useState<
    Map<string, number>
  >(() => new Map());
  const [volumeRatioRecordHighEventTimes, setVolumeRatioRecordHighEventTimes] =
    useState<Map<string, number>>(() => new Map());
  const [changePctRecordHighEventTimes, setChangePctRecordHighEventTimes] =
    useState<Map<string, number>>(() => new Map());
  const [openHitTsCode, setOpenHitTsCode] = useState<string | null>(null);
  const [openTemplateTsCode, setOpenTemplateTsCode] = useState<string | null>(
    null,
  );
  const [showParams, setShowParams] = useState(false);
  const [refreshSeconds, setRefreshSeconds] = useState(() =>
    Math.min(30, Math.max(1, readLocalStorageNumber("am_refresh_seconds", 1))),
  );
  const [volumeBodyMin, setVolumeBodyMin] = useState(() =>
    Math.max(0, readLocalStorageNumber("am_volume_body_min", 0)),
  );
  const [volumeBodyAbsolute, setVolumeBodyAbsolute] = useState(() =>
    readLocalStorageBoolean("am_volume_body_absolute", false),
  );
  const [volumeAboveAverage, setVolumeAboveAverage] = useState(() =>
    readLocalStorageBoolean("am_volume_above_average", false),
  );
  const [volumeRankMax, setVolumeRankMax] = useState(() =>
    Math.max(0, readLocalStorageNumber("am_volume_rank_max", 0)),
  );
  const [refreshSecondsText, setRefreshSecondsText] = useState(() =>
    String(refreshSeconds),
  );
  const [volumeBodyMinText, setVolumeBodyMinText] = useState(() =>
    String(volumeBodyMin),
  );
  const [volumeRankMaxText, setVolumeRankMaxText] = useState(() =>
    String(volumeRankMax),
  );
  const [volumeBoardFilter, setVolumeBoardFilter] = useState<BoardFilter>(() => {
    const stored = readLocalStorageText("am_volume_board_filter");
    return stored === "自选" ||
      STOCK_PICK_BOARD_OPTIONS.includes(
        stored as (typeof STOCK_PICK_BOARD_OPTIONS)[number],
      )
      ? (stored as BoardFilter)
      : "全部";
  });
  const volumeConditionsRef = useRef({
    volumeBodyMin,
    volumeBodyAbsolute,
    volumeAboveAverage,
    volumeRankMax,
    volumeBoardFilter,
    watchlistCodes: new Set<string>(),
  });
  useEffect(() => {
    try {
      for (const [key, value] of Object.entries({
        am_refresh_seconds: refreshSeconds,
        am_volume_body_min: volumeBodyMin,
        am_volume_body_absolute: volumeBodyAbsolute,
        am_volume_above_average: volumeAboveAverage,
        am_volume_rank_max: volumeRankMax,
        am_volume_board_filter: volumeBoardFilter,
      }))
        localStorage.setItem(key, String(value));
    } catch {
      /* 存储不可用时仍允许本次设置 */
    }
  }, [
    refreshSeconds,
    volumeBodyMin,
    volumeBodyAbsolute,
    volumeAboveAverage,
    volumeRankMax,
    volumeBoardFilter,
  ]);
  const [eventsHeld, setEventsHeld] = useState(false);
  const [heldEvents, setHeldEvents] = useState<MonitorEvent[]>([]);
  const eventListRef = useRef<HTMLDivElement>(null);
  const [volumeRatioNotifications, setVolumeRatioNotifications] = useState(() =>
    readLocalStorageBoolean("am_volume_ratio_notifications", false),
  );
  const [notificationBusy, setNotificationBusy] = useState(false);
  const [notificationError, setNotificationError] = useState("");
  const volumeRatioNotificationsRef = useRef(volumeRatioNotifications);
  volumeRatioNotificationsRef.current = volumeRatioNotifications;

  useEffect(() => {
    try {
      localStorage.setItem(
        "am_volume_ratio_notifications",
        String(volumeRatioNotifications),
      );
    } catch {
      // localStorage unavailable
    }
  }, [volumeRatioNotifications]);
  const [templateModalOpen, setTemplateModalOpen] = useState(false);
  const [watchlistCodes, setWatchlistCodes] = useState<string[]>(() =>
    readStoredIntradayMonitorWatchlist(),
  );
  const watchlistCodeSet = useMemo(
    () => new Set(watchlistCodes),
    [watchlistCodes],
  );
  volumeConditionsRef.current = {
    volumeBodyMin,
    volumeBodyAbsolute,
    volumeAboveAverage,
    volumeRankMax,
    volumeBoardFilter,
    watchlistCodes: watchlistCodeSet,
  };
  const watchlistSelected = !searchStock && boardFilter === "自选";
  const [watchlistModalOpen, setWatchlistModalOpen] = useState(false);
  const templateEnabled = templateDisplayMode !== "off";
  const showTemplateHitsOnly = templateDisplayMode === "hits_only";

  const inFlightRef = useRef(false);
  const enabledRef = useRef(false);
  const volumeRatioEnabledRef = useRef(false);
  const pollTimerRef = useRef<number | undefined>(undefined);
  const lastProcessedDataVersionRef = useRef<string | null>(null);
  const lastProcessedConfigFingerprintRef = useRef<string | null>(null);
  const forceProcessLatestRef = useRef(false);
  const refreshConfigRef = useRef<MonitorRefreshConfig>({
    sceneStageThreshold,
    templateEnabled,
    templates,
    otherSortExpression,
  });
  const historyRef = useRef<PriceSnapshot[]>([]);
  const volumeRatioRecordHighsRef = useRef<
    Map<string, { tradeDate: string; high: number; openingReady: boolean }>
  >(new Map());
  const changePctRecordHighsRef = useRef<
    Map<string, { tradeDate: string; high: number; atHigh: boolean }>
  >(new Map());

  refreshConfigRef.current = {
    sceneStageThreshold,
    templateEnabled,
    templates,
    otherSortExpression,
  };

  const sourcePathTrimmed = sourcePath.trim();
  const enabledTemplates = useMemo(
    () => templates.filter((template) => template.enabled),
    [templates],
  );
  const showOtherSortColumn = primarySortKey === "other_sort_value";
  const showHitPanel = sideListVisible;
  const topLimit = useMemo(
    () => parseNonNegativeIntegerInput(topLimitText, DEFAULT_TOP_LIMIT),
    [topLimitText],
  );

  useEffect(() => {
    if (!marketStatus) return;
    const timer = window.setTimeout(() => setMarketStatus(""), 5000);
    return () => window.clearTimeout(timer);
  }, [marketStatus]);

  useEffect(() => {
    void ensureManagedSourcePath()
      .then(setSourcePath)
      .catch(() => {});
  }, []);

  const updateTemplates = useCallback(
    (nextTemplates: IntradayMonitorTemplate[]) => {
      const enabledTemplateIds = new Set(
        nextTemplates
          .filter((template) => template.enabled)
          .map((template) => template.id),
      );
      setTemplates(nextTemplates);
      setRows((currentRows) =>
        currentRows.map((row) => ({
          ...row,
          template_hits: row.template_hits?.filter((hit) =>
            enabledTemplateIds.has(hit.template_id),
          ),
        })),
      );
      forceProcessLatestRef.current = true;
      writeJsonStorage(
        typeof window === "undefined" ? null : window.localStorage,
        INTRADAY_MONITOR_TEMPLATE_STORAGE_KEY,
        nextTemplates,
      );
    },
    [],
  );

  const updateWatchlist = useCallback((nextCodes: string[]) => {
    setWatchlistCodes(writeStoredIntradayMonitorWatchlist(nextCodes));
  }, []);

  const normalizeParamTexts = useCallback(() => {
    setSpeedThresholdText((value) =>
      withDefaultText(value, DEFAULT_SPEED_THRESHOLD),
    );
    setSpeedMinTicksText((value) =>
      normalizeNonNegativeIntegerText(value, DEFAULT_SPEED_MIN_TICKS),
    );
    setVolumeRatioThresholdText((value) =>
      withDefaultText(value, DEFAULT_VOLUME_RATIO_THRESHOLD),
    );
    setRankHighlightThresholdText((value) =>
      withDefaultText(value, DEFAULT_RANK_HIGHLIGHT_THRESHOLD),
    );
    setTopLimitText((value) =>
      normalizeNonNegativeIntegerText(value, DEFAULT_TOP_LIMIT),
    );
  }, []);

  const closeParams = useCallback(() => {
    normalizeParamTexts();
    setRefreshSecondsText(String(refreshSeconds));
    setVolumeBodyMinText(String(volumeBodyMin));
    setVolumeRankMaxText(String(volumeRankMax));
    setShowParams(false);
  }, [normalizeParamTexts, refreshSeconds, volumeBodyMin, volumeRankMax]);

  useEffect(() => {
    try {
      localStorage.setItem(LS_KEY_SPEED_PERIOD, String(speedPeriod));
    } catch {
      // localStorage unavailable
    }
  }, [speedPeriod]);

  useEffect(() => {
    try {
      localStorage.setItem(LS_KEY_SPEED_THRESHOLD, String(speedThresholdText));
    } catch {
      // localStorage unavailable
    }
  }, [speedThresholdText]);

  useEffect(() => {
    try {
      localStorage.setItem(LS_KEY_SPEED_MIN_TICKS, String(speedMinTicksText));
    } catch {
      // localStorage unavailable
    }
  }, [speedMinTicksText]);

  useEffect(() => {
    try {
      localStorage.setItem(
        LS_KEY_VOLUME_RATIO_THRESHOLD,
        String(volumeRatioThresholdText),
      );
    } catch {
      // localStorage unavailable
    }
  }, [volumeRatioThresholdText]);

  useEffect(() => {
    try {
      localStorage.setItem(
        LS_KEY_RANK_HIGHLIGHT_THRESHOLD,
        String(rankHighlightThresholdText),
      );
    } catch {
      // localStorage unavailable
    }
  }, [rankHighlightThresholdText]);

  useEffect(() => {
    try {
      localStorage.setItem(LS_KEY_SCENE_STAGE_THRESHOLD, sceneStageThreshold);
    } catch {
      // localStorage unavailable
    }
  }, [sceneStageThreshold]);

  useEffect(() => {
    try {
      localStorage.setItem(LS_KEY_TOP_LIMIT, String(topLimit));
    } catch {
      // localStorage unavailable
    }
  }, [topLimit]);

  useEffect(() => {
    try {
      localStorage.setItem(LS_KEY_OTHER_SORT_EXPRESSION, otherSortExpression);
    } catch {
      // localStorage unavailable
    }
  }, [otherSortExpression]);

  useEffect(() => {
    try {
      localStorage.setItem(LS_KEY_OTHER_SORT_DIRECTION, otherSortDirection);
    } catch {
      // localStorage unavailable
    }
  }, [otherSortDirection]);

  useEffect(() => {
    try {
      localStorage.setItem(LS_KEY_COMPACT_MODE, String(compactMode));
    } catch {
      // localStorage unavailable
    }
  }, [compactMode]);

  useEffect(() => {
    try {
      localStorage.setItem(LS_KEY_TEMPLATE_DISPLAY_MODE, templateDisplayMode);
    } catch {
      // localStorage unavailable
    }
  }, [templateDisplayMode]);

  useEffect(() => {
    writeJsonStorage(
      typeof window === "undefined" ? null : window.localStorage,
      LS_KEY_SIDE_LIST_VISIBLE,
      sideListVisible,
    );
  }, [sideListVisible]);

  useEffect(() => {
    try {
      localStorage.setItem(
        INTRADAY_MONITOR_BOARD_FILTER_STORAGE_KEY,
        boardFilter,
      );
    } catch {
      // localStorage unavailable
    }
  }, [boardFilter]);

  useEffect(() => {
    const intervalId = window.setInterval(() => {
      setCurrentTime(new Date());
    }, 1000);
    return () => {
      window.clearInterval(intervalId);
    };
  }, []);

  useEffect(() => {
    if (!showParams) return;

    function onKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        closeParams();
      }
    }

    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [closeParams, showParams]);

  useEffect(() => {
    if (openTemplateTsCode === null && openHitTsCode === null) return;

    function handleMouseDown(event: MouseEvent) {
      const target = event.target as HTMLElement;
      if (
        target.closest(".all-market-template-popover") ||
        target.closest(".all-market-hit-popover") ||
        target.closest(".all-market-template-trigger-btn") ||
        target.closest(".all-market-hit-row")
      ) {
        return;
      }
      setOpenTemplateTsCode(null);
      setOpenHitTsCode(null);
    }

    document.addEventListener("mousedown", handleMouseDown);
    return () => document.removeEventListener("mousedown", handleMouseDown);
  }, [openTemplateTsCode, openHitTsCode]);

  useEffect(() => {
    if (!eastmoneySessionOpen) return;

    function onPointerDown(event: PointerEvent) {
      if (!eastmoneySessionMenuRef.current?.contains(event.target as Node)) {
        setEastmoneySessionOpen(false);
      }
    }
    function onKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        setEastmoneySessionOpen(false);
        eastmoneySessionMenuRef.current
          ?.querySelector<HTMLButtonElement>(".eastmoney-session-trigger")
          ?.focus();
      }
    }
    document.addEventListener("pointerdown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [eastmoneySessionOpen]);

  useEffect(() => {
    lastProcessedDataVersionRef.current = null;
    lastProcessedConfigFingerprintRef.current = null;
    forceProcessLatestRef.current = true;
  }, [sourcePathTrimmed]);

  useEffect(() => {
    const wasEnabled = enabledRef.current;
    enabledRef.current = enabled;
    if (enabled && !wasEnabled) {
      forceProcessLatestRef.current = true;
      setMarketStatus("");
    }
    if (!enabled) {
      setLoading(false);
    }
  }, [enabled]);

  useEffect(() => {
    volumeRatioEnabledRef.current = volumeRatioEnabled;
    void setAllMarketVolumeRatioPaused(!volumeRatioEnabled).catch(
      (pauseError) => {
        setError(`切换盘中量比拉取失败: ${String(pauseError)}`);
        setVolumeRatioEnabled(false);
      },
    );
  }, [volumeRatioEnabled]);

  useEffect(
    () => () => {
      void setAllMarketVolumeRatioPaused(true).catch(() => {});
    },
    [],
  );

  const refreshSnapshot = useCallback(async () => {
    if (
      !sourcePathTrimmed ||
      inFlightRef.current ||
      (!enabledRef.current && !volumeRatioEnabledRef.current)
    ) {
      return;
    }

    inFlightRef.current = true;
    if (enabledRef.current) setLoading(true);
    try {
      const config = refreshConfigRef.current;
      const realtimeProvider = readStoredRealtimeQuoteProvider();
      const configFingerprint = fingerprintMonitorRefreshConfig(
        config,
        realtimeProvider,
      );
      const lastDataVersion =
        forceProcessLatestRef.current ||
        lastProcessedConfigFingerprintRef.current !== configFingerprint
          ? undefined
          : (lastProcessedDataVersionRef.current ?? undefined);
      const result = await getAllMarketMonitorSnapshot(
        sourcePathTrimmed,
        realtimeProvider,
        config.sceneStageThreshold,
        config.templateEnabled,
        config.templateEnabled
          ? config.templates.filter((template) => template.enabled)
          : undefined,
        undefined,
        config.otherSortExpression.trim() || undefined,
        true,
        lastDataVersion,
        volumeRatioEnabledRef.current,
      );
      if (!enabledRef.current && !volumeRatioEnabledRef.current) return;
      if (enabledRef.current) {
        setMarketStatus(result.market_status_message ?? "");
      }
      setVolumeRatioStatus(
        result.volume_ratio_status_message ??
          (volumeRatioEnabledRef.current ? "等待分时数据" : ""),
      );
      if (enabledRef.current && result.market_active === false) {
        if (!volumeRatioEnabledRef.current) {
          window.clearInterval(pollTimerRef.current);
        }
        setEnabled(false);
      }
      // 单独拉取量比只更新进度，保留已暂停监控的表格和异动记录。
      if (!enabledRef.current || result.has_new_data === false) return;

      lastProcessedDataVersionRef.current =
        result.data_version ?? result.refreshed_at ?? null;
      lastProcessedConfigFingerprintRef.current = configFingerprint;
      forceProcessLatestRef.current = false;

      const capturedAt = Date.now();
      const nextRows = result.rows ?? [];
      const nextIndexRows = result.index_rows ?? [];
      const conditions = volumeConditionsRef.current;
      const nextVolumeRatioNewHighTimes = new Map<string, number>();
      const resetVolumeRatioCodes = new Set<string>();
      for (const row of nextRows) {
        const tradeDate = row.realtime_trade_date;
        if (!tradeDate) continue;
        const previous = volumeRatioRecordHighsRef.current.get(row.ts_code);
        if (previous && tradeDate < previous.tradeDate) continue;
        const newDay = !previous || tradeDate > previous.tradeDate;
        if (newDay) {
          volumeRatioRecordHighsRef.current.set(row.ts_code, {
            tradeDate,
            high: 0,
            openingReady: false,
          });
          resetVolumeRatioCodes.add(row.ts_code);
        }
        const high = row.realtime_vol_ratio_high;
        if (!isFiniteNumber(high)) continue;
        const openingHigh = row.realtime_vol_ratio_opening_high;
        const current = row.realtime_vol_ratio;
        const openingReady = isFiniteNumber(openingHigh);
        const dayHigh = newDay ? high : Math.max(high, previous.high);
        // 首次补齐时仅当前正处于新高才提示，避免把中途启动前的历史突破补报。
        if (
          (conditions.volumeBoardFilter === "自选"
            ? conditions.watchlistCodes.has(row.ts_code)
            : conditions.volumeBoardFilter === "全部" ||
              row.board === conditions.volumeBoardFilter) &&
          (conditions.volumeBodyMin === 0 ||
            (isFiniteNumber(row.realtime_change_open_pct) &&
              (conditions.volumeBodyAbsolute
                ? Math.abs(row.realtime_change_open_pct)
                : row.realtime_change_open_pct) >= conditions.volumeBodyMin)) &&
          (!conditions.volumeAboveAverage || isAboveAvgPrice(row)) &&
          (conditions.volumeRankMax === 0 ||
            isRankHighlight(row.best_rank_3d, conditions.volumeRankMax) ||
            isRankHighlight(row.similarity_rank, conditions.volumeRankMax)) &&
          openingReady &&
          isFiniteNumber(current) &&
          current > openingHigh &&
          current >= dayHigh &&
          (newDay || !previous.openingReady || high > previous.high)
        ) {
          nextVolumeRatioNewHighTimes.set(row.ts_code, capturedAt);
        }
        volumeRatioRecordHighsRef.current.set(row.ts_code, {
          tradeDate,
          high: dayHigh,
          openingReady,
        });
      }
      const nextChangePctNewHighTimes = new Map<string, number>();
      const resetChangePctCodes = new Set<string>();
      for (const row of nextRows) {
        const tradeDate = row.realtime_trade_date;
        const high = row.realtime_high;
        const price = row.realtime_price;
        if (
          !tradeDate ||
          !isFiniteNumber(high) ||
          high <= 0 ||
          !isFiniteNumber(price) ||
          price <= 0 ||
          !isFiniteNumber(row.realtime_pre_close) ||
          row.realtime_pre_close <= 0
        ) {
          continue;
        }

        const previous = changePctRecordHighsRef.current.get(row.ts_code);
        if (previous && tradeDate < previous.tradeDate) continue;
        const dayHigh =
          previous?.tradeDate === tradeDate
            ? Math.max(high, previous.high)
            : high;
        // 距当日最高价不超过 0.1% 视为触及高点；持续在范围内不重复触发。
        const atHigh = price >= dayHigh * 0.999 - 1e-8;
        if (!previous || tradeDate > previous.tradeDate) {
          resetChangePctCodes.add(row.ts_code);
          if (atHigh) nextChangePctNewHighTimes.set(row.ts_code, capturedAt);
        } else if (high > previous.high || (atHigh && !previous.atHigh)) {
          nextChangePctNewHighTimes.set(row.ts_code, capturedAt);
        }
        changePctRecordHighsRef.current.set(row.ts_code, {
          tradeDate,
          high: dayHigh,
          atHigh,
        });
      }
      setVolumeRatioNewHighTimes(nextVolumeRatioNewHighTimes);
      setChangePctNewHighTimes(nextChangePctNewHighTimes);
      if (resetVolumeRatioCodes.size > 0) {
        setMonitorEvents((prev) =>
          prev.filter(
            (event) =>
              event.kind !== "realtime_vol_ratio" ||
              !resetVolumeRatioCodes.has(event.record.ts_code),
          ),
        );
      }
      if (
        resetVolumeRatioCodes.size > 0 ||
        nextVolumeRatioNewHighTimes.size > 0
      ) {
        setVolumeRatioRecordHighEventTimes((prev) => {
          const next = new Map(prev);
          resetVolumeRatioCodes.forEach((tsCode) => next.delete(tsCode));
          nextVolumeRatioNewHighTimes.forEach((time, tsCode) => {
            next.set(tsCode, time);
          });
          return next;
        });
      }
      if (resetChangePctCodes.size > 0) {
        setMonitorEvents((prev) =>
          prev.filter(
            (event) =>
              event.kind !== "realtime_change_pct" ||
              !resetChangePctCodes.has(event.record.ts_code),
          ),
        );
      }
      if (resetChangePctCodes.size > 0 || nextChangePctNewHighTimes.size > 0) {
        setChangePctRecordHighEventTimes((prev) => {
          const next = new Map(prev);
          resetChangePctCodes.forEach((tsCode) => next.delete(tsCode));
          nextChangePctNewHighTimes.forEach((time, tsCode) => {
            next.set(tsCode, time);
          });
          return next;
        });
      }
      historyRef.current = appendPriceSnapshot(
        historyRef.current,
        nextRows,
        capturedAt,
      );
      setRows(nextRows);
      setIndexRows(nextIndexRows);
      setRefreshedAt(result.refreshed_at ?? "");
      setRankDate(result.rank_date ?? "");
      setRequestedCount(result.requested_count ?? 0);
      setFetchedCount(result.fetched_count ?? 0);
      setError("");
      setTemplateWarning(result.template_warning_message ?? "");
      if (
        volumeRatioNotificationsRef.current &&
        nextVolumeRatioNewHighTimes.size > 0
      ) {
        const hits = nextRows.filter((row) =>
          nextVolumeRatioNewHighTimes.has(row.ts_code),
        );
        try {
          if (await isPermissionGranted()) {
            if (volumeRatioNotificationsRef.current && enabledRef.current) {
              await invoke("plugin:notification|notify", {
                options: {
                  id: 3101,
                  title:
                    hits.length === 1
                      ? hits[0].name || hits[0].ts_code
                      : `${hits[0].name || hits[0].ts_code} 等 ${hits.length} 只`,
                  body:
                    hits
                      .slice(0, 5)
                      .map(
                        (row) =>
                          `${row.name} ${row.ts_code} 量比 ${formatNumber(row.realtime_vol_ratio)}`,
                      )
                      .join("；") +
                    (hits.length > 5
                      ? `；另有 ${hits.length - 5} 只，详见异动记录`
                      : ""),
                },
              });
              setNotificationError("");
            }
          } else {
            setNotificationError(
              "系统通知权限已关闭，请在系统设置中允许本应用发送通知",
            );
          }
        } catch (notifyError) {
          setNotificationError(`量比异动通知失败: ${String(notifyError)}`);
        }
      }
    } catch (runError) {
      if (enabledRef.current || volumeRatioEnabledRef.current) {
        setError(`实时刷新失败: ${String(runError)}`);
        window.clearInterval(pollTimerRef.current);
        setEnabled(false);
        setVolumeRatioEnabled(false);
      }
    } finally {
      inFlightRef.current = false;
      setLoading(false);
    }
  }, [sourcePathTrimmed]);

  useEffect(() => {
    if (!enabled && !volumeRatioEnabled) return undefined;
    if (!sourcePathTrimmed) {
      setError("请先到“数据管理”页完成数据准备");
      setEnabled(false);
      setVolumeRatioEnabled(false);
      return undefined;
    }

    void refreshSnapshot();
    const intervalId = window.setInterval(() => {
      if (!inFlightRef.current) {
        void refreshSnapshot();
      }
    }, refreshSeconds * 1000);
    pollTimerRef.current = intervalId;

    return () => {
      window.clearInterval(intervalId);
    };
  }, [
    enabled,
    volumeRatioEnabled,
    refreshSnapshot,
    sourcePathTrimmed,
    refreshSeconds,
  ]);

  const speedMapsByPeriod = useMemo(() => {
    const now = Date.now();
    const maps = new Map<SpeedPeriod, Map<string, number>>();
    for (const period of SPEED_PERIOD_OPTIONS) {
      maps.set(period, buildSpeedMap(rows, historyRef.current, period, now));
    }
    return maps;
  }, [rows]);

  const speedMap = useMemo(
    () => speedMapsByPeriod.get(speedPeriod) ?? new Map<string, number>(),
    [speedMapsByPeriod, speedPeriod],
  );

  const speedThresholdPct = useMemo(() => {
    const value = Number(speedThresholdText);
    return Number.isFinite(value) && value > 0 ? value : null;
  }, [speedThresholdText]);

  const speedMinTicks = useMemo(() => {
    const value = Number(speedMinTicksText);
    return Number.isFinite(value) && value >= 0 ? Math.trunc(value) : null;
  }, [speedMinTicksText]);

  const volumeRatioThreshold = useMemo(() => {
    const value = Number(volumeRatioThresholdText);
    return Number.isFinite(value) && value > 0 ? value : null;
  }, [volumeRatioThresholdText]);

  const rankHighlightThreshold = useMemo(() => {
    const value = Number(rankHighlightThresholdText);
    return Number.isFinite(value) && value > 0 ? value : null;
  }, [rankHighlightThresholdText]);

  useEffect(() => {
    volumeRatioRecordHighsRef.current.clear();
    changePctRecordHighsRef.current.clear();
    setVolumeRatioNewHighTimes(new Map());
    setChangePctNewHighTimes(new Map());
    setVolumeRatioRecordHighEventTimes(new Map());
    setChangePctRecordHighEventTimes(new Map());
    setTemplateWarning("");
    setVolumeRatioStatus("");
  }, [sourcePathTrimmed]);

  useEffect(() => {
    setMonitorEvents([]);
    setHitRecordsByPeriod(() => createEmptyHitRecordsByPeriod());
    setOpenHitTsCode(null);
    setOpenTemplateTsCode(null);
  }, [sourcePathTrimmed]);

  useEffect(() => {
    setOpenTemplateTsCode(null);
  }, [templateEnabled, templates]);

  useEffect(() => {
    if (!showHitPanel) {
      setOpenHitTsCode(null);
    }
  }, [showHitPanel]);

  useEffect(() => {
    if (!isFiniteNumber(speedThresholdPct)) return;

    const capturedAt = Date.now();

    setHitRecordsByPeriod((prevByPeriod) => {
      const nextByPeriod = createEmptyHitRecordsByPeriod();

      for (const period of SPEED_PERIOD_OPTIONS) {
        const periodSpeedMap =
          speedMapsByPeriod.get(period) ?? new Map<string, number>();
        const nextRowsByCode = new Map(
          rows.map((row) => [
            row.ts_code,
            {
              ...row,
              speed_pct: periodSpeedMap.get(row.ts_code) ?? null,
            } satisfies DisplayRow,
          ]),
        );
        const nextRecords = new Map<string, SpeedHitRecord>();

        for (const record of prevByPeriod[period]) {
          const latestRow = nextRowsByCode.get(record.ts_code);
          if (!latestRow) continue;
          nextRecords.set(record.ts_code, {
            ...latestRow,
            hit_at: record.hit_at,
            hit_speed_pct: record.hit_speed_pct,
            hit_speed_threshold_pct: record.hit_speed_threshold_pct,
          });
        }

        for (const row of nextRowsByCode.values()) {
          const speedPct = row.speed_pct;
          const effectiveThresholdPct = getEffectiveSpeedThresholdPct(
            row,
            speedThresholdPct,
            speedMinTicks,
          );
          if (
            isFiniteNumber(speedPct) &&
            isFiniteNumber(effectiveThresholdPct) &&
            speedPct >= effectiveThresholdPct
          ) {
            nextRecords.set(row.ts_code, {
              ...row,
              hit_at: capturedAt,
              hit_speed_pct: speedPct,
              hit_speed_threshold_pct: effectiveThresholdPct,
            });
          }
        }

        nextByPeriod[period] = Array.from(nextRecords.values())
          .sort((left, right) => right.hit_at - left.hit_at)
          .slice(0, 200);
      }

      return nextByPeriod;
    });
  }, [rows, speedMapsByPeriod, speedMinTicks, speedThresholdPct]);

  const marketGroups = useMemo(() => {
    const latestDate = rows.reduce(
      (latest, row) =>
        row.realtime_price != null &&
        row.realtime_price > 0 &&
        row.realtime_trade_date &&
        row.realtime_trade_date > latest
          ? row.realtime_trade_date
          : latest,
      "",
    );
    const validRows = rows.filter(
      (row) =>
        row.realtime_price != null &&
        row.realtime_price > 0 &&
        row.realtime_trade_date === latestDate &&
        isFiniteNumber(row.realtime_change_pct),
    );
    return {
      concept: summarizeGroups(validRows, "concept", (row) =>
        filterConceptItems(splitConceptText(row.concept), excludedConcepts),
      ),
      industry: summarizeGroups(validRows, "industry", (row) => [
        row.industry?.trim() ?? "",
      ]),
    };
  }, [rows, excludedConcepts]);
  const selectedGroup = groupSelection
    ? marketGroups[groupSelection.kind].find(
        (group) => group.name === groupSelection.name,
      )
    : null;
  const groupCodes = useMemo(
    () =>
      groupSelection
        ? new Set(selectedGroup?.rows.map((row) => row.ts_code) ?? [])
        : null,
    [groupSelection, selectedGroup],
  );

  const displayRows = useMemo<DisplayRow[]>(() => {
    const filteredRows = rows
      .filter((row) => !searchStock || row.ts_code === searchStock.ts_code)
      .filter((row) => !groupCodes || groupCodes.has(row.ts_code))
      .filter(
        (row) =>
          searchStock ||
          (boardFilter === "自选"
            ? watchlistCodeSet.has(row.ts_code)
            : boardFilter === "全部" || row.board === boardFilter),
      )
      .filter((row) => !showTemplateHitsOnly || getTemplateHits(row).length > 0)
      .map((row) => ({
        ...row,
        speed_pct: speedMap.get(row.ts_code) ?? null,
      }));

    const sortDefinitions = {
      best_rank_3d: { value: (row: DisplayRow) => row.best_rank_3d },
      similarity_rank: { value: (row: DisplayRow) => row.similarity_rank },
      other_sort_value: { value: (row: DisplayRow) => row.other_sort_value },
      realtime_change_pct: {
        value: (row: DisplayRow) => row.realtime_change_pct,
      },
      return_5d_pct: { value: (row: DisplayRow) => row.return_5d_pct },
      speed_pct: { value: (row: DisplayRow) => row.speed_pct },
      realtime_vol_ratio: {
        value: (row: DisplayRow) => row.realtime_vol_ratio,
      },
      above_avg_price: {
        value: (row: DisplayRow) =>
          isFiniteNumber(row.realtime_price) &&
          isFiniteNumber(row.realtime_avg_price) &&
          row.realtime_avg_price > 0
            ? isAboveAvgPrice(row)
            : null,
      },
      realtime_change_open_pct: {
        value: (row: DisplayRow) => row.realtime_change_open_pct,
      },
      total_mv_yi: { value: (row: DisplayRow) => row.total_mv_yi },
    } satisfies Partial<Record<SortKey, SortDefinition<DisplayRow>>>;

    const primarySortDirection: SortDirection =
      primarySortKey === "other_sort_value" ? otherSortDirection : "desc";

    const primaryRows = sortRows(
      filteredRows,
      primarySortKey,
      primarySortDirection,
      sortDefinitions,
    );

    const sortedRows =
      sortKey && sortDirection
        ? sortRows(primaryRows, sortKey, sortDirection, sortDefinitions)
        : primaryRows;

    const templateHitRows: DisplayRow[] = [];
    const normalRows: DisplayRow[] = [];
    for (const row of sortedRows) {
      const templateHit = templateEnabled && getTemplateHits(row).length > 0;
      const speedEffectiveThreshold = getEffectiveSpeedThresholdPct(
        row,
        speedThresholdPct,
        speedMinTicks,
      );
      const shouldPinTemplateHit =
        !showTemplateHitsOnly &&
        templateHit &&
        (primarySortKey === "realtime_change_pct" ||
          (primarySortKey === "realtime_vol_ratio" &&
            isFiniteNumber(volumeRatioThreshold) &&
            isFiniteNumber(row.realtime_vol_ratio) &&
            row.realtime_vol_ratio > volumeRatioThreshold) ||
          (primarySortKey === "speed_pct" &&
            isFiniteNumber(row.speed_pct) &&
            isFiniteNumber(speedEffectiveThreshold) &&
            row.speed_pct >= speedEffectiveThreshold));

      if (shouldPinTemplateHit) {
        templateHitRows.push(row);
      } else {
        normalRows.push(row);
      }
    }

    return [...templateHitRows, ...normalRows].slice(0, topLimit);
  }, [
    boardFilter,
    watchlistCodeSet,
    groupCodes,
    searchStock,
    otherSortDirection,
    primarySortKey,
    rows,
    sortDirection,
    sortKey,
    speedMap,
    speedMinTicks,
    speedThresholdPct,
    showTemplateHitsOnly,
    templateEnabled,
    topLimit,
    volumeRatioThreshold,
  ]);

  const navigationItems = useMemo(
    () =>
      displayRows.map((row) => ({
        tsCode: row.ts_code,
        tradeDate: rankDate || row.realtime_trade_date || null,
        sourcePath: sourcePathTrimmed || undefined,
        name: row.name || undefined,
      })),
    [displayRows, rankDate, sourcePathTrimmed],
  );

  useEffect(() => {
    const detected: MonitorEvent[] = [];
    for (const row of rows) {
      for (const kind of [
        "realtime_change_pct",
        "realtime_vol_ratio",
      ] as const) {
        const at = (
          kind === "realtime_change_pct"
            ? changePctRecordHighEventTimes
            : volumeRatioRecordHighEventTimes
        ).get(row.ts_code);
        const value =
          kind === "realtime_change_pct"
            ? isFiniteNumber(row.realtime_high) &&
              isFiniteNumber(row.realtime_pre_close) &&
              row.realtime_pre_close > 0
              ? (row.realtime_high / row.realtime_pre_close - 1) * 100
              : null
            : row.realtime_vol_ratio_high;
        if (at != null && isFiniteNumber(value))
          detected.push({
            key: `${kind}:${row.ts_code}`,
            kind,
            record: { ...row, hit_at: at },
            value,
            firstAt: at,
            latestAt: at,
            hits: 1,
          });
      }
    }
    for (const period of SPEED_PERIOD_OPTIONS) {
      for (const record of hitRecordsByPeriod[period]) {
        if (isFiniteNumber(record.hit_speed_pct))
          detected.push({
            key: `speed_pct:${period}:${record.ts_code}`,
            kind: "speed_pct",
            period,
            record,
            value: record.hit_speed_pct,
            firstAt: record.hit_at,
            latestAt: record.hit_at,
            hits: 1,
          });
      }
    }
    setMonitorEvents((current) => {
      const next = new Map(current.map((event) => [event.key, event]));
      let changed = false;
      for (const event of detected) {
        const previous = next.get(event.key);
        if (previous && previous.latestAt >= event.latestAt) continue;
        changed = true;
        next.set(
          event.key,
          previous
            ? { ...event, firstAt: previous.firstAt, hits: previous.hits + 1 }
            : event,
        );
      }
      return changed
        ? [...next.values()]
            .sort((a, b) => b.latestAt - a.latestAt)
            .slice(0, 600)
        : current;
    });
  }, [
    rows,
    changePctRecordHighEventTimes,
    volumeRatioRecordHighEventTimes,
    hitRecordsByPeriod,
  ]);

  const visibleEvents = useMemo(() => {
    const filtered = (eventsHeld ? heldEvents : monitorEvents)
      .filter((event) => {
        const row = event.record;
        return (
          (!searchStock || row.ts_code === searchStock.ts_code) &&
          (hitPanelMode === "all" || event.kind === hitPanelMode) &&
          (event.kind !== "speed_pct" || event.period === speedPeriod) &&
          (!groupCodes || groupCodes.has(row.ts_code)) &&
          (searchStock ||
            (boardFilter === "自选"
              ? watchlistCodeSet.has(row.ts_code)
              : boardFilter === "全部" || row.board === boardFilter)) &&
          (!showTemplateHitsOnly || getTemplateHits(row).length > 0)
        );
      })
      .slice(0, 200);
    return filtered;
  }, [
    monitorEvents,
    eventsHeld,
    heldEvents,
    searchStock,
    hitPanelMode,
    speedPeriod,
    groupCodes,
    boardFilter,
    watchlistCodeSet,
    showTemplateHitsOnly,
  ]);
  const visibleHitRecords = visibleEvents
    .filter((event) => event.kind === "speed_pct")
    .map((event) => event.record);

  const hitNavigationItems = useMemo(
    () =>
      visibleHitRecords.map((record) => ({
        tsCode: record.ts_code,
        tradeDate: rankDate || record.realtime_trade_date || null,
        sourcePath: sourcePathTrimmed || undefined,
        name: record.name || undefined,
      })),
    [visibleHitRecords, rankDate, sourcePathTrimmed],
  );

  const openHitRecord = useMemo(
    () =>
      openHitTsCode
        ? (visibleHitRecords.find(
            (record) => record.ts_code === openHitTsCode,
          ) ?? null)
        : null,
    [visibleHitRecords, openHitTsCode],
  );

  useEffect(() => {
    if (openHitTsCode && !openHitRecord) {
      setOpenHitTsCode(null);
    }
  }, [openHitRecord, openHitTsCode]);

  function toggleSort(nextKey: SortKey) {
    if (sortKey !== nextKey) {
      setSortKey(nextKey);
      setSortDirection("desc");
      return;
    }
    if (sortDirection === "desc") {
      setSortDirection("asc");
      return;
    }
    if (sortDirection === "asc") {
      setSortKey(null);
      setSortDirection(null);
      return;
    }
    setSortDirection("desc");
  }

  function renderSortHeader(label: string, key: SortKey) {
    return (
      <TableSortButton
        label={label}
        isActive={sortKey === key}
        direction={sortDirection}
        onClick={() => toggleSort(key)}
      />
    );
  }

  function renderHitMeta(row: DisplayRow) {
    const rankParts = [
      isFiniteNumber(row.best_rank_3d)
        ? `3:${formatNumber(row.best_rank_3d, 0)}`
        : null,
      isFiniteNumber(row.similarity_rank)
        ? `相似:${formatNumber(row.similarity_rank, 0)}`
        : null,
    ].filter((item): item is string => item !== null);
    const rankHighlighted =
      isRankHighlight(row.best_rank_3d, rankHighlightThreshold) ||
      isRankHighlight(row.similarity_rank, rankHighlightThreshold);
    const hasAvgPrice =
      isFiniteNumber(row.realtime_price) &&
      isFiniteNumber(row.realtime_avg_price) &&
      row.realtime_avg_price > 0;
    const aboveAvgPrice = isAboveAvgPrice(row);

    if (
      rankParts.length === 0 &&
      !hasAvgPrice &&
      !isFiniteNumber(row.return_5d_pct) &&
      !isFiniteNumber(row.realtime_change_open_pct) &&
      !isFiniteNumber(row.total_mv_yi)
    ) {
      return null;
    }

    return (
      <div className="all-market-hit-meta">
        {rankParts.length > 0 ? (
          <span
            className={
              rankHighlighted
                ? "all-market-hit-tag is-rank-highlight"
                : "all-market-hit-tag"
            }
            title="3日优 / 策略触发相似榜排名"
          >
            排 {rankParts.map((item) => item.split(":")[1] ?? item).join("/")}
          </span>
        ) : null}
        {hasAvgPrice ? (
          <span
            className={
              aboveAvgPrice
                ? "all-market-hit-tag is-avg-up"
                : "all-market-hit-tag is-muted"
            }
            title={`日内均价 ${formatNumber(row.realtime_avg_price)}`}
          >
            {aboveAvgPrice ? "均上" : "均下"}
          </span>
        ) : null}
        {isFiniteNumber(row.return_5d_pct) ? (
          <span
            className={`all-market-hit-tag ${getPercentClassName(
              row.return_5d_pct,
            )}`}
            title="五日涨幅"
          >
            5日 {formatPercent(row.return_5d_pct)}
          </span>
        ) : null}
        {isFiniteNumber(row.realtime_change_open_pct) ? (
          <span
            className={`all-market-hit-tag ${getPercentClassName(
              row.realtime_change_open_pct,
            )}`}
            title="实体涨幅"
          >
            实 {formatPercent(row.realtime_change_open_pct)}
          </span>
        ) : null}
        {isFiniteNumber(row.total_mv_yi) ? (
          <span
            className="all-market-hit-tag is-muted is-market-value"
            title="总市值"
          >
            市 {formatMarketValueYi(row.total_mv_yi)}
          </span>
        ) : null}
      </div>
    );
  }

  function setPrimarySort(nextKey: PrimarySortKey) {
    setPrimarySortKey(nextKey);
    setSortKey(null);
    setSortDirection(null);
  }

  const statusText = enabled
    ? loading
      ? "抓取中"
      : `每${refreshSeconds}秒刷新`
    : rows.length > 0
      ? "已暂停，保留快照"
      : "已暂停";

  return (
    <div className="all-market-page">
      <section className="all-market-card">
        <div className="all-market-head">
          <div>
            <h2 className="all-market-title">实时监控</h2>
            <div className="all-market-status">
              <span>{statusText}</span>
              <span>
                行情 {fetchedCount}/{requestedCount}
              </span>
              {rankDate ? <span>排名 {rankDate}</span> : null}
              {templateEnabled ? (
                <span>
                  模板 {enabledTemplates.length}/{templates.length}
                </span>
              ) : null}
              {watchlistSelected ? (
                <span>自选 {watchlistCodes.length}只</span>
              ) : null}
            </div>
          </div>

          <div className="all-market-head-actions">
            <div
              className="all-market-template-mode"
              role="group"
              aria-label="模板展示模式"
            >
              <span className="all-market-template-mode-label">模板</span>
              <span className="all-market-template-mode-options">
                <button
                  type="button"
                  className={
                    templateDisplayMode === "off" ? "is-active is-off" : ""
                  }
                  aria-pressed={templateDisplayMode === "off"}
                  onClick={() => setTemplateDisplayMode("off")}
                >
                  关闭
                </button>
                <button
                  type="button"
                  className={templateDisplayMode === "mark" ? "is-active" : ""}
                  aria-pressed={templateDisplayMode === "mark"}
                  onClick={() => setTemplateDisplayMode("mark")}
                >
                  标记命中
                </button>
                <button
                  type="button"
                  className={
                    templateDisplayMode === "hits_only" ? "is-active" : ""
                  }
                  aria-pressed={templateDisplayMode === "hits_only"}
                  onClick={() => setTemplateDisplayMode("hits_only")}
                >
                  只看命中
                </button>
              </span>
            </div>

            <button
              type="button"
              className={
                enabled ? "all-market-toggle is-active" : "all-market-toggle"
              }
              role="switch"
              aria-checked={enabled}
              onClick={() => setEnabled((value) => !value)}
            >
              <span className="all-market-toggle-track" aria-hidden="true">
                <span className="all-market-toggle-thumb" />
              </span>
              <span className="all-market-toggle-text">
                {enabled ? "爬虫运行中" : "爬虫已暂停"}
              </span>
            </button>
          </div>
        </div>

        <div className="all-market-toolbar">
          <div className="all-market-toolbar-controls">
            <div className="all-market-sort-control">
              <span className="all-market-control-label">排序</span>
              <div
                className="all-market-sort-switch"
                role="group"
                aria-label="排序方式"
              >
                <button
                  type="button"
                  className={
                    primarySortKey === "realtime_change_pct" ? "is-active" : ""
                  }
                  onClick={() => setPrimarySort("realtime_change_pct")}
                >
                  涨幅
                </button>
                <button
                  type="button"
                  className={primarySortKey === "speed_pct" ? "is-active" : ""}
                  onClick={() => setPrimarySort("speed_pct")}
                >
                  涨速
                </button>
                <button
                  type="button"
                  className={
                    primarySortKey === "realtime_vol_ratio" ? "is-active" : ""
                  }
                  onClick={() => setPrimarySort("realtime_vol_ratio")}
                >
                  量比
                </button>
                <button
                  type="button"
                  className={
                    primarySortKey === "other_sort_value" ? "is-active" : ""
                  }
                  onClick={() => setPrimarySort("other_sort_value")}
                >
                  其他
                </button>
              </div>
            </div>

            <div className="all-market-config-controls">
              <button
                type="button"
                className="all-market-params-btn"
                onClick={() => setWatchlistModalOpen(true)}
              >
                自选管理
              </button>
              <button
                type="button"
                className="all-market-params-btn"
                onClick={() => setTemplateModalOpen(true)}
              >
                模板管理
              </button>
              <button
                type="button"
                className="all-market-params-btn"
                onClick={() => setShowParams(true)}
              >
                ⚙ 配置项
              </button>
            </div>
          </div>

          <div className="all-market-index-strip" aria-label="指数与市场表现">
            <span className="all-market-control-label">指数表现</span>
            <div className="all-market-index-list">
              {indexRows.length > 0 ? (
                indexRows.map((indexRow) => (
                  <div key={indexRow.ts_code} className="all-market-index-item">
                    <span>{indexRow.name || indexRow.ts_code}</span>
                    <strong
                      className={getPercentClassName(
                        indexRow.realtime_change_pct,
                      )}
                    >
                      {formatPercent(indexRow.realtime_change_pct)}
                    </strong>
                    <small>{formatNumber(indexRow.realtime_price)}</small>
                  </div>
                ))
              ) : (
                <div className="all-market-index-empty">等待指数行情</div>
              )}
            </div>
          </div>
          <div
            className="all-market-summary"
            aria-label="市场涨跌家数与沪深成交额"
            title={`涨跌按全市场有效行情统计（含北交所），不受页面筛选影响，比例条只比较上涨与下跌家数。${marketBreadth.tradeDate ?? "等待行情"}：平盘 ${marketBreadth.flat} 家，有效 ${marketBreadth.total} 家。成交额来自沪深指数行情，单位亿元；日期不一致或数据缺失时合计留空。`}
          >
            <div className="all-market-breadth-counts">
              <span className="all-market-value-up">
                涨{" "}
                <strong>
                  {marketBreadth.total > 0 ? marketBreadth.rising : "--"}
                </strong>
              </span>
              <span className="all-market-value-down">
                跌{" "}
                <strong>
                  {marketBreadth.total > 0 ? marketBreadth.falling : "--"}
                </strong>
              </span>
            </div>
            <div
              className="all-market-ratio-bar"
              role="img"
              aria-label={
                marketBreadth.total > 0
                  ? `上涨 ${marketBreadth.rising} 家，下跌 ${marketBreadth.falling} 家，平盘 ${marketBreadth.flat} 家`
                  : "等待涨跌行情"
              }
            >
              <span
                className="is-up"
                style={{ flexGrow: marketBreadth.rising }}
              />
              <span
                className="is-down"
                style={{ flexGrow: marketBreadth.falling }}
              />
            </div>
            <div className="all-market-turnover-total">
              合计{" "}
              <strong>
                {isFiniteNumber(marketTurnover.total)
                  ? (marketTurnover.total / 1e8).toFixed(0)
                  : "--"}
              </strong>{" "}
              亿
            </div>
          </div>
        </div>

        {error ? <div className="all-market-error">{error}</div> : null}
        {marketStatus ? (
          <div className="all-market-warning" role="status">
            {marketStatus}
          </div>
        ) : null}
        {templateWarning ? (
          <div className="all-market-warning">{templateWarning}</div>
        ) : null}
      </section>

      <div className="all-market-group-grid">
        {(["concept", "industry"] as const).map((kind) => (
          <section className="all-market-card all-market-group-card" key={kind}>
            <div className="all-market-group-head">
              <h3>
                {kind === "concept" ? "当日板块组合" : "行业组合排序"}
                <span>
                  {marketGroups[kind].length}
                  {kind === "concept" ? "个组合" : "个行业"}
                </span>
              </h3>
              <p title="固定按全市场有效行情聚合，不跟随个股表板块筛选 · 加权涨幅按总市值加权">
                全市场 · 等权 / 总市值加权
              </p>
            </div>
            <IntradayMarketGroupTable
              groups={marketGroups[kind]}
              selection={groupSelection}
              onSelect={(group) =>
                setGroupSelection((previous) =>
                  previous?.name === group.name && previous.kind === group.kind
                    ? null
                    : { name: group.name, kind: group.kind },
                )
              }
            />
          </section>
        ))}
      </div>
      {groupSelection ? (
        <div className="all-market-group-selection">
          <strong>{groupSelection.name}</strong>
          <span>
            {selectedGroup?.rows.length ?? 0}只 · 等权{" "}
            {percent(selectedGroup?.change)} · 中位数{" "}
            {percent(selectedGroup?.median)} · 成交额{" "}
            {amount(selectedGroup?.amount)}
          </span>
          <button type="button" onClick={() => setGroupSelection(null)}>
            清除筛选
          </button>
        </div>
      ) : null}

      <div
        className={
          showHitPanel
            ? "all-market-monitor-grid"
            : "all-market-monitor-grid is-hit-hidden"
        }
      >
        <section
          className={
            compactMode
              ? "all-market-card all-market-table-card is-compact"
              : "all-market-card all-market-table-card"
          }
        >
          <div className="all-market-table-head">
            <div
              className="all-market-board-tabs"
              role="group"
              aria-label="板块与搜索"
            >
              {([...STOCK_PICK_BOARD_OPTIONS, "自选"] as const).map((board) => (
                <button
                  key={board}
                  type="button"
                  className={
                    !searchStock && boardFilter === board ? "is-active" : ""
                  }
                  aria-pressed={!searchStock && boardFilter === board}
                  onClick={() => {
                    setSearchStock(null);
                    setBoardFilter(board);
                  }}
                >
                  {board === "科创板"
                    ? "科创"
                    : board === "创业板"
                      ? "创业"
                      : board === "北交所"
                        ? "北交"
                        : board}
                </button>
              ))}
              <GlobalStockSearch
                inline
                selectedStock={searchStock}
                onSelect={setSearchStock}
                onClear={() => setSearchStock(null)}
              />
            </div>
            <div className="all-market-time-strip" aria-live="polite">
              <span className="all-market-time-pill">
                <small>刷新</small>
                <strong>{formatRefreshTime(refreshedAt)}</strong>
              </span>
              <span className="all-market-time-pill is-current">
                <small>当前</small>
                <strong>{formatClock(currentTime)}</strong>
              </span>
            </div>
          </div>

          <div className="all-market-table-wrap">
            {displayRows.length > 0 ? (
              <table className="all-market-table">
                <thead>
                  <tr>
                    <th className="all-market-name-col" aria-sort="none">
                      名称
                    </th>
                    <th
                      className="all-market-basic-col"
                      aria-sort={getAriaSort(
                        sortKey === "best_rank_3d",
                        sortDirection,
                      )}
                    >
                      {renderSortHeader("3日优", "best_rank_3d")}
                    </th>
                    <th
                      className="all-market-basic-col"
                      aria-sort={getAriaSort(
                        sortKey === "similarity_rank",
                        sortDirection,
                      )}
                    >
                      {renderSortHeader("相似榜", "similarity_rank")}
                    </th>
                    <th
                      className="all-market-basic-col"
                      aria-sort={getAriaSort(
                        sortKey === "return_5d_pct",
                        sortDirection,
                      )}
                    >
                      {renderSortHeader("五日涨幅", "return_5d_pct")}
                    </th>
                    {showOtherSortColumn ? (
                      <th
                        className="all-market-other-sort-col all-market-basic-col"
                        aria-sort={getAriaSort(
                          sortKey === "other_sort_value",
                          sortDirection,
                        )}
                      >
                        {renderSortHeader("其他", "other_sort_value")}
                      </th>
                    ) : null}
                    <th
                      className="all-market-realtime-group-start all-market-realtime-change-col"
                      aria-sort={getAriaSort(
                        sortKey === "realtime_change_pct",
                        sortDirection,
                      )}
                    >
                      {renderSortHeader("涨幅", "realtime_change_pct")}
                    </th>
                    <th
                      className="all-market-speed-col"
                      aria-sort={getAriaSort(
                        sortKey === "speed_pct",
                        sortDirection,
                      )}
                    >
                      {renderSortHeader("涨速", "speed_pct")}
                    </th>
                    <th
                      className="all-market-volume-col"
                      aria-sort={getAriaSort(
                        sortKey === "realtime_vol_ratio",
                        sortDirection,
                      )}
                    >
                      {renderSortHeader("量比", "realtime_vol_ratio")}
                    </th>
                    <th
                      className="all-market-basic-col"
                      aria-sort={getAriaSort(
                        sortKey === "above_avg_price",
                        sortDirection,
                      )}
                    >
                      {renderSortHeader("高于均线", "above_avg_price")}
                    </th>
                    <th
                      className="all-market-basic-col"
                      aria-sort={getAriaSort(
                        sortKey === "realtime_change_open_pct",
                        sortDirection,
                      )}
                    >
                      {renderSortHeader("实体涨幅", "realtime_change_open_pct")}
                    </th>
                    <th
                      className="all-market-info-group-start all-market-scene-col all-market-basic-col"
                      aria-sort="none"
                    >
                      场景标记
                    </th>
                    <th
                      className="all-market-template-col all-market-basic-col"
                      aria-sort="none"
                    >
                      模板触发
                    </th>
                    <th
                      className="all-market-mv-col all-market-basic-col"
                      aria-sort={getAriaSort(
                        sortKey === "total_mv_yi",
                        sortDirection,
                      )}
                    >
                      {renderSortHeader("总市值", "total_mv_yi")}
                    </th>
                    <th
                      className="all-market-concept-col all-market-basic-col"
                      aria-sort="none"
                    >
                      概念
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {displayRows.map((row) => {
                    const conceptText = formatConceptText(
                      row.concept ?? "",
                      excludedConcepts,
                    );
                    const rank3dHighlighted = isRankHighlight(
                      row.best_rank_3d,
                      rankHighlightThreshold,
                    );
                    const rankSimHighlighted = isRankHighlight(
                      row.similarity_rank,
                      rankHighlightThreshold,
                    );
                    const volumeRatioAlert =
                      isFiniteNumber(volumeRatioThreshold) &&
                      isFiniteNumber(row.realtime_vol_ratio) &&
                      row.realtime_vol_ratio > volumeRatioThreshold;
                    const templateHit =
                      templateEnabled && getTemplateHits(row).length > 0;

                    return (
                      <tr
                        key={row.ts_code}
                        className={templateHit ? "is-template-hit" : undefined}
                      >
                        <td className="all-market-name-cell">
                          <div className="all-market-stock-id">
                            <DetailsLink
                              tsCode={row.ts_code}
                              tradeDate={
                                rankDate || row.realtime_trade_date || undefined
                              }
                              sourcePath={sourcePathTrimmed || undefined}
                              autoIntraday
                              className="all-market-stock-link"
                              title={`查看 ${row.name || row.ts_code} 详情`}
                              navigationItems={navigationItems}
                            >
                              {row.name || "--"}
                            </DetailsLink>
                            <span className="all-market-stock-code">
                              {row.ts_code}
                            </span>
                          </div>
                          <div className="all-market-compact-primary-list">
                            <span
                              className={
                                rank3dHighlighted
                                  ? "all-market-compact-item is-highlight"
                                  : "all-market-compact-item"
                              }
                            >
                              3日 {formatNumber(row.best_rank_3d, 0)}
                            </span>
                            <span
                              className={
                                rankSimHighlighted
                                  ? "all-market-compact-item is-highlight"
                                  : "all-market-compact-item"
                              }
                            >
                              相似 {formatNumber(row.similarity_rank, 0)}
                            </span>
                            <span
                              className={`all-market-compact-item ${getPercentClassName(
                                row.return_5d_pct,
                              )}`}
                            >
                              五日 {formatPercent(row.return_5d_pct)}
                            </span>
                            {showOtherSortColumn ? (
                              <span className="all-market-compact-item">
                                其他 {formatNumber(row.other_sort_value)}
                              </span>
                            ) : null}
                            <span
                              className={`all-market-compact-item all-market-compact-realtime-start ${getRealtimeChangeCellClassName(
                                row.realtime_change_pct,
                              )}`}
                            >
                              涨幅 {formatPercent(row.realtime_change_pct)}
                              {changePctNewHighTimes.has(row.ts_code) ? (
                                <span className="all-market-record-high-badge">
                                  新高
                                </span>
                              ) : null}
                            </span>
                            <span
                              className={`all-market-compact-item ${getPercentClassName(
                                row.speed_pct,
                              )}`}
                            >
                              涨速 {formatPercent(row.speed_pct)}
                            </span>
                            <span
                              className={
                                volumeRatioAlert
                                  ? "all-market-compact-item is-volume-alert"
                                  : "all-market-compact-item"
                              }
                            >
                              量比 {formatNumber(row.realtime_vol_ratio)}
                              {volumeRatioNewHighTimes.has(row.ts_code) ? (
                                <span className="all-market-record-high-badge">
                                  新高
                                </span>
                              ) : null}
                            </span>
                            <span
                              className={
                                isAboveAvgPrice(row)
                                  ? "all-market-compact-item is-yes"
                                  : "all-market-compact-item"
                              }
                            >
                              均线 {formatAboveAvgPrice(row)}
                            </span>
                            <span
                              className={`all-market-compact-item all-market-compact-open-change ${getPercentClassName(
                                row.realtime_change_open_pct,
                              )}`}
                            >
                              实体 {formatPercent(row.realtime_change_open_pct)}
                            </span>
                          </div>
                        </td>
                        <td
                          className={
                            rank3dHighlighted
                              ? "all-market-rank-cell all-market-basic-col is-highlight"
                              : "all-market-rank-cell all-market-basic-col"
                          }
                        >
                          {formatNumber(row.best_rank_3d, 0)}
                        </td>
                        <td
                          className={
                            rankSimHighlighted
                              ? "all-market-rank-cell all-market-basic-col is-highlight"
                              : "all-market-rank-cell all-market-basic-col"
                          }
                        >
                          {formatNumber(row.similarity_rank, 0)}
                        </td>
                        <td
                          className={`all-market-basic-col ${getPercentClassName(
                            row.return_5d_pct,
                          )}`}
                        >
                          {formatPercent(row.return_5d_pct)}
                        </td>
                        {showOtherSortColumn ? (
                          <td className="all-market-other-sort-col all-market-basic-col">
                            {formatNumber(row.other_sort_value)}
                          </td>
                        ) : null}
                        <td
                          className={`all-market-realtime-change-col ${getRealtimeChangeCellClassName(
                            row.realtime_change_pct,
                          )}`}
                        >
                          <span>{formatPercent(row.realtime_change_pct)}</span>
                          {changePctNewHighTimes.has(row.ts_code) ? (
                            <span
                              className="all-market-record-high-badge"
                              title="当前涨幅高于监控记录"
                            >
                              新高
                            </span>
                          ) : null}
                        </td>
                        <td
                          className={`all-market-speed-col ${getPercentClassName(
                            row.speed_pct,
                          )}`}
                        >
                          {formatPercent(row.speed_pct)}
                        </td>
                        <td
                          className={
                            volumeRatioAlert
                              ? "all-market-volume-col all-market-volume-ratio-cell is-alert"
                              : "all-market-volume-col all-market-volume-ratio-cell"
                          }
                        >
                          <span>{formatNumber(row.realtime_vol_ratio)}</span>
                          {volumeRatioNewHighTimes.has(row.ts_code) ? (
                            <span
                              className="all-market-record-high-badge"
                              title="量比突破开盘五分钟基准及日内已记录高点"
                            >
                              新高
                            </span>
                          ) : null}
                        </td>
                        <td
                          className={
                            isAboveAvgPrice(row)
                              ? "all-market-above-avg-cell all-market-basic-col is-yes"
                              : "all-market-above-avg-cell all-market-basic-col"
                          }
                          title={
                            isFiniteNumber(row.realtime_avg_price)
                              ? `日内均价 ${formatNumber(row.realtime_avg_price)}`
                              : "日内均价 --"
                          }
                        >
                          {formatAboveAvgPrice(row)}
                        </td>
                        <td
                          className={`all-market-basic-col ${getPercentClassName(
                            row.realtime_change_open_pct,
                          )}`}
                        >
                          {formatPercent(row.realtime_change_open_pct)}
                        </td>
                        <td
                          className="all-market-scene-marker-cell all-market-info-group-start all-market-scene-col all-market-basic-col"
                          title={row.scene_marker ?? "--"}
                        >
                          {row.scene_marker ?? "--"}
                        </td>
                        <td className="all-market-template-cell all-market-template-col all-market-basic-col">
                          {getTemplateHits(row).length > 0 ? (
                            <>
                              <button
                                type="button"
                                className="all-market-template-trigger-btn"
                                title={getTemplateHits(row)
                                  .map((hit) => hit.template_name)
                                  .join("、")}
                                onClick={() =>
                                  setOpenTemplateTsCode((value) =>
                                    value === row.ts_code ? null : row.ts_code,
                                  )
                                }
                              >
                                {formatTemplateHitText(row)}
                              </button>
                              {openTemplateTsCode === row.ts_code ? (
                                <div
                                  className="all-market-template-popover"
                                  role="dialog"
                                  onClick={(event) => event.stopPropagation()}
                                >
                                  <div className="all-market-template-popover-head">
                                    <strong>{row.name || row.ts_code}</strong>
                                    <button
                                      type="button"
                                      aria-label="关闭"
                                      onClick={() =>
                                        setOpenTemplateTsCode(null)
                                      }
                                    >
                                      ×
                                    </button>
                                  </div>
                                  <ul>
                                    {getTemplateHits(row).map((hit) => (
                                      <li key={hit.template_id}>
                                        {hit.template_name || "未命名模板"}
                                      </li>
                                    ))}
                                  </ul>
                                </div>
                              ) : null}
                            </>
                          ) : (
                            "--"
                          )}
                        </td>
                        <td className="all-market-mv-col all-market-basic-col">
                          {formatNumber(row.total_mv_yi)}
                        </td>
                        <td
                          className="all-market-concept-cell all-market-concept-col all-market-basic-col"
                          title={conceptText}
                        >
                          {conceptText}
                        </td>
                        <td className="all-market-compact-basic-cell">
                          <div className="all-market-compact-basic-list">
                            <span
                              className="all-market-compact-item all-market-compact-group-start"
                              title={row.scene_marker ?? "--"}
                            >
                              场景 {row.scene_marker ?? "--"}
                            </span>
                            <span
                              className="all-market-compact-item all-market-compact-template-hits"
                              title={
                                getTemplateHits(row).length > 0
                                  ? getTemplateHits(row)
                                      .map(
                                        (hit) =>
                                          hit.template_name || "未命名模板",
                                      )
                                      .join("、")
                                  : "--"
                              }
                            >
                              模板{" "}
                              {getTemplateHits(row).length > 0
                                ? getTemplateHits(row)
                                    .map(
                                      (hit) =>
                                        hit.template_name || "未命名模板",
                                    )
                                    .join("、")
                                : "--"}
                            </span>
                            <span className="all-market-compact-item">
                              市值 {formatNumber(row.total_mv_yi)}
                            </span>
                            <span
                              className="all-market-compact-item all-market-compact-concept"
                              title={conceptText}
                            >
                              概念 {conceptText || "--"}
                            </span>
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            ) : (
              <div className="all-market-empty-state">
                {showTemplateHitsOnly && rows.length > 0
                  ? "当前没有模板命中的股票。"
                  : enabled
                    ? "等待行情返回。"
                    : "开启爬虫后开始刷新。"}
              </div>
            )}
          </div>
        </section>

        {showHitPanel ? (
          <section className="all-market-card all-market-hit-card">
            <div className="all-market-hit-head">
              <div>
                <h3>异动流水</h3>
              </div>
              <strong title="最新在前，同股同类合并计次">{visibleEvents.length}</strong>
            </div>
            <div className="all-market-volume-ratio-row">
              <div
                className="all-market-volume-ratio-controls"
                role="group"
                aria-label="量比分时抓取"
              >
                <button
                  type="button"
                  className={`all-market-volume-switch${volumeRatioEnabled ? " is-active" : ""}`}
                  role="switch"
                  aria-checked={volumeRatioEnabled}
                  title="独立控制量比分时基准补齐，可在实时监控暂停时开启"
                  onClick={() => {
                    setVolumeRatioStatus(
                      volumeRatioEnabled ? "已暂停" : "准备拉取…",
                    );
                    setVolumeRatioEnabled((value) => !value);
                  }}
                >
                  <span>量比拉取</span>
                  <span className="all-market-volume-switch-track" aria-hidden="true" />
                </button>
                <div
                  className="eastmoney-session-menu"
                  ref={eastmoneySessionMenuRef}
                >
                  <button
                    type="button"
                    className="eastmoney-session-trigger"
                    title="东方财富访问验证"
                    aria-expanded={eastmoneySessionOpen}
                    aria-controls="eastmoney-session-popover"
                    onClick={() => setEastmoneySessionOpen((value) => !value)}
                  >
                    东财验证
                  </button>
                  {eastmoneySessionOpen ? (
                    <div
                      className="eastmoney-session-popover"
                      id="eastmoney-session-popover"
                    >
                      <div className="eastmoney-session-popover-head">
                        <strong>东财验证</strong>
                        <button
                          type="button"
                          aria-label="关闭东财验证弹窗"
                          onClick={() => setEastmoneySessionOpen(false)}
                        >
                          关闭
                        </button>
                      </div>
                      <div
                        className="eastmoney-session-actions"
                        role="group"
                        aria-label="东财验证操作"
                        aria-busy={eastmoneySessionBusy}
                      >
                        <button
                          className="all-market-toggle"
                          type="button"
                          disabled={eastmoneySessionBusy}
                          onClick={async () => {
                            setEastmoneySessionBusy(true);
                            try {
                              await openEastmoneyVerification();
                              setEastmoneySessionMessage(
                                "验证后点击“检查连接”。",
                              );
                            } catch (sessionError) {
                              setEastmoneySessionMessage(String(sessionError));
                            } finally {
                              setEastmoneySessionBusy(false);
                            }
                          }}
                        >
                          打开验证页
                        </button>
                        <button
                          className="all-market-toggle"
                          type="button"
                          disabled={eastmoneySessionBusy}
                          onClick={async () => {
                            setEastmoneySessionBusy(true);
                            setEastmoneySessionMessage("检查中…");
                            try {
                              await connectEastmoneySession();
                              setEastmoneySessionMessage("已连接");
                            } catch (sessionError) {
                              setEastmoneySessionMessage(
                                `连接失败：${String(sessionError)}`,
                              );
                            } finally {
                              setEastmoneySessionBusy(false);
                            }
                          }}
                        >
                          检查连接
                        </button>
                        <button
                          className="all-market-toggle"
                          type="button"
                          disabled={eastmoneySessionBusy}
                          onClick={async () => {
                            setEastmoneySessionBusy(true);
                            try {
                              await stopEastmoneyVerifiedSession();
                              setEastmoneySessionMessage("已停用");
                            } catch (sessionError) {
                              setEastmoneySessionMessage(String(sessionError));
                            } finally {
                              setEastmoneySessionBusy(false);
                            }
                          }}
                        >
                          停用会话
                        </button>
                      </div>
                      {eastmoneySessionMessage ? (
                        <p role="status">{eastmoneySessionMessage}</p>
                      ) : null}
                    </div>
                  ) : null}
                </div>
              </div>
              {volumeRatioEnabled || volumeRatioStatus ? (
                <span
                  className="all-market-volume-ratio-progress"
                  role="status"
                >
                  {volumeRatioStatus || "准备拉取…"}
                </span>
              ) : null}
            </div>
            <div
              className="all-market-event-tabs"
              role="group"
              aria-label="异动类型"
            >
              {(
                [
                  ["all", "全部"],
                  ["realtime_change_pct", "涨幅"],
                  ["realtime_vol_ratio", "量比"],
                  ["speed_pct", "涨速"],
                ] as const
              ).map(([kind, label]) => (
                <button
                  type="button"
                  key={kind}
                  className={hitPanelMode === kind ? "is-active" : ""}
                  aria-pressed={hitPanelMode === kind}
                  onClick={() => {
                    setEventsHeld(false);
                    eventListRef.current?.scrollTo({ top: 0 });
                    setHitPanelMode(kind);
                    setOpenHitTsCode(null);
                  }}
                >
                  {label}
                </button>
              ))}
            </div>
            {eventsHeld ? (
              <button
                type="button"
                className="all-market-params-btn"
                onClick={() => {
                  setEventsHeld(false);
                  eventListRef.current?.scrollTo({ top: 0 });
                }}
              >
                正在查看历史 · 回到最新
              </button>
            ) : null}
            <div
              className="all-market-hit-list"
              ref={eventListRef}
              onScroll={(event) => {
                const away = event.currentTarget.scrollTop > 8;
                if (away && !eventsHeld) {
                  setHeldEvents(visibleEvents);
                  setEventsHeld(true);
                } else if (!away && eventsHeld) setEventsHeld(false);
              }}
            >
              {visibleEvents.map((event) => {
                const record = event.record;
                return (
                  <div
                    key={event.key}
                    className="all-market-hit-row all-market-event-row"
                  >
                    <div className="all-market-hit-main">
                      <DetailsLink
                        tsCode={record.ts_code}
                        tradeDate={
                          rankDate || record.realtime_trade_date || undefined
                        }
                        sourcePath={sourcePathTrimmed || undefined}
                        autoIntraday
                        className="all-market-hit-name-link"
                        navigationItems={
                          event.kind === "speed_pct"
                            ? hitNavigationItems
                            : navigationItems
                        }
                      >
                        <strong>{record.name || "--"}</strong>
                        <small>{record.ts_code}</small>
                      </DetailsLink>
                      <div className="all-market-event-label">
                        <span>
                          {event.kind === "realtime_change_pct"
                            ? "涨幅新高"
                            : event.kind === "realtime_vol_ratio"
                              ? "日内量比新高"
                              : `${event.period ?? speedPeriod}秒涨速命中`}
                        </span>
                        <strong className={getPercentClassName(event.value)}>
                          {event.kind === "realtime_vol_ratio"
                            ? formatNumber(event.value)
                            : formatPercent(event.value)}
                        </strong>
                      </div>
                      {renderHitMeta(record)}
                    </div>
                    <div className="all-market-event-time">
                      <time>{formatClockFromMs(event.latestAt)}</time>
                      <small title={`首次 ${formatClockFromMs(event.firstAt)}`}>
                        {event.hits > 1 ? `${event.hits}次` : "首次"}
                      </small>
                      {event.kind === "speed_pct" ? (
                        <button
                          type="button"
                          aria-label={`查看${record.name}涨速记录`}
                          onClick={() =>
                            setOpenHitTsCode((value) =>
                              value === record.ts_code ? null : record.ts_code,
                            )
                          }
                        >
                          详情
                        </button>
                      ) : null}
                    </div>
                  </div>
                );
              })}
              {!visibleEvents.length ? (
                <div className="all-market-hit-empty">
                  {rows.length === 0
                    ? "等待监控异动"
                    : "当前筛选下暂无异动。"}
                </div>
              ) : null}
            </div>

            {openHitRecord ? (
              <div
                className="all-market-hit-popover"
                role="dialog"
                onClick={(event) => event.stopPropagation()}
              >
                <div className="all-market-hit-popover-head">
                  <DetailsLink
                    tsCode={openHitRecord.ts_code}
                    tradeDate={
                      rankDate || openHitRecord.realtime_trade_date || undefined
                    }
                    sourcePath={sourcePathTrimmed || undefined}
                    autoIntraday
                    className="all-market-stock-link"
                    title={`查看 ${openHitRecord.name || openHitRecord.ts_code} 详情`}
                    navigationItems={navigationItems}
                  >
                    {openHitRecord.name || "--"}
                  </DetailsLink>
                  <button
                    type="button"
                    aria-label="关闭"
                    onClick={() => setOpenHitTsCode(null)}
                  >
                    ×
                  </button>
                </div>
                <div className="all-market-hit-popover-code">
                  {openHitRecord.ts_code}
                </div>
                <dl className="all-market-hit-detail-grid">
                  <div>
                    <dt>3日优</dt>
                    <dd>{formatNumber(openHitRecord.best_rank_3d, 0)}</dd>
                  </div>
                  <div>
                    <dt>相似榜</dt>
                    <dd>{formatNumber(openHitRecord.similarity_rank, 0)}</dd>
                  </div>
                  <div>
                    <dt>涨幅</dt>
                    <dd
                      className={getPercentClassName(
                        openHitRecord.realtime_change_pct,
                      )}
                    >
                      {formatPercent(openHitRecord.realtime_change_pct)}
                    </dd>
                  </div>
                  <div>
                    <dt>涨速</dt>
                    <dd
                      className={getPercentClassName(openHitRecord.speed_pct)}
                    >
                      {formatPercent(openHitRecord.speed_pct)}
                    </dd>
                  </div>
                  <div>
                    <dt>盘中量比</dt>
                    <dd>{formatNumber(openHitRecord.realtime_vol_ratio)}</dd>
                  </div>
                  <div>
                    <dt>五日涨幅</dt>
                    <dd
                      className={getPercentClassName(
                        openHitRecord.return_5d_pct,
                      )}
                    >
                      {formatPercent(openHitRecord.return_5d_pct)}
                    </dd>
                  </div>
                  <div>
                    <dt>命中涨速</dt>
                    <dd
                      className={getPercentClassName(
                        openHitRecord.hit_speed_pct,
                      )}
                    >
                      {formatPercent(openHitRecord.hit_speed_pct)}
                    </dd>
                  </div>
                  <div>
                    <dt>生效阈值</dt>
                    <dd>
                      {formatPercent(
                        openHitRecord.hit_speed_threshold_pct ??
                          getEffectiveSpeedThresholdPct(
                            openHitRecord,
                            speedThresholdPct,
                            speedMinTicks,
                          ),
                      )}
                    </dd>
                  </div>
                  <div>
                    <dt>命中时间</dt>
                    <dd>{formatClockFromMs(openHitRecord.hit_at)}</dd>
                  </div>
                  <div>
                    <dt>实体涨幅</dt>
                    <dd
                      className={getPercentClassName(
                        openHitRecord.realtime_change_open_pct,
                      )}
                    >
                      {formatPercent(openHitRecord.realtime_change_open_pct)}
                    </dd>
                  </div>
                  <div>
                    <dt>总市值</dt>
                    <dd>{formatNumber(openHitRecord.total_mv_yi)}</dd>
                  </div>
                </dl>
                <div
                  className="all-market-hit-concept"
                  title={formatConceptText(
                    openHitRecord.concept ?? "",
                    excludedConcepts,
                  )}
                >
                  {formatConceptText(
                    openHitRecord.concept ?? "",
                    excludedConcepts,
                  )}
                </div>
              </div>
            ) : null}
          </section>
        ) : null}
      </div>

      {showParams ? (
        <div
          className="settings-modal-backdrop"
          role="presentation"
          onClick={(event) => {
            if (event.target === event.currentTarget) {
              closeParams();
            }
          }}
        >
          <section
            className="settings-modal settings-modal-narrow all-market-config-modal"
            role="dialog"
            aria-modal="true"
            aria-label="实时监控配置项"
          >
            <div className="settings-modal-head">
              <div>
                <h3 className="settings-subtitle-head">配置项</h3>
              </div>
              <div className="settings-actions">
                <button
                  className="settings-primary-btn"
                  type="button"
                  onClick={closeParams}
                >
                  完成
                </button>
              </div>
            </div>

            <div className="all-market-params-form">
              <section className="all-market-config-section" aria-label="刷新">
                <h4 className="all-market-config-heading">刷新</h4>
                <label className="settings-field">
                  <span>刷新间隔（1–30 秒）</span>
                  <input
                    type="number"
                    min="1"
                    max="30"
                    step="1"
                    value={refreshSecondsText}
                    onChange={(event) => {
                      const text = event.target.value;
                      setRefreshSecondsText(text);
                      if (text !== "" && Number.isFinite(Number(text))) {
                        setRefreshSeconds(
                          Math.min(30, Math.max(1, Math.trunc(Number(text)))),
                        );
                      }
                    }}
                    onBlur={() => setRefreshSecondsText(String(refreshSeconds))}
                  />
                </label>
              </section>
              <section className="all-market-config-section" aria-label="涨速异动">
                <h4 className="all-market-config-heading">涨速异动</h4>
                <label className="settings-field">
                  <span>涨速周期</span>
                  <select
                    value={speedPeriod}
                    onChange={(event) =>
                      setSpeedPeriod(Number(event.target.value) as SpeedPeriod)
                    }
                  >
                    {SPEED_PERIOD_OPTIONS.map((value) => (
                      <option key={value} value={value}>
                        {value}秒
                      </option>
                    ))}
                  </select>
                </label>

                <label className="settings-field">
                  <span>涨速命中阈值 %</span>
                  <input
                    type="number"
                    min="0.01"
                    step="0.01"
                    value={speedThresholdText}
                    onChange={(event) =>
                      setSpeedThresholdText(event.target.value)
                    }
                  />
                </label>

                <label className="settings-field">
                  <span>低价校正最低档位</span>
                  <input
                    type="number"
                    min="0"
                    step="1"
                    inputMode="numeric"
                    value={speedMinTicksText}
                    onChange={(event) => setSpeedMinTicksText(event.target.value)}
                  />
                </label>
              </section>
              <section className="all-market-config-section" aria-label="量比异动">
                <h4 className="all-market-config-heading">量比异动</h4>
                <label className="settings-field">
                  <span>板块范围</span>
                  <select
                    value={volumeBoardFilter}
                    onChange={(event) => {
                      setVolumeBoardFilter(event.target.value as BoardFilter);
                    }}
                  >
                    {([...STOCK_PICK_BOARD_OPTIONS, "自选"] as const).map(
                      (board) => (
                        <option key={board} value={board}>
                          {board}
                        </option>
                      ),
                    )}
                  </select>
                  <small>仅筛选量比异动；主行情仍全市场刷新</small>
                </label>
                <label className="settings-field">
                  <span>实体涨幅下限（%）</span>
                  <input
                    type="number"
                    min="0"
                    step="0.1"
                    title="最新价相对开盘价的涨幅；0 不限制"
                    value={volumeBodyMinText}
                    onChange={(event) => {
                      const text = event.target.value;
                      setVolumeBodyMinText(text);
                      if (text !== "" && Number.isFinite(Number(text))) {
                        setVolumeBodyMin(Math.max(0, Number(text)));
                      }
                    }}
                    onBlur={() => setVolumeBodyMinText(String(volumeBodyMin))}
                  />
                  <small>相对开盘价；0 不限</small>
                </label>
                <label className="settings-field">
                  <span>排名上限</span>
                  <input
                    type="number"
                    min="0"
                    step="1"
                    value={volumeRankMaxText}
                    onChange={(event) => {
                      const text = event.target.value;
                      setVolumeRankMaxText(text);
                      if (text !== "" && Number.isFinite(Number(text))) {
                        setVolumeRankMax(Math.max(0, Math.floor(Number(text))));
                      }
                    }}
                    onBlur={() => setVolumeRankMaxText(String(volumeRankMax))}
                  />
                  <small title="缺失排名不通过">3日优 / 相似榜任一满足；0 不限</small>
                </label>
                <label className="settings-field">
                  <span>排序 / 高亮阈值</span>
                  <input
                    type="number"
                    min="0.01"
                    step="0.01"
                    value={volumeRatioThresholdText}
                    onChange={(event) =>
                      setVolumeRatioThresholdText(event.target.value)
                    }
                  />
                </label>

                <label className="all-market-setting-switch">
                  <input
                    type="checkbox"
                    role="switch"
                    checked={volumeBodyAbsolute}
                    onChange={(event) =>
                      setVolumeBodyAbsolute(event.target.checked)
                    }
                  />
                  <span>实体取绝对值</span>
                </label>
                <label className="all-market-setting-switch">
                  <input
                    type="checkbox"
                    role="switch"
                    checked={volumeAboveAverage}
                    onChange={(event) =>
                      setVolumeAboveAverage(event.target.checked)
                    }
                  />
                  <span>仅均价线上方</span>
                </label>
                <p className="settings-section-note all-market-param-expression">
                  满足筛选条件，突破开盘 5 分钟量比高点并创日内新高时触发。
                </p>
                <div className="all-market-param-expression" title="监控运行时通知，同轮异动合并发送">
                  <label className="all-market-setting-switch">
                    <input
                      type="checkbox"
                      role="switch"
                      checked={volumeRatioNotifications}
                      disabled={notificationBusy}
                      onChange={async (event) => {
                        if (!event.target.checked) {
                          volumeRatioNotificationsRef.current = false;
                          setVolumeRatioNotifications(false);
                          setNotificationError("");
                          return;
                        }
                        setNotificationBusy(true);
                        try {
                          const granted =
                            (await isPermissionGranted()) ||
                            (await requestPermission()) === "granted";
                          volumeRatioNotificationsRef.current = granted;
                          setVolumeRatioNotifications(granted);
                          setNotificationError(
                            granted
                              ? ""
                              : "系统未允许通知，请在系统设置中开启本应用通知权限",
                          );
                        } catch (permissionError) {
                          setNotificationError(
                            `申请系统通知权限失败: ${String(permissionError)}`,
                          );
                        } finally {
                          setNotificationBusy(false);
                        }
                      }}
                    />
                    <span>系统通知</span>
                  </label>
                  {notificationError ? (
                    <p role="alert">{notificationError}</p>
                  ) : null}
                </div>
              </section>
              <section className="all-market-config-section" aria-label="展示与排序">
                <h4 className="all-market-config-heading">展示与排序</h4>
                <label className="settings-field">
                  <span>排名高亮阈值</span>
                  <input
                    type="number"
                    min="1"
                    step="1"
                    value={rankHighlightThresholdText}
                    onChange={(event) =>
                      setRankHighlightThresholdText(event.target.value)
                    }
                  />
                </label>

                <label className="settings-field">
                  <span>场景等级阈值</span>
                  <select
                    value={sceneStageThreshold}
                    onChange={(event) =>
                      setSceneStageThreshold(
                        event.target.value as SceneStageThreshold,
                      )
                    }
                  >
                    {SCENE_STAGE_THRESHOLD_OPTIONS.map((item) => (
                      <option key={item.value} value={item.value}>
                        {item.label}
                      </option>
                    ))}
                  </select>
                </label>

                <label className="settings-field">
                  <span>Top N</span>
                  <input
                    type="number"
                    min="0"
                    step="1"
                    inputMode="numeric"
                    value={topLimitText}
                    onChange={(event) => setTopLimitText(event.target.value)}
                  />
                </label>

                <label className="settings-field">
                  <span>其他排序方向</span>
                  <select
                    value={otherSortDirection}
                    onChange={(event) =>
                      setOtherSortDirection(
                        event.target.value === "asc" ? "asc" : "desc",
                      )
                    }
                  >
                    <option value="asc">小数/否在前</option>
                    <option value="desc">大数/是在前</option>
                  </select>
                </label>

                <label className="settings-checkbox-inline all-market-param-compact">
                  <input
                    type="checkbox"
                    checked={compactMode}
                    onChange={(event) => setCompactMode(event.target.checked)}
                  />
                  <span>开启紧凑模式</span>
                </label>

                <label className="settings-checkbox-inline all-market-param-side-list">
                  <input
                    type="checkbox"
                    checked={sideListVisible}
                    onChange={(event) => setSideListVisible(event.target.checked)}
                  />
                  <span>显示侧边名单</span>
                </label>

                <label className="settings-field all-market-param-expression">
                  <span>其他排序表达式</span>
                  <textarea
                    className="settings-textarea all-market-param-expression-textarea"
                    value={otherSortExpression}
                    onChange={(event) =>
                      setOtherSortExpression(event.target.value)
                    }
                    placeholder="示例：RT_OP；C > RT_AVG；TOTAL_MV_YI"
                  />
                  <small className="all-market-param-expression-hint">
                    默认使用实时最新一根；表达式需要 REF、MA、RANK
                    或指标预热时会自动补历史。RANK/SCORE
                    最新实时一根为空，上一交易日排名用
                    REF(RANK,1)。提示:可用IF语句二次排序
                  </small>
                </label>
              </section>
            </div>
          </section>
        </div>
      ) : null}

      <IntradayTemplateManagerModal
        open={templateModalOpen}
        sourcePath={sourcePathTrimmed}
        templates={templates}
        onChangeTemplates={updateTemplates}
        onClose={() => setTemplateModalOpen(false)}
      />

      <WatchlistModal
        open={watchlistModalOpen}
        sourcePath={sourcePathTrimmed}
        currentCodes={watchlistCodes}
        onChangeCodes={updateWatchlist}
        onClose={() => setWatchlistModalOpen(false)}
      />
    </div>
  );
}
