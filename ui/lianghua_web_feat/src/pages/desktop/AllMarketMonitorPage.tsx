import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import {
  getAllMarketMonitorSnapshot,
  type AllMarketIndexRow,
  type AllMarketMonitorRow,
  type IntradayMonitorTemplate,
} from "../../apis/reader";
import IntradayTemplateManagerModal from "./components/IntradayTemplateManagerModal";
import WatchlistModal from "./components/WatchlistModal";
import DetailsLink from "../../shared/DetailsLink";
import {
  formatConceptText,
  useConceptExclusions,
} from "../../shared/conceptExclusions";
import { readStoredRealtimeQuoteProvider } from "../../shared/realtimeSettings";
import {
  readStoredIntradayMonitorWatchlistEnabled,
  readStoredIntradayMonitorWatchlist,
  writeStoredIntradayMonitorWatchlistEnabled,
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

const POLL_INTERVAL_MS = 1000;
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
const LS_KEY_SIDE_LIST_DEBUG_ROWS = "am_side_list_debug_rows";
const INTRADAY_MONITOR_TEMPLATE_STORAGE_KEY =
  "lh_intraday_monitor_realtime_templates_v1";
const DEBUG_SIDE_LIST_LIMIT = 12;

function readLocalStorageNumber<T extends number>(key: string, fallback: T): T {
  try {
    const raw = localStorage.getItem(key);
    if (raw != null) {
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
  const field = base === "open" ? "REALTIME_CHANGE_OPEN_PCT" : "PCT_CHG";
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
type HitPanelMode = "realtime_change_pct" | "realtime_vol_ratio" | "speed_pct";
type SpeedPeriod = (typeof SPEED_PERIOD_OPTIONS)[number];
type BoardFilter = (typeof STOCK_PICK_BOARD_OPTIONS)[number];
type SortKey =
  | "best_rank_3d"
  | "best_rank_5d"
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

type DisplayRow = AllMarketMonitorRow & {
  speed_pct?: number | null;
};

type SpeedHitRecord = DisplayRow & {
  hit_at: number;
  hit_speed_pct?: number | null;
  hit_speed_threshold_pct?: number | null;
};

type SpeedHitRecordsByPeriod = Record<SpeedPeriod, SpeedHitRecord[]>;

type RecordHighDisplayRow = DisplayRow & {
  record_high_at: number;
};

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
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

function updateRecordHighs(
  recordHighs: Map<string, number>,
  rows: AllMarketMonitorRow[],
  capturedAt: number,
  getValue: (row: AllMarketMonitorRow) => number | null | undefined,
) {
  const newHighTimes = new Map<string, number>();

  for (const row of rows) {
    const value = getValue(row);
    if (!isFiniteNumber(value)) {
      continue;
    }

    const previous = recordHighs.get(row.ts_code);
    if (!isFiniteNumber(previous)) {
      recordHighs.set(row.ts_code, value);
      continue;
    }

    if (value <= previous) {
      continue;
    }

    recordHighs.set(row.ts_code, value);
    newHighTimes.set(row.ts_code, capturedAt);
  }

  return newHighTimes;
}

export default function AllMarketMonitorPage() {
  const { excludedConcepts } = useConceptExclusions();
  const [sourcePath, setSourcePath] = useState("");
  const [enabled, setEnabled] = useState(false);
  const [templateEnabled, setTemplateEnabled] = useState(false);
  const [templates, setTemplates] = useState<IntradayMonitorTemplate[]>(() =>
    readStoredTemplates(),
  );
  const [rows, setRows] = useState<AllMarketMonitorRow[]>([]);
  const [indexRows, setIndexRows] = useState<AllMarketIndexRow[]>([]);
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
  const [boardFilter, setBoardFilter] = useState<BoardFilter>("全部");
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
  const [sideListDebugRowsEnabled] = useState(() =>
    readLocalStorageBoolean(LS_KEY_SIDE_LIST_DEBUG_ROWS, false),
  );
  const [sortKey, setSortKey] = useState<SortKey | null>(null);
  const [sortDirection, setSortDirection] = useState<SortDirection>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [templateWarning, setTemplateWarning] = useState("");
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
  const [templateModalOpen, setTemplateModalOpen] = useState(false);
  const [watchlistEnabled, setWatchlistEnabled] = useState(() =>
    readStoredIntradayMonitorWatchlistEnabled(),
  );
  const [watchlistCodes, setWatchlistCodes] = useState<string[]>(() =>
    readStoredIntradayMonitorWatchlist(),
  );
  const [watchlistModalOpen, setWatchlistModalOpen] = useState(false);

  const inFlightRef = useRef(false);
  const enabledRef = useRef(false);
  const historyRef = useRef<PriceSnapshot[]>([]);
  const volumeRatioRecordHighsRef = useRef<Map<string, number>>(new Map());
  const changePctRecordHighsRef = useRef<Map<string, number>>(new Map());
  const debugSideListTimestampRef = useRef(Date.now());

  const sourcePathTrimmed = sourcePath.trim();
  const isVolumeRatioBoard = primarySortKey === "realtime_vol_ratio";
  const showOtherSortColumn = primarySortKey === "other_sort_value";
  const hitPanelMode: HitPanelMode =
    primarySortKey === "realtime_change_pct" ||
    primarySortKey === "realtime_vol_ratio"
      ? primarySortKey
      : "speed_pct";
  const showHitPanel = sideListVisible;
  const topLimit = useMemo(
    () => parseNonNegativeIntegerInput(topLimitText, DEFAULT_TOP_LIMIT),
    [topLimitText],
  );

  useEffect(() => {
    void ensureManagedSourcePath()
      .then(setSourcePath)
      .catch(() => {});
  }, []);

  const updateTemplates = useCallback(
    (nextTemplates: IntradayMonitorTemplate[]) => {
      setTemplates(nextTemplates);
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
    setShowParams(false);
  }, [normalizeParamTexts]);

  // 浏览器缓存参数配置
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
    writeJsonStorage(
      typeof window === "undefined" ? null : window.localStorage,
      LS_KEY_SIDE_LIST_VISIBLE,
      sideListVisible,
    );
  }, [sideListVisible]);

  useEffect(() => {
    writeStoredIntradayMonitorWatchlistEnabled(watchlistEnabled);
  }, [watchlistEnabled]);

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

  // 点击浮窗外部区域时关闭浮窗（模板触发浮窗 / 涨速命中浮窗）
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
    enabledRef.current = enabled;
    if (!enabled) {
      setLoading(false);
    }
  }, [enabled]);

  const refreshSnapshot = useCallback(async () => {
    if (!sourcePathTrimmed || inFlightRef.current || !enabledRef.current) {
      return;
    }

    inFlightRef.current = true;
    setLoading(true);
    try {
      const result = await getAllMarketMonitorSnapshot(
        sourcePathTrimmed,
        readStoredRealtimeQuoteProvider(),
        sceneStageThreshold,
        templateEnabled,
        templateEnabled ? templates : undefined,
        watchlistEnabled ? watchlistCodes : undefined,
        otherSortExpression.trim() || undefined,
        true,
      );
      if (!enabledRef.current) return;

      const capturedAt = Date.now();
      const nextRows = result.rows ?? [];
      const nextIndexRows = result.index_rows ?? [];
      const nextVolumeRatioNewHighTimes = updateRecordHighs(
        volumeRatioRecordHighsRef.current,
        nextRows,
        capturedAt,
        (row) => row.realtime_vol_ratio,
      );
      const nextChangePctNewHighTimes = updateRecordHighs(
        changePctRecordHighsRef.current,
        nextRows,
        capturedAt,
        (row) => row.realtime_change_pct,
      );
      setVolumeRatioNewHighTimes(nextVolumeRatioNewHighTimes);
      setChangePctNewHighTimes(nextChangePctNewHighTimes);
      if (nextVolumeRatioNewHighTimes.size > 0) {
        setVolumeRatioRecordHighEventTimes((prev) => {
          const next = new Map(prev);
          nextVolumeRatioNewHighTimes.forEach((time, tsCode) => {
            next.set(tsCode, time);
          });
          return next;
        });
      }
      if (nextChangePctNewHighTimes.size > 0) {
        setChangePctRecordHighEventTimes((prev) => {
          const next = new Map(prev);
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
    } catch (runError) {
      if (enabledRef.current) {
        setError(`实时刷新失败: ${String(runError)}`);
      }
    } finally {
      inFlightRef.current = false;
      if (enabledRef.current) {
        setLoading(false);
      }
    }
  }, [
    sceneStageThreshold,
    otherSortExpression,
    sourcePathTrimmed,
    templateEnabled,
    templates,
    watchlistEnabled,
    watchlistCodes,
  ]);

  useEffect(() => {
    if (!enabled) return undefined;
    if (!sourcePathTrimmed) {
      setError("请先到“数据管理”页完成数据准备");
      setEnabled(false);
      return undefined;
    }

    enabledRef.current = true;
    void refreshSnapshot();
    const intervalId = window.setInterval(() => {
      if (!inFlightRef.current) {
        void refreshSnapshot();
      }
    }, POLL_INTERVAL_MS);

    return () => {
      window.clearInterval(intervalId);
    };
  }, [enabled, refreshSnapshot, sourcePathTrimmed]);

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
  }, [sourcePathTrimmed]);

  useEffect(() => {
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
    setHitRecordsByPeriod(() => createEmptyHitRecordsByPeriod());
    setOpenHitTsCode(null);
  }, [watchlistEnabled, watchlistCodes]);

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

  const displayRows = useMemo<DisplayRow[]>(() => {
    const filteredRows = rows
      .filter((row) => boardFilter === "全部" || row.board === boardFilter)
      .map((row) => ({
        ...row,
        speed_pct: speedMap.get(row.ts_code) ?? null,
      }));

    const sortDefinitions = {
      best_rank_3d: { value: (row: DisplayRow) => row.best_rank_3d },
      best_rank_5d: { value: (row: DisplayRow) => row.best_rank_5d },
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
    otherSortDirection,
    primarySortKey,
    rows,
    sortDirection,
    sortKey,
    speedMap,
    speedMinTicks,
    speedThresholdPct,
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

  const hitRecords = hitRecordsByPeriod[speedPeriod];
  const filteredHitRecords = useMemo(
    () =>
      hitRecords.filter(
        (record) => boardFilter === "全部" || record.board === boardFilter,
      ),
    [boardFilter, hitRecords],
  );
  const recordHighRows = useMemo<RecordHighDisplayRow[]>(() => {
    const eventTimes =
      hitPanelMode === "realtime_change_pct"
        ? changePctRecordHighEventTimes
        : hitPanelMode === "realtime_vol_ratio"
          ? volumeRatioRecordHighEventTimes
          : null;
    if (!eventTimes) {
      return [];
    }

    return rows
      .filter((row) => boardFilter === "全部" || row.board === boardFilter)
      .filter((row) => {
        if (hitPanelMode !== "realtime_vol_ratio") {
          return true;
        }
        return (
          isFiniteNumber(volumeRatioThreshold) &&
          isFiniteNumber(row.realtime_vol_ratio) &&
          row.realtime_vol_ratio > volumeRatioThreshold
        );
      })
      .map((row): RecordHighDisplayRow | null => {
        const recordHighAt = eventTimes.get(row.ts_code);
        return isFiniteNumber(recordHighAt)
          ? {
              ...row,
              speed_pct: speedMap.get(row.ts_code) ?? null,
              record_high_at: recordHighAt,
            }
          : null;
      })
      .filter((row): row is RecordHighDisplayRow => row !== null)
      .sort((left, right) => right.record_high_at - left.record_high_at)
      .slice(0, 200);
  }, [
    boardFilter,
    changePctRecordHighEventTimes,
    hitPanelMode,
    rows,
    speedMap,
    volumeRatioThreshold,
    volumeRatioRecordHighEventTimes,
  ]);
  const visibleRecordHighRows = useMemo<RecordHighDisplayRow[]>(() => {
    if (!sideListDebugRowsEnabled || recordHighRows.length > 0) {
      return recordHighRows;
    }
    return displayRows.slice(0, DEBUG_SIDE_LIST_LIMIT).map((row) => ({
      ...row,
      record_high_at: debugSideListTimestampRef.current,
    }));
  }, [displayRows, recordHighRows, sideListDebugRowsEnabled]);
  const visibleHitRecords = useMemo<SpeedHitRecord[]>(() => {
    if (!sideListDebugRowsEnabled || filteredHitRecords.length > 0) {
      return filteredHitRecords;
    }
    return displayRows.slice(0, DEBUG_SIDE_LIST_LIMIT).map((row) => ({
      ...row,
      hit_at: debugSideListTimestampRef.current,
      hit_speed_pct: row.speed_pct,
      hit_speed_threshold_pct: getEffectiveSpeedThresholdPct(
        row,
        speedThresholdPct,
        speedMinTicks,
      ),
    }));
  }, [
    displayRows,
    filteredHitRecords,
    sideListDebugRowsEnabled,
    speedMinTicks,
    speedThresholdPct,
  ]);
  const isDebugSideListFallback =
    sideListDebugRowsEnabled &&
    (hitPanelMode === "speed_pct"
      ? filteredHitRecords.length === 0 && visibleHitRecords.length > 0
      : recordHighRows.length === 0 && visibleRecordHighRows.length > 0);
  const hitPanelTitle =
    hitPanelMode === "realtime_change_pct"
      ? "涨幅新高"
      : hitPanelMode === "realtime_vol_ratio"
        ? "量比新高"
        : "涨速命中";
  const hitPanelCount =
    hitPanelMode === "speed_pct"
      ? visibleHitRecords.length
      : visibleRecordHighRows.length;
  const hitPanelSubtitleBase =
    hitPanelMode === "realtime_change_pct"
      ? "最新在前"
      : hitPanelMode === "realtime_vol_ratio"
        ? "最新在前"
        : isFiniteNumber(speedThresholdPct)
          ? `${speedPeriod}秒 >= max(${speedThresholdPct.toFixed(2)}%, ${
              isFiniteNumber(speedMinTicks) && speedMinTicks > 0
                ? `${speedMinTicks}档`
                : "低价校正关闭"
            })`
          : "阈值无效";
  const hitPanelSubtitle = isDebugSideListFallback
    ? `${hitPanelSubtitleBase} · 测试数据`
    : hitPanelSubtitleBase;

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
      isFiniteNumber(row.best_rank_5d)
        ? `5:${formatNumber(row.best_rank_5d, 0)}`
        : null,
    ].filter((item): item is string => item !== null);
    const rankHighlighted =
      isRankHighlight(row.best_rank_3d, rankHighlightThreshold) ||
      isRankHighlight(row.best_rank_5d, rankHighlightThreshold);
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
            title="3日优 / 5日优"
          >
            排{" "}
            {rankParts
              .map((item) => item.split(":")[1] ?? item)
              .join("/")}
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
      : "每秒刷新"
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
              {templateEnabled ? <span>模板 {templates.length}</span> : null}
              {watchlistEnabled ? (
                <span>名单模式 {watchlistCodes.length}只</span>
              ) : null}
            </div>
          </div>

          <div className="all-market-head-actions">
            <button
              type="button"
              className={
                templateEnabled
                  ? "all-market-toggle is-active"
                  : "all-market-toggle"
              }
              role="switch"
              aria-checked={templateEnabled}
              onClick={() => setTemplateEnabled((value) => !value)}
            >
              <span className="all-market-toggle-track" aria-hidden="true">
                <span className="all-market-toggle-thumb" />
              </span>
              <span className="all-market-toggle-text">
                {templateEnabled ? "模板判断中" : "模板已关闭"}
              </span>
            </button>

            <button
              type="button"
              className={
                watchlistEnabled
                  ? "all-market-toggle is-active"
                  : "all-market-toggle"
              }
              role="switch"
              aria-checked={watchlistEnabled}
              onClick={() => setWatchlistEnabled((value) => !value)}
            >
              <span className="all-market-toggle-track" aria-hidden="true">
                <span className="all-market-toggle-thumb" />
              </span>
              <span className="all-market-toggle-text">
                {watchlistEnabled ? "名单模式" : "未限名单"}
              </span>
            </button>

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

          <div className="all-market-index-strip" aria-label="指数表现">
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

          <div className="all-market-config-controls">
            <button
              type="button"
              className="all-market-params-btn"
              onClick={() => setWatchlistModalOpen(true)}
            >
              名单管理
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
              ⚙ 参数
            </button>
          </div>
        </div>

        {error ? <div className="all-market-error">{error}</div> : null}
        {templateWarning ? (
          <div className="all-market-warning">{templateWarning}</div>
        ) : null}
      </section>

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
            <h3>{isVolumeRatioBoard ? "量比榜" : "实时行情"}</h3>
            <div
              className="all-market-board-tabs"
              role="group"
              aria-label="板块"
            >
              {STOCK_PICK_BOARD_OPTIONS.map((board) => (
                <button
                  key={board}
                  type="button"
                  className={boardFilter === board ? "is-active" : ""}
                  onClick={() => setBoardFilter(board)}
                >
                  {board}
                </button>
              ))}
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
                        sortKey === "best_rank_5d",
                        sortDirection,
                      )}
                    >
                      {renderSortHeader("5日优", "best_rank_5d")}
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
                    const rank5dHighlighted = isRankHighlight(
                      row.best_rank_5d,
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
                              autoRealtime
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
                                rank5dHighlighted
                                  ? "all-market-compact-item is-highlight"
                                  : "all-market-compact-item"
                              }
                            >
                              5日 {formatNumber(row.best_rank_5d, 0)}
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
                            rank5dHighlighted
                              ? "all-market-rank-cell all-market-basic-col is-highlight"
                              : "all-market-rank-cell all-market-basic-col"
                          }
                        >
                          {formatNumber(row.best_rank_5d, 0)}
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
                              title="当前量比高于监控记录"
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
                {enabled ? "等待行情返回。" : "开启爬虫后开始刷新。"}
              </div>
            )}
          </div>
        </section>

        {showHitPanel ? (
          <section className="all-market-card all-market-hit-card">
            <div className="all-market-hit-head">
              <div>
                <h3>{hitPanelTitle}</h3>
                <span>{hitPanelSubtitle}</span>
              </div>
              <strong>{hitPanelCount}</strong>
            </div>

            <div className="all-market-hit-list">
              {hitPanelMode === "realtime_change_pct" ? (
                visibleRecordHighRows.length > 0 ? (
                  visibleRecordHighRows.map((record) => (
                    <div key={record.ts_code} className="all-market-hit-row">
                      <div className="all-market-hit-main">
                        <span className="all-market-hit-name">
                          <DetailsLink
                            tsCode={record.ts_code}
                            tradeDate={
                              rankDate ||
                              record.realtime_trade_date ||
                              undefined
                            }
                            sourcePath={sourcePathTrimmed || undefined}
                            autoRealtime
                            className="all-market-hit-name-link"
                            title={`查看 ${record.name || record.ts_code} 详情`}
                            navigationItems={navigationItems}
                          >
                            <strong>{record.name || "--"}</strong>
                            <small>{record.ts_code}</small>
                          </DetailsLink>
                        </span>
                        {renderHitMeta(record)}
                      </div>
                      <span
                        className={`all-market-hit-change ${getPercentClassName(
                          record.realtime_change_pct,
                        )}`}
                      >
                        {formatPercent(record.realtime_change_pct)}
                      </span>
                      <span className="all-market-hit-time">
                        {formatClockFromMs(record.record_high_at)}
                      </span>
                    </div>
                  ))
                ) : (
                  <div className="all-market-hit-empty">暂无涨幅新高。</div>
                )
              ) : hitPanelMode === "realtime_vol_ratio" ? (
                visibleRecordHighRows.length > 0 ? (
                  visibleRecordHighRows.map((record) => (
                    <div key={record.ts_code} className="all-market-hit-row">
                      <div className="all-market-hit-main">
                        <span className="all-market-hit-name">
                          <DetailsLink
                            tsCode={record.ts_code}
                            tradeDate={
                              rankDate ||
                              record.realtime_trade_date ||
                              undefined
                            }
                            sourcePath={sourcePathTrimmed || undefined}
                            autoRealtime
                            className="all-market-hit-name-link"
                            title={`查看 ${record.name || record.ts_code} 详情`}
                            navigationItems={navigationItems}
                          >
                            <strong>{record.name || "--"}</strong>
                            <small>{record.ts_code}</small>
                          </DetailsLink>
                        </span>
                        {renderHitMeta(record)}
                      </div>
                      <span className="all-market-hit-change">
                        {formatNumber(record.realtime_vol_ratio)}
                      </span>
                      <span className="all-market-hit-time">
                        {formatClockFromMs(record.record_high_at)}
                      </span>
                    </div>
                  ))
                ) : (
                  <div className="all-market-hit-empty">暂无量比新高。</div>
                )
              ) : visibleHitRecords.length > 0 ? (
                visibleHitRecords.map((record) => {
                  const isOpen = openHitTsCode === record.ts_code;
                  const toggleHitPopover = () => {
                    setOpenHitTsCode((value) =>
                      value === record.ts_code ? null : record.ts_code,
                    );
                  };
                  return (
                    <div
                      key={record.ts_code}
                      role="button"
                      tabIndex={0}
                      className={
                        isOpen
                          ? "all-market-hit-row is-open"
                          : "all-market-hit-row"
                      }
                      onClick={toggleHitPopover}
                      onKeyDown={(event) => {
                        if (event.key !== "Enter" && event.key !== " ") return;
                        event.preventDefault();
                        toggleHitPopover();
                      }}
                    >
                      <div className="all-market-hit-main">
                        <span
                          className="all-market-hit-name"
                          onClickCapture={(event) => event.stopPropagation()}
                        >
                          <DetailsLink
                            tsCode={record.ts_code}
                            tradeDate={
                              rankDate ||
                              record.realtime_trade_date ||
                              undefined
                            }
                            sourcePath={sourcePathTrimmed || undefined}
                            autoRealtime
                            className="all-market-hit-name-link"
                            title={`查看 ${record.name || record.ts_code} 详情`}
                            navigationItems={hitNavigationItems}
                          >
                            <strong>{record.name || "--"}</strong>
                            <small>{record.ts_code}</small>
                          </DetailsLink>
                        </span>
                        {renderHitMeta(record)}
                      </div>
                      <span
                        className={`all-market-hit-change ${getPercentClassName(
                          record.realtime_change_pct,
                        )}`}
                      >
                        {formatPercent(record.realtime_change_pct)}
                      </span>
                      <span className="all-market-hit-time">
                        {formatClockFromMs(record.hit_at)}
                      </span>
                    </div>
                  );
                })
              ) : (
                <div className="all-market-hit-empty">
                  {isFiniteNumber(speedThresholdPct)
                    ? "暂无涨速命中。"
                    : "设置有效涨速阈值后开始记录。"}
                </div>
              )}
            </div>

            {hitPanelMode === "speed_pct" && openHitRecord ? (
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
                    autoRealtime
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
                    <dt>5日优</dt>
                    <dd>{formatNumber(openHitRecord.best_rank_5d, 0)}</dd>
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
            className="settings-modal settings-modal-narrow"
            role="dialog"
            aria-modal="true"
            aria-label="实时监控参数"
          >
            <div className="settings-modal-head">
              <div>
                <h3 className="settings-subtitle-head">监控参数</h3>
                <p className="settings-section-note">
                  调整实时监控的涨速计算与展示参数。
                </p>
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

              <label className="settings-field">
                <span>量比阈值</span>
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
