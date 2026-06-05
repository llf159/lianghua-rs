import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import {
  intradayMonitorPage,
  refreshIntradayMonitorRealtime,
  refreshIntradayMonitorTemplateTags,
  type IntradayMonitorRankModeConfig as RankModeConfig,
  type IntradayMonitorRow,
  type IntradayMonitorTemplate as MarkTemplate,
} from "../../apis/reader";
import { getStrategyManagePage } from "../../apis/strategyManage";
import IntradayTemplateManagerModal from "./components/IntradayTemplateManagerModal";
import {
  formatConceptText,
  isStBoard,
  useConceptExclusions,
} from "../../shared/conceptExclusions";
import {
  STOCK_PICK_BOARD_OPTIONS,
  buildBoardFilterOptions,
} from "../../shared/stockPickShared";
import DetailsLink from "../../shared/DetailsLink";
import {
  TableSortButton,
  getAriaSort,
  type SortDefinition,
  type SortDirection,
  useTableSort,
} from "../../shared/tableSort";
import { readJsonStorage, writeJsonStorage } from "../../shared/storage";
import {
  DEFAULT_DATE_OPTION,
  normalizeTradeDates,
  pickDateValue,
} from "../../shared/tradeDate";
import "./css/IntradayMonitorRealtimePage.css";

const INTRADAY_MONITOR_PAGE_STATE_KEY = "lh_intraday_monitor_realtime_page_v2";
const INTRADAY_MONITOR_PARAMS_STORAGE_KEY =
  "lh_intraday_monitor_realtime_params_v1";
const INTRADAY_MONITOR_TEMPLATE_STORAGE_KEY =
  "lh_intraday_monitor_realtime_templates_v1";
const REFRESH_BATCH_SIZE = 500;
const REFRESH_SCOPE_ALL = "__all__";
const REFRESH_SCOPE_TOTAL = "__total__";
const CONTINUOUS_MONITOR_INTERVAL_MS = 1000;
const SPEED_HISTORY_KEEP_MS = 90_000;
const SPEED_PERIOD_OPTIONS = [10, 30, 60] as const;

const TOTAL_MODE_COLUMNS = [
  "rank",
  "ts_code",
  "name",
  "realtime_price",
  "realtime_change_pct",
  "speed_pct",
  "template_tag",
  "realtime_vol_ratio",
  "total_score",
  "board",
  "total_mv_yi",
  "concept",
] as const;

const SCENE_MODE_COLUMNS = [
  "scene_name",
  "rank",
  "ts_code",
  "name",
  "realtime_price",
  "realtime_change_pct",
  "speed_pct",
  "template_tag",
  "realtime_vol_ratio",
  "scene_score",
  "risk_score",
  "total_score",
  "scene_status",
  "board",
  "total_mv_yi",
  "concept",
] as const;

type TotalModeColumn = (typeof TOTAL_MODE_COLUMNS)[number];
type SceneModeColumn = (typeof SCENE_MODE_COLUMNS)[number];
type VisibleColumn = TotalModeColumn | SceneModeColumn;
type NumericVisibleColumn =
  | "rank"
  | "realtime_price"
  | "realtime_change_pct"
  | "speed_pct"
  | "realtime_vol_ratio"
  | "total_score"
  | "scene_score"
  | "risk_score"
  | "total_mv_yi";

type RankMode = RankModeConfig["mode"];
type RowDeltaMap = Record<string, Partial<Record<NumericVisibleColumn, number>>>;
type SpeedPeriod = (typeof SPEED_PERIOD_OPTIONS)[number];
type PriceSnapshot = {
  capturedAt: number;
  prices: Record<string, number>;
};

type PersistedIntradayMonitorState = {
  sourcePath: string;
  rankDateInput: string;
  limitInput: string;
  boardFilter: (typeof STOCK_PICK_BOARD_OPTIONS)[number];
  totalMvMinInput: string;
  totalMvMaxInput: string;
  templates: MarkTemplate[];
  rankModeConfigs: RankModeConfig[];
  rows: IntradayMonitorRow[];
  dateOptions: string[];
  sceneOptions: string[];
  refreshedAt: string;
  sortKey: string | null;
  sortDirection: SortDirection;
};

type PersistedIntradayMonitorParams = Pick<
  PersistedIntradayMonitorState,
  | "sourcePath"
  | "rankDateInput"
  | "limitInput"
  | "boardFilter"
  | "totalMvMinInput"
  | "totalMvMaxInput"
  | "rankModeConfigs"
  | "sortKey"
  | "sortDirection"
>;

type SceneRowsGroup = {
  key: string;
  title: string;
  rows: IntradayMonitorRow[];
};

type RefreshStage = "idle" | "preparing" | "refreshing" | "retagging";

const COLUMN_LABELS: Record<VisibleColumn, string> = {
  scene_name: "场景",
  rank: "排名",
  ts_code: "代码",
  name: "名称",
  realtime_price: "实时价*",
  realtime_change_pct: "实时涨幅*",
  speed_pct: "涨速*",
  template_tag: "模板标记",
  realtime_vol_ratio: "实时量比*",
  total_score: "总分",
  scene_score: "场景分",
  risk_score: "风险分",
  scene_status: "场景状态",
  board: "板块",
  total_mv_yi: "总市值(亿)",
  concept: "概念",
};

const COLUMN_WIDTHS: Record<VisibleColumn, number> = {
  scene_name: 128,
  rank: 72,
  ts_code: 112,
  name: 110,
  realtime_price: 96,
  realtime_change_pct: 108,
  speed_pct: 92,
  template_tag: 160,
  realtime_vol_ratio: 108,
  total_score: 96,
  scene_score: 98,
  risk_score: 98,
  scene_status: 104,
  board: 96,
  total_mv_yi: 116,
  concept: 260,
};

const DELTA_COLUMNS = new Set<VisibleColumn>([
  "realtime_price",
  "realtime_change_pct",
  "realtime_vol_ratio",
]);

function createRankModeConfig(
  mode: RankMode,
  sceneName = "全部",
  templateId = "",
): RankModeConfig {
  return {
    mode,
    sceneName,
    templateId,
  };
}

function getRankModeLabel(mode: RankMode) {
  return mode === "total" ? "总榜" : "Scene榜";
}

function summarizeExpression(expression: string, maxLength = 44) {
  const compact = expression.replace(/\s+/g, " ").trim();
  if (compact.length <= maxLength) return compact || "--";
  return `${compact.slice(0, maxLength)}...`;
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

function normalizeTemplate(input: unknown): MarkTemplate | null {
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

function normalizeRankModeConfig(input: unknown): RankModeConfig | null {
  if (!input || typeof input !== "object") return null;
  const item = input as Record<string, unknown>;
  const mode =
    item.mode === "scene" ? "scene" : item.mode === "total" ? "total" : null;
  if (!mode) return null;
  return {
    mode,
    sceneName: typeof item.sceneName === "string" ? item.sceneName : "全部",
    templateId: typeof item.templateId === "string" ? item.templateId : "",
  };
}

function readStoredIntradayMonitorParams(): PersistedIntradayMonitorParams {
  const parsed = readJsonStorage<Partial<PersistedIntradayMonitorParams>>(
    typeof window === "undefined" ? null : window.localStorage,
    INTRADAY_MONITOR_PARAMS_STORAGE_KEY,
  );
  const rankModeConfigs = Array.isArray(parsed?.rankModeConfigs)
    ? parsed.rankModeConfigs
        .map(normalizeRankModeConfig)
        .filter((item): item is RankModeConfig => item !== null)
    : [];

  return {
    sourcePath: typeof parsed?.sourcePath === "string" ? parsed.sourcePath : "",
    rankDateInput:
      typeof parsed?.rankDateInput === "string"
        ? parsed.rankDateInput
        : DEFAULT_DATE_OPTION,
    limitInput: typeof parsed?.limitInput === "string" ? parsed.limitInput : "100",
    boardFilter:
      parsed?.boardFilter && STOCK_PICK_BOARD_OPTIONS.includes(parsed.boardFilter)
        ? parsed.boardFilter
        : "全部",
    totalMvMinInput:
      typeof parsed?.totalMvMinInput === "string" ? parsed.totalMvMinInput : "",
    totalMvMaxInput:
      typeof parsed?.totalMvMaxInput === "string" ? parsed.totalMvMaxInput : "",
    rankModeConfigs,
    sortKey: typeof parsed?.sortKey === "string" ? parsed.sortKey : null,
    sortDirection:
      parsed?.sortDirection === "asc" || parsed?.sortDirection === "desc"
        ? parsed.sortDirection
        : null,
  };
}

function writeStoredIntradayMonitorParams(value: PersistedIntradayMonitorParams) {
  writeJsonStorage(
    typeof window === "undefined" ? null : window.localStorage,
    INTRADAY_MONITOR_PARAMS_STORAGE_KEY,
    value,
  );
}

function formatNumber(value?: number | null, digits = 2) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  return Number.isInteger(value) ? String(value) : value.toFixed(digits);
}

function formatPercent(value?: number | null) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
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

function formatRefreshTime(raw: string) {
  const value = raw.trim();
  if (!value) return "--";
  const withSeconds = value.match(/(\d{2}:\d{2}:\d{2})/);
  if (withSeconds) return withSeconds[1];
  const withMinutes = value.match(/(\d{2}:\d{2})/);
  return withMinutes ? withMinutes[1] : value;
}

function formatDeltaValue(key: VisibleColumn, value?: number) {
  if (value === undefined || !Number.isFinite(value)) return null;
  const sign = value > 0 ? "+" : "";
  if (key === "realtime_change_pct") {
    return `${sign}${value.toFixed(2)}%`;
  }
  return `${sign}${value.toFixed(2)}`;
}

function getPercentClassName(value?: number | null) {
  if (
    value === null ||
    value === undefined ||
    !Number.isFinite(value) ||
    value === 0
  ) {
    return "intraday-monitor-value-flat";
  }
  return value > 0
    ? "intraday-monitor-value-up"
    : "intraday-monitor-value-down";
}

function isSortableColumn(key: VisibleColumn) {
  return !["ts_code", "name", "concept", "template_tag"].includes(key);
}

function formatCell(
  key: VisibleColumn,
  row: IntradayMonitorRow,
  excludedConcepts: readonly string[],
) {
  if (key === "concept")
    return formatConceptText(row.concept ?? "", excludedConcepts);
  if (key === "rank") return formatNumber(row.rank, 0);
  if (key === "scene_score") return formatNumber(row.scene_score);
  if (key === "risk_score") return formatNumber(row.risk_score);
  if (key === "total_score") return formatNumber(row.total_score);
  if (key === "total_mv_yi") return formatNumber(row.total_mv_yi);
  if (key === "realtime_price") return formatNumber(row.realtime_price);
  if (key === "realtime_change_pct")
    return formatPercent(row.realtime_change_pct);
  if (key === "realtime_vol_ratio") return formatNumber(row.realtime_vol_ratio);

  const value = row[key];
  if (value === null || value === undefined || value === "") return "--";
  return String(value);
}

function getRowMode(row: IntradayMonitorRow): RankMode {
  return row.rank_mode === "scene" ? "scene" : "total";
}

function getRowKey(row: IntradayMonitorRow) {
  return `${getRowMode(row)}|${row.scene_name}|${row.ts_code}|${row.trade_date ?? ""}`;
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function getNumericCellValue(row: IntradayMonitorRow, key: VisibleColumn) {
  if (!DELTA_COLUMNS.has(key)) return null;
  const value = row[key];
  return isFiniteNumber(value) ? value : null;
}

function buildRowDeltaMap(
  previousRows: IntradayMonitorRow[],
  nextRows: IntradayMonitorRow[],
) {
  const previousMap = new Map(previousRows.map((row) => [getRowKey(row), row]));
  const deltas: RowDeltaMap = {};

  for (const row of nextRows) {
    const previous = previousMap.get(getRowKey(row));
    if (!previous) continue;

    const rowDeltas: Partial<Record<NumericVisibleColumn, number>> = {};
    for (const key of DELTA_COLUMNS) {
      const previousValue = getNumericCellValue(previous, key);
      const nextValue = getNumericCellValue(row, key);
      if (previousValue === null || nextValue === null) continue;

      const delta = nextValue - previousValue;
      if (Math.abs(delta) > Number.EPSILON) {
        rowDeltas[key as NumericVisibleColumn] = delta;
      }
    }

    if (Object.keys(rowDeltas).length > 0) {
      deltas[getRowKey(row)] = rowDeltas;
    }
  }

  return deltas;
}

function mergeRowDeltaMap(
  current: RowDeltaMap,
  refreshedRows: IntradayMonitorRow[],
  refreshedDeltas: RowDeltaMap,
) {
  const next = { ...current };
  for (const row of refreshedRows) {
    const key = getRowKey(row);
    if (refreshedDeltas[key]) {
      next[key] = refreshedDeltas[key];
    } else {
      delete next[key];
    }
  }
  return next;
}

function buildPriceSnapshot(rows: IntradayMonitorRow[], capturedAt: number) {
  const prices: Record<string, number> = {};
  for (const row of rows) {
    if (
      typeof row.realtime_price === "number" &&
      Number.isFinite(row.realtime_price) &&
      row.realtime_price > 0
    ) {
      prices[getRowKey(row)] = row.realtime_price;
    }
  }
  return { capturedAt, prices };
}

function appendPriceSnapshot(
  history: PriceSnapshot[],
  rows: IntradayMonitorRow[],
  capturedAt: number,
) {
  const cutoff = capturedAt - SPEED_HISTORY_KEEP_MS;
  return [
    ...history.filter((item) => item.capturedAt >= cutoff),
    buildPriceSnapshot(rows, capturedAt),
  ];
}

function buildSpeedMap(
  rows: IntradayMonitorRow[],
  history: PriceSnapshot[],
  periodSec: SpeedPeriod,
  now: number,
) {
  const target = now - periodSec * 1000;
  let baseline: PriceSnapshot | null = null;
  for (const snapshot of history) {
    if (snapshot.capturedAt <= target) {
      baseline = snapshot;
    } else {
      break;
    }
  }
  if (!baseline) return new Map<string, number>();

  const out = new Map<string, number>();
  for (const row of rows) {
    const currentPrice = row.realtime_price;
    const previousPrice = baseline.prices[getRowKey(row)];
    if (
      typeof currentPrice === "number" &&
      Number.isFinite(currentPrice) &&
      currentPrice > 0 &&
      typeof previousPrice === "number" &&
      Number.isFinite(previousPrice) &&
      previousPrice > 0
    ) {
      out.set(getRowKey(row), (currentPrice / previousPrice - 1) * 100);
    }
  }
  return out;
}

function waitForNextPaint() {
  if (typeof window === "undefined") {
    return Promise.resolve();
  }
  return new Promise<void>((resolve) => {
    window.requestAnimationFrame(() => {
      window.requestAnimationFrame(() => resolve());
    });
  });
}

function getWarningMessage(data: { warningMessage?: string | null }) {
  return data.warningMessage?.trim() || "";
}

function isTemplateHitTag(tag: { tone: string; text: string }) {
  return tag.tone === "up" && tag.text.includes("命中");
}

function mergeStatusMessages(messages: Array<string | null | undefined>) {
  const normalized = Array.from(
    new Set(
      messages
        .map((message) => message?.trim() ?? "")
        .filter((message) => message !== ""),
    ),
  );
  return normalized.join("；");
}

export default function IntradayMonitorRealtimePage() {
  const { excludedConcepts, excludeStBoard } = useConceptExclusions();
  const persistedParams = useMemo(() => readStoredIntradayMonitorParams(), []);

  const persistedState = useMemo(() => {
    const parsed = readJsonStorage<Partial<PersistedIntradayMonitorState>>(
      typeof window === "undefined" ? null : window.sessionStorage,
      INTRADAY_MONITOR_PAGE_STATE_KEY,
    );
    if (!parsed || typeof parsed !== "object") return null;

    const templates = Array.isArray(parsed.templates)
      ? parsed.templates
          .map(normalizeTemplate)
          .filter((item): item is MarkTemplate => item !== null)
      : [];

    const rankModeConfigs = Array.isArray(parsed.rankModeConfigs)
      ? parsed.rankModeConfigs
          .map(normalizeRankModeConfig)
          .filter((item): item is RankModeConfig => item !== null)
      : [];

    return {
      sourcePath:
        typeof parsed.sourcePath === "string" ? parsed.sourcePath : "",
      rankDateInput:
        typeof parsed.rankDateInput === "string"
          ? parsed.rankDateInput
          : DEFAULT_DATE_OPTION,
      limitInput:
        typeof parsed.limitInput === "string" ? parsed.limitInput : "100",
      boardFilter:
        parsed.boardFilter &&
        STOCK_PICK_BOARD_OPTIONS.includes(parsed.boardFilter)
          ? parsed.boardFilter
          : "全部",
      totalMvMinInput:
        typeof parsed.totalMvMinInput === "string"
          ? parsed.totalMvMinInput
          : "",
      totalMvMaxInput:
        typeof parsed.totalMvMaxInput === "string"
          ? parsed.totalMvMaxInput
          : "",
      templates,
      rankModeConfigs:
        rankModeConfigs.length > 0
          ? rankModeConfigs
          : [],
      rows: Array.isArray(parsed.rows) ? parsed.rows : [],
      dateOptions: Array.isArray(parsed.dateOptions) ? parsed.dateOptions : [],
      sceneOptions: Array.isArray(parsed.sceneOptions)
        ? parsed.sceneOptions
        : [],
      refreshedAt:
        typeof parsed.refreshedAt === "string" ? parsed.refreshedAt : "",
      sortKey: typeof parsed.sortKey === "string" ? parsed.sortKey : null,
      sortDirection:
        parsed.sortDirection === "asc" || parsed.sortDirection === "desc"
          ? parsed.sortDirection
          : null,
    } satisfies PersistedIntradayMonitorState;
  }, []);

  const cachedTemplates = useMemo(() => {
    const parsed = readJsonStorage<unknown>(
      typeof window === "undefined" ? null : window.localStorage,
      INTRADAY_MONITOR_TEMPLATE_STORAGE_KEY,
    );
    if (Array.isArray(parsed)) {
      return parsed
        .map(normalizeTemplate)
        .filter((item): item is MarkTemplate => item !== null);
    }
    return persistedState?.templates ?? [];
  }, [persistedState]);

  const [sourcePath, setSourcePath] = useState(
    () => persistedState?.sourcePath ?? persistedParams.sourcePath,
  );
  const [rankDateInput, setRankDateInput] = useState(
    () => persistedState?.rankDateInput ?? persistedParams.rankDateInput,
  );
  const [limitInput, setLimitInput] = useState(
    () => persistedState?.limitInput ?? persistedParams.limitInput,
  );
  const [boardFilter, setBoardFilter] = useState<
    (typeof STOCK_PICK_BOARD_OPTIONS)[number]
  >(() => persistedState?.boardFilter ?? persistedParams.boardFilter);
  const [totalMvMinInput, setTotalMvMinInput] = useState(
    () => persistedState?.totalMvMinInput ?? persistedParams.totalMvMinInput,
  );
  const [totalMvMaxInput, setTotalMvMaxInput] = useState(
    () => persistedState?.totalMvMaxInput ?? persistedParams.totalMvMaxInput,
  );
  const [templates, setTemplates] = useState<MarkTemplate[]>(
    () => cachedTemplates,
  );
  const [rankModeConfigs, setRankModeConfigs] = useState<RankModeConfig[]>(
    () => persistedState?.rankModeConfigs ?? persistedParams.rankModeConfigs,
  );
  const [rows, setRows] = useState<IntradayMonitorRow[]>(
    () => persistedState?.rows ?? [],
  );
  const [rowDeltas, setRowDeltas] = useState<RowDeltaMap>({});
  const [dateOptions, setDateOptions] = useState<string[]>(
    () => persistedState?.dateOptions ?? [],
  );
  const [sceneOptions, setSceneOptions] = useState<string[]>(
    () => persistedState?.sceneOptions ?? [],
  );
  const [refreshedAt, setRefreshedAt] = useState(
    () => persistedState?.refreshedAt ?? "",
  );

  const [loading, setLoading] = useState(false);
  const [loadingAction, setLoadingAction] = useState<
    "读取" | "刷新实时" | null
  >(null);
  const [refreshStage, setRefreshStage] = useState<RefreshStage>("idle");
  const [refreshingScope, setRefreshingScope] = useState<string | null>(null);
  const [dateOptionsLoading, setDateOptionsLoading] = useState(false);
  const [error, setError] = useState("");
  const [currentTime, setCurrentTime] = useState(() => new Date());

  const [templateModalOpen, setTemplateModalOpen] = useState(false);
  const [continuousMonitorEnabled, setContinuousMonitorEnabled] =
    useState(false);
  const [speedPeriod, setSpeedPeriod] = useState<SpeedPeriod>(10);
  const rowsRef = useRef<IntradayMonitorRow[]>([]);
  const autoRefreshRef = useRef<() => Promise<void>>(async () => {});
  const priceHistoryRef = useRef<PriceSnapshot[]>([]);
  const refreshRunIdRef = useRef(0);
  const boardOptions = useMemo(
    () => buildBoardFilterOptions(STOCK_PICK_BOARD_OPTIONS, excludeStBoard),
    [excludeStBoard],
  );

  const sourcePathTrimmed = sourcePath.trim();

  const totalModeRows = useMemo(
    () => rows.filter((row) => getRowMode(row) === "total"),
    [rows],
  );

  const speedMap = useMemo(
    () => buildSpeedMap(rows, priceHistoryRef.current, speedPeriod, Date.now()),
    [rows, speedPeriod],
  );

  const sortDefinitions = useMemo(
    () =>
      Object.fromEntries(
        Array.from(new Set([...TOTAL_MODE_COLUMNS, ...SCENE_MODE_COLUMNS]))
          .filter((key) => isSortableColumn(key))
          .map((key) => [
            key,
            {
              value: (row: IntradayMonitorRow) =>
                key === "speed_pct" ? speedMap.get(getRowKey(row)) : row[key],
            } satisfies SortDefinition<IntradayMonitorRow>,
          ]),
      ) as Partial<Record<VisibleColumn, SortDefinition<IntradayMonitorRow>>>,
    [speedMap],
  );

  const { sortKey, sortDirection, sortedRows, toggleSort } = useTableSort(
    rows,
    sortDefinitions,
    {
      key:
        (persistedState?.sortKey as VisibleColumn | null) ??
        (persistedParams.sortKey as VisibleColumn | null),
      direction: persistedState?.sortDirection ?? persistedParams.sortDirection,
    },
  );

  const hasTotalConfig = useMemo(
    () => rankModeConfigs.some((item) => item.mode === "total"),
    [rankModeConfigs],
  );
  const isRefreshingAll =
    loading &&
    loadingAction === "刷新实时" &&
    refreshingScope === REFRESH_SCOPE_ALL;
  const isRefreshingTotal =
    loading &&
    loadingAction === "刷新实时" &&
    refreshingScope === REFRESH_SCOPE_TOTAL;

  function isRefreshingScene(sceneKey: string) {
    return (
      loading &&
      loadingAction === "刷新实时" &&
      refreshingScope === `scene:${sceneKey}`
    );
  }

  useEffect(() => {
    rowsRef.current = rows;
  }, [rows]);

  useEffect(() => {
    void ensureManagedSourcePath()
      .then(setSourcePath)
      .catch(() => {});
  }, []);

  useEffect(() => {
    const intervalId = window.setInterval(() => {
      setCurrentTime(new Date());
    }, 1000);
    return () => {
      window.clearInterval(intervalId);
    };
  }, []);

  useEffect(() => {
    if (excludeStBoard && isStBoard(boardFilter)) {
      setBoardFilter("全部");
    }
  }, [boardFilter, excludeStBoard]);

  const updateTemplates = useCallback((nextTemplates: MarkTemplate[]) => {
    setTemplates(nextTemplates);
    writeJsonStorage(
      typeof window === "undefined" ? null : window.localStorage,
      INTRADAY_MONITOR_TEMPLATE_STORAGE_KEY,
      nextTemplates,
    );
  }, []);

  useEffect(() => {
    writeJsonStorage(
      typeof window === "undefined" ? null : window.sessionStorage,
      INTRADAY_MONITOR_PAGE_STATE_KEY,
      {
        sourcePath,
        rankDateInput,
        limitInput,
        boardFilter,
        totalMvMinInput,
        totalMvMaxInput,
        templates,
        rankModeConfigs,
        rows,
        dateOptions,
        sceneOptions,
        refreshedAt,
        sortKey,
        sortDirection,
      } satisfies PersistedIntradayMonitorState,
    );
    writeStoredIntradayMonitorParams({
      sourcePath,
      rankDateInput,
      limitInput,
      boardFilter,
      totalMvMinInput,
      totalMvMaxInput,
      rankModeConfigs,
      sortKey,
      sortDirection,
    });
  }, [
    sourcePath,
    rankDateInput,
    limitInput,
    boardFilter,
    totalMvMinInput,
    totalMvMaxInput,
    templates,
    rankModeConfigs,
    rows,
    dateOptions,
    sceneOptions,
    refreshedAt,
    sortKey,
    sortDirection,
  ]);

  useEffect(() => {
    setRows((current) =>
      current.map((row) => ({
        ...row,
        template_tag_text: undefined,
        template_tag_tone: undefined,
      })),
    );
  }, [templates, rankModeConfigs]);

  useEffect(() => {
    if (!sourcePathTrimmed) {
      setDateOptions([]);
      setSceneOptions([]);
      setRankDateInput(DEFAULT_DATE_OPTION);
      return;
    }

    let cancelled = false;
    const loadFilters = async () => {
      setDateOptionsLoading(true);
      try {
        const [totalResult, sceneResult, strategyResult] = await Promise.allSettled([
          intradayMonitorPage({
            sourcePath: sourcePathTrimmed,
            rankMode: "total",
            rankDate: DEFAULT_DATE_OPTION,
            limit: 1,
          }),
          intradayMonitorPage({
            sourcePath: sourcePathTrimmed,
            rankMode: "scene",
            rankDate: DEFAULT_DATE_OPTION,
            limit: 1,
          }),
          getStrategyManagePage(sourcePathTrimmed),
        ]);
        if (cancelled) return;

        const totalData =
          totalResult.status === "fulfilled" ? totalResult.value : null;
        const sceneData =
          sceneResult.status === "fulfilled" ? sceneResult.value : null;

        if (!totalData && !sceneData) {
          throw new Error("总榜与Scene榜筛选项都读取失败");
        }

        const mergedDateOptions = normalizeTradeDates([
          ...(totalData?.rankDateOptions ?? []),
          ...(sceneData?.rankDateOptions ?? []),
        ]);
        setDateOptions(mergedDateOptions);
        setRankDateInput((current) =>
          pickDateValue(current, mergedDateOptions),
        );

        const strategySceneOptions =
          strategyResult.status === "fulfilled"
            ? (strategyResult.value.scenes ?? [])
                .map((item) => item.name.trim())
                .filter((item) => item !== "")
            : [];

        const nextSceneOptions = Array.from(
          new Set(
            [
              ...(sceneData?.sceneOptions ?? []),
              ...strategySceneOptions,
            ]
              .map((item) => item.trim())
              .filter((item) => item !== ""),
          ),
        );
        setSceneOptions(nextSceneOptions);

        setRankModeConfigs((current) =>
          current.map((config) => {
            if (config.mode !== "scene") return config;
            return {
              ...config,
              sceneName:
                config.sceneName === "全部" ||
                nextSceneOptions.includes(config.sceneName)
                  ? config.sceneName
                  : "全部",
            };
          }),
        );
      } catch (loadError) {
        if (!cancelled)
          setError(`读取盘中监控筛选项失败: ${String(loadError)}`);
      } finally {
        if (!cancelled) setDateOptionsLoading(false);
      }
    };

    void loadFilters();
    return () => {
      cancelled = true;
    };
  }, [sourcePathTrimmed]);

  const templateMap = useMemo(
    () => new Map(templates.map((item) => [item.id, item])),
    [templates],
  );

  const getAppliedTemplate = useCallback((row: IntradayMonitorRow) => {
    const mode = getRowMode(row);
    if (mode === "total") {
      const totalConfig = rankModeConfigs.find(
        (item) => item.mode === "total" && item.templateId,
      );
      return totalConfig
        ? (templateMap.get(totalConfig.templateId) ?? null)
        : null;
    }

    const exact = rankModeConfigs.find(
      (item) =>
        item.mode === "scene" &&
        item.sceneName === row.scene_name &&
        item.templateId,
    );
    if (exact && templateMap.has(exact.templateId))
      return templateMap.get(exact.templateId) ?? null;

    const allScene = rankModeConfigs.find(
      (item) =>
        item.mode === "scene" && item.sceneName === "全部" && item.templateId,
    );
    if (allScene && templateMap.has(allScene.templateId))
      return templateMap.get(allScene.templateId) ?? null;

    return null;
  }, [rankModeConfigs, templateMap]);

  const getTemplateTag = useCallback((row: IntradayMonitorRow) => {
    const tpl = getAppliedTemplate(row);
    if (!tpl) return { text: "未配置", tone: "neutral" as const };

    if (
      typeof row.template_tag_text === "string" &&
      row.template_tag_text.trim() !== ""
    ) {
      const tone =
        row.template_tag_tone === "up" || row.template_tag_tone === "down"
          ? row.template_tag_tone
          : "neutral";
      return {
        text: row.template_tag_text,
        tone,
      };
    }

    const hasRealtime =
      (typeof row.realtime_price === "number" &&
        Number.isFinite(row.realtime_price)) ||
      (typeof row.realtime_change_pct === "number" &&
        Number.isFinite(row.realtime_change_pct));
    if (!hasRealtime) {
      return {
        text: `${tpl.name} · 待刷新实时`,
        tone: "neutral" as const,
      };
    }

    return {
      text: `${tpl.name} · 待计算`,
      tone: "neutral" as const,
    };
  }, [getAppliedTemplate]);

  const prioritizeTemplateHits = useCallback((displayedRows: IntradayMonitorRow[]) => {
    const hitRows: IntradayMonitorRow[] = [];
    const otherRows: IntradayMonitorRow[] = [];
    for (const row of displayedRows) {
      if (isTemplateHitTag(getTemplateTag(row))) {
        hitRows.push(row);
      } else {
        otherRows.push(row);
      }
    }
    return [...hitRows, ...otherRows];
  }, [getTemplateTag]);

  const sortedTotalRows = useMemo(
    () => prioritizeTemplateHits(
      sortedRows.filter((row) => getRowMode(row) === "total"),
    ),
    [prioritizeTemplateHits, sortedRows],
  );

  const groupedSceneRows = useMemo<SceneRowsGroup[]>(() => {
    const groups = new Map<string, IntradayMonitorRow[]>();
    for (const row of sortedRows) {
      if (getRowMode(row) !== "scene") continue;
      const key = row.scene_name || "未命名场景";
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key)?.push(row);
    }
    return Array.from(groups.entries()).map(([sceneName, rowsInScene]) => ({
      key: sceneName,
      title: sceneName,
      rows: prioritizeTemplateHits(rowsInScene),
    }));
  }, [prioritizeTemplateHits, sortedRows]);

  function addRankModeConfig(mode: RankMode) {
    if (mode === "total") {
      if (hasTotalConfig) return;
      setRankModeConfigs((current) => [
        ...current,
        createRankModeConfig("total", "全部", templates[0]?.id ?? ""),
      ]);
      return;
    }
    setRankModeConfigs((current) => [
      ...current,
      createRankModeConfig("scene", "全部", templates[0]?.id ?? ""),
    ]);
  }

  function removeRankModeConfig(index: number) {
    setRankModeConfigs((current) => current.filter((_, idx) => idx !== index));
  }

  async function onRead(actionLabel: "读取" | "刷新实时") {
    if (!sourcePathTrimmed) {
      setError("请先到“数据管理”页完成数据准备");
      return;
    }

    const limitRaw = limitInput.trim();
    const limit = limitRaw ? Number(limitRaw) : undefined;
    if (limitRaw && (!Number.isInteger(limit) || (limit ?? 0) <= 0)) {
      setError("限制行数必须是正整数");
      return;
    }

    const totalMvMin = totalMvMinInput.trim()
      ? Number(totalMvMinInput.trim())
      : undefined;
    const totalMvMax = totalMvMaxInput.trim()
      ? Number(totalMvMaxInput.trim())
      : undefined;
    if (
      (totalMvMinInput.trim() && !Number.isFinite(totalMvMin)) ||
      (totalMvMaxInput.trim() && !Number.isFinite(totalMvMax))
    ) {
      setError("总市值输入必须是数字");
      return;
    }
    if (
      totalMvMin !== undefined &&
      totalMvMax !== undefined &&
      totalMvMin > totalMvMax
    ) {
      setError("总市值最小值不能大于最大值");
      return;
    }

    const runId = ++refreshRunIdRef.current;
    const isRealtimeRefresh = actionLabel === "刷新实时";

    setLoading(true);
    setLoadingAction(actionLabel);
    setRefreshingScope(isRealtimeRefresh ? REFRESH_SCOPE_ALL : null);
    setRefreshStage(isRealtimeRefresh ? "preparing" : "idle");
    setError("");

    const requestRankDate =
      actionLabel === "读取" ? rankDateInput : DEFAULT_DATE_OPTION;

    try {
      await waitForNextPaint();
      if (actionLabel === "读取") {
        const normalizedConfigs = rankModeConfigs;
        if (normalizedConfigs.length === 0) {
          setError("请先添加至少一个榜单区块后再读取");
          return;
        }

        const requests = normalizedConfigs.map((config) =>
          intradayMonitorPage({
            sourcePath: sourcePathTrimmed,
            rankMode: config.mode,
            rankDate: requestRankDate,
            sceneName:
              config.mode === "scene" && config.sceneName !== "全部"
                ? config.sceneName
                : undefined,
            limit,
            board: boardFilter === "全部" ? undefined : boardFilter,
            excludeStBoard: excludeStBoard || undefined,
            totalMvMin,
            totalMvMax,
          }),
        );

        const settledResults = await Promise.allSettled(requests);
        const successResults = settledResults
          .filter((item) => item.status === "fulfilled")
          .map((item) => item.value);

        if (successResults.length === 0) {
          const firstReason = settledResults.find(
            (item) => item.status === "rejected",
          );
          throw firstReason?.status === "rejected"
            ? firstReason.reason
            : new Error("读取失败");
        }

        const failedCount = settledResults.length - successResults.length;
        const partialFailureMessage =
          failedCount > 0
            ? `部分榜单读取失败：${failedCount} 个区块未返回，已展示其余数据`
            : "";

        const mergedDateOptions = normalizeTradeDates(
          successResults.flatMap((item) => item.rankDateOptions ?? []),
        );
        const resolvedDate =
          successResults.map((item) => item.resolvedRankDate ?? "").find(Boolean) ??
          rankDateInput;
        if (mergedDateOptions.length > 0) {
          setDateOptions(mergedDateOptions);
          setRankDateInput(pickDateValue(resolvedDate, mergedDateOptions));
        }

        const mergedSceneOptions = Array.from(
          new Set(
            successResults
              .flatMap((item) => item.sceneOptions ?? [])
              .map((item) => item.trim())
              .filter((item) => item !== ""),
          ),
        );
        if (mergedSceneOptions.length > 0) {
          setSceneOptions(mergedSceneOptions);
        }

        const rowMap = new Map<string, IntradayMonitorRow>();
        for (const result of successResults) {
          for (const row of result.rows ?? []) {
            rowMap.set(getRowKey(row), row);
          }
        }
        if (runId !== refreshRunIdRef.current) return;

        const warningMessage = mergeStatusMessages(
          successResults.map((item) => getWarningMessage(item)),
        );
        setRows(Array.from(rowMap.values()));
        setRowDeltas({});
        setRefreshedAt(
          successResults.map((item) => item.refreshedAt ?? "").find(Boolean) ??
            "",
        );
        setError(mergeStatusMessages([partialFailureMessage, warningMessage]));
      } else {
        setRefreshStage("refreshing");
        await waitForNextPaint();
        const refreshedRows: IntradayMonitorRow[] = [];
        let refreshed = "";
        let warningMessage = "";
        const rowsToRefresh = rowsRef.current;
        for (
          let start = 0;
          start < rowsToRefresh.length;
          start += REFRESH_BATCH_SIZE
        ) {
          const data = await refreshIntradayMonitorRealtime({
            sourcePath: sourcePathTrimmed,
            rows: rowsToRefresh.slice(start, start + REFRESH_BATCH_SIZE),
            templates,
            rankModeConfigs,
          });
          if (runId !== refreshRunIdRef.current) return;
          refreshedRows.push(...(data.rows ?? []));
          if (!refreshed) refreshed = data.refreshedAt ?? "";
          warningMessage = mergeStatusMessages([
            warningMessage,
            getWarningMessage(data),
          ]);
        }
        if (runId !== refreshRunIdRef.current) return;

        setRowDeltas(buildRowDeltaMap(rowsToRefresh, refreshedRows));
        setRows(refreshedRows);
        priceHistoryRef.current = appendPriceSnapshot(
          priceHistoryRef.current,
          refreshedRows,
          Date.now(),
        );
        setRefreshedAt(refreshed || formatClock(new Date()));
        setError(warningMessage);
      }
    } catch (readError) {
      if (runId !== refreshRunIdRef.current) return;
      setError(
        `${isRealtimeRefresh ? "刷新" : "读取"}失败: ${String(readError)}`,
      );
      if (!isRealtimeRefresh) {
        setRows([]);
        setRowDeltas({});
        setRefreshedAt("");
      }
    } finally {
      if (runId === refreshRunIdRef.current) {
        setLoading(false);
        setLoadingAction(null);
        setRefreshStage("idle");
        setRefreshingScope(null);
      }
    }
  }

  autoRefreshRef.current = () => {
    return onRead("刷新实时");
  };

  useEffect(() => {
    if (!continuousMonitorEnabled) return undefined;
    let cancelled = false;
    let timerId: number | null = null;

    const runLoop = async () => {
      if (cancelled) return;
      if (rowsRef.current.length > 0 && sourcePathTrimmed !== "") {
        await autoRefreshRef.current();
      }
      if (!cancelled) {
        timerId = window.setTimeout(runLoop, CONTINUOUS_MONITOR_INTERVAL_MS);
      }
    };

    timerId = window.setTimeout(runLoop, 0);

    return () => {
      cancelled = true;
      if (timerId !== null) {
        window.clearTimeout(timerId);
      }
    };
  }, [continuousMonitorEnabled, sourcePathTrimmed]);

  async function refreshRowsByGroup(groupKey: string) {
    if (!sourcePathTrimmed) return;

    setLoading(true);
    setLoadingAction("刷新实时");
    setRefreshingScope(
      groupKey === "total" ? REFRESH_SCOPE_TOTAL : `scene:${groupKey}`,
    );
    setRefreshStage("preparing");
    setError("");
    try {
      await waitForNextPaint();
      const targetRows =
        groupKey === "total"
          ? rows.filter((row) => getRowMode(row) === "total")
          : rows.filter(
              (row) =>
                getRowMode(row) === "scene" && row.scene_name === groupKey,
            );
      if (targetRows.length === 0) return;

      setRefreshStage("refreshing");
      await waitForNextPaint();
      const refreshedRows: IntradayMonitorRow[] = [];
      let refreshed = "";
      let warningMessage = "";
      for (
        let start = 0;
        start < targetRows.length;
        start += REFRESH_BATCH_SIZE
      ) {
        const data = await refreshIntradayMonitorRealtime({
          sourcePath: sourcePathTrimmed,
          rows: targetRows.slice(start, start + REFRESH_BATCH_SIZE),
          templates,
          rankModeConfigs,
        });
        refreshedRows.push(...(data.rows ?? []));
        if (!refreshed) refreshed = data.refreshedAt ?? "";
        warningMessage = mergeStatusMessages([
          warningMessage,
          getWarningMessage(data),
        ]);
      }

      const refreshedMap = new Map(
        refreshedRows.map((item) => [getRowKey(item), item]),
      );
      const refreshedDeltas = buildRowDeltaMap(targetRows, refreshedRows);
      setRows((currentRows) => {
        const nextRows = currentRows.map((item) => {
          const key = getRowKey(item);
          return refreshedMap.get(key) ?? item;
        });
        priceHistoryRef.current = appendPriceSnapshot(
          priceHistoryRef.current,
          nextRows,
          Date.now(),
        );
        return nextRows;
      });
      setRowDeltas((current) =>
        mergeRowDeltaMap(current, refreshedRows, refreshedDeltas),
      );
      setRefreshedAt(refreshed || formatClock(new Date()));
      setError(warningMessage);
    } catch (refreshError) {
      setError(`刷新失败: ${String(refreshError)}`);
    } finally {
      setLoading(false);
      setLoadingAction(null);
      setRefreshStage("idle");
      setRefreshingScope(null);
    }
  }

  async function refreshTemplateTagsByGroup(groupKey: string) {
    if (!sourcePathTrimmed) return;

    setLoading(true);
    setLoadingAction("刷新实时");
    setRefreshingScope(
      groupKey === "total" ? REFRESH_SCOPE_TOTAL : `scene:${groupKey}`,
    );
    setRefreshStage("preparing");
    setError("");
    try {
      await waitForNextPaint();
      const targetRows =
        groupKey === "total"
          ? rows.filter((row) => getRowMode(row) === "total")
          : rows.filter(
              (row) =>
                getRowMode(row) === "scene" && row.scene_name === groupKey,
            );
      if (targetRows.length === 0) return;

      setRefreshStage("retagging");
      await waitForNextPaint();
      const data = await refreshIntradayMonitorTemplateTags({
        sourcePath: sourcePathTrimmed,
        rows: targetRows,
        templates,
        rankModeConfigs,
      });

      const refreshedMap = new Map(
        (data.rows ?? []).map((item) => [getRowKey(item), item]),
      );
      setRows((currentRows) =>
        currentRows.map((item) => {
          const key = getRowKey(item);
          return refreshedMap.get(key) ?? item;
        }),
      );
      setError(getWarningMessage(data));
    } catch (refreshError) {
      setError(`仅刷新标记失败: ${String(refreshError)}`);
    } finally {
      setLoading(false);
      setLoadingAction(null);
      setRefreshStage("idle");
      setRefreshingScope(null);
    }
  }

  function onTemplateRemoved(templateId: string) {
    setRankModeConfigs((current) =>
      current.map((item) =>
        item.templateId === templateId ? { ...item, templateId: "" } : item,
      ),
    );
  }

  function renderTable(
    displayedRows: IntradayMonitorRow[],
    columns: readonly VisibleColumn[],
  ) {
    const tableMinWidth = columns.reduce(
      (total, key) => total + COLUMN_WIDTHS[key],
      0,
    );
    const navigationItems = displayedRows.map((row) => ({
      tsCode: row.ts_code,
      tradeDate: typeof row.trade_date === "string" ? row.trade_date : null,
      sourcePath: sourcePathTrimmed || undefined,
      name: typeof row.name === "string" ? row.name : undefined,
    }));

    return (
      <div className="intraday-monitor-table-wrap">
        <table
          className="intraday-monitor-table"
          style={{ minWidth: `${tableMinWidth}px` }}
        >
          <colgroup>
            {columns.map((key) => (
              <col key={key} style={{ width: `${COLUMN_WIDTHS[key]}px` }} />
            ))}
          </colgroup>
          <thead>
            <tr>
              {columns.map((key) => {
                if (!isSortableColumn(key))
                  return <th key={key}>{COLUMN_LABELS[key]}</th>;
                const isActive = sortKey === key && sortDirection !== null;
                return (
                  <th
                    key={key}
                    aria-sort={getAriaSort(isActive, sortDirection)}
                  >
                    <TableSortButton
                      label={COLUMN_LABELS[key]}
                      isActive={isActive}
                      direction={sortDirection}
                      onClick={() => toggleSort(key)}
                      title={`按${COLUMN_LABELS[key]}排序`}
                    />
                  </th>
                );
              })}
            </tr>
          </thead>
          <tbody>
            {displayedRows.map((row, index) => (
              <tr
                key={`${getRowMode(row)}-${row.scene_name}-${row.ts_code}-${row.trade_date ?? index}`}
              >
                {columns.map((key) => {
                  if (key === "template_tag") {
                    const tag = getTemplateTag(row);
                    return (
                      <td key={`${getRowMode(row)}-${row.ts_code}-${key}`}>
                        <span
                          className={`intraday-monitor-hit-badge intraday-monitor-hit-badge-${tag.tone}`}
                        >
                          {tag.text}
                        </span>
                      </td>
                    );
                  }

                  const speedValue =
                    key === "speed_pct"
                      ? speedMap.get(getRowKey(row))
                      : undefined;
                  const displayText =
                    key === "speed_pct"
                      ? formatPercent(speedValue)
                      : formatCell(key, row, excludedConcepts);
                  const isRealtimePct =
                    key === "realtime_change_pct" || key === "speed_pct";
                  const deltaValue = DELTA_COLUMNS.has(key)
                    ? rowDeltas[getRowKey(row)]?.[
                        key as NumericVisibleColumn
                      ]
                    : undefined;
                  const deltaText = DELTA_COLUMNS.has(key)
                    ? formatDeltaValue(key, deltaValue)
                    : null;
                  return (
                    <td
                      key={`${getRowMode(row)}-${row.ts_code}-${key}`}
                      className={
                        isRealtimePct
                          ? getPercentClassName(
                              key === "speed_pct"
                                ? speedValue
                                : row.realtime_change_pct,
                            )
                          : undefined
                      }
                    >
                      {key === "name" && displayText !== "--" ? (
                        <DetailsLink
                          className="intraday-monitor-stock-link"
                          tsCode={row.ts_code}
                          tradeDate={
                            typeof row.trade_date === "string"
                              ? row.trade_date
                              : null
                          }
                          sourcePath={sourcePathTrimmed}
                          title={`查看 ${displayText} 详情`}
                          navigationItems={navigationItems}
                        >
                          {displayText}
                        </DetailsLink>
                      ) : (
                        <span className="intraday-monitor-cell-value">
                          <span>{displayText}</span>
                          {deltaText ? (
                            <span
                              className={[
                                "intraday-monitor-delta",
                                (deltaValue ?? 0) > 0
                                  ? "intraday-monitor-delta-up"
                                  : "intraday-monitor-delta-down",
                              ].join(" ")}
                            >
                              {deltaText}
                            </span>
                          ) : null}
                        </span>
                      )}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  }

  const rankModeConfigItems = rankModeConfigs.map((config, index) => ({
    index,
    key: `${config.mode}-${index}`,
    mode: config.mode,
    sceneName: config.sceneName,
    templateId: config.templateId,
    template: templateMap.get(config.templateId),
    canDelete: true,
  }));
  return (
    <div className="intraday-monitor-page">
      <section className="intraday-monitor-card">
        <h2 className="intraday-monitor-title">实时监控（总榜 + Scene榜）</h2>

        <div className="intraday-monitor-form-grid">
          <label className="intraday-monitor-field">
            <span>排名日期</span>
            <select
              value={rankDateInput}
              onChange={(event) => setRankDateInput(event.target.value)}
              disabled={dateOptionsLoading}
            >
              {dateOptions.length === 0 ? (
                <option value={DEFAULT_DATE_OPTION}>
                  {dateOptionsLoading ? "加载日期中..." : "最新"}
                </option>
              ) : (
                dateOptions.map((tradeDate) => (
                  <option key={tradeDate} value={tradeDate}>
                    {tradeDate}
                  </option>
                ))
              )}
            </select>
          </label>

          <label className="intraday-monitor-field">
            <span>限制行数</span>
            <input
              type="number"
              min={1}
              step={1}
              value={limitInput}
              onChange={(event) => setLimitInput(event.target.value)}
              placeholder="100"
            />
          </label>

          <label className="intraday-monitor-field">
            <span>板块筛选</span>
            <select
              value={boardFilter}
              onChange={(event) =>
                setBoardFilter(
                  event.target
                    .value as (typeof STOCK_PICK_BOARD_OPTIONS)[number],
                )
              }
            >
              {boardOptions.map((board) => (
                <option key={board} value={board}>
                  {board}
                </option>
              ))}
            </select>
          </label>

          <label className="intraday-monitor-field">
            <span>总市值最小(亿)</span>
            <input
              type="number"
              step={0.01}
              value={totalMvMinInput}
              onChange={(event) => setTotalMvMinInput(event.target.value)}
              placeholder="留空=不限"
            />
          </label>

          <label className="intraday-monitor-field">
            <span>总市值最大(亿)</span>
            <input
              type="number"
              step={0.01}
              value={totalMvMaxInput}
              onChange={(event) => setTotalMvMaxInput(event.target.value)}
              placeholder="留空=不限"
            />
          </label>
        </div>

        <div className="intraday-monitor-actions">
          <button
            className="intraday-monitor-refresh-btn"
            type="button"
            onClick={() => setTemplateModalOpen(true)}
            disabled={loading || dateOptionsLoading}
          >
            模板管理
          </button>
          <button
            className="intraday-monitor-refresh-btn"
            type="button"
            onClick={() => addRankModeConfig("total")}
            disabled={loading || dateOptionsLoading || hasTotalConfig}
          >
            添加总榜区块
          </button>
          <button
            className="intraday-monitor-refresh-btn"
            type="button"
            onClick={() => addRankModeConfig("scene")}
            disabled={loading || dateOptionsLoading}
          >
            添加Scene榜区块
          </button>
          <button
            className="intraday-monitor-read-btn"
            type="button"
            onClick={() => void onRead("读取")}
            disabled={loading || dateOptionsLoading || sourcePathTrimmed === ""}
          >
            {loading && loadingAction === "读取" ? "读取中..." : "读取"}
          </button>
          <button
            className="intraday-monitor-refresh-btn"
            type="button"
            onClick={() => void onRead("刷新实时")}
            disabled={
              dateOptionsLoading ||
              sourcePathTrimmed === "" ||
              rows.length === 0
            }
          >
            {isRefreshingAll
              ? refreshStage === "preparing"
                ? "准备中..."
                : "刷新中..."
              : "全部刷新实时"}
          </button>
          <button
            className={
              continuousMonitorEnabled
                ? "intraday-monitor-auto-toggle is-active"
                : "intraday-monitor-auto-toggle"
            }
            type="button"
            role="switch"
            aria-checked={continuousMonitorEnabled}
            onClick={() => setContinuousMonitorEnabled((value) => !value)}
            disabled={dateOptionsLoading || sourcePathTrimmed === ""}
          >
            {continuousMonitorEnabled ? "持续监控中" : "持续监控"}
          </button>
          <label className="intraday-monitor-inline-field">
            <span>涨速</span>
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
        </div>

        <div className="intraday-monitor-config-list">
          {rankModeConfigItems.map((item) => (
            <section key={item.key} className="intraday-monitor-config-card">
              <div className="intraday-monitor-config-header">
                <h4>{getRankModeLabel(item.mode)}配置</h4>
                <button
                  className="intraday-monitor-delete-btn"
                  type="button"
                  disabled={!item.canDelete || loading || dateOptionsLoading}
                  onClick={() => removeRankModeConfig(item.index)}
                >
                  删除
                </button>
              </div>

              <div className="intraday-monitor-config-grid">
                <label className="intraday-monitor-field intraday-monitor-field-inline">
                  <span>榜单类型</span>
                  <select
                    value={item.mode}
                    onChange={(event) => {
                      const nextMode =
                        event.target.value === "scene" ? "scene" : "total";
                      setRankModeConfigs((current) => {
                        const currentRow = current[item.index];
                        if (!currentRow) return current;
                        if (nextMode === currentRow.mode) return current;
                        if (
                          nextMode === "total" &&
                          current.some(
                            (cfg, idx) =>
                              cfg.mode === "total" && idx !== item.index,
                          )
                        ) {
                          return current;
                        }
                        return current.map((cfg, idx) =>
                          idx === item.index
                            ? {
                                ...cfg,
                                mode: nextMode,
                                sceneName:
                                  nextMode === "scene" ? cfg.sceneName : "全部",
                              }
                            : cfg,
                        );
                      });
                    }}
                    disabled={loading || dateOptionsLoading}
                  >
                    <option value="total">总榜</option>
                    <option value="scene">Scene榜</option>
                  </select>
                </label>

                {item.mode === "scene" ? (
                  <label className="intraday-monitor-field intraday-monitor-field-inline">
                    <span>场景筛选</span>
                    <select
                      value={item.sceneName}
                      onChange={(event) => {
                        const value = event.target.value;
                        setRankModeConfigs((current) =>
                          current.map((cfg, idx) =>
                            idx === item.index
                              ? { ...cfg, sceneName: value }
                              : cfg,
                          ),
                        );
                      }}
                    >
                      <option value="全部">全部</option>
                      {sceneOptions.map((sceneName) => (
                        <option key={sceneName} value={sceneName}>
                          {sceneName}
                        </option>
                      ))}
                    </select>
                  </label>
                ) : (
                  <div className="intraday-monitor-placeholder">
                    总榜不需要场景筛选
                  </div>
                )}

                <label className="intraday-monitor-field intraday-monitor-field-inline">
                  <span>模板</span>
                  <select
                    value={item.templateId}
                    onChange={(event) => {
                      const value = event.target.value;
                      setRankModeConfigs((current) =>
                        current.map((cfg, idx) =>
                          idx === item.index
                            ? { ...cfg, templateId: value }
                            : cfg,
                        ),
                      );
                    }}
                  >
                    <option value="">未选择</option>
                    {templates.map((tpl) => (
                      <option key={tpl.id} value={tpl.id}>
                        {tpl.name}
                      </option>
                    ))}
                  </select>
                </label>
              </div>

              <div className="intraday-monitor-config-badge-row">
                <span className="intraday-monitor-config-badge intraday-monitor-config-badge-scene">
                  类型：{getRankModeLabel(item.mode)}
                </span>
                {item.mode === "scene" ? (
                  <span className="intraday-monitor-config-badge intraday-monitor-config-badge-scene">
                    场景：{item.sceneName}
                  </span>
                ) : null}
                <span className="intraday-monitor-config-badge intraday-monitor-config-badge-template">
                  模板：
                  {item.template
                    ? `${item.template.name} · ${summarizeExpression(item.template.expression)}`
                    : "未选择"}
                </span>
              </div>
            </section>
          ))}
        </div>

        {error ? <div className="intraday-monitor-error">{error}</div> : null}
      </section>

      <section className="intraday-monitor-card">
        <div className="intraday-monitor-table-head">
          <h3 className="intraday-monitor-subtitle">结果表格</h3>
          <div className="intraday-monitor-time-strip" aria-live="polite">
            <span className="intraday-monitor-time-pill">
              <small>刷新</small>
              <strong>{formatRefreshTime(refreshedAt)}</strong>
            </span>
            <span className="intraday-monitor-time-pill is-current">
              <small>当前</small>
              <strong>{formatClock(currentTime)}</strong>
            </span>
          </div>
        </div>

        {rows.length === 0 ? (
          <div className="intraday-monitor-empty">暂无数据</div>
        ) : (
          <div className="intraday-monitor-result-sections">
            {rankModeConfigs.some((item) => item.mode === "total") ? (
              <section className="intraday-monitor-result-block">
                <header className="intraday-monitor-scene-head">
                  <h4>总榜</h4>
                  <div className="intraday-monitor-scene-head-actions">
                    <button
                      className="intraday-monitor-refresh-btn"
                      type="button"
                      onClick={() => void refreshRowsByGroup("total")}
                      disabled={
                        dateOptionsLoading ||
                        sourcePathTrimmed === "" ||
                        totalModeRows.length === 0
                      }
                    >
                      {isRefreshingTotal
                        ? refreshStage === "preparing"
                          ? "准备中..."
                          : "刷新中..."
                        : "刷新总榜实时"}
                    </button>
                    <button
                      className="intraday-monitor-refresh-btn"
                      type="button"
                      onClick={() => void refreshTemplateTagsByGroup("total")}
                      disabled={
                        loading ||
                        dateOptionsLoading ||
                        sourcePathTrimmed === "" ||
                        totalModeRows.length === 0
                      }
                    >
                      {isRefreshingTotal
                        ? refreshStage === "preparing"
                          ? "准备中..."
                          : "重算中..."
                        : "仅刷新标记"}
                    </button>
                  </div>
                </header>
                {sortedTotalRows.length === 0 ? (
                  <div className="intraday-monitor-empty">总榜暂无数据</div>
                ) : (
                  renderTable(sortedTotalRows, TOTAL_MODE_COLUMNS)
                )}
              </section>
            ) : null}

            {rankModeConfigs.some((item) => item.mode === "scene") ? (
              <section className="intraday-monitor-result-block">
                <header className="intraday-monitor-scene-head">
                  <h4>Scene榜</h4>
                </header>
                {groupedSceneRows.length === 0 ? (
                  <div className="intraday-monitor-empty">Scene榜暂无数据</div>
                ) : (
                  <div className="intraday-monitor-scene-groups">
                    {groupedSceneRows.map((group) => (
                      <section
                        key={group.key}
                        className="intraday-monitor-scene-block"
                      >
                        <header className="intraday-monitor-scene-head">
                          <h4>{group.title}</h4>
                          <div className="intraday-monitor-scene-head-actions">
                            <button
                              className="intraday-monitor-refresh-btn"
                              type="button"
                              onClick={() => void refreshRowsByGroup(group.key)}
                              disabled={
                                loading ||
                                dateOptionsLoading ||
                                sourcePathTrimmed === ""
                              }
                            >
                              {isRefreshingScene(group.key)
                                ? refreshStage === "preparing"
                                  ? "准备中..."
                                  : "刷新中..."
                                : "刷新该Scene实时"}
                            </button>
                            <button
                              className="intraday-monitor-refresh-btn"
                              type="button"
                              onClick={() =>
                                void refreshTemplateTagsByGroup(group.key)
                              }
                              disabled={
                                loading ||
                                dateOptionsLoading ||
                                sourcePathTrimmed === ""
                              }
                            >
                              {isRefreshingScene(group.key)
                                ? refreshStage === "preparing"
                                  ? "准备中..."
                                  : "重算中..."
                                : "仅刷新标记"}
                            </button>
                          </div>
                        </header>
                        {renderTable(group.rows, SCENE_MODE_COLUMNS)}
                      </section>
                    ))}
                  </div>
                )}
              </section>
            ) : null}
          </div>
        )}
      </section>

      <IntradayTemplateManagerModal
        open={templateModalOpen}
        sourcePath={sourcePathTrimmed}
        templates={templates}
        onChangeTemplates={updateTemplates}
        onTemplateRemoved={onTemplateRemoved}
        onClose={() => setTemplateModalOpen(false)}
      />
    </div>
  );
}
