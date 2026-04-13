import {
  startTransition,
  useCallback,
  useDeferredValue,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type PointerEvent as ReactPointerEvent,
  type ReactNode,
  type WheelEvent as ReactWheelEvent,
} from "react";
import { useSearchParams } from "react-router-dom";
import {
  getStockDetailRealtime,
  getStockDetailPage,
  getStockDetailStrategySnapshot,
  type DetailKlinePanel,
  type DetailKlineRow,
  type DetailKlinePayload,
  type DetailOverview,
  type DetailPrevRankRow,
  type DetailSceneTriggerRow,
  type DetailStrategyTriggerRow,
  type StockDetailRealtimeData,
  type StockDetailPageData,
} from "../../apis/details";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import {
  listRankTradeDates,
  listStockLookupRows,
  rankOverview,
  type OverviewRow,
  type StockLookupRow,
} from "../../apis/reader";
import {
  sanitizeCodeInput,
  splitTsCode,
  stdTsCode,
} from "../../shared/stockCode";
import {
  buildStockLookupCandidates,
  findExactStockLookupMatch,
  getLookupDigits,
} from "../../shared/stockLookup";
import { readStoredSourcePath } from "../../shared/storage";
import {
  TableSortButton,
  getAriaSort,
  type SortDefinition,
  useTableSort,
} from "../../shared/tableSort";
import {
  DEFAULT_DATE_OPTION,
  normalizeTradeDates,
  pickDateValue,
} from "../../shared/tradeDate";
import {
  filterConceptItems,
  useConceptExclusions,
} from "../../shared/conceptExclusions";
import {
  findWatchObserveRow,
  listWatchObserveRows,
  removeWatchObserveRows,
  type WatchObserveRow,
  upsertWatchObserveRow,
} from "../../apis/watchObserve";
import type { DetailsNavigationItem } from "../../shared/detailsLinkState";
import type { DetailsStrategyCompareSnapshot } from "../../shared/detailsLinkState";
import "./css/DetailsPage.css";

const DEFAULT_TOP_LIMIT = "100";
const DEFAULT_CHART_HEIGHT = 880;
const DEFAULT_VISIBLE_BARS = 90;
const MIN_VISIBLE_BARS = 20;
const DEFAULT_ROW_WEIGHTS = [52, 16, 16, 16];
const CHART_VIEWBOX_WIDTH = 1120;
const CHART_VIEWBOX_HEIGHT = 240;
const CHART_MARGIN = { top: 12, right: 8, bottom: 28, left: 52 };
const CHART_DATE_TICK_COUNT = 6;
const CHART_CURSOR_Y_MIN = 6;
const CHART_CURSOR_Y_MAX = 94;
const CHART_TOOLTIP_LEFT_THRESHOLD = 62;
const CHART_POINTER_DRAG_THRESHOLD = 6;
const CHART_TOUCH_FOCUS_HIT_SLOP = 24;
const STRATEGY_SPLIT_DEFAULT = 0.64;
const STRATEGY_SPLIT_MIN = 0.24;
const STRATEGY_SPLIT_MAX = 0.76;
const STRATEGY_STACK_BREAKPOINT = 1180;
const WATERMARK_CONCEPT_LIMIT = 3;
const MAX_STOCK_NAME_CANDIDATES = 12;
const DETAIL_REALTIME_AUTO_REFRESH_INTERVAL_MS = 15_000;
const DETAIL_REALTIME_LONG_PRESS_MS = 600;
const CANDLE_UP_COLOR = "#d9485f";
const CANDLE_DOWN_COLOR = "#178f68";
const CANDLE_FLAT_COLOR = "#536273";
const CANDLE_REALTIME_UP_COLOR = "#eb7a34";
const CANDLE_REALTIME_DOWN_COLOR = "#2d6cdf";
const LINE_COLORS = ["#0057ff", "#e13a1f", "#6a00f4", "#00843d"];
const CANDLE_BASE_SERIES_KEYS = new Set(["open", "high", "low", "close"]);
type DetailStrategySortKey = "rule_score" | "hit_date" | "lag";
type PrevRankSortKey = "trade_date" | "rank";
type SceneSortKey = "scene_name" | "stage_score" | "risk_score" | "hit_date" | "lag";
const EMPTY_PREV_RANK_ROWS: DetailPrevRankRow[] = [];
const EMPTY_KLINE_ROWS: DetailKlineRow[] = [];
const EMPTY_STRATEGY_ROWS: DetailStrategyTriggerRow[] = [];
const EMPTY_SCENE_ROWS: DetailSceneTriggerRow[] = [];

type FieldRow = {
  label: string;
  value: string;
};

type ChartFocus = {
  absoluteIndex: number;
  panelKey: string;
  cursorXPercent: number;
  cursorYPercent: number;
  pinned: boolean;
};

type TooltipSection = {
  key: string;
  rows: FieldRow[];
  variant?: "default" | "ohlc";
};

type ChartPointerSnapshot = {
  cursorXPercent: number;
  cursorYPercent: number;
  visibleIndex: number;
};

type ChartDragState = {
  pointerId: number;
  panelKey: string;
  mode: "pan" | "focus" | "tap" | "dismiss";
  startClientX: number;
  startClientY: number;
  startVisibleStart: number;
  barsPerPixel: number;
  maxVisibleStart: number;
  moved: boolean;
};

type ScrollSnapshot = {
  left: number;
  top: number;
};

type StrategyCompareSnapshot = {
  tsCode: string;
  relativeTradeDate: string;
  rows: DetailStrategyTriggerRow[];
};

type DetailsPageVariant = "default" | "linked-overlay";

export type DetailsPageProps = {
  variant?: DetailsPageVariant;
  navigationItems?: DetailsNavigationItem[] | null;
  strategyCompareSnapshot?: DetailsStrategyCompareSnapshot | null;
};

function formatNumber(value: unknown, digits = 2) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "--";
  }
  return Number.isInteger(value) ? String(value) : value.toFixed(digits);
}

function formatSignedNumber(value: unknown, digits = 2) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "";
  }
  const formatted = Number.isInteger(value) ? String(value) : value.toFixed(digits);
  return value > 0 ? `+${formatted}` : formatted;
}

function formatFieldValue(value: unknown) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  if (typeof value === "number") {
    return formatNumber(value);
  }
  if (typeof value === "boolean") {
    return value ? "是" : "否";
  }
  return String(value);
}

function getStrategyRuleScore(row: DetailStrategyTriggerRow | null | undefined) {
  return typeof row?.rule_score === "number" && Number.isFinite(row.rule_score)
    ? row.rule_score
    : 0;
}

function isStrategyTriggered(row: DetailStrategyTriggerRow | null | undefined) {
  if (typeof row?.is_triggered === "boolean") {
    return row.is_triggered;
  }
  return getStrategyRuleScore(row) !== 0;
}

function getComparedStrategyRuleScore(
  ruleName: string,
  compareRowMap: Map<string, DetailStrategyTriggerRow> | null | undefined,
) {
  if (!compareRowMap) {
    return null;
  }
  return getStrategyRuleScore(compareRowMap.get(ruleName));
}

function getComparedStrategyTriggered(
  ruleName: string,
  compareRowMap: Map<string, DetailStrategyTriggerRow> | null | undefined,
) {
  if (!compareRowMap) {
    return null;
  }
  return isStrategyTriggered(compareRowMap.get(ruleName));
}

function isStrategyOutRow(
  row: DetailStrategyTriggerRow | null | undefined,
  compareRowMap: Map<string, DetailStrategyTriggerRow> | null | undefined,
  compareTradeDate?: string | null,
  outReferenceTradeDate?: string | null,
) {
  if (!row || isStrategyTriggered(row)) {
    return false;
  }

  const normalizedCompareTradeDate = compareTradeDate?.trim() ?? "";
  if (compareRowMap && normalizedCompareTradeDate !== "") {
    return getComparedStrategyTriggered(row.rule_name, compareRowMap) === true;
  }

  const normalizedOutReferenceTradeDate = outReferenceTradeDate?.trim() ?? "";
  return (
    normalizedOutReferenceTradeDate !== "" &&
    row.hit_date?.trim() === normalizedOutReferenceTradeDate
  );
}

function collectStrategyRows(detail: StockDetailPageData | null | undefined) {
  return [
    ...(detail?.strategy_triggers?.triggered ?? []),
    ...(detail?.strategy_triggers?.untriggered ?? []),
  ];
}

function formatSeriesLabel(key: string) {
  if (key === "j") {
    return "J";
  }
  if (key === "bupiao_long") {
    return "补票长";
  }
  if (key === "bupiao_short") {
    return "补票短";
  }
  if (key === "duokong_long") {
    return "多空长";
  }
  if (key === "duokong_short") {
    return "多空短";
  }
  if (key === "vol") {
    return "量";
  }
  if (key === "brick") {
    return "砖";
  }
  return key;
}

function getSeriesColor(key: string, seriesIndex: number) {
  if (key === "j" || key === "duokong_short") {
    return "#111111";
  }
  if (key === "bupiao_long" || key === "duokong_long") {
    return "#e74c3c";
  }
  if (key === "bupiao_short") {
    return "#2ecc71";
  }
  return LINE_COLORS[seriesIndex % LINE_COLORS.length];
}

function getRealtimeSeriesColor(row: DetailKlineRow, fallbackColor: string) {
  if (!row.is_realtime) {
    return fallbackColor;
  }
  if (row.realtime_color_hint === "up") {
    return CANDLE_REALTIME_UP_COLOR;
  }
  if (row.realtime_color_hint === "down") {
    return CANDLE_REALTIME_DOWN_COLOR;
  }
  if (row.realtime_color_hint === "flat") {
    return CANDLE_FLAT_COLOR;
  }
  return fallbackColor;
}

function buildTopOptionLabel(row: OverviewRow) {
  const rankText =
    typeof row.rank === "number" && Number.isFinite(row.rank)
      ? `#${Math.round(row.rank)}`
      : "#--";
  const scoreText =
    typeof row.total_score === "number" && Number.isFinite(row.total_score)
      ? row.total_score.toFixed(2)
      : "--";
  const nameText =
    typeof row.name === "string" && row.name.trim() !== ""
      ? row.name
      : "未命名";
  return `${rankText} ${row.ts_code} ${nameText} 分数 ${scoreText}`;
}

function toPositiveInt(raw: string) {
  const parsed = Number(raw.trim());
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return null;
  }
  return parsed;
}

function findMatchingTopValue(rows: OverviewRow[], codeInput: string) {
  const hit = rows.find((row) => splitTsCode(row.ts_code) === codeInput);
  return hit?.ts_code ?? "";
}

function buildNavigationItemFromOverviewRow(
  row: OverviewRow,
  fallbackTradeDate: string,
  sourcePath: string,
): DetailsNavigationItem {
  const tradeDate =
    typeof row.trade_date === "string" && row.trade_date.trim() !== ""
      ? row.trade_date.trim()
      : fallbackTradeDate.trim() || undefined;
  const name =
    typeof row.name === "string" && row.name.trim() !== ""
      ? row.name.trim()
      : undefined;

  return {
    tsCode: row.ts_code,
    tradeDate,
    sourcePath: sourcePath.trim() || undefined,
    name,
  };
}

function findNavigationIndex(
  items: DetailsNavigationItem[],
  tsCode: string,
  tradeDate: string,
) {
  const normalizedCode = sanitizeCodeInput(splitTsCode(tsCode));
  const normalizedTradeDate = tradeDate.trim() === "--" ? "" : tradeDate.trim();

  const exactIndex = items.findIndex((item) => {
    const itemCode = sanitizeCodeInput(splitTsCode(item.tsCode));
    const itemTradeDate = item.tradeDate?.trim() ?? "";
    return itemCode === normalizedCode && itemTradeDate === normalizedTradeDate;
  });
  if (exactIndex >= 0) {
    return exactIndex;
  }

  return items.findIndex(
    (item) => sanitizeCodeInput(splitTsCode(item.tsCode)) === normalizedCode,
  );
}

function clampStrategySplitRatio(value: number) {
  return Math.min(STRATEGY_SPLIT_MAX, Math.max(STRATEGY_SPLIT_MIN, value));
}

function buildOverviewRows(
  overview: DetailOverview | null | undefined,
  fallbackCode: string,
  fallbackDate: string,
) {
  return [
    {
      label: "代码",
      value: formatFieldValue(overview?.ts_code ?? fallbackCode),
    },
    { label: "名称", value: formatFieldValue(overview?.name) },
    { label: "市场板块", value: formatFieldValue(overview?.board) },
    { label: "所属行业", value: formatFieldValue(overview?.industry) },
    { label: "所属地区", value: formatFieldValue(overview?.area) },
    {
      label: "参考日",
      value: formatFieldValue(overview?.trade_date ?? fallbackDate),
    },
    { label: "排名", value: buildRankValue(overview?.rank, overview?.total) },
    { label: "总分", value: formatFieldValue(overview?.total_score) },
    { label: "总市值(亿)", value: formatFieldValue(overview?.total_mv_yi) },
    { label: "流通市值(亿)", value: formatFieldValue(overview?.circ_mv_yi) },
  ];
}

function buildConceptItems(value: unknown) {
  if (typeof value !== "string") {
    return [];
  }

  const normalized = value.trim();
  if (normalized === "") {
    return [];
  }

  const parts = normalized
    .split(/[;,，；|、/\n]+/)
    .map((item) => item.trim())
    .filter(Boolean);

  return parts.length > 0 ? Array.from(new Set(parts)) : [normalized];
}

function buildConceptPreview(items: string[], limit = WATERMARK_CONCEPT_LIMIT) {
  if (items.length === 0) {
    return "";
  }

  const preview = items.slice(0, limit).join(" / ");
  return items.length > limit ? `${preview} 等${items.length}项` : preview;
}

function formatAutoRefreshSeconds(intervalMs: number) {
  return `${Math.max(1, Math.round(intervalMs / 1000))}秒`;
}

function getContentScrollElement() {
  return document.querySelector<HTMLElement>(
    '[data-details-scroll-root="true"], .content',
  );
}

function buildRankValue(rank: unknown, total: unknown) {
  const rankText = formatFieldValue(rank);
  if (rankText === "--") {
    return "--";
  }
  const totalText = formatFieldValue(total);
  return totalText === "--" ? rankText : `${rankText} / ${totalText}`;
}

function formatPercentValue(value: number | null) {
  if (value === null || !Number.isFinite(value)) {
    return "--";
  }
  return `${value.toFixed(2)}%`;
}

function findTurnoverValue(item: DetailKlineRow | null) {
  if (!item) {
    return null;
  }

  const candidateKeys = [
    "tor",
    "turnover_rate",
    "turnover",
    "turnover_rate_f",
  ] as const;
  for (const key of candidateKeys) {
    const value = item[key];
    if (typeof value === "number" && Number.isFinite(value)) {
      return `${value.toFixed(2)}%`;
    }
    if (typeof value === "string" && value.trim() !== "") {
      return value;
    }
  }

  return null;
}

function formatRatioValue(value: number | null) {
  if (value === null || !Number.isFinite(value)) {
    return "--";
  }
  return value.toFixed(2);
}

function buildRankLookup(
  overview: DetailOverview | null | undefined,
  prevRanks: Array<{
    trade_date: string;
    rank?: number | null;
    total?: number | null;
  }>,
) {
  const lookup = new Map<string, string>();

  if (overview?.trade_date) {
    const currentRankValue = buildRankValue(overview.rank, overview.total);
    if (currentRankValue !== "--") {
      lookup.set(overview.trade_date, currentRankValue);
    }
  }

  prevRanks.forEach((row) => {
    const rankValue = buildRankValue(row.rank, row.total);
    if (rankValue !== "--") {
      lookup.set(row.trade_date, rankValue);
    }
  });

  return lookup;
}

function toSceneStageLabel(value: string | null | undefined) {
  const normalized = value?.trim().toLowerCase() ?? "";
  if (normalized === "observe") {
    return "观察";
  }
  if (normalized === "trigger") {
    return "触发";
  }
  if (normalized === "confirm") {
    return "确认";
  }
  if (normalized === "fail") {
    return "失效";
  }
  if (normalized === "idle") {
    return "空闲";
  }
  if (normalized === "") {
    return "未进入";
  }
  return value?.trim() ?? "未进入";
}

function getSceneStageToken(value: string | null | undefined) {
  const normalized = value?.trim().toLowerCase() ?? "idle";
  if (["observe", "trigger", "confirm", "fail"].includes(normalized)) {
    return normalized;
  }
  return "idle";
}

function buildSceneStatusRows(
  detail: StockDetailPageData | null | undefined,
): DetailSceneTriggerRow[] {
  const triggered = detail?.strategy_scenes?.triggered ?? EMPTY_SCENE_ROWS;
  const untriggered = detail?.strategy_scenes?.untriggered ?? EMPTY_SCENE_ROWS;
  return [...triggered, ...untriggered];
}

function buildSceneStatusStats(rows: DetailSceneTriggerRow[]) {
  const counter = new Map<string, number>([
    ["observe", 0],
    ["trigger", 0],
    ["confirm", 0],
    ["fail", 0],
    ["idle", 0],
  ]);

  rows.forEach((row) => {
    const key = getSceneStageToken(row.stage);
    counter.set(key, (counter.get(key) ?? 0) + 1);
  });

  const total = rows.length;
  const colors: Record<string, string> = {
    observe: "#f59e0b",
    trigger: "#2563eb",
    confirm: "#16a34a",
    fail: "#dc2626",
    idle: "#64748b",
  };

  return ["observe", "trigger", "confirm", "fail", "idle"].map((key) => {
    const count = counter.get(key) ?? 0;
    const ratio = total > 0 ? (count / total) * 100 : 0;
    return {
      key,
      label: toSceneStageLabel(key),
      count,
      ratio,
      color: colors[key],
    };
  });
}

type SceneOverviewItem = {
  sceneName: string;
  stage: string | null | undefined;
  stageScore: number | null;
  sceneRank: number | null;
  sceneRuleScore: number | null;
  contributionPct: number | null;
  contributionPctDisplay: number | null;
  color: string;
};

const SCENE_OVERVIEW_COLORS = [
  "#f59e0b",
  "#2563eb",
  "#16a34a",
  "#ef4444",
  "#0891b2",
  "#ea580c",
  "#4f46e5",
  "#65a30d",
] as const;

function getSceneOverviewColor(sceneName: string) {
  let hash = 0;
  for (const char of sceneName) {
    hash = (hash * 31 + char.codePointAt(0)!) >>> 0;
  }
  return SCENE_OVERVIEW_COLORS[hash % SCENE_OVERVIEW_COLORS.length];
}

function buildSceneRuleScoreMap(detail: StockDetailPageData | null | undefined) {
  const byScene = new Map<string, number>();
  const rows = [
    ...(detail?.strategy_triggers?.triggered ?? []),
    ...(detail?.strategy_triggers?.untriggered ?? []),
  ];

  let assignedTotal = 0;
  rows.forEach((row) => {
    const sceneName = row.scene_name?.trim();
    const ruleScore =
      typeof row.rule_score === "number" && Number.isFinite(row.rule_score)
        ? row.rule_score
        : null;
    if (!sceneName || ruleScore === null) {
      return;
    }
    assignedTotal += ruleScore;
    byScene.set(sceneName, (byScene.get(sceneName) ?? 0) + ruleScore);
  });

  return { byScene, assignedTotal };
}

function buildSceneOverviewItems(
  rows: DetailSceneTriggerRow[],
  sceneRuleScoreMap: Map<string, number>,
  assignedRuleTotal: number,
): SceneOverviewItem[] {
  const sortedRows = [...rows].sort((left, right) => {
    const leftRank = typeof left.scene_rank === "number" ? left.scene_rank : null;
    const rightRank = typeof right.scene_rank === "number" ? right.scene_rank : null;
    if (leftRank !== null && rightRank !== null && leftRank !== rightRank) {
      return leftRank - rightRank;
    }
    if (leftRank !== null && rightRank === null) {
      return -1;
    }
    if (leftRank === null && rightRank !== null) {
      return 1;
    }

    const leftScore = typeof left.stage_score === "number" && Number.isFinite(left.stage_score)
      ? left.stage_score
      : Number.NEGATIVE_INFINITY;
    const rightScore = typeof right.stage_score === "number" && Number.isFinite(right.stage_score)
      ? right.stage_score
      : Number.NEGATIVE_INFINITY;
    if (leftScore !== rightScore) {
      return rightScore - leftScore;
    }
    return left.scene_name.localeCompare(right.scene_name, "zh-Hans-CN");
  });

  const denominator = assignedRuleTotal !== 0 ? assignedRuleTotal : null;

  const baseItems = sortedRows.map((row) => {
    const stageScore =
      typeof row.stage_score === "number" && Number.isFinite(row.stage_score)
        ? row.stage_score
        : null;
    const sceneRuleScore = sceneRuleScoreMap.get(row.scene_name) ?? null;

    return {
      sceneName: row.scene_name,
      stage: row.stage,
      stageScore,
      sceneRank: typeof row.scene_rank === "number" ? row.scene_rank : null,
      sceneRuleScore,
      contributionPct:
        denominator !== null && sceneRuleScore !== null
          ? (sceneRuleScore / denominator) * 100
          : null,
      contributionPctDisplay: null,
      color: getSceneOverviewColor(row.scene_name),
    } as SceneOverviewItem;
  });

  const validIndices = baseItems
    .map((item, index) => ({ index, value: item.contributionPct }))
    .filter((item): item is { index: number; value: number } => item.value !== null);

  if (validIndices.length === 0) {
    return baseItems;
  }

  const scaled = validIndices.map(({ index, value }) => {
    const scaledValue = value * 10;
    const floorInt = Math.floor(scaledValue);
    return {
      index,
      floorInt,
      remainder: scaledValue - floorInt,
    };
  });

  const floorIntSum = scaled.reduce((acc, item) => acc + item.floorInt, 0);
  const needSteps = Math.max(0, 1000 - floorIntSum);

  scaled
    .sort((a, b) => b.remainder - a.remainder)
    .forEach((item, position) => {
      const displayInt = item.floorInt + (position < needSteps ? 1 : 0);
      baseItems[item.index].contributionPctDisplay = displayInt / 10;
    });

  return baseItems;
}

function buildDetailTooltipRows(
  panel: DetailKlinePanel,
  item: DetailKlineRow | null,
  absoluteIndex: number | null,
  allItems: DetailKlineRow[],
  rankLookup: Map<string, string>,
): TooltipSection[] {
  if (!item) {
    return [];
  }

  if (panel.key === "price" || panel.kind === "candles") {
    const prevClose =
      absoluteIndex !== null && absoluteIndex > 0
        ? getNumericField(allItems[absoluteIndex - 1], "close")
        : null;
    const currentClose = getNumericField(item, "close");
    const changePct =
      prevClose !== null && prevClose !== 0 && currentClose !== null
        ? ((currentClose - prevClose) / prevClose) * 100
        : null;
    const rows: FieldRow[] = [
      { label: "涨幅", value: formatPercentValue(changePct) },
      { label: "换手", value: findTurnoverValue(item) ?? "--" },
    ];

    const rankValue = rankLookup.get(item.trade_date);
    if (rankValue) {
      rows.push({ label: "排名", value: rankValue });
    }

    const overlayRows = (panel.series_keys ?? [])
      .filter((key) => !CANDLE_BASE_SERIES_KEYS.has(key))
      .map((key) => ({
        label: formatSeriesLabel(key),
        value: formatFieldValue(item[key]),
      }))
      .filter((row) => row.value !== "--");

    return [
      {
        key: `${panel.key}-summary`,
        rows,
      },
      {
        key: `${panel.key}-ohlc`,
        variant: "ohlc",
        rows: [
          { label: "C", value: formatFieldValue(item.close) },
          { label: "O", value: formatFieldValue(item.open) },
          { label: "H", value: formatFieldValue(item.high) },
          { label: "L", value: formatFieldValue(item.low) },
        ],
      },
      ...(overlayRows.length > 0
        ? [
            {
              key: `${panel.key}-overlay`,
              rows: overlayRows,
            } satisfies TooltipSection,
          ]
        : []),
    ];
  }

  if (panel.key === "volume" || panel.kind === "bar") {
    const prevVol =
      absoluteIndex !== null && absoluteIndex > 0
        ? getNumericField(allItems[absoluteIndex - 1], "vol")
        : null;
    const currentVol = getNumericField(item, "vol");
    const volumeRatio =
      prevVol !== null && prevVol !== 0 && currentVol !== null
        ? currentVol / prevVol
        : null;

    return [
      {
        key: `${panel.key}-raw`,
        rows: [
          { label: "量", value: formatFieldValue(item.vol) },
          { label: "量比", value: formatRatioValue(volumeRatio) },
        ],
      },
    ];
  }

  if (panel.key === "brick" || panel.kind === "brick") {
    const prevBrick =
      absoluteIndex !== null && absoluteIndex > 0
        ? getNumericField(allItems[absoluteIndex - 1], "brick")
        : null;

    return [
      {
        key: `${panel.key}-raw`,
        rows: [
          { label: "开", value: formatFieldValue(prevBrick) },
          { label: "收", value: formatFieldValue(item.brick) },
        ],
      },
    ];
  }

  const seriesKeys = panel.series_keys?.length ? panel.series_keys : [];
  return [
    {
      key: `${panel.key}-raw`,
      rows: seriesKeys.map((key) => ({
        label: formatSeriesLabel(key),
        value: formatFieldValue(item[key]),
      })),
    },
  ];
}

function buildDefaultPanels() {
  return [
    {
      key: "price",
      label: "主K",
      kind: "candles",
      series_keys: [
        "open",
        "high",
        "low",
        "close",
        "duokong_short",
        "duokong_long",
      ],
    },
    {
      key: "indicator",
      label: "指标",
      kind: "line",
      series_keys: ["j", "bupiao_long", "bupiao_short"],
    },
    { key: "volume", label: "量能", kind: "bar", series_keys: ["vol"] },
    { key: "brick", label: "砖型图", kind: "brick", series_keys: ["brick"] },
  ] satisfies DetailKlinePanel[];
}

function buildChartTemplateRows(kline: DetailKlinePayload | null | undefined) {
  const resolvedWeights =
    kline?.row_weights?.filter((weight) => weight > 0) ?? [];
  const weights =
    resolvedWeights.length > 0 ? resolvedWeights : DEFAULT_ROW_WEIGHTS;
  return weights.map((weight) => `${weight}fr`).join(" ");
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function getNumericField(row: DetailKlineRow, key: string) {
  const value = row[key];
  return isFiniteNumber(value) ? value : null;
}

function buildDomain(values: number[], includeZero = false) {
  if (values.length === 0) {
    return null;
  }

  let min = Math.min(...values);
  let max = Math.max(...values);

  if (includeZero) {
    min = Math.min(min, 0);
    max = Math.max(max, 0);
  }

  if (min === max) {
    const padding = Math.max(Math.abs(min) * 0.08, 1);
    return { min: min - padding, max: max + padding };
  }

  const span = max - min;
  const paddingTop = span * 0.08;
  const paddingBottom = includeZero && min >= 0 ? 0 : span * 0.08;

  return {
    min: min - paddingBottom,
    max: max + paddingTop,
  };
}

function formatAxisValue(value: number) {
  const abs = Math.abs(value);
  if (abs >= 100000000) {
    return `${(value / 100000000).toFixed(1)}亿`;
  }
  if (abs >= 10000) {
    return `${(value / 10000).toFixed(abs >= 1000000 ? 0 : 1)}万`;
  }
  if (Math.abs(value - Math.round(value)) < 1e-6) {
    return Math.round(value).toString();
  }
  if (abs >= 100) {
    return value.toFixed(0);
  }
  if (abs >= 1) {
    return value.toFixed(2);
  }
  return value.toFixed(3);
}

function formatTradeDateLabel(value: string) {
  if (/^\d{8}$/.test(value)) {
    return `${value.slice(2, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`;
  }
  return value;
}

function buildCenteredPercentGrid(min: number, max: number) {
  const midpoint = (min + max) / 2;
  const step = Math.max(Math.abs(midpoint), (max - min) / 2, 1) * 0.1;
  const values = new Set<number>();
  const epsilon = step * 0.1;

  values.add(Number(midpoint.toFixed(6)));

  for (let value = midpoint + step; value <= max + epsilon; value += step) {
    values.add(Number(value.toFixed(6)));
  }

  for (let value = midpoint - step; value >= min - epsilon; value -= step) {
    values.add(Number(value.toFixed(6)));
  }

  return [...values]
    .filter((value) => value >= min - epsilon && value <= max + epsilon)
    .sort((left, right) => right - left);
}

function buildNiceAxisGrid(min: number, max: number, targetTickCount = 7) {
  if (!Number.isFinite(min) || !Number.isFinite(max) || max <= min) {
    return [];
  }

  const span = max - min;
  const rawStep = span / Math.max(targetTickCount - 1, 1);
  const magnitude = 10 ** Math.floor(Math.log10(Math.max(rawStep, 1e-6)));
  const candidateSteps = Array.from(
    new Set(
      [1, 2, 2.5, 5, 10]
        .flatMap((factor) => [factor * magnitude, factor * magnitude * 0.1])
        .filter((step) => Number.isFinite(step) && step > 0),
    ),
  ).sort((left, right) => left - right);

  let bestValues: number[] = [];
  let bestScore = Number.POSITIVE_INFINITY;

  for (const step of candidateSteps) {
    const epsilon = step * 1e-6;
    const start = Math.ceil((min - epsilon) / step) * step;
    const end = Math.floor((max + epsilon) / step) * step;
    if (end < start) {
      continue;
    }

    const values: number[] = [];
    for (let value = end; value >= start - epsilon; value -= step) {
      values.push(Number(value.toFixed(8)));
    }

    if (values.length === 0) {
      continue;
    }

    const score =
      Math.abs(values.length - targetTickCount) +
      (values.length < 4 ? 2 : 0) +
      step / Math.max(span, 1);
    if (score < bestScore) {
      bestScore = score;
      bestValues = values;
    }
  }

  return bestValues;
}

function buildAxisLabelValues(
  values: number[],
  kind: DetailKlinePanel["kind"],
) {
  if (values.length <= 5 || kind === "candles") {
    return values;
  }

  const keep = new Set<number>();
  const midpointIndex = Math.floor(values.length / 2);
  const step = Math.max(Math.ceil(values.length / 5), 2);

  values.forEach((value, index) => {
    if (
      index === 0 ||
      index === values.length - 1 ||
      index === midpointIndex ||
      index % step === 0
    ) {
      keep.add(value);
    }
  });

  return values.filter((value) => keep.has(value));
}

function buildDateTickIndices(count: number, maxTicks = CHART_DATE_TICK_COUNT) {
  if (count <= 0) {
    return [];
  }

  const ticks = new Set<number>([0, count - 1]);
  const visibleCount = Math.min(maxTicks, count);

  if (visibleCount > 2) {
    const step = (count - 1) / (visibleCount - 1);
    for (let index = 1; index < visibleCount - 1; index += 1) {
      ticks.add(Math.round(index * step));
    }
  }

  return [...ticks].sort((left, right) => left - right);
}

function buildLineSegments(
  items: DetailKlineRow[],
  key: string,
  xAt: (index: number) => number,
  yAt: (value: number) => number,
) {
  const segments: Array<Array<{ x: number; y: number }>> = [];
  let current: Array<{ x: number; y: number }> = [];

  items.forEach((row, index) => {
    const value = getNumericField(row, key);
    if (value === null) {
      if (current.length > 0) {
        segments.push(current);
        current = [];
      }
      return;
    }

    current.push({ x: xAt(index), y: yAt(value) });
  });

  if (current.length > 0) {
    segments.push(current);
  }

  return segments;
}

function buildLinePath(points: Array<{ x: number; y: number }>) {
  return points
    .map(
      (point, index) =>
        `${index === 0 ? "M" : "L"} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`,
    )
    .join(" ");
}

function clampNumber(value: number, min: number, max: number) {
  if (max < min) {
    return min;
  }
  return Math.min(Math.max(value, min), max);
}

function resolveVisibleIndexFromChartX(
  chartXPercent: number,
  itemCount: number,
) {
  if (itemCount <= 0) {
    return null;
  }

  const plotStartPercent = (CHART_MARGIN.left / CHART_VIEWBOX_WIDTH) * 100;
  const plotEndPercent =
    ((CHART_VIEWBOX_WIDTH - CHART_MARGIN.right) / CHART_VIEWBOX_WIDTH) * 100;
  const plotXPercent = clampNumber(
    (chartXPercent - plotStartPercent) / (plotEndPercent - plotStartPercent),
    0,
    0.999999,
  );

  return clampNumber(
    Math.round(plotXPercent * itemCount - 0.5),
    0,
    itemCount - 1,
  );
}

function buildChartPointerSnapshot(
  viewport: HTMLDivElement,
  clientX: number,
  clientY: number,
  itemCount: number,
): ChartPointerSnapshot | null {
  if (itemCount <= 0) {
    return null;
  }

  const viewportRect = viewport.getBoundingClientRect();
  if (viewportRect.width <= 0 || viewportRect.height <= 0) {
    return null;
  }

  const svg = viewport.querySelector<SVGSVGElement>(".details-chart-svg");
  const svgRect =
    svg && svg.clientWidth > 0 && svg.clientHeight > 0
      ? svg.getBoundingClientRect()
      : viewportRect;
  const chartXPercent = clampNumber(
    ((clientX - svgRect.left) / svgRect.width) * 100,
    0,
    99.9999,
  );
  const visibleIndex = resolveVisibleIndexFromChartX(chartXPercent, itemCount);

  if (visibleIndex === null) {
    return null;
  }

  return {
    cursorXPercent: clampNumber(
      ((clientX - viewportRect.left) / viewportRect.width) * 100,
      0,
      99.9999,
    ),
    cursorYPercent: clampNumber(
      ((clientY - viewportRect.top) / viewportRect.height) * 100,
      CHART_CURSOR_Y_MIN,
      CHART_CURSOR_Y_MAX,
    ),
    visibleIndex,
  };
}

function isPointerNearChartFocus(
  _panelKey: string,
  viewport: HTMLDivElement,
  clientX: number,
  _clientY: number,
  focus: ChartFocus | null,
) {
  if (!focus) {
    return false;
  }

  const rect = viewport.getBoundingClientRect();
  if (rect.width <= 0 || rect.height <= 0) {
    return false;
  }

  const focusClientX = rect.left + (rect.width * focus.cursorXPercent) / 100;
  return Math.abs(clientX - focusClientX) <= CHART_TOUCH_FOCUS_HIT_SLOP;
}

function buildBrickBodies(
  items: DetailKlineRow[],
  key: string,
  initialPrevious: number | null = null,
) {
  const bodies: Array<{
    trade_date: string;
    item_index: number;
    open: number;
    close: number;
    high: number;
    low: number;
  }> = [];
  let previous: number | null = initialPrevious;

  items.forEach((row, itemIndex) => {
    const current = getNumericField(row, key);
    if (current === null) {
      previous = null;
      return;
    }

    if (previous === null) {
      previous = current;
      return;
    }

    const open = previous;
    const close = current;
    bodies.push({
      trade_date: row.trade_date,
      item_index: itemIndex,
      open,
      close,
      high: Math.max(open, close),
      low: Math.min(open, close),
    });
    previous = current;
  });

  return bodies;
}

function renderChartPanel(
  panel: DetailKlinePanel,
  items: DetailKlineRow[],
  index: number,
  panelCount: number,
  watermarkName: string,
  watermarkCode: string,
  watermarkConcept: string,
  chartFocus: ChartFocus | null,
  effectiveVisibleStart: number,
  allItems: DetailKlineRow[],
  referenceTradeDate: string | null,
  rankLookup: Map<string, string>,
  onChartPointerDown: (
    panelKey: string,
    event: ReactPointerEvent<HTMLDivElement>,
  ) => void,
  onChartPointerMove: (
    panelKey: string,
    event: ReactPointerEvent<HTMLDivElement>,
  ) => void,
  onChartPointerUp: (
    panelKey: string,
    event: ReactPointerEvent<HTMLDivElement>,
  ) => void,
  onChartPointerLeave: (
    panelKey: string,
    event: ReactPointerEvent<HTMLDivElement>,
  ) => void,
  onChartPointerCancel: (
    panelKey: string,
    event: ReactPointerEvent<HTMLDivElement>,
  ) => void,
  watchObserveButton: ReactNode,
) {
  const kind = panel.kind ?? "line";
  const showDateAxis = index === panelCount - 1;
  const seriesKeys = panel.series_keys?.length ? panel.series_keys : [];
  const candleOverlayKeys =
    kind === "candles"
      ? seriesKeys.filter((key) => !CANDLE_BASE_SERIES_KEYS.has(key))
      : [];
  const headerSeriesKeys = kind === "candles" ? candleOverlayKeys : seriesKeys;
  const plotWidth =
    CHART_VIEWBOX_WIDTH - CHART_MARGIN.left - CHART_MARGIN.right;
  const plotHeight =
    CHART_VIEWBOX_HEIGHT - CHART_MARGIN.top - CHART_MARGIN.bottom;
  const step = items.length > 0 ? plotWidth / items.length : plotWidth;
  const xAt = (itemIndex: number) =>
    CHART_MARGIN.left + step * itemIndex + step / 2;
  const activeVisibleIndex =
    chartFocus &&
    chartFocus.absoluteIndex >= effectiveVisibleStart &&
    chartFocus.absoluteIndex < effectiveVisibleStart + items.length
      ? chartFocus.absoluteIndex - effectiveVisibleStart
      : null;
  const activeAbsoluteIndex =
    activeVisibleIndex !== null
      ? effectiveVisibleStart + activeVisibleIndex
      : null;
  const isActivePanel = chartFocus?.panelKey === panel.key;
  const focusXPercent = chartFocus?.cursorXPercent ?? null;
  const referenceVisibleIndex =
    referenceTradeDate !== null
      ? items.findIndex((item) => item.trade_date === referenceTradeDate)
      : -1;
  const tooltipHorizontalClass =
    (chartFocus?.cursorXPercent ?? 0) > CHART_TOOLTIP_LEFT_THRESHOLD
      ? "details-chart-tooltip-left"
      : "details-chart-tooltip-right";
  const tooltipSections =
    isActivePanel && activeVisibleIndex !== null
      ? buildDetailTooltipRows(
          panel,
          items[activeVisibleIndex] ?? null,
          activeAbsoluteIndex,
          allItems,
          rankLookup,
        )
      : [];

  let domain: { min: number; max: number } | null = null;
  let zeroY: number | null = null;
  let svgContent: ReactNode = null;

  if (kind === "candles") {
    const values = items.flatMap((row) => {
      const out: number[] = [];
      const open = getNumericField(row, "open");
      const high = getNumericField(row, "high");
      const low = getNumericField(row, "low");
      const close = getNumericField(row, "close");

      if (open !== null) out.push(open);
      if (high !== null) out.push(high);
      if (low !== null) out.push(low);
      if (close !== null) out.push(close);
      for (const key of candleOverlayKeys) {
        const value = getNumericField(row, key);
        if (value !== null) {
          out.push(value);
        }
      }
      return out;
    });

    domain = buildDomain(values);
    if (domain) {
      const currentDomain = domain;
      const yAt = (value: number) =>
        CHART_MARGIN.top +
        ((currentDomain.max - value) /
          (currentDomain.max - currentDomain.min)) *
          plotHeight;
      const bodyWidth = Math.max(Math.min(step * 0.58, 18), 3);

      const candleNodes = items.map((row, itemIndex) => {
        const open = getNumericField(row, "open");
        const high = getNumericField(row, "high");
        const low = getNumericField(row, "low");
        const close = getNumericField(row, "close");

        if (open === null || high === null || low === null || close === null) {
          return null;
        }

        const x = xAt(itemIndex);
        const bodyTop = Math.min(yAt(open), yAt(close));
        const bodyHeight = Math.max(Math.abs(yAt(open) - yAt(close)), 1.2);
        const color =
          close > open
            ? CANDLE_UP_COLOR
            : close < open
              ? CANDLE_DOWN_COLOR
              : CANDLE_FLAT_COLOR;
        const resolvedColor = getRealtimeSeriesColor(row, color);

        return (
          <g key={`${panel.key}-${row.trade_date}`}>
            <line
              className="details-chart-candle-wick"
              x1={x}
              y1={yAt(high)}
              x2={x}
              y2={yAt(low)}
              stroke={resolvedColor}
            />
            <rect
              className="details-chart-candle-body"
              x={x - bodyWidth / 2}
              y={bodyTop}
              width={bodyWidth}
              height={bodyHeight}
              fill={resolvedColor}
              stroke={resolvedColor}
              rx={1.2}
            />
          </g>
        );
      });

      const overlayNodes = candleOverlayKeys.map((key, seriesIndex) => {
        const segments = buildLineSegments(items, key, xAt, yAt);
        if (segments.length === 0) {
          return null;
        }

        return (
          <g key={`${panel.key}-${key}`}>
            {segments.map((segment, segmentIndex) => (
              <path
                className="details-chart-line-path details-chart-line-path-main"
                key={`${key}-${segmentIndex}`}
                d={buildLinePath(segment)}
                stroke={getSeriesColor(key, seriesIndex)}
              />
            ))}
          </g>
        );
      });

      svgContent = [...candleNodes, ...overlayNodes];
    }
  } else if (kind === "line") {
    const values = items.flatMap((row) =>
      seriesKeys.flatMap((key) => {
        const value = getNumericField(row, key);
        return value === null ? [] : [value];
      }),
    );

    domain = buildDomain(values);
    if (domain) {
      const currentDomain = domain;
      const yAt = (value: number) =>
        CHART_MARGIN.top +
        ((currentDomain.max - value) /
          (currentDomain.max - currentDomain.min)) *
          plotHeight;

      svgContent = seriesKeys.map((key, seriesIndex) => {
        const segments = buildLineSegments(items, key, xAt, yAt);

        return (
          <g key={`${panel.key}-${key}`}>
            {segments.map((segment, segmentIndex) => (
              <path
                className="details-chart-line-path details-chart-line-path-indicator"
                key={`${key}-${segmentIndex}`}
                d={buildLinePath(segment)}
                stroke={getSeriesColor(key, seriesIndex)}
              />
            ))}
          </g>
        );
      });
    }
  } else if (kind === "bar") {
    const values = items.flatMap((row) =>
      seriesKeys.flatMap((key) => {
        const value = getNumericField(row, key);
        return value === null ? [] : [value];
      }),
    );

    domain = buildDomain(values, true);
    if (domain) {
      const currentDomain = domain;
      const yAt = (value: number) =>
        CHART_MARGIN.top +
        ((currentDomain.max - value) /
          (currentDomain.max - currentDomain.min)) *
          plotHeight;
      zeroY = yAt(0);
      const barWidth = Math.max(Math.min(step * 0.72, 18), 3);

      svgContent = items.map((row, itemIndex) => {
        const value =
          seriesKeys.length > 0 ? getNumericField(row, seriesKeys[0]) : null;
        if (value === null || zeroY === null) {
          return null;
        }

        const absoluteIndex = effectiveVisibleStart + itemIndex;
        const prevClose =
          absoluteIndex > 0
            ? getNumericField(allItems[absoluteIndex - 1], "close")
            : null;
        const close = getNumericField(row, "close");
        const color =
          close !== null && prevClose !== null
            ? close > prevClose
              ? CANDLE_UP_COLOR
              : close < prevClose
                ? CANDLE_DOWN_COLOR
                : CANDLE_FLAT_COLOR
            : CANDLE_FLAT_COLOR;
        const resolvedColor = getRealtimeSeriesColor(row, color);
        const x = xAt(itemIndex);
        const y = Math.min(yAt(value), zeroY);
        const height = Math.max(Math.abs(zeroY - yAt(value)), 1);

        return (
          <rect
            className="details-chart-bar"
            key={`${panel.key}-${row.trade_date}`}
            x={x - barWidth / 2}
            y={y}
            width={barWidth}
            height={height}
            fill={resolvedColor}
            rx={1}
          />
        );
      });
    }
  } else if (kind === "brick") {
    const brickKey = seriesKeys[0] ?? "brick";
    const previousRow =
      effectiveVisibleStart > 0
        ? allItems[effectiveVisibleStart - 1] ?? null
        : null;
    const previousBrick =
      previousRow !== null ? getNumericField(previousRow, brickKey) : null;
    const visibleBrickValues = items.flatMap((row) => {
      const value = getNumericField(row, brickKey);
      return value === null ? [] : [value];
    });
    const bodies = buildBrickBodies(items, brickKey, previousBrick);
    const values = [
      ...visibleBrickValues,
      ...(previousBrick === null ? [] : [previousBrick]),
      ...bodies.flatMap((body) => [body.low, body.high]),
    ];

    domain = buildDomain(values);
    if (domain) {
      const currentDomain = domain;
      const yAt = (value: number) =>
        CHART_MARGIN.top +
        ((currentDomain.max - value) /
          (currentDomain.max - currentDomain.min)) *
          plotHeight;
      const bodyWidth = Math.max(Math.min(step * 0.72, 22), 4);

      svgContent = bodies.map((body) => {
        const x = xAt(body.item_index);
        const openY = yAt(body.open);
        const closeY = yAt(body.close);
        const highY = yAt(body.high);
        const lowY = yAt(body.low);
        const bodyTop = Math.min(openY, closeY);
        const bodyHeight = Math.max(Math.abs(openY - closeY), 1.6);
        const color =
          body.close > body.open
            ? CANDLE_UP_COLOR
            : body.close < body.open
              ? CANDLE_DOWN_COLOR
              : CANDLE_FLAT_COLOR;
        const sourceRow = items[body.item_index];
        const resolvedColor =
          sourceRow !== undefined
            ? getRealtimeSeriesColor(sourceRow, color)
            : color;

        return (
          <g key={`${panel.key}-${body.trade_date}`}>
            <line
              className="details-chart-candle-wick"
              x1={x}
              y1={highY}
              x2={x}
              y2={lowY}
              stroke={resolvedColor}
            />
            <rect
              className="details-chart-brick-body"
              x={x - bodyWidth / 2}
              y={bodyTop}
              width={bodyWidth}
              height={bodyHeight}
              fill={resolvedColor}
              stroke={resolvedColor}
              rx={1.2}
            />
          </g>
        );
      });
    }
  }

  const dateTickIndices = buildDateTickIndices(items.length);
  const tickValues = domain
    ? kind === "brick"
      ? buildNiceAxisGrid(domain.min, domain.max)
      : buildCenteredPercentGrid(domain.min, domain.max)
    : [];
  const labelValues = buildAxisLabelValues(tickValues, kind);
  const gridValues = kind === "candles" ? tickValues : labelValues;
  const yAxisLabels = domain
    ? labelValues.map((value) => ({
        key: `${panel.key}-y-${value}`,
        value: formatAxisValue(value),
        topPercent:
          ((CHART_MARGIN.top +
            ((domain.max - value) / (domain.max - domain.min)) * plotHeight) /
            CHART_VIEWBOX_HEIGHT) *
          100,
      }))
    : [];
  const xAxisLabels =
    showDateAxis && items.length > 0
      ? dateTickIndices.map((itemIndex) => ({
          key: `${panel.key}-x-${items[itemIndex]?.trade_date ?? itemIndex}`,
          value: formatTradeDateLabel(items[itemIndex]?.trade_date ?? ""),
          leftPercent: (xAt(itemIndex) / CHART_VIEWBOX_WIDTH) * 100,
        }))
      : [];

  return (
    <section className="details-chart-panel" key={panel.key}>
      <header className="details-chart-panel-head">
        <div className="details-chart-panel-head-main">
          <strong>{panel.label}</strong>
          {headerSeriesKeys.length > 0 ? (
            <div className="details-chart-panel-head-series">
              {headerSeriesKeys.map((seriesKey, seriesIndex) => (
                <span
                  className="details-chart-panel-head-series-tag"
                  key={`${panel.key}-${seriesKey}`}
                  style={{ color: getSeriesColor(seriesKey, seriesIndex) }}
                >
                  {formatSeriesLabel(seriesKey)}
                </span>
              ))}
            </div>
          ) : (
            <small>
              {seriesKeys.length > 0 ? seriesKeys.join(" / ") : "--"}
            </small>
          )}
        </div>
        <span>{panel.kind ?? "line"}</span>
      </header>

      <div
        className="details-chart-viewport"
        onPointerDown={(event) => onChartPointerDown(panel.key, event)}
        onPointerMove={(event) => onChartPointerMove(panel.key, event)}
        onPointerUp={(event) => onChartPointerUp(panel.key, event)}
        onPointerLeave={(event) => onChartPointerLeave(panel.key, event)}
        onPointerCancel={(event) => onChartPointerCancel(panel.key, event)}
      >
        {index === 0 ? (
          <div className="details-chart-watermark">
            <strong>{watermarkName}</strong>
            <span>{watermarkCode}</span>
            {watermarkConcept !== "" ? <small>{watermarkConcept}</small> : null}
          </div>
        ) : null}

        {panel.key === "price" ? watchObserveButton : null}

        {domain && svgContent ? (
          <svg
            className="details-chart-svg"
            viewBox={`0 0 ${CHART_VIEWBOX_WIDTH} ${CHART_VIEWBOX_HEIGHT}`}
            preserveAspectRatio="none"
          >
            {gridValues.map((value) => {
              const y =
                CHART_MARGIN.top +
                ((domain.max - value) / (domain.max - domain.min)) * plotHeight;

              return (
                <g key={`${panel.key}-tick-${value}`}>
                  <line
                    className="details-chart-grid-line"
                    x1={CHART_MARGIN.left}
                    y1={y}
                    x2={CHART_VIEWBOX_WIDTH - CHART_MARGIN.right}
                    y2={y}
                  />
                </g>
              );
            })}

            {dateTickIndices.map((itemIndex) => (
              <line
                className="details-chart-vertical-line"
                key={`${panel.key}-guide-${items[itemIndex]?.trade_date ?? itemIndex}`}
                x1={xAt(itemIndex)}
                y1={CHART_MARGIN.top}
                x2={xAt(itemIndex)}
                y2={CHART_VIEWBOX_HEIGHT - CHART_MARGIN.bottom}
              />
            ))}

            {referenceVisibleIndex >= 0 ? (
              <line
                className="details-chart-reference-line"
                x1={xAt(referenceVisibleIndex)}
                y1={CHART_MARGIN.top}
                x2={xAt(referenceVisibleIndex)}
                y2={CHART_VIEWBOX_HEIGHT - CHART_MARGIN.bottom}
              />
            ) : null}

            {zeroY !== null ? (
              <line
                className="details-chart-zero-line"
                x1={CHART_MARGIN.left}
                y1={zeroY}
                x2={CHART_VIEWBOX_WIDTH - CHART_MARGIN.right}
                y2={zeroY}
              />
            ) : null}

            {svgContent}
          </svg>
        ) : (
          <div className="details-chart-empty">暂无有效图表数据</div>
        )}

        {yAxisLabels.length > 0 ? (
          <div className="details-chart-axis-layer details-chart-axis-layer-y">
            {yAxisLabels.map((label) => (
              <span
                className="details-chart-y-label"
                key={label.key}
                style={{ top: `${label.topPercent}%` }}
              >
                {label.value}
              </span>
            ))}
          </div>
        ) : null}

        {xAxisLabels.length > 0 ? (
          <div className="details-chart-axis-layer details-chart-axis-layer-x">
            {xAxisLabels.map((label) => (
              <span
                className="details-chart-x-label"
                key={label.key}
                style={{ left: `${label.leftPercent}%` }}
              >
                {label.value}
              </span>
            ))}
          </div>
        ) : null}

        {focusXPercent !== null ? (
          <div
            className="details-chart-crosshair-vertical"
            style={{ left: `${focusXPercent}%` }}
          />
        ) : null}

        {isActivePanel && chartFocus ? (
          <>
            <div
              className="details-chart-crosshair-horizontal"
              style={{ top: `${chartFocus.cursorYPercent}%` }}
            />
            {tooltipSections.length > 0 ? (
              <div
                className={[
                  "details-chart-tooltip",
                  tooltipHorizontalClass,
                  chartFocus.pinned ? "details-chart-tooltip-pinned" : "",
                ]
                  .filter(Boolean)
                  .join(" ")}
                style={{
                  left: `${chartFocus.cursorXPercent}%`,
                  top: `${chartFocus.cursorYPercent}%`,
                }}
              >
                <div className="details-chart-tooltip-head">
                  <strong>
                    {items[activeVisibleIndex ?? 0]?.trade_date ?? "--"}
                  </strong>
                </div>
                <div className="details-chart-tooltip-body">
                  {tooltipSections.map((section) => (
                    <div
                      className={[
                        "details-chart-tooltip-grid",
                        section.variant === "ohlc"
                          ? "details-chart-tooltip-grid-ohlc"
                          : "",
                      ]
                        .filter(Boolean)
                        .join(" ")}
                      key={section.key}
                    >
                      {section.rows.map((row) => (
                        <div
                          className="details-chart-tooltip-row"
                          key={`${section.key}-${row.label}`}
                        >
                          <span>{row.label}</span>
                          <strong>{row.value}</strong>
                        </div>
                      ))}
                    </div>
                  ))}
                </div>
              </div>
            ) : null}
          </>
        ) : null}
      </div>
    </section>
  );
}

function renderFieldGrid(rows: FieldRow[]) {
  return (
    <div className="details-info-grid">
      {rows.map((row) => (
        <div className="details-info-item" key={row.label}>
          <span>{row.label}</span>
          <strong title={row.value}>{row.value}</strong>
        </div>
      ))}
    </div>
  );
}

function StrategyTableSection({
  title,
  rows,
  emptyText,
  sectionKind,
  compareRowMap,
  compareTradeDate,
  outReferenceTradeDate,
}: {
  title: string;
  rows: DetailStrategyTriggerRow[] | undefined;
  emptyText: string;
  sectionKind: "triggered" | "untriggered" | "mixed";
  compareRowMap?: Map<string, DetailStrategyTriggerRow> | null;
  compareTradeDate?: string | null;
  outReferenceTradeDate?: string | null;
}) {
  const showScoreColumn = sectionKind !== "untriggered";
  const effectiveRows = useMemo(() => {
    const nextRows = [...(rows ?? [])];
    if (sectionKind !== "mixed" && sectionKind !== "untriggered") {
      return nextRows;
    }

    nextRows.sort((left, right) => {
      const leftOut = isStrategyOutRow(
        left,
        compareRowMap,
        compareTradeDate,
        outReferenceTradeDate,
      );
      const rightOut = isStrategyOutRow(
        right,
        compareRowMap,
        compareTradeDate,
        outReferenceTradeDate,
      );
      if (leftOut === rightOut) {
        return 0;
      }
      return leftOut ? -1 : 1;
    });

    return nextRows;
  }, [compareRowMap, compareTradeDate, outReferenceTradeDate, rows, sectionKind]);
  const sortDefinitions = useMemo(
    () =>
      ({
        ...(showScoreColumn
          ? {
              rule_score: {
                value: (row: DetailStrategyTriggerRow) => row.rule_score,
              },
            }
          : {}),
        hit_date: { value: (row) => row.hit_date },
        lag: { value: (row) => row.lag },
      }) satisfies Partial<
        Record<DetailStrategySortKey, SortDefinition<DetailStrategyTriggerRow>>
      >,
    [showScoreColumn],
  );
  const { sortKey, sortDirection, sortedRows, toggleSort } = useTableSort<
    DetailStrategyTriggerRow,
    DetailStrategySortKey
  >(effectiveRows, sortDefinitions);

  return (
    <section className="details-subcard">
      <h4 className="details-subcard-title">{title}</h4>
      {effectiveRows.length === 0 ? (
        <div className="details-empty details-empty-soft">{emptyText}</div>
      ) : (
        <div className="details-table-wrap">
          <table className="details-table details-table-strategy">
            <colgroup>
              <col className="details-col-rule" />
              {showScoreColumn ? <col className="details-col-score" /> : null}
              <col className="details-col-date" />
              <col className="details-col-lag" />
              <col className="details-col-tag" />
              <col className="details-col-explain" />
            </colgroup>
            <thead>
              <tr>
                <th>策略</th>
                {showScoreColumn ? (
                  <th
                    aria-sort={getAriaSort(
                      sortKey === "rule_score",
                      sortDirection,
                    )}
                  >
                    <TableSortButton
                      label="分值"
                      isActive={sortKey === "rule_score"}
                      direction={sortDirection}
                      onClick={() => toggleSort("rule_score")}
                      title={`按${title}中的分值排序`}
                    />
                  </th>
                ) : null}
                <th
                  aria-sort={getAriaSort(sortKey === "hit_date", sortDirection)}
                >
                  <TableSortButton
                    label="最近命中"
                    isActive={sortKey === "hit_date"}
                    direction={sortDirection}
                    onClick={() => toggleSort("hit_date")}
                    title={`按${title}中的最近命中日期排序`}
                  />
                </th>
                <th aria-sort={getAriaSort(sortKey === "lag", sortDirection)}>
                  <TableSortButton
                    label="距今"
                    isActive={sortKey === "lag"}
                    direction={sortDirection}
                    onClick={() => toggleSort("lag")}
                    title={`按${title}中的距今排序`}
                  />
                </th>
                <th>标签</th>
                <th>说明</th>
              </tr>
            </thead>
            <tbody>
              {sortedRows.map((row) => {
                const key = `${row.rule_name}-${row.hit_date ?? ""}-${row.tag ?? ""}`;
                const currentScore = getStrategyRuleScore(row);
                const normalizedCompareTradeDate = compareTradeDate?.trim() ?? "";
                const normalizedOutReferenceTradeDate =
                  outReferenceTradeDate?.trim() ?? "";
                const compareScore =
                  compareRowMap && normalizedCompareTradeDate !== ""
                    ? getComparedStrategyRuleScore(row.rule_name, compareRowMap)
                    : null;
                const hasCompareScore = compareScore !== null;
                const compareTriggered =
                  compareRowMap && normalizedCompareTradeDate !== ""
                    ? getComparedStrategyTriggered(row.rule_name, compareRowMap)
                    : null;
                const wasTriggered = compareTriggered === true;
                const isTriggered = isStrategyTriggered(row);
                const scoreDelta = currentScore - (compareScore ?? 0);
                const rowIsOut = isStrategyOutRow(
                  row,
                  compareRowMap,
                  normalizedCompareTradeDate,
                  normalizedOutReferenceTradeDate,
                );
                let changeType = "";
                let badgeText = "";
                let badgeTitle = "";

                if (rowIsOut) {
                  changeType = "out";
                  badgeText = "OUT";
                  badgeTitle =
                    normalizedCompareTradeDate !== ""
                      ? `相对 ${normalizedCompareTradeDate} 由触发转为未触发`
                      : `相对 ${normalizedOutReferenceTradeDate} 由触发转为未触发`;
                } else if (hasCompareScore) {
                  if (sectionKind !== "untriggered" && isTriggered && !wasTriggered) {
                    changeType = "new";
                    badgeText = "NEW";
                    badgeTitle = `相对 ${normalizedCompareTradeDate} 为新触发`;
                  } else if (
                    sectionKind !== "untriggered" &&
                    isTriggered &&
                    wasTriggered &&
                    Math.abs(scoreDelta) >= Number.EPSILON
                  ) {
                    changeType = scoreDelta > 0 ? "up" : "down";
                    badgeText = formatSignedNumber(scoreDelta);
                    badgeTitle = `相对 ${normalizedCompareTradeDate} ${badgeText}`;
                  }
                }

                return (
                  <tr
                    className={
                      changeType
                        ? `details-table-strategy-row details-table-strategy-row-${changeType}`
                        : ""
                    }
                    key={key}
                  >
                    <td>{formatFieldValue(row.rule_name)}</td>
                    {showScoreColumn ? (
                      <td>
                        <div className="details-strategy-score-cell">
                          <span>{formatFieldValue(row.rule_score)}</span>
                          {badgeText ? (
                            <span
                              className={`details-strategy-delta details-strategy-delta-${changeType}`}
                              title={badgeTitle}
                            >
                              {badgeText}
                            </span>
                          ) : null}
                        </div>
                      </td>
                    ) : null}
                    <td>{formatFieldValue(row.hit_date)}</td>
                    <td>{formatFieldValue(row.lag)}</td>
                    <td>{formatFieldValue(row.tag)}</td>
                    <td title={formatFieldValue(row.explain ?? row.when)}>
                      {formatFieldValue(row.explain ?? row.when)}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

function SceneTableSection({
  rows,
  emptyText,
  selectedKey,
  onSelectRow,
}: {
  rows: DetailSceneTriggerRow[];
  emptyText: string;
  selectedKey?: string | null;
  onSelectRow?: (row: DetailSceneTriggerRow) => void;
}) {
  const sortDefinitions = useMemo(
    () =>
      ({
        scene_name: { value: (row: DetailSceneTriggerRow) => row.scene_name },
        stage_score: { value: (row: DetailSceneTriggerRow) => row.stage_score },
        risk_score: { value: (row: DetailSceneTriggerRow) => row.risk_score },
        hit_date: { value: (row: DetailSceneTriggerRow) => row.hit_date },
        lag: { value: (row: DetailSceneTriggerRow) => row.lag },
      }) satisfies Partial<Record<SceneSortKey, SortDefinition<DetailSceneTriggerRow>>>,
    [],
  );
  const { sortKey, sortDirection, sortedRows, toggleSort } = useTableSort<
    DetailSceneTriggerRow,
    SceneSortKey
  >(rows, sortDefinitions, { key: "stage_score", direction: "desc" });

  if (rows.length === 0) {
    return <div className="details-empty details-empty-soft">{emptyText}</div>;
  }

  return (
    <div className="details-table-wrap">
      <table className="details-table details-table-scene">
        <colgroup>
          <col className="details-col-scene-name" />
          <col className="details-col-scene-stage" />
          <col className="details-col-scene-score" />
          <col className="details-col-scene-score" />
          <col className="details-col-date" />
          <col className="details-col-lag" />
        </colgroup>
        <thead>
          <tr>
            <th aria-sort={getAriaSort(sortKey === "scene_name", sortDirection)}>
              <TableSortButton
                label="场景"
                isActive={sortKey === "scene_name"}
                direction={sortDirection}
                onClick={() => toggleSort("scene_name")}
                title="按场景名称排序"
              />
            </th>
            <th>状态</th>
            <th aria-sort={getAriaSort(sortKey === "stage_score", sortDirection)}>
              <TableSortButton
                label="阶段分"
                isActive={sortKey === "stage_score"}
                direction={sortDirection}
                onClick={() => toggleSort("stage_score")}
                title="按阶段分排序"
              />
            </th>
            <th aria-sort={getAriaSort(sortKey === "risk_score", sortDirection)}>
              <TableSortButton
                label="风险分"
                isActive={sortKey === "risk_score"}
                direction={sortDirection}
                onClick={() => toggleSort("risk_score")}
                title="按风险分排序"
              />
            </th>
            <th aria-sort={getAriaSort(sortKey === "hit_date", sortDirection)}>
              <TableSortButton
                label="最近命中"
                isActive={sortKey === "hit_date"}
                direction={sortDirection}
                onClick={() => toggleSort("hit_date")}
                title="按最近命中排序"
              />
            </th>
            <th aria-sort={getAriaSort(sortKey === "lag", sortDirection)}>
              <TableSortButton
                label="距今"
                isActive={sortKey === "lag"}
                direction={sortDirection}
                onClick={() => toggleSort("lag")}
                title="按距今排序"
              />
            </th>
          </tr>
        </thead>
        <tbody>
          {sortedRows.map((row) => {
            const stageToken = getSceneStageToken(row.stage);
            const rowKey = `${row.scene_name}-${row.hit_date ?? "none"}`;
            const isSelected = selectedKey === rowKey;
            return (
              <tr
                className={isSelected ? "details-table-current-row" : ""}
                key={rowKey}
                onClick={() => onSelectRow?.(row)}
              >
                <td>
                  <button
                    className="details-scene-link-btn"
                    type="button"
                    onClick={(event) => {
                      event.stopPropagation();
                      onSelectRow?.(row);
                    }}
                  >
                    {formatFieldValue(row.scene_name)}
                  </button>
                </td>
                <td>
                  <span className={`details-scene-stage-chip is-${stageToken}`}>
                    {toSceneStageLabel(row.stage)}
                  </span>
                </td>
                <td>{formatFieldValue(row.stage_score)}</td>
                <td>{formatFieldValue(row.risk_score)}</td>
                <td>{formatFieldValue(row.hit_date)}</td>
                <td>{formatFieldValue(row.lag)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export default function DetailsPage({
  variant = "default",
  navigationItems,
  strategyCompareSnapshot: externalStrategyCompareSnapshot = null,
}: DetailsPageProps) {
  const [searchParams] = useSearchParams();
  const { excludedConcepts } = useConceptExclusions();
  const [sourcePath, setSourcePath] = useState(() => readStoredSourcePath());
  const [tradeDateInput, setTradeDateInput] = useState("");
  const [lookupInput, setLookupInput] = useState("");
  const [topLimitInput, setTopLimitInput] = useState(DEFAULT_TOP_LIMIT);

  const [topRows, setTopRows] = useState<OverviewRow[]>([]);
  const [stockLookupRows, setStockLookupRows] = useState<StockLookupRow[]>([]);
  const [topResolvedDate, setTopResolvedDate] = useState("");
  const [detailData, setDetailData] = useState<StockDetailPageData | null>(
    null,
  );
  const [dateOptions, setDateOptions] = useState<string[]>([]);
  const [lookupFocused, setLookupFocused] = useState(false);

  const [topLoading, setTopLoading] = useState(false);
  const [detailLoading, setDetailLoading] = useState(false);
  const [dateOptionsLoading, setDateOptionsLoading] = useState(false);
  const [topError, setTopError] = useState("");
  const [detailError, setDetailError] = useState("");
  const [visibleBarCount, setVisibleBarCount] = useState(DEFAULT_VISIBLE_BARS);
  const [visibleStartIndex, setVisibleStartIndex] = useState(0);
  const [chartFocus, setChartFocus] = useState<ChartFocus | null>(null);
  const [strategySplitRatio, setStrategySplitRatio] = useState(
    STRATEGY_SPLIT_DEFAULT,
  );
  const [strategySplitDragging, setStrategySplitDragging] = useState(false);
  const [isStrategyStacked, setIsStrategyStacked] = useState(false);
  const [watchObserveItems, setWatchObserveItems] = useState<WatchObserveRow[]>(
    [],
  );
  const [watchObserveNotice, setWatchObserveNotice] = useState("");
  const [detailRealtimeData, setDetailRealtimeData] =
    useState<StockDetailRealtimeData | null>(null);
  const [detailRealtimeLoading, setDetailRealtimeLoading] = useState(false);
  const [detailRealtimeNotice, setDetailRealtimeNotice] = useState("");
  const [detailRealtimePinned, setDetailRealtimePinned] = useState(false);
  const [strategyCompareSnapshot, setStrategyCompareSnapshot] =
    useState<StrategyCompareSnapshot | null>(
      externalStrategyCompareSnapshot ?? null,
    );
  const [sceneDetailModalOpen, setSceneDetailModalOpen] = useState(false);
  const [sceneDetailTarget, setSceneDetailTarget] =
    useState<DetailSceneTriggerRow | null>(null);
  const chartDragRef = useRef<ChartDragState | null>(null);
  const strategyGridRef = useRef<HTMLDivElement | null>(null);
  const strategyResizePointerIdRef = useRef<number | null>(null);
  const currentRankRowRef = useRef<HTMLTableRowElement | null>(null);
  const rankTableWrapRef = useRef<HTMLDivElement | null>(null);
  const pendingPageScrollRef = useRef<ScrollSnapshot | null>(null);
  const autoFillTopRef = useRef(true);
  const detailRealtimeLongPressTimerRef = useRef<number | null>(null);
  const detailRealtimeLongPressHandledRef = useRef(false);
  const detailRealtimeAutoRefreshKeyRef = useRef("");
  const strategyCompareRequestKeyRef = useRef("");

  const sourcePathTrimmed = sourcePath.trim();
  const isLinkedOverlay = variant === "linked-overlay";
  const routeTsCode = sanitizeCodeInput(searchParams.get("tsCode") ?? "");
  const routeTradeDate = searchParams.get("tradeDate")?.trim() ?? "";
  const routeSourcePath = searchParams.get("sourcePath")?.trim() ?? "";
  const inputCodeDigits = sanitizeCodeInput(lookupInput);
  const normalizedCode =
    inputCodeDigits.length === 6 ? stdTsCode(inputCodeDigits) : "";
  const deferredLookupInput = useDeferredValue(lookupInput);
  const stockNameCandidates = useMemo(
    () =>
      buildStockLookupCandidates(
        stockLookupRows,
        deferredLookupInput,
        MAX_STOCK_NAME_CANDIDATES,
      ),
    [deferredLookupInput, stockLookupRows],
  );
  const exactStockLookupMatch = useMemo(
    () => findExactStockLookupMatch(stockLookupRows, lookupInput),
    [stockLookupRows, lookupInput],
  );
  const selectedTopDigits = exactStockLookupMatch
    ? getLookupDigits(exactStockLookupMatch.ts_code)
    : inputCodeDigits;
  const selectedTopValue = findMatchingTopValue(topRows, selectedTopDigits);
  const readTargetCode =
    normalizedCode ||
    (exactStockLookupMatch
      ? stdTsCode(getLookupDigits(exactStockLookupMatch.ts_code))
      : "");
  const showStockNameCandidates =
    lookupFocused &&
    lookupInput.trim() !== "" &&
    stockNameCandidates.length > 0;
  const linkedNavigationItems = useMemo(
    () =>
      (navigationItems ?? [])
        .map((item) => {
          const normalizedCode = sanitizeCodeInput(splitTsCode(item.tsCode));
          if (normalizedCode === "") {
            return null;
          }

          return {
            tsCode: stdTsCode(normalizedCode),
            tradeDate: item.tradeDate?.trim() || undefined,
            sourcePath: item.sourcePath?.trim() || undefined,
            name: item.name?.trim() || undefined,
          } satisfies DetailsNavigationItem;
        })
        .filter(
          (item): item is NonNullable<typeof item> => item !== null,
        ),
    [navigationItems],
  );

  useEffect(() => {
    setStrategyCompareSnapshot(externalStrategyCompareSnapshot ?? null);
  }, [externalStrategyCompareSnapshot]);

  useEffect(() => {
    let cancelled = false;
    void ensureManagedSourcePath()
      .then((nextPath) => {
        if (!cancelled) {
          setSourcePath(nextPath);
        }
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, []);

  const readDetail = useCallback(
    async (
      nextSourcePath: string,
      nextTradeDate: string,
      nextNormalizedCode: string,
    ) => {
      if (!nextSourcePath) {
        setDetailError("请先到“数据管理”页完成数据准备");
        return null;
      }

      if (nextNormalizedCode.trim() === "") {
        setDetailError("当前仅支持 6 位纯数字代码");
        return null;
      }

      setDetailLoading(true);
      setDetailError("");
      try {
        const detail = await getStockDetailPage({
          sourcePath: nextSourcePath,
          tradeDate: nextTradeDate.trim() || undefined,
          tsCode: nextNormalizedCode,
          chartWindowDays: 280,
          prevRankDays: 15,
        });

        startTransition(() => {
          setDetailData(detail);
          setDetailRealtimeData(null);
          setDetailRealtimeNotice("");
          setLookupInput(
            detail.overview?.name?.trim() ??
              getLookupDigits(detail.resolved_ts_code ?? nextNormalizedCode),
          );
        });
        return detail;
      } catch (error) {
        setDetailData(null);
        setDetailError(`读取详情失败: ${String(error)}`);
        return null;
      } finally {
        setDetailLoading(false);
      }
    },
    [],
  );

  useEffect(() => {
    if (!sourcePathTrimmed) {
      setDateOptions([]);
      setTradeDateInput(DEFAULT_DATE_OPTION);
      setStockLookupRows([]);
      setDateOptionsLoading(false);
      return;
    }

    let cancelled = false;
    const loadDateOptions = async () => {
      setDateOptionsLoading(true);
      try {
        const values = normalizeTradeDates(
          await listRankTradeDates(sourcePathTrimmed),
        );
        if (cancelled) {
          return;
        }

        setDateOptions(values);
        setTradeDateInput((current) => pickDateValue(current, values));
        setTopError("");
      } catch (error) {
        if (cancelled) {
          return;
        }

        setDateOptions([]);
        setTradeDateInput(DEFAULT_DATE_OPTION);
        setTopError(`读取日期列表失败: ${String(error)}`);
      } finally {
        if (!cancelled) {
          setDateOptionsLoading(false);
        }
      }
    };

    void loadDateOptions();
    return () => {
      cancelled = true;
    };
  }, [sourcePathTrimmed]);

  useEffect(() => {
    if (!sourcePathTrimmed) {
      setStockLookupRows([]);
      return;
    }

    let cancelled = false;
    const loadStockLookupRows = async () => {
      try {
        const rows = await listStockLookupRows(sourcePathTrimmed);
        if (!cancelled) {
          setStockLookupRows(rows);
        }
      } catch {
        if (!cancelled) {
          setStockLookupRows([]);
        }
      }
    };

    void loadStockLookupRows();
    return () => {
      cancelled = true;
    };
  }, [sourcePathTrimmed]);

  useEffect(() => {
    if (!sourcePathTrimmed || tradeDateInput.trim() === "") {
      setTopLoading(false);
      setTopRows([]);
      setTopResolvedDate("");
      setTopError("");
      return;
    }

    const topLimit = toPositiveInt(topLimitInput);
    if (topLimit === null) {
      setTopLoading(false);
      setTopRows([]);
      setTopResolvedDate("");
      setTopError("前列数量必须是正整数");
      return;
    }

    let cancelled = false;
    const loadTopRows = async () => {
      setTopLoading(true);
      setTopError("");
      try {
        const rows = await rankOverview({
          sourcePath: sourcePathTrimmed,
          tradeDate: tradeDateInput.trim() || undefined,
          limit: topLimit,
        });
        if (cancelled) {
          return;
        }

        setTopRows(rows);
        setTopResolvedDate(
          rows.find(
            (row) =>
              typeof row.trade_date === "string" &&
              row.trade_date.trim() !== "",
          )?.trade_date ?? "",
        );
      } catch (error) {
        if (cancelled) {
          return;
        }
        setTopRows([]);
        setTopResolvedDate("");
        setTopError(`读取排名前列失败: ${String(error)}`);
      } finally {
        if (!cancelled) {
          setTopLoading(false);
        }
      }
    };

    void loadTopRows();
    return () => {
      cancelled = true;
    };
  }, [sourcePathTrimmed, tradeDateInput, topLimitInput]);

  useEffect(() => {
    if (
      !autoFillTopRef.current ||
      lookupInput.trim() !== "" ||
      topRows.length === 0 ||
      sourcePathTrimmed === ""
    ) {
      return;
    }

    const firstRow = topRows[0];
    if (!firstRow) {
      return;
    }

    const nextCode = sanitizeCodeInput(splitTsCode(firstRow.ts_code));
    const nextTradeDate =
      typeof firstRow.trade_date === "string" &&
      firstRow.trade_date.trim() !== ""
        ? firstRow.trade_date.trim()
        : tradeDateInput.trim();

    if (nextCode === "") {
      return;
    }

    autoFillTopRef.current = false;
    setLookupInput(firstRow.name?.trim() || nextCode);
    setDetailError("");
    setStrategyCompareSnapshot(null);
    void readDetail(sourcePathTrimmed, nextTradeDate, stdTsCode(nextCode));
  }, [lookupInput, readDetail, sourcePathTrimmed, topRows, tradeDateInput]);

  function onReadDetail() {
    if (readTargetCode === "") {
      setDetailError(
        lookupInput.trim() !== ""
          ? "请从候选中选择股票名称，或输入 6 位代码"
          : "当前仅支持 6 位纯数字代码",
      );
      return;
    }

    autoFillTopRef.current = false;
    if (normalizedCode === "" && exactStockLookupMatch) {
      setLookupInput(exactStockLookupMatch.name);
    }
    setDetailError("");
    setStrategyCompareSnapshot(null);
    void readDetail(sourcePathTrimmed, tradeDateInput, readTargetCode);
  }

  function onAutoReadDetail(
    nextTradeDate: string,
    nextNormalizedCode: string,
    nextLookupValue?: string,
  ) {
    if (sourcePathTrimmed === "" || nextNormalizedCode.trim() === "") {
      return;
    }

    autoFillTopRef.current = false;
    if (nextLookupValue !== undefined) {
      setLookupInput(nextLookupValue);
    }
    setDetailError("");
    setStrategyCompareSnapshot(null);
    void readDetail(sourcePathTrimmed, nextTradeDate, nextNormalizedCode);
  }

  function onSelectStockCandidate(row: StockLookupRow) {
    const nextCode = getLookupDigits(row.ts_code);
    if (nextCode === "") {
      return;
    }

    setLookupFocused(false);
    onAutoReadDetail(
      tradeDateInput,
      stdTsCode(nextCode),
      row.name || nextCode,
    );
  }

  function onSelectTopRow(value: string) {
    const matchedRow = topRows.find((row) => row.ts_code === value) ?? null;
    const nextCode = sanitizeCodeInput(
      splitTsCode(matchedRow?.ts_code ?? value),
    );
    if (nextCode === "") {
      return;
    }

    const nextTradeDate =
      typeof matchedRow?.trade_date === "string" &&
      matchedRow.trade_date.trim() !== ""
        ? matchedRow.trade_date.trim()
        : tradeDateInput;

    onAutoReadDetail(
      nextTradeDate,
      stdTsCode(nextCode),
      matchedRow?.name?.trim() || nextCode,
    );
  }

  function onLookupInputChange(rawValue: string) {
    autoFillTopRef.current = false;
    setLookupFocused(true);
    setLookupInput(rawValue);
  }

  function onCandidateWheel(event: ReactWheelEvent<HTMLDivElement>) {
    const element = event.currentTarget;
    const scrollTop = element.scrollTop;
    const maxScrollTop = Math.max(
      element.scrollHeight - element.clientHeight,
      0,
    );
    const isAtTop = scrollTop <= 0;
    const isAtBottom = scrollTop >= maxScrollTop - 1;

    event.stopPropagation();
    if ((event.deltaY < 0 && isAtTop) || (event.deltaY > 0 && isAtBottom)) {
      event.preventDefault();
    }
  }

  const resolvedTradeDate =
    detailData?.resolved_trade_date ??
    (tradeDateInput.trim() || topResolvedDate || "--");
  const resolvedTsCode =
    detailData?.resolved_ts_code ?? (readTargetCode || "--");
  const currentWatchObserveItem =
    resolvedTsCode !== "--"
      ? findWatchObserveRow(watchObserveItems, resolvedTsCode)
      : null;
  const isCurrentWatched = currentWatchObserveItem !== null;
  const overviewRows = buildOverviewRows(
    detailData?.overview,
    resolvedTsCode,
    resolvedTradeDate,
  );
  const conceptItems = filterConceptItems(
    buildConceptItems(detailData?.overview?.concept),
    excludedConcepts,
  );
  const conceptText = conceptItems.length > 0 ? conceptItems.join("、") : "--";
  const watermarkConcept = buildConceptPreview(conceptItems);
  const prevRanks = detailData?.prev_ranks ?? EMPTY_PREV_RANK_ROWS;
  const strategySnapshotTradeDate =
    strategyCompareSnapshot?.tsCode === resolvedTsCode
      ? strategyCompareSnapshot.relativeTradeDate
      : "";
  const strategyCompareRowMap = useMemo(() => {
    if (
      !strategyCompareSnapshot ||
      strategyCompareSnapshot.tsCode !== resolvedTsCode
    ) {
      return null;
    }
    return new Map(
      strategyCompareSnapshot.rows.map((row) => [row.rule_name, row]),
    );
  }, [resolvedTsCode, strategyCompareSnapshot]);
  const strategyReferenceDates = useMemo(() => {
    const seen = new Set<string>();
    const out: string[] = [];
    dateOptions.forEach((value) => {
      const tradeDate = value.trim();
      if (!tradeDate || seen.has(tradeDate)) {
        return;
      }
      seen.add(tradeDate);
      out.push(tradeDate);
    });
    return out;
  }, [dateOptions]);
  const strategyReferenceDateIndex = strategyReferenceDates.findIndex(
    (value) => value === resolvedTradeDate,
  );
  const previousStrategyTradeDate =
    strategyReferenceDateIndex >= 0 &&
    strategyReferenceDateIndex < strategyReferenceDates.length - 1
      ? strategyReferenceDates[strategyReferenceDateIndex + 1]
      : null;
  const nextStrategyTradeDate =
    strategyReferenceDateIndex > 0
      ? strategyReferenceDates[strategyReferenceDateIndex - 1]
      : null;
  const strategyTriggeredRows =
    detailData?.strategy_triggers?.triggered ?? undefined;
  const strategyUntriggeredRows =
    detailData?.strategy_triggers?.untriggered ?? EMPTY_STRATEGY_ROWS;
  const strategyOutRows = useMemo(
    () =>
      strategyUntriggeredRows.filter((row) =>
        isStrategyOutRow(
          row,
          strategyCompareRowMap,
          strategySnapshotTradeDate,
          previousStrategyTradeDate,
        ),
      ),
    [
      previousStrategyTradeDate,
      strategyCompareRowMap,
      strategySnapshotTradeDate,
      strategyUntriggeredRows,
    ],
  );
  const strategyActiveRows = useMemo(
    () => [...strategyOutRows, ...(strategyTriggeredRows ?? EMPTY_STRATEGY_ROWS)],
    [strategyOutRows, strategyTriggeredRows],
  );
  const strategyIdleRows = useMemo(
    () =>
      strategyUntriggeredRows.filter(
        (row) =>
          !isStrategyOutRow(
            row,
            strategyCompareRowMap,
            strategySnapshotTradeDate,
            previousStrategyTradeDate,
          ),
      ),
    [
      previousStrategyTradeDate,
      strategyCompareRowMap,
      strategySnapshotTradeDate,
      strategyUntriggeredRows,
    ],
  );
  const strategyDisplayRelativeTradeDate =
    strategySnapshotTradeDate || previousStrategyTradeDate || "";
  const sceneFilterToken =
    sceneDetailTarget?.scene_name?.trim().toLowerCase() ?? "";
  const modalStrategyActiveRows = useMemo(() => {
    if (sceneFilterToken === "") {
      return strategyActiveRows;
    }
    return strategyActiveRows.filter((row) => {
      const sceneName = row.scene_name?.trim().toLowerCase() ?? "";
      return sceneName === sceneFilterToken;
    });
  }, [sceneFilterToken, strategyActiveRows]);
  const modalStrategyIdleRows = useMemo(() => {
    if (sceneFilterToken === "") {
      return strategyIdleRows;
    }
    return strategyIdleRows.filter((row) => {
      const sceneName = row.scene_name?.trim().toLowerCase() ?? "";
      return sceneName === sceneFilterToken;
    });
  }, [sceneFilterToken, strategyIdleRows]);

  useEffect(() => {
    if (
      sourcePathTrimmed === "" ||
      !detailData ||
      resolvedTsCode === "--" ||
      resolvedTradeDate === "--" ||
      !previousStrategyTradeDate ||
      externalStrategyCompareSnapshot
    ) {
      strategyCompareRequestKeyRef.current = "";
      return;
    }

    if (
      strategyCompareSnapshot &&
      strategyCompareSnapshot.tsCode === resolvedTsCode &&
      strategyCompareSnapshot.relativeTradeDate.trim() !== ""
    ) {
      strategyCompareRequestKeyRef.current = "";
      return;
    }

    const requestKey = [
      sourcePathTrimmed,
      resolvedTsCode,
      resolvedTradeDate,
      previousStrategyTradeDate,
    ].join("|");
    if (strategyCompareRequestKeyRef.current === requestKey) {
      return;
    }

    strategyCompareRequestKeyRef.current = requestKey;
    let cancelled = false;

    const loadStrategyCompareSnapshot = async () => {
      try {
        const compareDetail = await getStockDetailStrategySnapshot({
          sourcePath: sourcePathTrimmed,
          tradeDate: previousStrategyTradeDate,
          tsCode: resolvedTsCode,
        });
        if (cancelled || strategyCompareRequestKeyRef.current !== requestKey) {
          return;
        }

        setStrategyCompareSnapshot({
          tsCode: resolvedTsCode,
          relativeTradeDate:
            compareDetail.resolved_trade_date?.trim() || previousStrategyTradeDate,
          rows: [
            ...(compareDetail.strategy_triggers?.triggered ?? []),
            ...(compareDetail.strategy_triggers?.untriggered ?? []),
          ],
        });
      } catch {
        if (cancelled || strategyCompareRequestKeyRef.current !== requestKey) {
          return;
        }
        setStrategyCompareSnapshot(null);
      } finally {
        if (strategyCompareRequestKeyRef.current === requestKey) {
          strategyCompareRequestKeyRef.current = "";
        }
      }
    };

    void loadStrategyCompareSnapshot();
    return () => {
      cancelled = true;
    };
  }, [
    detailData,
    previousStrategyTradeDate,
    resolvedTradeDate,
    resolvedTsCode,
    sourcePathTrimmed,
    externalStrategyCompareSnapshot,
    strategyCompareSnapshot,
  ]);

  const sceneRows = useMemo(() => buildSceneStatusRows(detailData), [detailData]);
  const sceneStatusStats = useMemo(
    () => buildSceneStatusStats(sceneRows),
    [sceneRows],
  );
  const sceneTotalCount = sceneRows.length;
  const sceneRuleScoreBundle = useMemo(() => buildSceneRuleScoreMap(detailData), [detailData]);
  const sceneOverviewItems = useMemo(
    () =>
      buildSceneOverviewItems(
        sceneRows,
        sceneRuleScoreBundle.byScene,
        sceneRuleScoreBundle.assignedTotal,
      ),
    [sceneRows, sceneRuleScoreBundle],
  );

  const prevRankSortDefinitions = useMemo(
    () =>
      ({
        trade_date: { value: (row) => row.trade_date },
        rank: { value: (row) => row.rank },
      }) satisfies Partial<
        Record<PrevRankSortKey, SortDefinition<DetailPrevRankRow>>
      >,
    [],
  );
  const { sortedRows: sortedPrevRanks } = useTableSort<
    DetailPrevRankRow,
    PrevRankSortKey
  >(
    prevRanks,
    prevRankSortDefinitions,
  );
  const kline = detailRealtimeData?.kline ?? detailData?.kline;
  const allChartItems = kline?.items ?? EMPTY_KLINE_ROWS;
  const totalChartItems = allChartItems.length;
  const minVisibleBars =
    totalChartItems === 0 ? 0 : Math.min(MIN_VISIBLE_BARS, totalChartItems);
  const effectiveVisibleBarCount =
    totalChartItems === 0
      ? 0
      : clampNumber(visibleBarCount, minVisibleBars, totalChartItems);
  const maxVisibleStart = Math.max(
    totalChartItems - effectiveVisibleBarCount,
    0,
  );
  const effectiveVisibleStart = clampNumber(
    visibleStartIndex,
    0,
    maxVisibleStart,
  );
  const chartItems = allChartItems.slice(
    effectiveVisibleStart,
    effectiveVisibleStart + effectiveVisibleBarCount,
  );
  const panels = kline?.panels?.length ? kline.panels : buildDefaultPanels();
  const chartShellHeight = Math.max(
    kline?.chart_height ?? DEFAULT_CHART_HEIGHT,
    DEFAULT_CHART_HEIGHT,
  );
  const watermarkName =
    kline?.watermark_name ?? detailData?.overview?.name ?? "个股详情";
  const watermarkCode = kline?.watermark_code ?? splitTsCode(resolvedTsCode);
  const matchedTopDate = topResolvedDate || "--";
  const defaultNavigationItems = useMemo(
    () =>
      topRows.map((row) =>
        buildNavigationItemFromOverviewRow(row, tradeDateInput, sourcePathTrimmed),
      ),
    [sourcePathTrimmed, topRows, tradeDateInput],
  );
  const activeNavigationItems =
    linkedNavigationItems.length > 0
      ? linkedNavigationItems
      : defaultNavigationItems;
  const currentNavigationIndex = findNavigationIndex(
    activeNavigationItems,
    resolvedTsCode,
    resolvedTradeDate,
  );
  const prevNavigationItem =
    currentNavigationIndex > 0
      ? activeNavigationItems[currentNavigationIndex - 1]
      : null;
  const nextNavigationItem =
    currentNavigationIndex >= 0 &&
    currentNavigationIndex < activeNavigationItems.length - 1
      ? activeNavigationItems[currentNavigationIndex + 1]
      : null;
  const rankLookup = buildRankLookup(detailData?.overview, prevRanks);
  const chartRangeText =
    chartItems.length > 0
      ? `${chartItems[0].trade_date} -> ${chartItems[chartItems.length - 1].trade_date}`
      : "--";
  const strategyGridStyle = useMemo(
    () =>
      isStrategyStacked
        ? undefined
        : {
            gridTemplateColumns: `minmax(0, ${strategySplitRatio}fr) 14px minmax(0, ${1 - strategySplitRatio}fr)`,
          },
    [isStrategyStacked, strategySplitRatio],
  );

  useEffect(() => {
    if (totalChartItems === 0) {
      chartDragRef.current = null;
      setVisibleBarCount(DEFAULT_VISIBLE_BARS);
      setVisibleStartIndex(0);
      setChartFocus(null);
      return;
    }

    const nextVisibleBarCount = clampNumber(
      DEFAULT_VISIBLE_BARS,
      Math.min(MIN_VISIBLE_BARS, totalChartItems),
      totalChartItems,
    );
    const referenceIndex = allChartItems.findIndex(
      (item) => item.trade_date === resolvedTradeDate,
    );
    const nextVisibleStart =
      referenceIndex >= 0
        ? clampNumber(
            referenceIndex - Math.floor(nextVisibleBarCount / 2),
            0,
            totalChartItems - nextVisibleBarCount,
          )
        : Math.max(totalChartItems - nextVisibleBarCount, 0);
    chartDragRef.current = null;
    setVisibleBarCount(nextVisibleBarCount);
    setVisibleStartIndex(nextVisibleStart);
    setChartFocus(null);
  }, [
    allChartItems,
    detailData?.resolved_trade_date,
    detailData?.resolved_ts_code,
    resolvedTradeDate,
    totalChartItems,
  ]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    const updateStrategyLayoutMode = () => {
      const grid = strategyGridRef.current;
      const gridWidth = grid?.getBoundingClientRect().width ?? window.innerWidth;
      const isPortraitViewport = window.innerHeight > window.innerWidth;
      setIsStrategyStacked(
        gridWidth <= STRATEGY_STACK_BREAKPOINT ||
          (isPortraitViewport && gridWidth <= 1360),
      );
    };

    updateStrategyLayoutMode();
    window.addEventListener("resize", updateStrategyLayoutMode);

    const grid = strategyGridRef.current;
    const observer =
      typeof ResizeObserver === "undefined" || !grid
        ? null
        : new ResizeObserver(() => {
            updateStrategyLayoutMode();
          });
    if (observer && grid) {
      observer.observe(grid);
    }

    return () => {
      window.removeEventListener("resize", updateStrategyLayoutMode);
      observer?.disconnect();
    };
  }, []);

  useEffect(() => {
    if (!isStrategyStacked || !strategySplitDragging) {
      return;
    }

    strategyResizePointerIdRef.current = null;
    setStrategySplitDragging(false);
  }, [isStrategyStacked, strategySplitDragging]);

  useEffect(() => {
    if (!strategySplitDragging) {
      return;
    }

    const onPointerMove = (event: PointerEvent) => {
      if (
        strategyResizePointerIdRef.current !== null &&
        event.pointerId !== strategyResizePointerIdRef.current
      ) {
        return;
      }

      const grid = strategyGridRef.current;
      if (!grid) {
        return;
      }

      const rect = grid.getBoundingClientRect();
      if (rect.width <= 0) {
        return;
      }

      const nextRatio = clampStrategySplitRatio(
        (event.clientX - rect.left) / rect.width,
      );
      setStrategySplitRatio(nextRatio);
    };

    const stopDragging = (event?: PointerEvent) => {
      if (
        event &&
        strategyResizePointerIdRef.current !== null &&
        event.pointerId !== strategyResizePointerIdRef.current
      ) {
        return;
      }

      strategyResizePointerIdRef.current = null;
      setStrategySplitDragging(false);
    };

    window.addEventListener("pointermove", onPointerMove);
    window.addEventListener("pointerup", stopDragging);
    window.addEventListener("pointercancel", stopDragging);

    return () => {
      window.removeEventListener("pointermove", onPointerMove);
      window.removeEventListener("pointerup", stopDragging);
      window.removeEventListener("pointercancel", stopDragging);
    };
  }, [strategySplitDragging]);

  useEffect(() => {
    const row = currentRankRowRef.current;
    const container = rankTableWrapRef.current;
    if (!row || !container) {
      return;
    }

    const rowRect = row.getBoundingClientRect();
    const containerRect = container.getBoundingClientRect();
    const rowTop = rowRect.top - containerRect.top + container.scrollTop;
    const maxScrollTop = Math.max(
      container.scrollHeight - container.clientHeight,
      0,
    );
    const nextScrollTop = Math.min(
      Math.max(rowTop - container.clientHeight / 2 + rowRect.height / 2, 0),
      maxScrollTop,
    );
    container.scrollTo({ top: nextScrollTop });
  }, [resolvedTradeDate, sortedPrevRanks]);

  useLayoutEffect(() => {
    if (detailLoading) {
      return;
    }

    const pendingScroll = pendingPageScrollRef.current;
    if (!pendingScroll) {
      return;
    }

    const contentElement = getContentScrollElement();
    if (contentElement) {
      contentElement.scrollTo({
        left: pendingScroll.left,
        top: pendingScroll.top,
      });
    } else {
      window.scrollTo(pendingScroll.left, pendingScroll.top);
    }
    pendingPageScrollRef.current = null;
  }, [
    detailLoading,
    detailData?.resolved_trade_date,
    detailData?.resolved_ts_code,
  ]);

  useEffect(() => {
    let cancelled = false;

    const syncWatchObserveItems = async () => {
      try {
        const nextItems = await listWatchObserveRows(sourcePathTrimmed);
        if (!cancelled) {
          setWatchObserveItems(nextItems);
        }
      } catch {
        if (!cancelled) {
          setWatchObserveItems([]);
        }
      }
    };

    void syncWatchObserveItems();
    const handleFocus = () => {
      void syncWatchObserveItems();
    };

    window.addEventListener("focus", handleFocus);
    return () => {
      cancelled = true;
      window.removeEventListener("focus", handleFocus);
    };
  }, [sourcePathTrimmed]);

  useEffect(() => {
    setWatchObserveNotice("");
  }, [resolvedTsCode, resolvedTradeDate]);

  useEffect(() => {
    setDetailRealtimeData(null);
    setDetailRealtimeNotice("");
  }, [detailData?.resolved_trade_date, detailData?.resolved_ts_code]);

  useEffect(() => {
    return () => {
      if (detailRealtimeLongPressTimerRef.current !== null) {
        window.clearTimeout(detailRealtimeLongPressTimerRef.current);
      }
    };
  }, []);

  useEffect(() => {
    if (routeTsCode === "") {
      return;
    }

    const nextSourcePath =
      routeSourcePath || sourcePathTrimmed || readStoredSourcePath().trim();
    if (nextSourcePath === "") {
      return;
    }

    autoFillTopRef.current = false;
    setSourcePath(nextSourcePath);
    setTradeDateInput(routeTradeDate);
    setLookupInput(routeTsCode);
    setTopError("");
    setDetailError("");
    setStrategyCompareSnapshot(null);
    void readDetail(nextSourcePath, routeTradeDate, stdTsCode(routeTsCode));
  }, [
    readDetail,
    routeSourcePath,
    routeTradeDate,
    routeTsCode,
    sourcePathTrimmed,
  ]);

  function setChartZoom(nextCount: number) {
    if (totalChartItems === 0) {
      return;
    }

    const resolvedCount = clampNumber(
      nextCount,
      minVisibleBars,
      totalChartItems,
    );
    const currentEnd = effectiveVisibleStart + effectiveVisibleBarCount;
    setVisibleBarCount(resolvedCount);
    setVisibleStartIndex(
      clampNumber(
        currentEnd - resolvedCount,
        0,
        totalChartItems - resolvedCount,
      ),
    );
  }

  function buildChartFocus(
    panelKey: string,
    viewport: HTMLDivElement,
    clientX: number,
    clientY: number,
    pinned: boolean,
  ) {
    const pointer = buildChartPointerSnapshot(
      viewport,
      clientX,
      clientY,
      chartItems.length,
    );
    if (!pointer) {
      return null;
    }

    return {
      absoluteIndex: effectiveVisibleStart + pointer.visibleIndex,
      panelKey,
      cursorXPercent: pointer.cursorXPercent,
      cursorYPercent: pointer.cursorYPercent,
      pinned,
    };
  }

  function onChartPointerDown(
    panelKey: string,
    event: ReactPointerEvent<HTMLDivElement>,
  ) {
    if (event.pointerType === "mouse" && event.button !== 0) {
      return;
    }

    const rect = event.currentTarget.getBoundingClientRect();
    if (rect.width <= 0) {
      return;
    }

    const isTouchPointer = event.pointerType !== "mouse";
    const mode = chartFocus?.pinned
      ? isTouchPointer &&
        !isPointerNearChartFocus(
          panelKey,
          event.currentTarget,
          event.clientX,
          event.clientY,
          chartFocus,
        )
        ? "dismiss"
        : "focus"
      : maxVisibleStart > 0
        ? "pan"
        : "tap";

    try {
      event.currentTarget.setPointerCapture(event.pointerId);
    } catch {
    }

    chartDragRef.current = {
      pointerId: event.pointerId,
      panelKey,
      mode,
      startClientX: event.clientX,
      startClientY: event.clientY,
      startVisibleStart: effectiveVisibleStart,
      barsPerPixel: effectiveVisibleBarCount / rect.width,
      maxVisibleStart,
      moved: false,
    };
  }

  function onChartPointerMove(
    panelKey: string,
    event: ReactPointerEvent<HTMLDivElement>,
  ) {
    const dragState = chartDragRef.current;
    if (!dragState) {
      if (event.pointerType !== "mouse" || !chartFocus?.pinned) {
        return;
      }

      const nextFocus = buildChartFocus(
        panelKey,
        event.currentTarget,
        event.clientX,
        event.clientY,
        true,
      );
      if (!nextFocus) {
        return;
      }

      setChartFocus(nextFocus);
      return;
    }

    if (
      dragState.pointerId !== event.pointerId ||
      dragState.panelKey !== panelKey
    ) {
      return;
    }

    const moveDistance = Math.hypot(
      event.clientX - dragState.startClientX,
      event.clientY - dragState.startClientY,
    );
    if (!dragState.moved && moveDistance >= CHART_POINTER_DRAG_THRESHOLD) {
      dragState.moved = true;
    }

    if (dragState.mode === "pan") {
      if (!dragState.moved) {
        return;
      }

      const deltaBars = Math.round(
        (event.clientX - dragState.startClientX) * dragState.barsPerPixel,
      );
      const nextVisibleStart = clampNumber(
        dragState.startVisibleStart - deltaBars,
        0,
        dragState.maxVisibleStart,
      );

      setVisibleStartIndex(nextVisibleStart);
      return;
    }

    if (dragState.mode === "dismiss") {
      return;
    }

    if (dragState.mode !== "focus" || !dragState.moved) {
      return;
    }

    const nextFocus = buildChartFocus(
      panelKey,
      event.currentTarget,
      event.clientX,
      event.clientY,
      true,
    );
    if (!nextFocus) {
      return;
    }

    setChartFocus(nextFocus);
  }

  function clearChartPointerState(event: ReactPointerEvent<HTMLDivElement>) {
    const dragState = chartDragRef.current;
    if (dragState?.pointerId === event.pointerId) {
      try {
        if (event.currentTarget.hasPointerCapture(event.pointerId)) {
          event.currentTarget.releasePointerCapture(event.pointerId);
        }
      } catch {
      }
    }
    chartDragRef.current = null;
  }

  function onChartPointerUp(
    panelKey: string,
    event: ReactPointerEvent<HTMLDivElement>,
  ) {
    const dragState = chartDragRef.current;
    clearChartPointerState(event);

    if (
      !dragState ||
      dragState.pointerId !== event.pointerId ||
      dragState.panelKey !== panelKey
    ) {
      return;
    }

    if (dragState.mode === "dismiss") {
      if (!dragState.moved) {
        setChartFocus(null);
      }
      return;
    }

    if (dragState.moved) {
      return;
    }

    const nextFocus = buildChartFocus(
      panelKey,
      event.currentTarget,
      event.clientX,
      event.clientY,
      true,
    );
    if (!nextFocus) {
      if (chartFocus?.pinned) {
        setChartFocus(null);
      }
      return;
    }

    if (
      chartFocus?.pinned &&
      chartFocus.panelKey === nextFocus.panelKey &&
      chartFocus.absoluteIndex === nextFocus.absoluteIndex
    ) {
      setChartFocus(null);
      return;
    }

    setChartFocus(nextFocus);
  }

  function onChartPointerLeave(
    panelKey: string,
    event: ReactPointerEvent<HTMLDivElement>,
  ) {
    const dragState = chartDragRef.current;
    if (
      dragState?.pointerId === event.pointerId &&
      dragState.panelKey === panelKey
    ) {
      return;
    }

    if (!chartFocus?.pinned) {
      setChartFocus(null);
    }
  }

  function onChartPointerCancel(
    _panelKey: string,
    event: ReactPointerEvent<HTMLDivElement>,
  ) {
    clearChartPointerState(event);
  }

  async function onAddWatchObserve() {
    if (resolvedTsCode === "--") {
      return;
    }

    try {
      if (currentWatchObserveItem) {
        const nextItems = await removeWatchObserveRows(
          [resolvedTsCode],
          sourcePathTrimmed,
        );
        setWatchObserveItems(nextItems);
        setWatchObserveNotice("");
        return;
      }

      if (resolvedTradeDate === "--") {
        return;
      }

      const nextItems = await upsertWatchObserveRow(
        {
          tsCode: resolvedTsCode,
          addedDate: resolvedTradeDate,
          tradeDate: resolvedTradeDate,
          name: detailData?.overview?.name,
          concept: detailData?.overview?.concept ?? "",
        },
        sourcePathTrimmed,
      );

      setWatchObserveItems(nextItems);
      setWatchObserveNotice("");
    } catch {
      setWatchObserveNotice(
        currentWatchObserveItem ? "取消自选失败" : "加入自选失败",
      );
    }
  }

  const clearDetailRealtimeLongPressTimer = useCallback(() => {
    if (detailRealtimeLongPressTimerRef.current !== null) {
      window.clearTimeout(detailRealtimeLongPressTimerRef.current);
      detailRealtimeLongPressTimerRef.current = null;
    }
  }, []);

  const onRefreshRealtimeDetail = useCallback(async () => {
    if (resolvedTsCode === "--" || sourcePathTrimmed === "") {
      return;
    }

    setDetailRealtimeLoading(true);
    setDetailRealtimeNotice("");
    try {
      const nextRealtimeData = await getStockDetailRealtime({
        sourcePath: sourcePathTrimmed,
        tsCode: resolvedTsCode,
        chartWindowDays: 280,
      });
      setDetailRealtimeData(nextRealtimeData);
    } catch (error) {
      setDetailRealtimeNotice(`刷新实时失败: ${String(error)}`);
    } finally {
      setDetailRealtimeLoading(false);
    }
  }, [resolvedTsCode, sourcePathTrimmed]);

  const toggleDetailRealtimePinned = useCallback(() => {
    let nextPinned = false;
    setDetailRealtimePinned((current) => {
      nextPinned = !current;
      return nextPinned;
    });
    detailRealtimeLongPressHandledRef.current = true;
    if (nextPinned) {
      void onRefreshRealtimeDetail();
    }
  }, [onRefreshRealtimeDetail]);

  const handleDetailRealtimeRefreshPointerDown = useCallback(
    (event: ReactPointerEvent<HTMLButtonElement>) => {
      event.stopPropagation();
      if (resolvedTsCode === "--" || detailRealtimeLoading) {
        return;
      }

      detailRealtimeLongPressHandledRef.current = false;
      clearDetailRealtimeLongPressTimer();
      detailRealtimeLongPressTimerRef.current = window.setTimeout(() => {
        toggleDetailRealtimePinned();
      }, DETAIL_REALTIME_LONG_PRESS_MS);
    },
    [
      clearDetailRealtimeLongPressTimer,
      detailRealtimeLoading,
      resolvedTsCode,
      toggleDetailRealtimePinned,
    ],
  );

  const handleDetailRealtimeRefreshPointerRelease = useCallback(
    (event: ReactPointerEvent<HTMLButtonElement>) => {
      event.stopPropagation();
      clearDetailRealtimeLongPressTimer();
    },
    [clearDetailRealtimeLongPressTimer],
  );

  useEffect(() => {
    if (!detailRealtimePinned) {
      detailRealtimeAutoRefreshKeyRef.current = "";
      return;
    }

    const nextResolvedTsCode = detailData?.resolved_ts_code?.trim() ?? "";
    const nextResolvedTradeDate = detailData?.resolved_trade_date?.trim() ?? "";
    if (nextResolvedTsCode === "" || sourcePathTrimmed === "") {
      detailRealtimeAutoRefreshKeyRef.current = "";
      return;
    }

    const nextAutoRefreshKey = [
      sourcePathTrimmed,
      nextResolvedTsCode,
      nextResolvedTradeDate,
    ].join("|");
    if (detailRealtimeAutoRefreshKeyRef.current === nextAutoRefreshKey) {
      return;
    }

    detailRealtimeAutoRefreshKeyRef.current = nextAutoRefreshKey;
    void onRefreshRealtimeDetail();
  }, [
    detailRealtimePinned,
    detailData?.resolved_trade_date,
    detailData?.resolved_ts_code,
    onRefreshRealtimeDetail,
    sourcePathTrimmed,
  ]);

  useEffect(() => {
    if (!detailRealtimePinned) {
      return;
    }
    if (resolvedTsCode === "--" || sourcePathTrimmed === "") {
      setDetailRealtimePinned(false);
      return;
    }

    const intervalId = window.setInterval(() => {
      if (!detailRealtimeLoading) {
        void onRefreshRealtimeDetail();
      }
    }, DETAIL_REALTIME_AUTO_REFRESH_INTERVAL_MS);

    return () => {
      window.clearInterval(intervalId);
    };
  }, [
    detailRealtimeLoading,
    detailRealtimePinned,
    onRefreshRealtimeDetail,
    resolvedTsCode,
    sourcePathTrimmed,
  ]);

  function onJumpStrategyTradeDate(nextTradeDate: string | null) {
    if (
      !nextTradeDate ||
      sourcePathTrimmed === "" ||
      resolvedTsCode === "--" ||
      detailLoading
    ) {
      return;
    }

    const contentElement = getContentScrollElement();
    pendingPageScrollRef.current = contentElement
      ? { left: contentElement.scrollLeft, top: contentElement.scrollTop }
      : { left: window.scrollX, top: window.scrollY };
    const nextCompareSnapshot =
      detailData && resolvedTradeDate !== "--"
        ? {
            tsCode: resolvedTsCode,
            relativeTradeDate: resolvedTradeDate,
            rows: collectStrategyRows(detailData),
          }
        : null;
    autoFillTopRef.current = false;
    setTradeDateInput(nextTradeDate);
    setLookupInput(detailData?.overview?.name?.trim() || splitTsCode(resolvedTsCode));
    setDetailError("");
    void (async () => {
      const nextDetail = await readDetail(
        sourcePathTrimmed,
        nextTradeDate,
        resolvedTsCode,
      );
      if (nextDetail && nextCompareSnapshot) {
        setStrategyCompareSnapshot(nextCompareSnapshot);
      }
    })();
  }

  function onJumpNavigationTarget(target: DetailsNavigationItem | null) {
    const nextSourcePath = target?.sourcePath?.trim() || sourcePathTrimmed;
    if (!target || nextSourcePath === "") {
      return;
    }

    const nextCode = sanitizeCodeInput(splitTsCode(target.tsCode));
    const nextTradeDate =
      target.tradeDate?.trim() || tradeDateInput.trim();

    if (nextCode === "") {
      return;
    }

    const contentElement = getContentScrollElement();
    pendingPageScrollRef.current = contentElement
      ? { left: contentElement.scrollLeft, top: contentElement.scrollTop }
      : { left: window.scrollX, top: window.scrollY };
    autoFillTopRef.current = false;
    setSourcePath(nextSourcePath);
    setLookupInput(target.name?.trim() || nextCode);
    if (nextTradeDate !== "") {
      setTradeDateInput(nextTradeDate);
    }
    setDetailError("");
    setStrategyCompareSnapshot(null);
    void readDetail(nextSourcePath, nextTradeDate, stdTsCode(nextCode));
  }

  function onStrategyResizePointerDown(event: ReactPointerEvent<HTMLDivElement>) {
    if (event.pointerType === "mouse" && event.button !== 0) {
      return;
    }

    event.preventDefault();
    strategyResizePointerIdRef.current = event.pointerId;
    setStrategySplitDragging(true);
  }

  return (
    <div
      className={
        isLinkedOverlay
          ? "details-page details-page-linked-overlay"
          : "details-page"
      }
    >
      {isLinkedOverlay ? null : (
        <section className="details-card details-query-card">
          <h2 className="details-title">个股详情</h2>
          <div className="details-source-note">
            数据目录由“数据管理”页统一管理，当前路径：
            {sourcePathTrimmed || "读取中..."}
          </div>

          <div className="details-form-grid">
            <label className="details-field">
              <span>参考日</span>
              <select
                value={tradeDateInput}
                onChange={(event) => {
                  const nextTradeDate = event.target.value;
                  setTradeDateInput(nextTradeDate);

                  const nextTargetCode =
                    readTargetCode ||
                    (resolvedTsCode !== "--" ? resolvedTsCode : "");
                  if (nextTargetCode !== "") {
                    onAutoReadDetail(
                      nextTradeDate,
                      nextTargetCode,
                      lookupInput,
                    );
                  }
                }}
                disabled={dateOptionsLoading || dateOptions.length === 0}
              >
                {dateOptions.length === 0 ? (
                  <option value="">
                    {dateOptionsLoading ? "读取日期中..." : "暂无可选日期"}
                  </option>
                ) : null}
                {dateOptions.map((option) => (
                  <option key={option} value={option}>
                    {option}
                  </option>
                ))}
              </select>
            </label>

            <label className="details-field">
              <span>前列数量</span>
              <input
                type="number"
                min={1}
                step={1}
                value={topLimitInput}
                onChange={(event) => setTopLimitInput(event.target.value)}
                placeholder={DEFAULT_TOP_LIMIT}
              />
            </label>

            <label className="details-field details-field-span-2">
              <span>代码/名称输入，预览代码：{readTargetCode || "--"}</span>
              <div className="details-autocomplete">
                <input
                  type="text"
                  value={lookupInput}
                  onChange={(event) => onLookupInputChange(event.target.value)}
                  onFocus={() => setLookupFocused(true)}
                  onBlur={() => setLookupFocused(false)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter" && stockNameCandidates.length > 0) {
                      event.preventDefault();
                      onSelectStockCandidate(stockNameCandidates[0]);
                    }
                  }}
                  placeholder="输入股票名称、代码或拼音首字母，支持候选补全"
                />
                {showStockNameCandidates ? (
                  <div
                    className="details-autocomplete-menu"
                    onWheel={onCandidateWheel}
                  >
                    {stockNameCandidates.map((row) => {
                      const code = getLookupDigits(row.ts_code);
                      return (
                        <button
                          className="details-autocomplete-option"
                          key={row.ts_code}
                          type="button"
                          onMouseDown={(event) => {
                            event.preventDefault();
                            onSelectStockCandidate(row);
                          }}
                        >
                          <strong>{row.name}</strong>
                          <span>{code || row.ts_code}</span>
                        </button>
                      );
                    })}
                  </div>
                ) : null}
              </div>
            </label>

            <label className="details-field details-field-span-2">
              <span>从排名前列选择</span>
              <div className="details-inline-row">
                <select
                  value={selectedTopValue}
                  onChange={(event) => onSelectTopRow(event.target.value)}
                >
                  <option value="">请选择</option>
                  {topRows.map((row) => (
                    <option key={row.ts_code} value={row.ts_code}>
                      {buildTopOptionLabel(row)}
                    </option>
                  ))}
                </select>
                <button
                  className="details-primary-btn details-primary-btn-alt"
                  type="button"
                  disabled={
                    detailLoading ||
                    sourcePathTrimmed === "" ||
                    readTargetCode === ""
                  }
                  onClick={onReadDetail}
                >
                  {detailLoading ? "读取详情中..." : "读取详情"}
                </button>
              </div>
              <small>
                候选来源日期: {matchedTopDate}
                {topLoading ? "（更新中...）" : ""}
              </small>
            </label>
          </div>
          {topError ? <div className="details-error">{topError}</div> : null}
          {detailError ? (
            <div className="details-error">{detailError}</div>
          ) : null}
        </section>
      )}

      {isLinkedOverlay && topError ? (
        <div className="details-error">{topError}</div>
      ) : null}
      {isLinkedOverlay && detailError ? (
        <div className="details-error">{detailError}</div>
      ) : null}

      <div className="details-overview-grid">
        <section className="details-card details-overview-card">
          <h3 className="details-subtitle">总览</h3>
          <div className="details-overview-card-body">
            {renderFieldGrid(overviewRows)}
            <div className="details-concept-block">
              <div className="details-concept-head">
                <strong>概念</strong>
                <span>
                  {conceptItems.length > 0
                    ? `${conceptItems.length} 项`
                    : "暂无概念信息"}
                </span>
              </div>
              <div className="details-concept-panel">
                {conceptItems.length > 0 ? (
                  <div className="details-concept-text" title={conceptText}>
                    {conceptText}
                  </div>
                ) : (
                  <div className="details-empty details-empty-soft">
                    暂无概念信息
                  </div>
                )}
              </div>
            </div>
          </div>
        </section>

        <section className="details-card details-rank-card">
          <h3 className="details-subtitle">Scene 状态总览</h3>
          <div className="details-rank-card-body details-scene-overview-card-body">
            {sceneTotalCount === 0 ? (
              <div className="details-empty details-empty-soft">暂无 scene 状态数据</div>
            ) : (
              <>
                <div className="details-scene-charts-row">
                  <div className="details-scene-status-panel">
                    <h4 className="details-scene-panel-title">状态总览</h4>
                    <div className="details-scene-donut-wrap">
                      <svg
                        className="details-scene-donut"
                        viewBox="0 0 120 120"
                        role="img"
                        aria-label="scene 状态占比"
                      >
                        <circle cx="60" cy="60" r="44" fill="none" stroke="#e7edf4" strokeWidth="16" />
                        {(() => {
                          const radius = 44;
                          const strokeWidth = 16;
                          const center = 60;
                          const circumference = 2 * Math.PI * radius;
                          let offset = 0;

                          return sceneStatusStats
                            .filter((item) => item.count > 0)
                            .map((item) => {
                              const dash = (item.ratio / 100) * circumference;
                              const node = (
                                <circle
                                  key={item.key}
                                  cx={center}
                                  cy={center}
                                  r={radius}
                                  fill="none"
                                  stroke={item.color}
                                  strokeWidth={strokeWidth}
                                  strokeDasharray={`${dash} ${Math.max(circumference - dash, 0)}`}
                                  strokeDashoffset={-offset}
                                  strokeLinecap="butt"
                                  transform="rotate(-90 60 60)"
                                />
                              );
                              offset += dash;
                              return node;
                            });
                        })()}
                        <circle cx="60" cy="60" r="34" fill="#ffffff" />
                        <text x="60" y="56" textAnchor="middle" className="details-scene-donut-total-label">
                          总场景
                        </text>
                        <text x="60" y="73" textAnchor="middle" className="details-scene-donut-total-value">
                          {sceneTotalCount}
                        </text>
                      </svg>

                      <div className="details-scene-donut-legend">
                        {sceneStatusStats.map((item) => (
                          <div className="details-scene-donut-legend-item" key={item.key}>
                            <span
                              className="details-scene-donut-legend-dot"
                              style={{ backgroundColor: item.color }}
                            />
                            <strong>{item.label}</strong>
                            <span>{item.count}</span>
                            <span>{item.ratio.toFixed(1)}%</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  </div>

                  <div className="details-scene-contrib-panel">
                    <h4 className="details-scene-panel-title">Scene贡献占比</h4>
                    <div className="details-scene-donut-wrap">
                      <svg
                        className="details-scene-donut"
                        viewBox="0 0 120 120"
                        role="img"
                        aria-label="Scene贡献占比环图"
                      >
                        <circle cx="60" cy="60" r="44" fill="none" stroke="#e7edf4" strokeWidth="16" />
                        {(() => {
                          const radius = 44;
                          const strokeWidth = 16;
                          const center = 60;
                          const circumference = 2 * Math.PI * radius;
                          let offset = 0;

                          return sceneOverviewItems
                            .filter((item) => (item.contributionPctDisplay ?? 0) > 0)
                            .map((item) => {
                              const dash = ((item.contributionPctDisplay ?? 0) / 100) * circumference;
                              const node = (
                                <circle
                                  key={`${item.sceneName}-contrib-ring`}
                                  cx={center}
                                  cy={center}
                                  r={radius}
                                  fill="none"
                                  stroke={item.color}
                                  strokeWidth={strokeWidth}
                                  strokeDasharray={`${dash} ${Math.max(circumference - dash, 0)}`}
                                  strokeDashoffset={-offset}
                                  strokeLinecap="butt"
                                  transform="rotate(-90 60 60)"
                                />
                              );
                              offset += dash;
                              return node;
                            });
                        })()}
                        <circle cx="60" cy="60" r="34" fill="#ffffff" />
                        <text x="60" y="56" textAnchor="middle" className="details-scene-donut-total-label">
                          贡献场景
                        </text>
                        <text x="60" y="73" textAnchor="middle" className="details-scene-donut-total-value">
                          {sceneOverviewItems.length}
                        </text>
                      </svg>

                      <div className="details-scene-contrib-legend">
                        {sceneOverviewItems.map((item) => (
                          <div
                            className="details-scene-contrib-legend-item"
                            key={`${item.sceneName}-contrib`}
                          >
                            <span
                              className="details-scene-donut-legend-dot"
                              style={{ backgroundColor: item.color }}
                            />
                            <strong title={item.sceneName}>{item.sceneName}</strong>
                            <span>
                              {item.contributionPctDisplay === null
                                ? '--'
                                : `${item.contributionPctDisplay.toFixed(1)}%`}
                            </span>
                          </div>
                        ))}
                      </div>
                    </div>
                  </div>
                </div>

                <div className="details-scene-overview-table-wrap">
                  <table className="details-table details-table-scene-overview">
                    <thead>
                      <tr>
                        <th>Scene</th>
                        <th>场景状态</th>
                        <th>全市场截面排名</th>
                        <th>scene得分</th>
                        <th>scene贡献占比</th>
                      </tr>
                    </thead>
                    <tbody>
                      {sceneOverviewItems.map((item) => (
                        <tr key={item.sceneName}>
                          <td>{formatFieldValue(item.sceneName)}</td>
                          <td>
                            <span className={`details-scene-stage-chip is-${getSceneStageToken(item.stage)}`}>
                              {toSceneStageLabel(item.stage)}
                            </span>
                          </td>
                          <td>{item.sceneRank === null ? "--" : `#${item.sceneRank}`}</td>
                          <td>{formatFieldValue(item.sceneRuleScore)}</td>
                          <td>
                            {item.contributionPctDisplay === null
                              ? "--"
                              : `${item.contributionPctDisplay.toFixed(1)}%`}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

              </>
            )}
          </div>
        </section>
      </div>

      <section className="details-card details-chart-card">
        <h3 className="details-subtitle">K线图</h3>

        <div className="details-chart-toolbar">
          <label className="details-chart-slider-field">
            <span>缩放</span>
            <input
              type="range"
              min={minVisibleBars || 1}
              max={Math.max(totalChartItems, 1)}
              step={1}
              value={Math.max(effectiveVisibleBarCount, 1)}
              onChange={(event) => setChartZoom(Number(event.target.value))}
              disabled={totalChartItems === 0}
            />
            <strong>{effectiveVisibleBarCount || 0} 根</strong>
          </label>

          <label className="details-chart-slider-field details-chart-slider-field-wide">
            <span>平移</span>
            <input
              type="range"
              min={0}
              max={Math.max(maxVisibleStart, 1)}
              step={1}
              value={effectiveVisibleStart}
              onChange={(event) =>
                setVisibleStartIndex(Number(event.target.value))
              }
              disabled={totalChartItems === 0 || maxVisibleStart === 0}
            />
            <strong>{chartRangeText}</strong>
          </label>
        </div>

        <div
          className="details-chart-shell"
          style={{
            height: `${chartShellHeight}px`,
            gridTemplateRows: buildChartTemplateRows(kline),
          }}
        >
          {panels.map((panel, index) =>
            renderChartPanel(
              panel,
              chartItems,
              index,
              panels.length,
              watermarkName,
              watermarkCode,
              watermarkConcept,
              chartFocus,
              effectiveVisibleStart,
              allChartItems,
              resolvedTradeDate !== "--" ? resolvedTradeDate : null,
              rankLookup,
              onChartPointerDown,
              onChartPointerMove,
              onChartPointerUp,
              onChartPointerLeave,
              onChartPointerCancel,
              <div className="details-chart-watch-action">
                <div className="details-chart-watch-row">
                  <span className="details-chart-watch-time">
                    {detailRealtimeData?.refreshedAt ?? "未刷新"}
                  </span>
                  <button
                    className={[
                      "details-chart-watch-btn",
                      "details-chart-watch-btn-refresh",
                      detailRealtimePinned ? "is-fixed" : "",
                    ]
                      .filter(Boolean)
                      .join(" ")}
                    type="button"
                    disabled={resolvedTsCode === "--" || detailRealtimeLoading}
                    title={
                      detailRealtimePinned
                        ? `固定自动刷新中（${formatAutoRefreshSeconds(DETAIL_REALTIME_AUTO_REFRESH_INTERVAL_MS)} / 次），长按取消`
                        : `点击立即刷新，长按固定自动刷新（${formatAutoRefreshSeconds(DETAIL_REALTIME_AUTO_REFRESH_INTERVAL_MS)} / 次）`
                    }
                    onPointerDown={handleDetailRealtimeRefreshPointerDown}
                    onPointerUp={handleDetailRealtimeRefreshPointerRelease}
                    onPointerLeave={handleDetailRealtimeRefreshPointerRelease}
                    onPointerCancel={handleDetailRealtimeRefreshPointerRelease}
                    onMouseDown={(event) => {
                      event.stopPropagation();
                    }}
                    onClick={(event) => {
                      event.stopPropagation();
                      if (detailRealtimeLongPressHandledRef.current) {
                        detailRealtimeLongPressHandledRef.current = false;
                        return;
                      }
                      void onRefreshRealtimeDetail();
                    }}
                  >
                    {detailRealtimeLoading
                      ? "刷新中..."
                      : detailRealtimePinned
                        ? "固定刷新中"
                        : "刷新实时"}
                  </button>
                  <button
                    className={[
                      "details-chart-watch-btn",
                      isCurrentWatched ? "is-added" : "",
                    ]
                      .filter(Boolean)
                      .join(" ")}
                    type="button"
                    disabled={
                      resolvedTsCode === "--" ||
                      (!isCurrentWatched && resolvedTradeDate === "--")
                    }
                    title={
                      isCurrentWatched
                        ? `当前自选日 ${currentWatchObserveItem?.addedDate ?? "--"}`
                        : `加入自选`
                    }
                    onPointerDown={(event) => {
                      event.stopPropagation();
                    }}
                    onPointerUp={(event) => {
                      event.stopPropagation();
                    }}
                    onMouseDown={(event) => {
                      event.stopPropagation();
                    }}
                    onClick={(event) => {
                      event.stopPropagation();
                      void onAddWatchObserve();
                    }}
                  >
                    {isCurrentWatched ? "取消自选" : "加自选"}
                  </button>
                </div>
                {detailRealtimeNotice ? (
                  <span className="details-chart-watch-note">
                    {detailRealtimeNotice}
                  </span>
                ) : null}
                {detailRealtimePinned ? (
                  <span className="details-chart-watch-note">
                    固定自动刷新中 {formatAutoRefreshSeconds(DETAIL_REALTIME_AUTO_REFRESH_INTERVAL_MS)} / 次，长按按钮取消
                  </span>
                ) : null}
                {watchObserveNotice ? (
                  <span className="details-chart-watch-note">
                    {watchObserveNotice}
                  </span>
                ) : null}
              </div>,
            ),
          )}
        </div>
      </section>

      <section className="details-card details-strategy-card">
        <div className="details-section-head details-section-head-strategy">
          <h3 className="details-subtitle">Scene 表现明细</h3>
          <div className="details-strategy-toolbar">
            <div className="details-strategy-params">
              <span>名称：{formatFieldValue(detailData?.overview?.name)}</span>
              <span>代码：{formatFieldValue(resolvedTsCode)}</span>
              <span>当前参考日：{formatFieldValue(resolvedTradeDate)}</span>
              <span>相对日期：{formatFieldValue(strategyDisplayRelativeTradeDate)}</span>
            </div>
            <div className="details-strategy-nav">
              <button
                className="details-primary-btn details-primary-btn-alt details-strategy-nav-btn"
                type="button"
                disabled={!previousStrategyTradeDate || detailLoading}
                onClick={() => onJumpStrategyTradeDate(previousStrategyTradeDate)}
                title={previousStrategyTradeDate ? `切换到 ${previousStrategyTradeDate}` : "没有更早的参考日"}
              >
                上一天
              </button>
              <button
                className="details-primary-btn details-primary-btn-alt details-strategy-nav-btn"
                type="button"
                disabled={!nextStrategyTradeDate || detailLoading}
                onClick={() => onJumpStrategyTradeDate(nextStrategyTradeDate)}
                title={nextStrategyTradeDate ? `切换到 ${nextStrategyTradeDate}` : "没有更新的参考日"}
              >
                下一天
              </button>
            </div>
          </div>
        </div>

        <SceneTableSection
          rows={sceneRows}
          emptyText="暂无 scene 表现明细"
          selectedKey={
            sceneDetailTarget
              ? `${sceneDetailTarget.scene_name}-${sceneDetailTarget.hit_date ?? "none"}`
              : null
          }
          onSelectRow={(row) => {
            setSceneDetailTarget(row);
            setSceneDetailModalOpen(true);
          }}
        />
      </section>

      {sceneDetailModalOpen ? (
        <div
          className="details-scene-modal-backdrop"
          onClick={() => setSceneDetailModalOpen(false)}
        >
          <section
            className="details-card details-scene-modal"
            onClick={(event) => event.stopPropagation()}
          >
            <div className="details-section-head details-section-head-strategy">
              <h3 className="details-subtitle">策略变化明细（{formatFieldValue(sceneDetailTarget?.scene_name)}）</h3>
              <div className="details-strategy-toolbar">
                <div className="details-strategy-params">
                  <span>状态：{toSceneStageLabel(sceneDetailTarget?.stage)}</span>
                  <span>阶段分：{formatFieldValue(sceneDetailTarget?.stage_score)}</span>
                  <span>风险分：{formatFieldValue(sceneDetailTarget?.risk_score)}</span>
                </div>
                <div className="details-strategy-nav">
                  <button
                    className="details-primary-btn details-primary-btn-alt details-strategy-nav-btn"
                    type="button"
                    disabled={!previousStrategyTradeDate || detailLoading}
                    onClick={() => onJumpStrategyTradeDate(previousStrategyTradeDate)}
                  >
                    上一天
                  </button>
                  <button
                    className="details-primary-btn details-primary-btn-alt details-strategy-nav-btn"
                    type="button"
                    disabled={!nextStrategyTradeDate || detailLoading}
                    onClick={() => onJumpStrategyTradeDate(nextStrategyTradeDate)}
                  >
                    下一天
                  </button>
                  <button
                    className="details-primary-btn details-primary-btn-alt details-strategy-nav-btn"
                    type="button"
                    onClick={() => setSceneDetailModalOpen(false)}
                  >
                    关闭
                  </button>
                </div>
              </div>
            </div>

            <div
              className={[
                "details-strategy-grid",
                isStrategyStacked ? "is-stacked" : "",
                strategySplitDragging ? "is-dragging" : "",
              ]
                .filter(Boolean)
                .join(" ")}
              ref={strategyGridRef}
              style={strategyGridStyle}
            >
              <div className="details-strategy-panel">
                <StrategyTableSection
                  title="已触发"
                  rows={modalStrategyActiveRows}
                  emptyText="当前 scene 下暂无已触发策略"
                  sectionKind="mixed"
                  compareRowMap={strategyCompareRowMap}
                  compareTradeDate={strategySnapshotTradeDate}
                  outReferenceTradeDate={previousStrategyTradeDate}
                />
              </div>
              {isStrategyStacked ? null : (
                <div
                  className="details-strategy-resize"
                  onPointerDown={onStrategyResizePointerDown}
                >
                  <span className="details-strategy-resize-line" />
                </div>
              )}
              <div className="details-strategy-panel">
                <StrategyTableSection
                  title="未触发"
                  rows={modalStrategyIdleRows}
                  emptyText="当前 scene 下暂无未触发策略"
                  sectionKind="untriggered"
                  compareRowMap={strategyCompareRowMap}
                  compareTradeDate={strategySnapshotTradeDate}
                />
              </div>
            </div>
          </section>
        </div>
      ) : null}

      <div className="details-float-nav">
        <button
          className="details-float-nav-btn"
          type="button"
          disabled={!prevNavigationItem || detailLoading}
          onClick={() => onJumpNavigationTarget(prevNavigationItem)}
        >
          上一条
        </button>
        <button
          className="details-float-nav-btn"
          type="button"
          disabled={!nextNavigationItem || detailLoading}
          onClick={() => onJumpNavigationTarget(nextNavigationItem)}
        >
          下一条
        </button>
      </div>
    </div>
  );
}
