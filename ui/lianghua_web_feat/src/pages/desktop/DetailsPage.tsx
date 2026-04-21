import {
  startTransition,
  useCallback,
  useDeferredValue,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
  type MouseEvent as ReactMouseEvent,
  type PointerEvent as ReactPointerEvent,
  type ReactNode,
  type WheelEvent as ReactWheelEvent,
} from "react";
import { useSearchParams } from "react-router-dom";
import {
  getStockDetailCyq,
  getStockDetailRealtime,
  getStockDetailPage,
  getStockDetailStrategySnapshot,
  type DetailCyqSnapshot,
  type DetailKlinePanel,
  type DetailKlineRow,
  type DetailKlinePayload,
  type DetailOverview,
  type DetailPrevRankRow,
  type DetailSceneTriggerRow,
  type DetailStrategyTriggerRow,
  type StockSimilarityPageData,
  type StockSimilarityRow,
  type StockDetailCyqData,
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
  readStoredDetailsNavLongPressIntervalSeconds,
  readStoredChartRankMarkerThreshold,
  readStoredChartIndicatorWidthRatio,
  readStoredChartMainWidthRatio,
} from "../../shared/chartSettings";
import {
  findWatchObserveRow,
  listWatchObserveRows,
  preloadWatchObserveRows,
  removeWatchObserveRows,
  type WatchObserveRow,
  upsertWatchObserveRow,
} from "../../apis/watchObserve";
import type { DetailsNavigationItem } from "../../shared/detailsLinkState";
import type { DetailsStrategyCompareSnapshot } from "../../shared/detailsLinkState";
import "./css/DetailsPage.css";

const DEFAULT_TOP_LIMIT = "100";
const DEFAULT_CHART_HEIGHT = 880;
const CHART_MIN_HEIGHT_DESKTOP = DEFAULT_CHART_HEIGHT;
const CHART_MIN_HEIGHT_MOBILE = 30;
const CHART_MOBILE_BREAKPOINT = 980;
const DETAIL_CHART_WINDOW_DAYS = 280;
const DEFAULT_VISIBLE_BARS = 90;
const MIN_VISIBLE_BARS = 20;
const CHART_MIN_RIGHT_ALIGNED_SLOTS = 60;
const DEFAULT_ROW_WEIGHTS = [52, 16, 16, 16];
const CHART_VIEWBOX_WIDTH = 1120;
const CHART_VIEWBOX_HEIGHT = 240;
const CHART_MARGIN = { top: 12, right: 8, bottom: 28, left: 52 };
const CHART_DATE_TICK_COUNT = 6;
const CHART_CURSOR_Y_MIN = 6;
const CHART_CURSOR_Y_MAX = 94;
const CHART_TOOLTIP_LEFT_THRESHOLD = 62;
const CHART_INTERVAL_PANEL_TOP_PERCENT = 18;
const CHART_POINTER_DRAG_THRESHOLD = 6;
const CHART_TOUCH_FOCUS_HIT_SLOP = 24;
const CHART_CYQ_PANEL_WIDTH_RATIO = 0.22;
const CHART_CYQ_PANEL_GAP = 12;
const VOLUME_OVERLAY_KEYS = ["VOL_SIGMA"] as const;
const CHART_PANEL_GAP_PX = 8;
const STRATEGY_SPLIT_DEFAULT = 0.64;
const STRATEGY_SPLIT_MIN = 0.24;
const STRATEGY_SPLIT_MAX = 0.76;
const STRATEGY_STACK_BREAKPOINT = 1180;
const WATERMARK_CONCEPT_LIMIT = 3;
const MAX_STOCK_NAME_CANDIDATES = 12;
const DETAIL_REALTIME_AUTO_REFRESH_INTERVAL_MS = 15_000;
const DETAIL_REALTIME_LONG_PRESS_MS = 600;
const DETAIL_NAV_LONG_PRESS_MS = 600;
const DETAIL_NAV_LONG_PRESS_TOUCH_MS = 320;
const CANDLE_UP_COLOR = "#d9485f";
const CANDLE_DOWN_COLOR = "#178f68";
const CANDLE_FLAT_COLOR = "#536273";
const CANDLE_REALTIME_UP_COLOR = "#eb7a34";
const CANDLE_REALTIME_DOWN_COLOR = "#2d6cdf";
const CHART_CYQ_UP_COLOR = "#4d95c9";
const CHART_CYQ_DOWN_COLOR = "#d9485f";
const LINE_COLORS = ["#0057ff", "#e13a1f", "#6a00f4", "#00843d"];
const CANDLE_BASE_SERIES_KEYS = new Set(["open", "high", "low", "close"]);

function waitForNextPaint() {
  if (typeof window === "undefined") {
    return Promise.resolve();
  }
  return new Promise<void>((resolve) => {
    window.requestAnimationFrame(() => resolve());
  });
}
type DetailStrategySortKey = "rule_score" | "hit_date" | "lag";
type PrevRankSortKey = "trade_date" | "rank";
type SceneOverviewSortKey =
  | "scene_name"
  | "scene_rank"
  | "stage_score"
  | "risk_score"
  | "hit_date"
  | "lag"
  | "scene_rule_score"
  | "contribution_pct";
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
  mode: "pan" | "focus" | "tap" | "dismiss" | "interval-select";
  startClientX: number;
  startClientY: number;
  startVisibleStart: number;
  barsPerPixel: number;
  maxVisibleStart: number;
  moved: boolean;
  anchorAbsoluteIndex?: number;
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

type IntervalRestoreRequest = {
  startTradeDate: string;
  endTradeDate: string;
};

type ResolvedIntervalRestore = {
  startAbsoluteIndex: number;
  endAbsoluteIndex: number;
  startTradeDate: string;
  endTradeDate: string;
};

type DetailsPageVariant = "default" | "linked-overlay";
type DetailsAutoNavDirection = "prev" | "next";

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

function isVolumeOverlayKey(key: string) {
  return VOLUME_OVERLAY_KEYS.some(
    (overlayKey) => overlayKey.toLowerCase() === key.toLowerCase(),
  );
}

function formatSeriesLabel(key: string) {
  if (key.toLowerCase() === "vol_sigma".toLowerCase()) {
    return "异动量能";
  }
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
  if (key.toLowerCase() === "vol_sigma".toLowerCase()) {
    return "#7dd3fc";
  }
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

function normalizeIntervalRestoreRequest(
  startTradeDate: string,
  endTradeDate: string,
): IntervalRestoreRequest | null {
  const normalizedStartTradeDate = startTradeDate.trim();
  const normalizedEndTradeDate = endTradeDate.trim();
  if (!normalizedStartTradeDate || !normalizedEndTradeDate) {
    return null;
  }

  if (normalizedStartTradeDate <= normalizedEndTradeDate) {
    return {
      startTradeDate: normalizedStartTradeDate,
      endTradeDate: normalizedEndTradeDate,
    };
  }

  return {
    startTradeDate: normalizedEndTradeDate,
    endTradeDate: normalizedStartTradeDate,
  };
}

function toUtcTimeFromTradeDate(tradeDate: string) {
  if (!/^\d{8}$/.test(tradeDate)) {
    return null;
  }

  const year = Number(tradeDate.slice(0, 4));
  const month = Number(tradeDate.slice(4, 6));
  const day = Number(tradeDate.slice(6, 8));
  const utcTime = Date.UTC(year, month - 1, day);
  return Number.isFinite(utcTime) ? utcTime : null;
}

function resolveChartWindowDays(intervalRestore: IntervalRestoreRequest | null) {
  if (!intervalRestore) {
    return 280;
  }

  const startUtcTime = toUtcTimeFromTradeDate(intervalRestore.startTradeDate);
  const endUtcTime = toUtcTimeFromTradeDate(intervalRestore.endTradeDate);
  if (startUtcTime === null || endUtcTime === null) {
    return 280;
  }

  const daySpan = Math.max(
    1,
    Math.floor(Math.abs(endUtcTime - startUtcTime) / 86_400_000) + 1,
  );
  return Math.max(280, daySpan + 40);
}

function findNearestPreviousTradeDateIndex(
  items: DetailKlineRow[],
  tradeDate: string,
) {
  let candidateIndex = -1;
  for (let index = 0; index < items.length; index += 1) {
    const currentTradeDate = items[index]?.trade_date?.trim() ?? "";
    if (!currentTradeDate) {
      continue;
    }
    if (currentTradeDate === tradeDate) {
      return index;
    }
    if (currentTradeDate < tradeDate) {
      candidateIndex = index;
      continue;
    }
    break;
  }
  return candidateIndex;
}

function resolveIntervalRestore(
  items: DetailKlineRow[],
  intervalRestore: IntervalRestoreRequest,
): ResolvedIntervalRestore | null {
  if (items.length === 0) {
    return null;
  }

  const startAbsoluteIndex = findNearestPreviousTradeDateIndex(
    items,
    intervalRestore.startTradeDate,
  );
  const endAbsoluteIndex = findNearestPreviousTradeDateIndex(
    items,
    intervalRestore.endTradeDate,
  );
  if (startAbsoluteIndex < 0 || endAbsoluteIndex < 0) {
    return null;
  }

  const normalizedStartAbsoluteIndex = Math.min(
    startAbsoluteIndex,
    endAbsoluteIndex,
  );
  const normalizedEndAbsoluteIndex = Math.max(startAbsoluteIndex, endAbsoluteIndex);
  const resolvedStartTradeDate =
    items[normalizedStartAbsoluteIndex]?.trade_date?.trim() ?? "";
  const resolvedEndTradeDate =
    items[normalizedEndAbsoluteIndex]?.trade_date?.trim() ?? "";
  if (!resolvedStartTradeDate || !resolvedEndTradeDate) {
    return null;
  }

  return {
    startAbsoluteIndex: normalizedStartAbsoluteIndex,
    endAbsoluteIndex: normalizedEndAbsoluteIndex,
    startTradeDate: resolvedStartTradeDate,
    endTradeDate: resolvedEndTradeDate,
  };
}

function buildIntervalSelectionFromAbsoluteIndices(
  items: DetailKlineRow[],
  startAbsoluteIndex: number,
  endAbsoluteIndex: number,
): ResolvedIntervalRestore | null {
  if (items.length === 0) {
    return null;
  }

  const normalizedStartAbsoluteIndex = Math.max(
    0,
    Math.min(startAbsoluteIndex, endAbsoluteIndex),
  );
  const normalizedEndAbsoluteIndex = Math.min(
    items.length - 1,
    Math.max(startAbsoluteIndex, endAbsoluteIndex),
  );
  const startTradeDate =
    items[normalizedStartAbsoluteIndex]?.trade_date?.trim() ?? "";
  const endTradeDate =
    items[normalizedEndAbsoluteIndex]?.trade_date?.trim() ?? "";
  if (!startTradeDate || !endTradeDate) {
    return null;
  }

  return {
    startAbsoluteIndex: normalizedStartAbsoluteIndex,
    endAbsoluteIndex: normalizedEndAbsoluteIndex,
    startTradeDate,
    endTradeDate,
  };
}

function buildIntervalRestoreNotice(
  intervalRestore: IntervalRestoreRequest,
  resolvedIntervalRestore: ResolvedIntervalRestore,
) {
  if (
    intervalRestore.startTradeDate === resolvedIntervalRestore.startTradeDate &&
    intervalRestore.endTradeDate === resolvedIntervalRestore.endTradeDate
  ) {
    return "";
  }

  return `区间已按最近可用K线还原：${resolvedIntervalRestore.startTradeDate} ~ ${resolvedIntervalRestore.endTradeDate}`;
}

function stopEventPropagation(event: { stopPropagation: () => void }) {
  event.stopPropagation();
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
    {
      label: "最相似概念",
      value: formatFieldValue(overview?.most_related_concept),
    },
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

function buildSimilarityReasonText(row: StockSimilarityRow) {
  const parts: string[] = [];

  if (row.matchedConcepts.length > 0) {
    parts.push(`概念 ${row.matchedConcepts.join("、")}`);
  }
  if (row.sameIndustry && row.industry) {
    parts.push(`行业 ${row.industry}`);
  }
  if (row.matchedSceneNames.length > 0) {
    parts.push(`场景 ${row.matchedSceneNames.join("、")}`);
  }

  return parts.length > 0 ? parts.join(" | ") : "未命中可展示的相似标签";
}

function formatPercentValue(value: number | null) {
  if (value === null || !Number.isFinite(value)) {
    return "--";
  }
  return `${value.toFixed(2)}%`;
}

const TURNOVER_VALUE_KEYS = [
  "tor",
  "turnover_rate",
  "turnover",
  "turnover_rate_f",
] as const;

function findTurnoverNumber(item: DetailKlineRow | null) {
  if (!item) {
    return null;
  }

  for (const key of TURNOVER_VALUE_KEYS) {
    const value = item[key];
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
  }

  return null;
}

function findTurnoverValue(item: DetailKlineRow | null) {
  if (!item) {
    return null;
  }

  for (const key of TURNOVER_VALUE_KEYS) {
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

function buildRankNumberLookup(
  overview: DetailOverview | null | undefined,
  prevRanks: Array<{
    trade_date: string;
    rank?: number | null;
  }>,
) {
  const lookup = new Map<string, number>();

  if (
    overview?.trade_date &&
    typeof overview.rank === "number" &&
    Number.isFinite(overview.rank)
  ) {
    lookup.set(overview.trade_date, overview.rank);
  }

  prevRanks.forEach((row) => {
    if (typeof row.rank === "number" && Number.isFinite(row.rank)) {
      lookup.set(row.trade_date, row.rank);
    }
  });

  return lookup;
}

function findCyqSnapshotForTradeDate(
  snapshots: DetailCyqSnapshot[],
  tradeDate: string | null,
) {
  if (!tradeDate || snapshots.length === 0) {
    return snapshots[snapshots.length - 1] ?? null;
  }

  let latestBeforeOrEqual: DetailCyqSnapshot | null = null;
  for (const snapshot of snapshots) {
    if (snapshot.trade_date === tradeDate) {
      return snapshot;
    }
    if (snapshot.trade_date <= tradeDate) {
      latestBeforeOrEqual = snapshot;
      continue;
    }
    break;
  }
  return latestBeforeOrEqual ?? snapshots[snapshots.length - 1] ?? null;
}

function buildSceneRowKey(row: DetailSceneTriggerRow) {
  return `${row.scene_name}-${row.hit_date ?? "none"}`;
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
  riskScore: number | null;
  sceneRank: number | null;
  hitDate: string;
  lag: number | null;
  sceneRuleScore: number | null;
  contributionPct: number | null;
  contributionPctDisplay: number | null;
  color: string;
  sceneRow: DetailSceneTriggerRow;
};

const SCENE_OVERVIEW_COLORS = [
  "#2563eb",
  "#dc2626",
  "#16a34a",
  "#d97706",
  "#7c3aed",
  "#0891b2",
  "#c026d3",
  "#92400e",
  "#4b5563",
  "#4d7c0f",
] as const;

function getSceneOverviewColor(index: number) {
  return SCENE_OVERVIEW_COLORS[index % SCENE_OVERVIEW_COLORS.length];
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

  const baseItems = sortedRows.map((row, index) => {
    const stageScore =
      typeof row.stage_score === "number" && Number.isFinite(row.stage_score)
        ? row.stage_score
        : null;
    const sceneRuleScore = sceneRuleScoreMap.get(row.scene_name) ?? null;

    return {
      sceneName: row.scene_name,
      stage: row.stage,
      stageScore,
      riskScore:
        typeof row.risk_score === "number" && Number.isFinite(row.risk_score)
          ? row.risk_score
          : null,
      sceneRank: typeof row.scene_rank === "number" ? row.scene_rank : null,
      hitDate: row.hit_date?.trim() ?? "",
      lag: typeof row.lag === "number" && Number.isFinite(row.lag) ? row.lag : null,
      sceneRuleScore,
      contributionPct:
        denominator !== null && sceneRuleScore !== null
          ? (sceneRuleScore / denominator) * 100
          : null,
      contributionPctDisplay: null,
      color: getSceneOverviewColor(index),
      sceneRow: row,
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
    const overlayRows = (panel.series_keys ?? [])
      .filter((key) => isVolumeOverlayKey(key))
      .map((key) => ({
        label: formatSeriesLabel(key),
        value: formatFieldValue(item[key]),
      }))
      .filter((row) => row.value !== "--");

    return [
      {
        key: `${panel.key}-raw`,
        rows: [
          { label: "量", value: formatFieldValue(item.vol) },
          { label: "量比", value: formatRatioValue(volumeRatio) },
          ...overlayRows,
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

function buildIntervalStatsSections(
  items: DetailKlineRow[],
  intervalSelection: ResolvedIntervalRestore | null,
): TooltipSection[] {
  if (!intervalSelection) {
    return [];
  }

  const selectedItems = items.slice(
    intervalSelection.startAbsoluteIndex,
    intervalSelection.endAbsoluteIndex + 1,
  );
  if (selectedItems.length === 0) {
    return [];
  }

  const startClose = getNumericField(selectedItems[0] ?? null, "close");
  const endClose = getNumericField(
    selectedItems[selectedItems.length - 1] ?? null,
    "close",
  );
  const changeAmount =
    startClose !== null && endClose !== null ? endClose - startClose : null;
  const changePct =
    startClose !== null && startClose !== 0 && endClose !== null
      ? ((endClose - startClose) / startClose) * 100
      : null;

  let maxHigh: number | null = null;
  let minLow: number | null = null;
  let totalTurnover = 0;
  let hasTurnover = false;
  selectedItems.forEach((item) => {
    const high = getNumericField(item, "high");
    const low = getNumericField(item, "low");
    const turnover = findTurnoverNumber(item);
    if (high !== null) {
      maxHigh = maxHigh === null ? high : Math.max(maxHigh, high);
    }
    if (low !== null) {
      minLow = minLow === null ? low : Math.min(minLow, low);
    }
    if (turnover !== null) {
      totalTurnover += turnover;
      hasTurnover = true;
    }
  });

  const amplitude =
    maxHigh !== null && minLow !== null && minLow !== 0
      ? ((maxHigh - minLow) / minLow) * 100
      : null;

  return [
    {
      key: "interval-summary",
      rows: [
        { label: "K线数", value: String(selectedItems.length) },
        {
          label: "涨跌额",
          value:
            changeAmount === null ? "--" : formatSignedNumber(changeAmount) || "--",
        },
        { label: "涨跌幅", value: formatPercentValue(changePct) },
        {
          label: "总计换手",
          value: hasTurnover ? `${totalTurnover.toFixed(2)}%` : "--",
        },
        { label: "区间最高", value: formatFieldValue(maxHigh) },
        { label: "区间最低", value: formatFieldValue(minLow) },
        { label: "振幅", value: formatPercentValue(amplitude) },
      ],
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
    {
      key: "volume",
      label: "量能",
      kind: "bar",
      series_keys: ["vol", ...VOLUME_OVERLAY_KEYS],
    },
    { key: "brick", label: "砖型图", kind: "brick", series_keys: ["brick"] },
  ] satisfies DetailKlinePanel[];
}

function buildChartTemplateRows(
  kline: DetailKlinePayload | null | undefined,
  panels: DetailKlinePanel[],
  mainPanelHeight: number,
  indicatorTotalHeight: number,
  chartMinHeight: number,
) {
  if (panels.length === 0) {
    return `${Math.max(mainPanelHeight, indicatorTotalHeight, chartMinHeight)}px`;
  }

  const matchedMainPanelIndex = panels.findIndex(
    (panel) => panel.key === "price" || panel.kind === "candles",
  );
  const mainPanelIndex = matchedMainPanelIndex >= 0 ? matchedMainPanelIndex : 0;
  const indicatorIndices = panels
    .map((_, index) => index)
    .filter((index) => index !== mainPanelIndex);

  if (indicatorIndices.length === 0) {
    return `${mainPanelHeight.toFixed(2)}px`;
  }

  const resolvedWeights =
    kline?.row_weights?.filter((weight) => weight > 0) ?? [];
  const weights =
    resolvedWeights.length === panels.length
      ? resolvedWeights
      : panels.map(
          (_, index) =>
            DEFAULT_ROW_WEIGHTS[index] ??
            DEFAULT_ROW_WEIGHTS[DEFAULT_ROW_WEIGHTS.length - 1] ??
            16,
        );

  const indicatorWeightSum = indicatorIndices.reduce(
    (sum, index) => sum + Math.max(weights[index] ?? 0, 0),
    0,
  );

  return panels
    .map((_, index) => {
      if (index === mainPanelIndex) {
        return `${mainPanelHeight.toFixed(2)}px`;
      }

      if (indicatorWeightSum <= 0) {
        return `${(indicatorTotalHeight / indicatorIndices.length).toFixed(2)}px`;
      }

      const panelWeight = Math.max(weights[index] ?? 0, 0);
      const panelHeight = (indicatorTotalHeight * panelWeight) / indicatorWeightSum;
      return `${panelHeight.toFixed(2)}px`;
    })
    .join(" ");
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

function getChartLayoutSlotCount(itemCount: number, totalItemCount: number) {
  if (itemCount <= 0) {
    return 0;
  }

  return totalItemCount === itemCount && itemCount < CHART_MIN_RIGHT_ALIGNED_SLOTS
    ? CHART_MIN_RIGHT_ALIGNED_SLOTS
    : itemCount;
}

function getChartKlinePlotWidth(reserveCyqPanelWidth: boolean) {
  const plotWidth =
    CHART_VIEWBOX_WIDTH - CHART_MARGIN.left - CHART_MARGIN.right;
  const chipPanelWidth = reserveCyqPanelWidth
    ? plotWidth * CHART_CYQ_PANEL_WIDTH_RATIO
    : 0;

  return Math.max(
    plotWidth -
      chipPanelWidth -
      (reserveCyqPanelWidth ? CHART_CYQ_PANEL_GAP : 0),
    1,
  );
}

function getChartKlinePlotRight(reserveCyqPanelWidth: boolean) {
  return CHART_MARGIN.left + getChartKlinePlotWidth(reserveCyqPanelWidth);
}

function getChartItemX(
  itemIndex: number,
  itemCount: number,
  layoutSlotCount: number,
  reserveCyqPanelWidth: boolean,
) {
  const resolvedLayoutSlotCount = Math.max(layoutSlotCount, itemCount);
  const klinePlotWidth = getChartKlinePlotWidth(reserveCyqPanelWidth);
  const step =
    resolvedLayoutSlotCount > 0
      ? klinePlotWidth / resolvedLayoutSlotCount
      : klinePlotWidth;
  const leadingSlotCount = Math.max(resolvedLayoutSlotCount - itemCount, 0);

  return (
    CHART_MARGIN.left +
    step * (leadingSlotCount + itemIndex) +
    step / 2
  );
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
  layoutSlotCount = itemCount,
  reserveCyqPanelWidth = false,
) {
  if (itemCount <= 0 || layoutSlotCount <= 0) {
    return null;
  }

  const plotStartPercent = (CHART_MARGIN.left / CHART_VIEWBOX_WIDTH) * 100;
  const plotEndPercent =
    (getChartKlinePlotRight(reserveCyqPanelWidth) / CHART_VIEWBOX_WIDTH) * 100;
  const plotXPercent = clampNumber(
    (chartXPercent - plotStartPercent) / (plotEndPercent - plotStartPercent),
    0,
    0.999999,
  );
  const leadingSlotCount = Math.max(layoutSlotCount - itemCount, 0);
  const slotIndex = clampNumber(
    Math.floor(plotXPercent * layoutSlotCount),
    0,
    layoutSlotCount - 1,
  );
  const visibleIndex = slotIndex - leadingSlotCount;

  if (visibleIndex < 0 || visibleIndex >= itemCount) {
    return null;
  }

  return visibleIndex;
}

function buildChartPointerSnapshot(
  viewport: HTMLDivElement,
  clientX: number,
  clientY: number,
  itemCount: number,
  layoutSlotCount = itemCount,
  reserveCyqPanelWidth = false,
): ChartPointerSnapshot | null {
  if (itemCount <= 0 || layoutSlotCount <= 0) {
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
  const visibleIndex = resolveVisibleIndexFromChartX(
    chartXPercent,
    itemCount,
    layoutSlotCount,
    reserveCyqPanelWidth,
  );

  if (visibleIndex === null) {
    return null;
  }

  return {
    cursorXPercent:
      (getChartItemX(
        visibleIndex,
        itemCount,
        layoutSlotCount,
        reserveCyqPanelWidth,
      ) /
        CHART_VIEWBOX_WIDTH) *
      100,
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
  rankMarkerThreshold: number,
  rankNumberLookup: Map<string, number>,
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
  isCyqPanelVisible: boolean,
  selectedCyqSnapshot: DetailCyqSnapshot | null,
  selectedCyqTradeDate: string | null,
  chipToggleButton: ReactNode,
  watchObserveButton: ReactNode,
  chartIntervalSelection: ResolvedIntervalRestore | null,
  chartIntervalDraftSelection: ResolvedIntervalRestore | null,
  chartIntervalPanelOpen: boolean,
  intervalStatsSections: TooltipSection[],
  onCloseChartIntervalPanel: () => void,
) {
  const kind = panel.kind ?? "line";
  const showDateAxis = index === panelCount - 1;
  const seriesKeys = panel.series_keys?.length ? panel.series_keys : [];
  const candleOverlayKeys =
    kind === "candles"
      ? seriesKeys.filter((key) => !CANDLE_BASE_SERIES_KEYS.has(key))
      : [];
  const headerSeriesKeys = kind === "candles" ? candleOverlayKeys : seriesKeys;
  const reserveCyqPanelWidth =
    isCyqPanelVisible && (selectedCyqSnapshot?.bins.length ?? 0) > 0;
  const showCyqPanel = panel.key === "price" && reserveCyqPanelWidth;
  const klinePlotWidth = getChartKlinePlotWidth(reserveCyqPanelWidth);
  const plotRight = CHART_VIEWBOX_WIDTH - CHART_MARGIN.right;
  const klinePlotRight = getChartKlinePlotRight(reserveCyqPanelWidth);
  const chipPanelLeft =
    klinePlotRight + (reserveCyqPanelWidth ? CHART_CYQ_PANEL_GAP : 0);
  const chipPanelRight = plotRight;
  const plotHeight =
    CHART_VIEWBOX_HEIGHT - CHART_MARGIN.top - CHART_MARGIN.bottom;
  const layoutSlotCount = getChartLayoutSlotCount(items.length, allItems.length);
  const step = layoutSlotCount > 0 ? klinePlotWidth / layoutSlotCount : klinePlotWidth;
  const xAt = (itemIndex: number) =>
    getChartItemX(
      itemIndex,
      items.length,
      layoutSlotCount,
      reserveCyqPanelWidth,
    );
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
  const focusXPercent =
    activeVisibleIndex !== null
      ? (xAt(activeVisibleIndex) / CHART_VIEWBOX_WIDTH) * 100
      : null;
  const referenceVisibleIndex =
    referenceTradeDate !== null
      ? items.findIndex((item) => item.trade_date === referenceTradeDate)
      : -1;
  const tooltipHorizontalClass =
    (focusXPercent ?? 0) > CHART_TOOLTIP_LEFT_THRESHOLD
      ? "details-chart-tooltip-left"
      : "details-chart-tooltip-right";
  const activeIntervalSelection =
    panel.key === "price"
      ? chartIntervalDraftSelection ?? chartIntervalSelection
      : null;
  const intervalStartVisibleIndex =
    activeIntervalSelection &&
    activeIntervalSelection.startAbsoluteIndex >= effectiveVisibleStart &&
    activeIntervalSelection.startAbsoluteIndex < effectiveVisibleStart + items.length
      ? activeIntervalSelection.startAbsoluteIndex - effectiveVisibleStart
      : null;
  const intervalEndVisibleIndex =
    activeIntervalSelection &&
    activeIntervalSelection.endAbsoluteIndex >= effectiveVisibleStart &&
    activeIntervalSelection.endAbsoluteIndex < effectiveVisibleStart + items.length
      ? activeIntervalSelection.endAbsoluteIndex - effectiveVisibleStart
      : null;
  const intervalSelectionLeftPercent =
    intervalStartVisibleIndex !== null && intervalEndVisibleIndex !== null
      ? (Math.max(
          0,
          Math.min(xAt(intervalStartVisibleIndex), xAt(intervalEndVisibleIndex)) -
            step / 2,
        ) /
          CHART_VIEWBOX_WIDTH) *
        100
      : null;
  const intervalSelectionWidthPercent =
    intervalStartVisibleIndex !== null && intervalEndVisibleIndex !== null
      ? (Math.min(
          CHART_VIEWBOX_WIDTH,
          Math.abs(xAt(intervalEndVisibleIndex) - xAt(intervalStartVisibleIndex)) +
            step,
        ) /
          CHART_VIEWBOX_WIDTH) *
        100
      : null;
  const intervalPanelXPercent =
    intervalStartVisibleIndex !== null && intervalEndVisibleIndex !== null
      ? (((xAt(intervalStartVisibleIndex) + xAt(intervalEndVisibleIndex)) / 2) /
          CHART_VIEWBOX_WIDTH) *
        100
      : null;
  const intervalPanelHorizontalClass =
    (intervalPanelXPercent ?? 0) > CHART_TOOLTIP_LEFT_THRESHOLD
      ? "details-chart-tooltip-left"
      : "details-chart-tooltip-right";
  const showDefaultTooltip =
    !(panel.key === "price" && chartIntervalPanelOpen && intervalStatsSections.length > 0);
  const tooltipSections =
    showDefaultTooltip && isActivePanel && activeVisibleIndex !== null
      ? buildDetailTooltipRows(
          panel,
          items[activeVisibleIndex] ?? null,
          activeAbsoluteIndex,
          allItems,
          rankLookup,
        )
      : [];
  const rankMarkerPoints =
    panel.key === "price" && rankMarkerThreshold > 0
      ? items.flatMap((row, itemIndex) => {
          const rankValue = rankNumberLookup.get(row.trade_date);
          if (rankValue === undefined || rankValue > rankMarkerThreshold) {
            return [];
          }

          return [
            {
              key: `${panel.key}-rank-marker-${row.trade_date}`,
              leftPercent: (xAt(itemIndex) / CHART_VIEWBOX_WIDTH) * 100,
              title: `${row.trade_date} 排名 #${Math.round(rankValue)}`,
            },
          ];
        })
      : [];

  let domain: { min: number; max: number } | null = null;
  let zeroY: number | null = null;
  let svgContent: ReactNode = null;
  let cyqSvgContent: ReactNode = null;

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

      if (showCyqPanel && selectedCyqSnapshot) {
        const selectedCloseCandidate =
          items[activeVisibleIndex ?? Math.max(items.length - 1, 0)]?.close ??
          selectedCyqSnapshot.close;
        const selectedClose =
          typeof selectedCloseCandidate === "number" &&
          Number.isFinite(selectedCloseCandidate)
            ? selectedCloseCandidate
            : selectedCyqSnapshot.close;
        const visibleCyqBins = selectedCyqSnapshot.bins.filter((bin) => {
          const binLow = Math.min(bin.price_low, bin.price_high);
          const binHigh = Math.max(bin.price_low, bin.price_high);
          return !(binHigh < currentDomain.min || binLow > currentDomain.max);
        });
        const maxChipPct = visibleCyqBins.reduce((acc, bin) => {
          return Number.isFinite(bin.chip_pct) ? Math.max(acc, bin.chip_pct) : acc;
        }, 0);

        if (visibleCyqBins.length > 0 && maxChipPct > 0) {
          cyqSvgContent = (
            <g key={`${panel.key}-cyq-${selectedCyqTradeDate ?? selectedCyqSnapshot.trade_date}`}>
              <line
                className="details-chart-cyq-divider"
                x1={chipPanelLeft}
                y1={CHART_MARGIN.top}
                x2={chipPanelLeft}
                y2={CHART_VIEWBOX_HEIGHT - CHART_MARGIN.bottom}
              />
              {visibleCyqBins.map((bin, binIndex) => {
                const clampedLow = Math.max(
                  Math.min(bin.price_low, bin.price_high),
                  currentDomain.min,
                );
                const clampedHigh = Math.min(
                  Math.max(bin.price_low, bin.price_high),
                  currentDomain.max,
                );
                if (clampedLow > clampedHigh) {
                  return null;
                }

                const yTop = yAt(clampedHigh);
                const yBottom = yAt(clampedLow);
                const barHeight = Math.max(yBottom - yTop, 1);
                const chipPct = Number.isFinite(bin.chip_pct) ? bin.chip_pct : 0;
                const maxBarWidth = Math.max(chipPanelRight - chipPanelLeft - 4, 0);
                const barWidth = (chipPct / maxChipPct) * maxBarWidth;
                const representativePrice = (bin.price_low + bin.price_high) / 2;
                const fill =
                  representativePrice > selectedClose
                    ? CHART_CYQ_UP_COLOR
                    : CHART_CYQ_DOWN_COLOR;

                return (
                  <rect
                    className="details-chart-cyq-bar"
                    key={`${selectedCyqSnapshot.trade_date}-${binIndex}`}
                    x={chipPanelRight - Math.max(barWidth, 1)}
                    y={yTop}
                    width={Math.max(barWidth, 1)}
                    height={barHeight}
                    fill={fill}
                    opacity={0.86}
                    rx={1}
                  />
                );
              })}
            </g>
          );
        }
      }

      svgContent = [...candleNodes, ...overlayNodes, cyqSvgContent];
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
    const primaryBarKey =
      seriesKeys.find((key) => !isVolumeOverlayKey(key)) ?? null;
    const overlayLineKeys =
      panel.key === "volume"
        ? seriesKeys.filter((key) => isVolumeOverlayKey(key))
        : [];
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
      const zeroBaseline = zeroY;
      const barWidth = Math.max(Math.min(step * 0.72, 18), 3);

      const barNodes = items.map((row, itemIndex) => {
        const value = primaryBarKey ? getNumericField(row, primaryBarKey) : null;
        if (value === null) {
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
        const y = Math.min(yAt(value), zeroBaseline);
        const height = Math.max(Math.abs(zeroBaseline - yAt(value)), 1);

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
      const overlayNodes =
        overlayLineKeys.flatMap((overlayLineKey, overlayIndex) =>
          buildLineSegments(
            items,
            overlayLineKey,
            xAt,
            yAt,
          ).map((segment, segmentIndex) => (
            <path
              className="details-chart-line-path details-chart-line-path-indicator"
              key={`${panel.key}-${overlayLineKey}-${segmentIndex}`}
              d={buildLinePath(segment)}
              stroke={getSeriesColor(overlayLineKey, overlayIndex + 1)}
            />
          )),
        );

      svgContent = [...barNodes, ...overlayNodes];
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

        {panel.key === "price" ? chipToggleButton : null}
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
                    x2={klinePlotRight}
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
                x2={klinePlotRight}
                y2={zeroY}
              />
            ) : null}

            {svgContent}
          </svg>
        ) : (
          <div className="details-chart-empty">暂无有效图表数据</div>
        )}

        {rankMarkerPoints.length > 0 ? (
          <div className="details-chart-rank-marker-layer" aria-hidden="true">
            {rankMarkerPoints.map((point) => (
              <span
                className="details-chart-rank-marker-dot"
                key={point.key}
                style={{ left: `${point.leftPercent}%` }}
                title={point.title}
              />
            ))}
          </div>
        ) : null}

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

        {panel.key === "price" &&
        intervalSelectionLeftPercent !== null &&
        intervalSelectionWidthPercent !== null ? (
          <div
            className={[
              "details-chart-interval-selection",
              chartIntervalDraftSelection ? "is-draft" : "",
            ]
              .filter(Boolean)
              .join(" ")}
            data-testid="details-interval-selection"
            style={{
              left: `${intervalSelectionLeftPercent}%`,
              width: `${intervalSelectionWidthPercent}%`,
            }}
          />
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
                  left: `${focusXPercent ?? chartFocus.cursorXPercent}%`,
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

        {panel.key === "price" &&
        chartIntervalSelection &&
        chartIntervalPanelOpen &&
        intervalStatsSections.length > 0 &&
        intervalPanelXPercent !== null ? (
          <div
            className={[
              "details-chart-tooltip",
              "details-chart-interval-panel",
              intervalPanelHorizontalClass,
            ]
              .filter(Boolean)
              .join(" ")}
            data-testid="details-interval-panel"
            onPointerDown={stopEventPropagation}
            onPointerMove={stopEventPropagation}
            onPointerUp={stopEventPropagation}
            onPointerCancel={stopEventPropagation}
            onClick={stopEventPropagation}
            style={{
              left: `${intervalPanelXPercent}%`,
              top: `${CHART_INTERVAL_PANEL_TOP_PERCENT}%`,
            }}
          >
            <div className="details-chart-tooltip-head details-chart-interval-panel-head">
              <strong>
                {chartIntervalSelection.startTradeDate} ~ {chartIntervalSelection.endTradeDate}
              </strong>
              <button
                type="button"
                className="details-chart-interval-close"
                data-testid="details-interval-close"
                onPointerDown={stopEventPropagation}
                onPointerUp={stopEventPropagation}
                onClick={(event) => {
                  event.stopPropagation();
                  onCloseChartIntervalPanel();
                }}
              >
                关闭
              </button>
            </div>
            <div className="details-chart-tooltip-body">
              {intervalStatsSections.map((section) => (
                <div
                  className={[
                    "details-chart-tooltip-grid",
                    section.variant === "ohlc" ? "details-chart-tooltip-grid-ohlc" : "",
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
      </div>
    </section>
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

function SceneOverviewTableSection({
  items,
  emptyText,
  selectedKey,
  onSelectRow,
}: {
  items: SceneOverviewItem[];
  emptyText: string;
  selectedKey?: string | null;
  onSelectRow?: (row: DetailSceneTriggerRow) => void;
}) {
  const sortDefinitions = useMemo(
    () =>
      ({
        scene_name: { value: (item: SceneOverviewItem) => item.sceneName },
        scene_rank: { value: (item: SceneOverviewItem) => item.sceneRank },
        stage_score: { value: (item: SceneOverviewItem) => item.stageScore },
        risk_score: { value: (item: SceneOverviewItem) => item.riskScore },
        hit_date: { value: (item: SceneOverviewItem) => item.hitDate },
        lag: { value: (item: SceneOverviewItem) => item.lag },
        scene_rule_score: {
          value: (item: SceneOverviewItem) => item.sceneRuleScore,
        },
        contribution_pct: {
          value: (item: SceneOverviewItem) => item.contributionPctDisplay,
        },
      }) satisfies Partial<Record<SceneOverviewSortKey, SortDefinition<SceneOverviewItem>>>,
    [],
  );
  const { sortKey, sortDirection, sortedRows, toggleSort } = useTableSort<
    SceneOverviewItem,
    SceneOverviewSortKey
  >(items, sortDefinitions, { key: "scene_rank", direction: "asc" });

  if (items.length === 0) {
    return <div className="details-empty details-empty-soft">{emptyText}</div>;
  }

  return (
    <div className="details-scene-overview-table-wrap">
      <table className="details-table details-table-scene details-table-scene-overview">
        <colgroup>
          <col className="details-col-scene-name" />
          <col className="details-col-scene-stage" />
          <col className="details-col-scene-rank" />
          <col className="details-col-scene-score" />
          <col className="details-col-scene-score" />
          <col className="details-col-date" />
          <col className="details-col-lag" />
          <col className="details-col-scene-score" />
          <col className="details-col-scene-contrib" />
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
            <th aria-sort={getAriaSort(sortKey === "scene_rank", sortDirection)}>
              <TableSortButton
                label="全市场截面排名"
                isActive={sortKey === "scene_rank"}
                direction={sortDirection}
                onClick={() => toggleSort("scene_rank")}
                title="按全市场截面排名排序"
              />
            </th>
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
            <th aria-sort={getAriaSort(sortKey === "scene_rule_score", sortDirection)}>
              <TableSortButton
                label="scene得分"
                isActive={sortKey === "scene_rule_score"}
                direction={sortDirection}
                onClick={() => toggleSort("scene_rule_score")}
                title="按 scene 得分排序"
              />
            </th>
            <th aria-sort={getAriaSort(sortKey === "contribution_pct", sortDirection)}>
              <TableSortButton
                label="scene贡献占比"
                isActive={sortKey === "contribution_pct"}
                direction={sortDirection}
                onClick={() => toggleSort("contribution_pct")}
                title="按 scene 贡献占比排序"
              />
            </th>
          </tr>
        </thead>
        <tbody>
          {sortedRows.map((item) => {
            const stageToken = getSceneStageToken(item.stage);
            const rowKey = buildSceneRowKey(item.sceneRow);
            const isSelected = selectedKey === rowKey;
            return (
              <tr
                className={isSelected ? "details-table-current-row" : ""}
                key={rowKey}
                onClick={() => onSelectRow?.(item.sceneRow)}
              >
                <td>
                  <button
                    className="details-scene-link-btn"
                    type="button"
                    onClick={(event) => {
                      event.stopPropagation();
                      onSelectRow?.(item.sceneRow);
                    }}
                  >
                    {formatFieldValue(item.sceneName)}
                  </button>
                </td>
                <td>
                  <span className={`details-scene-stage-chip is-${stageToken}`}>
                    {toSceneStageLabel(item.stage)}
                  </span>
                </td>
                <td>{item.sceneRank === null ? "--" : `#${item.sceneRank}`}</td>
                <td>{formatFieldValue(item.stageScore)}</td>
                <td>{formatFieldValue(item.riskScore)}</td>
                <td>{formatFieldValue(item.hitDate)}</td>
                <td>{formatFieldValue(item.lag)}</td>
                <td>{formatFieldValue(item.sceneRuleScore)}</td>
                <td>
                  {item.contributionPctDisplay === null
                    ? "--"
                    : `${item.contributionPctDisplay.toFixed(1)}%`}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
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

function OverviewSummarySection({
  rows,
  conceptText,
  conceptCount,
  overviewCardRef,
}: {
  rows: FieldRow[];
  conceptText: string;
  conceptCount: number;
  overviewCardRef: { current: HTMLElement | null };
}) {
  return (
    <section
      className="details-card details-overview-card"
      ref={overviewCardRef}
    >
      <h3 className="details-subtitle">总览</h3>
      <div className="details-overview-card-body">
        {renderFieldGrid(rows)}
        <div className="details-concept-block">
          <div className="details-concept-head">
            <strong>概念</strong>
            <span>{conceptCount > 0 ? `${conceptCount} 项` : "暂无概念信息"}</span>
          </div>
          <div className="details-concept-panel">
            {conceptCount > 0 ? (
              <div className="details-concept-text" title={conceptText}>
                {conceptText}
              </div>
            ) : (
              <div className="details-empty details-empty-soft">暂无概念信息</div>
            )}
          </div>
        </div>
      </div>
    </section>
  );
}

function SimilaritySection({
  data,
  onSelectStock,
}: {
  data: StockSimilarityPageData | null | undefined;
  onSelectStock: (row: StockSimilarityRow) => void;
}) {
  const items = data?.items ?? [];

  return (
    <section className="details-card details-rank-card details-similarity-card">
      <div className="details-section-head details-section-head-strategy details-similarity-head">
        <div>
          <h3 className="details-subtitle">相似股票</h3>
        </div>
      </div>
      <div className="details-rank-card-body details-similarity-card-body">
        {items.length === 0 ? (
          <div className="details-empty details-empty-soft">
            暂无相似股票
          </div>
        ) : (
          <div className="details-table-wrap details-similarity-table-wrap">
            <table className="details-table details-similarity-table">
              <thead>
                <tr>
                  <th>股票</th>
                  <th>相似度</th>
                  <th>总榜</th>
                </tr>
              </thead>
              <tbody>
                {items.map((row) => (
                  <tr key={row.tsCode}>
                    <td>
                      <button
                        className="details-similarity-stock-btn"
                        type="button"
                        onClick={() => onSelectStock(row)}
                      >
                        <strong>{row.name?.trim() || splitTsCode(row.tsCode)}</strong>
                        <span className="details-similarity-stock-code">
                          {row.tsCode}
                        </span>
                      </button>
                      <div className="details-similarity-meta" title={buildSimilarityReasonText(row)}>
                        {buildSimilarityReasonText(row)}
                      </div>
                    </td>
                    <td>{formatNumber(row.similarityScore, 1)}</td>
                    <td>{row.rank === null || row.rank === undefined ? "--" : `#${row.rank}`}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </section>
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
  const [inlineNavigationItems, setInlineNavigationItems] = useState<
    DetailsNavigationItem[] | null
  >(null);
  const [overviewCardHeight, setOverviewCardHeight] = useState<number | null>(
    null,
  );

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
  const [watchObserveSaving, setWatchObserveSaving] = useState(false);
  const [detailRealtimeData, setDetailRealtimeData] =
    useState<StockDetailRealtimeData | null>(null);
  const [detailCyqData, setDetailCyqData] = useState<StockDetailCyqData | null>(
    null,
  );
  const [detailCyqVisible, setDetailCyqVisible] = useState(false);
  const [detailCyqLoading, setDetailCyqLoading] = useState(false);
  const [detailCyqError, setDetailCyqError] = useState("");
  const [detailRealtimeLoading, setDetailRealtimeLoading] = useState(false);
  const [detailRealtimeNotice, setDetailRealtimeNotice] = useState("");
  const [detailRealtimePinned, setDetailRealtimePinned] = useState(false);
  const [detailsNavAutoDirection, setDetailsNavAutoDirection] =
    useState<DetailsAutoNavDirection | null>(null);
  const [detailsNavLongPressIntervalSeconds, setDetailsNavLongPressIntervalSeconds] =
    useState(() => readStoredDetailsNavLongPressIntervalSeconds());
  const [chartLayoutWidth, setChartLayoutWidth] = useState(() =>
    typeof window === "undefined" ? CHART_VIEWBOX_WIDTH : window.innerWidth,
  );
  const [chartMainWidthRatio, setChartMainWidthRatio] = useState(() =>
    readStoredChartMainWidthRatio(),
  );
  const [chartIndicatorWidthRatio, setChartIndicatorWidthRatio] = useState(() =>
    readStoredChartIndicatorWidthRatio(),
  );
  const [chartRankMarkerThreshold, setChartRankMarkerThreshold] = useState(() =>
    readStoredChartRankMarkerThreshold(),
  );
  const [strategyCompareSnapshot, setStrategyCompareSnapshot] =
    useState<StrategyCompareSnapshot | null>(
      externalStrategyCompareSnapshot ?? null,
    );
  const [sceneDetailModalOpen, setSceneDetailModalOpen] = useState(false);
  const [sceneDetailTarget, setSceneDetailTarget] =
    useState<DetailSceneTriggerRow | null>(null);
  const [activeIntervalContext, setActiveIntervalContext] =
    useState<IntervalRestoreRequest | null>(null);
  const [pendingIntervalRestore, setPendingIntervalRestore] =
    useState<IntervalRestoreRequest | null>(null);
  const [chartIntervalNotice, setChartIntervalNotice] = useState("");
  const [chartIntervalMode, setChartIntervalMode] = useState(false);
  const [chartIntervalSelection, setChartIntervalSelection] =
    useState<ResolvedIntervalRestore | null>(null);
  const [chartIntervalDraftSelection, setChartIntervalDraftSelection] =
    useState<ResolvedIntervalRestore | null>(null);
  const [chartIntervalPanelOpen, setChartIntervalPanelOpen] = useState(false);
  const chartDragRef = useRef<ChartDragState | null>(null);
  const chartCardRef = useRef<HTMLElement | null>(null);
  const overviewCardRef = useRef<HTMLElement | null>(null);
  const strategyGridRef = useRef<HTMLDivElement | null>(null);
  const strategyResizePointerIdRef = useRef<number | null>(null);
  const currentRankRowRef = useRef<HTMLTableRowElement | null>(null);
  const rankTableWrapRef = useRef<HTMLDivElement | null>(null);
  const pendingPageScrollRef = useRef<ScrollSnapshot | null>(null);
  const autoFillTopRef = useRef(true);
  const detailRealtimeLongPressTimerRef = useRef<number | null>(null);
  const detailRealtimeLongPressHandledRef = useRef(false);
  const detailRealtimeAutoRefreshKeyRef = useRef("");
  const detailCyqRequestKeyRef = useRef("");
  const detailsNavLongPressTimerRef = useRef<number | null>(null);
  const detailsNavLongPressHandledRef = useRef(false);
  const strategyCompareRequestKeyRef = useRef("");

  const sourcePathTrimmed = sourcePath.trim();
  const isLinkedOverlay = variant === "linked-overlay";
  const routeTsCode = sanitizeCodeInput(searchParams.get("tsCode") ?? "");
  const routeTradeDate = searchParams.get("tradeDate")?.trim() ?? "";
  const routeIntervalStartTradeDate =
    searchParams.get("intervalStartTradeDate")?.trim() ?? "";
  const routeIntervalEndTradeDate =
    searchParams.get("intervalEndTradeDate")?.trim() ?? "";
  const routeSourcePath = searchParams.get("sourcePath")?.trim() ?? "";
  const routeIntervalRestore = useMemo(
    () =>
      normalizeIntervalRestoreRequest(
        routeIntervalStartTradeDate,
        routeIntervalEndTradeDate,
      ),
    [routeIntervalEndTradeDate, routeIntervalStartTradeDate],
  );
  const routeEffectiveTradeDate =
    routeIntervalEndTradeDate || routeTradeDate;
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
            intervalStartTradeDate:
              item.intervalStartTradeDate?.trim() || undefined,
            intervalEndTradeDate: item.intervalEndTradeDate?.trim() || undefined,
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
      intervalRestore?: IntervalRestoreRequest | null,
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
          chartWindowDays: DETAIL_CHART_WINDOW_DAYS,
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

  const loadDetailCyq = useCallback(
    async (nextSourcePath: string, nextTsCode: string) => {
      const normalizedSourcePath = nextSourcePath.trim();
      const normalizedTsCode = nextTsCode.trim();
      if (normalizedSourcePath === "" || normalizedTsCode === "") {
        setDetailCyqData(null);
        setDetailCyqError("");
        return null;
      }

      const requestKey = `${normalizedSourcePath}|${normalizedTsCode}`;
      if (detailCyqRequestKeyRef.current === requestKey) {
        return null;
      }

      detailCyqRequestKeyRef.current = requestKey;
      setDetailCyqLoading(true);
      setDetailCyqError("");

      try {
        const cyq = await getStockDetailCyq({
          sourcePath: normalizedSourcePath,
          tsCode: normalizedTsCode,
        });
        if (detailCyqRequestKeyRef.current !== requestKey) {
          return null;
        }
        setDetailCyqData(cyq);
        return cyq;
      } catch (error) {
        if (detailCyqRequestKeyRef.current === requestKey) {
          setDetailCyqData(null);
          setDetailCyqError(`读取筹码分布失败: ${String(error)}`);
        }
        return null;
      } finally {
        if (detailCyqRequestKeyRef.current === requestKey) {
          detailCyqRequestKeyRef.current = "";
          setDetailCyqLoading(false);
        }
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
    setActiveIntervalContext(null);
    setPendingIntervalRestore(null);
    setChartIntervalNotice("");
    setChartIntervalMode(false);
    setChartIntervalSelection(null);
    setChartIntervalDraftSelection(null);
    setChartIntervalPanelOpen(false);
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
    setInlineNavigationItems(null);
    setActiveIntervalContext(null);
    setPendingIntervalRestore(null);
    setChartIntervalNotice("");
    setChartIntervalMode(false);
    setChartIntervalSelection(null);
    setChartIntervalDraftSelection(null);
    setChartIntervalPanelOpen(false);
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
    intervalRestore?: IntervalRestoreRequest | null,
  ) {
    if (sourcePathTrimmed === "" || nextNormalizedCode.trim() === "") {
      return;
    }

    autoFillTopRef.current = false;
    if (nextLookupValue !== undefined) {
      setLookupInput(nextLookupValue);
    }
    setActiveIntervalContext(intervalRestore ?? null);
    setPendingIntervalRestore(intervalRestore ?? null);
    setChartIntervalNotice("");
    setChartIntervalMode(Boolean(intervalRestore));
    setChartIntervalSelection(null);
    setChartIntervalDraftSelection(null);
    setChartIntervalPanelOpen(false);
    setDetailError("");
    setStrategyCompareSnapshot(null);
    void readDetail(
      sourcePathTrimmed,
      nextTradeDate,
      nextNormalizedCode,
      intervalRestore ?? null,
    );
  }

  function onSelectStockCandidate(row: StockLookupRow) {
    const nextCode = getLookupDigits(row.ts_code);
    if (nextCode === "") {
      return;
    }

    setLookupFocused(false);
    setInlineNavigationItems(null);
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

    setInlineNavigationItems(null);
    onAutoReadDetail(
      nextTradeDate,
      stdTsCode(nextCode),
      matchedRow?.name?.trim() || nextCode,
    );
  }

  function onSelectSimilarityRow(row: StockSimilarityRow) {
    const intervalRestore = activeIntervalContext;
    const similarityNavigationItems: DetailsNavigationItem[] =
      stockSimilarity?.items.map((item) => ({
        tsCode: item.tsCode,
        tradeDate:
          stockSimilarity.resolvedTradeDate ||
          detailData?.resolved_trade_date ||
          tradeDateInput.trim() ||
          null,
        intervalStartTradeDate: intervalRestore?.startTradeDate ?? null,
        intervalEndTradeDate: intervalRestore?.endTradeDate ?? null,
        sourcePath: sourcePathTrimmed || null,
        name: item.name?.trim() || splitTsCode(item.tsCode),
      })) ?? [];
    const nextTradeDate =
      stockSimilarity?.resolvedTradeDate?.trim() ||
      detailData?.resolved_trade_date?.trim() ||
      tradeDateInput.trim();
    const lookupValue = row.name?.trim() || getLookupDigits(row.tsCode);
    setInlineNavigationItems(
      similarityNavigationItems.length > 0 ? similarityNavigationItems : null,
    );
    onAutoReadDetail(nextTradeDate, row.tsCode, lookupValue, intervalRestore);
  }

  function onSelectPrevRankTradeDate(nextTradeDate: string) {
    const tradeDate = nextTradeDate.trim();
    if (!tradeDate || resolvedTsCode === "--") {
      return;
    }

    const lookupValue =
      detailData?.overview?.name?.trim() || getLookupDigits(resolvedTsCode);
    setInlineNavigationItems(null);
    onAutoReadDetail(tradeDate, resolvedTsCode, lookupValue);
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
  const stockSimilarity = detailData?.stock_similarity ?? null;
  useEffect(() => {
    setDetailCyqData(null);
    setDetailCyqError("");
    detailCyqRequestKeyRef.current = "";
  }, [resolvedTsCode, sourcePathTrimmed]);

  useEffect(() => {
    if (!detailCyqVisible || resolvedTsCode === "--" || sourcePathTrimmed === "") {
      return;
    }
    if (detailCyqData?.resolved_ts_code === resolvedTsCode) {
      return;
    }
    void loadDetailCyq(sourcePathTrimmed, resolvedTsCode);
  }, [
    detailCyqData?.resolved_ts_code,
    detailCyqVisible,
    loadDetailCyq,
    resolvedTsCode,
    sourcePathTrimmed,
  ]);
  const overviewGridStyle = useMemo(() => {
    if (overviewCardHeight === null) {
      return undefined;
    }
    return {
      ["--details-side-stack-height" as const]: `${overviewCardHeight}px`,
    } as CSSProperties;
  }, [overviewCardHeight]);
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
  const selectedSceneRowKey = sceneDetailTarget
    ? buildSceneRowKey(sceneDetailTarget)
    : null;

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
  const {
    sortKey: prevRankSortKey,
    sortDirection: prevRankSortDirection,
    sortedRows: sortedPrevRanks,
    toggleSort: togglePrevRankSort,
  } = useTableSort<DetailPrevRankRow, PrevRankSortKey>(
    prevRanks,
    prevRankSortDefinitions,
  );
  const kline = detailRealtimeData?.kline ?? detailData?.kline;
  const allChartItems = kline?.items ?? EMPTY_KLINE_ROWS;
  const detailCyqSnapshots = detailCyqData?.snapshots ?? [];
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
  const selectedCyqTradeDate =
    chartFocus && chartFocus.absoluteIndex >= 0 && chartFocus.absoluteIndex < allChartItems.length
      ? allChartItems[chartFocus.absoluteIndex]?.trade_date ?? null
      : allChartItems[allChartItems.length - 1]?.trade_date ??
        (resolvedTradeDate !== "--" ? resolvedTradeDate : null);
  const selectedCyqSnapshot = useMemo(
    () => findCyqSnapshotForTradeDate(detailCyqSnapshots, selectedCyqTradeDate),
    [detailCyqSnapshots, selectedCyqTradeDate],
  );
  const chartLayoutSlotCount = getChartLayoutSlotCount(
    chartItems.length,
    totalChartItems,
  );
  const panels = kline?.panels?.length ? kline.panels : buildDefaultPanels();
  const chartMainPanelHeight = chartLayoutWidth * chartMainWidthRatio;
  const chartIndicatorTotalHeight = chartLayoutWidth * chartIndicatorWidthRatio;
  const chartMinHeight =
    chartLayoutWidth <= CHART_MOBILE_BREAKPOINT
      ? CHART_MIN_HEIGHT_MOBILE
      : CHART_MIN_HEIGHT_DESKTOP;
  const chartPanelGapTotal = Math.max(0, panels.length - 1) * CHART_PANEL_GAP_PX;
  const chartShellHeight = Math.max(
    chartMainPanelHeight + chartIndicatorTotalHeight + chartPanelGapTotal,
    chartMinHeight,
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
      : inlineNavigationItems && inlineNavigationItems.length > 0
        ? inlineNavigationItems
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
  const isPrevAutoLocked = detailsNavAutoDirection === "prev";
  const isNextAutoLocked = detailsNavAutoDirection === "next";
  const rankLookup = buildRankLookup(detailData?.overview, prevRanks);
  const rankNumberLookup = useMemo(
    () => buildRankNumberLookup(detailData?.overview, prevRanks),
    [detailData?.overview, prevRanks],
  );
  const chartRangeText =
    chartItems.length > 0
      ? `${chartItems[0].trade_date} -> ${chartItems[chartItems.length - 1].trade_date}`
      : "--";
  const cyqToggleTitle = detailCyqLoading
    ? "筹码分布加载中..."
    : detailCyqError
      ? detailCyqError
      : detailCyqVisible
        ? `隐藏筹码分布${selectedCyqTradeDate ? `（${selectedCyqTradeDate}）` : ""}`
        : "显示筹码分布";
  const strategyGridStyle = useMemo(
    () =>
      isStrategyStacked
        ? undefined
          : {
              gridTemplateColumns: `minmax(0, ${strategySplitRatio}fr) 14px minmax(0, ${1 - strategySplitRatio}fr)`,
            },
    [isStrategyStacked, strategySplitRatio],
  );
  const intervalStatsSections = useMemo(
    () => buildIntervalStatsSections(allChartItems, chartIntervalSelection),
    [allChartItems, chartIntervalSelection],
  );

  const closeChartIntervalPanel = useCallback(() => {
    setChartIntervalMode(false);
    setChartIntervalSelection(null);
    setChartIntervalDraftSelection(null);
    setChartIntervalPanelOpen(false);
    setActiveIntervalContext(null);
    setPendingIntervalRestore(null);
    setChartIntervalNotice("");
  }, []);

  const toggleChartIntervalMode = useCallback(() => {
    if (chartIntervalMode || chartIntervalSelection || chartIntervalDraftSelection) {
      closeChartIntervalPanel();
      return;
    }

    setChartIntervalMode(true);
    setChartFocus(null);
    setChartIntervalPanelOpen(false);
    setChartIntervalNotice("");
  }, [
    chartIntervalDraftSelection,
    chartIntervalMode,
    chartIntervalSelection,
    closeChartIntervalPanel,
  ]);

  useLayoutEffect(() => {
    const element = overviewCardRef.current;
    if (!element) {
      setOverviewCardHeight(null);
      return;
    }

    const syncHeight = () => {
      const nextHeight = Math.round(element.getBoundingClientRect().height);
      setOverviewCardHeight(nextHeight > 0 ? nextHeight : null);
    };

    syncHeight();
    if (typeof ResizeObserver === "undefined") {
      return;
    }

    const observer = new ResizeObserver(() => {
      syncHeight();
    });
    observer.observe(element);
    return () => {
      observer.disconnect();
    };
  }, [conceptText, conceptItems.length, overviewRows.length]);

  useEffect(() => {
    if (totalChartItems === 0) {
      chartDragRef.current = null;
      setVisibleBarCount(DEFAULT_VISIBLE_BARS);
      setVisibleStartIndex(0);
      setChartFocus(null);
      setChartIntervalSelection(null);
      setChartIntervalDraftSelection(null);
      setChartIntervalPanelOpen(false);
      if (pendingIntervalRestore) {
        setChartIntervalNotice("区间超出当前图表窗口，已回退为单参考日。");
        setActiveIntervalContext(null);
        setChartIntervalMode(false);
        setPendingIntervalRestore(null);
      }
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

    if (pendingIntervalRestore) {
      const resolvedIntervalRestore = resolveIntervalRestore(
        allChartItems,
        pendingIntervalRestore,
      );
      if (resolvedIntervalRestore) {
        const intervalBarCount =
          resolvedIntervalRestore.endAbsoluteIndex -
            resolvedIntervalRestore.startAbsoluteIndex +
          1;
        const restoredVisibleBarCount = clampNumber(
          Math.max(DEFAULT_VISIBLE_BARS, intervalBarCount + 20),
          Math.min(MIN_VISIBLE_BARS, totalChartItems),
          totalChartItems,
        );
        const intervalMidpoint = Math.floor(
          (resolvedIntervalRestore.startAbsoluteIndex +
            resolvedIntervalRestore.endAbsoluteIndex) /
            2,
        );
        const restoredVisibleStart = clampNumber(
          intervalMidpoint - Math.floor(restoredVisibleBarCount / 2),
          0,
          totalChartItems - restoredVisibleBarCount,
        );

        chartDragRef.current = null;
        setVisibleBarCount(restoredVisibleBarCount);
        setVisibleStartIndex(restoredVisibleStart);
        setChartFocus(null);
        setActiveIntervalContext({
          startTradeDate: resolvedIntervalRestore.startTradeDate,
          endTradeDate: resolvedIntervalRestore.endTradeDate,
        });
        setChartIntervalMode(true);
        setChartIntervalSelection(resolvedIntervalRestore);
        setChartIntervalDraftSelection(null);
        setChartIntervalPanelOpen(true);
        setChartIntervalNotice(buildIntervalRestoreNotice(
          pendingIntervalRestore,
          resolvedIntervalRestore,
        ));
        setPendingIntervalRestore(null);
        return;
      }

      setChartIntervalNotice("区间超出当前图表窗口，已回退为单参考日。");
      setActiveIntervalContext(null);
      setChartIntervalMode(false);
      setChartIntervalSelection(null);
      setChartIntervalDraftSelection(null);
      setChartIntervalPanelOpen(false);
      setPendingIntervalRestore(null);
    }

    chartDragRef.current = null;
    setVisibleBarCount(nextVisibleBarCount);
    setVisibleStartIndex(nextVisibleStart);
    setChartFocus(null);
  }, [
    allChartItems,
    detailData?.resolved_trade_date,
    detailData?.resolved_ts_code,
    pendingIntervalRestore,
    resolvedTradeDate,
    totalChartItems,
  ]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    const updateChartLayoutWidth = () => {
      const cardWidth = chartCardRef.current?.getBoundingClientRect().width;
      const nextWidth =
        typeof cardWidth === "number" && cardWidth > 0
          ? cardWidth
          : window.innerWidth;
      setChartLayoutWidth(nextWidth);
    };

    updateChartLayoutWidth();
    window.addEventListener("resize", updateChartLayoutWidth);

    const card = chartCardRef.current;
    const observer =
      typeof ResizeObserver === "undefined" || !card
        ? null
        : new ResizeObserver(() => {
            updateChartLayoutWidth();
          });
    if (observer && card) {
      observer.observe(card);
    }

    return () => {
      window.removeEventListener("resize", updateChartLayoutWidth);
      observer?.disconnect();
    };
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    const onStorage = (event: StorageEvent) => {
      if (
        event.key === null ||
        event.key === "lh_chart_main_width_ratio_v1" ||
        event.key === "lh_chart_indicator_width_ratio_v1" ||
        event.key === "lh_chart_rank_marker_threshold_v1" ||
        event.key === "lh_details_nav_long_press_interval_seconds_v1"
      ) {
        setChartMainWidthRatio(readStoredChartMainWidthRatio());
        setChartIndicatorWidthRatio(readStoredChartIndicatorWidthRatio());
        setChartRankMarkerThreshold(readStoredChartRankMarkerThreshold());
        setDetailsNavLongPressIntervalSeconds(
          readStoredDetailsNavLongPressIntervalSeconds(),
        );
      }
    };

    setChartMainWidthRatio(readStoredChartMainWidthRatio());
    setChartIndicatorWidthRatio(readStoredChartIndicatorWidthRatio());
    setChartRankMarkerThreshold(readStoredChartRankMarkerThreshold());
    setDetailsNavLongPressIntervalSeconds(
      readStoredDetailsNavLongPressIntervalSeconds(),
    );
    window.addEventListener("storage", onStorage);
    return () => {
      window.removeEventListener("storage", onStorage);
    };
  }, []);

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
    setSceneDetailTarget((current) => {
      if (!current) {
        return current;
      }

      const currentSceneName = current.scene_name.trim().toLowerCase();
      if (currentSceneName === "") {
        return current;
      }

      const matchedRow =
        sceneRows.find(
          (row) => row.scene_name.trim().toLowerCase() === currentSceneName,
        ) ?? null;
      if (!matchedRow) {
        return current;
      }

      return matchedRow;
    });
  }, [sceneRows]);

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

    void preloadWatchObserveRows(sourcePathTrimmed).catch(() => {});

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
      if (detailsNavLongPressTimerRef.current !== null) {
        window.clearTimeout(detailsNavLongPressTimerRef.current);
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
    setTradeDateInput(routeEffectiveTradeDate);
    setActiveIntervalContext(routeIntervalRestore);
    setPendingIntervalRestore(routeIntervalRestore);
    setChartIntervalNotice("");
    setChartIntervalMode(Boolean(routeIntervalRestore));
    setChartIntervalSelection(null);
    setChartIntervalDraftSelection(null);
    setChartIntervalPanelOpen(false);
    setLookupInput(routeTsCode);
    setTopError("");
    setDetailError("");
    setStrategyCompareSnapshot(null);
    void readDetail(
      nextSourcePath,
      routeEffectiveTradeDate,
      stdTsCode(routeTsCode),
      routeIntervalRestore,
    );
  }, [
    readDetail,
    routeEffectiveTradeDate,
    routeIntervalRestore,
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
    const reserveCyqPanelWidth =
      detailCyqVisible && (selectedCyqSnapshot?.bins.length ?? 0) > 0;
    const pointer = buildChartPointerSnapshot(
      viewport,
      clientX,
      clientY,
      chartItems.length,
      chartLayoutSlotCount,
      reserveCyqPanelWidth,
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

  function buildChartIntervalSelection(
    viewport: HTMLDivElement,
    clientX: number,
    clientY: number,
    anchorAbsoluteIndex: number,
  ) {
    const reserveCyqPanelWidth =
      detailCyqVisible && (selectedCyqSnapshot?.bins.length ?? 0) > 0;
    const pointer = buildChartPointerSnapshot(
      viewport,
      clientX,
      clientY,
      chartItems.length,
      chartLayoutSlotCount,
      reserveCyqPanelWidth,
    );
    if (!pointer) {
      return null;
    }

    return buildIntervalSelectionFromAbsoluteIndices(
      allChartItems,
      anchorAbsoluteIndex,
      effectiveVisibleStart + pointer.visibleIndex,
    );
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
    let anchorAbsoluteIndex: number | undefined;
    const mode =
      panelKey === "price" && chartIntervalMode
        ? (() => {
            const reserveCyqPanelWidth =
              detailCyqVisible && (selectedCyqSnapshot?.bins.length ?? 0) > 0;
            const pointer = buildChartPointerSnapshot(
              event.currentTarget,
              event.clientX,
              event.clientY,
              chartItems.length,
              chartLayoutSlotCount,
              reserveCyqPanelWidth,
            );
            if (!pointer) {
              return "interval-select" as const;
            }

            anchorAbsoluteIndex = effectiveVisibleStart + pointer.visibleIndex;
            const intervalSelection = buildIntervalSelectionFromAbsoluteIndices(
              allChartItems,
              anchorAbsoluteIndex,
              anchorAbsoluteIndex,
            );
            if (intervalSelection) {
              setChartIntervalDraftSelection(intervalSelection);
              setChartIntervalPanelOpen(false);
              setChartFocus(null);
            }
            return "interval-select" as const;
          })()
        : chartFocus?.pinned
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

    if (mode === "interval-select" && anchorAbsoluteIndex === undefined) {
      setChartIntervalDraftSelection(null);
      return;
    }

    try {
      event.currentTarget.setPointerCapture(event.pointerId);
    } catch {
      // Ignore browsers that do not support pointer capture for this target.
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
      anchorAbsoluteIndex,
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

    if (dragState.mode === "interval-select") {
      const nextIntervalSelection = buildChartIntervalSelection(
        event.currentTarget,
        event.clientX,
        event.clientY,
        dragState.anchorAbsoluteIndex ?? effectiveVisibleStart,
      );
      if (!nextIntervalSelection) {
        return;
      }

      setChartIntervalDraftSelection(nextIntervalSelection);
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
        // Ignore stale pointer capture state during cleanup.
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

    if (dragState.mode === "interval-select") {
      const nextIntervalSelection = buildChartIntervalSelection(
        event.currentTarget,
        event.clientX,
        event.clientY,
        dragState.anchorAbsoluteIndex ?? effectiveVisibleStart,
      );
      if (nextIntervalSelection) {
        setChartIntervalDraftSelection(null);
        setChartIntervalSelection(nextIntervalSelection);
        setActiveIntervalContext({
          startTradeDate: nextIntervalSelection.startTradeDate,
          endTradeDate: nextIntervalSelection.endTradeDate,
        });
        setChartIntervalMode(true);
        setChartIntervalPanelOpen(true);
        setChartIntervalNotice("");
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
    if (chartDragRef.current?.mode === "interval-select") {
      setChartIntervalDraftSelection(null);
    }
    clearChartPointerState(event);
  }

  async function onAddWatchObserve() {
    if (resolvedTsCode === "--" || watchObserveSaving) {
      return;
    }

    const isRemoving = currentWatchObserveItem !== null;
    try {
      setWatchObserveSaving(true);
      setWatchObserveNotice(isRemoving ? "正在取消自选..." : "正在加入自选...");
      await waitForNextPaint();

      if (isRemoving) {
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
        isRemoving ? "取消自选失败" : "加入自选失败",
      );
    } finally {
      setWatchObserveSaving(false);
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

    const chartWindowDays = resolveChartWindowDays(
      chartIntervalSelection
        ? {
            startTradeDate: chartIntervalSelection.startTradeDate,
            endTradeDate: chartIntervalSelection.endTradeDate,
          }
        : activeIntervalContext,
    );

    setDetailRealtimeLoading(true);
    setDetailRealtimeNotice("");
    try {
      const nextRealtimeData = await getStockDetailRealtime({
        sourcePath: sourcePathTrimmed,
        tsCode: resolvedTsCode,
        chartWindowDays: DETAIL_CHART_WINDOW_DAYS,
      });
      setDetailRealtimeData(nextRealtimeData);
    } catch (error) {
      setDetailRealtimeNotice(`刷新实时失败: ${String(error)}`);
    } finally {
      setDetailRealtimeLoading(false);
    }
  }, [activeIntervalContext, chartIntervalSelection, resolvedTsCode, sourcePathTrimmed]);

  const onToggleCyqPanel = useCallback(async () => {
    const nextVisible = !detailCyqVisible;
    setDetailCyqVisible(nextVisible);
    if (!nextVisible || resolvedTsCode === "--" || sourcePathTrimmed === "") {
      return;
    }
    if (detailCyqData?.resolved_ts_code === resolvedTsCode) {
      return;
    }
    await loadDetailCyq(sourcePathTrimmed, resolvedTsCode);
  }, [
    detailCyqData?.resolved_ts_code,
    detailCyqVisible,
    loadDetailCyq,
    resolvedTsCode,
    sourcePathTrimmed,
  ]);

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
    setActiveIntervalContext(null);
    setPendingIntervalRestore(null);
    setChartIntervalMode(false);
    setChartIntervalSelection(null);
    setChartIntervalDraftSelection(null);
    setChartIntervalPanelOpen(false);
    setChartIntervalNotice("");
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
    const explicitIntervalRestore = normalizeIntervalRestoreRequest(
      target.intervalStartTradeDate ?? "",
      target.intervalEndTradeDate ?? "",
    );
    const nextIntervalRestore = explicitIntervalRestore ?? activeIntervalContext;

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
    setActiveIntervalContext(nextIntervalRestore);
    setPendingIntervalRestore(nextIntervalRestore);
    setChartIntervalMode(Boolean(nextIntervalRestore));
    setChartIntervalSelection(null);
    setChartIntervalDraftSelection(null);
    setChartIntervalPanelOpen(false);
    setChartIntervalNotice("");
    if (nextTradeDate !== "") {
      setTradeDateInput(nextTradeDate);
    }
    setDetailError("");
    setStrategyCompareSnapshot(null);
    void readDetail(
      nextSourcePath,
      nextTradeDate,
      stdTsCode(nextCode),
      nextIntervalRestore,
    );
  }

  const clearDetailsNavLongPressTimer = useCallback(() => {
    if (detailsNavLongPressTimerRef.current !== null) {
      window.clearTimeout(detailsNavLongPressTimerRef.current);
      detailsNavLongPressTimerRef.current = null;
    }
  }, []);

  const toggleDetailsNavAutoDirection = useCallback(
    (direction: DetailsAutoNavDirection) => {
      setDetailsNavAutoDirection((current) =>
        current === direction ? null : direction,
      );
      detailsNavLongPressHandledRef.current = true;
    },
    [],
  );

  const handleDetailsNavPointerDown = useCallback(
    (
      direction: DetailsAutoNavDirection,
      event: ReactPointerEvent<HTMLButtonElement>,
    ) => {
      event.stopPropagation();

      const targetItem =
        direction === "prev" ? prevNavigationItem : nextNavigationItem;
      if (!targetItem || detailLoading) {
        return;
      }

      detailsNavLongPressHandledRef.current = false;
      clearDetailsNavLongPressTimer();
      const longPressMs =
        event.pointerType === 'touch'
          ? DETAIL_NAV_LONG_PRESS_TOUCH_MS
          : DETAIL_NAV_LONG_PRESS_MS;
      detailsNavLongPressTimerRef.current = window.setTimeout(() => {
        toggleDetailsNavAutoDirection(direction);
      }, longPressMs);
    },
    [
      clearDetailsNavLongPressTimer,
      detailLoading,
      nextNavigationItem,
      prevNavigationItem,
      toggleDetailsNavAutoDirection,
    ],
  );

  const handleDetailsNavPointerRelease = useCallback(
    (event: ReactPointerEvent<HTMLButtonElement>) => {
      event.stopPropagation();
      clearDetailsNavLongPressTimer();
    },
    [clearDetailsNavLongPressTimer],
  );

  const handleDetailsNavContextMenu = useCallback(
    (event: ReactPointerEvent<HTMLButtonElement>) => {
      event.preventDefault();
      event.stopPropagation();
    },
    [],
  );

  const handleDetailsNavClick = useCallback(
    (
      direction: DetailsAutoNavDirection,
      event: ReactMouseEvent<HTMLButtonElement>,
    ) => {
      if (detailsNavLongPressHandledRef.current) {
        event.preventDefault();
        detailsNavLongPressHandledRef.current = false;
        return;
      }

      detailsNavLongPressHandledRef.current = false;
      if (direction === "prev") {
        onJumpNavigationTarget(prevNavigationItem);
        return;
      }
      onJumpNavigationTarget(nextNavigationItem);
    },
    [nextNavigationItem, onJumpNavigationTarget, prevNavigationItem],
  );

  useEffect(() => {
    if (!detailsNavAutoDirection) {
      return;
    }

    const intervalMs = Math.max(
      200,
      Math.round(detailsNavLongPressIntervalSeconds * 1000),
    );
    const intervalId = window.setInterval(() => {
      if (detailLoading) {
        return;
      }

      if (detailsNavAutoDirection === "prev") {
        if (!prevNavigationItem) {
          setDetailsNavAutoDirection(null);
          return;
        }
        onJumpNavigationTarget(prevNavigationItem);
        return;
      }

      if (!nextNavigationItem) {
        setDetailsNavAutoDirection(null);
        return;
      }
      onJumpNavigationTarget(nextNavigationItem);
    }, intervalMs);

    return () => {
      window.clearInterval(intervalId);
    };
  }, [
    detailLoading,
    detailsNavAutoDirection,
    detailsNavLongPressIntervalSeconds,
    nextNavigationItem,
    onJumpNavigationTarget,
    prevNavigationItem,
  ]);

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

      <section className="details-card details-chart-card" ref={chartCardRef}>
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
            gridTemplateRows: buildChartTemplateRows(
              kline,
              panels,
              chartMainPanelHeight,
              chartIndicatorTotalHeight,
              chartMinHeight,
            ),
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
              chartRankMarkerThreshold,
              rankNumberLookup,
              onChartPointerDown,
              onChartPointerMove,
              onChartPointerUp,
              onChartPointerLeave,
              onChartPointerCancel,
              detailCyqVisible,
              selectedCyqSnapshot,
              selectedCyqTradeDate,
              null,
              <div className="details-chart-watch-action">
                <div className="details-chart-watch-row">
                  <button
                    className={[
                      "details-chart-watch-btn",
                      "details-chart-watch-btn-interval",
                      chartIntervalMode || chartIntervalSelection
                        ? "is-active"
                        : "",
                    ]
                      .filter(Boolean)
                      .join(" ")}
                    type="button"
                    data-testid="details-interval-toggle"
                    onPointerDown={(event) => {
                      event.stopPropagation();
                    }}
                    onClick={(event) => {
                      event.stopPropagation();
                      toggleChartIntervalMode();
                    }}
                  >
                    区间统计
                  </button>
                  <button
                    className={[
                      "details-chart-cyq-toggle",
                      "details-chart-cyq-toggle-inline",
                      detailCyqVisible ? "is-active" : "",
                      detailCyqLoading ? "is-loading" : "",
                    ]
                      .filter(Boolean)
                      .join(" ")}
                    type="button"
                    onPointerDown={(event) => {
                      event.stopPropagation();
                    }}
                    onClick={(event) => {
                      event.stopPropagation();
                      void onToggleCyqPanel();
                    }}
                    title={cyqToggleTitle}
                  >
                    筹
                  </button>
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
                      watchObserveSaving ? "is-pending" : "",
                      isCurrentWatched ? "is-added" : "",
                    ]
                      .filter(Boolean)
                      .join(" ")}
                    type="button"
                    disabled={
                      watchObserveSaving ||
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
                    {watchObserveSaving
                      ? isCurrentWatched
                        ? "取消中..."
                        : "加入中..."
                      : isCurrentWatched
                        ? "取消自选"
                        : "加自选"}
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
                {chartIntervalNotice ? (
                  <span className="details-chart-watch-note">
                    {chartIntervalNotice}
                  </span>
                ) : null}
              </div>,
              chartIntervalSelection,
              chartIntervalDraftSelection,
              chartIntervalPanelOpen,
              intervalStatsSections,
              closeChartIntervalPanel,
            ),
          )}
        </div>
      </section>

      <div className="details-overview-grid" style={overviewGridStyle}>
        <OverviewSummarySection
          rows={overviewRows}
          conceptText={conceptText}
          conceptCount={conceptItems.length}
          overviewCardRef={overviewCardRef}
        />

        <div className="details-side-stack">
          <section className="details-card details-rank-card details-prev-rank-card">
            <div className="details-section-head details-section-head-strategy details-prev-rank-head">
              <div>
                <h3 className="details-subtitle">前日排名</h3>
              </div>
            </div>
            <div className="details-rank-card-body details-prev-rank-card-body">
              {prevRanks.length === 0 ? (
                <div className="details-empty details-empty-soft">
                  暂无前日排名
                </div>
              ) : (
                <div className="details-table-wrap details-prev-rank-table-wrap" ref={rankTableWrapRef}>
                  <table className="details-table details-prev-rank-table">
                    <thead>
                      <tr>
                        <th
                          aria-sort={getAriaSort(
                            prevRankSortKey === "trade_date",
                            prevRankSortDirection,
                          )}
                        >
                          <TableSortButton
                            label="日期"
                            isActive={prevRankSortKey === "trade_date"}
                            direction={prevRankSortDirection}
                            onClick={() => togglePrevRankSort("trade_date")}
                            title="按日期排序"
                          />
                        </th>
                        <th
                          aria-sort={getAriaSort(
                            prevRankSortKey === "rank",
                            prevRankSortDirection,
                          )}
                        >
                          <TableSortButton
                            label="排名"
                            isActive={prevRankSortKey === "rank"}
                            direction={prevRankSortDirection}
                            onClick={() => togglePrevRankSort("rank")}
                            title="按排名排序"
                          />
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      {sortedPrevRanks.map((row) => {
                        const isReferenceDate = row.trade_date === resolvedTradeDate;
                        return (
                          <tr
                            className={[
                              isReferenceDate ? "details-table-current-row" : "",
                              "details-prev-rank-row",
                            ]
                              .filter(Boolean)
                              .join(" ")}
                            key={row.trade_date}
                            ref={isReferenceDate ? currentRankRowRef : null}
                            role="button"
                            tabIndex={0}
                            onClick={() => onSelectPrevRankTradeDate(row.trade_date)}
                            onKeyDown={(event) => {
                              if (event.key === "Enter" || event.key === " ") {
                                event.preventDefault();
                                onSelectPrevRankTradeDate(row.trade_date);
                              }
                            }}
                          >
                            <td>
                              {row.trade_date}
                              {isReferenceDate ? (
                                <span className="details-current-date-chip">
                                  参考日
                                </span>
                              ) : null}
                            </td>
                            <td>{buildRankValue(row.rank, row.total)}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </section>

          <SimilaritySection
            data={stockSimilarity}
            onSelectStock={onSelectSimilarityRow}
          />
        </div>

        <section className="details-card details-rank-card details-scene-overview-card">
          <div className="details-section-head details-section-head-strategy details-scene-overview-head">
            <div>
              <h3 className="details-subtitle">Scene 状态总览</h3>
              <p className="details-note">点击场景可打开明细浮窗；基础信息已收入口径浮窗。</p>
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

                <SceneOverviewTableSection
                  items={sceneOverviewItems}
                  emptyText="暂无 scene 状态数据"
                  selectedKey={selectedSceneRowKey}
                  onSelectRow={(row) => {
                    setSceneDetailTarget(row);
                    setSceneDetailModalOpen(true);
                  }}
                />
              </>
            )}
          </div>
        </section>
      </div>

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
              <div>
                <h3 className="details-subtitle">策略变化明细（{formatFieldValue(sceneDetailTarget?.scene_name)}）</h3>
                <p className="details-note">基础信息已收入口径浮窗，便于快速切换场景。</p>
              </div>
              <div className="details-strategy-toolbar">
                <div className="details-strategy-params">
                  <span>名称：{formatFieldValue(detailData?.overview?.name)}</span>
                  <span>代码：{formatFieldValue(resolvedTsCode)}</span>
                  <span>当前参考日：{formatFieldValue(resolvedTradeDate)}</span>
                  <span>相对日期：{formatFieldValue(strategyDisplayRelativeTradeDate)}</span>
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
          className={[
            "details-float-nav-btn",
            isPrevAutoLocked ? "is-auto-locked" : "",
          ]
            .filter(Boolean)
            .join(" ")}
          type="button"
          disabled={!prevNavigationItem || detailLoading}
          title={isPrevAutoLocked ? "长按锁定中，再长按可取消" : "长按可锁定自动切换"}
          onPointerDown={(event) => handleDetailsNavPointerDown("prev", event)}
          onPointerUp={handleDetailsNavPointerRelease}
          onPointerCancel={handleDetailsNavPointerRelease}
          onPointerLeave={handleDetailsNavPointerRelease}
          onContextMenu={handleDetailsNavContextMenu}
          onClick={(event) => handleDetailsNavClick("prev", event)}
        >
          {isPrevAutoLocked ? "自动中" : "上一条"}
        </button>
        <button
          className={[
            "details-float-nav-btn",
            isNextAutoLocked ? "is-auto-locked" : "",
          ]
            .filter(Boolean)
            .join(" ")}
          type="button"
          disabled={!nextNavigationItem || detailLoading}
          title={isNextAutoLocked ? "长按锁定中，再长按可取消" : "长按可锁定自动切换"}
          onPointerDown={(event) => handleDetailsNavPointerDown("next", event)}
          onPointerUp={handleDetailsNavPointerRelease}
          onPointerCancel={handleDetailsNavPointerRelease}
          onPointerLeave={handleDetailsNavPointerRelease}
          onContextMenu={handleDetailsNavContextMenu}
          onClick={(event) => handleDetailsNavClick("next", event)}
        >
          {isNextAutoLocked ? "自动中" : "下一条"}
        </button>
      </div>
    </div>
  );
}
