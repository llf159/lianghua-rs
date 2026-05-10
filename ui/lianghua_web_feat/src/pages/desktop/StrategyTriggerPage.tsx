import {
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type PointerEvent as ReactPointerEvent,
} from "react";
import { createPortal } from "react-dom";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import {
  getSceneStatisticsPage,
  getStrategyStatisticsDetail,
  getStrategyStatisticsPage,
  type SceneStatisticsPageData,
  type SceneStageRow,
  type StrategyDailyRow,
  type StrategyHeatmapCell,
  type StrategyStatisticsDetailData,
  type StrategyStatisticsPageData,
  type TriggeredStockRow,
} from "../../apis/strategyTrigger";
import {
  formatConceptText,
  useConceptExclusions,
} from "../../shared/conceptExclusions";
import DetailsLink from "../../shared/DetailsLink";
import {
  TableSortButton,
  getAriaSort,
  type SortDefinition,
  useTableSort,
} from "../../shared/tableSort";
import {
  readJsonStorage,
  readStoredSourcePath,
  writeJsonStorage,
} from "../../shared/storage";
import { useRouteScrollRegion } from "../../shared/routeScroll";
import "./css/StrategyTriggerPage.css";

type StrategyDailySortKey =
  | "rule_name"
  | "trigger_mode"
  | "trigger_count"
  | "contribution_score"
  | "contribution_per_trigger"
  | "top100_trigger_count"
  | "coverage"
  | "median_trigger_count"
  | "best_rank";

type TriggeredStockSortKey = "rank" | "total_score" | "rule_score";

type SceneStatisticsSortKey =
  | "scene_name"
  | "trigger_count"
  | "confirm_count"
  | "observe_count"
  | "fail_count"
  | "none_count"
  | "scene_covered_count"
  | "scene_total_sample_count"
  | "scene_coverage_ratio"
  | "scene_rule_contribution_ratio";

type SceneStatisticsTableRow = {
  scene_name: string;
  trigger_count: number;
  confirm_count: number;
  observe_count: number;
  fail_count: number;
  none_count: number;
  scene_covered_count?: number | null;
  scene_total_sample_count?: number | null;
  scene_coverage_ratio?: number | null;
  scene_rule_contribution_ratio?: number | null;
};

type HeatmapSlot = {
  key: string;
  compactDate: string | null;
  label: string;
  cell: StrategyHeatmapCell | null;
  dayOfMonth: number | null;
};

type CalendarMonth = {
  key: string;
  label: string;
  slots: HeatmapSlot[];
};

type OverviewDeltaChartGeometry = {
  width: number;
  height: number;
  viewBox: string;
  plotLeft: number;
  plotRight: number;
  plotBottom: number;
  zeroY: number | null;
  yTicks: Array<{ key: string; value: number; y: number }>;
  xLabels: Array<{
    key: string;
    value: string;
    x: number;
    anchor: "start" | "middle" | "end";
  }>;
  candles: Array<{
    key: string;
    x: number;
    centerX: number;
    y: number;
    width: number;
    height: number;
    wickTopY: number;
    wickBottomY: number;
    open: number;
    close: number;
    high: number;
    low: number;
    item: StrategyHeatmapCell;
  }>;
  latestTradeDate: string;
  latestDelta: number | null;
};

type OverviewDeltaChartFocus = {
  index: number;
  cursorXPercent: number;
  cursorYPercent: number;
  pinned: boolean;
};

type OverviewDeltaChartPointerState = {
  mode: "tap" | "focus" | "dismiss" | "scroll";
  pointerId: number;
  pointerType: string;
  startClientX: number;
  startClientY: number;
  startScrollLeft: number;
  moved: boolean;
};

type RankDistributionBucket = {
  key: string;
  label: string;
  min: number | null;
  max: number | null;
};

type RankDistributionBucketGeometry = {
  key: string;
  label: string;
  x: number;
  y: number;
  width: number;
  height: number;
  positiveWidth: number;
  negativeWidth: number;
  flatWidth: number;
  count: number;
  positiveCount: number;
  negativeCount: number;
  flatCount: number;
  bucketRatio: number | null;
  cumulativeRatio: number | null;
  avgRuleScore: number | null;
  avgTotalScore: number | null;
};

type RankDistributionGeometry = {
  width: number;
  height: number;
  viewBox: string;
  plotLeft: number;
  plotRight: number;
  plotBottom: number;
  countTicks: Array<{ value: number; x: number }>;
  buckets: RankDistributionBucketGeometry[];
  rankedCount: number;
  unrankedCount: number;
  medianRank: number | null;
  avgRank: number | null;
  bestRank: number | null;
  worstRank: number | null;
  top10Count: number;
  top50Count: number;
  top300Count: number;
  avgRuleScore: number | null;
};

type PersistedStrategyTriggerState = {
  sourcePath: string;
  pageData: StrategyStatisticsPageData | null;
  scenePageData: SceneStatisticsPageData | null;
  sceneTableRows: SceneStatisticsTableRow[];
  sceneName: string;
  sceneError: string;
  strategyName: string;
  analysisTradeDate: string;
  selectedOverviewDate: string | null;
  detailModalOpen: boolean;
  detailData: StrategyStatisticsDetailData | null;
  detailError: string;
};

const WEEKDAY_LABELS = ["一", "二", "三", "四", "五", "六", "日"] as const;
const STRATEGY_TRIGGER_STATE_KEY = "lh_strategy_trigger_page_state_v6";
const MINI_CANDLE_UP_COLOR = "#d9485f";
const MINI_CANDLE_DOWN_COLOR = "#178f68";
const MINI_CANDLE_FLAT_COLOR = "#536273";
const OVERVIEW_CHART_TOOLTIP_LEFT_THRESHOLD = 62;
const OVERVIEW_CHART_POINTER_DRAG_THRESHOLD = 6;
const OVERVIEW_CHART_TOUCH_FOCUS_HIT_SLOP = 24;
const RANK_DISTRIBUTION_SCALE_EXPONENT = 0.72;

function parseCompactDate(value?: string | null) {
  if (!value || value.length !== 8) {
    return null;
  }

  const year = Number(value.slice(0, 4));
  const month = Number(value.slice(4, 6)) - 1;
  const day = Number(value.slice(6, 8));
  const date = new Date(Date.UTC(year, month, day));
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  return date;
}

function formatDateLabel(value?: string | null) {
  if (!value || value.length !== 8) {
    return "--";
  }
  return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`;
}

function formatNumber(value?: number | null, digits = 2) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  return value.toFixed(digits);
}

function formatSignedNumber(value?: number | null, digits = 2) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  const formatted = value.toFixed(digits);
  return value > 0 ? `+${formatted}` : formatted;
}

function formatInteger(value?: number | null) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  return String(Math.round(value));
}

function formatPercent(value?: number | null) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  return `${(value * 100).toFixed(2)}%`;
}

function formatCompactPercent(value?: number | null) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  return `${Number((value * 100).toFixed(2))}%`;
}

function formatStageCountWithShare(
  count?: number | null,
  totalTriggered?: number | null,
) {
  const formattedCount = formatInteger(count);
  if (formattedCount === "--") {
    return "--";
  }
  if (
    totalTriggered === null ||
    totalTriggered === undefined ||
    !Number.isFinite(totalTriggered) ||
    totalTriggered <= 0
  ) {
    return `${formattedCount} | --`;
  }
  return `${formattedCount} | ${formatCompactPercent((count ?? 0) / totalTriggered)}`;
}

function normalizeSceneStageKey(stage: SceneStageRow["stage"]) {
  const normalized = stage.trim().toLowerCase();
  if (normalized === "trigger") {
    return "trigger";
  }
  if (normalized === "confirm") {
    return "confirm";
  }
  if (normalized === "observe") {
    return "observe";
  }
  if (normalized === "fail") {
    return "fail";
  }
  return "none";
}

function buildSceneStatisticsTableRow(
  sceneName: string,
  data: SceneStatisticsPageData,
): SceneStatisticsTableRow {
  const stageCount = {
    trigger: 0,
    confirm: 0,
    observe: 0,
    fail: 0,
    none: 0,
  };

  for (const item of data.stage_rows ?? []) {
    const key = normalizeSceneStageKey(item.stage);
    stageCount[key] += Number(item.sample_count) || 0;
  }

  return {
    scene_name: sceneName,
    trigger_count: stageCount.trigger,
    confirm_count: stageCount.confirm,
    observe_count: stageCount.observe,
    fail_count: stageCount.fail,
    none_count: stageCount.none,
    scene_covered_count: data.summary?.scene_covered_count,
    scene_total_sample_count: data.summary?.scene_total_sample_count,
    scene_coverage_ratio: data.summary?.scene_coverage_ratio,
    scene_rule_contribution_ratio: data.summary?.scene_rule_contribution_ratio,
  };
}

function clampNumber(value: number, min: number, max: number) {
  if (max < min) {
    return min;
  }
  return Math.min(Math.max(value, min), max);
}

function getSignedValueClassName(value?: number | null) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "";
  }
  if (value > 0) {
    return "strategy-trigger-value-positive";
  }
  if (value < 0) {
    return "strategy-trigger-value-negative";
  }
  return "strategy-trigger-value-flat";
}

function buildHeatmapTitle(item: StrategyHeatmapCell | null, label: string) {
  if (!item) {
    return label;
  }

  return [
    `日期: ${label}`,
    `当日水平: ${formatNumber(item.day_level)}`,
    `平均水平: ${formatNumber(item.avg_level)}`,
    `差值: ${formatSignedNumber(item.delta_level)}`,
  ].join("\n");
}

function getOverviewDeltaValue(item: StrategyHeatmapCell) {
  if (
    item.delta_level !== null &&
    item.delta_level !== undefined &&
    Number.isFinite(item.delta_level)
  ) {
    return item.delta_level;
  }
  if (
    item.day_level !== null &&
    item.day_level !== undefined &&
    Number.isFinite(item.day_level) &&
    item.avg_level !== null &&
    item.avg_level !== undefined &&
    Number.isFinite(item.avg_level)
  ) {
    return item.day_level - item.avg_level;
  }
  return null;
}

function resolveOverviewDeltaIndexFromChartX(
  chartXPercent: number,
  geometry: OverviewDeltaChartGeometry,
) {
  if (geometry.candles.length <= 0) {
    return null;
  }

  const plotStartPercent = (geometry.plotLeft / geometry.width) * 100;
  const plotEndPercent = (geometry.plotRight / geometry.width) * 100;
  const plotXPercent = clampNumber(
    (chartXPercent - plotStartPercent) / (plotEndPercent - plotStartPercent),
    0,
    0.999999,
  );

  return clampNumber(
    Math.round(plotXPercent * geometry.candles.length - 0.5),
    0,
    geometry.candles.length - 1,
  );
}

function buildOverviewDeltaChartFocus(
  viewport: HTMLDivElement,
  geometry: OverviewDeltaChartGeometry,
  clientX: number,
  clientY: number,
  pinned: boolean,
): OverviewDeltaChartFocus | null {
  const rect = viewport.getBoundingClientRect();
  if (rect.width <= 0 || rect.height <= 0) {
    return null;
  }

  const chartXPercent = clampNumber(
    ((clientX - rect.left) / rect.width) * 100,
    0,
    99.9999,
  );
  const index = resolveOverviewDeltaIndexFromChartX(chartXPercent, geometry);
  if (index === null) {
    return null;
  }

  return {
    index,
    cursorXPercent: chartXPercent,
    cursorYPercent: clampNumber(
      ((clientY - rect.top) / rect.height) * 100,
      8,
      92,
    ),
    pinned,
  };
}

function isPointerNearOverviewChartFocus(
  viewport: HTMLDivElement,
  clientX: number,
  clientY: number,
  focus: OverviewDeltaChartFocus | null,
) {
  if (!focus) {
    return false;
  }

  const rect = viewport.getBoundingClientRect();
  if (rect.width <= 0 || rect.height <= 0) {
    return false;
  }

  const focusClientX = rect.left + (rect.width * focus.cursorXPercent) / 100;
  if (Math.abs(clientX - focusClientX) <= OVERVIEW_CHART_TOUCH_FOCUS_HIT_SLOP) {
    return true;
  }

  const focusClientY = rect.top + (rect.height * focus.cursorYPercent) / 100;
  return (
    Math.abs(clientY - focusClientY) <= OVERVIEW_CHART_TOUCH_FOCUS_HIT_SLOP
  );
}

function buildOverviewDeltaCandleTitle(
  item: StrategyHeatmapCell,
  deltaValue: number | null,
) {
  return [
    `日期: ${formatDateLabel(item.trade_date)}`,
    `当日差值: ${formatSignedNumber(deltaValue)}`,
    `当日值: ${formatNumber(item.day_level)}`,
    `平均值: ${formatNumber(item.avg_level)}`,
  ].join("\n");
}

function buildChartValueDomain(values: number[], includeZero = false) {
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

const RANK_DISTRIBUTION_BUCKETS: RankDistributionBucket[] = [
  { key: "top50", label: "1-50", min: 1, max: 50 },
  { key: "top100", label: "51-100", min: 51, max: 100 },
  { key: "top150", label: "101-150", min: 101, max: 150 },
  { key: "top300", label: "151-300", min: 151, max: 300 },
  { key: "top500", label: "301-500", min: 301, max: 500 },
  { key: "top1000", label: "501-1000", min: 501, max: 1000 },
  { key: "top2000", label: "1001-2000", min: 1001, max: 2000 },
  { key: "after2000", label: "2001+", min: 2001, max: null },
  { key: "unranked", label: "无排名", min: null, max: null },
];

function toFiniteNumber(value?: number | null) {
  return value !== null && value !== undefined && Number.isFinite(value)
    ? value
    : null;
}

function average(values: number[]) {
  if (values.length === 0) {
    return null;
  }
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function median(values: number[]) {
  if (values.length === 0) {
    return null;
  }
  const sortedValues = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sortedValues.length / 2);
  if (sortedValues.length % 2 === 1) {
    return sortedValues[middle];
  }
  return (sortedValues[middle - 1] + sortedValues[middle]) / 2;
}

function findRankBucket(row: TriggeredStockRow) {
  const rank = toFiniteNumber(row.rank);
  if (rank === null || rank <= 0) {
    return RANK_DISTRIBUTION_BUCKETS.at(-1) ?? RANK_DISTRIBUTION_BUCKETS[0];
  }
  return (
    RANK_DISTRIBUTION_BUCKETS.find((bucket) => {
      if (bucket.min === null) {
        return false;
      }
      if (rank < bucket.min) {
        return false;
      }
      return bucket.max === null || rank <= bucket.max;
    }) ?? RANK_DISTRIBUTION_BUCKETS[RANK_DISTRIBUTION_BUCKETS.length - 2]
  );
}

function buildCalendarMonths(items: StrategyHeatmapCell[]) {
  if (items.length === 0) {
    return [];
  }

  const sortedItems = [...items].sort((left, right) =>
    left.trade_date.localeCompare(right.trade_date),
  );
  const cellMap = new Map(sortedItems.map((item) => [item.trade_date, item]));
  const firstDate = parseCompactDate(sortedItems[0]?.trade_date);
  const lastDate = parseCompactDate(
    sortedItems[sortedItems.length - 1]?.trade_date,
  );
  if (!firstDate || !lastDate) {
    return [];
  }

  const months: CalendarMonth[] = [];
  let previousYear: number | null = null;
  const cursor = new Date(
    Date.UTC(firstDate.getUTCFullYear(), firstDate.getUTCMonth(), 1),
  );
  const endMonth = new Date(
    Date.UTC(lastDate.getUTCFullYear(), lastDate.getUTCMonth(), 1),
  );

  while (cursor <= endMonth) {
    const year = cursor.getUTCFullYear();
    const month = cursor.getUTCMonth();
    const monthLabel =
      previousYear === null || previousYear === year
        ? `${month + 1}月`
        : `${year}/${month + 1}月`;
    const monthKey = `${year}-${month + 1}`;
    const firstWeekday = (cursor.getUTCDay() + 6) % 7;
    const daysInMonth = new Date(Date.UTC(year, month + 1, 0)).getUTCDate();
    const slots: HeatmapSlot[] = [];

    for (let index = 0; index < firstWeekday; index += 1) {
      slots.push({
        key: `${monthKey}-pad-start-${index}`,
        compactDate: null,
        label: "",
        cell: null,
        dayOfMonth: null,
      });
    }

    for (let day = 1; day <= daysInMonth; day += 1) {
      const compactDate = `${year}${String(month + 1).padStart(2, "0")}${String(day).padStart(2, "0")}`;
      slots.push({
        key: compactDate,
        compactDate,
        label: formatDateLabel(compactDate),
        cell: cellMap.get(compactDate) ?? null,
        dayOfMonth: day,
      });
    }

    while (slots.length % 7 !== 0) {
      slots.push({
        key: `${monthKey}-pad-end-${slots.length}`,
        compactDate: null,
        label: "",
        cell: null,
        dayOfMonth: null,
      });
    }

    months.push({
      key: monthKey,
      label: monthLabel,
      slots,
    });
    previousYear = year;
    cursor.setUTCMonth(cursor.getUTCMonth() + 1);
  }

  return months;
}

function buildOverviewDeltaGeometry(
  items: StrategyHeatmapCell[],
): OverviewDeltaChartGeometry | null {
  const sortedItems = [...items]
    .filter((item) => getOverviewDeltaValue(item) !== null)
    .sort((left, right) => left.trade_date.localeCompare(right.trade_date));

  if (sortedItems.length === 0) {
    return null;
  }

  const marginTop = 18;
  const marginRight = 16;
  const marginBottom = 34;
  const marginLeft = 48;
  const minSlotWidth = 12;
  const width = Math.max(
    540,
    marginLeft + marginRight + sortedItems.length * minSlotWidth,
  );
  const height = 180;
  const plotWidth = width - marginLeft - marginRight;
  const plotHeight = height - marginTop - marginBottom;
  const plotLeft = marginLeft;
  const plotRight = width - marginRight;
  const plotBottom = height - marginBottom;
  const latest = sortedItems.at(-1) ?? null;
  const cumulativeValues = [0];
  let runningValue = 0;
  for (const item of sortedItems) {
    const delta = getOverviewDeltaValue(item) ?? 0;
    cumulativeValues.push(runningValue);
    runningValue += delta;
    cumulativeValues.push(runningValue);
  }
  const domain = buildChartValueDomain(cumulativeValues, true);
  if (!domain) {
    return null;
  }
  const scaledMin = domain.min;
  const scaledMax = domain.max;
  const domainSpan = Math.max(scaledMax - scaledMin, 0.01);
  const valueToY = (value: number) =>
    marginTop + ((scaledMax - value) / domainSpan) * plotHeight;
  const zeroY = scaledMin <= 0 && scaledMax >= 0 ? valueToY(0) : null;
  const slotWidth = plotWidth / sortedItems.length;
  const candleWidth = Math.max(3, Math.min(18, slotWidth * 0.58));

  let cumulativeOpen = 0;
  const candles = sortedItems.map((item, index) => {
    const delta = getOverviewDeltaValue(item) ?? 0;
    const open = cumulativeOpen;
    const close = cumulativeOpen + delta;
    cumulativeOpen = close;
    const high = Math.max(open, close);
    const low = Math.min(open, close);
    const openY = valueToY(open);
    const closeY = valueToY(close);
    const bodyTop = Math.min(openY, closeY);
    return {
      key: item.trade_date,
      x: marginLeft + index * slotWidth + (slotWidth - candleWidth) / 2,
      centerX: marginLeft + index * slotWidth + slotWidth / 2,
      y: bodyTop,
      width: candleWidth,
      height: Math.max(1.8, Math.abs(openY - closeY)),
      wickTopY: valueToY(high),
      wickBottomY: valueToY(low),
      open,
      close,
      high,
      low,
      item,
    };
  });

  const lastIndex = sortedItems.length - 1;
  const middleIndex = Math.floor(lastIndex / 2);
  const xLabelSpecs = [
    { index: 0, x: plotLeft, anchor: "start" as const },
    {
      index: middleIndex,
      x: marginLeft + middleIndex * slotWidth + slotWidth / 2,
      anchor: "middle" as const,
    },
    { index: lastIndex, x: plotRight, anchor: "end" as const },
  ];
  const seenIndexes = new Set<number>();
  const xLabels = xLabelSpecs
    .filter(({ index }) => {
      if (seenIndexes.has(index)) {
        return false;
      }
      seenIndexes.add(index);
      return true;
    })
    .map(({ index, x, anchor }) => ({
      key: `${sortedItems[index].trade_date}-${anchor}`,
      value: formatDateLabel(sortedItems[index].trade_date).slice(5),
      x,
      anchor,
    }));
  const yTickValues = [scaledMax, (scaledMax + scaledMin) / 2, scaledMin];
  const yTicks = yTickValues.map((value) => ({
    key: String(value),
    value,
    y: valueToY(value),
  }));

  return {
    width,
    height,
    viewBox: `0 0 ${width} ${height}`,
    plotLeft,
    plotRight,
    plotBottom,
    zeroY,
    yTicks,
    xLabels,
    candles,
    latestTradeDate: latest?.trade_date ?? "",
    latestDelta: latest ? getOverviewDeltaValue(latest) : null,
  };
}

function pickInitialHeatmapDate(
  slots: HeatmapSlot[],
  latestTradeDate?: string | null,
) {
  if (
    latestTradeDate &&
    slots.some((slot) => slot.compactDate === latestTradeDate)
  ) {
    return latestTradeDate;
  }

  for (let index = slots.length - 1; index >= 0; index -= 1) {
    if (slots[index]?.cell) {
      return slots[index]?.compactDate ?? null;
    }
  }

  return slots.at(-1)?.compactDate ?? null;
}

function buildRankDistributionGeometry(
  rows: TriggeredStockRow[],
): RankDistributionGeometry | null {
  if (rows.length === 0) {
    return null;
  }

  const width = 980;
  const height = 360;
  const marginTop = 22;
  const marginRight = 188;
  const marginBottom = 44;
  const marginLeft = 92;
  const plotWidth = width - marginLeft - marginRight;
  const plotHeight = height - marginTop - marginBottom;
  const bucketRows = new Map<string, TriggeredStockRow[]>(
    RANK_DISTRIBUTION_BUCKETS.map((bucket) => [bucket.key, []]),
  );

  for (const row of rows) {
    const bucket = findRankBucket(row);
    bucketRows.get(bucket.key)?.push(row);
  }

  const countMax = Math.max(
    1,
    ...RANK_DISTRIBUTION_BUCKETS.map(
      (bucket) => bucketRows.get(bucket.key)?.length ?? 0,
    ),
  );
  const countToX = (value: number) =>
    marginLeft +
    Math.pow(Math.max(value, 0) / countMax, RANK_DISTRIBUTION_SCALE_EXPONENT) *
      plotWidth;
  const rowGap = 9;
  const rowHeight =
    (plotHeight - rowGap * (RANK_DISTRIBUTION_BUCKETS.length - 1)) /
    RANK_DISTRIBUTION_BUCKETS.length;
  const barHeight = Math.max(16, Math.min(24, rowHeight));
  const rankedRanks = rows
    .map((row) => toFiniteNumber(row.rank))
    .filter((rank): rank is number => rank !== null && rank > 0);
  const ruleScores = rows
    .map((row) => toFiniteNumber(row.rule_score))
    .filter((score): score is number => score !== null);

  let cumulativeCount = 0;
  const buckets = RANK_DISTRIBUTION_BUCKETS.map((bucket, index) => {
    const bucketItems = bucketRows.get(bucket.key) ?? [];
    cumulativeCount += bucketItems.length;
    const positiveCount = bucketItems.filter(
      (row) => (toFiniteNumber(row.rule_score) ?? 0) > 0,
    ).length;
    const negativeCount = bucketItems.filter(
      (row) => (toFiniteNumber(row.rule_score) ?? 0) < 0,
    ).length;
    const flatCount = Math.max(
      0,
      bucketItems.length - positiveCount - negativeCount,
    );
    const widthValue =
      Math.pow(
        bucketItems.length / countMax,
        RANK_DISTRIBUTION_SCALE_EXPONENT,
      ) * plotWidth;
    const positiveWidth =
      bucketItems.length > 0
        ? (positiveCount / bucketItems.length) * widthValue
        : 0;
    const negativeWidth =
      bucketItems.length > 0
        ? (negativeCount / bucketItems.length) * widthValue
        : 0;
    const flatWidth = Math.max(0, widthValue - positiveWidth - negativeWidth);
    const y =
      marginTop + index * (rowHeight + rowGap) + (rowHeight - barHeight) / 2;
    const bucketRuleScores = bucketItems
      .map((row) => toFiniteNumber(row.rule_score))
      .filter((score): score is number => score !== null);
    const bucketTotalScores = bucketItems
      .map((row) => toFiniteNumber(row.total_score))
      .filter((score): score is number => score !== null);

    return {
      key: bucket.key,
      label: bucket.label,
      x: marginLeft,
      y,
      width: widthValue,
      height: barHeight,
      positiveWidth,
      negativeWidth,
      flatWidth,
      count: bucketItems.length,
      positiveCount,
      negativeCount,
      flatCount,
      bucketRatio: rows.length > 0 ? bucketItems.length / rows.length : null,
      cumulativeRatio: rows.length > 0 ? cumulativeCount / rows.length : null,
      avgRuleScore: average(bucketRuleScores),
      avgTotalScore: average(bucketTotalScores),
    };
  });

  const tickRatios = [0, 0.25, 0.5, 0.75, 1];
  const countTicks = tickRatios
    .map((ratio) => Math.round(countMax * ratio))
    .filter(
      (value, index, values) => index === 0 || value !== values[index - 1],
    )
    .map((value) => ({
      value,
      x: countToX(value),
    }));

  return {
    width,
    height,
    viewBox: `0 0 ${width} ${height}`,
    plotLeft: marginLeft,
    plotRight: width - marginRight,
    plotBottom: height - marginBottom,
    countTicks,
    buckets,
    rankedCount: rankedRanks.length,
    unrankedCount: rows.length - rankedRanks.length,
    medianRank: median(rankedRanks),
    avgRank: average(rankedRanks),
    bestRank: rankedRanks.length > 0 ? Math.min(...rankedRanks) : null,
    worstRank: rankedRanks.length > 0 ? Math.max(...rankedRanks) : null,
    top10Count: rankedRanks.filter((rank) => rank <= 10).length,
    top50Count: rankedRanks.filter((rank) => rank <= 50).length,
    top300Count: rankedRanks.filter((rank) => rank <= 300).length,
    avgRuleScore: average(ruleScores),
  };
}

function buildRankDistributionTitle(item: RankDistributionBucketGeometry) {
  return [
    `排名区间: ${item.label}`,
    `触发股票: ${formatInteger(item.count)}`,
    `正向得分: ${formatInteger(item.positiveCount)}`,
    `负向得分: ${formatInteger(item.negativeCount)}`,
    `平均策略得分: ${formatNumber(item.avgRuleScore)}`,
    `平均总分: ${formatNumber(item.avgTotalScore)}`,
  ].join("\n");
}

function summarizeOverview(items: StrategyHeatmapCell[]) {
  const strongDays = items.filter((item) => item.above_avg === true).length;
  const latest = [...items]
    .sort((left, right) => left.trade_date.localeCompare(right.trade_date))
    .at(-1);
  return {
    strongDays,
    latestDayLevel: latest?.day_level ?? null,
  };
}

function StrategyOverviewDeltaChart({
  items,
}: {
  items: StrategyHeatmapCell[];
}) {
  const geometry = useMemo(() => buildOverviewDeltaGeometry(items), [items]);
  const scrollRef = useRef<HTMLDivElement | null>(null);
  const [focus, setFocus] = useState<OverviewDeltaChartFocus | null>(null);
  const pointerStateRef = useRef<OverviewDeltaChartPointerState | null>(null);

  useLayoutEffect(() => {
    const container = scrollRef.current;
    if (!container || !geometry) {
      return;
    }

    const applyScroll = () => {
      container.scrollLeft = container.scrollWidth;
    };

    applyScroll();
    const frameId = window.requestAnimationFrame(applyScroll);
    return () => window.cancelAnimationFrame(frameId);
  }, [geometry]);

  if (!geometry) {
    return (
      <div className="strategy-trigger-overview-mini-chart strategy-trigger-overview-mini-chart-empty">
        暂无差值数据。
      </div>
    );
  }

  const chartGeometry = geometry;
  const focusCandle =
    focus !== null ? (chartGeometry.candles[focus.index] ?? null) : null;
  const tooltipHorizontalClass =
    (focus?.cursorXPercent ?? 0) > OVERVIEW_CHART_TOOLTIP_LEFT_THRESHOLD
      ? "strategy-trigger-overview-mini-chart-tooltip-left"
      : "strategy-trigger-overview-mini-chart-tooltip-right";
  const yAxisLabels = chartGeometry.yTicks.map((tick) => ({
    key: tick.key,
    value: formatNumber(tick.value),
    topPercent: (tick.y / chartGeometry.height) * 100,
  }));
  const xAxisLabels = chartGeometry.xLabels.map((label) => ({
    key: label.key,
    value: label.value,
    leftPercent: (label.x / chartGeometry.width) * 100,
  }));

  function buildFocusFromEvent(
    event: ReactPointerEvent<HTMLDivElement>,
    pinned: boolean,
  ) {
    return buildOverviewDeltaChartFocus(
      event.currentTarget,
      chartGeometry,
      event.clientX,
      event.clientY,
      pinned,
    );
  }

  function clearPointerState() {
    pointerStateRef.current = null;
  }

  function captureOverviewPointer(event: ReactPointerEvent<HTMLDivElement>) {
    try {
      event.currentTarget.setPointerCapture(event.pointerId);
    } catch {
      // Pointer capture can fail on unsupported devices/browsers.
    }
  }

  function releaseOverviewPointer(event: ReactPointerEvent<HTMLDivElement>) {
    try {
      if (event.currentTarget.hasPointerCapture(event.pointerId)) {
        event.currentTarget.releasePointerCapture(event.pointerId);
      }
    } catch {
      // Pointer release should not block interaction flow.
    }
  }

  function onOverviewPointerDown(event: ReactPointerEvent<HTMLDivElement>) {
    if (event.pointerType === "mouse" && event.button !== 0) {
      return;
    }

    const isTouchPointer = event.pointerType !== "mouse";
    const scrollContainer = scrollRef.current;
    const canScrollHorizontally =
      scrollContainer !== null &&
      scrollContainer.scrollWidth > scrollContainer.clientWidth + 1;
    const mode = focus?.pinned
      ? isTouchPointer &&
        !isPointerNearOverviewChartFocus(
          event.currentTarget,
          event.clientX,
          event.clientY,
          focus,
        )
        ? "dismiss"
        : "focus"
      : !isTouchPointer && canScrollHorizontally
        ? "scroll"
        : "tap";

    pointerStateRef.current = {
      mode,
      pointerId: event.pointerId,
      pointerType: event.pointerType,
      startClientX: event.clientX,
      startClientY: event.clientY,
      startScrollLeft: scrollContainer?.scrollLeft ?? 0,
      moved: false,
    };

    if (mode !== "tap") {
      captureOverviewPointer(event);
    }
  }

  function onOverviewPointerMove(event: ReactPointerEvent<HTMLDivElement>) {
    const pointerState = pointerStateRef.current;

    if (!pointerState) {
      if (event.pointerType !== "mouse" || !focus?.pinned) {
        return;
      }

      const nextFocus = buildFocusFromEvent(event, true);
      if (!nextFocus) {
        return;
      }
      setFocus(nextFocus);
      return;
    }

    if (pointerState.pointerId !== event.pointerId) {
      return;
    }

    const moveDistance = Math.hypot(
      event.clientX - pointerState.startClientX,
      event.clientY - pointerState.startClientY,
    );
    if (
      !pointerState.moved &&
      moveDistance >= OVERVIEW_CHART_POINTER_DRAG_THRESHOLD
    ) {
      pointerState.moved = true;
    }

    if (pointerState.mode === "dismiss") {
      return;
    }

    if (pointerState.mode === "scroll") {
      if (!pointerState.moved) {
        return;
      }

      const scrollContainer = scrollRef.current;
      if (!scrollContainer) {
        return;
      }

      scrollContainer.scrollLeft =
        pointerState.startScrollLeft -
        (event.clientX - pointerState.startClientX);
      return;
    }

    if (pointerState.mode !== "focus" || !pointerState.moved) {
      return;
    }

    const nextFocus = buildFocusFromEvent(event, true);
    if (!nextFocus) {
      return;
    }
    setFocus(nextFocus);
  }

  function onOverviewPointerUp(event: ReactPointerEvent<HTMLDivElement>) {
    const pointerState = pointerStateRef.current;
    releaseOverviewPointer(event);
    clearPointerState();

    if (!pointerState || pointerState.pointerId !== event.pointerId) {
      return;
    }

    if (pointerState.mode === "dismiss") {
      if (!pointerState.moved) {
        setFocus(null);
      }
      return;
    }

    if (pointerState.moved) {
      return;
    }

    const nextFocus = buildFocusFromEvent(event, true);
    if (!nextFocus) {
      return;
    }

    if (focus?.pinned && focus.index === nextFocus.index) {
      setFocus(null);
      return;
    }

    setFocus(nextFocus);
  }

  function onOverviewPointerLeave(event: ReactPointerEvent<HTMLDivElement>) {
    const pointerState = pointerStateRef.current;
    if (pointerState && pointerState.pointerId === event.pointerId) {
      return;
    }

    if (!focus?.pinned) {
      setFocus(null);
    }
  }

  function onOverviewPointerCancel() {
    clearPointerState();
  }

  return (
    <div className="strategy-trigger-overview-mini-chart">
      <div className="strategy-trigger-overview-mini-chart-head">
        <div className="strategy-trigger-overview-mini-chart-title">
          <strong>差值累计小K线</strong>
          <span>开收按累计差值延展，实体长度 = 当日差值</span>
        </div>
      </div>
      <div
        className="strategy-trigger-overview-mini-chart-scroll"
        ref={scrollRef}
      >
        <div
          className={[
            "strategy-trigger-overview-mini-chart-viewport",
            focus?.pinned ? "is-pinned" : "",
          ]
            .filter(Boolean)
            .join(" ")}
          onPointerDown={onOverviewPointerDown}
          onPointerMove={onOverviewPointerMove}
          onPointerUp={onOverviewPointerUp}
          onPointerLeave={onOverviewPointerLeave}
          onPointerCancel={(event) => {
            releaseOverviewPointer(event);
            onOverviewPointerCancel();
          }}
          style={{
            width: `${geometry.width}px`,
            minWidth: "100%",
            height: `${geometry.height}px`,
          }}
        >
          <svg
            className="strategy-trigger-overview-mini-chart-svg"
            viewBox={chartGeometry.viewBox}
            preserveAspectRatio="none"
          >
            {chartGeometry.yTicks
              .filter((tick) => tick.value !== 0)
              .map((tick) => (
                <line
                  key={tick.key}
                  x1={chartGeometry.plotLeft}
                  y1={tick.y}
                  x2={chartGeometry.plotRight}
                  y2={tick.y}
                  className="strategy-trigger-overview-mini-chart-grid"
                />
              ))}
            {chartGeometry.xLabels.map((label) => (
              <line
                key={`guide-${label.key}`}
                x1={label.x}
                y1={18}
                x2={label.x}
                y2={chartGeometry.plotBottom}
                className="strategy-trigger-overview-mini-chart-vertical-line"
              />
            ))}
            {chartGeometry.zeroY !== null ? (
              <line
                x1={chartGeometry.plotLeft}
                y1={chartGeometry.zeroY}
                x2={chartGeometry.plotRight}
                y2={chartGeometry.zeroY}
                className="strategy-trigger-overview-mini-chart-zero"
              />
            ) : null}
            {chartGeometry.candles.map((candle) => {
              const candleState =
                candle.close > candle.open
                  ? "is-positive"
                  : candle.close < candle.open
                    ? "is-negative"
                    : "is-flat";
              const candleColor =
                candleState === "is-positive"
                  ? MINI_CANDLE_UP_COLOR
                  : candleState === "is-negative"
                    ? MINI_CANDLE_DOWN_COLOR
                    : MINI_CANDLE_FLAT_COLOR;
              return (
                <g key={candle.key}>
                  <line
                    x1={candle.centerX}
                    y1={candle.wickTopY}
                    x2={candle.centerX}
                    y2={candle.wickBottomY}
                    className="strategy-trigger-overview-mini-chart-wick"
                    stroke={candleColor}
                  />
                  <rect
                    x={candle.x}
                    y={candle.y}
                    width={candle.width}
                    height={candle.height}
                    rx={1.2}
                    className="strategy-trigger-overview-mini-chart-body"
                    fill={candleColor}
                    stroke={candleColor}
                  >
                    <title>
                      {buildOverviewDeltaCandleTitle(
                        candle.item,
                        getOverviewDeltaValue(candle.item),
                      )}
                    </title>
                  </rect>
                </g>
              );
            })}
          </svg>
          {yAxisLabels.length > 0 ? (
            <div className="strategy-trigger-overview-mini-chart-axis-layer strategy-trigger-overview-mini-chart-axis-layer-y">
              {yAxisLabels.map((label) => (
                <span
                  className="strategy-trigger-overview-mini-chart-y-label"
                  key={label.key}
                  style={{ top: `${label.topPercent}%` }}
                >
                  {label.value}
                </span>
              ))}
            </div>
          ) : null}
          {xAxisLabels.length > 0 ? (
            <div className="strategy-trigger-overview-mini-chart-axis-layer strategy-trigger-overview-mini-chart-axis-layer-x">
              {xAxisLabels.map((label) => (
                <span
                  className="strategy-trigger-overview-mini-chart-x-label"
                  key={label.key}
                  style={{ left: `${label.leftPercent}%` }}
                >
                  {label.value}
                </span>
              ))}
            </div>
          ) : null}
          {focus !== null ? (
            <>
              <div
                className="strategy-trigger-overview-mini-chart-crosshair-vertical"
                style={{ left: `${focus.cursorXPercent}%` }}
              />
              <div
                className="strategy-trigger-overview-mini-chart-crosshair-horizontal"
                style={{ top: `${focus.cursorYPercent}%` }}
              />
            </>
          ) : null}
          {focusCandle ? (
            <div
              className={[
                "strategy-trigger-overview-mini-chart-tooltip",
                tooltipHorizontalClass,
                focus?.pinned
                  ? "strategy-trigger-overview-mini-chart-tooltip-pinned"
                  : "",
              ]
                .filter(Boolean)
                .join(" ")}
              style={{
                left: `${focus?.cursorXPercent ?? 0}%`,
                top: `${focus?.cursorYPercent ?? 0}%`,
              }}
            >
              <div className="strategy-trigger-overview-mini-chart-tooltip-head">
                <strong>{formatDateLabel(focusCandle.item.trade_date)}</strong>
                <span>
                  {formatSignedNumber(getOverviewDeltaValue(focusCandle.item))}
                </span>
              </div>
              <div className="strategy-trigger-overview-mini-chart-tooltip-body">
                <div className="strategy-trigger-overview-mini-chart-tooltip-grid">
                  <div className="strategy-trigger-overview-mini-chart-tooltip-row">
                    <span>当日差值</span>
                    <strong>
                      {formatSignedNumber(
                        getOverviewDeltaValue(focusCandle.item),
                      )}
                    </strong>
                  </div>
                  <div className="strategy-trigger-overview-mini-chart-tooltip-row">
                    <span>当日值</span>
                    <strong>{formatNumber(focusCandle.item.day_level)}</strong>
                  </div>
                  <div className="strategy-trigger-overview-mini-chart-tooltip-row">
                    <span>平均值</span>
                    <strong>{formatNumber(focusCandle.item.avg_level)}</strong>
                  </div>
                </div>
              </div>
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}

function StrategyRankDistributionChart({
  rows,
}: {
  rows: TriggeredStockRow[];
}) {
  const geometry = useMemo(() => buildRankDistributionGeometry(rows), [rows]);

  if (!geometry) {
    return (
      <div className="strategy-trigger-empty">
        当前策略暂无可展示的触发排名数据。
      </div>
    );
  }

  return (
    <div className="strategy-trigger-rank-chart-shell">
      <div className="strategy-trigger-rank-legend">
        <span className="strategy-trigger-rank-legend-item">
          <i className="strategy-trigger-rank-legend-swatch strategy-trigger-rank-legend-positive" />
          正向策略得分
        </span>
        <span className="strategy-trigger-rank-legend-item">
          <i className="strategy-trigger-rank-legend-swatch strategy-trigger-rank-legend-negative" />
          负向策略得分
        </span>
        <span className="strategy-trigger-rank-legend-item">
          <i className="strategy-trigger-rank-legend-swatch strategy-trigger-rank-legend-flat" />
          其他/零分
        </span>
      </div>
      <svg
        className="strategy-trigger-rank-chart"
        viewBox={geometry.viewBox}
        preserveAspectRatio="xMidYMid meet"
      >
        {geometry.countTicks.map((tick) => (
          <g key={`count-${tick.value}`}>
            <line
              x1={tick.x}
              y1={18}
              x2={tick.x}
              y2={geometry.plotBottom}
              className="strategy-trigger-rank-chart-grid"
            />
            <text
              x={tick.x}
              y={geometry.plotBottom + 25}
              textAnchor="middle"
              className="strategy-trigger-rank-chart-axis"
            >
              {formatInteger(tick.value)}
            </text>
          </g>
        ))}
        {geometry.buckets.map((bucket) => (
          <g key={bucket.key}>
            <text
              x={geometry.plotLeft - 12}
              y={bucket.y + bucket.height / 2 + 4}
              className="strategy-trigger-rank-chart-axis strategy-trigger-rank-chart-axis-label"
            >
              {bucket.label}
            </text>
            <rect
              x={bucket.x}
              y={bucket.y}
              width={Math.max(bucket.width, bucket.count > 0 ? 2 : 0)}
              height={bucket.height}
              rx={4}
              className="strategy-trigger-rank-chart-bar-bg"
            >
              <title>{buildRankDistributionTitle(bucket)}</title>
            </rect>
            {bucket.positiveWidth > 0 ? (
              <rect
                x={bucket.x}
                y={bucket.y}
                width={bucket.positiveWidth}
                height={bucket.height}
                rx={4}
                className="strategy-trigger-rank-chart-bar-positive"
              >
                <title>{buildRankDistributionTitle(bucket)}</title>
              </rect>
            ) : null}
            {bucket.negativeWidth > 0 ? (
              <rect
                x={bucket.x + bucket.positiveWidth}
                y={bucket.y}
                width={bucket.negativeWidth}
                height={bucket.height}
                rx={4}
                className="strategy-trigger-rank-chart-bar-negative"
              >
                <title>{buildRankDistributionTitle(bucket)}</title>
              </rect>
            ) : null}
            {bucket.flatWidth > 0 ? (
              <rect
                x={bucket.x + bucket.positiveWidth + bucket.negativeWidth}
                y={bucket.y}
                width={bucket.flatWidth}
                height={bucket.height}
                rx={4}
                className="strategy-trigger-rank-chart-bar-flat"
              >
                <title>{buildRankDistributionTitle(bucket)}</title>
              </rect>
            ) : null}
            <text
              x={Math.min(
                geometry.plotRight + 8,
                bucket.x +
                  Math.max(bucket.width, bucket.count > 0 ? 2 : 0) +
                  10,
              )}
              y={bucket.y + bucket.height / 2 + 4}
              className="strategy-trigger-rank-chart-value"
            >
              {formatInteger(bucket.count)} -{" "}
              {formatPercent(bucket.bucketRatio)} -{" "}
              {formatPercent(bucket.cumulativeRatio)}
            </text>
          </g>
        ))}
        <text
          x={(geometry.plotLeft + geometry.plotRight) / 2}
          y={geometry.height - 6}
          textAnchor="middle"
          className="strategy-trigger-rank-chart-axis strategy-trigger-rank-chart-axis-caption"
        >
          股票数量 · 0.72次幂缩放
        </text>
      </svg>
    </div>
  );
}

function StrategyDetailModal({
  sourcePath,
  strategyName,
  detailData,
  loading,
  error,
  onClose,
  onChangeTradeDate,
}: {
  sourcePath: string;
  strategyName: string;
  detailData: StrategyStatisticsDetailData | null;
  loading: boolean;
  error: string;
  onClose: () => void;
  onChangeTradeDate: (tradeDate: string) => void;
}) {
  const { excludedConcepts } = useConceptExclusions();
  const selectedDailyRow = detailData?.selected_daily_row ?? null;
  const detailTradeDate = detailData?.resolved_analysis_trade_date ?? "";
  const dateOptions = detailData?.analysis_trade_date_options ?? [];
  const modalShellRef = useRouteScrollRegion<HTMLDivElement>(
    `strategy-trigger-detail-modal-shell:${strategyName || "default"}`,
    [detailTradeDate, loading, error],
  );
  const currentDateIndex = dateOptions.findIndex(
    (item) => item === detailTradeDate,
  );
  const previousTradeDate =
    currentDateIndex >= 0 && currentDateIndex < dateOptions.length - 1
      ? dateOptions[currentDateIndex + 1]
      : null;
  const nextTradeDate =
    currentDateIndex > 0 ? dateOptions[currentDateIndex - 1] : null;
  const stockSortDefinitions = useMemo(
    () =>
      ({
        rank: { value: (row) => row.rank },
        total_score: { value: (row) => row.total_score },
        rule_score: { value: (row) => row.rule_score },
      }) satisfies Partial<
        Record<TriggeredStockSortKey, SortDefinition<TriggeredStockRow>>
      >,
    [],
  );
  const { sortKey, sortDirection, sortedRows, toggleSort } = useTableSort<
    TriggeredStockRow,
    TriggeredStockSortKey
  >(detailData?.triggered_stocks ?? [], stockSortDefinitions);
  const stocksTableWrapRef = useRouteScrollRegion<HTMLDivElement>(
    `strategy-trigger-detail-stocks:${strategyName || "default"}`,
    [detailTradeDate, sortedRows.length],
  );
  const detailNavigationItems = sortedRows.map((row) => ({
    tsCode: row.ts_code,
    tradeDate: detailTradeDate || undefined,
    sourcePath: sourcePath.trim() || undefined,
    name: row.name ?? undefined,
  }));
  const rankDistributionGeometry = useMemo(
    () => buildRankDistributionGeometry(detailData?.triggered_stocks ?? []),
    [detailData?.triggered_stocks],
  );
  const top300Ratio =
    rankDistributionGeometry && rankDistributionGeometry.rankedCount > 0
      ? rankDistributionGeometry.top300Count /
        rankDistributionGeometry.rankedCount
      : null;

  if (typeof document === "undefined") {
    return null;
  }

  return createPortal(
    <div
      className="strategy-trigger-modal-overlay"
      role="presentation"
      onClick={onClose}
    >
      <div
        className="strategy-trigger-modal-shell"
        role="dialog"
        aria-modal="true"
        aria-labelledby="strategy-trigger-modal-title"
        onClick={(event) => event.stopPropagation()}
        ref={modalShellRef}
      >
        <div className="strategy-trigger-modal-close-wrap">
          <button
            type="button"
            className="strategy-trigger-modal-close"
            onClick={onClose}
            aria-label="关闭策略分析浮窗"
          >
            关闭
          </button>
        </div>

        <section className="strategy-trigger-card">
          <div className="strategy-trigger-section-head">
            <div>
              <h3
                className="strategy-trigger-subtitle"
                id="strategy-trigger-modal-title"
              >
                策略分析浮窗
              </h3>
              <p className="strategy-trigger-caption">
                查看单策略走势和触发股明细。
              </p>
            </div>
          </div>

          {loading ? (
            <div className="strategy-trigger-empty">正在读取策略明细...</div>
          ) : error ? (
            <div className="strategy-trigger-error">{error}</div>
          ) : !detailData ? (
            <div className="strategy-trigger-empty">
              当前没有可展示的策略明细。
            </div>
          ) : (
            <div className="strategy-trigger-modal-content">
              <div className="strategy-trigger-summary-grid strategy-trigger-summary-grid-modal">
                <div className="strategy-trigger-summary-item">
                  <span>策略</span>
                  <strong>{strategyName || "--"}</strong>
                </div>
                <div className="strategy-trigger-summary-item">
                  <span>分析日期</span>
                  <strong>{formatDateLabel(detailTradeDate)}</strong>
                </div>
                <div className="strategy-trigger-summary-item">
                  <span>触发模式</span>
                  <strong>{selectedDailyRow?.trigger_mode ?? "--"}</strong>
                </div>
                <div className="strategy-trigger-summary-item">
                  <span>触发样本</span>
                  <strong>
                    {formatInteger(selectedDailyRow?.trigger_count)}
                    <small>
                      触发占比 {formatPercent(selectedDailyRow?.coverage)}
                    </small>
                  </strong>
                </div>
                <div className="strategy-trigger-summary-item">
                  <span>策略贡献度</span>
                  <strong
                    className={getSignedValueClassName(
                      selectedDailyRow?.contribution_score,
                    )}
                  >
                    {formatNumber(selectedDailyRow?.contribution_score)}
                    <small>
                      单次{" "}
                      {formatNumber(selectedDailyRow?.contribution_per_trigger)}
                    </small>
                  </strong>
                </div>
                <div className="strategy-trigger-summary-item">
                  <span>平均策略得分</span>
                  <strong
                    className={getSignedValueClassName(
                      rankDistributionGeometry?.avgRuleScore,
                    )}
                  >
                    {formatNumber(rankDistributionGeometry?.avgRuleScore)}
                  </strong>
                </div>
                <div className="strategy-trigger-summary-item">
                  <span>前100触发次数</span>
                  <strong>
                    {formatInteger(selectedDailyRow?.top100_trigger_count)}
                  </strong>
                </div>
                <div className="strategy-trigger-summary-item">
                  <span>前300占比</span>
                  <strong>
                    {formatInteger(rankDistributionGeometry?.top300Count)}
                    <small>{formatPercent(top300Ratio)}</small>
                  </strong>
                </div>
                <div className="strategy-trigger-summary-item">
                  <span>排名中位数</span>
                  <strong>
                    {formatInteger(rankDistributionGeometry?.medianRank)}
                  </strong>
                </div>
                <div className="strategy-trigger-summary-item">
                  <span>最佳排名</span>
                  <strong>
                    {formatInteger(rankDistributionGeometry?.bestRank)}
                  </strong>
                </div>
              </div>

              <div className="strategy-trigger-analysis-grid strategy-trigger-analysis-grid-modal">
                <section className="strategy-trigger-card strategy-trigger-card-analysis">
                  <div className="strategy-trigger-section-head">
                    <div>
                      <h3 className="strategy-trigger-subtitle">
                        触发排名分布
                      </h3>
                      <p className="strategy-trigger-caption">
                        {strategyName
                          ? `${strategyName} · ${formatDateLabel(detailTradeDate)} · 按当日排名分桶`
                          : "未选择策略"}
                      </p>
                    </div>
                  </div>
                  <StrategyRankDistributionChart
                    rows={detailData.triggered_stocks}
                  />
                </section>

                <section className="strategy-trigger-card strategy-trigger-card-stocks">
                  <div className="strategy-trigger-section-head">
                    <div>
                      <h3 className="strategy-trigger-subtitle">
                        触发股票列表
                      </h3>
                      <p className="strategy-trigger-caption">
                        {strategyName
                          ? `${strategyName} · ${formatDateLabel(detailTradeDate)}`
                          : formatDateLabel(detailTradeDate)}
                      </p>
                    </div>
                    <div className="strategy-trigger-section-head-actions">
                      <label className="strategy-trigger-field strategy-trigger-field-inline">
                        <span>触发日期</span>
                        <select
                          value={detailTradeDate}
                          onChange={(event) =>
                            onChangeTradeDate(event.target.value)
                          }
                          disabled={loading || dateOptions.length === 0}
                        >
                          {dateOptions.map((item) => (
                            <option key={item} value={item}>
                              {formatDateLabel(item)}
                            </option>
                          ))}
                        </select>
                      </label>
                      <button
                        type="button"
                        className="strategy-trigger-collapse-btn strategy-trigger-date-nav-btn"
                        disabled={!previousTradeDate || loading}
                        onClick={() =>
                          previousTradeDate
                            ? onChangeTradeDate(previousTradeDate)
                            : undefined
                        }
                      >
                        上一天
                      </button>
                      <button
                        type="button"
                        className="strategy-trigger-collapse-btn strategy-trigger-date-nav-btn"
                        disabled={!nextTradeDate || loading}
                        onClick={() =>
                          nextTradeDate
                            ? onChangeTradeDate(nextTradeDate)
                            : undefined
                        }
                      >
                        下一天
                      </button>
                    </div>
                  </div>

                  {sortedRows.length === 0 ? (
                    <div className="strategy-trigger-empty">
                      该策略在所选日期没有触发股票。
                    </div>
                  ) : (
                    <div
                      className="strategy-trigger-table-wrap strategy-trigger-table-wrap-stocks"
                      ref={stocksTableWrapRef}
                    >
                      <table className="strategy-trigger-table strategy-trigger-table-stocks">
                        <thead>
                          <tr>
                            <th
                              aria-sort={getAriaSort(
                                sortKey === "rank",
                                sortDirection,
                              )}
                            >
                              <TableSortButton
                                label="当日排名"
                                isActive={sortKey === "rank"}
                                direction={sortDirection}
                                onClick={() => toggleSort("rank")}
                                title="按当日排名排序"
                              />
                            </th>
                            <th>代码</th>
                            <th>名称</th>
                            <th
                              aria-sort={getAriaSort(
                                sortKey === "total_score",
                                sortDirection,
                              )}
                            >
                              <TableSortButton
                                label="总分"
                                isActive={sortKey === "total_score"}
                                direction={sortDirection}
                                onClick={() => toggleSort("total_score")}
                                title="按总分排序"
                              />
                            </th>
                            <th
                              aria-sort={getAriaSort(
                                sortKey === "rule_score",
                                sortDirection,
                              )}
                            >
                              <TableSortButton
                                label="该策略得分"
                                isActive={sortKey === "rule_score"}
                                direction={sortDirection}
                                onClick={() => toggleSort("rule_score")}
                                title="按该策略得分排序"
                              />
                            </th>
                            <th>所属概念</th>
                          </tr>
                        </thead>
                        <tbody>
                          {sortedRows.map((row) => (
                            <tr
                              key={`${detailTradeDate}-${strategyName}-${row.ts_code}`}
                            >
                              <td>{formatInteger(row.rank)}</td>
                              <td>{row.ts_code}</td>
                              <td>
                                <DetailsLink
                                  className="strategy-trigger-inline-btn strategy-trigger-inline-btn-name"
                                  tsCode={row.ts_code}
                                  tradeDate={detailTradeDate || undefined}
                                  sourcePath={sourcePath.trim()}
                                  navigationItems={detailNavigationItems}
                                >
                                  {row.name ?? row.ts_code}
                                </DetailsLink>
                              </td>
                              <td
                                className={getSignedValueClassName(
                                  row.total_score,
                                )}
                              >
                                {formatNumber(row.total_score)}
                              </td>
                              <td
                                className={getSignedValueClassName(
                                  row.rule_score,
                                )}
                              >
                                {formatNumber(row.rule_score)}
                              </td>
                              <td className="strategy-trigger-cell-concept">
                                {formatConceptText(
                                  row.concept,
                                  excludedConcepts,
                                )}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </section>
              </div>
            </div>
          )}
        </section>
      </div>
    </div>,
    document.body,
  );
}

export default function StrategyTriggerPage() {
  const persistedState = useMemo(() => {
    const parsed = readJsonStorage<Partial<PersistedStrategyTriggerState>>(
      typeof window === "undefined" ? null : window.sessionStorage,
      STRATEGY_TRIGGER_STATE_KEY,
    );
    if (!parsed || typeof parsed !== "object") {
      return null;
    }

    return {
      sourcePath:
        typeof parsed.sourcePath === "string" ? parsed.sourcePath : "",
      pageData:
        parsed.pageData && typeof parsed.pageData === "object"
          ? (parsed.pageData as StrategyStatisticsPageData)
          : null,
      scenePageData:
        parsed.scenePageData && typeof parsed.scenePageData === "object"
          ? (parsed.scenePageData as SceneStatisticsPageData)
          : null,
      sceneTableRows: Array.isArray(parsed.sceneTableRows)
        ? (parsed.sceneTableRows as SceneStatisticsTableRow[])
        : [],
      sceneName: typeof parsed.sceneName === "string" ? parsed.sceneName : "",
      sceneError:
        typeof parsed.sceneError === "string" ? parsed.sceneError : "",
      strategyName:
        typeof parsed.strategyName === "string" ? parsed.strategyName : "",
      analysisTradeDate:
        typeof parsed.analysisTradeDate === "string"
          ? parsed.analysisTradeDate
          : "",
      selectedOverviewDate:
        typeof parsed.selectedOverviewDate === "string"
          ? parsed.selectedOverviewDate
          : null,
      detailModalOpen:
        typeof parsed.detailModalOpen === "boolean"
          ? parsed.detailModalOpen
          : false,
      detailData:
        parsed.detailData && typeof parsed.detailData === "object"
          ? (parsed.detailData as StrategyStatisticsDetailData)
          : null,
      detailError:
        typeof parsed.detailError === "string" ? parsed.detailError : "",
    } satisfies PersistedStrategyTriggerState;
  }, []);
  const [sourcePath, setSourcePath] = useState(
    () => persistedState?.sourcePath ?? readStoredSourcePath(),
  );
  const [pageData, setPageData] = useState<StrategyStatisticsPageData | null>(
    () => persistedState?.pageData ?? null,
  );
  const [scenePageData, setScenePageData] =
    useState<SceneStatisticsPageData | null>(
      () => persistedState?.scenePageData ?? null,
    );
  const [sceneTableRows, setSceneTableRows] = useState<
    SceneStatisticsTableRow[]
  >(() => persistedState?.sceneTableRows ?? []);
  const [sceneName, setSceneName] = useState(
    () => persistedState?.sceneName ?? "",
  );
  const [sceneLoading, setSceneLoading] = useState(false);
  const [sceneError, setSceneError] = useState(
    () => persistedState?.sceneError ?? "",
  );
  const [strategyName, setStrategyName] = useState(
    () => persistedState?.strategyName ?? "",
  );
  const [analysisTradeDate, setAnalysisTradeDate] = useState(
    () => persistedState?.analysisTradeDate ?? "",
  );
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [selectedOverviewDate, setSelectedOverviewDate] = useState<
    string | null
  >(() => persistedState?.selectedOverviewDate ?? null);
  const [heatmapTooltip, setHeatmapTooltip] = useState<{
    left: number;
    top: number;
    placement: "top" | "bottom";
  } | null>(null);
  const [detailModalOpen, setDetailModalOpen] = useState(
    () => persistedState?.detailModalOpen ?? false,
  );
  const [detailData, setDetailData] =
    useState<StrategyStatisticsDetailData | null>(
      () => persistedState?.detailData ?? null,
    );
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState(
    () => persistedState?.detailError ?? "",
  );
  const [detailCache, setDetailCache] = useState<
    Record<string, StrategyStatisticsDetailData | null>
  >({});

  useEffect(() => {
    writeJsonStorage(
      typeof window === "undefined" ? null : window.sessionStorage,
      STRATEGY_TRIGGER_STATE_KEY,
      {
        sourcePath,
        pageData,
        scenePageData,
        sceneTableRows,
        sceneName,
        sceneError,
        strategyName,
        analysisTradeDate,
        selectedOverviewDate,
        detailModalOpen,
        detailData,
        detailError,
      } satisfies PersistedStrategyTriggerState,
    );
  }, [
    analysisTradeDate,
    detailData,
    detailError,
    detailModalOpen,
    pageData,
    sceneError,
    sceneName,
    scenePageData,
    sceneTableRows,
    selectedOverviewDate,
    sourcePath,
    strategyName,
  ]);

  const strategyOptions = pageData?.strategy_options ?? [];
  const analysisTradeDateOptions = pageData?.analysis_trade_date_options ?? [];
  const overviewItems = pageData?.overview?.items ?? [];
  const detailRows = pageData?.detail_rows ?? [];

  const calendarMonths = useMemo(
    () => buildCalendarMonths(overviewItems),
    [overviewItems],
  );
  const heatmapSlots = useMemo(
    () => calendarMonths.flatMap((month) => month.slots),
    [calendarMonths],
  );
  const overviewSummary = useMemo(
    () => summarizeOverview(overviewItems),
    [overviewItems],
  );
  const selectedOverviewSlot = useMemo(
    () =>
      heatmapSlots.find((slot) => slot.compactDate === selectedOverviewDate) ??
      null,
    [heatmapSlots, selectedOverviewDate],
  );
  const rowsForAnalysisDate = useMemo(
    () => detailRows.filter((row) => row.trade_date === analysisTradeDate),
    [analysisTradeDate, detailRows],
  );
  const dailySortDefinitions = useMemo(
    () =>
      ({
        rule_name: { value: (row) => row.rule_name },
        trigger_mode: { value: (row) => row.trigger_mode },
        trigger_count: { value: (row) => row.trigger_count },
        contribution_score: { value: (row) => row.contribution_score },
        contribution_per_trigger: {
          value: (row) => row.contribution_per_trigger,
        },
        top100_trigger_count: { value: (row) => row.top100_trigger_count },
        coverage: { value: (row) => row.coverage },
        median_trigger_count: { value: (row) => row.median_trigger_count },
        best_rank: { value: (row) => row.best_rank },
      }) satisfies Partial<
        Record<StrategyDailySortKey, SortDefinition<StrategyDailyRow>>
      >,
    [],
  );
  const {
    sortKey: dailySortKey,
    sortDirection: dailySortDirection,
    sortedRows: sortedRowsForAnalysisDate,
    toggleSort: toggleDailySort,
  } = useTableSort<StrategyDailyRow, StrategyDailySortKey>(
    rowsForAnalysisDate,
    dailySortDefinitions,
    {
      key: "contribution_score",
      direction: "desc",
    },
  );
  const dailyTableWrapRef = useRouteScrollRegion<HTMLDivElement>(
    "strategy-trigger-daily-table",
    [sortedRowsForAnalysisDate.length, analysisTradeDate],
  );
  const sceneSortDefinitions = useMemo(
    () =>
      ({
        scene_name: { value: (row) => row.scene_name },
        trigger_count: { value: (row) => row.trigger_count },
        confirm_count: { value: (row) => row.confirm_count },
        observe_count: { value: (row) => row.observe_count },
        fail_count: { value: (row) => row.fail_count },
        none_count: { value: (row) => row.none_count },
        scene_covered_count: { value: (row) => row.scene_covered_count },
        scene_total_sample_count: {
          value: (row) => row.scene_total_sample_count,
        },
        scene_coverage_ratio: { value: (row) => row.scene_coverage_ratio },
        scene_rule_contribution_ratio: {
          value: (row) => row.scene_rule_contribution_ratio,
        },
      }) satisfies Partial<
        Record<SceneStatisticsSortKey, SortDefinition<SceneStatisticsTableRow>>
      >,
    [],
  );
  const {
    sortKey: sceneSortKey,
    sortDirection: sceneSortDirection,
    sortedRows: sortedSceneRows,
    toggleSort: toggleSceneSort,
  } = useTableSort<SceneStatisticsTableRow, SceneStatisticsSortKey>(
    sceneTableRows,
    sceneSortDefinitions,
    {
      key: "scene_rule_contribution_ratio",
      direction: "desc",
    },
  );
  const sceneTableWrapRef = useRouteScrollRegion<HTMLDivElement>(
    "strategy-trigger-scene-table",
    [
      sortedSceneRows.length,
      scenePageData?.resolved_analysis_trade_date ?? analysisTradeDate,
    ],
  );

  useEffect(() => {
    const nextSelectedDate = pickInitialHeatmapDate(
      heatmapSlots,
      pageData?.overview?.latest_trade_date,
    );
    setSelectedOverviewDate((current) => {
      if (
        current &&
        heatmapSlots.some((slot) => slot.compactDate === current)
      ) {
        return current;
      }
      return nextSelectedDate;
    });
  }, [heatmapSlots, pageData?.overview?.latest_trade_date]);

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

  useEffect(() => {
    const handlePointerDown = (event: PointerEvent) => {
      if (!(event.target instanceof Element)) {
        return;
      }

      if (event.target.closest(".strategy-trigger-calendar-day")) {
        return;
      }

      setSelectedOverviewDate(null);
      setHeatmapTooltip(null);
    };

    window.addEventListener("pointerdown", handlePointerDown);
    return () => {
      window.removeEventListener("pointerdown", handlePointerDown);
    };
  }, []);

  async function loadSceneStatistics(
    nextAnalysisTradeDate?: string,
    nextSourcePath?: string,
  ) {
    const requestedTradeDate =
      nextAnalysisTradeDate?.trim() ?? analysisTradeDate.trim();

    setSceneLoading(true);
    setSceneError("");

    try {
      let resolvedSourcePath = nextSourcePath?.trim() ?? sourcePath.trim();
      if (!resolvedSourcePath) {
        resolvedSourcePath = await ensureManagedSourcePath();
      }
      setSourcePath(resolvedSourcePath);

      const baseData = await getSceneStatisticsPage({
        sourcePath: resolvedSourcePath,
        analysisTradeDate: requestedTradeDate || undefined,
      });
      const sceneOptions = baseData.scene_options ?? [];
      const resolvedTradeDate =
        baseData.resolved_analysis_trade_date?.trim() ?? requestedTradeDate;

      const expandedRows =
        sceneOptions.length > 0 && resolvedTradeDate
          ? await Promise.all(
              sceneOptions.map(async (scene) => {
                const sceneData = await getSceneStatisticsPage({
                  sourcePath: resolvedSourcePath,
                  sceneName: scene,
                  analysisTradeDate: resolvedTradeDate,
                });
                return buildSceneStatisticsTableRow(scene, sceneData);
              }),
            )
          : [];

      setScenePageData(baseData);
      setSceneTableRows(expandedRows);
      setSceneName((current) => {
        if (current && sceneOptions.includes(current)) {
          return current;
        }
        return baseData.resolved_scene_name ?? sceneOptions[0] ?? "";
      });
    } catch (loadError) {
      setScenePageData(null);
      setSceneTableRows([]);
      setSceneError(String(loadError));
    } finally {
      setSceneLoading(false);
    }
  }

  async function loadPage(nextAnalysisTradeDate?: string) {
    setLoading(true);
    setError("");

    try {
      const resolvedSourcePath = await ensureManagedSourcePath();
      setSourcePath(resolvedSourcePath);

      const data = await getStrategyStatisticsPage({
        sourcePath: resolvedSourcePath,
        analysisTradeDate: nextAnalysisTradeDate?.trim()
          ? nextAnalysisTradeDate.trim()
          : undefined,
      });

      setPageData(data);
      setStrategyName((current) => {
        if (current && (data.strategy_options ?? []).includes(current)) {
          return current;
        }
        return data.resolved_strategy_name ?? current;
      });
      const resolvedTradeDate = data.resolved_analysis_trade_date ?? "";
      setAnalysisTradeDate(resolvedTradeDate);
      await loadSceneStatistics(resolvedTradeDate, resolvedSourcePath);
    } catch (loadError) {
      setError(String(loadError));
    } finally {
      setLoading(false);
    }
  }

  function handleStrategyChange(nextValue: string) {
    setStrategyName(nextValue);
    if (nextValue.trim()) {
      void openStrategyDetail(nextValue, analysisTradeDate);
    }
  }

  function handleAnalysisTradeDateChange(nextValue: string) {
    setAnalysisTradeDate(nextValue);
  }

  async function openStrategyDetail(
    nextStrategyName: string,
    nextAnalysisTradeDate: string,
  ) {
    const normalizedStrategyName = nextStrategyName.trim();
    const normalizedTradeDate = nextAnalysisTradeDate.trim();
    if (!normalizedStrategyName) {
      setDetailModalOpen(true);
      setDetailData(null);
      setDetailError("请先选择一条策略。");
      return;
    }
    let resolvedSourcePath = sourcePath.trim();
    if (!resolvedSourcePath) {
      try {
        resolvedSourcePath = await ensureManagedSourcePath();
        setSourcePath(resolvedSourcePath);
      } catch (loadError) {
        setDetailModalOpen(true);
        setDetailData(null);
        setDetailError(String(loadError));
        return;
      }
    }
    const cacheKey = `${resolvedSourcePath}|${normalizedStrategyName}|${normalizedTradeDate}`;
    setStrategyName(normalizedStrategyName);
    setDetailModalOpen(true);
    if (Object.prototype.hasOwnProperty.call(detailCache, cacheKey)) {
      setDetailLoading(false);
      setDetailData(detailCache[cacheKey] ?? null);
      setDetailError("");
      return;
    }
    setDetailLoading(true);
    setDetailError("");

    try {
      const nextDetailData = await getStrategyStatisticsDetail({
        sourcePath: resolvedSourcePath,
        strategyName: normalizedStrategyName,
        analysisTradeDate: normalizedTradeDate || undefined,
      });
      setDetailData(nextDetailData);
      setDetailCache((current) => ({
        ...current,
        [cacheKey]: nextDetailData,
      }));
    } catch (loadError) {
      setDetailData(null);
      setDetailError(String(loadError));
    } finally {
      setDetailLoading(false);
    }
  }

  function openHeatmapTooltip(slot: HeatmapSlot, target: HTMLButtonElement) {
    if (!slot.compactDate) {
      return;
    }

    const targetRect = target.getBoundingClientRect();
    const tooltipWidth = 220;
    const screenPadding = 12;
    const centerX = targetRect.left + targetRect.width / 2;
    const left = Math.min(
      Math.max(centerX, screenPadding + tooltipWidth / 2),
      window.innerWidth - screenPadding - tooltipWidth / 2,
    );
    const preferBottom = targetRect.top < 132;
    const top = preferBottom ? targetRect.bottom + 12 : targetRect.top - 12;

    setSelectedOverviewDate(slot.compactDate);
    setHeatmapTooltip({
      left,
      top,
      placement: preferBottom ? "bottom" : "top",
    });
  }

  return (
    <div className="strategy-trigger-page">
      <section className="strategy-trigger-card">
        <h2 className="strategy-trigger-title">策略触发统计</h2>
        <div className="strategy-trigger-source-note">
          <span>
            统计口径基于结果库 `rule_details / scene_details / score_summary`
            与规则文件。
          </span>
        </div>
        <div className="strategy-trigger-form-grid">
          <label className="strategy-trigger-field">
            <span>分析日期</span>
            <select
              value={analysisTradeDate}
              onChange={(event) =>
                handleAnalysisTradeDateChange(event.target.value)
              }
              disabled={loading || analysisTradeDateOptions.length === 0}
            >
              {analysisTradeDateOptions.length === 0 ? (
                <option value="">暂无日期</option>
              ) : null}
              {analysisTradeDateOptions.map((item) => (
                <option key={item} value={item}>
                  {formatDateLabel(item)}
                </option>
              ))}
            </select>
          </label>
          <label className="strategy-trigger-field">
            <span>单策略</span>
            <select
              value={strategyName}
              onChange={(event) => handleStrategyChange(event.target.value)}
              disabled={strategyOptions.length === 0}
            >
              <option value="">
                {strategyOptions.length === 0 ? "暂无策略" : "请选择策略"}
              </option>
              {strategyOptions.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
          </label>
        </div>
        <div className="strategy-trigger-actions">
          <button
            type="button"
            className="strategy-trigger-primary-btn"
            onClick={() => void loadPage(analysisTradeDate)}
            disabled={loading}
          >
            {loading ? "读取中..." : "读取统计"}
          </button>
          <span className="strategy-trigger-tip">
            点击“读取统计”更新总览；点击列表里的策略名或上方单策略可查看明细。
          </span>
        </div>
        {error ? <div className="strategy-trigger-error">{error}</div> : null}
      </section>

      <section className="strategy-trigger-card strategy-trigger-card-overview">
        <div className="strategy-trigger-overview-layout">
          <div className="strategy-trigger-overview-main">
            <div className="strategy-trigger-overview-head">
              <h3 className="strategy-trigger-subtitle">总体策略情况</h3>
              <p className="strategy-trigger-caption">
                小格子图每格代表一个自然日；红色表示强于平均，绿色表示弱于平均，灰色表示非交易日；右侧小K线展示日度差值。
              </p>
            </div>
            {calendarMonths.length === 0 ? (
              <div className="strategy-trigger-empty">
                暂无总体策略统计数据。
              </div>
            ) : (
              <div
                className="strategy-trigger-calendar"
                onScroll={() => setHeatmapTooltip(null)}
              >
                {calendarMonths.map((month) => (
                  <section
                    key={month.key}
                    className="strategy-trigger-calendar-month"
                  >
                    <div className="strategy-trigger-calendar-month-title">
                      {month.label}
                    </div>
                    <div className="strategy-trigger-calendar-weekdays">
                      {WEEKDAY_LABELS.map((label) => (
                        <span key={`${month.key}-${label}`}>{label}</span>
                      ))}
                    </div>
                    <div className="strategy-trigger-calendar-grid">
                      {month.slots.map((slot) => {
                        if (!slot.compactDate) {
                          return (
                            <div
                              key={slot.key}
                              className="strategy-trigger-calendar-gap"
                              aria-hidden="true"
                            />
                          );
                        }

                        const className = slot.cell
                          ? slot.cell.above_avg
                            ? "strategy-trigger-heatmap-cell is-strong"
                            : "strategy-trigger-heatmap-cell is-weak"
                          : "strategy-trigger-heatmap-cell is-empty";

                        return (
                          <button
                            key={slot.key}
                            type="button"
                            className={[
                              "strategy-trigger-calendar-day",
                              className,
                              slot.compactDate === selectedOverviewDate &&
                              heatmapTooltip
                                ? "is-selected"
                                : "",
                            ]
                              .filter(Boolean)
                              .join(" ")}
                            title={buildHeatmapTitle(slot.cell, slot.label)}
                            aria-label={buildHeatmapTitle(
                              slot.cell,
                              slot.label,
                            )}
                            onClick={(event) => {
                              if (
                                slot.compactDate === selectedOverviewDate &&
                                heatmapTooltip
                              ) {
                                setSelectedOverviewDate(null);
                                setHeatmapTooltip(null);
                                return;
                              }
                              openHeatmapTooltip(slot, event.currentTarget);
                            }}
                          >
                            <span>{slot.dayOfMonth}</span>
                          </button>
                        );
                      })}
                    </div>
                  </section>
                ))}
                {selectedOverviewSlot && heatmapTooltip ? (
                  <div
                    className={[
                      "strategy-trigger-heatmap-tooltip",
                      heatmapTooltip.placement === "bottom"
                        ? "is-bottom"
                        : "is-top",
                    ]
                      .filter(Boolean)
                      .join(" ")}
                    role="status"
                    aria-live="polite"
                    style={{
                      left: `${heatmapTooltip.left}px`,
                      top: `${heatmapTooltip.top}px`,
                    }}
                  >
                    <div className="strategy-trigger-heatmap-tooltip-head">
                      <strong>{selectedOverviewSlot.label}</strong>
                      <span>
                        {selectedOverviewSlot.cell
                          ? selectedOverviewSlot.cell.above_avg
                            ? "强于平均"
                            : "弱于平均"
                          : "非交易日"}
                      </span>
                    </div>
                    {selectedOverviewSlot.cell ? (
                      <div className="strategy-trigger-heatmap-tooltip-grid">
                        <span>当日水平</span>
                        <strong>
                          {formatNumber(selectedOverviewSlot.cell.day_level)}
                        </strong>
                        <span>平均水平</span>
                        <strong>
                          {formatNumber(selectedOverviewSlot.cell.avg_level)}
                        </strong>
                        <span>差值</span>
                        <strong
                          className={getSignedValueClassName(
                            selectedOverviewSlot.cell.delta_level,
                          )}
                        >
                          {formatSignedNumber(
                            selectedOverviewSlot.cell.delta_level,
                          )}
                        </strong>
                      </div>
                    ) : (
                      <div className="strategy-trigger-heatmap-tooltip-empty">
                        当天没有交易数据。
                      </div>
                    )}
                  </div>
                ) : null}
              </div>
            )}
          </div>
          <div className="strategy-trigger-overview-side">
            <div className="strategy-trigger-summary-grid">
              <div className="strategy-trigger-summary-item">
                <span>最新交易日</span>
                <strong>
                  {formatDateLabel(pageData?.overview?.latest_trade_date)}
                </strong>
              </div>
              <div className="strategy-trigger-summary-item">
                <span>历史平均水平</span>
                <strong>
                  {formatNumber(pageData?.overview?.average_level)}
                </strong>
              </div>
              <div className="strategy-trigger-summary-item">
                <span>最新当日水平</span>
                <strong>{formatNumber(overviewSummary.latestDayLevel)}</strong>
              </div>
              <div className="strategy-trigger-summary-item">
                <span>强于平均天数</span>
                <strong>{formatInteger(overviewSummary.strongDays)}</strong>
              </div>
            </div>
            <StrategyOverviewDeltaChart items={overviewItems} />
          </div>
        </div>
      </section>

      <section className="strategy-trigger-card">
        <div className="strategy-trigger-section-head">
          <div>
            <h3 className="strategy-trigger-subtitle">Scene 触发统计</h3>
            <p className="strategy-trigger-caption">
              {formatDateLabel(
                scenePageData?.resolved_analysis_trade_date ??
                  analysisTradeDate,
              )}{" "}
              · 按 Scene 直出
            </p>
          </div>
        </div>
        {sceneError ? (
          <div className="strategy-trigger-error">{sceneError}</div>
        ) : null}
        {sceneLoading ? (
          <div className="strategy-trigger-empty">Scene 统计读取中...</div>
        ) : sortedSceneRows.length === 0 ? (
          <div className="strategy-trigger-empty">
            该分析日期暂无 Scene 统计样本。
          </div>
        ) : (
          <div className="strategy-trigger-table-wrap" ref={sceneTableWrapRef}>
            <table className="strategy-trigger-table strategy-trigger-table-scene">
              <thead>
                <tr>
                  <th
                    aria-sort={getAriaSort(
                      sceneSortKey === "scene_name",
                      sceneSortDirection,
                    )}
                  >
                    <TableSortButton
                      label="Scene"
                      isActive={sceneSortKey === "scene_name"}
                      direction={sceneSortDirection}
                      onClick={() => toggleSceneSort("scene_name")}
                      title="按 Scene 排序"
                    />
                  </th>
                  <th
                    aria-sort={getAriaSort(
                      sceneSortKey === "confirm_count",
                      sceneSortDirection,
                    )}
                  >
                    <TableSortButton
                      label="confirm"
                      isActive={sceneSortKey === "confirm_count"}
                      direction={sceneSortDirection}
                      onClick={() => toggleSceneSort("confirm_count")}
                      title="按 confirm 样本数排序"
                    />
                  </th>
                  <th
                    aria-sort={getAriaSort(
                      sceneSortKey === "trigger_count",
                      sceneSortDirection,
                    )}
                  >
                    <TableSortButton
                      label="trigger"
                      isActive={sceneSortKey === "trigger_count"}
                      direction={sceneSortDirection}
                      onClick={() => toggleSceneSort("trigger_count")}
                      title="按 trigger 样本数排序"
                    />
                  </th>
                  <th
                    aria-sort={getAriaSort(
                      sceneSortKey === "observe_count",
                      sceneSortDirection,
                    )}
                  >
                    <TableSortButton
                      label="observe"
                      isActive={sceneSortKey === "observe_count"}
                      direction={sceneSortDirection}
                      onClick={() => toggleSceneSort("observe_count")}
                      title="按 observe 样本数排序"
                    />
                  </th>
                  <th
                    aria-sort={getAriaSort(
                      sceneSortKey === "fail_count",
                      sceneSortDirection,
                    )}
                  >
                    <TableSortButton
                      label="fail"
                      isActive={sceneSortKey === "fail_count"}
                      direction={sceneSortDirection}
                      onClick={() => toggleSceneSort("fail_count")}
                      title="按 fail 样本数排序"
                    />
                  </th>
                  <th
                    aria-sort={getAriaSort(
                      sceneSortKey === "none_count",
                      sceneSortDirection,
                    )}
                  >
                    <TableSortButton
                      label="none"
                      isActive={sceneSortKey === "none_count"}
                      direction={sceneSortDirection}
                      onClick={() => toggleSceneSort("none_count")}
                      title="按 none 样本数排序"
                    />
                  </th>
                  <th
                    aria-sort={getAriaSort(
                      sceneSortKey === "scene_covered_count",
                      sceneSortDirection,
                    )}
                  >
                    <TableSortButton
                      label="已定级样本数"
                      isActive={sceneSortKey === "scene_covered_count"}
                      direction={sceneSortDirection}
                      onClick={() => toggleSceneSort("scene_covered_count")}
                      title="按已定级样本数排序"
                    />
                  </th>
                  <th
                    aria-sort={getAriaSort(
                      sceneSortKey === "scene_total_sample_count",
                      sceneSortDirection,
                    )}
                  >
                    <TableSortButton
                      label="Scene样本总数"
                      isActive={sceneSortKey === "scene_total_sample_count"}
                      direction={sceneSortDirection}
                      onClick={() =>
                        toggleSceneSort("scene_total_sample_count")
                      }
                      title="按 Scene 样本总数排序"
                    />
                  </th>
                  <th
                    aria-sort={getAriaSort(
                      sceneSortKey === "scene_coverage_ratio",
                      sceneSortDirection,
                    )}
                  >
                    <TableSortButton
                      label="Scene覆盖率"
                      isActive={sceneSortKey === "scene_coverage_ratio"}
                      direction={sceneSortDirection}
                      onClick={() => toggleSceneSort("scene_coverage_ratio")}
                      title="按 Scene 覆盖率排序"
                    />
                  </th>
                  <th
                    aria-sort={getAriaSort(
                      sceneSortKey === "scene_rule_contribution_ratio",
                      sceneSortDirection,
                    )}
                  >
                    <TableSortButton
                      label="scene贡献分占比"
                      isActive={
                        sceneSortKey === "scene_rule_contribution_ratio"
                      }
                      direction={sceneSortDirection}
                      onClick={() =>
                        toggleSceneSort("scene_rule_contribution_ratio")
                      }
                      title="按 scene贡献分占比排序"
                    />
                  </th>
                </tr>
              </thead>
              <tbody>
                {sortedSceneRows.map((row) => (
                  <tr
                    key={`${scenePageData?.resolved_analysis_trade_date ?? analysisTradeDate}-${row.scene_name}`}
                  >
                    <td>{row.scene_name}</td>
                    <td>
                      {formatStageCountWithShare(
                        row.confirm_count,
                        row.scene_covered_count,
                      )}
                    </td>
                    <td>
                      {formatStageCountWithShare(
                        row.trigger_count,
                        row.scene_covered_count,
                      )}
                    </td>
                    <td>
                      {formatStageCountWithShare(
                        row.observe_count,
                        row.scene_covered_count,
                      )}
                    </td>
                    <td>
                      {formatStageCountWithShare(
                        row.fail_count,
                        row.scene_covered_count,
                      )}
                    </td>
                    <td>
                      {formatStageCountWithShare(
                        row.none_count,
                        row.scene_total_sample_count,
                      )}
                    </td>
                    <td>{formatInteger(row.scene_covered_count)}</td>
                    <td>{formatInteger(row.scene_total_sample_count)}</td>
                    <td>{formatPercent(row.scene_coverage_ratio)}</td>
                    <td>{formatPercent(row.scene_rule_contribution_ratio)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section className="strategy-trigger-card">
        <div className="strategy-trigger-section-head">
          <div>
            <h3 className="strategy-trigger-subtitle">策略触发列表</h3>
            <p className="strategy-trigger-caption">
              {formatDateLabel(analysisTradeDate)}
            </p>
          </div>
        </div>
        {rowsForAnalysisDate.length === 0 ? (
          <div className="strategy-trigger-empty">该分析日期暂无触发策略。</div>
        ) : (
          <div className="strategy-trigger-table-wrap" ref={dailyTableWrapRef}>
            <table className="strategy-trigger-table">
              <thead>
                <tr>
                  <th
                    aria-sort={getAriaSort(
                      dailySortKey === "rule_name",
                      dailySortDirection,
                    )}
                  >
                    <TableSortButton
                      label="策略名"
                      isActive={dailySortKey === "rule_name"}
                      direction={dailySortDirection}
                      onClick={() => toggleDailySort("rule_name")}
                      title="按策略名排序"
                    />
                  </th>
                  <th
                    aria-sort={getAriaSort(
                      dailySortKey === "trigger_mode",
                      dailySortDirection,
                    )}
                  >
                    <TableSortButton
                      label="触发模式"
                      isActive={dailySortKey === "trigger_mode"}
                      direction={dailySortDirection}
                      onClick={() => toggleDailySort("trigger_mode")}
                      title="按触发模式排序"
                    />
                  </th>
                  <th
                    aria-sort={getAriaSort(
                      dailySortKey === "trigger_count",
                      dailySortDirection,
                    )}
                  >
                    <TableSortButton
                      label="触发次数"
                      isActive={dailySortKey === "trigger_count"}
                      direction={dailySortDirection}
                      onClick={() => toggleDailySort("trigger_count")}
                      title="按触发次数排序"
                    />
                  </th>
                  <th
                    aria-sort={getAriaSort(
                      dailySortKey === "contribution_score",
                      dailySortDirection,
                    )}
                  >
                    <TableSortButton
                      label="策略贡献度"
                      isActive={dailySortKey === "contribution_score"}
                      direction={dailySortDirection}
                      onClick={() => toggleDailySort("contribution_score")}
                      title="按策略贡献度排序"
                    />
                  </th>
                  <th
                    aria-sort={getAriaSort(
                      dailySortKey === "contribution_per_trigger",
                      dailySortDirection,
                    )}
                  >
                    <TableSortButton
                      label="单次贡献"
                      isActive={dailySortKey === "contribution_per_trigger"}
                      direction={dailySortDirection}
                      onClick={() =>
                        toggleDailySort("contribution_per_trigger")
                      }
                      title="按单次贡献排序"
                    />
                  </th>
                  <th
                    aria-sort={getAriaSort(
                      dailySortKey === "top100_trigger_count",
                      dailySortDirection,
                    )}
                  >
                    <TableSortButton
                      label="前100触发次数"
                      isActive={dailySortKey === "top100_trigger_count"}
                      direction={dailySortDirection}
                      onClick={() => toggleDailySort("top100_trigger_count")}
                      title="按前100触发次数排序"
                    />
                  </th>
                  <th
                    aria-sort={getAriaSort(
                      dailySortKey === "coverage",
                      dailySortDirection,
                    )}
                  >
                    <TableSortButton
                      label="覆盖率"
                      isActive={dailySortKey === "coverage"}
                      direction={dailySortDirection}
                      onClick={() => toggleDailySort("coverage")}
                      title="按覆盖率排序"
                    />
                  </th>
                  <th
                    aria-sort={getAriaSort(
                      dailySortKey === "median_trigger_count",
                      dailySortDirection,
                    )}
                  >
                    <TableSortButton
                      label="中位触发次数"
                      isActive={dailySortKey === "median_trigger_count"}
                      direction={dailySortDirection}
                      onClick={() => toggleDailySort("median_trigger_count")}
                      title="按中位触发次数排序"
                    />
                  </th>
                  <th
                    aria-sort={getAriaSort(
                      dailySortKey === "best_rank",
                      dailySortDirection,
                    )}
                  >
                    <TableSortButton
                      label="当日最优排名"
                      isActive={dailySortKey === "best_rank"}
                      direction={dailySortDirection}
                      onClick={() => toggleDailySort("best_rank")}
                      title="按当日最优排名排序"
                    />
                  </th>
                </tr>
              </thead>
              <tbody>
                {sortedRowsForAnalysisDate.map((row) => {
                  const isActive = row.rule_name === strategyName;
                  return (
                    <tr
                      key={`${row.trade_date}-${row.rule_name}`}
                      className={isActive ? "is-active" : ""}
                    >
                      <td>
                        <button
                          type="button"
                          className="strategy-trigger-inline-btn strategy-trigger-inline-btn-name"
                          onClick={() => {
                            setStrategyName(row.rule_name);
                            void openStrategyDetail(
                              row.rule_name,
                              analysisTradeDate,
                            );
                          }}
                        >
                          {row.rule_name}
                        </button>
                      </td>
                      <td>{row.trigger_mode ?? "--"}</td>
                      <td>{formatInteger(row.trigger_count)}</td>
                      <td
                        className={getSignedValueClassName(
                          row.contribution_score,
                        )}
                      >
                        {formatNumber(row.contribution_score)}
                      </td>
                      <td
                        className={getSignedValueClassName(
                          row.contribution_per_trigger,
                        )}
                      >
                        {formatNumber(row.contribution_per_trigger)}
                      </td>
                      <td>{formatInteger(row.top100_trigger_count)}</td>
                      <td>{formatPercent(row.coverage)}</td>
                      <td>{formatNumber(row.median_trigger_count)}</td>
                      <td>{formatInteger(row.best_rank)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {detailModalOpen ? (
        <StrategyDetailModal
          sourcePath={sourcePath}
          strategyName={strategyName}
          detailData={detailData}
          loading={detailLoading}
          error={detailError}
          onClose={() => {
            setDetailModalOpen(false);
            setDetailError("");
          }}
          onChangeTradeDate={(tradeDate) => {
            void openStrategyDetail(strategyName, tradeDate);
          }}
        />
      ) : null}
    </div>
  );
}
