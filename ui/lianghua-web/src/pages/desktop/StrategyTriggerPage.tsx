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
  getStrategyStatisticsDetail,
  getStrategyStatisticsPage,
  type StrategyChartPoint,
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

type StrategyChartGeometry = {
  width: number;
  height: number;
  viewBox: string;
  leftTicks: Array<{ value: number; y: number }>;
  rightTicks: Array<{ value: number; y: number }>;
  xLabels: Array<{ value: string; x: number }>;
  totalBars: Array<{
    key: string;
    x: number;
    y: number;
    width: number;
    height: number;
    item: StrategyChartPoint;
  }>;
  topBars: Array<{
    key: string;
    x: number;
    y: number;
    width: number;
    height: number;
    item: StrategyChartPoint;
  }>;
  linePath: string;
  dots: Array<{
    key: string;
    cx: number;
    cy: number;
    item: StrategyChartPoint;
  }>;
};

type PersistedStrategyTriggerState = {
  sourcePath: string;
  pageData: StrategyStatisticsPageData | null;
  strategyName: string;
  analysisTradeDate: string;
  selectedOverviewDate: string | null;
  detailModalOpen: boolean;
  detailData: StrategyStatisticsDetailData | null;
  detailError: string;
};

const WEEKDAY_LABELS = ["一", "二", "三", "四", "五", "六", "日"] as const;
const STRATEGY_TRIGGER_STATE_KEY = "lh_strategy_trigger_page_state_v5";
const MINI_CANDLE_UP_COLOR = "#d9485f";
const MINI_CANDLE_DOWN_COLOR = "#178f68";
const MINI_CANDLE_FLAT_COLOR = "#536273";
const OVERVIEW_CHART_TOOLTIP_LEFT_THRESHOLD = 62;
const OVERVIEW_CHART_POINTER_DRAG_THRESHOLD = 6;
const OVERVIEW_CHART_TOUCH_FOCUS_HIT_SLOP = 24;

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
  if (item.delta_level !== null && item.delta_level !== undefined && Number.isFinite(item.delta_level)) {
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
  return Math.abs(clientY - focusClientY) <= OVERVIEW_CHART_TOUCH_FOCUS_HIT_SLOP;
}

function buildOverviewDeltaCandleTitle(
  item: StrategyHeatmapCell,
  open: number,
  close: number,
) {
  const delta = getOverviewDeltaValue(item);
  return [
    `日期: ${formatDateLabel(item.trade_date)}`,
    `开: ${formatSignedNumber(open)}`,
    `收: ${formatSignedNumber(close)}`,
    `当日差值: ${formatSignedNumber(delta ?? close - open)}`,
    `当日值: ${formatNumber(item.day_level)}`,
    `平均值: ${formatNumber(item.avg_level)}`,
  ].join("\n");
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
  const rawMin = Math.min(...cumulativeValues);
  const rawMax = Math.max(...cumulativeValues);
  const rawSpan = rawMax - rawMin;
  const padding =
    rawSpan > 0
      ? rawSpan * 0.12
      : Math.max(Math.abs(rawMax || rawMin) * 0.08, 0.5);
  const scaledMin = rawMin - padding;
  const scaledMax = rawMax + padding;
  const domainSpan = Math.max(scaledMax - scaledMin, 0.01);
  const valueToY = (value: number) =>
    marginTop + ((scaledMax - value) / domainSpan) * plotHeight;
  const zeroY =
    scaledMin <= 0 && scaledMax >= 0 ? valueToY(0) : null;
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

function buildChartGeometry(
  items: StrategyChartPoint[],
): StrategyChartGeometry | null {
  if (items.length === 0) {
    return null;
  }

  const width = 960;
  const height = 320;
  const marginTop = 20;
  const marginRight = 58;
  const marginBottom = 48;
  const marginLeft = 56;
  const plotWidth = width - marginLeft - marginRight;
  const plotHeight = height - marginTop - marginBottom;
  const countMax = Math.max(
    1,
    ...items.map((item) =>
      item.trigger_count && Number.isFinite(item.trigger_count)
        ? item.trigger_count
        : 0,
    ),
  );
  const coverageMax = Math.max(
    0.05,
    Math.min(
      1,
      Math.max(
        ...items.map((item) =>
          item.coverage && Number.isFinite(item.coverage) ? item.coverage : 0,
        ),
      ) * 1.15,
    ),
  );
  const slotWidth = plotWidth / items.length;
  const barGap = Math.max(6, slotWidth * 0.16);
  const barWidth = Math.max(10, slotWidth - barGap * 2);
  const countScale = (value: number) =>
    marginTop + plotHeight - (value / countMax) * plotHeight;
  const coverageScale = (value: number) =>
    marginTop +
    plotHeight -
    (Math.min(value, coverageMax) / coverageMax) * plotHeight;

  const totalBars = items.map((item, index) => {
    const value =
      item.trigger_count && Number.isFinite(item.trigger_count)
        ? item.trigger_count
        : 0;
    const x = marginLeft + index * slotWidth + barGap;
    const y = countScale(value);
    return {
      key: `${item.trade_date}-total`,
      x,
      y,
      width: barWidth,
      height: marginTop + plotHeight - y,
      item,
    };
  });

  const topBars = items.map((item, index) => {
    const value =
      item.top100_trigger_count && Number.isFinite(item.top100_trigger_count)
        ? Math.min(item.top100_trigger_count, item.trigger_count ?? 0)
        : 0;
    const overlayWidth = Math.max(6, barWidth - 4);
    const x =
      marginLeft + index * slotWidth + barGap + (barWidth - overlayWidth) / 2;
    const y = countScale(value);
    return {
      key: `${item.trade_date}-top`,
      x,
      y,
      width: overlayWidth,
      height: marginTop + plotHeight - y,
      item,
    };
  });

  const dots = items.map((item, index) => {
    const coverage =
      item.coverage && Number.isFinite(item.coverage) ? item.coverage : 0;
    return {
      key: `${item.trade_date}-coverage`,
      cx: marginLeft + index * slotWidth + slotWidth / 2,
      cy: coverageScale(coverage),
      item,
    };
  });

  const linePath = dots
    .map(
      (dot, index) =>
        `${index === 0 ? "M" : "L"} ${dot.cx.toFixed(2)} ${dot.cy.toFixed(2)}`,
    )
    .join(" ");

  const leftTicks = [0, 0.25, 0.5, 0.75, 1].map((ratio) => ({
    value: Math.round(countMax * ratio),
    y: countScale(countMax * ratio),
  }));
  const rightTicks = [0, 0.25, 0.5, 0.75, 1].map((ratio) => ({
    value: coverageMax * ratio,
    y: coverageScale(coverageMax * ratio),
  }));

  const labelStep = Math.max(1, Math.ceil(items.length / 8));
  const xLabels = items
    .map((item, index) => ({ item, index }))
    .filter(
      ({ index }) => index % labelStep === 0 || index === items.length - 1,
    )
    .map(({ item, index }) => ({
      value: formatDateLabel(item.trade_date).slice(5),
      x: marginLeft + index * slotWidth + slotWidth / 2,
    }));

  return {
    width,
    height,
    viewBox: `0 0 ${width} ${height}`,
    leftTicks,
    rightTicks,
    xLabels,
    totalBars,
    topBars,
    linePath,
    dots,
  };
}

function buildChartTitle(item: StrategyChartPoint) {
  return [
    `日期: ${formatDateLabel(item.trade_date)}`,
    `触发次数: ${formatInteger(item.trigger_count)}`,
    `前100触发次数: ${formatInteger(item.top100_trigger_count)}`,
    `覆盖率: ${formatPercent(item.coverage)}`,
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

  useEffect(() => {
    setFocus(null);
  }, [geometry]);

  if (!geometry) {
    return (
      <div className="strategy-trigger-overview-mini-chart strategy-trigger-overview-mini-chart-empty">
        暂无差值数据。
      </div>
    );
  }

  const chartGeometry = geometry;
  const latestDelta = geometry.latestDelta ?? 0;
  const latestDeltaClassName =
    latestDelta > 0
      ? "is-positive"
      : latestDelta < 0
        ? "is-negative"
        : "is-flat";
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
      // Pointer capture is a progressive enhancement.
    }
  }

  function releaseOverviewPointer(event: ReactPointerEvent<HTMLDivElement>) {
    try {
      if (event.currentTarget.hasPointerCapture(event.pointerId)) {
        event.currentTarget.releasePointerCapture(event.pointerId);
      }
    } catch {
      // Ignore browsers that do not fully support pointer capture.
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
    const mode =
      focus?.pinned
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

    if (
      !pointerState ||
      pointerState.pointerId !== event.pointerId
    ) {
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
    if (
      pointerState &&
      pointerState.pointerId === event.pointerId
    ) {
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
        <div
          className={[
            "strategy-trigger-overview-mini-chart-latest",
            latestDeltaClassName,
          ]
            .filter(Boolean)
            .join(" ")}
        >
          <span>{formatDateLabel(geometry.latestTradeDate)}</span>
          <strong>{formatSignedNumber(geometry.latestDelta)}</strong>
        </div>
      </div>
      <div className="strategy-trigger-overview-mini-chart-scroll" ref={scrollRef}>
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
                        candle.open,
                        candle.close,
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
                <span>{formatSignedNumber(getOverviewDeltaValue(focusCandle.item))}</span>
              </div>
              <div className="strategy-trigger-overview-mini-chart-tooltip-body">
                <div className="strategy-trigger-overview-mini-chart-tooltip-grid">
                  <div className="strategy-trigger-overview-mini-chart-tooltip-row">
                    <span>开</span>
                    <strong>{formatSignedNumber(focusCandle.open)}</strong>
                  </div>
                  <div className="strategy-trigger-overview-mini-chart-tooltip-row">
                    <span>收</span>
                    <strong>{formatSignedNumber(focusCandle.close)}</strong>
                  </div>
                  <div className="strategy-trigger-overview-mini-chart-tooltip-row">
                    <span>当日差值</span>
                    <strong>
                      {formatSignedNumber(getOverviewDeltaValue(focusCandle.item))}
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

function StrategyAnalysisChart({ items }: { items: StrategyChartPoint[] }) {
  const geometry = useMemo(() => buildChartGeometry(items), [items]);

  if (!geometry) {
    return (
      <div className="strategy-trigger-empty">
        当前策略暂无可展示的日度数据。
      </div>
    );
  }

  return (
    <div className="strategy-trigger-chart-shell">
      <div className="strategy-trigger-legend">
        <span className="strategy-trigger-legend-item">
          <i className="strategy-trigger-legend-swatch strategy-trigger-legend-swatch-trigger" />
          总触发次数
        </span>
        <span className="strategy-trigger-legend-item">
          <i className="strategy-trigger-legend-swatch strategy-trigger-legend-swatch-top" />
          前100触发次数(重叠)
        </span>
        <span className="strategy-trigger-legend-item">
          <i className="strategy-trigger-legend-line" />
          覆盖率
        </span>
      </div>
      <svg
        className="strategy-trigger-chart"
        viewBox={geometry.viewBox}
        preserveAspectRatio="none"
      >
        {geometry.leftTicks.map((tick) => (
          <g key={`left-${tick.y.toFixed(2)}`}>
            <line
              x1={56}
              y1={tick.y}
              x2={902}
              y2={tick.y}
              className="strategy-trigger-chart-grid"
            />
            <text
              x={48}
              y={tick.y + 4}
              className="strategy-trigger-chart-axis strategy-trigger-chart-axis-left"
            >
              {formatInteger(tick.value)}
            </text>
          </g>
        ))}
        {geometry.rightTicks.map((tick) => (
          <text
            key={`right-${tick.y.toFixed(2)}`}
            x={912}
            y={tick.y + 4}
            className="strategy-trigger-chart-axis strategy-trigger-chart-axis-right"
          >
            {formatPercent(tick.value)}
          </text>
        ))}
        {geometry.xLabels.map((label) => (
          <text
            key={`${label.value}-${label.x.toFixed(2)}`}
            x={label.x}
            y={292}
            textAnchor="middle"
            className="strategy-trigger-chart-axis strategy-trigger-chart-axis-bottom"
          >
            {label.value}
          </text>
        ))}
        {geometry.totalBars.map((bar) => (
          <rect
            key={bar.key}
            x={bar.x}
            y={bar.y}
            width={bar.width}
            height={Math.max(bar.height, 1)}
            rx={3}
            className="strategy-trigger-chart-bar strategy-trigger-chart-bar-trigger"
          >
            <title>{buildChartTitle(bar.item)}</title>
          </rect>
        ))}
        {geometry.topBars.map((bar) => (
          <rect
            key={bar.key}
            x={bar.x}
            y={bar.y}
            width={bar.width}
            height={Math.max(bar.height, 1)}
            rx={3}
            className="strategy-trigger-chart-bar strategy-trigger-chart-bar-top"
          >
            <title>{buildChartTitle(bar.item)}</title>
          </rect>
        ))}
        <path d={geometry.linePath} className="strategy-trigger-chart-line" />
        {geometry.dots.map((dot) => (
          <circle
            key={dot.key}
            cx={dot.cx}
            cy={dot.cy}
            r={4.5}
            className="strategy-trigger-chart-dot"
          >
            <title>{buildChartTitle(dot.item)}</title>
          </circle>
        ))}
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
  const currentDateIndex = dateOptions.findIndex((item) => item === detailTradeDate);
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
  const {
    sortKey,
    sortDirection,
    sortedRows,
    toggleSort,
  } = useTableSort<TriggeredStockRow, TriggeredStockSortKey>(
    detailData?.triggered_stocks ?? [],
    stockSortDefinitions,
  );
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
              <h3 className="strategy-trigger-subtitle" id="strategy-trigger-modal-title">
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
            <div className="strategy-trigger-empty">当前没有可展示的策略明细。</div>
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
                  <span>触发次数</span>
                  <strong>{formatInteger(selectedDailyRow?.trigger_count)}</strong>
                </div>
                <div className="strategy-trigger-summary-item">
                  <span>策略贡献度</span>
                  <strong className={getSignedValueClassName(selectedDailyRow?.contribution_score)}>
                    {formatNumber(selectedDailyRow?.contribution_score)}
                  </strong>
                </div>
                <div className="strategy-trigger-summary-item">
                  <span>单次贡献</span>
                  <strong
                    className={getSignedValueClassName(
                      selectedDailyRow?.contribution_per_trigger,
                    )}
                  >
                    {formatNumber(selectedDailyRow?.contribution_per_trigger)}
                  </strong>
                </div>
                <div className="strategy-trigger-summary-item">
                  <span>前100触发次数</span>
                  <strong>{formatInteger(selectedDailyRow?.top100_trigger_count)}</strong>
                </div>
                <div className="strategy-trigger-summary-item">
                  <span>覆盖率</span>
                  <strong>{formatPercent(selectedDailyRow?.coverage)}</strong>
                </div>
                <div className="strategy-trigger-summary-item">
                  <span>中位触发次数</span>
                  <strong>{formatNumber(selectedDailyRow?.median_trigger_count)}</strong>
                </div>
                <div className="strategy-trigger-summary-item">
                  <span>当日最优排名</span>
                  <strong>{formatInteger(selectedDailyRow?.best_rank)}</strong>
                </div>
              </div>

              <div className="strategy-trigger-analysis-grid strategy-trigger-analysis-grid-modal">
                <section className="strategy-trigger-card strategy-trigger-card-analysis">
                  <div className="strategy-trigger-section-head">
                    <div>
                      <h3 className="strategy-trigger-subtitle">策略走势</h3>
                      <p className="strategy-trigger-caption">
                        {strategyName
                          ? `${strategyName} · ${formatDateLabel(detailTradeDate)}`
                          : "未选择策略"}
                      </p>
                    </div>
                  </div>
                  <StrategyAnalysisChart items={detailData.chart?.items ?? []} />
                </section>

                <section className="strategy-trigger-card strategy-trigger-card-stocks">
                  <div className="strategy-trigger-section-head">
                    <div>
                      <h3 className="strategy-trigger-subtitle">触发股票列表</h3>
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
                          onChange={(event) => onChangeTradeDate(event.target.value)}
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
                          nextTradeDate ? onChangeTradeDate(nextTradeDate) : undefined
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
                              aria-sort={getAriaSort(sortKey === "rank", sortDirection)}
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
                            <tr key={`${detailTradeDate}-${strategyName}-${row.ts_code}`}>
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
                              <td className={getSignedValueClassName(row.total_score)}>
                                {formatNumber(row.total_score)}
                              </td>
                              <td className={getSignedValueClassName(row.rule_score)}>
                                {formatNumber(row.rule_score)}
                              </td>
                              <td className="strategy-trigger-cell-concept">
                                {formatConceptText(row.concept, excludedConcepts)}
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
    () =>
      detailRows.filter(
        (row) =>
          row.trade_date === analysisTradeDate && (row.trigger_count ?? 0) > 0,
      ),
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

  async function loadPage(
    nextAnalysisTradeDate?: string,
  ) {
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
      setAnalysisTradeDate(data.resolved_analysis_trade_date ?? "");
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
          当前数据目录：<strong>{sourcePath || "--"}</strong>
          <span>
            统计口径基于结果库 `score_details / score_summary` 与规则文件。
          </span>
        </div>
        <div className="strategy-trigger-form-grid">
          <label className="strategy-trigger-field">
            <span>分析日期</span>
            <select
              value={analysisTradeDate}
              onChange={(event) => handleAnalysisTradeDateChange(event.target.value)}
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
              <div className="strategy-trigger-empty">暂无总体策略统计数据。</div>
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
                            aria-label={buildHeatmapTitle(slot.cell, slot.label)}
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
                          {formatSignedNumber(selectedOverviewSlot.cell.delta_level)}
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
                <strong>{formatNumber(pageData?.overview?.average_level)}</strong>
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
            <h3 className="strategy-trigger-subtitle">策略触发列表</h3>
            <p className="strategy-trigger-caption">{formatDateLabel(analysisTradeDate)}</p>
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
                            void openStrategyDetail(row.rule_name, analysisTradeDate);
                          }}
                        >
                          {row.rule_name}
                        </button>
                      </td>
                      <td>{row.trigger_mode ?? "--"}</td>
                      <td>{formatInteger(row.trigger_count)}</td>
                      <td className={getSignedValueClassName(row.contribution_score)}>
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
