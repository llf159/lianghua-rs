import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import {
  getReturnBacktestStrengthOverview,
  type ReturnBacktestStrengthHeatmapItem,
  type ReturnBacktestStrengthOverviewData,
} from "../../apis/returnBacktest";
import { readStoredSourcePath } from "../../shared/storage";
import { STOCK_PICK_BOARD_OPTIONS } from "../desktop/stockPickShared";
import "./css/ReturnBacktestStrengthWindow.css";

const DEFAULT_TOP_LIMIT = "100";
const DEFAULT_HOLDING_DAYS = "5";
const WEEKDAY_LABELS = ["一", "二", "三", "四", "五", "六", "日"] as const;
type BoardOption = (typeof STOCK_PICK_BOARD_OPTIONS)[number];

type HeatmapSlot = {
  key: string;
  compactDate: string | null;
  label: string;
  cell: ReturnBacktestStrengthHeatmapItem | null;
  dayOfMonth: number | null;
};

type CalendarMonth = {
  key: string;
  label: string;
  slots: HeatmapSlot[];
};

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

function formatPercent(value?: number | null) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function formatInteger(value?: number | null) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  return String(Math.round(value));
}

function normalizeBoardOption(value?: string | null): BoardOption {
  const candidate = value?.trim();
  return candidate && STOCK_PICK_BOARD_OPTIONS.includes(candidate as BoardOption)
    ? (candidate as BoardOption)
    : "全部";
}

function normalizePositiveInput(value: string | number | null | undefined, fallback: string) {
  if (typeof value === "number" && Number.isInteger(value) && value > 0) {
    return String(value);
  }
  if (typeof value === "string") {
    const next = value.trim();
    if (next !== "") {
      return next;
    }
  }
  return fallback;
}

function buildCalendarMonths(items: ReturnBacktestStrengthHeatmapItem[]) {
  if (items.length === 0) {
    return [];
  }

  const sortedItems = [...items].sort((left, right) =>
    left.rank_date.localeCompare(right.rank_date),
  );
  const cellMap = new Map(sortedItems.map((item) => [item.rank_date, item]));
  const firstDate = parseCompactDate(sortedItems[0]?.rank_date);
  const lastDate = parseCompactDate(sortedItems[sortedItems.length - 1]?.rank_date);
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

function buildHeatmapTitle(
  item: ReturnBacktestStrengthHeatmapItem | null,
  label: string,
) {
  if (!item) {
    return label;
  }

  return [
    `排名日: ${label}`,
    `参考日: ${formatDateLabel(item.ref_date)}`,
    `相对大盘: ${item.strength_label ?? "--"}`,
    `强弱分数: ${formatPercent(item.strength_score)}`,
    `Top均涨: ${formatPercent(item.top_avg_return_pct)}`,
    `大盘均涨: ${formatPercent(item.benchmark_return_pct)}`,
  ].join("\n");
}

function heatmapCellState(item: ReturnBacktestStrengthHeatmapItem | null) {
  if (!item) {
    return "is-empty";
  }
  if (item.strength_label === "强于大盘") {
    return "is-strong";
  }
  if (item.strength_label === "弱于大盘") {
    return "is-weak";
  }
  if (item.strength_label === "持平") {
    return "is-flat";
  }
  return "is-empty";
}

type ReturnBacktestStrengthPanelProps = {
  initialSourcePath?: string;
  initialHoldingDays?: string | number | null;
  initialTopLimit?: string | number | null;
  initialBoard?: string | null;
  title?: string;
  caption?: string;
  embedded?: boolean;
  showCloseButton?: boolean;
  onClose?: () => void;
  autoTitle?: boolean;
};

export function ReturnBacktestStrengthPanel({
  initialSourcePath,
  initialHoldingDays,
  initialTopLimit,
  initialBoard,
  title = "排名强弱格子图",
  caption = "以排名日后第 N 个交易日作为参考日，观察 Top 样本相对同期大盘的强弱变化。",
  embedded = false,
  showCloseButton = false,
  onClose,
  autoTitle = false,
}: ReturnBacktestStrengthPanelProps) {
  const [sourcePath, setSourcePath] = useState(
    () => initialSourcePath?.trim() || readStoredSourcePath(),
  );
  const [holdingDaysInput, setHoldingDaysInput] = useState(
    () => normalizePositiveInput(initialHoldingDays, DEFAULT_HOLDING_DAYS),
  );
  const [topLimitInput, setTopLimitInput] = useState(
    () => normalizePositiveInput(initialTopLimit, DEFAULT_TOP_LIMIT),
  );
  const [boardFilter, setBoardFilter] = useState<BoardOption>(
    () => normalizeBoardOption(initialBoard),
  );
  const [pageData, setPageData] = useState<ReturnBacktestStrengthOverviewData | null>(
    null,
  );
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [selectedDate, setSelectedDate] = useState<string | null>(null);
  const [tooltip, setTooltip] = useState<{
    left: number;
    top: number;
    placement: "top" | "bottom";
  } | null>(null);

  const items = pageData?.items ?? [];
  const calendarMonths = useMemo(() => buildCalendarMonths(items), [items]);
  const heatmapSlots = useMemo(
    () => calendarMonths.flatMap((month) => month.slots),
    [calendarMonths],
  );
  const selectedSlot = useMemo(
    () => heatmapSlots.find((slot) => slot.compactDate === selectedDate) ?? null,
    [heatmapSlots, selectedDate],
  );

  useEffect(() => {
    if (autoTitle) {
      document.title = title;
    }
  }, [autoTitle, title]);

  useEffect(() => {
    const nextSelectedDate = pickInitialHeatmapDate(
      heatmapSlots,
      pageData?.latest_rank_date,
    );
    setSelectedDate((current) => {
      if (current && heatmapSlots.some((slot) => slot.compactDate === current)) {
        return current;
      }
      return nextSelectedDate;
    });
  }, [heatmapSlots, pageData?.latest_rank_date]);

  useEffect(() => {
    const handlePointerDown = (event: PointerEvent) => {
      if (!(event.target instanceof Element)) {
        return;
      }
      if (event.target.closest(".return-strength-window-calendar-day")) {
        return;
      }
      setSelectedDate(null);
      setTooltip(null);
    };

    window.addEventListener("pointerdown", handlePointerDown);
    return () => {
      window.removeEventListener("pointerdown", handlePointerDown);
    };
  }, []);

  async function handleCompute() {
    const holdingDays = Number(holdingDaysInput);
    const topLimit = Number(topLimitInput);
    if (!Number.isInteger(holdingDays) || holdingDays <= 0) {
      setError("持仓天数必须是正整数");
      return;
    }
    if (!Number.isInteger(topLimit) || topLimit <= 0) {
      setError("Top 数量必须是正整数");
      return;
    }

    setLoading(true);
    setError("");
    try {
      let resolvedSourcePath = sourcePath.trim();
      if (!resolvedSourcePath) {
        resolvedSourcePath = await ensureManagedSourcePath();
        setSourcePath(resolvedSourcePath);
      }

      const data = await getReturnBacktestStrengthOverview({
        sourcePath: resolvedSourcePath,
        holdingDays,
        topLimit,
        board: boardFilter === "全部" ? undefined : boardFilter,
      });
      setPageData(data);
    } catch (loadError) {
      setPageData(null);
      setError(`读取强弱格子图失败: ${String(loadError)}`);
    } finally {
      setLoading(false);
    }
  }

  function openTooltip(slot: HeatmapSlot, target: HTMLButtonElement) {
    if (!slot.compactDate) {
      return;
    }
    const targetRect = target.getBoundingClientRect();
    const tooltipWidth = 240;
    const screenPadding = 12;
    const centerX = targetRect.left + targetRect.width / 2;
    const left = Math.min(
      Math.max(centerX, screenPadding + tooltipWidth / 2),
      window.innerWidth - screenPadding - tooltipWidth / 2,
    );
    const preferBottom = targetRect.top < 132;
    const top = preferBottom ? targetRect.bottom + 12 : targetRect.top - 12;

    setSelectedDate(slot.compactDate);
    setTooltip({
      left,
      top,
      placement: preferBottom ? "bottom" : "top",
    });
  }

  return (
    <div
      className={[
        "return-strength-window-page",
        embedded ? "is-embedded" : "",
      ]
        .filter(Boolean)
        .join(" ")}
    >
      <section className="return-strength-window-card">
        <div className="return-strength-window-head">
          <div>
            <h1 className="return-strength-window-title">{title}</h1>
            <p className="return-strength-window-caption">{caption}</p>
          </div>
          {showCloseButton && onClose ? (
            <button
              type="button"
              className="return-strength-window-close-btn"
              onClick={onClose}
              aria-label="关闭强弱格子图"
            >
              关闭
            </button>
          ) : null}
        </div>

        <div className="return-strength-window-form-grid">
          <label className="return-strength-window-field">
            <span>持仓天数</span>
            <input
              type="number"
              min={1}
              step={1}
              value={holdingDaysInput}
              onChange={(event) => setHoldingDaysInput(event.target.value)}
            />
          </label>

          <label className="return-strength-window-field">
            <span>Top 数量</span>
            <input
              type="number"
              min={1}
              step={1}
              value={topLimitInput}
              onChange={(event) => setTopLimitInput(event.target.value)}
            />
          </label>

          <label className="return-strength-window-field">
            <span>板块</span>
            <select
              value={boardFilter}
              onChange={(event) =>
                setBoardFilter(
                  event.target.value as (typeof STOCK_PICK_BOARD_OPTIONS)[number],
                )
              }
            >
              {STOCK_PICK_BOARD_OPTIONS.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </label>
        </div>

        <div className="return-strength-window-actions">
          <button
            type="button"
            className="return-strength-window-primary-btn"
            onClick={() => void handleCompute()}
            disabled={loading}
          >
            {loading ? "计算中..." : "计算格子图"}
          </button>
        </div>

        {error ? <div className="return-strength-window-error">{error}</div> : null}
      </section>

      {!pageData ? (
        <section className="return-strength-window-card">
          <div className="return-strength-window-empty">点击“计算格子图”后开始统计。</div>
        </section>
      ) : (
        <section className="return-strength-window-card">
          <div className="return-strength-window-summary-grid">
            <div className="return-strength-window-summary-item">
              <span>当前板块</span>
              <strong>{pageData.board ?? "--"}</strong>
            </div>
            <div className="return-strength-window-summary-item">
              <span>持仓天数</span>
              <strong>{formatInteger(pageData.holding_days)}</strong>
            </div>
            <div className="return-strength-window-summary-item">
              <span>Top 数量</span>
              <strong>{formatInteger(pageData.top_limit)}</strong>
            </div>
            <div className="return-strength-window-summary-item">
              <span>最新排名日</span>
              <strong>{formatDateLabel(pageData.latest_rank_date)}</strong>
            </div>
            <div className="return-strength-window-summary-item is-strong">
              <span>强于大盘</span>
              <strong>{formatInteger(pageData.strong_days)}</strong>
            </div>
            <div className="return-strength-window-summary-item is-weak">
              <span>弱于大盘</span>
              <strong>{formatInteger(pageData.weak_days)}</strong>
            </div>
            <div className="return-strength-window-summary-item is-flat">
              <span>持平</span>
              <strong>{formatInteger(pageData.flat_days)}</strong>
            </div>
          </div>

          {items.length === 0 ? (
            <div className="return-strength-window-empty">没有可计算的历史日期。</div>
          ) : (
            <div
              className="return-strength-window-calendar"
              onScroll={() => setTooltip(null)}
            >
              {calendarMonths.map((month) => (
                <section
                  key={month.key}
                  className="return-strength-window-calendar-month"
                >
                  <div className="return-strength-window-calendar-month-title">
                    {month.label}
                  </div>
                  <div className="return-strength-window-calendar-weekdays">
                    {WEEKDAY_LABELS.map((label) => (
                      <span key={`${month.key}-${label}`}>{label}</span>
                    ))}
                  </div>
                  <div className="return-strength-window-calendar-grid">
                    {month.slots.map((slot) => {
                      if (!slot.compactDate) {
                        return (
                          <div
                            key={slot.key}
                            className="return-strength-window-calendar-gap"
                            aria-hidden="true"
                          />
                        );
                      }

                      return (
                        <button
                          key={slot.key}
                          type="button"
                          className={[
                            "return-strength-window-calendar-day",
                            "return-strength-window-heatmap-cell",
                            heatmapCellState(slot.cell),
                            slot.compactDate === selectedDate && tooltip
                              ? "is-selected"
                              : "",
                          ]
                            .filter(Boolean)
                            .join(" ")}
                          title={buildHeatmapTitle(slot.cell, slot.label)}
                          aria-label={buildHeatmapTitle(slot.cell, slot.label)}
                          onClick={(event) => {
                            if (slot.compactDate === selectedDate && tooltip) {
                              setSelectedDate(null);
                              setTooltip(null);
                              return;
                            }
                            openTooltip(slot, event.currentTarget);
                          }}
                        >
                          <span>{slot.dayOfMonth}</span>
                        </button>
                      );
                    })}
                  </div>
                </section>
              ))}
              {selectedSlot?.cell && tooltip ? (
                <div
                  className={[
                    "return-strength-window-tooltip",
                    tooltip.placement === "bottom" ? "is-bottom" : "is-top",
                  ]
                    .filter(Boolean)
                    .join(" ")}
                  role="status"
                  aria-live="polite"
                  style={{
                    left: `${tooltip.left}px`,
                    top: `${tooltip.top}px`,
                  }}
                >
                  <div className="return-strength-window-tooltip-head">
                    <strong>{formatDateLabel(selectedSlot.cell.rank_date)}</strong>
                    <span>{selectedSlot.cell.strength_label ?? "--"}</span>
                  </div>
                  <div className="return-strength-window-tooltip-grid">
                    <span>参考日</span>
                    <strong>{formatDateLabel(selectedSlot.cell.ref_date)}</strong>
                    <span>强弱分数</span>
                    <strong>{formatPercent(selectedSlot.cell.strength_score)}</strong>
                    <span>Top均涨</span>
                    <strong>{formatPercent(selectedSlot.cell.top_avg_return_pct)}</strong>
                    <span>大盘均涨</span>
                    <strong>{formatPercent(selectedSlot.cell.benchmark_return_pct)}</strong>
                    <span>Top样本</span>
                    <strong>{formatInteger(selectedSlot.cell.valid_top_count)}</strong>
                    <span>大盘样本</span>
                    <strong>
                      {formatInteger(selectedSlot.cell.benchmark_sample_count)}
                    </strong>
                    <span>Top强/弱</span>
                    <strong>
                      {formatPercent(selectedSlot.cell.top_strong_hit_rate)} /{" "}
                      {formatPercent(selectedSlot.cell.top_weak_hit_rate)}
                    </strong>
                    <span>大盘强/弱</span>
                    <strong>
                      {formatPercent(selectedSlot.cell.benchmark_strong_hit_rate)} /{" "}
                      {formatPercent(selectedSlot.cell.benchmark_weak_hit_rate)}
                    </strong>
                  </div>
                </div>
              ) : null}
            </div>
          )}
        </section>
      )}
    </div>
  );
}

export default function ReturnBacktestStrengthWindow() {
  const [searchParams] = useSearchParams();

  return (
    <ReturnBacktestStrengthPanel
      initialSourcePath={searchParams.get("sourcePath") ?? undefined}
      initialHoldingDays={searchParams.get("holdingDays") ?? undefined}
      initialTopLimit={searchParams.get("topLimit") ?? undefined}
      initialBoard={searchParams.get("board") ?? undefined}
      autoTitle
    />
  );
}
