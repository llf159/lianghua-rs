import { useMemo, useState } from "react";
import type {
  TencentIntradayData,
  TencentIntradayPoint,
} from "../../../apis/details";

const VIEWBOX_WIDTH = 1120;
const VIEWBOX_HEIGHT = 440;
const PLOT_LEFT = 26;
const PLOT_RIGHT = 66;
const PRICE_TOP = 22;
const PRICE_BOTTOM = 288;
const VOLUME_TOP = 330;
const VOLUME_BOTTOM = 410;
const SESSION_SLOT_COUNT = 242;
const PRICE_GRID_COUNT = 4;
const EMPTY_INTRADAY_POINTS: TencentIntradayPoint[] = [];

type IntradayChartProps = {
  data: TencentIntradayData | null;
  loading: boolean;
  error: string;
  canRefresh: boolean;
  expanded: boolean;
  onToggle: () => void;
  onRefresh: () => void;
};

type PriceScaleMode = "dynamic" | "limit";

type IntradayDisplaySummary = {
  name: string;
  latestPrice: number | null;
  preClose: number | null;
  open: number | null;
  high: number | null;
  low: number | null;
  upperLimit: number | null;
  lowerLimit: number | null;
  changePct: number | null;
  averagePrice: number | null;
  totalVol: number | null;
  totalAmount: number | null;
  refreshedAt: string;
};

function finiteOrNull(value: unknown) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function formatNumber(value: number | null, digits = 2) {
  if (value === null) return "--";
  return value.toLocaleString("zh-CN", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function formatCompact(value: number | null, unit: string) {
  if (value === null) return "--";
  if (Math.abs(value) >= 100_000_000) {
    return `${formatNumber(value / 100_000_000)} 亿${unit}`;
  }
  if (Math.abs(value) >= 10_000) {
    return `${formatNumber(value / 10_000)} 万${unit}`;
  }
  return `${formatNumber(value, 0)} ${unit}`;
}

function formatTradeDate(value: string | undefined) {
  const digits = value?.replace(/\D/g, "") ?? "";
  return digits.length === 8
    ? `${digits.slice(0, 4)}-${digits.slice(4, 6)}-${digits.slice(6, 8)}`
    : value || "--";
}

function formatChangePct(value: number | null) {
  if (value === null) return "--";
  return `${value > 0 ? "+" : ""}${formatNumber(value)}%`;
}

function formatSignedNumber(value: number | null) {
  if (value === null) return "--";
  return `${value > 0 ? "+" : ""}${formatNumber(value)}`;
}

function timeToSlot(time: string) {
  const [hourRaw, minuteRaw] = time.split(":");
  const hour = Number(hourRaw);
  const minute = Number(minuteRaw);
  if (!Number.isInteger(hour) || !Number.isInteger(minute)) return null;

  const totalMinutes = hour * 60 + minute;
  if (totalMinutes >= 570 && totalMinutes <= 690) {
    return totalMinutes - 570;
  }
  if (totalMinutes >= 780 && totalMinutes <= 900) {
    return 121 + totalMinutes - 780;
  }
  return null;
}

function resolvePointSlot(point: TencentIntradayPoint, index: number) {
  return timeToSlot(point.time) ?? Math.min(index, SESSION_SLOT_COUNT - 1);
}

function buildDisplaySummary(data: TencentIntradayData): IntradayDisplaySummary {
  const points = data.points;
  const first = points[0];
  const last = points[points.length - 1];
  const summary = data.summary;
  const pointPrices = points
    .map((point) => point.price)
    .filter((value) => Number.isFinite(value));

  return {
    name: summary?.name?.trim() || "分时行情",
    latestPrice: finiteOrNull(summary?.latest_price ?? last?.price),
    preClose: finiteOrNull(summary?.pre_close),
    open: finiteOrNull(summary?.open ?? first?.price),
    high: finiteOrNull(
      summary?.high ?? (pointPrices.length ? Math.max(...pointPrices) : null),
    ),
    low: finiteOrNull(
      summary?.low ?? (pointPrices.length ? Math.min(...pointPrices) : null),
    ),
    upperLimit: finiteOrNull(summary?.upper_limit),
    lowerLimit: finiteOrNull(summary?.lower_limit),
    changePct: finiteOrNull(summary?.change_pct),
    averagePrice: finiteOrNull(summary?.average_price ?? last?.average_price),
    totalVol: finiteOrNull(summary?.total_vol ?? last?.cumulative_vol),
    totalAmount: finiteOrNull(summary?.total_amount ?? last?.cumulative_amount),
    refreshedAt: summary?.refreshed_at?.trim() || last?.time || "--",
  };
}

function buildPriceDomain(
  points: TencentIntradayPoint[],
  preClose: number | null,
  upperLimit: number | null,
  lowerLimit: number | null,
  scaleMode: PriceScaleMode,
) {
  if (
    scaleMode === "limit" &&
    upperLimit !== null &&
    lowerLimit !== null &&
    upperLimit > lowerLimit
  ) {
    return { min: lowerLimit, max: upperLimit };
  }

  const values = points.flatMap((point) =>
    point.average_price === null || point.average_price === undefined
      ? [point.price]
      : [point.price, point.average_price],
  );
  if (preClose !== null && preClose > 0) values.push(preClose);

  const finiteValues = values.filter(Number.isFinite);
  if (finiteValues.length === 0) return { min: 0, max: 1 };

  let min = Math.min(...finiteValues);
  let max = Math.max(...finiteValues);
  if (preClose !== null && preClose > 0) {
    const distance = Math.max(Math.abs(max - preClose), Math.abs(preClose - min));
    const padding = Math.max(distance * 1.08, preClose * 0.002, 0.01);
    min = preClose - padding;
    max = preClose + padding;
  } else {
    const padding = Math.max((max - min) * 0.08, max * 0.002, 0.01);
    min -= padding;
    max += padding;
  }
  return { min, max };
}

export default function IntradayChart({
  data,
  loading,
  error,
  canRefresh,
  expanded,
  onToggle,
  onRefresh,
}: IntradayChartProps) {
  const currentTsCode = data?.ts_code ?? null;
  const [priceScaleSelection, setPriceScaleSelection] = useState<{
    tsCode: string | null;
    mode: PriceScaleMode;
  }>({ tsCode: null, mode: "dynamic" });
  const priceScaleMode =
    priceScaleSelection.tsCode === currentTsCode
      ? priceScaleSelection.mode
      : "dynamic";
  const points = data?.points ?? EMPTY_INTRADAY_POINTS;
  const summary = useMemo(() => (data ? buildDisplaySummary(data) : null), [data]);
  const plotWidth = VIEWBOX_WIDTH - PLOT_LEFT - PLOT_RIGHT;
  const slotWidth = plotWidth / (SESSION_SLOT_COUNT - 1);
  const hasLimitScale =
    summary?.upperLimit !== null &&
    summary?.upperLimit !== undefined &&
    summary?.lowerLimit !== null &&
    summary?.lowerLimit !== undefined &&
    summary.upperLimit > summary.lowerLimit;
  const priceDomain = useMemo(
    () =>
      buildPriceDomain(
        points,
        summary?.preClose ?? null,
        summary?.upperLimit ?? null,
        summary?.lowerLimit ?? null,
        priceScaleMode,
      ),
    [
      points,
      priceScaleMode,
      summary?.lowerLimit,
      summary?.preClose,
      summary?.upperLimit,
    ],
  );
  const priceRange = Math.max(priceDomain.max - priceDomain.min, Number.EPSILON);
  const maxVolume = Math.max(...points.map((point) => point.vol), 1);
  const xForPoint = (point: TencentIntradayPoint, index: number) =>
    PLOT_LEFT + resolvePointSlot(point, index) * slotWidth;
  const yForPrice = (price: number) =>
    PRICE_BOTTOM -
    ((price - priceDomain.min) / priceRange) * (PRICE_BOTTOM - PRICE_TOP);
  const priceLine = points
    .map((point, index) => `${xForPoint(point, index)},${yForPrice(point.price)}`)
    .join(" ");
  const averageLine = points
    .map((point, index) =>
      point.average_price === null || point.average_price === undefined
        ? null
        : `${xForPoint(point, index)},${yForPrice(point.average_price)}`,
    )
    .filter((value): value is string => value !== null)
    .join(" ");
  const latestPoint = points[points.length - 1];
  const latestChange =
    summary?.latestPrice !== null &&
    summary?.latestPrice !== undefined &&
    summary?.preClose !== null &&
    summary?.preClose !== undefined
      ? summary.latestPrice - summary.preClose
      : null;
  const latestChangePct =
    summary?.changePct ??
    (latestPoint && summary?.preClose !== null && summary?.preClose !== undefined && summary.preClose > 0
      ? (latestPoint.price / summary.preClose - 1) * 100
      : null);
  const changeClass =
    (summary?.changePct ?? 0) > 0
      ? "is-up"
      : (summary?.changePct ?? 0) < 0
        ? "is-down"
        : "is-flat";
  const timeTicks = [
    { slot: 0, label: "09:30" },
    { slot: 60, label: "10:30" },
    { slot: 120.5, label: "11:30 / 13:00" },
    { slot: 181, label: "14:00" },
    { slot: 241, label: "15:00" },
  ];
  const refreshButton = (
    <button
      className={`details-intraday-refresh ${loading ? "is-loading" : ""}`}
      type="button"
      disabled={loading || !canRefresh}
      onClick={onRefresh}
      aria-label={loading ? "正在刷新分时行情" : "刷新分时行情"}
      title={loading ? "正在刷新..." : "刷新分时行情"}
    >
      <svg viewBox="0 0 20 20" aria-hidden="true">
        <path d="M16.4 6.1A7 7 0 1 0 17 11" />
        <path d="M16.4 2.8v3.7h-3.7" />
      </svg>
      <span>{loading ? "刷新中" : "刷新"}</span>
    </button>
  );

  return (
    <section className="details-card details-intraday-card">
      <div className="details-intraday-head">
        <div>
          <h3 className="details-subtitle">分时图</h3>
          <span className="details-intraday-source">腾讯 · 当日分钟行情</span>
        </div>
        <div className="details-intraday-head-actions" role="group" aria-label="分时图操作">
          {expanded && points.length === 0 ? refreshButton : null}
          <button
            className="details-intraday-toggle"
            type="button"
            aria-expanded={expanded}
            onClick={onToggle}
          >
            <span>{expanded ? "收起" : "展开"}</span>
            <svg viewBox="0 0 16 16" aria-hidden="true">
              <path d={expanded ? "M3.5 10.5 8 6l4.5 4.5" : "M3.5 5.5 8 10l4.5-4.5"} />
            </svg>
          </button>
        </div>
      </div>

      {expanded && summary && data ? (
        <>
          <div className="details-intraday-primary">
            <div>
              <strong>{summary.name}</strong>
              <span>{data.ts_code} · {formatTradeDate(data.trade_date)}</span>
            </div>
            <div className={`details-intraday-last ${changeClass}`}>
              <strong>{formatNumber(summary.latestPrice)}</strong>
              <span>
                {formatSignedNumber(latestChange)}&nbsp;&nbsp;
                {formatChangePct(summary.changePct)}
              </span>
            </div>
            <span className="details-intraday-updated">更新 {summary.refreshedAt}</span>
          </div>

          <div className="details-intraday-meta">
            <span>今开 <strong>{formatNumber(summary.open)}</strong></span>
            <span>最高 <strong>{formatNumber(summary.high)}</strong></span>
            <span>最低 <strong>{formatNumber(summary.low)}</strong></span>
            <span>昨收 <strong>{formatNumber(summary.preClose)}</strong></span>
            <span>均价 <strong>{formatNumber(summary.averagePrice)}</strong></span>
            <span>成交量 <strong>{formatCompact(summary.totalVol, "手")}</strong></span>
            <span>成交额 <strong>{formatCompact(summary.totalAmount, "元")}</strong></span>
          </div>
        </>
      ) : null}

      {expanded && error ? <div className="details-error details-intraday-error">{error}</div> : null}
      {expanded && !error && loading && points.length === 0 ? (
        <div className="details-intraday-empty">正在读取腾讯分时行情...</div>
      ) : null}
      {expanded && !error && !loading && data && points.length === 0 ? (
        <div className="details-intraday-empty">当前接口未返回分钟数据</div>
      ) : null}
      {expanded && !error && !loading && !data ? (
        <div className="details-intraday-empty">请选择股票查看当日分时</div>
      ) : null}

      {expanded && points.length > 0 ? (
        <div className="details-intraday-chart-wrap">
          <div className="details-intraday-legend">
            <div className="details-intraday-legend-series">
              <span><i className="is-price" />价格</span>
              <span><i className="is-average" />均价</span>
              <span><i className="is-volume" />分钟成交量</span>
            </div>
            <div className="details-intraday-chart-actions" role="group" aria-label="分时图表操作">
              <button
                className={`details-intraday-scale-toggle ${priceScaleMode === "limit" ? "is-limit" : ""}`}
                type="button"
                disabled={!hasLimitScale}
                aria-label={
                  hasLimitScale
                    ? `当前为${priceScaleMode === "dynamic" ? "动态高度" : "涨跌停高度"}，点击切换`
                    : "当前行情未返回涨跌停价格，只能使用动态高度"
                }
                title={
                  hasLimitScale
                    ? `切换为${priceScaleMode === "dynamic" ? "涨跌停高度" : "动态高度"}`
                    : "当前行情未返回涨跌停价格"
                }
                onClick={() =>
                  setPriceScaleSelection({
                    tsCode: currentTsCode,
                    mode: priceScaleMode === "dynamic" ? "limit" : "dynamic",
                  })
                }
              >
                <svg viewBox="0 0 16 16" aria-hidden="true">
                  <path d="M3 5h9l-2-2" />
                  <path d="m12 11H3l2 2" />
                </svg>
                <span>{priceScaleMode === "dynamic" ? "动态高度" : "涨跌停高度"}</span>
              </button>
              {refreshButton}
            </div>
          </div>
          <svg
            className="details-intraday-svg"
            viewBox={`0 0 ${VIEWBOX_WIDTH} ${VIEWBOX_HEIGHT}`}
            role="img"
            aria-label={`${data?.ts_code ?? "股票"}当日分时走势图`}
          >
            <rect
              x={PLOT_LEFT}
              y={PRICE_TOP}
              width={plotWidth}
              height={PRICE_BOTTOM - PRICE_TOP}
              className="details-intraday-plot-bg"
            />
            {Array.from({ length: PRICE_GRID_COUNT + 1 }, (_, index) => {
              const ratio = index / PRICE_GRID_COUNT;
              const y = PRICE_TOP + ratio * (PRICE_BOTTOM - PRICE_TOP);
              const price = priceDomain.max - ratio * priceRange;
              const changePct =
                summary?.preClose !== null &&
                summary?.preClose !== undefined &&
                summary.preClose > 0
                  ? (price / summary.preClose - 1) * 100
                  : null;
              return (
                <g key={`price-grid-${index}`}>
                  <line x1={PLOT_LEFT} x2={VIEWBOX_WIDTH - PLOT_RIGHT} y1={y} y2={y} className="details-intraday-grid" />
                  {changePct !== null && (index === 0 || index === PRICE_GRID_COUNT) ? (
                    <text
                      x={VIEWBOX_WIDTH - PLOT_RIGHT + 8}
                      y={y + 4}
                      className={
                        changePct > 0
                          ? "details-intraday-change-label is-up"
                          : changePct < 0
                            ? "details-intraday-change-label is-down"
                            : "details-intraday-change-label is-flat"
                      }
                    >
                      {formatChangePct(changePct)}
                    </text>
                  ) : null}
                  <text
                    x={VIEWBOX_WIDTH - PLOT_RIGHT - 8}
                    y={y + 4}
                    textAnchor="end"
                    className={
                      changePct !== null && changePct > 0
                        ? "details-intraday-axis-label details-intraday-price-label is-up"
                        : changePct !== null && changePct < 0
                          ? "details-intraday-axis-label details-intraday-price-label is-down"
                          : "details-intraday-axis-label details-intraday-price-label"
                    }
                  >
                    {formatNumber(price)}
                  </text>
                </g>
              );
            })}
            {timeTicks.map((tick) => {
              const x = PLOT_LEFT + tick.slot * slotWidth;
              return (
                <g key={tick.label}>
                  <line x1={x} x2={x} y1={PRICE_TOP} y2={VOLUME_BOTTOM} className="details-intraday-grid is-vertical" />
                  <text x={x} y={VIEWBOX_HEIGHT - 8} textAnchor="middle" className="details-intraday-axis-label">
                    {tick.label}
                  </text>
                </g>
              );
            })}
            {summary?.preClose !== null &&
            summary?.preClose !== undefined &&
            summary.preClose > 0 ? (
              <line
                x1={PLOT_LEFT}
                x2={VIEWBOX_WIDTH - PLOT_RIGHT}
                y1={yForPrice(summary.preClose)}
                y2={yForPrice(summary.preClose)}
                className="details-intraday-preclose"
              />
            ) : null}
            <polyline points={priceLine} className="details-intraday-price-line" />
            {averageLine ? <polyline points={averageLine} className="details-intraday-average-line" /> : null}
            {latestPoint ? (
              <>
                <line
                  x1={PLOT_LEFT}
                  x2={VIEWBOX_WIDTH - PLOT_RIGHT}
                  y1={yForPrice(latestPoint.price)}
                  y2={yForPrice(latestPoint.price)}
                  className={`details-intraday-current-line ${changeClass}`}
                />
                <circle
                  cx={xForPoint(latestPoint, points.length - 1)}
                  cy={yForPrice(latestPoint.price)}
                  r={2.8}
                  className="details-intraday-latest-dot"
                />
                {latestChangePct !== null ? (
                  <g className={`details-intraday-current-badge ${changeClass}`}>
                    <rect
                      x={VIEWBOX_WIDTH - PLOT_RIGHT + 4}
                      y={yForPrice(latestPoint.price) - 9}
                      width={50}
                      height={18}
                      rx={3}
                    />
                    <text
                      x={VIEWBOX_WIDTH - PLOT_RIGHT + 29}
                      y={yForPrice(latestPoint.price) + 3.5}
                      textAnchor="middle"
                    >
                      {formatChangePct(latestChangePct)}
                    </text>
                  </g>
                ) : null}
              </>
            ) : null}
            <line
              x1={PLOT_LEFT}
              x2={VIEWBOX_WIDTH - PLOT_RIGHT}
              y1={VOLUME_TOP}
              y2={VOLUME_TOP}
              className="details-intraday-grid details-intraday-section-divider"
            />
            <text
              x={PLOT_LEFT}
              y={VOLUME_TOP - 9}
              className="details-intraday-volume-label"
            >
              分时量
            </text>
            {points.map((point, index) => {
              const previousPrice = points[index - 1]?.price ?? summary?.preClose ?? point.price;
              const height = Math.max((point.vol / maxVolume) * (VOLUME_BOTTOM - VOLUME_TOP), 1);
              return (
                <rect
                  key={`${point.time}-${index}`}
                  x={xForPoint(point, index) - Math.max(slotWidth * 0.32, 0.5)}
                  y={VOLUME_BOTTOM - height}
                  width={Math.max(slotWidth * 0.64, 1)}
                  height={height}
                  className={point.price >= previousPrice ? "details-intraday-volume is-up" : "details-intraday-volume is-down"}
                />
              );
            })}
          </svg>
        </div>
      ) : null}
    </section>
  );
}
