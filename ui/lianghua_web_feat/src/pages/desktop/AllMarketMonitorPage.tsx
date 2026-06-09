import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import {
  getAllMarketMonitorSnapshot,
  type AllMarketMonitorRow,
} from "../../apis/reader";
import DetailsLink from "../../shared/DetailsLink";
import {
  formatConceptText,
  useConceptExclusions,
} from "../../shared/conceptExclusions";
import { STOCK_PICK_BOARD_OPTIONS } from "../../shared/stockPickShared";
import {
  TableSortButton,
  getAriaSort,
  type SortDefinition,
  type SortDirection,
  sortRows,
} from "../../shared/tableSort";
import "./css/AllMarketMonitorPage.css";

const POLL_INTERVAL_MS = 1000;
const HISTORY_KEEP_MS = 90_000;
const SPEED_PERIOD_OPTIONS = [10, 30, 60] as const;
const TOP_LIMIT_OPTIONS = [20, 50, 100, 200] as const;

type PrimarySortKey = "realtime_change_pct" | "speed_pct";
type SpeedPeriod = (typeof SPEED_PERIOD_OPTIONS)[number];
type BoardFilter = (typeof STOCK_PICK_BOARD_OPTIONS)[number];
type TopLimit = (typeof TOP_LIMIT_OPTIONS)[number];
type SortKey =
  | "best_rank_3d"
  | "best_rank_5d"
  | "realtime_change_pct"
  | "speed_pct"
  | "realtime_change_open_pct"
  | "realtime_price"
  | "total_mv_yi";

type PriceSnapshot = {
  capturedAt: number;
  prices: Record<string, number>;
};

type DisplayRow = AllMarketMonitorRow & {
  speed_pct?: number | null;
};

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function formatNumber(value?: number | null, digits = 2) {
  if (!isFiniteNumber(value)) return "--";
  return Number.isInteger(value) ? String(value) : value.toFixed(digits);
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
    const previousPrice = baseline.prices[row.ts_code];
    if (
      isFiniteNumber(currentPrice) &&
      currentPrice > 0 &&
      isFiniteNumber(previousPrice) &&
      previousPrice > 0
    ) {
      out.set(row.ts_code, (currentPrice / previousPrice - 1) * 100);
    }
  }
  return out;
}

export default function AllMarketMonitorPage() {
  const { excludedConcepts } = useConceptExclusions();
  const [sourcePath, setSourcePath] = useState("");
  const [enabled, setEnabled] = useState(false);
  const [rows, setRows] = useState<AllMarketMonitorRow[]>([]);
  const [primarySortKey, setPrimarySortKey] =
    useState<PrimarySortKey>("realtime_change_pct");
  const [speedPeriod, setSpeedPeriod] = useState<SpeedPeriod>(10);
  const [boardFilter, setBoardFilter] = useState<BoardFilter>("全部");
  const [topLimit, setTopLimit] = useState<TopLimit>(50);
  const [sortKey, setSortKey] = useState<SortKey | null>(null);
  const [sortDirection, setSortDirection] = useState<SortDirection>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [refreshedAt, setRefreshedAt] = useState("");
  const [rankDate, setRankDate] = useState("");
  const [requestedCount, setRequestedCount] = useState(0);
  const [fetchedCount, setFetchedCount] = useState(0);
  const [currentTime, setCurrentTime] = useState(() => new Date());

  const inFlightRef = useRef(false);
  const enabledRef = useRef(false);
  const historyRef = useRef<PriceSnapshot[]>([]);

  const sourcePathTrimmed = sourcePath.trim();

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
      const result = await getAllMarketMonitorSnapshot(sourcePathTrimmed);
      if (!enabledRef.current) return;

      const capturedAt = Date.now();
      const nextRows = result.rows ?? [];
      historyRef.current = appendPriceSnapshot(
        historyRef.current,
        nextRows,
        capturedAt,
      );
      setRows(nextRows);
      setRefreshedAt(result.refreshed_at ?? "");
      setRankDate(result.rank_date ?? "");
      setRequestedCount(result.requested_count ?? 0);
      setFetchedCount(result.fetched_count ?? 0);
      setError("");
    } catch (runError) {
      if (enabledRef.current) {
        setError(`全市场刷新失败: ${String(runError)}`);
      }
    } finally {
      inFlightRef.current = false;
      if (enabledRef.current) {
        setLoading(false);
      }
    }
  }, [sourcePathTrimmed]);

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

  const speedMap = useMemo(
    () => buildSpeedMap(rows, historyRef.current, speedPeriod, Date.now()),
    [rows, speedPeriod],
  );

  const displayRows = useMemo<DisplayRow[]>(() => {
    const filteredRows = rows
      .filter((row) => boardFilter === "全部" || row.board === boardFilter)
      .map((row) => ({
        ...row,
        speed_pct: speedMap.get(row.ts_code) ?? null,
      }));

    const effectiveSortKey = sortKey ?? primarySortKey;
    const effectiveSortDirection = sortDirection ?? "desc";
    const sortDefinitions = {
      best_rank_3d: { value: (row: DisplayRow) => row.best_rank_3d },
      best_rank_5d: { value: (row: DisplayRow) => row.best_rank_5d },
      realtime_change_pct: {
        value: (row: DisplayRow) => row.realtime_change_pct,
      },
      speed_pct: { value: (row: DisplayRow) => row.speed_pct },
      realtime_change_open_pct: {
        value: (row: DisplayRow) => row.realtime_change_open_pct,
      },
      realtime_price: { value: (row: DisplayRow) => row.realtime_price },
      total_mv_yi: { value: (row: DisplayRow) => row.total_mv_yi },
    } satisfies Partial<Record<SortKey, SortDefinition<DisplayRow>>>;

    return sortRows(
      filteredRows,
      effectiveSortKey,
      effectiveSortDirection,
      sortDefinitions,
    ).slice(0, topLimit);
  }, [boardFilter, primarySortKey, rows, sortDirection, sortKey, speedMap, topLimit]);

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
            <h2 className="all-market-title">全市场监控</h2>
            <div className="all-market-status">
              <span>{statusText}</span>
              <span>行情 {fetchedCount}/{requestedCount}</span>
              {rankDate ? <span>排名 {rankDate}</span> : null}
            </div>
          </div>

          <button
            type="button"
            className={enabled ? "all-market-toggle is-active" : "all-market-toggle"}
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
                <span>涨幅</span>
                <strong>从高到低</strong>
              </button>
              <button
                type="button"
                className={primarySortKey === "speed_pct" ? "is-active" : ""}
                onClick={() => setPrimarySort("speed_pct")}
              >
                <span>涨速</span>
                <strong>从高到低</strong>
              </button>
            </div>
          </div>

          <div className="all-market-config-controls">
            <span className="all-market-control-label">参数</span>
            <label className="all-market-field">
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

            <label className="all-market-field">
              <span>Top N</span>
              <select
                value={topLimit}
                onChange={(event) =>
                  setTopLimit(Number(event.target.value) as TopLimit)
                }
              >
                {TOP_LIMIT_OPTIONS.map((value) => (
                  <option key={value} value={value}>
                    {value}
                  </option>
                ))}
              </select>
            </label>
          </div>
        </div>

        <div className="all-market-board-tabs" role="group" aria-label="板块">
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

        {error ? <div className="all-market-error">{error}</div> : null}
      </section>

      <section className="all-market-card all-market-table-card">
        <div className="all-market-table-head">
          <h3>全市场行情</h3>
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
          <table className="all-market-table">
            <thead>
              <tr>
                <th aria-sort="none">代码</th>
                <th aria-sort="none">名称</th>
                <th
                  aria-sort={getAriaSort(
                    sortKey === "best_rank_3d",
                    sortDirection,
                  )}
                >
                  {renderSortHeader("3日优", "best_rank_3d")}
                </th>
                <th
                  aria-sort={getAriaSort(
                    sortKey === "best_rank_5d",
                    sortDirection,
                  )}
                >
                  {renderSortHeader("5日优", "best_rank_5d")}
                </th>
                <th
                  aria-sort={getAriaSort(
                    sortKey === "realtime_change_pct",
                    sortDirection,
                  )}
                >
                  {renderSortHeader("涨幅", "realtime_change_pct")}
                </th>
                <th
                  aria-sort={getAriaSort(sortKey === "speed_pct", sortDirection)}
                >
                  {renderSortHeader("涨速", "speed_pct")}
                </th>
                <th
                  aria-sort={getAriaSort(
                    sortKey === "realtime_price",
                    sortDirection,
                  )}
                >
                  {renderSortHeader("现价", "realtime_price")}
                </th>
                <th
                  aria-sort={getAriaSort(
                    sortKey === "realtime_change_open_pct",
                    sortDirection,
                  )}
                >
                  {renderSortHeader("开盘涨幅", "realtime_change_open_pct")}
                </th>
                <th aria-sort="none">概念</th>
                <th
                  aria-sort={getAriaSort(
                    sortKey === "total_mv_yi",
                    sortDirection,
                  )}
                >
                  {renderSortHeader("总市值", "total_mv_yi")}
                </th>
              </tr>
            </thead>
            <tbody>
              {displayRows.length > 0 ? (
                displayRows.map((row) => {
                  const conceptText = formatConceptText(
                    row.concept ?? "",
                    excludedConcepts,
                  );

                  return (
                    <tr key={row.ts_code}>
                      <td>{row.ts_code}</td>
                      <td>
                        <DetailsLink
                          tsCode={row.ts_code}
                          tradeDate={
                            rankDate || row.realtime_trade_date || undefined
                          }
                          sourcePath={sourcePathTrimmed || undefined}
                          className="all-market-stock-link"
                          title={`查看 ${row.name || row.ts_code} 详情`}
                          navigationItems={navigationItems}
                        >
                          {row.name || "--"}
                        </DetailsLink>
                      </td>
                      <td className="all-market-rank-cell">
                        {formatNumber(row.best_rank_3d, 0)}
                      </td>
                      <td className="all-market-rank-cell">
                        {formatNumber(row.best_rank_5d, 0)}
                      </td>
                      <td
                        className={getPercentClassName(row.realtime_change_pct)}
                      >
                        {formatPercent(row.realtime_change_pct)}
                      </td>
                      <td className={getPercentClassName(row.speed_pct)}>
                        {formatPercent(row.speed_pct)}
                      </td>
                      <td>{formatNumber(row.realtime_price)}</td>
                      <td
                        className={getPercentClassName(
                          row.realtime_change_open_pct,
                        )}
                      >
                        {formatPercent(row.realtime_change_open_pct)}
                      </td>
                      <td className="all-market-concept-cell" title={conceptText}>
                        {conceptText}
                      </td>
                      <td>{formatNumber(row.total_mv_yi)}</td>
                    </tr>
                  );
                })
              ) : (
                <tr>
                  <td colSpan={10} className="all-market-empty">
                    {enabled ? "等待全市场行情返回。" : "开启爬虫后开始刷新。"}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
