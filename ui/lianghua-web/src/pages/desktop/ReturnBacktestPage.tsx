import {
  useEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
} from "react";
import { createPortal } from "react-dom";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import {
  getReturnBacktestPage,
  type ReturnBacktestBucket,
  type ReturnBacktestPageData,
  type ReturnBacktestRow,
} from "../../apis/returnBacktest";
import { listRankTradeDates } from "../../apis/reader";
import DetailsLink from "../../shared/DetailsLink";
import {
  DEFAULT_DATE_OPTION,
  normalizeTradeDates,
} from "../../shared/tradeDate";
import {
  readJsonStorage,
  readStoredSourcePath,
  writeJsonStorage,
} from "../../shared/storage";
import { useRouteScrollRegion } from "../../shared/routeScroll";
import {
  TableSortButton,
  getAriaSort,
  type SortDefinition,
  useTableSort,
} from "../../shared/tableSort";
import { ReturnBacktestStrengthPanel } from "../popup/ReturnBacktestStrengthWindow";
import "./css/ReturnBacktestPage.css";

const RETURN_BACKTEST_STATE_KEY = "lh_return_backtest_page_v1";
const BOARD_OPTIONS = ["全部", "主板", "创业/科创", "北交所"] as const;
const DEFAULT_TOP_LIMIT = "100";
const DEFAULT_HEATMAP_HOLDING_DAYS = 5;

type PersistedReturnBacktestState = {
  sourcePath: string;
  dateOptions: string[];
  rankDateInput: string;
  refDateInput: string;
  topLimitInput: string;
  boardFilter: (typeof BOARD_OPTIONS)[number];
  pageData: ReturnBacktestPageData | null;
  submittedQuery: SubmittedBacktestQuery | null;
};

type ReturnTableSortKey =
  | "rank"
  | "best_rank"
  | "return_pct"
  | "excess_return_pct";
type ReturnTableVariant = "rank" | "benchmark";
type SubmittedBacktestQuery = {
  sourcePath: string;
  rankDate: string;
  refDate: string;
  topLimit: number;
  board?: string;
};

type StrengthWindowConfig = {
  sourcePath: string;
  holdingDays: number;
  topLimit: number;
  board: (typeof BOARD_OPTIONS)[number];
  instanceKey: number;
};

function formatDateLabel(value?: string | null) {
  if (!value) {
    return "--";
  }
  if (value.length === 8) {
    return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`;
  }
  return value;
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
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function formatRatioPercent(value: number, total: number) {
  if (!Number.isFinite(value) || !Number.isFinite(total) || total <= 0) {
    return "--";
  }
  return `${((value / total) * 100).toFixed(1)}%`;
}

function strengthClassName(value?: string | null) {
  if (value === "强于大盘") {
    return "return-backtest-strength-strong";
  }
  if (value === "弱于大盘") {
    return "return-backtest-strength-weak";
  }
  return "return-backtest-strength-flat";
}

function pickRankDate(currentValue: string, options: string[]) {
  if (options.length === 0) {
    return DEFAULT_DATE_OPTION;
  }
  if (currentValue && options.includes(currentValue)) {
    return currentValue;
  }
  return options[1] ?? options[0];
}

function pickRefDate(currentValue: string, options: string[], rankDate: string) {
  if (options.length === 0) {
    return DEFAULT_DATE_OPTION;
  }
  if (currentValue && options.includes(currentValue) && currentValue > rankDate) {
    return currentValue;
  }
  const fallback = options.find((value) => value > rankDate);
  return fallback ?? options[0];
}

function parsePositiveInt(value: string, fallback: number) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function inferHoldingDays(rankDate: string, refDate: string, dateOptions: string[]) {
  if (!rankDate || !refDate) {
    return DEFAULT_HEATMAP_HOLDING_DAYS;
  }
  const ascendingDates = [...dateOptions].sort((left, right) =>
    left.localeCompare(right),
  );
  const rankIndex = ascendingDates.findIndex((item) => item === rankDate);
  const refIndex = ascendingDates.findIndex((item) => item === refDate);
  if (rankIndex < 0 || refIndex <= rankIndex) {
    return DEFAULT_HEATMAP_HOLDING_DAYS;
  }
  return refIndex - rankIndex;
}

function DistributionChart({
  title,
  subtitle,
  buckets,
}: {
  title: string;
  subtitle: string;
  buckets: ReturnBacktestBucket[];
}) {
  const maxCount = Math.max(...buckets.map((item) => item.count), 1);
  const totalCount = buckets.reduce((sum, item) => sum + item.count, 0);

  return (
    <section className="return-backtest-panel-card">
      <div className="return-backtest-panel-head">
        <div>
          <h3>{title}</h3>
          <p>{subtitle}</p>
        </div>
      </div>
      <div className="return-backtest-chart">
        {buckets.map((bucket) => {
          const heightPercent = (bucket.count / maxCount) * 100;
          return (
            <div className="return-backtest-chart-bar-wrap" key={bucket.label}>
              <div className="return-backtest-chart-value">
                <strong>{bucket.count}</strong>
                <small>{formatRatioPercent(bucket.count, totalCount)}</small>
              </div>
              <div className="return-backtest-chart-bar-track">
                <div
                  className="return-backtest-chart-bar"
                  style={{ height: `${heightPercent}%` }}
                  title={`${bucket.label}: ${bucket.count}`}
                />
              </div>
              <span className="return-backtest-chart-label">{bucket.label}</span>
            </div>
          );
        })}
      </div>
    </section>
  );
}

function SummaryHint({
  id,
  activeId,
  onToggle,
  text,
}: {
  id: string;
  activeId: string | null;
  onToggle: (id: string) => void;
  text: string;
}) {
  const [isHovered, setIsHovered] = useState(false);
  const isOpen = activeId === id || isHovered;
  const rootRef = useRef<HTMLSpanElement | null>(null);
  const [bubbleStyle, setBubbleStyle] = useState<CSSProperties | null>(null);

  const updateBubblePosition = () => {
    if (typeof window === "undefined" || !rootRef.current) {
      return;
    }
    const rect = rootRef.current.getBoundingClientRect();
    const viewportPadding = 8;
    const bubbleWidth = Math.min(260, window.innerWidth * 0.72);
    const preferredLeft = rect.left + rect.width / 2 - bubbleWidth / 2;
    const clampedLeft = Math.min(
      Math.max(preferredLeft, viewportPadding),
      window.innerWidth - viewportPadding - bubbleWidth,
    );
    const top = Math.max(rect.top - 8, viewportPadding);
    setBubbleStyle({
      position: "fixed",
      left: `${clampedLeft}px`,
      top: `${top}px`,
      width: `${bubbleWidth}px`,
      transform: "translateY(-100%)",
    });
  };

  useEffect(() => {
    if (typeof window === "undefined" || !isOpen) {
      return;
    }
    updateBubblePosition();
    const handleResize = () => updateBubblePosition();
    const handleScroll = () => updateBubblePosition();
    window.addEventListener("resize", handleResize);
    window.addEventListener("scroll", handleScroll, true);
    return () => {
      window.removeEventListener("resize", handleResize);
      window.removeEventListener("scroll", handleScroll, true);
    };
  }, [isOpen]);

  return (
    <span
      ref={rootRef}
      className={`return-backtest-summary-hint ${isOpen ? "is-open" : ""}`}
      onMouseEnter={() => {
        updateBubblePosition();
        setIsHovered(true);
      }}
      onMouseLeave={() => setIsHovered(false)}
      onFocusCapture={() => {
        updateBubblePosition();
        setIsHovered(true);
      }}
      onBlurCapture={() => setIsHovered(false)}
      onClick={(event) => event.stopPropagation()}
    >
      <button
        type="button"
        className="return-backtest-summary-hint-button"
        aria-label="查看说明"
        aria-expanded={isOpen}
        onClick={() => {
          updateBubblePosition();
          onToggle(id);
        }}
      >
        ?
      </button>
      {typeof document !== "undefined" && bubbleStyle
        ? createPortal(
            <span
              className={`return-backtest-summary-hint-bubble ${
                isOpen ? "is-open" : ""
              }`}
              role="tooltip"
              style={bubbleStyle}
            >
              {text}
            </span>,
            document.body,
          )
        : null}
    </span>
  );
}

function ReturnTable({
  title,
  subtitle,
  rows,
  sourcePath,
  tradeDate,
  defaultSortKey,
  scrollRegionKey,
  variant,
}: {
  title: string;
  subtitle: string;
  rows: ReturnBacktestRow[];
  sourcePath: string;
  tradeDate: string;
  defaultSortKey: ReturnTableSortKey;
  scrollRegionKey: string;
  variant: ReturnTableVariant;
}) {
  const sortDefinitions = useMemo(
    () =>
      ({
        rank: { value: (row) => row.rank },
        best_rank: { value: (row) => row.best_rank },
        return_pct: { value: (row) => row.return_pct },
        excess_return_pct: { value: (row) => row.excess_return_pct },
      }) satisfies Partial<Record<ReturnTableSortKey, SortDefinition<ReturnBacktestRow>>>,
    [],
  );

  const { sortKey, sortDirection, sortedRows, toggleSort } = useTableSort<
    ReturnBacktestRow,
    ReturnTableSortKey
  >(rows, sortDefinitions, {
    key: defaultSortKey,
    direction: "desc",
  });
  const tableWrapRef = useRouteScrollRegion<HTMLDivElement>(
    scrollRegionKey,
    [sortedRows.length, rows.length],
  );

  return (
    <section className="return-backtest-panel-card">
      <div className="return-backtest-panel-head">
        <div>
          <h3>{title}</h3>
          <p>{subtitle}</p>
        </div>
      </div>
      <div className="return-backtest-table-wrap" ref={tableWrapRef}>
        <table className="return-backtest-table">
          <colgroup>
            <col className="return-backtest-col-rank" />
            <col className="return-backtest-col-stock" />
            {variant === "benchmark" ? (
              <col className="return-backtest-col-best-rank" />
            ) : null}
            <col className="return-backtest-col-return" />
            <col className="return-backtest-col-return" />
            <col className="return-backtest-col-period" />
          </colgroup>
          <thead>
            <tr>
              <th aria-sort={getAriaSort(sortKey === "rank", sortDirection)}>
                <TableSortButton
                  label="排名日名次"
                  isActive={sortKey === "rank"}
                  direction={sortDirection}
                  onClick={() => toggleSort("rank")}
                  title="按排名日期的原始名次排序"
                />
              </th>
              <th>股票</th>
              {variant === "benchmark" ? (
                <th aria-sort={getAriaSort(sortKey === "best_rank", sortDirection)}>
                  <TableSortButton
                    label="区间内最好排名"
                    isActive={sortKey === "best_rank"}
                    direction={sortDirection}
                    onClick={() => toggleSort("best_rank")}
                    title="按持有区间内出现过的最好排名排序"
                  />
                </th>
              ) : null}
              <th aria-sort={getAriaSort(sortKey === "return_pct", sortDirection)}>
                <TableSortButton
                  label="涨幅"
                  isActive={sortKey === "return_pct"}
                  direction={sortDirection}
                  onClick={() => toggleSort("return_pct")}
                  title="按区间涨幅排序"
                />
              </th>
              <th aria-sort={getAriaSort(sortKey === "excess_return_pct", sortDirection)}>
                <TableSortButton
                  label="超额"
                  isActive={sortKey === "excess_return_pct"}
                  direction={sortDirection}
                  onClick={() => toggleSort("excess_return_pct")}
                  title="按相对同期大盘超额排序"
                />
              </th>
              <th>区间</th>
            </tr>
          </thead>
          <tbody>
            {sortedRows.map((row) => (
              <tr key={row.ts_code}>
                <td>{formatNumber(row.rank, 0)}</td>
                <td
                  className="return-backtest-stock-cell"
                  title={`${row.name ?? row.ts_code} · ${row.ts_code} · ${row.board}`}
                >
                  <DetailsLink
                    className="return-backtest-link return-backtest-stock-link"
                    tsCode={row.ts_code}
                    tradeDate={tradeDate || undefined}
                    sourcePath={sourcePath}
                  >
                    {row.name ?? row.ts_code}
                  </DetailsLink>
                  <div className="return-backtest-cell-sub return-backtest-stock-meta">
                    {row.ts_code} · {row.board}
                  </div>
                </td>
                {variant === "benchmark" ? (
                  <td>{formatNumber(row.best_rank, 0)}</td>
                ) : null}
                <td
                  className={
                    (row.return_pct ?? 0) >= 0
                      ? "return-backtest-value-up"
                      : "return-backtest-value-down"
                  }
                >
                  {formatPercent(row.return_pct)}
                </td>
                <td
                  className={
                    (row.excess_return_pct ?? 0) >= 0
                      ? "return-backtest-value-up"
                      : "return-backtest-value-down"
                  }
                >
                  {formatPercent(row.excess_return_pct)}
                </td>
                <td className="return-backtest-period-cell">
                  <div>{formatDateLabel(row.entry_trade_date)} 开</div>
                  <div className="return-backtest-cell-sub">
                    {formatDateLabel(row.exit_trade_date)} 收
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

export default function ReturnBacktestPage() {
  const persistedState = useMemo(
    () =>
      typeof window === "undefined"
        ? null
        : readJsonStorage<Partial<PersistedReturnBacktestState>>(
            window.localStorage,
            RETURN_BACKTEST_STATE_KEY,
          ),
    [],
  );

  const [sourcePath, setSourcePath] = useState(
    () => persistedState?.sourcePath ?? readStoredSourcePath(),
  );
  const [dateOptions, setDateOptions] = useState<string[]>(
    () =>
      Array.isArray(persistedState?.dateOptions)
        ? persistedState.dateOptions.filter(
            (item): item is string => typeof item === "string",
          )
        : [],
  );
  const [rankDateInput, setRankDateInput] = useState(
    persistedState?.rankDateInput ?? DEFAULT_DATE_OPTION,
  );
  const [refDateInput, setRefDateInput] = useState(
    persistedState?.refDateInput ?? DEFAULT_DATE_OPTION,
  );
  const [topLimitInput, setTopLimitInput] = useState(
    persistedState?.topLimitInput ?? DEFAULT_TOP_LIMIT,
  );
  const [boardFilter, setBoardFilter] = useState<(typeof BOARD_OPTIONS)[number]>(
    persistedState?.boardFilter && BOARD_OPTIONS.includes(persistedState.boardFilter)
      ? persistedState.boardFilter
      : "全部",
  );
  const [pageData, setPageData] = useState<ReturnBacktestPageData | null>(
    () =>
      persistedState?.pageData && typeof persistedState.pageData === "object"
        ? (persistedState.pageData as ReturnBacktestPageData)
        : null,
  );
  const [loading, setLoading] = useState(false);
  const [dateLoading, setDateLoading] = useState(false);
  const [error, setError] = useState("");
  const [activeHintId, setActiveHintId] = useState<string | null>(null);
  const [strengthWindowConfig, setStrengthWindowConfig] =
    useState<StrengthWindowConfig | null>(null);
  const [submittedQuery, setSubmittedQuery] = useState<SubmittedBacktestQuery | null>(
    () => {
      const query = persistedState?.submittedQuery;
      if (!query || typeof query !== "object") {
        return null;
      }

      const sourcePath =
        typeof query.sourcePath === "string" ? query.sourcePath.trim() : "";
      const rankDate = typeof query.rankDate === "string" ? query.rankDate.trim() : "";
      const refDate = typeof query.refDate === "string" ? query.refDate.trim() : "";
      const topLimit =
        typeof query.topLimit === "number" && Number.isInteger(query.topLimit)
          ? query.topLimit
          : 0;
      if (!sourcePath || !rankDate || !refDate || topLimit <= 0) {
        return null;
      }

      return {
        sourcePath:
          sourcePath,
        rankDate,
        refDate,
        topLimit,
        board: typeof query.board === "string" ? query.board : undefined,
      };
    },
  );
  const sourcePathRef = useRef(sourcePath.trim());

  const sourcePathTrimmed = sourcePath.trim();
  const rankRows = pageData?.rank_rows ?? [];
  const benchmarkRows = (pageData?.benchmark_rows ?? []).filter(
    (row) => (row.return_pct ?? Number.NEGATIVE_INFINITY) > 5,
  );
  const toggleHint = (id: string) => {
    setActiveHintId((current) => (current === id ? null : id));
  };
  const buildSubmittedQuery = (
    nextSourcePath: string,
    nextRankDate: string,
    nextRefDate: string,
    nextTopLimitInput: string,
    nextBoardFilter: (typeof BOARD_OPTIONS)[number],
  ): SubmittedBacktestQuery | null => {
    if (!nextSourcePath || !nextRankDate || !nextRefDate) {
      return null;
    }
    const parsedTopLimit = Number(nextTopLimitInput);
    if (!Number.isInteger(parsedTopLimit) || parsedTopLimit <= 0) {
      return null;
    }
    return {
      sourcePath: nextSourcePath,
      rankDate: nextRankDate,
      refDate: nextRefDate,
      topLimit: parsedTopLimit,
      board: nextBoardFilter === "全部" ? undefined : nextBoardFilter,
    };
  };
  const applyFilters = () => {
    const nextQuery = buildSubmittedQuery(
      sourcePathTrimmed,
      rankDateInput,
      refDateInput,
      topLimitInput,
      boardFilter,
    );
    if (!nextQuery) {
      setError("Top 数量必须是正整数");
      return;
    }
    setError("");
    setSubmittedQuery(nextQuery);
  };
  const hasPendingChanges =
    submittedQuery === null ||
    submittedQuery.sourcePath !== sourcePathTrimmed ||
    submittedQuery.rankDate !== rankDateInput ||
    submittedQuery.refDate !== refDateInput ||
    submittedQuery.topLimit !== Number(topLimitInput) ||
    submittedQuery.board !== (boardFilter === "全部" ? undefined : boardFilter);

  const openStrengthWindow = async () => {
    const topLimit = parsePositiveInt(topLimitInput, Number(DEFAULT_TOP_LIMIT));
    const holdingDays = inferHoldingDays(rankDateInput, refDateInput, dateOptions);
    setStrengthWindowConfig({
      sourcePath: sourcePathTrimmed,
      topLimit,
      holdingDays,
      board: boardFilter,
      instanceKey: Date.now(),
    });
  };

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
    if (typeof window === "undefined" || activeHintId === null) {
      return;
    }
    const closeHint = () => setActiveHintId(null);
    window.addEventListener("pointerdown", closeHint);
    return () => window.removeEventListener("pointerdown", closeHint);
  }, [activeHintId]);

  useEffect(() => {
    if (typeof window === "undefined" || !strengthWindowConfig) {
      return;
    }

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setStrengthWindowConfig(null);
      }
    };

    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    window.addEventListener("keydown", handleKeyDown);
    return () => {
      document.body.style.overflow = previousOverflow;
      window.removeEventListener("keydown", handleKeyDown);
    };
  }, [strengthWindowConfig]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    writeJsonStorage(window.localStorage, RETURN_BACKTEST_STATE_KEY, {
      sourcePath: sourcePathTrimmed,
      dateOptions,
      rankDateInput,
      refDateInput,
      topLimitInput,
      boardFilter,
      pageData,
      submittedQuery,
    } satisfies PersistedReturnBacktestState);
  }, [
    boardFilter,
    dateOptions,
    pageData,
    rankDateInput,
    refDateInput,
    sourcePathTrimmed,
    submittedQuery,
    topLimitInput,
  ]);

  useEffect(() => {
    if (!sourcePathTrimmed) {
      setDateOptions([]);
      setSubmittedQuery(null);
      setPageData(null);
      sourcePathRef.current = sourcePathTrimmed;
      return;
    }

    const sourcePathChanged = sourcePathRef.current !== sourcePathTrimmed;
    sourcePathRef.current = sourcePathTrimmed;
    let cancelled = false;
    const load = async () => {
      setDateLoading(true);
      if (sourcePathChanged) {
        setSubmittedQuery(null);
        setPageData(null);
      }
      try {
        const values = normalizeTradeDates(
          await listRankTradeDates(sourcePathTrimmed),
        );
        if (cancelled) {
          return;
        }
        setDateOptions(values);
        const nextRankDate = pickRankDate(rankDateInput, values);
        const nextRefDate = pickRefDate(refDateInput, values, nextRankDate);
        setRankDateInput(nextRankDate);
        setRefDateInput(nextRefDate);
      } catch (loadError) {
        if (!cancelled) {
          setDateOptions([]);
          setError(`读取日期列表失败: ${String(loadError)}`);
        }
      } finally {
        if (!cancelled) {
          setDateLoading(false);
        }
      }
    };

    void load();
    return () => {
      cancelled = true;
    };
  }, [sourcePathTrimmed]);

  useEffect(() => {
    if (!submittedQuery) {
      return;
    }

    let cancelled = false;
    const load = async () => {
      setLoading(true);
      setError("");
      try {
        const nextPageData = await getReturnBacktestPage({
          sourcePath: submittedQuery.sourcePath,
          rankDate: submittedQuery.rankDate,
          refDate: submittedQuery.refDate,
          topLimit: submittedQuery.topLimit,
          board: submittedQuery.board,
        });
        if (!cancelled) {
          setPageData(nextPageData);
        }
      } catch (loadError) {
        if (!cancelled) {
          setPageData(null);
          setError(`读取排名回测失败: ${String(loadError)}`);
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    };

    void load();
    return () => {
      cancelled = true;
    };
  }, [submittedQuery]);
  return (
    <div className="return-backtest-page">
      <section className="return-backtest-card">
        <div className="return-backtest-head">
          <div>
            <h2>排名回测统计</h2>
            <p>
              统计口径为排名日期次日开盘买入，到参考日收盘卖出；同期大盘使用板块筛选后的全市场样本作为比较口径。
            </p>
          </div>
          <button
            type="button"
            className="return-backtest-apply-button return-backtest-window-button"
            onClick={() => void openStrengthWindow()}
          >
            强弱格子图
          </button>
        </div>

        <div className="return-backtest-form-grid">
          <label className="return-backtest-field">
            <span>排名日期</span>
            <select
              value={rankDateInput}
              onChange={(event) => {
                const nextRankDate = event.target.value;
                setRankDateInput(nextRankDate);
                setRefDateInput((current) =>
                  pickRefDate(current, dateOptions, nextRankDate),
                );
              }}
              disabled={dateLoading || dateOptions.length === 0}
            >
              {dateOptions.map((option) => (
                <option key={option} value={option}>
                  {formatDateLabel(option)}
                </option>
              ))}
            </select>
          </label>

          <label className="return-backtest-field">
            <span>参考日</span>
            <select
              value={refDateInput}
              onChange={(event) => setRefDateInput(event.target.value)}
              disabled={dateLoading || dateOptions.length === 0}
            >
              {dateOptions
                .filter((option) => !rankDateInput || option > rankDateInput)
                .map((option) => (
                  <option key={option} value={option}>
                    {formatDateLabel(option)}
                  </option>
                ))}
            </select>
          </label>

          <label className="return-backtest-field">
            <span>Top 数量</span>
            <input
              type="number"
              min={1}
              step={1}
              value={topLimitInput}
              onChange={(event) => setTopLimitInput(event.target.value)}
            />
          </label>

          <label className="return-backtest-field">
            <span>板块</span>
            <select
              value={boardFilter}
              onChange={(event) =>
                setBoardFilter(event.target.value as (typeof BOARD_OPTIONS)[number])
              }
            >
              {BOARD_OPTIONS.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </label>
        </div>
        <div className="return-backtest-actions">
          <button
            type="button"
            className="return-backtest-apply-button"
            onClick={applyFilters}
            disabled={loading || dateLoading}
          >
            {loading ? "统计中..." : "刷新统计"}
          </button>
          <span className="return-backtest-actions-note">
            {hasPendingChanges ? "未刷新" : "已刷新"}
          </span>
        </div>

        {error ? <div className="return-backtest-error">{error}</div> : null}

        <div className="return-backtest-summary-grid">
          <div className="return-backtest-summary-item">
            <span>当前板块</span>
            <strong>{pageData?.board ?? boardFilter}</strong>
          </div>
          <div
            className={`return-backtest-summary-item ${
              activeHintId === "strength-label" ? "is-floating" : ""
            }`}
          >
            <span className="return-backtest-summary-label">
              当前排名相对大盘
              <SummaryHint
                id="strength-label"
                activeId={activeHintId}
                onToggle={toggleHint}
                text="Top 内 >5% 记强势，<-3% 记弱势；强弱分数 >0 更强，<0 更弱。"
              />
            </span>
            <strong className={strengthClassName(pageData?.summary?.strength_label)}>
              {pageData?.summary?.strength_label ?? "--"}
            </strong>
          </div>
          <div className="return-backtest-summary-item">
            <span>Top 取样</span>
            <strong>
              {pageData?.summary
                ? `${pageData.summary.valid_top_count} / ${pageData.summary.selected_top_count}`
                : "--"}
            </strong>
          </div>
          <div className="return-backtest-summary-item">
            <span>{pageData?.benchmark_label ?? "同期大盘"}</span>
            <strong>{formatPercent(pageData?.summary?.benchmark_return_pct)}</strong>
          </div>
          <div className="return-backtest-summary-item">
            <span>Top 平均涨幅</span>
            <strong>{formatPercent(pageData?.summary?.top_avg_return_pct)}</strong>
          </div>
          <div className="return-backtest-summary-item">
            <span>Top 平均超额</span>
            <strong>{formatPercent(pageData?.summary?.top_avg_excess_return_pct)}</strong>
            <small className="return-backtest-summary-note">
              强势命中 {formatPercent(pageData?.summary?.top_strong_hit_rate)} / 弱势命中{" "}
              {formatPercent(pageData?.summary?.top_weak_hit_rate)}
            </small>
          </div>
          <div className="return-backtest-summary-item">
            <span>同期大盘样本数</span>
            <strong>{formatNumber(pageData?.summary?.benchmark_sample_count, 0)}</strong>
            <small className="return-backtest-summary-note">
              强势命中 {formatPercent(pageData?.summary?.benchmark_strong_hit_rate)} / 弱势命中{" "}
              {formatPercent(pageData?.summary?.benchmark_weak_hit_rate)}
            </small>
          </div>
          <div
            className={`return-backtest-summary-item ${
              activeHintId === "strength-score" ? "is-floating" : ""
            }`}
          >
            <span className="return-backtest-summary-label">
              强弱分数
              <SummaryHint
                id="strength-score"
                activeId={activeHintId}
                onToggle={toggleHint}
                text="大于 0 表示 Top 比市场更容易出大涨、也更少出明显回撤；小于 0 反之。公式：(Top 强势占比 - 市场强势占比) - (Top 弱势占比 - 市场弱势占比)。这里是百分点差，不是收益率。"
              />
            </span>
            <strong>{formatPercent(pageData?.summary?.strength_score)}</strong>
          </div>
        </div>
      </section>

      {loading ? (
        <section className="return-backtest-card">
          <div className="return-backtest-empty">加载中...</div>
        </section>
      ) : !pageData ? (
        <section className="return-backtest-card">
          <div className="return-backtest-empty">暂无结果</div>
        </section>
      ) : (
        <div className="return-backtest-grid">
          <div className="return-backtest-column">
            <DistributionChart
              title="排名样本涨幅分布"
              subtitle={`${formatDateLabel(
                pageData?.resolved_rank_date,
              )} 至 ${formatDateLabel(pageData?.resolved_ref_date)}`}
              buckets={pageData?.rank_distribution ?? []}
            />
            <ReturnTable
              title="Top 样本表现明细"
              subtitle="Top 样本"
              rows={rankRows}
              sourcePath={sourcePathTrimmed}
              tradeDate={pageData?.resolved_ref_date ?? refDateInput}
              defaultSortKey="return_pct"
              scrollRegionKey="return-backtest-rank-table"
              variant="rank"
            />
          </div>

          <div className="return-backtest-column">
            <DistributionChart
              title="同期大盘分布"
              subtitle={pageData?.benchmark_label ?? "同期大盘"}
              buckets={pageData?.benchmark_distribution ?? []}
            />
            <ReturnTable
              title="同期大盘强势样本明细"
              subtitle={`${pageData?.benchmark_label ?? "同期大盘"}强势样本`}
              rows={benchmarkRows}
              sourcePath={sourcePathTrimmed}
              tradeDate={pageData?.resolved_ref_date ?? refDateInput}
              defaultSortKey="return_pct"
              scrollRegionKey="return-backtest-benchmark-table"
              variant="benchmark"
            />
          </div>
        </div>
      )}
      {strengthWindowConfig && typeof document !== "undefined"
        ? createPortal(
            <div
              className="return-backtest-modal-overlay"
              onClick={() => setStrengthWindowConfig(null)}
            >
              <div
                className="return-backtest-modal-shell"
                onClick={(event) => event.stopPropagation()}
              >
                <ReturnBacktestStrengthPanel
                  key={strengthWindowConfig.instanceKey}
                  initialSourcePath={strengthWindowConfig.sourcePath}
                  initialHoldingDays={strengthWindowConfig.holdingDays}
                  initialTopLimit={strengthWindowConfig.topLimit}
                  initialBoard={strengthWindowConfig.board}
                  embedded
                  showCloseButton
                  onClose={() => setStrengthWindowConfig(null)}
                />
              </div>
            </div>,
            document.body,
          )
        : null}
    </div>
  );
}
