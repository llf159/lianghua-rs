import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import {
  convolutionRankPage,
  listRankTradeDates,
  type ConvolutionRankPageData,
  type ConvolutionRankPageQuery,
  type ConvolutionRankRow,
} from "../../apis/reader";
import DetailsLink from "../../shared/DetailsLink";
import {
  filterBoardItems,
  formatConceptText,
  isStBoard,
  useConceptExclusions,
} from "../../shared/conceptExclusions";
import { useRouteScrollRegion } from "../../shared/routeScroll";
import { STOCK_PICK_BOARD_OPTIONS } from "../../shared/stockPickShared";
import {
  readJsonStorage,
  readStoredSourcePath,
  writeJsonStorage,
} from "../../shared/storage";
import {
  TableSortButton,
  getAriaSort,
  type SortDefinition,
  type SortDirection,
  useTableSort,
} from "../../shared/tableSort";
import {
  DEFAULT_DATE_OPTION,
  normalizeTradeDates,
  pickDateValue,
} from "../../shared/tradeDate";
import "./css/OverviewScenePage.css";

const FILTER_STATE_KEY = "lh_convolution_overview_filter_state_v1";
const RESULT_STATE_KEY = "lh_convolution_overview_result_state_v1";

const VISIBLE_COLUMNS = [
  "convolution_rank",
  "ts_code",
  "name",
  "total_mv_yi",
  "board",
  "convolution_score",
  "raw_score",
  "concept",
  "database_rank",
  "raw_rank",
  "rank_change",
] as const;

type VisibleColumn = (typeof VISIBLE_COLUMNS)[number];

type AppliedConfig = {
  tradeDate: string;
  limit: number | null;
  board: string | null;
  totalMvMin: number | null;
  totalMvMax: number | null;
  rowCount: number;
  universeSize: number;
  kernelName: string;
  windowSize: number;
};

type PersistedFilterState = {
  sourcePath: string;
  tradeDateInput: string;
  limitInput: string;
  boardFilter: (typeof STOCK_PICK_BOARD_OPTIONS)[number];
  totalMvMinInput: string;
  totalMvMaxInput: string;
  sortKey: VisibleColumn | null;
  sortDirection: SortDirection;
};

type PersistedResultState = {
  rows: ConvolutionRankRow[];
  dateOptions: string[];
  pageMeta: Omit<ConvolutionRankPageData, "rows"> | null;
  lastConfig: AppliedConfig | null;
};

const COLUMN_LABELS: Record<VisibleColumn, string> = {
  convolution_rank: "卷积排名",
  rank_change: "名次变化",
  raw_rank: "窗口内原始排名",
  database_rank: "原始总榜排名",
  ts_code: "代码",
  name: "名称",
  total_mv_yi: "总市值(亿)",
  board: "板块",
  convolution_score: "卷积分",
  raw_score: "当日总分",
  concept: "概念",
};

const COLUMN_WIDTHS: Record<VisibleColumn, number> = {
  convolution_rank: 96,
  rank_change: 92,
  raw_rank: 128,
  database_rank: 112,
  ts_code: 120,
  name: 108,
  total_mv_yi: 110,
  board: 108,
  convolution_score: 96,
  raw_score: 96,
  concept: 260,
};

function formatNumber(value: number | null | undefined, digits = 2) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  return digits === 0 ? String(Math.round(value)) : value.toFixed(digits);
}

function formatCell(
  key: VisibleColumn,
  row: ConvolutionRankRow,
  excludedConcepts: readonly string[],
) {
  if (key === "concept") {
    return formatConceptText(row.concept, excludedConcepts);
  }
  if (key === "rank_change") {
    if (!Number.isFinite(row.rank_change) || row.rank_change === 0) {
      return "0";
    }
    return row.rank_change > 0
      ? `+${row.rank_change}`
      : String(row.rank_change);
  }
  if (
    key === "convolution_rank" ||
    key === "raw_rank" ||
    key === "database_rank"
  ) {
    return formatNumber(row[key], 0);
  }
  if (key === "convolution_score" || key === "raw_score") {
    return formatNumber(row[key]);
  }
  if (key === "total_mv_yi") {
    return formatNumber(row.total_mv_yi);
  }
  const value = row[key];
  return value === null || value === undefined || value === ""
    ? "--"
    : String(value);
}

function rankChangeClassName(value: number) {
  if (value > 0) {
    return "overview-cell-positive";
  }
  if (value < 0) {
    return "overview-cell-negative";
  }
  return undefined;
}

export default function OverviewConvolutionPage() {
  const { excludedConcepts, excludeStBoard } = useConceptExclusions();
  const persistedFilter = useMemo(
    () =>
      readJsonStorage<Partial<PersistedFilterState>>(
        typeof window === "undefined" ? null : window.sessionStorage,
        FILTER_STATE_KEY,
      ),
    [],
  );
  const persistedResult = useMemo(
    () =>
      readJsonStorage<Partial<PersistedResultState>>(
        typeof window === "undefined" ? null : window.sessionStorage,
        RESULT_STATE_KEY,
      ),
    [],
  );

  const [sourcePath, setSourcePath] = useState(
    () => persistedFilter?.sourcePath ?? readStoredSourcePath(),
  );
  const [tradeDateInput, setTradeDateInput] = useState(
    () => persistedFilter?.tradeDateInput ?? DEFAULT_DATE_OPTION,
  );
  const [limitInput, setLimitInput] = useState(
    () => persistedFilter?.limitInput ?? "100",
  );
  const [boardFilter, setBoardFilter] = useState<
    (typeof STOCK_PICK_BOARD_OPTIONS)[number]
  >(() => persistedFilter?.boardFilter ?? "全部");
  const [totalMvMinInput, setTotalMvMinInput] = useState(
    () => persistedFilter?.totalMvMinInput ?? "",
  );
  const [totalMvMaxInput, setTotalMvMaxInput] = useState(
    () => persistedFilter?.totalMvMaxInput ?? "",
  );
  const [rows, setRows] = useState<ConvolutionRankRow[]>(
    () => persistedResult?.rows ?? [],
  );
  const [dateOptions, setDateOptions] = useState<string[]>(
    () => persistedResult?.dateOptions ?? [],
  );
  const [pageMeta, setPageMeta] = useState<Omit<
    ConvolutionRankPageData,
    "rows"
  > | null>(() => persistedResult?.pageMeta ?? null);
  const [lastConfig, setLastConfig] = useState<AppliedConfig | null>(
    () => persistedResult?.lastConfig ?? null,
  );
  const [loading, setLoading] = useState(false);
  const [dateOptionsLoading, setDateOptionsLoading] = useState(false);
  const [error, setError] = useState("");
  const autoReadTriggeredRef = useRef(false);
  const sourcePathTrimmed = sourcePath.trim();
  const boardOptions = useMemo(
    () =>
      filterBoardItems(
        STOCK_PICK_BOARD_OPTIONS,
        excludeStBoard,
      ) as (typeof STOCK_PICK_BOARD_OPTIONS)[number][],
    [excludeStBoard],
  );

  const sortDefinitions = useMemo(
    () =>
      Object.fromEntries(
        VISIBLE_COLUMNS.map((key) => [
          key,
          {
            value: (row: ConvolutionRankRow) => row[key],
          } satisfies SortDefinition<ConvolutionRankRow>,
        ]),
      ) as Partial<Record<VisibleColumn, SortDefinition<ConvolutionRankRow>>>,
    [],
  );
  const { sortKey, sortDirection, sortedRows, toggleSort } = useTableSort(
    rows,
    sortDefinitions,
    {
      key: persistedFilter?.sortKey ?? null,
      direction: persistedFilter?.sortDirection ?? null,
    },
  );
  const tableMinWidth = VISIBLE_COLUMNS.reduce(
    (width, key) => width + COLUMN_WIDTHS[key],
    0,
  );
  const tableWrapRef = useRouteScrollRegion<HTMLDivElement>(
    "convolution-overview-table",
    [rows.length],
  );
  const detailNavigationItems = sortedRows.map((row) => ({
    tsCode: row.ts_code,
    tradeDate: row.trade_date,
    sourcePath: sourcePathTrimmed || undefined,
    name: row.name || undefined,
  }));

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
    if (excludeStBoard && isStBoard(boardFilter)) {
      setBoardFilter("全部");
    }
  }, [boardFilter, excludeStBoard]);

  useEffect(() => {
    writeJsonStorage(
      typeof window === "undefined" ? null : window.sessionStorage,
      FILTER_STATE_KEY,
      {
        sourcePath,
        tradeDateInput,
        limitInput,
        boardFilter,
        totalMvMinInput,
        totalMvMaxInput,
        sortKey,
        sortDirection,
      } satisfies PersistedFilterState,
    );
  }, [
    boardFilter,
    limitInput,
    sortDirection,
    sortKey,
    sourcePath,
    totalMvMaxInput,
    totalMvMinInput,
    tradeDateInput,
  ]);

  useEffect(() => {
    writeJsonStorage(
      typeof window === "undefined" ? null : window.sessionStorage,
      RESULT_STATE_KEY,
      {
        rows,
        dateOptions,
        pageMeta,
        lastConfig,
      } satisfies PersistedResultState,
    );
  }, [dateOptions, lastConfig, pageMeta, rows]);

  useEffect(() => {
    if (!sourcePathTrimmed) {
      setDateOptions([]);
      setTradeDateInput(DEFAULT_DATE_OPTION);
      return;
    }
    let cancelled = false;
    setDateOptionsLoading(true);
    void listRankTradeDates(sourcePathTrimmed)
      .then((values) => {
        if (cancelled) {
          return;
        }
        const nextOptions = normalizeTradeDates(values);
        setDateOptions(nextOptions);
        setTradeDateInput((current) => pickDateValue(current, nextOptions));
        setError("");
      })
      .catch((readError) => {
        if (!cancelled) {
          setDateOptions([]);
          setTradeDateInput(DEFAULT_DATE_OPTION);
          setError(`读取日期列表失败: ${String(readError)}`);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setDateOptionsLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [sourcePathTrimmed]);

  const onRead = useCallback(async () => {
    if (!sourcePathTrimmed) {
      setError("请先到“数据管理”页完成数据准备");
      return;
    }

    const parseOptionalNumber = (raw: string, label: string) => {
      if (!raw.trim()) {
        return undefined;
      }
      const value = Number(raw);
      if (!Number.isFinite(value)) {
        throw new Error(`${label}必须是数字`);
      }
      return value;
    };

    try {
      const limitRaw = limitInput.trim();
      const limit = limitRaw ? Number(limitRaw) : undefined;
      if (limit !== undefined && (!Number.isInteger(limit) || limit <= 0)) {
        throw new Error("限制行数必须是正整数");
      }
      const totalMvMin = parseOptionalNumber(totalMvMinInput, "总市值最小值");
      const totalMvMax = parseOptionalNumber(totalMvMaxInput, "总市值最大值");
      if (
        totalMvMin !== undefined &&
        totalMvMax !== undefined &&
        totalMvMin > totalMvMax
      ) {
        throw new Error("总市值最小值不能大于最大值");
      }

      const query: ConvolutionRankPageQuery = {
        sourcePath: sourcePathTrimmed,
        tradeDate: tradeDateInput || undefined,
        limit,
        board: boardFilter === "全部" ? undefined : boardFilter,
        excludeStBoard: excludeStBoard || undefined,
        totalMvMin,
        totalMvMax,
      };
      setLoading(true);
      setError("");
      const data = await convolutionRankPage(query);
      const nextRows = data.rows ?? [];
      setRows(nextRows);
      setPageMeta({
        resolved_trade_date: data.resolved_trade_date,
        kernel_name: data.kernel_name,
        kernel: data.kernel,
        history_trade_dates: data.history_trade_dates,
        universe_size: data.universe_size,
      });
      setTradeDateInput(data.resolved_trade_date);
      setLastConfig({
        tradeDate: data.resolved_trade_date,
        limit: limit ?? null,
        board: query.board ?? null,
        totalMvMin: totalMvMin ?? null,
        totalMvMax: totalMvMax ?? null,
        rowCount: nextRows.length,
        universeSize: data.universe_size,
        kernelName: data.kernel_name,
        windowSize: data.kernel.length,
      });
    } catch (readError) {
      setError(`读取失败: ${String(readError)}`);
      setRows([]);
      setPageMeta(null);
    } finally {
      setLoading(false);
    }
  }, [
    boardFilter,
    excludeStBoard,
    limitInput,
    sourcePathTrimmed,
    totalMvMaxInput,
    totalMvMinInput,
    tradeDateInput,
  ]);

  useEffect(() => {
    if (
      autoReadTriggeredRef.current ||
      dateOptionsLoading ||
      !sourcePathTrimmed
    ) {
      return;
    }
    autoReadTriggeredRef.current = true;
    void onRead();
  }, [dateOptionsLoading, onRead, sourcePathTrimmed]);

  const historyTitle = (row: ConvolutionRankRow) =>
    pageMeta?.history_trade_dates
      .map(
        (date, index) => `${date}: ${formatNumber(row.score_history[index])}`,
      )
      .join("\n");

  return (
    <div className="overview-page">
      <section className="overview-card">
        <h2 className="overview-title">卷积排名总览</h2>
        <div className="overview-source-note">
          对总分进行时间加权平滑后重新排名；正的名次变化表示卷积后排名上升。
        </div>
        <div className="overview-form-grid">
          <label className="overview-field">
            <span>排名日期</span>
            <select
              value={tradeDateInput}
              onChange={(event) => setTradeDateInput(event.target.value)}
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
          <label className="overview-field">
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
          <label className="overview-field">
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
          <label className="overview-field">
            <span>总市值最小(亿)</span>
            <input
              type="number"
              step={0.01}
              value={totalMvMinInput}
              onChange={(event) => setTotalMvMinInput(event.target.value)}
              placeholder="留空=不限"
            />
          </label>
          <label className="overview-field">
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
        <div className="overview-actions">
          <button
            className="overview-read-btn"
            type="button"
            onClick={() => void onRead()}
            disabled={loading || dateOptionsLoading || !sourcePathTrimmed}
          >
            {loading ? "读取中..." : "读取"}
          </button>
        </div>
        {error ? <div className="overview-error">{error}</div> : null}
      </section>

      {lastConfig ? (
        <section className="overview-card">
          <h3 className="overview-subtitle">本次读取配置</h3>
          <div className="overview-summary-grid">
            <div className="overview-summary-item">
              <span>排名日期</span>
              <strong>{lastConfig.tradeDate}</strong>
            </div>
            <div className="overview-summary-item">
              <span>卷积核</span>
              <strong>{lastConfig.kernelName}</strong>
            </div>
            <div className="overview-summary-item">
              <span>时间窗口</span>
              <strong>{lastConfig.windowSize} 个交易日</strong>
            </div>
            <div className="overview-summary-item">
              <span>完整窗口股票数</span>
              <strong>{lastConfig.universeSize}</strong>
            </div>
            <div className="overview-summary-item">
              <span>筛选后返回</span>
              <strong>{lastConfig.rowCount}</strong>
            </div>
            <div className="overview-summary-item">
              <span>板块 / 行数限制</span>
              <strong>
                {lastConfig.board ?? "不限"} / {lastConfig.limit ?? "不限"}
              </strong>
            </div>
          </div>
        </section>
      ) : null}

      <section className="overview-card">
        <h3 className="overview-subtitle">结果表格</h3>
        {sortedRows.length === 0 ? (
          <div className="overview-empty">暂无卷积排名数据</div>
        ) : (
          <div className="overview-table-wrap" ref={tableWrapRef}>
            <table
              className="overview-table"
              style={{ minWidth: `${tableMinWidth}px` }}
            >
              <colgroup>
                {VISIBLE_COLUMNS.map((key) => (
                  <col key={key} style={{ width: `${COLUMN_WIDTHS[key]}px` }} />
                ))}
              </colgroup>
              <thead>
                <tr>
                  {VISIBLE_COLUMNS.map((key) => {
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
                {sortedRows.map((row) => (
                  <tr key={`${row.ts_code}-${row.trade_date}`}>
                    {VISIBLE_COLUMNS.map((key) => {
                      const cellValue = formatCell(key, row, excludedConcepts);
                      return (
                        <td
                          key={`${row.ts_code}-${key}`}
                          className={
                            key === "rank_change"
                              ? rankChangeClassName(row.rank_change)
                              : undefined
                          }
                          title={
                            key === "convolution_score"
                              ? historyTitle(row)
                              : cellValue
                          }
                        >
                          {key === "name" && cellValue !== "--" ? (
                            <DetailsLink
                              className="overview-stock-link"
                              tsCode={row.ts_code}
                              tradeDate={row.trade_date}
                              sourcePath={sourcePathTrimmed}
                              title={`查看 ${cellValue} 详情`}
                              navigationItems={detailNavigationItems}
                            >
                              {cellValue}
                            </DetailsLink>
                          ) : (
                            cellValue
                          )}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  );
}
