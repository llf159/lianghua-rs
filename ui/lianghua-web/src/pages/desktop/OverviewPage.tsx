import { useEffect, useMemo, useState } from "react";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import {
  listRankTradeDates,
  rankOverviewPage,
  type OverviewPageQuery,
  type OverviewRow,
} from "../../apis/reader";
import DetailsLink from "../../shared/DetailsLink";
import {
  formatConceptText,
  useConceptExclusions,
} from "../../shared/conceptExclusions";
import { readJsonStorage, readStoredSourcePath } from "../../shared/storage";
import { useRouteScrollRegion } from "../../shared/routeScroll";
import {
  DEFAULT_DATE_OPTION,
  findFirstPopulatedString,
  normalizeTradeDates,
  pickDateValue,
} from "../../shared/tradeDate";
import {
  TableSortButton,
  getAriaSort,
  type SortDefinition,
  type SortDirection,
  useTableSort,
} from "../../shared/tableSort";
import "./css/OverviewPage.css";

const OVERVIEW_PAGE_STATE_KEY = "lh_overview_page_state";
const BOARD_OPTIONS = ["全部", "主板", "创业/科创", "北交所"] as const;
const FIXED_VISIBLE_COLUMNS = [
  "rank",
  "ts_code",
  "name",
  "total_mv_yi",
  "board",
  "total_score",
  "post_rank_return_pct",
  "ref_rank",
  "tiebreak_j",
  "concept",
] as const;

type ColumnConfig = {
  label?: string;
  order?: number;
  width?: number;
  isFlexible?: boolean;
};

type AppliedConfig = {
  rankDate: string | null;
  refDate: string | null;
  limit: number | null;
  board: string | null;
  totalMvMin: number | null;
  totalMvMax: number | null;
  rowCount: number;
};

type PersistedOverviewState = {
  sourcePath: string;
  rankDateInput: string;
  refDateInput: string;
  limitInput: string;
  boardFilter: (typeof BOARD_OPTIONS)[number];
  totalMvMinInput: string;
  totalMvMaxInput: string;
  rows: OverviewRow[];
  dateOptions: string[];
  lastConfig: AppliedConfig | null;
  sortKey: string | null;
  sortDirection: SortDirection;
};

const DEFAULT_COLUMN_WIDTH = 120;
const CONCEPT_COLUMN_MIN_WIDTH = 240;
const DEFAULT_COLUMN_ORDER = 500;

const COLUMN_CONFIG: Record<string, ColumnConfig> = {
  rank: { label: "排名", order: 10, width: 64 },
  ts_code: { label: "代码", order: 20, width: 120 },
  name: { label: "名称", order: 30, width: 108 },
  total_mv_yi: { label: "总市值(亿)", order: 40, width: 110 },
  board: { label: "板块", order: 50, width: 108 },
  board_category: { label: "板块分类" },
  total_score: { label: "总分", order: 60, width: 84 },
  post_rank_return_pct: { label: "至今涨幅(%)", order: 65, width: 118 },
  ref_rank: { label: "参考日排名", order: 66, width: 110 },
  tiebreak_j: { label: "同分排序J", order: 70, width: 96 },
  trade_date: { label: "排名日期", width: 110 },
  ref_date: { label: "参考日", width: 110 },
  concept: {
    label: "概念",
    order: 999,
    width: CONCEPT_COLUMN_MIN_WIDTH,
    isFlexible: true,
  },
};

function normalizeRows(rows: OverviewRow[]) {
  return rows.map((row) => {
    const normalized: OverviewRow = { ...row };
    if (
      normalized.board === undefined &&
      typeof normalized.board_category === "string"
    ) {
      normalized.board = normalized.board_category;
    }
    return normalized;
  });
}

function buildVisibleColumns(rows: OverviewRow[]): string[] {
  const keySet = new Set<string>(FIXED_VISIBLE_COLUMNS);

  rows.forEach((row) => {
    Object.keys(row).forEach((key) => {
      if (
        key === "trade_date" ||
        key === "ref_date" ||
        key === "resolved_rank_date" ||
        key === "resolved_ref_date"
      ) {
        return;
      }
      keySet.add(key);
    });
  });

  return [...keySet].sort((left, right) => {
    const leftOrder = COLUMN_CONFIG[left]?.order ?? DEFAULT_COLUMN_ORDER;
    const rightOrder = COLUMN_CONFIG[right]?.order ?? DEFAULT_COLUMN_ORDER;

    if (leftOrder !== rightOrder) {
      return leftOrder - rightOrder;
    }

    return left.localeCompare(right);
  });
}

function getColumnWidth(key: string) {
  return COLUMN_CONFIG[key]?.width ?? DEFAULT_COLUMN_WIDTH;
}

function isOverviewSortableColumn(key: string) {
  return key !== "ts_code" && key !== "name" && key !== "concept";
}

function formatCell(
  key: string,
  value: OverviewRow[string],
  excludedConcepts: readonly string[],
): string {
  if (key === "concept") {
    return formatConceptText(value, excludedConcepts);
  }

  if (value === null || value === undefined || value === "") {
    return "--";
  }

  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      return "--";
    }
    if (key === "rank" || key === "ref_rank") {
      return String(Math.round(value));
    }
    if (key === "post_rank_return_pct") {
      return value.toFixed(2);
    }
    if (Number.isInteger(value)) {
      return String(value);
    }
    return value.toFixed(2);
  }

  return String(value);
}

function getCellClassName(key: string, value: OverviewRow[string]) {
  if (key !== "post_rank_return_pct") {
    return "";
  }

  const numericValue =
    typeof value === "number" && Number.isFinite(value)
      ? value
      : typeof value === "string" && Number.isFinite(Number(value.trim()))
        ? Number(value.trim())
        : null;
  if (numericValue === null || numericValue === 0) {
    return "";
  }

  return numericValue > 0 ? "overview-cell-positive" : "overview-cell-negative";
}

export default function OverviewPage() {
  const { excludedConcepts } = useConceptExclusions();
  const persistedState = useMemo(() => {
    const parsed = readJsonStorage<Partial<PersistedOverviewState>>(
      typeof window === "undefined" ? null : window.sessionStorage,
      OVERVIEW_PAGE_STATE_KEY,
    );
    if (!parsed || typeof parsed !== "object") {
      return null;
    }

    return {
      sourcePath:
        typeof parsed.sourcePath === "string" ? parsed.sourcePath : "",
      rankDateInput:
        typeof parsed.rankDateInput === "string" ? parsed.rankDateInput : "",
      refDateInput:
        typeof parsed.refDateInput === "string" ? parsed.refDateInput : "",
      limitInput:
        typeof parsed.limitInput === "string" ? parsed.limitInput : "100",
      boardFilter:
        parsed.boardFilter && BOARD_OPTIONS.includes(parsed.boardFilter)
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
      rows: Array.isArray(parsed.rows) ? parsed.rows : [],
      dateOptions: Array.isArray(parsed.dateOptions) ? parsed.dateOptions : [],
      lastConfig:
        parsed.lastConfig && typeof parsed.lastConfig === "object"
          ? parsed.lastConfig
          : null,
      sortKey: typeof parsed.sortKey === "string" ? parsed.sortKey : null,
      sortDirection:
        parsed.sortDirection === "desc" || parsed.sortDirection === "asc"
          ? parsed.sortDirection
          : null,
    } satisfies PersistedOverviewState;
  }, []);
  const [sourcePath, setSourcePath] = useState(
    () => persistedState?.sourcePath ?? readStoredSourcePath(),
  );
  const [rankDateInput, setRankDateInput] = useState(
    () => persistedState?.rankDateInput ?? "",
  );
  const [refDateInput, setRefDateInput] = useState(
    () => persistedState?.refDateInput ?? "",
  );
  const [limitInput, setLimitInput] = useState(
    () => persistedState?.limitInput ?? "100",
  );
  const [boardFilter, setBoardFilter] = useState<
    (typeof BOARD_OPTIONS)[number]
  >(() => persistedState?.boardFilter ?? "全部");
  const [totalMvMinInput, setTotalMvMinInput] = useState(
    () => persistedState?.totalMvMinInput ?? "",
  );
  const [totalMvMaxInput, setTotalMvMaxInput] = useState(
    () => persistedState?.totalMvMaxInput ?? "",
  );

  const [rows, setRows] = useState<OverviewRow[]>(
    () => persistedState?.rows ?? [],
  );
  const [dateOptions, setDateOptions] = useState<string[]>(
    () => persistedState?.dateOptions ?? [],
  );
  const [lastConfig, setLastConfig] = useState<AppliedConfig | null>(
    () => persistedState?.lastConfig ?? null,
  );
  const [loading, setLoading] = useState(false);
  const [dateOptionsLoading, setDateOptionsLoading] = useState(false);
  const [error, setError] = useState("");

  const visibleColumns = useMemo(() => buildVisibleColumns(rows), [rows]);
  const sortDefinitions = useMemo(
    () =>
      Object.fromEntries(
        visibleColumns
          .filter((key) => isOverviewSortableColumn(key))
          .map((key) => [
            key,
            {
              value: (row: OverviewRow) =>
                key === "concept"
                  ? formatConceptText(row[key], excludedConcepts, "")
                  : row[key],
            } satisfies SortDefinition<OverviewRow>,
          ]),
      ) as Partial<Record<string, SortDefinition<OverviewRow>>>,
    [excludedConcepts, visibleColumns],
  );
  const { sortKey, sortDirection, sortedRows, toggleSort } = useTableSort(
    rows,
    sortDefinitions,
    {
      key: persistedState?.sortKey ?? null,
      direction: persistedState?.sortDirection ?? null,
    },
  );
  const tableMinWidth = useMemo(
    () =>
      visibleColumns.reduce(
        (totalWidth, key) => totalWidth + getColumnWidth(key),
        0,
      ),
    [visibleColumns],
  );
  const sourcePathTrimmed = sourcePath.trim();
  const detailNavigationItems = sortedRows.map((row) => ({
    tsCode: row.ts_code,
    tradeDate: getRowDetailsTradeDate(row),
    sourcePath: sourcePathTrimmed || undefined,
    name: typeof row.name === "string" ? row.name : undefined,
  }));
  const tableWrapRef = useRouteScrollRegion<HTMLDivElement>(
    "overview-table",
    [rows.length, tableMinWidth],
  );

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
    try {
      window.sessionStorage.setItem(
        OVERVIEW_PAGE_STATE_KEY,
        JSON.stringify({
          sourcePath,
          rankDateInput,
          refDateInput,
          limitInput,
          boardFilter,
          totalMvMinInput,
          totalMvMaxInput,
          rows,
          dateOptions,
          lastConfig,
          sortKey,
          sortDirection,
        } satisfies PersistedOverviewState),
      );
    } catch {
      // Ignore storage quota and serialization failures; the page still works without persistence.
    }
  }, [
    sourcePath,
    rankDateInput,
    refDateInput,
    limitInput,
    boardFilter,
    totalMvMinInput,
    totalMvMaxInput,
    rows,
    dateOptions,
    lastConfig,
    sortKey,
    sortDirection,
  ]);

  useEffect(() => {
    if (!sourcePathTrimmed) {
      setDateOptions([]);
      setRankDateInput(DEFAULT_DATE_OPTION);
      setRefDateInput(DEFAULT_DATE_OPTION);
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
        setRankDateInput((current) => pickDateValue(current, values));
        setRefDateInput((current) => pickDateValue(current, values));
        setError("");
      } catch (e) {
        if (cancelled) {
          return;
        }
        setDateOptions([]);
        setRankDateInput(DEFAULT_DATE_OPTION);
        setRefDateInput(DEFAULT_DATE_OPTION);
        setError(`读取日期列表失败: ${String(e)}`);
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

  function getRowDetailsTradeDate(row: OverviewRow) {
    return (
      (typeof row.trade_date === "string" && row.trade_date.trim() !== ""
        ? row.trade_date.trim()
        : null) ??
      lastConfig?.rankDate ??
      (rankDateInput.trim() || null)
    );
  }

  async function onRead() {
    if (!sourcePathTrimmed) {
      setError("请先到“数据管理”页完成数据准备");
      return;
    }

    let limit: number | undefined;
    const limitRaw = limitInput.trim();
    if (limitRaw) {
      const parsedLimit = Number(limitRaw);
      if (!Number.isInteger(parsedLimit) || parsedLimit <= 0) {
        setError("限制行数必须是正整数");
        return;
      }
      limit = parsedLimit;
    }

    let totalMvMin: number | undefined;
    const minRaw = totalMvMinInput.trim();
    if (minRaw) {
      const parsedMin = Number(minRaw);
      if (!Number.isFinite(parsedMin)) {
        setError("总市值最小值必须是数字");
        return;
      }
      totalMvMin = parsedMin;
    }

    let totalMvMax: number | undefined;
    const maxRaw = totalMvMaxInput.trim();
    if (maxRaw) {
      const parsedMax = Number(maxRaw);
      if (!Number.isFinite(parsedMax)) {
        setError("总市值最大值必须是数字");
        return;
      }
      totalMvMax = parsedMax;
    }

    if (
      totalMvMin !== undefined &&
      totalMvMax !== undefined &&
      totalMvMin > totalMvMax
    ) {
      setError("总市值最小值不能大于最大值");
      return;
    }

    const query: OverviewPageQuery = {
      sourcePath: sourcePathTrimmed,
      rankDate: rankDateInput.trim() || undefined,
      refDate: refDateInput.trim() || undefined,
      limit,
      board: boardFilter === "全部" ? undefined : boardFilter,
      totalMvMin,
      totalMvMax,
    };

    setLoading(true);
    setError("");
    try {
      const data = await rankOverviewPage(query);
      const normalizedRows = normalizeRows(data.rows ?? []);
      const nextDateOptions = normalizeTradeDates(data.rank_date_options ?? []);
      const resolvedRankDate =
        data.resolved_rank_date?.trim() ||
        findFirstPopulatedString(normalizedRows, "trade_date") ||
        query.rankDate ||
        null;
      const resolvedRefDate =
        data.resolved_ref_date?.trim() ||
        findFirstPopulatedString(normalizedRows, "ref_date") ||
        query.refDate ||
        null;

      if (nextDateOptions.length > 0) {
        setDateOptions(nextDateOptions);
        setRankDateInput(
          resolvedRankDate ?? pickDateValue(rankDateInput, nextDateOptions),
        );
        setRefDateInput(
          resolvedRefDate ?? pickDateValue(refDateInput, nextDateOptions),
        );
      } else {
        if (resolvedRankDate) {
          setRankDateInput(resolvedRankDate);
        }
        if (resolvedRefDate) {
          setRefDateInput(resolvedRefDate);
        }
      }

      setRows(normalizedRows);
      setLastConfig({
        rankDate: resolvedRankDate,
        refDate: resolvedRefDate,
        limit: limit ?? null,
        board: query.board ?? null,
        totalMvMin: totalMvMin ?? null,
        totalMvMax: totalMvMax ?? null,
        rowCount: normalizedRows.length,
      });
    } catch (e) {
      setError(`读取失败: ${String(e)}`);
      setRows([]);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="overview-page">
      <section className="overview-card">
        <h2 className="overview-title">排名总览</h2>
        <div className="overview-source-note">
          当前数据目录：
          {sourcePathTrimmed || "读取中..."}
        </div>

        <div className="overview-form-grid">
          <label className="overview-field">
            <span>排名日期</span>
            <select
              value={rankDateInput}
              onChange={(e) => setRankDateInput(e.target.value)}
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
            <span>参考日</span>
            <select
              value={refDateInput}
              onChange={(e) => setRefDateInput(e.target.value)}
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
              onChange={(e) => setLimitInput(e.target.value)}
              placeholder="100"
            />
          </label>

          <label className="overview-field">
            <span>板块筛选</span>
            <select
              value={boardFilter}
              onChange={(e) =>
                setBoardFilter(e.target.value as (typeof BOARD_OPTIONS)[number])
              }
            >
              {BOARD_OPTIONS.map((board) => (
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
              onChange={(e) => setTotalMvMinInput(e.target.value)}
              placeholder="留空=不限"
            />
          </label>

          <label className="overview-field">
            <span>总市值最大(亿)</span>
            <input
              type="number"
              step={0.01}
              value={totalMvMaxInput}
              onChange={(e) => setTotalMvMaxInput(e.target.value)}
              placeholder="留空=不限"
            />
          </label>
        </div>

        <div className="overview-actions">
          <button
            className="overview-read-btn"
            type="button"
            onClick={onRead}
            disabled={loading || dateOptionsLoading || sourcePathTrimmed === ""}
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
              <strong>{lastConfig.rankDate ?? "最新"}</strong>
            </div>
            <div className="overview-summary-item">
              <span>参考日</span>
              <strong>{lastConfig.refDate ?? "最新"}</strong>
            </div>
            <div className="overview-summary-item">
              <span>限制行数</span>
              <strong>{lastConfig.limit ?? "不限"}</strong>
            </div>
            <div className="overview-summary-item">
              <span>板块筛选</span>
              <strong>{lastConfig.board ?? "不限"}</strong>
            </div>
            <div className="overview-summary-item">
              <span>总市值范围</span>
              <strong>
                {lastConfig.totalMvMin ?? "-"} ~ {lastConfig.totalMvMax ?? "-"}{" "}
                亿
              </strong>
            </div>
            <div className="overview-summary-item">
              <span>返回行数</span>
              <strong>{lastConfig.rowCount}</strong>
            </div>
          </div>
        </section>
      ) : null}

      <section className="overview-card">
        <h3 className="overview-subtitle">结果表格</h3>
        {rows.length === 0 ? (
          <div className="overview-empty">暂无数据</div>
        ) : (
          <div className="overview-table-wrap" ref={tableWrapRef}>
            <table
              className="overview-table"
              style={{ minWidth: `${tableMinWidth}px` }}
            >
              <colgroup>
                {visibleColumns.map((key) => {
                  const columnConfig = COLUMN_CONFIG[key];
                  return (
                    <col
                      key={key}
                      style={
                        columnConfig?.isFlexible
                          ? undefined
                          : { width: `${getColumnWidth(key)}px` }
                      }
                    />
                  );
                })}
              </colgroup>
              <thead>
                <tr>
                  {visibleColumns.map((key) => {
                    const columnConfig = COLUMN_CONFIG[key];
                    if (!isOverviewSortableColumn(key)) {
                      return <th key={key}>{columnConfig?.label ?? key}</th>;
                    }
                    const isActive = sortKey === key && sortDirection !== null;
                    return (
                      <th
                        key={key}
                        aria-sort={getAriaSort(isActive, sortDirection)}
                      >
                        <TableSortButton
                          label={columnConfig?.label ?? key}
                          isActive={isActive}
                          direction={sortDirection}
                          onClick={() => toggleSort(key)}
                          title={`按${columnConfig?.label ?? key}排序`}
                        />
                      </th>
                    );
                  })}
                </tr>
              </thead>
              <tbody>
                {sortedRows.map((row, rowIndex) => (
                  <tr
                    key={`${row.ts_code}-${row.trade_date ?? row.ref_date ?? rowIndex}`}
                  >
                    {visibleColumns.map((key) => {
                      const cellValue = formatCell(
                        key,
                        row[key],
                        excludedConcepts,
                      );
                      const cellClassName = getCellClassName(key, row[key]);
                      return (
                        <td
                          key={`${row.ts_code}-${rowIndex}-${key}`}
                          className={cellClassName || undefined}
                          title={cellValue}
                        >
                          {key === "name" && cellValue !== "--" ? (
                            <DetailsLink
                              className="overview-stock-link"
                              tsCode={row.ts_code}
                              tradeDate={getRowDetailsTradeDate(row)}
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
