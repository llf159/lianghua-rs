import { useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import {
  getBoardAnalysisGroupDetail,
  getBoardAnalysisPage,
  type BoardAnalysisGroupDetail,
  type BoardAnalysisGroupRow,
  type BoardAnalysisPageData,
  type BoardAnalysisStockRow,
} from "../../apis/boardAnalysis";
import { listRankTradeDates } from "../../apis/reader";
import DetailsLink from "../../shared/DetailsLink";
import {
  filterConceptItems,
  formatConceptText,
  useConceptExclusions,
} from "../../shared/conceptExclusions";
import {
  readJsonStorage,
  readStoredSourcePath,
  writeJsonStorage,
} from "../../shared/storage";
import { useRouteScrollRegion } from "../../shared/routeScroll";
import {
  DEFAULT_DATE_OPTION,
  normalizeTradeDates,
  pickDateValue,
} from "../../shared/tradeDate";
import {
  TableSortButton,
  getAriaSort,
  type SortDefinition,
  useTableSort,
} from "../../shared/tableSort";
import "./css/BoardAnalysisPage.css";

const BOARD_ANALYSIS_STATE_KEY = "lh_board_analysis_page_v1";
const BOARD_ANALYSIS_MODAL_STATE_KEY = "lh_board_analysis_modal_state_v1";
const DEFAULT_WEIGHTING_RANGE_START = "1";
const DEFAULT_WEIGHTING_RANGE_END = "200";
const DEFAULT_BACKTEST_PERIOD_DAYS = "5";
const INDUSTRY_DISPLAY_LIMIT = 10;
const CONCEPT_DISPLAY_LIMIT = 12;

type SubmittedBoardAnalysisQuery = {
  sourcePath: string;
  refDate: string;
  weightingRangeStart: number;
  weightingRangeEnd: number;
  backtestPeriodDays: number;
};

type PersistedBoardAnalysisState = {
  sourcePath: string;
  dateOptions: string[];
  refDateInput: string;
  weightingRangeStartInput: string;
  weightingRangeEndInput: string;
  backtestPeriodInput: string;
  pageData: BoardAnalysisPageData | null;
  submittedQuery: SubmittedBoardAnalysisQuery | null;
};

type DetailSelection = {
  groupKind: "industry" | "concept";
  metricKind: "strength" | "return";
  groupName: string;
};

type PersistedBoardAnalysisModalState = {
  detailSelection: DetailSelection | null;
  detailData: BoardAnalysisGroupDetail | null;
  detailError: string;
  detailRequestKey: string | null;
};

type DetailSortKey = "rank" | "total_score" | "strength_weight" | "return_pct";

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

function formatSignedPercent(value?: number | null, digits = 2) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  return `${value >= 0 ? "+" : ""}${value.toFixed(digits)}%`;
}

function formatPlainPercent(value?: number | null, digits = 1) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  return `${value.toFixed(digits)}%`;
}

function metricValueClass(value?: number | null) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "";
  }
  if (value > 0) {
    return "board-analysis-value-up";
  }
  if (value < 0) {
    return "board-analysis-value-down";
  }
  return "board-analysis-value-flat";
}

function matchesSelection(
  currentSelection: DetailSelection | null,
  groupKind: DetailSelection["groupKind"],
  metricKind: DetailSelection["metricKind"],
  groupName: string,
) {
  return (
    currentSelection?.groupKind === groupKind &&
    currentSelection?.metricKind === metricKind &&
    currentSelection?.groupName === groupName
  );
}

function isVisibleConceptRow(
  row: BoardAnalysisGroupRow,
  excludedConcepts: readonly string[],
) {
  return filterConceptItems([row.name], excludedConcepts).length > 0;
}

function buildDetailRequestKey(
  query: SubmittedBoardAnalysisQuery,
  selection: DetailSelection,
) {
  return [
    query.sourcePath,
    query.refDate,
    query.weightingRangeStart,
    query.weightingRangeEnd,
    query.backtestPeriodDays,
    selection.groupKind,
    selection.metricKind,
    selection.groupName,
  ].join("|");
}

function SummaryStrip({
  pageData,
  excludedConcepts,
}: {
  pageData: BoardAnalysisPageData | null;
  excludedConcepts: readonly string[];
}) {
  return (
      <div className="board-analysis-summary-grid">
      <div className="board-analysis-summary-item">
        <span>参考日</span>
        <strong>{formatDateLabel(pageData?.resolved_ref_date)}</strong>
      </div>
      <div className="board-analysis-summary-item">
        <span>回测区间</span>
        <strong>
          {formatDateLabel(pageData?.resolved_backtest_start_date)} 至{" "}
          {formatDateLabel(pageData?.resolved_ref_date)}
        </strong>
      </div>
      <div className="board-analysis-summary-item">
        <span>排名加权区间</span>
        <strong>
          #{formatNumber(pageData?.weighting_range_start, 0)} - #
          {formatNumber(pageData?.weighting_range_end, 0)}
        </strong>
      </div>
      <div className="board-analysis-summary-item">
        <span>排名样本 / 回测样本</span>
        <strong>
          {formatNumber(pageData?.summary?.rank_sample_count, 0)} /{" "}
          {formatNumber(pageData?.summary?.return_sample_count, 0)}
        </strong>
      </div>
      <div className="board-analysis-summary-item">
        <span>已排除概念</span>
        <strong>{excludedConcepts.length}</strong>
      </div>
    </div>
  );
}

function LeaderboardPanel({
  title,
  subtitle,
  rows,
  groupKind,
  metricKind,
  activeSelection,
  onSelect,
  rowLabel,
  note,
}: {
  title: string;
  subtitle: string;
  rows: BoardAnalysisGroupRow[];
  groupKind: DetailSelection["groupKind"];
  metricKind: DetailSelection["metricKind"];
  activeSelection: DetailSelection | null;
  onSelect: (selection: DetailSelection) => void;
  rowLabel: string;
  note?: string;
}) {
  return (
    <section className="board-analysis-panel-card">
        <div className="board-analysis-panel-head">
          <div>
            <h3>{title}</h3>
            {subtitle ? <p>{subtitle}</p> : null}
          </div>
          {note ? <span className="board-analysis-panel-note">{note}</span> : null}
        </div>

      {rows.length === 0 ? (
        <div className="board-analysis-empty-soft">当前条件下没有可展示的数据。</div>
      ) : (
        <div className="board-analysis-table-wrap">
          <table className="board-analysis-table">
            <thead>
              <tr>
                <th>#</th>
                <th>{rowLabel}</th>
                <th>{metricKind === "strength" ? "强度" : "均涨"}</th>
                <th>{metricKind === "strength" ? "样本" : "红盘"}</th>
                <th>龙头</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row, index) => {
                const active = matchesSelection(
                  activeSelection,
                  groupKind,
                  metricKind,
                  row.name,
                );
                return (
                  <tr
                    key={`${groupKind}-${metricKind}-${row.name}`}
                    className={active ? "is-active" : ""}
                  >
                    <td>{index + 1}</td>
                    <td>
                      <button
                        type="button"
                        className="board-analysis-row-button"
                        onClick={() =>
                          onSelect({
                            groupKind,
                            metricKind,
                            groupName: row.name,
                          })
                        }
                      >
                        <strong>{row.name}</strong>
                        <span>
                          均排 {formatNumber(row.avg_rank, 1)} · 共{" "}
                          {formatNumber(row.sample_count, 0)} 只
                        </span>
                      </button>
                    </td>
                    <td
                      className={
                        metricKind === "return"
                          ? metricValueClass(row.avg_return_pct)
                          : ""
                      }
                    >
                      <div className="board-analysis-cell-main">
                        {metricKind === "strength"
                          ? formatPlainPercent(row.strength_score_pct)
                          : formatSignedPercent(row.avg_return_pct)}
                      </div>
                      <div className="board-analysis-cell-sub">
                        {metricKind === "strength"
                          ? `均涨 ${formatSignedPercent(row.avg_return_pct)}`
                          : `强度 ${formatPlainPercent(row.strength_score_pct)}`}
                      </div>
                    </td>
                    <td>
                      <div className="board-analysis-cell-main">
                        {metricKind === "strength"
                          ? formatNumber(row.sample_count, 0)
                          : formatPlainPercent(row.up_ratio_pct)}
                      </div>
                      <div className="board-analysis-cell-sub">
                        {metricKind === "strength"
                          ? `红盘 ${formatPlainPercent(row.up_ratio_pct)}`
                          : `样本 ${formatNumber(row.sample_count, 0)}`}
                      </div>
                    </td>
                    <td>
                      <div className="board-analysis-cell-main">
                        {row.leader_stock_name ??
                          row.leader_stock_ts_code ??
                          "--"}
                      </div>
                      <div className="board-analysis-cell-sub">
                        {metricKind === "strength"
                          ? `最高排位 #${formatNumber(row.top_rank, 0)}`
                          : formatSignedPercent(row.leader_stock_return_pct)}
                      </div>
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

function ConstituentsTable({
  detailData,
  rows,
  excludedConcepts,
  sourcePath,
}: {
  detailData: BoardAnalysisGroupDetail;
  rows: BoardAnalysisStockRow[];
  excludedConcepts: readonly string[];
  sourcePath: string;
}) {
  const sortDefinitions = useMemo(
    () =>
      ({
        rank: { value: (row) => row.rank },
        total_score: { value: (row) => row.total_score },
        strength_weight: { value: (row) => row.strength_weight },
        return_pct: { value: (row) => row.return_pct },
      }) satisfies Partial<
        Record<DetailSortKey, SortDefinition<BoardAnalysisStockRow>>
      >,
    [],
  );
  const { sortKey, sortDirection, sortedRows, toggleSort } = useTableSort<
    BoardAnalysisStockRow,
    DetailSortKey
  >(rows, sortDefinitions, {
    key: detailData.metric_kind === "strength" ? "rank" : "return_pct",
    direction: detailData.metric_kind === "strength" ? "asc" : "desc",
  });
  const tableWrapRef = useRouteScrollRegion<HTMLDivElement>(
    `board-analysis-detail-table:${detailData.group_kind}:${detailData.metric_kind}:${detailData.group_name}`,
    [sortedRows.length, detailData.resolved_ref_date],
  );
  const detailNavigationItems = sortedRows.map((row) => ({
    tsCode: row.ts_code,
    tradeDate: detailData.resolved_ref_date ?? undefined,
    sourcePath: sourcePath || undefined,
    name: row.name ?? undefined,
  }));

  return (
    <div className="board-analysis-detail-table-wrap" ref={tableWrapRef}>
      <table className="board-analysis-detail-table">
        <thead>
          <tr>
            <th>股票</th>
            <th aria-sort={getAriaSort(sortKey === "rank", sortDirection)}>
              <TableSortButton
                label="排名"
                isActive={sortKey === "rank"}
                direction={sortDirection}
                onClick={() => toggleSort("rank")}
                title="按参考日排名排序"
              />
            </th>
            <th aria-sort={getAriaSort(sortKey === "total_score", sortDirection)}>
              <TableSortButton
                label="总分"
                isActive={sortKey === "total_score"}
                direction={sortDirection}
                onClick={() => toggleSort("total_score")}
                title="按参考日总分排序"
              />
            </th>
            <th
              aria-sort={getAriaSort(
                sortKey === "strength_weight",
                sortDirection,
              )}
            >
              <TableSortButton
                label="权重"
                isActive={sortKey === "strength_weight"}
                direction={sortDirection}
                onClick={() => toggleSort("strength_weight")}
                title="按排名非线性权重排序"
              />
            </th>
            <th aria-sort={getAriaSort(sortKey === "return_pct", sortDirection)}>
              <TableSortButton
                label="涨幅"
                isActive={sortKey === "return_pct"}
                direction={sortDirection}
                onClick={() => toggleSort("return_pct")}
                title="按回测区间涨幅排序"
              />
            </th>
          </tr>
        </thead>
        <tbody>
          {sortedRows.map((row) => (
            <tr key={row.ts_code}>
              <td>
                <DetailsLink
                  className="board-analysis-link"
                  tsCode={row.ts_code}
                  tradeDate={detailData.resolved_ref_date ?? undefined}
                  sourcePath={sourcePath}
                  navigationItems={detailNavigationItems}
                >
                  {row.name ?? row.ts_code}
                </DetailsLink>
                <div className="board-analysis-cell-sub">
                  {row.ts_code} · {row.market_board} · {row.industry ?? "--"}
                </div>
                <div className="board-analysis-cell-sub">
                  {formatConceptText(row.concept, excludedConcepts, "无概念")}
                </div>
              </td>
              <td>{formatNumber(row.rank, 0)}</td>
              <td>{formatNumber(row.total_score)}</td>
              <td>{formatNumber(row.strength_weight)}</td>
              <td className={metricValueClass(row.return_pct)}>
                {formatSignedPercent(row.return_pct)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function DetailPanel({
  selection,
  detailData,
  loading,
  error,
  onClose,
  sourcePath,
  excludedConcepts,
}: {
  selection: DetailSelection | null;
  detailData: BoardAnalysisGroupDetail | null;
  loading: boolean;
  error: string;
  onClose: () => void;
  sourcePath: string;
  excludedConcepts: readonly string[];
}) {
  if (!selection || typeof document === "undefined") {
    return null;
  }
  const modalShellRef = useRouteScrollRegion<HTMLElement>(
    `board-analysis-detail-modal:${selection.groupKind}:${selection.metricKind}:${selection.groupName}`,
    [loading, error, detailData?.resolved_ref_date],
  );

  const summary = detailData?.summary;
  const detailTitle =
    detailData?.group_name ?? selection.groupName;
  const detailGroupLabel =
    detailData?.group_kind ?? selection.groupKind;
  const detailMetricLabel =
    detailData?.metric_kind ?? selection.metricKind;

  return createPortal(
    <div
      className="board-analysis-modal-backdrop"
      role="presentation"
      onClick={onClose}
    >
      <aside
        className="board-analysis-detail-card board-analysis-detail-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="board-analysis-detail-title"
        onClick={(event) => event.stopPropagation()}
        ref={modalShellRef}
      >
        <div className="board-analysis-modal-close-wrap">
          <button
            type="button"
            className="board-analysis-modal-close"
            onClick={onClose}
            aria-label="关闭成分股明细"
          >
            关闭
          </button>
        </div>

        <div className="board-analysis-panel-head">
          <div>
            <h3>成分股明细</h3>
          </div>
        </div>

        {loading ? (
          <div className="board-analysis-empty-soft">正在读取成分股...</div>
        ) : error ? (
          <div className="board-analysis-error">{error}</div>
        ) : !detailData ? (
          <div className="board-analysis-empty-soft">
            没有读取到当前分组的成分股。
          </div>
        ) : (
          <>
            <div className="board-analysis-detail-head">
              <div>
                <div className="board-analysis-detail-eyebrow">
                  {detailGroupLabel === "industry" ? "板块" : "概念"} ·{" "}
                  {detailMetricLabel === "strength" ? "排名强度榜" : "涨幅榜"}
                </div>
                <h4 id="board-analysis-detail-title">{detailTitle}</h4>
              </div>
              <div className="board-analysis-detail-period">
                {formatDateLabel(detailData.resolved_backtest_start_date)} 至{" "}
                {formatDateLabel(detailData.resolved_ref_date)}
              </div>
            </div>

            <div className="board-analysis-detail-summary-grid">
              <div className="board-analysis-detail-summary-item">
                <span>成分数量</span>
                <strong>{formatNumber(summary?.sample_count, 0)}</strong>
              </div>
              <div className="board-analysis-detail-summary-item">
                <span>平均强度</span>
                <strong>{formatPlainPercent(summary?.strength_score_pct)}</strong>
              </div>
              <div className="board-analysis-detail-summary-item">
                <span>区间均涨</span>
                <strong className={metricValueClass(summary?.avg_return_pct)}>
                  {formatSignedPercent(summary?.avg_return_pct)}
                </strong>
              </div>
              <div className="board-analysis-detail-summary-item">
                <span>红盘占比</span>
                <strong>{formatPlainPercent(summary?.up_ratio_pct)}</strong>
              </div>
            </div>

            <ConstituentsTable
              key={`${detailData.group_kind}-${detailData.metric_kind}-${detailData.group_name}`}
              detailData={detailData}
              rows={detailData.stocks}
              excludedConcepts={excludedConcepts}
              sourcePath={sourcePath}
            />
          </>
        )}
      </aside>
    </div>,
    document.body,
  );
}

export default function BoardAnalysisPage() {
  const { excludedConcepts } = useConceptExclusions();
  const persistedState = useMemo(
    () =>
      typeof window === "undefined"
        ? null
        : readJsonStorage<Partial<PersistedBoardAnalysisState>>(
            window.localStorage,
            BOARD_ANALYSIS_STATE_KEY,
          ),
    [],
  );
  const persistedModalState = useMemo(
    () =>
      typeof window === "undefined"
        ? null
        : readJsonStorage<Partial<PersistedBoardAnalysisModalState>>(
            window.sessionStorage,
            BOARD_ANALYSIS_MODAL_STATE_KEY,
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
  const [refDateInput, setRefDateInput] = useState(
    persistedState?.refDateInput ?? DEFAULT_DATE_OPTION,
  );
  const [weightingRangeStartInput, setWeightingRangeStartInput] = useState(
    persistedState?.weightingRangeStartInput ?? DEFAULT_WEIGHTING_RANGE_START,
  );
  const [weightingRangeEndInput, setWeightingRangeEndInput] = useState(
    persistedState?.weightingRangeEndInput ?? DEFAULT_WEIGHTING_RANGE_END,
  );
  const [backtestPeriodInput, setBacktestPeriodInput] = useState(
    persistedState?.backtestPeriodInput ?? DEFAULT_BACKTEST_PERIOD_DAYS,
  );
  const [pageData, setPageData] = useState<BoardAnalysisPageData | null>(
    () =>
      persistedState?.pageData && typeof persistedState.pageData === "object"
        ? (persistedState.pageData as BoardAnalysisPageData)
        : null,
  );
  const [submittedQuery, setSubmittedQuery] =
    useState<SubmittedBoardAnalysisQuery | null>(() => {
      const query = persistedState?.submittedQuery;
      if (!query || typeof query !== "object") {
        return null;
      }

      const sourcePath =
        typeof query.sourcePath === "string" ? query.sourcePath.trim() : "";
      const refDate = typeof query.refDate === "string" ? query.refDate.trim() : "";
      const weightingRangeStart =
        typeof query.weightingRangeStart === "number" &&
        Number.isInteger(query.weightingRangeStart)
          ? query.weightingRangeStart
          : 0;
      const weightingRangeEnd =
        typeof query.weightingRangeEnd === "number" &&
        Number.isInteger(query.weightingRangeEnd)
          ? query.weightingRangeEnd
          : 0;
      const backtestPeriodDays =
        typeof query.backtestPeriodDays === "number" &&
        Number.isInteger(query.backtestPeriodDays)
          ? query.backtestPeriodDays
          : 0;

      if (
        !sourcePath ||
        !refDate ||
        weightingRangeStart <= 0 ||
        weightingRangeEnd <= 0 ||
        backtestPeriodDays <= 0
      ) {
        return null;
      }

      return {
        sourcePath,
        refDate,
        weightingRangeStart,
        weightingRangeEnd,
        backtestPeriodDays,
      };
    });
  const [loading, setLoading] = useState(false);
  const [dateLoading, setDateLoading] = useState(false);
  const [error, setError] = useState("");
  const [detailSelection, setDetailSelection] = useState<DetailSelection | null>(
    () =>
      persistedModalState?.detailSelection &&
      typeof persistedModalState.detailSelection === "object"
        ? (persistedModalState.detailSelection as DetailSelection)
        : null,
  );
  const [detailData, setDetailData] = useState<BoardAnalysisGroupDetail | null>(
    () =>
      persistedModalState?.detailData &&
      typeof persistedModalState.detailData === "object"
        ? (persistedModalState.detailData as BoardAnalysisGroupDetail)
        : null,
  );
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState(
    () => persistedModalState?.detailError ?? "",
  );
  const [detailRequestKey, setDetailRequestKey] = useState<string | null>(
    () =>
      typeof persistedModalState?.detailRequestKey === "string"
        ? persistedModalState.detailRequestKey
        : null,
  );
  const sourcePathRef = useRef(sourcePath.trim());
  const skipInitialPageReloadRef = useRef(
    Boolean(persistedState?.pageData && persistedState?.submittedQuery),
  );

  const sourcePathTrimmed = sourcePath.trim();

  const buildSubmittedQuery = (
    nextSourcePath: string,
    nextRefDate: string,
    nextWeightingRangeStartInput: string,
    nextWeightingRangeEndInput: string,
    nextBacktestPeriodInput: string,
  ): SubmittedBoardAnalysisQuery | null => {
    if (!nextSourcePath || !nextRefDate) {
      return null;
    }

    const weightingRangeStart = Number(nextWeightingRangeStartInput);
    const weightingRangeEnd = Number(nextWeightingRangeEndInput);
    const backtestPeriodDays = Number(nextBacktestPeriodInput);
    if (
      !Number.isInteger(weightingRangeStart) ||
      !Number.isInteger(weightingRangeEnd) ||
      !Number.isInteger(backtestPeriodDays) ||
      weightingRangeStart <= 0 ||
      weightingRangeEnd <= 0 ||
      backtestPeriodDays <= 0
    ) {
      return null;
    }

    return {
      sourcePath: nextSourcePath,
      refDate: nextRefDate,
      weightingRangeStart,
      weightingRangeEnd,
      backtestPeriodDays,
    };
  };

  const applyFilters = () => {
    const nextQuery = buildSubmittedQuery(
      sourcePathTrimmed,
      refDateInput,
      weightingRangeStartInput,
      weightingRangeEndInput,
      backtestPeriodInput,
    );
    if (!nextQuery) {
      setError("排名区间和回测周期都必须是正整数");
      return;
    }
    setError("");
    setSubmittedQuery(nextQuery);
  };

  const hasPendingChanges =
    submittedQuery === null ||
    submittedQuery.sourcePath !== sourcePathTrimmed ||
    submittedQuery.refDate !== refDateInput ||
    submittedQuery.weightingRangeStart !== Number(weightingRangeStartInput) ||
    submittedQuery.weightingRangeEnd !== Number(weightingRangeEndInput) ||
    submittedQuery.backtestPeriodDays !== Number(backtestPeriodInput);

  const visibleIndustryStrengthRows = useMemo(
    () =>
      (pageData?.industry_strength_rows ?? []).slice(0, INDUSTRY_DISPLAY_LIMIT),
    [pageData],
  );
  const visibleConceptStrengthRows = useMemo(
    () =>
      (pageData?.concept_strength_rows ?? [])
        .filter((row) => isVisibleConceptRow(row, excludedConcepts))
        .slice(0, CONCEPT_DISPLAY_LIMIT),
    [excludedConcepts, pageData],
  );
  const visibleIndustryReturnRows = useMemo(
    () =>
      (pageData?.industry_return_rows ?? []).slice(0, INDUSTRY_DISPLAY_LIMIT),
    [pageData],
  );
  const visibleConceptReturnRows = useMemo(
    () =>
      (pageData?.concept_return_rows ?? [])
        .filter((row) => isVisibleConceptRow(row, excludedConcepts))
        .slice(0, CONCEPT_DISPLAY_LIMIT),
    [excludedConcepts, pageData],
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
    if (typeof window === "undefined") {
      return;
    }
    writeJsonStorage(window.localStorage, BOARD_ANALYSIS_STATE_KEY, {
      sourcePath: sourcePathTrimmed,
      dateOptions,
      refDateInput,
      weightingRangeStartInput,
      weightingRangeEndInput,
      backtestPeriodInput,
      pageData,
      submittedQuery,
    } satisfies PersistedBoardAnalysisState);
  }, [
    backtestPeriodInput,
    dateOptions,
    pageData,
    refDateInput,
    sourcePathTrimmed,
    submittedQuery,
    weightingRangeEndInput,
    weightingRangeStartInput,
  ]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    writeJsonStorage(window.sessionStorage, BOARD_ANALYSIS_MODAL_STATE_KEY, {
      detailSelection,
      detailData,
      detailError,
      detailRequestKey,
    } satisfies PersistedBoardAnalysisModalState);
  }, [detailData, detailError, detailRequestKey, detailSelection]);

  useEffect(() => {
    if (!sourcePathTrimmed) {
      setDateOptions([]);
      setSubmittedQuery(null);
      setPageData(null);
      setDetailSelection(null);
      setDetailData(null);
      setDetailError("");
      setDetailRequestKey(null);
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
        setRefDateInput((current) => pickDateValue(current, values));
        setError("");
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
    if (
      submittedQuery !== null ||
      dateLoading ||
      !sourcePathTrimmed ||
      !refDateInput
    ) {
      return;
    }

    const nextQuery = buildSubmittedQuery(
      sourcePathTrimmed,
      refDateInput,
      weightingRangeStartInput,
      weightingRangeEndInput,
      backtestPeriodInput,
    );
    if (nextQuery) {
      setSubmittedQuery(nextQuery);
    }
  }, [
    backtestPeriodInput,
    dateLoading,
    refDateInput,
    sourcePathTrimmed,
    submittedQuery,
    weightingRangeEndInput,
    weightingRangeStartInput,
  ]);

  useEffect(() => {
    if (!submittedQuery) {
      return;
    }

    if (skipInitialPageReloadRef.current && pageData) {
      skipInitialPageReloadRef.current = false;
      return;
    }

    let cancelled = false;
    setDetailSelection(null);
    setDetailData(null);
    setDetailError("");
    setDetailRequestKey(null);

    const load = async () => {
      setLoading(true);
      setError("");
      try {
        const nextPageData = await getBoardAnalysisPage({
          sourcePath: submittedQuery.sourcePath,
          refDate: submittedQuery.refDate,
          weightingRangeStart: submittedQuery.weightingRangeStart,
          weightingRangeEnd: submittedQuery.weightingRangeEnd,
          backtestPeriodDays: submittedQuery.backtestPeriodDays,
        });
        if (!cancelled) {
          setPageData(nextPageData);
        }
      } catch (loadError) {
        if (!cancelled) {
          setPageData(null);
          setError(`读取板块分析失败: ${String(loadError)}`);
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

  useEffect(() => {
    if (!detailSelection || !submittedQuery) {
      return;
    }

    const nextDetailRequestKey = buildDetailRequestKey(
      submittedQuery,
      detailSelection,
    );
    if (
      detailRequestKey === nextDetailRequestKey &&
      (detailData !== null || detailError !== "")
    ) {
      setDetailLoading(false);
      return;
    }

    let cancelled = false;
    const load = async () => {
      setDetailLoading(true);
      setDetailError("");
      try {
        const nextDetailData = await getBoardAnalysisGroupDetail({
          sourcePath: submittedQuery.sourcePath,
          refDate: submittedQuery.refDate,
          weightingRangeStart: submittedQuery.weightingRangeStart,
          weightingRangeEnd: submittedQuery.weightingRangeEnd,
          backtestPeriodDays: submittedQuery.backtestPeriodDays,
          groupKind: detailSelection.groupKind,
          metricKind: detailSelection.metricKind,
          groupName: detailSelection.groupName,
        });
        if (!cancelled) {
          setDetailData(nextDetailData);
          setDetailRequestKey(nextDetailRequestKey);
        }
      } catch (loadError) {
        if (!cancelled) {
          setDetailData(null);
          setDetailError(`读取成分股失败: ${String(loadError)}`);
          setDetailRequestKey(nextDetailRequestKey);
        }
      } finally {
        if (!cancelled) {
          setDetailLoading(false);
        }
      }
    };

    void load();
    return () => {
      cancelled = true;
    };
  }, [detailData, detailError, detailRequestKey, detailSelection, submittedQuery]);

  useEffect(() => {
    if (!detailSelection || typeof document === "undefined") {
      return;
    }

    const previousOverflow = document.body.style.overflow;
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setDetailSelection(null);
      }
    };

    document.body.style.overflow = "hidden";
    window.addEventListener("keydown", handleKeyDown);
    return () => {
      document.body.style.overflow = previousOverflow;
      window.removeEventListener("keydown", handleKeyDown);
    };
  }, [detailSelection]);

  return (
    <div className="board-analysis-page">
      <section className="board-analysis-card">
        <div className="board-analysis-head">
          <div>
            <h2>板块分析</h2>
          </div>
        </div>

        <div className="board-analysis-form-grid">
          <label className="board-analysis-field">
            <span>参考日</span>
            <select
              value={refDateInput}
              onChange={(event) => setRefDateInput(event.target.value)}
              disabled={dateLoading || dateOptions.length === 0}
            >
              {dateOptions.map((option) => (
                <option key={option} value={option}>
                  {formatDateLabel(option)}
                </option>
              ))}
            </select>
          </label>

          <label className="board-analysis-field">
            <span>排名非线性加权区间</span>
            <div className="board-analysis-range-fields">
              <input
                type="number"
                min={1}
                step={1}
                value={weightingRangeStartInput}
                onChange={(event) =>
                  setWeightingRangeStartInput(event.target.value)
                }
              />
              <span>至</span>
              <input
                type="number"
                min={1}
                step={1}
                value={weightingRangeEndInput}
                onChange={(event) =>
                  setWeightingRangeEndInput(event.target.value)
                }
              />
            </div>
          </label>

          <label className="board-analysis-field">
            <span>回测周期</span>
            <input
              type="number"
              min={1}
              step={1}
              value={backtestPeriodInput}
              onChange={(event) => setBacktestPeriodInput(event.target.value)}
            />
            <small>按交易日计，统计从周期起点收盘到参考日收盘的涨幅。</small>
          </label>
        </div>

        <div className="board-analysis-actions">
          <button
            type="button"
            className="board-analysis-apply-button"
            onClick={applyFilters}
            disabled={loading || dateLoading}
          >
            {loading ? "统计中..." : "刷新统计"}
          </button>
          <span className="board-analysis-actions-note">
            {hasPendingChanges ? "未刷新" : "已刷新"}
          </span>
        </div>

        {error ? <div className="board-analysis-error">{error}</div> : null}

        <SummaryStrip
          pageData={pageData}
          excludedConcepts={excludedConcepts}
        />
      </section>

      {loading && !pageData ? (
        <section className="board-analysis-card">
          <div className="board-analysis-empty">加载中...</div>
        </section>
      ) : !pageData ? (
        <section className="board-analysis-card">
          <div className="board-analysis-empty">暂无结果</div>
        </section>
      ) : (
        <div className="board-analysis-sections">
          <section className="board-analysis-section">
            <div className="board-analysis-section-head">
              <div>
                <h3>排名内强度</h3>
              </div>
            </div>
            <div className="board-analysis-grid">
              <LeaderboardPanel
                title="排名内板块强度"
                subtitle=""
                rows={visibleIndustryStrengthRows}
                groupKind="industry"
                metricKind="strength"
                activeSelection={detailSelection}
                onSelect={setDetailSelection}
                rowLabel="板块"
                note={`仅展示前 ${INDUSTRY_DISPLAY_LIMIT} 个`}
              />
              <LeaderboardPanel
                title="排名内概念强度"
                subtitle=""
                rows={visibleConceptStrengthRows}
                groupKind="concept"
                metricKind="strength"
                activeSelection={detailSelection}
                onSelect={setDetailSelection}
                rowLabel="概念"
                note={`已按设置排除 ${excludedConcepts.length} 个概念，仅展示前 ${CONCEPT_DISPLAY_LIMIT} 个`}
              />
            </div>
          </section>

          <section className="board-analysis-section">
            <div className="board-analysis-section-head">
              <div>
                <h3>回测周期涨幅榜</h3>
              </div>
            </div>
            <div className="board-analysis-grid">
              <LeaderboardPanel
                title="回测周期内板块涨幅榜"
                subtitle=""
                rows={visibleIndustryReturnRows}
                groupKind="industry"
                metricKind="return"
                activeSelection={detailSelection}
                onSelect={setDetailSelection}
                rowLabel="板块"
                note={`仅展示前 ${INDUSTRY_DISPLAY_LIMIT} 个`}
              />
              <LeaderboardPanel
                title="回测周期内概念涨幅榜"
                subtitle=""
                rows={visibleConceptReturnRows}
                groupKind="concept"
                metricKind="return"
                activeSelection={detailSelection}
                onSelect={setDetailSelection}
                rowLabel="概念"
                note={`已按设置排除 ${excludedConcepts.length} 个概念，仅统计样本≥2并展示前 ${CONCEPT_DISPLAY_LIMIT} 个`}
              />
            </div>
          </section>

        </div>
      )}

      <DetailPanel
        selection={detailSelection}
        detailData={detailData}
        loading={detailLoading}
        error={detailError}
        onClose={() => {
          setDetailSelection(null);
          setDetailError("");
        }}
        sourcePath={sourcePathTrimmed}
        excludedConcepts={excludedConcepts}
      />
    </div>
  );
}
