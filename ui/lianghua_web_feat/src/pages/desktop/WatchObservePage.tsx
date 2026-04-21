import { useEffect, useMemo, useState } from "react";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import { listRankTradeDates } from "../../apis/reader";
import {
  listWatchObserveRows,
  refreshWatchObserveRows,
  removeWatchObserveRows,
  updateWatchObserveTag,
  type WatchObserveRow,
} from "../../apis/watchObserve";
import {
  formatConceptText,
  useConceptExclusions,
} from "../../shared/conceptExclusions";
import DetailsLink from "../../shared/DetailsLink";
import { splitTsCode } from "../../shared/stockCode";
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
import {
  DEFAULT_DATE_OPTION,
  normalizeTradeDates,
  pickDateValue,
} from "../../shared/tradeDate";
import "./css/WatchObservePage.css";

type ViewMode = "db" | "realtime";
type WatchObserveSortKey =
  | "latestClose"
  | "latestChangePct"
  | "volumeRatio"
  | "addedDate"
  | "postWatchReturnPct"
  | "todayRank";
const WATCH_OBSERVE_STATE_KEY = "lh_watch_observe_page_state_v1";

type PersistedWatchObserveState = {
  rows: WatchObserveRow[];
  sourcePath: string;
  dateOptions: string[];
  referenceTradeDate: string;
  viewMode: ViewMode;
  refreshedAt: string | null;
  resolvedReferenceTradeDate: string | null;
  refreshSummary: string;
};

function formatNumber(value: number | null, digits = 2) {
  if (value === null || !Number.isFinite(value)) {
    return "--";
  }
  return Number.isInteger(value) ? String(value) : value.toFixed(digits);
}

function formatPercent(value: number | null) {
  if (value === null || !Number.isFinite(value)) {
    return "--";
  }
  return `${value.toFixed(2)}%`;
}

function formatRatio(value: number | null) {
  if (value === null || !Number.isFinite(value)) {
    return "--";
  }
  return value.toFixed(2);
}

function getPercentClassName(value: number | null) {
  if (value === null || !Number.isFinite(value) || value === 0) {
    return "watch-observe-value-flat";
  }
  return value > 0 ? "watch-observe-value-up" : "watch-observe-value-down";
}

function waitForNextPaint() {
  if (typeof window === "undefined") {
    return Promise.resolve();
  }
  return new Promise<void>((resolve) => {
    window.requestAnimationFrame(() => resolve());
  });
}

export default function WatchObservePage() {
  const { excludedConcepts } = useConceptExclusions();
  const persistedState = useMemo(() => {
    const parsed = readJsonStorage<Partial<PersistedWatchObserveState>>(
      typeof window === "undefined" ? null : window.sessionStorage,
      WATCH_OBSERVE_STATE_KEY,
    );
    if (!parsed || typeof parsed !== "object") {
      return null;
    }

    return {
      rows: Array.isArray(parsed.rows)
        ? (parsed.rows as WatchObserveRow[])
        : [],
      sourcePath:
        typeof parsed.sourcePath === "string" ? parsed.sourcePath : "",
      dateOptions: Array.isArray(parsed.dateOptions)
        ? parsed.dateOptions.filter(
            (item): item is string => typeof item === "string",
          )
        : [],
      referenceTradeDate:
        typeof parsed.referenceTradeDate === "string"
          ? parsed.referenceTradeDate
          : DEFAULT_DATE_OPTION,
      viewMode: parsed.viewMode === "realtime" ? "realtime" : "db",
      refreshedAt:
        typeof parsed.refreshedAt === "string" ? parsed.refreshedAt : null,
      resolvedReferenceTradeDate:
        typeof parsed.resolvedReferenceTradeDate === "string"
          ? parsed.resolvedReferenceTradeDate
          : null,
      refreshSummary:
        typeof parsed.refreshSummary === "string" ? parsed.refreshSummary : "",
    } satisfies PersistedWatchObserveState;
  }, []);
  const [rows, setRows] = useState<WatchObserveRow[]>(
    () => persistedState?.rows ?? [],
  );
  const [sourcePath, setSourcePath] = useState(
    () => persistedState?.sourcePath ?? readStoredSourcePath(),
  );
  const [dateOptions, setDateOptions] = useState<string[]>(
    () => persistedState?.dateOptions ?? [],
  );
  const [referenceTradeDate, setReferenceTradeDate] = useState(
    () => persistedState?.referenceTradeDate ?? DEFAULT_DATE_OPTION,
  );
  const [editingTsCode, setEditingTsCode] = useState<string | null>(null);
  const [tagDraft, setTagDraft] = useState("");
  const [isDeleteMode, setIsDeleteMode] = useState(false);
  const [pendingDeleteTsCodes, setPendingDeleteTsCodes] = useState<string[]>(
    [],
  );
  const [loading, setLoading] = useState(
    () => persistedState?.viewMode !== "realtime",
  );
  const [dateOptionsLoading, setDateOptionsLoading] = useState(false);
  const [refreshingRealtime, setRefreshingRealtime] = useState(false);
  const [error, setError] = useState("");
  const [viewMode, setViewMode] = useState<ViewMode>(
    () => persistedState?.viewMode ?? "db",
  );
  const [refreshedAt, setRefreshedAt] = useState<string | null>(
    () => persistedState?.refreshedAt ?? null,
  );
  const [resolvedReferenceTradeDate, setResolvedReferenceTradeDate] = useState<
    string | null
  >(() => persistedState?.resolvedReferenceTradeDate ?? null);
  const [refreshSummary, setRefreshSummary] = useState(
    () => persistedState?.refreshSummary ?? "",
  );

  const sourcePathTrimmed = sourcePath.trim();
  const sortDefinitions = useMemo(
    () =>
      ({
        latestClose: { value: (row) => row.latestClose },
        latestChangePct: { value: (row) => row.latestChangePct },
        volumeRatio: { value: (row) => row.volumeRatio },
        addedDate: { value: (row) => row.addedDate },
        postWatchReturnPct: { value: (row) => row.postWatchReturnPct },
        todayRank: { value: (row) => row.todayRank },
      }) satisfies Partial<
        Record<WatchObserveSortKey, SortDefinition<WatchObserveRow>>
      >,
    [],
  );
  const { sortKey, sortDirection, sortedRows, toggleSort } = useTableSort<
    WatchObserveRow,
    WatchObserveSortKey
  >(rows, sortDefinitions);
  const detailNavigationItems = sortedRows.map((row) => ({
    tsCode: row.tsCode,
    tradeDate: resolvedReferenceTradeDate ?? row.tradeDate,
    sourcePath: sourcePathTrimmed || undefined,
    name: row.name || undefined,
  }));
  const tableWrapRef = useRouteScrollRegion<HTMLDivElement>(
    "watch-observe-table",
    [sortedRows.length, isDeleteMode, viewMode],
  );

  useEffect(() => {
    writeJsonStorage(
      typeof window === "undefined" ? null : window.sessionStorage,
      WATCH_OBSERVE_STATE_KEY,
      {
        rows,
        sourcePath,
        dateOptions,
        referenceTradeDate,
        viewMode,
        refreshedAt,
        resolvedReferenceTradeDate,
        refreshSummary,
      } satisfies PersistedWatchObserveState,
    );
  }, [
    dateOptions,
    referenceTradeDate,
    refreshedAt,
    refreshSummary,
    resolvedReferenceTradeDate,
    rows,
    sourcePath,
    viewMode,
  ]);

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
    if (!sourcePathTrimmed) {
      setDateOptions([]);
      setReferenceTradeDate(DEFAULT_DATE_OPTION);
      return;
    }

    let cancelled = false;
    void (async () => {
      setDateOptionsLoading(true);
      try {
        const values = normalizeTradeDates(
          await listRankTradeDates(sourcePathTrimmed),
        );
        if (cancelled) {
          return;
        }
        setDateOptions(values);
        setReferenceTradeDate((current) => pickDateValue(current, values));
      } catch {
        if (!cancelled) {
          setDateOptions([]);
          setReferenceTradeDate(DEFAULT_DATE_OPTION);
        }
      } finally {
        if (!cancelled) {
          setDateOptionsLoading(false);
        }
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [sourcePathTrimmed]);

  function applyDatabaseRows(
    nextRows: WatchObserveRow[],
    nextReferenceTradeDate = referenceTradeDate,
  ) {
    setRows(nextRows);
    setViewMode("db");
    setRefreshedAt(null);
    setResolvedReferenceTradeDate(nextReferenceTradeDate || null);
    setRefreshSummary("");
  }

  async function loadDatabaseRows() {
    setLoading(true);
    setError("");
    try {
      const nextRows = await listWatchObserveRows(
        sourcePathTrimmed,
        referenceTradeDate,
      );
      applyDatabaseRows(nextRows);
    } catch (loadError) {
      setRows([]);
      setError(`读取自选观察失败: ${String(loadError)}`);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    if (viewMode !== "db") {
      return;
    }

    void loadDatabaseRows();
  }, [referenceTradeDate, sourcePathTrimmed]);

  useEffect(() => {
    if (viewMode !== "db") {
      return;
    }

    let cancelled = false;
    const syncRows = () => {
      if (!cancelled) {
        void loadDatabaseRows();
      }
    };

    window.addEventListener("focus", syncRows);
    return () => {
      cancelled = true;
      window.removeEventListener("focus", syncRows);
    };
  }, [referenceTradeDate, sourcePathTrimmed, viewMode]);

  useEffect(() => {
    if (viewMode === "realtime") {
      setLoading(false);
    }
  }, [viewMode]);

  const topStatusText = useMemo(() => {
    if (refreshingRealtime) {
      return [
        `当前共 ${rows.length} 条`,
        "正在刷新实时行情，请稍候…",
        resolvedReferenceTradeDate
          ? `参考日 ${resolvedReferenceTradeDate}`
          : null,
      ]
        .filter(Boolean)
        .join(" | ");
    }

    if (viewMode === "realtime") {
      return [
        refreshedAt ? `最新刷新 ${refreshedAt}` : null,
        resolvedReferenceTradeDate
          ? `当前参考日 ${resolvedReferenceTradeDate}`
          : null,
        refreshSummary || null,
      ]
        .filter(Boolean)
        .join(" | ");
    }
    return [
      `当前共 ${rows.length} 条`,
      resolvedReferenceTradeDate
        ? `当前参考日 ${resolvedReferenceTradeDate}`
        : null,
      "当前展示数据库最新价格",
    ]
      .filter(Boolean)
      .join(" | ");
  }, [
    refreshedAt,
    refreshSummary,
    refreshingRealtime,
    resolvedReferenceTradeDate,
    rows.length,
    viewMode,
  ]);

  async function onRefreshRealtime() {
    setRefreshingRealtime(true);
    setError("");
    await waitForNextPaint();
    try {
      const snapshot = await refreshWatchObserveRows(
        referenceTradeDate,
        sourcePathTrimmed,
      );
      setRows(snapshot.rows);
      setViewMode("realtime");
      setRefreshedAt(snapshot.refreshedAt);
      setResolvedReferenceTradeDate(snapshot.referenceTradeDate);
      setRefreshSummary(
        [
          `实时 ${snapshot.fetchedCount}/${snapshot.effectiveCount}`,
          snapshot.requestedCount > 50 ? "已按 50 只一批分批刷新" : null,
        ]
          .filter(Boolean)
          .join(" | "),
      );
    } catch (refreshError) {
      setError(`刷新实时数据失败: ${String(refreshError)}`);
    } finally {
      setRefreshingRealtime(false);
    }
  }

  async function onRestoreDatabase() {
    await loadDatabaseRows();
  }

  function onStartEditTag(row: WatchObserveRow) {
    setEditingTsCode(row.tsCode);
    setTagDraft(row.tag);
  }

  function onCancelEditTag() {
    setEditingTsCode(null);
    setTagDraft("");
  }

  async function onSaveTag(tsCode: string) {
    try {
      await updateWatchObserveTag(tsCode, tagDraft.trim(), sourcePathTrimmed);
      const nextRows = await listWatchObserveRows(
        sourcePathTrimmed,
        referenceTradeDate,
      );
      applyDatabaseRows(nextRows);
      setError("");
      setEditingTsCode(null);
      setTagDraft("");
    } catch (saveError) {
      setError(`保存标签失败: ${String(saveError)}`);
    }
  }

  function onEnterDeleteMode() {
    setIsDeleteMode(true);
    setPendingDeleteTsCodes([]);
    setEditingTsCode(null);
    setTagDraft("");
  }

  function onTogglePendingDelete(tsCode: string) {
    setPendingDeleteTsCodes((current) =>
      current.includes(tsCode)
        ? current.filter((item) => item !== tsCode)
        : [...current, tsCode],
    );
  }

  function onCancelDeleteMode() {
    setIsDeleteMode(false);
    setPendingDeleteTsCodes([]);
  }

  async function onSaveDeleteChanges() {
    try {
      await removeWatchObserveRows(pendingDeleteTsCodes, sourcePathTrimmed);
      const nextRows = await listWatchObserveRows(
        sourcePathTrimmed,
        referenceTradeDate,
      );
      applyDatabaseRows(nextRows);
      setError("");
      setIsDeleteMode(false);
      setPendingDeleteTsCodes([]);
    } catch (removeError) {
      setError(`删除自选失败: ${String(removeError)}`);
    }
  }

  const latestPriceHeader = viewMode === "realtime" ? "实时价*" : "最新收盘价";
  const latestChangeHeader = viewMode === "realtime" ? "实时涨幅*" : "最新涨幅";
  const rankHeader = "参考日排名";

  return (
    <div className="watch-observe-page">
      <section className="watch-observe-card">
        <div className="watch-observe-section-head">
          <div>
            <h2 className="watch-observe-title">自选观察</h2>
            <div className="watch-observe-tip">{topStatusText}</div>
          </div>
        </div>
        {error ? <div className="watch-observe-empty">{error}</div> : null}
        {loading ? (
          <div className="watch-observe-empty">读取自选观察中...</div>
        ) : rows.length === 0 ? (
          <div className="watch-observe-empty">暂无自选观察数据。</div>
        ) : (
          <>
            <div className="watch-observe-table-toolbar">
              <div className="watch-observe-table-toolbar-left">
                <label className="watch-observe-filter-field">
                  <span>参考日</span>
                  <select
                    value={referenceTradeDate}
                    onChange={(event) =>
                      setReferenceTradeDate(event.target.value)
                    }
                    disabled={
                      dateOptionsLoading ||
                      dateOptions.length === 0 ||
                      refreshingRealtime
                    }
                  >
                    {dateOptions.length === 0 ? (
                      <option value="">
                        {dateOptionsLoading ? "读取中..." : "暂无可选日期"}
                      </option>
                    ) : null}
                    {dateOptions.map((option) => (
                      <option key={option} value={option}>
                        {option}
                      </option>
                    ))}
                  </select>
                </label>

                <button
                  className={[
                    "watch-observe-toolbar-btn",
                    "watch-observe-toolbar-btn-primary",
                    refreshingRealtime ? "is-loading" : "",
                  ]
                    .filter(Boolean)
                    .join(" ")}
                  type="button"
                  disabled={refreshingRealtime || isDeleteMode}
                  onClick={() => void onRefreshRealtime()}
                >
                  {refreshingRealtime ? "刷新实时中" : "刷新实时"}
                </button>

                {viewMode === "realtime" ? (
                  <button
                    className="watch-observe-toolbar-btn watch-observe-toolbar-btn-secondary"
                    type="button"
                    disabled={isDeleteMode}
                    onClick={() => void onRestoreDatabase()}
                  >
                    恢复数据库
                  </button>
                ) : null}

                {isDeleteMode ? (
                  <>
                    <button
                      className="watch-observe-toolbar-btn watch-observe-toolbar-btn-secondary"
                      type="button"
                      onClick={onCancelDeleteMode}
                    >
                      取消
                    </button>
                    <button
                      className="watch-observe-toolbar-btn watch-observe-toolbar-btn-danger"
                      type="button"
                      onClick={onSaveDeleteChanges}
                    >
                      保存
                    </button>
                  </>
                ) : null}
              </div>
              <div className="watch-observe-table-toolbar-right">
                <button
                  className="watch-observe-toolbar-btn watch-observe-toolbar-btn-danger"
                  type="button"
                  onClick={isDeleteMode ? undefined : onEnterDeleteMode}
                  disabled={isDeleteMode || refreshingRealtime}
                >
                  {isDeleteMode ? "删除中" : "删除"}
                </button>
              </div>
            </div>

            <div
              className={[
                "watch-observe-table-wrap",
                refreshingRealtime ? "is-refreshing" : "",
              ]
                .filter(Boolean)
                .join(" ")}
              ref={tableWrapRef}
              aria-busy={refreshingRealtime}
            >
              <table
                className="watch-observe-table"
                style={{ minWidth: isDeleteMode ? "1216px" : "1168px" }}
              >
                <colgroup>
                  {isDeleteMode ? <col style={{ width: "48px" }} /> : null}
                  <col style={{ width: "96px" }} />
                  <col style={{ width: "88px" }} />
                  <col style={{ width: "104px" }} />
                  <col style={{ width: "96px" }} />
                  <col style={{ width: "88px" }} />
                  <col style={{ width: "92px" }} />
                  <col style={{ width: "118px" }} />
                  <col style={{ width: "120px" }} />
                  <col style={{ width: "152px" }} />
                  <col />
                </colgroup>
                <thead>
                  <tr>
                    {isDeleteMode ? (
                      <th className="watch-observe-action-col"></th>
                    ) : null}
                    <th>代码</th>
                    <th>名称</th>
                    <th
                      aria-sort={getAriaSort(
                        sortKey === "latestClose",
                        sortDirection,
                      )}
                    >
                      <TableSortButton
                        label={latestPriceHeader}
                        isActive={sortKey === "latestClose"}
                        direction={sortDirection}
                        onClick={() => toggleSort("latestClose")}
                        title={`按${latestPriceHeader}排序`}
                      />
                    </th>
                    <th
                      aria-sort={getAriaSort(
                        sortKey === "latestChangePct",
                        sortDirection,
                      )}
                    >
                      <TableSortButton
                        label={latestChangeHeader}
                        isActive={sortKey === "latestChangePct"}
                        direction={sortDirection}
                        onClick={() => toggleSort("latestChangePct")}
                        title={`按${latestChangeHeader}排序`}
                      />
                    </th>
                    <th
                      aria-sort={getAriaSort(
                        sortKey === "volumeRatio",
                        sortDirection,
                      )}
                    >
                      <TableSortButton
                        label="量比"
                        isActive={sortKey === "volumeRatio"}
                        direction={sortDirection}
                        onClick={() => toggleSort("volumeRatio")}
                        title="按量比排序"
                      />
                    </th>
                    <th
                      aria-sort={getAriaSort(
                        sortKey === "addedDate",
                        sortDirection,
                      )}
                    >
                      <TableSortButton
                        label="加入日期"
                        isActive={sortKey === "addedDate"}
                        direction={sortDirection}
                        onClick={() => toggleSort("addedDate")}
                        title="按加入日期排序"
                      />
                    </th>
                    <th
                      aria-sort={getAriaSort(
                        sortKey === "postWatchReturnPct",
                        sortDirection,
                      )}
                    >
                      <TableSortButton
                        label="自选后涨幅"
                        isActive={sortKey === "postWatchReturnPct"}
                        direction={sortDirection}
                        onClick={() => toggleSort("postWatchReturnPct")}
                        title="按自选后涨幅排序"
                      />
                    </th>
                    <th
                      aria-sort={getAriaSort(
                        sortKey === "todayRank",
                        sortDirection,
                      )}
                    >
                      <TableSortButton
                        label={rankHeader}
                        isActive={sortKey === "todayRank"}
                        direction={sortDirection}
                        onClick={() => toggleSort("todayRank")}
                        title={`按${rankHeader}排序`}
                      />
                    </th>
                    <th>标签</th>
                    <th>概念</th>
                  </tr>
                </thead>
                <tbody>
                  {sortedRows.map((row) => {
                    const conceptText = formatConceptText(
                      row.concept,
                      excludedConcepts,
                    );
                    return (
                      <tr
                        key={row.tsCode}
                        className={
                          pendingDeleteTsCodes.includes(row.tsCode)
                            ? "watch-observe-row-pending-delete"
                            : ""
                        }
                      >
                        {isDeleteMode ? (
                          <td className="watch-observe-action-col">
                            <button
                              className={[
                                "watch-observe-row-toggle",
                                pendingDeleteTsCodes.includes(row.tsCode)
                                  ? "is-pending-delete"
                                  : "",
                              ]
                                .filter(Boolean)
                                .join(" ")}
                              type="button"
                              title={
                                pendingDeleteTsCodes.includes(row.tsCode)
                                  ? "撤销删除"
                                  : "标记删除"
                              }
                              onClick={() => onTogglePendingDelete(row.tsCode)}
                            >
                              {pendingDeleteTsCodes.includes(row.tsCode)
                                ? "+"
                                : "-"}
                            </button>
                          </td>
                        ) : null}
                        <td title={row.tsCode}>{row.tsCode}</td>
                        <td title={row.name || "--"}>
                          {row.name ? (
                            <DetailsLink
                              className="watch-observe-stock-link"
                              tsCode={splitTsCode(row.tsCode)}
                              tradeDate={resolvedReferenceTradeDate ?? row.tradeDate}
                              sourcePath={sourcePathTrimmed}
                              title={`查看 ${row.name} 详情`}
                              navigationItems={detailNavigationItems}
                            >
                              {row.name}
                            </DetailsLink>
                          ) : (
                            "--"
                          )}
                        </td>
                        <td title={formatNumber(row.latestClose)}>
                          {formatNumber(row.latestClose)}
                        </td>
                        <td
                          className={getPercentClassName(row.latestChangePct)}
                          title={formatPercent(row.latestChangePct)}
                        >
                          {formatPercent(row.latestChangePct)}
                        </td>
                        <td title={formatRatio(row.volumeRatio)}>
                          {formatRatio(row.volumeRatio)}
                        </td>
                        <td title={row.addedDate || "--"}>
                          {row.addedDate || "--"}
                        </td>
                        <td
                          className={getPercentClassName(
                            row.postWatchReturnPct,
                          )}
                          title={formatPercent(row.postWatchReturnPct)}
                        >
                          {formatPercent(row.postWatchReturnPct)}
                        </td>
                        <td title={formatNumber(row.todayRank, 0)}>
                          {formatNumber(row.todayRank, 0)}
                        </td>
                        <td title={row.tag || "添加标签"}>
                          {isDeleteMode ? (
                            row.tag || "--"
                          ) : editingTsCode === row.tsCode ? (
                            <div className="watch-observe-tag-editor">
                              <input
                                className="watch-observe-tag-input"
                                type="text"
                                value={tagDraft}
                                onChange={(event) =>
                                  setTagDraft(event.target.value)
                                }
                                placeholder="输入标签"
                                onKeyDown={(event) => {
                                  if (event.key === "Enter") {
                                    void onSaveTag(row.tsCode);
                                  }
                                  if (event.key === "Escape") {
                                    onCancelEditTag();
                                  }
                                }}
                                autoFocus
                              />
                              <div className="watch-observe-tag-actions">
                                <button
                                  className="watch-observe-tag-save"
                                  type="button"
                                  onClick={() => void onSaveTag(row.tsCode)}
                                >
                                  保存
                                </button>
                                <button
                                  className="watch-observe-tag-cancel"
                                  type="button"
                                  onClick={onCancelEditTag}
                                >
                                  取消
                                </button>
                              </div>
                            </div>
                          ) : row.tag ? (
                            <button
                              className="watch-observe-tag-chip"
                              type="button"
                              onClick={() => onStartEditTag(row)}
                            >
                              {row.tag}
                            </button>
                          ) : (
                            <button
                              className="watch-observe-tag-add"
                              type="button"
                              onClick={() => onStartEditTag(row)}
                            >
                              添加
                            </button>
                          )}
                        </td>
                        <td title={conceptText}>{conceptText}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
              {refreshingRealtime ? (
                <div className="watch-observe-refresh-overlay" role="status">
                  <span className="watch-observe-refresh-spinner" aria-hidden="true" />
                  <span>正在刷新实时行情…</span>
                </div>
              ) : null}
            </div>
          </>
        )}
      </section>
    </div>
  );
}
