import {
  Fragment,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import { listRankTradeDates } from "../../apis/reader";
import {
  listWatchObserveRows,
  refreshWatchObserveRows,
  removeWatchObserveRows,
  updateWatchObserveMarkedDate,
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
  normalizeDateValue,
  normalizeTradeDates,
  pickDateValue,
} from "../../shared/tradeDate";
import "./css/WatchObservePage.css";

type ViewMode = "db" | "realtime";
type WatchObserveSortKey =
  | "latestClose"
  | "latestChangePct"
  | "return3dPct"
  | "volumeRatio"
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
        ? (parsed.rows as Array<
            WatchObserveRow & {
              addedDate?: string;
              tradeDate?: string | null;
            }
          >).map((row) => ({
            ...row,
            watchDate: normalizeDateValue(row.watchDate || row.addedDate || ""),
            markedDate:
              normalizeDateValue(row.markedDate || row.tradeDate || "") ||
              null,
          }))
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
  const [editingMarkedDateTsCode, setEditingMarkedDateTsCode] = useState<
    string | null
  >(null);
  const [markedDateDraft, setMarkedDateDraft] = useState("");
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
  const databaseLoadRequestRef = useRef(0);

  const sourcePathTrimmed = sourcePath.trim();
  const sortDefinitions = useMemo(
    () =>
      ({
        latestClose: { value: (row) => row.latestClose },
        latestChangePct: { value: (row) => row.latestChangePct },
        return3dPct: { value: (row) => row.return3dPct },
        volumeRatio: { value: (row) => row.volumeRatio },
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
  const displayRows = useMemo(
    () =>
      sortedRows
        .map((row, index) => ({ row, index }))
        .sort((left, right) => {
          const dateOrder = (right.row.watchDate || "").localeCompare(
            left.row.watchDate || "",
          );
          return dateOrder || left.index - right.index;
        })
        .map(({ row }) => row),
    [sortedRows],
  );
  const watchDateCounts = useMemo(() => {
    const counts = new Map<string, number>();
    displayRows.forEach((row) => {
      const date = row.watchDate || "未记录";
      counts.set(date, (counts.get(date) ?? 0) + 1);
    });
    return counts;
  }, [displayRows]);
  const markedDateOptions = useMemo(
    () =>
      normalizeTradeDates([
        ...dateOptions,
        ...rows.flatMap((row) =>
          [row.markedDate, row.watchDate].filter(
            (value): value is string => Boolean(value),
          ),
        ),
      ]),
    [dateOptions, rows],
  );
  const detailNavigationItems = displayRows.map((row) => ({
    tsCode: row.tsCode,
    tradeDate: resolvedReferenceTradeDate ?? row.markedDate,
    sourcePath: sourcePathTrimmed || undefined,
    name: row.name || undefined,
  }));
  const tableWrapRef = useRouteScrollRegion<HTMLDivElement>(
    "watch-observe-table",
    [displayRows.length, isDeleteMode, viewMode],
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

  const applyDatabaseRows = useCallback(
    (nextRows: WatchObserveRow[], nextReferenceTradeDate: string) => {
      setRows(nextRows);
      setViewMode("db");
      setRefreshedAt(null);
      setResolvedReferenceTradeDate(nextReferenceTradeDate || null);
      setRefreshSummary("");
    },
    [],
  );

  const loadDatabaseRows = useCallback(
    async (options?: { showLoading?: boolean }) => {
      const showLoading = options?.showLoading ?? true;
      const requestId = databaseLoadRequestRef.current + 1;
      const requestedReferenceTradeDate = referenceTradeDate;

      databaseLoadRequestRef.current = requestId;
      if (showLoading) {
        setLoading(true);
      }
      setError("");
      try {
        const nextRows = await listWatchObserveRows(
          sourcePathTrimmed,
          requestedReferenceTradeDate,
        );
        if (databaseLoadRequestRef.current !== requestId) {
          return;
        }
        applyDatabaseRows(nextRows, requestedReferenceTradeDate);
      } catch (loadError) {
        if (databaseLoadRequestRef.current !== requestId) {
          return;
        }
        if (showLoading) {
          setRows([]);
        }
        setError(`读取自选观察失败: ${String(loadError)}`);
      } finally {
        if (databaseLoadRequestRef.current === requestId) {
          setLoading(false);
        }
      }
    },
    [applyDatabaseRows, referenceTradeDate, sourcePathTrimmed],
  );

  useEffect(() => {
    if (viewMode !== "db") {
      return;
    }

    void loadDatabaseRows();
  }, [loadDatabaseRows, viewMode]);

  useEffect(() => {
    if (viewMode !== "db") {
      return;
    }

    let cancelled = false;
    const syncRows = () => {
      if (!cancelled) {
        void loadDatabaseRows({ showLoading: false });
      }
    };

    window.addEventListener("focus", syncRows);
    return () => {
      cancelled = true;
      window.removeEventListener("focus", syncRows);
    };
  }, [loadDatabaseRows, viewMode]);

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
          ? `排名参考日 ${resolvedReferenceTradeDate}`
          : null,
      ]
        .filter(Boolean)
        .join(" | ");
    }

    if (viewMode === "realtime") {
      return [
        refreshedAt ? `最新刷新 ${refreshedAt}` : null,
        resolvedReferenceTradeDate
          ? `排名参考日 ${resolvedReferenceTradeDate}`
          : null,
        refreshSummary || null,
      ]
        .filter(Boolean)
        .join(" | ");
    }
    return [
      `当前共 ${rows.length} 条`,
      resolvedReferenceTradeDate
        ? `排名参考日 ${resolvedReferenceTradeDate}`
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
    databaseLoadRequestRef.current += 1;
    setLoading(false);
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
    await loadDatabaseRows({ showLoading: false });
  }

  function onStartEditTag(row: WatchObserveRow) {
    setEditingMarkedDateTsCode(null);
    setMarkedDateDraft("");
    setEditingTsCode(row.tsCode);
    setTagDraft(row.tag);
  }

  function onCancelEditTag() {
    setEditingTsCode(null);
    setTagDraft("");
  }

  async function onSaveTag(tsCode: string) {
    const requestId = databaseLoadRequestRef.current + 1;
    databaseLoadRequestRef.current = requestId;
    try {
      await updateWatchObserveTag(tsCode, tagDraft.trim(), sourcePathTrimmed);
      const nextRows = await listWatchObserveRows(
        sourcePathTrimmed,
        referenceTradeDate,
      );
      if (databaseLoadRequestRef.current !== requestId) {
        return;
      }
      applyDatabaseRows(nextRows, referenceTradeDate);
      setError("");
      setEditingTsCode(null);
      setTagDraft("");
    } catch (saveError) {
      if (databaseLoadRequestRef.current !== requestId) {
        return;
      }
      setError(`保存标签失败: ${String(saveError)}`);
    }
  }

  function onStartEditMarkedDate(row: WatchObserveRow) {
    setEditingTsCode(null);
    setTagDraft("");
    setEditingMarkedDateTsCode(row.tsCode);
    setMarkedDateDraft(
      row.markedDate || row.watchDate || markedDateOptions[0] || "",
    );
  }

  function onCancelEditMarkedDate() {
    setEditingMarkedDateTsCode(null);
    setMarkedDateDraft("");
  }

  async function onSaveMarkedDate(tsCode: string) {
    if (!markedDateDraft) {
      setError("请选择标记日期。");
      return;
    }

    const requestId = databaseLoadRequestRef.current + 1;
    databaseLoadRequestRef.current = requestId;
    try {
      await updateWatchObserveMarkedDate(
        tsCode,
        markedDateDraft,
        sourcePathTrimmed,
      );
      const nextRows = await listWatchObserveRows(
        sourcePathTrimmed,
        referenceTradeDate,
      );
      if (databaseLoadRequestRef.current !== requestId) {
        return;
      }
      applyDatabaseRows(nextRows, referenceTradeDate);
      setError("");
      onCancelEditMarkedDate();
    } catch (saveError) {
      if (databaseLoadRequestRef.current !== requestId) {
        return;
      }
      setError(`保存标记日期失败: ${String(saveError)}`);
    }
  }

  function onEnterDeleteMode() {
    setIsDeleteMode(true);
    setPendingDeleteTsCodes([]);
    setEditingTsCode(null);
    setTagDraft("");
    setEditingMarkedDateTsCode(null);
    setMarkedDateDraft("");
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
    const requestId = databaseLoadRequestRef.current + 1;
    databaseLoadRequestRef.current = requestId;
    try {
      await removeWatchObserveRows(pendingDeleteTsCodes, sourcePathTrimmed);
      const nextRows = await listWatchObserveRows(
        sourcePathTrimmed,
        referenceTradeDate,
      );
      if (databaseLoadRequestRef.current !== requestId) {
        return;
      }
      applyDatabaseRows(nextRows, referenceTradeDate);
      setError("");
      setIsDeleteMode(false);
      setPendingDeleteTsCodes([]);
    } catch (removeError) {
      if (databaseLoadRequestRef.current !== requestId) {
        return;
      }
      setError(`删除自选失败: ${String(removeError)}`);
    }
  }

  const latestPriceHeader = viewMode === "realtime" ? "实时价*" : "最新价";
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
                  <span>排名参考日</span>
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
                style={{ minWidth: isDeleteMode ? "1476px" : "1428px" }}
              >
                <colgroup>
                  {isDeleteMode ? <col style={{ width: "48px" }} /> : null}
                  <col style={{ width: "108px" }} />
                  <col style={{ width: "80px" }} />
                  <col style={{ width: "96px" }} />
                  <col style={{ width: "104px" }} />
                  <col style={{ width: "104px" }} />
                  <col style={{ width: "72px" }} />
                  <col style={{ width: "90px" }} />
                  <col style={{ width: "116px" }} />
                  <col style={{ width: "116px" }} />
                  <col style={{ width: "116px" }} />
                  <col style={{ width: "170px" }} />
                  <col style={{ width: "116px" }} />
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
                        sortKey === "return3dPct",
                        sortDirection,
                      )}
                    >
                      <TableSortButton
                        label="三日涨幅"
                        isActive={sortKey === "return3dPct"}
                        direction={sortDirection}
                        onClick={() => toggleSort("return3dPct")}
                        title="按三日涨幅排序"
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
                    <th>自选日期</th>
                    <th>标记日期</th>
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
                    <th>最好场景排名</th>
                    <th>标签</th>
                    <th>概念</th>
                  </tr>
                </thead>
                <tbody>
                  {displayRows.map((row, index) => {
                    const conceptText = formatConceptText(
                      row.concept,
                      excludedConcepts,
                    );
                    const watchDate = row.watchDate || "未记录";
                    const isGroupStart =
                      index === 0 ||
                      (displayRows[index - 1]?.watchDate || "未记录") !==
                        watchDate;
                    return (
                      <Fragment key={row.tsCode}>
                        {isGroupStart ? (
                          <tr className="watch-observe-date-group">
                            <td colSpan={isDeleteMode ? 14 : 13}>
                              <span>自选日期</span>
                              <strong>{watchDate}</strong>
                              <span>{watchDateCounts.get(watchDate) ?? 0} 只</span>
                            </td>
                          </tr>
                        ) : null}
                        <tr
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
                              tradeDate={
                                resolvedReferenceTradeDate ?? row.markedDate
                              }
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
                        <td
                          className={getPercentClassName(row.return3dPct)}
                          title={formatPercent(row.return3dPct)}
                        >
                          {formatPercent(row.return3dPct)}
                        </td>
                        <td title={formatRatio(row.volumeRatio)}>
                          {formatRatio(row.volumeRatio)}
                        </td>
                        <td title={row.watchDate || "--"}>
                          {row.watchDate || "--"}
                        </td>
                        <td title={row.markedDate || "修改标记日期"}>
                          {isDeleteMode ? (
                            row.markedDate || "--"
                          ) : editingMarkedDateTsCode === row.tsCode ? (
                            <div className="watch-observe-date-editor">
                              <select
                                className="watch-observe-date-select"
                                value={markedDateDraft}
                                onChange={(event) =>
                                  setMarkedDateDraft(event.target.value)
                                }
                                onKeyDown={(event) => {
                                  if (event.key === "Enter") {
                                    void onSaveMarkedDate(row.tsCode);
                                  }
                                  if (event.key === "Escape") {
                                    onCancelEditMarkedDate();
                                  }
                                }}
                                autoFocus
                              >
                                {markedDateOptions.map((option) => (
                                  <option key={option} value={option}>
                                    {option}
                                  </option>
                                ))}
                              </select>
                              <div className="watch-observe-date-actions">
                                <button
                                  className="watch-observe-date-save"
                                  type="button"
                                  onClick={() =>
                                    void onSaveMarkedDate(row.tsCode)
                                  }
                                >
                                  保存
                                </button>
                                <button
                                  className="watch-observe-date-cancel"
                                  type="button"
                                  onClick={onCancelEditMarkedDate}
                                >
                                  取消
                                </button>
                              </div>
                            </div>
                          ) : (
                            <button
                              className="watch-observe-date-edit"
                              type="button"
                              onClick={() => onStartEditMarkedDate(row)}
                            >
                              {row.markedDate || "设置"}
                            </button>
                          )}
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
                        <td title={row.sceneMarker ?? "--"}>
                          {row.sceneMarker ?? "--"}
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
                      </Fragment>
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
