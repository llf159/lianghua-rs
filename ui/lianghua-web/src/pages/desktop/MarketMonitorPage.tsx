import { useEffect, useMemo, useState } from "react";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import {
  getMarketMonitorPage,
  type MarketMonitorPageData,
} from "../../apis/marketMonitor";
import { listRankTradeDates } from "../../apis/reader";
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
import "./css/MarketMonitorPage.css";

const DEFAULT_TOP_LIMIT = "50";
const MARKET_MONITOR_STATE_KEY = "lh_market_monitor_page_state_v1";
type MarketMonitorSortKey =
  | "referenceRank"
  | "totalScore"
  | "latestPrice"
  | "latestChangePct"
  | "volumeRatio"
  | "open";

type PersistedMarketMonitorState = {
  sourcePath: string;
  dateOptions: string[];
  referenceTradeDate: string;
  topLimitInput: string;
  pageData: MarketMonitorPageData | null;
};

function formatNumber(value: number | null | undefined, digits = 2) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "--";
  }
  return Number.isInteger(value) ? String(value) : value.toFixed(digits);
}

function formatPercent(value: number | null | undefined) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "--";
  }
  return `${value.toFixed(2)}%`;
}

function formatRatio(value: number | null | undefined) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "--";
  }
  return value.toFixed(2);
}

function getPercentClassName(value: number | null | undefined) {
  if (typeof value !== "number" || !Number.isFinite(value) || value === 0) {
    return "market-monitor-value-flat";
  }
  return value > 0 ? "market-monitor-value-up" : "market-monitor-value-down";
}

function toPositiveInt(raw: string) {
  const parsed = Number(raw.trim());
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return null;
  }
  return parsed;
}

export default function MarketMonitorPage() {
  const { excludedConcepts } = useConceptExclusions();
  const persistedState = useMemo(() => {
    const parsed = readJsonStorage<Partial<PersistedMarketMonitorState>>(
      typeof window === "undefined" ? null : window.sessionStorage,
      MARKET_MONITOR_STATE_KEY,
    );
    if (!parsed || typeof parsed !== "object") {
      return null;
    }

    return {
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
      topLimitInput:
        typeof parsed.topLimitInput === "string"
          ? parsed.topLimitInput
          : DEFAULT_TOP_LIMIT,
      pageData:
        parsed.pageData && typeof parsed.pageData === "object"
          ? (parsed.pageData as MarketMonitorPageData)
          : null,
    } satisfies PersistedMarketMonitorState;
  }, []);
  const [sourcePath, setSourcePath] = useState(
    () => persistedState?.sourcePath ?? readStoredSourcePath(),
  );
  const [dateOptions, setDateOptions] = useState<string[]>(
    () => persistedState?.dateOptions ?? [],
  );
  const [referenceTradeDate, setReferenceTradeDate] = useState(
    () => persistedState?.referenceTradeDate ?? DEFAULT_DATE_OPTION,
  );
  const [topLimitInput, setTopLimitInput] = useState(
    () => persistedState?.topLimitInput ?? DEFAULT_TOP_LIMIT,
  );
  const [loading, setLoading] = useState(false);
  const [dateOptionsLoading, setDateOptionsLoading] = useState(false);
  const [error, setError] = useState("");
  const [pageData, setPageData] = useState<MarketMonitorPageData | null>(
    () => persistedState?.pageData ?? null,
  );

  const sourcePathTrimmed = sourcePath.trim();

  useEffect(() => {
    writeJsonStorage(
      typeof window === "undefined" ? null : window.sessionStorage,
      MARKET_MONITOR_STATE_KEY,
      {
        sourcePath,
        dateOptions,
        referenceTradeDate,
        topLimitInput,
        pageData,
      } satisfies PersistedMarketMonitorState,
    );
  }, [dateOptions, pageData, referenceTradeDate, sourcePath, topLimitInput]);

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
      } catch (loadError) {
        if (!cancelled) {
          setDateOptions([]);
          setReferenceTradeDate(DEFAULT_DATE_OPTION);
          setError(`读取参考日失败: ${String(loadError)}`);
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

  async function onRefresh() {
    const topLimit = toPositiveInt(topLimitInput);
    if (topLimit === null) {
      setError("前列数量必须是正整数");
      return;
    }
    if (!sourcePathTrimmed) {
      setError("请先到“数据管理”页确认当前目录");
      return;
    }

    setLoading(true);
    setError("");
    try {
      const nextPageData = await getMarketMonitorPage({
        sourcePath: sourcePathTrimmed,
        referenceTradeDate: referenceTradeDate.trim() || undefined,
        topLimit,
      });
      setPageData(nextPageData);
    } catch (refreshError) {
      setError(`刷新盘中监控失败: ${String(refreshError)}`);
    } finally {
      setLoading(false);
    }
  }

  const rows = pageData?.rows ?? [];
  const sortDefinitions = useMemo(
    () =>
      ({
        referenceRank: { value: (row) => row.referenceRank },
        totalScore: { value: (row) => row.totalScore },
        latestPrice: { value: (row) => row.latestPrice },
        latestChangePct: { value: (row) => row.latestChangePct },
        volumeRatio: { value: (row) => row.volumeRatio },
        open: { value: (row) => row.open },
      }) satisfies Partial<
        Record<MarketMonitorSortKey, SortDefinition<(typeof rows)[number]>>
      >,
    [rows],
  );
  const { sortKey, sortDirection, sortedRows, toggleSort } = useTableSort<
    (typeof rows)[number],
    MarketMonitorSortKey
  >(rows, sortDefinitions);
  const detailNavigationItems = sortedRows.map((row) => ({
    tsCode: row.tsCode,
    tradeDate: pageData?.referenceTradeDate ?? referenceTradeDate,
    sourcePath: sourcePathTrimmed || undefined,
    name: row.name || undefined,
  }));
  const tableWrapRef = useRouteScrollRegion<HTMLDivElement>(
    "market-monitor-table",
    [sortedRows.length],
  );
  const statusText = pageData
    ? [
        pageData.refreshedAt ? `最新刷新 ${pageData.refreshedAt}` : null,
        pageData.referenceTradeDate
          ? `参考日 ${pageData.referenceTradeDate}`
          : null,
        `已抓取 ${pageData.fetchedCount}/${pageData.effectiveCount}`,
        pageData.requestedCount > 50 ? "已按 50 只一批分批抓取" : null,
      ]
        .filter(Boolean)
        .join(" | ")
    : "手动刷新后抓取所选参考日前列股票的实时数据，不自动轮询。";

  return (
    <div className="market-monitor-page">
      <section className="market-monitor-card">
        <div className="market-monitor-head">
          <div>
            <h2 className="market-monitor-title">盘中监控</h2>
            <p className="market-monitor-tip">{statusText}</p>
          </div>
        </div>

        <div className="market-monitor-toolbar">
          <label className="market-monitor-field">
            <span>参考日</span>
            <select
              value={referenceTradeDate}
              onChange={(event) => setReferenceTradeDate(event.target.value)}
              disabled={dateOptionsLoading || dateOptions.length === 0}
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

          <label className="market-monitor-field">
            <span>前列数量</span>
            <input
              type="number"
              min={1}
              step={1}
              value={topLimitInput}
              onChange={(event) => setTopLimitInput(event.target.value)}
            />
          </label>

          <div className="market-monitor-actions">
            <button
              className="market-monitor-refresh-btn"
              type="button"
              onClick={() => void onRefresh()}
              disabled={loading || dateOptionsLoading}
            >
              {loading ? "刷新中..." : "手动刷新"}
            </button>
          </div>
        </div>

        {error ? <div className="market-monitor-empty">{error}</div> : null}
        {!error && rows.length === 0 ? (
          <div className="market-monitor-empty">
            当前还没有盘中监控结果，先选参数再刷新。
          </div>
        ) : null}

        {rows.length > 0 ? (
          <div className="market-monitor-table-wrap" ref={tableWrapRef}>
            <table className="market-monitor-table">
              <colgroup>
                <col style={{ width: "118px" }} />
                <col style={{ width: "108px" }} />
                <col style={{ width: "94px" }} />
                <col style={{ width: "86px" }} />
                <col style={{ width: "96px" }} />
                <col style={{ width: "96px" }} />
                <col style={{ width: "96px" }} />
                <col style={{ width: "88px" }} />
                <col style={{ width: "96px" }} />
                <col />
              </colgroup>
              <thead>
                <tr>
                  <th>代码</th>
                  <th>名称</th>
                  <th
                    aria-sort={getAriaSort(
                      sortKey === "referenceRank",
                      sortDirection,
                    )}
                  >
                    <TableSortButton
                      label="参考日排名"
                      isActive={sortKey === "referenceRank"}
                      direction={sortDirection}
                      onClick={() => toggleSort("referenceRank")}
                      title="按参考日排名排序"
                    />
                  </th>
                  <th
                    aria-sort={getAriaSort(
                      sortKey === "totalScore",
                      sortDirection,
                    )}
                  >
                    <TableSortButton
                      label="总分"
                      isActive={sortKey === "totalScore"}
                      direction={sortDirection}
                      onClick={() => toggleSort("totalScore")}
                      title="按总分排序"
                    />
                  </th>
                  <th
                    aria-sort={getAriaSort(
                      sortKey === "latestPrice",
                      sortDirection,
                    )}
                  >
                    <TableSortButton
                      label="实时价"
                      isActive={sortKey === "latestPrice"}
                      direction={sortDirection}
                      onClick={() => toggleSort("latestPrice")}
                      title="按实时价排序"
                    />
                  </th>
                  <th
                    aria-sort={getAriaSort(
                      sortKey === "latestChangePct",
                      sortDirection,
                    )}
                  >
                    <TableSortButton
                      label="实时涨幅"
                      isActive={sortKey === "latestChangePct"}
                      direction={sortDirection}
                      onClick={() => toggleSort("latestChangePct")}
                      title="按实时涨幅排序"
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
                    aria-sort={getAriaSort(sortKey === "open", sortDirection)}
                  >
                    <TableSortButton
                      label="开盘"
                      isActive={sortKey === "open"}
                      direction={sortDirection}
                      onClick={() => toggleSort("open")}
                      title="按开盘价排序"
                    />
                  </th>
                  <th>区间</th>
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
                    <tr key={row.tsCode}>
                      <td>{row.tsCode}</td>
                      <td>
                        <DetailsLink
                          className="market-monitor-stock-link"
                          tsCode={splitTsCode(row.tsCode)}
                          tradeDate={
                            pageData?.referenceTradeDate ?? referenceTradeDate
                          }
                          sourcePath={sourcePathTrimmed}
                          navigationItems={detailNavigationItems}
                        >
                          {row.name || "--"}
                        </DetailsLink>
                      </td>
                      <td>{formatNumber(row.referenceRank, 0)}</td>
                      <td>{formatNumber(row.totalScore)}</td>
                      <td>{formatNumber(row.latestPrice)}</td>
                      <td
                        className={getPercentClassName(row.latestChangePct)}
                        title={formatPercent(row.latestChangePct)}
                      >
                        {formatPercent(row.latestChangePct)}
                      </td>
                      <td title={formatRatio(row.volumeRatio)}>
                        {formatRatio(row.volumeRatio)}
                      </td>
                      <td>{formatNumber(row.open)}</td>
                      <td
                        title={`${formatNumber(row.low)} - ${formatNumber(row.high)}`}
                      >
                        {formatNumber(row.low)} - {formatNumber(row.high)}
                      </td>
                      <td title={conceptText}>{conceptText}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : null}
      </section>
    </div>
  );
}
