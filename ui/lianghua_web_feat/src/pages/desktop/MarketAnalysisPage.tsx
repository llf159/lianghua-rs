import { useEffect, useMemo, useState } from "react";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import {
  getMarketAnalysis,
  getMarketContribution,
  type MarketAnalysisData,
  type MarketContributionData,
} from "../../apis/strategyTrigger";
import DetailsLink from "../../shared/DetailsLink";
import { splitTsCode } from "../../shared/stockCode";
import { filterBoardItems, isStBoard, useConceptExclusions } from "../../shared/conceptExclusions";
import { readJsonStorage, readStoredSourcePath, writeJsonStorage } from "../../shared/storage";
import { STOCK_PICK_BOARD_OPTIONS } from "../../shared/stockPickShared";
import "./css/SceneLayerBacktestPage.css";

function formatDateLabel(value?: string | null) {
  if (!value || value.length !== 8) {
    return "--";
  }
  return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`;
}

function normalizeDateInput(value: string) {
  return value.replaceAll("-", "").trim();
}

function compactDateToInput(value?: string | null) {
  if (!value || !/^\d{8}$/.test(value)) {
    return "";
  }
  return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`;
}

function formatMarketDateRange(value?: string | null) {
  if (!value) {
    return "--";
  }
  const [start, end] = value.split("~");
  if (start && end) {
    return `${formatDateLabel(start)} ~ ${formatDateLabel(end)}`;
  }
  return formatDateLabel(value);
}

function parseMarketDateRange(value?: string | null) {
  if (!value) {
    return { startDate: undefined, endDate: undefined };
  }

  const [start, end] = value.split("~");
  if (start && end) {
    return {
      startDate: start.trim() || undefined,
      endDate: end.trim() || undefined,
    };
  }

  const normalized = value.trim();
  return {
    startDate: normalized || undefined,
    endDate: normalized || undefined,
  };
}

function formatPercent(value?: number | null, digits = 2) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  return `${value.toFixed(digits)}%`;
}

function getPercentToneClass(value?: number | null) {
  if (value === null || value === undefined || !Number.isFinite(value) || value === 0) {
    return "scene-layer-value-flat";
  }
  return value > 0 ? "scene-layer-value-up" : "scene-layer-value-down";
}

function extractTsCodeFromRankName(value: string) {
  const matched = value.match(/\((\d{6}\.[A-Z]{2})\)\s*$/);
  return matched?.[1] ?? null;
}

function getRankItemTsCode(item: { name: string; ts_code?: string | null }) {
  return item.ts_code?.trim() || extractTsCodeFromRankName(item.name);
}

function getRankItemDisplayName(item: { name: string; ts_code?: string | null }) {
  const tsCode = getRankItemTsCode(item);
  return tsCode ? item.name.replace(/\s*\(\d{6}\.[A-Z]{2}\)\s*$/, "") : item.name;
}

function isNonNull<T>(value: T | null): value is T {
  return value !== null;
}

const MARKET_BOARD_FILTER_OPTIONS = STOCK_PICK_BOARD_OPTIONS.filter(
  (item) => item !== "全部",
);

const MARKET_ANALYSIS_PARAMS_STORAGE_KEY = "lh_market_analysis_params_v1";

type MarketAnalysisParamsDraft = {
  lookbackPeriod: string;
  stockRankLimit: string;
  subIntervalPeriod: string;
  minListedTradeDays: string;
  minBoardStockCount: string;
  selectedBoard: string;
  carryInterval: boolean;
};

function readMarketAnalysisParamsDraft(): MarketAnalysisParamsDraft {
  const fallback: MarketAnalysisParamsDraft = {
    lookbackPeriod: "20",
    stockRankLimit: "20",
    subIntervalPeriod: "3",
    minListedTradeDays: "60",
    minBoardStockCount: "1",
    selectedBoard: "",
    carryInterval: true,
  };
  const parsed = readJsonStorage<Partial<MarketAnalysisParamsDraft>>(
    typeof window === "undefined" ? null : window.localStorage,
    MARKET_ANALYSIS_PARAMS_STORAGE_KEY,
  );
  if (!parsed) {
    return fallback;
  }

  return {
    lookbackPeriod: typeof parsed.lookbackPeriod === "string" ? parsed.lookbackPeriod : fallback.lookbackPeriod,
    stockRankLimit: typeof parsed.stockRankLimit === "string" ? parsed.stockRankLimit : fallback.stockRankLimit,
    subIntervalPeriod: typeof parsed.subIntervalPeriod === "string" ? parsed.subIntervalPeriod : fallback.subIntervalPeriod,
    minListedTradeDays: typeof parsed.minListedTradeDays === "string" ? parsed.minListedTradeDays : fallback.minListedTradeDays,
    minBoardStockCount: typeof parsed.minBoardStockCount === "string" ? parsed.minBoardStockCount : fallback.minBoardStockCount,
    selectedBoard: typeof parsed.selectedBoard === "string" ? parsed.selectedBoard : fallback.selectedBoard,
    carryInterval: typeof parsed.carryInterval === "boolean" ? parsed.carryInterval : fallback.carryInterval,
  };
}

export default function MarketAnalysisPage() {
  const { excludedConcepts, excludeStBoard } = useConceptExclusions();
  const persistedParams = useMemo(() => readMarketAnalysisParamsDraft(), []);
  const [sourcePath, setSourcePath] = useState(() => readStoredSourcePath());
  const [lookbackPeriod, setLookbackPeriod] = useState(persistedParams.lookbackPeriod);
  const [stockRankLimit, setStockRankLimit] = useState(persistedParams.stockRankLimit);
  const [subIntervalPeriod, setSubIntervalPeriod] = useState(persistedParams.subIntervalPeriod);
  const [minListedTradeDays, setMinListedTradeDays] = useState(persistedParams.minListedTradeDays);
  const [minBoardStockCount, setMinBoardStockCount] = useState(persistedParams.minBoardStockCount);
  const [referenceDateInput, setReferenceDateInput] = useState("");
  const [selectedBoard, setSelectedBoard] = useState(persistedParams.selectedBoard);
  const [carryInterval, setCarryInterval] = useState(persistedParams.carryInterval);
  const [initializing, setInitializing] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState<MarketAnalysisData | null>(null);
  const [contributionLoading, setContributionLoading] = useState(false);
  const [contributionError, setContributionError] = useState("");
  const [contributionResult, setContributionResult] = useState<MarketContributionData | null>(null);
  const [isContributionModalOpen, setIsContributionModalOpen] = useState(false);
  const boardFilterOptions = useMemo(
    () => filterBoardItems(MARKET_BOARD_FILTER_OPTIONS, excludeStBoard),
    [excludeStBoard],
  );

  useEffect(() => {
    let cancelled = false;
    const init = async () => {
      setInitializing(true);
      try {
        const resolved = await ensureManagedSourcePath();
        if (cancelled) {
          return;
        }
        setSourcePath(resolved);
      } catch (initError) {
        if (!cancelled) {
          setError(`读取默认数据目录失败: ${String(initError)}`);
        }
      } finally {
        if (!cancelled) {
          setInitializing(false);
        }
      }
    };

    void init();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (excludeStBoard && isStBoard(selectedBoard)) {
      setSelectedBoard("");
    }
  }, [excludeStBoard, selectedBoard]);

  useEffect(() => {
    writeJsonStorage(typeof window === "undefined" ? null : window.localStorage, MARKET_ANALYSIS_PARAMS_STORAGE_KEY, {
      lookbackPeriod,
      stockRankLimit,
      subIntervalPeriod,
      minListedTradeDays,
      minBoardStockCount,
      selectedBoard,
      carryInterval,
    });
  }, [
    carryInterval,
    lookbackPeriod,
    minBoardStockCount,
    minListedTradeDays,
    selectedBoard,
    stockRankLimit,
    subIntervalPeriod,
  ]);

  async function onRunMarketAnalysis() {
    const normalizedRefDate = normalizeDateInput(referenceDateInput);

    if (!sourcePath.trim()) {
      setError("当前数据目录为空，请先在数据管理页确认目录。");
      return;
    }

    setLoading(true);
    setError("");
    try {
      const data = await getMarketAnalysis({
        sourcePath,
        lookbackPeriod: Math.max(1, Number(lookbackPeriod) || 1),
        referenceTradeDate: normalizedRefDate || undefined,
        board: selectedBoard.trim() || undefined,
        excludeStBoard: excludeStBoard || undefined,
        minListedTradeDays: Math.max(0, Number(minListedTradeDays) || 0),
        minBoardStockCount: Math.max(1, Number(minBoardStockCount) || 1),
        stockRankLimit: Math.max(1, Number(stockRankLimit) || 1),
        subIntervalPeriod: Math.max(3, Number(subIntervalPeriod) || 3),
      });
      setResult(data);
      setContributionResult(null);
      setContributionError("");
      setReferenceDateInput(compactDateToInput(data.resolved_reference_trade_date));
      setSelectedBoard(data.resolved_board ?? "");
    } catch (runError) {
      setResult(null);
      setError(`执行市场分析失败: ${String(runError)}`);
    } finally {
      setLoading(false);
    }
  }

  async function onLoadContribution(scope: "interval" | "daily", kind: "concept" | "industry", name: string) {
    if (!result) {
      return;
    }
    setContributionLoading(true);
    setContributionError("");
    try {
      const detail = await getMarketContribution({
        sourcePath,
        scope,
        kind,
        name,
        lookbackPeriod: result.lookback_period,
        referenceTradeDate: result.resolved_reference_trade_date ?? undefined,
      });
      setContributionResult(detail);
      setIsContributionModalOpen(true);
    } catch (loadError) {
      setContributionResult(null);
      setContributionError(`加载贡献明细失败: ${String(loadError)}`);
    } finally {
      setContributionLoading(false);
    }
  }

  const detailNavigationItems = useMemo(() => {
    const contributors = contributionResult?.contributors ?? [];
    const baseTradeDate = contributionResult?.trade_date ?? result?.resolved_reference_trade_date ?? undefined;
    const start = contributionResult?.scope === "interval"
      ? contributionResult.start_date ?? undefined
      : undefined;
    const end = contributionResult?.scope === "interval"
      ? contributionResult.end_date ?? result?.resolved_reference_trade_date ?? undefined
      : undefined;
    const sourcePathTrimmed = sourcePath.trim() || undefined;
    return contributors.map((item) => ({
      tsCode: item.ts_code,
      tradeDate: (carryInterval || contributionResult?.scope !== "interval") ? baseTradeDate : (start ?? baseTradeDate),
      intervalStartTradeDate: carryInterval ? start : undefined,
      intervalEndTradeDate: carryInterval ? end : undefined,
      sourcePath: sourcePathTrimmed,
      name: item.name ?? undefined,
    }));
  }, [contributionResult, result?.resolved_reference_trade_date, sourcePath, carryInterval]);

  const intervalGainNavigationItems = useMemo(() => {
    if (!result) {
      return [];
    }
    const baseTradeDate = result.resolved_reference_trade_date ?? undefined;
    const { startDate: parsedIntervalStartTradeDate, endDate: parsedIntervalEndTradeDate } =
      parseMarketDateRange(result.interval.trade_date);
    const fallbackEnd = parsedIntervalEndTradeDate ?? result.resolved_reference_trade_date ?? undefined;
    const sourcePathTrimmed = sourcePath.trim() || undefined;
    return result.interval.gain_top
      .map((item) => {
        const tsCode = getRankItemTsCode(item);
        if (!tsCode) {
          return null;
        }
        const start = item.start_date ?? parsedIntervalStartTradeDate;
        const end = item.end_date ?? fallbackEnd;
        return {
          tsCode,
          tradeDate: carryInterval ? baseTradeDate : (start ?? baseTradeDate),
          intervalStartTradeDate: carryInterval ? start : undefined,
          intervalEndTradeDate: carryInterval ? end : undefined,
          sourcePath: sourcePathTrimmed,
          name: getRankItemDisplayName(item),
        };
      })
      .filter(isNonNull);
  }, [result, sourcePath, carryInterval]);

  const subIntervalGainNavigationItems = useMemo(() => {
    if (!result) {
      return [];
    }
    const baseTradeDate = result.resolved_reference_trade_date ?? undefined;
    const sourcePathTrimmed = sourcePath.trim() || undefined;
    return result.interval.sub_interval_gain_top
      .map((item) => {
        const tsCode = getRankItemTsCode(item);
        if (!tsCode) {
          return null;
        }
        const start = item.start_date ?? undefined;
        const end = item.end_date ?? result.resolved_reference_trade_date ?? undefined;
        return {
          tsCode,
          tradeDate: carryInterval ? baseTradeDate : (start ?? baseTradeDate),
          intervalStartTradeDate: carryInterval ? start : undefined,
          intervalEndTradeDate: carryInterval ? end : undefined,
          sourcePath: sourcePathTrimmed,
          name: getRankItemDisplayName(item),
        };
      })
      .filter(isNonNull);
  }, [result, sourcePath, carryInterval]);

  const dailyGainNavigationItems = useMemo(() => {
    if (!result) {
      return [];
    }
    const tradeDate = result.resolved_reference_trade_date ?? undefined;
    const sourcePathTrimmed = sourcePath.trim() || undefined;
    return result.daily.gain_top
      .map((item) => {
        const tsCode = getRankItemTsCode(item);
        if (!tsCode) {
          return null;
        }
        return {
          tsCode,
          tradeDate,
          sourcePath: sourcePathTrimmed,
          name: getRankItemDisplayName(item),
        };
      })
      .filter(isNonNull);
  }, [result, sourcePath]);

  const excludedConceptSet = useMemo(
    () => new Set(excludedConcepts.map((value) => value.trim().toLocaleLowerCase())),
    [excludedConcepts],
  );

  const filteredIntervalConceptTop = useMemo(
    () =>
      (result?.interval.concept_top ?? []).filter(
        (item) => !excludedConceptSet.has(item.name.trim().toLocaleLowerCase()),
      ),
    [excludedConceptSet, result?.interval.concept_top],
  );

  const filteredDailyConceptTop = useMemo(
    () =>
      (result?.daily.concept_top ?? []).filter(
        (item) => !excludedConceptSet.has(item.name.trim().toLocaleLowerCase()),
      ),
    [excludedConceptSet, result?.daily.concept_top],
  );

  return (
    <div className="scene-layer-page market-analysis-page">
      <section className="scene-layer-card market-analysis-filter-card">
        <h2 className="scene-layer-title">市场分析</h2>
        <p className="scene-layer-caption">
          根据回看周期和参考日，展示区间与当日的概念榜、行业榜（list.market 分类）、涨幅榜（参考日默认自动取最新交易日）。
        </p>

        <div className="scene-layer-form-grid scene-layer-form-grid-market">
          <label className="scene-layer-field">
            <span>回看周期（交易日）</span>
            <input
              type="number"
              min="1"
              value={lookbackPeriod}
              onChange={(event) => setLookbackPeriod(event.target.value)}
            />
          </label>
          <label className="scene-layer-field">
            <span>参考日（默认最新）</span>
            <input
              type="date"
              value={referenceDateInput}
              onChange={(event) => setReferenceDateInput(event.target.value)}
            />
          </label>
          <label className="scene-layer-field">
            <span>最少上市交易日</span>
            <input
              type="number"
              min="0"
              value={minListedTradeDays}
              onChange={(event) => setMinListedTradeDays(event.target.value)}
            />
          </label>
          <label className="scene-layer-field">
            <span>最低板块内个股数</span>
            <input
              type="number"
              min="1"
              value={minBoardStockCount}
              onChange={(event) => setMinBoardStockCount(event.target.value)}
            />
          </label>
          <label className="scene-layer-field">
            <span>个股榜数量</span>
            <input
              type="number"
              min="1"
              max="200"
              value={stockRankLimit}
              onChange={(event) => setStockRankLimit(event.target.value)}
            />
          </label>
          <label className="scene-layer-field">
            <span>子区间天数（&gt;=3）</span>
            <input
              type="number"
              min="3"
              value={subIntervalPeriod}
              onChange={(event) => setSubIntervalPeriod(event.target.value)}
            />
          </label>
          <label className="scene-layer-field">
            <span>板块筛选（应用到个股榜）</span>
            <select value={selectedBoard} onChange={(event) => setSelectedBoard(event.target.value)}>
              <option value="">全部板块</option>
              {boardFilterOptions.map((board) => (
                <option key={board} value={board}>
                  {board}
                </option>
              ))}
            </select>
          </label>
          <label className="scene-layer-field">
            <span>跳转携带</span>
            <select
              value={carryInterval ? "interval" : "start"}
              onChange={(event) => setCarryInterval(event.target.value === "interval")}
            >
              <option value="interval">带入区间</option>
              <option value="start">带入起点</option>
            </select>
          </label>
        </div>

        <div className="scene-layer-actions">
          <button
            type="button"
            className="scene-layer-primary-btn"
            onClick={() => void onRunMarketAnalysis()}
            disabled={loading || initializing}
          >
            {loading ? "分析中..." : "执行市场分析"}
          </button>
        </div>

        {error ? <div className="scene-layer-error">{error}</div> : null}
      </section>

      {result ? (
        <section className="scene-layer-card scene-layer-market-main-card market-analysis-main-card">
          <div className="scene-layer-market-wrap">
            <div className="scene-layer-summary-grid scene-layer-summary-grid-market">
              <div className="scene-layer-summary-item scene-layer-summary-item-kpi">
                <span>回看周期</span>
                <strong>{result.lookback_period} 日</strong>
              </div>
              <div className="scene-layer-summary-item scene-layer-summary-item-kpi">
                <span>最新交易日</span>
                <strong>{formatDateLabel(result.latest_trade_date)}</strong>
              </div>
              <div className="scene-layer-summary-item scene-layer-summary-item-kpi">
                <span>参考日</span>
                <strong>{formatDateLabel(result.resolved_reference_trade_date)}</strong>
              </div>
              <div className="scene-layer-summary-item scene-layer-summary-item-kpi">
                <span>区间</span>
                <strong>{formatMarketDateRange(result.interval.trade_date)}</strong>
              </div>
              <div className="scene-layer-summary-item scene-layer-summary-item-kpi">
                <span>个股榜数量</span>
                <strong>{result.stock_rank_limit} 只</strong>
              </div>
              <div className="scene-layer-summary-item scene-layer-summary-item-kpi">
                <span>主题最少个股</span>
                <strong>{result.min_board_stock_count} 只</strong>
              </div>
              <div className="scene-layer-summary-item scene-layer-summary-item-kpi">
                <span>子区间</span>
                <strong>{result.sub_interval_period} 日</strong>
              </div>
            </div>

            <div className="scene-layer-market-section-grid">
              <section className="scene-layer-market-panel">
                <h3>区间主题榜单</h3>
                <div className="scene-layer-market-lists scene-layer-market-lists-two">
                  <div className="scene-layer-market-list">
                    <h4>概念榜</h4>
                    <ol>
                      {filteredIntervalConceptTop.map((item) => (
                        <li key={`interval-concept-${item.name}`}>
                          <button
                            type="button"
                            className="scene-layer-market-name-btn"
                            onClick={() => void onLoadContribution("interval", "concept", item.name)}
                            disabled={contributionLoading}
                            title={`查看 ${item.name} 贡献列表`}
                          >
                            {item.name}
                          </button>
                          <strong className={getPercentToneClass(item.value)}>{formatPercent(item.value)}</strong>
                        </li>
                      ))}
                    </ol>
                  </div>
                  <div className="scene-layer-market-list">
                    <h4>行业榜</h4>
                    <ol>
                      {result.interval.industry_top.map((item) => (
                        <li key={`interval-board-${item.name}`}>
                          <button
                            type="button"
                            className="scene-layer-market-name-btn"
                            onClick={() => void onLoadContribution("interval", "industry", item.name)}
                            disabled={contributionLoading}
                            title={`查看 ${item.name} 贡献列表`}
                          >
                            {item.name}
                          </button>
                          <strong className={getPercentToneClass(item.value)}>{formatPercent(item.value)}</strong>
                        </li>
                      ))}
                    </ol>
                  </div>
                </div>
              </section>

              <section className="scene-layer-market-panel">
                <h3>当日主题榜单</h3>
                <div className="scene-layer-market-lists scene-layer-market-lists-two">
                  <div className="scene-layer-market-list">
                    <h4>概念榜</h4>
                    <ol>
                      {filteredDailyConceptTop.map((item) => (
                        <li key={`daily-concept-${item.name}`}>
                          <button
                            type="button"
                            className="scene-layer-market-name-btn"
                            onClick={() => void onLoadContribution("daily", "concept", item.name)}
                            disabled={contributionLoading}
                            title={`查看 ${item.name} 贡献列表`}
                          >
                            {item.name}
                          </button>
                          <strong>{formatPercent(item.value)}</strong>
                        </li>
                      ))}
                    </ol>
                  </div>
                  <div className="scene-layer-market-list">
                    <h4>行业榜</h4>
                    <ol>
                      {result.daily.industry_top.map((item) => (
                        <li key={`daily-board-${item.name}`}>
                          <button
                            type="button"
                            className="scene-layer-market-name-btn"
                            onClick={() => void onLoadContribution("daily", "industry", item.name)}
                            disabled={contributionLoading}
                            title={`查看 ${item.name} 贡献列表`}
                          >
                            {item.name}
                          </button>
                          <strong>{formatPercent(item.value)}</strong>
                        </li>
                      ))}
                    </ol>
                  </div>
                </div>
              </section>
            </div>

            <div className="scene-layer-market-gainers-grid">
              <section className="scene-layer-market-panel scene-layer-market-gainers-panel">
                <h3>区间个股涨幅榜</h3>
                <div className="scene-layer-contrib-table-wrap">
                  <table className="scene-layer-contrib-table">
                    <thead>
                      <tr>
                        <th>#</th>
                        <th>代码</th>
                        <th>名称</th>
                        <th>涨幅</th>
                      </tr>
                    </thead>
                    <tbody>
                      {result.interval.gain_top.map((item, index) => {
                        const tsCode = getRankItemTsCode(item);
                        const displayName = getRankItemDisplayName(item);
                        const baseTradeDate = result.resolved_reference_trade_date ?? undefined;
                        const start = item.start_date ?? parseMarketDateRange(result.interval.trade_date).startDate;
                        const end = item.end_date ?? parseMarketDateRange(result.interval.trade_date).endDate ?? baseTradeDate;
                        return (
                          <tr key={`interval-gain-${item.name}`}>
                            <td>{index + 1}</td>
                            <td>{tsCode ?? "--"}</td>
                            <td>
                              {tsCode ? (
                                <DetailsLink
                                  className="scene-layer-market-stock-link"
                                  tsCode={splitTsCode(tsCode)}
                                  tradeDate={carryInterval ? baseTradeDate : (start ?? baseTradeDate)}
                                  intervalStartTradeDate={carryInterval ? start : undefined}
                                  intervalEndTradeDate={carryInterval ? end : undefined}
                                  sourcePath={sourcePath.trim() || undefined}
                                  title={`查看 ${item.name} 详情`}
                                  navigationItems={intervalGainNavigationItems}
                                >
                                  {displayName}
                                </DetailsLink>
                              ) : (
                                displayName
                              )}
                            </td>
                            <td className={getPercentToneClass(item.value)}>{formatPercent(item.value)}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </section>

              <section className="scene-layer-market-panel scene-layer-market-gainers-panel">
                <h3>子区间个股涨幅榜（{result.sub_interval_period} 日）</h3>
                <div className="scene-layer-contrib-table-wrap">
                  <table className="scene-layer-contrib-table market-analysis-gain-table">
                    <thead>
                      <tr>
                        <th>#</th>
                        <th>代码</th>
                        <th>名称</th>
                        <th>区间</th>
                        <th>涨幅</th>
                      </tr>
                    </thead>
                    <tbody>
                      {result.interval.sub_interval_gain_top.map((item, index) => {
                        const tsCode = getRankItemTsCode(item);
                        const displayName = getRankItemDisplayName(item);
                        const baseTradeDate = result.resolved_reference_trade_date ?? undefined;
                        const start = item.start_date ?? undefined;
                        const end = item.end_date ?? baseTradeDate;
                        return (
                          <tr key={`sub-interval-gain-${item.name}-${item.start_date ?? ""}-${item.end_date ?? ""}`}>
                            <td>{index + 1}</td>
                            <td>{tsCode ?? "--"}</td>
                            <td>
                              {tsCode ? (
                                <DetailsLink
                                  className="scene-layer-market-stock-link"
                                  tsCode={splitTsCode(tsCode)}
                                  tradeDate={carryInterval ? baseTradeDate : (start ?? baseTradeDate)}
                                  intervalStartTradeDate={carryInterval ? start : undefined}
                                  intervalEndTradeDate={carryInterval ? end : undefined}
                                  sourcePath={sourcePath.trim() || undefined}
                                  title={`查看 ${item.name} 详情`}
                                  navigationItems={subIntervalGainNavigationItems}
                                >
                                  {displayName}
                                </DetailsLink>
                              ) : (
                                displayName
                              )}
                            </td>
                            <td>{item.start_date && item.end_date ? `${formatDateLabel(item.start_date)} ~ ${formatDateLabel(item.end_date)}` : "--"}</td>
                            <td className={getPercentToneClass(item.value)}>{formatPercent(item.value)}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </section>

              <section className="scene-layer-market-panel scene-layer-market-gainers-panel">
                <h3>当日个股涨幅榜（{formatDateLabel(result.daily.trade_date)}）</h3>
                <div className="scene-layer-contrib-table-wrap">
                  <table className="scene-layer-contrib-table">
                    <thead>
                      <tr>
                        <th>#</th>
                        <th>代码</th>
                        <th>名称</th>
                        <th>涨幅</th>
                      </tr>
                    </thead>
                    <tbody>
                      {result.daily.gain_top.map((item, index) => {
                        const tsCode = getRankItemTsCode(item);
                        const displayName = getRankItemDisplayName(item);
                        return (
                          <tr key={`daily-gain-${item.name}`}>
                            <td>{index + 1}</td>
                            <td>{tsCode ?? "--"}</td>
                            <td>
                              {tsCode ? (
                                <DetailsLink
                                  className="scene-layer-market-stock-link"
                                  tsCode={splitTsCode(tsCode)}
                                  tradeDate={result.resolved_reference_trade_date ?? undefined}
                                  sourcePath={sourcePath.trim() || undefined}
                                  title={`查看 ${item.name} 详情`}
                                  navigationItems={dailyGainNavigationItems}
                                >
                                  {displayName}
                                </DetailsLink>
                              ) : (
                                displayName
                              )}
                            </td>
                            <td className={getPercentToneClass(item.value)}>{formatPercent(item.value)}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </section>
            </div>

            {contributionError ? <div className="scene-layer-error">{contributionError}</div> : null}
          </div>
        </section>
      ) : null}

      {isContributionModalOpen && contributionResult ? (
        <div
          className="scene-layer-modal-mask"
          role="dialog"
          aria-modal="true"
          aria-label="贡献明细"
          onClick={() => setIsContributionModalOpen(false)}
        >
          <section
            className="scene-layer-modal-card"
            onClick={(event) => event.stopPropagation()}
          >
            <header className="scene-layer-modal-header">
              <h3>
                贡献明细（{contributionResult.scope === "interval" ? "区间" : "当日"} ·
                {contributionResult.kind === "concept" ? "概念" : "行业"}：{contributionResult.name}）
              </h3>
              <button
                type="button"
                className="scene-layer-modal-close"
                onClick={() => setIsContributionModalOpen(false)}
              >
                关闭
              </button>
            </header>
            <div className="scene-layer-source-note">
              日期：
              {contributionResult.scope === "interval"
                ? `${formatDateLabel(contributionResult.start_date)} ~ ${formatDateLabel(contributionResult.end_date)}`
                : formatDateLabel(contributionResult.trade_date)}
            </div>
            <div className="scene-layer-contrib-table-wrap scene-layer-modal-table-wrap">
              <table className="scene-layer-contrib-table">
                <thead>
                  <tr>
                    <th>#</th>
                    <th>代码</th>
                    <th>名称</th>
                    <th>板块</th>
                    <th>贡献涨幅</th>
                  </tr>
                </thead>
                <tbody>
                  {contributionResult.contributors.map((item, index) => {
                    const baseTradeDate = contributionResult.trade_date ?? result?.resolved_reference_trade_date ?? undefined;
                    const start = contributionResult.scope === "interval" ? contributionResult.start_date ?? undefined : undefined;
                    const end = contributionResult.scope === "interval" ? contributionResult.end_date ?? result?.resolved_reference_trade_date ?? undefined : undefined;
                    return (
                    <tr key={`${item.ts_code}-${index}`}>
                      <td>{index + 1}</td>
                      <td>{item.ts_code}</td>
                      <td>
                         <DetailsLink
                           className="scene-layer-market-stock-link"
                           tsCode={splitTsCode(item.ts_code)}
                           tradeDate={(carryInterval || contributionResult.scope !== "interval") ? baseTradeDate : (start ?? baseTradeDate)}
                           intervalStartTradeDate={carryInterval ? start : undefined}
                           intervalEndTradeDate={carryInterval ? end : undefined}
                           sourcePath={sourcePath.trim() || undefined}
                           title={`查看 ${item.name ?? item.ts_code} 详情`}
                           navigationItems={detailNavigationItems}
                        >
                          {item.name ?? item.ts_code}
                        </DetailsLink>
                      </td>
                      <td>{item.industry ?? "--"}</td>
                      <td className={getPercentToneClass(item.contribution_pct)}>{formatPercent(item.contribution_pct)}</td>
                    </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
            {contributionLoading ? <div className="scene-layer-source-note">加载中...</div> : null}
          </section>
        </div>
      ) : null}
    </div>
  );
}
