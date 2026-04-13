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
import { useConceptExclusions } from "../../shared/conceptExclusions";
import { readStoredSourcePath } from "../../shared/storage";
import { STOCK_PICK_BOARD_OPTIONS } from "../../share/stockPickShared";
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

function isNonNull<T>(value: T | null): value is T {
  return value !== null;
}

const MARKET_BOARD_FILTER_OPTIONS = STOCK_PICK_BOARD_OPTIONS.filter(
  (item) => item !== "全部",
);

export default function MarketAnalysisPage() {
  const { excludedConcepts } = useConceptExclusions();
  const [sourcePath, setSourcePath] = useState(() => readStoredSourcePath());
  const [lookbackPeriod, setLookbackPeriod] = useState("20");
  const [referenceDateInput, setReferenceDateInput] = useState("");
  const [selectedBoard, setSelectedBoard] = useState("");
  const [initializing, setInitializing] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState<MarketAnalysisData | null>(null);
  const [contributionLoading, setContributionLoading] = useState(false);
  const [contributionError, setContributionError] = useState("");
  const [contributionResult, setContributionResult] = useState<MarketContributionData | null>(null);
  const [isContributionModalOpen, setIsContributionModalOpen] = useState(false);

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
    const tradeDate = contributionResult?.trade_date ?? result?.resolved_reference_trade_date ?? undefined;
    const sourcePathTrimmed = sourcePath.trim() || undefined;
    return contributors.map((item) => ({
      tsCode: item.ts_code,
      tradeDate,
      sourcePath: sourcePathTrimmed,
      name: item.name ?? undefined,
    }));
  }, [contributionResult, result?.resolved_reference_trade_date, sourcePath]);

  const intervalGainNavigationItems = useMemo(() => {
    if (!result) {
      return [];
    }
    const tradeDate = result.resolved_reference_trade_date ?? undefined;
    const sourcePathTrimmed = sourcePath.trim() || undefined;
    return result.interval.gain_top
      .map((item) => {
        const tsCode = extractTsCodeFromRankName(item.name);
        if (!tsCode) {
          return null;
        }
        const displayName = item.name.replace(/\s*\(\d{6}\.[A-Z]{2}\)\s*$/, "");
        return {
          tsCode,
          tradeDate,
          sourcePath: sourcePathTrimmed,
          name: displayName,
        };
      })
      .filter(isNonNull);
  }, [result, sourcePath]);

  const dailyGainNavigationItems = useMemo(() => {
    if (!result) {
      return [];
    }
    const tradeDate = result.resolved_reference_trade_date ?? undefined;
    const sourcePathTrimmed = sourcePath.trim() || undefined;
    return result.daily.gain_top
      .map((item) => {
        const tsCode = extractTsCodeFromRankName(item.name);
        if (!tsCode) {
          return null;
        }
        const displayName = item.name.replace(/\s*\(\d{6}\.[A-Z]{2}\)\s*$/, "");
        return {
          tsCode,
          tradeDate,
          sourcePath: sourcePathTrimmed,
          name: displayName,
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

        <div className="scene-layer-source-note">
          当前数据目录：<strong>{sourcePath || "--"}</strong>
        </div>

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
            <span>板块筛选（应用到个股榜）</span>
            <select value={selectedBoard} onChange={(event) => setSelectedBoard(event.target.value)}>
              <option value="">全部板块</option>
              {MARKET_BOARD_FILTER_OPTIONS.map((board) => (
                <option key={board} value={board}>
                  {board}
                </option>
              ))}
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
                    <h4>板块榜</h4>
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
                        const tsCode = extractTsCodeFromRankName(item.name);
                        const displayName = tsCode ? item.name.replace(/\s*\(\d{6}\.[A-Z]{2}\)\s*$/, "") : item.name;
                        return (
                          <tr key={`interval-gain-${item.name}`}>
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
                        const tsCode = extractTsCodeFromRankName(item.name);
                        const displayName = tsCode ? item.name.replace(/\s*\(\d{6}\.[A-Z]{2}\)\s*$/, "") : item.name;
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
                            <td>{formatPercent(item.value)}</td>
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
                  {contributionResult.contributors.map((item, index) => (
                    <tr key={`${item.ts_code}-${index}`}>
                      <td>{index + 1}</td>
                      <td>{item.ts_code}</td>
                      <td>
                        <DetailsLink
                          className="scene-layer-market-stock-link"
                          tsCode={splitTsCode(item.ts_code)}
                          tradeDate={contributionResult.trade_date ?? result?.resolved_reference_trade_date ?? undefined}
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
                  ))}
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
