import { useEffect, useMemo, useState } from "react";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import {
  getRuleLayerBacktestDefaults,
  getSceneLayerBacktestDefaults,
  runRuleLayerBacktest,
  runSceneLayerBacktest,
  type RuleLayerBacktestData,
  type RuleLayerRuleSummary,
  type SceneLayerBacktestData,
} from "../../apis/strategyTrigger";
import {
  TableSortButton,
  getAriaSort,
  useTableSort,
  type SortDefinition,
} from "../../shared/tableSort";
import { readStoredSourcePath } from "../../shared/storage";
import "./css/SceneLayerBacktestPage.css";

type RuleSummarySortKey =
  | "rule_name"
  | "point_count"
  | "avg_residual_mean"
  | "spread_mean"
  | "ic_mean"
  | "ic_std"
  | "icir";

function formatDateLabel(value?: string | null) {
  if (!value || value.length !== 8) {
    return "--";
  }
  return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`;
}

function formatNumber(value?: number | null, digits = 4) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  return value.toFixed(digits);
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

function formatPercent(value?: number | null, digits = 2) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  return `${value.toFixed(digits)}%`;
}

const INDEX_OPTIONS = [
  { value: "000001.SH", label: "上证指数" },
  { value: "399001.SZ", label: "深证成指" },
  { value: "399006.SZ", label: "创业板指" },
  { value: "000300.SH", label: "沪深300" },
  { value: "000905.SH", label: "中证500" },
  { value: "000852.SH", label: "中证1000" },
  { value: "000688.SH", label: "科创50" },
] as const;

export default function SceneLayerBacktestPage() {
  const [sourcePath, setSourcePath] = useState(() => readStoredSourcePath());
  const [stockAdjType, setStockAdjType] = useState("qfq");
  const [indexTsCode, setIndexTsCode] = useState<string>(INDEX_OPTIONS[0].value);
  const [indexBeta, setIndexBeta] = useState("0.5");
  const [conceptBeta, setConceptBeta] = useState("0.2");
  const [industryBeta, setIndustryBeta] = useState("0.0");
  const [startDateInput, setStartDateInput] = useState("");
  const [endDateInput, setEndDateInput] = useState("");
  const [minSamplesPerSceneDay, setMinSamplesPerSceneDay] = useState("5");
  const [backtestPeriod, setBacktestPeriod] = useState("1");
  const [loading, setLoading] = useState(false);
  const [initializing, setInitializing] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState<SceneLayerBacktestData | null>(null);

  const [ruleStockAdjType, setRuleStockAdjType] = useState("qfq");
  const [ruleIndexTsCode, setRuleIndexTsCode] = useState<string>(INDEX_OPTIONS[0].value);
  const [ruleIndexBeta, setRuleIndexBeta] = useState("0.5");
  const [ruleConceptBeta, setRuleConceptBeta] = useState("0.2");
  const [ruleIndustryBeta, setRuleIndustryBeta] = useState("0.0");
  const [ruleStartDateInput, setRuleStartDateInput] = useState("");
  const [ruleEndDateInput, setRuleEndDateInput] = useState("");
  const [minSamplesPerRuleDay, setMinSamplesPerRuleDay] = useState("5");
  const [ruleBacktestPeriod, setRuleBacktestPeriod] = useState("1");
  const [ruleLoading, setRuleLoading] = useState(false);
  const [ruleError, setRuleError] = useState("");
  const [ruleResult, setRuleResult] = useState<RuleLayerBacktestData | null>(null);

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

        try {
          const sceneDefaults = await getSceneLayerBacktestDefaults(resolved);
          if (cancelled) {
            return;
          }
          setStartDateInput(compactDateToInput(sceneDefaults.start_date));
          setEndDateInput(compactDateToInput(sceneDefaults.end_date));
        } catch (sceneInitError) {
          if (!cancelled) {
            setError(`读取场景默认参数失败: ${String(sceneInitError)}`);
          }
        }

        try {
          const ruleDefaults = await getRuleLayerBacktestDefaults(resolved);
          if (cancelled) {
            return;
          }
          setRuleStartDateInput(compactDateToInput(ruleDefaults.start_date));
          setRuleEndDateInput(compactDateToInput(ruleDefaults.end_date));
        } catch (ruleInitError) {
          if (!cancelled) {
            setRuleError(`读取策略默认参数失败: ${String(ruleInitError)}`);
          }
        }
      } catch (initError) {
        if (!cancelled) {
          setError(`读取回测默认参数失败: ${String(initError)}`);
          setRuleError(`读取回测默认参数失败: ${String(initError)}`);
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

  const allSceneSummaries = result?.all_scene_summaries ?? [];
  const allRuleSummaries = ruleResult?.all_rule_summaries ?? [];

  const ruleSummarySortDefinitions = useMemo(
    () =>
      ({
        rule_name: {
          value: (row: RuleLayerRuleSummary) => row.rule_name,
        },
        point_count: {
          value: (row: RuleLayerRuleSummary) => row.point_count,
        },
        avg_residual_mean: {
          value: (row: RuleLayerRuleSummary) => row.avg_residual_mean,
        },
        spread_mean: {
          value: (row: RuleLayerRuleSummary) => row.spread_mean,
        },
        ic_mean: {
          value: (row: RuleLayerRuleSummary) => row.ic_mean,
        },
        ic_std: {
          value: (row: RuleLayerRuleSummary) => row.ic_std,
        },
        icir: {
          value: (row: RuleLayerRuleSummary) => row.icir,
        },
      }) satisfies Partial<
        Record<RuleSummarySortKey, SortDefinition<RuleLayerRuleSummary>>
      >,
    [],
  );

  const {
    sortKey: ruleSummarySortKey,
    sortDirection: ruleSummarySortDirection,
    sortedRows: sortedRuleSummaries,
    toggleSort: toggleRuleSummarySort,
  } = useTableSort<RuleLayerRuleSummary, RuleSummarySortKey>(
    allRuleSummaries,
    ruleSummarySortDefinitions,
    {
      key: "spread_mean",
      direction: "desc",
    },
  );

  async function onRunBacktest() {
    const normalizedStart = normalizeDateInput(startDateInput);
    const normalizedEnd = normalizeDateInput(endDateInput);

    if (!sourcePath.trim()) {
      setError("当前数据目录为空，请先在数据管理页确认目录。");
      return;
    }
    if (!indexTsCode.trim()) {
      setError("请选择指数。");
      return;
    }
    if (!normalizedStart || !normalizedEnd) {
      setError("请填写开始和结束日期。");
      return;
    }
    if (normalizedStart > normalizedEnd) {
      setError("开始日期不能晚于结束日期。");
      return;
    }

    setLoading(true);
    setError("");
    try {
      const data = await runSceneLayerBacktest({
        sourcePath,
        stockAdjType: stockAdjType.trim() || "qfq",
        indexTsCode: indexTsCode.trim(),
        indexBeta: Number(indexBeta),
        conceptBeta: Number(conceptBeta),
        industryBeta: Number(industryBeta),
        startDate: normalizedStart,
        endDate: normalizedEnd,
        minSamplesPerSceneDay: Math.max(1, Number(minSamplesPerSceneDay) || 1),
        backtestPeriod: Math.max(1, Number(backtestPeriod) || 1),
      });
      setResult(data);
    } catch (runError) {
      setResult(null);
      setError(`执行场景整体回测失败: ${String(runError)}`);
    } finally {
      setLoading(false);
    }
  }

  async function onRunRuleBacktest() {
    const normalizedStart = normalizeDateInput(ruleStartDateInput);
    const normalizedEnd = normalizeDateInput(ruleEndDateInput);

    if (!sourcePath.trim()) {
      setRuleError("当前数据目录为空，请先在数据管理页确认目录。");
      return;
    }
    if (!ruleIndexTsCode.trim()) {
      setRuleError("请选择指数。");
      return;
    }
    if (!normalizedStart || !normalizedEnd) {
      setRuleError("请填写开始和结束日期。");
      return;
    }
    if (normalizedStart > normalizedEnd) {
      setRuleError("开始日期不能晚于结束日期。");
      return;
    }

    setRuleLoading(true);
    setRuleError("");
    try {
      const data = await runRuleLayerBacktest({
        sourcePath,
        stockAdjType: ruleStockAdjType.trim() || "qfq",
        indexTsCode: ruleIndexTsCode.trim(),
        indexBeta: Number(ruleIndexBeta),
        conceptBeta: Number(ruleConceptBeta),
        industryBeta: Number(ruleIndustryBeta),
        startDate: normalizedStart,
        endDate: normalizedEnd,
        minSamplesPerRuleDay: Math.max(1, Number(minSamplesPerRuleDay) || 1),
        backtestPeriod: Math.max(1, Number(ruleBacktestPeriod) || 1),
      });
      setRuleResult(data);
    } catch (runError) {
      setRuleResult(null);
      setRuleError(`执行策略回测失败: ${String(runError)}`);
    } finally {
      setRuleLoading(false);
    }
  }

  return (
    <div className="scene-layer-page">
      <section className="scene-layer-card">
        <h2 className="scene-layer-title">场景整体回测</h2>
        <p className="scene-layer-caption">
          使用 scene_details 中的场景状态与排序，计算各场景状态下的分层残差收益、Top-Bottom Spread、IC / ICIR。
        </p>

        <div className="scene-layer-source-note">
          当前数据目录：<strong>{sourcePath || "--"}</strong>
        </div>

        <div className="scene-layer-form-grid">
          <label className="scene-layer-field">
            <span>股票复权</span>
            <input value={stockAdjType} onChange={(event) => setStockAdjType(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>指数</span>
            <select value={indexTsCode} onChange={(event) => setIndexTsCode(event.target.value)}>
              {INDEX_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
          <label className="scene-layer-field">
            <span>指数 Beta</span>
            <input type="number" step="0.01" value={indexBeta} onChange={(event) => setIndexBeta(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>概念 Beta</span>
            <input type="number" step="0.01" value={conceptBeta} onChange={(event) => setConceptBeta(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>行业 Beta</span>
            <input type="number" step="0.01" value={industryBeta} onChange={(event) => setIndustryBeta(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>开始日期（scene_details 最早）</span>
            <input type="date" value={startDateInput} onChange={(event) => setStartDateInput(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>结束日期（scene_details 最晚）</span>
            <input type="date" value={endDateInput} onChange={(event) => setEndDateInput(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>场景日最少样本</span>
            <input type="number" min="1" value={minSamplesPerSceneDay} onChange={(event) => setMinSamplesPerSceneDay(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>回测周期（天）</span>
            <input type="number" min="1" value={backtestPeriod} onChange={(event) => setBacktestPeriod(event.target.value)} />
          </label>
        </div>

        <div className="scene-layer-actions">
          <button type="button" className="scene-layer-primary-btn" onClick={() => void onRunBacktest()} disabled={loading || initializing}>
            {loading ? "回测中..." : "执行场景整体回测"}
          </button>
        </div>

        {error ? <div className="scene-layer-error">{error}</div> : null}
      </section>


      {result ? (
        <section className="scene-layer-card">
          <div className="scene-layer-summary-grid">
            <div className="scene-layer-summary-item">
              <span>场景</span>
              <strong>全部场景</strong>
            </div>
            <div className="scene-layer-summary-item">
              <span>区间</span>
              <strong>{formatDateLabel(result.start_date)} ~ {formatDateLabel(result.end_date)}</strong>
            </div>
            <div className="scene-layer-summary-item">
              <span>场景数</span>
              <strong>{allSceneSummaries.length}</strong>
            </div>
            <div className="scene-layer-summary-item">
              <span>最小样本阈值</span>
              <strong>{result.min_samples_per_scene_day}</strong>
            </div>
            <div className="scene-layer-summary-item">
              <span>回测周期（天）</span>
              <strong>{result.backtest_period}</strong>
            </div>
          </div>

          {allSceneSummaries.length === 0 ? (
            <div className="scene-layer-empty">当前没有可回测的场景。</div>
          ) : null}

          {allSceneSummaries.length > 0 ? (
            <div className="scene-layer-layer-summary">
              <h3>全部场景汇总（按 Spread 均值降序）</h3>
              <div className="scene-layer-layer-grid">
                {allSceneSummaries.map((item) => (
                  <div key={item.scene_name} className="scene-layer-layer-item">
                    <span className="scene-layer-layer-state">{item.scene_name}</span>
                    <span>有效交易日：{item.point_count}</span>
                    <span>Spread 均值：{formatPercent(item.spread_mean)}</span>
                    <span>IC 均值：{formatNumber(item.ic_mean)}</span>
                    <span>ICIR：{formatNumber(item.icir)}</span>
                  </div>
                ))}
              </div>
            </div>
          ) : null}
        </section>
      ) : null}

      <section className="scene-layer-card">
        <h2 className="scene-layer-title">策略回测</h2>
        <p className="scene-layer-caption">
          使用 rule_details 中的策略得分与残差收益，计算策略日度均值、Top-Bottom Spread、IC / ICIR。
        </p>

        <div className="scene-layer-source-note">
          当前数据目录：<strong>{sourcePath || "--"}</strong>
        </div>

        <div className="scene-layer-form-grid">
          <label className="scene-layer-field">
            <span>股票复权</span>
            <input value={ruleStockAdjType} onChange={(event) => setRuleStockAdjType(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>指数</span>
            <select value={ruleIndexTsCode} onChange={(event) => setRuleIndexTsCode(event.target.value)}>
              {INDEX_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
          <label className="scene-layer-field">
            <span>指数 Beta</span>
            <input type="number" step="0.01" value={ruleIndexBeta} onChange={(event) => setRuleIndexBeta(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>概念 Beta</span>
            <input type="number" step="0.01" value={ruleConceptBeta} onChange={(event) => setRuleConceptBeta(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>行业 Beta</span>
            <input type="number" step="0.01" value={ruleIndustryBeta} onChange={(event) => setRuleIndustryBeta(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>开始日期（rule_details 最早）</span>
            <input type="date" value={ruleStartDateInput} onChange={(event) => setRuleStartDateInput(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>结束日期（rule_details 最晚）</span>
            <input type="date" value={ruleEndDateInput} onChange={(event) => setRuleEndDateInput(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>策略日最少样本</span>
            <input type="number" min="1" value={minSamplesPerRuleDay} onChange={(event) => setMinSamplesPerRuleDay(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>回测周期（天）</span>
            <input type="number" min="1" value={ruleBacktestPeriod} onChange={(event) => setRuleBacktestPeriod(event.target.value)} />
          </label>
        </div>

        <div className="scene-layer-actions">
          <button type="button" className="scene-layer-primary-btn" onClick={() => void onRunRuleBacktest()} disabled={ruleLoading || initializing}>
            {ruleLoading ? "回测中..." : "执行策略回测"}
          </button>
        </div>

        {ruleError ? <div className="scene-layer-error">{ruleError}</div> : null}
      </section>

      {ruleResult ? (
        <section className="scene-layer-card">
          <div className="scene-layer-layer-summary">
            <h3>策略回测汇总</h3>
            <div className="scene-layer-contrib-table-wrap">
              <table className="scene-layer-contrib-table">
                <thead>
                  <tr>
                    <th>策略</th>
                    <th>区间</th>
                    <th>指数</th>
                    <th>Beta（指/概/行）</th>
                    <th>策略数</th>
                    <th>最小样本阈值</th>
                    <th>回测周期（天）</th>
                    <th>残差均值（日度）</th>
                    <th>Spread 均值（日度高分-低分）</th>
                    <th>IC 均值</th>
                    <th>IC 标准差</th>
                    <th>ICIR</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td>全部策略</td>
                    <td>{formatDateLabel(ruleResult.start_date)} ~ {formatDateLabel(ruleResult.end_date)}</td>
                    <td>{ruleResult.index_ts_code}</td>
                    <td>{formatNumber(ruleResult.index_beta, 2)} / {formatNumber(ruleResult.concept_beta, 2)} / {formatNumber(ruleResult.industry_beta, 2)}</td>
                    <td>{allRuleSummaries.length}</td>
                    <td>{ruleResult.min_samples_per_rule_day}</td>
                    <td>{ruleResult.backtest_period}</td>
                    <td>{formatPercent(ruleResult.avg_residual_mean)}</td>
                    <td>{formatPercent(ruleResult.spread_mean)}</td>
                    <td>{formatNumber(ruleResult.ic_mean)}</td>
                    <td>{formatNumber(ruleResult.ic_std)}</td>
                    <td>{formatNumber(ruleResult.icir)}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>

          {allRuleSummaries.length === 0 ? (
            <div className="scene-layer-empty">当前没有可回测的策略。</div>
          ) : null}

          {allRuleSummaries.length > 0 ? (
            <div className="scene-layer-layer-summary">
              <h3>全部策略明细（点击表头排序）</h3>
              <div className="scene-layer-contrib-table-wrap">
                <table className="scene-layer-contrib-table scene-layer-rule-detail-table">
                  <thead>
                    <tr>
                      <th aria-sort={getAriaSort(ruleSummarySortKey === "rule_name", ruleSummarySortDirection)}>
                        <TableSortButton
                          label="策略名"
                          isActive={ruleSummarySortKey === "rule_name" && ruleSummarySortDirection !== null}
                          direction={ruleSummarySortDirection}
                          onClick={() => toggleRuleSummarySort("rule_name")}
                          title="按策略名排序"
                        />
                      </th>
                      <th aria-sort={getAriaSort(ruleSummarySortKey === "point_count", ruleSummarySortDirection)}>
                        <TableSortButton
                          label="有效交易日"
                          isActive={ruleSummarySortKey === "point_count" && ruleSummarySortDirection !== null}
                          direction={ruleSummarySortDirection}
                          onClick={() => toggleRuleSummarySort("point_count")}
                          title="按有效交易日排序"
                        />
                      </th>
                      <th aria-sort={getAriaSort(ruleSummarySortKey === "avg_residual_mean", ruleSummarySortDirection)}>
                        <TableSortButton
                          label="残差均值（日度）"
                          isActive={ruleSummarySortKey === "avg_residual_mean" && ruleSummarySortDirection !== null}
                          direction={ruleSummarySortDirection}
                          onClick={() => toggleRuleSummarySort("avg_residual_mean")}
                          title="按残差均值排序"
                        />
                      </th>
                      <th aria-sort={getAriaSort(ruleSummarySortKey === "spread_mean", ruleSummarySortDirection)}>
                        <TableSortButton
                          label="Spread 均值"
                          isActive={ruleSummarySortKey === "spread_mean" && ruleSummarySortDirection !== null}
                          direction={ruleSummarySortDirection}
                          onClick={() => toggleRuleSummarySort("spread_mean")}
                          title="按 Spread 均值排序"
                        />
                      </th>
                      <th aria-sort={getAriaSort(ruleSummarySortKey === "ic_mean", ruleSummarySortDirection)}>
                        <TableSortButton
                          label="IC 均值"
                          isActive={ruleSummarySortKey === "ic_mean" && ruleSummarySortDirection !== null}
                          direction={ruleSummarySortDirection}
                          onClick={() => toggleRuleSummarySort("ic_mean")}
                          title="按 IC 均值排序"
                        />
                      </th>
                      <th aria-sort={getAriaSort(ruleSummarySortKey === "ic_std", ruleSummarySortDirection)}>
                        <TableSortButton
                          label="IC 标准差"
                          isActive={ruleSummarySortKey === "ic_std" && ruleSummarySortDirection !== null}
                          direction={ruleSummarySortDirection}
                          onClick={() => toggleRuleSummarySort("ic_std")}
                          title="按 IC 标准差排序"
                        />
                      </th>
                      <th aria-sort={getAriaSort(ruleSummarySortKey === "icir", ruleSummarySortDirection)}>
                        <TableSortButton
                          label="ICIR"
                          isActive={ruleSummarySortKey === "icir" && ruleSummarySortDirection !== null}
                          direction={ruleSummarySortDirection}
                          onClick={() => toggleRuleSummarySort("icir")}
                          title="按 ICIR 排序"
                        />
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {sortedRuleSummaries.map((item) => (
                      <tr key={item.rule_name}>
                        <td>{item.rule_name}</td>
                        <td>{item.point_count}</td>
                        <td>{formatPercent(item.avg_residual_mean)}</td>
                        <td>{formatPercent(item.spread_mean)}</td>
                        <td>{formatNumber(item.ic_mean)}</td>
                        <td>{formatNumber(item.ic_std)}</td>
                        <td>{formatNumber(item.icir)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          ) : null}
        </section>
      ) : null}
    </div>
  );
}
