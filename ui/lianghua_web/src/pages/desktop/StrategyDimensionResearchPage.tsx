import { useMemo, useState } from "react";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import {
  getStrategyDimensionResearchDefaults,
  runStrategyDimensionResearch,
  type StrategyDimensionPairMetrics,
  type StrategyDimensionResearchData,
  type StrategyDimensionResearchDefaultsData,
} from "../../apis/strategyDimensionResearch";
import { readStoredSourcePath } from "../../shared/storage";
import "./css/StrategyDimensionResearchPage.css";

const formatNumber = (value: number | null | undefined, digits = 3) =>
  value == null || !Number.isFinite(value) ? "—" : value.toFixed(digits);

const formatPercent = (value: number | null | undefined) =>
  value == null || !Number.isFinite(value)
    ? "—"
    : `${(value * 100).toFixed(2)}%`;

const formatPctPoint = (value: number | null | undefined) =>
  value == null || !Number.isFinite(value) ? "—" : `${value.toFixed(3)}%`;

const compactToInputDate = (value: string | null | undefined) =>
  value && /^\d{8}$/.test(value)
    ? `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`
    : (value ?? "");

const inputToCompactDate = (value: string) => value.replaceAll("-", "");

const dependenceLabel = (value: number | null | undefined) => {
  if (value == null || !Number.isFinite(value)) return "样本不足";
  const strength = Math.abs(value);
  if (strength >= 0.8) return "很强";
  if (strength >= 0.6) return "较强";
  if (strength >= 0.3) return "中等";
  return "较弱";
};

const residualLabel = (value: number | null | undefined) => {
  if (value == null || !Number.isFinite(value)) return "无法估计";
  if (value < 0.25) return "高度可解释";
  if (value < 0.6) return "部分独立";
  return "线性增量较高";
};

export default function StrategyDimensionResearchPage() {
  const [sourcePath, setSourcePath] = useState(() => readStoredSourcePath());
  const [defaults, setDefaults] =
    useState<StrategyDimensionResearchDefaultsData | null>(null);
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [selectedRules, setSelectedRules] = useState<string[]>([]);
  const [coreRules, setCoreRules] = useState<string[]>([]);
  const [ruleFilter, setRuleFilter] = useState("");
  const [nonlinearSampleLimit, setNonlinearSampleLimit] = useState("512");
  const [ridgeLambda, setRidgeLambda] = useState("0.000001");
  const [holdingPeriod, setHoldingPeriod] = useState("5");
  const [initializing, setInitializing] = useState(false);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState<StrategyDimensionResearchData | null>(
    null,
  );

  const loadResearchDefaults = async () => {
    setInitializing(true);
    setError("");
    setResult(null);
    try {
      const resolvedSourcePath = await ensureManagedSourcePath();
      const loadedDefaults =
        await getStrategyDimensionResearchDefaults(resolvedSourcePath);
      const rulesByActivity = [...loadedDefaults.rule_options].sort(
        (left, right) =>
          right.trigger_count - left.trigger_count ||
          left.rule_name.localeCompare(right.rule_name),
      );
      setSourcePath(resolvedSourcePath);
      setDefaults(loadedDefaults);
      setStartDate(compactToInputDate(loadedDefaults.start_date));
      setEndDate(compactToInputDate(loadedDefaults.end_date));
      setNonlinearSampleLimit(
        String(loadedDefaults.default_nonlinear_sample_limit),
      );
      setRidgeLambda(String(loadedDefaults.default_ridge_lambda));
      setHoldingPeriod(String(loadedDefaults.default_holding_period));
      const initiallySelectedRules = rulesByActivity
        .slice(0, Math.min(10, loadedDefaults.max_strategy_count))
        .map((rule) => rule.rule_name);
      setSelectedRules(initiallySelectedRules);
      setCoreRules(initiallySelectedRules.slice(0, 1));
    } catch (initializeError) {
      setDefaults(null);
      setError(`读取研究数据失败：${String(initializeError)}`);
    } finally {
      setInitializing(false);
    }
  };

  const filteredRuleOptions = useMemo(() => {
    const keyword = ruleFilter.trim().toLocaleLowerCase();
    return [...(defaults?.rule_options ?? [])]
      .filter(
        (rule) =>
          !keyword || rule.rule_name.toLocaleLowerCase().includes(keyword),
      )
      .sort(
        (left, right) =>
          right.trigger_count - left.trigger_count ||
          left.rule_name.localeCompare(right.rule_name),
      );
  }, [defaults, ruleFilter]);

  const pairByRules = useMemo(() => {
    const pairs = new Map<string, StrategyDimensionPairMetrics>();
    for (const pair of result?.pair_metrics ?? []) {
      pairs.set(`${pair.left_rule_name}\u0000${pair.right_rule_name}`, pair);
      pairs.set(`${pair.right_rule_name}\u0000${pair.left_rule_name}`, pair);
    }
    return pairs;
  }, [result]);

  const strongestDistancePair = useMemo(
    () =>
      [...(result?.pair_metrics ?? [])]
        .filter(
          (pair) =>
            pair.distance_correlation_daily_mean != null &&
            (coreRules.includes(pair.left_rule_name) ||
              coreRules.includes(pair.right_rule_name)),
        )
        .sort(
          (left, right) =>
            (right.distance_correlation_daily_mean ?? -1) -
            (left.distance_correlation_daily_mean ?? -1),
        )[0],
    [coreRules, result],
  );

  const strongestOverlapPair = useMemo(
    () =>
      [...(result?.pair_metrics ?? [])]
        .filter(
          (pair) =>
            pair.jaccard != null &&
            (coreRules.includes(pair.left_rule_name) ||
              coreRules.includes(pair.right_rule_name)),
        )
        .sort((left, right) => (right.jaccard ?? -1) - (left.jaccard ?? -1))[0],
    [coreRules, result],
  );

  const mostExplainedRule = useMemo(
    () =>
      [...(result?.orthogonal_diagnostics ?? [])]
        .filter(
          (item) =>
            item.residual_variance_ratio != null &&
            item.basis_coefficients.length > 0,
        )
        .sort(
          (left, right) =>
            (left.residual_variance_ratio ?? 1) -
            (right.residual_variance_ratio ?? 1),
        )[0],
    [result],
  );

  const strongestFullPeriodReturn = useMemo(
    () =>
      [...(result?.return_summaries ?? [])]
        .filter(
          (summary) =>
            (summary.avg_residual_return ?? 0) > 0 &&
            summary.hac_t_value != null,
        )
        .sort(
          (left, right) =>
            (right.hac_t_value ?? Number.NEGATIVE_INFINITY) -
            (left.hac_t_value ?? Number.NEGATIVE_INFINITY),
        )[0],
    [result],
  );

  const strongestPositiveIncrement = useMemo(
    () =>
      [...(result?.return_increments ?? [])]
        .filter(
          (increment) =>
            !coreRules.includes(increment.rule_name) &&
            (increment.test_incremental_mean ?? 0) > 0 &&
            increment.test_incremental_hac_t_value != null,
        )
        .sort(
          (left, right) =>
            (right.test_incremental_hac_t_value ?? Number.NEGATIVE_INFINITY) -
            (left.test_incremental_hac_t_value ?? Number.NEGATIVE_INFINITY),
        )[0],
    [coreRules, result],
  );

  const strongestNegativeIncrement = useMemo(
    () =>
      [...(result?.return_increments ?? [])]
        .filter(
          (increment) =>
            !coreRules.includes(increment.rule_name) &&
            (increment.test_incremental_mean ?? 0) < 0 &&
            increment.test_incremental_hac_t_value != null,
        )
        .sort(
          (left, right) =>
            (left.test_incremental_hac_t_value ?? Number.POSITIVE_INFINITY) -
            (right.test_incremental_hac_t_value ?? Number.POSITIVE_INFINITY),
        )[0],
    [coreRules, result],
  );

  const strongestMarketDependence = useMemo(
    () =>
      (result?.style_exposures ?? [])
        .map((exposure) => ({
          ruleName: exposure.rule_name,
          value: exposure.dimensions.find(
            (dimension) => dimension.key === "market_regime",
          )?.value,
        }))
        .filter(
          (item): item is { ruleName: string; value: number } =>
            item.value != null && Number.isFinite(item.value),
        )
        .sort((left, right) => Math.abs(right.value) - Math.abs(left.value))[0],
    [result],
  );

  const styleAnalysis = useMemo(() => {
    const exposures = result?.style_exposures ?? [];
    const firstDimensions = exposures[0]?.dimensions ?? [];
    const dimensions = firstDimensions
      .map((dimension) => {
        const values = exposures
          .map(
            (exposure) =>
              exposure.dimensions.find((item) => item.key === dimension.key)
                ?.value,
          )
          .filter((value): value is number => value != null && Number.isFinite(value));
        return {
          ...dimension,
          averageAbs:
            values.length > 0
              ? values.reduce((sum, value) => sum + Math.abs(value), 0) /
                values.length
              : 0,
          activeCount: values.filter((value) => Math.abs(value) >= 0.3).length,
        };
      })
      .sort((left, right) => right.averageAbs - left.averageAbs);
    return dimensions.length > 0
      ? {
          dimensions,
          dominant: dimensions[0],
          neutralCount: dimensions.filter((dimension) => dimension.averageAbs < 0.3)
            .length,
        }
      : null;
  }, [result]);

  const toggleRule = (ruleName: string) => {
    setResult(null);
    if (selectedRules.includes(ruleName)) {
      setSelectedRules((current) =>
        current.filter((name) => name !== ruleName),
      );
      setCoreRules((current) => current.filter((name) => name !== ruleName));
      return;
    }
    if (selectedRules.length >= (defaults?.max_strategy_count ?? 20)) return;
    setSelectedRules((current) => [...current, ruleName]);
    if (selectedRules.length === 0) setCoreRules([ruleName]);
  };

  const toggleCoreRule = (ruleName: string) => {
    setResult(null);
    setCoreRules((current) => {
      if (current.includes(ruleName))
        return current.filter((name) => name !== ruleName);
      return [...current, ruleName];
    });
  };

  const runResearch = async () => {
    if (!sourcePath) {
      setError("尚未找到结果库路径，请先在原数据管理中完成数据初始化。");
      return;
    }
    if (!startDate || !endDate) {
      setError("请选择完整的开始和结束日期。");
      return;
    }
    if (selectedRules.length < 2) {
      setError("至少选择两个不同规则才能进行相关性研究。");
      return;
    }
    if (coreRules.length === 0) {
      setError("请至少选择一个核心策略作为比较基准。");
      return;
    }
    const sampleLimit = Number(nonlinearSampleLimit);
    const lambda = Number(ridgeLambda);
    const holdingDays = Number(holdingPeriod);
    if (
      !Number.isInteger(sampleLimit) ||
      sampleLimit < 3 ||
      sampleLimit > (defaults?.max_nonlinear_sample_limit ?? 1024)
    ) {
      setError(
        `非线性样本数必须是 3 到 ${defaults?.max_nonlinear_sample_limit ?? 1024} 的整数。`,
      );
      return;
    }
    if (!Number.isFinite(lambda) || lambda < 0) {
      setError("岭正则系数必须是有限的非负数。");
      return;
    }
    if (
      !Number.isInteger(holdingDays) ||
      holdingDays < 1 ||
      holdingDays > (defaults?.max_holding_period ?? 60)
    ) {
      setError(
        `持有交易日必须是 1 到 ${defaults?.max_holding_period ?? 60} 的整数。`,
      );
      return;
    }
    setRunning(true);
    setError("");
    try {
      setResult(
        await runStrategyDimensionResearch({
          sourcePath,
          startDate: inputToCompactDate(startDate),
          endDate: inputToCompactDate(endDate),
          ruleNames: [
            ...selectedRules.filter((ruleName) => coreRules.includes(ruleName)),
            ...selectedRules.filter(
              (ruleName) => !coreRules.includes(ruleName),
            ),
          ],
          nonlinearSampleLimit: sampleLimit,
          ridgeLambda: lambda,
          holdingPeriod: holdingDays,
        }),
      );
    } catch (researchError) {
      setResult(null);
      setError(`研究计算失败：${String(researchError)}`);
    } finally {
      setRunning(false);
    }
  };

  return (
    <main className="dimension-research-page">
      <section className="dimension-research-hero">
        <div className="dimension-research-hero-copy">
          {result ? (
            <span className="dimension-research-status">
              实证结果 · {result.start_date}—{result.end_date}
            </span>
          ) : null}
          <h2>相关性与正交研究</h2>
        </div>
      </section>

      <section
        className="dimension-research-workbench"
        aria-labelledby="dimension-workbench-title"
      >
        <div className="dimension-research-section-heading">
          <div>
            <h3 id="dimension-workbench-title">选择规则并运行研究</h3>
          </div>
          <div className="dimension-research-load-control">
            <button
              type="button"
              onClick={() => void loadResearchDefaults()}
              disabled={initializing || running}
            >
              {initializing
                ? "正在扫描结果库…"
                : defaults
                  ? "重新加载日期与规则"
                  : "加载日期与规则"}
            </button>
          </div>
        </div>

        <div className="dimension-research-config-grid">
          <div className="dimension-research-fields">
            <label>
              <span>开始日期</span>
              <input
                type="date"
                value={startDate}
                max={endDate || undefined}
                onChange={(event) => {
                  setStartDate(event.target.value);
                  setResult(null);
                }}
                disabled={initializing || running}
              />
            </label>
            <label>
              <span>结束日期</span>
              <input
                type="date"
                value={endDate}
                min={startDate || undefined}
                onChange={(event) => {
                  setEndDate(event.target.value);
                  setResult(null);
                }}
                disabled={initializing || running}
              />
            </label>
            <label>
              <span>非线性抽样交易日</span>
              <input
                type="number"
                min={3}
                max={defaults?.max_nonlinear_sample_limit ?? 1024}
                step={1}
                value={nonlinearSampleLimit}
                onChange={(event) => {
                  setNonlinearSampleLimit(event.target.value);
                  setResult(null);
                }}
                disabled={initializing || running}
              />
            </label>
            <label>
              <span>岭正则系数</span>
              <input
                type="number"
                min={0}
                step="0.000001"
                value={ridgeLambda}
                onChange={(event) => {
                  setRidgeLambda(event.target.value);
                  setResult(null);
                }}
                disabled={initializing || running}
              />
            </label>
            <label>
              <span>持有交易日</span>
              <input
                type="number"
                min={1}
                max={defaults?.max_holding_period ?? 60}
                step={1}
                value={holdingPeriod}
                onChange={(event) => {
                  setHoldingPeriod(event.target.value);
                  setResult(null);
                }}
                disabled={initializing || running}
              />
            </label>
          </div>

          <div className="dimension-research-selection">
            <div className="dimension-research-selection-head">
              <div>
                <strong>核心策略选择</strong>
                <small>
                  核心 {coreRules.length} · 已选 {selectedRules.length}/
                  {defaults?.max_strategy_count ?? 20}
                </small>
                <small className="dimension-research-order-note">
                  可选择多个代表性核心策略。运行时核心组自动优先，其他规则用于判断相对核心组的重复程度和新增收益。
                </small>
              </div>
              <button
                type="button"
                onClick={() => {
                  setSelectedRules([]);
                  setCoreRules([]);
                  setResult(null);
                }}
                disabled={running || selectedRules.length === 0}
              >
                清空
              </button>
            </div>
            <ol>
              {selectedRules.map((ruleName) => (
                <li
                  className={coreRules.includes(ruleName) ? "core" : ""}
                  key={ruleName}
                >
                  <span>{coreRules.includes(ruleName) ? "核" : "比"}</span>
                  <strong title={ruleName}>{ruleName}</strong>
                  <button
                    className="dimension-research-core-toggle"
                    type="button"
                    aria-pressed={coreRules.includes(ruleName)}
                    onClick={() => toggleCoreRule(ruleName)}
                    disabled={running}
                  >
                    {coreRules.includes(ruleName) ? "取消核心" : "设为核心"}
                  </button>
                  <button
                    type="button"
                    aria-label={`移除 ${ruleName}`}
                    onClick={() => toggleRule(ruleName)}
                    disabled={running}
                  >
                    ×
                  </button>
                </li>
              ))}
            </ol>
          </div>
        </div>

        <div className="dimension-research-rule-picker">
          <div className="dimension-research-rule-picker-head">
            <label>
              <span>筛选规则</span>
              <input
                type="search"
                value={ruleFilter}
                placeholder="输入规则名称"
                onChange={(event) => setRuleFilter(event.target.value)}
                disabled={initializing || running}
              />
            </label>
            <button
              type="button"
              onClick={() => {
                const nextRules = filteredRuleOptions
                  .slice(0, defaults?.max_strategy_count ?? 20)
                  .map((rule) => rule.rule_name);
                setSelectedRules(nextRules);
                setCoreRules(nextRules.slice(0, 1));
                setResult(null);
              }}
              disabled={
                initializing || running || filteredRuleOptions.length === 0
              }
            >
              选择当前前{" "}
              {Math.min(
                filteredRuleOptions.length,
                defaults?.max_strategy_count ?? 20,
              )}{" "}
              条
            </button>
          </div>
          <div className="dimension-research-rule-options">
            {filteredRuleOptions.map((rule) => (
              <label
                className={
                  selectedRules.includes(rule.rule_name) ? "selected" : ""
                }
                key={rule.rule_name}
              >
                <input
                  type="checkbox"
                  checked={selectedRules.includes(rule.rule_name)}
                  onChange={() => toggleRule(rule.rule_name)}
                  disabled={
                    running ||
                    (!selectedRules.includes(rule.rule_name) &&
                      selectedRules.length >=
                        (defaults?.max_strategy_count ?? 20))
                  }
                />
                <span title={rule.rule_name}>{rule.rule_name}</span>
                <small>{rule.trigger_count.toLocaleString()} 次</small>
              </label>
            ))}
          </div>
        </div>

        {error ? (
          <div className="dimension-research-error" role="alert">
            {error}
          </div>
        ) : null}
        <div className="dimension-research-run-row">
          <button
            className="dimension-research-run-button"
            type="button"
            onClick={() => void runResearch()}
            disabled={initializing || running || !defaults}
          >
            {running ? "正在计算研究结果…" : "运行相关性、收益与正交研究"}
          </button>
        </div>
      </section>

      {result ? (
        <>
          <section className="dimension-research-summary" aria-label="研究概览">
            <article>
              <span>完整股票日宇宙</span>
              <strong>{result.universe_sample_count.toLocaleString()}</strong>
              <small>未触发规则按 0 分进入线性统计</small>
            </article>
            <article>
              <span>研究规则</span>
              <strong>{result.strategies.length}</strong>
              <small>核心策略组优先进入比较基准</small>
            </article>
            <article>
              <span>规则组合</span>
              <strong>{result.pair_metrics.length}</strong>
              <small>每一对均计算重叠和相关</small>
            </article>
            <article>
              <span>持仓收益口径</span>
              <strong>{result.holding_period} 日</strong>
              <small>
                次日开盘入场；每日触发至少 {result.return_min_samples_per_day}{" "}
                只
              </small>
            </article>
          </section>

          <section className="dimension-research-section">
            <div className="dimension-research-section-heading">
              <div>
                <h3>自动参考解读</h3>
                <p className="dimension-research-section-note">
                  这是按固定参考线生成的筛查提示：Jaccard ≥ 0.70 视为高重叠，
                  |HAC t| ≥ 2 视为较强统计证据，|市场相关| ≥ 0.30
                  视为明显状态依赖。它不包含手续费、滑点、冲击成本和容量约束。
                </p>
              </div>
            </div>
            <div className="dimension-research-insight-grid">
              <article>
                <span>相对核心组触发重叠最高</span>
                <strong>
                  {strongestOverlapPair
                    ? `${strongestOverlapPair.left_rule_name} / ${strongestOverlapPair.right_rule_name}`
                    : "样本不足"}
                </strong>
                <p>
                  {strongestOverlapPair
                    ? `Jaccard 为 ${formatNumber(strongestOverlapPair.jaccard)}，${(strongestOverlapPair.jaccard ?? 0) >= 0.7 ? "达到高重叠参考线，组合前应核对是否重复持仓。" : "未达到高重叠参考线。"} 它描述共同选中，不描述分数方向。`
                    : "没有可计算的共同触发数据。"}
                </p>
              </article>
              <article>
                <span>相对核心组一般依赖最强</span>
                <strong>
                  {strongestDistancePair
                    ? `${strongestDistancePair.left_rule_name} / ${strongestDistancePair.right_rule_name}`
                    : "样本不足"}
                </strong>
                <p>
                  {strongestDistancePair
                    ? `日度距离相关为 ${formatNumber(strongestDistancePair.distance_correlation_daily_mean)}（${dependenceLabel(strongestDistancePair.distance_correlation_daily_mean)}）。即使 Pearson 较低，也要警惕曲线或阈值型共同变化。`
                    : "有效抽样交易日不足，无法计算距离相关。"}
                </p>
              </article>
              <article>
                <span>被已有基准解释最多</span>
                <strong>{mostExplainedRule?.rule_name ?? "暂无"}</strong>
                <p>
                  {mostExplainedRule
                    ? `线性残差比例为 ${formatPercent(mostExplainedRule.residual_variance_ratio)}，属于“${residualLabel(mostExplainedRule.residual_variance_ratio)}”。该结论依赖核心组和系统内部稳定顺序。`
                    : "首个核心策略没有已有基准；增加规则后才能形成解释关系。"}
                </p>
              </article>
              <article>
                <span>全区间正收益证据最强</span>
                <strong>
                  {strongestFullPeriodReturn?.rule_name ?? "样本不足"}
                </strong>
                <p>
                  {strongestFullPeriodReturn
                    ? `日均残差收益 ${formatPctPoint(strongestFullPeriodReturn.avg_residual_return)}，HAC t 为 ${formatNumber(strongestFullPeriodReturn.hac_t_value, 2)}；${(strongestFullPeriodReturn.hac_t_value ?? 0) >= 2 ? "达到较强证据参考线。" : "尚未达到较强证据参考线。"} 全区间结果仍可能包含选择偏差。`
                    : "没有足够的正收益有效日期。"}
                </p>
              </article>
              <article>
                <span>样本外正增量候选</span>
                <strong>
                  {strongestPositiveIncrement?.rule_name ?? "暂无"}
                </strong>
                <p>
                  {strongestPositiveIncrement
                    ? `扣除核心组和此前已比较规则后，检验期增量 ${formatPctPoint(strongestPositiveIncrement.test_incremental_mean)}，HAC t 为 ${formatNumber(strongestPositiveIncrement.test_incremental_hac_t_value, 2)}，正增量日期占 ${formatPercent(strongestPositiveIncrement.test_incremental_positive_ratio)}；${(strongestPositiveIncrement.test_incremental_hac_t_value ?? 0) >= 2 ? "达到较强证据参考线。" : "仅可视为正向线索。"}`
                    : "非核心规则中没有可计算的正向样本外增量。"}
                </p>
              </article>
              <article>
                <span>样本外负增量警示</span>
                <strong>
                  {strongestNegativeIncrement?.rule_name ?? "暂无"}
                </strong>
                <p>
                  {strongestNegativeIncrement
                    ? `检验期增量 ${formatPctPoint(strongestNegativeIncrement.test_incremental_mean)}，HAC t 为 ${formatNumber(strongestNegativeIncrement.test_incremental_hac_t_value, 2)}；${(strongestNegativeIncrement.test_incremental_hac_t_value ?? 0) <= -2 ? "达到负向警示参考线，建议降权、移除或换序复核。" : "负向证据尚不充分。"}`
                    : "非核心规则中没有可计算的负向样本外增量。"}
                </p>
              </article>
              <article>
                <span>市场状态依赖</span>
                <strong>
                  {strongestMarketDependence?.ruleName ?? "样本不足"}
                </strong>
                <p>
                  {strongestMarketDependence
                    ? `与 ${result.return_index_ts_code} 涨跌的覆盖率相关为 ${formatNumber(strongestMarketDependence.value)}；${Math.abs(strongestMarketDependence.value) >= 0.3 ? "达到明显状态依赖参考线，跨行情阶段使用时应分段复核。" : "最强者仍低于 0.30，当前规则整体未显示明显市场状态依赖。"}`
                    : "没有可计算的市场状态暴露。"}
                </p>
              </article>
              <article>
                <span>核心组说明</span>
                <strong>核心规则可以多选</strong>
                <p>
                  系统将核心策略组放在内部序列前面，非核心规则按稳定顺序继续计算。核心组内部仍是顺序正交，因此结果表示相对整组已有代表策略的边际贡献，不代表单条规则的绝对排名。
                </p>
              </article>
            </div>
          </section>

          <section className="dimension-research-section">
            <div className="dimension-research-section-heading">
              <div>
                <h3>规则覆盖与分数分布</h3>
              </div>
            </div>
            <div className="dimension-research-table-wrap dimension-research-profile-table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>角色</th>
                    <th>规则</th>
                    <th>触发数</th>
                    <th>覆盖率</th>
                    <th>补零均值</th>
                    <th>补零标准差</th>
                  </tr>
                </thead>
                <tbody>
                  {result.strategies.map((strategy) => (
                    <tr key={strategy.rule_name}>
                      <td>
                        {coreRules.includes(strategy.rule_name)
                          ? "核心"
                          : "比较"}
                      </td>
                      <th>{strategy.rule_name}</th>
                      <td>{strategy.trigger_count.toLocaleString()}</td>
                      <td>{formatPercent(strategy.coverage)}</td>
                      <td>{formatNumber(strategy.score_mean_with_zeros, 4)}</td>
                      <td>{formatNumber(strategy.score_std_with_zeros, 4)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <section className="dimension-research-section">
            <div className="dimension-research-section-heading">
              <div>
                <h3>两两相关明细</h3>
              </div>
            </div>
            <div className="dimension-research-table-wrap dimension-research-pair-table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>规则对</th>
                    <th>共同触发</th>
                    <th>并集</th>
                    <th>Jaccard</th>
                    <th>Phi</th>
                    <th>Pearson</th>
                    <th>日度 Spearman</th>
                    <th>日度距离相关</th>
                    <th>持仓收益相关</th>
                    <th>收益共同日期</th>
                    <th>八维风格距离</th>
                    <th>共同风格维度</th>
                    <th>解读</th>
                  </tr>
                </thead>
                <tbody>
                  {result.pair_metrics.map((pair) => (
                    <tr key={`${pair.left_rule_name}-${pair.right_rule_name}`}>
                      <th>
                        {pair.left_rule_name}
                        <small>{pair.right_rule_name}</small>
                      </th>
                      <td>{pair.joint_trigger_count.toLocaleString()}</td>
                      <td>{pair.union_trigger_count.toLocaleString()}</td>
                      <td>{formatNumber(pair.jaccard)}</td>
                      <td>{formatNumber(pair.phi)}</td>
                      <td>{formatNumber(pair.score_pearson_with_zeros)}</td>
                      <td>{formatNumber(pair.score_spearman_daily_mean)}</td>
                      <td>
                        {formatNumber(pair.distance_correlation_daily_mean)}
                      </td>
                      <td>{formatNumber(pair.return_pearson)}</td>
                      <td>{pair.return_shared_day_count.toLocaleString()}</td>
                      <td>{formatNumber(pair.style_distance)}</td>
                      <td>{pair.style_shared_dimension_count}</td>
                      <td>
                        <span
                          className={`dimension-research-strength strength-${dependenceLabel(pair.distance_correlation_daily_mean)}`}
                        >
                          {dependenceLabel(
                            pair.distance_correlation_daily_mean,
                          )}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <section className="dimension-research-section">
            <div className="dimension-research-section-heading">
              <div>
                <h3>八维策略风格暴露</h3>
                <p className="dimension-research-section-note">
                  数值统一在 -1 到 1：正方向依次表示偏近期上涨、接近 20
                  日突破、连续触发、 60 日高位、波动放大、高流动性、随指数（
                  {result.return_index_ts_code}
                  ）上涨日增加触发、正残差占优；负值表示相反风格。
                  量价维度使用每日横截面百分位，避免价格和成交额量纲直接混合。
                </p>
              </div>
            </div>
            {styleAnalysis && (
              <div className="dimension-research-style-analysis">
                <article className="dimension-research-style-verdict">
                  <span>组合结论</span>
                  <strong>
                    {styleAnalysis.dominant.label}是当前主导维度
                  </strong>
                  <p>
                    {styleAnalysis.dominant.averageAbs >= 0.6
                      ? `平均绝对暴露 ${formatNumber(styleAnalysis.dominant.averageAbs)}，策略风格明显集中，不属于八维均衡组合。`
                      : "各维度暴露较接近，暂未发现单一维度明显主导。"}
                  </p>
                </article>
                <article>
                  <span>主导维度</span>
                  <strong>{styleAnalysis.dominant.label}</strong>
                  <p>
                    {styleAnalysis.dominant.activeCount} / {result.style_exposures.length} 条规则达到 ±0.30 风格暴露线。
                  </p>
                </article>
                <article>
                  <span>近似中性维度</span>
                  <strong>{styleAnalysis.neutralCount} / 8</strong>
                  <p>平均绝对暴露低于 0.30 的维度，表示当前规则没有明显偏向。</p>
                </article>
                <div className="dimension-research-style-bars">
                  {styleAnalysis.dimensions.map((dimension) => (
                    <div key={dimension.key}>
                      <div>
                        <span>{dimension.label}</span>
                        <b>{formatNumber(dimension.averageAbs)}</b>
                      </div>
                      <i>
                        <em style={{ width: `${Math.min(100, dimension.averageAbs * 100)}%` }} />
                      </i>
                    </div>
                  ))}
                </div>
              </div>
            )}
            <div className="dimension-research-table-wrap dimension-research-pair-table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>规则</th>
                    <th>有效触发样本</th>
                    <th>主导风格</th>
                    {(result.style_exposures[0]?.dimensions ?? []).map(
                      (dimension) => (
                        <th key={dimension.key}>{dimension.label}</th>
                      ),
                    )}
                  </tr>
                </thead>
                <tbody>
                  {result.style_exposures.map((exposure) => (
                    <tr key={exposure.rule_name}>
                      <th>{exposure.rule_name}</th>
                      <td>{exposure.sample_count.toLocaleString()}</td>
                      <td>
                        {[...exposure.dimensions]
                          .filter(
                            (dimension) =>
                              dimension.value != null &&
                              Number.isFinite(dimension.value),
                          )
                          .sort(
                            (left, right) =>
                              Math.abs(right.value ?? 0) -
                              Math.abs(left.value ?? 0),
                          )[0]?.label ?? "样本不足"}
                      </td>
                      {exposure.dimensions.map((dimension) => (
                        <td key={dimension.key}>
                          {formatNumber(dimension.value)}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <section className="dimension-research-section">
            <div className="dimension-research-section-heading">
              <div>
                <h3>可交易持仓收益路径</h3>
                <p className="dimension-research-section-note">
                  评分日确认后从下一交易日开盘进入，持有 {result.holding_period}{" "}
                  个交易日； 股票收益扣除 {result.return_index_beta}×指数（
                  {result.return_index_ts_code}）、
                  {result.return_concept_beta}×概念和{" "}
                  {result.return_industry_beta}×行业收益。 HAC 滞后阶数为{" "}
                  {Math.max(0, result.holding_period - 1)}。
                </p>
              </div>
            </div>
            <div className="dimension-research-table-wrap dimension-research-profile-table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>规则</th>
                    <th>有效日期</th>
                    <th>日均残差收益</th>
                    <th>HAC t值</th>
                    <th>训练期均值</th>
                    <th>检验期均值</th>
                  </tr>
                </thead>
                <tbody>
                  {result.return_summaries.map((summary) => (
                    <tr key={summary.rule_name}>
                      <th>{summary.rule_name}</th>
                      <td>{summary.valid_day_count.toLocaleString()}</td>
                      <td>{formatPctPoint(summary.avg_residual_return)}</td>
                      <td>{formatNumber(summary.hac_t_value, 2)}</td>
                      <td>
                        {formatPctPoint(summary.train_avg_residual_return)}
                      </td>
                      <td>
                        {formatPctPoint(summary.test_avg_residual_return)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <section className="dimension-research-section">
            <div className="dimension-research-section-heading">
              <div>
                <h3>相对核心组的样本外收益增量</h3>
                <p className="dimension-research-section-note">
                  前 {formatPercent(result.oos_train_ratio)}{" "}
                  日期只用于拟合收益关系；核心策略组优先作为已有基准，非核心规则按稳定顺序加入；
                  {result.oos_test_start_date
                    ? `从 ${result.oos_test_start_date} 起只做检验。`
                    : "当前有效日期不足以切分训练期和检验期。"}
                </p>
              </div>
            </div>
            <div className="dimension-research-table-wrap dimension-research-profile-table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>规则</th>
                    <th>训练 / 检验样本</th>
                    <th>检验期未解释收益</th>
                    <th>增量 HAC t值</th>
                    <th>增量为正比例</th>
                    <th>训练期已有基准系数</th>
                  </tr>
                </thead>
                <tbody>
                  {result.return_increments.map((increment, index) => (
                    <tr key={increment.rule_name}>
                      <th>{increment.rule_name}</th>
                      <td>
                        {increment.train_sample_count} /{" "}
                        {increment.test_sample_count}
                      </td>
                      <td>{formatPctPoint(increment.test_incremental_mean)}</td>
                      <td>
                        {formatNumber(
                          increment.test_incremental_hac_t_value,
                          2,
                        )}
                      </td>
                      <td>
                        {formatPercent(
                          increment.test_incremental_positive_ratio,
                        )}
                      </td>
                      <td>
                        {index === 0
                          ? "首个核心基准"
                          : increment.basis_coefficients.length > 0
                            ? increment.basis_coefficients
                                .map(
                                  (coefficient) =>
                                    `${coefficient.rule_name} ${formatNumber(coefficient.coefficient)}`,
                                )
                                .join("；")
                            : "训练样本不足或收益无方差"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <section className="dimension-research-section">
            <div className="dimension-research-section-heading">
              <div>
                <h3>相对核心组剥离重复信息</h3>
                <p className="dimension-research-section-note">
                  核心策略组排在内部序列前面，其余规则按稳定顺序逐步剥离已有解释力。
                </p>
              </div>
            </div>
            <div className="dimension-research-orthogonal-results">
              {result.orthogonal_diagnostics.map((diagnostic) => (
                <article key={diagnostic.rule_name}>
                  <div>
                    <span>
                      {coreRules.includes(diagnostic.rule_name) ? "核" : "比"}
                    </span>
                    <strong>{diagnostic.rule_name}</strong>
                    <em>{residualLabel(diagnostic.residual_variance_ratio)}</em>
                  </div>
                  <div className="dimension-research-residual-bar">
                    <i
                      style={{
                        width: `${Math.max(0, Math.min(100, (diagnostic.residual_variance_ratio ?? 0) * 100))}%`,
                      }}
                    />
                  </div>
                  <dl>
                    <div>
                      <dt>残差方差</dt>
                      <dd>
                        {formatPercent(diagnostic.residual_variance_ratio)}
                      </dd>
                    </div>
                    <div>
                      <dt>已解释方差</dt>
                      <dd>
                        {formatPercent(diagnostic.explained_variance_ratio)}
                      </dd>
                    </div>
                  </dl>
                  <p>
                    {diagnostic.basis_coefficients.length === 0 ? (
                      "首个核心策略作为比较基准，残差为自身。"
                    ) : (
                      <>
                        已有基准系数：
                        {diagnostic.basis_coefficients
                          .map(
                            (coefficient) =>
                              `${coefficient.rule_name} ${formatNumber(coefficient.coefficient)}`,
                          )
                          .join("；")}
                      </>
                    )}
                  </p>
                </article>
              ))}
            </div>
          </section>

          <section className="dimension-research-section">
            <div className="dimension-research-section-heading">
              <div>
                <h3>股票日 Pearson 矩阵</h3>
              </div>
            </div>
            <div className="dimension-research-table-wrap dimension-research-matrix-wrap">
              <table>
                <thead>
                  <tr>
                    <th>规则</th>
                    {result.strategies.map((strategy, index) => (
                      <th title={strategy.rule_name} key={strategy.rule_name}>
                        {index + 1}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {result.strategies.map((row, rowIndex) => (
                    <tr key={row.rule_name}>
                      <th>
                        {rowIndex + 1}. {row.rule_name}
                      </th>
                      {result.strategies.map((column, columnIndex) => {
                        const value =
                          rowIndex === columnIndex
                            ? 1
                            : pairByRules.get(
                                `${row.rule_name}\u0000${column.rule_name}`,
                              )?.score_pearson_with_zeros;
                        return (
                          <td
                            key={column.rule_name}
                            style={{
                              backgroundColor:
                                value == null
                                  ? undefined
                                  : `rgba(${value < 0 ? "59, 130, 246" : "249, 115, 22"}, ${0.08 + Math.abs(value) * 0.38})`,
                            }}
                          >
                            {formatNumber(value, 2)}
                          </td>
                        );
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          {result.pending_layers.length > 0 ? (
            <section className="dimension-research-pending">
              <strong>尚不能从本次结果回答</strong>
              <ul>
                {result.pending_layers.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
            </section>
          ) : null}
        </>
      ) : null}
    </main>
  );
}
