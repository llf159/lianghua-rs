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

const STYLE_DIMENSIONS = [
  ["方向反应", "趋势延续 ↔ 均值回归", "前序收益方向、反转窗口、趋势持续率"],
  ["入场形态", "突破追随 ↔ 回撤 / 反转", "突破幅度、回撤深度、形态确认耗时"],
  ["时间尺度", "短周期 ↔ 中长周期", "信号半衰期、持有天数、换手率"],
  [
    "价格位置",
    "低位 / 区间内 ↔ 高位 / 区间外",
    "区间分位、距均线幅度、累计涨跌幅",
  ],
  ["波动与跳跃", "平稳低波 ↔ 高波动 / 跳跃", "实现波动率、ATR、缺口、极端收益"],
  ["量能与流动性", "缩量低关注 ↔ 放量高参与", "量比、换手率、成交额、冲击成本"],
  [
    "市场状态依赖",
    "跨状态稳健 ↔ 依赖特定状态",
    "分状态触发率、收益、IC 与离散度",
  ],
  ["收益形态", "高胜率小盈亏 ↔ 低胜率右尾", "胜率、盈亏比、偏度、回撤与修复"],
] as const;

const formatNumber = (value: number | null | undefined, digits = 3) =>
  value == null || !Number.isFinite(value) ? "—" : value.toFixed(digits);

const formatPercent = (value: number | null | undefined) =>
  value == null || !Number.isFinite(value)
    ? "—"
    : `${(value * 100).toFixed(2)}%`;

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
  const [ruleFilter, setRuleFilter] = useState("");
  const [nonlinearSampleLimit, setNonlinearSampleLimit] = useState("512");
  const [ridgeLambda, setRidgeLambda] = useState("0.000001");
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
      setSelectedRules(
        rulesByActivity
          .slice(0, Math.min(10, loadedDefaults.max_strategy_count))
          .map((rule) => rule.rule_name),
      );
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
        .filter((pair) => pair.distance_correlation_daily_mean != null)
        .sort(
          (left, right) =>
            (right.distance_correlation_daily_mean ?? -1) -
            (left.distance_correlation_daily_mean ?? -1),
        )[0],
    [result],
  );

  const strongestOverlapPair = useMemo(
    () =>
      [...(result?.pair_metrics ?? [])]
        .filter((pair) => pair.jaccard != null)
        .sort((left, right) => (right.jaccard ?? -1) - (left.jaccard ?? -1))[0],
    [result],
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

  const toggleRule = (ruleName: string) => {
    setResult(null);
    setSelectedRules((current) => {
      if (current.includes(ruleName))
        return current.filter((name) => name !== ruleName);
      if (current.length >= (defaults?.max_strategy_count ?? 20))
        return current;
      return [...current, ruleName];
    });
  };

  const moveRule = (index: number, direction: -1 | 1) => {
    setResult(null);
    setSelectedRules((current) => {
      const target = index + direction;
      if (target < 0 || target >= current.length) return current;
      const next = [...current];
      [next[index], next[target]] = [next[target], next[index]];
      return next;
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
    const sampleLimit = Number(nonlinearSampleLimit);
    const lambda = Number(ridgeLambda);
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
    setRunning(true);
    setError("");
    try {
      setResult(
        await runStrategyDimensionResearch({
          sourcePath,
          startDate: inputToCompactDate(startDate),
          endDate: inputToCompactDate(endDate),
          ruleNames: selectedRules,
          nonlinearSampleLimit: sampleLimit,
          ridgeLambda: lambda,
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
          <span className="dimension-research-status">
            {result
              ? `实证结果 · ${result.start_date}—${result.end_date}`
              : "信号层实证研究"}
          </span>
          <p className="dimension-research-eyebrow">
            STRATEGY DIMENSION RESEARCH
          </p>
          <h2>相关性与正交研究</h2>
          <p className="dimension-research-lead">
            从结果库读取真实规则触发和分数，在完整股票日宇宙中比较覆盖、重叠、线性相关与一般依赖，
            再按所选顺序检查每条规则还有多少不能被前序规则线性解释的方差。
          </p>
        </div>
        <aside
          className="dimension-research-principle"
          aria-label="研究结论边界"
        >
          <span>当前能力边界</span>
          <strong>低线性相关，不等于策略独立</strong>
          <p>
            当前结果确认信号层结构；风格、收益路径和样本外组合增量尚未接入，不能据此直接决定资金配置。
          </p>
        </aside>
      </section>

      <section
        className="dimension-research-workbench"
        aria-labelledby="dimension-workbench-title"
      >
        <div className="dimension-research-section-heading">
          <div>
            <p className="dimension-research-eyebrow">RESEARCH WORKBENCH</p>
            <h3 id="dimension-workbench-title">选择规则并运行研究</h3>
          </div>
          <div className="dimension-research-load-control">
            <p>
              规则顺序会影响线性正交结果；后面的规则只用排在它前面的规则进行解释。
            </p>
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
              <small>
                距离相关为 O(n²)，默认确定性抽取{" "}
                {defaults?.default_nonlinear_sample_limit ?? 512} 日。
              </small>
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
              <small>用于稳定高度共线规则的回归求解，不是策略权重。</small>
            </label>
          </div>

          <div className="dimension-research-selection">
            <div className="dimension-research-selection-head">
              <div>
                <strong>正交解释顺序</strong>
                <small>
                  已选 {selectedRules.length}/
                  {defaults?.max_strategy_count ?? 20}
                </small>
              </div>
              <button
                type="button"
                onClick={() => {
                  setSelectedRules([]);
                  setResult(null);
                }}
                disabled={running || selectedRules.length === 0}
              >
                清空
              </button>
            </div>
            <ol>
              {selectedRules.map((ruleName, index) => (
                <li key={ruleName}>
                  <span>{index + 1}</span>
                  <strong title={ruleName}>{ruleName}</strong>
                  <button
                    type="button"
                    aria-label={`上移 ${ruleName}`}
                    onClick={() => moveRule(index, -1)}
                    disabled={running || index === 0}
                  >
                    ↑
                  </button>
                  <button
                    type="button"
                    aria-label={`下移 ${ruleName}`}
                    onClick={() => moveRule(index, 1)}
                    disabled={running || index === selectedRules.length - 1}
                  >
                    ↓
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
                setSelectedRules(
                  filteredRuleOptions
                    .slice(0, defaults?.max_strategy_count ?? 20)
                    .map((rule) => rule.rule_name),
                );
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
          <small>
            {initializing
              ? "正在读取规则统计…"
              : defaults
                ? `结果库：${sourcePath}`
                : "进入页面不会扫描结果库；请按需加载日期与规则。"}
          </small>
          <button
            className="dimension-research-run-button"
            type="button"
            onClick={() => void runResearch()}
            disabled={initializing || running || !defaults}
          >
            {running ? "正在计算相关性…" : "运行相关性与正交研究"}
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
              <small>按当前正交解释顺序</small>
            </article>
            <article>
              <span>规则组合</span>
              <strong>{result.pair_metrics.length}</strong>
              <small>每一对均计算重叠和相关</small>
            </article>
            <article>
              <span>非线性样本</span>
              <strong>{result.nonlinear_sample_limit}</strong>
              <small>
                实际共同交易日{" "}
                {result.pair_metrics[0]?.nonlinear_sample_count ?? 0}
              </small>
            </article>
          </section>

          <section className="dimension-research-section">
            <div className="dimension-research-section-heading">
              <div>
                <p className="dimension-research-eyebrow">
                  DATA-GROUNDED READING
                </p>
                <h3>先读这组数据告诉我们的内容</h3>
              </div>
              <p>
                下面是描述性诊断，不包含显著性检验，也不构成新增策略维度的最终证明。
              </p>
            </div>
            <div className="dimension-research-insight-grid">
              <article>
                <span>触发重叠最高</span>
                <strong>
                  {strongestOverlapPair
                    ? `${strongestOverlapPair.left_rule_name} / ${strongestOverlapPair.right_rule_name}`
                    : "样本不足"}
                </strong>
                <p>
                  {strongestOverlapPair
                    ? `Jaccard 为 ${formatNumber(strongestOverlapPair.jaccard)}，两规则触发集合的交集约占并集的 ${formatPercent(strongestOverlapPair.jaccard)}。它描述共同选中，不描述分数方向。`
                    : "没有可计算的共同触发数据。"}
                </p>
              </article>
              <article>
                <span>一般依赖最强</span>
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
                <span>最易被前序规则解释</span>
                <strong>{mostExplainedRule?.rule_name ?? "暂无"}</strong>
                <p>
                  {mostExplainedRule
                    ? `线性残差比例为 ${formatPercent(mostExplainedRule.residual_variance_ratio)}，属于“${residualLabel(mostExplainedRule.residual_variance_ratio)}”。该结论依赖当前规则顺序与岭系数。`
                    : "首条规则没有前序基准；增加规则后才能形成解释关系。"}
                </p>
              </article>
            </div>
          </section>

          <section className="dimension-research-section">
            <div className="dimension-research-section-heading">
              <div>
                <p className="dimension-research-eyebrow">SIGNAL PROFILE</p>
                <h3>规则覆盖与分数分布</h3>
              </div>
              <p>均值和标准差均在完整股票日宇宙上计算，未触发记为 0。</p>
            </div>
            <div className="dimension-research-table-wrap dimension-research-profile-table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>顺序</th>
                    <th>规则</th>
                    <th>触发数</th>
                    <th>覆盖率</th>
                    <th>补零均值</th>
                    <th>补零标准差</th>
                  </tr>
                </thead>
                <tbody>
                  {result.strategies.map((strategy, index) => (
                    <tr key={strategy.rule_name}>
                      <td>{index + 1}</td>
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
                <p className="dimension-research-eyebrow">
                  PAIRWISE DEPENDENCE
                </p>
                <h3>两两相关明细</h3>
              </div>
              <p>
                Pearson 看股票日线性联动；Spearman
                与距离相关看日度横截面平均强度。
              </p>
            </div>
            <div className="dimension-research-table-wrap">
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
                <p className="dimension-research-eyebrow">
                  ORTHOGONAL RESIDUAL
                </p>
                <h3>按顺序剥离已有规则</h3>
              </div>
              <p>
                残差比例越低，当前规则越容易被前序规则线性解释；它不是收益增量。
              </p>
            </div>
            <div className="dimension-research-orthogonal-results">
              {result.orthogonal_diagnostics.map((diagnostic, index) => (
                <article key={diagnostic.rule_name}>
                  <div>
                    <span>{index + 1}</span>
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
                      "首条规则作为比较基准，残差为自身。"
                    ) : (
                      <>
                        前序系数：
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
                <p className="dimension-research-eyebrow">CORRELATION MATRIX</p>
                <h3>股票日 Pearson 矩阵</h3>
              </div>
              <p>颜色深浅表示绝对线性相关强度，正负号表示同向或反向。</p>
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

      <section className="dimension-research-details" aria-label="研究方法说明">
        <details open={!result}>
          <summary>
            <span>
              <strong>怎样结合上面的具体数据读相关性</strong>
              <small>Jaccard、Pearson、Spearman 和距离相关回答不同问题</small>
            </span>
            <i aria-hidden="true" />
          </summary>
          <div className="dimension-research-detail-body dimension-research-method-grid">
            <article>
              <h4>共同选中：Jaccard / Phi</h4>
              <p>
                Jaccard 只看至少一方触发的集合；Phi
                把共同不触发也纳入二元关联。覆盖率悬殊时，两者出现差异是正常现象。
              </p>
            </article>
            <article>
              <h4>线性同向：Pearson</h4>
              <p>
                本页在全部评分股票日上补零计算。数值接近 0
                只表示直线关系弱，不能推出两个策略独立。
              </p>
            </article>
            <article>
              <h4>日度单调：Spearman</h4>
              <p>
                先把每条规则聚合为当日总分除以当日股票数，再比较日期排序，回答策略是否随市场环境共同增强或减弱。
              </p>
            </article>
            <article>
              <h4>一般依赖：距离相关</h4>
              <p>
                能够捕捉 U
                形、阈值和分群关系，但不提供方向，也不是显著性概率。受 O(n²)
                成本限制，本页使用确定性日期抽样。
              </p>
            </article>
          </div>
        </details>
        <details>
          <summary>
            <span>
              <strong>怎样理解线性正交残差</strong>
              <small>顺序、正则化和结论边界</small>
            </span>
            <i aria-hidden="true" />
          </summary>
          <div className="dimension-research-detail-body">
            <ol className="dimension-research-orthogonal-steps">
              <li>
                <span>01</span>
                <div>
                  <strong>顺序就是研究假设</strong>
                  <p>
                    第 N 条规则只由前 N−1
                    条解释。改变顺序后残差会改变，所以应把成熟基准策略放在前面、候选策略放在后面。
                  </p>
                </div>
              </li>
              <li>
                <span>02</span>
                <div>
                  <strong>残差不是可交易组合</strong>
                  <p>
                    残差比例表示分数方差中未被线性解释的部分，没有计入换手、成本、容量或收益。
                  </p>
                </div>
              </li>
              <li>
                <span>03</span>
                <div>
                  <strong>还要检查一般依赖</strong>
                  <p>
                    线性残差高仍可能存在强距离相关；只有后续收益路径与样本外组合增量也成立，才可称为独立策略维度。
                  </p>
                </div>
              </li>
            </ol>
          </div>
        </details>
        <details>
          <summary>
            <span>
              <strong>八维风格画像：下一阶段要解释什么</strong>
              <small>目前仅展示定义，不生成没有数据支持的分数</small>
            </span>
            <i aria-hidden="true" />
          </summary>
          <div className="dimension-research-detail-body dimension-research-style-grid">
            {STYLE_DIMENSIONS.map(([title, poles, observable], index) => (
              <article className="dimension-research-style-card" key={title}>
                <div className="dimension-research-style-card-head">
                  <span>{String(index + 1).padStart(2, "0")}</span>
                  <h4>{title}</h4>
                </div>
                <strong className="dimension-research-poles">{poles}</strong>
                <p>待结果库物化：{observable}</p>
              </article>
            ))}
          </div>
        </details>
      </section>
    </main>
  );
}
