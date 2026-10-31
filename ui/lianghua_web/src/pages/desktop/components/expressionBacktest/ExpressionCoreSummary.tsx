import type { RuleValidationComboResult } from "../../../../apis/strategyTrigger";
import { formatNumber, formatBacktestValue } from "../../../../shared/backtestFormat";
import { directionAdjustedResidual, formatValidationWindowRatio, type ValidationDirection } from "../../../../shared/expressionValidation";

export function ExpressionCoreSummary({ combo, direction }: {combo: RuleValidationComboResult; direction: ValidationDirection}) {
  const decay20 = combo.backtest.decay_validations?.find((item) => item.window_days === 20);
  return <section className="expression-backtest-section">
    <h3>绩效摘要 <span className="expression-backtest-section-note">{combo.combo_label}</span></h3>
    <div className="research-feedback-grid">
      <div><span>{combo.backtest.return_metric === "rise_amount" ? "超额上涨金额" : "残差超额收益"}</span><strong>{formatBacktestValue(directionAdjustedResidual(combo.backtest.avg_excess_residual_mean, direction), combo.backtest.return_metric)}</strong><small>触发组相对全市场的平均差值，按{direction === "negative" ? "负向" : "正向"}策略调整符号。</small></div>
      <div><span>信息系数（IC）</span><strong>{formatNumber(combo.backtest.ic_mean)}</strong><small>每日信号分数与后验收益的秩相关；正值表示排序方向一致。</small></div>
      <div><span>信息系数比率（ICIR）</span><strong>{formatNumber(combo.backtest.icir)}</strong><small>日度 IC 均值除以其标准差，未年化；反映排序效果的稳定程度。</small></div>
      <div><span>样本外正收益窗口（OOS）</span><strong>{formatValidationWindowRatio(combo, "residual")}</strong><small>分段样本外检验中，方向调整后超额收益为正的窗口数／总窗口数；样本不足的窗口仍计入分母。</small></div>
      <div><span>近期效应衰减（20日）</span><strong>{formatBacktestValue(decay20?.decay_change, combo.backtest.return_metric)}</strong><small>近期指标减此前指标，负值表示近期效应减弱。</small></div>
      <div><span>有效样本量（N）</span><strong>{combo.trigger_samples.toLocaleString()}</strong><small>{combo.triggered_days} 个触发日中有有效后验的股票日；同一股票可重复计数。</small></div>
    </div>
    <p className="research-meta">收益指标对应所选持有期；“—”表示无法估计或未计算。</p>
  </section>;
}
