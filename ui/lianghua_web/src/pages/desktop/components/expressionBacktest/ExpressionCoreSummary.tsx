import { useMemo } from "react";
import type { RuleValidationComboResult } from "../../../../apis/strategyTrigger";
import { readStoredBacktestHighlightSettings } from "../../../../shared/backtestHighlightSettings";
import { formatNumber, formatBacktestValue } from "../../../../shared/backtestFormat";
import {
  directionAdjustedResidual,
  formatValidationInsufficientHint,
  formatValidationWindowRatio,
  metricHighlightClass,
  type ValidationDirection,
} from "../../../../shared/expressionValidation";

export function ExpressionCoreSummary({
  combo,
  direction,
}: {
  combo: RuleValidationComboResult;
  direction: ValidationDirection;
}) {
  const highlightSettings = useMemo(() => readStoredBacktestHighlightSettings(), []);
  const walkForward = combo.walk_forward ?? null;
  const incremental = combo.incremental ?? null;
  const directionResidual = directionAdjustedResidual(combo.backtest.avg_excess_residual_mean, direction);
  const decay20 = (combo.backtest.decay_validations ?? []).find((item) => item.window_days === 20);

  return (
    <div className="expression-backtest-section">
      <h3>
        核心结果
        <span className="expression-backtest-section-note">· {combo.combo_label}</span>
      </h3>
      <div className="research-feedback-grid">
        <div><span>方向回测指标</span><strong>{formatBacktestValue(directionResidual, combo.backtest.return_metric)}</strong><small>超额残差按{direction === "negative" ? "负向" : "正向"}换算</small></div>
        <div><span>有后验的触发股票日</span><strong>{combo.trigger_samples.toLocaleString()}</strong><small>{combo.triggered_days} 个有效触发日</small></div>
        <div><span>OOS 残差正窗口</span><strong>{walkForward?.folds.length ? formatValidationWindowRatio(combo, "residual") : "—"}</strong></div>
        <div><span>最近衰减（20日）</span><strong>{formatBacktestValue(decay20?.decay_change, combo.backtest.return_metric)}</strong><small>近期减此前</small></div>
      </div>
      <details><summary>查看排序指标与完整统计</summary>
      <div className="expression-backtest-summary-grid">
        <div className="expression-backtest-summary-item">
          <span>有后验的触发股票日</span>
          <strong>
            {combo.trigger_samples} / {combo.triggered_days} 日
          </strong>
        </div>
        <div className="expression-backtest-summary-item">
          <span>原始相对全市场差</span>
          <strong>{formatBacktestValue(combo.backtest.avg_excess_residual_mean, combo.backtest.return_metric)}</strong>
        </div>
        <div className="expression-backtest-summary-item">
          <span>IC</span>
          <strong className={metricHighlightClass(highlightSettings, "ic", combo.backtest.ic_mean)}>
            {formatNumber(combo.backtest.ic_mean)}
          </strong>
        </div>
        <div className="expression-backtest-summary-item">
          <span>IC t</span>
          <strong className={metricHighlightClass(highlightSettings, "t", combo.backtest.ic_t_value)}>
            {formatNumber(combo.backtest.ic_t_value)}
          </strong>
        </div>
        <div className="expression-backtest-summary-item">
          <span>ICIR</span>
          <strong className={metricHighlightClass(highlightSettings, "ir", combo.backtest.icir)}>
            {formatNumber(combo.backtest.icir)}
          </strong>
        </div>
        <div
          className="expression-backtest-summary-item"
          title={`原始回测指标（方向盲）：${formatBacktestValue(combo.backtest.avg_excess_residual_mean, combo.backtest.return_metric)}`}
        >
          <span>方向回测指标</span>
          <strong
            className={metricHighlightClass(highlightSettings, "residual", directionResidual)}
          >
            <span className="expression-backtest-residual">
              <span>{formatBacktestValue(directionResidual, combo.backtest.return_metric)}</span>
              <span
                className={`expression-backtest-residual-badge expression-backtest-residual-badge-${direction}`}
              >
                {direction === "negative" ? "扣" : "加"}
              </span>
            </span>
          </strong>
        </div>
        <div className="expression-backtest-summary-item">
          <span>Top-Bottom Spread</span>
          <strong>{formatBacktestValue(combo.backtest.spread_mean, combo.backtest.return_metric)}</strong>
        </div>
        <div
          className="expression-backtest-summary-item"
          title={formatValidationInsufficientHint(combo, "walk_forward")}
        >
          <span>OOS IC 正窗口</span>
          <strong>{formatValidationWindowRatio(combo, "ic")}</strong>
        </div>
        <div
          className="expression-backtest-summary-item"
          title={formatValidationInsufficientHint(combo, "walk_forward")}
        >
          <span>OOS 残差正窗口</span>
          <strong>{formatValidationWindowRatio(combo, "residual")}</strong>
        </div>
        <div
          className="expression-backtest-summary-item"
          title={formatValidationInsufficientHint(combo, "incremental")}
        >
          <span>控制后收益正窗口</span>
          <strong>
            {incremental && incremental.core_rule_names.length > 0
              ? formatValidationWindowRatio(combo, "incremental")
              : "未选核心策略"}
          </strong>
        </div>
        <div className="expression-backtest-summary-item">
          <span>最近衰减（20日）</span>
          <strong>{formatBacktestValue(decay20?.decay_change, combo.backtest.return_metric)}</strong>
        </div>
      </div>
      <p className="expression-backtest-caption">
        这里只展示事实指标，不产出综合得分、评级或推荐。IC 与 Spread 保持原始值；
        回测指标是方向盲的均值，主摘要同时展示原始值与按表达式方向换算后的读数；触发股票日仅包含已取得有效后验的样本，
        与 walk-forward 的残差口径保持一致。
        {walkForward && walkForward.folds.length > 0
          ? ` walk-forward 共 ${walkForward.folds.length} 个窗口，样本不足的窗口计入分母但不计为正窗口。`
          : ""}
      </p>
      </details>
    </div>
  );
}
