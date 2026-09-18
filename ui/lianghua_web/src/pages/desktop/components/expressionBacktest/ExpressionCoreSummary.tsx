import { useMemo } from "react";
import type { RuleValidationComboResult } from "../../../../apis/strategyTrigger";
import { readStoredBacktestHighlightSettings } from "../../../../shared/backtestHighlightSettings";
import { formatNumber, formatPercent } from "../../../../shared/backtestFormat";
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
      <div className="expression-backtest-summary-grid">
        <div className="expression-backtest-summary-item">
          <span>触发样本</span>
          <strong>
            {combo.trigger_samples} / {combo.triggered_days} 日
          </strong>
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
          title={`原始残差收益（方向盲）：${formatPercent(combo.backtest.avg_excess_residual_mean)}`}
        >
          <span>方向残差收益</span>
          <strong
            className={metricHighlightClass(highlightSettings, "residual", directionResidual)}
          >
            <span className="expression-backtest-residual">
              <span>{formatPercent(directionResidual)}</span>
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
          <strong>{formatPercent(combo.backtest.spread_mean)}</strong>
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
          <span>增量正窗口</span>
          <strong>
            {incremental && incremental.core_rule_names.length > 0
              ? formatValidationWindowRatio(combo, "incremental")
              : "未选核心策略"}
          </strong>
        </div>
        <div className="expression-backtest-summary-item">
          <span>最近衰减（20日）</span>
          <strong>{formatPercent(decay20?.decay_change)}</strong>
        </div>
      </div>
      <p className="expression-backtest-caption">
        这里只展示事实指标，不产出综合得分、评级或推荐。IC 与 Spread 保持原始值；
        残差收益是方向盲的均值，主摘要按表达式方向换算为方向残差（悬停可看原始值），
        与 walk-forward 的残差口径保持一致。
        {walkForward && walkForward.folds.length > 0
          ? ` walk-forward 共 ${walkForward.folds.length} 个窗口，样本不足的窗口计入分母但不计为正窗口。`
          : ""}
      </p>
    </div>
  );
}
