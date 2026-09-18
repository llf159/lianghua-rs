import { useMemo } from "react";
import type { RuleValidationComboResult } from "../../../../apis/strategyTrigger";
import { readStoredBacktestHighlightSettings } from "../../../../shared/backtestHighlightSettings";
import { formatDateLabel, formatNumber, formatPercent } from "../../../../shared/backtestFormat";
import { metricHighlightClass } from "../../../../shared/expressionValidation";

export function ExpressionWalkForward({ combo }: { combo: RuleValidationComboResult }) {
  const highlightSettings = useMemo(() => readStoredBacktestHighlightSettings(), []);
  const walkForward = combo.walk_forward ?? null;
  if (!walkForward || walkForward.folds.length === 0) {
    return (
      <div className="expression-backtest-section">
        <h3>Walk-forward 稳定性</h3>
        <div className="expression-backtest-empty">当前组合没有可用的样本外窗口。</div>
      </div>
    );
  }

  const foldCount = walkForward.folds.length;
  const insufficientFolds = walkForward.folds.filter((fold) => fold.status === "insufficient").length;

  return (
    <div className="expression-backtest-section">
      <h3>Walk-forward 稳定性</h3>
      <div className="expression-backtest-table-wrap">
        <table className="expression-backtest-table">
          <thead>
            <tr>
              <th>Fold</th>
              <th>Train</th>
              <th>Test</th>
              <th>IC</th>
              <th>IC t</th>
              <th>残差收益（方向）</th>
              <th>Spread</th>
              <th>样本</th>
            </tr>
          </thead>
          <tbody>
            {walkForward.folds.map((fold) => (
              <tr key={`${combo.combo_key}-fold-${fold.fold_index}`}>
                <td>
                  {fold.fold_index + 1}
                  {fold.status === "insufficient" ? (
                    <span className="expression-backtest-insufficient">样本不足</span>
                  ) : null}
                </td>
                <td>
                  {formatDateLabel(fold.train_start_date)} ~ {formatDateLabel(fold.train_end_date)}
                </td>
                <td>
                  {formatDateLabel(fold.test_start_date)} ~ {formatDateLabel(fold.test_end_date)}
                </td>
                <td className={metricHighlightClass(highlightSettings, "ic", fold.ic_mean)}>
                  {formatNumber(fold.ic_mean)}
                </td>
                <td className={metricHighlightClass(highlightSettings, "t", fold.ic_t_value)}>
                  {formatNumber(fold.ic_t_value, 2)}
                </td>
                <td>{formatPercent(fold.avg_residual_return)}</td>
                <td>{formatPercent(fold.spread_mean)}</td>
                <td>
                  {fold.test_sample_count} / {fold.test_day_count} 日
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="expression-backtest-summary-grid">
        <div className="expression-backtest-summary-item">
          <span>IC 正窗口</span>
          <strong>
            {walkForward.ic_positive_folds} / {foldCount}
          </strong>
        </div>
        <div className="expression-backtest-summary-item">
          <span>残差正窗口</span>
          <strong>
            {walkForward.residual_positive_folds} / {foldCount}
          </strong>
        </div>
        <div className="expression-backtest-summary-item">
          <span>Spread 正窗口</span>
          <strong>
            {walkForward.spread_positive_folds} / {foldCount}
          </strong>
        </div>
        <div className="expression-backtest-summary-item">
          <span>样本不足</span>
          <strong>
            {insufficientFolds} / {foldCount}
          </strong>
        </div>
      </div>
      <p className="expression-backtest-caption">
        训练区间末与样本外起点之间固定排除 holding period 个交易日（HAC lag 仍按 holding period - 1）；
        IC 与 Spread 由分数符号/排序天然携带方向，负向规则不再额外翻转；残差收益是方向盲的均值，
        已按表达式方向调整符号。样本不足的 fold 计入分母但不计为正窗口
        {insufficientFolds > 0 ? `（本次 ${insufficientFolds} / ${foldCount}）` : ""}。
      </p>
    </div>
  );
}
