import type { RuleValidationComboResult } from "../../../../apis/strategyTrigger";
import { formatBacktestValue } from "../../../../shared/backtestFormat";

export function ExpressionLayerResult({ combo }: { combo: RuleValidationComboResult }) {
  const layerSummaries = combo.backtest.layer_summaries ?? [];
  const maxAbsLayerReturn = Math.max(
    ...layerSummaries.map((row) => Math.abs(row.avg_residual_return ?? 0)),
    0,
  );

  return (
    <div className="expression-backtest-section">
      <h3>
        收益分层
        {combo.backtest.layer_method_label
          ? `（${combo.backtest.layer_method_label}，共 ${combo.backtest.layer_count ?? layerSummaries.length} 层）`
          : ""}
      </h3>
      {layerSummaries.length === 0 ? (
        <div className="expression-backtest-empty">当前组合没有可用于分层的触发样本。</div>
      ) : (
        <>
          <div className="expression-backtest-layer-list">
            {layerSummaries.map((item) => {
              const value = item.avg_residual_return;
              const barWidth =
                maxAbsLayerReturn > 0 && value !== null && value !== undefined && Number.isFinite(value)
                  ? Math.max(2, (Math.abs(value) / maxAbsLayerReturn) * 50)
                  : 0;
              const tone =
                value === null || value === undefined || !Number.isFinite(value) || value === 0
                  ? "neutral"
                  : value > 0
                    ? "positive"
                    : "negative";
              return (
                <div key={`${combo.combo_key}-layer-${item.layer_index}`} className="expression-backtest-layer-row">
                  <span className="expression-backtest-layer-label">{item.layer_label}</span>
                  <div className="expression-backtest-layer-track">
                    {barWidth > 0 ? (
                      <div
                        className={`expression-backtest-layer-bar expression-backtest-layer-bar-${tone}`}
                        style={{ width: `${barWidth}%` }}
                        aria-label={`${item.layer_label}: ${formatBacktestValue(value, combo.backtest.return_metric)}`}
                      />
                    ) : null}
                  </div>
                  <span className="expression-backtest-layer-value">{formatBacktestValue(value, combo.backtest.return_metric)}</span>
                </div>
              );
            })}
          </div>
          <p className="expression-backtest-caption">
            层级收益为日度原始指标均值（方向盲，与 walk-forward 的方向残差口径不同）；
            Top-Bottom Spread {formatBacktestValue(combo.backtest.spread_mean, combo.backtest.return_metric)} 按每日最高分层减最低分层计算，
            由分数排序天然携带方向。
          </p>
        </>
      )}
    </div>
  );
}
