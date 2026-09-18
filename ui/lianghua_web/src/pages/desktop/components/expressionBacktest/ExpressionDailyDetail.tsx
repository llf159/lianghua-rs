import type { RuleValidationComboResult } from "../../../../apis/strategyTrigger";
import { formatDateLabel, formatNumber, formatPercent, formatRate } from "../../../../shared/backtestFormat";

export function ExpressionDailyDetail({ combo }: { combo: RuleValidationComboResult }) {
  const dailyMetrics = combo.daily_metrics ?? [];
  const returnDistribution = combo.return_distribution ?? [];
  const maxDistributionCount = returnDistribution.reduce(
    (maxCount, bucket) => Math.max(maxCount, bucket.sample_count),
    0,
  );

  if (dailyMetrics.length === 0 && returnDistribution.length === 0) {
    return null;
  }

  return (
    <div className="expression-backtest-section">
      <h3>日度明细 / 收益分布（默认折叠）</h3>
      {dailyMetrics.length > 0 ? (
        <details className="expression-backtest-fold">
          <summary className="expression-backtest-fold-summary">
            <h4>展开日度明细（{dailyMetrics.length} 个交易日）</h4>
          </summary>
          <div className="expression-backtest-table-wrap expression-backtest-daily-scroll">
            <table className="expression-backtest-table">
              <thead>
                <tr>
                  <th>交易日</th>
                  <th>IC</th>
                  <th>残差收益（方向）</th>
                </tr>
              </thead>
              <tbody>
                {dailyMetrics.map((row) => (
                  <tr key={`${combo.combo_key}-daily-${row.trade_date}`}>
                    <td>{formatDateLabel(row.trade_date)}</td>
                    <td>{formatNumber(row.ic, 4)}</td>
                    <td>{formatPercent(row.avg_residual_return)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="expression-backtest-caption">
            日度 IC 由分数秩相关给出（负向规则工作时期望已为正），残差收益按表达式方向调整符号。
          </p>
        </details>
      ) : null}
      {returnDistribution.length > 0 ? (
        <details className="expression-backtest-fold">
          <summary className="expression-backtest-fold-summary">
            <h4>展开收益分布（{combo.trigger_samples} 个触发样本）</h4>
          </summary>
          <div className="expression-backtest-distribution-chart">
            <div className="expression-backtest-distribution-plot">
              {returnDistribution.map((bucket, index) => {
                const barHeight =
                  maxDistributionCount > 0 && bucket.sample_count > 0
                    ? Math.max(4, (bucket.sample_count / maxDistributionCount) * 100)
                    : 0;
                const tone = index < 3 ? "negative" : index === 3 ? "neutral" : "positive";
                return (
                  <div
                    key={`${combo.combo_key}-return-${bucket.bucket_label}`}
                    className="expression-backtest-distribution-bucket"
                  >
                    <span className="expression-backtest-distribution-count">{bucket.sample_count}</span>
                    <div className="expression-backtest-distribution-track">
                      <div
                        className={`expression-backtest-distribution-bar expression-backtest-distribution-bar-${tone}`}
                        style={{ height: `${barHeight}%` }}
                        aria-label={`${bucket.bucket_label}: ${bucket.sample_count} 个样本`}
                      />
                    </div>
                    <span className="expression-backtest-distribution-rate">
                      {formatRate(bucket.sample_ratio)}
                    </span>
                    <span className="expression-backtest-distribution-label">{bucket.bucket_label}</span>
                  </div>
                );
              })}
            </div>
          </div>
          <p className="expression-backtest-caption">
            收益分布为触发样本的原始残差收益分桶，不区分方向；只用于看触发样本的收益形状。
          </p>
        </details>
      ) : null}
    </div>
  );
}
