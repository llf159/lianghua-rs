import type { RuleValidationComboResult } from "../../../../apis/strategyTrigger";
import { formatRate } from "../../../../shared/backtestFormat";

export function ExpressionReturnDistribution({ combo }: { combo: RuleValidationComboResult }) {
  const returnDistribution = combo.return_distribution ?? [];
  const maxDistributionCount = returnDistribution.reduce(
    (maxCount, bucket) => Math.max(maxCount, bucket.sample_count),
    0,
  );

  if (returnDistribution.length === 0) {
    return null;
  }

  return (
    <div className="expression-backtest-section">
      <h3>收益分布（Return Distribution）</h3>
      {returnDistribution.length > 0 ? (
        <div>
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
            收益分布为触发样本的原始回测指标分桶，不区分方向；只用于看触发样本的收益形状。
          </p>
        </div>
      ) : null}
    </div>
  );
}
