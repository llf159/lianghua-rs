import type { RuleValidationComboResult } from "../../../../apis/strategyTrigger";
import { formatDateLabel, formatNumber, formatPercent, formatRate } from "../../../../shared/backtestFormat";

export function ExpressionIncremental({ combo }: { combo: RuleValidationComboResult }) {
  const incremental = combo.incremental ?? null;
  const coreRuleNames = incremental?.core_rule_names ?? [];

  if (coreRuleNames.length === 0) {
    return (
      <div className="expression-backtest-section">
        <h3>对核心策略组合新增价值</h3>
        <div className="expression-backtest-empty">
          尚未选择核心策略。选择核心策略并重新运行后可查看样本外增量。
        </div>
      </div>
    );
  }

  const folds = incremental?.folds ?? [];
  const insufficientFolds = folds.filter((fold) => fold.status === "insufficient").length;

  return (
    <div className="expression-backtest-section">
      <h3>对核心策略组合新增价值</h3>
      <p className="expression-backtest-caption">
        核心策略：{coreRuleNames.join(" / ")}
      </p>
      {folds.length === 0 ? (
        <div className="expression-backtest-empty">样本不足，当前没有可用的样本外增量窗口。</div>
      ) : (
        <>
          <div className="expression-backtest-table-wrap">
            <table className="expression-backtest-table">
              <thead>
                <tr>
                  <th>Fold</th>
                  <th>Incremental</th>
                  <th>HAC t</th>
                  <th>Positive</th>
                  <th>Test days</th>
                </tr>
              </thead>
              <tbody>
                {folds.map((fold) => (
                  <tr key={`${combo.combo_key}-incremental-${fold.fold_index}`}>
                    <td title={`${formatDateLabel(fold.train_start_date)} ~ ${formatDateLabel(fold.train_end_date)} → ${formatDateLabel(fold.test_start_date)} ~ ${formatDateLabel(fold.test_end_date)}`}>
                      {fold.fold_index + 1}
                      {fold.status === "insufficient" ? (
                        <span className="expression-backtest-insufficient">样本不足</span>
                      ) : null}
                    </td>
                    <td>{formatPercent(fold.incremental_mean)}</td>
                    <td>{formatNumber(fold.incremental_hac_t, 2)}</td>
                    <td>{formatRate(fold.positive_day_ratio)}</td>
                    <td>{fold.test_day_count}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="expression-backtest-summary-grid">
            <div className="expression-backtest-summary-item">
              <span>正增量窗口</span>
              <strong>
                {incremental?.positive_folds ?? 0} / {folds.length}
              </strong>
            </div>
            <div className="expression-backtest-summary-item">
              <span>样本不足</span>
              <strong>
                {insufficientFolds} / {folds.length}
              </strong>
            </div>
          </div>
        </>
      )}
      <p className="expression-backtest-caption">
        每个 fold 只用 train 做标准化 ridge 回归（train 行至少 max(30, 核心策略数 × 5)，样本外至少 20
        个共同有效日），predictors 是核心策略，候选表达式始终是 target；test 增量定义为
        y − Σβx（不减 train intercept），均值即控制核心策略后的样本外增量 alpha；样本不足的 fold
        标记为 insufficient 且不计入正增量窗口（悬停 Fold 可看区间）。
      </p>
    </div>
  );
}
