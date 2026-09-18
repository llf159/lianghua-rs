import type { RuleValidationComboResult } from "../../../../apis/strategyTrigger";
import { formatLift, formatNumber, formatRate } from "../../../../shared/backtestFormat";

export function ExpressionSimilarity({ combo }: { combo: RuleValidationComboResult }) {
  const topSimilarity = combo.similarity_rows[0] ?? null;

  return (
    <div className="expression-backtest-section">
      <h3>
        与已有策略重复性
        {topSimilarity
          ? `（最相似：${topSimilarity.rule_name} · Jaccard ${formatNumber(topSimilarity.jaccard, 3)}）`
          : ""}
      </h3>
      {combo.similarity_rows.length === 0 ? (
        <div className="expression-backtest-empty">
          暂无与当前组合同日同股同时触发的现有策略（相关性与正交研究见独立页面）。
        </div>
      ) : (
        <>
          <div className="expression-backtest-table-wrap">
            <table className="expression-backtest-table">
              <thead>
                <tr>
                  <th>策略</th>
                  <th>Jaccard</th>
                  <th>Phi</th>
                  <th>Score Corr</th>
                  <th>Return Corr</th>
                  <th>同时触发</th>
                </tr>
              </thead>
              <tbody>
                {combo.similarity_rows.slice(0, 5).map((row) => (
                  <tr key={`${combo.combo_key}-similarity-${row.rule_name}`}>
                    <td>
                      <strong>{row.rule_name}</strong>
                      {row.explain ? (
                        <div className="expression-backtest-similarity-explain">{row.explain}</div>
                      ) : null}
                    </td>
                    <td>{formatNumber(row.jaccard, 3)}</td>
                    <td>{formatNumber(row.phi, 3)}</td>
                    <td>{formatNumber(row.score_pearson, 3)}</td>
                    <td>{formatNumber(row.return_pearson, 3)}</td>
                    <td>{row.overlap_samples}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <details className="expression-backtest-fold">
            <summary className="expression-backtest-fold-summary">
              <h4>展开全部重复性指标（{combo.similarity_rows.length} 条）</h4>
            </summary>
            <div className="expression-backtest-table-wrap">
              <table className="expression-backtest-table">
                <thead>
                  <tr>
                    <th>策略</th>
                    <th>Jaccard</th>
                    <th>Phi</th>
                    <th>Score Corr</th>
                    <th>Return Corr</th>
                    <th>同时触发</th>
                    <th>占当前组合</th>
                    <th>占已有策略</th>
                    <th>Lift</th>
                    <th>共享收益日</th>
                  </tr>
                </thead>
                <tbody>
                  {combo.similarity_rows.map((row) => (
                    <tr key={`${combo.combo_key}-similarity-all-${row.rule_name}`}>
                      <td>{row.rule_name}</td>
                      <td>{formatNumber(row.jaccard, 3)}</td>
                      <td>{formatNumber(row.phi, 3)}</td>
                      <td>{formatNumber(row.score_pearson, 3)}</td>
                      <td>{formatNumber(row.return_pearson, 3)}</td>
                      <td>{row.overlap_samples}</td>
                      <td>{formatRate(row.overlap_rate_vs_validation)}</td>
                      <td>{formatRate(row.overlap_rate_vs_existing)}</td>
                      <td>{formatLift(row.overlap_lift)}</td>
                      <td>{row.shared_return_days}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </details>
        </>
      )}
      <p className="expression-backtest-caption">
        相关性只描述重合程度，不代表策略价值：重合高但样本外仍有正增量可以同时成立，低相关也不等于更好。
        其中 Return Corr 与增量收益统一使用 Σ(score × residual) / Σ|score| 的日收益口径。
      </p>
    </div>
  );
}
