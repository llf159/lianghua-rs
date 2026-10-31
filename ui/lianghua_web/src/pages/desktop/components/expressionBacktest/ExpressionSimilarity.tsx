import { useState } from "react";
import type { RuleValidationComboResult } from "../../../../apis/strategyTrigger";
import { formatNumber } from "../../../../shared/backtestFormat";

export function ExpressionSimilarity({ combo, baselineOrigin }: { combo: RuleValidationComboResult; baselineOrigin?: string }) {
  const [period, setPeriod] = useState("全部");
  const periods = Array.from(new Set(combo.similarity_rows.map((row) => row.period || "全部")));
  const selectedPeriod = periods.includes(period) ? period : "全部";
  const rows = combo.similarity_rows.filter((row) => (row.period || "全部") === selectedPeriod);
  const topSimilarity = rows[0] ?? null;
  return <div className="expression-backtest-section">
    <h3>策略相关性与重叠度</h3>
    {rows.length === 0 ? <div className="expression-backtest-empty">当前没有可比较的基线分项。</div> : <>
      <div className="research-toolbar">
        <label className="research-field">统计期间<select value={selectedPeriod} onChange={(event) => setPeriod(event.target.value)}>{periods.map((value) => <option key={value} value={value}>{value}</option>)}</select></label>
        <span className="research-meta">最相似：{topSimilarity?.rule_name} · Jaccard {formatNumber(topSimilarity?.jaccard, 3)}</span>
      </div>
      {topSimilarity?.shared_score_samples != null ? <p className="research-meta">共同评分 {topSimilarity.shared_score_samples} / 可评价股票日 {topSimilarity.universe_samples}；表达式可计算 {topSimilarity.candidate_available_samples}，基线评分可用 {topSimilarity.baseline_available_samples}。</p> : null}
      <div className="expression-backtest-table-wrap"><table className="expression-backtest-table">
        <thead><tr><th>策略</th><th>Jaccard</th><th>Phi</th><th>得分相关</th><th>收益相关</th><th>共同收益日</th><th>同时触发</th></tr></thead>
        <tbody>{rows.slice(0,5).map((row) => <tr key={row.rule_name}>
          <td><strong>{row.rule_name}</strong>{row.explain ? <div className="expression-backtest-similarity-explain">{row.explain}</div> : null}</td>
          <td>{formatNumber(row.jaccard,3)}</td><td>{formatNumber(row.phi,3)}</td><td>{formatNumber(row.score_pearson,3)}</td><td>{formatNumber(row.return_pearson,3)}</td><td>{row.shared_return_days}</td><td>{row.overlap_samples}</td>
        </tr>)}</tbody>
      </table></div>
    </>}
    <p className="research-meta">Jaccard 衡量触发集合的交并比；Phi 衡量是否触发的相关性；得分相关与收益相关分别比较评分和日度收益序列。仅展示重叠最高的5条规则。</p>
    <p className="expression-backtest-caption">{baselineOrigin === "isolated_native_scoring_v1" ? "使用本次隔离基线，表达式不可计算和缺评分的股票日不补零。" : "旧结果使用历史评分，同源未认证。"}季度沿用全区间最相似规则；重合程度不代表策略价值。</p>
  </div>;
}
