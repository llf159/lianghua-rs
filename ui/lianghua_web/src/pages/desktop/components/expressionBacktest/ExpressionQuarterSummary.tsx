import { useMemo, useState } from "react";
import type { RuleValidationComboResult } from "../../../../apis/strategyTrigger";
import { formatBacktestValue, formatNumber, normalizeDateInput } from "../../../../shared/backtestFormat";

export function ExpressionQuarterSummary({combo, startDate, endDate}: {combo: RuleValidationComboResult; startDate: string; endDate: string}) {
  const [year, setYear] = useState("");
  const quarters = useMemo(() => {
    const start = normalizeDateInput(startDate);
    const end = normalizeDateInput(endDate);
    if (!/^\d{8}$/.test(start) || !/^\d{8}$/.test(end) || start > end) return [];
    const rows: Array<{period: string; partial: boolean; return_days: number; mean_return: number | null; ic_days: number; mean_ic: number | null}> = [];
    for (let index = Number(start.slice(0, 4)) * 4 + Math.floor((Number(start.slice(4, 6)) - 1) / 3); index <= Number(end.slice(0, 4)) * 4 + Math.floor((Number(end.slice(4, 6)) - 1) / 3); index += 1) {
      const period = `${Math.floor(index / 4)} Q${index % 4 + 1}`;
      const saved = combo.quarter_summaries?.find((item) => item.period === period);
      const days = (combo.daily_metrics ?? []).filter((item) => `${item.trade_date.slice(0, 4)} Q${Math.floor((Number(item.trade_date.slice(4, 6)) - 1) / 3) + 1}` === period);
      const returns = days.flatMap((item) => item.avg_residual_return != null && Number.isFinite(item.avg_residual_return) ? [item.avg_residual_return] : []);
      const ics = days.flatMap((item) => item.ic != null && Number.isFinite(item.ic) ? [item.ic] : []);
      rows.push({period, partial: start > `${Math.floor(index / 4)}${["0101", "0401", "0701", "1001"][index % 4]}` || end < `${Math.floor(index / 4)}${["0331", "0630", "0930", "1231"][index % 4]}`, return_days: saved?.return_days ?? returns.length, mean_return: saved ? saved.mean_return : returns.length ? returns.reduce((sum, value) => sum + value, 0) / returns.length : null, ic_days: saved?.ic_days ?? ics.length, mean_ic: saved ? saved.mean_ic : ics.length ? ics.reduce((sum, value) => sum + value, 0) / ics.length : null});
    }
    return rows;
  }, [combo, startDate, endDate]);
  const years = Array.from(new Set(quarters.map((item) => item.period.slice(0, 4))));
  const selectedYear = years.includes(year) ? year : years.at(-1) ?? "";
  return <section className="expression-backtest-section">
    <h3>季度绩效（Quarterly Performance）</h3>
    <p className="research-meta">按有效交易日等权统计触发组相对全市场的超额表现，已按策略方向调整；与“匹配对照分析”的比较基准不同。</p>
    <p className="research-meta">全区间共 {quarters.length} 个季度：正收益 {quarters.filter((item) => item.mean_return != null && item.mean_return > 0).length}，负收益 {quarters.filter((item) => item.mean_return != null && item.mean_return < 0).length}，无有效收益 {quarters.filter((item) => !item.return_days).length}。</p>
    <label className="research-field">年份<select value={selectedYear} onChange={(event) => setYear(event.target.value)}>{years.map((item) => <option key={item} value={item}>{item}</option>)}</select></label>
    <div className="expression-backtest-table-wrap"><table className="expression-backtest-table"><thead><tr><th>季度</th><th>区间覆盖</th><th>有效收益日</th><th>超额收益均值</th><th>IC 有效日</th><th>平均 IC</th></tr></thead><tbody>{quarters.filter((item) => item.period.startsWith(selectedYear)).map((item) => <tr key={item.period}><td>{item.period}</td><td>{item.partial ? "区间截断" : "自然边界覆盖"}</td><td>{item.return_days}</td><td>{formatBacktestValue(item.mean_return, combo.backtest.return_metric)}</td><td>{item.ic_days}</td><td>{formatNumber(item.mean_ic)}</td></tr>)}</tbody></table></div>
    <p className="research-meta">IC 为信号分数与后验收益的秩相关。日期边界覆盖不代表标签全部成熟，无有效数据的季度显示“—”。</p>
  </section>;
}
