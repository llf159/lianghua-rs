import { useMemo } from "react";
import type { RuleValidationComboResult } from "../../../../apis/strategyTrigger";
import { formatBacktestValue, formatNumber, normalizeDateInput } from "../../../../shared/backtestFormat";

export function ExpressionQuarterSummary({ combo, startDate, endDate }: {
  combo: RuleValidationComboResult;
  startDate: string;
  endDate: string;
}) {
  const quarters = useMemo(() => {
    const start = normalizeDateInput(startDate);
    const end = normalizeDateInput(endDate);
    if (!/^\d{8}$/.test(start) || !/^\d{8}$/.test(end) || start > end) return [];
    const startYear = Number(start.slice(0, 4));
    const endYear = Number(end.slice(0, 4));
    const startQuarter = Math.floor((Number(start.slice(4, 6)) - 1) / 3);
    const endQuarter = Math.floor((Number(end.slice(4, 6)) - 1) / 3);
    if (startQuarter < 0 || startQuarter > 3 || endQuarter < 0 || endQuarter > 3) return [];
    const rows: Array<{ key: string; boundary: string; returns: number[]; ics: number[]; months: Array<{ key: string; boundary: string; returns: number[]; ics: number[] }> }> = [];
    for (let index = startYear * 4 + startQuarter; index <= endYear * 4 + endQuarter; index += 1) {
      const year = Math.floor(index / 4);
      const quarter = index % 4;
      const first = `${year}${["0101", "0401", "0701", "1001"][quarter]}`;
      const last = `${year}${["0331", "0630", "0930", "1231"][quarter]}`;
      const months = [1, 2, 3].map((offset) => {
        const month = quarter * 3 + offset;
        const key = `${year}${String(month).padStart(2, "0")}`;
        const firstDay = `${key}01`;
        const lastDay = `${key}${String(new Date(Date.UTC(year, month, 0)).getUTCDate()).padStart(2, "0")}`;
        return { key, boundary: end < firstDay || start > lastDay ? "区间外" : start > firstDay || end < lastDay ? "自然边界未覆盖" : "自然边界覆盖", returns: [] as number[], ics: [] as number[] };
      });
      rows.push({ key: `${year} Q${quarter + 1}`, boundary: start > first || end < last ? "自然边界未覆盖" : "自然边界覆盖", returns: [], ics: [], months });
    }
    const byQuarter = new Map(rows.map((row) => [row.key, row]));
    for (const day of combo.daily_metrics ?? []) {
      const month = Number(day.trade_date.slice(4, 6));
      if (!Number.isInteger(month) || month < 1 || month > 12) continue;
      const row = byQuarter.get(`${day.trade_date.slice(0, 4)} Q${Math.floor((month - 1) / 3) + 1}`);
      if (!row) continue;
      const monthRow = row.months.find((item) => item.key === day.trade_date.slice(0, 6));
      if (day.avg_residual_return != null && Number.isFinite(day.avg_residual_return)) {
        row.returns.push(day.avg_residual_return);
        monthRow?.returns.push(day.avg_residual_return);
      }
      if (day.ic != null && Number.isFinite(day.ic)) {
        row.ics.push(day.ic);
        monthRow?.ics.push(day.ic);
      }
    }
    return rows;
  }, [combo.daily_metrics, startDate, endDate]);

  if (!quarters.length) return null;
  return <div className="expression-backtest-section">
    <h3>季度表现</h3>
    <p className="expression-backtest-caption">
      使用每日触发样本相对全市场的方向调整后差值，按有效日期等权；它不是同板块、同市值对照。
      有效日受每日最少样本限制。无有效日的季度保留为空，不计为负；自然日期边界与标签成熟情况不能证明季度完整。
    </p>
    <p className="expression-backtest-caption">自然边界覆盖的季度中：有有效数据且日均差为负 {quarters.filter((row) => row.boundary === "自然边界覆盖" && row.returns.length > 0 && row.returns.reduce((sum, value) => sum + value, 0) < 0).length} 个，无有效日 {quarters.filter((row) => row.boundary === "自然边界覆盖" && row.returns.length === 0).length} 个；区间截断 {quarters.filter((row) => row.boundary !== "自然边界覆盖").length} 个。自然边界覆盖不代表标签成熟。</p>
    <div className="expression-backtest-table-wrap"><table className="expression-backtest-table">
      <thead><tr><th>季度</th><th>区间</th><th>收益有效日</th><th>方向调整后日均差</th><th>IC 有效日</th><th>平均 IC</th><th>月度</th></tr></thead>
      <tbody>{quarters.map((row) => <tr key={row.key}>
        <td>{row.key}</td><td>{row.boundary}</td><td>{row.returns.length}</td>
        <td>{formatBacktestValue(row.returns.length ? row.returns.reduce((sum, value) => sum + value, 0) / row.returns.length : null, combo.backtest.return_metric)}</td>
        <td>{row.ics.length}</td><td>{formatNumber(row.ics.length ? row.ics.reduce((sum, value) => sum + value, 0) / row.ics.length : null)}</td>
        <td><details><summary>查看月份</summary>{row.months.map((month) => <div key={month.key}>
          {month.key.slice(0, 4)}-{month.key.slice(4)} · {month.boundary} · {month.returns.length} 个有效收益日 · {formatBacktestValue(month.returns.length ? month.returns.reduce((sum, value) => sum + value, 0) / month.returns.length : null, combo.backtest.return_metric)} · IC {formatNumber(month.ics.length ? month.ics.reduce((sum, value) => sum + value, 0) / month.ics.length : null)}
        </div>)}</details></td>
      </tr>)}</tbody>
    </table></div>
  </div>;
}
