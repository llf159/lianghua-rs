import { useEffect, useState } from "react";
import { summarizeResearchEvidence, type ResearchEvidenceSummary } from "../../../apis/strategyTrigger";
import { formatBacktestValue, formatNumber } from "../../../shared/backtestFormat";

export function ResearchEvidencePanel({sourcePath, runId, comboKey, horizon, startDate, endDate, direction, savedSummaries}: {
  sourcePath: string; runId: string; comboKey: string; horizon: number; startDate: string; endDate: string; direction: number;
  savedSummaries?: Array<{dimension: string; rows: ResearchEvidenceSummary[]}>;
}) {
  const [dimension, setDimension] = useState("all");
  const [bucket, setBucket] = useState("");
  const [response, setResponse] = useState<{key: string; rows: ResearchEvidenceSummary[]; error?: string}>({key: "", rows: []});
  const key = JSON.stringify([sourcePath, runId, comboKey, horizon, dimension]);
  useEffect(() => {
    if (savedSummaries) return;
    let cancelled = false;
    void summarizeResearchEvidence(sourcePath, {run_id: runId, combo_key: comboKey, horizon, start_date: startDate, end_date: endDate, dimension, bucket: null, state: "all", offset: 0, limit: 50}, "all").then((rows) => {
      if (!cancelled) setResponse({key, rows});
    }).catch((error) => {if (!cancelled) setResponse({key, rows: [], error: String(error)});});
    return () => {cancelled = true;};
  }, [sourcePath, runId, comboKey, horizon, startDate, endDate, dimension, savedSummaries, key]);
  const rows = savedSummaries?.find((item) => item.dimension === dimension)?.rows ?? (response.key === key ? response.rows : []);
  const selected = rows.find((item) => item.bucket === bucket) ?? rows[0];
  return <section className="expression-backtest-section">
    <h3>匹配对照分析</h3>
    <p className="research-meta">Matched Control：将触发股票与同日、同板块及已配置市值组／分数档的未触发股票比较，日内计算差值，再对日期等权平均。</p>
    <div className="research-toolbar">
      <label className="research-field">分组维度<select value={dimension} onChange={(event) => {setDimension(event.target.value); setBucket("");}}>
        {(savedSummaries?.map((item) => item.dimension) ?? ["all", "board", "market_cap", "baseline_score"]).map((item) => <option key={item} value={item}>{({all: "全样本", board: "代码板块", market_cap: "市值分组", baseline_score: "基线分数分组"} as Record<string, string>)[item] ?? item}</option>)}
      </select></label>
      <label className="research-field">分组<select value={selected?.bucket ?? ""} onChange={(event) => setBucket(event.target.value)}>{rows.map((item) => <option key={item.bucket} value={item.bucket}>{item.bucket}</option>)}</select></label>
    </div>
    {!savedSummaries && response.key !== key ? <p role="status">正在读取统计…</p> : null}
    {response.key === key && response.error ? <p role="alert">{response.error}</p> : null}
    {selected ? <>
      <div className="research-feedback-grid">
        <div><span>匹配超额收益</span><strong>{formatBacktestValue(selected.matched_difference == null ? null : selected.matched_difference * direction, "residual_return")}</strong><small>触发组减匹配对照组，已按策略方向调整，单位为百分点。</small></div>
        <div><span>95% 置信区间（CI）</span><strong>{formatNumber(direction < 0 && selected.statistics.upper != null ? -selected.statistics.upper : selected.statistics.lower)} ~ {formatNumber(direction < 0 && selected.statistics.lower != null ? -selected.statistics.lower : selected.statistics.upper)}</strong><small>日期块重采样得到的描述性区间；包含零时，优势方向尚不明确。</small></div>
        <div><span>匹配有效日</span><strong>{selected.matched_days}</strong><small>{selected.matched_triggered.toLocaleString()} 个触发股票日同时具有收益标签和可用对照。</small></div>
        <div><span>标签覆盖率</span><strong>{selected.triggered ? `${(selected.labeled / selected.triggered * 100).toFixed(1)}%` : "—"}</strong><small>{selected.triggered.toLocaleString()} 个触发股票日中，{selected.labeled.toLocaleString()} 个具有有效后验，涉及 {selected.different_stocks.toLocaleString()} 只股票。</small></div>
      </div>
      <p className="research-meta">缺失统计：标签缺失 {selected.missing_label.toLocaleString()}，表达式不可计算 {selected.unavailable.toLocaleString()}，行情缺失 {selected.missing_history.toLocaleString()}，资格未知 {selected.unknown_eligibility.toLocaleString()}。缺失收益不按零计入。</p>
    </> : null}
  </section>;
}
