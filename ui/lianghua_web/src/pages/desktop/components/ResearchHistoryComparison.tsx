import { useEffect, useState } from "react";
import {
  readResearchArchive,
  updateResearchArchiveMetadata,
  researchRunStatusLabel,
  type ResearchArchiveHeader,
  type RuleExpressionValidationData,
  type RuleExpressionValidationQuery,
  type RuleValidationComboResult,
  type RankComparisonResult,
  type RankComparisonRequest,
  type RankComparisonSummary,
  type BacktestReturnMetric,
} from "../../../apis/strategyTrigger";
import { formatBacktestValue, formatNumber } from "../../../shared/backtestFormat";

type Props = {
  sourcePath: string;
  archives: ResearchArchiveHeader[];
  disabled: boolean;
  onOpen: (runId: string) => void;
  onUpdated: (header: ResearchArchiveHeader) => void;
} & ({
  kind: "expression_validation";
  result: RuleExpressionValidationData | null;
  settings: RuleExpressionValidationQuery | null;
  combo: RuleValidationComboResult | null;
  horizon: number | null;
  direction: "positive" | "negative";
} | {
  kind: "rank_comparison";
  result: RankComparisonResult | null;
  settings: Omit<RankComparisonRequest, "candidate_config"> | null;
  summary: RankComparisonSummary | null;
});

function formatComparisonValue(value: number | null | undefined, unit: BacktestReturnMetric | "number" | "count") {
  if (value == null || !Number.isFinite(value)) return "—";
  return unit === "number" ? formatNumber(value) : unit === "count" ? value.toLocaleString() : formatBacktestValue(value, unit);
}

function archiveRecordLabel(header: ResearchArchiveHeader) {
  return [header.question || header.summary, header.note?.split("\n")[0].slice(0, 40), new Date(header.created_ms).toLocaleString()].filter(Boolean).join(" · ");
}

export function ResearchHistoryComparison(props: Props) {
  const [compareId, setCompareId] = useState("");
  const [editor, setEditor] = useState<{runId: string; name: string; note: string} | null>(null);
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState<{runId: string; text: string; error?: boolean} | null>(null);
  const [response, setResponse] = useState<{key: string; data?: Awaited<ReturnType<typeof readResearchArchive<RuleExpressionValidationData | RankComparisonResult>>>; error?: string}>({key: ""});
  const selectedCompareId = compareId !== props.result?.archive_run_id ? compareId : "";
  const responseKey = `${props.sourcePath}:${selectedCompareId}`;
  useEffect(() => {
    if (!selectedCompareId) return;
    let cancelled = false;
    void readResearchArchive<RuleExpressionValidationData | RankComparisonResult>(props.sourcePath, selectedCompareId).then((data) => {
      if (data.header.kind !== props.kind || (data.header.status && data.header.status !== "completed")) throw new Error("该记录没有可对比的已完成结果");
      if (!cancelled) setResponse({key: responseKey, data});
    }).catch((failure) => {if (!cancelled) setResponse({key: responseKey, error: String(failure)});});
    return () => {cancelled = true;};
  }, [props.sourcePath, props.kind, selectedCompareId, responseKey]);

  async function saveRecord() {
    if (!editor || saving || props.disabled) return;
    setSaving(true);
    try {
      const header = await updateResearchArchiveMetadata(props.sourcePath, editor.runId, editor.name, editor.note);
      props.onUpdated(header);
      setEditor(null);
      setMessage({runId: header.run_id, text: "已保存"});
    } catch (failure) {setMessage({runId: editor.runId, text: `保存失败：${String(failure)}`, error: true});}
    finally {setSaving(false);}
  }
  const saved = response.key === responseKey ? response.data : undefined;
  const completed = props.archives.filter((item) => item.kind === props.kind && (!item.status || item.status === "completed"));
  const rows: {label: string; current: number | null | undefined; saved: number | null | undefined; unit: BacktestReturnMetric | "number" | "count"; savedUnit?: BacktestReturnMetric}[] = [];
  let comparable = false;
  let formulas: [string, string] | undefined;
  if (saved && props.kind === "expression_validation" && props.combo && props.settings) {
    const configuration = saved.configuration as {query?: RuleExpressionValidationQuery; form?: {direction?: "positive" | "negative"}};
    const result = saved.result as RuleExpressionValidationData;
    const period = [result, ...(result.additional_periods ?? [])].find((item) => (item.horizon ?? configuration.query?.backtestPeriod) === props.horizon);
    const combo = period?.combo_results?.find((item) => JSON.stringify(item.unknown_values) === JSON.stringify(props.combo?.unknown_values));
    if (combo) {
      comparable = configuration.form?.direction === props.direction &&
        (["startDate", "endDate", "stockAdjType", "indexTsCode", "indexBeta", "conceptBeta", "industryBeta", "returnMetric", "minSamplesPerRuleDay", "minListedTradeDays", "board", "excludeStBoard", "totalMvMin", "totalMvMax"] as const)
          .every((key) => JSON.stringify(configuration.query?.[key]) === JSON.stringify(props.settings?.[key]));
      rows.push(
        {label: "方向回测指标", current: props.combo.backtest.avg_excess_residual_mean == null ? null : props.combo.backtest.avg_excess_residual_mean * (props.direction === "negative" ? -1 : 1), saved: combo.backtest.avg_excess_residual_mean == null || !configuration.form?.direction ? null : combo.backtest.avg_excess_residual_mean * (configuration.form.direction === "negative" ? -1 : 1), unit: props.combo.backtest.return_metric, savedUnit: combo.backtest.return_metric},
        {label: "IC", current: props.combo.backtest.ic_mean, saved: combo.backtest.ic_mean, unit: "number"},
        {label: "ICIR", current: props.combo.backtest.icir, saved: combo.backtest.icir, unit: "number"},
        {label: "触发股票日", current: props.combo.trigger_samples, saved: combo.trigger_samples, unit: "count"},
        {label: "有效触发日", current: props.combo.triggered_days, saved: combo.triggered_days, unit: "count"},
      );
      formulas = [props.combo.formula, combo.formula];
    }
  } else if (saved && props.kind === "rank_comparison" && props.summary && props.settings) {
    const configuration = saved.configuration as {settings?: Omit<RankComparisonRequest, "candidate_config">};
    const current = props.summary;
    const summary = (saved.result as RankComparisonResult).summaries?.find((item) => item.pool_kind === current.pool_kind && item.top_k === current.top_k && item.period === current.period && (item.dimension || "all") === (current.dimension || "all") && (item.bucket || "全部") === (current.bucket || "全部"));
    if (summary) {
      comparable = (["start_date", "end_date", "backtest_period", "stock_adj_type", "index_ts_code", "index_beta", "concept_beta", "industry_beta", "min_listed_trade_days", "grouping"] as const).every((key) => JSON.stringify(configuration.settings?.[key]) === JSON.stringify(props.settings?.[key]));
      rows.push(
        {label: "候选日均残差", current: current.candidate_paired_daily_mean, saved: summary.candidate_paired_daily_mean, unit: "residual_return"},
        {label: "相对基线成对差", current: current.paired_daily_difference, saved: summary.paired_daily_difference, unit: "residual_return"},
        {label: "IC", current: current.candidate_ic_mean, saved: summary.candidate_ic_mean, unit: "number"},
        {label: "ICIR", current: current.candidate_icir, saved: summary.candidate_icir, unit: "number"},
        {label: "成对收益日", current: current.paired_return_days, saved: summary.paired_return_days, unit: "count"},
      );
    }
  }
  return <section className="research-history">
    <div className="research-history-head"><h3>回测历史与对比</h3><span>{completed.length} 次已完成</span></div>
    <div className="research-history-selectors">
      <label className="research-field">查看结果<select aria-label="查看历史结果" disabled={props.disabled || saving} value={props.result?.archive_run_id ?? ""} onChange={(event) => {if (event.target.value) props.onOpen(event.target.value);}}>
        <option value="">{props.result ? "当前回测" : "选择历史记录"}</option>
        {props.archives.map((item) => <option key={item.run_id} value={item.run_id}>{archiveRecordLabel(item)} · {researchRunStatusLabel(item.status)}</option>)}
      </select></label>
      <label className="research-field">对比结果<select aria-label="选择对比结果" disabled={props.disabled || saving || !props.result} value={selectedCompareId} onChange={(event) => setCompareId(event.target.value)}>
        <option value="">不对比</option>
        {completed.filter((item) => item.run_id !== props.result?.archive_run_id).map((item) => <option key={item.run_id} value={item.run_id}>{archiveRecordLabel(item)}</option>)}
      </select></label>
    </div>
    <div className="research-records">{[
      {label: "当前结果", header: props.archives.find((item) => item.run_id === props.result?.archive_run_id)},
      {label: "对比结果", header: props.archives.find((item) => item.run_id === selectedCompareId)},
    ].map(({label, header}) => header ? <article className="research-record" key={header.run_id}>
      <div className="research-record-head"><strong>{header.question || label}</strong><button type="button" disabled={props.disabled || saving || header.status === "running"} onClick={() => {setEditor({runId: header.run_id, name: header.question, note: header.note ?? ""}); setMessage(null);}}>编辑备注</button></div>
      <time className="research-meta">{new Date(header.created_ms).toLocaleString()} · {researchRunStatusLabel(header.status)}</time>
      {header.summary ? <p className="research-record-summary">{header.summary}</p> : null}
      {editor?.runId === header.run_id ? <div className="research-record-editor">
        <label className="research-field">名称<input aria-label={`${label}名称`} value={editor.name} disabled={saving} onChange={(event) => setEditor({...editor, name: event.target.value})} /></label>
        <label className="research-field">备注<textarea aria-label={`${label}备注`} rows={3} value={editor.note} disabled={saving} onChange={(event) => setEditor({...editor, note: event.target.value})} /></label>
        <div className="research-toolbar"><button type="button" disabled={saving || props.disabled} onClick={() => void saveRecord()}>{saving ? "保存中…" : "保存备注"}</button><button type="button" disabled={saving} onClick={() => setEditor(null)}>取消</button></div>
      </div> : <p className="research-record-note">{header.note || "未填写备注"}</p>}
      {message?.runId === header.run_id ? <p className="research-meta" role={message.error ? "alert" : "status"}>{message.text}</p> : null}
    </article> : null)}</div>
    {selectedCompareId && props.result ?  <>
      {response.key !== responseKey ? <p role="status">读取对比结果…</p> : response.error ? <p role="alert">{response.error}</p> : !rows.length ? <p className="research-meta">该记录没有相同周期、参数组合或成员池的结果。</p> : <>
        <div className="expression-backtest-table-wrap"><table className="expression-backtest-table research-comparison-table"><thead><tr><th>指标</th><th>当前结果</th><th>对比结果</th><th>差值</th></tr></thead><tbody>{rows.map((row) => <tr key={row.label}><th>{row.label}</th><td>{formatComparisonValue(row.current, row.unit)}</td><td>{formatComparisonValue(row.saved, row.savedUnit ?? row.unit)}</td><td>{formatComparisonValue(comparable && row.current != null && row.saved != null ? row.current - row.saved : null, row.unit)}</td></tr>)}</tbody></table></div>
        <p className="research-meta">{comparable ? "差值为当前结果减对比结果，使用各自保存的统计值。" : "评价设置或方向不同，仅并列展示，不计算差值。"}</p>
        {formulas ? <details><summary>表达式对照</summary><div className="research-history-formulas"><div><span>当前结果</span><pre>{formulas[0]}</pre></div><div><span>对比结果</span><pre>{formulas[1]}</pre></div></div></details> : null}
      </>}
    </> : null}
  </section>;
}
