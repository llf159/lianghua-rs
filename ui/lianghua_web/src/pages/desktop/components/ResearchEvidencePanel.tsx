import { useEffect, useState } from "react";
import { listResearchEvidence, summarizeResearchEvidence, readResearchArchive, saveResearchArchive, type ResearchEvidenceQuery, type ResearchEvidenceSample, type ResearchEvidenceSummary } from "../../../apis/strategyTrigger";
import { formatDateLabel, formatNumber, formatBacktestValue } from "../../../shared/backtestFormat";
import DetailsLink from "../../../shared/DetailsLink";

export function ResearchEvidencePanel({sourcePath, runId, comboKey, horizon, startDate, endDate, direction, initialQuery, onBranchSaved}: {initialQuery?: Partial<ResearchEvidenceQuery>; onBranchSaved?: (runId: string) => void; sourcePath: string; runId: string; comboKey: string; horizon: number; startDate: string; endDate: string; direction: number}) {
  const [dimension, setDimension] = useState(initialQuery?.dimension ?? "all");
  const [frequency, setFrequency] = useState("all");
  const [summaryResponse, setSummaryResponse] = useState<{key: string; rows: ResearchEvidenceSummary[]; error: string}>({key: "", rows: [], error: ""});
  const [query, setQuery] = useState<ResearchEvidenceQuery>({start_date: startDate, end_date: endDate, dimension: "all", bucket: null, state: "triggered", offset: 0, limit: 50, ...initialQuery, run_id: runId, combo_key: comboKey, horizon});
  const [page, setPage] = useState<{key: string; total: number; items: ResearchEvidenceSample[]; error: string}>({key: "", total: 0, items: [], error: ""});
  const summaryKey = JSON.stringify([sourcePath,runId,comboKey,horizon,startDate,endDate,dimension,frequency]);
  const pageKey = JSON.stringify([sourcePath,query]);
  const summaries = summaryResponse.key === summaryKey ? summaryResponse.rows : [];
  const items = page.key === pageKey ? page.items : [];
  const total = page.key === pageKey ? page.total : 0;
  const loading = page.key !== pageKey;
  const error = (page.key === pageKey ? page.error : "") || (summaryResponse.key === summaryKey ? summaryResponse.error : "");
  const [branchMessage, setBranchMessage] = useState("");
  const [branchBusy, setBranchBusy] = useState(false);
  const [showOutcomes, setShowOutcomes] = useState(true);
  const [showSamples, setShowSamples] = useState(Boolean(initialQuery));
  const [showSampleFields, setShowSampleFields] = useState(false);
  useEffect(() => {
    let cancelled = false;
    void summarizeResearchEvidence(sourcePath, {run_id: runId, combo_key: comboKey, horizon, start_date: startDate, end_date: endDate, dimension, bucket: null, state: "all", offset: 0, limit: 50}, frequency).then((rows) => {
      if (!cancelled) setSummaryResponse({key: summaryKey, rows, error: ""});
    }).catch((failure) => {if (!cancelled) setSummaryResponse({key: summaryKey, rows: [], error: String(failure)});});
    return () => {cancelled = true;};
  }, [sourcePath, runId, comboKey, horizon, startDate, endDate, dimension, frequency, summaryKey]);
  useEffect(() => {
    if (!showSamples) return;
    let cancelled = false;
    void listResearchEvidence(sourcePath, query).then((response) => {
      if (!cancelled) setPage({key: pageKey, total: response.total, items: response.items, error: ""});
    }).catch((failure) => {if (!cancelled) setPage({key: pageKey, total: 0, items: [], error: String(failure)});});
    return () => {cancelled = true;};
  }, [sourcePath, query, pageKey, showSamples]);
  async function saveBranch() {
    setBranchBusy(true); setBranchMessage("");
    try {
      const source = await readResearchArchive(sourcePath,runId);
      const header = await saveResearchArchive(sourcePath,runId,`探索切片：${query.dimension} / ${query.bucket ?? "全部层"}`,{exploratory_filter: query},source.result,"expression_validation",runId);
      setBranchMessage("探索分支已保存。");
      onBranchSaved?.(header.run_id);
    } catch (failure) {setBranchMessage(`保存分支失败：${String(failure)}`);}
    finally {setBranchBusy(false);}
  }
  function drill(row: ResearchEvidenceSummary) {
    setShowSamples(true);
    const quarter = /^(\d{4}) Q([1-4])$/.exec(row.period);
    const month = /^(\d{4})(\d{2})$/.exec(row.period);
    const from = quarter ? `${quarter[1]}${["0101","0401","0701","1001"][Number(quarter[2])-1]}` : month ? `${row.period}01` : startDate;
    const to = quarter ? `${quarter[1]}${["0331","0630","0930","1231"][Number(quarter[2])-1]}` : month ? `${row.period}${new Date(Number(month[1]), Number(month[2]), 0).getDate()}` : endDate;
    setQuery({...query, dimension, bucket: row.bucket, start_date: from > startDate ? from : startDate, end_date: to < endDate ? to : endDate, offset: 0});
  }
  return <section className="expression-backtest-section">
    <h3>适用范围与同类对照</h3>

    <div className="research-toolbar">
      <label className="research-field">查看范围<select value={dimension} onChange={(event) => setDimension(event.target.value)}>
        <option value="all">全部</option><option value="board">代码板块</option><option value="market_cap">信号日市值组</option><option value="baseline_score">原榜分数档</option>{["D10","F5","CP5","VA5","LO5","AC5","IS5","CF20","DT5","SC5"].map((name) => <option key={name} value={`MKT_${name}`}>MKT_{name}</option>)}
      </select></label>{" "}
      <label className="research-field">统计周期<select value={frequency} onChange={(event) => setFrequency(event.target.value)}><option value="quarter">季度</option><option value="month">月度</option><option value="all">全部</option></select></label>
    </div>
    {error ? <p role="alert">{error}</p> : null}
    {summaryResponse.key !== summaryKey ? <p role="status">正在汇总保存结果，无需重新回测…</p> : null}
    <div className="research-feedback-grid">{summaries.map((row) => <div key={`${row.period}-${row.bucket}`}>
      <span>{row.period} / {row.bucket} · 同类对照差（方向调整）</span>
      <strong>{formatBacktestValue(row.matched_difference == null ? null : row.matched_difference * direction, "residual_return")}</strong>
      <small>{row.matched_days} 个匹配日，{row.matched_triggered.toLocaleString()} 个匹配触发股票日。</small>
      <small>共触发 {row.triggered.toLocaleString()} 个股票日，其中 {row.labeled.toLocaleString()} 个有标签，覆盖 {row.different_stocks.toLocaleString()} 只股票。缺标签 {row.missing_label.toLocaleString()} 个。</small>
      <button type="button" onClick={() => drill(row)}>样本明细</button>
    </div>)}</div>
    <p className="research-meta">以上是日期等权的残差差值，单位为百分点；同类对照先匹配同日、同板块及已配置的市值组和分数档。样本数不等于独立证据数。</p>
    <details><summary>查看分组完整统计与区间</summary>
    <div style={{overflowX: "auto"}}><table className="expression-backtest-table"><thead><tr><th>期间 / 层</th><th>触发 / 标签 / 股票</th><th>日均残差</th><th>股票日均</th><th>同类差（原始 / 方向调整）</th><th>匹配触发 / 未匹配标签 / 日</th><th>同类差95%区间 / HAC t / 家族p</th><th>标签最晚末日</th><th>样本</th></tr></thead><tbody>
      {summaries.map((row) => <tr key={`${row.period}-${row.bucket}`}><td>{row.period} / {row.bucket}</td><td>{row.triggered} / {row.labeled} / {row.different_stocks}</td><td>{formatBacktestValue(row.raw_mean, "residual_return")}</td><td>{formatBacktestValue(row.stock_day_mean, "residual_return")}</td><td>{formatBacktestValue(row.matched_difference, "residual_return")} / {formatBacktestValue(row.matched_difference == null ? null : row.matched_difference * direction, "residual_return")}</td><td>{row.matched_triggered} / {row.labeled-row.matched_triggered} / {row.matched_days}</td><td>{formatNumber(row.statistics?.lower)} ~ {formatNumber(row.statistics?.upper)} / {formatNumber(row.statistics?.hac_t)} / {formatNumber(row.statistics?.family_adjusted_p)}</td><td>{row.label_end_date ? formatDateLabel(row.label_end_date) : "—"}</td><td><button type="button" onClick={() => drill(row)}>查看</button></td></tr>)}
    </tbody></table></div>
    </details>
    <details><summary>覆盖与缺失计数</summary><p>{summaries[0]?.market_status}</p>{summaries.map((row) => <p key={`${row.period}-${row.bucket}`}>{row.period} / {row.bucket}：{row.observations} 个股票日；资格未知 {row.unknown_eligibility}，资格不符 {row.ineligible}，历史缺行 {row.missing_history}，表达式不可用 {row.unavailable}，触发缺标签 {row.missing_label}（区间末端未成熟 {row.immature_label}，个股后续行情不足 {row.insufficient_quote_label}，无效行情或扣减资料缺失 {row.missing_factor_label}）。不足与截断不作为负收益，季度边界以运行日期为准。</p>)}</details>
    <details open={showSamples} onToggle={(event) => setShowSamples(event.currentTarget.open)}><summary>样本明细</summary>
    {showSamples ? <>
    <button type="button" disabled={branchBusy || loading} onClick={() => void saveBranch()}>保存为探索分支</button>{branchMessage ? <p role="status">{branchMessage}</p> : null}
    <h4>已保存样本 · {query.bucket ?? "全部层"} · {formatDateLabel(query.start_date)} ~ {formatDateLabel(query.end_date)}</h4>
    <div className="research-toolbar"><label className="research-field">样本范围<select value={query.state} onChange={(event) => {setQuery({...query, state: event.target.value, offset: 0}); setShowOutcomes(true);}}>{[["triggered","全部触发"],["negative","原始残差为负"],["positive","原始残差为正"],["missing_label","触发缺标签"],["unavailable","表达式不可用"],["missing_history","历史行情缺失"],["unknown_eligibility","资格未知"],["ineligible","资格不符"],["not_triggered","可计算未触发"],["all","全部股票日"]].map(([value,label]) => <option key={value} value={value}>{label}</option>)}</select></label>{" "}
    <button type="button" onClick={() => setQuery({...query, dimension, bucket: null, start_date: startDate, end_date: endDate, offset: 0})}>清除期间与层筛选</button>{" "}
    <label className="research-checkbox"><input type="checkbox" checked={showOutcomes} onChange={(event) => setShowOutcomes(event.target.checked)} />显示后验与行情</label>
    <label className="research-checkbox"><input type="checkbox" checked={showSampleFields} onChange={(event) => setShowSampleFields(event.target.checked)} />展开技术字段</label></div>
    <div style={{overflowX: "auto"}}><table className="expression-backtest-table"><thead><tr><th>信号日</th><th>股票 / 板块 / 市值</th>{showSampleFields ? <><th>信号分</th><th>原榜分 / 名次</th><th>入场 / 标签末日</th></> : null}<th>残差</th><th>缺失原因</th></tr></thead><tbody>{items.map((row) => <tr key={`${row.ts_code}-${row.trade_date}`}><td>{formatDateLabel(row.trade_date)}</td><td>{showOutcomes ? <DetailsLink sourcePath={sourcePath} tsCode={row.ts_code} tradeDate={row.trade_date}>{row.ts_code}</DetailsLink> : row.ts_code} / {row.board} / {formatNumber(row.market_cap)}</td>{showSampleFields ? <><td>{formatNumber(row.score)}</td><td>{formatNumber(row.baseline_score)} / {row.baseline_rank ?? "—"}</td><td>{showOutcomes ? `${row.entry_date ?? "—"} / ${row.label_end_date ?? "—"}` : "已遮住"}</td></> : null}<td>{showOutcomes ? formatBacktestValue(row.residual_return, "residual_return") : "已遮住"}</td><td>{row.missing_reason ?? "—"}</td></tr>)}</tbody></table></div>
    <p className="research-meta">明细用于核对信号日与后验结果，整体效果以上方汇总为准；按正负结果筛选后的页面不能用于估计整体成功率。</p>
    <div className="research-pagination"><button type="button" disabled={loading || query.offset === 0} onClick={() => setQuery({...query, offset: Math.max(0,query.offset-query.limit)})}>上一页</button>{" "}{loading ? "读取中…" : `${total ? query.offset+1 : 0}–${Math.min(total,query.offset+query.limit)} / ${total}`}{" "}<button type="button" disabled={loading || query.offset+query.limit>=total} onClick={() => setQuery({...query, offset: query.offset+query.limit})}>下一页</button></div>
    </> : null}</details>
  </section>;
}
