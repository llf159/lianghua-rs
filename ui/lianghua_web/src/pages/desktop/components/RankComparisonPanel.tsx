import "../css/SceneLayerBacktestPage.css";
import "../css/ResearchWorkbench.css";
import { Link } from "react-router-dom";
import { ResearchExportButton } from "./ResearchExportButton";
import { ResearchImportButton } from "./ResearchImportButton";
import { useEffect, useRef, useState } from "react";
import { listen } from "@tauri-apps/api/event";
import {
  readRankComparisonBaseline,
  type ResearchGrouping,
  type ResearchStatisticsSettings,
  runRankComparison,
  cancelRankComparison,
  listResearchArchives,
  readResearchArchive,
  saveResearchArchive,
  researchRunStatusLabel,
  deleteResearchArchive,
  type ResearchArchiveHeader,
  type RankComparisonRequest,
  type RankComparisonResult,
  type RankComparisonSummary,
} from "../../../apis/strategyTrigger";
import { ResearchGroupingFields } from "./ResearchGroupingFields";
import { ResearchStatisticsFields } from "./ResearchStatisticsFields";
import { RankComparisonSamples } from "./RankComparisonSamples";
import { ResearchNotesFields, type ResearchNotes } from "./ResearchNotesFields";
import { formatBacktestValue, formatDateLabel, formatNumber } from "../../../shared/backtestFormat";

type RequestSettings = Omit<RankComparisonRequest, "candidate_config">;

export function RankComparisonPanel({ settings, disabled, onRunningChange, onRestoreSettings }: {
  settings: RequestSettings;
  disabled: boolean;
  onRunningChange: (running: boolean) => void;
  onRestoreSettings?: (settings: RequestSettings) => void;
}) {
  const [candidate, setCandidate] = useState("");
  const [grouping, setGrouping] = useState<ResearchGrouping>({market_cap_boundaries: [], market_metric: "", market_boundaries: []});
  const [statistics, setStatistics] = useState<ResearchStatisticsSettings>({block_length: 10, resamples: 500, seed: 0});
  const [slice, setSlice] = useState("all::全部");
  const [editing, setEditing] = useState(false);
  const [verifyInputs, setVerifyInputs] = useState(false);
  const [result, setResult] = useState<RankComparisonResult | null>(null);
  const [selectedSamples, setSelectedSamples] = useState<RankComparisonSummary | null>(null);
  const [runSettings, setRunSettings] = useState<RequestSettings | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [archives, setArchives] = useState<ResearchArchiveHeader[]>([]);
  const [archiveParent, setArchiveParent] = useState<string | null>(null);
  const [question, setQuestion] = useState("");
  const [notes, setNotes] = useState<ResearchNotes>({ hypothesis: "", scope: "", explorationStart: "", explorationEnd: "", verificationStart: "", verificationEnd: "", historyReviewed: false, sortingEvidence: "", returnEvidence: "", conclusion: "" });
  const [archiveMessage, setArchiveMessage] = useState("");
  const [archiveBusy, setArchiveBusy] = useState(false);
  const [phase, setPhase] = useState("");
  const [cancelling, setCancelling] = useState(false);
  const activeTask = useRef("");
  const activeSource = useRef(settings.source_path);

  useEffect(() => {
    let unlisten: (() => void) | undefined;
    let cancelled = false;
    void listen<{ task_id: string; phase: string }>("rank-comparison-progress", (event) => {
      if (event.payload.task_id === activeTask.current) setPhase(event.payload.phase);
    }).then((stop) => { if (cancelled) stop(); else unlisten = stop; });
    return () => { cancelled = true; unlisten?.(); };
  }, []);

  useEffect(() => {
    activeSource.current = settings.source_path;
    let cancelled = false;
    if (settings.source_path) {
      void listResearchArchives(settings.source_path).then((items) => {
        if (!cancelled) setArchives(items.filter((item) => item.kind === "rank_comparison"));
      }).catch((failure) => { if (!cancelled) setArchiveMessage(`读取存档失败：${String(failure)}`); });
    }
    return () => { cancelled = true; activeSource.current = ""; };
  }, [settings.source_path]);

  async function loadBaseline() {
    try {
      setCandidate(await readRankComparisonBaseline(settings.source_path));
      setEditing(true);
      setError("");
    } catch (failure) { setError(`读取策略配置失败：${String(failure)}`); }
  }

  async function run() {
    if (!settings.source_path || !settings.index_ts_code || !candidate.trim() || !settings.start_date || !settings.end_date || settings.start_date > settings.end_date ||
      !Number.isInteger(settings.backtest_period) || settings.backtest_period < 1 || !Number.isInteger(settings.min_listed_trade_days) || settings.min_listed_trade_days < 0 ||
      ![settings.index_beta, settings.concept_beta, settings.industry_beta].every(Number.isFinite)) {
      setError("请填写候选配置、有效日期及回测参数。");
      return;
    }
    setLoading(true);
    setCancelling(false);
    activeTask.current = `${Date.now()}-${Math.random().toString(36).slice(2)}`;
    setPhase("等待仓库可用");
    onRunningChange(true);
    setError("");
    setResult(null);
    setSelectedSamples(null);
    try {
      const response = await runRankComparison({ ...settings, grouping, statistics, verify_inputs: verifyInputs, task_id: activeTask.current, research_question: question, research_notes: notes, parent_run_id: archiveParent, candidate_config: candidate });
      if (activeSource.current === settings.source_path) {
        setResult(response);
        setRunSettings({ ...settings, grouping, statistics, verify_inputs: verifyInputs, task_id: activeTask.current, research_question: question, research_notes: notes, parent_run_id: archiveParent });
        if (response.archive_run_id) {
          setArchiveParent(response.archive_run_id);
          setArchiveMessage(`结果已保存为 ${response.archive_run_id}。`);
          try { setArchives((await listResearchArchives(settings.source_path)).filter((item) => item.kind === "rank_comparison")); }
          catch (failure) { setArchiveMessage(`结果已保存，但读取存档列表失败：${String(failure)}`); }
        } else if (response.archive_error) setArchiveMessage(`结果尚未自动保存：${response.archive_error}`);
      }
    } catch (failure) {
      setError(`执行排名对照失败：${String(failure)}`);
      if (activeSource.current === settings.source_path) {
        try { setArchives((await listResearchArchives(settings.source_path)).filter((item) => item.kind === "rank_comparison")); }
        catch { /* 原始运行错误仍需显示。 */ }
      }
    }
    finally { activeTask.current = ""; setPhase(""); setCancelling(false); setLoading(false); onRunningChange(false); }
  }

  async function cancel() {
    if (!activeTask.current || cancelling) return;
    setCancelling(true);
    try {
      await cancelRankComparison(activeTask.current);
      setPhase("已请求取消，等待仓库锁或当前评分、后验阶段结束");
    } catch (failure) { setError(`取消请求失败：${String(failure)}`); setCancelling(false); }
  }

  async function saveDraft() {
    if (loading || archiveBusy || !settings.source_path) return;
    setArchiveBusy(true);
    try {
      const header = await saveResearchArchive(settings.source_path,archiveParent,question,{settings: {...settings, candidate_config: candidate, grouping, statistics, verify_inputs: verifyInputs}, notes},{},"rank_comparison",null,"draft");
      setArchiveParent(header.run_id);
      setArchives((await listResearchArchives(settings.source_path)).filter((run) => run.kind === "rank_comparison"));
      setArchiveMessage("候选草稿已保存。");
    } catch (failure) {setArchiveMessage(`保存草稿失败：${String(failure)}`);}
    finally {setArchiveBusy(false);}
  }

  async function saveArchive() {
    if (!result || !runSettings || archiveBusy) return;
    if (runSettings.source_path !== settings.source_path) {
      setArchiveMessage("这份结果属于其他数据目录，请在原目录保存。");
      return;
    }
    setArchiveBusy(true);
    try {
      const saved = await saveResearchArchive(settings.source_path, archiveParent, question, { settings: runSettings, notes }, result, "rank_comparison", result.membership_saved ? result.archive_run_id ?? null : null);
      setArchiveParent(saved.run_id);
      setResult({ ...result, archive_run_id: saved.run_id, membership_saved: saved.membership_saved });
      setArchives((await listResearchArchives(settings.source_path)).filter((item) => item.kind === "rank_comparison"));
      setArchiveMessage("结论已另存。");
    } catch (failure) { setArchiveMessage(`保存存档失败：${String(failure)}`); }
    finally { setArchiveBusy(false); }
  }

  async function openArchive(runId: string) {
    setArchiveBusy(true);
    try {
      const saved = await readResearchArchive<RankComparisonResult & { status?: string; error?: string }>(settings.source_path, runId);
      const config = saved.configuration as { settings?: RequestSettings & { candidate_config?: string }; notes?: ResearchNotes };
      if (!config?.settings) throw new Error("存档缺少排名对照配置");
      onRestoreSettings?.(config.settings);
      if (saved.header.status && saved.header.status !== "completed") {
        setCandidate(config.settings.candidate_config ?? "");
        setEditing(Boolean(config.settings.candidate_config));
        setResult(null);
        setSelectedSamples(null);
        setRunSettings(config.settings);
        setGrouping(config.settings.grouping ?? {market_cap_boundaries: [], market_metric: "", market_boundaries: []});
      setStatistics(config.settings.statistics ?? {block_length: 10, resamples: 500, seed: 0});
      setVerifyInputs(Boolean(config.settings.verify_inputs));
        setArchiveParent(saved.header.run_id);
        setQuestion(saved.header.question);
        setNotes({ hypothesis: "", scope: "", explorationStart: "", explorationEnd: "", verificationStart: "", verificationEnd: "", historyReviewed: false, sortingEvidence: "", returnEvidence: "", conclusion: "", ...config.notes });
        setArchiveMessage(`${researchRunStatusLabel(saved.header.status)}：${saved.result.error ?? "候选配置已恢复"}。可编辑后按当前页面参数创建新运行。`);
        return;
      }
      if (!Array.isArray(saved.result?.summaries) || typeof saved.result.candidate_config !== "string") throw new Error("存档缺少排名对照结果");
      setSelectedSamples(null);
      setResult({ ...saved.result, archive_run_id: saved.header.run_id, membership_saved: Boolean(saved.header.membership_saved) });
      setRunSettings(config.settings);
      setCandidate(saved.result.candidate_config);
      setGrouping(config.settings.grouping ?? {market_cap_boundaries: [], market_metric: "", market_boundaries: []});
      setStatistics(config.settings.statistics ?? {block_length: 10, resamples: 500, seed: 0});
      setVerifyInputs(Boolean(config.settings.verify_inputs));
      setEditing(true);
      setArchiveParent(saved.header.run_id);
      setQuestion(saved.header.question);
      setNotes({ hypothesis: "", scope: "", explorationStart: "", explorationEnd: "", verificationStart: "", verificationEnd: "", historyReviewed: false, sortingEvidence: "", returnEvidence: "", conclusion: "", ...config.notes });
      setArchiveMessage("已打开保存结果。");
    } catch (failure) { setArchiveMessage(`读取存档失败：${String(failure)}`); }
    finally { setArchiveBusy(false); }
  }

  async function removeArchive(runId: string) {
    if (!window.confirm("删除这份本机排名对照存档？")) return;
    setArchiveBusy(true);
    try {
      await deleteResearchArchive(settings.source_path, runId);
      setArchives((await listResearchArchives(settings.source_path)).filter((item) => item.kind === "rank_comparison"));
      if (archiveParent === runId) setArchiveParent(null);
      setArchiveMessage("排名对照存档已删除。");
    } catch (failure) { setArchiveMessage(`删除存档失败：${String(failure)}`); }
    finally { setArchiveBusy(false); }
  }

  const visibleSummaries = result?.summaries.filter((item) => `${item.dimension || "all"}::${item.bucket || "全部"}` === slice) ?? [];
  const sliceOptions = Array.from(new Set(result?.summaries.map((item) => `${item.dimension || "all"}::${item.bucket || "全部"}`) ?? []));
  return <section className="scene-layer-card research-workbench">
    <h2 className="scene-layer-title">排名对照</h2>

    <p className="scene-layer-caption">先复制当前策略，在候选框修改表达式、分值或规则，再重算对照。</p>
    <details><summary>实验记录与存档{archives.length ? ` · ${archives.length}` : ""}</summary>
    <ResearchImportButton sourcePath={settings.source_path} disabled={loading} onImported={() => {void listResearchArchives(settings.source_path).then((items) => setArchives(items.filter((item) => item.kind === "rank_comparison"))).catch((failure) => setArchiveMessage(String(failure)));}} />
    <div className="research-toolbar">
      <label className="research-field">研究问题<input placeholder="选填，便于查找这次实验" value={question} onChange={(event) => setQuestion(event.target.value)} /></label>
      <button type="button" disabled={archiveBusy || loading || !settings.source_path} onClick={() => void saveDraft()}>保存候选草稿</button>
      <button type="button" className="scene-layer-secondary-btn" disabled={!result || !runSettings || archiveBusy || loading} onClick={() => void saveArchive()}>另存结论</button>
    </div>
    <ResearchNotesFields value={notes} onChange={setNotes} />
    {archiveMessage ? <p className="scene-layer-caption" role="status">{archiveMessage}</p> : null}
    {archives.length ? <details><summary>已保存的 {archives.length} 次排名对照</summary>
      <ul className="research-archive-list">{archives.map((item) => <li key={item.run_id}>
        <span className="research-archive-name" title={item.run_id}>{item.question || "未命名实验"} · {researchRunStatusLabel(item.status)}<br />{new Date(item.created_ms).toLocaleString()}</span>
        <button type="button" disabled={archiveBusy || loading} onClick={() => void openArchive(item.run_id)}>查看</button>{" "}
        <button type="button" disabled={archiveBusy || loading} onClick={() => void removeArchive(item.run_id)}>删除</button>
      </li>)}</ul>
    </details> : null}
    </details>
    <div className="research-settings-grid"><ResearchGroupingFields value={grouping} onChange={setGrouping} disabled={loading} />
    <ResearchStatisticsFields value={statistics} onChange={setStatistics} disabled={loading} /></div>
    <label className="research-checkbox" title="运行前后两次读取完整数据库。文件变化时丢弃结果，不代表一致快照。"><input type="checkbox" checked={verifyInputs} disabled={loading} onChange={(event) => setVerifyInputs(event.target.checked)} />校验输入内容</label>
    <details><summary>计算口径</summary><p>先按完整榜单确定席位，再读取标签和切片，缺标签不补位。三日池取最近三个评分日的TopK并集；收益按共同有效日期成对比较，净贡献保留原基线池分母。</p><p>历史ST、行业和概念资料覆盖有限。输入校验不等于数据库一致快照；长区间会增加内存占用。</p></details>
    <div className="scene-layer-actions">
      <button type="button" className="scene-layer-secondary-btn" disabled={disabled || loading} onClick={() => void loadBaseline()}>复制当前策略到候选</button>
      <button type="button" className="scene-layer-primary-btn" disabled={disabled || loading || !candidate.trim()} onClick={() => void run()}>{loading ? "计算中…" : "重算基线与候选"}</button>
    </div>
    {editing ? <label className="research-field">候选方案 · TOML
      <textarea aria-label="候选策略 TOML" value={candidate} onChange={(event) => setCandidate(event.target.value)} rows={10} style={{ display: "block", width: "100%", boxSizing: "border-box", fontFamily: "monospace" }} />
    </label> : null}
    {loading && phase ? <p className="scene-layer-caption" role="status">{phase} {" "}<button type="button" disabled={cancelling} onClick={() => void cancel()}>取消</button></p> : null}
    {error ? <p role="alert">{error}</p> : null}
    {runSettings ? <details><summary>查看已记录的运行参数</summary>
      <p className="scene-layer-caption">区间 {formatDateLabel(runSettings.start_date)} ~ {formatDateLabel(runSettings.end_date)}；持有 {runSettings.backtest_period} 个交易日；最少上市 {runSettings.min_listed_trade_days} 个交易日；复权方式 {runSettings.stock_adj_type}；基准 {runSettings.index_ts_code}；指数、概念、行业扣减系数分别为 {runSettings.index_beta}、{runSettings.concept_beta}、{runSettings.industry_beta}；{runSettings.verify_inputs ? "开启" : "未开启"}输入内容校验。重新运行使用当前页面参数。</p>
    </details> : null}
    {result && runSettings ? <>
      <label className="research-field">查看范围<select value={slice} onChange={(event) => {setSlice(event.target.value); setSelectedSamples(null);}}>{sliceOptions.map((value) => <option key={value} value={value}>{value.replace("::", " / ")}</option>)}</select></label>{" "}
      {result.archive_run_id ? <Link to={`/backtest/strategy-paper-validation?researchRun=${encodeURIComponent(result.archive_run_id)}`} className="research-button">验证交易</Link> : null}{" "}
      {result.archive_run_id ? <ResearchExportButton sourcePath={settings.source_path} runId={result.archive_run_id} disabled={loading} /> : null}
      {!result.summaries.some((item) => item.common_ranking_days > 0) ? <p className="scene-layer-caption">没有共同有效排名日，无法计算对照。</p> : null}
      <details><summary>结果来源与覆盖</summary><p className="scene-layer-caption">当前结果使用运行时的配置快照，修改上方草稿不会改变它。{result.input_identity === "content_checked_not_snapshot" ? "所列文件运行前后内容一致（非一致快照）。" : "未进行输入内容校验。"} 运行区间 {formatDateLabel(runSettings.start_date)} ~ {formatDateLabel(runSettings.end_date)}，评分从 {formatDateLabel(result.scoring_start_date || runSettings.start_date)} 开始准备，持有 {runSettings.backtest_period} 日；基线 {result.baseline_ranking_days} 个排名日，候选 {result.candidate_ranking_days} 个排名日。资格未知数按统计日逐窗口累计；三日池的重叠历史会重复计数，资格未知可能改变席位。</p>
      </details>
      {result.baseline_warnings.concat(result.candidate_warnings).map((warning, index) => <p className="scene-layer-caption" key={`${index}-${warning}`}>{warning}</p>)}
      <div className="scene-layer-contrib-table-wrap"><table className="scene-layer-contrib-table">
        <thead><tr><th>期间</th><th>区间状态</th><th>成员池</th><th>组合</th><th>成对收益日</th><th>基线日均</th><th>候选日均</th><th>成对差</th><th>95%区间 / HAC t / 家族p</th><th>样本</th></tr></thead>
        <tbody>{visibleSummaries.map((item) => <tr key={`${item.pool_kind}-${item.top_k}-${item.period}`}>
          <td>{item.period}</td><td>{item.period === "全部" ? "—" : item.quarter_status === "partial" ? "区间截断" : item.quarter_status === "missing_rank_days" ? (item.pool_kind === "three_day" ? "共同日或三日预热不足" : "缺少共同排名日") : item.quarter_status === "range_covered" ? "范围覆盖" : "日历不足"}</td><td>{item.pool_kind === "three_day" ? "三日并集" : "当日"}</td><td>{item.top_k ? `Top ${item.top_k}` : "全市场"}</td><td>{item.paired_return_days} / {item.common_ranking_days}</td>
          <td>{formatBacktestValue(item.baseline_paired_daily_mean, "residual_return")}</td>
          <td>{formatBacktestValue(item.candidate_paired_daily_mean, "residual_return")}</td>
          <td>{formatBacktestValue(item.paired_daily_difference, "residual_return")}</td>
          <td>{formatNumber(item.statistics?.lower)} ~ {formatNumber(item.statistics?.upper)} / {formatNumber(item.statistics?.hac_t)} / {formatNumber(item.statistics?.family_adjusted_p)}</td>
          <td><button type="button" disabled={!result.membership_saved || !result.archive_run_id} onClick={() => setSelectedSamples(item)}>查看成员</button></td>
        </tr>)}</tbody>
      </table></div>
      {selectedSamples && result.archive_run_id && result.membership_saved ? <RankComparisonSamples
        key={`${result.archive_run_id}-${selectedSamples.pool_kind}-${selectedSamples.top_k}-${selectedSamples.period}`}
        sourcePath={settings.source_path} runId={result.archive_run_id} startDate={runSettings.start_date} endDate={runSettings.end_date}
        summary={selectedSamples} onClose={() => setSelectedSamples(null)} /> : null}
      <details><summary>固定基线成员内的排序诊断</summary>
        <p className="scene-layer-caption">固定基线前排或全市场成员，在双方分数与标签共同有效的股票中逐日计算 Spearman IC、ICIR 和最高/最低 20% 残差差；不足 5 只不计算分层差，常数分数不计算 IC。ICIR 使用成对有效日的样本标准差。</p>
        <div className="scene-layer-contrib-table-wrap"><table className="scene-layer-contrib-table">
          <thead><tr><th>期间</th><th>成员池</th><th>固定基线池</th><th>共同样本</th><th>IC日</th><th>基线 / 候选 IC</th><th>IC差</th><th>基线 / 候选 ICIR</th><th>ICIR差</th><th>分层日</th><th>基线 / 候选分层差</th><th>分层差变化</th></tr></thead>
          <tbody>{visibleSummaries.map((item) => <tr key={`${item.pool_kind}-${item.top_k}-${item.period}`}>
            <td>{item.period}</td><td>{item.pool_kind === "three_day" ? "三日并集" : "当日"}</td><td>{item.top_k ? `前 ${item.top_k}` : "全市场"}</td><td>{item.common_score_label_members}</td><td>{item.paired_ic_days}</td>
            <td>{formatNumber(item.baseline_ic_mean)} / {formatNumber(item.candidate_ic_mean)}</td><td>{formatNumber(item.ic_difference)}</td>
            <td>{formatNumber(item.baseline_icir)} / {formatNumber(item.candidate_icir)}</td><td>{formatNumber(item.icir_difference ?? (item.baseline_icir != null && item.candidate_icir != null ? item.candidate_icir - item.baseline_icir : null))}</td><td>{item.paired_spread_days}</td>
            <td>{formatBacktestValue(item.baseline_spread_mean, "residual_return")} / {formatBacktestValue(item.candidate_spread_mean, "residual_return")}</td>
            <td>{formatBacktestValue(item.spread_difference, "residual_return")}</td>
          </tr>)}</tbody>
        </table></div>
      </details>
      <details><summary>覆盖、成员变化及净贡献</summary>
        <p className="scene-layer-caption">新增和移除的数量按日期累计，并非去重股票数；收益是可观察成员的股票日均值，缺标签不计入均值。资格未知按整个评分范围统计。</p>
        <div className="scene-layer-contrib-table-wrap"><table className="scene-layer-contrib-table">
          <thead><tr><th>期间</th><th>成员池</th><th>组合</th><th>基线当日分数 / 标签 / 成员</th><th>候选当日分数 / 标签 / 成员</th><th>新增 / 移除</th><th>新增 / 移除标签</th><th>新增收益</th><th>移除收益</th><th>净贡献 / 可计算日</th><th>资格未知</th></tr></thead>
          <tbody>{visibleSummaries.map((item) => <tr key={`${item.pool_kind}-${item.top_k}-${item.period}`}>
            <td>{item.period}</td><td>{item.pool_kind === "three_day" ? "三日并集" : "当日"}</td><td>{item.top_k ? `Top ${item.top_k}` : "全市场"}</td><td>{item.baseline_scored_members} / {item.baseline_labels} / {item.baseline_members}</td><td>{item.candidate_scored_members} / {item.candidate_labels} / {item.candidate_members}</td>
            <td>{item.added} / {item.removed}</td><td>{item.added_labels} / {item.removed_labels}</td>
            <td>{formatBacktestValue(item.added_return_mean, "residual_return")}</td><td>{formatBacktestValue(item.removed_return_mean, "residual_return")}</td>
            <td>{formatBacktestValue(item.net_contribution_mean, "residual_return")} / {item.net_contribution_days}</td><td>{item.unknown_eligibility}</td>
          </tr>)}</tbody>
        </table></div>
      </details>
      <p className="scene-layer-caption">标签版本：{result.label_algorithm || "旧结果未记录"}；排名版本：{result.ranking_algorithm || "旧结果未记录"}。</p>
      {result.input_digests?.length ? <details><summary>查看输入文件摘要</summary><ul>{result.input_digests.map((item) => <li key={item.file}>{item.file}：{item.sha256 ? `${item.bytes} 字节 · ${item.sha256}` : "不存在"}</li>)}</ul></details> : null}
      <details><summary>查看本次配置快照</summary><h4>基线</h4><pre style={{ whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}>{result.baseline_config}</pre><h4>候选</h4><pre style={{ whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}>{result.candidate_config}</pre></details>
    </> : null}
  </section>;
}
