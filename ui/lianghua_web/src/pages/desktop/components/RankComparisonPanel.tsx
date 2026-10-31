import "../css/SceneLayerBacktestPage.css";
import "../css/ResearchWorkbench.css";
import { Link } from "react-router-dom";
import { ResearchExportButton } from "./ResearchExportButton";
import { ResearchImportButton } from "./ResearchImportButton";
import { useEffect, useRef, useState, type ReactNode } from "react";
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
} from "../../../apis/strategyTrigger";
import { ResearchGroupingFields } from "./ResearchGroupingFields";
import { ResearchStatisticsFields } from "./ResearchStatisticsFields";
import StrategyManagePage from "../StrategyManagePage";
import { ResearchHistoryComparison } from "./ResearchHistoryComparison";
import { ResearchRunProgress } from "./ResearchRunProgress";
import { type ResearchNotes } from "./ResearchNotesFields";
import { formatBacktestValue, formatDateLabel, formatNumber } from "../../../shared/backtestFormat";

type RequestSettings = Omit<RankComparisonRequest, "candidate_config">;

export function RankComparisonPanel({ settings, disabled, onRunningChange, onRestoreSettings, active = true, evaluationFields }: {
  settings: RequestSettings;
  active?: boolean;
  evaluationFields?: ReactNode;
  disabled: boolean;
  onRunningChange: (running: boolean) => void;
  onRestoreSettings?: (settings: RequestSettings) => void;
}) {
  const [candidate, setCandidate] = useState("");
  const [loadingBaseline, setLoadingBaseline] = useState(false);
  const [feedbackPool, setFeedbackPool] = useState("daily-100");
  const feedbackRef = useRef<HTMLDivElement>(null);
  const [grouping, setGrouping] = useState<ResearchGrouping>({market_cap_boundaries: [], market_metric: "", market_boundaries: []});
  const [statistics, setStatistics] = useState<ResearchStatisticsSettings>({block_length: 10, resamples: 500, seed: 0});
  const [slice, setSlice] = useState("all::全部");
  const [editing, setEditing] = useState(false);
  const [editorOpen, setEditorOpen] = useState(true);
  const [editorRevision, setEditorRevision] = useState(0);
  const initializedSource = useRef("");
  const [verifyInputs, setVerifyInputs] = useState(false);
  const [retainMembership, setRetainMembership] = useState(false);
  const [result, setResult] = useState<RankComparisonResult | null>(null);
  const [period, setPeriod] = useState("全部");
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

  useEffect(() => {
    if (!active || !settings.source_path || initializedSource.current === settings.source_path) return;
    let cancelled = false;
    void readRankComparisonBaseline(settings.source_path).then((configuration) => {
      if (!cancelled) { initializedSource.current = settings.source_path; setCandidate(configuration); setEditing(true); setEditorRevision((value) => value + 1); }
    }).catch((failure) => {if (!cancelled) setError(`读取策略配置失败：${String(failure)}`);});
    return () => {cancelled = true;};
  }, [active, settings.source_path]);

  async function loadBaseline() {
    if (loadingBaseline || loading || archiveBusy || disabled) return;
    setLoadingBaseline(true);
    try {
      setCandidate(await readRankComparisonBaseline(settings.source_path));
      setEditing(true);
      setEditorRevision((value) => value + 1);
      setError("");
    } catch (failure) { setError(`读取策略配置失败：${String(failure)}`); }
    finally { setLoadingBaseline(false); }
  }

  async function run() {
    if (loading || loadingBaseline || archiveBusy || disabled) return;
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
    try {
      const response = await runRankComparison({ ...settings, retain_membership: retainMembership, grouping, statistics, verify_inputs: verifyInputs, task_id: activeTask.current, research_question: "", research_notes: notes, parent_run_id: archiveParent, candidate_config: candidate });
      if (activeSource.current === settings.source_path) {
        setResult(response);
        setQuestion("");
        requestAnimationFrame(() => feedbackRef.current?.scrollIntoView({behavior: "smooth", block: "start"}));
        setRunSettings({ ...settings, retain_membership: retainMembership, grouping, statistics, verify_inputs: verifyInputs, task_id: activeTask.current, research_question: question, research_notes: notes, parent_run_id: archiveParent });
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
      const header = await saveResearchArchive(settings.source_path,archiveParent,question,{settings: {...settings, retain_membership: retainMembership, candidate_config: candidate, grouping, statistics, verify_inputs: verifyInputs}, notes},{},"rank_comparison",null,"draft");
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
      setRetainMembership(Boolean(config.settings.retain_membership));
      setEditorRevision((value) => value + 1);
      if (saved.header.status && saved.header.status !== "completed") {
        setCandidate(config.settings.candidate_config ?? "");
        setEditing(Boolean(config.settings.candidate_config));
        setResult(null);
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
      if (result?.archive_run_id === runId) {setResult(null); setRunSettings(null);}
      setArchiveMessage("排名对照存档已删除。");
    } catch (failure) { setArchiveMessage(`删除存档失败：${String(failure)}`); }
    finally { setArchiveBusy(false); }
  }

  const visibleSummaries = result?.summaries.filter((item) => `${item.dimension || "all"}::${item.bucket || "全部"}` === slice) ?? [];
  const sliceOptions = Array.from(new Set(result?.summaries.map((item) => `${item.dimension || "all"}::${item.bucket || "全部"}`) ?? []));
  const selectedPeriod = visibleSummaries.some((item) => item.period === period) ? period : "全部";
  const feedbackOptions = visibleSummaries.filter((item) => item.period === selectedPeriod);
  const feedback = feedbackOptions.find((item) => `${item.pool_kind}-${item.top_k}` === feedbackPool) ?? feedbackOptions[0];
  return <section className="scene-layer-card research-workbench">
    <h2 className="scene-layer-title">排名对照</h2>
    <ResearchHistoryComparison kind="rank_comparison" sourcePath={settings.source_path} archives={archives} disabled={loading || archiveBusy} onOpen={(runId) => void openArchive(runId)} onUpdated={(header) => {setArchives((items) => items.map((item) => item.run_id === header.run_id ? header : item)); if (result?.archive_run_id === header.run_id) setQuestion(header.question);}} result={result} settings={runSettings} summary={feedback ?? null} />
    {archiveMessage ? <p className="scene-layer-caption" role="status">{archiveMessage}</p> : null}

    <div className="research-toolbar research-file-actions">
    <ResearchImportButton sourcePath={settings.source_path} disabled={loading} onImported={() => {void listResearchArchives(settings.source_path).then((items) => setArchives(items.filter((item) => item.kind === "rank_comparison"))).catch((failure) => setArchiveMessage(String(failure)));}} />
      <button type="button" disabled={archiveBusy || loading || !settings.source_path} onClick={() => void saveDraft()}>保存候选草稿</button>
      <button type="button" className="scene-layer-secondary-btn" disabled={!result || !runSettings || archiveBusy || loading} onClick={() => void saveArchive()}>另存结果</button>
    </div>
    <details className="research-advanced"><summary>更多参数</summary>
    {evaluationFields}
    <div className="research-settings-grid"><ResearchGroupingFields value={grouping} onChange={setGrouping} disabled={loading} />
    <ResearchStatisticsFields value={statistics} onChange={setStatistics} disabled={loading} /></div>
    <label className="research-checkbox" title="运行前后两次读取完整数据库。文件变化时丢弃结果，不代表一致快照。"><input type="checkbox" checked={verifyInputs} disabled={loading} onChange={(event) => setVerifyInputs(event.target.checked)} />校验输入内容</label>
    <label className="research-checkbox"><input type="checkbox" checked={retainMembership} disabled={loading} onChange={(event) => setRetainMembership(event.target.checked)} />保存交易模拟候选池（增加存储）</label></details>
    <details className="research-editor" open={editorOpen} onToggle={(event) => setEditorOpen(event.currentTarget.open)}>
      <summary>编辑策略</summary>
      {editing && active && editorOpen ? <div inert={loading || disabled || archiveBusy} className="research-strategy-editor">
        <StrategyManagePage key={editorRevision} backtest={{configuration: candidate, sourcePath: settings.source_path, onChange: setCandidate}} />
      </div> : null}
    </details>
    <div className="scene-layer-actions research-run-actions">
      <button type="button" className="scene-layer-primary-btn" disabled={disabled || loading || loadingBaseline || archiveBusy || !candidate.trim()} onClick={() => void run()}>{loading ? "计算中…" : "运行排名对照"}</button>
      <button type="button" className="scene-layer-secondary-btn" disabled={disabled || loading || archiveBusy || loadingBaseline} onClick={() => void loadBaseline()}>{loadingBaseline ? "读取中…" : "恢复当前策略"}</button>
    </div>
    {loading ? <ResearchRunProgress phase={phase} cancelling={cancelling} onCancel={() => void cancel()} /> : null}
    {error ? <p role="alert">{error}</p> : null}
    <div ref={feedbackRef} className="research-feedback-target">
    {result?.archive_run_id ? <button type="button" className="research-history-delete" disabled={loading || archiveBusy} onClick={() => void removeArchive(result.archive_run_id!)}>删除当前记录</button> : null}
    {result && runSettings ? <>
      <h3>排名对照结果</h3>
      <div className="research-toolbar">
        <label className="research-field">分组范围<select value={slice} onChange={(event) => setSlice(event.target.value)}>{sliceOptions.map((value) => <option key={value} value={value}>{value === "all::全部" ? "全样本" : value.replace("::", " / ")}</option>)}</select></label>
        <label className="research-field">统计期间<select value={selectedPeriod} onChange={(event) => setPeriod(event.target.value)}>{Array.from(new Set(visibleSummaries.map((item) => item.period))).map((value) => <option key={value} value={value}>{value}</option>)}</select></label>
        {feedback ? <label className="research-field">投资组合<select value={`${feedback.pool_kind}-${feedback.top_k}`} onChange={(event) => setFeedbackPool(event.target.value)}>{feedbackOptions.map((item) => <option key={`${item.pool_kind}-${item.top_k}`} value={`${item.pool_kind}-${item.top_k}`}>{item.pool_kind === "three_day" ? "三日入选并集" : "当日入选"} · {item.top_k ? `Top ${item.top_k}` : "全市场"}</option>)}</select></label> : null}
      </div>
      <p className="research-meta">{formatDateLabel(runSettings.start_date)} ~ {formatDateLabel(runSettings.end_date)} · 持有 {runSettings.backtest_period} 个交易日 · 基线是当前正式策略，候选是本次修改的策略。</p>
      {result.baseline_warnings.concat(result.candidate_warnings).map((warning, index) => <p className="scene-layer-caption" key={`${index}-${warning}`}>{warning}</p>)}
      {result.archive_run_id ? <ResearchExportButton sourcePath={settings.source_path} runId={result.archive_run_id} disabled={loading} /> : null}
      {result.archive_run_id && result.membership_saved ? <Link to={`/backtest/strategy-paper-validation?researchRun=${encodeURIComponent(result.archive_run_id)}`} className="research-button">验证交易</Link> : null}
      {feedback ? <>
        <div className="research-feedback-grid">
          <div><span>基线平均残差收益</span><strong>{formatBacktestValue(feedback.baseline_paired_daily_mean, "residual_return")}</strong><small>正式策略入选组合的日均残差收益。</small></div>
          <div><span>候选平均残差收益</span><strong>{formatBacktestValue(feedback.candidate_paired_daily_mean, "residual_return")}</strong><small>修改后入选组合的日均残差收益。</small></div>
          <div><span>配对收益差（Paired Difference）</span><strong>{formatBacktestValue(feedback.paired_daily_difference, "residual_return")}</strong><small>同一有效日期上候选减基线，正值表示候选更优，单位为百分点。</small></div>
          <div><span>95% 置信区间（CI）</span><strong>{formatNumber(feedback.statistics?.lower)} ~ {formatNumber(feedback.statistics?.upper)}</strong><small>日期块重采样的描述性区间；包含零时，差异方向尚不明确。</small></div>
          <div><span>配对有效日</span><strong>{feedback.paired_return_days} / {feedback.common_ranking_days}</strong><small>共同排名日中，双方组合收益均可观察的日期数。</small></div>
        </div>
        <p className="research-meta">收益按双方共同有效信号日等权统计，反映所选持有期的残差收益；并非账户累计收益。{feedback.quarter_status === "partial" ? "所选季度被回测区间截断。" : ""}</p>
        <h4>排序能力</h4>
        <p className="research-meta">固定基线成员池后比较排序能力。IC 为分数与后验收益的秩相关；ICIR 为日度 IC 均值／标准差；分层收益差为最高分组减最低分组。</p>
        <div className="expression-backtest-table-wrap"><table className="expression-backtest-table"><thead><tr><th>指标</th><th>基线</th><th>候选</th><th>差值</th></tr></thead><tbody>
          <tr><th>信息系数（Rank IC）</th><td>{formatNumber(feedback.baseline_ic_mean)}</td><td>{formatNumber(feedback.candidate_ic_mean)}</td><td>{formatNumber(feedback.ic_difference)}</td></tr>
          <tr><th>信息系数比率（ICIR）</th><td>{formatNumber(feedback.baseline_icir)}</td><td>{formatNumber(feedback.candidate_icir)}</td><td>{formatNumber(feedback.icir_difference ?? (feedback.baseline_icir != null && feedback.candidate_icir != null ? feedback.candidate_icir - feedback.baseline_icir : null))}</td></tr>
          <tr><th>分层收益差（Spread）</th><td>{formatBacktestValue(feedback.baseline_spread_mean, "residual_return")}</td><td>{formatBacktestValue(feedback.candidate_spread_mean, "residual_return")}</td><td>{formatBacktestValue(feedback.spread_difference, "residual_return")}</td></tr>
        </tbody></table></div>
        <h4>组合换入换出与数据覆盖</h4>
        <div className="research-feedback-grid">
          <div><span>换入／换出股票日</span><strong>{feedback.added.toLocaleString()} / {feedback.removed.toLocaleString()}</strong><small>相对基线的成员变化，按日期累计，同一股票可重复计数。</small></div>
          <div><span>换入／换出平均残差</span><strong>{formatBacktestValue(feedback.added_return_mean, "residual_return")} / {formatBacktestValue(feedback.removed_return_mean, "residual_return")}</strong><small>仅统计具备有效收益标签的换入、换出成员。</small></div>
          <div><span>固定席位净贡献</span><strong>{formatBacktestValue(feedback.net_contribution_mean, "residual_return")}</strong><small>相同席位分母下换入收益减换出收益；有效 {feedback.net_contribution_days} 日。</small></div>
        </div>
        <p className="research-meta">收益标签覆盖：基线 {feedback.baseline_labels.toLocaleString()} / {feedback.baseline_members.toLocaleString()} 股票日，候选 {feedback.candidate_labels.toLocaleString()} / {feedback.candidate_members.toLocaleString()} 股票日；资格未知 {feedback.unknown_eligibility.toLocaleString()}。缺失收益不补零。</p>
      </> : <p className="research-meta">所选范围没有可比较的统计结果。</p>}
    </> : null}
    </div>
  </section>;
}
