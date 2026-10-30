import "./css/ResearchWorkbench.css";
import { useEffect, useMemo, useRef, useState } from "react";
import { listen } from "@tauri-apps/api/event";
import { getIndicatorManagePage } from "../../apis/dataDownload";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import { getStrategyManagePage, type StrategyManageRuleItem } from "../../apis/strategyManage";
import {
  deleteResearchArchive,
  type ResearchGrouping,
  type ResearchStatisticsSettings,
  type ResearchEvidenceQuery,
  listResearchArchives,
  readResearchArchive,
  saveResearchArchive,
  researchRunStatusLabel,
  getValidationCoreRuleOptions,
  getRuleLayerBacktestDefaults,
  runRuleExpressionValidation,
  cancelRuleExpressionValidation,
  type ResearchArchiveHeader,
  type RuleExpressionValidationQuery,
  type RuleExpressionValidationData,
  type ValidationCoreRuleOption,
} from "../../apis/strategyTrigger";
import {
  buildBoardFilterOptions,
  STOCK_PICK_BOARD_OPTIONS,
} from "../../shared/stockPickShared";
import {
  compactDateToInput,
  normalizeDateInput,
  parseOptionalNumberInput,
} from "../../shared/backtestFormat";
import {
  readStoredBacktestCommonParams,
  writeStoredBacktestCommonParams,
} from "../../shared/backtestCommonParams";
import {
  compactRuleExpressionValidationData,
  inferUnknownConfigs,
  resolveValidationScopeWay,
  VALIDATION_DEFAULT_SAMPLE_LIMIT,
  VALIDATION_MAX_SAMPLE_LIMIT,
  type ExpressionValidationContext,
} from "../../shared/expressionValidation";
import { useConceptExclusions } from "../../shared/conceptExclusions";
import { readStoredSourcePath } from "../../shared/storage";
import {
  ExpressionBacktestForm,
  type ExpressionBacktestFormValues,
} from "./components/expressionBacktest/ExpressionBacktestForm";
import { ResearchExportButton } from "./components/ResearchExportButton";
import { ResearchImportButton } from "./components/ResearchImportButton";
import { ResearchStatisticsFields } from "./components/ResearchStatisticsFields";
import { ResearchGroupingFields } from "./components/ResearchGroupingFields";
import { ResearchEvidencePanel } from "./components/ResearchEvidencePanel";
import { RankComparisonPanel } from "./components/RankComparisonPanel";
import { ExpressionCoreSummary } from "./components/expressionBacktest/ExpressionCoreSummary";
import { ResearchNotesFields, type ResearchNotes } from "./components/ResearchNotesFields";
import { ExpressionQuarterSummary } from "./components/expressionBacktest/ExpressionQuarterSummary";
import { ExpressionDailyDetail } from "./components/expressionBacktest/ExpressionDailyDetail";
import { ExpressionIncremental } from "./components/expressionBacktest/ExpressionIncremental";
import { ExpressionLayerResult } from "./components/expressionBacktest/ExpressionLayerResult";
import { ExpressionParameterRobustness } from "./components/expressionBacktest/ExpressionParameterRobustness";
import { ExpressionSamplesCheck } from "./components/expressionBacktest/ExpressionSamplesCheck";
import { ExpressionSimilarity } from "./components/expressionBacktest/ExpressionSimilarity";
import { ExpressionWalkForward } from "./components/expressionBacktest/ExpressionWalkForward";
import "./css/ExpressionBacktestPage.css";

export default function ExpressionBacktestPage() {
  const { excludeStBoard } = useConceptExclusions();
  const storedCommonParams = useMemo(() => readStoredBacktestCommonParams(), []);
  const [sourcePath, setSourcePath] = useState(() => readStoredSourcePath());
  const [strategyOptions, setStrategyOptions] = useState<StrategyManageRuleItem[]>([]);
  const [indicatorNames, setIndicatorNames] = useState<string[]>([]);
  const [coreRuleOptions, setCoreRuleOptions] = useState<ValidationCoreRuleOption[]>([]);
  const [form, setForm] = useState<ExpressionBacktestFormValues>(() => ({
    importRuleName: "",
    direction: "positive",
    scopeWay: "LAST",
    consecThresholdText: "2",
    scopeWindowsText: "1",
    holdingPeriodText: storedCommonParams.backtestPeriod,
    expression: "",
    startDateInput: storedCommonParams.startDateInput,
    endDateInput: storedCommonParams.endDateInput,
    boardFilter: storedCommonParams.backtestBoardFilter,
    totalMvMinText: storedCommonParams.totalMvMin,
    totalMvMaxText: storedCommonParams.totalMvMax,
    minListedTradeDaysText: storedCommonParams.minListedTradeDays,
    walkForwardFoldsText: "4",
    sampleLimitText: String(VALIDATION_DEFAULT_SAMPLE_LIMIT),
    minSamplesPerDayText: storedCommonParams.minSamplesPerDay,
    stockAdjType: storedCommonParams.stockAdjType,
    returnMetric: storedCommonParams.returnMetric,
    indexTsCode: storedCommonParams.indexTsCode,
    indexBetaText: storedCommonParams.indexBeta,
    conceptBetaText: storedCommonParams.conceptBeta,
    industryBetaText: storedCommonParams.industryBeta,
    enableUnknown: false,
    unknownConfigs: [],
    coreRuleNames: [],
  }));
  const [initializing, setInitializing] = useState(false);
  const [loading, setLoading] = useState(false);
  const [phase, setPhase] = useState("");
  const [cancelling, setCancelling] = useState(false);
  const activeTask = useRef("");
  const [error, setError] = useState("");
  const [result, setResult] = useState<RuleExpressionValidationData | null>(null);
  const [selectedComboKey, setSelectedComboKey] = useState("");
  const [runForm, setRunForm] = useState<ExpressionBacktestFormValues | null>(null);
  const [runQuery, setRunQuery] = useState<RuleExpressionValidationQuery | null>(null);
  const [archiveParent, setArchiveParent] = useState<string | null>(null);
  const [archives, setArchives] = useState<ResearchArchiveHeader[]>([]);
  const [researchQuestion, setResearchQuestion] = useState("");
  const [researchNotes, setResearchNotes] = useState<ResearchNotes>({ hypothesis: "", scope: "", explorationStart: "", explorationEnd: "", verificationStart: "", verificationEnd: "", historyReviewed: false, sortingEvidence: "", returnEvidence: "", conclusion: "" });
  const [archiveMessage, setArchiveMessage] = useState("");
  const [archiveBusy, setArchiveBusy] = useState(false);
  const [grouping, setGrouping] = useState<ResearchGrouping>({market_cap_boundaries: [], market_metric: "", market_boundaries: []});
  const [statistics, setStatistics] = useState<ResearchStatisticsSettings>({block_length: 10, resamples: 500, seed: 0});
  const [multiPeriod, setMultiPeriod] = useState(true);
  const [exploratoryFilter, setExploratoryFilter] = useState<Partial<ResearchEvidenceQuery> | undefined>();
  const [selectedHorizon, setSelectedHorizon] = useState<number | null>(null);
  const [verifyInputs, setVerifyInputs] = useState(false);
  const [researchTab, setResearchTab] = useState("expression");
  const [rankingRunning, setRankingRunning] = useState(false);

  useEffect(() => {
    let unlisten: (() => void) | undefined;
    let disposed = false;
    void listen<{ task_id: string; phase: string }>("expression-validation-progress", (event) => {
      if (event.payload.task_id === activeTask.current) setPhase(event.payload.phase);
    }).then((stop) => { if (disposed) stop(); else unlisten = stop; });
    return () => { disposed = true; unlisten?.(); };
  }, []);

  const backtestBoardOptions = useMemo(
    () => buildBoardFilterOptions(STOCK_PICK_BOARD_OPTIONS, excludeStBoard),
    [excludeStBoard],
  );
  const displayedResult = selectedHorizon === result?.horizon || selectedHorizon == null ? result : result?.additional_periods?.find((item) => item.horizon === selectedHorizon) ?? result;
  const comboRows = displayedResult?.combo_results ?? [];
  const selectedCombo =
    comboRows.find((item) => item.combo_key === selectedComboKey) ?? comboRows[0] ?? null;
  const hasUnknownCombo = comboRows.some((item) => item.unknown_values.length > 0);
  const selectedRule = strategyOptions.find((item) => item.name === form.importRuleName.trim());
  const displayedForm = runForm ?? form;
  const validationContext: ExpressionValidationContext = {
    sourcePath,
    expression: displayedForm.expression,
    direction: displayedForm.direction,
    importRuleName: displayedForm.importRuleName.trim() || "manual_expression_strategy",
    importRuleExplain: result?.import_rule_explain || `表达式回测：${displayedForm.importRuleName.trim() || "手动表达式"}`,
    sampleLimitPerGroup: result?.sample_limit_per_group ?? (Number(form.sampleLimitText) || 0),
  };

  useEffect(() => {
    if (!sourcePath.trim()) return;
    let cancelled = false;
    void listResearchArchives(sourcePath).then((items) => {
      if (!cancelled) setArchives(items.filter((item) => item.kind === "expression_validation"));
    }).catch((failure) => {
      if (!cancelled) setArchiveMessage(`读取研究存档失败：${String(failure)}`);
    });
    return () => { cancelled = true; };
  }, [sourcePath]);

  useEffect(() => {
    let cancelled = false;
    const init = async () => {
      setInitializing(true);
      try {
        const resolved = await ensureManagedSourcePath();
        if (cancelled) {
          return;
        }
        setSourcePath(resolved);
        try {
          const ruleDefaults = await getRuleLayerBacktestDefaults(resolved);
          if (cancelled) {
            return;
          }
          setForm((current) => ({
            ...current,
            startDateInput: current.startDateInput || compactDateToInput(ruleDefaults.start_date),
            endDateInput: compactDateToInput(ruleDefaults.end_date) || current.endDateInput,
          }));
        } catch (defaultError) {
          if (!cancelled) {
            setError(`读取回测默认日期失败: ${String(defaultError)}`);
          }
        }
        try {
          const managePage = await getStrategyManagePage(resolved);
          if (!cancelled) {
            setStrategyOptions(managePage.rules ?? []);
          }
        } catch (strategyError) {
          if (!cancelled) {
            setError(`读取策略列表失败: ${String(strategyError)}`);
          }
        }
        try {
          const options = await getValidationCoreRuleOptions(resolved);
          if (!cancelled) {
            setCoreRuleOptions(options);
          }
        } catch (coreRuleError) {
          if (!cancelled) {
            setError(`读取核心策略可用性失败: ${String(coreRuleError)}`);
          }
        }
        try {
          const indicatorPage = await getIndicatorManagePage(resolved);
          if (!cancelled) {
            setIndicatorNames(indicatorPage.items.map((item) => item.name));
          }
        } catch (indicatorError) {
          if (!cancelled) {
            setError(`读取指标配置失败: ${String(indicatorError)}`);
          }
        }
      } catch (initError) {
        if (!cancelled) {
          setError(`读取回测默认参数失败: ${String(initError)}`);
        }
      } finally {
        if (!cancelled) {
          setInitializing(false);
        }
      }
    };

    void init();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    const latest = readStoredBacktestCommonParams();
    writeStoredBacktestCommonParams({
      ...latest,
      stockAdjType: form.stockAdjType,
      returnMetric: form.returnMetric,
      indexTsCode: form.indexTsCode,
      indexBeta: form.indexBetaText,
      conceptBeta: form.conceptBetaText,
      industryBeta: form.industryBetaText,
      startDateInput: form.startDateInput,
      endDateInput: form.endDateInput,
      minSamplesPerDay: form.minSamplesPerDayText,
      minListedTradeDays: form.minListedTradeDaysText,
      backtestPeriod: form.holdingPeriodText,
      totalMvMin: form.totalMvMinText,
      totalMvMax: form.totalMvMaxText,
      backtestBoardFilter: form.boardFilter,
    });
  }, [
    form.stockAdjType,
    form.indexTsCode,
    form.indexBetaText,
    form.returnMetric,
    form.conceptBetaText,
    form.industryBetaText,
    form.startDateInput,
    form.endDateInput,
    form.minSamplesPerDayText,
    form.minListedTradeDaysText,
    form.holdingPeriodText,
    form.totalMvMinText,
    form.totalMvMaxText,
    form.boardFilter,
  ]);

  const effectiveBoardFilter = backtestBoardOptions.includes(form.boardFilter) ? form.boardFilter : "全部";

  function updateForm(patch: Partial<ExpressionBacktestFormValues>) {
    setForm((current) => ({ ...current, ...patch }));
  }

  function applyRule(ruleName: string) {
    const matched = strategyOptions.find((item) => item.name === ruleName);
    setResult(null);
    setError("");
    if (!matched) {
      updateForm({ importRuleName: "", unknownConfigs: [] });
      return;
    }

    const parsedScopeWay = resolveValidationScopeWay(matched.scope_way);
    updateForm({
      importRuleName: ruleName,
      expression: matched.when ?? "",
      direction: Number.isFinite(matched.points) && matched.points < 0 ? "negative" : "positive",
      scopeWay: parsedScopeWay.scopeWay,
      consecThresholdText: String(parsedScopeWay.consecThreshold),
      scopeWindowsText: String(Math.max(1, matched.scope_windows ?? 1)),
      unknownConfigs: form.enableUnknown
        ? inferUnknownConfigs(matched.when ?? "", indicatorNames)
        : [],
    });
  }

  async function onRunValidation() {
    const normalizedStart = normalizeDateInput(form.startDateInput);
    const normalizedEnd = normalizeDateInput(form.endDateInput);
    if (!sourcePath.trim()) {
      setError("当前数据目录为空，请先在数据管理页确认目录。");
      return;
    }
    if (!form.indexTsCode.trim()) {
      setError("请选择指数。");
      return;
    }
    if (!normalizedStart || !normalizedEnd) {
      setError("请填写开始和结束日期。");
      return;
    }
    if (normalizedStart > normalizedEnd) {
      setError("开始日期不能晚于结束日期。");
      return;
    }
    const totalMvMin = parseOptionalNumberInput(form.totalMvMinText);
    const totalMvMax = parseOptionalNumberInput(form.totalMvMaxText);
    if (form.totalMvMinText.trim() && totalMvMin === undefined) {
      setError("总市值最小值必须是数字。");
      return;
    }
    if (form.totalMvMaxText.trim() && totalMvMax === undefined) {
      setError("总市值最大值必须是数字。");
      return;
    }
    if (totalMvMin !== undefined && totalMvMax !== undefined && totalMvMin > totalMvMax) {
      setError("总市值最小值不能大于最大值。");
      return;
    }
    if (!form.expression.trim()) {
      setError("表达式不能为空。");
      return;
    }

    const scopeWindows = Number(form.scopeWindowsText);
    if (!Number.isFinite(scopeWindows) || !Number.isInteger(scopeWindows) || scopeWindows < 1) {
      setError("scope_windows 必须是 >= 1 的整数。");
      return;
    }
    let normalizedScopeWay: string = form.scopeWay;
    if (form.scopeWay === "CONSEC") {
      const consecThreshold = Number(form.consecThresholdText);
      if (
        !Number.isFinite(consecThreshold) ||
        !Number.isInteger(consecThreshold) ||
        consecThreshold < 1
      ) {
        setError("CONSEC 阈值必须是 >= 1 的整数。");
        return;
      }
      if (scopeWindows < consecThreshold) {
        setError("scope_windows 不能小于 CONSEC 阈值。");
        return;
      }
      normalizedScopeWay = `CONSEC>=${consecThreshold}`;
    }

    const unknownConfigs = form.enableUnknown
      ? form.unknownConfigs
          .map((item) => ({
            name: item.name.trim(),
            start: Number(item.start.trim()),
            end: Number(item.end.trim()),
            step: Number(item.step.trim()),
          }))
          .filter((item) => item.name.length > 0)
      : [];
    if (form.enableUnknown && unknownConfigs.length === 0) {
      setError("启用参数研究后，至少需要一个参数配置。");
      return;
    }
    for (const item of unknownConfigs) {
      if (!Number.isFinite(item.start) || !Number.isFinite(item.end) || !Number.isFinite(item.step)) {
        setError(`参数 ${item.name} 存在非法数值。`);
        return;
      }
      if (item.step <= 0) {
        setError(`参数 ${item.name} 的步长必须 > 0。`);
        return;
      }
      if (item.end < item.start) {
        setError(`参数 ${item.name} 的结束值不能小于起始值。`);
        return;
      }
    }

    const sampleLimitPerGroupRaw = Number(form.sampleLimitText);
    if (
      !Number.isFinite(sampleLimitPerGroupRaw) ||
      !Number.isInteger(sampleLimitPerGroupRaw) ||
      sampleLimitPerGroupRaw < 1
    ) {
      setError("样本展示上限必须是 >= 1 的整数。");
      return;
    }
    const sampleLimitPerGroup = Math.min(VALIDATION_MAX_SAMPLE_LIMIT, sampleLimitPerGroupRaw);

    const walkForwardFolds = Number(form.walkForwardFoldsText);
    if (
      !Number.isFinite(walkForwardFolds) ||
      !Number.isInteger(walkForwardFolds) ||
      walkForwardFolds < 1 ||
      walkForwardFolds > 8
    ) {
      setError("Walk-forward folds 必须是 1 到 8 的整数。");
      return;
    }
    if (form.coreRuleNames.length > 8) {
      setError("核心策略最多选择 8 个，请减少 predictors 后重试。");
      return;
    }

    const resolvedRuleName = form.importRuleName.trim();
    const manualStrategyName = resolvedRuleName || "manual_expression_strategy";
    const normalizedManualPoints = form.direction === "negative" ? -1 : 1;

    setResult(null);
    setError("");
    setLoading(true);
    setCancelling(false);
    setPhase("等待仓库可用");
    activeTask.current = `${Date.now()}-${Math.random().toString(36).slice(2)}`;
    let runQuerySnapshot: RuleExpressionValidationQuery | null = null;
    try {
      const query: RuleExpressionValidationQuery = {
        taskId: activeTask.current,
        additionalPeriods: multiPeriod ? [3, 5, 10] : [],
        researchGrouping: grouping,
        researchStatistics: statistics,
        verifyInputs,
        sourcePath,
        importRuleName: resolvedRuleName,
        manualStrategy: {
          name: manualStrategyName,
          sceneName: selectedRule?.scene_name,
          stage: selectedRule?.stage,
          scopeWay: normalizedScopeWay,
          scopeWindows,
          when: form.expression.trim(),
          points: normalizedManualPoints,
          explain: selectedRule?.explain?.trim() || `手动表达式验证：${manualStrategyName}`,
        },
        when: form.expression.trim(),
        scopeWay: normalizedScopeWay,
        scopeWindows,
        stockAdjType: form.stockAdjType.trim() || "qfq",
        indexTsCode: form.indexTsCode.trim(),
        indexBeta: Number(form.indexBetaText),
        returnMetric: form.returnMetric,
        conceptBeta: Number(form.conceptBetaText),
        industryBeta: Number(form.industryBetaText),
        startDate: normalizedStart,
        endDate: normalizedEnd,
        minSamplesPerRuleDay: Math.max(1, Number(form.minSamplesPerDayText) || 1),
        minListedTradeDays: Math.max(0, Number(form.minListedTradeDaysText) || 0),
        backtestPeriod: Math.max(1, Number(form.holdingPeriodText) || 1),
        unknownConfigs,
        sampleLimitPerGroup,
        board: effectiveBoardFilter === "全部" ? undefined : effectiveBoardFilter,
        excludeStBoard: excludeStBoard || undefined,
        walkForwardFolds,
        coreRuleNames: form.coreRuleNames,
        totalMvMin,
        totalMvMax,
      };
      runQuerySnapshot = query;
      const data = await runRuleExpressionValidation({...query, researchQuestion, researchConfiguration: {form: {...form, boardFilter: effectiveBoardFilter}, query, notes: researchNotes}, parentRunId: archiveParent});
      activeTask.current = "";
      setPhase("正在保存结果");
      setRunForm({...form, boardFilter: effectiveBoardFilter});
      setSelectedComboKey(data.combo_results[0]?.combo_key ?? "");
      setRunQuery(query);
      const compacted = compactRuleExpressionValidationData(data);
      setResult(compacted);
      updateForm({ sampleLimitText: String(compacted.sample_limit_per_group) });
      setExploratoryFilter(undefined);
      setSelectedHorizon(data.horizon ?? query.backtestPeriod ?? null);
      if (data.archive_run_id) {
        setArchiveParent(data.archive_run_id);
        setArchiveMessage("完整结果已保存。");
      }
      try { setArchives((await listResearchArchives(sourcePath)).filter((item) => item.kind === "expression_validation")); }
      catch (failure) { setArchiveMessage(`读取存档列表失败：${String(failure)}`); }
    } catch (runError) {
      activeTask.current = "";
      setPhase("正在保存运行记录");
      setResult(null);
      setError(`执行表达式回测失败: ${String(runError)}`);
      if (runQuerySnapshot) {
        try {setArchives((await listResearchArchives(sourcePath)).filter((item) => item.kind === "expression_validation"));}
        catch (failure) {setArchiveMessage(`读取运行记录失败：${String(failure)}`);}
      }
    } finally {
      activeTask.current = "";
      setPhase("");
      setCancelling(false);
      setLoading(false);
    }
  }

  async function saveDraft() {
    if (archiveBusy || loading || rankingRunning || !sourcePath) return;
    setArchiveBusy(true);
    try {
      const query = {sourcePath, importRuleName: form.importRuleName, indexTsCode: form.indexTsCode, startDate: normalizeDateInput(form.startDateInput) ?? "", endDate: normalizeDateInput(form.endDateInput) ?? "", researchGrouping: grouping, researchStatistics: statistics, additionalPeriods: multiPeriod ? [3,5,10] : [], verifyInputs};
      const header = await saveResearchArchive(sourcePath, archiveParent, researchQuestion, {form, query, notes: researchNotes}, {}, "expression_validation", null, "draft");
      setArchiveParent(header.run_id);
      setArchives((await listResearchArchives(sourcePath)).filter((run) => run.kind === "expression_validation"));
      setArchiveMessage("草稿已保存。");
    } catch (failure) {setArchiveMessage(`保存草稿失败：${String(failure)}`);}
    finally {setArchiveBusy(false);}
  }

  async function saveArchive() {
    if (!result || !runForm || !runQuery || archiveBusy) return;
    setArchiveBusy(true);
    setArchiveMessage("");
    try {
      const saved = await saveResearchArchive(sourcePath, archiveParent, researchQuestion,
        { form: runForm, query: runQuery, notes: researchNotes }, result, "expression_validation", result.membership_saved ? result.archive_run_id ?? null : null);
      setArchiveParent(saved.run_id);
      setResult({...result, archive_run_id: saved.run_id});
      setArchives((await listResearchArchives(sourcePath)).filter((item) => item.kind === "expression_validation"));
      setArchiveMessage("结论已另存。");
    } catch (failure) { setArchiveMessage(`保存研究存档失败：${String(failure)}`); }
    finally { setArchiveBusy(false); }
  }

  async function openArchive(runId: string) {
    if (archiveBusy) return;
    setArchiveBusy(true);
    setArchiveMessage("");
    try {
      const saved = await readResearchArchive<RuleExpressionValidationData & { error?: string }>(sourcePath, runId);
      const config = saved.configuration as { form?: ExpressionBacktestFormValues; query?: RuleExpressionValidationQuery; notes?: ResearchNotes; exploratory_filter?: Partial<ResearchEvidenceQuery> };
      if (!config?.form || !config.query) {
        throw new Error("存档缺少表达式回测配置或结果");
      }
      setForm(config.form);
      setGrouping(config.query.researchGrouping ?? {market_cap_boundaries: [], market_metric: "", market_boundaries: []});
      setStatistics(config.query.researchStatistics ?? {block_length: 10, resamples: 500, seed: 0});
      setMultiPeriod(Boolean(config.query.additionalPeriods?.length));
      setVerifyInputs(Boolean(config.query.verifyInputs));
      setExploratoryFilter(config.exploratory_filter);
      setSelectedHorizon(config.exploratory_filter?.horizon ?? null);
      setSelectedComboKey(config.exploratory_filter?.combo_key ?? saved.result.combo_results?.[0]?.combo_key ?? "");
      setArchiveParent(saved.header.run_id);
      setResearchQuestion(saved.header.question);
      setResearchNotes({ hypothesis: "", scope: "", explorationStart: "", explorationEnd: "", verificationStart: "", verificationEnd: "", historyReviewed: false, sortingEvidence: "", returnEvidence: "", conclusion: "", ...config.notes });
      if (saved.header.status && saved.header.status !== "completed") {
        setRunForm(null);
        setRunQuery(null);
        setResult(null);
        setArchiveMessage(`${researchRunStatusLabel(saved.header.status)}：${saved.result.error || "配置已恢复"}。可修改后创建新运行。`);
        return;
      }
      if (!Array.isArray(saved.result?.combo_results)) throw new Error("存档缺少表达式回测结果");
      setRunForm(config.form);
      setRunQuery(config.query);
      setResult({...saved.result, archive_run_id: saved.header.run_id, membership_saved: Boolean(saved.header.membership_saved)});
      setArchiveMessage(`正在查看 ${saved.header.run_id}。图表读取当前行情和配置，可能与存档运行时不同。`);
    } catch (failure) { setArchiveMessage(`读取研究存档失败：${String(failure)}`); }
    finally { setArchiveBusy(false); }
  }

  async function removeArchive(runId: string) {
    if (archiveBusy || !window.confirm("删除这份本机研究存档？")) return;
    setArchiveBusy(true);
    try {
      await deleteResearchArchive(sourcePath, runId);
      setArchives((await listResearchArchives(sourcePath)).filter((item) => item.kind === "expression_validation"));
      if (archiveParent === runId) setArchiveParent(null);
      setArchiveMessage("研究存档已删除。");
    } catch (failure) { setArchiveMessage(`删除研究存档失败：${String(failure)}`); }
    finally { setArchiveBusy(false); }
  }

  return (
    <div className="expression-backtest-page research-workbench">
      <header className="expression-backtest-header">
        <h2 className="expression-backtest-title">研究回测</h2>
        <p className="expression-backtest-caption">研究信号表现、适用范围与真实排名变化。</p>
      </header>

      <div className="research-mode-tabs" aria-label="研究模式">
        <button type="button" aria-pressed={researchTab === "expression"} disabled={loading || rankingRunning} onClick={() => setResearchTab("expression")}>策略研究</button>
        <button type="button" aria-pressed={researchTab === "ranking"} disabled={loading || rankingRunning} onClick={() => setResearchTab("ranking")}>排名对照</button>
      </div>
      <details className="expression-backtest-section research-archive" hidden={researchTab !== "expression"}>
        <summary>实验记录与存档{archives.length ? ` · ${archives.length}` : ""}</summary>
        <ResearchImportButton sourcePath={sourcePath} disabled={loading || rankingRunning} onImported={() => {void listResearchArchives(sourcePath).then((items) => setArchives(items.filter((item) => item.kind === "expression_validation"))).catch((failure) => setArchiveMessage(String(failure)));}} />

        <div className="research-toolbar"><label className="research-field">研究问题<input placeholder="选填，便于查找这次实验" value={researchQuestion} onChange={(event) => setResearchQuestion(event.target.value)} /></label>{" "}
        <button type="button" disabled={!sourcePath || archiveBusy || loading || rankingRunning} onClick={() => void saveDraft()}>保存草稿</button>{" "}
        <button type="button" disabled={!result || !runQuery || archiveBusy || loading} onClick={() => void saveArchive()}>另存结论</button></div>
        <ResearchNotesFields value={researchNotes} onChange={setResearchNotes} />
        {archiveParent ? <p className="expression-backtest-caption">下次保存的来源：{archiveParent}</p> : null}
        {archiveMessage ? <p className="expression-backtest-caption" role="status">{archiveMessage}</p> : null}
        {archives.length ? <details><summary>查看已保存的 {archives.length} 次运行</summary>
          <ul className="research-archive-list">{archives.map((item) => <li key={item.run_id}>
            <span className="research-archive-name" title={item.run_id}>{item.question || "未命名实验"} · {researchRunStatusLabel(item.status)}<br />{new Date(item.created_ms).toLocaleString()}</span>
            <button type="button" disabled={archiveBusy || loading} onClick={() => void openArchive(item.run_id)}>查看</button>{" "}
            <button type="button" disabled={archiveBusy || loading} onClick={() => void removeArchive(item.run_id)}>删除</button>
          </li>)}</ul>
        </details> : null}
      </details>

      <div hidden={researchTab !== "expression"}>
      <div className="research-settings-grid"><ResearchGroupingFields showBaselineControl value={grouping} onChange={setGrouping} disabled={loading} />
      <ResearchStatisticsFields value={statistics} onChange={setStatistics} disabled={loading} /></div>
      <div className="research-check-row"><label className="research-checkbox"><input type="checkbox" checked={multiPeriod} disabled={loading} onChange={(event) => setMultiPeriod(event.target.checked)} />同时计算3 / 5 / 10日</label>{" "}
      <label className="research-checkbox" title="运行前后两次读取完整数据库。文件变化时丢弃结果，不代表一致快照。"><input type="checkbox" checked={verifyInputs} disabled={loading} onChange={(event) => setVerifyInputs(event.target.checked)} />校验输入内容</label></div>
      <ExpressionBacktestForm
        values={{...form, boardFilter: effectiveBoardFilter}}
        strategyOptions={strategyOptions}
        indicatorNames={indicatorNames}
        coreRuleOptions={coreRuleOptions}
        boardOptions={backtestBoardOptions}
        loading={loading}
        disabled={initializing}
        error={error}
        onChange={updateForm}
        onApplyRule={applyRule}
        onRun={() => void onRunValidation()}
      />

      {loading ? <p className="expression-backtest-caption" role="status">{phase} <button type="button" disabled={cancelling || phase.startsWith("正在保存")} onClick={() => {
        setCancelling(true);
        void cancelRuleExpressionValidation(activeTask.current).then(() => setPhase("已请求取消，等待当前读取或计算阶段结束")).catch((failure) => { setError(`取消请求失败：${String(failure)}`); setCancelling(false); });
      }}>取消</button></p> : null}

      {result ? <div className="expression-backtest-actions">
        <label>评价周期 <select value={displayedResult?.horizon ?? ""} onChange={(event) => setSelectedHorizon(Number(event.target.value))}>{[result, ...(result.additional_periods ?? [])].map((period, index) => <option key={period.horizon ?? index} value={period.horizon}>{period.horizon ?? runQuery?.backtestPeriod ?? "旧运行"}日</option>)}</select></label>{" "}
        {result.archive_run_id ? <ResearchExportButton sourcePath={sourcePath} runId={result.archive_run_id} disabled={loading} /> : null}
        <details><summary>配置、输入与标签版本</summary><p>本次分组：{JSON.stringify(runQuery?.researchGrouping ?? {})}；统计设置：{JSON.stringify(runQuery?.researchStatistics ?? {})}。</p><p>{displayedResult?.label_algorithm ?? "旧结果未记录"}；{displayedResult?.input_identity === "content_checked_not_snapshot" ? "所列文件运行前后内容一致，非一致快照" : "输入同源和运行期间一致性未认证"}。{displayedResult?.baseline_origin === "isolated_native_scoring_v1" ? "相关性、收益回归及分数档来自本次隔离重算基线。" : "旧结果的相关性及收益回归来自历史评分，同源未认证。"}</p><pre style={{whiteSpace: "pre-wrap", overflowWrap: "anywhere"}}>{JSON.stringify(displayedResult?.input_manifest ?? {}, null, 2)}</pre></details>
      </div> : null}
      {selectedCombo ? (
        <>

          <ExpressionCoreSummary combo={selectedCombo} direction={displayedForm.direction} />
          {hasUnknownCombo ? (
            <ExpressionParameterRobustness
              combos={comboRows}
              selectedComboKey={selectedCombo.combo_key}
              onSelectCombo={setSelectedComboKey}
            />
          ) : null}
          <ExpressionQuarterSummary combo={selectedCombo} startDate={displayedForm.startDateInput} endDate={displayedForm.endDateInput} />
          <details className="expression-backtest-section"><summary>高级诊断</summary>
          <ExpressionWalkForward combo={selectedCombo} />
          <ExpressionLayerResult combo={selectedCombo} />
          <ExpressionSimilarity combo={selectedCombo} baselineOrigin={displayedResult?.baseline_origin} />
          <ExpressionIncremental combo={selectedCombo} />
          </details>
          {result?.membership_saved && result.archive_run_id && runQuery ? <ResearchEvidencePanel key={`${result.archive_run_id}-${selectedCombo.combo_key}-${displayedResult?.horizon}`} sourcePath={sourcePath} runId={result.archive_run_id} comboKey={selectedCombo.combo_key} horizon={displayedResult?.horizon ?? runQuery.backtestPeriod ?? 1} startDate={runQuery.startDate} endDate={runQuery.endDate} direction={displayedForm.direction === "negative" ? -1 : 1} initialQuery={exploratoryFilter} onBranchSaved={() => {void listResearchArchives(sourcePath).then((runs) => setArchives(runs.filter((run) => run.kind === "expression_validation"))).catch((failure) => setArchiveMessage(String(failure)));}} /> : null}
          <details className="expression-backtest-section"><summary>样本复核与日度明细</summary>
          <p className="research-meta">抽样方法：{result?.random_sample_method || "旧运行未记录"}</p>
          <ExpressionSamplesCheck combo={selectedCombo} context={validationContext} />
          <ExpressionDailyDetail combo={selectedCombo} />
          </details>
        </>
      ) : null}
      </div>
      <div hidden={researchTab !== "ranking"}>
        <div className="research-toolbar"><label className="research-field">开始日期 <input type="date" value={form.startDateInput} disabled={rankingRunning} onChange={(event) => updateForm({startDateInput: event.target.value})} /></label><label className="research-field">结束日期 <input type="date" value={form.endDateInput} disabled={rankingRunning} onChange={(event) => updateForm({endDateInput: event.target.value})} /></label><label className="research-field">持有期 <input type="number" min={1} max={252} value={form.holdingPeriodText} disabled={rankingRunning} onChange={(event) => updateForm({holdingPeriodText: event.target.value})} /></label><button type="button" disabled={rankingRunning} onClick={() => setResearchTab("expression")}>其他共用评价设置</button></div>
        <RankComparisonPanel key={sourcePath} settings={{source_path: sourcePath, stock_adj_type: form.stockAdjType, index_ts_code: form.indexTsCode, index_beta: Number(form.indexBetaText), concept_beta: Number(form.conceptBetaText), industry_beta: Number(form.industryBetaText), start_date: normalizeDateInput(form.startDateInput) ?? "", end_date: normalizeDateInput(form.endDateInput) ?? "", min_listed_trade_days: Number(form.minListedTradeDaysText), backtest_period: Number(form.holdingPeriodText)}} disabled={loading || initializing} onRunningChange={setRankingRunning} onRestoreSettings={(saved) => updateForm({startDateInput: compactDateToInput(saved.start_date), endDateInput: compactDateToInput(saved.end_date), holdingPeriodText: String(saved.backtest_period), minListedTradeDaysText: String(saved.min_listed_trade_days), stockAdjType: saved.stock_adj_type, indexTsCode: saved.index_ts_code, indexBetaText: String(saved.index_beta), conceptBetaText: String(saved.concept_beta), industryBetaText: String(saved.industry_beta)})} /></div>
    </div>
  );
}
