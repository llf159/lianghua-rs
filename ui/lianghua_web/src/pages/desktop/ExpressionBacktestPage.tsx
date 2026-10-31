import "./css/ResearchWorkbench.css";
import { useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { listen } from "@tauri-apps/api/event";
import { getIndicatorManagePage } from "../../apis/dataDownload";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import { getStrategyManagePage, type StrategyManageRuleItem } from "../../apis/strategyManage";
import {
  deleteResearchArchive,
  type ResearchEvidenceQuery,
  type ResearchGrouping,
  type ResearchStatisticsSettings,
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
  INDEX_OPTIONS,
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
import { createResearchRuleDraft } from "../../shared/researchRuleDraft";
import { prepareTrackingDefinitions } from "../../shared/strategyTrackingDraft";
import { validateDistPointRows } from "../../shared/strategyDistPoints";
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
import { ResearchHistoryComparison } from "./components/ResearchHistoryComparison";
import { ResearchRunProgress } from "./components/ResearchRunProgress";
import { ExpressionCoreSummary } from "./components/expressionBacktest/ExpressionCoreSummary";
import { type ResearchNotes } from "./components/ResearchNotesFields";
import { ExpressionQuarterSummary } from "./components/expressionBacktest/ExpressionQuarterSummary";
import { ExpressionReturnDistribution } from "./components/expressionBacktest/ExpressionReturnDistribution";
import { ExpressionIncremental } from "./components/expressionBacktest/ExpressionIncremental";
import { ExpressionLayerResult } from "./components/expressionBacktest/ExpressionLayerResult";
import { ExpressionParameterRobustness } from "./components/expressionBacktest/ExpressionParameterRobustness";
import { ExpressionSamplesCheck } from "./components/expressionBacktest/ExpressionSamplesCheck";
import { ExpressionSimilarity } from "./components/expressionBacktest/ExpressionSimilarity";
import { ExpressionWalkForward } from "./components/expressionBacktest/ExpressionWalkForward";
import "./css/ExpressionBacktestPage.css";

export default function ExpressionBacktestPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const initialRule = useRef(searchParams.get("rule"));
  const feedbackRef = useRef<HTMLDivElement>(null);
  const { excludeStBoard } = useConceptExclusions();
  const storedCommonParams = useMemo(() => readStoredBacktestCommonParams(), []);
  const [sourcePath, setSourcePath] = useState(() => readStoredSourcePath());
  const [strategyOptions, setStrategyOptions] = useState<StrategyManageRuleItem[]>([]);
  const [indicatorNames, setIndicatorNames] = useState<string[]>([]);
  const [coreRuleOptions, setCoreRuleOptions] = useState<ValidationCoreRuleOption[]>([]);
  const [form, setForm] = useState<ExpressionBacktestFormValues>(() => ({
    importRuleName: "",
    nativeRule: null,
    ruleDraft: createResearchRuleDraft(),
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
  const [resultView, setResultView] = useState("overview");
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
  const [multiPeriod, setMultiPeriod] = useState(false);
  const [baselineDiagnostics, setBaselineDiagnostics] = useState(false);
  const [selectedHorizon, setSelectedHorizon] = useState<number | null>(null);
  const [verifyInputs, setVerifyInputs] = useState(false);
  const researchTab = searchParams.get("mode") === "ranking" ? "ranking" : "expression";
  function setResearchTab(mode: string) {
    setSearchParams((current) => { current.set("mode", mode); return current; });
  }
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
    comboRows.find((item) => item.combo_key === selectedComboKey) ?? comboRows.find((item) => item.component === "combined") ?? comboRows[0] ?? null;
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
            const original = managePage.rules?.find((rule) => rule.name === initialRule.current);
            if (original) {
              setForm((current) => ({...current, importRuleName: original.name, ruleDraft: createResearchRuleDraft(original), nativeRule: original.kind === "combination" || original.dist_points?.length || original.max_points != null || original.tracking.length ? structuredClone(original) : null, expression: original.when ?? "", direction: (original.points || original.points_by_hits?.find((point) => point !== 0) || original.dist_points?.find((item) => item.points !== 0)?.points || 1) < 0 ? "negative" : "positive", scopeWay: resolveValidationScopeWay(original.scope_way).scopeWay, scopeWindowsText: String(Math.max(1, original.scope_windows ?? 1)), consecThresholdText: String(resolveValidationScopeWay(original.scope_way).consecThreshold)}));
            }
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
    setError("");
    if (!matched) {
      updateForm({ importRuleName: "", nativeRule: null, ruleDraft: createResearchRuleDraft(), expression: "", direction: "positive", scopeWay: "LAST", scopeWindowsText: "1", enableUnknown: false, unknownConfigs: [] });
      return;
    }

    const parsedScopeWay = resolveValidationScopeWay(matched.scope_way);
    updateForm({
      importRuleName: ruleName,
      ruleDraft: createResearchRuleDraft(matched),
      nativeRule: matched.kind === "combination" || matched.dist_points?.length || matched.max_points != null || matched.tracking.length ? structuredClone(matched) : null,
      enableUnknown: matched.kind === "combination" || matched.dist_points?.length || matched.max_points != null || matched.tracking.length ? false : form.enableUnknown,
      expression: matched.when ?? "",
      direction: (matched.points || matched.points_by_hits?.find((point) => point !== 0) || matched.dist_points?.find((item) => item.points !== 0)?.points || 1) < 0 ? "negative" : "positive",
      scopeWay: parsedScopeWay.scopeWay,
      consecThresholdText: String(parsedScopeWay.consecThreshold),
      scopeWindowsText: String(Math.max(1, matched.scope_windows ?? 1)),
      unknownConfigs: form.enableUnknown && matched.kind !== "combination" && !matched.dist_points?.length && matched.max_points == null && !matched.tracking.length
        ? inferUnknownConfigs(matched.when ?? "", indicatorNames)
        : [],
    });
  }

  async function onRunValidation() {
    if (loading || rankingRunning || initializing || archiveBusy) return;
    const ruleDraft = form.ruleDraft ?? createResearchRuleDraft(form.nativeRule ?? undefined);
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
    if (ruleDraft.kind === "combination") {
      if (!ruleDraft.conditions.length || ruleDraft.conditions.some((condition) => !condition.name.trim() || !condition.when.trim())) {
        setError("请填写每个组合条件的名称和表达式。");
        return;
      }
      if (new Set(ruleDraft.conditions.map((condition) => condition.name.trim())).size !== ruleDraft.conditions.length) {
        setError("组合条件名称不能重复。");
        return;
      }
      if (ruleDraft.hitPointTexts.length !== ruleDraft.conditions.length || ruleDraft.bonusPointTexts.length !== ruleDraft.conditions.length || [...ruleDraft.hitPointTexts, ...ruleDraft.bonusPointTexts].some((text) => !text.trim() || !Number.isFinite(Number(text)))) {
        setError("请填写每种命中数量的分值和每个条件的附加分，支持负数和小数。");
        return;
      }
    }
    if (!form.expression.trim() && ruleDraft.kind === "single") {
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

    const fixedPoints = Number(ruleDraft.fixedPointsText);
    if (ruleDraft.kind === "single" && ruleDraft.scoreMode === "fixed" && (!ruleDraft.fixedPointsText.trim() || !Number.isFinite(fixedPoints))) {
      setError("固定分值必须是有效数字，支持负数和小数。");
      return;
    }
    const distPoints = ruleDraft.kind === "single" && ruleDraft.scoreMode === "dist" ? validateDistPointRows(ruleDraft.distPointRows) : null;
    if (distPoints && (normalizedScopeWay !== "EACH" && normalizedScopeWay !== "RECENT")) {
      setError("区间字典仅支持 EACH 或 RECENT 触发方式。");
      return;
    }
    if (distPoints && Object.keys(distPoints.errors).length) {
      setError(Object.values(distPoints.errors)[0]);
      return;
    }
    const maxPoints = ruleDraft.kind === "combination" || normalizedScopeWay === "EACH" ? parseOptionalNumberInput(ruleDraft.maxPointsText) : undefined;
    const maxBonusPoints = ruleDraft.kind === "combination" ? parseOptionalNumberInput(ruleDraft.maxBonusPointsText) : undefined;
    if (((ruleDraft.kind === "combination" || normalizedScopeWay === "EACH") && ruleDraft.maxPointsText.trim() && maxPoints === undefined) || (maxPoints != null && maxPoints < 0) || (ruleDraft.kind === "combination" && ruleDraft.maxBonusPointsText.trim() && maxBonusPoints === undefined) || (maxBonusPoints != null && maxBonusPoints < 0)) {
      setError("分数限制值必须为非负数；留空表示不限。");
      return;
    }

    let tracking: ReturnType<typeof prepareTrackingDefinitions>;
    try { tracking = prepareTrackingDefinitions(ruleDraft.tracking ?? form.nativeRule?.tracking ?? [], ruleDraft.captureTexts ?? {}); }
    catch (failure) { setError(String(failure)); return; }

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
    const manualStrategyName = ruleDraft.name.trim() || resolvedRuleName || (ruleDraft.kind === "combination" ? "未命名组合策略" : "未命名策略");
    const normalizedManualPoints = form.direction === "negative" ? -1 : 1;

    setError("");
    setLoading(true);
    setCancelling(false);
    setPhase("等待仓库可用");
    activeTask.current = `${Date.now()}-${Math.random().toString(36).slice(2)}`;
    let runQuerySnapshot: RuleExpressionValidationQuery | null = null;
    try {
      const query: RuleExpressionValidationQuery = {
        taskId: activeTask.current,
        baselineDiagnostics: baselineDiagnostics || form.coreRuleNames.length > 0 || Boolean(grouping.baseline_score_boundaries?.length),
        additionalPeriods: multiPeriod ? [3, 5, 10] : [],
        researchGrouping: grouping,
        researchStatistics: statistics,
        verifyInputs,
        sourcePath,
        importRuleName: resolvedRuleName,
        manualStrategy: {
          name: manualStrategyName,
          tracking,
          nativeRule: {
            name: manualStrategyName, enabled: true,
            scene: form.nativeRule?.scene_name || selectedRule?.scene_name || "研究策略",
            kind: ruleDraft.kind, stage: form.nativeRule?.stage || selectedRule?.stage || "base",
            scope_way: normalizedScopeWay, scope_windows: scopeWindows,
            when: ruleDraft.kind === "combination" ? "" : form.expression.trim(),
            points: ruleDraft.kind === "combination" || distPoints ? 0 : fixedPoints,
            dist_points: distPoints?.points ?? null,
            condition: ruleDraft.kind === "combination" ? ruleDraft.conditions.map((condition, index) => ({name: condition.name.trim(), when: condition.when.trim(), bonus_points: Number(ruleDraft.bonusPointTexts[index])})) : [],
            points_by_hits: ruleDraft.kind === "combination" ? [0, ...ruleDraft.hitPointTexts.map(Number)] : null,
            max_points: maxPoints ?? null, max_bonus_points: maxBonusPoints ?? null,
            explain: form.nativeRule?.explain || selectedRule?.explain || `${manualStrategyName}回测`,
          },
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
      const data = await runRuleExpressionValidation({...query, researchQuestion: "", researchConfiguration: {form: {...form, boardFilter: effectiveBoardFilter}, query, notes: researchNotes}, parentRunId: archiveParent});
      activeTask.current = "";
      setPhase("正在保存结果");
      setRunForm({...form, boardFilter: effectiveBoardFilter});
      setSelectedComboKey((data.combo_results.find((item) => item.component === "combined") ?? data.combo_results[0])?.combo_key ?? "");
      setRunQuery(query);
      const compacted = compactRuleExpressionValidationData(data);
      setResult(compacted);
      setResearchQuestion("");
      requestAnimationFrame(() => feedbackRef.current?.scrollIntoView({ behavior: "smooth", block: "start" }));
      updateForm({ sampleLimitText: String(compacted.sample_limit_per_group) });
      setSelectedHorizon(data.horizon ?? query.backtestPeriod ?? null);
      if (data.archive_run_id) {
        setArchiveParent(data.archive_run_id);
        setArchiveMessage("统计与样本已保存。");
      }
      try { setArchives((await listResearchArchives(sourcePath)).filter((item) => item.kind === "expression_validation")); }
      catch (failure) { setArchiveMessage(`读取存档列表失败：${String(failure)}`); }
    } catch (runError) {
      activeTask.current = "";
      setPhase("正在保存运行记录");
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
      const query = {sourcePath, importRuleName: form.importRuleName, indexTsCode: form.indexTsCode, startDate: normalizeDateInput(form.startDateInput) ?? "", endDate: normalizeDateInput(form.endDateInput) ?? "", researchGrouping: grouping, researchStatistics: statistics, additionalPeriods: multiPeriod ? [3,5,10] : [], baselineDiagnostics: baselineDiagnostics || form.coreRuleNames.length > 0 || Boolean(grouping.baseline_score_boundaries?.length), verifyInputs};
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
      const restoredForm = {...config.form, ruleDraft: config.form.ruleDraft ?? {...createResearchRuleDraft(config.form.nativeRule ?? undefined), ...(!config.form.nativeRule ? {name: config.form.importRuleName, fixedPointsText: config.form.direction === "negative" ? "-1" : "1"} : {})}};
      setForm(restoredForm);
      setGrouping(config.query.researchGrouping ?? {market_cap_boundaries: [], market_metric: "", market_boundaries: []});
      setStatistics(config.query.researchStatistics ?? {block_length: 10, resamples: 500, seed: 0});
      setMultiPeriod(Boolean(config.query.additionalPeriods?.length));
      setBaselineDiagnostics(config.query.baselineDiagnostics ?? true);
      setVerifyInputs(Boolean(config.query.verifyInputs));
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
      setRunForm(restoredForm);
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
      if (result?.archive_run_id === runId) {setResult(null); setRunForm(null); setRunQuery(null);}
      setArchiveMessage("研究存档已删除。");
    } catch (failure) { setArchiveMessage(`删除研究存档失败：${String(failure)}`); }
    finally { setArchiveBusy(false); }
  }

  return (
    <div className="expression-backtest-page research-workbench">
      <header className="expression-backtest-header">
        <h2 className="expression-backtest-title">研究回测</h2>
      </header>

      <div className="research-mode-tabs" aria-label="研究模式">
        <button type="button" aria-pressed={researchTab === "expression"} disabled={loading || rankingRunning} onClick={() => setResearchTab("expression")}>策略研究</button>
        <button type="button" aria-pressed={researchTab === "ranking"} disabled={loading || rankingRunning} onClick={() => setResearchTab("ranking")}>排名对照</button>
      </div>
      <div className="research-toolbar research-file-actions" hidden={researchTab !== "expression"}>
        <ResearchImportButton sourcePath={sourcePath} disabled={loading || rankingRunning} onImported={() => {void listResearchArchives(sourcePath).then((items) => setArchives(items.filter((item) => item.kind === "expression_validation"))).catch((failure) => setArchiveMessage(String(failure)));}} />

        <button type="button" disabled={!sourcePath || archiveBusy || loading || rankingRunning} onClick={() => void saveDraft()}>保存草稿</button>{" "}
        <button type="button" disabled={!result || !runQuery || archiveBusy || loading} onClick={() => void saveArchive()}>另存结果</button>
      </div>

      <div hidden={researchTab !== "expression"}>
      <ResearchHistoryComparison kind="expression_validation" sourcePath={sourcePath} archives={archives} disabled={loading || archiveBusy} onOpen={(runId) => void openArchive(runId)} onUpdated={(header) => {setArchives((items) => items.map((item) => item.run_id === header.run_id ? header : item)); if (result?.archive_run_id === header.run_id) setResearchQuestion(header.question);}} result={result} settings={runQuery} combo={selectedCombo} horizon={displayedResult?.horizon ?? runQuery?.backtestPeriod ?? null} direction={displayedForm.direction} />
      {archiveMessage ? <p className="expression-backtest-caption" role="status">{archiveMessage}</p> : null}
      <div>
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
        extraParameters={<>

      <div className="research-settings-grid"><ResearchGroupingFields showBaselineControl value={grouping} onChange={setGrouping} disabled={loading} />
      <ResearchStatisticsFields value={statistics} onChange={setStatistics} disabled={loading} /></div>
      <div className="research-check-row"><label className="research-checkbox"><input type="checkbox" checked={multiPeriod} disabled={loading} onChange={(event) => setMultiPeriod(event.target.checked)} />额外计算3 / 5 / 10日（增加耗时）</label>{" "}
      <label className="research-checkbox" title="运行前后两次读取完整数据库。文件变化时丢弃结果，不代表一致快照。"><input type="checkbox" checked={verifyInputs} disabled={loading} onChange={(event) => setVerifyInputs(event.target.checked)} />校验输入内容</label>
      <label className="research-checkbox"><input type="checkbox" checked={baselineDiagnostics} disabled={loading} onChange={(event) => setBaselineDiagnostics(event.target.checked)} />完整策略基线诊断（增加耗时）</label></div>

        </>}
      />
      </div>

      {loading ? <ResearchRunProgress phase={phase} cancelling={cancelling} onCancel={() => {
        setCancelling(true);
        void cancelRuleExpressionValidation(activeTask.current).then(() => setPhase("已请求取消，等待当前读取或计算阶段结束")).catch((failure) => { setError(`取消请求失败：${String(failure)}`); setCancelling(false); });
      }} /> : null}

      <div ref={feedbackRef} className="research-feedback-target">
      {result?.archive_run_id ? <button type="button" className="research-history-delete" disabled={loading || archiveBusy} onClick={() => void removeArchive(result.archive_run_id!)}>删除当前记录</button> : null}
      {result && runForm ? <p className="research-meta">{runForm.ruleDraft?.name || runForm.importRuleName || (runForm.ruleDraft?.kind === "combination" ? "组合策略" : "表达式")} · {runForm.startDateInput} ~ {runForm.endDateInput}{JSON.stringify(form) !== JSON.stringify(runForm) ? " · 参数已修改，结果尚未重算" : ""}</p> : null}
      {result ? <div className="expression-backtest-actions">
        {selectedCombo && new Set(comboRows.map((item) => item.component ?? "base")).size > 1 ? <label>信号组成 <select value={selectedCombo.component ?? "base"} onChange={(event) => {
          const next = comboRows.find((item) => (item.component ?? "base") === event.target.value && JSON.stringify(item.unknown_values) === JSON.stringify(selectedCombo.unknown_values));
          if (next) setSelectedComboKey(next.combo_key);
        }}>{Array.from(new Map(comboRows.map((item) => [item.component ?? "base", item.component_label ?? "原始信号"]))).map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></label> : null}
        <label>评价周期 <select value={displayedResult?.horizon ?? ""} onChange={(event) => setSelectedHorizon(Number(event.target.value))}>{[result, ...(result.additional_periods ?? [])].map((period, index) => <option key={period.horizon ?? index} value={period.horizon}>{period.horizon ?? runQuery?.backtestPeriod ?? "旧运行"}日</option>)}</select></label>{" "}
        {result.archive_run_id ? <ResearchExportButton sourcePath={sourcePath} runId={result.archive_run_id} disabled={loading} /> : null}

      </div> : null}
      {selectedCombo ? (
        <>

          {selectedCombo.component && selectedCombo.component !== "base" ? <p className="research-meta">跟踪按每日有效得分统计，包含保分期；0 分不产生得分样本。各条跟踪独立回放，合并信号为原始分与全部有效跟踪分之和。</p> : null}
          <ExpressionCoreSummary combo={selectedCombo} direction={displayedForm.direction} />
          <nav className="research-result-tabs" aria-label="结果内容">
            {[{value: "overview", label: "分组统计"}, {value: "periods", label: "季度表现"}, {value: "diagnostics", label: "稳健性诊断"}, {value: "samples", label: "正负样本"}].map((item) => <button type="button" key={item.value} aria-pressed={resultView === item.value} onClick={() => setResultView(item.value)}>{item.label}</button>)}
          </nav>
          {resultView === "overview" ? <>
          {hasUnknownCombo ? (
            <ExpressionParameterRobustness
              combos={comboRows.filter((item) => (item.component ?? "base") === (selectedCombo.component ?? "base"))}
              selectedComboKey={selectedCombo.combo_key}
              onSelectCombo={setSelectedComboKey}
            />
          ) : null}
          {result?.archive_run_id && runQuery && (selectedCombo.evidence_summaries || result.membership_saved) ? <ResearchEvidencePanel key={`${result.archive_run_id}-${selectedCombo.combo_key}-${displayedResult?.horizon}`} sourcePath={sourcePath} runId={result.archive_run_id} comboKey={selectedCombo.combo_key} horizon={displayedResult?.horizon ?? runQuery.backtestPeriod ?? 1} startDate={runQuery.startDate} endDate={runQuery.endDate} direction={displayedForm.direction === "negative" ? -1 : 1} savedSummaries={selectedCombo.evidence_summaries} /> : null}
          </> : null}
          {resultView === "periods" ? <ExpressionQuarterSummary combo={selectedCombo} startDate={displayedForm.startDateInput} endDate={displayedForm.endDateInput} /> : null}
          {resultView === "diagnostics" ? <>
          <ExpressionWalkForward combo={selectedCombo} />
          <ExpressionLayerResult combo={selectedCombo} />
          {displayedResult?.baseline_origin !== "not_requested" ? <ExpressionSimilarity combo={selectedCombo} baselineOrigin={displayedResult?.baseline_origin} /> : <p className="research-meta">本次未计算规则相关性；需要时在“更多参数”启用完整策略基线诊断。</p>}
          <ExpressionIncremental combo={selectedCombo} />
          </> : null}
          {resultView === "samples" ? <>
          <ExpressionSamplesCheck combo={selectedCombo} context={validationContext} />
          <ExpressionReturnDistribution combo={selectedCombo} />
          </> : null}
        </>
      ) : null}
      </div>
      </div>
      <div hidden={researchTab !== "ranking"}>
        <div className="research-toolbar"><label className="research-field">开始日期 <input type="date" value={form.startDateInput} disabled={rankingRunning} onChange={(event) => updateForm({startDateInput: event.target.value})} /></label><label className="research-field">结束日期 <input type="date" value={form.endDateInput} disabled={rankingRunning} onChange={(event) => updateForm({endDateInput: event.target.value})} /></label><label className="research-field">持有期 <input type="number" min={1} max={252} value={form.holdingPeriodText} disabled={rankingRunning} onChange={(event) => updateForm({holdingPeriodText: event.target.value})} /></label></div>
        <RankComparisonPanel key={sourcePath} active={researchTab === "ranking"} evaluationFields={<div className="research-form-grid">
          <label className="research-field">股票复权<input value={form.stockAdjType} disabled={rankingRunning} onChange={(event) => updateForm({stockAdjType: event.target.value})} /></label>
          <label className="research-field">指数<select value={form.indexTsCode} disabled={rankingRunning} onChange={(event) => updateForm({indexTsCode: event.target.value})}>{INDEX_OPTIONS.map((item) => <option key={item.value} value={item.value}>{item.label}</option>)}</select></label>
          {([{key: "indexBetaText", label: "指数 Beta"}, {key: "conceptBetaText", label: "概念 Beta"}, {key: "industryBetaText", label: "行业 Beta"}] as const).map((item) => <label className="research-field" key={item.key}>{item.label}<input type="number" step="0.01" value={form[item.key]} disabled={rankingRunning} onChange={(event) => updateForm({[item.key]: event.target.value})} /></label>)}
          <label className="research-field">最少上市交易日<input type="number" min={0} value={form.minListedTradeDaysText} disabled={rankingRunning} onChange={(event) => updateForm({minListedTradeDaysText: event.target.value})} /></label>
        </div>} settings={{source_path: sourcePath, stock_adj_type: form.stockAdjType, index_ts_code: form.indexTsCode, index_beta: Number(form.indexBetaText), concept_beta: Number(form.conceptBetaText), industry_beta: Number(form.industryBetaText), start_date: normalizeDateInput(form.startDateInput) ?? "", end_date: normalizeDateInput(form.endDateInput) ?? "", min_listed_trade_days: Number(form.minListedTradeDaysText), backtest_period: Number(form.holdingPeriodText)}} disabled={loading || initializing} onRunningChange={setRankingRunning} onRestoreSettings={(saved) => updateForm({startDateInput: compactDateToInput(saved.start_date), endDateInput: compactDateToInput(saved.end_date), holdingPeriodText: String(saved.backtest_period), minListedTradeDaysText: String(saved.min_listed_trade_days), stockAdjType: saved.stock_adj_type, indexTsCode: saved.index_ts_code, indexBetaText: String(saved.index_beta), conceptBetaText: String(saved.concept_beta), industryBetaText: String(saved.industry_beta)})} /></div>
    </div>
  );
}
