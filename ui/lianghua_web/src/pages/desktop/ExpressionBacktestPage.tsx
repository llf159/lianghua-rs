import { useEffect, useMemo, useState } from "react";
import { getIndicatorManagePage } from "../../apis/dataDownload";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import { getStrategyManagePage, type StrategyManageRuleItem } from "../../apis/strategyManage";
import {
  getValidationCoreRuleOptions,
  getRuleLayerBacktestDefaults,
  runRuleExpressionValidation,
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
import { ExpressionCoreSummary } from "./components/expressionBacktest/ExpressionCoreSummary";
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
  const [error, setError] = useState("");
  const [result, setResult] = useState<RuleExpressionValidationData | null>(null);
  const [selectedComboKey, setSelectedComboKey] = useState("");

  const backtestBoardOptions = useMemo(
    () => buildBoardFilterOptions(STOCK_PICK_BOARD_OPTIONS, excludeStBoard),
    [excludeStBoard],
  );
  const comboRows = result?.combo_results ?? [];
  const selectedCombo =
    comboRows.find((item) => item.combo_key === selectedComboKey) ?? comboRows[0] ?? null;
  const hasUnknownCombo = comboRows.some((item) => item.unknown_values.length > 0);
  const selectedRule = strategyOptions.find((item) => item.name === form.importRuleName.trim());
  const validationContext: ExpressionValidationContext = {
    sourcePath,
    expression: form.expression,
    direction: form.direction,
    importRuleName: form.importRuleName.trim() || "manual_expression_strategy",
    importRuleExplain:
      selectedRule?.explain?.trim() || `表达式回测：${form.importRuleName.trim() || "手动表达式"}`,
    sampleLimitPerGroup: result?.sample_limit_per_group ?? (Number(form.sampleLimitText) || 0),
  };

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

  useEffect(() => {
    if (!backtestBoardOptions.includes(form.boardFilter)) {
      setForm((current) => ({ ...current, boardFilter: "全部" }));
    }
  }, [backtestBoardOptions, form.boardFilter]);

  useEffect(() => {
    setSelectedComboKey(result?.combo_results[0]?.combo_key ?? "");
  }, [result]);

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
    try {
      const data = await runRuleExpressionValidation({
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
        conceptBeta: Number(form.conceptBetaText),
        industryBeta: Number(form.industryBetaText),
        startDate: normalizedStart,
        endDate: normalizedEnd,
        minSamplesPerRuleDay: Math.max(1, Number(form.minSamplesPerDayText) || 1),
        minListedTradeDays: Math.max(0, Number(form.minListedTradeDaysText) || 0),
        backtestPeriod: Math.max(1, Number(form.holdingPeriodText) || 1),
        unknownConfigs,
        sampleLimitPerGroup,
        board: form.boardFilter === "全部" ? undefined : form.boardFilter,
        excludeStBoard: excludeStBoard || undefined,
        walkForwardFolds,
        coreRuleNames: form.coreRuleNames,
        totalMvMin,
        totalMvMax,
      });
      const compacted = compactRuleExpressionValidationData(data);
      setResult(compacted);
      updateForm({ sampleLimitText: String(compacted.sample_limit_per_group) });
    } catch (runError) {
      setResult(null);
      setError(`执行表达式回测失败: ${String(runError)}`);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="expression-backtest-page">
      <header className="expression-backtest-header">
        <h2 className="expression-backtest-title">表达式回测</h2>
        <p className="expression-backtest-caption">
          验证表达式的有效性、样本外稳定性、参数鲁棒性、与已有策略的重复性和新增价值。
          表达式回测忽略正式 points / dist_points，只验证信号本体。
        </p>
      </header>

      <ExpressionBacktestForm
        values={form}
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

      {selectedCombo ? (
        <>
          <ExpressionCoreSummary combo={selectedCombo} direction={form.direction} />
          {hasUnknownCombo ? (
            <ExpressionParameterRobustness
              combos={comboRows}
              selectedComboKey={selectedCombo.combo_key}
              onSelectCombo={setSelectedComboKey}
            />
          ) : null}
          <ExpressionWalkForward combo={selectedCombo} />
          <ExpressionLayerResult combo={selectedCombo} />
          <ExpressionSimilarity combo={selectedCombo} />
          <ExpressionIncremental combo={selectedCombo} />
          <ExpressionSamplesCheck combo={selectedCombo} context={validationContext} />
          <ExpressionDailyDetail combo={selectedCombo} />
        </>
      ) : null}
    </div>
  );
}
