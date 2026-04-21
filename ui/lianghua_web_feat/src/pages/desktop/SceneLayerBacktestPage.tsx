import { useEffect, useMemo, useState } from "react";
import { useLocation } from "react-router-dom";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import { getStrategyManagePage, type StrategyManageRuleItem } from "../../apis/strategyManage";
import {
  getRuleLayerBacktestDefaults,
  runRankLayerBacktest,
  runRuleExpressionValidation,
  getSceneLayerBacktestDefaults,
  runRuleLayerBacktest,
  runSceneLayerBacktest,
  type RankLayerBacktestData,
  type RuleExpressionValidationData,
  type RuleValidationComboResult,
  type RuleLayerBacktestData,
  type RuleLayerRuleSummary,
  type RuleValidationUnknownConfig,
  type SceneLayerBacktestData,
} from "../../apis/strategyTrigger";
import {
  TableSortButton,
  getAriaSort,
  useTableSort,
  type SortDefinition,
} from "../../shared/tableSort";
import {
  readStoredBacktestHighlightSettings,
  shouldHighlightBacktestMetric,
  type BacktestHighlightMetric,
} from "../../shared/backtestHighlightSettings";
import { readStoredSourcePath } from "../../shared/storage";
import {
  ExpressionValidationSamplesPanel,
  type SceneLayerValidationReturnState,
} from "./ExpressionValidationSamplesPage";
import {
  readTransientStrategyBacktestResult,
  writeTransientStrategyBacktestResult,
} from "../../shared/transientSceneLayerBacktestState";
import "./css/SceneLayerBacktestPage.css";

type RuleSummarySortKey =
  | "rule_name"
  | "point_count"
  | "avg_residual_mean"
  | "spread_mean"
  | "ic_mean"
  | "icir"
  | "ic_t_value";

type ValidationScopeWayOption = "ANY" | "LAST" | "EACH" | "RECENT" | "CONSEC";
type ValidationDirection = "positive" | "negative";
type ValidationUnknownConfigDraft = {
  name: string;
  start: string;
  end: string;
  step: string;
};

const VALIDATION_DEFAULT_SAMPLE_LIMIT = 5;
const VALIDATION_MAX_SAMPLE_LIMIT = 200;

type SceneLayerBacktestLocationState = {
  validationReturnState?: SceneLayerValidationReturnState;
};

function formatDateLabel(value?: string | null) {
  if (!value || value.length !== 8) {
    return "--";
  }
  return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`;
}

function formatNumber(value?: number | null, digits = 4) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  return value.toFixed(digits);
}

function normalizeDateInput(value: string) {
  return value.replaceAll("-", "").trim();
}

function compactDateToInput(value?: string | null) {
  if (!value || !/^\d{8}$/.test(value)) {
    return "";
  }
  return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`;
}

function formatPercent(value?: number | null, digits = 2) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  return `${value.toFixed(digits)}%`;
}

function formatRate(value?: number | null, digits = 1) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  return `${(value * 100).toFixed(digits)}%`;
}

function formatLift(value?: number | null) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  return `${value.toFixed(2)}x`;
}

function buildEmptyUnknownConfig(): ValidationUnknownConfigDraft {
  return {
    name: "",
    start: "2",
    end: "20",
    step: "2",
  };
}

function toUnknownConfigDraft(item: RuleValidationUnknownConfig): ValidationUnknownConfigDraft {
  return {
    name: item.name,
    start: String(item.start),
    end: String(item.end),
    step: String(item.step),
  };
}

function hasValidUnknownConfig(configs: ValidationUnknownConfigDraft[]): boolean {
  return configs.some((item) => item.name.trim().length > 0);
}

function formatUnknownValuesForCombo(item: RuleValidationComboResult) {
  return item.unknown_values.length > 0
    ? item.unknown_values
        .map((unknown) => `${unknown.name}=${formatNumber(unknown.value, 4)}`)
        .join(", ")
    : "默认参数";
}

const BASE_SERIES_IDENTIFIERS = new Set([
  "O",
  "H",
  "L",
  "C",
  "V",
  "AMOUNT",
  "PRE_CLOSE",
  "CHANGE",
  "PCT_CHG",
  "ZHANG",
]);

const RESERVED_BOOLEAN_IDENTIFIERS = new Set(["AND", "OR", "NOT", "TRUE", "FALSE"]);

function readNextNonSpaceChar(expression: string, from: number): string {
  for (let index = from; index < expression.length; index += 1) {
    const ch = expression[index];
    if (!/\s/.test(ch)) {
      return ch;
    }
  }
  return "";
}

function inferUnknownConfigs(expression: string): ValidationUnknownConfigDraft[] {
  const assigned = new Set<string>();
  for (const match of expression.matchAll(/\b([A-Za-z_][A-Za-z0-9_]*)\s*:=/g)) {
    const name = match[1]?.trim();
    if (!name) {
      continue;
    }
    assigned.add(name.toUpperCase());
  }

  const found = new Set<string>();
  const tokenRegExp = /\b([A-Za-z_][A-Za-z0-9_]*)\b/g;
  for (const match of expression.matchAll(tokenRegExp)) {
    const token = match[1]?.trim();
    const full = match[0];
    const matchStart = match.index;
    if (!token) {
      continue;
    }
    if (matchStart === undefined) {
      continue;
    }
    const upper = token.toUpperCase();

    if (RESERVED_BOOLEAN_IDENTIFIERS.has(upper) || BASE_SERIES_IDENTIFIERS.has(upper) || assigned.has(upper)) {
      continue;
    }

    const nextNonSpaceChar = readNextNonSpaceChar(expression, matchStart + full.length);
    const isFunctionCall = nextNonSpaceChar === "(";
    if (isFunctionCall) {
      continue;
    }

    found.add(token);
  }

  const names = Array.from(found).sort((left, right) => left.localeCompare(right));
  if (names.length === 0) {
    return [buildEmptyUnknownConfig()];
  }

  return names.map((name) => ({
    name,
    start: "2",
    end: "20",
    step: "2",
  }));
}

function resolveValidationScopeWay(rawValue?: string | null): {
  scopeWay: ValidationScopeWayOption;
  consecThreshold: number;
} {
  const normalized = (rawValue ?? "").trim().toUpperCase();
  if (!normalized) {
    return {
      scopeWay: "ANY",
      consecThreshold: 2,
    };
  }
  if (normalized === "ANY" || normalized === "LAST" || normalized === "EACH" || normalized === "RECENT") {
    return {
      scopeWay: normalized,
      consecThreshold: 2,
    };
  }
  if (normalized.startsWith("CONSEC>=")) {
    const rawThreshold = normalized.slice("CONSEC>=".length).trim();
    const parsedThreshold = Number(rawThreshold);
    return {
      scopeWay: "CONSEC",
      consecThreshold:
        Number.isFinite(parsedThreshold) && Number.isInteger(parsedThreshold) && parsedThreshold >= 1
          ? parsedThreshold
          : 2,
    };
  }
  return {
    scopeWay: "ANY",
    consecThreshold: 2,
  };
}

const INDEX_OPTIONS = [
  { value: "000001.SH", label: "上证指数" },
  { value: "399001.SZ", label: "深证成指" },
  { value: "399006.SZ", label: "创业板指" },
  { value: "000300.SH", label: "沪深300" },
  { value: "000905.SH", label: "中证500" },
  { value: "000852.SH", label: "中证1000" },
  { value: "000688.SH", label: "科创50" },
] as const;

const VALIDATION_SCOPE_WAY_OPTIONS: Array<{ value: ValidationScopeWayOption; label: string }> = [
  { value: "ANY", label: "ANY" },
  { value: "LAST", label: "LAST" },
  { value: "EACH", label: "EACH" },
  { value: "RECENT", label: "RECENT" },
  { value: "CONSEC", label: "CONSEC" },
];

export default function SceneLayerBacktestPage() {
  const location = useLocation();
  const locationState =
    location.state && typeof location.state === "object"
      ? (location.state as SceneLayerBacktestLocationState)
      : null;
  const [sourcePath, setSourcePath] = useState(() => readStoredSourcePath());
  const [stockAdjType, setStockAdjType] = useState("qfq");
  const [indexTsCode, setIndexTsCode] = useState<string>(INDEX_OPTIONS[0].value);
  const [indexBeta, setIndexBeta] = useState("0.5");
  const [conceptBeta, setConceptBeta] = useState("0.1");
  const [industryBeta, setIndustryBeta] = useState("0.1");
  const [startDateInput, setStartDateInput] = useState("");
  const [endDateInput, setEndDateInput] = useState("");
  const [minSamplesPerDay, setMinSamplesPerDay] = useState("5");
  const [minListedTradeDays, setMinListedTradeDays] = useState("60");
  const [backtestPeriod, setBacktestPeriod] = useState("3");

  const [loading, setLoading] = useState(false);
  const [initializing, setInitializing] = useState(false);
  const [error, setError] = useState("");
  const [rankLoading, setRankLoading] = useState(false);
  const [rankError, setRankError] = useState("");
  const [rankResult, setRankResult] = useState<RankLayerBacktestData | null>(null);
  const [result, setResult] = useState<SceneLayerBacktestData | null>(null);

  const [ruleLoading, setRuleLoading] = useState(false);
  const [ruleError, setRuleError] = useState("");
  const [ruleResult, setRuleResult] = useState<RuleLayerBacktestData | null>(() =>
    readTransientStrategyBacktestResult(),
  );

  const [strategyRuleOptions, setStrategyRuleOptions] = useState<StrategyManageRuleItem[]>([]);
  const [validationImportRuleName, setValidationImportRuleName] = useState("");
  const [validationExpression, setValidationExpression] = useState("");
  const [validationDirection, setValidationDirection] = useState<ValidationDirection>("positive");
  const [validationScopeWay, setValidationScopeWay] = useState<ValidationScopeWayOption>("ANY");
  const [validationConsecThresholdText, setValidationConsecThresholdText] = useState("2");
  const [validationScopeWindowsText, setValidationScopeWindowsText] = useState("1");
  const [validationEnableUnknown, setValidationEnableUnknown] = useState(false);
  const [validationUnknownConfigs, setValidationUnknownConfigs] = useState<
    ValidationUnknownConfigDraft[]
  >([]);
  const [validationSampleLimitText, setValidationSampleLimitText] = useState(
    String(VALIDATION_DEFAULT_SAMPLE_LIMIT),
  );
  const [validationLoading, setValidationLoading] = useState(false);
  const [validationError, setValidationError] = useState("");
  const [validationResult, setValidationResult] = useState<RuleExpressionValidationData | null>(null);
  const [validationSelectedComboKey, setValidationSelectedComboKey] = useState("");
  const [validationRestoredComboKey, setValidationRestoredComboKey] = useState("");
  const [validationDetailModalOpen, setValidationDetailModalOpen] = useState(false);
  const [validationSamplesModalOpen, setValidationSamplesModalOpen] = useState(false);

  useEffect(() => {
    const returnState = locationState?.validationReturnState;
    if (!returnState) {
      return;
    }

    setStockAdjType(returnState.stockAdjType);
    setIndexTsCode(returnState.indexTsCode);
    setIndexBeta(returnState.indexBeta);
    setConceptBeta(returnState.conceptBeta);
    setIndustryBeta(returnState.industryBeta);
    setStartDateInput(returnState.startDateInput);
    setEndDateInput(returnState.endDateInput);
    setMinSamplesPerDay(returnState.minSamplesPerDay);
    setMinListedTradeDays(returnState.minListedTradeDays ?? "60");
    setBacktestPeriod(returnState.backtestPeriod);
    setValidationImportRuleName(returnState.validationImportRuleName);
    setValidationExpression(returnState.validationExpression);
    setValidationDirection("positive");
    setValidationScopeWay(returnState.validationScopeWay);
    setValidationConsecThresholdText(returnState.validationConsecThresholdText);
    setValidationScopeWindowsText(returnState.validationScopeWindowsText);
    setValidationEnableUnknown(returnState.validationEnableUnknown);
    setValidationUnknownConfigs(returnState.validationUnknownConfigs.map(toUnknownConfigDraft));
    setValidationSampleLimitText(returnState.validationSampleLimitText);
    setValidationError("");
    setValidationResult(returnState.validationResult);
    setValidationRestoredComboKey(returnState.validationSelectedComboKey);
    setValidationSelectedComboKey(returnState.validationSelectedComboKey);
    setValidationDetailModalOpen(false);
    setValidationSamplesModalOpen(false);
  }, [locationState]);

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

        let hasSceneDates = false;
        try {
          const sceneDefaults = await getSceneLayerBacktestDefaults(resolved);
          if (cancelled) {
            return;
          }
          if (!locationState?.validationReturnState && sceneDefaults.start_date && sceneDefaults.end_date) {
            setStartDateInput(compactDateToInput(sceneDefaults.start_date));
            setEndDateInput(compactDateToInput(sceneDefaults.end_date));
            hasSceneDates = true;
          }
        } catch (sceneInitError) {
          if (!cancelled) {
            setError(`读取场景默认参数失败: ${String(sceneInitError)}`);
          }
        }

        try {
          const ruleDefaults = await getRuleLayerBacktestDefaults(resolved);
          if (cancelled) {
            return;
          }
          if (!locationState?.validationReturnState && !hasSceneDates) {
            setStartDateInput(compactDateToInput(ruleDefaults.start_date));
            setEndDateInput(compactDateToInput(ruleDefaults.end_date));
          }
        } catch (ruleInitError) {
          if (!cancelled) {
            setRuleError(`读取策略默认参数失败: ${String(ruleInitError)}`);
          }
        }

        try {
          const managePage = await getStrategyManagePage(resolved);
          if (cancelled) {
            return;
          }
          const options = managePage.rules ?? [];
          setStrategyRuleOptions(options);
        } catch (strategyInitError) {
          if (!cancelled) {
            setValidationError(`读取策略编辑参数失败: ${String(strategyInitError)}`);
          }
        }
      } catch (initError) {
        if (!cancelled) {
          setError(`读取回测默认参数失败: ${String(initError)}`);
          setRuleError(`读取回测默认参数失败: ${String(initError)}`);
          setValidationError(`读取回测默认参数失败: ${String(initError)}`);
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
  }, [locationState]);

  const allSceneSummaries = result?.all_scene_summaries ?? [];
  const allRuleSummaries = ruleResult?.all_rule_summaries ?? [];
  const rankLayerSummaries = rankResult?.layer_summaries ?? [];
  const backtestHighlightSettings = readStoredBacktestHighlightSettings();

  function metricHighlightClass(
    metric: BacktestHighlightMetric,
    value?: number | null,
  ) {
    return shouldHighlightBacktestMetric(metric, value, backtestHighlightSettings)
      ? "scene-layer-metric-hit"
      : undefined;
  }

  const selectedValidationCombo = useMemo(() => {
    if (!validationResult) {
      return null;
    }
    return (
      validationResult.combo_results.find(
        (item) => item.combo_key === validationSelectedComboKey,
      ) ?? validationResult.combo_results[0] ?? null
    );
  }, [validationResult, validationSelectedComboKey]);

  const validationComboCount = validationResult?.combo_results.length ?? 0;
  const hasUnknownValidationCombo =
    validationResult?.combo_results.some((item) => item.unknown_values.length > 0) ?? false;
  const shouldUseValidationDetailModal =
    validationComboCount > 1 && hasUnknownValidationCombo;
  const shouldUseInlineComboSelection =
    !shouldUseValidationDetailModal && validationComboCount > 1;

  useEffect(() => {
    if (!validationResult) {
      setValidationRestoredComboKey("");
      setValidationSelectedComboKey("");
      setValidationDetailModalOpen(false);
      setValidationSamplesModalOpen(false);
      return;
    }

    if (
      validationRestoredComboKey &&
      validationResult.combo_results.some((item) => item.combo_key === validationRestoredComboKey)
    ) {
      setValidationSelectedComboKey(validationRestoredComboKey);
      setValidationRestoredComboKey("");
      setValidationDetailModalOpen(false);
      setValidationSamplesModalOpen(false);
      return;
    }

    const preferred =
      validationResult.best_combo_key?.trim() ||
      validationResult.combo_results[0]?.combo_key ||
      "";
    setValidationRestoredComboKey("");
    setValidationSelectedComboKey(preferred);
    setValidationDetailModalOpen(false);
    setValidationSamplesModalOpen(false);
  }, [validationResult, validationRestoredComboKey]);

  useEffect(() => {
    if (!validationDetailModalOpen && !validationSamplesModalOpen) {
      return;
    }

    const previousOverflow = document.body.style.overflow;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        if (validationSamplesModalOpen) {
          setValidationSamplesModalOpen(false);
          return;
        }
        setValidationDetailModalOpen(false);
      }
    };

    document.body.style.overflow = "hidden";
    window.addEventListener("keydown", onKeyDown);
    return () => {
      window.removeEventListener("keydown", onKeyDown);
      document.body.style.overflow = previousOverflow;
    };
  }, [validationDetailModalOpen, validationSamplesModalOpen]);

  useEffect(() => {
    if (!shouldUseValidationDetailModal) {
      setValidationDetailModalOpen(false);
    }
  }, [shouldUseValidationDetailModal]);

  const ruleSummarySortDefinitions = useMemo(
    () =>
      ({
        rule_name: {
          value: (row: RuleLayerRuleSummary) => row.rule_name,
        },
        point_count: {
          value: (row: RuleLayerRuleSummary) => row.point_count,
        },
        avg_residual_mean: {
          value: (row: RuleLayerRuleSummary) => row.avg_residual_mean,
        },
        spread_mean: {
          value: (row: RuleLayerRuleSummary) => row.spread_mean,
        },
        ic_mean: {
          value: (row: RuleLayerRuleSummary) => row.ic_mean,
        },
        icir: {
          value: (row: RuleLayerRuleSummary) => row.icir,
        },
        ic_t_value: {
          value: (row: RuleLayerRuleSummary) => row.ic_t_value,
        },
      }) satisfies Partial<
        Record<RuleSummarySortKey, SortDefinition<RuleLayerRuleSummary>>
      >,
    [],
  );

  const {
    sortKey: ruleSummarySortKey,
    sortDirection: ruleSummarySortDirection,
    sortedRows: sortedRuleSummaries,
    toggleSort: toggleRuleSummarySort,
  } = useTableSort<RuleLayerRuleSummary, RuleSummarySortKey>(
    allRuleSummaries,
    ruleSummarySortDefinitions,
    {
      key: "spread_mean",
      direction: "desc",
    },
  );

  async function onRunBacktest() {
    const normalizedStart = normalizeDateInput(startDateInput);
    const normalizedEnd = normalizeDateInput(endDateInput);

    if (!sourcePath.trim()) {
      setError("当前数据目录为空，请先在数据管理页确认目录。");
      return;
    }
    if (!indexTsCode.trim()) {
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

    setLoading(true);
    setError("");
    try {
      const data = await runSceneLayerBacktest({
        sourcePath,
        stockAdjType: stockAdjType.trim() || "qfq",
        indexTsCode: indexTsCode.trim(),
        indexBeta: Number(indexBeta),
        conceptBeta: Number(conceptBeta),
        industryBeta: Number(industryBeta),
        startDate: normalizedStart,
        endDate: normalizedEnd,
        minSamplesPerSceneDay: Math.max(1, Number(minSamplesPerDay) || 1),
        minListedTradeDays: Math.max(0, Number(minListedTradeDays) || 0),
        backtestPeriod: Math.max(1, Number(backtestPeriod) || 1),
      });
      setResult(data);
    } catch (runError) {
      setResult(null);
      setError(`执行场景整体回测失败: ${String(runError)}`);
    } finally {
      setLoading(false);
    }
  }

  async function onRunRankBacktest() {
    const normalizedStart = normalizeDateInput(startDateInput);
    const normalizedEnd = normalizeDateInput(endDateInput);

    if (!sourcePath.trim()) {
      setRankError("当前数据目录为空，请先在数据管理页确认目录。");
      return;
    }
    if (!indexTsCode.trim()) {
      setRankError("请选择指数。");
      return;
    }
    if (!normalizedStart || !normalizedEnd) {
      setRankError("请填写开始和结束日期。");
      return;
    }
    if (normalizedStart > normalizedEnd) {
      setRankError("开始日期不能晚于结束日期。");
      return;
    }

    setRankLoading(true);
    setRankError("");
    try {
      const data = await runRankLayerBacktest({
        sourcePath,
        stockAdjType: stockAdjType.trim() || "qfq",
        indexTsCode: indexTsCode.trim(),
        indexBeta: Number(indexBeta),
        conceptBeta: Number(conceptBeta),
        industryBeta: Number(industryBeta),
        startDate: normalizedStart,
        endDate: normalizedEnd,
        minSamplesPerRankDay: Math.max(1, Number(minSamplesPerDay) || 1),
        minListedTradeDays: Math.max(0, Number(minListedTradeDays) || 0),
        backtestPeriod: Math.max(1, Number(backtestPeriod) || 1),
      });
      setRankResult(data);
    } catch (runError) {
      setRankResult(null);
      setRankError(`执行排名整体回测失败: ${String(runError)}`);
    } finally {
      setRankLoading(false);
    }
  }

  async function onRunRuleBacktest() {
    const normalizedStart = normalizeDateInput(startDateInput);
    const normalizedEnd = normalizeDateInput(endDateInput);

    if (!sourcePath.trim()) {
      setRuleError("当前数据目录为空，请先在数据管理页确认目录。");
      return;
    }
    if (!indexTsCode.trim()) {
      setRuleError("请选择指数。");
      return;
    }
    if (!normalizedStart || !normalizedEnd) {
      setRuleError("请填写开始和结束日期。");
      return;
    }
    if (normalizedStart > normalizedEnd) {
      setRuleError("开始日期不能晚于结束日期。");
      return;
    }

    setRuleLoading(true);
    setRuleError("");
    try {
      const data = await runRuleLayerBacktest({
        sourcePath,
        stockAdjType: stockAdjType.trim() || "qfq",
        indexTsCode: indexTsCode.trim(),
        indexBeta: Number(indexBeta),
        conceptBeta: Number(conceptBeta),
        industryBeta: Number(industryBeta),
        startDate: normalizedStart,
        endDate: normalizedEnd,
        minSamplesPerRuleDay: Math.max(1, Number(minSamplesPerDay) || 1),
        minListedTradeDays: Math.max(0, Number(minListedTradeDays) || 0),
        backtestPeriod: Math.max(1, Number(backtestPeriod) || 1),
      });
      setRuleResult(data);
      writeTransientStrategyBacktestResult(data);
    } catch (runError) {
      setRuleResult(null);
      writeTransientStrategyBacktestResult(null);
      setRuleError(`执行策略回测失败: ${String(runError)}`);
    } finally {
      setRuleLoading(false);
    }
  }

  function applyValidationRule(ruleName: string) {
    setValidationImportRuleName(ruleName);
    const matched = strategyRuleOptions.find((item) => item.name === ruleName);
    if (!matched) {
      setValidationResult(null);
      setValidationError("");
      return;
    }

    setValidationExpression(matched.when ?? "");
    setValidationDirection(
      Number.isFinite(matched.points) && Number(matched.points) < 0 ? "negative" : "positive",
    );
    const parsedScopeWay = resolveValidationScopeWay(matched.scope_way);
    setValidationScopeWay(parsedScopeWay.scopeWay);
    setValidationConsecThresholdText(String(parsedScopeWay.consecThreshold));
    setValidationScopeWindowsText(String(Math.max(1, matched.scope_windows ?? 1)));
    if (validationEnableUnknown) {
      setValidationUnknownConfigs(inferUnknownConfigs(matched.when ?? ""));
    } else {
      setValidationUnknownConfigs([]);
    }
    setValidationResult(null);
    setValidationError("");
  }

  async function onRunRuleExpressionValidation() {
    const normalizedStart = normalizeDateInput(startDateInput);
    const normalizedEnd = normalizeDateInput(endDateInput);

    if (!sourcePath.trim()) {
      setValidationError("当前数据目录为空，请先在数据管理页确认目录。");
      return;
    }
    if (!indexTsCode.trim()) {
      setValidationError("请选择指数。");
      return;
    }
    if (!normalizedStart || !normalizedEnd) {
      setValidationError("请填写开始和结束日期。");
      return;
    }
    if (normalizedStart > normalizedEnd) {
      setValidationError("开始日期不能晚于结束日期。");
      return;
    }
    if (!validationExpression.trim()) {
      setValidationError("表达式不能为空。");
      return;
    }
    const scopeWindows = Number(validationScopeWindowsText);
    if (!Number.isFinite(scopeWindows) || !Number.isInteger(scopeWindows) || scopeWindows < 1) {
      setValidationError("scope_windows 必须是 >= 1 的整数。");
      return;
    }
    let normalizedScopeWay: string = validationScopeWay;
    if (validationScopeWay === "CONSEC") {
      const consecThreshold = Number(validationConsecThresholdText);
      if (!Number.isFinite(consecThreshold) || !Number.isInteger(consecThreshold) || consecThreshold < 1) {
        setValidationError("CONSEC 阈值必须是 >= 1 的整数。");
        return;
      }
      if (scopeWindows < consecThreshold) {
        setValidationError("scope_windows 不能小于 CONSEC 阈值。");
        return;
      }
      normalizedScopeWay = `CONSEC>=${consecThreshold}`;
    }

    const unknownConfigs = validationEnableUnknown
      ? validationUnknownConfigs
          .map((item) => ({
            name: item.name.trim(),
            start: Number(item.start.trim()),
            end: Number(item.end.trim()),
            step: Number(item.step.trim()),
          }))
          .filter((item) => item.name.length > 0)
      : [];

    if (validationEnableUnknown && unknownConfigs.length === 0) {
      setValidationError("启用未知数后，至少需要一个未知数配置。");
      return;
    }

    for (const item of unknownConfigs) {
      if (!Number.isFinite(item.start) || !Number.isFinite(item.end) || !Number.isFinite(item.step)) {
        setValidationError(`未知数 ${item.name} 存在非法数值。`);
        return;
      }
      if (item.step <= 0) {
        setValidationError(`未知数 ${item.name} 的步长必须 > 0。`);
        return;
      }
      if (item.end < item.start) {
        setValidationError(`未知数 ${item.name} 的结束值不能小于起始值。`);
        return;
      }
    }

    const sampleLimitPerGroupRaw = Number(validationSampleLimitText);
    if (
      !Number.isFinite(sampleLimitPerGroupRaw) ||
      !Number.isInteger(sampleLimitPerGroupRaw) ||
      sampleLimitPerGroupRaw < 1
    ) {
      setValidationError("样本展示上限必须是 >= 1 的整数。");
      return;
    }
    const sampleLimitPerGroup = Math.min(
      VALIDATION_MAX_SAMPLE_LIMIT,
      sampleLimitPerGroupRaw,
    );

    const selectedRule = strategyRuleOptions.find(
      (item) => item.name === validationImportRuleName.trim(),
    );
    const resolvedRuleName = validationImportRuleName.trim();
    const manualStrategyName = resolvedRuleName || "manual_expression_strategy";
    const normalizedManualPoints = validationDirection === "negative" ? -1 : 1;

    setValidationLoading(true);
    setValidationError("");
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
          when: validationExpression.trim(),
          points: normalizedManualPoints,
          explain: selectedRule?.explain?.trim() || `手动表达式验证：${manualStrategyName}`,
        },
        when: validationExpression.trim(),
        scopeWay: normalizedScopeWay,
        scopeWindows,
        stockAdjType: stockAdjType.trim() || "qfq",
        indexTsCode: indexTsCode.trim(),
        indexBeta: Number(indexBeta),
        conceptBeta: Number(conceptBeta),
        industryBeta: Number(industryBeta),
        startDate: normalizedStart,
        endDate: normalizedEnd,
        minSamplesPerRuleDay: Math.max(1, Number(minSamplesPerDay) || 1),
        minListedTradeDays: Math.max(0, Number(minListedTradeDays) || 0),
        backtestPeriod: Math.max(1, Number(backtestPeriod) || 1),
        unknownConfigs,
        sampleLimitPerGroup,
      });
      setValidationResult(data);
      setValidationSampleLimitText(String(data.sample_limit_per_group));
    } catch (runError) {
      setValidationResult(null);
      setValidationError(`执行表达式验证失败: ${String(runError)}`);
    } finally {
      setValidationLoading(false);
    }
  }

  function openValidationDetail(comboKey: string) {
    if (!shouldUseValidationDetailModal) {
      setValidationSelectedComboKey(comboKey);
      return;
    }
    setValidationSelectedComboKey(comboKey);
    setValidationDetailModalOpen(true);
  }

  function closeValidationDetailModal() {
    setValidationDetailModalOpen(false);
  }

  function openValidationSamplesModal(comboKey: string) {
    setValidationSelectedComboKey(comboKey);
    setValidationDetailModalOpen(false);
    setValidationSamplesModalOpen(true);
  }

  function closeValidationSamplesModal() {
    setValidationSamplesModalOpen(false);
  }

  function renderValidationComboDetailSections(
    combo: RuleValidationComboResult,
    useModalLayout = false,
  ) {
    if (!validationResult) {
      return null;
    }

    const sectionClassName = useModalLayout
      ? "scene-layer-layer-summary scene-layer-validation-detail-section"
      : "scene-layer-layer-summary";

    return (
      <>
        <div className="scene-layer-formula-box">
          <strong>替换后表达式</strong>
          <p>{combo.formula || "--"}</p>
        </div>

        <div className={sectionClassName}>
          <h3>
            触发样本（点击样本卡片在浮窗查看；当前每个板块的正向 / 负向 / 随机最多展示 {validationResult.sample_limit_per_group} 条）
          </h3>
          <div className="scene-layer-validation-sample-summary">
            <div className="scene-layer-validation-sample-stats">
              <span>总样本：{combo.sample_stats.total_samples}</span>
              <span>正样本：{combo.sample_stats.positive_count}</span>
              <span>负样本：{combo.sample_stats.negative_count}</span>
              <span>随机池：{combo.sample_stats.random_count}</span>
            </div>

            <button
              type="button"
              className="scene-layer-validation-sample-entry"
              onClick={() => openValidationSamplesModal(combo.combo_key)}
            >
              <span className="scene-layer-validation-sample-entry-label">打开样本浮窗</span>
              <strong>{combo.sample_stats.total_samples} 个样本</strong>
              <span className="scene-layer-validation-sample-entry-meta">
                正样本 {combo.sample_stats.positive_count} · 负样本 {combo.sample_stats.negative_count} · 随机池 {combo.sample_stats.random_count}
              </span>
            </button>
          </div>
        </div>

        <div className={sectionClassName}>
          <h3>策略相似度检查</h3>
          <div className="scene-layer-contrib-table-wrap">
            <table className="scene-layer-contrib-table">
              <thead>
                <tr>
                  <th>现有策略</th>
                  <th>同时触发样本</th>
                  <th>占当前组合</th>
                  <th>占现有策略</th>
                  <th>Lift</th>
                </tr>
              </thead>
              <tbody>
                {combo.similarity_rows.length > 0 ? (
                  combo.similarity_rows.map((row) => (
                    <tr key={row.rule_name}>
                      <td>
                        <strong>{row.rule_name}</strong>
                        {row.explain ? (
                          <div className="scene-layer-similarity-explain">{row.explain}</div>
                        ) : null}
                      </td>
                      <td>{row.overlap_samples}</td>
                      <td>{formatRate(row.overlap_rate_vs_validation)}</td>
                      <td>{formatRate(row.overlap_rate_vs_existing)}</td>
                      <td>{formatLift(row.overlap_lift)}</td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={5}>暂无与当前组合同日同股同时触发的现有策略。</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </>
    );
  }

  return (
    <div className="scene-layer-page">
      <section className="scene-layer-card">
        <h2 className="scene-layer-title">回测全局参数</h2>
        <div className="scene-layer-form-grid">
          <label className="scene-layer-field">
            <span>股票复权</span>
            <input value={stockAdjType} onChange={(event) => setStockAdjType(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>指数</span>
            <select value={indexTsCode} onChange={(event) => setIndexTsCode(event.target.value)}>
              {INDEX_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
          <label className="scene-layer-field">
            <span>指数 Beta</span>
            <input type="number" step="0.01" value={indexBeta} onChange={(event) => setIndexBeta(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>概念 Beta</span>
            <input type="number" step="0.01" value={conceptBeta} onChange={(event) => setConceptBeta(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>行业 Beta</span>
            <input type="number" step="0.01" value={industryBeta} onChange={(event) => setIndustryBeta(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>开始日期</span>
            <input type="date" value={startDateInput} onChange={(event) => setStartDateInput(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>结束日期</span>
            <input type="date" value={endDateInput} onChange={(event) => setEndDateInput(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>日最少样本</span>
            <input type="number" min="1" value={minSamplesPerDay} onChange={(event) => setMinSamplesPerDay(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>最少上市交易日</span>
            <input type="number" min="0" value={minListedTradeDays} onChange={(event) => setMinListedTradeDays(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>回测周期（天）</span>
            <input type="number" min="1" value={backtestPeriod} onChange={(event) => setBacktestPeriod(event.target.value)} />
          </label>
        </div>
      </section>

      <section className="scene-layer-card">
        <h2 className="scene-layer-title">排名整体回测</h2>
        <p className="scene-layer-caption">
          使用 score_summary 中的总分做五层分层，检验总分对后续残差收益的影响，并展示分层差、IC、t、ICIR 及五层依据表。
        </p>

        <div className="scene-layer-actions">
          <button type="button" className="scene-layer-primary-btn" onClick={() => void onRunRankBacktest()} disabled={rankLoading || initializing}>
            {rankLoading ? "回测中..." : "执行排名整体回测"}
          </button>
        </div>

        {rankError ? <div className="scene-layer-error">{rankError}</div> : null}
      </section>

      {rankResult ? (
        <section className="scene-layer-card">
          <div className="scene-layer-layer-summary">
            <h3>排名整体回测汇总</h3>
            <div className="scene-layer-contrib-table-wrap">
              <table className="scene-layer-contrib-table">
                <thead>
                  <tr>
                    <th>对象</th>
                    <th>区间</th>
                    <th>指数</th>
                    <th>Beta（指/概/行）</th>
                    <th>有效交易日</th>
                    <th>总样本数</th>
                    <th>最小样本阈值</th>
                    <th>最少上市交易日</th>
                    <th>回测周期（天）</th>
                    <th>分层差均值（日度第5层-第1层）</th>
                    <th>IC 均值</th>
                    <th>IC t值</th>
                    <th>ICIR</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td>总分</td>
                    <td>{formatDateLabel(rankResult.start_date)} ~ {formatDateLabel(rankResult.end_date)}</td>
                    <td>{rankResult.index_ts_code}</td>
                    <td>{formatNumber(rankResult.index_beta, 2)} / {formatNumber(rankResult.concept_beta, 2)} / {formatNumber(rankResult.industry_beta, 2)}</td>
                    <td>{rankResult.point_count}</td>
                    <td>{rankResult.sample_count}</td>
                    <td>{rankResult.min_samples_per_rank_day}</td>
                    <td>{rankResult.min_listed_trade_days}</td>
                    <td>{rankResult.backtest_period}</td>
                    <td>{formatPercent(rankResult.spread_mean)}</td>
                    <td className={metricHighlightClass("ic", rankResult.ic_mean)}>{formatNumber(rankResult.ic_mean)}</td>
                    <td className={metricHighlightClass("t", rankResult.ic_t_value)}>{formatNumber(rankResult.ic_t_value)}</td>
                    <td className={metricHighlightClass("ir", rankResult.icir)}>{formatNumber(rankResult.icir)}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>

          {rankLayerSummaries.length === 0 ? (
            <div className="scene-layer-empty">当前没有可用于五层分层的总分样本。</div>
          ) : (
            <div className="scene-layer-layer-summary">
              <h3>五层分层依据（按总分从低到高）</h3>
              <div className="scene-layer-contrib-table-wrap">
                <table className="scene-layer-contrib-table">
                  <thead>
                    <tr>
                      <th>分层</th>
                      <th>有效交易日</th>
                      <th>分层样本数</th>
                      <th>分层均分</th>
                      <th>层级收益（日度残差均值）</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rankLayerSummaries.map((item) => (
                      <tr key={item.layer_index}>
                        <td>{item.layer_label}</td>
                        <td>{item.point_count}</td>
                        <td>{item.sample_count}</td>
                        <td>{formatNumber(item.avg_score, 4)}</td>
                        <td>{formatPercent(item.avg_residual_return)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </section>
      ) : null}

      <section className="scene-layer-card">
        <h2 className="scene-layer-title">场景整体回测</h2>
        <p className="scene-layer-caption">
          使用 scene_details 中的场景状态与排序，计算各场景状态下的分层残差收益、分层差、IC / ICIR。
        </p>

        <div className="scene-layer-actions">
          <button type="button" className="scene-layer-primary-btn" onClick={() => void onRunBacktest()} disabled={loading || initializing}>
            {loading ? "回测中..." : "执行场景整体回测"}
          </button>
        </div>

        {error ? <div className="scene-layer-error">{error}</div> : null}
      </section>


      {result ? (
        <section className="scene-layer-card">
          <div className="scene-layer-summary-grid">
            <div className="scene-layer-summary-item">
              <span>场景</span>
              <strong>全部场景</strong>
            </div>
            <div className="scene-layer-summary-item">
              <span>区间</span>
              <strong>{formatDateLabel(result.start_date)} ~ {formatDateLabel(result.end_date)}</strong>
            </div>
            <div className="scene-layer-summary-item">
              <span>场景数</span>
              <strong>{allSceneSummaries.length}</strong>
            </div>
            <div className="scene-layer-summary-item">
              <span>最小样本阈值</span>
              <strong>{result.min_samples_per_scene_day}</strong>
            </div>
            <div className="scene-layer-summary-item">
              <span>最少上市交易日</span>
              <strong>{result.min_listed_trade_days}</strong>
            </div>
            <div className="scene-layer-summary-item">
              <span>回测周期（天）</span>
              <strong>{result.backtest_period}</strong>
            </div>
          </div>

          {allSceneSummaries.length === 0 ? (
            <div className="scene-layer-empty">当前没有可回测的场景。</div>
          ) : null}

          {allSceneSummaries.length > 0 ? (
            <div className="scene-layer-layer-summary">
              <h3>全部场景汇总（按分层差均值降序）</h3>
              <div className="scene-layer-layer-grid">
                {allSceneSummaries.map((item) => (
                  <div key={item.scene_name} className="scene-layer-layer-item">
                    <span className="scene-layer-layer-state">{item.scene_name}</span>
                    <span>有效交易日：{item.point_count}</span>
                    <span>分层差均值：{formatPercent(item.spread_mean)}</span>
                    <span className={metricHighlightClass("ic", item.ic_mean)}>IC 均值：{formatNumber(item.ic_mean)}</span>
                    <span className={metricHighlightClass("ir", item.icir)}>ICIR：{formatNumber(item.icir)}</span>
                    <span className={metricHighlightClass("t", item.ic_t_value)}>IC t值：{formatNumber(item.ic_t_value)}</span>
                  </div>
                ))}
              </div>
            </div>
          ) : null}
        </section>
      ) : null}

      <section className="scene-layer-card">
        <h2 className="scene-layer-title">策略回测</h2>
        <p className="scene-layer-caption">
          使用 rule_details 中的策略得分与残差收益，计算策略日度均值、分层差、IC / ICIR。
        </p>

        <div className="scene-layer-actions">
          <button type="button" className="scene-layer-primary-btn" onClick={() => void onRunRuleBacktest()} disabled={ruleLoading || initializing}>
            {ruleLoading ? "回测中..." : "执行策略回测"}
          </button>
        </div>

        {ruleError ? <div className="scene-layer-error">{ruleError}</div> : null}
      </section>

      {ruleResult ? (
        <section className="scene-layer-card">
          <div className="scene-layer-layer-summary">
            <h3>策略回测汇总</h3>
            <div className="scene-layer-contrib-table-wrap">
              <table className="scene-layer-contrib-table">
                <thead>
                  <tr>
                    <th>策略</th>
                    <th>区间</th>
                    <th>指数</th>
                    <th>Beta（指/概/行）</th>
                    <th>策略数</th>
                    <th>最小样本阈值</th>
                    <th>最少上市交易日</th>
                    <th>回测周期（天）</th>
                    <th>残差均值（日度）</th>
                    <th>分层差均值（日度高分-低分）</th>
                    <th>IC 均值</th>
                    <th>IC t值</th>
                    <th>ICIR</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td>全部策略</td>
                    <td>{formatDateLabel(ruleResult.start_date)} ~ {formatDateLabel(ruleResult.end_date)}</td>
                    <td>{ruleResult.index_ts_code}</td>
                    <td>{formatNumber(ruleResult.index_beta, 2)} / {formatNumber(ruleResult.concept_beta, 2)} / {formatNumber(ruleResult.industry_beta, 2)}</td>
                    <td>{allRuleSummaries.length}</td>
                    <td>{ruleResult.min_samples_per_rule_day}</td>
                    <td>{ruleResult.min_listed_trade_days}</td>
                    <td>{ruleResult.backtest_period}</td>
                    <td>{formatPercent(ruleResult.avg_residual_mean)}</td>
                    <td>{formatPercent(ruleResult.spread_mean)}</td>
                    <td className={metricHighlightClass("ic", ruleResult.ic_mean)}>{formatNumber(ruleResult.ic_mean)}</td>
                    <td className={metricHighlightClass("t", ruleResult.ic_t_value)}>{formatNumber(ruleResult.ic_t_value)}</td>
                    <td className={metricHighlightClass("ir", ruleResult.icir)}>{formatNumber(ruleResult.icir)}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>

          {allRuleSummaries.length === 0 ? (
            <div className="scene-layer-empty">当前没有可回测的策略。</div>
          ) : null}

          {allRuleSummaries.length > 0 ? (
            <div className="scene-layer-layer-summary">
              <h3>全部策略明细（点击表头排序）</h3>
              <div className="scene-layer-contrib-table-wrap">
                <table className="scene-layer-contrib-table scene-layer-rule-detail-table">
                  <thead>
                    <tr>
                      <th aria-sort={getAriaSort(ruleSummarySortKey === "rule_name", ruleSummarySortDirection)}>
                        <TableSortButton
                          label="策略名"
                          isActive={ruleSummarySortKey === "rule_name" && ruleSummarySortDirection !== null}
                          direction={ruleSummarySortDirection}
                          onClick={() => toggleRuleSummarySort("rule_name")}
                          title="按策略名排序"
                        />
                      </th>
                      <th aria-sort={getAriaSort(ruleSummarySortKey === "point_count", ruleSummarySortDirection)}>
                        <TableSortButton
                          label="有效交易日"
                          isActive={ruleSummarySortKey === "point_count" && ruleSummarySortDirection !== null}
                          direction={ruleSummarySortDirection}
                          onClick={() => toggleRuleSummarySort("point_count")}
                          title="按有效交易日排序"
                        />
                      </th>
                      <th aria-sort={getAriaSort(ruleSummarySortKey === "avg_residual_mean", ruleSummarySortDirection)}>
                        <TableSortButton
                          label="残差均值（日度）"
                          isActive={ruleSummarySortKey === "avg_residual_mean" && ruleSummarySortDirection !== null}
                          direction={ruleSummarySortDirection}
                          onClick={() => toggleRuleSummarySort("avg_residual_mean")}
                          title="按残差均值排序"
                        />
                      </th>
                      <th aria-sort={getAriaSort(ruleSummarySortKey === "spread_mean", ruleSummarySortDirection)}>
                        <TableSortButton
                          label="分层差均值"
                          isActive={ruleSummarySortKey === "spread_mean" && ruleSummarySortDirection !== null}
                          direction={ruleSummarySortDirection}
                          onClick={() => toggleRuleSummarySort("spread_mean")}
                          title="按分层差均值排序"
                        />
                      </th>
                      <th aria-sort={getAriaSort(ruleSummarySortKey === "ic_mean", ruleSummarySortDirection)}>
                        <TableSortButton
                          label="IC 均值"
                          isActive={ruleSummarySortKey === "ic_mean" && ruleSummarySortDirection !== null}
                          direction={ruleSummarySortDirection}
                          onClick={() => toggleRuleSummarySort("ic_mean")}
                          title="按 IC 均值排序"
                        />
                      </th>
                      <th aria-sort={getAriaSort(ruleSummarySortKey === "ic_t_value", ruleSummarySortDirection)}>
                        <TableSortButton
                          label="IC t值"
                          isActive={ruleSummarySortKey === "ic_t_value" && ruleSummarySortDirection !== null}
                          direction={ruleSummarySortDirection}
                          onClick={() => toggleRuleSummarySort("ic_t_value")}
                          title="按 IC t值 排序"
                        />
                      </th>
                      <th aria-sort={getAriaSort(ruleSummarySortKey === "icir", ruleSummarySortDirection)}>
                        <TableSortButton
                          label="ICIR"
                          isActive={ruleSummarySortKey === "icir" && ruleSummarySortDirection !== null}
                          direction={ruleSummarySortDirection}
                          onClick={() => toggleRuleSummarySort("icir")}
                          title="按 ICIR 排序"
                        />
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {sortedRuleSummaries.map((item) => (
                      <tr key={item.rule_name}>
                        <td>{item.rule_name}</td>
                        <td>{item.point_count}</td>
                        <td>{formatPercent(item.avg_residual_mean)}</td>
                        <td>{formatPercent(item.spread_mean)}</td>
                        <td className={metricHighlightClass("ic", item.ic_mean)}>{formatNumber(item.ic_mean)}</td>
                        <td className={metricHighlightClass("t", item.ic_t_value)}>{formatNumber(item.ic_t_value)}</td>
                        <td className={metricHighlightClass("ir", item.icir)}>{formatNumber(item.icir)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          ) : null}
        </section>
      ) : null}

      <section className="scene-layer-card">
        <h2 className="scene-layer-title">表达式验证</h2>
        <p className="scene-layer-caption">
          默认空白模板；选择策略后自动带入表达式与参数，后续可继续手动调整并展开参数组合验证。
        </p>

        <div className="scene-layer-form-grid">
          <label className="scene-layer-field">
            <span>策略（来自策略编辑）</span>
            <select
              value={validationImportRuleName}
              onChange={(event) => applyValidationRule(event.target.value)}
            >
              <option value="">请选择策略</option>
              {strategyRuleOptions.map((item) => (
                <option key={item.name} value={item.name}>
                  {item.name}
                </option>
              ))}
            </select>
          </label>
          <label className="scene-layer-field">
            <span>方向</span>
            <select
              value={validationDirection}
              onChange={(event) => setValidationDirection(event.target.value as ValidationDirection)}
            >
              <option value="positive">正向</option>
              <option value="negative">负向</option>
            </select>
          </label>
          <label className="scene-layer-field">
            <span>scope_way</span>
            <select
              value={validationScopeWay}
              onChange={(event) => setValidationScopeWay(event.target.value as ValidationScopeWayOption)}
            >
              {VALIDATION_SCOPE_WAY_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
          {validationScopeWay === "CONSEC" ? (
            <label className="scene-layer-field">
              <span>CONSEC 阈值</span>
              <input
                type="number"
                min={1}
                step={1}
                value={validationConsecThresholdText}
                onChange={(event) => setValidationConsecThresholdText(event.target.value)}
              />
            </label>
          ) : null}
          <label className="scene-layer-field">
            <span>scope_windows</span>
            <input
              type="number"
              min={1}
              step={1}
              value={validationScopeWindowsText}
              onChange={(event) => setValidationScopeWindowsText(event.target.value)}
            />
          </label>
          <label className="scene-layer-field">
            <span>样本展示上限/组</span>
            <input
              type="number"
              min={1}
              max={VALIDATION_MAX_SAMPLE_LIMIT}
              step={1}
              value={validationSampleLimitText}
              onChange={(event) => setValidationSampleLimitText(event.target.value)}
            />
          </label>
        </div>

        <label className="scene-layer-field scene-layer-field-span-full">
          <span>表达式</span>
          <textarea
            rows={6}
            value={validationExpression}
            onChange={(event) => setValidationExpression(event.target.value)}
            placeholder="例如: C > REF(C, N) and V > MA(V, M)"
          />
        </label>

        <div className="scene-layer-validation-unknown-block">
          <div className="scene-layer-validation-unknown-toolbar">
            <label className="scene-layer-validation-checkbox">
              <input
                type="checkbox"
                checked={validationEnableUnknown}
                onChange={(event) => {
                  const checked = event.target.checked;
                  setValidationEnableUnknown(checked);
                  if (checked) {
                    setValidationUnknownConfigs((current) =>
                      hasValidUnknownConfig(current)
                        ? current
                        : inferUnknownConfigs(validationExpression),
                    );
                  } else {
                    setValidationUnknownConfigs([]);
                  }
                }}
              />
              <span>启用未知数</span>
            </label>
            {validationEnableUnknown ? (
              <div className="scene-layer-validation-unknown-actions">
                <button
                  type="button"
                  className="scene-layer-secondary-btn"
                  onClick={() => setValidationUnknownConfigs(inferUnknownConfigs(validationExpression))}
                >
                  自动填入未知数
                </button>
                <button
                  type="button"
                  className="scene-layer-secondary-btn"
                  onClick={() =>
                    setValidationUnknownConfigs((current) => [
                      ...current,
                      buildEmptyUnknownConfig(),
                    ])
                  }
                >
                  + 增加未知数
                </button>
              </div>
            ) : null}
          </div>

          {validationEnableUnknown ? (
            <div className="scene-layer-validation-unknown-list">
              {validationUnknownConfigs.map((item, index) => (
                <div key={`validation-unknown-${index}`} className="scene-layer-validation-unknown-row">
                  <label className="scene-layer-field">
                    <span>变量名</span>
                    <input
                      value={item.name}
                      onChange={(event) =>
                        setValidationUnknownConfigs((current) =>
                          current.map((config, configIndex) =>
                            configIndex === index
                              ? { ...config, name: event.target.value }
                              : config,
                          ),
                        )
                      }
                    />
                  </label>
                  <label className="scene-layer-field">
                    <span>起始</span>
                    <input
                      type="number"
                      step="any"
                      value={item.start}
                      onChange={(event) =>
                        setValidationUnknownConfigs((current) =>
                          current.map((config, configIndex) =>
                            configIndex === index
                              ? { ...config, start: event.target.value }
                              : config,
                          ),
                        )
                      }
                    />
                  </label>
                  <label className="scene-layer-field">
                    <span>结束</span>
                    <input
                      type="number"
                      step="any"
                      value={item.end}
                      onChange={(event) =>
                        setValidationUnknownConfigs((current) =>
                          current.map((config, configIndex) =>
                            configIndex === index
                              ? { ...config, end: event.target.value }
                              : config,
                          ),
                        )
                      }
                    />
                  </label>
                  <label className="scene-layer-field">
                    <span>步长</span>
                    <input
                      type="number"
                      step="any"
                      value={item.step}
                      onChange={(event) =>
                        setValidationUnknownConfigs((current) =>
                          current.map((config, configIndex) =>
                            configIndex === index
                              ? { ...config, step: event.target.value }
                              : config,
                          ),
                        )
                      }
                    />
                  </label>
                  <button
                    type="button"
                    className="scene-layer-secondary-btn scene-layer-validation-unknown-remove"
                    onClick={() =>
                      setValidationUnknownConfigs((current) =>
                        current.length <= 1
                          ? [buildEmptyUnknownConfig()]
                          : current.filter((_, configIndex) => configIndex !== index),
                      )
                    }
                  >
                    删除
                  </button>
                </div>
              ))}
            </div>
          ) : null}
        </div>

        <div className="scene-layer-actions">
          <button
            type="button"
            className="scene-layer-primary-btn"
            onClick={() => void onRunRuleExpressionValidation()}
            disabled={validationLoading || initializing}
          >
            {validationLoading ? "验证中..." : "执行表达式验证"}
          </button>
        </div>

        {validationError ? <div className="scene-layer-error">{validationError}</div> : null}
      </section>

      {validationResult ? (
        <section className="scene-layer-card">
          <div className="scene-layer-layer-summary">
            <h3>参数组合表现（按分层差 / ICIR 排序）</h3>
            {shouldUseValidationDetailModal ? (
              <p className="scene-layer-validation-table-hint">
                保留基础统计表；点击“策略参数”列可在浮窗中查看样本与相似度明细。
              </p>
            ) : shouldUseInlineComboSelection ? (
              <p className="scene-layer-validation-table-hint">点击任意组合行，切换下方详情内容。</p>
            ) : null}
            <div className="scene-layer-contrib-table-wrap">
              <table className="scene-layer-contrib-table scene-layer-validation-table">
                <thead>
                  <tr>
                    <th>组合</th>
                    <th>策略参数</th>
                    <th>触发样本</th>
                    <th>触发交易日</th>
                    <th>平均每日触发</th>
                    <th>残差均值（日度）</th>
                    <th>分层差均值</th>
                    <th>IC 均值</th>
                    <th>IC t值</th>
                    <th>ICIR</th>
                  </tr>
                </thead>
                <tbody>
                  {validationResult.combo_results.map((item) => {
                    const isActive = selectedValidationCombo?.combo_key === item.combo_key;
                    const rowClassName = [
                      isActive ? "scene-layer-validation-row-active" : "",
                      shouldUseInlineComboSelection
                        ? "scene-layer-validation-row-selectable"
                        : "",
                    ]
                      .filter((name) => name.length > 0)
                      .join(" ");
                    const unknownValueText = formatUnknownValuesForCombo(item);
                    return (
                      <tr
                        key={item.combo_key}
                        className={rowClassName || undefined}
                        onClick={
                          shouldUseInlineComboSelection
                            ? () => setValidationSelectedComboKey(item.combo_key)
                            : undefined
                        }
                      >
                        <td>
                          <strong>{item.combo_label}</strong>
                        </td>
                        <td>
                          {shouldUseValidationDetailModal ? (
                            <button
                              type="button"
                              className="scene-layer-validation-detail-link"
                              title={unknownValueText}
                              onClick={() => openValidationDetail(item.combo_key)}
                            >
                              {unknownValueText}
                            </button>
                          ) : (
                            <span className="scene-layer-validation-params-text" title={unknownValueText}>
                              {unknownValueText}
                            </span>
                          )}
                        </td>
                        <td>{item.trigger_samples}</td>
                        <td>{item.triggered_days}</td>
                        <td>{formatNumber(item.avg_daily_trigger, 2)}</td>
                        <td>{formatPercent(item.backtest.avg_residual_mean)}</td>
                        <td>{formatPercent(item.backtest.spread_mean)}</td>
                        <td className={metricHighlightClass("ic", item.backtest.ic_mean)}>{formatNumber(item.backtest.ic_mean)}</td>
                        <td className={metricHighlightClass("t", item.backtest.ic_t_value)}>{formatNumber(item.backtest.ic_t_value)}</td>
                        <td className={metricHighlightClass("ir", item.backtest.icir)}>{formatNumber(item.backtest.icir)}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>

          {!shouldUseValidationDetailModal && selectedValidationCombo ? (
            <>
              {shouldUseInlineComboSelection ? (
                <div className="scene-layer-layer-summary">
                  <h3>选中组合：{selectedValidationCombo.combo_label}</h3>
                </div>
              ) : null}
              {renderValidationComboDetailSections(selectedValidationCombo)}
            </>
          ) : null}

          {shouldUseValidationDetailModal && validationDetailModalOpen && selectedValidationCombo ? (
            <div className="scene-layer-modal-mask" onClick={closeValidationDetailModal}>
              <div
                className="scene-layer-modal-card scene-layer-validation-detail-modal"
                role="dialog"
                aria-modal="true"
                aria-label={`参数组合详情：${selectedValidationCombo.combo_label}`}
                onClick={(event) => event.stopPropagation()}
              >
                <div className="scene-layer-modal-header">
                  <h3>参数组合详情：{selectedValidationCombo.combo_label}</h3>
                  <button type="button" className="scene-layer-modal-close" onClick={closeValidationDetailModal}>
                    关闭
                  </button>
                </div>
                <div className="scene-layer-modal-scroll-body">
                  {renderValidationComboDetailSections(selectedValidationCombo, true)}
                </div>
              </div>
            </div>
          ) : null}

          {validationSamplesModalOpen && validationResult && selectedValidationCombo ? (
            <div className="scene-layer-modal-mask" onClick={closeValidationSamplesModal}>
              <div
                className="scene-layer-modal-card scene-layer-validation-samples-modal"
                role="dialog"
                aria-modal="true"
                aria-label={`触发样本详情：${selectedValidationCombo.combo_label}`}
                onClick={(event) => event.stopPropagation()}
              >
                <div className="scene-layer-modal-header">
                  <h3>触发样本详情：{selectedValidationCombo.combo_label}</h3>
                  <button
                    type="button"
                    className="scene-layer-modal-close"
                    onClick={closeValidationSamplesModal}
                  >
                    关闭
                  </button>
                </div>
                <div className="scene-layer-modal-scroll-body">
                  <ExpressionValidationSamplesPanel
                    data={{
                      importRuleName: validationResult.import_rule_name,
                      importRuleExplain: validationResult.import_rule_explain,
                      expression: validationExpression,
                      combo: selectedValidationCombo,
                      comboParamSummary: formatUnknownValuesForCombo(selectedValidationCombo),
                      sampleLimitPerGroup: validationResult.sample_limit_per_group,
                      sourcePath,
                    }}
                    layout="modal"
                  />
                </div>
              </div>
            </div>
          ) : null}
        </section>
      ) : null}
    </div>
  );
}
