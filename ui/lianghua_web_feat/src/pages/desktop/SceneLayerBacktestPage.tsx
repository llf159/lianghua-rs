import { useEffect, useMemo, useState } from "react";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import { getStrategyManagePage, type StrategyManageRuleItem } from "../../apis/strategyManage";
import {
  getRuleLayerBacktestDefaults,
  runRuleExpressionValidation,
  getSceneLayerBacktestDefaults,
  runRuleLayerBacktest,
  runSceneLayerBacktest,
  type RuleExpressionValidationData,
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
import DetailsLink from "../../shared/DetailsLink";
import { readStoredSourcePath } from "../../shared/storage";
import "./css/SceneLayerBacktestPage.css";

type RuleSummarySortKey =
  | "rule_name"
  | "point_count"
  | "avg_residual_mean"
  | "spread_mean"
  | "ic_mean"
  | "ic_std"
  | "icir";

type ValidationScopeWayOption = "ANY" | "LAST" | "EACH" | "RECENT" | "CONSEC";

type ValidationSampleGroupKey = "positive" | "negative" | "random";

const VALIDATION_DEFAULT_SAMPLE_LIMIT = 30;
const VALIDATION_MAX_SAMPLE_LIMIT = 200;

const VALIDATION_SAMPLE_GROUP_META: Array<{ key: ValidationSampleGroupKey; title: string }> = [
  { key: "positive", title: "正样本" },
  { key: "negative", title: "负样本" },
  { key: "random", title: "随机样本" },
];

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

function buildEmptyUnknownConfig(): RuleValidationUnknownConfig {
  return {
    name: "",
    start: 2,
    end: 20,
    step: 2,
  };
}

function hasValidUnknownConfig(configs: RuleValidationUnknownConfig[]): boolean {
  return configs.some((item) => item.name.trim().length > 0);
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

function inferUnknownConfigs(expression: string): RuleValidationUnknownConfig[] {
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
    start: 2,
    end: 20,
    step: 2,
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
  const [sourcePath, setSourcePath] = useState(() => readStoredSourcePath());
  const [stockAdjType, setStockAdjType] = useState("qfq");
  const [indexTsCode, setIndexTsCode] = useState<string>(INDEX_OPTIONS[0].value);
  const [indexBeta, setIndexBeta] = useState("0.5");
  const [conceptBeta, setConceptBeta] = useState("0.2");
  const [industryBeta, setIndustryBeta] = useState("0.0");
  const [startDateInput, setStartDateInput] = useState("");
  const [endDateInput, setEndDateInput] = useState("");
  const [minSamplesPerDay, setMinSamplesPerDay] = useState("5");
  const [backtestPeriod, setBacktestPeriod] = useState("1");

  const [loading, setLoading] = useState(false);
  const [initializing, setInitializing] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState<SceneLayerBacktestData | null>(null);

  const [ruleLoading, setRuleLoading] = useState(false);
  const [ruleError, setRuleError] = useState("");
  const [ruleResult, setRuleResult] = useState<RuleLayerBacktestData | null>(null);

  const [strategyRuleOptions, setStrategyRuleOptions] = useState<StrategyManageRuleItem[]>([]);
  const [validationImportRuleName, setValidationImportRuleName] = useState("");
  const [validationExpression, setValidationExpression] = useState("");
  const [validationScopeWay, setValidationScopeWay] = useState<ValidationScopeWayOption>("ANY");
  const [validationConsecThresholdText, setValidationConsecThresholdText] = useState("2");
  const [validationScopeWindowsText, setValidationScopeWindowsText] = useState("1");
  const [validationEnableUnknown, setValidationEnableUnknown] = useState(false);
  const [validationUnknownConfigs, setValidationUnknownConfigs] = useState<
    RuleValidationUnknownConfig[]
  >([]);
  const [validationSampleLimitText, setValidationSampleLimitText] = useState(
    String(VALIDATION_DEFAULT_SAMPLE_LIMIT),
  );
  const [validationLoading, setValidationLoading] = useState(false);
  const [validationError, setValidationError] = useState("");
  const [validationResult, setValidationResult] = useState<RuleExpressionValidationData | null>(null);
  const [validationSelectedComboKey, setValidationSelectedComboKey] = useState("");

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
          if (sceneDefaults.start_date && sceneDefaults.end_date) {
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
          if (!hasSceneDates) {
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
  }, []);

  const allSceneSummaries = result?.all_scene_summaries ?? [];
  const allRuleSummaries = ruleResult?.all_rule_summaries ?? [];

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

  useEffect(() => {
    if (!validationResult) {
      setValidationSelectedComboKey("");
      return;
    }
    const preferred =
      validationResult.best_combo_key?.trim() ||
      validationResult.combo_results[0]?.combo_key ||
      "";
    setValidationSelectedComboKey(preferred);
  }, [validationResult]);

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
        ic_std: {
          value: (row: RuleLayerRuleSummary) => row.ic_std,
        },
        icir: {
          value: (row: RuleLayerRuleSummary) => row.icir,
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
        backtestPeriod: Math.max(1, Number(backtestPeriod) || 1),
      });
      setRuleResult(data);
    } catch (runError) {
      setRuleResult(null);
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
            start: Number(item.start),
            end: Number(item.end),
            step: Number(item.step),
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
    const distPoints = selectedRule?.dist_points?.length
      ? selectedRule.dist_points.map((item) => ({
          min: Number(item.min),
          max: Number(item.max),
          points: Number(item.points),
        }))
      : undefined;

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
          points: Number.isFinite(selectedRule?.points) ? Number(selectedRule?.points) : 1,
          distPoints,
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

  return (
    <div className="scene-layer-page">
      <section className="scene-layer-card">
        <h2 className="scene-layer-title">回测全局参数</h2>
        <div className="scene-layer-source-note">
          当前数据目录：<strong>{sourcePath || "--"}</strong>
        </div>

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
            <span>回测周期（天）</span>
            <input type="number" min="1" value={backtestPeriod} onChange={(event) => setBacktestPeriod(event.target.value)} />
          </label>
        </div>
      </section>

      <section className="scene-layer-card">
        <h2 className="scene-layer-title">场景整体回测</h2>
        <p className="scene-layer-caption">
          使用 scene_details 中的场景状态与排序，计算各场景状态下的分层残差收益、Top-Bottom Spread、IC / ICIR。
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
              <span>回测周期（天）</span>
              <strong>{result.backtest_period}</strong>
            </div>
          </div>

          {allSceneSummaries.length === 0 ? (
            <div className="scene-layer-empty">当前没有可回测的场景。</div>
          ) : null}

          {allSceneSummaries.length > 0 ? (
            <div className="scene-layer-layer-summary">
              <h3>全部场景汇总（按 Spread 均值降序）</h3>
              <div className="scene-layer-layer-grid">
                {allSceneSummaries.map((item) => (
                  <div key={item.scene_name} className="scene-layer-layer-item">
                    <span className="scene-layer-layer-state">{item.scene_name}</span>
                    <span>有效交易日：{item.point_count}</span>
                    <span>Spread 均值：{formatPercent(item.spread_mean)}</span>
                    <span>IC 均值：{formatNumber(item.ic_mean)}</span>
                    <span>ICIR：{formatNumber(item.icir)}</span>
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
          使用 rule_details 中的策略得分与残差收益，计算策略日度均值、Top-Bottom Spread、IC / ICIR。
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
                    <th>回测周期（天）</th>
                    <th>残差均值（日度）</th>
                    <th>Spread 均值（日度高分-低分）</th>
                    <th>IC 均值</th>
                    <th>IC 标准差</th>
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
                    <td>{ruleResult.backtest_period}</td>
                    <td>{formatPercent(ruleResult.avg_residual_mean)}</td>
                    <td>{formatPercent(ruleResult.spread_mean)}</td>
                    <td>{formatNumber(ruleResult.ic_mean)}</td>
                    <td>{formatNumber(ruleResult.ic_std)}</td>
                    <td>{formatNumber(ruleResult.icir)}</td>
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
                          label="Spread 均值"
                          isActive={ruleSummarySortKey === "spread_mean" && ruleSummarySortDirection !== null}
                          direction={ruleSummarySortDirection}
                          onClick={() => toggleRuleSummarySort("spread_mean")}
                          title="按 Spread 均值排序"
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
                      <th aria-sort={getAriaSort(ruleSummarySortKey === "ic_std", ruleSummarySortDirection)}>
                        <TableSortButton
                          label="IC 标准差"
                          isActive={ruleSummarySortKey === "ic_std" && ruleSummarySortDirection !== null}
                          direction={ruleSummarySortDirection}
                          onClick={() => toggleRuleSummarySort("ic_std")}
                          title="按 IC 标准差排序"
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
                        <td>{formatNumber(item.ic_mean)}</td>
                        <td>{formatNumber(item.ic_std)}</td>
                        <td>{formatNumber(item.icir)}</td>
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
                              ? { ...config, start: Number(event.target.value) }
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
                              ? { ...config, end: Number(event.target.value) }
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
                              ? { ...config, step: Number(event.target.value) }
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
            <h3>参数组合表现（按 Spread / ICIR 排序）</h3>
            <div className="scene-layer-contrib-table-wrap">
              <table className="scene-layer-contrib-table scene-layer-validation-table">
                <thead>
                  <tr>
                    <th>组合</th>
                    <th>未知数</th>
                    <th>触发样本</th>
                    <th>触发交易日</th>
                    <th>平均每日触发</th>
                    <th>残差均值（日度）</th>
                    <th>Spread 均值</th>
                    <th>IC 均值</th>
                    <th>ICIR</th>
                  </tr>
                </thead>
                <tbody>
                  {validationResult.combo_results.map((item) => {
                    const isActive = selectedValidationCombo?.combo_key === item.combo_key;
                    return (
                      <tr
                        key={item.combo_key}
                        className={isActive ? "scene-layer-validation-row-active" : undefined}
                        onClick={() => setValidationSelectedComboKey(item.combo_key)}
                      >
                        <td>
                          <strong>{item.combo_label}</strong>
                        </td>
                        <td>
                          {item.unknown_values.length > 0
                            ? item.unknown_values
                                .map((unknown) => `${unknown.name}=${formatNumber(unknown.value, 4)}`)
                                .join(", ")
                            : "默认参数"}
                        </td>
                        <td>{item.trigger_samples}</td>
                        <td>{item.triggered_days}</td>
                        <td>{formatNumber(item.avg_daily_trigger, 2)}</td>
                        <td>{formatPercent(item.backtest.avg_residual_mean)}</td>
                        <td>{formatPercent(item.backtest.spread_mean)}</td>
                        <td>{formatNumber(item.backtest.ic_mean)}</td>
                        <td>{formatNumber(item.backtest.icir)}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>

          {selectedValidationCombo ? (
            <>
              <div className="scene-layer-layer-summary">
                <h3>选中组合：{selectedValidationCombo.combo_label}</h3>
                <div className="scene-layer-formula-box">
                  <strong>替换后表达式</strong>
                  <p>{selectedValidationCombo.formula || "--"}</p>
                </div>
              </div>

              <div className="scene-layer-layer-summary">
                <h3>
                  触发样本（每组展示 {validationResult.sample_limit_per_group} 条，可在上方调整）
                </h3>
                <div className="scene-layer-validation-sample-stats">
                  <span>总样本：{selectedValidationCombo.sample_stats.total_samples}</span>
                  <span>正样本：{selectedValidationCombo.sample_stats.positive_count}</span>
                  <span>负样本：{selectedValidationCombo.sample_stats.negative_count}</span>
                  <span>随机池：{selectedValidationCombo.sample_stats.random_count}</span>
                </div>

                <div className="scene-layer-validation-sample-groups">
                  {VALIDATION_SAMPLE_GROUP_META.map((group) => {
                    const rows = selectedValidationCombo.sample_groups[group.key];
                    const totalCount =
                      group.key === "positive"
                        ? selectedValidationCombo.sample_stats.positive_count
                        : group.key === "negative"
                          ? selectedValidationCombo.sample_stats.negative_count
                          : selectedValidationCombo.sample_stats.random_count;
                    return (
                      <div key={group.key} className="scene-layer-validation-sample-group">
                        <h4>
                          {group.title}
                          <span>
                            {rows.length} / {totalCount}
                          </span>
                        </h4>
                        <div className="scene-layer-contrib-table-wrap">
                          <table className="scene-layer-contrib-table scene-layer-validation-sample-table">
                            <thead>
                              <tr>
                                <th>代码</th>
                                <th>名称</th>
                                <th>交易日</th>
                                <th>触发得分</th>
                                <th>残差收益</th>
                              </tr>
                            </thead>
                            <tbody>
                              {rows.length > 0 ? (
                                rows.map((row) => (
                                  <tr key={`${group.key}-${row.ts_code}-${row.trade_date}`}>
                                    <td>{row.ts_code}</td>
                                    <td>
                                      <DetailsLink
                                        className="scene-layer-validation-sample-link"
                                        tsCode={row.ts_code}
                                        tradeDate={row.trade_date}
                                        sourcePath={sourcePath.trim()}
                                      >
                                        {row.name?.trim() || row.ts_code}
                                      </DetailsLink>
                                    </td>
                                    <td>{formatDateLabel(row.trade_date)}</td>
                                    <td>{formatNumber(row.rule_score, 4)}</td>
                                    <td>{formatPercent(row.residual_return, 3)}</td>
                                  </tr>
                                ))
                              ) : (
                                <tr>
                                  <td colSpan={5}>暂无样本。</td>
                                </tr>
                              )}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>

              <div className="scene-layer-layer-summary">
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
                      {selectedValidationCombo.similarity_rows.length > 0 ? (
                        selectedValidationCombo.similarity_rows.map((row) => (
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
          ) : null}
        </section>
      ) : null}
    </div>
  );
}
