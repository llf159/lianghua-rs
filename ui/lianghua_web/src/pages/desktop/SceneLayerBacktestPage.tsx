import { useEffect, useMemo, useState } from "react";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import { getStrategyManagePage, type StrategyManageRuleItem } from "../../apis/strategyManage";
import {
  getCachedRuleLayerBacktestDetail,
  getRuleLayerBacktestDefaults,
  runRankLayerBacktest,
  runTransientRankLayerBacktest,
  runTransientRuleLayerBacktest,
  runTransientSceneLayerBacktest,
  getSceneLayerBacktestDefaults,
  runRuleLayerBacktest,
  runSceneLayerBacktest,
  type RankLayerBacktestData,
  type RankLayerMethod,
  type RankLayerSampleGroup,
  type RuleValidationComboResult,
  type RuleDecayValidation,
  type RuleLayerBacktestData,
  type RuleLayerRuleSummary,
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
import { useConceptExclusions } from "../../shared/conceptExclusions";
import {
  STOCK_PICK_BOARD_OPTIONS,
  buildBoardFilterOptions,
} from "../../shared/stockPickShared";
import {
  compactDateToInput,
  formatDateLabel,
  formatNumber,
  formatPercent,
  formatProfitLossRatio,
  formatRate,
  normalizeDateInput,
  parseOptionalNumberInput,
} from "../../shared/backtestFormat";
import {
  INDEX_OPTIONS,
  RANK_LAYER_METHOD_OPTIONS,
  readStoredBacktestCommonParams,
  writeStoredBacktestCommonParams,
} from "../../shared/backtestCommonParams";
import {
  VALIDATION_DEFAULT_SAMPLE_LIMIT,
  compactRuleLayerBacktestPayload,
  resolveResidualDirection,
  type ExpressionValidationContext,
  type ValidationDirection,
} from "../../shared/expressionValidation";
import { ExpressionValidationSamplesPanel } from "./ExpressionValidationSamplesPage";
import { ExpressionCoreSummary } from "./components/expressionBacktest/ExpressionCoreSummary";
import { ExpressionDailyDetail } from "./components/expressionBacktest/ExpressionDailyDetail";
import { ExpressionIncremental } from "./components/expressionBacktest/ExpressionIncremental";
import { ExpressionLayerResult } from "./components/expressionBacktest/ExpressionLayerResult";
import { ExpressionSamplesCheck } from "./components/expressionBacktest/ExpressionSamplesCheck";
import { ExpressionSimilarity } from "./components/expressionBacktest/ExpressionSimilarity";
import { ExpressionWalkForward } from "./components/expressionBacktest/ExpressionWalkForward";
import "./css/SceneLayerBacktestPage.css";
import "./css/ExpressionBacktestPage.css";

type RuleSummarySortKey =
  | "rule_name"
  | "point_count"
  | "profit_loss_ratio"
  | "avg_excess_residual_mean"
  | "avg_er_change"
  | "avg_contribution_score"
  | "avg_contribution_per_trigger"
  | "decay_20"
  | "ic_mean"
  | "icir"
  | "ic_t_value";

const RANK_LAYER_SAMPLE_LIMIT_PER_GROUP = 5;

function getRuleDecayValidation(
  row: { decay_validations?: RuleDecayValidation[] | null },
  windowDays: number,
): RuleDecayValidation | undefined {
  return (row.decay_validations ?? []).find((item) => item.window_days === windowDays);
}

function decayStatusClass(status?: string | null) {
  switch (status) {
    case "significant_decay":
    case "decay":
      return "scene-layer-decay-status-danger";
    case "weakening":
    case "weak":
      return "scene-layer-decay-status-warning";
    case "improving":
      return "scene-layer-decay-status-improving";
    case "stable":
      return "scene-layer-decay-status-stable";
    default:
      return "scene-layer-decay-status-insufficient";
  }
}

function formatDecayValidationTitle(
  item: RuleDecayValidation,
  valueLabel = "方向超额",
) {
  const dateRange = item.recent_start_date && item.recent_end_date
    ? `${formatDateLabel(item.recent_start_date)} ~ ${formatDateLabel(item.recent_end_date)}`
    : "--";
  return [
    `最近窗口：${dateRange}`,
    `近期${valueLabel}：${formatPercent(item.recent_directional_excess_mean, 4)}`,
    `此前${valueLabel}：${formatPercent(item.prior_directional_excess_mean, 4)}`,
    `变化：${formatPercent(item.decay_change, 4)}`,
    `Welch t值：${formatNumber(item.decay_t_value, 3)}`,
    `有效日：近期${item.recent_day_count} / 此前${item.prior_day_count}`,
  ].join("；");
}

function renderDecayValidations(
  validations: RuleDecayValidation[] | undefined,
  keyPrefix: string,
  valueLabel?: string,
) {
  const rows = validations ?? [];
  if (rows.length === 0) {
    return "--";
  }
  return (
    <div className="scene-layer-decay-list">
      {rows.map((item) => (
        <div
          key={`${keyPrefix}-decay-${item.window_days}`}
          className={`scene-layer-decay-item ${decayStatusClass(item.status)}`}
          title={formatDecayValidationTitle(item, valueLabel)}
        >
          <span>{item.window_days}日</span>
          <strong>{item.status_label}</strong>
          <em>
            近 {formatPercent(item.recent_directional_excess_mean, 2)} / Δ {formatPercent(item.decay_change, 2)}
          </em>
        </div>
      ))}
    </div>
  );
}

function renderRuleDecayValidations(row: RuleLayerRuleSummary) {
  const validations = row.decay_validations ?? [];
  if (validations.length === 0) {
    return "--";
  }
  return renderDecayValidations(validations, row.rule_name);
}

function formatBacktestBoardLabel(value?: {
  resolved_board?: string | null;
  exclude_st_board?: boolean | null;
}) {
  return [value?.resolved_board ?? "不限", value?.exclude_st_board ? "排除ST" : ""]
    .filter(Boolean)
    .join(" / ");
}

function formatMarketValueRange(value?: {
  total_mv_min?: number | null;
  total_mv_max?: number | null;
}) {
  const minValue = value?.total_mv_min;
  const maxValue = value?.total_mv_max;
  if (
    (minValue === null || minValue === undefined || !Number.isFinite(minValue)) &&
    (maxValue === null || maxValue === undefined || !Number.isFinite(maxValue))
  ) {
    return "不限";
  }
  const minText = minValue !== null && minValue !== undefined && Number.isFinite(minValue)
    ? `${formatNumber(minValue, 0)}亿`
    : "-∞";
  const maxText = maxValue !== null && maxValue !== undefined && Number.isFinite(maxValue)
    ? `${formatNumber(maxValue, 0)}亿`
    : "+∞";
  return `${minText} ~ ${maxText}`;
}

export default function SceneLayerBacktestPage() {
  const { excludeStBoard } = useConceptExclusions();
  const storedCommonParams = useMemo(() => readStoredBacktestCommonParams(), []);
  const [sourcePath, setSourcePath] = useState(() => readStoredSourcePath());
  const [stockAdjType, setStockAdjType] = useState(storedCommonParams.stockAdjType);
  const [indexTsCode, setIndexTsCode] = useState<string>(storedCommonParams.indexTsCode);
  const [indexBeta, setIndexBeta] = useState(storedCommonParams.indexBeta);
  const [conceptBeta, setConceptBeta] = useState(storedCommonParams.conceptBeta);
  const [industryBeta, setIndustryBeta] = useState(storedCommonParams.industryBeta);
  const [startDateInput, setStartDateInput] = useState(storedCommonParams.startDateInput);
  const [endDateInput, setEndDateInput] = useState(storedCommonParams.endDateInput);
  const [minSamplesPerDay, setMinSamplesPerDay] = useState(storedCommonParams.minSamplesPerDay);
  const [minListedTradeDays, setMinListedTradeDays] = useState(storedCommonParams.minListedTradeDays);
  const [backtestPeriod, setBacktestPeriod] = useState(storedCommonParams.backtestPeriod);
  const [parallelBatchSize, setParallelBatchSize] = useState(storedCommonParams.parallelBatchSize);
  const [totalMvMin, setTotalMvMin] = useState(storedCommonParams.totalMvMin);
  const [totalMvMax, setTotalMvMax] = useState(storedCommonParams.totalMvMax);
  const [rankLayerCount, setRankLayerCount] = useState(storedCommonParams.rankLayerCount);
  const [rankLayerMethod, setRankLayerMethod] = useState<RankLayerMethod>(
    storedCommonParams.rankLayerMethod,
  );
  const [backtestBoardFilter, setBacktestBoardFilter] = useState<
    (typeof STOCK_PICK_BOARD_OPTIONS)[number]
  >(storedCommonParams.backtestBoardFilter);

  const [loading, setLoading] = useState(false);
  const [initializing, setInitializing] = useState(false);
  const [error, setError] = useState("");
  const [rankLoading, setRankLoading] = useState(false);
  const [rankTransientLoading, setRankTransientLoading] = useState(false);
  const [rankError, setRankError] = useState("");
  const [rankResult, setRankResult] = useState<RankLayerBacktestData | null>(null);
  const [rankLayerSampleModal, setRankLayerSampleModal] = useState<RankLayerSampleGroup | null>(null);
  const [result, setResult] = useState<SceneLayerBacktestData | null>(null);
  const [transientLoading, setTransientLoading] = useState(false);

  const [ruleLoading, setRuleLoading] = useState(false);
  const [ruleTransientLoading, setRuleTransientLoading] = useState(false);
  const [ruleError, setRuleError] = useState("");
  const [ruleResult, setRuleResult] = useState<RuleLayerBacktestData | null>(null);
  const [strategyRuleOptions, setStrategyRuleOptions] = useState<StrategyManageRuleItem[]>([]);
  const [ruleDetailCombo, setRuleDetailCombo] = useState<RuleValidationComboResult | null>(null);
  const [ruleDetailContext, setRuleDetailContext] = useState<ExpressionValidationContext | null>(
    null,
  );
  const [ruleDetailLoadingName, setRuleDetailLoadingName] = useState("");
  const heavyTaskRunning =
    loading ||
    transientLoading ||
    rankLoading ||
    rankTransientLoading ||
    ruleLoading ||
    ruleTransientLoading;
  const backtestBoardOptions = useMemo(
    () => buildBoardFilterOptions(STOCK_PICK_BOARD_OPTIONS, excludeStBoard),
    [excludeStBoard],
  );

  useEffect(() => {
    writeStoredBacktestCommonParams({
      stockAdjType,
      indexTsCode,
      indexBeta,
      conceptBeta,
      industryBeta,
      startDateInput,
      endDateInput,
      minSamplesPerDay,
      minListedTradeDays,
      backtestPeriod,
      parallelBatchSize,
      totalMvMin,
      totalMvMax,
      rankLayerCount,
      rankLayerMethod,
      backtestBoardFilter,
    });
  }, [
    stockAdjType,
    indexTsCode,
    indexBeta,
    conceptBeta,
    industryBeta,
    startDateInput,
    endDateInput,
    minSamplesPerDay,
    minListedTradeDays,
    backtestPeriod,
    parallelBatchSize,
    totalMvMin,
    totalMvMax,
    rankLayerCount,
    rankLayerMethod,
    backtestBoardFilter,
  ]);

  useEffect(() => {
    if (!backtestBoardOptions.includes(backtestBoardFilter)) {
      setBacktestBoardFilter("全部");
    }
  }, [backtestBoardFilter, backtestBoardOptions]);

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

        let hasSceneDateDefaults = false;
        try {
          const sceneDefaults = await getSceneLayerBacktestDefaults(resolved);
          if (cancelled) {
            return;
          }
          hasSceneDateDefaults = Boolean(sceneDefaults.start_date && sceneDefaults.end_date);
          if (sceneDefaults.start_date && !storedCommonParams.startDateInput) {
            setStartDateInput(compactDateToInput(sceneDefaults.start_date));
          }
          if (sceneDefaults.end_date) {
            setEndDateInput(compactDateToInput(sceneDefaults.end_date));
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
          if (!hasSceneDateDefaults && ruleDefaults.start_date && !storedCommonParams.startDateInput) {
            setStartDateInput(compactDateToInput(ruleDefaults.start_date));
          }
          if (!hasSceneDateDefaults && ruleDefaults.end_date) {
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
            setRuleError(`读取策略编辑参数失败: ${String(strategyInitError)}`);
          }
        }
      } catch (initError) {
        if (!cancelled) {
          setError(`读取回测默认参数失败: ${String(initError)}`);
          setRuleError(`读取回测默认参数失败: ${String(initError)}`);
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
  }, [storedCommonParams]);

  const allSceneSummaries = result?.all_scene_summaries ?? [];
  const allRuleSummaries = ruleResult?.all_rule_summaries ?? [];
  const rankLayerSummaries = rankResult?.layer_summaries ?? [];
  const rankTopKSummaries = rankResult?.top_k_summaries ?? [];
  const rankTopKPeriodSummaries = rankResult?.top_k_period_summaries ?? [];
  const rankLayerSampleGroupByIndex = useMemo(() => {
    const groups = rankResult?.layer_sample_groups ?? [];
    return new Map(groups.map((item) => [item.layer_index, item]));
  }, [rankResult]);
  const backtestHighlightSettings = readStoredBacktestHighlightSettings();

  function metricHighlightClass(
    metric: BacktestHighlightMetric,
    value?: number | null,
  ) {
    return shouldHighlightBacktestMetric(metric, value, backtestHighlightSettings)
      ? "scene-layer-metric-hit"
      : undefined;
  }

  function residualMetricHighlightClass(
    value?: number | null,
    direction?: ValidationDirection | null,
  ) {
    const checkedValue = direction === "negative" && value !== null && value !== undefined
      ? -value
      : value;
    return metricHighlightClass("residual", checkedValue);
  }

  function renderResidualMetric(
    value?: number | null,
    direction?: ValidationDirection | null,
  ) {
    const directionLabel = direction === "negative" ? "扣" : direction === "positive" ? "加" : null;
    return (
      <span className="scene-layer-residual-metric">
        <span>{formatPercent(value)}</span>
        {directionLabel ? (
          <span className={`scene-layer-residual-badge scene-layer-residual-badge-${direction}`}>
            {directionLabel}
          </span>
        ) : null}
      </span>
    );
  }

  useEffect(() => {
    if (!rankLayerSampleModal) {
      return;
    }

    const previousOverflow = document.body.style.overflow;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setRankLayerSampleModal(null);
      }
    };

    document.body.style.overflow = "hidden";
    window.addEventListener("keydown", onKeyDown);
    return () => {
      window.removeEventListener("keydown", onKeyDown);
      document.body.style.overflow = previousOverflow;
    };
  }, [rankLayerSampleModal]);

  useEffect(() => {
    if (!ruleDetailCombo) {
      return;
    }

    const previousOverflow = document.body.style.overflow;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        closeRuleDetailModal();
      }
    };

    document.body.style.overflow = "hidden";
    window.addEventListener("keydown", onKeyDown);
    return () => {
      window.removeEventListener("keydown", onKeyDown);
      document.body.style.overflow = previousOverflow;
    };
  }, [ruleDetailCombo]);

  const ruleSummarySortDefinitions = useMemo(
    () =>
      ({
        rule_name: {
          value: (row: RuleLayerRuleSummary) => row.rule_name,
        },
        point_count: {
          value: (row: RuleLayerRuleSummary) => row.point_count,
        },
        profit_loss_ratio: {
          value: (row: RuleLayerRuleSummary) => row.profit_loss_ratio,
        },
        avg_excess_residual_mean: {
          value: (row: RuleLayerRuleSummary) => row.avg_excess_residual_mean,
        },
        avg_er_change: {
          value: (row: RuleLayerRuleSummary) => row.avg_er_change,
        },
        avg_contribution_score: {
          value: (row: RuleLayerRuleSummary) => row.avg_contribution_score,
        },
        avg_contribution_per_trigger: {
          value: (row: RuleLayerRuleSummary) => row.avg_contribution_per_trigger,
        },
        decay_20: {
          value: (row: RuleLayerRuleSummary) =>
            getRuleDecayValidation(row, 20)?.decay_change,
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
      key: "profit_loss_ratio",
      direction: "desc",
    },
  );

  function readManualMarketValueFilter(setMessage: (message: string) => void) {
    const minText = totalMvMin.trim();
    const maxText = totalMvMax.trim();
    const parsedMin = parseOptionalNumberInput(minText);
    const parsedMax = parseOptionalNumberInput(maxText);
    if (minText && parsedMin === undefined) {
      setMessage("总市值最小值必须是数字。");
      return null;
    }
    if (maxText && parsedMax === undefined) {
      setMessage("总市值最大值必须是数字。");
      return null;
    }
    if (parsedMin !== undefined && parsedMax !== undefined && parsedMin > parsedMax) {
      setMessage("总市值最小值不能大于最大值。");
      return null;
    }
    return {
      totalMvMin: parsedMin,
      totalMvMax: parsedMax,
    };
  }

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
    const marketValueFilter = readManualMarketValueFilter(setError);
    if (!marketValueFilter) {
      return;
    }

    setResult(null);
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
        board: backtestBoardFilter === "全部" ? undefined : backtestBoardFilter,
        excludeStBoard: excludeStBoard || undefined,
        ...marketValueFilter,
      });
      setResult(data);
    } catch (runError) {
      setResult(null);
      setError(`执行场景整体回测失败: ${String(runError)}`);
    } finally {
      setLoading(false);
    }
  }

  async function onRunTransientSceneBacktest() {
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
    const marketValueFilter = readManualMarketValueFilter(setError);
    if (!marketValueFilter) {
      return;
    }

    setResult(null);
    setTransientLoading(true);
    setError("");
    try {
      const data = await runTransientSceneLayerBacktest({
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
        board: backtestBoardFilter === "全部" ? undefined : backtestBoardFilter,
        excludeStBoard: excludeStBoard || undefined,
        ...marketValueFilter,
      });
      setResult(data);
    } catch (runError) {
      setResult(null);
      setError(`执行变更策略场景验证失败: ${String(runError)}`);
    } finally {
      setTransientLoading(false);
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
    const normalizedLayerCount = Number(rankLayerCount);
    if (
      !Number.isFinite(normalizedLayerCount) ||
      !Number.isInteger(normalizedLayerCount) ||
      normalizedLayerCount < 2
    ) {
      setRankError("分层层数必须是 >= 2 的整数。");
      return;
    }

    setRankResult(null);
    setRankLayerSampleModal(null);
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
        layerCount: normalizedLayerCount,
        layerMethod: rankLayerMethod,
        board: backtestBoardFilter === "全部" ? undefined : backtestBoardFilter,
        excludeStBoard: excludeStBoard || undefined,
      });
      setRankResult(data);
    } catch (runError) {
      setRankResult(null);
      setRankError(`执行排名整体回测失败: ${String(runError)}`);
    } finally {
      setRankLoading(false);
    }
  }

  async function onRunTransientRankBacktest() {
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
    const normalizedLayerCount = Number(rankLayerCount);
    if (
      !Number.isFinite(normalizedLayerCount) ||
      !Number.isInteger(normalizedLayerCount) ||
      normalizedLayerCount < 2
    ) {
      setRankError("分层层数必须是 >= 2 的整数。");
      return;
    }

    setRankResult(null);
    setRankLayerSampleModal(null);
    setRankTransientLoading(true);
    setRankError("");
    try {
      const data = await runTransientRankLayerBacktest({
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
        layerCount: normalizedLayerCount,
        layerMethod: rankLayerMethod,
        board: backtestBoardFilter === "全部" ? undefined : backtestBoardFilter,
        excludeStBoard: excludeStBoard || undefined,
      });
      setRankResult(data);
    } catch (runError) {
      setRankResult(null);
      setRankError(`执行变更策略排名验证失败: ${String(runError)}`);
    } finally {
      setRankTransientLoading(false);
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
    const marketValueFilter = readManualMarketValueFilter(setRuleError);
    if (!marketValueFilter) {
      return;
    }

    setRuleResult(null);
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
        parallelBatchSize: Math.max(1, Number(parallelBatchSize) || 4),
        board: backtestBoardFilter === "全部" ? undefined : backtestBoardFilter,
        excludeStBoard: excludeStBoard || undefined,
        ...marketValueFilter,
      });
      const compacted = compactRuleLayerBacktestPayload(data);
      setRuleResult(compacted);
    } catch (runError) {
      setRuleResult(null);
      setRuleError(`执行策略回测失败: ${String(runError)}`);
    } finally {
      setRuleLoading(false);
    }
  }

  async function onRunTransientRuleBacktest() {
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
    const marketValueFilter = readManualMarketValueFilter(setRuleError);
    if (!marketValueFilter) {
      return;
    }

    setRuleResult(null);
    setRuleTransientLoading(true);
    setRuleError("");
    try {
      const data = await runTransientRuleLayerBacktest({
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
        parallelBatchSize: Math.max(1, Number(parallelBatchSize) || 4),
        board: backtestBoardFilter === "全部" ? undefined : backtestBoardFilter,
        excludeStBoard: excludeStBoard || undefined,
        ...marketValueFilter,
      });
      const compacted = compactRuleLayerBacktestPayload(data);
      setRuleResult(compacted);
    } catch (runError) {
      setRuleResult(null);
      setRuleError(`执行变更策略回测验证失败: ${String(runError)}`);
    } finally {
      setRuleTransientLoading(false);
    }
  }

  async function openStoredRuleValidationDetail(ruleName: string) {
    const matched = strategyRuleOptions.find((item) => item.name === ruleName);

    if (!matched) {
      setRuleError(`策略 ${ruleName} 没有可用的详细配置。`);
      return;
    }

    const direction: ValidationDirection =
      Number.isFinite(matched.points) && matched.points < 0 ? "negative" : "positive";
    setRuleError("");
    setRuleDetailLoadingName(ruleName);
    try {
      const combo = await getCachedRuleLayerBacktestDetail({ sourcePath, ruleName });
      const compacted = compactRuleLayerBacktestPayload(combo.backtest);
      setRuleDetailCombo(compacted === combo.backtest ? combo : { ...combo, backtest: compacted });
      setRuleDetailContext({
        sourcePath,
        expression: matched.when ?? combo.formula,
        direction,
        importRuleName: ruleName,
        importRuleExplain: matched.explain?.trim() || `策略详细统计：${ruleName}`,
        sampleLimitPerGroup: VALIDATION_DEFAULT_SAMPLE_LIMIT,
      });
    } catch (detailError) {
      setRuleDetailCombo(null);
      setRuleDetailContext(null);
      setRuleError(`读取策略 ${ruleName} 已计算明细失败: ${String(detailError)}`);
    } finally {
      setRuleDetailLoadingName("");
    }
  }

  function closeRuleDetailModal() {
    setRuleDetailCombo(null);
    setRuleDetailContext(null);
  }

  function openRankLayerSamples(layerIndex: number) {
    const group = rankLayerSampleGroupByIndex.get(layerIndex);
    if (!group || group.total_samples === 0) {
      setRankError("当前分层没有可展示的样本。");
      return;
    }
    setRankError("");
    setRankLayerSampleModal(group);
  }

  function closeRankLayerSampleModal() {
    setRankLayerSampleModal(null);
  }

  function buildRankLayerSampleCombo(
    group: RankLayerSampleGroup,
    rankBacktest: RankLayerBacktestData,
  ): RuleValidationComboResult {
    const triggeredDays = group.triggered_days;
    return {
      combo_key: `rank_layer_${group.layer_index}`,
      combo_label: group.layer_label,
      formula: "TOTAL_SCORE",
      unknown_values: [],
      trigger_samples: group.total_samples,
      triggered_days: triggeredDays,
      avg_daily_trigger: triggeredDays > 0 ? group.total_samples / triggeredDays : 0,
      sample_stats: {
        positive_count: group.positive_count,
        negative_count: group.negative_count,
        random_count: group.random_count,
        total_samples: group.total_samples,
      },
      trigger_count_stats: [
        {
          trigger_count: 1,
          positive_count: group.positive_count,
          negative_count: group.negative_count,
          random_count: group.random_count,
          total_samples: group.total_samples,
        },
      ],
      sample_groups: {
        positive: group.positive,
        negative: group.negative,
        random: group.random,
      },
      return_distribution: [],
      backtest: {
        rule_name: group.layer_label,
        stock_adj_type: rankBacktest.stock_adj_type,
        index_ts_code: rankBacktest.index_ts_code,
        index_beta: rankBacktest.index_beta,
        concept_beta: rankBacktest.concept_beta,
        industry_beta: rankBacktest.industry_beta,
        start_date: rankBacktest.start_date,
        end_date: rankBacktest.end_date,
        resolved_board: rankBacktest.resolved_board,
        exclude_st_board: rankBacktest.exclude_st_board,
        min_samples_per_rule_day: rankBacktest.min_samples_per_rank_day,
        min_listed_trade_days: rankBacktest.min_listed_trade_days,
        backtest_period: rankBacktest.backtest_period,
        points: [],
        avg_residual_mean: rankBacktest.layer_summaries.find(
          (item) => item.layer_index === group.layer_index,
        )?.avg_residual_return,
        avg_excess_residual_mean: null,
        avg_er_change: null,
        profit_loss_ratio: null,
        spread_mean: rankBacktest.spread_mean,
        avg_contribution_score: null,
        avg_contribution_per_trigger: null,
        ic_mean: rankBacktest.ic_mean,
        ic_std: rankBacktest.ic_std,
        icir: rankBacktest.icir,
        ic_t_value: rankBacktest.ic_t_value,
        layer_count: rankBacktest.layer_count,
        layer_method: rankBacktest.layer_method,
        layer_method_label: rankBacktest.layer_method_label,
        layer_summaries: rankBacktest.layer_summaries,
        is_all_rules: false,
        all_rule_summaries: [],
        rule_validation_details: [],
      },
      similarity_rows: [],
    };
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
            <span title="规则回测按实际触发数过滤；排名和场景回测按当日有效横截面样本数过滤">日最少样本（规则=触发数）</span>
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
          <label className="scene-layer-field">
            <span>策略并发数</span>
            <input type="number" min="1" value={parallelBatchSize} onChange={(event) => setParallelBatchSize(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>总市值最小(亿)</span>
            <input type="number" min="0" step="1" value={totalMvMin} onChange={(event) => setTotalMvMin(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>总市值最大(亿)</span>
            <input type="number" min="0" step="1" value={totalMvMax} onChange={(event) => setTotalMvMax(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>限定板块</span>
            <select
              value={backtestBoardFilter}
              onChange={(event) =>
                setBacktestBoardFilter(
                  event.target.value as (typeof STOCK_PICK_BOARD_OPTIONS)[number],
                )
              }
            >
              {backtestBoardOptions.map((board) => (
                <option key={board} value={board}>
                  {board}
                </option>
              ))}
            </select>
          </label>
          <label className="scene-layer-field">
            <span>分层层数</span>
            <input type="number" min="2" value={rankLayerCount} onChange={(event) => setRankLayerCount(event.target.value)} />
          </label>
          <label className="scene-layer-field">
            <span>分层方法</span>
            <select value={rankLayerMethod} onChange={(event) => setRankLayerMethod(event.target.value as RankLayerMethod)}>
              {RANK_LAYER_METHOD_OPTIONS.map((item) => (
                <option key={item.value} value={item.value}>
                  {item.label}
                </option>
              ))}
            </select>
          </label>
        </div>
      </section>

      <section className="scene-layer-card">
        <h2 className="scene-layer-title">排名整体回测</h2>
        <p className="scene-layer-caption">
          使用 score_summary 中的总分检验后续残差收益，除分层与 IC 外，直接展示每日等权 Top-K、持有期重叠修正后的 HAC t值及年度稳定性。
        </p>

        <div className="scene-layer-actions">
          <button type="button" className="scene-layer-primary-btn" onClick={() => void onRunRankBacktest()} disabled={heavyTaskRunning || initializing}>
            {rankLoading ? "回测中..." : "执行排名整体回测"}
          </button>
          <button type="button" className="scene-layer-secondary-btn" onClick={() => void onRunTransientRankBacktest()} disabled={heavyTaskRunning || initializing}>
            {rankTransientLoading ? "验证中..." : "变更策略验证"}
          </button>
        </div>

        {rankError ? <div className="scene-layer-error">{rankError}</div> : null}
      </section>

      {rankResult ? (
        <section className="scene-layer-card">
          <div className="scene-layer-layer-summary">
            <h3>排名整体回测汇总</h3>
            <div className="scene-layer-summary-section">
              <h4>基础信息</h4>
              <div className="scene-layer-contrib-table-wrap">
                <table className="scene-layer-contrib-table">
                  <thead>
                    <tr>
                      <th>对象</th>
                      <th>区间</th>
                      <th>指数</th>
                      <th>Beta（指/概/行）</th>
                      <th>限定板块</th>
                      <th>市值区分</th>
                      <th>有效交易日</th>
                      <th>总样本数</th>
                      <th>最小样本阈值</th>
                      <th>分层层数</th>
                      <th>分层方法</th>
                      <th>最少上市交易日</th>
                      <th>回测周期（天）</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td>总分</td>
                      <td>{formatDateLabel(rankResult.start_date)} ~ {formatDateLabel(rankResult.end_date)}</td>
                      <td>{rankResult.index_ts_code}</td>
                      <td>{formatNumber(rankResult.index_beta, 2)} / {formatNumber(rankResult.concept_beta, 2)} / {formatNumber(rankResult.industry_beta, 2)}</td>
                      <td>{formatBacktestBoardLabel(rankResult)}</td>
                      <td>{rankResult.market_value_grouping ? "默认分组聚合" : "不区分"}</td>
                      <td>{rankResult.point_count}</td>
                      <td>{rankResult.sample_count}</td>
                      <td>{rankResult.min_samples_per_rank_day}</td>
                      <td>{rankResult.layer_count}</td>
                      <td>{rankResult.layer_method_label}</td>
                      <td>{rankResult.min_listed_trade_days}</td>
                      <td>{rankResult.backtest_period}</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
            <div className="scene-layer-summary-section">
              <h4>回测表现</h4>
              <div className="scene-layer-contrib-table-wrap">
                <table className="scene-layer-contrib-table">
                  <thead>
                    <tr>
                      <th>分层差均值（日度高分层-低分层）</th>
                      <th title="收益统计区间最后一天 ER(20) 减触发日 ER(20)">ΔER(20)</th>
                      <th>IC 均值</th>
                      <th title="按回测持有期修正重叠样本的 Newey-West t值">IC HAC t值</th>
                      <th>ICIR</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td>{formatPercent(rankResult.spread_mean)}</td>
                      <td>{formatNumber(rankResult.avg_er_change, 4)}</td>
                      <td className={metricHighlightClass("ic", rankResult.ic_mean)}>{formatNumber(rankResult.ic_mean)}</td>
                      <td className={metricHighlightClass("t", rankResult.ic_t_value)}>{formatNumber(rankResult.ic_t_value)}</td>
                      <td className={metricHighlightClass("ir", rankResult.icir)}>{formatNumber(rankResult.icir)}</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          {rankTopKSummaries.length > 0 ? (
            <div className="scene-layer-layer-summary">
              <h3>Top-K 直接组合检验（每日等权）</h3>
              <p className="scene-layer-caption">
                直接检验实际会买到的头部股票；当日不足 K 只使用有效样本，HAC t值按持有期重叠修正标准误，避免普通t值虚高。
              </p>
              <div className="scene-layer-contrib-table-wrap">
                <table className="scene-layer-contrib-table">
                  <thead>
                    <tr>
                      <th>组合</th>
                      <th>有效交易日</th>
                      <th>样本数</th>
                      <th>日均残差收益</th>
                      <th>日收益中位数</th>
                      <th>正收益日占比</th>
                      <th>日波动</th>
                      <th>HAC t值</th>
                      <th>HAC滞后</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rankTopKSummaries.map((item) => (
                      <tr key={item.top_k}>
                        <td>Top {item.top_k}</td>
                        <td>{item.point_count}</td>
                        <td>{item.sample_count}</td>
                        <td>{formatPercent(item.avg_daily_residual_return)}</td>
                        <td>{formatPercent(item.median_daily_residual_return)}</td>
                        <td>{formatRate(item.positive_day_ratio)}</td>
                        <td>{formatPercent(item.daily_std)}</td>
                        <td className={metricHighlightClass("t", item.hac_t_value)}>{formatNumber(item.hac_t_value)}</td>
                        <td>{item.hac_lag}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          ) : null}

          {rankTopKPeriodSummaries.length > 0 ? (
            <div className="scene-layer-layer-summary">
              <h3>Top-K 年度稳定性</h3>
              <div className="scene-layer-contrib-table-wrap">
                <table className="scene-layer-contrib-table">
                  <thead>
                    <tr>
                      <th>年度</th>
                      <th>组合</th>
                      <th>区间</th>
                      <th>有效交易日</th>
                      <th>日均残差收益</th>
                      <th>日收益中位数</th>
                      <th>正收益日占比</th>
                      <th>HAC t值</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rankTopKPeriodSummaries.map((item) => (
                      <tr key={`${item.period_label}-${item.top_k}`}>
                        <td>{item.period_label}</td>
                        <td>Top {item.top_k}</td>
                        <td>{formatDateLabel(item.start_date)} ~ {formatDateLabel(item.end_date)}</td>
                        <td>{item.point_count}</td>
                        <td>{formatPercent(item.avg_daily_residual_return)}</td>
                        <td>{formatPercent(item.median_daily_residual_return)}</td>
                        <td>{formatRate(item.positive_day_ratio)}</td>
                        <td className={metricHighlightClass("t", item.hac_t_value)}>{formatNumber(item.hac_t_value)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          ) : null}

          {rankLayerSummaries.length === 0 ? (
            <div className="scene-layer-empty">当前没有可用于分层的总分样本。</div>
          ) : (
            <div className="scene-layer-layer-summary">
              <h3>{rankResult.layer_count}层分层依据（{rankResult.layer_method_label}，按总分从低到高）</h3>
              <div className="scene-layer-contrib-table-wrap">
                <table className="scene-layer-contrib-table">
                  <thead>
                    <tr>
                      <th>分层</th>
                      <th>有效交易日</th>
                      <th>分层样本数</th>
                      <th>分层均分</th>
                      <th>层级收益（日度残差均值）</th>
                      <th title="收益统计区间最后一天 ER(20) 减触发日 ER(20)">ΔER(20)</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rankLayerSummaries.map((item) => {
                      const sampleGroup = rankLayerSampleGroupByIndex.get(item.layer_index);
                      const canOpenSamples = Boolean(sampleGroup && sampleGroup.total_samples > 0);
                      return (
                        <tr key={item.layer_index}>
                          <td>
                            {canOpenSamples ? (
                              <button
                                type="button"
                                className="scene-layer-validation-detail-link"
                                onClick={() => openRankLayerSamples(item.layer_index)}
                                title={`查看${item.layer_label}样本`}
                              >
                                {item.layer_label}
                              </button>
                            ) : (
                              item.layer_label
                            )}
                          </td>
                          <td>{item.point_count}</td>
                          <td>{item.sample_count}</td>
                          <td>{formatNumber(item.avg_score, 4)}</td>
                          <td>{formatPercent(item.avg_residual_return)}</td>
                          <td>{formatNumber(item.avg_er_change, 4)}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {rankResult.market_value_summaries && rankResult.market_value_summaries.length > 0 ? (
            <div className="scene-layer-layer-summary">
              <h3>市值分组回测表现</h3>
              <div className="scene-layer-contrib-table-wrap">
                <table className="scene-layer-contrib-table">
                  <thead>
                    <tr>
                      <th>市值分组</th>
                      <th>有效交易日</th>
                      <th>总样本数</th>
                      <th title="收益统计区间最后一天 ER(20) 减触发日 ER(20)">ΔER(20)</th>
                      <th>分层差均值</th>
                      <th>IC 均值</th>
                      <th>IC t值</th>
                      <th>ICIR</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rankResult.market_value_summaries.map((item) => (
                      <tr key={item.group_label}>
                        <td>{item.group_label}</td>
                        <td>{item.point_count}</td>
                        <td>{item.sample_count}</td>
                        <td>{formatNumber(item.avg_er_change, 4)}</td>
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

          {rankLayerSampleModal && rankResult ? (
            <div className="scene-layer-modal-mask" onClick={closeRankLayerSampleModal}>
              <div
                className="scene-layer-modal-card scene-layer-validation-detail-modal"
                role="dialog"
                aria-modal="true"
                aria-label={`排名分层样本：${rankLayerSampleModal.layer_label}`}
                onClick={(event) => event.stopPropagation()}
              >
                <div className="scene-layer-modal-header">
                  <h3>排名分层样本：{rankLayerSampleModal.layer_label}</h3>
                  <button type="button" className="scene-layer-modal-close" onClick={closeRankLayerSampleModal}>
                    关闭
                  </button>
                </div>
                <div className="scene-layer-modal-scroll-body">
                  <ExpressionValidationSamplesPanel
                    data={{
                      importRuleName: "排名整体回测",
                      importRuleExplain: `排名整体回测：${rankLayerSampleModal.layer_label}`,
                      expression: "TOTAL_SCORE",
                      combo: buildRankLayerSampleCombo(rankLayerSampleModal, rankResult),
                      comboParamSummary: `${rankResult.layer_method_label} · ${rankLayerSampleModal.layer_label}`,
                      sampleLimitPerGroup: RANK_LAYER_SAMPLE_LIMIT_PER_GROUP,
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

      <section className="scene-layer-card">
        <h2 className="scene-layer-title">场景整体回测</h2>
        <p className="scene-layer-caption">
          使用 scene_details 中的场景状态与排序，计算各场景状态下的分层残差收益、分层差、IC / ICIR。
        </p>

        <div className="scene-layer-actions">
          <button type="button" className="scene-layer-primary-btn" onClick={() => void onRunBacktest()} disabled={heavyTaskRunning || initializing}>
            {loading ? "回测中..." : "执行场景整体回测"}
          </button>
          <button type="button" className="scene-layer-secondary-btn" onClick={() => void onRunTransientSceneBacktest()} disabled={heavyTaskRunning || initializing}>
            {transientLoading ? "验证中..." : "变更策略验证"}
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
              <span>限定板块</span>
              <strong>{formatBacktestBoardLabel(result)}</strong>
            </div>
            <div className="scene-layer-summary-item">
              <span>总市值范围</span>
              <strong>{formatMarketValueRange(result)}</strong>
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
          使用 rule_details 中的策略得分与次日开盘起算的复合残差收益，按实际触发数过滤有效日期，计算策略日度残差均值、贡献度、IC / ICIR；多日持有的 IC t值使用 HAC 修正。
        </p>

        <div className="scene-layer-actions">
          <button type="button" className="scene-layer-primary-btn" onClick={() => void onRunRuleBacktest()} disabled={heavyTaskRunning || initializing}>
            {ruleLoading ? "回测中..." : "执行策略回测"}
          </button>
          <button type="button" className="scene-layer-secondary-btn" onClick={() => void onRunTransientRuleBacktest()} disabled={heavyTaskRunning || initializing}>
            {ruleTransientLoading ? "验证中..." : "变更策略验证"}
          </button>
        </div>

        {ruleError ? <div className="scene-layer-error">{ruleError}</div> : null}
      </section>

      {ruleResult ? (
        <section className="scene-layer-card">
          {allRuleSummaries.length === 0 ? (
            <div className="scene-layer-empty">当前没有可回测的策略。</div>
          ) : null}

          {allRuleSummaries.length > 0 ? (
            <div className="scene-layer-layer-summary">
              <h3>全部策略明细（点击表头排序）</h3>
              <p className="scene-layer-caption">
                衰减验证按策略方向归一化：正分策略上涨、负分策略下跌都记为正向超额；分别比较最近 20/40/60 个有效触发日与此前历史，Δ 为近期减此前。
              </p>
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
                      <th aria-sort={getAriaSort(ruleSummarySortKey === "avg_contribution_score", ruleSummarySortDirection)}>
                        <TableSortButton
                          label="平均贡献度"
                          isActive={ruleSummarySortKey === "avg_contribution_score" && ruleSummarySortDirection !== null}
                          direction={ruleSummarySortDirection}
                          onClick={() => toggleRuleSummarySort("avg_contribution_score")}
                          title="按平均贡献度排序"
                        />
                      </th>
                      <th aria-sort={getAriaSort(ruleSummarySortKey === "avg_contribution_per_trigger", ruleSummarySortDirection)}>
                        <TableSortButton
                          label="平均单次贡献"
                          isActive={ruleSummarySortKey === "avg_contribution_per_trigger" && ruleSummarySortDirection !== null}
                          direction={ruleSummarySortDirection}
                          onClick={() => toggleRuleSummarySort("avg_contribution_per_trigger")}
                          title="按平均单次贡献排序"
                        />
                      </th>
                      <th aria-sort={getAriaSort(ruleSummarySortKey === "profit_loss_ratio", ruleSummarySortDirection)}>
                        <TableSortButton
                          label="样本利润因子"
                          isActive={ruleSummarySortKey === "profit_loss_ratio" && ruleSummarySortDirection !== null}
                          direction={ruleSummarySortDirection}
                          onClick={() => toggleRuleSummarySort("profit_loss_ratio")}
                          title="按触发股票样本的 Profit Factor 排序"
                        />
                      </th>
                      <th aria-sort={getAriaSort(ruleSummarySortKey === "avg_excess_residual_mean", ruleSummarySortDirection)}>
                        <TableSortButton
                          label="超额残差"
                          isActive={ruleSummarySortKey === "avg_excess_residual_mean" && ruleSummarySortDirection !== null}
                          direction={ruleSummarySortDirection}
                          onClick={() => toggleRuleSummarySort("avg_excess_residual_mean")}
                          title="按超额残差排序"
                        />
                      </th>
                      <th aria-sort={getAriaSort(ruleSummarySortKey === "decay_20", ruleSummarySortDirection)}>
                        <TableSortButton
                          label="近期衰减"
                          isActive={ruleSummarySortKey === "decay_20" && ruleSummarySortDirection !== null}
                          direction={ruleSummarySortDirection}
                          onClick={() => toggleRuleSummarySort("decay_20")}
                          title="按最近20个有效触发日相对此前历史的方向超额变化排序"
                        />
                      </th>
                      <th aria-sort={getAriaSort(ruleSummarySortKey === "avg_er_change", ruleSummarySortDirection)}>
                        <TableSortButton
                          label="ΔER(20)"
                          isActive={ruleSummarySortKey === "avg_er_change" && ruleSummarySortDirection !== null}
                          direction={ruleSummarySortDirection}
                          onClick={() => toggleRuleSummarySort("avg_er_change")}
                          title="按 ER(20) 区间变动均值排序"
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
                        <td title={item.rule_name}>
                          <button
                            type="button"
                            className="scene-layer-validation-detail-link"
                            disabled={ruleDetailLoadingName === item.rule_name}
                            onClick={() => void openStoredRuleValidationDetail(item.rule_name)}
                          >
                            {item.rule_name}
                          </button>
                        </td>
                        <td>{item.point_count}</td>
                        <td>{formatNumber(item.avg_contribution_score, 2)}</td>
                        <td>{formatNumber(item.avg_contribution_per_trigger, 2)}</td>
                        <td>{formatProfitLossRatio(item.profit_loss_ratio)}</td>
                        <td className={residualMetricHighlightClass(item.avg_excess_residual_mean, resolveResidualDirection(item.avg_contribution_score))}>
                          {renderResidualMetric(item.avg_excess_residual_mean, resolveResidualDirection(item.avg_contribution_score))}
                        </td>
                        <td className="scene-layer-decay-cell">{renderRuleDecayValidations(item)}</td>
                        <td>{formatNumber(item.avg_er_change, 4)}</td>
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

      {ruleDetailCombo && ruleDetailContext ? (
        <div className="scene-layer-modal-mask" onClick={closeRuleDetailModal}>
          <div
            className="scene-layer-modal-card scene-layer-validation-detail-modal"
            role="dialog"
            aria-modal="true"
            aria-label={`策略详细统计：${ruleDetailCombo.combo_label}`}
            onClick={(event) => event.stopPropagation()}
          >
            <div className="scene-layer-modal-header">
              <h3>策略详细统计：{ruleDetailCombo.combo_label}</h3>
              <button type="button" className="scene-layer-modal-close" onClick={closeRuleDetailModal}>
                关闭
              </button>
            </div>
            <div className="scene-layer-modal-scroll-body">
              <ExpressionCoreSummary combo={ruleDetailCombo} direction={ruleDetailContext.direction} />
              <ExpressionWalkForward combo={ruleDetailCombo} />
              <ExpressionLayerResult combo={ruleDetailCombo} />
              <ExpressionSimilarity combo={ruleDetailCombo} />
              <ExpressionIncremental combo={ruleDetailCombo} />
              <ExpressionSamplesCheck combo={ruleDetailCombo} context={ruleDetailContext} />
              <ExpressionDailyDetail combo={ruleDetailCombo} />
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
