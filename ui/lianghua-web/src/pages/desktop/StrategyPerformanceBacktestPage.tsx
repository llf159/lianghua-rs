import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { createPortal } from "react-dom";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import {
  getStrategyPickCache,
  getStrategyPerformanceHorizonView,
  getStrategyPerformancePage,
  getStrategyPerformanceRuleDetail,
  saveManualStrategyPickCache,
  type StrategyPerformanceCompanionRow,
  type StrategyPerformanceFutureSummary,
  type StrategyPerformanceHorizonViewData,
  type StrategyPerformanceOverallScoreAnalysis,
  type StrategyPerformancePageData,
  type StrategyPerformanceRuleDetail,
  type StrategyPerformanceRuleDirectionDetail,
  type StrategyPerformanceRuleRow,
} from "../../apis/strategyPerformance";
import {
  readJsonStorage,
  readStoredSourcePath,
  writeJsonStorage,
} from "../../shared/storage";
import {
  readStrategyPerformanceAdvancedPickSynced,
  type StrategyPerformanceManualAdvantageSelection,
  writeStrategyPerformanceAdvancedPickDraft,
  writeStrategyPerformanceAdvancedPickSynced,
} from "../../shared/strategyPerformanceAdvancedPickStorage";
import {
  TableSortButton,
  getAriaSort,
  type SortDefinition,
  useTableSort,
} from "../../shared/tableSort";
import "./css/StrategyPerformanceBacktestPage.css";

const STRATEGY_PERFORMANCE_STATE_KEY = "lh_strategy_performance_page_v13";
const HORIZON_OPTIONS = [2, 3, 5] as const;
const QUANTILE_OPTIONS = [0.8, 0.9, 0.95] as const;
const DEFAULT_AUTO_MIN_SAMPLES = {
  2: 5,
  3: 5,
  5: 10,
  10: 20,
} as const;
type SubmittedQuery = {
  sourcePath: string;
  selectedHorizon: number;
  strongQuantile: number;
  autoMinSamples2: number;
  autoMinSamples3: number;
  autoMinSamples5: number;
  autoMinSamples10: number;
  requireWinRateAboveMarket: boolean;
  minPassHorizons: number;
  minAdvHits: number;
  topLimit: number;
};

type PersistedState = {
  sourcePath: string;
  selectedHorizon: string;
  strongQuantile: string;
  manualRuleNames: string[];
  strategyKeyword: string;
  autoMinSamples2: string;
  autoMinSamples3: string;
  autoMinSamples5: string;
  autoMinSamples10: string;
  requireWinRateAboveMarket: boolean;
  minPassHorizons: string;
  minAdvHits: string;
  topLimit: string;
  selectedRuleName: string;
  pageData: PersistedStrategyPerformancePageData | null;
  submittedQuery: SubmittedQuery | null;
};

type StrategyPerformanceRuntimeState = Omit<PersistedState, "pageData"> & {
  pageData: StrategyPerformancePageData | null;
};

let strategyPerformanceRuntimeState: StrategyPerformanceRuntimeState | null =
  null;

type PersistedStrategyPerformanceMetric = Pick<
  StrategyPerformanceRuleRow["metrics"][number],
  | "score_mode"
  | "horizon"
  | "hit_n"
  | "avg_future_return_pct"
  | "strong_hit_rate"
  | "strong_lift"
  | "rank_ic_mean"
  | "icir"
  | "sharpe_ratio"
  | "composite_score"
  | "hit_vs_non_hit_delta_pct"
  | "low_confidence"
>;

type PersistedStrategyPerformanceRuleRow = Pick<
  StrategyPerformanceRuleRow,
  | "rule_name"
  | "explain"
  | "tag"
  | "scope_way"
  | "scope_windows"
  | "points"
  | "has_dist_points"
  | "signal_direction"
  | "direction_label"
  | "auto_candidate"
  | "manually_selected"
  | "in_advantage_set"
  | "in_companion_set"
  | "negative_effective"
  | "negative_effectiveness_label"
  | "negative_review_notes"
  | "overall_composite_score"
> & {
  metrics: PersistedStrategyPerformanceMetric[];
};

type PersistedStrategyPerformanceScoreBucketRow = Pick<
  NonNullable<StrategyPerformanceOverallScoreAnalysis>["score_rows"][number],
  | "bucket_label"
  | "sample_count"
  | "avg_future_return_pct"
  | "strong_hit_rate"
  | "win_rate"
>;

type PersistedStrategyPerformanceOverallScoreAnalysis = Pick<
  NonNullable<StrategyPerformanceOverallScoreAnalysis>,
  | "horizon"
  | "sample_count"
  | "avg_future_return_pct"
  | "strong_hit_rate"
  | "win_rate"
  | "spearman_corr"
  | "rank_ic_mean"
  | "icir"
  | "layer_return_spread_pct"
  | "bucket_mode"
> & {
  score_rows: PersistedStrategyPerformanceScoreBucketRow[];
};

type PersistedStrategyPerformancePageData = {
  selected_horizon: number;
  future_summaries: StrategyPerformancePageData["future_summaries"];
  auto_advantage_rule_names: string[];
  manual_advantage_rule_names: string[];
  auto_candidate_rule_names: string[];
  ignored_manual_rule_names: string[];
  resolved_advantage_rule_names: string[];
  effective_negative_rule_names: string[];
  ineffective_negative_rule_names: string[];
  rule_rows: PersistedStrategyPerformanceRuleRow[];
  companion_rows: StrategyPerformancePageData["companion_rows"];
  overall_score_analysis?: PersistedStrategyPerformanceOverallScoreAnalysis | null;
};

function arrayFromUnknown(value: unknown) {
  return Array.isArray(value)
    ? value.filter((item): item is string => typeof item === "string")
    : [];
}

function parsePositiveInt(value: string, fallback: number) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function parseQuantile(value: string) {
  const parsed = Number(value);
  if (Number.isFinite(parsed) && parsed > 0 && parsed < 1) {
    return parsed;
  }
  return 0.9;
}

function normalizeStringArray(values: string[]) {
  const out: string[] = [];
  const seen = new Set<string>();
  values.forEach((value) => {
    const trimmed = value.trim();
    if (!trimmed || seen.has(trimmed)) {
      return;
    }
    seen.add(trimmed);
    out.push(trimmed);
  });
  return out;
}

function sameManualAdvantageSelection(
  left: StrategyPerformanceManualAdvantageSelection | null,
  right: StrategyPerformanceManualAdvantageSelection | null,
) {
  if (!left || !right) {
    return false;
  }
  return (
    left.sourcePath === right.sourcePath &&
    left.selectedHorizon === right.selectedHorizon &&
    left.strongQuantile === right.strongQuantile &&
    left.autoMinSamples2 === right.autoMinSamples2 &&
    left.autoMinSamples3 === right.autoMinSamples3 &&
    left.autoMinSamples5 === right.autoMinSamples5 &&
    left.autoMinSamples10 === right.autoMinSamples10 &&
    left.requireWinRateAboveMarket === right.requireWinRateAboveMarket &&
    left.minPassHorizons === right.minPassHorizons &&
    left.minAdvHits === right.minAdvHits &&
    left.manualAdvantageRuleNames.length ===
      right.manualAdvantageRuleNames.length &&
    left.manualAdvantageRuleNames.every(
      (item, index) => item === right.manualAdvantageRuleNames[index],
    )
  );
}

function compactPageDataForStorage(
  pageData: StrategyPerformancePageData | null,
): PersistedStrategyPerformancePageData | null {
  if (!pageData) {
    return null;
  }

  return {
    selected_horizon: pageData.selected_horizon,
    future_summaries: pageData.future_summaries,
    auto_advantage_rule_names: pageData.auto_advantage_rule_names,
    manual_advantage_rule_names: pageData.manual_advantage_rule_names,
    auto_candidate_rule_names: pageData.auto_candidate_rule_names,
    ignored_manual_rule_names: pageData.ignored_manual_rule_names,
    resolved_advantage_rule_names: pageData.resolved_advantage_rule_names,
    effective_negative_rule_names: pageData.effective_negative_rule_names,
    ineffective_negative_rule_names: pageData.ineffective_negative_rule_names,
    rule_rows: pageData.rule_rows.map((row) => ({
      rule_name: row.rule_name,
      explain: row.explain,
      tag: row.tag,
      scope_way: row.scope_way,
      scope_windows: row.scope_windows,
      points: row.points,
      has_dist_points: row.has_dist_points,
      signal_direction: row.signal_direction,
      direction_label: row.direction_label,
      auto_candidate: row.auto_candidate,
      manually_selected: row.manually_selected,
      in_advantage_set: row.in_advantage_set,
      in_companion_set: row.in_companion_set,
      negative_effective: row.negative_effective,
      negative_effectiveness_label: row.negative_effectiveness_label,
      negative_review_notes: row.negative_review_notes,
      overall_composite_score: row.overall_composite_score,
      metrics: row.metrics.map((metric) => ({
        score_mode: metric.score_mode,
        horizon: metric.horizon,
        hit_n: metric.hit_n,
        avg_future_return_pct: metric.avg_future_return_pct,
        strong_hit_rate: metric.strong_hit_rate,
        strong_lift: metric.strong_lift,
        rank_ic_mean: metric.rank_ic_mean,
        icir: metric.icir,
        sharpe_ratio: metric.sharpe_ratio,
        composite_score: metric.composite_score,
        hit_vs_non_hit_delta_pct: metric.hit_vs_non_hit_delta_pct,
        low_confidence: metric.low_confidence,
      })),
    })),
    companion_rows: pageData.companion_rows,
    overall_score_analysis: pageData.overall_score_analysis
      ? {
          horizon: pageData.overall_score_analysis.horizon,
          sample_count: pageData.overall_score_analysis.sample_count,
          avg_future_return_pct:
            pageData.overall_score_analysis.avg_future_return_pct,
          strong_hit_rate: pageData.overall_score_analysis.strong_hit_rate,
          win_rate: pageData.overall_score_analysis.win_rate,
          spearman_corr: pageData.overall_score_analysis.spearman_corr,
          rank_ic_mean: pageData.overall_score_analysis.rank_ic_mean,
          icir: pageData.overall_score_analysis.icir,
          layer_return_spread_pct:
            pageData.overall_score_analysis.layer_return_spread_pct,
          bucket_mode: pageData.overall_score_analysis.bucket_mode,
          score_rows: pageData.overall_score_analysis.score_rows.map((row) => ({
            bucket_label: row.bucket_label,
            sample_count: row.sample_count,
            avg_future_return_pct: row.avg_future_return_pct,
            strong_hit_rate: row.strong_hit_rate,
            win_rate: row.win_rate,
          })),
        }
      : null,
  };
}

function restorePageDataFromStorage(
  raw:
    | PersistedStrategyPerformancePageData
    | StrategyPerformancePageData
    | null
    | undefined,
): StrategyPerformancePageData | null {
  if (!raw || typeof raw !== "object") {
    return null;
  }

  const pageData = raw as Partial<PersistedStrategyPerformancePageData>;
  return {
    horizons: [...HORIZON_OPTIONS],
    selected_horizon:
      typeof pageData.selected_horizon === "number"
        ? pageData.selected_horizon
        : 2,
    strong_quantile: 0.9,
    strategy_options: [],
    future_summaries: Array.isArray(pageData.future_summaries)
      ? pageData.future_summaries
      : [],
    auto_filter: {
      min_samples_2: DEFAULT_AUTO_MIN_SAMPLES[2],
      min_samples_3: DEFAULT_AUTO_MIN_SAMPLES[3],
      min_samples_5: DEFAULT_AUTO_MIN_SAMPLES[5],
      min_samples_10: DEFAULT_AUTO_MIN_SAMPLES[10],
      require_win_rate_above_market: false,
      min_pass_horizons: 2,
    },
    resolved_advantage_mode: "",
    auto_advantage_rule_names: Array.isArray(pageData.auto_advantage_rule_names)
      ? pageData.auto_advantage_rule_names
      : [],
    manual_advantage_rule_names: Array.isArray(
      pageData.manual_advantage_rule_names,
    )
      ? pageData.manual_advantage_rule_names
      : [],
    auto_candidate_rule_names: Array.isArray(pageData.auto_candidate_rule_names)
      ? pageData.auto_candidate_rule_names
      : [],
    manual_rule_names: [],
    ignored_manual_rule_names: Array.isArray(pageData.ignored_manual_rule_names)
      ? pageData.ignored_manual_rule_names
      : [],
    resolved_advantage_rule_names: Array.isArray(
      pageData.resolved_advantage_rule_names,
    )
      ? pageData.resolved_advantage_rule_names
      : [],
    resolved_companion_rule_names: [],
    effective_negative_rule_names: Array.isArray(
      pageData.effective_negative_rule_names,
    )
      ? pageData.effective_negative_rule_names
      : [],
    ineffective_negative_rule_names: Array.isArray(
      pageData.ineffective_negative_rule_names,
    )
      ? pageData.ineffective_negative_rule_names
      : [],
    min_adv_hits: 1,
    top_limit: 100,
    noisy_companion_rule_names: [],
    rule_rows: Array.isArray(pageData.rule_rows)
      ? (pageData.rule_rows as StrategyPerformanceRuleRow[])
      : [],
    companion_rows: Array.isArray(pageData.companion_rows)
      ? pageData.companion_rows
      : [],
    overall_score_analysis:
      pageData.overall_score_analysis &&
      typeof pageData.overall_score_analysis === "object"
        ? (pageData.overall_score_analysis as StrategyPerformanceOverallScoreAnalysis)
        : null,
    selected_rule_name: null,
    rule_detail: null,
    methods: [],
  };
}

function hasLegacyAutoMinSampleStrings(
  values:
    | {
        autoMinSamples2?: string;
        autoMinSamples3?: string;
        autoMinSamples5?: string;
        autoMinSamples10?: string;
      }
    | null
    | undefined,
) {
  return (
    values?.autoMinSamples2 === "30" &&
    values?.autoMinSamples3 === "30" &&
    values?.autoMinSamples5 === "30" &&
    values?.autoMinSamples10 === "30"
  );
}

function hasLegacyAutoMinSampleNumbers(
  values:
    | {
        autoMinSamples2?: number;
        autoMinSamples3?: number;
        autoMinSamples5?: number;
        autoMinSamples10?: number;
      }
    | null
    | undefined,
) {
  return (
    values?.autoMinSamples2 === 30 &&
    values?.autoMinSamples3 === 30 &&
    values?.autoMinSamples5 === 30 &&
    values?.autoMinSamples10 === 30
  );
}

function sameSubmittedQuery(
  left: SubmittedQuery | null,
  right: SubmittedQuery | null,
) {
  if (!left || !right) {
    return false;
  }
  return (
    left.sourcePath === right.sourcePath &&
    left.selectedHorizon === right.selectedHorizon &&
    left.strongQuantile === right.strongQuantile &&
    left.autoMinSamples2 === right.autoMinSamples2 &&
    left.autoMinSamples3 === right.autoMinSamples3 &&
    left.autoMinSamples5 === right.autoMinSamples5 &&
    left.autoMinSamples10 === right.autoMinSamples10 &&
    left.requireWinRateAboveMarket === right.requireWinRateAboveMarket &&
    left.minPassHorizons === right.minPassHorizons &&
    left.minAdvHits === right.minAdvHits &&
    left.topLimit === right.topLimit
  );
}

function sameSubmittedQueryExceptHorizon(
  left: SubmittedQuery | null,
  right: SubmittedQuery | null,
) {
  if (!left || !right) {
    return false;
  }
  return (
    left.sourcePath === right.sourcePath &&
    left.strongQuantile === right.strongQuantile &&
    left.autoMinSamples2 === right.autoMinSamples2 &&
    left.autoMinSamples3 === right.autoMinSamples3 &&
    left.autoMinSamples5 === right.autoMinSamples5 &&
    left.autoMinSamples10 === right.autoMinSamples10 &&
    left.requireWinRateAboveMarket === right.requireWinRateAboveMarket &&
    left.minPassHorizons === right.minPassHorizons &&
    left.minAdvHits === right.minAdvHits &&
    left.topLimit === right.topLimit
  );
}

function formatNumber(value?: number | null, digits = 2) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  return Number.isInteger(value) ? String(value) : value.toFixed(digits);
}

function formatPercent(value?: number | null, digits = 2) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  return `${value >= 0 ? "+" : ""}${value.toFixed(digits)}%`;
}

function formatRate(value?: number | null, digits = 1) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  return `${(value * 100).toFixed(digits)}%`;
}

function valueClassName(value?: number | null) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "";
  }
  if (value > 0) {
    return "strategy-performance-positive";
  }
  if (value < 0) {
    return "strategy-performance-negative";
  }
  return "strategy-performance-neutral";
}

function isHitVsNonHitScoreMode(scoreMode?: string | null) {
  return scoreMode === "hit_vs_non_hit";
}

function metricPrimaryValue(
  metric?: StrategyPerformanceRuleRow["metrics"][number] | null,
) {
  if (!metric) {
    return null;
  }
  return isHitVsNonHitScoreMode(metric.score_mode)
    ? metric.hit_vs_non_hit_delta_pct
    : metric.rank_ic_mean;
}

function metricSecondaryValue(
  metric?: StrategyPerformanceRuleRow["metrics"][number] | null,
) {
  if (!metric) {
    return null;
  }
  return isHitVsNonHitScoreMode(metric.score_mode)
    ? metric.avg_future_return_pct
    : metric.icir;
}

function metricForHorizon(row: StrategyPerformanceRuleRow, horizon: number) {
  return row.metrics.find((item) => item.horizon === horizon) ?? null;
}

function findSummary(
  summaries: StrategyPerformanceFutureSummary[],
  horizon: number,
) {
  return summaries.find((item) => item.horizon === horizon) ?? null;
}

function compareDescNumber(left?: number | null, right?: number | null) {
  const leftValue = left ?? Number.NEGATIVE_INFINITY;
  const rightValue = right ?? Number.NEGATIVE_INFINITY;
  if (leftValue === rightValue) {
    return 0;
  }
  return rightValue > leftValue ? 1 : -1;
}

function compareRuleRows(
  left: StrategyPerformanceRuleRow,
  right: StrategyPerformanceRuleRow,
  horizon: number,
) {
  const leftMetric = metricForHorizon(left, horizon);
  const rightMetric = metricForHorizon(right, horizon);

  if (
    left.signal_direction === "negative" &&
    right.signal_direction === "negative"
  ) {
    return (
      Number(right.negative_effective === true) -
        Number(left.negative_effective === true) ||
      compareDescNumber(
        left.overall_composite_score != null
          ? -left.overall_composite_score
          : null,
        right.overall_composite_score != null
          ? -right.overall_composite_score
          : null,
      ) ||
      compareDescNumber(
        leftMetric?.composite_score != null
          ? -leftMetric.composite_score
          : null,
        rightMetric?.composite_score != null
          ? -rightMetric.composite_score
          : null,
      ) ||
      compareDescNumber(
        leftMetric?.hit_vs_non_hit_delta_pct != null
          ? -leftMetric.hit_vs_non_hit_delta_pct
          : null,
        rightMetric?.hit_vs_non_hit_delta_pct != null
          ? -rightMetric.hit_vs_non_hit_delta_pct
          : null,
      ) ||
      compareDescNumber(
        leftMetric?.strong_lift != null ? -leftMetric.strong_lift : null,
        rightMetric?.strong_lift != null ? -rightMetric.strong_lift : null,
      ) ||
      compareDescNumber(
        leftMetric?.avg_future_return_pct != null
          ? -leftMetric.avg_future_return_pct
          : null,
        rightMetric?.avg_future_return_pct != null
          ? -rightMetric.avg_future_return_pct
          : null,
      ) ||
      (rightMetric?.hit_n ?? 0) - (leftMetric?.hit_n ?? 0) ||
      left.rule_name.localeCompare(right.rule_name)
    );
  }

  return (
    Number(right.in_advantage_set) - Number(left.in_advantage_set) ||
    Number(right.auto_candidate) - Number(left.auto_candidate) ||
    compareDescNumber(
      leftMetric?.composite_score,
      rightMetric?.composite_score,
    ) ||
    compareDescNumber(
      metricPrimaryValue(leftMetric),
      metricPrimaryValue(rightMetric),
    ) ||
    compareDescNumber(
      metricSecondaryValue(leftMetric),
      metricSecondaryValue(rightMetric),
    ) ||
    compareDescNumber(
      leftMetric?.avg_future_return_pct,
      rightMetric?.avg_future_return_pct,
    ) ||
    (rightMetric?.hit_n ?? 0) - (leftMetric?.hit_n ?? 0) ||
    compareDescNumber(
      left.overall_composite_score,
      right.overall_composite_score,
    ) ||
    left.rule_name.localeCompare(right.rule_name)
  );
}

type RuleTableSortKey = "rule_name" | "h2" | "h3" | "h5";
type CompanionTableSortKey =
  | "rule_name"
  | "hit_n"
  | "avg_future_return_pct"
  | "eligible_pool_avg_return_pct"
  | "delta_return_pct"
  | "win_rate"
  | "delta_win_rate";

function horizonSortKey(horizon: number): RuleTableSortKey {
  if (horizon === 2) {
    return "h2";
  }
  if (horizon === 3) {
    return "h3";
  }
  if (horizon === 5) {
    return "h5";
  }
  return "h5";
}

function hasPositiveHits(row: StrategyPerformanceRuleRow) {
  return (
    row.signal_direction === "positive" &&
    row.metrics.some((metric) => (metric.hit_n ?? 0) > 0)
  );
}

function StatusBadge({
  children,
  tone,
}: {
  children: ReactNode;
  tone: "neutral" | "good" | "warn";
}) {
  return (
    <span
      className={`strategy-performance-badge strategy-performance-badge-${tone}`}
    >
      {children}
    </span>
  );
}

function MetricCell({
  row,
  horizon,
}: {
  row: StrategyPerformanceRuleRow;
  horizon: number;
}) {
  const metric = metricForHorizon(row, horizon);
  if (!metric) {
    return <span className="strategy-performance-muted">--</span>;
  }
  const usesHitVsNonHit = isHitVsNonHitScoreMode(metric.score_mode);
  return (
    <div className="strategy-performance-metric-cell">
      <div>
        <span>样本</span>
        <strong>{formatNumber(metric.hit_n, 0)}</strong>
      </div>
      <div>
        <span>{usesHitVsNonHit ? "Hit vs Non-hit" : "IC"}</span>
        <strong
          className={valueClassName(
            usesHitVsNonHit
              ? metric.hit_vs_non_hit_delta_pct
              : metric.rank_ic_mean,
          )}
        >
          {usesHitVsNonHit
            ? formatPercent(metric.hit_vs_non_hit_delta_pct)
            : formatNumber(metric.rank_ic_mean, 3)}
        </strong>
      </div>
      <div>
        <span>{usesHitVsNonHit ? "命中均收益" : "ICIR"}</span>
        <strong
          className={valueClassName(
            usesHitVsNonHit ? metric.avg_future_return_pct : metric.icir,
          )}
        >
          {usesHitVsNonHit
            ? formatPercent(metric.avg_future_return_pct)
            : formatNumber(metric.icir, 2)}
        </strong>
      </div>
      <div>
        <span>强势命中</span>
        <strong>{formatRate(metric.strong_hit_rate)}</strong>
      </div>
      <div>
        <span>Sharpe</span>
        <strong className={valueClassName(metric.sharpe_ratio)}>
          {formatNumber(metric.sharpe_ratio, 2)}
        </strong>
      </div>
      <div>
        <span>综合分</span>
        <strong className={valueClassName(metric.composite_score)}>
          {formatNumber(metric.composite_score, 2)}
        </strong>
      </div>
      {metric.low_confidence ? (
        <small className="strategy-performance-low-confidence">低样本</small>
      ) : null}
    </div>
  );
}

function SummarySection({
  summaries,
  selectedHorizon,
  pendingHorizon,
  loading,
  onSelectHorizon,
}: {
  summaries: StrategyPerformanceFutureSummary[];
  selectedHorizon: number;
  pendingHorizon: number;
  loading: boolean;
  onSelectHorizon: (horizon: number) => void;
}) {
  return (
    <section className="strategy-performance-card">
      <div className="strategy-performance-section-head">
        <div>
          <h3>1. 强势股定义</h3>
        </div>
      </div>
      <div className="strategy-performance-summary-grid">
        {summaries.map((summary) => (
          <button
            className={
              summary.horizon === selectedHorizon
                ? "strategy-performance-summary-card is-active"
                : "strategy-performance-summary-card"
            }
            key={summary.horizon}
            onClick={() => onSelectHorizon(summary.horizon)}
            type="button"
            disabled={loading}
          >
            <div className="strategy-performance-summary-head">
              <strong>{summary.horizon} 日</strong>
              {summary.horizon === selectedHorizon ? (
                <StatusBadge tone="good">当前视角</StatusBadge>
              ) : loading && summary.horizon === pendingHorizon ? (
                <StatusBadge tone="warn">切换中</StatusBadge>
              ) : null}
            </div>
            <div className="strategy-performance-summary-rows">
              <div>
                <span>全样本数</span>
                <strong>{formatNumber(summary.sample_count, 0)}</strong>
              </div>
              <div>
                <span>均收益</span>
                <strong
                  className={valueClassName(summary.avg_future_return_pct)}
                >
                  {formatPercent(summary.avg_future_return_pct)}
                </strong>
              </div>
              <div>
                <span>p80 / p90 / p95</span>
                <strong>
                  {formatPercent(summary.p80_return_pct)} /{" "}
                  {formatPercent(summary.p90_return_pct)} /{" "}
                  {formatPercent(summary.p95_return_pct)}
                </strong>
              </div>
              <div>
                <span>强势阈值</span>
                <strong>{formatPercent(summary.strong_threshold_pct)}</strong>
              </div>
              <div>
                <span>强势基准占比</span>
                <strong>{formatRate(summary.strong_base_rate)}</strong>
              </div>
              <div>
                <span>胜率 / 最大收益</span>
                <strong>
                  {formatRate(summary.win_rate)} /{" "}
                  {formatPercent(summary.max_future_return_pct)}
                </strong>
              </div>
            </div>
          </button>
        ))}
      </div>
    </section>
  );
}

function OverallScoreAnalysisSection({
  title = "2. 策略整体分层分析",
  description = "按总分 `total_score` 分层，观察整体策略分高分低时的未来收益差异。",
  emptyText = "当前没有可展示的整体分层数据。",
  detail,
  selectedHorizon,
  pendingHorizon,
  loading,
}: {
  title?: string;
  description?: string;
  emptyText?: string;
  detail: StrategyPerformanceOverallScoreAnalysis | null | undefined;
  selectedHorizon: number;
  pendingHorizon: number;
  loading: boolean;
}) {
  return (
    <section className="strategy-performance-card">
      <div className="strategy-performance-section-head">
        <div>
          <h3>{title}</h3>
          <p>{description}</p>
          {loading && pendingHorizon !== selectedHorizon ? (
            <p>
              当前展示 {selectedHorizon} 日已加载结果，{pendingHorizon}{" "}
              日切换中。
            </p>
          ) : null}
        </div>
      </div>
      {!detail ? (
        <div className="strategy-performance-empty">{emptyText}</div>
      ) : (
        <>
          <div className="strategy-performance-detail-summary">
            <div>
              <span>持有周期</span>
              <strong>{formatNumber(detail.horizon, 0)} 日</strong>
            </div>
            <div>
              <span>样本数</span>
              <strong>{formatNumber(detail.sample_count, 0)}</strong>
            </div>
            <div>
              <span>均收益</span>
              <strong className={valueClassName(detail.avg_future_return_pct)}>
                {formatPercent(detail.avg_future_return_pct)}
              </strong>
            </div>
            <div>
              <span>强势命中率</span>
              <strong>{formatRate(detail.strong_hit_rate)}</strong>
            </div>
            <div>
              <span>胜率</span>
              <strong>{formatRate(detail.win_rate)}</strong>
            </div>
            <div>
              <span>全样本相关</span>
              <strong>{formatNumber(detail.spearman_corr, 3)}</strong>
            </div>
            <div>
              <span>IC</span>
              <strong className={valueClassName(detail.rank_ic_mean)}>
                {formatNumber(detail.rank_ic_mean, 3)}
              </strong>
            </div>
            <div>
              <span>ICIR</span>
              <strong className={valueClassName(detail.icir)}>
                {formatNumber(detail.icir, 2)}
              </strong>
            </div>
            <div>
              <span>分层差</span>
              <strong
                className={valueClassName(detail.layer_return_spread_pct)}
              >
                {formatPercent(detail.layer_return_spread_pct)}
              </strong>
            </div>
          </div>

          <div className="strategy-performance-table-wrap">
            <table className="strategy-performance-table">
              <thead>
                <tr>
                  <th>
                    {detail.bucket_mode === "score_value" ? "总分" : "总分桶"}
                  </th>
                  <th>样本数</th>
                  <th>均收益</th>
                  <th>强势命中率</th>
                  <th>胜率</th>
                </tr>
              </thead>
              <tbody>
                {detail.score_rows.map((row) => (
                  <tr key={row.bucket_label}>
                    <td>{row.bucket_label}</td>
                    <td>{formatNumber(row.sample_count, 0)}</td>
                    <td className={valueClassName(row.avg_future_return_pct)}>
                      {formatPercent(row.avg_future_return_pct)}
                    </td>
                    <td>{formatRate(row.strong_hit_rate)}</td>
                    <td>{formatRate(row.win_rate)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </section>
  );
}

function RuleTable({
  title,
  subtitle,
  rows,
  selectedHorizon,
  selectedRuleName,
  onPickRule,
}: {
  title: string;
  subtitle?: string;
  rows: StrategyPerformanceRuleRow[];
  selectedHorizon: number;
  selectedRuleName: string;
  onPickRule: (ruleName: string) => void;
}) {
  const hasNegativeNotes = rows.some(
    (row) => row.signal_direction === "negative",
  );
  const sortDefinitions = useMemo(
    () =>
      ({
        rule_name: {
          value: (row) => row.rule_name,
        },
        h2: {
          compare: (left, right) => compareRuleRows(left, right, 2),
        },
        h3: {
          compare: (left, right) => compareRuleRows(left, right, 3),
        },
        h5: {
          compare: (left, right) => compareRuleRows(left, right, 5),
        },
      }) satisfies Partial<
        Record<RuleTableSortKey, SortDefinition<StrategyPerformanceRuleRow>>
      >,
    [],
  );
  const {
    sortKey,
    sortDirection,
    sortedRows,
    toggleSort,
    setSortKey,
    setSortDirection,
  } = useTableSort<StrategyPerformanceRuleRow, RuleTableSortKey>(
    rows,
    sortDefinitions,
    {
      key: horizonSortKey(selectedHorizon),
      direction: "desc",
    },
  );

  useEffect(() => {
    if (!sortKey || sortKey.startsWith("h")) {
      setSortKey(horizonSortKey(selectedHorizon));
      setSortDirection("desc");
    }
  }, [selectedHorizon, setSortDirection, setSortKey, sortKey]);

  return (
    <section className="strategy-performance-card">
      <div className="strategy-performance-section-head">
        <div>
          <h3>{title}</h3>
          {subtitle ? <p>{subtitle}</p> : null}
        </div>
      </div>
      {rows.length === 0 ? (
        <div className="strategy-performance-empty">当前没有可展示的规则。</div>
      ) : (
        <div className="strategy-performance-table-wrap">
          <table className="strategy-performance-table">
            <thead>
              <tr>
                <th
                  className="strategy-performance-col-rule"
                  aria-sort={getAriaSort(
                    sortKey === "rule_name",
                    sortDirection,
                  )}
                >
                  <TableSortButton
                    label="规则"
                    isActive={sortKey === "rule_name"}
                    direction={sortDirection}
                    onClick={() => toggleSort("rule_name")}
                    title="按规则名排序"
                  />
                </th>
                <th className="strategy-performance-col-scope">口径</th>
                {HORIZON_OPTIONS.map((horizon) => (
                  <th
                    key={`horizon:${horizon}`}
                    aria-sort={getAriaSort(
                      sortKey === horizonSortKey(horizon),
                      sortDirection,
                    )}
                  >
                    <TableSortButton
                      label={`${horizon} 日`}
                      isActive={sortKey === horizonSortKey(horizon)}
                      direction={sortDirection}
                      onClick={() => toggleSort(horizonSortKey(horizon))}
                      title={`按 ${horizon} 日综合表现排序`}
                    />
                  </th>
                ))}
                <th className="strategy-performance-col-status">状态</th>
                {hasNegativeNotes ? (
                  <th className="strategy-performance-col-notes">判定说明</th>
                ) : null}
                <th>详情</th>
              </tr>
            </thead>
            <tbody>
              {sortedRows.map((row) => {
                const isSelected = selectedRuleName === row.rule_name;
                return (
                  <tr key={`${row.rule_name}:${row.signal_direction}`}>
                    <td className="strategy-performance-col-rule">
                      <div className="strategy-performance-rule-name">
                        <strong>{row.rule_name}</strong>
                        <div className="strategy-performance-inline-badges">
                          <StatusBadge
                            tone={
                              row.signal_direction === "positive"
                                ? "good"
                                : "warn"
                            }
                          >
                            {row.direction_label}
                          </StatusBadge>
                          {row.tag ? (
                            <StatusBadge tone="neutral">{row.tag}</StatusBadge>
                          ) : null}
                        </div>
                      </div>
                      <div className="strategy-performance-rule-meta">
                        {row.explain}
                      </div>
                    </td>
                    <td className="strategy-performance-col-scope">
                      <div className="strategy-performance-rule-meta">
                        <div>{row.scope_way ?? "--"}</div>
                        <div>窗口 {formatNumber(row.scope_windows, 0)}</div>
                        <div>points {formatNumber(row.points)}</div>
                        {row.has_dist_points ? <div>含 dist_points</div> : null}
                      </div>
                    </td>
                    {HORIZON_OPTIONS.map((horizon) => (
                      <td
                        key={`${row.rule_name}:${row.signal_direction}:${horizon}`}
                      >
                        <MetricCell row={row} horizon={horizon} />
                      </td>
                    ))}
                    <td className="strategy-performance-col-status">
                      <div className="strategy-performance-inline-badges">
                        {row.auto_candidate ? (
                          <StatusBadge tone="good">综合候选</StatusBadge>
                        ) : null}
                        {row.manually_selected ? (
                          <StatusBadge tone="neutral">手工</StatusBadge>
                        ) : null}
                        {row.in_advantage_set ? (
                          <StatusBadge tone="good">优势集</StatusBadge>
                        ) : null}
                        {row.in_companion_set ? (
                          <StatusBadge tone="neutral">伴随集</StatusBadge>
                        ) : null}
                        {row.negative_effectiveness_label ? (
                          <StatusBadge
                            tone={row.negative_effective ? "warn" : "neutral"}
                          >
                            {row.negative_effectiveness_label}
                          </StatusBadge>
                        ) : null}
                      </div>
                    </td>
                    {hasNegativeNotes ? (
                      <td className="strategy-performance-col-notes">
                        <div className="strategy-performance-note-list">
                          {(row.negative_review_notes ?? []).length > 0 ? (
                            (row.negative_review_notes ?? []).map((note) => (
                              <span
                                className="strategy-performance-note-pill"
                                key={`${row.rule_name}:${note}`}
                              >
                                {note}
                              </span>
                            ))
                          ) : (
                            <span className="strategy-performance-muted">
                              --
                            </span>
                          )}
                        </div>
                      </td>
                    ) : null}
                    <td>
                      <button
                        className={
                          isSelected
                            ? "strategy-performance-secondary-btn is-active"
                            : "strategy-performance-secondary-btn"
                        }
                        onClick={() => onPickRule(row.rule_name)}
                        type="button"
                      >
                        {isSelected ? "当前策略" : "查看"}
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

function CompanionTable({
  title,
  subtitle,
  rows,
  defaultSortDirection = "desc",
}: {
  title: string;
  subtitle?: string;
  rows: StrategyPerformanceCompanionRow[];
  defaultSortDirection?: "desc" | "asc";
}) {
  const sortDefinitions = useMemo(
    () =>
      ({
        rule_name: { value: (row) => row.rule_name },
        hit_n: { value: (row) => row.hit_n },
        avg_future_return_pct: {
          value: (row) => row.avg_future_return_pct,
        },
        eligible_pool_avg_return_pct: {
          value: (row) => row.eligible_pool_avg_return_pct,
        },
        delta_return_pct: { value: (row) => row.delta_return_pct },
        win_rate: { value: (row) => row.win_rate },
        delta_win_rate: { value: (row) => row.delta_win_rate },
      }) satisfies Partial<
        Record<
          CompanionTableSortKey,
          SortDefinition<StrategyPerformanceCompanionRow>
        >
      >,
    [],
  );
  const { sortKey, sortDirection, sortedRows, toggleSort } = useTableSort<
    StrategyPerformanceCompanionRow,
    CompanionTableSortKey
  >(rows, sortDefinitions, {
    key: "delta_return_pct",
    direction: defaultSortDirection,
  });

  return (
    <section className="strategy-performance-card">
      <div className="strategy-performance-section-head">
        <div>
          <h3>{title}</h3>
          {subtitle ? <p>{subtitle}</p> : null}
        </div>
      </div>
      {rows.length === 0 ? (
        <div className="strategy-performance-empty">当前条件下没有样本。</div>
      ) : (
        <div className="strategy-performance-table-wrap">
          <table className="strategy-performance-table">
            <thead>
              <tr>
                <th
                  aria-sort={getAriaSort(
                    sortKey === "rule_name",
                    sortDirection,
                  )}
                >
                  <TableSortButton
                    label="伴随策略"
                    isActive={sortKey === "rule_name"}
                    direction={sortDirection}
                    onClick={() => toggleSort("rule_name")}
                    title="按伴随策略名排序"
                  />
                </th>
                <th aria-sort={getAriaSort(sortKey === "hit_n", sortDirection)}>
                  <TableSortButton
                    label="命中样本"
                    isActive={sortKey === "hit_n"}
                    direction={sortDirection}
                    onClick={() => toggleSort("hit_n")}
                    title="按命中样本数排序"
                  />
                </th>
                <th
                  aria-sort={getAriaSort(
                    sortKey === "avg_future_return_pct",
                    sortDirection,
                  )}
                >
                  <TableSortButton
                    label="伴随均收益"
                    isActive={sortKey === "avg_future_return_pct"}
                    direction={sortDirection}
                    onClick={() => toggleSort("avg_future_return_pct")}
                    title="按伴随样本均收益排序"
                  />
                </th>
                <th
                  aria-sort={getAriaSort(
                    sortKey === "eligible_pool_avg_return_pct",
                    sortDirection,
                  )}
                >
                  <TableSortButton
                    label="池均收益"
                    isActive={sortKey === "eligible_pool_avg_return_pct"}
                    direction={sortDirection}
                    onClick={() => toggleSort("eligible_pool_avg_return_pct")}
                    title="按优势池均收益排序"
                  />
                </th>
                <th
                  aria-sort={getAriaSort(
                    sortKey === "delta_return_pct",
                    sortDirection,
                  )}
                >
                  <TableSortButton
                    label="收益差"
                    isActive={sortKey === "delta_return_pct"}
                    direction={sortDirection}
                    onClick={() => toggleSort("delta_return_pct")}
                    title="按伴随收益差排序"
                  />
                </th>
                <th
                  aria-sort={getAriaSort(sortKey === "win_rate", sortDirection)}
                >
                  <TableSortButton
                    label="伴随胜率"
                    isActive={sortKey === "win_rate"}
                    direction={sortDirection}
                    onClick={() => toggleSort("win_rate")}
                    title="按伴随胜率排序"
                  />
                </th>
                <th
                  aria-sort={getAriaSort(
                    sortKey === "delta_win_rate",
                    sortDirection,
                  )}
                >
                  <TableSortButton
                    label="胜率差"
                    isActive={sortKey === "delta_win_rate"}
                    direction={sortDirection}
                    onClick={() => toggleSort("delta_win_rate")}
                    title="按伴随胜率差排序"
                  />
                </th>
              </tr>
            </thead>
            <tbody>
              {sortedRows.map((row) => (
                <tr key={row.rule_name}>
                  <td>
                    <div className="strategy-performance-rule-name">
                      <strong>{row.rule_name}</strong>
                    </div>
                    {row.low_confidence ? (
                      <div className="strategy-performance-low-confidence">
                        低样本
                      </div>
                    ) : null}
                  </td>
                  <td>{formatNumber(row.hit_n, 0)}</td>
                  <td className={valueClassName(row.avg_future_return_pct)}>
                    {formatPercent(row.avg_future_return_pct)}
                  </td>
                  <td>{formatPercent(row.eligible_pool_avg_return_pct)}</td>
                  <td className={valueClassName(row.delta_return_pct)}>
                    {formatPercent(row.delta_return_pct)}
                  </td>
                  <td>{formatRate(row.win_rate)}</td>
                  <td className={valueClassName(row.delta_win_rate)}>
                    {formatRate(row.delta_win_rate)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

function ApplyActionCard({
  loading,
  hasPendingChanges,
  onApply,
}: {
  loading: boolean;
  hasPendingChanges: boolean;
  onApply: () => void;
}) {
  return (
    <div className="strategy-performance-actions-card">
      <div className="strategy-performance-actions">
        <button
          className="strategy-performance-primary-btn"
          disabled={loading}
          onClick={onApply}
          type="button"
        >
          {loading ? "统计中..." : "应用统计"}
        </button>
      </div>
      <div className="strategy-performance-note-strip">
        <span className="strategy-performance-note">
          {hasPendingChanges
            ? "参数已变更，重新应用后刷新自动优势集。"
            : "点击后会重新运行自动统计，并刷新自动优势集。"}
        </span>
      </div>
    </div>
  );
}

function RuleDirectionCard({
  detail,
}: {
  detail: StrategyPerformanceRuleDirectionDetail;
}) {
  return (
    <article className="strategy-performance-detail-card">
      <div className="strategy-performance-section-head">
        <div>
          <h4>{detail.direction_label}</h4>
        </div>
        <div className="strategy-performance-inline-badges">
          <StatusBadge
            tone={
              isHitVsNonHitScoreMode(detail.score_mode) ? "neutral" : "good"
            }
          >
            {isHitVsNonHitScoreMode(detail.score_mode)
              ? "Hit vs Non-hit 评分"
              : "IC / ICIR 评分"}
          </StatusBadge>
          {detail.has_dist_points ? (
            <StatusBadge tone="neutral">dist_points</StatusBadge>
          ) : null}
        </div>
      </div>
      <div className="strategy-performance-detail-summary">
        <div>
          <span>样本数</span>
          <strong>{formatNumber(detail.sample_count, 0)}</strong>
        </div>
        <div>
          <span>命中均收益</span>
          <strong className={valueClassName(detail.avg_future_return_pct)}>
            {formatPercent(detail.avg_future_return_pct)}
          </strong>
        </div>
        <div>
          <span>强势命中率</span>
          <strong>{formatRate(detail.strong_hit_rate)}</strong>
        </div>
        <div>
          <span>胜率</span>
          <strong>{formatRate(detail.win_rate)}</strong>
        </div>
        <div>
          <span>全样本相关</span>
          <strong>{formatNumber(detail.spearman_corr, 3)}</strong>
        </div>
        <div>
          <span>Hit vs Non-hit</span>
          <strong className={valueClassName(detail.hit_vs_non_hit_delta_pct)}>
            {formatPercent(detail.hit_vs_non_hit_delta_pct)}
          </strong>
        </div>
        <div>
          <span>
            {isHitVsNonHitScoreMode(detail.score_mode) ? "参考 IC" : "IC"}
          </span>
          <strong className={valueClassName(detail.rank_ic_mean)}>
            {formatNumber(detail.rank_ic_mean, 3)}
          </strong>
        </div>
        <div>
          <span>
            {isHitVsNonHitScoreMode(detail.score_mode) ? "参考 ICIR" : "ICIR"}
          </span>
          <strong className={valueClassName(detail.icir)}>
            {formatNumber(detail.icir, 2)}
          </strong>
        </div>
        <div>
          <span>Sharpe</span>
          <strong className={valueClassName(detail.sharpe_ratio)}>
            {formatNumber(detail.sharpe_ratio, 2)}
          </strong>
        </div>
      </div>

      <div className="strategy-performance-detail-split">
        <div className="strategy-performance-table-wrap">
          <table className="strategy-performance-table">
            <thead>
              <tr>
                <th>
                  {detail.bucket_mode === "score_value" ? "score" : "score 桶"}
                </th>
                <th>样本数</th>
                <th>均收益</th>
                <th>强势命中率</th>
                <th>胜率</th>
              </tr>
            </thead>
            <tbody>
              {detail.score_rows.map((row) => (
                <tr key={row.bucket_label}>
                  <td>{row.bucket_label}</td>
                  <td>{formatNumber(row.sample_count, 0)}</td>
                  <td className={valueClassName(row.avg_future_return_pct)}>
                    {formatPercent(row.avg_future_return_pct)}
                  </td>
                  <td>{formatRate(row.strong_hit_rate)}</td>
                  <td>{formatRate(row.win_rate)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="strategy-performance-table-wrap">
          <table className="strategy-performance-table">
            <thead>
              <tr>
                <th>hit_count</th>
                <th>样本数</th>
                <th>均收益</th>
                <th>强势命中率</th>
                <th>胜率</th>
              </tr>
            </thead>
            <tbody>
              {detail.hit_count_rows.length > 0 ? (
                detail.hit_count_rows.map((row) => (
                  <tr key={row.hit_count}>
                    <td>{formatNumber(row.hit_count, 0)}</td>
                    <td>{formatNumber(row.sample_count, 0)}</td>
                    <td className={valueClassName(row.avg_future_return_pct)}>
                      {formatPercent(row.avg_future_return_pct)}
                    </td>
                    <td>{formatRate(row.strong_hit_rate)}</td>
                    <td>{formatRate(row.win_rate)}</td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={5} className="strategy-performance-empty-cell">
                    当前规则不支持按 hit_count 可靠回推。
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </article>
  );
}

function RuleDetailModal({
  detail,
  loading,
  error,
  onClose,
}: {
  detail: StrategyPerformanceRuleDetail | null;
  loading: boolean;
  error: string;
  onClose: () => void;
}) {
  if (typeof document === "undefined") {
    return null;
  }

  return createPortal(
    <div
      className="strategy-performance-modal-overlay"
      role="presentation"
      onClick={onClose}
    >
      <div
        className="strategy-performance-modal-shell"
        role="dialog"
        aria-modal="true"
        aria-labelledby="strategy-performance-rule-detail-title"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="strategy-performance-modal-close-wrap">
          <button
            type="button"
            className="strategy-performance-modal-close"
            onClick={onClose}
            aria-label="关闭策略得分影响"
          >
            关闭
          </button>
        </div>

        <section className="strategy-performance-card">
          <div className="strategy-performance-section-head">
            <div>
              <h3 id="strategy-performance-rule-detail-title">策略得分影响</h3>
            </div>
          </div>

          {loading ? (
            <div className="strategy-performance-empty">
              正在读取单策略明细...
            </div>
          ) : error ? (
            <div className="strategy-performance-error">{error}</div>
          ) : !detail ? (
            <div className="strategy-performance-empty">
              当前没有可展示的单策略得分影响数据。
            </div>
          ) : (
            <div className="strategy-performance-detail-shell">
              <div className="strategy-performance-inline-badges">
                <StatusBadge tone="neutral">
                  规则 {detail.rule_name}
                </StatusBadge>
                <StatusBadge tone="neutral">
                  持有周期 {detail.horizon} 日
                </StatusBadge>
                {detail.scope_way ? (
                  <StatusBadge tone="neutral">{detail.scope_way}</StatusBadge>
                ) : null}
                {detail.has_dist_points ? (
                  <StatusBadge tone="neutral">含 dist_points</StatusBadge>
                ) : null}
              </div>
              <div className="strategy-performance-note-box">
                {detail.explain || "--"}
              </div>
              {detail.directions.map((directionDetail) => (
                <RuleDirectionCard
                  detail={directionDetail}
                  key={`${detail.rule_name}:${directionDetail.signal_direction}`}
                />
              ))}
            </div>
          )}
        </section>
      </div>
    </div>,
    document.body,
  );
}

export default function StrategyPerformanceBacktestPage() {
  const persistedState = useMemo(() => {
    if (strategyPerformanceRuntimeState) {
      return strategyPerformanceRuntimeState;
    }
    if (typeof window === "undefined") {
      return null;
    }
    return readJsonStorage<Partial<PersistedState>>(
      window.localStorage,
      STRATEGY_PERFORMANCE_STATE_KEY,
    );
  }, []);
  const useMigratedAutoMinSampleDefaults =
    hasLegacyAutoMinSampleStrings(persistedState);

  const [sourcePath, setSourcePath] = useState(
    () => persistedState?.sourcePath ?? readStoredSourcePath(),
  );
  const [selectedHorizonInput, setSelectedHorizonInput] = useState(
    persistedState?.selectedHorizon ?? "2",
  );
  const [strongQuantileInput, setStrongQuantileInput] = useState(
    persistedState?.strongQuantile ?? "0.9",
  );
  const [manualRuleNames, setManualRuleNames] = useState<string[]>(() =>
    arrayFromUnknown(persistedState?.manualRuleNames),
  );
  const [strategyKeyword, setStrategyKeyword] = useState(
    persistedState?.strategyKeyword ?? "",
  );
  const [autoMinSamples2, setAutoMinSamples2] = useState(() =>
    useMigratedAutoMinSampleDefaults
      ? String(DEFAULT_AUTO_MIN_SAMPLES[2])
      : (persistedState?.autoMinSamples2 ??
        String(DEFAULT_AUTO_MIN_SAMPLES[2])),
  );
  const [autoMinSamples3, setAutoMinSamples3] = useState(() =>
    useMigratedAutoMinSampleDefaults
      ? String(DEFAULT_AUTO_MIN_SAMPLES[3])
      : (persistedState?.autoMinSamples3 ??
        String(DEFAULT_AUTO_MIN_SAMPLES[3])),
  );
  const [autoMinSamples5, setAutoMinSamples5] = useState(() =>
    useMigratedAutoMinSampleDefaults
      ? String(DEFAULT_AUTO_MIN_SAMPLES[5])
      : (persistedState?.autoMinSamples5 ??
        String(DEFAULT_AUTO_MIN_SAMPLES[5])),
  );
  const [autoMinSamples10] = useState(() =>
    useMigratedAutoMinSampleDefaults
      ? String(DEFAULT_AUTO_MIN_SAMPLES[10])
      : (persistedState?.autoMinSamples10 ??
        String(DEFAULT_AUTO_MIN_SAMPLES[10])),
  );
  const [requireWinRateAboveMarket, setRequireWinRateAboveMarket] = useState(
    persistedState?.requireWinRateAboveMarket ?? false,
  );
  const [minPassHorizonsInput, setMinPassHorizonsInput] = useState(
    persistedState?.minPassHorizons ?? "2",
  );
  const [minAdvHitsInput, setMinAdvHitsInput] = useState(
    persistedState?.minAdvHits ?? "1",
  );
  const [topLimitInput, setTopLimitInput] = useState(
    persistedState?.topLimit ?? "100",
  );
  const [selectedRuleNameInput, setSelectedRuleNameInput] = useState(
    persistedState?.selectedRuleName ?? "",
  );
  const [pageData, setPageData] = useState<StrategyPerformancePageData | null>(
    () =>
      persistedState?.pageData && "strategy_options" in persistedState.pageData
        ? (persistedState.pageData as StrategyPerformancePageData)
        : restorePageDataFromStorage(
            persistedState?.pageData as
              | PersistedStrategyPerformancePageData
              | StrategyPerformancePageData
              | null
              | undefined,
          ),
  );
  const [submittedQuery, setSubmittedQuery] = useState<SubmittedQuery | null>(
    () => {
      const query = persistedState?.submittedQuery;
      if (!query || typeof query !== "object") {
        return null;
      }
      const sourcePath =
        typeof query.sourcePath === "string" ? query.sourcePath.trim() : "";
      if (!sourcePath) {
        return null;
      }
      const useMigratedQueryAutoMinSamples =
        hasLegacyAutoMinSampleNumbers(query);
      return {
        sourcePath,
        selectedHorizon:
          typeof query.selectedHorizon === "number" ? query.selectedHorizon : 2,
        strongQuantile:
          typeof query.strongQuantile === "number" ? query.strongQuantile : 0.9,
        autoMinSamples2: useMigratedQueryAutoMinSamples
          ? DEFAULT_AUTO_MIN_SAMPLES[2]
          : typeof query.autoMinSamples2 === "number"
            ? query.autoMinSamples2
            : DEFAULT_AUTO_MIN_SAMPLES[2],
        autoMinSamples3: useMigratedQueryAutoMinSamples
          ? DEFAULT_AUTO_MIN_SAMPLES[3]
          : typeof query.autoMinSamples3 === "number"
            ? query.autoMinSamples3
            : DEFAULT_AUTO_MIN_SAMPLES[3],
        autoMinSamples5: useMigratedQueryAutoMinSamples
          ? DEFAULT_AUTO_MIN_SAMPLES[5]
          : typeof query.autoMinSamples5 === "number"
            ? query.autoMinSamples5
            : DEFAULT_AUTO_MIN_SAMPLES[5],
        autoMinSamples10: useMigratedQueryAutoMinSamples
          ? DEFAULT_AUTO_MIN_SAMPLES[10]
          : typeof query.autoMinSamples10 === "number"
            ? query.autoMinSamples10
            : DEFAULT_AUTO_MIN_SAMPLES[10],
        requireWinRateAboveMarket: query.requireWinRateAboveMarket === true,
        minPassHorizons:
          typeof query.minPassHorizons === "number" ? query.minPassHorizons : 2,
        minAdvHits: typeof query.minAdvHits === "number" ? query.minAdvHits : 1,
        topLimit: typeof query.topLimit === "number" ? query.topLimit : 100,
      };
    },
  );
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [ruleDetailOpen, setRuleDetailOpen] = useState(false);
  const [ruleDetailData, setRuleDetailData] =
    useState<StrategyPerformanceRuleDetail | null>(null);
  const [ruleDetailLoading, setRuleDetailLoading] = useState(false);
  const [ruleDetailError, setRuleDetailError] = useState("");
  const [ruleDetailCache, setRuleDetailCache] = useState<
    Record<string, StrategyPerformanceRuleDetail | null>
  >({});
  const [syncedManualSelection, setSyncedManualSelection] =
    useState<StrategyPerformanceManualAdvantageSelection | null>(() =>
      readStrategyPerformanceAdvancedPickSynced(),
    );
  const [manualValidationData, setManualValidationData] =
    useState<StrategyPerformanceHorizonViewData | null>(null);
  const [manualSyncLoading, setManualSyncLoading] = useState(false);
  const [manualSyncError, setManualSyncError] = useState("");
  const [manualValidationLoading, setManualValidationLoading] = useState(false);
  const [manualValidationError, setManualValidationError] = useState("");
  const [manualValidationSelection, setManualValidationSelection] =
    useState<StrategyPerformanceManualAdvantageSelection | null>(null);
  const manualSyncRequestIdRef = useRef(0);

  const sourcePathTrimmed = sourcePath.trim();

  const positiveRuleNames = useMemo(
    () =>
      (pageData?.rule_rows ?? [])
        .filter(hasPositiveHits)
        .map((row) => row.rule_name),
    [pageData],
  );
  const autoAdvantageRuleNames = useMemo(
    () =>
      normalizeStringArray(
        (pageData?.auto_advantage_rule_names ?? []).filter((item) =>
          positiveRuleNames.includes(item),
        ),
      ),
    [pageData, positiveRuleNames],
  );
  const autoAdvantageRuleNameSet = useMemo(
    () => new Set(autoAdvantageRuleNames),
    [autoAdvantageRuleNames],
  );
  const draftManualAdvantageRuleNames = useMemo(
    () =>
      normalizeStringArray(
        manualRuleNames.filter((item) => positiveRuleNames.includes(item)),
      ),
    [manualRuleNames, positiveRuleNames],
  );
  const draftManualAdvantageRuleNameSet = useMemo(
    () => new Set(draftManualAdvantageRuleNames),
    [draftManualAdvantageRuleNames],
  );
  const currentCandidateRuleNames = useMemo(
    () =>
      normalizeStringArray(
        positiveRuleNames.filter(
          (item) => !draftManualAdvantageRuleNameSet.has(item),
        ),
      ),
    [draftManualAdvantageRuleNameSet, positiveRuleNames],
  );
  const filteredAutoAdvantageRuleNames = useMemo(() => {
    const keyword = strategyKeyword.trim().toLowerCase();
    if (!keyword) {
      return autoAdvantageRuleNames;
    }
    return autoAdvantageRuleNames.filter((item) =>
      item.toLowerCase().includes(keyword),
    );
  }, [autoAdvantageRuleNames, strategyKeyword]);
  const filteredManualAdvantageRuleNames = useMemo(() => {
    const keyword = strategyKeyword.trim().toLowerCase();
    if (!keyword) {
      return draftManualAdvantageRuleNames;
    }
    return draftManualAdvantageRuleNames.filter((item) =>
      item.toLowerCase().includes(keyword),
    );
  }, [draftManualAdvantageRuleNames, strategyKeyword]);
  const filteredCurrentCandidateRuleNames = useMemo(() => {
    const keyword = strategyKeyword.trim().toLowerCase();
    if (!keyword) {
      return currentCandidateRuleNames;
    }
    return currentCandidateRuleNames.filter((item) =>
      item.toLowerCase().includes(keyword),
    );
  }, [currentCandidateRuleNames, strategyKeyword]);
  const manualAdvantageSelectionDraft =
    useMemo<StrategyPerformanceManualAdvantageSelection | null>(() => {
      if (!sourcePathTrimmed) {
        return null;
      }
      return {
        sourcePath: sourcePathTrimmed,
        selectedHorizon: parsePositiveInt(selectedHorizonInput, 2),
        strongQuantile: parseQuantile(strongQuantileInput),
        manualAdvantageRuleNames: draftManualAdvantageRuleNames,
        autoMinSamples2: parsePositiveInt(
          autoMinSamples2,
          DEFAULT_AUTO_MIN_SAMPLES[2],
        ),
        autoMinSamples3: parsePositiveInt(
          autoMinSamples3,
          DEFAULT_AUTO_MIN_SAMPLES[3],
        ),
        autoMinSamples5: parsePositiveInt(
          autoMinSamples5,
          DEFAULT_AUTO_MIN_SAMPLES[5],
        ),
        autoMinSamples10: parsePositiveInt(
          autoMinSamples10,
          DEFAULT_AUTO_MIN_SAMPLES[10],
        ),
        requireWinRateAboveMarket,
        minPassHorizons: parsePositiveInt(minPassHorizonsInput, 2),
        minAdvHits: parsePositiveInt(minAdvHitsInput, 1),
      };
    }, [
      autoMinSamples2,
      autoMinSamples3,
      autoMinSamples5,
      autoMinSamples10,
      draftManualAdvantageRuleNames,
      minAdvHitsInput,
      minPassHorizonsInput,
      requireWinRateAboveMarket,
      selectedHorizonInput,
      sourcePathTrimmed,
      strongQuantileInput,
    ]);
  const syncedManualSelectionForSource = useMemo(
    () =>
      syncedManualSelection?.sourcePath === sourcePathTrimmed
        ? syncedManualSelection
        : null,
    [sourcePathTrimmed, syncedManualSelection],
  );
  const manualValidationSelectionForSource = useMemo(
    () =>
      manualValidationSelection?.sourcePath === sourcePathTrimmed
        ? manualValidationSelection
        : null,
    [manualValidationSelection, sourcePathTrimmed],
  );
  const isManualDraftSynced = useMemo(
    () =>
      sameManualAdvantageSelection(
        manualAdvantageSelectionDraft,
        syncedManualSelectionForSource,
      ),
    [manualAdvantageSelectionDraft, syncedManualSelectionForSource],
  );

  const buildSubmittedQuery = (
    overrides?: Partial<Pick<SubmittedQuery, "selectedHorizon">>,
  ): SubmittedQuery | null => {
    if (!sourcePathTrimmed) {
      return null;
    }
    const selectedHorizon =
      overrides?.selectedHorizon ?? parsePositiveInt(selectedHorizonInput, 2);
    const strongQuantile = parseQuantile(strongQuantileInput);
    return {
      sourcePath: sourcePathTrimmed,
      selectedHorizon,
      strongQuantile,
      autoMinSamples2: parsePositiveInt(
        autoMinSamples2,
        DEFAULT_AUTO_MIN_SAMPLES[2],
      ),
      autoMinSamples3: parsePositiveInt(
        autoMinSamples3,
        DEFAULT_AUTO_MIN_SAMPLES[3],
      ),
      autoMinSamples5: parsePositiveInt(
        autoMinSamples5,
        DEFAULT_AUTO_MIN_SAMPLES[5],
      ),
      autoMinSamples10: parsePositiveInt(
        autoMinSamples10,
        DEFAULT_AUTO_MIN_SAMPLES[10],
      ),
      requireWinRateAboveMarket,
      minPassHorizons: parsePositiveInt(minPassHorizonsInput, 2),
      minAdvHits: parsePositiveInt(minAdvHitsInput, 1),
      topLimit: parsePositiveInt(topLimitInput, 100),
    };
  };

  const runPageQuery = async (nextQuery: SubmittedQuery) => {
    setLoading(true);
    setError("");
    try {
      const nextPageData = await getStrategyPerformancePage({
        ...nextQuery,
        advantageRuleMode: "auto",
      });
      const nextRuleName =
        selectedRuleNameInput &&
        nextPageData.strategy_options.includes(selectedRuleNameInput)
          ? selectedRuleNameInput
          : (nextPageData.selected_rule_name ?? "");
      setPageData(nextPageData);
      setSubmittedQuery(nextQuery);
      setSelectedRuleNameInput((current) => {
        if (current && nextPageData.strategy_options.includes(current)) {
          return current;
        }
        return nextPageData.selected_rule_name ?? current;
      });
      if (ruleDetailOpen && nextRuleName) {
        void loadRuleDetail(nextRuleName, nextQuery.selectedHorizon);
      }
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      setLoading(false);
    }
  };

  const runHorizonQuery = async (nextQuery: SubmittedQuery) => {
    if (!pageData) {
      await runPageQuery(nextQuery);
      return;
    }
    setLoading(true);
    setError("");
    try {
      const nextHorizonData = await getStrategyPerformanceHorizonView({
        sourcePath: nextQuery.sourcePath,
        selectedHorizon: nextQuery.selectedHorizon,
        strongQuantile: nextQuery.strongQuantile,
        resolvedAdvantageRuleNames: pageData.resolved_advantage_rule_names,
        autoMinSamples2: nextQuery.autoMinSamples2,
        autoMinSamples3: nextQuery.autoMinSamples3,
        autoMinSamples5: nextQuery.autoMinSamples5,
        autoMinSamples10: nextQuery.autoMinSamples10,
        requireWinRateAboveMarket: nextQuery.requireWinRateAboveMarket,
        minPassHorizons: nextQuery.minPassHorizons,
        minAdvHits: nextQuery.minAdvHits,
        noisyCompanionRuleNames: pageData.noisy_companion_rule_names,
      });
      setPageData((current) =>
        current
          ? {
              ...current,
              selected_horizon: nextHorizonData.selected_horizon,
              noisy_companion_rule_names:
                nextHorizonData.noisy_companion_rule_names,
              companion_rows: nextHorizonData.companion_rows,
              overall_score_analysis:
                nextHorizonData.overall_score_analysis ?? null,
            }
          : current,
      );
      setSubmittedQuery(nextQuery);
      if (ruleDetailOpen && selectedRuleNameInput) {
        void loadRuleDetail(selectedRuleNameInput, nextQuery.selectedHorizon);
      }
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      setLoading(false);
    }
  };

  const applyFilters = () => {
    const nextQuery = buildSubmittedQuery();
    if (!nextQuery) {
      setError("缺少可用的数据源路径");
      return;
    }
    setError("");
    void runPageQuery(nextQuery);
  };

  const switchHorizon = (horizon: number) => {
    const nextHorizon = String(horizon);
    setSelectedHorizonInput(nextHorizon);
    const nextQuery = buildSubmittedQuery({ selectedHorizon: horizon });
    if (!nextQuery) {
      return;
    }
    if (sameSubmittedQuery(nextQuery, submittedQuery) && pageData) {
      return;
    }
    if (
      sameSubmittedQueryExceptHorizon(nextQuery, submittedQuery) &&
      pageData
    ) {
      void runHorizonQuery(nextQuery);
      return;
    }
    void runPageQuery(nextQuery);
  };

  const hasPendingChanges = useMemo(() => {
    if (!pageData) {
      return true;
    }
    if (!submittedQuery) {
      return true;
    }
    const currentQuery = buildSubmittedQuery();
    if (!currentQuery) {
      return true;
    }
    return (
      submittedQuery.sourcePath !== currentQuery.sourcePath ||
      submittedQuery.selectedHorizon !== currentQuery.selectedHorizon ||
      submittedQuery.strongQuantile !== currentQuery.strongQuantile ||
      submittedQuery.autoMinSamples2 !== currentQuery.autoMinSamples2 ||
      submittedQuery.autoMinSamples3 !== currentQuery.autoMinSamples3 ||
      submittedQuery.autoMinSamples5 !== currentQuery.autoMinSamples5 ||
      submittedQuery.autoMinSamples10 !== currentQuery.autoMinSamples10 ||
      submittedQuery.requireWinRateAboveMarket !==
        currentQuery.requireWinRateAboveMarket ||
      submittedQuery.minPassHorizons !== currentQuery.minPassHorizons ||
      submittedQuery.minAdvHits !== currentQuery.minAdvHits ||
      submittedQuery.topLimit !== currentQuery.topLimit
    );
  }, [
    autoMinSamples2,
    autoMinSamples3,
    autoMinSamples10,
    autoMinSamples5,
    minAdvHitsInput,
    minPassHorizonsInput,
    requireWinRateAboveMarket,
    selectedHorizonInput,
    sourcePathTrimmed,
    strongQuantileInput,
    submittedQuery,
    topLimitInput,
  ]);

  useEffect(() => {
    let cancelled = false;
    void ensureManagedSourcePath()
      .then((nextPath) => {
        if (!cancelled) {
          setSourcePath(nextPath);
        }
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    writeStrategyPerformanceAdvancedPickDraft(
      window.localStorage,
      manualAdvantageSelectionDraft,
    );
  }, [manualAdvantageSelectionDraft]);

  useEffect(() => {
    setManualSyncError("");
    setManualValidationError("");
  }, [manualAdvantageSelectionDraft]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    strategyPerformanceRuntimeState = {
      sourcePath: sourcePathTrimmed,
      selectedHorizon: selectedHorizonInput,
      strongQuantile: strongQuantileInput,
      manualRuleNames,
      strategyKeyword,
      autoMinSamples2,
      autoMinSamples3,
      autoMinSamples5,
      autoMinSamples10,
      requireWinRateAboveMarket,
      minPassHorizons: minPassHorizonsInput,
      minAdvHits: minAdvHitsInput,
      topLimit: topLimitInput,
      selectedRuleName: selectedRuleNameInput,
      pageData,
      submittedQuery,
    };
    const compactPageData = compactPageDataForStorage(pageData);
    const payload = {
      sourcePath: sourcePathTrimmed,
      selectedHorizon: selectedHorizonInput,
      strongQuantile: strongQuantileInput,
      manualRuleNames,
      strategyKeyword,
      autoMinSamples2,
      autoMinSamples3,
      autoMinSamples5,
      autoMinSamples10,
      requireWinRateAboveMarket,
      minPassHorizons: minPassHorizonsInput,
      minAdvHits: minAdvHitsInput,
      topLimit: topLimitInput,
      selectedRuleName: selectedRuleNameInput,
      pageData: compactPageData,
      submittedQuery,
    } satisfies PersistedState;
    if (
      !writeJsonStorage(
        window.localStorage,
        STRATEGY_PERFORMANCE_STATE_KEY,
        payload,
      )
    ) {
      writeJsonStorage(window.localStorage, STRATEGY_PERFORMANCE_STATE_KEY, {
        ...payload,
        pageData: null,
      } satisfies PersistedState);
    }
  }, [
    autoMinSamples2,
    autoMinSamples3,
    autoMinSamples10,
    autoMinSamples5,
    manualRuleNames,
    minAdvHitsInput,
    minPassHorizonsInput,
    pageData,
    requireWinRateAboveMarket,
    selectedHorizonInput,
    selectedRuleNameInput,
    sourcePathTrimmed,
    strategyKeyword,
    strongQuantileInput,
    submittedQuery,
    topLimitInput,
  ]);

  const advantageRuleRows = useMemo(() => {
    return (pageData?.rule_rows ?? []).filter(
      (row) => row.signal_direction === "positive" && row.in_advantage_set,
    );
  }, [pageData]);
  const companionRuleRows = useMemo(() => {
    return (pageData?.rule_rows ?? []).filter(
      (row) => row.signal_direction === "positive" && row.in_companion_set,
    );
  }, [pageData]);
  const effectiveNegativeRuleRows = useMemo(() => {
    return (pageData?.rule_rows ?? []).filter(
      (row) => row.signal_direction === "negative" && row.negative_effective,
    );
  }, [pageData]);
  const ineffectiveNegativeRuleRows = useMemo(() => {
    return (pageData?.rule_rows ?? []).filter(
      (row) =>
        row.signal_direction === "negative" && row.negative_effective === false,
    );
  }, [pageData]);
  const enhancingCompanionRows = useMemo(
    () =>
      (pageData?.companion_rows ?? []).filter(
        (row) => (row.delta_return_pct ?? Number.NEGATIVE_INFINITY) >= 0,
      ),
    [pageData],
  );
  const noisyCompanionRows = useMemo(
    () =>
      (pageData?.companion_rows ?? []).filter(
        (row) => (row.delta_return_pct ?? Number.POSITIVE_INFINITY) < 0,
      ),
    [pageData],
  );
  const loadedSelectedHorizon =
    pageData?.selected_horizon ??
    submittedQuery?.selectedHorizon ??
    parsePositiveInt(selectedHorizonInput, 2);
  const selectedSummary = useMemo(
    () => findSummary(pageData?.future_summaries ?? [], loadedSelectedHorizon),
    [loadedSelectedHorizon, pageData],
  );
  const pendingSelectedHorizon = parsePositiveInt(selectedHorizonInput, 2);

  const buildManualAdvantageSelectionDraft = (
    nextManualRuleNames: string[],
  ): StrategyPerformanceManualAdvantageSelection | null => {
    if (!sourcePathTrimmed) {
      return null;
    }
    const nextPositiveManualRuleNames = normalizeStringArray(
      nextManualRuleNames.filter((item) => positiveRuleNames.includes(item)),
    );
    return {
      sourcePath: sourcePathTrimmed,
      selectedHorizon: parsePositiveInt(selectedHorizonInput, 2),
      strongQuantile: parseQuantile(strongQuantileInput),
      manualAdvantageRuleNames: nextPositiveManualRuleNames,
      autoMinSamples2: parsePositiveInt(
        autoMinSamples2,
        DEFAULT_AUTO_MIN_SAMPLES[2],
      ),
      autoMinSamples3: parsePositiveInt(
        autoMinSamples3,
        DEFAULT_AUTO_MIN_SAMPLES[3],
      ),
      autoMinSamples5: parsePositiveInt(
        autoMinSamples5,
        DEFAULT_AUTO_MIN_SAMPLES[5],
      ),
      autoMinSamples10: parsePositiveInt(
        autoMinSamples10,
        DEFAULT_AUTO_MIN_SAMPLES[10],
      ),
      requireWinRateAboveMarket,
      minPassHorizons: parsePositiveInt(minPassHorizonsInput, 2),
      minAdvHits: parsePositiveInt(minAdvHitsInput, 1),
    };
  };

  const applySyncedManualSelection = (
    nextSelection: StrategyPerformanceManualAdvantageSelection | null,
  ) => {
    if (typeof window !== "undefined") {
      writeStrategyPerformanceAdvancedPickSynced(
        window.localStorage,
        nextSelection,
      );
    }
    setSyncedManualSelection(nextSelection);
  };

  const syncManualAdvantageSelection = async (
    nextSelection: StrategyPerformanceManualAdvantageSelection | null,
  ) => {
    const requestId = manualSyncRequestIdRef.current + 1;
    manualSyncRequestIdRef.current = requestId;
    applySyncedManualSelection(null);
    setManualValidationSelection(null);
    setManualValidationData(null);
    setManualValidationError("");
    if (!nextSelection || nextSelection.manualAdvantageRuleNames.length === 0) {
      setManualSyncLoading(false);
      setManualSyncError("");
      return;
    }
    setManualSyncLoading(true);
    setManualSyncError("");
    try {
      const manualPickCache = await saveManualStrategyPickCache({
        sourcePath: nextSelection.sourcePath,
        selectedHorizon: nextSelection.selectedHorizon,
        strongQuantile: nextSelection.strongQuantile,
        manualRuleNames: nextSelection.manualAdvantageRuleNames,
        autoMinSamples2: nextSelection.autoMinSamples2,
        autoMinSamples3: nextSelection.autoMinSamples3,
        autoMinSamples5: nextSelection.autoMinSamples5,
        autoMinSamples10: nextSelection.autoMinSamples10,
        requireWinRateAboveMarket: nextSelection.requireWinRateAboveMarket,
        minPassHorizons: nextSelection.minPassHorizons,
        minAdvHits: nextSelection.minAdvHits,
      });
      if (manualSyncRequestIdRef.current !== requestId) {
        return;
      }
      applySyncedManualSelection({
        ...nextSelection,
        selectedHorizon: manualPickCache.selected_horizon,
        strongQuantile: manualPickCache.strong_quantile,
        manualAdvantageRuleNames: normalizeStringArray(
          (manualPickCache.manual_rule_names ?? []).length > 0
            ? manualPickCache.manual_rule_names ?? []
            : nextSelection.manualAdvantageRuleNames,
        ),
      });
    } catch (reason) {
      if (manualSyncRequestIdRef.current !== requestId) {
        return;
      }
      setManualSyncError(
        reason instanceof Error ? reason.message : String(reason),
      );
    } finally {
      if (manualSyncRequestIdRef.current === requestId) {
        setManualSyncLoading(false);
      }
    }
  };

  const applyManualRuleNames = (nextManualRuleNames: string[]) => {
    const normalizedManualRuleNames = normalizeStringArray(nextManualRuleNames);
    if (
      normalizedManualRuleNames.length === manualRuleNames.length &&
      normalizedManualRuleNames.every(
        (item, index) => item === manualRuleNames[index],
      )
    ) {
      return;
    }
    const nextSelection =
      buildManualAdvantageSelectionDraft(normalizedManualRuleNames);
    setManualRuleNames(normalizedManualRuleNames);
    if (typeof window !== "undefined") {
      writeStrategyPerformanceAdvancedPickDraft(
        window.localStorage,
        nextSelection,
      );
    }
    void syncManualAdvantageSelection(nextSelection);
  };

  const addRuleToManualAdvantage = (ruleName: string) => {
    if (manualRuleNames.includes(ruleName)) {
      return;
    }
    applyManualRuleNames([...manualRuleNames, ruleName]);
  };

  const removeRuleFromManualAdvantage = (ruleName: string) => {
    if (!manualRuleNames.includes(ruleName)) {
      return;
    }
    applyManualRuleNames(manualRuleNames.filter((item) => item !== ruleName));
  };

  const validateManualAdvantageSet = async () => {
    if (manualSyncLoading) {
      setManualValidationError("手动优势集缓存还在自动同步，请稍后再验证。");
      return;
    }
    const cachedSelection = syncedManualSelectionForSource;
    if (!cachedSelection || !isManualDraftSynced) {
      setManualValidationError(
        "当前手动优势集还没有同步到缓存，请先调整手动优势集并等待自动同步完成。",
      );
      return;
    }
    if (cachedSelection.manualAdvantageRuleNames.length === 0) {
      setManualValidationError("当前已同步缓存里没有手动优势集。");
      return;
    }
    setManualValidationLoading(true);
    setManualValidationError("");
    try {
      const manualPickCache = await getStrategyPickCache({
        sourcePath: cachedSelection.sourcePath,
        selectedHorizon: cachedSelection.selectedHorizon,
        strongQuantile: cachedSelection.strongQuantile,
        advantageRuleMode: "manual",
        manualRuleNames: cachedSelection.manualAdvantageRuleNames,
        autoMinSamples2: cachedSelection.autoMinSamples2,
        autoMinSamples3: cachedSelection.autoMinSamples3,
        autoMinSamples5: cachedSelection.autoMinSamples5,
        autoMinSamples10: cachedSelection.autoMinSamples10,
        requireWinRateAboveMarket:
          cachedSelection.requireWinRateAboveMarket,
        minPassHorizons: cachedSelection.minPassHorizons,
        minAdvHits: cachedSelection.minAdvHits,
      });
      const nextValidationData = await getStrategyPerformanceHorizonView({
        sourcePath: cachedSelection.sourcePath,
        selectedHorizon: cachedSelection.selectedHorizon,
        strongQuantile: cachedSelection.strongQuantile,
        resolvedAdvantageRuleNames:
          manualPickCache.resolved_advantage_rule_names,
        autoMinSamples2: cachedSelection.autoMinSamples2,
        autoMinSamples3: cachedSelection.autoMinSamples3,
        autoMinSamples5: cachedSelection.autoMinSamples5,
        autoMinSamples10: cachedSelection.autoMinSamples10,
        requireWinRateAboveMarket:
          cachedSelection.requireWinRateAboveMarket,
        minPassHorizons: cachedSelection.minPassHorizons,
        minAdvHits: cachedSelection.minAdvHits,
      });
      setManualValidationSelection(cachedSelection);
      setManualValidationData(nextValidationData);
    } catch (reason) {
      setManualValidationError(
        reason instanceof Error ? reason.message : String(reason),
      );
    } finally {
      setManualValidationLoading(false);
    }
  };

  const loadRuleDetail = async (ruleName: string, horizonOverride?: number) => {
    const normalizedRuleName = ruleName.trim();
    if (!sourcePathTrimmed || !normalizedRuleName) {
      setRuleDetailError("缺少可用的数据源路径或策略名");
      setRuleDetailData(null);
      return;
    }
    const detailHorizon =
      horizonOverride ?? parsePositiveInt(selectedHorizonInput, 2);
    const cacheKey = [
      sourcePathTrimmed,
      detailHorizon,
      parseQuantile(strongQuantileInput),
      normalizedRuleName,
    ].join("|");
    if (Object.prototype.hasOwnProperty.call(ruleDetailCache, cacheKey)) {
      setRuleDetailData(ruleDetailCache[cacheKey] ?? null);
      setRuleDetailError("");
      return;
    }

    setRuleDetailLoading(true);
    setRuleDetailError("");
    try {
      const nextDetail = await getStrategyPerformanceRuleDetail({
        sourcePath: sourcePathTrimmed,
        selectedHorizon: detailHorizon,
        strongQuantile: parseQuantile(strongQuantileInput),
        selectedRuleName: normalizedRuleName,
      });
      setRuleDetailData(nextDetail);
      setRuleDetailCache((current) => ({
        ...current,
        [cacheKey]: nextDetail,
      }));
    } catch (reason) {
      setRuleDetailData(null);
      setRuleDetailError(
        reason instanceof Error ? reason.message : String(reason),
      );
    } finally {
      setRuleDetailLoading(false);
    }
  };

  const pickRule = (ruleName: string) => {
    setSelectedRuleNameInput(ruleName);
    setRuleDetailOpen(true);
    void loadRuleDetail(ruleName);
  };

  return (
    <div className="strategy-performance-page">
      <section className="strategy-performance-card">
        <div className="strategy-performance-section-head">
          <div>
            <h2>策略表现回测 / 优势策略分析</h2>
            <p>2 / 3 / 5 日统计。</p>
          </div>
          <div className="strategy-performance-inline-badges">
            <StatusBadge tone="neutral">
              数据源: {sourcePathTrimmed || "--"}
            </StatusBadge>
            {selectedSummary && !hasPendingChanges ? (
              <StatusBadge tone="good">
                {selectedSummary.horizon} 日强势阈值{" "}
                {formatPercent(selectedSummary.strong_threshold_pct)}
              </StatusBadge>
            ) : pageData && hasPendingChanges ? (
              <StatusBadge tone="warn">
                {pendingSelectedHorizon} 日强势阈值待刷新
              </StatusBadge>
            ) : null}
          </div>
        </div>

        <div className="strategy-performance-form-grid">
          <label className="strategy-performance-field">
            <span>分析持有周期</span>
            <select
              value={selectedHorizonInput}
              onChange={(event) => setSelectedHorizonInput(event.target.value)}
            >
              {HORIZON_OPTIONS.map((item) => (
                <option key={item} value={item}>
                  {item} 日
                </option>
              ))}
            </select>
          </label>

          <label className="strategy-performance-field">
            <span>强势分位阈值</span>
            <select
              value={strongQuantileInput}
              onChange={(event) => setStrongQuantileInput(event.target.value)}
            >
              {QUANTILE_OPTIONS.map((item) => (
                <option key={item} value={item}>
                  {item.toFixed(2)}
                </option>
              ))}
            </select>
          </label>

          <label className="strategy-performance-field">
            <span>2 日最小样本</span>
            <input
              value={autoMinSamples2}
              onChange={(event) => setAutoMinSamples2(event.target.value)}
            />
          </label>

          <label className="strategy-performance-field">
            <span>3 日最小样本</span>
            <input
              value={autoMinSamples3}
              onChange={(event) => setAutoMinSamples3(event.target.value)}
            />
          </label>

          <label className="strategy-performance-field">
            <span>5 日最小样本</span>
            <input
              value={autoMinSamples5}
              onChange={(event) => setAutoMinSamples5(event.target.value)}
            />
          </label>

          <label className="strategy-performance-field">
            <span>负向判定最少周期</span>
            <select
              value={minPassHorizonsInput}
              onChange={(event) => setMinPassHorizonsInput(event.target.value)}
            >
              <option value="1">1</option>
              <option value="2">2</option>
              <option value="3">3</option>
            </select>
          </label>

          <label className="strategy-performance-field">
            <span>优势命中门槛</span>
            <select
              value={minAdvHitsInput}
              onChange={(event) => setMinAdvHitsInput(event.target.value)}
            >
              <option value="1">1</option>
              <option value="2">2</option>
            </select>
          </label>

          <label className="strategy-performance-field">
            <span>TopN</span>
            <input
              value={topLimitInput}
              onChange={(event) => setTopLimitInput(event.target.value)}
            />
          </label>

          <div className="strategy-performance-field strategy-performance-checkbox">
            <span>自动候选额外条件</span>
            <label className="strategy-performance-checkbox-box">
              <input
                checked={requireWinRateAboveMarket}
                onChange={(event) =>
                  setRequireWinRateAboveMarket(event.target.checked)
                }
                type="checkbox"
              />
              <span>要求胜率高于市场</span>
            </label>
          </div>
        </div>

        {!pageData ? (
          <ApplyActionCard
            loading={loading}
            hasPendingChanges={hasPendingChanges}
            onApply={applyFilters}
          />
        ) : null}

        {pageData ? (
          <div className="strategy-performance-status-strip">
            <StatusBadge tone="good">
              自动优势集 {autoAdvantageRuleNames.length}
            </StatusBadge>
            <StatusBadge tone="good">
              手动草稿 {draftManualAdvantageRuleNames.length}
            </StatusBadge>
            <StatusBadge tone={isManualDraftSynced ? "good" : "neutral"}>
              已同步缓存{" "}
              {syncedManualSelectionForSource?.manualAdvantageRuleNames.length ??
                0}
            </StatusBadge>
            <StatusBadge tone="neutral">
              候选集 {currentCandidateRuleNames.length}
            </StatusBadge>
            <StatusBadge tone="warn">
              明确负向 {(pageData.effective_negative_rule_names ?? []).length}
            </StatusBadge>
            <StatusBadge tone="neutral">
              待验证负向{" "}
              {(pageData.ineffective_negative_rule_names ?? []).length}
            </StatusBadge>
          </div>
        ) : null}

        {pageData ? (
          <div className="strategy-performance-pool-grid strategy-performance-pool-grid-edit">
            <div className="strategy-performance-pool-card strategy-performance-pool-card-editor">
              <div className="strategy-performance-pool-card-head">
                <strong>自动优势集 / 手动优势集 / 候选集</strong>
              </div>
              <div className="strategy-performance-pool-toolbar">
                <input
                  type="text"
                  value={strategyKeyword}
                  onChange={(event) => setStrategyKeyword(event.target.value)}
                  placeholder="搜索策略"
                  className="strategy-performance-pool-search"
                />
                <button
                  type="button"
                  className="strategy-performance-secondary-btn"
                  onClick={() => applyManualRuleNames(autoAdvantageRuleNames)}
                >
                  自动填入手动集
                </button>
                <button
                  type="button"
                  className="strategy-performance-secondary-btn"
                  onClick={() => applyManualRuleNames([])}
                >
                  清空手动集
                </button>
                <button
                  type="button"
                  className="strategy-performance-secondary-btn"
                  onClick={() => void validateManualAdvantageSet()}
                  disabled={
                    manualSyncLoading ||
                    manualValidationLoading ||
                    !syncedManualSelectionForSource ||
                    !isManualDraftSynced
                  }
                >
                  {manualValidationLoading ? "验证中..." : "验证手动优势集"}
                </button>
              </div>
              <div className="strategy-performance-note-box">
                {manualSyncLoading
                  ? "当前正在自动同步手动优势集缓存，验证会读取同步完成后的缓存。"
                  : isManualDraftSynced
                    ? "当前手动优势集已自动同步到缓存，验证只读取这份缓存。"
                    : "当前手动优势集和缓存还不一致。点击策略调整手动集后会自动同步。"}
              </div>
              {manualSyncError ? (
                <div className="strategy-performance-error">
                  {manualSyncError}
                </div>
              ) : null}
              <div className="strategy-performance-pool-triple-grid">
                <div className="strategy-performance-pool-subcard">
                  <strong>自动优势集</strong>
                  <div className="strategy-performance-pool-chip-wrap">
                    {filteredAutoAdvantageRuleNames.length > 0 ? (
                      filteredAutoAdvantageRuleNames.map((ruleName) => (
                        <button
                          className="strategy-performance-pool-chip is-auto"
                          key={`auto:${ruleName}`}
                          onClick={() => addRuleToManualAdvantage(ruleName)}
                          type="button"
                        >
                          {ruleName}
                        </button>
                      ))
                    ) : (
                      <span className="strategy-performance-muted">
                        当前自动优势集为空。
                      </span>
                    )}
                  </div>
                </div>
                <div className="strategy-performance-pool-subcard">
                  <strong>手动优势集</strong>
                  <div className="strategy-performance-pool-chip-wrap">
                    {filteredManualAdvantageRuleNames.length > 0 ? (
                      filteredManualAdvantageRuleNames.map((ruleName) => (
                        <button
                          className={
                            autoAdvantageRuleNameSet.has(ruleName)
                              ? "strategy-performance-pool-chip is-manual is-manual-auto"
                              : "strategy-performance-pool-chip is-manual"
                          }
                          key={`manual:${ruleName}`}
                          onClick={() =>
                            removeRuleFromManualAdvantage(ruleName)
                          }
                          type="button"
                        >
                          {ruleName}
                        </button>
                      ))
                    ) : (
                      <span className="strategy-performance-muted">
                        当前手动优势集为空。
                      </span>
                    )}
                  </div>
                </div>
                <div className="strategy-performance-pool-subcard">
                  <strong>候选集</strong>
                  <div className="strategy-performance-pool-chip-wrap">
                    {filteredCurrentCandidateRuleNames.length > 0 ? (
                      filteredCurrentCandidateRuleNames.map((ruleName) => (
                        <button
                          className={
                            autoAdvantageRuleNameSet.has(ruleName)
                              ? "strategy-performance-pool-chip is-candidate is-candidate-auto"
                              : "strategy-performance-pool-chip is-candidate"
                          }
                          key={`candidate:${ruleName}`}
                          onClick={() => addRuleToManualAdvantage(ruleName)}
                          type="button"
                        >
                          {ruleName}
                        </button>
                      ))
                    ) : (
                      <span className="strategy-performance-muted">
                        当前候选策略集为空。
                      </span>
                    )}
                  </div>
                </div>
              </div>
            </div>
          </div>
        ) : null}

        {pageData?.ignored_manual_rule_names.length ? (
          <div className="strategy-performance-note-box">
            已忽略不存在的手工规则：
            {pageData.ignored_manual_rule_names.join("、")}
          </div>
        ) : null}
        {pageData ? (
          <ApplyActionCard
            loading={loading}
            hasPendingChanges={hasPendingChanges}
            onApply={applyFilters}
          />
        ) : null}
        {error ? (
          <div className="strategy-performance-error">{error}</div>
        ) : null}
      </section>

      <SummarySection
        summaries={pageData?.future_summaries ?? []}
        selectedHorizon={loadedSelectedHorizon}
        pendingHorizon={pendingSelectedHorizon}
        loading={loading}
        onSelectHorizon={switchHorizon}
      />

      <OverallScoreAnalysisSection
        detail={pageData?.overall_score_analysis ?? null}
        selectedHorizon={loadedSelectedHorizon}
        pendingHorizon={pendingSelectedHorizon}
        loading={loading}
      />

      {(manualValidationSelectionForSource ||
        manualValidationLoading ||
        manualValidationError) && (
        <>
          <OverallScoreAnalysisSection
            title="2. 手动优势集验证"
            description="按已同步手动优势集缓存中的命中总分 `adv_score_sum` 分层，观察这组手动策略组合是否具备稳定优势。"
            emptyText="当前还没有读取到手动优势集缓存验证结果。"
            detail={
              manualValidationSelectionForSource
                ? manualValidationData?.advantage_score_analysis ?? null
                : null
            }
            selectedHorizon={
              manualValidationData?.selected_horizon ??
              manualValidationSelectionForSource?.selectedHorizon ??
              loadedSelectedHorizon
            }
            pendingHorizon={pendingSelectedHorizon}
            loading={manualValidationLoading}
          />
          {manualValidationError ? (
            <section className="strategy-performance-card">
              <div className="strategy-performance-error">
                {manualValidationError}
              </div>
            </section>
          ) : null}
        </>
      )}

      <RuleTable
        title="3. 自动优势集"
        subtitle=""
        rows={advantageRuleRows}
        selectedHorizon={loadedSelectedHorizon}
        selectedRuleName={selectedRuleNameInput}
        onPickRule={pickRule}
      />

      <RuleTable
        title="4. 伴随策略集"
        subtitle=""
        rows={companionRuleRows}
        selectedHorizon={loadedSelectedHorizon}
        selectedRuleName={selectedRuleNameInput}
        onPickRule={pickRule}
      />

      <div className="strategy-performance-grid-2">
        <RuleTable
          title="5. 方向明确负向"
          subtitle=""
          rows={effectiveNegativeRuleRows}
          selectedHorizon={loadedSelectedHorizon}
          selectedRuleName={selectedRuleNameInput}
          onPickRule={pickRule}
        />
        <RuleTable
          title="待验证负向"
          subtitle=""
          rows={ineffectiveNegativeRuleRows}
          selectedHorizon={loadedSelectedHorizon}
          selectedRuleName={selectedRuleNameInput}
          onPickRule={pickRule}
        />
      </div>

      <div className="strategy-performance-grid-2">
        <CompanionTable
          title="6. 伴随策略分析: 增强项"
          subtitle=""
          rows={enhancingCompanionRows}
          defaultSortDirection="desc"
        />
        <CompanionTable
          title="伴随策略分析: 噪音项"
          subtitle=""
          rows={noisyCompanionRows}
          defaultSortDirection="asc"
        />
      </div>

      {ruleDetailOpen ? (
        <RuleDetailModal
          detail={ruleDetailData}
          loading={ruleDetailLoading}
          error={ruleDetailError}
          onClose={() => {
            setRuleDetailOpen(false);
            setRuleDetailError("");
          }}
        />
      ) : null}
    </div>
  );
}
