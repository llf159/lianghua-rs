import { useEffect, useMemo, useState, type ReactNode } from "react";
import { createPortal } from "react-dom";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import {
  getStrategyPerformancePage,
  getStrategyPerformanceRuleDetail,
  type StrategyPerformanceCompanionRow,
  type StrategyPerformanceFutureSummary,
  type StrategyPerformancePageData,
  type StrategyPerformanceRuleDetail,
  type StrategyPerformancePortfolioRow,
  type StrategyPerformanceRuleDirectionDetail,
  type StrategyPerformanceRuleRow,
} from "../../apis/strategyPerformance";
import {
  readJsonStorage,
  readStoredSourcePath,
  writeJsonStorage,
} from "../../shared/storage";
import {
  TableSortButton,
  getAriaSort,
  type SortDefinition,
  useTableSort,
} from "../../shared/tableSort";
import "./css/StrategyPerformanceBacktestPage.css";

const STRATEGY_PERFORMANCE_STATE_KEY = "lh_strategy_performance_page_v8";
const HORIZON_OPTIONS = [2, 3, 5] as const;
const QUANTILE_OPTIONS = [0.8, 0.9, 0.95] as const;
const DEFAULT_AUTO_MIN_SAMPLES = {
  2: 5,
  3: 5,
  5: 10,
  10: 20,
} as const;
const DEFAULT_MAX_COMBINATION_SIZE = 3;
const MIXED_SORT_KEY_OPTIONS = [
  { value: "adv_hit_cnt", label: "优势命中数" },
  { value: "adv_score_sum", label: "优势得分和" },
  { value: "pos_hit_cnt", label: "正向命中数" },
  { value: "pos_score_sum", label: "正向得分和" },
  { value: "rank", label: "原始排名" },
] as const;

type SubmittedQuery = {
  sourcePath: string;
  selectedHorizon: number;
  strongQuantile: number;
  manualRuleNames: string[];
  autoMinSamples2: number;
  autoMinSamples3: number;
  autoMinSamples5: number;
  autoMinSamples10: number;
  requireWinRateAboveMarket: boolean;
  minPassHorizons: number;
  minAdvHits: number;
  topLimit: number;
  maxCombinationSize: number;
  mixedSortKeys: string[];
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
  maxCombinationSize: string;
  mixedSortKeys: string[];
  selectedRuleName: string;
  pageData: StrategyPerformancePageData | null;
  submittedQuery: SubmittedQuery | null;
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

function sameStringArray(left: string[], right: string[]) {
  if (left.length !== right.length) {
    return false;
  }
  return left.every((item, index) => item === right[index]);
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
    sameStringArray(left.manualRuleNames, right.manualRuleNames) &&
    left.autoMinSamples2 === right.autoMinSamples2 &&
    left.autoMinSamples3 === right.autoMinSamples3 &&
    left.autoMinSamples5 === right.autoMinSamples5 &&
    left.autoMinSamples10 === right.autoMinSamples10 &&
    left.requireWinRateAboveMarket === right.requireWinRateAboveMarket &&
    left.minPassHorizons === right.minPassHorizons &&
    left.minAdvHits === right.minAdvHits &&
    left.topLimit === right.topLimit &&
    left.maxCombinationSize === right.maxCombinationSize &&
    sameStringArray(left.mixedSortKeys, right.mixedSortKeys)
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
  selectedHorizon: number,
) {
  const leftMetric = metricForHorizon(left, selectedHorizon);
  const rightMetric = metricForHorizon(right, selectedHorizon);

  if (
    left.signal_direction === "negative" &&
    right.signal_direction === "negative"
  ) {
    return (
      Number(right.negative_effective === true) -
        Number(left.negative_effective === true) ||
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
    compareDescNumber(left.overall_composite_score, right.overall_composite_score) ||
    compareDescNumber(leftMetric?.composite_score, rightMetric?.composite_score) ||
    compareDescNumber(leftMetric?.rank_ic_mean, rightMetric?.rank_ic_mean) ||
    compareDescNumber(leftMetric?.icir, rightMetric?.icir) ||
    compareDescNumber(
      leftMetric?.avg_future_return_pct,
      rightMetric?.avg_future_return_pct,
    ) ||
    (rightMetric?.hit_n ?? 0) - (leftMetric?.hit_n ?? 0) ||
    left.rule_name.localeCompare(right.rule_name)
  );
}

function sortRuleRows(
  rows: StrategyPerformanceRuleRow[],
  selectedHorizon: number,
) {
  return [...rows].sort((left, right) =>
    compareRuleRows(left, right, selectedHorizon),
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
type PortfolioTableSortKey =
  | "strategy_label"
  | "full"
  | "recent_40"
  | "recent_20";

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

function findPortfolioWindow(
  row: StrategyPerformancePortfolioRow,
  windowKey: "full" | "recent_40" | "recent_20",
) {
  return row.windows.find((item) => item.window_key === windowKey) ?? null;
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
  return (
    <div className="strategy-performance-metric-cell">
      <div>
        <span>样本</span>
        <strong>{formatNumber(metric.hit_n, 0)}</strong>
      </div>
      <div>
        <span>IC</span>
        <strong className={valueClassName(metric.rank_ic_mean)}>
          {formatNumber(metric.rank_ic_mean, 3)}
        </strong>
      </div>
      <div>
        <span>ICIR</span>
        <strong className={valueClassName(metric.icir)}>
          {formatNumber(metric.icir, 2)}
        </strong>
      </div>
      <div>
        <span>分层差</span>
        <strong className={valueClassName(metric.layer_return_spread_pct)}>
          {formatPercent(metric.layer_return_spread_pct)}
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
}: {
  summaries: StrategyPerformanceFutureSummary[];
  selectedHorizon: number;
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
          <article
            className={
              summary.horizon === selectedHorizon
                ? "strategy-performance-summary-card is-active"
                : "strategy-performance-summary-card"
            }
            key={summary.horizon}
          >
            <div className="strategy-performance-summary-head">
              <strong>{summary.horizon} 日</strong>
              {summary.horizon === selectedHorizon ? (
                <StatusBadge tone="good">当前视角</StatusBadge>
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
          </article>
        ))}
      </div>
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
                  aria-sort={getAriaSort(sortKey === "rule_name", sortDirection)}
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
                <th>得分影响</th>
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
                      <td key={`${row.rule_name}:${row.signal_direction}:${horizon}`}>
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
                            tone={
                              row.negative_effective ? "warn" : "neutral"
                            }
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
                  aria-sort={getAriaSort(sortKey === "rule_name", sortDirection)}
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

function PortfolioTable({ rows }: { rows: StrategyPerformancePortfolioRow[] }) {
  const sortDefinitions = useMemo(
    () =>
      ({
        strategy_label: {
          value: (row) => row.strategy_label,
        },
        full: {
          compare: (left, right) => {
            const leftWindow = findPortfolioWindow(left, "full");
            const rightWindow = findPortfolioWindow(right, "full");
            return (
              compareDescNumber(
                leftWindow?.avg_excess_return_pct,
                rightWindow?.avg_excess_return_pct,
              ) ||
              compareDescNumber(
                leftWindow?.avg_portfolio_return_pct,
                rightWindow?.avg_portfolio_return_pct,
              ) ||
              compareDescNumber(leftWindow?.excess_win_rate, rightWindow?.excess_win_rate) ||
              left.strategy_label.localeCompare(right.strategy_label)
            );
          },
        },
        recent_40: {
          compare: (left, right) => {
            const leftWindow = findPortfolioWindow(left, "recent_40");
            const rightWindow = findPortfolioWindow(right, "recent_40");
            return (
              compareDescNumber(
                leftWindow?.avg_excess_return_pct,
                rightWindow?.avg_excess_return_pct,
              ) ||
              compareDescNumber(
                leftWindow?.avg_portfolio_return_pct,
                rightWindow?.avg_portfolio_return_pct,
              ) ||
              compareDescNumber(leftWindow?.excess_win_rate, rightWindow?.excess_win_rate) ||
              left.strategy_label.localeCompare(right.strategy_label)
            );
          },
        },
        recent_20: {
          compare: (left, right) => {
            const leftWindow = findPortfolioWindow(left, "recent_20");
            const rightWindow = findPortfolioWindow(right, "recent_20");
            return (
              compareDescNumber(
                leftWindow?.avg_excess_return_pct,
                rightWindow?.avg_excess_return_pct,
              ) ||
              compareDescNumber(
                leftWindow?.avg_portfolio_return_pct,
                rightWindow?.avg_portfolio_return_pct,
              ) ||
              compareDescNumber(leftWindow?.excess_win_rate, rightWindow?.excess_win_rate) ||
              left.strategy_label.localeCompare(right.strategy_label)
            );
          },
        },
      }) satisfies Partial<
        Record<
          PortfolioTableSortKey,
          SortDefinition<StrategyPerformancePortfolioRow>
        >
      >,
    [],
  );
  const { sortKey, sortDirection, sortedRows, toggleSort } = useTableSort<
    StrategyPerformancePortfolioRow,
    PortfolioTableSortKey
  >(rows, sortDefinitions, {
    key: "full",
    direction: "desc",
  });

  return (
    <section className="strategy-performance-card">
      <div className="strategy-performance-section-head">
        <div>
          <h3>6. 单因子 / 多因子组合回测</h3>
          <p>TopN 组合结果。</p>
        </div>
      </div>
      <div className="strategy-performance-table-wrap">
        <table className="strategy-performance-table">
          <thead>
            <tr>
              <th
                aria-sort={getAriaSort(
                  sortKey === "strategy_label",
                  sortDirection,
                )}
              >
                <TableSortButton
                  label="组合"
                  isActive={sortKey === "strategy_label"}
                  direction={sortDirection}
                  onClick={() => toggleSort("strategy_label")}
                  title="按组合名称排序"
                />
              </th>
              <th>排序逻辑</th>
              <th aria-sort={getAriaSort(sortKey === "full", sortDirection)}>
                <TableSortButton
                  label="全样本"
                  isActive={sortKey === "full"}
                  direction={sortDirection}
                  onClick={() => toggleSort("full")}
                  title="按全样本均超额排序"
                />
              </th>
              <th
                aria-sort={getAriaSort(sortKey === "recent_40", sortDirection)}
              >
                <TableSortButton
                  label="近 40 期"
                  isActive={sortKey === "recent_40"}
                  direction={sortDirection}
                  onClick={() => toggleSort("recent_40")}
                  title="按近 40 期均超额排序"
                />
              </th>
              <th
                aria-sort={getAriaSort(sortKey === "recent_20", sortDirection)}
              >
                <TableSortButton
                  label="近 20 期"
                  isActive={sortKey === "recent_20"}
                  direction={sortDirection}
                  onClick={() => toggleSort("recent_20")}
                  title="按近 20 期均超额排序"
                />
              </th>
            </tr>
          </thead>
          <tbody>
            {sortedRows.map((row) => (
              <tr key={row.strategy_key}>
                <td>
                  <div className="strategy-performance-rule-name">
                    <strong>{row.strategy_label}</strong>
                  </div>
                  {row.factor_count ? (
                    <div className="strategy-performance-rule-meta">
                      {row.factor_count} 因子
                    </div>
                  ) : null}
                </td>
                <td className="strategy-performance-rule-meta">
                  {row.sort_description}
                </td>
                {["full", "recent_40", "recent_20"].map((windowKey) => {
                  const summary = row.windows.find(
                    (item) => item.window_key === windowKey,
                  );
                  return (
                    <td key={windowKey}>
                      <div className="strategy-performance-window-cell">
                        <div>
                          <span>样本期数</span>
                          <strong>
                            {formatNumber(summary?.sample_days, 0)}
                          </strong>
                        </div>
                        <div>
                          <span>组合均收益</span>
                          <strong
                            className={valueClassName(
                              summary?.avg_portfolio_return_pct,
                            )}
                          >
                            {formatPercent(summary?.avg_portfolio_return_pct)}
                          </strong>
                        </div>
                        <div>
                          <span>市场均收益</span>
                          <strong>
                            {formatPercent(summary?.avg_market_return_pct)}
                          </strong>
                        </div>
                        <div>
                          <span>均超额</span>
                          <strong
                            className={valueClassName(
                              summary?.avg_excess_return_pct,
                            )}
                          >
                            {formatPercent(summary?.avg_excess_return_pct)}
                          </strong>
                        </div>
                        <div>
                          <span>超额胜率</span>
                          <strong>
                            {formatRate(summary?.excess_win_rate)}
                          </strong>
                        </div>
                        <div>
                          <span>平均持仓数</span>
                          <strong>
                            {formatNumber(summary?.avg_selected_count)}
                          </strong>
                        </div>
                        <div>
                          <span>IC</span>
                          <strong className={valueClassName(summary?.rank_ic_mean)}>
                            {formatNumber(summary?.rank_ic_mean, 3)}
                          </strong>
                        </div>
                        <div>
                          <span>ICIR</span>
                          <strong className={valueClassName(summary?.icir)}>
                            {formatNumber(summary?.icir, 2)}
                          </strong>
                        </div>
                        <div>
                          <span>分层差</span>
                          <strong
                            className={valueClassName(
                              summary?.layer_return_spread_pct,
                            )}
                          >
                            {formatPercent(summary?.layer_return_spread_pct)}
                          </strong>
                        </div>
                        <div>
                          <span>Sharpe</span>
                          <strong>{formatNumber(summary?.sharpe_ratio, 2)}</strong>
                        </div>
                      </div>
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
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
            ? "参数已变更，重新应用后刷新。"
            : "显示最近一次结果。"}
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
          <span>ICIR</span>
          <strong className={valueClassName(detail.icir)}>
            {formatNumber(detail.icir, 2)}
          </strong>
        </div>
        <div>
          <span>Hit vs Non-hit</span>
          <strong className={valueClassName(detail.hit_vs_non_hit_delta_pct)}>
            {formatPercent(detail.hit_vs_non_hit_delta_pct)}
          </strong>
        </div>
        <div>
          <span>IC</span>
          <strong className={valueClassName(detail.rank_ic_mean)}>
            {formatNumber(detail.rank_ic_mean, 3)}
          </strong>
        </div>
        <div>
          <span>Sharpe</span>
          <strong className={valueClassName(detail.sharpe_ratio)}>
            {formatNumber(detail.sharpe_ratio, 2)}
          </strong>
        </div>
        <div>
          <span>高分层 - 低分层</span>
          <strong
            className={valueClassName(
              detail.extreme_score_minus_mild_score_pct,
            )}
          >
            {formatPercent(detail.extreme_score_minus_mild_score_pct)}
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
              <h3 id="strategy-performance-rule-detail-title">
                策略得分影响
              </h3>
            </div>
          </div>

          {loading ? (
            <div className="strategy-performance-empty">正在读取单策略明细...</div>
          ) : error ? (
            <div className="strategy-performance-error">{error}</div>
          ) : !detail ? (
            <div className="strategy-performance-empty">
              当前没有可展示的单策略得分影响数据。
            </div>
          ) : (
            <div className="strategy-performance-detail-shell">
              <div className="strategy-performance-inline-badges">
                <StatusBadge tone="neutral">规则 {detail.rule_name}</StatusBadge>
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
  const persistedState = useMemo(
    () =>
      typeof window === "undefined"
        ? null
        : readJsonStorage<Partial<PersistedState>>(
            window.localStorage,
            STRATEGY_PERFORMANCE_STATE_KEY,
          ),
    [],
  );
  const useMigratedAutoMinSampleDefaults =
    hasLegacyAutoMinSampleStrings(persistedState);

  const [sourcePath, setSourcePath] = useState(
    () => persistedState?.sourcePath ?? readStoredSourcePath(),
  );
  const [selectedHorizonInput, setSelectedHorizonInput] = useState(
    persistedState?.selectedHorizon ?? "5",
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
  const [autoMinSamples2, setAutoMinSamples2] = useState(
    () =>
      useMigratedAutoMinSampleDefaults
        ? String(DEFAULT_AUTO_MIN_SAMPLES[2])
        : (persistedState?.autoMinSamples2 ?? String(DEFAULT_AUTO_MIN_SAMPLES[2])),
  );
  const [autoMinSamples3, setAutoMinSamples3] = useState(
    () =>
      useMigratedAutoMinSampleDefaults
        ? String(DEFAULT_AUTO_MIN_SAMPLES[3])
        : (persistedState?.autoMinSamples3 ?? String(DEFAULT_AUTO_MIN_SAMPLES[3])),
  );
  const [autoMinSamples5, setAutoMinSamples5] = useState(
    () =>
      useMigratedAutoMinSampleDefaults
        ? String(DEFAULT_AUTO_MIN_SAMPLES[5])
        : (persistedState?.autoMinSamples5 ?? String(DEFAULT_AUTO_MIN_SAMPLES[5])),
  );
  const [autoMinSamples10] = useState(
    () =>
      useMigratedAutoMinSampleDefaults
        ? String(DEFAULT_AUTO_MIN_SAMPLES[10])
        : (persistedState?.autoMinSamples10 ?? String(DEFAULT_AUTO_MIN_SAMPLES[10])),
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
  const [maxCombinationSizeInput, setMaxCombinationSizeInput] = useState(
    persistedState?.maxCombinationSize ?? String(DEFAULT_MAX_COMBINATION_SIZE),
  );
  const [mixedSortKeys, setMixedSortKeys] = useState<string[]>(() =>
    arrayFromUnknown(persistedState?.mixedSortKeys).length > 0
      ? arrayFromUnknown(persistedState?.mixedSortKeys)
      : ["adv_hit_cnt", "adv_score_sum", "rank"],
  );
  const [selectedRuleNameInput, setSelectedRuleNameInput] = useState(
    persistedState?.selectedRuleName ?? "",
  );
  const [pageData, setPageData] = useState<StrategyPerformancePageData | null>(
    () =>
      persistedState?.pageData && typeof persistedState.pageData === "object"
        ? (persistedState.pageData as StrategyPerformancePageData)
        : null,
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
          typeof query.selectedHorizon === "number"
            ? query.selectedHorizon
            : 5,
        strongQuantile:
          typeof query.strongQuantile === "number" ? query.strongQuantile : 0.9,
        manualRuleNames: arrayFromUnknown(query.manualRuleNames),
        autoMinSamples2:
          useMigratedQueryAutoMinSamples
            ? DEFAULT_AUTO_MIN_SAMPLES[2]
            : typeof query.autoMinSamples2 === "number"
            ? query.autoMinSamples2
            : DEFAULT_AUTO_MIN_SAMPLES[2],
        autoMinSamples3:
          useMigratedQueryAutoMinSamples
            ? DEFAULT_AUTO_MIN_SAMPLES[3]
            : typeof query.autoMinSamples3 === "number"
            ? query.autoMinSamples3
            : DEFAULT_AUTO_MIN_SAMPLES[3],
        autoMinSamples5:
          useMigratedQueryAutoMinSamples
            ? DEFAULT_AUTO_MIN_SAMPLES[5]
            : typeof query.autoMinSamples5 === "number"
            ? query.autoMinSamples5
            : DEFAULT_AUTO_MIN_SAMPLES[5],
        autoMinSamples10:
          useMigratedQueryAutoMinSamples
            ? DEFAULT_AUTO_MIN_SAMPLES[10]
            : typeof query.autoMinSamples10 === "number"
            ? query.autoMinSamples10
            : DEFAULT_AUTO_MIN_SAMPLES[10],
        requireWinRateAboveMarket: query.requireWinRateAboveMarket === true,
        minPassHorizons:
          typeof query.minPassHorizons === "number" ? query.minPassHorizons : 2,
        minAdvHits: typeof query.minAdvHits === "number" ? query.minAdvHits : 1,
        topLimit: typeof query.topLimit === "number" ? query.topLimit : 100,
        maxCombinationSize:
          typeof query.maxCombinationSize === "number"
            ? query.maxCombinationSize
            : DEFAULT_MAX_COMBINATION_SIZE,
        mixedSortKeys: arrayFromUnknown(query.mixedSortKeys),
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

  const sourcePathTrimmed = sourcePath.trim();
  const selectedHorizonValue = Number(selectedHorizonInput);

  const positiveRuleNames = useMemo(
    () =>
      (pageData?.rule_rows ?? [])
        .filter(hasPositiveHits)
        .map((row) => row.rule_name),
    [pageData],
  );
  const autoCandidateRuleNames = useMemo(
    () => pageData?.auto_candidate_rule_names ?? [],
    [pageData],
  );
  const currentAdvantageRuleNames = useMemo(() => {
    if (manualRuleNames.length > 0) {
      return normalizeStringArray(
        manualRuleNames.filter((item) => positiveRuleNames.includes(item)),
      );
    }
    return normalizeStringArray(
      (pageData?.resolved_advantage_rule_names ?? []).filter((item) =>
        positiveRuleNames.includes(item),
      ),
    );
  }, [manualRuleNames, pageData, positiveRuleNames]);
  const currentCompanionRuleNames = useMemo(() => {
    const advantageSet = new Set(currentAdvantageRuleNames);
    return positiveRuleNames.filter((item) => !advantageSet.has(item));
  }, [currentAdvantageRuleNames, positiveRuleNames]);
  const filteredAutoCandidateRuleNames = useMemo(() => {
    const keyword = strategyKeyword.trim().toLowerCase();
    if (!keyword) {
      return autoCandidateRuleNames;
    }
    return autoCandidateRuleNames.filter((item) =>
      item.toLowerCase().includes(keyword),
    );
  }, [autoCandidateRuleNames, strategyKeyword]);
  const filteredCurrentAdvantageRuleNames = useMemo(() => {
    const keyword = strategyKeyword.trim().toLowerCase();
    if (!keyword) {
      return currentAdvantageRuleNames;
    }
    return currentAdvantageRuleNames.filter((item) =>
      item.toLowerCase().includes(keyword),
    );
  }, [currentAdvantageRuleNames, strategyKeyword]);
  const filteredCurrentCompanionRuleNames = useMemo(() => {
    const keyword = strategyKeyword.trim().toLowerCase();
    if (!keyword) {
      return currentCompanionRuleNames;
    }
    return currentCompanionRuleNames.filter((item) =>
      item.toLowerCase().includes(keyword),
    );
  }, [currentCompanionRuleNames, strategyKeyword]);

  const buildSubmittedQuery = (): SubmittedQuery | null => {
    if (!sourcePathTrimmed) {
      return null;
    }
    const selectedHorizon = parsePositiveInt(selectedHorizonInput, 5);
    const strongQuantile = parseQuantile(strongQuantileInput);
    const normalizedManualRuleNames = normalizeStringArray(currentAdvantageRuleNames);
    return {
      sourcePath: sourcePathTrimmed,
      selectedHorizon,
      strongQuantile,
      manualRuleNames: normalizedManualRuleNames,
      autoMinSamples2: parsePositiveInt(autoMinSamples2, DEFAULT_AUTO_MIN_SAMPLES[2]),
      autoMinSamples3: parsePositiveInt(autoMinSamples3, DEFAULT_AUTO_MIN_SAMPLES[3]),
      autoMinSamples5: parsePositiveInt(autoMinSamples5, DEFAULT_AUTO_MIN_SAMPLES[5]),
      autoMinSamples10: parsePositiveInt(autoMinSamples10, DEFAULT_AUTO_MIN_SAMPLES[10]),
      requireWinRateAboveMarket,
      minPassHorizons: parsePositiveInt(minPassHorizonsInput, 2),
      minAdvHits: parsePositiveInt(minAdvHitsInput, 1),
      topLimit: parsePositiveInt(topLimitInput, 100),
      maxCombinationSize: parsePositiveInt(
        maxCombinationSizeInput,
        DEFAULT_MAX_COMBINATION_SIZE,
      ),
      mixedSortKeys: normalizeStringArray(mixedSortKeys),
    };
  };

  const runPageQuery = async (nextQuery: SubmittedQuery) => {
    setLoading(true);
    setError("");
    try {
      const nextPageData = await getStrategyPerformancePage({
        ...nextQuery,
        advantageRuleMode:
          nextQuery.manualRuleNames.length > 0 ? "manual" : "auto",
      });
      setPageData(nextPageData);
      setSubmittedQuery(nextQuery);
      setSelectedRuleNameInput((current) => {
        if (
          current &&
          nextPageData.strategy_options.includes(current)
        ) {
          return current;
        }
        return nextPageData.selected_rule_name ?? current;
      });
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
    if (sameSubmittedQuery(nextQuery, submittedQuery) && pageData) {
      return;
    }
    void runPageQuery(nextQuery);
  };

  const hasPendingChanges = useMemo(() => {
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
      !sameStringArray(
        submittedQuery.manualRuleNames,
        currentQuery.manualRuleNames,
      ) ||
      submittedQuery.autoMinSamples2 !== currentQuery.autoMinSamples2 ||
      submittedQuery.autoMinSamples3 !== currentQuery.autoMinSamples3 ||
      submittedQuery.autoMinSamples5 !== currentQuery.autoMinSamples5 ||
      submittedQuery.autoMinSamples10 !== currentQuery.autoMinSamples10 ||
      submittedQuery.requireWinRateAboveMarket !==
        currentQuery.requireWinRateAboveMarket ||
      submittedQuery.minPassHorizons !== currentQuery.minPassHorizons ||
      submittedQuery.minAdvHits !== currentQuery.minAdvHits ||
      submittedQuery.topLimit !== currentQuery.topLimit ||
      submittedQuery.maxCombinationSize !== currentQuery.maxCombinationSize ||
      !sameStringArray(
        submittedQuery.mixedSortKeys,
        currentQuery.mixedSortKeys,
      )
    );
  }, [
    autoMinSamples2,
    autoMinSamples3,
    autoMinSamples10,
    autoMinSamples5,
    minAdvHitsInput,
    minPassHorizonsInput,
    maxCombinationSizeInput,
    mixedSortKeys,
    currentAdvantageRuleNames,
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
    writeJsonStorage(window.localStorage, STRATEGY_PERFORMANCE_STATE_KEY, {
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
      maxCombinationSize: maxCombinationSizeInput,
      mixedSortKeys,
      selectedRuleName: selectedRuleNameInput,
      pageData,
      submittedQuery,
    } satisfies PersistedState);
  }, [
    autoMinSamples2,
    autoMinSamples3,
    autoMinSamples10,
    autoMinSamples5,
    manualRuleNames,
    minAdvHitsInput,
    minPassHorizonsInput,
    mixedSortKeys,
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
    const rows = (pageData?.rule_rows ?? []).filter(
      (row) => row.signal_direction === "positive" && row.in_advantage_set,
    );
    return sortRuleRows(rows, selectedHorizonValue);
  }, [pageData, selectedHorizonValue]);
  const companionRuleRows = useMemo(() => {
    const rows = (pageData?.rule_rows ?? []).filter(
      (row) => row.signal_direction === "positive" && row.in_companion_set,
    );
    return sortRuleRows(rows, selectedHorizonValue);
  }, [pageData, selectedHorizonValue]);
  const effectiveNegativeRuleRows = useMemo(() => {
    const rows = (pageData?.rule_rows ?? []).filter(
      (row) => row.signal_direction === "negative" && row.negative_effective,
    );
    return sortRuleRows(rows, selectedHorizonValue);
  }, [pageData, selectedHorizonValue]);
  const ineffectiveNegativeRuleRows = useMemo(() => {
    const rows = (pageData?.rule_rows ?? []).filter(
      (row) =>
        row.signal_direction === "negative" && row.negative_effective === false,
    );
    return sortRuleRows(rows, selectedHorizonValue);
  }, [pageData, selectedHorizonValue]);
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
  const selectedSummary = useMemo(
    () => findSummary(pageData?.future_summaries ?? [], selectedHorizonValue),
    [pageData, selectedHorizonValue],
  );
  const pendingSelectedHorizon = parsePositiveInt(selectedHorizonInput, 5);

  const moveRuleToAdvantage = (ruleName: string) => {
    setManualRuleNames((current) =>
      current.includes(ruleName) ? current : [...current, ruleName],
    );
  };

  const moveRuleToCompanion = (ruleName: string) => {
    setManualRuleNames((current) => current.filter((item) => item !== ruleName));
  };

  const toggleMixedSortKey = (key: string) => {
    setMixedSortKeys((current) => {
      const hasKey = current.includes(key);
      if (hasKey) {
        const next = current.filter((item) => item !== key);
        return next.length > 0 ? next : current;
      }
      return [...current, key];
    });
  };

  const loadRuleDetail = async (ruleName: string) => {
    const normalizedRuleName = ruleName.trim();
    if (!sourcePathTrimmed || !normalizedRuleName) {
      setRuleDetailError("缺少可用的数据源路径或策略名");
      setRuleDetailData(null);
      return;
    }
    const cacheKey = [
      sourcePathTrimmed,
      parsePositiveInt(selectedHorizonInput, 5),
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
        selectedHorizon: parsePositiveInt(selectedHorizonInput, 5),
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
            <span>组合因子上限</span>
            <input
              value={maxCombinationSizeInput}
              onChange={(event) => setMaxCombinationSizeInput(event.target.value)}
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

        <div className="strategy-performance-control-stack">
          <div className="strategy-performance-mixed-sort">
            <span>混合排序 TopN 的排序键顺序</span>
            <div className="strategy-performance-chip-wrap">
              {MIXED_SORT_KEY_OPTIONS.map((option) => (
                <button
                  className={
                    mixedSortKeys.includes(option.value)
                      ? "strategy-performance-chip is-active"
                      : "strategy-performance-chip"
                  }
                  key={option.value}
                  onClick={() => toggleMixedSortKey(option.value)}
                  type="button"
                  aria-pressed={mixedSortKeys.includes(option.value)}
                >
                  <span>{option.label}</span>
                </button>
              ))}
            </div>
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
              自动前十优势 {autoCandidateRuleNames.length}
            </StatusBadge>
            <StatusBadge tone="good">
              当前优势集 {currentAdvantageRuleNames.length}
            </StatusBadge>
            <StatusBadge tone="neutral">
              当前伴随集 {currentCompanionRuleNames.length}
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
            <div className="strategy-performance-pool-card">
              <strong>自动前十优势策略</strong>
              <div className="strategy-performance-pool-chip-wrap">
                {filteredAutoCandidateRuleNames.length > 0 ? (
                  filteredAutoCandidateRuleNames.map((ruleName) => (
                    <button
                      className="strategy-performance-pool-chip is-auto"
                      key={`auto:${ruleName}`}
                      onClick={() => pickRule(ruleName)}
                      type="button"
                    >
                      {ruleName}
                    </button>
                  ))
                ) : (
                  <span className="strategy-performance-muted">
                    当前没有满足条件的自动优势策略。
                  </span>
                )}
              </div>
            </div>
            <div className="strategy-performance-pool-card strategy-performance-pool-card-editor">
              <div className="strategy-performance-pool-card-head">
                <strong>当前优势 / 伴随集</strong>
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
                  onClick={() => setManualRuleNames(autoCandidateRuleNames)}
                >
                  恢复自动前十
                </button>
                <button
                  type="button"
                  className="strategy-performance-secondary-btn"
                  onClick={() => setManualRuleNames([])}
                >
                  清空手工调整
                </button>
              </div>
              <div className="strategy-performance-pool-dual-grid">
                <div className="strategy-performance-pool-subcard">
                  <strong>当前优势集</strong>
                  <div className="strategy-performance-pool-chip-wrap">
                    {filteredCurrentAdvantageRuleNames.length > 0 ? (
                      filteredCurrentAdvantageRuleNames.map((ruleName) => (
                        <button
                          className="strategy-performance-pool-chip is-advantage"
                          key={`resolved:${ruleName}`}
                          onClick={() => moveRuleToCompanion(ruleName)}
                          type="button"
                        >
                          {ruleName}
                        </button>
                      ))
                    ) : (
                      <span className="strategy-performance-muted">
                        当前优势策略集为空。
                      </span>
                    )}
                  </div>
                </div>
                <div className="strategy-performance-pool-subcard">
                  <strong>当前伴随集</strong>
                  <div className="strategy-performance-pool-chip-wrap">
                    {filteredCurrentCompanionRuleNames.length > 0 ? (
                      filteredCurrentCompanionRuleNames.map((ruleName) => (
                        <button
                          className="strategy-performance-pool-chip is-companion"
                          key={`companion:${ruleName}`}
                          onClick={() => moveRuleToAdvantage(ruleName)}
                          type="button"
                        >
                          {ruleName}
                        </button>
                      ))
                    ) : (
                      <span className="strategy-performance-muted">
                        当前伴随策略集为空。
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
        selectedHorizon={selectedHorizonValue}
      />

      <RuleTable
        title="2. 优势策略集"
        subtitle=""
        rows={advantageRuleRows}
        selectedHorizon={selectedHorizonValue}
        selectedRuleName={selectedRuleNameInput}
        onPickRule={pickRule}
      />

      <RuleTable
        title="3. 伴随策略集"
        subtitle=""
        rows={companionRuleRows}
        selectedHorizon={selectedHorizonValue}
        selectedRuleName={selectedRuleNameInput}
        onPickRule={pickRule}
      />

      <div className="strategy-performance-grid-2">
        <RuleTable
          title="4. 方向明确负向"
          subtitle=""
          rows={effectiveNegativeRuleRows}
          selectedHorizon={selectedHorizonValue}
          selectedRuleName={selectedRuleNameInput}
          onPickRule={pickRule}
        />
        <RuleTable
          title="待验证负向"
          subtitle=""
          rows={ineffectiveNegativeRuleRows}
          selectedHorizon={selectedHorizonValue}
          selectedRuleName={selectedRuleNameInput}
          onPickRule={pickRule}
        />
      </div>

      <div className="strategy-performance-grid-2">
        <CompanionTable
          title="5. 伴随策略分析: 增强项"
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

      <PortfolioTable rows={pageData?.portfolio_rows ?? []} />

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
