import { useEffect, useMemo, useState } from "react";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import {
  getStrategyManagePage,
  type StrategyManageRuleItem,
} from "../../apis/strategyManage";
import {
  getStrategyPerformanceValidationPage,
  type StrategyDirection,
  type StrategyPerformanceValidationCaseData,
  type StrategyPerformanceValidationDraft,
  type StrategyPerformanceValidationPageData,
  type StrategyValidationUnknownConfig,
} from "../../apis/strategyValidation";
import {
  readJsonStorage,
  readStoredSourcePath,
  writeJsonStorage,
} from "../../shared/storage";
import "./css/StrategyManagePage.css";
import "./css/StrategyValidationBacktestPage.css";

const STRATEGY_VALIDATION_STATE_KEY = "lh_strategy_validation_backtest_v6";
const QUANTILE_OPTIONS = [0.8, 0.9, 0.95] as const;
const HORIZON_OPTIONS = [2, 3, 5] as const;
const DIRECTION_OPTIONS = ["positive", "negative"] as const;
const SCOPE_OPTIONS = ["LAST", "ANY", "EACH", "RECENT", "CONSEC"] as const;

type ScopeMode = (typeof SCOPE_OPTIONS)[number];

type PersistedState = {
  sourcePath: string;
  selectedImportName: string;
  selectedHorizon: string;
  strongQuantile: string;
  enableUnknownConfigs: boolean;
  draft: StrategyPerformanceValidationDraft;
  pageData: StrategyPerformanceValidationPageData | null;
};

function buildEmptyUnknown(): StrategyValidationUnknownConfig {
  return {
    name: "",
    start: 0,
    end: 0,
    step: 1,
  };
}

function buildEmptyDraft(): StrategyPerformanceValidationDraft {
  return {
    strategy_direction: "positive",
    scope_way: "LAST",
    scope_windows: 1,
    when: "",
    import_name: null,
    unknown_configs: [],
  };
}

function parseScopeWayDraft(scopeWay: string): {
  mode: ScopeMode;
  consecThreshold: number;
} {
  const normalized = scopeWay.trim().toUpperCase();
  if (normalized.startsWith("CONSEC>=")) {
    const raw = Number(normalized.slice("CONSEC>=".length));
    return {
      mode: "CONSEC",
      consecThreshold: Number.isInteger(raw) && raw > 0 ? raw : 2,
    };
  }
  if (SCOPE_OPTIONS.includes(normalized as ScopeMode)) {
    return { mode: normalized as ScopeMode, consecThreshold: 2 };
  }
  return { mode: "LAST", consecThreshold: 2 };
}

function buildScopeWayValue(mode: ScopeMode, consecThreshold: number) {
  if (mode === "CONSEC") {
    return `CONSEC>=${Math.max(1, Math.floor(consecThreshold || 1))}`;
  }
  return mode;
}

function buildDraftFromRule(
  rule: StrategyManageRuleItem,
): StrategyPerformanceValidationDraft {
  return {
    strategy_direction: "positive",
    scope_way: rule.scope_way,
    scope_windows: rule.scope_windows,
    when: rule.when,
    import_name: rule.name,
    unknown_configs: [],
  };
}

function strategyDirectionLabel(direction: StrategyDirection) {
  return direction === "negative" ? "负向" : "正向";
}

function normalizeNumberInput(
  raw: string,
  fallback: number,
  options?: {
    integer?: boolean;
    min?: number;
  },
) {
  const trimmed = raw.trim();
  if (!trimmed) {
    return fallback;
  }
  const parsed = Number(trimmed);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  const normalized = options?.integer ? Math.floor(parsed) : parsed;
  if (options?.min !== undefined) {
    return Math.max(options.min, normalized);
  }
  return normalized;
}

function DraftNumberInput({
  value,
  onCommit,
  min,
  step,
  integer = false,
  disabled = false,
}: {
  value: number;
  onCommit: (value: number) => void;
  min?: number;
  step?: number | string;
  integer?: boolean;
  disabled?: boolean;
}) {
  const [text, setText] = useState(
    Number.isFinite(value) ? String(value) : "",
  );

  useEffect(() => {
    setText(Number.isFinite(value) ? String(value) : "");
  }, [value]);

  return (
    <input
      type="number"
      min={min}
      step={step}
      value={text}
      disabled={disabled}
      onChange={(event) => {
        const raw = event.target.value;
        setText(raw);
        if (!raw.trim()) {
          return;
        }
        const nextValue = normalizeNumberInput(raw, value, { integer, min });
        onCommit(nextValue);
      }}
      onBlur={() => {
        const fallback = Number.isFinite(value) ? value : min ?? 0;
        const normalized = normalizeNumberInput(text, fallback, {
          integer,
          min,
        });
        setText(String(normalized));
        if (normalized !== value) {
          onCommit(normalized);
        }
      }}
    />
  );
}

function buildInitialState(): PersistedState {
  const fallbackDraft = buildEmptyDraft();
  if (typeof window === "undefined") {
    return {
      sourcePath: "",
      selectedImportName: "",
      selectedHorizon: "5",
      strongQuantile: "0.9",
      enableUnknownConfigs: false,
      draft: fallbackDraft,
      pageData: null,
    };
  }

  const stored = readJsonStorage<Partial<PersistedState>>(
    window.localStorage,
    STRATEGY_VALIDATION_STATE_KEY,
  );

  return {
    sourcePath:
      typeof stored?.sourcePath === "string"
        ? stored.sourcePath
        : readStoredSourcePath(),
    selectedImportName:
      typeof stored?.selectedImportName === "string"
        ? stored.selectedImportName
        : "",
    selectedHorizon:
      typeof stored?.selectedHorizon === "string"
        ? stored.selectedHorizon
        : "5",
    strongQuantile:
      typeof stored?.strongQuantile === "string"
        ? stored.strongQuantile
        : "0.9",
    enableUnknownConfigs:
      typeof stored?.enableUnknownConfigs === "boolean"
        ? stored.enableUnknownConfigs
        : false,
    draft:
      stored?.draft && typeof stored.draft === "object"
        ? {
            ...fallbackDraft,
            ...stored.draft,
            unknown_configs: Array.isArray(stored.draft.unknown_configs)
              ? stored.draft.unknown_configs
              : [],
          }
        : fallbackDraft,
    pageData:
      stored?.pageData && typeof stored.pageData === "object"
        ? (stored.pageData as StrategyPerformanceValidationPageData)
        : null,
  };
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

function formatLift(value?: number | null) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  return `${value.toFixed(2)}x`;
}

function valueClassName(value?: number | null) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "";
  }
  if (value > 0) {
    return "strategy-validation-positive";
  }
  if (value < 0) {
    return "strategy-validation-negative";
  }
  return "strategy-validation-neutral";
}

function isHitVsNonHitScoreMode(scoreMode?: string | null) {
  return scoreMode === "hit_vs_non_hit";
}

function summaryMetricLabel(
  scoreMode: string | undefined,
  slot: "primary" | "secondary",
) {
  if (isHitVsNonHitScoreMode(scoreMode)) {
    return slot === "primary" ? "Hit差" : "命中均收益";
  }
  return slot === "primary" ? "IC" : "ICIR";
}

function summaryMetricValue(
  scoreMode: string | undefined,
  slot: "primary" | "secondary",
  value?: number | null,
) {
  if (isHitVsNonHitScoreMode(scoreMode)) {
    return slot === "primary" ? formatPercent(value) : formatPercent(value);
  }
  return slot === "primary" ? formatNumber(value, 3) : formatNumber(value, 2);
}

function metricForHorizon(
  row:
    | StrategyPerformanceValidationCaseData["positive_row"]
    | StrategyPerformanceValidationCaseData["negative_row"],
  horizon: number,
) {
  return row?.metrics.find((item) => item.horizon === horizon) ?? null;
}

function sanitizeDraft(
  draft: StrategyPerformanceValidationDraft,
  enableUnknownConfigs: boolean,
) {
  const unknown_configs = enableUnknownConfigs
    ? draft.unknown_configs
        .map((item) => ({
          name: item.name.trim(),
          start: Number(item.start),
          end: Number(item.end),
          step: Number(item.step),
        }))
        .filter((item) => item.name.length > 0)
    : [];

  return {
    strategy_direction: draft.strategy_direction,
    scope_way: draft.scope_way.trim().toUpperCase(),
    scope_windows: Math.max(1, Math.floor(draft.scope_windows || 1)),
    when: draft.when.trim(),
    import_name: draft.import_name?.trim() || null,
    unknown_configs,
  } satisfies StrategyPerformanceValidationDraft;
}

function ValidationCaseSection({
  title,
  direction,
  caseData,
  horizons,
  showSharedDetails = true,
}: {
  title: string;
  direction: "positive" | "negative";
  caseData?: StrategyPerformanceValidationCaseData | null;
  horizons: number[];
  showSharedDetails?: boolean;
}) {
  if (!caseData) {
    return null;
  }
  const actualRow =
    direction === "positive" ? caseData.positive_row : caseData.negative_row;
  if (!actualRow) {
    return null;
  }

  return (
    <section className="strategy-manage-card strategy-validation-card">
      <div className="strategy-manage-section-head">
        <div>
          <h3 className="strategy-manage-subtitle">{title}</h3>
          <p className="strategy-manage-note">
            {caseData.combo_summary.combo_label}
          </p>
        </div>
        <div className="strategy-validation-chip-row">
          <span className="strategy-validation-chip">
            {isHitVsNonHitScoreMode(caseData.combo_summary.score_mode)
              ? "二元触发统计"
              : "多级触发统计"}
          </span>
          <span className="strategy-validation-chip is-warm">
            触发样本 {formatNumber(caseData.combo_summary.trigger_samples, 0)}
          </span>
          <span className="strategy-validation-chip">
            平均每日触发{" "}
            {formatNumber(caseData.combo_summary.avg_daily_trigger)}
          </span>
        </div>
      </div>

      <div className="strategy-validation-note-box">
        <strong>替换后公式</strong>
        <p>{caseData.combo_summary.formula || "--"}</p>
      </div>

      <div className="strategy-validation-direction-layout">
        <article
          key={actualRow.signal_direction}
          className="strategy-validation-direction-card"
        >
          <div className="strategy-validation-direction-head">
            <div>
              <h4>{actualRow.direction_label}</h4>
              <p>{actualRow.explain || "--"}</p>
            </div>
            <div className="strategy-validation-chip-row">
              <span className="strategy-validation-chip">
                {isHitVsNonHitScoreMode(
                  metricForHorizon(actualRow, horizons[0])?.score_mode,
                )
                  ? "Hit vs Non-hit"
                  : "IC / ICIR"}
              </span>
              {actualRow.signal_direction === "positive" ? (
                <span
                  className={
                    actualRow.auto_candidate
                      ? "strategy-validation-chip is-positive"
                      : "strategy-validation-chip"
                  }
                >
                  {actualRow.auto_candidate ? "偏正向" : "正向优势一般"}
                </span>
              ) : (
                <span
                  className={
                    actualRow.negative_effective
                      ? "strategy-validation-chip is-negative"
                      : "strategy-validation-chip"
                  }
                >
                  {actualRow.negative_effective ? "偏负向" : "负向未定"}
                </span>
              )}
            </div>
          </div>

          <div className="strategy-validation-metric-row">
            {horizons.map((horizon) => {
              const metric = metricForHorizon(actualRow, horizon);
              const usesHitVsNonHit = isHitVsNonHitScoreMode(
                metric?.score_mode,
              );
              return (
                <div key={horizon} className="strategy-validation-metric-card">
                  <div className="strategy-validation-metric-title">
                    {horizon} 日
                  </div>
                  <div className="strategy-validation-metric-stack">
                    <span>样本 {formatNumber(metric?.hit_n, 0)}</span>
                    <span
                      className={valueClassName(metric?.avg_future_return_pct)}
                    >
                      均收益 {formatPercent(metric?.avg_future_return_pct)}
                    </span>
                    {usesHitVsNonHit ? (
                      <>
                        <span>胜率 {formatRate(metric?.win_rate)}</span>
                        <span>Lift {formatLift(metric?.strong_lift)}</span>
                        <span
                          className={valueClassName(
                            metric?.hit_vs_non_hit_delta_pct,
                          )}
                        >
                          Hit差{" "}
                          {formatPercent(metric?.hit_vs_non_hit_delta_pct)}
                        </span>
                      </>
                    ) : (
                      <>
                        <span className={valueClassName(metric?.rank_ic_mean)}>
                          IC {formatNumber(metric?.rank_ic_mean, 3)}
                        </span>
                        <span className={valueClassName(metric?.icir)}>
                          ICIR {formatNumber(metric?.icir, 2)}
                        </span>
                        <span className={valueClassName(metric?.sharpe_ratio)}>
                          Sharpe {formatNumber(metric?.sharpe_ratio, 2)}
                        </span>
                      </>
                    )}
                  </div>
                </div>
              );
            })}
          </div>

          {actualRow.signal_direction === "negative" &&
          actualRow.negative_review_notes.length > 0 ? (
            <div className="strategy-validation-note-box is-soft">
              <strong>负向判断备注</strong>
              <ul>
                {actualRow.negative_review_notes.map((note) => (
                  <li key={note}>{note}</li>
                ))}
              </ul>
            </div>
          ) : null}
        </article>
      </div>

      {showSharedDetails ? <ValidationSharedDetails caseData={caseData} /> : null}
    </section>
  );
}

function ValidationSharedDetails({
  caseData,
  title = "共享验证明细",
}: {
  caseData: StrategyPerformanceValidationCaseData;
  title?: string;
}) {
  return (
    <section className="strategy-manage-card strategy-validation-card">
      <div className="strategy-manage-section-head">
        <div>
          <h3 className="strategy-manage-subtitle">{title}</h3>
          <p className="strategy-manage-note">{caseData.combo_summary.combo_label}</p>
        </div>
      </div>

      <div className="strategy-validation-detail-layout">
        <article className="strategy-validation-detail-card">
          <div className="strategy-validation-direction-head">
            <div>
              <h4>触发分层</h4>
              <p>
                {caseData.layer_mode === "each_count"
                  ? "按 EACH 的触发次数分层"
                  : caseData.layer_mode === "recent_distance"
                    ? "按 RECENT 的最近触发距离分层"
                    : "当前模式没有额外层级，展示整体触发样本"}
              </p>
            </div>
          </div>
          <div className="strategy-validation-table-wrap">
            <table className="strategy-validation-table">
              <thead>
                <tr>
                  <th>层级</th>
                  <th>样本数</th>
                  <th>均收益</th>
                  <th>强势命中率</th>
                  <th>胜率</th>
                </tr>
              </thead>
              <tbody>
                {caseData.layer_rows.map((row) => (
                  <tr key={`${row.label}-${row.layer_value}`}>
                    <td>{row.label}</td>
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
        </article>

        <article className="strategy-validation-detail-card">
          <div className="strategy-validation-direction-head">
            <div>
              <h4>触发相似度</h4>
              <p>观察它与其他现有策略在同一标的同一日期同时触发的占比。</p>
            </div>
          </div>
          <div className="strategy-validation-table-wrap">
            <table className="strategy-validation-table">
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
                {caseData.similarity_rows.length > 0 ? (
                  caseData.similarity_rows.map((row) => (
                    <tr key={row.rule_name}>
                      <td>
                        <strong>{row.rule_name}</strong>
                        {row.explain ? <div>{row.explain}</div> : null}
                      </td>
                      <td>{formatNumber(row.overlap_samples, 0)}</td>
                      <td>{formatRate(row.overlap_rate_vs_validation)}</td>
                      <td>{formatRate(row.overlap_rate_vs_existing)}</td>
                      <td>{formatLift(row.overlap_lift)}</td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={5}>暂无其他现有策略与当前方案同日同股同时触发。</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </article>
      </div>
    </section>
  );
}

export default function StrategyValidationBacktestPage() {
  const initialState = useMemo(buildInitialState, []);
  const [sourcePath, setSourcePath] = useState(initialState.sourcePath);
  const [rules, setRules] = useState<StrategyManageRuleItem[]>([]);
  const [selectedImportName, setSelectedImportName] = useState(
    initialState.selectedImportName,
  );
  const [selectedHorizon, setSelectedHorizon] = useState(
    initialState.selectedHorizon,
  );
  const [strongQuantile, setStrongQuantile] = useState(
    initialState.strongQuantile,
  );
  const [enableUnknownConfigs, setEnableUnknownConfigs] = useState(
    initialState.enableUnknownConfigs,
  );
  const [draft, setDraft] = useState<StrategyPerformanceValidationDraft>(
    initialState.draft,
  );
  const [pageData, setPageData] =
    useState<StrategyPerformanceValidationPageData | null>(
      initialState.pageData,
    );
  const [loadingRules, setLoadingRules] = useState(true);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");

  const sourcePathTrimmed = sourcePath.trim();
  const draftScopeState = useMemo(
    () => parseScopeWayDraft(draft.scope_way ?? "LAST"),
    [draft.scope_way],
  );
  const hasUnknownVariants = useMemo(
    () =>
      (pageData?.combo_summaries ?? []).some(
        (row) => (row.unknown_values?.length ?? 0) > 0,
      ),
    [pageData],
  );
  const bestPositiveCase = pageData?.best_positive_case ?? null;
  const bestNegativeCase = pageData?.best_negative_case ?? null;
  const strategyDirection = pageData?.strategy_direction ?? draft.strategy_direction;
  const primaryDirection = strategyDirection;
  const secondaryDirection =
    primaryDirection === "positive" ? "negative" : "positive";
  const primaryCase =
    primaryDirection === "positive" ? bestPositiveCase : bestNegativeCase;
  const secondaryCase =
    primaryDirection === "positive" ? bestNegativeCase : bestPositiveCase;
  const sharedCaseData = useMemo(() => {
    if (!bestPositiveCase || !bestNegativeCase) {
      return null;
    }
    return bestPositiveCase.combo_summary.combo_key ===
      bestNegativeCase.combo_summary.combo_key
      ? bestPositiveCase
      : null;
  }, [bestNegativeCase, bestPositiveCase]);
  const primaryCaseTitle = hasUnknownVariants
    ? `最优${strategyDirectionLabel(primaryDirection)}方案`
    : `当前草稿${strategyDirectionLabel(primaryDirection)}表现`;
  const secondaryCaseTitle = hasUnknownVariants
    ? `${strategyDirectionLabel(secondaryDirection)}参考方案`
    : `${strategyDirectionLabel(secondaryDirection)}参考`;

  useEffect(() => {
    let cancelled = false;

    const runLoad = async () => {
      setLoadingRules(true);
      try {
        const resolvedSourcePath = await ensureManagedSourcePath();
        const data = await getStrategyManagePage(resolvedSourcePath);
        if (cancelled) {
          return;
        }
        setSourcePath(resolvedSourcePath);
        setRules(data.rules ?? []);
      } catch (loadError) {
        if (!cancelled) {
          setRules([]);
          setError(`读取策略列表失败: ${String(loadError)}`);
        }
      } finally {
        if (!cancelled) {
          setLoadingRules(false);
        }
      }
    };

    void runLoad();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    writeJsonStorage(window.localStorage, STRATEGY_VALIDATION_STATE_KEY, {
      sourcePath,
      selectedImportName,
      selectedHorizon,
      strongQuantile,
      enableUnknownConfigs,
      draft,
      pageData,
    } satisfies PersistedState);
  }, [
    draft,
    enableUnknownConfigs,
    pageData,
    selectedHorizon,
    selectedImportName,
    sourcePath,
    strongQuantile,
  ]);

  function applyImportedRule(rule: StrategyManageRuleItem) {
    setSelectedImportName(rule.name);
    setEnableUnknownConfigs(false);
    setDraft(buildDraftFromRule(rule));
    setNotice(`已导入策略：${rule.name}`);
    setError("");
  }

  function resetDraft() {
    setSelectedImportName("");
    setEnableUnknownConfigs(false);
    setDraft(buildEmptyDraft());
    setPageData(null);
    setNotice("已清空验证草稿。");
    setError("");
  }

  async function onRunValidation() {
    if (!sourcePathTrimmed) {
      setError("当前数据目录为空，无法运行策略验证。");
      return;
    }

    const preparedDraft = sanitizeDraft(draft, enableUnknownConfigs);
    if (!preparedDraft.when) {
      setError("表达式不能为空。");
      return;
    }

    setRunning(true);
    setError("");
    setNotice("");
    try {
      const result = await getStrategyPerformanceValidationPage({
        sourcePath: sourcePathTrimmed,
        selectedHorizon: Number(selectedHorizon),
        strongQuantile: Number(strongQuantile),
        draft: preparedDraft,
      });
      setPageData(result);
      setNotice(
        preparedDraft.unknown_configs.length > 0
          ? `已完成策略验证，共评估 ${result.combo_summaries.length} 组未知数组合。`
          : "已完成策略验证，当前按单一参数方案运行。",
      );
    } catch (runError) {
      setError(`运行策略验证失败: ${String(runError)}`);
    } finally {
      setRunning(false);
    }
  }

  return (
    <div className="strategy-validation-page">
      <section className="strategy-manage-card strategy-validation-card">
        <div className="strategy-manage-section-head">
          <div>
            <h2 className="strategy-manage-title">策略验证微调</h2>
            <p className="strategy-manage-note">
              可直接验证当前草稿；如果勾选未知数，再按字符串替换展开多组参数统一做触发与回测验证。
            </p>
          </div>
          <span className="strategy-manage-tip">点击后计算</span>
        </div>

        <div className="strategy-manage-source-note">
          当前数据目录：<strong>{sourcePathTrimmed || "--"}</strong>
        </div>

        <div className="strategy-manage-source-note">
          当前临时策略方向：<strong>{strategyDirectionLabel(strategyDirection)}</strong>
        </div>

        <div className="strategy-validation-import-bar">
          <label className="strategy-manage-field strategy-validation-import-field">
            <span>从现有策略导入</span>
            <select
              value={selectedImportName}
              onChange={(event) => {
                const nextName = event.target.value;
                setSelectedImportName(nextName);
                const matched = rules.find((item) => item.name === nextName);
                if (matched) {
                  applyImportedRule(matched);
                }
              }}
              disabled={loadingRules || running}
            >
              <option value="">请选择现有策略</option>
              {rules.map((rule) => (
                <option key={rule.name} value={rule.name}>
                  {rule.name}
                </option>
              ))}
            </select>
          </label>

          <div className="strategy-manage-toolbar-right">
            <button
              type="button"
              className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-secondary"
              onClick={resetDraft}
              disabled={running}
            >
              清空草稿
            </button>
            <button
              type="button"
              className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-secondary"
              onClick={() => {
                void (async () => {
                  setLoadingRules(true);
                  try {
                    const resolvedSourcePath = await ensureManagedSourcePath();
                    const data =
                      await getStrategyManagePage(resolvedSourcePath);
                    setSourcePath(resolvedSourcePath);
                    setRules(data.rules ?? []);
                    setNotice("已刷新现有策略列表。");
                    setError("");
                  } catch (loadError) {
                    setError(`刷新策略列表失败: ${String(loadError)}`);
                  } finally {
                    setLoadingRules(false);
                  }
                })();
              }}
              disabled={loadingRules || running}
            >
              {loadingRules ? "刷新中..." : "刷新策略列表"}
            </button>
          </div>
        </div>

        <div className="strategy-manage-editor-grid strategy-validation-editor-grid">
          <label className="strategy-manage-field">
            <span>临时策略方向</span>
            <select
              value={draft.strategy_direction}
              onChange={(event) =>
                setDraft((current) => ({
                  ...current,
                  strategy_direction: event.target.value as StrategyDirection,
                }))
              }
            >
              {DIRECTION_OPTIONS.map((item) => (
                <option key={item} value={item}>
                  {strategyDirectionLabel(item)}
                </option>
              ))}
            </select>
          </label>

          <label className="strategy-manage-field">
            <span>触发方式</span>
            <select
              value={draftScopeState.mode}
              onChange={(event) => {
                const nextMode = event.target.value as ScopeMode;
                setDraft((current) => ({
                  ...current,
                  scope_way: buildScopeWayValue(
                    nextMode,
                    draftScopeState.consecThreshold,
                  ),
                }));
              }}
            >
              {SCOPE_OPTIONS.map((item) => (
                <option key={item} value={item}>
                  {item === "CONSEC" ? "CONSEC>=" : item}
                </option>
              ))}
            </select>
          </label>

          <label className="strategy-manage-field">
            <span>窗口长度</span>
            <DraftNumberInput
              min={1}
              value={draft.scope_windows}
              integer
              onCommit={(value) =>
                setDraft((current) => ({
                  ...current,
                  scope_windows: value,
                }))
              }
            />
          </label>

          {draftScopeState.mode === "CONSEC" ? (
            <label className="strategy-manage-field">
              <span>连续命中阈值</span>
              <DraftNumberInput
                min={1}
                value={draftScopeState.consecThreshold}
                integer
                onCommit={(nextThreshold) => {
                  setDraft((current) => ({
                    ...current,
                    scope_way: buildScopeWayValue("CONSEC", nextThreshold),
                  }));
                }}
              />
            </label>
          ) : null}

          <label className="strategy-manage-field">
            <span>回测持有周期</span>
            <select
              value={selectedHorizon}
              onChange={(event) => setSelectedHorizon(event.target.value)}
            >
              {HORIZON_OPTIONS.map((item) => (
                <option key={item} value={item}>
                  {item} 日
                </option>
              ))}
            </select>
          </label>

          <label className="strategy-manage-field">
            <span>强势分位阈值</span>
            <select
              value={strongQuantile}
              onChange={(event) => setStrongQuantile(event.target.value)}
            >
              {QUANTILE_OPTIONS.map((item) => (
                <option key={item} value={item}>
                  {item.toFixed(2)}
                </option>
              ))}
            </select>
          </label>

          <div className="strategy-validation-unknown-block strategy-manage-field-span-full">
            <div className="strategy-validation-unknown-toolbar">
              <span className="strategy-validation-unknown-label">未知数配置</span>
              <div className="strategy-validation-unknown-actions">
                <label className="strategy-validation-checkbox">
                  <input
                    type="checkbox"
                    checked={enableUnknownConfigs}
                    onChange={(event) => {
                      const checked = event.target.checked;
                      setEnableUnknownConfigs(checked);
                      if (checked) {
                        setDraft((current) => ({
                          ...current,
                          unknown_configs:
                            current.unknown_configs.length > 0
                              ? current.unknown_configs
                              : [buildEmptyUnknown()],
                        }));
                      }
                    }}
                    disabled={running}
                  />
                  <span>启用未知数</span>
                </label>
                {enableUnknownConfigs ? (
                  <button
                    type="button"
                    className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-secondary strategy-validation-unknown-add"
                    onClick={() =>
                      setDraft((current) => ({
                        ...current,
                        unknown_configs: [
                          ...current.unknown_configs,
                          buildEmptyUnknown(),
                        ],
                      }))
                    }
                    disabled={running}
                  >
                    + 增加未知数
                  </button>
                ) : null}
              </div>
            </div>

            {enableUnknownConfigs ? (
              <div className="strategy-validation-unknown-list">
                {draft.unknown_configs.map((item, index) => (
                  <div
                    key={`unknown-${index}`}
                    className="strategy-validation-unknown-row"
                  >
                    <label className="strategy-manage-field">
                      <span>变量名</span>
                      <input
                        value={item.name}
                        onChange={(event) =>
                          setDraft((current) => ({
                            ...current,
                            unknown_configs: current.unknown_configs.map(
                              (config, configIndex) =>
                                configIndex === index
                                  ? { ...config, name: event.target.value }
                                  : config,
                            ),
                          }))
                        }
                        placeholder="N"
                      />
                    </label>
                    <label className="strategy-manage-field">
                      <span>起始</span>
                      <DraftNumberInput
                        value={item.start}
                        step="any"
                        onCommit={(value) =>
                          setDraft((current) => ({
                            ...current,
                            unknown_configs: current.unknown_configs.map(
                              (config, configIndex) =>
                                configIndex === index
                                  ? {
                                      ...config,
                                      start: value,
                                    }
                                  : config,
                            ),
                          }))
                        }
                      />
                    </label>
                    <label className="strategy-manage-field">
                      <span>结束</span>
                      <DraftNumberInput
                        value={item.end}
                        step="any"
                        onCommit={(value) =>
                          setDraft((current) => ({
                            ...current,
                            unknown_configs: current.unknown_configs.map(
                              (config, configIndex) =>
                                configIndex === index
                                  ? {
                                      ...config,
                                      end: value,
                                    }
                                  : config,
                            ),
                          }))
                        }
                      />
                    </label>
                    <label className="strategy-manage-field">
                      <span>步长</span>
                      <DraftNumberInput
                        value={item.step}
                        step="any"
                        onCommit={(value) =>
                          setDraft((current) => ({
                            ...current,
                            unknown_configs: current.unknown_configs.map(
                              (config, configIndex) =>
                                configIndex === index
                                  ? {
                                      ...config,
                                      step: value,
                                    }
                                  : config,
                            ),
                          }))
                        }
                      />
                    </label>
                    <button
                      type="button"
                      className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-secondary strategy-validation-unknown-remove"
                      onClick={() =>
                        setDraft((current) => ({
                          ...current,
                          unknown_configs:
                            current.unknown_configs.length <= 1
                              ? [buildEmptyUnknown()]
                              : current.unknown_configs.filter(
                                  (_, configIndex) => configIndex !== index,
                                ),
                        }))
                      }
                      disabled={running}
                    >
                      删除
                    </button>
                  </div>
                ))}
              </div>
            ) : null}
          </div>

          <label className="strategy-manage-field strategy-manage-field-span-full">
            <span>表达式</span>
            <textarea
              rows={8}
              value={draft.when}
              onChange={(event) =>
                setDraft((current) => ({
                  ...current,
                  when: event.target.value,
                }))
              }
              placeholder="例如：C > REF(C, N) and V > MA(V, M)"
            />
          </label>
        </div>

        {error ? (
          <div className="strategy-manage-message strategy-manage-message-error">
            {error}
          </div>
        ) : null}
        {notice ? (
          <div className="strategy-manage-message strategy-manage-message-notice">
            {notice}
          </div>
        ) : null}

        <div className="strategy-manage-editor-actions strategy-validation-actions">
          <button
            type="button"
            className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-primary"
            onClick={() => void onRunValidation()}
            disabled={running}
          >
            {running ? "验证中..." : "运行验证"}
          </button>
        </div>
      </section>

      {pageData ? (
        <>
          <section className="strategy-manage-card strategy-validation-card">
            <div className="strategy-manage-section-head">
              <div>
                <h3 className="strategy-manage-subtitle">未来强势股阈值</h3>
              </div>
            </div>

            <div className="strategy-validation-future-grid">
              {pageData.future_summaries.map((summary) => (
                <article
                  key={summary.horizon}
                  className={
                    summary.horizon === pageData.selected_horizon
                      ? "strategy-validation-future-card is-active"
                      : "strategy-validation-future-card"
                  }
                >
                  <div className="strategy-validation-future-card-head">
                    <strong>{summary.horizon} 日</strong>
                  </div>
                  <dl className="strategy-validation-future-list">
                    <div>
                      <dt>全样本数</dt>
                      <dd>{formatNumber(summary.sample_count, 0)}</dd>
                    </div>
                    <div>
                      <dt>均收益</dt>
                      <dd
                        className={valueClassName(
                          summary.avg_future_return_pct,
                        )}
                      >
                        {formatPercent(summary.avg_future_return_pct)}
                      </dd>
                    </div>
                    <div>
                      <dt>强势阈值</dt>
                      <dd>{formatPercent(summary.strong_threshold_pct)}</dd>
                    </div>
                    <div>
                      <dt>强势基准 / 胜率</dt>
                      <dd>
                        {formatRate(summary.strong_base_rate)} /{" "}
                        {formatRate(summary.win_rate)}
                      </dd>
                    </div>
                  </dl>
                </article>
              ))}
            </div>
          </section>

          {hasUnknownVariants ? (
            <section className="strategy-manage-card strategy-validation-card">
              <div className="strategy-manage-section-head">
                <div>
                  <h3 className="strategy-manage-subtitle">未知数组合表现</h3>
                  <p className="strategy-manage-note">
                    仅在启用未知数时展示不同参数方案的触发样本与正负向表现。
                  </p>
                </div>
                <span className="strategy-validation-pill">
                  共 {pageData.combo_summaries.length} 组
                </span>
              </div>

              <div className="strategy-validation-table-wrap">
                <table className="strategy-validation-table">
                  <thead>
                    <tr>
                      <th>组合</th>
                      <th>触发样本</th>
                      <th>触发日</th>
                      <th>平均每日触发</th>
                      <th>评分模式</th>
                      <th>正向综合分</th>
                      <th>正向统计</th>
                      <th>负向判定</th>
                      <th>负向统计</th>
                    </tr>
                  </thead>
                  <tbody>
                    {pageData.combo_summaries.map((row) => (
                      <tr key={row.combo_key}>
                        <td>
                          <strong>{row.combo_label}</strong>
                          <div>{row.formula}</div>
                        </td>
                        <td>{formatNumber(row.trigger_samples, 0)}</td>
                        <td>{formatNumber(row.triggered_days, 0)}</td>
                        <td>{formatNumber(row.avg_daily_trigger)}</td>
                        <td>
                          {isHitVsNonHitScoreMode(row.score_mode)
                            ? "Hit vs Non-hit"
                            : "IC / ICIR"}
                        </td>
                        <td>
                          {formatNumber(
                            row.positive_overall_composite_score,
                            3,
                          )}
                        </td>
                        <td
                          className={valueClassName(row.positive_primary_metric)}
                        >
                          {summaryMetricLabel(row.score_mode, "primary")}{" "}
                          {summaryMetricValue(
                            row.score_mode,
                            "primary",
                            row.positive_primary_metric,
                          )}
                          <div>
                            {summaryMetricLabel(row.score_mode, "secondary")}{" "}
                            {summaryMetricValue(
                              row.score_mode,
                              "secondary",
                              row.positive_secondary_metric,
                            )}
                          </div>
                        </td>
                        <td>{row.negative_effective ? "偏负向" : "未确定"}</td>
                        <td
                          className={valueClassName(row.negative_primary_metric)}
                        >
                          {summaryMetricLabel(row.score_mode, "primary")}{" "}
                          {summaryMetricValue(
                            row.score_mode,
                            "primary",
                            row.negative_primary_metric,
                          )}
                          <div>
                            {summaryMetricLabel(row.score_mode, "secondary")}{" "}
                            {summaryMetricValue(
                              row.score_mode,
                              "secondary",
                              row.negative_secondary_metric,
                            )}
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          ) : null}

          <ValidationCaseSection
            title={primaryCaseTitle}
            direction={primaryDirection}
            caseData={primaryCase}
            horizons={pageData.horizons}
            showSharedDetails={!sharedCaseData}
          />
          <ValidationCaseSection
            title={secondaryCaseTitle}
            direction={secondaryDirection}
            caseData={secondaryCase}
            horizons={pageData.horizons}
            showSharedDetails={!sharedCaseData}
          />
          {sharedCaseData ? (
            <ValidationSharedDetails
              caseData={sharedCaseData}
              title={hasUnknownVariants ? "最优方案共享明细" : "当前草稿共享明细"}
            />
          ) : null}

          <section className="strategy-manage-card strategy-validation-card">
            <div className="strategy-manage-section-head">
              <div>
                <h3 className="strategy-manage-subtitle">统计口径</h3>
              </div>
            </div>
            <div className="strategy-validation-method-list">
              {pageData.methods.map((item) => (
                <article
                  key={item.key}
                  className="strategy-validation-method-card"
                >
                  <strong>{item.title}</strong>
                  <p>{item.description}</p>
                </article>
              ))}
            </div>
          </section>
        </>
      ) : null}
    </div>
  );
}
