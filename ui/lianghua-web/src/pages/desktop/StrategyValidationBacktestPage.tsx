import { useEffect, useMemo, useState } from "react";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import {
  checkStrategyManageRuleDraft,
  getStrategyManagePage,
  type StrategyManageDistPoint,
  type StrategyManageRuleDraft,
  type StrategyManageRuleItem,
} from "../../apis/strategyManage";
import {
  getStrategyPerformanceValidationPage,
  type StrategyPerformanceValidationPageData,
} from "../../apis/strategyValidation";
import {
  readJsonStorage,
  readStoredSourcePath,
  writeJsonStorage,
} from "../../shared/storage";
import "./css/StrategyManagePage.css";
import "./css/StrategyValidationBacktestPage.css";

const STRATEGY_VALIDATION_STATE_KEY = "lh_strategy_validation_backtest_v1";
const QUANTILE_OPTIONS = [0.8, 0.9, 0.95] as const;
const HORIZON_OPTIONS = [2, 3, 5, 10] as const;
const TAG_OPTIONS = ["Normal", "Opportunity", "Rare"] as const;
const SCOPE_OPTIONS = ["LAST", "ANY", "EACH", "RECENT", "CONSEC"] as const;

type ScoreMode = "fixed" | "dist";
type ScopeMode = (typeof SCOPE_OPTIONS)[number];

type PersistedState = {
  sourcePath: string;
  selectedImportName: string;
  selectedHorizon: string;
  strongQuantile: string;
  draft: StrategyManageRuleDraft;
  scoreMode: ScoreMode;
  distPointsText: string;
  fixedPointsText: string;
  pageData: StrategyPerformanceValidationPageData | null;
};

function hasDistPoints(items?: StrategyManageDistPoint[] | null) {
  return Boolean(items && items.length > 0);
}

function buildDraftFromRule(rule: StrategyManageRuleItem): StrategyManageRuleDraft {
  return {
    name: rule.name,
    scope_way: rule.scope_way,
    scope_windows: rule.scope_windows,
    when: rule.when,
    points: rule.points,
    dist_points: rule.dist_points ?? null,
    explain: rule.explain,
    tag: rule.tag,
  };
}

function buildEmptyDraft(): StrategyManageRuleDraft {
  return {
    name: "",
    scope_way: "LAST",
    scope_windows: 1,
    when: "",
    points: 0,
    dist_points: null,
    explain: "",
    tag: "Normal",
  };
}

function distPointsToText(items?: StrategyManageDistPoint[] | null) {
  if (!items || items.length === 0) {
    return "";
  }
  return items.map((item) => `${item.min},${item.max},${item.points}`).join("\n");
}

function parseDistPointsText(raw: string) {
  const trimmed = raw.trim();
  if (!trimmed) {
    return null;
  }

  return trimmed.split("\n").map((line, index) => {
    const parts = line
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean);
    if (parts.length !== 3) {
      throw new Error(`字典得分第 ${index + 1} 行格式错误，应为 min,max,points`);
    }

    const min = Number(parts[0]);
    const max = Number(parts[1]);
    const points = Number(parts[2]);
    if (!Number.isInteger(min) || !Number.isInteger(max) || !Number.isFinite(points)) {
      throw new Error(`字典得分第 ${index + 1} 行存在非法数值`);
    }
    return { min, max, points };
  });
}

function parseFixedPointsText(raw: string) {
  const trimmed = raw.trim();
  if (!trimmed) {
    throw new Error("固定分值不能为空");
  }

  const parsed = Number(trimmed);
  if (!Number.isFinite(parsed)) {
    throw new Error("固定分值必须是合法数字");
  }
  return parsed;
}

function buildPreparedDraft(
  draft: StrategyManageRuleDraft,
  scoreMode: ScoreMode,
  distPointsText: string,
  fixedPointsText: string,
) {
  const parsedDistPoints = scoreMode === "dist" ? parseDistPointsText(distPointsText) : null;
  if (scoreMode === "dist" && (!parsedDistPoints || parsedDistPoints.length === 0)) {
    throw new Error("当前选择的是字典分，至少需要填写一条字典得分");
  }
  const parsedFixedPoints =
    scoreMode === "fixed" ? parseFixedPointsText(fixedPointsText) : draft.points;

  return {
    ...draft,
    name: draft.name.trim(),
    scope_way: draft.scope_way.trim().toUpperCase(),
    when: draft.when.trim(),
    explain: draft.explain.trim(),
    tag: draft.tag.trim() || "Normal",
    dist_points: parsedDistPoints,
    points: parsedFixedPoints,
  } satisfies StrategyManageRuleDraft;
}

function parseScopeWayDraft(scopeWay: string): { mode: ScopeMode; consecThreshold: number } {
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
    const normalized = Math.max(1, Math.floor(consecThreshold || 1));
    return `CONSEC>=${normalized}`;
  }
  return mode;
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

function metricForHorizon(
  row: NonNullable<StrategyPerformanceValidationPageData["rule_rows"]>[number],
  horizon: number,
) {
  return row.metrics.find((item) => item.horizon === horizon) ?? null;
}

function findRuleByName(rules: StrategyManageRuleItem[], name: string) {
  return rules.find((item) => item.name === name) ?? null;
}

function buildInitialState(): PersistedState {
  const fallbackDraft = buildEmptyDraft();
  if (typeof window === "undefined") {
    return {
      sourcePath: "",
      selectedImportName: "",
      selectedHorizon: "10",
      strongQuantile: "0.9",
      draft: fallbackDraft,
      scoreMode: "fixed",
      distPointsText: "",
      fixedPointsText: "0",
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
      typeof stored?.selectedImportName === "string" ? stored.selectedImportName : "",
    selectedHorizon:
      typeof stored?.selectedHorizon === "string" ? stored.selectedHorizon : "10",
    strongQuantile:
      typeof stored?.strongQuantile === "string" ? stored.strongQuantile : "0.9",
    draft:
      stored?.draft && typeof stored.draft === "object"
        ? {
            ...fallbackDraft,
            ...stored.draft,
          }
        : fallbackDraft,
    scoreMode:
      stored?.scoreMode === "dist" || stored?.scoreMode === "fixed"
        ? stored.scoreMode
        : "fixed",
    distPointsText:
      typeof stored?.distPointsText === "string" ? stored.distPointsText : "",
    fixedPointsText:
      typeof stored?.fixedPointsText === "string" ? stored.fixedPointsText : "0",
    pageData:
      stored?.pageData && typeof stored.pageData === "object"
        ? (stored.pageData as StrategyPerformanceValidationPageData)
        : null,
  };
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
  const [draft, setDraft] = useState<StrategyManageRuleDraft>(initialState.draft);
  const [scoreMode, setScoreMode] = useState<ScoreMode>(initialState.scoreMode);
  const [distPointsText, setDistPointsText] = useState(initialState.distPointsText);
  const [fixedPointsText, setFixedPointsText] = useState(initialState.fixedPointsText);
  const [pageData, setPageData] = useState<StrategyPerformanceValidationPageData | null>(
    initialState.pageData,
  );
  const [loadingRules, setLoadingRules] = useState(true);
  const [checking, setChecking] = useState(false);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [checkNotice, setCheckNotice] = useState("");

  const sourcePathTrimmed = sourcePath.trim();
  const draftScopeState = useMemo(
    () => parseScopeWayDraft(draft.scope_way ?? "LAST"),
    [draft.scope_way],
  );
  const distPreviewError = useMemo(() => {
    if (scoreMode !== "dist") {
      return "";
    }
    try {
      parseDistPointsText(distPointsText);
      return "";
    } catch (previewError) {
      return String(previewError);
    }
  }, [distPointsText, scoreMode]);

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
        setLoadingRules(false);
      } catch (loadError) {
        if (cancelled) {
          return;
        }
        setLoadingRules(false);
        setRules([]);
        setError(`读取策略列表失败: ${String(loadError)}`);
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
      draft,
      scoreMode,
      distPointsText,
      fixedPointsText,
      pageData,
    } satisfies PersistedState);
  }, [
    distPointsText,
    draft,
    fixedPointsText,
    pageData,
    scoreMode,
    selectedHorizon,
    selectedImportName,
    sourcePath,
    strongQuantile,
  ]);

  function applyImportedRule(rule: StrategyManageRuleItem) {
    setSelectedImportName(rule.name);
    setDraft(buildDraftFromRule(rule));
    setScoreMode(hasDistPoints(rule.dist_points) ? "dist" : "fixed");
    setDistPointsText(distPointsToText(rule.dist_points));
    setFixedPointsText(String(rule.points));
    setCheckNotice("");
    setNotice(`已导入策略：${rule.name}`);
    setError("");
  }

  function resetDraft() {
    setSelectedImportName("");
    setDraft(buildEmptyDraft());
    setScoreMode("fixed");
    setDistPointsText("");
    setFixedPointsText("0");
    setCheckNotice("");
    setNotice("已清空草稿，可从零开始配置。");
    setError("");
  }

  function getPreparedDraft() {
    return buildPreparedDraft(draft, scoreMode, distPointsText, fixedPointsText);
  }

  async function onCheckDraft() {
    if (!sourcePathTrimmed) {
      setError("当前数据目录为空，无法检查策略草稿。");
      return;
    }

    setChecking(true);
    setError("");
    setNotice("");
    try {
      const preparedDraft = getPreparedDraft();
      const message = await checkStrategyManageRuleDraft(
        sourcePathTrimmed,
        preparedDraft,
      );
      setCheckNotice(message);
    } catch (checkError) {
      setError(`检查策略失败: ${String(checkError)}`);
    } finally {
      setChecking(false);
    }
  }

  async function onRunValidation() {
    if (!sourcePathTrimmed) {
      setError("当前数据目录为空，无法运行策略验证。");
      return;
    }

    setRunning(true);
    setError("");
    setNotice("");
    try {
      const preparedDraft = getPreparedDraft();
      const result = await getStrategyPerformanceValidationPage({
        sourcePath: sourcePathTrimmed,
        selectedHorizon: Number(selectedHorizon),
        strongQuantile: Number(strongQuantile),
        draft: preparedDraft,
      });
      setPageData(result);
      setCheckNotice("");
      setNotice(`已完成策略验证：${preparedDraft.name}`);
    } catch (runError) {
      setError(`运行策略验证失败: ${String(runError)}`);
    } finally {
      setRunning(false);
    }
  }

  const positiveRow =
    pageData?.rule_rows.find((item) => item.signal_direction === "positive") ?? null;
  const negativeRow =
    pageData?.rule_rows.find((item) => item.signal_direction === "negative") ?? null;

  return (
    <div className="strategy-validation-page">
      <section className="strategy-manage-card strategy-validation-card">
        <div className="strategy-manage-section-head">
          <div>
            <h2 className="strategy-manage-title">策略验证微调</h2>
            <p className="strategy-manage-note">
              固定页面配置单条策略草稿，可直接导入现有策略，检查表达式与参数后，复用策略表现回测底层口径跑出该策略自身的未来表现。
            </p>
          </div>
          <span className="strategy-manage-tip">
            仅在点击“运行验证”后计算，避免进页即重算
          </span>
        </div>

        <div className="strategy-manage-source-note">
          当前数据目录：<strong>{sourcePathTrimmed || "--"}</strong>
        </div>

        <div className="strategy-validation-import-bar">
          <label className="strategy-manage-field strategy-validation-import-field">
            <span>从现有策略导入</span>
            <select
              value={selectedImportName}
              onChange={(event) => {
                const nextName = event.target.value;
                setSelectedImportName(nextName);
                const matched = findRuleByName(rules, nextName);
                if (matched) {
                  applyImportedRule(matched);
                }
              }}
              disabled={loadingRules || checking || running}
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
              disabled={checking || running}
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
                    const data = await getStrategyManagePage(resolvedSourcePath);
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
              disabled={loadingRules || checking || running}
            >
              {loadingRules ? "刷新中..." : "刷新策略列表"}
            </button>
          </div>
        </div>

        <div className="strategy-manage-editor-grid strategy-validation-editor-grid">
          <label className="strategy-manage-field">
            <span>策略名称</span>
            <input
              value={draft.name}
              onChange={(event) =>
                setDraft((current) => ({ ...current, name: event.target.value }))
              }
              placeholder="例如：涨停后放量加强版"
            />
          </label>

          <label className="strategy-manage-field">
            <span>标签</span>
            <select
              value={draft.tag}
              onChange={(event) =>
                setDraft((current) => ({ ...current, tag: event.target.value }))
              }
            >
              {TAG_OPTIONS.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
          </label>

          <label className="strategy-manage-field">
            <span>得分方法</span>
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
            <input
              type="number"
              min={1}
              value={draft.scope_windows}
              onChange={(event) =>
                setDraft((current) => ({
                  ...current,
                  scope_windows: Math.max(1, Number(event.target.value) || 1),
                }))
              }
            />
          </label>

          {draftScopeState.mode === "CONSEC" ? (
            <label className="strategy-manage-field">
              <span>连续命中阈值</span>
              <input
                type="number"
                min={1}
                value={draftScopeState.consecThreshold}
                onChange={(event) => {
                  const nextThreshold = Math.max(
                    1,
                    Number(event.target.value) || 1,
                  );
                  setDraft((current) => ({
                    ...current,
                    scope_way: buildScopeWayValue("CONSEC", nextThreshold),
                  }));
                }}
              />
            </label>
          ) : null}

          <label className="strategy-manage-field strategy-manage-field-span-full">
            <span>策略说明</span>
            <input
              value={draft.explain}
              onChange={(event) =>
                setDraft((current) => ({ ...current, explain: event.target.value }))
              }
              placeholder="描述这条策略想捕捉的形态和预期"
            />
          </label>

          <div className="strategy-manage-field">
            <span>得分方式</span>
            <div className="strategy-manage-score-mode">
              <button
                type="button"
                className={
                  scoreMode === "fixed"
                    ? "strategy-manage-score-mode-btn is-active"
                    : "strategy-manage-score-mode-btn"
                }
                onClick={() => setScoreMode("fixed")}
              >
                固定分
              </button>
              <button
                type="button"
                className={
                  scoreMode === "dist"
                    ? "strategy-manage-score-mode-btn is-active"
                    : "strategy-manage-score-mode-btn"
                }
                onClick={() => setScoreMode("dist")}
              >
                字典分
              </button>
            </div>
          </div>

          {scoreMode === "fixed" ? (
            <label className="strategy-manage-field">
              <span>固定分值</span>
              <input
                value={fixedPointsText}
                onChange={(event) => setFixedPointsText(event.target.value)}
                placeholder="例如：2 或 -3"
              />
            </label>
          ) : (
            <label className="strategy-manage-field strategy-manage-field-span-full">
              <span>字典得分</span>
              <textarea
                rows={5}
                value={distPointsText}
                onChange={(event) => setDistPointsText(event.target.value)}
                placeholder={"每行一段：min,max,points\n例如：0,1,4\n2,4,2"}
              />
            </label>
          )}

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

          <label className="strategy-manage-field strategy-manage-field-span-full">
            <span>表达式</span>
            <textarea
              rows={8}
              value={draft.when}
              onChange={(event) =>
                setDraft((current) => ({ ...current, when: event.target.value }))
              }
              placeholder="例如：C > O and V > REF(V, 1)"
            />
          </label>
        </div>

        {distPreviewError ? (
          <div className="strategy-manage-message strategy-manage-message-error">
            {distPreviewError}
          </div>
        ) : null}
        {error ? (
          <div className="strategy-manage-message strategy-manage-message-error">
            {error}
          </div>
        ) : null}
        {checkNotice ? (
          <div className="strategy-manage-message strategy-manage-message-notice">
            {checkNotice}
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
            className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-secondary"
            onClick={() => void onCheckDraft()}
            disabled={checking || running}
          >
            {checking ? "检查中..." : "检查草稿"}
          </button>
          <button
            type="button"
            className="strategy-manage-toolbar-btn strategy-manage-toolbar-btn-primary"
            onClick={() => void onRunValidation()}
            disabled={checking || running}
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
                <h3 className="strategy-manage-subtitle">策略草稿概览</h3>
                <p className="strategy-manage-note">
                  这里展示当前草稿在既有数据库样本上的整体表现。正向命中和负向命中分开统计，口径与策略表现回测一致。
                </p>
              </div>
              <div className="strategy-validation-chip-row">
                <span className="strategy-validation-chip is-warm">
                  {pageData.draft_summary.scope_way} / {pageData.draft_summary.scope_windows} 窗口
                </span>
                <span className="strategy-validation-chip">
                  {pageData.draft_summary.score_mode === "dist" ? "字典分" : "固定分"}
                </span>
                {pageData.draft_summary.tag ? (
                  <span className="strategy-validation-chip">
                    标签 {pageData.draft_summary.tag}
                  </span>
                ) : null}
              </div>
            </div>

            <div className="strategy-manage-rule-metrics strategy-manage-rule-metrics-detail strategy-validation-summary-grid">
              <div className="strategy-manage-rule-metric is-score">
                <span>策略名称</span>
                <strong>{pageData.draft_summary.name}</strong>
              </div>
              <div className="strategy-manage-rule-metric">
                <span>基础分值</span>
                <strong>{formatNumber(pageData.draft_summary.points)}</strong>
              </div>
              <div className="strategy-manage-rule-metric">
                <span>当前持有周期</span>
                <strong>{pageData.selected_horizon} 日</strong>
              </div>
              <div className="strategy-manage-rule-metric">
                <span>强势阈值口径</span>
                <strong>{pageData.strong_quantile.toFixed(2)}</strong>
              </div>
            </div>

            <div className="strategy-validation-note-box">
              <strong>策略说明</strong>
              <p>{pageData.draft_summary.explain || "--"}</p>
            </div>
          </section>

          <section className="strategy-manage-card strategy-validation-card">
            <div className="strategy-manage-section-head">
              <div>
                <h3 className="strategy-manage-subtitle">未来强势股阈值</h3>
                <p className="strategy-manage-note">
                  四个持有周期共用一行展示，用来校准这条草稿命中后到底处在什么收益分布位置。
                </p>
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
                    {summary.horizon === pageData.selected_horizon ? (
                      <span className="strategy-validation-pill">当前视角</span>
                    ) : null}
                  </div>
                  <dl className="strategy-validation-future-list">
                    <div>
                      <dt>全样本数</dt>
                      <dd>{formatNumber(summary.sample_count, 0)}</dd>
                    </div>
                    <div>
                      <dt>均收益</dt>
                      <dd className={valueClassName(summary.avg_future_return_pct)}>
                        {formatPercent(summary.avg_future_return_pct)}
                      </dd>
                    </div>
                    <div>
                      <dt>p80 / p90 / p95</dt>
                      <dd>
                        {formatPercent(summary.p80_return_pct)} /{" "}
                        {formatPercent(summary.p90_return_pct)} /{" "}
                        {formatPercent(summary.p95_return_pct)}
                      </dd>
                    </div>
                    <div>
                      <dt>强势阈值</dt>
                      <dd className={valueClassName(summary.strong_threshold_pct)}>
                        {formatPercent(summary.strong_threshold_pct)}
                      </dd>
                    </div>
                    <div>
                      <dt>胜率 / 最大收益</dt>
                      <dd>
                        {formatRate(summary.win_rate)} /{" "}
                        {formatPercent(summary.max_future_return_pct)}
                      </dd>
                    </div>
                  </dl>
                </article>
              ))}
            </div>
          </section>

          <section className="strategy-manage-card strategy-validation-card">
            <div className="strategy-manage-section-head">
              <div>
                <h3 className="strategy-manage-subtitle">命中方向表现</h3>
                <p className="strategy-manage-note">
                  正向命中看这条策略是否真能抓到更好的未来收益；负向命中看风险提示是否成立。每个方向都按 2 / 3 / 5 / 10 日分别统计。
                </p>
              </div>
            </div>

            <div className="strategy-validation-direction-layout">
              {[positiveRow, negativeRow]
                .filter((item): item is NonNullable<typeof positiveRow> => Boolean(item))
                .map((row) => (
                  <article key={row.signal_direction} className="strategy-validation-direction-card">
                    <div className="strategy-validation-direction-head">
                      <div>
                        <h4>{row.direction_label}</h4>
                        <p>{row.explain || "--"}</p>
                      </div>
                      <div className="strategy-validation-chip-row">
                        {row.signal_direction === "positive" ? (
                          <span
                            className={
                              row.auto_candidate
                                ? "strategy-validation-chip is-positive"
                                : "strategy-validation-chip"
                            }
                          >
                            {row.auto_candidate
                              ? "满足默认优势候选口径"
                              : "未满足默认优势候选口径"}
                          </span>
                        ) : row.negative_effectiveness_label ? (
                          <span
                            className={
                              row.negative_effective
                                ? "strategy-validation-chip is-negative"
                                : "strategy-validation-chip"
                            }
                          >
                            {row.negative_effectiveness_label}
                          </span>
                        ) : null}
                      </div>
                    </div>

                    <div className="strategy-validation-metric-row">
                      {pageData.horizons.map((horizon) => {
                        const metric = metricForHorizon(row, horizon);
                        return (
                          <div key={horizon} className="strategy-validation-metric-card">
                            <div className="strategy-validation-metric-title">
                              {horizon} 日
                            </div>
                            <div className="strategy-validation-metric-stack">
                              <span>样本 {formatNumber(metric?.hit_n, 0)}</span>
                              <span className={valueClassName(metric?.avg_future_return_pct)}>
                                均收益 {formatPercent(metric?.avg_future_return_pct)}
                              </span>
                              <span>Lift {formatLift(metric?.strong_lift)}</span>
                              <span>胜率 {formatRate(metric?.win_rate)}</span>
                              <span className={valueClassName(metric?.hit_vs_non_hit_delta_pct)}>
                                Hit差 {formatPercent(metric?.hit_vs_non_hit_delta_pct)}
                              </span>
                            </div>
                          </div>
                        );
                      })}
                    </div>

                    {row.signal_direction === "negative" &&
                    row.negative_review_notes.length > 0 ? (
                      <div className="strategy-validation-note-box is-soft">
                        <strong>判定说明</strong>
                        <ul>
                          {row.negative_review_notes.map((note) => (
                            <li key={note}>{note}</li>
                          ))}
                        </ul>
                      </div>
                    ) : null}
                  </article>
                ))}
            </div>
          </section>

          {pageData.rule_detail ? (
            <section className="strategy-manage-card strategy-validation-card">
              <div className="strategy-manage-section-head">
                <div>
                  <h3 className="strategy-manage-subtitle">得分强度与命中分层</h3>
                  <p className="strategy-manage-note">
                    当前展示 {pageData.rule_detail.horizon} 日持有周期下的分数分层、相关性和命中次数效果，用来判断这条草稿的分值设计是否真的有解释力。
                  </p>
                </div>
              </div>

              <div className="strategy-validation-detail-layout">
                {pageData.rule_detail.directions.map((direction) => (
                  <article key={direction.signal_direction} className="strategy-validation-detail-card">
                    <div className="strategy-validation-direction-head">
                      <div>
                        <h4>{direction.direction_label}</h4>
                        <p>
                          {direction.bucket_mode === "exact"
                            ? "按精确分值分层"
                            : "按分位桶分层"}
                        </p>
                      </div>
                    </div>

                    <div className="strategy-manage-rule-metrics strategy-manage-rule-metrics-compact">
                      <div className="strategy-manage-rule-metric">
                        <span>样本数</span>
                        <strong>{formatNumber(direction.sample_count, 0)}</strong>
                      </div>
                      <div className="strategy-manage-rule-metric">
                        <span>均收益</span>
                        <strong className={valueClassName(direction.avg_future_return_pct)}>
                          {formatPercent(direction.avg_future_return_pct)}
                        </strong>
                      </div>
                      <div className="strategy-manage-rule-metric">
                        <span>强势命中率</span>
                        <strong>{formatRate(direction.strong_hit_rate)}</strong>
                      </div>
                      <div className="strategy-manage-rule-metric">
                        <span>胜率</span>
                        <strong>{formatRate(direction.win_rate)}</strong>
                      </div>
                      <div className="strategy-manage-rule-metric">
                        <span>Spearman</span>
                        <strong>{formatNumber(direction.spearman_corr, 3)}</strong>
                      </div>
                      <div className="strategy-manage-rule-metric">
                        <span>Hit vs Non-hit</span>
                        <strong className={valueClassName(direction.hit_vs_non_hit_delta_pct)}>
                          {formatPercent(direction.hit_vs_non_hit_delta_pct)}
                        </strong>
                      </div>
                    </div>

                    <div className="strategy-validation-table-wrap">
                      <table className="strategy-validation-table">
                        <thead>
                          <tr>
                            <th>分值组</th>
                            <th>样本数</th>
                            <th>均收益</th>
                            <th>强势命中率</th>
                            <th>胜率</th>
                          </tr>
                        </thead>
                        <tbody>
                          {direction.score_rows.map((row) => (
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

                    {direction.hit_count_rows.length > 0 ? (
                      <div className="strategy-validation-table-wrap">
                        <table className="strategy-validation-table">
                          <thead>
                            <tr>
                              <th>命中次数</th>
                              <th>样本数</th>
                              <th>均收益</th>
                              <th>强势命中率</th>
                              <th>胜率</th>
                            </tr>
                          </thead>
                          <tbody>
                            {direction.hit_count_rows.map((row) => (
                              <tr key={row.hit_count}>
                                <td>{row.hit_count}</td>
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
                    ) : null}
                  </article>
                ))}
              </div>
            </section>
          ) : null}

          <section className="strategy-manage-card strategy-validation-card">
            <div className="strategy-manage-section-head">
              <div>
                <h3 className="strategy-manage-subtitle">统计口径</h3>
              </div>
            </div>
            <div className="strategy-validation-method-list">
              {pageData.methods.map((item) => (
                <article key={item.key} className="strategy-validation-method-card">
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
