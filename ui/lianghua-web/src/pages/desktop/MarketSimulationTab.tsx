import { useEffect, useMemo, useState } from "react";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import {
  getMarketSimulationPage,
  type MarketSimulationPageData,
  type MarketSimulationRow,
  type MarketSimulationScenarioInput,
} from "../../apis/marketSimulation";
import type { DetailStrategyTriggerRow } from "../../apis/details";
import { listRankTradeDates } from "../../apis/reader";
import {
  formatConceptText,
  useConceptExclusions,
} from "../../shared/conceptExclusions";
import DetailsLink from "../../shared/DetailsLink";
import { splitTsCode } from "../../shared/stockCode";
import {
  readJsonStorage,
  readStoredSourcePath,
  writeJsonStorage,
} from "../../shared/storage";
import { useRouteScrollRegion } from "../../shared/routeScroll";
import {
  DEFAULT_DATE_OPTION,
  normalizeTradeDates,
  pickDateValue,
} from "../../shared/tradeDate";
import "./css/MarketSimulationTab.css";

const DEFAULT_TOP_LIMIT = "50";
const MARKET_SIMULATION_STATE_KEY = "lh_market_simulation_page_state_v1";

type SortMode = "sim_score" | "score_delta";
type ScenarioPresetKey =
  | "rise_expand"
  | "rise_shrink"
  | "fall_expand"
  | "fall_shrink"
  | "flat";

type ScenarioDraft = {
  id: string;
  presetKey: ScenarioPresetKey;
  label: string;
  pctChgInput: string;
  volumeRatioInput: string;
};

type PersistedMarketSimulationState = {
  sourcePath: string;
  dateOptions: string[];
  referenceTradeDate: string;
  topLimitInput: string;
  sortMode: SortMode;
  strongScoreFloorInput: string;
  scenarios: ScenarioDraft[];
  pageData: MarketSimulationPageData | null;
};

type MarketSimulationRowDelta = {
  latestPrice: number | null;
  latestChangePct: number | null;
  volumeRatio: number | null;
};

type MatchBadge = "NEW" | "OUT" | "IN" | null;

const SCENARIO_PRESETS: Record<
  ScenarioPresetKey,
  { label: string; pctChg: string; volumeRatio: string }
> = {
  rise_expand: { label: "放量涨", pctChg: "3.0", volumeRatio: "1.8" },
  rise_shrink: { label: "缩量涨", pctChg: "2.0", volumeRatio: "0.7" },
  fall_expand: { label: "放量跌", pctChg: "-3.0", volumeRatio: "1.8" },
  fall_shrink: { label: "缩量跌", pctChg: "-2.0", volumeRatio: "0.7" },
  flat: { label: "平量平盘", pctChg: "0.0", volumeRatio: "1.0" },
};

const PRESET_SEQUENCE: ScenarioPresetKey[] = [
  "rise_expand",
  "rise_shrink",
  "fall_expand",
  "fall_shrink",
  "flat",
];

function createScenarioDraft(
  presetKey: ScenarioPresetKey,
  seed = Date.now().toString(),
): ScenarioDraft {
  const preset = SCENARIO_PRESETS[presetKey];
  return {
    id: `scenario_${presetKey}_${seed}`,
    presetKey,
    label: preset.label,
    pctChgInput: preset.pctChg,
    volumeRatioInput: preset.volumeRatio,
  };
}

function formatNumber(value: number | null | undefined, digits = 2) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "--";
  }
  return Number.isInteger(value) ? String(value) : value.toFixed(digits);
}

function formatPercent(value: number | null | undefined) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "--";
  }
  return `${value.toFixed(2)}%`;
}

function formatSignedNumber(value: number | null | undefined, digits = 2) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "--";
  }
  const formatted = value.toFixed(digits);
  return value > 0 ? `+${formatted}` : formatted;
}

function formatSignedPercent(value: number | null | undefined) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "--";
  }
  const formatted = value.toFixed(2);
  return value > 0 ? `+${formatted}%` : `${formatted}%`;
}

function formatRatio(value: number | null | undefined) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "--";
  }
  return value.toFixed(2);
}

function getSignedValueClassName(value: number | null | undefined) {
  if (typeof value !== "number" || !Number.isFinite(value) || value === 0) {
    return "market-simulation-value-flat";
  }
  return value > 0 ? "market-simulation-value-up" : "market-simulation-value-down";
}

function computeDelta(
  nextValue: number | null | undefined,
  previousValue: number | null | undefined,
) {
  if (
    typeof nextValue !== "number" ||
    !Number.isFinite(nextValue) ||
    typeof previousValue !== "number" ||
    !Number.isFinite(previousValue)
  ) {
    return null;
  }
  return nextValue - previousValue;
}

function ValueWithDelta({
  value,
  delta,
  valueClassName,
  deltaClassName,
}: {
  value: string;
  delta: string | null;
  valueClassName?: string;
  deltaClassName?: string;
}) {
  return (
    <span className="market-simulation-value-inline">
      <span className={valueClassName}>{value}</span>
      {delta ? (
        <small className={deltaClassName ?? "market-simulation-value-flat"}>
          {delta}
        </small>
      ) : null}
    </span>
  );
}

function toPositiveInt(raw: string) {
  const parsed = Number(raw.trim());
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return null;
  }
  return parsed;
}

function parseOptionalNumber(raw: string) {
  const trimmed = raw.trim();
  if (trimmed === "") {
    return null;
  }
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : null;
}

function buildScenarioQuery(
  scenarios: ScenarioDraft[],
): MarketSimulationScenarioInput[] | null {
  const out: MarketSimulationScenarioInput[] = [];

  for (let index = 0; index < scenarios.length; index += 1) {
    const scenario = scenarios[index];
    const pctChg = parseOptionalNumber(scenario.pctChgInput);
    const volumeRatio = parseOptionalNumber(scenario.volumeRatioInput);
    if (pctChg === null || volumeRatio === null || volumeRatio < 0) {
      return null;
    }
    out.push({
      id: scenario.id,
      label: scenario.label.trim() || `场景 ${index + 1}`,
      pctChg,
      volumeRatio,
    });
  }

  return out;
}

function buildStrategyCompareRows(row: MarketSimulationRow): DetailStrategyTriggerRow[] {
  return row.triggeredRules.map((rule) => ({
    rule_name: rule.ruleName,
    rule_score: rule.ruleScore,
    is_triggered: true,
    hit_date: undefined,
    lag: null,
    explain: "模拟命中",
    tag: "simulate",
    when: undefined,
  }));
}

export default function MarketSimulationTab() {
  const { excludedConcepts } = useConceptExclusions();
  const persistedState = useMemo(() => {
    const parsed = readJsonStorage<Partial<PersistedMarketSimulationState>>(
      typeof window === "undefined" ? null : window.sessionStorage,
      MARKET_SIMULATION_STATE_KEY,
    );
    if (!parsed || typeof parsed !== "object") {
      return null;
    }

    const scenarios = Array.isArray(parsed.scenarios)
      ? parsed.scenarios.filter(
          (item): item is ScenarioDraft =>
            !!item &&
            typeof item === "object" &&
            typeof item.id === "string" &&
            typeof item.label === "string" &&
            typeof item.pctChgInput === "string" &&
            typeof item.volumeRatioInput === "string" &&
            typeof item.presetKey === "string",
        )
      : [];

    return {
      sourcePath:
        typeof parsed.sourcePath === "string" ? parsed.sourcePath : "",
      dateOptions: Array.isArray(parsed.dateOptions)
        ? parsed.dateOptions.filter(
            (item): item is string => typeof item === "string",
          )
        : [],
      referenceTradeDate:
        typeof parsed.referenceTradeDate === "string"
          ? parsed.referenceTradeDate
          : DEFAULT_DATE_OPTION,
      topLimitInput:
        typeof parsed.topLimitInput === "string"
          ? parsed.topLimitInput
          : DEFAULT_TOP_LIMIT,
      sortMode:
        parsed.sortMode === "score_delta" ? "score_delta" : "sim_score",
      strongScoreFloorInput:
        typeof parsed.strongScoreFloorInput === "string"
          ? parsed.strongScoreFloorInput
          : "",
      scenarios:
        scenarios.length > 0
          ? scenarios
          : [createScenarioDraft("rise_expand", "default")],
      pageData:
        parsed.pageData && typeof parsed.pageData === "object"
          ? (parsed.pageData as MarketSimulationPageData)
          : null,
    } satisfies PersistedMarketSimulationState;
  }, []);

  const [sourcePath, setSourcePath] = useState(
    () => persistedState?.sourcePath ?? readStoredSourcePath(),
  );
  const [dateOptions, setDateOptions] = useState<string[]>(
    () => persistedState?.dateOptions ?? [],
  );
  const [referenceTradeDate, setReferenceTradeDate] = useState(
    () => persistedState?.referenceTradeDate ?? DEFAULT_DATE_OPTION,
  );
  const [topLimitInput, setTopLimitInput] = useState(
    () => persistedState?.topLimitInput ?? DEFAULT_TOP_LIMIT,
  );
  const [sortMode, setSortMode] = useState<SortMode>(
    () => persistedState?.sortMode ?? "sim_score",
  );
  const [strongScoreFloorInput, setStrongScoreFloorInput] = useState(
    () => persistedState?.strongScoreFloorInput ?? "",
  );
  const [scenarios, setScenarios] = useState<ScenarioDraft[]>(
    () => persistedState?.scenarios ?? [createScenarioDraft("rise_expand", "default")],
  );
  const [loading, setLoading] = useState(false);
  const [dateOptionsLoading, setDateOptionsLoading] = useState(false);
  const [error, setError] = useState("");
  const [pageData, setPageData] = useState<MarketSimulationPageData | null>(
    () => persistedState?.pageData ?? null,
  );
  const [rowDeltas, setRowDeltas] = useState<
    Record<string, Record<string, MarketSimulationRowDelta>>
  >({});
  const [matchBadges, setMatchBadges] = useState<
    Record<string, Record<string, MatchBadge>>
  >({});

  const sourcePathTrimmed = sourcePath.trim();

  useEffect(() => {
    writeJsonStorage(
      typeof window === "undefined" ? null : window.sessionStorage,
      MARKET_SIMULATION_STATE_KEY,
      {
        sourcePath,
        dateOptions,
        referenceTradeDate,
        topLimitInput,
        sortMode,
        strongScoreFloorInput,
        scenarios,
        pageData,
      } satisfies PersistedMarketSimulationState,
    );
  }, [
    dateOptions,
    pageData,
    referenceTradeDate,
    scenarios,
    sortMode,
    sourcePath,
    strongScoreFloorInput,
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
    if (!sourcePathTrimmed) {
      setDateOptions([]);
      setReferenceTradeDate(DEFAULT_DATE_OPTION);
      return;
    }

    let cancelled = false;
    void (async () => {
      setDateOptionsLoading(true);
      try {
        const values = normalizeTradeDates(
          await listRankTradeDates(sourcePathTrimmed),
        );
        if (cancelled) {
          return;
        }
        setDateOptions(values);
        setReferenceTradeDate((current) => pickDateValue(current, values));
      } catch (loadError) {
        if (!cancelled) {
          setDateOptions([]);
          setReferenceTradeDate(DEFAULT_DATE_OPTION);
          setError(`读取参考日失败: ${String(loadError)}`);
        }
      } finally {
        if (!cancelled) {
          setDateOptionsLoading(false);
        }
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [sourcePathTrimmed]);

  async function onRefresh() {
    const topLimit = toPositiveInt(topLimitInput);
    const strongScoreFloor = parseOptionalNumber(strongScoreFloorInput);
    const scenarioQuery = buildScenarioQuery(scenarios);

    if (topLimit === null) {
      setError("参与模拟的排名数量必须是正整数");
      return;
    }
    if (!sourcePathTrimmed) {
      setError("请先到“数据管理”页确认当前目录");
      return;
    }
    if (!scenarioQuery) {
      setError("请检查场景涨跌幅和量比输入");
      return;
    }

    setLoading(true);
    setError("");
    try {
      const nextPageData = await getMarketSimulationPage({
        sourcePath: sourcePathTrimmed,
        referenceTradeDate: referenceTradeDate.trim() || undefined,
        topLimit,
        scenarios: scenarioQuery,
        sortMode,
        strongScoreFloor: strongScoreFloor ?? undefined,
      });

      const previousScenarioMap =
        pageData?.referenceTradeDate === nextPageData.referenceTradeDate
          ? new Map(
              (pageData?.scenarios ?? []).map((scenario) => [scenario.id, scenario] as const),
            )
          : new Map<string, (typeof nextPageData.scenarios)[number]>();

      const nextRowDeltas: Record<string, Record<string, MarketSimulationRowDelta>> = {};
      const nextMatchBadges: Record<string, Record<string, MatchBadge>> = {};

      for (const scenario of nextPageData.scenarios) {
        const previousRows = new Map(
          (previousScenarioMap.get(scenario.id)?.rows ?? []).map((row) => [row.tsCode, row] as const),
        );
        nextRowDeltas[scenario.id] = {};
        nextMatchBadges[scenario.id] = {};

        for (const row of scenario.rows) {
          const previousRow = previousRows.get(row.tsCode);
          nextRowDeltas[scenario.id][row.tsCode] = {
            latestPrice: computeDelta(row.latestPrice, previousRow?.latestPrice),
            latestChangePct: computeDelta(
              row.latestChangePct,
              previousRow?.latestChangePct,
            ),
            volumeRatio: computeDelta(row.volumeRatio, previousRow?.volumeRatio),
          };

          const previousMatched = previousRow?.realtimeMatched ?? false;
          nextMatchBadges[scenario.id][row.tsCode] = row.realtimeMatched
            ? previousMatched
              ? "IN"
              : "NEW"
            : previousMatched
              ? "OUT"
              : null;
        }
      }

      setRowDeltas(nextRowDeltas);
      setMatchBadges(nextMatchBadges);
      setPageData(nextPageData);
    } catch (refreshError) {
      setError(`刷新预演买点失败: ${String(refreshError)}`);
    } finally {
      setLoading(false);
    }
  }

  const scenarioCards = pageData?.scenarios ?? [];
  const statusText = pageData
    ? [
        pageData.refreshedAt ? `最新刷新 ${pageData.refreshedAt}` : null,
        pageData.referenceTradeDate ? `参考日 ${pageData.referenceTradeDate}` : null,
        pageData.simulatedTradeDate ? `模拟日 ${pageData.simulatedTradeDate}` : null,
        `候选 ${pageData.candidateCount} 只`,
        `实时已抓取 ${pageData.fetchedCount}/${pageData.effectiveCount}`,
      ]
        .filter(Boolean)
        .join(" | ")
    : "先配置场景，再手动刷新，按参考日前列股票批量模拟并联动实时涨幅判断。";
  const scenarioGridRef = useRouteScrollRegion<HTMLDivElement>(
    "market-simulation-grid",
    [scenarioCards.length, scenarioCards.map((item) => item.rows.length).join("|")],
  );

  return (
    <section className="market-simulation-card">
      <div className="market-simulation-head">
        <div>
          <h2 className="market-simulation-title">预演买点</h2>
          <p className="market-simulation-tip">{statusText}</p>
        </div>
      </div>

      <div className="market-simulation-toolbar">
        <label className="market-simulation-field">
          <span>参考日</span>
          <select
            value={referenceTradeDate}
            onChange={(event) => setReferenceTradeDate(event.target.value)}
            disabled={dateOptionsLoading || dateOptions.length === 0}
          >
            {dateOptions.length === 0 ? (
              <option value="">
                {dateOptionsLoading ? "读取中..." : "暂无可选日期"}
              </option>
            ) : null}
            {dateOptions.map((option) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
        </label>

        <label className="market-simulation-field">
          <span>参与模拟排名数</span>
          <input
            type="number"
            min={1}
            step={1}
            value={topLimitInput}
            onChange={(event) => setTopLimitInput(event.target.value)}
          />
        </label>

        <label className="market-simulation-field">
          <span>展示方式</span>
          <select
            value={sortMode}
            onChange={(event) => setSortMode(event.target.value as SortMode)}
          >
            <option value="sim_score">按模拟后得分</option>
            <option value="score_delta">按得分增量</option>
          </select>
        </label>

        <label className="market-simulation-field">
          <span>强势维持门槛</span>
          <input
            type="number"
            step="0.1"
            value={strongScoreFloorInput}
            onChange={(event) => setStrongScoreFloorInput(event.target.value)}
            placeholder="留空则不启用"
          />
        </label>

        <div className="market-simulation-actions">
          <button
            className="market-simulation-add-btn"
            type="button"
            disabled={scenarios.length >= 5}
            onClick={() =>
              setScenarios((current) => [
                ...current,
                createScenarioDraft(
                  PRESET_SEQUENCE[current.length % PRESET_SEQUENCE.length],
                  `${Date.now()}_${current.length}`,
                ),
              ])
            }
          >
            {scenarios.length >= 5 ? "最多 5 个场景" : "新增场景"}
          </button>
          <button
            className="market-simulation-refresh-btn"
            type="button"
            onClick={() => void onRefresh()}
            disabled={loading || dateOptionsLoading}
          >
            {loading ? "刷新中..." : "手动刷新"}
          </button>
        </div>
      </div>

      <div className="market-simulation-config-grid">
        {scenarios.map((scenario, index) => (
          <article className="market-simulation-config-card" key={scenario.id}>
            <div className="market-simulation-config-head">
              <strong>场景 {index + 1}</strong>
              <button
                className="market-simulation-remove-btn"
                type="button"
                disabled={scenarios.length <= 1}
                onClick={() =>
                  setScenarios((current) =>
                    current.filter((item) => item.id !== scenario.id),
                  )
                }
              >
                删除
              </button>
            </div>

            <label className="market-simulation-field market-simulation-field-compact">
              <span>预设</span>
              <select
                value={scenario.presetKey}
                onChange={(event) => {
                  const presetKey = event.target.value as ScenarioPresetKey;
                  const preset = SCENARIO_PRESETS[presetKey];
                  setScenarios((current) =>
                    current.map((item) =>
                      item.id === scenario.id
                        ? {
                            ...item,
                            presetKey,
                            label: preset.label,
                            pctChgInput: preset.pctChg,
                            volumeRatioInput: preset.volumeRatio,
                          }
                        : item,
                    ),
                  );
                }}
              >
                <option value="rise_expand">放量涨</option>
                <option value="rise_shrink">缩量涨</option>
                <option value="fall_expand">放量跌</option>
                <option value="fall_shrink">缩量跌</option>
                <option value="flat">平量平盘</option>
              </select>
            </label>

            <label className="market-simulation-field market-simulation-field-compact">
              <span>名称</span>
              <input
                type="text"
                value={scenario.label}
                onChange={(event) =>
                  setScenarios((current) =>
                    current.map((item) =>
                      item.id === scenario.id
                        ? { ...item, label: event.target.value }
                        : item,
                    ),
                  )
                }
              />
            </label>

            <div className="market-simulation-field-row">
              <label className="market-simulation-field market-simulation-field-compact">
                <span>涨跌幅 %</span>
                <input
                  type="number"
                  step="0.1"
                  value={scenario.pctChgInput}
                  onChange={(event) =>
                    setScenarios((current) =>
                      current.map((item) =>
                        item.id === scenario.id
                          ? { ...item, pctChgInput: event.target.value }
                          : item,
                      ),
                    )
                  }
                />
              </label>

              <label className="market-simulation-field market-simulation-field-compact">
                <span>量比</span>
                <input
                  type="number"
                  step="0.1"
                  min="0"
                  value={scenario.volumeRatioInput}
                  onChange={(event) =>
                    setScenarios((current) =>
                      current.map((item) =>
                        item.id === scenario.id
                          ? { ...item, volumeRatioInput: event.target.value }
                          : item,
                      ),
                    )
                  }
                />
              </label>
            </div>
          </article>
        ))}
      </div>

      {error ? <div className="market-simulation-empty">{error}</div> : null}
      {!error && scenarioCards.length === 0 ? (
        <div className="market-simulation-empty">
          当前还没有模拟结果，先配置场景再刷新。
        </div>
      ) : null}

      {scenarioCards.length > 0 ? (
        <div className="market-simulation-scenarios" ref={scenarioGridRef}>
          {scenarioCards.map((scenario) => {
            const rowDeltaMap = rowDeltas[scenario.id] ?? {};
            const matchBadgeMap = matchBadges[scenario.id] ?? {};
            const detailNavigationItems = scenario.rows.map((row) => ({
              tsCode: row.tsCode,
              tradeDate: pageData?.referenceTradeDate ?? referenceTradeDate,
              sourcePath: sourcePathTrimmed || undefined,
              name: row.name || undefined,
            }));

            return (
              <section className="market-simulation-scenario-panel" key={scenario.id}>
                <header className="market-simulation-scenario-head">
                  <div>
                    <h3>{scenario.label}</h3>
                    <p>
                      设定 {formatSignedPercent(scenario.pctChg)} / 量比{" "}
                      {formatRatio(scenario.volumeRatio)}
                    </p>
                  </div>
                  <div className="market-simulation-scenario-meta">
                    <span>命中 {scenario.matchedCount}</span>
                    <span>强维持 {scenario.strongHoldCount}</span>
                  </div>
                </header>

                <div className="market-simulation-table-wrap">
                  <table className="market-simulation-table">
                    <thead>
                      <tr>
                        <th>代码</th>
                        <th>名称</th>
                        <th>参考排</th>
                        <th>原分</th>
                        <th>模拟分</th>
                        <th>增分</th>
                        <th>实时价</th>
                        <th>实时涨幅</th>
                        <th>量比</th>
                        <th>模拟规则</th>
                      </tr>
                    </thead>
                    <tbody>
                      {scenario.rows.map((row) => {
                        const conceptText = formatConceptText(
                          row.concept,
                          excludedConcepts,
                        );
                        const rowDelta = rowDeltaMap[row.tsCode];
                        const badge = matchBadgeMap[row.tsCode];
                        const compareSnapshot = {
                          tsCode: row.tsCode,
                          relativeTradeDate: `${scenario.label} 模拟`,
                          rows: buildStrategyCompareRows(row),
                        };

                        return (
                          <tr
                            className={[
                              row.realtimeMatched ? "is-matched" : "",
                              row.strongHold ? "is-strong" : "",
                            ]
                              .filter(Boolean)
                              .join(" ")}
                            key={`${scenario.id}:${row.tsCode}`}
                            title={conceptText}
                          >
                            <td>{row.tsCode}</td>
                            <td>
                              <div className="market-simulation-stock-cell">
                                <DetailsLink
                                  className="market-simulation-stock-link"
                                  tsCode={splitTsCode(row.tsCode)}
                                  tradeDate={
                                    pageData?.referenceTradeDate ?? referenceTradeDate
                                  }
                                  sourcePath={sourcePathTrimmed}
                                  navigationItems={detailNavigationItems}
                                  strategyCompareSnapshot={compareSnapshot}
                                >
                                  {row.name || "--"}
                                </DetailsLink>
                                <div className="market-simulation-badges">
                                  {row.strongHold ? (
                                    <span className="market-simulation-badge is-strong">
                                      强维持
                                    </span>
                                  ) : null}
                                  {badge ? (
                                    <span
                                      className={`market-simulation-badge is-${badge.toLowerCase()}`}
                                    >
                                      {badge}
                                    </span>
                                  ) : null}
                                </div>
                              </div>
                            </td>
                            <td>{formatNumber(row.referenceRank, 0)}</td>
                            <td>{formatNumber(row.baseTotalScore)}</td>
                            <td className={getSignedValueClassName(row.simulatedTotalScore)}>
                              {formatNumber(row.simulatedTotalScore)}
                            </td>
                            <td className={getSignedValueClassName(row.scoreDelta)}>
                              {formatSignedNumber(row.scoreDelta)}
                            </td>
                            <td>
                              <ValueWithDelta
                                value={formatNumber(row.latestPrice)}
                                delta={
                                  rowDelta?.latestPrice !== null &&
                                  rowDelta?.latestPrice !== undefined
                                    ? formatSignedNumber(rowDelta.latestPrice)
                                    : null
                                }
                                deltaClassName={getSignedValueClassName(
                                  rowDelta?.latestPrice,
                                )}
                              />
                            </td>
                            <td>
                              <ValueWithDelta
                                value={formatPercent(row.latestChangePct)}
                                delta={
                                  rowDelta?.latestChangePct !== null &&
                                  rowDelta?.latestChangePct !== undefined
                                    ? formatSignedPercent(rowDelta.latestChangePct)
                                    : null
                                }
                                valueClassName={getSignedValueClassName(
                                  row.latestChangePct,
                                )}
                                deltaClassName={getSignedValueClassName(
                                  rowDelta?.latestChangePct,
                                )}
                              />
                            </td>
                            <td>
                              <ValueWithDelta
                                value={formatRatio(row.volumeRatio)}
                                delta={
                                  rowDelta?.volumeRatio !== null &&
                                  rowDelta?.volumeRatio !== undefined
                                    ? formatSignedNumber(rowDelta.volumeRatio)
                                    : null
                                }
                                deltaClassName={getSignedValueClassName(
                                  rowDelta?.volumeRatio,
                                )}
                              />
                            </td>
                            <td
                              className="market-simulation-rule-cell"
                              title={
                                row.triggeredRules.length > 0
                                  ? row.triggeredRules
                                      .map(
                                        (item) =>
                                          `${item.ruleName}(${formatNumber(item.ruleScore)})`,
                                      )
                                      .join("、")
                                  : "暂无触发规则"
                              }
                            >
                              {row.triggeredRules.length > 0
                                ? row.triggeredRules
                                    .map((item) => item.ruleName)
                                    .join("、")
                                : "--"}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </section>
            );
          })}
        </div>
      ) : null}
    </section>
  );
}
