import { useEffect, useMemo, useState } from "react";
import {
  runAdvancedStockPick,
  type AdvancedStockPickResultData,
  type AdvancedStockPickRow,
} from "../../apis/stockPick";
import {
  getLatestStrategyPickCache,
  type StrategyPerformancePickCachePayload,
} from "../../apis/strategyPerformance";
import {
  formatConceptText,
  useConceptExclusions,
} from "../../shared/conceptExclusions";
import DetailsLink from "../../shared/DetailsLink";
import { useRouteScrollRegion } from "../../shared/routeScroll";
import {
  TableSortButton,
  getAriaSort,
  type SortDefinition,
  useTableSort,
} from "../../shared/tableSort";
import { readJsonStorage } from "../../shared/storage";
import {
  STOCK_PICK_BOARD_OPTIONS,
  formatDateLabel,
  formatNumber,
} from "./stockPickShared";
import {
  ConceptIncludeExcludePanels,
  buildAvailableConceptOptions,
  normalizeStringArray,
  toggleStringSelection,
} from "./stockPickConceptFilter";
import { useStockPickOutletContext } from "./StockPickPage";

const ADVANCED_STOCK_PICK_STATE_KEY = "advanced-stock-pick-state";
const DEFAULT_METHOD_KEY = "mixed_topn";
const DEFAULT_BOARD = "主板";
const DEFAULT_MIN_ADV_HITS = "1";
const DEFAULT_TOP_LIMIT = "100";
const DEFAULT_RANK_MAX = "500";
const DEFAULT_MIXED_SORT_KEYS = [
  "adv_score_sum",
  "adv_hit_cnt",
  "rank",
] as const;
const MIXED_SORT_KEY_OPTIONS = [
  { value: "adv_hit_cnt", label: "优势命中数" },
  { value: "adv_score_sum", label: "优势得分和" },
  { value: "noisy_companion_cnt", label: "噪音伴随数(少优先)" },
  { value: "pos_hit_cnt", label: "正向命中数" },
  { value: "pos_score_sum", label: "正向得分和" },
  { value: "rank", label: "原始排名" },
] as const;
const METHOD_OPTIONS = [
  { value: "mixed_topn", label: "综合排序" },
  { value: "adv_hit_topn", label: "优势命中" },
  { value: "adv_score_topn", label: "优势得分" },
  { value: "companion_penalty_topn", label: "噪音更少" },
] as const;
type MethodOptionValue = (typeof METHOD_OPTIONS)[number]["value"];
type MixedSortKeyOptionValue = (typeof MIXED_SORT_KEY_OPTIONS)[number]["value"];

type AdvancedRowSortKey =
  | "rank"
  | "total_score"
  | "adv_hit_cnt"
  | "adv_score_sum"
  | "noisy_companion_cnt"
  | "pos_hit_cnt"
  | "pos_score_sum"
  ;

type PersistedAdvancedState = {
  tradeDate: string;
  board: (typeof STOCK_PICK_BOARD_OPTIONS)[number];
  area: string;
  industry: string;
  enableConceptFilter: boolean;
  includeConcepts: string[];
  excludeConcepts: string[];
  conceptKeyword: string;
  conceptMatchMode: string;
  methodKey: string;
  strategyKeyword: string;
  minAdvHits: string;
  topLimit: string;
  rankMax: string;
  mixedSortKeys: string[];
  result: AdvancedStockPickResultData | null;
};

function parsePositiveInt(value: string, fallback: number) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function normalizeMixedSortKeyValues(
  values?: string[] | null,
): MixedSortKeyOptionValue[] {
  const normalized = normalizeStringArray(values ?? []).filter((value) =>
    MIXED_SORT_KEY_OPTIONS.some((option) => option.value === value),
  ) as MixedSortKeyOptionValue[];
  return normalized.length > 0
    ? normalized
    : [...DEFAULT_MIXED_SORT_KEYS];
}

function formatPlainText(value?: string | null) {
  const trimmed = value?.trim();
  return trimmed ? trimmed : "--";
}

function methodHint(methodKey: MethodOptionValue) {
  switch (methodKey) {
    case "mixed_topn":
      return "综合排序会按下方排序键做字典序比较，越靠左优先级越高。";
    case "adv_hit_topn":
      return "优先选优势因子命中更多的股票。";
    case "adv_score_topn":
      return "优先选优势因子得分和更高的股票。";
    case "companion_penalty_topn":
      return "优先保留优势因子强、同时噪音伴随更少的股票。";
    default:
      return "按当前条件生成选股结果。";
  }
}

function normalizeMethodKey(value?: string | null) {
  return value && METHOD_OPTIONS.some((option) => option.value === value)
    ? (value as MethodOptionValue)
    : DEFAULT_METHOD_KEY;
}

function mixedSortKeyLabel(value: string) {
  return (
    MIXED_SORT_KEY_OPTIONS.find((option) => option.value === value)?.label ?? value
  );
}

function ReadonlyRuleChipPanel({
  title,
  items,
  emptyText,
  tone = "active",
}: {
  title: string;
  items: string[];
  emptyText: string;
  tone?: "active" | "neutral" | "warn";
}) {
  return (
    <div className="stock-pick-concept-panel stock-pick-advanced-panel">
      <div className="stock-pick-concept-head">
        <strong>{title}</strong>
        <span>{items.length} 项</span>
      </div>
      <div className="stock-pick-concept-list stock-pick-concept-list-inline">
        {items.length > 0 ? (
          items.map((item) => (
            <span
              className={
                tone === "neutral"
                  ? "stock-pick-chip-btn is-neutral"
                  : tone === "warn"
                    ? "stock-pick-chip-btn is-warn"
                    : "stock-pick-chip-btn is-active"
              }
              key={item}
            >
              {item}
            </span>
          ))
        ) : (
          <span className="stock-pick-note">{emptyText}</span>
        )}
      </div>
    </div>
  );
}

function AdvancedStockPickTable({
  rows,
  tradeDate,
  sourcePath,
}: {
  rows: AdvancedStockPickRow[];
  tradeDate?: string;
  sourcePath: string;
}) {
  const { excludedConcepts } = useConceptExclusions();
  const sortDefinitions = useMemo(
    () =>
      ({
        rank: { value: (row) => row.rank },
        total_score: { value: (row) => row.total_score },
        adv_hit_cnt: { value: (row) => row.adv_hit_cnt },
        adv_score_sum: { value: (row) => row.adv_score_sum },
        pos_hit_cnt: { value: (row) => row.pos_hit_cnt },
        pos_score_sum: { value: (row) => row.pos_score_sum },
        noisy_companion_cnt: { value: (row) => row.noisy_companion_cnt },
      }) satisfies Partial<
        Record<AdvancedRowSortKey, SortDefinition<AdvancedStockPickRow>>
      >,
    [],
  );
  const { sortKey, sortDirection, sortedRows, toggleSort } = useTableSort<
    AdvancedStockPickRow,
    AdvancedRowSortKey
  >(rows, sortDefinitions);
  const detailNavigationItems = sortedRows.map((row) => ({
    tsCode: row.ts_code,
    tradeDate: tradeDate || undefined,
    sourcePath: sourcePath || undefined,
    name: row.name ?? undefined,
  }));
  const tableWrapRef = useRouteScrollRegion<HTMLDivElement>(
    "advanced-stock-pick-table",
    [sortedRows.length, tradeDate, sourcePath],
  );

  if (rows.length === 0) {
    return <div className="stock-pick-empty">当前条件下没有选出股票。</div>;
  }

  return (
    <div
      className="stock-pick-table-wrap stock-pick-table-wrap-advanced"
      ref={tableWrapRef}
    >
      <table className="stock-pick-table stock-pick-table-advanced">
        <thead>
          <tr>
            <th aria-sort={getAriaSort(sortKey === "rank", sortDirection)}>
              <TableSortButton
                label="排名"
                isActive={sortKey === "rank"}
                direction={sortDirection}
                onClick={() => toggleSort("rank")}
                title="按排名排序"
              />
            </th>
            <th>股票</th>
            <th aria-sort={getAriaSort(sortKey === "total_score", sortDirection)}>
              <TableSortButton
                label="总分"
                isActive={sortKey === "total_score"}
                direction={sortDirection}
                onClick={() => toggleSort("total_score")}
                title="按总分排序"
              />
            </th>
            <th aria-sort={getAriaSort(sortKey === "adv_hit_cnt", sortDirection)}>
              <TableSortButton
                label="优势命中"
                isActive={sortKey === "adv_hit_cnt"}
                direction={sortDirection}
                onClick={() => toggleSort("adv_hit_cnt")}
                title="按优势命中数排序"
              />
            </th>
            <th aria-sort={getAriaSort(sortKey === "adv_score_sum", sortDirection)}>
              <TableSortButton
                label="优势得分"
                isActive={sortKey === "adv_score_sum"}
                direction={sortDirection}
                onClick={() => toggleSort("adv_score_sum")}
                title="按优势得分和排序"
              />
            </th>
            <th aria-sort={getAriaSort(sortKey === "pos_hit_cnt", sortDirection)}>
              <TableSortButton
                label="正向命中"
                isActive={sortKey === "pos_hit_cnt"}
                direction={sortDirection}
                onClick={() => toggleSort("pos_hit_cnt")}
                title="按正向命中数排序"
              />
            </th>
            <th aria-sort={getAriaSort(sortKey === "pos_score_sum", sortDirection)}>
              <TableSortButton
                label="正向得分"
                isActive={sortKey === "pos_score_sum"}
                direction={sortDirection}
                onClick={() => toggleSort("pos_score_sum")}
                title="按正向得分和排序"
              />
            </th>
            <th aria-sort={getAriaSort(sortKey === "noisy_companion_cnt", sortDirection)}>
              <TableSortButton
                label="噪音伴随"
                isActive={sortKey === "noisy_companion_cnt"}
                direction={sortDirection}
                onClick={() => toggleSort("noisy_companion_cnt")}
                title="按噪音伴随数量排序"
              />
            </th>
            <th>行业 / 地域</th>
            <th>优势命中</th>
            <th>伴随命中</th>
            <th>概念</th>
            <th>选股说明</th>
          </tr>
        </thead>
        <tbody>
          {sortedRows.map((row) => (
            <tr key={`${tradeDate ?? ""}-${row.ts_code}`}>
              <td>{formatNumber(row.rank, 0)}</td>
              <td>
                <DetailsLink
                  className="stock-pick-link-btn"
                  tsCode={row.ts_code}
                  tradeDate={tradeDate}
                  sourcePath={sourcePath}
                  navigationItems={detailNavigationItems}
                >
                  {row.name ?? row.ts_code}
                </DetailsLink>
                <div className="stock-pick-advanced-cell-sub">
                  {row.ts_code} · {row.board}
                </div>
              </td>
              <td>{formatNumber(row.total_score)}</td>
              <td>{formatNumber(row.adv_hit_cnt, 0)}</td>
              <td>{formatNumber(row.adv_score_sum)}</td>
              <td>{formatNumber(row.pos_hit_cnt, 0)}</td>
              <td>{formatNumber(row.pos_score_sum)}</td>
              <td>{formatNumber(row.noisy_companion_cnt, 0)}</td>
              <td className="stock-pick-cell-concept">
                <div className="stock-pick-advanced-cell-sub">
                  {formatPlainText(row.industry)}
                </div>
                <div className="stock-pick-advanced-cell-sub">
                  {formatPlainText(row.area)}
                </div>
              </td>
              <td className="stock-pick-cell-concept">
                {row.advantage_hits || "--"}
              </td>
              <td className="stock-pick-cell-concept">
                {row.companion_hits || "--"}
              </td>
              <td className="stock-pick-cell-concept">
                {formatConceptText(row.concept, excludedConcepts)}
              </td>
              <td className="stock-pick-cell-concept">{row.pick_note}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function AdvancedStockPickPage() {
  const {
    sourcePath,
    scoreTradeDateOptions,
    latestScoreTradeDate,
    conceptOptions,
    areaOptions,
    industryOptions,
    optionsLoading,
  } = useStockPickOutletContext();
  const { excludedConcepts } = useConceptExclusions();
  const persistedState = useMemo(
    () =>
      readJsonStorage<Partial<PersistedAdvancedState>>(
        typeof window === "undefined" ? null : window.sessionStorage,
        ADVANCED_STOCK_PICK_STATE_KEY,
      ),
    [],
  );
  const [tradeDate, setTradeDate] = useState(
    () => persistedState?.tradeDate ?? "",
  );
  const [board, setBoard] = useState<(typeof STOCK_PICK_BOARD_OPTIONS)[number]>(
    () =>
      persistedState?.board &&
      STOCK_PICK_BOARD_OPTIONS.includes(persistedState.board)
        ? persistedState.board
        : DEFAULT_BOARD,
  );
  const [area, setArea] = useState(() => persistedState?.area ?? "全部");
  const [industry, setIndustry] = useState(
    () => persistedState?.industry ?? "全部",
  );
  const [enableConceptFilter, setEnableConceptFilter] = useState(
    () =>
      persistedState?.enableConceptFilter ??
      Boolean(
        (persistedState?.includeConcepts?.length ?? 0) > 0 ||
          (persistedState?.excludeConcepts?.length ?? 0) > 0,
      ),
  );
  const [includeConcepts, setIncludeConcepts] = useState<string[]>(
    () => persistedState?.includeConcepts ?? [],
  );
  const [excludeConcepts, setExcludeConcepts] = useState<string[]>(
    () => persistedState?.excludeConcepts ?? [],
  );
  const [conceptKeyword, setConceptKeyword] = useState(
    () => persistedState?.conceptKeyword ?? "",
  );
  const [conceptMatchMode, setConceptMatchMode] = useState(
    () => persistedState?.conceptMatchMode ?? "OR",
  );
  const [methodKey, setMethodKey] = useState<MethodOptionValue>(
    () => normalizeMethodKey(persistedState?.methodKey),
  );
  const [strategyKeyword, setStrategyKeyword] = useState(
    () => persistedState?.strategyKeyword ?? "",
  );
  const [minAdvHits, setMinAdvHits] = useState(
    () => persistedState?.minAdvHits ?? DEFAULT_MIN_ADV_HITS,
  );
  const [topLimit, setTopLimit] = useState(
    () => persistedState?.topLimit ?? DEFAULT_TOP_LIMIT,
  );
  const [rankMax, setRankMax] = useState(
    () => persistedState?.rankMax ?? DEFAULT_RANK_MAX,
  );
  const [mixedSortKeys, setMixedSortKeys] = useState<MixedSortKeyOptionValue[]>(
    () => normalizeMixedSortKeyValues(persistedState?.mixedSortKeys),
  );
  const [result, setResult] = useState<AdvancedStockPickResultData | null>(
    () => persistedState?.result ?? null,
  );
  const [preprocessData, setPreprocessData] =
    useState<StrategyPerformancePickCachePayload | null>(null);
  const [preprocessLoading, setPreprocessLoading] = useState(false);
  const [preprocessError, setPreprocessError] = useState("");
  const [preprocessSignature, setPreprocessSignature] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [runSignature, setRunSignature] = useState("");

  const availableConceptOptions = useMemo(
    () => buildAvailableConceptOptions(conceptOptions, excludedConcepts),
    [conceptOptions, excludedConcepts],
  );
  const preprocessAutoCandidateRuleNames = useMemo(
    () => preprocessData?.resolved_advantage_rule_names ?? [],
    [preprocessData],
  );
  const filteredPreprocessAutoCandidateRuleNames = useMemo(() => {
    const keyword = strategyKeyword.trim().toLowerCase();
    if (!keyword) {
      return preprocessAutoCandidateRuleNames;
    }
    return preprocessAutoCandidateRuleNames.filter((item) =>
      item.toLowerCase().includes(keyword),
    );
  }, [preprocessAutoCandidateRuleNames, strategyKeyword]);
  const currentAdvantageRuleNames = useMemo(
    () => preprocessData?.resolved_advantage_rule_names ?? [],
    [preprocessData],
  );
  const currentCompanionRuleNames = useMemo(
    () => preprocessData?.resolved_noisy_companion_rule_names ?? [],
    [preprocessData],
  );
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
  const normalizedIncludeConcepts = useMemo(
    () => normalizeStringArray(includeConcepts),
    [includeConcepts],
  );
  const normalizedExcludeConcepts = useMemo(
    () => normalizeStringArray(excludeConcepts),
    [excludeConcepts],
  );
  const effectiveIncludeConcepts = useMemo(
    () => (enableConceptFilter ? normalizedIncludeConcepts : []),
    [enableConceptFilter, normalizedIncludeConcepts],
  );
  const effectiveExcludeConcepts = useMemo(
    () => (enableConceptFilter ? normalizedExcludeConcepts : []),
    [enableConceptFilter, normalizedExcludeConcepts],
  );
  const normalizedMixedSortKeys = useMemo(
    () => normalizeMixedSortKeyValues(mixedSortKeys),
    [mixedSortKeys],
  );
  const preprocessConfigSignature = useMemo(
    () =>
      JSON.stringify({
        sourcePath: sourcePath.trim(),
      }),
    [sourcePath],
  );
  const runConfigSignature = useMemo(
    () =>
      JSON.stringify({
        preprocessConfigSignature,
        tradeDate,
        board,
        area,
        industry,
        enableConceptFilter,
        includeConcepts: effectiveIncludeConcepts,
        excludeConcepts: effectiveExcludeConcepts,
        conceptMatchMode: enableConceptFilter ? conceptMatchMode : "OR",
        methodKey,
        minAdvHits: parsePositiveInt(minAdvHits, 1),
        topLimit: parsePositiveInt(topLimit, 100),
        rankMax: parsePositiveInt(rankMax, 1000),
        mixedSortKeys: normalizedMixedSortKeys,
      }),
    [
      area,
      board,
      conceptMatchMode,
      enableConceptFilter,
      industry,
      methodKey,
      minAdvHits,
      effectiveExcludeConcepts,
      effectiveIncludeConcepts,
      normalizedMixedSortKeys,
      preprocessConfigSignature,
      rankMax,
      topLimit,
      tradeDate,
    ],
  );
  const preprocessDirty = useMemo(
    () => Boolean(preprocessData) && preprocessSignature !== preprocessConfigSignature,
    [preprocessConfigSignature, preprocessData, preprocessSignature],
  );
  const resultDirty = useMemo(
    () => Boolean(result) && runSignature !== runConfigSignature,
    [result, runConfigSignature, runSignature],
  );

  useEffect(() => {
    if (!latestScoreTradeDate) {
      return;
    }
    setTradeDate((current) => current || latestScoreTradeDate);
  }, [latestScoreTradeDate]);

  useEffect(() => {
    setIncludeConcepts((current) => {
      const nextIncludeConcepts = buildAvailableConceptOptions(
        current,
        excludedConcepts,
      );
      return nextIncludeConcepts.length === current.length &&
        nextIncludeConcepts.every((item, index) => item === current[index])
        ? current
        : nextIncludeConcepts;
    });
    setExcludeConcepts((current) => {
      const nextExcludeConcepts = buildAvailableConceptOptions(
        current,
        excludedConcepts,
      );
      return nextExcludeConcepts.length === current.length &&
        nextExcludeConcepts.every((item, index) => item === current[index])
        ? current
        : nextExcludeConcepts;
    });
  }, [excludedConcepts]);

  useEffect(() => {
    try {
      window.sessionStorage.setItem(
        ADVANCED_STOCK_PICK_STATE_KEY,
        JSON.stringify({
          tradeDate,
          board,
          area,
          industry,
          enableConceptFilter,
          includeConcepts,
          excludeConcepts,
          conceptKeyword,
          conceptMatchMode,
          methodKey,
          strategyKeyword,
          minAdvHits,
          topLimit,
          rankMax,
          mixedSortKeys,
          result,
        } satisfies PersistedAdvancedState),
      );
    } catch {
      // Ignore storage failures.
    }
  }, [
    area,
    board,
    conceptKeyword,
    conceptMatchMode,
    enableConceptFilter,
    excludeConcepts,
    includeConcepts,
    industry,
    methodKey,
    minAdvHits,
    mixedSortKeys,
    rankMax,
    result,
    strategyKeyword,
    topLimit,
    tradeDate,
  ]);

  function addMixedSortKey(key: MixedSortKeyOptionValue) {
    setMixedSortKeys((current) =>
      current.includes(key) ? current : [...current, key],
    );
  }

  function removeMixedSortKey(key: MixedSortKeyOptionValue) {
    setMixedSortKeys((current) => {
      if (!current.includes(key) || current.length <= 1) {
        return current;
      }
      return current.filter((item) => item !== key);
    });
  }

  function moveMixedSortKey(key: MixedSortKeyOptionValue, direction: -1 | 1) {
    setMixedSortKeys((current) => {
      const index = current.indexOf(key);
      if (index < 0) {
        return current;
      }
      const nextIndex = index + direction;
      if (nextIndex < 0 || nextIndex >= current.length) {
        return current;
      }
      const next = [...current];
      [next[index], next[nextIndex]] = [next[nextIndex], next[index]];
      return next;
    });
  }

  function toggleIncludeConcept(value: string) {
    setIncludeConcepts((current) => toggleStringSelection(current, value));
    setExcludeConcepts((current) => current.filter((item) => item !== value));
  }

  function toggleExcludeConcept(value: string) {
    setExcludeConcepts((current) => toggleStringSelection(current, value));
    setIncludeConcepts((current) => current.filter((item) => item !== value));
  }

  async function preprocessAdvantageRules() {
    if (!sourcePath.trim()) {
      setPreprocessError("当前数据目录为空。");
      return;
    }
    setPreprocessLoading(true);
    setPreprocessError("");
    try {
      const pageData = await getLatestStrategyPickCache(sourcePath);
      setPreprocessData(pageData);
      setPreprocessSignature(preprocessConfigSignature);
      setResult(null);
      setRunSignature("");
      setError("");
    } catch (loadError) {
      setPreprocessData(null);
      setPreprocessError(`预处理优势集失败: ${String(loadError)}`);
    } finally {
      setPreprocessLoading(false);
    }
  }

  async function onRun() {
    if (!sourcePath.trim()) {
      setError("当前数据目录为空。");
      return;
    }
    if (!preprocessData || preprocessDirty) {
      setError("请先到策略回测页运行回测，然后回到这里读取同参数缓存。");
      return;
    }
    setLoading(true);
    setError("");
    try {
      const nextResult = await runAdvancedStockPick({
        sourcePath,
        tradeDate,
        board,
        area,
        industry,
        includeConcepts: effectiveIncludeConcepts,
        excludeConcepts: effectiveExcludeConcepts,
        conceptMatchMode: enableConceptFilter ? conceptMatchMode : "OR",
        methodKey,
        minAdvHits: parsePositiveInt(minAdvHits, 1),
        topLimit: parsePositiveInt(topLimit, 100),
        mixedSortKeys: normalizedMixedSortKeys,
        rankMax: parsePositiveInt(rankMax, 1000),
      });
      setResult(nextResult);
      setRunSignature(runConfigSignature);
    } catch (runError) {
      setResult(null);
      setError(`高级选股失败: ${String(runError)}`);
    } finally {
      setLoading(false);
    }
  }

  return (
    <section className="stock-pick-card">
      <div className="stock-pick-section-head">
        <div>
          <h3 className="stock-pick-subtitle">高级选股</h3>
          <p className="stock-pick-note">先去策略回测页跑完回测，再回来读取缓存并执行选股。</p>
        </div>
      </div>

      <div className="stock-pick-advanced-flow">
        <section className="stock-pick-advanced-step">
          <div className="stock-pick-concept-head">
            <strong>1. 读取回测缓存</strong>
            <span>直接读取最近一次策略回测结果</span>
          </div>

          <div className="stock-pick-actions stock-pick-actions-split">
            <button
              type="button"
              className="stock-pick-primary-btn"
              onClick={() => void preprocessAdvantageRules()}
              disabled={preprocessLoading || optionsLoading}
            >
              {preprocessLoading ? "读取中..." : "读取回测缓存"}
            </button>
            <span className="stock-pick-tip">
              高级选股不再触发策略回测，也不再单独设置回测参数。
            </span>
          </div>

          {preprocessData ? (
            <div className="stock-pick-advanced-status-strip">
              <span className="stock-pick-chip-btn is-neutral">
                自动优势 {preprocessAutoCandidateRuleNames.length}
              </span>
              <span className="stock-pick-chip-btn is-active">
                当前优势 {currentAdvantageRuleNames.length}
              </span>
              <span className="stock-pick-chip-btn">
                当前噪音伴随 {currentCompanionRuleNames.length}
              </span>
              <span className="stock-pick-chip-btn is-neutral">
                缓存周期 {preprocessData.selected_horizon} 日
              </span>
              <span className="stock-pick-chip-btn is-neutral">
                强势阈值 {preprocessData.strong_quantile.toFixed(2)}
              </span>
            </div>
          ) : null}

          {preprocessDirty ? (
            <div className="stock-pick-advanced-callout is-warn">
              参数和当前缓存不一致。请先到策略回测页按同样参数重跑一次，再回来读取缓存。
            </div>
          ) : null}
        </section>

        <section className="stock-pick-advanced-step">
          <div className="stock-pick-concept-head">
            <strong>2. 查看缓存策略集合</strong>
            <span>这里展示策略回测页已经确定下来的集合</span>
          </div>

          {!preprocessData ? (
            <div className="stock-pick-empty">
              先去策略回测页跑完回测，再在上一步读取缓存。
            </div>
          ) : (
            <div className="stock-pick-advanced-stack">
              <div className="stock-pick-concept-panel stock-pick-advanced-panel">
                <div className="stock-pick-concept-toolbar">
                  <input
                    type="text"
                    value={strategyKeyword}
                    onChange={(event) => setStrategyKeyword(event.target.value)}
                    placeholder="搜索策略"
                    className="stock-pick-concept-search"
                  />
                </div>
              </div>
              <ReadonlyRuleChipPanel
                title="自动优势集"
                items={filteredPreprocessAutoCandidateRuleNames}
                emptyText="当前搜索条件下没有匹配的自动优势策略。"
              />
              <ReadonlyRuleChipPanel
                title="当前优势集"
                items={filteredCurrentAdvantageRuleNames}
                emptyText="当前优势集为空。"
              />
              <ReadonlyRuleChipPanel
                title="当前噪音伴随集"
                items={filteredCurrentCompanionRuleNames}
                emptyText="当前没有噪音伴随集。"
                tone="neutral"
              />
            </div>
          )}
        </section>

        <section className="stock-pick-advanced-step">
          <div className="stock-pick-concept-head">
            <strong>3. 执行高级选股</strong>
            <span>生成选股结果</span>
          </div>

          <div className="stock-pick-form-grid stock-pick-form-grid-advanced-run">
            <label className="stock-pick-field">
              <span>评分日期</span>
              <select
                value={tradeDate}
                onChange={(event) => setTradeDate(event.target.value)}
                disabled={optionsLoading}
              >
                {scoreTradeDateOptions.map((item) => (
                  <option key={item} value={item}>
                    {formatDateLabel(item)}
                  </option>
                ))}
              </select>
            </label>

            <label className="stock-pick-field">
              <span>选股方法</span>
              <select
                value={methodKey}
                onChange={(event) =>
                  setMethodKey(normalizeMethodKey(event.target.value))
                }
              >
                {METHOD_OPTIONS.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>

            <label className="stock-pick-field">
              <span>TopN</span>
              <input
                value={topLimit}
                onChange={(event) => setTopLimit(event.target.value)}
              />
            </label>

            <label className="stock-pick-field">
              <span>排名上限</span>
              <input
                value={rankMax}
                onChange={(event) => setRankMax(event.target.value)}
              />
            </label>

            <label className="stock-pick-field">
              <span>优势命中门槛</span>
              <select
                value={minAdvHits}
                onChange={(event) => setMinAdvHits(event.target.value)}
              >
                <option value="1">1</option>
                <option value="2">2</option>
              </select>
            </label>

            <label className="stock-pick-field">
              <span>板块</span>
              <select
                value={board}
                onChange={(event) =>
                  setBoard(
                    event.target.value as (typeof STOCK_PICK_BOARD_OPTIONS)[number],
                  )
                }
              >
                {STOCK_PICK_BOARD_OPTIONS.map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>
            </label>

            <label className="stock-pick-field">
              <span>地域</span>
              <select value={area} onChange={(event) => setArea(event.target.value)}>
                <option value="全部">全部</option>
                {areaOptions.map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>
            </label>

            <label className="stock-pick-field">
              <span>行业</span>
              <select
                value={industry}
                onChange={(event) => setIndustry(event.target.value)}
              >
                <option value="全部">全部</option>
                {industryOptions.map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>
            </label>

            <label className="stock-pick-field">
              <span>概念筛选</span>
              <label className="stock-pick-checkbox-box">
                <input
                  type="checkbox"
                  checked={enableConceptFilter}
                  onChange={(event) => setEnableConceptFilter(event.target.checked)}
                />
                <span>启用概念筛选</span>
              </label>
            </label>

            <label className="stock-pick-field">
              <span>概念匹配</span>
              <select
                value={conceptMatchMode}
                onChange={(event) => setConceptMatchMode(event.target.value)}
                disabled={!enableConceptFilter}
              >
                <option value="OR">任一命中</option>
                <option value="AND">全部命中</option>
              </select>
            </label>
          </div>

          <div className="stock-pick-advanced-callout">
            {methodHint(methodKey)}
          </div>

          {methodKey === "mixed_topn" ? (
            <div className="stock-pick-concept-panel stock-pick-advanced-panel">
              <div className="stock-pick-concept-head">
                <strong>混合排序键顺序</strong>
                <span>支持自定义先后顺序</span>
              </div>
              <div className="stock-pick-advanced-sort-builder">
                <div className="stock-pick-advanced-sort-list">
                  {normalizedMixedSortKeys.map((key, index) => (
                    <div className="stock-pick-advanced-sort-item" key={key}>
                      <span className="stock-pick-advanced-sort-rank">
                        {index + 1}
                      </span>
                      <strong>{mixedSortKeyLabel(key)}</strong>
                      <div className="stock-pick-advanced-sort-actions">
                        <button
                          type="button"
                          className="stock-pick-chip-btn"
                          onClick={() => moveMixedSortKey(key, -1)}
                          disabled={index === 0}
                        >
                          前移
                        </button>
                        <button
                          type="button"
                          className="stock-pick-chip-btn"
                          onClick={() => moveMixedSortKey(key, 1)}
                          disabled={index === normalizedMixedSortKeys.length - 1}
                        >
                          后移
                        </button>
                        <button
                          type="button"
                          className="stock-pick-chip-btn"
                          onClick={() => removeMixedSortKey(key)}
                          disabled={normalizedMixedSortKeys.length <= 1}
                        >
                          移除
                        </button>
                      </div>
                    </div>
                  ))}
                </div>
                <div className="stock-pick-advanced-sort-palette">
                  <span className="stock-pick-note">
                    可加入排序键：先比较第 1 项，再比较第 2 项，最后再比较后续项。
                  </span>
                  <div className="stock-pick-concept-list stock-pick-concept-list-inline">
                    {MIXED_SORT_KEY_OPTIONS.filter(
                      (option) => !normalizedMixedSortKeys.includes(option.value),
                    ).map((option) => (
                      <button
                        key={option.value}
                        type="button"
                        className="stock-pick-chip-btn"
                        onClick={() => addMixedSortKey(option.value)}
                      >
                        加入 {option.label}
                      </button>
                    ))}
                  </div>
                </div>
              </div>
            </div>
          ) : null}

          {enableConceptFilter ? (
            <ConceptIncludeExcludePanels
              includeConcepts={includeConcepts}
              excludeConcepts={excludeConcepts}
              availableConceptOptions={availableConceptOptions}
              keyword={conceptKeyword}
              onKeywordChange={setConceptKeyword}
              onToggleInclude={toggleIncludeConcept}
              onToggleExclude={toggleExcludeConcept}
              onClearInclude={() => setIncludeConcepts([])}
              onClearExclude={() => setExcludeConcepts([])}
              panelClassName="stock-pick-advanced-panel"
            />
          ) : null}

          <div className="stock-pick-actions stock-pick-actions-split">
            <button
              type="button"
              className="stock-pick-primary-btn"
              onClick={() => void onRun()}
              disabled={loading || optionsLoading || preprocessLoading}
            >
              {loading ? "选股中..." : "执行高级选股"}
            </button>
            <span className="stock-pick-tip">
              {!preprocessData
                ? "请先去策略回测页运行，再回来读取缓存。"
                : preprocessDirty
                  ? "当前参数和缓存不一致；请先去策略回测页按同参数重跑。"
                  : resultDirty
                    ? "当前结果未同步最近一次改动，执行后会刷新结果。"
                    : "当前结果已和最近一次参数保持一致。"}
            </span>
          </div>
        </section>
      </div>

      {preprocessError ? (
        <div className="stock-pick-message stock-pick-message-error">{preprocessError}</div>
      ) : null}
      {error ? <div className="stock-pick-message stock-pick-message-error">{error}</div> : null}

      {result ? (
        <>
          <div className="stock-pick-advanced-summary-grid">
            <div className="stock-pick-advanced-summary-item">
              <span>方法</span>
              <strong>{result.resolved_method_label}</strong>
            </div>
            <div className="stock-pick-advanced-summary-item">
              <span>评分日期</span>
              <strong>{formatDateLabel(result.resolved_trade_date)}</strong>
            </div>
            <div className="stock-pick-advanced-summary-item">
              <span>筛选后候选</span>
              <strong>{formatNumber(result.total_candidate_count, 0)}</strong>
            </div>
            <div className="stock-pick-advanced-summary-item">
              <span>入池候选</span>
              <strong>{formatNumber(result.eligible_candidate_count, 0)}</strong>
            </div>
            <div className="stock-pick-advanced-summary-item">
              <span>最终入选</span>
              <strong>{formatNumber(result.selected_count, 0)}</strong>
            </div>
            <div className="stock-pick-advanced-summary-item">
              <span>优势规则数</span>
              <strong>{formatNumber(result.resolved_advantage_rule_names.length, 0)}</strong>
            </div>
          </div>

          <div className="stock-pick-advanced-chip-grid">
            <div className="stock-pick-advanced-chip-card">
              <strong>当前优势集</strong>
              <div className="stock-pick-concept-list stock-pick-concept-list-inline">
                {result.resolved_advantage_rule_names.length > 0 ? (
                  result.resolved_advantage_rule_names.map((item) => (
                    <span className="stock-pick-chip-btn is-active" key={item}>
                      {item}
                    </span>
                  ))
                ) : (
                  <span className="stock-pick-note">当前优势集为空。</span>
                )}
              </div>
            </div>
            <div className="stock-pick-advanced-chip-card">
              <strong>噪音伴随集</strong>
              <div className="stock-pick-concept-list stock-pick-concept-list-inline">
                {result.resolved_noisy_companion_rule_names.length > 0 ? (
                  result.resolved_noisy_companion_rule_names.map((item) => (
                    <span className="stock-pick-chip-btn is-neutral" key={item}>
                      {item}
                    </span>
                  ))
                ) : (
                  <span className="stock-pick-note">当前未设置噪音伴随。</span>
                )}
              </div>
            </div>
          </div>

          <div className="stock-pick-result-head">
            <strong>结果列表</strong>
            <span>
              共 {result.rows.length} 只，评分日期：
              {formatDateLabel(result.resolved_trade_date)}
            </span>
          </div>
          <AdvancedStockPickTable
            rows={result.rows}
            tradeDate={result.resolved_trade_date ?? undefined}
            sourcePath={sourcePath}
          />
        </>
      ) : (
        <div className="stock-pick-empty">尚未执行高级选股。</div>
      )}
    </section>
  );
}
