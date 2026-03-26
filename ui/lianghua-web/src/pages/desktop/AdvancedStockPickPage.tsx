import { useEffect, useMemo, useState } from "react";
import {
  runAdvancedStockPick,
  type AdvancedStockPickResultData,
  type AdvancedStockPickRow,
} from "../../apis/stockPick";
import {
  getStrategyPerformancePage,
  type StrategyPerformancePageData,
  type StrategyPerformanceRuleRow,
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
import { useStockPickOutletContext } from "./StockPickPage";

const ADVANCED_STOCK_PICK_STATE_KEY = "advanced-stock-pick-state-v5";
const HORIZON_OPTIONS = [2, 3, 5, 10] as const;
const QUANTILE_OPTIONS = [0.8, 0.9, 0.95] as const;
const DEFAULT_AUTO_MIN_SAMPLES = {
  2: 5,
  3: 5,
  5: 10,
  10: 20,
} as const;
const MIXED_SORT_KEY_OPTIONS = [
  { value: "adv_hit_cnt", label: "优势命中数" },
  { value: "adv_score_sum", label: "优势得分和" },
  { value: "pos_hit_cnt", label: "正向命中数" },
  { value: "pos_score_sum", label: "正向得分和" },
  { value: "rank", label: "原始排名" },
] as const;
const METHOD_OPTIONS = [
  { value: "adv_score_topn", label: "优势得分优先" },
  { value: "adv_hit_topn", label: "优势命中优先" },
  { value: "mixed_topn", label: "混合排序" },
  { value: "companion_penalty_topn", label: "噪音惩罚" },
  { value: "advantage_pool", label: "优势池优先" },
  { value: "clean_adv_topn", label: "纯净优势池" },
  { value: "pos_score_topn", label: "正向得分优先" },
  { value: "pos_hit_topn", label: "正向命中优先" },
  { value: "raw_topn", label: "原始 TopN" },
] as const;

type AdvancedRowSortKey =
  | "rank"
  | "total_score"
  | "adv_hit_cnt"
  | "adv_score_sum"
  | "pos_hit_cnt"
  | "pos_score_sum"
  | "noisy_companion_cnt";

type PersistedAdvancedState = {
  tradeDate: string;
  board: (typeof STOCK_PICK_BOARD_OPTIONS)[number];
  area: string;
  industry: string;
  includeConcepts: string[];
  excludeConcepts: string[];
  conceptKeyword: string;
  conceptMatchMode: string;
  methodKey: string;
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
  rankMax: string;
  mixedSortKeys: string[];
  result: AdvancedStockPickResultData | null;
};

function parsePositiveInt(value: string, fallback: number) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
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

function formatPlainText(value?: string | null) {
  const trimmed = value?.trim();
  return trimmed ? trimmed : "--";
}

function hasPositiveHits(row: StrategyPerformanceRuleRow) {
  return (
    row.signal_direction === "positive" &&
    row.metrics.some((metric) => (metric.hit_n ?? 0) > 0)
  );
}

function methodHint(methodKey: string) {
  switch (methodKey) {
    case "mixed_topn":
      return "混合排序会按下方排序键做字典序比较，越靠左优先级越高。";
    case "companion_penalty_topn":
      return "噪音伴随按当前优势集识别。";
    case "clean_adv_topn":
      return "纯净优势池会优先保留没有噪音伴随的样本。";
    case "advantage_pool":
      return "优势池优先只保留 adv_hit_cnt 达标的样本，再在池内等权或排序。";
    default:
      return "按当前条件生成选股结果。";
  }
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
  tone?: "active" | "neutral";
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

function FilterChipList({
  title,
  selectedItems,
  availableItems,
  onToggle,
  keyword,
  onKeywordChange,
  activeTone = "primary",
}: {
  title: string;
  selectedItems: string[];
  availableItems: string[];
  onToggle: (value: string) => void;
  keyword: string;
  onKeywordChange: (value: string) => void;
  activeTone?: "primary" | "warn" | "neutral";
}) {
  const filteredItems = useMemo(() => {
    const needle = keyword.trim().toLowerCase();
    if (!needle) {
      return availableItems;
    }
    return availableItems.filter((item) => item.toLowerCase().includes(needle));
  }, [availableItems, keyword]);

  return (
    <div className="stock-pick-concept-panel stock-pick-advanced-panel">
      <div className="stock-pick-concept-head">
        <strong>{title}</strong>
        <span>已选 {selectedItems.length} 项</span>
      </div>
      <div className="stock-pick-concept-toolbar">
        <input
          type="text"
          value={keyword}
          onChange={(event) => onKeywordChange(event.target.value)}
          placeholder="搜索"
          className="stock-pick-concept-search"
        />
      </div>
      <div className="stock-pick-concept-list">
        {filteredItems.map((item) => {
          const active = selectedItems.includes(item);
          const toneClass =
            activeTone === "warn"
              ? "stock-pick-chip-btn is-warn"
              : activeTone === "neutral"
                ? "stock-pick-chip-btn is-neutral"
                : "stock-pick-chip-btn is-active";
          return (
            <button
              key={item}
              type="button"
              className={active ? toneClass : "stock-pick-chip-btn"}
              onClick={() => onToggle(item)}
            >
              {item}
            </button>
          );
        })}
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
  const useMigratedAutoMinSampleDefaults =
    hasLegacyAutoMinSampleStrings(persistedState);

  const [tradeDate, setTradeDate] = useState(
    () => persistedState?.tradeDate ?? "",
  );
  const [board, setBoard] = useState<(typeof STOCK_PICK_BOARD_OPTIONS)[number]>(
    () =>
      persistedState?.board &&
      STOCK_PICK_BOARD_OPTIONS.includes(persistedState.board)
        ? persistedState.board
        : "全部",
  );
  const [area, setArea] = useState(() => persistedState?.area ?? "全部");
  const [industry, setIndustry] = useState(
    () => persistedState?.industry ?? "全部",
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
  const [methodKey, setMethodKey] = useState(
    () => persistedState?.methodKey ?? "mixed_topn",
  );
  const [selectedHorizon, setSelectedHorizon] = useState(
    () => persistedState?.selectedHorizon ?? "10",
  );
  const [strongQuantile, setStrongQuantile] = useState(
    () => persistedState?.strongQuantile ?? "0.9",
  );
  const [manualRuleNames, setManualRuleNames] = useState<string[]>(
    () => persistedState?.manualRuleNames ?? [],
  );
  const [strategyKeyword, setStrategyKeyword] = useState(
    () => persistedState?.strategyKeyword ?? "",
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
  const [autoMinSamples10, setAutoMinSamples10] = useState(
    () =>
      useMigratedAutoMinSampleDefaults
        ? String(DEFAULT_AUTO_MIN_SAMPLES[10])
        : (persistedState?.autoMinSamples10 ?? String(DEFAULT_AUTO_MIN_SAMPLES[10])),
  );
  const [requireWinRateAboveMarket, setRequireWinRateAboveMarket] = useState(
    () => persistedState?.requireWinRateAboveMarket ?? false,
  );
  const [minPassHorizons, setMinPassHorizons] = useState(
    () => persistedState?.minPassHorizons ?? "2",
  );
  const [minAdvHits, setMinAdvHits] = useState(
    () => persistedState?.minAdvHits ?? "1",
  );
  const [topLimit, setTopLimit] = useState(
    () => persistedState?.topLimit ?? "100",
  );
  const [rankMax, setRankMax] = useState(
    () => persistedState?.rankMax ?? "100",
  );
  const [mixedSortKeys, setMixedSortKeys] = useState<string[]>(
    () =>
      persistedState?.mixedSortKeys?.length
        ? persistedState.mixedSortKeys
        : ["adv_hit_cnt", "adv_score_sum", "rank"],
  );
  const [result, setResult] = useState<AdvancedStockPickResultData | null>(
    () => persistedState?.result ?? null,
  );
  const [preprocessData, setPreprocessData] =
    useState<StrategyPerformancePageData | null>(null);
  const [preprocessLoading, setPreprocessLoading] = useState(false);
  const [preprocessError, setPreprocessError] = useState("");
  const [preprocessSignature, setPreprocessSignature] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [runSignature, setRunSignature] = useState("");

  const availableConceptOptions = useMemo(
    () => conceptOptions.filter((item) => !excludedConcepts.includes(item)),
    [conceptOptions, excludedConcepts],
  );
  const preprocessAutoCandidateRuleNames = useMemo(
    () => preprocessData?.auto_candidate_rule_names ?? [],
    [preprocessData],
  );
  const preprocessPositiveRuleNames = useMemo(
    () =>
      (preprocessData?.rule_rows ?? [])
        .filter(hasPositiveHits)
        .map((row) => row.rule_name),
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
    () =>
      normalizeStringArray(
        manualRuleNames.filter((item) =>
          preprocessPositiveRuleNames.includes(item),
        ),
      ),
    [manualRuleNames, preprocessPositiveRuleNames],
  );
  const currentCompanionRuleNames = useMemo(() => {
    const currentAdvantageSet = new Set(currentAdvantageRuleNames);
    return preprocessPositiveRuleNames.filter(
      (item) => !currentAdvantageSet.has(item),
    );
  }, [currentAdvantageRuleNames, preprocessPositiveRuleNames]);
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
  const normalizedMixedSortKeys = useMemo(
    () => normalizeStringArray(mixedSortKeys),
    [mixedSortKeys],
  );
  const preprocessConfigSignature = useMemo(
    () =>
      JSON.stringify({
        sourcePath: sourcePath.trim(),
        selectedHorizon: parsePositiveInt(selectedHorizon, 10),
        strongQuantile: Number(strongQuantile),
        autoMinSamples2: parsePositiveInt(autoMinSamples2, DEFAULT_AUTO_MIN_SAMPLES[2]),
        autoMinSamples3: parsePositiveInt(autoMinSamples3, DEFAULT_AUTO_MIN_SAMPLES[3]),
        autoMinSamples5: parsePositiveInt(autoMinSamples5, DEFAULT_AUTO_MIN_SAMPLES[5]),
        autoMinSamples10: parsePositiveInt(autoMinSamples10, DEFAULT_AUTO_MIN_SAMPLES[10]),
        requireWinRateAboveMarket,
        minPassHorizons: parsePositiveInt(minPassHorizons, 2),
      }),
    [
      autoMinSamples2,
      autoMinSamples3,
      autoMinSamples10,
      autoMinSamples5,
      minPassHorizons,
      requireWinRateAboveMarket,
      selectedHorizon,
      sourcePath,
      strongQuantile,
    ],
  );
  const runConfigSignature = useMemo(
    () =>
      JSON.stringify({
        preprocessConfigSignature,
        tradeDate,
        board,
        area,
        industry,
        includeConcepts: normalizedIncludeConcepts,
        excludeConcepts: normalizedExcludeConcepts,
        conceptMatchMode,
        methodKey,
        manualRuleNames: currentAdvantageRuleNames,
        minAdvHits: parsePositiveInt(minAdvHits, 1),
        topLimit: parsePositiveInt(topLimit, 100),
        rankMax: parsePositiveInt(rankMax, 100),
        mixedSortKeys: normalizedMixedSortKeys,
      }),
    [
      area,
      board,
      conceptMatchMode,
      currentAdvantageRuleNames,
      industry,
      methodKey,
      minAdvHits,
      normalizedExcludeConcepts,
      normalizedIncludeConcepts,
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
    try {
      window.sessionStorage.setItem(
        ADVANCED_STOCK_PICK_STATE_KEY,
        JSON.stringify({
          tradeDate,
          board,
          area,
          industry,
          includeConcepts,
          excludeConcepts,
          conceptKeyword,
          conceptMatchMode,
          methodKey,
          selectedHorizon,
          strongQuantile,
          manualRuleNames,
          strategyKeyword,
          autoMinSamples2,
          autoMinSamples3,
          autoMinSamples5,
          autoMinSamples10,
          requireWinRateAboveMarket,
          minPassHorizons,
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
    autoMinSamples2,
    autoMinSamples3,
    autoMinSamples10,
    autoMinSamples5,
    board,
    conceptKeyword,
    conceptMatchMode,
    excludeConcepts,
    includeConcepts,
    industry,
    manualRuleNames,
    methodKey,
    minAdvHits,
    minPassHorizons,
    mixedSortKeys,
    rankMax,
    requireWinRateAboveMarket,
    result,
    selectedHorizon,
    strategyKeyword,
    strongQuantile,
    topLimit,
    tradeDate,
  ]);

  function addMixedSortKey(key: string) {
    setMixedSortKeys((current) =>
      current.includes(key) ? current : [...current, key],
    );
  }

  function removeMixedSortKey(key: string) {
    setMixedSortKeys((current) => {
      if (!current.includes(key) || current.length <= 1) {
        return current;
      }
      return current.filter((item) => item !== key);
    });
  }

  function moveMixedSortKey(key: string, direction: -1 | 1) {
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
    setIncludeConcepts((current) =>
      current.includes(value)
        ? current.filter((item) => item !== value)
        : [...current, value],
    );
    setExcludeConcepts((current) => current.filter((item) => item !== value));
  }

  function toggleExcludeConcept(value: string) {
    setExcludeConcepts((current) =>
      current.includes(value)
        ? current.filter((item) => item !== value)
        : [...current, value],
    );
    setIncludeConcepts((current) => current.filter((item) => item !== value));
  }

  function moveRuleToAdvantage(value: string) {
    setManualRuleNames((current) =>
      current.includes(value) ? current : [...current, value],
    );
  }

  function moveRuleToCompanion(value: string) {
    setManualRuleNames((current) => current.filter((item) => item !== value));
  }

  async function preprocessAdvantageRules() {
    if (!sourcePath.trim()) {
      setPreprocessError("当前数据目录为空。");
      return;
    }
    setPreprocessLoading(true);
    setPreprocessError("");
    try {
      const pageData = await getStrategyPerformancePage({
        sourcePath,
        selectedHorizon: parsePositiveInt(selectedHorizon, 10),
        strongQuantile: Number(strongQuantile),
        advantageRuleMode: "auto",
        autoMinSamples2: parsePositiveInt(autoMinSamples2, DEFAULT_AUTO_MIN_SAMPLES[2]),
        autoMinSamples3: parsePositiveInt(autoMinSamples3, DEFAULT_AUTO_MIN_SAMPLES[3]),
        autoMinSamples5: parsePositiveInt(autoMinSamples5, DEFAULT_AUTO_MIN_SAMPLES[5]),
        autoMinSamples10: parsePositiveInt(autoMinSamples10, DEFAULT_AUTO_MIN_SAMPLES[10]),
        requireWinRateAboveMarket,
        minPassHorizons: parsePositiveInt(minPassHorizons, 2),
      });
      setPreprocessData(pageData);
      setManualRuleNames(pageData.auto_candidate_rule_names ?? []);
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
    if (!preprocessData) {
      setError("请先刷新优势集，再手工调整优势集和伴随集。");
      return;
    }
    if (preprocessDirty) {
      setError("策略口径已变更，请先重新刷新优势集。");
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
        includeConcepts: normalizedIncludeConcepts,
        excludeConcepts: normalizedExcludeConcepts,
        conceptMatchMode,
        methodKey,
        selectedHorizon: parsePositiveInt(selectedHorizon, 10),
        strongQuantile: Number(strongQuantile),
        advantageRuleMode: "manual",
        manualRuleNames: normalizeStringArray(currentAdvantageRuleNames),
        minAdvHits: parsePositiveInt(minAdvHits, 1),
        topLimit: parsePositiveInt(topLimit, 100),
        mixedSortKeys: normalizedMixedSortKeys,
        rankMax: parsePositiveInt(rankMax, 100),
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
          <p className="stock-pick-note">
            先刷新策略口径得到自动优势集，再手工核验当前优势/伴随集，最后叠加评分日、板块、概念和排序方法执行选股。板块、行业、地域、概念筛选不会影响上面的优势集识别。
          </p>
        </div>
      </div>

      <div className="stock-pick-advanced-flow">
        <section className="stock-pick-advanced-step">
          <div className="stock-pick-concept-head">
            <strong>1. 刷新优势集</strong>
            <span>更新优势/伴随集</span>
          </div>
          <div className="stock-pick-form-grid stock-pick-form-grid-advanced-core">
            <label className="stock-pick-field">
              <span>持有周期</span>
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

            <label className="stock-pick-field">
              <span>强势阈值</span>
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

            <label className="stock-pick-field">
              <span>至少通过周期</span>
              <select
                value={minPassHorizons}
                onChange={(event) => setMinPassHorizons(event.target.value)}
              >
                <option value="1">1</option>
                <option value="2">2</option>
                <option value="3">3</option>
                <option value="4">4</option>
              </select>
            </label>

            <label className="stock-pick-field stock-pick-checkbox-field stock-pick-field-compact">
              <span>自动候选附加条件</span>
              <label className="stock-pick-checkbox-box">
                <input
                  type="checkbox"
                  checked={requireWinRateAboveMarket}
                  onChange={(event) =>
                    setRequireWinRateAboveMarket(event.target.checked)
                  }
                />
                <span>要求胜率高于市场</span>
              </label>
            </label>
          </div>

          <div className="stock-pick-form-grid stock-pick-form-grid-advanced-min-sample">
            <label className="stock-pick-field stock-pick-field-compact">
              <span>2 日最小样本</span>
              <input
                value={autoMinSamples2}
                onChange={(event) => setAutoMinSamples2(event.target.value)}
              />
            </label>
            <label className="stock-pick-field stock-pick-field-compact">
              <span>3 日最小样本</span>
              <input
                value={autoMinSamples3}
                onChange={(event) => setAutoMinSamples3(event.target.value)}
              />
            </label>
            <label className="stock-pick-field stock-pick-field-compact">
              <span>5 日最小样本</span>
              <input
                value={autoMinSamples5}
                onChange={(event) => setAutoMinSamples5(event.target.value)}
              />
            </label>
            <label className="stock-pick-field stock-pick-field-compact">
              <span>10 日最小样本</span>
              <input
                value={autoMinSamples10}
                onChange={(event) => setAutoMinSamples10(event.target.value)}
              />
            </label>
          </div>

          <div className="stock-pick-actions stock-pick-actions-split">
            <button
              type="button"
              className="stock-pick-primary-btn"
              onClick={() => void preprocessAdvantageRules()}
              disabled={preprocessLoading || optionsLoading}
            >
              {preprocessLoading ? "刷新中..." : "刷新优势集"}
            </button>
            <span className="stock-pick-tip">
              这里只决定自动优势集怎么识别，不看板块、行业、地域和概念筛选。
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
                当前伴随 {currentCompanionRuleNames.length}
              </span>
              <span className="stock-pick-chip-btn is-neutral">
                持有周期 {selectedHorizon} 日
              </span>
            </div>
          ) : null}

          {preprocessDirty ? (
            <div className="stock-pick-advanced-callout is-warn">
              策略口径已变更，当前优势集已过期。请先重新刷新优势集，再执行后面的手工核验和高级选股。
            </div>
          ) : null}
        </section>

        <section className="stock-pick-advanced-step">
          <div className="stock-pick-concept-head">
            <strong>2. 手工核验优势 / 伴随集</strong>
            <span>只在刷新后的策略集合上做人工微调</span>
          </div>

          {!preprocessData ? (
            <div className="stock-pick-empty">
              先在上一步刷新优势集，拿到自动优势集后再手工调整。
            </div>
          ) : (
            <div className="stock-pick-advanced-stack">
              <ReadonlyRuleChipPanel
                title="自动优势集"
                items={filteredPreprocessAutoCandidateRuleNames}
                emptyText="当前搜索条件下没有匹配的自动优势策略。"
              />
              <div className="stock-pick-concept-panel stock-pick-advanced-panel">
                <div className="stock-pick-concept-head">
                  <strong>当前优势 / 伴随集</strong>
                  <span>点击规则可在两侧移动</span>
                </div>
                <div className="stock-pick-concept-toolbar">
                  <input
                    type="text"
                    value={strategyKeyword}
                    onChange={(event) => setStrategyKeyword(event.target.value)}
                    placeholder="搜索策略"
                    className="stock-pick-concept-search"
                  />
                  <button
                    type="button"
                    className="stock-pick-chip-btn"
                    onClick={() => setManualRuleNames(preprocessAutoCandidateRuleNames)}
                  >
                    恢复自动优势集
                  </button>
                </div>
                <div className="stock-pick-advanced-dual-grid">
                  <div className="stock-pick-advanced-chip-card">
                    <strong>当前优势集</strong>
                    <span className="stock-pick-note">点击移出到伴随集</span>
                    <div className="stock-pick-concept-list stock-pick-concept-list-inline">
                      {filteredCurrentAdvantageRuleNames.length > 0 ? (
                        filteredCurrentAdvantageRuleNames.map((item) => (
                          <button
                            key={`adv:${item}`}
                            type="button"
                            className="stock-pick-chip-btn is-active"
                            onClick={() => moveRuleToCompanion(item)}
                          >
                            {item}
                          </button>
                        ))
                      ) : (
                        <span className="stock-pick-note">当前优势集为空。</span>
                      )}
                    </div>
                  </div>
                  <div className="stock-pick-advanced-chip-card">
                    <strong>当前伴随集</strong>
                    <span className="stock-pick-note">点击纳入优势集</span>
                    <div className="stock-pick-concept-list stock-pick-concept-list-inline">
                      {filteredCurrentCompanionRuleNames.length > 0 ? (
                        filteredCurrentCompanionRuleNames.map((item) => (
                          <button
                            key={`companion:${item}`}
                            type="button"
                            className="stock-pick-chip-btn"
                            onClick={() => moveRuleToAdvantage(item)}
                          >
                            {item}
                          </button>
                        ))
                      ) : (
                        <span className="stock-pick-note">当前伴随集为空。</span>
                      )}
                    </div>
                  </div>
                </div>
              </div>
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
                onChange={(event) => setMethodKey(event.target.value)}
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
              <span>概念匹配</span>
              <select
                value={conceptMatchMode}
                onChange={(event) => setConceptMatchMode(event.target.value)}
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

          <div className="stock-pick-advanced-grid">
            <FilterChipList
              title="包含概念"
              selectedItems={includeConcepts}
              availableItems={availableConceptOptions}
              onToggle={toggleIncludeConcept}
              keyword={conceptKeyword}
              onKeywordChange={setConceptKeyword}
            />
            <FilterChipList
              title="排除概念"
              selectedItems={excludeConcepts}
              availableItems={availableConceptOptions}
              onToggle={toggleExcludeConcept}
              keyword={conceptKeyword}
              onKeywordChange={setConceptKeyword}
              activeTone="warn"
            />
          </div>

          <div className="stock-pick-actions stock-pick-actions-split">
            <button
              type="button"
              className="stock-pick-primary-btn"
              onClick={() => void onRun()}
              disabled={
                loading || optionsLoading || preprocessLoading || !preprocessData || preprocessDirty
              }
            >
              {loading ? "选股中..." : "执行高级选股"}
            </button>
            <span className="stock-pick-tip">
              {!preprocessData
                ? "先刷新优势集，再执行高级选股。"
                : preprocessDirty
                  ? "策略口径有更新，请先刷新优势集。"
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
              <span>优势池数量</span>
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
