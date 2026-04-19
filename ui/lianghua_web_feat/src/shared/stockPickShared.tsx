import { memo, useMemo, useState } from "react";
import { type StockPickRow } from "../apis/stockPick";
import DetailsLink from "./DetailsLink";
import { useRouteScrollRegion } from "./routeScroll";
import {
  formatConceptText,
  filterBoardItems,
  filterConceptItems,
  useConceptExclusions,
} from "./conceptExclusions";
import {
  TableSortButton,
  getAriaSort,
  type SortDefinition,
  type SortDirection,
  getNextSortState,
  sortRows,
} from "./tableSort";

export const STOCK_PICK_BOARD_OPTIONS = [
  "全部",
  "主板",
  "创业/科创",
  "北交所",
  "ST",
] as const;
export const STOCK_PICK_SCOPE_OPTIONS = [
  "LAST",
  "ANY",
  "EACH",
  "RECENT",
  "CONSEC",
] as const;
export const STOCK_PICK_MATCH_MODE_OPTIONS = ["OR", "AND"] as const;

type ConceptSelectionTone = "primary" | "warn" | "neutral";

function toneClassName(tone: ConceptSelectionTone) {
  if (tone === "warn") {
    return "stock-pick-chip-btn is-warn";
  }
  if (tone === "neutral") {
    return "stock-pick-chip-btn is-neutral";
  }
  return "stock-pick-chip-btn is-active";
}

export function normalizeStringArray(values: readonly string[]) {
  return filterConceptItems(values, []);
}

export function buildAvailableConceptOptions(
  conceptOptions: readonly string[],
  excludedConcepts: readonly string[],
) {
  return filterConceptItems(conceptOptions, excludedConcepts);
}

export function toggleStringSelection(values: readonly string[], value: string) {
  return values.includes(value)
    ? values.filter((item) => item !== value)
    : [...values, value];
}

function ConceptFilterPanel({
  title,
  selectedItems,
  availableItems,
  onToggle,
  onClear,
  keyword,
  onKeywordChange,
  activeTone = "primary",
  clearLabel = "清空",
  searchPlaceholder = "搜索概念",
  emptyText = "没有匹配的概念。",
  panelClassName,
}: {
  title: string;
  selectedItems: string[];
  availableItems: string[];
  onToggle: (value: string) => void;
  onClear: () => void;
  keyword: string;
  onKeywordChange: (value: string) => void;
  activeTone?: ConceptSelectionTone;
  clearLabel?: string;
  searchPlaceholder?: string;
  emptyText?: string;
  panelClassName?: string;
}) {
  const filteredItems = useMemo(() => {
    const needle = keyword.trim().toLowerCase();
    if (!needle) {
      return availableItems;
    }
    return availableItems.filter((item) => item.toLowerCase().includes(needle));
  }, [availableItems, keyword]);

  return (
    <div
      className={
        panelClassName
          ? `stock-pick-concept-panel ${panelClassName}`
          : "stock-pick-concept-panel"
      }
    >
      <div className="stock-pick-concept-head">
        <strong>{title}</strong>
        <span>已选 {selectedItems.length} 项</span>
      </div>
      <div className="stock-pick-concept-toolbar">
        <input
          type="text"
          value={keyword}
          onChange={(event) => onKeywordChange(event.target.value)}
          placeholder={searchPlaceholder}
          className="stock-pick-concept-search"
        />
        <button
          type="button"
          className="stock-pick-chip-btn"
          onClick={() => onKeywordChange("")}
          disabled={!keyword.trim()}
        >
          清空搜索
        </button>
        <button
          type="button"
          className="stock-pick-chip-btn"
          onClick={onClear}
          disabled={selectedItems.length === 0}
        >
          {clearLabel}
        </button>
      </div>
      <div className="stock-pick-concept-list">
        {filteredItems.length > 0 ? (
          filteredItems.map((item) => {
            const active = selectedItems.includes(item);
            return (
              <button
                key={item}
                type="button"
                className={
                  active ? toneClassName(activeTone) : "stock-pick-chip-btn"
                }
                onClick={() => onToggle(item)}
              >
                {item}
              </button>
            );
          })
        ) : (
          <span className="stock-pick-note">{emptyText}</span>
        )}
      </div>
    </div>
  );
}

export function ConceptIncludeExcludePanels({
  includeConcepts,
  excludeConcepts,
  availableConceptOptions,
  keyword,
  onKeywordChange,
  onToggleInclude,
  onToggleExclude,
  onClearInclude,
  onClearExclude,
  panelClassName,
}: {
  includeConcepts: string[];
  excludeConcepts: string[];
  availableConceptOptions: string[];
  keyword: string;
  onKeywordChange: (value: string) => void;
  onToggleInclude: (value: string) => void;
  onToggleExclude: (value: string) => void;
  onClearInclude: () => void;
  onClearExclude: () => void;
  panelClassName?: string;
}) {
  return (
    <div className="stock-pick-concept-grid">
      <ConceptFilterPanel
        title="包含概念"
        selectedItems={includeConcepts}
        availableItems={availableConceptOptions}
        onToggle={onToggleInclude}
        onClear={onClearInclude}
        keyword={keyword}
        onKeywordChange={onKeywordChange}
        clearLabel="清空包含"
        panelClassName={panelClassName}
      />
      <ConceptFilterPanel
        title="排除概念"
        selectedItems={excludeConcepts}
        availableItems={availableConceptOptions}
        onToggle={onToggleExclude}
        onClear={onClearExclude}
        keyword={keyword}
        onKeywordChange={onKeywordChange}
        activeTone="warn"
        clearLabel="清空排除"
        panelClassName={panelClassName}
      />
    </div>
  );
}

export function ConceptSinglePanel({
  title,
  selectedItems,
  availableItems,
  keyword,
  onKeywordChange,
  onToggle,
  onClear,
  clearLabel = "清空",
  searchPlaceholder = "搜索",
  emptyText = "没有匹配项。",
  panelClassName,
  noGrid = false,
}: {
  title: string;
  selectedItems: string[];
  availableItems: string[];
  keyword: string;
  onKeywordChange: (value: string) => void;
  onToggle: (value: string) => void;
  onClear: () => void;
  clearLabel?: string;
  searchPlaceholder?: string;
  emptyText?: string;
  panelClassName?: string;
  noGrid?: boolean;
}) {
  const panel = (
    <ConceptFilterPanel
      title={title}
      selectedItems={selectedItems}
      availableItems={availableItems}
      onToggle={onToggle}
      onClear={onClear}
      keyword={keyword}
      onKeywordChange={onKeywordChange}
      clearLabel={clearLabel}
      searchPlaceholder={searchPlaceholder}
      emptyText={emptyText}
      panelClassName={panelClassName}
    />
  );

  if (noGrid) {
    return panel;
  }

  return <div className="stock-pick-concept-grid">{panel}</div>;
}

export function buildBoardFilterOptions(
  options: readonly (typeof STOCK_PICK_BOARD_OPTIONS)[number][],
  excludeStBoard: boolean,
) {
  return filterBoardItems(options, excludeStBoard) as (typeof STOCK_PICK_BOARD_OPTIONS)[number][];
}

export function formatDateLabel(value?: string | null) {
  if (!value || value.length !== 8) {
    return "--";
  }
  return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`;
}

export function formatNumber(value?: number | null, digits = 2) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  return Number.isInteger(value) ? String(value) : value.toFixed(digits);
}

export const StockPickResultTable = memo(function StockPickResultTable({
  rows,
  tradeDate,
  sourcePath,
}: {
  rows: StockPickRow[];
  tradeDate?: string;
  sourcePath?: string;
}) {
  const { excludedConcepts } = useConceptExclusions();
  const [sortKey, setSortKey] = useState<string | null>(null);
  const [sortDirection, setSortDirection] = useState<SortDirection>(null);
  const sortDefinitions = useMemo(
    () =>
      ({
        rank: { value: (row) => row.rank },
        total_score: { value: (row) => row.total_score },
        board: { value: (row) => row.board },
      }) satisfies Partial<Record<string, SortDefinition<StockPickRow>>>,
    [],
  );
  const sortedRows = useMemo(
    () => sortRows(rows, sortKey, sortDirection, sortDefinitions),
    [rows, sortDefinitions, sortDirection, sortKey],
  );
  const tableWrapRef = useRouteScrollRegion<HTMLDivElement>(
    "stock-pick-result-table",
    [sortedRows.length, tradeDate],
  );
  const navigationItems = useMemo(
    () =>
      sortedRows.map((row) => ({
        tsCode: row.ts_code,
        tradeDate: tradeDate ?? null,
        sourcePath: sourcePath?.trim() || undefined,
        name: row.name ?? undefined,
      })),
    [sortedRows, sourcePath, tradeDate],
  );

  if (rows.length === 0) {
    return <div className="stock-pick-empty">当前条件下没有选出股票。</div>;
  }

  function onToggleSort(nextKey: string) {
    const nextState = getNextSortState(sortKey, sortDirection, nextKey);
    setSortKey(nextState.key);
    setSortDirection(nextState.direction);
  }

  return (
    <div className="stock-pick-table-wrap" ref={tableWrapRef}>
      <table className="stock-pick-table">
        <thead>
          <tr>
            <th aria-sort={getAriaSort(sortKey === "rank", sortDirection)}>
              <TableSortButton
                label="排名"
                isActive={sortKey === "rank"}
                direction={sortDirection}
                onClick={() => onToggleSort("rank")}
                title="按排名排序"
              />
            </th>
            <th>代码</th>
            <th>名称</th>
            <th
              aria-sort={getAriaSort(sortKey === "total_score", sortDirection)}
            >
              <TableSortButton
                label="总分"
                isActive={sortKey === "total_score"}
                direction={sortDirection}
                onClick={() => onToggleSort("total_score")}
                title="按总分排序"
              />
            </th>
            <th aria-sort={getAriaSort(sortKey === "board", sortDirection)}>
              <TableSortButton
                label="板块"
                isActive={sortKey === "board"}
                direction={sortDirection}
                onClick={() => onToggleSort("board")}
                title="按板块排序"
              />
            </th>
            <th>命中说明</th>
            <th>概念</th>
          </tr>
        </thead>
        <tbody>
          {sortedRows.map((row) => {
            const conceptText = formatConceptText(
              row.concept,
              excludedConcepts,
            );
            return (
              <tr key={`${tradeDate ?? ""}-${row.ts_code}`}>
                <td>{formatNumber(row.rank, 0)}</td>
                <td>{row.ts_code}</td>
                <td>
                  <DetailsLink
                    className="stock-pick-link-btn"
                    tsCode={row.ts_code}
                    tradeDate={tradeDate ?? null}
                    sourcePath={sourcePath?.trim() || undefined}
                    title={`查看 ${row.name ?? row.ts_code} 详情`}
                    navigationItems={navigationItems}
                  >
                    {row.name ?? row.ts_code}
                  </DetailsLink>
                </td>
                <td>{formatNumber(row.total_score)}</td>
                <td>{row.board}</td>
                <td className="stock-pick-cell-concept">{row.pick_note}</td>
                <td className="stock-pick-cell-concept">{conceptText}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
});
