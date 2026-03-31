import { useMemo, useState } from "react";
import { type StockPickRow } from "../../apis/stockPick";
import DetailsLink from "../../shared/DetailsLink";
import { useRouteScrollRegion } from "../../shared/routeScroll";
import {
  formatConceptText,
  useConceptExclusions,
} from "../../shared/conceptExclusions";
import {
  TableSortButton,
  getAriaSort,
  type SortDefinition,
  type SortDirection,
  getNextSortState,
  sortRows,
} from "../../shared/tableSort";

export const STOCK_PICK_BOARD_OPTIONS = [
  "全部",
  "主板",
  "创业/科创",
  "北交所",
  "ST",
  "*ST",
] as const;
export const STOCK_PICK_SCOPE_OPTIONS = [
  "LAST",
  "ANY",
  "EACH",
  "RECENT",
  "CONSEC",
] as const;
export const STOCK_PICK_MATCH_MODE_OPTIONS = ["OR", "AND"] as const;

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

export function StockPickResultTable({
  rows,
  tradeDate,
}: {
  rows: StockPickRow[];
  tradeDate?: string;
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
  const detailNavigationItems = useMemo(
    () =>
      sortedRows.map((row) => ({
        tsCode: row.ts_code,
        tradeDate: tradeDate || undefined,
        name: row.name ?? undefined,
      })),
    [sortedRows, tradeDate],
  );
  const tableWrapRef = useRouteScrollRegion<HTMLDivElement>(
    "stock-pick-result-table",
    [sortedRows.length, tradeDate],
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
                    tradeDate={tradeDate}
                    navigationItems={detailNavigationItems}
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
}
