import { useMemo, useState, type ReactNode } from "react";

export type SortDirection = "desc" | "asc" | null;

export type SortState<Key extends string> = {
  key: Key | null;
  direction: SortDirection;
};

export type SortDefinition<Row> = {
  value?: (row: Row) => unknown;
  compare?: (left: Row, right: Row) => number;
};

function parseSortableDate(value: string) {
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }

  const compactDigits = trimmed.replace(/[^0-9]/g, "");
  if (/^\d{8}$/.test(compactDigits)) {
    return Number(compactDigits);
  }

  const timestamp = Date.parse(trimmed);
  return Number.isFinite(timestamp) ? timestamp : null;
}

function parseSortableNumber(value: string) {
  const trimmed = value.trim();
  if (!/^[+-]?\d+(?:\.\d+)?$/.test(trimmed)) {
    return null;
  }

  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeSortValue(value: unknown): number | string | boolean | null {
  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }

  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed || trimmed === "--") {
      return null;
    }

    const dateValue = parseSortableDate(trimmed);
    if (dateValue !== null) {
      return dateValue;
    }

    const numberValue = parseSortableNumber(trimmed);
    if (numberValue !== null) {
      return numberValue;
    }

    return trimmed;
  }

  return String(value);
}

function compareNormalizedValues(
  left: number | string | boolean | null,
  right: number | string | boolean | null,
) {
  if (typeof left === "number" && typeof right === "number") {
    return left - right;
  }

  if (typeof left === "boolean" && typeof right === "boolean") {
    return Number(left) - Number(right);
  }

  return String(left).localeCompare(String(right), "zh-CN", {
    numeric: true,
    sensitivity: "base",
  });
}

export function compareNullableNumbers(
  left: number | null | undefined,
  right: number | null | undefined,
) {
  const normalizedLeft =
    typeof left === "number" && Number.isFinite(left) ? left : null;
  const normalizedRight =
    typeof right === "number" && Number.isFinite(right) ? right : null;
  return compareNormalizedValues(normalizedLeft, normalizedRight);
}

export function sortRows<Row, Key extends string>(
  rows: Row[],
  sortKey: Key | null,
  sortDirection: SortDirection,
  definitions: Partial<Record<Key, SortDefinition<Row>>>,
) {
  if (!sortKey || !sortDirection) {
    return rows;
  }

  const definition = definitions[sortKey];

  return rows
    .map((row, index) => ({ row, index }))
    .sort((left, right) => {
      if (definition?.compare) {
        const result = definition.compare(left.row, right.row);
        if (result === 0) {
          return left.index - right.index;
        }
        return sortDirection === "desc" ? -result : result;
      }

      const leftValue = normalizeSortValue(definition?.value?.(left.row));
      const rightValue = normalizeSortValue(definition?.value?.(right.row));

      if (leftValue === null && rightValue === null) {
        return left.index - right.index;
      }
      if (leftValue === null) {
        return 1;
      }
      if (rightValue === null) {
        return -1;
      }

      const result = compareNormalizedValues(leftValue, rightValue);

      if (result === 0) {
        return left.index - right.index;
      }

      return sortDirection === "desc" ? -result : result;
    })
    .map(({ row }) => row);
}

export function getNextSortState<Key extends string>(
  currentKey: Key | null,
  currentDirection: SortDirection,
  nextKey: Key,
): SortState<Key> {
  if (currentKey !== nextKey) {
    return {
      key: nextKey,
      direction: "desc",
    };
  }

  if (currentDirection === "desc") {
    return {
      key: nextKey,
      direction: "asc",
    };
  }

  if (currentDirection === "asc") {
    return {
      key: null,
      direction: null,
    };
  }

  return {
    key: nextKey,
    direction: "desc",
  };
}

export function getSortIndicator(direction: SortDirection) {
  if (direction === "desc") {
    return "↓";
  }
  if (direction === "asc") {
    return "↑";
  }
  return "";
}

export function getAriaSort(
  isActive: boolean,
  direction: SortDirection,
): "ascending" | "descending" | "none" {
  if (!isActive || !direction) {
    return "none";
  }
  return direction === "asc" ? "ascending" : "descending";
}

export function useTableSort<Row, Key extends string>(
  rows: Row[],
  definitions: Partial<Record<Key, SortDefinition<Row>>>,
  initialState?: SortState<Key>,
) {
  const [sortKey, setSortKey] = useState<Key | null>(initialState?.key ?? null);
  const [sortDirection, setSortDirection] = useState<SortDirection>(
    initialState?.direction ?? null,
  );

  const sortedRows = useMemo(
    () => sortRows(rows, sortKey, sortDirection, definitions),
    [definitions, rows, sortDirection, sortKey],
  );

  function toggleSort(nextKey: Key) {
    const nextState = getNextSortState(sortKey, sortDirection, nextKey);
    setSortKey(nextState.key);
    setSortDirection(nextState.direction);
  }

  return {
    sortKey,
    sortDirection,
    sortedRows,
    setSortKey,
    setSortDirection,
    toggleSort,
  };
}

export function TableSortButton({
  label,
  isActive,
  direction,
  onClick,
  title,
}: {
  label: ReactNode;
  isActive: boolean;
  direction: SortDirection;
  onClick: () => void;
  title?: string;
}) {
  const indicator = isActive ? getSortIndicator(direction) : "";

  return (
    <button
      className={["table-sort-button", isActive ? "is-active" : ""]
        .filter(Boolean)
        .join(" ")}
      type="button"
      onClick={onClick}
      title={title}
    >
      <span>{label}</span>
      {indicator ? (
        <span className="table-sort-indicator">{indicator}</span>
      ) : null}
    </button>
  );
}
