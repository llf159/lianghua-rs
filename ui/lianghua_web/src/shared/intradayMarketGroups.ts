import type { AllMarketMonitorRow } from "../apis/reader";

export type MarketGroup = {
  name: string;
  kind: "concept" | "industry";
  rows: AllMarketMonitorRow[];
  change: number;
  weightedChange: number | null;
  median: number;
  rising: number;
  amount: number;
  leader: AllMarketMonitorRow;
};

export function numeric(value: number | null | undefined, digits = 2) {
  return value == null || !Number.isFinite(value)
    ? "—"
    : value.toLocaleString("zh-CN", {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits,
      });
}

export function percent(value: number | null | undefined) {
  return value == null || !Number.isFinite(value)
    ? "—"
    : `${value > 0 ? "+" : ""}${numeric(value)}%`;
}

export function amount(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return "—";
  return Math.abs(value) >= 100_000_000
    ? `${numeric(value / 100_000_000)}亿`
    : `${numeric(value / 10_000, 0)}万`;
}

export function tone(value: number | null | undefined) {
  return value == null || !Number.isFinite(value) || value === 0
    ? ""
    : value > 0
      ? "is-up"
      : "is-down";
}

export function summarizeGroups(
  rows: AllMarketMonitorRow[],
  kind: MarketGroup["kind"],
  namesForRow: (row: AllMarketMonitorRow) => string[],
) {
  const groups = new Map<string, AllMarketMonitorRow[]>();
  for (const row of rows) {
    if (
      row.realtime_change_pct == null ||
      !Number.isFinite(row.realtime_change_pct)
    )
      continue;
    for (const name of new Set(namesForRow(row).filter(Boolean))) {
      const members = groups.get(name) ?? [];
      members.push(row);
      groups.set(name, members);
    }
  }
  return Array.from(groups, ([name, members]): MarketGroup => {
    const changes = members
      .map((row) => row.realtime_change_pct!)
      .sort((a, b) => a - b);
    const middle = Math.floor(changes.length / 2);
    let totalMv = 0;
    let weightedChangeSum = 0;
    for (const row of members) {
      if (
        row.total_mv_yi != null &&
        Number.isFinite(row.total_mv_yi) &&
        row.total_mv_yi > 0
      ) {
        totalMv += row.total_mv_yi;
        weightedChangeSum += row.realtime_change_pct! * row.total_mv_yi;
      }
    }
    return {
      name,
      kind,
      rows: members,
      change: changes.reduce((sum, value) => sum + value, 0) / changes.length,
      weightedChange: totalMv > 0 ? weightedChangeSum / totalMv : null,
      median:
        changes.length % 2
          ? changes[middle]
          : (changes[middle - 1] + changes[middle]) / 2,
      rising: changes.filter((value) => value > 0).length,
      amount: members.reduce((sum, row) => sum + (row.realtime_amount ?? 0), 0),
      leader: members.reduce((best, row) =>
        row.realtime_change_pct! > best.realtime_change_pct! ? row : best,
      ),
    };
  }).sort(
    (a, b) => b.change - a.change || a.name.localeCompare(b.name, "zh-CN"),
  );
}
