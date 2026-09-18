import type {
  RuleExpressionValidationData,
  RuleLayerBacktestData,
  RuleValidationComboResult,
} from "../apis/strategyTrigger";
import {
  shouldHighlightBacktestMetric,
  type BacktestHighlightMetric,
  type BacktestHighlightSettings,
} from "./backtestHighlightSettings";
import { formatNumber } from "./backtestFormat";

export type ValidationDirection = "positive" | "negative";

export type ValidationScopeWayOption = "ANY" | "LAST" | "EACH" | "RECENT" | "CONSEC";

export type ValidationUnknownConfigDraft = {
  name: string;
  start: string;
  end: string;
  step: string;
};

export type ExpressionValidationContext = {
  sourcePath: string;
  expression: string;
  direction: ValidationDirection;
  importRuleName: string;
  importRuleExplain: string;
  sampleLimitPerGroup: number;
};

export const VALIDATION_DEFAULT_SAMPLE_LIMIT = 5;
export const VALIDATION_MAX_SAMPLE_LIMIT = 200;

export const VALIDATION_SCOPE_WAY_OPTIONS: Array<{ value: ValidationScopeWayOption; label: string }> = [
  { value: "ANY", label: "ANY" },
  { value: "LAST", label: "LAST" },
  { value: "EACH", label: "EACH" },
  { value: "RECENT", label: "RECENT" },
  { value: "CONSEC", label: "CONSEC" },
];

const BASE_SERIES_IDENTIFIERS = new Set([
  "O",
  "H",
  "L",
  "C",
  "V",
  "AMOUNT",
  "PRE_CLOSE",
  "CHANGE",
  "PCT_CHG",
  "ZHANG",
]);

const RESERVED_BOOLEAN_IDENTIFIERS = new Set(["AND", "OR", "NOT", "TRUE", "FALSE"]);

function readNextNonSpaceChar(expression: string, from: number): string {
  for (let index = from; index < expression.length; index += 1) {
    const ch = expression[index];
    if (!/\s/.test(ch)) {
      return ch;
    }
  }
  return "";
}

export function buildEmptyUnknownConfig(): ValidationUnknownConfigDraft {
  return {
    name: "",
    start: "",
    end: "",
    step: "",
  };
}

export function hasValidUnknownConfig(configs: ValidationUnknownConfigDraft[]): boolean {
  return configs.some((item) => item.name.trim().length > 0);
}

export function inferUnknownConfigs(expression: string): ValidationUnknownConfigDraft[] {
  const assigned = new Set<string>();
  for (const match of expression.matchAll(/\b([A-Za-z_][A-Za-z0-9_]*)\s*:=/g)) {
    const name = match[1]?.trim();
    if (!name) {
      continue;
    }
    assigned.add(name.toUpperCase());
  }

  const found = new Set<string>();
  const tokenRegExp = /\b([A-Za-z_][A-Za-z0-9_]*)\b/g;
  for (const match of expression.matchAll(tokenRegExp)) {
    const token = match[1]?.trim();
    const full = match[0];
    const matchStart = match.index;
    if (!token) {
      continue;
    }
    if (matchStart === undefined) {
      continue;
    }
    const upper = token.toUpperCase();

    if (
      RESERVED_BOOLEAN_IDENTIFIERS.has(upper) ||
      BASE_SERIES_IDENTIFIERS.has(upper) ||
      /^(?:I|ISZ|I300|I500|ICY|I50|I1000)(?:_[A-Z][A-Z0-9_]*)?$/.test(upper) ||
      assigned.has(upper)
    ) {
      continue;
    }

    const nextNonSpaceChar = readNextNonSpaceChar(expression, matchStart + full.length);
    const isFunctionCall = nextNonSpaceChar === "(";
    if (isFunctionCall) {
      continue;
    }

    found.add(token);
  }

  const names = Array.from(found).sort((left, right) => left.localeCompare(right));
  if (names.length === 0) {
    return [buildEmptyUnknownConfig()];
  }

  return names.map((name) => ({
    name,
    start: "",
    end: "",
    step: "",
  }));
}

export function resolveValidationScopeWay(rawValue?: string | null): {
  scopeWay: ValidationScopeWayOption;
  consecThreshold: number;
} {
  const normalized = (rawValue ?? "").trim().toUpperCase();
  if (!normalized) {
    return {
      scopeWay: "LAST",
      consecThreshold: 2,
    };
  }
  if (normalized === "ANY" || normalized === "LAST" || normalized === "EACH" || normalized === "RECENT") {
    return {
      scopeWay: normalized,
      consecThreshold: 2,
    };
  }
  if (normalized.startsWith("CONSEC>=")) {
    const rawThreshold = normalized.slice("CONSEC>=".length).trim();
    const parsedThreshold = Number(rawThreshold);
    return {
      scopeWay: "CONSEC",
      consecThreshold:
        Number.isFinite(parsedThreshold) && Number.isInteger(parsedThreshold) && parsedThreshold >= 1
          ? parsedThreshold
          : 2,
    };
  }
  return {
    scopeWay: "LAST",
    consecThreshold: 2,
  };
}

export function formatUnknownValuesForCombo(item: RuleValidationComboResult) {
  return item.unknown_values.length > 0
    ? item.unknown_values
        .map((unknown) => `${unknown.name}=${formatNumber(unknown.value, 4)}`)
        .join(", ")
    : "默认参数";
}

export function resolveResidualDirection(
  contributionScore?: number | null,
  fallbackDirection?: ValidationDirection,
): ValidationDirection | null {
  if (contributionScore !== null && contributionScore !== undefined && Number.isFinite(contributionScore)) {
    if (contributionScore < 0) {
      return "negative";
    }
    if (contributionScore > 0) {
      return "positive";
    }
  }
  return fallbackDirection ?? null;
}

export function directionAdjustedResidual(
  value?: number | null,
  direction?: ValidationDirection | null,
) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return null;
  }
  return direction === "negative" ? -value : value;
}

export function metricHighlightClass(
  settings: BacktestHighlightSettings,
  metric: BacktestHighlightMetric,
  value?: number | null,
) {
  return shouldHighlightBacktestMetric(metric, value, settings)
    ? "expression-backtest-metric-hit"
    : undefined;
}

export function formatValidationWindowRatio(
  combo: RuleValidationComboResult,
  kind: "ic" | "residual" | "incremental",
) {
  const folds = (kind === "incremental" ? combo.incremental?.folds : combo.walk_forward?.folds) ?? [];
  if (folds.length === 0) {
    return "--";
  }
  const positive =
    kind === "incremental"
      ? combo.incremental?.positive_folds
      : kind === "ic"
        ? combo.walk_forward?.ic_positive_folds
        : combo.walk_forward?.residual_positive_folds;
  return `${positive ?? 0} / ${folds.length}`;
}

export function formatValidationInsufficientHint(
  combo: RuleValidationComboResult,
  kind: "walk_forward" | "incremental",
) {
  const folds = (kind === "incremental" ? combo.incremental?.folds : combo.walk_forward?.folds) ?? [];
  const insufficient = folds.filter((fold) => fold.status === "insufficient").length;
  return insufficient > 0 ? `${insufficient} / ${folds.length} 个 fold 样本不足（计入分母但不计为正窗口）` : "";
}

export function compactRuleLayerBacktestPayload(data: RuleLayerBacktestData): RuleLayerBacktestData {
  if (data.points.length === 0) {
    return data;
  }
  return {
    ...data,
    points: [],
  };
}

export function compactRuleExpressionValidationData(
  data: RuleExpressionValidationData,
): RuleExpressionValidationData {
  return {
    ...data,
    combo_results: data.combo_results.map((combo) => {
      const backtest = compactRuleLayerBacktestPayload(combo.backtest);
      return backtest === combo.backtest
        ? combo
        : {
            ...combo,
            backtest,
          };
    }),
  };
}
