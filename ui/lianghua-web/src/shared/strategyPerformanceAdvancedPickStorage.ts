import { readJsonStorage, writeJsonStorage } from "./storage";

export const STRATEGY_PERFORMANCE_ADVANCED_PICK_STATE_KEY =
  "lh_strategy_performance_advanced_pick_state_v2";

const LEGACY_STRATEGY_PERFORMANCE_ADVANCED_PICK_STATE_KEY =
  "lh_strategy_performance_advanced_pick_state_v1";
const LEGACY_STRATEGY_PERFORMANCE_STATE_KEY =
  "lh_strategy_performance_page_v13";

const DEFAULT_AUTO_MIN_SAMPLES = {
  2: 5,
  3: 5,
  5: 10,
  10: 20,
} as const;

export type StrategyPerformanceManualAdvantageSelection = {
  sourcePath: string;
  selectedHorizon: number;
  strongQuantile: number;
  manualAdvantageRuleNames: string[];
  autoMinSamples2: number;
  autoMinSamples3: number;
  autoMinSamples5: number;
  autoMinSamples10: number;
  requireWinRateAboveMarket: boolean;
  minPassHorizons: number;
  minAdvHits: number;
};

export type StrategyPerformanceAdvancedPickState = {
  draftSelection: StrategyPerformanceManualAdvantageSelection | null;
  syncedSelection: StrategyPerformanceManualAdvantageSelection | null;
};

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

function parsePositiveIntValue(value: unknown, fallback: number) {
  const parsed =
    typeof value === "number"
      ? value
      : typeof value === "string"
        ? Number(value)
        : Number.NaN;
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function parseQuantileValue(value: unknown, fallback = 0.9) {
  const parsed =
    typeof value === "number"
      ? value
      : typeof value === "string"
        ? Number(value)
        : Number.NaN;
  return Number.isFinite(parsed) && parsed > 0 && parsed < 1
    ? parsed
    : fallback;
}

function arrayFromUnknown(value: unknown) {
  return Array.isArray(value)
    ? value.filter((item): item is string => typeof item === "string")
    : [];
}

function normalizeSelectionFromUnknown(
  raw: unknown,
): StrategyPerformanceManualAdvantageSelection | null {
  if (!raw || typeof raw !== "object") {
    return null;
  }
  const stored = raw as Record<string, unknown>;
  const sourcePath =
    typeof stored.sourcePath === "string" ? stored.sourcePath.trim() : "";
  if (!sourcePath) {
    return null;
  }
  return {
    sourcePath,
    selectedHorizon: parsePositiveIntValue(stored.selectedHorizon, 2),
    strongQuantile: parseQuantileValue(stored.strongQuantile, 0.9),
    manualAdvantageRuleNames: normalizeStringArray(
      arrayFromUnknown(stored.manualAdvantageRuleNames),
    ),
    autoMinSamples2: parsePositiveIntValue(
      stored.autoMinSamples2,
      DEFAULT_AUTO_MIN_SAMPLES[2],
    ),
    autoMinSamples3: parsePositiveIntValue(
      stored.autoMinSamples3,
      DEFAULT_AUTO_MIN_SAMPLES[3],
    ),
    autoMinSamples5: parsePositiveIntValue(
      stored.autoMinSamples5,
      DEFAULT_AUTO_MIN_SAMPLES[5],
    ),
    autoMinSamples10: parsePositiveIntValue(
      stored.autoMinSamples10,
      DEFAULT_AUTO_MIN_SAMPLES[10],
    ),
    requireWinRateAboveMarket: stored.requireWinRateAboveMarket === true,
    minPassHorizons: parsePositiveIntValue(stored.minPassHorizons, 2),
    minAdvHits: parsePositiveIntValue(stored.minAdvHits, 1),
  };
}

function normalizeStateFromUnknown(
  raw: unknown,
): StrategyPerformanceAdvancedPickState | null {
  if (!raw || typeof raw !== "object") {
    return null;
  }
  const stored = raw as Record<string, unknown>;
  const draftSelection = normalizeSelectionFromUnknown(stored.draftSelection);
  const syncedSelection = normalizeSelectionFromUnknown(stored.syncedSelection);
  if (!draftSelection && !syncedSelection) {
    return null;
  }
  return {
    draftSelection,
    syncedSelection,
  };
}

function normalizeStateFromFlatValue(
  raw: unknown,
): StrategyPerformanceAdvancedPickState | null {
  const draftSelection = normalizeSelectionFromUnknown(raw);
  if (!draftSelection) {
    return null;
  }
  const stored = raw as Record<string, unknown>;
  const syncedManualAdvantageRuleNames = normalizeStringArray(
    arrayFromUnknown(
      stored.syncedManualAdvantageRuleNames ??
        stored.manualAdvantageRuleNames,
    ),
  );
  return {
    draftSelection,
    syncedSelection:
      syncedManualAdvantageRuleNames.length > 0
        ? {
            ...draftSelection,
            manualAdvantageRuleNames: syncedManualAdvantageRuleNames,
          }
        : null,
  };
}

function extractLegacyPositiveRuleNames(pageData: unknown) {
  if (!pageData || typeof pageData !== "object") {
    return new Set<string>();
  }
  const storedPageData = pageData as Record<string, unknown>;
  if (!Array.isArray(storedPageData.rule_rows)) {
    return new Set<string>();
  }

  const positiveRuleNames = new Set<string>();
  storedPageData.rule_rows.forEach((row) => {
    if (!row || typeof row !== "object") {
      return;
    }
    const storedRow = row as Record<string, unknown>;
    const ruleName =
      typeof storedRow.rule_name === "string" ? storedRow.rule_name.trim() : "";
    if (!ruleName || storedRow.signal_direction !== "positive") {
      return;
    }
    const metrics = Array.isArray(storedRow.metrics) ? storedRow.metrics : [];
    const hasPositiveHits = metrics.some((metric) => {
      if (!metric || typeof metric !== "object") {
        return false;
      }
      const storedMetric = metric as Record<string, unknown>;
      return typeof storedMetric.hit_n === "number" && storedMetric.hit_n > 0;
    });
    if (hasPositiveHits) {
      positiveRuleNames.add(ruleName);
    }
  });

  return positiveRuleNames;
}

function readLegacyState(storage: Storage | null | undefined) {
  if (!storage) {
    return null;
  }
  const raw = readJsonStorage<Record<string, unknown>>(
    storage,
    LEGACY_STRATEGY_PERFORMANCE_STATE_KEY,
  );
  if (!raw) {
    return null;
  }

  const sourcePath =
    typeof raw.sourcePath === "string" ? raw.sourcePath.trim() : "";
  if (!sourcePath) {
    return null;
  }

  const rawManualRuleNames = normalizeStringArray(
    arrayFromUnknown(raw.manualRuleNames),
  );
  const positiveRuleNames = extractLegacyPositiveRuleNames(raw.pageData);
  const manualAdvantageRuleNames =
    positiveRuleNames.size > 0
      ? rawManualRuleNames.filter((item) => positiveRuleNames.has(item))
      : rawManualRuleNames;

  const selection = {
    sourcePath,
    selectedHorizon: parsePositiveIntValue(raw.selectedHorizon, 2),
    strongQuantile: parseQuantileValue(raw.strongQuantile, 0.9),
    manualAdvantageRuleNames,
    autoMinSamples2: parsePositiveIntValue(
      raw.autoMinSamples2,
      DEFAULT_AUTO_MIN_SAMPLES[2],
    ),
    autoMinSamples3: parsePositiveIntValue(
      raw.autoMinSamples3,
      DEFAULT_AUTO_MIN_SAMPLES[3],
    ),
    autoMinSamples5: parsePositiveIntValue(
      raw.autoMinSamples5,
      DEFAULT_AUTO_MIN_SAMPLES[5],
    ),
    autoMinSamples10: parsePositiveIntValue(
      raw.autoMinSamples10,
      DEFAULT_AUTO_MIN_SAMPLES[10],
    ),
    requireWinRateAboveMarket: raw.requireWinRateAboveMarket === true,
    minPassHorizons: parsePositiveIntValue(raw.minPassHorizons, 2),
    minAdvHits: parsePositiveIntValue(raw.minAdvHits, 1),
  } satisfies StrategyPerformanceManualAdvantageSelection;

  return {
    draftSelection: selection,
    syncedSelection: selection,
  } satisfies StrategyPerformanceAdvancedPickState;
}

function readPersistedState(
  storage: Storage | null | undefined,
): StrategyPerformanceAdvancedPickState | null {
  if (!storage) {
    return null;
  }
  const nextState = normalizeStateFromUnknown(
    readJsonStorage<Record<string, unknown>>(
      storage,
      STRATEGY_PERFORMANCE_ADVANCED_PICK_STATE_KEY,
    ),
  );
  if (nextState) {
    return nextState;
  }

  const migratedFlatState = normalizeStateFromFlatValue(
    readJsonStorage<Record<string, unknown>>(
      storage,
      LEGACY_STRATEGY_PERFORMANCE_ADVANCED_PICK_STATE_KEY,
    ),
  );
  if (migratedFlatState) {
    writeStrategyPerformanceAdvancedPickState(storage, migratedFlatState);
    return migratedFlatState;
  }

  const migratedLegacyState = readLegacyState(storage);
  if (migratedLegacyState) {
    writeStrategyPerformanceAdvancedPickState(storage, migratedLegacyState);
  }
  return migratedLegacyState;
}

export function readStrategyPerformanceAdvancedPickState(
  storage: Storage | null | undefined = typeof window === "undefined"
    ? null
    : window.localStorage,
) {
  return readPersistedState(storage);
}

export function readStrategyPerformanceAdvancedPickDraft(
  storage: Storage | null | undefined = typeof window === "undefined"
    ? null
    : window.localStorage,
) {
  return readPersistedState(storage)?.draftSelection ?? null;
}

export function readStrategyPerformanceAdvancedPickSynced(
  storage: Storage | null | undefined = typeof window === "undefined"
    ? null
    : window.localStorage,
) {
  return readPersistedState(storage)?.syncedSelection ?? null;
}

export function writeStrategyPerformanceAdvancedPickState(
  storage: Storage | null | undefined,
  value: StrategyPerformanceAdvancedPickState | null,
) {
  if (!storage) {
    return false;
  }

  const draftSelection = normalizeSelectionFromUnknown(value?.draftSelection);
  const syncedSelection = normalizeSelectionFromUnknown(value?.syncedSelection);

  if (!draftSelection && !syncedSelection) {
    try {
      storage.removeItem(STRATEGY_PERFORMANCE_ADVANCED_PICK_STATE_KEY);
      return true;
    } catch {
      return false;
    }
  }

  return writeJsonStorage(storage, STRATEGY_PERFORMANCE_ADVANCED_PICK_STATE_KEY, {
    draftSelection,
    syncedSelection,
  } satisfies StrategyPerformanceAdvancedPickState);
}

export function writeStrategyPerformanceAdvancedPickDraft(
  storage: Storage | null | undefined,
  value: StrategyPerformanceManualAdvantageSelection | null,
) {
  const currentState = readPersistedState(storage);
  return writeStrategyPerformanceAdvancedPickState(storage, {
    draftSelection: value,
    syncedSelection: currentState?.syncedSelection ?? null,
  });
}

export function writeStrategyPerformanceAdvancedPickSynced(
  storage: Storage | null | undefined,
  value: StrategyPerformanceManualAdvantageSelection | null,
) {
  const currentState = readPersistedState(storage);
  return writeStrategyPerformanceAdvancedPickState(storage, {
    draftSelection: currentState?.draftSelection ?? null,
    syncedSelection: value,
  });
}
