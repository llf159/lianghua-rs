import type { RankLayerMethod } from "../apis/strategyTrigger";
import { STOCK_PICK_BOARD_OPTIONS } from "./stockPickShared";
import { readJsonStorage, writeJsonStorage } from "./storage";

export type BacktestCommonParamsDraft = {
  stockAdjType: string;
  indexTsCode: string;
  indexBeta: string;
  conceptBeta: string;
  industryBeta: string;
  startDateInput: string;
  endDateInput: string;
  minSamplesPerDay: string;
  minListedTradeDays: string;
  backtestPeriod: string;
  parallelBatchSize: string;
  totalMvMin: string;
  totalMvMax: string;
  rankLayerCount: string;
  rankLayerMethod: RankLayerMethod;
  backtestBoardFilter: (typeof STOCK_PICK_BOARD_OPTIONS)[number];
};

type StoredBacktestCommonParamsDraft = Omit<BacktestCommonParamsDraft, "endDateInput">;

export const BACKTEST_COMMON_PARAMS_STORAGE_KEY = "lh_scene_layer_backtest_common_params";

export const INDEX_OPTIONS = [
  { value: "000001.SH", label: "上证指数" },
  { value: "399001.SZ", label: "深证成指" },
  { value: "399006.SZ", label: "创业板指" },
  { value: "000300.SH", label: "沪深300" },
  { value: "000905.SH", label: "中证500" },
  { value: "000852.SH", label: "中证1000" },
  { value: "000688.SH", label: "科创50" },
] as const;

export const DEFAULT_BACKTEST_COMMON_PARAMS: BacktestCommonParamsDraft = {
  stockAdjType: "qfq",
  indexTsCode: INDEX_OPTIONS[0].value,
  indexBeta: "0.5",
  conceptBeta: "0.1",
  industryBeta: "0.1",
  startDateInput: "",
  endDateInput: "",
  minSamplesPerDay: "5",
  minListedTradeDays: "60",
  backtestPeriod: "3",
  parallelBatchSize: "4",
  totalMvMin: "",
  totalMvMax: "",
  rankLayerCount: "5",
  rankLayerMethod: "sample_count",
  backtestBoardFilter: "全部",
};

export const RANK_LAYER_METHOD_OPTIONS: Array<{ value: RankLayerMethod; label: string }> = [
  { value: "sample_count", label: "按样本数分层" },
  { value: "score", label: "按分数分层" },
  { value: "rank", label: "按排名分层" },
];

function normalizeStoredString(value: unknown, fallback: string) {
  return typeof value === "string" ? value : fallback;
}

function normalizeRankLayerMethod(value: unknown): RankLayerMethod {
  return RANK_LAYER_METHOD_OPTIONS.some((item) => item.value === value)
    ? (value as RankLayerMethod)
    : DEFAULT_BACKTEST_COMMON_PARAMS.rankLayerMethod;
}

export function readStoredBacktestCommonParams(): BacktestCommonParamsDraft {
  const parsed = readJsonStorage<
    Partial<StoredBacktestCommonParamsDraft> & {
      ruleBoardFilter?: (typeof STOCK_PICK_BOARD_OPTIONS)[number];
    }
  >(
    typeof window === "undefined" ? null : window.localStorage,
    BACKTEST_COMMON_PARAMS_STORAGE_KEY,
  );
  const indexTsCode = normalizeStoredString(parsed?.indexTsCode, DEFAULT_BACKTEST_COMMON_PARAMS.indexTsCode);
  const parsedBoardFilter =
    parsed?.backtestBoardFilter && STOCK_PICK_BOARD_OPTIONS.includes(parsed.backtestBoardFilter)
      ? parsed.backtestBoardFilter
      : parsed?.ruleBoardFilter && STOCK_PICK_BOARD_OPTIONS.includes(parsed.ruleBoardFilter)
        ? parsed.ruleBoardFilter
        : DEFAULT_BACKTEST_COMMON_PARAMS.backtestBoardFilter;

  return {
    stockAdjType: normalizeStoredString(parsed?.stockAdjType, DEFAULT_BACKTEST_COMMON_PARAMS.stockAdjType),
    indexTsCode: INDEX_OPTIONS.some((item) => item.value === indexTsCode)
      ? indexTsCode
      : DEFAULT_BACKTEST_COMMON_PARAMS.indexTsCode,
    indexBeta: normalizeStoredString(parsed?.indexBeta, DEFAULT_BACKTEST_COMMON_PARAMS.indexBeta),
    conceptBeta: normalizeStoredString(parsed?.conceptBeta, DEFAULT_BACKTEST_COMMON_PARAMS.conceptBeta),
    industryBeta: normalizeStoredString(parsed?.industryBeta, DEFAULT_BACKTEST_COMMON_PARAMS.industryBeta),
    startDateInput: normalizeStoredString(parsed?.startDateInput, DEFAULT_BACKTEST_COMMON_PARAMS.startDateInput),
    endDateInput: DEFAULT_BACKTEST_COMMON_PARAMS.endDateInput,
    minSamplesPerDay: normalizeStoredString(parsed?.minSamplesPerDay, DEFAULT_BACKTEST_COMMON_PARAMS.minSamplesPerDay),
    minListedTradeDays: normalizeStoredString(
      parsed?.minListedTradeDays,
      DEFAULT_BACKTEST_COMMON_PARAMS.minListedTradeDays,
    ),
    backtestPeriod: normalizeStoredString(parsed?.backtestPeriod, DEFAULT_BACKTEST_COMMON_PARAMS.backtestPeriod),
    parallelBatchSize: normalizeStoredString(
      parsed?.parallelBatchSize,
      DEFAULT_BACKTEST_COMMON_PARAMS.parallelBatchSize,
    ),
    totalMvMin: normalizeStoredString(parsed?.totalMvMin, DEFAULT_BACKTEST_COMMON_PARAMS.totalMvMin),
    totalMvMax: normalizeStoredString(parsed?.totalMvMax, DEFAULT_BACKTEST_COMMON_PARAMS.totalMvMax),
    rankLayerCount: normalizeStoredString(parsed?.rankLayerCount, DEFAULT_BACKTEST_COMMON_PARAMS.rankLayerCount),
    rankLayerMethod: normalizeRankLayerMethod(parsed?.rankLayerMethod),
    backtestBoardFilter: parsedBoardFilter,
  };
}

export function writeStoredBacktestCommonParams(value: BacktestCommonParamsDraft) {
  const storedValue: StoredBacktestCommonParamsDraft = {
    stockAdjType: value.stockAdjType,
    indexTsCode: value.indexTsCode,
    indexBeta: value.indexBeta,
    conceptBeta: value.conceptBeta,
    industryBeta: value.industryBeta,
    startDateInput: value.startDateInput,
    minSamplesPerDay: value.minSamplesPerDay,
    minListedTradeDays: value.minListedTradeDays,
    backtestPeriod: value.backtestPeriod,
    parallelBatchSize: value.parallelBatchSize,
    totalMvMin: value.totalMvMin,
    totalMvMax: value.totalMvMax,
    rankLayerCount: value.rankLayerCount,
    rankLayerMethod: value.rankLayerMethod,
    backtestBoardFilter: value.backtestBoardFilter,
  };
  writeJsonStorage(
    typeof window === "undefined" ? null : window.localStorage,
    BACKTEST_COMMON_PARAMS_STORAGE_KEY,
    storedValue,
  );
}
