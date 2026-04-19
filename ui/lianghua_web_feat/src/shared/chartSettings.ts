const CHART_MAIN_WIDTH_RATIO_STORAGE_KEY = 'lh_chart_main_width_ratio_v1'
const CHART_INDICATOR_WIDTH_RATIO_STORAGE_KEY = 'lh_chart_indicator_width_ratio_v1'
const CHART_RANK_MARKER_THRESHOLD_STORAGE_KEY = 'lh_chart_rank_marker_threshold_v1'

export const CHART_MAIN_WIDTH_RATIO_DEFAULT = 0.36
export const CHART_MAIN_WIDTH_RATIO_MIN = 0.1
export const CHART_MAIN_WIDTH_RATIO_MAX = 1.2
export const CHART_INDICATOR_WIDTH_RATIO_DEFAULT = 0.5
export const CHART_INDICATOR_WIDTH_RATIO_MIN = 0.1
export const CHART_INDICATOR_WIDTH_RATIO_MAX = 1.2
export const CHART_RANK_MARKER_THRESHOLD_DEFAULT = 100
export const CHART_RANK_MARKER_THRESHOLD_MIN = 1
export const CHART_RANK_MARKER_THRESHOLD_MAX = 5000

export function clampChartMainWidthRatio(value: number) {
  if (!Number.isFinite(value)) {
    return CHART_MAIN_WIDTH_RATIO_DEFAULT
  }

  return Math.min(
    CHART_MAIN_WIDTH_RATIO_MAX,
    Math.max(CHART_MAIN_WIDTH_RATIO_MIN, value),
  )
}

export function readStoredChartMainWidthRatio() {
  if (typeof window === 'undefined') {
    return CHART_MAIN_WIDTH_RATIO_DEFAULT
  }

  const rawValue = window.localStorage.getItem(CHART_MAIN_WIDTH_RATIO_STORAGE_KEY)
  if (!rawValue) {
    return CHART_MAIN_WIDTH_RATIO_DEFAULT
  }

  const parsedValue = Number(rawValue)
  if (!Number.isFinite(parsedValue)) {
    return CHART_MAIN_WIDTH_RATIO_DEFAULT
  }

  return clampChartMainWidthRatio(parsedValue)
}

export function writeStoredChartMainWidthRatio(nextValue: number) {
  if (typeof window === 'undefined') {
    return
  }

  const normalizedValue = clampChartMainWidthRatio(nextValue)
  window.localStorage.setItem(
    CHART_MAIN_WIDTH_RATIO_STORAGE_KEY,
    normalizedValue.toString(),
  )
}

export function clampChartIndicatorWidthRatio(value: number) {
  if (!Number.isFinite(value)) {
    return CHART_INDICATOR_WIDTH_RATIO_DEFAULT
  }

  return Math.min(
    CHART_INDICATOR_WIDTH_RATIO_MAX,
    Math.max(CHART_INDICATOR_WIDTH_RATIO_MIN, value),
  )
}

export function readStoredChartIndicatorWidthRatio() {
  if (typeof window === 'undefined') {
    return CHART_INDICATOR_WIDTH_RATIO_DEFAULT
  }

  const rawValue = window.localStorage.getItem(CHART_INDICATOR_WIDTH_RATIO_STORAGE_KEY)
  if (!rawValue) {
    return CHART_INDICATOR_WIDTH_RATIO_DEFAULT
  }

  const parsedValue = Number(rawValue)
  if (!Number.isFinite(parsedValue)) {
    return CHART_INDICATOR_WIDTH_RATIO_DEFAULT
  }

  return clampChartIndicatorWidthRatio(parsedValue)
}

export function writeStoredChartIndicatorWidthRatio(nextValue: number) {
  if (typeof window === 'undefined') {
    return
  }

  const normalizedValue = clampChartIndicatorWidthRatio(nextValue)
  window.localStorage.setItem(
    CHART_INDICATOR_WIDTH_RATIO_STORAGE_KEY,
    normalizedValue.toString(),
  )
}

export function clampChartRankMarkerThreshold(value: number) {
  if (!Number.isFinite(value)) {
    return CHART_RANK_MARKER_THRESHOLD_DEFAULT
  }

  return Math.round(
    Math.min(
      CHART_RANK_MARKER_THRESHOLD_MAX,
      Math.max(CHART_RANK_MARKER_THRESHOLD_MIN, value),
    ),
  )
}

export function readStoredChartRankMarkerThreshold() {
  if (typeof window === 'undefined') {
    return CHART_RANK_MARKER_THRESHOLD_DEFAULT
  }

  const rawValue = window.localStorage.getItem(CHART_RANK_MARKER_THRESHOLD_STORAGE_KEY)
  if (!rawValue) {
    return CHART_RANK_MARKER_THRESHOLD_DEFAULT
  }

  const parsedValue = Number(rawValue)
  if (!Number.isFinite(parsedValue)) {
    return CHART_RANK_MARKER_THRESHOLD_DEFAULT
  }

  return clampChartRankMarkerThreshold(parsedValue)
}

export function writeStoredChartRankMarkerThreshold(nextValue: number) {
  if (typeof window === 'undefined') {
    return
  }

  const normalizedValue = clampChartRankMarkerThreshold(nextValue)
  window.localStorage.setItem(
    CHART_RANK_MARKER_THRESHOLD_STORAGE_KEY,
    normalizedValue.toString(),
  )
}
