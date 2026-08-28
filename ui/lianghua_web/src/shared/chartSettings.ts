const CHART_MAIN_WIDTH_RATIO_STORAGE_KEY = 'lh_chart_main_width_ratio_v1'
const CHART_INDICATOR_WIDTH_RATIO_STORAGE_KEY = 'lh_chart_indicator_width_ratio_v1'
const CHART_MAIN_HEIGHT_MODE_STORAGE_KEY = 'lh_chart_main_height_mode_v1'
const CHART_MAIN_PERCENT_PIXELS_STORAGE_KEY = 'lh_chart_main_percent_pixels_v1'
const CHART_MAIN_PERCENT_MIN_HEIGHT_STORAGE_KEY = 'lh_chart_main_percent_min_height_v1'
const CHART_MAIN_PERCENT_MAX_HEIGHT_STORAGE_KEY = 'lh_chart_main_percent_max_height_v1'
const DETAILS_NAV_LONG_PRESS_INTERVAL_SECONDS_STORAGE_KEY = 'lh_details_nav_long_press_interval_seconds_v1'
const DETAIL_CYQ_MODEL_STORAGE_KEY = 'lh_detail_cyq_model_v1'
const CHART_DEFAULT_VISIBLE_BARS_STORAGE_KEY = 'lh_chart_default_visible_bars_v1'

export type DetailCyqModel = 'legacy' | 'chen'
export type ChartMainHeightMode = 'fixed' | 'percent'

export const CHART_MAIN_WIDTH_RATIO_DEFAULT = 0.36
export const CHART_MAIN_WIDTH_RATIO_MIN = 0.1
export const CHART_MAIN_WIDTH_RATIO_MAX = 1.2
export const CHART_INDICATOR_WIDTH_RATIO_DEFAULT = 0.5
export const CHART_INDICATOR_WIDTH_RATIO_MIN = 0.1
export const CHART_INDICATOR_WIDTH_RATIO_MAX = 1.2
export const CHART_MAIN_HEIGHT_MODE_DEFAULT: ChartMainHeightMode = 'fixed'
export const CHART_MAIN_PERCENT_PIXELS_DEFAULT = 8
export const CHART_MAIN_PERCENT_PIXELS_MIN = 1
export const CHART_MAIN_PERCENT_PIXELS_MAX = 30
export const CHART_MAIN_PERCENT_MIN_HEIGHT_DEFAULT = 320
export const CHART_MAIN_PERCENT_MAX_HEIGHT_DEFAULT = 720
export const CHART_MAIN_PERCENT_HEIGHT_LIMIT_MIN = 200
export const CHART_MAIN_PERCENT_HEIGHT_LIMIT_MAX = 1600
export const DETAILS_NAV_LONG_PRESS_INTERVAL_SECONDS_DEFAULT = 1
export const DETAILS_NAV_LONG_PRESS_INTERVAL_SECONDS_MIN = 0.2
export const DETAILS_NAV_LONG_PRESS_INTERVAL_SECONDS_MAX = 10
export const DETAIL_CYQ_MODEL_DEFAULT: DetailCyqModel = 'legacy'
export const CHART_DEFAULT_VISIBLE_BARS_DEFAULT = 90
export const CHART_DEFAULT_VISIBLE_BARS_MIN = 20
export const CHART_DEFAULT_VISIBLE_BARS_MAX = 280

export function normalizeChartMainHeightMode(
  value: string | null | undefined,
): ChartMainHeightMode {
  return value === 'percent' ? 'percent' : CHART_MAIN_HEIGHT_MODE_DEFAULT
}

export function clampChartDefaultVisibleBars(value: number) {
  return Math.round(clampNumber(
    value,
    CHART_DEFAULT_VISIBLE_BARS_DEFAULT,
    CHART_DEFAULT_VISIBLE_BARS_MIN,
    CHART_DEFAULT_VISIBLE_BARS_MAX,
  ))
}

export function readStoredChartDefaultVisibleBars() {
  return clampChartDefaultVisibleBars(readStoredNumber(
    CHART_DEFAULT_VISIBLE_BARS_STORAGE_KEY,
    CHART_DEFAULT_VISIBLE_BARS_DEFAULT,
  ))
}

export function writeStoredChartDefaultVisibleBars(nextValue: number) {
  if (typeof window === 'undefined') {
    return
  }

  window.localStorage.setItem(
    CHART_DEFAULT_VISIBLE_BARS_STORAGE_KEY,
    clampChartDefaultVisibleBars(nextValue).toString(),
  )
}

function clampNumber(value: number, fallback: number, min: number, max: number) {
  if (!Number.isFinite(value)) {
    return fallback
  }

  return Math.min(max, Math.max(min, value))
}

export function clampChartMainPercentPixels(value: number) {
  return Number(clampNumber(
    value,
    CHART_MAIN_PERCENT_PIXELS_DEFAULT,
    CHART_MAIN_PERCENT_PIXELS_MIN,
    CHART_MAIN_PERCENT_PIXELS_MAX,
  ).toFixed(2))
}

export function clampChartMainPercentHeight(value: number, fallback: number) {
  return Math.round(clampNumber(
    value,
    fallback,
    CHART_MAIN_PERCENT_HEIGHT_LIMIT_MIN,
    CHART_MAIN_PERCENT_HEIGHT_LIMIT_MAX,
  ))
}

function readStoredNumber(key: string, fallback: number) {
  if (typeof window === 'undefined') {
    return fallback
  }

  const rawValue = window.localStorage.getItem(key)
  if (rawValue === null || rawValue.trim() === '') {
    return fallback
  }

  const parsedValue = Number(rawValue)
  return Number.isFinite(parsedValue) ? parsedValue : fallback
}

export function readStoredChartMainHeightMode() {
  if (typeof window === 'undefined') {
    return CHART_MAIN_HEIGHT_MODE_DEFAULT
  }

  return normalizeChartMainHeightMode(
    window.localStorage.getItem(CHART_MAIN_HEIGHT_MODE_STORAGE_KEY),
  )
}

export function writeStoredChartMainHeightMode(nextValue: ChartMainHeightMode) {
  if (typeof window === 'undefined') {
    return
  }

  window.localStorage.setItem(
    CHART_MAIN_HEIGHT_MODE_STORAGE_KEY,
    normalizeChartMainHeightMode(nextValue),
  )
}

export function readStoredChartMainPercentPixels() {
  return clampChartMainPercentPixels(readStoredNumber(
    CHART_MAIN_PERCENT_PIXELS_STORAGE_KEY,
    CHART_MAIN_PERCENT_PIXELS_DEFAULT,
  ))
}

export function writeStoredChartMainPercentPixels(nextValue: number) {
  if (typeof window === 'undefined') {
    return
  }

  window.localStorage.setItem(
    CHART_MAIN_PERCENT_PIXELS_STORAGE_KEY,
    clampChartMainPercentPixels(nextValue).toString(),
  )
}

export function readStoredChartMainPercentMinHeight() {
  return clampChartMainPercentHeight(
    readStoredNumber(
      CHART_MAIN_PERCENT_MIN_HEIGHT_STORAGE_KEY,
      CHART_MAIN_PERCENT_MIN_HEIGHT_DEFAULT,
    ),
    CHART_MAIN_PERCENT_MIN_HEIGHT_DEFAULT,
  )
}

export function writeStoredChartMainPercentMinHeight(nextValue: number) {
  if (typeof window === 'undefined') {
    return
  }

  window.localStorage.setItem(
    CHART_MAIN_PERCENT_MIN_HEIGHT_STORAGE_KEY,
    clampChartMainPercentHeight(nextValue, CHART_MAIN_PERCENT_MIN_HEIGHT_DEFAULT).toString(),
  )
}

export function readStoredChartMainPercentMaxHeight() {
  return clampChartMainPercentHeight(
    readStoredNumber(
      CHART_MAIN_PERCENT_MAX_HEIGHT_STORAGE_KEY,
      CHART_MAIN_PERCENT_MAX_HEIGHT_DEFAULT,
    ),
    CHART_MAIN_PERCENT_MAX_HEIGHT_DEFAULT,
  )
}

export function writeStoredChartMainPercentMaxHeight(nextValue: number) {
  if (typeof window === 'undefined') {
    return
  }

  window.localStorage.setItem(
    CHART_MAIN_PERCENT_MAX_HEIGHT_STORAGE_KEY,
    clampChartMainPercentHeight(nextValue, CHART_MAIN_PERCENT_MAX_HEIGHT_DEFAULT).toString(),
  )
}

export function normalizeDetailCyqModel(value: string | null | undefined): DetailCyqModel {
  return value === 'chen' ? 'chen' : DETAIL_CYQ_MODEL_DEFAULT
}

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

export function clampDetailsNavLongPressIntervalSeconds(value: number) {
  if (!Number.isFinite(value)) {
    return DETAILS_NAV_LONG_PRESS_INTERVAL_SECONDS_DEFAULT
  }

  const clampedValue = Math.min(
    DETAILS_NAV_LONG_PRESS_INTERVAL_SECONDS_MAX,
    Math.max(DETAILS_NAV_LONG_PRESS_INTERVAL_SECONDS_MIN, value),
  )
  return Number(clampedValue.toFixed(2))
}

export function readStoredDetailsNavLongPressIntervalSeconds() {
  if (typeof window === 'undefined') {
    return DETAILS_NAV_LONG_PRESS_INTERVAL_SECONDS_DEFAULT
  }

  const rawValue = window.localStorage.getItem(
    DETAILS_NAV_LONG_PRESS_INTERVAL_SECONDS_STORAGE_KEY,
  )
  if (!rawValue) {
    return DETAILS_NAV_LONG_PRESS_INTERVAL_SECONDS_DEFAULT
  }

  const parsedValue = Number(rawValue)
  if (!Number.isFinite(parsedValue)) {
    return DETAILS_NAV_LONG_PRESS_INTERVAL_SECONDS_DEFAULT
  }

  return clampDetailsNavLongPressIntervalSeconds(parsedValue)
}

export function writeStoredDetailsNavLongPressIntervalSeconds(nextValue: number) {
  if (typeof window === 'undefined') {
    return
  }

  const normalizedValue = clampDetailsNavLongPressIntervalSeconds(nextValue)
  window.localStorage.setItem(
    DETAILS_NAV_LONG_PRESS_INTERVAL_SECONDS_STORAGE_KEY,
    normalizedValue.toString(),
  )
}

export function readStoredDetailCyqModel() {
  if (typeof window === 'undefined') {
    return DETAIL_CYQ_MODEL_DEFAULT
  }

  return normalizeDetailCyqModel(
    window.localStorage.getItem(DETAIL_CYQ_MODEL_STORAGE_KEY),
  )
}

export function writeStoredDetailCyqModel(nextValue: DetailCyqModel) {
  if (typeof window === 'undefined') {
    return
  }

  window.localStorage.setItem(
    DETAIL_CYQ_MODEL_STORAGE_KEY,
    normalizeDetailCyqModel(nextValue),
  )
}
