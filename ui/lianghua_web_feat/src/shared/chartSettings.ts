const CHART_MAIN_WIDTH_RATIO_STORAGE_KEY = 'lh_chart_main_width_ratio_v1'
const CHART_INDICATOR_WIDTH_RATIO_STORAGE_KEY = 'lh_chart_indicator_width_ratio_v1'
const DETAILS_NAV_LONG_PRESS_INTERVAL_SECONDS_STORAGE_KEY = 'lh_details_nav_long_press_interval_seconds_v1'

export const CHART_MAIN_WIDTH_RATIO_DEFAULT = 0.36
export const CHART_MAIN_WIDTH_RATIO_MIN = 0.1
export const CHART_MAIN_WIDTH_RATIO_MAX = 1.2
export const CHART_INDICATOR_WIDTH_RATIO_DEFAULT = 0.5
export const CHART_INDICATOR_WIDTH_RATIO_MIN = 0.1
export const CHART_INDICATOR_WIDTH_RATIO_MAX = 1.2
export const DETAILS_NAV_LONG_PRESS_INTERVAL_SECONDS_DEFAULT = 1
export const DETAILS_NAV_LONG_PRESS_INTERVAL_SECONDS_MIN = 0.2
export const DETAILS_NAV_LONG_PRESS_INTERVAL_SECONDS_MAX = 10

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
