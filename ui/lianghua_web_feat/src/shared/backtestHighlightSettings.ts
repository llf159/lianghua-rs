const BACKTEST_HIGHLIGHT_SETTINGS_STORAGE_KEY = 'lh_backtest_highlight_settings_v1'

export const BACKTEST_IC_THRESHOLD_DEFAULT = 0.003
export const BACKTEST_IR_THRESHOLD_DEFAULT = 0.3
export const BACKTEST_T_THRESHOLD_DEFAULT = 2

export type BacktestHighlightMetric = 'ic' | 'ir' | 't'

export type BacktestHighlightSettings = {
  icThreshold: number
  icUseAbs: boolean
  irThreshold: number
  irUseAbs: boolean
  tThreshold: number
  tUseAbs: boolean
}

export function defaultBacktestHighlightSettings(): BacktestHighlightSettings {
  return {
    icThreshold: BACKTEST_IC_THRESHOLD_DEFAULT,
    icUseAbs: false,
    irThreshold: BACKTEST_IR_THRESHOLD_DEFAULT,
    irUseAbs: false,
    tThreshold: BACKTEST_T_THRESHOLD_DEFAULT,
    tUseAbs: false,
  }
}

function normalizeThreshold(value: unknown, fallback: number) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed) || parsed < 0) {
    return fallback
  }
  return parsed
}

function normalizeBoolean(value: unknown, fallback: boolean) {
  if (typeof value !== 'boolean') {
    return fallback
  }
  return value
}

function normalizeSettings(raw: Partial<BacktestHighlightSettings> | null | undefined) {
  const defaults = defaultBacktestHighlightSettings()
  if (!raw) {
    return defaults
  }

  return {
    icThreshold: normalizeThreshold(raw.icThreshold, defaults.icThreshold),
    icUseAbs: normalizeBoolean(raw.icUseAbs, defaults.icUseAbs),
    irThreshold: normalizeThreshold(raw.irThreshold, defaults.irThreshold),
    irUseAbs: normalizeBoolean(raw.irUseAbs, defaults.irUseAbs),
    tThreshold: normalizeThreshold(raw.tThreshold, defaults.tThreshold),
    tUseAbs: normalizeBoolean(raw.tUseAbs, defaults.tUseAbs),
  }
}

export function readStoredBacktestHighlightSettings() {
  if (typeof window === 'undefined') {
    return defaultBacktestHighlightSettings()
  }

  try {
    const rawText = window.localStorage.getItem(BACKTEST_HIGHLIGHT_SETTINGS_STORAGE_KEY)
    if (!rawText) {
      return defaultBacktestHighlightSettings()
    }
    const parsed = JSON.parse(rawText) as Partial<BacktestHighlightSettings>
    return normalizeSettings(parsed)
  } catch {
    return defaultBacktestHighlightSettings()
  }
}

export function writeStoredBacktestHighlightSettings(settings: BacktestHighlightSettings) {
  if (typeof window === 'undefined') {
    return
  }

  const normalized = normalizeSettings(settings)
  window.localStorage.setItem(
    BACKTEST_HIGHLIGHT_SETTINGS_STORAGE_KEY,
    JSON.stringify(normalized),
  )
}

export function shouldHighlightBacktestMetric(
  metric: BacktestHighlightMetric,
  value: number | null | undefined,
  settings: BacktestHighlightSettings,
) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return false
  }

  if (metric === 'ic') {
    const checked = settings.icUseAbs ? Math.abs(value) : value
    return checked >= settings.icThreshold
  }
  if (metric === 'ir') {
    const checked = settings.irUseAbs ? Math.abs(value) : value
    return checked >= settings.irThreshold
  }

  const checked = settings.tUseAbs ? Math.abs(value) : value
  return checked >= settings.tThreshold
}
