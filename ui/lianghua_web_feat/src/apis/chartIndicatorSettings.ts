import { invoke } from '@tauri-apps/api/core'

export type ChartPanelRole = 'main' | 'sub'

export type ChartPanelKind = 'candles' | 'line' | 'bar' | 'brick'

export type ChartSeriesKind = 'line' | 'bar' | 'histogram' | 'area' | 'band' | 'brick'

export type ChartMarkerPosition = 'above' | 'below' | 'value'

export type ChartMarkerShape = 'dot' | 'triangle_up' | 'triangle_down' | 'flag'

export type ChartColorRuleDraft = {
  when: string
  color: string
}

export type ChartSeriesDraft = {
  key: string
  label?: string | null
  expr: string
  kind: ChartSeriesKind
  color?: string | null
  color_when?: ChartColorRuleDraft[] | null
  line_width?: number | null
  opacity?: number | null
  base_value?: number | null
}

export type ChartMarkerDraft = {
  key: string
  label?: string | null
  when: string
  y?: string | null
  position?: ChartMarkerPosition | null
  shape?: ChartMarkerShape | null
  color?: string | null
  text?: string | null
}

export type ChartPanelDraft = {
  key: string
  label: string
  role: ChartPanelRole
  kind: ChartPanelKind
  series?: ChartSeriesDraft[] | null
  marker?: ChartMarkerDraft[] | null
}

export type ChartIndicatorConfigDraft = {
  version: number
  panel: ChartPanelDraft[]
}

export type ChartIndicatorSettingsSummary = {
  panelCount: number
  seriesCount: number
  markerCount: number
  databaseIndicatorColumns: string[]
}

export type ChartIndicatorSettingsPayload = {
  sourcePath: string
  filePath: string
  exists: boolean
  text: string
  config: ChartIndicatorConfigDraft
  summary: ChartIndicatorSettingsSummary
  error?: string | null
}

export type ChartIndicatorValidationResult = {
  ok: boolean
  error?: string | null
  config?: ChartIndicatorConfigDraft | null
  summary?: ChartIndicatorSettingsSummary | null
}

export async function getChartIndicatorSettings(sourcePath: string) {
  return invoke<ChartIndicatorSettingsPayload>('get_chart_indicator_settings', { sourcePath })
}

export async function validateChartIndicatorSettings(sourcePath: string, text: string) {
  return invoke<ChartIndicatorValidationResult>('validate_chart_indicator_settings', {
    sourcePath,
    text,
  })
}

export async function saveChartIndicatorSettings(sourcePath: string, text: string) {
  return invoke<ChartIndicatorSettingsPayload>('save_chart_indicator_settings', {
    sourcePath,
    text,
  })
}

export async function resetChartIndicatorSettings(sourcePath: string) {
  return invoke<ChartIndicatorSettingsPayload>('reset_chart_indicator_settings', { sourcePath })
}
