import { invoke } from '@tauri-apps/api/core'

type DetailPrimitive = string | number | boolean | null | undefined

export type StockDetailQuery = {
  sourcePath: string
  tradeDate?: string
  tsCode: string
  chartWindowDays?: number
  prevRankDays?: number
}

export type DetailOverview = {
  ts_code: string
  name?: string
  board?: string
  area?: string
  industry?: string
  trade_date?: string
  total_score?: number
  rank?: number | null
  total?: number | null
  total_mv_yi?: number
  circ_mv_yi?: number
  most_related_concept?: string
  concept?: string
  [key: string]: DetailPrimitive
}

export type DetailPrevRankRow = {
  trade_date: string
  rank?: number | null
  total?: number | null
}

export type DetailChartPanelRole = 'main' | 'sub'

export type DetailChartPanelKind = 'candles' | 'line' | 'bar' | 'brick'

export type DetailChartSeriesKind = 'line' | 'bar' | 'histogram' | 'area' | 'band' | 'brick'

export type DetailChartMarkerPosition = 'above' | 'below' | 'value'

export type DetailChartMarkerShape = 'dot' | 'triangle_up' | 'triangle_down' | 'flag'

export type DetailChartColorRule = {
  when_key: string
  color: string
}

export type DetailChartSeries = {
  key: string
  label?: string | null
  kind: DetailChartSeriesKind
  color?: string | null
  color_when?: DetailChartColorRule[] | null
  line_width?: number | null
  opacity?: number | null
  base_value?: number | null
}

export type DetailChartMarker = {
  key: string
  label?: string | null
  when_key: string
  y_key?: string | null
  position?: DetailChartMarkerPosition | null
  shape?: DetailChartMarkerShape | null
  color?: string | null
  text?: string | null
}

export type DetailKlineRow = {
  trade_date: string
  open?: number | null
  high?: number | null
  low?: number | null
  close?: number | null
  vol?: number | null
  amount?: number | null
  tor?: number | null
  brick?: number | null
  j?: number | null
  duokong_short?: number | null
  duokong_long?: number | null
  bupiao_short?: number | null
  bupiao_long?: number | null
  VOL_SIGMA?: number | null
  is_realtime?: boolean | null
  realtime_color_hint?: 'up' | 'down' | 'flat' | null
  [key: string]: DetailPrimitive
}

export type DetailKlinePanel = {
  key: string
  label: string
  role?: DetailChartPanelRole | null
  kind?: DetailChartPanelKind
  series?: DetailChartSeries[] | null
  markers?: DetailChartMarker[] | null
}

export type DetailKlinePayload = {
  items?: DetailKlineRow[]
  panels?: DetailKlinePanel[]
  default_window?: number
  chart_height?: number
  watermark_name?: string
  watermark_code?: string
}

export type DetailStrategyTriggerRow = {
  rule_name: string
  scene_name?: string | null
  rule_score?: number | null
  is_triggered?: boolean | null
  hit_date?: string
  lag?: number | null
  explain?: string
  tag?: string
  when?: string
}

export type DetailStrategyPayload = {
  triggered?: DetailStrategyTriggerRow[]
  untriggered?: DetailStrategyTriggerRow[]
}

export type DetailSceneTriggerRow = {
  scene_name: string
  direction?: string | null
  stage?: string | null
  stage_score?: number | null
  risk_score?: number | null
  confirm_strength?: number | null
  risk_intensity?: number | null
  scene_rank?: number | null
  hit_date?: string
  lag?: number | null
  observe_threshold?: number | null
  trigger_threshold?: number | null
  confirm_threshold?: number | null
  fail_threshold?: number | null
}

export type DetailScenePayload = {
  triggered?: DetailSceneTriggerRow[]
  untriggered?: DetailSceneTriggerRow[]
}

export type StockSimilarityTarget = {
  tsCode: string
  name?: string | null
  industry?: string | null
  concept?: string | null
  conceptItems: string[]
  triggerSceneNames: string[]
  availableScore: number
}

export type StockSimilarityRow = {
  tsCode: string
  name?: string | null
  industry?: string | null
  concept?: string | null
  totalScore?: number | null
  rank?: number | null
  similarityScore: number
  conceptScore: number
  industryScore: number
  sceneScore: number
  sameIndustry: boolean
  matchedConcepts: string[]
  matchedSceneNames: string[]
  conceptMatchRatio?: number | null
  sceneMatchRatio?: number | null
}

export type StockSimilarityPageData = {
  resolvedTradeDate: string
  resolvedTsCode: string
  target: StockSimilarityTarget
  items: StockSimilarityRow[]
}

export type StockDetailPageData = {
  resolved_trade_date?: string
  resolved_ts_code?: string
  overview?: DetailOverview | null
  prev_ranks?: DetailPrevRankRow[]
  stock_similarity?: StockSimilarityPageData | null
  stock_similarity_error?: string | null
  kline?: DetailKlinePayload | null
  strategy_triggers?: DetailStrategyPayload | null
  strategy_scenes?: DetailScenePayload | null
}

export type StockDetailStrategySnapshotData = {
  resolved_trade_date?: string
  resolved_ts_code?: string
  strategy_triggers?: DetailStrategyPayload | null
}

export type StockDetailRealtimeData = {
  tsCode: string
  refreshedAt?: string | null
  quoteTradeDate?: string | null
  quoteTime?: string | null
  hasDatabaseTradeDate: boolean
  kline: DetailKlinePayload
}

export type DetailCyqBin = {
  price: number
  price_low: number
  price_high: number
  chip: number
  chip_pct: number
}

export type DetailCyqSnapshot = {
  trade_date: string
  close: number
  bins: DetailCyqBin[]
}

export type StockDetailCyqData = {
  resolved_ts_code: string
  factor?: number | null
  snapshots: DetailCyqSnapshot[]
}

export async function getStockDetailPage(query: StockDetailQuery) {
  return invoke<StockDetailPageData>('get_stock_detail_page', query)
}

export async function getStockDetailStrategySnapshot(query: {
  sourcePath: string
  tradeDate?: string
  tsCode: string
}) {
  return invoke<StockDetailStrategySnapshotData>('get_stock_detail_strategy_snapshot', query)
}

export async function getStockDetailRealtime(query: {
  sourcePath: string
  tsCode: string
  chartWindowDays?: number
}) {
  return invoke<StockDetailRealtimeData>('get_stock_detail_realtime', query)
}

export async function getStockDetailCyq(query: {
  sourcePath: string
  tsCode: string
}) {
  return invoke<StockDetailCyqData>('get_stock_detail_cyq', query)
}
