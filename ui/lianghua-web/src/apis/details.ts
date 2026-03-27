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
  // 总览栏的所需数据结构
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
  concept?: string
  [key: string]: DetailPrimitive
}

export type DetailPrevRankRow = {
  // 前日排名数据
  trade_date: string
  rank?: number | null
  total?: number | null
}

export type DetailKlineRow = {
  // k线画图数据
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
  is_realtime?: boolean | null
  realtime_color_hint?: 'up' | 'down' | 'flat' | null
  [key: string]: DetailPrimitive
}

export type DetailKlinePanel = {
  // ?
  key: string
  label: string
  kind?: 'candles' | 'line' | 'bar' | 'brick'
  series_keys?: string[]
  row_weight?: number
}

export type DetailKlinePayload = {
  // 画图参数?
  items?: DetailKlineRow[]
  panels?: DetailKlinePanel[]
  default_window?: number
  chart_height?: number
  row_weights?: number[]
  watermark_name?: string
  watermark_code?: string
}

export type DetailStrategyTriggerRow = {
  // 策略触发表用数据
  rule_name: string
  rule_score?: number | null
  is_triggered?: boolean | null
  hit_date?: string
  lag?: number | null
  explain?: string
  tag?: string
  when?: string
}

export type DetailStrategyPayload = {
  // 触发和不触发区分
  triggered?: DetailStrategyTriggerRow[]
  untriggered?: DetailStrategyTriggerRow[]
}

export type StockDetailPageData = {
  // 总返回数据结构
  resolved_trade_date?: string
  resolved_ts_code?: string
  overview?: DetailOverview | null
  prev_ranks?: DetailPrevRankRow[]
  kline?: DetailKlinePayload | null
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

export async function getStockDetailPage(query: StockDetailQuery) {
  return invoke<StockDetailPageData>('get_stock_detail_page', query)
}

export async function getStockDetailRealtime(query: {
  sourcePath: string
  tsCode: string
  chartWindowDays?: number
}) {
  return invoke<StockDetailRealtimeData>('get_stock_detail_realtime', query)
}
