import { invoke } from '@tauri-apps/api/core'

export type MarketMonitorRow = {
  tsCode: string
  name: string
  referenceTradeDate?: string | null
  referenceRank?: number | null
  totalScore?: number | null
  latestPrice?: number | null
  latestChangePct?: number | null
  open?: number | null
  high?: number | null
  low?: number | null
  concept?: string
}

export type MarketMonitorPageData = {
  rows: MarketMonitorRow[]
  requestedCount: number
  effectiveCount: number
  fetchedCount: number
  truncated: boolean
  refreshedAt?: string | null
  referenceTradeDate?: string | null
}

export type MarketMonitorQuery = {
  sourcePath: string
  referenceTradeDate?: string
  topLimit?: number
}

export async function getMarketMonitorPage(query: MarketMonitorQuery) {
  return invoke<MarketMonitorPageData>('get_market_monitor_page', query)
}
