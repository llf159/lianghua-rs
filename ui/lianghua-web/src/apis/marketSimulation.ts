import { invoke } from '@tauri-apps/api/core'

export type MarketSimulationScenarioInput = {
  id: string
  label: string
  openGapPct: number
  pctChg: number
  pctChgRelativeToOpen: boolean
  volumeRatio: number
  upperShadowPct: number
  lowerShadowPct: number
}

export type MarketSimulationTriggeredRule = {
  ruleName: string
  ruleScore: number
}

export type MarketSimulationRow = {
  tsCode: string
  name: string
  concept: string
  referenceRank?: number | null
  baseTotalScore?: number | null
  simulatedTotalScore: number
  scoreDelta: number
  strongHold: boolean
  latestPrice?: number | null
  latestChangePct?: number | null
  volumeRatio?: number | null
  pullbackPct?: number | null
  realtimeMatched: boolean
  triggeredRules: MarketSimulationTriggeredRule[]
}

export type MarketSimulationScenarioResult = {
  id: string
  label: string
  openGapPct: number
  pctChg: number
  pctChgRelativeToOpen: boolean
  volumeRatio: number
  upperShadowPct: number
  lowerShadowPct: number
  rows: MarketSimulationRow[]
  matchedCount: number
  strongHoldCount: number
}

export type MarketSimulationPageData = {
  scenarios: MarketSimulationScenarioResult[]
  requestedCount: number
  effectiveCount: number
  fetchedCount: number
  truncated: boolean
  refreshedAt?: string | null
  referenceTradeDate?: string | null
  simulatedTradeDate?: string | null
  sortMode: string
  strongScoreFloor?: number | null
  candidateCount: number
}

export type MarketSimulationRealtimeScenarioQuery = {
  id: string
  pctChgRelativeToOpen: boolean
  pctChg: number
  volumeRatio: number
  upperShadowPct: number
  lowerShadowPct: number
  tsCodes: string[]
}

export type MarketSimulationRealtimeRowData = {
  tsCode: string
  latestPrice?: number | null
  latestChangePct?: number | null
  volumeRatio?: number | null
  pullbackPct?: number | null
  realtimeMatched: boolean
}

export type MarketSimulationRealtimeScenarioResult = {
  id: string
  rows: MarketSimulationRealtimeRowData[]
  matchedCount: number
}

export type MarketSimulationRealtimeRefreshData = {
  scenarios: MarketSimulationRealtimeScenarioResult[]
  requestedCount: number
  effectiveCount: number
  fetchedCount: number
  truncated: boolean
  refreshedAt?: string | null
  quoteTradeDate?: string | null
  quoteTime?: string | null
}

export type MarketSimulationQuery = {
  sourcePath: string
  referenceTradeDate?: string
  topLimit?: number
  board?: string
  scenarios: MarketSimulationScenarioInput[]
  sortMode?: string
  strongScoreFloor?: number
  fetchRealtime?: boolean
}

export async function getMarketSimulationPage(query: MarketSimulationQuery) {
  return invoke<MarketSimulationPageData>('get_market_simulation_page', query)
}

export async function refreshMarketSimulationRealtime(query: {
  sourcePath: string
  scenarios: MarketSimulationRealtimeScenarioQuery[]
}) {
  return invoke<MarketSimulationRealtimeRefreshData>(
    'refresh_market_simulation_realtime',
    query,
  )
}
