import { invoke } from '@tauri-apps/api/core'

export type StrategyHeatmapCell = {
  trade_date: string
  day_level?: number | null
  avg_level?: number | null
  delta_level?: number | null
  above_avg?: boolean | null
}

export type StrategyOverviewPayload = {
  items?: StrategyHeatmapCell[]
  latest_trade_date?: string | null
  average_level?: number | null
}

export type StrategyDailyRow = {
  trade_date: string
  rule_name: string
  trigger_mode?: string | null
  sample_count?: number | null
  trigger_count?: number | null
  coverage?: number | null
  contribution_score?: number | null
  contribution_per_trigger?: number | null
  median_trigger_count?: number | null
  top100_trigger_count?: number | null
  best_rank?: number | null
}

export type StrategyChartPoint = {
  trade_date: string
  trigger_count?: number | null
  top100_trigger_count?: number | null
  coverage?: number | null
}

export type StrategyChartPayload = {
  items?: StrategyChartPoint[]
}

export type TriggeredStockRow = {
  rank?: number | null
  ts_code: string
  name?: string | null
  total_score?: number | null
  rule_score?: number | null
  concept?: string | null
}

export type StrategyStatisticsPageData = {
  overview?: StrategyOverviewPayload | null
  detail_rows?: StrategyDailyRow[]
  strategy_options?: string[]
  resolved_strategy_name?: string | null
  analysis_trade_date_options?: string[]
  resolved_analysis_trade_date?: string | null
  chart?: StrategyChartPayload | null
  triggered_stocks?: TriggeredStockRow[]
}

export type StrategyStatisticsDetailData = {
  strategy_name: string
  analysis_trade_date_options: string[]
  resolved_analysis_trade_date?: string | null
  selected_daily_row?: StrategyDailyRow | null
  chart?: StrategyChartPayload | null
  triggered_stocks: TriggeredStockRow[]
}

export type StrategyStatisticsQuery = {
  sourcePath: string
  strategyName?: string
  analysisTradeDate?: string
}

export type StrategyTriggeredStocksQuery = {
  sourcePath: string
  strategyName: string
  analysisTradeDate: string
}

export async function getStrategyStatisticsPage(query: StrategyStatisticsQuery) {
  return invoke<StrategyStatisticsPageData>('get_strategy_statistics_page', query)
}

export async function getStrategyStatisticsDetail(query: {
  sourcePath: string
  strategyName: string
  analysisTradeDate?: string
}) {
  return invoke<StrategyStatisticsDetailData>('get_strategy_statistics_detail', query)
}

export async function getStrategyTriggeredStocks(query: StrategyTriggeredStocksQuery) {
  return invoke<TriggeredStockRow[]>('get_strategy_triggered_stocks', query)
}

export type SceneLayerStateAvgResidualReturn = {
  scene_state: string
  avg_residual_return?: number | null
}

export type SceneLayerPoint = {
  trade_date: string
  state_avg_residual_returns: SceneLayerStateAvgResidualReturn[]
  top_bottom_spread?: number | null
  ic?: number | null
}

export type SceneLayerBacktestData = {
  scene_name: string
  stock_adj_type: string
  index_ts_code: string
  index_beta: number
  concept_beta: number
  start_date: string
  end_date: string
  min_samples_per_scene_day: number
  points: SceneLayerPoint[]
  spread_mean?: number | null
  ic_mean?: number | null
  ic_std?: number | null
  icir?: number | null
}

export type SceneLayerBacktestDefaultsData = {
  scene_options: string[]
  resolved_scene_name?: string | null
  start_date?: string | null
  end_date?: string | null
}

export type SceneLayerBacktestQuery = {
  sourcePath: string
  sceneName: string
  stockAdjType?: string
  indexTsCode: string
  indexBeta?: number
  conceptBeta?: number
  startDate: string
  endDate: string
  minSamplesPerSceneDay?: number
}

export async function getSceneLayerBacktestDefaults(sourcePath: string) {
  return invoke<SceneLayerBacktestDefaultsData>('get_scene_layer_backtest_defaults', { sourcePath })
}

export async function runSceneLayerBacktest(query: SceneLayerBacktestQuery) {
  return invoke<SceneLayerBacktestData>('run_scene_layer_backtest', query)
}
