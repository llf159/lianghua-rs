import { invoke } from '@tauri-apps/api/core'

export type StrategyPaperValidationDefaultsData = {
  latest_trade_date?: string | null
  start_date?: string | null
  end_date?: string | null
  min_listed_trade_days: number
  index_ts_code: string
  buy_price_basis: string
  slippage_pct: number
}

export type StrategyPaperValidationSummaryData = {
  buy_signal_count: number
  total_trade_count: number
  closed_trade_count: number
  open_trade_count: number
  win_rate?: number | null
  avg_return_pct?: number | null
  avg_hold_days?: number | null
  best_return_pct?: number | null
  worst_return_pct?: number | null
}

export type StrategyPaperValidationTradeRow = {
  ts_code: string
  name?: string | null
  buy_date: string
  sell_date?: string | null
  buy_rank?: number | null
  hold_days: number
  buy_price_basis: string
  buy_basis_price?: number | null
  buy_cost_price?: number | null
  sell_price?: number | null
  open_return_pct?: number | null
  high_return_pct?: number | null
  close_return_pct?: number | null
  realized_return_pct?: number | null
  daily_holding_close_returns: StrategyPaperValidationDailyHoldingCloseReturn[]
  status: string
}

export type StrategyPaperValidationDailyHoldingCloseReturn = {
  trade_date: string
  close_return_pct: number
}

export type StrategyPaperValidationIndexDailyReturn = {
  trade_date: string
  pct_chg: number
}

export type StrategyPaperValidationData = {
  latest_trade_date?: string | null
  start_date: string
  end_date: string
  min_listed_trade_days: number
  index_ts_code: string
  resolved_board?: string | null
  test_ts_code?: string | null
  test_stock_name?: string | null
  buy_price_basis: string
  slippage_pct: number
  buy_expression: string
  sell_expression: string
  summary: StrategyPaperValidationSummaryData
  trades: StrategyPaperValidationTradeRow[]
  index_daily_returns: StrategyPaperValidationIndexDailyReturn[]
}

export type StrategyPaperValidationTemplateValidationData = {
  normalized_buy_expression?: string
  normalized_sell_expression?: string
  buy_warmup_need: number
  sell_warmup_need: number
  warmup_need: number
  message: string
}

export type StrategyPaperValidationQuery = {
  sourcePath: string
  startDate?: string
  endDate?: string
  minListedTradeDays?: number
  indexTsCode?: string
  testTsCode?: string
  board?: string
  buyPriceBasis: string
  slippagePct?: number
  buyExpression: string
  sellExpression: string
}

export async function getStrategyPaperValidationDefaults(sourcePath: string) {
  return invoke<StrategyPaperValidationDefaultsData>('get_strategy_paper_validation_defaults', {
    sourcePath,
  })
}

export async function runStrategyPaperValidation(query: StrategyPaperValidationQuery) {
  return invoke<StrategyPaperValidationData>('run_strategy_paper_validation', query)
}

export async function validateStrategyPaperValidationTemplateExpressions(
  buyExpression: string,
  sellExpression: string,
) {
  return invoke<StrategyPaperValidationTemplateValidationData>(
    'validate_strategy_paper_validation_template_expressions',
    {
      buyExpression,
      sellExpression,
    },
  )
}
