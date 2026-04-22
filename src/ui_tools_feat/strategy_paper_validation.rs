use std::collections::HashMap;

use chrono::{Days, NaiveDate};
use duckdb::{params, Connection};
use rayon::prelude::*;
use serde::Serialize;

use crate::{
    data::scoring_data::row_into_rt,
    data::{load_stock_list, load_trade_date_list, result_db_path, stock_list_path, DataReader},
    expr::{
        eval::{Runtime, Value},
        parser::{lex_all, Expr, Parser, Stmt, Stmts},
    },
    scoring::tools::{
        calc_query_need_rows, calc_query_start_date, inject_stock_extra_fields, load_st_list,
        rt_max_len,
    },
    simulate::DEFAULT_BACKTEST_MIN_LISTED_TRADE_DAYS,
    ui_tools_feat::watch_observe::normalize_ts_code,
    utils::utils::{board_category, eval_binary_for_warmup, impl_expr_warmup},
};

use super::build_name_map;

const DEFAULT_ADJ_TYPE: &str = "qfq";
const DEFAULT_INDEX_TS_CODE: &str = "000001.SH";
const DEFAULT_BUY_PRICE_BASIS: &str = "open";
const PRICE_EPS: f64 = 1e-12;
const PAPER_VALIDATION_INPUT_KEYS: [&str; 10] = [
    "O",
    "H",
    "L",
    "C",
    "V",
    "AMOUNT",
    "PRE_CLOSE",
    "CHANGE",
    "PCT_CHG",
    "TURNOVER_RATE",
];

#[derive(Debug, Clone, Serialize)]
pub struct StrategyPaperValidationDefaultsData {
    pub latest_trade_date: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub min_listed_trade_days: usize,
    pub index_ts_code: String,
    pub buy_price_basis: String,
    pub slippage_pct: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyPaperValidationSummaryData {
    pub buy_signal_count: usize,
    pub total_trade_count: usize,
    pub closed_trade_count: usize,
    pub open_trade_count: usize,
    pub win_rate: Option<f64>,
    pub avg_return_pct: Option<f64>,
    pub avg_hold_days: Option<f64>,
    pub best_return_pct: Option<f64>,
    pub worst_return_pct: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyPaperValidationTradeRow {
    pub ts_code: String,
    pub name: Option<String>,
    pub buy_date: String,
    pub sell_date: Option<String>,
    pub buy_rank: Option<i64>,
    pub hold_days: usize,
    pub buy_price_basis: String,
    pub buy_basis_price: Option<f64>,
    pub buy_cost_price: Option<f64>,
    pub sell_price: Option<f64>,
    pub open_return_pct: Option<f64>,
    pub high_return_pct: Option<f64>,
    pub close_return_pct: Option<f64>,
    pub realized_return_pct: Option<f64>,
    pub status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyPaperValidationData {
    pub latest_trade_date: Option<String>,
    pub start_date: String,
    pub end_date: String,
    pub min_listed_trade_days: usize,
    pub index_ts_code: String,
    pub resolved_board: Option<String>,
    pub test_ts_code: Option<String>,
    pub test_stock_name: Option<String>,
    pub buy_price_basis: String,
    pub slippage_pct: f64,
    pub buy_expression: String,
    pub sell_expression: String,
    pub summary: StrategyPaperValidationSummaryData,
    pub trades: Vec<StrategyPaperValidationTradeRow>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyPaperValidationTemplateValidationData {
    pub normalized_buy_expression: String,
    pub normalized_sell_expression: String,
    pub buy_warmup_need: usize,
    pub sell_warmup_need: usize,
    pub warmup_need: usize,
    pub message: String,
}

#[derive(Debug, Clone, Copy)]
enum BuyPriceBasis {
    Open,
    Close,
}

#[derive(Debug, Clone, Copy)]
enum TradeDateResolveMode {
    Start,
    End,
}

impl BuyPriceBasis {
    fn parse(raw: &str) -> Result<Self, String> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "open" => Ok(Self::Open),
            "close" => Ok(Self::Close),
            other => Err(format!("不支持的买点基准: {other}")),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Close => "close",
        }
    }
}

#[derive(Debug, Clone)]
struct OpenPosition {
    buy_date: String,
    buy_index: usize,
    buy_rank: Option<i64>,
    buy_basis_price: f64,
    buy_cost_price: f64,
    last_sell_runtime_index: usize,
    pending_sell: bool,
    sell_runtime: Runtime,
}

#[derive(Debug, Clone, Default)]
struct PaperTradeEligibility {
    trade_date_to_index: HashMap<String, usize>,
    listed_trade_index_by_ts: HashMap<String, usize>,
    min_listed_trade_days: usize,
}

impl PaperTradeEligibility {
    fn allows_buy(&self, ts_code: &str, trade_date: &str) -> bool {
        if self.min_listed_trade_days == 0 {
            return true;
        }

        let Some(sample_index) = self.trade_date_to_index.get(trade_date).copied() else {
            return false;
        };
        let Some(listed_index) = self.listed_trade_index_by_ts.get(ts_code).copied() else {
            return true;
        };

        sample_index + 1 >= listed_index + self.min_listed_trade_days
    }
}

pub fn get_strategy_paper_validation_defaults(
    source_path: &str,
) -> Result<StrategyPaperValidationDefaultsData, String> {
    let source_path = source_path.trim();
    if source_path.is_empty() {
        return Err("source_path 不能为空".to_string());
    }

    let trade_date_options = load_trade_date_options(source_path)?;
    let latest_trade_date = trade_date_options.last().cloned();
    let end_date = latest_trade_date.clone();
    let start_date = end_date
        .as_deref()
        .and_then(|value| resolve_one_year_earlier_trade_date(&trade_date_options, value));

    Ok(StrategyPaperValidationDefaultsData {
        latest_trade_date,
        start_date,
        end_date,
        min_listed_trade_days: DEFAULT_BACKTEST_MIN_LISTED_TRADE_DAYS,
        index_ts_code: DEFAULT_INDEX_TS_CODE.to_string(),
        buy_price_basis: DEFAULT_BUY_PRICE_BASIS.to_string(),
        slippage_pct: 0.0,
    })
}

pub fn run_strategy_paper_validation(
    source_path: &str,
    start_date: Option<String>,
    end_date: Option<String>,
    min_listed_trade_days: Option<usize>,
    index_ts_code: Option<String>,
    test_ts_code: Option<String>,
    board: Option<String>,
    buy_price_basis: String,
    slippage_pct: Option<f64>,
    buy_expression: String,
    sell_expression: String,
) -> Result<StrategyPaperValidationData, String> {
    let source_path = source_path.trim();
    if source_path.is_empty() {
        return Err("source_path 不能为空".to_string());
    }

    let defaults = get_strategy_paper_validation_defaults(source_path)?;
    let trade_date_options = load_trade_date_options(source_path)?;
    let resolved_end_date = normalize_trade_date_input(
        &trade_date_options,
        end_date,
        defaults.end_date.clone(),
        "结束日期",
        TradeDateResolveMode::End,
    )?;
    let resolved_start_date = normalize_trade_date_input(
        &trade_date_options,
        start_date,
        defaults
            .start_date
            .clone()
            .or_else(|| defaults.end_date.clone()),
        "开始日期",
        TradeDateResolveMode::Start,
    )?;

    if resolved_start_date > resolved_end_date {
        return Err("开始日期不能晚于结束日期".to_string());
    }

    let buy_expression = buy_expression.trim().to_string();
    if buy_expression.is_empty() {
        return Err("买点方程不能为空".to_string());
    }
    let sell_expression = sell_expression.trim().to_string();
    if sell_expression.is_empty() {
        return Err("卖点方程不能为空".to_string());
    }

    let parsed_buy_price_basis = BuyPriceBasis::parse(&buy_price_basis)?;
    let resolved_slippage_pct = slippage_pct.unwrap_or(0.0);
    if !resolved_slippage_pct.is_finite() {
        return Err("滑点系数必须是有限数字".to_string());
    }
    let normalized_test_ts_code = test_ts_code
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            normalize_ts_code(value)
                .ok_or_else(|| "测试股票格式无效，请输入 6 位代码或标准 ts_code".to_string())
        })
        .transpose()?;
    let resolved_board = board
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    let buy_program = parse_expression_program(&buy_expression, "买点方程")?;
    let sell_program = parse_expression_program(&sell_expression, "卖点方程")?;
    let warmup_need =
        estimate_expression_warmup(&buy_program)?.max(estimate_expression_warmup(&sell_program)?);
    let query_start_date = calc_query_start_date(source_path, warmup_need, &resolved_start_date)?;
    let need_rows = calc_query_need_rows(
        source_path,
        warmup_need,
        &resolved_start_date,
        &resolved_end_date,
    )?;

    let reader = DataReader::new(source_path)?;
    let st_list = load_st_list(source_path)?;
    let name_map = build_name_map(source_path).unwrap_or_default();
    let ts_codes = if let Some(ts_code) = normalized_test_ts_code.as_ref() {
        vec![ts_code.clone()]
    } else {
        DataReader::list_ts_code(
            &reader,
            DEFAULT_ADJ_TYPE,
            &resolved_start_date,
            &resolved_end_date,
        )?
    };
    let ts_codes = ts_codes
        .into_iter()
        .filter(|ts_code| {
            let Some(selected_board) = resolved_board.as_deref() else {
                return true;
            };
            board_category(ts_code, name_map.get(ts_code).map(|value| value.as_str()))
                == selected_board
        })
        .collect::<Vec<_>>();
    let test_stock_name = normalized_test_ts_code
        .as_ref()
        .and_then(|ts_code| name_map.get(ts_code).cloned());
    let eligibility = build_paper_trade_eligibility(
        source_path,
        min_listed_trade_days.unwrap_or(DEFAULT_BACKTEST_MIN_LISTED_TRADE_DAYS),
    )?;
    let rank_series_map = load_rank_series_map(source_path, &query_start_date, &resolved_end_date);

    let grouped_rows = ts_codes
        .par_chunks(128)
        .map(
            |ts_group| -> Result<Vec<StrategyPaperValidationTradeRow>, String> {
                let worker_buy_program = buy_program.clone();
                let worker_sell_program = sell_program.clone();
                let worker_reader = DataReader::new(source_path)?;
                let mut out = Vec::new();

                for ts_code in ts_group {
                    let mut rows = simulate_one_stock_trades(
                        &worker_reader,
                        ts_code,
                        name_map.get(ts_code),
                        st_list.contains(ts_code),
                        &worker_buy_program,
                        &worker_sell_program,
                        &eligibility,
                        &rank_series_map,
                        &resolved_start_date,
                        &resolved_end_date,
                        need_rows,
                        parsed_buy_price_basis,
                        resolved_slippage_pct,
                    )?;
                    out.append(&mut rows);
                }

                Ok(out)
            },
        )
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    let mut trades = grouped_rows;
    trades.sort_by(|left, right| {
        left.buy_date
            .cmp(&right.buy_date)
            .then_with(|| left.ts_code.cmp(&right.ts_code))
            .then_with(|| left.sell_date.cmp(&right.sell_date))
    });

    let summary = build_trade_summary(&trades);

    Ok(StrategyPaperValidationData {
        latest_trade_date: defaults.latest_trade_date,
        start_date: resolved_start_date,
        end_date: resolved_end_date,
        min_listed_trade_days: eligibility.min_listed_trade_days,
        index_ts_code: index_ts_code
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| DEFAULT_INDEX_TS_CODE.to_string()),
        resolved_board,
        test_ts_code: normalized_test_ts_code,
        test_stock_name,
        buy_price_basis: parsed_buy_price_basis.as_str().to_string(),
        slippage_pct: resolved_slippage_pct,
        buy_expression,
        sell_expression,
        summary,
        trades,
    })
}

pub fn validate_strategy_paper_validation_template_expressions(
    buy_expression: String,
    sell_expression: String,
) -> Result<StrategyPaperValidationTemplateValidationData, String> {
    let normalized_buy_expression = buy_expression.trim().to_string();
    if normalized_buy_expression.is_empty() {
        return Err("买点方程不能为空".to_string());
    }

    let normalized_sell_expression = sell_expression.trim().to_string();
    if normalized_sell_expression.is_empty() {
        return Err("卖点方程不能为空".to_string());
    }

    let buy_program = parse_expression_program(&normalized_buy_expression, "买点方程")?;
    let sell_program = parse_expression_program(&normalized_sell_expression, "卖点方程")?;
    let buy_warmup_need = estimate_expression_warmup(&buy_program)?;
    let sell_warmup_need = estimate_expression_warmup(&sell_program)?;

    let mut buy_runtime = build_template_validation_runtime(buy_warmup_need, false);
    evaluate_program_as_bool_series(&mut buy_runtime, &buy_program)
        .map_err(|error| format!("买点方程运行失败: {error}"))?;

    let mut sell_runtime = build_template_validation_runtime(sell_warmup_need, true);
    evaluate_program_as_bool_series(&mut sell_runtime, &sell_program)
        .map_err(|error| format!("卖点方程运行失败: {error}"))?;

    Ok(StrategyPaperValidationTemplateValidationData {
        normalized_buy_expression,
        normalized_sell_expression,
        buy_warmup_need,
        sell_warmup_need,
        warmup_need: buy_warmup_need.max(sell_warmup_need),
        message: "买点/卖点方程可用于策略模拟盘验证".to_string(),
    })
}

fn build_template_validation_runtime(warmup_need: usize, include_sell_fields: bool) -> Runtime {
    let len = (warmup_need + 2).max(8);
    let mut vars = HashMap::with_capacity(PAPER_VALIDATION_INPUT_KEYS.len() + 10);

    for key in PAPER_VALIDATION_INPUT_KEYS {
        let series = (0..len)
            .map(|index| Some(index as f64 + 1.0))
            .collect::<Vec<_>>();
        vars.insert(key.to_string(), Value::NumSeries(series));
    }

    let rank_series = (0..len)
        .map(|index| Some((index + 1) as f64))
        .collect::<Vec<_>>();
    vars.insert("RANK".to_string(), Value::NumSeries(rank_series.clone()));
    vars.insert("rank".to_string(), Value::NumSeries(rank_series));
    vars.insert(
        "ZHANG".to_string(),
        Value::NumSeries(vec![Some(0.095); len]),
    );
    vars.insert(
        "TOTAL_MV_YI".to_string(),
        Value::NumSeries(vec![Some(100.0); len]),
    );

    if include_sell_fields {
        let time_series = (0..len).map(|index| Some(index as f64)).collect::<Vec<_>>();
        let rateo_series = (0..len)
            .map(|index| Some(index as f64 - 2.0))
            .collect::<Vec<_>>();
        let rateh_series = (0..len)
            .map(|index| Some(index as f64 + 1.0))
            .collect::<Vec<_>>();
        vars.insert("TIME".to_string(), Value::NumSeries(time_series.clone()));
        vars.insert("time".to_string(), Value::NumSeries(time_series));
        vars.insert("RATEO".to_string(), Value::NumSeries(rateo_series.clone()));
        vars.insert("rateo".to_string(), Value::NumSeries(rateo_series));
        vars.insert("RATEH".to_string(), Value::NumSeries(rateh_series.clone()));
        vars.insert("rateh".to_string(), Value::NumSeries(rateh_series));
    }

    Runtime { vars }
}

fn build_trade_summary(
    trades: &[StrategyPaperValidationTradeRow],
) -> StrategyPaperValidationSummaryData {
    let mut closed_trade_count = 0usize;
    let mut open_trade_count = 0usize;
    let mut win_count = 0usize;
    let mut return_sum = 0.0;
    let mut return_count = 0usize;
    let mut hold_days_sum = 0usize;
    let mut best_return: Option<f64> = None;
    let mut worst_return: Option<f64> = None;

    for trade in trades {
        if trade.status == "open" {
            open_trade_count += 1;
            continue;
        }
        if trade.status != "closed" {
            continue;
        }
        closed_trade_count += 1;
        let Some(return_pct) = trade.realized_return_pct else {
            continue;
        };
        if return_pct > 0.0 {
            win_count += 1;
        }
        return_sum += return_pct;
        return_count += 1;
        hold_days_sum += trade.hold_days;
        best_return = Some(best_return.map_or(return_pct, |value| value.max(return_pct)));
        worst_return = Some(worst_return.map_or(return_pct, |value| value.min(return_pct)));
    }

    StrategyPaperValidationSummaryData {
        buy_signal_count: trades.len(),
        total_trade_count: trades.len(),
        closed_trade_count,
        open_trade_count,
        win_rate: if return_count == 0 {
            None
        } else {
            Some(win_count as f64 / return_count as f64)
        },
        avg_return_pct: if return_count == 0 {
            None
        } else {
            Some(return_sum / return_count as f64)
        },
        avg_hold_days: if return_count == 0 {
            None
        } else {
            Some(hold_days_sum as f64 / return_count as f64)
        },
        best_return_pct: best_return,
        worst_return_pct: worst_return,
    }
}

#[allow(clippy::too_many_arguments)]
fn simulate_one_stock_trades(
    reader: &DataReader,
    ts_code: &str,
    stock_name: Option<&String>,
    is_st: bool,
    buy_program: &Stmts,
    sell_program: &Stmts,
    eligibility: &PaperTradeEligibility,
    rank_series_map: &HashMap<String, HashMap<String, Option<f64>>>,
    start_date: &str,
    end_date: &str,
    need_rows: usize,
    buy_price_basis: BuyPriceBasis,
    slippage_pct: f64,
) -> Result<Vec<StrategyPaperValidationTradeRow>, String> {
    let mut row_data = reader.load_one_tail_rows(ts_code, DEFAULT_ADJ_TYPE, end_date, need_rows)?;
    if row_data.trade_dates.is_empty() {
        return Ok(Vec::new());
    }

    inject_stock_extra_fields(&mut row_data, ts_code, is_st, None)?;
    inject_runtime_rank_series(&mut row_data, ts_code, rank_series_map)?;

    let trade_dates = std::mem::take(&mut row_data.trade_dates);
    let keep_from = trade_dates
        .binary_search_by(|value| value.as_str().cmp(start_date))
        .unwrap_or_else(|index| index);
    if keep_from >= trade_dates.len() {
        return Ok(Vec::new());
    }

    let base_runtime = row_into_rt(row_data)?;
    simulate_trade_rows_from_runtime(
        ts_code,
        stock_name,
        &trade_dates,
        &base_runtime,
        buy_program,
        sell_program,
        eligibility,
        keep_from,
        buy_price_basis,
        slippage_pct,
    )
}

#[allow(clippy::too_many_arguments)]
fn simulate_trade_rows_from_runtime(
    ts_code: &str,
    stock_name: Option<&String>,
    trade_dates: &[String],
    base_runtime: &Runtime,
    buy_program: &Stmts,
    sell_program: &Stmts,
    eligibility: &PaperTradeEligibility,
    keep_from: usize,
    buy_price_basis: BuyPriceBasis,
    slippage_pct: f64,
) -> Result<Vec<StrategyPaperValidationTradeRow>, String> {
    let mut buy_runtime = base_runtime.clone();
    let buy_signal_series = evaluate_program_as_bool_series(&mut buy_runtime, buy_program)
        .map_err(|error| format!("{ts_code} 买点方程执行失败: {error}"))?;
    let open_series =
        runtime_num_series(base_runtime, "O").map_err(|error| format!("{ts_code} {error}"))?;
    let high_series =
        runtime_num_series(base_runtime, "H").map_err(|error| format!("{ts_code} {error}"))?;
    let close_series =
        runtime_num_series(base_runtime, "C").map_err(|error| format!("{ts_code} {error}"))?;
    let pre_close_series = runtime_num_series(base_runtime, "PRE_CLOSE")
        .map_err(|error| format!("{ts_code} {error}"))?;
    let zhang_series =
        runtime_num_series(base_runtime, "ZHANG").map_err(|error| format!("{ts_code} {error}"))?;
    let rank_series = runtime_num_series_optional(base_runtime, "RANK");

    let mut open_positions = Vec::new();
    let mut trades = Vec::new();

    for scan_index in keep_from..trade_dates.len() {
        let trade_date = &trade_dates[scan_index];

        if open_positions.is_empty()
            && buy_signal_series.get(scan_index).copied().unwrap_or(false)
            && eligibility.allows_buy(ts_code, trade_date)
        {
            let basis_price = match buy_price_basis {
                BuyPriceBasis::Open => open_series.get(scan_index).copied().flatten(),
                BuyPriceBasis::Close => close_series.get(scan_index).copied().flatten(),
            };
            if let Some(basis_price) = normalize_valid_price(basis_price) {
                let pre_close = pre_close_series.get(scan_index).copied().flatten();
                let zhang_pct = zhang_series.get(scan_index).copied().flatten();
                if is_buy_basis_executable(basis_price, pre_close, zhang_pct) {
                    let buy_cost_price = basis_price * (1.0 + slippage_pct / 100.0);
                    if buy_cost_price.is_finite() && buy_cost_price > PRICE_EPS {
                        let sell_runtime = init_sell_runtime(
                            base_runtime,
                            trade_dates.len(),
                            open_series,
                            high_series,
                            scan_index,
                            buy_cost_price,
                        )?;
                        open_positions.push(OpenPosition {
                            buy_date: trade_date.clone(),
                            buy_index: scan_index,
                            buy_rank: rank_series
                                .and_then(|series| series.get(scan_index))
                                .copied()
                                .flatten()
                                .map(|value| value.round() as i64),
                            buy_basis_price: basis_price,
                            buy_cost_price,
                            last_sell_runtime_index: scan_index,
                            pending_sell: false,
                            sell_runtime,
                        });
                    }
                }
            }
        }

        if open_positions.is_empty() {
            continue;
        }

        let mut remaining_positions = Vec::with_capacity(open_positions.len());
        for mut position in open_positions.drain(..) {
            if position.last_sell_runtime_index < scan_index {
                extend_sell_runtime_series(
                    &mut position.sell_runtime,
                    open_series,
                    high_series,
                    position.buy_index,
                    position.buy_cost_price,
                    position.last_sell_runtime_index + 1,
                    scan_index,
                )?;
                position.last_sell_runtime_index = scan_index;
            }

            let sell_hit = if position.pending_sell {
                true
            } else {
                evaluate_sell_hit(&mut position.sell_runtime, sell_program, scan_index)
                    .map_err(|error| format!("{ts_code} 卖点方程执行失败: {error}"))?
            };
            let sell_price = resolve_normal_sell_price(
                close_series.get(scan_index).copied().flatten(),
                open_series.get(scan_index).copied().flatten(),
            );
            let pre_close = pre_close_series.get(scan_index).copied().flatten();
            let zhang_pct = zhang_series.get(scan_index).copied().flatten();
            let sell_executable = is_sell_price_executable(sell_price, pre_close, zhang_pct);

            if sell_hit && sell_executable {
                let open_return_pct = calc_return_pct(
                    open_series.get(scan_index).copied().flatten(),
                    position.buy_cost_price,
                );
                let high_return_pct = calc_return_pct(
                    high_series.get(scan_index).copied().flatten(),
                    position.buy_cost_price,
                );
                let close_return_pct = calc_return_pct(
                    close_series.get(scan_index).copied().flatten(),
                    position.buy_cost_price,
                );

                trades.push(StrategyPaperValidationTradeRow {
                    ts_code: ts_code.to_string(),
                    name: stock_name.cloned(),
                    buy_date: position.buy_date,
                    sell_date: Some(trade_date.clone()),
                    buy_rank: position.buy_rank,
                    hold_days: scan_index.saturating_sub(position.buy_index),
                    buy_price_basis: buy_price_basis.as_str().to_string(),
                    buy_basis_price: Some(position.buy_basis_price),
                    buy_cost_price: Some(position.buy_cost_price),
                    sell_price,
                    open_return_pct,
                    high_return_pct,
                    close_return_pct,
                    realized_return_pct: calc_return_pct(sell_price, position.buy_cost_price),
                    status: "closed".to_string(),
                });
            } else {
                if sell_hit {
                    position.pending_sell = true;
                }
                remaining_positions.push(position);
            }
        }
        open_positions = remaining_positions;
    }

    let last_index = trade_dates.len().saturating_sub(1);
    for position in open_positions {
        trades.push(StrategyPaperValidationTradeRow {
            ts_code: ts_code.to_string(),
            name: stock_name.cloned(),
            buy_date: position.buy_date,
            sell_date: None,
            buy_rank: position.buy_rank,
            hold_days: last_index.saturating_sub(position.buy_index),
            buy_price_basis: buy_price_basis.as_str().to_string(),
            buy_basis_price: Some(position.buy_basis_price),
            buy_cost_price: Some(position.buy_cost_price),
            sell_price: None,
            open_return_pct: calc_return_pct(
                open_series.get(last_index).copied().flatten(),
                position.buy_cost_price,
            ),
            high_return_pct: calc_return_pct(
                high_series.get(last_index).copied().flatten(),
                position.buy_cost_price,
            ),
            close_return_pct: calc_return_pct(
                close_series.get(last_index).copied().flatten(),
                position.buy_cost_price,
            ),
            realized_return_pct: calc_return_pct(
                close_series.get(last_index).copied().flatten(),
                position.buy_cost_price,
            ),
            status: "open".to_string(),
        });
    }

    Ok(trades)
}

fn evaluate_sell_hit(
    runtime: &mut Runtime,
    sell_program: &Stmts,
    scan_index: usize,
) -> Result<bool, String> {
    let value = runtime
        .eval_program(sell_program)
        .map_err(|error| format!("表达式计算错误: {}", error.msg))?;
    let len = rt_max_len(&runtime);
    let bool_series = Value::as_bool_series(&value, len)
        .map_err(|error| format!("表达式返回值非布尔: {}", error.msg))?;
    Ok(bool_series.get(scan_index).copied().unwrap_or(false))
}

fn init_sell_runtime(
    base_runtime: &Runtime,
    len: usize,
    open_series: &[Option<f64>],
    high_series: &[Option<f64>],
    buy_index: usize,
    buy_cost_price: f64,
) -> Result<Runtime, String> {
    let mut runtime = base_runtime.clone();
    runtime
        .vars
        .insert("TIME".to_string(), Value::NumSeries(vec![None; len]));
    runtime
        .vars
        .insert("time".to_string(), Value::NumSeries(vec![None; len]));
    runtime
        .vars
        .insert("RATEO".to_string(), Value::NumSeries(vec![None; len]));
    runtime
        .vars
        .insert("rateo".to_string(), Value::NumSeries(vec![None; len]));
    runtime
        .vars
        .insert("RATEH".to_string(), Value::NumSeries(vec![None; len]));
    runtime
        .vars
        .insert("rateh".to_string(), Value::NumSeries(vec![None; len]));
    extend_sell_runtime_series(
        &mut runtime,
        open_series,
        high_series,
        buy_index,
        buy_cost_price,
        buy_index,
        buy_index,
    )?;
    Ok(runtime)
}

fn extend_sell_runtime_series(
    runtime: &mut Runtime,
    open_series: &[Option<f64>],
    high_series: &[Option<f64>],
    buy_index: usize,
    buy_cost_price: f64,
    update_from_index: usize,
    scan_index: usize,
) -> Result<(), String> {
    let len = open_series.len().max(high_series.len());
    if buy_index >= len || scan_index >= len {
        return Err("卖点扫描索引越界".to_string());
    }
    let start_index = update_from_index.max(buy_index);
    if start_index > scan_index {
        return Ok(());
    }

    for index in start_index..=scan_index {
        let time_value = Some((index - buy_index) as f64);
        let rateo_value =
            calc_return_pct(open_series.get(index).copied().flatten(), buy_cost_price);
        let rateh_value =
            calc_return_pct(high_series.get(index).copied().flatten(), buy_cost_price);
        set_runtime_num_series_value(runtime, "TIME", index, time_value)?;
        set_runtime_num_series_value(runtime, "time", index, time_value)?;
        set_runtime_num_series_value(runtime, "RATEO", index, rateo_value)?;
        set_runtime_num_series_value(runtime, "rateo", index, rateo_value)?;
        set_runtime_num_series_value(runtime, "RATEH", index, rateh_value)?;
        set_runtime_num_series_value(runtime, "rateh", index, rateh_value)?;
    }
    Ok(())
}

fn resolve_limit_up_price(pre_close: Option<f64>, zhang_pct: Option<f64>) -> Option<f64> {
    let pre_close = normalize_valid_price(pre_close)?;
    let zhang_pct = zhang_pct.filter(|value| value.is_finite() && *value >= 0.0)?;
    let limit_up = pre_close * (1.0 + zhang_pct);
    if limit_up.is_finite() && limit_up > PRICE_EPS {
        Some(limit_up)
    } else {
        None
    }
}

fn resolve_limit_down_price(pre_close: Option<f64>, zhang_pct: Option<f64>) -> Option<f64> {
    let pre_close = normalize_valid_price(pre_close)?;
    let zhang_pct = zhang_pct.filter(|value| value.is_finite() && *value >= 0.0)?;
    let limit_down = pre_close * (1.0 - zhang_pct);
    if limit_down.is_finite() && limit_down > PRICE_EPS {
        Some(limit_down)
    } else {
        None
    }
}

fn is_buy_basis_executable(
    basis_price: f64,
    pre_close: Option<f64>,
    zhang_pct: Option<f64>,
) -> bool {
    resolve_limit_up_price(pre_close, zhang_pct)
        .map(|limit_up| basis_price < limit_up - PRICE_EPS)
        .unwrap_or(true)
}

fn resolve_normal_sell_price(close_price: Option<f64>, open_price: Option<f64>) -> Option<f64> {
    normalize_valid_price(close_price).or_else(|| normalize_valid_price(open_price))
}

fn is_sell_price_executable(
    sell_price: Option<f64>,
    pre_close: Option<f64>,
    zhang_pct: Option<f64>,
) -> bool {
    let Some(sell_price) = sell_price else {
        return false;
    };

    resolve_limit_down_price(pre_close, zhang_pct)
        .map(|limit_down| sell_price > limit_down + PRICE_EPS)
        .unwrap_or(true)
}

fn calc_return_pct(price: Option<f64>, buy_cost_price: f64) -> Option<f64> {
    let price = normalize_valid_price(price)?;
    if !buy_cost_price.is_finite() || buy_cost_price.abs() < PRICE_EPS {
        return None;
    }
    Some((price / buy_cost_price - 1.0) * 100.0)
}

fn normalize_valid_price(price: Option<f64>) -> Option<f64> {
    let value = price?;
    if value.is_finite() && value > PRICE_EPS {
        Some(value)
    } else {
        None
    }
}

fn parse_expression_program(expression: &str, label: &str) -> Result<Stmts, String> {
    let tokens = lex_all(expression);
    let mut parser = Parser::new(tokens);
    parser
        .parse_main()
        .map_err(|error| format!("{label}解析错误在{}:{}", error.idx, error.msg))
}

fn evaluate_program_as_bool_series(
    runtime: &mut Runtime,
    program: &Stmts,
) -> Result<Vec<bool>, String> {
    let value = runtime
        .eval_program(program)
        .map_err(|error| format!("表达式计算错误: {}", error.msg))?;
    let len = rt_max_len(&runtime);
    Value::as_bool_series(&value, len).map_err(|error| format!("表达式返回值非布尔: {}", error.msg))
}

fn estimate_expression_warmup(stmts: &Stmts) -> Result<usize, String> {
    let mut locals = HashMap::new();
    let mut consts: HashMap<String, usize> = HashMap::new();
    let mut expr_need = 0usize;

    for stmt in stmts.item.clone() {
        match stmt {
            Stmt::Assign { name, value } => match value {
                Expr::Number(value) => {
                    if value < 0.0 {
                        return Err("表达式常量赋值结果不能为负数".to_string());
                    }
                    consts.insert(name, value as usize);
                }
                Expr::Binary { op, lhs, rhs } => {
                    if let Some(out) = eval_binary_for_warmup(&op, &lhs, &rhs, &consts)? {
                        consts.insert(name, out as usize);
                    } else {
                        let need =
                            impl_expr_warmup(Expr::Binary { op, lhs, rhs }, &locals, &consts)?;
                        locals.insert(name, need);
                    }
                }
                other => {
                    let need = impl_expr_warmup(other, &locals, &consts)?;
                    locals.insert(name, need);
                }
            },
            Stmt::Expr(expr) => {
                expr_need = expr_need.max(impl_expr_warmup(expr, &locals, &consts)?);
            }
        }
    }

    Ok(expr_need)
}

fn build_paper_trade_eligibility(
    source_path: &str,
    min_listed_trade_days: usize,
) -> Result<PaperTradeEligibility, String> {
    if min_listed_trade_days == 0 {
        return Ok(PaperTradeEligibility::default());
    }

    let trade_dates = load_trade_date_list(source_path)?;
    let mut trade_date_to_index = HashMap::with_capacity(trade_dates.len());
    for (index, trade_date) in trade_dates.iter().enumerate() {
        trade_date_to_index.insert(trade_date.clone(), index);
    }

    let stock_list = stock_list_path(source_path);
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(&stock_list)
        .map_err(|e| format!("打开stock_list.csv失败:路径:{:?},错误:{e}", stock_list))?;
    let headers = reader
        .headers()
        .map_err(|e| format!("读取stock_list.csv表头失败:{e}"))?
        .iter()
        .map(|value| value.trim().to_string())
        .collect::<Vec<_>>();

    let Some(ts_code_index) = headers
        .iter()
        .position(|header| header.eq_ignore_ascii_case("ts_code"))
    else {
        return Ok(PaperTradeEligibility {
            trade_date_to_index,
            listed_trade_index_by_ts: HashMap::new(),
            min_listed_trade_days,
        });
    };
    let Some(list_date_index) = headers
        .iter()
        .position(|header| header.eq_ignore_ascii_case("list_date"))
    else {
        return Ok(PaperTradeEligibility {
            trade_date_to_index,
            listed_trade_index_by_ts: HashMap::new(),
            min_listed_trade_days,
        });
    };

    let mut listed_trade_index_by_ts = HashMap::new();
    for row_result in reader.records() {
        let row = row_result.map_err(|e| format!("解析stock_list.csv失败:{e}"))?;
        let Some(ts_code) = row
            .get(ts_code_index)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let Some(list_date) = row
            .get(list_date_index)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };

        let listed_index = match trade_dates.binary_search_by(|value| value.as_str().cmp(list_date))
        {
            Ok(index) => index,
            Err(index) if index < trade_dates.len() => index,
            Err(_) => continue,
        };
        listed_trade_index_by_ts.insert(ts_code.to_string(), listed_index);
    }

    Ok(PaperTradeEligibility {
        trade_date_to_index,
        listed_trade_index_by_ts,
        min_listed_trade_days,
    })
}

fn load_trade_date_options(source_path: &str) -> Result<Vec<String>, String> {
    let source_db = crate::data::source_db_path(source_path);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "原始库路径不是有效UTF-8".to_string())?;
    let conn = Connection::open(source_db_str).map_err(|e| format!("打开原始库失败: {e}"))?;
    let mut stmt = conn
        .prepare(
            r#"
            SELECT DISTINCT trade_date
            FROM stock_data
            WHERE adj_type = ?
            ORDER BY trade_date ASC
            "#,
        )
        .map_err(|e| format!("预编译交易日查询失败: {e}"))?;
    let mut rows = stmt
        .query(params![DEFAULT_ADJ_TYPE])
        .map_err(|e| format!("读取交易日失败: {e}"))?;

    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(|e| format!("读取交易日行失败: {e}"))? {
        let trade_date: String = row.get(0).map_err(|e| format!("读取交易日字段失败: {e}"))?;
        out.push(trade_date);
    }
    Ok(out)
}

fn normalize_trade_date_input(
    trade_date_options: &[String],
    input: Option<String>,
    default_value: Option<String>,
    field_name: &str,
    mode: TradeDateResolveMode,
) -> Result<String, String> {
    if trade_date_options.is_empty() {
        return Err("没有可用交易日".to_string());
    }

    let raw = input
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or(default_value)
        .ok_or_else(|| format!("{field_name}不能为空"))?;

    let digits = raw
        .chars()
        .filter(|ch| ch.is_ascii_digit())
        .collect::<String>();
    if digits.len() != 8 {
        return Err(format!("{field_name}格式无效: {raw}"));
    }

    match trade_date_options.binary_search_by(|value| value.as_str().cmp(&digits)) {
        Ok(index) => Ok(trade_date_options[index].clone()),
        Err(index) => match mode {
            TradeDateResolveMode::Start if index < trade_date_options.len() => {
                Ok(trade_date_options[index].clone())
            }
            TradeDateResolveMode::Start => {
                Err(format!("{field_name}晚于交易日历最后一天: {digits}"))
            }
            TradeDateResolveMode::End if index > 0 => Ok(trade_date_options[index - 1].clone()),
            TradeDateResolveMode::End => Err(format!("{field_name}早于交易日历第一天: {digits}")),
        },
    }
}

fn resolve_one_year_earlier_trade_date(
    trade_date_options: &[String],
    end_date: &str,
) -> Option<String> {
    let end_date = NaiveDate::parse_from_str(end_date, "%Y%m%d").ok()?;
    let target = end_date
        .checked_sub_days(Days::new(365))
        .unwrap_or(end_date);
    let target_text = target.format("%Y%m%d").to_string();

    let index = match trade_date_options.binary_search_by(|value| value.as_str().cmp(&target_text))
    {
        Ok(index) => index,
        Err(index) if index < trade_date_options.len() => index,
        Err(_) => trade_date_options.len().saturating_sub(1),
    };

    trade_date_options.get(index).cloned()
}

fn load_rank_series_map(
    source_path: &str,
    start_date: &str,
    end_date: &str,
) -> HashMap<String, HashMap<String, Option<f64>>> {
    let result_db = result_db_path(source_path);
    if !result_db.exists() {
        return HashMap::new();
    }

    let Some(result_db_str) = result_db.to_str() else {
        return HashMap::new();
    };
    let Ok(conn) = Connection::open(result_db_str) else {
        return HashMap::new();
    };
    let Ok(mut stmt) = conn.prepare(
        r#"
        SELECT ts_code, trade_date, rank
        FROM score_summary
        WHERE trade_date >= ? AND trade_date <= ?
        "#,
    ) else {
        return HashMap::new();
    };
    let Ok(mut rows) = stmt.query(params![start_date, end_date]) else {
        return HashMap::new();
    };

    let mut out: HashMap<String, HashMap<String, Option<f64>>> = HashMap::new();
    while let Ok(Some(row)) = rows.next() {
        let Ok(ts_code) = row.get::<_, String>(0) else {
            continue;
        };
        let Ok(trade_date) = row.get::<_, String>(1) else {
            continue;
        };
        let rank = row
            .get::<_, Option<i64>>(2)
            .ok()
            .flatten()
            .map(|value| value as f64);
        out.entry(ts_code).or_default().insert(trade_date, rank);
    }

    out
}

fn inject_runtime_rank_series(
    row_data: &mut crate::data::RowData,
    ts_code: &str,
    rank_series_map: &HashMap<String, HashMap<String, Option<f64>>>,
) -> Result<(), String> {
    let len = row_data.trade_dates.len();
    let mut rank_series = vec![None; len];

    if let Some(date_to_rank) = rank_series_map.get(ts_code) {
        for (index, trade_date) in row_data.trade_dates.iter().enumerate() {
            rank_series[index] = date_to_rank.get(trade_date).copied().flatten();
        }
    }

    row_data
        .cols
        .insert("RANK".to_string(), rank_series.clone());
    row_data.cols.insert("rank".to_string(), rank_series);
    row_data.validate()
}

fn runtime_num_series<'a>(runtime: &'a Runtime, name: &str) -> Result<&'a [Option<f64>], String> {
    match runtime.vars.get(name) {
        Some(Value::NumSeries(series)) => Ok(series.as_slice()),
        Some(_) => Err(format!("{name} 不是数值序列")),
        None => Err(format!("缺少{name}序列")),
    }
}

fn runtime_num_series_optional<'a>(runtime: &'a Runtime, name: &str) -> Option<&'a [Option<f64>]> {
    match runtime.vars.get(name) {
        Some(Value::NumSeries(series)) => Some(series.as_slice()),
        _ => None,
    }
}

fn set_runtime_num_series_value(
    runtime: &mut Runtime,
    name: &str,
    index: usize,
    value: Option<f64>,
) -> Result<(), String> {
    let Some(series_value) = runtime.vars.get_mut(name) else {
        return Err(format!("缺少{name}序列"));
    };
    let Value::NumSeries(series) = series_value else {
        return Err(format!("{name} 不是数值序列"));
    };
    let Some(slot) = series.get_mut(index) else {
        return Err(format!("{name} 序列索引越界"));
    };
    *slot = value;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use crate::{data::RowData, scoring::tools::calc_zhang_pct};

    #[derive(Clone, Copy)]
    struct SampleBar {
        trade_date: &'static str,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        pre_close: f64,
    }

    fn build_sample_row_data(ts_code: &str, bars: &[SampleBar]) -> RowData {
        let mut cols = HashMap::new();
        let zhang_pct = calc_zhang_pct(ts_code, false);
        let trade_dates = bars
            .iter()
            .map(|bar| bar.trade_date.to_string())
            .collect::<Vec<_>>();
        let open_series = bars.iter().map(|bar| Some(bar.open)).collect::<Vec<_>>();
        let high_series = bars.iter().map(|bar| Some(bar.high)).collect::<Vec<_>>();
        let low_series = bars.iter().map(|bar| Some(bar.low)).collect::<Vec<_>>();
        let close_series = bars.iter().map(|bar| Some(bar.close)).collect::<Vec<_>>();
        let pre_close_series = bars
            .iter()
            .map(|bar| Some(bar.pre_close))
            .collect::<Vec<_>>();
        let change_series = bars
            .iter()
            .map(|bar| Some(bar.close - bar.pre_close))
            .collect::<Vec<_>>();
        let pct_chg_series = bars
            .iter()
            .map(|bar| Some((bar.close / bar.pre_close - 1.0) * 100.0))
            .collect::<Vec<_>>();
        let len = bars.len();

        cols.insert("O".to_string(), open_series);
        cols.insert("H".to_string(), high_series);
        cols.insert("L".to_string(), low_series);
        cols.insert("C".to_string(), close_series);
        cols.insert("V".to_string(), vec![Some(100.0); len]);
        cols.insert("AMOUNT".to_string(), vec![Some(1000.0); len]);
        cols.insert("PRE_CLOSE".to_string(), pre_close_series);
        cols.insert("CHANGE".to_string(), change_series);
        cols.insert("PCT_CHG".to_string(), pct_chg_series);
        cols.insert("TURNOVER_RATE".to_string(), vec![Some(1.0); len]);
        cols.insert("ZHANG".to_string(), vec![Some(zhang_pct); len]);

        RowData { trade_dates, cols }
    }

    fn run_runtime_trade_simulation(
        ts_code: &str,
        bars: &[SampleBar],
        buy_expression: &str,
        sell_expression: &str,
        buy_price_basis: BuyPriceBasis,
    ) -> Vec<StrategyPaperValidationTradeRow> {
        let row_data = build_sample_row_data(ts_code, bars);
        row_data
            .validate()
            .expect("sample row data should be valid");
        let trade_dates = row_data.trade_dates.clone();
        let runtime = row_into_rt(row_data).expect("runtime should build");
        let buy_program = parse_expression_program(buy_expression, "买点方程")
            .expect("buy expression should parse");
        let sell_program = parse_expression_program(sell_expression, "卖点方程")
            .expect("sell expression should parse");

        simulate_trade_rows_from_runtime(
            ts_code,
            None,
            &trade_dates,
            &runtime,
            &buy_program,
            &sell_program,
            &PaperTradeEligibility::default(),
            0,
            buy_price_basis,
            0.0,
        )
        .expect("simulation should succeed")
    }

    #[test]
    fn start_date_input_snaps_to_next_trade_date() {
        let trade_dates = vec![
            "20240102".to_string(),
            "20240103".to_string(),
            "20240105".to_string(),
        ];

        let resolved = normalize_trade_date_input(
            &trade_dates,
            Some("2024-01-04".to_string()),
            None,
            "开始日期",
            TradeDateResolveMode::Start,
        )
        .unwrap();

        assert_eq!(resolved, "20240105");
    }

    #[test]
    fn end_date_input_snaps_to_previous_trade_date() {
        let trade_dates = vec![
            "20240102".to_string(),
            "20240103".to_string(),
            "20240105".to_string(),
        ];

        let resolved = normalize_trade_date_input(
            &trade_dates,
            Some("2024-01-04".to_string()),
            None,
            "结束日期",
            TradeDateResolveMode::End,
        )
        .unwrap();

        assert_eq!(resolved, "20240103");
    }

    #[test]
    fn limit_up_buy_basis_blocks_trade_entry() {
        let trades = run_runtime_trade_simulation(
            "000001.SZ",
            &[SampleBar {
                trade_date: "20240102",
                open: 10.0,
                high: 10.95,
                low: 10.0,
                close: 10.95,
                pre_close: 10.0,
            }],
            "C > 0",
            "TIME > 10",
            BuyPriceBasis::Close,
        );

        assert!(trades.is_empty());
    }

    #[test]
    fn limit_down_sell_latches_until_first_later_executable_day() {
        let trades = run_runtime_trade_simulation(
            "000001.SZ",
            &[
                SampleBar {
                    trade_date: "20240102",
                    open: 10.0,
                    high: 10.1,
                    low: 9.9,
                    close: 10.0,
                    pre_close: 10.0,
                },
                SampleBar {
                    trade_date: "20240103",
                    open: 9.5,
                    high: 9.5,
                    low: 9.05,
                    close: 9.05,
                    pre_close: 10.0,
                },
                SampleBar {
                    trade_date: "20240104",
                    open: 9.15,
                    high: 9.25,
                    low: 9.1,
                    close: 9.2,
                    pre_close: 9.05,
                },
            ],
            "C > 0",
            "TIME == 1",
            BuyPriceBasis::Open,
        );

        assert_eq!(trades.len(), 1);
        let trade = &trades[0];
        assert_eq!(trade.status, "closed");
        assert_eq!(trade.buy_date, "20240102");
        assert_eq!(trade.sell_date.as_deref(), Some("20240104"));
        assert_eq!(trade.hold_days, 2);
        assert_eq!(trade.sell_price, Some(9.2));
        assert_eq!(trade.realized_return_pct, Some(-8.000000000000007));
    }

    #[test]
    fn pending_limit_down_sell_stays_open_when_window_ends() {
        let trades = run_runtime_trade_simulation(
            "000001.SZ",
            &[
                SampleBar {
                    trade_date: "20240102",
                    open: 10.0,
                    high: 10.1,
                    low: 9.9,
                    close: 10.0,
                    pre_close: 10.0,
                },
                SampleBar {
                    trade_date: "20240103",
                    open: 9.5,
                    high: 9.5,
                    low: 9.05,
                    close: 9.05,
                    pre_close: 10.0,
                },
            ],
            "C > 0",
            "TIME == 1",
            BuyPriceBasis::Open,
        );

        assert_eq!(trades.len(), 1);
        let trade = &trades[0];
        assert_eq!(trade.status, "open");
        assert_eq!(trade.sell_date, None);
        assert_eq!(trade.sell_price, None);
        assert!((trade.realized_return_pct.unwrap() + 9.5).abs() < 1e-9);
        assert_eq!(trade.hold_days, 1);
    }
}

#[allow(dead_code)]
fn _load_stock_name_map(source_path: &str) -> Result<HashMap<String, String>, String> {
    let rows = load_stock_list(source_path)?;
    let mut out = HashMap::with_capacity(rows.len());
    for cols in rows {
        let Some(ts_code) = cols.first().map(|value| value.trim()) else {
            continue;
        };
        let Some(name) = cols.get(2).map(|value| value.trim()) else {
            continue;
        };
        if !ts_code.is_empty() && !name.is_empty() {
            out.insert(ts_code.to_string(), name.to_string());
        }
    }
    Ok(out)
}
