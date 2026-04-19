use std::collections::{HashMap, HashSet};

use duckdb::{Connection, params};
use rayon::prelude::*;
use serde::Serialize;

use crate::{
    data::scoring_data::row_into_rt,
    data::{DataReader, ScoreRule, load_ths_concepts_list, result_db_path},
    expr::{
        eval::Value,
        parser::{Expr, Parser, Stmt, Stmts, lex_all},
    },
    scoring::tools::{
        calc_query_need_rows, calc_query_start_date, inject_stock_extra_fields, load_st_list,
        rt_max_len,
    },
    utils::utils::{board_category, eval_binary_for_warmup, impl_expr_warmup},
};

use super::{
    build_area_map, build_concepts_map, build_industry_map, build_name_map, build_total_mv_map,
    filter_mv,
};

const DEFAULT_ADJ_TYPE: &str = "qfq";
const BOARD_ALL: &str = "全部";
const BOARD_ST: &str = "ST";

#[derive(Debug, Clone, Copy)]
enum PickScopeWay {
    Last,
    Any,
    Each,
    Recent,
    Consec(usize),
}

#[derive(Debug, Serialize)]
pub struct StockPickOptionsData {
    pub trade_date_options: Vec<String>,
    pub latest_trade_date: Option<String>,
    pub score_trade_date_options: Vec<String>,
    pub latest_score_trade_date: Option<String>,
    pub concept_options: Vec<String>,
    pub area_options: Vec<String>,
    pub industry_options: Vec<String>,
    pub strategy_options: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StockPickRow {
    pub ts_code: String,
    pub name: Option<String>,
    pub board: String,
    pub concept: Option<String>,
    pub rank: Option<i64>,
    pub total_score: Option<f64>,
    pub pick_note: String,
}

#[derive(Debug, Serialize)]
pub struct StockPickResultData {
    pub rows: Vec<StockPickRow>,
    pub resolved_start_date: Option<String>,
    pub resolved_end_date: Option<String>,
}

#[derive(Debug)]
enum ScopeHit {
    Bool(bool),
    Count(usize),
    Recent(Option<usize>),
}

#[derive(Debug)]
struct SummaryInfo {
    rank: Option<i64>,
    total_score: Option<f64>,
}

fn parse_scope_way(
    scope_way: &str,
    consec_threshold: Option<usize>,
) -> Result<PickScopeWay, String> {
    match scope_way.trim().to_ascii_uppercase().as_str() {
        "LAST" => Ok(PickScopeWay::Last),
        "ANY" => Ok(PickScopeWay::Any),
        "EACH" => Ok(PickScopeWay::Each),
        "RECENT" => Ok(PickScopeWay::Recent),
        "CONSEC" => {
            let threshold = consec_threshold.unwrap_or(2);
            if threshold == 0 {
                return Err("连续命中阈值必须 >= 1".to_string());
            }
            Ok(PickScopeWay::Consec(threshold))
        }
        other => Err(format!("不支持的选股方法: {other}")),
    }
}

fn normalize_date_range(
    trade_date_options: &[String],
    start_date: Option<String>,
    end_date: Option<String>,
) -> Result<(String, String), String> {
    let Some(latest_trade_date) = trade_date_options.last().cloned() else {
        return Err("没有可用交易日".to_string());
    };

    let resolved_start = start_date
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| latest_trade_date.clone());
    let resolved_end = end_date
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| latest_trade_date.clone());

    if resolved_start > resolved_end {
        return Err("起始日期不能晚于结束日期".to_string());
    }

    Ok((resolved_start, resolved_end))
}

fn resolve_expression_trade_window(
    trade_date_options: &[String],
    reference_trade_date: Option<String>,
    lookback_periods: Option<usize>,
    scope_way: PickScopeWay,
) -> Result<(String, String), String> {
    let resolved_reference_trade_date =
        normalize_single_trade_date(trade_date_options, reference_trade_date)?;
    let reference_index = trade_date_options
        .iter()
        .position(|item| item == &resolved_reference_trade_date)
        .ok_or_else(|| format!("交易日不存在: {resolved_reference_trade_date}"))?;

    let resolved_start_index = match scope_way {
        PickScopeWay::Last => reference_index,
        _ => {
            let periods = lookback_periods.unwrap_or(1);
            if periods == 0 {
                return Err("前推周期数必须 >= 1".to_string());
            }
            reference_index.saturating_sub(periods.saturating_sub(1))
        }
    };

    Ok((
        trade_date_options[resolved_start_index].clone(),
        resolved_reference_trade_date,
    ))
}

fn load_trade_date_options(source_path: &str) -> Result<Vec<String>, String> {
    let reader = DataReader::new(source_path)?;
    let mut stmt = reader
        .conn
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

fn split_concept_items(value: &str) -> Vec<String> {
    let normalized = value.trim();
    if normalized.is_empty() {
        return Vec::new();
    }

    let parts: Vec<String> = normalized
        .split(|ch| matches!(ch, ';' | ',' | '，' | '；' | '|' | '、' | '/' | '\n'))
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(ToOwned::to_owned)
        .collect();

    if parts.is_empty() {
        vec![normalized.to_string()]
    } else {
        let mut uniq = Vec::new();
        let mut seen = HashSet::new();
        for item in parts {
            if seen.insert(item.clone()) {
                uniq.push(item);
            }
        }
        uniq
    }
}

fn load_concept_options(source_path: &str) -> Result<Vec<String>, String> {
    let rows = load_ths_concepts_list(source_path)?;
    let mut items = Vec::new();
    let mut seen = HashSet::new();
    for cols in rows {
        let Some(value) = cols.get(2) else {
            continue;
        };
        for item in split_concept_items(value) {
            if seen.insert(item.clone()) {
                items.push(item);
            }
        }
    }
    items.sort();
    Ok(items)
}

fn load_score_trade_date_options(source_path: &str) -> Result<Vec<String>, String> {
    let result_db = result_db_path(source_path);
    if !result_db.exists() {
        return Ok(Vec::new());
    }

    let result_db_str = result_db
        .to_str()
        .ok_or_else(|| "结果库路径不是有效UTF-8".to_string())?;
    let conn = Connection::open(result_db_str).map_err(|e| format!("打开结果库失败: {e}"))?;
    let mut stmt = conn
        .prepare(
            r#"
            SELECT DISTINCT trade_date
            FROM score_summary
            ORDER BY trade_date ASC
            "#,
        )
        .map_err(|e| format!("预编译评分交易日查询失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("读取评分交易日失败: {e}"))?;

    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取评分交易日行失败: {e}"))?
    {
        let trade_date: String = row
            .get(0)
            .map_err(|e| format!("读取评分交易日字段失败: {e}"))?;
        out.push(trade_date);
    }
    Ok(out)
}

fn unique_sorted_options(values: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for value in values {
        let trimmed = value.trim().to_string();
        if trimmed.is_empty() || !seen.insert(trimmed.clone()) {
            continue;
        }
        out.push(trimmed);
    }
    out.sort();
    out
}

fn load_area_options(source_path: &str) -> Result<Vec<String>, String> {
    Ok(unique_sorted_options(
        build_area_map(source_path)?
            .into_values()
            .collect::<Vec<_>>(),
    ))
}

fn load_industry_options(source_path: &str) -> Result<Vec<String>, String> {
    Ok(unique_sorted_options(
        build_industry_map(source_path)?
            .into_values()
            .collect::<Vec<_>>(),
    ))
}

fn load_strategy_options(source_path: &str) -> Result<Vec<String>, String> {
    let rules = ScoreRule::load_rules(source_path)?;
    Ok(rules.into_iter().map(|rule| rule.name).collect())
}

fn normalize_single_trade_date(
    trade_date_options: &[String],
    trade_date: Option<String>,
) -> Result<String, String> {
    let Some(latest_trade_date) = trade_date_options.last().cloned() else {
        return Err("没有可用交易日".to_string());
    };
    let resolved = trade_date
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or(latest_trade_date);
    if trade_date_options.iter().any(|item| item == &resolved) {
        Ok(resolved)
    } else {
        Err(format!("交易日不存在: {resolved}"))
    }
}

pub fn get_stock_pick_options(source_path: &str) -> Result<StockPickOptionsData, String> {
    let trade_date_options = load_trade_date_options(source_path)?;
    let latest_trade_date = trade_date_options.last().cloned();
    let score_trade_date_options = load_score_trade_date_options(source_path)?;
    let latest_score_trade_date = score_trade_date_options.last().cloned();
    let concept_options = load_concept_options(source_path)?;
    let area_options = load_area_options(source_path)?;
    let industry_options = load_industry_options(source_path)?;
    let strategy_options = load_strategy_options(source_path)?;

    Ok(StockPickOptionsData {
        trade_date_options,
        latest_trade_date,
        score_trade_date_options,
        latest_score_trade_date,
        concept_options,
        area_options,
        industry_options,
        strategy_options,
    })
}

fn estimate_custom_warmup(stmts: &Stmts, scope_way: PickScopeWay) -> Result<usize, String> {
    let mut locals = HashMap::new();
    let mut consts: HashMap<String, usize> = HashMap::new();
    let mut expr_need = 0usize;

    for stmt in stmts.item.clone() {
        match stmt {
            Stmt::Assign { name, value } => match value {
                Expr::Number(v) => {
                    if v < 0.0 {
                        return Err("表达式常量赋值结果不能为负数".to_string());
                    }
                    consts.insert(name, v as usize);
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

    let extra_need = match scope_way {
        PickScopeWay::Last => 0,
        PickScopeWay::Any | PickScopeWay::Each | PickScopeWay::Recent => 0,
        PickScopeWay::Consec(threshold) => threshold.saturating_sub(1),
    };

    Ok(expr_need + extra_need)
}

fn hit_scope_period(scope_way: PickScopeWay, bs: &[bool]) -> ScopeHit {
    if bs.is_empty() {
        return match scope_way {
            PickScopeWay::Each => ScopeHit::Count(0),
            PickScopeWay::Recent => ScopeHit::Recent(None),
            _ => ScopeHit::Bool(false),
        };
    }

    match scope_way {
        PickScopeWay::Last => ScopeHit::Bool(bs.last().copied().unwrap_or(false)),
        PickScopeWay::Any => ScopeHit::Bool(bs.iter().any(|item| *item)),
        PickScopeWay::Each => ScopeHit::Count(bs.iter().filter(|item| **item).count()),
        PickScopeWay::Recent => {
            let end_index = bs.len() - 1;
            for index in (0..=end_index).rev() {
                if bs[index] {
                    return ScopeHit::Recent(Some(end_index - index));
                }
            }
            ScopeHit::Recent(None)
        }
        PickScopeWay::Consec(threshold) => {
            let mut best = 0usize;
            let mut current = 0usize;
            for item in bs {
                if *item {
                    current += 1;
                    best = best.max(current);
                } else {
                    current = 0;
                }
            }
            ScopeHit::Bool(best >= threshold)
        }
    }
}

fn scope_hit_matches(hit: &ScopeHit) -> bool {
    match hit {
        ScopeHit::Bool(value) => *value,
        ScopeHit::Count(value) => *value > 0,
        ScopeHit::Recent(value) => value.is_some(),
    }
}

fn scope_hit_note(hit: &ScopeHit, scope_way: PickScopeWay) -> String {
    match (scope_way, hit) {
        (PickScopeWay::Last, ScopeHit::Bool(true)) => "当日命中".to_string(),
        (PickScopeWay::Any, ScopeHit::Bool(true)) => "周期内命中".to_string(),
        (PickScopeWay::Consec(threshold), ScopeHit::Bool(true)) => format!("连续命中>={threshold}"),
        (PickScopeWay::Each, ScopeHit::Count(value)) => format!("命中 {value} 次"),
        (PickScopeWay::Recent, ScopeHit::Recent(Some(value))) => {
            format!("最近命中距今 {value} 个交易日")
        }
        _ => "--".to_string(),
    }
}

fn load_summary_map(source_path: &str, trade_date: &str) -> HashMap<String, SummaryInfo> {
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
        SELECT ts_code, rank, total_score
        FROM score_summary
        WHERE trade_date = ?
        "#,
    ) else {
        return HashMap::new();
    };
    let Ok(mut rows) = stmt.query(params![trade_date]) else {
        return HashMap::new();
    };

    let mut out = HashMap::new();
    while let Ok(Some(row)) = rows.next() {
        let Ok(ts_code) = row.get::<_, String>(0) else {
            continue;
        };
        let rank = row.get::<_, Option<i64>>(1).ok().flatten();
        let total_score = row.get::<_, Option<f64>>(2).ok().flatten();
        out.insert(ts_code, SummaryInfo { rank, total_score });
    }
    out
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

fn filter_board(
    ts_code: &str,
    stock_name: Option<&str>,
    board: Option<&str>,
    exclude_st_board: bool,
) -> bool {
    let current_board = board_category(ts_code, stock_name);
    if exclude_st_board && current_board == BOARD_ST {
        return false;
    }

    let Some(board) = board else {
        return true;
    };
    if board.is_empty() || board == BOARD_ALL {
        return true;
    }
    current_board == board
}

fn concept_matches(
    concept_text: Option<&str>,
    include_concepts: &[String],
    match_mode: &str,
) -> bool {
    if include_concepts.is_empty() {
        return true;
    }
    let items = concept_text.map(split_concept_items).unwrap_or_default();
    if match_mode == "AND" {
        include_concepts
            .iter()
            .all(|item| items.iter().any(|value| value == item))
    } else {
        include_concepts
            .iter()
            .any(|item| items.iter().any(|value| value == item))
    }
}

fn concept_excluded(concept_text: Option<&str>, exclude_concepts: &[String]) -> bool {
    if exclude_concepts.is_empty() {
        return false;
    }
    let items = concept_text.map(split_concept_items).unwrap_or_default();
    exclude_concepts
        .iter()
        .any(|item| items.iter().any(|value| value == item))
}

pub fn run_expression_stock_pick(
    source_path: &str,
    board: Option<String>,
    exclude_st_board: Option<bool>,
    reference_trade_date: Option<String>,
    lookback_periods: Option<usize>,
    scope_way: String,
    expression: String,
    consec_threshold: Option<usize>,
) -> Result<StockPickResultData, String> {
    let trade_date_options = load_trade_date_options(source_path)?;
    let parsed_scope_way = parse_scope_way(&scope_way, consec_threshold)?;
    let (resolved_start_date, resolved_end_date) = resolve_expression_trade_window(
        &trade_date_options,
        reference_trade_date,
        lookback_periods,
        parsed_scope_way,
    )?;

    let expression = expression.trim();
    if expression.is_empty() {
        return Err("表达式不能为空".to_string());
    }

    let tokens = lex_all(expression);
    let mut parser = Parser::new(tokens);
    let stmts = parser
        .parse_main()
        .map_err(|e| format!("表达式解析错误在{}:{}", e.idx, e.msg))?;

    let warmup_need = estimate_custom_warmup(&stmts, parsed_scope_way)?;
    let query_start_date = calc_query_start_date(source_path, warmup_need, &resolved_start_date)?;
    let need_rows = calc_query_need_rows(
        source_path,
        warmup_need,
        &resolved_start_date,
        &resolved_end_date,
    )?;

    let reader = DataReader::new(source_path)?;
    let ts_codes = DataReader::list_ts_code(
        &reader,
        DEFAULT_ADJ_TYPE,
        &resolved_start_date,
        &resolved_end_date,
    )?;
    let board_filter = board
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let exclude_st_board = exclude_st_board.unwrap_or(false);
    let st_list = load_st_list(source_path)?;
    let name_map = build_name_map(source_path).unwrap_or_default();
    let concept_map = build_concepts_map(source_path).unwrap_or_default();
    let summary_map = load_summary_map(source_path, &resolved_end_date);
    let rank_series_map = load_rank_series_map(source_path, &query_start_date, &resolved_end_date);
    let filtered_ts_codes = ts_codes
        .into_iter()
        .filter(|ts_code| {
            let stock_name = name_map.get(ts_code).map(|value| value.as_str());
            filter_board(ts_code, stock_name, board_filter, exclude_st_board)
        })
        .collect::<Vec<_>>();

    let rows = filtered_ts_codes
        .par_chunks(256)
        .map(|ts_group| -> Result<Vec<StockPickRow>, String> {
            let worker_reader = DataReader::new(source_path)?;
            let mut group_rows = Vec::new();

            for ts_code in ts_group {
                let mut row_data = worker_reader.load_one_tail_rows(
                    ts_code,
                    DEFAULT_ADJ_TYPE,
                    &resolved_end_date,
                    need_rows,
                )?;
                inject_stock_extra_fields(&mut row_data, ts_code, st_list.contains(ts_code), None)?;
                inject_runtime_rank_series(&mut row_data, ts_code, &rank_series_map)?;
                let trade_dates = row_data.trade_dates.clone();
                let keep_from = trade_dates
                    .binary_search_by(|d| d.as_str().cmp(&resolved_start_date))
                    .unwrap_or_else(|index| index);
                if keep_from >= trade_dates.len() {
                    continue;
                }

                let mut runtime = row_into_rt(row_data)?;
                let value = runtime
                    .eval_program(&stmts)
                    .map_err(|e| format!("表达式计算错误:{}", e.msg))?;
                let len = rt_max_len(&runtime);
                let bool_series = Value::as_bool_series(&value, len)
                    .map_err(|e| format!("表达式返回值非布尔:{}", e.msg))?;
                let kept_series = &bool_series[keep_from..];
                let hit = hit_scope_period(parsed_scope_way, kept_series);
                if !scope_hit_matches(&hit) {
                    continue;
                }

                let summary = summary_map.get(ts_code);
                group_rows.push(StockPickRow {
                    ts_code: ts_code.clone(),
                    name: name_map.get(ts_code).cloned(),
                    board: board_category(
                        ts_code,
                        name_map.get(ts_code).map(|value| value.as_str()),
                    )
                    .to_string(),
                    concept: concept_map.get(ts_code).cloned(),
                    rank: summary.and_then(|item| item.rank),
                    total_score: summary.and_then(|item| item.total_score),
                    pick_note: scope_hit_note(&hit, parsed_scope_way),
                });
            }

            Ok(group_rows)
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    let mut rows = rows;
    rows.sort_by(|left, right| match (left.rank, right.rank) {
        (Some(lv), Some(rv)) => lv.cmp(&rv).then_with(|| left.ts_code.cmp(&right.ts_code)),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => left.ts_code.cmp(&right.ts_code),
    });

    Ok(StockPickResultData {
        rows,
        resolved_start_date: Some(resolved_start_date),
        resolved_end_date: Some(resolved_end_date),
    })
}

pub fn run_concept_stock_pick(
    source_path: &str,
    board: Option<String>,
    exclude_st_board: Option<bool>,
    trade_date: Option<String>,
    include_areas: Vec<String>,
    include_industries: Vec<String>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
    include_concepts: Vec<String>,
    exclude_concepts: Vec<String>,
    match_mode: String,
) -> Result<StockPickResultData, String> {
    let trade_date_options = load_trade_date_options(source_path)?;
    let (_, resolved_trade_date) =
        normalize_date_range(&trade_date_options, trade_date.clone(), trade_date)?;
    let include_concepts: Vec<String> = include_concepts
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect();
    let include_industries: Vec<String> = include_industries
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect();
    let include_areas: Vec<String> = include_areas
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty() && value != BOARD_ALL)
        .collect();
    let exclude_concepts: Vec<String> = exclude_concepts
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect();
    let match_mode = match_mode.trim().to_ascii_uppercase();
    let summary_map = load_summary_map(source_path, &resolved_trade_date);
    let name_map = build_name_map(source_path).unwrap_or_default();
    let concept_map = build_concepts_map(source_path).unwrap_or_default();
    let area_map = build_area_map(source_path).unwrap_or_default();
    let industry_map = build_industry_map(source_path).unwrap_or_default();
    let total_mv_map = build_total_mv_map(source_path).unwrap_or_default();
    let board_filter = board
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let exclude_st_board = exclude_st_board.unwrap_or(false);

    let industry_filters = include_industries
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    let area_filters = include_areas.iter().cloned().collect::<HashSet<_>>();
    let ts_codes = name_map.keys().cloned().collect::<Vec<_>>();

    let mut rows = ts_codes
        .into_iter()
        .filter_map(|ts_code| {
            let stock_name = name_map.get(&ts_code).map(|value| value.as_str());
            if !filter_board(&ts_code, stock_name, board_filter, exclude_st_board) {
                return None;
            }

            let name = name_map.get(&ts_code).cloned();
            let concept_text = concept_map.get(&ts_code).cloned();
            if !area_filters.is_empty() {
                let matched = area_map
                    .get(&ts_code)
                    .map(|item| area_filters.contains(item))
                    .unwrap_or(false);
                if !matched {
                    return None;
                }
            }
            if !industry_filters.is_empty() {
                let matched = industry_map
                    .get(&ts_code)
                    .map(|item| industry_filters.contains(item))
                    .unwrap_or(false);
                if !matched {
                    return None;
                }
            }
            if !filter_mv(&total_mv_map, &ts_code, total_mv_min, total_mv_max) {
                return None;
            }
            if !concept_matches(
                concept_text.as_deref(),
                &include_concepts,
                match_mode.as_str(),
            ) {
                return None;
            }
            if concept_excluded(concept_text.as_deref(), &exclude_concepts) {
                return None;
            }

            let summary = summary_map.get(&ts_code);
            let mut notes = Vec::new();
            if include_concepts.is_empty() {
                notes.push("概念不限".to_string());
            } else {
                notes.push(format!(
                    "概念{}匹配",
                    if match_mode == "AND" { "AND" } else { "OR" }
                ));
            }
            if !include_industries.is_empty() {
                notes.push(format!("行业命中{}项", include_industries.len()));
            }
            if !include_areas.is_empty() {
                notes.push(format!("地区命中{}项", include_areas.len()));
            }
            if total_mv_min.is_some() || total_mv_max.is_some() {
                let min_text = total_mv_min.map(|value| format!("{value}"))
                    .unwrap_or_else(|| "-inf".to_string());
                let max_text = total_mv_max.map(|value| format!("{value}"))
                    .unwrap_or_else(|| "+inf".to_string());
                notes.push(format!("总市值[{min_text}, {max_text}]亿"));
            }
            let pick_note = notes.join("；");
            Some(StockPickRow {
                ts_code: ts_code.clone(),
                name: name.clone(),
                board: board_category(&ts_code, name.as_deref())
                    .to_string(),
                concept: concept_text,
                rank: summary.and_then(|item| item.rank),
                total_score: summary.and_then(|item| item.total_score),
                pick_note: if exclude_concepts.is_empty() {
                    pick_note
                } else {
                    format!("{pick_note}，排除{}项", exclude_concepts.len())
                },
            })
        })
        .collect::<Vec<_>>();

    rows.sort_by(|left, right| match (left.rank, right.rank) {
        (Some(lv), Some(rv)) => lv.cmp(&rv).then_with(|| left.ts_code.cmp(&right.ts_code)),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => left.ts_code.cmp(&right.ts_code),
    });

    Ok(StockPickResultData {
        rows,
        resolved_start_date: Some(resolved_trade_date.clone()),
        resolved_end_date: Some(resolved_trade_date),
    })
}
