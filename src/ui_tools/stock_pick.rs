use std::collections::{HashMap, HashSet};

use duckdb::{Connection, params};
use rayon::prelude::*;
use serde::Serialize;

use crate::{
    data::{DataReader, RowData, load_ths_concepts_list, result_db_path},
    data::scoring_data::row_into_rt,
    expr::{
        eval::Value,
        parser::{Expr, Parser, Stmt, Stmts, lex_all},
    },
    scoring::tools::{calc_query_need_rows, calc_zhang_pct, load_st_list, rt_max_len},
    ui_tools::{build_concepts_map, build_name_map},
    utils::utils::{board_category, eval_binary_for_warmup, impl_expr_warmup},
};

const DEFAULT_ADJ_TYPE: &str = "qfq";
const BOARD_ALL: &str = "全部";

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
    pub concept_options: Vec<String>,
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

fn parse_scope_way(scope_way: &str, consec_threshold: Option<usize>) -> Result<PickScopeWay, String> {
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

pub fn get_stock_pick_options(source_path: &str) -> Result<StockPickOptionsData, String> {
    let trade_date_options = load_trade_date_options(source_path)?;
    let latest_trade_date = trade_date_options.last().cloned();
    let concept_options = load_concept_options(source_path)?;

    Ok(StockPickOptionsData {
        trade_date_options,
        latest_trade_date,
        concept_options,
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

fn fill_pick_extra_fields(
    row_data: &mut RowData,
    ts_code: &str,
    is_st: bool,
) -> Result<(), String> {
    let zhang = calc_zhang_pct(ts_code, is_st);
    let zhang_series = vec![Some(zhang); row_data.trade_dates.len()];
    row_data.cols.insert("ZHANG".to_string(), zhang_series);
    row_data.validate()
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
        (PickScopeWay::Recent, ScopeHit::Recent(Some(value))) => format!("最近命中距今 {value} 天"),
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

fn filter_board(ts_code: &str, board: Option<&str>) -> bool {
    let Some(board) = board else {
        return true;
    };
    if board.is_empty() || board == BOARD_ALL {
        return true;
    }
    board_category(ts_code) == board
}

pub fn run_expression_stock_pick(
    source_path: &str,
    board: Option<String>,
    start_date: Option<String>,
    end_date: Option<String>,
    scope_way: String,
    expression: String,
    consec_threshold: Option<usize>,
) -> Result<StockPickResultData, String> {
    let trade_date_options = load_trade_date_options(source_path)?;
    let (resolved_start_date, resolved_end_date) =
        normalize_date_range(&trade_date_options, start_date, end_date)?;
    let parsed_scope_way = parse_scope_way(&scope_way, consec_threshold)?;

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
    let need_rows = calc_query_need_rows(
        source_path,
        warmup_need,
        &resolved_start_date,
        &resolved_end_date,
    )?;

    let reader = DataReader::new(source_path)?;
    let ts_codes =
        DataReader::list_ts_code(&reader, DEFAULT_ADJ_TYPE, &resolved_start_date, &resolved_end_date)?;
    let board_filter = board.as_deref().map(str::trim).filter(|value| !value.is_empty());
    let filtered_ts_codes = ts_codes
        .into_iter()
        .filter(|ts_code| filter_board(ts_code, board_filter))
        .collect::<Vec<_>>();
    let st_list = load_st_list(source_path)?;
    let name_map = build_name_map(source_path).unwrap_or_default();
    let concept_map = build_concepts_map(source_path).unwrap_or_default();
    let summary_map = load_summary_map(source_path, &resolved_end_date);

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
                fill_pick_extra_fields(&mut row_data, ts_code, st_list.contains(ts_code))?;
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
                    board: board_category(ts_code).to_string(),
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
    trade_date: Option<String>,
    concepts: Vec<String>,
    match_mode: String,
) -> Result<StockPickResultData, String> {
    let trade_date_options = load_trade_date_options(source_path)?;
    let (_, resolved_trade_date) =
        normalize_date_range(&trade_date_options, trade_date.clone(), trade_date)?;
    let selected_concepts: Vec<String> = concepts
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect();
    let match_mode = match_mode.trim().to_ascii_uppercase();
    let summary_map = load_summary_map(source_path, &resolved_trade_date);
    let name_map = build_name_map(source_path).unwrap_or_default();
    let concept_map = build_concepts_map(source_path).unwrap_or_default();
    let board_filter = board.as_deref().map(str::trim).filter(|value| !value.is_empty());

    let mut rows = concept_map
        .into_iter()
        .filter(|(ts_code, _)| filter_board(ts_code, board_filter))
        .filter_map(|(ts_code, concept_text)| {
            let concept_items = split_concept_items(&concept_text);
            let matched = if selected_concepts.is_empty() {
                true
            } else if match_mode == "AND" {
                selected_concepts
                    .iter()
                    .all(|item| concept_items.iter().any(|value| value == item))
            } else {
                selected_concepts
                    .iter()
                    .any(|item| concept_items.iter().any(|value| value == item))
            };
            if !matched {
                return None;
            }

            let summary = summary_map.get(&ts_code);
            Some(StockPickRow {
                ts_code: ts_code.clone(),
                name: name_map.get(&ts_code).cloned(),
                board: board_category(&ts_code).to_string(),
                concept: Some(concept_text),
                rank: summary.and_then(|item| item.rank),
                total_score: summary.and_then(|item| item.total_score),
                pick_note: if selected_concepts.is_empty() {
                    "全部概念".to_string()
                } else {
                    format!("概念{}匹配", if match_mode == "AND" { "AND" } else { "OR" })
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
