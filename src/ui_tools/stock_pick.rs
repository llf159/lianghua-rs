use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use duckdb::{Connection, params};
use rayon::prelude::*;
use serde::Serialize;

use crate::{
    data::scoring_data::row_into_rt,
    data::{DataReader, RowData, ScoreRule, load_ths_concepts_list, result_db_path},
    expr::{
        eval::Value,
        parser::{Expr, Parser, Stmt, Stmts, lex_all},
    },
    scoring::tools::{calc_query_need_rows, calc_zhang_pct, load_st_list, rt_max_len},
    ui_tools::{
        build_area_map, build_circ_mv_map, build_concepts_map, build_industry_map, build_name_map,
        build_total_mv_map,
        strategy_performance::get_strategy_performance_page as core_get_strategy_performance_page,
    },
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

#[derive(Debug, Clone, Serialize)]
pub struct AdvancedStockPickRow {
    pub ts_code: String,
    pub name: Option<String>,
    pub board: String,
    pub area: Option<String>,
    pub industry: Option<String>,
    pub concept: Option<String>,
    pub rank: Option<i64>,
    pub total_score: Option<f64>,
    pub adv_hit_cnt: u32,
    pub adv_score_sum: f64,
    pub pos_hit_cnt: u32,
    pub pos_score_sum: f64,
    pub all_hit_cnt: u32,
    pub all_score_sum: f64,
    pub noisy_companion_cnt: u32,
    pub advantage_hits: String,
    pub companion_hits: String,
    pub pick_note: String,
}

#[derive(Debug, Serialize)]
pub struct AdvancedStockPickResultData {
    pub rows: Vec<AdvancedStockPickRow>,
    pub resolved_trade_date: Option<String>,
    pub resolved_method_key: String,
    pub resolved_method_label: String,
    pub total_candidate_count: u32,
    pub eligible_candidate_count: u32,
    pub selected_count: u32,
    pub resolved_advantage_rule_names: Vec<String>,
    pub resolved_noisy_companion_rule_names: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdvancedPickMethod {
    RawTopN,
    AdvantagePool,
    AdvHitTopN,
    AdvScoreTopN,
    MixedTopN,
    CompanionPenaltyTopN,
    PosHitTopN,
    PosScoreTopN,
    CleanAdvTopN,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum AdvancedMixedSortKey {
    AdvHitCnt,
    AdvScoreSum,
    PosHitCnt,
    PosScoreSum,
    TotalScore,
    Rank,
}

#[derive(Debug, Default, Clone)]
struct AdvancedPickAggRow {
    rank: Option<i64>,
    total_score: Option<f64>,
    adv_hit_cnt: u32,
    adv_score_sum: f64,
    pos_hit_cnt: u32,
    pos_score_sum: f64,
    all_hit_cnt: u32,
    all_score_sum: f64,
    noisy_companion_cnt: u32,
    advantage_hits: Vec<(String, f64)>,
    companion_hits: Vec<(String, f64)>,
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

fn filter_text_option(value: Option<&str>, selected: Option<&str>) -> bool {
    let Some(selected) = selected else {
        return true;
    };
    if selected.is_empty() || selected == BOARD_ALL {
        return true;
    }
    value.map(|item| item == selected).unwrap_or(false)
}

fn normalize_method(method_key: Option<String>) -> AdvancedPickMethod {
    match method_key
        .as_deref()
        .map(str::trim)
        .unwrap_or("adv_score_topn")
        .to_ascii_lowercase()
        .as_str()
    {
        "raw_topn" => AdvancedPickMethod::RawTopN,
        "advantage_pool" => AdvancedPickMethod::AdvantagePool,
        "adv_hit_topn" => AdvancedPickMethod::AdvHitTopN,
        "adv_score_topn" => AdvancedPickMethod::AdvScoreTopN,
        "mixed_topn" => AdvancedPickMethod::MixedTopN,
        "companion_penalty_topn" => AdvancedPickMethod::CompanionPenaltyTopN,
        "pos_hit_topn" => AdvancedPickMethod::PosHitTopN,
        "pos_score_topn" => AdvancedPickMethod::PosScoreTopN,
        "clean_adv_topn" => AdvancedPickMethod::CleanAdvTopN,
        _ => AdvancedPickMethod::AdvScoreTopN,
    }
}

fn advanced_method_key(method: AdvancedPickMethod) -> &'static str {
    match method {
        AdvancedPickMethod::RawTopN => "raw_topn",
        AdvancedPickMethod::AdvantagePool => "advantage_pool",
        AdvancedPickMethod::AdvHitTopN => "adv_hit_topn",
        AdvancedPickMethod::AdvScoreTopN => "adv_score_topn",
        AdvancedPickMethod::MixedTopN => "mixed_topn",
        AdvancedPickMethod::CompanionPenaltyTopN => "companion_penalty_topn",
        AdvancedPickMethod::PosHitTopN => "pos_hit_topn",
        AdvancedPickMethod::PosScoreTopN => "pos_score_topn",
        AdvancedPickMethod::CleanAdvTopN => "clean_adv_topn",
    }
}

fn advanced_method_label(method: AdvancedPickMethod) -> &'static str {
    match method {
        AdvancedPickMethod::RawTopN => "原始 TopN",
        AdvancedPickMethod::AdvantagePool => "优势池优先",
        AdvancedPickMethod::AdvHitTopN => "优势命中优先",
        AdvancedPickMethod::AdvScoreTopN => "优势得分优先",
        AdvancedPickMethod::MixedTopN => "混合排序",
        AdvancedPickMethod::CompanionPenaltyTopN => "噪音惩罚",
        AdvancedPickMethod::PosHitTopN => "正向命中优先",
        AdvancedPickMethod::PosScoreTopN => "正向得分优先",
        AdvancedPickMethod::CleanAdvTopN => "纯净优势池",
    }
}

fn normalize_mixed_sort_keys(keys: Option<Vec<String>>) -> Vec<AdvancedMixedSortKey> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for key in keys.unwrap_or_default() {
        let normalized = match key.trim().to_ascii_lowercase().as_str() {
            "adv_hit_cnt" => Some(AdvancedMixedSortKey::AdvHitCnt),
            "adv_score_sum" => Some(AdvancedMixedSortKey::AdvScoreSum),
            "pos_hit_cnt" => Some(AdvancedMixedSortKey::PosHitCnt),
            "pos_score_sum" => Some(AdvancedMixedSortKey::PosScoreSum),
            "total_score" => Some(AdvancedMixedSortKey::TotalScore),
            "rank" => Some(AdvancedMixedSortKey::Rank),
            _ => None,
        };
        if let Some(value) = normalized {
            if seen.insert(value) {
                out.push(value);
            }
        }
    }
    if out.is_empty() {
        vec![
            AdvancedMixedSortKey::AdvHitCnt,
            AdvancedMixedSortKey::AdvScoreSum,
            AdvancedMixedSortKey::TotalScore,
            AdvancedMixedSortKey::Rank,
        ]
    } else {
        out
    }
}

fn compare_option_f64_desc(left: Option<f64>, right: Option<f64>) -> Ordering {
    right.partial_cmp(&left).unwrap_or(Ordering::Equal)
}

fn compare_option_i64_asc(left: Option<i64>, right: Option<i64>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn compare_advanced_rows_with_key(
    left: &AdvancedStockPickRow,
    right: &AdvancedStockPickRow,
    key: AdvancedMixedSortKey,
) -> Ordering {
    match key {
        AdvancedMixedSortKey::AdvHitCnt => right.adv_hit_cnt.cmp(&left.adv_hit_cnt),
        AdvancedMixedSortKey::AdvScoreSum => {
            compare_option_f64_desc(Some(left.adv_score_sum), Some(right.adv_score_sum))
        }
        AdvancedMixedSortKey::PosHitCnt => right.pos_hit_cnt.cmp(&left.pos_hit_cnt),
        AdvancedMixedSortKey::PosScoreSum => {
            compare_option_f64_desc(Some(left.pos_score_sum), Some(right.pos_score_sum))
        }
        AdvancedMixedSortKey::TotalScore => {
            compare_option_f64_desc(left.total_score, right.total_score)
        }
        AdvancedMixedSortKey::Rank => compare_option_i64_asc(left.rank, right.rank),
    }
}

fn sort_advanced_rows_with_keys(rows: &mut [AdvancedStockPickRow], keys: &[AdvancedMixedSortKey]) {
    rows.sort_by(|left, right| {
        for key in keys {
            let ordering = compare_advanced_rows_with_key(left, right, *key);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        left.ts_code.cmp(&right.ts_code)
    });
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

fn format_rule_hits(items: &[(String, f64)]) -> String {
    if items.is_empty() {
        return "--".to_string();
    }
    items
        .iter()
        .map(|(rule_name, score)| format!("{rule_name}({score:.2})"))
        .collect::<Vec<_>>()
        .join("、")
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
    let board_filter = board
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

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

#[allow(clippy::too_many_arguments)]
pub fn run_advanced_stock_pick(
    source_path: &str,
    trade_date: Option<String>,
    board: Option<String>,
    area: Option<String>,
    industry: Option<String>,
    include_concepts: Vec<String>,
    exclude_concepts: Vec<String>,
    concept_match_mode: Option<String>,
    method_key: Option<String>,
    selected_horizon: Option<u32>,
    strong_quantile: Option<f64>,
    advantage_rule_mode: Option<String>,
    manual_rule_names: Option<Vec<String>>,
    auto_min_samples_2: Option<u32>,
    auto_min_samples_3: Option<u32>,
    auto_min_samples_5: Option<u32>,
    auto_min_samples_10: Option<u32>,
    require_win_rate_above_market: Option<bool>,
    min_pass_horizons: Option<u32>,
    min_adv_hits: Option<u32>,
    top_limit: Option<u32>,
    mixed_sort_keys: Option<Vec<String>>,
    noisy_companion_rule_names: Option<Vec<String>>,
    rank_max: Option<u32>,
    total_score_min: Option<f64>,
    total_score_max: Option<f64>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
    circ_mv_min: Option<f64>,
    circ_mv_max: Option<f64>,
) -> Result<AdvancedStockPickResultData, String> {
    let score_trade_date_options = load_score_trade_date_options(source_path)?;
    let resolved_trade_date = normalize_single_trade_date(&score_trade_date_options, trade_date)?;
    let board_filter = board
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let area_filter = area
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty() && *value != BOARD_ALL);
    let industry_filter = industry
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty() && *value != BOARD_ALL);
    let include_concepts = include_concepts
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    let exclude_concepts = exclude_concepts
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    let concept_match_mode = concept_match_mode
        .unwrap_or_else(|| "OR".to_string())
        .trim()
        .to_ascii_uppercase();
    let method = normalize_method(method_key);
    let min_adv_hits = min_adv_hits.unwrap_or(1).max(1);
    let top_limit = top_limit.unwrap_or(100).max(1);
    let mixed_sort_keys = normalize_mixed_sort_keys(mixed_sort_keys);
    let rank_max = rank_max.map(|value| value.max(1) as i64);

    let strategy_page = core_get_strategy_performance_page(
        source_path.to_string(),
        selected_horizon,
        strong_quantile,
        advantage_rule_mode,
        manual_rule_names,
        auto_min_samples_2,
        auto_min_samples_3,
        auto_min_samples_5,
        auto_min_samples_10,
        require_win_rate_above_market,
        min_pass_horizons,
        Some(min_adv_hits),
        Some(top_limit),
        Some(
            mixed_sort_keys
                .iter()
                .map(|key| match key {
                    AdvancedMixedSortKey::AdvHitCnt => "adv_hit_cnt".to_string(),
                    AdvancedMixedSortKey::AdvScoreSum => "adv_score_sum".to_string(),
                    AdvancedMixedSortKey::PosHitCnt => "pos_hit_cnt".to_string(),
                    AdvancedMixedSortKey::PosScoreSum => "pos_score_sum".to_string(),
                    AdvancedMixedSortKey::TotalScore => "total_score".to_string(),
                    AdvancedMixedSortKey::Rank => "rank".to_string(),
                })
                .collect(),
        ),
        noisy_companion_rule_names.clone(),
        None,
    )?;
    let advantage_rule_set = strategy_page
        .resolved_advantage_rule_names
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    let noisy_rule_names = noisy_companion_rule_names.unwrap_or_default();
    let noisy_rule_set = noisy_rule_names.iter().cloned().collect::<HashSet<_>>();

    let result_db = result_db_path(source_path);
    let result_db_str = result_db
        .to_str()
        .ok_or_else(|| "结果库路径不是有效UTF-8".to_string())?;
    let conn = Connection::open(result_db_str).map_err(|e| format!("打开结果库失败: {e}"))?;
    let mut stmt = conn
        .prepare(
            r#"
            SELECT
                s.ts_code,
                s.rank,
                s.total_score,
                d.rule_name,
                d.rule_score
            FROM score_summary AS s
            LEFT JOIN score_details AS d
              ON d.ts_code = s.ts_code
             AND d.trade_date = s.trade_date
             AND d.rule_score != 0
            WHERE s.trade_date = ?
            ORDER BY s.rank ASC NULLS LAST, s.ts_code ASC
            "#,
        )
        .map_err(|e| format!("预编译高级选股查询失败: {e}"))?;
    let mut rows = stmt
        .query(params![resolved_trade_date.as_str()])
        .map_err(|e| format!("读取高级选股数据失败: {e}"))?;

    let mut agg_map = HashMap::<String, AdvancedPickAggRow>::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取高级选股行失败: {e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取股票代码失败: {e}"))?;
        let entry = agg_map.entry(ts_code).or_default();
        if entry.rank.is_none() {
            entry.rank = row.get(1).map_err(|e| format!("读取排名失败: {e}"))?;
        }
        if entry.total_score.is_none() {
            entry.total_score = row.get(2).map_err(|e| format!("读取总分失败: {e}"))?;
        }
        let rule_name: Option<String> = row.get(3).map_err(|e| format!("读取规则名失败: {e}"))?;
        let rule_score: Option<f64> = row.get(4).map_err(|e| format!("读取规则分失败: {e}"))?;
        let (Some(rule_name), Some(rule_score)) = (rule_name, rule_score) else {
            continue;
        };
        if rule_score == 0.0 {
            continue;
        }
        entry.all_hit_cnt += 1;
        entry.all_score_sum += rule_score;
        if rule_score > 0.0 {
            entry.pos_hit_cnt += 1;
            entry.pos_score_sum += rule_score;
            if advantage_rule_set.contains(&rule_name) {
                entry.adv_hit_cnt += 1;
                entry.adv_score_sum += rule_score;
                entry.advantage_hits.push((rule_name.clone(), rule_score));
            } else {
                entry.companion_hits.push((rule_name.clone(), rule_score));
            }
            if noisy_rule_set.contains(&rule_name) {
                entry.noisy_companion_cnt += 1;
            }
        }
    }

    let name_map = build_name_map(source_path).unwrap_or_default();
    let area_map = build_area_map(source_path).unwrap_or_default();
    let industry_map = build_industry_map(source_path).unwrap_or_default();
    let concept_map = build_concepts_map(source_path).unwrap_or_default();
    let total_mv_map = build_total_mv_map(source_path).unwrap_or_default();
    let circ_mv_map = build_circ_mv_map(source_path).unwrap_or_default();

    let mut all_rows = agg_map
        .into_iter()
        .filter_map(|(ts_code, agg)| {
            let concept_text = concept_map.get(&ts_code).cloned();
            let area_text = area_map.get(&ts_code).cloned();
            let industry_text = industry_map.get(&ts_code).cloned();

            if !filter_board(&ts_code, board_filter) {
                return None;
            }
            if !filter_text_option(area_text.as_deref(), area_filter) {
                return None;
            }
            if !filter_text_option(industry_text.as_deref(), industry_filter) {
                return None;
            }
            if !concept_matches(
                concept_text.as_deref(),
                &include_concepts,
                concept_match_mode.as_str(),
            ) {
                return None;
            }
            if concept_excluded(concept_text.as_deref(), &exclude_concepts) {
                return None;
            }
            if let Some(limit) = rank_max {
                if agg.rank.map(|value| value > limit).unwrap_or(true) {
                    return None;
                }
            }
            if let Some(min_value) = total_score_min {
                if agg
                    .total_score
                    .map(|value| value < min_value)
                    .unwrap_or(true)
                {
                    return None;
                }
            }
            if let Some(max_value) = total_score_max {
                if agg
                    .total_score
                    .map(|value| value > max_value)
                    .unwrap_or(true)
                {
                    return None;
                }
            }
            if !super::filter_mv(&total_mv_map, &ts_code, total_mv_min, total_mv_max) {
                return None;
            }
            if !super::filter_mv(&circ_mv_map, &ts_code, circ_mv_min, circ_mv_max) {
                return None;
            }

            let advantage_hits = format_rule_hits(&agg.advantage_hits);
            let companion_hits = format_rule_hits(&agg.companion_hits);
            Some(AdvancedStockPickRow {
                ts_code: ts_code.clone(),
                name: name_map.get(&ts_code).cloned(),
                board: board_category(&ts_code).to_string(),
                area: area_text,
                industry: industry_text,
                concept: concept_text,
                rank: agg.rank,
                total_score: agg.total_score,
                adv_hit_cnt: agg.adv_hit_cnt,
                adv_score_sum: agg.adv_score_sum,
                pos_hit_cnt: agg.pos_hit_cnt,
                pos_score_sum: agg.pos_score_sum,
                all_hit_cnt: agg.all_hit_cnt,
                all_score_sum: agg.all_score_sum,
                noisy_companion_cnt: agg.noisy_companion_cnt,
                advantage_hits: advantage_hits.clone(),
                companion_hits: companion_hits.clone(),
                pick_note: format!(
                    "优势{}条 / 优势分{:.2} / 正向{}条 / 噪音{}条",
                    agg.adv_hit_cnt, agg.adv_score_sum, agg.pos_hit_cnt, agg.noisy_companion_cnt
                ),
            })
        })
        .collect::<Vec<_>>();
    let total_candidate_count = all_rows.len() as u32;

    let mut eligible_rows = all_rows
        .iter()
        .filter(|row| row.adv_hit_cnt >= min_adv_hits)
        .cloned()
        .collect::<Vec<_>>();
    let eligible_candidate_count = eligible_rows.len() as u32;

    match method {
        AdvancedPickMethod::RawTopN => {
            all_rows.sort_by(|left, right| {
                compare_option_i64_asc(left.rank, right.rank)
                    .then_with(|| compare_option_f64_desc(left.total_score, right.total_score))
                    .then_with(|| left.ts_code.cmp(&right.ts_code))
            });
            all_rows.truncate(top_limit as usize);
        }
        AdvancedPickMethod::AdvantagePool => {
            eligible_rows.sort_by(|left, right| {
                compare_option_i64_asc(left.rank, right.rank)
                    .then_with(|| compare_option_f64_desc(left.total_score, right.total_score))
                    .then_with(|| left.ts_code.cmp(&right.ts_code))
            });
            eligible_rows.truncate(top_limit as usize);
            all_rows = eligible_rows;
        }
        AdvancedPickMethod::AdvHitTopN => {
            eligible_rows.sort_by(|left, right| {
                right
                    .adv_hit_cnt
                    .cmp(&left.adv_hit_cnt)
                    .then_with(|| compare_option_i64_asc(left.rank, right.rank))
                    .then_with(|| compare_option_f64_desc(left.total_score, right.total_score))
                    .then_with(|| left.ts_code.cmp(&right.ts_code))
            });
            eligible_rows.truncate(top_limit as usize);
            all_rows = eligible_rows;
        }
        AdvancedPickMethod::AdvScoreTopN => {
            eligible_rows.sort_by(|left, right| {
                compare_option_f64_desc(Some(left.adv_score_sum), Some(right.adv_score_sum))
                    .then_with(|| compare_option_i64_asc(left.rank, right.rank))
                    .then_with(|| compare_option_f64_desc(left.total_score, right.total_score))
                    .then_with(|| left.ts_code.cmp(&right.ts_code))
            });
            eligible_rows.truncate(top_limit as usize);
            all_rows = eligible_rows;
        }
        AdvancedPickMethod::MixedTopN => {
            sort_advanced_rows_with_keys(&mut eligible_rows, &mixed_sort_keys);
            eligible_rows.truncate(top_limit as usize);
            all_rows = eligible_rows;
        }
        AdvancedPickMethod::CompanionPenaltyTopN => {
            eligible_rows.sort_by(|left, right| {
                right
                    .adv_hit_cnt
                    .cmp(&left.adv_hit_cnt)
                    .then_with(|| left.noisy_companion_cnt.cmp(&right.noisy_companion_cnt))
                    .then_with(|| {
                        compare_option_f64_desc(Some(left.adv_score_sum), Some(right.adv_score_sum))
                    })
                    .then_with(|| compare_option_i64_asc(left.rank, right.rank))
                    .then_with(|| compare_option_f64_desc(left.total_score, right.total_score))
                    .then_with(|| left.ts_code.cmp(&right.ts_code))
            });
            eligible_rows.truncate(top_limit as usize);
            all_rows = eligible_rows;
        }
        AdvancedPickMethod::PosHitTopN => {
            all_rows.sort_by(|left, right| {
                right
                    .pos_hit_cnt
                    .cmp(&left.pos_hit_cnt)
                    .then_with(|| compare_option_i64_asc(left.rank, right.rank))
                    .then_with(|| compare_option_f64_desc(left.total_score, right.total_score))
                    .then_with(|| left.ts_code.cmp(&right.ts_code))
            });
            all_rows.truncate(top_limit as usize);
        }
        AdvancedPickMethod::PosScoreTopN => {
            all_rows.sort_by(|left, right| {
                compare_option_f64_desc(Some(left.pos_score_sum), Some(right.pos_score_sum))
                    .then_with(|| compare_option_i64_asc(left.rank, right.rank))
                    .then_with(|| compare_option_f64_desc(left.total_score, right.total_score))
                    .then_with(|| left.ts_code.cmp(&right.ts_code))
            });
            all_rows.truncate(top_limit as usize);
        }
        AdvancedPickMethod::CleanAdvTopN => {
            eligible_rows.retain(|row| row.noisy_companion_cnt == 0);
            eligible_rows.sort_by(|left, right| {
                compare_option_f64_desc(Some(left.adv_score_sum), Some(right.adv_score_sum))
                    .then_with(|| right.adv_hit_cnt.cmp(&left.adv_hit_cnt))
                    .then_with(|| compare_option_i64_asc(left.rank, right.rank))
                    .then_with(|| left.ts_code.cmp(&right.ts_code))
            });
            eligible_rows.truncate(top_limit as usize);
            all_rows = eligible_rows;
        }
    }

    Ok(AdvancedStockPickResultData {
        selected_count: all_rows.len() as u32,
        rows: all_rows,
        resolved_trade_date: Some(resolved_trade_date),
        resolved_method_key: advanced_method_key(method).to_string(),
        resolved_method_label: advanced_method_label(method).to_string(),
        total_candidate_count,
        eligible_candidate_count,
        resolved_advantage_rule_names: strategy_page.resolved_advantage_rule_names,
        resolved_noisy_companion_rule_names: noisy_rule_names,
    })
}
