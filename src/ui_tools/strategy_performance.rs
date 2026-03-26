use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};

use duckdb::{Connection, params};
use rayon::prelude::*;
use serde::Serialize;

use crate::data::{
    DataReader, DistPoint, RowData, RuleTag, ScopeWay, ScoreRule, load_trade_date_list,
    result_db_path,
    scoring_data::{ScoreDetails, row_into_rt},
    source_db_path,
};
use crate::expr::parser::{Expr, Parser, Stmt, Stmts, lex_all};
use crate::scoring::{
    CachedRule, scoring_rules_details_cache,
    tools::{calc_query_need_rows, calc_zhang_pct, load_st_list},
};
use crate::ui_tools::strategy_manage::StrategyManageRuleDraft;
use crate::utils::utils::{eval_binary_for_warmup, impl_expr_warmup};

const DEFAULT_ADJ_TYPE: &str = "qfq";
const HORIZONS: [u32; 4] = [2, 3, 5, 10];
const DEFAULT_SELECTED_HORIZON: u32 = 10;
const DEFAULT_STRONG_QUANTILE: f64 = 0.9;
const DEFAULT_MIN_SAMPLE: u32 = 30;
const DEFAULT_MIN_PASS_HORIZONS: u32 = 2;
const DEFAULT_MIN_ADV_HITS: u32 = 1;
const DEFAULT_TOP_LIMIT: u32 = 100;
const DEFAULT_AUTO_ADVANTAGE_LIMIT: usize = 10;
const RECENT_WINDOWS: [usize; 2] = [20, 40];
const SCORE_BUCKET_LIMIT: usize = 8;
const SCORE_BUCKET_QUANTILES: usize = 5;
const RESULT_DETAILS_TABLE: &str = "result_db.score_details";
const VALIDATION_DETAILS_TABLE: &str = "strategy_validate_rule_details";

#[derive(Debug, Serialize, Clone)]
pub struct StrategyPerformanceMethodNote {
    pub key: String,
    pub title: String,
    pub description: String,
}

#[derive(Debug, Serialize, Clone)]
pub struct StrategyPerformanceFutureSummary {
    pub horizon: u32,
    pub sample_count: u32,
    pub avg_future_return_pct: Option<f64>,
    pub p80_return_pct: Option<f64>,
    pub p90_return_pct: Option<f64>,
    pub p95_return_pct: Option<f64>,
    pub strong_quantile: f64,
    pub strong_threshold_pct: Option<f64>,
    pub strong_base_rate: Option<f64>,
    pub win_rate: Option<f64>,
    pub max_future_return_pct: Option<f64>,
}

#[derive(Debug, Serialize, Clone)]
pub struct StrategyPerformanceHorizonMetric {
    pub horizon: u32,
    pub hit_n: u32,
    pub avg_future_return_pct: Option<f64>,
    pub strong_hit_rate: Option<f64>,
    pub strong_lift: Option<f64>,
    pub win_rate: Option<f64>,
    pub avg_total_score: Option<f64>,
    pub avg_rank: Option<f64>,
    pub hit_vs_non_hit_delta_pct: Option<f64>,
    pub low_confidence: bool,
    pub passes_auto_filter: bool,
    pub passes_negative_filter: bool,
}

#[derive(Debug, Serialize, Clone)]
pub struct StrategyPerformanceRuleRow {
    pub rule_name: String,
    pub explain: Option<String>,
    pub tag: Option<String>,
    pub scope_way: Option<String>,
    pub scope_windows: Option<u32>,
    pub points: Option<f64>,
    pub has_dist_points: bool,
    pub signal_direction: String,
    pub direction_label: String,
    pub auto_candidate: bool,
    pub manually_selected: bool,
    pub in_advantage_set: bool,
    pub in_companion_set: bool,
    pub negative_effective: Option<bool>,
    pub negative_effectiveness_label: Option<String>,
    pub negative_review_notes: Vec<String>,
    pub metrics: Vec<StrategyPerformanceHorizonMetric>,
}

#[derive(Debug, Serialize, Clone)]
pub struct StrategyPerformanceCompanionRow {
    pub rule_name: String,
    pub hit_n: u32,
    pub avg_future_return_pct: Option<f64>,
    pub eligible_pool_avg_return_pct: Option<f64>,
    pub delta_return_pct: Option<f64>,
    pub win_rate: Option<f64>,
    pub eligible_pool_win_rate: Option<f64>,
    pub delta_win_rate: Option<f64>,
    pub low_confidence: bool,
}

#[derive(Debug, Serialize, Clone)]
pub struct StrategyPerformancePortfolioWindow {
    pub window_key: String,
    pub label: String,
    pub sample_days: u32,
    pub avg_portfolio_return_pct: Option<f64>,
    pub avg_market_return_pct: Option<f64>,
    pub avg_excess_return_pct: Option<f64>,
    pub excess_win_rate: Option<f64>,
    pub avg_selected_count: Option<f64>,
}

#[derive(Debug, Serialize, Clone)]
pub struct StrategyPerformancePortfolioRow {
    pub strategy_key: String,
    pub strategy_label: String,
    pub sort_description: String,
    pub windows: Vec<StrategyPerformancePortfolioWindow>,
}

#[derive(Debug, Serialize, Clone)]
pub struct StrategyPerformanceScoreBucketRow {
    pub bucket_label: String,
    pub score_min: Option<f64>,
    pub score_max: Option<f64>,
    pub sample_count: u32,
    pub avg_future_return_pct: Option<f64>,
    pub strong_hit_rate: Option<f64>,
    pub win_rate: Option<f64>,
}

#[derive(Debug, Serialize, Clone)]
pub struct StrategyPerformanceHitCountRow {
    pub hit_count: u32,
    pub sample_count: u32,
    pub avg_future_return_pct: Option<f64>,
    pub strong_hit_rate: Option<f64>,
    pub win_rate: Option<f64>,
}

#[derive(Debug, Serialize, Clone)]
pub struct StrategyPerformanceRuleDirectionDetail {
    pub signal_direction: String,
    pub direction_label: String,
    pub bucket_mode: String,
    pub sample_count: u32,
    pub avg_future_return_pct: Option<f64>,
    pub strong_hit_rate: Option<f64>,
    pub win_rate: Option<f64>,
    pub spearman_corr: Option<f64>,
    pub abs_spearman_corr: Option<f64>,
    pub hit_vs_non_hit_delta_pct: Option<f64>,
    pub extreme_score_minus_mild_score_pct: Option<f64>,
    pub has_dist_points: bool,
    pub score_rows: Vec<StrategyPerformanceScoreBucketRow>,
    pub hit_count_rows: Vec<StrategyPerformanceHitCountRow>,
}

#[derive(Debug, Serialize, Clone)]
pub struct StrategyPerformanceRuleDetail {
    pub rule_name: String,
    pub horizon: u32,
    pub explain: Option<String>,
    pub tag: Option<String>,
    pub scope_way: Option<String>,
    pub scope_windows: Option<u32>,
    pub points: Option<f64>,
    pub has_dist_points: bool,
    pub directions: Vec<StrategyPerformanceRuleDirectionDetail>,
}

#[derive(Debug, Serialize, Clone)]
pub struct StrategyPerformanceAutoFilterConfig {
    pub min_samples_2: u32,
    pub min_samples_3: u32,
    pub min_samples_5: u32,
    pub min_samples_10: u32,
    pub require_win_rate_above_market: bool,
    pub min_pass_horizons: u32,
}

#[derive(Debug, Serialize, Clone)]
pub struct StrategyPerformancePageData {
    pub horizons: Vec<u32>,
    pub selected_horizon: u32,
    pub strong_quantile: f64,
    pub strategy_options: Vec<String>,
    pub future_summaries: Vec<StrategyPerformanceFutureSummary>,
    pub auto_filter: StrategyPerformanceAutoFilterConfig,
    pub resolved_advantage_mode: String,
    pub auto_candidate_rule_names: Vec<String>,
    pub manual_rule_names: Vec<String>,
    pub ignored_manual_rule_names: Vec<String>,
    pub resolved_advantage_rule_names: Vec<String>,
    pub resolved_companion_rule_names: Vec<String>,
    pub effective_negative_rule_names: Vec<String>,
    pub ineffective_negative_rule_names: Vec<String>,
    pub min_adv_hits: u32,
    pub top_limit: u32,
    pub mixed_sort_keys: Vec<String>,
    pub noisy_companion_rule_names: Vec<String>,
    pub rule_rows: Vec<StrategyPerformanceRuleRow>,
    pub companion_rows: Vec<StrategyPerformanceCompanionRow>,
    pub portfolio_rows: Vec<StrategyPerformancePortfolioRow>,
    pub selected_rule_name: Option<String>,
    pub rule_detail: Option<StrategyPerformanceRuleDetail>,
    pub methods: Vec<StrategyPerformanceMethodNote>,
}

#[derive(Debug, Serialize, Clone)]
pub struct StrategyPerformanceValidationDraftSummary {
    pub name: String,
    pub explain: String,
    pub tag: Option<String>,
    pub scope_way: String,
    pub scope_windows: u32,
    pub points: f64,
    pub has_dist_points: bool,
    pub score_mode: String,
}

#[derive(Debug, Serialize, Clone)]
pub struct StrategyPerformanceValidationPageData {
    pub horizons: Vec<u32>,
    pub selected_horizon: u32,
    pub strong_quantile: f64,
    pub future_summaries: Vec<StrategyPerformanceFutureSummary>,
    pub auto_filter: StrategyPerformanceAutoFilterConfig,
    pub draft_summary: StrategyPerformanceValidationDraftSummary,
    pub rule_rows: Vec<StrategyPerformanceRuleRow>,
    pub rule_detail: Option<StrategyPerformanceRuleDetail>,
    pub methods: Vec<StrategyPerformanceMethodNote>,
}

#[derive(Debug, Clone)]
struct RuleMeta {
    explain: String,
    tag: RuleTag,
    scope_way_label: String,
    scope_windows: u32,
    points: f64,
    has_dist_points: bool,
}

#[derive(Debug, Default, Clone, Copy)]
struct RuleAggMetric {
    hit_n: u32,
    avg_future_return_pct: Option<f64>,
    strong_hit_rate: Option<f64>,
    win_rate: Option<f64>,
    avg_total_score: Option<f64>,
    avg_rank: Option<f64>,
}

#[derive(Debug, Clone)]
struct SampleFeatureRow {
    signal_date: String,
    ts_code: String,
    rank: Option<i64>,
    total_score: Option<f64>,
    future_return_pct: f64,
    adv_hit_cnt: u32,
    adv_score_sum: f64,
    pos_hit_cnt: u32,
    pos_score_sum: f64,
    noisy_companion_cnt: u32,
}

#[derive(Debug, Clone)]
struct DailyPortfolioPoint {
    signal_date: String,
    portfolio_return_pct: f64,
    market_return_pct: f64,
    selected_count: usize,
}

#[derive(Debug, Clone)]
struct ScoreObservation {
    score: f64,
    future_return_pct: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdvantageRuleMode {
    Auto,
    Manual,
    Combined,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortKey {
    AdvHitCnt,
    AdvScoreSum,
    PosHitCnt,
    PosScoreSum,
    TotalScore,
    Rank,
}

fn open_source_conn(source_path: &str) -> Result<Connection, String> {
    let source_db = source_db_path(source_path);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "原始库路径不是有效UTF-8".to_string())?;
    Connection::open(source_db_str).map_err(|e| format!("打开原始库失败: {e}"))
}

fn attach_result_db(source_conn: &Connection, source_path: &str) -> Result<(), String> {
    let result_db = result_db_path(source_path);
    let result_db_str = result_db
        .to_str()
        .ok_or_else(|| "结果库路径不是有效UTF-8".to_string())?;
    let escaped = result_db_str.replace('\'', "''");
    source_conn
        .execute_batch(&format!("ATTACH '{}' AS result_db (READ_ONLY);", escaped))
        .map_err(|e| format!("挂载结果库失败: {e}"))
}

fn normalize_selected_horizon(value: Option<u32>) -> u32 {
    let requested = value.unwrap_or(DEFAULT_SELECTED_HORIZON);
    if HORIZONS.contains(&requested) {
        requested
    } else {
        DEFAULT_SELECTED_HORIZON
    }
}

fn normalize_strong_quantile(value: Option<f64>) -> Result<f64, String> {
    let quantile = value.unwrap_or(DEFAULT_STRONG_QUANTILE);
    if !(0.0..1.0).contains(&quantile) {
        return Err("strong_quantile 必须在 0 和 1 之间".to_string());
    }
    Ok(quantile)
}

fn normalize_manual_rule_names(
    requested: Option<Vec<String>>,
    strategy_options: &[String],
) -> (Vec<String>, Vec<String>) {
    let option_set = strategy_options.iter().cloned().collect::<HashSet<_>>();
    let mut manual = Vec::new();
    let mut ignored = Vec::new();
    let mut seen = HashSet::new();

    for name in requested.unwrap_or_default() {
        let trimmed = name.trim();
        if trimmed.is_empty() || !seen.insert(trimmed.to_string()) {
            continue;
        }
        if option_set.contains(trimmed) {
            manual.push(trimmed.to_string());
        } else {
            ignored.push(trimmed.to_string());
        }
    }

    (manual, ignored)
}

fn normalize_advantage_mode(value: Option<String>) -> AdvantageRuleMode {
    match value
        .as_deref()
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("manual") => AdvantageRuleMode::Manual,
        Some("combined") => AdvantageRuleMode::Combined,
        _ => AdvantageRuleMode::Auto,
    }
}

fn advantage_mode_label(mode: AdvantageRuleMode) -> String {
    match mode {
        AdvantageRuleMode::Auto => "auto".to_string(),
        AdvantageRuleMode::Manual => "manual".to_string(),
        AdvantageRuleMode::Combined => "combined".to_string(),
    }
}

fn normalize_sort_keys(requested: Option<Vec<String>>) -> Vec<SortKey> {
    let mut out = Vec::new();
    for raw in requested.unwrap_or_default() {
        let key = match raw.trim() {
            "adv_hit_cnt" => Some(SortKey::AdvHitCnt),
            "adv_score_sum" => Some(SortKey::AdvScoreSum),
            "pos_hit_cnt" => Some(SortKey::PosHitCnt),
            "pos_score_sum" => Some(SortKey::PosScoreSum),
            "total_score" => Some(SortKey::TotalScore),
            "rank" => Some(SortKey::Rank),
            _ => None,
        };
        if let Some(key) = key {
            if !out.contains(&key) {
                out.push(key);
            }
        }
    }
    if out.is_empty() {
        vec![
            SortKey::AdvHitCnt,
            SortKey::AdvScoreSum,
            SortKey::TotalScore,
            SortKey::Rank,
        ]
    } else {
        out
    }
}

fn sort_key_label(key: SortKey) -> &'static str {
    match key {
        SortKey::AdvHitCnt => "adv_hit_cnt",
        SortKey::AdvScoreSum => "adv_score_sum",
        SortKey::PosHitCnt => "pos_hit_cnt",
        SortKey::PosScoreSum => "pos_score_sum",
        SortKey::TotalScore => "total_score",
        SortKey::Rank => "rank",
    }
}

fn normalize_noisy_rule_names(
    requested: Option<Vec<String>>,
    strategy_options: &[String],
) -> Vec<String> {
    let option_set = strategy_options.iter().cloned().collect::<HashSet<_>>();
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for name in requested.unwrap_or_default() {
        let trimmed = name.trim();
        if trimmed.is_empty() || !seen.insert(trimmed.to_string()) {
            continue;
        }
        if option_set.contains(trimmed) {
            out.push(trimmed.to_string());
        }
    }
    out
}

fn scope_way_label(scope_way: ScopeWay) -> String {
    match scope_way {
        ScopeWay::Any => "ANY".to_string(),
        ScopeWay::Last => "LAST".to_string(),
        ScopeWay::Each => "EACH".to_string(),
        ScopeWay::Recent => "RECENT".to_string(),
        ScopeWay::Consec(n) => format!("CONSEC>={n}"),
    }
}

fn rule_tag_label(tag: RuleTag) -> Option<String> {
    match tag {
        RuleTag::Normal => None,
        RuleTag::Opportunity => Some("Opportunity".to_string()),
        RuleTag::Rare => Some("Rare".to_string()),
    }
}

fn load_rule_meta(source_path: &str) -> Result<(Vec<String>, HashMap<String, RuleMeta>), String> {
    let rules = ScoreRule::load_rules(source_path)?;
    let mut order = Vec::with_capacity(rules.len());
    let mut meta = HashMap::with_capacity(rules.len());
    for rule in rules {
        order.push(rule.name.clone());
        meta.insert(
            rule.name.clone(),
            RuleMeta {
                explain: rule.explain,
                tag: rule.tag,
                scope_way_label: scope_way_label(rule.scope_way),
                scope_windows: rule.scope_windows as u32,
                points: rule.points,
                has_dist_points: rule.dist_points.is_some(),
            },
        );
    }
    Ok((order, meta))
}

fn parse_scope_way_text(scope_way: &str) -> Result<ScopeWay, String> {
    match scope_way.trim().to_ascii_uppercase().as_str() {
        "ANY" => Ok(ScopeWay::Any),
        "LAST" => Ok(ScopeWay::Last),
        "EACH" => Ok(ScopeWay::Each),
        "RECENT" => Ok(ScopeWay::Recent),
        raw => {
            let Some(value) = raw.strip_prefix("CONSEC>=") else {
                return Err(format!("scope_way 不支持: {scope_way}"));
            };
            let threshold = value
                .parse::<usize>()
                .map_err(|_| format!("scope_way 连续阈值非法: {scope_way}"))?;
            if threshold == 0 {
                return Err("scope_way 连续阈值必须 >= 1".to_string());
            }
            Ok(ScopeWay::Consec(threshold))
        }
    }
}

fn parse_rule_tag_text(tag: &str) -> Result<RuleTag, String> {
    match tag.trim().to_ascii_lowercase().as_str() {
        "" | "normal" => Ok(RuleTag::Normal),
        "opportunity" => Ok(RuleTag::Opportunity),
        "rare" => Ok(RuleTag::Rare),
        other => Err(format!("tag 不支持: {other}")),
    }
}

fn estimate_custom_rule_warmup(
    stmts: &Stmts,
    scope_way: ScopeWay,
    scope_windows: usize,
) -> Result<usize, String> {
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

    let scope_extra = match scope_way {
        ScopeWay::Last => 0,
        ScopeWay::Any | ScopeWay::Each | ScopeWay::Recent => scope_windows.saturating_sub(1),
        ScopeWay::Consec(threshold) => scope_windows
            .saturating_sub(1)
            .max(threshold.saturating_sub(1)),
    };

    Ok(expr_need + scope_extra)
}

fn fill_strategy_validation_extra_fields(
    row_data: &mut RowData,
    ts_code: &str,
    is_st: bool,
) -> Result<(), String> {
    let zhang = calc_zhang_pct(ts_code, is_st);
    let zhang_series = vec![Some(zhang); row_data.trade_dates.len()];
    row_data.cols.insert("ZHANG".to_string(), zhang_series);
    row_data.validate()
}

fn build_cached_rule_from_draft(draft: &StrategyManageRuleDraft) -> Result<CachedRule, String> {
    let rule_name = draft.name.trim();
    if rule_name.is_empty() {
        return Err("策略名不能为空".to_string());
    }
    let explain = draft.explain.trim();
    if explain.is_empty() {
        return Err("策略说明不能为空".to_string());
    }
    let when = draft.when.trim();
    if when.is_empty() {
        return Err("策略表达式不能为空".to_string());
    }
    if draft.scope_windows == 0 {
        return Err("scope_windows 必须 >= 1".to_string());
    }
    if !draft.points.is_finite() {
        return Err("points 非法".to_string());
    }

    let scope_way = parse_scope_way_text(&draft.scope_way)?;
    let tag = parse_rule_tag_text(&draft.tag)?;
    let dist_points = if let Some(items) = draft.dist_points.as_ref() {
        let mut out = Vec::with_capacity(items.len());
        for (index, item) in items.iter().enumerate() {
            if item.min > item.max {
                return Err(format!("dist_points 第{}段 min > max", index + 1));
            }
            if !item.points.is_finite() {
                return Err(format!("dist_points 第{}段 points 非法", index + 1));
            }
            out.push(DistPoint {
                min: item.min,
                max: item.max,
                points: item.points,
            });
        }
        Some(out)
    } else {
        None
    };

    let tokens = lex_all(when);
    let mut parser = Parser::new(tokens);
    let stmts = parser
        .parse_main()
        .map_err(|e| format!("策略 {} 表达式解析错误在{}:{}", rule_name, e.idx, e.msg))?;

    Ok(CachedRule {
        name: rule_name.to_string(),
        scope_windows: draft.scope_windows,
        scope_way,
        points: draft.points,
        dist_points,
        tag,
        when_src: when.to_string(),
        when_ast: stmts,
    })
}

fn build_rule_meta_from_draft(
    draft: &StrategyManageRuleDraft,
) -> Result<(String, HashMap<String, RuleMeta>), String> {
    let tag = parse_rule_tag_text(&draft.tag)?;
    let rule_name = draft.name.trim().to_string();
    let mut rule_meta = HashMap::new();
    rule_meta.insert(
        rule_name.clone(),
        RuleMeta {
            explain: draft.explain.trim().to_string(),
            tag,
            scope_way_label: draft.scope_way.trim().to_ascii_uppercase(),
            scope_windows: draft.scope_windows as u32,
            points: draft.points,
            has_dist_points: draft
                .dist_points
                .as_ref()
                .map(|items| !items.is_empty())
                .unwrap_or(false),
        },
    );
    Ok((rule_name, rule_meta))
}

fn query_rank_trade_dates(source_conn: &Connection) -> Result<Vec<String>, String> {
    let mut stmt = source_conn
        .prepare(
            r#"
            SELECT DISTINCT trade_date
            FROM result_db.score_summary
            ORDER BY trade_date ASC
            "#,
        )
        .map_err(|e| format!("预编译排名日期列表失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询排名日期列表失败: {e}"))?;
    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取排名日期列表失败: {e}"))?
    {
        let trade_date: String = row.get(0).map_err(|e| format!("读取日期失败: {e}"))?;
        if !trade_date.trim().is_empty() {
            out.push(trade_date);
        }
    }
    Ok(out)
}

fn query_rank_ts_codes(source_conn: &Connection) -> Result<Vec<String>, String> {
    let mut stmt = source_conn
        .prepare(
            r#"
            SELECT DISTINCT ts_code
            FROM result_db.score_summary
            ORDER BY ts_code ASC
            "#,
        )
        .map_err(|e| format!("预编译排名股票列表失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询排名股票列表失败: {e}"))?;
    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取排名股票列表失败: {e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取 ts_code 失败: {e}"))?;
        if !ts_code.trim().is_empty() {
            out.push(ts_code);
        }
    }
    Ok(out)
}

fn query_score_summary_date_range(source_conn: &Connection) -> Result<(String, String), String> {
    let rank_dates = query_rank_trade_dates(source_conn)?;
    let Some(start_date) = rank_dates.first() else {
        return Err("score_summary 没有可用交易日".to_string());
    };
    let Some(end_date) = rank_dates.last() else {
        return Err("score_summary 没有可用交易日".to_string());
    };
    Ok((start_date.clone(), end_date.clone()))
}

fn collect_validation_rule_hits(
    source_path: &str,
    end_date: &str,
    start_date: &str,
    signal_date_set: &HashSet<String>,
    ts_codes: &[String],
    rule_cache: &CachedRule,
    need_rows: usize,
) -> Result<Vec<ScoreDetails>, String> {
    let st_list = load_st_list(source_path)?;
    let rules_cache = vec![rule_cache.clone()];

    ts_codes
        .par_chunks(256)
        .map(|ts_group| -> Result<Vec<ScoreDetails>, String> {
            let worker_reader = DataReader::new(source_path)?;
            let mut group_rows = Vec::new();

            for ts_code in ts_group {
                let mut row_data = worker_reader.load_one_tail_rows(
                    ts_code,
                    DEFAULT_ADJ_TYPE,
                    end_date,
                    need_rows,
                )?;
                fill_strategy_validation_extra_fields(
                    &mut row_data,
                    ts_code,
                    st_list.contains(ts_code),
                )?;
                let trade_dates = row_data.trade_dates.clone();
                let keep_from = trade_dates
                    .binary_search_by(|date| date.as_str().cmp(start_date))
                    .unwrap_or_else(|index| index);
                if keep_from >= trade_dates.len() {
                    continue;
                }

                let mut runtime = row_into_rt(row_data)?;
                let (_, mut details) = scoring_rules_details_cache(&mut runtime, &rules_cache)?;
                let Some(rule_series) = details.pop() else {
                    continue;
                };

                for (offset, trade_date) in trade_dates.iter().enumerate().skip(keep_from) {
                    if !signal_date_set.contains(trade_date) {
                        continue;
                    }
                    let rule_score = rule_series.series.get(offset).copied().unwrap_or(0.0);
                    if rule_score == 0.0 {
                        continue;
                    }
                    group_rows.push(ScoreDetails {
                        ts_code: ts_code.clone(),
                        trade_date: trade_date.clone(),
                        rule_name: rule_cache.name.clone(),
                        rule_score,
                    });
                }
            }

            Ok(group_rows)
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|groups| groups.into_iter().flatten().collect())
}

fn prepare_temp_validation_rule_details(
    source_conn: &Connection,
    source_path: &str,
    draft: &StrategyManageRuleDraft,
) -> Result<(String, HashMap<String, RuleMeta>), String> {
    let rule_cache = build_cached_rule_from_draft(draft)?;
    let warmup_need = estimate_custom_rule_warmup(
        &rule_cache.when_ast,
        rule_cache.scope_way,
        rule_cache.scope_windows,
    )?;
    let (start_date, end_date) = query_score_summary_date_range(source_conn)?;
    let rank_dates = query_rank_trade_dates(source_conn)?;
    let signal_date_set = rank_dates.into_iter().collect::<HashSet<_>>();
    let ts_codes = query_rank_ts_codes(source_conn)?;
    let need_rows = calc_query_need_rows(source_path, warmup_need, &start_date, &end_date)?;
    let hit_rows = collect_validation_rule_hits(
        source_path,
        &end_date,
        &start_date,
        &signal_date_set,
        &ts_codes,
        &rule_cache,
        need_rows,
    )?;

    source_conn
        .execute_batch(&format!(
            r#"
            DROP TABLE IF EXISTS {VALIDATION_DETAILS_TABLE};
            CREATE TEMP TABLE {VALIDATION_DETAILS_TABLE} (
                ts_code VARCHAR,
                trade_date VARCHAR,
                rule_name VARCHAR,
                rule_score DOUBLE
            );
            "#
        ))
        .map_err(|e| format!("创建临时草稿策略明细表失败: {e}"))?;

    {
        let mut appender = source_conn
            .appender(VALIDATION_DETAILS_TABLE)
            .map_err(|e| format!("创建临时草稿策略明细 Appender 失败: {e}"))?;
        for row in hit_rows {
            appender
                .append_row(params![
                    row.ts_code,
                    row.trade_date,
                    row.rule_name,
                    row.rule_score
                ])
                .map_err(|e| format!("写入临时草稿策略明细失败: {e}"))?;
        }
        appender
            .flush()
            .map_err(|e| format!("刷新临时草稿策略明细 Appender 失败: {e}"))?;
    }

    build_rule_meta_from_draft(draft)
}

fn prepare_temp_exit_map(source_conn: &Connection, source_path: &str) -> Result<(), String> {
    source_conn
        .execute_batch(
            r#"
            DROP TABLE IF EXISTS strategy_perf_exit_map;
            CREATE TEMP TABLE strategy_perf_exit_map (
                signal_date VARCHAR,
                horizon INTEGER,
                exit_trade_date VARCHAR
            );
            "#,
        )
        .map_err(|e| format!("创建临时 horizon 映射表失败: {e}"))?;

    let rank_dates = query_rank_trade_dates(source_conn)?;
    let mut trade_dates = load_trade_date_list(source_path)?;
    trade_dates.sort();
    trade_dates.dedup();
    let trade_date_index = trade_dates
        .iter()
        .enumerate()
        .map(|(index, item)| (item.clone(), index))
        .collect::<HashMap<_, _>>();

    {
        let mut appender = source_conn
            .appender("strategy_perf_exit_map")
            .map_err(|e| format!("创建 horizon 映射 Appender 失败: {e}"))?;

        for signal_date in rank_dates {
            let Some(&signal_index) = trade_date_index.get(&signal_date) else {
                continue;
            };
            for horizon in HORIZONS {
                let exit_index = signal_index + horizon as usize;
                let Some(exit_trade_date) = trade_dates.get(exit_index) else {
                    continue;
                };
                appender
                    .append_row(params![signal_date, horizon as i64, exit_trade_date])
                    .map_err(|e| format!("写入 horizon 映射失败: {e}"))?;
            }
        }

        appender
            .flush()
            .map_err(|e| format!("刷新 horizon 映射 Appender 失败: {e}"))?;
    }

    Ok(())
}

fn prepare_temp_sample_returns(source_conn: &Connection) -> Result<(), String> {
    source_conn
        .execute_batch(&format!(
            r#"
            DROP TABLE IF EXISTS strategy_perf_sample_returns;
            CREATE TEMP TABLE strategy_perf_sample_returns AS
            WITH summary AS (
                SELECT
                    ts_code,
                    trade_date AS signal_date,
                    total_score,
                    rank
                FROM result_db.score_summary
            ),
            next_open AS (
                SELECT
                    ts_code,
                    signal_date,
                    entry_trade_date,
                    entry_open
                FROM (
                    SELECT
                        s.ts_code,
                        s.signal_date,
                        d.trade_date AS entry_trade_date,
                        TRY_CAST(d.open AS DOUBLE) AS entry_open,
                        ROW_NUMBER() OVER (
                            PARTITION BY s.ts_code, s.signal_date
                            ORDER BY d.trade_date ASC
                        ) AS rn
                    FROM summary AS s
                    INNER JOIN stock_data AS d
                        ON d.ts_code = s.ts_code
                       AND d.adj_type = '{DEFAULT_ADJ_TYPE}'
                       AND d.trade_date > s.signal_date
                ) AS ranked_next_open
                WHERE rn = 1
            )
            SELECT
                s.ts_code,
                s.signal_date,
                s.total_score,
                s.rank,
                m.horizon,
                n.entry_trade_date,
                n.entry_open,
                m.exit_trade_date,
                TRY_CAST(c.close AS DOUBLE) AS exit_close,
                (TRY_CAST(c.close AS DOUBLE) / n.entry_open - 1.0) * 100.0 AS future_return_pct
            FROM summary AS s
            INNER JOIN next_open AS n
                ON n.ts_code = s.ts_code
               AND n.signal_date = s.signal_date
            INNER JOIN strategy_perf_exit_map AS m
                ON m.signal_date = s.signal_date
            INNER JOIN stock_data AS c
                ON c.ts_code = s.ts_code
               AND c.adj_type = '{DEFAULT_ADJ_TYPE}'
               AND c.trade_date = m.exit_trade_date
            WHERE n.entry_open > 0
              AND n.entry_trade_date <= m.exit_trade_date;
            "#,
        ))
        .map_err(|e| format!("构建临时未来收益样本失败: {e}"))?;
    Ok(())
}

fn prepare_temp_thresholds(source_conn: &Connection, strong_quantile: f64) -> Result<(), String> {
    source_conn
        .execute_batch("DROP TABLE IF EXISTS strategy_perf_thresholds;")
        .map_err(|e| format!("删除临时阈值表失败: {e}"))?;
    source_conn
        .execute(
            r#"
            CREATE TEMP TABLE strategy_perf_thresholds AS
            SELECT
                horizon,
                COUNT(*) AS sample_count,
                AVG(future_return_pct) AS avg_future_return_pct,
                QUANTILE_CONT(future_return_pct, 0.80) AS p80_return_pct,
                QUANTILE_CONT(future_return_pct, 0.90) AS p90_return_pct,
                QUANTILE_CONT(future_return_pct, 0.95) AS p95_return_pct,
                QUANTILE_CONT(future_return_pct, ?) AS strong_threshold_pct,
                MAX(future_return_pct) AS max_future_return_pct,
                AVG(CASE WHEN future_return_pct > 0 THEN 1.0 ELSE 0.0 END) AS win_rate
            FROM strategy_perf_sample_returns
            GROUP BY horizon
            "#,
            params![strong_quantile],
        )
        .map_err(|e| format!("创建临时阈值表失败: {e}"))?;
    Ok(())
}

fn load_future_summaries(
    source_conn: &Connection,
    strong_quantile: f64,
) -> Result<Vec<StrategyPerformanceFutureSummary>, String> {
    let mut stmt = source_conn
        .prepare(
            r#"
            SELECT
                t.horizon,
                t.sample_count,
                t.avg_future_return_pct,
                t.p80_return_pct,
                t.p90_return_pct,
                t.p95_return_pct,
                t.strong_threshold_pct,
                AVG(CASE WHEN r.future_return_pct >= t.strong_threshold_pct THEN 1.0 ELSE 0.0 END) AS strong_base_rate,
                t.win_rate,
                t.max_future_return_pct
            FROM strategy_perf_thresholds AS t
            INNER JOIN strategy_perf_sample_returns AS r
                ON r.horizon = t.horizon
            GROUP BY
                t.horizon,
                t.sample_count,
                t.avg_future_return_pct,
                t.p80_return_pct,
                t.p90_return_pct,
                t.p95_return_pct,
                t.strong_threshold_pct,
                t.win_rate,
                t.max_future_return_pct
            ORDER BY t.horizon ASC
            "#,
        )
        .map_err(|e| format!("预编译未来收益摘要失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询未来收益摘要失败: {e}"))?;
    let mut raw = HashMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取未来收益摘要失败: {e}"))?
    {
        let horizon: i64 = row.get(0).map_err(|e| format!("读取 horizon 失败: {e}"))?;
        raw.insert(
            horizon as u32,
            StrategyPerformanceFutureSummary {
                horizon: horizon as u32,
                sample_count: row
                    .get::<_, i64>(1)
                    .map_err(|e| format!("读取样本数失败: {e}"))?
                    .max(0) as u32,
                avg_future_return_pct: row.get(2).map_err(|e| format!("读取平均收益失败: {e}"))?,
                p80_return_pct: row.get(3).map_err(|e| format!("读取 p80 失败: {e}"))?,
                p90_return_pct: row.get(4).map_err(|e| format!("读取 p90 失败: {e}"))?,
                p95_return_pct: row.get(5).map_err(|e| format!("读取 p95 失败: {e}"))?,
                strong_quantile,
                strong_threshold_pct: row.get(6).map_err(|e| format!("读取强势阈值失败: {e}"))?,
                strong_base_rate: row
                    .get(7)
                    .map_err(|e| format!("读取强势基准占比失败: {e}"))?,
                win_rate: row.get(8).map_err(|e| format!("读取胜率失败: {e}"))?,
                max_future_return_pct: row.get(9).map_err(|e| format!("读取最大收益失败: {e}"))?,
            },
        );
    }

    let out = HORIZONS
        .iter()
        .map(|horizon| {
            raw.remove(horizon)
                .unwrap_or(StrategyPerformanceFutureSummary {
                    horizon: *horizon,
                    sample_count: 0,
                    avg_future_return_pct: None,
                    p80_return_pct: None,
                    p90_return_pct: None,
                    p95_return_pct: None,
                    strong_quantile,
                    strong_threshold_pct: None,
                    strong_base_rate: None,
                    win_rate: None,
                    max_future_return_pct: None,
                })
        })
        .collect::<Vec<_>>();

    Ok(out)
}

fn load_rule_aggregates(
    source_conn: &Connection,
    detail_table_name: &str,
) -> Result<HashMap<(String, bool, u32), RuleAggMetric>, String> {
    let mut stmt = source_conn
        .prepare(&format!(
            r#"
            SELECT
                r.horizon,
                d.rule_name,
                CASE WHEN d.rule_score > 0 THEN TRUE ELSE FALSE END AS is_positive,
                COUNT(*) AS hit_n,
                AVG(r.future_return_pct) AS avg_future_return_pct,
                AVG(CASE WHEN r.future_return_pct >= t.strong_threshold_pct THEN 1.0 ELSE 0.0 END) AS strong_hit_rate,
                AVG(CASE WHEN r.future_return_pct > 0 THEN 1.0 ELSE 0.0 END) AS win_rate,
                AVG(r.total_score) AS avg_total_score,
                AVG(CASE WHEN r.rank IS NULL THEN NULL ELSE CAST(r.rank AS DOUBLE) END) AS avg_rank
            FROM strategy_perf_sample_returns AS r
            INNER JOIN {detail_table_name} AS d
                ON d.ts_code = r.ts_code
               AND d.trade_date = r.signal_date
               AND d.rule_score != 0
            INNER JOIN strategy_perf_thresholds AS t
                ON t.horizon = r.horizon
            GROUP BY r.horizon, d.rule_name, is_positive
            "#,
        ))
        .map_err(|e| format!("预编译规则统计失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询规则统计失败: {e}"))?;
    let mut out = HashMap::new();
    while let Some(row) = rows.next().map_err(|e| format!("读取规则统计失败: {e}"))? {
        let horizon: i64 = row.get(0).map_err(|e| format!("读取 horizon 失败: {e}"))?;
        let rule_name: String = row
            .get(1)
            .map_err(|e| format!("读取 rule_name 失败: {e}"))?;
        let is_positive: bool = row.get(2).map_err(|e| format!("读取命中方向失败: {e}"))?;
        out.insert(
            (rule_name, is_positive, horizon as u32),
            RuleAggMetric {
                hit_n: row
                    .get::<_, i64>(3)
                    .map_err(|e| format!("读取命中数失败: {e}"))?
                    .max(0) as u32,
                avg_future_return_pct: row.get(4).map_err(|e| format!("读取平均收益失败: {e}"))?,
                strong_hit_rate: row.get(5).map_err(|e| format!("读取强势命中率失败: {e}"))?,
                win_rate: row.get(6).map_err(|e| format!("读取胜率失败: {e}"))?,
                avg_total_score: row.get(7).map_err(|e| format!("读取平均总分失败: {e}"))?,
                avg_rank: row.get(8).map_err(|e| format!("读取平均排名失败: {e}"))?,
            },
        );
    }
    Ok(out)
}

fn min_sample_for_horizon(config: &StrategyPerformanceAutoFilterConfig, horizon: u32) -> u32 {
    match horizon {
        2 => config.min_samples_2,
        3 => config.min_samples_3,
        5 => config.min_samples_5,
        10 => config.min_samples_10,
        _ => DEFAULT_MIN_SAMPLE,
    }
}

fn passes_positive_auto_filter(
    agg: RuleAggMetric,
    market_summary: Option<&StrategyPerformanceFutureSummary>,
    strong_lift: Option<f64>,
    auto_filter: &StrategyPerformanceAutoFilterConfig,
    horizon: u32,
) -> bool {
    let market_avg = market_summary.and_then(|summary| summary.avg_future_return_pct);
    let market_win = market_summary.and_then(|summary| summary.win_rate);
    agg.hit_n >= min_sample_for_horizon(auto_filter, horizon)
        && match (agg.avg_future_return_pct, market_avg) {
            (Some(rule_avg), Some(market_avg)) => rule_avg > market_avg,
            _ => false,
        }
        && strong_lift.unwrap_or(0.0) > 1.0
        && if auto_filter.require_win_rate_above_market {
            match (agg.win_rate, market_win) {
                (Some(rule_win), Some(market_win)) => rule_win > market_win,
                _ => false,
            }
        } else {
            true
        }
}

fn passes_negative_effective_filter(
    agg: RuleAggMetric,
    market_summary: Option<&StrategyPerformanceFutureSummary>,
    strong_lift: Option<f64>,
    hit_vs_non_hit_delta_pct: Option<f64>,
    auto_filter: &StrategyPerformanceAutoFilterConfig,
    horizon: u32,
) -> bool {
    let market_avg = market_summary.and_then(|summary| summary.avg_future_return_pct);
    let market_win = market_summary.and_then(|summary| summary.win_rate);
    agg.hit_n >= min_sample_for_horizon(auto_filter, horizon)
        && match (agg.avg_future_return_pct, market_avg) {
            (Some(rule_avg), Some(market_avg)) => rule_avg < market_avg,
            _ => false,
        }
        && strong_lift.map(|lift| lift < 1.0).unwrap_or(false)
        && match (agg.win_rate, market_win) {
            (Some(rule_win), Some(market_win)) => rule_win < market_win,
            _ => false,
        }
        && hit_vs_non_hit_delta_pct
            .map(|delta| delta < 0.0)
            .unwrap_or(false)
}

fn build_negative_review_notes(
    metrics: &[StrategyPerformanceHorizonMetric],
    selected_horizon: u32,
    selected_market_summary: Option<&StrategyPerformanceFutureSummary>,
    auto_filter: &StrategyPerformanceAutoFilterConfig,
) -> Vec<String> {
    let Some(selected_metric) = metric_for_horizon(metrics, selected_horizon) else {
        return vec!["当前周期无样本".to_string()];
    };

    let pass_count = metrics
        .iter()
        .filter(|metric| metric.passes_negative_filter)
        .count() as u32;
    let required_pass_count = auto_filter.min_pass_horizons;
    if selected_metric.passes_negative_filter && pass_count >= required_pass_count {
        return vec![
            "当前周期已转弱".to_string(),
            format!("{pass_count} 个周期方向一致"),
        ];
    }

    let mut notes = Vec::new();
    let min_sample = min_sample_for_horizon(auto_filter, selected_horizon);
    if selected_metric.hit_n < min_sample {
        notes.push(format!(
            "样本偏少({}/{})",
            selected_metric.hit_n, min_sample
        ));
    }

    if let (Some(rule_avg), Some(market_avg)) = (
        selected_metric.avg_future_return_pct,
        selected_market_summary.and_then(|summary| summary.avg_future_return_pct),
    ) {
        if rule_avg >= market_avg {
            notes.push("均收益不弱于市场".to_string());
        }
    }

    if let Some(strong_lift) = selected_metric.strong_lift {
        if strong_lift >= 1.0 {
            notes.push("赢家占比偏高".to_string());
        }
    }

    if let (Some(rule_win), Some(market_win)) = (
        selected_metric.win_rate,
        selected_market_summary.and_then(|summary| summary.win_rate),
    ) {
        if rule_win >= market_win {
            notes.push("胜率不低于市场".to_string());
        }
    }

    if let Some(delta) = selected_metric.hit_vs_non_hit_delta_pct {
        if delta >= 0.0 {
            notes.push("命中后不比 non-hit 更弱".to_string());
        }
    }

    if pass_count < required_pass_count {
        notes.push(format!("仅 {pass_count}/{required_pass_count} 个周期转弱"));
    }

    if notes.is_empty() {
        notes.push("当前更接近负向，但稳定性仍待观察".to_string());
    }
    notes
}

fn build_rule_rows(
    strategy_options: &[String],
    rule_meta: &HashMap<String, RuleMeta>,
    rule_aggregates: &HashMap<(String, bool, u32), RuleAggMetric>,
    future_summary_map: &HashMap<u32, StrategyPerformanceFutureSummary>,
    auto_filter: &StrategyPerformanceAutoFilterConfig,
    selected_horizon: u32,
    manual_rule_names: &[String],
    resolved_advantage_rules: &HashSet<String>,
) -> (Vec<StrategyPerformanceRuleRow>, Vec<String>) {
    let manual_set = manual_rule_names.iter().cloned().collect::<HashSet<_>>();
    let mut auto_candidates = Vec::new();
    let mut rows = Vec::new();

    for rule_name in strategy_options {
        let Some(meta) = rule_meta.get(rule_name) else {
            continue;
        };

        for is_positive in [true, false] {
            let mut metrics = Vec::with_capacity(HORIZONS.len());
            let mut any_hit = false;
            let mut positive_pass_count = 0u32;
            let mut negative_pass_count = 0u32;

            for horizon in HORIZONS {
                let agg = rule_aggregates
                    .get(&(rule_name.clone(), is_positive, horizon))
                    .copied()
                    .unwrap_or_default();
                if agg.hit_n > 0 {
                    any_hit = true;
                }
                let market_summary = future_summary_map.get(&horizon);
                let strong_lift = match (
                    agg.strong_hit_rate,
                    market_summary.and_then(|summary| summary.strong_base_rate),
                ) {
                    (Some(rule_rate), Some(base_rate)) if base_rate > 0.0 => {
                        Some(rule_rate / base_rate)
                    }
                    _ => None,
                };
                let low_confidence = agg.hit_n < min_sample_for_horizon(auto_filter, horizon);
                let hit_vs_non_hit_delta_pct = match (
                    agg.avg_future_return_pct,
                    non_hit_avg(
                        market_summary
                            .map(|summary| summary.sample_count)
                            .unwrap_or_default(),
                        market_summary.and_then(|summary| summary.avg_future_return_pct),
                        agg.hit_n,
                        agg.avg_future_return_pct,
                    ),
                ) {
                    (Some(hit_avg), Some(non_hit_avg)) => Some(hit_avg - non_hit_avg),
                    _ => None,
                };
                let passes_auto_filter = if is_positive {
                    passes_positive_auto_filter(
                        agg,
                        market_summary,
                        strong_lift,
                        auto_filter,
                        horizon,
                    )
                } else {
                    false
                };
                let passes_negative_filter = if is_positive {
                    false
                } else {
                    passes_negative_effective_filter(
                        agg,
                        market_summary,
                        strong_lift,
                        hit_vs_non_hit_delta_pct,
                        auto_filter,
                        horizon,
                    )
                };
                if passes_auto_filter {
                    positive_pass_count += 1;
                }
                if passes_negative_filter {
                    negative_pass_count += 1;
                }
                metrics.push(StrategyPerformanceHorizonMetric {
                    horizon,
                    hit_n: agg.hit_n,
                    avg_future_return_pct: agg.avg_future_return_pct,
                    strong_hit_rate: agg.strong_hit_rate,
                    strong_lift,
                    win_rate: agg.win_rate,
                    avg_total_score: agg.avg_total_score,
                    avg_rank: agg.avg_rank,
                    hit_vs_non_hit_delta_pct,
                    low_confidence,
                    passes_auto_filter,
                    passes_negative_filter,
                });
            }

            if !is_positive && !any_hit {
                continue;
            }

            let auto_candidate =
                is_positive && positive_pass_count >= auto_filter.min_pass_horizons;
            if auto_candidate {
                auto_candidates.push(rule_name.clone());
            }
            let negative_review_notes = if is_positive {
                Vec::new()
            } else {
                build_negative_review_notes(
                    &metrics,
                    selected_horizon,
                    future_summary_map.get(&selected_horizon),
                    auto_filter,
                )
            };
            let negative_effective = if is_positive {
                None
            } else {
                Some(
                    negative_pass_count >= auto_filter.min_pass_horizons
                        && metric_for_horizon(&metrics, selected_horizon)
                            .map(|metric| metric.passes_negative_filter)
                            .unwrap_or(false),
                )
            };
            rows.push(StrategyPerformanceRuleRow {
                rule_name: rule_name.clone(),
                explain: Some(meta.explain.clone()),
                tag: rule_tag_label(meta.tag),
                scope_way: Some(meta.scope_way_label.clone()),
                scope_windows: Some(meta.scope_windows),
                points: Some(meta.points),
                has_dist_points: meta.has_dist_points,
                signal_direction: if is_positive {
                    "positive".to_string()
                } else {
                    "negative".to_string()
                },
                direction_label: if is_positive {
                    "正向命中".to_string()
                } else {
                    "负向命中".to_string()
                },
                auto_candidate,
                manually_selected: is_positive && manual_set.contains(rule_name),
                in_advantage_set: is_positive && resolved_advantage_rules.contains(rule_name),
                in_companion_set: false,
                negative_effective,
                negative_effectiveness_label: negative_effective.map(|is_effective| {
                    if is_effective {
                        "方向明确负向".to_string()
                    } else {
                        "待验证负向".to_string()
                    }
                }),
                negative_review_notes,
                metrics,
            });
        }
    }

    let index_map = strategy_options
        .iter()
        .enumerate()
        .map(|(index, name)| (name.clone(), index))
        .collect::<HashMap<_, _>>();

    rows.sort_by(|left, right| {
        let left_positive = left.signal_direction == "positive";
        let right_positive = right.signal_direction == "positive";
        left_positive
            .cmp(&right_positive)
            .reverse()
            .then_with(|| {
                if left_positive && right_positive {
                    right
                        .auto_candidate
                        .cmp(&left.auto_candidate)
                        .then_with(|| {
                            metric_for_horizon(&right.metrics, selected_horizon)
                                .and_then(|metric| metric.strong_lift)
                                .partial_cmp(
                                    &metric_for_horizon(&left.metrics, selected_horizon)
                                        .and_then(|metric| metric.strong_lift),
                                )
                                .unwrap_or(Ordering::Equal)
                        })
                        .then_with(|| {
                            metric_for_horizon(&right.metrics, selected_horizon)
                                .and_then(|metric| metric.avg_future_return_pct)
                                .partial_cmp(
                                    &metric_for_horizon(&left.metrics, selected_horizon)
                                        .and_then(|metric| metric.avg_future_return_pct),
                                )
                                .unwrap_or(Ordering::Equal)
                        })
                        .then_with(|| {
                            metric_for_horizon(&right.metrics, selected_horizon)
                                .map(|metric| metric.hit_n)
                                .cmp(
                                    &metric_for_horizon(&left.metrics, selected_horizon)
                                        .map(|metric| metric.hit_n),
                                )
                        })
                } else {
                    right
                        .negative_effective
                        .unwrap_or(false)
                        .cmp(&left.negative_effective.unwrap_or(false))
                        .then_with(|| {
                            metric_for_horizon(&left.metrics, selected_horizon)
                                .and_then(|metric| metric.hit_vs_non_hit_delta_pct)
                                .partial_cmp(
                                    &metric_for_horizon(&right.metrics, selected_horizon)
                                        .and_then(|metric| metric.hit_vs_non_hit_delta_pct),
                                )
                                .unwrap_or(Ordering::Equal)
                        })
                        .then_with(|| {
                            metric_for_horizon(&left.metrics, selected_horizon)
                                .and_then(|metric| metric.strong_lift)
                                .partial_cmp(
                                    &metric_for_horizon(&right.metrics, selected_horizon)
                                        .and_then(|metric| metric.strong_lift),
                                )
                                .unwrap_or(Ordering::Equal)
                        })
                        .then_with(|| {
                            metric_for_horizon(&left.metrics, selected_horizon)
                                .and_then(|metric| metric.avg_future_return_pct)
                                .partial_cmp(
                                    &metric_for_horizon(&right.metrics, selected_horizon)
                                        .and_then(|metric| metric.avg_future_return_pct),
                                )
                                .unwrap_or(Ordering::Equal)
                        })
                        .then_with(|| {
                            metric_for_horizon(&right.metrics, selected_horizon)
                                .map(|metric| metric.hit_n)
                                .cmp(
                                    &metric_for_horizon(&left.metrics, selected_horizon)
                                        .map(|metric| metric.hit_n),
                                )
                        })
                }
            })
            .then_with(|| {
                index_map
                    .get(&left.rule_name)
                    .copied()
                    .unwrap_or(usize::MAX)
                    .cmp(
                        &index_map
                            .get(&right.rule_name)
                            .copied()
                            .unwrap_or(usize::MAX),
                    )
            })
    });

    auto_candidates.sort_by(|left, right| {
        let left_metric = rows
            .iter()
            .find(|row| row.rule_name == *left && row.signal_direction == "positive")
            .and_then(|row| metric_for_horizon(&row.metrics, selected_horizon))
            .cloned();
        let right_metric = rows
            .iter()
            .find(|row| row.rule_name == *right && row.signal_direction == "positive")
            .and_then(|row| metric_for_horizon(&row.metrics, selected_horizon))
            .cloned();
        right_metric
            .as_ref()
            .and_then(|metric| metric.strong_lift)
            .partial_cmp(&left_metric.as_ref().and_then(|metric| metric.strong_lift))
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                right_metric
                    .as_ref()
                    .and_then(|metric| metric.avg_future_return_pct)
                    .partial_cmp(
                        &left_metric
                            .as_ref()
                            .and_then(|metric| metric.avg_future_return_pct),
                    )
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| {
                right_metric
                    .as_ref()
                    .map(|metric| metric.hit_n)
                    .cmp(&left_metric.as_ref().map(|metric| metric.hit_n))
            })
            .then_with(|| {
                index_map
                    .get(left)
                    .copied()
                    .unwrap_or(usize::MAX)
                    .cmp(&index_map.get(right).copied().unwrap_or(usize::MAX))
            })
    });
    auto_candidates.dedup();
    auto_candidates.truncate(DEFAULT_AUTO_ADVANTAGE_LIMIT);

    (rows, auto_candidates)
}

fn row_has_positive_hits(row: &StrategyPerformanceRuleRow) -> bool {
    row.signal_direction == "positive" && row.metrics.iter().any(|metric| metric.hit_n > 0)
}

fn build_resolved_companion_rule_names(rows: &[StrategyPerformanceRuleRow]) -> Vec<String> {
    rows.iter()
        .filter(|row| row.in_companion_set && row_has_positive_hits(row))
        .map(|row| row.rule_name.clone())
        .collect()
}

fn build_negative_rule_names(
    rows: &[StrategyPerformanceRuleRow],
    negative_effective: bool,
) -> Vec<String> {
    rows.iter()
        .filter(|row| {
            row.signal_direction == "negative" && row.negative_effective == Some(negative_effective)
        })
        .map(|row| row.rule_name.clone())
        .collect()
}

fn metric_for_horizon(
    metrics: &[StrategyPerformanceHorizonMetric],
    horizon: u32,
) -> Option<&StrategyPerformanceHorizonMetric> {
    metrics.iter().find(|metric| metric.horizon == horizon)
}

fn resolve_advantage_rule_names(
    strategy_options: &[String],
    mode: AdvantageRuleMode,
    auto_candidates: &[String],
    manual_rule_names: &[String],
) -> Vec<String> {
    let auto_set = auto_candidates.iter().cloned().collect::<HashSet<_>>();
    let manual_set = manual_rule_names.iter().cloned().collect::<HashSet<_>>();
    strategy_options
        .iter()
        .filter(|name| match mode {
            AdvantageRuleMode::Auto => auto_set.contains(*name),
            AdvantageRuleMode::Manual => manual_set.contains(*name),
            AdvantageRuleMode::Combined => auto_set.contains(*name) || manual_set.contains(*name),
        })
        .cloned()
        .collect()
}

fn prepare_temp_string_table(
    source_conn: &Connection,
    table_name: &str,
    values: &[String],
) -> Result<(), String> {
    source_conn
        .execute_batch(&format!(
            r#"
            DROP TABLE IF EXISTS {table_name};
            CREATE TEMP TABLE {table_name} (
                rule_name VARCHAR
            );
            "#,
        ))
        .map_err(|e| format!("创建临时字符串表 {table_name} 失败: {e}"))?;
    if values.is_empty() {
        return Ok(());
    }
    let insert_sql = format!("INSERT INTO {table_name} (rule_name) VALUES (?)");
    let mut stmt = source_conn
        .prepare(&insert_sql)
        .map_err(|e| format!("预编译临时字符串表写入失败: {e}"))?;
    for value in values {
        stmt.execute(params![value])
            .map_err(|e| format!("写入临时字符串表失败: {e}"))?;
    }
    Ok(())
}

fn rebuild_temp_sample_features(
    source_conn: &Connection,
    selected_horizon: u32,
) -> Result<(), String> {
    source_conn
        .execute_batch("DROP TABLE IF EXISTS strategy_perf_sample_features;")
        .map_err(|e| format!("删除旧样本特征表失败: {e}"))?;
    source_conn
        .execute(
            r#"
            CREATE TEMP TABLE strategy_perf_sample_features AS
            WITH horizon_returns AS (
                SELECT *
                FROM strategy_perf_sample_returns
                WHERE horizon = ?
            )
            SELECT
                r.signal_date,
                r.ts_code,
                r.rank,
                r.total_score,
                r.future_return_pct,
                SUM(CASE WHEN a.rule_name IS NOT NULL AND d.rule_score > 0 THEN 1 ELSE 0 END) AS adv_hit_cnt,
                SUM(CASE WHEN a.rule_name IS NOT NULL AND d.rule_score > 0 THEN d.rule_score ELSE 0 END) AS adv_score_sum,
                SUM(CASE WHEN d.rule_score > 0 THEN 1 ELSE 0 END) AS pos_hit_cnt,
                SUM(CASE WHEN d.rule_score > 0 THEN d.rule_score ELSE 0 END) AS pos_score_sum,
                SUM(CASE WHEN d.rule_score != 0 THEN 1 ELSE 0 END) AS all_hit_cnt,
                SUM(CASE WHEN d.rule_score != 0 THEN d.rule_score ELSE 0 END) AS all_score_sum,
                SUM(CASE WHEN n.rule_name IS NOT NULL AND d.rule_score > 0 THEN 1 ELSE 0 END) AS noisy_companion_cnt
            FROM horizon_returns AS r
            LEFT JOIN result_db.score_details AS d
                ON d.ts_code = r.ts_code
               AND d.trade_date = r.signal_date
               AND d.rule_score != 0
            LEFT JOIN strategy_perf_advantage_rules AS a
                ON a.rule_name = d.rule_name
            LEFT JOIN strategy_perf_noisy_rules AS n
                ON n.rule_name = d.rule_name
            GROUP BY
                r.signal_date,
                r.ts_code,
                r.rank,
                r.total_score,
                r.future_return_pct
            "#,
            params![selected_horizon as i64],
        )
        .map_err(|e| format!("构建样本特征临时表失败: {e}"))?;
    Ok(())
}

fn load_companion_rows(
    source_conn: &Connection,
    min_adv_hits: u32,
    selected_min_sample: u32,
) -> Result<Vec<StrategyPerformanceCompanionRow>, String> {
    let mut baseline_stmt = source_conn
        .prepare(
            r#"
            SELECT
                COUNT(*) AS eligible_n,
                AVG(future_return_pct) AS eligible_avg_return_pct,
                AVG(CASE WHEN future_return_pct > 0 THEN 1.0 ELSE 0.0 END) AS eligible_win_rate
            FROM strategy_perf_sample_features
            WHERE adv_hit_cnt >= ?
            "#,
        )
        .map_err(|e| format!("预编译优势池基准失败: {e}"))?;
    let mut baseline_rows = baseline_stmt
        .query(params![min_adv_hits as i64])
        .map_err(|e| format!("查询优势池基准失败: {e}"))?;
    let Some(baseline_row) = baseline_rows
        .next()
        .map_err(|e| format!("读取优势池基准失败: {e}"))?
    else {
        return Ok(Vec::new());
    };
    let eligible_n = baseline_row
        .get::<_, i64>(0)
        .map_err(|e| format!("读取优势池样本数失败: {e}"))?
        .max(0) as u32;
    let eligible_avg_return_pct: Option<f64> = baseline_row
        .get(1)
        .map_err(|e| format!("读取优势池平均收益失败: {e}"))?;
    let eligible_win_rate: Option<f64> = baseline_row
        .get(2)
        .map_err(|e| format!("读取优势池胜率失败: {e}"))?;
    if eligible_n == 0 {
        return Ok(Vec::new());
    }

    let mut stmt = source_conn
        .prepare(
            r#"
            SELECT
                d.rule_name,
                COUNT(*) AS hit_n,
                AVG(f.future_return_pct) AS avg_future_return_pct,
                AVG(CASE WHEN f.future_return_pct > 0 THEN 1.0 ELSE 0.0 END) AS win_rate
            FROM strategy_perf_sample_features AS f
            INNER JOIN result_db.score_details AS d
                ON d.ts_code = f.ts_code
               AND d.trade_date = f.signal_date
               AND d.rule_score > 0
            LEFT JOIN strategy_perf_advantage_rules AS a
                ON a.rule_name = d.rule_name
            WHERE f.adv_hit_cnt >= ?
              AND a.rule_name IS NULL
            GROUP BY d.rule_name
            ORDER BY AVG(f.future_return_pct) DESC, COUNT(*) DESC, d.rule_name ASC
            "#,
        )
        .map_err(|e| format!("预编译伴随策略分析失败: {e}"))?;
    let mut rows = stmt
        .query(params![min_adv_hits as i64])
        .map_err(|e| format!("查询伴随策略分析失败: {e}"))?;
    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取伴随策略分析失败: {e}"))?
    {
        let hit_n = row
            .get::<_, i64>(1)
            .map_err(|e| format!("读取伴随命中数失败: {e}"))?
            .max(0) as u32;
        let avg_future_return_pct: Option<f64> = row
            .get(2)
            .map_err(|e| format!("读取伴随平均收益失败: {e}"))?;
        let win_rate: Option<f64> = row.get(3).map_err(|e| format!("读取伴随胜率失败: {e}"))?;
        out.push(StrategyPerformanceCompanionRow {
            rule_name: row.get(0).map_err(|e| format!("读取伴随规则名失败: {e}"))?,
            hit_n,
            avg_future_return_pct,
            eligible_pool_avg_return_pct: eligible_avg_return_pct,
            delta_return_pct: match (avg_future_return_pct, eligible_avg_return_pct) {
                (Some(rule_avg), Some(pool_avg)) => Some(rule_avg - pool_avg),
                _ => None,
            },
            win_rate,
            eligible_pool_win_rate: eligible_win_rate,
            delta_win_rate: match (win_rate, eligible_win_rate) {
                (Some(rule_win), Some(pool_win)) => Some(rule_win - pool_win),
                _ => None,
            },
            low_confidence: hit_n < selected_min_sample,
        });
    }

    out.sort_by(|left, right| {
        right
            .delta_return_pct
            .partial_cmp(&left.delta_return_pct)
            .unwrap_or(Ordering::Equal)
            .then_with(|| right.hit_n.cmp(&left.hit_n))
            .then_with(|| left.rule_name.cmp(&right.rule_name))
    });

    Ok(out)
}

fn load_sample_feature_rows(source_conn: &Connection) -> Result<Vec<SampleFeatureRow>, String> {
    let mut stmt = source_conn
        .prepare(
            r#"
            SELECT
                signal_date,
                ts_code,
                rank,
                total_score,
                future_return_pct,
                adv_hit_cnt,
                adv_score_sum,
                pos_hit_cnt,
                pos_score_sum,
                noisy_companion_cnt
            FROM strategy_perf_sample_features
            ORDER BY signal_date ASC, ts_code ASC
            "#,
        )
        .map_err(|e| format!("预编译样本特征读取失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询样本特征失败: {e}"))?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(|e| format!("读取样本特征失败: {e}"))? {
        out.push(SampleFeatureRow {
            signal_date: row
                .get(0)
                .map_err(|e| format!("读取 signal_date 失败: {e}"))?,
            ts_code: row.get(1).map_err(|e| format!("读取 ts_code 失败: {e}"))?,
            rank: row.get(2).map_err(|e| format!("读取 rank 失败: {e}"))?,
            total_score: row
                .get(3)
                .map_err(|e| format!("读取 total_score 失败: {e}"))?,
            future_return_pct: row
                .get::<_, Option<f64>>(4)
                .map_err(|e| format!("读取 future_return_pct 失败: {e}"))?
                .unwrap_or(0.0),
            adv_hit_cnt: row
                .get::<_, i64>(5)
                .map_err(|e| format!("读取 adv_hit_cnt 失败: {e}"))?
                .max(0) as u32,
            adv_score_sum: row
                .get::<_, Option<f64>>(6)
                .map_err(|e| format!("读取 adv_score_sum 失败: {e}"))?
                .unwrap_or(0.0),
            pos_hit_cnt: row
                .get::<_, i64>(7)
                .map_err(|e| format!("读取 pos_hit_cnt 失败: {e}"))?
                .max(0) as u32,
            pos_score_sum: row
                .get::<_, Option<f64>>(8)
                .map_err(|e| format!("读取 pos_score_sum 失败: {e}"))?
                .unwrap_or(0.0),
            noisy_companion_cnt: row
                .get::<_, i64>(9)
                .map_err(|e| format!("读取 noisy_companion_cnt 失败: {e}"))?
                .max(0) as u32,
        });
    }
    Ok(out)
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

fn compare_sort_key(left: &SampleFeatureRow, right: &SampleFeatureRow, key: SortKey) -> Ordering {
    match key {
        SortKey::AdvHitCnt => right.adv_hit_cnt.cmp(&left.adv_hit_cnt),
        SortKey::AdvScoreSum => {
            compare_option_f64_desc(Some(left.adv_score_sum), Some(right.adv_score_sum))
        }
        SortKey::PosHitCnt => right.pos_hit_cnt.cmp(&left.pos_hit_cnt),
        SortKey::PosScoreSum => {
            compare_option_f64_desc(Some(left.pos_score_sum), Some(right.pos_score_sum))
        }
        SortKey::TotalScore => compare_option_f64_desc(left.total_score, right.total_score),
        SortKey::Rank => compare_option_i64_asc(left.rank, right.rank),
    }
}

fn sort_rows_with_keys(rows: &mut [SampleFeatureRow], keys: &[SortKey]) {
    rows.sort_by(|left, right| {
        for key in keys {
            let ord = compare_sort_key(left, right, *key);
            if ord != Ordering::Equal {
                return ord;
            }
        }
        compare_option_i64_asc(left.rank, right.rank)
            .then_with(|| compare_option_f64_desc(left.total_score, right.total_score))
            .then_with(|| left.ts_code.cmp(&right.ts_code))
    });
}

fn build_market_daily_returns(rows: &[SampleFeatureRow]) -> BTreeMap<String, f64> {
    let mut grouped: BTreeMap<String, (f64, u32)> = BTreeMap::new();
    for row in rows {
        let entry = grouped.entry(row.signal_date.clone()).or_insert((0.0, 0));
        entry.0 += row.future_return_pct;
        entry.1 += 1;
    }
    grouped
        .into_iter()
        .filter_map(|(date, (sum, count))| {
            if count == 0 {
                None
            } else {
                Some((date, sum / count as f64))
            }
        })
        .collect()
}

fn summarize_daily_points(
    key: &str,
    label: &str,
    sort_description: &str,
    daily_points: &[DailyPortfolioPoint],
) -> StrategyPerformancePortfolioRow {
    let mut windows = Vec::new();
    let full = build_portfolio_window("full", "全样本", daily_points);
    windows.push(full);
    for recent in RECENT_WINDOWS {
        let subset_start = daily_points.len().saturating_sub(recent);
        let subset = &daily_points[subset_start..];
        windows.push(build_portfolio_window(
            &format!("recent_{recent}"),
            &format!("近{recent}期"),
            subset,
        ));
    }
    StrategyPerformancePortfolioRow {
        strategy_key: key.to_string(),
        strategy_label: label.to_string(),
        sort_description: sort_description.to_string(),
        windows,
    }
}

fn build_portfolio_window(
    window_key: &str,
    label: &str,
    daily_points: &[DailyPortfolioPoint],
) -> StrategyPerformancePortfolioWindow {
    if daily_points.is_empty() {
        return StrategyPerformancePortfolioWindow {
            window_key: window_key.to_string(),
            label: label.to_string(),
            sample_days: 0,
            avg_portfolio_return_pct: None,
            avg_market_return_pct: None,
            avg_excess_return_pct: None,
            excess_win_rate: None,
            avg_selected_count: None,
        };
    }

    let sample_days = daily_points.len() as u32;
    let avg_portfolio_return_pct = Some(
        daily_points
            .iter()
            .map(|item| item.portfolio_return_pct)
            .sum::<f64>()
            / sample_days as f64,
    );
    let avg_market_return_pct = Some(
        daily_points
            .iter()
            .map(|item| item.market_return_pct)
            .sum::<f64>()
            / sample_days as f64,
    );
    let avg_excess_return_pct = match (avg_portfolio_return_pct, avg_market_return_pct) {
        (Some(portfolio), Some(market)) => Some(portfolio - market),
        _ => None,
    };
    let excess_win_rate = Some(
        daily_points
            .iter()
            .filter(|item| item.portfolio_return_pct > item.market_return_pct)
            .count() as f64
            / sample_days as f64,
    );
    let avg_selected_count = Some(
        daily_points
            .iter()
            .map(|item| item.selected_count as f64)
            .sum::<f64>()
            / sample_days as f64,
    );

    let _ = daily_points
        .last()
        .map(|point| point.signal_date.as_str())
        .unwrap_or_default();

    StrategyPerformancePortfolioWindow {
        window_key: window_key.to_string(),
        label: label.to_string(),
        sample_days,
        avg_portfolio_return_pct,
        avg_market_return_pct,
        avg_excess_return_pct,
        excess_win_rate,
        avg_selected_count,
    }
}

fn build_portfolio_rows(
    sample_rows: &[SampleFeatureRow],
    min_adv_hits: u32,
    top_limit: u32,
    mixed_sort_keys: &[SortKey],
    noisy_rule_names: &[String],
) -> Vec<StrategyPerformancePortfolioRow> {
    let market_daily = build_market_daily_returns(sample_rows);
    let mut grouped_by_date: BTreeMap<String, Vec<SampleFeatureRow>> = BTreeMap::new();
    for row in sample_rows {
        grouped_by_date
            .entry(row.signal_date.clone())
            .or_default()
            .push(row.clone());
    }

    let mut raw_points = Vec::new();
    let mut only_adv_points = Vec::new();
    let mut adv_hit_points = Vec::new();
    let mut adv_score_points = Vec::new();
    let mut mixed_points = Vec::new();
    let mut companion_penalty_points = Vec::new();

    for (signal_date, rows) in grouped_by_date {
        let Some(market_return_pct) = market_daily.get(&signal_date).copied() else {
            continue;
        };

        let mut raw_rows = rows.clone();
        raw_rows.sort_by(|left, right| {
            compare_option_i64_asc(left.rank, right.rank)
                .then_with(|| compare_option_f64_desc(left.total_score, right.total_score))
                .then_with(|| left.ts_code.cmp(&right.ts_code))
        });
        let raw_selected = raw_rows
            .into_iter()
            .take(top_limit as usize)
            .collect::<Vec<_>>();
        if !raw_selected.is_empty() {
            raw_points.push(DailyPortfolioPoint {
                signal_date: signal_date.clone(),
                portfolio_return_pct: raw_selected
                    .iter()
                    .map(|row| row.future_return_pct)
                    .sum::<f64>()
                    / raw_selected.len() as f64,
                market_return_pct,
                selected_count: raw_selected.len(),
            });
        }

        let eligible_rows = rows
            .iter()
            .filter(|row| row.adv_hit_cnt >= min_adv_hits)
            .cloned()
            .collect::<Vec<_>>();

        if !eligible_rows.is_empty() {
            only_adv_points.push(DailyPortfolioPoint {
                signal_date: signal_date.clone(),
                portfolio_return_pct: eligible_rows
                    .iter()
                    .map(|row| row.future_return_pct)
                    .sum::<f64>()
                    / eligible_rows.len() as f64,
                market_return_pct,
                selected_count: eligible_rows.len(),
            });
        }

        if !eligible_rows.is_empty() {
            let mut adv_hit_rows = eligible_rows.clone();
            adv_hit_rows.sort_by(|left, right| {
                right
                    .adv_hit_cnt
                    .cmp(&left.adv_hit_cnt)
                    .then_with(|| compare_option_i64_asc(left.rank, right.rank))
                    .then_with(|| compare_option_f64_desc(left.total_score, right.total_score))
                    .then_with(|| left.ts_code.cmp(&right.ts_code))
            });
            let adv_hit_selected = adv_hit_rows
                .into_iter()
                .take(top_limit as usize)
                .collect::<Vec<_>>();
            if !adv_hit_selected.is_empty() {
                adv_hit_points.push(DailyPortfolioPoint {
                    signal_date: signal_date.clone(),
                    portfolio_return_pct: adv_hit_selected
                        .iter()
                        .map(|row| row.future_return_pct)
                        .sum::<f64>()
                        / adv_hit_selected.len() as f64,
                    market_return_pct,
                    selected_count: adv_hit_selected.len(),
                });
            }

            let mut adv_score_rows = eligible_rows.clone();
            adv_score_rows.sort_by(|left, right| {
                compare_option_f64_desc(Some(left.adv_score_sum), Some(right.adv_score_sum))
                    .then_with(|| compare_option_i64_asc(left.rank, right.rank))
                    .then_with(|| compare_option_f64_desc(left.total_score, right.total_score))
                    .then_with(|| left.ts_code.cmp(&right.ts_code))
            });
            let adv_score_selected = adv_score_rows
                .into_iter()
                .take(top_limit as usize)
                .collect::<Vec<_>>();
            if !adv_score_selected.is_empty() {
                adv_score_points.push(DailyPortfolioPoint {
                    signal_date: signal_date.clone(),
                    portfolio_return_pct: adv_score_selected
                        .iter()
                        .map(|row| row.future_return_pct)
                        .sum::<f64>()
                        / adv_score_selected.len() as f64,
                    market_return_pct,
                    selected_count: adv_score_selected.len(),
                });
            }

            let mut mixed_rows = eligible_rows.clone();
            sort_rows_with_keys(&mut mixed_rows, mixed_sort_keys);
            let mixed_selected = mixed_rows
                .into_iter()
                .take(top_limit as usize)
                .collect::<Vec<_>>();
            if !mixed_selected.is_empty() {
                mixed_points.push(DailyPortfolioPoint {
                    signal_date: signal_date.clone(),
                    portfolio_return_pct: mixed_selected
                        .iter()
                        .map(|row| row.future_return_pct)
                        .sum::<f64>()
                        / mixed_selected.len() as f64,
                    market_return_pct,
                    selected_count: mixed_selected.len(),
                });
            }

            if !noisy_rule_names.is_empty() {
                let mut penalty_rows = eligible_rows.clone();
                penalty_rows.sort_by(|left, right| {
                    right
                        .adv_hit_cnt
                        .cmp(&left.adv_hit_cnt)
                        .then_with(|| left.noisy_companion_cnt.cmp(&right.noisy_companion_cnt))
                        .then_with(|| {
                            compare_option_f64_desc(
                                Some(left.adv_score_sum),
                                Some(right.adv_score_sum),
                            )
                        })
                        .then_with(|| compare_option_i64_asc(left.rank, right.rank))
                        .then_with(|| compare_option_f64_desc(left.total_score, right.total_score))
                        .then_with(|| left.ts_code.cmp(&right.ts_code))
                });
                let penalty_selected = penalty_rows
                    .into_iter()
                    .take(top_limit as usize)
                    .collect::<Vec<_>>();
                if !penalty_selected.is_empty() {
                    companion_penalty_points.push(DailyPortfolioPoint {
                        signal_date,
                        portfolio_return_pct: penalty_selected
                            .iter()
                            .map(|row| row.future_return_pct)
                            .sum::<f64>()
                            / penalty_selected.len() as f64,
                        market_return_pct,
                        selected_count: penalty_selected.len(),
                    });
                }
            }
        }
    }

    let mut out = vec![
        summarize_daily_points(
            "raw_topn",
            "原始 TopN",
            "直接按原始 rank 取前 N",
            &raw_points,
        ),
        summarize_daily_points(
            "only_adv_pool",
            "仅优势池",
            "仅保留优势命中数 >= min_adv_hits 的样本，池内等权",
            &only_adv_points,
        ),
        summarize_daily_points(
            "adv_hit_topn",
            "优势命中 TopN",
            "在优势池内优先按优势命中数降序，再按 rank 升序",
            &adv_hit_points,
        ),
        summarize_daily_points(
            "adv_score_topn",
            "优势得分 TopN",
            "在优势池内优先按优势得分和降序，再按 rank 升序",
            &adv_score_points,
        ),
        summarize_daily_points(
            "mixed_topn",
            "混合排序 TopN",
            &format!(
                "在优势池内按 {} 做字典序排序",
                mixed_sort_keys
                    .iter()
                    .map(|key| sort_key_label(*key))
                    .collect::<Vec<_>>()
                    .join(" > ")
            ),
            &mixed_points,
        ),
    ];

    if !noisy_rule_names.is_empty() {
        out.push(summarize_daily_points(
            "companion_penalty_topn",
            "噪音惩罚 TopN",
            "在优势池内优先优势命中数，噪音伴随数量更少者优先",
            &companion_penalty_points,
        ));
    }

    out
}

fn resolve_selected_rule_name(
    requested: Option<String>,
    resolved_advantage_rule_names: &[String],
    auto_candidate_rule_names: &[String],
    strategy_options: &[String],
) -> Option<String> {
    let requested = requested
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if let Some(name) = requested {
        if strategy_options.iter().any(|item| item == &name) {
            return Some(name);
        }
    }
    resolved_advantage_rule_names
        .first()
        .cloned()
        .or_else(|| auto_candidate_rule_names.first().cloned())
        .or_else(|| strategy_options.first().cloned())
}

fn rank_values(values: &[f64]) -> Vec<f64> {
    let mut indexed = values
        .iter()
        .copied()
        .enumerate()
        .collect::<Vec<(usize, f64)>>();
    indexed.sort_by(|left, right| left.1.partial_cmp(&right.1).unwrap_or(Ordering::Equal));

    let mut ranks = vec![0.0; values.len()];
    let mut start = 0usize;
    while start < indexed.len() {
        let mut end = start + 1;
        while end < indexed.len()
            && indexed[end]
                .1
                .partial_cmp(&indexed[start].1)
                .unwrap_or(Ordering::Equal)
                == Ordering::Equal
        {
            end += 1;
        }
        let avg_rank = ((start + 1 + end) as f64) / 2.0;
        for item in &indexed[start..end] {
            ranks[item.0] = avg_rank;
        }
        start = end;
    }
    ranks
}

fn pearson_corr(xs: &[f64], ys: &[f64]) -> Option<f64> {
    if xs.len() != ys.len() || xs.len() < 2 {
        return None;
    }
    let mean_x = xs.iter().sum::<f64>() / xs.len() as f64;
    let mean_y = ys.iter().sum::<f64>() / ys.len() as f64;
    let mut numerator = 0.0;
    let mut denom_x = 0.0;
    let mut denom_y = 0.0;
    for (x, y) in xs.iter().zip(ys.iter()) {
        let dx = *x - mean_x;
        let dy = *y - mean_y;
        numerator += dx * dy;
        denom_x += dx * dx;
        denom_y += dy * dy;
    }
    if denom_x <= 0.0 || denom_y <= 0.0 {
        return None;
    }
    Some(numerator / (denom_x.sqrt() * denom_y.sqrt()))
}

fn spearman_corr(samples: &[ScoreObservation], use_abs_score: bool) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let scores = samples
        .iter()
        .map(|sample| {
            if use_abs_score {
                sample.score.abs()
            } else {
                sample.score
            }
        })
        .collect::<Vec<_>>();
    let returns = samples
        .iter()
        .map(|sample| sample.future_return_pct)
        .collect::<Vec<_>>();
    let score_ranks = rank_values(&scores);
    let return_ranks = rank_values(&returns);
    pearson_corr(&score_ranks, &return_ranks)
}

fn build_score_bucket_rows(
    samples: &[ScoreObservation],
    strong_threshold_pct: Option<f64>,
) -> (String, Vec<StrategyPerformanceScoreBucketRow>) {
    if samples.is_empty() {
        return ("none".to_string(), Vec::new());
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(|left, right| {
        left.score
            .partial_cmp(&right.score)
            .unwrap_or(Ordering::Equal)
    });

    let distinct_count = {
        let mut count = 0usize;
        let mut previous = None::<f64>;
        for sample in &sorted {
            if previous
                .map(|value| {
                    value.partial_cmp(&sample.score).unwrap_or(Ordering::Equal) != Ordering::Equal
                })
                .unwrap_or(true)
            {
                count += 1;
                previous = Some(sample.score);
            }
        }
        count
    };

    if distinct_count <= SCORE_BUCKET_LIMIT {
        let mut groups = Vec::<Vec<ScoreObservation>>::new();
        for sample in sorted {
            if groups
                .last()
                .and_then(|group| group.first())
                .map(|first| {
                    first
                        .score
                        .partial_cmp(&sample.score)
                        .unwrap_or(Ordering::Equal)
                        == Ordering::Equal
                })
                .unwrap_or(false)
            {
                groups.last_mut().expect("group exists").push(sample);
            } else {
                groups.push(vec![sample]);
            }
        }

        let rows = groups
            .into_iter()
            .map(|group| {
                let score = group.first().map(|item| item.score).unwrap_or_default();
                StrategyPerformanceScoreBucketRow {
                    bucket_label: format!("{score:.2}"),
                    score_min: Some(score),
                    score_max: Some(score),
                    sample_count: group.len() as u32,
                    avg_future_return_pct: Some(
                        group.iter().map(|item| item.future_return_pct).sum::<f64>()
                            / group.len() as f64,
                    ),
                    strong_hit_rate: strong_threshold_pct.map(|threshold| {
                        group
                            .iter()
                            .filter(|item| item.future_return_pct >= threshold)
                            .count() as f64
                            / group.len() as f64
                    }),
                    win_rate: Some(
                        group
                            .iter()
                            .filter(|item| item.future_return_pct > 0.0)
                            .count() as f64
                            / group.len() as f64,
                    ),
                }
            })
            .collect::<Vec<_>>();
        return ("score_value".to_string(), rows);
    }

    let bucket_count = SCORE_BUCKET_QUANTILES.min(sorted.len());
    let mut rows = Vec::new();
    for bucket_index in 0..bucket_count {
        let start = bucket_index * sorted.len() / bucket_count;
        let end = ((bucket_index + 1) * sorted.len() / bucket_count).max(start + 1);
        let end = end.min(sorted.len());
        let group = &sorted[start..end];
        if group.is_empty() {
            continue;
        }
        let score_min = group.first().map(|item| item.score);
        let score_max = group.last().map(|item| item.score);
        rows.push(StrategyPerformanceScoreBucketRow {
            bucket_label: match (score_min, score_max) {
                (Some(min), Some(max))
                    if min.partial_cmp(&max).unwrap_or(Ordering::Equal) == Ordering::Equal =>
                {
                    format!("{min:.2}")
                }
                (Some(min), Some(max)) => format!("{min:.2} ~ {max:.2}"),
                _ => format!("bucket_{}", bucket_index + 1),
            },
            score_min,
            score_max,
            sample_count: group.len() as u32,
            avg_future_return_pct: Some(
                group.iter().map(|item| item.future_return_pct).sum::<f64>() / group.len() as f64,
            ),
            strong_hit_rate: strong_threshold_pct.map(|threshold| {
                group
                    .iter()
                    .filter(|item| item.future_return_pct >= threshold)
                    .count() as f64
                    / group.len() as f64
            }),
            win_rate: Some(
                group
                    .iter()
                    .filter(|item| item.future_return_pct > 0.0)
                    .count() as f64
                    / group.len() as f64,
            ),
        });
    }

    ("score_bucket".to_string(), rows)
}

fn build_hit_count_rows(
    samples: &[ScoreObservation],
    base_points: f64,
    strong_threshold_pct: Option<f64>,
) -> Vec<StrategyPerformanceHitCountRow> {
    if base_points == 0.0 {
        return Vec::new();
    }
    let mut grouped: BTreeMap<u32, Vec<f64>> = BTreeMap::new();
    for sample in samples {
        let raw_count = (sample.score.abs() / base_points.abs()).round();
        if raw_count < 1.0 {
            continue;
        }
        let ratio = sample.score.abs() / base_points.abs();
        if (ratio - raw_count).abs() > 1e-6 {
            continue;
        }
        grouped
            .entry(raw_count as u32)
            .or_default()
            .push(sample.future_return_pct);
    }

    grouped
        .into_iter()
        .map(|(hit_count, returns)| StrategyPerformanceHitCountRow {
            hit_count,
            sample_count: returns.len() as u32,
            avg_future_return_pct: Some(returns.iter().sum::<f64>() / returns.len() as f64),
            strong_hit_rate: strong_threshold_pct.map(|threshold| {
                returns.iter().filter(|value| **value >= threshold).count() as f64
                    / returns.len() as f64
            }),
            win_rate: Some(
                returns.iter().filter(|value| **value > 0.0).count() as f64 / returns.len() as f64,
            ),
        })
        .collect()
}

fn load_rule_detail_observations(
    source_conn: &Connection,
    detail_table_name: &str,
    selected_horizon: u32,
    rule_name: &str,
) -> Result<HashMap<bool, Vec<ScoreObservation>>, String> {
    let mut stmt = source_conn
        .prepare(&format!(
            r#"
            SELECT
                CASE WHEN d.rule_score > 0 THEN TRUE ELSE FALSE END AS is_positive,
                d.rule_score,
                r.future_return_pct
            FROM strategy_perf_sample_returns AS r
            INNER JOIN {detail_table_name} AS d
                ON d.ts_code = r.ts_code
               AND d.trade_date = r.signal_date
            WHERE r.horizon = ?
              AND d.rule_name = ?
              AND d.rule_score != 0
            ORDER BY d.rule_score ASC, r.future_return_pct ASC
            "#,
        ))
        .map_err(|e| format!("预编译单策略明细失败: {e}"))?;
    let mut rows = stmt
        .query(params![selected_horizon as i64, rule_name])
        .map_err(|e| format!("查询单策略明细失败: {e}"))?;
    let mut out: HashMap<bool, Vec<ScoreObservation>> = HashMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取单策略明细失败: {e}"))?
    {
        out.entry(row.get(0).map_err(|e| format!("读取方向失败: {e}"))?)
            .or_default()
            .push(ScoreObservation {
                score: row
                    .get::<_, Option<f64>>(1)
                    .map_err(|e| format!("读取 rule_score 失败: {e}"))?
                    .unwrap_or(0.0),
                future_return_pct: row
                    .get::<_, Option<f64>>(2)
                    .map_err(|e| format!("读取 future_return_pct 失败: {e}"))?
                    .unwrap_or(0.0),
            });
    }
    Ok(out)
}

fn non_hit_avg(
    overall_count: u32,
    overall_avg: Option<f64>,
    hit_count: u32,
    hit_avg: Option<f64>,
) -> Option<f64> {
    if overall_count <= hit_count {
        return None;
    }
    let overall_avg = overall_avg?;
    let hit_avg = hit_avg?;
    Some(
        (overall_avg * overall_count as f64 - hit_avg * hit_count as f64)
            / (overall_count - hit_count) as f64,
    )
}

fn build_rule_detail(
    source_conn: &Connection,
    detail_table_name: &str,
    selected_horizon: u32,
    selected_rule_name: &str,
    rule_meta: &HashMap<String, RuleMeta>,
    future_summary_map: &HashMap<u32, StrategyPerformanceFutureSummary>,
) -> Result<Option<StrategyPerformanceRuleDetail>, String> {
    let Some(meta) = rule_meta.get(selected_rule_name) else {
        return Ok(None);
    };
    let Some(future_summary) = future_summary_map.get(&selected_horizon) else {
        return Ok(None);
    };
    let grouped = load_rule_detail_observations(
        source_conn,
        detail_table_name,
        selected_horizon,
        selected_rule_name,
    )?;
    let mut directions = Vec::new();

    for is_positive in [true, false] {
        let Some(samples) = grouped.get(&is_positive) else {
            continue;
        };
        if samples.is_empty() {
            continue;
        }
        let avg_future_return_pct = Some(
            samples
                .iter()
                .map(|item| item.future_return_pct)
                .sum::<f64>()
                / samples.len() as f64,
        );
        let strong_hit_rate = future_summary.strong_threshold_pct.map(|threshold| {
            samples
                .iter()
                .filter(|item| item.future_return_pct >= threshold)
                .count() as f64
                / samples.len() as f64
        });
        let win_rate = Some(
            samples
                .iter()
                .filter(|item| item.future_return_pct > 0.0)
                .count() as f64
                / samples.len() as f64,
        );
        let non_hit_return_pct = non_hit_avg(
            future_summary.sample_count,
            future_summary.avg_future_return_pct,
            samples.len() as u32,
            avg_future_return_pct,
        );
        let bucket_mode_and_rows =
            build_score_bucket_rows(samples, future_summary.strong_threshold_pct);
        let bucket_mode = bucket_mode_and_rows.0;
        let score_rows = bucket_mode_and_rows.1;
        let extreme_score_minus_mild_score_pct = match (score_rows.first(), score_rows.last()) {
            (Some(first), Some(last)) if score_rows.len() >= 2 => {
                if is_positive {
                    match (last.avg_future_return_pct, first.avg_future_return_pct) {
                        (Some(extreme), Some(mild)) => Some(extreme - mild),
                        _ => None,
                    }
                } else {
                    match (first.avg_future_return_pct, last.avg_future_return_pct) {
                        (Some(extreme), Some(mild)) => Some(extreme - mild),
                        _ => None,
                    }
                }
            }
            _ => None,
        };
        let hit_count_rows = if meta.scope_way_label == "EACH" && !meta.has_dist_points {
            build_hit_count_rows(samples, meta.points, future_summary.strong_threshold_pct)
        } else {
            Vec::new()
        };
        directions.push(StrategyPerformanceRuleDirectionDetail {
            signal_direction: if is_positive {
                "positive".to_string()
            } else {
                "negative".to_string()
            },
            direction_label: if is_positive {
                "正向命中".to_string()
            } else {
                "负向命中".to_string()
            },
            bucket_mode,
            sample_count: samples.len() as u32,
            avg_future_return_pct,
            strong_hit_rate,
            win_rate,
            spearman_corr: spearman_corr(samples, false),
            abs_spearman_corr: spearman_corr(samples, true),
            hit_vs_non_hit_delta_pct: match (avg_future_return_pct, non_hit_return_pct) {
                (Some(hit_avg), Some(non_hit_avg)) => Some(hit_avg - non_hit_avg),
                _ => None,
            },
            extreme_score_minus_mild_score_pct,
            has_dist_points: meta.has_dist_points,
            score_rows,
            hit_count_rows,
        });
    }

    Ok(Some(StrategyPerformanceRuleDetail {
        rule_name: selected_rule_name.to_string(),
        horizon: selected_horizon,
        explain: Some(meta.explain.clone()),
        tag: rule_tag_label(meta.tag),
        scope_way: Some(meta.scope_way_label.clone()),
        scope_windows: Some(meta.scope_windows),
        points: Some(meta.points),
        has_dist_points: meta.has_dist_points,
        directions,
    }))
}

fn build_method_notes() -> Vec<StrategyPerformanceMethodNote> {
    vec![
        StrategyPerformanceMethodNote {
            key: "future_return".to_string(),
            title: "未来收益口径".to_string(),
            description: "信号日=score_summary.trade_date；次日开盘买入；第N个后续交易日收盘卖出；future_return=(exit_close/entry_open-1)*100%。仅统计 entry_open 与 exit_close 都存在的 qfq 样本。".to_string(),
        },
        StrategyPerformanceMethodNote {
            key: "strong_sample".to_string(),
            title: "未来强势股定义".to_string(),
            description: "对每个持有周期单独统计全部有效样本 future_return 分布，输出 p80/p90/p95/max。强势样本阈值使用 quantile_cont(future_return, strong_quantile)，默认 strong_quantile=0.90。".to_string(),
        },
        StrategyPerformanceMethodNote {
            key: "advantage_rule".to_string(),
            title: "优势策略筛选".to_string(),
            description: "对每条规则分别统计正向命中(rule_score>0)与负向命中(rule_score<0)。自动优势策略默认先要求：命中样本不太少、未来平均收益高于市场、在赢家中更常见；再按当前持有周期下的 strong_lift、avg_future_return、hit_n 综合排序，只取前10条作为自动优势策略。伴随集定义为当前样本期内有正向命中、但不在优势集中的其他规则。".to_string(),
        },
        StrategyPerformanceMethodNote {
            key: "negative_rule".to_string(),
            title: "负向方向判定".to_string(),
            description: "负向规则先看 rule_score<0 命中样本是否足够，再按持有周期分别验证四件事：avg_future_return 低于市场、strong_lift < 1、win_rate 低于市场、hit_vs_non_hit = hit组均收益 - non_hit组均收益 < 0。若当前持有周期成立，且满足条件的周期数达到“至少通过几个持有周期”，则归入“方向明确负向”；否则归入“待验证负向”，并显示未通过的原因。".to_string(),
        },
        StrategyPerformanceMethodNote {
            key: "companion".to_string(),
            title: "伴随策略分析".to_string(),
            description: "先定义优势策略集 advantage_rules，再在 adv_hit_cnt >= min_adv_hits 的优势样本池内统计其他正向伴随策略。delta_return = companion_avg_return - eligible_pool_avg_return；delta_win = companion_win_rate - eligible_pool_win_rate。".to_string(),
        },
        StrategyPerformanceMethodNote {
            key: "score_strength".to_string(),
            title: "得分强度分析".to_string(),
            description: "单策略明细按 rule_score 精确值或分位桶统计 sample_count / avg_future_return / strong_hit_rate / win_rate，并给出 corr(rule_score, future_return) 与 corr(abs(rule_score), future_return) 的 Spearman 相关。对 EACH 且非 dist_points 规则，额外估算 hit_count = |rule_score| / |points| 的命中次数分层。".to_string(),
        },
        StrategyPerformanceMethodNote {
            key: "portfolio".to_string(),
            title: "组合回测".to_string(),
            description: "对每个信号日横截面构造组合并做日度等权收益，再统计全样本、近40期、近20期的平均组合收益、市场收益、超额收益和超额胜率。原始 TopN 为基准，其余组合只在 adv_hit_cnt >= min_adv_hits 的优势样本池内排序。".to_string(),
        },
    ]
}

pub fn get_strategy_performance_rule_detail(
    source_path: String,
    selected_horizon: Option<u32>,
    strong_quantile: Option<f64>,
    selected_rule_name: String,
) -> Result<Option<StrategyPerformanceRuleDetail>, String> {
    let rule_name = selected_rule_name.trim();
    if rule_name.is_empty() {
        return Ok(None);
    }

    let selected_horizon = normalize_selected_horizon(selected_horizon);
    let strong_quantile = normalize_strong_quantile(strong_quantile)?;
    let (_, rule_meta) = load_rule_meta(&source_path)?;
    let source_conn = open_source_conn(&source_path)?;
    attach_result_db(&source_conn, &source_path)?;
    prepare_temp_exit_map(&source_conn, &source_path)?;
    prepare_temp_sample_returns(&source_conn)?;
    prepare_temp_thresholds(&source_conn, strong_quantile)?;
    let future_summaries = load_future_summaries(&source_conn, strong_quantile)?;
    let future_summary_map = future_summaries
        .into_iter()
        .map(|summary| (summary.horizon, summary))
        .collect::<HashMap<_, _>>();

    build_rule_detail(
        &source_conn,
        RESULT_DETAILS_TABLE,
        selected_horizon,
        rule_name,
        &rule_meta,
        &future_summary_map,
    )
}

pub fn get_strategy_performance_validation_page(
    source_path: String,
    selected_horizon: Option<u32>,
    strong_quantile: Option<f64>,
    draft: StrategyManageRuleDraft,
) -> Result<StrategyPerformanceValidationPageData, String> {
    let selected_horizon = normalize_selected_horizon(selected_horizon);
    let strong_quantile = normalize_strong_quantile(strong_quantile)?;
    let auto_filter = StrategyPerformanceAutoFilterConfig {
        min_samples_2: DEFAULT_MIN_SAMPLE,
        min_samples_3: DEFAULT_MIN_SAMPLE,
        min_samples_5: DEFAULT_MIN_SAMPLE,
        min_samples_10: DEFAULT_MIN_SAMPLE,
        require_win_rate_above_market: false,
        min_pass_horizons: DEFAULT_MIN_PASS_HORIZONS,
    };

    let source_conn = open_source_conn(&source_path)?;
    attach_result_db(&source_conn, &source_path)?;
    prepare_temp_exit_map(&source_conn, &source_path)?;
    prepare_temp_sample_returns(&source_conn)?;
    prepare_temp_thresholds(&source_conn, strong_quantile)?;
    let future_summaries = load_future_summaries(&source_conn, strong_quantile)?;
    if future_summaries
        .iter()
        .all(|summary| summary.sample_count == 0)
    {
        return Err("没有可用未来收益样本".to_string());
    }
    let future_summary_map = future_summaries
        .iter()
        .cloned()
        .map(|summary| (summary.horizon, summary))
        .collect::<HashMap<_, _>>();

    let (rule_name, rule_meta) =
        prepare_temp_validation_rule_details(&source_conn, &source_path, &draft)?;
    let rule_aggregates = load_rule_aggregates(&source_conn, VALIDATION_DETAILS_TABLE)?;
    let strategy_options = vec![rule_name.clone()];
    let rule_rows = build_rule_rows(
        &strategy_options,
        &rule_meta,
        &rule_aggregates,
        &future_summary_map,
        &auto_filter,
        selected_horizon,
        &Vec::new(),
        &HashSet::new(),
    )
    .0;
    let rule_detail = build_rule_detail(
        &source_conn,
        VALIDATION_DETAILS_TABLE,
        selected_horizon,
        &rule_name,
        &rule_meta,
        &future_summary_map,
    )?;
    let meta = rule_meta
        .get(&rule_name)
        .ok_or_else(|| "草稿策略元信息缺失".to_string())?;

    Ok(StrategyPerformanceValidationPageData {
        horizons: HORIZONS.to_vec(),
        selected_horizon,
        strong_quantile,
        future_summaries,
        auto_filter,
        draft_summary: StrategyPerformanceValidationDraftSummary {
            name: rule_name,
            explain: meta.explain.clone(),
            tag: rule_tag_label(meta.tag),
            scope_way: meta.scope_way_label.clone(),
            scope_windows: meta.scope_windows,
            points: meta.points,
            has_dist_points: meta.has_dist_points,
            score_mode: if meta.has_dist_points {
                "dist".to_string()
            } else {
                "fixed".to_string()
            },
        },
        rule_rows,
        rule_detail,
        methods: build_method_notes(),
    })
}

pub fn get_strategy_performance_page(
    source_path: String,
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
    selected_rule_name: Option<String>,
) -> Result<StrategyPerformancePageData, String> {
    let selected_horizon = normalize_selected_horizon(selected_horizon);
    let strong_quantile = normalize_strong_quantile(strong_quantile)?;
    let top_limit = top_limit.unwrap_or(DEFAULT_TOP_LIMIT).max(1);
    let min_adv_hits = min_adv_hits.unwrap_or(DEFAULT_MIN_ADV_HITS).max(1);
    let auto_filter = StrategyPerformanceAutoFilterConfig {
        min_samples_2: auto_min_samples_2.unwrap_or(DEFAULT_MIN_SAMPLE).max(1),
        min_samples_3: auto_min_samples_3.unwrap_or(DEFAULT_MIN_SAMPLE).max(1),
        min_samples_5: auto_min_samples_5.unwrap_or(DEFAULT_MIN_SAMPLE).max(1),
        min_samples_10: auto_min_samples_10.unwrap_or(DEFAULT_MIN_SAMPLE).max(1),
        require_win_rate_above_market: require_win_rate_above_market.unwrap_or(false),
        min_pass_horizons: min_pass_horizons
            .unwrap_or(DEFAULT_MIN_PASS_HORIZONS)
            .clamp(1, HORIZONS.len() as u32),
    };

    let (strategy_options, rule_meta) = load_rule_meta(&source_path)?;
    let (manual_rule_names, ignored_manual_rule_names) =
        normalize_manual_rule_names(manual_rule_names, &strategy_options);
    let requested_noisy_companion_rule_names =
        normalize_noisy_rule_names(noisy_companion_rule_names, &strategy_options);
    let mixed_sort_keys = normalize_sort_keys(mixed_sort_keys);
    let advantage_rule_mode = normalize_advantage_mode(advantage_rule_mode);

    let source_conn = open_source_conn(&source_path)?;
    attach_result_db(&source_conn, &source_path)?;
    prepare_temp_exit_map(&source_conn, &source_path)?;
    prepare_temp_sample_returns(&source_conn)?;
    prepare_temp_thresholds(&source_conn, strong_quantile)?;

    let future_summaries = load_future_summaries(&source_conn, strong_quantile)?;
    if future_summaries
        .iter()
        .all(|summary| summary.sample_count == 0)
    {
        return Err("没有可用未来收益样本".to_string());
    }
    let future_summary_map = future_summaries
        .iter()
        .cloned()
        .map(|summary| (summary.horizon, summary))
        .collect::<HashMap<_, _>>();

    let rule_aggregates = load_rule_aggregates(&source_conn, RESULT_DETAILS_TABLE)?;
    let resolved_advantage_rules = resolve_advantage_rule_names(
        &strategy_options,
        advantage_rule_mode,
        &Vec::new(),
        &manual_rule_names,
    );
    let resolved_advantage_rule_set = resolved_advantage_rules
        .iter()
        .cloned()
        .collect::<HashSet<_>>();

    let (mut rule_rows, auto_candidate_rule_names_initial) = build_rule_rows(
        &strategy_options,
        &rule_meta,
        &rule_aggregates,
        &future_summary_map,
        &auto_filter,
        selected_horizon,
        &manual_rule_names,
        &resolved_advantage_rule_set,
    );

    let resolved_advantage_rule_names = resolve_advantage_rule_names(
        &strategy_options,
        advantage_rule_mode,
        &auto_candidate_rule_names_initial,
        &manual_rule_names,
    );
    let resolved_advantage_rule_set = resolved_advantage_rule_names
        .iter()
        .cloned()
        .collect::<HashSet<_>>();

    for row in &mut rule_rows {
        row.auto_candidate = row.signal_direction == "positive"
            && auto_candidate_rule_names_initial
                .iter()
                .any(|item| item == &row.rule_name);
        row.manually_selected = row.signal_direction == "positive"
            && manual_rule_names.iter().any(|item| item == &row.rule_name);
        row.in_advantage_set = row.signal_direction == "positive"
            && resolved_advantage_rule_set.contains(&row.rule_name);
        row.in_companion_set = row.signal_direction == "positive"
            && !row.in_advantage_set
            && row.metrics.iter().any(|metric| metric.hit_n > 0);
    }

    let resolved_companion_rule_names = build_resolved_companion_rule_names(&rule_rows);
    let effective_negative_rule_names = build_negative_rule_names(&rule_rows, true);
    let ineffective_negative_rule_names = build_negative_rule_names(&rule_rows, false);

    prepare_temp_string_table(
        &source_conn,
        "strategy_perf_advantage_rules",
        &resolved_advantage_rule_names,
    )?;
    prepare_temp_string_table(&source_conn, "strategy_perf_noisy_rules", &Vec::new())?;
    rebuild_temp_sample_features(&source_conn, selected_horizon)?;

    let companion_rows = load_companion_rows(
        &source_conn,
        min_adv_hits,
        min_sample_for_horizon(&auto_filter, selected_horizon),
    )?;
    let noisy_companion_rule_names = if requested_noisy_companion_rule_names.is_empty() {
        companion_rows
            .iter()
            .filter(|row| row.delta_return_pct.unwrap_or(0.0) < 0.0)
            .map(|row| row.rule_name.clone())
            .collect::<Vec<_>>()
    } else {
        requested_noisy_companion_rule_names
    };
    prepare_temp_string_table(
        &source_conn,
        "strategy_perf_noisy_rules",
        &noisy_companion_rule_names,
    )?;
    rebuild_temp_sample_features(&source_conn, selected_horizon)?;
    let sample_feature_rows = load_sample_feature_rows(&source_conn)?;
    let portfolio_rows = build_portfolio_rows(
        &sample_feature_rows,
        min_adv_hits,
        top_limit,
        &mixed_sort_keys,
        &noisy_companion_rule_names,
    );

    let selected_rule_name = resolve_selected_rule_name(
        selected_rule_name,
        &resolved_advantage_rule_names,
        &auto_candidate_rule_names_initial,
        &strategy_options,
    );
    let rule_detail = if let Some(rule_name) = selected_rule_name.as_deref() {
        build_rule_detail(
            &source_conn,
            RESULT_DETAILS_TABLE,
            selected_horizon,
            rule_name,
            &rule_meta,
            &future_summary_map,
        )?
    } else {
        None
    };

    Ok(StrategyPerformancePageData {
        horizons: HORIZONS.to_vec(),
        selected_horizon,
        strong_quantile,
        strategy_options,
        future_summaries,
        auto_filter,
        resolved_advantage_mode: advantage_mode_label(advantage_rule_mode),
        auto_candidate_rule_names: auto_candidate_rule_names_initial,
        manual_rule_names,
        ignored_manual_rule_names,
        resolved_advantage_rule_names,
        resolved_companion_rule_names,
        effective_negative_rule_names,
        ineffective_negative_rule_names,
        min_adv_hits,
        top_limit,
        mixed_sort_keys: mixed_sort_keys
            .iter()
            .map(|key| sort_key_label(*key).to_string())
            .collect(),
        noisy_companion_rule_names,
        rule_rows,
        companion_rows,
        portfolio_rows,
        selected_rule_name,
        rule_detail,
        methods: build_method_notes(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    };

    use duckdb::Connection;

    use crate::data::{result_db_path, scoring_data::init_result_db, source_db_path};

    fn unique_temp_dir() -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock drift")
            .as_nanos();
        std::env::temp_dir().join(format!("lianghua_rs_strategy_performance_{stamp}"))
    }

    fn write_fixture_files(source_dir: &Path) {
        fs::create_dir_all(source_dir).expect("create source dir");
        fs::write(
            source_dir.join("trade_calendar.csv"),
            "cal_date\n20240101\n20240102\n20240103\n20240104\n20240105\n20240108\n20240109\n20240110\n20240111\n20240112\n20240115\n20240116\n20240117\n20240118\n20240119\n20240122\n20240123\n",
        )
        .expect("write trade_calendar");
        fs::write(
            source_dir.join("score_rule.toml"),
            concat!(
                "version = 1\n\n",
                "[[rule]]\n",
                "name = \"ADV\"\n",
                "scope_windows = 1\n",
                "scope_way = \"LAST\"\n",
                "when = \"C > O\"\n",
                "points = 2.0\n",
                "explain = \"adv rule\"\n\n",
                "[[rule]]\n",
                "name = \"COMP\"\n",
                "scope_windows = 1\n",
                "scope_way = \"LAST\"\n",
                "when = \"C > O\"\n",
                "points = 1.0\n",
                "explain = \"companion rule\"\n\n",
                "[[rule]]\n",
                "name = \"NEG\"\n",
                "scope_windows = 1\n",
                "scope_way = \"LAST\"\n",
                "when = \"C < O\"\n",
                "points = -1.0\n",
                "explain = \"negative rule\"\n"
            ),
        )
        .expect("write score_rule");
        fs::write(
            source_dir.join("stock_list.csv"),
            concat!(
                "ts_code,symbol,name,area,industry,list_date,market,exchange,fullname,total_mv,circ_mv\n",
                "000001.SZ,000001,平安银行,深圳,银行,19910403,主板,SZSE,平安银行,1000000,800000\n",
                "000002.SZ,000002,万科A,深圳,地产,19910129,主板,SZSE,万科A,1000000,800000\n",
                "000003.SZ,000003,国农科技,深圳,医药,19901201,主板,SZSE,国农科技,1000000,800000\n",
            ),
        )
        .expect("write stock_list");
    }

    fn write_fixture_source_db(source_dir: &Path) {
        let db_path = source_db_path(source_dir.to_str().expect("utf8 path"));
        let conn = Connection::open(&db_path).expect("open source db");
        conn.execute(
            r#"
            CREATE TABLE stock_data (
                ts_code VARCHAR,
                trade_date VARCHAR,
                adj_type VARCHAR,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE
                ,
                vol DOUBLE,
                amount DOUBLE
            )
            "#,
            [],
        )
        .expect("create stock_data");

        let rows = [
            (
                "000001.SZ",
                "20240101",
                "qfq",
                10.0,
                10.2,
                9.8,
                10.1,
                1000.0,
                10000.0,
            ),
            (
                "000001.SZ",
                "20240102",
                "qfq",
                10.0,
                10.2,
                9.8,
                10.0,
                1000.0,
                10000.0,
            ),
            (
                "000001.SZ",
                "20240103",
                "qfq",
                10.0,
                10.3,
                9.9,
                10.2,
                1000.0,
                10000.0,
            ),
            (
                "000001.SZ",
                "20240104",
                "qfq",
                10.0,
                10.4,
                9.9,
                10.3,
                1000.0,
                10000.0,
            ),
            (
                "000001.SZ",
                "20240105",
                "qfq",
                10.0,
                10.5,
                9.9,
                10.4,
                1000.0,
                10000.0,
            ),
            (
                "000001.SZ",
                "20240108",
                "qfq",
                10.0,
                10.6,
                9.9,
                10.5,
                1000.0,
                10000.0,
            ),
            (
                "000001.SZ",
                "20240109",
                "qfq",
                10.0,
                11.3,
                9.9,
                11.2,
                1000.0,
                10000.0,
            ),
            (
                "000001.SZ",
                "20240110",
                "qfq",
                10.0,
                11.7,
                9.9,
                11.6,
                1000.0,
                10000.0,
            ),
            (
                "000001.SZ",
                "20240111",
                "qfq",
                10.0,
                12.0,
                9.9,
                11.9,
                1000.0,
                10000.0,
            ),
            (
                "000001.SZ",
                "20240112",
                "qfq",
                10.0,
                12.3,
                9.9,
                12.2,
                1000.0,
                10000.0,
            ),
            (
                "000001.SZ",
                "20240115",
                "qfq",
                10.0,
                12.7,
                9.9,
                12.6,
                1000.0,
                10000.0,
            ),
            (
                "000001.SZ",
                "20240116",
                "qfq",
                10.0,
                12.9,
                9.9,
                12.8,
                1000.0,
                10000.0,
            ),
            (
                "000001.SZ",
                "20240117",
                "qfq",
                10.0,
                13.1,
                9.9,
                13.0,
                1000.0,
                10000.0,
            ),
            (
                "000002.SZ",
                "20240101",
                "qfq",
                10.0,
                10.1,
                9.7,
                9.9,
                1000.0,
                10000.0,
            ),
            (
                "000002.SZ",
                "20240102",
                "qfq",
                10.0,
                10.1,
                9.7,
                10.0,
                1000.0,
                10000.0,
            ),
            (
                "000002.SZ",
                "20240103",
                "qfq",
                10.0,
                10.0,
                9.8,
                9.9,
                1000.0,
                10000.0,
            ),
            (
                "000002.SZ",
                "20240104",
                "qfq",
                10.0,
                9.9,
                9.7,
                9.8,
                1000.0,
                10000.0,
            ),
            (
                "000002.SZ",
                "20240105",
                "qfq",
                10.0,
                9.8,
                9.6,
                9.7,
                1000.0,
                10000.0,
            ),
            (
                "000002.SZ",
                "20240108",
                "qfq",
                10.0,
                9.7,
                9.5,
                9.6,
                1000.0,
                10000.0,
            ),
            (
                "000002.SZ",
                "20240109",
                "qfq",
                10.0,
                9.2,
                8.9,
                9.1,
                1000.0,
                10000.0,
            ),
            (
                "000002.SZ",
                "20240110",
                "qfq",
                10.0,
                9.1,
                8.8,
                9.0,
                1000.0,
                10000.0,
            ),
            (
                "000002.SZ",
                "20240111",
                "qfq",
                10.0,
                8.9,
                8.6,
                8.8,
                1000.0,
                10000.0,
            ),
            (
                "000002.SZ",
                "20240112",
                "qfq",
                10.0,
                8.7,
                8.4,
                8.6,
                1000.0,
                10000.0,
            ),
            (
                "000002.SZ",
                "20240115",
                "qfq",
                10.0,
                8.5,
                8.2,
                8.4,
                1000.0,
                10000.0,
            ),
            (
                "000002.SZ",
                "20240116",
                "qfq",
                10.0,
                8.3,
                8.0,
                8.2,
                1000.0,
                10000.0,
            ),
            (
                "000002.SZ",
                "20240117",
                "qfq",
                10.0,
                8.2,
                7.9,
                8.1,
                1000.0,
                10000.0,
            ),
            (
                "000003.SZ",
                "20240101",
                "qfq",
                10.0,
                10.1,
                9.9,
                10.0,
                1000.0,
                10000.0,
            ),
            (
                "000003.SZ",
                "20240102",
                "qfq",
                10.0,
                10.1,
                9.9,
                10.0,
                1000.0,
                10000.0,
            ),
            (
                "000003.SZ",
                "20240103",
                "qfq",
                10.0,
                10.1,
                9.9,
                10.0,
                1000.0,
                10000.0,
            ),
            (
                "000003.SZ",
                "20240104",
                "qfq",
                10.0,
                10.1,
                9.9,
                10.0,
                1000.0,
                10000.0,
            ),
            (
                "000003.SZ",
                "20240105",
                "qfq",
                10.0,
                10.1,
                9.9,
                10.0,
                1000.0,
                10000.0,
            ),
            (
                "000003.SZ",
                "20240108",
                "qfq",
                10.0,
                10.1,
                9.9,
                10.0,
                1000.0,
                10000.0,
            ),
            (
                "000003.SZ",
                "20240109",
                "qfq",
                10.0,
                10.3,
                9.9,
                10.2,
                1000.0,
                10000.0,
            ),
            (
                "000003.SZ",
                "20240110",
                "qfq",
                10.0,
                10.2,
                9.9,
                10.1,
                1000.0,
                10000.0,
            ),
            (
                "000003.SZ",
                "20240111",
                "qfq",
                10.0,
                10.1,
                9.9,
                10.0,
                1000.0,
                10000.0,
            ),
            (
                "000003.SZ",
                "20240112",
                "qfq",
                10.0,
                10.2,
                9.9,
                10.1,
                1000.0,
                10000.0,
            ),
            (
                "000003.SZ",
                "20240115",
                "qfq",
                10.0,
                10.1,
                9.9,
                10.0,
                1000.0,
                10000.0,
            ),
            (
                "000003.SZ",
                "20240116",
                "qfq",
                10.0,
                10.2,
                9.9,
                10.1,
                1000.0,
                10000.0,
            ),
            (
                "000003.SZ",
                "20240117",
                "qfq",
                10.0,
                10.1,
                9.9,
                10.0,
                1000.0,
                10000.0,
            ),
        ];
        for (ts_code, trade_date, adj_type, open, high, low, close, vol, amount) in rows {
            conn.execute(
                "INSERT INTO stock_data (ts_code, trade_date, adj_type, open, high, low, close, vol, amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![ts_code, trade_date, adj_type, open, high, low, close, vol, amount],
            )
            .expect("insert stock_data");
        }
    }

    fn write_fixture_result_db(source_dir: &Path) {
        let db_path = result_db_path(source_dir.to_str().expect("utf8 path"));
        init_result_db(&db_path).expect("init result db");
        let conn = Connection::open(&db_path).expect("open result db");

        let summary_rows = [
            ("000001.SZ", "20240101", 92.0, 1_i64),
            ("000002.SZ", "20240101", 70.0, 3_i64),
            ("000003.SZ", "20240101", 80.0, 2_i64),
        ];
        for (ts_code, trade_date, total_score, rank) in summary_rows {
            conn.execute(
                "INSERT INTO score_summary (ts_code, trade_date, total_score, rank) VALUES (?, ?, ?, ?)",
                params![ts_code, trade_date, total_score, rank],
            )
            .expect("insert score_summary");
        }

        let detail_rows = [
            ("000001.SZ", "20240101", "ADV", 4.0),
            ("000001.SZ", "20240101", "COMP", 1.0),
            ("000002.SZ", "20240101", "NEG", -1.0),
            ("000003.SZ", "20240101", "ADV", 2.0),
        ];
        for (ts_code, trade_date, rule_name, rule_score) in detail_rows {
            conn.execute(
                "INSERT INTO score_details (ts_code, trade_date, rule_name, rule_score) VALUES (?, ?, ?, ?)",
                params![ts_code, trade_date, rule_name, rule_score],
            )
            .expect("insert score_details");
        }
    }

    #[test]
    fn spearman_corr_handles_monotonic_series() {
        let samples = vec![
            ScoreObservation {
                score: 1.0,
                future_return_pct: 1.0,
            },
            ScoreObservation {
                score: 2.0,
                future_return_pct: 2.0,
            },
            ScoreObservation {
                score: 3.0,
                future_return_pct: 3.0,
            },
        ];
        let corr = spearman_corr(&samples, false).expect("corr");
        assert!((corr - 1.0).abs() < 1e-9);
    }

    #[test]
    fn strategy_performance_page_builds_candidates_and_portfolios() {
        let source_dir = unique_temp_dir();
        write_fixture_files(&source_dir);
        write_fixture_source_db(&source_dir);
        write_fixture_result_db(&source_dir);

        let page = get_strategy_performance_page(
            source_dir.to_str().expect("utf8").to_string(),
            Some(5),
            Some(0.9),
            Some("manual".to_string()),
            Some(vec!["ADV".to_string()]),
            Some(1),
            Some(1),
            Some(1),
            Some(1),
            Some(false),
            Some(1),
            Some(1),
            Some(2),
            None,
            None,
            Some("ADV".to_string()),
        )
        .expect("load page");

        assert_eq!(page.selected_horizon, 5);
        assert!(
            page.resolved_advantage_rule_names
                .iter()
                .any(|item| item == "ADV")
        );
        assert!(
            page.rule_rows
                .iter()
                .any(|row| row.rule_name == "ADV" && row.auto_candidate)
        );
        assert!(
            page.resolved_companion_rule_names
                .iter()
                .any(|item| item == "COMP")
        );
        assert!(
            page.effective_negative_rule_names
                .iter()
                .any(|item| item == "NEG")
        );
        assert!(
            page.portfolio_rows
                .iter()
                .any(|row| row.strategy_key == "raw_topn")
        );
        assert_eq!(page.selected_rule_name.as_deref(), Some("ADV"));
        assert!(
            page.rule_detail
                .as_ref()
                .expect("rule detail")
                .directions
                .iter()
                .any(|direction| direction.signal_direction == "positive")
        );
    }

    #[test]
    fn strategy_validation_page_runs_for_draft_rule() {
        let source_dir = unique_temp_dir();
        write_fixture_files(&source_dir);
        write_fixture_source_db(&source_dir);
        write_fixture_result_db(&source_dir);

        let page = get_strategy_performance_validation_page(
            source_dir.to_str().expect("utf8").to_string(),
            Some(5),
            Some(0.9),
            StrategyManageRuleDraft {
                name: "ADV_DRAFT".to_string(),
                scope_way: "LAST".to_string(),
                scope_windows: 1,
                when: "C > O".to_string(),
                points: 2.0,
                dist_points: None,
                explain: "draft rule".to_string(),
                tag: "Normal".to_string(),
            },
        )
        .expect("load validation page");

        assert_eq!(page.selected_horizon, 5);
        assert_eq!(page.draft_summary.name, "ADV_DRAFT");
        assert!(
            page.rule_rows
                .iter()
                .any(|row| row.signal_direction == "positive")
        );
        assert!(
            page.rule_detail
                .as_ref()
                .expect("rule detail")
                .directions
                .iter()
                .any(|direction| direction.signal_direction == "positive")
        );
    }
}
