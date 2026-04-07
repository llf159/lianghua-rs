use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};

use duckdb::{Connection, params};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::data::{
    DataReader, RowData, RuleTag, ScopeWay, ScoreRule, load_trade_date_list, result_db_path,
    scoring_data::row_into_rt, source_db_path,
};
use crate::expr::{
    eval::{Runtime, Value},
    parser::{Expr, Parser, Stmt, Stmts, lex_all},
};
use crate::scoring::{
    CachedRule,
    tools::{calc_query_need_rows, calc_zhang_pct, load_st_list},
};
use crate::utils::utils::{eval_binary_for_warmup, impl_expr_warmup};

const DEFAULT_ADJ_TYPE: &str = "qfq";
const HORIZONS: [u32; 3] = [2, 3, 5];
const DEFAULT_SELECTED_HORIZON: u32 = 5;
const DEFAULT_STRONG_QUANTILE: f64 = 0.9;
const DEFAULT_MIN_SAMPLE: u32 = 30;
const DEFAULT_MIN_PASS_HORIZONS: u32 = 2;
const DEFAULT_MIN_ADV_HITS: u32 = 1;
const DEFAULT_TOP_LIMIT: u32 = 100;
const DEFAULT_AUTO_ADVANTAGE_LIMIT: usize = 10;
const PORTFOLIO_MIN_SAMPLE_COUNT: u32 = 50;
const SCORE_BUCKET_LIMIT: usize = 8;
const SCORE_BUCKET_QUANTILES: usize = 5;
const RESULT_DETAILS_TABLE: &str = "result_db.score_details";
const VALIDATION_DETAILS_TABLE: &str = "strategy_validate_rule_details";
const VALIDATION_MAX_COMBINATIONS: usize = 512;
const SCORE_MODE_IC_IR: &str = "ic_ir";
const SCORE_MODE_HIT_VS_NON_HIT: &str = "hit_vs_non_hit";

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ValidationStrategyDirection {
    #[default]
    Positive,
    Negative,
}

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
    pub score_mode: String,
    pub hit_n: u32,
    pub avg_future_return_pct: Option<f64>,
    pub strong_hit_rate: Option<f64>,
    pub strong_lift: Option<f64>,
    pub win_rate: Option<f64>,
    pub avg_total_score: Option<f64>,
    pub avg_rank: Option<f64>,
    pub hit_vs_non_hit_delta_pct: Option<f64>,
    pub rank_ic_mean: Option<f64>,
    pub icir: Option<f64>,
    pub sharpe_ratio: Option<f64>,
    pub layer_return_spread_pct: Option<f64>,
    pub composite_score: Option<f64>,
    pub ic_passes_floor: bool,
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
    pub overall_composite_score: Option<f64>,
    pub avg_rank_ic_mean: Option<f64>,
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
    pub sample_count: u32,
    pub avg_portfolio_return_pct: Option<f64>,
    pub avg_market_return_pct: Option<f64>,
    pub avg_excess_return_pct: Option<f64>,
    pub excess_win_rate: Option<f64>,
    pub strong_hit_rate: Option<f64>,
    pub strong_lift: Option<f64>,
    pub avg_selected_count: Option<f64>,
    pub rank_ic_mean: Option<f64>,
    pub icir: Option<f64>,
    pub layer_return_spread_pct: Option<f64>,
    pub composite_score: Option<f64>,
    pub sharpe_ratio: Option<f64>,
}

#[derive(Debug, Serialize, Clone)]
pub struct StrategyPerformancePortfolioRow {
    pub strategy_key: String,
    pub strategy_label: String,
    pub sort_description: String,
    pub windows: Vec<StrategyPerformancePortfolioWindow>,
}

#[derive(Debug, Serialize, Clone)]
pub struct StrategyPerformanceOverallScoreAnalysis {
    pub horizon: u32,
    pub sample_count: u32,
    pub avg_future_return_pct: Option<f64>,
    pub strong_hit_rate: Option<f64>,
    pub win_rate: Option<f64>,
    pub spearman_corr: Option<f64>,
    pub rank_ic_mean: Option<f64>,
    pub icir: Option<f64>,
    pub layer_return_spread_pct: Option<f64>,
    pub bucket_mode: String,
    pub score_rows: Vec<StrategyPerformanceScoreBucketRow>,
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
    pub score_mode: String,
    pub bucket_mode: String,
    pub sample_count: u32,
    pub avg_future_return_pct: Option<f64>,
    pub strong_hit_rate: Option<f64>,
    pub win_rate: Option<f64>,
    pub spearman_corr: Option<f64>,
    pub abs_spearman_corr: Option<f64>,
    pub rank_ic_mean: Option<f64>,
    pub icir: Option<f64>,
    pub sharpe_ratio: Option<f64>,
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
    pub auto_advantage_rule_names: Vec<String>,
    pub manual_advantage_rule_names: Vec<String>,
    pub auto_candidate_rule_names: Vec<String>,
    pub manual_rule_names: Vec<String>,
    pub ignored_manual_rule_names: Vec<String>,
    pub resolved_advantage_rule_names: Vec<String>,
    pub resolved_companion_rule_names: Vec<String>,
    pub effective_negative_rule_names: Vec<String>,
    pub ineffective_negative_rule_names: Vec<String>,
    pub min_adv_hits: u32,
    pub top_limit: u32,
    pub noisy_companion_rule_names: Vec<String>,
    pub rule_rows: Vec<StrategyPerformanceRuleRow>,
    pub companion_rows: Vec<StrategyPerformanceCompanionRow>,
    pub overall_score_analysis: Option<StrategyPerformanceOverallScoreAnalysis>,
    pub selected_rule_name: Option<String>,
    pub rule_detail: Option<StrategyPerformanceRuleDetail>,
    pub methods: Vec<StrategyPerformanceMethodNote>,
}

#[derive(Debug, Serialize, Clone)]
pub struct StrategyPerformanceHorizonViewData {
    pub selected_horizon: u32,
    pub noisy_companion_rule_names: Vec<String>,
    pub companion_rows: Vec<StrategyPerformanceCompanionRow>,
    pub overall_score_analysis: Option<StrategyPerformanceOverallScoreAnalysis>,
    pub advantage_score_analysis: Option<StrategyPerformanceOverallScoreAnalysis>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StrategyPerformancePickCachePayload {
    pub selected_horizon: u32,
    pub strong_quantile: f64,
    #[serde(default)]
    pub resolved_advantage_mode: String,
    #[serde(default)]
    pub auto_advantage_rule_names: Vec<String>,
    #[serde(default)]
    pub manual_rule_names: Vec<String>,
    pub resolved_advantage_rule_names: Vec<String>,
    pub resolved_noisy_companion_rule_names: Vec<String>,
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
    pub strategy_direction: ValidationStrategyDirection,
    pub horizons: Vec<u32>,
    pub selected_horizon: u32,
    pub strong_quantile: f64,
    pub future_summaries: Vec<StrategyPerformanceFutureSummary>,
    pub combo_summaries: Vec<StrategyPerformanceValidationComboSummary>,
    pub best_positive_case: Option<StrategyPerformanceValidationCaseData>,
    pub best_negative_case: Option<StrategyPerformanceValidationCaseData>,
    pub methods: Vec<StrategyPerformanceMethodNote>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StrategyValidationUnknownConfig {
    pub name: String,
    pub start: f64,
    pub end: f64,
    pub step: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StrategyPerformanceValidationDraft {
    #[serde(default)]
    pub strategy_direction: ValidationStrategyDirection,
    pub scope_way: String,
    pub scope_windows: usize,
    pub when: String,
    #[serde(default)]
    pub import_name: Option<String>,
    #[serde(default)]
    pub unknown_configs: Vec<StrategyValidationUnknownConfig>,
}

#[derive(Debug, Serialize, Clone)]
pub struct StrategyPerformanceValidationUnknownValue {
    pub name: String,
    pub value: f64,
}

#[derive(Debug, Serialize, Clone)]
pub struct StrategyPerformanceValidationComboSummary {
    pub combo_key: String,
    pub combo_label: String,
    pub import_name: Option<String>,
    pub formula: String,
    pub unknown_values: Vec<StrategyPerformanceValidationUnknownValue>,
    pub score_mode: String,
    pub trigger_samples: u32,
    pub triggered_days: u32,
    pub avg_daily_trigger: f64,
    pub positive_overall_composite_score: Option<f64>,
    pub positive_avg_future_return_pct: Option<f64>,
    pub positive_primary_metric: Option<f64>,
    pub positive_secondary_metric: Option<f64>,
    pub positive_hit_n: u32,
    pub negative_overall_composite_score: Option<f64>,
    pub negative_effective: bool,
    pub negative_avg_future_return_pct: Option<f64>,
    pub negative_primary_metric: Option<f64>,
    pub negative_secondary_metric: Option<f64>,
    pub negative_hit_n: u32,
}

#[derive(Debug, Serialize, Clone)]
pub struct StrategyPerformanceValidationLayerRow {
    pub label: String,
    pub layer_value: f64,
    pub sample_count: u32,
    pub avg_future_return_pct: Option<f64>,
    pub strong_hit_rate: Option<f64>,
    pub win_rate: Option<f64>,
}

#[derive(Debug, Serialize, Clone)]
pub struct StrategyPerformanceValidationSimilarityRow {
    pub rule_name: String,
    pub explain: Option<String>,
    pub overlap_samples: u32,
    pub overlap_rate_vs_validation: Option<f64>,
    pub overlap_rate_vs_existing: Option<f64>,
    pub overlap_lift: Option<f64>,
}

#[derive(Debug, Serialize, Clone)]
pub struct StrategyPerformanceValidationCaseData {
    pub combo_summary: StrategyPerformanceValidationComboSummary,
    pub positive_row: Option<StrategyPerformanceRuleRow>,
    pub negative_row: Option<StrategyPerformanceRuleRow>,
    pub layer_mode: String,
    pub layer_rows: Vec<StrategyPerformanceValidationLayerRow>,
    pub similarity_rows: Vec<StrategyPerformanceValidationSimilarityRow>,
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

#[derive(Debug, Default, Clone, Copy)]
struct DailyReturnSummary {
    sample_count: u32,
    mean_return_pct: f64,
    mean_square_return_pct: f64,
}

#[derive(Debug, Default, Clone, Copy)]
struct RuleDailyPositiveMetric {
    hit_n: u32,
    sum_return_pct: f64,
}

#[derive(Debug, Default, Clone, Copy)]
struct RuleFactorMetric {
    rank_ic_mean: Option<f64>,
    icir: Option<f64>,
    sharpe_ratio: Option<f64>,
}

#[derive(Debug, Clone)]
struct ScoreObservation {
    signal_date: String,
    score: f64,
    future_return_pct: f64,
}

#[derive(Clone)]
struct ValidationVariant {
    combo_key: String,
    combo_label: String,
    import_name: Option<String>,
    formula: String,
    unknown_values: Vec<StrategyPerformanceValidationUnknownValue>,
    cached_rule: CachedRule,
    layer_mode: ValidationLayerMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ValidationLayerMode {
    Flat,
    EachCount,
    RecentDistance,
}

#[derive(Debug, Clone)]
struct ValidationHitRow {
    ts_code: String,
    trade_date: String,
    rule_name: String,
    rule_score: f64,
    layer_value: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdvantageRuleMode {
    Auto,
    Manual,
    Combined,
}

fn open_source_conn(source_path: &str) -> Result<Connection, String> {
    let source_db = source_db_path(source_path);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "原始库路径不是有效UTF-8".to_string())?;
    Connection::open(source_db_str).map_err(|e| format!("打开原始库失败: {e}"))
}

fn open_result_conn(source_path: &str) -> Result<Connection, String> {
    let result_db = result_db_path(source_path);
    let result_db_str = result_db
        .to_str()
        .ok_or_else(|| "结果库路径不是有效UTF-8".to_string())?;
    Connection::open(result_db_str).map_err(|e| format!("打开结果库失败: {e}"))
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

fn ensure_strategy_pick_cache_table(result_conn: &Connection) -> Result<(), String> {
    result_conn
        .execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS strategy_pick_cache (
                cache_key VARCHAR PRIMARY KEY,
                payload_json VARCHAR,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            "#,
        )
        .map_err(|e| format!("创建策略选股缓存表失败: {e}"))
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

fn build_strategy_pick_cache_key(
    selected_horizon: u32,
    strong_quantile: f64,
    advantage_rule_mode: AdvantageRuleMode,
    manual_rule_names: &[String],
    auto_filter: &StrategyPerformanceAutoFilterConfig,
    min_adv_hits: u32,
) -> String {
    format!(
        "v5|h={selected_horizon}|q={strong_quantile:.6}|mode={}|manual={}|s2={}|s3={}|s5={}|s10={}|win={}|pass={}|adv_hits={}",
        advantage_mode_label(advantage_rule_mode),
        serde_json::to_string(manual_rule_names).unwrap_or_else(|_| "[]".to_string()),
        auto_filter.min_samples_2,
        auto_filter.min_samples_3,
        auto_filter.min_samples_5,
        auto_filter.min_samples_10,
        auto_filter.require_win_rate_above_market,
        auto_filter.min_pass_horizons,
        min_adv_hits,
    )
}

fn build_strategy_pick_cache_payload(
    selected_horizon: u32,
    strong_quantile: f64,
    resolved_advantage_mode: &str,
    auto_advantage_rule_names: &[String],
    manual_rule_names: &[String],
    resolved_advantage_rule_names: &[String],
    resolved_noisy_companion_rule_names: &[String],
) -> StrategyPerformancePickCachePayload {
    StrategyPerformancePickCachePayload {
        selected_horizon,
        strong_quantile,
        resolved_advantage_mode: resolved_advantage_mode.to_string(),
        auto_advantage_rule_names: auto_advantage_rule_names.to_vec(),
        manual_rule_names: manual_rule_names.to_vec(),
        resolved_advantage_rule_names: resolved_advantage_rule_names.to_vec(),
        resolved_noisy_companion_rule_names: resolved_noisy_companion_rule_names.to_vec(),
    }
}

fn load_strategy_pick_cache(
    source_path: &str,
    cache_key: &str,
) -> Result<Option<StrategyPerformancePickCachePayload>, String> {
    let result_conn = open_result_conn(source_path)?;
    ensure_strategy_pick_cache_table(&result_conn)?;
    let mut stmt = result_conn
        .prepare("SELECT payload_json FROM strategy_pick_cache WHERE cache_key = ?")
        .map_err(|e| format!("预编译策略选股缓存读取失败: {e}"))?;
    let mut rows = stmt
        .query(params![cache_key])
        .map_err(|e| format!("查询策略选股缓存失败: {e}"))?;
    let Some(row) = rows
        .next()
        .map_err(|e| format!("读取策略选股缓存失败: {e}"))?
    else {
        return Ok(None);
    };
    let payload_json: String = row
        .get(0)
        .map_err(|e| format!("读取策略选股缓存内容失败: {e}"))?;
    let payload = serde_json::from_str::<StrategyPerformancePickCachePayload>(&payload_json)
        .map_err(|e| format!("解析策略选股缓存失败: {e}"))?;
    Ok(Some(payload))
}

fn load_latest_strategy_pick_cache(
    source_path: &str,
) -> Result<Option<StrategyPerformancePickCachePayload>, String> {
    let result_conn = open_result_conn(source_path)?;
    ensure_strategy_pick_cache_table(&result_conn)?;
    let mut stmt = result_conn
        .prepare(
            "SELECT payload_json FROM strategy_pick_cache ORDER BY updated_at DESC NULLS LAST LIMIT 1",
        )
        .map_err(|e| format!("预编译最新策略选股缓存读取失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询最新策略选股缓存失败: {e}"))?;
    let Some(row) = rows
        .next()
        .map_err(|e| format!("读取最新策略选股缓存失败: {e}"))?
    else {
        return Ok(None);
    };
    let payload_json: String = row
        .get(0)
        .map_err(|e| format!("读取最新策略选股缓存内容失败: {e}"))?;
    let payload = serde_json::from_str::<StrategyPerformancePickCachePayload>(&payload_json)
        .map_err(|e| format!("解析最新策略选股缓存失败: {e}"))?;
    Ok(Some(payload))
}

fn save_strategy_pick_cache(
    source_path: &str,
    cache_key: &str,
    payload: &StrategyPerformancePickCachePayload,
) -> Result<(), String> {
    let result_conn = open_result_conn(source_path)?;
    ensure_strategy_pick_cache_table(&result_conn)?;
    let payload_json =
        serde_json::to_string(payload).map_err(|e| format!("序列化策略选股缓存失败: {e}"))?;
    result_conn
        .execute(
            r#"
            INSERT INTO strategy_pick_cache (cache_key, payload_json, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (cache_key) DO UPDATE
            SET payload_json = EXCLUDED.payload_json,
                updated_at = EXCLUDED.updated_at
            "#,
            params![cache_key, payload_json],
        )
        .map_err(|e| format!("写入策略选股缓存失败: {e}"))?;
    Ok(())
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

fn format_validation_number(value: f64) -> String {
    let rounded = value.round();
    if (value - rounded).abs() < 1e-9 {
        format!("{rounded:.0}")
    } else {
        let mut text = format!("{value:.6}");
        while text.contains('.') && text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.pop();
        }
        text
    }
}

fn expand_unknown_config(config: &StrategyValidationUnknownConfig) -> Result<Vec<f64>, String> {
    let name = config.name.trim();
    if name.is_empty() {
        return Err("未知数名称不能为空".to_string());
    }
    if !config.start.is_finite() || !config.end.is_finite() || !config.step.is_finite() {
        return Err(format!("未知数 {name} 存在非法数值"));
    }
    if config.step <= 0.0 {
        return Err(format!("未知数 {name} 的 step 必须 > 0"));
    }
    if config.end < config.start {
        return Err(format!("未知数 {name} 的 end 不能小于 start"));
    }

    let mut values = Vec::new();
    let mut current = config.start;
    let mut guard = 0usize;
    while current <= config.end + config.step * 1e-9 {
        values.push(current.min(config.end));
        current += config.step;
        guard += 1;
        if guard > VALIDATION_MAX_COMBINATIONS * 8 {
            return Err(format!(
                "未知数 {name} 的取值数量过多，请增大 step 或缩小范围"
            ));
        }
    }
    if values.is_empty() {
        values.push(config.start);
    }
    Ok(values)
}

fn validation_summary_matches_direction(
    summary: &StrategyPerformanceValidationComboSummary,
    strategy_direction: ValidationStrategyDirection,
) -> bool {
    match strategy_direction {
        ValidationStrategyDirection::Positive => summary.positive_hit_n > 0,
        ValidationStrategyDirection::Negative => summary.negative_hit_n > 0,
    }
}

fn select_best_validation_summary(
    combo_summaries: &[StrategyPerformanceValidationComboSummary],
    strategy_direction: ValidationStrategyDirection,
) -> Option<StrategyPerformanceValidationComboSummary> {
    combo_summaries
        .iter()
        .filter(|summary| validation_summary_matches_direction(summary, strategy_direction))
        .max_by(|left, right| match strategy_direction {
            ValidationStrategyDirection::Positive => compare_option_f64_desc(
                left.positive_overall_composite_score,
                right.positive_overall_composite_score,
            )
            .then_with(|| {
                compare_option_f64_desc(
                    left.positive_avg_future_return_pct,
                    right.positive_avg_future_return_pct,
                )
            })
            .then_with(|| right.positive_hit_n.cmp(&left.positive_hit_n))
            .then_with(|| left.combo_key.cmp(&right.combo_key)),
            ValidationStrategyDirection::Negative => left
                .negative_effective
                .cmp(&right.negative_effective)
                .then_with(|| {
                    compare_option_f64_asc(
                        left.negative_overall_composite_score,
                        right.negative_overall_composite_score,
                    )
                })
                .then_with(|| {
                    compare_option_f64_asc(
                        left.negative_avg_future_return_pct,
                        right.negative_avg_future_return_pct,
                    )
                })
                .then_with(|| right.negative_hit_n.cmp(&left.negative_hit_n))
                .then_with(|| left.combo_key.cmp(&right.combo_key)),
        })
        .cloned()
}

fn build_validation_variants(
    draft: &StrategyPerformanceValidationDraft,
) -> Result<Vec<ValidationVariant>, String> {
    let when = draft.when.trim();
    if when.is_empty() {
        return Err("策略表达式不能为空".to_string());
    }
    if draft.scope_windows == 0 {
        return Err("scope_windows 必须 >= 1".to_string());
    }

    let scope_way = parse_scope_way_text(&draft.scope_way)?;
    let layer_mode = match scope_way {
        ScopeWay::Each => ValidationLayerMode::EachCount,
        ScopeWay::Recent => ValidationLayerMode::RecentDistance,
        _ => ValidationLayerMode::Flat,
    };

    let mut unknown_groups = Vec::<(String, Vec<f64>)>::new();
    let mut total_combinations = 1usize;
    let mut seen = HashSet::new();
    for config in &draft.unknown_configs {
        let name = config.name.trim();
        if name.is_empty() {
            continue;
        }
        if !seen.insert(name.to_string()) {
            return Err(format!("未知数名称重复: {name}"));
        }
        let values = expand_unknown_config(config)?;
        total_combinations = total_combinations.saturating_mul(values.len().max(1));
        if total_combinations > VALIDATION_MAX_COMBINATIONS {
            return Err(format!(
                "未知数组合过多({total_combinations})，当前上限为 {VALIDATION_MAX_COMBINATIONS}"
            ));
        }
        unknown_groups.push((name.to_string(), values));
    }

    let mut variants = Vec::new();
    let mut assignments = Vec::<(String, f64)>::new();

    fn replace_validation_unknowns(formula: &str, assignments: &[(String, f64)]) -> String {
        if assignments.is_empty() {
            return formula.to_string();
        }

        let replace_map = assignments
            .iter()
            .map(|(name, value)| (name.as_str(), format_validation_number(*value)))
            .collect::<HashMap<_, _>>();
        let tokens = lex_all(formula);
        let mut out = String::with_capacity(formula.len() + assignments.len() * 4);
        let mut cursor = 0usize;

        for token in tokens {
            if token.start > cursor {
                out.push_str(&formula[cursor..token.start]);
            }
            match token.kind {
                crate::expr::lexer::TokenKind::Ident(name) => {
                    if let Some(replacement) = replace_map.get(name.as_str()) {
                        out.push_str(replacement);
                    } else {
                        out.push_str(&formula[token.start..token.end]);
                    }
                }
                crate::expr::lexer::TokenKind::Eof => {}
                _ => out.push_str(&formula[token.start..token.end]),
            }
            cursor = token.end;
        }

        if cursor < formula.len() {
            out.push_str(&formula[cursor..]);
        }
        out
    }

    fn walk_variants(
        index: usize,
        unknown_groups: &[(String, Vec<f64>)],
        assignments: &mut Vec<(String, f64)>,
        draft: &StrategyPerformanceValidationDraft,
        scope_way: ScopeWay,
        layer_mode: ValidationLayerMode,
        out: &mut Vec<ValidationVariant>,
    ) -> Result<(), String> {
        if index >= unknown_groups.len() {
            let mut sorted = assignments.clone();
            sorted
                .sort_by(|left, right| right.0.len().cmp(&left.0.len()).then(left.0.cmp(&right.0)));
            let unknown_values = sorted
                .iter()
                .map(|(name, value)| StrategyPerformanceValidationUnknownValue {
                    name: name.clone(),
                    value: *value,
                })
                .collect::<Vec<_>>();
            let formula = replace_validation_unknowns(draft.when.trim(), &sorted);

            let tokens = lex_all(&formula);
            let mut parser = Parser::new(tokens);
            let stmts = parser
                .parse_main()
                .map_err(|e| format!("表达式解析错误在{}:{}", e.idx, e.msg))?;
            let combo_key = format!("validation_combo_{:03}", out.len() + 1);
            let combo_label = if unknown_values.is_empty() {
                "默认参数".to_string()
            } else {
                unknown_values
                    .iter()
                    .map(|item| format!("{}={}", item.name, format_validation_number(item.value)))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            out.push(ValidationVariant {
                combo_key: combo_key.clone(),
                combo_label: combo_label.clone(),
                import_name: draft
                    .import_name
                    .clone()
                    .filter(|value| !value.trim().is_empty()),
                formula,
                unknown_values,
                cached_rule: CachedRule {
                    name: combo_key,
                    scope_windows: draft.scope_windows,
                    scope_way,
                    points: 1.0,
                    dist_points: None,
                    tag: RuleTag::Normal,
                    when_src: draft.when.trim().to_string(),
                    when_ast: stmts,
                },
                layer_mode,
            });
            return Ok(());
        }

        let (name, values) = &unknown_groups[index];
        for value in values {
            assignments.push((name.clone(), *value));
            walk_variants(
                index + 1,
                unknown_groups,
                assignments,
                draft,
                scope_way,
                layer_mode,
                out,
            )?;
            assignments.pop();
        }
        Ok(())
    }

    walk_variants(
        0,
        &unknown_groups,
        &mut assignments,
        draft,
        scope_way,
        layer_mode,
        &mut variants,
    )?;

    if variants.is_empty() {
        return Err("没有可用的验证组合".to_string());
    }
    Ok(variants)
}

fn validation_hit_value(
    scope_way: ScopeWay,
    windows: usize,
    bs: &[bool],
    index: usize,
) -> Option<f64> {
    match scope_way {
        ScopeWay::Last => bs[index].then_some(1.0),
        ScopeWay::Any => {
            let start = (index + 1).saturating_sub(windows);
            (start..=index).any(|j| bs[j]).then_some(1.0)
        }
        ScopeWay::Consec(threshold) => {
            let start = (index + 1).saturating_sub(windows);
            let mut current = 0usize;
            let mut best = 0usize;
            for hit in bs.iter().take(index + 1).skip(start) {
                if *hit {
                    current += 1;
                } else {
                    current = 0;
                }
                best = best.max(current);
            }
            (best >= threshold).then_some(best as f64)
        }
        ScopeWay::Each => {
            let start = (index + 1).saturating_sub(windows);
            let count = bs
                .iter()
                .take(index + 1)
                .skip(start)
                .filter(|hit| **hit)
                .count();
            (count > 0).then_some(count as f64)
        }
        ScopeWay::Recent => {
            let start = (index + 1).saturating_sub(windows);
            for j in (start..=index).rev() {
                if bs[j] {
                    return Some((index - j) as f64);
                }
            }
            None
        }
    }
}

fn eval_validation_hits_for_variant(
    rt: &mut Runtime,
    trade_dates: &[String],
    variant: &ValidationVariant,
    signal_date_set: &HashSet<String>,
    keep_from: usize,
    ts_code: &str,
) -> Result<Vec<ValidationHitRow>, String> {
    let value = rt
        .eval_program(&variant.cached_rule.when_ast)
        .map_err(|e| format!("表达式计算错误: {}", e.msg))?;
    let bs = Value::as_bool_series(&value, trade_dates.len())
        .map_err(|e| format!("表达式返回值非布尔: {}", e.msg))?;

    let mut rows = Vec::new();
    for (index, trade_date) in trade_dates.iter().enumerate().skip(keep_from) {
        if !signal_date_set.contains(trade_date) {
            continue;
        }
        let Some(layer_value) = validation_hit_value(
            variant.cached_rule.scope_way,
            variant.cached_rule.scope_windows,
            &bs,
            index,
        ) else {
            continue;
        };
        rows.push(ValidationHitRow {
            ts_code: ts_code.to_string(),
            trade_date: trade_date.clone(),
            rule_name: variant.combo_key.clone(),
            rule_score: 1.0,
            layer_value,
        });
    }
    Ok(rows)
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

fn collect_validation_variant_hits(
    source_path: &str,
    end_date: &str,
    start_date: &str,
    signal_date_set: &HashSet<String>,
    ts_codes: &[String],
    variants: &[ValidationVariant],
    need_rows: usize,
) -> Result<Vec<ValidationHitRow>, String> {
    let st_list = load_st_list(source_path)?;

    ts_codes
        .par_chunks(128)
        .map(|ts_group| -> Result<Vec<ValidationHitRow>, String> {
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

                for variant in variants {
                    let mut runtime = row_into_rt(row_data.clone())?;
                    group_rows.extend(eval_validation_hits_for_variant(
                        &mut runtime,
                        &trade_dates,
                        variant,
                        signal_date_set,
                        keep_from,
                        ts_code,
                    )?);
                }
            }

            Ok(group_rows)
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|groups| groups.into_iter().flatten().collect())
}

fn build_validation_rule_meta(variants: &[ValidationVariant]) -> HashMap<String, RuleMeta> {
    variants
        .iter()
        .map(|variant| {
            (
                variant.combo_key.clone(),
                RuleMeta {
                    explain: variant.combo_label.clone(),
                    tag: RuleTag::Normal,
                    scope_way_label: scope_way_label(variant.cached_rule.scope_way),
                    scope_windows: variant.cached_rule.scope_windows as u32,
                    points: 1.0,
                    has_dist_points: false,
                },
            )
        })
        .collect()
}

fn prepare_temp_validation_variant_details(
    source_conn: &Connection,
    source_path: &str,
    draft: &StrategyPerformanceValidationDraft,
) -> Result<Vec<ValidationVariant>, String> {
    let variants = build_validation_variants(draft)?;
    let warmup_need = variants.iter().try_fold(0usize, |current, variant| {
        estimate_custom_rule_warmup(
            &variant.cached_rule.when_ast,
            variant.cached_rule.scope_way,
            variant.cached_rule.scope_windows,
        )
        .map(|need| current.max(need))
    })?;
    let (start_date, end_date) = query_score_summary_date_range(source_conn)?;
    let rank_dates = query_rank_trade_dates(source_conn)?;
    let signal_date_set = rank_dates.into_iter().collect::<HashSet<_>>();
    let ts_codes = query_rank_ts_codes(source_conn)?;
    let need_rows = calc_query_need_rows(source_path, warmup_need, &start_date, &end_date)?;
    let hit_rows = collect_validation_variant_hits(
        source_path,
        &end_date,
        &start_date,
        &signal_date_set,
        &ts_codes,
        &variants,
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
                rule_score DOUBLE,
                layer_value DOUBLE
            );
            "#
        ))
        .map_err(|e| format!("创建临时验证明细表失败: {e}"))?;

    let mut appender = source_conn
        .appender(VALIDATION_DETAILS_TABLE)
        .map_err(|e| format!("创建临时验证明细 Appender 失败: {e}"))?;
    for row in hit_rows {
        appender
            .append_row(params![
                row.ts_code,
                row.trade_date,
                row.rule_name,
                row.rule_score,
                row.layer_value
            ])
            .map_err(|e| format!("写入临时验证明细失败: {e}"))?;
    }
    appender
        .flush()
        .map_err(|e| format!("刷新临时验证明细 Appender 失败: {e}"))?;

    Ok(variants)
}

fn build_validation_rule_rows(
    strategy_options: &[String],
    rule_meta: &HashMap<String, RuleMeta>,
    rule_aggregates: &HashMap<(String, bool, u32), RuleAggMetric>,
    positive_rule_factor_metrics: &HashMap<(String, u32), RuleFactorMetric>,
    negative_rule_factor_metrics: &HashMap<(String, u32), RuleFactorMetric>,
    future_summary_map: &HashMap<u32, StrategyPerformanceFutureSummary>,
    auto_filter: &StrategyPerformanceAutoFilterConfig,
    selected_horizon: u32,
) -> Vec<StrategyPerformanceRuleRow> {
    let mut rows = Vec::new();

    for rule_name in strategy_options {
        let Some(meta) = rule_meta.get(rule_name) else {
            continue;
        };

        let mut positive_metrics = Vec::with_capacity(HORIZONS.len());
        let mut negative_metrics = Vec::with_capacity(HORIZONS.len());
        let mut any_hit = false;
        let score_mode = score_mode_for_rule(
            &meta.scope_way_label,
            meta.scope_windows,
            meta.has_dist_points,
        );

        for horizon in HORIZONS {
            let agg = rule_aggregates
                .get(&(rule_name.clone(), true, horizon))
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
            let positive_factor_metric = positive_rule_factor_metrics
                .get(&(rule_name.clone(), horizon))
                .copied()
                .unwrap_or_default();
            let negative_factor_metric = negative_rule_factor_metrics
                .get(&(rule_name.clone(), horizon))
                .copied()
                .unwrap_or_default();
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
            let positive_composite_score = build_positive_composite_score_with_mode(
                score_mode,
                agg,
                market_summary,
                strong_lift,
                hit_vs_non_hit_delta_pct,
                positive_factor_metric,
            );
            let negative_composite_score = build_negative_composite_score_with_mode(
                score_mode,
                agg,
                market_summary,
                strong_lift,
                hit_vs_non_hit_delta_pct,
                negative_factor_metric,
            );
            let has_positive_score = rule_score_passes_floor(
                score_mode,
                hit_vs_non_hit_delta_pct,
                positive_factor_metric,
            );
            let has_negative_score = rule_score_passes_floor(
                score_mode,
                hit_vs_non_hit_delta_pct,
                negative_factor_metric,
            );
            let passes_auto_filter = agg.hit_n >= min_sample_for_horizon(auto_filter, horizon)
                && has_positive_score
                && positive_composite_score.unwrap_or(f64::NEG_INFINITY) > 0.0;
            let passes_negative_filter = passes_negative_effective_filter(
                agg,
                negative_composite_score,
                has_negative_score,
                auto_filter,
                horizon,
            );
            let metric = StrategyPerformanceHorizonMetric {
                horizon,
                score_mode: score_mode.to_string(),
                hit_n: agg.hit_n,
                avg_future_return_pct: agg.avg_future_return_pct,
                strong_hit_rate: agg.strong_hit_rate,
                strong_lift,
                win_rate: agg.win_rate,
                avg_total_score: agg.avg_total_score,
                avg_rank: agg.avg_rank,
                hit_vs_non_hit_delta_pct,
                rank_ic_mean: positive_factor_metric.rank_ic_mean,
                icir: positive_factor_metric.icir,
                sharpe_ratio: positive_factor_metric.sharpe_ratio,
                layer_return_spread_pct: hit_vs_non_hit_delta_pct,
                composite_score: positive_composite_score,
                ic_passes_floor: has_positive_score,
                low_confidence,
                passes_auto_filter,
                passes_negative_filter,
            };
            positive_metrics.push(metric.clone());
            negative_metrics.push(StrategyPerformanceHorizonMetric {
                composite_score: negative_composite_score,
                rank_ic_mean: negative_factor_metric.rank_ic_mean,
                icir: negative_factor_metric.icir,
                sharpe_ratio: negative_factor_metric.sharpe_ratio,
                ic_passes_floor: has_negative_score,
                passes_auto_filter: false,
                ..metric
            });
        }

        if !any_hit {
            continue;
        }

        let positive_overall = positive_metrics
            .iter()
            .filter_map(|metric| metric.composite_score)
            .collect::<Vec<_>>();
        let positive_overall = mean_and_std(&positive_overall).map(|(mean, _)| mean);
        let avg_rank_ic_mean = mean_and_std(
            &positive_metrics
                .iter()
                .filter_map(|metric| metric.rank_ic_mean)
                .collect::<Vec<_>>(),
        )
        .map(|(mean, _)| mean);

        rows.push(StrategyPerformanceRuleRow {
            rule_name: rule_name.clone(),
            explain: Some(meta.explain.clone()),
            tag: None,
            scope_way: Some(meta.scope_way_label.clone()),
            scope_windows: Some(meta.scope_windows),
            points: None,
            has_dist_points: false,
            signal_direction: "positive".to_string(),
            direction_label: "正向策略".to_string(),
            auto_candidate: positive_overall.unwrap_or(f64::NEG_INFINITY) > 0.0
                && positive_metrics.iter().any(|metric| metric.ic_passes_floor),
            manually_selected: false,
            in_advantage_set: false,
            in_companion_set: false,
            negative_effective: None,
            negative_effectiveness_label: None,
            negative_review_notes: Vec::new(),
            overall_composite_score: positive_overall,
            avg_rank_ic_mean,
            metrics: positive_metrics,
        });

        let negative_base_composite_score = mean_and_std(
            &negative_metrics
                .iter()
                .filter_map(|metric| metric.composite_score)
                .collect::<Vec<_>>(),
        )
        .map(|(mean, _)| mean);
        let negative_effective = is_negative_effective(
            &negative_metrics,
            selected_horizon,
            auto_filter,
            negative_base_composite_score,
        );
        rows.push(StrategyPerformanceRuleRow {
            rule_name: rule_name.clone(),
            explain: Some(meta.explain.clone()),
            tag: None,
            scope_way: Some(meta.scope_way_label.clone()),
            scope_windows: Some(meta.scope_windows),
            points: None,
            has_dist_points: false,
            signal_direction: "negative".to_string(),
            direction_label: "负向策略".to_string(),
            auto_candidate: false,
            manually_selected: false,
            in_advantage_set: false,
            in_companion_set: false,
            negative_effective: Some(negative_effective),
            negative_effectiveness_label: Some(if negative_effective {
                "当前更像负向策略".to_string()
            } else {
                "负向特征仍待观察".to_string()
            }),
            negative_review_notes: build_negative_review_notes(
                &negative_metrics,
                selected_horizon,
                future_summary_map.get(&selected_horizon),
                auto_filter,
                negative_base_composite_score,
            ),
            overall_composite_score: negative_base_composite_score,
            avg_rank_ic_mean: avg_rank_ic_mean,
            metrics: negative_metrics,
        });
    }

    rows
}

fn load_validation_layer_rows(
    source_conn: &Connection,
    selected_horizon: u32,
    combo_key: &str,
    layer_mode: ValidationLayerMode,
    strong_threshold_pct: Option<f64>,
) -> Result<Vec<StrategyPerformanceValidationLayerRow>, String> {
    let mut stmt = source_conn
        .prepare(&format!(
            r#"
            SELECT
                d.layer_value,
                COUNT(*) AS sample_count,
                AVG(r.future_return_pct) AS avg_future_return_pct,
                AVG(CASE WHEN r.future_return_pct > 0 THEN 1.0 ELSE 0.0 END) AS win_rate
            FROM {VALIDATION_DETAILS_TABLE} AS d
            INNER JOIN strategy_perf_sample_returns AS r
                ON r.ts_code = d.ts_code
               AND r.signal_date = d.trade_date
               AND r.horizon = ?
            WHERE d.rule_name = ?
            GROUP BY d.layer_value
            ORDER BY d.layer_value ASC
            "#
        ))
        .map_err(|e| format!("预编译验证分层读取失败: {e}"))?;
    let mut rows = stmt
        .query(params![selected_horizon as i64, combo_key])
        .map_err(|e| format!("查询验证分层失败: {e}"))?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(|e| format!("读取验证分层失败: {e}"))? {
        let layer_value = row
            .get::<_, Option<f64>>(0)
            .map_err(|e| format!("读取分层值失败: {e}"))?
            .unwrap_or(0.0);
        let sample_count = row
            .get::<_, i64>(1)
            .map_err(|e| format!("读取分层样本失败: {e}"))?
            .max(0) as u32;
        let avg_future_return_pct = row
            .get::<_, Option<f64>>(2)
            .map_err(|e| format!("读取分层平均收益失败: {e}"))?;
        let win_rate = row
            .get::<_, Option<f64>>(3)
            .map_err(|e| format!("读取分层胜率失败: {e}"))?;
        let label = match layer_mode {
            ValidationLayerMode::EachCount => {
                format!("命中 {} 次", format_validation_number(layer_value))
            }
            ValidationLayerMode::RecentDistance => {
                format!("最近触发距今 {} 根", format_validation_number(layer_value))
            }
            ValidationLayerMode::Flat => "触发样本".to_string(),
        };
        let strong_hit_rate = strong_threshold_pct.and_then(|threshold| {
            if sample_count == 0 {
                None
            } else {
                let mut strong_stmt = source_conn
                    .prepare(&format!(
                        r#"
                        SELECT
                            AVG(CASE WHEN r.future_return_pct >= ? THEN 1.0 ELSE 0.0 END)
                        FROM {VALIDATION_DETAILS_TABLE} AS d
                        INNER JOIN strategy_perf_sample_returns AS r
                            ON r.ts_code = d.ts_code
                           AND r.signal_date = d.trade_date
                           AND r.horizon = ?
                        WHERE d.rule_name = ?
                          AND d.layer_value = ?
                        "#
                    ))
                    .ok()?;
                let mut strong_rows = strong_stmt
                    .query(params![
                        threshold,
                        selected_horizon as i64,
                        combo_key,
                        layer_value
                    ])
                    .ok()?;
                let row = strong_rows.next().ok()??;
                row.get::<_, Option<f64>>(0).ok().flatten()
            }
        });
        out.push(StrategyPerformanceValidationLayerRow {
            label,
            layer_value,
            sample_count,
            avg_future_return_pct,
            strong_hit_rate,
            win_rate,
        });
    }
    Ok(out)
}

fn load_validation_similarity_rows(
    source_conn: &Connection,
    combo_key: &str,
    exclude_rule_name: Option<&str>,
    rule_meta: &HashMap<String, RuleMeta>,
) -> Result<Vec<StrategyPerformanceValidationSimilarityRow>, String> {
    let excluded_rule_name = exclude_rule_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let total_samples = source_conn
        .query_row("SELECT COUNT(*) FROM result_db.score_summary", [], |row| {
            row.get::<_, i64>(0)
        })
        .map_err(|e| format!("读取验证样本总数失败: {e}"))?
        .max(0) as f64;
    let combo_hit_count = source_conn
        .query_row(
            &format!("SELECT COUNT(*) FROM {VALIDATION_DETAILS_TABLE} WHERE rule_name = ?"),
            params![combo_key],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| format!("读取验证命中数失败: {e}"))?
        .max(0) as f64;
    if combo_hit_count <= 0.0 {
        return Ok(Vec::new());
    }

    let mut stmt = source_conn
        .prepare(&format!(
            r#"
            WITH validation_hits AS (
                SELECT ts_code, trade_date
                FROM {VALIDATION_DETAILS_TABLE}
                WHERE rule_name = ?
            ),
            existing_hits AS (
                SELECT rule_name, COUNT(*) AS hit_count
                FROM result_db.score_details
                WHERE rule_score > 0
                GROUP BY rule_name
            )
            SELECT
                e.rule_name,
                COUNT(*) AS overlap_samples,
                eh.hit_count
            FROM validation_hits AS v
            INNER JOIN result_db.score_details AS e
                ON e.ts_code = v.ts_code
               AND e.trade_date = v.trade_date
               AND e.rule_score > 0
            INNER JOIN existing_hits AS eh
                ON eh.rule_name = e.rule_name
            WHERE (? IS NULL OR e.rule_name <> ?)
            GROUP BY e.rule_name, eh.hit_count
            ORDER BY overlap_samples DESC, e.rule_name ASC
            LIMIT 20
            "#
        ))
        .map_err(|e| format!("预编译触发相似度失败: {e}"))?;
    let mut rows = stmt
        .query(params![
            combo_key,
            excluded_rule_name.as_deref(),
            excluded_rule_name.as_deref()
        ])
        .map_err(|e| format!("查询触发相似度失败: {e}"))?;
    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取触发相似度失败: {e}"))?
    {
        let rule_name: String = row.get(0).map_err(|e| format!("读取相似策略名失败: {e}"))?;
        if excluded_rule_name
            .as_deref()
            .is_some_and(|excluded| rule_name == excluded)
        {
            continue;
        }
        let overlap_samples = row
            .get::<_, i64>(1)
            .map_err(|e| format!("读取同时触发样本失败: {e}"))?
            .max(0) as u32;
        let existing_hit_count = row
            .get::<_, i64>(2)
            .map_err(|e| format!("读取现有策略命中数失败: {e}"))?
            .max(0) as f64;
        let overlap_rate_vs_validation = Some(overlap_samples as f64 / combo_hit_count);
        let overlap_rate_vs_existing = if existing_hit_count > 0.0 {
            Some(overlap_samples as f64 / existing_hit_count)
        } else {
            None
        };
        let overlap_lift = if total_samples > 0.0 && existing_hit_count > 0.0 {
            Some(overlap_samples as f64 * total_samples / (combo_hit_count * existing_hit_count))
        } else {
            None
        };
        out.push(StrategyPerformanceValidationSimilarityRow {
            rule_name: rule_name.clone(),
            explain: rule_meta.get(&rule_name).map(|meta| meta.explain.clone()),
            overlap_samples,
            overlap_rate_vs_validation,
            overlap_rate_vs_existing,
            overlap_lift,
        });
    }
    Ok(out)
}

fn build_validation_combo_summaries(
    variants: &[ValidationVariant],
    rule_rows: &[StrategyPerformanceRuleRow],
    source_conn: &Connection,
    selected_horizon: u32,
) -> Result<Vec<StrategyPerformanceValidationComboSummary>, String> {
    let positive_map = rule_rows
        .iter()
        .filter(|row| row.signal_direction == "positive")
        .map(|row| (row.rule_name.clone(), row))
        .collect::<HashMap<_, _>>();
    let negative_map = rule_rows
        .iter()
        .filter(|row| row.signal_direction == "negative")
        .map(|row| (row.rule_name.clone(), row))
        .collect::<HashMap<_, _>>();

    let mut summaries = Vec::new();
    for variant in variants {
        let positive_row = positive_map.get(&variant.combo_key);
        let negative_row = negative_map.get(&variant.combo_key);
        let trigger_samples = source_conn
            .query_row(
                &format!("SELECT COUNT(*) FROM {VALIDATION_DETAILS_TABLE} WHERE rule_name = ?"),
                params![variant.combo_key.as_str()],
                |row| row.get::<_, i64>(0),
            )
            .map_err(|e| format!("读取触发样本数失败: {e}"))?
            .max(0) as u32;
        let triggered_days = source_conn
            .query_row(
                &format!(
                    "SELECT COUNT(DISTINCT trade_date) FROM {VALIDATION_DETAILS_TABLE} WHERE rule_name = ?"
                ),
                params![variant.combo_key.as_str()],
                |row| row.get::<_, i64>(0),
            )
            .map_err(|e| format!("读取触发交易日数失败: {e}"))?
            .max(0) as u32;
        let avg_daily_trigger = if triggered_days > 0 {
            trigger_samples as f64 / triggered_days as f64
        } else {
            0.0
        };
        let positive_metric =
            positive_row.and_then(|row| metric_for_horizon(&row.metrics, selected_horizon));
        let negative_metric =
            negative_row.and_then(|row| metric_for_horizon(&row.metrics, selected_horizon));
        let score_mode = positive_metric
            .map(|metric| metric.score_mode.clone())
            .or_else(|| negative_metric.map(|metric| metric.score_mode.clone()))
            .unwrap_or_else(|| SCORE_MODE_HIT_VS_NON_HIT.to_string());
        summaries.push(StrategyPerformanceValidationComboSummary {
            combo_key: variant.combo_key.clone(),
            combo_label: variant.combo_label.clone(),
            import_name: variant.import_name.clone(),
            formula: variant.formula.clone(),
            unknown_values: variant.unknown_values.clone(),
            score_mode,
            trigger_samples,
            triggered_days,
            avg_daily_trigger,
            positive_overall_composite_score: positive_row
                .and_then(|row| row.overall_composite_score),
            positive_avg_future_return_pct: positive_metric
                .and_then(|metric| metric.avg_future_return_pct),
            positive_primary_metric: positive_metric.and_then(metric_primary_score),
            positive_secondary_metric: positive_metric.and_then(metric_secondary_score),
            positive_hit_n: positive_metric.map(|metric| metric.hit_n).unwrap_or(0),
            negative_overall_composite_score: negative_row
                .and_then(|row| row.overall_composite_score),
            negative_effective: negative_row
                .and_then(|row| row.negative_effective)
                .unwrap_or(false),
            negative_avg_future_return_pct: negative_metric
                .and_then(|metric| metric.avg_future_return_pct),
            negative_primary_metric: negative_metric.and_then(metric_primary_score),
            negative_secondary_metric: negative_metric.and_then(metric_secondary_score),
            negative_hit_n: negative_metric.map(|metric| metric.hit_n).unwrap_or(0),
        });
    }
    Ok(summaries)
}

fn build_validation_case_data(
    source_path: &str,
    source_conn: &Connection,
    selected_horizon: u32,
    strong_quantile: f64,
    summary: &StrategyPerformanceValidationComboSummary,
    positive_row: Option<StrategyPerformanceRuleRow>,
    negative_row: Option<StrategyPerformanceRuleRow>,
    layer_mode: ValidationLayerMode,
    future_summary_map: &HashMap<u32, StrategyPerformanceFutureSummary>,
    existing_rule_meta: &HashMap<String, RuleMeta>,
) -> Result<StrategyPerformanceValidationCaseData, String> {
    let layer_rows = load_validation_layer_rows(
        source_conn,
        selected_horizon,
        &summary.combo_key,
        layer_mode,
        future_summary_map
            .get(&selected_horizon)
            .and_then(|summary| summary.strong_threshold_pct),
    )?;
    let similarity_rows = load_validation_similarity_rows(
        source_conn,
        &summary.combo_key,
        summary.import_name.as_deref(),
        existing_rule_meta,
    )?;
    let _ = (
        source_path,
        strong_quantile,
        future_summary_map
            .get(&selected_horizon)
            .map(|summary| summary.horizon),
    );

    Ok(StrategyPerformanceValidationCaseData {
        combo_summary: summary.clone(),
        positive_row,
        negative_row,
        layer_mode: match layer_mode {
            ValidationLayerMode::Flat => "flat".to_string(),
            ValidationLayerMode::EachCount => "each_count".to_string(),
            ValidationLayerMode::RecentDistance => "recent_distance".to_string(),
        },
        layer_rows,
        similarity_rows,
    })
}

fn prepare_temp_trade_map(source_conn: &Connection, source_path: &str) -> Result<(), String> {
    source_conn
        .execute_batch(
            r#"
            DROP TABLE IF EXISTS strategy_perf_trade_map;
            CREATE TEMP TABLE strategy_perf_trade_map (
                signal_date VARCHAR,
                horizon INTEGER,
                entry_trade_date VARCHAR,
                exit_trade_date VARCHAR
            );
            "#,
        )
        .map_err(|e| format!("创建临时交易日映射表失败: {e}"))?;

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
            .appender("strategy_perf_trade_map")
            .map_err(|e| format!("创建交易日映射 Appender 失败: {e}"))?;

        for signal_date in rank_dates {
            let Some(&signal_index) = trade_date_index.get(&signal_date) else {
                continue;
            };
            let entry_index = signal_index + 1;
            let Some(entry_trade_date) = trade_dates.get(entry_index) else {
                continue;
            };
            for horizon in HORIZONS {
                let exit_index = signal_index + horizon as usize;
                let Some(exit_trade_date) = trade_dates.get(exit_index) else {
                    continue;
                };
                appender
                    .append_row(params![
                        signal_date,
                        horizon as i64,
                        entry_trade_date,
                        exit_trade_date
                    ])
                    .map_err(|e| format!("写入交易日映射失败: {e}"))?;
            }
        }

        appender
            .flush()
            .map_err(|e| format!("刷新交易日映射 Appender 失败: {e}"))?;
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
            )
            SELECT
                s.ts_code,
                s.signal_date,
                s.total_score,
                s.rank,
                m.horizon,
                m.entry_trade_date,
                TRY_CAST(o.open AS DOUBLE) AS entry_open,
                m.exit_trade_date,
                TRY_CAST(c.close AS DOUBLE) AS exit_close,
                (TRY_CAST(c.close AS DOUBLE) / TRY_CAST(o.open AS DOUBLE) - 1.0) * 100.0 AS future_return_pct
            FROM summary AS s
            INNER JOIN strategy_perf_trade_map AS m
                ON m.signal_date = s.signal_date
            INNER JOIN stock_data AS o
                ON o.ts_code = s.ts_code
               AND o.adj_type = '{DEFAULT_ADJ_TYPE}'
               AND o.trade_date = m.entry_trade_date
            INNER JOIN stock_data AS c
                ON c.ts_code = s.ts_code
               AND c.adj_type = '{DEFAULT_ADJ_TYPE}'
               AND c.trade_date = m.exit_trade_date
            WHERE TRY_CAST(o.open AS DOUBLE) > 0;
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

fn build_future_summary_map(
    future_summaries: &[StrategyPerformanceFutureSummary],
) -> HashMap<u32, StrategyPerformanceFutureSummary> {
    future_summaries
        .iter()
        .cloned()
        .map(|summary| (summary.horizon, summary))
        .collect::<HashMap<_, _>>()
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

fn mean_and_std(values: &[f64]) -> Option<(f64, f64)> {
    if values.is_empty() {
        return None;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    if values.len() < 2 {
        return Some((mean, 0.0));
    }
    let variance = values
        .iter()
        .map(|value| {
            let diff = *value - mean;
            diff * diff
        })
        .sum::<f64>()
        / (values.len() as f64 - 1.0);
    Some((mean, variance.max(0.0).sqrt()))
}

fn load_daily_return_summaries(
    source_conn: &Connection,
) -> Result<HashMap<(u32, String), DailyReturnSummary>, String> {
    let mut stmt = source_conn
        .prepare(
            r#"
            SELECT
                horizon,
                signal_date,
                COUNT(*) AS sample_count,
                AVG(future_return_pct) AS mean_return_pct,
                AVG(future_return_pct * future_return_pct) AS mean_square_return_pct
            FROM strategy_perf_sample_returns
            GROUP BY horizon, signal_date
            "#,
        )
        .map_err(|e| format!("预编译日度收益统计失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询日度收益统计失败: {e}"))?;
    let mut out = HashMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取日度收益统计失败: {e}"))?
    {
        let horizon: i64 = row.get(0).map_err(|e| format!("读取 horizon 失败: {e}"))?;
        let signal_date: String = row
            .get(1)
            .map_err(|e| format!("读取 signal_date 失败: {e}"))?;
        let sample_count = row
            .get::<_, i64>(2)
            .map_err(|e| format!("读取 sample_count 失败: {e}"))?
            .max(0) as u32;
        let mean_return_pct = row
            .get::<_, Option<f64>>(3)
            .map_err(|e| format!("读取 mean_return_pct 失败: {e}"))?
            .unwrap_or(0.0);
        let mean_square_return_pct = row
            .get::<_, Option<f64>>(4)
            .map_err(|e| format!("读取 mean_square_return_pct 失败: {e}"))?
            .unwrap_or(0.0);
        out.insert(
            (horizon as u32, signal_date),
            DailyReturnSummary {
                sample_count,
                mean_return_pct,
                mean_square_return_pct,
            },
        );
    }
    Ok(out)
}

fn load_rule_daily_direction_metrics(
    source_conn: &Connection,
    detail_table_name: &str,
    is_positive: bool,
) -> Result<HashMap<(String, u32, String), RuleDailyPositiveMetric>, String> {
    let sign_filter = if is_positive {
        "d.rule_score > 0"
    } else {
        "d.rule_score < 0"
    };
    let mut stmt = source_conn
        .prepare(&format!(
            r#"
            SELECT
                d.rule_name,
                r.horizon,
                r.signal_date,
                COUNT(*) AS hit_n,
                SUM(r.future_return_pct) AS sum_return_pct
            FROM strategy_perf_sample_returns AS r
            INNER JOIN {detail_table_name} AS d
                ON d.ts_code = r.ts_code
               AND d.trade_date = r.signal_date
               AND {sign_filter}
            GROUP BY d.rule_name, r.horizon, r.signal_date
            "#,
        ))
        .map_err(|e| format!("预编译策略日度方向统计失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询策略日度方向统计失败: {e}"))?;
    let mut out = HashMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取策略日度方向统计失败: {e}"))?
    {
        let rule_name: String = row
            .get(0)
            .map_err(|e| format!("读取 rule_name 失败: {e}"))?;
        let horizon: i64 = row.get(1).map_err(|e| format!("读取 horizon 失败: {e}"))?;
        let signal_date: String = row
            .get(2)
            .map_err(|e| format!("读取 signal_date 失败: {e}"))?;
        let hit_n = row
            .get::<_, i64>(3)
            .map_err(|e| format!("读取 hit_n 失败: {e}"))?
            .max(0) as u32;
        let sum_return_pct = row
            .get::<_, Option<f64>>(4)
            .map_err(|e| format!("读取 sum_return_pct 失败: {e}"))?
            .unwrap_or(0.0);
        out.insert(
            (rule_name, horizon as u32, signal_date),
            RuleDailyPositiveMetric {
                hit_n,
                sum_return_pct,
            },
        );
    }
    Ok(out)
}

fn build_rule_factor_metrics(
    daily_return_summaries: &HashMap<(u32, String), DailyReturnSummary>,
    rule_daily_positive_metrics: &HashMap<(String, u32, String), RuleDailyPositiveMetric>,
) -> HashMap<(String, u32), RuleFactorMetric> {
    let mut grouped = HashMap::<(String, u32), Vec<(String, RuleDailyPositiveMetric)>>::new();
    for ((rule_name, horizon, signal_date), metric) in rule_daily_positive_metrics {
        grouped
            .entry((rule_name.clone(), *horizon))
            .or_default()
            .push((signal_date.clone(), *metric));
    }

    grouped
        .into_par_iter()
        .map(|((rule_name, horizon), daily_metrics)| {
            let mut daily_ics = Vec::new();
            let mut daily_hit_returns = Vec::new();
            for (signal_date, metric) in daily_metrics {
                let Some(summary) = daily_return_summaries.get(&(horizon, signal_date)) else {
                    continue;
                };
                let total_n = summary.sample_count;
                if total_n == 0 || metric.hit_n == 0 || metric.hit_n >= total_n {
                    continue;
                }
                let p = metric.hit_n as f64 / total_n as f64;
                let var_x = p * (1.0 - p);
                if var_x <= 0.0 {
                    continue;
                }
                let mean_y = summary.mean_return_pct;
                let mean_square_y = summary.mean_square_return_pct;
                let var_y = (mean_square_y - mean_y * mean_y).max(0.0);
                if var_y <= 0.0 {
                    continue;
                }
                let mean_xy = metric.sum_return_pct / total_n as f64;
                let covariance = mean_xy - p * mean_y;
                daily_ics.push(covariance / (var_x.sqrt() * var_y.sqrt()));
                daily_hit_returns.push(metric.sum_return_pct / metric.hit_n as f64);
            }

            let rank_ic_mean = mean_and_std(&daily_ics).map(|(mean, _)| mean);
            let icir =
                mean_and_std(&daily_ics).and_then(
                    |(mean, std)| {
                        if std > 0.0 { Some(mean / std) } else { None }
                    },
                );
            let sharpe_ratio = mean_and_std(&daily_hit_returns).and_then(|(mean, std)| {
                if std > 0.0 { Some(mean / std) } else { None }
            });

            (
                (rule_name, horizon),
                RuleFactorMetric {
                    rank_ic_mean,
                    icir,
                    sharpe_ratio,
                },
            )
        })
        .collect()
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

fn passes_negative_effective_filter(
    agg: RuleAggMetric,
    negative_composite_score: Option<f64>,
    score_passes_floor: bool,
    auto_filter: &StrategyPerformanceAutoFilterConfig,
    horizon: u32,
) -> bool {
    agg.hit_n >= min_sample_for_horizon(auto_filter, horizon)
        && score_passes_floor
        && negative_composite_score
            .map(|score| score < 0.0)
            .unwrap_or(false)
}

fn is_negative_effective(
    metrics: &[StrategyPerformanceHorizonMetric],
    selected_horizon: u32,
    auto_filter: &StrategyPerformanceAutoFilterConfig,
    overall_composite_score: Option<f64>,
) -> bool {
    let pass_count = metrics
        .iter()
        .filter(|metric| metric.passes_negative_filter)
        .count() as u32;
    pass_count >= auto_filter.min_pass_horizons
        && metric_for_horizon(metrics, selected_horizon)
            .map(|metric| {
                metric.hit_n >= min_sample_for_horizon(auto_filter, selected_horizon)
                    && metric.ic_passes_floor
                    && metric.passes_negative_filter
            })
            .unwrap_or(false)
        && overall_composite_score
            .map(|score| score < 0.0)
            .unwrap_or(false)
}

fn build_negative_review_notes(
    metrics: &[StrategyPerformanceHorizonMetric],
    selected_horizon: u32,
    selected_market_summary: Option<&StrategyPerformanceFutureSummary>,
    auto_filter: &StrategyPerformanceAutoFilterConfig,
    overall_composite_score: Option<f64>,
) -> Vec<String> {
    let Some(selected_metric) = metric_for_horizon(metrics, selected_horizon) else {
        return vec!["当前周期无样本".to_string()];
    };

    let pass_count = metrics
        .iter()
        .filter(|metric| metric.passes_negative_filter)
        .count() as u32;
    let required_pass_count = auto_filter.min_pass_horizons;
    if is_negative_effective(
        metrics,
        selected_horizon,
        auto_filter,
        overall_composite_score,
    ) {
        return vec![
            "当前周期已转弱".to_string(),
            format!("{pass_count} 个周期方向一致"),
            "负向综合分已转负".to_string(),
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

    if !selected_metric.ic_passes_floor {
        let note = if selected_metric.score_mode == SCORE_MODE_HIT_VS_NON_HIT {
            "缺少可用 Hit vs Non-hit"
        } else {
            "缺少可用 IC"
        };
        notes.push(note.to_string());
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

    if let Some(composite_score) = selected_metric.composite_score {
        if composite_score >= 0.0 {
            notes.push("负向综合分未转负".to_string());
        }
    } else {
        notes.push("负向综合分缺失".to_string());
    }

    match overall_composite_score {
        Some(score) if score >= 0.0 => notes.push("整体负向综合分未转负".to_string()),
        None => notes.push("整体负向综合分缺失".to_string()),
        _ => {}
    }

    if pass_count < required_pass_count {
        notes.push(format!("仅 {pass_count}/{required_pass_count} 个周期转弱"));
    }

    if notes.is_empty() {
        notes.push("当前更接近负向，但稳定性仍待观察".to_string());
    }
    notes
}

fn clamp_score(value: f64, min_value: f64, max_value: f64) -> f64 {
    value.max(min_value).min(max_value)
}

fn uses_hit_vs_non_hit_score_mode(score_mode: &str) -> bool {
    score_mode == SCORE_MODE_HIT_VS_NON_HIT
}

fn score_mode_for_rule(
    scope_way_label: &str,
    scope_windows: u32,
    has_dist_points: bool,
) -> &'static str {
    if has_dist_points || (scope_way_label == "EACH" && scope_windows > 1) {
        SCORE_MODE_IC_IR
    } else {
        SCORE_MODE_HIT_VS_NON_HIT
    }
}

fn rule_score_passes_floor(
    score_mode: &str,
    hit_vs_non_hit_delta_pct: Option<f64>,
    factor_metric: RuleFactorMetric,
) -> bool {
    if uses_hit_vs_non_hit_score_mode(score_mode) {
        hit_vs_non_hit_delta_pct.is_some()
    } else {
        factor_metric.rank_ic_mean.is_some()
    }
}

fn metric_primary_score(metric: &StrategyPerformanceHorizonMetric) -> Option<f64> {
    if uses_hit_vs_non_hit_score_mode(&metric.score_mode) {
        metric.hit_vs_non_hit_delta_pct
    } else {
        metric.rank_ic_mean
    }
}

fn metric_secondary_score(metric: &StrategyPerformanceHorizonMetric) -> Option<f64> {
    if uses_hit_vs_non_hit_score_mode(&metric.score_mode) {
        metric.avg_future_return_pct
    } else {
        metric.icir
    }
}

fn build_positive_composite_score_with_mode(
    score_mode: &str,
    agg: RuleAggMetric,
    market_summary: Option<&StrategyPerformanceFutureSummary>,
    strong_lift: Option<f64>,
    hit_vs_non_hit_delta_pct: Option<f64>,
    factor_metric: RuleFactorMetric,
) -> Option<f64> {
    if uses_hit_vs_non_hit_score_mode(score_mode) {
        return build_positive_composite_score(
            agg,
            market_summary,
            strong_lift,
            hit_vs_non_hit_delta_pct,
            factor_metric,
        );
    }

    let market_avg = market_summary.and_then(|summary| summary.avg_future_return_pct);
    let excess_return_component = match (agg.avg_future_return_pct, market_avg) {
        (Some(rule_avg), Some(base_avg)) => clamp_score((rule_avg - base_avg) / 2.0, -2.0, 3.0),
        _ => 0.0,
    };
    let ic_component = clamp_score(
        factor_metric
            .rank_ic_mean
            .map(|value| value * 100.0)
            .unwrap_or(-1.0),
        -2.0,
        3.0,
    );
    let icir_component = clamp_score(factor_metric.icir.unwrap_or(0.0), -2.0, 3.0);
    let layer_component = clamp_score(hit_vs_non_hit_delta_pct.unwrap_or(0.0) / 2.0, -2.0, 3.0);
    let strong_component = clamp_score((strong_lift.unwrap_or(1.0) - 1.0) * 2.0, -2.0, 3.0);

    Some(
        strong_component * 0.20
            + excess_return_component * 0.25
            + ic_component * 0.20
            + icir_component * 0.20
            + layer_component * 0.15,
    )
}

fn build_negative_composite_score_with_mode(
    score_mode: &str,
    agg: RuleAggMetric,
    market_summary: Option<&StrategyPerformanceFutureSummary>,
    strong_lift: Option<f64>,
    hit_vs_non_hit_delta_pct: Option<f64>,
    factor_metric: RuleFactorMetric,
) -> Option<f64> {
    if uses_hit_vs_non_hit_score_mode(score_mode) {
        return build_negative_composite_score(
            agg,
            market_summary,
            strong_lift,
            hit_vs_non_hit_delta_pct,
            factor_metric,
        );
    }

    let market_avg = market_summary.and_then(|summary| summary.avg_future_return_pct);
    let excess_return_component = match (agg.avg_future_return_pct, market_avg) {
        (Some(rule_avg), Some(base_avg)) => clamp_score((base_avg - rule_avg) / 2.0, -2.0, 3.0),
        _ => 0.0,
    };
    let ic_component = clamp_score(
        factor_metric
            .rank_ic_mean
            .map(|value| -value * 100.0)
            .unwrap_or(-1.0),
        -2.0,
        3.0,
    );
    let icir_component = clamp_score(-factor_metric.icir.unwrap_or(0.0), -2.0, 3.0);
    let layer_component = clamp_score(-hit_vs_non_hit_delta_pct.unwrap_or(0.0) / 2.0, -2.0, 3.0);
    let strong_component = clamp_score((1.0 - strong_lift.unwrap_or(1.0)) * 2.0, -2.0, 3.0);

    Some(
        -(strong_component * 0.20
            + excess_return_component * 0.25
            + ic_component * 0.20
            + icir_component * 0.20
            + layer_component * 0.15),
    )
}

fn build_positive_composite_score(
    agg: RuleAggMetric,
    market_summary: Option<&StrategyPerformanceFutureSummary>,
    strong_lift: Option<f64>,
    hit_vs_non_hit_delta_pct: Option<f64>,
    _factor_metric: RuleFactorMetric,
) -> Option<f64> {
    let market_avg = market_summary.and_then(|summary| summary.avg_future_return_pct);
    let excess_return_component = match (agg.avg_future_return_pct, market_avg) {
        (Some(rule_avg), Some(base_avg)) => clamp_score((rule_avg - base_avg) / 2.0, -2.0, 3.0),
        _ => 0.0,
    };
    let hit_vs_non_hit_component =
        clamp_score(hit_vs_non_hit_delta_pct.unwrap_or(0.0) / 2.0, -2.0, 3.0);
    let strong_component = clamp_score((strong_lift.unwrap_or(1.0) - 1.0) * 2.0, -2.0, 3.0);

    Some(strong_component * 0.25 + excess_return_component * 0.30 + hit_vs_non_hit_component * 0.45)
}

fn build_negative_composite_score(
    agg: RuleAggMetric,
    market_summary: Option<&StrategyPerformanceFutureSummary>,
    strong_lift: Option<f64>,
    hit_vs_non_hit_delta_pct: Option<f64>,
    _factor_metric: RuleFactorMetric,
) -> Option<f64> {
    let market_avg = market_summary.and_then(|summary| summary.avg_future_return_pct);
    let excess_return_component = match (agg.avg_future_return_pct, market_avg) {
        (Some(rule_avg), Some(base_avg)) => clamp_score((base_avg - rule_avg) / 2.0, -2.0, 3.0),
        _ => 0.0,
    };
    let hit_vs_non_hit_component =
        clamp_score(-hit_vs_non_hit_delta_pct.unwrap_or(0.0) / 2.0, -2.0, 3.0);
    let strong_component = clamp_score((1.0 - strong_lift.unwrap_or(1.0)) * 2.0, -2.0, 3.0);

    Some(
        -(strong_component * 0.25
            + excess_return_component * 0.30
            + hit_vs_non_hit_component * 0.45),
    )
}

fn build_rule_rows(
    strategy_options: &[String],
    rule_meta: &HashMap<String, RuleMeta>,
    rule_aggregates: &HashMap<(String, bool, u32), RuleAggMetric>,
    positive_rule_factor_metrics: &HashMap<(String, u32), RuleFactorMetric>,
    negative_rule_factor_metrics: &HashMap<(String, u32), RuleFactorMetric>,
    future_summary_map: &HashMap<u32, StrategyPerformanceFutureSummary>,
    auto_filter: &StrategyPerformanceAutoFilterConfig,
    selected_horizon: u32,
    manual_rule_names: &[String],
    resolved_advantage_rules: &HashSet<String>,
) -> (Vec<StrategyPerformanceRuleRow>, Vec<String>) {
    let manual_set = manual_rule_names.iter().cloned().collect::<HashSet<_>>();
    let per_rule_results = strategy_options
        .par_iter()
        .filter_map(|rule_name| {
            let meta = rule_meta.get(rule_name)?;
            let mut local_rows = Vec::new();
            let mut local_auto_candidate = None;

            for is_positive in [true, false] {
                let mut metrics = Vec::with_capacity(HORIZONS.len());
                let mut any_hit = false;
                let score_mode = score_mode_for_rule(
                    &meta.scope_way_label,
                    meta.scope_windows,
                    meta.has_dist_points,
                );

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
                    let factor_metric = if is_positive {
                        positive_rule_factor_metrics
                    } else {
                        negative_rule_factor_metrics
                    }
                    .get(&(rule_name.clone(), horizon))
                    .copied()
                    .unwrap_or_default();
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
                    let composite_score = if is_positive {
                        build_positive_composite_score_with_mode(
                            score_mode,
                            agg,
                            market_summary,
                            strong_lift,
                            hit_vs_non_hit_delta_pct,
                            factor_metric,
                        )
                    } else {
                        build_negative_composite_score_with_mode(
                            score_mode,
                            agg,
                            market_summary,
                            strong_lift,
                            hit_vs_non_hit_delta_pct,
                            factor_metric,
                        )
                    };
                    let score_passes_floor = rule_score_passes_floor(
                        score_mode,
                        hit_vs_non_hit_delta_pct,
                        factor_metric,
                    );
                    let passes_auto_filter = if is_positive {
                        agg.hit_n >= min_sample_for_horizon(auto_filter, horizon)
                            && score_passes_floor
                            && composite_score.unwrap_or(f64::NEG_INFINITY) > 0.0
                    } else {
                        false
                    };
                    let passes_negative_filter = if is_positive {
                        false
                    } else {
                        passes_negative_effective_filter(
                            agg,
                            composite_score,
                            score_passes_floor,
                            auto_filter,
                            horizon,
                        )
                    };
                    metrics.push(StrategyPerformanceHorizonMetric {
                        horizon,
                        score_mode: score_mode.to_string(),
                        hit_n: agg.hit_n,
                        avg_future_return_pct: agg.avg_future_return_pct,
                        strong_hit_rate: agg.strong_hit_rate,
                        strong_lift,
                        win_rate: agg.win_rate,
                        avg_total_score: agg.avg_total_score,
                        avg_rank: agg.avg_rank,
                        hit_vs_non_hit_delta_pct,
                        rank_ic_mean: factor_metric.rank_ic_mean,
                        icir: factor_metric.icir,
                        sharpe_ratio: factor_metric.sharpe_ratio,
                        layer_return_spread_pct: hit_vs_non_hit_delta_pct,
                        composite_score,
                        ic_passes_floor: score_passes_floor,
                        low_confidence,
                        passes_auto_filter,
                        passes_negative_filter,
                    });
                }

                if !is_positive && !any_hit {
                    continue;
                }

                let overall_composite_score = if is_positive {
                    let values = metrics
                        .iter()
                        .filter_map(|metric| metric.composite_score)
                        .collect::<Vec<_>>();
                    mean_and_std(&values).map(|(mean, _)| mean)
                } else {
                    let values = metrics
                        .iter()
                        .filter_map(|metric| metric.composite_score)
                        .collect::<Vec<_>>();
                    mean_and_std(&values).map(|(mean, _)| mean)
                };
                let avg_rank_ic_mean = if is_positive {
                    let values = metrics
                        .iter()
                        .filter_map(|metric| metric.rank_ic_mean)
                        .collect::<Vec<_>>();
                    mean_and_std(&values).map(|(mean, _)| mean)
                } else {
                    let values = metrics
                        .iter()
                        .filter_map(|metric| metric.rank_ic_mean)
                        .collect::<Vec<_>>();
                    mean_and_std(&values).map(|(mean, _)| mean)
                };
                let auto_candidate = is_positive
                    && overall_composite_score.unwrap_or(f64::NEG_INFINITY) > 0.0
                    && metrics.iter().any(|metric| metric.ic_passes_floor);
                if auto_candidate {
                    local_auto_candidate = Some(rule_name.clone());
                }
                let negative_review_notes = if is_positive {
                    Vec::new()
                } else {
                    build_negative_review_notes(
                        &metrics,
                        selected_horizon,
                        future_summary_map.get(&selected_horizon),
                        auto_filter,
                        overall_composite_score,
                    )
                };
                let negative_effective = if is_positive {
                    None
                } else {
                    Some(is_negative_effective(
                        &metrics,
                        selected_horizon,
                        auto_filter,
                        overall_composite_score,
                    ))
                };
                local_rows.push(StrategyPerformanceRuleRow {
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
                    overall_composite_score,
                    avg_rank_ic_mean,
                    metrics,
                });
            }

            Some((local_rows, local_auto_candidate))
        })
        .collect::<Vec<_>>();

    let mut auto_candidates = Vec::new();
    let mut rows = Vec::new();
    for (local_rows, local_auto_candidate) in per_rule_results {
        rows.extend(local_rows);
        if let Some(rule_name) = local_auto_candidate {
            auto_candidates.push(rule_name);
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
                            right
                                .overall_composite_score
                                .partial_cmp(&left.overall_composite_score)
                                .unwrap_or(Ordering::Equal)
                        })
                        .then_with(|| {
                            metric_for_horizon(&right.metrics, selected_horizon)
                                .and_then(|metric| metric.composite_score)
                                .partial_cmp(
                                    &metric_for_horizon(&left.metrics, selected_horizon)
                                        .and_then(|metric| metric.composite_score),
                                )
                                .unwrap_or(Ordering::Equal)
                        })
                        .then_with(|| {
                            metric_for_horizon(&right.metrics, selected_horizon)
                                .and_then(metric_primary_score)
                                .partial_cmp(
                                    &metric_for_horizon(&left.metrics, selected_horizon)
                                        .and_then(metric_primary_score),
                                )
                                .unwrap_or(Ordering::Equal)
                        })
                        .then_with(|| {
                            metric_for_horizon(&right.metrics, selected_horizon)
                                .and_then(metric_secondary_score)
                                .partial_cmp(
                                    &metric_for_horizon(&left.metrics, selected_horizon)
                                        .and_then(metric_secondary_score),
                                )
                                .unwrap_or(Ordering::Equal)
                        })
                        .then_with(|| {
                            metric_for_horizon(&right.metrics, selected_horizon)
                                .and_then(|metric| metric.strong_lift)
                                .partial_cmp(
                                    &metric_for_horizon(&left.metrics, selected_horizon)
                                        .and_then(|metric| metric.strong_lift),
                                )
                                .unwrap_or(Ordering::Equal)
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

    let positive_row_map = rows
        .iter()
        .filter(|row| row.signal_direction == "positive")
        .map(|row| (row.rule_name.as_str(), row))
        .collect::<HashMap<_, _>>();
    auto_candidates.sort_by(|left, right| {
        let left_row = positive_row_map.get(left.as_str()).copied();
        let right_row = positive_row_map.get(right.as_str()).copied();
        let left_metric = left_row
            .and_then(|row| metric_for_horizon(&row.metrics, selected_horizon))
            .cloned();
        let right_metric = right_row
            .and_then(|row| metric_for_horizon(&row.metrics, selected_horizon))
            .cloned();
        right_row
            .and_then(|row| row.overall_composite_score)
            .partial_cmp(&left_row.and_then(|row| row.overall_composite_score))
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                right_metric
                    .as_ref()
                    .and_then(|metric| metric.composite_score)
                    .partial_cmp(
                        &left_metric
                            .as_ref()
                            .and_then(|metric| metric.composite_score),
                    )
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| {
                right_metric
                    .as_ref()
                    .and_then(metric_primary_score)
                    .partial_cmp(&left_metric.as_ref().and_then(metric_primary_score))
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| {
                right_metric
                    .as_ref()
                    .and_then(metric_secondary_score)
                    .partial_cmp(&left_metric.as_ref().and_then(metric_secondary_score))
                    .unwrap_or(Ordering::Equal)
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

fn compare_option_f64_desc(left: Option<f64>, right: Option<f64>) -> Ordering {
    right.partial_cmp(&left).unwrap_or(Ordering::Equal)
}

fn compare_option_f64_asc(left: Option<f64>, right: Option<f64>) -> Ordering {
    left.partial_cmp(&right).unwrap_or(Ordering::Equal)
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

fn spearman_corr_refs(samples: &[&ScoreObservation], use_abs_score: bool) -> Option<f64> {
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

fn build_daily_spearman_ics(samples: &[ScoreObservation]) -> Vec<f64> {
    let mut grouped = BTreeMap::<&str, Vec<&ScoreObservation>>::new();
    for sample in samples {
        grouped
            .entry(sample.signal_date.as_str())
            .or_default()
            .push(sample);
    }
    grouped
        .values()
        .filter_map(|date_samples| spearman_corr_refs(date_samples, false))
        .collect()
}

fn compute_rank_ic_with_fallback(
    samples: &[ScoreObservation],
    sample_count: u32,
    min_sample_count: u32,
) -> (Option<f64>, Option<f64>) {
    if sample_count < min_sample_count {
        return (None, None);
    }

    let daily_ics = build_daily_spearman_ics(samples);
    let rank_ic_mean = mean_and_std(&daily_ics)
        .map(|(mean, _)| mean)
        .or_else(|| spearman_corr(samples, false));
    let icir = mean_and_std(&daily_ics)
        .and_then(|(mean, std)| if std > 0.0 { Some(mean / std) } else { None });

    (rank_ic_mean, icir)
}

fn build_score_bucket_rows(
    samples: &[ScoreObservation],
    strong_threshold_pct: Option<f64>,
) -> (String, Vec<StrategyPerformanceScoreBucketRow>) {
    if samples.is_empty() {
        return ("none".to_string(), Vec::new());
    }
    let mut sorted = samples.iter().collect::<Vec<_>>();
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
        let mut rows = Vec::new();
        let mut start = 0usize;
        while start < sorted.len() {
            let score = sorted[start].score;
            let mut end = start + 1;
            while end < sorted.len()
                && sorted[end]
                    .score
                    .partial_cmp(&score)
                    .unwrap_or(Ordering::Equal)
                    == Ordering::Equal
            {
                end += 1;
            }

            let group = &sorted[start..end];
            rows.push(StrategyPerformanceScoreBucketRow {
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
            });

            start = end;
        }
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

fn load_strategy_score_observations(
    source_conn: &Connection,
    selected_horizon: u32,
) -> Result<Vec<ScoreObservation>, String> {
    let mut stmt = source_conn
        .prepare(
            r#"
            SELECT
                signal_date,
                total_score,
                future_return_pct
            FROM strategy_perf_sample_returns
            WHERE horizon = ?
              AND total_score IS NOT NULL
              AND future_return_pct IS NOT NULL
            ORDER BY signal_date ASC, ts_code ASC
            "#,
        )
        .map_err(|e| format!("预编译策略整体得分分层读取失败: {e}"))?;
    let mut rows = stmt
        .query(params![selected_horizon as i64])
        .map_err(|e| format!("查询策略整体得分分层失败: {e}"))?;
    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取策略整体得分分层失败: {e}"))?
    {
        out.push(ScoreObservation {
            signal_date: row
                .get(0)
                .map_err(|e| format!("读取 signal_date 失败: {e}"))?,
            score: row
                .get::<_, Option<f64>>(1)
                .map_err(|e| format!("读取 total_score 失败: {e}"))?
                .unwrap_or(0.0),
            future_return_pct: row
                .get::<_, Option<f64>>(2)
                .map_err(|e| format!("读取 future_return_pct 失败: {e}"))?
                .unwrap_or(0.0),
        });
    }
    Ok(out)
}

fn load_advantage_score_observations(
    source_conn: &Connection,
) -> Result<Vec<ScoreObservation>, String> {
    let mut stmt = source_conn
        .prepare(
            r#"
            SELECT
                signal_date,
                adv_score_sum,
                future_return_pct
            FROM strategy_perf_sample_features
            WHERE adv_score_sum IS NOT NULL
            ORDER BY signal_date ASC, ts_code ASC
            "#,
        )
        .map_err(|e| format!("预编译优势集分层读取失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询优势集分层失败: {e}"))?;
    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取优势集分层失败: {e}"))?
    {
        out.push(ScoreObservation {
            signal_date: row
                .get(0)
                .map_err(|e| format!("读取 signal_date 失败: {e}"))?,
            score: row
                .get::<_, Option<f64>>(1)
                .map_err(|e| format!("读取 adv_score_sum 失败: {e}"))?
                .unwrap_or(0.0),
            future_return_pct: row
                .get::<_, Option<f64>>(2)
                .map_err(|e| format!("读取 future_return_pct 失败: {e}"))?
                .unwrap_or(0.0),
        });
    }
    Ok(out)
}

fn build_score_analysis_from_samples(
    samples: &[ScoreObservation],
    selected_horizon: u32,
    future_summary_map: &HashMap<u32, StrategyPerformanceFutureSummary>,
) -> Option<StrategyPerformanceOverallScoreAnalysis> {
    let future_summary = future_summary_map.get(&selected_horizon)?;
    if samples.is_empty() {
        return None;
    }

    let sample_count = samples.len() as u32;
    let avg_future_return_pct = Some(
        samples
            .iter()
            .map(|item| item.future_return_pct)
            .sum::<f64>()
            / sample_count as f64,
    );
    let strong_hit_rate = future_summary.strong_threshold_pct.map(|threshold| {
        samples
            .iter()
            .filter(|item| item.future_return_pct >= threshold)
            .count() as f64
            / sample_count as f64
    });
    let win_rate = Some(
        samples
            .iter()
            .filter(|item| item.future_return_pct > 0.0)
            .count() as f64
            / sample_count as f64,
    );
    let (bucket_mode, score_rows) =
        build_score_bucket_rows(samples, future_summary.strong_threshold_pct);
    let layer_return_spread_pct = match (score_rows.first(), score_rows.last()) {
        (Some(first), Some(last)) if score_rows.len() >= 2 => {
            match (first.avg_future_return_pct, last.avg_future_return_pct) {
                (Some(low), Some(high)) => Some(high - low),
                _ => None,
            }
        }
        _ => None,
    };

    let (rank_ic_mean, icir) =
        compute_rank_ic_with_fallback(samples, sample_count, PORTFOLIO_MIN_SAMPLE_COUNT);

    Some(StrategyPerformanceOverallScoreAnalysis {
        horizon: selected_horizon,
        sample_count,
        avg_future_return_pct,
        strong_hit_rate,
        win_rate,
        spearman_corr: spearman_corr(samples, false),
        rank_ic_mean,
        icir,
        layer_return_spread_pct,
        bucket_mode,
        score_rows,
    })
}

fn build_overall_score_analysis(
    source_conn: &Connection,
    selected_horizon: u32,
    future_summary_map: &HashMap<u32, StrategyPerformanceFutureSummary>,
) -> Result<Option<StrategyPerformanceOverallScoreAnalysis>, String> {
    let samples = load_strategy_score_observations(source_conn, selected_horizon)?;
    Ok(build_score_analysis_from_samples(
        &samples,
        selected_horizon,
        future_summary_map,
    ))
}

fn build_advantage_score_analysis(
    source_conn: &Connection,
    selected_horizon: u32,
    future_summary_map: &HashMap<u32, StrategyPerformanceFutureSummary>,
) -> Result<Option<StrategyPerformanceOverallScoreAnalysis>, String> {
    let samples = load_advantage_score_observations(source_conn)?;
    Ok(build_score_analysis_from_samples(
        &samples,
        selected_horizon,
        future_summary_map,
    ))
}

fn build_hit_count_rows(
    samples: &[&ScoreObservation],
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
) -> Result<Vec<ScoreObservation>, String> {
    let mut stmt = source_conn
        .prepare(&format!(
            r#"
            SELECT
                r.signal_date,
                COALESCE(d.rule_score, 0) AS rule_score,
                r.future_return_pct
            FROM strategy_perf_sample_returns AS r
            LEFT JOIN {detail_table_name} AS d
                ON d.ts_code = r.ts_code
               AND d.trade_date = r.signal_date
               AND d.rule_name = ?
            WHERE r.horizon = ?
            ORDER BY COALESCE(d.rule_score, 0) ASC, r.future_return_pct ASC
            "#,
        ))
        .map_err(|e| format!("预编译单策略明细失败: {e}"))?;
    let mut rows = stmt
        .query(params![rule_name, selected_horizon as i64])
        .map_err(|e| format!("查询单策略明细失败: {e}"))?;
    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取单策略明细失败: {e}"))?
    {
        out.push(ScoreObservation {
            signal_date: row
                .get(0)
                .map_err(|e| format!("读取 signal_date 失败: {e}"))?,
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
    let samples = load_rule_detail_observations(
        source_conn,
        detail_table_name,
        selected_horizon,
        selected_rule_name,
    )?;
    if samples.is_empty() {
        return Ok(None);
    }

    let positive_samples = samples
        .iter()
        .filter(|sample| sample.score > 0.0)
        .collect::<Vec<_>>();
    let avg_future_return_pct = if positive_samples.is_empty() {
        None
    } else {
        Some(
            positive_samples
                .iter()
                .map(|item| item.future_return_pct)
                .sum::<f64>()
                / positive_samples.len() as f64,
        )
    };
    let strong_hit_rate = if positive_samples.is_empty() {
        None
    } else {
        future_summary.strong_threshold_pct.map(|threshold| {
            positive_samples
                .iter()
                .filter(|item| item.future_return_pct >= threshold)
                .count() as f64
                / positive_samples.len() as f64
        })
    };
    let win_rate = if positive_samples.is_empty() {
        None
    } else {
        Some(
            positive_samples
                .iter()
                .filter(|item| item.future_return_pct > 0.0)
                .count() as f64
                / positive_samples.len() as f64,
        )
    };
    let non_hit_return_pct = non_hit_avg(
        future_summary.sample_count,
        future_summary.avg_future_return_pct,
        positive_samples.len() as u32,
        avg_future_return_pct,
    );
    let (bucket_mode, score_rows) =
        build_score_bucket_rows(&samples, future_summary.strong_threshold_pct);
    let extreme_score_minus_mild_score_pct = match (score_rows.first(), score_rows.last()) {
        (Some(first), Some(last)) if score_rows.len() >= 2 => {
            match (last.avg_future_return_pct, first.avg_future_return_pct) {
                (Some(extreme), Some(mild)) => Some(extreme - mild),
                _ => None,
            }
        }
        _ => None,
    };
    let hit_count_rows = if meta.scope_way_label == "EACH" && !meta.has_dist_points {
        build_hit_count_rows(
            &positive_samples,
            meta.points,
            future_summary.strong_threshold_pct,
        )
    } else {
        Vec::new()
    };

    let (rank_ic_mean, icir) = compute_rank_ic_with_fallback(&samples, samples.len() as u32, 2);
    let mut daily_hit_returns = Vec::new();
    let mut grouped = BTreeMap::<&str, Vec<&ScoreObservation>>::new();
    for sample in &samples {
        grouped
            .entry(sample.signal_date.as_str())
            .or_default()
            .push(sample);
    }
    for date_samples in grouped.values() {
        let total_n = date_samples.len() as u32;
        let hit_n = date_samples
            .iter()
            .filter(|sample| sample.score > 0.0)
            .count() as u32;
        if hit_n == 0 || hit_n >= total_n {
            continue;
        }
        let mean_square_y = date_samples
            .iter()
            .map(|sample| sample.future_return_pct * sample.future_return_pct)
            .sum::<f64>()
            / total_n as f64;
        let mean_y = date_samples
            .iter()
            .map(|sample| sample.future_return_pct)
            .sum::<f64>()
            / total_n as f64;
        let var_y = (mean_square_y - mean_y * mean_y).max(0.0);
        if var_y <= 0.0 {
            continue;
        }
        let hit_sum = date_samples
            .iter()
            .filter(|sample| sample.score > 0.0)
            .map(|sample| sample.future_return_pct)
            .sum::<f64>();
        daily_hit_returns.push(hit_sum / hit_n as f64);
    }
    let sharpe_ratio = mean_and_std(&daily_hit_returns)
        .and_then(|(mean, std)| if std > 0.0 { Some(mean / std) } else { None });
    let directions = vec![StrategyPerformanceRuleDirectionDetail {
        signal_direction: "factor".to_string(),
        direction_label: "全样本分层".to_string(),
        score_mode: score_mode_for_rule(
            &meta.scope_way_label,
            meta.scope_windows,
            meta.has_dist_points,
        )
        .to_string(),
        bucket_mode,
        sample_count: samples.len() as u32,
        avg_future_return_pct,
        strong_hit_rate,
        win_rate,
        spearman_corr: spearman_corr(&samples, false),
        abs_spearman_corr: spearman_corr(&samples, true),
        rank_ic_mean,
        icir,
        sharpe_ratio,
        hit_vs_non_hit_delta_pct: match (avg_future_return_pct, non_hit_return_pct) {
            (Some(hit_avg), Some(non_hit_avg)) => Some(hit_avg - non_hit_avg),
            _ => None,
        },
        extreme_score_minus_mild_score_pct,
        has_dist_points: meta.has_dist_points,
        score_rows,
        hit_count_rows,
    }];

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
            description: "对每条规则分别统计正向命中(rule_score>0)与负向命中(rule_score<0)。单策略默认按 Hit vs Non-hit 模式评分，更关注“触发后相对未触发是否更强”；自动优势策略默认先要求：命中样本不太少、未来平均收益高于市场、触发相对未触发有正增益；再按当前持有周期下的综合分排序，只取前10条作为自动优势策略。伴随集定义为当前样本期内有正向命中、但不在优势集中的其他规则。".to_string(),
        },
        StrategyPerformanceMethodNote {
            key: "negative_rule".to_string(),
            title: "负向方向判定".to_string(),
            description: "负向规则同样走单策略 Hit vs Non-hit 评分，但方向取镜像：rule_score<0 的命中样本会单独计算 strong_lift / avg_future_return / hit_vs_non_hit，并汇总成负向综合分。若当前周期综合分已转负、单策略评分可用、样本数达标，且达到“至少通过几个持有周期”，则归入“方向明确负向”；否则归入“待验证负向”，并显示未通过原因。".to_string(),
        },
        StrategyPerformanceMethodNote {
            key: "companion".to_string(),
            title: "伴随策略分析".to_string(),
            description: "先定义优势策略集 advantage_rules，再在 adv_hit_cnt >= min_adv_hits 的优势样本池内统计其他正向伴随策略。delta_return = companion_avg_return - eligible_pool_avg_return；delta_win = companion_win_rate - eligible_pool_win_rate。".to_string(),
        },
        StrategyPerformanceMethodNote {
            key: "score_strength".to_string(),
            title: "得分强度分析".to_string(),
            description: "单策略默认按 Hit vs Non-hit 模式解读，优先看触发后相对未触发的收益差；明细仍按 rule_score 精确值或分位桶统计 sample_count / avg_future_return / strong_hit_rate / win_rate，并给出 corr(rule_score, future_return) 与 corr(abs(rule_score), future_return) 的 Spearman 相关作为参考。对 EACH 且非 dist_points 规则，额外估算 hit_count = |rule_score| / |points| 的命中次数分层。".to_string(),
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
    prepare_temp_trade_map(&source_conn, &source_path)?;
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
    draft: StrategyPerformanceValidationDraft,
) -> Result<StrategyPerformanceValidationPageData, String> {
    let strategy_direction = draft.strategy_direction;
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
    prepare_temp_trade_map(&source_conn, &source_path)?;
    prepare_temp_sample_returns(&source_conn)?;
    prepare_temp_thresholds(&source_conn, strong_quantile)?;
    let future_summaries = load_future_summaries(&source_conn, strong_quantile)?;
    if future_summaries
        .iter()
        .all(|summary| summary.sample_count == 0)
    {
        return Err("没有可用未来收益样本".to_string());
    }
    let future_summary_map = build_future_summary_map(&future_summaries);
    let variants = prepare_temp_validation_variant_details(&source_conn, &source_path, &draft)?;
    let rule_meta = build_validation_rule_meta(&variants);
    let rule_aggregates = load_rule_aggregates(&source_conn, VALIDATION_DETAILS_TABLE)?;
    let daily_return_summaries = load_daily_return_summaries(&source_conn)?;
    let positive_rule_daily_metrics =
        load_rule_daily_direction_metrics(&source_conn, VALIDATION_DETAILS_TABLE, true)?;
    let negative_rule_daily_metrics =
        load_rule_daily_direction_metrics(&source_conn, VALIDATION_DETAILS_TABLE, false)?;
    let positive_rule_factor_metrics =
        build_rule_factor_metrics(&daily_return_summaries, &positive_rule_daily_metrics);
    let negative_rule_factor_metrics =
        build_rule_factor_metrics(&daily_return_summaries, &negative_rule_daily_metrics);
    let strategy_options = variants
        .iter()
        .map(|variant| variant.combo_key.clone())
        .collect::<Vec<_>>();
    let rule_rows = build_validation_rule_rows(
        &strategy_options,
        &rule_meta,
        &rule_aggregates,
        &positive_rule_factor_metrics,
        &negative_rule_factor_metrics,
        &future_summary_map,
        &auto_filter,
        selected_horizon,
    );
    let combo_summaries =
        build_validation_combo_summaries(&variants, &rule_rows, &source_conn, selected_horizon)?;
    let positive_map = rule_rows
        .iter()
        .filter(|row| row.signal_direction == "positive")
        .map(|row| (row.rule_name.clone(), row.clone()))
        .collect::<HashMap<_, _>>();
    let negative_map = rule_rows
        .iter()
        .filter(|row| row.signal_direction == "negative")
        .map(|row| (row.rule_name.clone(), row.clone()))
        .collect::<HashMap<_, _>>();
    let existing_rule_meta = load_rule_meta(&source_path)
        .map(|(_, meta)| meta)
        .unwrap_or_default();

    let best_positive_summary =
        select_best_validation_summary(&combo_summaries, ValidationStrategyDirection::Positive);
    let best_negative_summary =
        select_best_validation_summary(&combo_summaries, ValidationStrategyDirection::Negative);

    let best_positive_case = best_positive_summary
        .as_ref()
        .and_then(|summary| {
            variants
                .iter()
                .find(|variant| variant.combo_key == summary.combo_key)
                .map(|variant| (summary, variant.layer_mode))
        })
        .map(|(summary, layer_mode)| {
            build_validation_case_data(
                &source_path,
                &source_conn,
                selected_horizon,
                strong_quantile,
                summary,
                positive_map.get(&summary.combo_key).cloned(),
                negative_map.get(&summary.combo_key).cloned(),
                layer_mode,
                &future_summary_map,
                &existing_rule_meta,
            )
        })
        .transpose()?;
    let best_negative_case = best_negative_summary
        .as_ref()
        .and_then(|summary| {
            variants
                .iter()
                .find(|variant| variant.combo_key == summary.combo_key)
                .map(|variant| (summary, variant.layer_mode))
        })
        .map(|(summary, layer_mode)| {
            build_validation_case_data(
                &source_path,
                &source_conn,
                selected_horizon,
                strong_quantile,
                summary,
                positive_map.get(&summary.combo_key).cloned(),
                negative_map.get(&summary.combo_key).cloned(),
                layer_mode,
                &future_summary_map,
                &existing_rule_meta,
            )
        })
        .transpose()?;

    Ok(StrategyPerformanceValidationPageData {
        strategy_direction,
        horizons: HORIZONS.to_vec(),
        selected_horizon,
        strong_quantile,
        future_summaries,
        combo_summaries,
        best_positive_case,
        best_negative_case,
        methods: build_method_notes(),
    })
}

#[allow(clippy::too_many_arguments)]
pub fn get_or_build_strategy_pick_cache(
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
) -> Result<StrategyPerformancePickCachePayload, String> {
    let selected_horizon = normalize_selected_horizon(selected_horizon);
    let strong_quantile = normalize_strong_quantile(strong_quantile)?;
    let advantage_rule_mode = normalize_advantage_mode(advantage_rule_mode);
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
    let min_adv_hits = min_adv_hits.unwrap_or(DEFAULT_MIN_ADV_HITS).max(1);
    let (strategy_options, _) = load_rule_meta(&source_path)?;
    let (manual_rule_names, _) = normalize_manual_rule_names(manual_rule_names, &strategy_options);
    let cache_key = build_strategy_pick_cache_key(
        selected_horizon,
        strong_quantile,
        advantage_rule_mode,
        &manual_rule_names,
        &auto_filter,
        min_adv_hits,
    );
    if let Some(payload) = load_strategy_pick_cache(&source_path, &cache_key)? {
        return Ok(payload);
    }

    let page = get_strategy_performance_page(
        source_path.clone(),
        Some(selected_horizon),
        Some(strong_quantile),
        Some(advantage_mode_label(advantage_rule_mode)),
        Some(manual_rule_names),
        Some(auto_filter.min_samples_2),
        Some(auto_filter.min_samples_3),
        Some(auto_filter.min_samples_5),
        Some(auto_filter.min_samples_10),
        Some(auto_filter.require_win_rate_above_market),
        Some(auto_filter.min_pass_horizons),
        Some(min_adv_hits),
        None,
        None,
        None,
    )?;

    Ok(build_strategy_pick_cache_payload(
        page.selected_horizon,
        strong_quantile,
        &page.resolved_advantage_mode,
        &page.auto_advantage_rule_names,
        &page.manual_advantage_rule_names,
        &page.resolved_advantage_rule_names,
        &page.noisy_companion_rule_names,
    ))
}

pub fn get_strategy_pick_cache(
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
) -> Result<StrategyPerformancePickCachePayload, String> {
    let selected_horizon = normalize_selected_horizon(selected_horizon);
    let strong_quantile = normalize_strong_quantile(strong_quantile)?;
    let advantage_rule_mode = normalize_advantage_mode(advantage_rule_mode);
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
    let min_adv_hits = min_adv_hits.unwrap_or(DEFAULT_MIN_ADV_HITS).max(1);
    let (strategy_options, _) = load_rule_meta(&source_path)?;
    let (manual_rule_names, _) = normalize_manual_rule_names(manual_rule_names, &strategy_options);
    let cache_key = build_strategy_pick_cache_key(
        selected_horizon,
        strong_quantile,
        advantage_rule_mode,
        &manual_rule_names,
        &auto_filter,
        min_adv_hits,
    );
    load_strategy_pick_cache(&source_path, &cache_key)?.ok_or_else(|| {
        format!(
            "未找到对应的策略回测缓存。请先到策略回测页按同样参数运行一次回测后，再来高级选股。当前周期={}日",
            selected_horizon
        )
    })
}

pub fn get_latest_strategy_pick_cache(
    source_path: String,
) -> Result<StrategyPerformancePickCachePayload, String> {
    load_latest_strategy_pick_cache(&source_path)?.ok_or_else(|| {
        "未找到策略回测缓存。请先到策略回测页运行一次回测后，再来高级选股。".to_string()
    })
}

fn build_strategy_performance_horizon_view_with_future_summary_map(
    source_conn: &Connection,
    selected_horizon: u32,
    auto_filter: &StrategyPerformanceAutoFilterConfig,
    min_adv_hits: u32,
    resolved_advantage_rule_names: &[String],
    requested_noisy_companion_rule_names: &[String],
    future_summary_map: &HashMap<u32, StrategyPerformanceFutureSummary>,
) -> Result<StrategyPerformanceHorizonViewData, String> {
    prepare_temp_string_table(
        source_conn,
        "strategy_perf_advantage_rules",
        resolved_advantage_rule_names,
    )?;
    prepare_temp_string_table(source_conn, "strategy_perf_noisy_rules", &Vec::new())?;
    rebuild_temp_sample_features(source_conn, selected_horizon)?;

    let companion_rows = load_companion_rows(
        source_conn,
        min_adv_hits,
        min_sample_for_horizon(auto_filter, selected_horizon),
    )?;
    let noisy_companion_rule_names = if requested_noisy_companion_rule_names.is_empty() {
        companion_rows
            .iter()
            .filter(|row| row.delta_return_pct.unwrap_or(0.0) < 0.0)
            .map(|row| row.rule_name.clone())
            .collect::<Vec<_>>()
    } else {
        requested_noisy_companion_rule_names.to_vec()
    };
    let overall_score_analysis =
        build_overall_score_analysis(source_conn, selected_horizon, &future_summary_map)?;
    let advantage_score_analysis =
        build_advantage_score_analysis(source_conn, selected_horizon, &future_summary_map)?;

    Ok(StrategyPerformanceHorizonViewData {
        selected_horizon: selected_horizon,
        noisy_companion_rule_names,
        companion_rows,
        overall_score_analysis,
        advantage_score_analysis,
    })
}

fn build_strategy_performance_horizon_view(
    source_conn: &Connection,
    selected_horizon: u32,
    strong_quantile: f64,
    auto_filter: &StrategyPerformanceAutoFilterConfig,
    min_adv_hits: u32,
    resolved_advantage_rule_names: &[String],
    requested_noisy_companion_rule_names: &[String],
) -> Result<StrategyPerformanceHorizonViewData, String> {
    prepare_temp_thresholds(source_conn, strong_quantile)?;
    let future_summaries = load_future_summaries(source_conn, strong_quantile)?;
    let future_summary_map = build_future_summary_map(&future_summaries);
    build_strategy_performance_horizon_view_with_future_summary_map(
        source_conn,
        selected_horizon,
        auto_filter,
        min_adv_hits,
        resolved_advantage_rule_names,
        requested_noisy_companion_rule_names,
        &future_summary_map,
    )
}

pub fn get_strategy_performance_horizon_view(
    source_path: String,
    selected_horizon: Option<u32>,
    strong_quantile: Option<f64>,
    resolved_advantage_rule_names: Vec<String>,
    auto_min_samples_2: Option<u32>,
    auto_min_samples_3: Option<u32>,
    auto_min_samples_5: Option<u32>,
    auto_min_samples_10: Option<u32>,
    require_win_rate_above_market: Option<bool>,
    min_pass_horizons: Option<u32>,
    min_adv_hits: Option<u32>,
    noisy_companion_rule_names: Option<Vec<String>>,
) -> Result<StrategyPerformanceHorizonViewData, String> {
    let selected_horizon = normalize_selected_horizon(selected_horizon);
    let strong_quantile = normalize_strong_quantile(strong_quantile)?;
    let auto_filter = StrategyPerformanceAutoFilterConfig {
        min_samples_2: auto_min_samples_2.unwrap_or(DEFAULT_MIN_SAMPLE),
        min_samples_3: auto_min_samples_3.unwrap_or(DEFAULT_MIN_SAMPLE),
        min_samples_5: auto_min_samples_5.unwrap_or(DEFAULT_MIN_SAMPLE),
        min_samples_10: auto_min_samples_10.unwrap_or(DEFAULT_MIN_SAMPLE),
        require_win_rate_above_market: require_win_rate_above_market.unwrap_or(false),
        min_pass_horizons: min_pass_horizons.unwrap_or(DEFAULT_MIN_PASS_HORIZONS),
    };
    let min_adv_hits = min_adv_hits.unwrap_or(DEFAULT_MIN_ADV_HITS).max(1);
    let strategy_options = load_rule_meta(&source_path)?.0;
    let resolved_advantage_rule_names =
        normalize_manual_rule_names(Some(resolved_advantage_rule_names), &strategy_options).0;
    let requested_noisy_companion_rule_names = noisy_companion_rule_names.unwrap_or_default();

    let source_conn = open_source_conn(&source_path)?;
    attach_result_db(&source_conn, &source_path)?;
    prepare_temp_trade_map(&source_conn, &source_path)?;
    prepare_temp_sample_returns(&source_conn)?;

    build_strategy_performance_horizon_view(
        &source_conn,
        selected_horizon,
        strong_quantile,
        &auto_filter,
        min_adv_hits,
        &resolved_advantage_rule_names,
        &requested_noisy_companion_rule_names,
    )
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
    let advantage_rule_mode = normalize_advantage_mode(advantage_rule_mode);

    let source_conn = open_source_conn(&source_path)?;
    attach_result_db(&source_conn, &source_path)?;
    prepare_temp_trade_map(&source_conn, &source_path)?;
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
    let daily_return_summaries = load_daily_return_summaries(&source_conn)?;
    let positive_rule_daily_metrics =
        load_rule_daily_direction_metrics(&source_conn, RESULT_DETAILS_TABLE, true)?;
    let negative_rule_daily_metrics =
        load_rule_daily_direction_metrics(&source_conn, RESULT_DETAILS_TABLE, false)?;
    let positive_rule_factor_metrics =
        build_rule_factor_metrics(&daily_return_summaries, &positive_rule_daily_metrics);
    let negative_rule_factor_metrics =
        build_rule_factor_metrics(&daily_return_summaries, &negative_rule_daily_metrics);
    let (mut rule_rows, auto_candidate_rule_names_initial) = build_rule_rows(
        &strategy_options,
        &rule_meta,
        &rule_aggregates,
        &positive_rule_factor_metrics,
        &negative_rule_factor_metrics,
        &future_summary_map,
        &auto_filter,
        selected_horizon,
        &manual_rule_names,
        &HashSet::new(),
    );
    let auto_candidate_rule_names_final = auto_candidate_rule_names_initial.clone();

    let resolved_advantage_rule_names = resolve_advantage_rule_names(
        &strategy_options,
        advantage_rule_mode,
        &auto_candidate_rule_names_final,
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

    let horizon_view = build_strategy_performance_horizon_view_with_future_summary_map(
        &source_conn,
        selected_horizon,
        &auto_filter,
        min_adv_hits,
        &resolved_advantage_rule_names,
        &requested_noisy_companion_rule_names,
        &future_summary_map,
    )?;
    let pick_cache_payload = build_strategy_pick_cache_payload(
        selected_horizon,
        strong_quantile,
        &advantage_mode_label(advantage_rule_mode),
        &auto_candidate_rule_names_final,
        &manual_rule_names,
        &resolved_advantage_rule_names,
        &horizon_view.noisy_companion_rule_names,
    );
    let pick_cache_key = build_strategy_pick_cache_key(
        selected_horizon,
        strong_quantile,
        advantage_rule_mode,
        &manual_rule_names,
        &auto_filter,
        min_adv_hits,
    );
    save_strategy_pick_cache(&source_path, &pick_cache_key, &pick_cache_payload)?;

    let selected_rule_name = resolve_selected_rule_name(
        selected_rule_name,
        &resolved_advantage_rule_names,
        &auto_candidate_rule_names_final,
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
    let StrategyPerformanceHorizonViewData {
        noisy_companion_rule_names,
        companion_rows,
        overall_score_analysis,
        advantage_score_analysis: _,
        ..
    } = horizon_view;

    Ok(StrategyPerformancePageData {
        horizons: HORIZONS.to_vec(),
        selected_horizon,
        strong_quantile,
        strategy_options,
        future_summaries,
        auto_filter,
        resolved_advantage_mode: advantage_mode_label(advantage_rule_mode),
        auto_advantage_rule_names: auto_candidate_rule_names_final.clone(),
        manual_advantage_rule_names: manual_rule_names.clone(),
        auto_candidate_rule_names: auto_candidate_rule_names_final,
        manual_rule_names,
        ignored_manual_rule_names,
        resolved_advantage_rule_names,
        resolved_companion_rule_names,
        effective_negative_rule_names,
        ineffective_negative_rule_names,
        min_adv_hits,
        top_limit,
        noisy_companion_rule_names,
        rule_rows,
        companion_rows,
        overall_score_analysis,
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

    fn assert_close(left: f64, right: f64, epsilon: f64) {
        assert!(
            (left - right).abs() <= epsilon,
            "left={left}, right={right}, epsilon={epsilon}"
        );
    }

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
                close DOUBLE,
                pre_close DOUBLE,
                change DOUBLE,
                pct_chg DOUBLE,
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
                10.0,
                0.1,
                1.0,
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
                10.1,
                -0.1,
                -0.9900990099009901,
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
                10.0,
                0.2,
                2.0,
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
                10.2,
                0.1,
                0.9803921568627451,
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
                10.3,
                0.1,
                0.9708737864077669,
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
                10.4,
                0.1,
                0.9615384615384615,
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
                10.5,
                0.7,
                6.666666666666667,
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
                11.2,
                0.4,
                3.5714285714285716,
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
                11.6,
                0.3,
                2.586206896551724,
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
                11.9,
                0.3,
                2.521008403361345,
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
                12.2,
                0.4,
                3.278688524590164,
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
                12.6,
                0.2,
                1.5873015873015872,
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
                12.8,
                0.2,
                1.5625,
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
                10.0,
                -0.1,
                -1.0,
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
                9.9,
                0.1,
                1.0101010101010102,
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
                10.0,
                -0.1,
                -1.0,
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
                9.9,
                -0.1,
                -1.0101010101010102,
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
                9.8,
                -0.1,
                -1.0204081632653061,
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
                9.7,
                -0.1,
                -1.0309278350515463,
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
                9.6,
                -0.5,
                -5.208333333333333,
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
                9.1,
                -0.1,
                -1.098901098901099,
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
                9.0,
                -0.2,
                -2.2222222222222223,
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
                8.8,
                -0.2,
                -2.272727272727273,
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
                8.6,
                -0.2,
                -2.3255813953488373,
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
                8.4,
                -0.2,
                -2.380952380952381,
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
                8.2,
                -0.1,
                -1.2195121951219512,
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
                10.0,
                0.0,
                0.0,
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
                10.0,
                0.0,
                0.0,
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
                10.0,
                0.0,
                0.0,
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
                10.0,
                0.0,
                0.0,
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
                10.0,
                0.0,
                0.0,
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
                10.0,
                0.0,
                0.0,
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
                10.0,
                0.2,
                2.0,
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
                10.2,
                -0.1,
                -0.9803921568627451,
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
                10.1,
                -0.1,
                -0.9900990099009901,
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
                10.0,
                0.1,
                1.0,
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
                10.1,
                -0.1,
                -0.9900990099009901,
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
                10.0,
                0.1,
                1.0,
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
                10.1,
                -0.1,
                -0.9900990099009901,
                1000.0,
                10000.0,
            ),
        ];
        for (
            ts_code,
            trade_date,
            adj_type,
            open,
            high,
            low,
            close,
            pre_close,
            change,
            pct_chg,
            vol,
            amount,
        ) in rows
        {
            conn.execute(
                "INSERT INTO stock_data (ts_code, trade_date, adj_type, open, high, low, close, pre_close, change, pct_chg, vol, amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    ts_code,
                    trade_date,
                    adj_type,
                    open,
                    high,
                    low,
                    close,
                    pre_close,
                    change,
                    pct_chg,
                    vol,
                    amount
                ],
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
    fn positive_composite_score_prefers_hit_vs_non_hit_for_single_factor_rules() {
        let score = build_positive_composite_score(
            RuleAggMetric {
                avg_future_return_pct: Some(5.0),
                ..RuleAggMetric::default()
            },
            Some(&StrategyPerformanceFutureSummary {
                horizon: 5,
                sample_count: 100,
                avg_future_return_pct: Some(1.0),
                p80_return_pct: None,
                p90_return_pct: None,
                p95_return_pct: None,
                strong_quantile: 0.9,
                strong_threshold_pct: None,
                strong_base_rate: Some(0.2),
                win_rate: None,
                max_future_return_pct: None,
            }),
            Some(1.5),
            Some(4.0),
            RuleFactorMetric {
                rank_ic_mean: Some(0.03),
                icir: Some(1.5),
                sharpe_ratio: Some(2.0),
            },
        )
        .expect("score");

        let expected = 1.0 * 0.25 + 2.0 * 0.30 + 2.0 * 0.45;
        assert_close(score, expected, 1e-9);
    }

    #[test]
    fn build_rule_rows_accepts_hit_vs_non_hit_for_auto_candidates() {
        let strategy_options = vec!["ADV".to_string()];
        let rule_meta = HashMap::from([(
            "ADV".to_string(),
            RuleMeta {
                explain: "adv".to_string(),
                tag: RuleTag::Normal,
                scope_way_label: "LAST".to_string(),
                scope_windows: 1,
                points: 1.0,
                has_dist_points: false,
            },
        )]);
        let rule_aggregates = HashMap::from([
            (
                ("ADV".to_string(), true, 2),
                RuleAggMetric {
                    hit_n: 20,
                    avg_future_return_pct: Some(3.0),
                    strong_hit_rate: Some(0.4),
                    win_rate: Some(0.6),
                    avg_total_score: Some(10.0),
                    avg_rank: Some(5.0),
                },
            ),
            (
                ("ADV".to_string(), true, 3),
                RuleAggMetric {
                    hit_n: 20,
                    avg_future_return_pct: Some(3.0),
                    strong_hit_rate: Some(0.4),
                    win_rate: Some(0.6),
                    avg_total_score: Some(10.0),
                    avg_rank: Some(5.0),
                },
            ),
            (
                ("ADV".to_string(), true, 5),
                RuleAggMetric {
                    hit_n: 20,
                    avg_future_return_pct: Some(3.0),
                    strong_hit_rate: Some(0.4),
                    win_rate: Some(0.6),
                    avg_total_score: Some(10.0),
                    avg_rank: Some(5.0),
                },
            ),
        ]);
        let rule_factor_metrics = HashMap::new();
        let future_summary_map = HashMap::from([
            (
                2,
                StrategyPerformanceFutureSummary {
                    horizon: 2,
                    sample_count: 100,
                    avg_future_return_pct: Some(1.0),
                    p80_return_pct: None,
                    p90_return_pct: None,
                    p95_return_pct: None,
                    strong_quantile: 0.9,
                    strong_threshold_pct: None,
                    strong_base_rate: Some(0.2),
                    win_rate: Some(0.5),
                    max_future_return_pct: None,
                },
            ),
            (
                3,
                StrategyPerformanceFutureSummary {
                    horizon: 3,
                    sample_count: 100,
                    avg_future_return_pct: Some(1.0),
                    p80_return_pct: None,
                    p90_return_pct: None,
                    p95_return_pct: None,
                    strong_quantile: 0.9,
                    strong_threshold_pct: None,
                    strong_base_rate: Some(0.2),
                    win_rate: Some(0.5),
                    max_future_return_pct: None,
                },
            ),
            (
                5,
                StrategyPerformanceFutureSummary {
                    horizon: 5,
                    sample_count: 100,
                    avg_future_return_pct: Some(1.0),
                    p80_return_pct: None,
                    p90_return_pct: None,
                    p95_return_pct: None,
                    strong_quantile: 0.9,
                    strong_threshold_pct: None,
                    strong_base_rate: Some(0.2),
                    win_rate: Some(0.5),
                    max_future_return_pct: None,
                },
            ),
        ]);
        let auto_filter = StrategyPerformanceAutoFilterConfig {
            min_samples_2: 1,
            min_samples_3: 1,
            min_samples_5: 1,
            min_samples_10: 1,
            require_win_rate_above_market: false,
            min_pass_horizons: 1,
        };

        let (rows, auto_candidates) = build_rule_rows(
            &strategy_options,
            &rule_meta,
            &rule_aggregates,
            &rule_factor_metrics,
            &rule_factor_metrics,
            &future_summary_map,
            &auto_filter,
            5,
            &[],
            &HashSet::new(),
        );

        assert_eq!(auto_candidates, vec!["ADV".to_string()]);
        let row = rows
            .iter()
            .find(|row| row.rule_name == "ADV" && row.signal_direction == "positive")
            .expect("positive row");
        assert!(row.auto_candidate);
        assert!(
            row.metrics
                .iter()
                .all(|metric| metric.score_mode == SCORE_MODE_HIT_VS_NON_HIT)
        );
        assert!(row.metrics.iter().all(|metric| metric.ic_passes_floor));
        assert!(row.metrics.iter().all(|metric| metric.passes_auto_filter));
    }

    #[test]
    fn overall_score_analysis_falls_back_to_overall_ic_when_daily_ic_is_unavailable() {
        let conn = Connection::open_in_memory().expect("open in-memory db");
        conn.execute(
            r#"
            CREATE TABLE strategy_perf_sample_returns (
                signal_date VARCHAR,
                ts_code VARCHAR,
                horizon INTEGER,
                total_score DOUBLE,
                future_return_pct DOUBLE
            )
            "#,
            [],
        )
        .expect("create strategy_perf_sample_returns");

        for index in 0..25 {
            conn.execute(
                "INSERT INTO strategy_perf_sample_returns (signal_date, ts_code, horizon, total_score, future_return_pct) VALUES (?, ?, ?, ?, ?)",
                params![
                    "20240101",
                    format!("0001{index:02}.SZ"),
                    5i64,
                    1.0f64,
                    (index + 1) as f64,
                ],
            )
            .expect("insert day1 row");
            conn.execute(
                "INSERT INTO strategy_perf_sample_returns (signal_date, ts_code, horizon, total_score, future_return_pct) VALUES (?, ?, ?, ?, ?)",
                params![
                    "20240102",
                    format!("0002{index:02}.SZ"),
                    5i64,
                    2.0f64,
                    (index + 26) as f64,
                ],
            )
            .expect("insert day2 row");
        }

        let future_summary_map = HashMap::from([(
            5,
            StrategyPerformanceFutureSummary {
                horizon: 5,
                sample_count: 50,
                avg_future_return_pct: Some(25.5),
                p80_return_pct: None,
                p90_return_pct: None,
                p95_return_pct: None,
                strong_quantile: 0.9,
                strong_threshold_pct: Some(40.0),
                strong_base_rate: Some(0.2),
                win_rate: Some(1.0),
                max_future_return_pct: Some(50.0),
            },
        )]);

        let analysis = build_overall_score_analysis(&conn, 5, &future_summary_map)
            .expect("overall score analysis")
            .expect("analysis row");

        assert_eq!(analysis.sample_count, 50);
        assert_close(
            analysis.rank_ic_mean.expect("rank_ic_mean"),
            0.8661986608440465,
            1e-9,
        );
        assert!(analysis.icir.is_none());
        assert_close(
            analysis.spearman_corr.expect("spearman_corr"),
            0.8661986608440465,
            1e-9,
        );
    }

    #[test]
    fn spearman_corr_handles_monotonic_series() {
        let samples = vec![
            ScoreObservation {
                signal_date: "20240101".to_string(),
                score: 1.0,
                future_return_pct: 1.0,
            },
            ScoreObservation {
                signal_date: "20240101".to_string(),
                score: 2.0,
                future_return_pct: 2.0,
            },
            ScoreObservation {
                signal_date: "20240101".to_string(),
                score: 3.0,
                future_return_pct: 3.0,
            },
        ];
        let corr = spearman_corr(&samples, false).expect("corr");
        assert!((corr - 1.0).abs() < 1e-9);
    }

    #[test]
    fn strategy_perf_trade_map_uses_next_open_and_horizon_exit() {
        let source_dir = unique_temp_dir();
        write_fixture_files(&source_dir);
        write_fixture_source_db(&source_dir);
        write_fixture_result_db(&source_dir);

        let source_conn =
            open_source_conn(source_dir.to_str().expect("utf8")).expect("open source");
        attach_result_db(&source_conn, source_dir.to_str().expect("utf8"))
            .expect("attach result db");
        prepare_temp_trade_map(&source_conn, source_dir.to_str().expect("utf8"))
            .expect("prepare trade map");
        prepare_temp_sample_returns(&source_conn).expect("prepare sample returns");

        let sample_count = source_conn
            .query_row(
                "SELECT COUNT(*) FROM strategy_perf_sample_returns",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("count sample rows");
        assert_eq!(sample_count, 9);

        let row = source_conn
            .query_row(
                r#"
                SELECT
                    entry_trade_date,
                    entry_open,
                    exit_trade_date,
                    exit_close,
                    future_return_pct
                FROM strategy_perf_sample_returns
                WHERE ts_code = '000001.SZ'
                  AND signal_date = '20240101'
                  AND horizon = 5
                "#,
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, f64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, f64>(3)?,
                        row.get::<_, f64>(4)?,
                    ))
                },
            )
            .expect("load sample row");

        assert_eq!(row.0, "20240102");
        assert_close(row.1, 10.0, 1e-9);
        assert_eq!(row.2, "20240108");
        assert_close(row.3, 10.5, 1e-9);
        assert_close(row.4, 5.0, 1e-9);
    }

    #[test]
    fn strategy_performance_page_builds_candidates() {
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
        assert_eq!(page.selected_rule_name.as_deref(), Some("ADV"));
        assert!(
            page.rule_detail
                .as_ref()
                .expect("rule detail")
                .directions
                .iter()
                .any(|direction| direction.signal_direction == "factor")
        );
    }

    #[test]
    fn rule_detail_rank_ic_uses_full_score_cross_section() {
        let source_dir = unique_temp_dir();
        write_fixture_files(&source_dir);
        write_fixture_source_db(&source_dir);
        write_fixture_result_db(&source_dir);

        let detail = get_strategy_performance_rule_detail(
            source_dir.to_str().expect("utf8").to_string(),
            Some(5),
            Some(0.9),
            "ADV".to_string(),
        )
        .expect("load detail")
        .expect("detail row");

        let factor = detail
            .directions
            .iter()
            .find(|direction| direction.signal_direction == "factor")
            .expect("factor direction");

        assert_close(
            factor.rank_ic_mean.expect("rank_ic_mean"),
            factor.spearman_corr.expect("spearman_corr"),
            1e-9,
        );
        assert!(factor.icir.is_none());
    }

    #[test]
    fn strategy_performance_horizon_view_matches_page_horizon_sections() {
        let source_dir = unique_temp_dir();
        write_fixture_files(&source_dir);
        write_fixture_source_db(&source_dir);
        write_fixture_result_db(&source_dir);

        let page = get_strategy_performance_page(
            source_dir.to_str().expect("utf8").to_string(),
            Some(3),
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
            Some("ADV".to_string()),
        )
        .expect("load page");

        let horizon_view = get_strategy_performance_horizon_view(
            source_dir.to_str().expect("utf8").to_string(),
            Some(3),
            Some(0.9),
            page.resolved_advantage_rule_names.clone(),
            Some(1),
            Some(1),
            Some(1),
            Some(1),
            Some(false),
            Some(1),
            Some(1),
            None,
        )
        .expect("load horizon view");

        assert_eq!(horizon_view.selected_horizon, page.selected_horizon);
        assert_eq!(
            horizon_view.noisy_companion_rule_names,
            page.noisy_companion_rule_names
        );
        assert_eq!(horizon_view.companion_rows.len(), page.companion_rows.len());
        assert_eq!(
            horizon_view
                .overall_score_analysis
                .as_ref()
                .map(|item| item.horizon),
            page.overall_score_analysis
                .as_ref()
                .map(|item| item.horizon)
        );
    }

    #[test]
    fn strategy_validation_page_runs_without_unknown_configs() {
        let source_dir = unique_temp_dir();
        write_fixture_files(&source_dir);
        write_fixture_source_db(&source_dir);
        write_fixture_result_db(&source_dir);

        let page = get_strategy_performance_validation_page(
            source_dir.to_str().expect("utf8").to_string(),
            Some(5),
            Some(0.9),
            StrategyPerformanceValidationDraft {
                strategy_direction: ValidationStrategyDirection::Positive,
                scope_way: "LAST".to_string(),
                scope_windows: 1,
                when: "C > O".to_string(),
                import_name: Some("ADV_DRAFT".to_string()),
                unknown_configs: Vec::new(),
            },
        )
        .expect("validation without unknown configs");

        assert_eq!(page.combo_summaries.len(), 1);
        assert_eq!(page.combo_summaries[0].combo_label, "默认参数");
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
            StrategyPerformanceValidationDraft {
                strategy_direction: ValidationStrategyDirection::Positive,
                scope_way: "LAST".to_string(),
                scope_windows: 1,
                when: "C > O and C >= N".to_string(),
                import_name: Some("ADV_DRAFT".to_string()),
                unknown_configs: vec![StrategyValidationUnknownConfig {
                    name: "N".to_string(),
                    start: 0.0,
                    end: 0.0,
                    step: 1.0,
                }],
            },
        )
        .expect("load validation page");

        assert_eq!(page.selected_horizon, 5);
        assert_eq!(page.combo_summaries.len(), 1);
        assert_eq!(
            page.combo_summaries[0].score_mode,
            SCORE_MODE_HIT_VS_NON_HIT
        );
        assert!(
            page.best_positive_case
                .as_ref()
                .and_then(|item| item.positive_row.as_ref())
                .is_some()
        );
        assert!(
            page.best_positive_case
                .as_ref()
                .expect("best positive case")
                .layer_rows
                .iter()
                .any(|direction| direction.sample_count > 0)
        );
    }

    #[test]
    fn strategy_validation_similarity_excludes_imported_rule() {
        let source_dir = unique_temp_dir();
        write_fixture_files(&source_dir);
        write_fixture_source_db(&source_dir);
        write_fixture_result_db(&source_dir);

        let page = get_strategy_performance_validation_page(
            source_dir.to_str().expect("utf8").to_string(),
            Some(5),
            Some(0.9),
            StrategyPerformanceValidationDraft {
                strategy_direction: ValidationStrategyDirection::Positive,
                scope_way: "LAST".to_string(),
                scope_windows: 1,
                when: "C > O".to_string(),
                import_name: Some("ADV".to_string()),
                unknown_configs: Vec::new(),
            },
        )
        .expect("load validation page");

        let similarity_rows = &page
            .best_positive_case
            .as_ref()
            .expect("best positive case")
            .similarity_rows;
        assert!(similarity_rows.iter().all(|row| row.rule_name != "ADV"));
    }

    #[test]
    fn strategy_validation_page_uses_ic_ir_for_each_multi_hit_rule() {
        let source_dir = unique_temp_dir();
        write_fixture_files(&source_dir);
        write_fixture_source_db(&source_dir);
        write_fixture_result_db(&source_dir);

        let page = get_strategy_performance_validation_page(
            source_dir.to_str().expect("utf8").to_string(),
            Some(5),
            Some(0.9),
            StrategyPerformanceValidationDraft {
                strategy_direction: ValidationStrategyDirection::Positive,
                scope_way: "EACH".to_string(),
                scope_windows: 3,
                when: "C > O and C >= N".to_string(),
                import_name: Some("ADV_DRAFT".to_string()),
                unknown_configs: vec![StrategyValidationUnknownConfig {
                    name: "N".to_string(),
                    start: 0.0,
                    end: 0.0,
                    step: 1.0,
                }],
            },
        )
        .expect("load validation page");

        assert_eq!(page.combo_summaries.len(), 1);
        assert_eq!(page.combo_summaries[0].score_mode, SCORE_MODE_IC_IR);
        assert!(
            page.best_positive_case
                .as_ref()
                .and_then(|item| item.positive_row.as_ref())
                .map(|row| row
                    .metrics
                    .iter()
                    .all(|metric| metric.score_mode == SCORE_MODE_IC_IR))
                .unwrap_or(false)
        );
    }

    #[test]
    fn strategy_validation_unknown_replacement_only_replaces_full_identifier() {
        let source_dir = unique_temp_dir();
        write_fixture_files(&source_dir);
        write_fixture_source_db(&source_dir);
        write_fixture_result_db(&source_dir);

        let page = get_strategy_performance_validation_page(
            source_dir.to_str().expect("utf8").to_string(),
            Some(5),
            Some(0.9),
            StrategyPerformanceValidationDraft {
                strategy_direction: ValidationStrategyDirection::Positive,
                scope_way: "LAST".to_string(),
                scope_windows: 1,
                when: "C > O and V > MA(V, M)".to_string(),
                import_name: Some("ADV_DRAFT".to_string()),
                unknown_configs: vec![StrategyValidationUnknownConfig {
                    name: "M".to_string(),
                    start: 1.0,
                    end: 1.0,
                    step: 1.0,
                }],
            },
        )
        .expect("load validation page with identifier-safe replacement");

        assert_eq!(page.combo_summaries.len(), 1);
        assert_eq!(page.combo_summaries[0].formula, "C > O and V > MA(V, 1)");
    }
}
