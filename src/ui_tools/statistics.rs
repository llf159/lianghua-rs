use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

use duckdb::{Connection, params};
use rand::random;
#[cfg(test)]
use rand::{Rng, SeedableRng, rngs::StdRng};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    data::scoring_data::{
        SceneDetails, ScoreDetails, ScoreSummary, cache_rule_build as build_scoring_rule_cache,
        row_into_rt,
    },
    data::{
        DataReader, RuleKind, RuleStage, RuleTag, RuntimeKeyCollectOptions, ScopeWay, ScoreRule,
        ScoreScene, collect_assigned_names_from_expr_program,
        collect_runtime_keys_from_expr_programs, concept_performance_db_path,
        expr_program_uses_runtime_key, load_stock_list, load_ths_concepts_list, result_db_path,
        source_db_path,
    },
    expr::{
        eval::{Runtime, Value},
        lexer::TokenKind,
        parser::{Stmt, Stmts, lex_all},
        validation::{
            estimate_expression_warmup, parse_expression_program, validate_expression_functions,
        },
    },
    scoring::runner::{ScoringMemoryMode, scoring_all_to_memory_with_mode},
    scoring::tools::{
        CyqChenFieldInjector, calc_query_need_rows, calc_query_start_date,
        collect_used_cyq_chen_runtime_keys, cyq_chen_runtime_key_names, inject_stock_extra_fields,
        load_st_list, load_total_share_map,
    },
    scoring::{CachedRule, evaluate_cached_rule_scores},
    simulate::{
        DEFAULT_BACKTEST_MIN_LISTED_TRADE_DAYS, build_backtest_sample_eligibility,
        rank::{
            RankLayerConfig, RankLayerFromDbInput, RankLayerMethod,
            calc_rank_layer_metrics_from_rank_samples, calc_rank_layer_metrics_from_score_rows,
        },
        rule::{
            DEFAULT_RULE_WITH_SAMPLES_PARALLEL_BATCH_SIZE, RuleJointRidgeDayStats, RuleLayerConfig,
            RuleLayerFromDbInput, RuleLayerMetricsWithSamples, RuleLayerRuntimeCache,
            RuleLayerSamplePointRef, build_rule_layer_runtime_cache_from_stock_data_with_ts_filter,
            build_rule_layer_runtime_cache_from_summary_rows,
            calc_all_rule_layer_metrics_with_samples_from_rows_map,
            calc_rule_joint_ridge_day_stats_from_cache_head_weighted,
            calc_rule_layer_metrics_from_cache, calc_rule_layer_metrics_from_db_with_ts_filter,
            calc_rule_layer_metrics_with_samples_from_cache,
            visit_rule_layer_base_samples_from_cache, visit_triggered_rule_samples_from_cache,
        },
        scene::{
            SceneLayerConfig, SceneLayerFromDbInput,
            calc_all_scene_layer_metrics_from_db_with_ts_filter,
            calc_all_scene_layer_metrics_from_rows,
            calc_scene_layer_metrics_from_db_with_ts_filter,
        },
    },
    ui_tools::{build_concepts_map, build_name_map, build_total_mv_map, filter_mv},
    utils::utils::board_category,
};

const TOP_RANK_THRESHOLD: i64 = 100;
const RULE_DECAY_WINDOWS: [usize; 3] = [20, 40, 60];
const RULE_DECAY_MIN_PRIOR_DAYS: usize = 10;
const RULE_VALIDATION_INJECTED_RUNTIME_KEYS: [&str; 4] = ["RANK", "SCORE", "ZHANG", "TOTAL_MV_YI"];
const RULE_VALIDATION_RUNTIME_ALIASES: [(&str, &str); 0] = [];
const BACKTEST_INJECTED_RUNTIME_KEYS: [&str; 2] = ["ZHANG", "TOTAL_MV_YI"];
const BACKTEST_RUNTIME_ALIASES: [(&str, &str); 0] = [];

#[derive(Debug, Clone)]
struct RuleMeta {
    when: String,
    explain: String,
    trigger_mode: String,
    is_each: bool,
    points: f64,
}

#[derive(Debug, Clone, Default)]
struct RuleDayAgg {
    trigger_count: i64,
    contribution_score: f64,
    top100_trigger_count: i64,
    best_rank: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct StrategyHeatmapCell {
    pub trade_date: String,
    pub day_level: Option<f64>,
    pub avg_level: Option<f64>,
    pub delta_level: Option<f64>,
    pub above_avg: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct StrategyOverviewPayload {
    pub items: Option<Vec<StrategyHeatmapCell>>,
    pub latest_trade_date: Option<String>,
    pub average_level: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyDailyRow {
    pub trade_date: String,
    pub rule_name: String,
    pub trigger_mode: Option<String>,
    pub sample_count: Option<i64>,
    pub trigger_count: Option<i64>,
    pub coverage: Option<f64>,
    pub contribution_score: Option<f64>,
    pub contribution_per_trigger: Option<f64>,
    pub median_trigger_count: Option<f64>,
    pub top100_trigger_count: Option<i64>,
    pub best_rank: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct StrategyChartPoint {
    pub trade_date: String,
    pub trigger_count: Option<i64>,
    pub top100_trigger_count: Option<i64>,
    pub coverage: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct StrategyChartPayload {
    pub items: Option<Vec<StrategyChartPoint>>,
}

#[derive(Debug, Serialize)]
pub struct TriggeredStockRow {
    pub rank: Option<i64>,
    pub ts_code: String,
    pub name: Option<String>,
    pub total_score: Option<f64>,
    pub rule_score: Option<f64>,
    pub concept: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct StrategyStatisticsPageData {
    pub overview: Option<StrategyOverviewPayload>,
    pub detail_rows: Option<Vec<StrategyDailyRow>>,
    pub strategy_options: Option<Vec<String>>,
    pub resolved_strategy_name: Option<String>,
    pub analysis_trade_date_options: Option<Vec<String>>,
    pub resolved_analysis_trade_date: Option<String>,
    pub chart: Option<StrategyChartPayload>,
    pub triggered_stocks: Option<Vec<TriggeredStockRow>>,
}

#[derive(Debug, Serialize)]
pub struct StrategyStatisticsDetailData {
    pub strategy_name: String,
    pub analysis_trade_date_options: Vec<String>,
    pub resolved_analysis_trade_date: Option<String>,
    pub selected_daily_row: Option<StrategyDailyRow>,
    pub chart: Option<StrategyChartPayload>,
    pub triggered_stocks: Vec<TriggeredStockRow>,
}

#[derive(Debug, Serialize)]
pub struct SceneStageRow {
    pub stage: String,
    pub sample_count: i64,
    pub stage_ratio_in_scene: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct SceneContributionSummary {
    pub scene_covered_count: i64,
    pub scene_total_sample_count: i64,
    pub scene_coverage_ratio: Option<f64>,
    pub scene_rule_contribution_score: Option<f64>,
    pub all_rule_contribution_score: Option<f64>,
    pub scene_rule_contribution_ratio: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct SceneStatisticsPageData {
    pub scene_options: Option<Vec<String>>,
    pub resolved_scene_name: Option<String>,
    pub analysis_trade_date_options: Option<Vec<String>>,
    pub resolved_analysis_trade_date: Option<String>,
    pub stage_rows: Option<Vec<SceneStageRow>>,
    pub summary: Option<SceneContributionSummary>,
}

#[derive(Debug, Serialize)]
pub struct SceneLayerStateAvgResidualReturn {
    pub scene_state: String,
    pub avg_residual_return: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct SceneLayerPointPayload {
    pub trade_date: String,
    pub state_avg_residual_returns: Vec<SceneLayerStateAvgResidualReturn>,
    pub top_bottom_spread: Option<f64>,
    pub ic: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct SceneLayerSceneSummary {
    pub scene_name: String,
    pub point_count: usize,
    pub spread_mean: Option<f64>,
    pub ic_mean: Option<f64>,
    pub ic_std: Option<f64>,
    pub icir: Option<f64>,
    pub ic_t_value: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct SceneLayerBacktestData {
    pub scene_name: String,
    pub stock_adj_type: String,
    pub index_ts_code: String,
    pub index_beta: f64,
    pub concept_beta: f64,
    pub industry_beta: f64,
    pub start_date: String,
    pub end_date: String,
    pub resolved_board: Option<String>,
    pub exclude_st_board: bool,
    pub total_mv_min: Option<f64>,
    pub total_mv_max: Option<f64>,
    pub min_samples_per_scene_day: usize,
    pub min_listed_trade_days: usize,
    pub backtest_period: usize,
    pub points: Vec<SceneLayerPointPayload>,
    pub spread_mean: Option<f64>,
    pub ic_mean: Option<f64>,
    pub ic_std: Option<f64>,
    pub icir: Option<f64>,
    pub ic_t_value: Option<f64>,
    pub is_all_scenes: bool,
    pub all_scene_summaries: Vec<SceneLayerSceneSummary>,
}

#[derive(Debug, Serialize)]
pub struct SceneLayerBacktestDefaultsData {
    pub scene_options: Vec<String>,
    pub resolved_scene_name: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RuleLayerPointPayload {
    pub trade_date: String,
    pub sample_count: usize,
    pub avg_rule_score: Option<f64>,
    pub avg_residual_return: Option<f64>,
    pub avg_excess_residual_return: Option<f64>,
    pub top_bottom_spread: Option<f64>,
    pub ic: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RuleDecayValidation {
    pub window_days: usize,
    pub recent_start_date: Option<String>,
    pub recent_end_date: Option<String>,
    pub recent_day_count: usize,
    pub prior_day_count: usize,
    pub recent_directional_excess_mean: Option<f64>,
    pub prior_directional_excess_mean: Option<f64>,
    pub decay_change: Option<f64>,
    pub decay_t_value: Option<f64>,
    pub status: String,
    pub status_label: String,
}

#[derive(Debug, Serialize)]
pub struct RuleLayerRuleSummary {
    pub rule_name: String,
    pub point_count: usize,
    pub avg_residual_mean: Option<f64>,
    pub avg_excess_residual_mean: Option<f64>,
    pub avg_er_change: Option<f64>,
    #[serde(skip)]
    pub er_change_sample_count: usize,
    pub profit_loss_ratio: Option<f64>,
    pub spread_mean: Option<f64>,
    pub avg_contribution_score: Option<f64>,
    pub avg_contribution_per_trigger: Option<f64>,
    pub ic_mean: Option<f64>,
    pub ic_std: Option<f64>,
    pub icir: Option<f64>,
    pub ic_t_value: Option<f64>,
    pub decay_validations: Vec<RuleDecayValidation>,
    #[serde(skip)]
    pub decay_daily_values: Vec<(String, f64)>,
}

#[derive(Debug, Serialize)]
pub struct RuleLayerBacktestData {
    pub rule_name: String,
    pub stock_adj_type: String,
    pub index_ts_code: String,
    pub index_beta: f64,
    pub concept_beta: f64,
    pub industry_beta: f64,
    pub start_date: String,
    pub end_date: String,
    pub resolved_board: Option<String>,
    pub exclude_st_board: bool,
    pub total_mv_min: Option<f64>,
    pub total_mv_max: Option<f64>,
    pub min_samples_per_rule_day: usize,
    pub min_listed_trade_days: usize,
    pub backtest_period: usize,
    pub points: Vec<RuleLayerPointPayload>,
    pub avg_residual_mean: Option<f64>,
    pub avg_excess_residual_mean: Option<f64>,
    pub decay_validations: Vec<RuleDecayValidation>,
    pub avg_er_change: Option<f64>,
    pub profit_loss_ratio: Option<f64>,
    pub spread_mean: Option<f64>,
    pub avg_contribution_score: Option<f64>,
    pub avg_contribution_per_trigger: Option<f64>,
    pub ic_mean: Option<f64>,
    pub ic_std: Option<f64>,
    pub icir: Option<f64>,
    pub ic_t_value: Option<f64>,
    pub layer_count: Option<usize>,
    pub layer_method: Option<String>,
    pub layer_method_label: Option<String>,
    pub layer_summaries: Vec<RankLayerBucketSummary>,
    pub is_all_rules: bool,
    pub all_rule_summaries: Vec<RuleLayerRuleSummary>,
    pub rule_validation_details: Vec<RuleValidationComboResult>,
}

#[derive(Debug, Serialize)]
pub struct RuleLayerBacktestDefaultsData {
    pub rule_options: Vec<String>,
    pub resolved_rule_name: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RuleJointWalkForwardFold {
    pub fold_index: usize,
    pub train_start_date: String,
    pub train_end_date: String,
    pub test_start_date: String,
    pub test_end_date: String,
    pub train_days: usize,
    pub purge_days: usize,
    pub test_days: usize,
    pub ridge_alpha: f64,
    pub ridge_oos_r2: Option<f64>,
    pub current_score_oos_r2: Option<f64>,
    pub ridge_head_excess_mean: Option<f64>,
    pub current_head_excess_mean: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RuleJointRidgeRuleResult {
    pub rule_name: String,
    pub explain: String,
    pub current_points: f64,
    pub score_scale: f64,
    pub trigger_samples: usize,
    pub ridge_coefficient: f64,
    pub standardized_coefficient: f64,
    pub raw_suggested_points: f64,
    pub suggested_points: f64,
    pub point_change: f64,
    pub positive_fold_rate: Option<f64>,
    pub oos_contribution: Option<f64>,
    pub max_correlation: Option<f64>,
    pub most_correlated_rule: Option<String>,
    pub status: String,
    pub status_label: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RuleJointHeadMetric {
    pub key: String,
    pub label: String,
    pub ridge_head_excess_mean: Option<f64>,
    pub current_head_excess_mean: Option<f64>,
    pub ridge_winning_fold_count: usize,
    pub valid_fold_count: usize,
    pub evaluated_day_count: usize,
}

#[derive(Debug, Serialize)]
pub struct RuleJointRidgeValidationData {
    pub continuation_id: String,
    pub start_date: String,
    pub end_date: String,
    pub feature_count: usize,
    pub sample_count: usize,
    pub exposed_sample_count: usize,
    pub valid_days: usize,
    pub fold_count: usize,
    pub purge_days: usize,
    pub selected_ridge_alpha: f64,
    pub ridge_oos_r2: Option<f64>,
    pub current_score_oos_r2: Option<f64>,
    pub ridge_head_excess_mean: Option<f64>,
    pub current_head_excess_mean: Option<f64>,
    pub primary_head_key: String,
    pub primary_head_label: String,
    pub head_metrics: Vec<RuleJointHeadMetric>,
    pub validation_passed: bool,
    pub validation_status_label: String,
    pub head_winning_fold_count: usize,
    pub required_head_winning_folds: usize,
    pub latest_head_fold_passed: bool,
    pub training_weight_description: String,
    pub point_scale_description: String,
    pub folds: Vec<RuleJointWalkForwardFold>,
    pub rules: Vec<RuleJointRidgeRuleResult>,
}

#[derive(Debug, Serialize)]
pub struct RankLayerBucketSummary {
    pub layer_index: usize,
    pub layer_label: String,
    pub point_count: usize,
    pub sample_count: usize,
    pub avg_score: Option<f64>,
    pub avg_residual_return: Option<f64>,
    pub avg_er_change: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct RankLayerSampleGroup {
    pub layer_index: usize,
    pub layer_label: String,
    pub total_samples: usize,
    pub triggered_days: usize,
    pub positive_count: usize,
    pub negative_count: usize,
    pub random_count: usize,
    pub positive: Vec<RuleValidationSampleRow>,
    pub negative: Vec<RuleValidationSampleRow>,
    pub random: Vec<RuleValidationSampleRow>,
}

#[derive(Debug, Serialize)]
pub struct RankLayerBacktestData {
    pub stock_adj_type: String,
    pub index_ts_code: String,
    pub index_beta: f64,
    pub concept_beta: f64,
    pub industry_beta: f64,
    pub start_date: String,
    pub end_date: String,
    pub resolved_board: Option<String>,
    pub exclude_st_board: bool,
    pub market_value_grouping: bool,
    pub min_samples_per_rank_day: usize,
    pub min_listed_trade_days: usize,
    pub backtest_period: usize,
    pub layer_count: usize,
    pub layer_method: String,
    pub layer_method_label: String,
    pub point_count: usize,
    pub sample_count: usize,
    pub avg_er_change: Option<f64>,
    pub spread_mean: Option<f64>,
    pub ic_mean: Option<f64>,
    pub ic_std: Option<f64>,
    pub icir: Option<f64>,
    pub ic_t_value: Option<f64>,
    pub layer_summaries: Vec<RankLayerBucketSummary>,
    pub layer_sample_groups: Vec<RankLayerSampleGroup>,
    pub market_value_summaries: Vec<RankLayerMarketValueSummary>,
    pub joint_validation_continuation_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RankLayerMarketValueSummary {
    pub group_label: String,
    pub total_mv_min: Option<f64>,
    pub total_mv_max: Option<f64>,
    pub point_count: usize,
    pub sample_count: usize,
    pub avg_er_change: Option<f64>,
    pub spread_mean: Option<f64>,
    pub ic_mean: Option<f64>,
    pub ic_t_value: Option<f64>,
    pub icir: Option<f64>,
}

const VALIDATION_EPS: f64 = 1e-12;
const RULE_BACKTEST_EPS: f64 = 1e-12;
const VALIDATION_MAX_COMBINATIONS: usize = 256;
const VALIDATION_COMBO_EVAL_BATCH_SIZE: usize = 16;
const VALIDATION_DEFAULT_SAMPLE_LIMIT_PER_GROUP: usize = 30;
const VALIDATION_MAX_SAMPLE_LIMIT_PER_GROUP: usize = 200;
const VALIDATION_CONTINUATION_CACHE_LIMIT: usize = 1;
const VALIDATION_CONTINUATION_TTL: Duration = Duration::from_secs(30 * 60);
const VALIDATION_CALIBRATION_MIN_SAMPLES: usize = 100;
const VALIDATION_CALIBRATION_MIN_DAYS: usize = 20;
const VALIDATION_CALIBRATION_LCB_Z: f64 = 1.28;
const VALIDATION_CALIBRATION_POINT_SCALE: f64 = 40.0;
const RULE_JOINT_VALIDATION_CACHE_LIMIT: usize = 1;
const RULE_JOINT_VALIDATION_TTL: Duration = Duration::from_secs(30 * 60);
const RULE_JOINT_RIDGE_MAX_FEATURES: usize = 256;
const RULE_JOINT_WALK_FORWARD_MAX_FOLDS: usize = 5;
const RULE_JOINT_WALK_FORWARD_MIN_TRAIN_DAYS: usize = 40;
const RULE_JOINT_WALK_FORWARD_MIN_TEST_DAYS: usize = 10;
const RULE_JOINT_PRIMARY_HEAD_INDEX: usize = 2;
const RULE_JOINT_RULE_MIN_POSITIVE_FOLD_RATE: f64 = 0.8;
const RULE_JOINT_POINT_SCALE: f64 = 40.0;
const RULE_JOINT_RIDGE_ALPHAS: [f64; 9] = [0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1.0, 3.0, 10.0];
const RULE_BACKTEST_DETAIL_SAMPLE_LIMIT_PER_GROUP: usize = 5;
const RANK_BACKTEST_LAYER_SAMPLE_LIMIT_PER_GROUP: usize = 5;
#[cfg(test)]
const VALIDATION_RANDOM_SAMPLE_SEED: u64 = 0x9E37_79B9_7F4A_7C15;

#[derive(Debug, Clone, Deserialize)]
pub struct RuleValidationUnknownConfig {
    pub name: String,
    pub start: f64,
    pub end: f64,
    pub step: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RuleValidationUnknownValue {
    pub name: String,
    pub value: f64,
}

#[derive(Debug, Serialize)]
pub struct RuleValidationSimilarityRow {
    pub rule_name: String,
    pub explain: Option<String>,
    pub overlap_samples: usize,
    pub overlap_rate_vs_validation: Option<f64>,
    pub overlap_rate_vs_existing: Option<f64>,
    pub overlap_lift: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct RuleValidationSampleStats {
    pub positive_count: usize,
    pub negative_count: usize,
    pub random_count: usize,
    pub total_samples: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct RuleValidationTriggerCountStats {
    pub trigger_count: usize,
    pub positive_count: usize,
    pub negative_count: usize,
    pub random_count: usize,
    pub total_samples: usize,
}

#[derive(Debug, Serialize)]
pub struct RuleValidationSampleRow {
    pub ts_code: String,
    pub name: Option<String>,
    pub board: String,
    pub volatility_group: String,
    pub trade_date: String,
    pub trigger_count: usize,
    pub rule_score: f64,
    pub residual_return: f64,
}

#[derive(Debug, Serialize)]
pub struct RuleValidationSampleGroups {
    pub positive: Vec<RuleValidationSampleRow>,
    pub negative: Vec<RuleValidationSampleRow>,
    pub random: Vec<RuleValidationSampleRow>,
}

#[derive(Debug, Serialize)]
pub struct RuleValidationReturnDistributionBucket {
    pub bucket_label: String,
    pub sample_count: usize,
    pub sample_ratio: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct RuleValidationComboResult {
    pub combo_key: String,
    pub combo_label: String,
    pub formula: String,
    pub unknown_values: Vec<RuleValidationUnknownValue>,
    pub trigger_samples: usize,
    pub triggered_days: usize,
    pub avg_daily_trigger: f64,
    pub sample_stats: RuleValidationSampleStats,
    pub trigger_count_stats: Vec<RuleValidationTriggerCountStats>,
    pub sample_groups: RuleValidationSampleGroups,
    pub return_distribution: Vec<RuleValidationReturnDistributionBucket>,
    pub backtest: RuleLayerBacktestData,
    pub similarity_rows: Vec<RuleValidationSimilarityRow>,
}

#[derive(Debug, Serialize)]
pub struct RuleExpressionValidationData {
    pub import_rule_name: String,
    pub import_rule_explain: String,
    pub scope_way: String,
    pub scope_windows: usize,
    pub sample_limit_per_group: usize,
    pub combo_results: Vec<RuleValidationComboResult>,
    pub best_combo_key: Option<String>,
    pub continuation_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RuleExpressionCalibrationBucket {
    pub score_multiplier: f64,
    pub sample_count: usize,
    pub avg_residual_return: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RuleExpressionCalibrationDistancePoint {
    pub min: usize,
    pub max: usize,
    pub points: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RuleExpressionCalibrationCandidate {
    pub candidate_key: String,
    pub scope_way: String,
    pub scope_label: String,
    pub scope_windows: usize,
    pub is_current: bool,
    pub trigger_samples: usize,
    pub triggered_days: usize,
    pub avg_daily_trigger: f64,
    pub avg_residual_mean: Option<f64>,
    pub avg_excess_residual_mean: Option<f64>,
    pub daily_std: Option<f64>,
    pub standard_error: Option<f64>,
    pub conservative_edge: Option<f64>,
    pub early_excess_residual_mean: Option<f64>,
    pub late_excess_residual_mean: Option<f64>,
    pub ic_mean: Option<f64>,
    pub ic_t_value: Option<f64>,
    pub score_monotonicity: Option<f64>,
    pub avg_score_multiplier: Option<f64>,
    pub suggested_points: f64,
    pub suggested_total_points: f64,
    pub calibration_score: f64,
    pub status: String,
    pub status_label: String,
    pub score_buckets: Vec<RuleExpressionCalibrationBucket>,
    pub suggested_dist_points: Vec<RuleExpressionCalibrationDistancePoint>,
}

#[derive(Debug, Serialize)]
pub struct RuleExpressionCalibrationData {
    pub continuation_id: String,
    pub combo_key: String,
    pub combo_label: String,
    pub direction: String,
    pub candidate_count: usize,
    pub point_scale_description: String,
    pub recommended_candidate_key: Option<String>,
    pub candidates: Vec<RuleExpressionCalibrationCandidate>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuleExpressionValidationManualStrategy {
    pub name: Option<String>,
    pub scene_name: Option<String>,
    pub stage: Option<String>,
    pub scope_way: Option<String>,
    pub scope_windows: Option<usize>,
    pub when: Option<String>,
    pub points: Option<f64>,
    pub dist_points: Option<Vec<crate::data::DistPoint>>,
    pub explain: Option<String>,
    pub tag: Option<String>,
}

#[derive(Debug)]
struct ValidationVariant {
    combo_key: String,
    combo_label: String,
    formula: String,
    unknown_values: Vec<RuleValidationUnknownValue>,
}

type ValidationTriggeredScoreMap = HashMap<String, HashMap<String, f64>>;

struct PreparedValidationCombo {
    variant: ValidationVariant,
    cached_rule: CachedRule,
    assigned_names: Vec<String>,
}

struct ValidationExecutionPlan {
    combos: Vec<PreparedValidationCombo>,
    need_rows: usize,
    query_start_date: String,
}

struct ValidationTsCodeEvaluation {
    ts_code: String,
    combo_hits: Vec<(usize, HashMap<String, f64>)>,
}

#[derive(Debug, Clone)]
struct ValidationSeedRule {
    rule_name: String,
    rule_explain: String,
    scope_way: ScopeWay,
    scope_windows: usize,
    formula: String,
    points: f64,
    dist_points: Option<Vec<crate::data::DistPoint>>,
    tag: RuleTag,
    exclude_rule_name: Option<String>,
}

#[derive(Debug, Clone)]
struct ValidationContinuationCombo {
    combo_key: String,
    combo_label: String,
    formula: String,
}

#[derive(Debug)]
struct ValidationContinuationSession {
    created_at: Instant,
    source_path: String,
    params: RuleLayerBacktestRunParams,
    runtime_cache: Arc<RuleLayerRuntimeCache>,
    seed_rule: ValidationSeedRule,
    validation_ts_codes: Vec<String>,
    combos: HashMap<String, ValidationContinuationCombo>,
}

static VALIDATION_CONTINUATION_CACHE: OnceLock<
    Mutex<HashMap<String, Arc<ValidationContinuationSession>>>,
> = OnceLock::new();

fn validation_continuation_cache()
-> &'static Mutex<HashMap<String, Arc<ValidationContinuationSession>>> {
    VALIDATION_CONTINUATION_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn store_validation_continuation_session(
    session: ValidationContinuationSession,
) -> Result<String, String> {
    let mut cache = validation_continuation_cache()
        .lock()
        .map_err(|_| "保存表达式继续验证基础数据失败:缓存锁已损坏".to_string())?;
    cache.retain(|_, item| item.created_at.elapsed() <= VALIDATION_CONTINUATION_TTL);
    while cache.len() >= VALIDATION_CONTINUATION_CACHE_LIMIT {
        let Some(oldest_key) = cache
            .iter()
            .max_by_key(|(_, item)| item.created_at.elapsed())
            .map(|(key, _)| key.clone())
        else {
            break;
        };
        cache.remove(&oldest_key);
    }

    let continuation_id = loop {
        let candidate = format!(
            "expr-calibration-{:016x}{:016x}",
            random::<u64>(),
            random::<u64>()
        );
        if !cache.contains_key(&candidate) {
            break candidate;
        }
    };
    cache.insert(continuation_id.clone(), Arc::new(session));
    Ok(continuation_id)
}

fn load_validation_continuation_session(
    continuation_id: &str,
) -> Result<Arc<ValidationContinuationSession>, String> {
    let mut cache = validation_continuation_cache()
        .lock()
        .map_err(|_| "读取表达式继续验证基础数据失败:缓存锁已损坏".to_string())?;
    cache.retain(|_, item| item.created_at.elapsed() <= VALIDATION_CONTINUATION_TTL);
    cache
        .get(continuation_id.trim())
        .cloned()
        .ok_or_else(|| "表达式基础验证缓存已失效，请重新执行一次表达式验证".to_string())
}

#[derive(Debug, Clone)]
struct RuleJointValidationFeature {
    rule_name: String,
    explain: String,
    current_points: f64,
    score_scale: f64,
}

#[derive(Debug)]
struct RuleJointValidationSession {
    created_at: Instant,
    source_path: String,
    params: RuleLayerBacktestRunParams,
    summary_rows: Arc<Vec<ScoreSummary>>,
    features: Vec<RuleJointValidationFeature>,
}

static RULE_JOINT_VALIDATION_CACHE: OnceLock<
    Mutex<HashMap<String, Arc<RuleJointValidationSession>>>,
> = OnceLock::new();

fn rule_joint_validation_cache() -> &'static Mutex<HashMap<String, Arc<RuleJointValidationSession>>>
{
    RULE_JOINT_VALIDATION_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn store_rule_joint_validation_session(
    session: RuleJointValidationSession,
) -> Result<String, String> {
    let mut cache = rule_joint_validation_cache()
        .lock()
        .map_err(|_| "保存排名整体继续验证数据失败:缓存锁已损坏".to_string())?;
    cache.retain(|_, item| item.created_at.elapsed() <= RULE_JOINT_VALIDATION_TTL);
    while cache.len() >= RULE_JOINT_VALIDATION_CACHE_LIMIT {
        let Some(oldest_key) = cache
            .iter()
            .max_by_key(|(_, item)| item.created_at.elapsed())
            .map(|(key, _)| key.clone())
        else {
            break;
        };
        cache.remove(&oldest_key);
    }
    let continuation_id = loop {
        let candidate = format!(
            "rule-joint-ridge-{:016x}{:016x}",
            random::<u64>(),
            random::<u64>()
        );
        if !cache.contains_key(&candidate) {
            break candidate;
        }
    };
    cache.insert(continuation_id.clone(), Arc::new(session));
    Ok(continuation_id)
}

fn load_rule_joint_validation_session(
    continuation_id: &str,
) -> Result<Arc<RuleJointValidationSession>, String> {
    let mut cache = rule_joint_validation_cache()
        .lock()
        .map_err(|_| "读取排名整体继续验证数据失败:缓存锁已损坏".to_string())?;
    cache.retain(|_, item| item.created_at.elapsed() <= RULE_JOINT_VALIDATION_TTL);
    cache
        .get(continuation_id.trim())
        .cloned()
        .ok_or_else(|| "排名整体回测缓存已失效，请重新执行一次排名整体回测".to_string())
}

#[derive(Debug, Serialize)]
pub struct MarketRankItem {
    pub name: String,
    pub value: f64,
    pub ts_code: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub concepts: Option<String>,
    pub three_day_gain: Option<f64>,
    pub five_day_gain: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct MarketAnalysisSnapshot {
    pub trade_date: Option<String>,
    pub concept_top: Vec<MarketRankItem>,
    pub industry_top: Vec<MarketRankItem>,
    pub concept_money_flow_top: Vec<MarketRankItem>,
    pub industry_money_flow_top: Vec<MarketRankItem>,
    pub concept_money_outflow_top: Vec<MarketRankItem>,
    pub industry_money_outflow_top: Vec<MarketRankItem>,
    pub gain_top: Vec<MarketRankItem>,
    pub sub_interval_gain_top: Vec<MarketRankItem>,
}

#[derive(Debug, Serialize)]
pub struct MarketAnalysisData {
    pub lookback_period: usize,
    pub stock_rank_limit: usize,
    pub sub_interval_period: usize,
    pub min_board_stock_count: usize,
    pub latest_trade_date: Option<String>,
    pub resolved_reference_trade_date: Option<String>,
    pub board_options: Vec<String>,
    pub resolved_board: Option<String>,
    pub interval: MarketAnalysisSnapshot,
    pub daily: MarketAnalysisSnapshot,
}

#[derive(Debug, Serialize)]
pub struct MarketContributorItem {
    pub ts_code: String,
    pub name: Option<String>,
    pub industry: Option<String>,
    pub contribution_pct: f64,
}

fn market_rank_item(name: String, value: f64) -> MarketRankItem {
    MarketRankItem {
        name,
        value,
        ts_code: None,
        start_date: None,
        end_date: None,
        concepts: None,
        three_day_gain: None,
        five_day_gain: None,
    }
}

fn market_stock_rank_item(
    stock_name_map: &HashMap<String, String>,
    ts_code: String,
    value: f64,
    start_date: Option<String>,
    end_date: Option<String>,
) -> MarketRankItem {
    let name = stock_name_map
        .get(&ts_code)
        .cloned()
        .unwrap_or_else(|| ts_code.clone());
    MarketRankItem {
        name: format!("{} ({})", name, ts_code),
        value,
        ts_code: Some(ts_code),
        start_date,
        end_date,
        concepts: None,
        three_day_gain: None,
        five_day_gain: None,
    }
}

fn trailing_period_gain(rows: &[(String, f64)], period: usize) -> Option<f64> {
    if period == 0 || rows.len() <= period {
        return None;
    }
    let start_close = rows.get(rows.len() - period - 1)?.1;
    let end_close = rows.last()?.1;
    if !start_close.is_finite() || !end_close.is_finite() || start_close <= f64::EPSILON {
        return None;
    }
    let value = (end_close / start_close - 1.0) * 100.0;
    value.is_finite().then_some(value)
}

#[derive(Debug, Serialize)]
pub struct MarketContributionData {
    pub scope: String,
    pub kind: String,
    pub name: String,
    pub trade_date: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub lookback_period: usize,
    pub contributors: Vec<MarketContributorItem>,
}

fn open_result_conn(source_path: &str) -> Result<Connection, String> {
    let result_db = result_db_path(source_path);
    let result_db_str = result_db
        .to_str()
        .ok_or_else(|| "结果库路径不是有效UTF-8".to_string())?;
    Connection::open(result_db_str).map_err(|e| format!("打开结果库失败: {e}"))
}

fn scope_way_label(scope_way: ScopeWay) -> String {
    match scope_way {
        ScopeWay::Any => "any".to_string(),
        ScopeWay::Last => "last".to_string(),
        ScopeWay::Each => "each".to_string(),
        ScopeWay::Recent => "recent".to_string(),
        ScopeWay::Consec(n) => format!("consec>={n}"),
    }
}

fn parse_scope_way_input(scope_way_raw: &str) -> Result<ScopeWay, String> {
    let normalized = scope_way_raw.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "ANY" => Ok(ScopeWay::Any),
        "LAST" => Ok(ScopeWay::Last),
        "EACH" => Ok(ScopeWay::Each),
        "RECENT" => Ok(ScopeWay::Recent),
        value => {
            let Some(num) = value.strip_prefix("CONSEC>=") else {
                return Err(format!(
                    "scope_way 不支持: {scope_way_raw}，仅支持 ANY/LAST/EACH/RECENT/CONSEC>=N"
                ));
            };
            let threshold = num
                .parse::<usize>()
                .map_err(|_| format!("scope_way 连续阈值非法: {scope_way_raw}"))?;
            if threshold == 0 {
                return Err("scope_way 连续阈值必须 >= 1".to_string());
            }
            Ok(ScopeWay::Consec(threshold))
        }
    }
}

fn parse_rule_stage_input(stage_raw: &str) -> Result<RuleStage, String> {
    match stage_raw.trim().to_ascii_lowercase().as_str() {
        "base" => Ok(RuleStage::Base),
        "trigger" => Ok(RuleStage::Trigger),
        "confirm" => Ok(RuleStage::Confirm),
        "risk" => Ok(RuleStage::Risk),
        "fail" => Ok(RuleStage::Fail),
        _ => Err(format!(
            "stage 不支持: {stage_raw}，仅支持 base/trigger/confirm/risk/fail"
        )),
    }
}

fn parse_rule_tag_input(tag_raw: &str) -> Result<RuleTag, String> {
    match tag_raw.trim().to_ascii_lowercase().as_str() {
        "" | "normal" => Ok(RuleTag::Normal),
        "opportunity" => Ok(RuleTag::Opportunity),
        "rare" => Ok(RuleTag::Rare),
        _ => Err(format!(
            "tag 不支持: {tag_raw}，仅支持 normal/opportunity/rare"
        )),
    }
}

fn read_non_empty_owned(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn normalize_validation_points(raw: Option<f64>) -> Result<f64, String> {
    match raw {
        Some(value) if !value.is_finite() => Err("手动策略 points 非法".to_string()),
        Some(value) if value < 0.0 => Ok(-1.0),
        Some(_) | None => Ok(1.0),
    }
}

fn resolve_validation_seed_rule(
    import_rule_name_raw: &str,
    manual_strategy: Option<&RuleExpressionValidationManualStrategy>,
    when: Option<&str>,
    scope_way: Option<&str>,
    scope_windows: Option<usize>,
    all_rules: &[ScoreRule],
) -> Result<ValidationSeedRule, String> {
    let import_rule_name = import_rule_name_raw.trim();
    let import_rule = if import_rule_name.is_empty() {
        None
    } else {
        all_rules
            .iter()
            .find(|rule| rule.name.trim() == import_rule_name)
            .cloned()
    };

    let top_formula = read_non_empty_owned(when);
    let top_scope_way = read_non_empty_owned(scope_way);

    let manual_name =
        manual_strategy.and_then(|strategy| read_non_empty_owned(strategy.name.as_deref()));
    let manual_formula =
        manual_strategy.and_then(|strategy| read_non_empty_owned(strategy.when.as_deref()));
    let manual_explain =
        manual_strategy.and_then(|strategy| read_non_empty_owned(strategy.explain.as_deref()));
    let manual_scope_windows = manual_strategy.and_then(|strategy| strategy.scope_windows);
    let manual_points =
        normalize_validation_points(manual_strategy.and_then(|strategy| strategy.points))?;
    let manual_dist_points = manual_strategy
        .and_then(|strategy| strategy.dist_points.clone())
        .and_then(|items| if items.is_empty() { None } else { Some(items) });

    if import_rule
        .as_ref()
        .is_some_and(|rule| rule.kind == RuleKind::Combination)
        && top_formula.is_none()
        && manual_formula.is_none()
    {
        return Err("组合策略不能导入到单表达式验证，请直接在“策略回测”中验证".to_string());
    }

    let manual_scope_way = match manual_strategy
        .and_then(|strategy| read_non_empty_owned(strategy.scope_way.as_deref()))
    {
        Some(raw) => Some(parse_scope_way_input(&raw)?),
        None => None,
    };

    let manual_tag = match manual_strategy.and_then(|strategy| strategy.tag.as_deref()) {
        Some(raw) if !raw.trim().is_empty() => Some(parse_rule_tag_input(raw)?),
        _ => None,
    };

    if let Some(stage_raw) = manual_strategy.and_then(|strategy| strategy.stage.as_deref()) {
        if !stage_raw.trim().is_empty() {
            let _ = parse_rule_stage_input(stage_raw)?;
        }
    }

    let has_manual_override = manual_name.is_some()
        || manual_formula.is_some()
        || manual_scope_way.is_some()
        || manual_scope_windows.is_some()
        || manual_dist_points.is_some()
        || manual_explain.is_some()
        || manual_tag.is_some()
        || manual_strategy
            .and_then(|strategy| strategy.scene_name.as_deref())
            .map(str::trim)
            .is_some_and(|value| !value.is_empty())
        || manual_strategy
            .and_then(|strategy| strategy.stage.as_deref())
            .map(str::trim)
            .is_some_and(|value| !value.is_empty());

    if !import_rule_name.is_empty() && import_rule.is_none() && !has_manual_override {
        return Err(format!("未找到策略: {import_rule_name}"));
    }

    let formula = top_formula
        .or(manual_formula)
        .or_else(|| {
            import_rule
                .as_ref()
                .map(|rule| rule.when.trim().to_string())
        })
        .ok_or_else(|| "表达式不能为空".to_string())?;

    let resolved_scope_way = if let Some(raw) = top_scope_way {
        parse_scope_way_input(&raw)?
    } else if let Some(value) = manual_scope_way {
        value
    } else if let Some(rule) = import_rule.as_ref() {
        rule.scope_way
    } else {
        ScopeWay::Any
    };

    let resolved_scope_windows = scope_windows
        .or(manual_scope_windows)
        .or_else(|| import_rule.as_ref().map(|rule| rule.scope_windows))
        .unwrap_or(1)
        .max(1);

    if let ScopeWay::Consec(threshold) = resolved_scope_way {
        if resolved_scope_windows < threshold {
            return Err(format!(
                "scope_windows({resolved_scope_windows}) 不能小于 CONSEC 阈值 {threshold}"
            ));
        }
    }

    let rule_name = manual_name
        .or_else(|| {
            import_rule
                .as_ref()
                .map(|rule| rule.name.trim().to_string())
        })
        .or_else(|| read_non_empty_owned(Some(import_rule_name)))
        .unwrap_or_else(|| "manual_validation_rule".to_string());

    let rule_explain = manual_explain
        .or_else(|| {
            import_rule
                .as_ref()
                .map(|rule| rule.explain.trim().to_string())
        })
        .unwrap_or_else(|| format!("表达式验证策略: {rule_name}"));

    let points = manual_points;
    if !points.is_finite() {
        return Err("策略 points 非法".to_string());
    }

    let dist_points = manual_dist_points;

    let tag = manual_tag
        .or_else(|| import_rule.as_ref().map(|rule| rule.tag))
        .unwrap_or(RuleTag::Normal);

    let exclude_rule_name = if let Some(rule) = import_rule.as_ref() {
        Some(rule.name.clone())
    } else if all_rules.iter().any(|rule| rule.name.trim() == rule_name) {
        Some(rule_name.clone())
    } else {
        None
    };

    Ok(ValidationSeedRule {
        rule_name,
        rule_explain,
        scope_way: resolved_scope_way,
        scope_windows: resolved_scope_windows,
        formula,
        points,
        dist_points,
        tag,
        exclude_rule_name,
    })
}

fn load_rule_meta(source_path: &str) -> Result<(Vec<String>, HashMap<String, RuleMeta>), String> {
    let rules = ScoreRule::load_rules(source_path)?;
    let mut order = Vec::with_capacity(rules.len());
    let mut meta_map = HashMap::with_capacity(rules.len());

    for rule in rules {
        order.push(rule.name.clone());
        let when = match rule.kind {
            RuleKind::Single => rule.when.clone(),
            RuleKind::Combination => format_combination_rule_formula(&rule),
        };
        let points = rule.representative_points();
        meta_map.insert(
            rule.name,
            RuleMeta {
                when,
                explain: rule.explain,
                trigger_mode: scope_way_label(rule.scope_way),
                is_each: rule.kind == RuleKind::Single && matches!(rule.scope_way, ScopeWay::Each),
                points,
            },
        );
    }

    Ok((order, meta_map))
}

fn format_combination_rule_formula(rule: &ScoreRule) -> String {
    let conditions = rule
        .conditions
        .iter()
        .map(|condition| format!("{}: {}", condition.name, condition.when))
        .collect::<Vec<_>>()
        .join("；");
    let bonuses = rule
        .conditions
        .iter()
        .filter(|condition| condition.bonus_points != 0.0)
        .map(|condition| format!("{}: {:+}", condition.name, condition.bonus_points))
        .collect::<Vec<_>>()
        .join("；");
    let mut parts = vec![
        format!("组合条件：{conditions}"),
        format!(
            "命中数得分：{:?}",
            rule.points_by_hits.as_deref().unwrap_or_default()
        ),
    ];
    if !bonuses.is_empty() {
        parts.push(format!("额外加分：{bonuses}"));
    }
    parts.join("；")
}

fn load_rule_joint_validation_features(
    source_path: &str,
) -> Result<Vec<RuleJointValidationFeature>, String> {
    let rules = ScoreRule::load_rules(source_path)?;
    if rules.len() > RULE_JOINT_RIDGE_MAX_FEATURES {
        return Err(format!(
            "整体岭回归最多支持{}条策略，当前为{}条",
            RULE_JOINT_RIDGE_MAX_FEATURES,
            rules.len()
        ));
    }

    rules
        .into_iter()
        .map(|rule| {
            let representative_points = rule.representative_points();
            let score_scale = representative_points.abs();
            if !score_scale.is_finite() || score_scale <= VALIDATION_EPS {
                return Err(format!("策略 {} 缺少可归一化的有效分数", rule.name));
            }
            Ok(RuleJointValidationFeature {
                rule_name: rule.name,
                explain: rule.explain,
                current_points: representative_points,
                score_scale,
            })
        })
        .collect()
}

fn store_rule_joint_validation_from_rows(
    source_path: &str,
    params: &RuleLayerBacktestRunParams,
    summary_rows: Vec<ScoreSummary>,
) -> Option<String> {
    let features = load_rule_joint_validation_features(source_path).ok()?;
    store_rule_joint_validation_session(RuleJointValidationSession {
        created_at: Instant::now(),
        source_path: source_path.to_string(),
        params: params.clone(),
        summary_rows: Arc::new(summary_rows),
        features,
    })
    .ok()
}

fn load_scene_options(source_path: &str) -> Result<Vec<String>, String> {
    let scenes = ScoreScene::load_scenes(source_path)?;
    Ok(scenes.into_iter().map(|scene| scene.name).collect())
}

fn load_scene_rule_name_sets(
    source_path: &str,
) -> Result<HashMap<String, HashSet<String>>, String> {
    let rules = ScoreRule::load_rules(source_path)?;
    let mut out: HashMap<String, HashSet<String>> = HashMap::new();

    for rule in rules {
        out.entry(rule.scene_name).or_default().insert(rule.name);
    }

    Ok(out)
}

fn query_overview(conn: &Connection) -> Result<StrategyOverviewPayload, String> {
    let sql = r#"
        WITH per_stock_day AS (
            SELECT
                trade_date,
                ts_code,
                COUNT(*) AS hit_rule_count
            FROM rule_details
            WHERE rule_score IS NOT NULL
              AND ABS(rule_score) > 1e-12
            GROUP BY 1, 2
        ),
        daily_level AS (
            SELECT
                trade_date,
                AVG(hit_rule_count) AS day_level
            FROM per_stock_day
            GROUP BY 1
        ),
        overall_level AS (
            SELECT AVG(hit_rule_count) AS avg_level
            FROM per_stock_day
        )
        SELECT
            d.trade_date,
            d.day_level,
            o.avg_level,
            d.day_level - o.avg_level AS delta_level,
            CASE
                WHEN d.day_level IS NULL OR o.avg_level IS NULL THEN NULL
                ELSE d.day_level > o.avg_level
            END AS above_avg
        FROM daily_level AS d
        CROSS JOIN overall_level AS o
        ORDER BY d.trade_date ASC
    "#;

    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| format!("预编译总体统计 SQL 失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("执行总体统计 SQL 失败: {e}"))?;

    let mut items = Vec::new();
    let mut latest_trade_date = None;
    let mut average_level = None;

    while let Some(row) = rows.next().map_err(|e| format!("读取总体统计失败: {e}"))? {
        let trade_date: String = row.get(0).map_err(|e| format!("读取交易日失败: {e}"))?;
        let avg_level: Option<f64> = row.get(2).map_err(|e| format!("读取平均水平失败: {e}"))?;

        latest_trade_date = Some(trade_date.clone());
        average_level = avg_level;
        items.push(StrategyHeatmapCell {
            trade_date,
            day_level: row.get(1).map_err(|e| format!("读取当日水平失败: {e}"))?,
            avg_level,
            delta_level: row.get(3).map_err(|e| format!("读取差值失败: {e}"))?,
            above_avg: row.get(4).map_err(|e| format!("读取强弱标记失败: {e}"))?,
        });
    }

    Ok(StrategyOverviewPayload {
        items: Some(items),
        latest_trade_date,
        average_level,
    })
}

fn query_each_rule_medians(
    conn: &Connection,
    meta_map: &HashMap<String, RuleMeta>,
) -> Result<HashMap<(String, String), f64>, String> {
    let mut out = HashMap::new();

    for (rule_name, meta) in meta_map {
        if !meta.is_each || meta.points == 0.0 {
            continue;
        }

        let mut stmt = conn
            .prepare(
                r#"
                SELECT
                    trade_date,
                    QUANTILE_CONT(ABS(rule_score / ?), 0.5) AS median_trigger_count
                FROM rule_details
                WHERE rule_name = ?
                  AND rule_score IS NOT NULL
                  AND ABS(rule_score) > 1e-12
                GROUP BY 1
                ORDER BY 1 ASC
                "#,
            )
            .map_err(|e| format!("预编译 EACH 中位触发次数 SQL 失败: {e}"))?;
        let mut rows = stmt
            .query(params![meta.points, rule_name])
            .map_err(|e| format!("执行 EACH 中位触发次数 SQL 失败: {e}"))?;

        while let Some(row) = rows
            .next()
            .map_err(|e| format!("读取 EACH 中位触发次数失败: {e}"))?
        {
            let trade_date: String = row.get(0).map_err(|e| format!("读取交易日失败: {e}"))?;
            let median: Option<f64> = row
                .get(1)
                .map_err(|e| format!("读取中位触发次数失败: {e}"))?;
            if let Some(value) = median {
                out.insert((trade_date, rule_name.clone()), value);
            }
        }
    }

    Ok(out)
}

fn query_daily_rows(
    conn: &Connection,
    rule_order: &[String],
    meta_map: &HashMap<String, RuleMeta>,
) -> Result<Vec<StrategyDailyRow>, String> {
    let each_medians = query_each_rule_medians(conn, meta_map)?;
    let mut sample_stmt = conn
        .prepare(
            r#"
        SELECT
            trade_date,
            COUNT(*) AS sample_count
        FROM score_summary
        GROUP BY 1
        ORDER BY 1 ASC
        "#,
        )
        .map_err(|e| format!("预编译日度样本数 SQL 失败: {e}"))?;
    let mut sample_rows = sample_stmt
        .query([])
        .map_err(|e| format!("执行日度样本数 SQL 失败: {e}"))?;

    let mut daily_samples = Vec::new();
    while let Some(row) = sample_rows
        .next()
        .map_err(|e| format!("读取日度样本数失败: {e}"))?
    {
        let trade_date: String = row.get(0).map_err(|e| format!("读取交易日失败: {e}"))?;
        let sample_count: i64 = row.get(1).map_err(|e| format!("读取样本数失败: {e}"))?;
        daily_samples.push((trade_date, sample_count));
    }

    let sql = r#"
        WITH daily_rank_bounds AS (
            SELECT
                trade_date,
                MAX(rank) AS max_rank
            FROM score_summary
            GROUP BY 1
        ),
        triggered_rule_rows AS (
            SELECT *
            FROM rule_details
            WHERE rule_score IS NOT NULL
              AND ABS(rule_score) > 1e-12
        )
        SELECT
            d.trade_date,
            d.rule_name,
            COUNT(*) AS trigger_count,
            SUM(
                CASE
                    WHEN s.rank IS NOT NULL
                      AND b.max_rank IS NOT NULL
                      AND b.max_rank > 0
                    THEN d.rule_score * CAST((b.max_rank + 1 - s.rank) AS DOUBLE) / CAST(b.max_rank AS DOUBLE)
                    ELSE 0
                END
            ) AS contribution_score,
            SUM(CASE WHEN s.rank <= ? THEN 1 ELSE 0 END) AS top100_trigger_count,
            MIN(s.rank) AS best_rank
        FROM triggered_rule_rows AS d
        LEFT JOIN score_summary AS s
          ON s.ts_code = d.ts_code
         AND s.trade_date = d.trade_date
        LEFT JOIN daily_rank_bounds AS b
          ON b.trade_date = d.trade_date
        GROUP BY 1, 2
        ORDER BY d.trade_date ASC, d.rule_name ASC
    "#;

    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| format!("预编译日度策略统计 SQL 失败: {e}"))?;
    let mut rows = stmt
        .query(params![TOP_RANK_THRESHOLD])
        .map_err(|e| format!("执行日度策略统计 SQL 失败: {e}"))?;

    let mut out = Vec::new();
    let mut daily_agg_map: HashMap<(String, String), RuleDayAgg> = HashMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取日度策略统计失败: {e}"))?
    {
        let trade_date: String = row.get(0).map_err(|e| format!("读取交易日失败: {e}"))?;
        let rule_name: String = row.get(1).map_err(|e| format!("读取策略名失败: {e}"))?;
        daily_agg_map.insert(
            (trade_date, rule_name),
            RuleDayAgg {
                trigger_count: row.get(2).map_err(|e| format!("读取触发次数失败: {e}"))?,
                contribution_score: row
                    .get::<usize, Option<f64>>(3)
                    .map_err(|e| format!("读取策略贡献度失败: {e}"))?
                    .unwrap_or(0.0),
                top100_trigger_count: row
                    .get::<usize, Option<i64>>(4)
                    .map_err(|e| format!("读取前100触发次数失败: {e}"))?
                    .unwrap_or(0),
                best_rank: row.get(5).map_err(|e| format!("读取最优排名失败: {e}"))?,
            },
        );
    }

    for (trade_date, sample_count) in daily_samples {
        for rule_name in rule_order {
            let agg = daily_agg_map
                .get(&(trade_date.clone(), rule_name.clone()))
                .cloned()
                .unwrap_or_default();
            let meta = meta_map.get(rule_name);
            let contribution_score = if agg.trigger_count > 0 {
                Some(agg.contribution_score)
            } else {
                None
            };
            let contribution_per_trigger =
                contribution_score.map(|score| score / agg.trigger_count as f64);
            let coverage = if sample_count > 0 {
                Some(agg.trigger_count as f64 / sample_count as f64)
            } else {
                None
            };

            out.push(StrategyDailyRow {
                median_trigger_count: each_medians
                    .get(&(trade_date.clone(), rule_name.clone()))
                    .copied(),
                trade_date: trade_date.clone(),
                rule_name: rule_name.clone(),
                trigger_mode: meta.map(|v| v.trigger_mode.clone()),
                sample_count: Some(sample_count),
                trigger_count: Some(agg.trigger_count),
                coverage,
                contribution_score,
                contribution_per_trigger,
                top100_trigger_count: Some(agg.top100_trigger_count),
                best_rank: agg.best_rank,
            });
        }
    }

    Ok(out)
}

fn resolve_strategy_name(requested: Option<String>, strategy_options: &[String]) -> Option<String> {
    let requested = requested
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    if let Some(name) = requested {
        if strategy_options.iter().any(|item| item == &name) {
            return Some(name);
        }
    }
    None
}

fn resolve_scene_name(requested: Option<String>, scene_options: &[String]) -> Option<String> {
    let requested = requested
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if let Some(scene_name) = requested {
        if scene_options.iter().any(|item| item == &scene_name) {
            return Some(scene_name);
        }
    }
    scene_options.first().cloned()
}

fn resolve_analysis_trade_date(
    requested: Option<String>,
    trade_date_options: &[String],
) -> Option<String> {
    let requested = requested
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    if let Some(trade_date) = requested {
        if trade_date_options.iter().any(|item| item == &trade_date) {
            return Some(trade_date);
        }
    }
    trade_date_options.first().cloned()
}

fn query_scene_trade_date_options(conn: &Connection) -> Result<Vec<String>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT DISTINCT trade_date
            FROM scene_details
            ORDER BY trade_date DESC
            "#,
        )
        .map_err(|e| format!("预编译 scene 交易日 SQL 失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("执行 scene 交易日 SQL 失败: {e}"))?;

    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取 scene 交易日失败: {e}"))?
    {
        let trade_date: String = row.get(0).map_err(|e| format!("读取交易日字段失败: {e}"))?;
        if !trade_date.trim().is_empty() {
            out.push(trade_date);
        }
    }

    Ok(out)
}

fn query_scene_stage_rows(
    conn: &Connection,
    scene_name: &str,
    trade_date: &str,
) -> Result<(Vec<SceneStageRow>, i64, i64), String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT
                COALESCE(NULLIF(stage, ''), 'none') AS stage,
                COUNT(*) AS sample_count
            FROM scene_details
            WHERE trade_date = ?
              AND scene_name = ?
            GROUP BY 1
            "#,
        )
        .map_err(|e| format!("预编译 scene 阶段统计 SQL 失败: {e}"))?;
    let mut rows = stmt
        .query(params![trade_date, scene_name])
        .map_err(|e| format!("执行 scene 阶段统计 SQL 失败: {e}"))?;

    let mut stage_count_map: HashMap<String, i64> = HashMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取 scene 阶段统计失败: {e}"))?
    {
        let stage: String = row.get(0).map_err(|e| format!("读取阶段字段失败: {e}"))?;
        let sample_count: i64 = row.get(1).map_err(|e| format!("读取阶段数量失败: {e}"))?;
        let normalized_stage = stage.trim().to_ascii_lowercase();
        stage_count_map.insert(normalized_stage, sample_count);
    }

    let total_sample_count: i64 = stage_count_map.values().sum();
    let none_count = stage_count_map.get("none").copied().unwrap_or(0);
    let covered_count = (total_sample_count - none_count).max(0);

    let mut rows_out = Vec::new();
    let stage_order = ["trigger", "confirm", "observe", "fail", "none"];

    for stage in stage_order {
        let sample_count = stage_count_map.remove(stage).unwrap_or(0);
        rows_out.push(SceneStageRow {
            stage: stage.to_string(),
            sample_count,
            stage_ratio_in_scene: if total_sample_count > 0 {
                Some(sample_count as f64 / total_sample_count as f64)
            } else {
                None
            },
        });
    }

    let mut remain_stages = stage_count_map.into_iter().collect::<Vec<_>>();
    remain_stages.sort_by(|a, b| a.0.cmp(&b.0));
    for (stage, sample_count) in remain_stages {
        rows_out.push(SceneStageRow {
            stage,
            sample_count,
            stage_ratio_in_scene: if total_sample_count > 0 {
                Some(sample_count as f64 / total_sample_count as f64)
            } else {
                None
            },
        });
    }

    Ok((rows_out, total_sample_count, covered_count))
}

fn query_rule_contribution_by_date(
    conn: &Connection,
    trade_date: &str,
) -> Result<HashMap<String, f64>, String> {
    let sql = r#"
        WITH daily_rank_bounds AS (
            SELECT
                trade_date,
                MAX(rank) AS max_rank
            FROM score_summary
            WHERE trade_date = ?
            GROUP BY 1
        ),
        triggered_rule_rows AS (
            SELECT *
            FROM rule_details
            WHERE trade_date = ?
              AND rule_score IS NOT NULL
              AND ABS(rule_score) > 1e-12
        )
        SELECT
            d.rule_name,
            SUM(
                CASE
                    WHEN s.rank IS NOT NULL
                      AND b.max_rank IS NOT NULL
                      AND b.max_rank > 0
                    THEN d.rule_score * CAST((b.max_rank + 1 - s.rank) AS DOUBLE) / CAST(b.max_rank AS DOUBLE)
                    ELSE 0
                END
            ) AS contribution_score
        FROM triggered_rule_rows AS d
        LEFT JOIN score_summary AS s
          ON s.ts_code = d.ts_code
         AND s.trade_date = d.trade_date
        LEFT JOIN daily_rank_bounds AS b
          ON b.trade_date = d.trade_date
        GROUP BY 1
    "#;

    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| format!("预编译 scene 规则贡献度 SQL 失败: {e}"))?;
    let mut rows = stmt
        .query(params![trade_date, trade_date])
        .map_err(|e| format!("执行 scene 规则贡献度 SQL 失败: {e}"))?;

    let mut out = HashMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取 scene 规则贡献度失败: {e}"))?
    {
        let rule_name: String = row.get(0).map_err(|e| format!("读取规则名失败: {e}"))?;
        let contribution_score = row
            .get::<usize, Option<f64>>(1)
            .map_err(|e| format!("读取规则贡献度失败: {e}"))?
            .unwrap_or(0.0);
        out.insert(rule_name, contribution_score);
    }

    Ok(out)
}

fn build_scene_contribution_summary(
    scene_total_sample_count: i64,
    scene_covered_count: i64,
    scene_rule_names: Option<&HashSet<String>>,
    contribution_by_rule: &HashMap<String, f64>,
) -> SceneContributionSummary {
    let scene_rule_contribution_score = scene_rule_names.map(|rule_names| {
        contribution_by_rule
            .iter()
            .filter(|(rule_name, _)| rule_names.contains(*rule_name))
            .map(|(_, score)| *score)
            .sum::<f64>()
    });
    let all_rule_contribution_score = if contribution_by_rule.is_empty() {
        None
    } else {
        Some(contribution_by_rule.values().sum::<f64>())
    };
    let scene_rule_contribution_ratio =
        match (scene_rule_contribution_score, all_rule_contribution_score) {
            (Some(scene_score), Some(all_score)) if all_score.abs() > 1e-12 => {
                Some(scene_score / all_score)
            }
            _ => None,
        };

    SceneContributionSummary {
        scene_covered_count,
        scene_total_sample_count,
        scene_coverage_ratio: if scene_total_sample_count > 0 {
            Some(scene_covered_count as f64 / scene_total_sample_count as f64)
        } else {
            None
        },
        scene_rule_contribution_score,
        all_rule_contribution_score,
        scene_rule_contribution_ratio,
    }
}

fn build_chart(strategy_rows: &[StrategyDailyRow]) -> StrategyChartPayload {
    let items = strategy_rows
        .iter()
        .map(|row| StrategyChartPoint {
            trade_date: row.trade_date.clone(),
            trigger_count: row.trigger_count,
            top100_trigger_count: row.top100_trigger_count,
            coverage: row.coverage,
        })
        .collect();

    StrategyChartPayload { items: Some(items) }
}

fn query_triggered_stocks(
    conn: &Connection,
    source_path: &str,
    rule_name: &str,
    trade_date: &str,
) -> Result<Vec<TriggeredStockRow>, String> {
    let name_map = build_name_map(source_path).unwrap_or_default();
    let concept_map = build_concepts_map(source_path).unwrap_or_default();
    let mut stmt = conn
        .prepare(
            r#"
            SELECT
                s.rank,
                d.ts_code,
                s.total_score,
                d.rule_score
            FROM rule_details AS d
            LEFT JOIN score_summary AS s
              ON s.ts_code = d.ts_code
             AND s.trade_date = d.trade_date
            WHERE d.trade_date = ?
              AND d.rule_name = ?
              AND d.rule_score IS NOT NULL
              AND ABS(d.rule_score) > 1e-12
            ORDER BY s.rank ASC NULLS LAST, d.ts_code ASC
            "#,
        )
        .map_err(|e| format!("预编译触发股票 SQL 失败: {e}"))?;
    let mut rows = stmt
        .query(params![trade_date, rule_name])
        .map_err(|e| format!("执行触发股票 SQL 失败: {e}"))?;

    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(|e| format!("读取触发股票失败: {e}"))? {
        let ts_code: String = row.get(1).map_err(|e| format!("读取股票代码失败: {e}"))?;
        out.push(TriggeredStockRow {
            rank: row.get(0).map_err(|e| format!("读取排名失败: {e}"))?,
            total_score: row.get(2).map_err(|e| format!("读取总分失败: {e}"))?,
            rule_score: row.get(3).map_err(|e| format!("读取策略得分失败: {e}"))?,
            name: name_map.get(&ts_code).cloned(),
            concept: concept_map.get(&ts_code).cloned(),
            ts_code,
        });
    }

    Ok(out)
}

pub fn get_strategy_triggered_stocks(
    source_path: String,
    strategy_name: String,
    analysis_trade_date: String,
) -> Result<Vec<TriggeredStockRow>, String> {
    let strategy_name = strategy_name.trim();
    let analysis_trade_date = analysis_trade_date.trim();
    if strategy_name.is_empty() || analysis_trade_date.is_empty() {
        return Ok(Vec::new());
    }

    let conn = open_result_conn(&source_path)?;
    query_triggered_stocks(&conn, &source_path, strategy_name, analysis_trade_date)
}

pub fn get_strategy_statistics_detail(
    source_path: String,
    strategy_name: String,
    analysis_trade_date: Option<String>,
) -> Result<StrategyStatisticsDetailData, String> {
    let strategy_name = strategy_name.trim().to_string();
    if strategy_name.is_empty() {
        return Err("策略名不能为空".to_string());
    }

    let conn = open_result_conn(&source_path)?;
    let (rule_order, meta_map) = load_rule_meta(&source_path)?;
    let detail_rows_all = query_daily_rows(&conn, &rule_order, &meta_map)?;
    let strategy_rows = detail_rows_all
        .iter()
        .filter(|row| row.rule_name == strategy_name)
        .cloned()
        .collect::<Vec<_>>();

    let mut analysis_trade_date_options = strategy_rows
        .iter()
        .filter(|row| row.trigger_count.unwrap_or(0) > 0)
        .map(|row| row.trade_date.clone())
        .collect::<Vec<_>>();
    analysis_trade_date_options.sort();
    analysis_trade_date_options.dedup();
    analysis_trade_date_options.reverse();
    if analysis_trade_date_options.is_empty() {
        analysis_trade_date_options = strategy_rows
            .iter()
            .map(|row| row.trade_date.clone())
            .collect::<Vec<_>>();
        analysis_trade_date_options.sort();
        analysis_trade_date_options.dedup();
        analysis_trade_date_options.reverse();
    }
    let resolved_analysis_trade_date =
        resolve_analysis_trade_date(analysis_trade_date, &analysis_trade_date_options);
    let selected_daily_row = resolved_analysis_trade_date
        .as_ref()
        .and_then(|trade_date| {
            strategy_rows
                .iter()
                .find(|row| row.trade_date == *trade_date)
                .cloned()
        });
    let triggered_stocks = if let Some(trade_date) = resolved_analysis_trade_date.as_deref() {
        query_triggered_stocks(&conn, &source_path, &strategy_name, trade_date)?
    } else {
        Vec::new()
    };

    Ok(StrategyStatisticsDetailData {
        strategy_name,
        analysis_trade_date_options,
        resolved_analysis_trade_date,
        selected_daily_row,
        chart: Some(build_chart(&strategy_rows)),
        triggered_stocks,
    })
}

pub fn get_strategy_statistics_page(
    source_path: String,
    strategy_name: Option<String>,
    analysis_trade_date: Option<String>,
) -> Result<StrategyStatisticsPageData, String> {
    let conn = open_result_conn(&source_path)?;
    let overview = query_overview(&conn)?;
    let (strategy_options, meta_map) = load_rule_meta(&source_path)?;
    let detail_rows_all = query_daily_rows(&conn, &strategy_options, &meta_map)?;

    let resolved_strategy_name = resolve_strategy_name(strategy_name, &strategy_options);

    let strategy_rows: Vec<StrategyDailyRow> =
        if let Some(selected_name) = resolved_strategy_name.as_ref() {
            detail_rows_all
                .iter()
                .filter(|row| row.rule_name == *selected_name)
                .cloned()
                .collect()
        } else {
            Vec::new()
        };

    let mut analysis_trade_date_options: Vec<String> = detail_rows_all
        .iter()
        .filter(|row| row.trigger_count.unwrap_or(0) > 0)
        .map(|row| row.trade_date.clone())
        .collect();
    analysis_trade_date_options.sort();
    analysis_trade_date_options.dedup();
    analysis_trade_date_options.reverse();

    if analysis_trade_date_options.is_empty() {
        analysis_trade_date_options = detail_rows_all
            .iter()
            .map(|row| row.trade_date.clone())
            .collect();
        analysis_trade_date_options.sort();
        analysis_trade_date_options.dedup();
        analysis_trade_date_options.reverse();
    }

    let resolved_analysis_trade_date =
        resolve_analysis_trade_date(analysis_trade_date, &analysis_trade_date_options);

    let triggered_stocks = if let (Some(rule_name), Some(trade_date)) = (
        resolved_strategy_name.as_deref(),
        resolved_analysis_trade_date.as_deref(),
    ) {
        query_triggered_stocks(&conn, &source_path, rule_name, trade_date)?
    } else {
        Vec::new()
    };

    let mut detail_rows = detail_rows_all;
    detail_rows.sort_by(|a, b| {
        b.trade_date
            .cmp(&a.trade_date)
            .then_with(|| {
                b.trigger_count
                    .unwrap_or(0)
                    .cmp(&a.trigger_count.unwrap_or(0))
            })
            .then_with(|| a.rule_name.cmp(&b.rule_name))
    });

    Ok(StrategyStatisticsPageData {
        overview: Some(overview),
        detail_rows: Some(detail_rows),
        strategy_options: Some(strategy_options),
        resolved_strategy_name,
        analysis_trade_date_options: Some(analysis_trade_date_options),
        resolved_analysis_trade_date,
        chart: Some(build_chart(&strategy_rows)),
        triggered_stocks: Some(triggered_stocks),
    })
}

pub fn get_scene_statistics_page(
    source_path: String,
    scene_name: Option<String>,
    analysis_trade_date: Option<String>,
) -> Result<SceneStatisticsPageData, String> {
    let conn = open_result_conn(&source_path)?;
    let scene_options = load_scene_options(&source_path)?;
    let resolved_scene_name = resolve_scene_name(scene_name, &scene_options);
    let analysis_trade_date_options = query_scene_trade_date_options(&conn)?;
    let resolved_analysis_trade_date =
        resolve_analysis_trade_date(analysis_trade_date, &analysis_trade_date_options);

    let mut stage_rows = Vec::new();
    let mut summary = None;

    if let (Some(selected_scene_name), Some(selected_trade_date)) = (
        resolved_scene_name.as_deref(),
        resolved_analysis_trade_date.as_deref(),
    ) {
        let (next_stage_rows, total_sample_count, covered_count) =
            query_scene_stage_rows(&conn, selected_scene_name, selected_trade_date)?;
        stage_rows = next_stage_rows;

        let scene_rule_name_sets = load_scene_rule_name_sets(&source_path)?;
        let contribution_by_rule = query_rule_contribution_by_date(&conn, selected_trade_date)?;
        summary = Some(build_scene_contribution_summary(
            total_sample_count,
            covered_count,
            scene_rule_name_sets.get(selected_scene_name),
            &contribution_by_rule,
        ));
    }

    Ok(SceneStatisticsPageData {
        scene_options: Some(scene_options),
        resolved_scene_name,
        analysis_trade_date_options: Some(analysis_trade_date_options),
        resolved_analysis_trade_date,
        stage_rows: Some(stage_rows),
        summary,
    })
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

fn expand_unknown_config(config: &RuleValidationUnknownConfig) -> Result<Vec<f64>, String> {
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
            TokenKind::Ident(name) => {
                if let Some(replacement) = replace_map.get(name.as_str()) {
                    out.push_str(replacement);
                } else {
                    out.push_str(&formula[token.start..token.end]);
                }
            }
            TokenKind::Eof => {}
            _ => out.push_str(&formula[token.start..token.end]),
        }
        cursor = token.end;
    }

    if cursor < formula.len() {
        out.push_str(&formula[cursor..]);
    }

    out
}

fn build_validation_variants(
    formula: &str,
    unknown_configs: &[RuleValidationUnknownConfig],
) -> Result<Vec<ValidationVariant>, String> {
    let formula = formula.trim();
    if formula.is_empty() {
        return Err("表达式不能为空".to_string());
    }

    let mut unknown_groups = Vec::<(String, Vec<f64>)>::new();
    let mut total_combinations = 1usize;
    let mut seen = HashSet::new();

    for config in unknown_configs {
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

    let mut out = Vec::new();
    let mut assignments = Vec::<(String, f64)>::new();

    fn walk_variants(
        index: usize,
        unknown_groups: &[(String, Vec<f64>)],
        assignments: &mut Vec<(String, f64)>,
        formula: &str,
        out: &mut Vec<ValidationVariant>,
    ) {
        if index >= unknown_groups.len() {
            let mut sorted = assignments.clone();
            sorted.sort_by(|left, right| {
                right
                    .0
                    .len()
                    .cmp(&left.0.len())
                    .then_with(|| left.0.cmp(&right.0))
            });
            let unknown_values = sorted
                .iter()
                .map(|(name, value)| RuleValidationUnknownValue {
                    name: name.clone(),
                    value: *value,
                })
                .collect::<Vec<_>>();
            let replaced_formula = replace_validation_unknowns(formula, &sorted);
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
                combo_key,
                combo_label,
                formula: replaced_formula,
                unknown_values,
            });
            return;
        }

        let (name, values) = &unknown_groups[index];
        for value in values {
            assignments.push((name.clone(), *value));
            walk_variants(index + 1, unknown_groups, assignments, formula, out);
            assignments.pop();
        }
    }

    walk_variants(0, &unknown_groups, &mut assignments, formula, &mut out);

    if out.is_empty() {
        out.push(ValidationVariant {
            combo_key: "validation_combo_001".to_string(),
            combo_label: "默认参数".to_string(),
            formula: formula.to_string(),
            unknown_values: Vec::new(),
        });
    }

    Ok(out)
}

fn estimate_rule_warmup(
    stmts: &Stmts,
    scope_way: ScopeWay,
    scope_windows: usize,
) -> Result<usize, String> {
    let expression_need = estimate_expression_warmup(stmts)?;

    let scope_extra = match scope_way {
        ScopeWay::Last => 0,
        ScopeWay::Any | ScopeWay::Each | ScopeWay::Recent => scope_windows.saturating_sub(1),
        ScopeWay::Consec(threshold) => scope_windows
            .saturating_sub(1)
            .max(threshold.saturating_sub(1)),
    };

    Ok(expression_need + scope_extra)
}

fn build_validation_cached_rule(
    rule_name: String,
    scope_way: ScopeWay,
    scope_windows: usize,
    points: f64,
    dist_points: Option<Vec<crate::data::DistPoint>>,
    tag: crate::data::RuleTag,
    formula: &str,
) -> Result<CachedRule, String> {
    let stmts = parse_expression_program(formula)
        .map_err(|e| format!("表达式解析错误在{}:{}", e.idx, e.msg))?;
    validate_expression_functions(&stmts)?;
    let assigned_names = collect_assigned_names_from_expr_program(&stmts);

    Ok(CachedRule {
        name: rule_name,
        scope_windows,
        scope_way,
        points,
        dist_points,
        max_points: None,
        tag,
        when_src: formula.to_string(),
        when_ast: stmts,
        assigned_names,
        combination: None,
    })
}

fn collect_validation_assigned_names(stmts: &Stmts) -> Vec<String> {
    let mut assigned = HashSet::new();
    for stmt in &stmts.item {
        if let Stmt::Assign { name, .. } = stmt {
            assigned.insert(name.clone());
        }
    }

    let mut out = assigned.into_iter().collect::<Vec<_>>();
    out.sort();
    out
}

fn collect_rule_validation_runtime_keys(combos: &[PreparedValidationCombo]) -> HashSet<String> {
    let programs = combos
        .iter()
        .map(|combo| &combo.cached_rule.when_ast)
        .collect::<Vec<_>>();
    let cyq_chen_keys = cyq_chen_runtime_key_names();
    let injected_keys = RULE_VALIDATION_INJECTED_RUNTIME_KEYS
        .iter()
        .copied()
        .chain(cyq_chen_keys)
        .collect::<Vec<_>>();

    collect_runtime_keys_from_expr_programs(
        &programs,
        RuntimeKeyCollectOptions {
            always_keys: &[],
            injected_keys: &injected_keys,
            aliases: &RULE_VALIDATION_RUNTIME_ALIASES,
        },
    )
}

fn collect_rule_validation_cyq_chen_runtime_keys(
    combos: &[PreparedValidationCombo],
) -> HashSet<String> {
    let programs = combos
        .iter()
        .map(|combo| &combo.cached_rule.when_ast)
        .collect::<Vec<_>>();
    collect_used_cyq_chen_runtime_keys(&programs)
}

fn prepare_validation_combo(
    seed_rule: &ValidationSeedRule,
    variant: ValidationVariant,
) -> Result<PreparedValidationCombo, String> {
    let cached_rule = build_validation_cached_rule(
        variant.combo_key.clone(),
        seed_rule.scope_way,
        seed_rule.scope_windows,
        seed_rule.points,
        seed_rule.dist_points.clone(),
        seed_rule.tag,
        &variant.formula,
    )?;
    let assigned_names = collect_validation_assigned_names(&cached_rule.when_ast);

    Ok(PreparedValidationCombo {
        variant,
        cached_rule,
        assigned_names,
    })
}

fn build_validation_execution_plan(
    source_path: &str,
    start_date: &str,
    end_date: &str,
    seed_rule: &ValidationSeedRule,
    variants: Vec<ValidationVariant>,
) -> Result<ValidationExecutionPlan, String> {
    let mut max_warmup_need = 0usize;
    let mut combos = Vec::with_capacity(variants.len());

    for variant in variants {
        let combo = prepare_validation_combo(seed_rule, variant)?;
        max_warmup_need = max_warmup_need.max(estimate_rule_warmup(
            &combo.cached_rule.when_ast,
            combo.cached_rule.scope_way,
            combo.cached_rule.scope_windows,
        )?);
        combos.push(combo);
    }

    let need_rows = calc_query_need_rows(source_path, max_warmup_need, start_date, end_date)?;
    let query_start_date = calc_query_start_date(source_path, max_warmup_need, start_date)?;
    Ok(ValidationExecutionPlan {
        combos,
        need_rows,
        query_start_date,
    })
}

fn snapshot_runtime_values(runtime: &Runtime, names: &[String]) -> Vec<(String, Value)> {
    names
        .iter()
        .filter_map(|name| {
            runtime
                .vars
                .get(name)
                .cloned()
                .map(|value| (name.clone(), value))
        })
        .collect()
}

fn restore_runtime_values(runtime: &mut Runtime, values: &[(String, Value)]) {
    for (name, value) in values {
        runtime.vars.insert(name.clone(), value.clone());
    }
}

fn build_validation_date_score_map(
    trade_dates: &[String],
    keep_from: usize,
    scores: &[f64],
    triggered_flags: &[bool],
    rule_points: f64,
) -> Option<HashMap<String, f64>> {
    let min_len = usize::min(
        trade_dates.len(),
        usize::min(scores.len(), triggered_flags.len()),
    );
    if keep_from >= min_len {
        return None;
    }

    let mut date_score_map = HashMap::new();
    for index in keep_from..min_len {
        let Some(score) =
            normalize_validation_trigger_score(scores[index], triggered_flags[index], rule_points)
        else {
            continue;
        };
        date_score_map.insert(trade_dates[index].clone(), score);
    }

    if date_score_map.is_empty() {
        None
    } else {
        Some(date_score_map)
    }
}

fn validation_combos_use_rank_score(combos: &[PreparedValidationCombo]) -> bool {
    combos.iter().any(|combo| {
        expr_program_uses_runtime_key(&combo.cached_rule.when_ast, "RANK")
            || expr_program_uses_runtime_key(&combo.cached_rule.when_ast, "SCORE")
    })
}

#[derive(Debug, Clone, Copy, Default)]
struct ValidationRankScoreInfo {
    rank: Option<f64>,
    score: Option<f64>,
}

fn load_validation_rank_score_series_map(
    source_path: &str,
    start_date: &str,
    end_date: &str,
) -> HashMap<String, HashMap<String, ValidationRankScoreInfo>> {
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
        SELECT ts_code, trade_date, rank, total_score
        FROM score_summary
        WHERE trade_date >= ? AND trade_date <= ?
        "#,
    ) else {
        return HashMap::new();
    };
    let Ok(mut rows) = stmt.query(params![start_date, end_date]) else {
        return HashMap::new();
    };

    let mut out: HashMap<String, HashMap<String, ValidationRankScoreInfo>> = HashMap::new();
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
        let score = row.get::<_, Option<f64>>(3).ok().flatten();
        out.entry(ts_code)
            .or_default()
            .insert(trade_date, ValidationRankScoreInfo { rank, score });
    }

    out
}

fn inject_validation_rank_score_series(
    row_data: &mut crate::data::RowData,
    ts_code: &str,
    rank_score_series_map: &HashMap<String, HashMap<String, ValidationRankScoreInfo>>,
) -> Result<(), String> {
    let len = row_data.trade_dates.len();
    let mut rank_series = vec![None; len];
    let mut score_series = vec![None; len];

    if let Some(date_to_values) = rank_score_series_map.get(ts_code) {
        for (index, trade_date) in row_data.trade_dates.iter().enumerate() {
            if let Some(values) = date_to_values.get(trade_date).copied() {
                rank_series[index] = values.rank;
                score_series[index] = values.score;
            }
        }
    }

    row_data.cols.insert("RANK".to_string(), rank_series);
    row_data.cols.insert("SCORE".to_string(), score_series);
    row_data.validate()
}

fn evaluate_validation_combos_for_ts_code(
    reader: &mut DataReader,
    cyq_chen_injector: &CyqChenFieldInjector,
    ts_code: &str,
    stock_adj_type: &str,
    start_date: &str,
    end_date: &str,
    need_rows: usize,
    st_list: &HashSet<String>,
    total_share_map: &HashMap<String, f64>,
    rank_score_series_map: &HashMap<String, HashMap<String, ValidationRankScoreInfo>>,
    needs_rank_score: bool,
    combos: &[PreparedValidationCombo],
) -> Result<ValidationTsCodeEvaluation, String> {
    let mut row_data = reader.load_one_tail_rows(ts_code, stock_adj_type, end_date, need_rows)?;
    let _ = cyq_chen_injector.inject(&mut row_data, ts_code);
    inject_stock_extra_fields(
        &mut row_data,
        ts_code,
        st_list.contains(ts_code),
        total_share_map.get(ts_code).copied(),
    )?;
    if needs_rank_score {
        inject_validation_rank_score_series(&mut row_data, ts_code, rank_score_series_map)?;
    }

    let trade_dates = row_data.trade_dates.clone();
    if trade_dates.is_empty() {
        return Ok(ValidationTsCodeEvaluation {
            ts_code: ts_code.to_string(),
            combo_hits: Vec::new(),
        });
    }

    let keep_from = trade_dates
        .binary_search_by(|date| date.as_str().cmp(start_date))
        .unwrap_or_else(|index| index);
    let mut runtime = row_into_rt(row_data)?;
    let restore_values = combos
        .iter()
        .map(|combo| snapshot_runtime_values(&runtime, &combo.assigned_names))
        .collect::<Vec<_>>();
    let mut combo_hits = Vec::new();

    // All combos originate from the same formula template with different constants,
    // so one runtime load can be reused as long as any overwritten base columns are restored.
    for (combo_index, combo) in combos.iter().enumerate() {
        if !restore_values[combo_index].is_empty() {
            restore_runtime_values(&mut runtime, &restore_values[combo_index]);
        }

        let (scores, triggered_flags) =
            evaluate_cached_rule_scores(&combo.cached_rule, &mut runtime)?;
        let Some(date_score_map) = build_validation_date_score_map(
            &trade_dates,
            keep_from,
            &scores,
            &triggered_flags,
            combo.cached_rule.points,
        ) else {
            continue;
        };
        combo_hits.push((combo_index, date_score_map));
    }

    Ok(ValidationTsCodeEvaluation {
        ts_code: ts_code.to_string(),
        combo_hits,
    })
}

fn build_validation_triggered_scores_for_combos(
    source_path: &str,
    stock_adj_type: &str,
    query_start_date: &str,
    start_date: &str,
    end_date: &str,
    need_rows: usize,
    ts_codes: &[String],
    st_list: &HashSet<String>,
    combos: &[PreparedValidationCombo],
) -> Result<Vec<ValidationTriggeredScoreMap>, String> {
    if combos.is_empty() {
        return Ok(Vec::new());
    }

    let required_runtime_keys = collect_rule_validation_runtime_keys(combos);
    let used_cyq_chen_keys = collect_rule_validation_cyq_chen_runtime_keys(combos);
    let total_share_map = load_total_share_map(source_path).unwrap_or_default();
    let needs_rank_score = validation_combos_use_rank_score(combos);
    let rank_score_series_map = if needs_rank_score {
        load_validation_rank_score_series_map(source_path, query_start_date, end_date)
    } else {
        HashMap::new()
    };
    let combo_triggered_maps = Mutex::new(
        std::iter::repeat_with(HashMap::new)
            .take(combos.len())
            .collect::<Vec<ValidationTriggeredScoreMap>>(),
    );
    let results = ts_codes
        .par_iter()
        .map_init(
            || {
                DataReader::new_with_runtime_keys(source_path, &required_runtime_keys).map(
                    |reader| {
                        (
                            reader,
                            CyqChenFieldInjector::new(source_path, &used_cyq_chen_keys),
                        )
                    },
                )
            },
            |worker_res, ts_code| {
                let (reader, cyq_chen_injector) = worker_res.as_mut().map_err(|err| err.clone())?;
                let ValidationTsCodeEvaluation {
                    ts_code,
                    combo_hits,
                } = evaluate_validation_combos_for_ts_code(
                    reader,
                    cyq_chen_injector,
                    ts_code,
                    stock_adj_type,
                    start_date,
                    end_date,
                    need_rows,
                    st_list,
                    &total_share_map,
                    &rank_score_series_map,
                    needs_rank_score,
                    combos,
                )?;

                if !combo_hits.is_empty() {
                    let mut maps = combo_triggered_maps
                        .lock()
                        .map_err(|_| "写入验证触发结果失败:锁已损坏".to_string())?;
                    for (combo_index, date_score_map) in combo_hits {
                        maps[combo_index].insert(ts_code.clone(), date_score_map);
                    }
                }

                Ok::<(), String>(())
            },
        )
        .collect::<Vec<_>>();

    for result in results {
        result?;
    }

    combo_triggered_maps
        .into_inner()
        .map_err(|_| "读取验证触发结果失败:锁已损坏".to_string())
}

#[cfg(test)]
fn build_validation_triggered_scores(
    source_path: &str,
    stock_adj_type: &str,
    start_date: &str,
    end_date: &str,
    cached_rule: &CachedRule,
) -> Result<HashMap<String, HashMap<String, f64>>, String> {
    let combo = PreparedValidationCombo {
        variant: ValidationVariant {
            combo_key: cached_rule.name.clone(),
            combo_label: cached_rule.name.clone(),
            formula: cached_rule.when_src.clone(),
            unknown_values: Vec::new(),
        },
        cached_rule: cached_rule.clone(),
        assigned_names: collect_validation_assigned_names(&cached_rule.when_ast),
    };
    let required_runtime_keys = collect_rule_validation_runtime_keys(std::slice::from_ref(&combo));
    let reader = DataReader::new_with_runtime_keys(source_path, &required_runtime_keys)?;
    let ts_codes = reader.list_ts_code(stock_adj_type, start_date, end_date)?;
    let st_list = load_st_list(source_path)?;
    let warmup_need = estimate_rule_warmup(
        &cached_rule.when_ast,
        cached_rule.scope_way,
        cached_rule.scope_windows,
    )?;
    let need_rows = calc_query_need_rows(source_path, warmup_need, start_date, end_date)?;
    let mut triggered_maps = build_validation_triggered_scores_for_combos(
        source_path,
        stock_adj_type,
        &calc_query_start_date(source_path, warmup_need, start_date)?,
        start_date,
        end_date,
        need_rows,
        &ts_codes,
        &st_list,
        &[combo],
    )?;
    Ok(triggered_maps.pop().unwrap_or_default())
}

fn build_validation_combo_result(
    params: &RuleLayerBacktestRunParams,
    seed_rule: &ValidationSeedRule,
    combo: &PreparedValidationCombo,
    triggered_score_map: ValidationTriggeredScoreMap,
    runtime_cache: &RuleLayerRuntimeCache,
    layer_config: &RuleLayerConfig,
    similarity_cache: &ValidationSimilarityCache,
    explain_map: &HashMap<String, String>,
    stock_meta_map: &HashMap<String, ValidationSampleStockMeta>,
    sample_limit_per_group: usize,
) -> Result<RuleValidationComboResult, String> {
    let metrics_with_samples = calc_rule_layer_metrics_with_samples_from_cache(
        runtime_cache,
        &triggered_score_map,
        layer_config,
    )?;
    let validation_layer_details = build_validation_score_layer_details(
        &metrics_with_samples.samples,
        layer_config.min_samples_per_day,
    );
    let return_distribution = build_validation_return_distribution(&metrics_with_samples.samples);
    let mut sample_accumulator = ValidationSampleAccumulator::new(
        sample_limit_per_group,
        stock_meta_map,
        similarity_cache,
        matches!(seed_rule.scope_way, ScopeWay::Each),
        seed_rule.points,
        seed_rule.dist_points.is_some(),
    );
    visit_triggered_rule_samples_from_cache(runtime_cache, &triggered_score_map, |sample| {
        sample_accumulator.push(sample);
        Ok(())
    })?;
    let (
        trigger_samples,
        triggered_days,
        sample_stats,
        trigger_count_stats,
        sample_groups,
        overlap_hit_count,
    ) = sample_accumulator.into_parts();
    let backtest = build_rule_backtest_payload(
        &combo.variant.combo_key,
        params,
        metrics_with_samples.metrics,
        Some(validation_layer_details),
    );
    let similarity_rows = build_validation_similarity_rows_from_overlap(
        similarity_cache,
        trigger_samples,
        overlap_hit_count,
        seed_rule.exclude_rule_name.as_deref(),
        explain_map,
    );

    Ok(RuleValidationComboResult {
        combo_key: combo.variant.combo_key.clone(),
        combo_label: combo.variant.combo_label.clone(),
        formula: combo.variant.formula.clone(),
        unknown_values: combo.variant.unknown_values.clone(),
        trigger_samples,
        triggered_days,
        avg_daily_trigger: if triggered_days > 0 {
            trigger_samples as f64 / triggered_days as f64
        } else {
            0.0
        },
        sample_stats,
        trigger_count_stats,
        sample_groups,
        return_distribution,
        backtest,
        similarity_rows,
    })
}

fn normalize_validation_trigger_score(
    score: f64,
    triggered: bool,
    rule_points: f64,
) -> Option<f64> {
    if !score.is_finite() {
        return None;
    }
    if score.abs() > VALIDATION_EPS {
        return Some(score);
    }
    if !triggered {
        return None;
    }

    if rule_points.is_finite() && rule_points.abs() > VALIDATION_EPS {
        return Some(rule_points.signum());
    }
    Some(1.0)
}

struct ValidationScoreLayerAgg {
    score: f64,
    point_count: usize,
    sample_count: usize,
    residual_sum: f64,
}

struct ValidationScoreLayerDetails {
    spread_mean: Option<f64>,
    layer_summaries: Vec<RankLayerBucketSummary>,
}

fn mean_f64(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    Some(values.iter().sum::<f64>() / values.len() as f64)
}

fn build_validation_score_layer_details(
    samples: &[crate::simulate::rule::RuleLayerSamplePoint],
    min_samples_per_day: usize,
) -> ValidationScoreLayerDetails {
    let mut grouped_by_day: std::collections::BTreeMap<
        &str,
        Vec<&crate::simulate::rule::RuleLayerSamplePoint>,
    > = std::collections::BTreeMap::new();
    for sample in samples {
        let trade_date = sample.trade_date.trim();
        if trade_date.is_empty()
            || !sample.rule_score.is_finite()
            || !sample.residual_return.is_finite()
        {
            continue;
        }
        grouped_by_day.entry(trade_date).or_default().push(sample);
    }

    let mut spread_values = Vec::new();
    let mut summary_map = HashMap::<u64, ValidationScoreLayerAgg>::new();

    for day_samples in grouped_by_day.into_values() {
        if day_samples.len() < min_samples_per_day {
            continue;
        }

        let mut ordered = day_samples
            .into_iter()
            .map(|sample| {
                let score = if sample.rule_score.abs() < VALIDATION_EPS {
                    0.0
                } else {
                    sample.rule_score
                };
                (score, sample.residual_return)
            })
            .collect::<Vec<_>>();
        ordered.sort_by(|left, right| {
            left.0
                .partial_cmp(&right.0)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let mut day_layer_returns = Vec::new();
        let mut index = 0usize;
        while index < ordered.len() {
            let score = ordered[index].0;
            let score_bits = score.to_bits();
            let mut residuals = Vec::new();
            while index < ordered.len() && ordered[index].0.to_bits() == score_bits {
                residuals.push(ordered[index].1);
                index += 1;
            }

            let Some(avg_residual_return) = mean_f64(&residuals) else {
                continue;
            };
            day_layer_returns.push(avg_residual_return);
            let agg = summary_map
                .entry(score_bits)
                .or_insert_with(|| ValidationScoreLayerAgg {
                    score,
                    point_count: 0,
                    sample_count: 0,
                    residual_sum: 0.0,
                });
            agg.point_count += 1;
            agg.sample_count += residuals.len();
            agg.residual_sum += avg_residual_return;
        }

        if let (Some(low), Some(high)) = (day_layer_returns.first(), day_layer_returns.last()) {
            if day_layer_returns.len() >= 2 {
                spread_values.push(high - low);
            }
        }
    }

    let mut layer_summaries = summary_map.into_values().collect::<Vec<_>>();
    layer_summaries.sort_by(|left, right| {
        left.score
            .partial_cmp(&right.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    ValidationScoreLayerDetails {
        spread_mean: mean_f64(&spread_values),
        layer_summaries: layer_summaries
            .into_iter()
            .enumerate()
            .map(|(index, item)| RankLayerBucketSummary {
                layer_index: index + 1,
                layer_label: format_validation_score_layer_label(item.score),
                point_count: item.point_count,
                sample_count: item.sample_count,
                avg_score: Some(item.score),
                avg_residual_return: if item.point_count == 0 {
                    None
                } else {
                    Some(item.residual_sum / item.point_count as f64)
                },
                avg_er_change: None,
            })
            .collect(),
    }
}

fn build_validation_return_distribution(
    samples: &[crate::simulate::rule::RuleLayerSamplePoint],
) -> Vec<RuleValidationReturnDistributionBucket> {
    const BUCKET_LABELS: [&str; 7] = [
        "<= -10%", "-10%~-5%", "-5%~-2%", "-2%~2%", "2%~5%", "5%~10%", ">= 10%",
    ];

    let mut counts = [0usize; 7];
    let mut total = 0usize;
    for sample in samples {
        if !sample.residual_return.is_finite() {
            continue;
        }

        let bucket_index = if sample.residual_return <= -10.0 {
            0
        } else if sample.residual_return <= -5.0 {
            1
        } else if sample.residual_return <= -2.0 {
            2
        } else if sample.residual_return <= 2.0 {
            3
        } else if sample.residual_return <= 5.0 {
            4
        } else if sample.residual_return <= 10.0 {
            5
        } else {
            6
        };
        counts[bucket_index] += 1;
        total += 1;
    }

    BUCKET_LABELS
        .into_iter()
        .enumerate()
        .map(|(index, label)| RuleValidationReturnDistributionBucket {
            bucket_label: label.to_string(),
            sample_count: counts[index],
            sample_ratio: if total > 0 {
                Some(counts[index] as f64 / total as f64)
            } else {
                None
            },
        })
        .collect()
}

fn format_validation_score_layer_label(score: f64) -> String {
    if (score.round() - score).abs() < VALIDATION_EPS {
        format!("得分 {}", score.round() as i64)
    } else {
        format!("得分 {:.4}", score)
    }
}

fn build_rule_backtest_payload(
    combo_key: &str,
    params: &RuleLayerBacktestRunParams,
    metrics: crate::simulate::rule::RuleLayerMetrics,
    layer_details: Option<ValidationScoreLayerDetails>,
) -> RuleLayerBacktestData {
    let decay_validations = build_rule_decay_validations(&metrics.points);
    let (spread_mean, layer_count, layer_method, layer_method_label, layer_summaries) =
        match layer_details {
            Some(layer_details) => {
                let layer_count = layer_details.layer_summaries.len();
                (
                    layer_details.spread_mean,
                    Some(layer_count),
                    Some("score_value".to_string()),
                    Some("按得分值分层".to_string()),
                    layer_details.layer_summaries,
                )
            }
            None => (None, None, None, None, Vec::new()),
        };

    RuleLayerBacktestData {
        rule_name: combo_key.to_string(),
        stock_adj_type: params.stock_adj_type.clone(),
        index_ts_code: params.index_ts_code.clone(),
        index_beta: params.index_beta,
        concept_beta: params.concept_beta,
        industry_beta: params.industry_beta,
        start_date: params.start_date.clone(),
        end_date: params.end_date.clone(),
        resolved_board: params.resolved_board.clone(),
        exclude_st_board: params.exclude_st_board,
        total_mv_min: params.total_mv_min,
        total_mv_max: params.total_mv_max,
        min_samples_per_rule_day: params.min_samples_per_day,
        min_listed_trade_days: params.min_listed_trade_days,
        backtest_period: params.backtest_period,
        points: Vec::new(),
        avg_residual_mean: metrics.avg_residual_mean,
        avg_excess_residual_mean: metrics.avg_excess_residual_mean,
        decay_validations,
        avg_er_change: metrics.avg_er_change,
        profit_loss_ratio: metrics.profit_loss_ratio,
        spread_mean,
        avg_contribution_score: None,
        avg_contribution_per_trigger: None,
        ic_mean: metrics.ic_mean,
        ic_std: metrics.ic_std,
        icir: metrics.icir,
        ic_t_value: metrics.ic_t_value,
        layer_count,
        layer_method,
        layer_method_label,
        layer_summaries,
        is_all_rules: false,
        all_rule_summaries: Vec::new(),
        rule_validation_details: Vec::new(),
    }
}

fn build_strategy_rule_validation_detail(
    params: &RuleLayerBacktestRunParams,
    rule_name: &str,
    rule_meta: &RuleMeta,
    metrics_with_samples: RuleLayerMetricsWithSamples,
    layer_config: &RuleLayerConfig,
    similarity_cache: &ValidationSimilarityCache,
    explain_map: &HashMap<String, String>,
    stock_meta_map: &HashMap<String, ValidationSampleStockMeta>,
) -> RuleValidationComboResult {
    let validation_layer_details = build_validation_score_layer_details(
        &metrics_with_samples.samples,
        layer_config.min_samples_per_day,
    );
    let return_distribution = build_validation_return_distribution(&metrics_with_samples.samples);
    let mut sample_accumulator = ValidationSampleAccumulator::new(
        RULE_BACKTEST_DETAIL_SAMPLE_LIMIT_PER_GROUP,
        stock_meta_map,
        similarity_cache,
        rule_meta.is_each,
        rule_meta.points,
        false,
    );
    for sample in &metrics_with_samples.samples {
        if sample.rule_score.abs() <= RULE_BACKTEST_EPS {
            continue;
        }
        sample_accumulator.push(RuleLayerSamplePointRef {
            ts_code: &sample.ts_code,
            trade_date: &sample.trade_date,
            rule_score: sample.rule_score,
            residual_return: sample.residual_return,
        });
    }

    let (
        trigger_samples,
        triggered_days,
        sample_stats,
        trigger_count_stats,
        sample_groups,
        overlap_hit_count,
    ) = sample_accumulator.into_parts();
    let backtest = build_rule_backtest_payload(
        rule_name,
        params,
        metrics_with_samples.metrics,
        Some(validation_layer_details),
    );
    let similarity_rows = build_validation_similarity_rows_from_overlap(
        similarity_cache,
        trigger_samples,
        overlap_hit_count,
        Some(rule_name),
        explain_map,
    );

    RuleValidationComboResult {
        combo_key: rule_name.to_string(),
        combo_label: rule_name.to_string(),
        formula: rule_meta.when.clone(),
        unknown_values: Vec::new(),
        trigger_samples,
        triggered_days,
        avg_daily_trigger: if triggered_days > 0 {
            trigger_samples as f64 / triggered_days as f64
        } else {
            0.0
        },
        sample_stats,
        trigger_count_stats,
        sample_groups,
        return_distribution,
        backtest,
        similarity_rows,
    }
}

#[derive(Debug, Clone)]
struct ValidationSimilarityCache {
    total_samples: f64,
    rule_names: Vec<String>,
    rule_hit_counts: Vec<usize>,
    pair_to_rule_indices: HashMap<String, HashMap<String, Vec<usize>>>,
}

fn load_validation_similarity_cache(
    result_conn: &Connection,
    start_date: &str,
    end_date: &str,
) -> Result<ValidationSimilarityCache, String> {
    let total_samples = result_conn
        .query_row(
            r#"
            SELECT COUNT(*)
            FROM score_summary
            WHERE trade_date >= ?
              AND trade_date <= ?
            "#,
            params![start_date, end_date],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| format!("读取验证样本总数失败: {e}"))?
        .max(0) as f64;

    let mut stmt = result_conn
        .prepare(
            r#"
            SELECT
                rule_name,
                ts_code,
                trade_date
            FROM rule_details
            WHERE trade_date >= ?
              AND trade_date <= ?
              AND TRY_CAST(rule_score AS DOUBLE) IS NOT NULL
              AND ABS(TRY_CAST(rule_score AS DOUBLE)) > 1e-12
            "#,
        )
        .map_err(|e| format!("预编译触发相似度查询失败: {e}"))?;
    let mut rows = stmt
        .query(params![start_date, end_date])
        .map_err(|e| format!("查询触发相似度失败: {e}"))?;

    let mut rule_names = Vec::new();
    let mut rule_name_to_index = HashMap::<String, usize>::new();
    let mut rule_hit_counts = Vec::new();
    let mut pair_to_rule_indices = HashMap::<String, HashMap<String, Vec<usize>>>::new();

    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取触发相似度失败: {e}"))?
    {
        let rule_name: String = row.get(0).map_err(|e| format!("读取规则名失败: {e}"))?;
        let ts_code: String = row.get(1).map_err(|e| format!("读取代码失败: {e}"))?;
        let trade_date: String = row.get(2).map_err(|e| format!("读取交易日失败: {e}"))?;
        let rule_index = if let Some(index) = rule_name_to_index.get(&rule_name) {
            *index
        } else {
            let index = rule_names.len();
            rule_name_to_index.insert(rule_name.clone(), index);
            rule_names.push(rule_name);
            rule_hit_counts.push(0);
            index
        };

        rule_hit_counts[rule_index] += 1;
        pair_to_rule_indices
            .entry(ts_code)
            .or_default()
            .entry(trade_date)
            .or_default()
            .push(rule_index);
    }

    Ok(ValidationSimilarityCache {
        total_samples,
        rule_names,
        rule_hit_counts,
        pair_to_rule_indices,
    })
}

fn empty_validation_similarity_cache() -> ValidationSimilarityCache {
    ValidationSimilarityCache {
        total_samples: 0.0,
        rule_names: Vec::new(),
        rule_hit_counts: Vec::new(),
        pair_to_rule_indices: HashMap::new(),
    }
}

fn load_validation_similarity_cache_optional(
    source_path: &str,
    start_date: &str,
    end_date: &str,
) -> Result<ValidationSimilarityCache, String> {
    let result_db = result_db_path(source_path);
    if !result_db.exists() {
        return Ok(empty_validation_similarity_cache());
    }

    let result_conn = open_result_conn(source_path)?;
    match load_validation_similarity_cache(&result_conn, start_date, end_date) {
        Ok(cache) => Ok(cache),
        Err(_) => Ok(empty_validation_similarity_cache()),
    }
}

#[cfg(test)]
fn build_validation_similarity_rows(
    similarity_cache: &ValidationSimilarityCache,
    triggered_samples: &[crate::simulate::rule::RuleLayerSamplePoint],
    exclude_rule_name: Option<&str>,
    explain_map: &HashMap<String, String>,
) -> Vec<RuleValidationSimilarityRow> {
    let mut overlap_hit_count = HashMap::<usize, usize>::new();

    for sample in triggered_samples {
        let Some(date_map) = similarity_cache.pair_to_rule_indices.get(&sample.ts_code) else {
            continue;
        };
        let Some(rule_indices) = date_map.get(&sample.trade_date) else {
            continue;
        };

        for rule_index in rule_indices {
            *overlap_hit_count.entry(*rule_index).or_default() += 1;
        }
    }

    build_validation_similarity_rows_from_overlap(
        similarity_cache,
        triggered_samples.len(),
        overlap_hit_count,
        exclude_rule_name,
        explain_map,
    )
}

fn build_validation_similarity_rows_from_overlap(
    similarity_cache: &ValidationSimilarityCache,
    combo_hit_count: usize,
    overlap_hit_count: HashMap<usize, usize>,
    exclude_rule_name: Option<&str>,
    explain_map: &HashMap<String, String>,
) -> Vec<RuleValidationSimilarityRow> {
    let combo_hit_count = combo_hit_count as f64;
    if combo_hit_count <= 0.0 {
        return Vec::new();
    }

    let excluded_rule_name = exclude_rule_name
        .map(str::trim)
        .filter(|value| !value.is_empty());

    let mut out = overlap_hit_count
        .into_iter()
        .filter_map(|(rule_index, overlap_samples)| {
            if overlap_samples == 0 {
                return None;
            }

            let rule_name = similarity_cache.rule_names.get(rule_index)?;
            if excluded_rule_name.is_some_and(|excluded| rule_name == excluded) {
                return None;
            }

            let existing_count = similarity_cache
                .rule_hit_counts
                .get(rule_index)
                .copied()
                .unwrap_or(0) as f64;
            let overlap_rate_vs_validation = Some(overlap_samples as f64 / combo_hit_count);
            let overlap_rate_vs_existing = if existing_count > 0.0 {
                Some(overlap_samples as f64 / existing_count)
            } else {
                None
            };
            let overlap_lift = if similarity_cache.total_samples > 0.0 && existing_count > 0.0 {
                Some(
                    overlap_samples as f64 * similarity_cache.total_samples
                        / (combo_hit_count * existing_count),
                )
            } else {
                None
            };

            Some(RuleValidationSimilarityRow {
                rule_name: rule_name.clone(),
                explain: explain_map.get(rule_name).cloned(),
                overlap_samples,
                overlap_rate_vs_validation,
                overlap_rate_vs_existing,
                overlap_lift,
            })
        })
        .collect::<Vec<_>>();

    out.sort_by(|left, right| {
        right
            .overlap_samples
            .cmp(&left.overlap_samples)
            .then_with(|| left.rule_name.cmp(&right.rule_name))
    });
    out.truncate(20);
    out
}

fn compare_option_f64_desc(left: Option<f64>, right: Option<f64>) -> std::cmp::Ordering {
    match (left, right) {
        (Some(l), Some(r)) => r.partial_cmp(&l).unwrap_or(std::cmp::Ordering::Equal),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

#[derive(Debug, Clone)]
struct ValidationSampleRawRow {
    ts_code: String,
    trade_date: String,
    trigger_count: usize,
    rule_score: f64,
    residual_return: f64,
}

#[derive(Debug, Clone)]
struct ValidationSampleStockMeta {
    name: Option<String>,
    board: String,
    volatility_group: String,
}

struct ValidationSampleAccumulator<'a> {
    sample_limit_per_group: usize,
    stock_meta_map: &'a HashMap<String, ValidationSampleStockMeta>,
    similarity_cache: &'a ValidationSimilarityCache,
    is_each: bool,
    trigger_unit_points: f64,
    has_dist_points: bool,
    total_triggers: usize,
    triggered_days: HashSet<String>,
    sample_by_stock: HashMap<String, ValidationSampleRawRow>,
    overlap_hit_count: HashMap<usize, usize>,
}

impl<'a> ValidationSampleAccumulator<'a> {
    fn new(
        sample_limit_per_group: usize,
        stock_meta_map: &'a HashMap<String, ValidationSampleStockMeta>,
        similarity_cache: &'a ValidationSimilarityCache,
        is_each: bool,
        trigger_unit_points: f64,
        has_dist_points: bool,
    ) -> Self {
        Self {
            sample_limit_per_group,
            stock_meta_map,
            similarity_cache,
            is_each,
            trigger_unit_points,
            has_dist_points,
            total_triggers: 0,
            triggered_days: HashSet::new(),
            sample_by_stock: HashMap::new(),
            overlap_hit_count: HashMap::new(),
        }
    }

    fn push(&mut self, sample: crate::simulate::rule::RuleLayerSamplePointRef<'_>) {
        self.total_triggers += 1;
        self.triggered_days.insert(sample.trade_date.to_string());
        self.update_similarity_overlap(sample.ts_code, sample.trade_date);

        let row = ValidationSampleRawRow {
            ts_code: sample.ts_code.to_string(),
            trade_date: sample.trade_date.to_string(),
            trigger_count: resolve_validation_trigger_count(
                sample.rule_score,
                self.is_each,
                self.trigger_unit_points,
                self.has_dist_points,
            ),
            rule_score: sample.rule_score,
            residual_return: sample.residual_return,
        };

        let sample_key = format!("{}__{}", row.trigger_count, row.ts_code);
        self.sample_by_stock
            .entry(sample_key)
            .and_modify(|current| {
                if should_replace_validation_stock_sample(current, &row) {
                    *current = row.clone();
                }
            })
            .or_insert(row);
    }

    fn update_similarity_overlap(&mut self, ts_code: &str, trade_date: &str) {
        let Some(date_map) = self.similarity_cache.pair_to_rule_indices.get(ts_code) else {
            return;
        };
        let Some(rule_indices) = date_map.get(trade_date) else {
            return;
        };

        for rule_index in rule_indices {
            *self.overlap_hit_count.entry(*rule_index).or_default() += 1;
        }
    }

    fn into_parts(
        self,
    ) -> (
        usize,
        usize,
        RuleValidationSampleStats,
        Vec<RuleValidationTriggerCountStats>,
        RuleValidationSampleGroups,
        HashMap<usize, usize>,
    ) {
        let unique_samples = self.sample_by_stock.into_values().collect::<Vec<_>>();
        let unique_sample_count = unique_samples.len();
        let positive_count = unique_samples
            .iter()
            .filter(|row| row.residual_return > 0.0)
            .count();
        let negative_count = unique_samples
            .iter()
            .filter(|row| row.residual_return < 0.0)
            .count();
        let mut trigger_count_stats_map = HashMap::<usize, RuleValidationTriggerCountStats>::new();
        for row in &unique_samples {
            let stats = trigger_count_stats_map
                .entry(row.trigger_count)
                .or_insert_with(|| RuleValidationTriggerCountStats {
                    trigger_count: row.trigger_count,
                    positive_count: 0,
                    negative_count: 0,
                    random_count: 0,
                    total_samples: 0,
                });
            stats.total_samples += 1;
            stats.random_count += 1;
            if row.residual_return > 0.0 {
                stats.positive_count += 1;
            } else if row.residual_return < 0.0 {
                stats.negative_count += 1;
            }
        }
        let mut trigger_count_stats = trigger_count_stats_map.into_values().collect::<Vec<_>>();
        trigger_count_stats.sort_by_key(|item| item.trigger_count);

        let mut positive_by_board: HashMap<(usize, String), Vec<ValidationSampleRawRow>> =
            HashMap::new();
        let mut negative_by_board: HashMap<(usize, String), Vec<ValidationSampleRawRow>> =
            HashMap::new();
        let mut random_by_board: HashMap<(usize, String), Vec<(u64, ValidationSampleRawRow)>> =
            HashMap::new();

        for row in unique_samples {
            let board = sample_board(&row.ts_code, self.stock_meta_map);
            let bucket_key = (row.trigger_count, board);
            if row.residual_return > 0.0 {
                push_limited_sample(
                    positive_by_board.entry(bucket_key.clone()).or_default(),
                    row.clone(),
                    self.sample_limit_per_group,
                    compare_positive_validation_sample,
                );
            } else if row.residual_return < 0.0 {
                push_limited_sample(
                    negative_by_board.entry(bucket_key.clone()).or_default(),
                    row.clone(),
                    self.sample_limit_per_group,
                    compare_negative_validation_sample,
                );
            }

            push_limited_random_sample(
                random_by_board.entry(bucket_key).or_default(),
                random::<u64>(),
                row,
                self.sample_limit_per_group,
            );
        }

        let mut positive = positive_by_board
            .into_values()
            .flatten()
            .collect::<Vec<_>>();
        let mut negative = negative_by_board
            .into_values()
            .flatten()
            .collect::<Vec<_>>();
        let mut random = random_by_board.into_values().flatten().collect::<Vec<_>>();

        positive.sort_by(compare_positive_validation_sample);
        negative.sort_by(compare_negative_validation_sample);
        random.sort_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then_with(|| compare_random_validation_sample(&left.1, &right.1))
        });

        let groups = RuleValidationSampleGroups {
            positive: validation_sample_rows_to_payload(positive, self.stock_meta_map),
            negative: validation_sample_rows_to_payload(negative, self.stock_meta_map),
            random: validation_sample_rows_to_payload(
                random.into_iter().map(|(_, row)| row),
                self.stock_meta_map,
            ),
        };
        let stats = RuleValidationSampleStats {
            positive_count,
            negative_count,
            random_count: unique_sample_count,
            total_samples: unique_sample_count,
        };

        (
            self.total_triggers,
            self.triggered_days.len(),
            stats,
            trigger_count_stats,
            groups,
            self.overlap_hit_count,
        )
    }
}

fn resolve_validation_trigger_count(
    rule_score: f64,
    is_each: bool,
    trigger_unit_points: f64,
    has_dist_points: bool,
) -> usize {
    if !is_each || has_dist_points || trigger_unit_points.abs() <= VALIDATION_EPS {
        return 1;
    }

    let count = (rule_score / trigger_unit_points).abs().round();
    if count.is_finite() && count >= 1.0 {
        count as usize
    } else {
        1
    }
}

fn sample_board(
    ts_code: &str,
    stock_meta_map: &HashMap<String, ValidationSampleStockMeta>,
) -> String {
    stock_meta_map
        .get(ts_code)
        .map(|meta| meta.board.clone())
        .unwrap_or_else(|| "其他".to_string())
}

fn should_replace_validation_stock_sample(
    current: &ValidationSampleRawRow,
    candidate: &ValidationSampleRawRow,
) -> bool {
    let strength_order = candidate
        .residual_return
        .abs()
        .partial_cmp(&current.residual_return.abs())
        .unwrap_or(Ordering::Equal);
    if strength_order != Ordering::Equal {
        return strength_order == Ordering::Greater;
    }

    let date_order = candidate.trade_date.cmp(&current.trade_date);
    if date_order != Ordering::Equal {
        return date_order == Ordering::Greater;
    }

    candidate
        .rule_score
        .abs()
        .partial_cmp(&current.rule_score.abs())
        .unwrap_or(Ordering::Equal)
        == Ordering::Greater
}

fn compare_positive_validation_sample(
    left: &ValidationSampleRawRow,
    right: &ValidationSampleRawRow,
) -> Ordering {
    right
        .residual_return
        .partial_cmp(&left.residual_return)
        .unwrap_or(Ordering::Equal)
        .then_with(|| left.trade_date.cmp(&right.trade_date))
        .then_with(|| left.ts_code.cmp(&right.ts_code))
}

fn compare_negative_validation_sample(
    left: &ValidationSampleRawRow,
    right: &ValidationSampleRawRow,
) -> Ordering {
    left.residual_return
        .partial_cmp(&right.residual_return)
        .unwrap_or(Ordering::Equal)
        .then_with(|| left.trade_date.cmp(&right.trade_date))
        .then_with(|| left.ts_code.cmp(&right.ts_code))
}

fn compare_random_validation_sample(
    left: &ValidationSampleRawRow,
    right: &ValidationSampleRawRow,
) -> Ordering {
    left.trade_date
        .cmp(&right.trade_date)
        .then_with(|| left.ts_code.cmp(&right.ts_code))
}

fn push_limited_sample(
    rows: &mut Vec<ValidationSampleRawRow>,
    row: ValidationSampleRawRow,
    limit: usize,
    compare: fn(&ValidationSampleRawRow, &ValidationSampleRawRow) -> Ordering,
) {
    if limit == 0 {
        return;
    }

    rows.push(row);
    rows.sort_by(compare);
    rows.truncate(limit);
}

fn push_limited_random_sample(
    rows: &mut Vec<(u64, ValidationSampleRawRow)>,
    key: u64,
    row: ValidationSampleRawRow,
    limit: usize,
) {
    if limit == 0 {
        return;
    }

    if rows.len() < limit {
        rows.push((key, row));
        return;
    }

    let Some((worst_index, _)) = rows.iter().enumerate().max_by(|(_, left), (_, right)| {
        left.0
            .cmp(&right.0)
            .then_with(|| compare_random_validation_sample(&left.1, &right.1))
    }) else {
        return;
    };

    if key < rows[worst_index].0 {
        rows[worst_index] = (key, row);
    }
}

fn validation_sample_rows_to_payload(
    rows: impl IntoIterator<Item = ValidationSampleRawRow>,
    stock_meta_map: &HashMap<String, ValidationSampleStockMeta>,
) -> Vec<RuleValidationSampleRow> {
    rows.into_iter()
        .map(|row| RuleValidationSampleRow {
            name: stock_meta_map
                .get(&row.ts_code)
                .and_then(|meta| meta.name.clone()),
            board: stock_meta_map
                .get(&row.ts_code)
                .map(|meta| meta.board.clone())
                .unwrap_or_else(|| "其他".to_string()),
            volatility_group: stock_meta_map
                .get(&row.ts_code)
                .map(|meta| meta.volatility_group.clone())
                .unwrap_or_else(|| "其他波动".to_string()),
            ts_code: row.ts_code,
            trade_date: row.trade_date,
            trigger_count: row.trigger_count,
            rule_score: row.rule_score,
            residual_return: row.residual_return,
        })
        .collect()
}

#[cfg(test)]
fn build_validation_sample_groups(
    samples: &[ValidationSampleRawRow],
    sample_limit_per_group: usize,
    stock_meta_map: &HashMap<String, ValidationSampleStockMeta>,
) -> (RuleValidationSampleStats, RuleValidationSampleGroups) {
    let mut positive = Vec::new();
    let mut negative = Vec::new();

    for row in samples {
        if row.residual_return > 0.0 {
            positive.push(row.clone());
        } else if row.residual_return < 0.0 {
            negative.push(row.clone());
        }
    }

    positive.par_sort_by(|left, right| {
        right
            .residual_return
            .partial_cmp(&left.residual_return)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.trade_date.cmp(&right.trade_date))
            .then_with(|| left.ts_code.cmp(&right.ts_code))
    });
    negative.par_sort_by(|left, right| {
        left.residual_return
            .partial_cmp(&right.residual_return)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.trade_date.cmp(&right.trade_date))
            .then_with(|| left.ts_code.cmp(&right.ts_code))
    });

    let mut rng = StdRng::seed_from_u64(VALIDATION_RANDOM_SAMPLE_SEED);
    let mut random_pool = samples.to_vec();
    random_pool.par_sort_by(|left, right| {
        left.trade_date
            .cmp(&right.trade_date)
            .then_with(|| left.ts_code.cmp(&right.ts_code))
    });
    if random_pool.len() > 1 {
        for index in (1..random_pool.len()).rev() {
            let swap_index = rng.random_range(0..=index);
            random_pool.swap(index, swap_index);
        }
    }

    let limit_rows_per_board = |rows: Vec<ValidationSampleRawRow>| {
        let mut board_counts = HashMap::<String, usize>::new();
        let mut limited = Vec::new();

        for row in rows {
            let board = stock_meta_map
                .get(&row.ts_code)
                .map(|meta| meta.board.clone())
                .unwrap_or_else(|| "其他".to_string());
            let count = board_counts.entry(board).or_insert(0);
            if *count >= sample_limit_per_group {
                continue;
            }
            *count += 1;
            limited.push(row);
        }

        limited
    };

    let to_payload = |rows: Vec<ValidationSampleRawRow>| {
        limit_rows_per_board(rows)
            .into_iter()
            .map(|row| RuleValidationSampleRow {
                name: stock_meta_map
                    .get(&row.ts_code)
                    .and_then(|meta| meta.name.clone()),
                board: stock_meta_map
                    .get(&row.ts_code)
                    .map(|meta| meta.board.clone())
                    .unwrap_or_else(|| "其他".to_string()),
                volatility_group: stock_meta_map
                    .get(&row.ts_code)
                    .map(|meta| meta.volatility_group.clone())
                    .unwrap_or_else(|| "其他波动".to_string()),
                ts_code: row.ts_code,
                trade_date: row.trade_date,
                trigger_count: row.trigger_count,
                rule_score: row.rule_score,
                residual_return: row.residual_return,
            })
            .collect::<Vec<_>>()
    };

    let stats = RuleValidationSampleStats {
        positive_count: positive.len(),
        negative_count: negative.len(),
        random_count: random_pool.len(),
        total_samples: samples.len(),
    };

    let groups = RuleValidationSampleGroups {
        positive: to_payload(positive),
        negative: to_payload(negative),
        random: to_payload(random_pool),
    };

    (stats, groups)
}

pub fn run_rule_expression_validation(
    source_path: String,
    import_rule_name: String,
    when: Option<String>,
    scope_way: Option<String>,
    scope_windows: Option<usize>,
    stock_adj_type: Option<String>,
    index_ts_code: String,
    index_beta: Option<f64>,
    concept_beta: Option<f64>,
    industry_beta: Option<f64>,
    start_date: String,
    end_date: String,
    min_samples_per_rule_day: Option<usize>,
    min_listed_trade_days: Option<usize>,
    backtest_period: Option<usize>,
    manual_strategy: Option<RuleExpressionValidationManualStrategy>,
    unknown_configs: Option<Vec<RuleValidationUnknownConfig>>,
    sample_limit_per_group: Option<usize>,
    board: Option<String>,
    exclude_st_board: Option<bool>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
) -> Result<RuleExpressionValidationData, String> {
    let source_path = source_path.trim().to_string();
    if source_path.is_empty() {
        return Err("数据目录不能为空".to_string());
    }

    let import_rule_name = import_rule_name.trim().to_string();
    let all_rules = ScoreRule::load_rules(&source_path)?;
    let seed_rule = resolve_validation_seed_rule(
        &import_rule_name,
        manual_strategy.as_ref(),
        when.as_deref(),
        scope_way.as_deref(),
        scope_windows,
        &all_rules,
    )?;
    let start_date = start_date.trim().to_string();
    let end_date = end_date.trim().to_string();
    let variants =
        build_validation_variants(&seed_rule.formula, &unknown_configs.unwrap_or_default())?;
    let execution_plan = build_validation_execution_plan(
        &source_path,
        &start_date,
        &end_date,
        &seed_rule,
        variants,
    )?;

    let (resolved_board, exclude_st_board, total_mv_min, total_mv_max, allowed_ts_codes) =
        build_backtest_stock_filter(
            &source_path,
            board,
            exclude_st_board,
            total_mv_min,
            total_mv_max,
        )?;

    let params = RuleLayerBacktestRunParams {
        stock_adj_type: stock_adj_type
            .unwrap_or_else(|| "qfq".to_string())
            .trim()
            .to_string(),
        index_ts_code: index_ts_code.trim().to_string(),
        index_beta: index_beta.unwrap_or(0.5),
        concept_beta: concept_beta.unwrap_or(0.2),
        industry_beta: industry_beta.unwrap_or(0.0),
        start_date,
        end_date,
        min_samples_per_day: min_samples_per_rule_day.unwrap_or(5).max(1),
        min_listed_trade_days: min_listed_trade_days
            .unwrap_or(DEFAULT_BACKTEST_MIN_LISTED_TRADE_DAYS),
        backtest_period: backtest_period.unwrap_or(1).max(1),
        parallel_batch_size: DEFAULT_RULE_WITH_SAMPLES_PARALLEL_BATCH_SIZE,
        resolved_board,
        exclude_st_board,
        total_mv_min,
        total_mv_max,
        allowed_ts_codes,
    };

    let sample_limit_per_group = sample_limit_per_group
        .unwrap_or(VALIDATION_DEFAULT_SAMPLE_LIMIT_PER_GROUP)
        .clamp(1, VALIDATION_MAX_SAMPLE_LIMIT_PER_GROUP);
    let source_db = source_db_path(&source_path);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "原始库路径不是有效UTF-8".to_string())?;
    let source_conn =
        Connection::open(source_db_str).map_err(|e| format!("打开原始库失败: {e}"))?;
    let layer_config = RuleLayerConfig {
        min_samples_per_day: params.min_samples_per_day,
        backtest_period: params.backtest_period,
        min_listed_trade_days: params.min_listed_trade_days,
    };
    let runtime_cache = Arc::new(
        build_rule_layer_runtime_cache_from_stock_data_with_ts_filter(
            &source_conn,
            &source_path,
            &params.stock_adj_type,
            &params.index_ts_code,
            params.index_beta,
            params.concept_beta,
            params.industry_beta,
            &params.start_date,
            &params.end_date,
            &layer_config,
            params.allowed_ts_codes.as_ref(),
        )?,
    );
    let validation_required_runtime_keys =
        collect_rule_validation_runtime_keys(&execution_plan.combos);
    let validation_reader =
        DataReader::new_with_runtime_keys(&source_path, &validation_required_runtime_keys)?;
    let mut validation_ts_codes = validation_reader.list_ts_code(
        &params.stock_adj_type,
        &params.start_date,
        &params.end_date,
    )?;
    if let Some(allowed_ts_codes) = params.allowed_ts_codes.as_ref() {
        validation_ts_codes
            .retain(|ts_code| ts_code_allowed_by_filter(Some(allowed_ts_codes), ts_code));
    }
    let st_list = load_st_list(&source_path)?;
    let explain_map = all_rules
        .iter()
        .map(|rule| (rule.name.clone(), rule.explain.clone()))
        .collect::<HashMap<_, _>>();
    let stock_meta_map = load_validation_sample_stock_meta_map(&source_path)?;
    let similarity_cache = load_validation_similarity_cache_optional(
        &source_path,
        &params.start_date,
        &params.end_date,
    )?;
    let mut combo_results = Vec::with_capacity(execution_plan.combos.len());
    for combo_chunk in execution_plan
        .combos
        .chunks(VALIDATION_COMBO_EVAL_BATCH_SIZE)
    {
        let combo_triggered_maps = build_validation_triggered_scores_for_combos(
            &source_path,
            &params.stock_adj_type,
            &execution_plan.query_start_date,
            &params.start_date,
            &params.end_date,
            execution_plan.need_rows,
            &validation_ts_codes,
            &st_list,
            combo_chunk,
        )?;

        for (combo, triggered_score_map) in combo_chunk.iter().zip(combo_triggered_maps.into_iter())
        {
            combo_results.push(build_validation_combo_result(
                &params,
                &seed_rule,
                combo,
                triggered_score_map,
                runtime_cache.as_ref(),
                &layer_config,
                &similarity_cache,
                &explain_map,
                &stock_meta_map,
                sample_limit_per_group,
            )?);
        }
    }

    combo_results.sort_by(|left, right| {
        compare_option_f64_desc(left.backtest.spread_mean, right.backtest.spread_mean)
            .then_with(|| compare_option_f64_desc(left.backtest.icir, right.backtest.icir))
            .then_with(|| right.trigger_samples.cmp(&left.trigger_samples))
            .then_with(|| left.combo_key.cmp(&right.combo_key))
    });

    let best_combo_key = combo_results.first().map(|item| item.combo_key.clone());
    let continuation_combos = execution_plan
        .combos
        .iter()
        .map(|combo| {
            (
                combo.variant.combo_key.clone(),
                ValidationContinuationCombo {
                    combo_key: combo.variant.combo_key.clone(),
                    combo_label: combo.variant.combo_label.clone(),
                    formula: combo.variant.formula.clone(),
                },
            )
        })
        .collect::<HashMap<_, _>>();
    let continuation_id = store_validation_continuation_session(ValidationContinuationSession {
        created_at: Instant::now(),
        source_path: source_path.clone(),
        params: params.clone(),
        runtime_cache: Arc::clone(&runtime_cache),
        seed_rule: seed_rule.clone(),
        validation_ts_codes,
        combos: continuation_combos,
    })
    .ok();

    Ok(RuleExpressionValidationData {
        import_rule_name: seed_rule.rule_name,
        import_rule_explain: seed_rule.rule_explain,
        scope_way: scope_way_label(seed_rule.scope_way),
        scope_windows: seed_rule.scope_windows,
        sample_limit_per_group,
        combo_results,
        best_combo_key,
        continuation_id,
    })
}

#[derive(Debug, Clone)]
struct ValidationCalibrationSpec {
    candidate_key: String,
    scope_way: ScopeWay,
    scope_windows: usize,
    scope_label: String,
    dist_points: Option<Vec<crate::data::DistPoint>>,
    is_current: bool,
}

#[derive(Debug, Default)]
struct ValidationCalibrationBucketAgg {
    score_multiplier: f64,
    sample_count: usize,
    residual_sum: f64,
}

fn scope_way_config_label(scope_way: ScopeWay) -> String {
    match scope_way {
        ScopeWay::Any => "ANY".to_string(),
        ScopeWay::Last => "LAST".to_string(),
        ScopeWay::Each => "EACH".to_string(),
        ScopeWay::Recent => "RECENT".to_string(),
        ScopeWay::Consec(threshold) => format!("CONSEC>={threshold}"),
    }
}

fn normalize_calibration_dist_points(
    items: Option<Vec<crate::data::DistPoint>>,
    direction_sign: f64,
) -> Option<Vec<crate::data::DistPoint>> {
    let items = items?;
    let max_abs = items
        .iter()
        .map(|item| item.points.abs())
        .fold(0.0_f64, f64::max);
    if max_abs <= VALIDATION_EPS {
        return None;
    }
    Some(
        items
            .into_iter()
            .map(|item| crate::data::DistPoint {
                min: item.min,
                max: item.max,
                points: direction_sign * item.points.abs() / max_abs,
            })
            .collect(),
    )
}

fn build_recent_decay_dist_points(
    scope_windows: usize,
    direction_sign: f64,
) -> Vec<crate::data::DistPoint> {
    let half_life = ((scope_windows.max(2) - 1) as f64 / 2.0).max(1.0);
    (0..scope_windows)
        .map(|offset| crate::data::DistPoint {
            min: offset,
            max: offset,
            points: direction_sign * 0.5_f64.powf(offset as f64 / half_life),
        })
        .collect()
}

fn build_validation_calibration_specs(
    seed_rule: &ValidationSeedRule,
) -> Vec<ValidationCalibrationSpec> {
    let direction_sign = if seed_rule.points < 0.0 { -1.0 } else { 1.0 };
    let current_scope_label = scope_way_config_label(seed_rule.scope_way);
    let mut specs = vec![ValidationCalibrationSpec {
        candidate_key: "current".to_string(),
        scope_way: seed_rule.scope_way,
        scope_windows: seed_rule.scope_windows,
        scope_label: format!("{}（当前）", current_scope_label),
        dist_points: normalize_calibration_dist_points(
            seed_rule.dist_points.clone(),
            direction_sign,
        ),
        is_current: true,
    }];
    let mut seen = HashSet::from([format!(
        "{}:{}",
        current_scope_label, seed_rule.scope_windows
    )]);

    let mut push_plain = |scope_way: ScopeWay, scope_windows: usize| {
        let label = scope_way_config_label(scope_way);
        let dedupe_key = format!("{label}:{scope_windows}");
        if !seen.insert(dedupe_key) {
            return;
        }
        specs.push(ValidationCalibrationSpec {
            candidate_key: format!(
                "{}-{}",
                label.to_ascii_lowercase().replace(">=", "-"),
                scope_windows
            ),
            scope_way,
            scope_windows,
            scope_label: label,
            dist_points: None,
            is_current: false,
        });
    };

    push_plain(ScopeWay::Last, 1);
    for window in [3, 5, 10] {
        push_plain(ScopeWay::Any, window);
    }
    for window in [3, 5, 10] {
        push_plain(ScopeWay::Each, window);
    }
    for (threshold, windows) in [(2, [3, 5, 10]), (3, [3, 5, 10])] {
        for window in windows {
            if window >= threshold {
                push_plain(ScopeWay::Consec(threshold), window);
            }
        }
    }
    drop(push_plain);

    for window in [3, 5, 10] {
        specs.push(ValidationCalibrationSpec {
            candidate_key: format!("recent-decay-{window}"),
            scope_way: ScopeWay::Recent,
            scope_windows: window,
            scope_label: "RECENT（自动衰减）".to_string(),
            dist_points: Some(build_recent_decay_dist_points(window, direction_sign)),
            is_current: false,
        });
    }
    specs
}

fn sample_std_f64(values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (values.len() - 1) as f64;
    variance.is_finite().then_some(variance.sqrt())
}

fn round_to_half(value: f64) -> f64 {
    (value * 2.0).round() / 2.0
}

fn calibration_stability_factor(
    direction_sign: f64,
    early_mean: Option<f64>,
    late_mean: Option<f64>,
) -> f64 {
    match (
        early_mean.map(|value| value * direction_sign),
        late_mean.map(|value| value * direction_sign),
    ) {
        (Some(early), Some(late)) if early > 0.0 && late > 0.0 => 1.0,
        (Some(early), Some(late)) if early > 0.0 || late > 0.0 => 0.5,
        _ => 0.0,
    }
}

fn calibration_scope_structure_factor(scope_way: ScopeWay, monotonicity: Option<f64>) -> f64 {
    match scope_way {
        ScopeWay::Each => monotonicity.unwrap_or(0.25).clamp(0.25, 1.0),
        ScopeWay::Recent => (0.5 + monotonicity.unwrap_or(0.5) * 0.5).clamp(0.5, 1.0),
        _ => 1.0,
    }
}

fn build_calibration_candidate(
    spec: &ValidationCalibrationSpec,
    direction_sign: f64,
    metrics: crate::simulate::rule::RuleLayerMetrics,
    runtime_cache: &RuleLayerRuntimeCache,
    triggered_score_map: &ValidationTriggeredScoreMap,
) -> Result<RuleExpressionCalibrationCandidate, String> {
    let mut daily_excess = metrics
        .points
        .iter()
        .filter_map(|point| {
            point
                .avg_excess_residual_return
                .filter(|value| value.is_finite())
                .map(|value| (point.trade_date.clone(), value))
        })
        .collect::<Vec<_>>();
    daily_excess.sort_by(|left, right| left.0.cmp(&right.0));
    let daily_values = daily_excess
        .iter()
        .map(|(_, value)| *value)
        .collect::<Vec<_>>();
    let daily_mean = mean_f64(&daily_values);
    let daily_std = sample_std_f64(&daily_values);
    let standard_error = daily_std.map(|std| std / (daily_values.len() as f64).sqrt());
    let conservative_edge = daily_mean.zip(standard_error).map(|(mean, se)| {
        let oriented_lcb = mean * direction_sign - VALIDATION_CALIBRATION_LCB_Z * se;
        direction_sign * oriented_lcb
    });

    let split_index = daily_excess.len() / 2;
    let early_values = daily_excess[..split_index]
        .iter()
        .map(|(_, value)| *value)
        .collect::<Vec<_>>();
    let late_values = daily_excess[split_index..]
        .iter()
        .map(|(_, value)| *value)
        .collect::<Vec<_>>();
    let early_mean = mean_f64(&early_values);
    let late_mean = mean_f64(&late_values);
    let stability_factor = calibration_stability_factor(direction_sign, early_mean, late_mean);

    let mut trigger_samples = 0usize;
    let mut triggered_days = HashSet::new();
    let mut multiplier_sum = 0.0;
    let mut bucket_map = HashMap::<u64, ValidationCalibrationBucketAgg>::new();
    visit_triggered_rule_samples_from_cache(runtime_cache, triggered_score_map, |sample| {
        let score_multiplier = sample.rule_score.abs();
        if !score_multiplier.is_finite() || score_multiplier <= VALIDATION_EPS {
            return Ok(());
        }
        trigger_samples += 1;
        triggered_days.insert(sample.trade_date.to_string());
        multiplier_sum += score_multiplier;
        let entry = bucket_map
            .entry(score_multiplier.to_bits())
            .or_insert_with(|| ValidationCalibrationBucketAgg {
                score_multiplier,
                ..ValidationCalibrationBucketAgg::default()
            });
        entry.sample_count += 1;
        entry.residual_sum += sample.residual_return;
        Ok(())
    })?;
    let triggered_day_count = triggered_days.len();
    let avg_score_multiplier = if trigger_samples > 0 {
        Some(multiplier_sum / trigger_samples as f64)
    } else {
        None
    };

    let mut score_buckets = bucket_map
        .into_values()
        .map(|bucket| RuleExpressionCalibrationBucket {
            score_multiplier: bucket.score_multiplier,
            sample_count: bucket.sample_count,
            avg_residual_return: (bucket.sample_count > 0)
                .then_some(bucket.residual_sum / bucket.sample_count as f64),
        })
        .collect::<Vec<_>>();
    score_buckets.sort_by(|left, right| {
        left.score_multiplier
            .partial_cmp(&right.score_multiplier)
            .unwrap_or(Ordering::Equal)
    });
    let monotonic_buckets = score_buckets
        .iter()
        .filter(|bucket| bucket.sample_count >= 30)
        .filter_map(|bucket| {
            bucket
                .avg_residual_return
                .map(|value| value * direction_sign)
        })
        .collect::<Vec<_>>();
    let score_monotonicity = if monotonic_buckets.len() >= 2 {
        let monotonic_pairs = monotonic_buckets
            .windows(2)
            .filter(|window| window[1] + VALIDATION_EPS >= window[0])
            .count();
        Some(monotonic_pairs as f64 / (monotonic_buckets.len() - 1) as f64)
    } else {
        None
    };

    let enough_samples = trigger_samples >= VALIDATION_CALIBRATION_MIN_SAMPLES
        && triggered_day_count >= VALIDATION_CALIBRATION_MIN_DAYS;
    let oriented_lcb = conservative_edge.map(|value| value * direction_sign);
    let (status, status_label) = if !enough_samples {
        ("insufficient", "样本不足")
    } else if oriented_lcb.is_none_or(|value| value <= 0.0) {
        ("no_edge", "保守边际不足")
    } else if stability_factor < 1.0 {
        ("unstable", "前后段不稳定")
    } else {
        ("reliable", "相对稳定")
    };

    let normalized_edge = match (oriented_lcb, daily_std) {
        (Some(edge), Some(std)) if edge > 0.0 && std > VALIDATION_EPS => edge / std,
        _ => 0.0,
    };
    let structure_factor = calibration_scope_structure_factor(spec.scope_way, score_monotonicity);
    let ic_support = metrics
        .ic_t_value
        .filter(|value| value.is_finite() && *value > 0.0)
        .map(|value| value / (daily_values.len().max(1) as f64).sqrt())
        .unwrap_or(0.0);
    let calibration_score = if enough_samples {
        (normalized_edge + ic_support * 0.15) * stability_factor * structure_factor
    } else {
        0.0
    };
    let desired_total_points = if enough_samples && normalized_edge > 0.0 {
        round_to_half(
            (VALIDATION_CALIBRATION_POINT_SCALE
                * normalized_edge
                * stability_factor
                * structure_factor)
                .clamp(0.0, 10.0),
        )
    } else {
        0.0
    };
    let unit_points_abs = avg_score_multiplier
        .filter(|value| *value > VALIDATION_EPS)
        .map(|value| round_to_half((desired_total_points / value).clamp(0.0, 10.0)))
        .unwrap_or(0.0);
    let suggested_points = direction_sign * unit_points_abs;
    let suggested_total_points = direction_sign * desired_total_points;
    let suggested_dist_points = spec
        .dist_points
        .as_ref()
        .map(|items| {
            items
                .iter()
                .map(|item| RuleExpressionCalibrationDistancePoint {
                    min: item.min,
                    max: item.max,
                    points: suggested_points * item.points.abs(),
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    Ok(RuleExpressionCalibrationCandidate {
        candidate_key: spec.candidate_key.clone(),
        scope_way: scope_way_config_label(spec.scope_way),
        scope_label: spec.scope_label.clone(),
        scope_windows: spec.scope_windows,
        is_current: spec.is_current,
        trigger_samples,
        triggered_days: triggered_day_count,
        avg_daily_trigger: if triggered_day_count > 0 {
            trigger_samples as f64 / triggered_day_count as f64
        } else {
            0.0
        },
        avg_residual_mean: metrics.avg_residual_mean,
        avg_excess_residual_mean: daily_mean,
        daily_std,
        standard_error,
        conservative_edge,
        early_excess_residual_mean: early_mean,
        late_excess_residual_mean: late_mean,
        ic_mean: metrics.ic_mean,
        ic_t_value: metrics.ic_t_value,
        score_monotonicity,
        avg_score_multiplier,
        suggested_points,
        suggested_total_points,
        calibration_score,
        status: status.to_string(),
        status_label: status_label.to_string(),
        score_buckets,
        suggested_dist_points,
    })
}

fn calibration_status_rank(status: &str) -> usize {
    match status {
        "reliable" => 0,
        "unstable" => 1,
        "no_edge" => 2,
        _ => 3,
    }
}

pub fn run_rule_expression_calibration(
    continuation_id: String,
    combo_key: String,
) -> Result<RuleExpressionCalibrationData, String> {
    let continuation_id = continuation_id.trim().to_string();
    let combo_key = combo_key.trim().to_string();
    if continuation_id.is_empty() || combo_key.is_empty() {
        return Err("继续验证标识和参数组合不能为空".to_string());
    }
    let session = load_validation_continuation_session(&continuation_id)?;
    let combo = session
        .combos
        .get(&combo_key)
        .cloned()
        .ok_or_else(|| format!("继续验证基础数据中不存在参数组合:{combo_key}"))?;
    let direction_sign = if session.seed_rule.points < 0.0 {
        -1.0
    } else {
        1.0
    };
    let specs = build_validation_calibration_specs(&session.seed_rule);
    let mut prepared = Vec::with_capacity(specs.len());
    for spec in &specs {
        let cached_rule = build_validation_cached_rule(
            format!("calibration__{}", spec.candidate_key),
            spec.scope_way,
            spec.scope_windows,
            direction_sign,
            spec.dist_points.clone(),
            session.seed_rule.tag,
            &combo.formula,
        )?;
        prepared.push(PreparedValidationCombo {
            variant: ValidationVariant {
                combo_key: spec.candidate_key.clone(),
                combo_label: spec.scope_label.clone(),
                formula: combo.formula.clone(),
                unknown_values: Vec::new(),
            },
            assigned_names: collect_validation_assigned_names(&cached_rule.when_ast),
            cached_rule,
        });
    }

    let max_warmup_need = prepared.iter().try_fold(0usize, |current, item| {
        estimate_rule_warmup(
            &item.cached_rule.when_ast,
            item.cached_rule.scope_way,
            item.cached_rule.scope_windows,
        )
        .map(|need| current.max(need))
    })?;
    let need_rows = calc_query_need_rows(
        &session.source_path,
        max_warmup_need,
        &session.params.start_date,
        &session.params.end_date,
    )?;
    let query_start_date = calc_query_start_date(
        &session.source_path,
        max_warmup_need,
        &session.params.start_date,
    )?;
    let st_list = load_st_list(&session.source_path)?;
    let triggered_maps = build_validation_triggered_scores_for_combos(
        &session.source_path,
        &session.params.stock_adj_type,
        &query_start_date,
        &session.params.start_date,
        &session.params.end_date,
        need_rows,
        &session.validation_ts_codes,
        &st_list,
        &prepared,
    )?;
    let layer_config = RuleLayerConfig {
        min_samples_per_day: session.params.min_samples_per_day,
        backtest_period: session.params.backtest_period,
        min_listed_trade_days: session.params.min_listed_trade_days,
    };
    let mut candidates = Vec::with_capacity(specs.len());
    for (spec, triggered_score_map) in specs.iter().zip(triggered_maps.iter()) {
        let metrics = calc_rule_layer_metrics_from_cache(
            session.runtime_cache.as_ref(),
            triggered_score_map,
            &layer_config,
        )?;
        candidates.push(build_calibration_candidate(
            spec,
            direction_sign,
            metrics,
            session.runtime_cache.as_ref(),
            triggered_score_map,
        )?);
    }

    let recommended_candidate_key = candidates
        .iter()
        .filter(|item| matches!(item.status.as_str(), "reliable" | "unstable"))
        .max_by(|left, right| {
            calibration_status_rank(right.status.as_str())
                .cmp(&calibration_status_rank(left.status.as_str()))
                .then_with(|| {
                    left.calibration_score
                        .partial_cmp(&right.calibration_score)
                        .unwrap_or(Ordering::Equal)
                })
        })
        .map(|item| item.candidate_key.clone());
    candidates.sort_by(|left, right| {
        calibration_status_rank(left.status.as_str())
            .cmp(&calibration_status_rank(right.status.as_str()))
            .then_with(|| {
                right
                    .calibration_score
                    .partial_cmp(&left.calibration_score)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| left.scope_label.cmp(&right.scope_label))
            .then_with(|| left.scope_windows.cmp(&right.scope_windows))
    });

    Ok(RuleExpressionCalibrationData {
        continuation_id,
        combo_key: combo.combo_key,
        combo_label: combo.combo_label,
        direction: if direction_sign < 0.0 {
            "negative".to_string()
        } else {
            "positive".to_string()
        },
        candidate_count: candidates.len(),
        point_scale_description:
            "建议分使用按交易日超额残差的90%保守边际；4分约对应0.1个日度标准差，EACH/RECENT同时折算为单次基础分"
                .to_string(),
        recommended_candidate_key,
        candidates,
    })
}

#[derive(Debug, Clone)]
struct JointRidgeAggregate {
    feature_cross_products: Vec<f64>,
    feature_residual_products: Vec<f64>,
    residual_sum_squares: f64,
    sample_count: usize,
    exposed_sample_count: usize,
}

#[derive(Debug, Clone)]
struct JointWalkForwardModel {
    fold_index: usize,
    train_start_date: String,
    train_end_date: String,
    test_start_date: String,
    test_end_date: String,
    train_days: usize,
    purge_days: usize,
    test_days: usize,
    ridge_alpha: f64,
    ridge_beta: Vec<f64>,
    head_test_dates: Vec<String>,
    test_residual_sum_squares: f64,
    ridge_oos_r2: Option<f64>,
    current_score_oos_r2: Option<f64>,
    oos_contributions: Vec<f64>,
}

#[derive(Debug, Default, Clone)]
struct JointHeadFoldAggregate {
    ridge_excess_sum: f64,
    current_excess_sum: f64,
    day_count: usize,
}

#[derive(Debug, Clone, Copy)]
enum JointHeadSize {
    Fixed(usize),
    Fraction(f64),
}

#[derive(Debug, Clone, Copy)]
struct JointHeadDefinition {
    key: &'static str,
    label: &'static str,
    size: JointHeadSize,
}

const RULE_JOINT_HEAD_DEFINITIONS: [JointHeadDefinition; 6] = [
    JointHeadDefinition {
        key: "top20",
        label: "Top20",
        size: JointHeadSize::Fixed(20),
    },
    JointHeadDefinition {
        key: "top50",
        label: "Top50",
        size: JointHeadSize::Fixed(50),
    },
    JointHeadDefinition {
        key: "top100",
        label: "Top100",
        size: JointHeadSize::Fixed(100),
    },
    JointHeadDefinition {
        key: "top1pct",
        label: "Top1%",
        size: JointHeadSize::Fraction(0.01),
    },
    JointHeadDefinition {
        key: "top5pct",
        label: "Top5%",
        size: JointHeadSize::Fraction(0.05),
    },
    JointHeadDefinition {
        key: "top10pct",
        label: "Top10%",
        size: JointHeadSize::Fraction(0.10),
    },
];

fn resolve_joint_head_count(definition: JointHeadDefinition, sample_count: usize) -> usize {
    match definition.size {
        JointHeadSize::Fixed(count) => count,
        JointHeadSize::Fraction(fraction) => (sample_count as f64 * fraction).ceil() as usize,
    }
    .max(1)
    .min(sample_count)
}

fn evaluate_joint_validation_gate(
    valid_fold_count: usize,
    winning_fold_count: usize,
    ridge_head_excess_mean: Option<f64>,
    current_head_excess_mean: Option<f64>,
    latest_fold_passed: bool,
) -> (bool, usize, String) {
    let required_winning_folds = if valid_fold_count == 0 {
        0
    } else {
        (valid_fold_count * 3).div_ceil(5)
    };
    let enough_folds = valid_fold_count >= 3;
    let aggregate_passed = ridge_head_excess_mean
        .zip(current_head_excess_mean)
        .is_some_and(|(ridge, current)| ridge > current);
    let fold_win_passed = winning_fold_count >= required_winning_folds.max(1);
    let passed = enough_folds && aggregate_passed && fold_win_passed && latest_fold_passed;
    let label = if !enough_folds {
        "未通过：有效走步折少于3折"
    } else if !aggregate_passed {
        "未通过：Top100总体未胜过当前总分"
    } else if !fold_win_passed {
        "未通过：Top100获胜折数不足60%"
    } else if !latest_fold_passed {
        "未通过：最近一折Top100未胜过当前总分"
    } else {
        "通过：允许输出经过单策略稳定性门槛的建议分"
    };
    (passed, required_winning_folds, label.to_string())
}

fn aggregate_joint_ridge_days(
    days: &[RuleJointRidgeDayStats],
    feature_count: usize,
) -> JointRidgeAggregate {
    let mut aggregate = JointRidgeAggregate {
        feature_cross_products: vec![0.0; feature_count * feature_count],
        feature_residual_products: vec![0.0; feature_count],
        residual_sum_squares: 0.0,
        sample_count: 0,
        exposed_sample_count: 0,
    };
    for day in days {
        for (target, value) in aggregate
            .feature_cross_products
            .iter_mut()
            .zip(day.feature_cross_products.iter())
        {
            *target += *value;
        }
        for (target, value) in aggregate
            .feature_residual_products
            .iter_mut()
            .zip(day.feature_residual_products.iter())
        {
            *target += *value;
        }
        aggregate.residual_sum_squares += day.residual_sum_squares;
        aggregate.sample_count += day.sample_count;
        aggregate.exposed_sample_count += day.exposed_sample_count;
    }
    aggregate
}

fn solve_positive_definite(mut matrix: Vec<f64>, mut rhs: Vec<f64>) -> Option<Vec<f64>> {
    let size = rhs.len();
    if size == 0 || matrix.len() != size * size {
        return None;
    }

    for row in 0..size {
        for col in 0..=row {
            let mut value = matrix[row * size + col];
            for inner in 0..col {
                value -= matrix[row * size + inner] * matrix[col * size + inner];
            }
            if row == col {
                if !value.is_finite() || value <= VALIDATION_EPS {
                    return None;
                }
                matrix[row * size + col] = value.sqrt();
            } else {
                matrix[row * size + col] = value / matrix[col * size + col];
            }
        }
    }

    for row in 0..size {
        let mut value = rhs[row];
        for col in 0..row {
            value -= matrix[row * size + col] * rhs[col];
        }
        rhs[row] = value / matrix[row * size + row];
    }
    for row in (0..size).rev() {
        let mut value = rhs[row];
        for col in (row + 1)..size {
            value -= matrix[col * size + row] * rhs[col];
        }
        rhs[row] = value / matrix[row * size + row];
    }
    rhs.iter().all(|value| value.is_finite()).then_some(rhs)
}

fn fit_joint_ridge(
    aggregate: &JointRidgeAggregate,
    feature_count: usize,
    alpha: f64,
) -> Option<Vec<f64>> {
    if aggregate.sample_count == 0 || feature_count == 0 {
        return None;
    }
    let mut scales = vec![1.0; feature_count];
    for feature_index in 0..feature_count {
        let diagonal =
            aggregate.feature_cross_products[feature_index * feature_count + feature_index];
        if diagonal > VALIDATION_EPS {
            scales[feature_index] = (diagonal / aggregate.sample_count as f64).sqrt();
        }
    }

    let mut standardized_cross = vec![0.0; feature_count * feature_count];
    let mut standardized_target = vec![0.0; feature_count];
    let penalty = alpha.max(1e-8) * aggregate.sample_count as f64;
    for row in 0..feature_count {
        standardized_target[row] = aggregate.feature_residual_products[row] / scales[row];
        for col in 0..feature_count {
            standardized_cross[row * feature_count + col] = aggregate.feature_cross_products
                [row * feature_count + col]
                / (scales[row] * scales[col]);
        }
        standardized_cross[row * feature_count + row] += penalty;
    }

    let standardized_beta = solve_positive_definite(standardized_cross, standardized_target)?;
    Some(
        standardized_beta
            .into_iter()
            .zip(scales)
            .map(|(coefficient, scale)| coefficient / scale)
            .collect(),
    )
}

fn joint_prediction_gain(beta: &[f64], aggregate: &JointRidgeAggregate) -> f64 {
    let feature_count = beta.len();
    let linear = beta
        .iter()
        .zip(aggregate.feature_residual_products.iter())
        .map(|(coefficient, target)| coefficient * target)
        .sum::<f64>();
    let mut quadratic = 0.0;
    for row in 0..feature_count {
        for col in 0..feature_count {
            quadratic +=
                beta[row] * aggregate.feature_cross_products[row * feature_count + col] * beta[col];
        }
    }
    2.0 * linear - quadratic
}

fn joint_prediction_r2(beta: &[f64], aggregate: &JointRidgeAggregate) -> Option<f64> {
    (aggregate.residual_sum_squares > VALIDATION_EPS)
        .then_some(joint_prediction_gain(beta, aggregate) / aggregate.residual_sum_squares)
}

fn joint_oos_contributions(beta: &[f64], aggregate: &JointRidgeAggregate) -> Vec<f64> {
    let feature_count = beta.len();
    (0..feature_count)
        .map(|row| {
            let fitted_cross = (0..feature_count)
                .map(|col| aggregate.feature_cross_products[row * feature_count + col] * beta[col])
                .sum::<f64>();
            beta[row] * (2.0 * aggregate.feature_residual_products[row] - fitted_cross)
        })
        .collect()
}

fn fit_current_score_scale(aggregate: &JointRidgeAggregate, current_weights: &[f64]) -> Vec<f64> {
    let feature_count = current_weights.len();
    let numerator = current_weights
        .iter()
        .zip(aggregate.feature_residual_products.iter())
        .map(|(weight, target)| weight * target)
        .sum::<f64>();
    let mut denominator = 0.0;
    for row in 0..feature_count {
        for col in 0..feature_count {
            denominator += current_weights[row]
                * aggregate.feature_cross_products[row * feature_count + col]
                * current_weights[col];
        }
    }
    let scale = if denominator > VALIDATION_EPS {
        (numerator / (denominator * 1.001)).max(0.0)
    } else {
        0.0
    };
    current_weights
        .iter()
        .map(|weight| weight * scale)
        .collect()
}

fn choose_joint_ridge_alpha(
    training_days: &[RuleJointRidgeDayStats],
    feature_count: usize,
    purge_days: usize,
) -> f64 {
    let validation_days = (training_days.len() / 5).max(RULE_JOINT_WALK_FORWARD_MIN_TEST_DAYS);
    if training_days.len() < RULE_JOINT_WALK_FORWARD_MIN_TRAIN_DAYS + purge_days + validation_days {
        return 0.1;
    }
    let validation_start = training_days.len() - validation_days;
    let inner_train_end = validation_start.saturating_sub(purge_days);
    if inner_train_end < RULE_JOINT_WALK_FORWARD_MIN_TRAIN_DAYS {
        return 0.1;
    }
    let train = aggregate_joint_ridge_days(&training_days[..inner_train_end], feature_count);
    let validation = aggregate_joint_ridge_days(&training_days[validation_start..], feature_count);

    RULE_JOINT_RIDGE_ALPHAS
        .iter()
        .filter_map(|alpha| {
            fit_joint_ridge(&train, feature_count, *alpha)
                .map(|beta| (*alpha, joint_prediction_gain(&beta, &validation)))
        })
        .max_by(|left, right| left.1.partial_cmp(&right.1).unwrap_or(Ordering::Equal))
        .map(|(alpha, _)| alpha)
        .unwrap_or(0.1)
}

fn build_joint_walk_forward_models(
    days: &[RuleJointRidgeDayStats],
    feature_count: usize,
    purge_days: usize,
    current_weights: &[f64],
) -> Vec<JointWalkForwardModel> {
    let minimum_need =
        RULE_JOINT_WALK_FORWARD_MIN_TRAIN_DAYS + purge_days + RULE_JOINT_WALK_FORWARD_MIN_TEST_DAYS;
    if days.len() < minimum_need {
        return Vec::new();
    }
    let available = days.len() - RULE_JOINT_WALK_FORWARD_MIN_TRAIN_DAYS - purge_days;
    let test_days =
        (available / RULE_JOINT_WALK_FORWARD_MAX_FOLDS).max(RULE_JOINT_WALK_FORWARD_MIN_TEST_DAYS);
    let fold_count = (available / test_days).min(RULE_JOINT_WALK_FORWARD_MAX_FOLDS);
    if fold_count == 0 {
        return Vec::new();
    }
    let initial_train_days = days.len() - purge_days - fold_count * test_days;

    let mut models = Vec::with_capacity(fold_count);
    for fold_offset in 0..fold_count {
        let test_start = initial_train_days + purge_days + fold_offset * test_days;
        let train_end = test_start - purge_days;
        let test_end = (test_start + test_days).min(days.len());
        if train_end < RULE_JOINT_WALK_FORWARD_MIN_TRAIN_DAYS || test_start >= test_end {
            continue;
        }
        let training_days = &days[..train_end];
        let testing_days = &days[test_start..test_end];
        let ridge_alpha = choose_joint_ridge_alpha(training_days, feature_count, purge_days);
        let train = aggregate_joint_ridge_days(training_days, feature_count);
        let test = aggregate_joint_ridge_days(testing_days, feature_count);
        let Some(ridge_beta) = fit_joint_ridge(&train, feature_count, ridge_alpha) else {
            continue;
        };
        let current_beta = fit_current_score_scale(&train, current_weights);
        models.push(JointWalkForwardModel {
            fold_index: models.len() + 1,
            train_start_date: days[0].trade_date.clone(),
            train_end_date: days[train_end - 1].trade_date.clone(),
            test_start_date: days[test_start].trade_date.clone(),
            test_end_date: days[test_end - 1].trade_date.clone(),
            train_days: train_end,
            purge_days,
            test_days: test_end - test_start,
            ridge_alpha,
            ridge_oos_r2: joint_prediction_r2(&ridge_beta, &test),
            current_score_oos_r2: joint_prediction_r2(&current_beta, &test),
            oos_contributions: joint_oos_contributions(&ridge_beta, &test),
            ridge_beta,
            head_test_dates: testing_days
                .iter()
                .filter(|day| day.exposed_sample_count > 0)
                .map(|day| day.trade_date.clone())
                .collect(),
            test_residual_sum_squares: test.residual_sum_squares,
        });
    }
    models
}

fn finish_joint_head_day(
    fold_index: usize,
    rows: &mut Vec<(f64, f64, f64)>,
    aggregates: &mut [Vec<JointHeadFoldAggregate>],
) {
    if rows.is_empty()
        || aggregates
            .first()
            .is_none_or(|folds| fold_index >= folds.len())
    {
        rows.clear();
        return;
    }
    let market_mean = rows.iter().map(|item| item.2).sum::<f64>() / rows.len() as f64;
    let mut ridge_sorted = rows.clone();
    let mut current_sorted = rows.clone();
    ridge_sorted.sort_by(|left, right| right.0.partial_cmp(&left.0).unwrap_or(Ordering::Equal));
    current_sorted.sort_by(|left, right| right.1.partial_cmp(&left.1).unwrap_or(Ordering::Equal));
    for (definition_index, definition) in RULE_JOINT_HEAD_DEFINITIONS.iter().enumerate() {
        let head_count = resolve_joint_head_count(*definition, rows.len());
        let ridge_mean = ridge_sorted[..head_count]
            .iter()
            .map(|item| item.2)
            .sum::<f64>()
            / head_count as f64;
        let current_mean = current_sorted[..head_count]
            .iter()
            .map(|item| item.2)
            .sum::<f64>()
            / head_count as f64;
        aggregates[definition_index][fold_index].ridge_excess_sum += ridge_mean - market_mean;
        aggregates[definition_index][fold_index].current_excess_sum += current_mean - market_mean;
        aggregates[definition_index][fold_index].day_count += 1;
    }
    rows.clear();
}

fn evaluate_joint_walk_forward_heads(
    runtime_cache: &RuleLayerRuntimeCache,
    exposures_by_ts_date: &HashMap<String, HashMap<String, Vec<(usize, f64)>>>,
    models: &[JointWalkForwardModel],
    current_weights: &[f64],
) -> Result<Vec<Vec<JointHeadFoldAggregate>>, String> {
    let mut aggregates = vec![
        vec![JointHeadFoldAggregate::default(); models.len()];
        RULE_JOINT_HEAD_DEFINITIONS.len()
    ];
    let mut active_fold_index = None;
    let mut active_trade_date = String::new();
    let mut day_rows = Vec::<(f64, f64, f64)>::new();

    visit_rule_layer_base_samples_from_cache(runtime_cache, |sample| {
        let fold_index = models.iter().position(|model| {
            model
                .head_test_dates
                .binary_search_by(|date| date.as_str().cmp(sample.trade_date))
                .is_ok()
        });
        if active_fold_index != fold_index || active_trade_date != sample.trade_date {
            if let Some(previous_fold_index) = active_fold_index {
                finish_joint_head_day(previous_fold_index, &mut day_rows, &mut aggregates);
            } else {
                day_rows.clear();
            }
            active_fold_index = fold_index;
            active_trade_date = sample.trade_date.to_string();
        }
        let Some(fold_index) = fold_index else {
            return Ok(());
        };
        let exposures = exposures_by_ts_date
            .get(sample.ts_code)
            .and_then(|date_map| date_map.get(sample.trade_date));
        let ridge_score = exposures
            .into_iter()
            .flatten()
            .map(|(feature_index, value)| models[fold_index].ridge_beta[*feature_index] * *value)
            .sum::<f64>();
        let current_score = exposures
            .into_iter()
            .flatten()
            .map(|(feature_index, value)| current_weights[*feature_index] * *value)
            .sum::<f64>();
        day_rows.push((ridge_score, current_score, sample.residual_return));
        Ok(())
    })?;
    if let Some(fold_index) = active_fold_index {
        finish_joint_head_day(fold_index, &mut day_rows, &mut aggregates);
    }
    Ok(aggregates)
}

fn build_joint_exposures(
    features: &[RuleJointValidationFeature],
    detail_rows: &[ScoreDetails],
) -> (
    HashMap<String, HashMap<String, Vec<(usize, f64)>>>,
    Vec<usize>,
) {
    let feature_indices = features
        .iter()
        .enumerate()
        .map(|(index, feature)| (feature.rule_name.as_str(), index))
        .collect::<HashMap<_, _>>();
    let mut exposures = HashMap::<String, HashMap<String, Vec<(usize, f64)>>>::new();
    let mut trigger_samples = vec![0usize; features.len()];
    for row in detail_rows {
        let Some(feature_index) = feature_indices.get(row.rule_name.as_str()).copied() else {
            continue;
        };
        let value = row.rule_score / features[feature_index].score_scale;
        if !value.is_finite() || value.abs() <= VALIDATION_EPS {
            continue;
        }
        let sample_exposures = exposures
            .entry(row.ts_code.clone())
            .or_default()
            .entry(row.trade_date.clone())
            .or_default();
        if let Some((_, existing)) = sample_exposures
            .iter_mut()
            .find(|(index, _)| *index == feature_index)
        {
            *existing += value;
        } else {
            sample_exposures.push((feature_index, value));
            trigger_samples[feature_index] += 1;
        }
    }
    (exposures, trigger_samples)
}

fn round_joint_points(value: f64) -> f64 {
    (value.clamp(0.0, 10.0) * 2.0).round() / 2.0
}

pub fn run_rule_joint_ridge_validation(
    continuation_id: String,
) -> Result<RuleJointRidgeValidationData, String> {
    let continuation_id = continuation_id.trim().to_string();
    if continuation_id.is_empty() {
        return Err("排名整体继续验证标识不能为空".to_string());
    }
    let session = load_rule_joint_validation_session(&continuation_id)?;
    if session.features.is_empty() {
        return Err("当前策略配置中没有可用于联合回归的策略".to_string());
    }
    let feature_count = session.features.len();
    let source_db = source_db_path(&session.source_path);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "原始库路径不是有效UTF-8".to_string())?;
    let source_conn =
        Connection::open(source_db_str).map_err(|error| format!("打开原始库失败:{error}"))?;
    let layer_config = RuleLayerConfig {
        min_samples_per_day: session.params.min_samples_per_day,
        backtest_period: session.params.backtest_period,
        min_listed_trade_days: session.params.min_listed_trade_days,
    };
    let runtime_cache = build_rule_layer_runtime_cache_from_summary_rows(
        &source_conn,
        &session.source_path,
        session.summary_rows.as_ref(),
        &session.params.stock_adj_type,
        &session.params.index_ts_code,
        session.params.index_beta,
        session.params.concept_beta,
        session.params.industry_beta,
        &session.params.start_date,
        &session.params.end_date,
        &layer_config,
    )?;
    let detail_rows = load_score_detail_rows_from_db(
        &session.source_path,
        &session.params.start_date,
        &session.params.end_date,
    )?;
    let (exposures, trigger_samples) = build_joint_exposures(&session.features, &detail_rows);
    let current_weights = session
        .features
        .iter()
        .map(|feature| feature.score_scale)
        .collect::<Vec<_>>();
    let days = calc_rule_joint_ridge_day_stats_from_cache_head_weighted(
        &runtime_cache,
        &exposures,
        feature_count,
        session.params.min_samples_per_day,
        &current_weights,
    );
    if days.is_empty() {
        return Err("没有形成可用于整体岭回归的股票交易日样本".to_string());
    }

    let purge_days = session.params.backtest_period.max(1);
    let models =
        build_joint_walk_forward_models(&days, feature_count, purge_days, &current_weights);
    let head_aggregates =
        evaluate_joint_walk_forward_heads(&runtime_cache, &exposures, &models, &current_weights)?;
    let mut selected_alphas = models
        .iter()
        .map(|model| model.ridge_alpha)
        .collect::<Vec<_>>();
    selected_alphas.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    let selected_ridge_alpha = selected_alphas
        .get(selected_alphas.len() / 2)
        .copied()
        .unwrap_or(0.1);
    let full = aggregate_joint_ridge_days(&days, feature_count);
    if full.exposed_sample_count == 0 {
        return Err("回测区间内没有任何策略触发样本，无法执行整体岭回归".to_string());
    }
    let full_beta = fit_joint_ridge(&full, feature_count, selected_ridge_alpha)
        .ok_or_else(|| "整体岭回归求解失败，请检查策略特征是否全部为空".to_string())?;
    let residual_std = if full.sample_count > 0 && full.residual_sum_squares > VALIDATION_EPS {
        (full.residual_sum_squares / full.sample_count as f64).sqrt()
    } else {
        0.0
    };

    let total_oos_yty = models
        .iter()
        .map(|model| model.test_residual_sum_squares)
        .sum::<f64>();
    let ridge_oos_gain = models
        .iter()
        .map(|model| model.ridge_oos_r2.unwrap_or(0.0) * model.test_residual_sum_squares)
        .sum::<f64>();
    let current_oos_gain = models
        .iter()
        .map(|model| model.current_score_oos_r2.unwrap_or(0.0) * model.test_residual_sum_squares)
        .sum::<f64>();
    let ridge_oos_r2 = (total_oos_yty > VALIDATION_EPS).then_some(ridge_oos_gain / total_oos_yty);
    let current_score_oos_r2 =
        (total_oos_yty > VALIDATION_EPS).then_some(current_oos_gain / total_oos_yty);

    let head_metrics = RULE_JOINT_HEAD_DEFINITIONS
        .iter()
        .zip(head_aggregates.iter())
        .map(|(definition, folds)| {
            let evaluated_day_count = folds.iter().map(|item| item.day_count).sum::<usize>();
            let valid_folds = folds
                .iter()
                .filter(|item| item.day_count > 0)
                .collect::<Vec<_>>();
            let ridge_winning_fold_count = valid_folds
                .iter()
                .filter(|item| item.ridge_excess_sum > item.current_excess_sum)
                .count();
            RuleJointHeadMetric {
                key: definition.key.to_string(),
                label: definition.label.to_string(),
                ridge_head_excess_mean: (evaluated_day_count > 0).then_some(
                    folds.iter().map(|item| item.ridge_excess_sum).sum::<f64>()
                        / evaluated_day_count as f64,
                ),
                current_head_excess_mean: (evaluated_day_count > 0).then_some(
                    folds
                        .iter()
                        .map(|item| item.current_excess_sum)
                        .sum::<f64>()
                        / evaluated_day_count as f64,
                ),
                ridge_winning_fold_count,
                valid_fold_count: valid_folds.len(),
                evaluated_day_count,
            }
        })
        .collect::<Vec<_>>();
    let primary_head = &head_metrics[RULE_JOINT_PRIMARY_HEAD_INDEX];
    let latest_head_fold_passed = head_aggregates[RULE_JOINT_PRIMARY_HEAD_INDEX]
        .iter()
        .rev()
        .find(|item| item.day_count > 0)
        .is_some_and(|item| item.ridge_excess_sum > item.current_excess_sum);
    let (validation_passed, required_head_winning_folds, validation_status_label) =
        evaluate_joint_validation_gate(
            primary_head.valid_fold_count,
            primary_head.ridge_winning_fold_count,
            primary_head.ridge_head_excess_mean,
            primary_head.current_head_excess_mean,
            latest_head_fold_passed,
        );

    let mut contribution_sums = vec![0.0; feature_count];
    let mut contribution_positive_counts = vec![0usize; feature_count];
    for model in &models {
        for (feature_index, contribution) in model.oos_contributions.iter().enumerate() {
            contribution_sums[feature_index] += *contribution;
            if *contribution > 0.0 {
                contribution_positive_counts[feature_index] += 1;
            }
        }
    }

    let mut rules = Vec::with_capacity(feature_count);
    for feature_index in 0..feature_count {
        let feature = &session.features[feature_index];
        let diagonal = full.feature_cross_products[feature_index * feature_count + feature_index];
        let feature_std = if full.sample_count > 0 && diagonal > VALIDATION_EPS {
            (diagonal / full.sample_count as f64).sqrt()
        } else {
            0.0
        };
        let coefficient = full_beta[feature_index];
        let standardized_coefficient = if residual_std > VALIDATION_EPS {
            coefficient * feature_std / residual_std
        } else {
            0.0
        };
        let raw_points_abs = if residual_std > VALIDATION_EPS && coefficient > 0.0 {
            round_joint_points(RULE_JOINT_POINT_SCALE * coefficient / residual_std)
        } else {
            0.0
        };
        let positive_fold_rate = (!models.is_empty())
            .then_some(contribution_positive_counts[feature_index] as f64 / models.len() as f64);
        let direction_sign = if feature.current_points < 0.0 {
            -1.0
        } else {
            1.0
        };

        let mut max_correlation = None::<f64>;
        let mut most_correlated_rule = None;
        for other_index in 0..feature_count {
            if other_index == feature_index {
                continue;
            }
            let other_diagonal =
                full.feature_cross_products[other_index * feature_count + other_index];
            let denominator = (diagonal * other_diagonal).sqrt();
            if denominator <= VALIDATION_EPS {
                continue;
            }
            let correlation = full.feature_cross_products
                [feature_index * feature_count + other_index]
                / denominator;
            if max_correlation.is_none_or(|current| correlation.abs() > current.abs()) {
                max_correlation = Some(correlation);
                most_correlated_rule = Some(session.features[other_index].rule_name.clone());
            }
        }

        let current_abs = feature.current_points.abs();
        let oos_contribution = (total_oos_yty > VALIDATION_EPS)
            .then_some(contribution_sums[feature_index] / total_oos_yty);
        let rule_stability_passed = positive_fold_rate
            .is_some_and(|rate| rate + VALIDATION_EPS >= RULE_JOINT_RULE_MIN_POSITIVE_FOLD_RATE)
            && oos_contribution.is_some_and(|value| value > 0.0);
        let diagnostic_points_abs =
            round_joint_points(raw_points_abs * positive_fold_rate.unwrap_or(0.0));
        let diagnostic_points = direction_sign * diagnostic_points_abs;
        let (suggested_points, status, status_label) = if trigger_samples[feature_index] < 100 {
            (feature.current_points, "insufficient", "样本不足，暂不改分")
        } else if models.is_empty() {
            (
                feature.current_points,
                "insufficient",
                "走步区间不足，暂不改分",
            )
        } else if !validation_passed && coefficient <= 0.0 {
            (feature.current_points, "observe", "观察：联合方向反转")
        } else if !validation_passed && !rule_stability_passed {
            (feature.current_points, "observe", "观察：样本外贡献不稳")
        } else if !validation_passed {
            (feature.current_points, "hold", "整体未通过，暂不改分")
        } else if !rule_stability_passed {
            (feature.current_points, "observe", "单策略门槛未通过")
        } else if coefficient <= 0.0 {
            (0.0, "suppress", "稳定反向，建议停用")
        } else if diagnostic_points_abs + 0.25 < current_abs {
            (diagnostic_points, "reduce", "建议压低")
        } else if diagnostic_points_abs > current_abs + 0.25 {
            (diagnostic_points, "increase", "建议提高")
        } else {
            (feature.current_points, "keep", "建议保持")
        };
        rules.push(RuleJointRidgeRuleResult {
            rule_name: feature.rule_name.clone(),
            explain: feature.explain.clone(),
            current_points: feature.current_points,
            score_scale: feature.score_scale,
            trigger_samples: trigger_samples[feature_index],
            ridge_coefficient: coefficient,
            standardized_coefficient,
            raw_suggested_points: direction_sign * raw_points_abs,
            suggested_points,
            point_change: suggested_points - feature.current_points,
            positive_fold_rate,
            oos_contribution,
            max_correlation,
            most_correlated_rule,
            status: status.to_string(),
            status_label: status_label.to_string(),
        });
    }
    rules.sort_by(|left, right| {
        left.suggested_points
            .abs()
            .partial_cmp(&right.suggested_points.abs())
            .unwrap_or(Ordering::Equal)
            .reverse()
            .then_with(|| left.rule_name.cmp(&right.rule_name))
    });

    let folds = models
        .iter()
        .zip(head_aggregates[RULE_JOINT_PRIMARY_HEAD_INDEX].iter())
        .map(|(model, head)| RuleJointWalkForwardFold {
            fold_index: model.fold_index,
            train_start_date: model.train_start_date.clone(),
            train_end_date: model.train_end_date.clone(),
            test_start_date: model.test_start_date.clone(),
            test_end_date: model.test_end_date.clone(),
            train_days: model.train_days,
            purge_days: model.purge_days,
            test_days: model.test_days,
            ridge_alpha: model.ridge_alpha,
            ridge_oos_r2: model.ridge_oos_r2,
            current_score_oos_r2: model.current_score_oos_r2,
            ridge_head_excess_mean: (head.day_count > 0)
                .then_some(head.ridge_excess_sum / head.day_count as f64),
            current_head_excess_mean: (head.day_count > 0)
                .then_some(head.current_excess_sum / head.day_count as f64),
        })
        .collect::<Vec<_>>();
    let ridge_head_excess_mean = primary_head.ridge_head_excess_mean;
    let current_head_excess_mean = primary_head.current_head_excess_mean;
    let head_winning_fold_count = primary_head.ridge_winning_fold_count;
    let primary_head_definition = RULE_JOINT_HEAD_DEFINITIONS[RULE_JOINT_PRIMARY_HEAD_INDEX];

    Ok(RuleJointRidgeValidationData {
        continuation_id,
        start_date: session.params.start_date.clone(),
        end_date: session.params.end_date.clone(),
        feature_count,
        sample_count: full.sample_count,
        exposed_sample_count: full.exposed_sample_count,
        valid_days: days.len(),
        fold_count: folds.len(),
        purge_days,
        selected_ridge_alpha,
        ridge_oos_r2,
        current_score_oos_r2,
        ridge_head_excess_mean,
        current_head_excess_mean,
        primary_head_key: primary_head_definition.key.to_string(),
        primary_head_label: primary_head_definition.label.to_string(),
        head_metrics,
        validation_passed,
        validation_status_label,
        head_winning_fold_count,
        required_head_winning_folds,
        latest_head_fold_passed,
        training_weight_description:
            "按当前总分排名进行头部加权：Top20/50/100权重最高，Top1%/5%/10%依次递减，其余样本保留基础权重；每日权重归一化避免改变岭参数尺度"
                .to_string(),
        point_scale_description:
            "只有整体Top100通过、最近一折通过，且单策略至少80%测试折正贡献并且累计样本外贡献为正时才输出改分；否则建议分保持当前值"
                .to_string(),
        folds,
        rules,
    })
}

fn load_stock_name_map(source_path: &str) -> Result<HashMap<String, String>, String> {
    let rows = load_stock_list(source_path)?;
    let mut out = HashMap::with_capacity(rows.len());

    for cols in rows {
        let Some(ts_code) = cols.first().map(|value| value.trim()) else {
            continue;
        };
        let Some(name_raw) = cols.get(2).map(|value| value.trim()) else {
            continue;
        };
        if ts_code.is_empty() || name_raw.is_empty() {
            continue;
        }

        out.insert(ts_code.to_string(), name_raw.to_string());
    }

    Ok(out)
}

fn load_validation_sample_stock_meta_map(
    source_path: &str,
) -> Result<HashMap<String, ValidationSampleStockMeta>, String> {
    let rows = load_stock_list(source_path)?;
    let mut out = HashMap::with_capacity(rows.len());

    for cols in rows {
        let Some(ts_code_raw) = cols.first().map(|value| value.trim()) else {
            continue;
        };
        if ts_code_raw.is_empty() {
            continue;
        }

        let ts_code = ts_code_raw.to_ascii_uppercase();
        let stock_name = cols
            .get(2)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string());
        let board = resolve_validation_sample_board_label(
            &ts_code,
            stock_name.as_deref(),
            cols.get(14)
                .map(|value| value.trim())
                .filter(|value| !value.is_empty()),
        );

        out.insert(
            ts_code,
            ValidationSampleStockMeta {
                name: stock_name,
                volatility_group: derive_validation_volatility_group(&board).to_string(),
                board,
            },
        );
    }

    Ok(out)
}

fn resolve_validation_sample_board_label(
    ts_code: &str,
    stock_name: Option<&str>,
    market_label: Option<&str>,
) -> String {
    let category_board = board_category(ts_code, stock_name);
    if category_board == "ST" {
        return category_board.to_string();
    }

    if let Some(board) = market_label.and_then(normalize_validation_market_label) {
        return board;
    }

    category_board.to_string()
}

fn normalize_validation_market_label(market_label: &str) -> Option<String> {
    let market_label = market_label.trim();
    if market_label.is_empty() {
        return None;
    }

    if market_label.contains("北交") {
        return Some("北交所".to_string());
    }
    if market_label.contains("科创") {
        return Some("科创板".to_string());
    }
    if market_label.contains("创业") {
        return Some("创业板".to_string());
    }
    if market_label.contains("主板") {
        return Some("主板".to_string());
    }

    Some(market_label.to_string())
}

fn derive_validation_volatility_group(board: &str) -> &'static str {
    let board = board.trim();
    if board.contains("北交") || board.contains("创业") || board.contains("科创") {
        "高波动"
    } else if board == "ST" {
        "其他波动"
    } else if board.contains("主板") {
        "常规波动"
    } else {
        "其他波动"
    }
}

fn split_board_tags(board_raw: &str) -> Vec<String> {
    board_raw
        .split(|ch| matches!(ch, ',' | ';' | '，' | '；' | '|' | '、' | '/' | '\n' | '\r'))
        .map(|part| part.trim())
        .filter(|part| !part.is_empty())
        .map(|part| part.to_string())
        .collect()
}

/// 缓存 `build_board_maps` 的解析结果，避免每次回测都读取并解析 stock_list.csv。
static BOARD_MAPS_CACHE: Mutex<Option<(String, Vec<String>, HashMap<String, Vec<String>>)>> =
    Mutex::new(None);

fn get_or_build_board_maps(
    source_path: &str,
) -> Result<(Vec<String>, HashMap<String, Vec<String>>), String> {
    {
        let cache = BOARD_MAPS_CACHE
            .lock()
            .map_err(|e| format!("读取板块映射缓存失败: {e}"))?;
        if let Some((cached_path, board_options, ts_board_map)) = cache.as_ref() {
            if cached_path == source_path {
                return Ok((board_options.clone(), ts_board_map.clone()));
            }
        }
    }
    let (board_options, ts_board_map) = build_board_maps(source_path)?;
    let mut cache = BOARD_MAPS_CACHE
        .lock()
        .map_err(|e| format!("写入板块映射缓存失败: {e}"))?;
    *cache = Some((
        source_path.to_string(),
        board_options.clone(),
        ts_board_map.clone(),
    ));
    Ok((board_options, ts_board_map))
}

fn build_board_maps(
    source_path: &str,
) -> Result<(Vec<String>, HashMap<String, Vec<String>>), String> {
    let stock_rows = load_stock_list(source_path)?;
    let mut ts_board_map: HashMap<String, Vec<String>> = HashMap::with_capacity(stock_rows.len());
    let mut board_set: HashSet<String> = HashSet::new();

    for cols in stock_rows {
        let Some(ts_code_raw) = cols.first().map(|value| value.trim()) else {
            continue;
        };
        let ts_code = ts_code_raw.to_ascii_uppercase();
        let stock_name = cols
            .get(2)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty());

        let mut board_list = Vec::new();
        let category_board = board_category(&ts_code, stock_name).to_string();
        board_set.insert(category_board.clone());
        board_list.push(category_board);

        if let Some(board_raw) = cols.get(14).map(|value| value.trim()) {
            if !board_raw.is_empty() {
                let detail_boards = split_board_tags(board_raw);
                for board in detail_boards {
                    if board_list.iter().any(|item| item == &board) {
                        continue;
                    }
                    board_set.insert(board.clone());
                    board_list.push(board);
                }
            }
        }

        ts_board_map.insert(ts_code, board_list);
    }

    let mut board_options = board_set.into_iter().collect::<Vec<_>>();
    board_options.sort();

    Ok((board_options, ts_board_map))
}

fn resolve_board_filter(requested: Option<String>, board_options: &[String]) -> Option<String> {
    let requested = requested
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if let Some(board) = requested {
        if board_options.iter().any(|item| item == &board) {
            return Some(board);
        }
    }
    None
}

fn match_board_filter(board_list: &[String], selected_board: Option<&str>) -> bool {
    let Some(selected_board) = selected_board else {
        return true;
    };
    board_list.iter().any(|board| board == selected_board)
}

fn match_board_filter_with_st(
    board_list: &[String],
    selected_board: Option<&str>,
    exclude_st_board: bool,
) -> bool {
    if exclude_st_board && board_list.iter().any(|board| board == "ST") {
        return false;
    }
    match_board_filter(board_list, selected_board)
}

fn normalize_market_value_bounds(
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
) -> Result<(Option<f64>, Option<f64>), String> {
    let total_mv_min = total_mv_min.filter(|value| value.is_finite());
    let total_mv_max = total_mv_max.filter(|value| value.is_finite());
    if let (Some(min_v), Some(max_v)) = (total_mv_min, total_mv_max) {
        if min_v > max_v {
            return Err("总市值最小值不能大于最大值".to_string());
        }
    }
    Ok((total_mv_min, total_mv_max))
}

fn build_backtest_stock_filter(
    source_path: &str,
    board: Option<String>,
    exclude_st_board: Option<bool>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
) -> Result<
    (
        Option<String>,
        bool,
        Option<f64>,
        Option<f64>,
        Option<HashSet<String>>,
    ),
    String,
> {
    let exclude_st_board = exclude_st_board.unwrap_or(false);
    let requested_board = board
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty() && value != "全部");
    let (total_mv_min, total_mv_max) = normalize_market_value_bounds(total_mv_min, total_mv_max)?;
    let has_mv_filter = total_mv_min.is_some() || total_mv_max.is_some();

    if requested_board.is_none() && !exclude_st_board && !has_mv_filter {
        return Ok((None, false, None, None, None));
    }

    let (board_options, ts_board_map) = get_or_build_board_maps(source_path)?;
    let resolved_board = resolve_board_filter(requested_board, &board_options);
    let total_mv_map = if has_mv_filter {
        build_total_mv_map(source_path)?
    } else {
        HashMap::new()
    };
    let allowed_ts_codes = ts_board_map
        .into_iter()
        .filter_map(|(ts_code, board_list)| {
            if match_board_filter_with_st(&board_list, resolved_board.as_deref(), exclude_st_board)
                && filter_mv(&total_mv_map, &ts_code, total_mv_min, total_mv_max)
            {
                Some(ts_code)
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();

    Ok((
        resolved_board,
        exclude_st_board,
        total_mv_min,
        total_mv_max,
        Some(allowed_ts_codes),
    ))
}

fn build_industry_maps(
    source_path: &str,
) -> Result<(HashMap<String, Vec<String>>, HashMap<String, usize>), String> {
    let stock_rows = load_stock_list(source_path)?;
    Ok(build_industry_maps_from_rows(stock_rows))
}

fn build_industry_maps_from_rows(
    stock_rows: Vec<Vec<String>>,
) -> (HashMap<String, Vec<String>>, HashMap<String, usize>) {
    let mut ts_industry_map: HashMap<String, Vec<String>> =
        HashMap::with_capacity(stock_rows.len());
    let mut industry_stocks: HashMap<String, HashSet<String>> = HashMap::new();

    for cols in stock_rows {
        let Some(ts_code) = cols
            .first()
            .map(|value| value.trim().to_ascii_uppercase())
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let Some(industry_raw) = cols.get(4).map(|value| value.trim()) else {
            continue;
        };
        if industry_raw.is_empty() {
            continue;
        }

        let industries = split_board_tags(industry_raw);
        for industry in &industries {
            industry_stocks
                .entry(industry.clone())
                .or_default()
                .insert(ts_code.clone());
        }
        if !industries.is_empty() {
            ts_industry_map.insert(ts_code, industries);
        }
    }

    let industry_stock_counts = industry_stocks
        .into_iter()
        .map(|(industry, stocks)| (industry, stocks.len()))
        .collect();
    (ts_industry_map, industry_stock_counts)
}

fn has_min_stock_count(
    stock_counts: &HashMap<String, usize>,
    name: &str,
    min_stock_count: usize,
) -> bool {
    min_stock_count <= 1 || stock_counts.get(name).copied().unwrap_or(0) >= min_stock_count
}

fn build_concept_maps(
    source_path: &str,
) -> Result<(HashMap<String, Vec<String>>, HashMap<String, usize>), String> {
    let rows = match load_ths_concepts_list(source_path) {
        Ok(rows) => rows,
        Err(error) if error.contains("打开stock_concepts.csv失败") => {
            return Ok((HashMap::new(), HashMap::new()));
        }
        Err(error) => return Err(error),
    };
    let mut ts_concept_map: HashMap<String, Vec<String>> = HashMap::new();
    let mut concept_stocks: HashMap<String, HashSet<String>> = HashMap::new();

    for cols in rows {
        let Some(ts_code) = cols
            .first()
            .map(|value| value.trim().to_ascii_uppercase())
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let Some(concept_raw) = cols.get(2).map(|value| value.trim()) else {
            continue;
        };
        if concept_raw.is_empty() {
            continue;
        }

        let concepts = split_board_tags(concept_raw);
        for concept in &concepts {
            concept_stocks
                .entry(concept.clone())
                .or_default()
                .insert(ts_code.clone());
        }
        if !concepts.is_empty() {
            ts_concept_map.entry(ts_code).or_default().extend(concepts);
        }
    }

    for concepts in ts_concept_map.values_mut() {
        concepts.sort();
        concepts.dedup();
    }
    let concept_stock_counts = concept_stocks
        .into_iter()
        .map(|(concept, stocks)| (concept, stocks.len()))
        .collect();
    Ok((ts_concept_map, concept_stock_counts))
}

fn estimate_net_money_flow_yuan(net_mf_vol: f64, vol: f64, amount: f64) -> Option<f64> {
    if !net_mf_vol.is_finite()
        || !vol.is_finite()
        || !amount.is_finite()
        || vol <= f64::EPSILON
        || amount < 0.0
    {
        return None;
    }

    // Tushare 日线 amount 的单位为千元，vol / net_mf_vol 的单位均为手。
    // 用成交额 / 成交量得到当日均价后折算净流入金额，结果统一为元。
    let value = net_mf_vol / vol * amount * 1_000.0;
    value.is_finite().then_some(value)
}

fn accumulate_board_money_flow(
    acc: &mut HashMap<String, f64>,
    board_map: &HashMap<String, Vec<String>>,
    ts_code: &str,
    net_amount_yuan: f64,
) {
    let Some(boards) = board_map.get(ts_code) else {
        return;
    };
    for board in boards {
        *acc.entry(board.clone()).or_insert(0.0) += net_amount_yuan;
    }
}

fn money_flow_rank_items(
    acc: HashMap<String, f64>,
    stock_counts: &HashMap<String, usize>,
    min_stock_count: usize,
) -> Vec<MarketRankItem> {
    let mut items = acc
        .into_iter()
        .filter_map(|(name, value)| {
            if value <= 0.0
                || !value.is_finite()
                || !has_min_stock_count(stock_counts, &name, min_stock_count)
            {
                return None;
            }
            Some(market_rank_item(name, value))
        })
        .collect::<Vec<_>>();
    items.sort_by(|a, b| {
        b.value
            .partial_cmp(&a.value)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.name.cmp(&b.name))
    });
    items.truncate(20);
    items
}

fn money_outflow_rank_items(
    acc: HashMap<String, f64>,
    stock_counts: &HashMap<String, usize>,
    min_stock_count: usize,
) -> Vec<MarketRankItem> {
    let mut items = acc
        .into_iter()
        .filter_map(|(name, value)| {
            if value >= 0.0
                || !value.is_finite()
                || !has_min_stock_count(stock_counts, &name, min_stock_count)
            {
                return None;
            }
            Some(market_rank_item(name, value))
        })
        .collect::<Vec<_>>();
    items.sort_by(|a, b| {
        a.value
            .partial_cmp(&b.value)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.name.cmp(&b.name))
    });
    items.truncate(20);
    items
}

fn stock_data_has_money_flow_columns(conn: &Connection) -> Result<bool, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT LOWER(column_name)
            FROM information_schema.columns
            WHERE LOWER(table_name) = 'stock_data'
            "#,
        )
        .map_err(|e| format!("预编译资金流向列检查失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("执行资金流向列检查失败: {e}"))?;
    let mut columns = HashSet::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取资金流向列检查失败: {e}"))?
    {
        let name: String = row
            .get(0)
            .map_err(|e| format!("读取资金流向列名失败: {e}"))?;
        columns.insert(name);
    }
    Ok(["net_mf_v", "vol", "amount"]
        .iter()
        .all(|name| columns.contains(*name)))
}

pub fn get_market_analysis(
    source_path: String,
    lookback_period: Option<usize>,
    reference_trade_date: Option<String>,
    board: Option<String>,
    exclude_st_board: Option<bool>,
    min_listed_trade_days: Option<usize>,
    stock_rank_limit: Option<usize>,
    sub_interval_period: Option<usize>,
    min_board_stock_count: Option<usize>,
) -> Result<MarketAnalysisData, String> {
    let lookback_period = lookback_period.unwrap_or(20).max(1);
    let stock_rank_limit = stock_rank_limit.unwrap_or(20).clamp(1, 200);
    let sub_interval_period = if lookback_period >= 3 {
        sub_interval_period.unwrap_or(3).max(3).min(lookback_period)
    } else {
        3
    };
    let min_listed_trade_days =
        min_listed_trade_days.unwrap_or(DEFAULT_BACKTEST_MIN_LISTED_TRADE_DAYS);
    let min_board_stock_count = min_board_stock_count.unwrap_or(1).max(1);

    let source_db = source_db_path(&source_path);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "原始库路径不是有效UTF-8".to_string())?;
    let source_conn =
        Connection::open(source_db_str).map_err(|e| format!("打开原始库失败: {e}"))?;

    let latest_trade_date: Option<String> = source_conn
        .query_row(
            "SELECT MAX(trade_date) FROM stock_data WHERE adj_type = 'qfq'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| format!("查询最新交易日失败: {e}"))?;

    let resolved_reference_trade_date = reference_trade_date
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| latest_trade_date.clone());

    let (board_options, ts_board_map) = get_or_build_board_maps(&source_path)?;
    let (ts_concept_map, concept_stock_counts) = build_concept_maps(&source_path)?;
    let (ts_industry_map, industry_stock_counts) = build_industry_maps(&source_path)?;
    let resolved_board = resolve_board_filter(board, &board_options);
    let exclude_st_board = exclude_st_board.unwrap_or(false);
    let sample_eligibility =
        build_backtest_sample_eligibility(&source_path, min_listed_trade_days)?;

    let Some(ref_date) = resolved_reference_trade_date.clone() else {
        return Ok(MarketAnalysisData {
            lookback_period,
            stock_rank_limit,
            sub_interval_period,
            min_board_stock_count,
            latest_trade_date,
            resolved_reference_trade_date: None,
            board_options,
            resolved_board,
            interval: MarketAnalysisSnapshot {
                trade_date: None,
                concept_top: Vec::new(),
                industry_top: Vec::new(),
                concept_money_flow_top: Vec::new(),
                industry_money_flow_top: Vec::new(),
                concept_money_outflow_top: Vec::new(),
                industry_money_outflow_top: Vec::new(),
                gain_top: Vec::new(),
                sub_interval_gain_top: Vec::new(),
            },
            daily: MarketAnalysisSnapshot {
                trade_date: None,
                concept_top: Vec::new(),
                industry_top: Vec::new(),
                concept_money_flow_top: Vec::new(),
                industry_money_flow_top: Vec::new(),
                concept_money_outflow_top: Vec::new(),
                industry_money_outflow_top: Vec::new(),
                gain_top: Vec::new(),
                sub_interval_gain_top: Vec::new(),
            },
        });
    };

    let mut date_stmt = source_conn
        .prepare(
            r#"
            SELECT trade_date
            FROM (
                SELECT DISTINCT trade_date
                FROM stock_data
                WHERE adj_type = 'qfq'
                  AND trade_date <= ?
                ORDER BY trade_date DESC
                LIMIT ?
            ) AS t
            ORDER BY trade_date ASC
            "#,
        )
        .map_err(|e| format!("预编译市场分析区间日期 SQL 失败: {e}"))?;
    let mut date_rows = date_stmt
        .query(params![&ref_date, lookback_period as i64])
        .map_err(|e| format!("执行市场分析区间日期 SQL 失败: {e}"))?;
    let mut dates = Vec::new();
    while let Some(row) = date_rows
        .next()
        .map_err(|e| format!("读取市场分析区间日期失败: {e}"))?
    {
        let trade_date: String = row.get(0).map_err(|e| format!("读取交易日失败: {e}"))?;
        dates.push(trade_date);
    }

    if dates.is_empty() {
        return Ok(MarketAnalysisData {
            lookback_period,
            stock_rank_limit,
            sub_interval_period,
            min_board_stock_count,
            latest_trade_date,
            resolved_reference_trade_date: Some(ref_date.clone()),
            board_options,
            resolved_board,
            interval: MarketAnalysisSnapshot {
                trade_date: None,
                concept_top: Vec::new(),
                industry_top: Vec::new(),
                concept_money_flow_top: Vec::new(),
                industry_money_flow_top: Vec::new(),
                concept_money_outflow_top: Vec::new(),
                industry_money_outflow_top: Vec::new(),
                gain_top: Vec::new(),
                sub_interval_gain_top: Vec::new(),
            },
            daily: MarketAnalysisSnapshot {
                trade_date: Some(ref_date),
                concept_top: Vec::new(),
                industry_top: Vec::new(),
                concept_money_flow_top: Vec::new(),
                industry_money_flow_top: Vec::new(),
                concept_money_outflow_top: Vec::new(),
                industry_money_outflow_top: Vec::new(),
                gain_top: Vec::new(),
                sub_interval_gain_top: Vec::new(),
            },
        });
    }

    let interval_start = dates.first().cloned().unwrap_or_else(|| ref_date.clone());
    let interval_end = dates.last().cloned().unwrap_or_else(|| ref_date.clone());

    let concept_db = concept_performance_db_path(&source_path);
    let concept_db_str = concept_db
        .to_str()
        .ok_or_else(|| "概念表现库路径不是有效UTF-8".to_string())?;
    let concept_conn =
        Connection::open(concept_db_str).map_err(|e| format!("打开概念表现库失败: {e}"))?;
    let concept_interval_sql = r#"
        SELECT concept, AVG(TRY_CAST(performance_pct AS DOUBLE)) AS avg_pct
        FROM concept_performance
        WHERE performance_type = 'concept'
          AND trade_date >= ?
          AND trade_date <= ?
        GROUP BY 1
        ORDER BY avg_pct DESC NULLS LAST, concept ASC
        "#;

    let mut concept_interval_stmt = concept_conn
        .prepare(concept_interval_sql)
        .map_err(|e| format!("预编译概念区间榜 SQL 失败: {e}"))?;
    let mut concept_interval_rows = concept_interval_stmt
        .query(params![&interval_start, &interval_end])
        .map_err(|e| format!("执行概念区间榜 SQL 失败: {e}"))?;
    let mut interval_concept_top = Vec::new();
    while let Some(row) = concept_interval_rows
        .next()
        .map_err(|e| format!("读取概念区间榜失败: {e}"))?
    {
        let name: String = row.get(0).map_err(|e| format!("读取概念名失败: {e}"))?;
        if !has_min_stock_count(&concept_stock_counts, &name, min_board_stock_count) {
            continue;
        }
        let value: Option<f64> = row.get(1).map_err(|e| format!("读取概念值失败: {e}"))?;
        if let Some(value) = value.filter(|v| v.is_finite()) {
            interval_concept_top.push(market_rank_item(name, value));
        }
    }
    interval_concept_top.truncate(20);

    let mut interval_industry_stmt = source_conn
        .prepare(
            r#"
            SELECT ts_code, AVG(TRY_CAST(pct_chg AS DOUBLE)) AS avg_pct
            FROM stock_data
            WHERE adj_type = 'qfq'
              AND trade_date >= ?
              AND trade_date <= ?
            GROUP BY 1
            "#,
        )
        .map_err(|e| format!("预编译行业区间榜 SQL 失败: {e}"))?;
    let mut interval_industry_rows = interval_industry_stmt
        .query(params![&interval_start, &interval_end])
        .map_err(|e| format!("执行行业区间榜 SQL 失败: {e}"))?;
    let mut interval_industry_acc: HashMap<String, (f64, usize)> = HashMap::new();
    while let Some(row) = interval_industry_rows
        .next()
        .map_err(|e| format!("读取行业区间榜失败: {e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取代码失败: {e}"))?;
        let avg_pct: Option<f64> = row.get(1).map_err(|e| format!("读取行业值失败: {e}"))?;
        let Some(avg_pct) = avg_pct.filter(|v| v.is_finite()) else {
            continue;
        };
        let ts_code = ts_code.to_ascii_uppercase();
        let Some(industry_list) = ts_industry_map.get(&ts_code) else {
            continue;
        };
        for industry in industry_list {
            let entry = interval_industry_acc
                .entry(industry.clone())
                .or_insert((0.0, 0));
            entry.0 += avg_pct;
            entry.1 += 1;
        }
    }
    let mut interval_industry_top = interval_industry_acc
        .into_iter()
        .filter_map(|(name, (sum, cnt))| {
            if cnt == 0 {
                return None;
            }
            if !has_min_stock_count(&industry_stock_counts, &name, min_board_stock_count) {
                return None;
            }
            let value = sum / cnt as f64;
            if !value.is_finite() {
                return None;
            }
            Some(market_rank_item(name, value))
        })
        .collect::<Vec<_>>();
    interval_industry_top.sort_by(|a, b| {
        b.value
            .partial_cmp(&a.value)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.name.cmp(&b.name))
    });
    interval_industry_top.truncate(20);

    let stock_name_map = load_stock_name_map(&source_path)?;

    let mut interval_gain_stmt = source_conn
        .prepare(
            r#"
            SELECT ts_code, trade_date, TRY_CAST(close AS DOUBLE) AS close_price
            FROM stock_data
            WHERE adj_type = 'qfq'
              AND trade_date >= ?
              AND trade_date <= ?
            ORDER BY trade_date ASC, ts_code ASC
            "#,
        )
        .map_err(|e| format!("预编译涨幅区间榜 SQL 失败: {e}"))?;
    let mut interval_gain_rows = interval_gain_stmt
        .query(params![&interval_start, &interval_end])
        .map_err(|e| format!("执行涨幅区间榜 SQL 失败: {e}"))?;
    let mut interval_gain_acc: HashMap<String, Vec<(String, f64)>> = HashMap::new();
    while let Some(row) = interval_gain_rows
        .next()
        .map_err(|e| format!("读取涨幅区间榜失败: {e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取代码失败: {e}"))?;
        let trade_date: String = row.get(1).map_err(|e| format!("读取交易日失败: {e}"))?;
        let close_price: Option<f64> = row.get(2).map_err(|e| format!("读取收盘价失败: {e}"))?;
        let Some(close_price) = close_price.filter(|v| v.is_finite() && *v > f64::EPSILON) else {
            continue;
        };
        let ts_code = ts_code.to_ascii_uppercase();
        if !sample_eligibility.allows_sample(&ts_code, &trade_date) {
            continue;
        }
        let Some(board_list) = ts_board_map.get(&ts_code) else {
            continue;
        };
        if !match_board_filter_with_st(board_list, resolved_board.as_deref(), exclude_st_board) {
            continue;
        }
        interval_gain_acc
            .entry(ts_code)
            .or_default()
            .push((trade_date, close_price));
    }
    let mut interval_gain_top = interval_gain_acc
        .iter()
        .filter_map(|(ts_code, rows)| {
            let (start_date, start_close) = rows.first()?;
            let (end_date, end_close) = rows.last()?;
            if *start_close <= f64::EPSILON {
                return None;
            }
            let value = (*end_close / *start_close - 1.0) * 100.0;
            if !value.is_finite() {
                return None;
            }
            let mut rank_item = market_stock_rank_item(
                &stock_name_map,
                ts_code.clone(),
                value,
                Some(start_date.clone()),
                Some(end_date.clone()),
            );
            rank_item.concepts = ts_concept_map
                .get(ts_code)
                .map(|items| items.join(" / "))
                .filter(|value| !value.is_empty());
            Some(rank_item)
        })
        .collect::<Vec<_>>();
    interval_gain_top.sort_by(|a, b| {
        b.value
            .partial_cmp(&a.value)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.name.cmp(&b.name))
    });
    interval_gain_top.truncate(stock_rank_limit);

    let mut sub_interval_gain_top = if dates.len() >= sub_interval_period {
        interval_gain_acc
            .iter()
            .filter_map(|(ts_code, rows)| {
                if rows.len() < sub_interval_period {
                    return None;
                }
                let mut best: Option<(f64, String, String)> = None;
                for window in rows.windows(sub_interval_period) {
                    let Some((start_date, start_close)) = window.first() else {
                        continue;
                    };
                    let Some((end_date, end_close)) = window.last() else {
                        continue;
                    };
                    if *start_close <= f64::EPSILON {
                        continue;
                    }
                    let value = (end_close / start_close - 1.0) * 100.0;
                    if !value.is_finite() {
                        continue;
                    }
                    let should_replace = best
                        .as_ref()
                        .is_none_or(|(best_value, _, _)| value > *best_value);
                    if should_replace {
                        best = Some((value, start_date.clone(), end_date.clone()));
                    }
                }
                let (value, start_date, end_date) = best?;
                Some(market_stock_rank_item(
                    &stock_name_map,
                    ts_code.clone(),
                    value,
                    Some(start_date),
                    Some(end_date),
                ))
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    sub_interval_gain_top.sort_by(|a, b| {
        b.value
            .partial_cmp(&a.value)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.name.cmp(&b.name))
    });
    sub_interval_gain_top.truncate(stock_rank_limit);

    let daily_concept_sql = r#"
        SELECT concept, TRY_CAST(performance_pct AS DOUBLE)
        FROM concept_performance
        WHERE performance_type = 'concept'
          AND trade_date = ?
        ORDER BY TRY_CAST(performance_pct AS DOUBLE) DESC NULLS LAST, concept ASC
        "#;

    let mut daily_concept_stmt = concept_conn
        .prepare(daily_concept_sql)
        .map_err(|e| format!("预编译概念当日榜 SQL 失败: {e}"))?;
    let mut daily_concept_rows = daily_concept_stmt
        .query(params![&ref_date])
        .map_err(|e| format!("执行概念当日榜 SQL 失败: {e}"))?;
    let mut daily_concept_top = Vec::new();
    while let Some(row) = daily_concept_rows
        .next()
        .map_err(|e| format!("读取概念当日榜失败: {e}"))?
    {
        let name: String = row.get(0).map_err(|e| format!("读取概念名失败: {e}"))?;
        if !has_min_stock_count(&concept_stock_counts, &name, min_board_stock_count) {
            continue;
        }
        let value: Option<f64> = row.get(1).map_err(|e| format!("读取概念值失败: {e}"))?;
        if let Some(value) = value.filter(|v| v.is_finite()) {
            daily_concept_top.push(market_rank_item(name, value));
        }
    }
    daily_concept_top.truncate(20);

    let mut daily_industry_stmt = source_conn
        .prepare(
            r#"
            SELECT ts_code, TRY_CAST(pct_chg AS DOUBLE) AS pct
            FROM stock_data
            WHERE adj_type = 'qfq'
              AND trade_date = ?
            "#,
        )
        .map_err(|e| format!("预编译行业当日榜 SQL 失败: {e}"))?;
    let mut daily_industry_rows = daily_industry_stmt
        .query(params![&ref_date])
        .map_err(|e| format!("执行行业当日榜 SQL 失败: {e}"))?;
    let mut daily_industry_acc: HashMap<String, (f64, usize)> = HashMap::new();
    while let Some(row) = daily_industry_rows
        .next()
        .map_err(|e| format!("读取行业当日榜失败: {e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取代码失败: {e}"))?;
        let pct: Option<f64> = row.get(1).map_err(|e| format!("读取行业值失败: {e}"))?;
        let Some(pct) = pct.filter(|v| v.is_finite()) else {
            continue;
        };
        let ts_code = ts_code.to_ascii_uppercase();
        let Some(industry_list) = ts_industry_map.get(&ts_code) else {
            continue;
        };
        for industry in industry_list {
            let entry = daily_industry_acc
                .entry(industry.clone())
                .or_insert((0.0, 0));
            entry.0 += pct;
            entry.1 += 1;
        }
    }
    let mut daily_industry_top = daily_industry_acc
        .into_iter()
        .filter_map(|(name, (sum, cnt))| {
            if cnt == 0 {
                return None;
            }
            if !has_min_stock_count(&industry_stock_counts, &name, min_board_stock_count) {
                return None;
            }
            let value = sum / cnt as f64;
            if !value.is_finite() {
                return None;
            }
            Some(market_rank_item(name, value))
        })
        .collect::<Vec<_>>();
    daily_industry_top.sort_by(|a, b| {
        b.value
            .partial_cmp(&a.value)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.name.cmp(&b.name))
    });
    daily_industry_top.truncate(20);

    let mut trailing_gain_stmt = source_conn
        .prepare(
            r#"
            SELECT ts_code, trade_date, TRY_CAST(close AS DOUBLE) AS close_price
            FROM stock_data
            WHERE adj_type = 'qfq'
              AND trade_date IN (
                  SELECT trade_date
                  FROM (
                      SELECT DISTINCT trade_date
                      FROM stock_data
                      WHERE adj_type = 'qfq'
                        AND trade_date <= ?
                      ORDER BY trade_date DESC
                      LIMIT 6
                  ) AS recent_dates
              )
            ORDER BY ts_code ASC, trade_date ASC
            "#,
        )
        .map_err(|e| format!("预编译当日多周期涨幅 SQL 失败: {e}"))?;
    let mut trailing_gain_rows = trailing_gain_stmt
        .query(params![&ref_date])
        .map_err(|e| format!("执行当日多周期涨幅 SQL 失败: {e}"))?;
    let mut trailing_gain_acc: HashMap<String, Vec<(String, f64)>> = HashMap::new();
    while let Some(row) = trailing_gain_rows
        .next()
        .map_err(|e| format!("读取当日多周期涨幅失败: {e}"))?
    {
        let ts_code: String = row
            .get(0)
            .map_err(|e| format!("读取多周期涨幅代码失败: {e}"))?;
        let trade_date: String = row
            .get(1)
            .map_err(|e| format!("读取多周期涨幅日期失败: {e}"))?;
        let close_price: Option<f64> = row
            .get(2)
            .map_err(|e| format!("读取多周期收盘价失败: {e}"))?;
        let Some(close_price) = close_price.filter(|value| value.is_finite() && *value > 0.0)
        else {
            continue;
        };
        trailing_gain_acc
            .entry(ts_code.trim().to_ascii_uppercase())
            .or_default()
            .push((trade_date, close_price));
    }

    let mut daily_gain_stmt = source_conn
        .prepare(
            r#"
            SELECT ts_code, TRY_CAST(pct_chg AS DOUBLE)
            FROM stock_data
            WHERE adj_type = 'qfq'
              AND trade_date = ?
            ORDER BY TRY_CAST(pct_chg AS DOUBLE) DESC NULLS LAST, ts_code ASC
            "#,
        )
        .map_err(|e| format!("预编译涨幅当日榜 SQL 失败: {e}"))?;
    let mut daily_gain_rows = daily_gain_stmt
        .query(params![&ref_date])
        .map_err(|e| format!("执行涨幅当日榜 SQL 失败: {e}"))?;
    let mut daily_gain_top = Vec::new();
    while let Some(row) = daily_gain_rows
        .next()
        .map_err(|e| format!("读取涨幅当日榜失败: {e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取代码失败: {e}"))?;
        let value: Option<f64> = row.get(1).map_err(|e| format!("读取涨幅值失败: {e}"))?;
        let Some(value) = value.filter(|v| v.is_finite()) else {
            continue;
        };
        let ts_code = ts_code.to_ascii_uppercase();
        if !sample_eligibility.allows_sample(&ts_code, &ref_date) {
            continue;
        }
        let Some(board_list) = ts_board_map.get(&ts_code) else {
            continue;
        };
        if !match_board_filter_with_st(board_list, resolved_board.as_deref(), exclude_st_board) {
            continue;
        }

        let concepts = ts_concept_map
            .get(&ts_code)
            .map(|items| items.join(" / "))
            .filter(|value| !value.is_empty());
        let trailing_rows = trailing_gain_acc
            .get(&ts_code)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let mut rank_item = market_stock_rank_item(
            &stock_name_map,
            ts_code,
            value,
            Some(ref_date.clone()),
            Some(ref_date.clone()),
        );
        rank_item.concepts = concepts;
        rank_item.three_day_gain = trailing_period_gain(trailing_rows, 3);
        rank_item.five_day_gain = trailing_period_gain(trailing_rows, 5);
        daily_gain_top.push(rank_item);
        if daily_gain_top.len() >= stock_rank_limit {
            break;
        }
    }

    let (
        interval_concept_money_flow_top,
        interval_industry_money_flow_top,
        daily_concept_money_flow_top,
        daily_industry_money_flow_top,
        interval_concept_money_outflow_top,
        interval_industry_money_outflow_top,
        daily_concept_money_outflow_top,
        daily_industry_money_outflow_top,
    ) = if stock_data_has_money_flow_columns(&source_conn)? {
        let mut money_flow_stmt = source_conn
            .prepare(
                r#"
                SELECT
                    ts_code,
                    trade_date,
                    TRY_CAST(net_mf_v AS DOUBLE) AS net_mf_vol,
                    TRY_CAST(vol AS DOUBLE) AS trade_vol,
                    TRY_CAST(amount AS DOUBLE) AS trade_amount
                FROM stock_data
                WHERE adj_type = 'qfq'
                  AND trade_date >= ?
                  AND trade_date <= ?
                  AND net_mf_v IS NOT NULL
                "#,
            )
            .map_err(|e| format!("预编译资金流向统计 SQL 失败: {e}"))?;
        let mut money_flow_rows = money_flow_stmt
            .query(params![&interval_start, &interval_end])
            .map_err(|e| format!("执行资金流向统计 SQL 失败: {e}"))?;
        let mut interval_concept_acc = HashMap::new();
        let mut interval_industry_acc = HashMap::new();
        let mut daily_concept_acc = HashMap::new();
        let mut daily_industry_acc = HashMap::new();

        while let Some(row) = money_flow_rows
            .next()
            .map_err(|e| format!("读取资金流向统计失败: {e}"))?
        {
            let ts_code: String = row
                .get(0)
                .map_err(|e| format!("读取资金流向代码失败: {e}"))?;
            let trade_date: String = row
                .get(1)
                .map_err(|e| format!("读取资金流向日期失败: {e}"))?;
            let net_mf_vol: Option<f64> =
                row.get(2).map_err(|e| format!("读取净流入量失败: {e}"))?;
            let vol: Option<f64> = row.get(3).map_err(|e| format!("读取成交量失败: {e}"))?;
            let amount: Option<f64> = row.get(4).map_err(|e| format!("读取成交额失败: {e}"))?;
            let Some(net_amount_yuan) =
                net_mf_vol
                    .zip(vol)
                    .zip(amount)
                    .and_then(|((net_mf_vol, vol), amount)| {
                        estimate_net_money_flow_yuan(net_mf_vol, vol, amount)
                    })
            else {
                continue;
            };
            let ts_code = ts_code.trim().to_ascii_uppercase();
            accumulate_board_money_flow(
                &mut interval_concept_acc,
                &ts_concept_map,
                &ts_code,
                net_amount_yuan,
            );
            accumulate_board_money_flow(
                &mut interval_industry_acc,
                &ts_industry_map,
                &ts_code,
                net_amount_yuan,
            );
            if trade_date == ref_date {
                accumulate_board_money_flow(
                    &mut daily_concept_acc,
                    &ts_concept_map,
                    &ts_code,
                    net_amount_yuan,
                );
                accumulate_board_money_flow(
                    &mut daily_industry_acc,
                    &ts_industry_map,
                    &ts_code,
                    net_amount_yuan,
                );
            }
        }

        (
            money_flow_rank_items(
                interval_concept_acc.clone(),
                &concept_stock_counts,
                min_board_stock_count,
            ),
            money_flow_rank_items(
                interval_industry_acc.clone(),
                &industry_stock_counts,
                min_board_stock_count,
            ),
            money_flow_rank_items(
                daily_concept_acc.clone(),
                &concept_stock_counts,
                min_board_stock_count,
            ),
            money_flow_rank_items(
                daily_industry_acc.clone(),
                &industry_stock_counts,
                min_board_stock_count,
            ),
            money_outflow_rank_items(
                interval_concept_acc,
                &concept_stock_counts,
                min_board_stock_count,
            ),
            money_outflow_rank_items(
                interval_industry_acc,
                &industry_stock_counts,
                min_board_stock_count,
            ),
            money_outflow_rank_items(
                daily_concept_acc,
                &concept_stock_counts,
                min_board_stock_count,
            ),
            money_outflow_rank_items(
                daily_industry_acc,
                &industry_stock_counts,
                min_board_stock_count,
            ),
        )
    } else {
        (
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
    };

    Ok(MarketAnalysisData {
        lookback_period,
        stock_rank_limit,
        sub_interval_period,
        min_board_stock_count,
        latest_trade_date,
        resolved_reference_trade_date: Some(ref_date.clone()),
        board_options,
        resolved_board,
        interval: MarketAnalysisSnapshot {
            trade_date: Some(format!("{}~{}", interval_start, interval_end)),
            concept_top: interval_concept_top,
            industry_top: interval_industry_top,
            concept_money_flow_top: interval_concept_money_flow_top,
            industry_money_flow_top: interval_industry_money_flow_top,
            concept_money_outflow_top: interval_concept_money_outflow_top,
            industry_money_outflow_top: interval_industry_money_outflow_top,
            gain_top: interval_gain_top,
            sub_interval_gain_top,
        },
        daily: MarketAnalysisSnapshot {
            trade_date: Some(ref_date),
            concept_top: daily_concept_top,
            industry_top: daily_industry_top,
            concept_money_flow_top: daily_concept_money_flow_top,
            industry_money_flow_top: daily_industry_money_flow_top,
            concept_money_outflow_top: daily_concept_money_outflow_top,
            industry_money_outflow_top: daily_industry_money_outflow_top,
            gain_top: daily_gain_top,
            sub_interval_gain_top: Vec::new(),
        },
    })
}

pub fn get_market_contribution(
    source_path: String,
    scope: String,
    kind: String,
    name: String,
    lookback_period: Option<usize>,
    reference_trade_date: Option<String>,
) -> Result<MarketContributionData, String> {
    let scope = scope.trim().to_ascii_lowercase();
    let kind = kind.trim().to_ascii_lowercase();
    let target_name = name.trim().to_string();
    if !matches!(scope.as_str(), "interval" | "daily") {
        return Err("scope 仅支持 interval/daily".to_string());
    }
    let kind = match kind.as_str() {
        "concept" => "concept".to_string(),
        "industry" | "board" | "market" => "industry".to_string(),
        _ => return Err("kind 仅支持 concept/industry".to_string()),
    };
    if target_name.is_empty() {
        return Err("名称不能为空".to_string());
    }

    let lookback_period = lookback_period.unwrap_or(20).max(1);
    let source_db = source_db_path(&source_path);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "原始库路径不是有效UTF-8".to_string())?;
    let source_conn =
        Connection::open(source_db_str).map_err(|e| format!("打开原始库失败: {e}"))?;

    let latest_trade_date: Option<String> = source_conn
        .query_row(
            "SELECT MAX(trade_date) FROM stock_data WHERE adj_type = 'qfq'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| format!("查询最新交易日失败: {e}"))?;
    let ref_date = reference_trade_date
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or(latest_trade_date)
        .ok_or_else(|| "缺少有效参考日".to_string())?;

    let mut date_stmt = source_conn
        .prepare(
            r#"
            SELECT trade_date
            FROM (
                SELECT DISTINCT trade_date
                FROM stock_data
                WHERE adj_type = 'qfq'
                  AND trade_date <= ?
                ORDER BY trade_date DESC
                LIMIT ?
            ) AS t
            ORDER BY trade_date ASC
            "#,
        )
        .map_err(|e| format!("预编译市场贡献区间日期 SQL 失败: {e}"))?;
    let mut date_rows = date_stmt
        .query(params![&ref_date, lookback_period as i64])
        .map_err(|e| format!("执行市场贡献区间日期 SQL 失败: {e}"))?;
    let mut dates = Vec::new();
    while let Some(row) = date_rows
        .next()
        .map_err(|e| format!("读取市场贡献区间日期失败: {e}"))?
    {
        let trade_date: String = row.get(0).map_err(|e| format!("读取交易日失败: {e}"))?;
        dates.push(trade_date);
    }

    if dates.is_empty() {
        return Ok(MarketContributionData {
            scope,
            kind,
            name: target_name,
            trade_date: Some(ref_date),
            start_date: None,
            end_date: None,
            lookback_period,
            contributors: Vec::new(),
        });
    }

    let interval_start = dates.first().cloned();
    let interval_end = dates.last().cloned();

    let stock_rows = load_stock_list(&source_path)?;
    let mut ts_name_map: HashMap<String, String> = HashMap::with_capacity(stock_rows.len());
    let mut ts_industry_map: HashMap<String, String> = HashMap::with_capacity(stock_rows.len());
    let mut target_codes: HashSet<String> = HashSet::new();

    for cols in stock_rows {
        let Some(ts_code) = cols.first().map(|value| value.trim()) else {
            continue;
        };
        if ts_code.is_empty() {
            continue;
        }

        let stock_name = cols
            .get(2)
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        if let Some(stock_name) = stock_name {
            ts_name_map.insert(ts_code.to_string(), stock_name);
        }

        let industry_name = cols
            .get(4)
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        if let Some(industry_name) = industry_name.clone() {
            ts_industry_map.insert(ts_code.to_string(), industry_name.clone());
        }

        if kind == "industry" {
            let is_match = industry_name
                .as_deref()
                .map(|value| {
                    value
                        .split(|ch| {
                            matches!(ch, ',' | ';' | '，' | '；' | '|' | '、' | '/' | '\n' | '\r')
                        })
                        .map(|part| part.trim())
                        .any(|part| !part.is_empty() && part == target_name)
                })
                .unwrap_or(false);
            if is_match {
                target_codes.insert(ts_code.to_string());
            }
        }
    }

    if kind == "concept" {
        let concept_rows = load_ths_concepts_list(&source_path)?;
        for cols in concept_rows {
            let Some(ts_code) = cols.first().map(|value| value.trim()) else {
                continue;
            };
            let Some(concept_raw) = cols.get(2).map(|value| value.trim()) else {
                continue;
            };
            if ts_code.is_empty() || concept_raw.is_empty() {
                continue;
            }
            let is_match = concept_raw
                .split(|ch| matches!(ch, ',' | ';' | '，' | '；' | '|' | '、' | '/' | '\n' | '\r'))
                .map(|part| part.trim())
                .any(|part| !part.is_empty() && part == target_name);
            if is_match {
                target_codes.insert(ts_code.to_string());
            }
        }
    }

    if target_codes.is_empty() {
        return Ok(MarketContributionData {
            scope,
            kind,
            name: target_name,
            trade_date: Some(ref_date),
            start_date: interval_start,
            end_date: interval_end,
            lookback_period,
            contributors: Vec::new(),
        });
    }

    let mut contributors = Vec::new();
    if scope == "daily" {
        let mut stmt = source_conn
            .prepare(
                r#"
                SELECT ts_code, TRY_CAST(pct_chg AS DOUBLE) AS pct
                FROM stock_data
                WHERE adj_type = 'qfq'
                  AND trade_date = ?
                "#,
            )
            .map_err(|e| format!("预编译市场贡献当日 SQL 失败: {e}"))?;
        let mut rows = stmt
            .query(params![&ref_date])
            .map_err(|e| format!("执行市场贡献当日 SQL 失败: {e}"))?;

        while let Some(row) = rows
            .next()
            .map_err(|e| format!("读取市场贡献当日数据失败: {e}"))?
        {
            let ts_code: String = row.get(0).map_err(|e| format!("读取代码失败: {e}"))?;
            if !target_codes.contains(&ts_code) {
                continue;
            }
            let pct: Option<f64> = row.get(1).map_err(|e| format!("读取涨幅失败: {e}"))?;
            let Some(contribution_pct) = pct.filter(|v| v.is_finite()) else {
                continue;
            };
            contributors.push(MarketContributorItem {
                ts_code: ts_code.clone(),
                name: ts_name_map.get(&ts_code).cloned(),
                industry: ts_industry_map.get(&ts_code).cloned(),
                contribution_pct,
            });
        }
    } else {
        let start = interval_start.clone().unwrap_or_else(|| ref_date.clone());
        let end = interval_end.clone().unwrap_or_else(|| ref_date.clone());
        let mut stmt = source_conn
            .prepare(
                r#"
                SELECT ts_code, AVG(TRY_CAST(pct_chg AS DOUBLE)) AS avg_pct
                FROM stock_data
                WHERE adj_type = 'qfq'
                  AND trade_date >= ?
                  AND trade_date <= ?
                GROUP BY 1
                "#,
            )
            .map_err(|e| format!("预编译市场贡献区间 SQL 失败: {e}"))?;
        let mut rows = stmt
            .query(params![&start, &end])
            .map_err(|e| format!("执行市场贡献区间 SQL 失败: {e}"))?;

        while let Some(row) = rows
            .next()
            .map_err(|e| format!("读取市场贡献区间数据失败: {e}"))?
        {
            let ts_code: String = row.get(0).map_err(|e| format!("读取代码失败: {e}"))?;
            if !target_codes.contains(&ts_code) {
                continue;
            }
            let pct: Option<f64> = row.get(1).map_err(|e| format!("读取涨幅失败: {e}"))?;
            let Some(contribution_pct) = pct.filter(|v| v.is_finite()) else {
                continue;
            };
            contributors.push(MarketContributorItem {
                ts_code: ts_code.clone(),
                name: ts_name_map.get(&ts_code).cloned(),
                industry: ts_industry_map.get(&ts_code).cloned(),
                contribution_pct,
            });
        }
    }

    contributors.sort_by(|a, b| {
        b.contribution_pct
            .partial_cmp(&a.contribution_pct)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.ts_code.cmp(&b.ts_code))
    });
    contributors.truncate(100);

    Ok(MarketContributionData {
        scope,
        kind,
        name: target_name,
        trade_date: Some(ref_date),
        start_date: interval_start,
        end_date: interval_end,
        lookback_period,
        contributors,
    })
}

pub fn get_scene_layer_backtest_defaults(
    source_path: String,
) -> Result<SceneLayerBacktestDefaultsData, String> {
    let scene_options = load_scene_options(&source_path)?;

    let conn = open_result_conn(&source_path)?;
    let mut stmt = conn
        .prepare(
            r#"
            SELECT
                MIN(trade_date) AS min_trade_date,
                MAX(trade_date) AS max_trade_date
            FROM scene_details
            "#,
        )
        .map_err(|e| format!("预编译 scene_details 日期区间 SQL 失败: {e}"))?;

    let mut rows = stmt
        .query([])
        .map_err(|e| format!("执行 scene_details 日期区间 SQL 失败: {e}"))?;

    let (start_date, end_date) = if let Some(row) = rows
        .next()
        .map_err(|e| format!("读取 scene_details 日期区间失败: {e}"))?
    {
        let min_trade_date: Option<String> =
            row.get(0).map_err(|e| format!("读取最小交易日失败: {e}"))?;
        let _max_trade_date: Option<String> =
            row.get(1).map_err(|e| format!("读取最大交易日失败: {e}"))?;
        (
            min_trade_date,
            query_score_summary_latest_trade_date(&conn)?,
        )
    } else {
        (None, query_score_summary_latest_trade_date(&conn)?)
    };

    Ok(SceneLayerBacktestDefaultsData {
        resolved_scene_name: scene_options.first().cloned(),
        scene_options,
        start_date,
        end_date,
    })
}

pub fn get_rule_layer_backtest_defaults(
    source_path: String,
) -> Result<RuleLayerBacktestDefaultsData, String> {
    let (rule_options, _) = load_rule_meta(&source_path)?;

    let conn = open_result_conn(&source_path)?;
    let mut stmt = conn
        .prepare(
            r#"
            SELECT
                MIN(trade_date) AS min_trade_date,
                MAX(trade_date) AS max_trade_date
            FROM rule_details
            "#,
        )
        .map_err(|e| format!("预编译 rule_details 日期区间 SQL 失败: {e}"))?;

    let mut rows = stmt
        .query([])
        .map_err(|e| format!("执行 rule_details 日期区间 SQL 失败: {e}"))?;

    let (start_date, end_date) = if let Some(row) = rows
        .next()
        .map_err(|e| format!("读取 rule_details 日期区间失败: {e}"))?
    {
        let min_trade_date: Option<String> =
            row.get(0).map_err(|e| format!("读取最小交易日失败: {e}"))?;
        let _max_trade_date: Option<String> =
            row.get(1).map_err(|e| format!("读取最大交易日失败: {e}"))?;
        (
            min_trade_date,
            query_score_summary_latest_trade_date(&conn)?,
        )
    } else {
        (None, query_score_summary_latest_trade_date(&conn)?)
    };

    Ok(RuleLayerBacktestDefaultsData {
        resolved_rule_name: rule_options.first().cloned(),
        rule_options,
        start_date,
        end_date,
    })
}

fn query_score_summary_latest_trade_date(conn: &Connection) -> Result<Option<String>, String> {
    let mut stmt = conn
        .prepare("SELECT MAX(trade_date) FROM score_summary")
        .map_err(|e| format!("预编译 score_summary 最新日期 SQL 失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("执行 score_summary 最新日期 SQL 失败: {e}"))?;

    if let Some(row) = rows
        .next()
        .map_err(|e| format!("读取 score_summary 最新日期失败: {e}"))?
    {
        let latest_trade_date: Option<String> = row
            .get(0)
            .map_err(|e| format!("读取 score_summary 最新日期字段失败: {e}"))?;
        return Ok(latest_trade_date.and_then(|value| {
            let trimmed = value.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        }));
    }

    Ok(None)
}

#[derive(Debug, Clone)]
struct SceneLayerBacktestRunParams {
    stock_adj_type: String,
    index_ts_code: String,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: String,
    end_date: String,
    min_samples_per_day: usize,
    min_listed_trade_days: usize,
    backtest_period: usize,
    resolved_board: Option<String>,
    exclude_st_board: bool,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
    allowed_ts_codes: Option<HashSet<String>>,
}

#[derive(Debug, Clone)]
struct RuleLayerBacktestRunParams {
    stock_adj_type: String,
    index_ts_code: String,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: String,
    end_date: String,
    min_samples_per_day: usize,
    min_listed_trade_days: usize,
    backtest_period: usize,
    parallel_batch_size: usize,
    resolved_board: Option<String>,
    exclude_st_board: bool,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
    allowed_ts_codes: Option<HashSet<String>>,
}

#[derive(Debug, Clone)]
struct RankLayerBacktestRunParams {
    stock_adj_type: String,
    index_ts_code: String,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: String,
    end_date: String,
    min_samples_per_day: usize,
    min_listed_trade_days: usize,
    backtest_period: usize,
    layer_count: usize,
    layer_method: RankLayerMethod,
    resolved_board: Option<String>,
    exclude_st_board: bool,
    allowed_ts_codes: Option<HashSet<String>>,
}

fn rule_joint_validation_params_from_rank(
    params: &RankLayerBacktestRunParams,
) -> RuleLayerBacktestRunParams {
    RuleLayerBacktestRunParams {
        stock_adj_type: params.stock_adj_type.clone(),
        index_ts_code: params.index_ts_code.clone(),
        index_beta: params.index_beta,
        concept_beta: params.concept_beta,
        industry_beta: params.industry_beta,
        start_date: params.start_date.clone(),
        end_date: params.end_date.clone(),
        min_samples_per_day: params.min_samples_per_day,
        min_listed_trade_days: params.min_listed_trade_days,
        backtest_period: params.backtest_period,
        parallel_batch_size: DEFAULT_RULE_WITH_SAMPLES_PARALLEL_BATCH_SIZE,
        resolved_board: params.resolved_board.clone(),
        exclude_st_board: params.exclude_st_board,
        total_mv_min: None,
        total_mv_max: None,
        allowed_ts_codes: params.allowed_ts_codes.clone(),
    }
}

#[derive(Debug, Clone, Default)]
struct RuleContributionAverages {
    avg_contribution_score: Option<f64>,
    avg_contribution_per_trigger: Option<f64>,
}

#[derive(Debug, Clone, Default)]
struct RuleContributionAccumulator {
    contribution_sum: f64,
    contribution_days: usize,
    trigger_count: i64,
}

fn finalize_rule_contribution_averages(
    acc_map: HashMap<String, RuleContributionAccumulator>,
) -> HashMap<String, RuleContributionAverages> {
    acc_map
        .into_iter()
        .map(|(rule_name, acc)| {
            let avg_contribution_score = if acc.contribution_days > 0 {
                Some(acc.contribution_sum / acc.contribution_days as f64)
            } else {
                None
            };
            let avg_contribution_per_trigger = if acc.trigger_count > 0 {
                Some(acc.contribution_sum / acc.trigger_count as f64)
            } else {
                None
            };

            (
                rule_name,
                RuleContributionAverages {
                    avg_contribution_score,
                    avg_contribution_per_trigger,
                },
            )
        })
        .collect()
}

fn ts_code_allowed_by_filter(allowed_ts_codes: Option<&HashSet<String>>, ts_code: &str) -> bool {
    let Some(allowed_ts_codes) = allowed_ts_codes else {
        return true;
    };
    allowed_ts_codes.contains(ts_code.trim())
        || allowed_ts_codes.contains(ts_code.trim().to_ascii_uppercase().as_str())
}

fn filter_score_summary_rows_by_ts_codes(
    rows: Vec<ScoreSummary>,
    allowed_ts_codes: Option<&HashSet<String>>,
) -> Vec<ScoreSummary> {
    if allowed_ts_codes.is_none() {
        return rows;
    }
    rows.into_iter()
        .filter(|row| ts_code_allowed_by_filter(allowed_ts_codes, &row.ts_code))
        .collect()
}

fn filter_score_detail_rows_by_ts_codes(
    rows: Vec<ScoreDetails>,
    allowed_ts_codes: Option<&HashSet<String>>,
) -> Vec<ScoreDetails> {
    if allowed_ts_codes.is_none() {
        return rows;
    }
    rows.into_iter()
        .filter(|row| ts_code_allowed_by_filter(allowed_ts_codes, &row.ts_code))
        .collect()
}

fn filter_scene_detail_rows_by_ts_codes(
    rows: Vec<SceneDetails>,
    allowed_ts_codes: Option<&HashSet<String>>,
) -> Vec<SceneDetails> {
    if allowed_ts_codes.is_none() {
        return rows;
    }
    rows.into_iter()
        .filter(|row| ts_code_allowed_by_filter(allowed_ts_codes, &row.ts_code))
        .collect()
}

fn load_score_summary_rows_from_db(
    source_path: &str,
    start_date: &str,
    end_date: &str,
    allowed_ts_codes: Option<&HashSet<String>>,
) -> Result<Vec<ScoreSummary>, String> {
    let result_conn = open_result_conn(source_path)?;

    let mut summary_stmt = result_conn
        .prepare(
            r#"
            SELECT ts_code, trade_date, total_score, rank
            FROM score_summary
            WHERE trade_date >= ?
              AND trade_date <= ?
            ORDER BY trade_date ASC, ts_code ASC
            "#,
        )
        .map_err(|e| format!("预编译策略回测总榜原始行失败: {e}"))?;
    let mut summary_rows = summary_stmt
        .query(params![start_date, end_date])
        .map_err(|e| format!("查询策略回测总榜原始行失败: {e}"))?;
    let mut summaries = Vec::new();
    while let Some(row) = summary_rows
        .next()
        .map_err(|e| format!("读取策略回测总榜原始行失败: {e}"))?
    {
        let item = ScoreSummary {
            ts_code: row.get(0).map_err(|e| format!("读取总榜代码失败: {e}"))?,
            trade_date: row.get(1).map_err(|e| format!("读取总榜日期失败: {e}"))?,
            total_score: row.get(2).map_err(|e| format!("读取总榜分数失败: {e}"))?,
            rank: row.get(3).map_err(|e| format!("读取总榜排名失败: {e}"))?,
        };
        if ts_code_allowed_by_filter(allowed_ts_codes, &item.ts_code) {
            summaries.push(item);
        }
    }

    Ok(summaries)
}

fn load_rule_backtest_score_rows_from_db(
    source_path: &str,
    start_date: &str,
    end_date: &str,
    allowed_ts_codes: Option<&HashSet<String>>,
) -> Result<(Vec<ScoreSummary>, Vec<ScoreDetails>), String> {
    let result_conn = open_result_conn(source_path)?;
    let summaries =
        load_score_summary_rows_from_db(source_path, start_date, end_date, allowed_ts_codes)?;

    let mut detail_stmt = result_conn
        .prepare(
            r#"
            SELECT ts_code, trade_date, rule_name, TRY_CAST(rule_score AS DOUBLE)
            FROM rule_details
            WHERE trade_date >= ?
              AND trade_date <= ?
              AND TRY_CAST(rule_score AS DOUBLE) IS NOT NULL
            ORDER BY trade_date ASC, rule_name ASC, ts_code ASC
            "#,
        )
        .map_err(|e| format!("预编译策略回测规则原始行失败: {e}"))?;
    let mut detail_rows = detail_stmt
        .query(params![start_date, end_date])
        .map_err(|e| format!("查询策略回测规则原始行失败: {e}"))?;
    let mut details = Vec::new();
    while let Some(row) = detail_rows
        .next()
        .map_err(|e| format!("读取策略回测规则原始行失败: {e}"))?
    {
        let rule_score: f64 = row.get(3).map_err(|e| format!("读取规则分数失败: {e}"))?;
        let item = ScoreDetails {
            ts_code: row.get(0).map_err(|e| format!("读取规则代码失败: {e}"))?,
            trade_date: row.get(1).map_err(|e| format!("读取规则日期失败: {e}"))?,
            rule_name: row.get(2).map_err(|e| format!("读取规则名称失败: {e}"))?,
            rule_score,
        };
        if rule_score.is_finite() && ts_code_allowed_by_filter(allowed_ts_codes, &item.ts_code) {
            details.push(item);
        }
    }

    Ok((summaries, details))
}

fn load_score_detail_rows_from_db(
    source_path: &str,
    start_date: &str,
    end_date: &str,
) -> Result<Vec<ScoreDetails>, String> {
    let result_conn = open_result_conn(source_path)?;
    let mut detail_stmt = result_conn
        .prepare(
            r#"
            SELECT ts_code, trade_date, rule_name, TRY_CAST(rule_score AS DOUBLE)
            FROM rule_details
            WHERE trade_date >= ?
              AND trade_date <= ?
              AND TRY_CAST(rule_score AS DOUBLE) IS NOT NULL
            ORDER BY trade_date ASC, rule_name ASC, ts_code ASC
            "#,
        )
        .map_err(|e| format!("预编译策略回测规则原始行失败: {e}"))?;
    let mut detail_rows = detail_stmt
        .query(params![start_date, end_date])
        .map_err(|e| format!("查询策略回测规则原始行失败: {e}"))?;
    let mut details = Vec::new();
    while let Some(row) = detail_rows
        .next()
        .map_err(|e| format!("读取策略回测规则原始行失败: {e}"))?
    {
        let rule_score: f64 = row.get(3).map_err(|e| format!("读取规则分数失败: {e}"))?;
        let item = ScoreDetails {
            ts_code: row.get(0).map_err(|e| format!("读取规则代码失败: {e}"))?,
            trade_date: row.get(1).map_err(|e| format!("读取规则日期失败: {e}"))?,
            rule_name: row.get(2).map_err(|e| format!("读取规则名称失败: {e}"))?,
            rule_score,
        };
        if rule_score.is_finite() {
            details.push(item);
        }
    }

    Ok(details)
}

fn build_rule_contribution_averages_from_rows(
    summary_rows: &[ScoreSummary],
    detail_rows: &[ScoreDetails],
    start_date: &str,
    end_date: &str,
) -> HashMap<String, RuleContributionAverages> {
    let mut daily_max_rank: HashMap<String, i64> = HashMap::new();
    let mut rank_by_sample: HashMap<(String, String), i64> = HashMap::new();

    for row in summary_rows {
        if row.trade_date.as_str() < start_date || row.trade_date.as_str() > end_date {
            continue;
        }
        let Some(rank) = row.rank.filter(|value| *value > 0) else {
            continue;
        };

        daily_max_rank
            .entry(row.trade_date.clone())
            .and_modify(|max_rank| *max_rank = (*max_rank).max(rank))
            .or_insert(rank);
        rank_by_sample.insert((row.ts_code.clone(), row.trade_date.clone()), rank);
    }

    let mut daily_agg_map: HashMap<(String, String), RuleDayAgg> = HashMap::new();
    for row in detail_rows {
        if row.trade_date.as_str() < start_date || row.trade_date.as_str() > end_date {
            continue;
        }
        if !row.rule_score.is_finite() || row.rule_score.abs() <= RULE_BACKTEST_EPS {
            continue;
        }

        let agg = daily_agg_map
            .entry((row.trade_date.clone(), row.rule_name.clone()))
            .or_default();
        agg.trigger_count += 1;

        let Some(rank) = rank_by_sample.get(&(row.ts_code.clone(), row.trade_date.clone())) else {
            continue;
        };
        let Some(max_rank) = daily_max_rank.get(&row.trade_date) else {
            continue;
        };
        if *max_rank <= 0 {
            continue;
        }

        agg.contribution_score +=
            row.rule_score * (*max_rank + 1 - *rank) as f64 / *max_rank as f64;
    }

    let mut acc_map: HashMap<String, RuleContributionAccumulator> = HashMap::new();
    for ((_trade_date, rule_name), agg) in daily_agg_map {
        if agg.trigger_count <= 0 {
            continue;
        }
        let acc = acc_map.entry(rule_name).or_default();
        acc.contribution_sum += agg.contribution_score;
        acc.contribution_days += 1;
        acc.trigger_count += agg.trigger_count.max(0);
    }

    finalize_rule_contribution_averages(acc_map)
}

fn weighted_rule_summary_metric(
    summaries: &[RuleLayerRuleSummary],
    value: impl Fn(&RuleLayerRuleSummary) -> Option<f64>,
) -> Option<f64> {
    let mut weighted_sum = 0.0;
    let mut total_weight = 0usize;

    for summary in summaries {
        if summary.point_count == 0 {
            continue;
        }
        let Some(metric_value) = value(summary) else {
            continue;
        };
        if !metric_value.is_finite() {
            continue;
        }

        weighted_sum += metric_value * summary.point_count as f64;
        total_weight += summary.point_count;
    }

    if total_weight == 0 {
        None
    } else {
        Some(weighted_sum / total_weight as f64)
    }
}

fn aggregate_rule_er_change(summaries: &[RuleLayerRuleSummary]) -> Option<f64> {
    let mut weighted_sum = 0.0;
    let mut total_weight = 0usize;

    for summary in summaries {
        let Some(avg_er_change) = summary.avg_er_change.filter(|value| value.is_finite()) else {
            continue;
        };
        if summary.er_change_sample_count == 0 {
            continue;
        }
        weighted_sum += avg_er_change * summary.er_change_sample_count as f64;
        total_weight += summary.er_change_sample_count;
    }

    if total_weight == 0 {
        None
    } else {
        Some(weighted_sum / total_weight as f64)
    }
}

fn aggregate_all_rule_summary_metrics(
    summaries: &[RuleLayerRuleSummary],
) -> (
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
) {
    let avg_residual_mean = weighted_rule_summary_metric(summaries, |item| item.avg_residual_mean);
    let avg_excess_residual_mean =
        weighted_rule_summary_metric(summaries, |item| item.avg_excess_residual_mean);
    let avg_er_change = aggregate_rule_er_change(summaries);
    let profit_loss_ratio = weighted_rule_summary_metric(summaries, |item| item.profit_loss_ratio);
    let spread_mean = weighted_rule_summary_metric(summaries, |item| item.spread_mean);
    let ic_mean = weighted_rule_summary_metric(summaries, |item| item.ic_mean);
    let ic_std = weighted_rule_summary_metric(summaries, |item| item.ic_std);
    let icir = match (ic_mean, ic_std) {
        (Some(mean), Some(std)) if std.abs() >= RULE_BACKTEST_EPS => Some(mean / std),
        _ => weighted_rule_summary_metric(summaries, |item| item.icir),
    };
    let total_points = summaries.iter().map(|item| item.point_count).sum::<usize>();
    let ic_t_value = match (ic_mean, ic_std) {
        (Some(mean), Some(std)) if total_points > 1 && std.abs() >= RULE_BACKTEST_EPS => {
            Some(mean * (total_points as f64).sqrt() / std)
        }
        _ => weighted_rule_summary_metric(summaries, |item| item.ic_t_value),
    };

    (
        avg_residual_mean,
        avg_excess_residual_mean,
        avg_er_change,
        profit_loss_ratio,
        spread_mean,
        ic_mean,
        ic_std,
        icir,
        ic_t_value,
    )
}

fn run_scene_layer_backtest_core(
    source_conn: &Connection,
    source_path: &str,
    scene_name: Option<&str>,
    params: &SceneLayerBacktestRunParams,
) -> Result<SceneLayerBacktestData, String> {
    let layer_config = SceneLayerConfig {
        min_samples_per_day: params.min_samples_per_day,
        backtest_period: params.backtest_period,
        min_listed_trade_days: params.min_listed_trade_days,
    };

    if let Some(scene_name) = scene_name {
        let scene_name = scene_name.trim();
        if scene_name.is_empty() {
            return Err("scene_name不能为空".to_string());
        }

        let input = SceneLayerFromDbInput {
            scene_name: scene_name.to_string(),
            stock_adj_type: params.stock_adj_type.clone(),
            index_ts_code: params.index_ts_code.clone(),
            index_beta: params.index_beta,
            concept_beta: params.concept_beta,
            industry_beta: params.industry_beta,
            start_date: params.start_date.clone(),
            end_date: params.end_date.clone(),
            layer_config,
        };

        let metrics = calc_scene_layer_metrics_from_db_with_ts_filter(
            source_conn,
            source_path,
            &input,
            params.allowed_ts_codes.as_ref(),
        )?;

        return Ok(SceneLayerBacktestData {
            scene_name: input.scene_name,
            stock_adj_type: input.stock_adj_type,
            index_ts_code: input.index_ts_code,
            index_beta: input.index_beta,
            concept_beta: input.concept_beta,
            industry_beta: input.industry_beta,
            start_date: input.start_date,
            end_date: input.end_date,
            resolved_board: params.resolved_board.clone(),
            exclude_st_board: params.exclude_st_board,
            total_mv_min: params.total_mv_min,
            total_mv_max: params.total_mv_max,
            min_samples_per_scene_day: input.layer_config.min_samples_per_day,
            min_listed_trade_days: input.layer_config.min_listed_trade_days,
            backtest_period: input.layer_config.backtest_period,
            points: metrics
                .points
                .into_iter()
                .map(|point| SceneLayerPointPayload {
                    trade_date: point.trade_date,
                    state_avg_residual_returns: point
                        .state_avg_residual_returns
                        .into_iter()
                        .map(|(scene_state, avg_residual_return)| {
                            SceneLayerStateAvgResidualReturn {
                                scene_state,
                                avg_residual_return: Some(avg_residual_return),
                            }
                        })
                        .collect(),
                    top_bottom_spread: point.top_bottom_spread,
                    ic: point.ic,
                })
                .collect(),
            spread_mean: metrics.spread_mean,
            ic_mean: metrics.ic_mean,
            ic_std: metrics.ic_std,
            icir: metrics.icir,
            ic_t_value: metrics.ic_t_value,
            is_all_scenes: false,
            all_scene_summaries: Vec::new(),
        });
    }

    let scene_options = load_scene_options(source_path)?;
    let all_metrics = calc_all_scene_layer_metrics_from_db_with_ts_filter(
        source_conn,
        source_path,
        &scene_options,
        &params.stock_adj_type,
        &params.index_ts_code,
        params.index_beta,
        params.concept_beta,
        params.industry_beta,
        &params.start_date,
        &params.end_date,
        &layer_config,
        params.allowed_ts_codes.as_ref(),
    )?;
    let mut all_scene_summaries = Vec::with_capacity(all_metrics.len());

    for (one_scene_name, metrics) in all_metrics {
        all_scene_summaries.push(SceneLayerSceneSummary {
            scene_name: one_scene_name,
            point_count: metrics.points.len(),
            spread_mean: metrics.spread_mean,
            ic_mean: metrics.ic_mean,
            ic_std: metrics.ic_std,
            icir: metrics.icir,
            ic_t_value: metrics.ic_t_value,
        });
    }

    all_scene_summaries.sort_by(|a, b| {
        b.spread_mean
            .unwrap_or(f64::NEG_INFINITY)
            .partial_cmp(&a.spread_mean.unwrap_or(f64::NEG_INFINITY))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.point_count.cmp(&a.point_count))
            .then_with(|| a.scene_name.cmp(&b.scene_name))
    });

    Ok(SceneLayerBacktestData {
        scene_name: String::new(),
        stock_adj_type: params.stock_adj_type.clone(),
        index_ts_code: params.index_ts_code.clone(),
        index_beta: params.index_beta,
        concept_beta: params.concept_beta,
        industry_beta: params.industry_beta,
        start_date: params.start_date.clone(),
        end_date: params.end_date.clone(),
        resolved_board: params.resolved_board.clone(),
        exclude_st_board: params.exclude_st_board,
        total_mv_min: params.total_mv_min,
        total_mv_max: params.total_mv_max,
        min_samples_per_scene_day: params.min_samples_per_day,
        min_listed_trade_days: params.min_listed_trade_days,
        backtest_period: params.backtest_period,
        points: Vec::new(),
        spread_mean: None,
        ic_mean: None,
        ic_std: None,
        icir: None,
        ic_t_value: None,
        is_all_scenes: true,
        all_scene_summaries,
    })
}

fn run_rule_layer_backtest_core(
    source_conn: &Connection,
    source_path: &str,
    rule_name: Option<&str>,
    params: &RuleLayerBacktestRunParams,
) -> Result<RuleLayerBacktestData, String> {
    let layer_config = RuleLayerConfig {
        min_samples_per_day: params.min_samples_per_day,
        backtest_period: params.backtest_period,
        min_listed_trade_days: params.min_listed_trade_days,
    };

    if let Some(rule_name) = rule_name {
        let rule_name = rule_name.trim();
        if rule_name.is_empty() {
            return Err("rule_name不能为空".to_string());
        }

        let input = RuleLayerFromDbInput {
            rule_name: rule_name.to_string(),
            stock_adj_type: params.stock_adj_type.clone(),
            index_ts_code: params.index_ts_code.clone(),
            index_beta: params.index_beta,
            concept_beta: params.concept_beta,
            industry_beta: params.industry_beta,
            start_date: params.start_date.clone(),
            end_date: params.end_date.clone(),
            layer_config,
        };

        let metrics = calc_rule_layer_metrics_from_db_with_ts_filter(
            source_conn,
            source_path,
            &input,
            params.allowed_ts_codes.as_ref(),
        )?;
        let decay_validations = build_rule_decay_validations(&metrics.points);

        return Ok(RuleLayerBacktestData {
            rule_name: input.rule_name,
            stock_adj_type: input.stock_adj_type,
            index_ts_code: input.index_ts_code,
            index_beta: input.index_beta,
            concept_beta: input.concept_beta,
            industry_beta: input.industry_beta,
            start_date: input.start_date,
            end_date: input.end_date,
            resolved_board: params.resolved_board.clone(),
            exclude_st_board: params.exclude_st_board,
            total_mv_min: params.total_mv_min,
            total_mv_max: params.total_mv_max,
            min_samples_per_rule_day: input.layer_config.min_samples_per_day,
            min_listed_trade_days: input.layer_config.min_listed_trade_days,
            backtest_period: input.layer_config.backtest_period,
            points: metrics
                .points
                .into_iter()
                .map(|point| RuleLayerPointPayload {
                    trade_date: point.trade_date,
                    sample_count: point.sample_count,
                    avg_rule_score: point.avg_rule_score,
                    avg_residual_return: point.avg_residual_return,
                    avg_excess_residual_return: point.avg_excess_residual_return,
                    top_bottom_spread: None,
                    ic: point.ic,
                })
                .collect(),
            avg_residual_mean: metrics.avg_residual_mean,
            avg_excess_residual_mean: metrics.avg_excess_residual_mean,
            decay_validations,
            avg_er_change: metrics.avg_er_change,
            profit_loss_ratio: metrics.profit_loss_ratio,
            spread_mean: None,
            avg_contribution_score: None,
            avg_contribution_per_trigger: None,
            ic_mean: metrics.ic_mean,
            ic_std: metrics.ic_std,
            icir: metrics.icir,
            ic_t_value: metrics.ic_t_value,
            layer_count: None,
            layer_method: None,
            layer_method_label: None,
            layer_summaries: Vec::new(),
            is_all_rules: false,
            all_rule_summaries: Vec::new(),
            rule_validation_details: Vec::new(),
        });
    }

    let (rule_options, rule_meta_map) = load_rule_meta(source_path)?;
    let explain_map = rule_meta_map
        .iter()
        .map(|(rule_name, meta)| (rule_name.clone(), meta.explain.clone()))
        .collect::<HashMap<_, _>>();
    let has_rule_meta_match = rule_options
        .iter()
        .any(|rule_name| rule_meta_map.contains_key(rule_name));
    let stock_meta_map = if has_rule_meta_match {
        load_validation_sample_stock_meta_map(source_path)?
    } else {
        HashMap::new()
    };
    let similarity_cache = if has_rule_meta_match {
        load_validation_similarity_cache_optional(
            source_path,
            &params.start_date,
            &params.end_date,
        )?
    } else {
        empty_validation_similarity_cache()
    };
    let (joint_summary_rows, joint_detail_rows) = load_rule_backtest_score_rows_from_db(
        source_path,
        &params.start_date,
        &params.end_date,
        params.allowed_ts_codes.as_ref(),
    )?;
    let contribution_averages = build_rule_contribution_averages_from_rows(
        &joint_summary_rows,
        &joint_detail_rows,
        &params.start_date,
        &params.end_date,
    );
    let summary_detail_items = calc_all_rule_layer_metrics_with_samples_from_rows_map(
        source_conn,
        source_path,
        &rule_options,
        &joint_summary_rows,
        &joint_detail_rows,
        &params.stock_adj_type,
        &params.index_ts_code,
        params.index_beta,
        params.concept_beta,
        params.industry_beta,
        &params.start_date,
        &params.end_date,
        &layer_config,
        params.parallel_batch_size,
        |one_rule_name, metrics_with_samples| {
            Ok(build_one_rule_backtest_summary_and_detail(
                one_rule_name,
                metrics_with_samples,
                &rule_meta_map,
                &contribution_averages,
                &explain_map,
                params,
                &layer_config,
                &similarity_cache,
                &stock_meta_map,
            ))
        },
    );
    let (all_rule_summaries, rule_validation_details) =
        split_and_sort_rule_backtest_summaries_and_details(summary_detail_items?);
    let decay_validations = build_all_rule_decay_validations(&all_rule_summaries);

    let (
        avg_residual_mean,
        avg_excess_residual_mean,
        avg_er_change,
        profit_loss_ratio,
        _spread_mean,
        ic_mean,
        ic_std,
        icir,
        ic_t_value,
    ) = aggregate_all_rule_summary_metrics(&all_rule_summaries);
    Ok(RuleLayerBacktestData {
        rule_name: String::new(),
        stock_adj_type: params.stock_adj_type.clone(),
        index_ts_code: params.index_ts_code.clone(),
        index_beta: params.index_beta,
        concept_beta: params.concept_beta,
        industry_beta: params.industry_beta,
        start_date: params.start_date.clone(),
        end_date: params.end_date.clone(),
        resolved_board: params.resolved_board.clone(),
        exclude_st_board: params.exclude_st_board,
        total_mv_min: params.total_mv_min,
        total_mv_max: params.total_mv_max,
        min_samples_per_rule_day: params.min_samples_per_day,
        min_listed_trade_days: params.min_listed_trade_days,
        backtest_period: params.backtest_period,
        points: Vec::new(),
        avg_residual_mean,
        avg_excess_residual_mean,
        decay_validations,
        avg_er_change,
        profit_loss_ratio,
        spread_mean: None,
        avg_contribution_score: weighted_rule_summary_metric(&all_rule_summaries, |item| {
            item.avg_contribution_score
        }),
        avg_contribution_per_trigger: weighted_rule_summary_metric(&all_rule_summaries, |item| {
            item.avg_contribution_per_trigger
        }),
        ic_mean,
        ic_std,
        icir,
        ic_t_value,
        layer_count: None,
        layer_method: None,
        layer_method_label: None,
        layer_summaries: Vec::new(),
        is_all_rules: true,
        all_rule_summaries,
        rule_validation_details,
    })
}

fn rule_decay_t_value(recent: &[f64], prior: &[f64], change: f64) -> Option<f64> {
    let recent_std = sample_std_f64(recent)?;
    let prior_std = sample_std_f64(prior)?;
    let standard_error = ((recent_std * recent_std / recent.len() as f64)
        + (prior_std * prior_std / prior.len() as f64))
        .sqrt();
    if !standard_error.is_finite() || standard_error <= RULE_BACKTEST_EPS {
        None
    } else {
        Some(change / standard_error)
    }
}

fn rule_decay_status(
    recent_mean: f64,
    change: f64,
    t_value: Option<f64>,
) -> (&'static str, &'static str) {
    if change < 0.0 && t_value.is_some_and(|value| value <= -2.0) {
        ("significant_decay", "显著衰减")
    } else if change < 0.0 && recent_mean < 0.0 {
        ("decay", "衰减")
    } else if change < 0.0 {
        ("weakening", "走弱")
    } else if recent_mean < 0.0 {
        ("weak", "近期偏弱")
    } else if change > 0.0 {
        ("improving", "改善")
    } else {
        ("stable", "稳定")
    }
}

fn build_decay_validations_from_daily_values(
    mut daily_values: Vec<(String, f64)>,
) -> Vec<RuleDecayValidation> {
    daily_values.retain(|(_, value)| value.is_finite());
    daily_values.sort_by(|left, right| left.0.cmp(&right.0));

    RULE_DECAY_WINDOWS
        .into_iter()
        .map(|window_days| {
            let recent_day_count = daily_values.len().min(window_days);
            let recent_start_index = daily_values.len().saturating_sub(recent_day_count);
            let prior_day_count = recent_start_index;
            let recent_start_date = daily_values
                .get(recent_start_index)
                .map(|(trade_date, _)| trade_date.clone());
            let recent_end_date = daily_values
                .last()
                .map(|(trade_date, _)| trade_date.clone());

            if recent_day_count < window_days || prior_day_count < RULE_DECAY_MIN_PRIOR_DAYS {
                return RuleDecayValidation {
                    window_days,
                    recent_start_date,
                    recent_end_date,
                    recent_day_count,
                    prior_day_count,
                    recent_directional_excess_mean: None,
                    prior_directional_excess_mean: None,
                    decay_change: None,
                    decay_t_value: None,
                    status: "insufficient".to_string(),
                    status_label: "样本不足".to_string(),
                };
            }

            let prior = daily_values[..recent_start_index]
                .iter()
                .map(|(_, value)| *value)
                .collect::<Vec<_>>();
            let recent = daily_values[recent_start_index..]
                .iter()
                .map(|(_, value)| *value)
                .collect::<Vec<_>>();
            let recent_mean = mean_f64(&recent).unwrap_or_default();
            let prior_mean = mean_f64(&prior).unwrap_or_default();
            let change = recent_mean - prior_mean;
            let t_value = rule_decay_t_value(&recent, &prior, change);
            let (status, status_label) = rule_decay_status(recent_mean, change, t_value);

            RuleDecayValidation {
                window_days,
                recent_start_date,
                recent_end_date,
                recent_day_count,
                prior_day_count,
                recent_directional_excess_mean: Some(recent_mean),
                prior_directional_excess_mean: Some(prior_mean),
                decay_change: Some(change),
                decay_t_value: t_value,
                status: status.to_string(),
                status_label: status_label.to_string(),
            }
        })
        .collect()
}

fn build_rule_directional_excess_daily_values(
    points: &[crate::simulate::rule::RuleLayerPoint],
) -> Vec<(String, f64)> {
    let direction_score_sum = points
        .iter()
        .filter_map(|point| point.avg_rule_score.filter(|value| value.is_finite()))
        .sum::<f64>();
    let direction_sign = if direction_score_sum < 0.0 { -1.0 } else { 1.0 };
    points
        .iter()
        .filter_map(|point| {
            point
                .avg_excess_residual_return
                .filter(|value| value.is_finite())
                .map(|value| (point.trade_date.clone(), value * direction_sign))
        })
        .collect()
}

fn build_rule_decay_validations(
    points: &[crate::simulate::rule::RuleLayerPoint],
) -> Vec<RuleDecayValidation> {
    build_decay_validations_from_daily_values(build_rule_directional_excess_daily_values(points))
}

fn build_rule_basket_decay_from_daily_groups<'a>(
    daily_groups: impl IntoIterator<Item = &'a [(String, f64)]>,
) -> Vec<RuleDecayValidation> {
    let mut daily_aggregates = HashMap::<String, (f64, usize)>::new();
    for (trade_date, value) in daily_groups.into_iter().flatten() {
        if !value.is_finite() {
            continue;
        }
        let aggregate = daily_aggregates.entry(trade_date.clone()).or_default();
        aggregate.0 += *value;
        aggregate.1 += 1;
    }
    build_decay_validations_from_daily_values(
        daily_aggregates
            .into_iter()
            .filter_map(|(trade_date, (sum, count))| {
                (count > 0).then_some((trade_date, sum / count as f64))
            })
            .collect(),
    )
}

fn build_all_rule_decay_validations(
    summaries: &[RuleLayerRuleSummary],
) -> Vec<RuleDecayValidation> {
    build_rule_basket_decay_from_daily_groups(
        summaries
            .iter()
            .map(|summary| summary.decay_daily_values.as_slice()),
    )
}

fn build_one_rule_backtest_summary_and_detail(
    one_rule_name: &str,
    metrics_with_samples: RuleLayerMetricsWithSamples,
    rule_meta_map: &HashMap<String, RuleMeta>,
    contribution_averages: &HashMap<String, RuleContributionAverages>,
    explain_map: &HashMap<String, String>,
    params: &RuleLayerBacktestRunParams,
    layer_config: &RuleLayerConfig,
    similarity_cache: &ValidationSimilarityCache,
    stock_meta_map: &HashMap<String, ValidationSampleStockMeta>,
) -> (RuleLayerRuleSummary, Option<RuleValidationComboResult>) {
    let RuleLayerMetricsWithSamples { metrics, samples } = metrics_with_samples;
    let contribution_average = contribution_averages
        .get(one_rule_name)
        .cloned()
        .unwrap_or_default();
    let decay_daily_values = build_rule_directional_excess_daily_values(&metrics.points);
    let decay_validations = build_decay_validations_from_daily_values(decay_daily_values.clone());
    let summary = RuleLayerRuleSummary {
        rule_name: one_rule_name.to_string(),
        point_count: metrics.points.len(),
        avg_residual_mean: metrics.avg_residual_mean,
        avg_excess_residual_mean: metrics.avg_excess_residual_mean,
        avg_er_change: metrics.avg_er_change,
        er_change_sample_count: metrics.er_change_sample_count,
        profit_loss_ratio: metrics.profit_loss_ratio,
        spread_mean: None,
        avg_contribution_score: contribution_average.avg_contribution_score,
        avg_contribution_per_trigger: contribution_average.avg_contribution_per_trigger,
        ic_mean: metrics.ic_mean,
        ic_std: metrics.ic_std,
        icir: metrics.icir,
        ic_t_value: metrics.ic_t_value,
        decay_validations,
        decay_daily_values,
    };
    let detail = rule_meta_map.get(one_rule_name).map(|rule_meta| {
        build_strategy_rule_validation_detail(
            params,
            one_rule_name,
            rule_meta,
            RuleLayerMetricsWithSamples { metrics, samples },
            layer_config,
            similarity_cache,
            explain_map,
            stock_meta_map,
        )
    });

    (summary, detail)
}

fn split_and_sort_rule_backtest_summaries_and_details(
    items: Vec<(RuleLayerRuleSummary, Option<RuleValidationComboResult>)>,
) -> (Vec<RuleLayerRuleSummary>, Vec<RuleValidationComboResult>) {
    let mut all_rule_summaries = Vec::with_capacity(items.len());
    let mut rule_validation_details = Vec::new();

    for (summary, detail) in items {
        all_rule_summaries.push(summary);
        if let Some(detail) = detail {
            rule_validation_details.push(detail);
        }
    }

    all_rule_summaries.sort_by(|a, b| {
        b.profit_loss_ratio
            .unwrap_or(f64::NEG_INFINITY)
            .partial_cmp(&a.profit_loss_ratio.unwrap_or(f64::NEG_INFINITY))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.point_count.cmp(&a.point_count))
            .then_with(|| a.rule_name.cmp(&b.rule_name))
    });

    (all_rule_summaries, rule_validation_details)
}

fn rank_layer_label(layer_index: usize, layer_count: usize) -> String {
    if layer_index == 1 {
        "第1层（低分）".to_string()
    } else if layer_index == layer_count {
        format!("第{layer_index}层（高分）")
    } else {
        format!("第{layer_index}层")
    }
}

fn rank_layer_method_label(layer_method: RankLayerMethod) -> &'static str {
    match layer_method {
        RankLayerMethod::Score => "按分数分层",
        RankLayerMethod::SampleCount => "按样本数分层（同分按数据库排名）",
        RankLayerMethod::Rank => "按数据库排名分层",
    }
}

fn rank_market_value_groups() -> [(&'static str, Option<f64>, Option<f64>); 3] {
    [
        ("小市值(<50亿)", None, Some(50.0)),
        ("中市值(50-200亿)", Some(50.0), Some(200.0)),
        ("大市值(>=200亿)", Some(200.0), None),
    ]
}

fn stock_total_mv(total_mv_map: &HashMap<String, f64>, ts_code: &str) -> Option<f64> {
    let ts_code = ts_code.trim();
    total_mv_map.get(ts_code).copied().or_else(|| {
        total_mv_map
            .get(ts_code.to_ascii_uppercase().as_str())
            .copied()
    })
}

fn rank_row_in_market_value_group(
    total_mv_map: &HashMap<String, f64>,
    row: &ScoreSummary,
    min_value: Option<f64>,
    max_value: Option<f64>,
) -> bool {
    let Some(total_mv) = stock_total_mv(total_mv_map, &row.ts_code) else {
        return false;
    };
    if let Some(min_value) = min_value {
        if total_mv < min_value {
            return false;
        }
    }
    if let Some(max_value) = max_value {
        if total_mv >= max_value {
            return false;
        }
    }
    true
}

fn build_rank_market_value_summaries(
    source_path: &str,
    input: &RankLayerFromDbInput,
    summary_rows: &[ScoreSummary],
    samples: &[crate::simulate::rank::RankLayerSamplePoint],
) -> Result<Vec<RankLayerMarketValueSummary>, String> {
    let total_mv_map = build_total_mv_map(source_path)?;
    let mut out = Vec::new();

    for (group_label, total_mv_min, total_mv_max) in rank_market_value_groups() {
        let group_rows = summary_rows
            .iter()
            .filter(|row| {
                rank_row_in_market_value_group(&total_mv_map, row, total_mv_min, total_mv_max)
            })
            .cloned()
            .collect::<Vec<_>>();
        let group_samples = samples
            .iter()
            .filter(|sample| {
                stock_total_mv(&total_mv_map, &sample.ts_code).is_some_and(|total_mv| {
                    total_mv_min.is_none_or(|min_value| total_mv >= min_value)
                        && total_mv_max.is_none_or(|max_value| total_mv < max_value)
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        let metrics = calc_rank_layer_metrics_from_rank_samples(
            &group_samples,
            &input.layer_config,
            &group_rows,
        )?;
        out.push(RankLayerMarketValueSummary {
            group_label: group_label.to_string(),
            total_mv_min,
            total_mv_max,
            point_count: metrics.point_count,
            sample_count: metrics.sample_count,
            avg_er_change: metrics.avg_er_change,
            spread_mean: metrics.spread_mean,
            ic_mean: metrics.ic_mean,
            ic_t_value: metrics.ic_t_value,
            icir: metrics.icir,
        });
    }

    Ok(out)
}

fn run_rank_layer_backtest_core(
    source_conn: &Connection,
    source_path: &str,
    params: &RankLayerBacktestRunParams,
) -> Result<RankLayerBacktestData, String> {
    let layer_config = RankLayerConfig {
        min_samples_per_day: params.min_samples_per_day,
        backtest_period: params.backtest_period,
        min_listed_trade_days: params.min_listed_trade_days,
        layer_count: params.layer_count,
        layer_method: params.layer_method,
    };
    let input = RankLayerFromDbInput {
        stock_adj_type: params.stock_adj_type.clone(),
        index_ts_code: params.index_ts_code.clone(),
        index_beta: params.index_beta,
        concept_beta: params.concept_beta,
        industry_beta: params.industry_beta,
        start_date: params.start_date.clone(),
        end_date: params.end_date.clone(),
        layer_config,
    };
    let summary_rows = load_score_summary_rows_from_db(
        source_path,
        &params.start_date,
        &params.end_date,
        params.allowed_ts_codes.as_ref(),
    )?;
    let metrics =
        calc_rank_layer_metrics_from_score_rows(source_conn, source_path, &input, &summary_rows)?;
    let market_value_summaries = build_rank_market_value_summaries(
        source_path,
        &input,
        &summary_rows,
        &metrics.layer_samples,
    )?;
    let stock_meta_map = load_validation_sample_stock_meta_map(source_path)?;
    let layer_sample_groups = build_rank_layer_sample_groups(
        &metrics.layer_samples,
        input.layer_config.layer_count,
        &stock_meta_map,
    );
    let joint_params = rule_joint_validation_params_from_rank(params);
    let joint_validation_continuation_id =
        store_rule_joint_validation_from_rows(source_path, &joint_params, summary_rows);

    Ok(RankLayerBacktestData {
        stock_adj_type: input.stock_adj_type,
        index_ts_code: input.index_ts_code,
        index_beta: input.index_beta,
        concept_beta: input.concept_beta,
        industry_beta: input.industry_beta,
        start_date: input.start_date,
        end_date: input.end_date,
        resolved_board: params.resolved_board.clone(),
        exclude_st_board: params.exclude_st_board,
        market_value_grouping: true,
        min_samples_per_rank_day: input.layer_config.effective_min_samples_per_day(),
        min_listed_trade_days: input.layer_config.min_listed_trade_days,
        backtest_period: input.layer_config.backtest_period,
        layer_count: input.layer_config.layer_count,
        layer_method: input.layer_config.layer_method.as_str().to_string(),
        layer_method_label: rank_layer_method_label(input.layer_config.layer_method).to_string(),
        point_count: metrics.point_count,
        sample_count: metrics.sample_count,
        avg_er_change: metrics.avg_er_change,
        spread_mean: metrics.spread_mean,
        ic_mean: metrics.ic_mean,
        ic_std: metrics.ic_std,
        icir: metrics.icir,
        ic_t_value: metrics.ic_t_value,
        layer_summaries: metrics
            .layers
            .into_iter()
            .map(|item| RankLayerBucketSummary {
                layer_index: item.layer_index,
                layer_label: rank_layer_label(item.layer_index, input.layer_config.layer_count),
                point_count: item.point_count,
                sample_count: item.sample_count,
                avg_score: item.avg_score,
                avg_residual_return: item.avg_residual_return,
                avg_er_change: item.avg_er_change,
            })
            .collect(),
        layer_sample_groups,
        market_value_summaries,
        joint_validation_continuation_id,
    })
}

#[derive(Default)]
struct RankLayerSampleGroupAccumulator {
    total_samples: usize,
    positive_count: usize,
    negative_count: usize,
    trade_dates: HashSet<String>,
    positive_by_board: HashMap<String, Vec<ValidationSampleRawRow>>,
    negative_by_board: HashMap<String, Vec<ValidationSampleRawRow>>,
    random_by_board: HashMap<String, Vec<(u64, ValidationSampleRawRow)>>,
}

fn build_rank_layer_sample_groups(
    samples: &[crate::simulate::rank::RankLayerSamplePoint],
    layer_count: usize,
    stock_meta_map: &HashMap<String, ValidationSampleStockMeta>,
) -> Vec<RankLayerSampleGroup> {
    let mut groups = (0..layer_count)
        .map(|_| RankLayerSampleGroupAccumulator::default())
        .collect::<Vec<_>>();

    for sample in samples {
        if sample.layer_index == 0 || sample.layer_index > layer_count {
            continue;
        }
        let group = &mut groups[sample.layer_index - 1];
        let row = ValidationSampleRawRow {
            ts_code: sample.ts_code.clone(),
            trade_date: sample.trade_date.clone(),
            trigger_count: 1,
            rule_score: sample.score,
            residual_return: sample.residual_return,
        };

        group.total_samples += 1;
        group.trade_dates.insert(row.trade_date.clone());
        let board = sample_board(&row.ts_code, stock_meta_map);
        if row.residual_return > 0.0 {
            group.positive_count += 1;
            push_limited_sample(
                group.positive_by_board.entry(board.clone()).or_default(),
                row.clone(),
                RANK_BACKTEST_LAYER_SAMPLE_LIMIT_PER_GROUP,
                compare_positive_validation_sample,
            );
        } else if row.residual_return < 0.0 {
            group.negative_count += 1;
            push_limited_sample(
                group.negative_by_board.entry(board.clone()).or_default(),
                row.clone(),
                RANK_BACKTEST_LAYER_SAMPLE_LIMIT_PER_GROUP,
                compare_negative_validation_sample,
            );
        }
        push_limited_random_sample(
            group.random_by_board.entry(board).or_default(),
            random::<u64>(),
            row,
            RANK_BACKTEST_LAYER_SAMPLE_LIMIT_PER_GROUP,
        );
    }

    groups
        .into_iter()
        .enumerate()
        .map(|(index, group)| {
            let triggered_days = group.trade_dates.len();
            let mut positive = group
                .positive_by_board
                .into_values()
                .flatten()
                .collect::<Vec<_>>();
            let mut negative = group
                .negative_by_board
                .into_values()
                .flatten()
                .collect::<Vec<_>>();
            let mut random = group
                .random_by_board
                .into_values()
                .flatten()
                .collect::<Vec<_>>();
            positive.sort_by(compare_positive_validation_sample);
            negative.sort_by(compare_negative_validation_sample);
            random.sort_by(|left, right| {
                left.0
                    .cmp(&right.0)
                    .then_with(|| compare_random_validation_sample(&left.1, &right.1))
            });

            RankLayerSampleGroup {
                layer_index: index + 1,
                layer_label: rank_layer_label(index + 1, layer_count),
                total_samples: group.total_samples,
                triggered_days,
                positive_count: group.positive_count,
                negative_count: group.negative_count,
                random_count: group.total_samples,
                positive: validation_sample_rows_to_payload(positive, stock_meta_map),
                negative: validation_sample_rows_to_payload(negative, stock_meta_map),
                random: validation_sample_rows_to_payload(
                    random.into_iter().map(|(_, row)| row),
                    stock_meta_map,
                ),
            }
        })
        .collect()
}

fn validate_backtest_strategy_expressions(source_path: &str) -> Result<(), String> {
    let rules_cache = build_scoring_rule_cache(source_path, None)?;
    let programs = rules_cache
        .iter()
        .flat_map(CachedRule::expression_programs)
        .collect::<Vec<_>>();
    let cyq_chen_keys = cyq_chen_runtime_key_names();
    let injected_keys = BACKTEST_INJECTED_RUNTIME_KEYS
        .iter()
        .copied()
        .chain(cyq_chen_keys)
        .collect::<Vec<_>>();
    let required_runtime_keys = collect_runtime_keys_from_expr_programs(
        &programs,
        RuntimeKeyCollectOptions {
            always_keys: &[],
            injected_keys: &injected_keys,
            aliases: &BACKTEST_RUNTIME_ALIASES,
        },
    );

    DataReader::new_with_runtime_keys(source_path, &required_runtime_keys)
        .map(|_| ())
        .map_err(|error| format!("策略表达式预检失败: {error}"))
}

pub fn run_scene_layer_backtest(
    source_path: String,
    stock_adj_type: Option<String>,
    index_ts_code: String,
    index_beta: Option<f64>,
    concept_beta: Option<f64>,
    industry_beta: Option<f64>,
    start_date: String,
    end_date: String,
    min_samples_per_scene_day: Option<usize>,
    min_listed_trade_days: Option<usize>,
    backtest_period: Option<usize>,
    board: Option<String>,
    exclude_st_board: Option<bool>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
) -> Result<SceneLayerBacktestData, String> {
    validate_backtest_strategy_expressions(&source_path)?;
    let source_db = source_db_path(&source_path);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "原始库路径不是有效UTF-8".to_string())?;
    let source_conn =
        Connection::open(source_db_str).map_err(|e| format!("打开原始库失败: {e}"))?;
    let (resolved_board, exclude_st_board, total_mv_min, total_mv_max, allowed_ts_codes) =
        build_backtest_stock_filter(
            &source_path,
            board,
            exclude_st_board,
            total_mv_min,
            total_mv_max,
        )?;

    let params = SceneLayerBacktestRunParams {
        stock_adj_type: stock_adj_type
            .unwrap_or_else(|| "qfq".to_string())
            .trim()
            .to_string(),
        index_ts_code: index_ts_code.trim().to_string(),
        index_beta: index_beta.unwrap_or(0.5),
        concept_beta: concept_beta.unwrap_or(0.2),
        industry_beta: industry_beta.unwrap_or(0.0),
        start_date: start_date.trim().to_string(),
        end_date: end_date.trim().to_string(),
        min_samples_per_day: min_samples_per_scene_day.unwrap_or(5),
        min_listed_trade_days: min_listed_trade_days
            .unwrap_or(DEFAULT_BACKTEST_MIN_LISTED_TRADE_DAYS),
        backtest_period: backtest_period.unwrap_or(1),
        resolved_board,
        exclude_st_board,
        total_mv_min,
        total_mv_max,
        allowed_ts_codes,
    };

    // 当前入口固定全量；后续如需恢复单场景，仅需传入 Some(scene_name)。
    run_scene_layer_backtest_core(&source_conn, &source_path, None, &params)
}

pub fn run_rule_layer_backtest(
    source_path: String,
    stock_adj_type: Option<String>,
    index_ts_code: String,
    index_beta: Option<f64>,
    concept_beta: Option<f64>,
    industry_beta: Option<f64>,
    start_date: String,
    end_date: String,
    min_samples_per_rule_day: Option<usize>,
    min_listed_trade_days: Option<usize>,
    backtest_period: Option<usize>,
    parallel_batch_size: Option<usize>,
    board: Option<String>,
    exclude_st_board: Option<bool>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
) -> Result<RuleLayerBacktestData, String> {
    validate_backtest_strategy_expressions(&source_path)?;
    let source_db = source_db_path(&source_path);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "原始库路径不是有效UTF-8".to_string())?;
    let source_conn =
        Connection::open(source_db_str).map_err(|e| format!("打开原始库失败: {e}"))?;
    let (resolved_board, exclude_st_board, total_mv_min, total_mv_max, allowed_ts_codes) =
        build_backtest_stock_filter(
            &source_path,
            board,
            exclude_st_board,
            total_mv_min,
            total_mv_max,
        )?;

    let params = RuleLayerBacktestRunParams {
        stock_adj_type: stock_adj_type
            .unwrap_or_else(|| "qfq".to_string())
            .trim()
            .to_string(),
        index_ts_code: index_ts_code.trim().to_string(),
        index_beta: index_beta.unwrap_or(0.5),
        concept_beta: concept_beta.unwrap_or(0.2),
        industry_beta: industry_beta.unwrap_or(0.0),
        start_date: start_date.trim().to_string(),
        end_date: end_date.trim().to_string(),
        min_samples_per_day: min_samples_per_rule_day.unwrap_or(5),
        min_listed_trade_days: min_listed_trade_days
            .unwrap_or(DEFAULT_BACKTEST_MIN_LISTED_TRADE_DAYS),
        backtest_period: backtest_period.unwrap_or(1),
        parallel_batch_size: parallel_batch_size
            .unwrap_or(DEFAULT_RULE_WITH_SAMPLES_PARALLEL_BATCH_SIZE)
            .max(1),
        resolved_board,
        exclude_st_board,
        total_mv_min,
        total_mv_max,
        allowed_ts_codes,
    };

    // 当前入口固定全量；后续如需恢复单策略，仅需传入 Some(rule_name)。
    run_rule_layer_backtest_core(&source_conn, &source_path, None, &params)
}

pub fn run_rank_layer_backtest(
    source_path: String,
    stock_adj_type: Option<String>,
    index_ts_code: String,
    index_beta: Option<f64>,
    concept_beta: Option<f64>,
    industry_beta: Option<f64>,
    start_date: String,
    end_date: String,
    min_samples_per_rank_day: Option<usize>,
    min_listed_trade_days: Option<usize>,
    backtest_period: Option<usize>,
    layer_count: Option<usize>,
    layer_method: Option<String>,
    board: Option<String>,
    exclude_st_board: Option<bool>,
) -> Result<RankLayerBacktestData, String> {
    validate_backtest_strategy_expressions(&source_path)?;
    let source_db = source_db_path(&source_path);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "原始库路径不是有效UTF-8".to_string())?;
    let source_conn =
        Connection::open(source_db_str).map_err(|e| format!("打开原始库失败: {e}"))?;
    let (resolved_board, exclude_st_board, _total_mv_min, _total_mv_max, allowed_ts_codes) =
        build_backtest_stock_filter(&source_path, board, exclude_st_board, None, None)?;

    let params = RankLayerBacktestRunParams {
        stock_adj_type: stock_adj_type
            .unwrap_or_else(|| "qfq".to_string())
            .trim()
            .to_string(),
        index_ts_code: index_ts_code.trim().to_string(),
        index_beta: index_beta.unwrap_or(0.5),
        concept_beta: concept_beta.unwrap_or(0.2),
        industry_beta: industry_beta.unwrap_or(0.0),
        start_date: start_date.trim().to_string(),
        end_date: end_date.trim().to_string(),
        min_samples_per_day: min_samples_per_rank_day.unwrap_or(5),
        min_listed_trade_days: min_listed_trade_days
            .unwrap_or(DEFAULT_BACKTEST_MIN_LISTED_TRADE_DAYS),
        backtest_period: backtest_period.unwrap_or(1),
        layer_count: layer_count.unwrap_or_else(RankLayerConfig::default_layer_count),
        layer_method: match layer_method {
            Some(value) => RankLayerMethod::from_str(&value)?,
            None => RankLayerMethod::SampleCount,
        },
        resolved_board,
        exclude_st_board,
        allowed_ts_codes,
    };

    run_rank_layer_backtest_core(&source_conn, &source_path, &params)
}

pub fn run_transient_scene_layer_backtest(
    source_path: String,
    stock_adj_type: Option<String>,
    index_ts_code: String,
    index_beta: Option<f64>,
    concept_beta: Option<f64>,
    industry_beta: Option<f64>,
    start_date: String,
    end_date: String,
    min_samples_per_scene_day: Option<usize>,
    min_listed_trade_days: Option<usize>,
    backtest_period: Option<usize>,
    board: Option<String>,
    exclude_st_board: Option<bool>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
) -> Result<SceneLayerBacktestData, String> {
    validate_backtest_strategy_expressions(&source_path)?;
    let source_db = source_db_path(&source_path);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "原始库路径不是有效UTF-8".to_string())?;
    let source_conn =
        Connection::open(source_db_str).map_err(|e| format!("打开原始库失败: {e}"))?;
    let (resolved_board, exclude_st_board, total_mv_min, total_mv_max, allowed_ts_codes) =
        build_backtest_stock_filter(
            &source_path,
            board,
            exclude_st_board,
            total_mv_min,
            total_mv_max,
        )?;

    let params = SceneLayerBacktestRunParams {
        stock_adj_type: stock_adj_type
            .unwrap_or_else(|| "qfq".to_string())
            .trim()
            .to_string(),
        index_ts_code: index_ts_code.trim().to_string(),
        index_beta: index_beta.unwrap_or(0.5),
        concept_beta: concept_beta.unwrap_or(0.2),
        industry_beta: industry_beta.unwrap_or(0.0),
        start_date: start_date.trim().to_string(),
        end_date: end_date.trim().to_string(),
        min_samples_per_day: min_samples_per_scene_day.unwrap_or(5),
        min_listed_trade_days: min_listed_trade_days
            .unwrap_or(DEFAULT_BACKTEST_MIN_LISTED_TRADE_DAYS),
        backtest_period: backtest_period.unwrap_or(1),
        resolved_board,
        exclude_st_board,
        total_mv_min,
        total_mv_max,
        allowed_ts_codes,
    };
    let layer_config = SceneLayerConfig {
        min_samples_per_day: params.min_samples_per_day,
        backtest_period: params.backtest_period,
        min_listed_trade_days: params.min_listed_trade_days,
    };
    let (score_batch, _) = scoring_all_to_memory_with_mode(
        &source_path,
        None,
        &params.stock_adj_type,
        &params.start_date,
        &params.end_date,
        ScoringMemoryMode::SceneOnly,
    )?;
    let scene_rows = filter_scene_detail_rows_by_ts_codes(
        score_batch.scene_rows,
        params.allowed_ts_codes.as_ref(),
    );
    let scene_options = load_scene_options(&source_path)?;
    let all_metrics = calc_all_scene_layer_metrics_from_rows(
        &source_conn,
        &source_path,
        &scene_options,
        &scene_rows,
        &params.stock_adj_type,
        &params.index_ts_code,
        params.index_beta,
        params.concept_beta,
        params.industry_beta,
        &params.start_date,
        &params.end_date,
        &layer_config,
    )?;
    let mut all_scene_summaries = Vec::with_capacity(all_metrics.len());
    for (one_scene_name, metrics) in all_metrics {
        all_scene_summaries.push(SceneLayerSceneSummary {
            scene_name: one_scene_name,
            point_count: metrics.points.len(),
            spread_mean: metrics.spread_mean,
            ic_mean: metrics.ic_mean,
            ic_std: metrics.ic_std,
            icir: metrics.icir,
            ic_t_value: metrics.ic_t_value,
        });
    }
    all_scene_summaries.sort_by(|a, b| {
        b.spread_mean
            .unwrap_or(f64::NEG_INFINITY)
            .partial_cmp(&a.spread_mean.unwrap_or(f64::NEG_INFINITY))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.point_count.cmp(&a.point_count))
            .then_with(|| a.scene_name.cmp(&b.scene_name))
    });

    Ok(SceneLayerBacktestData {
        scene_name: String::new(),
        stock_adj_type: params.stock_adj_type,
        index_ts_code: params.index_ts_code,
        index_beta: params.index_beta,
        concept_beta: params.concept_beta,
        industry_beta: params.industry_beta,
        start_date: params.start_date,
        end_date: params.end_date,
        resolved_board: params.resolved_board,
        exclude_st_board: params.exclude_st_board,
        total_mv_min: params.total_mv_min,
        total_mv_max: params.total_mv_max,
        min_samples_per_scene_day: params.min_samples_per_day,
        min_listed_trade_days: params.min_listed_trade_days,
        backtest_period: params.backtest_period,
        points: Vec::new(),
        spread_mean: None,
        ic_mean: None,
        ic_std: None,
        icir: None,
        ic_t_value: None,
        is_all_scenes: true,
        all_scene_summaries,
    })
}

pub fn run_transient_rule_layer_backtest(
    source_path: String,
    stock_adj_type: Option<String>,
    index_ts_code: String,
    index_beta: Option<f64>,
    concept_beta: Option<f64>,
    industry_beta: Option<f64>,
    start_date: String,
    end_date: String,
    min_samples_per_rule_day: Option<usize>,
    min_listed_trade_days: Option<usize>,
    backtest_period: Option<usize>,
    parallel_batch_size: Option<usize>,
    board: Option<String>,
    exclude_st_board: Option<bool>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
) -> Result<RuleLayerBacktestData, String> {
    validate_backtest_strategy_expressions(&source_path)?;
    let source_db = source_db_path(&source_path);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "原始库路径不是有效UTF-8".to_string())?;
    let source_conn =
        Connection::open(source_db_str).map_err(|e| format!("打开原始库失败: {e}"))?;
    let (resolved_board, exclude_st_board, total_mv_min, total_mv_max, allowed_ts_codes) =
        build_backtest_stock_filter(
            &source_path,
            board,
            exclude_st_board,
            total_mv_min,
            total_mv_max,
        )?;

    let params = RuleLayerBacktestRunParams {
        stock_adj_type: stock_adj_type
            .unwrap_or_else(|| "qfq".to_string())
            .trim()
            .to_string(),
        index_ts_code: index_ts_code.trim().to_string(),
        index_beta: index_beta.unwrap_or(0.5),
        concept_beta: concept_beta.unwrap_or(0.2),
        industry_beta: industry_beta.unwrap_or(0.0),
        start_date: start_date.trim().to_string(),
        end_date: end_date.trim().to_string(),
        min_samples_per_day: min_samples_per_rule_day.unwrap_or(5),
        min_listed_trade_days: min_listed_trade_days
            .unwrap_or(DEFAULT_BACKTEST_MIN_LISTED_TRADE_DAYS),
        backtest_period: backtest_period.unwrap_or(1),
        parallel_batch_size: parallel_batch_size
            .unwrap_or(DEFAULT_RULE_WITH_SAMPLES_PARALLEL_BATCH_SIZE)
            .max(1),
        resolved_board,
        exclude_st_board,
        total_mv_min,
        total_mv_max,
        allowed_ts_codes,
    };
    let layer_config = RuleLayerConfig {
        min_samples_per_day: params.min_samples_per_day,
        backtest_period: params.backtest_period,
        min_listed_trade_days: params.min_listed_trade_days,
    };
    let (score_batch, _) = scoring_all_to_memory_with_mode(
        &source_path,
        None,
        &params.stock_adj_type,
        &params.start_date,
        &params.end_date,
        ScoringMemoryMode::SummaryAndDetails,
    )?;
    let summary_rows = filter_score_summary_rows_by_ts_codes(
        score_batch.summary_rows,
        params.allowed_ts_codes.as_ref(),
    );
    let detail_rows = filter_score_detail_rows_by_ts_codes(
        score_batch.detail_rows,
        params.allowed_ts_codes.as_ref(),
    );
    let (rule_options, rule_meta_map) = load_rule_meta(&source_path)?;
    let explain_map = rule_meta_map
        .iter()
        .map(|(rule_name, meta)| (rule_name.clone(), meta.explain.clone()))
        .collect::<HashMap<_, _>>();
    let has_rule_meta_match = rule_options
        .iter()
        .any(|rule_name| rule_meta_map.contains_key(rule_name));
    let stock_meta_map = if has_rule_meta_match {
        load_validation_sample_stock_meta_map(&source_path)?
    } else {
        HashMap::new()
    };
    let similarity_cache = if has_rule_meta_match {
        load_validation_similarity_cache_optional(
            &source_path,
            &params.start_date,
            &params.end_date,
        )?
    } else {
        empty_validation_similarity_cache()
    };
    let contribution_averages = build_rule_contribution_averages_from_rows(
        &summary_rows,
        &detail_rows,
        &params.start_date,
        &params.end_date,
    );
    let summary_detail_items = calc_all_rule_layer_metrics_with_samples_from_rows_map(
        &source_conn,
        &source_path,
        &rule_options,
        &summary_rows,
        &detail_rows,
        &params.stock_adj_type,
        &params.index_ts_code,
        params.index_beta,
        params.concept_beta,
        params.industry_beta,
        &params.start_date,
        &params.end_date,
        &layer_config,
        params.parallel_batch_size,
        |one_rule_name, metrics_with_samples| {
            Ok(build_one_rule_backtest_summary_and_detail(
                one_rule_name,
                metrics_with_samples,
                &rule_meta_map,
                &contribution_averages,
                &explain_map,
                &params,
                &layer_config,
                &similarity_cache,
                &stock_meta_map,
            ))
        },
    );
    let (all_rule_summaries, rule_validation_details) =
        split_and_sort_rule_backtest_summaries_and_details(summary_detail_items?);
    let decay_validations = build_all_rule_decay_validations(&all_rule_summaries);

    let (
        avg_residual_mean,
        avg_excess_residual_mean,
        avg_er_change,
        profit_loss_ratio,
        _spread_mean,
        ic_mean,
        ic_std,
        icir,
        ic_t_value,
    ) = aggregate_all_rule_summary_metrics(&all_rule_summaries);
    Ok(RuleLayerBacktestData {
        rule_name: String::new(),
        stock_adj_type: params.stock_adj_type,
        index_ts_code: params.index_ts_code,
        index_beta: params.index_beta,
        concept_beta: params.concept_beta,
        industry_beta: params.industry_beta,
        start_date: params.start_date,
        end_date: params.end_date,
        resolved_board: params.resolved_board,
        exclude_st_board: params.exclude_st_board,
        total_mv_min: params.total_mv_min,
        total_mv_max: params.total_mv_max,
        min_samples_per_rule_day: params.min_samples_per_day,
        min_listed_trade_days: params.min_listed_trade_days,
        backtest_period: params.backtest_period,
        points: Vec::new(),
        avg_residual_mean,
        avg_excess_residual_mean,
        decay_validations,
        avg_er_change,
        profit_loss_ratio,
        spread_mean: None,
        avg_contribution_score: weighted_rule_summary_metric(&all_rule_summaries, |item| {
            item.avg_contribution_score
        }),
        avg_contribution_per_trigger: weighted_rule_summary_metric(&all_rule_summaries, |item| {
            item.avg_contribution_per_trigger
        }),
        ic_mean,
        ic_std,
        icir,
        ic_t_value,
        layer_count: None,
        layer_method: None,
        layer_method_label: None,
        layer_summaries: Vec::new(),
        is_all_rules: true,
        all_rule_summaries,
        rule_validation_details,
    })
}

pub fn run_transient_rank_layer_backtest(
    source_path: String,
    stock_adj_type: Option<String>,
    index_ts_code: String,
    index_beta: Option<f64>,
    concept_beta: Option<f64>,
    industry_beta: Option<f64>,
    start_date: String,
    end_date: String,
    min_samples_per_rank_day: Option<usize>,
    min_listed_trade_days: Option<usize>,
    backtest_period: Option<usize>,
    layer_count: Option<usize>,
    layer_method: Option<String>,
    board: Option<String>,
    exclude_st_board: Option<bool>,
) -> Result<RankLayerBacktestData, String> {
    validate_backtest_strategy_expressions(&source_path)?;
    let source_db = source_db_path(&source_path);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "原始库路径不是有效UTF-8".to_string())?;
    let source_conn =
        Connection::open(source_db_str).map_err(|e| format!("打开原始库失败: {e}"))?;
    let (resolved_board, exclude_st_board, _total_mv_min, _total_mv_max, allowed_ts_codes) =
        build_backtest_stock_filter(&source_path, board, exclude_st_board, None, None)?;

    let params = RankLayerBacktestRunParams {
        stock_adj_type: stock_adj_type
            .unwrap_or_else(|| "qfq".to_string())
            .trim()
            .to_string(),
        index_ts_code: index_ts_code.trim().to_string(),
        index_beta: index_beta.unwrap_or(0.5),
        concept_beta: concept_beta.unwrap_or(0.2),
        industry_beta: industry_beta.unwrap_or(0.0),
        start_date: start_date.trim().to_string(),
        end_date: end_date.trim().to_string(),
        min_samples_per_day: min_samples_per_rank_day.unwrap_or(5),
        min_listed_trade_days: min_listed_trade_days
            .unwrap_or(DEFAULT_BACKTEST_MIN_LISTED_TRADE_DAYS),
        backtest_period: backtest_period.unwrap_or(1),
        layer_count: layer_count.unwrap_or_else(RankLayerConfig::default_layer_count),
        layer_method: match layer_method {
            Some(value) => RankLayerMethod::from_str(&value)?,
            None => RankLayerMethod::SampleCount,
        },
        resolved_board,
        exclude_st_board,
        allowed_ts_codes,
    };
    let joint_params = rule_joint_validation_params_from_rank(&params);
    let layer_config = RankLayerConfig {
        min_samples_per_day: params.min_samples_per_day,
        backtest_period: params.backtest_period,
        min_listed_trade_days: params.min_listed_trade_days,
        layer_count: params.layer_count,
        layer_method: params.layer_method,
    };
    let input = RankLayerFromDbInput {
        stock_adj_type: params.stock_adj_type,
        index_ts_code: params.index_ts_code,
        index_beta: params.index_beta,
        concept_beta: params.concept_beta,
        industry_beta: params.industry_beta,
        start_date: params.start_date,
        end_date: params.end_date,
        layer_config,
    };
    let (score_batch, _) = scoring_all_to_memory_with_mode(
        &source_path,
        None,
        &input.stock_adj_type,
        &input.start_date,
        &input.end_date,
        ScoringMemoryMode::SummaryOnly,
    )?;
    let summary_rows = filter_score_summary_rows_by_ts_codes(
        score_batch.summary_rows,
        params.allowed_ts_codes.as_ref(),
    );
    let metrics =
        calc_rank_layer_metrics_from_score_rows(&source_conn, &source_path, &input, &summary_rows)?;
    let market_value_summaries = build_rank_market_value_summaries(
        &source_path,
        &input,
        &summary_rows,
        &metrics.layer_samples,
    )?;
    let stock_meta_map = load_validation_sample_stock_meta_map(&source_path)?;
    let layer_sample_groups = build_rank_layer_sample_groups(
        &metrics.layer_samples,
        input.layer_config.layer_count,
        &stock_meta_map,
    );
    let joint_validation_continuation_id =
        store_rule_joint_validation_from_rows(&source_path, &joint_params, summary_rows);

    Ok(RankLayerBacktestData {
        stock_adj_type: input.stock_adj_type,
        index_ts_code: input.index_ts_code,
        index_beta: input.index_beta,
        concept_beta: input.concept_beta,
        industry_beta: input.industry_beta,
        start_date: input.start_date,
        end_date: input.end_date,
        resolved_board: params.resolved_board,
        exclude_st_board: params.exclude_st_board,
        market_value_grouping: true,
        min_samples_per_rank_day: input.layer_config.effective_min_samples_per_day(),
        min_listed_trade_days: input.layer_config.min_listed_trade_days,
        backtest_period: input.layer_config.backtest_period,
        layer_count: input.layer_config.layer_count,
        layer_method: input.layer_config.layer_method.as_str().to_string(),
        layer_method_label: rank_layer_method_label(input.layer_config.layer_method).to_string(),
        point_count: metrics.point_count,
        sample_count: metrics.sample_count,
        avg_er_change: metrics.avg_er_change,
        spread_mean: metrics.spread_mean,
        ic_mean: metrics.ic_mean,
        ic_std: metrics.ic_std,
        icir: metrics.icir,
        ic_t_value: metrics.ic_t_value,
        layer_summaries: metrics
            .layers
            .into_iter()
            .map(|item| RankLayerBucketSummary {
                layer_index: item.layer_index,
                layer_label: rank_layer_label(item.layer_index, input.layer_config.layer_count),
                point_count: item.point_count,
                sample_count: item.sample_count,
                avg_score: item.avg_score,
                avg_residual_return: item.avg_residual_return,
                avg_er_change: item.avg_er_change,
            })
            .collect(),
        layer_sample_groups,
        market_value_summaries,
        joint_validation_continuation_id,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        fs::{create_dir_all, write},
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use duckdb::{Connection, params};

    use crate::{
        data::{
            DataReader, RuleTag, result_db_path,
            scoring_data::{ScoreDetails, ScoreSummary},
            source_db_path,
        },
        scoring::tools::load_st_list,
        simulate::rank::RankLayerSamplePoint,
        simulate::rule::{RuleJointRidgeDayStats, RuleLayerPoint, RuleLayerSamplePoint},
    };

    use super::{
        JointRidgeAggregate, PreparedValidationCombo, RuleJointValidationFeature, VALIDATION_EPS,
        ValidationSampleRawRow, ValidationSampleStockMeta, ValidationSeedRule,
        ValidationSimilarityCache, ValidationVariant, build_industry_maps_from_rows,
        build_joint_exposures, build_joint_walk_forward_models, build_rank_layer_sample_groups,
        build_recent_decay_dist_points, build_rule_basket_decay_from_daily_groups,
        build_rule_contribution_averages_from_rows, build_rule_decay_validations,
        build_validation_cached_rule, build_validation_calibration_specs,
        build_validation_return_distribution, build_validation_sample_groups,
        build_validation_similarity_rows, build_validation_triggered_scores,
        build_validation_triggered_scores_for_combos, calibration_stability_factor,
        collect_rule_validation_runtime_keys, collect_validation_assigned_names,
        derive_validation_volatility_group, estimate_net_money_flow_yuan,
        evaluate_joint_validation_gate, joint_oos_contributions, joint_prediction_gain,
        money_flow_rank_items, money_outflow_rank_items, resolve_validation_sample_board_label,
        resolve_validation_trigger_count, scope_way_config_label, solve_positive_definite,
        trailing_period_gain,
    };
    use crate::data::ScopeWay;

    #[test]
    fn market_analysis_industry_map_uses_industry_instead_of_market_board() {
        let rows = vec![
            vec![
                "000001.SZ",
                "000001",
                "平安银行",
                "深圳",
                "银行",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "主板",
            ],
            vec![
                "300001.SZ",
                "300001",
                "特锐德",
                "青岛",
                "专用设备",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "创业板",
            ],
        ]
        .into_iter()
        .map(|row| row.into_iter().map(str::to_string).collect())
        .collect();

        let (industry_map, industry_counts) = build_industry_maps_from_rows(rows);

        assert_eq!(
            industry_map.get("000001.SZ"),
            Some(&vec!["银行".to_string()])
        );
        assert_eq!(
            industry_map.get("300001.SZ"),
            Some(&vec!["专用设备".to_string()])
        );
        assert!(!industry_counts.contains_key("主板"));
        assert!(!industry_counts.contains_key("创业板"));
    }

    #[test]
    fn market_analysis_money_flow_converts_volume_to_yuan() {
        assert_eq!(
            estimate_net_money_flow_yuan(100.0, 1_000.0, 5_000.0),
            Some(500_000.0)
        );
        assert_eq!(estimate_net_money_flow_yuan(100.0, 0.0, 5_000.0), None);
        assert_eq!(
            estimate_net_money_flow_yuan(f64::NAN, 1_000.0, 5_000.0),
            None
        );
    }

    #[test]
    fn market_analysis_money_flow_only_ranks_positive_eligible_boards() {
        let acc = HashMap::from([
            ("算力".to_string(), 200_000_000.0),
            ("机器人".to_string(), 80_000_000.0),
            ("银行".to_string(), -50_000_000.0),
        ]);
        let counts = HashMap::from([
            ("算力".to_string(), 12),
            ("机器人".to_string(), 1),
            ("银行".to_string(), 20),
        ]);

        let items = money_flow_rank_items(acc, &counts, 2);

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].name, "算力");
        assert_eq!(items[0].value, 200_000_000.0);
    }

    #[test]
    fn market_analysis_money_outflow_ranks_largest_outflow_first() {
        let acc = HashMap::from([
            ("算力".to_string(), 20_000_000.0),
            ("机器人".to_string(), -80_000_000.0),
            ("银行".to_string(), -150_000_000.0),
        ]);
        let counts = HashMap::from([
            ("算力".to_string(), 12),
            ("机器人".to_string(), 8),
            ("银行".to_string(), 20),
        ]);

        let items = money_outflow_rank_items(acc, &counts, 2);

        assert_eq!(items.len(), 2);
        assert_eq!(items[0].name, "银行");
        assert_eq!(items[0].value, -150_000_000.0);
        assert_eq!(items[1].name, "机器人");
    }

    #[test]
    fn market_analysis_trailing_gain_uses_requested_trade_day_window() {
        let rows = vec![
            ("20240102".to_string(), 10.0),
            ("20240103".to_string(), 11.0),
            ("20240104".to_string(), 12.0),
            ("20240105".to_string(), 15.0),
            ("20240108".to_string(), 20.0),
            ("20240109".to_string(), 24.0),
        ];

        let three_day = trailing_period_gain(&rows, 3).expect("three day gain");
        let five_day = trailing_period_gain(&rows, 5).expect("five day gain");

        assert!((three_day - 100.0).abs() < 1e-9);
        assert!((five_day - 140.0).abs() < 1e-9);
        assert_eq!(trailing_period_gain(&rows[..5], 5), None);
    }

    fn temp_source_dir() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("lianghua_validation_trigger_scores_{unique}"))
    }

    fn prepare_validation_source_files(source_dir: &str) {
        create_dir_all(source_dir).expect("create source dir");

        write(
            PathBuf::from(source_dir).join("trade_calendar.csv"),
            "cal_date\n20240102\n20240103\n20240104\n",
        )
        .expect("write trade_calendar.csv");

        write(
            PathBuf::from(source_dir).join("stock_list.csv"),
            "ts_code,unused,name\n000001.SZ,,样本股\n",
        )
        .expect("write stock_list.csv");

        let source_conn = Connection::open(source_db_path(source_dir)).expect("open source db");
        source_conn
            .execute(
                r#"
                CREATE TABLE stock_data (
                    ts_code VARCHAR,
                    trade_date VARCHAR,
                    adj_type VARCHAR,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    vol DOUBLE,
                    amount DOUBLE,
                    pre_close DOUBLE,
                    change DOUBLE,
                    pct_chg DOUBLE
                )
                "#,
                [],
            )
            .expect("create stock_data");

        let mut app = source_conn
            .appender("stock_data")
            .expect("stock_data appender");
        app.append_row(params![
            "000001.SZ",
            "20240102",
            "qfq",
            10.0_f64,
            10.5_f64,
            9.8_f64,
            10.2_f64,
            1000.0_f64,
            10000.0_f64,
            10.0_f64,
            0.2_f64,
            2.0_f64,
        ])
        .expect("insert stock row1");
        app.append_row(params![
            "000001.SZ",
            "20240103",
            "qfq",
            10.2_f64,
            11.0_f64,
            10.1_f64,
            10.8_f64,
            1100.0_f64,
            11000.0_f64,
            10.2_f64,
            0.6_f64,
            5.88_f64,
        ])
        .expect("insert stock row2");
        app.append_row(params![
            "000001.SZ",
            "20240104",
            "qfq",
            10.8_f64,
            11.3_f64,
            10.7_f64,
            11.1_f64,
            1200.0_f64,
            12000.0_f64,
            10.8_f64,
            0.3_f64,
            2.78_f64,
        ])
        .expect("insert stock row3");
        app.flush().expect("flush stock_data");
    }

    #[test]
    fn rule_expression_validation_reports_bad_expression_before_stock_filter() {
        let source_dir = temp_source_dir();
        let source_dir_str = source_dir.to_str().expect("utf8 source dir");
        create_dir_all(source_dir_str).expect("create source dir");
        write(
            PathBuf::from(source_dir_str).join("score_rule.toml"),
            r#"
version = 1

[[scene]]
name = "趋势启动"
direction = "long"
observe_threshold = 1.0
trigger_threshold = 2.0
confirm_threshold = 3.0
fail_threshold = 1.0

[[rule]]
name = "有效策略"
scene = "趋势启动"
stage = "base"
scope_windows = 1
scope_way = "LAST"
when = "C > O"
points = 1.0
explain = "test"
"#,
        )
        .expect("write score_rule.toml");

        let error = super::run_rule_expression_validation(
            source_dir_str.to_string(),
            String::new(),
            Some("MA(C,".to_string()),
            Some("LAST".to_string()),
            Some(1),
            Some("qfq".to_string()),
            "000001.SH".to_string(),
            Some(0.5),
            Some(0.2),
            Some(0.0),
            "20240102".to_string(),
            "20240104".to_string(),
            Some(1),
            Some(0),
            Some(1),
            None,
            None,
            Some(1),
            Some("主板".to_string()),
            Some(false),
            None,
            None,
        )
        .expect_err("bad expression should fail before stock filtering");

        assert!(error.contains("表达式解析错误"), "{error}");
        assert!(!error.contains("stock_list.csv"), "{error}");
    }

    #[test]
    fn transient_rule_contribution_averages_match_rank_weight_formula() {
        let summary_rows = vec![
            ScoreSummary {
                ts_code: "000001.SZ".to_string(),
                trade_date: "20240102".to_string(),
                total_score: 10.0,
                rank: Some(1),
            },
            ScoreSummary {
                ts_code: "000002.SZ".to_string(),
                trade_date: "20240102".to_string(),
                total_score: 5.0,
                rank: Some(2),
            },
            ScoreSummary {
                ts_code: "000001.SZ".to_string(),
                trade_date: "20240103".to_string(),
                total_score: 3.0,
                rank: Some(2),
            },
            ScoreSummary {
                ts_code: "000002.SZ".to_string(),
                trade_date: "20240103".to_string(),
                total_score: 9.0,
                rank: Some(1),
            },
        ];
        let detail_rows = vec![
            ScoreDetails {
                ts_code: "000001.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_name: "规则A".to_string(),
                rule_score: 2.0,
            },
            ScoreDetails {
                ts_code: "000002.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_name: "规则A".to_string(),
                rule_score: 1.0,
            },
            ScoreDetails {
                ts_code: "000001.SZ".to_string(),
                trade_date: "20240103".to_string(),
                rule_name: "规则A".to_string(),
                rule_score: -2.0,
            },
            ScoreDetails {
                ts_code: "000002.SZ".to_string(),
                trade_date: "20240103".to_string(),
                rule_name: "规则B".to_string(),
                rule_score: 3.0,
            },
        ];

        let averages = build_rule_contribution_averages_from_rows(
            &summary_rows,
            &detail_rows,
            "20240102",
            "20240103",
        );

        let rule_a = averages.get("规则A").expect("rule A averages");
        assert_eq!(rule_a.avg_contribution_score, Some(0.75));
        assert_eq!(rule_a.avg_contribution_per_trigger, Some(0.5));

        let rule_b = averages.get("规则B").expect("rule B averages");
        assert_eq!(rule_b.avg_contribution_score, Some(3.0));
        assert_eq!(rule_b.avg_contribution_per_trigger, Some(3.0));
    }

    #[test]
    fn validation_return_distribution_uses_symmetric_percent_buckets() {
        let samples = [-12.0, -10.0, -7.0, -3.0, -2.0, 0.0, 2.0, 3.0, 8.0, 11.0]
            .into_iter()
            .map(|residual_return| RuleLayerSamplePoint {
                ts_code: "000001.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 1.0,
                residual_return,
                er_change: f64::INFINITY,
            })
            .collect::<Vec<_>>();

        let buckets = build_validation_return_distribution(&samples);

        assert_eq!(buckets.len(), 7);
        assert_eq!(
            buckets
                .iter()
                .map(|bucket| bucket.sample_count)
                .collect::<Vec<_>>(),
            vec![2, 1, 2, 2, 1, 1, 1]
        );
        assert_eq!(buckets[0].sample_ratio, Some(0.2));
    }

    fn prepare_validation_result_rank_rows(source_dir: &str) {
        let result_conn = Connection::open(result_db_path(source_dir)).expect("open result db");
        result_conn
            .execute(
                r#"
                CREATE TABLE score_summary (
                    ts_code VARCHAR,
                    trade_date VARCHAR,
                    total_score DOUBLE,
                    rank BIGINT
                )
                "#,
                [],
            )
            .expect("create score_summary");
        result_conn
            .execute(
                "INSERT INTO score_summary VALUES (?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?)",
                params![
                    "000001.SZ",
                    "20240102",
                    80.0_f64,
                    3_i64,
                    "000001.SZ",
                    "20240103",
                    90.0_f64,
                    2_i64,
                    "000001.SZ",
                    "20240104",
                    100.0_f64,
                    1_i64,
                ],
            )
            .expect("insert rank rows");
    }

    #[test]
    fn validation_triggered_scores_cover_full_analysis_window() {
        let source_dir = temp_source_dir();
        let source_dir_str = source_dir.to_str().expect("utf8 source dir");
        prepare_validation_source_files(source_dir_str);

        let cached_rule = build_validation_cached_rule(
            "validation_test_rule".to_string(),
            ScopeWay::Any,
            1,
            1.0,
            None,
            RuleTag::Normal,
            "C > 0",
        )
        .expect("build cached rule");

        let triggered_score_map = build_validation_triggered_scores(
            source_dir_str,
            "qfq",
            "20240102",
            "20240104",
            &cached_rule,
        )
        .expect("build triggered scores");

        let date_score_map = triggered_score_map
            .get("000001.SZ")
            .expect("ts_code should have triggered scores");

        assert_eq!(date_score_map.len(), 3);
        assert!(date_score_map.contains_key("20240102"));
        assert!(date_score_map.contains_key("20240103"));
        assert!(date_score_map.contains_key("20240104"));
        assert_eq!(
            triggered_score_map
                .values()
                .map(|item| item.len())
                .sum::<usize>(),
            3
        );
    }

    #[test]
    fn validation_triggered_scores_inject_uppercase_rank() {
        let source_dir = temp_source_dir();
        let source_dir_str = source_dir.to_str().expect("utf8 source dir");
        prepare_validation_source_files(source_dir_str);
        prepare_validation_result_rank_rows(source_dir_str);

        let cached_rule = build_validation_cached_rule(
            "validation_rank_rule".to_string(),
            ScopeWay::Any,
            1,
            1.0,
            None,
            RuleTag::Normal,
            "RANK <= 2",
        )
        .expect("build cached rule");

        let triggered_score_map = build_validation_triggered_scores(
            source_dir_str,
            "qfq",
            "20240102",
            "20240104",
            &cached_rule,
        )
        .expect("build triggered scores");

        let date_score_map = triggered_score_map
            .get("000001.SZ")
            .expect("ts_code should have rank-triggered scores");

        assert_eq!(date_score_map.len(), 2);
        assert!(!date_score_map.contains_key("20240102"));
        assert!(date_score_map.contains_key("20240103"));
        assert!(date_score_map.contains_key("20240104"));
    }

    #[test]
    fn validation_sample_board_prefers_market_label_and_derives_group() {
        assert_eq!(
            resolve_validation_sample_board_label("688001.SH", Some("样本股"), Some("科创板")),
            "科创板"
        );
        assert_eq!(derive_validation_volatility_group("科创板"), "高波动");
    }

    #[test]
    fn validation_sample_board_keeps_st_override() {
        assert_eq!(
            resolve_validation_sample_board_label("000001.SZ", Some("*ST样本"), Some("主板")),
            "ST"
        );
        assert_eq!(derive_validation_volatility_group("ST"), "其他波动");
    }

    #[test]
    fn validation_trigger_count_uses_each_score_multiple() {
        assert_eq!(resolve_validation_trigger_count(3.0, true, 1.0, false), 3);
        assert_eq!(resolve_validation_trigger_count(-4.0, true, -1.0, false), 4);
        assert_eq!(resolve_validation_trigger_count(6.0, true, 2.0, false), 3);
        assert_eq!(resolve_validation_trigger_count(3.0, false, 1.0, false), 1);
        assert_eq!(resolve_validation_trigger_count(3.0, true, 1.0, true), 1);
    }

    #[test]
    fn validation_sample_limit_applies_per_board_and_direction() {
        let samples = vec![
            ValidationSampleRawRow {
                ts_code: "BJ0001.BJ".to_string(),
                trade_date: "20240102".to_string(),
                trigger_count: 1,
                rule_score: 1.0,
                residual_return: 9.0,
            },
            ValidationSampleRawRow {
                ts_code: "BJ0001.BJ".to_string(),
                trade_date: "20240103".to_string(),
                trigger_count: 1,
                rule_score: 1.0,
                residual_return: 8.0,
            },
            ValidationSampleRawRow {
                ts_code: "MB0001.SZ".to_string(),
                trade_date: "20240102".to_string(),
                trigger_count: 1,
                rule_score: 1.0,
                residual_return: 7.0,
            },
            ValidationSampleRawRow {
                ts_code: "MB0001.SZ".to_string(),
                trade_date: "20240103".to_string(),
                trigger_count: 1,
                rule_score: 1.0,
                residual_return: 6.0,
            },
            ValidationSampleRawRow {
                ts_code: "BJ0001.BJ".to_string(),
                trade_date: "20240104".to_string(),
                trigger_count: 1,
                rule_score: 1.0,
                residual_return: -7.0,
            },
            ValidationSampleRawRow {
                ts_code: "BJ0001.BJ".to_string(),
                trade_date: "20240105".to_string(),
                trigger_count: 1,
                rule_score: 1.0,
                residual_return: -8.0,
            },
            ValidationSampleRawRow {
                ts_code: "MB0001.SZ".to_string(),
                trade_date: "20240104".to_string(),
                trigger_count: 1,
                rule_score: 1.0,
                residual_return: -5.0,
            },
            ValidationSampleRawRow {
                ts_code: "MB0001.SZ".to_string(),
                trade_date: "20240105".to_string(),
                trigger_count: 1,
                rule_score: 1.0,
                residual_return: -6.0,
            },
        ];
        let stock_meta_map = HashMap::from([
            (
                "BJ0001.BJ".to_string(),
                ValidationSampleStockMeta {
                    name: Some("北交样本".to_string()),
                    board: "北交所".to_string(),
                    volatility_group: "高波动".to_string(),
                },
            ),
            (
                "MB0001.SZ".to_string(),
                ValidationSampleStockMeta {
                    name: Some("主板样本".to_string()),
                    board: "主板".to_string(),
                    volatility_group: "常规波动".to_string(),
                },
            ),
        ]);

        let (stats, groups) = build_validation_sample_groups(&samples, 1, &stock_meta_map);

        assert_eq!(stats.positive_count, 4);
        assert_eq!(stats.negative_count, 4);
        assert_eq!(stats.random_count, 8);
        assert_eq!(stats.total_samples, 8);

        let count_boards = |rows: &[super::RuleValidationSampleRow]| {
            rows.iter()
                .fold(HashMap::<String, usize>::new(), |mut acc, row| {
                    *acc.entry(row.board.clone()).or_insert(0) += 1;
                    acc
                })
        };

        let positive_boards = count_boards(&groups.positive);
        let negative_boards = count_boards(&groups.negative);
        let random_boards = count_boards(&groups.random);

        assert_eq!(groups.positive.len(), 2);
        assert_eq!(positive_boards.get("北交所"), Some(&1));
        assert_eq!(positive_boards.get("主板"), Some(&1));

        assert_eq!(groups.negative.len(), 2);
        assert_eq!(negative_boards.get("北交所"), Some(&1));
        assert_eq!(negative_boards.get("主板"), Some(&1));

        assert_eq!(groups.random.len(), 2);
        assert_eq!(random_boards.get("北交所"), Some(&1));
        assert_eq!(random_boards.get("主板"), Some(&1));
    }

    #[test]
    fn rank_layer_samples_keep_full_observation_counts_and_per_board_limit() {
        let mut samples = Vec::new();
        let mut stock_meta_map = HashMap::new();
        for (board_prefix, board) in [("MB", "主板"), ("CY", "创业板")] {
            for index in 0..6 {
                let ts_code = format!("{board_prefix}{index:04}.SZ");
                stock_meta_map.insert(
                    ts_code.clone(),
                    ValidationSampleStockMeta {
                        name: None,
                        board: board.to_string(),
                        volatility_group: "常规波动".to_string(),
                    },
                );
                samples.push(RankLayerSamplePoint {
                    layer_index: 1,
                    ts_code: ts_code.clone(),
                    trade_date: "20240102".to_string(),
                    score: 10.0,
                    residual_return: index as f64 + 1.0,
                    er_change: f64::INFINITY,
                });
                samples.push(RankLayerSamplePoint {
                    layer_index: 1,
                    ts_code,
                    trade_date: "20240103".to_string(),
                    score: 10.0,
                    residual_return: index as f64 + 11.0,
                    er_change: f64::INFINITY,
                });
            }
        }

        let groups = build_rank_layer_sample_groups(&samples, 1, &stock_meta_map);
        let group = &groups[0];

        assert_eq!(group.total_samples, 24);
        assert_eq!(group.triggered_days, 2);
        assert_eq!(group.positive_count, 24);
        assert_eq!(group.positive.len(), 10);
        assert_eq!(group.positive[0].residual_return, 16.0);
        assert!(
            group
                .positive
                .iter()
                .all(|row| row.trade_date == "20240103")
        );
    }

    #[test]
    fn validation_batch_scores_restore_overwritten_base_series() {
        let source_dir = temp_source_dir();
        let source_dir_str = source_dir.to_str().expect("utf8 source dir");
        prepare_validation_source_files(source_dir_str);

        let first_rule = build_validation_cached_rule(
            "validation_combo_001".to_string(),
            ScopeWay::Any,
            1,
            1.0,
            None,
            RuleTag::Normal,
            "C := REF(C, 1); C > 0",
        )
        .expect("build first cached rule");
        let second_rule = build_validation_cached_rule(
            "validation_combo_002".to_string(),
            ScopeWay::Any,
            1,
            1.0,
            None,
            RuleTag::Normal,
            "C := REF(C, 2); C > 0",
        )
        .expect("build second cached rule");

        let expected_first = build_validation_triggered_scores(
            source_dir_str,
            "qfq",
            "20240102",
            "20240104",
            &first_rule,
        )
        .expect("build first triggered scores");
        let expected_second = build_validation_triggered_scores(
            source_dir_str,
            "qfq",
            "20240102",
            "20240104",
            &second_rule,
        )
        .expect("build second triggered scores");

        let reader = DataReader::new(source_dir_str).expect("build reader");
        let ts_codes = reader
            .list_ts_code("qfq", "20240102", "20240104")
            .expect("list ts codes");
        let st_list = load_st_list(source_dir_str).expect("load st list");
        let combos = vec![
            PreparedValidationCombo {
                variant: ValidationVariant {
                    combo_key: first_rule.name.clone(),
                    combo_label: first_rule.name.clone(),
                    formula: first_rule.when_src.clone(),
                    unknown_values: Vec::new(),
                },
                cached_rule: first_rule.clone(),
                assigned_names: collect_validation_assigned_names(&first_rule.when_ast),
            },
            PreparedValidationCombo {
                variant: ValidationVariant {
                    combo_key: second_rule.name.clone(),
                    combo_label: second_rule.name.clone(),
                    formula: second_rule.when_src.clone(),
                    unknown_values: Vec::new(),
                },
                cached_rule: second_rule.clone(),
                assigned_names: collect_validation_assigned_names(&second_rule.when_ast),
            },
        ];

        let batch_results = build_validation_triggered_scores_for_combos(
            source_dir_str,
            "qfq",
            "20240102",
            "20240102",
            "20240104",
            3,
            &ts_codes,
            &st_list,
            &combos,
        )
        .expect("build batch triggered scores");

        assert_eq!(batch_results.len(), 2);
        assert_eq!(batch_results[0], expected_first);
        assert_eq!(batch_results[1], expected_second);
    }

    #[test]
    fn rule_validation_runtime_key_collection_skips_injected_fields() {
        let rule = build_validation_cached_rule(
            "validation_runtime_keys".to_string(),
            ScopeWay::Any,
            1,
            1.0,
            None,
            RuleTag::Normal,
            "M := MA(C, 5); M > MY_VALIDATION_IND AND RANK <= 100 AND SCORE > 0 AND ZHANG > 0 AND TOTAL_MV_YI <= 300 AND CYQ_TPR > 0.6",
        )
        .expect("build cached rule");
        let combo = PreparedValidationCombo {
            variant: ValidationVariant {
                combo_key: rule.name.clone(),
                combo_label: rule.name.clone(),
                formula: rule.when_src.clone(),
                unknown_values: Vec::new(),
            },
            cached_rule: rule.clone(),
            assigned_names: collect_validation_assigned_names(&rule.when_ast),
        };

        let keys = collect_rule_validation_runtime_keys(&[combo]);

        for required_key in ["C", "MY_VALIDATION_IND"] {
            assert!(keys.contains(required_key), "missing {required_key}");
        }
        assert!(!keys.contains("TOTAL_MV"));
        for injected_key in ["RANK", "SCORE", "ZHANG", "TOTAL_MV_YI", "CYQ_TPR"] {
            assert!(!keys.contains(injected_key), "unexpected {injected_key}");
        }
        assert!(!keys.contains("O"));
    }

    #[test]
    fn validation_similarity_rows_use_pair_index_cache() {
        let similarity_cache = ValidationSimilarityCache {
            total_samples: 12.0,
            rule_names: vec!["规则A".to_string(), "规则B".to_string()],
            rule_hit_counts: vec![3, 1],
            pair_to_rule_indices: HashMap::from([
                (
                    "000001.SZ".to_string(),
                    HashMap::from([("20240102".to_string(), vec![0, 1])]),
                ),
                (
                    "000002.SZ".to_string(),
                    HashMap::from([("20240103".to_string(), vec![0])]),
                ),
            ]),
        };
        let triggered_samples = vec![
            RuleLayerSamplePoint {
                ts_code: "000001.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 1.0,
                residual_return: 0.5,
                er_change: f64::INFINITY,
            },
            RuleLayerSamplePoint {
                ts_code: "000002.SZ".to_string(),
                trade_date: "20240103".to_string(),
                rule_score: 1.0,
                residual_return: 0.3,
                er_change: f64::INFINITY,
            },
        ];
        let explain_map = HashMap::from([("规则A".to_string(), "说明A".to_string())]);

        let rows = build_validation_similarity_rows(
            &similarity_cache,
            &triggered_samples,
            None,
            &explain_map,
        );

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].rule_name, "规则A");
        assert_eq!(rows[0].overlap_samples, 2);
        assert_eq!(rows[0].overlap_rate_vs_validation, Some(1.0));
        assert_eq!(rows[0].overlap_rate_vs_existing, Some(2.0 / 3.0));
        assert_eq!(rows[0].overlap_lift, Some(4.0));
        assert_eq!(rows[0].explain.as_deref(), Some("说明A"));

        assert_eq!(rows[1].rule_name, "规则B");
        assert_eq!(rows[1].overlap_samples, 1);
        assert_eq!(rows[1].overlap_rate_vs_validation, Some(0.5));
        assert_eq!(rows[1].overlap_rate_vs_existing, Some(1.0));
        assert_eq!(rows[1].overlap_lift, Some(6.0));
        assert!(rows[1].explain.is_none());
    }

    #[test]
    fn validation_calibration_candidates_cover_trigger_modes_without_plain_duplicate() {
        let seed_rule = ValidationSeedRule {
            rule_name: "测试策略".to_string(),
            rule_explain: String::new(),
            scope_way: ScopeWay::Last,
            scope_windows: 1,
            formula: "C > O".to_string(),
            points: 1.0,
            dist_points: None,
            tag: RuleTag::Normal,
            exclude_rule_name: None,
        };

        let specs = build_validation_calibration_specs(&seed_rule);

        assert_eq!(
            specs
                .iter()
                .filter(|item| {
                    scope_way_config_label(item.scope_way) == "LAST" && item.scope_windows == 1
                })
                .count(),
            1
        );
        for scope_label in ["ANY", "EACH", "CONSEC>=2", "CONSEC>=3", "RECENT"] {
            assert!(
                specs
                    .iter()
                    .any(|item| scope_way_config_label(item.scope_way) == scope_label),
                "missing {scope_label}"
            );
        }
    }

    #[test]
    fn validation_recent_decay_weights_keep_direction_and_decay() {
        let positive = build_recent_decay_dist_points(3, 1.0);
        let negative = build_recent_decay_dist_points(3, -1.0);

        assert_eq!(positive.len(), 3);
        assert!((positive[0].points - 1.0).abs() < VALIDATION_EPS);
        assert!((positive[1].points - 0.5).abs() < VALIDATION_EPS);
        assert!((positive[2].points - 0.25).abs() < VALIDATION_EPS);
        assert!((negative[0].points + 1.0).abs() < VALIDATION_EPS);
        assert!((negative[1].points + 0.5).abs() < VALIDATION_EPS);
        assert!((negative[2].points + 0.25).abs() < VALIDATION_EPS);
    }

    #[test]
    fn validation_calibration_stability_requires_both_time_halves() {
        assert_eq!(calibration_stability_factor(1.0, Some(0.3), Some(0.1)), 1.0);
        assert_eq!(
            calibration_stability_factor(-1.0, Some(-0.3), Some(-0.1)),
            1.0
        );
        assert_eq!(
            calibration_stability_factor(1.0, Some(0.3), Some(-0.1)),
            0.5
        );
        assert_eq!(
            calibration_stability_factor(-1.0, Some(0.3), Some(-0.1)),
            0.5
        );
        assert_eq!(
            calibration_stability_factor(1.0, Some(-0.3), Some(-0.1)),
            0.0
        );
    }

    #[test]
    fn joint_ridge_cholesky_solver_matches_known_solution() {
        let solution = solve_positive_definite(vec![4.0, 1.0, 1.0, 3.0], vec![1.0, 2.0])
            .expect("solve positive definite system");

        assert!((solution[0] - 1.0 / 11.0).abs() < 1e-10);
        assert!((solution[1] - 7.0 / 11.0).abs() < 1e-10);
    }

    #[test]
    fn joint_validation_gate_requires_aggregate_fold_and_latest_head_wins() {
        let passed = evaluate_joint_validation_gate(5, 3, Some(0.2), Some(0.1), true);
        assert!(passed.0);
        assert_eq!(passed.1, 3);

        assert!(!evaluate_joint_validation_gate(5, 3, Some(0.05), Some(0.1), true).0);
        assert!(!evaluate_joint_validation_gate(5, 2, Some(0.2), Some(0.1), true).0);
        assert!(!evaluate_joint_validation_gate(5, 4, Some(0.2), Some(0.1), false).0);
        assert!(!evaluate_joint_validation_gate(2, 2, Some(0.2), Some(0.1), true).0);
    }

    #[test]
    fn joint_ridge_oos_contributions_decompose_prediction_gain() {
        let aggregate = JointRidgeAggregate {
            feature_cross_products: vec![100.0, 30.0, 30.0, 80.0],
            feature_residual_products: vec![12.0, 8.0],
            residual_sum_squares: 200.0,
            sample_count: 100,
            exposed_sample_count: 60,
        };
        let beta = vec![0.08, 0.04];
        let contributions = joint_oos_contributions(&beta, &aggregate);

        assert!(
            (contributions.iter().sum::<f64>() - joint_prediction_gain(&beta, &aggregate)).abs()
                < 1e-10
        );
    }

    #[test]
    fn joint_walk_forward_keeps_purge_gap_and_only_trains_on_past_days() {
        let days = (0..120)
            .map(|index| RuleJointRidgeDayStats {
                trade_date: format!("{index:08}"),
                sample_count: 100,
                exposed_sample_count: 50,
                feature_cross_products: vec![100.0],
                feature_residual_products: vec![10.0],
                residual_sum_squares: 100.0,
            })
            .collect::<Vec<_>>();

        let models = build_joint_walk_forward_models(&days, 1, 5, &[1.0]);

        assert_eq!(models.len(), 5);
        for model in models {
            let train_end = model.train_end_date.parse::<usize>().expect("train date");
            let test_start = model.test_start_date.parse::<usize>().expect("test date");
            assert_eq!(test_start - train_end - 1, 5);
            assert!(model.ridge_oos_r2.is_some_and(|value| value > 0.0));
            assert!(model.oos_contributions[0] > 0.0);
        }
    }

    #[test]
    fn joint_exposures_normalize_scores_without_losing_rule_direction() {
        let features = vec![
            RuleJointValidationFeature {
                rule_name: "加分".to_string(),
                explain: String::new(),
                current_points: 3.0,
                score_scale: 3.0,
            },
            RuleJointValidationFeature {
                rule_name: "扣分".to_string(),
                explain: String::new(),
                current_points: -4.0,
                score_scale: 4.0,
            },
        ];
        let details = vec![
            ScoreDetails {
                ts_code: "000001.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_name: "加分".to_string(),
                rule_score: 6.0,
            },
            ScoreDetails {
                ts_code: "000001.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_name: "扣分".to_string(),
                rule_score: -8.0,
            },
        ];

        let (exposures, trigger_counts) = build_joint_exposures(&features, &details);
        let row = &exposures["000001.SZ"]["20240102"];

        assert_eq!(row, &vec![(0, 2.0), (1, -2.0)]);
        assert_eq!(trigger_counts, vec![1, 1]);
    }

    fn decay_test_point(index: usize, score: f64, excess: f64) -> RuleLayerPoint {
        RuleLayerPoint {
            trade_date: format!("{index:08}"),
            sample_count: 10,
            avg_rule_score: Some(score),
            avg_residual_return: Some(excess),
            avg_excess_residual_return: Some(excess),
            top_bottom_spread: None,
            ic: None,
        }
    }

    #[test]
    fn rule_decay_validation_detects_recent_positive_rule_decay() {
        let points = (0..80)
            .map(|index| {
                let excess = if index < 60 {
                    0.20 + (index % 2) as f64 * 0.02
                } else {
                    -0.50 + (index % 2) as f64 * 0.02
                };
                decay_test_point(index, 1.0, excess)
            })
            .collect::<Vec<_>>();

        let validations = build_rule_decay_validations(&points);
        let recent_20 = validations
            .iter()
            .find(|item| item.window_days == 20)
            .expect("20-day validation");

        assert_eq!(recent_20.status, "significant_decay");
        assert_eq!(recent_20.recent_day_count, 20);
        assert_eq!(recent_20.prior_day_count, 60);
        assert!(
            recent_20
                .recent_directional_excess_mean
                .is_some_and(|value| value < 0.0)
        );
        assert!(recent_20.decay_change.is_some_and(|value| value < -0.6));
        assert!(recent_20.decay_t_value.is_some_and(|value| value < -2.0));
    }

    #[test]
    fn rule_decay_validation_normalizes_negative_rule_direction() {
        let points = (0..80)
            .map(|index| {
                let excess = if index < 60 {
                    -0.30 - (index % 2) as f64 * 0.02
                } else {
                    0.20 - (index % 2) as f64 * 0.02
                };
                decay_test_point(index, -1.0, excess)
            })
            .collect::<Vec<_>>();

        let validations = build_rule_decay_validations(&points);
        let recent_20 = validations
            .iter()
            .find(|item| item.window_days == 20)
            .expect("20-day validation");

        assert_eq!(recent_20.status, "significant_decay");
        assert!(
            recent_20
                .prior_directional_excess_mean
                .is_some_and(|value| value > 0.0)
        );
        assert!(
            recent_20
                .recent_directional_excess_mean
                .is_some_and(|value| value < 0.0)
        );
        assert!(recent_20.decay_change.is_some_and(|value| value < 0.0));
    }

    #[test]
    fn rule_decay_validation_marks_short_history_as_insufficient() {
        let points = (0..25)
            .map(|index| decay_test_point(index, 1.0, 0.10))
            .collect::<Vec<_>>();

        let validations = build_rule_decay_validations(&points);

        assert_eq!(validations.len(), 3);
        assert!(validations.iter().all(|item| item.status == "insufficient"));
        let recent_20 = validations
            .iter()
            .find(|item| item.window_days == 20)
            .expect("20-day validation");
        assert_eq!(recent_20.recent_day_count, 20);
        assert_eq!(recent_20.prior_day_count, 5);
        assert_eq!(recent_20.decay_change, None);
    }

    #[test]
    fn all_rule_basket_decay_averages_directional_strategy_days() {
        let first = (0..80)
            .map(|index| {
                let value = if index < 60 {
                    0.20 + (index % 2) as f64 * 0.02
                } else {
                    -0.30 + (index % 2) as f64 * 0.02
                };
                (format!("{index:08}"), value)
            })
            .collect::<Vec<_>>();
        let second = (0..80)
            .map(|index| {
                let value = if index < 60 {
                    0.40 + (index % 2) as f64 * 0.02
                } else {
                    -0.10 + (index % 2) as f64 * 0.02
                };
                (format!("{index:08}"), value)
            })
            .collect::<Vec<_>>();

        let validations =
            build_rule_basket_decay_from_daily_groups([first.as_slice(), second.as_slice()]);
        let recent_20 = validations
            .iter()
            .find(|item| item.window_days == 20)
            .expect("20-day basket validation");

        assert_eq!(recent_20.status, "significant_decay");
        assert_eq!(recent_20.recent_day_count, 20);
        assert_eq!(recent_20.prior_day_count, 60);
        assert!(
            recent_20
                .recent_directional_excess_mean
                .is_some_and(|value| value < -0.18)
        );
        assert!(recent_20.decay_change.is_some_and(|value| value < -0.49));
    }
}
