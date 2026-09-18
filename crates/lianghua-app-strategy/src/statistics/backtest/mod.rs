//! 分层回测的默认参数与对外运行入口（场景/规则/排名层）。

pub(super) mod rank_layer;
pub(super) mod summary;

use crate::statistics::backtest::rank_layer::{
    build_rank_layer_sample_groups, build_rank_market_value_summaries, rank_layer_label,
    rank_layer_method_label, rank_top_k_period_summary_data, rank_top_k_summary_data,
};
use crate::statistics::backtest::summary::{
    aggregate_all_rule_summary_metrics, build_all_rule_decay_validations,
    build_one_rule_backtest_summary_and_detail, build_one_rule_contribution_average,
    build_rule_contribution_averages, build_rule_contribution_averages_from_compact_rows,
    build_rule_decay_validations, filter_compact_rule_score_batch,
    filter_score_summary_rows_by_ts_codes, load_daily_max_rank, load_score_summary_rows_from_db,
    split_and_sort_rule_backtest_summaries_and_details, weighted_rule_summary_metric,
};
use crate::statistics::common::{load_rule_meta, open_result_conn};
use crate::statistics::detail_cache::RuleBacktestDetailCacheWriter;
use crate::statistics::page::load_scene_options;
use crate::statistics::universe::{
    build_backtest_stock_filter, load_validation_sample_stock_meta_map, ts_code_allowed_by_filter,
};
use crate::statistics::validation::similarity::{
    CompactRuleSimilarityCache, build_compact_rule_similarity_rows,
    empty_validation_similarity_cache, load_compact_rule_similarity_cache,
};
use crate::statistics::validation::{RuleValidationComboResult, RuleValidationSampleRow};

use crate::data::DataReader;
use crate::data::RuntimeKeyCollectOptions;
use crate::data::collect_runtime_keys_from_expr_programs;
use crate::data::source_db_path;
use crate::scoring::CachedRule;
use crate::scoring::rule_cache::cache_rule_build as build_scoring_rule_cache;
use crate::scoring::runner::ScoringMemoryMode;
use crate::scoring::runner::scoring_all_to_memory_with_mode;
use crate::scoring::tools::cyq_chen_runtime_key_names;
use crate::scoring_model::SceneBacktestRow;
use crate::simulate::DEFAULT_BACKTEST_MIN_LISTED_TRADE_DAYS;
use crate::simulate::rank::RankLayerConfig;
use crate::simulate::rank::RankLayerFromDbInput;
use crate::simulate::rank::RankLayerMethod;
use crate::simulate::rank::calc_rank_layer_metrics_from_score_rows;
use crate::simulate::rule::DEFAULT_RULE_WITH_SAMPLES_PARALLEL_BATCH_SIZE;
use crate::simulate::rule::RuleLayerConfig;
use crate::simulate::rule::RuleLayerFromDbInput;
use crate::simulate::rule::calc_all_rule_layer_metrics_with_validation_from_compact_rows_map;
use crate::simulate::rule::calc_all_rule_layer_metrics_with_validation_from_db_map_with_ts_filter;
use crate::simulate::rule::calc_rule_layer_metrics_from_db_with_ts_filter;
use crate::simulate::scene::SceneLayerConfig;
use crate::simulate::scene::SceneLayerFromDbInput;
use crate::simulate::scene::calc_all_scene_layer_metrics_from_db_with_ts_filter;
use crate::simulate::scene::calc_all_scene_layer_metrics_from_rows;
use crate::simulate::scene::calc_scene_layer_metrics_from_db_with_ts_filter;
use duckdb::Connection;
use serde::Serialize;
use std::collections::HashMap;
use std::collections::HashSet;
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
    #[serde(skip)]
    pub(in crate::statistics) portfolio_daily_values: Vec<RulePortfolioDailyValue>,
}

#[derive(Debug, Clone)]
pub(super) struct RulePortfolioDailyValue {
    pub(in crate::statistics) trade_date: String,
    pub(in crate::statistics) avg_residual_return: Option<f64>,
    pub(in crate::statistics) avg_excess_residual_return: Option<f64>,
    pub(in crate::statistics) top_bottom_spread: Option<f64>,
    pub(in crate::statistics) ic: Option<f64>,
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
pub struct RankTopKSummaryData {
    pub top_k: usize,
    pub point_count: usize,
    pub sample_count: usize,
    pub avg_daily_residual_return: Option<f64>,
    pub median_daily_residual_return: Option<f64>,
    pub positive_day_ratio: Option<f64>,
    pub daily_std: Option<f64>,
    pub hac_t_value: Option<f64>,
    pub hac_lag: usize,
}

#[derive(Debug, Serialize)]
pub struct RankTopKPeriodSummaryData {
    pub period_label: String,
    pub start_date: String,
    pub end_date: String,
    pub top_k: usize,
    pub point_count: usize,
    pub sample_count: usize,
    pub avg_daily_residual_return: Option<f64>,
    pub median_daily_residual_return: Option<f64>,
    pub positive_day_ratio: Option<f64>,
    pub hac_t_value: Option<f64>,
    pub hac_lag: usize,
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
    pub top_k_summaries: Vec<RankTopKSummaryData>,
    pub top_k_period_summaries: Vec<RankTopKPeriodSummaryData>,
    pub layer_summaries: Vec<RankLayerBucketSummary>,
    pub layer_sample_groups: Vec<RankLayerSampleGroup>,
    pub market_value_summaries: Vec<RankLayerMarketValueSummary>,
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

pub(super) const RULE_BACKTEST_EPS: f64 = 1e-12;
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

pub(super) fn query_score_summary_latest_trade_date(
    conn: &Connection,
) -> Result<Option<String>, String> {
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
pub(super) struct SceneLayerBacktestRunParams {
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
pub(super) struct RuleLayerBacktestRunParams {
    pub(super) stock_adj_type: String,
    pub(super) index_ts_code: String,
    pub(super) index_beta: f64,
    pub(super) concept_beta: f64,
    pub(super) industry_beta: f64,
    pub(super) start_date: String,
    pub(super) end_date: String,
    pub(super) min_samples_per_day: usize,
    pub(super) min_listed_trade_days: usize,
    pub(super) backtest_period: usize,
    pub(in crate::statistics) parallel_batch_size: usize,
    pub(in crate::statistics) resolved_board: Option<String>,
    pub(in crate::statistics) exclude_st_board: bool,
    pub(in crate::statistics) total_mv_min: Option<f64>,
    pub(in crate::statistics) total_mv_max: Option<f64>,
    pub(super) allowed_ts_codes: Option<HashSet<String>>,
}

#[derive(Debug, Clone)]
pub(super) struct RankLayerBacktestRunParams {
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

pub(super) fn validate_backtest_strategy_expressions(source_path: &str) -> Result<(), String> {
    let rules_cache = build_scoring_rule_cache(source_path, None)?;
    let programs = rules_cache
        .iter()
        .flat_map(CachedRule::expression_programs)
        .collect::<Vec<_>>();
    let cyq_chen_keys = cyq_chen_runtime_key_names();
    let injected_keys = (["ZHANG", "TOTAL_MV_YI", "S_RANK"])
        .iter()
        .copied()
        .chain(cyq_chen_keys)
        .collect::<Vec<_>>();
    let required_runtime_keys = collect_runtime_keys_from_expr_programs(
        &programs,
        RuntimeKeyCollectOptions {
            always_keys: &[],
            injected_keys: &injected_keys,
            aliases: &([]),
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
    (|source_conn: &Connection,
      source_path: &str,
      scene_name: Option<&str>,
      params: &SceneLayerBacktestRunParams|
     -> Result<SceneLayerBacktestData, String> {
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
    })(&source_conn, &source_path, None, &params)
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
    (|source_conn: &Connection,
      source_path: &str,
      rule_name: Option<&str>,
      params: &RuleLayerBacktestRunParams|
     -> Result<RuleLayerBacktestData, String> {
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
            let decay_validations =
                build_rule_decay_validations(&metrics.points, params.backtest_period);

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
            .map(|(name, meta)| (name.clone(), meta.explain.clone()))
            .collect::<HashMap<_, _>>();
        let stock_meta_map = load_validation_sample_stock_meta_map(source_path)?;
        let similarity_cache =
            load_compact_rule_similarity_cache(source_path, &params.start_date, &params.end_date)?;
        let (contribution_averages, daily_max_rank) = if params.allowed_ts_codes.is_none() {
            (
                build_rule_contribution_averages(
                    source_path,
                    &rule_options,
                    &params.start_date,
                    &params.end_date,
                )?,
                None,
            )
        } else {
            (
                HashMap::new(),
                Some(load_daily_max_rank(
                    source_path,
                    &params.start_date,
                    &params.end_date,
                    params.allowed_ts_codes.as_ref(),
                )?),
            )
        };
        let detail_cache = RuleBacktestDetailCacheWriter::new()?;
        let items = calc_all_rule_layer_metrics_with_validation_from_db_map_with_ts_filter(
            source_conn,
            source_path,
            &rule_options,
            &params.stock_adj_type,
            &params.index_ts_code,
            params.index_beta,
            params.concept_beta,
            params.industry_beta,
            &params.start_date,
            &params.end_date,
            &layer_config,
            params.allowed_ts_codes.as_ref(),
            params.parallel_batch_size,
            |one_rule_name, validation| {
                let contribution_average =
                    if let Some(average) = contribution_averages.get(one_rule_name) {
                        average.clone()
                    } else {
                        build_one_rule_contribution_average(
                            source_path,
                            one_rule_name,
                            &params.start_date,
                            &params.end_date,
                            params.allowed_ts_codes.as_ref(),
                            daily_max_rank
                                .as_ref()
                                .ok_or_else(|| "策略贡献度排名上限缺失".to_string())?,
                        )?
                    };
                let one_rule_contribution_averages =
                    HashMap::from([(one_rule_name.to_string(), contribution_average)]);
                let similarity_rows = build_compact_rule_similarity_rows(
                    &similarity_cache,
                    &validation.triggered_samples,
                    one_rule_name,
                    &explain_map,
                );
                let (summary, detail) = build_one_rule_backtest_summary_and_detail(
                    one_rule_name,
                    validation,
                    &rule_meta_map,
                    &one_rule_contribution_averages,
                    &explain_map,
                    params,
                    &empty_validation_similarity_cache(),
                    &stock_meta_map,
                );
                if let Some(mut detail) = detail {
                    detail.similarity_rows = similarity_rows;
                    detail_cache.write(one_rule_name, &detail)?;
                }
                Ok((summary, None))
            },
        )?;
        let (all_rule_summaries, _) = split_and_sort_rule_backtest_summaries_and_details(items);
        let decay_validations =
            build_all_rule_decay_validations(&all_rule_summaries, params.backtest_period);

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
        ) = aggregate_all_rule_summary_metrics(&all_rule_summaries, params.backtest_period);
        let result = RuleLayerBacktestData {
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
            avg_contribution_per_trigger: weighted_rule_summary_metric(
                &all_rule_summaries,
                |item| item.avg_contribution_per_trigger,
            ),
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
            rule_validation_details: Vec::new(),
        };
        detail_cache.commit(source_path)?;
        Ok(result)
    })(&source_conn, &source_path, None, &params)
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

    (|source_conn: &Connection,
      source_path: &str,
      params: &RankLayerBacktestRunParams|
     -> Result<RankLayerBacktestData, String> {
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
        let metrics = calc_rank_layer_metrics_from_score_rows(
            source_conn,
            source_path,
            &input,
            &summary_rows,
        )?;
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
            layer_method_label: rank_layer_method_label(input.layer_config.layer_method)
                .to_string(),
            point_count: metrics.point_count,
            sample_count: metrics.sample_count,
            avg_er_change: metrics.avg_er_change,
            spread_mean: metrics.spread_mean,
            ic_mean: metrics.ic_mean,
            ic_std: metrics.ic_std,
            icir: metrics.icir,
            ic_t_value: metrics.ic_t_value,
            top_k_summaries: rank_top_k_summary_data(metrics.top_k_summaries),
            top_k_period_summaries: rank_top_k_period_summary_data(metrics.top_k_period_summaries),
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
        })
    })(&source_conn, &source_path, &params)
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
    let scene_rows = (|rows: Vec<SceneBacktestRow>,
                       allowed_ts_codes: Option<&HashSet<String>>|
     -> Vec<SceneBacktestRow> {
        if allowed_ts_codes.is_none() {
            return rows;
        }
        rows.into_iter()
            .filter(|row| ts_code_allowed_by_filter(allowed_ts_codes, &row.ts_code))
            .collect()
    })(
        score_batch.scene_backtest_rows,
        params.allowed_ts_codes.as_ref(),
    );
    let scene_options = load_scene_options(&source_path)?;
    let all_metrics = calc_all_scene_layer_metrics_from_rows(
        &source_conn,
        &source_path,
        &scene_options,
        scene_rows,
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
    let (mut score_batch, scoring_profile) = scoring_all_to_memory_with_mode(
        &source_path,
        None,
        &params.stock_adj_type,
        &params.start_date,
        &params.end_date,
        ScoringMemoryMode::RuleBacktest,
    )?;
    filter_compact_rule_score_batch(&mut score_batch, params.allowed_ts_codes.as_ref());
    let summary_rows = score_batch.summary_rows;
    let compact_rule_rows = score_batch.compact_rule_rows;
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
        load_compact_rule_similarity_cache(&source_path, &params.start_date, &params.end_date)?
    } else {
        CompactRuleSimilarityCache {
            total_samples: 0.0,
            rule_names: Vec::new(),
            rule_hit_counts: Vec::new(),
            hits_by_pair: Vec::new(),
        }
    };
    let contribution_averages = build_rule_contribution_averages_from_compact_rows(
        &summary_rows,
        &compact_rule_rows,
        &scoring_profile.rule_names,
        &params.start_date,
        &params.end_date,
    );
    let detail_cache = RuleBacktestDetailCacheWriter::new()?;
    let summary_detail_items = calc_all_rule_layer_metrics_with_validation_from_compact_rows_map(
        &source_conn,
        &source_path,
        &rule_options,
        &scoring_profile.rule_names,
        &summary_rows,
        compact_rule_rows,
        &params.stock_adj_type,
        &params.index_ts_code,
        params.index_beta,
        params.concept_beta,
        params.industry_beta,
        &params.start_date,
        &params.end_date,
        &layer_config,
        params.parallel_batch_size,
        |one_rule_name, validation| {
            let similarity_rows = build_compact_rule_similarity_rows(
                &similarity_cache,
                &validation.triggered_samples,
                one_rule_name,
                &explain_map,
            );
            let (summary, detail) = build_one_rule_backtest_summary_and_detail(
                one_rule_name,
                validation,
                &rule_meta_map,
                &contribution_averages,
                &explain_map,
                &params,
                &empty_validation_similarity_cache(),
                &stock_meta_map,
            );
            if let Some(mut detail) = detail {
                detail.similarity_rows = similarity_rows;
                detail_cache.write(one_rule_name, &detail)?;
            }
            Ok((summary, None))
        },
    );
    let (all_rule_summaries, _) =
        split_and_sort_rule_backtest_summaries_and_details(summary_detail_items?);
    let decay_validations =
        build_all_rule_decay_validations(&all_rule_summaries, params.backtest_period);

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
    ) = aggregate_all_rule_summary_metrics(&all_rule_summaries, params.backtest_period);
    let result = RuleLayerBacktestData {
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
        rule_validation_details: Vec::new(),
    };
    detail_cache.commit(&source_path)?;
    Ok(result)
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
        top_k_summaries: rank_top_k_summary_data(metrics.top_k_summaries),
        top_k_period_summaries: rank_top_k_period_summary_data(metrics.top_k_period_summaries),
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
    })
}
