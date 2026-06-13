use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use duckdb::{Connection, params_from_iter};
use rayon::prelude::*;

use super::{
    BacktestSampleEligibility, DEFAULT_BACKTEST_MIN_LISTED_TRADE_DAYS, ResidualFactorSeriesRefs,
    ResidualReturnInput, build_backtest_sample_eligibility,
    calc_stock_residual_returns_from_loaded_series,
};
use crate::data::{
    concept_performance_data::{load_concept_trend_series, load_industry_trend_series},
    load_stock_list, load_ths_concepts_named_map, result_db_path,
    scoring_data::{ScoreDetails, ScoreSummary},
};

use crate::simulate::fp_utils::{
    EPS, ProfitLossSums, calc_profit_loss_sums, calc_t_value, calc_top_bottom_spread, mean,
    sample_std, spearman_corr,
};
const PCT_CHG_BATCH_SIZE: usize = 512;

#[derive(Debug, Clone)]
pub struct RuleLayerConfig {
    pub min_samples_per_day: usize,
    pub backtest_period: usize,
    pub min_listed_trade_days: usize,
}

impl Default for RuleLayerConfig {
    fn default() -> Self {
        Self {
            min_samples_per_day: 5,
            backtest_period: 1,
            min_listed_trade_days: DEFAULT_BACKTEST_MIN_LISTED_TRADE_DAYS,
        }
    }
}

impl RuleLayerConfig {
    fn validate(&self) -> Result<(), String> {
        if self.min_samples_per_day == 0 {
            return Err("每日最少样本数必须>=1".to_string());
        }
        if self.backtest_period == 0 {
            return Err("回测周期必须>=1".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RuleLayerFromDbInput {
    pub rule_name: String,
    pub stock_adj_type: String,
    pub index_ts_code: String,
    pub index_beta: f64,
    pub concept_beta: f64,
    pub industry_beta: f64,
    pub start_date: String,
    pub end_date: String,
    pub layer_config: RuleLayerConfig,
}

impl RuleLayerFromDbInput {
    fn validate(&self) -> Result<(), String> {
        if self.rule_name.trim().is_empty() {
            return Err("rule_name不能为空".to_string());
        }
        if self.stock_adj_type.trim().is_empty() {
            return Err("股票复权类型不能为空".to_string());
        }
        if self.index_ts_code.trim().is_empty() {
            return Err("指数代码不能为空".to_string());
        }
        if self.start_date.trim().is_empty() || self.end_date.trim().is_empty() {
            return Err("区间日期不能为空".to_string());
        }
        if self.start_date > self.end_date {
            return Err(format!(
                "区间日期非法:start_date({})大于end_date({})",
                self.start_date, self.end_date
            ));
        }
        if !self.index_beta.is_finite() {
            return Err("指数系数必须是有限数字".to_string());
        }
        if !self.concept_beta.is_finite() {
            return Err("概念系数必须是有限数字".to_string());
        }
        if !self.industry_beta.is_finite() {
            return Err("行业系数必须是有限数字".to_string());
        }
        self.layer_config.validate()
    }
}

#[derive(Debug, Clone)]
pub struct RuleSample {
    pub ts_code: String,
    pub trade_date: String,
    pub rule_score: f64,
    pub residual_return: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuleLayerPoint {
    pub trade_date: String,
    pub sample_count: usize,
    pub avg_rule_score: Option<f64>,
    pub avg_residual_return: Option<f64>,
    pub avg_excess_residual_return: Option<f64>,
    pub top_bottom_spread: Option<f64>,
    pub ic: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuleLayerMetrics {
    pub points: Vec<RuleLayerPoint>,
    pub avg_residual_mean: Option<f64>,
    pub avg_excess_residual_mean: Option<f64>,
    pub profit_loss_ratio: Option<f64>,
    pub spread_mean: Option<f64>,
    pub ic_mean: Option<f64>,
    pub ic_std: Option<f64>,
    pub icir: Option<f64>,
    pub ic_t_value: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuleLayerSamplePoint {
    pub ts_code: String,
    pub trade_date: String,
    pub rule_score: f64,
    pub residual_return: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuleLayerMetricsWithSamples {
    pub metrics: RuleLayerMetrics,
    pub samples: Vec<RuleLayerSamplePoint>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuleLayerMetricsWithTriggeredSamples {
    pub metrics: RuleLayerMetrics,
    pub triggered_samples: Vec<RuleLayerSamplePoint>,
}

#[derive(Debug, Clone)]
pub struct RuleLayerRuntimeCache {
    day_groups: Vec<RuleDayGroup>,
}

#[derive(Debug, Clone)]
struct RuleDbRow {
    rule_name: String,
    ts_code: String,
    trade_date: String,
    rule_score: f64,
}

#[derive(Debug, Clone)]
struct RuleUniverseRow {
    ts_code: String,
    trade_date: String,
}

#[derive(Debug, Clone)]
struct RuleDayBaseSample {
    ts_code: Arc<str>,
    residual_return: f64,
}

#[derive(Debug, Clone)]
struct RuleDayGroup {
    trade_date: Arc<str>,
    samples: Vec<RuleDayBaseSample>,
}

#[derive(Debug, Clone, Copy)]
struct RuleLayerCollectOptions {
    metrics: bool,
    all_samples: bool,
    triggered_samples: bool,
}

struct RuleLayerComputation {
    metrics: RuleLayerMetrics,
    all_samples: Vec<RuleLayerSamplePoint>,
    triggered_samples: Vec<RuleLayerSamplePoint>,
}

type TriggeredScoreMap = HashMap<String, HashMap<String, f64>>;

struct ResidualCacheInput<'a> {
    stock_adj_type: &'a str,
    index_ts_code: &'a str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &'a str,
    end_date: &'a str,
    backtest_period: usize,
    min_listed_trade_days: usize,
}

pub fn calc_rule_layer_metrics_from_db(
    source_conn: &Connection,
    source_dir: &str,
    input: &RuleLayerFromDbInput,
) -> Result<RuleLayerMetrics, String> {
    calc_rule_layer_metrics_from_db_with_ts_filter(source_conn, source_dir, input, None)
}

pub fn calc_rule_layer_metrics_from_db_with_ts_filter(
    source_conn: &Connection,
    source_dir: &str,
    input: &RuleLayerFromDbInput,
    allowed_ts_codes: Option<&HashSet<String>>,
) -> Result<RuleLayerMetrics, String> {
    input.validate()?;

    let runtime_cache = build_rule_layer_runtime_cache_with_ts_filter(
        source_conn,
        source_dir,
        &input.stock_adj_type,
        &input.index_ts_code,
        input.index_beta,
        input.concept_beta,
        input.industry_beta,
        &input.start_date,
        &input.end_date,
        &input.layer_config,
        allowed_ts_codes,
    )?;
    let rule_rows = load_rule_rows_filtered(source_dir, input, allowed_ts_codes)?;
    let triggered_score_map = build_triggered_score_map(rule_rows);

    calc_rule_layer_metrics_from_cache(&runtime_cache, &triggered_score_map, &input.layer_config)
}

pub fn calc_all_rule_layer_metrics_from_db(
    source_conn: &Connection,
    source_dir: &str,
    rule_names: &[String],
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &RuleLayerConfig,
) -> Result<Vec<(String, RuleLayerMetrics)>, String> {
    calc_all_rule_layer_metrics_from_db_with_ts_filter(
        source_conn,
        source_dir,
        rule_names,
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
        None,
    )
}

pub fn calc_all_rule_layer_metrics_from_db_with_ts_filter(
    source_conn: &Connection,
    source_dir: &str,
    rule_names: &[String],
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &RuleLayerConfig,
    allowed_ts_codes: Option<&HashSet<String>>,
) -> Result<Vec<(String, RuleLayerMetrics)>, String> {
    validate_rule_common_input(
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
    )?;

    if rule_names.is_empty() {
        return Ok(Vec::new());
    }

    let runtime_cache = build_rule_layer_runtime_cache_with_ts_filter(
        source_conn,
        source_dir,
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
        allowed_ts_codes,
    )?;
    let triggered_score_map_by_rule = load_triggered_score_maps_for_names_filtered(
        source_dir,
        rule_names,
        start_date,
        end_date,
        allowed_ts_codes,
    )?;

    let grouped_results: Vec<Result<(String, RuleLayerMetrics), String>> = rule_names
        .par_iter()
        .map(|rule_name| {
            let empty_triggered_score_map = TriggeredScoreMap::new();
            let triggered_score_map = triggered_score_map_by_rule
                .get(rule_name)
                .unwrap_or(&empty_triggered_score_map);
            let metrics = calc_rule_layer_metrics_from_cache(
                &runtime_cache,
                triggered_score_map,
                layer_config,
            )?;
            Ok((rule_name.clone(), metrics))
        })
        .collect();

    let mut out = Vec::with_capacity(grouped_results.len());
    for item in grouped_results {
        out.push(item?);
    }

    Ok(out)
}

pub fn calc_all_rule_layer_metrics_with_samples_from_db_with_ts_filter(
    source_conn: &Connection,
    source_dir: &str,
    rule_names: &[String],
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &RuleLayerConfig,
    allowed_ts_codes: Option<&HashSet<String>>,
) -> Result<Vec<(String, RuleLayerMetricsWithSamples)>, String> {
    calc_all_rule_layer_metrics_with_samples_from_db_map_with_ts_filter(
        source_conn,
        source_dir,
        rule_names,
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
        allowed_ts_codes,
        |rule_name, metrics| Ok((rule_name.to_string(), metrics)),
    )
}

pub fn calc_all_rule_layer_metrics_with_samples_from_db_map_with_ts_filter<T, F>(
    source_conn: &Connection,
    source_dir: &str,
    rule_names: &[String],
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &RuleLayerConfig,
    allowed_ts_codes: Option<&HashSet<String>>,
    map_result: F,
) -> Result<Vec<T>, String>
where
    T: Send,
    F: Fn(&str, RuleLayerMetricsWithSamples) -> Result<T, String> + Sync,
{
    validate_rule_common_input(
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
    )?;

    if rule_names.is_empty() {
        return Ok(Vec::new());
    }

    let runtime_cache = build_rule_layer_runtime_cache_with_ts_filter(
        source_conn,
        source_dir,
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
        allowed_ts_codes,
    )?;
    let triggered_score_map_by_rule = load_triggered_score_maps_for_names_filtered(
        source_dir,
        rule_names,
        start_date,
        end_date,
        allowed_ts_codes,
    )?;

    let grouped_results: Vec<Result<T, String>> = rule_names
        .par_iter()
        .map(|rule_name| {
            let empty_triggered_score_map = TriggeredScoreMap::new();
            let triggered_score_map = triggered_score_map_by_rule
                .get(rule_name)
                .unwrap_or(&empty_triggered_score_map);
            let metrics = calc_rule_layer_metrics_with_samples_from_cache(
                &runtime_cache,
                triggered_score_map,
                layer_config,
            )?;
            map_result(rule_name, metrics)
        })
        .collect();

    let mut out = Vec::with_capacity(grouped_results.len());
    for item in grouped_results {
        out.push(item?);
    }

    Ok(out)
}

pub fn calc_all_rule_layer_metrics_from_rows(
    source_conn: &Connection,
    source_dir: &str,
    rule_names: &[String],
    score_summary_rows: &[ScoreSummary],
    score_detail_rows: &[ScoreDetails],
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &RuleLayerConfig,
) -> Result<Vec<(String, RuleLayerMetrics)>, String> {
    validate_rule_common_input(
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
    )?;

    if rule_names.is_empty() {
        return Ok(Vec::new());
    }

    let runtime_cache = build_rule_layer_runtime_cache_from_summary_rows(
        source_conn,
        source_dir,
        score_summary_rows,
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
    )?;
    let triggered_score_map_by_rule = build_triggered_score_maps_from_detail_rows(
        rule_names,
        score_detail_rows,
        start_date,
        end_date,
    );

    let grouped_results: Vec<Result<(String, RuleLayerMetrics), String>> = rule_names
        .par_iter()
        .map(|rule_name| {
            let empty_triggered_score_map = TriggeredScoreMap::new();
            let triggered_score_map = triggered_score_map_by_rule
                .get(rule_name)
                .unwrap_or(&empty_triggered_score_map);
            let metrics = calc_rule_layer_metrics_from_cache(
                &runtime_cache,
                triggered_score_map,
                layer_config,
            )?;
            Ok((rule_name.clone(), metrics))
        })
        .collect();

    let mut out = Vec::with_capacity(grouped_results.len());
    for item in grouped_results {
        out.push(item?);
    }

    Ok(out)
}

pub fn calc_all_rule_layer_metrics_with_samples_from_rows(
    source_conn: &Connection,
    source_dir: &str,
    rule_names: &[String],
    score_summary_rows: &[ScoreSummary],
    score_detail_rows: &[ScoreDetails],
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &RuleLayerConfig,
) -> Result<Vec<(String, RuleLayerMetricsWithSamples)>, String> {
    calc_all_rule_layer_metrics_with_samples_from_rows_map(
        source_conn,
        source_dir,
        rule_names,
        score_summary_rows,
        score_detail_rows,
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
        |rule_name, metrics| Ok((rule_name.to_string(), metrics)),
    )
}

pub fn calc_all_rule_layer_metrics_with_samples_from_rows_map<T, F>(
    source_conn: &Connection,
    source_dir: &str,
    rule_names: &[String],
    score_summary_rows: &[ScoreSummary],
    score_detail_rows: &[ScoreDetails],
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &RuleLayerConfig,
    map_result: F,
) -> Result<Vec<T>, String>
where
    T: Send,
    F: Fn(&str, RuleLayerMetricsWithSamples) -> Result<T, String> + Sync,
{
    validate_rule_common_input(
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
    )?;

    if rule_names.is_empty() {
        return Ok(Vec::new());
    }

    let runtime_cache = build_rule_layer_runtime_cache_from_summary_rows(
        source_conn,
        source_dir,
        score_summary_rows,
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
    )?;
    let triggered_score_map_by_rule = build_triggered_score_maps_from_detail_rows(
        rule_names,
        score_detail_rows,
        start_date,
        end_date,
    );

    let grouped_results: Vec<Result<T, String>> = rule_names
        .par_iter()
        .map(|rule_name| {
            let empty_triggered_score_map = TriggeredScoreMap::new();
            let triggered_score_map = triggered_score_map_by_rule
                .get(rule_name)
                .unwrap_or(&empty_triggered_score_map);
            let metrics = calc_rule_layer_metrics_with_samples_from_cache(
                &runtime_cache,
                triggered_score_map,
                layer_config,
            )?;
            map_result(rule_name, metrics)
        })
        .collect();

    let mut out = Vec::with_capacity(grouped_results.len());
    for item in grouped_results {
        out.push(item?);
    }

    Ok(out)
}

pub fn build_rule_layer_runtime_cache(
    source_conn: &Connection,
    source_dir: &str,
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &RuleLayerConfig,
) -> Result<RuleLayerRuntimeCache, String> {
    build_rule_layer_runtime_cache_with_ts_filter(
        source_conn,
        source_dir,
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
        None,
    )
}

pub fn build_rule_layer_runtime_cache_with_ts_filter(
    source_conn: &Connection,
    source_dir: &str,
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &RuleLayerConfig,
    allowed_ts_codes: Option<&HashSet<String>>,
) -> Result<RuleLayerRuntimeCache, String> {
    validate_rule_common_input(
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
    )?;

    let universe_rows = filter_universe_rows_by_ts_codes(
        load_rule_universe_rows(source_dir, start_date, end_date)?,
        allowed_ts_codes,
    );
    if universe_rows.is_empty() {
        return Ok(RuleLayerRuntimeCache {
            day_groups: Vec::new(),
        });
    }

    let concept_map = load_most_related_concept_map(source_dir)?;
    let industry_map = load_stock_industry_map(source_dir)?;
    let mut unique_ts_codes: HashSet<&str> = HashSet::new();
    for row in &universe_rows {
        unique_ts_codes.insert(row.ts_code.as_str());
    }

    let residual_map_cache = build_residual_map_cache(
        source_conn,
        source_dir,
        unique_ts_codes
            .into_iter()
            .map(|ts_code| ts_code.to_string())
            .collect(),
        &concept_map,
        &industry_map,
        &ResidualCacheInput {
            stock_adj_type,
            index_ts_code,
            index_beta,
            concept_beta,
            industry_beta,
            start_date,
            end_date,
            backtest_period: layer_config.backtest_period,
            min_listed_trade_days: layer_config.min_listed_trade_days,
        },
    )?;
    let day_groups = build_rule_day_groups(universe_rows, &residual_map_cache);

    Ok(RuleLayerRuntimeCache { day_groups })
}

pub fn build_rule_layer_runtime_cache_from_stock_data(
    source_conn: &Connection,
    source_dir: &str,
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &RuleLayerConfig,
) -> Result<RuleLayerRuntimeCache, String> {
    build_rule_layer_runtime_cache_from_stock_data_with_ts_filter(
        source_conn,
        source_dir,
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
        None,
    )
}

pub fn build_rule_layer_runtime_cache_from_stock_data_with_ts_filter(
    source_conn: &Connection,
    source_dir: &str,
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &RuleLayerConfig,
    allowed_ts_codes: Option<&HashSet<String>>,
) -> Result<RuleLayerRuntimeCache, String> {
    validate_rule_common_input(
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
    )?;

    let universe_rows = filter_universe_rows_by_ts_codes(
        load_stock_data_universe_rows(source_conn, stock_adj_type, start_date, end_date)?,
        allowed_ts_codes,
    );
    build_rule_layer_runtime_cache_from_universe_rows(
        source_conn,
        source_dir,
        universe_rows,
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
    )
}

pub(crate) fn build_rule_layer_runtime_cache_from_summary_rows(
    source_conn: &Connection,
    source_dir: &str,
    score_summary_rows: &[ScoreSummary],
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &RuleLayerConfig,
) -> Result<RuleLayerRuntimeCache, String> {
    build_rule_layer_runtime_cache_from_summary_rows_with_ts_filter(
        source_conn,
        source_dir,
        score_summary_rows,
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
        None,
    )
}

pub(crate) fn build_rule_layer_runtime_cache_from_summary_rows_with_ts_filter(
    source_conn: &Connection,
    source_dir: &str,
    score_summary_rows: &[ScoreSummary],
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &RuleLayerConfig,
    allowed_ts_codes: Option<&HashSet<String>>,
) -> Result<RuleLayerRuntimeCache, String> {
    validate_rule_common_input(
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
    )?;

    let mut universe_rows = score_summary_rows
        .iter()
        .filter_map(|row| {
            if row.trade_date.as_str() < start_date
                || row.trade_date.as_str() > end_date
                || row.ts_code.trim().is_empty()
                || row.trade_date.trim().is_empty()
                || !ts_code_allowed(allowed_ts_codes, &row.ts_code)
            {
                return None;
            }
            Some(RuleUniverseRow {
                ts_code: row.ts_code.clone(),
                trade_date: row.trade_date.clone(),
            })
        })
        .collect::<Vec<_>>();
    universe_rows.sort_by(|left, right| {
        left.trade_date
            .cmp(&right.trade_date)
            .then_with(|| left.ts_code.cmp(&right.ts_code))
    });

    if universe_rows.is_empty() {
        return Ok(RuleLayerRuntimeCache {
            day_groups: Vec::new(),
        });
    }

    build_rule_layer_runtime_cache_from_universe_rows(
        source_conn,
        source_dir,
        universe_rows,
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
    )
}

fn build_rule_layer_runtime_cache_from_universe_rows(
    source_conn: &Connection,
    source_dir: &str,
    universe_rows: Vec<RuleUniverseRow>,
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &RuleLayerConfig,
) -> Result<RuleLayerRuntimeCache, String> {
    if universe_rows.is_empty() {
        return Ok(RuleLayerRuntimeCache {
            day_groups: Vec::new(),
        });
    }

    let concept_map = load_most_related_concept_map(source_dir)?;
    let industry_map = load_stock_industry_map(source_dir)?;
    let mut unique_ts_codes: HashSet<&str> = HashSet::new();
    for row in &universe_rows {
        unique_ts_codes.insert(row.ts_code.as_str());
    }

    let residual_map_cache = build_residual_map_cache(
        source_conn,
        source_dir,
        unique_ts_codes
            .into_iter()
            .map(|ts_code| ts_code.to_string())
            .collect(),
        &concept_map,
        &industry_map,
        &ResidualCacheInput {
            stock_adj_type,
            index_ts_code,
            index_beta,
            concept_beta,
            industry_beta,
            start_date,
            end_date,
            backtest_period: layer_config.backtest_period,
            min_listed_trade_days: layer_config.min_listed_trade_days,
        },
    )?;
    let day_groups = build_rule_day_groups(universe_rows, &residual_map_cache);

    Ok(RuleLayerRuntimeCache { day_groups })
}

pub fn calc_rule_layer_metrics_from_triggered_scores(
    source_conn: &Connection,
    source_dir: &str,
    triggered_score_map: &HashMap<String, HashMap<String, f64>>,
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &RuleLayerConfig,
) -> Result<RuleLayerMetrics, String> {
    let runtime_cache = build_rule_layer_runtime_cache(
        source_conn,
        source_dir,
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
    )?;

    calc_rule_layer_metrics_from_cache(&runtime_cache, triggered_score_map, layer_config)
}

pub fn calc_rule_layer_metrics_with_samples_from_triggered_scores(
    source_conn: &Connection,
    source_dir: &str,
    triggered_score_map: &HashMap<String, HashMap<String, f64>>,
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &RuleLayerConfig,
) -> Result<RuleLayerMetricsWithSamples, String> {
    let runtime_cache = build_rule_layer_runtime_cache(
        source_conn,
        source_dir,
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
    )?;

    calc_rule_layer_metrics_with_samples_from_cache(
        &runtime_cache,
        triggered_score_map,
        layer_config,
    )
}

pub fn calc_rule_layer_metrics_with_samples_from_cache(
    runtime_cache: &RuleLayerRuntimeCache,
    triggered_score_map: &HashMap<String, HashMap<String, f64>>,
    layer_config: &RuleLayerConfig,
) -> Result<RuleLayerMetricsWithSamples, String> {
    let computation = compute_rule_layer_from_day_groups(
        &runtime_cache.day_groups,
        Some(triggered_score_map),
        layer_config,
        RuleLayerCollectOptions {
            metrics: true,
            all_samples: true,
            triggered_samples: false,
        },
    )?;

    Ok(RuleLayerMetricsWithSamples {
        metrics: computation.metrics,
        samples: computation.all_samples,
    })
}

pub fn calc_rule_layer_metrics_with_triggered_samples_from_cache(
    runtime_cache: &RuleLayerRuntimeCache,
    triggered_score_map: &HashMap<String, HashMap<String, f64>>,
    layer_config: &RuleLayerConfig,
) -> Result<RuleLayerMetricsWithTriggeredSamples, String> {
    let computation = compute_rule_layer_from_day_groups(
        &runtime_cache.day_groups,
        Some(triggered_score_map),
        layer_config,
        RuleLayerCollectOptions {
            metrics: true,
            all_samples: false,
            triggered_samples: true,
        },
    )?;

    Ok(RuleLayerMetricsWithTriggeredSamples {
        metrics: computation.metrics,
        triggered_samples: computation.triggered_samples,
    })
}

pub fn calc_rule_layer_metrics_from_cache(
    runtime_cache: &RuleLayerRuntimeCache,
    triggered_score_map: &HashMap<String, HashMap<String, f64>>,
    layer_config: &RuleLayerConfig,
) -> Result<RuleLayerMetrics, String> {
    Ok(compute_rule_layer_from_day_groups(
        &runtime_cache.day_groups,
        Some(triggered_score_map),
        layer_config,
        RuleLayerCollectOptions {
            metrics: true,
            all_samples: false,
            triggered_samples: false,
        },
    )?
    .metrics)
}

#[derive(Debug, Clone, Copy)]
pub struct RuleLayerSamplePointRef<'a> {
    pub ts_code: &'a str,
    pub trade_date: &'a str,
    pub rule_score: f64,
    pub residual_return: f64,
}

pub fn visit_triggered_rule_samples_from_cache<F>(
    runtime_cache: &RuleLayerRuntimeCache,
    triggered_score_map: &HashMap<String, HashMap<String, f64>>,
    mut visit: F,
) -> Result<(), String>
where
    F: FnMut(RuleLayerSamplePointRef<'_>) -> Result<(), String>,
{
    for day_group in &runtime_cache.day_groups {
        for sample in &day_group.samples {
            let Some(rule_score) = triggered_score_map
                .get(sample.ts_code.as_ref())
                .and_then(|date_score| date_score.get(day_group.trade_date.as_ref()))
                .copied()
            else {
                continue;
            };

            visit(RuleLayerSamplePointRef {
                ts_code: &*sample.ts_code,
                trade_date: &*day_group.trade_date,
                rule_score,
                residual_return: sample.residual_return,
            })?;
        }
    }

    Ok(())
}

pub fn collect_triggered_rule_samples_from_cache(
    runtime_cache: &RuleLayerRuntimeCache,
    triggered_score_map: &HashMap<String, HashMap<String, f64>>,
) -> Vec<RuleLayerSamplePoint> {
    compute_rule_layer_from_day_groups(
        &runtime_cache.day_groups,
        Some(triggered_score_map),
        &RuleLayerConfig::default(),
        RuleLayerCollectOptions {
            metrics: false,
            all_samples: false,
            triggered_samples: true,
        },
    )
    .map(|computation| computation.triggered_samples)
    .unwrap_or_default()
}

pub fn collect_all_rule_samples_from_cache(
    runtime_cache: &RuleLayerRuntimeCache,
    triggered_score_map: &HashMap<String, HashMap<String, f64>>,
    layer_config: &RuleLayerConfig,
) -> Result<Vec<RuleLayerSamplePoint>, String> {
    Ok(compute_rule_layer_from_day_groups(
        &runtime_cache.day_groups,
        Some(triggered_score_map),
        layer_config,
        RuleLayerCollectOptions {
            metrics: false,
            all_samples: true,
            triggered_samples: false,
        },
    )?
    .all_samples)
}

pub fn calc_rule_layer_metrics(
    samples: &[RuleSample],
    config: &RuleLayerConfig,
) -> Result<RuleLayerMetrics, String> {
    config.validate()?;

    let mut grouped_by_day: BTreeMap<&str, Vec<&RuleSample>> = BTreeMap::new();
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

    let day_results = grouped_by_day
        .into_iter()
        .collect::<Vec<_>>()
        .into_par_iter()
        .map(|(trade_date, day_samples)| {
            if day_samples.len() < config.min_samples_per_day {
                return None;
            }

            let mut rule_scores = Vec::with_capacity(day_samples.len());
            let mut residuals = Vec::with_capacity(day_samples.len());

            for sample in day_samples {
                rule_scores.push(sample.rule_score);
                residuals.push(sample.residual_return);
            }

            let avg_rule_score = mean(&rule_scores);
            let avg_residual_return = mean(&residuals);
            let avg_excess_residual_return = avg_residual_return.map(|_| 0.0);
            let profit_loss_sums = calc_profit_loss_sums(&residuals);
            let top_bottom_spread = calc_top_bottom_spread(&rule_scores, &residuals);
            let ic = spearman_corr(&rule_scores, &residuals);

            Some((
                RuleLayerPoint {
                    trade_date: trade_date.to_string(),
                    sample_count: rule_scores.len(),
                    avg_rule_score,
                    avg_residual_return,
                    avg_excess_residual_return,
                    top_bottom_spread,
                    ic,
                },
                avg_residual_return,
                avg_excess_residual_return,
                profit_loss_sums,
                top_bottom_spread,
                ic,
            ))
        })
        .collect::<Vec<_>>();

    let mut points = Vec::with_capacity(day_results.len());
    let mut avg_residual_values = Vec::new();
    let mut avg_excess_residual_values = Vec::new();
    let mut profit_loss_sums = ProfitLossSums::default();
    let mut spread_values = Vec::new();
    let mut ic_values = Vec::new();

    for item in day_results.into_iter().flatten() {
        let (
            point,
            avg_residual_return,
            avg_excess_residual_return,
            day_profit_loss_sums,
            top_bottom_spread,
            ic,
        ) = item;
        if let Some(value) = avg_residual_return {
            avg_residual_values.push(value);
        }
        if let Some(value) = avg_excess_residual_return {
            avg_excess_residual_values.push(value);
        }
        profit_loss_sums.merge(day_profit_loss_sums);
        if let Some(spread) = top_bottom_spread {
            spread_values.push(spread);
        }
        if let Some(value) = ic {
            ic_values.push(value);
        }
        points.push(point);
    }

    let avg_residual_mean = mean(&avg_residual_values);
    let avg_excess_residual_mean = mean(&avg_excess_residual_values);
    let profit_loss_ratio = profit_loss_sums.ratio();
    let spread_mean = mean(&spread_values);
    let ic_mean = mean(&ic_values);
    let ic_std = sample_std(&ic_values);
    let icir = match (ic_mean, ic_std) {
        (Some(m), Some(s)) if s.abs() >= EPS => Some(m / s),
        _ => None,
    };
    let ic_t_value = calc_t_value(ic_mean, ic_std, ic_values.len());

    Ok(RuleLayerMetrics {
        points,
        avg_residual_mean,
        avg_excess_residual_mean,
        profit_loss_ratio,
        spread_mean,
        ic_mean,
        ic_std,
        icir,
        ic_t_value,
    })
}

fn compute_rule_layer_from_day_groups(
    day_groups: &[RuleDayGroup],
    triggered_score_map: Option<&TriggeredScoreMap>,
    config: &RuleLayerConfig,
    collect_options: RuleLayerCollectOptions,
) -> Result<RuleLayerComputation, String> {
    config.validate()?;

    if day_groups.is_empty() {
        return Ok(RuleLayerComputation {
            metrics: empty_metrics(),
            all_samples: Vec::new(),
            triggered_samples: Vec::new(),
        });
    }

    // 使用 fold+reduce 消除中间 day_results Vec，避免全部交易日的
    // all_samples / triggered_samples 同时占据内存。
    let identity = || DayGroupsFoldAccum::default();
    let accum = day_groups
        .par_iter()
        .fold(identity, |mut acc, day_group| {
            acc.process_day_group(day_group, triggered_score_map, config, collect_options);
            acc
        })
        .reduce(identity, |mut a, b| {
            a.merge(b);
            a
        });

    let avg_residual_mean = mean(&accum.avg_residual_values);
    let avg_excess_residual_mean = mean(&accum.avg_excess_residual_values);
    let profit_loss_ratio = accum.profit_loss_sums.ratio();
    let spread_mean = mean(&accum.spread_values);
    let ic_mean = mean(&accum.ic_values);
    let ic_std = sample_std(&accum.ic_values);
    let icir = match (ic_mean, ic_std) {
        (Some(m), Some(s)) if s.abs() >= EPS => Some(m / s),
        _ => None,
    };
    let ic_t_value = calc_t_value(ic_mean, ic_std, accum.ic_values.len());

    Ok(RuleLayerComputation {
        metrics: RuleLayerMetrics {
            points: accum.points,
            avg_residual_mean,
            avg_excess_residual_mean,
            profit_loss_ratio,
            spread_mean,
            ic_mean,
            ic_std,
            icir,
            ic_t_value,
        },
        all_samples: accum.all_samples,
        triggered_samples: accum.triggered_samples,
    })
}

/// 线程本地累加器，用于 fold+reduce 模式逐个交易日处理而不物化中间 Vec。
#[derive(Debug, Default)]
struct DayGroupsFoldAccum {
    points: Vec<RuleLayerPoint>,
    avg_residual_values: Vec<f64>,
    avg_excess_residual_values: Vec<f64>,
    profit_loss_sums: ProfitLossSums,
    spread_values: Vec<f64>,
    ic_values: Vec<f64>,
    all_samples: Vec<RuleLayerSamplePoint>,
    triggered_samples: Vec<RuleLayerSamplePoint>,
}

impl DayGroupsFoldAccum {
    fn process_day_group(
        &mut self,
        day_group: &RuleDayGroup,
        triggered_score_map: Option<&TriggeredScoreMap>,
        config: &RuleLayerConfig,
        collect_options: RuleLayerCollectOptions,
    ) {
        let collect_metrics = collect_options.metrics;
        let cap = day_group.samples.len();
        let mut rule_scores: Vec<f64> = if collect_metrics {
            Vec::with_capacity(cap)
        } else {
            Vec::new()
        };
        let mut residuals: Vec<f64> = if collect_metrics {
            Vec::with_capacity(cap)
        } else {
            Vec::new()
        };
        let mut triggered_residuals: Vec<f64> = if collect_metrics {
            Vec::with_capacity(cap)
        } else {
            Vec::new()
        };

        for sample in &day_group.samples {
            let triggered_score = triggered_score_map
                .and_then(|score_map| score_map.get(sample.ts_code.as_ref()))
                .and_then(|date_score| date_score.get(day_group.trade_date.as_ref()))
                .copied();
            let rule_score = triggered_score.unwrap_or(0.0);

            if collect_metrics {
                rule_scores.push(rule_score);
                residuals.push(sample.residual_return);
            }

            if let Some(rule_score) = triggered_score {
                if collect_metrics {
                    triggered_residuals.push(sample.residual_return);
                }

                if collect_options.triggered_samples {
                    self.triggered_samples.push(RuleLayerSamplePoint {
                        ts_code: String::from(&*sample.ts_code),
                        trade_date: String::from(&*day_group.trade_date),
                        rule_score,
                        residual_return: sample.residual_return,
                    });
                }
            }

            if collect_options.all_samples {
                self.all_samples.push(RuleLayerSamplePoint {
                    ts_code: String::from(&*sample.ts_code),
                    trade_date: String::from(&*day_group.trade_date),
                    rule_score,
                    residual_return: sample.residual_return,
                });
            }
        }

        if collect_metrics && rule_scores.len() >= config.min_samples_per_day {
            let avg_rule_score = mean(&rule_scores);
            let avg_residual_return = mean(&triggered_residuals);
            let market_avg_residual_return = mean(&residuals);
            let avg_excess_residual_return = match (avg_residual_return, market_avg_residual_return)
            {
                (Some(triggered_avg), Some(market_avg)) => Some(triggered_avg - market_avg),
                _ => None,
            };
            let day_profit_loss_sums = calc_profit_loss_sums(&triggered_residuals);
            let top_bottom_spread = calc_top_bottom_spread(&rule_scores, &residuals);
            let ic = spearman_corr(&rule_scores, &residuals);

            if let Some(value) = avg_residual_return {
                self.avg_residual_values.push(value);
            }
            if let Some(value) = avg_excess_residual_return {
                self.avg_excess_residual_values.push(value);
            }
            self.profit_loss_sums.merge(day_profit_loss_sums);
            if let Some(spread) = top_bottom_spread {
                self.spread_values.push(spread);
            }
            if let Some(value) = ic {
                self.ic_values.push(value);
            }
            self.points.push(RuleLayerPoint {
                trade_date: String::from(&*day_group.trade_date),
                sample_count: rule_scores.len(),
                avg_rule_score,
                avg_residual_return,
                avg_excess_residual_return,
                top_bottom_spread,
                ic,
            });
        }
    }

    fn merge(&mut self, other: DayGroupsFoldAccum) {
        self.points.extend(other.points);
        self.avg_residual_values.extend(other.avg_residual_values);
        self.avg_excess_residual_values
            .extend(other.avg_excess_residual_values);
        self.profit_loss_sums.merge(other.profit_loss_sums);
        self.spread_values.extend(other.spread_values);
        self.ic_values.extend(other.ic_values);
        self.all_samples.extend(other.all_samples);
        self.triggered_samples.extend(other.triggered_samples);
    }
}

fn build_rule_day_groups(
    universe_rows: Vec<RuleUniverseRow>,
    residual_map_cache: &HashMap<String, HashMap<String, f64>>,
) -> Vec<RuleDayGroup> {
    let mut day_groups: Vec<RuleDayGroup> = Vec::new();
    let mut current_trade_date: Arc<str> = Arc::from("");
    let mut current_samples: Vec<RuleDayBaseSample> = Vec::new();

    for row in universe_rows {
        let Some(residual_map) = residual_map_cache.get(&row.ts_code) else {
            continue;
        };
        let Some(residual_return) = residual_map.get(&row.trade_date).copied() else {
            continue;
        };

        if current_trade_date.as_ref() != row.trade_date.as_str() {
            if !current_trade_date.as_ref().is_empty() {
                current_samples.shrink_to_fit();
                day_groups.push(RuleDayGroup {
                    trade_date: current_trade_date,
                    samples: std::mem::take(&mut current_samples),
                });
            }
            current_trade_date = Arc::from(row.trade_date.as_str());
        }

        current_samples.push(RuleDayBaseSample {
            ts_code: Arc::from(row.ts_code.as_str()),
            residual_return,
        });
    }

    if !current_trade_date.as_ref().is_empty() {
        current_samples.shrink_to_fit();
        day_groups.push(RuleDayGroup {
            trade_date: current_trade_date,
            samples: current_samples,
        });
    }

    day_groups.shrink_to_fit();
    day_groups
}

fn empty_metrics() -> RuleLayerMetrics {
    RuleLayerMetrics {
        points: Vec::new(),
        avg_residual_mean: None,
        avg_excess_residual_mean: None,
        profit_loss_ratio: None,
        spread_mean: None,
        ic_mean: None,
        ic_std: None,
        icir: None,
        ic_t_value: None,
    }
}

fn validate_rule_common_input(
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &RuleLayerConfig,
) -> Result<(), String> {
    if stock_adj_type.trim().is_empty() {
        return Err("股票复权类型不能为空".to_string());
    }
    if index_ts_code.trim().is_empty() {
        return Err("指数代码不能为空".to_string());
    }
    if start_date.trim().is_empty() || end_date.trim().is_empty() {
        return Err("区间日期不能为空".to_string());
    }
    if start_date > end_date {
        return Err(format!(
            "区间日期非法:start_date({})大于end_date({})",
            start_date, end_date
        ));
    }
    if !index_beta.is_finite() {
        return Err("指数系数必须是有限数字".to_string());
    }
    if !concept_beta.is_finite() {
        return Err("概念系数必须是有限数字".to_string());
    }
    if !industry_beta.is_finite() {
        return Err("行业系数必须是有限数字".to_string());
    }
    layer_config.validate()
}

fn build_triggered_score_map(rule_rows: Vec<RuleDbRow>) -> TriggeredScoreMap {
    let mut rows_by_ts: TriggeredScoreMap = HashMap::new();
    for RuleDbRow {
        ts_code,
        trade_date,
        rule_score,
        ..
    } in rule_rows
    {
        rows_by_ts
            .entry(ts_code)
            .or_default()
            .insert(trade_date, rule_score);
    }
    rows_by_ts
}

fn load_triggered_score_maps_for_names_filtered(
    source_dir: &str,
    rule_names: &[String],
    start_date: &str,
    end_date: &str,
    allowed_ts_codes: Option<&HashSet<String>>,
) -> Result<HashMap<String, TriggeredScoreMap>, String> {
    let rule_rows = load_rule_rows_for_names_filtered(
        source_dir,
        rule_names,
        start_date,
        end_date,
        allowed_ts_codes,
    )?;
    let mut rows_by_rule: HashMap<String, TriggeredScoreMap> = HashMap::new();

    for row in rule_rows {
        rows_by_rule
            .entry(row.rule_name)
            .or_default()
            .entry(row.ts_code)
            .or_default()
            .insert(row.trade_date, row.rule_score);
    }

    Ok(rows_by_rule)
}

fn build_triggered_score_maps_from_detail_rows(
    rule_names: &[String],
    score_detail_rows: &[ScoreDetails],
    start_date: &str,
    end_date: &str,
) -> HashMap<String, TriggeredScoreMap> {
    let rule_name_set: HashSet<&str> = rule_names.iter().map(String::as_str).collect();
    let mut rows_by_rule: HashMap<String, TriggeredScoreMap> = HashMap::new();

    for row in score_detail_rows {
        if !rule_name_set.contains(row.rule_name.as_str())
            || row.trade_date.as_str() < start_date
            || row.trade_date.as_str() > end_date
            || !row.rule_score.is_finite()
            || row.ts_code.trim().is_empty()
            || row.trade_date.trim().is_empty()
        {
            continue;
        }

        rows_by_rule
            .entry(row.rule_name.clone())
            .or_default()
            .entry(row.ts_code.clone())
            .or_default()
            .insert(row.trade_date.clone(), row.rule_score);
    }

    rows_by_rule
}

fn ts_code_allowed(allowed_ts_codes: Option<&HashSet<String>>, ts_code: &str) -> bool {
    let Some(allowed_ts_codes) = allowed_ts_codes else {
        return true;
    };
    let normalized = ts_code.trim().to_ascii_uppercase();
    allowed_ts_codes.contains(normalized.as_str())
}

fn filter_universe_rows_by_ts_codes(
    universe_rows: Vec<RuleUniverseRow>,
    allowed_ts_codes: Option<&HashSet<String>>,
) -> Vec<RuleUniverseRow> {
    if allowed_ts_codes.is_none() {
        return universe_rows;
    }
    universe_rows
        .into_iter()
        .filter(|row| ts_code_allowed(allowed_ts_codes, &row.ts_code))
        .collect()
}

fn load_rule_universe_rows(
    source_dir: &str,
    start_date: &str,
    end_date: &str,
) -> Result<Vec<RuleUniverseRow>, String> {
    let result_db = result_db_path(source_dir);
    if !result_db.exists() {
        return Ok(Vec::new());
    }

    let result_db_str = result_db
        .to_str()
        .ok_or_else(|| "result_db路径不是有效UTF-8".to_string())?;
    let conn =
        Connection::open(result_db_str).map_err(|e| format!("打开scoring_result.db失败:{e}"))?;

    let mut stmt = conn
        .prepare(
            r#"
            SELECT
                ts_code,
                trade_date
            FROM score_summary
            WHERE trade_date >= ?
              AND trade_date <= ?
            ORDER BY trade_date ASC, ts_code ASC
            "#,
        )
        .map_err(|e| format!("预编译score_summary查询失败:{e}"))?;

    let mut rows = stmt
        .query(params_from_iter([start_date.trim(), end_date.trim()]))
        .map_err(|e| format!("查询score_summary失败:{e}"))?;

    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取score_summary失败:{e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取ts_code失败:{e}"))?;
        let trade_date: String = row.get(1).map_err(|e| format!("读取trade_date失败:{e}"))?;

        if ts_code.trim().is_empty() || trade_date.trim().is_empty() {
            continue;
        }

        out.push(RuleUniverseRow {
            ts_code,
            trade_date,
        });
    }

    Ok(out)
}

fn load_stock_data_universe_rows(
    source_conn: &Connection,
    stock_adj_type: &str,
    start_date: &str,
    end_date: &str,
) -> Result<Vec<RuleUniverseRow>, String> {
    let mut stmt = source_conn
        .prepare(
            r#"
            SELECT DISTINCT
                ts_code,
                trade_date
            FROM stock_data
            WHERE adj_type = ?
              AND trade_date >= ?
              AND trade_date <= ?
            ORDER BY trade_date ASC, ts_code ASC
            "#,
        )
        .map_err(|e| format!("预编译stock_data样本范围查询失败:{e}"))?;

    let mut rows = stmt
        .query(params_from_iter([
            stock_adj_type.trim(),
            start_date.trim(),
            end_date.trim(),
        ]))
        .map_err(|e| format!("查询stock_data样本范围失败:{e}"))?;

    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取stock_data样本范围失败:{e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取ts_code失败:{e}"))?;
        let trade_date: String = row.get(1).map_err(|e| format!("读取trade_date失败:{e}"))?;

        if ts_code.trim().is_empty() || trade_date.trim().is_empty() {
            continue;
        }

        out.push(RuleUniverseRow {
            ts_code,
            trade_date,
        });
    }

    Ok(out)
}

fn build_residual_map_cache(
    source_conn: &Connection,
    source_dir: &str,
    ts_codes: Vec<String>,
    concept_map: &HashMap<String, String>,
    industry_map: &HashMap<String, String>,
    input: &ResidualCacheInput<'_>,
) -> Result<HashMap<String, HashMap<String, f64>>, String> {
    if ts_codes.is_empty() {
        return Ok(HashMap::new());
    }
    let sample_eligibility =
        build_backtest_sample_eligibility(source_dir, input.min_listed_trade_days)?;

    let mut concept_series_cache = build_concept_series_cache(
        source_dir,
        &ts_codes,
        concept_map,
        input.start_date,
        input.end_date,
        input.concept_beta.abs() > EPS,
    )?;
    concept_series_cache.shrink_to_fit();
    let mut industry_series_cache = build_industry_series_cache(
        source_dir,
        &ts_codes,
        industry_map,
        input.start_date,
        input.end_date,
        input.industry_beta.abs() > EPS,
    )?;
    industry_series_cache.shrink_to_fit();
    let mut stock_series_cache = load_pct_chg_series_cache_for_ts_codes(
        source_conn,
        &ts_codes,
        input.stock_adj_type,
        input.start_date,
        input.end_date,
    )?;
    shrink_stock_series_cache(&mut stock_series_cache);
    let index_series = load_pct_chg_series_cache_for_ts_codes(
        source_conn,
        &[input.index_ts_code.to_string()],
        "ind",
        input.start_date,
        input.end_date,
    )?
    .remove(input.index_ts_code)
    .unwrap_or_default();

    let grouped_results: Vec<Result<(String, HashMap<String, f64>), String>> = ts_codes
        .into_par_iter()
        .map(|ts_code| {
            let residual_map = build_residual_map_for_ts_code(
                &ts_code,
                &stock_series_cache,
                &index_series,
                concept_map,
                industry_map,
                &concept_series_cache,
                &industry_series_cache,
                input,
                &sample_eligibility,
            )?;
            Ok((ts_code, residual_map))
        })
        .collect();

    // 中间缓存数据已完成使命，显式释放以降低内存峰值
    drop(stock_series_cache);
    drop(concept_series_cache);
    drop(industry_series_cache);
    drop(index_series);

    let mut out = HashMap::with_capacity(grouped_results.len());
    for item in grouped_results {
        let (ts_code, residual_map) = item?;
        out.insert(ts_code, residual_map);
    }
    out.shrink_to_fit();
    Ok(out)
}

fn build_residual_map_for_ts_code(
    ts_code: &str,
    stock_series_cache: &HashMap<String, HashMap<String, f64>>,
    index_series: &HashMap<String, f64>,
    concept_map: &HashMap<String, String>,
    industry_map: &HashMap<String, String>,
    concept_series_cache: &HashMap<String, HashMap<String, f64>>,
    industry_series_cache: &HashMap<String, HashMap<String, f64>>,
    input: &ResidualCacheInput<'_>,
    sample_eligibility: &BacktestSampleEligibility,
) -> Result<HashMap<String, f64>, String> {
    let Some(stock_series) = stock_series_cache.get(ts_code) else {
        return Ok(HashMap::new());
    };

    let most_related_concept = concept_map.get(ts_code).cloned().unwrap_or_default();
    let industry = industry_map.get(ts_code).cloned().unwrap_or_default();
    let concept_series = if most_related_concept.trim().is_empty() {
        None
    } else {
        concept_series_cache.get(most_related_concept.trim())
    };
    let industry_series = if industry.trim().is_empty() {
        None
    } else {
        industry_series_cache.get(industry.trim())
    };

    let residual_points = calc_stock_residual_returns_from_loaded_series(
        &ResidualReturnInput {
            ts_code: ts_code.to_string(),
            stock_adj_type: input.stock_adj_type.to_string(),
            index_ts_code: input.index_ts_code.to_string(),
            concept: most_related_concept,
            industry,
            index_beta: input.index_beta,
            concept_beta: input.concept_beta,
            industry_beta: input.industry_beta,
            start_date: input.start_date.to_string(),
            end_date: input.end_date.to_string(),
        },
        stock_series,
        index_series,
        ResidualFactorSeriesRefs {
            concept_series,
            industry_series,
        },
    )?;

    let mut residual_map =
        build_forward_backtest_residual_map(residual_points, input.backtest_period);
    residual_map.retain(|trade_date, _| sample_eligibility.allows_sample(ts_code, trade_date));
    residual_map.shrink_to_fit();
    Ok(residual_map)
}

fn shrink_stock_series_cache(cache: &mut HashMap<String, HashMap<String, f64>>) {
    for series in cache.values_mut() {
        series.shrink_to_fit();
    }
    cache.shrink_to_fit();
}

fn load_pct_chg_series_cache_for_ts_codes(
    conn: &Connection,
    ts_codes: &[String],
    adj_type: &str,
    start_date: &str,
    end_date: &str,
) -> Result<HashMap<String, HashMap<String, f64>>, String> {
    if ts_codes.is_empty() {
        return Ok(HashMap::new());
    }

    let mut out = HashMap::<String, HashMap<String, f64>>::with_capacity(ts_codes.len());
    for chunk in ts_codes.chunks(PCT_CHG_BATCH_SIZE) {
        let placeholders = std::iter::repeat_n("?", chunk.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            r#"
            SELECT
                ts_code,
                trade_date,
                TRY_CAST(pct_chg AS DOUBLE)
            FROM stock_data
            WHERE adj_type = ?
              AND ts_code IN ({placeholders})
              AND trade_date >= ?
              AND trade_date <= ?
            ORDER BY ts_code ASC, trade_date ASC
            "#
        );
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| format!("预编译批量涨跌幅查询失败:{e}"))?;
        let query_params = std::iter::once(adj_type.trim())
            .chain(chunk.iter().map(|ts_code| ts_code.trim()))
            .chain(std::iter::once(start_date.trim()))
            .chain(std::iter::once(end_date.trim()));
        let mut rows = stmt
            .query(params_from_iter(query_params))
            .map_err(|e| format!("查询批量涨跌幅失败:{e}"))?;

        while let Some(row) = rows.next().map_err(|e| format!("读取批量涨跌幅失败:{e}"))? {
            let ts_code: String = row.get(0).map_err(|e| format!("读取ts_code失败:{e}"))?;
            let trade_date: String = row.get(1).map_err(|e| format!("读取trade_date失败:{e}"))?;
            let pct: Option<f64> = row.get(2).map_err(|e| format!("读取pct_chg失败:{e}"))?;

            let Some(pct) = pct.filter(|value| value.is_finite()) else {
                continue;
            };
            out.entry(ts_code).or_default().insert(trade_date, pct);
        }
    }

    Ok(out)
}

fn build_concept_series_cache(
    source_dir: &str,
    ts_codes: &[String],
    concept_map: &HashMap<String, String>,
    start_date: &str,
    end_date: &str,
    enabled: bool,
) -> Result<HashMap<String, HashMap<String, f64>>, String> {
    if !enabled {
        return Ok(HashMap::new());
    }

    let mut names = HashSet::new();
    for ts_code in ts_codes {
        if let Some(name) = concept_map
            .get(ts_code)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
        {
            names.insert(name.to_string());
        }
    }

    let mut out = HashMap::with_capacity(names.len());
    for name in names {
        let series = load_concept_trend_series(
            source_dir,
            &name,
            Some(start_date.trim()),
            Some(end_date.trim()),
        )?;
        out.insert(
            name,
            series
                .points
                .into_iter()
                .map(|point| (point.trade_date, point.performance_pct))
                .collect(),
        );
    }
    Ok(out)
}

fn build_industry_series_cache(
    source_dir: &str,
    ts_codes: &[String],
    industry_map: &HashMap<String, String>,
    start_date: &str,
    end_date: &str,
    enabled: bool,
) -> Result<HashMap<String, HashMap<String, f64>>, String> {
    if !enabled {
        return Ok(HashMap::new());
    }

    let mut names = HashSet::new();
    for ts_code in ts_codes {
        if let Some(name) = industry_map
            .get(ts_code)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
        {
            names.insert(name.to_string());
        }
    }

    let mut out = HashMap::with_capacity(names.len());
    for name in names {
        let series = load_industry_trend_series(
            source_dir,
            &name,
            Some(start_date.trim()),
            Some(end_date.trim()),
        )?;
        out.insert(
            name,
            series
                .points
                .into_iter()
                .map(|point| (point.trade_date, point.performance_pct))
                .collect(),
        );
    }
    Ok(out)
}

fn build_forward_backtest_residual_map(
    mut residual_points: Vec<super::ResidualReturnPoint>,
    backtest_period: usize,
) -> HashMap<String, f64> {
    if backtest_period == 0 || residual_points.len() < backtest_period + 1 {
        return HashMap::new();
    }

    residual_points.sort_by(|a, b| a.trade_date.cmp(&b.trade_date));

    let mut out = HashMap::with_capacity(residual_points.len() - backtest_period);
    for i in 0..residual_points.len() {
        let end = i + backtest_period;
        if end >= residual_points.len() {
            break;
        }

        let mut sum = 0.0_f64;
        let mut valid = true;
        for point in residual_points.iter().take(end + 1).skip(i + 1) {
            let v = point.residual_pct;
            if !v.is_finite() {
                valid = false;
                break;
            }
            sum += v;
        }

        if valid {
            out.insert(residual_points[i].trade_date.clone(), sum);
        }
    }

    out
}

fn load_rule_rows_filtered(
    source_dir: &str,
    input: &RuleLayerFromDbInput,
    allowed_ts_codes: Option<&HashSet<String>>,
) -> Result<Vec<RuleDbRow>, String> {
    load_rule_rows_for_names(
        source_dir,
        std::slice::from_ref(&input.rule_name),
        &input.start_date,
        &input.end_date,
    )
    .map(|rows| {
        if allowed_ts_codes.is_none() {
            return rows;
        }
        rows.into_iter()
            .filter(|row| ts_code_allowed(allowed_ts_codes, &row.ts_code))
            .collect()
    })
}

fn load_rule_rows_for_names(
    source_dir: &str,
    rule_names: &[String],
    start_date: &str,
    end_date: &str,
) -> Result<Vec<RuleDbRow>, String> {
    load_rule_rows_for_names_filtered(source_dir, rule_names, start_date, end_date, None)
}

fn load_rule_rows_for_names_filtered(
    source_dir: &str,
    rule_names: &[String],
    start_date: &str,
    end_date: &str,
    allowed_ts_codes: Option<&HashSet<String>>,
) -> Result<Vec<RuleDbRow>, String> {
    let mut rows =
        load_rule_rows_for_names_unfiltered(source_dir, rule_names, start_date, end_date)?;
    if allowed_ts_codes.is_some() {
        rows.retain(|row| ts_code_allowed(allowed_ts_codes, &row.ts_code));
    }
    Ok(rows)
}

fn load_rule_rows_for_names_unfiltered(
    source_dir: &str,
    rule_names: &[String],
    start_date: &str,
    end_date: &str,
) -> Result<Vec<RuleDbRow>, String> {
    if rule_names.is_empty() {
        return Ok(Vec::new());
    }

    let result_db = result_db_path(source_dir);
    if !result_db.exists() {
        return Ok(Vec::new());
    }

    let result_db_str = result_db
        .to_str()
        .ok_or_else(|| "result_db路径不是有效UTF-8".to_string())?;
    let conn =
        Connection::open(result_db_str).map_err(|e| format!("打开scoring_result.db失败:{e}"))?;

    let placeholders = std::iter::repeat_n("?", rule_names.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
        SELECT
            rule_name,
            ts_code,
            trade_date,
            TRY_CAST(rule_score AS DOUBLE)
        FROM rule_details
        WHERE rule_name IN ({placeholders})
          AND trade_date >= ?
          AND trade_date <= ?
          AND TRY_CAST(rule_score AS DOUBLE) IS NOT NULL
        ORDER BY rule_name ASC, trade_date ASC, ts_code ASC
        "#
    );

    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("预编译rule_details查询失败:{e}"))?;
    let query_params = rule_names
        .iter()
        .map(|name| name.trim())
        .chain(std::iter::once(start_date.trim()))
        .chain(std::iter::once(end_date.trim()));
    let mut rows = stmt
        .query(params_from_iter(query_params))
        .map_err(|e| format!("查询rule_details失败:{e}"))?;

    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取rule_details失败:{e}"))?
    {
        let rule_name: String = row.get(0).map_err(|e| format!("读取rule_name失败:{e}"))?;
        let ts_code: String = row.get(1).map_err(|e| format!("读取ts_code失败:{e}"))?;
        let trade_date: String = row.get(2).map_err(|e| format!("读取trade_date失败:{e}"))?;
        let rule_score: f64 = row.get(3).map_err(|e| format!("读取rule_score失败:{e}"))?;

        if rule_name.trim().is_empty()
            || ts_code.trim().is_empty()
            || trade_date.trim().is_empty()
            || !rule_score.is_finite()
        {
            continue;
        }

        out.push(RuleDbRow {
            rule_name,
            ts_code,
            trade_date,
            rule_score,
        });
    }

    Ok(out)
}

fn load_most_related_concept_map(source_dir: &str) -> Result<HashMap<String, String>, String> {
    load_ths_concepts_named_map(source_dir, &["most_related_concept", "concept"])
}

fn load_stock_industry_map(source_dir: &str) -> Result<HashMap<String, String>, String> {
    let rows = load_stock_list(source_dir)?;
    let mut map = HashMap::with_capacity(rows.len());

    for row in rows {
        let Some(ts_code) = row.first().map(|v| v.trim()) else {
            continue;
        };
        if ts_code.is_empty() {
            continue;
        }

        let industry = row
            .get(4)
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
            .unwrap_or("")
            .to_string();

        map.insert(ts_code.to_string(), industry);
    }

    Ok(map)
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

    use crate::data::{result_db_path, source_db_path};

    use super::{
        RuleLayerConfig, RuleLayerFromDbInput, RuleSample, build_rule_layer_runtime_cache,
        build_rule_layer_runtime_cache_from_stock_data, calc_all_rule_layer_metrics_from_db,
        calc_rule_layer_metrics, calc_rule_layer_metrics_from_db,
        calc_rule_layer_metrics_with_samples_from_cache,
        calc_rule_layer_metrics_with_triggered_samples_from_cache,
        collect_triggered_rule_samples_from_cache,
    };

    fn assert_opt_close(left: Option<f64>, right: Option<f64>) {
        match (left, right) {
            (Some(a), Some(b)) => assert!((a - b).abs() < 1e-9, "left={a}, right={b}"),
            (None, None) => {}
            _ => panic!("left={left:?}, right={right:?}"),
        }
    }

    #[test]
    fn calc_rule_layer_metrics_reports_profit_loss_ratio() {
        let samples = vec![
            RuleSample {
                ts_code: "000001.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 1.0,
                residual_return: 4.0,
            },
            RuleSample {
                ts_code: "000002.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 1.0,
                residual_return: -2.0,
            },
            RuleSample {
                ts_code: "000003.SZ".to_string(),
                trade_date: "20240103".to_string(),
                rule_score: 1.0,
                residual_return: 6.0,
            },
            RuleSample {
                ts_code: "000004.SZ".to_string(),
                trade_date: "20240103".to_string(),
                rule_score: 1.0,
                residual_return: -3.0,
            },
        ];

        let metrics = calc_rule_layer_metrics(
            &samples,
            &RuleLayerConfig {
                min_samples_per_day: 2,
                backtest_period: 1,
                min_listed_trade_days: 0,
            },
        )
        .expect("metrics");

        assert_opt_close(metrics.profit_loss_ratio, Some(2.0));
    }

    fn temp_source_dir() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("lianghua_rule_layer_{unique}"))
    }

    fn prepare_test_files(source_dir: &str) {
        create_dir_all(source_dir).expect("create source dir");

        write(
            PathBuf::from(source_dir).join("stock_list.csv"),
            concat!(
                "ts_code,symbol,name,area,industry,list_date,trade_date,total_share,float_share,total_mv,circ_mv,fullname,enname,cnspell,market,exchange,curr_type,list_status,delist_date,is_hs,act_name,act_ent_type\n",
                "000001.SZ,,样本股A,,main,20230001,,,,,,,,,,,,,,,,\n",
                "000002.SZ,,样本股B,,main,20230001,,,,,,,,,,,,,,,,\n"
            ),
        )
        .expect("write stock_list.csv");
        let trade_calendar = std::iter::once("cal_date".to_string())
            .chain((1..=70).map(|index| format!("202300{:02}", index)))
            .chain(
                ["20240102", "20240103", "20240104"]
                    .into_iter()
                    .map(str::to_string),
            )
            .collect::<Vec<_>>()
            .join("\n");
        write(
            PathBuf::from(source_dir).join("trade_calendar.csv"),
            format!("{trade_calendar}\n"),
        )
        .expect("write trade_calendar.csv");
        write(
            PathBuf::from(source_dir).join("stock_concepts.csv"),
            "ts_code,c1,c2,c3,concept\n000001.SZ,,,,concept-a\n000002.SZ,,,,concept-b\n",
        )
        .expect("write stock_concepts.csv");

        let source_conn = Connection::open(source_db_path(source_dir)).expect("open source db");
        source_conn
            .execute(
                r#"
                CREATE TABLE stock_data (
                    ts_code VARCHAR,
                    trade_date VARCHAR,
                    adj_type VARCHAR,
                    pct_chg DOUBLE
                )
                "#,
                [],
            )
            .expect("create stock_data");

        let mut source_app = source_conn
            .appender("stock_data")
            .expect("stock_data appender");

        source_app
            .append_row(params!["000001.SZ", "20240102", "qfq", 0.0_f64])
            .expect("stock a row1");
        source_app
            .append_row(params!["000001.SZ", "20240103", "qfq", 3.0_f64])
            .expect("stock a row2");
        source_app
            .append_row(params!["000001.SZ", "20240104", "qfq", 5.0_f64])
            .expect("stock a row3");

        source_app
            .append_row(params!["000002.SZ", "20240102", "qfq", 0.0_f64])
            .expect("stock b row1");
        source_app
            .append_row(params!["000002.SZ", "20240103", "qfq", 1.0_f64])
            .expect("stock b row2");
        source_app
            .append_row(params!["000002.SZ", "20240104", "qfq", -1.0_f64])
            .expect("stock b row3");

        source_app
            .append_row(params!["000300.SH", "20240102", "ind", 0.0_f64])
            .expect("index row1");
        source_app
            .append_row(params!["000300.SH", "20240103", "ind", 0.0_f64])
            .expect("index row2");
        source_app
            .append_row(params!["000300.SH", "20240104", "ind", 0.0_f64])
            .expect("index row3");
        source_app.flush().expect("flush stock_data");

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
                r#"
                CREATE TABLE rule_details (
                    rule_name VARCHAR,
                    ts_code VARCHAR,
                    trade_date VARCHAR,
                    rule_score DOUBLE
                )
                "#,
                [],
            )
            .expect("create rule_details");

        let mut summary_app = result_conn
            .appender("score_summary")
            .expect("score_summary appender");
        summary_app
            .append_row(params!["000001.SZ", "20240102", 10.0_f64, 1_i64])
            .expect("summary row1");
        summary_app
            .append_row(params!["000002.SZ", "20240102", 9.0_f64, 2_i64])
            .expect("summary row2");
        summary_app
            .append_row(params!["000001.SZ", "20240103", 11.0_f64, 1_i64])
            .expect("summary row3");
        summary_app
            .append_row(params!["000002.SZ", "20240103", 8.0_f64, 2_i64])
            .expect("summary row4");
        summary_app.flush().expect("flush score_summary");

        let mut result_app = result_conn
            .appender("rule_details")
            .expect("rule_details appender");

        result_app
            .append_row(params!["规则A", "000001.SZ", "20240102", 1.0_f64])
            .expect("rule a row1");
        result_app
            .append_row(params!["规则A", "000002.SZ", "20240102", -1.0_f64])
            .expect("rule a row2");
        result_app
            .append_row(params!["规则A", "000001.SZ", "20240103", 2.0_f64])
            .expect("rule a row3");
        result_app
            .append_row(params!["规则A", "000002.SZ", "20240103", -2.0_f64])
            .expect("rule a row4");

        result_app
            .append_row(params!["规则B", "000001.SZ", "20240102", 0.5_f64])
            .expect("rule b row1");
        result_app
            .append_row(params!["规则B", "000002.SZ", "20240102", 0.2_f64])
            .expect("rule b row2");
        result_app
            .append_row(params!["规则B", "000001.SZ", "20240103", 0.4_f64])
            .expect("rule b row3");
        result_app
            .append_row(params!["规则B", "000002.SZ", "20240103", 0.1_f64])
            .expect("rule b row4");
        result_app
            .append_row(params!["规则Zero", "000001.SZ", "20240102", 0.0_f64])
            .expect("rule zero row");
        result_app.flush().expect("flush rule_details");
    }

    #[test]
    fn calc_rule_layer_metrics_from_db_returns_expected_metrics() {
        let source_dir = temp_source_dir();
        let source_dir_str = source_dir.to_str().expect("utf8 source dir");
        prepare_test_files(source_dir_str);

        let source_conn = Connection::open(source_db_path(source_dir_str)).expect("open source db");
        let metrics = calc_rule_layer_metrics_from_db(
            &source_conn,
            source_dir_str,
            &RuleLayerFromDbInput {
                rule_name: "规则A".to_string(),
                stock_adj_type: "qfq".to_string(),
                index_ts_code: "000300.SH".to_string(),
                index_beta: 0.0,
                concept_beta: 0.0,
                industry_beta: 0.0,
                start_date: "20240102".to_string(),
                end_date: "20240104".to_string(),
                layer_config: RuleLayerConfig {
                    min_samples_per_day: 2,
                    backtest_period: 1,
                    min_listed_trade_days: 0,
                },
            },
        )
        .expect("rule metrics");

        assert_eq!(metrics.points.len(), 2);

        let p0 = &metrics.points[0];
        assert_eq!(p0.trade_date, "20240102");
        assert_eq!(p0.sample_count, 2);
        assert_opt_close(p0.avg_rule_score, Some(0.0));
        assert_opt_close(p0.avg_residual_return, Some(2.0));
        assert_opt_close(p0.avg_excess_residual_return, Some(0.0));
        assert_opt_close(p0.top_bottom_spread, Some(2.0));
        assert_opt_close(p0.ic, Some(1.0));

        let p1 = &metrics.points[1];
        assert_eq!(p1.trade_date, "20240103");
        assert_eq!(p1.sample_count, 2);
        assert_opt_close(p1.avg_rule_score, Some(0.0));
        assert_opt_close(p1.avg_residual_return, Some(2.0));
        assert_opt_close(p1.avg_excess_residual_return, Some(0.0));
        assert_opt_close(p1.top_bottom_spread, Some(6.0));
        assert_opt_close(p1.ic, Some(1.0));

        assert_opt_close(metrics.avg_residual_mean, Some(2.0));
        assert_opt_close(metrics.avg_excess_residual_mean, Some(0.0));
        assert_opt_close(metrics.spread_mean, Some(4.0));
        assert_opt_close(metrics.ic_mean, Some(1.0));
        assert_opt_close(metrics.ic_std, Some(0.0));
        assert_eq!(metrics.icir, None);
    }

    #[test]
    fn calc_rule_layer_metrics_from_db_keeps_zero_score_rows_as_triggered_samples() {
        let source_dir = temp_source_dir();
        let source_dir_str = source_dir.to_str().expect("utf8 source dir");
        prepare_test_files(source_dir_str);

        let source_conn = Connection::open(source_db_path(source_dir_str)).expect("open source db");
        let metrics = calc_rule_layer_metrics_from_db(
            &source_conn,
            source_dir_str,
            &RuleLayerFromDbInput {
                rule_name: "规则Zero".to_string(),
                stock_adj_type: "qfq".to_string(),
                index_ts_code: "000300.SH".to_string(),
                index_beta: 0.0,
                concept_beta: 0.0,
                industry_beta: 0.0,
                start_date: "20240102".to_string(),
                end_date: "20240104".to_string(),
                layer_config: RuleLayerConfig {
                    min_samples_per_day: 2,
                    backtest_period: 1,
                    min_listed_trade_days: 0,
                },
            },
        )
        .expect("rule metrics");

        assert_eq!(metrics.points.len(), 2);
        let p0 = &metrics.points[0];
        assert_eq!(p0.trade_date, "20240102");
        assert_eq!(p0.sample_count, 2);
        assert_opt_close(p0.avg_rule_score, Some(0.0));
        assert_opt_close(p0.avg_residual_return, Some(3.0));
        assert_opt_close(p0.avg_excess_residual_return, Some(1.0));

        let p1 = &metrics.points[1];
        assert_eq!(p1.trade_date, "20240103");
        assert_eq!(p1.avg_residual_return, None);
    }

    #[test]
    fn calc_rule_layer_metrics_from_db_keeps_full_universe_for_non_triggered_metrics() {
        let source_dir = temp_source_dir();
        let source_dir_str = source_dir.to_str().expect("utf8 source dir");
        prepare_test_files(source_dir_str);

        let source_conn = Connection::open(source_db_path(source_dir_str)).expect("open source db");
        let metrics = calc_rule_layer_metrics_from_db(
            &source_conn,
            source_dir_str,
            &RuleLayerFromDbInput {
                rule_name: "规则C".to_string(),
                stock_adj_type: "qfq".to_string(),
                index_ts_code: "000300.SH".to_string(),
                index_beta: 0.0,
                concept_beta: 0.0,
                industry_beta: 0.0,
                start_date: "20240102".to_string(),
                end_date: "20240104".to_string(),
                layer_config: RuleLayerConfig {
                    min_samples_per_day: 2,
                    backtest_period: 1,
                    min_listed_trade_days: 0,
                },
            },
        )
        .expect("rule metrics");

        assert_eq!(metrics.points.len(), 2);

        let p0 = &metrics.points[0];
        assert_eq!(p0.trade_date, "20240102");
        assert_eq!(p0.sample_count, 2);
        assert_opt_close(p0.avg_rule_score, Some(0.0));
        assert_eq!(p0.avg_residual_return, None);
        assert_eq!(p0.avg_excess_residual_return, None);
        assert_eq!(p0.top_bottom_spread, None);
        assert_eq!(p0.ic, None);

        let p1 = &metrics.points[1];
        assert_eq!(p1.trade_date, "20240103");
        assert_eq!(p1.sample_count, 2);
        assert_opt_close(p1.avg_rule_score, Some(0.0));
        assert_eq!(p1.avg_residual_return, None);
        assert_eq!(p1.avg_excess_residual_return, None);
        assert_eq!(p1.top_bottom_spread, None);
        assert_eq!(p1.ic, None);

        assert_eq!(metrics.avg_residual_mean, None);
        assert_eq!(metrics.avg_excess_residual_mean, None);
        assert_eq!(metrics.spread_mean, None);
        assert_eq!(metrics.ic_mean, None);
    }

    #[test]
    fn batch_rule_layer_metrics_match_single_rule_results() {
        let source_dir = temp_source_dir();
        let source_dir_str = source_dir.to_str().expect("utf8 source dir");
        prepare_test_files(source_dir_str);

        let source_conn = Connection::open(source_db_path(source_dir_str)).expect("open source db");
        let rule_names = vec!["规则A".to_string(), "规则B".to_string()];
        let layer_config = RuleLayerConfig {
            min_samples_per_day: 1,
            backtest_period: 1,
            min_listed_trade_days: 0,
        };

        let batch_metrics = calc_all_rule_layer_metrics_from_db(
            &source_conn,
            source_dir_str,
            &rule_names,
            "qfq",
            "000300.SH",
            0.0,
            0.0,
            0.0,
            "20240102",
            "20240104",
            &layer_config,
        )
        .expect("batch metrics");

        assert_eq!(batch_metrics.len(), 2);

        for (rule_name, metrics) in batch_metrics {
            let single_metrics = calc_rule_layer_metrics_from_db(
                &source_conn,
                source_dir_str,
                &RuleLayerFromDbInput {
                    rule_name: rule_name.clone(),
                    stock_adj_type: "qfq".to_string(),
                    index_ts_code: "000300.SH".to_string(),
                    index_beta: 0.0,
                    concept_beta: 0.0,
                    industry_beta: 0.0,
                    start_date: "20240102".to_string(),
                    end_date: "20240104".to_string(),
                    layer_config: layer_config.clone(),
                },
            )
            .expect("single metrics");

            assert_eq!(metrics, single_metrics);
        }
    }

    #[test]
    fn runtime_cache_uses_triggered_residual_mean_and_full_universe_layer_metrics() {
        let source_dir = temp_source_dir();
        let source_dir_str = source_dir.to_str().expect("utf8 source dir");
        prepare_test_files(source_dir_str);

        let source_conn = Connection::open(source_db_path(source_dir_str)).expect("open source db");
        let runtime_cache = build_rule_layer_runtime_cache(
            &source_conn,
            source_dir_str,
            "qfq",
            "000300.SH",
            0.0,
            0.0,
            0.0,
            "20240102",
            "20240104",
            &RuleLayerConfig {
                min_samples_per_day: 1,
                backtest_period: 1,
                min_listed_trade_days: 0,
            },
        )
        .expect("build runtime cache");
        let triggered_score_map = HashMap::from([(
            "000001.SZ".to_string(),
            HashMap::from([("20240102".to_string(), 1.5_f64)]),
        )]);

        let full_samples = calc_rule_layer_metrics_with_samples_from_cache(
            &runtime_cache,
            &triggered_score_map,
            &RuleLayerConfig {
                min_samples_per_day: 1,
                backtest_period: 1,
                min_listed_trade_days: 0,
            },
        )
        .expect("compute full samples");
        let triggered_samples = calc_rule_layer_metrics_with_triggered_samples_from_cache(
            &runtime_cache,
            &triggered_score_map,
            &RuleLayerConfig {
                min_samples_per_day: 1,
                backtest_period: 1,
                min_listed_trade_days: 0,
            },
        )
        .expect("compute triggered samples");
        let collected_triggered =
            collect_triggered_rule_samples_from_cache(&runtime_cache, &triggered_score_map);

        assert_eq!(full_samples.metrics, triggered_samples.metrics);
        assert_eq!(full_samples.metrics.points.len(), 2);
        assert_eq!(full_samples.metrics.points[0].trade_date, "20240102");
        assert_eq!(full_samples.metrics.points[0].sample_count, 2);
        assert_opt_close(full_samples.metrics.points[0].avg_rule_score, Some(0.75));
        assert_opt_close(
            full_samples.metrics.points[0].avg_residual_return,
            Some(3.0),
        );
        assert_opt_close(
            full_samples.metrics.points[0].avg_excess_residual_return,
            Some(1.0),
        );
        assert_opt_close(full_samples.metrics.points[0].top_bottom_spread, Some(2.0));
        assert_opt_close(full_samples.metrics.points[0].ic, Some(1.0));
        assert_eq!(full_samples.metrics.points[1].trade_date, "20240103");
        assert_eq!(full_samples.metrics.points[1].sample_count, 2);
        assert_opt_close(full_samples.metrics.points[1].avg_rule_score, Some(0.0));
        assert_eq!(full_samples.metrics.points[1].avg_residual_return, None);
        assert_eq!(
            full_samples.metrics.points[1].avg_excess_residual_return,
            None
        );
        assert_eq!(full_samples.metrics.points[1].top_bottom_spread, None);
        assert_eq!(full_samples.metrics.points[1].ic, None);
        assert_opt_close(full_samples.metrics.avg_residual_mean, Some(3.0));
        assert_opt_close(full_samples.metrics.avg_excess_residual_mean, Some(1.0));
        assert_opt_close(full_samples.metrics.spread_mean, Some(2.0));
        assert_opt_close(full_samples.metrics.ic_mean, Some(1.0));
        assert_eq!(full_samples.samples.len(), 4);
        assert_eq!(
            full_samples
                .samples
                .iter()
                .filter(|sample| sample.rule_score.abs() < 1e-12)
                .count(),
            3
        );

        assert_eq!(triggered_samples.triggered_samples.len(), 1);
        assert_eq!(triggered_samples.triggered_samples, collected_triggered);
        assert_eq!(triggered_samples.triggered_samples[0].ts_code, "000001.SZ");
        assert_eq!(
            triggered_samples.triggered_samples[0].trade_date,
            "20240102"
        );
        assert!((triggered_samples.triggered_samples[0].rule_score - 1.5).abs() < 1e-12);
    }

    #[test]
    fn stock_data_runtime_cache_is_not_limited_by_score_summary_dates() {
        let source_dir = temp_source_dir();
        let source_dir_str = source_dir.to_str().expect("utf8 source dir");
        prepare_test_files(source_dir_str);

        let result_conn = Connection::open(result_db_path(source_dir_str)).expect("open result db");
        result_conn
            .execute(
                "DELETE FROM score_summary WHERE trade_date = '20240103'",
                [],
            )
            .expect("delete score summary date");

        let source_conn = Connection::open(source_db_path(source_dir_str)).expect("open source db");
        let layer_config = RuleLayerConfig {
            min_samples_per_day: 1,
            backtest_period: 1,
            min_listed_trade_days: 0,
        };
        let triggered_score_map = HashMap::from([
            (
                "000001.SZ".to_string(),
                HashMap::from([
                    ("20240102".to_string(), 1.0_f64),
                    ("20240103".to_string(), 1.0_f64),
                ]),
            ),
            (
                "000002.SZ".to_string(),
                HashMap::from([
                    ("20240102".to_string(), 1.0_f64),
                    ("20240103".to_string(), 1.0_f64),
                ]),
            ),
        ]);

        let result_limited_cache = build_rule_layer_runtime_cache(
            &source_conn,
            source_dir_str,
            "qfq",
            "000300.SH",
            0.0,
            0.0,
            0.0,
            "20240102",
            "20240104",
            &layer_config,
        )
        .expect("build result limited runtime cache");
        let stock_data_cache = build_rule_layer_runtime_cache_from_stock_data(
            &source_conn,
            source_dir_str,
            "qfq",
            "000300.SH",
            0.0,
            0.0,
            0.0,
            "20240102",
            "20240104",
            &layer_config,
        )
        .expect("build stock data runtime cache");

        let result_limited_metrics = calc_rule_layer_metrics_with_samples_from_cache(
            &result_limited_cache,
            &triggered_score_map,
            &layer_config,
        )
        .expect("compute result limited metrics");
        let stock_data_metrics = calc_rule_layer_metrics_with_samples_from_cache(
            &stock_data_cache,
            &triggered_score_map,
            &layer_config,
        )
        .expect("compute stock data metrics");

        assert_eq!(
            result_limited_metrics
                .metrics
                .points
                .iter()
                .map(|point| point.trade_date.as_str())
                .collect::<Vec<_>>(),
            vec!["20240102"]
        );
        assert_eq!(
            stock_data_metrics
                .metrics
                .points
                .iter()
                .map(|point| point.trade_date.as_str())
                .collect::<Vec<_>>(),
            vec!["20240102", "20240103"]
        );
    }
}
