//! 规则分层回测：类型、对外入口与子模块装配。
//!
//! - `cache`：运行时缓存构建与输入准备
//! - `metrics`：分层指标计算
//! - `residual`：残差与行情序列的流式计算

mod cache;
mod metrics;
mod residual;
use crate::simulate::rule::cache::{
    build_triggered_score_maps_from_detail_rows, ts_code_allowed, validate_rule_common_input,
};
use crate::simulate::rule::metrics::{
    calc_rule_layer_metrics_from_cache, calc_rule_layer_metrics_from_score_column,
    calc_rule_layer_metrics_with_samples_from_score_column,
    calc_rule_layer_metrics_with_validation_from_score_column,
    compute_rule_layer_from_runtime_cache,
};
#[cfg(test)]
pub(super) mod test_support;

pub(crate) use cache::build_rule_layer_runtime_cache_from_summary_rows;
pub use cache::{
    build_rule_layer_runtime_cache, build_rule_layer_runtime_cache_from_stock_data,
    build_rule_layer_runtime_cache_from_stock_data_with_ts_filter,
    build_rule_layer_runtime_cache_with_ts_filter,
};
pub use metrics::{
    RuleLayerSamplePointRef, calc_rule_layer_metrics,
    calc_rule_layer_metrics_from_triggered_scores, calc_rule_layer_metrics_with_samples_from_cache,
    calc_rule_layer_metrics_with_samples_from_triggered_scores,
    calc_rule_layer_metrics_with_triggered_samples_from_cache,
    calc_rule_layer_metrics_with_validation_from_cache, collect_all_rule_samples_from_cache,
    collect_triggered_rule_samples_from_cache, visit_triggered_rule_samples_from_cache,
};

use super::DEFAULT_BACKTEST_MIN_LISTED_TRADE_DAYS;
use crate::data::result_db_path;
use crate::scoring_model::CompactRuleScore;
use crate::scoring_model::ScoreDetails;
use crate::scoring_model::ScoreSummary;
use duckdb::Connection;
use duckdb::params_from_iter;
use rayon::prelude::*;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
pub(super) const PCT_CHG_BATCH_SIZE: usize = 512;
pub(super) const RESIDUAL_SERIES_TARGET_POINTS: usize = 256 * 1024;
pub(super) const RESIDUAL_STOCK_BATCH_MIN: usize = 128;
pub(super) const RESIDUAL_STOCK_BATCH_MAX: usize = PCT_CHG_BATCH_SIZE;
pub(super) const EFFICIENCY_RATIO_PERIOD: usize = 20;
pub const DEFAULT_RULE_WITH_SAMPLES_PARALLEL_BATCH_SIZE: usize = 4;

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
    pub score_weighted_residual_return: Option<f64>,
    pub top_bottom_spread: Option<f64>,
    pub ic: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuleLayerMetrics {
    pub points: Vec<RuleLayerPoint>,
    pub avg_residual_mean: Option<f64>,
    pub avg_excess_residual_mean: Option<f64>,
    pub avg_er_change: Option<f64>,
    pub er_change_sample_count: usize,
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
    pub er_change: f64,
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

#[derive(Debug, Clone, PartialEq)]
pub struct RuleLayerDailyScoreGroup {
    pub score: f64,
    pub sample_count: usize,
    pub avg_residual_return: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuleLayerDailyScoreLayers {
    pub trade_date: String,
    pub groups: Vec<RuleLayerDailyScoreGroup>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuleLayerMetricsWithValidation {
    pub metrics: RuleLayerMetrics,
    pub triggered_samples: Vec<RuleLayerSamplePoint>,
    pub daily_score_layers: Vec<RuleLayerDailyScoreLayers>,
    pub return_distribution_counts: [usize; 7],
}

#[derive(Debug, Clone)]
pub struct RuleLayerRuntimeCache {
    day_groups: Vec<RuleDayGroup>,
    ts_codes: Vec<Arc<str>>,
    ts_code_ids: HashMap<String, u32>,
    day_group_ids: HashMap<String, usize>,
    score_column_len: usize,
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(super) struct RuleDbRow {
    rule_name: String,
    ts_code: String,
    trade_date: String,
    rule_score: f64,
}

#[derive(Debug, Clone)]
pub(super) struct RuleUniverseRow {
    ts_code: String,
    trade_date: String,
}

#[derive(Debug, Clone)]
pub(super) struct RuleDayBaseSample {
    ts_code_id: u32,
    residual_return: f64,
    er_change: f64,
}

#[derive(Debug, Clone)]
pub(super) struct RuleDayGroup {
    trade_date: Arc<str>,
    score_offset: usize,
    samples: Vec<RuleDayBaseSample>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RuleLayerCollectOptions {
    metrics: bool,
    all_samples: bool,
    triggered_samples: bool,
    validation_details: bool,
}

pub(super) struct RuleLayerComputation {
    metrics: RuleLayerMetrics,
    all_samples: Vec<RuleLayerSamplePoint>,
    triggered_samples: Vec<RuleLayerSamplePoint>,
    daily_score_layers: Vec<RuleLayerDailyScoreLayers>,
    return_distribution_counts: [usize; 7],
}

pub(super) type TriggeredScoreMap = HashMap<String, HashMap<String, f64>>;

/// 规则触发分数按运行时样本顺序连续存放。`valid` 必须与 `values` 分离，
/// 因为分数为 0 的记录仍然表示“已触发”，不能用 0 或 NaN 充当缺失哨兵。
#[derive(Debug, Clone)]
pub(super) struct TriggeredScoreColumn {
    values: Vec<f64>,
    valid: Vec<bool>,
}

impl TriggeredScoreColumn {
    fn empty() -> Self {
        Self {
            values: Vec::new(),
            valid: Vec::new(),
        }
    }

    fn with_len(len: usize) -> Self {
        Self {
            values: vec![0.0; len],
            valid: vec![false; len],
        }
    }

    fn from_indexed(len: usize, scores: Vec<(usize, f64)>) -> Self {
        if scores.is_empty() || len == 0 {
            return Self::empty();
        }
        let mut encoded = Self::with_len(len);
        for (index, score) in scores {
            if index < len {
                encoded.values[index] = score;
                encoded.valid[index] = true;
            }
        }
        encoded
    }

    #[inline]
    fn get(&self, index: usize) -> Option<f64> {
        self.valid
            .get(index)
            .copied()
            .unwrap_or(false)
            .then(|| self.values[index])
    }
}

pub(super) struct ResidualCacheInput<'a> {
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

#[derive(Debug, Clone, Copy)]
pub(super) struct RuleBacktestOutcome {
    residual_return: f64,
    er_change: f64,
}

impl RuleLayerRuntimeCache {
    pub fn trade_dates(&self) -> impl Iterator<Item = &str> {
        self.day_groups
            .iter()
            .map(|day_group| day_group.trade_date.as_ref())
    }

    fn empty() -> Self {
        Self {
            day_groups: Vec::new(),
            ts_codes: Vec::new(),
            ts_code_ids: HashMap::new(),
            day_group_ids: HashMap::new(),
            score_column_len: 0,
        }
    }

    fn encode_triggered_scores(&self, scores: &TriggeredScoreMap) -> TriggeredScoreColumn {
        if scores.is_empty() || self.score_column_len == 0 {
            return TriggeredScoreColumn::empty();
        }

        let mut encoded = TriggeredScoreColumn::with_len(self.score_column_len);
        for (ts_code, scores_by_date) in scores {
            let Some(&ts_code_id) = self.ts_code_ids.get(ts_code) else {
                continue;
            };
            for (trade_date, &score) in scores_by_date {
                let Some(&day_group_id) = self.day_group_ids.get(trade_date) else {
                    continue;
                };
                let day_group = &self.day_groups[day_group_id];
                let flat_index = day_group.score_offset + ts_code_id as usize;
                encoded.values[flat_index] = score;
                encoded.valid[flat_index] = true;
            }
        }
        encoded
    }

    #[inline]
    fn ts_code(&self, ts_code_id: u32) -> &str {
        &self.ts_codes[ts_code_id as usize]
    }

    #[inline]
    fn score_index(&self, day_group: &RuleDayGroup, sample: &RuleDayBaseSample) -> usize {
        day_group.score_offset + sample.ts_code_id as usize
    }
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
    let triggered_scores = load_triggered_score_column_for_name_filtered(
        source_dir,
        &input.rule_name,
        &input.start_date,
        &input.end_date,
        allowed_ts_codes,
        &runtime_cache,
    )?;
    calc_rule_layer_metrics_from_score_column(
        &runtime_cache,
        &triggered_scores,
        &input.layer_config,
    )
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
    calc_all_rule_layer_metrics_from_db_map_with_ts_filter(
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
        DEFAULT_RULE_WITH_SAMPLES_PARALLEL_BATCH_SIZE,
        |rule_name, metrics| Ok((rule_name.to_string(), metrics)),
    )
}

pub fn calc_all_rule_layer_metrics_from_db_map_with_ts_filter<T, F>(
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
    parallel_batch_size: usize,
    map_result: F,
) -> Result<Vec<T>, String>
where
    T: Send,
    F: Fn(&str, RuleLayerMetrics) -> Result<T, String> + Sync,
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
    let mut out = Vec::with_capacity(rule_names.len());
    for rule_batch in rule_names.chunks(parallel_batch_size.max(1)) {
        let batch_results = rule_batch
            .par_iter()
            .map(|rule_name| {
                let triggered_scores = load_triggered_score_column_for_name_filtered(
                    source_dir,
                    rule_name,
                    start_date,
                    end_date,
                    allowed_ts_codes,
                    &runtime_cache,
                )?;
                let metrics = calc_rule_layer_metrics_from_score_column(
                    &runtime_cache,
                    &triggered_scores,
                    layer_config,
                )?;
                map_result(rule_name, metrics)
            })
            .collect::<Vec<_>>();
        for item in batch_results {
            out.push(item?);
        }
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
        DEFAULT_RULE_WITH_SAMPLES_PARALLEL_BATCH_SIZE,
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
    parallel_batch_size: usize,
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
    // 每条规则都会物化一份全市场样本和触发分数。两者都按小批次加载、计算、
    // 释放；否则 parallel_batch_size=1 只限制计算并发，全部规则的触发明细仍会
    // 在计算前同时常驻，峰值内存不会随该参数下降。
    let mut grouped_results = Vec::with_capacity(rule_names.len());
    for rule_batch in rule_names.chunks(parallel_batch_size.max(1)) {
        let mut batch_results: Vec<Result<T, String>> = rule_batch
            .par_iter()
            .map(|rule_name| {
                let triggered_scores = load_triggered_score_column_for_name_filtered(
                    source_dir,
                    rule_name,
                    start_date,
                    end_date,
                    allowed_ts_codes,
                    &runtime_cache,
                )?;
                let metrics = calc_rule_layer_metrics_with_samples_from_score_column(
                    &runtime_cache,
                    &triggered_scores,
                    layer_config,
                )?;
                map_result(rule_name, metrics)
            })
            .collect();
        grouped_results.append(&mut batch_results);
    }

    let mut out = Vec::with_capacity(grouped_results.len());
    for item in grouped_results {
        out.push(item?);
    }

    Ok(out)
}

pub fn calc_all_rule_layer_metrics_with_validation_from_db_map_with_ts_filter<T, F>(
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
    parallel_batch_size: usize,
    map_result: F,
) -> Result<Vec<T>, String>
where
    T: Send,
    F: Fn(&str, RuleLayerMetricsWithValidation) -> Result<T, String> + Sync,
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
    let mut grouped_results = Vec::with_capacity(rule_names.len());
    for rule_batch in rule_names.chunks(parallel_batch_size.max(1)) {
        let mut batch_results: Vec<Result<T, String>> = rule_batch
            .par_iter()
            .map(|rule_name| {
                let triggered_scores = load_triggered_score_column_for_name_filtered(
                    source_dir,
                    rule_name,
                    start_date,
                    end_date,
                    allowed_ts_codes,
                    &runtime_cache,
                )?;
                let metrics = calc_rule_layer_metrics_with_validation_from_score_column(
                    &runtime_cache,
                    &triggered_scores,
                    layer_config,
                )?;
                map_result(rule_name, metrics)
            })
            .collect();
        grouped_results.append(&mut batch_results);
    }

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
        DEFAULT_RULE_WITH_SAMPLES_PARALLEL_BATCH_SIZE,
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
    parallel_batch_size: usize,
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

    // 与数据库路径一致：固定小批次并行，限制同时存活的全市场样本 Vec 数量。
    let mut grouped_results = Vec::with_capacity(rule_names.len());
    for rule_batch in rule_names.chunks(parallel_batch_size.max(1)) {
        let mut batch_results: Vec<Result<T, String>> = rule_batch
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
        grouped_results.append(&mut batch_results);
    }

    let mut out = Vec::with_capacity(grouped_results.len());
    for item in grouped_results {
        out.push(item?);
    }

    Ok(out)
}

pub fn calc_all_rule_layer_metrics_with_validation_from_rows_map<T, F>(
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
    parallel_batch_size: usize,
    map_result: F,
) -> Result<Vec<T>, String>
where
    T: Send,
    F: Fn(&str, RuleLayerMetricsWithValidation) -> Result<T, String> + Sync,
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

    let mut grouped_results = Vec::with_capacity(rule_names.len());
    for rule_batch in rule_names.chunks(parallel_batch_size.max(1)) {
        let mut batch_results: Vec<Result<T, String>> = rule_batch
            .par_iter()
            .map(|rule_name| {
                let empty_triggered_score_map = TriggeredScoreMap::new();
                let triggered_score_map = triggered_score_map_by_rule
                    .get(rule_name)
                    .unwrap_or(&empty_triggered_score_map);
                let metrics = calc_rule_layer_metrics_with_validation_from_cache(
                    &runtime_cache,
                    triggered_score_map,
                    layer_config,
                )?;
                map_result(rule_name, metrics)
            })
            .collect();
        grouped_results.append(&mut batch_results);
    }

    let mut out = Vec::with_capacity(grouped_results.len());
    for item in grouped_results {
        out.push(item?);
    }
    Ok(out)
}

/// 消费评分明细并立即压缩为运行时下标。用于临时策略回测，避免在原始
/// `ScoreDetails` 之外再为全部规则复制一份三层字符串 HashMap。
pub fn calc_all_rule_layer_metrics_with_validation_from_owned_rows_map<T, F>(
    source_conn: &Connection,
    source_dir: &str,
    rule_names: &[String],
    score_summary_rows: &[ScoreSummary],
    score_detail_rows: Vec<ScoreDetails>,
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &RuleLayerConfig,
    parallel_batch_size: usize,
    map_result: F,
) -> Result<Vec<T>, String>
where
    T: Send,
    F: Fn(&str, RuleLayerMetricsWithValidation) -> Result<T, String> + Sync,
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
    let rule_name_ids = rule_names
        .iter()
        .enumerate()
        .map(|(index, rule_name)| (rule_name.as_str(), index))
        .collect::<HashMap<_, _>>();
    let mut indexed_scores_by_rule = (0..rule_names.len())
        .map(|_| Vec::<(usize, f64)>::new())
        .collect::<Vec<_>>();
    for row in score_detail_rows {
        if row.trade_date.as_str() < start_date
            || row.trade_date.as_str() > end_date
            || !row.rule_score.is_finite()
        {
            continue;
        }
        let (Some(&rule_id), Some(&ts_code_id), Some(&day_group_id)) = (
            rule_name_ids.get(row.rule_name.as_str()),
            runtime_cache.ts_code_ids.get(&row.ts_code),
            runtime_cache.day_group_ids.get(&row.trade_date),
        ) else {
            continue;
        };
        let flat_index = runtime_cache.day_groups[day_group_id].score_offset + ts_code_id as usize;
        indexed_scores_by_rule[rule_id].push((flat_index, row.rule_score));
    }

    compute_validation_from_indexed_scores(
        &runtime_cache,
        rule_names,
        indexed_scores_by_rule,
        layer_config,
        parallel_batch_size,
        map_result,
    )
}

pub(super) fn compute_validation_from_indexed_scores<T, F>(
    runtime_cache: &RuleLayerRuntimeCache,
    rule_names: &[String],
    mut indexed_scores_by_rule: Vec<Vec<(usize, f64)>>,
    layer_config: &RuleLayerConfig,
    parallel_batch_size: usize,
    map_result: F,
) -> Result<Vec<T>, String>
where
    T: Send,
    F: Fn(&str, RuleLayerMetricsWithValidation) -> Result<T, String> + Sync,
{
    let batch_size = parallel_batch_size.max(1);
    let mut grouped_results = Vec::with_capacity(rule_names.len());
    for batch_start in (0..rule_names.len()).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(rule_names.len());
        let encoded_scores = (batch_start..batch_end)
            .map(|rule_id| {
                TriggeredScoreColumn::from_indexed(
                    runtime_cache.score_column_len,
                    std::mem::take(&mut indexed_scores_by_rule[rule_id]),
                )
            })
            .collect::<Vec<_>>();
        let mut batch_results = rule_names[batch_start..batch_end]
            .par_iter()
            .zip(encoded_scores.par_iter())
            .map(|(rule_name, triggered_scores)| {
                let computation = compute_rule_layer_from_runtime_cache(
                    runtime_cache,
                    Some(triggered_scores),
                    layer_config,
                    RuleLayerCollectOptions {
                        metrics: true,
                        all_samples: false,
                        triggered_samples: false,
                        validation_details: true,
                    },
                )?;
                map_result(
                    rule_name,
                    RuleLayerMetricsWithValidation {
                        metrics: computation.metrics,
                        triggered_samples: computation.triggered_samples,
                        daily_score_layers: computation.daily_score_layers,
                        return_distribution_counts: computation.return_distribution_counts,
                    },
                )
            })
            .collect::<Vec<Result<T, String>>>();
        grouped_results.append(&mut batch_results);
    }

    let mut out = Vec::with_capacity(grouped_results.len());
    for item in grouped_results {
        out.push(item?);
    }
    Ok(out)
}

/// 直接消费评分阶段生成的紧凑触发行；触发行只含两个 `u32` 下标和分数，
/// 不再构造全市场 `ScoreDetails` 字符串对象。
pub fn calc_all_rule_layer_metrics_with_validation_from_compact_rows_map<T, F>(
    source_conn: &Connection,
    source_dir: &str,
    rule_names: &[String],
    scoring_rule_names: &[String],
    score_summary_rows: &[ScoreSummary],
    compact_rule_rows: Vec<CompactRuleScore>,
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &RuleLayerConfig,
    parallel_batch_size: usize,
    map_result: F,
) -> Result<Vec<T>, String>
where
    T: Send,
    F: Fn(&str, RuleLayerMetricsWithValidation) -> Result<T, String> + Sync,
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
    let requested_rule_ids = rule_names
        .iter()
        .enumerate()
        .map(|(index, rule_name)| (rule_name.as_str(), index))
        .collect::<HashMap<_, _>>();
    let scoring_to_requested = scoring_rule_names
        .iter()
        .map(|rule_name| requested_rule_ids.get(rule_name.as_str()).copied())
        .collect::<Vec<_>>();
    let mut indexed_scores_by_rule = (0..rule_names.len())
        .map(|_| Vec::<(usize, f64)>::new())
        .collect::<Vec<_>>();
    for row in compact_rule_rows {
        let (Some(summary), Some(Some(rule_id))) = (
            score_summary_rows.get(row.summary_index as usize),
            scoring_to_requested.get(row.rule_id as usize),
        ) else {
            continue;
        };
        if !row.rule_score.is_finite() {
            continue;
        }
        let (Some(&ts_code_id), Some(&day_group_id)) = (
            runtime_cache.ts_code_ids.get(&summary.ts_code),
            runtime_cache.day_group_ids.get(&summary.trade_date),
        ) else {
            continue;
        };
        let flat_index = runtime_cache.day_groups[day_group_id].score_offset + ts_code_id as usize;
        indexed_scores_by_rule[*rule_id].push((flat_index, row.rule_score));
    }

    compute_validation_from_indexed_scores(
        &runtime_cache,
        rule_names,
        indexed_scores_by_rule,
        layer_config,
        parallel_batch_size,
        map_result,
    )
}

pub(super) fn load_triggered_score_column_for_name_filtered(
    source_dir: &str,
    rule_name: &str,
    start_date: &str,
    end_date: &str,
    allowed_ts_codes: Option<&HashSet<String>>,
    runtime_cache: &RuleLayerRuntimeCache,
) -> Result<TriggeredScoreColumn, String> {
    if rule_name.trim().is_empty() || runtime_cache.score_column_len == 0 {
        return Ok(TriggeredScoreColumn::empty());
    }
    let result_db = result_db_path(source_dir);
    if !result_db.exists() {
        return Ok(TriggeredScoreColumn::empty());
    }
    let result_db_str = result_db
        .to_str()
        .ok_or_else(|| "result_db路径不是有效UTF-8".to_string())?;
    let conn =
        Connection::open(result_db_str).map_err(|e| format!("打开scoring_result.db失败:{e}"))?;
    let mut stmt = conn
        .prepare(
            r#"
            SELECT ts_code, trade_date, TRY_CAST(rule_score AS DOUBLE)
            FROM rule_details
            WHERE rule_name = ?
              AND trade_date >= ?
              AND trade_date <= ?
              AND TRY_CAST(rule_score AS DOUBLE) IS NOT NULL
            ORDER BY trade_date ASC, ts_code ASC
            "#,
        )
        .map_err(|e| format!("预编译单策略rule_details查询失败:{e}"))?;
    let mut rows = stmt
        .query(params_from_iter([
            rule_name.trim(),
            start_date.trim(),
            end_date.trim(),
        ]))
        .map_err(|e| format!("查询单策略rule_details失败:{e}"))?;
    let mut indexed_scores = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取单策略rule_details失败:{e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取ts_code失败:{e}"))?;
        let trade_date: String = row.get(1).map_err(|e| format!("读取trade_date失败:{e}"))?;
        let rule_score: f64 = row.get(2).map_err(|e| format!("读取rule_score失败:{e}"))?;
        if !rule_score.is_finite() || !ts_code_allowed(allowed_ts_codes, &ts_code) {
            continue;
        }
        let (Some(&ts_code_id), Some(&day_group_id)) = (
            runtime_cache.ts_code_ids.get(&ts_code),
            runtime_cache.day_group_ids.get(&trade_date),
        ) else {
            continue;
        };
        indexed_scores.push((
            runtime_cache.day_groups[day_group_id].score_offset + ts_code_id as usize,
            rule_score,
        ));
    }
    Ok(TriggeredScoreColumn::from_indexed(
        runtime_cache.score_column_len,
        indexed_scores,
    ))
}

#[cfg(test)]
mod tests {
    use crate::data::result_db_path;
    use crate::data::source_db_path;
    use crate::scoring_model::ScoreDetails;
    use crate::scoring_model::ScoreSummary;
    use crate::simulate::rule::RuleBacktestOutcome;
    use crate::simulate::rule::RuleLayerConfig;
    use crate::simulate::rule::RuleLayerFromDbInput;
    use crate::simulate::rule::RuleLayerRuntimeCache;
    use crate::simulate::rule::RuleUniverseRow;
    use crate::simulate::rule::TriggeredScoreColumn;
    use crate::simulate::rule::TriggeredScoreMap;
    use crate::simulate::rule::cache::build_rule_layer_runtime_cache;
    use crate::simulate::rule::cache::build_triggered_score_map;
    use crate::simulate::rule::calc_all_rule_layer_metrics_from_db;
    use crate::simulate::rule::calc_all_rule_layer_metrics_with_validation_from_owned_rows_map;
    use crate::simulate::rule::calc_all_rule_layer_metrics_with_validation_from_rows_map;
    use crate::simulate::rule::calc_rule_layer_metrics_from_db;
    use crate::simulate::rule::metrics::build_rule_day_groups;
    use crate::simulate::rule::residual::load_rule_rows_filtered;
    use crate::simulate::rule::test_support::*;
    use duckdb::Connection;
    use duckdb::params;
    use std::collections::HashMap;
    use std::hint::black_box;
    use std::time::Duration;
    use std::time::Instant;
    fn scan_legacy_triggered_scores(
        runtime_cache: &RuleLayerRuntimeCache,
        triggered_scores: &TriggeredScoreMap,
    ) -> (usize, f64) {
        let mut triggered_count = 0usize;
        let mut checksum = 0.0;
        for day_group in &runtime_cache.day_groups {
            for sample in &day_group.samples {
                let score = triggered_scores
                    .get(runtime_cache.ts_code(sample.ts_code_id))
                    .and_then(|scores_by_date| scores_by_date.get(day_group.trade_date.as_ref()))
                    .copied();
                if let Some(score) = score {
                    triggered_count += 1;
                    checksum += black_box(score);
                }
            }
        }
        black_box((triggered_count, checksum))
    }

    fn scan_encoded_triggered_scores(
        runtime_cache: &RuleLayerRuntimeCache,
        triggered_scores: &TriggeredScoreColumn,
    ) -> (usize, f64) {
        let mut triggered_count = 0usize;
        let mut checksum = 0.0;
        for day_group in &runtime_cache.day_groups {
            for sample in &day_group.samples {
                let score = triggered_scores.get(runtime_cache.score_index(day_group, sample));
                if let Some(score) = score {
                    triggered_count += 1;
                    checksum += black_box(score);
                }
            }
        }
        black_box((triggered_count, checksum))
    }

    fn median_duration(mut durations: Vec<Duration>) -> Duration {
        durations.sort_unstable();
        durations[durations.len() / 2]
    }

    #[test]
    fn encoded_triggered_scores_match_nested_map_for_zero_missing_and_duplicate_rows() {
        let universe_rows = vec![
            RuleUniverseRow {
                ts_code: "a".to_string(),
                trade_date: "d0".to_string(),
            },
            RuleUniverseRow {
                ts_code: "a".to_string(),
                trade_date: "d0".to_string(),
            },
            RuleUniverseRow {
                ts_code: "b".to_string(),
                trade_date: "d0".to_string(),
            },
            RuleUniverseRow {
                ts_code: "a".to_string(),
                trade_date: "d1".to_string(),
            },
        ];
        let residual_map = HashMap::from([
            (
                "a".to_string(),
                HashMap::from([
                    (
                        "d0".to_string(),
                        RuleBacktestOutcome {
                            residual_return: 1.0,
                            er_change: 0.0,
                        },
                    ),
                    (
                        "d1".to_string(),
                        RuleBacktestOutcome {
                            residual_return: 2.0,
                            er_change: 0.0,
                        },
                    ),
                ]),
            ),
            (
                "b".to_string(),
                HashMap::from([(
                    "d0".to_string(),
                    RuleBacktestOutcome {
                        residual_return: 3.0,
                        er_change: 0.0,
                    },
                )]),
            ),
        ]);
        let runtime_cache = build_rule_day_groups(universe_rows, &residual_map);
        let triggered_scores = HashMap::from([
            (
                "a".to_string(),
                HashMap::from([("d0".to_string(), 0.0), ("missing-date".to_string(), 7.0)]),
            ),
            (
                "missing-stock".to_string(),
                HashMap::from([("d0".to_string(), 9.0)]),
            ),
        ]);

        let encoded = runtime_cache.encode_triggered_scores(&triggered_scores);
        assert_eq!(
            runtime_cache
                .day_groups
                .iter()
                .map(|group| group.samples.len())
                .sum::<usize>(),
            4
        );
        assert_eq!(
            scan_legacy_triggered_scores(&runtime_cache, &triggered_scores),
            (2, 0.0)
        );
        assert_eq!(
            scan_encoded_triggered_scores(&runtime_cache, &encoded),
            (2, 0.0)
        );
        assert_eq!(encoded.get(0), Some(0.0));
        assert_eq!(encoded.get(1), None);
        assert_eq!(encoded.get(2), None);
        assert_eq!(encoded.get(3), None);
    }

    /// 真实库比较基准。运行示例：
    /// `LIANGHUA_BENCH_DATA_DIR=/path/to/source cargo test --release
    /// benchmark_encoded_triggered_score_lookup_real_data -- --ignored --nocapture`
    #[test]
    #[ignore = "需要本机真实行情与评分数据库"]
    fn benchmark_encoded_triggered_score_lookup_real_data() {
        let source_dir = std::env::var("LIANGHUA_BENCH_DATA_DIR")
            .expect("set LIANGHUA_BENCH_DATA_DIR to a real data directory");
        let start_date =
            std::env::var("LIANGHUA_BENCH_START_DATE").unwrap_or_else(|_| "20250101".to_string());
        let end_date =
            std::env::var("LIANGHUA_BENCH_END_DATE").unwrap_or_else(|_| "20250801".to_string());
        let source_conn = Connection::open(source_db_path(&source_dir)).expect("open source db");
        let result_conn = Connection::open(result_db_path(&source_dir)).expect("open result db");
        let rule_name: String = result_conn
            .query_row(
                r#"
                SELECT rule_name
                FROM rule_details
                WHERE trade_date >= ? AND trade_date <= ?
                GROUP BY rule_name
                ORDER BY COUNT(*) DESC, rule_name ASC
                LIMIT 1
                "#,
                params![&start_date, &end_date],
                |row| row.get(0),
            )
            .expect("select benchmark rule");
        let config = RuleLayerConfig {
            min_samples_per_day: 1,
            backtest_period: 1,
            min_listed_trade_days: 0,
        };
        let input = RuleLayerFromDbInput {
            rule_name: rule_name.clone(),
            stock_adj_type: "qfq".to_string(),
            index_ts_code: "399300.SZ".to_string(),
            index_beta: 0.0,
            concept_beta: 0.0,
            industry_beta: 0.0,
            start_date: start_date.clone(),
            end_date: end_date.clone(),
            layer_config: config.clone(),
        };

        let cache_started = Instant::now();
        let runtime_cache = build_rule_layer_runtime_cache(
            &source_conn,
            &source_dir,
            "qfq",
            "399300.SZ",
            0.0,
            0.0,
            0.0,
            &start_date,
            &end_date,
            &config,
        )
        .expect("build runtime cache");
        let cache_elapsed = cache_started.elapsed();
        let rule_rows = load_rule_rows_filtered(&source_dir, &input, None).expect("load rule rows");
        let triggered_scores = build_triggered_score_map(rule_rows);

        let legacy_expected = scan_legacy_triggered_scores(&runtime_cache, &triggered_scores);
        let encoded = runtime_cache.encode_triggered_scores(&triggered_scores);
        assert_eq!(
            scan_encoded_triggered_scores(&runtime_cache, &encoded),
            legacy_expected
        );

        let rounds = 7;
        let mut legacy_times = Vec::with_capacity(rounds);
        let mut encode_times = Vec::with_capacity(rounds);
        let mut indexed_times = Vec::with_capacity(rounds);
        for _ in 0..rounds {
            let started = Instant::now();
            let result = scan_legacy_triggered_scores(&runtime_cache, &triggered_scores);
            legacy_times.push(started.elapsed());
            assert_eq!(result, legacy_expected);

            let started = Instant::now();
            let one_encoded = runtime_cache.encode_triggered_scores(&triggered_scores);
            encode_times.push(started.elapsed());

            let started = Instant::now();
            let result = scan_encoded_triggered_scores(&runtime_cache, &one_encoded);
            indexed_times.push(started.elapsed());
            assert_eq!(result, legacy_expected);
        }

        let legacy_median = median_duration(legacy_times);
        let encode_median = median_duration(encode_times);
        let indexed_median = median_duration(indexed_times);
        let new_total = encode_median + indexed_median;
        println!(
            "real-data triggered-score lookup benchmark: rule={rule_name}, dates={start_date}..{end_date}, days={}, samples={}, triggered={}, cache_build_ms={}, legacy_scan_ms={:.3}, encode_ms={:.3}, indexed_scan_ms={:.3}, new_total_ms={:.3}, total_speedup={:.3}x, scan_speedup={:.3}x, encoded_bytes~{}",
            runtime_cache.day_groups.len(),
            runtime_cache
                .day_groups
                .iter()
                .map(|group| group.samples.len())
                .sum::<usize>(),
            legacy_expected.0,
            cache_elapsed.as_millis(),
            legacy_median.as_secs_f64() * 1_000.0,
            encode_median.as_secs_f64() * 1_000.0,
            indexed_median.as_secs_f64() * 1_000.0,
            new_total.as_secs_f64() * 1_000.0,
            legacy_median.as_secs_f64() / new_total.as_secs_f64(),
            legacy_median.as_secs_f64() / indexed_median.as_secs_f64(),
            runtime_cache.score_column_len * std::mem::size_of::<f64>()
                + runtime_cache.score_column_len.div_ceil(8),
        );
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
                    min_samples_per_day: 1,
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
    fn owned_detail_rows_match_legacy_validation_results_with_single_rule_batches() {
        let source_dir = temp_source_dir();
        let source_dir_str = source_dir.to_str().expect("utf8 source dir");
        prepare_test_files(source_dir_str);
        let source_conn = Connection::open(source_db_path(source_dir_str)).expect("open source db");
        let rule_names = vec!["规则A".to_string(), "规则B".to_string()];
        let summary_rows = ["20240102", "20240103"]
            .into_iter()
            .flat_map(|trade_date| {
                ["000001.SZ", "000002.SZ"]
                    .into_iter()
                    .map(move |ts_code| ScoreSummary {
                        ts_code: ts_code.to_string(),
                        trade_date: trade_date.to_string(),
                        total_score: 1.0,
                        rank: None,
                    })
            })
            .collect::<Vec<_>>();
        let detail_rows = rule_names
            .iter()
            .flat_map(|rule_name| {
                ["20240102", "20240103"]
                    .into_iter()
                    .flat_map(move |trade_date| {
                        ["000001.SZ", "000002.SZ"]
                            .into_iter()
                            .map(move |ts_code| ScoreDetails {
                                ts_code: ts_code.to_string(),
                                trade_date: trade_date.to_string(),
                                rule_name: rule_name.clone(),
                                rule_score: if ts_code == "000001.SZ" { 1.0 } else { 0.0 },
                            })
                    })
            })
            .collect::<Vec<_>>();
        let layer_config = RuleLayerConfig {
            min_samples_per_day: 1,
            backtest_period: 1,
            min_listed_trade_days: 0,
        };

        let legacy = calc_all_rule_layer_metrics_with_validation_from_rows_map(
            &source_conn,
            source_dir_str,
            &rule_names,
            &summary_rows,
            &detail_rows,
            "qfq",
            "000300.SH",
            0.0,
            0.0,
            0.0,
            "20240102",
            "20240104",
            &layer_config,
            1,
            |rule_name, metrics| Ok((rule_name.to_string(), metrics)),
        )
        .expect("legacy validation results");
        let owned = calc_all_rule_layer_metrics_with_validation_from_owned_rows_map(
            &source_conn,
            source_dir_str,
            &rule_names,
            &summary_rows,
            detail_rows,
            "qfq",
            "000300.SH",
            0.0,
            0.0,
            0.0,
            "20240102",
            "20240104",
            &layer_config,
            1,
            |rule_name, metrics| Ok((rule_name.to_string(), metrics)),
        )
        .expect("owned validation results");

        assert_eq!(owned, legacy);
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
                    min_samples_per_day: 1,
                    backtest_period: 1,
                    min_listed_trade_days: 0,
                },
            },
        )
        .expect("rule metrics");

        assert_eq!(metrics.points.len(), 1);
        let p0 = &metrics.points[0];
        assert_eq!(p0.trade_date, "20240102");
        assert_eq!(p0.sample_count, 2);
        assert_opt_close(p0.avg_rule_score, Some(0.0));
        assert_opt_close(p0.avg_residual_return, Some(3.0));
        assert_opt_close(p0.avg_excess_residual_return, Some(1.0));
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

        assert!(metrics.points.is_empty());

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
}
