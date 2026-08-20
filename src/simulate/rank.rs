use std::collections::{BTreeMap, HashMap};

use duckdb::{Connection, params_from_iter};
use rayon::prelude::*;

use super::fp_utils::calc_newey_west_t_value;
use super::rule::{
    RuleLayerConfig, RuleLayerSamplePoint, build_rule_layer_runtime_cache,
    build_rule_layer_runtime_cache_from_summary_rows, collect_all_rule_samples_from_cache,
};
use crate::data::{result_db_path, scoring_data::ScoreSummary};

const EPS: f64 = 1e-12;
const DEFAULT_LAYER_COUNT: usize = 5;
const MAX_LAYER_COUNT: usize = 100;
const DEFAULT_TOP_KS: [usize; 4] = [1, 5, 20, 100];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RankLayerMethod {
    Score,
    SampleCount,
    Rank,
}

impl RankLayerMethod {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Score => "score",
            Self::SampleCount => "sample_count",
            Self::Rank => "rank",
        }
    }

    pub fn from_str(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "score" | "score_range" | "by_score" => Ok(Self::Score),
            "sample_count" | "sample" | "count" | "by_sample_count" | "quantile" => {
                Ok(Self::SampleCount)
            }
            "rank" | "ranking" | "by_rank" => Ok(Self::Rank),
            other => Err(format!(
                "未知分层方法:{other}，可选值为 score、sample_count 或 rank"
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RankLayerConfig {
    pub min_samples_per_day: usize,
    pub backtest_period: usize,
    pub min_listed_trade_days: usize,
    pub layer_count: usize,
    pub layer_method: RankLayerMethod,
}

impl RankLayerConfig {
    fn validate(&self) -> Result<(), String> {
        if self.min_samples_per_day == 0 {
            return Err("每日最少样本数必须>=1".to_string());
        }
        if self.layer_count < 2 {
            return Err("分层层数必须>=2".to_string());
        }
        if self.layer_count > MAX_LAYER_COUNT {
            return Err(format!("分层层数不能超过{MAX_LAYER_COUNT}"));
        }
        if self.backtest_period == 0 {
            return Err("回测周期必须>=1".to_string());
        }
        Ok(())
    }

    pub fn effective_min_samples_per_day(&self) -> usize {
        self.min_samples_per_day.max(self.layer_count)
    }

    pub fn default_layer_count() -> usize {
        DEFAULT_LAYER_COUNT
    }

    fn as_rule_layer_config(&self) -> RuleLayerConfig {
        RuleLayerConfig {
            min_samples_per_day: self.min_samples_per_day,
            backtest_period: self.backtest_period,
            min_listed_trade_days: self.min_listed_trade_days,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RankLayerFromDbInput {
    pub stock_adj_type: String,
    pub index_ts_code: String,
    pub index_beta: f64,
    pub concept_beta: f64,
    pub industry_beta: f64,
    pub start_date: String,
    pub end_date: String,
    pub layer_config: RankLayerConfig,
}

impl RankLayerFromDbInput {
    fn validate(&self) -> Result<(), String> {
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

#[derive(Debug, Clone, PartialEq)]
pub struct RankLayerBucketPoint {
    pub layer_index: usize,
    pub sample_count: usize,
    pub avg_score: Option<f64>,
    pub avg_residual_return: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RankLayerPoint {
    pub trade_date: String,
    pub sample_count: usize,
    pub top_bottom_spread: Option<f64>,
    pub ic: Option<f64>,
    pub layers: Vec<RankLayerBucketPoint>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RankLayerSummaryBucket {
    pub layer_index: usize,
    pub point_count: usize,
    pub sample_count: usize,
    pub avg_score: Option<f64>,
    pub avg_residual_return: Option<f64>,
    pub avg_er_change: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RankLayerSamplePoint {
    pub layer_index: usize,
    pub ts_code: String,
    pub trade_date: String,
    pub score: f64,
    pub residual_return: f64,
    pub er_change: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RankTopKSummary {
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

#[derive(Debug, Clone, PartialEq)]
pub struct RankTopKPeriodSummary {
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

#[derive(Debug, Clone, PartialEq)]
pub struct RankLayerMetrics {
    pub points: Vec<RankLayerPoint>,
    pub point_count: usize,
    pub sample_count: usize,
    pub avg_er_change: Option<f64>,
    pub spread_mean: Option<f64>,
    pub ic_mean: Option<f64>,
    pub ic_std: Option<f64>,
    pub icir: Option<f64>,
    pub ic_t_value: Option<f64>,
    pub layers: Vec<RankLayerSummaryBucket>,
    pub layer_samples: Vec<RankLayerSamplePoint>,
    pub top_k_summaries: Vec<RankTopKSummary>,
    pub top_k_period_summaries: Vec<RankTopKPeriodSummary>,
}

#[derive(Debug, Default, Clone)]
struct RankLayerLookup {
    sample_ranks: HashMap<String, HashMap<String, i64>>,
    day_max_ranks: HashMap<String, i64>,
}

pub fn calc_rank_layer_metrics_from_db(
    source_conn: &Connection,
    source_dir: &str,
    input: &RankLayerFromDbInput,
) -> Result<RankLayerMetrics, String> {
    input.validate()?;

    let rule_layer_config = input.layer_config.as_rule_layer_config();
    let runtime_cache = build_rule_layer_runtime_cache(
        source_conn,
        source_dir,
        &input.stock_adj_type,
        &input.index_ts_code,
        input.index_beta,
        input.concept_beta,
        input.industry_beta,
        &input.start_date,
        &input.end_date,
        &rule_layer_config,
    )?;
    let (triggered_score_map, rank_lookup) =
        load_score_summary_data(source_dir, &input.start_date, &input.end_date)?;
    let all_samples = collect_all_rule_samples_from_cache(
        &runtime_cache,
        &triggered_score_map,
        &rule_layer_config,
    )?;

    calc_rank_layer_metrics_with_lookup(&all_samples, &input.layer_config, Some(&rank_lookup))
}

pub fn calc_rank_layer_metrics_from_score_rows(
    source_conn: &Connection,
    source_dir: &str,
    input: &RankLayerFromDbInput,
    score_summary_rows: &[ScoreSummary],
) -> Result<RankLayerMetrics, String> {
    input.validate()?;

    let rule_layer_config = input.layer_config.as_rule_layer_config();
    let runtime_cache = build_rule_layer_runtime_cache_from_summary_rows(
        source_conn,
        source_dir,
        score_summary_rows,
        &input.stock_adj_type,
        &input.index_ts_code,
        input.index_beta,
        input.concept_beta,
        input.industry_beta,
        &input.start_date,
        &input.end_date,
        &rule_layer_config,
    )?;
    let (triggered_score_map, rank_lookup) =
        build_score_summary_data_from_rows(score_summary_rows, &input.start_date, &input.end_date);
    let all_samples = collect_all_rule_samples_from_cache(
        &runtime_cache,
        &triggered_score_map,
        &rule_layer_config,
    )?;

    calc_rank_layer_metrics_with_lookup(&all_samples, &input.layer_config, Some(&rank_lookup))
}

pub fn calc_rank_layer_metrics(
    samples: &[RuleLayerSamplePoint],
    config: &RankLayerConfig,
) -> Result<RankLayerMetrics, String> {
    calc_rank_layer_metrics_with_lookup(samples, config, None)
}

pub fn calc_rank_layer_metrics_from_rank_samples(
    samples: &[RankLayerSamplePoint],
    config: &RankLayerConfig,
    score_summary_rows: &[ScoreSummary],
) -> Result<RankLayerMetrics, String> {
    let samples = samples
        .iter()
        .map(|sample| RuleLayerSamplePoint {
            ts_code: sample.ts_code.clone(),
            trade_date: sample.trade_date.clone(),
            rule_score: sample.score,
            residual_return: sample.residual_return,
            er_change: sample.er_change,
        })
        .collect::<Vec<_>>();
    let (_, rank_lookup) = build_score_summary_data_from_rows(
        score_summary_rows,
        score_summary_rows
            .iter()
            .map(|row| row.trade_date.as_str())
            .min()
            .unwrap_or(""),
        score_summary_rows
            .iter()
            .map(|row| row.trade_date.as_str())
            .max()
            .unwrap_or(""),
    );
    calc_rank_layer_metrics_with_lookup(&samples, config, Some(&rank_lookup))
}

fn calc_rank_layer_metrics_with_lookup(
    samples: &[RuleLayerSamplePoint],
    config: &RankLayerConfig,
    rank_lookup: Option<&RankLayerLookup>,
) -> Result<RankLayerMetrics, String> {
    config.validate()?;

    let (top_k_summaries, top_k_period_summaries) =
        calc_rank_top_k_summaries(samples, config, rank_lookup);

    let mut grouped_by_day: BTreeMap<&str, Vec<&RuleLayerSamplePoint>> = BTreeMap::new();
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

    let min_samples_per_day = config.effective_min_samples_per_day();
    let day_results = grouped_by_day
        .into_iter()
        .collect::<Vec<_>>()
        .into_par_iter()
        .map(|(trade_date, day_samples)| {
            calc_rank_layer_day(
                trade_date,
                day_samples,
                config,
                rank_lookup,
                min_samples_per_day,
            )
        })
        .collect::<Vec<_>>();

    let mut points = Vec::new();
    let mut spread_values = Vec::new();
    let mut ic_values = Vec::new();
    let mut total_sample_count = 0usize;
    let layer_count = config.layer_count;
    let mut layer_day_score_sums = vec![0.0_f64; layer_count];
    let mut layer_day_score_counts = vec![0usize; layer_count];
    let mut layer_day_return_sums = vec![0.0_f64; layer_count];
    let mut layer_day_return_counts = vec![0usize; layer_count];
    let mut layer_sample_counts = vec![0usize; layer_count];
    let mut layer_samples = Vec::new();

    for day_result in day_results.into_iter().flatten() {
        if let Some(value) = day_result.point.top_bottom_spread {
            spread_values.push(value);
        }
        if let Some(value) = day_result.point.ic {
            ic_values.push(value);
        }
        total_sample_count += day_result.point.sample_count;
        for index in 0..layer_count {
            if let Some(value) = day_result.layer_avg_scores[index] {
                layer_day_score_sums[index] += value;
                layer_day_score_counts[index] += 1;
            }
            if let Some(value) = day_result.layer_avg_returns[index] {
                layer_day_return_sums[index] += value;
                layer_day_return_counts[index] += 1;
            }
            layer_sample_counts[index] += day_result.layer_sample_counts[index];
        }
        layer_samples.extend(day_result.layer_samples);
        points.push(day_result.point);
    }

    let ic_mean = mean(&ic_values);
    let ic_std = sample_std(&ic_values);
    let icir = match (ic_mean, ic_std) {
        (Some(avg), Some(std)) if std.abs() >= EPS => Some(avg / std),
        _ => None,
    };

    let mut er_change_sum = 0.0;
    let mut er_change_count = 0usize;
    let mut layer_er_change_sums = vec![0.0_f64; layer_count];
    let mut layer_er_change_counts = vec![0usize; layer_count];
    for sample in &layer_samples {
        let er_change = sample.er_change;
        if !er_change.is_finite() {
            continue;
        }
        er_change_sum += er_change;
        er_change_count += 1;
        let layer_index = sample.layer_index.saturating_sub(1);
        if layer_index < layer_count {
            layer_er_change_sums[layer_index] += er_change;
            layer_er_change_counts[layer_index] += 1;
        }
    }

    Ok(RankLayerMetrics {
        point_count: points.len(),
        sample_count: total_sample_count,
        avg_er_change: (er_change_count > 0).then_some(er_change_sum / er_change_count as f64),
        spread_mean: mean(&spread_values),
        ic_mean,
        ic_std,
        icir,
        ic_t_value: calc_newey_west_t_value(&ic_values, config.backtest_period.saturating_sub(1)),
        points,
        layers: (0..layer_count)
            .map(|index| RankLayerSummaryBucket {
                layer_index: index + 1,
                point_count: layer_day_return_counts[index],
                sample_count: layer_sample_counts[index],
                avg_score: if layer_day_score_counts[index] == 0 {
                    None
                } else {
                    Some(layer_day_score_sums[index] / layer_day_score_counts[index] as f64)
                },
                avg_residual_return: if layer_day_return_counts[index] == 0 {
                    None
                } else {
                    Some(layer_day_return_sums[index] / layer_day_return_counts[index] as f64)
                },
                avg_er_change: if layer_er_change_counts[index] == 0 {
                    None
                } else {
                    Some(layer_er_change_sums[index] / layer_er_change_counts[index] as f64)
                },
            })
            .collect(),
        layer_samples,
        top_k_summaries,
        top_k_period_summaries,
    })
}

#[derive(Debug, Clone)]
struct RankTopKDailyPoint {
    trade_date: String,
    sample_count: usize,
    avg_residual_return: f64,
}

fn calc_rank_top_k_summaries(
    samples: &[RuleLayerSamplePoint],
    config: &RankLayerConfig,
    rank_lookup: Option<&RankLayerLookup>,
) -> (Vec<RankTopKSummary>, Vec<RankTopKPeriodSummary>) {
    let mut grouped_by_day: BTreeMap<&str, Vec<&RuleLayerSamplePoint>> = BTreeMap::new();
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

    let mut daily_by_k = DEFAULT_TOP_KS
        .iter()
        .copied()
        .map(|top_k| (top_k, Vec::<RankTopKDailyPoint>::new()))
        .collect::<BTreeMap<_, _>>();
    let min_samples_per_day = config.effective_min_samples_per_day();

    for (trade_date, mut day_samples) in grouped_by_day {
        if day_samples.len() < min_samples_per_day {
            continue;
        }
        day_samples.sort_by(|left, right| {
            let left_rank = rank_lookup.and_then(|lookup| {
                lookup
                    .sample_ranks
                    .get(&left.ts_code)
                    .and_then(|rows| rows.get(trade_date))
                    .copied()
            });
            let right_rank = rank_lookup.and_then(|lookup| {
                lookup
                    .sample_ranks
                    .get(&right.ts_code)
                    .and_then(|rows| rows.get(trade_date))
                    .copied()
            });
            match (left_rank, right_rank) {
                (Some(a), Some(b)) => a.cmp(&b),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => right
                    .rule_score
                    .total_cmp(&left.rule_score)
                    .then_with(|| left.ts_code.cmp(&right.ts_code)),
            }
        });

        for top_k in DEFAULT_TOP_KS {
            let selected_count = top_k.min(day_samples.len());
            if selected_count == 0 {
                continue;
            }
            let avg_residual_return = day_samples
                .iter()
                .take(selected_count)
                .map(|sample| sample.residual_return)
                .sum::<f64>()
                / selected_count as f64;
            daily_by_k
                .get_mut(&top_k)
                .expect("default Top-K bucket must exist")
                .push(RankTopKDailyPoint {
                    trade_date: trade_date.to_string(),
                    sample_count: selected_count,
                    avg_residual_return,
                });
        }
    }

    let mut summaries = Vec::with_capacity(DEFAULT_TOP_KS.len());
    let mut period_summaries = Vec::new();
    for top_k in DEFAULT_TOP_KS {
        let daily_points = daily_by_k.remove(&top_k).unwrap_or_default();
        summaries.push(build_rank_top_k_summary(
            top_k,
            &daily_points,
            config.backtest_period,
        ));

        let mut by_period: BTreeMap<String, Vec<RankTopKDailyPoint>> = BTreeMap::new();
        for point in daily_points {
            let period_label = point
                .trade_date
                .get(..4)
                .filter(|value| value.chars().all(|ch| ch.is_ascii_digit()))
                .unwrap_or("其他")
                .to_string();
            by_period.entry(period_label).or_default().push(point);
        }
        for (period_label, points) in by_period {
            let summary = build_rank_top_k_summary(top_k, &points, config.backtest_period);
            period_summaries.push(RankTopKPeriodSummary {
                period_label,
                start_date: points
                    .first()
                    .map(|point| point.trade_date.clone())
                    .unwrap_or_default(),
                end_date: points
                    .last()
                    .map(|point| point.trade_date.clone())
                    .unwrap_or_default(),
                top_k,
                point_count: summary.point_count,
                sample_count: summary.sample_count,
                avg_daily_residual_return: summary.avg_daily_residual_return,
                median_daily_residual_return: summary.median_daily_residual_return,
                positive_day_ratio: summary.positive_day_ratio,
                hac_t_value: summary.hac_t_value,
                hac_lag: summary.hac_lag,
            });
        }
    }

    (summaries, period_summaries)
}

fn build_rank_top_k_summary(
    top_k: usize,
    daily_points: &[RankTopKDailyPoint],
    backtest_period: usize,
) -> RankTopKSummary {
    let daily_returns = daily_points
        .iter()
        .map(|point| point.avg_residual_return)
        .collect::<Vec<_>>();
    let positive_days = daily_returns.iter().filter(|value| **value > 0.0).count();
    let hac_lag = backtest_period
        .saturating_sub(1)
        .min(daily_returns.len().saturating_sub(1));

    RankTopKSummary {
        top_k,
        point_count: daily_points.len(),
        sample_count: daily_points.iter().map(|point| point.sample_count).sum(),
        avg_daily_residual_return: mean(&daily_returns),
        median_daily_residual_return: median(&daily_returns),
        positive_day_ratio: (!daily_returns.is_empty())
            .then_some(positive_days as f64 / daily_returns.len() as f64),
        daily_std: sample_std(&daily_returns),
        hac_t_value: calc_newey_west_t_value(&daily_returns, hac_lag),
        hac_lag,
    }
}

fn median(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut ordered = values
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    if ordered.is_empty() {
        return None;
    }
    ordered.sort_by(f64::total_cmp);
    let middle = ordered.len() / 2;
    if ordered.len() % 2 == 0 {
        Some((ordered[middle - 1] + ordered[middle]) / 2.0)
    } else {
        Some(ordered[middle])
    }
}

struct RankLayerDayResult {
    point: RankLayerPoint,
    layer_avg_scores: Vec<Option<f64>>,
    layer_avg_returns: Vec<Option<f64>>,
    layer_sample_counts: Vec<usize>,
    layer_samples: Vec<RankLayerSamplePoint>,
}

fn calc_rank_layer_day(
    trade_date: &str,
    day_samples: Vec<&RuleLayerSamplePoint>,
    config: &RankLayerConfig,
    rank_lookup: Option<&RankLayerLookup>,
    min_samples_per_day: usize,
) -> Option<RankLayerDayResult> {
    if day_samples.len() < min_samples_per_day {
        return None;
    }

    let mut ordered = day_samples
        .iter()
        .map(|sample| (sample.rule_score, sample.residual_return))
        .enumerate()
        .collect::<Vec<_>>();
    ordered.sort_by(|left, right| {
        left.1
            .0
            .partial_cmp(&right.1.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.0.cmp(&right.0))
    });

    let layer_count = config.layer_count;
    let layer_sample_indices_by_index =
        build_layer_sample_indices(trade_date, &day_samples, &ordered, config, rank_lookup);
    let mut layers = Vec::with_capacity(layer_count);
    let mut layer_avg_scores = vec![None; layer_count];
    let mut layer_avg_returns = vec![None; layer_count];
    let mut layer_sample_counts = vec![0; layer_count];
    let mut layer_samples = Vec::with_capacity(day_samples.len());
    let mut scores = Vec::with_capacity(day_samples.len());
    let mut residuals = Vec::with_capacity(day_samples.len());

    for sample in &day_samples {
        scores.push(sample.rule_score);
        residuals.push(sample.residual_return);
    }

    for (layer_index, layer_sample_indices) in layer_sample_indices_by_index.into_iter().enumerate()
    {
        let mut score_sum = 0.0;
        let mut residual_sum = 0.0;
        for sample_index in &layer_sample_indices {
            let sample = day_samples[*sample_index];
            score_sum += sample.rule_score;
            residual_sum += sample.residual_return;
            layer_samples.push(RankLayerSamplePoint {
                layer_index: layer_index + 1,
                ts_code: sample.ts_code.clone(),
                trade_date: trade_date.to_string(),
                score: sample.rule_score,
                residual_return: sample.residual_return,
                er_change: sample.er_change,
            });
        }

        let sample_count = layer_sample_indices.len();
        let avg_score = (sample_count > 0).then_some(score_sum / sample_count as f64);
        let avg_residual_return = (sample_count > 0).then_some(residual_sum / sample_count as f64);
        layer_avg_scores[layer_index] = avg_score;
        layer_avg_returns[layer_index] = avg_residual_return;
        layer_sample_counts[layer_index] = sample_count;
        layers.push(RankLayerBucketPoint {
            layer_index: layer_index + 1,
            sample_count,
            avg_score,
            avg_residual_return,
        });
    }

    let top_bottom_spread = match (layer_avg_returns[0], layer_avg_returns[layer_count - 1]) {
        (Some(low), Some(high)) => Some(high - low),
        _ => None,
    };
    let ic = spearman_corr(&scores, &residuals);

    Some(RankLayerDayResult {
        point: RankLayerPoint {
            trade_date: trade_date.to_string(),
            sample_count: day_samples.len(),
            top_bottom_spread,
            ic,
            layers,
        },
        layer_avg_scores,
        layer_avg_returns,
        layer_sample_counts,
        layer_samples,
    })
}

fn build_layer_sample_indices(
    trade_date: &str,
    day_samples: &[&RuleLayerSamplePoint],
    ordered: &[(usize, (f64, f64))],
    config: &RankLayerConfig,
    rank_lookup: Option<&RankLayerLookup>,
) -> Vec<Vec<usize>> {
    match config.layer_method {
        RankLayerMethod::Score => {
            build_score_range_layer_sample_indices(ordered, config.layer_count)
        }
        RankLayerMethod::SampleCount => build_sample_count_layer_sample_indices(
            trade_date,
            day_samples,
            ordered,
            config.layer_count,
            rank_lookup,
        ),
        RankLayerMethod::Rank => build_rank_layer_sample_indices(
            trade_date,
            day_samples,
            ordered,
            config.layer_count,
            rank_lookup,
        ),
    }
}

fn build_sample_count_layer_sample_indices(
    trade_date: &str,
    day_samples: &[&RuleLayerSamplePoint],
    ordered: &[(usize, (f64, f64))],
    layer_count: usize,
    rank_lookup: Option<&RankLayerLookup>,
) -> Vec<Vec<usize>> {
    let mut layers = vec![Vec::new(); layer_count];
    if ordered.is_empty() || layer_count == 0 {
        return layers;
    }

    let mut ranked = ordered.to_vec();
    ranked.sort_by(|left, right| {
        left.1
            .0
            .partial_cmp(&right.1.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                // 数据库 rank=1 表示最高排名。总分从低到高分层时，同分股票按
                // 数据库排名从低到高排列，因此较大的 rank 先进入低层。
                let left_rank = sample_database_rank(trade_date, day_samples[left.0], rank_lookup);
                let right_rank =
                    sample_database_rank(trade_date, day_samples[right.0], rank_lookup);
                right_rank.cmp(&left_rank)
            })
            .then_with(|| left.0.cmp(&right.0))
    });

    for (ordered_index, (sample_index, _)) in ranked.iter().enumerate() {
        let layer_index = ordered_index * layer_count / ranked.len();
        layers[layer_index].push(*sample_index);
    }

    layers
}

fn build_score_range_layer_sample_indices(
    ordered: &[(usize, (f64, f64))],
    layer_count: usize,
) -> Vec<Vec<usize>> {
    let mut layers = vec![Vec::new(); layer_count];
    if ordered.is_empty() || layer_count == 0 {
        return layers;
    }

    let min_score = ordered.first().map(|(_, pair)| pair.0).unwrap_or(0.0);
    let max_score = ordered.last().map(|(_, pair)| pair.0).unwrap_or(min_score);
    let score_span = max_score - min_score;
    if score_span.abs() < EPS {
        layers[0].extend(ordered.iter().map(|(sample_index, _)| *sample_index));
        return layers;
    }

    for (sample_index, pair) in ordered {
        let ratio = ((pair.0 - min_score) / score_span).clamp(0.0, 1.0);
        let mut layer_index = (ratio * layer_count as f64).floor() as usize;
        if layer_index >= layer_count {
            layer_index = layer_count - 1;
        }
        layers[layer_index].push(*sample_index);
    }

    layers
}

fn build_rank_layer_sample_indices(
    trade_date: &str,
    day_samples: &[&RuleLayerSamplePoint],
    ordered: &[(usize, (f64, f64))],
    layer_count: usize,
    rank_lookup: Option<&RankLayerLookup>,
) -> Vec<Vec<usize>> {
    let mut layers = vec![Vec::new(); layer_count];
    if ordered.is_empty() || layer_count == 0 {
        return layers;
    }

    let fallback_day_max_rank = ordered.len() as i64;
    let day_max_rank = rank_lookup
        .and_then(|lookup| lookup.day_max_ranks.get(trade_date).copied())
        .filter(|value| *value > 0)
        .unwrap_or(fallback_day_max_rank);

    for (ordered_index, (sample_index, _)) in ordered.iter().enumerate() {
        let fallback_rank = ordered.len() as i64 - ordered_index as i64;
        let sample_rank = sample_database_rank(trade_date, day_samples[*sample_index], rank_lookup)
            .unwrap_or(fallback_rank)
            .clamp(1, day_max_rank);
        let position_from_low = day_max_rank - sample_rank + 1;
        let layer_index = ((position_from_low * layer_count as i64 - 1) / day_max_rank) as usize;
        layers[layer_index].push(*sample_index);
    }

    layers
}

fn sample_database_rank(
    trade_date: &str,
    sample: &RuleLayerSamplePoint,
    rank_lookup: Option<&RankLayerLookup>,
) -> Option<i64> {
    rank_lookup?
        .sample_ranks
        .get(&sample.ts_code)?
        .get(trade_date)
        .copied()
        .filter(|rank| *rank > 0)
}

fn load_score_summary_data(
    source_dir: &str,
    start_date: &str,
    end_date: &str,
) -> Result<(HashMap<String, HashMap<String, f64>>, RankLayerLookup), String> {
    let result_db = result_db_path(source_dir);
    if !result_db.exists() {
        return Ok((HashMap::new(), RankLayerLookup::default()));
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
                trade_date,
                TRY_CAST(total_score AS DOUBLE) AS total_score,
                rank
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

    let mut out = HashMap::<String, HashMap<String, f64>>::new();
    let mut lookup = RankLayerLookup::default();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取score_summary失败:{e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取ts_code失败:{e}"))?;
        let trade_date: String = row.get(1).map_err(|e| format!("读取trade_date失败:{e}"))?;
        let total_score: Option<f64> =
            row.get(2).map_err(|e| format!("读取total_score失败:{e}"))?;
        let rank: Option<i64> = row.get(3).map_err(|e| format!("读取rank失败:{e}"))?;

        if ts_code.trim().is_empty() || trade_date.trim().is_empty() {
            continue;
        }

        if let Some(total_score) = total_score.filter(|value| value.is_finite()) {
            out.entry(ts_code.clone())
                .or_default()
                .insert(trade_date.clone(), total_score);
        }

        if let Some(rank) = rank.filter(|value| *value > 0) {
            lookup
                .sample_ranks
                .entry(ts_code)
                .or_default()
                .insert(trade_date.clone(), rank);
            lookup
                .day_max_ranks
                .entry(trade_date)
                .and_modify(|current| *current = (*current).max(rank))
                .or_insert(rank);
        }
    }

    Ok((out, lookup))
}

fn build_score_summary_data_from_rows(
    score_summary_rows: &[ScoreSummary],
    start_date: &str,
    end_date: &str,
) -> (HashMap<String, HashMap<String, f64>>, RankLayerLookup) {
    let mut out = HashMap::<String, HashMap<String, f64>>::new();
    let mut lookup = RankLayerLookup::default();
    for row in score_summary_rows {
        if row.trade_date.as_str() < start_date
            || row.trade_date.as_str() > end_date
            || row.ts_code.trim().is_empty()
            || row.trade_date.trim().is_empty()
        {
            continue;
        }

        if row.total_score.is_finite() {
            out.entry(row.ts_code.clone())
                .or_default()
                .insert(row.trade_date.clone(), row.total_score);
        }

        if let Some(rank) = row.rank.filter(|value| *value > 0) {
            lookup
                .sample_ranks
                .entry(row.ts_code.clone())
                .or_default()
                .insert(row.trade_date.clone(), rank);
            lookup
                .day_max_ranks
                .entry(row.trade_date.clone())
                .and_modify(|current| *current = (*current).max(rank))
                .or_insert(rank);
        }
    }
    (out, lookup)
}

fn mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    Some(values.iter().sum::<f64>() / values.len() as f64)
}

fn sample_std(values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    let avg = mean(values)?;
    let var = values
        .iter()
        .map(|v| {
            let delta = *v - avg;
            delta * delta
        })
        .sum::<f64>()
        / (values.len() as f64 - 1.0);
    Some(var.sqrt())
}

fn spearman_corr(x: &[f64], y: &[f64]) -> Option<f64> {
    if x.len() != y.len() || x.len() < 2 {
        return None;
    }
    let xr = average_ranks(x);
    let yr = average_ranks(y);
    pearson_corr(&xr, &yr)
}

fn average_ranks(values: &[f64]) -> Vec<f64> {
    let mut indexed = values.iter().copied().enumerate().collect::<Vec<_>>();
    indexed.sort_by(|left, right| {
        left.1
            .partial_cmp(&right.1)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut ranks = vec![0.0; values.len()];
    let mut index = 0usize;
    while index < indexed.len() {
        let mut next = index + 1;
        while next < indexed.len() && (indexed[next].1 - indexed[index].1).abs() < EPS {
            next += 1;
        }

        let avg_rank = (index + 1 + next) as f64 / 2.0;
        for item in &indexed[index..next] {
            ranks[item.0] = avg_rank;
        }
        index = next;
    }

    ranks
}

fn pearson_corr(x: &[f64], y: &[f64]) -> Option<f64> {
    if x.len() != y.len() || x.len() < 2 {
        return None;
    }

    let mean_x = mean(x)?;
    let mean_y = mean(y)?;
    let mut covariance = 0.0;
    let mut variance_x = 0.0;
    let mut variance_y = 0.0;

    for (vx, vy) in x.iter().zip(y.iter()) {
        let dx = *vx - mean_x;
        let dy = *vy - mean_y;
        covariance += dx * dy;
        variance_x += dx * dx;
        variance_y += dy * dy;
    }

    if variance_x <= EPS || variance_y <= EPS {
        return None;
    }

    Some(covariance / (variance_x.sqrt() * variance_y.sqrt()))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{
        RankLayerConfig, RankLayerLookup, RankLayerMethod, calc_rank_layer_metrics,
        calc_rank_layer_metrics_with_lookup,
    };
    use crate::simulate::rule::RuleLayerSamplePoint;

    fn assert_opt_close(left: Option<f64>, right: Option<f64>) {
        match (left, right) {
            (Some(a), Some(b)) => assert!((a - b).abs() < 1e-9, "left={a}, right={b}"),
            (None, None) => {}
            _ => panic!("left={left:?}, right={right:?}"),
        }
    }

    #[test]
    fn rank_layer_metrics_use_five_score_buckets() {
        let samples = vec![
            RuleLayerSamplePoint {
                ts_code: "000001.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 1.0,
                residual_return: 10.0,
                er_change: 0.1,
            },
            RuleLayerSamplePoint {
                ts_code: "000002.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 2.0,
                residual_return: 20.0,
                er_change: 0.2,
            },
            RuleLayerSamplePoint {
                ts_code: "000003.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 3.0,
                residual_return: 30.0,
                er_change: 0.3,
            },
            RuleLayerSamplePoint {
                ts_code: "000004.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 4.0,
                residual_return: 40.0,
                er_change: 0.4,
            },
            RuleLayerSamplePoint {
                ts_code: "000005.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 5.0,
                residual_return: 50.0,
                er_change: 0.5,
            },
        ];

        let metrics = calc_rank_layer_metrics(
            &samples,
            &RankLayerConfig {
                min_samples_per_day: 5,
                backtest_period: 1,
                min_listed_trade_days: 0,
                layer_count: 5,
                layer_method: RankLayerMethod::SampleCount,
            },
        )
        .expect("rank metrics should build");

        assert_eq!(metrics.point_count, 1);
        assert_eq!(metrics.sample_count, 5);
        assert_opt_close(metrics.avg_er_change, Some(0.3));
        assert_opt_close(metrics.spread_mean, Some(40.0));
        assert_opt_close(metrics.ic_mean, Some(1.0));
        assert_eq!(metrics.layers.len(), 5);
        assert_eq!(metrics.layers[0].sample_count, 1);
        assert_opt_close(metrics.layers[0].avg_residual_return, Some(10.0));
        assert_opt_close(metrics.layers[0].avg_er_change, Some(0.1));
        assert_eq!(metrics.layers[4].sample_count, 1);
        assert_opt_close(metrics.layers[4].avg_residual_return, Some(50.0));
        assert_opt_close(metrics.layers[4].avg_er_change, Some(0.5));
    }

    #[test]
    fn rank_layer_metrics_support_score_range_buckets() {
        let samples = vec![
            RuleLayerSamplePoint {
                ts_code: "000001.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 0.0,
                residual_return: 10.0,
                er_change: f64::INFINITY,
            },
            RuleLayerSamplePoint {
                ts_code: "000002.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 10.0,
                residual_return: 20.0,
                er_change: f64::INFINITY,
            },
            RuleLayerSamplePoint {
                ts_code: "000003.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 20.0,
                residual_return: 30.0,
                er_change: f64::INFINITY,
            },
            RuleLayerSamplePoint {
                ts_code: "000004.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 30.0,
                residual_return: 40.0,
                er_change: f64::INFINITY,
            },
        ];

        let metrics = calc_rank_layer_metrics(
            &samples,
            &RankLayerConfig {
                min_samples_per_day: 1,
                backtest_period: 1,
                min_listed_trade_days: 0,
                layer_count: 3,
                layer_method: RankLayerMethod::Score,
            },
        )
        .expect("rank metrics should build");

        assert_eq!(metrics.layers.len(), 3);
        assert_eq!(metrics.layers[0].sample_count, 1);
        assert_eq!(metrics.layers[1].sample_count, 1);
        assert_eq!(metrics.layers[2].sample_count, 2);
        assert_opt_close(metrics.layers[0].avg_score, Some(0.0));
        assert_opt_close(metrics.layers[2].avg_residual_return, Some(35.0));
        assert_opt_close(metrics.spread_mean, Some(25.0));
    }

    #[test]
    fn rank_layer_metrics_support_rank_buckets() {
        let samples = vec![
            RuleLayerSamplePoint {
                ts_code: "000001.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 10.0,
                residual_return: 1.0,
                er_change: f64::INFINITY,
            },
            RuleLayerSamplePoint {
                ts_code: "000002.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 20.0,
                residual_return: 2.0,
                er_change: f64::INFINITY,
            },
            RuleLayerSamplePoint {
                ts_code: "000003.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 30.0,
                residual_return: 3.0,
                er_change: f64::INFINITY,
            },
            RuleLayerSamplePoint {
                ts_code: "000004.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 40.0,
                residual_return: 4.0,
                er_change: f64::INFINITY,
            },
        ];

        let metrics = calc_rank_layer_metrics(
            &samples,
            &RankLayerConfig {
                min_samples_per_day: 1,
                backtest_period: 1,
                min_listed_trade_days: 0,
                layer_count: 2,
                layer_method: RankLayerMethod::Rank,
            },
        )
        .expect("rank metrics should build");

        assert_eq!(metrics.layers.len(), 2);
        assert_eq!(metrics.layers[0].sample_count, 2);
        assert_eq!(metrics.layers[1].sample_count, 2);
        assert_opt_close(metrics.layers[0].avg_score, Some(15.0));
        assert_opt_close(metrics.layers[1].avg_score, Some(35.0));
        assert_opt_close(metrics.spread_mean, Some(2.0));
    }

    #[test]
    fn sample_count_layers_use_database_rank_to_split_equal_scores() {
        let samples = (0..4)
            .map(|index| RuleLayerSamplePoint {
                ts_code: format!("{index:06}.SZ"),
                trade_date: "20240102".to_string(),
                rule_score: 10.0,
                residual_return: index as f64,
                er_change: f64::INFINITY,
            })
            .collect::<Vec<_>>();
        let rank_lookup = RankLayerLookup {
            sample_ranks: HashMap::from([
                (
                    "000000.SZ".to_string(),
                    HashMap::from([("20240102".to_string(), 1)]),
                ),
                (
                    "000001.SZ".to_string(),
                    HashMap::from([("20240102".to_string(), 4)]),
                ),
                (
                    "000002.SZ".to_string(),
                    HashMap::from([("20240102".to_string(), 2)]),
                ),
                (
                    "000003.SZ".to_string(),
                    HashMap::from([("20240102".to_string(), 3)]),
                ),
            ]),
            day_max_ranks: HashMap::from([("20240102".to_string(), 4)]),
        };

        let metrics = calc_rank_layer_metrics_with_lookup(
            &samples,
            &RankLayerConfig {
                min_samples_per_day: 1,
                backtest_period: 1,
                min_listed_trade_days: 0,
                layer_count: 2,
                layer_method: RankLayerMethod::SampleCount,
            },
            Some(&rank_lookup),
        )
        .expect("rank metrics should build");

        assert_eq!(metrics.layers[0].sample_count, 2);
        assert_eq!(metrics.layers[1].sample_count, 2);
        assert_opt_close(metrics.layers[0].avg_residual_return, Some(2.0));
        assert_opt_close(metrics.layers[1].avg_residual_return, Some(1.0));
    }

    #[test]
    fn rank_layers_use_database_rank() {
        let samples = vec![
            RuleLayerSamplePoint {
                ts_code: "000001.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 10.0,
                residual_return: 1.0,
                er_change: f64::INFINITY,
            },
            RuleLayerSamplePoint {
                ts_code: "000002.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 20.0,
                residual_return: 2.0,
                er_change: f64::INFINITY,
            },
            RuleLayerSamplePoint {
                ts_code: "000003.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 30.0,
                residual_return: 3.0,
                er_change: f64::INFINITY,
            },
            RuleLayerSamplePoint {
                ts_code: "000004.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 40.0,
                residual_return: 4.0,
                er_change: f64::INFINITY,
            },
        ];
        let stale_lookup = RankLayerLookup {
            sample_ranks: HashMap::from([
                (
                    "000001.SZ".to_string(),
                    HashMap::from([("20240102".to_string(), 1)]),
                ),
                (
                    "000002.SZ".to_string(),
                    HashMap::from([("20240102".to_string(), 100)]),
                ),
                (
                    "000003.SZ".to_string(),
                    HashMap::from([("20240102".to_string(), 500)]),
                ),
                (
                    "000004.SZ".to_string(),
                    HashMap::from([("20240102".to_string(), 1000)]),
                ),
            ]),
            day_max_ranks: HashMap::from([("20240102".to_string(), 1000)]),
        };

        let metrics = calc_rank_layer_metrics_with_lookup(
            &samples,
            &RankLayerConfig {
                min_samples_per_day: 1,
                backtest_period: 1,
                min_listed_trade_days: 0,
                layer_count: 2,
                layer_method: RankLayerMethod::Rank,
            },
            Some(&stale_lookup),
        )
        .expect("rank metrics should build");

        assert_eq!(metrics.layers[0].sample_count, 1);
        assert_eq!(metrics.layers[1].sample_count, 3);
        assert_opt_close(metrics.layers[0].avg_score, Some(40.0));
        assert_opt_close(metrics.layers[1].avg_score, Some(20.0));
    }

    #[test]
    fn rank_metrics_report_exact_top_k_and_year_stability() {
        let samples = [
            ("20240102", "000001.SZ", 30.0, 2.0),
            ("20240102", "000002.SZ", 20.0, -1.0),
            ("20240102", "000003.SZ", 10.0, 0.5),
            ("20250102", "000001.SZ", 30.0, 1.0),
            ("20250102", "000002.SZ", 20.0, 0.0),
            ("20250102", "000003.SZ", 10.0, -0.5),
        ]
        .into_iter()
        .map(
            |(trade_date, ts_code, rule_score, residual_return)| RuleLayerSamplePoint {
                ts_code: ts_code.to_string(),
                trade_date: trade_date.to_string(),
                rule_score,
                residual_return,
                er_change: f64::INFINITY,
            },
        )
        .collect::<Vec<_>>();
        let rank_lookup = RankLayerLookup {
            sample_ranks: HashMap::from([
                (
                    "000001.SZ".to_string(),
                    HashMap::from([("20240102".to_string(), 2), ("20250102".to_string(), 2)]),
                ),
                (
                    "000002.SZ".to_string(),
                    HashMap::from([("20240102".to_string(), 3), ("20250102".to_string(), 3)]),
                ),
                (
                    "000003.SZ".to_string(),
                    HashMap::from([("20240102".to_string(), 1), ("20250102".to_string(), 1)]),
                ),
            ]),
            day_max_ranks: HashMap::from([
                ("20240102".to_string(), 3),
                ("20250102".to_string(), 3),
            ]),
        };

        let metrics = calc_rank_layer_metrics_with_lookup(
            &samples,
            &RankLayerConfig {
                min_samples_per_day: 1,
                backtest_period: 3,
                min_listed_trade_days: 0,
                layer_count: 2,
                layer_method: RankLayerMethod::Rank,
            },
            Some(&rank_lookup),
        )
        .expect("rank metrics should build");

        let top_one = metrics
            .top_k_summaries
            .iter()
            .find(|item| item.top_k == 1)
            .expect("Top 1 summary");
        assert_eq!(top_one.point_count, 2);
        assert_eq!(top_one.sample_count, 2);
        // Top-K 应遵循数据库排名，而不是再次按分数排序；这里最低分股票 rank=1。
        assert_opt_close(top_one.avg_daily_residual_return, Some(0.0));
        assert_opt_close(top_one.median_daily_residual_return, Some(0.0));
        assert_opt_close(top_one.positive_day_ratio, Some(0.5));
        assert_eq!(top_one.hac_lag, 1);

        let top_five = metrics
            .top_k_summaries
            .iter()
            .find(|item| item.top_k == 5)
            .expect("Top 5 summary");
        assert_eq!(top_five.sample_count, 6);
        assert_eq!(metrics.top_k_period_summaries.len(), 8);
        assert!(
            metrics
                .top_k_period_summaries
                .iter()
                .any(|item| item.period_label == "2024" && item.top_k == 1)
        );
        assert!(
            metrics
                .top_k_period_summaries
                .iter()
                .any(|item| item.period_label == "2025" && item.top_k == 1)
        );
    }
}
