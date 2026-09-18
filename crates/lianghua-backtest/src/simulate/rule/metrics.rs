//! 分层指标计算：从触发分数或运行时缓存得到各层 metrics。

use crate::simulate::rule::cache::build_rule_layer_runtime_cache;
use crate::simulate::rule::{
    RuleDayGroup, RuleLayerCollectOptions, RuleLayerComputation, RuleLayerConfig,
    RuleLayerDailyScoreGroup, RuleLayerDailyScoreLayers, RuleLayerMetrics,
    RuleLayerMetricsWithSamples, RuleLayerMetricsWithTriggeredSamples,
    RuleLayerMetricsWithValidation, RuleLayerPoint, RuleLayerRuntimeCache, RuleLayerSamplePoint,
    RuleSample, TriggeredScoreColumn,
};

#[cfg(test)]
use crate::simulate::rule::{RuleBacktestOutcome, RuleDayBaseSample, RuleUniverseRow};
#[cfg(test)]
use std::sync::Arc;

use crate::simulate::fp_utils::EPS;
use crate::simulate::fp_utils::ProfitLossSums;
use crate::simulate::fp_utils::calc_newey_west_t_value;
use crate::simulate::fp_utils::calc_profit_loss_sums;
use crate::simulate::fp_utils::calc_score_weighted_return;
use crate::simulate::fp_utils::calc_top_bottom_spread;
use crate::simulate::fp_utils::mean;
use crate::simulate::fp_utils::sample_std;
use crate::simulate::fp_utils::spearman_corr;
use duckdb::Connection;
use rayon::prelude::*;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::HashMap;
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
    let triggered_scores = runtime_cache.encode_triggered_scores(triggered_score_map);
    calc_rule_layer_metrics_with_samples_from_score_column(
        runtime_cache,
        &triggered_scores,
        layer_config,
    )
}

pub(super) fn calc_rule_layer_metrics_with_samples_from_score_column(
    runtime_cache: &RuleLayerRuntimeCache,
    triggered_scores: &TriggeredScoreColumn,
    layer_config: &RuleLayerConfig,
) -> Result<RuleLayerMetricsWithSamples, String> {
    let computation = compute_rule_layer_from_runtime_cache(
        runtime_cache,
        Some(triggered_scores),
        layer_config,
        RuleLayerCollectOptions {
            metrics: true,
            all_samples: true,
            triggered_samples: false,
            validation_details: false,
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
    let triggered_scores = runtime_cache.encode_triggered_scores(triggered_score_map);
    let computation = compute_rule_layer_from_runtime_cache(
        runtime_cache,
        Some(&triggered_scores),
        layer_config,
        RuleLayerCollectOptions {
            metrics: true,
            all_samples: false,
            triggered_samples: true,
            validation_details: false,
        },
    )?;

    Ok(RuleLayerMetricsWithTriggeredSamples {
        metrics: computation.metrics,
        triggered_samples: computation.triggered_samples,
    })
}

pub fn calc_rule_layer_metrics_with_validation_from_cache(
    runtime_cache: &RuleLayerRuntimeCache,
    triggered_score_map: &HashMap<String, HashMap<String, f64>>,
    layer_config: &RuleLayerConfig,
) -> Result<RuleLayerMetricsWithValidation, String> {
    let triggered_scores = runtime_cache.encode_triggered_scores(triggered_score_map);
    calc_rule_layer_metrics_with_validation_from_score_column(
        runtime_cache,
        &triggered_scores,
        layer_config,
    )
}

pub(super) fn calc_rule_layer_metrics_with_validation_from_score_column(
    runtime_cache: &RuleLayerRuntimeCache,
    triggered_scores: &TriggeredScoreColumn,
    layer_config: &RuleLayerConfig,
) -> Result<RuleLayerMetricsWithValidation, String> {
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

    Ok(RuleLayerMetricsWithValidation {
        metrics: computation.metrics,
        triggered_samples: computation.triggered_samples,
        daily_score_layers: computation.daily_score_layers,
        return_distribution_counts: computation.return_distribution_counts,
    })
}

pub fn calc_rule_layer_metrics_from_cache(
    runtime_cache: &RuleLayerRuntimeCache,
    triggered_score_map: &HashMap<String, HashMap<String, f64>>,
    layer_config: &RuleLayerConfig,
) -> Result<RuleLayerMetrics, String> {
    let triggered_scores = runtime_cache.encode_triggered_scores(triggered_score_map);
    calc_rule_layer_metrics_from_score_column(runtime_cache, &triggered_scores, layer_config)
}

pub(super) fn calc_rule_layer_metrics_from_score_column(
    runtime_cache: &RuleLayerRuntimeCache,
    triggered_scores: &TriggeredScoreColumn,
    layer_config: &RuleLayerConfig,
) -> Result<RuleLayerMetrics, String> {
    Ok(compute_rule_layer_from_runtime_cache(
        runtime_cache,
        Some(triggered_scores),
        layer_config,
        RuleLayerCollectOptions {
            metrics: true,
            all_samples: false,
            triggered_samples: false,
            validation_details: false,
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
    let triggered_scores = runtime_cache.encode_triggered_scores(triggered_score_map);
    for day_group in &runtime_cache.day_groups {
        for sample in &day_group.samples {
            let Some(rule_score) =
                triggered_scores.get(runtime_cache.score_index(day_group, sample))
            else {
                continue;
            };

            visit(RuleLayerSamplePointRef {
                ts_code: runtime_cache.ts_code(sample.ts_code_id),
                trade_date: day_group.trade_date.as_ref(),
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
    let triggered_scores = runtime_cache.encode_triggered_scores(triggered_score_map);
    compute_rule_layer_from_runtime_cache(
        runtime_cache,
        Some(&triggered_scores),
        &RuleLayerConfig::default(),
        RuleLayerCollectOptions {
            metrics: false,
            all_samples: false,
            triggered_samples: true,
            validation_details: false,
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
    let triggered_scores = runtime_cache.encode_triggered_scores(triggered_score_map);
    Ok(compute_rule_layer_from_runtime_cache(
        runtime_cache,
        Some(&triggered_scores),
        layer_config,
        RuleLayerCollectOptions {
            metrics: false,
            all_samples: true,
            triggered_samples: false,
            validation_details: false,
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
            let score_weighted_residual_return =
                calc_score_weighted_return(&rule_scores, &residuals);
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
                    score_weighted_residual_return,
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
    let ic_t_value = calc_newey_west_t_value(&ic_values, config.backtest_period.saturating_sub(1));

    Ok(RuleLayerMetrics {
        points,
        avg_residual_mean,
        avg_excess_residual_mean,
        avg_er_change: None,
        er_change_sample_count: 0,
        profit_loss_ratio,
        spread_mean,
        ic_mean,
        ic_std,
        icir,
        ic_t_value,
    })
}

pub(super) fn compute_rule_layer_from_runtime_cache(
    runtime_cache: &RuleLayerRuntimeCache,
    triggered_scores: Option<&TriggeredScoreColumn>,
    config: &RuleLayerConfig,
    collect_options: RuleLayerCollectOptions,
) -> Result<RuleLayerComputation, String> {
    config.validate()?;

    if runtime_cache.day_groups.is_empty() {
        return Ok(RuleLayerComputation {
            metrics: empty_metrics(),
            all_samples: Vec::new(),
            triggered_samples: Vec::new(),
            daily_score_layers: Vec::new(),
            return_distribution_counts: [0; 7],
        });
    }

    // 使用 fold+reduce 消除中间 day_results Vec，避免全部交易日的
    // all_samples / triggered_samples 同时占据内存。
    let identity = || DayGroupsFoldAccum::default();
    let accum = runtime_cache
        .day_groups
        .par_iter()
        .fold(identity, |mut acc, day_group| {
            acc.process_day_group(
                runtime_cache,
                day_group,
                triggered_scores,
                config,
                collect_options,
            );
            acc
        })
        .reduce(identity, |mut a, b| {
            a.merge(b);
            a
        });

    let avg_residual_mean = mean(&accum.avg_residual_values);
    let avg_excess_residual_mean = mean(&accum.avg_excess_residual_values);
    let avg_er_change = mean(&accum.er_change_values);
    let er_change_sample_count = accum.er_change_values.len();
    let profit_loss_ratio = accum.profit_loss_sums.ratio();
    let spread_mean = mean(&accum.spread_values);
    let ic_mean = mean(&accum.ic_values);
    let ic_std = sample_std(&accum.ic_values);
    let icir = match (ic_mean, ic_std) {
        (Some(m), Some(s)) if s.abs() >= EPS => Some(m / s),
        _ => None,
    };
    let ic_t_value =
        calc_newey_west_t_value(&accum.ic_values, config.backtest_period.saturating_sub(1));

    Ok(RuleLayerComputation {
        metrics: RuleLayerMetrics {
            points: accum.points,
            avg_residual_mean,
            avg_excess_residual_mean,
            avg_er_change,
            er_change_sample_count,
            profit_loss_ratio,
            spread_mean,
            ic_mean,
            ic_std,
            icir,
            ic_t_value,
        },
        all_samples: accum.all_samples,
        triggered_samples: accum.triggered_samples,
        daily_score_layers: accum.daily_score_layers,
        return_distribution_counts: accum.return_distribution_counts,
    })
}

/// 线程本地累加器，用于 fold+reduce 模式逐个交易日处理而不物化中间 Vec。
#[derive(Debug, Default)]
pub(super) struct DayGroupsFoldAccum {
    points: Vec<RuleLayerPoint>,
    avg_residual_values: Vec<f64>,
    avg_excess_residual_values: Vec<f64>,
    er_change_values: Vec<f64>,
    profit_loss_sums: ProfitLossSums,
    spread_values: Vec<f64>,
    ic_values: Vec<f64>,
    all_samples: Vec<RuleLayerSamplePoint>,
    triggered_samples: Vec<RuleLayerSamplePoint>,
    daily_score_layers: Vec<RuleLayerDailyScoreLayers>,
    return_distribution_counts: [usize; 7],
}

impl DayGroupsFoldAccum {
    fn process_day_group(
        &mut self,
        runtime_cache: &RuleLayerRuntimeCache,
        day_group: &RuleDayGroup,
        triggered_scores: Option<&TriggeredScoreColumn>,
        config: &RuleLayerConfig,
        collect_options: RuleLayerCollectOptions,
    ) {
        let collect_metrics = collect_options.metrics;
        let collect_validation_details = collect_options.validation_details;
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
        let mut triggered_er_changes: Vec<f64> = if collect_metrics {
            Vec::with_capacity(cap)
        } else {
            Vec::new()
        };

        for sample in &day_group.samples {
            let triggered_score = triggered_scores
                .and_then(|scores| scores.get(runtime_cache.score_index(day_group, sample)));
            let rule_score = triggered_score.unwrap_or(0.0);

            if collect_metrics {
                rule_scores.push(rule_score);
                residuals.push(sample.residual_return);
            }

            if collect_validation_details
                && let Some(bucket_index) = (|residual_return: f64| -> Option<usize> {
                    if !residual_return.is_finite() {
                        return None;
                    }
                    Some(if residual_return <= -10.0 {
                        0
                    } else if residual_return <= -5.0 {
                        1
                    } else if residual_return <= -2.0 {
                        2
                    } else if residual_return <= 2.0 {
                        3
                    } else if residual_return <= 5.0 {
                        4
                    } else if residual_return <= 10.0 {
                        5
                    } else {
                        6
                    })
                })(sample.residual_return)
            {
                self.return_distribution_counts[bucket_index] += 1;
            }

            if let Some(rule_score) = triggered_score {
                if collect_metrics {
                    triggered_residuals.push(sample.residual_return);
                    if sample.er_change.is_finite() {
                        triggered_er_changes.push(sample.er_change);
                    }
                }

                if collect_options.triggered_samples || collect_validation_details {
                    self.triggered_samples.push(RuleLayerSamplePoint {
                        ts_code: runtime_cache.ts_code(sample.ts_code_id).to_string(),
                        trade_date: String::from(&*day_group.trade_date),
                        rule_score,
                        residual_return: sample.residual_return,
                        er_change: sample.er_change,
                    });
                }
            }

            if collect_options.all_samples {
                self.all_samples.push(RuleLayerSamplePoint {
                    ts_code: runtime_cache.ts_code(sample.ts_code_id).to_string(),
                    trade_date: String::from(&*day_group.trade_date),
                    rule_score,
                    residual_return: sample.residual_return,
                    er_change: sample.er_change,
                });
            }
        }

        if collect_metrics && triggered_residuals.len() >= config.min_samples_per_day {
            let avg_rule_score = mean(&rule_scores);
            let avg_residual_return = mean(&triggered_residuals);
            let market_avg_residual_return = mean(&residuals);
            let avg_excess_residual_return = match (avg_residual_return, market_avg_residual_return)
            {
                (Some(triggered_avg), Some(market_avg)) => Some(triggered_avg - market_avg),
                _ => None,
            };
            let score_weighted_residual_return =
                calc_score_weighted_return(&rule_scores, &residuals);
            let day_profit_loss_sums = calc_profit_loss_sums(&triggered_residuals);
            let top_bottom_spread = calc_top_bottom_spread(&rule_scores, &residuals);
            let ic = spearman_corr(&rule_scores, &residuals);

            if let Some(value) = avg_residual_return {
                self.avg_residual_values.push(value);
            }
            if let Some(value) = avg_excess_residual_return {
                self.avg_excess_residual_values.push(value);
            }
            self.er_change_values.extend(triggered_er_changes);
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
                score_weighted_residual_return,
                top_bottom_spread,
                ic,
            });
            if collect_validation_details {
                self.daily_score_layers.push((|trade_date: &str,
                                               rule_scores: &[f64],
                                               residuals: &[f64]|
                 -> RuleLayerDailyScoreLayers {
                    let mut ordered = rule_scores
                        .iter()
                        .zip(residuals)
                        .map(|(score, residual_return)| {
                            let score = if score.abs() < EPS { 0.0 } else { *score };
                            (score, *residual_return)
                        })
                        .collect::<Vec<_>>();
                    ordered.sort_by(|left, right| {
                        left.0.partial_cmp(&right.0).unwrap_or(Ordering::Equal)
                    });

                    let mut groups = Vec::new();
                    let mut index = 0usize;
                    while index < ordered.len() {
                        let score = ordered[index].0;
                        let score_bits = score.to_bits();
                        let mut residual_sum = 0.0;
                        let mut sample_count = 0usize;
                        while index < ordered.len() && ordered[index].0.to_bits() == score_bits {
                            residual_sum += ordered[index].1;
                            sample_count += 1;
                            index += 1;
                        }
                        groups.push(RuleLayerDailyScoreGroup {
                            score,
                            sample_count,
                            avg_residual_return: residual_sum / sample_count as f64,
                        });
                    }

                    RuleLayerDailyScoreLayers {
                        trade_date: trade_date.to_string(),
                        groups,
                    }
                })(
                    day_group.trade_date.as_ref(),
                    &rule_scores,
                    &residuals,
                ));
            }
        }
    }

    fn merge(&mut self, other: DayGroupsFoldAccum) {
        self.points.extend(other.points);
        self.avg_residual_values.extend(other.avg_residual_values);
        self.avg_excess_residual_values
            .extend(other.avg_excess_residual_values);
        self.er_change_values.extend(other.er_change_values);
        self.profit_loss_sums.merge(other.profit_loss_sums);
        self.spread_values.extend(other.spread_values);
        self.ic_values.extend(other.ic_values);
        self.all_samples.extend(other.all_samples);
        self.triggered_samples.extend(other.triggered_samples);
        self.daily_score_layers.extend(other.daily_score_layers);
        for (count, other_count) in self
            .return_distribution_counts
            .iter_mut()
            .zip(other.return_distribution_counts)
        {
            *count += other_count;
        }
    }
}

#[cfg(test)]
pub(super) fn build_rule_day_groups(
    universe_rows: Vec<RuleUniverseRow>,
    residual_map_cache: &HashMap<String, HashMap<String, RuleBacktestOutcome>>,
) -> RuleLayerRuntimeCache {
    if universe_rows.is_empty() || residual_map_cache.is_empty() {
        return RuleLayerRuntimeCache::empty();
    }

    let mut ts_code_names = universe_rows
        .iter()
        .map(|row| row.ts_code.as_str())
        .collect::<Vec<_>>();
    ts_code_names.sort_unstable();
    ts_code_names.dedup();
    let ts_codes = ts_code_names
        .iter()
        .map(|ts_code| Arc::<str>::from(*ts_code))
        .collect::<Vec<_>>();
    let ts_code_ids = ts_code_names
        .into_iter()
        .enumerate()
        .map(|(index, ts_code)| (ts_code.to_string(), index as u32))
        .collect::<HashMap<_, _>>();

    let mut day_groups: Vec<RuleDayGroup> = Vec::new();
    let mut day_group_ids = HashMap::new();
    let mut current_trade_date: Arc<str> = Arc::from("");
    let mut current_samples: Vec<RuleDayBaseSample> = Vec::new();
    let stock_count = ts_codes.len();

    for row in universe_rows {
        let Some(residual_map) = residual_map_cache.get(&row.ts_code) else {
            continue;
        };
        let Some(outcome) = residual_map.get(&row.trade_date).copied() else {
            continue;
        };

        if current_trade_date.as_ref() != row.trade_date.as_str() {
            if !current_trade_date.as_ref().is_empty() {
                current_samples.shrink_to_fit();
                let day_group_id = day_groups.len();
                day_group_ids.insert(current_trade_date.to_string(), day_group_id);
                day_groups.push(RuleDayGroup {
                    trade_date: current_trade_date,
                    score_offset: day_group_id * stock_count,
                    samples: std::mem::take(&mut current_samples),
                });
            }
            current_trade_date = Arc::from(row.trade_date.as_str());
        }

        let Some(&ts_code_id) = ts_code_ids.get(&row.ts_code) else {
            continue;
        };
        current_samples.push(RuleDayBaseSample {
            ts_code_id,
            residual_return: outcome.residual_return,
            er_change: outcome.er_change,
        });
    }

    if !current_trade_date.as_ref().is_empty() {
        current_samples.shrink_to_fit();
        let day_group_id = day_groups.len();
        day_group_ids.insert(current_trade_date.to_string(), day_group_id);
        day_groups.push(RuleDayGroup {
            trade_date: current_trade_date,
            score_offset: day_group_id * stock_count,
            samples: current_samples,
        });
    }

    day_groups.shrink_to_fit();
    let score_column_len = day_groups.len() * stock_count;
    RuleLayerRuntimeCache {
        day_groups,
        ts_codes,
        ts_code_ids,
        day_group_ids,
        score_column_len,
    }
}

pub(super) fn empty_metrics() -> RuleLayerMetrics {
    RuleLayerMetrics {
        points: Vec::new(),
        avg_residual_mean: None,
        avg_excess_residual_mean: None,
        avg_er_change: None,
        er_change_sample_count: 0,
        profit_loss_ratio: None,
        spread_mean: None,
        ic_mean: None,
        ic_std: None,
        icir: None,
        ic_t_value: None,
    }
}

#[cfg(test)]
mod tests {

    use crate::simulate::rule::RuleDayBaseSample;
    use crate::simulate::rule::RuleDayGroup;
    use crate::simulate::rule::RuleLayerConfig;
    use crate::simulate::rule::RuleLayerRuntimeCache;
    use crate::simulate::rule::RuleSample;
    use crate::simulate::rule::metrics::calc_rule_layer_metrics;
    use crate::simulate::rule::metrics::calc_rule_layer_metrics_from_cache;
    use crate::simulate::rule::test_support::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    #[test]
    fn average_er_change_is_weighted_by_triggered_samples() {
        let runtime_cache = RuleLayerRuntimeCache {
            day_groups: vec![
                RuleDayGroup {
                    trade_date: Arc::from("d0"),
                    score_offset: 0,
                    samples: vec![RuleDayBaseSample {
                        ts_code_id: 0,
                        residual_return: 1.0,
                        er_change: 1.0,
                    }],
                },
                RuleDayGroup {
                    trade_date: Arc::from("d1"),
                    score_offset: 2,
                    samples: vec![
                        RuleDayBaseSample {
                            ts_code_id: 0,
                            residual_return: 1.0,
                            er_change: 0.0,
                        },
                        RuleDayBaseSample {
                            ts_code_id: 1,
                            residual_return: 1.0,
                            er_change: 0.0,
                        },
                    ],
                },
            ],
            ts_codes: vec![Arc::from("a"), Arc::from("b")],
            ts_code_ids: HashMap::from([("a".to_string(), 0), ("b".to_string(), 1)]),
            day_group_ids: HashMap::from([("d0".to_string(), 0), ("d1".to_string(), 1)]),
            score_column_len: 4,
        };
        let triggered_score_map = HashMap::from([
            (
                "a".to_string(),
                HashMap::from([("d0".to_string(), 1.0), ("d1".to_string(), 1.0)]),
            ),
            ("b".to_string(), HashMap::from([("d1".to_string(), 1.0)])),
        ]);

        let metrics = calc_rule_layer_metrics_from_cache(
            &runtime_cache,
            &triggered_score_map,
            &RuleLayerConfig {
                min_samples_per_day: 1,
                backtest_period: 1,
                min_listed_trade_days: 0,
            },
        )
        .expect("metrics");

        assert_opt_close(metrics.avg_er_change, Some(1.0 / 3.0));
        assert_eq!(metrics.er_change_sample_count, 3);
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
}
