use std::collections::{BTreeMap, HashMap, HashSet};

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
};

const EPS: f64 = 1e-12;
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
    pub top_bottom_spread: Option<f64>,
    pub ic: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuleLayerMetrics {
    pub points: Vec<RuleLayerPoint>,
    pub avg_residual_mean: Option<f64>,
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
    ts_code: String,
    residual_return: f64,
}

#[derive(Debug, Clone)]
struct RuleDayGroup {
    trade_date: String,
    samples: Vec<RuleDayBaseSample>,
}

#[derive(Debug, Clone, Copy)]
struct RuleLayerCollectOptions {
    metrics: bool,
    all_samples: bool,
    triggered_samples: bool,
}

struct RuleLayerDayComputation {
    point: Option<(RuleLayerPoint, Option<f64>, Option<f64>, Option<f64>)>,
    all_samples: Vec<RuleLayerSamplePoint>,
    triggered_samples: Vec<RuleLayerSamplePoint>,
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
    input.validate()?;

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
        &input.layer_config,
    )?;
    let rule_rows = load_rule_rows(source_dir, input)?;
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
    let rule_rows = load_rule_rows_for_names(source_dir, rule_names, start_date, end_date)?;
    let mut triggered_score_map_by_rule: HashMap<String, TriggeredScoreMap> = HashMap::new();

    for row in rule_rows {
        triggered_score_map_by_rule
            .entry(row.rule_name)
            .or_default()
            .entry(row.ts_code)
            .or_default()
            .insert(row.trade_date, row.rule_score);
    }

    // Runtime cache is already shared; parallelize per-rule metric calculation.
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

    let universe_rows = load_rule_universe_rows(source_dir, start_date, end_date)?;
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
            let top_bottom_spread = calc_top_bottom_spread(&rule_scores, &residuals);
            let ic = spearman_corr(&rule_scores, &residuals);

            Some((
                RuleLayerPoint {
                    trade_date: trade_date.to_string(),
                    sample_count: rule_scores.len(),
                    avg_rule_score,
                    avg_residual_return,
                    top_bottom_spread,
                    ic,
                },
                avg_residual_return,
                top_bottom_spread,
                ic,
            ))
        })
        .collect::<Vec<_>>();

    let mut points = Vec::with_capacity(day_results.len());
    let mut avg_residual_values = Vec::new();
    let mut spread_values = Vec::new();
    let mut ic_values = Vec::new();

    for item in day_results.into_iter().flatten() {
        let (point, avg_residual_return, top_bottom_spread, ic) = item;
        if let Some(value) = avg_residual_return {
            avg_residual_values.push(value);
        }
        if let Some(spread) = top_bottom_spread {
            spread_values.push(spread);
        }
        if let Some(value) = ic {
            ic_values.push(value);
        }
        points.push(point);
    }

    let avg_residual_mean = mean(&avg_residual_values);
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

    let day_results = day_groups
        .par_iter()
        .map(|day_group| {
            let need_metrics =
                collect_options.metrics && day_group.samples.len() >= config.min_samples_per_day;
            let mut rule_scores = if need_metrics {
                Vec::with_capacity(day_group.samples.len())
            } else {
                Vec::new()
            };
            let mut residuals = if need_metrics {
                Vec::with_capacity(day_group.samples.len())
            } else {
                Vec::new()
            };
            let mut all_samples = if collect_options.all_samples {
                Vec::with_capacity(day_group.samples.len())
            } else {
                Vec::new()
            };
            let mut triggered_samples = Vec::new();

            for sample in &day_group.samples {
                let triggered_score = triggered_score_map
                    .and_then(|score_map| score_map.get(&sample.ts_code))
                    .and_then(|date_score| date_score.get(&day_group.trade_date))
                    .copied();
                let rule_score = triggered_score.unwrap_or(0.0);

                if need_metrics {
                    rule_scores.push(rule_score);
                    residuals.push(sample.residual_return);
                }

                if collect_options.all_samples {
                    all_samples.push(RuleLayerSamplePoint {
                        ts_code: sample.ts_code.clone(),
                        trade_date: day_group.trade_date.clone(),
                        rule_score,
                        residual_return: sample.residual_return,
                    });
                }

                if collect_options.triggered_samples {
                    if let Some(rule_score) = triggered_score {
                        triggered_samples.push(RuleLayerSamplePoint {
                            ts_code: sample.ts_code.clone(),
                            trade_date: day_group.trade_date.clone(),
                            rule_score,
                            residual_return: sample.residual_return,
                        });
                    }
                }
            }

            let point = if need_metrics {
                let avg_rule_score = mean(&rule_scores);
                let avg_residual_return = mean(&residuals);
                let top_bottom_spread = calc_top_bottom_spread(&rule_scores, &residuals);
                let ic = spearman_corr(&rule_scores, &residuals);

                Some((
                    RuleLayerPoint {
                        trade_date: day_group.trade_date.clone(),
                        sample_count: rule_scores.len(),
                        avg_rule_score,
                        avg_residual_return,
                        top_bottom_spread,
                        ic,
                    },
                    avg_residual_return,
                    top_bottom_spread,
                    ic,
                ))
            } else {
                None
            };

            RuleLayerDayComputation {
                point,
                all_samples,
                triggered_samples,
            }
        })
        .collect::<Vec<_>>();

    let mut points = Vec::with_capacity(day_results.len());
    let mut avg_residual_values = Vec::new();
    let mut spread_values = Vec::new();
    let mut ic_values = Vec::new();
    let all_sample_count = day_results.iter().map(|item| item.all_samples.len()).sum();
    let triggered_sample_count = day_results
        .iter()
        .map(|item| item.triggered_samples.len())
        .sum();
    let mut all_samples = Vec::with_capacity(all_sample_count);
    let mut triggered_samples = Vec::with_capacity(triggered_sample_count);

    for item in day_results {
        if let Some((point, avg_residual_return, top_bottom_spread, ic)) = item.point {
            if let Some(value) = avg_residual_return {
                avg_residual_values.push(value);
            }
            if let Some(spread) = top_bottom_spread {
                spread_values.push(spread);
            }
            if let Some(value) = ic {
                ic_values.push(value);
            }
            points.push(point);
        }

        all_samples.extend(item.all_samples);
        triggered_samples.extend(item.triggered_samples);
    }

    let avg_residual_mean = mean(&avg_residual_values);
    let spread_mean = mean(&spread_values);
    let ic_mean = mean(&ic_values);
    let ic_std = sample_std(&ic_values);
    let icir = match (ic_mean, ic_std) {
        (Some(m), Some(s)) if s.abs() >= EPS => Some(m / s),
        _ => None,
    };
    let ic_t_value = calc_t_value(ic_mean, ic_std, ic_values.len());

    Ok(RuleLayerComputation {
        metrics: RuleLayerMetrics {
            points,
            avg_residual_mean,
            spread_mean,
            ic_mean,
            ic_std,
            icir,
            ic_t_value,
        },
        all_samples,
        triggered_samples,
    })
}

fn build_rule_day_groups(
    universe_rows: Vec<RuleUniverseRow>,
    residual_map_cache: &HashMap<String, HashMap<String, f64>>,
) -> Vec<RuleDayGroup> {
    let mut day_groups = Vec::new();
    let mut current_trade_date = String::new();
    let mut current_samples = Vec::<RuleDayBaseSample>::new();

    for row in universe_rows {
        let Some(residual_map) = residual_map_cache.get(&row.ts_code) else {
            continue;
        };
        let Some(residual_return) = residual_map.get(&row.trade_date).copied() else {
            continue;
        };

        if current_trade_date != row.trade_date {
            if !current_trade_date.is_empty() {
                day_groups.push(RuleDayGroup {
                    trade_date: std::mem::take(&mut current_trade_date),
                    samples: std::mem::take(&mut current_samples),
                });
            }
            current_trade_date = row.trade_date.clone();
        }

        current_samples.push(RuleDayBaseSample {
            ts_code: row.ts_code,
            residual_return,
        });
    }

    if !current_trade_date.is_empty() {
        day_groups.push(RuleDayGroup {
            trade_date: current_trade_date,
            samples: current_samples,
        });
    }

    day_groups
}

fn empty_metrics() -> RuleLayerMetrics {
    RuleLayerMetrics {
        points: Vec::new(),
        avg_residual_mean: None,
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

    let concept_series_cache = build_concept_series_cache(
        source_dir,
        &ts_codes,
        concept_map,
        input.start_date,
        input.end_date,
        input.concept_beta.abs() > EPS,
    )?;
    let industry_series_cache = build_industry_series_cache(
        source_dir,
        &ts_codes,
        industry_map,
        input.start_date,
        input.end_date,
        input.industry_beta.abs() > EPS,
    )?;
    let stock_series_cache = load_pct_chg_series_cache_for_ts_codes(
        source_conn,
        &ts_codes,
        input.stock_adj_type,
        input.start_date,
        input.end_date,
    )?;
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

    let mut out = HashMap::with_capacity(grouped_results.len());
    for item in grouped_results {
        let (ts_code, residual_map) = item?;
        out.insert(ts_code, residual_map);
    }
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
    Ok(residual_map)
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

fn load_rule_rows(
    source_dir: &str,
    input: &RuleLayerFromDbInput,
) -> Result<Vec<RuleDbRow>, String> {
    load_rule_rows_for_names(
        source_dir,
        std::slice::from_ref(&input.rule_name),
        &input.start_date,
        &input.end_date,
    )
}

fn load_rule_rows_for_names(
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
                TRY_CAST(rule_score AS DOUBLE) AS rule_score
            FROM rule_details
            WHERE rule_name IN ({placeholders})
              AND trade_date >= ?
              AND trade_date <= ?
            ORDER BY rule_name ASC, trade_date ASC, ts_code ASC
            "#
    );
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("预编译rule_details查询失败:{e}"))?;

    let query_params = rule_names
        .iter()
        .map(|value| value.trim())
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
        let rule_score: Option<f64> = row.get(3).map_err(|e| format!("读取rule_score失败:{e}"))?;

        let Some(rule_score) = rule_score.filter(|value| value.is_finite()) else {
            continue;
        };

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

fn calc_top_bottom_spread(rule_scores: &[f64], residuals: &[f64]) -> Option<f64> {
    if rule_scores.len() != residuals.len() || rule_scores.len() < 2 {
        return None;
    }

    let mut min_score = f64::INFINITY;
    let mut max_score = f64::NEG_INFINITY;
    for score in rule_scores {
        min_score = min_score.min(*score);
        max_score = max_score.max(*score);
    }
    if (max_score - min_score).abs() < EPS {
        return None;
    }

    let mut ordered = rule_scores
        .iter()
        .copied()
        .enumerate()
        .collect::<Vec<(usize, f64)>>();
    ordered.sort_by(|a, b| {
        a.1.partial_cmp(&b.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });

    let half = ordered.len() / 2;
    if half == 0 {
        return None;
    }

    let low_sum: f64 = ordered
        .iter()
        .take(half)
        .map(|(idx, _)| residuals[*idx])
        .sum();
    let high_sum: f64 = ordered
        .iter()
        .rev()
        .take(half)
        .map(|(idx, _)| residuals[*idx])
        .sum();

    Some(high_sum / half as f64 - low_sum / half as f64)
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
            let d = *v - avg;
            d * d
        })
        .sum::<f64>()
        / (values.len() as f64 - 1.0);
    Some(var.sqrt())
}

fn calc_t_value(mean: Option<f64>, std: Option<f64>, sample_count: usize) -> Option<f64> {
    match (mean, std) {
        (Some(m), Some(s)) if sample_count > 1 && s.abs() >= EPS => {
            Some(m * (sample_count as f64).sqrt() / s)
        }
        _ => None,
    }
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
    let mut indexed: Vec<(usize, f64)> = values.iter().copied().enumerate().collect();
    indexed.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

    let mut ranks = vec![0.0_f64; values.len()];
    let mut i = 0usize;
    while i < indexed.len() {
        let mut j = i + 1;
        while j < indexed.len() && (indexed[j].1 - indexed[i].1).abs() < EPS {
            j += 1;
        }

        let avg_rank = (i + 1 + j) as f64 / 2.0;
        for k in i..j {
            ranks[indexed[k].0] = avg_rank;
        }
        i = j;
    }

    ranks
}

fn pearson_corr(x: &[f64], y: &[f64]) -> Option<f64> {
    if x.len() != y.len() || x.len() < 2 {
        return None;
    }

    let mean_x = mean(x)?;
    let mean_y = mean(y)?;

    let mut cov = 0.0_f64;
    let mut var_x = 0.0_f64;
    let mut var_y = 0.0_f64;

    for (vx, vy) in x.iter().zip(y.iter()) {
        let dx = *vx - mean_x;
        let dy = *vy - mean_y;
        cov += dx * dy;
        var_x += dx * dx;
        var_y += dy * dy;
    }

    if var_x <= EPS || var_y <= EPS {
        return None;
    }

    Some(cov / (var_x.sqrt() * var_y.sqrt()))
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
        RuleLayerConfig, RuleLayerFromDbInput, build_rule_layer_runtime_cache,
        calc_all_rule_layer_metrics_from_db, calc_rule_layer_metrics_from_db,
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
                "000001.SZ,,样本股A,,main,20230001,,,,,,,,,,,,,,,\n",
                "000002.SZ,,样本股B,,main,20230001,,,,,,,,,,,,,,,\n"
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
        assert_opt_close(p0.top_bottom_spread, Some(2.0));
        assert_opt_close(p0.ic, Some(1.0));

        let p1 = &metrics.points[1];
        assert_eq!(p1.trade_date, "20240103");
        assert_eq!(p1.sample_count, 2);
        assert_opt_close(p1.avg_rule_score, Some(0.0));
        assert_opt_close(p1.avg_residual_return, Some(2.0));
        assert_opt_close(p1.top_bottom_spread, Some(6.0));
        assert_opt_close(p1.ic, Some(1.0));

        assert_opt_close(metrics.avg_residual_mean, Some(2.0));
        assert_opt_close(metrics.spread_mean, Some(4.0));
        assert_opt_close(metrics.ic_mean, Some(1.0));
        assert_opt_close(metrics.ic_std, Some(0.0));
        assert_eq!(metrics.icir, None);
    }

    #[test]
    fn calc_rule_layer_metrics_from_db_defaults_non_triggered_to_zero() {
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
        assert_opt_close(p0.avg_residual_return, Some(2.0));
        assert_eq!(p0.top_bottom_spread, None);
        assert_eq!(p0.ic, None);

        let p1 = &metrics.points[1];
        assert_eq!(p1.trade_date, "20240103");
        assert_eq!(p1.sample_count, 2);
        assert_opt_close(p1.avg_rule_score, Some(0.0));
        assert_opt_close(p1.avg_residual_return, Some(2.0));
        assert_eq!(p1.top_bottom_spread, None);
        assert_eq!(p1.ic, None);
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
    fn runtime_cache_keeps_full_universe_metrics_and_trigger_only_samples() {
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
}
