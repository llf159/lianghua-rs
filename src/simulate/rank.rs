use std::collections::{BTreeMap, HashMap};

use duckdb::{Connection, params_from_iter};

use super::rule::{
    RuleLayerConfig, RuleLayerSamplePoint, build_rule_layer_runtime_cache,
    collect_all_rule_samples_from_cache,
};
use crate::data::result_db_path;

const EPS: f64 = 1e-12;
const LAYER_COUNT: usize = 5;

#[derive(Debug, Clone)]
pub struct RankLayerConfig {
    pub min_samples_per_day: usize,
    pub backtest_period: usize,
    pub min_listed_trade_days: usize,
}

impl RankLayerConfig {
    fn validate(&self) -> Result<(), String> {
        if self.min_samples_per_day == 0 {
            return Err("每日最少样本数必须>=1".to_string());
        }
        if self.backtest_period == 0 {
            return Err("回测周期必须>=1".to_string());
        }
        Ok(())
    }

    pub fn effective_min_samples_per_day(&self) -> usize {
        self.min_samples_per_day.max(LAYER_COUNT)
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
}

#[derive(Debug, Clone, PartialEq)]
pub struct RankLayerMetrics {
    pub points: Vec<RankLayerPoint>,
    pub point_count: usize,
    pub sample_count: usize,
    pub spread_mean: Option<f64>,
    pub ic_mean: Option<f64>,
    pub ic_std: Option<f64>,
    pub icir: Option<f64>,
    pub ic_t_value: Option<f64>,
    pub layers: Vec<RankLayerSummaryBucket>,
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
    let triggered_score_map = load_total_score_map(source_dir, &input.start_date, &input.end_date)?;
    let all_samples = collect_all_rule_samples_from_cache(
        &runtime_cache,
        &triggered_score_map,
        &rule_layer_config,
    )?;

    calc_rank_layer_metrics(&all_samples, &input.layer_config)
}

pub fn calc_rank_layer_metrics(
    samples: &[RuleLayerSamplePoint],
    config: &RankLayerConfig,
) -> Result<RankLayerMetrics, String> {
    config.validate()?;

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
    let mut points = Vec::new();
    let mut spread_values = Vec::new();
    let mut ic_values = Vec::new();
    let mut total_sample_count = 0usize;
    let mut layer_day_score_sums = [0.0_f64; LAYER_COUNT];
    let mut layer_day_score_counts = [0usize; LAYER_COUNT];
    let mut layer_day_return_sums = [0.0_f64; LAYER_COUNT];
    let mut layer_day_return_counts = [0usize; LAYER_COUNT];
    let mut layer_sample_counts = [0usize; LAYER_COUNT];

    for (trade_date, day_samples) in grouped_by_day {
        if day_samples.len() < min_samples_per_day {
            continue;
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

        let mut layers = Vec::with_capacity(LAYER_COUNT);
        let mut layer_avg_returns = [None; LAYER_COUNT];
        let mut scores = Vec::with_capacity(day_samples.len());
        let mut residuals = Vec::with_capacity(day_samples.len());

        for sample in &day_samples {
            scores.push(sample.rule_score);
            residuals.push(sample.residual_return);
        }

        for layer_index in 0..LAYER_COUNT {
            let layer_items = ordered
                .iter()
                .enumerate()
                .filter(|(ordered_index, _)| {
                    ordered_index * LAYER_COUNT / ordered.len() == layer_index
                })
                .map(|(_, (_, pair))| *pair)
                .collect::<Vec<_>>();
            let layer_scores = layer_items.iter().map(|item| item.0).collect::<Vec<_>>();
            let layer_residuals = layer_items.iter().map(|item| item.1).collect::<Vec<_>>();
            let avg_score = mean(&layer_scores);
            let avg_residual_return = mean(&layer_residuals);

            if let Some(value) = avg_score {
                layer_day_score_sums[layer_index] += value;
                layer_day_score_counts[layer_index] += 1;
            }
            if let Some(value) = avg_residual_return {
                layer_day_return_sums[layer_index] += value;
                layer_day_return_counts[layer_index] += 1;
                layer_avg_returns[layer_index] = Some(value);
            }
            layer_sample_counts[layer_index] += layer_items.len();

            layers.push(RankLayerBucketPoint {
                layer_index: layer_index + 1,
                sample_count: layer_items.len(),
                avg_score,
                avg_residual_return,
            });
        }

        let top_bottom_spread = match (layer_avg_returns[0], layer_avg_returns[LAYER_COUNT - 1]) {
            (Some(low), Some(high)) => Some(high - low),
            _ => None,
        };
        let ic = spearman_corr(&scores, &residuals);

        if let Some(value) = top_bottom_spread {
            spread_values.push(value);
        }
        if let Some(value) = ic {
            ic_values.push(value);
        }
        total_sample_count += day_samples.len();
        points.push(RankLayerPoint {
            trade_date: trade_date.to_string(),
            sample_count: day_samples.len(),
            top_bottom_spread,
            ic,
            layers,
        });
    }

    let ic_mean = mean(&ic_values);
    let ic_std = sample_std(&ic_values);
    let icir = match (ic_mean, ic_std) {
        (Some(avg), Some(std)) if std.abs() >= EPS => Some(avg / std),
        _ => None,
    };

    Ok(RankLayerMetrics {
        point_count: points.len(),
        sample_count: total_sample_count,
        spread_mean: mean(&spread_values),
        ic_mean,
        ic_std,
        icir,
        ic_t_value: calc_t_value(ic_mean, ic_std, ic_values.len()),
        points,
        layers: (0..LAYER_COUNT)
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
            })
            .collect(),
    })
}

fn load_total_score_map(
    source_dir: &str,
    start_date: &str,
    end_date: &str,
) -> Result<HashMap<String, HashMap<String, f64>>, String> {
    let result_db = result_db_path(source_dir);
    if !result_db.exists() {
        return Ok(HashMap::new());
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
                TRY_CAST(total_score AS DOUBLE) AS total_score
            FROM score_summary
            WHERE trade_date >= ?
              AND trade_date <= ?
            ORDER BY trade_date ASC, ts_code ASC
            "#,
        )
        .map_err(|e| format!("预编译score_summary总分查询失败:{e}"))?;

    let mut rows = stmt
        .query(params_from_iter([start_date.trim(), end_date.trim()]))
        .map_err(|e| format!("查询score_summary总分失败:{e}"))?;

    let mut out = HashMap::<String, HashMap<String, f64>>::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取score_summary总分失败:{e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取ts_code失败:{e}"))?;
        let trade_date: String = row.get(1).map_err(|e| format!("读取trade_date失败:{e}"))?;
        let total_score: Option<f64> =
            row.get(2).map_err(|e| format!("读取total_score失败:{e}"))?;

        let Some(total_score) = total_score.filter(|value| value.is_finite()) else {
            continue;
        };
        if ts_code.trim().is_empty() || trade_date.trim().is_empty() {
            continue;
        }

        out.entry(ts_code)
            .or_default()
            .insert(trade_date, total_score);
    }

    Ok(out)
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

fn calc_t_value(mean: Option<f64>, std: Option<f64>, sample_count: usize) -> Option<f64> {
    match (mean, std) {
        (Some(avg), Some(dev)) if sample_count > 1 && dev.abs() >= EPS => {
            Some(avg * (sample_count as f64).sqrt() / dev)
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
    use super::{RankLayerConfig, calc_rank_layer_metrics};
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
            },
            RuleLayerSamplePoint {
                ts_code: "000002.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 2.0,
                residual_return: 20.0,
            },
            RuleLayerSamplePoint {
                ts_code: "000003.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 3.0,
                residual_return: 30.0,
            },
            RuleLayerSamplePoint {
                ts_code: "000004.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 4.0,
                residual_return: 40.0,
            },
            RuleLayerSamplePoint {
                ts_code: "000005.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 5.0,
                residual_return: 50.0,
            },
        ];

        let metrics = calc_rank_layer_metrics(
            &samples,
            &RankLayerConfig {
                min_samples_per_day: 5,
                backtest_period: 1,
                min_listed_trade_days: 0,
            },
        )
        .expect("rank metrics should build");

        assert_eq!(metrics.point_count, 1);
        assert_eq!(metrics.sample_count, 5);
        assert_opt_close(metrics.spread_mean, Some(40.0));
        assert_opt_close(metrics.ic_mean, Some(1.0));
        assert_eq!(metrics.layers.len(), 5);
        assert_eq!(metrics.layers[0].sample_count, 1);
        assert_opt_close(metrics.layers[0].avg_residual_return, Some(10.0));
        assert_eq!(metrics.layers[4].sample_count, 1);
        assert_opt_close(metrics.layers[4].avg_residual_return, Some(50.0));
    }
}
