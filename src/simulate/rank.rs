use std::collections::{BTreeMap, HashMap};

use duckdb::{Connection, params_from_iter};

use super::rule::{
    RuleLayerConfig, RuleLayerSamplePoint, build_rule_layer_runtime_cache,
    build_rule_layer_runtime_cache_from_summary_rows, collect_all_rule_samples_from_cache,
};
use crate::data::{result_db_path, scoring_data::ScoreSummary};

const EPS: f64 = 1e-12;
const DEFAULT_LAYER_COUNT: usize = 5;
const MAX_LAYER_COUNT: usize = 100;

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
}

#[derive(Debug, Clone, PartialEq)]
pub struct RankLayerSamplePoint {
    pub layer_index: usize,
    pub ts_code: String,
    pub trade_date: String,
    pub score: f64,
    pub residual_return: f64,
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
    pub layer_samples: Vec<RankLayerSamplePoint>,
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

fn calc_rank_layer_metrics_with_lookup(
    samples: &[RuleLayerSamplePoint],
    config: &RankLayerConfig,
    rank_lookup: Option<&RankLayerLookup>,
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
    let layer_count = config.layer_count;
    let mut layer_day_score_sums = vec![0.0_f64; layer_count];
    let mut layer_day_score_counts = vec![0usize; layer_count];
    let mut layer_day_return_sums = vec![0.0_f64; layer_count];
    let mut layer_day_return_counts = vec![0usize; layer_count];
    let mut layer_sample_counts = vec![0usize; layer_count];
    let mut layer_samples = Vec::new();

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

        let layer_sample_indices_by_index =
            build_layer_sample_indices(trade_date, &day_samples, &ordered, config, rank_lookup);
        let mut layers = Vec::with_capacity(layer_count);
        let mut layer_avg_returns = vec![None; layer_count];
        let mut scores = Vec::with_capacity(day_samples.len());
        let mut residuals = Vec::with_capacity(day_samples.len());

        for sample in &day_samples {
            scores.push(sample.rule_score);
            residuals.push(sample.residual_return);
        }

        for (layer_index, layer_sample_indices) in
            layer_sample_indices_by_index.into_iter().enumerate()
        {
            let layer_scores = layer_sample_indices
                .iter()
                .map(|sample_index| day_samples[*sample_index].rule_score)
                .collect::<Vec<_>>();
            let layer_residuals = layer_sample_indices
                .iter()
                .map(|sample_index| day_samples[*sample_index].residual_return)
                .collect::<Vec<_>>();
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
            layer_sample_counts[layer_index] += layer_sample_indices.len();

            for sample_index in &layer_sample_indices {
                let sample = day_samples[*sample_index];
                layer_samples.push(RankLayerSamplePoint {
                    layer_index: layer_index + 1,
                    ts_code: sample.ts_code.clone(),
                    trade_date: trade_date.to_string(),
                    score: sample.rule_score,
                    residual_return: sample.residual_return,
                });
            }

            layers.push(RankLayerBucketPoint {
                layer_index: layer_index + 1,
                sample_count: layer_sample_indices.len(),
                avg_score,
                avg_residual_return,
            });
        }

        let top_bottom_spread = match (layer_avg_returns[0], layer_avg_returns[layer_count - 1]) {
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
            })
            .collect(),
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
        RankLayerMethod::SampleCount => {
            build_sample_count_layer_sample_indices(ordered, config.layer_count)
        }
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
    ordered: &[(usize, (f64, f64))],
    layer_count: usize,
) -> Vec<Vec<usize>> {
    let mut layers = vec![Vec::new(); layer_count];
    if ordered.is_empty() || layer_count == 0 {
        return layers;
    }

    for (ordered_index, (sample_index, _)) in ordered.iter().enumerate() {
        let layer_index = ordered_index * layer_count / ordered.len();
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

    let mut fallback_ranks = HashMap::with_capacity(ordered.len());
    for (ordered_index, (sample_index, _)) in ordered.iter().enumerate() {
        fallback_ranks.insert(*sample_index, ordered.len() as i64 - ordered_index as i64);
    }

    let fallback_day_max_rank = ordered.len() as i64;
    let day_max_rank = rank_lookup
        .and_then(|lookup| lookup.day_max_ranks.get(trade_date).copied())
        .filter(|value| *value > 0)
        .unwrap_or(fallback_day_max_rank);

    for (sample_index, sample) in day_samples.iter().enumerate() {
        let sample_rank = rank_lookup
            .and_then(|lookup| lookup.sample_ranks.get(&sample.ts_code))
            .and_then(|rank_by_day| rank_by_day.get(trade_date))
            .copied()
            .filter(|value| *value > 0)
            .unwrap_or_else(|| fallback_ranks.get(&sample_index).copied().unwrap_or(1));
        let clamped_rank = sample_rank.clamp(1, day_max_rank);
        let score_position_from_low = day_max_rank - clamped_rank + 1;
        let mut layer_index =
            ((score_position_from_low * layer_count as i64 - 1) / day_max_rank) as usize;
        if layer_index >= layer_count {
            layer_index = layer_count - 1;
        }
        layers[layer_index].push(sample_index);
    }

    layers
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
    use super::{RankLayerConfig, RankLayerMethod, calc_rank_layer_metrics};
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
                layer_count: 5,
                layer_method: RankLayerMethod::SampleCount,
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

    #[test]
    fn rank_layer_metrics_support_score_range_buckets() {
        let samples = vec![
            RuleLayerSamplePoint {
                ts_code: "000001.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 0.0,
                residual_return: 10.0,
            },
            RuleLayerSamplePoint {
                ts_code: "000002.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 10.0,
                residual_return: 20.0,
            },
            RuleLayerSamplePoint {
                ts_code: "000003.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 20.0,
                residual_return: 30.0,
            },
            RuleLayerSamplePoint {
                ts_code: "000004.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 30.0,
                residual_return: 40.0,
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
            },
            RuleLayerSamplePoint {
                ts_code: "000002.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 20.0,
                residual_return: 2.0,
            },
            RuleLayerSamplePoint {
                ts_code: "000003.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 30.0,
                residual_return: 3.0,
            },
            RuleLayerSamplePoint {
                ts_code: "000004.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 40.0,
                residual_return: 4.0,
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
}
