use std::collections::{BTreeMap, HashMap, HashSet};

use duckdb::{Connection, params_from_iter};
use rayon::prelude::*;

use super::{
    ResidualFactorSeriesRefs, ResidualReturnInput, calc_stock_residual_returns_with_factor_series,
};
use crate::data::{
    concept_performance_data::{load_concept_trend_series, load_industry_trend_series},
    load_stock_list, load_ths_concepts_named_map, result_db_path, source_db_path,
};

const EPS: f64 = 1e-12;

#[derive(Debug, Clone)]
pub struct SceneLayerConfig {
    pub min_samples_per_day: usize,
    pub backtest_period: usize,
}

impl Default for SceneLayerConfig {
    fn default() -> Self {
        Self {
            min_samples_per_day: 5,
            backtest_period: 1,
        }
    }
}

impl SceneLayerConfig {
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
pub struct SceneLayerFromDbInput {
    pub scene_name: String,
    pub stock_adj_type: String,
    pub index_ts_code: String,
    pub index_beta: f64,
    pub concept_beta: f64,
    pub industry_beta: f64,
    pub start_date: String,
    pub end_date: String,
    pub layer_config: SceneLayerConfig,
}

impl SceneLayerFromDbInput {
    fn validate(&self) -> Result<(), String> {
        if self.scene_name.trim().is_empty() {
            return Err("scene_name不能为空".to_string());
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
pub struct SceneSample {
    pub trade_date: String,
    pub scene_state: String,
    pub residual_return: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SceneLayerPoint {
    pub trade_date: String,
    pub state_avg_residual_returns: Vec<(String, f64)>,
    pub top_bottom_spread: Option<f64>,
    pub ic: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SceneLayerMetrics {
    pub points: Vec<SceneLayerPoint>,
    pub spread_mean: Option<f64>,
    pub ic_mean: Option<f64>,
    pub ic_std: Option<f64>,
    pub icir: Option<f64>,
}

#[derive(Debug, Clone)]
struct SceneDbRow {
    scene_name: String,
    ts_code: String,
    trade_date: String,
    scene_state: String,
}

struct ResidualCacheInput<'a> {
    stock_adj_type: &'a str,
    index_ts_code: &'a str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &'a str,
    end_date: &'a str,
    backtest_period: usize,
}

pub fn calc_scene_layer_metrics_from_db(
    source_conn: &Connection,
    source_dir: &str,
    input: &SceneLayerFromDbInput,
) -> Result<SceneLayerMetrics, String> {
    input.validate()?;

    let scene_rows = load_scene_rows(source_dir, input)?;
    let concept_map = load_most_related_concept_map(source_dir)?;
    let industry_map = load_stock_industry_map(source_dir)?;
    if scene_rows.is_empty() {
        return Ok(empty_metrics());
    }

    let rows_by_ts = group_rows_by_ts(scene_rows);
    let residual_map_cache = build_residual_map_cache(
        source_conn,
        source_dir,
        rows_by_ts.keys().cloned().collect(),
        &concept_map,
        &industry_map,
        &ResidualCacheInput {
            stock_adj_type: &input.stock_adj_type,
            index_ts_code: &input.index_ts_code,
            index_beta: input.index_beta,
            concept_beta: input.concept_beta,
            industry_beta: input.industry_beta,
            start_date: &input.start_date,
            end_date: &input.end_date,
            backtest_period: input.layer_config.backtest_period,
        },
    )?;
    let samples = collect_scene_samples(rows_by_ts, &residual_map_cache)?;

    calc_scene_layer_metrics(&samples, &input.layer_config)
}

pub fn calc_all_scene_layer_metrics_from_db(
    source_conn: &Connection,
    source_dir: &str,
    scene_names: &[String],
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &SceneLayerConfig,
) -> Result<Vec<(String, SceneLayerMetrics)>, String> {
    validate_scene_common_input(
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
    )?;

    if scene_names.is_empty() {
        return Ok(Vec::new());
    }

    let scene_rows = load_scene_rows_for_names(source_dir, scene_names, start_date, end_date)?;
    let concept_map = load_most_related_concept_map(source_dir)?;
    let industry_map = load_stock_industry_map(source_dir)?;
    let mut rows_by_scene: HashMap<String, HashMap<String, Vec<SceneDbRow>>> = HashMap::new();
    let mut unique_ts_codes = HashSet::new();

    for row in scene_rows {
        unique_ts_codes.insert(row.ts_code.clone());
        rows_by_scene
            .entry(row.scene_name.clone())
            .or_default()
            .entry(row.ts_code.clone())
            .or_default()
            .push(row);
    }

    let residual_map_cache = build_residual_map_cache(
        source_conn,
        source_dir,
        unique_ts_codes.into_iter().collect(),
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
        },
    )?;

    let mut out = Vec::with_capacity(scene_names.len());
    for scene_name in scene_names {
        let rows_by_ts = rows_by_scene.remove(scene_name).unwrap_or_default();
        let samples = collect_scene_samples(rows_by_ts, &residual_map_cache)?;
        let metrics = calc_scene_layer_metrics(&samples, layer_config)?;
        out.push((scene_name.clone(), metrics));
    }

    Ok(out)
}

pub fn calc_scene_layer_metrics(
    samples: &[SceneSample],
    config: &SceneLayerConfig,
) -> Result<SceneLayerMetrics, String> {
    config.validate()?;

    let mut grouped_by_day: BTreeMap<String, Vec<&SceneSample>> = BTreeMap::new();
    for sample in samples {
        if sample.trade_date.trim().is_empty() || !sample.residual_return.is_finite() {
            continue;
        }
        grouped_by_day
            .entry(sample.trade_date.trim().to_string())
            .or_default()
            .push(sample);
    }

    let mut points = Vec::new();
    let mut spread_values = Vec::new();
    let mut ic_values = Vec::new();

    for (trade_date, day_samples) in grouped_by_day {
        if day_samples.len() < config.min_samples_per_day {
            continue;
        }

        let mut state_group: BTreeMap<String, Vec<f64>> = BTreeMap::new();
        let mut state_scores = Vec::new();
        let mut residuals = Vec::new();

        for sample in day_samples {
            let state = normalize_state(&sample.scene_state);
            state_group
                .entry(state.clone())
                .or_default()
                .push(sample.residual_return);

            state_scores.push(state_rank(&state) as f64);
            residuals.push(sample.residual_return);
        }

        let mut state_avg_residual_returns = Vec::new();
        for (state, values) in state_group {
            if let Some(avg) = mean(&values) {
                state_avg_residual_returns.push((state, avg));
            }
        }

        let top_bottom_spread = if state_avg_residual_returns.len() >= 2 {
            let mut ordered = state_avg_residual_returns.clone();
            ordered.sort_by(|a, b| {
                state_rank(&a.0)
                    .cmp(&state_rank(&b.0))
                    .then_with(|| a.0.cmp(&b.0))
            });
            let low = ordered.first().map(|(_, v)| *v);
            let high = ordered.last().map(|(_, v)| *v);
            match (low, high) {
                (Some(l), Some(h)) => Some(h - l),
                _ => None,
            }
        } else {
            None
        };

        if let Some(spread) = top_bottom_spread {
            spread_values.push(spread);
        }

        let ic = spearman_corr(&state_scores, &residuals);
        if let Some(v) = ic {
            ic_values.push(v);
        }

        points.push(SceneLayerPoint {
            trade_date,
            state_avg_residual_returns,
            top_bottom_spread,
            ic,
        });
    }

    let spread_mean = mean(&spread_values);
    let ic_mean = mean(&ic_values);
    let ic_std = sample_std(&ic_values);
    let icir = match (ic_mean, ic_std) {
        (Some(m), Some(s)) if s.abs() >= EPS => Some(m / s),
        _ => None,
    };

    Ok(SceneLayerMetrics {
        points,
        spread_mean,
        ic_mean,
        ic_std,
        icir,
    })
}

fn empty_metrics() -> SceneLayerMetrics {
    SceneLayerMetrics {
        points: Vec::new(),
        spread_mean: None,
        ic_mean: None,
        ic_std: None,
        icir: None,
    }
}

fn validate_scene_common_input(
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &SceneLayerConfig,
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

fn group_rows_by_ts(scene_rows: Vec<SceneDbRow>) -> HashMap<String, Vec<SceneDbRow>> {
    let mut rows_by_ts: HashMap<String, Vec<SceneDbRow>> = HashMap::new();
    for row in scene_rows {
        rows_by_ts.entry(row.ts_code.clone()).or_default().push(row);
    }
    rows_by_ts
}

fn collect_scene_samples(
    rows_by_ts: HashMap<String, Vec<SceneDbRow>>,
    residual_map_cache: &HashMap<String, HashMap<String, f64>>,
) -> Result<Vec<SceneSample>, String> {
    let mut samples = Vec::new();

    for (ts_code, rows) in rows_by_ts {
        let Some(residual_map) = residual_map_cache.get(&ts_code) else {
            continue;
        };

        for row in rows {
            if let Some(residual_return) = residual_map.get(&row.trade_date).copied() {
                samples.push(SceneSample {
                    trade_date: row.trade_date,
                    scene_state: row.scene_state,
                    residual_return,
                });
            }
        }
    }

    Ok(samples)
}

fn validate_direction(direction: &str) -> Result<(), String> {
    match direction.trim().to_ascii_lowercase().as_str() {
        "long" | "short" => Ok(()),
        other => Err(format!("scene direction非法: {other}，仅支持long/short")),
    }
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

    let source_db = source_db_path(source_dir);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?
        .to_string();

    if ts_codes.len() == 1 {
        let ts_code = ts_codes.into_iter().next().unwrap_or_default();
        let residual_map = build_residual_map_for_ts_code(
            source_conn,
            source_dir,
            &ts_code,
            concept_map,
            industry_map,
            &concept_series_cache,
            &industry_series_cache,
            input,
        )?;
        let mut out = HashMap::with_capacity(1);
        out.insert(ts_code, residual_map);
        return Ok(out);
    }

    let grouped_results: Vec<Result<(String, HashMap<String, f64>), String>> = ts_codes
        .into_par_iter()
        .map(|ts_code| {
            let conn = Connection::open(&source_db_str)
                .map_err(|e| format!("并发打开source_db失败:{e}"))?;
            let residual_map = build_residual_map_for_ts_code(
                &conn,
                source_dir,
                &ts_code,
                concept_map,
                industry_map,
                &concept_series_cache,
                &industry_series_cache,
                input,
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
    conn: &Connection,
    source_dir: &str,
    ts_code: &str,
    concept_map: &HashMap<String, String>,
    industry_map: &HashMap<String, String>,
    concept_series_cache: &HashMap<String, HashMap<String, f64>>,
    industry_series_cache: &HashMap<String, HashMap<String, f64>>,
    input: &ResidualCacheInput<'_>,
) -> Result<HashMap<String, f64>, String> {
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
    let residual_points = calc_stock_residual_returns_with_factor_series(
        conn,
        source_dir,
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
        ResidualFactorSeriesRefs {
            concept_series,
            industry_series,
        },
    )?;

    Ok(build_forward_backtest_residual_map(
        residual_points,
        input.backtest_period,
    ))
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

    let mut out = HashMap::new();
    for i in 0..residual_points.len() {
        let end = i + backtest_period;
        if end >= residual_points.len() {
            break;
        }

        let mut sum = 0.0_f64;
        let mut valid = true;
        for j in (i + 1)..=end {
            let v = residual_points[j].residual_pct;
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

fn normalize_state(state: &str) -> String {
    let s = state.trim().to_ascii_lowercase();
    if s.is_empty() {
        return "unknown".to_string();
    }
    s
}

fn state_rank(state: &str) -> i32 {
    match state {
        "fail" => 0,
        "observe" => 1,
        "trigger" => 2,
        "confirm" => 3,
        _ => 1,
    }
}

fn load_scene_rows(
    source_dir: &str,
    input: &SceneLayerFromDbInput,
) -> Result<Vec<SceneDbRow>, String> {
    load_scene_rows_for_names(
        source_dir,
        std::slice::from_ref(&input.scene_name),
        &input.start_date,
        &input.end_date,
    )
}

fn load_scene_rows_for_names(
    source_dir: &str,
    scene_names: &[String],
    start_date: &str,
    end_date: &str,
) -> Result<Vec<SceneDbRow>, String> {
    if scene_names.is_empty() {
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

    let placeholders = std::iter::repeat("?")
        .take(scene_names.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
            SELECT
                scene_name,
                ts_code,
                trade_date,
                direction,
                stage
            FROM scene_details
            WHERE scene_name IN ({placeholders})
              AND trade_date >= ?
              AND trade_date <= ?
            ORDER BY scene_name ASC, trade_date ASC, ts_code ASC
            "#
    );
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("预编译scene_details查询失败:{e}"))?;

    let query_params = scene_names
        .iter()
        .map(|value| value.trim())
        .chain(std::iter::once(start_date.trim()))
        .chain(std::iter::once(end_date.trim()));
    let mut rows = stmt
        .query(params_from_iter(query_params))
        .map_err(|e| format!("查询scene_details失败:{e}"))?;

    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取scene_details失败:{e}"))?
    {
        let scene_name: String = row.get(0).map_err(|e| format!("读取scene_name失败:{e}"))?;
        let ts_code: String = row.get(1).map_err(|e| format!("读取ts_code失败:{e}"))?;
        let trade_date: String = row.get(2).map_err(|e| format!("读取trade_date失败:{e}"))?;
        let direction: String = row.get(3).map_err(|e| format!("读取direction失败:{e}"))?;
        let stage: Option<String> = row.get(4).map_err(|e| format!("读取stage失败:{e}"))?;
        validate_direction(&direction)?;

        out.push(SceneDbRow {
            scene_name,
            ts_code,
            trade_date,
            scene_state: stage.unwrap_or_default(),
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
        fs::{create_dir_all, write},
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use duckdb::{Connection, params};

    use crate::data::{result_db_path, source_db_path};

    use super::{
        SceneLayerConfig, SceneLayerFromDbInput, calc_all_scene_layer_metrics_from_db,
        calc_scene_layer_metrics_from_db,
    };

    fn temp_source_dir() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("lianghua_scene_layer_{unique}"))
    }

    fn prepare_test_files(source_dir: &str) {
        create_dir_all(source_dir).expect("create source dir");

        write(
            PathBuf::from(source_dir).join("stock_list.csv"),
            "ts_code,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,industry\n000001.SZ,,,,,,,,,,,,,,main\n",
        )
        .expect("write stock_list.csv");
        write(
            PathBuf::from(source_dir).join("stock_concepts.csv"),
            "ts_code,c1,c2,c3,concept\n000001.SZ,,,,concept-a\n",
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
            .append_row(params!["000001.SZ", "20240102", "qfq", 2.0_f64])
            .expect("stock row1");
        source_app
            .append_row(params!["000001.SZ", "20240103", "qfq", 4.0_f64])
            .expect("stock row2");
        source_app
            .append_row(params!["000001.SZ", "20240104", "qfq", 6.0_f64])
            .expect("stock row3");
        source_app
            .append_row(params!["000300.SH", "20240102", "ind", 1.0_f64])
            .expect("index row1");
        source_app
            .append_row(params!["000300.SH", "20240103", "ind", 1.0_f64])
            .expect("index row2");
        source_app
            .append_row(params!["000300.SH", "20240104", "ind", 1.0_f64])
            .expect("index row3");
        source_app.flush().expect("flush stock_data");

        let result_conn = Connection::open(result_db_path(source_dir)).expect("open result db");
        result_conn
            .execute(
                r#"
                CREATE TABLE scene_details (
                    scene_name VARCHAR,
                    ts_code VARCHAR,
                    trade_date VARCHAR,
                    direction VARCHAR,
                    stage VARCHAR
                )
                "#,
                [],
            )
            .expect("create scene_details");

        let mut result_app = result_conn
            .appender("scene_details")
            .expect("scene_details appender");
        result_app
            .append_row(params!["场景A", "000001.SZ", "20240102", "long", "trigger"])
            .expect("scene a row1");
        result_app
            .append_row(params!["场景A", "000001.SZ", "20240103", "long", "confirm"])
            .expect("scene a row2");
        result_app
            .append_row(params!["场景B", "000001.SZ", "20240102", "short", "observe"])
            .expect("scene b row1");
        result_app
            .append_row(params!["场景B", "000001.SZ", "20240103", "short", "trigger"])
            .expect("scene b row2");
        result_app.flush().expect("flush scene_details");
    }

    #[test]
    fn batch_scene_layer_metrics_match_single_scene_results() {
        let source_dir = temp_source_dir();
        let source_dir_str = source_dir.to_str().expect("utf8 source dir");
        prepare_test_files(source_dir_str);

        let source_conn = Connection::open(source_db_path(source_dir_str)).expect("open source db");
        let scene_names = vec!["场景A".to_string(), "场景B".to_string()];
        let layer_config = SceneLayerConfig {
            min_samples_per_day: 1,
            backtest_period: 1,
        };

        let batch_metrics = calc_all_scene_layer_metrics_from_db(
            &source_conn,
            source_dir_str,
            &scene_names,
            "qfq",
            "000300.SH",
            0.5,
            0.0,
            0.0,
            "20240102",
            "20240104",
            &layer_config,
        )
        .expect("batch metrics");

        assert_eq!(batch_metrics.len(), 2);

        for (scene_name, metrics) in batch_metrics {
            let single_metrics = calc_scene_layer_metrics_from_db(
                &source_conn,
                source_dir_str,
                &SceneLayerFromDbInput {
                    scene_name: scene_name.clone(),
                    stock_adj_type: "qfq".to_string(),
                    index_ts_code: "000300.SH".to_string(),
                    index_beta: 0.5,
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
