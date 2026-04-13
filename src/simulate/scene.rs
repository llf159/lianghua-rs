use std::collections::{BTreeMap, HashMap};

use duckdb::{Connection, params};
use rayon::prelude::*;

use super::{ResidualReturnInput, calc_stock_residual_returns};
use crate::data::{load_stock_list, load_ths_concepts_list, result_db_path, source_db_path};

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
    pub board_beta: f64,
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
        if !self.board_beta.is_finite() {
            return Err("板块系数必须是有限数字".to_string());
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
    ts_code: String,
    trade_date: String,
    scene_state: String,
}

pub fn calc_scene_layer_metrics_from_db(
    _source_conn: &Connection,
    source_dir: &str,
    input: &SceneLayerFromDbInput,
) -> Result<SceneLayerMetrics, String> {
    input.validate()?;

    let scene_rows = load_scene_rows(source_dir, input)?;
    let concept_map = load_most_related_concept_map(source_dir)?;
    let board_map = load_stock_board_map(source_dir)?;
    if scene_rows.is_empty() {
        return Ok(empty_metrics());
    }

    let mut rows_by_ts: HashMap<String, Vec<SceneDbRow>> = HashMap::new();
    for row in scene_rows {
        rows_by_ts.entry(row.ts_code.clone()).or_default().push(row);
    }

    let source_db = source_db_path(source_dir);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?
        .to_string();

    let mut samples = Vec::new();

    if rows_by_ts.len() <= 1 {
        for (ts_code, rows) in rows_by_ts {
            let most_related_concept = concept_map.get(&ts_code).cloned().unwrap_or_default();
            let board = board_map.get(&ts_code).cloned().unwrap_or_default();
            let residual_points = calc_stock_residual_returns(
                _source_conn,
                source_dir,
                &ResidualReturnInput {
                    ts_code: ts_code.clone(),
                    stock_adj_type: input.stock_adj_type.clone(),
                    index_ts_code: input.index_ts_code.clone(),
                    concept: most_related_concept,
                    board,
                    index_beta: input.index_beta,
                    concept_beta: input.concept_beta,
                    board_beta: input.board_beta,
                    start_date: input.start_date.clone(),
                    end_date: input.end_date.clone(),
                },
            )?;

            let residual_map = build_forward_backtest_residual_map(
                residual_points,
                input.layer_config.backtest_period,
            );

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
    } else {
        let grouped_results: Vec<Result<Vec<SceneSample>, String>> = rows_by_ts
            .into_par_iter()
            .map(|(ts_code, rows)| {
                let conn = Connection::open(&source_db_str)
                    .map_err(|e| format!("并发打开source_db失败:{e}"))?;

                let most_related_concept = concept_map.get(&ts_code).cloned().unwrap_or_default();
                let board = board_map.get(&ts_code).cloned().unwrap_or_default();
                let residual_points = calc_stock_residual_returns(
                    &conn,
                    source_dir,
                    &ResidualReturnInput {
                        ts_code: ts_code.clone(),
                        stock_adj_type: input.stock_adj_type.clone(),
                        index_ts_code: input.index_ts_code.clone(),
                        concept: most_related_concept,
                        board,
                        index_beta: input.index_beta,
                        concept_beta: input.concept_beta,
                        board_beta: input.board_beta,
                        start_date: input.start_date.clone(),
                        end_date: input.end_date.clone(),
                    },
                )?;

                let residual_map = build_forward_backtest_residual_map(
                    residual_points,
                    input.layer_config.backtest_period,
                );

                let mut one_ts_samples = Vec::new();
                for row in rows {
                    if let Some(residual_return) = residual_map.get(&row.trade_date).copied() {
                        one_ts_samples.push(SceneSample {
                            trade_date: row.trade_date,
                            scene_state: row.scene_state,
                            residual_return,
                        });
                    }
                }
                Ok(one_ts_samples)
            })
            .collect();

        for item in grouped_results {
            samples.extend(item?);
        }
    }

    calc_scene_layer_metrics(&samples, &input.layer_config)
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
                trade_date,
                COALESCE(TRY_CAST(stage AS VARCHAR), 'unknown') AS stage
            FROM scene_details
            WHERE scene_name = ?
              AND trade_date >= ?
              AND trade_date <= ?
            ORDER BY trade_date ASC, ts_code ASC
            "#,
        )
        .map_err(|e| format!("预编译scene_details查询失败:{e}"))?;

    let mut rows = stmt
        .query(params![
            input.scene_name.trim(),
            input.start_date.trim(),
            input.end_date.trim()
        ])
        .map_err(|e| format!("查询scene_details失败:{e}"))?;

    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取scene_details失败:{e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取ts_code失败:{e}"))?;
        let trade_date: String = row.get(1).map_err(|e| format!("读取trade_date失败:{e}"))?;
        let stage: Option<String> = row.get(2).map_err(|e| format!("读取stage失败:{e}"))?;

        out.push(SceneDbRow {
            ts_code,
            trade_date,
            scene_state: stage.unwrap_or_default(),
        });
    }

    Ok(out)
}

fn load_most_related_concept_map(source_dir: &str) -> Result<HashMap<String, String>, String> {
    let rows = load_ths_concepts_list(source_dir)?;
    let mut map = HashMap::new();

    for row in rows {
        let Some(ts_code) = row.first().map(|v| v.trim()) else {
            continue;
        };
        if ts_code.is_empty() {
            continue;
        }

        let most_related = row
            .get(4)
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
            .unwrap_or("")
            .to_string();

        map.entry(ts_code.to_string()).or_insert(most_related);
    }

    Ok(map)
}

fn load_stock_board_map(source_dir: &str) -> Result<HashMap<String, String>, String> {
    let rows = load_stock_list(source_dir)?;
    let mut map = HashMap::with_capacity(rows.len());

    for row in rows {
        let Some(ts_code) = row.first().map(|v| v.trim()) else {
            continue;
        };
        if ts_code.is_empty() {
            continue;
        }

        let board = row
            .get(14)
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
            .unwrap_or("")
            .to_string();

        map.insert(ts_code.to_string(), board);
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
