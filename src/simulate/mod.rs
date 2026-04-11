use rayon::prelude::*;

use crate::{
    data::{
        RowData,
        scoring_data::{cache_rule_build, row_into_rt},
        simulate::{SimulateBarInput, build_simulated_row_data},
    },
    download::ind_calc::{IndsCache, cache_ind_build, calc_inds_with_cache},
    expr::eval::Runtime,
    scoring::{CachedRule, scoring_rules_details_cache},
};

const EPS: f64 = 1e-12;

#[derive(Debug)]
pub struct SimulationRuntimeBundle {
    pub row_data: RowData,
    pub runtime: Runtime,
    pub simulated_days: usize,
}

#[derive(Clone)]
pub struct SimulationCaches {
    pub rule_cache: Vec<CachedRule>,
    pub indicator_cache: Vec<IndsCache>,
}

#[derive(Debug)]
pub struct SimulationBatchInput {
    pub ts_code: String,
    pub row_data: RowData,
}

#[derive(Debug)]
pub struct SimulationBatchResult {
    pub ts_code: String,
    pub scoring: SimulationScoringResult,
}

#[derive(Debug, Clone)]
pub struct SimulatedRuleScore {
    pub rule_name: String,
    pub rule_score: f64,
    pub triggered: bool,
}

#[derive(Debug, Clone)]
pub struct SimulatedDayScore {
    pub trade_date: String,
    pub total_score: f64,
    pub rule_scores: Vec<SimulatedRuleScore>,
}

#[derive(Debug)]
pub struct SimulationScoringResult {
    pub row_data: RowData,
    pub simulated_days: usize,
    pub days: Vec<SimulatedDayScore>,
}

pub fn load_simulation_caches(source_dir: &str) -> Result<SimulationCaches, String> {
    Ok(SimulationCaches {
        rule_cache: cache_rule_build(source_dir)?,
        indicator_cache: cache_ind_build(source_dir)?,
    })
}

pub fn build_simulated_row_data_series(
    row_data: RowData,
    inputs: &[SimulateBarInput],
) -> Result<RowData, String> {
    let mut row_data = row_data;
    for input in inputs {
        row_data = build_simulated_row_data(row_data, input)?;
    }
    Ok(row_data)
}

pub fn build_simulated_runtime_bundle(
    source_dir: &str,
    row_data: RowData,
    inputs: &[SimulateBarInput],
) -> Result<SimulationRuntimeBundle, String> {
    let caches = load_simulation_caches(source_dir)?;
    build_simulated_runtime_bundle_with_cache(row_data, inputs, &caches.indicator_cache)
}

pub fn build_simulated_runtime_bundle_with_cache(
    row_data: RowData,
    inputs: &[SimulateBarInput],
    indicator_cache: &[IndsCache],
) -> Result<SimulationRuntimeBundle, String> {
    let mut row_data = build_simulated_row_data_series(row_data, inputs)?;

    if !indicator_cache.is_empty() {
        for (name, series) in calc_inds_with_cache(indicator_cache, row_data.clone())? {
            row_data.cols.insert(name, series);
        }
        row_data.validate()?;
    }

    Ok(SimulationRuntimeBundle {
        runtime: row_into_rt(row_data.clone())?,
        row_data,
        simulated_days: inputs.len(),
    })
}

pub fn run_simulated_scoring(
    source_dir: &str,
    row_data: RowData,
    inputs: &[SimulateBarInput],
) -> Result<SimulationScoringResult, String> {
    let caches = load_simulation_caches(source_dir)?;
    run_simulated_scoring_with_cache(row_data, inputs, &caches)
}

pub fn run_simulated_scoring_with_cache(
    row_data: RowData,
    inputs: &[SimulateBarInput],
    caches: &SimulationCaches,
) -> Result<SimulationScoringResult, String> {
    let SimulationRuntimeBundle {
        row_data,
        mut runtime,
        simulated_days,
    } = build_simulated_runtime_bundle_with_cache(row_data, inputs, &caches.indicator_cache)?;
    let (total_scores, rule_score_series) =
        scoring_rules_details_cache(&mut runtime, &caches.rule_cache)?;

    let start_idx = row_data.trade_dates.len().saturating_sub(simulated_days);
    let mut days = Vec::with_capacity(simulated_days);

    for idx in start_idx..row_data.trade_dates.len() {
        let total_score = total_scores
            .get(idx)
            .copied()
            .ok_or_else(|| format!("总分结果缺少索引:{idx}"))?;
        let mut rule_scores = Vec::new();

        for item in &rule_score_series {
            let Some(rule_score) = item.series.get(idx).copied() else {
                continue;
            };
            let triggered = item.triggered.get(idx).copied().unwrap_or(false);
            if !triggered && rule_score.abs() <= EPS {
                continue;
            }

            rule_scores.push(SimulatedRuleScore {
                rule_name: item.name.clone(),
                rule_score,
                triggered,
            });
        }

        days.push(SimulatedDayScore {
            trade_date: row_data.trade_dates[idx].clone(),
            total_score,
            rule_scores,
        });
    }

    Ok(SimulationScoringResult {
        row_data,
        simulated_days,
        days,
    })
}

pub fn run_simulated_scoring_batch(
    source_dir: &str,
    items: Vec<SimulationBatchInput>,
    inputs: &[SimulateBarInput],
) -> Result<Vec<SimulationBatchResult>, String> {
    let caches = load_simulation_caches(source_dir)?;
    run_simulated_scoring_batch_with_cache(items, inputs, &caches)
}

pub fn run_simulated_scoring_batch_with_cache(
    items: Vec<SimulationBatchInput>,
    inputs: &[SimulateBarInput],
    caches: &SimulationCaches,
) -> Result<Vec<SimulationBatchResult>, String> {
    items
        .into_par_iter()
        .map(|item| {
            let scoring = run_simulated_scoring_with_cache(item.row_data, inputs, caches)?;
            Ok(SimulationBatchResult {
                ts_code: item.ts_code,
                scoring,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{build_simulated_row_data_series, run_simulated_scoring};
    use crate::data::{RowData, simulate::SimulateBarInput};
    use std::{
        collections::HashMap,
        fs::{create_dir_all, remove_dir_all, write},
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn sample_row_data() -> RowData {
        let mut cols = HashMap::new();
        cols.insert("O".to_string(), vec![Some(9.8), Some(10.0)]);
        cols.insert("H".to_string(), vec![Some(10.1), Some(10.3)]);
        cols.insert("L".to_string(), vec![Some(9.7), Some(9.9)]);
        cols.insert("C".to_string(), vec![Some(10.0), Some(10.2)]);
        cols.insert("V".to_string(), vec![Some(100.0), Some(120.0)]);
        cols.insert("AMOUNT".to_string(), vec![Some(1000.0), Some(1260.0)]);
        cols.insert("PRE_CLOSE".to_string(), vec![Some(9.7), Some(10.0)]);
        cols.insert("CHANGE".to_string(), vec![Some(0.3), Some(0.2)]);
        cols.insert("PCT_CHG".to_string(), vec![Some(3.0928), Some(2.0)]);
        cols.insert("TOR".to_string(), vec![Some(1.0), Some(1.2)]);
        cols.insert("ZHANG".to_string(), vec![Some(0.095), Some(0.095)]);

        RowData {
            trade_dates: vec!["20260403".to_string(), "20260407".to_string()],
            cols,
        }
    }

    fn create_test_source_dir() -> Result<PathBuf, String> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| format!("系统时间错误: {e}"))?
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("lianghua-simulate-{unique}"));
        create_dir_all(&dir).map_err(|e| format!("创建临时目录失败: {e}"))?;

        let rule_toml = r#"
version = 1

[[rule]]
name = "UP_DAY"
scope_windows = 1
scope_way = "LAST"
when = "C > PRE_CLOSE"
points = 10.0
explain = "test"
"#;

        write(dir.join("score_rule.toml"), rule_toml)
            .map_err(|e| format!("写入规则文件失败: {e}"))?;
        Ok(dir)
    }

    #[test]
    fn build_simulated_row_data_series_supports_multiple_days() {
        let inputs = vec![
            SimulateBarInput {
                trade_date: Some("20260408".to_string()),
                open_gap_pct: 0.0,
                pct_chg: 5.0,
                pct_chg_relative_to_open: false,
                volume_ratio: 1.5,
                upper_shadow_pct: 0.0,
                lower_shadow_pct: 0.0,
            },
            SimulateBarInput {
                trade_date: Some("20260409".to_string()),
                open_gap_pct: 0.0,
                pct_chg: -3.0,
                pct_chg_relative_to_open: false,
                volume_ratio: 0.8,
                upper_shadow_pct: 0.0,
                lower_shadow_pct: 0.0,
            },
        ];

        let row_data =
            build_simulated_row_data_series(sample_row_data(), &inputs).expect("simulate series");

        assert_eq!(row_data.trade_dates.len(), 4);
        assert_eq!(row_data.trade_dates[2], "20260408");
        assert_eq!(row_data.trade_dates[3], "20260409");
        assert_eq!(
            row_data.cols["PRE_CLOSE"].last().copied().flatten(),
            Some(10.71)
        );
    }

    #[test]
    fn run_simulated_scoring_returns_last_simulated_days() {
        const EPS: f64 = 1e-12;
        let source_dir = create_test_source_dir().expect("create source dir");
        let inputs = vec![SimulateBarInput {
            trade_date: Some("20260408".to_string()),
            open_gap_pct: 0.0,
            pct_chg: 5.0,
            pct_chg_relative_to_open: false,
            volume_ratio: 1.5,
            upper_shadow_pct: 0.0,
            lower_shadow_pct: 0.0,
        }];

        let result = run_simulated_scoring(
            source_dir
                .to_str()
                .expect("temp source dir should be valid utf8"),
            sample_row_data(),
            &inputs,
        )
        .expect("run simulated scoring");

        assert_eq!(result.simulated_days, 1);
        assert_eq!(result.days.len(), 1);
        assert_eq!(result.days[0].trade_date, "20260408");
        assert!((result.days[0].total_score - 60.0).abs() <= EPS);
        assert_eq!(result.days[0].rule_scores.len(), 1);
        assert_eq!(result.days[0].rule_scores[0].rule_name, "UP_DAY");
        assert!((result.days[0].rule_scores[0].rule_score - 10.0).abs() <= EPS);

        let _ = remove_dir_all(source_dir);
    }
}
