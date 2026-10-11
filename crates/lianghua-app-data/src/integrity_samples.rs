use std::collections::HashSet;

use duckdb::{Connection, params};
use lianghua_data::data::{
    DataReader,
    cyq::{CyqConfig, compute_cyq_snapshots_from_row_data},
};

// 只返回保存值与独立重算值；调用方负责误差判定和报告，不写任何结果库。
pub(super) fn recompute_samples(
    source_path: &str,
    results: &Connection,
    component: &str,
    date: &str,
) -> Result<Vec<(String, f64, f64)>, String> {
    let mut comparisons = Vec::new();
    if component == "concept" {
        let mut stmt = results.prepare("SELECT performance_type, concept, performance_pct FROM concept_performance WHERE trade_date=? ORDER BY hash(concept || performance_type || trade_date) LIMIT 3").map_err(|e| e.to_string())?;
        let samples = stmt
            .query_map([date], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, f64>(2)?,
                ))
            })
            .map_err(|e| e.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        let recomputed =
            lianghua_data::data::concept_performance_data::compute_concept_performance_range(
                source_path,
                date,
                date,
            )?;
        for (kind, name, saved) in samples {
            let computed = recomputed
                .iter()
                .find(|row| row.performance_type == kind && row.concept == name)
                .ok_or_else(|| format!("{date} {kind}/{name} 无法按当前成分重算"))?;
            comparisons.push((format!("{kind}/{name}"), saved, computed.performance_pct));
        }
        return Ok(comparisons);
    }
    let table = match component {
        "ranking" => "score_summary",
        "legacy-chip" => "cyq_snapshot",
        "similarity" => "strategy_trigger_similarity_rank",
        _ => return Err("未知抽样组件".to_string()),
    };
    let saved_config: Option<String> = if component == "similarity" {
        Some(results.query_row("SELECT config_key FROM strategy_trigger_similarity_rank_meta WHERE trade_date=? ORDER BY generated_at_epoch_seconds DESC LIMIT 1", [date], |row| row.get(0)).map_err(|e| e.to_string())?)
    } else {
        None
    };
    let sql = if saved_config.is_some() {
        "SELECT DISTINCT ts_code FROM strategy_trigger_similarity_rank WHERE trade_date=? AND config_key=? ORDER BY hash(ts_code || ?) LIMIT 3".to_string()
    } else {
        format!(
            "SELECT DISTINCT ts_code FROM {table} WHERE trade_date=? ORDER BY hash(ts_code || ?) LIMIT 3"
        )
    };
    let arguments = if let Some(config) = &saved_config {
        vec![date, config.as_str(), date]
    } else {
        vec![date, date]
    };
    let mut stmt = results.prepare(&sql).map_err(|e| e.to_string())?;
    let codes = stmt
        .query_map(duckdb::params_from_iter(arguments), |row| {
            row.get::<_, String>(0)
        })
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    if component == "similarity" {
        let config = saved_config
            .as_deref()
            .ok_or_else(|| "缺少相似度参数".to_string())?;
        let recomputed = lianghua_app_strategy::trigger_similarity::ranking::recompute_strategy_trigger_similarity_samples(source_path, date, config, &codes)?;
        for code in codes {
            let row = recomputed
                .iter()
                .find(|row| row.ts_code == code)
                .ok_or_else(|| format!("{date} {code} 相似度未生成重算结果"))?;
            let saved: (f64, f64, Option<f64>, Option<f64>) = results.query_row("SELECT confidence, effective_sample_count, prediction_signal, best_similarity FROM strategy_trigger_similarity_rank WHERE trade_date=? AND ts_code=? AND config_key=?", params![date, code, config], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))).map_err(|e| e.to_string())?;
            for (field, old, new) in [
                ("confidence", Some(saved.0), Some(row.confidence)),
                (
                    "effective_sample_count",
                    Some(saved.1),
                    Some(row.effective_sample_count),
                ),
                ("prediction_signal", saved.2, row.prediction_signal),
                ("best_similarity", saved.3, row.best_similarity),
            ] {
                match (old, new) {
                    (Some(old), Some(new)) => {
                        comparisons.push((format!("{code}/{field}"), old, new))
                    }
                    (None, None) => {}
                    _ => comparisons.push((format!("{code}/{field}"), f64::NAN, f64::NAN)),
                }
            }
        }
        return Ok(comparisons);
    }
    for code in codes {
        if component == "ranking" {
            let (summary, _, _) = lianghua_scoring::scoring::runner::scoring_single_period(
                source_path,
                None,
                &code,
                "qfq",
                date,
                date,
            )?;
            let computed = summary
                .iter()
                .find(|row| row.trade_date == date)
                .ok_or_else(|| format!("{date} {code} 未生成重算评分"))?;
            let saved = results
                .query_row(
                    "SELECT total_score FROM score_summary WHERE ts_code=? AND trade_date=?",
                    params![code, date],
                    |row| row.get::<_, f64>(0),
                )
                .map_err(|e| e.to_string())?;
            comparisons.push((format!("{code}/total_score"), saved, computed.total_score));
        } else {
            let config = results.query_row("SELECT range, factor, min_accuracy FROM cyq_snapshot WHERE ts_code=? AND trade_date=? AND adj_type='qfq'", params![code, date], |row| Ok(CyqConfig { range: row.get(0)?, factor: row.get(1)?, min_accuracy: row.get(2)? })).map_err(|e| e.to_string())?;
            let reader = DataReader::new_with_runtime_keys(
                source_path,
                &HashSet::from(["O", "H", "L", "C", "TOR"].map(str::to_string)),
            )?;
            let data = if config.range == 0 {
                reader.load_one(&code, "qfq", "00000000", date)?
            } else {
                reader.load_one_tail_rows(&code, "qfq", date, config.range)?
            };
            let snapshots = compute_cyq_snapshots_from_row_data(&data, config)?;
            let computed = snapshots
                .iter()
                .find(|row| row.trade_date.as_deref() == Some(date))
                .ok_or_else(|| format!("{date} {code} 未生成旧筹码重算结果"))?;
            let saved: (f64, f64, f64) = results.query_row("SELECT total_chips, benefit_part, avg_cost FROM cyq_snapshot WHERE ts_code=? AND trade_date=? AND adj_type='qfq'", params![code, date], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).map_err(|e| e.to_string())?;
            for (field, old, new) in [
                ("total_chips", saved.0, computed.total_chips),
                ("benefit_part", saved.1, computed.benefit_part),
                ("avg_cost", saved.2, computed.avg_cost),
            ] {
                comparisons.push((format!("{code}/{field}"), old, new));
            }
        }
    }
    Ok(comparisons)
}
