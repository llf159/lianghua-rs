use duckdb::{Connection, params};

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
    if component != "similarity" {
        return Err("未知抽样组件".to_string());
    }
    let config: String = results.query_row("SELECT config_key FROM strategy_trigger_similarity_rank_meta WHERE trade_date=? ORDER BY generated_at_epoch_seconds DESC LIMIT 1", [date], |row| row.get(0)).map_err(|e| e.to_string())?;
    let sql = "SELECT DISTINCT ts_code FROM strategy_trigger_similarity_rank WHERE trade_date=? AND config_key=? ORDER BY hash(ts_code || ?) LIMIT 3";
    let mut stmt = results.prepare(sql).map_err(|e| e.to_string())?;
    let codes = stmt
        .query_map(params![date, &config, date], |row| row.get::<_, String>(0))
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    let recomputed = lianghua_app_strategy::trigger_similarity::ranking::recompute_strategy_trigger_similarity_samples(source_path, date, &config, &codes)?;
    for code in codes {
        let row = recomputed
            .iter()
            .find(|row| row.ts_code == code)
            .ok_or_else(|| format!("{date} {code} 相似度未生成重算结果"))?;
        let saved: (f64, f64, Option<f64>, Option<f64>) = results.query_row("SELECT confidence, effective_sample_count, prediction_signal, best_similarity FROM strategy_trigger_similarity_rank WHERE trade_date=? AND ts_code=? AND config_key=?", params![date, code, &config], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))).map_err(|e| e.to_string())?;
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
                (Some(old), Some(new)) => comparisons.push((format!("{code}/{field}"), old, new)),
                (None, None) => {}
                _ => comparisons.push((format!("{code}/{field}"), f64::NAN, f64::NAN)),
            }
        }
    }
    Ok(comparisons)
}
