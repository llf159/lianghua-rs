use super::{atomic_write, digest};
use duckdb::Connection;
use lianghua_data::data::open_db_readonly;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{Cursor, Read, Write},
    path::Path,
};
use zip::{ZipArchive, ZipWriter, write::SimpleFileOptions};

// 每类榜单一个逻辑数据库快照；包内所有日期的完整表在同一事务恢复。
fn tables(similarity: bool) -> &'static [(&'static str, &'static str, &'static str)] {
    if similarity {
        &[
            (
                "strategy_trigger_similarity_rank",
                "",
                "trade_date,config_key,rank,ts_code,name,industry,concept,original_score,original_rank,ranking_score,prediction_signal,confidence,sample_count,effective_sample_count,expected_return_pct,expected_excess_return_pct,shrunk_excess_return_pct,excess_positive_rate,expected_mfe_pct,expected_mae_pct,average_similarity,best_similarity,trigger_count,top_matches_json",
            ),
            (
                "strategy_trigger_similarity_rank_meta",
                "",
                "trade_date,config_key,data_signature,generated_at_epoch_seconds,historical_cutoff_date,universe_count,ranked_count,candidate_universe_count,candidate_anchor_count,evaluated_anchor_count,elapsed_ms,timings_json",
            ),
            (
                "strategy_trigger_similarity_summary",
                "trade_date,ts_code",
                "trade_date,ts_code,rank",
            ),
            (
                "strategy_trigger_similarity_active_config",
                "id",
                "id,config_key,algorithm_version,window_trade_days,pool_segments,outcome_trade_days,sample_gap_trade_days,benchmark_index_code,scope_trade_date,scope_signature,updated_at_epoch_seconds",
            ),
        ]
    } else {
        &[
            (
                "score_summary",
                "ts_code,trade_date",
                "ts_code,trade_date,total_score,rank",
            ),
            (
                "rule_details",
                "ts_code,trade_date,rule_name",
                "ts_code,trade_date,rule_name,rule_score",
            ),
            (
                "scene_details",
                "ts_code,trade_date,scene_name",
                "ts_code,trade_date,scene_name,direction,stage,stage_score,risk_score,confirm_strength,risk_intensity,scene_rank",
            ),
        ]
    }
}

pub(super) fn is_snapshot(path: &str) -> bool {
    matches!(
        path,
        "ranking/scoring_result.zip" | "similarity/strategy_similarity.zip"
    )
}

pub(super) fn date_from_path(path: &str) -> Result<String, String> {
    let (kind, file) = path.split_once('/').ok_or("榜单路径无效")?;
    let date = file.strip_suffix(".zip").ok_or("榜单文件格式无效")?;
    if !matches!(kind, "ranking" | "similarity")
        || date.len() != 8
        || !date.bytes().all(|b| b.is_ascii_digit())
        || chrono::NaiveDate::parse_from_str(date, "%Y%m%d").is_err()
    {
        return Err("榜单日期无效".into());
    }
    Ok(date.into())
}
fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
fn db_path(source: &Path, similarity: bool) -> std::path::PathBuf {
    source.join(if similarity {
        "strategy_similarity.db"
    } else {
        "scoring_result.db"
    })
}

pub(super) fn export(source: &Path) -> Result<BTreeMap<String, Vec<u8>>, String> {
    let mut result = BTreeMap::new();
    for (similarity, count, kind, main) in [
        (false, 5, "ranking", "score_summary"),
        (
            true,
            1,
            "similarity",
            "strategy_trigger_similarity_rank_meta",
        ),
    ] {
        let db = db_path(source, similarity);
        if !db.exists() {
            continue;
        }
        let conn = open_db_readonly(&db).map_err(|e| format!("读取榜单库失败：{e}"))?;
        let exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = ?)",
                [main],
                |r| r.get(0),
            )
            .map_err(|e| e.to_string())?;
        if !exists {
            continue;
        }
        let mut stmt = conn
            .prepare(&format!(
                "SELECT DISTINCT trade_date FROM {main} ORDER BY trade_date DESC LIMIT {count}"
            ))
            .map_err(|e| e.to_string())?;
        let dates = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(|e| e.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        drop(stmt);
        if !dates.is_empty() {
            let path = format!(
                "{kind}/{}.zip",
                if similarity {
                    "strategy_similarity"
                } else {
                    "scoring_result"
                }
            );
            result.insert(path, pack(&conn, similarity, &dates, true)?);
        }
    }
    Ok(result)
}

pub(super) fn export_day(source: &Path, path: &str) -> Result<Option<Vec<u8>>, String> {
    if is_snapshot(path) {
        return export(source).map(|mut snapshots| snapshots.remove(path));
    }
    let date = date_from_path(path)?;
    let similarity = path.starts_with("similarity/");
    let db = db_path(source, similarity);
    if !db.exists() {
        return Ok(None);
    }
    let conn = open_db_readonly(db).map_err(|e| e.to_string())?;
    let main = tables(similarity)[if similarity { 1 } else { 0 }].0;
    let exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = ?)",
            [main],
            |r| r.get(0),
        )
        .map_err(|e| e.to_string())?;
    if !exists {
        return Ok(None);
    }
    let present: bool = conn
        .query_row(
            &format!("SELECT EXISTS(SELECT 1 FROM {main} WHERE replace(trade_date, '-', '') = ?)"),
            [&date],
            |r| r.get(0),
        )
        .map_err(|e| e.to_string())?;
    if !present {
        return Ok(None);
    }
    pack(&conn, similarity, &[date], false).map(Some)
}

fn pack(
    conn: &Connection,
    similarity: bool,
    dates: &[String],
    snapshot: bool,
) -> Result<Vec<u8>, String> {
    let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let mut zip = ZipWriter::new(Cursor::new(Vec::new()));
    if snapshot {
        zip.start_file(
            "snapshot.json",
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored),
        )
        .map_err(|e| e.to_string())?;
        zip.write_all(
            &serde_json::to_vec(
                &dates
                    .iter()
                    .map(|date| date.replace('-', ""))
                    .collect::<Vec<_>>(),
            )
            .map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
    }
    let date_list = dates
        .iter()
        .map(|date| sql_string(&date.replace('-', "")))
        .collect::<Vec<_>>()
        .join(",");
    let transaction = conn.unchecked_transaction().map_err(|e| e.to_string())?;
    for (table, _, _) in tables(similarity) {
        let present: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = ?)",
                [table],
                |r| r.get(0),
            )
            .map_err(|e| e.to_string())?;
        if !present {
            return Err(format!("榜单缺少 {table}，请先完成计算；未上传不完整日期"));
        }
        let target = temp.path().join(format!("{table}.parquet"));
        let filter = if table.ends_with("active_config") {
            String::new()
        } else {
            format!("WHERE replace(trade_date, '-', '') IN ({date_list})")
        };
        transaction.execute_batch(&format!("COPY (SELECT * FROM {table} {filter} ORDER BY ALL) TO {} (FORMAT PARQUET, COMPRESSION ZSTD)", sql_string(&target.to_string_lossy()))).map_err(|e| e.to_string())?;
        zip.start_file(
            format!("{table}.parquet"),
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored),
        )
        .map_err(|e| e.to_string())?;
        zip.write_all(&fs::read(target).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
    }
    transaction.commit().map_err(|e| e.to_string())?;
    zip.finish()
        .map(|v| v.into_inner())
        .map_err(|e| e.to_string())
}

pub(super) fn import(source: &Path, path: &str, bytes: &[u8]) -> Result<(), String> {
    let similarity = path.starts_with("similarity/");
    let mut archive = ZipArchive::new(Cursor::new(bytes)).map_err(|e| e.to_string())?;
    let dates: Vec<String> = if is_snapshot(path) {
        let entry = archive
            .by_name("snapshot.json")
            .map_err(|_| "数据库快照缺少日期清单")?;
        if entry.size() > 1024 {
            return Err("数据库快照日期清单过大".into());
        }
        serde_json::from_reader(entry.take(1025))
            .map_err(|e| format!("数据库快照日期清单无效：{e}"))?
    } else {
        vec![date_from_path(path)?]
    };
    if dates.is_empty()
        || dates.len() > if similarity { 1 } else { 5 }
        || dates.iter().collect::<BTreeSet<_>>().len() != dates.len()
        || dates.iter().any(|date| {
            date_from_path(&format!(
                "{}/{date}.zip",
                if similarity { "similarity" } else { "ranking" }
            ))
            .is_err()
        })
    {
        return Err("数据库快照日期范围无效".into());
    }
    let date_list = dates
        .iter()
        .map(|date| sql_string(date))
        .collect::<Vec<_>>()
        .join(",");
    let expected = tables(similarity);
    if archive.len() != expected.len() + usize::from(is_snapshot(path)) {
        return Err("云端榜单表不完整，未替换本地数据".into());
    }
    let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let staging = Connection::open_in_memory().map_err(|e| e.to_string())?;
    for (table, _, columns) in expected {
        let mut entry = archive
            .by_name(&format!("{table}.parquet"))
            .map_err(|_| format!("云端榜单缺少 {table}"))?;
        if entry.size() > 512 * 1024 * 1024 {
            return Err("云端表超过大小限制".into());
        }
        let target = temp.path().join(format!("{table}.parquet"));
        let mut raw = Vec::new();
        entry.read_to_end(&mut raw).map_err(|e| e.to_string())?;
        fs::write(&target, raw).map_err(|e| e.to_string())?;
        staging
            .execute_batch(&format!(
                "CREATE TABLE {table} AS SELECT * FROM read_parquet({})",
                sql_string(&target.to_string_lossy())
            ))
            .map_err(|e| e.to_string())?;
        let actual: String = staging.query_row("SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_name = ?", [table], |r| r.get(0)).map_err(|e| e.to_string())?;
        if actual != *columns {
            return Err(format!("云端 {table} 列结构不兼容，未替换本地数据"));
        }
        if !table.ends_with("active_config") {
            let invalid: i64 = staging.query_row(&format!("SELECT COUNT(*) FROM {table} WHERE trade_date IS NULL OR replace(trade_date, '-', '') NOT IN ({date_list})"), [], |r| r.get(0)).map_err(|e| e.to_string())?;
            if invalid > 0 {
                return Err("云端榜单包含其他日期，未替换本地数据".into());
            }
        } else {
            let invalid: i64 = staging
                .query_row(
                    &format!("SELECT COUNT(*) FROM {table} WHERE id IS NULL OR id <> 1"),
                    [],
                    |r| r.get(0),
                )
                .map_err(|e| e.to_string())?;
            if invalid > 0 {
                return Err("云端相似度配置无效".into());
            }
        }
    }
    let main = expected[if similarity { 1 } else { 0 }].0;
    let mut stmt = staging
        .prepare(&format!(
            "SELECT DISTINCT replace(trade_date, '-', '') FROM {main}"
        ))
        .map_err(|e| e.to_string())?;
    let actual_dates = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(|e| e.to_string())?
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(|e| e.to_string())?;
    if actual_dates != dates.iter().cloned().collect() {
        return Err("数据库快照日期清单与内容不符".into());
    }
    // 备份当前快照；即使后续恢复失败，事务也会回滚正式库。
    if let Some(old) = export_day(source, path)? {
        atomic_write(
            &source.join("cloud_sync_backups").join(format!(
                "{}-{}.zip",
                if similarity { "similarity" } else { "ranking" },
                digest(&old)
            )),
            &old,
        )?;
    }
    fs::create_dir_all(source).map_err(|e| e.to_string())?;
    let mut conn = Connection::open(db_path(source, similarity)).map_err(|e| e.to_string())?;
    let transaction = conn.transaction().map_err(|e| e.to_string())?;
    for (table, primary, _) in expected {
        let parquet = sql_string(
            &temp
                .path()
                .join(format!("{table}.parquet"))
                .to_string_lossy(),
        );
        let exists: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = ?)",
                [table],
                |r| r.get(0),
            )
            .map_err(|e| e.to_string())?;
        if !exists {
            transaction
                .execute_batch(&format!(
                    "CREATE TABLE {table} AS SELECT * FROM read_parquet({parquet}) WHERE FALSE"
                ))
                .map_err(|e| e.to_string())?;
            if !primary.is_empty() {
                transaction
                    .execute_batch(&format!("ALTER TABLE {table} ADD PRIMARY KEY ({primary})"))
                    .map_err(|e| e.to_string())?;
            }
        }
        if table.ends_with("active_config") {
            transaction
                .execute(&format!("DELETE FROM {table}"), [])
                .map_err(|e| e.to_string())?;
        } else {
            transaction
                .execute(
                    &format!(
                        "DELETE FROM {table} WHERE replace(trade_date, '-', '') IN ({date_list})"
                    ),
                    [],
                )
                .map_err(|e| e.to_string())?;
        }
        transaction
            .execute_batch(&format!(
                "INSERT INTO {table} BY NAME SELECT * FROM read_parquet({parquet})"
            ))
            .map_err(|e| e.to_string())?;
    }
    transaction.commit().map_err(|e| e.to_string())?;
    Ok(())
}
