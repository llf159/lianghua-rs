use super::digest;
use duckdb::Connection;
use lianghua_data::data::open_db_readonly;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{Cursor, Read, Write},
    path::Path,
};
use zip::{ZipArchive, ZipWriter, write::SimpleFileOptions};

#[derive(Clone, serde::Deserialize, serde::Serialize)]
pub(super) struct RankSources {
    version: u32,
    pub dates: BTreeMap<String, Option<String>>,
    pub strategies: BTreeMap<String, String>,
}

pub(super) fn rank_sources(path: &str, bytes: &[u8]) -> Result<RankSources, String> {
    let mut archive = ZipArchive::new(Cursor::new(bytes)).map_err(|e| e.to_string())?;
    let dates: Vec<String> = if is_snapshot(path) {
        serde_json::from_reader(
            archive
                .by_name("snapshot.json")
                .map_err(|_| "榜单缺少日期清单")?
                .take(1025),
        )
        .map_err(|e| format!("榜单日期清单无效：{e}"))?
    } else {
        vec![date_from_path(path)?]
    };
    if dates.is_empty()
        || dates.len() > 5
        || dates
            .iter()
            .any(|date| date_from_path(&format!("ranking/{date}.zip")).is_err())
    {
        return Err("排名日期清单无效".into());
    }
    let sources = match archive.by_name("strategies.json") {
        Ok(entry) => {
            if entry.size() > 16 * 1024 * 1024 {
                return Err("排名策略快照过大".into());
            }
            serde_json::from_reader::<_, RankSources>(entry.take(16 * 1024 * 1024 + 1))
                .map_err(|e| e.to_string())?
        }
        Err(zip::result::ZipError::FileNotFound) => RankSources {
            version: 1,
            dates: dates.iter().map(|date| (date.clone(), None)).collect(),
            strategies: BTreeMap::new(),
        },
        Err(error) => return Err(error.to_string()),
    };
    if sources.version != 1
        || sources.dates.keys().cloned().collect::<BTreeSet<_>>() != dates.into_iter().collect()
        || sources
            .strategies
            .iter()
            .any(|(hash, text)| digest(text.as_bytes()) != *hash)
        || sources
            .dates
            .values()
            .flatten()
            .any(|hash| !sources.strategies.contains_key(hash))
        || sources.strategies.keys().any(|hash| {
            !sources
                .dates
                .values()
                .any(|value| value.as_ref() == Some(hash))
        })
    {
        return Err("排名策略来源清单无效".into());
    }
    Ok(sources)
}

pub(super) fn same_rank_strategy(path: &str, left: &[u8], right: &[u8]) -> Result<bool, String> {
    let left = rank_sources(path, left)?;
    let right = rank_sources(path, right)?;
    Ok(left.dates.values().all(Option::is_some)
        && right.dates.values().all(Option::is_some)
        && left.strategies.keys().collect::<BTreeSet<_>>() == right.strategies.keys().collect()
        && left
            .dates
            .iter()
            .all(|(date, hash)| right.dates.get(date).is_none_or(|other| other == hash)))
}

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

// 同一同步包只携带最新结果所用策略的日期，避免包本身混装不同来源。
fn snapshot_dates(conn: &Connection, similarity: bool) -> Result<Vec<String>, String> {
    let has_sources: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='rank_compute_strategy')", [], |r| r.get(0)).map_err(|e| e.to_string())?;
    let sql = if similarity {
        "SELECT DISTINCT trade_date FROM strategy_trigger_similarity_rank_meta ORDER BY trade_date DESC LIMIT 1"
    } else if has_sources {
        "SELECT DISTINCT d.trade_date FROM score_summary d LEFT JOIN rank_compute_strategy p USING(trade_date) WHERE p.strategy_text IS NOT DISTINCT FROM (SELECT strategy_text FROM rank_compute_strategy WHERE trade_date=(SELECT MAX(trade_date) FROM score_summary)) ORDER BY d.trade_date DESC LIMIT 5"
    } else {
        "SELECT DISTINCT trade_date FROM score_summary ORDER BY trade_date DESC LIMIT 5"
    };
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    stmt.query_map([], |r| r.get::<_, String>(0))
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())
}

pub(super) fn rank_replaces_all(source: &Path, path: &str, bytes: &[u8]) -> Result<bool, String> {
    if !path.starts_with("ranking/") {
        return Ok(false);
    }
    let incoming = rank_sources(path, bytes)?;
    if incoming.strategies.len() > 1
        || (!incoming.strategies.is_empty() && incoming.dates.values().any(Option::is_none))
    {
        return Err("此旧排名包混有不同策略快照，请在来源设备更新后重新上传".into());
    }
    let db = db_path(source, false);
    if !db.exists() {
        return Ok(false);
    }
    let conn = open_db_readonly(db).map_err(|e| e.to_string())?;
    let has_rows: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='score_summary')", [], |r| r.get(0)).map_err(|e| e.to_string())?;
    if !has_rows {
        return Ok(false);
    }
    let has_rows: bool = conn
        .query_row("SELECT EXISTS(SELECT 1 FROM score_summary)", [], |r| {
            r.get(0)
        })
        .map_err(|e| e.to_string())?;
    if !has_rows {
        return Ok(false);
    }
    let has_sources: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='rank_compute_strategy')", [], |r| r.get(0)).map_err(|e| e.to_string())?;
    if !has_sources || incoming.dates.values().any(Option::is_none) {
        return Ok(true);
    }
    conn.query_row("SELECT EXISTS(SELECT 1 FROM (SELECT DISTINCT trade_date FROM score_summary) d LEFT JOIN rank_compute_strategy p USING(trade_date) WHERE p.strategy_text IS NULL OR p.strategy_text <> ?)", [incoming.strategies.values().next().ok_or("排名包缺少策略来源")?], |r| r.get(0)).map_err(|e| e.to_string())
}

// 整库替换复核文件和 WAL 的修改水位，避免为确认操作反复哈希多年历史。
pub(super) fn local_revision(source: &Path, path: &str) -> Result<String, String> {
    let db = db_path(source, path.starts_with("similarity/"));
    let mut stamp = String::new();
    for file in [&db, &db.with_extension("db.wal")] {
        match fs::metadata(file) {
            Ok(meta) => stamp.push_str(&format!(
                "{}:{:?};",
                meta.len(),
                meta.modified().map_err(|e| e.to_string())?
            )),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                stamp.push_str("missing;")
            }
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok(digest(stamp.as_bytes()))
}

pub(super) fn export(source: &Path) -> Result<BTreeMap<String, Vec<u8>>, String> {
    let mut result = BTreeMap::new();
    for (similarity, kind, main) in [
        (false, "ranking", "score_summary"),
        (true, "similarity", "strategy_trigger_similarity_rank_meta"),
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
        let dates = snapshot_dates(&conn, similarity)?;
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
    let snapshot = is_snapshot(path);
    let date = if snapshot {
        None
    } else {
        Some(date_from_path(path)?)
    };
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
    let dates = if let Some(date) = date {
        let mut stmt = conn.prepare(&format!("SELECT DISTINCT trade_date FROM {main} WHERE replace(trade_date, '-', '') = {} ORDER BY trade_date DESC", sql_string(&date))).map_err(|e| e.to_string())?;
        stmt.query_map([], |r| r.get::<_, String>(0))
            .map_err(|e| e.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?
    } else {
        snapshot_dates(&conn, similarity)?
    };
    if dates.is_empty() {
        Ok(None)
    } else {
        pack(&conn, similarity, &dates, snapshot).map(Some)
    }
}

// 按明确的候选日期截取本机交集，供覆盖预览、提交复核和原数据备份共用。
pub(super) fn export_dates(
    source: &Path,
    path: &str,
    dates: &[String],
) -> Result<Option<Vec<u8>>, String> {
    let similarity = path.starts_with("similarity/");
    if !db_path(source, similarity).exists() {
        return Ok(None);
    }
    let conn = open_db_readonly(db_path(source, similarity)).map_err(|e| e.to_string())?;
    let main = tables(similarity)[if similarity { 1 } else { 0 }].0;
    let exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name=?)",
            [main],
            |r| r.get(0),
        )
        .map_err(|e| e.to_string())?;
    if !exists {
        return Ok(None);
    }
    let date_list = dates
        .iter()
        .map(|date| sql_string(date))
        .collect::<Vec<_>>()
        .join(",");
    if date_list.is_empty() {
        return Ok(None);
    }
    let mut stmt = conn.prepare(&format!("SELECT DISTINCT replace(trade_date, '-', '') FROM {main} WHERE replace(trade_date, '-', '') IN ({date_list}) ORDER BY 1")).map_err(|e| e.to_string())?;
    let dates = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    if dates.is_empty() {
        Ok(None)
    } else {
        pack(&conn, similarity, &dates, true).map(Some)
    }
}

pub(super) fn local_date_summary(source: &Path, path: &str) -> Result<Vec<String>, String> {
    let similarity = path.starts_with("similarity/");
    if !db_path(source, similarity).exists() {
        return Ok(Vec::new());
    }
    let conn = open_db_readonly(db_path(source, similarity)).map_err(|e| e.to_string())?;
    let main = tables(similarity)[if similarity { 1 } else { 0 }].0;
    let exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name=?)",
            [main],
            |r| r.get(0),
        )
        .map_err(|e| e.to_string())?;
    if !exists {
        return Ok(Vec::new());
    }
    let mut stmt = conn
        .prepare(&format!(
            "SELECT DISTINCT replace(trade_date, '-', '') FROM {main} ORDER BY 1"
        ))
        .map_err(|e| e.to_string())?;
    stmt.query_map([], |r| r.get::<_, String>(0))
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())
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
    if !similarity {
        let mut sources = RankSources {
            version: 1,
            dates: BTreeMap::new(),
            strategies: BTreeMap::new(),
        };
        let exists: bool = transaction.query_row("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='rank_compute_strategy')", [], |r| r.get(0)).map_err(|e| e.to_string())?;
        for date in dates {
            let text = if exists {
                let mut stmt = transaction.prepare("SELECT strategy_text FROM rank_compute_strategy WHERE replace(trade_date, '-', '') = ?").map_err(|e| e.to_string())?;
                let mut rows = stmt
                    .query([date.replace('-', "")])
                    .map_err(|e| e.to_string())?;
                rows.next()
                    .map_err(|e| e.to_string())?
                    .map(|row| row.get::<_, String>(0))
                    .transpose()
                    .map_err(|e| e.to_string())?
            } else {
                None
            };
            let hash = text.map(|text| {
                let text = text.replace("\r\n", "\n");
                let hash = digest(text.as_bytes());
                sources.strategies.insert(hash.clone(), text);
                hash
            });
            sources.dates.insert(date.replace('-', ""), hash);
        }
        zip.start_file(
            "strategies.json",
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored),
        )
        .map_err(|e| e.to_string())?;
        zip.write_all(&serde_json::to_vec(&sources).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
    }
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

pub(super) fn import(
    source: &Path,
    path: &str,
    bytes: &[u8],
    replace_all: bool,
) -> Result<(), String> {
    let required = rank_replaces_all(source, path, bytes)?;
    if required && !replace_all {
        return Err("策略快照不同或来源未知，必须明确选择替换本地全部排名历史".into());
    }
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
    let sources = if similarity {
        None
    } else {
        Some(rank_sources(path, bytes)?)
    };
    let has_sources = archive.by_name("strategies.json").is_ok();
    let expected = tables(similarity);
    if archive.len()
        != expected.len() + usize::from(is_snapshot(path)) + usize::from(!similarity && has_sources)
    {
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
        if replace_all || table.ends_with("active_config") {
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
    if let Some(sources) = sources {
        transaction.execute_batch("CREATE TABLE IF NOT EXISTS rank_compute_strategy (trade_date VARCHAR PRIMARY KEY, strategy_text VARCHAR NOT NULL)").map_err(|e| e.to_string())?;
        transaction.execute(&if replace_all { "DELETE FROM rank_compute_strategy".to_string() } else { format!("DELETE FROM rank_compute_strategy WHERE replace(trade_date, '-', '') IN ({date_list})") }, []).map_err(|e| e.to_string())?;
        if replace_all {
            transaction.execute_batch("DROP TABLE IF EXISTS strategy_tracking_state; DROP TABLE IF EXISTS strategy_tracking_meta; DROP TABLE IF EXISTS convolution_rank;").map_err(|e| e.to_string())?;
        }
        for (date, hash) in sources.dates {
            if let Some(hash) = hash {
                transaction
                    .execute(
                        "INSERT INTO rank_compute_strategy VALUES (?, ?)",
                        [&date, &sources.strategies[&hash]],
                    )
                    .map_err(|e| e.to_string())?;
            }
        }
    }
    transaction.commit().map_err(|e| e.to_string())?;
    Ok(())
}
