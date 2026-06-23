use std::{
    collections::{HashMap, HashSet},
    fs::{self, create_dir_all, read_to_string},
    path::Path,
    sync::{
        Arc,
        mpsc::{Receiver, sync_channel},
    },
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

use duckdb::{
    Appender, Connection,
    arrow::{
        array::{ArrayRef, Float64Array, Int32Array, StringArray, builder::StringBuilder},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    params,
};
use rayon::prelude::*;

use crate::{
    data::{
        DataReader, RowData, chip_change_rule_path,
        cyq_chen::{
            ChenChipBin, ChenChipConfig, ChenChipSnapshot, CompiledChipChangeConfig,
            collect_chen_chip_runtime_keys,
            compute_chen_chip_snapshots_from_initial_bins_with_compiled_config,
            compute_chen_chip_snapshots_with_compiled_config, estimate_chen_chip_expression_warmup,
            load_compiled_chip_change_config, round_chen_chip_snapshot, round_chen_chip_value,
        },
        cyq_chen_db_path, load_trade_date_list, source_db_path,
    },
    download::runner::{DownloadProgress, DownloadProgressCallback},
    scoring::tools::{inject_stock_extra_fields, load_st_list, load_total_share_map},
};

const CYQ_CHEN_SNAPSHOT_TABLE: &str = "cyq_chen_snapshot";
const CYQ_CHEN_BIN_TABLE: &str = "cyq_chen_bin";
const CYQ_CHEN_META_TABLE: &str = "cyq_chen_meta";
const DEFAULT_ADJ_TYPE: &str = "qfq";
const CYQ_CHEN_GROUP_SIZE: usize = 128;
const CYQ_CHEN_GROUP_SIZE_INCREMENTAL: usize = 8;
const CYQ_CHEN_QUEUE_BOUND: usize = 8;
const CYQ_CHEN_FLUSH_BATCH_SIZE: usize = 32;
const CYQ_CHEN_SCHEMA_VERSION: &str = "2";

#[derive(Debug, Clone, PartialEq)]
pub struct CyqChenRebuildSummary {
    pub snapshot_rows: usize,
    pub bin_rows: usize,
    pub warmup_days: usize,
    pub bucket_pct: f64,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CyqChenStrategyMaintenanceStatus {
    pub db_exists: bool,
    pub has_data: bool,
    pub strategy_changed: bool,
    pub detail: String,
}

#[derive(Debug)]
struct ComputedCyqChenStock {
    ts_code: String,
    snapshots: Vec<ChenChipSnapshot>,
}

struct CyqChenInitialState {
    state_trade_date: String,
    bins: Vec<ChenChipBin>,
    main_ratio_history: Vec<Arc<Vec<Option<f64>>>>,
}

#[derive(Debug, Default)]
struct CyqChenWriteBatch {
    stocks: Vec<ComputedCyqChenStock>,
}

#[derive(Debug)]
enum CyqChenWriteMessage {
    Batch(CyqChenWriteBatch),
    Abort(String),
}

pub fn init_cyq_chen_db(db_path: &Path) -> Result<(), String> {
    if let Some(parent_dir) = db_path.parent() {
        if !parent_dir.as_os_str().is_empty() {
            create_dir_all(parent_dir).map_err(|e| format!("创建筹码库目录失败:{e}"))?;
        }
    }

    let conn = Connection::open(db_path).map_err(|e| format!("打开筹码库失败:{e}"))?;
    conn.execute(
        r#"
        CREATE TABLE IF NOT EXISTS cyq_chen_snapshot (
            ts_code VARCHAR,
            trade_date VARCHAR,
            adj_type VARCHAR,
            warmup_days INTEGER,
            bucket_pct DOUBLE,
            close DOUBLE,
            min_price DOUBLE,
            max_price DOUBLE,
            main_total DOUBLE,
            retail_total DOUBLE,
            total_chips DOUBLE,
            total_profit_ratio DOUBLE,
            total_trapped_ratio DOUBLE,
            main_avg_cost DOUBLE,
            chip_peak_price DOUBLE,
            percent_70_price_low DOUBLE,
            percent_70_price_high DOUBLE,
            percent_70_concentration DOUBLE,
            percent_90_price_low DOUBLE,
            percent_90_price_high DOUBLE,
            percent_90_concentration DOUBLE,
            main_profit_ratio DOUBLE,
            main_trapped_ratio DOUBLE
        )
        "#,
        [],
    )
    .map_err(|e| format!("创建cyq_chen_snapshot失败:{e}"))?;
    conn.execute(
        r#"
        CREATE TABLE IF NOT EXISTS cyq_chen_bin (
            ts_code VARCHAR,
            trade_date VARCHAR,
            adj_type VARCHAR,
            bin_index INTEGER,
            price DOUBLE,
            price_low DOUBLE,
            price_high DOUBLE,
            main_chip DOUBLE,
            retail_chip DOUBLE,
            total_chip DOUBLE
        )
        "#,
        [],
    )
    .map_err(|e| format!("创建cyq_chen_bin失败:{e}"))?;
    conn.execute(
        r#"
        CREATE TABLE IF NOT EXISTS cyq_chen_meta (
            key VARCHAR PRIMARY KEY,
            value VARCHAR
        )
        "#,
        [],
    )
    .map_err(|e| format!("创建cyq_chen_meta失败:{e}"))?;

    ensure_cyq_chen_snapshot_columns(&conn)?;
    ensure_cyq_chen_snapshot_index(&conn)?;

    Ok(())
}

fn drop_cyq_chen_db_indexes(conn: &Connection) -> Result<(), String> {
    conn.execute("DROP INDEX IF EXISTS idx_cyq_chen_snapshot_stock_date", [])
        .map_err(|e| format!("删除cyq_chen_snapshot股票日期索引失败:{e}"))?;
    Ok(())
}

fn ensure_cyq_chen_db_indexes(conn: &Connection) -> Result<(), String> {
    ensure_cyq_chen_snapshot_index(conn)
}

fn ensure_cyq_chen_snapshot_index(conn: &Connection) -> Result<(), String> {
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cyq_chen_snapshot_stock_date ON cyq_chen_snapshot(ts_code, adj_type, trade_date)",
        [],
    )
    .map_err(|e| format!("创建cyq_chen_snapshot股票日期索引失败:{e}"))?;
    Ok(())
}

fn path_with_suffix(path: &Path, suffix: &str) -> std::path::PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    value.into()
}

fn remove_cyq_chen_db_artifacts(db_path: &Path) {
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(path_with_suffix(db_path, ".wal"));
    let _ = fs::remove_dir_all(path_with_suffix(db_path, ".tmp"));
}

fn cyq_chen_rebuild_temp_path(db_path: &Path) -> Result<std::path::PathBuf, String> {
    let parent = db_path
        .parent()
        .ok_or_else(|| format!("新筹码库路径缺少父目录: {}", db_path.display()))?;
    let file_name = db_path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| format!("新筹码库文件名不是有效UTF-8: {}", db_path.display()))?;
    let unique_suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    Ok(parent.join(format!(
        ".{file_name}.rebuild-{}-{unique_suffix}.tmp",
        std::process::id()
    )))
}

fn checkpoint_cyq_chen_db_if_exists(db_path: &Path) -> Result<(), String> {
    if !db_path.exists() {
        return Ok(());
    }
    let conn = Connection::open(db_path)
        .map_err(|e| format!("替换前打开旧新筹码库失败, path={}: {e}", db_path.display()))?;
    conn.execute_batch("CHECKPOINT").map_err(|e| {
        format!(
            "替换前检查点旧新筹码库失败, path={}: {e}",
            db_path.display()
        )
    })
}

#[cfg(unix)]
fn replace_cyq_chen_db(temp_path: &Path, db_path: &Path) -> Result<(), String> {
    checkpoint_cyq_chen_db_if_exists(db_path)?;
    let _ = fs::remove_file(path_with_suffix(db_path, ".wal"));
    fs::rename(temp_path, db_path).map_err(|e| {
        format!(
            "替换新筹码库失败, temp={}, target={}: {e}",
            temp_path.display(),
            db_path.display()
        )
    })
}

#[cfg(not(unix))]
fn replace_cyq_chen_db(temp_path: &Path, db_path: &Path) -> Result<(), String> {
    checkpoint_cyq_chen_db_if_exists(db_path)?;
    let backup_path = path_with_suffix(db_path, ".replace-backup");
    remove_cyq_chen_db_artifacts(&backup_path);

    if db_path.exists() {
        fs::rename(db_path, &backup_path).map_err(|e| {
            format!(
                "备份旧新筹码库失败, source={}, backup={}: {e}",
                db_path.display(),
                backup_path.display()
            )
        })?;
    }

    match fs::rename(temp_path, db_path) {
        Ok(()) => {
            remove_cyq_chen_db_artifacts(&backup_path);
            Ok(())
        }
        Err(error) => {
            if backup_path.exists() {
                let _ = fs::rename(&backup_path, db_path);
            }
            Err(format!(
                "替换新筹码库失败, temp={}, target={}: {error}",
                temp_path.display(),
                db_path.display()
            ))
        }
    }
}

fn ensure_cyq_chen_snapshot_columns(conn: &Connection) -> Result<(), String> {
    for (column_name, column_type) in [
        ("total_profit_ratio", "DOUBLE"),
        ("total_trapped_ratio", "DOUBLE"),
        ("main_avg_cost", "DOUBLE"),
        ("chip_peak_price", "DOUBLE"),
        ("percent_70_price_low", "DOUBLE"),
        ("percent_70_price_high", "DOUBLE"),
        ("percent_70_concentration", "DOUBLE"),
        ("percent_90_price_low", "DOUBLE"),
        ("percent_90_price_high", "DOUBLE"),
        ("percent_90_concentration", "DOUBLE"),
        ("main_profit_ratio", "DOUBLE"),
        ("main_trapped_ratio", "DOUBLE"),
    ] {
        let exists = conn
            .query_row(
                r#"
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_name = ? AND column_name = ?
                "#,
                params![CYQ_CHEN_SNAPSHOT_TABLE, column_name],
                |row| row.get::<_, i64>(0),
            )
            .map_err(|e| format!("检查cyq_chen_snapshot字段失败:{e}"))?;
        if exists <= 0 {
            conn.execute(
                &format!(
                    "ALTER TABLE {CYQ_CHEN_SNAPSHOT_TABLE} ADD COLUMN {column_name} {column_type}"
                ),
                [],
            )
            .map_err(|e| format!("补充cyq_chen_snapshot字段 {column_name} 失败:{e}"))?;
        }
    }

    Ok(())
}

fn clear_cyq_chen_tables(db_path: &Path) -> Result<(), String> {
    init_cyq_chen_db(db_path)?;

    let mut conn = Connection::open(db_path).map_err(|e| format!("打开筹码库失败:{e}"))?;
    let tx = conn
        .transaction()
        .map_err(|e| format!("创建筹码库事务失败:{e}"))?;
    tx.execute("DELETE FROM cyq_chen_bin", [])
        .map_err(|e| format!("清空cyq_chen_bin失败:{e}"))?;
    tx.execute("DELETE FROM cyq_chen_snapshot", [])
        .map_err(|e| format!("清空cyq_chen_snapshot失败:{e}"))?;
    tx.execute("DELETE FROM cyq_chen_meta", [])
        .map_err(|e| format!("清空cyq_chen_meta失败:{e}"))?;
    tx.commit().map_err(|e| format!("提交筹码库事务失败:{e}"))?;
    Ok(())
}

fn stable_text_hash(text: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in text.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}:{:016x}", text.len())
}

fn current_chip_change_strategy_hash(source_dir: &str) -> Result<String, String> {
    let path = chip_change_rule_path(source_dir);
    let text = read_to_string(&path).map_err(|e| {
        format!(
            "读取筹码变化策略文件失败，无法校验增量状态: path={}, err={e}",
            path.display()
        )
    })?;
    Ok(stable_text_hash(&text))
}

fn query_cyq_chen_meta_value(db_path: &Path, key: &str) -> Result<Option<String>, String> {
    init_cyq_chen_db(db_path)?;
    let conn = Connection::open(db_path).map_err(|e| format!("打开新筹码库失败:{e}"))?;
    conn.query_row(
        &format!("SELECT value FROM {CYQ_CHEN_META_TABLE} WHERE key = ?"),
        params![key],
        |row| row.get::<_, Option<String>>(0),
    )
    .or_else(|e| match e {
        duckdb::Error::QueryReturnedNoRows => Ok(None),
        other => Err(other),
    })
    .map_err(|e| format!("读取新筹码元信息失败:{e}"))
}

fn write_cyq_chen_meta(
    tx: &duckdb::Transaction<'_>,
    config: ChenChipConfig,
    strategy_hash: &str,
) -> Result<(), String> {
    tx.execute(&format!("DELETE FROM {CYQ_CHEN_META_TABLE}"), [])
        .map_err(|e| format!("清空cyq_chen_meta失败:{e}"))?;
    for (key, value) in [
        ("schema_version", CYQ_CHEN_SCHEMA_VERSION.to_string()),
        ("warmup_days", config.warmup_days.to_string()),
        (
            "bucket_pct",
            round_chen_chip_value(config.bucket_pct).to_string(),
        ),
        ("strategy_hash", strategy_hash.to_string()),
    ] {
        tx.execute(
            &format!("INSERT INTO {CYQ_CHEN_META_TABLE} (key, value) VALUES (?, ?)"),
            params![key, value],
        )
        .map_err(|e| format!("写入cyq_chen_meta失败:{e}"))?;
    }
    Ok(())
}

fn query_latest_cyq_chen_metadata(
    db_path: &Path,
) -> Result<Option<(String, ChenChipConfig)>, String> {
    init_cyq_chen_db(db_path)?;
    let conn = Connection::open(db_path).map_err(|e| format!("打开新筹码库失败:{e}"))?;
    let table_exists = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [CYQ_CHEN_SNAPSHOT_TABLE],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| format!("检查新筹码库表结构失败:{e}"))?;
    if table_exists <= 0 {
        return Ok(None);
    }

    let mut stmt = conn
        .prepare(
            r#"
            SELECT trade_date, warmup_days, bucket_pct
            FROM cyq_chen_snapshot
            WHERE trade_date = (SELECT MAX(trade_date) FROM cyq_chen_snapshot)
            LIMIT 1
            "#,
        )
        .map_err(|e| format!("预编译最新新筹码元数据查询失败:{e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询最新新筹码元数据失败:{e}"))?;
    let Some(row) = rows
        .next()
        .map_err(|e| format!("读取最新新筹码元数据失败:{e}"))?
    else {
        return Ok(None);
    };

    let trade_date: String = row.get(0).map_err(|e| format!("读取新筹码日期失败:{e}"))?;
    let warmup_days: Option<i64> = row
        .get(1)
        .map_err(|e| format!("读取新筹码 warmup_days 失败:{e}"))?;
    let bucket_pct: Option<f64> = row
        .get(2)
        .map_err(|e| format!("读取新筹码 bucket_pct 失败:{e}"))?;
    let default_config = ChenChipConfig::default();
    let config = ChenChipConfig {
        warmup_days: warmup_days
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or(default_config.warmup_days),
        bucket_pct: bucket_pct
            .filter(|value| value.is_finite() && *value > 0.0)
            .unwrap_or(default_config.bucket_pct),
    };

    Ok(Some((trade_date, config)))
}

fn query_existing_cyq_chen_trade_date_range(
    db_path: &Path,
) -> Result<Option<(String, String)>, String> {
    init_cyq_chen_db(db_path)?;
    let conn = Connection::open(db_path).map_err(|e| format!("打开新筹码库失败:{e}"))?;
    let table_exists = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [CYQ_CHEN_SNAPSHOT_TABLE],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| format!("检查新筹码库表结构失败:{e}"))?;
    if table_exists <= 0 {
        return Ok(None);
    }

    let mut stmt = conn
        .prepare(
            r#"
            SELECT MIN(trade_date), MAX(trade_date)
            FROM cyq_chen_snapshot
            WHERE adj_type = ?
            "#,
        )
        .map_err(|e| format!("预编译新筹码库日期范围查询失败:{e}"))?;
    let mut rows = stmt
        .query(params![DEFAULT_ADJ_TYPE])
        .map_err(|e| format!("查询新筹码库日期范围失败:{e}"))?;
    let Some(row) = rows
        .next()
        .map_err(|e| format!("读取新筹码库日期范围失败:{e}"))?
    else {
        return Ok(None);
    };

    let min_trade_date: Option<String> = row
        .get(0)
        .map_err(|e| format!("读取新筹码库最早日期失败:{e}"))?;
    let max_trade_date: Option<String> = row
        .get(1)
        .map_err(|e| format!("读取新筹码库最晚日期失败:{e}"))?;

    Ok(match (min_trade_date, max_trade_date) {
        (Some(min_trade_date), Some(max_trade_date)) => Some((min_trade_date, max_trade_date)),
        _ => None,
    })
}

pub fn query_cyq_chen_strategy_maintenance_status(
    source_dir: &str,
) -> Result<CyqChenStrategyMaintenanceStatus, String> {
    let cyq_chen_db = cyq_chen_db_path(source_dir);
    if !cyq_chen_db.exists() {
        return Ok(CyqChenStrategyMaintenanceStatus {
            db_exists: false,
            has_data: false,
            strategy_changed: false,
            detail: "未发现新筹码库 cyq_chen.db，下载后会跳过新筹码维护。".to_string(),
        });
    }

    let latest_metadata = query_latest_cyq_chen_metadata(&cyq_chen_db)?;
    if latest_metadata.is_none() {
        return Ok(CyqChenStrategyMaintenanceStatus {
            db_exists: true,
            has_data: false,
            strategy_changed: false,
            detail: "新筹码库已存在，但还没有可维护的筹码数据。".to_string(),
        });
    }

    let stored_hash = query_cyq_chen_meta_value(&cyq_chen_db, "strategy_hash")?;
    let current_hash = match current_chip_change_strategy_hash(source_dir) {
        Ok(value) => value,
        Err(error) => {
            return Ok(CyqChenStrategyMaintenanceStatus {
                db_exists: true,
                has_data: true,
                strategy_changed: false,
                detail: format!("无法检查筹码策略变化: {error}"),
            });
        }
    };
    let strategy_changed = stored_hash.as_deref() != Some(current_hash.as_str());

    Ok(CyqChenStrategyMaintenanceStatus {
        db_exists: true,
        has_data: true,
        strategy_changed,
        detail: if strategy_changed {
            "检测到 chip_change_rule.toml 与新筹码库记录的策略快照不一致，增量维护会触发全量重建。"
                .to_string()
        } else {
            "新筹码策略与当前库记录一致，下载后可按增量维护。".to_string()
        },
    })
}

fn bucket_history_key(price_low: f64, price_high: f64) -> (u64, u64) {
    (
        round_chen_chip_value(price_low).to_bits(),
        round_chen_chip_value(price_high).to_bits(),
    )
}

fn load_cyq_chen_initial_state(
    conn: &Connection,
    ts_code: &str,
    output_start_date: &str,
    row_trade_dates: &[String],
    output_start_index: usize,
) -> Result<Option<CyqChenInitialState>, String> {
    let latest_state_date = conn
        .query_row(
            r#"
            SELECT MAX(trade_date)
            FROM cyq_chen_snapshot
            WHERE ts_code = ? AND adj_type = ? AND trade_date < ?
            "#,
            params![ts_code, DEFAULT_ADJ_TYPE, output_start_date],
            |row| row.get::<_, Option<String>>(0),
        )
        .map_err(|e| format!("查询新筹码最新状态失败, ts_code={ts_code}: {e}"))?;
    let Some(state_trade_date) = latest_state_date else {
        return Ok(None);
    };

    let mut stmt = conn
        .prepare(
            r#"
            SELECT bin_index, price, price_low, price_high, main_chip, retail_chip, total_chip
            FROM cyq_chen_bin
            WHERE ts_code = ? AND adj_type = ? AND trade_date = ?
            ORDER BY bin_index ASC
            "#,
        )
        .map_err(|e| format!("预编译新筹码状态分桶查询失败, ts_code={ts_code}: {e}"))?;
    let mut rows = stmt
        .query(params![
            ts_code,
            DEFAULT_ADJ_TYPE,
            state_trade_date.as_str()
        ])
        .map_err(|e| format!("查询新筹码状态分桶失败, ts_code={ts_code}: {e}"))?;
    let mut bins = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取新筹码状态分桶失败, ts_code={ts_code}: {e}"))?
    {
        let index_i64: i64 = row
            .get(0)
            .map_err(|e| format!("读取新筹码分桶序号失败, ts_code={ts_code}: {e}"))?;
        bins.push(ChenChipBin {
            index: index_i64.max(0) as usize,
            price: row
                .get(1)
                .map_err(|e| format!("读取新筹码分桶价格失败, ts_code={ts_code}: {e}"))?,
            price_low: row
                .get(2)
                .map_err(|e| format!("读取新筹码分桶下沿失败, ts_code={ts_code}: {e}"))?,
            price_high: row
                .get(3)
                .map_err(|e| format!("读取新筹码分桶上沿失败, ts_code={ts_code}: {e}"))?,
            main_chip: row
                .get(4)
                .map_err(|e| format!("读取新筹码主力筹码失败, ts_code={ts_code}: {e}"))?,
            retail_chip: row
                .get(5)
                .map_err(|e| format!("读取新筹码散户筹码失败, ts_code={ts_code}: {e}"))?,
            total_chip: row
                .get(6)
                .map_err(|e| format!("读取新筹码总筹码失败, ts_code={ts_code}: {e}"))?,
        });
    }
    if bins.is_empty() {
        return Ok(None);
    }

    let mut bin_index_by_key = HashMap::new();
    for (index, bin) in bins.iter().enumerate() {
        bin_index_by_key.insert(bucket_history_key(bin.price_low, bin.price_high), index);
    }

    let mut main_ratio_history: Vec<Arc<Vec<Option<f64>>>> = (0..bins.len())
        .map(|_| Arc::new(vec![None; row_trade_dates.len()]))
        .collect();
    if output_start_index > 0 {
        let history_start_date = row_trade_dates
            .first()
            .map(String::as_str)
            .unwrap_or(state_trade_date.as_str());
        let mut history_stmt = conn
            .prepare(
                r#"
                SELECT trade_date, price_low, price_high, main_chip, retail_chip
                FROM cyq_chen_bin
                WHERE ts_code = ?
                  AND adj_type = ?
                  AND trade_date >= ?
                  AND trade_date < ?
                ORDER BY trade_date ASC, bin_index ASC
                "#,
            )
            .map_err(|e| format!("预编译新筹码历史比例查询失败, ts_code={ts_code}: {e}"))?;
        let mut history_rows = history_stmt
            .query(params![
                ts_code,
                DEFAULT_ADJ_TYPE,
                history_start_date,
                output_start_date
            ])
            .map_err(|e| format!("查询新筹码历史比例失败, ts_code={ts_code}: {e}"))?;
        let row_index_by_date = row_trade_dates
            .iter()
            .take(output_start_index)
            .enumerate()
            .map(|(index, trade_date)| (trade_date.as_str(), index))
            .collect::<HashMap<_, _>>();

        while let Some(row) = history_rows
            .next()
            .map_err(|e| format!("读取新筹码历史比例失败, ts_code={ts_code}: {e}"))?
        {
            let trade_date: String = row
                .get(0)
                .map_err(|e| format!("读取新筹码历史日期失败, ts_code={ts_code}: {e}"))?;
            let Some(row_index) = row_index_by_date.get(trade_date.as_str()).copied() else {
                continue;
            };
            let price_low: f64 = row
                .get(1)
                .map_err(|e| format!("读取新筹码历史分桶下沿失败, ts_code={ts_code}: {e}"))?;
            let price_high: f64 = row
                .get(2)
                .map_err(|e| format!("读取新筹码历史分桶上沿失败, ts_code={ts_code}: {e}"))?;
            let main_chip: f64 = row
                .get(3)
                .map_err(|e| format!("读取新筹码历史主力筹码失败, ts_code={ts_code}: {e}"))?;
            let retail_chip: f64 = row
                .get(4)
                .map_err(|e| format!("读取新筹码历史散户筹码失败, ts_code={ts_code}: {e}"))?;
            let Some(bucket_index) = bin_index_by_key
                .get(&bucket_history_key(price_low, price_high))
                .copied()
            else {
                continue;
            };
            let total = main_chip + retail_chip;
            Arc::make_mut(&mut main_ratio_history[bucket_index])[row_index] = if total > 1e-10 {
                Some(main_chip / total)
            } else {
                Some(0.0)
            };
        }
    }

    Ok(Some(CyqChenInitialState {
        state_trade_date,
        bins,
        main_ratio_history,
    }))
}

fn source_stock_data_exists(conn: &Connection) -> Result<bool, String> {
    let table_exists = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'stock_data'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| format!("检查stock_data表失败:{e}"))?;
    Ok(table_exists > 0)
}

fn query_source_trade_date_range(conn: &Connection) -> Result<Option<(String, String)>, String> {
    let mut stmt = conn
        .prepare("SELECT MIN(trade_date), MAX(trade_date) FROM stock_data WHERE adj_type = ?")
        .map_err(|e| format!("预编译筹码计算日期范围失败:{e}"))?;
    let mut rows = stmt
        .query(params![DEFAULT_ADJ_TYPE])
        .map_err(|e| format!("查询筹码计算日期范围失败:{e}"))?;
    let Some(row) = rows
        .next()
        .map_err(|e| format!("读取筹码计算日期范围失败:{e}"))?
    else {
        return Ok(None);
    };

    let min_trade_date: Option<String> = row.get(0).map_err(|e| format!("读取最早日期失败:{e}"))?;
    let max_trade_date: Option<String> = row.get(1).map_err(|e| format!("读取最晚日期失败:{e}"))?;

    Ok(match (min_trade_date, max_trade_date) {
        (Some(min_trade_date), Some(max_trade_date)) => Some((min_trade_date, max_trade_date)),
        _ => None,
    })
}

fn resolve_cyq_chen_rebuild_trade_date_range(
    conn: &Connection,
    requested_start_date: Option<&str>,
    requested_end_date: Option<&str>,
) -> Result<Option<(String, String)>, String> {
    let Some((source_min_trade_date, source_max_trade_date)) = query_source_trade_date_range(conn)?
    else {
        return Ok(None);
    };

    let requested_start_date = requested_start_date
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let requested_end_date = requested_end_date
        .map(str::trim)
        .filter(|value| !value.is_empty());

    let resolved_start_date = requested_start_date
        .map(|value| value.max(source_min_trade_date.as_str()).to_string())
        .unwrap_or_else(|| source_min_trade_date.clone());
    let resolved_end_date = requested_end_date
        .map(|value| value.min(source_max_trade_date.as_str()).to_string())
        .unwrap_or_else(|| source_max_trade_date.clone());

    if resolved_start_date > resolved_end_date {
        let requested_range = format!(
            "{} 至 {}",
            requested_start_date.unwrap_or(source_min_trade_date.as_str()),
            requested_end_date.unwrap_or(source_max_trade_date.as_str())
        );
        let source_range = format!("{source_min_trade_date} 至 {source_max_trade_date}");
        return Err(format!(
            "所选筹码计算区间 {requested_range} 与原始库可用区间 {source_range} 没有交集"
        ));
    }

    Ok(Some((resolved_start_date, resolved_end_date)))
}

fn resolve_cyq_chen_load_start_date(
    source_dir: &str,
    output_start_date: &str,
    output_end_date: &str,
    warmup_days: usize,
) -> Result<Option<String>, String> {
    let trade_dates = load_trade_date_list(source_dir)?;
    if trade_dates.is_empty() {
        return Ok(None);
    }

    let Some(first_output_trade_date_index) = trade_dates.iter().position(|trade_date| {
        let trade_date = trade_date.as_str();
        trade_date >= output_start_date && trade_date <= output_end_date
    }) else {
        return Ok(None);
    };

    if warmup_days == 0 {
        return Ok(Some(trade_dates[first_output_trade_date_index].clone()));
    }

    let load_start_index = first_output_trade_date_index.saturating_sub(warmup_days);
    Ok(Some(trade_dates[load_start_index].clone()))
}

fn count_output_rows(row_data: &RowData, start_date: &str, end_date: &str) -> usize {
    row_data
        .trade_dates
        .iter()
        .filter(|trade_date| {
            let trade_date = trade_date.as_str();
            trade_date >= start_date && trade_date <= end_date
        })
        .count()
}

fn resolve_first_computable_output_date(
    row_data: &RowData,
    start_date: &str,
    end_date: &str,
    warmup_days: usize,
) -> Option<String> {
    row_data
        .trade_dates
        .iter()
        .enumerate()
        .find_map(|(index, trade_date)| {
            let trade_date_str = trade_date.as_str();
            if trade_date_str < start_date || trade_date_str > end_date {
                return None;
            }
            if index < warmup_days {
                return None;
            }
            Some(trade_date.clone())
        })
}

fn compute_cyq_chen_stock(
    mut row_data: RowData,
    state_conn: Option<&Connection>,
    ts_code: &str,
    start_date: &str,
    end_date: &str,
    chip_config: &CompiledChipChangeConfig,
    config: ChenChipConfig,
    st_list: &HashSet<String>,
    total_share_map: &HashMap<String, f64>,
) -> Result<ComputedCyqChenStock, String> {
    if row_data.trade_dates.is_empty() {
        return Ok(ComputedCyqChenStock {
            ts_code: ts_code.to_string(),
            snapshots: Vec::new(),
        });
    }

    inject_stock_extra_fields(
        &mut row_data,
        ts_code,
        st_list.contains(ts_code),
        total_share_map.get(ts_code).copied(),
    )?;

    let Some(output_start_date) =
        resolve_first_computable_output_date(&row_data, start_date, end_date, 0)
    else {
        return Ok(ComputedCyqChenStock {
            ts_code: ts_code.to_string(),
            snapshots: Vec::new(),
        });
    };

    let output_start_index = row_data
        .trade_dates
        .iter()
        .position(|trade_date| trade_date == &output_start_date)
        .ok_or_else(|| format!("缺少新筹码输出起始日期: {output_start_date}"))?;
    let initial_state = match state_conn {
        Some(conn) => load_cyq_chen_initial_state(
            conn,
            ts_code,
            &output_start_date,
            &row_data.trade_dates,
            output_start_index,
        )?,
        None => None,
    };

    let snapshots = if let Some(initial_state) = initial_state {
        let Some(continuation_start_date) = row_data
            .trade_dates
            .iter()
            .find(|trade_date| trade_date.as_str() > initial_state.state_trade_date.as_str())
            .cloned()
        else {
            return Ok(ComputedCyqChenStock {
                ts_code: ts_code.to_string(),
                snapshots: Vec::new(),
            });
        };
        compute_chen_chip_snapshots_from_initial_bins_with_compiled_config(
            &row_data,
            &continuation_start_date,
            &initial_state.bins,
            &initial_state.main_ratio_history,
            chip_config,
            config,
        )?
    } else {
        let Some(output_start_date) = resolve_first_computable_output_date(
            &row_data,
            start_date,
            end_date,
            config.warmup_days,
        ) else {
            return Ok(ComputedCyqChenStock {
                ts_code: ts_code.to_string(),
                snapshots: Vec::new(),
            });
        };
        compute_chen_chip_snapshots_with_compiled_config(
            &row_data,
            &output_start_date,
            chip_config,
            config,
        )?
    };
    let snapshots = snapshots
        .into_iter()
        .filter(|snapshot| {
            snapshot
                .trade_date
                .as_deref()
                .is_some_and(|trade_date| trade_date >= start_date && trade_date <= end_date)
        })
        .collect();

    Ok(ComputedCyqChenStock {
        ts_code: ts_code.to_string(),
        snapshots,
    })
}

fn compute_cyq_chen_stock_group_batch(
    worker_reader: &DataReader,
    state_conn: Option<&Connection>,
    load_start_date: &str,
    start_date: &str,
    end_date: &str,
    chip_config: &CompiledChipChangeConfig,
    config: ChenChipConfig,
    st_list: &HashSet<String>,
    total_share_map: &HashMap<String, f64>,
    ts_group: &[String],
    on_stock_done: Option<&dyn Fn(&str)>,
) -> Result<CyqChenWriteBatch, String> {
    let mut rows_map =
        worker_reader.load_batch(ts_group, DEFAULT_ADJ_TYPE, load_start_date, end_date)?;
    let mut batch = CyqChenWriteBatch::default();
    for ts_code in ts_group {
        let mut row_data = match rows_map.remove(ts_code.as_str()) {
            Some(r) => r,
            None => {
                let tail = worker_reader.load_one_tail_rows(
                    ts_code,
                    DEFAULT_ADJ_TYPE,
                    end_date,
                    config.warmup_days.max(1) * 2,
                )?;
                if tail.trade_dates.is_empty() {
                    RowData {
                        trade_dates: Vec::new(),
                        cols: HashMap::new(),
                    }
                } else {
                    tail
                }
            }
        };

        if !row_data.trade_dates.is_empty()
            && resolve_first_computable_output_date(
                &row_data,
                start_date,
                end_date,
                config.warmup_days,
            )
            .is_none()
        {
            let output_rows = count_output_rows(&row_data, start_date, end_date);
            if output_rows > 0 {
                let need_rows = config.warmup_days.saturating_add(output_rows);
                if need_rows > 0 {
                    let tail = worker_reader.load_one_tail_rows(
                        ts_code,
                        DEFAULT_ADJ_TYPE,
                        end_date,
                        need_rows,
                    )?;
                    if !tail.trade_dates.is_empty() {
                        row_data = tail;
                    }
                }
            }
        }

        if row_data.trade_dates.is_empty() {
            let need_rows = config.warmup_days.max(60) * 2;
            if need_rows > 0 {
                let tail = worker_reader.load_one_tail_rows(
                    ts_code,
                    DEFAULT_ADJ_TYPE,
                    end_date,
                    need_rows,
                )?;
                if !tail.trade_dates.is_empty() {
                    row_data = tail;
                }
            }
        }

        let stock = compute_cyq_chen_stock(
            row_data,
            state_conn,
            ts_code,
            start_date,
            end_date,
            chip_config,
            config,
            st_list,
            total_share_map,
        )?;
        if !stock.snapshots.is_empty() {
            batch.stocks.push(stock);
        }
        if let Some(on_stock_done) = on_stock_done {
            on_stock_done(ts_code);
        }
    }
    Ok(batch)
}

fn append_cyq_chen_batch_rows(
    snapshot_app: &mut Appender<'_>,
    bin_app: &mut Appender<'_>,
    batch: CyqChenWriteBatch,
    config: ChenChipConfig,
) -> Result<(usize, usize), String> {
    let snapshot_rows = batch
        .stocks
        .iter()
        .map(|stock| stock.snapshots.len())
        .sum::<usize>();
    let bin_rows = batch
        .stocks
        .iter()
        .flat_map(|stock| &stock.snapshots)
        .map(|snapshot| snapshot.bins.len())
        .sum::<usize>();
    let rounded_bucket_pct = round_chen_chip_value(config.bucket_pct);

    let mut snapshot_ts_code =
        StringBuilder::with_capacity(snapshot_rows, snapshot_rows.saturating_mul(12));
    let mut snapshot_trade_date =
        StringBuilder::with_capacity(snapshot_rows, snapshot_rows.saturating_mul(8));
    let mut snapshot_adj_type = StringBuilder::with_capacity(
        snapshot_rows,
        snapshot_rows.saturating_mul(DEFAULT_ADJ_TYPE.len()),
    );
    let mut snapshot_warmup_days = Vec::with_capacity(snapshot_rows);
    let mut snapshot_bucket_pct = Vec::with_capacity(snapshot_rows);
    let mut snapshot_close = Vec::with_capacity(snapshot_rows);
    let mut snapshot_min_price = Vec::with_capacity(snapshot_rows);
    let mut snapshot_max_price = Vec::with_capacity(snapshot_rows);
    let mut snapshot_main_total = Vec::with_capacity(snapshot_rows);
    let mut snapshot_retail_total = Vec::with_capacity(snapshot_rows);
    let mut snapshot_total_chips = Vec::with_capacity(snapshot_rows);
    let mut snapshot_total_profit_ratio = Vec::with_capacity(snapshot_rows);
    let mut snapshot_total_trapped_ratio = Vec::with_capacity(snapshot_rows);
    let mut snapshot_main_avg_cost = Vec::with_capacity(snapshot_rows);
    let mut snapshot_chip_peak_price = Vec::with_capacity(snapshot_rows);
    let mut snapshot_percent_70_price_low = Vec::with_capacity(snapshot_rows);
    let mut snapshot_percent_70_price_high = Vec::with_capacity(snapshot_rows);
    let mut snapshot_percent_70_concentration = Vec::with_capacity(snapshot_rows);
    let mut snapshot_percent_90_price_low = Vec::with_capacity(snapshot_rows);
    let mut snapshot_percent_90_price_high = Vec::with_capacity(snapshot_rows);
    let mut snapshot_percent_90_concentration = Vec::with_capacity(snapshot_rows);
    let mut snapshot_main_profit_ratio = Vec::with_capacity(snapshot_rows);
    let mut snapshot_main_trapped_ratio = Vec::with_capacity(snapshot_rows);

    let mut bin_ts_code = StringBuilder::with_capacity(bin_rows, bin_rows.saturating_mul(12));
    let mut bin_trade_date = StringBuilder::with_capacity(bin_rows, bin_rows.saturating_mul(8));
    let mut bin_adj_type =
        StringBuilder::with_capacity(bin_rows, bin_rows.saturating_mul(DEFAULT_ADJ_TYPE.len()));
    let mut bin_index = Vec::with_capacity(bin_rows);
    let mut bin_price = Vec::with_capacity(bin_rows);
    let mut bin_price_low = Vec::with_capacity(bin_rows);
    let mut bin_price_high = Vec::with_capacity(bin_rows);
    let mut bin_main_chip = Vec::with_capacity(bin_rows);
    let mut bin_retail_chip = Vec::with_capacity(bin_rows);
    let mut bin_total_chip = Vec::with_capacity(bin_rows);

    for stock in batch.stocks {
        let ts_code = stock.ts_code;
        for mut snapshot in stock.snapshots {
            let trade_date = snapshot
                .trade_date
                .take()
                .ok_or_else(|| format!("{ts_code} 的新筹码快照缺少交易日期"))?;
            round_chen_chip_snapshot(&mut snapshot);
            snapshot_ts_code.append_value(&ts_code);
            snapshot_trade_date.append_value(&trade_date);
            snapshot_adj_type.append_value(DEFAULT_ADJ_TYPE);
            snapshot_warmup_days.push(config.warmup_days as i32);
            snapshot_bucket_pct.push(rounded_bucket_pct);
            snapshot_close.push(snapshot.close);
            snapshot_min_price.push(snapshot.min_price);
            snapshot_max_price.push(snapshot.max_price);
            snapshot_main_total.push(snapshot.main_total);
            snapshot_retail_total.push(snapshot.retail_total);
            snapshot_total_chips.push(snapshot.total_chips);
            snapshot_total_profit_ratio.push(snapshot.total_profit_ratio);
            snapshot_total_trapped_ratio.push(snapshot.total_trapped_ratio);
            snapshot_main_avg_cost.push(snapshot.main_avg_cost);
            snapshot_chip_peak_price.push(snapshot.chip_peak_price);
            snapshot_percent_70_price_low.push(snapshot.percent_70.price_low);
            snapshot_percent_70_price_high.push(snapshot.percent_70.price_high);
            snapshot_percent_70_concentration.push(snapshot.percent_70.concentration);
            snapshot_percent_90_price_low.push(snapshot.percent_90.price_low);
            snapshot_percent_90_price_high.push(snapshot.percent_90.price_high);
            snapshot_percent_90_concentration.push(snapshot.percent_90.concentration);
            snapshot_main_profit_ratio.push(snapshot.main_profit_ratio);
            snapshot_main_trapped_ratio.push(snapshot.main_trapped_ratio);

            for bin in snapshot.bins {
                bin_ts_code.append_value(&ts_code);
                bin_trade_date.append_value(&trade_date);
                bin_adj_type.append_value(DEFAULT_ADJ_TYPE);
                bin_index.push(bin.index as i32);
                bin_price.push(bin.price);
                bin_price_low.push(bin.price_low);
                bin_price_high.push(bin.price_high);
                bin_main_chip.push(bin.main_chip);
                bin_retail_chip.push(bin.retail_chip);
                bin_total_chip.push(bin.total_chip);
            }
        }
    }

    if snapshot_rows > 0 {
        snapshot_app
            .append_record_batch(build_cyq_chen_snapshot_record_batch(
                snapshot_ts_code.finish(),
                snapshot_trade_date.finish(),
                snapshot_adj_type.finish(),
                snapshot_warmup_days,
                snapshot_bucket_pct,
                snapshot_close,
                snapshot_min_price,
                snapshot_max_price,
                snapshot_main_total,
                snapshot_retail_total,
                snapshot_total_chips,
                snapshot_total_profit_ratio,
                snapshot_total_trapped_ratio,
                snapshot_main_avg_cost,
                snapshot_chip_peak_price,
                snapshot_percent_70_price_low,
                snapshot_percent_70_price_high,
                snapshot_percent_70_concentration,
                snapshot_percent_90_price_low,
                snapshot_percent_90_price_high,
                snapshot_percent_90_concentration,
                snapshot_main_profit_ratio,
                snapshot_main_trapped_ratio,
            )?)
            .map_err(|e| format!("批量写入cyq_chen_snapshot失败:{e}"))?;
    }

    if bin_rows > 0 {
        bin_app
            .append_record_batch(build_cyq_chen_bin_record_batch(
                bin_ts_code.finish(),
                bin_trade_date.finish(),
                bin_adj_type.finish(),
                bin_index,
                bin_price,
                bin_price_low,
                bin_price_high,
                bin_main_chip,
                bin_retail_chip,
                bin_total_chip,
            )?)
            .map_err(|e| format!("批量写入cyq_chen_bin失败:{e}"))?;
    }

    Ok((snapshot_rows, bin_rows))
}

fn string_array(values: StringArray) -> ArrayRef {
    Arc::new(values)
}

fn int32_array(values: Vec<i32>) -> ArrayRef {
    Arc::new(Int32Array::from(values))
}

fn float64_array(values: Vec<f64>) -> ArrayRef {
    Arc::new(Float64Array::from(values))
}

#[allow(clippy::too_many_arguments)]
fn build_cyq_chen_snapshot_record_batch(
    ts_code: StringArray,
    trade_date: StringArray,
    adj_type: StringArray,
    warmup_days: Vec<i32>,
    bucket_pct: Vec<f64>,
    close: Vec<f64>,
    min_price: Vec<f64>,
    max_price: Vec<f64>,
    main_total: Vec<f64>,
    retail_total: Vec<f64>,
    total_chips: Vec<f64>,
    total_profit_ratio: Vec<f64>,
    total_trapped_ratio: Vec<f64>,
    main_avg_cost: Vec<f64>,
    chip_peak_price: Vec<f64>,
    percent_70_price_low: Vec<f64>,
    percent_70_price_high: Vec<f64>,
    percent_70_concentration: Vec<f64>,
    percent_90_price_low: Vec<f64>,
    percent_90_price_high: Vec<f64>,
    percent_90_concentration: Vec<f64>,
    main_profit_ratio: Vec<f64>,
    main_trapped_ratio: Vec<f64>,
) -> Result<RecordBatch, String> {
    let schema = Schema::new(vec![
        Field::new("ts_code", DataType::Utf8, false),
        Field::new("trade_date", DataType::Utf8, false),
        Field::new("adj_type", DataType::Utf8, false),
        Field::new("warmup_days", DataType::Int32, false),
        Field::new("bucket_pct", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("min_price", DataType::Float64, false),
        Field::new("max_price", DataType::Float64, false),
        Field::new("main_total", DataType::Float64, false),
        Field::new("retail_total", DataType::Float64, false),
        Field::new("total_chips", DataType::Float64, false),
        Field::new("total_profit_ratio", DataType::Float64, false),
        Field::new("total_trapped_ratio", DataType::Float64, false),
        Field::new("main_avg_cost", DataType::Float64, false),
        Field::new("chip_peak_price", DataType::Float64, false),
        Field::new("percent_70_price_low", DataType::Float64, false),
        Field::new("percent_70_price_high", DataType::Float64, false),
        Field::new("percent_70_concentration", DataType::Float64, false),
        Field::new("percent_90_price_low", DataType::Float64, false),
        Field::new("percent_90_price_high", DataType::Float64, false),
        Field::new("percent_90_concentration", DataType::Float64, false),
        Field::new("main_profit_ratio", DataType::Float64, false),
        Field::new("main_trapped_ratio", DataType::Float64, false),
    ]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            string_array(ts_code),
            string_array(trade_date),
            string_array(adj_type),
            int32_array(warmup_days),
            float64_array(bucket_pct),
            float64_array(close),
            float64_array(min_price),
            float64_array(max_price),
            float64_array(main_total),
            float64_array(retail_total),
            float64_array(total_chips),
            float64_array(total_profit_ratio),
            float64_array(total_trapped_ratio),
            float64_array(main_avg_cost),
            float64_array(chip_peak_price),
            float64_array(percent_70_price_low),
            float64_array(percent_70_price_high),
            float64_array(percent_70_concentration),
            float64_array(percent_90_price_low),
            float64_array(percent_90_price_high),
            float64_array(percent_90_concentration),
            float64_array(main_profit_ratio),
            float64_array(main_trapped_ratio),
        ],
    )
    .map_err(|e| format!("创建cyq_chen_snapshot批次失败:{e}"))
}

#[allow(clippy::too_many_arguments)]
fn build_cyq_chen_bin_record_batch(
    ts_code: StringArray,
    trade_date: StringArray,
    adj_type: StringArray,
    bin_index: Vec<i32>,
    price: Vec<f64>,
    price_low: Vec<f64>,
    price_high: Vec<f64>,
    main_chip: Vec<f64>,
    retail_chip: Vec<f64>,
    total_chip: Vec<f64>,
) -> Result<RecordBatch, String> {
    let schema = Schema::new(vec![
        Field::new("ts_code", DataType::Utf8, false),
        Field::new("trade_date", DataType::Utf8, false),
        Field::new("adj_type", DataType::Utf8, false),
        Field::new("bin_index", DataType::Int32, false),
        Field::new("price", DataType::Float64, false),
        Field::new("price_low", DataType::Float64, false),
        Field::new("price_high", DataType::Float64, false),
        Field::new("main_chip", DataType::Float64, false),
        Field::new("retail_chip", DataType::Float64, false),
        Field::new("total_chip", DataType::Float64, false),
    ]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            string_array(ts_code),
            string_array(trade_date),
            string_array(adj_type),
            int32_array(bin_index),
            float64_array(price),
            float64_array(price_low),
            float64_array(price_high),
            float64_array(main_chip),
            float64_array(retail_chip),
            float64_array(total_chip),
        ],
    )
    .map_err(|e| format!("创建cyq_chen_bin批次失败:{e}"))
}

fn write_cyq_chen_batches_from_channel(
    db_path: &str,
    rx: Receiver<CyqChenWriteMessage>,
    config: ChenChipConfig,
    strategy_hash: String,
) -> Result<(usize, usize), String> {
    let mut conn = Connection::open(db_path).map_err(|e| format!("打开筹码库失败:{e}"))?;
    let tx = conn
        .transaction()
        .map_err(|e| format!("创建筹码库事务失败:{e}"))?;
    drop_cyq_chen_db_indexes(&tx)?;
    tx.execute("DELETE FROM cyq_chen_bin", [])
        .map_err(|e| format!("清空cyq_chen_bin失败:{e}"))?;
    tx.execute("DELETE FROM cyq_chen_snapshot", [])
        .map_err(|e| format!("清空cyq_chen_snapshot失败:{e}"))?;

    let mut snapshot_rows = 0usize;
    let mut bin_rows = 0usize;
    let mut batch_count = 0usize;
    let mut abort_reason = None;
    {
        let mut snapshot_app = tx
            .appender(CYQ_CHEN_SNAPSHOT_TABLE)
            .map_err(|e| format!("创建cyq_chen_snapshot写入器失败:{e}"))?;
        let mut bin_app = tx
            .appender(CYQ_CHEN_BIN_TABLE)
            .map_err(|e| format!("创建cyq_chen_bin写入器失败:{e}"))?;

        for message in rx {
            let batch = match message {
                CyqChenWriteMessage::Batch(batch) => batch,
                CyqChenWriteMessage::Abort(reason) => {
                    abort_reason = Some(reason);
                    break;
                }
            };

            let (added_snapshot_rows, added_bin_rows) =
                append_cyq_chen_batch_rows(&mut snapshot_app, &mut bin_app, batch, config)?;
            snapshot_rows += added_snapshot_rows;
            bin_rows += added_bin_rows;
            batch_count += 1;

            if batch_count % CYQ_CHEN_FLUSH_BATCH_SIZE == 0 {
                snapshot_app
                    .flush()
                    .map_err(|e| format!("刷新cyq_chen_snapshot写入器失败:{e}"))?;
                bin_app
                    .flush()
                    .map_err(|e| format!("刷新cyq_chen_bin写入器失败:{e}"))?;
            }
        }

        if abort_reason.is_none() {
            snapshot_app
                .flush()
                .map_err(|e| format!("刷新cyq_chen_snapshot写入器失败:{e}"))?;
            bin_app
                .flush()
                .map_err(|e| format!("刷新cyq_chen_bin写入器失败:{e}"))?;
        }
    }

    if let Some(reason) = abort_reason {
        tx.rollback()
            .map_err(|e| format!("筹码计算中断且结果库回滚失败:{reason}; {e}"))?;
        return Err(format!("筹码计算中断，结果库已回滚:{reason}"));
    }

    write_cyq_chen_meta(&tx, config, &strategy_hash)?;
    tx.commit().map_err(|e| format!("提交筹码库事务失败:{e}"))?;
    ensure_cyq_chen_db_indexes(&conn)?;
    conn.execute_batch("CHECKPOINT")
        .map_err(|e| format!("检查点新筹码库失败:{e}"))?;
    Ok((snapshot_rows, bin_rows))
}

fn write_cyq_chen_incremental_batches_from_channel(
    db_path: &str,
    rx: Receiver<CyqChenWriteMessage>,
    config: ChenChipConfig,
    start_date: &str,
    end_date: &str,
    strategy_hash: String,
) -> Result<(usize, usize), String> {
    let mut conn = Connection::open(db_path).map_err(|e| format!("打开筹码库失败:{e}"))?;
    ensure_cyq_chen_db_indexes(&conn)?;

    let write_result = (|| -> Result<(usize, usize), String> {
        let tx = conn
            .transaction()
            .map_err(|e| format!("创建新筹码增量事务失败:{e}"))?;
        tx.execute(
            "DELETE FROM cyq_chen_bin WHERE adj_type = ? AND trade_date >= ? AND trade_date <= ?",
            params![DEFAULT_ADJ_TYPE, start_date, end_date],
        )
        .map_err(|e| format!("清理增量区间cyq_chen_bin失败:{e}"))?;
        tx.execute(
            "DELETE FROM cyq_chen_snapshot WHERE adj_type = ? AND trade_date >= ? AND trade_date <= ?",
            params![DEFAULT_ADJ_TYPE, start_date, end_date],
        )
        .map_err(|e| format!("清理增量区间cyq_chen_snapshot失败:{e}"))?;

        let mut snapshot_rows = 0usize;
        let mut bin_rows = 0usize;
        let mut batch_count = 0usize;
        let mut abort_reason = None;
        {
            let mut snapshot_app = tx
                .appender(CYQ_CHEN_SNAPSHOT_TABLE)
                .map_err(|e| format!("创建cyq_chen_snapshot写入器失败:{e}"))?;
            let mut bin_app = tx
                .appender(CYQ_CHEN_BIN_TABLE)
                .map_err(|e| format!("创建cyq_chen_bin写入器失败:{e}"))?;

            for message in rx {
                let batch = match message {
                    CyqChenWriteMessage::Batch(batch) => batch,
                    CyqChenWriteMessage::Abort(reason) => {
                        abort_reason = Some(reason);
                        break;
                    }
                };

                let (added_snapshot_rows, added_bin_rows) =
                    append_cyq_chen_batch_rows(&mut snapshot_app, &mut bin_app, batch, config)?;
                snapshot_rows += added_snapshot_rows;
                bin_rows += added_bin_rows;
                batch_count += 1;

                if batch_count % CYQ_CHEN_FLUSH_BATCH_SIZE == 0 {
                    snapshot_app
                        .flush()
                        .map_err(|e| format!("刷新cyq_chen_snapshot写入器失败:{e}"))?;
                    bin_app
                        .flush()
                        .map_err(|e| format!("刷新cyq_chen_bin写入器失败:{e}"))?;
                }
            }

            if abort_reason.is_none() {
                snapshot_app
                    .flush()
                    .map_err(|e| format!("刷新cyq_chen_snapshot写入器失败:{e}"))?;
                bin_app
                    .flush()
                    .map_err(|e| format!("刷新cyq_chen_bin写入器失败:{e}"))?;
            }
        }

        if let Some(reason) = abort_reason {
            tx.rollback()
                .map_err(|e| format!("新筹码增量计算中断且结果库回滚失败:{reason}; {e}"))?;
            return Err(format!("新筹码增量计算中断，结果库已回滚:{reason}"));
        }

        write_cyq_chen_meta(&tx, config, &strategy_hash)?;
        tx.commit()
            .map_err(|e| format!("提交新筹码增量事务失败:{e}"))?;
        Ok((snapshot_rows, bin_rows))
    })();

    let index_result = ensure_cyq_chen_db_indexes(&conn);
    match (write_result, index_result) {
        (Ok(rows), Ok(())) => Ok(rows),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

fn write_cyq_chen_stock_repair_batches_from_channel(
    db_path: &str,
    rx: Receiver<CyqChenWriteMessage>,
    config: ChenChipConfig,
    ts_codes: &[String],
    start_date: &str,
    end_date: &str,
    strategy_hash: String,
) -> Result<(usize, usize), String> {
    let mut conn = Connection::open(db_path).map_err(|e| format!("打开筹码库失败:{e}"))?;
    ensure_cyq_chen_db_indexes(&conn)?;

    let write_result = (|| -> Result<(usize, usize), String> {
        let tx = conn
            .transaction()
            .map_err(|e| format!("创建筹码库事务失败:{e}"))?;

        for ts_code in ts_codes {
            tx.execute(
                "DELETE FROM cyq_chen_bin WHERE ts_code = ? AND adj_type = ? AND trade_date >= ? AND trade_date <= ?",
                params![ts_code, DEFAULT_ADJ_TYPE, start_date, end_date],
            )
            .map_err(|e| format!("清理股票新筹码分桶失败, ts_code={ts_code}: {e}"))?;
            tx.execute(
                "DELETE FROM cyq_chen_snapshot WHERE ts_code = ? AND adj_type = ? AND trade_date >= ? AND trade_date <= ?",
                params![ts_code, DEFAULT_ADJ_TYPE, start_date, end_date],
            )
            .map_err(|e| format!("清理股票新筹码摘要失败, ts_code={ts_code}: {e}"))?;
        }

        let mut snapshot_rows = 0usize;
        let mut bin_rows = 0usize;
        let mut batch_count = 0usize;
        let mut abort_reason = None;
        {
            let mut snapshot_app = tx
                .appender(CYQ_CHEN_SNAPSHOT_TABLE)
                .map_err(|e| format!("创建cyq_chen_snapshot写入器失败:{e}"))?;
            let mut bin_app = tx
                .appender(CYQ_CHEN_BIN_TABLE)
                .map_err(|e| format!("创建cyq_chen_bin写入器失败:{e}"))?;

            for message in rx {
                let batch = match message {
                    CyqChenWriteMessage::Batch(batch) => batch,
                    CyqChenWriteMessage::Abort(reason) => {
                        abort_reason = Some(reason);
                        break;
                    }
                };

                let (added_snapshot_rows, added_bin_rows) =
                    append_cyq_chen_batch_rows(&mut snapshot_app, &mut bin_app, batch, config)?;
                snapshot_rows += added_snapshot_rows;
                bin_rows += added_bin_rows;
                batch_count += 1;

                if batch_count % CYQ_CHEN_FLUSH_BATCH_SIZE == 0 {
                    snapshot_app
                        .flush()
                        .map_err(|e| format!("刷新cyq_chen_snapshot写入器失败:{e}"))?;
                    bin_app
                        .flush()
                        .map_err(|e| format!("刷新cyq_chen_bin写入器失败:{e}"))?;
                }
            }

            if abort_reason.is_none() {
                snapshot_app
                    .flush()
                    .map_err(|e| format!("刷新cyq_chen_snapshot写入器失败:{e}"))?;
                bin_app
                    .flush()
                    .map_err(|e| format!("刷新cyq_chen_bin写入器失败:{e}"))?;
            }
        }

        if let Some(reason) = abort_reason {
            tx.rollback()
                .map_err(|e| format!("新筹码局部修复中断且结果库回滚失败:{reason}; {e}"))?;
            return Err(format!("新筹码局部修复中断，结果库已回滚:{reason}"));
        }

        write_cyq_chen_meta(&tx, config, &strategy_hash)?;
        tx.commit().map_err(|e| format!("提交筹码库事务失败:{e}"))?;
        Ok((snapshot_rows, bin_rows))
    })();

    let recreate_result = ensure_cyq_chen_db_indexes(&conn);
    match (write_result, recreate_result) {
        (Ok(rows), Ok(())) => Ok(rows),
        (Err(write_error), _) => Err(write_error),
        (Ok(_), Err(index_error)) => Err(index_error),
    }
}

pub fn maintain_cyq_chen_incremental_if_db_exists(
    source_dir: &str,
    allow_strategy_rebuild: bool,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<Option<CyqChenRebuildSummary>, String> {
    let cyq_chen_db = cyq_chen_db_path(source_dir);
    if !cyq_chen_db.exists() {
        return Ok(None);
    }

    init_cyq_chen_db(&cyq_chen_db)?;

    let source_db = source_db_path(source_dir);
    if !source_db.exists() {
        return Ok(Some(CyqChenRebuildSummary {
            snapshot_rows: 0,
            bin_rows: 0,
            warmup_days: ChenChipConfig::default().warmup_days,
            bucket_pct: ChenChipConfig::default().bucket_pct,
            start_date: None,
            end_date: None,
        }));
    }

    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?;
    let source_conn = Connection::open(source_db_str).map_err(|e| format!("打开原始库失败:{e}"))?;
    if !source_stock_data_exists(&source_conn)? {
        return Ok(Some(CyqChenRebuildSummary {
            snapshot_rows: 0,
            bin_rows: 0,
            warmup_days: ChenChipConfig::default().warmup_days,
            bucket_pct: ChenChipConfig::default().bucket_pct,
            start_date: None,
            end_date: None,
        }));
    }

    let Some((source_min_trade_date, source_max_trade_date)) =
        query_source_trade_date_range(&source_conn)?
    else {
        return Ok(Some(CyqChenRebuildSummary {
            snapshot_rows: 0,
            bin_rows: 0,
            warmup_days: ChenChipConfig::default().warmup_days,
            bucket_pct: ChenChipConfig::default().bucket_pct,
            start_date: None,
            end_date: None,
        }));
    };

    let latest_metadata = query_latest_cyq_chen_metadata(&cyq_chen_db)?;
    let config = latest_metadata
        .as_ref()
        .map(|(_, config)| *config)
        .unwrap_or_default();
    let chip_config = load_compiled_chip_change_config(source_dir)?;
    let strategy_hash = current_chip_change_strategy_hash(source_dir)?;
    let strategy_changed = query_cyq_chen_meta_value(&cyq_chen_db, "strategy_hash")?.as_deref()
        != Some(strategy_hash.as_str());
    if latest_metadata.is_some() && strategy_changed {
        if !allow_strategy_rebuild {
            return Ok(Some(CyqChenRebuildSummary {
                snapshot_rows: 0,
                bin_rows: 0,
                warmup_days: config.warmup_days,
                bucket_pct: config.bucket_pct,
                start_date: None,
                end_date: None,
            }));
        }
        return rebuild_cyq_chen_all_with_progress(source_dir, config, None, None, progress_cb)
            .map(Some);
    }
    let start_date = match latest_metadata.as_ref() {
        Some((latest_trade_date, _)) if latest_trade_date >= &source_max_trade_date => {
            return Ok(Some(CyqChenRebuildSummary {
                snapshot_rows: 0,
                bin_rows: 0,
                warmup_days: config.warmup_days,
                bucket_pct: config.bucket_pct,
                start_date: None,
                end_date: None,
            }));
        }
        Some((latest_trade_date, _)) => {
            let trade_dates = load_trade_date_list(source_dir)?;
            let Some(next_trade_date) = trade_dates.into_iter().find(|trade_date| {
                trade_date > latest_trade_date && trade_date <= &source_max_trade_date
            }) else {
                return Ok(Some(CyqChenRebuildSummary {
                    snapshot_rows: 0,
                    bin_rows: 0,
                    warmup_days: config.warmup_days,
                    bucket_pct: config.bucket_pct,
                    start_date: None,
                    end_date: None,
                }));
            };
            next_trade_date
        }
        None => source_min_trade_date,
    };
    let end_date = source_max_trade_date;

    let expression_warmup_need = estimate_chen_chip_expression_warmup(&chip_config)?;
    let load_warmup_need = config.warmup_days.max(expression_warmup_need);
    let Some(load_start_date) =
        resolve_cyq_chen_load_start_date(source_dir, &start_date, &end_date, load_warmup_need)?
    else {
        return Ok(Some(CyqChenRebuildSummary {
            snapshot_rows: 0,
            bin_rows: 0,
            warmup_days: config.warmup_days,
            bucket_pct: config.bucket_pct,
            start_date: Some(start_date),
            end_date: Some(end_date),
        }));
    };
    let required_runtime_keys = collect_chen_chip_runtime_keys(&chip_config);
    let st_list = load_st_list(source_dir).unwrap_or_default();
    let total_share_map = load_total_share_map(source_dir).unwrap_or_default();
    let reader = DataReader::new_with_runtime_keys(source_dir, &required_runtime_keys)?;
    let ts_codes = reader.list_ts_code(DEFAULT_ADJ_TYPE, &load_start_date, &end_date)?;
    let cyq_chen_db_str = cyq_chen_db
        .to_str()
        .ok_or_else(|| "筹码库路径不是有效UTF-8".to_string())?
        .to_string();
    if let Some(progress_cb) = progress_cb {
        progress_cb(DownloadProgress {
            phase: "compute_cyq_chen".to_string(),
            finished: 0,
            total: ts_codes.len(),
            current_label: None,
            message: format!(
                "新筹码增量维护已开始，区间 {} 至 {}，共 {} 只股票。",
                start_date,
                end_date,
                ts_codes.len()
            ),
        });
    }

    let (tx, rx) = sync_channel(CYQ_CHEN_QUEUE_BOUND);
    let abort_tx = tx.clone();
    let write_start_date = start_date.clone();
    let write_end_date = end_date.clone();
    let writer_strategy_hash = strategy_hash.clone();
    let writer_handle = thread::spawn(move || {
        write_cyq_chen_incremental_batches_from_channel(
            &cyq_chen_db_str,
            rx,
            config,
            &write_start_date,
            &write_end_date,
            writer_strategy_hash,
        )
    });

    let finished_stock_count = std::sync::atomic::AtomicUsize::new(0);
    let compute_result = ts_codes
        .par_chunks(CYQ_CHEN_GROUP_SIZE_INCREMENTAL)
        .try_for_each_with(tx, |sender, ts_group| -> Result<(), String> {
            let worker_reader =
                DataReader::new_with_runtime_keys(source_dir, &required_runtime_keys)?;
            let state_conn =
                Connection::open(&cyq_chen_db).map_err(|e| format!("打开新筹码库状态失败:{e}"))?;
            let progress_stock_done = |ts_code: &str| {
                if let Some(progress_cb) = progress_cb {
                    let finished =
                        finished_stock_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                    progress_cb(DownloadProgress {
                        phase: "compute_cyq_chen".to_string(),
                        finished,
                        total: ts_codes.len(),
                        current_label: Some(ts_code.to_string()),
                        message: format!(
                            "新筹码增量维护中，已完成 {finished} / {} 只股票。",
                            ts_codes.len()
                        ),
                    });
                }
            };
            let batch = compute_cyq_chen_stock_group_batch(
                &worker_reader,
                Some(&state_conn),
                &load_start_date,
                &start_date,
                &end_date,
                &chip_config,
                config,
                &st_list,
                &total_share_map,
                ts_group,
                Some(&progress_stock_done),
            )?;
            sender
                .send(CyqChenWriteMessage::Batch(batch))
                .map_err(|e| format!("发送新筹码增量批次失败:{e}"))?;
            Ok(())
        });

    if let Err(err) = &compute_result {
        let _ = abort_tx.send(CyqChenWriteMessage::Abort(err.clone()));
    }
    drop(abort_tx);

    let writer_result = match writer_handle.join() {
        Ok(result) => result,
        Err(_) => Err("筹码库写线程异常退出".to_string()),
    };

    compute_result?;
    let (snapshot_rows, bin_rows) = writer_result?;
    Ok(Some(CyqChenRebuildSummary {
        snapshot_rows,
        bin_rows,
        warmup_days: config.warmup_days,
        bucket_pct: config.bucket_pct,
        start_date: Some(start_date),
        end_date: Some(end_date),
    }))
}

pub fn rebuild_cyq_chen_all_if_db_exists(
    source_dir: &str,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<Option<CyqChenRebuildSummary>, String> {
    let cyq_chen_db = cyq_chen_db_path(source_dir);
    if !cyq_chen_db.exists() {
        return Ok(None);
    }

    init_cyq_chen_db(&cyq_chen_db)?;
    let config = query_latest_cyq_chen_metadata(&cyq_chen_db)?
        .map(|(_, config)| config)
        .unwrap_or_default();
    rebuild_cyq_chen_all_with_progress(source_dir, config, None, None, progress_cb).map(Some)
}

pub fn repair_cyq_chen_stocks_if_db_exists(
    source_dir: &str,
    ts_codes: &[String],
    allow_strategy_rebuild: bool,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<Option<CyqChenRebuildSummary>, String> {
    let cyq_chen_db = cyq_chen_db_path(source_dir);
    if !cyq_chen_db.exists() {
        return Ok(None);
    }

    let mut ts_codes = ts_codes
        .iter()
        .map(|ts_code| ts_code.trim())
        .filter(|ts_code| !ts_code.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    let mut seen = HashSet::new();
    ts_codes.retain(|ts_code| seen.insert(ts_code.clone()));
    ts_codes.sort();
    if ts_codes.is_empty() {
        return Ok(Some(CyqChenRebuildSummary {
            snapshot_rows: 0,
            bin_rows: 0,
            warmup_days: ChenChipConfig::default().warmup_days,
            bucket_pct: ChenChipConfig::default().bucket_pct,
            start_date: None,
            end_date: None,
        }));
    }

    init_cyq_chen_db(&cyq_chen_db)?;
    let Some((_, config)) = query_latest_cyq_chen_metadata(&cyq_chen_db)? else {
        return Ok(Some(CyqChenRebuildSummary {
            snapshot_rows: 0,
            bin_rows: 0,
            warmup_days: ChenChipConfig::default().warmup_days,
            bucket_pct: ChenChipConfig::default().bucket_pct,
            start_date: None,
            end_date: None,
        }));
    };

    let chip_config = load_compiled_chip_change_config(source_dir)?;
    let strategy_hash = current_chip_change_strategy_hash(source_dir)?;
    if query_cyq_chen_meta_value(&cyq_chen_db, "strategy_hash")?.as_deref()
        != Some(strategy_hash.as_str())
    {
        if !allow_strategy_rebuild {
            return Ok(Some(CyqChenRebuildSummary {
                snapshot_rows: 0,
                bin_rows: 0,
                warmup_days: config.warmup_days,
                bucket_pct: config.bucket_pct,
                start_date: None,
                end_date: None,
            }));
        }
        return rebuild_cyq_chen_all_with_progress(source_dir, config, None, None, progress_cb)
            .map(Some);
    }

    let Some((existing_start_date, existing_end_date)) =
        query_existing_cyq_chen_trade_date_range(&cyq_chen_db)?
    else {
        return Ok(Some(CyqChenRebuildSummary {
            snapshot_rows: 0,
            bin_rows: 0,
            warmup_days: config.warmup_days,
            bucket_pct: config.bucket_pct,
            start_date: None,
            end_date: None,
        }));
    };

    let source_db = source_db_path(source_dir);
    if !source_db.exists() {
        return Ok(Some(CyqChenRebuildSummary {
            snapshot_rows: 0,
            bin_rows: 0,
            warmup_days: config.warmup_days,
            bucket_pct: config.bucket_pct,
            start_date: None,
            end_date: None,
        }));
    }
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?;
    let source_conn = Connection::open(source_db_str).map_err(|e| format!("打开原始库失败:{e}"))?;
    let Some((start_date, end_date)) = resolve_cyq_chen_rebuild_trade_date_range(
        &source_conn,
        Some(&existing_start_date),
        Some(&existing_end_date),
    )?
    else {
        return Ok(Some(CyqChenRebuildSummary {
            snapshot_rows: 0,
            bin_rows: 0,
            warmup_days: config.warmup_days,
            bucket_pct: config.bucket_pct,
            start_date: None,
            end_date: None,
        }));
    };

    let expression_warmup_need = estimate_chen_chip_expression_warmup(&chip_config)?;
    let load_warmup_need = config.warmup_days.max(expression_warmup_need);
    let Some(load_start_date) =
        resolve_cyq_chen_load_start_date(source_dir, &start_date, &end_date, load_warmup_need)?
    else {
        return Ok(Some(CyqChenRebuildSummary {
            snapshot_rows: 0,
            bin_rows: 0,
            warmup_days: config.warmup_days,
            bucket_pct: config.bucket_pct,
            start_date: Some(start_date),
            end_date: Some(end_date),
        }));
    };

    let required_runtime_keys = collect_chen_chip_runtime_keys(&chip_config);
    let st_list = load_st_list(source_dir).unwrap_or_default();
    let total_share_map = load_total_share_map(source_dir).unwrap_or_default();
    let cyq_chen_db_str = cyq_chen_db
        .to_str()
        .ok_or_else(|| "筹码库路径不是有效UTF-8".to_string())?
        .to_string();
    if let Some(progress_cb) = progress_cb {
        progress_cb(DownloadProgress {
            phase: "compute_cyq_chen".to_string(),
            finished: 0,
            total: ts_codes.len(),
            current_label: None,
            message: format!(
                "新筹码局部修复已开始，区间 {} 至 {}，共 {} 只股票。",
                start_date,
                end_date,
                ts_codes.len()
            ),
        });
    }

    let (tx, rx) = sync_channel(CYQ_CHEN_QUEUE_BOUND);
    let abort_tx = tx.clone();
    let writer_ts_codes = ts_codes.clone();
    let write_start_date = start_date.clone();
    let write_end_date = end_date.clone();
    let writer_strategy_hash = strategy_hash.clone();
    let writer_handle = thread::spawn(move || {
        write_cyq_chen_stock_repair_batches_from_channel(
            &cyq_chen_db_str,
            rx,
            config,
            &writer_ts_codes,
            &write_start_date,
            &write_end_date,
            writer_strategy_hash,
        )
    });

    let finished_stock_count = std::sync::atomic::AtomicUsize::new(0);
    let compute_result = ts_codes
        .par_chunks(CYQ_CHEN_GROUP_SIZE_INCREMENTAL)
        .try_for_each_with(tx, |sender, ts_group| -> Result<(), String> {
            let worker_reader =
                DataReader::new_with_runtime_keys(source_dir, &required_runtime_keys)?;
            let progress_stock_done = |ts_code: &str| {
                if let Some(progress_cb) = progress_cb {
                    let finished =
                        finished_stock_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                    progress_cb(DownloadProgress {
                        phase: "compute_cyq_chen".to_string(),
                        finished,
                        total: ts_codes.len(),
                        current_label: Some(ts_code.to_string()),
                        message: format!(
                            "新筹码局部修复中，已完成 {finished} / {} 只股票。",
                            ts_codes.len()
                        ),
                    });
                }
            };
            let batch = compute_cyq_chen_stock_group_batch(
                &worker_reader,
                None,
                &load_start_date,
                &start_date,
                &end_date,
                &chip_config,
                config,
                &st_list,
                &total_share_map,
                ts_group,
                Some(&progress_stock_done),
            )?;
            sender
                .send(CyqChenWriteMessage::Batch(batch))
                .map_err(|e| format!("发送新筹码局部修复批次失败:{e}"))?;
            Ok(())
        });

    if let Err(err) = &compute_result {
        let _ = abort_tx.send(CyqChenWriteMessage::Abort(err.clone()));
    }
    drop(abort_tx);

    let writer_result = match writer_handle.join() {
        Ok(result) => result,
        Err(_) => Err("筹码库写线程异常退出".to_string()),
    };

    compute_result?;
    let (snapshot_rows, bin_rows) = writer_result?;
    if let Some(progress_cb) = progress_cb {
        progress_cb(DownloadProgress {
            phase: "done".to_string(),
            finished: ts_codes.len(),
            total: ts_codes.len(),
            current_label: None,
            message: format!(
                "新筹码局部修复完成，写入 {snapshot_rows} 条摘要和 {bin_rows} 条分桶。"
            ),
        });
    }

    Ok(Some(CyqChenRebuildSummary {
        snapshot_rows,
        bin_rows,
        warmup_days: config.warmup_days,
        bucket_pct: config.bucket_pct,
        start_date: Some(start_date),
        end_date: Some(end_date),
    }))
}

pub fn rebuild_cyq_chen_all(
    source_dir: &str,
    config: ChenChipConfig,
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> Result<CyqChenRebuildSummary, String> {
    rebuild_cyq_chen_all_with_progress(source_dir, config, start_date, end_date, None)
}

pub fn rebuild_cyq_chen_all_with_progress(
    source_dir: &str,
    config: ChenChipConfig,
    start_date: Option<&str>,
    end_date: Option<&str>,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<CyqChenRebuildSummary, String> {
    let cyq_chen_db = cyq_chen_db_path(source_dir);
    init_cyq_chen_db(&cyq_chen_db)?;

    let source_db = source_db_path(source_dir);
    if !source_db.exists() {
        clear_cyq_chen_tables(&cyq_chen_db)?;
        return Ok(CyqChenRebuildSummary {
            snapshot_rows: 0,
            bin_rows: 0,
            warmup_days: config.warmup_days,
            bucket_pct: config.bucket_pct,
            start_date: None,
            end_date: None,
        });
    }

    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?;
    let source_conn = Connection::open(source_db_str).map_err(|e| format!("打开原始库失败:{e}"))?;
    if !source_stock_data_exists(&source_conn)? {
        clear_cyq_chen_tables(&cyq_chen_db)?;
        return Ok(CyqChenRebuildSummary {
            snapshot_rows: 0,
            bin_rows: 0,
            warmup_days: config.warmup_days,
            bucket_pct: config.bucket_pct,
            start_date: None,
            end_date: None,
        });
    }

    let Some((start_date, end_date)) =
        resolve_cyq_chen_rebuild_trade_date_range(&source_conn, start_date, end_date)?
    else {
        clear_cyq_chen_tables(&cyq_chen_db)?;
        return Ok(CyqChenRebuildSummary {
            snapshot_rows: 0,
            bin_rows: 0,
            warmup_days: config.warmup_days,
            bucket_pct: config.bucket_pct,
            start_date: None,
            end_date: None,
        });
    };
    let chip_config = load_compiled_chip_change_config(source_dir)?;
    let strategy_hash = current_chip_change_strategy_hash(source_dir)?;
    let expression_warmup_need = estimate_chen_chip_expression_warmup(&chip_config)?;
    let load_warmup_need = config.warmup_days.max(expression_warmup_need);
    let Some(load_start_date) =
        resolve_cyq_chen_load_start_date(source_dir, &start_date, &end_date, load_warmup_need)?
    else {
        clear_cyq_chen_tables(&cyq_chen_db)?;
        return Ok(CyqChenRebuildSummary {
            snapshot_rows: 0,
            bin_rows: 0,
            warmup_days: config.warmup_days,
            bucket_pct: config.bucket_pct,
            start_date: Some(start_date),
            end_date: Some(end_date),
        });
    };

    let required_runtime_keys = collect_chen_chip_runtime_keys(&chip_config);
    let st_list = load_st_list(source_dir).unwrap_or_default();
    let total_share_map = load_total_share_map(source_dir).unwrap_or_default();
    let reader = DataReader::new_with_runtime_keys(source_dir, &required_runtime_keys)?;
    let ts_codes = reader.list_ts_code(DEFAULT_ADJ_TYPE, &load_start_date, &end_date)?;
    let rebuild_db = cyq_chen_rebuild_temp_path(&cyq_chen_db)?;
    remove_cyq_chen_db_artifacts(&rebuild_db);
    init_cyq_chen_db(&rebuild_db).inspect_err(|_| {
        remove_cyq_chen_db_artifacts(&rebuild_db);
    })?;
    let rebuild_db_str = match rebuild_db.to_str() {
        Some(path) => path.to_string(),
        None => {
            remove_cyq_chen_db_artifacts(&rebuild_db);
            return Err("新筹码临时库路径不是有效UTF-8".to_string());
        }
    };
    if let Some(progress_cb) = progress_cb {
        progress_cb(DownloadProgress {
            phase: "compute_cyq_chen".to_string(),
            finished: 0,
            total: ts_codes.len(),
            current_label: None,
            message: format!(
                "新筹码计算已开始，区间 {} 至 {}，共 {} 只股票。",
                start_date,
                end_date,
                ts_codes.len()
            ),
        });
    }

    let (tx, rx) = sync_channel(CYQ_CHEN_QUEUE_BOUND);
    let abort_tx = tx.clone();
    let writer_handle = thread::spawn(move || {
        write_cyq_chen_batches_from_channel(&rebuild_db_str, rx, config, strategy_hash)
    });

    let finished_stock_count = std::sync::atomic::AtomicUsize::new(0);
    let compute_result = ts_codes.par_chunks(CYQ_CHEN_GROUP_SIZE).try_for_each_with(
        tx,
        |sender, ts_group| -> Result<(), String> {
            let worker_reader =
                DataReader::new_with_runtime_keys(source_dir, &required_runtime_keys)?;
            let progress_stock_done = |ts_code: &str| {
                if let Some(progress_cb) = progress_cb {
                    let finished =
                        finished_stock_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                    progress_cb(DownloadProgress {
                        phase: "compute_cyq_chen".to_string(),
                        finished,
                        total: ts_codes.len(),
                        current_label: Some(ts_code.to_string()),
                        message: format!(
                            "新筹码计算中，已完成 {finished} / {} 只股票。",
                            ts_codes.len()
                        ),
                    });
                }
            };
            let batch = compute_cyq_chen_stock_group_batch(
                &worker_reader,
                None,
                &load_start_date,
                &start_date,
                &end_date,
                &chip_config,
                config,
                &st_list,
                &total_share_map,
                ts_group,
                Some(&progress_stock_done),
            )?;
            sender
                .send(CyqChenWriteMessage::Batch(batch))
                .map_err(|e| format!("发送筹码批次失败:{e}"))?;
            Ok(())
        },
    );

    if let Err(err) = &compute_result {
        let _ = abort_tx.send(CyqChenWriteMessage::Abort(err.clone()));
    }
    drop(abort_tx);

    let writer_result = match writer_handle.join() {
        Ok(result) => result,
        Err(_) => Err("筹码库写线程异常退出".to_string()),
    };

    let write_rows = match (compute_result, writer_result) {
        (Ok(()), Ok(rows)) => Ok(rows),
        (Err(error), _) | (_, Err(error)) => Err(error),
    };
    let (snapshot_rows, bin_rows) = match write_rows {
        Ok(rows) => rows,
        Err(error) => {
            remove_cyq_chen_db_artifacts(&rebuild_db);
            return Err(error);
        }
    };
    if let Err(error) = replace_cyq_chen_db(&rebuild_db, &cyq_chen_db) {
        remove_cyq_chen_db_artifacts(&rebuild_db);
        return Err(error);
    }
    if let Some(progress_cb) = progress_cb {
        progress_cb(DownloadProgress {
            phase: "done".to_string(),
            finished: ts_codes.len(),
            total: ts_codes.len(),
            current_label: None,
            message: format!("新筹码计算完成，写入 {snapshot_rows} 条摘要和 {bin_rows} 条分桶。"),
        });
    }

    Ok(CyqChenRebuildSummary {
        snapshot_rows,
        bin_rows,
        warmup_days: config.warmup_days,
        bucket_pct: config.bucket_pct,
        start_date: Some(start_date),
        end_date: Some(end_date),
    })
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::mpsc::sync_channel,
        time::{SystemTime, UNIX_EPOCH},
    };

    use duckdb::{Connection, params};

    use super::{
        CYQ_CHEN_BIN_TABLE, CYQ_CHEN_SCHEMA_VERSION, CYQ_CHEN_SNAPSHOT_TABLE, CyqChenWriteMessage,
        maintain_cyq_chen_incremental_if_db_exists, query_cyq_chen_strategy_maintenance_status,
        rebuild_cyq_chen_all, write_cyq_chen_batches_from_channel,
        write_cyq_chen_incremental_batches_from_channel,
    };
    use crate::data::{
        chip_change_rule_path, cyq_chen::ChenChipConfig, cyq_chen_db_path, source_db_path,
        stock_list_path, trade_calendar_path,
    };

    fn unique_temp_source_dir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!("lianghua-cyq-chen-test-{nanos}"))
    }

    fn write_strategy(source_dir: &Path) {
        fs::write(
            chip_change_rule_path(source_dir.to_str().expect("utf8 path")),
            r#"
version = 1

[[strategy]]
name = "主力买入"
holder = "main"
direction = "buy"
when = "C > O AND ZHANG > 0 AND TOTAL_MV_YI > 0"
bias = 1.0

[[strategy]]
name = "散户卖出"
holder = "retail"
direction = "sell"
when = "RATEC > 1"
bias = 1.0
"#,
        )
        .expect("write strategy");
    }

    fn prepare_source_db(source_dir: &Path) {
        fs::create_dir_all(source_dir).expect("create temp dir");
        fs::write(
            trade_calendar_path(source_dir.to_str().expect("utf8 path")),
            "cal_date\n20260401\n20260402\n20260403\n20260407\n20260408\n",
        )
        .expect("write trade calendar");
        fs::write(
            stock_list_path(source_dir.to_str().expect("utf8 path")),
            "ts_code,symbol,name,area,industry,list_date,market,total_share\n000001.SZ,000001,平安银行,深圳,银行,19910403,主板,20000\n",
        )
        .expect("write stock list");
        write_strategy(source_dir);

        let source_db = source_db_path(source_dir.to_str().expect("utf8 path"));
        let conn = Connection::open(&source_db).expect("open source db");
        conn.execute(
            r#"
            CREATE TABLE stock_data (
                ts_code VARCHAR,
                trade_date VARCHAR,
                adj_type VARCHAR,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                pre_close DOUBLE,
                change DOUBLE,
                pct_chg DOUBLE,
                vol DOUBLE,
                amount DOUBLE,
                tor DOUBLE
            )
            "#,
            [],
        )
        .expect("create stock_data");

        let rows = [
            ("000001.SZ", "20260401", 10.0, 10.3, 9.8, 10.1, 5.0),
            ("000001.SZ", "20260402", 10.1, 10.4, 10.0, 10.3, 5.0),
            ("000001.SZ", "20260403", 10.3, 10.8, 10.2, 10.6, 5.0),
            ("000001.SZ", "20260407", 10.6, 11.6, 10.5, 11.4, 5.0),
            ("000001.SZ", "20260408", 11.4, 11.8, 11.0, 11.6, 5.0),
            ("000002.SZ", "20260401", 20.0, 20.2, 19.8, 20.1, 3.0),
            ("000002.SZ", "20260402", 20.1, 20.4, 20.0, 20.3, 3.0),
        ];

        for (ts_code, trade_date, open, high, low, close, tor) in rows {
            insert_stock_row(&conn, ts_code, trade_date, open, high, low, close, tor);
        }
    }

    fn insert_stock_row(
        conn: &Connection,
        ts_code: &str,
        trade_date: &str,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        tor: f64,
    ) {
        conn.execute(
            r#"
            INSERT INTO stock_data (
                ts_code, trade_date, adj_type, open, high, low, close,
                pre_close, change, pct_chg, vol, amount, tor
            ) VALUES (?, ?, 'qfq', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
            params![
                ts_code, trade_date, open, high, low, close, close, 0.0_f64, 0.0_f64, 1.0_f64,
                1.0_f64, tor
            ],
        )
        .expect("insert source row");
    }

    fn insert_paused_stock_resume_row(source_dir: &Path) {
        let source_db = source_db_path(source_dir.to_str().expect("utf8 path"));
        let conn = Connection::open(&source_db).expect("open source db");
        insert_stock_row(&conn, "000002.SZ", "20260408", 20.3, 21.0, 20.2, 20.8, 4.0);
    }

    fn snapshot_rows_for_compare(source_path: &str) -> Vec<(String, String, f64, f64, f64)> {
        let cyq_chen_db = cyq_chen_db_path(source_path);
        let conn = Connection::open(&cyq_chen_db).expect("open cyq chen db");
        let mut stmt = conn
            .prepare(
                r#"
                SELECT ts_code, trade_date, close, main_total, retail_total
                FROM cyq_chen_snapshot
                ORDER BY ts_code ASC, trade_date ASC
                "#,
            )
            .expect("prepare snapshot compare");
        let mut rows = stmt.query([]).expect("query snapshot compare");
        let mut out = Vec::new();
        while let Some(row) = rows.next().expect("read snapshot compare") {
            out.push((
                row.get(0).expect("ts_code"),
                row.get(1).expect("trade_date"),
                row.get(2).expect("close"),
                row.get(3).expect("main_total"),
                row.get(4).expect("retail_total"),
            ));
        }
        out
    }

    fn bin_rows_for_compare(source_path: &str) -> Vec<(String, String, i64, f64, f64, f64, f64)> {
        let cyq_chen_db = cyq_chen_db_path(source_path);
        let conn = Connection::open(&cyq_chen_db).expect("open cyq chen db");
        let mut stmt = conn
            .prepare(
                r#"
                SELECT ts_code, trade_date, bin_index, price_low, price_high, main_chip, retail_chip
                FROM cyq_chen_bin
                ORDER BY ts_code ASC, trade_date ASC, bin_index ASC
                "#,
            )
            .expect("prepare bin compare");
        let mut rows = stmt.query([]).expect("query bin compare");
        let mut out = Vec::new();
        while let Some(row) = rows.next().expect("read bin compare") {
            out.push((
                row.get(0).expect("ts_code"),
                row.get(1).expect("trade_date"),
                row.get(2).expect("bin_index"),
                row.get(3).expect("price_low"),
                row.get(4).expect("price_high"),
                row.get(5).expect("main_chip"),
                row.get(6).expect("retail_chip"),
            ));
        }
        out
    }

    fn meta_rows_for_compare(source_path: &str) -> Vec<(String, String)> {
        let cyq_chen_db = cyq_chen_db_path(source_path);
        let conn = Connection::open(&cyq_chen_db).expect("open cyq chen db");
        let mut stmt = conn
            .prepare("SELECT key, value FROM cyq_chen_meta ORDER BY key")
            .expect("prepare meta compare");
        let mut rows = stmt.query([]).expect("query meta compare");
        let mut out = Vec::new();
        while let Some(row) = rows.next().expect("read meta compare") {
            out.push((
                row.get(0).expect("meta key"),
                row.get(1).expect("meta value"),
            ));
        }
        out
    }

    fn index_names_for_compare(source_path: &str) -> Vec<String> {
        let cyq_chen_db = cyq_chen_db_path(source_path);
        let conn = Connection::open(&cyq_chen_db).expect("open cyq chen db");
        let mut stmt = conn
            .prepare(
                "SELECT index_name FROM duckdb_indexes() WHERE table_name IN ('cyq_chen_snapshot', 'cyq_chen_bin') ORDER BY index_name",
            )
            .expect("prepare index compare");
        let mut rows = stmt.query([]).expect("query index compare");
        let mut out = Vec::new();
        while let Some(row) = rows.next().expect("read index compare") {
            out.push(row.get(0).expect("index name"));
        }
        out
    }

    fn data_table_primary_key_count(source_path: &str) -> i64 {
        let cyq_chen_db = cyq_chen_db_path(source_path);
        let conn = Connection::open(&cyq_chen_db).expect("open cyq chen db");
        conn.query_row(
            r#"
            SELECT COUNT(*)
            FROM duckdb_constraints()
            WHERE table_name IN ('cyq_chen_snapshot', 'cyq_chen_bin')
              AND constraint_type = 'PRIMARY KEY'
            "#,
            [],
            |row| row.get(0),
        )
        .expect("count data table primary keys")
    }

    fn rebuild_temp_file_count(source_dir: &Path) -> usize {
        fs::read_dir(source_dir)
            .expect("read source dir")
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".cyq_chen.db.rebuild-")
            })
            .count()
    }

    #[test]
    fn rebuild_cyq_chen_all_writes_snapshot_and_bin_rows() {
        let source_dir = unique_temp_source_dir();
        prepare_source_db(&source_dir);
        let source_path = source_dir.to_str().expect("utf8 path");

        let summary = rebuild_cyq_chen_all(
            source_path,
            ChenChipConfig {
                warmup_days: 2,
                bucket_pct: 5.0,
            },
            None,
            None,
        )
        .expect("rebuild cyq chen");

        assert_eq!(summary.snapshot_rows, 3);
        assert!(summary.bin_rows >= summary.snapshot_rows);
        assert_eq!(summary.start_date.as_deref(), Some("20260401"));
        assert_eq!(summary.end_date.as_deref(), Some("20260408"));

        let cyq_chen_db = cyq_chen_db_path(source_path);
        let conn = Connection::open(&cyq_chen_db).expect("open cyq chen db");
        let snapshot_rows = conn
            .query_row(
                &format!("SELECT COUNT(*) FROM {CYQ_CHEN_SNAPSHOT_TABLE}"),
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("count snapshot rows");
        let bin_rows = conn
            .query_row(
                &format!("SELECT COUNT(*) FROM {CYQ_CHEN_BIN_TABLE}"),
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("count bin rows");
        assert_eq!(snapshot_rows, 3);
        assert_eq!(bin_rows as usize, summary.bin_rows);
        assert_eq!(
            index_names_for_compare(source_path),
            vec!["idx_cyq_chen_snapshot_stock_date".to_string()]
        );
        assert_eq!(data_table_primary_key_count(source_path), 0);
        assert_eq!(
            meta_rows_for_compare(source_path)
                .into_iter()
                .find(|(key, _)| key == "schema_version")
                .map(|(_, value)| value),
            Some(CYQ_CHEN_SCHEMA_VERSION.to_string())
        );
        assert_eq!(rebuild_temp_file_count(&source_dir), 0);

        let first_trade_date = conn
            .query_row(
                "SELECT MIN(trade_date) FROM cyq_chen_snapshot WHERE ts_code = '000001.SZ'",
                [],
                |row| row.get::<_, Option<String>>(0),
            )
            .expect("read first trade date");
        assert_eq!(first_trade_date.as_deref(), Some("20260403"));

        let main_total = conn
            .query_row(
                "SELECT main_total FROM cyq_chen_snapshot WHERE ts_code = '000001.SZ' AND trade_date = '20260403'",
                [],
                |row| row.get::<_, f64>(0),
        )
        .expect("read main total");
        assert!(main_total > 50.0);

        let (
            total_profit_ratio,
            total_trapped_ratio,
            main_profit_ratio,
            main_trapped_ratio,
            main_avg_cost,
            chip_peak_price,
            percent_70_price_low,
            percent_70_price_high,
            percent_90_price_low,
            percent_90_price_high,
        ) = conn
            .query_row(
                r#"
                SELECT total_profit_ratio, total_trapped_ratio,
                       main_profit_ratio, main_trapped_ratio,
                       main_avg_cost,
                       chip_peak_price,
                       percent_70_price_low, percent_70_price_high,
                       percent_90_price_low, percent_90_price_high
                FROM cyq_chen_snapshot
                WHERE ts_code = '000001.SZ' AND trade_date = '20260403'
                "#,
                [],
                |row| {
                    Ok((
                        row.get::<_, f64>(0)?,
                        row.get::<_, f64>(1)?,
                        row.get::<_, f64>(2)?,
                        row.get::<_, f64>(3)?,
                        row.get::<_, f64>(4)?,
                        row.get::<_, f64>(5)?,
                        row.get::<_, f64>(6)?,
                        row.get::<_, f64>(7)?,
                        row.get::<_, f64>(8)?,
                        row.get::<_, f64>(9)?,
                    ))
                },
            )
            .expect("read chen snapshot metrics");
        assert!((total_profit_ratio + total_trapped_ratio - 1.0).abs() < 1e-9);
        assert!((main_profit_ratio + main_trapped_ratio - 1.0).abs() < 1e-9);
        assert!((0.0..=1.0).contains(&main_profit_ratio));
        assert!(main_avg_cost > 0.0);
        assert!(chip_peak_price > 0.0);
        assert!(percent_70_price_low <= percent_70_price_high);
        assert!(percent_90_price_low <= percent_90_price_high);

        fs::remove_dir_all(source_dir).expect("cleanup temp dir");
    }

    #[test]
    fn rebuild_cyq_chen_all_supports_requested_trade_date_range() {
        let source_dir = unique_temp_source_dir();
        prepare_source_db(&source_dir);
        let source_path = source_dir.to_str().expect("utf8 path");

        let summary = rebuild_cyq_chen_all(
            source_path,
            ChenChipConfig {
                warmup_days: 2,
                bucket_pct: 5.0,
            },
            Some("20260407"),
            Some("20260408"),
        )
        .expect("rebuild cyq chen range");

        assert_eq!(summary.snapshot_rows, 2);
        assert_eq!(summary.start_date.as_deref(), Some("20260407"));
        assert_eq!(summary.end_date.as_deref(), Some("20260408"));

        let cyq_chen_db = cyq_chen_db_path(source_path);
        let conn = Connection::open(&cyq_chen_db).expect("open cyq chen db");
        let min_trade_date = conn
            .query_row("SELECT MIN(trade_date) FROM cyq_chen_snapshot", [], |row| {
                row.get::<_, Option<String>>(0)
            })
            .expect("read min trade date");
        assert_eq!(min_trade_date.as_deref(), Some("20260407"));

        fs::remove_dir_all(source_dir).expect("cleanup temp dir");
    }

    #[test]
    fn maintain_cyq_chen_incremental_matches_full_rebuild_after_resume_from_pause() {
        let incremental_dir = unique_temp_source_dir();
        prepare_source_db(&incremental_dir);
        let incremental_path = incremental_dir.to_str().expect("utf8 path");
        let config = ChenChipConfig {
            warmup_days: 1,
            bucket_pct: 5.0,
        };

        rebuild_cyq_chen_all(incremental_path, config, Some("20260401"), Some("20260403"))
            .expect("seed incremental cyq chen");
        insert_paused_stock_resume_row(&incremental_dir);

        let summary = maintain_cyq_chen_incremental_if_db_exists(incremental_path, false, None)
            .expect("maintain cyq chen incremental")
            .expect("cyq chen db exists");
        assert_eq!(summary.start_date.as_deref(), Some("20260407"));
        assert_eq!(summary.end_date.as_deref(), Some("20260408"));

        let full_dir = unique_temp_source_dir();
        prepare_source_db(&full_dir);
        insert_paused_stock_resume_row(&full_dir);
        let full_path = full_dir.to_str().expect("utf8 path");
        rebuild_cyq_chen_all(full_path, config, None, None).expect("full rebuild cyq chen");

        assert_eq!(
            snapshot_rows_for_compare(incremental_path),
            snapshot_rows_for_compare(full_path)
        );
        assert_eq!(
            bin_rows_for_compare(incremental_path),
            bin_rows_for_compare(full_path)
        );

        fs::remove_dir_all(incremental_dir).expect("cleanup incremental temp dir");
        fs::remove_dir_all(full_dir).expect("cleanup full temp dir");
    }

    #[test]
    fn interrupted_incremental_write_keeps_official_db_unchanged() {
        let source_dir = unique_temp_source_dir();
        prepare_source_db(&source_dir);
        let source_path = source_dir.to_str().expect("utf8 path");
        let config = ChenChipConfig {
            warmup_days: 1,
            bucket_pct: 5.0,
        };
        rebuild_cyq_chen_all(source_path, config, Some("20260401"), Some("20260403"))
            .expect("seed cyq chen");

        let snapshots_before = snapshot_rows_for_compare(source_path);
        let bins_before = bin_rows_for_compare(source_path);
        let meta_before = meta_rows_for_compare(source_path);
        let cyq_chen_db = cyq_chen_db_path(source_path);
        let (tx, rx) = sync_channel(1);
        tx.send(CyqChenWriteMessage::Abort("test interrupt".to_string()))
            .expect("send abort");
        drop(tx);

        let error = write_cyq_chen_incremental_batches_from_channel(
            cyq_chen_db.to_str().expect("utf8 db path"),
            rx,
            config,
            "20260407",
            "20260408",
            "changed-strategy-hash".to_string(),
        )
        .expect_err("incremental write should abort");

        assert!(error.contains("结果库已回滚"));
        assert_eq!(snapshot_rows_for_compare(source_path), snapshots_before);
        assert_eq!(bin_rows_for_compare(source_path), bins_before);
        assert_eq!(meta_rows_for_compare(source_path), meta_before);

        fs::remove_dir_all(source_dir).expect("cleanup temp dir");
    }

    #[test]
    fn interrupted_full_rebuild_rolls_back_data_meta_and_indexes() {
        let source_dir = unique_temp_source_dir();
        prepare_source_db(&source_dir);
        let source_path = source_dir.to_str().expect("utf8 path");
        let config = ChenChipConfig {
            warmup_days: 1,
            bucket_pct: 5.0,
        };
        rebuild_cyq_chen_all(source_path, config, Some("20260401"), Some("20260403"))
            .expect("seed cyq chen");

        let snapshots_before = snapshot_rows_for_compare(source_path);
        let bins_before = bin_rows_for_compare(source_path);
        let meta_before = meta_rows_for_compare(source_path);
        let indexes_before = index_names_for_compare(source_path);
        assert_eq!(
            indexes_before,
            vec!["idx_cyq_chen_snapshot_stock_date".to_string()]
        );

        let cyq_chen_db = cyq_chen_db_path(source_path);
        let (tx, rx) = sync_channel(1);
        tx.send(CyqChenWriteMessage::Abort("test interrupt".to_string()))
            .expect("send abort");
        drop(tx);

        let error = write_cyq_chen_batches_from_channel(
            cyq_chen_db.to_str().expect("utf8 db path"),
            rx,
            config,
            "changed-strategy-hash".to_string(),
        )
        .expect_err("full rebuild should abort");

        assert!(error.contains("结果库已回滚"));
        assert_eq!(snapshot_rows_for_compare(source_path), snapshots_before);
        assert_eq!(bin_rows_for_compare(source_path), bins_before);
        assert_eq!(meta_rows_for_compare(source_path), meta_before);
        assert_eq!(index_names_for_compare(source_path), indexes_before);

        fs::remove_dir_all(source_dir).expect("cleanup temp dir");
    }

    #[test]
    fn maintain_cyq_chen_incremental_rebuilds_all_when_strategy_changes() {
        let source_dir = unique_temp_source_dir();
        prepare_source_db(&source_dir);
        let source_path = source_dir.to_str().expect("utf8 path");
        let config = ChenChipConfig {
            warmup_days: 1,
            bucket_pct: 5.0,
        };

        rebuild_cyq_chen_all(source_path, config, Some("20260401"), Some("20260403"))
            .expect("seed cyq chen");

        fs::write(
            chip_change_rule_path(source_path),
            r#"
version = 1

[[strategy]]
name = "改动后的主力买入"
holder = "main"
direction = "buy"
when = "C >= O AND TOTAL_MV_YI > 0"
bias = 2.0
"#,
        )
        .expect("rewrite strategy");

        let summary = maintain_cyq_chen_incremental_if_db_exists(source_path, true, None)
            .expect("maintain cyq chen incremental")
            .expect("cyq chen db exists");

        assert_eq!(summary.start_date.as_deref(), Some("20260401"));
        assert_eq!(summary.end_date.as_deref(), Some("20260408"));
        assert!(summary.snapshot_rows > 2);

        let cyq_chen_db = cyq_chen_db_path(source_path);
        let conn = Connection::open(&cyq_chen_db).expect("open cyq chen db");
        let min_trade_date = conn
            .query_row("SELECT MIN(trade_date) FROM cyq_chen_snapshot", [], |row| {
                row.get::<_, Option<String>>(0)
            })
            .expect("read min trade date");
        assert_eq!(min_trade_date.as_deref(), Some("20260402"));

        fs::remove_dir_all(source_dir).expect("cleanup temp dir");
    }

    #[test]
    fn maintain_cyq_chen_incremental_skips_strategy_rebuild_without_confirmation() {
        let source_dir = unique_temp_source_dir();
        prepare_source_db(&source_dir);
        let source_path = source_dir.to_str().expect("utf8 path");
        let config = ChenChipConfig {
            warmup_days: 1,
            bucket_pct: 5.0,
        };

        rebuild_cyq_chen_all(source_path, config, Some("20260401"), Some("20260403"))
            .expect("seed cyq chen");
        let snapshot_rows_before = snapshot_rows_for_compare(source_path);
        let bin_rows_before = bin_rows_for_compare(source_path);

        fs::write(
            chip_change_rule_path(source_path),
            r#"
version = 1

[[strategy]]
name = "未确认的主力买入改动"
holder = "main"
direction = "buy"
when = "C >= O AND TOTAL_MV_YI > 0"
bias = 2.0
"#,
        )
        .expect("rewrite strategy");

        let status = query_cyq_chen_strategy_maintenance_status(source_path)
            .expect("query cyq chen maintenance status");
        assert!(status.strategy_changed);

        let summary = maintain_cyq_chen_incremental_if_db_exists(source_path, false, None)
            .expect("maintain cyq chen incremental")
            .expect("cyq chen db exists");

        assert_eq!(summary.snapshot_rows, 0);
        assert_eq!(summary.bin_rows, 0);
        assert_eq!(summary.start_date, None);
        assert_eq!(snapshot_rows_for_compare(source_path), snapshot_rows_before);
        assert_eq!(bin_rows_for_compare(source_path), bin_rows_before);

        fs::remove_dir_all(source_dir).expect("cleanup temp dir");
    }

    #[test]
    fn maintain_cyq_chen_incremental_preserves_zero_warmup_config() {
        let source_dir = unique_temp_source_dir();
        prepare_source_db(&source_dir);
        let source_path = source_dir.to_str().expect("utf8 path");
        let config = ChenChipConfig {
            warmup_days: 0,
            bucket_pct: 5.0,
        };

        rebuild_cyq_chen_all(source_path, config, Some("20260401"), Some("20260403"))
            .expect("seed cyq chen with zero warmup");

        let summary = maintain_cyq_chen_incremental_if_db_exists(source_path, false, None)
            .expect("maintain cyq chen incremental")
            .expect("cyq chen db exists");

        assert_eq!(summary.start_date.as_deref(), Some("20260407"));
        assert_eq!(summary.end_date.as_deref(), Some("20260408"));
        assert_eq!(summary.warmup_days, 0);

        let cyq_chen_db = cyq_chen_db_path(source_path);
        let conn = Connection::open(&cyq_chen_db).expect("open cyq chen db");
        let max_incremental_warmup = conn
            .query_row(
                "SELECT MAX(warmup_days) FROM cyq_chen_snapshot WHERE trade_date >= '20260407'",
                [],
                |row| row.get::<_, Option<i64>>(0),
            )
            .expect("read incremental warmup days");
        assert_eq!(max_incremental_warmup, Some(0));

        fs::remove_dir_all(source_dir).expect("cleanup temp dir");
    }
}
