use crate::data::cyq_chen_data::{
    CYQ_CHEN_META_TABLE, CYQ_CHEN_SCHEMA_VERSION, CYQ_CHEN_SNAPSHOT_TABLE,
    CyqChenStrategyMaintenanceStatus,
};
// 见父模块 mod.rs

use crate::data::chip_change_rule_path;
use crate::data::cyq_chen::ChenChipConfig;
use crate::data::cyq_chen::round_chen_chip_value;
use crate::data::cyq_chen_db_path;
use duckdb::Connection;
use duckdb::params;
use std::fs;
use std::fs::create_dir_all;
use std::fs::read_to_string;
use std::path::Path;
pub fn init_cyq_chen_db(db_path: &Path) -> Result<(), String> {
    if let Some(parent_dir) = db_path.parent() {
        if !parent_dir.as_os_str().is_empty() {
            create_dir_all(parent_dir).map_err(|e| format!("创建筹码库目录失败:{e}"))?;
        }
    }

    if db_path.file_name().and_then(|name| name.to_str()) == Some("cyq_chen.db") {
        let source_dir = db_path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        if let Some(_lock) = crate::data::cyq_chen_temp::lock_rebuild_directory(source_dir)? {
            crate::data::cyq_chen_temp::remove_stale_rebuilds(source_dir)?;
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

    (|conn: &Connection| -> Result<(), String> {
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
    })(&conn)?;
    conn.execute("CREATE TABLE IF NOT EXISTS cyq_chen_checkpoint (ts_code VARCHAR, adj_type VARCHAR, trade_date VARCHAR, bins VARCHAR, PRIMARY KEY(ts_code, adj_type))", [])
        .map_err(|e| format!("创建新筹码续算状态表失败: {e}"))?;
    ensure_cyq_chen_snapshot_index(&conn)?;

    Ok(())
}

pub(super) fn drop_cyq_chen_db_indexes(conn: &Connection) -> Result<(), String> {
    conn.execute("DROP INDEX IF EXISTS idx_cyq_chen_snapshot_stock_date", [])
        .map_err(|e| format!("删除cyq_chen_snapshot股票日期索引失败:{e}"))?;
    Ok(())
}

pub(super) fn ensure_cyq_chen_db_indexes(conn: &Connection) -> Result<(), String> {
    ensure_cyq_chen_snapshot_index(conn)
}

pub(super) fn ensure_cyq_chen_snapshot_index(conn: &Connection) -> Result<(), String> {
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cyq_chen_snapshot_stock_date ON cyq_chen_snapshot(ts_code, adj_type, trade_date)",
        [],
    )
    .map_err(|e| format!("创建cyq_chen_snapshot股票日期索引失败:{e}"))?;
    Ok(())
}

pub(super) fn path_with_suffix(path: &Path, suffix: &str) -> std::path::PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    value.into()
}

pub(super) fn remove_cyq_chen_db_artifacts(db_path: &Path) {
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(path_with_suffix(db_path, ".wal"));
    let _ = fs::remove_dir_all(path_with_suffix(db_path, ".tmp"));
}

pub(super) fn checkpoint_cyq_chen_db_if_exists(db_path: &Path) -> Result<(), String> {
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
pub(super) fn replace_cyq_chen_db(temp_path: &Path, db_path: &Path) -> Result<(), String> {
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
pub(super) fn replace_cyq_chen_db(temp_path: &Path, db_path: &Path) -> Result<(), String> {
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

pub(super) fn clear_cyq_chen_tables(db_path: &Path) -> Result<(), String> {
    init_cyq_chen_db(db_path)?;

    let mut conn = Connection::open(db_path).map_err(|e| format!("打开筹码库失败:{e}"))?;
    let tx = conn
        .transaction()
        .map_err(|e| format!("创建筹码库事务失败:{e}"))?;
    tx.execute("DELETE FROM cyq_chen_checkpoint", [])
        .map_err(|e| e.to_string())?;
    tx.execute("DELETE FROM cyq_chen_bin", [])
        .map_err(|e| format!("清空cyq_chen_bin失败:{e}"))?;
    tx.execute("DELETE FROM cyq_chen_snapshot", [])
        .map_err(|e| format!("清空cyq_chen_snapshot失败:{e}"))?;
    tx.execute("DELETE FROM cyq_chen_meta", [])
        .map_err(|e| format!("清空cyq_chen_meta失败:{e}"))?;
    tx.commit().map_err(|e| format!("提交筹码库事务失败:{e}"))?;
    Ok(())
}

pub(super) fn current_chip_change_strategy_hash(source_dir: &str) -> Result<String, String> {
    let path = chip_change_rule_path(source_dir);
    let text = read_to_string(&path).map_err(|e| {
        format!(
            "读取筹码变化策略文件失败，无法校验增量状态: path={}, err={e}",
            path.display()
        )
    })?;
    Ok((|text: &str| -> String {
        let mut hash = 0xcbf2_9ce4_8422_2325_u64;
        for byte in text.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        format!("{CYQ_CHEN_SCHEMA_VERSION}:{hash:016x}:{:016x}", text.len())
    })(&text))
}

pub(super) fn query_cyq_chen_meta_value(
    db_path: &Path,
    key: &str,
) -> Result<Option<String>, String> {
    let conn = Connection::open(db_path).map_err(|e| format!("打开新筹码库失败:{e}"))?;
    let table_exists = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [CYQ_CHEN_META_TABLE],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| format!("检查新筹码元信息表失败:{e}"))?;
    if table_exists == 0 {
        return Ok(None);
    }
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

pub(super) fn write_cyq_chen_meta(
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

pub(super) fn query_latest_cyq_chen_metadata(
    db_path: &Path,
) -> Result<Option<(String, ChenChipConfig)>, String> {
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

#[cfg(test)]
mod tests {
    use crate::data::chip_change_rule_path;
    use crate::data::cyq_chen_data::store::query_cyq_chen_strategy_maintenance_status;
    use crate::data::cyq_chen_data::test_support::*;
    use crate::data::cyq_chen_db_path;
    use duckdb::Connection;
    use std::fs;

    #[test]
    fn maintenance_status_does_not_initialize_or_clean_rebuild_files() {
        let source_dir = unique_temp_source_dir();
        fs::create_dir_all(&source_dir).unwrap();
        // An unusable lock path must not prevent a metadata query.
        fs::create_dir(source_dir.join(".cyq_chen.rebuild.lock")).unwrap();
        let stale = source_dir.join(".cyq_chen.db.rebuild-123-456.tmp");
        fs::write(&stale, "keep").unwrap();
        let source_path = source_dir.to_str().unwrap();
        fs::write(chip_change_rule_path(source_path), "version = 1\n").unwrap();
        let db_path = cyq_chen_db_path(source_path);
        let conn = Connection::open(&db_path).unwrap();
        let status = query_cyq_chen_strategy_maintenance_status(source_path).unwrap();
        assert!(status.db_exists);
        assert!(!status.has_data);
        conn.execute_batch(
            "CREATE TABLE cyq_chen_snapshot (trade_date VARCHAR, warmup_days BIGINT, bucket_pct DOUBLE);
             INSERT INTO cyq_chen_snapshot VALUES ('20260401', 120, 1.0);",
        )
        .unwrap();
        let status = query_cyq_chen_strategy_maintenance_status(source_path).unwrap();
        assert!(status.has_data);
        assert!(status.strategy_changed);
        let tables: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(tables, 1);
        assert!(stale.exists());
        drop(conn);
        fs::remove_dir_all(source_dir).unwrap();
    }
}
