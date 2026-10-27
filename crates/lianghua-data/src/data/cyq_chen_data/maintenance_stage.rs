use super::compute::{append_prepared_cyq_chen_batch_rows, prepare_cyq_chen_batch_rows};
use super::store::{init_cyq_chen_db, remove_cyq_chen_db_artifacts, write_cyq_chen_meta};
use super::{CYQ_CHEN_BIN_TABLE, CYQ_CHEN_SNAPSHOT_TABLE, CyqChenWriteMessage, DEFAULT_ADJ_TYPE};
use crate::data::cyq_chen::ChenChipConfig;
use duckdb::{Connection, OptionalExt, params};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::mpsc::Receiver;

pub(super) fn prepare_maintenance_stage(
    source_dir: &str,
    config: ChenChipConfig,
    strategy_hash: &str,
    start_date: &str,
    end_date: &str,
    load_start_date: &str,
    ts_codes: &[String],
    repair: bool,
) -> Result<(PathBuf, HashSet<String>), String> {
    let path = Path::new(source_dir).join(".cyq_chen.db.maintenance.tmp");
    if !path.exists() && super::store::path_with_suffix(&path, ".wal").exists() {
        return Err(format!(
            "新筹码维护库缺失但 WAL 仍存在，请检查磁盘；保留现场: {}",
            path.display()
        ));
    }
    let mut inputs = Vec::new();
    // 正式结果也是增量的输入，不能将另一轮发布前后的续算状态混用。
    for name in [
        "stock_data.db",
        "stock_data.db.wal",
        "cyq_chen.db",
        "cyq_chen.db.wal",
    ] {
        let generation = match std::fs::metadata(Path::new(source_dir).join(name)) {
            Ok(meta) => Some((meta.len(), meta.modified().map_err(|e| e.to_string())?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(format!("读取维护输入信息失败, {name}: {e}")),
        };
        inputs.push((name, generation));
    }
    // 基础资料在重复下载时可能原样重写；按内容比较，避免仅修改时间变化就丢弃进度。
    let mut small_inputs = Vec::new();
    for name in ["stock_list.csv", "trade_calendar.csv", "ind.toml"] {
        let contents = match std::fs::read_to_string(Path::new(source_dir).join(name)) {
            Ok(contents) => Some(contents),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(format!("读取维护输入失败, {name}: {e}")),
        };
        small_inputs.push((name, contents));
    }
    let signature = serde_json::to_string(&(
        inputs,
        small_inputs,
        config,
        strategy_hash,
        start_date,
        end_date,
        load_start_date,
        ts_codes,
        repair,
        super::CYQ_CHEN_SCHEMA_VERSION,
        crate::data::cyq_chen::CYQ_CHEN_FEATURE_VERSION,
    ))
    .map_err(|e| e.to_string())?;
    let mut completed = HashSet::new();
    if path.exists() {
        // 读取或 WAL 恢复出错必须保留现场；只有确认输入已变才丢弃旧代次。
        let conn = Connection::open(&path)
            .map_err(|e| format!("恢复新筹码维护进度失败，文件保留在 {}: {e}", path.display()))?;
        let exists: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'cyq_chen_maintenance_input')",
            [], |row| row.get(0),
        ).map_err(|e| e.to_string())?;
        let previous: Option<String> = if exists {
            conn.query_row(
                "SELECT signature FROM cyq_chen_maintenance_input",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| e.to_string())?
        } else {
            None
        };
        if previous.as_deref() == Some(signature.as_str()) {
            let mut stmt = conn
                .prepare("SELECT ts_code FROM cyq_chen_maintenance_done")
                .map_err(|e| e.to_string())?;
            completed = stmt
                .query_map([], |row| row.get(0))
                .map_err(|e| e.to_string())?
                .collect::<Result<_, _>>()
                .map_err(|e| e.to_string())?;
        } else {
            drop(conn);
            remove_cyq_chen_db_artifacts(&path)?;
        }
    }
    init_cyq_chen_db(&path)?;
    let mut conn = Connection::open(&path).map_err(|e| e.to_string())?;
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    tx.execute_batch("CREATE TABLE IF NOT EXISTS cyq_chen_maintenance_input(signature VARCHAR, total BIGINT); CREATE TABLE IF NOT EXISTS cyq_chen_maintenance_done(ts_code VARCHAR PRIMARY KEY, replace_history BOOLEAN); DELETE FROM cyq_chen_maintenance_input;")
        .map_err(|e| e.to_string())?;
    tx.execute(
        "INSERT INTO cyq_chen_maintenance_input VALUES (?, ?)",
        params![signature, ts_codes.len() as i64],
    )
    .map_err(|e| e.to_string())?;
    tx.commit().map_err(|e| e.to_string())?;
    Ok((path, completed))
}

pub(super) fn write_cyq_chen_maintenance_batches_from_channel(
    mut conn: Connection,
    stage_path: &Path,
    rx: Receiver<CyqChenWriteMessage>,
    config: ChenChipConfig,
    start_date: &str,
    end_date: &str,
    strategy_hash: String,
    repair_ts_codes: Option<&[String]>,
) -> Result<(usize, usize), String> {
    let result = (|| -> Result<(usize, usize), String> {
        let mut stage = Connection::open(stage_path).map_err(|e| e.to_string())?;
        let mut finished = false;
        let mut interrupted = None;
        while !finished && interrupted.is_none() {
            let tx = stage.transaction().map_err(|e| e.to_string())?;
            {
                let mut snapshots = tx
                    .appender(CYQ_CHEN_SNAPSHOT_TABLE)
                    .map_err(|e| e.to_string())?;
                let mut bins = tx.appender(CYQ_CHEN_BIN_TABLE).map_err(|e| e.to_string())?;
                let mut stocks = 0;
                let mut bin_rows = 0;
                for _ in 0..64 {
                    let batch = match rx.recv() {
                        Ok(CyqChenWriteMessage::Batch(batch)) => {
                            prepare_cyq_chen_batch_rows(batch, config)?
                        }
                        Ok(CyqChenWriteMessage::PreparedBatch(batch)) => batch,
                        Ok(CyqChenWriteMessage::Finish) => {
                            finished = true;
                            break;
                        }
                        Ok(CyqChenWriteMessage::Abort(reason)) => {
                            interrupted = Some(reason);
                            break;
                        }
                        Err(_) => {
                            interrupted = Some("计算通道提前关闭，未收到全部成功标记".into());
                            break;
                        }
                    };
                    if !batch.errors.is_empty() {
                        return Err("维护批次包含未完成的个股错误".into());
                    }
                    for code in &batch.completed_ts_codes {
                        tx.execute(
                            "INSERT INTO cyq_chen_maintenance_done VALUES (?, ?)",
                            params![code, batch.replace_ts_codes.contains(code)],
                        )
                        .map_err(|e| format!("保存维护股票进度失败:{e}"))?;
                    }
                    stocks += batch.completed_ts_codes.len();
                    bin_rows += batch.bin_rows;
                    append_prepared_cyq_chen_batch_rows(&tx, &mut snapshots, &mut bins, batch)?;
                    if stocks >= 32 || bin_rows >= 500_000 {
                        break;
                    }
                }
                snapshots.flush().map_err(|e| e.to_string())?;
                bins.flush().map_err(|e| e.to_string())?;
            }
            // Abort/断连也提交已成功收到的完整股票，正式结果仍完全不动。
            tx.commit().map_err(|e| format!("提交维护进度失败:{e}"))?;
        }
        if let Some(reason) = interrupted {
            return Err(format!("新筹码维护计算中断:{reason}"));
        }
        let (done, total): (i64, i64) = stage.query_row(
            "SELECT (SELECT count(*) FROM cyq_chen_maintenance_done), total FROM cyq_chen_maintenance_input",
            [], |row| Ok((row.get(0)?, row.get(1)?)),
        ).map_err(|e| e.to_string())?;
        if done != total {
            return Err(format!("维护股票未全部完成: {done}/{total}，拒绝发布"));
        }
        drop(stage);

        // 暂存库只读挂载，发布事务只写正式库，避免跨库写事务。
        conn.execute_batch(&format!(
            "ATTACH '{}' AS cyq_maintenance_stage (READ_ONLY)",
            stage_path.to_string_lossy().replace('\'', "''")
        ))
        .map_err(|e| format!("挂载维护进度失败:{e}"))?;
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        let snapshot_rows: i64 = tx
            .query_row(
                "SELECT count(*) FROM cyq_maintenance_stage.cyq_chen_snapshot",
                [],
                |row| row.get(0),
            )
            .map_err(|e| e.to_string())?;
        let bin_rows: i64 = tx
            .query_row(
                "SELECT count(*) FROM cyq_maintenance_stage.cyq_chen_bin",
                [],
                |row| row.get(0),
            )
            .map_err(|e| e.to_string())?;
        for table in [
            "cyq_chen_checkpoint",
            CYQ_CHEN_BIN_TABLE,
            CYQ_CHEN_SNAPSHOT_TABLE,
        ] {
            if let Some(codes) = repair_ts_codes {
                for code in codes {
                    tx.execute(
                        &format!("DELETE FROM {table} WHERE ts_code = ? AND adj_type = ?"),
                        params![code, DEFAULT_ADJ_TYPE],
                    )
                    .map_err(|e| e.to_string())?;
                }
            } else if table == "cyq_chen_checkpoint" {
                tx.execute("DELETE FROM cyq_chen_checkpoint WHERE adj_type = ? AND ts_code IN (SELECT ts_code FROM cyq_maintenance_stage.cyq_chen_checkpoint)", [DEFAULT_ADJ_TYPE]).map_err(|e| e.to_string())?;
            } else {
                tx.execute(&format!("DELETE FROM {table} WHERE adj_type = ? AND (trade_date BETWEEN ? AND ? OR ts_code IN (SELECT ts_code FROM cyq_maintenance_stage.cyq_chen_maintenance_done WHERE replace_history))"), params![DEFAULT_ADJ_TYPE, start_date, end_date]).map_err(|e| e.to_string())?;
            }
            tx.execute_batch(&format!(
                "INSERT INTO {table} BY NAME SELECT * FROM cyq_maintenance_stage.{table}"
            ))
            .map_err(|e| format!("发布维护结果失败, {table}: {e}"))?;
        }
        write_cyq_chen_meta(&tx, config, &strategy_hash)?;
        tx.commit().map_err(|e| format!("提交维护结果失败:{e}"))?;
        Ok((snapshot_rows as usize, bin_rows as usize))
    })();
    // try_clone 的主连接仍可能存活；先解除实例上的挂载，再清理暂存文件。
    let _ = conn.execute_batch("DETACH DATABASE IF EXISTS cyq_maintenance_stage");
    drop(conn);
    match result {
        Ok(rows) => {
            // 发布成功后的清理不应把已成功的任务报成失败。
            if let Err(error) = remove_cyq_chen_db_artifacts(stage_path) {
                eprintln!("清理已发布维护进度失败:{error}");
            }
            Ok(rows)
        }
        Err(error) => Err(format!(
            "{error}；正式结果未发布，已提交进度保留在 {}，相同输入重试可自动续算",
            stage_path.display()
        )),
    }
}
