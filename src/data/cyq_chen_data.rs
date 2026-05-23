use std::{
    fs::create_dir_all,
    path::Path,
    sync::mpsc::{Receiver, sync_channel},
    thread,
};

use duckdb::{Appender, Connection, params};
use rayon::prelude::*;

use crate::data::{
    DataReader, RowData,
    cyq_chen::{
        ChenChipConfig, ChenChipSnapshot, CompiledChipChangeConfig,
        compute_chen_chip_snapshots_with_compiled_config, load_compiled_chip_change_config,
    },
    cyq_chen_db_path, load_trade_date_list, source_db_path,
};

const CYQ_CHEN_SNAPSHOT_TABLE: &str = "cyq_chen_snapshot";
const CYQ_CHEN_BIN_TABLE: &str = "cyq_chen_bin";
const DEFAULT_ADJ_TYPE: &str = "qfq";
const CYQ_CHEN_GROUP_SIZE: usize = 128;
const CYQ_CHEN_QUEUE_BOUND: usize = 8;
const CYQ_CHEN_FLUSH_BATCH_SIZE: usize = 32;

#[derive(Debug, Clone, PartialEq)]
pub struct CyqChenRebuildSummary {
    pub snapshot_rows: usize,
    pub bin_rows: usize,
    pub warmup_days: usize,
    pub bucket_pct: f64,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
}

#[derive(Debug)]
struct ComputedCyqChenStock {
    ts_code: String,
    snapshots: Vec<(String, ChenChipSnapshot)>,
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
            create_dir_all(parent_dir).map_err(|e| format!("创建陈版筹码库目录失败:{e}"))?;
        }
    }

    let conn = Connection::open(db_path).map_err(|e| format!("打开陈版筹码库失败:{e}"))?;
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
            PRIMARY KEY (ts_code, trade_date, adj_type)
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
            total_chip DOUBLE,
            PRIMARY KEY (ts_code, trade_date, adj_type, bin_index)
        )
        "#,
        [],
    )
    .map_err(|e| format!("创建cyq_chen_bin失败:{e}"))?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cyq_chen_snapshot_trade_date ON cyq_chen_snapshot(trade_date, ts_code)",
        [],
    )
    .map_err(|e| format!("创建cyq_chen_snapshot索引失败:{e}"))?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cyq_chen_bin_trade_date ON cyq_chen_bin(trade_date, ts_code, bin_index)",
        [],
    )
    .map_err(|e| format!("创建cyq_chen_bin索引失败:{e}"))?;

    Ok(())
}

fn clear_cyq_chen_tables(db_path: &Path) -> Result<(), String> {
    init_cyq_chen_db(db_path)?;

    let mut conn = Connection::open(db_path).map_err(|e| format!("打开陈版筹码库失败:{e}"))?;
    let tx = conn
        .transaction()
        .map_err(|e| format!("创建陈版筹码库事务失败:{e}"))?;
    tx.execute("DELETE FROM cyq_chen_bin", [])
        .map_err(|e| format!("清空cyq_chen_bin失败:{e}"))?;
    tx.execute("DELETE FROM cyq_chen_snapshot", [])
        .map_err(|e| format!("清空cyq_chen_snapshot失败:{e}"))?;
    tx.commit()
        .map_err(|e| format!("提交陈版筹码库事务失败:{e}"))?;
    Ok(())
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
        .map_err(|e| format!("预编译陈版筹码计算日期范围失败:{e}"))?;
    let mut rows = stmt
        .query(params![DEFAULT_ADJ_TYPE])
        .map_err(|e| format!("查询陈版筹码计算日期范围失败:{e}"))?;
    let Some(row) = rows
        .next()
        .map_err(|e| format!("读取陈版筹码计算日期范围失败:{e}"))?
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
            "所选陈版筹码计算区间 {requested_range} 与原始库可用区间 {source_range} 没有交集"
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
    reader: &DataReader,
    ts_code: &str,
    load_start_date: &str,
    start_date: &str,
    end_date: &str,
    chip_config: &CompiledChipChangeConfig,
    config: ChenChipConfig,
) -> Result<ComputedCyqChenStock, String> {
    let mut row_data = reader.load_one(ts_code, DEFAULT_ADJ_TYPE, load_start_date, end_date)?;
    if row_data.trade_dates.is_empty() {
        return Ok(ComputedCyqChenStock {
            ts_code: ts_code.to_string(),
            snapshots: Vec::new(),
        });
    }

    if resolve_first_computable_output_date(&row_data, start_date, end_date, config.warmup_days)
        .is_none()
    {
        let output_rows = count_output_rows(&row_data, start_date, end_date);
        if output_rows == 0 {
            return Ok(ComputedCyqChenStock {
                ts_code: ts_code.to_string(),
                snapshots: Vec::new(),
            });
        }

        let need_rows = config.warmup_days.saturating_add(output_rows);
        if need_rows > 0 {
            let tail_row_data =
                reader.load_one_tail_rows(ts_code, DEFAULT_ADJ_TYPE, end_date, need_rows)?;
            if !tail_row_data.trade_dates.is_empty() {
                row_data = tail_row_data;
            }
        }
    }

    let Some(output_start_date) =
        resolve_first_computable_output_date(&row_data, start_date, end_date, config.warmup_days)
    else {
        return Ok(ComputedCyqChenStock {
            ts_code: ts_code.to_string(),
            snapshots: Vec::new(),
        });
    };

    let snapshots = compute_chen_chip_snapshots_with_compiled_config(
        &row_data,
        &output_start_date,
        chip_config,
        config,
    )?;
    let snapshots = snapshots
        .into_iter()
        .filter_map(|snapshot| {
            let trade_date = snapshot.trade_date.clone().unwrap_or_default();
            if trade_date.as_str() < start_date || trade_date.as_str() > end_date {
                return None;
            }
            Some((trade_date, snapshot))
        })
        .collect();

    Ok(ComputedCyqChenStock {
        ts_code: ts_code.to_string(),
        snapshots,
    })
}

fn compute_cyq_chen_stock_group_batch(
    worker_reader: &DataReader,
    load_start_date: &str,
    start_date: &str,
    end_date: &str,
    chip_config: &CompiledChipChangeConfig,
    config: ChenChipConfig,
    ts_group: &[String],
) -> Result<CyqChenWriteBatch, String> {
    let mut batch = CyqChenWriteBatch::default();
    for ts_code in ts_group {
        let stock = compute_cyq_chen_stock(
            worker_reader,
            ts_code,
            load_start_date,
            start_date,
            end_date,
            chip_config,
            config,
        )?;
        if !stock.snapshots.is_empty() {
            batch.stocks.push(stock);
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
    let mut snapshot_rows = 0usize;
    let mut bin_rows = 0usize;

    for stock in batch.stocks {
        let ts_code = stock.ts_code;
        for (trade_date, snapshot) in stock.snapshots {
            snapshot_app
                .append_row(params![
                    &ts_code,
                    &trade_date,
                    DEFAULT_ADJ_TYPE,
                    config.warmup_days as i32,
                    config.bucket_pct,
                    snapshot.close,
                    snapshot.min_price,
                    snapshot.max_price,
                    snapshot.main_total,
                    snapshot.retail_total,
                    snapshot.total_chips
                ])
                .map_err(|e| {
                    format!(
                        "写入cyq_chen_snapshot失败, ts_code={}, trade_date={}: {e}",
                        ts_code, trade_date
                    )
                })?;
            snapshot_rows += 1;

            for bin in snapshot.bins {
                bin_app
                    .append_row(params![
                        &ts_code,
                        &trade_date,
                        DEFAULT_ADJ_TYPE,
                        bin.index as i32,
                        bin.price,
                        bin.price_low,
                        bin.price_high,
                        bin.main_chip,
                        bin.retail_chip,
                        bin.total_chip
                    ])
                    .map_err(|e| {
                        format!(
                            "写入cyq_chen_bin失败, ts_code={}, trade_date={}, bin_index={}: {e}",
                            ts_code, trade_date, bin.index
                        )
                    })?;
                bin_rows += 1;
            }
        }
    }

    Ok((snapshot_rows, bin_rows))
}

fn write_cyq_chen_batches_from_channel(
    db_path: &str,
    rx: Receiver<CyqChenWriteMessage>,
    config: ChenChipConfig,
) -> Result<(usize, usize), String> {
    let mut conn = Connection::open(db_path).map_err(|e| format!("打开陈版筹码库失败:{e}"))?;
    let tx = conn
        .transaction()
        .map_err(|e| format!("创建陈版筹码库事务失败:{e}"))?;
    tx.execute("DELETE FROM cyq_chen_bin", [])
        .map_err(|e| format!("清空cyq_chen_bin失败:{e}"))?;
    tx.execute("DELETE FROM cyq_chen_snapshot", [])
        .map_err(|e| format!("清空cyq_chen_snapshot失败:{e}"))?;

    let mut snapshot_rows = 0usize;
    let mut bin_rows = 0usize;
    let mut batch_count = 0usize;
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
                    return Err(format!("陈版筹码计算中断，结果库回滚:{reason}"));
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

        snapshot_app
            .flush()
            .map_err(|e| format!("刷新cyq_chen_snapshot写入器失败:{e}"))?;
        bin_app
            .flush()
            .map_err(|e| format!("刷新cyq_chen_bin写入器失败:{e}"))?;
    }

    tx.commit()
        .map_err(|e| format!("提交陈版筹码库事务失败:{e}"))?;
    Ok((snapshot_rows, bin_rows))
}

pub fn rebuild_cyq_chen_all(
    source_dir: &str,
    config: ChenChipConfig,
    start_date: Option<&str>,
    end_date: Option<&str>,
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
    let Some(load_start_date) =
        resolve_cyq_chen_load_start_date(source_dir, &start_date, &end_date, config.warmup_days)?
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

    let chip_config = load_compiled_chip_change_config(source_dir)?;
    let reader = DataReader::new(source_dir)?;
    let ts_codes = reader.list_ts_code(DEFAULT_ADJ_TYPE, &load_start_date, &end_date)?;
    let cyq_chen_db_str = cyq_chen_db
        .to_str()
        .ok_or_else(|| "陈版筹码库路径不是有效UTF-8".to_string())?
        .to_string();

    let (tx, rx) = sync_channel(CYQ_CHEN_QUEUE_BOUND);
    let abort_tx = tx.clone();
    let writer_handle =
        thread::spawn(move || write_cyq_chen_batches_from_channel(&cyq_chen_db_str, rx, config));

    let compute_result = ts_codes.par_chunks(CYQ_CHEN_GROUP_SIZE).try_for_each_with(
        tx,
        |sender, ts_group| -> Result<(), String> {
            let worker_reader = DataReader::new(source_dir)?;
            let batch = compute_cyq_chen_stock_group_batch(
                &worker_reader,
                &load_start_date,
                &start_date,
                &end_date,
                &chip_config,
                config,
                ts_group,
            )?;
            sender
                .send(CyqChenWriteMessage::Batch(batch))
                .map_err(|e| format!("发送陈版筹码批次失败:{e}"))?;
            Ok(())
        },
    );

    if let Err(err) = &compute_result {
        let _ = abort_tx.send(CyqChenWriteMessage::Abort(err.clone()));
    }
    drop(abort_tx);

    let writer_result = match writer_handle.join() {
        Ok(result) => result,
        Err(_) => Err("陈版筹码库写线程异常退出".to_string()),
    };

    compute_result?;
    let (snapshot_rows, bin_rows) = writer_result?;

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
        time::{SystemTime, UNIX_EPOCH},
    };

    use duckdb::{Connection, params};

    use super::{CYQ_CHEN_BIN_TABLE, CYQ_CHEN_SNAPSHOT_TABLE, rebuild_cyq_chen_all};
    use crate::data::{
        chip_change_rule_path, cyq_chen::ChenChipConfig, cyq_chen_db_path, source_db_path,
        trade_calendar_path,
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
when = "C > O"
bias = 1.0

[[strategy]]
name = "散户卖出"
holder = "retail"
direction = "sell"
when = "RATEC > 0.01"
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
}
