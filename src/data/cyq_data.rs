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
    cyq::{CyqConfig, CyqSnapshot, compute_cyq_snapshots_from_row_data},
    cyq_db_path, load_trade_date_list, source_db_path,
};

const CYQ_SNAPSHOT_TABLE: &str = "cyq_snapshot";
const CYQ_BIN_TABLE: &str = "cyq_bin";
const DEFAULT_ADJ_TYPE: &str = "qfq";
const CYQ_GROUP_SIZE: usize = 128;
const CYQ_QUEUE_BOUND: usize = 8;
const CYQ_FLUSH_BATCH_SIZE: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CyqRebuildSummary {
    pub snapshot_rows: usize,
    pub bin_rows: usize,
    pub factor: usize,
    pub range: usize,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
}

#[derive(Debug)]
struct ComputedCyqStock {
    ts_code: String,
    snapshots: Vec<(String, CyqSnapshot)>,
}

#[derive(Debug, Default)]
struct CyqWriteBatch {
    stocks: Vec<ComputedCyqStock>,
}

#[derive(Debug)]
enum CyqWriteMessage {
    Batch(CyqWriteBatch),
    Abort(String),
}

pub fn init_cyq_db(db_path: &Path) -> Result<(), String> {
    if let Some(parent_dir) = db_path.parent() {
        if !parent_dir.as_os_str().is_empty() {
            create_dir_all(parent_dir).map_err(|e| format!("创建筹码库目录失败:{e}"))?;
        }
    }

    let conn = Connection::open(db_path).map_err(|e| format!("打开筹码库失败:{e}"))?;
    conn.execute(
        r#"
        CREATE TABLE IF NOT EXISTS cyq_snapshot (
            ts_code VARCHAR,
            trade_date VARCHAR,
            adj_type VARCHAR,
            range INTEGER,
            factor INTEGER,
            min_accuracy DOUBLE,
            close DOUBLE,
            min_price DOUBLE,
            max_price DOUBLE,
            accuracy DOUBLE,
            total_chips DOUBLE,
            benefit_part DOUBLE,
            avg_cost DOUBLE,
            percent_70_price_low DOUBLE,
            percent_70_price_high DOUBLE,
            percent_70_concentration DOUBLE,
            percent_90_price_low DOUBLE,
            percent_90_price_high DOUBLE,
            percent_90_concentration DOUBLE,
            PRIMARY KEY (ts_code, trade_date, adj_type)
        )
        "#,
        [],
    )
    .map_err(|e| format!("创建cyq_snapshot失败:{e}"))?;
    conn.execute(
        r#"
        CREATE TABLE IF NOT EXISTS cyq_bin (
            ts_code VARCHAR,
            trade_date VARCHAR,
            adj_type VARCHAR,
            bin_index INTEGER,
            price DOUBLE,
            price_low DOUBLE,
            price_high DOUBLE,
            chip DOUBLE,
            chip_pct DOUBLE,
            PRIMARY KEY (ts_code, trade_date, adj_type, bin_index)
        )
        "#,
        [],
    )
    .map_err(|e| format!("创建cyq_bin失败:{e}"))?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cyq_snapshot_trade_date ON cyq_snapshot(trade_date, ts_code)",
        [],
    )
    .map_err(|e| format!("创建cyq_snapshot索引失败:{e}"))?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cyq_bin_trade_date ON cyq_bin(trade_date, ts_code, bin_index)",
        [],
    )
    .map_err(|e| format!("创建cyq_bin索引失败:{e}"))?;

    Ok(())
}

fn clear_cyq_tables(db_path: &Path) -> Result<(), String> {
    init_cyq_db(db_path)?;

    let mut conn = Connection::open(db_path).map_err(|e| format!("打开筹码库失败:{e}"))?;
    let tx = conn
        .transaction()
        .map_err(|e| format!("创建筹码库事务失败:{e}"))?;
    tx.execute("DELETE FROM cyq_bin", [])
        .map_err(|e| format!("清空cyq_bin失败:{e}"))?;
    tx.execute("DELETE FROM cyq_snapshot", [])
        .map_err(|e| format!("清空cyq_snapshot失败:{e}"))?;
    tx.commit().map_err(|e| format!("提交筹码库事务失败:{e}"))?;
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

fn cyq_table_exists(conn: &Connection, table_name: &str) -> Result<bool, String> {
    let table_exists = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| format!("检查筹码库表结构失败:{e}"))?;
    Ok(table_exists > 0)
}

fn query_latest_cyq_metadata(db_path: &Path) -> Result<Option<(String, CyqConfig)>, String> {
    let conn = Connection::open(db_path).map_err(|e| format!("打开筹码库失败:{e}"))?;
    if !cyq_table_exists(&conn, CYQ_SNAPSHOT_TABLE)? {
        return Ok(None);
    }

    let mut stmt = conn
        .prepare(
            r#"
            SELECT trade_date, range, factor, min_accuracy
            FROM cyq_snapshot
            WHERE adj_type = ?
            ORDER BY trade_date DESC
            LIMIT 1
            "#,
        )
        .map_err(|e| format!("预编译最新筹码元数据查询失败:{e}"))?;
    let mut rows = stmt
        .query(params![DEFAULT_ADJ_TYPE])
        .map_err(|e| format!("查询最新筹码元数据失败:{e}"))?;

    let Some(row) = rows
        .next()
        .map_err(|e| format!("读取最新筹码元数据失败:{e}"))?
    else {
        return Ok(None);
    };

    let trade_date: String = row
        .get(0)
        .map_err(|e| format!("读取最新筹码日期失败:{e}"))?;
    let range: Option<i64> = row.get(1).map_err(|e| format!("读取筹码 range 失败:{e}"))?;
    let factor: Option<i64> = row
        .get(2)
        .map_err(|e| format!("读取筹码 factor 失败:{e}"))?;
    let min_accuracy: Option<f64> = row
        .get(3)
        .map_err(|e| format!("读取筹码 min_accuracy 失败:{e}"))?;
    let default_config = CyqConfig::default();
    let config = CyqConfig {
        range: range
            .filter(|value| *value > 0)
            .map(|value| value as usize)
            .unwrap_or(default_config.range),
        factor: factor
            .filter(|value| *value >= 2)
            .map(|value| value as usize)
            .unwrap_or(default_config.factor),
        min_accuracy: min_accuracy
            .filter(|value| value.is_finite() && *value > 0.0)
            .unwrap_or(default_config.min_accuracy),
    };

    Ok(Some((trade_date, config)))
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

fn resolve_cyq_rebuild_trade_date_range(
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

fn resolve_cyq_load_start_date(
    source_dir: &str,
    output_start_date: &str,
    output_end_date: &str,
    range: usize,
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

    if range <= 1 {
        return Ok(Some(trade_dates[first_output_trade_date_index].clone()));
    }

    let load_start_index = first_output_trade_date_index.saturating_sub(range - 1);
    Ok(Some(trade_dates[load_start_index].clone()))
}

fn resolve_cyq_tail_rows_fallback_need(
    row_data: &RowData,
    start_date: &str,
    end_date: &str,
    range: usize,
) -> Option<usize> {
    if range == 0 {
        return None;
    }

    let mut first_output_index = None;
    let mut output_rows = 0usize;
    for (index, trade_date) in row_data.trade_dates.iter().enumerate() {
        let trade_date = trade_date.as_str();
        if trade_date < start_date || trade_date > end_date {
            continue;
        }

        if first_output_index.is_none() {
            first_output_index = Some(index);
        }
        output_rows += 1;
    }

    let first_output_index = first_output_index?;
    if first_output_index + 1 >= range {
        return None;
    }

    Some(range + output_rows.saturating_sub(1))
}

fn compute_cyq_stock(
    reader: &DataReader,
    ts_code: &str,
    load_start_date: &str,
    start_date: &str,
    end_date: &str,
    config: CyqConfig,
) -> Result<ComputedCyqStock, String> {
    let mut row_data = reader.load_one(ts_code, DEFAULT_ADJ_TYPE, load_start_date, end_date)?;
    if row_data.trade_dates.is_empty() {
        return Ok(ComputedCyqStock {
            ts_code: ts_code.to_string(),
            snapshots: Vec::new(),
        });
    }
    if let Some(need_rows) =
        resolve_cyq_tail_rows_fallback_need(&row_data, start_date, end_date, config.range)
    {
        let tail_row_data =
            reader.load_one_tail_rows(ts_code, DEFAULT_ADJ_TYPE, end_date, need_rows)?;
        if !tail_row_data.trade_dates.is_empty() {
            row_data = tail_row_data;
        }
    }
    if config.range > 0 && row_data.trade_dates.len() < config.range {
        return Ok(ComputedCyqStock {
            ts_code: ts_code.to_string(),
            snapshots: Vec::new(),
        });
    }

    let snapshots = compute_cyq_snapshots_from_row_data(&row_data, config)?;
    let snapshots = snapshots
        .into_iter()
        .enumerate()
        .filter_map(|(snapshot_index, snapshot)| {
            if config.range > 0 && snapshot_index + 1 < config.range {
                return None;
            }

            let trade_date = snapshot.trade_date.clone().unwrap_or_default();
            if trade_date.as_str() < start_date || trade_date.as_str() > end_date {
                return None;
            }

            Some((trade_date, snapshot))
        })
        .collect();

    Ok(ComputedCyqStock {
        ts_code: ts_code.to_string(),
        snapshots,
    })
}

fn compute_cyq_stock_group_batch(
    worker_reader: &DataReader,
    load_start_date: &str,
    start_date: &str,
    end_date: &str,
    config: CyqConfig,
    ts_group: &[String],
) -> Result<CyqWriteBatch, String> {
    let mut batch = CyqWriteBatch::default();
    for ts_code in ts_group {
        let stock = compute_cyq_stock(
            worker_reader,
            ts_code,
            load_start_date,
            start_date,
            end_date,
            config,
        )?;
        if !stock.snapshots.is_empty() {
            batch.stocks.push(stock);
        }
    }
    Ok(batch)
}

fn append_cyq_batch_rows(
    snapshot_app: &mut Appender<'_>,
    bin_app: &mut Appender<'_>,
    batch: CyqWriteBatch,
    config: CyqConfig,
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
                    config.range as i32,
                    config.factor as i32,
                    config.min_accuracy,
                    snapshot.close,
                    snapshot.min_price,
                    snapshot.max_price,
                    snapshot.accuracy,
                    snapshot.total_chips,
                    snapshot.benefit_part,
                    snapshot.avg_cost,
                    snapshot.percent_70.price_low,
                    snapshot.percent_70.price_high,
                    snapshot.percent_70.concentration,
                    snapshot.percent_90.price_low,
                    snapshot.percent_90.price_high,
                    snapshot.percent_90.concentration
                ])
                .map_err(|e| {
                    format!(
                        "写入cyq_snapshot失败, ts_code={}, trade_date={}: {e}",
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
                        bin.chip,
                        bin.chip_pct
                    ])
                    .map_err(|e| {
                        format!(
                            "写入cyq_bin失败, ts_code={}, trade_date={}, bin_index={}: {e}",
                            ts_code, trade_date, bin.index
                        )
                    })?;
                bin_rows += 1;
            }
        }
    }

    Ok((snapshot_rows, bin_rows))
}

fn write_cyq_batches_from_channel(
    db_path: &str,
    rx: Receiver<CyqWriteMessage>,
    config: CyqConfig,
) -> Result<(usize, usize), String> {
    let mut conn = Connection::open(db_path).map_err(|e| format!("打开筹码库失败:{e}"))?;
    let tx = conn
        .transaction()
        .map_err(|e| format!("创建筹码库事务失败:{e}"))?;
    tx.execute("DELETE FROM cyq_bin", [])
        .map_err(|e| format!("清空cyq_bin失败:{e}"))?;
    tx.execute("DELETE FROM cyq_snapshot", [])
        .map_err(|e| format!("清空cyq_snapshot失败:{e}"))?;

    let mut snapshot_rows = 0usize;
    let mut bin_rows = 0usize;
    let mut batch_count = 0usize;
    {
        let mut snapshot_app = tx
            .appender(CYQ_SNAPSHOT_TABLE)
            .map_err(|e| format!("创建cyq_snapshot写入器失败:{e}"))?;
        let mut bin_app = tx
            .appender(CYQ_BIN_TABLE)
            .map_err(|e| format!("创建cyq_bin写入器失败:{e}"))?;

        for message in rx {
            let batch = match message {
                CyqWriteMessage::Batch(batch) => batch,
                CyqWriteMessage::Abort(reason) => {
                    return Err(format!("筹码计算中断，结果库回滚:{reason}"));
                }
            };

            let (added_snapshot_rows, added_bin_rows) =
                append_cyq_batch_rows(&mut snapshot_app, &mut bin_app, batch, config)?;
            snapshot_rows += added_snapshot_rows;
            bin_rows += added_bin_rows;
            batch_count += 1;

            if batch_count % CYQ_FLUSH_BATCH_SIZE == 0 {
                snapshot_app
                    .flush()
                    .map_err(|e| format!("刷新cyq_snapshot写入器失败:{e}"))?;
                bin_app
                    .flush()
                    .map_err(|e| format!("刷新cyq_bin写入器失败:{e}"))?;
            }
        }

        snapshot_app
            .flush()
            .map_err(|e| format!("刷新cyq_snapshot写入器失败:{e}"))?;
        bin_app
            .flush()
            .map_err(|e| format!("刷新cyq_bin写入器失败:{e}"))?;
    }

    tx.commit().map_err(|e| format!("提交筹码库事务失败:{e}"))?;
    Ok((snapshot_rows, bin_rows))
}

fn write_cyq_incremental_batches_from_channel(
    db_path: &str,
    rx: Receiver<CyqWriteMessage>,
    config: CyqConfig,
    start_date: &str,
    end_date: &str,
) -> Result<(usize, usize), String> {
    let mut conn = Connection::open(db_path).map_err(|e| format!("打开筹码库失败:{e}"))?;
    let tx = conn
        .transaction()
        .map_err(|e| format!("创建筹码库事务失败:{e}"))?;
    tx.execute(
        "DELETE FROM cyq_bin WHERE adj_type = ? AND trade_date >= ? AND trade_date <= ?",
        params![DEFAULT_ADJ_TYPE, start_date, end_date],
    )
    .map_err(|e| format!("清理增量区间cyq_bin失败:{e}"))?;
    tx.execute(
        "DELETE FROM cyq_snapshot WHERE adj_type = ? AND trade_date >= ? AND trade_date <= ?",
        params![DEFAULT_ADJ_TYPE, start_date, end_date],
    )
    .map_err(|e| format!("清理增量区间cyq_snapshot失败:{e}"))?;

    let mut snapshot_rows = 0usize;
    let mut bin_rows = 0usize;
    let mut batch_count = 0usize;
    {
        let mut snapshot_app = tx
            .appender(CYQ_SNAPSHOT_TABLE)
            .map_err(|e| format!("创建cyq_snapshot写入器失败:{e}"))?;
        let mut bin_app = tx
            .appender(CYQ_BIN_TABLE)
            .map_err(|e| format!("创建cyq_bin写入器失败:{e}"))?;

        for message in rx {
            let batch = match message {
                CyqWriteMessage::Batch(batch) => batch,
                CyqWriteMessage::Abort(reason) => {
                    return Err(format!("筹码增量计算中断，结果库回滚:{reason}"));
                }
            };

            let (added_snapshot_rows, added_bin_rows) =
                append_cyq_batch_rows(&mut snapshot_app, &mut bin_app, batch, config)?;
            snapshot_rows += added_snapshot_rows;
            bin_rows += added_bin_rows;
            batch_count += 1;

            if batch_count % CYQ_FLUSH_BATCH_SIZE == 0 {
                snapshot_app
                    .flush()
                    .map_err(|e| format!("刷新cyq_snapshot写入器失败:{e}"))?;
                bin_app
                    .flush()
                    .map_err(|e| format!("刷新cyq_bin写入器失败:{e}"))?;
            }
        }

        snapshot_app
            .flush()
            .map_err(|e| format!("刷新cyq_snapshot写入器失败:{e}"))?;
        bin_app
            .flush()
            .map_err(|e| format!("刷新cyq_bin写入器失败:{e}"))?;
    }

    tx.commit().map_err(|e| format!("提交筹码库事务失败:{e}"))?;
    Ok((snapshot_rows, bin_rows))
}

pub fn maintain_cyq_incremental_if_db_exists(
    source_dir: &str,
) -> Result<Option<CyqRebuildSummary>, String> {
    let cyq_db = cyq_db_path(source_dir);
    if !cyq_db.exists() {
        return Ok(None);
    }

    init_cyq_db(&cyq_db)?;

    let source_db = source_db_path(source_dir);
    if !source_db.exists() {
        return Ok(Some(CyqRebuildSummary {
            snapshot_rows: 0,
            bin_rows: 0,
            factor: CyqConfig::default().factor,
            range: CyqConfig::default().range,
            start_date: None,
            end_date: None,
        }));
    }

    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?;
    let source_conn = Connection::open(source_db_str).map_err(|e| format!("打开原始库失败:{e}"))?;
    if !source_stock_data_exists(&source_conn)? {
        return Ok(Some(CyqRebuildSummary {
            snapshot_rows: 0,
            bin_rows: 0,
            factor: CyqConfig::default().factor,
            range: CyqConfig::default().range,
            start_date: None,
            end_date: None,
        }));
    }

    let Some((source_min_trade_date, source_max_trade_date)) =
        query_source_trade_date_range(&source_conn)?
    else {
        return Ok(Some(CyqRebuildSummary {
            snapshot_rows: 0,
            bin_rows: 0,
            factor: CyqConfig::default().factor,
            range: CyqConfig::default().range,
            start_date: None,
            end_date: None,
        }));
    };

    let latest_metadata = query_latest_cyq_metadata(&cyq_db)?;
    let config = latest_metadata
        .as_ref()
        .map(|(_, config)| *config)
        .unwrap_or_default();
    let start_date = match latest_metadata.as_ref() {
        Some((latest_trade_date, _)) if latest_trade_date >= &source_max_trade_date => {
            return Ok(Some(CyqRebuildSummary {
                snapshot_rows: 0,
                bin_rows: 0,
                factor: config.factor,
                range: config.range,
                start_date: None,
                end_date: None,
            }));
        }
        Some((latest_trade_date, _)) => {
            let trade_dates = load_trade_date_list(source_dir)?;
            let Some(next_trade_date) = trade_dates.into_iter().find(|trade_date| {
                trade_date > latest_trade_date && trade_date <= &source_max_trade_date
            }) else {
                return Ok(Some(CyqRebuildSummary {
                    snapshot_rows: 0,
                    bin_rows: 0,
                    factor: config.factor,
                    range: config.range,
                    start_date: None,
                    end_date: None,
                }));
            };
            next_trade_date
        }
        None => source_min_trade_date,
    };
    let end_date = source_max_trade_date;

    let Some(load_start_date) =
        resolve_cyq_load_start_date(source_dir, &start_date, &end_date, config.range)?
    else {
        return Ok(Some(CyqRebuildSummary {
            snapshot_rows: 0,
            bin_rows: 0,
            factor: config.factor,
            range: config.range,
            start_date: Some(start_date),
            end_date: Some(end_date),
        }));
    };

    let reader = DataReader::new(source_dir)?;
    let ts_codes = reader.list_ts_code(DEFAULT_ADJ_TYPE, &load_start_date, &end_date)?;
    let cyq_db_str = cyq_db
        .to_str()
        .ok_or_else(|| "筹码库路径不是有效UTF-8".to_string())?
        .to_string();

    let (tx, rx) = sync_channel(CYQ_QUEUE_BOUND);
    let abort_tx = tx.clone();
    let write_start_date = start_date.clone();
    let write_end_date = end_date.clone();
    let writer_handle = thread::spawn(move || {
        write_cyq_incremental_batches_from_channel(
            &cyq_db_str,
            rx,
            config,
            &write_start_date,
            &write_end_date,
        )
    });

    let compute_result = ts_codes.par_chunks(CYQ_GROUP_SIZE).try_for_each_with(
        tx,
        |sender, ts_group| -> Result<(), String> {
            let worker_reader = DataReader::new(source_dir)?;
            let batch = compute_cyq_stock_group_batch(
                &worker_reader,
                &load_start_date,
                &start_date,
                &end_date,
                config,
                ts_group,
            )?;
            sender
                .send(CyqWriteMessage::Batch(batch))
                .map_err(|e| format!("发送筹码增量批次失败:{e}"))?;
            Ok(())
        },
    );

    if let Err(err) = &compute_result {
        let _ = abort_tx.send(CyqWriteMessage::Abort(err.clone()));
    }
    drop(abort_tx);

    let writer_result = match writer_handle.join() {
        Ok(result) => result,
        Err(_) => Err("筹码库写线程异常退出".to_string()),
    };

    compute_result?;
    let (snapshot_rows, bin_rows) = writer_result?;

    Ok(Some(CyqRebuildSummary {
        snapshot_rows,
        bin_rows,
        factor: config.factor,
        range: config.range,
        start_date: Some(start_date),
        end_date: Some(end_date),
    }))
}

pub fn rebuild_cyq_all(
    source_dir: &str,
    config: CyqConfig,
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> Result<CyqRebuildSummary, String> {
    let cyq_db = cyq_db_path(source_dir);
    init_cyq_db(&cyq_db)?;

    let source_db = source_db_path(source_dir);
    if !source_db.exists() {
        clear_cyq_tables(&cyq_db)?;
        return Ok(CyqRebuildSummary {
            snapshot_rows: 0,
            bin_rows: 0,
            factor: config.factor,
            range: config.range,
            start_date: None,
            end_date: None,
        });
    }

    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?;
    let source_conn = Connection::open(source_db_str).map_err(|e| format!("打开原始库失败:{e}"))?;
    if !source_stock_data_exists(&source_conn)? {
        clear_cyq_tables(&cyq_db)?;
        return Ok(CyqRebuildSummary {
            snapshot_rows: 0,
            bin_rows: 0,
            factor: config.factor,
            range: config.range,
            start_date: None,
            end_date: None,
        });
    }

    let Some((start_date, end_date)) =
        resolve_cyq_rebuild_trade_date_range(&source_conn, start_date, end_date)?
    else {
        clear_cyq_tables(&cyq_db)?;
        return Ok(CyqRebuildSummary {
            snapshot_rows: 0,
            bin_rows: 0,
            factor: config.factor,
            range: config.range,
            start_date: None,
            end_date: None,
        });
    };
    let Some(load_start_date) =
        resolve_cyq_load_start_date(source_dir, &start_date, &end_date, config.range)?
    else {
        clear_cyq_tables(&cyq_db)?;
        return Ok(CyqRebuildSummary {
            snapshot_rows: 0,
            bin_rows: 0,
            factor: config.factor,
            range: config.range,
            start_date: Some(start_date),
            end_date: Some(end_date),
        });
    };

    let reader = DataReader::new(source_dir)?;
    let ts_codes = reader.list_ts_code(DEFAULT_ADJ_TYPE, &load_start_date, &end_date)?;
    let cyq_db_str = cyq_db
        .to_str()
        .ok_or_else(|| "筹码库路径不是有效UTF-8".to_string())?
        .to_string();

    let (tx, rx) = sync_channel(CYQ_QUEUE_BOUND);
    let abort_tx = tx.clone();
    let writer_handle =
        thread::spawn(move || write_cyq_batches_from_channel(&cyq_db_str, rx, config));

    let compute_result = ts_codes.par_chunks(CYQ_GROUP_SIZE).try_for_each_with(
        tx,
        |sender, ts_group| -> Result<(), String> {
            let worker_reader = DataReader::new(source_dir)?;
            let batch = compute_cyq_stock_group_batch(
                &worker_reader,
                &load_start_date,
                &start_date,
                &end_date,
                config,
                ts_group,
            )?;
            sender
                .send(CyqWriteMessage::Batch(batch))
                .map_err(|e| format!("发送筹码批次失败:{e}"))?;
            Ok(())
        },
    );

    if let Err(err) = &compute_result {
        let _ = abort_tx.send(CyqWriteMessage::Abort(err.clone()));
    }
    drop(abort_tx);

    let writer_result = match writer_handle.join() {
        Ok(result) => result,
        Err(_) => Err("筹码库写线程异常退出".to_string()),
    };

    compute_result?;
    let (snapshot_rows, bin_rows) = writer_result?;

    Ok(CyqRebuildSummary {
        snapshot_rows,
        bin_rows,
        factor: config.factor,
        range: config.range,
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

    use super::{
        CYQ_BIN_TABLE, CYQ_SNAPSHOT_TABLE, maintain_cyq_incremental_if_db_exists, rebuild_cyq_all,
    };
    use crate::data::{cyq::CyqConfig, cyq_db_path, source_db_path, trade_calendar_path};

    fn unique_temp_source_dir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!("lianghua-cyq-test-{nanos}"))
    }

    fn prepare_source_db(source_dir: &Path) {
        fs::create_dir_all(source_dir).expect("create temp dir");
        fs::write(
            trade_calendar_path(source_dir.to_str().expect("utf8 path")),
            "cal_date\n20260401\n20260402\n20260403\n",
        )
        .expect("write trade calendar");

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
            ("000001.SZ", "20260401", 10.0, 10.3, 9.9, 10.2, 1.2),
            ("000001.SZ", "20260402", 10.2, 10.5, 10.1, 10.4, 1.6),
            ("000001.SZ", "20260403", 10.4, 10.6, 10.2, 10.5, 2.1),
            ("000002.SZ", "20260401", 20.0, 20.2, 19.8, 20.1, 0.9),
            ("000002.SZ", "20260402", 20.1, 20.4, 20.0, 20.3, 1.1),
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
    fn rebuild_cyq_all_writes_snapshot_and_bins() {
        let source_dir = unique_temp_source_dir();
        prepare_source_db(&source_dir);

        let summary = rebuild_cyq_all(
            source_dir.to_str().expect("utf8 path"),
            CyqConfig {
                range: 2,
                factor: 8,
                min_accuracy: 0.01,
            },
            None,
            None,
        )
        .expect("rebuild cyq");

        assert_eq!(summary.snapshot_rows, 3);
        assert_eq!(summary.bin_rows, 24);
        assert_eq!(summary.start_date.as_deref(), Some("20260401"));
        assert_eq!(summary.end_date.as_deref(), Some("20260403"));

        let cyq_db = cyq_db_path(source_dir.to_str().expect("utf8 path"));
        let conn = Connection::open(&cyq_db).expect("open cyq db");
        let snapshot_rows = conn
            .query_row(
                &format!("SELECT COUNT(*) FROM {CYQ_SNAPSHOT_TABLE}"),
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("count snapshot rows");
        let bin_rows = conn
            .query_row(
                &format!("SELECT COUNT(*) FROM {CYQ_BIN_TABLE}"),
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("count bin rows");
        assert_eq!(snapshot_rows, 3);
        assert_eq!(bin_rows, 24);

        let stored_factor = conn
            .query_row("SELECT MAX(factor) FROM cyq_snapshot", [], |row| {
                row.get::<_, Option<i32>>(0)
            })
            .expect("read factor");
        assert_eq!(stored_factor, Some(8));

        fs::remove_dir_all(source_dir).expect("cleanup temp dir");
    }

    #[test]
    fn rebuild_cyq_all_supports_requested_trade_date_range() {
        let source_dir = unique_temp_source_dir();
        prepare_source_db(&source_dir);

        let summary = rebuild_cyq_all(
            source_dir.to_str().expect("utf8 path"),
            CyqConfig {
                range: 3,
                factor: 8,
                min_accuracy: 0.01,
            },
            Some("20260402"),
            Some("20260403"),
        )
        .expect("rebuild cyq with date range");

        assert_eq!(summary.snapshot_rows, 1);
        assert_eq!(summary.bin_rows, 8);
        assert_eq!(summary.start_date.as_deref(), Some("20260402"));
        assert_eq!(summary.end_date.as_deref(), Some("20260403"));

        let cyq_db = cyq_db_path(source_dir.to_str().expect("utf8 path"));
        let conn = Connection::open(&cyq_db).expect("open cyq db");
        let snapshot_rows = conn
            .query_row(
                &format!("SELECT COUNT(*) FROM {CYQ_SNAPSHOT_TABLE}"),
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("count snapshot rows");
        assert_eq!(snapshot_rows, 1);

        fs::remove_dir_all(source_dir).expect("cleanup temp dir");
    }

    #[test]
    fn rebuild_cyq_all_falls_back_to_tail_rows_when_calendar_warmup_is_sparse() {
        let source_dir = unique_temp_source_dir();
        fs::create_dir_all(&source_dir).expect("create temp dir");
        fs::write(
            trade_calendar_path(source_dir.to_str().expect("utf8 path")),
            "cal_date\n20260331\n20260401\n20260402\n20260403\n",
        )
        .expect("write trade calendar");

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
            ("20260331", 9.8, 10.1, 9.7, 10.0, 1.0),
            ("20260401", 10.0, 10.3, 9.9, 10.2, 1.2),
            ("20260403", 10.4, 10.6, 10.2, 10.5, 2.1),
        ];
        for (trade_date, open, high, low, close, tor) in rows {
            conn.execute(
                r#"
                INSERT INTO stock_data (
                    ts_code, trade_date, adj_type, open, high, low, close,
                    pre_close, change, pct_chg, vol, amount, tor
                ) VALUES ('000001.SZ', ?, 'qfq', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
                params![
                    trade_date, open, high, low, close, close, 0.0_f64, 0.0_f64, 1.0_f64, 1.0_f64,
                    tor
                ],
            )
            .expect("insert source row");
        }

        let summary = rebuild_cyq_all(
            source_dir.to_str().expect("utf8 path"),
            CyqConfig {
                range: 3,
                factor: 8,
                min_accuracy: 0.01,
            },
            Some("20260403"),
            Some("20260403"),
        )
        .expect("rebuild cyq with sparse warmup");

        assert_eq!(summary.snapshot_rows, 1);
        assert_eq!(summary.bin_rows, 8);
        assert_eq!(summary.start_date.as_deref(), Some("20260403"));
        assert_eq!(summary.end_date.as_deref(), Some("20260403"));

        fs::remove_dir_all(source_dir).expect("cleanup temp dir");
    }

    #[test]
    fn maintain_cyq_incremental_skips_when_db_missing() {
        let source_dir = unique_temp_source_dir();
        prepare_source_db(&source_dir);

        let summary =
            maintain_cyq_incremental_if_db_exists(source_dir.to_str().expect("utf8 path"))
                .expect("maintain cyq incremental");

        assert!(summary.is_none());
        assert!(!cyq_db_path(source_dir.to_str().expect("utf8 path")).exists());

        fs::remove_dir_all(source_dir).expect("cleanup temp dir");
    }

    #[test]
    fn maintain_cyq_incremental_appends_missing_trade_dates() {
        let source_dir = unique_temp_source_dir();
        prepare_source_db(&source_dir);

        let source_path = source_dir.to_str().expect("utf8 path");
        rebuild_cyq_all(
            source_path,
            CyqConfig {
                range: 2,
                factor: 8,
                min_accuracy: 0.01,
            },
            Some("20260401"),
            Some("20260402"),
        )
        .expect("seed cyq db");

        let summary = maintain_cyq_incremental_if_db_exists(source_path)
            .expect("maintain cyq incremental")
            .expect("cyq db exists");

        assert_eq!(summary.snapshot_rows, 1);
        assert_eq!(summary.bin_rows, 8);
        assert_eq!(summary.factor, 8);
        assert_eq!(summary.start_date.as_deref(), Some("20260403"));
        assert_eq!(summary.end_date.as_deref(), Some("20260403"));

        let cyq_db = cyq_db_path(source_path);
        let conn = Connection::open(&cyq_db).expect("open cyq db");
        let snapshot_rows = conn
            .query_row(
                &format!("SELECT COUNT(*) FROM {CYQ_SNAPSHOT_TABLE}"),
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("count snapshot rows");
        let bin_rows = conn
            .query_row(
                &format!("SELECT COUNT(*) FROM {CYQ_BIN_TABLE}"),
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("count bin rows");
        let latest_trade_date = conn
            .query_row("SELECT MAX(trade_date) FROM cyq_snapshot", [], |row| {
                row.get::<_, Option<String>>(0)
            })
            .expect("read latest cyq date");

        assert_eq!(snapshot_rows, 3);
        assert_eq!(bin_rows, 24);
        assert_eq!(latest_trade_date.as_deref(), Some("20260403"));

        fs::remove_dir_all(source_dir).expect("cleanup temp dir");
    }
}
