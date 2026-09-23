use crate::data::cyq_chen_data::compute::{
    append_cyq_chen_batch_rows, compute_cyq_chen_stock_group_batch, finish_cyq_chen_write,
    query_source_trade_date_range, resolve_cyq_chen_load_start_date,
    resolve_cyq_chen_rebuild_trade_date_range, source_stock_data_exists,
    write_cyq_chen_batches_from_channel, write_cyq_chen_incremental_batches_from_channel,
};
use crate::data::cyq_chen_data::store::replace_cyq_chen_db;
use crate::data::cyq_chen_data::store::{
    clear_cyq_chen_tables, current_chip_change_strategy_hash, init_cyq_chen_db,
    query_cyq_chen_meta_value, query_latest_cyq_chen_metadata, remove_cyq_chen_db_artifacts,
    write_cyq_chen_meta,
};
use crate::data::cyq_chen_data::{
    CYQ_CHEN_BIN_TABLE, CYQ_CHEN_FLUSH_BATCH_SIZE, CYQ_CHEN_GROUP_SIZE, CYQ_CHEN_QUEUE_BOUND,
    CYQ_CHEN_SNAPSHOT_TABLE, CyqChenRebuildSummary, CyqChenWriteMessage, DEFAULT_ADJ_TYPE,
};

use crate::data::DataReader;
use crate::data::cyq_chen::ChenChipConfig;
use crate::data::cyq_chen::collect_chen_chip_runtime_keys;
use crate::data::cyq_chen::estimate_chen_chip_expression_warmup;
use crate::data::cyq_chen::load_compiled_chip_change_config;
use crate::data::cyq_chen_db_path;
use crate::data::extras::load_st_list;
use crate::data::extras::load_total_share_map;
use crate::data::load_trade_date_list;
use crate::data::source_db_path;
use duckdb::Connection;
use duckdb::params;
use lianghua_model::DownloadProgress;
use lianghua_model::DownloadProgressCallback;
use rayon::prelude::*;
use std::collections::HashSet;
use std::fs::create_dir_all;
use std::path::Path;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::sync_channel;
use std::thread;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
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
    let compute_result = ts_codes.par_chunks(CYQ_CHEN_GROUP_SIZE).try_for_each_init(
        || DataReader::new_with_runtime_keys(source_dir, &required_runtime_keys),
        |worker_reader, ts_group| -> Result<(), String> {
            let worker_reader = worker_reader.as_ref().map_err(Clone::clone)?;
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
                worker_reader,
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
            tx.send(CyqChenWriteMessage::Batch(batch))
                .map_err(|e| format!("发送新筹码增量批次失败:{e}"))?;
            Ok(())
        },
    );

    if let Err(err) = &compute_result {
        let _ = abort_tx.send(CyqChenWriteMessage::Abort(err.clone()));
    }
    drop(abort_tx);
    drop(tx);

    let writer_result = match writer_handle.join() {
        Ok(result) => result,
        Err(_) => Err("筹码库写线程异常退出".to_string()),
    };

    let (snapshot_rows, bin_rows) = finish_cyq_chen_write(compute_result, writer_result)?;
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

    let Some((existing_start_date, existing_end_date)) = (|db_path: &Path| -> Result<
        Option<(String, String)>,
        String,
    > {
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
    })(&cyq_chen_db)?
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
        (|db_path: &str,
          rx: Receiver<CyqChenWriteMessage>,
          config: ChenChipConfig,
          ts_codes: &[String],
          start_date: &str,
          end_date: &str,
          strategy_hash: String|
         -> Result<(usize, usize), String> {
            let mut conn = Connection::open(db_path).map_err(|e| format!("打开筹码库失败:{e}"))?;

            let write_result = (|| -> Result<(usize, usize), String> {
                let tx = conn
                    .transaction()
                    .map_err(|e| format!("创建筹码库事务失败:{e}"))?;

                for ts_code in ts_codes {
                    tx.execute(
                        "DELETE FROM cyq_chen_checkpoint WHERE ts_code = ? AND adj_type = ?",
                        params![ts_code, DEFAULT_ADJ_TYPE],
                    )
                    .map_err(|e| format!("清理股票新筹码检查点失败: {e}"))?;
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

                        let (added_snapshot_rows, added_bin_rows) = append_cyq_chen_batch_rows(
                            &tx,
                            &mut snapshot_app,
                            &mut bin_app,
                            batch,
                            config,
                        )?;
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

            write_result
        })(
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
    let compute_result = ts_codes.par_chunks(CYQ_CHEN_GROUP_SIZE).try_for_each_init(
        || DataReader::new_with_runtime_keys(source_dir, &required_runtime_keys),
        |worker_reader, ts_group| -> Result<(), String> {
            let worker_reader = worker_reader.as_ref().map_err(Clone::clone)?;
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
                worker_reader,
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
            tx.send(CyqChenWriteMessage::Batch(batch))
                .map_err(|e| format!("发送新筹码局部修复批次失败:{e}"))?;
            Ok(())
        },
    );

    if let Err(err) = &compute_result {
        let _ = abort_tx.send(CyqChenWriteMessage::Abort(err.clone()));
    }
    drop(abort_tx);
    drop(tx);

    let writer_result = match writer_handle.join() {
        Ok(result) => result,
        Err(_) => Err("筹码库写线程异常退出".to_string()),
    };

    let (snapshot_rows, bin_rows) = finish_cyq_chen_write(compute_result, writer_result)?;
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
    create_dir_all(source_dir).map_err(|e| format!("创建新筹码目录失败:{e}"))?;
    let _rebuild_lock = crate::data::cyq_chen_temp::lock_rebuild_directory(Path::new(source_dir))?
        .ok_or_else(|| "该数据目录已有新筹码重建任务，请等待完成后再试".to_string())?;
    crate::data::cyq_chen_temp::remove_stale_rebuilds(Path::new(source_dir))?;
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
    let rebuild_db = (|db_path: &Path| -> Result<std::path::PathBuf, String> {
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
    })(&cyq_chen_db)?;
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

    let compute_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(rayon::current_num_threads().min(4))
        .build()
        .map_err(|e| {
            remove_cyq_chen_db_artifacts(&rebuild_db);
            format!("创建新筹码计算线程池失败:{e}")
        })?;
    let (tx, rx) = sync_channel(CYQ_CHEN_QUEUE_BOUND);
    let abort_tx = tx.clone();
    let writer_handle = thread::spawn(move || {
        write_cyq_chen_batches_from_channel(&rebuild_db_str, rx, config, strategy_hash)
    });

    let finished_stock_count = std::sync::atomic::AtomicUsize::new(0);
    let compute_result = compute_pool.install(|| {
        ts_codes.par_chunks(CYQ_CHEN_GROUP_SIZE).try_for_each_init(
            || DataReader::new_with_runtime_keys(source_dir, &required_runtime_keys),
            |worker_reader, ts_group| -> Result<(), String> {
                let worker_reader = worker_reader.as_ref().map_err(Clone::clone)?;
                let progress_stock_done = |ts_code: &str| {
                    if let Some(progress_cb) = progress_cb {
                        let finished = finished_stock_count
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                            + 1;
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
                    worker_reader,
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
                tx.send(CyqChenWriteMessage::Batch(batch))
                    .map_err(|e| format!("发送筹码批次失败:{e}"))?;
                Ok(())
            },
        )
    });

    if let Err(err) = &compute_result {
        let _ = abort_tx.send(CyqChenWriteMessage::Abort(err.clone()));
    }
    drop(abort_tx);
    drop(tx);

    let writer_result = match writer_handle.join() {
        Ok(result) => result,
        Err(_) => Err("筹码库写线程异常退出".to_string()),
    };

    let write_rows = finish_cyq_chen_write(compute_result, writer_result);
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
    use crate::data::chip_change_rule_path;
    use crate::data::cyq_chen::ChenChipConfig;
    use crate::data::cyq_chen_data::CYQ_CHEN_BIN_TABLE;
    use crate::data::cyq_chen_data::CYQ_CHEN_SCHEMA_VERSION;
    use crate::data::cyq_chen_data::CYQ_CHEN_SNAPSHOT_TABLE;
    use crate::data::cyq_chen_data::CyqChenWriteMessage;
    use crate::data::cyq_chen_data::compute::write_cyq_chen_batches_from_channel;
    use crate::data::cyq_chen_data::compute::write_cyq_chen_incremental_batches_from_channel;
    use crate::data::cyq_chen_data::maintain::maintain_cyq_chen_incremental_if_db_exists;
    use crate::data::cyq_chen_data::maintain::rebuild_cyq_chen_all;
    use crate::data::cyq_chen_data::maintain::repair_cyq_chen_stocks_if_db_exists;
    use crate::data::cyq_chen_data::store::query_cyq_chen_strategy_maintenance_status;
    use crate::data::cyq_chen_data::test_support::*;
    use crate::data::cyq_chen_db_path;
    use duckdb::Connection;
    use std::fs;
    use std::path::Path;
    use std::sync::mpsc::sync_channel;

    #[test]
    fn posterior_database_resume_matches_rebuild_after_pause() {
        let incremental_dir = unique_temp_source_dir();
        let full_dir = unique_temp_source_dir();
        for dir in [&incremental_dir, &full_dir] {
            prepare_source_db(dir);
            let path = chip_change_rule_path(dir.to_str().unwrap());
            let mut text = fs::read_to_string(&path).unwrap();
            text.push_str(
                r#"
    [[strategy]]
    name = "delayed retail confirmation"
    holder = "retail"
    direction = "buy"
    when = "C > REF(C, 2)"
    bias = 0.65
    confirm_after = 2
    "#,
            );
            fs::write(path, text).unwrap();
        }
        let incremental = incremental_dir.to_str().unwrap();
        let full = full_dir.to_str().unwrap();
        let config = ChenChipConfig {
            warmup_days: 1,
            bucket_pct: 5.0,
        };
        rebuild_cyq_chen_all(incremental, config, Some("20260401"), Some("20260403")).unwrap();
        insert_paused_stock_resume_row(&incremental_dir);
        insert_paused_stock_resume_row(&full_dir);
        maintain_cyq_chen_incremental_if_db_exists(incremental, false, None).unwrap();
        rebuild_cyq_chen_all(full, config, None, None).unwrap();
        assert_eq!(
            snapshot_rows_for_compare(incremental),
            snapshot_rows_for_compare(full)
        );
        assert_eq!(
            bin_rows_for_compare(incremental),
            bin_rows_for_compare(full)
        );
        fs::remove_dir_all(incremental_dir).unwrap();
        fs::remove_dir_all(full_dir).unwrap();
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
        assert_eq!(
            (|source_path: &str| -> i64 {
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
            })(source_path),
            0
        );
        assert_eq!(
            meta_rows_for_compare(source_path)
                .into_iter()
                .find(|(key, _)| key == "schema_version")
                .map(|(_, value)| value),
            Some(CYQ_CHEN_SCHEMA_VERSION.to_string())
        );
        assert_eq!(
            (|source_dir: &Path| -> usize {
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
            })(&source_dir),
            0
        );

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
        let ratio_strategy = r#"
[[strategy]]
name = "历史主力比例减仓"
holder = "main"
direction = "sell"
when = "REF(MAIN_CHIP_RATIO, 1) > 0"
bias = 0.5
"#;
        let incremental_strategy_path = chip_change_rule_path(incremental_path);
        let mut incremental_strategy = fs::read_to_string(&incremental_strategy_path).unwrap();
        incremental_strategy.push_str(ratio_strategy);
        fs::write(incremental_strategy_path, incremental_strategy).unwrap();
        let config = ChenChipConfig {
            warmup_days: 1,
            bucket_pct: 5.0,
        };

        rebuild_cyq_chen_all(incremental_path, config, Some("20260401"), Some("20260403"))
            .expect("seed incremental cyq chen");
        let conn = Connection::open(cyq_chen_db_path(incremental_path)).unwrap();
        conn.execute("DROP INDEX idx_cyq_chen_snapshot_stock_date", [])
            .unwrap();
        drop(conn);
        insert_paused_stock_resume_row(&incremental_dir);

        let summary = maintain_cyq_chen_incremental_if_db_exists(incremental_path, false, None)
            .expect("maintain cyq chen incremental")
            .expect("cyq chen db exists");
        assert_eq!(summary.start_date.as_deref(), Some("20260407"));
        assert_eq!(summary.end_date.as_deref(), Some("20260408"));
        assert!(index_names_for_compare(incremental_path).is_empty());

        let full_dir = unique_temp_source_dir();
        prepare_source_db(&full_dir);
        let full_strategy_path = chip_change_rule_path(full_dir.to_str().expect("utf8 path"));
        let mut full_strategy = fs::read_to_string(&full_strategy_path).unwrap();
        full_strategy.push_str(ratio_strategy);
        fs::write(full_strategy_path, full_strategy).unwrap();
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
    fn repair_cyq_chen_single_stock_replaces_existing_indexed_rows() {
        let source_dir = unique_temp_source_dir();
        prepare_source_db(&source_dir);
        let source_path = source_dir.to_str().expect("utf8 path");
        let config = ChenChipConfig {
            warmup_days: 1,
            bucket_pct: 5.0,
        };
        rebuild_cyq_chen_all(source_path, config, None, None).expect("seed cyq chen");
        assert_eq!(
            index_names_for_compare(source_path),
            vec!["idx_cyq_chen_snapshot_stock_date".to_string()]
        );

        let snapshots_before = snapshot_rows_for_compare(source_path);
        let bins_before = bin_rows_for_compare(source_path);

        let summary = repair_cyq_chen_stocks_if_db_exists(
            source_path,
            &["000001.SZ".to_string()],
            false,
            None,
        )
        .expect("repair cyq chen stock")
        .expect("cyq chen db exists");

        assert_eq!(summary.start_date.as_deref(), Some("20260402"));
        assert_eq!(summary.end_date.as_deref(), Some("20260408"));
        assert_eq!(snapshot_rows_for_compare(source_path), snapshots_before);
        assert_eq!(bin_rows_for_compare(source_path), bins_before);
        assert_eq!(
            index_names_for_compare(source_path),
            vec!["idx_cyq_chen_snapshot_stock_date".to_string()]
        );

        fs::remove_dir_all(source_dir).expect("cleanup temp dir");
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
    fn interrupted_staged_rebuild_keeps_committed_batches_out_of_official_db() {
        let source_dir = unique_temp_source_dir();
        prepare_source_db(&source_dir);
        let source_path = source_dir.to_str().unwrap();
        let config = ChenChipConfig {
            warmup_days: 2,
            bucket_pct: 5.0,
        };
        rebuild_cyq_chen_all(source_path, config, None, None).unwrap();
        let before = snapshot_rows_for_compare(source_path);
        let reader = super::DataReader::new(source_path).unwrap();
        let row = reader
            .load_one("000001.SZ", "qfq", "20260401", "20260408")
            .unwrap();
        let snapshots = crate::data::cyq_chen::compute_chen_chip_snapshots_from_row_data(
            &row,
            "20260403",
            &crate::data::cyq_chen::ChipChangeConfig {
                version: 1,
                strategy: Vec::new(),
            },
            config,
        )
        .unwrap();
        assert!(snapshots.len() > 1);
        let count = snapshots.len();
        let stage = source_dir.join("stage.db");
        super::init_cyq_chen_db(&stage).unwrap();
        let (tx, rx) = sync_channel(count + 1);
        for snapshot in snapshots {
            tx.send(CyqChenWriteMessage::Batch(
                crate::data::cyq_chen_data::CyqChenWriteBatch {
                    stocks: vec![crate::data::cyq_chen_data::ComputedCyqChenStock {
                        ts_code: "000001.SZ".to_string(),
                        snapshots: vec![snapshot],
                    }],
                },
            ))
            .unwrap();
        }
        tx.send(CyqChenWriteMessage::Abort(
            "after committed batches".to_string(),
        ))
        .unwrap();
        drop(tx);
        let error = write_cyq_chen_batches_from_channel(
            stage.to_str().unwrap(),
            rx,
            config,
            "unfinished".to_string(),
        )
        .unwrap_err();
        assert!(error.contains("临时库不发布"));
        let conn = Connection::open(&stage).unwrap();
        let stored: usize = conn
            .query_row("SELECT count(*) FROM cyq_chen_snapshot", [], |r| r.get(0))
            .unwrap();
        assert_eq!(stored, count);
        let metadata: usize = conn
            .query_row("SELECT count(*) FROM cyq_chen_meta", [], |r| r.get(0))
            .unwrap();
        assert_eq!(metadata, 0);
        assert_eq!(snapshot_rows_for_compare(source_path), before);
        drop(conn);
        drop(reader);
        fs::remove_dir_all(source_dir).unwrap();
    }

    #[test]
    fn full_rebuild_writer_rejects_nonempty_database() {
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

        assert!(error.contains("只允许写入空临时库"));
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
