use crate::data::cyq_chen_data::compute::{
    compute_cyq_chen_stock_group_batch, compute_prepared_cyq_chen_stock_group_batch,
    finish_cyq_chen_write, query_source_trade_date_range, resolve_cyq_chen_load_start_date,
    resolve_cyq_chen_rebuild_trade_date_range, source_stock_data_exists,
    write_cyq_chen_batches_from_channel, write_cyq_chen_maintenance_batches_from_channel,
};
use crate::data::cyq_chen_data::store::replace_cyq_chen_db;
use crate::data::cyq_chen_data::store::{
    current_chip_change_strategy_hash, init_cyq_chen_db, query_cyq_chen_meta_value,
    query_latest_cyq_chen_metadata, remove_cyq_chen_db_artifacts, remove_cyq_chen_db_sidecars,
};
use crate::data::cyq_chen_data::{
    CYQ_CHEN_QUEUE_BOUND, CYQ_CHEN_SNAPSHOT_TABLE, CyqChenCheckpointRecovery,
    CyqChenRebuildSummary, CyqChenWriteMessage, DEFAULT_ADJ_TYPE,
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
use duckdb::OptionalExt;
use duckdb::params;
use lianghua_model::DownloadProgress;
use lianghua_model::DownloadProgressCallback;
use rayon::prelude::*;
use std::collections::HashSet;
use std::fs::create_dir_all;
use std::path::Path;
use std::sync::mpsc::sync_channel;
use std::thread;

fn cyq_chen_streaming_stock_block_size() -> usize {
    128
}

pub fn maintain_cyq_chen_incremental_if_db_exists(
    source_dir: &str,
    allow_strategy_rebuild: bool,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<Option<CyqChenRebuildSummary>, String> {
    crate::data::download_state::ensure_stock_data_ready_for_derived(source_dir)?;
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
    let source_conn =
        crate::data::open_db_readonly(source_db_str).map_err(|e| format!("打开原始库失败:{e}"))?;
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

    drop(source_conn);
    let latest_metadata = query_latest_cyq_chen_metadata(&cyq_chen_db)?;
    let config = latest_metadata
        .as_ref()
        .map(|(_, config)| *config)
        .unwrap_or_default();
    if latest_metadata.is_none() {
        return rebuild_cyq_chen_all_with_progress(source_dir, config, None, None, progress_cb)
            .map(Some);
    }
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
    if latest_metadata.is_some()
        && query_cyq_chen_meta_value(&cyq_chen_db, "feature_version")?.as_deref()
            != Some(
                crate::data::cyq_chen::CYQ_CHEN_FEATURE_VERSION
                    .to_string()
                    .as_str(),
            )
    {
        let conn = crate::data::open_db_readonly(&cyq_chen_db).map_err(|e| e.to_string())?;
        let first_date: String = conn
            .query_row(
                "SELECT MIN(trade_date) FROM cyq_chen_snapshot WHERE adj_type = ?",
                [DEFAULT_ADJ_TYPE],
                |row| row.get(0),
            )
            .map_err(|e| format!("读取筹码特征重放起点失败: {e}"))?;
        drop(conn);
        return rebuild_cyq_chen_all_with_progress(
            source_dir,
            config,
            Some(&first_date),
            None,
            progress_cb,
        )
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
    // 即使模型无需预热，续算也需要该股上一根行情来校验检查点衔接。
    let load_warmup_need = config.warmup_days.max(expression_warmup_need).max(1);
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
    let state_db = std::sync::Mutex::new(
        Connection::open(&cyq_chen_db).map_err(|e| format!("打开新筹码库失败:{e}"))?,
    );
    let writer_conn = state_db
        .lock()
        .map_err(|_| "新筹码数据库连接锁异常".to_string())?
        .try_clone()
        .map_err(|e| format!("克隆新筹码写入连接失败:{e}"))?;
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

    // 独立只读实例也不参与写实例的块管理；续算状态必须与写线程共享实例。
    // scope 保证异常退出也先结束写线程，再释放 try_clone 借用的主数据库句柄。
    let replay_first_date = std::sync::OnceLock::new();
    let (compute_result, writer_result) = thread::scope(|scope| {
        let (tx, rx) = sync_channel(CYQ_CHEN_QUEUE_BOUND);
        let abort_tx = tx.clone();
        let write_start_date = start_date.clone();
        let write_end_date = end_date.clone();
        let writer_strategy_hash = strategy_hash.clone();
        let writer_handle = scope.spawn(move || {
            write_cyq_chen_maintenance_batches_from_channel(
                writer_conn,
                rx,
                config,
                &write_start_date,
                &write_end_date,
                writer_strategy_hash,
                None,
            )
        });

        let finished_stock_count = std::sync::atomic::AtomicUsize::new(0);
        let compute_group_size = 64usize.div_ceil(rayon::current_num_threads().max(1)).max(1);
        let compute_result = ts_codes.par_chunks(compute_group_size).try_for_each_init(
            || -> Result<(DataReader, Connection), String> {
                let state_conn = state_db
                    .lock()
                    .map_err(|_| "新筹码数据库连接锁异常".to_string())?
                    .try_clone()
                    .map_err(|e| format!("克隆新筹码状态读取连接失败:{e}"))?;
                let worker_reader =
                    DataReader::new_with_runtime_keys(source_dir, &required_runtime_keys)?;
                Ok((worker_reader, state_conn))
            },
            |worker_reader, ts_group| -> Result<(), String> {
                let (worker_reader, state_conn) = worker_reader.as_ref().map_err(Clone::clone)?;
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
                                "新筹码增量计算中，已计算 {finished} / {} 只股票。",
                                ts_codes.len()
                            ),
                        });
                    }
                };
                let checkpoint_recovery = |ts_code: &str, reason: &str| {
                    if let Some(progress_cb) = progress_cb {
                        progress_cb(DownloadProgress {
                            phase: "compute_cyq_chen".into(),
                            finished: finished_stock_count.load(std::sync::atomic::Ordering::Relaxed),
                            total: ts_codes.len(),
                            current_label: Some(ts_code.to_string()),
                            message: format!("{ts_code} 续算状态不可用，正在从行情重放该股；完成后与本轮增量一起提交。原因: {reason}"),
                        });
                    }
                };
                let batch = compute_cyq_chen_stock_group_batch(
                    worker_reader,
                    Some(state_conn),
                    &load_start_date,
                    &start_date,
                    &end_date,
                    &chip_config,
                    config,
                    &st_list,
                    &total_share_map,
                    ts_group,
                    Some(&progress_stock_done),
                    Some(&CyqChenCheckpointRecovery {
                        source_dir,
                        first_date: &replay_first_date,
                        report: &checkpoint_recovery,
                    }),
                )?;
                tx.send(CyqChenWriteMessage::Batch(batch))
                    .map_err(|e| format!("发送新筹码增量批次失败:{e}"))?;
                Ok(())
            },
        );

        match &compute_result {
            Ok(()) => {
                let _ = abort_tx.send(CyqChenWriteMessage::Finish);
            }
            Err(err) => {
                let _ = abort_tx.send(CyqChenWriteMessage::Abort(err.clone()));
            }
        }
        drop(abort_tx);
        drop(tx);

        if compute_result.is_ok() {
            if let Some(progress_cb) = progress_cb {
                progress_cb(DownloadProgress {
                    phase: "compute_cyq_chen".to_string(),
                    finished: ts_codes.len(),
                    total: ts_codes.len(),
                    current_label: None,
                    message: "筹码计算已结束，正在等待写入、事务提交及结果库收尾。".to_string(),
                });
            }
        }

        let writer_result = match writer_handle.join() {
            Ok(result) => result,
            Err(_) => Err("筹码库写线程异常退出".to_string()),
        };

        (compute_result, writer_result)
    });

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
    crate::data::download_state::ensure_stock_data_ready_for_derived(source_dir)?;
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

    if query_cyq_chen_meta_value(&cyq_chen_db, "feature_version")?.as_deref()
        != Some(
            crate::data::cyq_chen::CYQ_CHEN_FEATURE_VERSION
                .to_string()
                .as_str(),
        )
    {
        return maintain_cyq_chen_incremental_if_db_exists(
            source_dir,
            allow_strategy_rebuild,
            progress_cb,
        );
    }

    let Some((existing_start_date, existing_end_date)) = (|db_path: &Path| -> Result<
        Option<(String, String)>,
        String,
    > {
        init_cyq_chen_db(db_path)?;
        let conn =
            crate::data::open_db_readonly(db_path).map_err(|e| format!("打开新筹码库失败:{e}"))?;
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
    let source_conn =
        crate::data::open_db_readonly(source_db_str).map_err(|e| format!("打开原始库失败:{e}"))?;
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
        let conn =
            Connection::open(&cyq_chen_db_str).map_err(|e| format!("打开新筹码库失败:{e}"))?;
        write_cyq_chen_maintenance_batches_from_channel(
            conn,
            rx,
            config,
            &write_start_date,
            &write_end_date,
            writer_strategy_hash,
            Some(&writer_ts_codes),
        )
    });

    let finished_stock_count = std::sync::atomic::AtomicUsize::new(0);
    let compute_group_size = cyq_chen_streaming_stock_block_size();
    let compute_result = ts_codes.par_chunks(compute_group_size).try_for_each_init(
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
                            "新筹码局部修复计算中，已计算 {finished} / {} 只股票。",
                            ts_codes.len()
                        ),
                    });
                }
            };
            compute_prepared_cyq_chen_stock_group_batch(
                worker_reader,
                &load_start_date,
                &start_date,
                &end_date,
                &chip_config,
                config,
                &st_list,
                &total_share_map,
                ts_group,
                Some(&progress_stock_done),
                false,
                &mut |batch| {
                    tx.send(CyqChenWriteMessage::PreparedBatch(batch))
                        .map_err(|e| format!("发送新筹码局部修复批次失败:{e}"))
                },
            )?;
            Ok(())
        },
    );

    match &compute_result {
        Ok(()) => {
            let _ = abort_tx.send(CyqChenWriteMessage::Finish);
        }
        Err(err) => {
            let _ = abort_tx.send(CyqChenWriteMessage::Abort(err.clone()));
        }
    }
    drop(abort_tx);
    drop(tx);

    if compute_result.is_ok() {
        if let Some(progress_cb) = progress_cb {
            progress_cb(DownloadProgress {
                phase: "compute_cyq_chen".to_string(),
                finished: ts_codes.len(),
                total: ts_codes.len(),
                current_label: None,
                message: "筹码计算已结束，正在等待写入、事务提交及结果库收尾。".to_string(),
            });
        }
    }

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
    crate::data::download_state::ensure_stock_data_ready_for_derived(source_dir)?;
    create_dir_all(source_dir).map_err(|e| format!("创建新筹码目录失败:{e}"))?;
    let _rebuild_lock = crate::data::cyq_chen_temp::lock_rebuild_directory(Path::new(source_dir))?
        .ok_or_else(|| "该数据目录已有新筹码重建任务，请等待完成后再试".to_string())?;
    crate::data::cyq_chen_temp::remove_stale_rebuilds(Path::new(source_dir))?;
    let rebuild_db = Path::new(source_dir).join(".cyq_chen.db.resume.tmp");
    if !rebuild_db.exists() {
        remove_cyq_chen_db_sidecars(&rebuild_db)?;
    }
    let cyq_chen_db = cyq_chen_db_path(source_dir);

    let source_db = source_db_path(source_dir);
    if !source_db.exists() {
        return Err("缺少可重建的行情或交易日历，新筹码旧结果保持不变".into());
    }

    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?;
    let source_conn =
        crate::data::open_db_readonly(source_db_str).map_err(|e| format!("打开原始库失败:{e}"))?;
    if !source_stock_data_exists(&source_conn)? {
        return Err("缺少可重建的行情或交易日历，新筹码旧结果保持不变".into());
    }

    let Some((start_date, end_date)) =
        resolve_cyq_chen_rebuild_trade_date_range(&source_conn, start_date, end_date)?
    else {
        return Err("缺少可重建的行情或交易日历，新筹码旧结果保持不变".into());
    };
    let chip_config = load_compiled_chip_change_config(source_dir)?;
    let strategy_hash = current_chip_change_strategy_hash(source_dir)?;
    let expression_warmup_need = estimate_chen_chip_expression_warmup(&chip_config)?;
    let load_warmup_need = config.warmup_days.max(expression_warmup_need);
    let Some(load_start_date) =
        resolve_cyq_chen_load_start_date(source_dir, &start_date, &end_date, load_warmup_need)?
    else {
        return Err("缺少可重建的行情或交易日历，新筹码旧结果保持不变".into());
    };

    let required_runtime_keys = collect_chen_chip_runtime_keys(&chip_config);
    let st_list = load_st_list(source_dir).unwrap_or_default();
    let total_share_map = load_total_share_map(source_dir).unwrap_or_default();
    let reader = DataReader::new_with_runtime_keys(source_dir, &required_runtime_keys)?;
    let ts_codes = reader.list_ts_code(DEFAULT_ADJ_TYPE, &load_start_date, &end_date)?;
    // 文件代次保守校验：即使日期不变，历史修订、WAL 和股票资料变化也不能混用。
    let mut input_files = Vec::new();
    for name in [
        "stock_data.db",
        "stock_data.db.wal",
        "stock_list.csv",
        "trade_calendar.csv",
        "ind.toml",
    ] {
        let path = Path::new(source_dir).join(name);
        let generation = match std::fs::metadata(&path) {
            Ok(metadata) => Some((
                metadata.len(),
                metadata
                    .modified()
                    .map_err(|e| format!("读取输入修改时间失败:{e}"))?,
            )),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => {
                return Err(format!(
                    "读取重建输入信息失败, path={}: {error}",
                    path.display()
                ));
            }
        };
        input_files.push((name, generation));
    }
    let signature = serde_json::to_string(&(
        &input_files,
        &strategy_hash,
        config,
        &start_date,
        &end_date,
        &load_start_date,
        &ts_codes,
        super::CYQ_CHEN_SCHEMA_VERSION,
        crate::data::cyq_chen::CYQ_CHEN_FEATURE_VERSION,
    ))
    .map_err(|e| format!("序列化重建输入标识失败:{e}"))?;
    let mut completed = HashSet::<String>::new();
    if rebuild_db.exists() {
        // 打不开或 WAL 无法恢复时保留现场，不把 I/O 故障当成无进度而直接删库。
        let conn = Connection::open(&rebuild_db).map_err(|e| {
            format!(
                "恢复新筹码重建库失败，进度保留在 {}: {e}",
                rebuild_db.display()
            )
        })?;
        let has_manifest: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'cyq_chen_rebuild_input')",
            [], |row| row.get(0),
        ).map_err(|e| format!("读取重建标识失败:{e}"))?;
        let previous: Option<String> = if has_manifest {
            conn.query_row("SELECT signature FROM cyq_chen_rebuild_input", [], |row| {
                row.get(0)
            })
            .optional()
            .map_err(|e| format!("读取重建输入失败:{e}"))?
        } else {
            None
        };
        if previous.as_deref() == Some(&signature) {
            let mut stmt = conn
                .prepare("SELECT ts_code FROM cyq_chen_rebuild_done")
                .map_err(|e| format!("读取重建进度失败:{e}"))?;
            completed = stmt
                .query_map([], |row| row.get(0))
                .map_err(|e| format!("查询重建进度失败:{e}"))?
                .collect::<Result<_, _>>()
                .map_err(|e| format!("读取重建股票失败:{e}"))?;
        } else {
            drop(conn);
            // 输入变化时丢弃整个旧代次，不能仅凭股票末日判断可复用。
            remove_cyq_chen_db_artifacts(&rebuild_db)?;
            if rebuild_db.exists() || super::store::path_with_suffix(&rebuild_db, ".wal").exists() {
                return Err("清理输入已变化的重建库失败，请检查磁盘后重试".into());
            }
        }
    }
    init_cyq_chen_db(&rebuild_db)?;
    {
        let mut conn =
            Connection::open(&rebuild_db).map_err(|e| format!("打开重建进度库失败:{e}"))?;
        let tx = conn
            .transaction()
            .map_err(|e| format!("创建重建标识事务失败:{e}"))?;
        tx.execute_batch("CREATE TABLE IF NOT EXISTS cyq_chen_rebuild_input(signature VARCHAR); CREATE TABLE IF NOT EXISTS cyq_chen_rebuild_done(ts_code VARCHAR PRIMARY KEY); CREATE TABLE IF NOT EXISTS cyq_chen_rebuild_error(ts_code VARCHAR PRIMARY KEY, error VARCHAR);")
            .map_err(|e| format!("创建重建进度表失败:{e}"))?;
        tx.execute("DELETE FROM cyq_chen_rebuild_input", [])
            .map_err(|e| e.to_string())?;
        tx.execute(
            "INSERT INTO cyq_chen_rebuild_input VALUES (?)",
            [&signature],
        )
        .map_err(|e| e.to_string())?;
        tx.commit().map_err(|e| format!("提交重建标识失败:{e}"))?;
    }
    let total_stocks = ts_codes.len();
    let resumed_stocks = completed.len();
    let ts_codes = ts_codes
        .into_iter()
        .filter(|code| !completed.contains(code))
        .collect::<Vec<_>>();
    let rebuild_db_str = rebuild_db
        .to_str()
        .ok_or_else(|| "新筹码临时库路径不是有效UTF-8".to_string())?
        .to_string();
    if let Some(progress_cb) = progress_cb {
        progress_cb(DownloadProgress {
            phase: "compute_cyq_chen".to_string(),
            finished: resumed_stocks,
            total: total_stocks,
            current_label: None,
            message: format!(
                "新筹码计算已开始，区间 {} 至 {}，剩余 {} 只股票，已恢复 {resumed_stocks} 只已提交进度。",
                start_date,
                end_date,
                ts_codes.len()
            ),
        });
    }

    let compute_threads = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1);
    let compute_group_size = cyq_chen_streaming_stock_block_size();
    let compute_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(compute_threads)
        .build()
        .map_err(|e| format!("创建新筹码计算线程池失败:{e}"))?;
    let (tx, rx) = sync_channel(4);
    let abort_tx = tx.clone();
    let written_stocks = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(resumed_stocks));
    let writer_written_stocks = std::sync::Arc::clone(&written_stocks);
    let writer_handle = thread::spawn(move || {
        write_cyq_chen_batches_from_channel(
            &rebuild_db_str,
            rx,
            config,
            strategy_hash,
            Some(writer_written_stocks),
        )
    });

    let finished_stock_count = std::sync::atomic::AtomicUsize::new(resumed_stocks);
    let profile_writes = std::env::var_os("LIANGHUA_CYQ_WRITE_PROFILE").is_some();
    let compute_result = compute_pool.install(|| {
        ts_codes.par_chunks(compute_group_size).try_for_each_init(
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
                            total: total_stocks,
                            current_label: Some(ts_code.to_string()),
                            message: format!(
                                "新筹码计算中，已计算 {finished} / {} 只股票，已写入提交 {} 只。",
                                total_stocks,
                                written_stocks.load(std::sync::atomic::Ordering::Relaxed),
                            ),
                        });
                    }
                };
                let started = std::time::Instant::now();
                let mut send_wait_ms = 0u64;
                let rows = compute_prepared_cyq_chen_stock_group_batch(
                    worker_reader,
                    &load_start_date,
                    &start_date,
                    &end_date,
                    &chip_config,
                    config,
                    &st_list,
                    &total_share_map,
                    ts_group,
                    Some(&progress_stock_done),
                    true,
                    &mut |batch| {
                        let send_started = std::time::Instant::now();
                        let result = tx
                            .send(CyqChenWriteMessage::PreparedBatch(batch))
                            .map_err(|e| format!("发送筹码批次失败:{e}"));
                        send_wait_ms += send_started.elapsed().as_millis() as u64;
                        result
                    },
                )?;
                let compute_and_send_ms = started.elapsed().as_millis();
                if profile_writes {
                    eprintln!("新筹码计算转列 stocks={} snapshots={} bins={} compute_and_send_ms={compute_and_send_ms} send_wait_ms={send_wait_ms}", ts_group.len(), rows.0, rows.1);
                }
                Ok(())
            },
        )
    });

    if let Err(err) = &compute_result {
        let _ = abort_tx.send(CyqChenWriteMessage::Abort(err.clone()));
    }
    drop(abort_tx);
    drop(tx);

    if compute_result.is_ok() {
        if let Some(progress_cb) = progress_cb {
            progress_cb(DownloadProgress {
                phase: "compute_cyq_chen".to_string(),
                finished: total_stocks,
                total: total_stocks,
                current_label: None,
                message: format!(
                    "筹码计算已结束，已写入提交 {} / {} 只股票，正在等待剩余写入及结果库收尾。",
                    written_stocks.load(std::sync::atomic::Ordering::Relaxed),
                    total_stocks,
                ),
            });
        }
    }

    let writer_result = match writer_handle.join() {
        Ok(result) => result,
        Err(_) => Err("筹码库写线程异常退出".to_string()),
    };

    let write_rows = finish_cyq_chen_write(compute_result, writer_result);
    let (snapshot_rows, bin_rows) = match write_rows {
        Ok(rows) => rows,
        Err(error) => {
            return Err(format!(
                "{error}；已提交的新筹码进度保留在 {}，磁盘恢复后以相同输入重试可自动续算",
                rebuild_db.display()
            ));
        }
    };
    if snapshot_rows == 0 {
        let errors = super::store::query_cyq_chen_rebuild_errors(source_dir)?;
        if !errors.is_empty() {
            return Err(format!(
                "新筹码重建未生成任何快照，{} 只股票计算失败；错误记录和续算进度保留在 {}。首个错误：{} {}",
                errors.len(),
                rebuild_db.display(),
                errors[0].ts_code,
                errors[0].error
            ));
        }
        remove_cyq_chen_db_artifacts(&rebuild_db)?;
        return Err("新筹码重建未生成任何快照，旧结果保持不变，请检查行情和预热区间".into());
    }
    if let Err(error) = replace_cyq_chen_db(&rebuild_db, &cyq_chen_db) {
        return Err(format!(
            "{error}；重建结果保留在 {}，重试可继续收尾",
            rebuild_db.display()
        ));
    }
    drop(reader);
    drop(source_conn);
    crate::data::download_state::complete_chip_rebuild(source_dir, "chen", &start_date, &end_date)?;
    if let Some(progress_cb) = progress_cb {
        progress_cb(DownloadProgress {
            phase: "done".to_string(),
            finished: total_stocks,
            total: total_stocks,
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
    use crate::data::cyq_chen_data::compute::write_cyq_chen_maintenance_batches_from_channel;
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
    fn feature_database_upgrade_resume_and_repair_match_rebuild() {
        let incremental_dir = unique_temp_source_dir();
        let full_dir = unique_temp_source_dir();
        let dates = (0..150)
            .map(|day| {
                (chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap() + chrono::Duration::days(day))
                    .format("%Y%m%d")
                    .to_string()
            })
            .collect::<Vec<_>>();
        for directory in [&incremental_dir, &full_dir] {
            prepare_source_db(directory);
            fs::write(
                crate::data::trade_calendar_path(directory.to_str().unwrap()),
                format!("cal_date\n{}\n", dates.join("\n")),
            )
            .unwrap();
            let conn =
                Connection::open(crate::data::source_db_path(directory.to_str().unwrap())).unwrap();
            conn.execute("DELETE FROM stock_data", []).unwrap();
            for (day, date) in dates.iter().enumerate() {
                let price = if day < 125 {
                    12.0
                } else if day < 140 {
                    8.0
                } else {
                    15.0
                };
                insert_stock_row(
                    &conn,
                    "000001.SZ",
                    date,
                    price,
                    price,
                    price,
                    price,
                    if day == 130 { 0.0 } else { 10.0 },
                );
            }
        }
        let config = ChenChipConfig {
            warmup_days: 120,
            bucket_pct: 0.3,
        };
        let incremental = incremental_dir.to_str().unwrap();
        let full = full_dir.to_str().unwrap();
        rebuild_cyq_chen_all(incremental, config, Some(&dates[120]), Some(&dates[132])).unwrap();
        maintain_cyq_chen_incremental_if_db_exists(incremental, false, None).unwrap();
        rebuild_cyq_chen_all(full, config, Some(&dates[120]), None).unwrap();
        let features = |source: &str| {
            let conn = Connection::open(crate::data::cyq_chen_db_path(source)).unwrap();
            let mut stmt = conn.prepare("SELECT trade_date, trap_coef, real_loss20, feature_days, unknown_trapped FROM cyq_chen_snapshot ORDER BY trade_date").unwrap();
            stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<f64>>(1)?,
                    row.get::<_, Option<f64>>(2)?,
                    row.get::<_, Option<i32>>(3)?,
                    row.get::<_, Option<f64>>(4)?,
                ))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
        };
        let expected = features(full);
        assert_eq!(features(incremental), expected);
        assert_eq!(
            snapshot_rows_for_compare(incremental),
            snapshot_rows_for_compare(full)
        );
        let db = crate::data::cyq_chen_db_path(incremental);
        let conn = Connection::open(&db).unwrap();
        conn.execute("UPDATE cyq_chen_checkpoint SET feature_state = NULL", [])
            .unwrap();
        drop(conn);
        maintain_cyq_chen_incremental_if_db_exists(incremental, false, None).unwrap();
        assert_eq!(features(incremental), expected);
        let conn = Connection::open(&db).unwrap();
        conn.execute(
            "DELETE FROM cyq_chen_meta WHERE key = 'feature_version'",
            [],
        )
        .unwrap();
        drop(conn);
        maintain_cyq_chen_incremental_if_db_exists(incremental, false, None).unwrap();
        assert_eq!(features(incremental), expected);
        let conn = Connection::open(&db).unwrap();
        conn.execute(
            "UPDATE cyq_chen_meta SET value = '1' WHERE key = 'feature_version'",
            [],
        )
        .unwrap();
        conn.execute(
            "UPDATE cyq_chen_snapshot SET trap_coef = 999, feature_version = 1",
            [],
        )
        .unwrap();
        drop(conn);
        maintain_cyq_chen_incremental_if_db_exists(incremental, false, None).unwrap();
        assert_eq!(features(incremental), expected);
        let conn = Connection::open(crate::data::source_db_path(incremental)).unwrap();
        conn.execute(
            "UPDATE stock_data SET open=9, high=9, low=9, close=9 WHERE trade_date = ?",
            [&dates[138]],
        )
        .unwrap();
        drop(conn);
        repair_cyq_chen_stocks_if_db_exists(incremental, &["000001.SZ".to_string()], false, None)
            .unwrap();
        let repaired = features(incremental);
        assert_ne!(repaired, expected);
        rebuild_cyq_chen_all(incremental, config, Some(&dates[120]), None).unwrap();
        assert_eq!(features(incremental), repaired);
        fs::remove_dir_all(incremental_dir).unwrap();
        fs::remove_dir_all(full_dir).unwrap();
    }

    #[test]
    fn incremental_repairs_mixed_checkpoint_corruption_before_resuming() {
        let source_dir = unique_temp_source_dir();
        prepare_source_db(&source_dir);
        insert_paused_stock_resume_row(&source_dir);
        let source = source_dir.to_str().unwrap();
        let config = ChenChipConfig {
            warmup_days: 1,
            bucket_pct: 5.0,
        };
        rebuild_cyq_chen_all(source, config, None, None).unwrap();
        let expected_snapshots = snapshot_rows_for_compare(source);
        let expected_bins = bin_rows_for_compare(source);
        rebuild_cyq_chen_all(source, config, None, Some("20260403")).unwrap();
        let conn = Connection::open(cyq_chen_db_path(source)).unwrap();
        let json: String = conn
            .query_row(
                "SELECT bins FROM cyq_chen_checkpoint WHERE ts_code = '000001.SZ'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let mut bins: serde_json::Value = serde_json::from_str(&json).unwrap();
        bins[0].as_object_mut().unwrap().remove("price").unwrap();
        let damaged = serde_json::to_string(&bins).unwrap();
        assert!(
            serde_json::from_str::<Vec<crate::data::cyq_chen::ChenChipBin>>(&damaged)
                .unwrap_err()
                .to_string()
                .contains("missing field `price`")
        );
        conn.execute(
            "UPDATE cyq_chen_checkpoint SET bins = ? WHERE ts_code = '000001.SZ'",
            [damaged],
        )
        .unwrap();
        let feature_json: String = conn
            .query_row(
                "SELECT feature_state FROM cyq_chen_checkpoint WHERE ts_code = '000002.SZ'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let mut state: serde_json::Value = serde_json::from_str(&feature_json).unwrap();
        state["buckets"][0]["initial"] = serde_json::json!(999.0);
        conn.execute(
            "UPDATE cyq_chen_checkpoint SET feature_state = ? WHERE ts_code = '000002.SZ'",
            [serde_json::to_string(&state).unwrap()],
        )
        .unwrap();
        drop(conn);

        maintain_cyq_chen_incremental_if_db_exists(source, false, None).unwrap();
        assert_eq!(snapshot_rows_for_compare(source), expected_snapshots);
        assert_eq!(bin_rows_for_compare(source), expected_bins);
        let conn = Connection::open(cyq_chen_db_path(source)).unwrap();
        let json: String = conn
            .query_row(
                "SELECT bins FROM cyq_chen_checkpoint WHERE ts_code = '000001.SZ'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            !serde_json::from_str::<Vec<crate::data::cyq_chen::ChenChipBin>>(&json)
                .unwrap()
                .is_empty()
        );
        drop(conn);
        // 没有新行情不读取长 JSON；坏状态等下次实际使用时再重放。
        let conn = Connection::open(cyq_chen_db_path(source)).unwrap();
        conn.execute(
            "UPDATE cyq_chen_checkpoint SET feature_state = '{' WHERE ts_code = '000001.SZ'",
            [],
        )
        .unwrap();
        drop(conn);
        maintain_cyq_chen_incremental_if_db_exists(source, false, None).unwrap();
        assert_eq!(snapshot_rows_for_compare(source), expected_snapshots);
        assert_eq!(bin_rows_for_compare(source), expected_bins);
        let conn = Connection::open(cyq_chen_db_path(source)).unwrap();
        let stored: String = conn
            .query_row(
                "SELECT feature_state FROM cyq_chen_checkpoint WHERE ts_code = '000001.SZ'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored, "{");
        drop(conn);
        let conn = Connection::open(crate::data::source_db_path(source)).unwrap();
        insert_stock_row(&conn, "000001.SZ", "20260409", 11.6, 12.0, 11.5, 11.8, 4.0);
        drop(conn);
        let calendar = crate::data::trade_calendar_path(source);
        fs::write(
            &calendar,
            format!("{}20260409\n", fs::read_to_string(&calendar).unwrap()),
        )
        .unwrap();
        maintain_cyq_chen_incremental_if_db_exists(source, false, None).unwrap();
        let resumed = snapshot_rows_for_compare(source);
        let resumed_bins = bin_rows_for_compare(source);
        rebuild_cyq_chen_all(source, config, None, None).unwrap();
        assert_eq!(snapshot_rows_for_compare(source), resumed);
        assert_eq!(bin_rows_for_compare(source), resumed_bins);
        fs::remove_dir_all(source_dir).unwrap();
    }

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
    fn rebuild_starts_after_last_missing_turnover_with_full_warmup() {
        let source_dir = unique_temp_source_dir();
        prepare_source_db(&source_dir);
        let source_path = source_dir.to_str().unwrap();
        let source_conn = Connection::open(crate::data::source_db_path(source_path)).unwrap();
        source_conn
            .execute(
                "UPDATE stock_data SET tor = NULL WHERE ts_code = '000001.SZ' AND trade_date = '20260402'",
                [],
            )
            .unwrap();
        drop(source_conn);

        let summary = rebuild_cyq_chen_all(
            source_path,
            ChenChipConfig {
                warmup_days: 2,
                bucket_pct: 5.0,
            },
            None,
            None,
        )
        .unwrap();
        assert_eq!(summary.snapshot_rows, 1);
        let snapshots = snapshot_rows_for_compare(source_path);
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].0, "000001.SZ");
        assert_eq!(snapshots[0].1, "20260408");
        fs::remove_dir_all(source_dir).unwrap();
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

        // 已知行情修订由下载修复清单触发整股重放，不靠日常增量扫描全库。
        insert_paused_stock_resume_row(&source_dir);
        repair_cyq_chen_stocks_if_db_exists(source_path, &["000002.SZ".into()], false, None)
            .unwrap();
        maintain_cyq_chen_incremental_if_db_exists(source_path, false, None).unwrap();
        let resumed = snapshot_rows_for_compare(source_path);
        let resumed_bins = bin_rows_for_compare(source_path);
        rebuild_cyq_chen_all(source_path, config, None, None).unwrap();
        assert_eq!(snapshot_rows_for_compare(source_path), resumed);
        assert_eq!(bin_rows_for_compare(source_path), resumed_bins);

        fs::remove_dir_all(source_dir).expect("cleanup temp dir");
    }

    #[test]
    fn rebuild_without_trade_calendar_preserves_existing_results() {
        let source_dir = unique_temp_source_dir();
        prepare_source_db(&source_dir);
        let source = source_dir.to_str().unwrap();
        let config = ChenChipConfig {
            warmup_days: 1,
            bucket_pct: 5.0,
        };
        rebuild_cyq_chen_all(source, config, None, None).unwrap();
        let before = snapshot_rows_for_compare(source);
        let bins_before = bin_rows_for_compare(source);
        let meta_before = meta_rows_for_compare(source);
        assert!(
            rebuild_cyq_chen_all(
                source,
                ChenChipConfig {
                    warmup_days: 100,
                    ..config
                },
                None,
                None,
            )
            .unwrap_err()
            .contains("旧结果保持不变")
        );
        fs::write(crate::data::trade_calendar_path(source), "cal_date\n").unwrap();
        assert!(
            rebuild_cyq_chen_all(source, config, None, None)
                .unwrap_err()
                .contains("旧结果保持不变")
        );
        assert_eq!(snapshot_rows_for_compare(source), before);
        assert_eq!(bin_rows_for_compare(source), bins_before);
        assert_eq!(meta_rows_for_compare(source), meta_before);
        fs::remove_dir_all(source_dir).unwrap();
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
        let checkpoint_rows = |source: &str| {
            let conn = Connection::open(cyq_chen_db_path(source)).unwrap();
            let mut stmt = conn.prepare("SELECT ts_code, trade_date, bins, feature_state FROM cyq_chen_checkpoint ORDER BY ts_code").unwrap();
            stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
        };
        let checkpoint_before = checkpoint_rows(source_path);
        let reader = super::DataReader::new(source_path).unwrap();
        let row = reader
            .load_one("000001.SZ", "qfq", "20260401", "20260408")
            .unwrap();
        let mut state = crate::data::cyq_chen::ChenChipFeatureState::default();
        let chip = crate::data::cyq_chen::ChipChangeConfig {
            version: 1,
            strategy: Vec::new(),
        }
        .compile()
        .unwrap();
        let snapshots: Vec<_> =
            crate::data::cyq_chen::compute_chen_chip_snapshots_with_compiled_config_with_features(
                &row, "20260402", &chip, config, &mut state,
            )
            .unwrap()
            .into_iter()
            .filter(|snapshot| snapshot.trade_date.as_deref().unwrap() >= "20260407")
            .collect();
        let codes = vec!["000001.SZ".to_string()];
        for repair in [None, Some(codes.as_slice())] {
            for failure in ["abort", "disconnect", "invalid_state"] {
                let (tx, rx) = sync_channel(3);
                tx.send(CyqChenWriteMessage::Batch(
                    crate::data::cyq_chen_data::CyqChenWriteBatch {
                        stocks: vec![crate::data::cyq_chen_data::ComputedCyqChenStock {
                            ts_code: "000001.SZ".into(),
                            snapshots: snapshots.clone(),
                            replace_history: true,
                            feature_state: Some(state.clone()),
                        }],
                    },
                ))
                .unwrap();
                match failure {
                    "abort" => tx
                        .send(CyqChenWriteMessage::Abort("test interrupt".into()))
                        .unwrap(),
                    "invalid_state" => {
                        let mut damaged = snapshots.clone();
                        damaged.last_mut().unwrap().bins[0].main_chip += 1.0;
                        tx.send(CyqChenWriteMessage::Batch(
                            crate::data::cyq_chen_data::CyqChenWriteBatch {
                                stocks: vec![crate::data::cyq_chen_data::ComputedCyqChenStock {
                                    ts_code: "000001.SZ".into(),
                                    snapshots: damaged,
                                    replace_history: false,
                                    feature_state: Some(state.clone()),
                                }],
                            },
                        ))
                        .unwrap();
                        tx.send(CyqChenWriteMessage::Finish).unwrap();
                    }
                    _ => {}
                }
                drop(tx);
                let error = write_cyq_chen_maintenance_batches_from_channel(
                    Connection::open(&cyq_chen_db).unwrap(),
                    rx,
                    config,
                    "20260407",
                    "20260408",
                    "changed-strategy-hash".into(),
                    repair,
                )
                .expect_err("incomplete or invalid write must roll back");
                if failure == "invalid_state" {
                    assert!(error.contains("拒绝写入无效新筹码检查点"), "{error}");
                } else {
                    assert!(error.contains("结果库已回滚"), "{error}");
                }
                assert_eq!(snapshot_rows_for_compare(source_path), snapshots_before);
                assert_eq!(bin_rows_for_compare(source_path), bins_before);
                assert_eq!(meta_rows_for_compare(source_path), meta_before);
                assert_eq!(checkpoint_rows(source_path), checkpoint_before);
            }
        }
        // 回滚之后重新执行正常维护，结果仍须与完整重放一致。
        maintain_cyq_chen_incremental_if_db_exists(source_path, false, None).unwrap();
        let resumed_snapshots = snapshot_rows_for_compare(source_path);
        let resumed_bins = bin_rows_for_compare(source_path);
        rebuild_cyq_chen_all(source_path, config, None, None).unwrap();
        assert_eq!(snapshot_rows_for_compare(source_path), resumed_snapshots);
        assert_eq!(bin_rows_for_compare(source_path), resumed_bins);
        drop(reader);
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
        let mut state = crate::data::cyq_chen::ChenChipFeatureState::default();
        let snapshots =
            crate::data::cyq_chen::compute_chen_chip_snapshots_with_compiled_config_with_features(
                &row,
                "20260403",
                &crate::data::cyq_chen::ChipChangeConfig {
                    version: 1,
                    strategy: Vec::new(),
                }
                .compile()
                .unwrap(),
                config,
                &mut state,
            )
            .unwrap();
        let snapshot = snapshots.last().unwrap();
        let count = 65;
        let stage = source_dir.join(".cyq_chen.db.resume.tmp");
        super::init_cyq_chen_db(&stage).unwrap();
        Connection::open(&stage)
            .unwrap()
            .execute_batch("CREATE TABLE cyq_chen_rebuild_done(ts_code VARCHAR PRIMARY KEY)")
            .unwrap();
        let (tx, rx) = sync_channel(count + 1);
        for index in 0..count {
            tx.send(CyqChenWriteMessage::Batch(
                crate::data::cyq_chen_data::CyqChenWriteBatch {
                    stocks: vec![crate::data::cyq_chen_data::ComputedCyqChenStock {
                        ts_code: format!("{index:06}.SZ"),
                        snapshots: vec![snapshot.clone()],
                        replace_history: false,
                        feature_state: Some(state.clone()),
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
            None,
        )
        .unwrap_err();
        assert!(error.contains("临时库不发布"));
        let conn = Connection::open(&stage).unwrap();
        let stored: usize = conn
            .query_row("SELECT count(*) FROM cyq_chen_snapshot", [], |r| r.get(0))
            .unwrap();
        assert!((32..=count).contains(&stored));
        let mut stmt = conn
            .prepare("SELECT ts_code FROM cyq_chen_rebuild_done")
            .unwrap();
        let completed = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<std::collections::HashSet<_>, _>>()
            .unwrap();
        assert_eq!(completed.len(), stored);
        drop(stmt);
        let metadata: usize = conn
            .query_row("SELECT count(*) FROM cyq_chen_meta", [], |r| r.get(0))
            .unwrap();
        assert_eq!(metadata, 0);
        assert_eq!(snapshot_rows_for_compare(source_path), before);
        drop(conn);
        let (tx, rx) = sync_channel(count);
        for index in 0..count {
            let ts_code = format!("{index:06}.SZ");
            if !completed.contains(&ts_code) {
                tx.send(CyqChenWriteMessage::Batch(
                    super::super::CyqChenWriteBatch {
                        stocks: vec![super::super::ComputedCyqChenStock {
                            ts_code,
                            snapshots: vec![snapshot.clone()],
                            replace_history: false,
                            feature_state: Some(state.clone()),
                        }],
                    },
                ))
                .unwrap();
            }
        }
        drop(tx);
        let (rows, _) = write_cyq_chen_batches_from_channel(
            stage.to_str().unwrap(),
            rx,
            config,
            "finished".into(),
            None,
        )
        .unwrap();
        assert_eq!(rows, count);
        let conn = Connection::open(&stage).unwrap();
        assert_eq!(
            conn.query_row("SELECT count(*) FROM cyq_chen_rebuild_done", [], |row| {
                row.get::<_, usize>(0)
            })
            .unwrap(),
            count
        );
        assert_eq!(
            conn.query_row(
                "SELECT count(DISTINCT ts_code) FROM cyq_chen_snapshot",
                [],
                |row| row.get::<_, usize>(0)
            )
            .unwrap(),
            count
        );
        assert_eq!(snapshot_rows_for_compare(source_path), before);
        drop(conn);
        drop(reader);
        fs::remove_dir_all(source_dir).unwrap();
    }

    #[test]
    fn full_rebuild_resumes_matching_inputs_and_restarts_changed_inputs() {
        let source_dir = unique_temp_source_dir();
        prepare_source_db(&source_dir);
        let source = source_dir.to_str().unwrap();
        let config = ChenChipConfig {
            warmup_days: 2,
            bucket_pct: 5.0,
        };
        let expected = rebuild_cyq_chen_all(source, config, None, None).unwrap();
        let snapshots = snapshot_rows_for_compare(source);
        let bins = bin_rows_for_compare(source);
        let stage = source_dir.join(".cyq_chen.db.resume.tmp");
        fs::copy(cyq_chen_db_path(source), &stage).unwrap();
        {
            let conn = Connection::open(&stage).unwrap();
            conn.execute_batch("BEGIN; DELETE FROM cyq_chen_rebuild_done WHERE ts_code = '000001.SZ'; DELETE FROM cyq_chen_checkpoint WHERE ts_code = '000001.SZ'; DELETE FROM cyq_chen_snapshot WHERE ts_code = '000001.SZ'; DELETE FROM cyq_chen_bin WHERE ts_code = '000001.SZ'; COMMIT; CHECKPOINT;").unwrap();
        }
        let resumed = std::sync::atomic::AtomicUsize::new(0);
        let stage_before = fs::read(&stage).unwrap();
        let saved = super::super::query_cyq_chen_rebuild_status(source)
            .unwrap()
            .unwrap();
        assert_eq!(saved.completed_stocks, 1);
        assert_eq!(saved.total_stocks, 2);
        assert_eq!(saved.start_date, "20260401");
        assert_eq!(saved.end_date, "20260408");
        assert_eq!(saved.warmup_days, config.warmup_days);
        assert_eq!(saved.bucket_pct, config.bucket_pct);
        assert_eq!(fs::read(&stage).unwrap(), stage_before);
        let progress = |event: lianghua_model::DownloadProgress| {
            if event.message.contains("已恢复") {
                resumed.store(event.finished, std::sync::atomic::Ordering::Relaxed);
            }
        };
        let actual = super::rebuild_cyq_chen_all_with_progress(
            source,
            ChenChipConfig {
                warmup_days: saved.warmup_days,
                bucket_pct: saved.bucket_pct,
            },
            Some(&saved.start_date),
            Some(&saved.end_date),
            Some(&progress),
        )
        .unwrap();
        assert_eq!(resumed.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(actual.snapshot_rows, expected.snapshot_rows);
        assert_eq!(actual.bin_rows, expected.bin_rows);
        assert_eq!(snapshot_rows_for_compare(source), snapshots);
        assert_eq!(bin_rows_for_compare(source), bins);
        assert!(!stage.exists());
        assert!(
            super::super::query_cyq_chen_rebuild_status(source)
                .unwrap()
                .is_none()
        );
        // 已全部提交但未发布时，只需收尾，不应重复追加。
        fs::copy(cyq_chen_db_path(source), &stage).unwrap();
        super::rebuild_cyq_chen_all_with_progress(source, config, None, None, Some(&progress))
            .unwrap();
        assert_eq!(resumed.load(std::sync::atomic::Ordering::Relaxed), 2);
        assert_eq!(snapshot_rows_for_compare(source), snapshots);
        fs::copy(cyq_chen_db_path(source), &stage).unwrap();
        let changed = ChenChipConfig {
            bucket_pct: 10.0,
            ..config
        };
        super::rebuild_cyq_chen_all_with_progress(source, changed, None, None, Some(&progress))
            .unwrap();
        assert_eq!(resumed.load(std::sync::atomic::Ordering::Relaxed), 0);
        let changed_snapshots = snapshot_rows_for_compare(source);
        let changed_bins = bin_rows_for_compare(source);
        rebuild_cyq_chen_all(source, changed, None, None).unwrap();
        assert_eq!(snapshot_rows_for_compare(source), changed_snapshots);
        assert_eq!(bin_rows_for_compare(source), changed_bins);
        fs::remove_dir_all(source_dir).unwrap();
    }

    #[test]
    fn storage_cleanup_removes_resume_artifacts_and_optionally_official_db() {
        let source_dir = unique_temp_source_dir();
        prepare_source_db(&source_dir);
        let source_path = source_dir.to_str().expect("utf8 path");
        let official = cyq_chen_db_path(source_path);
        let stage = source_dir.join(".cyq_chen.db.resume.tmp");
        fs::write(&official, "official").unwrap();
        fs::write(official.with_extension("db.wal"), "official wal").unwrap();
        fs::write(&stage, "stage").unwrap();
        fs::write(stage.with_extension("tmp.wal"), "stage wal").unwrap();
        let stage_scratch = stage.with_extension("tmp.tmp");
        fs::create_dir(&stage_scratch).unwrap();
        fs::write(stage_scratch.join("block"), "scratch").unwrap();

        super::super::remove_cyq_chen_storage(source_path, false).unwrap();
        assert!(official.exists());
        assert!(!stage.exists());
        assert!(!stage.with_extension("tmp.wal").exists());
        assert!(!stage_scratch.exists());

        fs::write(&stage, "stage").unwrap();
        fs::write(stage.with_extension("tmp.wal"), "stage wal").unwrap();
        super::super::remove_cyq_chen_storage(source_path, true).unwrap();
        assert!(!official.exists());
        assert!(!official.with_extension("db.wal").exists());
        assert!(!stage.exists());
        assert!(!stage.with_extension("tmp.wal").exists());

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
            None,
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
    fn full_rebuild_continues_after_stock_error_and_persists_it() {
        let source_dir = unique_temp_source_dir();
        prepare_source_db(&source_dir);
        let source_path = source_dir.to_str().expect("utf8 path");
        let source_conn =
            Connection::open(crate::data::source_db_path(source_path)).expect("open source db");
        insert_stock_row(
            &source_conn,
            "000002.SZ",
            "20260403",
            20.3,
            19.0,
            20.5,
            20.6,
            3.0,
        );
        drop(source_conn);

        let summary = rebuild_cyq_chen_all(
            source_path,
            ChenChipConfig {
                warmup_days: 2,
                bucket_pct: 5.0,
            },
            None,
            None,
        )
        .expect("valid stocks should still publish");
        assert!(summary.snapshot_rows > 0);
        let snapshots = snapshot_rows_for_compare(source_path);
        assert!(snapshots.iter().any(|row| row.0 == "000001.SZ"));
        assert!(!snapshots.iter().any(|row| row.0 == "000002.SZ"));
        let errors =
            crate::data::cyq_chen_data::query_cyq_chen_rebuild_errors(source_path).unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].ts_code, "000002.SZ");
        assert!(errors[0].error.contains("最高价小于最低价"));

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

        let source = Connection::open(crate::data::source_db_path(source_path)).unwrap();
        crate::data::download_state::mark_chip_repairs_required(
            &source,
            &std::collections::HashSet::from(["000001.SZ".to_string()]),
            "00000000",
        )
        .unwrap();
        drop(source);

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

        drop(conn);
        let source = crate::data::download_state::open_source_db_readonly(source_path).unwrap();
        assert!(
            crate::data::download_state::load_pending_chip_repairs_for_model(&source, "chen")
                .unwrap()
                .is_empty()
        );
        drop(source);
        let maintained_rows = snapshot_rows_for_compare(source_path);
        let maintained_bins = summary.bin_rows;
        let rebuilt = rebuild_cyq_chen_all(source_path, config, None, None).unwrap();
        assert_eq!(snapshot_rows_for_compare(source_path), maintained_rows);
        assert_eq!(rebuilt.bin_rows, maintained_bins);

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
