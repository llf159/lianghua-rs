use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread::{self, sleep},
    time::Duration,
};

use chrono::{Datelike, Local, Timelike};
use duckdb::Connection;
use rayon::{ThreadPool, ThreadPoolBuilder, prelude::*};

use crate::{
    config::AppConfig,
    crawler::concept::{ThsConceptFetchItem, ThsConceptRow, fetch_one_ths_concept_row},
    data::{
        DataReader,
        download_data::{
            append_stage_pro_bar_rows, checkpoint_stock_data, delete_one_stock_range,
            delete_trade_date_rows, ensure_indicator_columns, flush_stock_data_stage_table,
            init_stock_data_db, load_latest_close_map_before, load_latest_trade_date,
            reset_stock_data_stage_table, write_ths_concepts_csv,
        },
        load_stock_list, load_ths_concepts_list, load_trade_date_list, source_db_path,
        stock_list_path, trade_calendar_path,
    },
    download::{
        AdjType, BarFreq, DownloadSummary, DownloadTask, PreparedDownloadBatch,
        PreparedStockDownload, ProBarRow, TushareClient,
        ind_calc::{
            IndsCache, cache_ind_build, calc_increment_one_stock_inds, warmup_ind_estimate,
        },
    },
};

fn price_equal(ref_close: f64, pre_close: f64) -> bool {
    (ref_close - pre_close).abs() < 0.001
}

#[derive(Debug, Clone)]
pub struct DownloadProgress {
    pub phase: String,
    pub finished: usize,
    pub total: usize,
    pub current_label: Option<String>,
    pub message: String,
}

pub type DownloadProgressCallback = dyn Fn(DownloadProgress) + Send + Sync;

const INCREMENTAL_INDICATOR_CHUNK_SIZE: usize = 256;
const THS_CONCEPT_RETRY_DELAY_SECS: u64 = 30;
const THS_CONCEPT_RETRY_LIMIT: usize = 5;

#[derive(Debug, Clone, Copy)]
pub struct ThsConceptDownloadConfig {
    pub retry_enabled: bool,
    pub retry_times: usize,
    pub retry_interval_secs: u64,
    pub concurrent_enabled: bool,
    pub worker_threads: usize,
}

impl Default for ThsConceptDownloadConfig {
    fn default() -> Self {
        Self {
            retry_enabled: true,
            retry_times: THS_CONCEPT_RETRY_LIMIT.saturating_sub(1),
            retry_interval_secs: THS_CONCEPT_RETRY_DELAY_SECS,
            concurrent_enabled: false,
            worker_threads: 4,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ThsConceptDownloadSummary {
    pub total_items: usize,
    pub saved_rows: usize,
}

fn trade_calendar_needs_refresh(
    source_dir: &str,
    trade_calendar_end: &str,
) -> Result<bool, String> {
    let path = trade_calendar_path(source_dir);
    if !path.exists() {
        return Ok(true);
    }

    let trade_dates = load_trade_date_list(source_dir)?;
    Ok(match trade_dates.last() {
        Some(last_trade_date) => last_trade_date.as_str() < trade_calendar_end,
        None => true,
    })
}

fn stock_list_needs_refresh(source_dir: &str, effective_trade_date: &str) -> Result<bool, String> {
    let path = stock_list_path(source_dir);
    if !path.exists() {
        return Ok(true);
    }

    let rows = load_stock_list(source_dir)?;
    let latest_trade_date = rows
        .iter()
        .filter_map(|cols| cols.get(6))
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .max();

    Ok(match latest_trade_date {
        Some(trade_date) => trade_date < effective_trade_date,
        None => true,
    })
}

fn emit_progress(
    progress_cb: Option<&DownloadProgressCallback>,
    phase: &str,
    finished: usize,
    total: usize,
    current_label: Option<String>,
    message: impl Into<String>,
) {
    if let Some(cb) = progress_cb {
        cb(DownloadProgress {
            phase: phase.to_string(),
            finished,
            total,
            current_label,
            message: message.into(),
        });
    }
}

fn load_existing_ths_concept_map(
    source_dir: &str,
) -> Result<HashMap<String, ThsConceptRow>, String> {
    let mut out = HashMap::new();
    let rows = match load_ths_concepts_list(source_dir) {
        Ok(rows) => rows,
        Err(error) if error.contains("打开stock_concepts.csv失败") => return Ok(out),
        Err(error) => return Err(error),
    };

    for cols in rows {
        let Some(ts_code) = cols.first().map(|value| value.trim()) else {
            continue;
        };
        let Some(name) = cols.get(1).map(|value| value.trim()) else {
            continue;
        };
        let Some(concept) = cols.get(2).map(|value| value.trim()) else {
            continue;
        };
        if ts_code.is_empty() {
            continue;
        }

        out.insert(
            ts_code.to_string(),
            ThsConceptRow {
                ts_code: ts_code.to_string(),
                name: name.to_string(),
                concept: concept.to_string(),
            },
        );
    }

    Ok(out)
}

fn build_missing_ths_concept_items(
    source_dir: &str,
) -> Result<
    (
        Vec<ThsConceptFetchItem>,
        Vec<ThsConceptFetchItem>,
        HashMap<String, ThsConceptRow>,
    ),
    String,
> {
    let stock_list = load_stock_list(source_dir)?;
    let existing_map = load_existing_ths_concept_map(source_dir)?;
    let mut all_items = Vec::new();
    let mut missing_items = Vec::new();

    for cols in stock_list {
        let Some(ts_code) = cols.first().map(|value| value.trim()) else {
            continue;
        };
        let Some(name) = cols.get(2).map(|value| value.trim()) else {
            continue;
        };
        if ts_code.is_empty() || name.is_empty() {
            continue;
        }

        let item = ThsConceptFetchItem {
            ts_code: ts_code.to_string(),
            name: name.to_string(),
        };
        if !existing_map.contains_key(ts_code) {
            missing_items.push(item.clone());
        }
        all_items.push(item);
    }

    Ok((all_items, missing_items, existing_map))
}

fn build_ordered_ths_concept_rows(
    all_items: &[ThsConceptFetchItem],
    concept_map: &HashMap<String, ThsConceptRow>,
) -> Vec<ThsConceptRow> {
    let mut rows = Vec::with_capacity(all_items.len());

    for item in all_items {
        let Some(existing) = concept_map.get(&item.ts_code) else {
            continue;
        };
        rows.push(ThsConceptRow {
            ts_code: item.ts_code.clone(),
            name: item.name.clone(),
            concept: existing.concept.clone(),
        });
    }

    rows
}

fn download_ths_concepts_once(
    source_dir: &str,
    progress_cb: Option<&DownloadProgressCallback>,
) -> Result<ThsConceptDownloadSummary, String> {
    let (all_items, missing_items, mut concept_map) = build_missing_ths_concept_items(source_dir)?;

    emit_progress(
        progress_cb,
        "prepare_ths_concepts",
        all_items.len().saturating_sub(missing_items.len()),
        all_items.len(),
        None,
        format!(
            "同花顺概念已存在 {} 只，待补抓 {} 只。",
            all_items.len().saturating_sub(missing_items.len()),
            missing_items.len()
        ),
    );

    if all_items.is_empty() {
        emit_progress(
            progress_cb,
            "done_ths_concepts",
            0,
            0,
            None,
            "股票列表为空，跳过同花顺概念同步。",
        );
        return Ok(ThsConceptDownloadSummary::default());
    }

    if missing_items.is_empty() {
        emit_progress(
            progress_cb,
            "done_ths_concepts",
            all_items.len(),
            all_items.len(),
            None,
            "同花顺概念已完整，跳过同步。",
        );
        return Ok(ThsConceptDownloadSummary {
            total_items: all_items.len(),
            saved_rows: all_items.len(),
        });
    }

    let http = reqwest::blocking::Client::builder()
        .build()
        .map_err(|e| format!("创建同花顺概念 HTTP 客户端失败: {e}"))?;

    let mut completed = all_items.len().saturating_sub(missing_items.len());
    let total = all_items.len();

    for item in &missing_items {
        emit_progress(
            progress_cb,
            "fetch_ths_concept",
            completed,
            total,
            Some(item.ts_code.clone()),
            format!(
                "正在抓取 {}/{}: {} {}",
                completed + 1,
                total,
                item.ts_code,
                item.name
            ),
        );

        let row = fetch_one_ths_concept_row(&http, &item.ts_code, &item.name).map_err(|error| {
            emit_progress(
                progress_cb,
                "failed_ths_concept",
                completed,
                total,
                Some(item.ts_code.clone()),
                format!("{} {} 抓取失败并停止: {}", item.ts_code, item.name, error),
            );
            format!(
                "抓取中断: ts_code={}, name={}, err={}",
                item.ts_code, item.name, error
            )
        })?;

        concept_map.insert(row.ts_code.clone(), row);
        let ordered_rows = build_ordered_ths_concept_rows(&all_items, &concept_map);
        emit_progress(
            progress_cb,
            "write_ths_concepts",
            completed + 1,
            total,
            Some(item.ts_code.clone()),
            format!(
                "正在写入 stock_concepts.csv，当前共 {} 只。",
                ordered_rows.len()
            ),
        );
        write_ths_concepts_csv(source_dir, &ordered_rows)?;
        completed += 1;
        emit_progress(
            progress_cb,
            "fetch_ths_concept",
            completed,
            total,
            Some(item.ts_code.clone()),
            format!(
                "已完成 {}/{}: {} {}",
                completed, total, item.ts_code, item.name
            ),
        );
    }

    emit_progress(
        progress_cb,
        "done_ths_concepts",
        completed,
        total,
        None,
        format!("同花顺概念同步完成，共 {} 只。", completed),
    );

    Ok(ThsConceptDownloadSummary {
        total_items: total,
        saved_rows: completed,
    })
}

enum ThsConceptConcurrentMessage {
    Success {
        item: ThsConceptFetchItem,
        row: ThsConceptRow,
    },
    Failure {
        item: Option<ThsConceptFetchItem>,
        error: String,
    },
}

fn download_ths_concepts_concurrent_once(
    source_dir: &str,
    worker_threads: usize,
    progress_cb: Option<&DownloadProgressCallback>,
) -> Result<ThsConceptDownloadSummary, String> {
    let (all_items, missing_items, mut concept_map) = build_missing_ths_concept_items(source_dir)?;
    let existing_count = all_items.len().saturating_sub(missing_items.len());

    emit_progress(
        progress_cb,
        "prepare_ths_concepts",
        existing_count,
        all_items.len(),
        None,
        format!(
            "同花顺概念已存在 {} 只，待补抓 {} 只，并行线程 {}。",
            existing_count,
            missing_items.len(),
            worker_threads.max(1)
        ),
    );

    if all_items.is_empty() {
        emit_progress(
            progress_cb,
            "done_ths_concepts",
            0,
            0,
            None,
            "股票列表为空，跳过同花顺概念同步。",
        );
        return Ok(ThsConceptDownloadSummary::default());
    }

    if missing_items.is_empty() {
        emit_progress(
            progress_cb,
            "done_ths_concepts",
            all_items.len(),
            all_items.len(),
            None,
            "同花顺概念已完整，跳过同步。",
        );
        return Ok(ThsConceptDownloadSummary {
            total_items: all_items.len(),
            saved_rows: all_items.len(),
        });
    }

    let worker_count = worker_threads.max(1).min(missing_items.len().max(1));
    let total = all_items.len();
    let mut completed = existing_count;
    let queue = Arc::new(Mutex::new(VecDeque::from(missing_items.clone())));
    let stop_flag = Arc::new(AtomicBool::new(false));
    let (tx, rx) = mpsc::channel::<ThsConceptConcurrentMessage>();
    let mut handles = Vec::with_capacity(worker_count);

    for worker_idx in 0..worker_count {
        let queue = Arc::clone(&queue);
        let stop_flag = Arc::clone(&stop_flag);
        let tx = tx.clone();
        let handle = thread::Builder::new()
            .name(format!("ths-concept-worker-{worker_idx}"))
            .spawn(move || {
                let http = match reqwest::blocking::Client::builder().build() {
                    Ok(client) => client,
                    Err(error) => {
                        let _ = tx.send(ThsConceptConcurrentMessage::Failure {
                            item: None,
                            error: format!("创建同花顺概念 HTTP 客户端失败: {error}"),
                        });
                        stop_flag.store(true, Ordering::Release);
                        return;
                    }
                };

                loop {
                    if stop_flag.load(Ordering::Acquire) {
                        break;
                    }

                    let item = match queue.lock() {
                        Ok(mut pending) => pending.pop_front(),
                        Err(_) => {
                            let _ = tx.send(ThsConceptConcurrentMessage::Failure {
                                item: None,
                                error: "概念下载任务队列锁已损坏".to_string(),
                            });
                            stop_flag.store(true, Ordering::Release);
                            break;
                        }
                    };

                    let Some(item) = item else {
                        break;
                    };

                    if stop_flag.load(Ordering::Acquire) {
                        break;
                    }

                    match fetch_one_ths_concept_row(&http, &item.ts_code, &item.name) {
                        Ok(row) => {
                            if tx
                                .send(ThsConceptConcurrentMessage::Success { item, row })
                                .is_err()
                            {
                                break;
                            }
                        }
                        Err(error) => {
                            stop_flag.store(true, Ordering::Release);
                            let _ = tx.send(ThsConceptConcurrentMessage::Failure {
                                item: Some(item),
                                error,
                            });
                            break;
                        }
                    }
                }
            })
            .map_err(|error| format!("创建概念下载线程失败: {error}"))?;
        handles.push(handle);
    }
    drop(tx);

    let mut fatal_error = None;

    while completed < total {
        let message = match rx.recv() {
            Ok(message) => message,
            Err(_) => break,
        };

        match message {
            ThsConceptConcurrentMessage::Success { item, row } => {
                concept_map.insert(row.ts_code.clone(), row);
                let ordered_rows = build_ordered_ths_concept_rows(&all_items, &concept_map);
                emit_progress(
                    progress_cb,
                    "write_ths_concepts",
                    completed + 1,
                    total,
                    Some(item.ts_code.clone()),
                    format!(
                        "并行抓取完成，正在写入 stock_concepts.csv，当前共 {} 只。",
                        ordered_rows.len()
                    ),
                );
                if let Err(error) = write_ths_concepts_csv(source_dir, &ordered_rows) {
                    stop_flag.store(true, Ordering::Release);
                    fatal_error = Some(error);
                    break;
                }

                completed += 1;
                emit_progress(
                    progress_cb,
                    "fetch_ths_concept",
                    completed,
                    total,
                    Some(item.ts_code.clone()),
                    format!(
                        "并行已完成 {}/{}: {} {}",
                        completed, total, item.ts_code, item.name
                    ),
                );
            }
            ThsConceptConcurrentMessage::Failure { item, error } => {
                stop_flag.store(true, Ordering::Release);
                let current_label = item.as_ref().map(|value| value.ts_code.clone());
                let message = match item {
                    Some(item) => {
                        format!("{} {} 抓取失败并停止: {}", item.ts_code, item.name, error)
                    }
                    None => format!("概念并行任务初始化失败并停止: {error}"),
                };
                emit_progress(
                    progress_cb,
                    "failed_ths_concept",
                    completed,
                    total,
                    current_label,
                    message,
                );
                fatal_error = Some(error);
                break;
            }
        }
    }

    stop_flag.store(true, Ordering::Release);
    for handle in handles {
        handle
            .join()
            .map_err(|_| "概念下载线程异常退出".to_string())?;
    }

    if let Some(error) = fatal_error {
        return Err(error);
    }

    if completed < total {
        return Err(format!(
            "概念并行下载提前结束: 已完成 {} / {}",
            completed, total
        ));
    }

    emit_progress(
        progress_cb,
        "done_ths_concepts",
        completed,
        total,
        None,
        format!("同花顺概念同步完成，共 {} 只。", completed),
    );

    Ok(ThsConceptDownloadSummary {
        total_items: total,
        saved_rows: completed,
    })
}

pub fn download_ths_concepts(
    source_dir: &str,
    download_config: ThsConceptDownloadConfig,
    progress_cb: Option<&DownloadProgressCallback>,
) -> Result<ThsConceptDownloadSummary, String> {
    let retry_total = if download_config.retry_enabled {
        download_config.retry_times.saturating_add(1).max(1)
    } else {
        1
    };

    for attempt_idx in 0..retry_total {
        let attempt_result = if download_config.concurrent_enabled {
            download_ths_concepts_concurrent_once(
                source_dir,
                download_config.worker_threads,
                progress_cb,
            )
        } else {
            download_ths_concepts_once(source_dir, progress_cb)
        };

        match attempt_result {
            Ok(summary) => return Ok(summary),
            Err(error) => {
                let attempt = attempt_idx + 1;
                if attempt >= retry_total {
                    return Err(format!(
                        "同花顺概念同步失败，已达到最大重试次数 {}: {}",
                        retry_total, error
                    ));
                }

                emit_progress(
                    progress_cb,
                    "retry_ths_concepts",
                    0,
                    0,
                    None,
                    format!(
                        "同花顺概念{}同步第 {}/{} 次失败，{} 秒后整体重试: {}",
                        if download_config.concurrent_enabled {
                            format!("并发({}线程)", download_config.worker_threads.max(1))
                        } else {
                            "串行".to_string()
                        },
                        attempt,
                        retry_total,
                        download_config.retry_interval_secs,
                        error
                    ),
                );
                if download_config.retry_interval_secs > 0 {
                    sleep(Duration::from_secs(download_config.retry_interval_secs));
                }
            }
        }
    }

    Err("同花顺概念同步进入未知重试状态".to_string())
}

pub fn init_stock_basic_data(
    config: &AppConfig,
    progress_cb: Option<&DownloadProgressCallback>,
) -> Result<String, String> {
    // 初始化基础数据,返回当前有效交易日
    let download_config = &config.download;
    let source_dir = config.output.dir.as_str();
    let client = TushareClient::new(
        download_config.token.clone(),
        download_config.limit_calls_per_min,
    )?;

    let now = Local::now();
    let today = now.format("%Y%m%d").to_string();
    let trade_calendar_end = format!("{:04}1231", now.year());
    let current_hhmm = now.hour() * 100 + now.minute();

    // 2. 先检查是否需要刷新交易日历
    if trade_calendar_needs_refresh(source_dir, trade_calendar_end.as_str())? {
        emit_progress(
            progress_cb,
            "prepare_trade_calendar",
            0,
            1,
            Some("trade_calendar.csv".to_string()),
            "正在刷新交易日历。",
        );
        client.download_trade_calendar_csv(
            source_dir,
            download_config.start_date.as_str(),
            trade_calendar_end.as_str(),
        )?;
        emit_progress(
            progress_cb,
            "prepare_trade_calendar",
            1,
            1,
            Some("trade_calendar.csv".to_string()),
            "交易日历刷新完成。",
        );
    } else {
        emit_progress(
            progress_cb,
            "prepare_trade_calendar",
            1,
            1,
            Some("trade_calendar.csv".to_string()),
            "交易日历已覆盖到当年年末，跳过刷新。",
        );
    }

    // 3. 读取交易日历
    let trade_dates = crate::data::load_trade_date_list(source_dir)?;

    // 4. 在这里直接判断有效交易日
    let effective_trade_date = if let Some(pos) = trade_dates.iter().position(|d| d == &today) {
        if current_hhmm >= 1600 {
            trade_dates[pos].clone()
        } else if pos > 0 {
            trade_dates[pos - 1].clone()
        } else {
            return Err("交易日历中没有前一个交易日".to_string());
        }
    } else {
        match trade_dates
            .iter()
            .rev()
            .find(|d| d.as_str() < today.as_str())
        {
            Some(date) => date.clone(),
            None => return Err("交易日历中找不到小于今天的交易日".to_string()),
        }
    };

    // 5. 再检查是否需要刷新股票列表
    if stock_list_needs_refresh(source_dir, effective_trade_date.as_str())? {
        emit_progress(
            progress_cb,
            "prepare_stock_list",
            0,
            1,
            Some(effective_trade_date.clone()),
            format!("正在刷新股票列表，交易日 {}。", effective_trade_date),
        );
        client.download_stock_list_csv(source_dir, effective_trade_date.as_str())?;
        emit_progress(
            progress_cb,
            "prepare_stock_list",
            1,
            1,
            Some(effective_trade_date.clone()),
            format!("股票列表刷新完成，交易日 {}。", effective_trade_date),
        );
    } else {
        emit_progress(
            progress_cb,
            "prepare_stock_list",
            1,
            1,
            Some(effective_trade_date.clone()),
            format!(
                "股票列表已是交易日 {} 的最新版本，跳过刷新。",
                effective_trade_date
            ),
        );
    }

    Ok(effective_trade_date)
}

pub fn build_download_task<'a>(
    ts_codes: &'a [String],
    start_date: &'a str,
    end_date: &'a str,
    adj_type: AdjType,
    with_factors: bool,
) -> Vec<DownloadTask> {
    let mut tasks = Vec::with_capacity(ts_codes.len());

    for ts_code in ts_codes {
        tasks.push(DownloadTask {
            ts_code: ts_code.as_str().to_string(),
            start_date: start_date.to_string(),
            end_date: end_date.to_string(),
            freq: BarFreq::Daily,
            adj_type,
            with_factors,
        });
    }

    tasks
}

fn merge_summary(total: &mut DownloadSummary, batch: DownloadSummary) {
    total.success_count += batch.success_count;
    total.failed_count += batch.failed_count;
    total.saved_rows += batch.saved_rows;
    total.failed_items.extend(batch.failed_items);
}

fn collect_indicator_names(prepared_items: &[PreparedStockDownload]) -> Vec<String> {
    let mut names = HashSet::new();
    let mut ordered = Vec::new();

    for item in prepared_items {
        for name in item.indicators.keys() {
            if names.insert(name.clone()) {
                ordered.push(name.clone());
            }
        }
    }

    ordered
}

fn write_prepared_stock_batch(
    conn: &Connection,
    prepared_items: &[PreparedStockDownload],
) -> Result<(), String> {
    if prepared_items.is_empty() {
        return Ok(());
    }

    with_transaction(conn, |tx| {
        reset_stock_data_stage_table(tx)?;

        for item in prepared_items {
            delete_one_stock_range(
                tx,
                item.ts_code.as_str(),
                item.adj_type,
                item.start_date.as_str(),
                item.end_date.as_str(),
            )?;
            append_stage_pro_bar_rows(tx, item.adj_type, &item.rows, &item.indicators)?;
        }

        flush_stock_data_stage_table(tx)
    })
}

fn build_download_pool(threads: usize) -> Result<ThreadPool, String> {
    ThreadPoolBuilder::new()
        .num_threads(threads.max(1))
        .build()
        .map_err(|e| format!("创建下载线程池失败: {e}"))
}

struct PendingTradeDateBatch {
    trade_date: String,
    rows: Vec<ProBarRow>,
    indicators: HashMap<String, Vec<Option<f64>>>,
}

fn build_trade_date_write_batches(
    prepared_items: &[PreparedStockDownload],
) -> Result<Vec<PendingTradeDateBatch>, String> {
    let mut rows_map = HashMap::<String, Vec<ProBarRow>>::new();
    let mut indicator_map = HashMap::<String, HashMap<String, Vec<Option<f64>>>>::new();

    for item in prepared_items {
        for series in item.indicators.values() {
            if series.len() != item.rows.len() {
                return Err(format!(
                    "按交易日重组写库批次失败: ts_code={} 指标长度与行数不一致",
                    item.ts_code
                ));
            }
        }

        for (row_idx, row) in item.rows.iter().enumerate() {
            rows_map
                .entry(row.trade_date.clone())
                .or_default()
                .push(row.clone());

            let trade_date_indicators = indicator_map.entry(row.trade_date.clone()).or_default();
            for (name, series) in &item.indicators {
                trade_date_indicators
                    .entry(name.clone())
                    .or_default()
                    .push(series[row_idx]);
            }
        }
    }

    let mut trade_dates = rows_map.keys().cloned().collect::<Vec<_>>();
    trade_dates.sort();

    let mut out = Vec::with_capacity(trade_dates.len());
    for trade_date in trade_dates {
        let rows = rows_map.remove(&trade_date).unwrap_or_default();
        let mut indicators = indicator_map.remove(&trade_date).unwrap_or_default();

        let mut order = rows
            .iter()
            .enumerate()
            .map(|(idx, row)| (idx, row.ts_code.clone()))
            .collect::<Vec<_>>();
        order.sort_by(|a, b| a.1.cmp(&b.1));

        let reordered_rows = order
            .iter()
            .map(|(idx, _)| rows[*idx].clone())
            .collect::<Vec<_>>();

        for series in indicators.values_mut() {
            let reordered_series = order
                .iter()
                .map(|(idx, _)| series[*idx])
                .collect::<Vec<_>>();
            *series = reordered_series;
        }

        out.push(PendingTradeDateBatch {
            trade_date,
            rows: reordered_rows,
            indicators,
        });
    }

    Ok(out)
}

fn with_transaction<T, F>(conn: &Connection, action: F) -> Result<T, String>
where
    F: FnOnce(&Connection) -> Result<T, String>,
{
    conn.execute_batch("BEGIN TRANSACTION")
        .map_err(|e| format!("开启事务失败: {e}"))?;

    match action(conn) {
        Ok(value) => {
            conn.execute_batch("COMMIT")
                .map_err(|e| format!("提交事务失败: {e}"))?;
            Ok(value)
        }
        Err(err) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(err)
        }
    }
}

fn keep_failed_tasks(
    pending_tasks: Vec<DownloadTask>,
    failed_items: &[(String, String)],
) -> Vec<DownloadTask> {
    let failed_ts_codes: HashSet<&str> = failed_items
        .iter()
        .map(|(ts_code, _)| ts_code.as_str())
        .collect();

    pending_tasks
        .into_iter()
        .filter(|task| failed_ts_codes.contains(task.ts_code.as_str()))
        .collect()
}

fn redownload_failed_stocks(
    client: &TushareClient,
    source_dir: &str,
    failed_items: &[(String, String)],
    start_date: &str,
    trade_date: &str,
    adj_type: AdjType,
    with_factors: bool,
    pool: &ThreadPool,
) -> Result<PreparedDownloadBatch, String> {
    if failed_items.is_empty() {
        return Ok(PreparedDownloadBatch::default());
    }

    let failed_ts_codes = failed_items
        .iter()
        .map(|(ts_code, _)| ts_code.clone())
        .collect::<Vec<_>>();
    let tasks = build_download_task(
        &failed_ts_codes,
        start_date,
        trade_date,
        adj_type,
        with_factors,
    );
    let batch = pool.install(|| client.prepare_stock_downloads(source_dir, &tasks));
    if batch.failed_items.is_empty() {
        return Ok(batch);
    }

    let failed_preview = batch
        .failed_items
        .iter()
        .take(3)
        .map(|(ts_code, error)| format!("{ts_code}: {error}"))
        .collect::<Vec<_>>()
        .join("；");
    Err(format!(
        "单股补救重下仍有 {} 只股票失败: {}",
        batch.failed_items.len(),
        failed_preview
    ))
}

fn retry_failed_downloads(
    client: &TushareClient,
    source_dir: &str,
    tasks: Vec<DownloadTask>,
    retry_times: usize,
    pool: &ThreadPool,
    progress_cb: Option<&DownloadProgressCallback>,
) -> PreparedDownloadBatch {
    let mut merged_batch = PreparedDownloadBatch::default();
    let mut pending_tasks = tasks;
    let retry_total = retry_times.max(1);

    for attempt_idx in 0..retry_times {
        if pending_tasks.is_empty() {
            break;
        }

        let retry_batch =
            pool.install(|| client.prepare_stock_downloads(source_dir, &pending_tasks));
        let retry_summary = retry_batch.summary();

        merged_batch
            .prepared_items
            .extend(retry_batch.prepared_items.into_iter());

        if retry_summary.failed_items.is_empty() {
            emit_progress(
                progress_cb,
                "retry_failed",
                attempt_idx + 1,
                retry_total,
                Some(format!("第 {} 轮重试", attempt_idx + 1)),
                format!(
                    "失败补救完成，第 {} 轮重试后已没有剩余失败项。",
                    attempt_idx + 1
                ),
            );
            return merged_batch;
        }

        pending_tasks = keep_failed_tasks(pending_tasks, &retry_summary.failed_items);
        emit_progress(
            progress_cb,
            "retry_failed",
            attempt_idx + 1,
            retry_total,
            Some(format!("第 {} 轮重试", attempt_idx + 1)),
            format!(
                "失败补救进行中，第 {} 轮后仍有 {} 只股票待重试。",
                attempt_idx + 1,
                pending_tasks.len()
            ),
        );

        if attempt_idx + 1 == retry_times {
            merged_batch.failed_items = retry_summary.failed_items;
        }
    }

    merged_batch
}

fn download_first_all_market(
    config: &AppConfig,
    progress_cb: Option<&DownloadProgressCallback>,
) -> Result<DownloadSummary, String> {
    let effective_trade_date = init_stock_basic_data(config, progress_cb)?;

    let adj_type = match config.data.adj_type.trim().to_ascii_lowercase().as_str() {
        "qfq" => Ok(AdjType::Qfq),
        "hfq" => Ok(AdjType::Hfq),
        "raw" => Ok(AdjType::Raw),
        other => Err(format!("不支持的复权类型: {other}")),
    }?;

    let download_config = &config.download;
    let source_dir = config.output.dir.as_str();
    let start_date = download_config.start_date.as_str();
    let end_date = if download_config.end_date.eq_ignore_ascii_case("today") {
        effective_trade_date.as_str()
    } else {
        download_config.end_date.as_str()
    };
    let with_factors = download_config.include_turnover;

    let client = TushareClient::new(
        download_config.token.clone(),
        download_config.limit_calls_per_min,
    )?;
    let pool = build_download_pool(download_config.threads)?;
    let db_path = source_db_path(source_dir);
    let db_path_str = db_path
        .to_str()
        .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?;
    init_stock_data_db(db_path_str)?;
    let conn = Connection::open(db_path_str).map_err(|e| format!("数据库连接错误:{e}"))?;
    let mut indicator_columns_ready = false;

    let ts_codes: Vec<String> = {
        let rows = crate::data::load_stock_list(source_dir)?;
        rows.into_iter()
            .filter_map(|row| row.first().cloned())
            .collect()
    };

    download_selected_stocks_with_context(
        source_dir,
        &effective_trade_date,
        &client,
        &pool,
        &conn,
        &mut indicator_columns_ready,
        &ts_codes,
        start_date,
        end_date,
        adj_type,
        with_factors,
        download_config.retry_times,
        "首次全量下载开始",
        "首次全量下载结束",
        progress_cb,
    )
}

#[allow(clippy::too_many_arguments)]
fn download_selected_stocks_with_context(
    source_dir: &str,
    effective_trade_date: &str,
    client: &TushareClient,
    pool: &ThreadPool,
    conn: &Connection,
    indicator_columns_ready: &mut bool,
    ts_codes: &[String],
    start_date: &str,
    end_date: &str,
    adj_type: AdjType,
    with_factors: bool,
    retry_times: usize,
    start_message: &str,
    done_message: &str,
    progress_cb: Option<&DownloadProgressCallback>,
) -> Result<DownloadSummary, String> {
    if ts_codes.is_empty() {
        emit_progress(
            progress_cb,
            "done",
            0,
            0,
            Some(effective_trade_date.to_string()),
            "没有需要处理的股票。",
        );
        return Ok(DownloadSummary::default());
    }

    let mut total = DownloadSummary::default();
    let tasks = build_download_task(ts_codes, start_date, end_date, adj_type, with_factors);
    let total_tasks = tasks.len();
    let mut processed_tasks = 0usize;
    emit_progress(
        progress_cb,
        "download_bars",
        0,
        total_tasks,
        None,
        format!("{start_message}，共 {total_tasks} 只股票待处理。"),
    );

    for (batch_idx, batch) in tasks.chunks(pool.current_num_threads().max(1)).enumerate() {
        let prepared_batch = pool.install(|| client.prepare_stock_downloads(source_dir, batch));
        let batch_summary = prepared_batch.summary();
        if !*indicator_columns_ready && !prepared_batch.prepared_items.is_empty() {
            let indicator_names = collect_indicator_names(&prepared_batch.prepared_items);
            ensure_indicator_columns(conn, &indicator_names)?;
            *indicator_columns_ready = true;
        }
        emit_progress(
            progress_cb,
            "write_db",
            processed_tasks,
            total_tasks,
            Some(format!("第 {} 批", batch_idx + 1)),
            format!(
                "第 {} 批下载完成，正在写入数据库，本批 {} 只。",
                batch_idx + 1,
                batch.len()
            ),
        );
        write_prepared_stock_batch(conn, &prepared_batch.prepared_items)?;
        merge_summary(&mut total, batch_summary);
        processed_tasks += batch.len();
        emit_progress(
            progress_cb,
            "download_bars",
            processed_tasks,
            total_tasks,
            Some(format!("第 {} 批", batch_idx + 1)),
            format!("已处理 {} / {} 只股票。", processed_tasks, total_tasks),
        );
    }

    if !total.failed_items.is_empty() && retry_times > 0 {
        let failed_tasks = keep_failed_tasks(tasks, &total.failed_items);
        let retry_task_count = failed_tasks.len();
        emit_progress(
            progress_cb,
            "retry_failed",
            0,
            retry_times.max(1),
            Some(format!("待重试 {} 只", retry_task_count)),
            format!("共有 {} 只股票失败，准备进入重试阶段。", retry_task_count),
        );
        let retry_batch = retry_failed_downloads(
            client,
            source_dir,
            failed_tasks,
            retry_times,
            pool,
            progress_cb,
        );
        let retry_summary = retry_batch.summary();
        if !*indicator_columns_ready && !retry_batch.prepared_items.is_empty() {
            let indicator_names = collect_indicator_names(&retry_batch.prepared_items);
            ensure_indicator_columns(conn, &indicator_names)?;
        }
        emit_progress(
            progress_cb,
            "write_db",
            processed_tasks,
            total_tasks,
            Some("重试结果".to_string()),
            format!(
                "重试阶段完成，正在写入 {} 只重试成功股票。",
                retry_batch.prepared_items.len()
            ),
        );
        write_prepared_stock_batch(conn, &retry_batch.prepared_items)?;

        total.success_count += retry_summary.success_count;
        total.saved_rows += retry_summary.saved_rows;
        total.failed_count = retry_summary.failed_count;
        total.failed_items = retry_summary.failed_items;
    }

    checkpoint_stock_data(conn)?;

    emit_progress(
        progress_cb,
        "done",
        total_tasks,
        total_tasks,
        Some(effective_trade_date.to_string()),
        format!(
            "{done_message}，成功 {} 只，失败 {} 只。",
            total.success_count, total.failed_count
        ),
    );

    Ok(total)
}

pub fn download_selected_stocks(
    config: &AppConfig,
    ts_codes: &[String],
    progress_cb: Option<&DownloadProgressCallback>,
) -> Result<DownloadSummary, String> {
    let effective_trade_date = init_stock_basic_data(config, progress_cb)?;

    let adj_type = match config.data.adj_type.trim().to_ascii_lowercase().as_str() {
        "qfq" => Ok(AdjType::Qfq),
        "hfq" => Ok(AdjType::Hfq),
        "raw" => Ok(AdjType::Raw),
        other => Err(format!("不支持的复权类型: {other}")),
    }?;

    let download_config = &config.download;
    let source_dir = config.output.dir.as_str();
    let start_date = download_config.start_date.as_str();
    let end_date = if download_config.end_date.eq_ignore_ascii_case("today") {
        effective_trade_date.as_str()
    } else {
        download_config.end_date.as_str()
    };
    let with_factors = download_config.include_turnover;
    let client = TushareClient::new(
        download_config.token.clone(),
        download_config.limit_calls_per_min,
    )?;
    let pool = build_download_pool(download_config.threads)?;
    let db_path = source_db_path(source_dir);
    let db_path_str = db_path
        .to_str()
        .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?;
    init_stock_data_db(db_path_str)?;
    let conn = Connection::open(db_path_str).map_err(|e| format!("数据库连接错误:{e}"))?;
    let mut indicator_columns_ready = false;

    download_selected_stocks_with_context(
        source_dir,
        &effective_trade_date,
        &client,
        &pool,
        &conn,
        &mut indicator_columns_ready,
        ts_codes,
        start_date,
        end_date,
        adj_type,
        with_factors,
        download_config.retry_times,
        "缺失股票补全开始",
        "缺失股票补全结束",
        progress_cb,
    )
}

// 增量部分

fn calc_passed_prepared_items(
    pool: &ThreadPool,
    source_dir: &str,
    adj_type: AdjType,
    inds_cache: &[IndsCache],
    warmup_need: usize,
    history_end_dates: &HashMap<String, String>,
    passed_rows_by_stock: &HashMap<String, Vec<ProBarRow>>,
) -> Result<Vec<PreparedStockDownload>, String> {
    if passed_rows_by_stock.is_empty() {
        return Ok(Vec::new());
    }

    let mut stock_rows = passed_rows_by_stock
        .iter()
        .map(|(ts_code, rows)| (ts_code.clone(), rows.clone()))
        .collect::<Vec<_>>();
    stock_rows.sort_by(|a, b| a.0.cmp(&b.0));

    let chunk_results = pool.install(|| {
        stock_rows
            .par_chunks(INCREMENTAL_INDICATOR_CHUNK_SIZE)
            .map(|chunk| -> Result<Vec<PreparedStockDownload>, String> {
                let dr = DataReader::new(source_dir)?;
                let mut chunk_out = Vec::with_capacity(chunk.len());

                for (ts_code, rows) in chunk {
                    let history_end_date = history_end_dates.get(ts_code).map(String::as_str);
                    let indicators = calc_increment_one_stock_inds(
                        &dr,
                        inds_cache,
                        warmup_need,
                        ts_code.as_str(),
                        "qfq",
                        history_end_date,
                        rows.as_slice(),
                    )?;
                    let start_date = rows
                        .first()
                        .map(|row| row.trade_date.clone())
                        .ok_or_else(|| format!("缺少通过校验的增量行: ts_code={ts_code}"))?;
                    let end_date = rows
                        .last()
                        .map(|row| row.trade_date.clone())
                        .ok_or_else(|| format!("缺少通过校验的增量行: ts_code={ts_code}"))?;

                    chunk_out.push(PreparedStockDownload {
                        ts_code: ts_code.clone(),
                        start_date,
                        end_date,
                        adj_type,
                        rows: rows.clone(),
                        indicators,
                    });
                }

                Ok(chunk_out)
            })
            .collect::<Vec<_>>()
    });

    let mut out = Vec::with_capacity(stock_rows.len());
    for chunk_result in chunk_results {
        out.extend(chunk_result?);
    }
    out.sort_by(|a, b| a.ts_code.cmp(&b.ts_code));

    Ok(out)
}

pub fn download_pending_all_market(
    config: &AppConfig,
    progress_cb: Option<&DownloadProgressCallback>,
) -> Result<DownloadSummary, String> {
    let effective_trade_date = init_stock_basic_data(config, progress_cb)?;
    let download_config = &config.download;
    let source_dir = config.output.dir.as_str();
    let start_date = download_config.start_date.as_str();
    let with_factors = download_config.include_turnover;

    let adj_type = match config.data.adj_type.trim().to_ascii_lowercase().as_str() {
        "qfq" => AdjType::Qfq,
        "hfq" | "raw" => {
            return Err("当前增量pre_close校验只支持 qfq".to_string());
        }
        other => {
            return Err(format!("不支持的复权类型: {other}"));
        }
    };

    let last_saved_trade_date = load_latest_trade_date(source_dir, adj_type)?
        .ok_or_else(|| "数据库里还没有可用于增量的历史数据，请先做首次下载".to_string())?;

    if last_saved_trade_date >= effective_trade_date {
        return Ok(DownloadSummary::default());
    }

    let trade_dates = load_trade_date_list(source_dir)?;
    let pending_trade_dates: Vec<String> = trade_dates
        .iter()
        .filter(|d| d.as_str() > last_saved_trade_date.as_str())
        .filter(|d| d.as_str() <= effective_trade_date.as_str())
        .cloned()
        .collect();

    if pending_trade_dates.is_empty() {
        return Ok(DownloadSummary::default());
    }
    let total_trade_dates = pending_trade_dates.len();
    emit_progress(
        progress_cb,
        "download_pending_trade_dates",
        0,
        total_trade_dates,
        None,
        format!("增量更新开始，共 {} 个交易日待处理。", total_trade_dates),
    );

    let client = TushareClient::new(
        download_config.token.clone(),
        download_config.limit_calls_per_min,
    )?;
    let pool = build_download_pool(download_config.threads)?;
    let inds_cache = cache_ind_build(source_dir)?;
    let warmup_need = warmup_ind_estimate(source_dir)?;
    let indicator_names = inds_cache
        .iter()
        .map(|ind| ind.name.clone())
        .collect::<Vec<_>>();

    emit_progress(
        progress_cb,
        "download_pending_trade_dates",
        0,
        total_trade_dates,
        Some("加载历史收盘基线".to_string()),
        format!(
            "正在为 {} 个待处理交易日加载增量校验基线。",
            total_trade_dates
        ),
    );
    let latest_map_before =
        load_latest_close_map_before(source_dir, "qfq", pending_trade_dates[0].as_str())?;
    let history_end_dates = latest_map_before
        .iter()
        .map(|(ts_code, latest)| (ts_code.clone(), latest.trade_date.clone()))
        .collect::<HashMap<_, _>>();
    let mut total = DownloadSummary::default();
    let mut fetched_trade_dates = Vec::with_capacity(total_trade_dates);

    for (trade_date_idx, trade_date) in pending_trade_dates.iter().enumerate() {
        emit_progress(
            progress_cb,
            "download_pending_trade_dates",
            trade_date_idx,
            total_trade_dates,
            Some(format!("{trade_date} · 拉取全市场行情")),
            format!("正在拉取交易日 {} 的全市场行情。", trade_date),
        );
        let rows = client.fetch_market_daily(trade_date.as_str(), with_factors)?;
        total.success_count += rows.len();
        emit_progress(
            progress_cb,
            "download_pending_trade_dates",
            trade_date_idx + 1,
            total_trade_dates,
            Some(trade_date.clone()),
            format!(
                "交易日 {} 全市场行情拉取完成，共 {} 条，进度 {}/{}。",
                trade_date,
                rows.len(),
                trade_date_idx + 1,
                total_trade_dates
            ),
        );
        fetched_trade_dates.push(PendingTradeDateBatch {
            trade_date: trade_date.clone(),
            rows,
            indicators: HashMap::new(),
        });
    }

    emit_progress(
        progress_cb,
        "validate_pending_trade_dates",
        0,
        total_trade_dates,
        None,
        format!(
            "全市场行情已拉取完成，开始按时间顺序统一校验 {} 个缺失交易日。",
            total_trade_dates
        ),
    );

    let mut latest_close_map = latest_map_before
        .iter()
        .map(|(ts_code, latest)| (ts_code.clone(), (latest.trade_date.clone(), latest.close)))
        .collect::<HashMap<_, _>>();
    let mut failed_ts_codes = HashSet::new();
    let mut failed_items = Vec::new();
    let mut passed_rows_by_stock: HashMap<String, Vec<ProBarRow>> = HashMap::new();

    for (trade_date_idx, pending) in fetched_trade_dates.iter().enumerate() {
        let mut passed_count = 0usize;
        let mut skipped_count = 0usize;

        for row in &pending.rows {
            if failed_ts_codes.contains(&row.ts_code) {
                skipped_count += 1;
                continue;
            }

            let validation_failed = match latest_close_map.get(&row.ts_code) {
                Some((latest_trade_date, latest_close))
                    if !price_equal(*latest_close, row.pre_close) =>
                {
                    Some(format!(
                        "trade_date={} pre_close校验失败: db_latest_date={}, db_close={}, daily_pre_close={}",
                        row.trade_date, latest_trade_date, latest_close, row.pre_close
                    ))
                }
                _ => None,
            };

            if let Some(error) = validation_failed {
                failed_ts_codes.insert(row.ts_code.clone());
                passed_rows_by_stock.remove(&row.ts_code);
                failed_items.push((row.ts_code.clone(), error));
                continue;
            }

            latest_close_map.insert(row.ts_code.clone(), (row.trade_date.clone(), row.close));
            passed_rows_by_stock
                .entry(row.ts_code.clone())
                .or_default()
                .push(row.clone());
            passed_count += 1;
        }

        emit_progress(
            progress_cb,
            "validate_pending_trade_dates",
            trade_date_idx + 1,
            total_trade_dates,
            Some(pending.trade_date.clone()),
            format!(
                "交易日 {} 校验完成，通过 {} 条，待整段补救 {} 只股票，已跳过 {} 条后续重复校验记录。",
                pending.trade_date,
                passed_count,
                failed_ts_codes.len(),
                skipped_count
            ),
        );
    }

    let mut passed_prepared_items = Vec::new();
    if !passed_rows_by_stock.is_empty() {
        emit_progress(
            progress_cb,
            "calc_incremental_indicators",
            0,
            passed_rows_by_stock.len(),
            None,
            format!(
                "统一校验完成，开始按股票整段计算 {} 只通过校验股票的增量指标。",
                passed_rows_by_stock.len()
            ),
        );
        passed_prepared_items = calc_passed_prepared_items(
            &pool,
            source_dir,
            adj_type,
            &inds_cache,
            warmup_need,
            &history_end_dates,
            &passed_rows_by_stock,
        )?;
        emit_progress(
            progress_cb,
            "calc_incremental_indicators",
            passed_prepared_items.len(),
            passed_rows_by_stock.len(),
            None,
            format!(
                "通过校验股票的整段增量指标计算完成，共 {} 只。",
                passed_prepared_items.len()
            ),
        );
    }

    let recovered_batch = if failed_items.is_empty() {
        PreparedDownloadBatch::default()
    } else {
        emit_progress(
            progress_cb,
            "recover_failed_stocks",
            0,
            failed_items.len(),
            None,
            format!(
                "共有 {} 只股票在缺失区间内出现 pre_close 断点，开始整段补救重下。",
                failed_items.len()
            ),
        );
        let recovered = redownload_failed_stocks(
            &client,
            source_dir,
            &failed_items,
            start_date,
            effective_trade_date.as_str(),
            adj_type,
            with_factors,
            &pool,
        )?;
        emit_progress(
            progress_cb,
            "recover_failed_stocks",
            recovered.prepared_items.len(),
            failed_items.len(),
            None,
            format!(
                "整段补救重下完成，成功准备 {} 只股票的数据。",
                recovered.prepared_items.len()
            ),
        );
        recovered
    };

    let passed_write_batches = build_trade_date_write_batches(&passed_prepared_items)?;
    let recovered_items = recovered_batch.prepared_items;

    if passed_write_batches.is_empty() && recovered_items.is_empty() {
        return Ok(total);
    }

    total.saved_rows = passed_write_batches
        .iter()
        .map(|batch| batch.rows.len())
        .sum::<usize>()
        + recovered_items
            .iter()
            .map(|item| item.rows.len())
            .sum::<usize>();
    let write_total = passed_write_batches.len() + recovered_items.len();

    let db_path = source_db_path(source_dir);
    let db_path_str = db_path
        .to_str()
        .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?;
    let conn = Connection::open(db_path_str).map_err(|e| format!("数据库连接错误:{e}"))?;
    emit_progress(
        progress_cb,
        "write_db",
        0,
        write_total,
        None,
        format!(
            "增量校验与补救完成，准备写入 {} 个交易日批次和 {} 只补救股票。",
            passed_write_batches.len(),
            recovered_items.len()
        ),
    );

    let mut written_steps = 0usize;
    with_transaction(&conn, |tx| {
        ensure_indicator_columns(tx, &indicator_names)?;
        reset_stock_data_stage_table(tx)?;

        for batch in &passed_write_batches {
            delete_trade_date_rows(tx, adj_type, batch.trade_date.as_str())?;
            append_stage_pro_bar_rows(tx, adj_type, &batch.rows, &batch.indicators)?;

            written_steps += 1;
            emit_progress(
                progress_cb,
                "write_db",
                written_steps,
                write_total,
                Some(batch.trade_date.clone()),
                format!(
                    "已写入 {}/{} 个批次，当前交易日 {}。",
                    written_steps, write_total, batch.trade_date
                ),
            );
        }

        for item in &recovered_items {
            delete_one_stock_range(
                tx,
                item.ts_code.as_str(),
                adj_type,
                item.start_date.as_str(),
                item.end_date.as_str(),
            )?;
            append_stage_pro_bar_rows(tx, adj_type, &item.rows, &item.indicators)?;

            written_steps += 1;
            emit_progress(
                progress_cb,
                "write_db",
                written_steps,
                write_total,
                Some(item.ts_code.clone()),
                format!(
                    "已写入 {}/{} 个批次，当前补救股票 {}。",
                    written_steps, write_total, item.ts_code
                ),
            );
        }

        flush_stock_data_stage_table(tx)
    })?;
    checkpoint_stock_data(&conn)?;

    emit_progress(
        progress_cb,
        "done",
        total_trade_dates,
        total_trade_dates,
        Some(effective_trade_date),
        format!(
            "增量更新完成，共处理 {} 个交易日，写入 {} 行，整段补救 {} 只股票。",
            total_trade_dates,
            total.saved_rows,
            failed_items.len()
        ),
    );

    Ok(total)
}

pub fn download(
    config: &AppConfig,
    progress_cb: Option<&DownloadProgressCallback>,
) -> Result<DownloadSummary, String> {
    let adj_type = match config.data.adj_type.trim().to_ascii_lowercase().as_str() {
        "qfq" => Ok(AdjType::Qfq),
        "hfq" => Ok(AdjType::Hfq),
        "raw" => Ok(AdjType::Raw),
        other => Err(format!("不支持的复权类型: {other}")),
    }?;

    let source_dir = config.output.dir.as_str();
    let db_path = source_db_path(source_dir);

    if !Path::new(&db_path).exists() {
        return download_first_all_market(config, progress_cb);
    }

    match load_latest_trade_date(source_dir, adj_type)? {
        Some(_) => download_pending_all_market(config, progress_cb),
        None => download_first_all_market(config, progress_cb),
    }
}
