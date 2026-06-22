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

use chrono::{Datelike, Local, NaiveDate, Timelike, Weekday};
use duckdb::Connection;
use rayon::{ThreadPool, ThreadPoolBuilder, prelude::*};

use crate::{
    crawler::concept::{ThsConceptFetchItem, ThsConceptRow, fetch_one_ths_concept_row},
    data::{
        DataReader,
        concept_performance_data::rebuild_concept_performance_range,
        download_data::{
            append_stage_pro_bar_rows, checkpoint_stock_data, delete_one_stock_all_rows,
            delete_one_stock_range, delete_trade_date_rows, ensure_indicator_columns,
            flush_stock_data_stage_table, init_stock_data_db, load_latest_close_map_before,
            load_latest_trade_date, reset_stock_data_stage_table, write_stock_list_csv,
            write_ths_concepts_csv,
        },
        load_stock_list, load_ths_concepts_list, load_trade_date_list, source_db_path,
        stock_list_path, trade_calendar_path,
    },
    download::{
        AdjType, BarFreq, DownloadSummary, DownloadTask, PreparedDownloadBatch,
        PreparedStockDownload, ProBarRow, TushareClient,
        ind_calc::{
            IndsCache, cache_ind_build, calc_increment_inds_from_history,
            load_many_tail_rows_with_warmup_need, warmup_ind_estimate,
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

pub type DownloadProgressCallback<'a> = dyn Fn(DownloadProgress) + Send + Sync + 'a;

const INCREMENTAL_INDICATOR_CHUNK_SIZE: usize = 256;
const THS_CONCEPT_RETRY_DELAY_SECS: u64 = 30;
const THS_CONCEPT_RETRY_LIMIT: usize = 5;
pub const STALE_STOCK_LIST_CONFIRM_REQUIRED_PREFIX: &str = "STALE_STOCK_LIST_CONFIRM_REQUIRED:";
const INDEX_TS_CODES: [&str; 7] = [
    "000001.SH",
    "399001.SZ",
    "399300.SZ",
    "399905.SZ",
    "399006.SZ",
    "000016.SH",
    "000852.SH",
];

#[derive(Debug, Clone)]
pub struct DownloadRuntimeConfig {
    pub source_dir: String,
    pub adj_type: AdjType,
    pub token: String,
    pub start_date: String,
    pub end_date: String,
    pub threads: usize,
    pub retry_times: usize,
    pub limit_calls_per_min: usize,
    pub include_turnover: bool,
    pub allow_stale_stock_list: bool,
}

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
    let refresh_cutoff = trade_calendar_refresh_cutoff(trade_calendar_end);
    Ok(match trade_dates.last() {
        Some(last_trade_date) => last_trade_date.as_str() < refresh_cutoff.as_str(),
        None => true,
    })
}

fn trade_calendar_refresh_cutoff(trade_calendar_end: &str) -> String {
    let Ok(mut date) = NaiveDate::parse_from_str(trade_calendar_end, "%Y%m%d") else {
        return trade_calendar_end.to_string();
    };

    while matches!(date.weekday(), Weekday::Sat | Weekday::Sun) {
        let Some(previous_date) = date.checked_sub_signed(chrono::Duration::days(1)) else {
            return trade_calendar_end.to_string();
        };
        date = previous_date;
    }

    date.format("%Y%m%d").to_string()
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

fn latest_stock_list_trade_date(source_dir: &str) -> Result<Option<String>, String> {
    let rows = load_stock_list(source_dir)?;
    Ok(rows
        .iter()
        .filter_map(|cols| cols.get(6))
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .max()
        .map(str::to_string))
}

fn stock_list_market_value_snapshot_is_usable(
    result: &crate::download::StockListFetchResult,
) -> bool {
    if result.basic_row_count == 0 {
        return false;
    }

    let min_usable_rows = (result.basic_row_count / 2).max(100);
    result.snapshot_row_count >= min_usable_rows && result.market_value_row_count >= min_usable_rows
}

fn stale_stock_list_confirm_error(
    source_dir: &str,
    effective_trade_date: &str,
    result: &crate::download::StockListFetchResult,
) -> String {
    let current_list_date = latest_stock_list_trade_date(source_dir)
        .ok()
        .flatten()
        .unwrap_or_else(|| "未知".to_string());

    format!(
        "{}股票列表市值数据尚未更新。目标交易日 {} 的 daily_basic 快照返回 {} 行，其中可用市值 {} 行；当前本地 stock_list.csv 最新交易日为 {}。为避免覆盖掉现有市值数据，本次没有写入新的 stock_list.csv。是否沿用现有股票列表继续下载行情？",
        STALE_STOCK_LIST_CONFIRM_REQUIRED_PREFIX,
        effective_trade_date,
        result.snapshot_row_count,
        result.market_value_row_count,
        current_list_date
    )
}

fn resolve_clock_effective_trade_date(
    trade_dates: &[String],
    today: &str,
    current_hhmm: u32,
) -> Result<(String, bool), String> {
    if let Some(pos) = trade_dates.iter().position(|d| d == today) {
        if current_hhmm >= 1600 {
            return Ok((trade_dates[pos].clone(), true));
        }
        if pos > 0 {
            return Ok((trade_dates[pos - 1].clone(), false));
        }
        return Err("交易日历中没有前一个交易日".to_string());
    }

    match trade_dates.iter().rev().find(|d| d.as_str() < today) {
        Some(date) => Ok((date.clone(), false)),
        None => Err("交易日历中找不到小于今天的交易日".to_string()),
    }
}

fn resolve_effective_trade_date(
    client: &TushareClient,
    trade_dates: &[String],
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<String, String> {
    let now = Local::now();
    let today = now.format("%Y%m%d").to_string();
    let current_hhmm = now.hour() * 100 + now.minute();
    let (candidate, is_today_after_close) =
        resolve_clock_effective_trade_date(trade_dates, today.as_str(), current_hhmm)?;

    if !is_today_after_close {
        return Ok(candidate);
    }

    let today_bar_count = client.fetch_market_daily_bar_count(candidate.as_str())?;
    if today_bar_count > 0 {
        return Ok(candidate);
    }

    let pos = trade_dates
        .iter()
        .position(|d| d == &candidate)
        .ok_or_else(|| "交易日历中找不到当前候选交易日".to_string())?;
    if pos == 0 {
        return Err("今日行情尚未更新，且交易日历中没有前一个交易日".to_string());
    }

    let fallback = trade_dates[pos - 1].clone();
    emit_progress(
        progress_cb,
        "prepare_trade_calendar",
        1,
        1,
        Some(fallback.clone()),
        format!(
            "{} 已过 16:00 但 Tushare daily 尚无当日行情，自动回退到上一交易日 {}。",
            candidate, fallback
        ),
    );
    Ok(fallback)
}

fn resolve_download_ts_codes(source_dir: &str) -> Result<Vec<String>, String> {
    let rows = load_stock_list(source_dir)?;
    Ok(rows
        .into_iter()
        .filter_map(|row| row.first().cloned())
        .collect())
}

fn normalize_list_date(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.len() == 8 && trimmed.chars().all(|ch| ch.is_ascii_digit()) {
        return Some(trimmed.to_string());
    }

    None
}

fn resolve_stock_list_date_map(source_dir: &str) -> Result<HashMap<String, String>, String> {
    let rows = load_stock_list(source_dir)?;
    let mut out = HashMap::with_capacity(rows.len());

    for row in rows {
        let Some(ts_code) = row.first().map(|value| value.trim()) else {
            continue;
        };
        let Some(list_date) = row.get(5).and_then(|value| normalize_list_date(value)) else {
            continue;
        };
        if !ts_code.is_empty() {
            out.insert(ts_code.to_string(), list_date);
        }
    }

    Ok(out)
}

fn resolve_index_ts_codes() -> Vec<String> {
    INDEX_TS_CODES
        .iter()
        .map(|item| (*item).to_string())
        .collect()
}

fn emit_progress(
    progress_cb: Option<&DownloadProgressCallback<'_>>,
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

fn build_ths_concept_http_client() -> Result<reqwest::blocking::Client, String> {
    let builder = reqwest::blocking::Client::builder().http1_only();

    #[cfg(target_os = "android")]
    let builder = {
        let root_store = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        let mut tls_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();
        tls_config.alpn_protocols = vec![b"http/1.1".to_vec()];
        builder.use_preconfigured_tls(tls_config)
    };

    builder
        .build()
        .map_err(|e| format!("创建同花顺概念 HTTP 客户端失败: {e}"))
}

fn sync_gaini_bx_range(
    source_dir: &str,
    start_date: &str,
    end_date: &str,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<usize, String> {
    emit_progress(
        progress_cb,
        "rebuild_concept_performance",
        0,
        1,
        Some(format!("{start_date}-{end_date}")),
        format!("开始重建概念表现区间 {} ~ {}。", start_date, end_date),
    );
    let bx_row_count = rebuild_concept_performance_range(source_dir, start_date, end_date)?;
    emit_progress(
        progress_cb,
        "rebuild_concept_performance",
        1,
        1,
        Some(format!("{start_date}-{end_date}")),
        format!(
            "概念表现区间 {} ~ {} 重建完成，共写入 {} 行。",
            start_date, end_date, bx_row_count
        ),
    );
    Ok(bx_row_count)
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
    progress_cb: Option<&DownloadProgressCallback<'_>>,
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

    let http = build_ths_concept_http_client()?;

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
    progress_cb: Option<&DownloadProgressCallback<'_>>,
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
                let http = match build_ths_concept_http_client() {
                    Ok(client) => client,
                    Err(error) => {
                        let _ = tx.send(ThsConceptConcurrentMessage::Failure { item: None, error });
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
    progress_cb: Option<&DownloadProgressCallback<'_>>,
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
    config: &DownloadRuntimeConfig,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<String, String> {
    // 初始化基础数据,返回当前有效交易日
    let source_dir = config.source_dir.as_str();
    let client = TushareClient::new(config.token.clone(), config.limit_calls_per_min)?;

    let now = Local::now();
    let trade_calendar_end = format!("{:04}1231", now.year());

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
            config.start_date.as_str(),
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

    // 4. 16:00 后还会探测 Tushare daily 是否已经有今日行情，避免供应端延迟时误进今日增量。
    let effective_trade_date = resolve_effective_trade_date(&client, &trade_dates, progress_cb)?;

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
        let stock_list_result =
            client.fetch_stock_list_rows_with_snapshot_stats(effective_trade_date.as_str())?;
        if stock_list_market_value_snapshot_is_usable(&stock_list_result) {
            let row_count = stock_list_result.rows.len();
            write_stock_list_csv(source_dir, &stock_list_result.rows)?;
            emit_progress(
                progress_cb,
                "prepare_stock_list",
                1,
                1,
                Some(effective_trade_date.clone()),
                format!(
                    "股票列表刷新完成，交易日 {}，写入 {} 行，可用市值 {} 行。",
                    effective_trade_date, row_count, stock_list_result.market_value_row_count
                ),
            );
        } else if stock_list_path(source_dir).exists() {
            if !config.allow_stale_stock_list {
                return Err(stale_stock_list_confirm_error(
                    source_dir,
                    effective_trade_date.as_str(),
                    &stock_list_result,
                ));
            }

            emit_progress(
                progress_cb,
                "prepare_stock_list",
                1,
                1,
                latest_stock_list_trade_date(source_dir)?,
                format!(
                    "交易日 {} 的股票列表市值数据尚未更新，已按用户确认保留现有 stock_list.csv 继续。",
                    effective_trade_date
                ),
            );
        } else {
            return Err(format!(
                "交易日 {} 的股票列表市值数据尚未更新，且本地没有可沿用的 stock_list.csv；请稍后重试。",
                effective_trade_date
            ));
        }
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

fn init_index_basic_data(
    config: &DownloadRuntimeConfig,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<String, String> {
    let source_dir = config.source_dir.as_str();
    let client = TushareClient::new(config.token.clone(), config.limit_calls_per_min)?;

    let now = Local::now();
    let trade_calendar_end = format!("{:04}1231", now.year());

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
            config.start_date.as_str(),
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

    let trade_dates = crate::data::load_trade_date_list(source_dir)?;
    let effective_trade_date = resolve_effective_trade_date(&client, &trade_dates, progress_cb)?;

    emit_progress(
        progress_cb,
        "prepare_index_list",
        1,
        1,
        Some(effective_trade_date.clone()),
        format!("指数下载使用内置指数池，交易日 {}。", effective_trade_date),
    );

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

fn build_download_task_with_list_dates(
    ts_codes: &[String],
    start_date: &str,
    end_date: &str,
    adj_type: AdjType,
    with_factors: bool,
    list_dates: &HashMap<String, String>,
) -> Vec<DownloadTask> {
    let mut tasks = Vec::with_capacity(ts_codes.len());

    for ts_code in ts_codes {
        let adjusted_start = list_dates
            .get(ts_code)
            .filter(|list_date| list_date.as_str() > start_date)
            .map_or_else(|| start_date.to_string(), Clone::clone);
        if adjusted_start.as_str() > end_date {
            continue;
        }

        tasks.push(DownloadTask {
            ts_code: ts_code.clone(),
            start_date: adjusted_start,
            end_date: end_date.to_string(),
            freq: BarFreq::Daily,
            adj_type,
            with_factors,
        });
    }

    tasks
}

fn build_download_task_from_stock_list_dates(
    source_dir: &str,
    ts_codes: &[String],
    start_date: &str,
    end_date: &str,
    adj_type: AdjType,
    with_factors: bool,
) -> Result<Vec<DownloadTask>, String> {
    let list_dates = resolve_stock_list_date_map(source_dir)?;
    Ok(build_download_task_with_list_dates(
        ts_codes,
        start_date,
        end_date,
        adj_type,
        with_factors,
        &list_dates,
    ))
}

fn merge_summary(total: &mut DownloadSummary, batch: DownloadSummary) {
    total.success_count += batch.success_count;
    total.failed_count += batch.failed_count;
    total.saved_rows += batch.saved_rows;
    total.concept_performance_rows += batch.concept_performance_rows;
    total.recovered_stock_count += batch.recovered_stock_count;
    total
        .recovered_stock_codes
        .extend(batch.recovered_stock_codes);
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

fn rebuild_index_indicators_with_history(
    source_dir: &str,
    prepared_items: &mut [PreparedStockDownload],
) -> Result<(), String> {
    if prepared_items.is_empty() {
        return Ok(());
    }

    let inds_cache = cache_ind_build(source_dir)?;
    if inds_cache.is_empty() {
        for item in prepared_items {
            item.indicators.clear();
        }
        return Ok(());
    }

    let warmup_need = warmup_ind_estimate(source_dir)?;
    let history_anchor_date = prepared_items
        .iter()
        .map(|item| item.start_date.as_str())
        .min()
        .ok_or_else(|| "缺少指数增量起始日期".to_string())?;
    let latest_map_before = load_latest_close_map_before(source_dir, "ind", history_anchor_date)?;
    let history_end_dates = latest_map_before
        .iter()
        .map(|(ts_code, latest)| (ts_code.clone(), latest.trade_date.clone()))
        .collect::<HashMap<_, _>>();
    let dr = DataReader::new(source_dir)?;
    let history_rows_by_stock =
        load_many_tail_rows_with_warmup_need(&dr, "ind", &history_end_dates, warmup_need)?;

    for item in prepared_items {
        item.indicators = calc_increment_inds_from_history(
            &inds_cache,
            history_rows_by_stock.get(&item.ts_code).cloned(),
            &item.rows,
        )?;
    }

    Ok(())
}

fn write_prepared_stock_batch(
    conn: &Connection,
    prepared_items: &[PreparedStockDownload],
) -> Result<(), String> {
    if prepared_items.is_empty() {
        return Ok(());
    }

    with_transaction(conn, |tx| {
        let indicator_names = collect_indicator_names(prepared_items);
        ensure_indicator_columns(tx, &indicator_names)?;
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

fn adj_type_to_db_label(adj_type: AdjType) -> &'static str {
    match adj_type {
        AdjType::Qfq => "qfq",
        AdjType::Hfq => "hfq",
        AdjType::Raw => "raw",
        AdjType::Ind => "ind",
    }
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
        Ok(value) => match conn.execute_batch("COMMIT") {
            Ok(()) => Ok(value),
            Err(commit_error) => match conn.execute_batch("ROLLBACK") {
                Ok(()) => Err(format!("提交事务失败，已回滚: {commit_error}")),
                Err(rollback_error) => Err(format!(
                    "提交事务失败且回滚失败: commit={commit_error}; rollback={rollback_error}"
                )),
            },
        },
        Err(action_error) => match conn.execute_batch("ROLLBACK") {
            Ok(()) => Err(format!("{action_error}；本步骤数据库事务已回滚")),
            Err(rollback_error) => Err(format!(
                "{action_error}；本步骤数据库事务回滚失败: {rollback_error}"
            )),
        },
    }
}

fn delete_stocks_all_rows(
    conn: &Connection,
    ts_codes: &HashSet<String>,
    adj_type: AdjType,
) -> Result<(), String> {
    if ts_codes.is_empty() {
        return Ok(());
    }

    with_transaction(conn, |tx| {
        for ts_code in ts_codes {
            delete_one_stock_all_rows(tx, ts_code, adj_type)?;
        }
        Ok(())
    })
}

fn write_incremental_trade_date_batches<F>(
    conn: &Connection,
    batches: &[PendingTradeDateBatch],
    adj_type: AdjType,
    indicator_names: &[String],
    mut on_batch_written: F,
) -> Result<(), String>
where
    F: FnMut(&PendingTradeDateBatch),
{
    if batches.is_empty() {
        return Ok(());
    }

    with_transaction(conn, |tx| {
        ensure_indicator_columns(tx, indicator_names)?;
        reset_stock_data_stage_table(tx)?;

        for batch in batches {
            delete_trade_date_rows(tx, adj_type, batch.trade_date.as_str())?;
            append_stage_pro_bar_rows(tx, adj_type, &batch.rows, &batch.indicators)?;
            on_batch_written(batch);
        }

        flush_stock_data_stage_table(tx)
    })
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

fn recover_failed_stocks_with_independent_writes(
    client: &TushareClient,
    source_dir: &str,
    failed_items: &[(String, String)],
    start_date: &str,
    trade_date: &str,
    adj_type: AdjType,
    with_factors: bool,
    pool: &ThreadPool,
    conn: &Connection,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<DownloadSummary, String> {
    if failed_items.is_empty() {
        return Ok(DownloadSummary::default());
    }

    let failed_ts_codes = failed_items
        .iter()
        .map(|(ts_code, _)| ts_code.clone())
        .collect::<Vec<_>>();
    let tasks = build_download_task_from_stock_list_dates(
        source_dir,
        &failed_ts_codes,
        start_date,
        trade_date,
        adj_type,
        with_factors,
    )?;
    let total_tasks = tasks.len();
    let mut total = DownloadSummary::default();

    for (task_idx, task) in tasks.iter().enumerate() {
        emit_progress(
            progress_cb,
            "recover_failed_stocks",
            task_idx,
            total_tasks,
            Some(task.ts_code.clone()),
            format!(
                "正在整段补救重下 {}，进度 {}/{}。",
                task.ts_code, task_idx, total_tasks
            ),
        );
        let one_batch =
            pool.install(|| client.prepare_stock_downloads(source_dir, std::slice::from_ref(task)));
        let one_summary = one_batch.summary();
        let one_success_count = one_summary.success_count;
        let one_failed_count = one_summary.failed_count;
        if !one_batch.prepared_items.is_empty() {
            write_prepared_stock_batch(conn, &one_batch.prepared_items)?;
            total.recovered_stock_count += one_batch.prepared_items.len();
            total.recovered_stock_codes.extend(
                one_batch
                    .prepared_items
                    .iter()
                    .map(|item| item.ts_code.clone()),
            );
        }
        merge_summary(&mut total, one_summary);
        emit_progress(
            progress_cb,
            "recover_failed_stocks",
            task_idx + 1,
            total_tasks,
            Some(task.ts_code.clone()),
            format!(
                "整段补救重下 {} 完成，成功 {}，失败 {}，进度 {}/{}。",
                task.ts_code,
                one_success_count,
                one_failed_count,
                task_idx + 1,
                total_tasks
            ),
        );
    }

    Ok(total)
}

fn retry_failed_downloads(
    client: &TushareClient,
    source_dir: &str,
    tasks: Vec<DownloadTask>,
    retry_times: usize,
    pool: &ThreadPool,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
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

fn download_first_all_market_after_basic_data(
    config: &DownloadRuntimeConfig,
    effective_trade_date: &str,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<DownloadSummary, String> {
    let adj_type = config.adj_type;
    let source_dir = config.source_dir.as_str();
    let start_date = config.start_date.as_str();
    let end_date = if config.end_date.eq_ignore_ascii_case("today") {
        effective_trade_date
    } else {
        config.end_date.as_str()
    };
    let with_factors = config.include_turnover;

    let client = TushareClient::new(config.token.clone(), config.limit_calls_per_min)?;
    let pool = build_download_pool(config.threads)?;
    let db_path = source_db_path(source_dir);
    let db_path_str = db_path
        .to_str()
        .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?;
    init_stock_data_db(db_path_str)?;
    let conn = Connection::open(db_path_str).map_err(|e| format!("数据库连接错误:{e}"))?;

    let ts_codes = resolve_download_ts_codes(source_dir)?;

    download_selected_stocks_with_context(
        source_dir,
        &effective_trade_date,
        &client,
        &pool,
        &conn,
        &ts_codes,
        start_date,
        end_date,
        adj_type,
        with_factors,
        config.retry_times,
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
    ts_codes: &[String],
    start_date: &str,
    end_date: &str,
    adj_type: AdjType,
    with_factors: bool,
    retry_times: usize,
    start_message: &str,
    done_message: &str,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
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
    let tasks = build_download_task_from_stock_list_dates(
        source_dir,
        ts_codes,
        start_date,
        end_date,
        adj_type,
        with_factors,
    )?;
    let total_tasks = tasks.len();
    if tasks.is_empty() {
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
    if adj_type == AdjType::Qfq {
        total.concept_performance_rows +=
            sync_gaini_bx_range(source_dir, start_date, end_date, progress_cb)?;
    }

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
    config: &DownloadRuntimeConfig,
    ts_codes: &[String],
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<DownloadSummary, String> {
    let effective_trade_date = init_stock_basic_data(config, progress_cb)?;
    let adj_type = config.adj_type;
    let source_dir = config.source_dir.as_str();
    let start_date = config.start_date.as_str();
    let end_date = if config.end_date.eq_ignore_ascii_case("today") {
        effective_trade_date.as_str()
    } else {
        config.end_date.as_str()
    };
    let with_factors = config.include_turnover;
    let client = TushareClient::new(config.token.clone(), config.limit_calls_per_min)?;
    let pool = build_download_pool(config.threads)?;
    let db_path = source_db_path(source_dir);
    let db_path_str = db_path
        .to_str()
        .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?;
    init_stock_data_db(db_path_str)?;
    let conn = Connection::open(db_path_str).map_err(|e| format!("数据库连接错误:{e}"))?;

    download_selected_stocks_with_context(
        source_dir,
        &effective_trade_date,
        &client,
        &pool,
        &conn,
        ts_codes,
        start_date,
        end_date,
        adj_type,
        with_factors,
        config.retry_times,
        "缺失股票补全开始",
        "缺失股票补全结束",
        progress_cb,
    )
}

fn download_indices_with_context(
    config: &DownloadRuntimeConfig,
    start_date: &str,
    end_date: &str,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<DownloadSummary, String> {
    let effective_trade_date = end_date.to_string();
    let source_dir = config.source_dir.as_str();
    let client = TushareClient::new(config.token.clone(), config.limit_calls_per_min)?;
    let pool = build_download_pool(config.threads)?;
    let db_path = source_db_path(source_dir);
    let db_path_str = db_path
        .to_str()
        .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?;
    init_stock_data_db(db_path_str)?;
    let conn = Connection::open(db_path_str).map_err(|e| format!("数据库连接错误:{e}"))?;
    let ts_codes = resolve_index_ts_codes();

    if ts_codes.is_empty() {
        return Ok(DownloadSummary::default());
    }

    let total_tasks = ts_codes.len();
    let mut processed_tasks = 0usize;
    let mut total = DownloadSummary::default();
    emit_progress(
        progress_cb,
        "download_index_bars",
        0,
        total_tasks,
        None,
        format!("指数下载开始，共 {total_tasks} 只指数待处理。"),
    );

    for (batch_idx, batch) in ts_codes
        .chunks(pool.current_num_threads().max(1))
        .enumerate()
    {
        let mut prepared_batch = pool
            .install(|| client.prepare_index_downloads(source_dir, batch, start_date, end_date));
        rebuild_index_indicators_with_history(
            source_dir,
            prepared_batch.prepared_items.as_mut_slice(),
        )?;
        let batch_summary = prepared_batch.summary();
        emit_progress(
            progress_cb,
            "write_db",
            processed_tasks,
            total_tasks,
            Some(format!("指数第 {} 批", batch_idx + 1)),
            format!(
                "指数第 {} 批下载完成，正在写入数据库，本批 {} 只。",
                batch_idx + 1,
                batch.len()
            ),
        );
        write_prepared_stock_batch(&conn, &prepared_batch.prepared_items)?;
        merge_summary(&mut total, batch_summary);
        processed_tasks += batch.len();
        emit_progress(
            progress_cb,
            "download_index_bars",
            processed_tasks,
            total_tasks,
            Some(format!("指数第 {} 批", batch_idx + 1)),
            format!("已处理 {} / {} 只指数。", processed_tasks, total_tasks),
        );
    }

    checkpoint_stock_data(&conn)?;
    emit_progress(
        progress_cb,
        "done",
        total_tasks,
        total_tasks,
        Some(effective_trade_date),
        format!(
            "指数下载结束，成功 {} 只，失败 {} 只。",
            total.success_count, total.failed_count
        ),
    );

    Ok(total)
}

pub fn download_indices(
    config: &DownloadRuntimeConfig,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<DownloadSummary, String> {
    let effective_trade_date = init_index_basic_data(config, progress_cb)?;
    download_indices_after_basic_data(config, effective_trade_date.as_str(), progress_cb)
}

pub fn download_indices_after_basic_data(
    config: &DownloadRuntimeConfig,
    effective_trade_date: &str,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<DownloadSummary, String> {
    let source_dir = config.source_dir.as_str();
    emit_progress(
        progress_cb,
        "prepare_index_list",
        1,
        1,
        Some(effective_trade_date.to_string()),
        format!(
            "指数下载复用开头基础数据检查结果，交易日 {}。",
            effective_trade_date
        ),
    );

    match load_latest_trade_date(source_dir, AdjType::Ind)? {
        Some(last_saved_trade_date) if last_saved_trade_date.as_str() < effective_trade_date => {
            let trade_dates = load_trade_date_list(source_dir)?;
            let pending_trade_dates: Vec<String> = trade_dates
                .iter()
                .filter(|d| d.as_str() > last_saved_trade_date.as_str())
                .filter(|d| d.as_str() <= effective_trade_date)
                .cloned()
                .collect();
            if pending_trade_dates.is_empty() {
                return Ok(DownloadSummary::default());
            }
            download_indices_with_context(
                config,
                pending_trade_dates[0].as_str(),
                effective_trade_date,
                progress_cb,
            )
        }
        Some(_) => Ok(DownloadSummary::default()),
        None => download_indices_with_context(
            config,
            config.start_date.as_str(),
            effective_trade_date,
            progress_cb,
        ),
    }
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
                let chunk_end_dates = chunk
                    .iter()
                    .filter_map(|(ts_code, _)| {
                        history_end_dates
                            .get(ts_code)
                            .map(|end_date| (ts_code.clone(), end_date.clone()))
                    })
                    .collect::<HashMap<_, _>>();
                let mut history_rows_by_stock = load_many_tail_rows_with_warmup_need(
                    &dr,
                    adj_type_to_db_label(adj_type),
                    &chunk_end_dates,
                    warmup_need,
                )?;
                let mut chunk_out = Vec::with_capacity(chunk.len());

                for (ts_code, rows) in chunk {
                    let indicators = calc_increment_inds_from_history(
                        inds_cache,
                        history_rows_by_stock.remove(ts_code),
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
    config: &DownloadRuntimeConfig,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<DownloadSummary, String> {
    let effective_trade_date = init_stock_basic_data(config, progress_cb)?;
    download_pending_all_market_after_basic_data(config, effective_trade_date.as_str(), progress_cb)
}

fn download_pending_all_market_after_basic_data(
    config: &DownloadRuntimeConfig,
    effective_trade_date: &str,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<DownloadSummary, String> {
    let source_dir = config.source_dir.as_str();
    let start_date = config.start_date.as_str();
    let with_factors = config.include_turnover;
    let adj_type = config.adj_type;
    if adj_type != AdjType::Qfq {
        return Err("当前增量pre_close校验只支持 qfq".to_string());
    }

    let last_saved_trade_date = load_latest_trade_date(source_dir, adj_type)?
        .ok_or_else(|| "数据库里还没有可用于增量的历史数据，请先做首次下载".to_string())?;

    if last_saved_trade_date.as_str() >= effective_trade_date {
        return Ok(DownloadSummary::default());
    }

    let trade_dates = load_trade_date_list(source_dir)?;
    let pending_trade_dates: Vec<String> = trade_dates
        .iter()
        .filter(|d| d.as_str() > last_saved_trade_date.as_str())
        .filter(|d| d.as_str() <= effective_trade_date)
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

    let client = TushareClient::new(config.token.clone(), config.limit_calls_per_min)?;
    let pool = build_download_pool(config.threads)?;
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

    let passed_write_batches = build_trade_date_write_batches(&passed_prepared_items)?;
    if passed_write_batches.is_empty() && failed_items.is_empty() {
        return Ok(total);
    }

    let passed_saved_rows = passed_write_batches
        .iter()
        .map(|batch| batch.rows.len())
        .sum::<usize>();
    let write_total = passed_write_batches.len() + failed_items.len();

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
            "增量校验完成，准备写入 {} 个交易日批次，并逐只补救 {} 只断点股票。",
            passed_write_batches.len(),
            failed_items.len()
        ),
    );

    let mut written_steps = 0usize;
    if !failed_ts_codes.is_empty() {
        delete_stocks_all_rows(&conn, &failed_ts_codes, adj_type)?;
        emit_progress(
            progress_cb,
            "write_db",
            written_steps,
            write_total,
            Some(format!("清理 {} 只断点股票", failed_ts_codes.len())),
            format!(
                "已在独立事务中删除 {} 只断点股票的全部历史行情，后续将逐只补救。",
                failed_ts_codes.len()
            ),
        );
    }

    if !passed_write_batches.is_empty() {
        write_incremental_trade_date_batches(
            &conn,
            &passed_write_batches,
            adj_type,
            &indicator_names,
            |batch| {
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
            },
        )?;
        total.saved_rows += passed_saved_rows;
    }

    if !failed_items.is_empty() {
        emit_progress(
            progress_cb,
            "recover_failed_stocks",
            0,
            failed_items.len(),
            None,
            format!(
                "按日增量数据已提交；开始逐只整段补救 {} 只断点股票，每只股票独立提交。",
                failed_items.len()
            ),
        );
        let recovered = recover_failed_stocks_with_independent_writes(
            &client,
            source_dir,
            &failed_items,
            start_date,
            effective_trade_date,
            adj_type,
            with_factors,
            &pool,
            &conn,
            progress_cb,
        )?;
        total.saved_rows += recovered.saved_rows;
        total.failed_count += recovered.failed_count;
        total.failed_items.extend(recovered.failed_items);
        total.recovered_stock_count = recovered.recovered_stock_count;
        total.recovered_stock_codes = recovered.recovered_stock_codes;
        emit_progress(
            progress_cb,
            "recover_failed_stocks",
            total.recovered_stock_count,
            failed_items.len(),
            None,
            format!(
                "逐只补救完成，成功 {} 只，失败 {} 只；成功股票均已独立提交。",
                total.recovered_stock_count, total.failed_count
            ),
        );
    }

    checkpoint_stock_data(&conn)?;
    total.concept_performance_rows += sync_gaini_bx_range(
        source_dir,
        pending_trade_dates[0].as_str(),
        effective_trade_date,
        progress_cb,
    )?;

    emit_progress(
        progress_cb,
        "done",
        total_trade_dates,
        total_trade_dates,
        Some(effective_trade_date.to_string()),
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
    config: &DownloadRuntimeConfig,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<DownloadSummary, String> {
    let effective_trade_date = init_stock_basic_data(config, progress_cb)?;
    download_after_basic_data(config, effective_trade_date.as_str(), progress_cb)
}

pub fn download_after_basic_data(
    config: &DownloadRuntimeConfig,
    effective_trade_date: &str,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<DownloadSummary, String> {
    let adj_type = config.adj_type;
    let source_dir = config.source_dir.as_str();
    let db_path = source_db_path(source_dir);

    if !Path::new(&db_path).exists() {
        return download_first_all_market_after_basic_data(
            config,
            effective_trade_date,
            progress_cb,
        );
    }

    match load_latest_trade_date(source_dir, adj_type)? {
        Some(_) => {
            download_pending_all_market_after_basic_data(config, effective_trade_date, progress_cb)
        }
        None => {
            download_first_all_market_after_basic_data(config, effective_trade_date, progress_cb)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn temp_source_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("lianghua_runner_{prefix}_{nanos}"))
    }

    fn write_trade_calendar(source_dir: &PathBuf, dates: &[&str]) {
        fs::create_dir_all(source_dir).expect("create temp source dir");
        let mut text = String::from("cal_date\n");
        for date in dates {
            text.push_str(date);
            text.push('\n');
        }
        fs::write(
            trade_calendar_path(source_dir.to_str().expect("utf8 path")),
            text,
        )
        .expect("write trade_calendar.csv");
    }

    fn needs_refresh(source_dir: &PathBuf, trade_calendar_end: &str) -> bool {
        trade_calendar_needs_refresh(source_dir.to_str().expect("utf8 path"), trade_calendar_end)
            .expect("refresh check should succeed")
    }

    fn stock_list_fetch_result(
        basic_row_count: usize,
        snapshot_row_count: usize,
        market_value_row_count: usize,
    ) -> crate::download::StockListFetchResult {
        crate::download::StockListFetchResult {
            rows: Vec::new(),
            basic_row_count,
            snapshot_row_count,
            market_value_row_count,
        }
    }

    #[test]
    fn trade_calendar_refresh_cutoff_keeps_weekday_year_end() {
        assert_eq!(trade_calendar_refresh_cutoff("20261231"), "20261231");
    }

    #[test]
    fn trade_calendar_refresh_cutoff_moves_weekend_year_end_to_previous_weekday() {
        assert_eq!(trade_calendar_refresh_cutoff("20221231"), "20221230");
        assert_eq!(trade_calendar_refresh_cutoff("20231231"), "20231229");
    }

    #[test]
    fn trade_calendar_does_not_refresh_when_open_dates_cover_weekend_year_end() {
        let source_dir = temp_source_dir("calendar_weekend_covered");
        write_trade_calendar(&source_dir, &["20221229", "20221230"]);

        assert!(!needs_refresh(&source_dir, "20221231"));

        fs::remove_dir_all(source_dir).ok();
    }

    #[test]
    fn trade_calendar_refreshes_when_open_dates_stop_before_year_end_cutoff() {
        let source_dir = temp_source_dir("calendar_stale");
        write_trade_calendar(&source_dir, &["20221229"]);

        assert!(needs_refresh(&source_dir, "20221231"));

        fs::remove_dir_all(source_dir).ok();
    }

    #[test]
    fn trade_calendar_refreshes_when_file_is_missing_or_empty() {
        let missing_source_dir = temp_source_dir("calendar_missing");
        assert!(needs_refresh(&missing_source_dir, "20261231"));

        let empty_source_dir = temp_source_dir("calendar_empty");
        write_trade_calendar(&empty_source_dir, &[]);
        assert!(needs_refresh(&empty_source_dir, "20261231"));

        fs::remove_dir_all(empty_source_dir).ok();
    }

    #[test]
    fn clock_effective_trade_date_uses_previous_trade_day_before_close() {
        let trade_dates = vec!["20260512".to_string(), "20260513".to_string()];
        let (effective_trade_date, needs_today_probe) =
            resolve_clock_effective_trade_date(&trade_dates, "20260513", 1559).expect("resolve");

        assert_eq!(effective_trade_date, "20260512");
        assert!(!needs_today_probe);
    }

    #[test]
    fn clock_effective_trade_date_marks_today_after_close_for_probe() {
        let trade_dates = vec!["20260512".to_string(), "20260513".to_string()];
        let (effective_trade_date, needs_today_probe) =
            resolve_clock_effective_trade_date(&trade_dates, "20260513", 1600).expect("resolve");

        assert_eq!(effective_trade_date, "20260513");
        assert!(needs_today_probe);
    }

    #[test]
    fn stock_list_snapshot_requires_usable_market_value_coverage() {
        assert!(stock_list_market_value_snapshot_is_usable(
            &stock_list_fetch_result(5000, 4200, 4100)
        ));
        assert!(!stock_list_market_value_snapshot_is_usable(
            &stock_list_fetch_result(5000, 4200, 20)
        ));
        assert!(!stock_list_market_value_snapshot_is_usable(
            &stock_list_fetch_result(5000, 20, 20)
        ));
    }

    #[test]
    fn stock_download_tasks_are_clamped_to_list_date() {
        let ts_codes = vec!["000001.SZ".to_string(), "301999.SZ".to_string()];
        let list_dates = HashMap::from([
            ("000001.SZ".to_string(), "19910403".to_string()),
            ("301999.SZ".to_string(), "20250115".to_string()),
        ]);

        let tasks = build_download_task_with_list_dates(
            &ts_codes,
            "20240101",
            "20250131",
            AdjType::Qfq,
            true,
            &list_dates,
        );

        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[0].ts_code, "000001.SZ");
        assert_eq!(tasks[0].start_date, "20240101");
        assert_eq!(tasks[1].ts_code, "301999.SZ");
        assert_eq!(tasks[1].start_date, "20250115");
    }

    #[test]
    fn stock_download_tasks_skip_stocks_listed_after_end_date() {
        let ts_codes = vec!["301999.SZ".to_string()];
        let list_dates = HashMap::from([("301999.SZ".to_string(), "20250201".to_string())]);

        let tasks = build_download_task_with_list_dates(
            &ts_codes,
            "20240101",
            "20250131",
            AdjType::Qfq,
            false,
            &list_dates,
        );

        assert!(tasks.is_empty());
    }

    #[test]
    fn incremental_daily_commit_survives_later_stock_repair_failure() {
        let source_dir = temp_source_dir("incremental_repair_transaction_boundaries");
        fs::create_dir_all(&source_dir).expect("create temp source dir");
        let db_path = source_dir.join("stock_data.db");
        init_stock_data_db(db_path.to_str().expect("utf8 db path")).expect("init stock db");
        let conn = Connection::open(&db_path).expect("open stock db");
        conn.execute_batch(
            r#"
            INSERT INTO stock_data (
                ts_code, trade_date, adj_type, open, high, low, close, pre_close,
                change, pct_chg, vol, amount, tor
            ) VALUES
                ('000001.SZ', '20260102', 'qfq', 10, 11, 9, 10.5, 10, 0.5, 5, 1000, 10000, 1.2),
                ('000001.SZ', '20260105', 'qfq', 11, 12, 10, 11.5, 10.5, 1, 9.5, 1100, 11000, 1.3),
                ('000001.SZ', '20260102', 'hfq', 20, 21, 19, 20.5, 20, 0.5, 2.5, 1000, 10000, 1.2),
                ('000002.SZ', '20260105', 'qfq', 30, 31, 29, 30.5, 30, 0.5, 1.7, 1200, 12000, 1.4);
            "#,
        )
        .expect("seed stock rows");

        delete_stocks_all_rows(
            &conn,
            &HashSet::from(["000001.SZ".to_string()]),
            AdjType::Qfq,
        )
        .expect("delete broken stock history");

        let broken_qfq_rows = conn
            .query_row(
                "SELECT COUNT(*) FROM stock_data WHERE ts_code = '000001.SZ' AND adj_type = 'qfq'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("count broken qfq rows");
        let preserved_hfq_rows = conn
            .query_row(
                "SELECT COUNT(*) FROM stock_data WHERE ts_code = '000001.SZ' AND adj_type = 'hfq'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("count preserved hfq rows");
        assert_eq!(broken_qfq_rows, 0);
        assert_eq!(preserved_hfq_rows, 1);

        let daily_batch = PendingTradeDateBatch {
            trade_date: "20260106".to_string(),
            rows: vec![ProBarRow {
                ts_code: "000002.SZ".to_string(),
                trade_date: "20260106".to_string(),
                open: 30.5,
                high: 32.0,
                low: 30.0,
                close: 31.5,
                pre_close: 30.5,
                change: 1.0,
                pct_chg: 3.28,
                vol: 1300.0,
                amount: 13000.0,
                turnover_rate: Some(1.5),
                volume_ratio: None,
            }],
            indicators: HashMap::new(),
        };
        write_incremental_trade_date_batches(&conn, &[daily_batch], AdjType::Qfq, &[], |_| {})
            .expect("commit daily increment");

        let invalid_repair = PreparedStockDownload {
            ts_code: "000001.SZ".to_string(),
            start_date: "20260102".to_string(),
            end_date: "20260106".to_string(),
            adj_type: AdjType::Qfq,
            rows: vec![ProBarRow {
                ts_code: "000001.SZ".to_string(),
                trade_date: "20260102".to_string(),
                open: 10.0,
                high: 11.0,
                low: 9.0,
                close: 10.5,
                pre_close: 10.0,
                change: 0.5,
                pct_chg: 5.0,
                vol: 1000.0,
                amount: 10000.0,
                turnover_rate: Some(1.2),
                volume_ratio: None,
            }],
            indicators: HashMap::from([("MA5".to_string(), Vec::new())]),
        };
        write_prepared_stock_batch(&conn, &[invalid_repair])
            .expect_err("repair write should fail independently");

        let committed_daily_rows = conn
            .query_row(
                "SELECT COUNT(*) FROM stock_data WHERE ts_code = '000002.SZ' AND trade_date = '20260106' AND adj_type = 'qfq'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("count committed daily rows");
        let broken_stock_rows = conn
            .query_row(
                "SELECT COUNT(*) FROM stock_data WHERE ts_code = '000001.SZ' AND adj_type = 'qfq'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("count failed repair rows");
        assert_eq!(committed_daily_rows, 1);
        assert_eq!(broken_stock_rows, 0);

        fs::remove_dir_all(source_dir).ok();
    }

    #[test]
    fn stock_batch_failure_rolls_back_rows_and_indicator_columns() {
        let source_dir = temp_source_dir("stock_batch_rollback");
        fs::create_dir_all(&source_dir).expect("create temp source dir");
        let db_path = source_dir.join("stock_data.db");
        init_stock_data_db(db_path.to_str().expect("utf8 db path")).expect("init stock db");
        let conn = Connection::open(&db_path).expect("open stock db");
        conn.execute(
            r#"
            INSERT INTO stock_data (
                ts_code, trade_date, adj_type, open, high, low, close, pre_close,
                change, pct_chg, vol, amount, tor
            ) VALUES (
                '000001.SZ', '20260105', 'qfq', 10, 11, 9, 10.5, 10,
                0.5, 5, 1000, 10000, 1.2
            )
            "#,
            [],
        )
        .expect("seed stock row");

        let prepared = PreparedStockDownload {
            ts_code: "000001.SZ".to_string(),
            start_date: "20260105".to_string(),
            end_date: "20260105".to_string(),
            adj_type: AdjType::Qfq,
            rows: vec![ProBarRow {
                ts_code: "000001.SZ".to_string(),
                trade_date: "20260105".to_string(),
                open: 20.0,
                high: 21.0,
                low: 19.0,
                close: 20.5,
                pre_close: 20.0,
                change: 0.5,
                pct_chg: 2.5,
                vol: 2000.0,
                amount: 20000.0,
                turnover_rate: Some(2.0),
                volume_ratio: None,
            }],
            indicators: HashMap::from([("MA5".to_string(), Vec::new())]),
        };

        let error = write_prepared_stock_batch(&conn, &[prepared])
            .expect_err("invalid indicator length should fail");
        assert!(error.contains("数据库事务已回滚"));

        let close = conn
            .query_row(
                "SELECT CAST(close AS DOUBLE) FROM stock_data WHERE ts_code = '000001.SZ' AND trade_date = '20260105' AND adj_type = 'qfq'",
                [],
                |row| row.get::<_, f64>(0),
            )
            .expect("read preserved close");
        assert_eq!(close, 10.5);

        let ma5_columns = conn
            .query_row(
                "SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'stock_data' AND column_name = 'MA5'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("count MA5 columns");
        assert_eq!(ma5_columns, 0);

        fs::remove_dir_all(source_dir).ok();
    }
}
