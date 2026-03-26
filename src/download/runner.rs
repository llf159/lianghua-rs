use std::{
    collections::{HashMap, HashSet},
    path::Path,
};

use chrono::{Datelike, Local, Timelike};
use duckdb::Connection;
use rayon::{ThreadPool, ThreadPoolBuilder, prelude::*};

use crate::{
    config::AppConfig,
    data::{
        download_data::{
            LatestCloseRow, append_stage_pro_bar_rows, checkpoint_stock_data,
            delete_one_stock_range, delete_trade_date_rows, ensure_indicator_columns,
            flush_stock_data_stage_table, init_stock_data_db, load_latest_close_map_before,
            load_latest_trade_date, reset_stock_data_stage_table,
        },
        load_stock_list, load_trade_date_list, source_db_path, stock_list_path,
        DataReader,
        trade_calendar_path,
    },
    download::{
        AdjType, BarFreq, DownloadSummary, DownloadTask, PreparedDownloadBatch,
        PreparedStockDownload, ProBarRow, TushareClient,
        ind_calc::{IndsCache, cache_ind_build, calc_increment_one_stock_inds, warmup_ind_estimate},
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

struct PendingTradeDateWrite {
    trade_date: String,
    passed_rows: Vec<ProBarRow>,
    passed_indicator_updates: Vec<PendingIndicatorRows>,
    recovered_prepared_items: Vec<PreparedStockDownload>,
}

struct PendingIndicatorRows {
    ts_code: String,
    rows: Vec<ProBarRow>,
    indicators: HashMap<String, Vec<Option<f64>>>,
}

fn build_trade_date_indicator_matrix(
    rows: &[ProBarRow],
    indicator_updates: &[PendingIndicatorRows],
) -> Result<HashMap<String, Vec<Option<f64>>>, String> {
    if rows.is_empty() {
        return Ok(HashMap::new());
    }

    let mut indicator_names = HashSet::new();
    let mut per_stock = HashMap::<String, HashMap<String, Option<f64>>>::new();

    for item in indicator_updates {
        if item.rows.len() != 1 {
            return Err(format!(
                "按交易日构建指标矩阵时发现非单行更新: ts_code={}",
                item.ts_code
            ));
        }

        let mut row_map = HashMap::new();
        for (name, series) in &item.indicators {
            if series.len() != 1 {
                return Err(format!(
                    "按交易日构建指标矩阵时长度不为1: ts_code={}, indicator={}, len={}",
                    item.ts_code,
                    name,
                    series.len()
                ));
            }
            indicator_names.insert(name.clone());
            row_map.insert(name.clone(), series[0]);
        }
        per_stock.insert(item.ts_code.clone(), row_map);
    }

    let mut ordered_names = indicator_names.into_iter().collect::<Vec<_>>();
    ordered_names.sort();

    let mut out = HashMap::with_capacity(ordered_names.len());
    for name in ordered_names {
        let mut series = Vec::with_capacity(rows.len());
        for row in rows {
            let value = per_stock
                .get(&row.ts_code)
                .and_then(|item| item.get(&name))
                .copied()
                .flatten();
            series.push(value);
        }
        out.insert(name, series);
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
)-> Result<PreparedDownloadBatch, String> {
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
    let mut total = DownloadSummary::default();
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

    let tasks = build_download_task(&ts_codes, start_date, end_date, adj_type, with_factors);
    let total_tasks = tasks.len();
    let mut processed_tasks = 0usize;
    emit_progress(
        progress_cb,
        "download_bars",
        0,
        total_tasks,
        None,
        format!("首次全量下载开始，共 {} 只股票待处理。", total_tasks),
    );

    for (batch_idx, batch) in tasks.chunks(download_config.threads.max(1)).enumerate() {
        let prepared_batch = pool.install(|| client.prepare_stock_downloads(source_dir, batch));
        let batch_summary = prepared_batch.summary();
        if !indicator_columns_ready && !prepared_batch.prepared_items.is_empty() {
            let indicator_names = collect_indicator_names(&prepared_batch.prepared_items);
            ensure_indicator_columns(&conn, &indicator_names)?;
            indicator_columns_ready = true;
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
        write_prepared_stock_batch(&conn, &prepared_batch.prepared_items)?;
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

    if !total.failed_items.is_empty() && download_config.retry_times > 0 {
        let failed_tasks = keep_failed_tasks(tasks, &total.failed_items);
        let retry_task_count = failed_tasks.len();
        emit_progress(
            progress_cb,
            "retry_failed",
            0,
            download_config.retry_times.max(1),
            Some(format!("待重试 {} 只", retry_task_count)),
            format!("共有 {} 只股票失败，准备进入重试阶段。", retry_task_count),
        );
        let retry_batch = retry_failed_downloads(
            &client,
            source_dir,
            failed_tasks,
            download_config.retry_times,
            &pool,
            progress_cb,
        );
        let retry_summary = retry_batch.summary();
        if !indicator_columns_ready && !retry_batch.prepared_items.is_empty() {
            let indicator_names = collect_indicator_names(&retry_batch.prepared_items);
            ensure_indicator_columns(&conn, &indicator_names)?;
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
        write_prepared_stock_batch(&conn, &retry_batch.prepared_items)?;

        total.success_count += retry_summary.success_count;
        total.saved_rows += retry_summary.saved_rows;
        total.failed_count = retry_summary.failed_count;
        total.failed_items = retry_summary.failed_items;
    }

    checkpoint_stock_data(&conn)?;

    emit_progress(
        progress_cb,
        "done",
        total_tasks,
        total_tasks,
        Some(effective_trade_date),
        format!(
            "首次全量下载结束，成功 {} 只，失败 {} 只。",
            total.success_count, total.failed_count
        ),
    );

    Ok(total)
}

// 增量部分

fn validate_market_daily_rows(
    rows: Vec<ProBarRow>,
    latest_map: &HashMap<String, LatestCloseRow>,
) -> (Vec<ProBarRow>, Vec<(String, String)>) {
    let mut passed_rows = Vec::with_capacity(rows.len());
    let mut failed_items = Vec::new();

    for row in rows {
        match latest_map.get(&row.ts_code) {
            Some(latest) => {
                if price_equal(latest.close, row.pre_close) {
                    passed_rows.push(row);
                } else {
                    failed_items.push((
                        row.ts_code.clone(),
                        format!(
                            "trade_date={} pre_close校验失败: db_latest_date={}, db_close={}, daily_pre_close={}",
                            row.trade_date,
                            latest.trade_date,
                            latest.close,
                            row.pre_close
                        ),
                    ));
                }
            }
            None => {
                passed_rows.push(row);
            }
        }
    }

    (passed_rows, failed_items)
}

fn update_latest_close_map(
    latest_map: &mut HashMap<String, LatestCloseRow>,
    passed_rows: &[ProBarRow],
) {
    for row in passed_rows {
        latest_map.insert(
            row.ts_code.clone(),
            LatestCloseRow {
                ts_code: row.ts_code.clone(),
                trade_date: row.trade_date.clone(),
                close: row.close,
            },
        );
    }
}

fn calc_passed_indicator_updates(
    pool: &ThreadPool,
    source_dir: &str,
    inds_cache: &[IndsCache],
    warmup_need: usize,
    latest_map: &HashMap<String, LatestCloseRow>,
    passed_rows: &[ProBarRow],
) -> Result<Vec<PendingIndicatorRows>, String> {
    if passed_rows.is_empty() {
        return Ok(Vec::new());
    }

    let chunk_results = pool.install(|| {
        passed_rows
            .par_chunks(INCREMENTAL_INDICATOR_CHUNK_SIZE)
            .map(|chunk| -> Result<Vec<PendingIndicatorRows>, String> {
                let dr = DataReader::new(source_dir)?;
                let mut chunk_out = Vec::with_capacity(chunk.len());

                for row in chunk {
                    let history_end_date = latest_map
                        .get(&row.ts_code)
                        .map(|latest| latest.trade_date.as_str());
                    let indicators = calc_increment_one_stock_inds(
                        &dr,
                        inds_cache,
                        warmup_need,
                        row.ts_code.as_str(),
                        "qfq",
                        history_end_date,
                        std::slice::from_ref(row),
                    )?;
                    chunk_out.push(PendingIndicatorRows {
                        ts_code: row.ts_code.clone(),
                        rows: vec![row.clone()],
                        indicators,
                    });
                }

                Ok(chunk_out)
            })
            .collect::<Vec<_>>()
    });

    let mut out = Vec::with_capacity(passed_rows.len());
    for chunk_result in chunk_results {
        out.extend(chunk_result?);
    }

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
    let mut latest_map =
        load_latest_close_map_before(source_dir, "qfq", pending_trade_dates[0].as_str())?;
    let mut total = DownloadSummary::default();
    let mut pending_writes = Vec::with_capacity(pending_trade_dates.len());

    for (trade_date_idx, trade_date) in pending_trade_dates.into_iter().enumerate() {
        emit_progress(
            progress_cb,
            "download_pending_trade_dates",
            trade_date_idx,
            total_trade_dates,
            Some(format!("{trade_date} · 拉取全市场行情")),
            format!("正在拉取交易日 {} 的全市场行情。", trade_date),
        );
        let rows = client.fetch_market_daily(trade_date.as_str(), with_factors)?;
        let market_rows_len = rows.len();
        let (passed_rows, failed_items) = validate_market_daily_rows(rows, &latest_map);
        emit_progress(
            progress_cb,
            "download_pending_trade_dates",
            trade_date_idx,
            total_trade_dates,
            Some(format!("{trade_date} · 计算通过校验股票指标")),
            format!(
                "交易日 {} 已拿到 {} 条行情，正在计算 {} 只通过校验股票的增量指标。",
                trade_date,
                market_rows_len,
                passed_rows.len()
            ),
        );
        let passed_indicator_updates = calc_passed_indicator_updates(
            &pool,
            source_dir,
            &inds_cache,
            warmup_need,
            &latest_map,
            &passed_rows,
        )?;

        let recovered_batch = if failed_items.is_empty() {
            PreparedDownloadBatch::default()
        } else {
            emit_progress(
                progress_cb,
                "download_pending_trade_dates",
                trade_date_idx,
                total_trade_dates,
                Some(format!("{trade_date} · 补救重下失败股票")),
                format!(
                    "交易日 {} 有 {} 只股票未通过 pre_close 校验，正在逐股补救重下。",
                    trade_date,
                    failed_items.len()
                ),
            );
            redownload_failed_stocks(
                &client,
                source_dir,
                &failed_items,
                start_date,
                trade_date.as_str(),
                adj_type,
                with_factors,
                &pool,
            )?
        };
        emit_progress(
            progress_cb,
            "download_pending_trade_dates",
            trade_date_idx,
            total_trade_dates,
            Some(format!("{trade_date} · 整理补救股票写入数据")),
            format!(
                "交易日 {} 补救重下完成，已准备 {} 只补救股票的整段写入数据。",
                trade_date,
                recovered_batch.prepared_items.len()
            ),
        );

        let recovered_history_rows = recovered_batch
            .prepared_items
            .iter()
            .map(|item| item.rows.len())
            .sum::<usize>();

        if !passed_rows.is_empty() {
            update_latest_close_map(&mut latest_map, &passed_rows);
        }
        for item in &recovered_batch.prepared_items {
            if let Some(last_row) = item.rows.last() {
                update_latest_close_map(&mut latest_map, std::slice::from_ref(last_row));
            }
        }

        if !passed_rows.is_empty() || !recovered_batch.prepared_items.is_empty() {
            pending_writes.push(PendingTradeDateWrite {
                trade_date: trade_date.clone(),
                passed_rows,
                passed_indicator_updates,
                recovered_prepared_items: recovered_batch.prepared_items,
            });
            total.success_count += market_rows_len;
            total.saved_rows += market_rows_len + recovered_history_rows;
        }
        emit_progress(
            progress_cb,
            "download_pending_trade_dates",
            trade_date_idx + 1,
            total_trade_dates,
            Some(trade_date.clone()),
            format!(
                "增量交易日 {} 已处理完成，进度 {}/{}。",
                trade_date,
                trade_date_idx + 1,
                total_trade_dates
            ),
        );
    }

    if pending_writes.is_empty() {
        return Ok(total);
    }

    let db_path = source_db_path(source_dir);
    let db_path_str = db_path
        .to_str()
        .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?;
    let conn = Connection::open(db_path_str).map_err(|e| format!("数据库连接错误:{e}"))?;
    emit_progress(
        progress_cb,
        "write_db",
        0,
        pending_writes.len(),
        None,
        format!(
            "增量下载完成，准备写入 {} 个交易日的数据。",
            pending_writes.len()
        ),
    );

    let mut written_trade_dates = 0usize;
    with_transaction(&conn, |tx| {
        ensure_indicator_columns(tx, &indicator_names)?;
        reset_stock_data_stage_table(tx)?;

        for pending in &pending_writes {
            if !pending.passed_rows.is_empty() {
                let passed_indicator_matrix = build_trade_date_indicator_matrix(
                    &pending.passed_rows,
                    &pending.passed_indicator_updates,
                )?;
                delete_trade_date_rows(tx, adj_type, pending.trade_date.as_str())?;
                append_stage_pro_bar_rows(
                    tx,
                    adj_type,
                    &pending.passed_rows,
                    &passed_indicator_matrix,
                )?;
            }

            for item in &pending.recovered_prepared_items {
                delete_one_stock_range(
                    tx,
                    item.ts_code.as_str(),
                    adj_type,
                    start_date,
                    pending.trade_date.as_str(),
                )?;
                append_stage_pro_bar_rows(tx, adj_type, &item.rows, &item.indicators)?;
            }

            written_trade_dates += 1;
            emit_progress(
                progress_cb,
                "write_db",
                written_trade_dates,
                pending_writes.len(),
                Some(pending.trade_date.clone()),
                format!(
                    "已写入 {}/{} 个交易日，当前 {}。",
                    written_trade_dates,
                    pending_writes.len(),
                    pending.trade_date
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
            "增量更新完成，共处理 {} 个交易日，写入 {} 行。",
            total_trade_dates, total.saved_rows
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
