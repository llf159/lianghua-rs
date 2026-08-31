use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
    sync::mpsc::{SyncSender, sync_channel},
    thread,
};

use chrono::{Local, Timelike};
use duckdb::{Connection, params};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    data::{
        DataReader, IndsData,
        concept_performance_data::{
            rebuild_concept_performance_all, rebuild_most_related_concept_csv,
        },
        concept_performance_db_path,
        cyq_chen_data::{
            maintain_cyq_chen_incremental_if_db_exists, query_cyq_chen_strategy_maintenance_status,
            repair_cyq_chen_stocks_if_db_exists,
        },
        cyq_data::{maintain_cyq_incremental_if_db_exists, repair_cyq_stocks_if_db_exists},
        download_data::{
            append_stock_data_indicator_stage_rows_with_appender,
            create_stock_data_indicator_stage_appender_for_columns, drop_stock_data_columns,
            ensure_indicator_columns, flush_stock_data_indicator_stage_table,
            list_stock_data_indicator_columns, reset_stock_data_indicator_stage_table,
        },
        dragon_tiger_db_path, ind_toml_path, load_stock_list, load_ths_concepts_list,
        load_trade_date_list, source_db_path, stock_list_path, ths_concepts_path,
        trade_calendar_path,
    },
    expr::validation::{parse_expression_program, validate_expression_functions},
};

use lianghua_app_shared::normalize_trade_date;
use lianghua_download::download::{
    AdjType, DownloadSummary,
    dragon_tiger::{
        DragonTigerDownloadConfig, download_dragon_tiger as core_download_dragon_tiger,
    },
    ind_calc::{cache_ind_build, calc_inds_with_cache},
    runner::{
        DownloadProgress, DownloadProgressCallback, DownloadRuntimeConfig,
        ThsConceptDownloadConfig, download_after_basic_data as core_run_download_with_progress,
        download_indices_after_basic_data as core_run_index_download_with_progress,
        download_selected_stocks as core_run_selected_stock_download_with_progress,
        download_ths_concepts as core_download_ths_concepts, init_stock_basic_data,
    },
};

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DataDownloadDbRange {
    pub file_name: String,
    pub table_name: String,
    pub exists: bool,
    pub min_trade_date: Option<String>,
    pub max_trade_date: Option<String>,
    pub distinct_trade_dates: u64,
    pub row_count: u64,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DataDownloadFileStatus {
    pub file_name: String,
    pub exists: bool,
    pub row_count: u64,
    pub min_trade_date: Option<String>,
    pub max_trade_date: Option<String>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DragonTigerDbStatus {
    pub file_name: String,
    pub exists: bool,
    pub min_trade_date: Option<String>,
    pub max_trade_date: Option<String>,
    pub synced_trade_dates: u64,
    pub top_list_rows: u64,
    pub top_inst_rows: u64,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DataDownloadStatus {
    pub source_path: String,
    pub source_db: DataDownloadDbRange,
    pub concept_performance_db: DataDownloadDbRange,
    pub dragon_tiger_db: DragonTigerDbStatus,
    pub stock_list: DataDownloadFileStatus,
    pub trade_calendar: DataDownloadFileStatus,
    pub ths_concepts: DataDownloadFileStatus,
    pub missing_stock_repair: DataDownloadMissingStockRepairStatus,
    pub cyq_chen_maintenance: DataDownloadCyqChenMaintenanceStatus,
    pub daily_target_trade_date: Option<String>,
    pub planned_action: String,
    pub planned_action_label: String,
    pub planned_action_detail: String,
}

fn resolve_daily_target_trade_date(
    trade_dates: &[String],
    today: &str,
    current_hhmm: u32,
) -> Option<String> {
    let today_is_trade_date = trade_dates.iter().any(|trade_date| trade_date == today);
    if today_is_trade_date && current_hhmm >= 1600 {
        return Some(today.to_string());
    }

    trade_dates
        .iter()
        .rev()
        .find(|trade_date| trade_date.as_str() < today)
        .cloned()
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DataDownloadRunInput {
    pub source_path: String,
    pub token: String,
    pub start_date: String,
    pub end_date: String,
    pub threads: usize,
    pub retry_times: usize,
    pub limit_calls_per_min: usize,
    pub include_turnover: bool,
    pub allow_stale_stock_list: bool,
    pub allow_cyq_chen_strategy_rebuild: bool,
    pub chip_model: Option<String>,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MissingStockRepairRunInput {
    pub source_path: String,
    pub token: String,
    pub threads: usize,
    pub retry_times: usize,
    pub limit_calls_per_min: usize,
    pub include_turnover: bool,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DragonTigerDownloadRunInput {
    pub source_path: String,
    pub token: String,
    pub start_date: String,
    pub end_date: String,
    pub retry_times: usize,
    pub limit_calls_per_min: usize,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThsConceptDownloadRunInput {
    pub source_path: String,
    pub retry_enabled: bool,
    pub retry_times: usize,
    pub retry_interval_secs: u64,
    pub concurrent_enabled: bool,
    pub worker_threads: usize,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConceptPerformanceRepairRunInput {
    pub source_path: String,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConceptMostRelatedRepairRunInput {
    pub source_path: String,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StockDataIndicatorColumnsDeleteRunInput {
    pub source_path: String,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StockDataIndicatorColumnsRebuildRunInput {
    pub source_path: String,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DataDownloadSummary {
    pub success_count: u64,
    pub failed_count: u64,
    pub saved_rows: u64,
    pub concept_performance_rows: u64,
    pub failed_items: Vec<String>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DataDownloadRunResult {
    pub action: String,
    pub action_label: String,
    pub elapsed_ms: u64,
    pub summary: DataDownloadSummary,
    pub completion_details: Vec<String>,
    pub status: DataDownloadStatus,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DataDownloadMissingStockRepairStatus {
    pub ready: bool,
    pub missing_count: u64,
    pub missing_samples: Vec<String>,
    pub suggested_start_date: Option<String>,
    pub suggested_end_date: Option<String>,
    pub detail: String,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DataDownloadCyqChenMaintenanceStatus {
    pub db_exists: bool,
    pub has_data: bool,
    pub strategy_changed: bool,
    pub detail: String,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndicatorManageItem {
    pub index: usize,
    pub name: String,
    pub expr: String,
    pub prec: usize,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndicatorManagePageData {
    pub exists: bool,
    pub file_path: String,
    pub items: Vec<IndicatorManageItem>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndicatorManageDraft {
    pub name: String,
    pub expr: String,
    pub prec: usize,
}

#[derive(Serialize)]
struct IndicatorManageFile {
    version: u32,
    ind: Vec<IndicatorManageFileItem>,
}

#[derive(Serialize)]
struct IndicatorManageFileItem {
    name: String,
    expr: String,
    prec: usize,
}

#[derive(Clone)]
pub struct PreparedDataDownloadRun {
    pub source_path: String,
    pub token: String,
    pub start_date: String,
    pub end_date: String,
    pub threads: usize,
    pub retry_times: usize,
    pub limit_calls_per_min: usize,
    pub include_turnover: bool,
    pub allow_stale_stock_list: bool,
    pub allow_cyq_chen_strategy_rebuild: bool,
    pub chip_model: String,
    pub action: String,
    pub action_label: String,
}

#[derive(Clone)]
pub struct PreparedMissingStockRepairRun {
    pub source_path: String,
    pub token: String,
    pub start_date: String,
    pub end_date: String,
    pub threads: usize,
    pub retry_times: usize,
    pub limit_calls_per_min: usize,
    pub include_turnover: bool,
    pub action: String,
    pub action_label: String,
    pub missing_ts_codes: Vec<String>,
}

#[derive(Clone)]
pub struct PreparedDragonTigerDownloadRun {
    pub source_path: String,
    pub token: String,
    pub start_date: String,
    pub end_date: String,
    pub retry_times: usize,
    pub limit_calls_per_min: usize,
    pub action: String,
    pub action_label: String,
}

#[derive(Clone)]
pub struct PreparedThsConceptDownloadRun {
    pub source_path: String,
    pub retry_enabled: bool,
    pub retry_times: usize,
    pub retry_interval_secs: u64,
    pub concurrent_enabled: bool,
    pub worker_threads: usize,
    pub action: String,
    pub action_label: String,
}

#[derive(Clone)]
pub struct PreparedConceptPerformanceRepairRun {
    pub source_path: String,
    pub action: String,
    pub action_label: String,
}

#[derive(Clone)]
pub struct PreparedConceptMostRelatedRepairRun {
    pub source_path: String,
    pub action: String,
    pub action_label: String,
}

#[derive(Clone)]
pub struct PreparedStockDataIndicatorColumnsDeleteRun {
    pub source_path: String,
    pub action: String,
    pub action_label: String,
}

#[derive(Clone)]
pub struct PreparedStockDataIndicatorColumnsRebuildRun {
    pub source_path: String,
    pub action: String,
    pub action_label: String,
}

fn normalize_download_date(raw: &str, field_name: &str) -> Result<String, String> {
    normalize_trade_date(raw)
        .ok_or_else(|| format!("{field_name} 格式无效，应为 YYYYMMDD 或 YYYY-MM-DD"))
}

fn normalize_download_end_date(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("today") {
        return Ok("today".to_string());
    }

    normalize_download_date(trimmed, "结束日期")
}

#[derive(Clone)]
struct StockDataIndicatorWorkItem {
    ts_code: String,
    adj_type: String,
    start_date: String,
    end_date: String,
    row_count: u64,
}

struct StockDataIndicatorRebuildBatch {
    ts_code: String,
    adj_type: AdjType,
    adj_type_label: String,
    row_count: u64,
    trade_dates: Vec<String>,
    indicators: HashMap<String, Vec<Option<f64>>>,
}

enum StockDataIndicatorRebuildMessage {
    Batch(StockDataIndicatorRebuildBatch),
    Abort(String),
    Done,
}

#[derive(Clone, Copy)]
enum DataDownloadNestedProgressScope {
    Stock,
    Index,
}

fn normalize_nested_data_download_progress(
    scope: DataDownloadNestedProgressScope,
    mut progress: DownloadProgress,
) -> DownloadProgress {
    match scope {
        DataDownloadNestedProgressScope::Stock => match progress.phase.as_str() {
            "rebuild_concept_performance" => {
                progress.phase = "rebuild_incremental_concept_performance".to_string();
            }
            "done" => {
                progress.phase = "stock_download_done".to_string();
                progress.finished = 1;
                progress.total = 1;
                progress.current_label = Some("股票行情".to_string());
                progress.message = "股票行情下载阶段完成，准备下载指数行情。".to_string();
            }
            _ => {}
        },
        DataDownloadNestedProgressScope::Index => match progress.phase.as_str() {
            "prepare_trade_calendar" | "prepare_index_list" => {
                progress.phase = "prepare_index_download".to_string();
                progress.current_label.get_or_insert("指数行情".to_string());
            }
            "write_db" => {
                progress.phase = "write_index_db".to_string();
            }
            "done" => {
                progress.phase = "index_download_done".to_string();
                progress.finished = 1;
                progress.total = 1;
                progress.current_label = Some("指数行情".to_string());
                progress.message = "指数行情下载阶段完成，准备维护概念/行业表现库。".to_string();
            }
            _ => {}
        },
    }

    progress
}

fn emit_nested_data_download_progress(
    progress_cb: Option<&DownloadProgressCallback<'_>>,
    scope: DataDownloadNestedProgressScope,
    progress: DownloadProgress,
) {
    if let Some(cb) = progress_cb {
        cb(normalize_nested_data_download_progress(scope, progress));
    }
}

fn emit_chip_maintenance_progress(
    progress_cb: Option<&DownloadProgressCallback<'_>>,
    mut progress: DownloadProgress,
) {
    if let Some(cb) = progress_cb {
        progress.phase = "maintain_cyq_incremental".to_string();
        cb(progress);
    }
}

fn hide_chip_repair_local_counter(mut progress: DownloadProgress) -> DownloadProgress {
    let repair_done = progress.total > 0 && progress.finished >= progress.total;
    progress.finished = 0;
    progress.total = 0;
    progress.message = if repair_done {
        "断点股票的筹码局部修复已完成，正在衔接其余增量股票。".to_string()
    } else if let Some(ts_code) = progress.current_label.as_deref() {
        format!("正在修复断点股票 {ts_code} 的筹码数据，随后继续维护其余增量股票。")
    } else {
        "正在修复本轮断点股票的筹码数据，随后继续维护其余增量股票。".to_string()
    };
    progress
}

fn merge_chip_repair_into_incremental_progress(
    mut progress: DownloadProgress,
    repaired_stock_count: usize,
) -> DownloadProgress {
    if repaired_stock_count == 0 || progress.total == 0 {
        return progress;
    }

    let incremental_finished = progress.finished.min(progress.total);
    let incremental_total = progress.total;
    progress.finished = repaired_stock_count.saturating_add(incremental_finished);
    progress.total = repaired_stock_count.saturating_add(incremental_total);
    progress.message = format!(
        "断点股票已修复 {repaired_stock_count} 只；其余增量维护已完成 \
         {incremental_finished} / {incremental_total} 只，总进度 {} / {}。",
        progress.finished, progress.total
    );
    progress
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

fn open_source_db_conn(source_path: &str) -> Result<Connection, String> {
    let db_path = source_db_path(source_path);
    let db_path_str = db_path
        .to_str()
        .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?;
    Connection::open(db_path_str).map_err(|e| format!("数据库连接错误:{e}"))
}

fn list_stock_data_indicator_work_items(
    conn: &Connection,
) -> Result<Vec<StockDataIndicatorWorkItem>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT ts_code, adj_type, MIN(trade_date), MAX(trade_date), COUNT(*)
            FROM stock_data
            GROUP BY ts_code, adj_type
            ORDER BY adj_type ASC, ts_code ASC
            "#,
        )
        .map_err(|e| format!("预编译指标补算分组SQL失败:{e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询指标补算分组失败:{e}"))?;
    let mut items = Vec::new();

    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取指标补算分组失败:{e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取ts_code失败:{e}"))?;
        let adj_type: String = row.get(1).map_err(|e| format!("读取adj_type失败:{e}"))?;
        let start_date: String = row
            .get(2)
            .map_err(|e| format!("读取最早trade_date失败:{e}"))?;
        let end_date: String = row
            .get(3)
            .map_err(|e| format!("读取最晚trade_date失败:{e}"))?;
        let row_count: i64 = row.get(4).map_err(|e| format!("读取行情数量失败:{e}"))?;
        items.push(StockDataIndicatorWorkItem {
            ts_code,
            adj_type,
            start_date,
            end_date,
            row_count: row_count.max(0) as u64,
        });
    }

    Ok(items)
}

fn compute_stock_data_indicator_rebuild_batch(
    sender: &SyncSender<StockDataIndicatorRebuildMessage>,
    source_path: &str,
    inds_cache: &[lianghua_download::download::ind_calc::IndsCache],
    work_group: &[StockDataIndicatorWorkItem],
) -> Result<(), String> {
    let reader = DataReader::new(source_path)?;

    for item in work_group {
        let row_data = reader.load_one(
            item.ts_code.as_str(),
            item.adj_type.as_str(),
            item.start_date.as_str(),
            item.end_date.as_str(),
        )?;
        if row_data.trade_dates.is_empty() {
            continue;
        }
        if row_data.trade_dates.len() as u64 != item.row_count {
            return Err(format!(
                "指标补算读取行数变化: {} / {}, {} != {}",
                item.ts_code,
                item.adj_type,
                row_data.trade_dates.len(),
                item.row_count
            ));
        }

        let trade_dates = row_data.trade_dates.clone();
        let indicators = calc_inds_with_cache(inds_cache, row_data)?;
        let adj_type = (|raw: &str| -> Result<AdjType, String> {
            match raw.trim().to_ascii_lowercase().as_str() {
                "qfq" => Ok(AdjType::Qfq),
                "hfq" => Ok(AdjType::Hfq),
                "raw" => Ok(AdjType::Raw),
                "ind" => Ok(AdjType::Ind),
                _ => Err(format!("不支持的adj_type: {raw}")),
            }
        })(item.adj_type.as_str())?;

        sender
            .send(StockDataIndicatorRebuildMessage::Batch(
                StockDataIndicatorRebuildBatch {
                    ts_code: item.ts_code.clone(),
                    adj_type,
                    adj_type_label: item.adj_type.clone(),
                    row_count: item.row_count,
                    trade_dates,
                    indicators,
                },
            ))
            .map_err(|e| {
                format!(
                    "发送指标补算批次失败:{} / {}: {e}",
                    item.ts_code, item.adj_type
                )
            })?;
    }

    Ok(())
}

fn query_trade_date_range(
    db_path: &Path,
    file_name: &str,
    table_name: &str,
) -> Result<DataDownloadDbRange, String> {
    if !db_path.exists() {
        return Ok(DataDownloadDbRange {
            file_name: file_name.to_string(),
            table_name: table_name.to_string(),
            exists: false,
            min_trade_date: None,
            max_trade_date: None,
            distinct_trade_dates: 0,
            row_count: 0,
        });
    }

    let db_path_str = db_path
        .to_str()
        .ok_or_else(|| format!("{file_name} 路径不是有效 UTF-8"))?;
    let conn = Connection::open(db_path_str).map_err(|e| format!("打开 {file_name} 失败: {e}"))?;
    let table_exists = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| format!("检查 {file_name} 表结构失败: {e}"))?;
    if table_exists <= 0 {
        return Ok(DataDownloadDbRange {
            file_name: file_name.to_string(),
            table_name: table_name.to_string(),
            exists: true,
            min_trade_date: None,
            max_trade_date: None,
            distinct_trade_dates: 0,
            row_count: 0,
        });
    }

    let sql = format!(
        "SELECT MIN(trade_date), MAX(trade_date), COUNT(DISTINCT trade_date), COUNT(*) FROM {table_name}"
    );
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("查询 {file_name} 日期范围失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("读取 {file_name} 日期范围失败: {e}"))?;

    if let Some(row) = rows
        .next()
        .map_err(|e| format!("读取 {file_name} 日期范围行失败: {e}"))?
    {
        let min_trade_date: Option<String> = row
            .get(0)
            .map_err(|e| format!("读取 {file_name} 最小日期失败: {e}"))?;
        let max_trade_date: Option<String> = row
            .get(1)
            .map_err(|e| format!("读取 {file_name} 最大日期失败: {e}"))?;
        let distinct_trade_dates_i64: i64 = row
            .get(2)
            .map_err(|e| format!("读取 {file_name} 交易日数量失败: {e}"))?;
        let row_count_i64: i64 = row
            .get(3)
            .map_err(|e| format!("读取 {file_name} 行数失败: {e}"))?;
        return Ok(DataDownloadDbRange {
            file_name: file_name.to_string(),
            table_name: table_name.to_string(),
            exists: true,
            min_trade_date,
            max_trade_date,
            distinct_trade_dates: distinct_trade_dates_i64.max(0) as u64,
            row_count: row_count_i64.max(0) as u64,
        });
    }

    Ok(DataDownloadDbRange {
        file_name: file_name.to_string(),
        table_name: table_name.to_string(),
        exists: true,
        min_trade_date: None,
        max_trade_date: None,
        distinct_trade_dates: 0,
        row_count: 0,
    })
}

fn query_stock_data_adj_type_range(
    source_path: &str,
    adj_type: &str,
) -> Result<DataDownloadDbRange, String> {
    let db_path = source_db_path(source_path);
    if !db_path.exists() {
        return Ok(DataDownloadDbRange {
            file_name: "stock_data.db".to_string(),
            table_name: "stock_data".to_string(),
            exists: false,
            min_trade_date: None,
            max_trade_date: None,
            distinct_trade_dates: 0,
            row_count: 0,
        });
    }

    let db_path_str = db_path
        .to_str()
        .ok_or_else(|| "stock_data.db 路径不是有效 UTF-8".to_string())?;
    let conn =
        Connection::open(db_path_str).map_err(|e| format!("打开 stock_data.db 失败: {e}"))?;
    let table_exists = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'stock_data'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| format!("检查 stock_data.db 表结构失败: {e}"))?;
    if table_exists <= 0 {
        return Ok(DataDownloadDbRange {
            file_name: "stock_data.db".to_string(),
            table_name: "stock_data".to_string(),
            exists: true,
            min_trade_date: None,
            max_trade_date: None,
            distinct_trade_dates: 0,
            row_count: 0,
        });
    }

    let mut stmt = conn
        .prepare(
            r#"
            SELECT MIN(trade_date), MAX(trade_date), COUNT(DISTINCT trade_date), COUNT(*)
            FROM stock_data
            WHERE adj_type = ?
            "#,
        )
        .map_err(|e| format!("查询 stock_data.db {adj_type} 日期范围失败: {e}"))?;
    let mut rows = stmt
        .query(params![adj_type])
        .map_err(|e| format!("读取 stock_data.db {adj_type} 日期范围失败: {e}"))?;

    if let Some(row) = rows
        .next()
        .map_err(|e| format!("读取 stock_data.db {adj_type} 日期范围行失败: {e}"))?
    {
        let min_trade_date: Option<String> = row
            .get(0)
            .map_err(|e| format!("读取 stock_data.db {adj_type} 最小日期失败: {e}"))?;
        let max_trade_date: Option<String> = row
            .get(1)
            .map_err(|e| format!("读取 stock_data.db {adj_type} 最大日期失败: {e}"))?;
        let distinct_trade_dates_i64: i64 = row
            .get(2)
            .map_err(|e| format!("读取 stock_data.db {adj_type} 交易日数量失败: {e}"))?;
        let row_count_i64: i64 = row
            .get(3)
            .map_err(|e| format!("读取 stock_data.db {adj_type} 行数失败: {e}"))?;

        return Ok(DataDownloadDbRange {
            file_name: "stock_data.db".to_string(),
            table_name: "stock_data".to_string(),
            exists: true,
            min_trade_date,
            max_trade_date,
            distinct_trade_dates: distinct_trade_dates_i64.max(0) as u64,
            row_count: row_count_i64.max(0) as u64,
        });
    }

    Ok(DataDownloadDbRange {
        file_name: "stock_data.db".to_string(),
        table_name: "stock_data".to_string(),
        exists: true,
        min_trade_date: None,
        max_trade_date: None,
        distinct_trade_dates: 0,
        row_count: 0,
    })
}

fn query_dragon_tiger_db_status(source_path: &str) -> Result<DragonTigerDbStatus, String> {
    let db_path = dragon_tiger_db_path(source_path);
    if !db_path.exists() {
        return Ok(DragonTigerDbStatus {
            file_name: "dragon_tiger.db".to_string(),
            exists: false,
            min_trade_date: None,
            max_trade_date: None,
            synced_trade_dates: 0,
            top_list_rows: 0,
            top_inst_rows: 0,
        });
    }

    let conn = Connection::open(&db_path)
        .map_err(|error| format!("打开 dragon_tiger.db 失败: {error}"))?;
    let (min_trade_date, max_trade_date, synced_trade_dates): (
        Option<String>,
        Option<String>,
        i64,
    ) = conn
        .query_row(
            r#"
            SELECT MIN(trade_date), MAX(trade_date), COUNT(*)
            FROM dragon_tiger_sync_log
            "#,
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(|error| format!("查询龙虎榜同步范围失败: {error}"))?;
    let top_list_rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM top_list", [], |row| row.get(0))
        .map_err(|error| format!("查询龙虎榜每日明细行数失败: {error}"))?;
    let top_inst_rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM top_inst", [], |row| row.get(0))
        .map_err(|error| format!("查询龙虎榜席位明细行数失败: {error}"))?;

    Ok(DragonTigerDbStatus {
        file_name: "dragon_tiger.db".to_string(),
        exists: true,
        min_trade_date,
        max_trade_date,
        synced_trade_dates: synced_trade_dates.max(0) as u64,
        top_list_rows: top_list_rows.max(0) as u64,
        top_inst_rows: top_inst_rows.max(0) as u64,
    })
}

fn scan_missing_stock_codes(
    source_path: &str,
    source_db: &DataDownloadDbRange,
    stock_list: &DataDownloadFileStatus,
    _trade_calendar: &DataDownloadFileStatus,
) -> Result<(Vec<String>, DataDownloadMissingStockRepairStatus), String> {
    if !stock_list.exists || stock_list.row_count == 0 {
        return Ok((
            Vec::new(),
            DataDownloadMissingStockRepairStatus {
                ready: false,
                missing_count: 0,
                missing_samples: Vec::new(),
                suggested_start_date: None,
                suggested_end_date: None,
                detail: "股票列表不存在或为空，先刷新基础状态。".to_string(),
            },
        ));
    }

    if !source_db.exists || source_db.row_count == 0 {
        return Ok((
            Vec::new(),
            DataDownloadMissingStockRepairStatus {
                ready: false,
                missing_count: 0,
                missing_samples: Vec::new(),
                suggested_start_date: None,
                suggested_end_date: None,
                detail: "原始库为空，请直接执行首次全量下载。".to_string(),
            },
        ));
    }

    let list_codes: Vec<String> = load_stock_list(source_path)?
        .into_iter()
        .filter_map(|row| row.first().cloned())
        .filter(|value| !value.trim().is_empty())
        .collect();
    let existing_codes = (|source_path: &str| -> Result<HashSet<String>, String> {
        let db_path = source_db_path(source_path);
        if !db_path.exists() {
            return Ok(HashSet::new());
        }

        let db_path_str = db_path
            .to_str()
            .ok_or_else(|| "stock_data.db 路径不是有效 UTF-8".to_string())?;
        let conn =
            Connection::open(db_path_str).map_err(|e| format!("打开 stock_data.db 失败: {e}"))?;
        let table_exists = conn
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                ["stock_data"],
                |row| row.get::<_, i64>(0),
            )
            .map_err(|e| format!("检查 stock_data 表结构失败: {e}"))?;
        if table_exists <= 0 {
            return Ok(HashSet::new());
        }

        let mut stmt = conn
            .prepare(
                r#"
            SELECT DISTINCT ts_code
            FROM stock_data
            WHERE adj_type = ? AND ts_code IS NOT NULL AND TRIM(ts_code) <> ''
            "#,
            )
            .map_err(|e| format!("预编译现有股票代码查询失败: {e}"))?;
        let mut rows = stmt
            .query(["qfq"])
            .map_err(|e| format!("查询现有股票代码失败: {e}"))?;

        let mut out = HashSet::new();
        while let Some(row) = rows
            .next()
            .map_err(|e| format!("读取现有股票代码失败: {e}"))?
        {
            let ts_code: String = row.get(0).map_err(|e| format!("读取 ts_code 失败: {e}"))?;
            if !ts_code.trim().is_empty() {
                out.insert(ts_code);
            }
        }

        Ok(out)
    })(source_path)?;

    let mut missing_codes: Vec<String> = list_codes
        .into_iter()
        .filter(|ts_code| !existing_codes.contains(ts_code))
        .collect();
    missing_codes.sort();
    missing_codes.dedup();

    let detail = if missing_codes.is_empty() {
        "当前 stock_list.csv 中的股票都已在原始库里出现过，无需补全。".to_string()
    } else {
        format!(
            "将按当前原始库起始日期到当前原始库最新交易日，补全 {} 只完全缺失的股票。",
            missing_codes.len()
        )
    };

    Ok((
        missing_codes.clone(),
        DataDownloadMissingStockRepairStatus {
            ready: true,
            missing_count: missing_codes.len() as u64,
            missing_samples: missing_codes.into_iter().take(12).collect(),
            suggested_start_date: source_db.min_trade_date.clone(),
            suggested_end_date: source_db.max_trade_date.clone(),
            detail,
        },
    ))
}

fn build_data_download_summary(summary: DownloadSummary) -> DataDownloadSummary {
    DataDownloadSummary {
        success_count: summary.success_count as u64,
        failed_count: summary.failed_count as u64,
        saved_rows: summary.saved_rows as u64,
        concept_performance_rows: summary.concept_performance_rows as u64,
        failed_items: summary
            .failed_items
            .into_iter()
            .take(12)
            .map(|(ts_code, error)| format!("{ts_code}: {error}"))
            .collect(),
    }
}

fn concept_performance_completion_detail(rows: usize) -> Option<String> {
    if rows > 0 {
        Some(format!("概念表现写入 {rows} 行"))
    } else {
        None
    }
}

pub fn get_data_download_status(source_path: &str) -> Result<DataDownloadStatus, String> {
    let trimmed = source_path.trim();
    if trimmed.is_empty() {
        return Err("数据目录为空，请先到数据管理页确认当前目录".to_string());
    }

    let source_db =
        query_trade_date_range(&source_db_path(trimmed), "stock_data.db", "stock_data")?;
    let concept_performance_db = query_trade_date_range(
        &concept_performance_db_path(trimmed),
        "concept_performance.db",
        "concept_performance",
    )?;
    let dragon_tiger_db = query_dragon_tiger_db_status(trimmed)?;
    let trade_calendar = (|source_path: &str| -> Result<DataDownloadFileStatus, String> {
        let file_path = trade_calendar_path(source_path);
        if !file_path.exists() {
            return Ok(DataDownloadFileStatus {
                file_name: "trade_calendar.csv".to_string(),
                exists: false,
                row_count: 0,
                min_trade_date: None,
                max_trade_date: None,
            });
        }

        let trade_dates = load_trade_date_list(source_path)?;
        let min_trade_date = trade_dates.first().cloned();
        let max_trade_date = trade_dates.last().cloned();

        Ok(DataDownloadFileStatus {
            file_name: "trade_calendar.csv".to_string(),
            exists: true,
            row_count: trade_dates.len() as u64,
            min_trade_date,
            max_trade_date,
        })
    })(trimmed)?;
    let stock_list = (|source_path: &str| -> Result<DataDownloadFileStatus, String> {
        let file_path = stock_list_path(source_path);
        if !file_path.exists() {
            return Ok(DataDownloadFileStatus {
                file_name: "stock_list.csv".to_string(),
                exists: false,
                row_count: 0,
                min_trade_date: None,
                max_trade_date: None,
            });
        }

        let rows = load_stock_list(source_path)?;
        let mut min_trade_date: Option<String> = None;
        let mut max_trade_date: Option<String> = None;

        for cols in &rows {
            let Some(trade_date) = cols.get(6).map(|value| value.trim()) else {
                continue;
            };
            if trade_date.is_empty() {
                continue;
            }

            match min_trade_date.as_deref() {
                Some(current) if current <= trade_date => {}
                _ => min_trade_date = Some(trade_date.to_string()),
            }
            match max_trade_date.as_deref() {
                Some(current) if current >= trade_date => {}
                _ => max_trade_date = Some(trade_date.to_string()),
            }
        }

        Ok(DataDownloadFileStatus {
            file_name: "stock_list.csv".to_string(),
            exists: true,
            row_count: rows.len() as u64,
            min_trade_date,
            max_trade_date,
        })
    })(trimmed)?;
    let ths_concepts = (|source_path: &str| -> Result<DataDownloadFileStatus, String> {
        let file_path = ths_concepts_path(source_path);
        if !file_path.exists() {
            return Ok(DataDownloadFileStatus {
                file_name: "stock_concepts.csv".to_string(),
                exists: false,
                row_count: 0,
                min_trade_date: None,
                max_trade_date: None,
            });
        }

        let rows = load_ths_concepts_list(source_path)?;
        Ok(DataDownloadFileStatus {
            file_name: "stock_concepts.csv".to_string(),
            exists: true,
            row_count: rows.len() as u64,
            min_trade_date: None,
            max_trade_date: None,
        })
    })(trimmed)?;
    let (_, missing_stock_repair) =
        scan_missing_stock_codes(trimmed, &source_db, &stock_list, &trade_calendar)?;
    let cyq_chen_maintenance =
        query_cyq_chen_strategy_maintenance_status(trimmed).map(|status| {
            DataDownloadCyqChenMaintenanceStatus {
                db_exists: status.db_exists,
                has_data: status.has_data,
                strategy_changed: status.strategy_changed,
                detail: status.detail,
            }
        })?;
    let (planned_action, planned_action_label, planned_action_detail) =
        (|source_db: &DataDownloadDbRange| -> (String, String, String) {
            match source_db.max_trade_date.as_deref() {
                Some(max_trade_date) if source_db.row_count > 0 => (
                    "incremental-download".to_string(),
                    "增量更新下载".to_string(),
                    format!(
                        "将先刷新交易日历和股票列表，再从当前原始库最新日期 {} 之后继续补齐行情与指标。",
                        max_trade_date
                    ),
                ),
                _ => (
                    "first-download".to_string(),
                    "首次全量下载".to_string(),
                    "将先刷新交易日历和股票列表，再下载全市场历史行情与指标，并初始化原始库。"
                        .to_string(),
                ),
            }
        })(&source_db);
    let now = Local::now();
    let today = now.format("%Y%m%d").to_string();
    let current_hhmm = now.hour() * 100 + now.minute();
    let daily_target_trade_date = load_trade_date_list(trimmed).ok().and_then(|trade_dates| {
        resolve_daily_target_trade_date(&trade_dates, &today, current_hhmm)
    });

    Ok(DataDownloadStatus {
        source_path: trimmed.to_string(),
        source_db,
        concept_performance_db,
        dragon_tiger_db,
        stock_list,
        trade_calendar,
        ths_concepts,
        missing_stock_repair,
        cyq_chen_maintenance,
        daily_target_trade_date,
        planned_action,
        planned_action_label,
        planned_action_detail,
    })
}

pub fn prepare_missing_stock_repair_run(
    input: MissingStockRepairRunInput,
) -> Result<PreparedMissingStockRepairRun, String> {
    let source_path = input.source_path.trim().to_string();
    if source_path.is_empty() {
        return Err("数据目录为空，请先到数据管理页确认当前目录".to_string());
    }

    let token = input.token.trim().to_string();
    if token.is_empty() {
        return Err("Token 不能为空".to_string());
    }

    let status = get_data_download_status(&source_path)?;
    if !status.missing_stock_repair.ready {
        return Err(status.missing_stock_repair.detail);
    }
    if status.missing_stock_repair.missing_count == 0 {
        return Err("当前没有需要补全的缺失股票".to_string());
    }
    let start_date = status
        .missing_stock_repair
        .suggested_start_date
        .clone()
        .ok_or_else(|| "缺失股票补全缺少可用起始日期".to_string())?;
    let end_date = status
        .missing_stock_repair
        .suggested_end_date
        .clone()
        .ok_or_else(|| "缺失股票补全缺少可用结束日期".to_string())?;
    let (missing_ts_codes, _) = scan_missing_stock_codes(
        &source_path,
        &status.source_db,
        &status.stock_list,
        &status.trade_calendar,
    )?;

    Ok(PreparedMissingStockRepairRun {
        source_path,
        token,
        start_date,
        end_date,
        threads: input.threads.max(1),
        retry_times: input.retry_times,
        limit_calls_per_min: input.limit_calls_per_min.max(1),
        include_turnover: input.include_turnover,
        action: "repair-missing-stocks".to_string(),
        action_label: "缺失股票补全".to_string(),
        missing_ts_codes,
    })
}

pub fn prepare_data_download_run(
    input: DataDownloadRunInput,
) -> Result<PreparedDataDownloadRun, String> {
    let source_path = input.source_path.trim().to_string();
    if source_path.is_empty() {
        return Err("数据目录为空，请先到数据管理页确认当前目录".to_string());
    }

    let token = input.token.trim().to_string();
    if token.is_empty() {
        return Err("Token 不能为空".to_string());
    }

    let start_date = normalize_download_date(&input.start_date, "开始日期")?;
    let end_date = normalize_download_end_date(&input.end_date)?;
    if end_date != "today" && start_date > end_date {
        return Err("开始日期不能晚于结束日期".to_string());
    }

    let status = get_data_download_status(&source_path)?;

    Ok(PreparedDataDownloadRun {
        source_path,
        token,
        start_date,
        end_date,
        threads: input.threads.max(1),
        retry_times: input.retry_times,
        limit_calls_per_min: input.limit_calls_per_min.max(1),
        include_turnover: input.include_turnover,
        allow_stale_stock_list: input.allow_stale_stock_list,
        allow_cyq_chen_strategy_rebuild: input.allow_cyq_chen_strategy_rebuild,
        chip_model: (|raw: Option<&str>| -> String {
            match raw.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
                Some("chen") | Some("new") => "chen".to_string(),
                _ => "legacy".to_string(),
            }
        })(input.chip_model.as_deref()),
        action: status.planned_action,
        action_label: status.planned_action_label,
    })
}

pub fn prepare_dragon_tiger_download_run(
    input: DragonTigerDownloadRunInput,
) -> Result<PreparedDragonTigerDownloadRun, String> {
    let source_path = input.source_path.trim().to_string();
    if source_path.is_empty() {
        return Err("数据目录为空，请先到数据管理页确认当前目录".to_string());
    }
    let token = input.token.trim().to_string();
    if token.is_empty() {
        return Err("Token 不能为空".to_string());
    }
    let start_date = normalize_download_date(&input.start_date, "开始日期")?;
    let end_date = normalize_download_end_date(&input.end_date)?;
    if end_date != "today" && start_date > end_date {
        return Err("开始日期不能晚于结束日期".to_string());
    }
    let status = get_data_download_status(&source_path)?;
    if !status.trade_calendar.exists || status.trade_calendar.row_count == 0 {
        return Err("交易日历不存在或为空，请先完成基础数据刷新。".to_string());
    }

    Ok(PreparedDragonTigerDownloadRun {
        source_path,
        token,
        start_date,
        end_date,
        retry_times: input.retry_times,
        limit_calls_per_min: input.limit_calls_per_min.max(1),
        action: "download-dragon-tiger".to_string(),
        action_label: "龙虎榜下载".to_string(),
    })
}

pub fn prepare_ths_concept_download_run(
    input: ThsConceptDownloadRunInput,
) -> Result<PreparedThsConceptDownloadRun, String> {
    let source_path = input.source_path.trim().to_string();
    if source_path.is_empty() {
        return Err("数据目录为空，请先到数据管理页确认当前目录".to_string());
    }

    let status = get_data_download_status(&source_path)?;
    if !status.stock_list.exists || status.stock_list.row_count == 0 {
        return Err("股票列表不存在或为空，请先完成基础数据刷新。".to_string());
    }

    Ok(PreparedThsConceptDownloadRun {
        source_path,
        retry_enabled: input.retry_enabled,
        retry_times: input.retry_times,
        retry_interval_secs: input.retry_interval_secs,
        concurrent_enabled: input.concurrent_enabled,
        worker_threads: input.worker_threads.max(1),
        action: "download-ths-concepts".to_string(),
        action_label: "概念数据下载".to_string(),
    })
}

pub fn prepare_concept_performance_repair_run(
    input: ConceptPerformanceRepairRunInput,
) -> Result<PreparedConceptPerformanceRepairRun, String> {
    let source_path = input.source_path.trim().to_string();
    if source_path.is_empty() {
        return Err("数据目录为空，请先到数据管理页确认当前目录".to_string());
    }

    let status = get_data_download_status(&source_path)?;
    let source_qfq = query_stock_data_adj_type_range(&source_path, "qfq")?;
    if !source_qfq.exists || source_qfq.row_count == 0 {
        return Err("原始库不存在或为空，请先完成 qfq 行情下载。".to_string());
    }
    if !status.stock_list.exists || status.stock_list.row_count == 0 {
        return Err("股票列表不存在或为空，请先完成基础数据刷新。".to_string());
    }
    if !status.ths_concepts.exists || status.ths_concepts.row_count == 0 {
        return Err("概念文件不存在或为空，请先完成概念数据下载。".to_string());
    }

    Ok(PreparedConceptPerformanceRepairRun {
        source_path,
        action: "rebuild-concept-performance".to_string(),
        action_label: "概念表现补全".to_string(),
    })
}

pub fn prepare_concept_most_related_repair_run(
    input: ConceptMostRelatedRepairRunInput,
) -> Result<PreparedConceptMostRelatedRepairRun, String> {
    let source_path = input.source_path.trim().to_string();
    if source_path.is_empty() {
        return Err("数据目录为空，请先到数据管理页确认当前目录".to_string());
    }

    let status = get_data_download_status(&source_path)?;
    if !status.source_db.exists || status.source_db.row_count == 0 {
        return Err("原始库不存在或为空，请先完成 qfq 行情下载。".to_string());
    }
    if !status.ths_concepts.exists || status.ths_concepts.row_count == 0 {
        return Err("概念文件不存在或为空，请先完成概念数据下载。".to_string());
    }
    if !status.concept_performance_db.exists || status.concept_performance_db.row_count == 0 {
        return Err("概念表现库不存在或为空，请先执行概念表现补全。".to_string());
    }

    Ok(PreparedConceptMostRelatedRepairRun {
        source_path,
        action: "repair-concept-most-related".to_string(),
        action_label: "最相关概念补算".to_string(),
    })
}

pub fn prepare_stock_data_indicator_columns_delete_run(
    input: StockDataIndicatorColumnsDeleteRunInput,
) -> Result<PreparedStockDataIndicatorColumnsDeleteRun, String> {
    let source_path = input.source_path.trim().to_string();
    if source_path.is_empty() {
        return Err("数据目录为空，请先到数据管理页确认当前目录".to_string());
    }

    let status = get_data_download_status(&source_path)?;
    if !status.source_db.exists {
        return Err("原始库不存在，请先完成 qfq 行情下载。".to_string());
    }

    Ok(PreparedStockDataIndicatorColumnsDeleteRun {
        source_path,
        action: "delete-stock-data-indicator-columns".to_string(),
        action_label: "指标列删除".to_string(),
    })
}

pub fn prepare_stock_data_indicator_columns_rebuild_run(
    input: StockDataIndicatorColumnsRebuildRunInput,
) -> Result<PreparedStockDataIndicatorColumnsRebuildRun, String> {
    let source_path = input.source_path.trim().to_string();
    if source_path.is_empty() {
        return Err("数据目录为空，请先到数据管理页确认当前目录".to_string());
    }

    let status = get_data_download_status(&source_path)?;
    if !status.source_db.exists || status.source_db.row_count == 0 {
        return Err("原始库不存在或为空，请先完成 qfq 行情下载。".to_string());
    }

    ensure_default_indicator_manage_file(&source_path)?;
    let inds_cache = cache_ind_build(&source_path)?;
    if inds_cache.is_empty() {
        return Err("指标配置不存在或为空，请先维护 ind.toml。".to_string());
    }

    Ok(PreparedStockDataIndicatorColumnsRebuildRun {
        source_path,
        action: "rebuild-stock-data-indicator-columns".to_string(),
        action_label: "指标列补算".to_string(),
    })
}

pub fn run_prepared_data_download(
    prepared: &PreparedDataDownloadRun,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<DataDownloadRunResult, String> {
    let stock_config = DownloadRuntimeConfig {
        source_dir: prepared.source_path.clone(),
        adj_type: AdjType::Qfq,
        token: prepared.token.clone(),
        start_date: prepared.start_date.clone(),
        end_date: prepared.end_date.clone(),
        threads: prepared.threads,
        retry_times: prepared.retry_times,
        limit_calls_per_min: prepared.limit_calls_per_min,
        include_turnover: prepared.include_turnover,
        allow_stale_stock_list: prepared.allow_stale_stock_list,
    };

    let stock_progress_cb = |progress: DownloadProgress| {
        emit_nested_data_download_progress(
            progress_cb,
            DataDownloadNestedProgressScope::Stock,
            progress,
        );
    };
    let effective_trade_date = init_stock_basic_data(&stock_config, Some(&stock_progress_cb))?;
    let mut summary = core_run_download_with_progress(
        &stock_config,
        effective_trade_date.as_str(),
        Some(&stock_progress_cb),
    )?;
    let stock_recovered_stock_count = summary.recovered_stock_count;
    let index_config = DownloadRuntimeConfig {
        source_dir: prepared.source_path.clone(),
        adj_type: AdjType::Ind,
        token: prepared.token.clone(),
        start_date: prepared.start_date.clone(),
        end_date: prepared.end_date.clone(),
        threads: prepared.threads,
        retry_times: prepared.retry_times,
        limit_calls_per_min: prepared.limit_calls_per_min,
        include_turnover: false,
        allow_stale_stock_list: prepared.allow_stale_stock_list,
    };
    let index_progress_cb = |progress: DownloadProgress| {
        emit_nested_data_download_progress(
            progress_cb,
            DataDownloadNestedProgressScope::Index,
            progress,
        );
    };
    let index_summary = core_run_index_download_with_progress(
        &index_config,
        effective_trade_date.as_str(),
        Some(&index_progress_cb),
    )?;
    summary.success_count += index_summary.success_count;
    summary.failed_count += index_summary.failed_count;
    summary.saved_rows += index_summary.saved_rows;
    summary.concept_performance_rows += index_summary.concept_performance_rows;
    summary.failed_items.extend(index_summary.failed_items);

    let mut completion_details = Vec::new();
    if prepared.action == "incremental-download" {
        let recovered_stock_codes = summary.recovered_stock_codes.clone();
        let has_recovered_stocks = !recovered_stock_codes.is_empty();
        if let Some(cb) = progress_cb {
            cb(lianghua_download::download::runner::DownloadProgress {
                phase: "maintain_cyq_incremental".to_string(),
                finished: 0,
                total: 0,
                current_label: None,
                message: if has_recovered_stocks {
                    format!(
                        "本轮有 {} 只股票发生整段补救重下，开始局部修复对应筹码并维护增量数据。",
                        stock_recovered_stock_count
                    )
                } else {
                    "开始按设置检查筹码库并维护增量筹码数据。".to_string()
                },
            });
        }
        let chip_message = (|source_path: &str,
                             chip_model: &str,
                             recovered_stock_codes: &[String],
                             allow_cyq_chen_strategy_rebuild: bool,
                             progress_cb: Option<&DownloadProgressCallback<'_>>|
         -> Result<Option<String>, String> {
            let chip_progress_cb = |progress: DownloadProgress| {
                emit_chip_maintenance_progress(progress_cb, progress);
            };

            match chip_model {
                "chen" => {
                    let maintenance_status =
                        query_cyq_chen_strategy_maintenance_status(source_path)?;
                    if maintenance_status.strategy_changed && !allow_cyq_chen_strategy_rebuild {
                        return Ok(Some(
                            "检测到筹码策略已变化，已按确认选择跳过新筹码全量维护。".to_string(),
                        ));
                    }
                    let merge_repair_progress =
                        !maintenance_status.strategy_changed && !recovered_stock_codes.is_empty();
                    let repair_progress_cb = |progress: DownloadProgress| {
                        if merge_repair_progress && progress.total > 0 {
                            emit_chip_maintenance_progress(
                                progress_cb,
                                hide_chip_repair_local_counter(progress),
                            );
                        } else {
                            emit_chip_maintenance_progress(progress_cb, progress);
                        }
                    };
                    let repair_summary = if recovered_stock_codes.is_empty() {
                        None
                    } else {
                        repair_cyq_chen_stocks_if_db_exists(
                            source_path,
                            recovered_stock_codes,
                            allow_cyq_chen_strategy_rebuild,
                            Some(&repair_progress_cb),
                        )?
                    };
                    let repaired_stock_count = if repair_summary.is_some() && merge_repair_progress
                    {
                        // These are the successfully recovered market-data stocks passed into the
                        // chip repair. Use the stable task count instead of deriving it from callback
                        // timing, otherwise the following incremental phase can briefly report 1/14
                        // for a 15-stock combined job.
                        recovered_stock_codes.len()
                    } else {
                        0
                    };
                    let incremental_progress_cb = |progress: DownloadProgress| {
                        emit_chip_maintenance_progress(
                            progress_cb,
                            merge_chip_repair_into_incremental_progress(
                                progress,
                                repaired_stock_count,
                            ),
                        );
                    };
                    let incremental_summary = if maintenance_status.strategy_changed
                        && allow_cyq_chen_strategy_rebuild
                        && repair_summary.is_some()
                    {
                        None
                    } else {
                        maintain_cyq_chen_incremental_if_db_exists(
                            source_path,
                            allow_cyq_chen_strategy_rebuild,
                            Some(&incremental_progress_cb),
                        )?
                    };
                    let snapshot_rows = repair_summary
                        .as_ref()
                        .map(|summary| summary.snapshot_rows)
                        .unwrap_or(0)
                        + incremental_summary
                            .as_ref()
                            .map(|summary| summary.snapshot_rows)
                            .unwrap_or(0);
                    let bin_rows = repair_summary
                        .as_ref()
                        .map(|summary| summary.bin_rows)
                        .unwrap_or(0)
                        + incremental_summary
                            .as_ref()
                            .map(|summary| summary.bin_rows)
                            .unwrap_or(0);
                    let start_date = repair_summary
                        .as_ref()
                        .and_then(|summary| summary.start_date.as_deref())
                        .or_else(|| {
                            incremental_summary
                                .as_ref()
                                .and_then(|summary| summary.start_date.as_deref())
                        });
                    let end_date = incremental_summary
                        .as_ref()
                        .and_then(|summary| summary.end_date.as_deref())
                        .or_else(|| {
                            repair_summary
                                .as_ref()
                                .and_then(|summary| summary.end_date.as_deref())
                        });
                    Ok(Some(
                        if repair_summary.is_none() && incremental_summary.is_none() {
                            "未发现新筹码库 cyq_chen.db，已跳过筹码数据维护。".to_string()
                        } else if snapshot_rows > 0 || bin_rows > 0 {
                            let mode = if recovered_stock_codes.is_empty() {
                                "增量"
                            } else if maintenance_status.strategy_changed {
                                "全量"
                            } else {
                                "局部+增量"
                            };
                            format!(
                                "新筹码{}维护完成，区间 {} 至 {}，写入 {} 条摘要和 {} 条分桶。",
                                mode,
                                start_date.unwrap_or("--"),
                                end_date.unwrap_or("--"),
                                snapshot_rows,
                                bin_rows
                            )
                        } else {
                            "新筹码库已存在，但当前没有需要补算的筹码数据。".to_string()
                        },
                    ))
                }
                _ => {
                    let repair_summary = if recovered_stock_codes.is_empty() {
                        None
                    } else {
                        repair_cyq_stocks_if_db_exists(
                            source_path,
                            recovered_stock_codes,
                            Some(&chip_progress_cb),
                        )?
                    };
                    let incremental_summary = maintain_cyq_incremental_if_db_exists(source_path)?;
                    let snapshot_rows = repair_summary
                        .as_ref()
                        .map(|summary| summary.snapshot_rows)
                        .unwrap_or(0)
                        + incremental_summary
                            .as_ref()
                            .map(|summary| summary.snapshot_rows)
                            .unwrap_or(0);
                    let bin_rows = repair_summary
                        .as_ref()
                        .map(|summary| summary.bin_rows)
                        .unwrap_or(0)
                        + incremental_summary
                            .as_ref()
                            .map(|summary| summary.bin_rows)
                            .unwrap_or(0);
                    let start_date = repair_summary
                        .as_ref()
                        .and_then(|summary| summary.start_date.as_deref())
                        .or_else(|| {
                            incremental_summary
                                .as_ref()
                                .and_then(|summary| summary.start_date.as_deref())
                        });
                    let end_date = incremental_summary
                        .as_ref()
                        .and_then(|summary| summary.end_date.as_deref())
                        .or_else(|| {
                            repair_summary
                                .as_ref()
                                .and_then(|summary| summary.end_date.as_deref())
                        });
                    Ok(Some(
                        if repair_summary.is_none() && incremental_summary.is_none() {
                            "未发现筹码库 cyq.db，已跳过筹码数据维护。".to_string()
                        } else if snapshot_rows > 0 || bin_rows > 0 {
                            format!(
                                "筹码{}维护完成，区间 {} 至 {}，写入 {} 条摘要和 {} 条分桶。",
                                if recovered_stock_codes.is_empty() {
                                    "增量"
                                } else {
                                    "局部+增量"
                                },
                                start_date.unwrap_or("--"),
                                end_date.unwrap_or("--"),
                                snapshot_rows,
                                bin_rows
                            )
                        } else {
                            "筹码库已存在，但当前没有需要补算的筹码数据。".to_string()
                        },
                    ))
                }
            }
        })(
            &prepared.source_path,
            prepared.chip_model.as_str(),
            &recovered_stock_codes,
            prepared.allow_cyq_chen_strategy_rebuild,
            progress_cb,
        )?;
        if let Some(detail) = chip_message.as_ref().and_then(|message| {
            (|message: String| -> Option<String> {
                let message = message.trim().trim_end_matches('。').trim();
                if message.is_empty() {
                    None
                } else {
                    Some(message.to_string())
                }
            })(message.clone())
        }) {
            completion_details.push(detail);
        }
        if let Some(cb) = progress_cb {
            cb(lianghua_download::download::runner::DownloadProgress {
                phase: "maintain_cyq_incremental".to_string(),
                finished: 0,
                total: 0,
                current_label: None,
                message: chip_message.unwrap_or_else(|| "筹码数据维护完成。".to_string()),
            });
        }
    }

    if summary.concept_performance_rows > 0 {
        if let Some(detail) =
            concept_performance_completion_detail(summary.concept_performance_rows)
        {
            completion_details.insert(0, detail);
        }
    }

    let status = get_data_download_status(&prepared.source_path)?;

    Ok(DataDownloadRunResult {
        action: prepared.action.clone(),
        action_label: prepared.action_label.clone(),
        elapsed_ms: 0,
        summary: build_data_download_summary(summary),
        completion_details,
        status,
    })
}

pub fn run_prepared_missing_stock_repair(
    prepared: &PreparedMissingStockRepairRun,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<DataDownloadRunResult, String> {
    let config = DownloadRuntimeConfig {
        source_dir: prepared.source_path.clone(),
        adj_type: AdjType::Qfq,
        token: prepared.token.clone(),
        start_date: prepared.start_date.clone(),
        end_date: prepared.end_date.clone(),
        threads: prepared.threads,
        retry_times: prepared.retry_times,
        limit_calls_per_min: prepared.limit_calls_per_min,
        include_turnover: prepared.include_turnover,
        allow_stale_stock_list: false,
    };

    let summary = core_run_selected_stock_download_with_progress(
        &config,
        &prepared.missing_ts_codes,
        progress_cb,
    )?;
    let completion_details =
        concept_performance_completion_detail(summary.concept_performance_rows)
            .into_iter()
            .collect();
    let status = get_data_download_status(&prepared.source_path)?;

    Ok(DataDownloadRunResult {
        action: prepared.action.clone(),
        action_label: prepared.action_label.clone(),
        elapsed_ms: 0,
        summary: build_data_download_summary(summary),
        completion_details,
        status,
    })
}

pub fn run_prepared_dragon_tiger_download(
    prepared: &PreparedDragonTigerDownloadRun,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<DataDownloadRunResult, String> {
    let summary = core_download_dragon_tiger(
        &DragonTigerDownloadConfig {
            source_dir: prepared.source_path.clone(),
            token: prepared.token.clone(),
            start_date: prepared.start_date.clone(),
            end_date: prepared.end_date.clone(),
            retry_times: prepared.retry_times,
            limit_calls_per_min: prepared.limit_calls_per_min,
        },
        progress_cb,
    )?;
    let status = get_data_download_status(&prepared.source_path)?;

    Ok(DataDownloadRunResult {
        action: prepared.action.clone(),
        action_label: prepared.action_label.clone(),
        elapsed_ms: 0,
        summary: DataDownloadSummary {
            success_count: summary.synced_trade_dates as u64,
            failed_count: 0,
            saved_rows: (summary.top_list_rows + summary.top_inst_rows) as u64,
            concept_performance_rows: 0,
            failed_items: Vec::new(),
        },
        completion_details: vec![
            format!("龙虎榜每日明细 {} 行", summary.top_list_rows),
            format!("龙虎榜席位明细 {} 行", summary.top_inst_rows),
            format!("跳过 {} 个已同步交易日", summary.skipped_trade_dates),
            format!("暂缓 {} 个尚未更新交易日", summary.deferred_trade_dates),
        ],
        status,
    })
}

pub fn run_prepared_ths_concept_download(
    prepared: &PreparedThsConceptDownloadRun,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<DataDownloadRunResult, String> {
    let summary = core_download_ths_concepts(
        &prepared.source_path,
        ThsConceptDownloadConfig {
            retry_enabled: prepared.retry_enabled,
            retry_times: prepared.retry_times,
            retry_interval_secs: prepared.retry_interval_secs,
            concurrent_enabled: prepared.concurrent_enabled,
            worker_threads: prepared.worker_threads,
        },
        progress_cb,
    )?;
    let status = get_data_download_status(&prepared.source_path)?;

    Ok(DataDownloadRunResult {
        action: prepared.action.clone(),
        action_label: prepared.action_label.clone(),
        elapsed_ms: 0,
        summary: DataDownloadSummary {
            success_count: summary.saved_rows as u64,
            failed_count: 0,
            saved_rows: summary.saved_rows as u64,
            concept_performance_rows: 0,
            failed_items: Vec::new(),
        },
        completion_details: Vec::new(),
        status,
    })
}

pub fn run_prepared_concept_performance_repair(
    prepared: &PreparedConceptPerformanceRepairRun,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<DataDownloadRunResult, String> {
    if let Some(cb) = progress_cb {
        cb(lianghua_download::download::runner::DownloadProgress {
            phase: "rebuild_concept_performance".to_string(),
            finished: 0,
            total: 1,
            current_label: None,
            message: "开始全量补全概念/行业表现库。".to_string(),
        });
    }

    let saved_rows = rebuild_concept_performance_all(&prepared.source_path)?;

    if let Some(cb) = progress_cb {
        cb(lianghua_download::download::runner::DownloadProgress {
            phase: "rebuild_concept_performance".to_string(),
            finished: 1,
            total: 1,
            current_label: None,
            message: format!("概念表现补全完成，共写入 {} 行。", saved_rows),
        });
    }

    let status = get_data_download_status(&prepared.source_path)?;

    Ok(DataDownloadRunResult {
        action: prepared.action.clone(),
        action_label: prepared.action_label.clone(),
        elapsed_ms: 0,
        summary: DataDownloadSummary {
            success_count: 1,
            failed_count: 0,
            saved_rows: saved_rows as u64,
            concept_performance_rows: 0,
            failed_items: Vec::new(),
        },
        completion_details: Vec::new(),
        status,
    })
}

pub fn run_prepared_concept_most_related_repair(
    prepared: &PreparedConceptMostRelatedRepairRun,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<DataDownloadRunResult, String> {
    if let Some(cb) = progress_cb {
        cb(lianghua_download::download::runner::DownloadProgress {
            phase: "repair_concept_most_related".to_string(),
            finished: 0,
            total: 1,
            current_label: None,
            message: "开始补算每只股票的最相关概念。".to_string(),
        });
    }

    let updated_rows = rebuild_most_related_concept_csv(&prepared.source_path)?;

    if let Some(cb) = progress_cb {
        cb(lianghua_download::download::runner::DownloadProgress {
            phase: "repair_concept_most_related".to_string(),
            finished: 1,
            total: 1,
            current_label: None,
            message: format!("最相关概念补算完成，共更新 {} 行。", updated_rows),
        });
    }

    let status = get_data_download_status(&prepared.source_path)?;

    Ok(DataDownloadRunResult {
        action: prepared.action.clone(),
        action_label: prepared.action_label.clone(),
        elapsed_ms: 0,
        summary: DataDownloadSummary {
            success_count: updated_rows as u64,
            failed_count: 0,
            saved_rows: updated_rows as u64,
            concept_performance_rows: 0,
            failed_items: Vec::new(),
        },
        completion_details: Vec::new(),
        status,
    })
}

pub fn run_prepared_stock_data_indicator_columns_delete(
    prepared: &PreparedStockDataIndicatorColumnsDeleteRun,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<DataDownloadRunResult, String> {
    let conn = open_source_db_conn(&prepared.source_path)?;
    let indicator_columns = list_stock_data_indicator_columns(&conn)?;

    if let Some(cb) = progress_cb {
        cb(lianghua_download::download::runner::DownloadProgress {
            phase: "delete_stock_data_indicator_columns".to_string(),
            finished: 0,
            total: indicator_columns.len(),
            current_label: None,
            message: if indicator_columns.is_empty() {
                "stock_data 当前没有可删除的指标列。".to_string()
            } else {
                format!(
                    "开始删除 {} 个行情指标列，只保留基础行情列。",
                    indicator_columns.len()
                )
            },
        });
    }

    with_transaction(&conn, |tx| {
        drop_stock_data_columns(tx, &indicator_columns)?;
        Ok(())
    })?;

    if let Some(cb) = progress_cb {
        cb(lianghua_download::download::runner::DownloadProgress {
            phase: "delete_stock_data_indicator_columns".to_string(),
            finished: indicator_columns.len(),
            total: indicator_columns.len(),
            current_label: None,
            message: format!("指标列删除完成，共删除 {} 列。", indicator_columns.len()),
        });
    }

    let status = get_data_download_status(&prepared.source_path)?;

    Ok(DataDownloadRunResult {
        action: prepared.action.clone(),
        action_label: prepared.action_label.clone(),
        elapsed_ms: 0,
        summary: DataDownloadSummary {
            success_count: indicator_columns.len() as u64,
            failed_count: 0,
            saved_rows: 0,
            concept_performance_rows: 0,
            failed_items: Vec::new(),
        },
        completion_details: Vec::new(),
        status,
    })
}

pub fn run_prepared_stock_data_indicator_columns_rebuild(
    prepared: &PreparedStockDataIndicatorColumnsRebuildRun,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<DataDownloadRunResult, String> {
    ensure_default_indicator_manage_file(&prepared.source_path)?;
    let inds_cache = cache_ind_build(&prepared.source_path)?;
    if inds_cache.is_empty() {
        return Err("指标配置不存在或为空，请先维护 ind.toml。".to_string());
    }

    let indicator_names = inds_cache
        .iter()
        .map(|item| item.name.clone())
        .collect::<Vec<_>>();
    let conn = open_source_db_conn(&prepared.source_path)?;
    let work_items = list_stock_data_indicator_work_items(&conn)?;
    if work_items.is_empty() {
        return Err("stock_data 没有可补算的行情记录。".to_string());
    }

    if let Some(cb) = progress_cb {
        cb(lianghua_download::download::runner::DownloadProgress {
            phase: "rebuild_stock_data_indicator_columns".to_string(),
            finished: 0,
            total: work_items.len(),
            current_label: None,
            message: format!(
                "开始按现有行情补算 {} 组股票/复权序列的指标列。",
                work_items.len()
            ),
        });
    }

    let (tx, rx) = sync_channel(16);
    let abort_tx = tx.clone();
    let source_path = prepared.source_path.clone();
    let inds_cache_for_workers = inds_cache.clone();
    let work_items_for_workers = work_items.clone();
    let compute_handle = thread::spawn(move || {
        let compute_result =
            work_items_for_workers
                .par_chunks(256)
                .try_for_each_with(tx, |sender, work_group| {
                    compute_stock_data_indicator_rebuild_batch(
                        sender,
                        &source_path,
                        &inds_cache_for_workers,
                        work_group,
                    )
                });

        match &compute_result {
            Ok(_) => {
                let _ = abort_tx.send(StockDataIndicatorRebuildMessage::Done);
            }
            Err(err) => {
                let _ = abort_tx.send(StockDataIndicatorRebuildMessage::Abort(err.clone()));
            }
        }
        drop(abort_tx);
        compute_result
    });

    let mut updated_rows = 0_u64;
    let mut finished_groups = 0_usize;
    let write_result = with_transaction(&conn, |tx| {
        ensure_indicator_columns(tx, &indicator_names)?;
        reset_stock_data_indicator_stage_table(tx, &indicator_names)?;
        let mut stage_appender =
            create_stock_data_indicator_stage_appender_for_columns(tx, &indicator_names)?;
        let mut compute_done = false;

        while let Ok(message) = rx.recv() {
            match message {
                StockDataIndicatorRebuildMessage::Batch(batch) => {
                    append_stock_data_indicator_stage_rows_with_appender(
                        &mut stage_appender,
                        &indicator_names,
                        batch.ts_code.as_str(),
                        batch.adj_type,
                        &batch.trade_dates,
                        &batch.indicators,
                    )?;
                    updated_rows += batch.row_count;
                    finished_groups += 1;

                    if let Some(cb) = progress_cb {
                        cb(lianghua_download::download::runner::DownloadProgress {
                            phase: "rebuild_stock_data_indicator_columns".to_string(),
                            finished: finished_groups,
                            total: work_items.len(),
                            current_label: Some(format!(
                                "{} / {}",
                                batch.ts_code, batch.adj_type_label
                            )),
                            message: format!(
                                "已补算 {}/{} 组，当前 {} / {}，本组 {} 行。",
                                finished_groups,
                                work_items.len(),
                                batch.ts_code,
                                batch.adj_type_label,
                                batch.row_count
                            ),
                        });
                    }
                }
                StockDataIndicatorRebuildMessage::Abort(err) => return Err(err),
                StockDataIndicatorRebuildMessage::Done => {
                    compute_done = true;
                    break;
                }
            }
        }

        if !compute_done {
            return Err("指标补算计算线程未正常完成，已回滚本次指标列维护。".to_string());
        }

        stage_appender
            .flush()
            .map_err(|e| format!("刷新指标临时表 Appender 失败: {e}"))?;
        drop(stage_appender);
        flush_stock_data_indicator_stage_table(tx, &indicator_names)?;
        Ok(())
    });

    let compute_result = match compute_handle.join() {
        Ok(result) => result,
        Err(_) => Err("指标补算线程异常退出".to_string()),
    };

    write_result?;
    compute_result?;

    let status = get_data_download_status(&prepared.source_path)?;

    Ok(DataDownloadRunResult {
        action: prepared.action.clone(),
        action_label: prepared.action_label.clone(),
        elapsed_ms: 0,
        summary: DataDownloadSummary {
            success_count: work_items.len() as u64,
            failed_count: 0,
            saved_rows: updated_rows,
            concept_performance_rows: 0,
            failed_items: Vec::new(),
        },
        completion_details: Vec::new(),
        status,
    })
}

pub fn get_indicator_manage_page(source_path: &str) -> Result<IndicatorManagePageData, String> {
    let trimmed = source_path.trim();
    if trimmed.is_empty() {
        return Err("数据目录为空，请先到数据管理页确认当前目录".to_string());
    }

    ensure_default_indicator_manage_file(trimmed)?;
    let path = ind_toml_path(trimmed);
    let content = fs::read_to_string(&path)
        .map_err(|e| format!("读取指标配置失败: path={}, err={e}", path.display()))?;
    let items = if content.trim().is_empty() {
        Vec::new()
    } else {
        IndsData::parse_from_text(&content)?
            .into_iter()
            .enumerate()
            .map(|(index, item)| IndicatorManageItem {
                index,
                name: item.name,
                expr: item.expr,
                prec: item.prec,
            })
            .collect()
    };

    Ok(IndicatorManagePageData {
        exists: true,
        file_path: path.display().to_string(),
        items,
    })
}

fn ensure_default_indicator_manage_file(source_path: &str) -> Result<(), String> {
    let path = ind_toml_path(source_path);
    if path.exists() {
        let content = fs::read_to_string(&path)
            .map_err(|e| format!("读取指标配置失败: path={}, err={e}", path.display()))?;
        if !content.trim().is_empty() {
            return Ok(());
        }
    } else if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("创建指标配置目录失败: path={}, err={e}", parent.display()))?;
        }
    }

    let text = build_indicator_manage_toml(&(|| -> Vec<IndicatorManageDraft> {
        vec![
            IndicatorManageDraft {
                name: "J".to_string(),
                expr: (r#"RSV1 := RSV(C, H, L, 9);
K1 := SMA(RSV1, 3, 1);
D1 := SMA(K1, 3, 1);
3 * K1 - 2 * D1;"#)
                    .to_string(),
                prec: 2,
            },
            IndicatorManageDraft {
                name: "ER".to_string(),
                expr: (r#"N := 20;
(C - REF(C, N)) / SUM(ABS(C - REF(C, 1)), N);"#)
                    .to_string(),
                prec: 6,
            },
        ]
    })())?;
    fs::write(&path, text)
        .map_err(|e| format!("写入默认指标配置失败: path={}, err={e}", path.display()))?;
    Ok(())
}

fn build_indicator_manage_toml(items: &[IndicatorManageDraft]) -> Result<String, String> {
    let normalized_items = items
        .iter()
        .map(|item| {
            let name = item.name.trim().to_ascii_uppercase();
            let expr = item.expr.trim().to_string();
            if name.is_empty() {
                return Err("指标名称不能为空".to_string());
            }
            if expr.is_empty() {
                return Err(format!("指标 {name} 的表达式不能为空"));
            }

            Ok(IndicatorManageFileItem {
                name,
                expr,
                prec: item.prec,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let text = toml::to_string_pretty(&IndicatorManageFile {
        version: 1,
        ind: normalized_items,
    })
    .map_err(|e| format!("序列化指标配置失败: {e}"))?;

    let parsed_items = IndsData::parse_from_text(&text)?;
    for item in parsed_items {
        let program = parse_expression_program(&item.expr)
            .map_err(|e| format!("指标 {} 表达式解析错误在{}:{}", item.name, e.idx, e.msg))?;
        validate_expression_functions(&program)
            .map_err(|error| format!("指标 {} {error}", item.name))?;
    }

    Ok(text)
}

pub fn save_indicator_manage_page(
    source_path: &str,
    items: Vec<IndicatorManageDraft>,
) -> Result<IndicatorManagePageData, String> {
    let trimmed = source_path.trim();
    if trimmed.is_empty() {
        return Err("数据目录为空，请先到数据管理页确认当前目录".to_string());
    }

    let path = ind_toml_path(trimmed);
    let text = build_indicator_manage_toml(&items)?;
    fs::write(&path, text)
        .map_err(|e| format!("写入指标配置失败: path={}, err={e}", path.display()))?;
    get_indicator_manage_page(trimmed)
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{create_dir_all, remove_dir_all},
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    };

    use duckdb::{Connection, params};

    use super::*;

    fn progress(phase: &str, finished: usize, total: usize) -> DownloadProgress {
        DownloadProgress {
            phase: phase.to_string(),
            finished,
            total,
            current_label: None,
            message: phase.to_string(),
        }
    }

    fn temp_dir_path(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}_{nanos}"))
    }

    #[test]
    fn normalizes_stock_nested_terminal_progress_to_non_terminal_phase() {
        let next = normalize_nested_data_download_progress(
            DataDownloadNestedProgressScope::Stock,
            progress("done", 10, 10),
        );

        assert_eq!(next.phase, "stock_download_done");
        assert_eq!(next.finished, 1);
        assert_eq!(next.total, 1);
        assert_ne!(next.message, "done");
    }

    #[test]
    fn normalizes_index_prepare_and_write_phases() {
        let prepare = normalize_nested_data_download_progress(
            DataDownloadNestedProgressScope::Index,
            progress("prepare_index_list", 1, 1),
        );
        let write = normalize_nested_data_download_progress(
            DataDownloadNestedProgressScope::Index,
            progress("write_db", 0, 7),
        );

        assert_eq!(prepare.phase, "prepare_index_download");
        assert_eq!(write.phase, "write_index_db");
    }

    #[test]
    fn hides_the_separate_chip_repair_counter_before_incremental_progress() {
        let mut repair_progress = progress("compute_cyq_chen", 1, 1);
        repair_progress.current_label = Some("000001.SZ".to_string());

        let next = hide_chip_repair_local_counter(repair_progress);

        assert_eq!(next.finished, 0);
        assert_eq!(next.total, 0);
        assert!(next.message.contains("局部修复已完成"));
        assert!(next.message.contains("其余增量股票"));
    }

    #[test]
    fn merges_repaired_stocks_into_following_chip_incremental_counter() {
        let next =
            merge_chip_repair_into_incremental_progress(progress("compute_cyq_chen", 6, 14), 1);

        assert_eq!(next.finished, 7);
        assert_eq!(next.total, 15);
        assert!(next.message.contains("总进度 7 / 15"));
    }

    #[test]
    fn daily_target_uses_previous_trade_date_before_close_and_today_after_close() {
        let trade_dates = vec!["20260825".to_string(), "20260826".to_string()];

        assert_eq!(
            resolve_daily_target_trade_date(&trade_dates, "20260826", 1559).as_deref(),
            Some("20260825")
        );
        assert_eq!(
            resolve_daily_target_trade_date(&trade_dates, "20260826", 1600).as_deref(),
            Some("20260826")
        );
        assert_eq!(
            resolve_daily_target_trade_date(&trade_dates, "20260827", 1200).as_deref(),
            Some("20260826")
        );
    }

    #[test]
    fn qfq_range_ignores_newer_index_rows() {
        let source_dir = temp_dir_path("lianghua_data_download_qfq_range");
        create_dir_all(&source_dir).expect("create temp dir");
        (|source_dir: &Path, rows: &[(&str, &str, &str)]| {
            let db_path = source_db_path(source_dir.to_str().expect("utf8 path"));
            let conn = Connection::open(db_path).expect("open source db");
            conn.execute(
                r#"
            CREATE TABLE stock_data (
                ts_code VARCHAR,
                trade_date VARCHAR,
                adj_type VARCHAR
            )
            "#,
                [],
            )
            .expect("create stock_data");
            let mut app = conn.appender("stock_data").expect("appender stock_data");
            for (ts_code, trade_date, adj_type) in rows {
                app.append_row(params![ts_code, trade_date, adj_type])
                    .expect("append stock row");
            }
            app.flush().expect("flush stock rows");
        })(
            &source_dir,
            &[
                ("000001.SZ", "20240102", "qfq"),
                ("000002.SZ", "20240102", "qfq"),
                ("000001.SH", "20240103", "ind"),
            ],
        );

        let source_path = source_dir.to_str().expect("utf8 path");
        let source_qfq = query_stock_data_adj_type_range(source_path, "qfq").expect("qfq range");
        let source_all =
            query_trade_date_range(&source_db_path(source_path), "stock_data.db", "stock_data")
                .expect("source all range");

        assert_eq!(source_all.max_trade_date.as_deref(), Some("20240103"));
        assert_eq!(source_qfq.max_trade_date.as_deref(), Some("20240102"));

        let _ = remove_dir_all(source_dir);
    }

    #[test]
    fn indicator_rebuild_uses_persisted_moneyflow_columns() {
        let source_dir = temp_dir_path("lianghua_indicator_rebuild_moneyflow");
        create_dir_all(&source_dir).expect("create temp dir");
        let source_path = source_dir.to_str().expect("utf8 path");
        let db_path = source_db_path(source_path);
        crate::data::download_data::init_stock_data_db(
            db_path.to_str().expect("utf8 database path"),
        )
        .expect("init stock data");
        let conn = Connection::open(&db_path).expect("open stock data");
        crate::data::download_data::insert_pro_bar_rows(
            &conn,
            AdjType::Qfq,
            &[lianghua_download::download::ProBarRow {
                ts_code: "000001.SZ".to_string(),
                trade_date: "20240102".to_string(),
                open: 10.0,
                high: 10.5,
                low: 9.8,
                close: 10.2,
                pre_close: 10.0,
                change: 0.2,
                pct_chg: 2.0,
                vol: 1000.0,
                amount: 10000.0,
                turnover_rate: Some(1.2),
                volume_ratio: None,
                moneyflow: Some(lianghua_download::download::MoneyflowRow {
                    ts_code: "000001.SZ".to_string(),
                    trade_date: "20240102".to_string(),
                    b_sm_v: Some(1.0),
                    s_sm_v: Some(3.0),
                    b_md_v: Some(5.0),
                    s_md_v: Some(7.0),
                    b_lg_v: Some(9.0),
                    s_lg_v: Some(11.0),
                    b_elg_v: Some(13.0),
                    s_elg_v: Some(15.0),
                    net_mf_v: Some(17.0),
                }),
            }],
        )
        .expect("insert stock data");
        fs::write(
            ind_toml_path(source_path),
            r#"
            version = 1

            [[ind]]
            name = "FLOW_SUM"
            expr = "B_SM_V + NET_MF_V"
            prec = 2
            "#,
        )
        .expect("write indicator config");

        let cache = cache_ind_build(source_path).expect("build indicator cache");
        let work_items = list_stock_data_indicator_work_items(&conn).expect("list work items");
        let (sender, receiver) = sync_channel(1);
        compute_stock_data_indicator_rebuild_batch(&sender, source_path, &cache, &work_items)
            .expect("compute rebuilt indicators");

        let StockDataIndicatorRebuildMessage::Batch(batch) =
            receiver.recv().expect("receive rebuilt indicators")
        else {
            panic!("expected rebuilt indicator batch");
        };
        assert_eq!(batch.trade_dates, vec!["20240102".to_string()]);
        assert_eq!(batch.indicators.get("FLOW_SUM"), Some(&vec![Some(18.0)]));

        drop(conn);
        let _ = remove_dir_all(source_dir);
    }

    #[test]
    fn dragon_tiger_status_reports_both_table_counts() {
        let source_dir = temp_dir_path("lianghua_data_download_dragon_tiger_status");
        let source_path = source_dir.to_str().expect("utf8 path");
        let conn = crate::data::dragon_tiger_data::open_dragon_tiger_db(source_path)
            .expect("open dragon tiger db");
        conn.execute(
            "INSERT INTO top_list (trade_date, ts_code, name, reason) VALUES (?, ?, ?, ?)",
            params!["20260724", "000011.SZ", "深物业A", "测试原因"],
        )
        .expect("insert top_list");
        conn.execute(
            "INSERT INTO top_inst (trade_date, ts_code, exalter, side, reason) VALUES (?, ?, ?, ?, ?)",
            params!["20260724", "000011.SZ", "测试营业部", "0", "测试原因"],
        )
        .expect("insert top_inst");
        conn.execute(
            "INSERT INTO dragon_tiger_sync_log (trade_date, top_list_row_count, top_inst_row_count) VALUES (?, ?, ?)",
            params!["20260724", 1_i64, 1_i64],
        )
        .expect("insert sync log");
        drop(conn);

        let status = query_dragon_tiger_db_status(source_path).expect("query status");
        assert!(status.exists);
        assert_eq!(status.min_trade_date.as_deref(), Some("20260724"));
        assert_eq!(status.max_trade_date.as_deref(), Some("20260724"));
        assert_eq!(status.synced_trade_dates, 1);
        assert_eq!(status.top_list_rows, 1);
        assert_eq!(status.top_inst_rows, 1);

        let _ = remove_dir_all(source_dir);
    }

    #[test]
    fn indicator_manage_page_creates_default_indicator_config_when_missing() {
        let source_dir = temp_dir_path("lianghua_indicator_default");
        let source_path = source_dir.to_str().expect("utf8 path");

        let page = get_indicator_manage_page(source_path).expect("indicator page");

        assert!(ind_toml_path(source_path).exists());
        assert!(page.exists);
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].name, "J");
        assert_eq!(page.items[0].prec, 2);
        assert!(page.items[0].expr.contains("RSV(C, H, L, 9)"));
        assert_eq!(page.items[1].name, "ER");
        assert_eq!(page.items[1].prec, 6);
        assert!(page.items[1].expr.contains("REF(C, N)"));

        let _ = remove_dir_all(source_dir);
    }

    #[test]
    fn indicator_manage_page_fills_default_indicator_config_when_empty() {
        let source_dir = temp_dir_path("lianghua_indicator_empty_default");
        create_dir_all(&source_dir).expect("create temp dir");
        fs::write(ind_toml_path(source_dir.to_str().expect("utf8 path")), "\n")
            .expect("write empty ind file");
        let source_path = source_dir.to_str().expect("utf8 path");

        let page = get_indicator_manage_page(source_path).expect("indicator page");

        assert!(page.exists);
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].name, "J");
        assert_eq!(page.items[1].name, "ER");

        let _ = remove_dir_all(source_dir);
    }
}
