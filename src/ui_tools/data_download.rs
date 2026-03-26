use std::path::Path;

use duckdb::Connection;
use serde::{Deserialize, Serialize};

use crate::{
    config::{AppConfig, DataConfig, DownloadConfig, OutputConfig},
    data::{
        load_stock_list, load_trade_date_list, source_db_path, stock_list_path, trade_calendar_path,
    },
    download::{
        DownloadSummary,
        runner::{DownloadProgressCallback, download as core_run_download_with_progress},
    },
};

use super::watch_observe::normalize_trade_date;

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
pub struct DataDownloadStatus {
    pub source_path: String,
    pub source_db: DataDownloadDbRange,
    pub stock_list: DataDownloadFileStatus,
    pub trade_calendar: DataDownloadFileStatus,
    pub planned_action: String,
    pub planned_action_label: String,
    pub planned_action_detail: String,
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
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DataDownloadSummary {
    pub success_count: u64,
    pub failed_count: u64,
    pub saved_rows: u64,
    pub failed_items: Vec<String>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DataDownloadRunResult {
    pub action: String,
    pub action_label: String,
    pub elapsed_ms: u64,
    pub summary: DataDownloadSummary,
    pub status: DataDownloadStatus,
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

fn query_trade_calendar_status(source_path: &str) -> Result<DataDownloadFileStatus, String> {
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
}

fn query_stock_list_status(source_path: &str) -> Result<DataDownloadFileStatus, String> {
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
}

fn plan_download_action(source_db: &DataDownloadDbRange) -> (String, String, String) {
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
            "将先刷新交易日历和股票列表，再下载全市场历史行情与指标，并初始化原始库。".to_string(),
        ),
    }
}

fn build_data_download_summary(summary: DownloadSummary) -> DataDownloadSummary {
    DataDownloadSummary {
        success_count: summary.success_count as u64,
        failed_count: summary.failed_count as u64,
        saved_rows: summary.saved_rows as u64,
        failed_items: summary
            .failed_items
            .into_iter()
            .take(12)
            .map(|(ts_code, error)| format!("{ts_code}: {error}"))
            .collect(),
    }
}

pub fn get_data_download_status(source_path: &str) -> Result<DataDownloadStatus, String> {
    let trimmed = source_path.trim();
    if trimmed.is_empty() {
        return Err("数据目录为空，请先到数据管理页确认当前目录".to_string());
    }

    let source_db =
        query_trade_date_range(&source_db_path(trimmed), "stock_data.db", "stock_data")?;
    let trade_calendar = query_trade_calendar_status(trimmed)?;
    let stock_list = query_stock_list_status(trimmed)?;
    let (planned_action, planned_action_label, planned_action_detail) =
        plan_download_action(&source_db);

    Ok(DataDownloadStatus {
        source_path: trimmed.to_string(),
        source_db,
        stock_list,
        trade_calendar,
        planned_action,
        planned_action_label,
        planned_action_detail,
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
        action: status.planned_action,
        action_label: status.planned_action_label,
    })
}

pub fn run_prepared_data_download(
    prepared: &PreparedDataDownloadRun,
    progress_cb: Option<&DownloadProgressCallback>,
) -> Result<DataDownloadRunResult, String> {
    let source_db = source_db_path(&prepared.source_path);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "原始库路径不是有效 UTF-8".to_string())?
        .to_string();

    let config = AppConfig {
        data: DataConfig {
            source_db: source_db_str,
            adj_type: "qfq".to_string(),
        },
        output: OutputConfig {
            dir: prepared.source_path.clone(),
            result_db: "scoring_result.db".to_string(),
        },
        download: DownloadConfig {
            token: prepared.token.clone(),
            start_date: prepared.start_date.clone(),
            end_date: prepared.end_date.clone(),
            threads: prepared.threads,
            retry_times: prepared.retry_times,
            limit_calls_per_min: prepared.limit_calls_per_min,
            refresh_stock_list: true,
            include_turnover: prepared.include_turnover,
        },
    };

    let summary = core_run_download_with_progress(&config, progress_cb)?;
    let status = get_data_download_status(&prepared.source_path)?;

    Ok(DataDownloadRunResult {
        action: prepared.action.clone(),
        action_label: prepared.action_label.clone(),
        elapsed_ms: 0,
        summary: build_data_download_summary(summary),
        status,
    })
}
