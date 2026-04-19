use std::{collections::HashSet, fs, path::Path};

use duckdb::{Connection, params};
use serde::{Deserialize, Serialize};

use crate::{
    data::{
        IndsData,
        concept_performance_data::{
            rebuild_concept_performance_all, rebuild_most_related_concept_csv,
        },
        concept_performance_db_path,
        download_data::{
            drop_stock_data_columns, ensure_indicator_columns, list_stock_data_indicator_columns,
            update_one_stock_indicator_rows,
        },
        ind_toml_path, load_stock_list, load_ths_concepts_list, load_trade_date_list,
        source_db_path, stock_list_path, ths_concepts_path, trade_calendar_path,
    },
    download::{
        AdjType, DownloadSummary, ProBarRow,
        ind_calc::{cache_ind_build, calc_inds_for_rows_with_cache},
        runner::{
            DownloadProgressCallback, DownloadRuntimeConfig, ThsConceptDownloadConfig,
            download as core_run_download_with_progress,
            download_indices as core_run_index_download_with_progress,
            download_selected_stocks as core_run_selected_stock_download_with_progress,
            download_ths_concepts as core_download_ths_concepts,
        },
    },
    expr::parser::{Parser, lex_all},
};

use super::normalize_trade_date;

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
    pub concept_performance_db: DataDownloadDbRange,
    pub stock_list: DataDownloadFileStatus,
    pub trade_calendar: DataDownloadFileStatus,
    pub ths_concepts: DataDownloadFileStatus,
    pub missing_stock_repair: DataDownloadMissingStockRepairStatus,
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
    row_count: u64,
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

fn open_source_db_conn(source_path: &str) -> Result<Connection, String> {
    let db_path = source_db_path(source_path);
    let db_path_str = db_path
        .to_str()
        .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?;
    Connection::open(db_path_str).map_err(|e| format!("数据库连接错误:{e}"))
}

fn parse_stock_data_adj_type(raw: &str) -> Result<AdjType, String> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "qfq" => Ok(AdjType::Qfq),
        "hfq" => Ok(AdjType::Hfq),
        "raw" => Ok(AdjType::Raw),
        "ind" => Ok(AdjType::Ind),
        _ => Err(format!("不支持的adj_type: {raw}")),
    }
}

fn list_stock_data_indicator_work_items(
    conn: &Connection,
) -> Result<Vec<StockDataIndicatorWorkItem>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT ts_code, adj_type, COUNT(*)
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
        let row_count: i64 = row.get(2).map_err(|e| format!("读取row_count失败:{e}"))?;
        items.push(StockDataIndicatorWorkItem {
            ts_code,
            adj_type,
            row_count: row_count.max(0) as u64,
        });
    }

    Ok(items)
}

fn load_stock_data_rows_for_indicator_rebuild(
    conn: &Connection,
    ts_code: &str,
    adj_type: &str,
) -> Result<Vec<ProBarRow>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT
                ts_code,
                trade_date,
                TRY_CAST(open AS DOUBLE),
                TRY_CAST(high AS DOUBLE),
                TRY_CAST(low AS DOUBLE),
                TRY_CAST(close AS DOUBLE),
                TRY_CAST(pre_close AS DOUBLE),
                TRY_CAST(change AS DOUBLE),
                TRY_CAST(pct_chg AS DOUBLE),
                TRY_CAST(vol AS DOUBLE),
                TRY_CAST(amount AS DOUBLE),
                TRY_CAST(tor AS DOUBLE)
            FROM stock_data
            WHERE ts_code = ?
              AND adj_type = ?
            ORDER BY trade_date ASC
            "#,
        )
        .map_err(|e| format!("预编译指标补算行情SQL失败:{e}"))?;
    let mut query = stmt
        .query(params![ts_code, adj_type])
        .map_err(|e| format!("查询指标补算行情失败:{e}"))?;
    let mut rows = Vec::new();

    while let Some(row) = query.next().map_err(|e| format!("读取行情行失败:{e}"))? {
        rows.push(ProBarRow {
            ts_code: row.get(0).map_err(|e| format!("读取ts_code失败:{e}"))?,
            trade_date: row.get(1).map_err(|e| format!("读取trade_date失败:{e}"))?,
            open: row.get(2).map_err(|e| format!("读取open失败:{e}"))?,
            high: row.get(3).map_err(|e| format!("读取high失败:{e}"))?,
            low: row.get(4).map_err(|e| format!("读取low失败:{e}"))?,
            close: row.get(5).map_err(|e| format!("读取close失败:{e}"))?,
            pre_close: row.get(6).map_err(|e| format!("读取pre_close失败:{e}"))?,
            change: row.get(7).map_err(|e| format!("读取change失败:{e}"))?,
            pct_chg: row.get(8).map_err(|e| format!("读取pct_chg失败:{e}"))?,
            vol: row.get(9).map_err(|e| format!("读取vol失败:{e}"))?,
            amount: row.get(10).map_err(|e| format!("读取amount失败:{e}"))?,
            turnover_rate: row.get(11).map_err(|e| format!("读取tor失败:{e}"))?,
            volume_ratio: None,
        });
    }

    Ok(rows)
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

fn query_ths_concepts_status(source_path: &str) -> Result<DataDownloadFileStatus, String> {
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

fn query_existing_stock_codes(source_path: &str) -> Result<HashSet<String>, String> {
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
    let existing_codes = query_existing_stock_codes(source_path)?;

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
    let concept_performance_db = query_trade_date_range(
        &concept_performance_db_path(trimmed),
        "concept_performance.db",
        "concept_performance",
    )?;
    let trade_calendar = query_trade_calendar_status(trimmed)?;
    let stock_list = query_stock_list_status(trimmed)?;
    let ths_concepts = query_ths_concepts_status(trimmed)?;
    let (_, missing_stock_repair) =
        scan_missing_stock_codes(trimmed, &source_db, &stock_list, &trade_calendar)?;
    let (planned_action, planned_action_label, planned_action_detail) =
        plan_download_action(&source_db);

    Ok(DataDownloadStatus {
        source_path: trimmed.to_string(),
        source_db,
        concept_performance_db,
        stock_list,
        trade_calendar,
        ths_concepts,
        missing_stock_repair,
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
        action: status.planned_action,
        action_label: status.planned_action_label,
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
    if !status.source_db.exists || status.source_db.row_count == 0 {
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
    progress_cb: Option<&DownloadProgressCallback>,
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
    };

    let mut summary = core_run_download_with_progress(&stock_config, progress_cb)?;
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
    };
    let index_summary = core_run_index_download_with_progress(&index_config, progress_cb)?;
    summary.success_count += index_summary.success_count;
    summary.failed_count += index_summary.failed_count;
    summary.saved_rows += index_summary.saved_rows;
    summary.failed_items.extend(index_summary.failed_items);

    if let Some(cb) = progress_cb {
        cb(crate::download::runner::DownloadProgress {
            phase: "rebuild_concept_performance".to_string(),
            finished: 0,
            total: 1,
            current_label: None,
            message: "开始维护概念/行业/板块表现库。".to_string(),
        });
    }
    let _ = rebuild_concept_performance_all(&prepared.source_path)?;
    if let Some(cb) = progress_cb {
        cb(crate::download::runner::DownloadProgress {
            phase: "rebuild_concept_performance".to_string(),
            finished: 1,
            total: 1,
            current_label: None,
            message: "概念/行业/板块表现维护完成。".to_string(),
        });
    }

    let status = get_data_download_status(&prepared.source_path)?;

    Ok(DataDownloadRunResult {
        action: prepared.action.clone(),
        action_label: prepared.action_label.clone(),
        elapsed_ms: 0,
        summary: build_data_download_summary(summary),
        status,
    })
}

pub fn run_prepared_missing_stock_repair(
    prepared: &PreparedMissingStockRepairRun,
    progress_cb: Option<&DownloadProgressCallback>,
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
    };

    let summary = core_run_selected_stock_download_with_progress(
        &config,
        &prepared.missing_ts_codes,
        progress_cb,
    )?;
    let status = get_data_download_status(&prepared.source_path)?;

    Ok(DataDownloadRunResult {
        action: prepared.action.clone(),
        action_label: prepared.action_label.clone(),
        elapsed_ms: 0,
        summary: build_data_download_summary(summary),
        status,
    })
}

pub fn run_prepared_ths_concept_download(
    prepared: &PreparedThsConceptDownloadRun,
    progress_cb: Option<&DownloadProgressCallback>,
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
            failed_items: Vec::new(),
        },
        status,
    })
}

pub fn run_prepared_concept_performance_repair(
    prepared: &PreparedConceptPerformanceRepairRun,
    progress_cb: Option<&DownloadProgressCallback>,
) -> Result<DataDownloadRunResult, String> {
    if let Some(cb) = progress_cb {
        cb(crate::download::runner::DownloadProgress {
            phase: "rebuild_concept_performance".to_string(),
            finished: 0,
            total: 1,
            current_label: None,
            message: "开始全量补全概念/行业/板块表现库。".to_string(),
        });
    }

    let saved_rows = rebuild_concept_performance_all(&prepared.source_path)?;

    if let Some(cb) = progress_cb {
        cb(crate::download::runner::DownloadProgress {
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
            failed_items: Vec::new(),
        },
        status,
    })
}

pub fn run_prepared_concept_most_related_repair(
    prepared: &PreparedConceptMostRelatedRepairRun,
    progress_cb: Option<&DownloadProgressCallback>,
) -> Result<DataDownloadRunResult, String> {
    if let Some(cb) = progress_cb {
        cb(crate::download::runner::DownloadProgress {
            phase: "repair_concept_most_related".to_string(),
            finished: 0,
            total: 1,
            current_label: None,
            message: "开始补算每只股票的最相关概念。".to_string(),
        });
    }

    let updated_rows = rebuild_most_related_concept_csv(&prepared.source_path)?;

    if let Some(cb) = progress_cb {
        cb(crate::download::runner::DownloadProgress {
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
            failed_items: Vec::new(),
        },
        status,
    })
}

pub fn run_prepared_stock_data_indicator_columns_delete(
    prepared: &PreparedStockDataIndicatorColumnsDeleteRun,
    progress_cb: Option<&DownloadProgressCallback>,
) -> Result<DataDownloadRunResult, String> {
    let conn = open_source_db_conn(&prepared.source_path)?;
    let indicator_columns = list_stock_data_indicator_columns(&conn)?;

    if let Some(cb) = progress_cb {
        cb(crate::download::runner::DownloadProgress {
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
        cb(crate::download::runner::DownloadProgress {
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
            failed_items: Vec::new(),
        },
        status,
    })
}

pub fn run_prepared_stock_data_indicator_columns_rebuild(
    prepared: &PreparedStockDataIndicatorColumnsRebuildRun,
    progress_cb: Option<&DownloadProgressCallback>,
) -> Result<DataDownloadRunResult, String> {
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
        cb(crate::download::runner::DownloadProgress {
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

    let mut updated_rows = 0_u64;
    with_transaction(&conn, |tx| {
        ensure_indicator_columns(tx, &indicator_names)?;

        for (index, item) in work_items.iter().enumerate() {
            let rows = load_stock_data_rows_for_indicator_rebuild(
                tx,
                item.ts_code.as_str(),
                item.adj_type.as_str(),
            )?;
            if rows.is_empty() {
                continue;
            }

            let indicators = calc_inds_for_rows_with_cache(&inds_cache, &rows)?;
            let adj_type = parse_stock_data_adj_type(item.adj_type.as_str())?;
            update_one_stock_indicator_rows(
                tx,
                item.ts_code.as_str(),
                adj_type,
                &rows,
                &indicators,
            )?;
            updated_rows += rows.len() as u64;

            if let Some(cb) = progress_cb {
                cb(crate::download::runner::DownloadProgress {
                    phase: "rebuild_stock_data_indicator_columns".to_string(),
                    finished: index + 1,
                    total: work_items.len(),
                    current_label: Some(format!("{} / {}", item.ts_code, item.adj_type)),
                    message: format!(
                        "已补算 {}/{} 组，当前 {} / {}，本组 {} 行。",
                        index + 1,
                        work_items.len(),
                        item.ts_code,
                        item.adj_type,
                        item.row_count
                    ),
                });
            }
        }

        Ok(())
    })?;

    let status = get_data_download_status(&prepared.source_path)?;

    Ok(DataDownloadRunResult {
        action: prepared.action.clone(),
        action_label: prepared.action_label.clone(),
        elapsed_ms: 0,
        summary: DataDownloadSummary {
            success_count: work_items.len() as u64,
            failed_count: 0,
            saved_rows: updated_rows,
            failed_items: Vec::new(),
        },
        status,
    })
}

pub fn get_indicator_manage_page(source_path: &str) -> Result<IndicatorManagePageData, String> {
    let trimmed = source_path.trim();
    if trimmed.is_empty() {
        return Err("数据目录为空，请先到数据管理页确认当前目录".to_string());
    }

    let path = ind_toml_path(trimmed);
    if !path.exists() {
        return Ok(IndicatorManagePageData {
            exists: false,
            file_path: path.display().to_string(),
            items: Vec::new(),
        });
    }

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
        let tokens = lex_all(&item.expr);
        let mut parser = Parser::new(tokens);
        parser
            .parse_main()
            .map_err(|e| format!("指标 {} 表达式解析错误在{}:{}", item.name, e.idx, e.msg))?;
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
