use std::{collections::HashSet, path::Path, time::Instant};

use duckdb::Connection;
use serde::Serialize;

use crate::{
    data::{load_trade_date_list, result_db_path, source_db_path},
    scoring::{TieBreakWay, build_rank_tiebreak, runner::scoring_all_to_db},
};

use super::normalize_trade_date;

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RankComputeDbRange {
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
pub struct RankComputeResultContinuity {
    pub checked: bool,
    pub is_continuous: bool,
    pub range_start: Option<String>,
    pub range_end: Option<String>,
    pub expected_trade_dates: u64,
    pub actual_trade_dates: u64,
    pub missing_trade_dates_count: u64,
    pub missing_trade_dates_sample: Vec<String>,
    pub unexpected_trade_dates_count: u64,
    pub unexpected_trade_dates_sample: Vec<String>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RankComputeStatus {
    pub source_path: String,
    pub source_db: RankComputeDbRange,
    pub result_db: RankComputeDbRange,
    pub result_db_continuity: RankComputeResultContinuity,
    pub suggested_start_date: Option<String>,
    pub suggested_end_date: Option<String>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RankComputeRunResult {
    pub action: String,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub elapsed_ms: u64,
    pub status: RankComputeStatus,
}

fn normalize_rank_compute_date(raw: &str, field_name: &str) -> Result<String, String> {
    normalize_trade_date(raw)
        .ok_or_else(|| format!("{field_name} 格式无效，应为 YYYYMMDD 或 YYYY-MM-DD"))
}

fn query_trade_date_range(
    db_path: &Path,
    file_name: &str,
    table_name: &str,
) -> Result<RankComputeDbRange, String> {
    if !db_path.exists() {
        return Ok(RankComputeDbRange {
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
        return Ok(RankComputeDbRange {
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
        return Ok(RankComputeDbRange {
            file_name: file_name.to_string(),
            table_name: table_name.to_string(),
            exists: true,
            min_trade_date,
            max_trade_date,
            distinct_trade_dates: distinct_trade_dates_i64.max(0) as u64,
            row_count: row_count_i64.max(0) as u64,
        });
    }

    Ok(RankComputeDbRange {
        file_name: file_name.to_string(),
        table_name: table_name.to_string(),
        exists: true,
        min_trade_date: None,
        max_trade_date: None,
        distinct_trade_dates: 0,
        row_count: 0,
    })
}

fn query_next_trade_date_after(
    db_path: &Path,
    after_trade_date: &str,
) -> Result<Option<String>, String> {
    if !db_path.exists() {
        return Ok(None);
    }

    let db_path_str = db_path
        .to_str()
        .ok_or_else(|| "原始库路径不是有效 UTF-8".to_string())?;
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
        return Ok(None);
    }

    conn.query_row(
        "SELECT MIN(trade_date) FROM stock_data WHERE trade_date > ?",
        [after_trade_date],
        |row| row.get::<_, Option<String>>(0),
    )
    .map_err(|e| format!("查询下一个交易日失败: {e}"))
}

fn query_distinct_trade_dates(
    db_path: &Path,
    file_name: &str,
    table_name: &str,
) -> Result<Vec<String>, String> {
    if !db_path.exists() {
        return Ok(Vec::new());
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
        return Ok(Vec::new());
    }

    let sql = format!(
        "SELECT DISTINCT trade_date FROM {table_name} WHERE trade_date IS NOT NULL ORDER BY trade_date"
    );
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("准备 {file_name} 交易日列表失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("读取 {file_name} 交易日列表失败: {e}"))?;
    let mut trade_dates = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("遍历 {file_name} 交易日列表失败: {e}"))?
    {
        let trade_date: String = row
            .get(0)
            .map_err(|e| format!("读取 {file_name} 交易日失败: {e}"))?;
        trade_dates.push(trade_date);
    }
    Ok(trade_dates)
}

fn sample_trade_dates(values: &[String], limit: usize) -> Vec<String> {
    values.iter().take(limit).cloned().collect()
}

fn check_result_db_continuity(
    source_path: &str,
    result_db_range: &RankComputeDbRange,
) -> Result<RankComputeResultContinuity, String> {
    if !result_db_range.exists {
        return Ok(RankComputeResultContinuity {
            checked: false,
            is_continuous: false,
            range_start: None,
            range_end: None,
            expected_trade_dates: 0,
            actual_trade_dates: 0,
            missing_trade_dates_count: 0,
            missing_trade_dates_sample: Vec::new(),
            unexpected_trade_dates_count: 0,
            unexpected_trade_dates_sample: Vec::new(),
        });
    }

    let Some(range_start) = result_db_range.min_trade_date.clone() else {
        return Ok(RankComputeResultContinuity {
            checked: false,
            is_continuous: false,
            range_start: None,
            range_end: None,
            expected_trade_dates: 0,
            actual_trade_dates: 0,
            missing_trade_dates_count: 0,
            missing_trade_dates_sample: Vec::new(),
            unexpected_trade_dates_count: 0,
            unexpected_trade_dates_sample: Vec::new(),
        });
    };
    let Some(range_end) = result_db_range.max_trade_date.clone() else {
        return Ok(RankComputeResultContinuity {
            checked: false,
            is_continuous: false,
            range_start: None,
            range_end: None,
            expected_trade_dates: 0,
            actual_trade_dates: 0,
            missing_trade_dates_count: 0,
            missing_trade_dates_sample: Vec::new(),
            unexpected_trade_dates_count: 0,
            unexpected_trade_dates_sample: Vec::new(),
        });
    };

    let expected_dates: Vec<String> = load_trade_date_list(source_path)?
        .into_iter()
        .filter(|trade_date| trade_date >= &range_start && trade_date <= &range_end)
        .collect();
    let expected_set: HashSet<&str> = expected_dates.iter().map(String::as_str).collect();
    let result_db = result_db_path(source_path);
    let actual_dates = query_distinct_trade_dates(&result_db, "scoring_result.db", "score_summary")?;
    let actual_set: HashSet<&str> = actual_dates.iter().map(String::as_str).collect();

    let missing_dates: Vec<String> = expected_dates
        .iter()
        .filter(|trade_date| !actual_set.contains(trade_date.as_str()))
        .cloned()
        .collect();
    let unexpected_dates: Vec<String> = actual_dates
        .iter()
        .filter(|trade_date| !expected_set.contains(trade_date.as_str()))
        .cloned()
        .collect();

    Ok(RankComputeResultContinuity {
        checked: true,
        is_continuous: missing_dates.is_empty() && unexpected_dates.is_empty(),
        range_start: Some(range_start),
        range_end: Some(range_end),
        expected_trade_dates: expected_dates.len() as u64,
        actual_trade_dates: actual_dates.len() as u64,
        missing_trade_dates_count: missing_dates.len() as u64,
        missing_trade_dates_sample: sample_trade_dates(&missing_dates, 8),
        unexpected_trade_dates_count: unexpected_dates.len() as u64,
        unexpected_trade_dates_sample: sample_trade_dates(&unexpected_dates, 8),
    })
}

fn get_rank_compute_status_inner(source_path: &str) -> Result<RankComputeStatus, String> {
    let source_db = source_db_path(source_path);
    let result_db = result_db_path(source_path);
    let source_db_range = query_trade_date_range(&source_db, "stock_data.db", "stock_data")?;
    let result_db_range = query_trade_date_range(&result_db, "scoring_result.db", "score_summary")?;
    let result_db_continuity = check_result_db_continuity(source_path, &result_db_range)?;

    let suggested_end_date = source_db_range.max_trade_date.clone();
    let suggested_start_date = match (
        source_db_range.min_trade_date.as_deref(),
        source_db_range.max_trade_date.as_deref(),
        result_db_range.max_trade_date.as_deref(),
    ) {
        (_, None, _) => None,
        (Some(source_min), Some(_), None) => Some(source_min.to_string()),
        (_, Some(source_max), Some(result_max)) if result_max < source_max => {
            query_next_trade_date_after(&source_db, result_max)?.or_else(|| Some(source_max.to_string()))
        }
        (_, Some(source_max), _) => Some(source_max.to_string()),
    };

    Ok(RankComputeStatus {
        source_path: source_path.trim().to_string(),
        source_db: source_db_range,
        result_db: result_db_range,
        result_db_continuity,
        suggested_start_date,
        suggested_end_date,
    })
}

pub fn get_ranking_compute_status(source_path: &str) -> Result<RankComputeStatus, String> {
    let trimmed = source_path.trim();
    if trimmed.is_empty() {
        return Err("数据目录为空，请先到数据管理页确认当前目录".to_string());
    }
    get_rank_compute_status_inner(trimmed)
}

pub fn run_ranking_score_calculation(
    source_path: &str,
    start_date: &str,
    end_date: &str,
) -> Result<RankComputeRunResult, String> {
    let source_path = source_path.trim().to_string();
    if source_path.is_empty() {
        return Err("数据目录为空，请先到数据管理页确认当前目录".to_string());
    }

    let start_date = normalize_rank_compute_date(start_date, "开始日期")?;
    let end_date = normalize_rank_compute_date(end_date, "结束日期")?;
    if start_date > end_date {
        return Err("开始日期不能晚于结束日期".to_string());
    }

    let started_at = Instant::now();
    scoring_all_to_db(&source_path, "qfq", &start_date, &end_date)?;
    let status = get_rank_compute_status_inner(&source_path)?;
    Ok(RankComputeRunResult {
        action: "score".to_string(),
        start_date: Some(start_date),
        end_date: Some(end_date),
        elapsed_ms: started_at.elapsed().as_millis() as u64,
        status,
    })
}

pub fn run_ranking_tiebreak_fill(source_path: &str) -> Result<RankComputeRunResult, String> {
    let source_path = source_path.trim().to_string();
    if source_path.is_empty() {
        return Err("数据目录为空，请先到数据管理页确认当前目录".to_string());
    }

    let started_at = Instant::now();
    let result_db = result_db_path(&source_path);
    let source_db = source_db_path(&source_path);
    let result_db_str = result_db
        .to_str()
        .ok_or_else(|| "结果库路径不是有效 UTF-8".to_string())?;
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "原始库路径不是有效 UTF-8".to_string())?;

    build_rank_tiebreak(result_db_str, source_db_str, "qfq", TieBreakWay::KdjJ)?;
    let status = get_rank_compute_status_inner(&source_path)?;
    Ok(RankComputeRunResult {
        action: "tiebreak".to_string(),
        start_date: None,
        end_date: None,
        elapsed_ms: started_at.elapsed().as_millis() as u64,
        status,
    })
}
