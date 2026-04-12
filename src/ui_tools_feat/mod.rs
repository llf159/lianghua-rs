use std::collections::HashMap;

use duckdb::{Connection, params_from_iter};

use crate::data::{load_stock_list, load_ths_concepts_list, source_db_path};

pub mod concept_stock_pick;
pub mod data_download;
pub mod data_import;
pub mod data_viewer;
pub mod details;
pub mod expression_stock_pick;
pub mod intraday_monitor;
pub mod overview;
pub mod overview_classic;
pub mod ranking_compute;
pub mod realtime;
pub mod stock_pick;
pub mod strategy_manage;
pub mod statistics;
pub mod watch_observe;

const DEFAULT_ADJ_TYPE: &str = "qfq";

pub fn normalize_trade_date(raw: &str) -> Option<String> {
    let digits: String = raw
        .trim()
        .chars()
        .filter(|ch| ch.is_ascii_digit())
        .collect();
    if digits.len() == 8 {
        Some(digits)
    } else {
        None
    }
}

pub fn resolve_trade_date(conn: &Connection, trade_date: Option<String>) -> Result<String, String> {
    if let Some(d) = trade_date {
        let d = d.trim().to_string();
        if !d.is_empty() {
            return Ok(d);
        }
    }

    let mut stmt = conn
        .prepare("SELECT MAX(trade_date) FROM score_summary")
        .map_err(|e| format!("查询最新交易日预编译失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询最新交易日失败: {e}"))?;

    if let Some(row) = rows
        .next()
        .map_err(|e| format!("读取最新交易日结果失败: {e}"))?
    {
        let d: Option<String> = row
            .get(0)
            .map_err(|e| format!("读取最新交易日字段失败: {e}"))?;
        if let Some(v) = d {
            if !v.trim().is_empty() {
                return Ok(v);
            }
        }
    }
    Err("score_summary 没有可用交易日".to_string())
}

fn build_stock_list_text_map(
    source_dir: &str,
    value_index: usize,
) -> Result<HashMap<String, String>, String> {
    let stock_list = load_stock_list(source_dir)?;
    let mut out = HashMap::with_capacity(stock_list.len());

    for cols in stock_list {
        let Some(ts_code) = cols.first() else {
            continue;
        };
        let Some(value) = cols.get(value_index) else {
            continue;
        };

        let ts_code = ts_code.trim();
        let value = value.trim();
        if ts_code.is_empty() || value.is_empty() {
            continue;
        }

        out.insert(ts_code.to_string(), value.to_string());
    }

    Ok(out)
}

pub fn build_total_mv_map(source_dir: &str) -> Result<HashMap<String, f64>, String> {
    let stock_list = load_stock_list(source_dir)?;
    let mut out = HashMap::with_capacity(stock_list.len());
    for cols in stock_list {
        let Some(ts_code) = cols.first() else {
            continue;
        };
        let Some(total_mv_raw) = cols.get(9) else {
            continue;
        };
        let Ok(total_mv) = total_mv_raw.trim().parse::<f64>() else {
            continue;
        };
        out.insert(ts_code.trim().to_string(), total_mv / 1e4);
    }
    Ok(out)
}

pub fn build_circ_mv_map(source_dir: &str) -> Result<HashMap<String, f64>, String> {
    let stock_list = load_stock_list(source_dir)?;
    let mut out = HashMap::with_capacity(stock_list.len());
    for cols in stock_list {
        let Some(ts_code) = cols.first() else {
            continue;
        };
        let Some(circ_mv_raw) = cols.get(10) else {
            continue;
        };
        let Ok(circ_mv) = circ_mv_raw.trim().parse::<f64>() else {
            continue;
        };
        out.insert(ts_code.trim().to_string(), circ_mv / 1e4);
    }
    Ok(out)
}

pub fn filter_mv(
    total_mv_map: &HashMap<String, f64>,
    ts_code: &str,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
) -> bool {
    if total_mv_max.is_none() && total_mv_min.is_none() {
        return true;
    }
    let Some(total_mv) = total_mv_map.get(ts_code).copied() else {
        return false;
    };

    if let Some(min_v) = total_mv_min {
        if total_mv < min_v {
            return false;
        }
    }

    if let Some(max_v) = total_mv_max {
        if total_mv > max_v {
            return false;
        }
    }

    true
}

pub fn build_concepts_map(source_dir: &str) -> Result<HashMap<String, String>, String> {
    let concepts_list = load_ths_concepts_list(source_dir)?;
    let mut out = HashMap::new();
    for cols in concepts_list {
        let Some(ts_code) = cols.first() else {
            continue;
        };
        let Some(concept) = cols.get(2) else {
            continue;
        };
        out.insert(ts_code.to_string(), concept.to_string());
    }
    Ok(out)
}

pub fn build_name_map(source_dir: &str) -> Result<HashMap<String, String>, String> {
    build_stock_list_text_map(source_dir, 2)
}

pub fn build_area_map(source_dir: &str) -> Result<HashMap<String, String>, String> {
    build_stock_list_text_map(source_dir, 3)
}

pub fn build_industry_map(source_dir: &str) -> Result<HashMap<String, String>, String> {
    build_stock_list_text_map(source_dir, 4)
}

pub fn build_latest_vol_map(
    source_dir: &str,
    ts_codes: &[String],
) -> Result<HashMap<String, f64>, String> {
    if ts_codes.is_empty() {
        return Ok(HashMap::new());
    }

    let source_db = source_db_path(source_dir);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "原始库路径不是有效UTF-8".to_string())?;
    let conn = Connection::open(source_db_str).map_err(|e| format!("打开原始库失败: {e}"))?;

    let placeholders = std::iter::repeat_n("?", ts_codes.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
        SELECT ts_code, latest_vol
        FROM (
            SELECT
                ts_code,
                TRY_CAST(vol AS DOUBLE) AS latest_vol,
                ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS row_num
            FROM stock_data
            WHERE adj_type = ? AND ts_code IN ({placeholders})
        ) latest_rows
        WHERE row_num = 1
        "#
    );

    let mut params = Vec::with_capacity(ts_codes.len() + 1);
    params.push(DEFAULT_ADJ_TYPE.to_string());
    params.extend(ts_codes.iter().cloned());

    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("预编译最新成交量查询失败: {e}"))?;
    let mut rows = stmt
        .query(params_from_iter(params.iter()))
        .map_err(|e| format!("查询最新成交量失败: {e}"))?;

    let mut out = HashMap::with_capacity(ts_codes.len());
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取最新成交量失败: {e}"))?
    {
        let ts_code: String = row
            .get(0)
            .map_err(|e| format!("读取最新成交量代码失败: {e}"))?;
        let latest_vol: Option<f64> = row
            .get(1)
            .map_err(|e| format!("读取最新成交量数值失败: {e}"))?;
        if let Some(value) = latest_vol {
            out.insert(ts_code, value);
        }
    }

    Ok(out)
}
