pub mod board_analysis;
pub mod data_download;
pub mod details;
pub mod market_monitor;
pub mod overview;
pub mod realtime;
pub mod return_backtest;
pub mod statistics;
pub mod stock_pick;
pub mod strategy_manage;
pub mod strategy_performance;
pub mod watch_observe;

use std::collections::HashMap;

use duckdb::Connection;

use crate::data::{load_stock_list, load_ths_concepts_list};

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

fn build_total_mv_map(source_dir: &str) -> Result<HashMap<String, f64>, String> {
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
        // total_mv 单位是万元，这里统一换算到“亿”
        out.insert(ts_code.trim().to_string(), total_mv / 1e4);
    }
    Ok(out)
}

fn build_circ_mv_map(source_dir: &str) -> Result<HashMap<String, f64>, String> {
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
        // circ_mv 单位是万元，这里统一换算到“亿”
        out.insert(ts_code.trim().to_string(), circ_mv / 1e4);
    }
    Ok(out)
}

fn filter_mv(
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

fn build_concepts_map(source_dir: &str) -> Result<HashMap<String, String>, String> {
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

// fn concept_query(ts_code: &str, concepts_map: &HashMap<String, String>) -> String {
//     concepts_map.get(ts_code).cloned().unwrap_or_default()
// }

fn build_name_map(source_dir: &str) -> Result<HashMap<String, String>, String> {
    build_stock_list_text_map(source_dir, 2)
}

fn build_area_map(source_dir: &str) -> Result<HashMap<String, String>, String> {
    build_stock_list_text_map(source_dir, 3)
}

fn build_industry_map(source_dir: &str) -> Result<HashMap<String, String>, String> {
    build_stock_list_text_map(source_dir, 4)
}

fn resolve_trade_date(conn: &Connection, trade_date: Option<String>) -> Result<String, String> {
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
