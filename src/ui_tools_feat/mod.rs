use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
};

use duckdb::{Connection, params_from_iter};

use crate::data::{
    load_stock_list, load_ths_concepts_list, load_ths_concepts_named_map, open_source_db_connection,
};

pub mod all_market_monitor;
pub mod chart_indicator;
pub mod chart_indicator_settings;
pub mod concept_stock_pick;
pub mod cyq_chen;
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
pub mod statistics;
pub mod stock_pick;
pub mod stock_similarity;
pub mod strategy_manage;
pub mod strategy_paper_validation;
pub mod strategy_trigger_similarity;
pub mod watch_observe;

const DEFAULT_ADJ_TYPE: &str = "qfq";

static STOCK_TEXT_MAP_CACHE: OnceLock<Mutex<HashMap<String, HashMap<String, String>>>> =
    OnceLock::new();
static STOCK_NUM_MAP_CACHE: OnceLock<Mutex<HashMap<String, HashMap<String, f64>>>> =
    OnceLock::new();

fn stock_text_map_cache() -> &'static Mutex<HashMap<String, HashMap<String, String>>> {
    STOCK_TEXT_MAP_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn stock_num_map_cache() -> &'static Mutex<HashMap<String, HashMap<String, f64>>> {
    STOCK_NUM_MAP_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn cached_string_map(
    cache_key: String,
    build: impl FnOnce() -> Result<HashMap<String, String>, String>,
) -> Result<HashMap<String, String>, String> {
    if let Some(value) = stock_text_map_cache()
        .lock()
        .map_err(|_| "基础文本缓存锁已损坏".to_string())?
        .get(&cache_key)
        .cloned()
    {
        return Ok(value);
    }

    let value = build()?;
    stock_text_map_cache()
        .lock()
        .map_err(|_| "基础文本缓存锁已损坏".to_string())?
        .insert(cache_key, value.clone());
    Ok(value)
}

fn cached_number_map(
    cache_key: String,
    build: impl FnOnce() -> Result<HashMap<String, f64>, String>,
) -> Result<HashMap<String, f64>, String> {
    if let Some(value) = stock_num_map_cache()
        .lock()
        .map_err(|_| "基础数值缓存锁已损坏".to_string())?
        .get(&cache_key)
        .cloned()
    {
        return Ok(value);
    }

    let value = build()?;
    stock_num_map_cache()
        .lock()
        .map_err(|_| "基础数值缓存锁已损坏".to_string())?
        .insert(cache_key, value.clone());
    Ok(value)
}

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

fn load_total_mv_map(source_dir: &str) -> Result<HashMap<String, f64>, String> {
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

pub fn build_total_mv_map(source_dir: &str) -> Result<HashMap<String, f64>, String> {
    cached_number_map(format!("{source_dir}\0total_mv"), || {
        load_total_mv_map(source_dir)
    })
}

fn load_circ_mv_map(source_dir: &str) -> Result<HashMap<String, f64>, String> {
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

pub fn build_circ_mv_map(source_dir: &str) -> Result<HashMap<String, f64>, String> {
    cached_number_map(format!("{source_dir}\0circ_mv"), || {
        load_circ_mv_map(source_dir)
    })
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

fn load_concepts_map(source_dir: &str) -> Result<HashMap<String, String>, String> {
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

pub fn build_concepts_map(source_dir: &str) -> Result<HashMap<String, String>, String> {
    cached_string_map(format!("{source_dir}\0concepts"), || {
        load_concepts_map(source_dir)
    })
}

pub fn build_most_related_concept_map(source_dir: &str) -> Result<HashMap<String, String>, String> {
    cached_string_map(format!("{source_dir}\0most_related_concept"), || {
        load_ths_concepts_named_map(source_dir, &["most_related_concept"])
    })
}

pub fn build_name_map(source_dir: &str) -> Result<HashMap<String, String>, String> {
    cached_string_map(format!("{source_dir}\0name"), || {
        build_stock_list_text_map(source_dir, 2)
    })
}

pub fn build_area_map(source_dir: &str) -> Result<HashMap<String, String>, String> {
    cached_string_map(format!("{source_dir}\0area"), || {
        build_stock_list_text_map(source_dir, 3)
    })
}

pub fn build_industry_map(source_dir: &str) -> Result<HashMap<String, String>, String> {
    cached_string_map(format!("{source_dir}\0industry"), || {
        build_stock_list_text_map(source_dir, 4)
    })
}

pub fn build_latest_vol_map(
    source_dir: &str,
    ts_codes: &[String],
) -> Result<HashMap<String, f64>, String> {
    if ts_codes.is_empty() {
        return Ok(HashMap::new());
    }

    let conn = open_source_db_connection(source_dir)?;

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

    conn.with(|conn| {
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
    })
}
