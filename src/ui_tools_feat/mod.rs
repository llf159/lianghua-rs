use std::collections::HashMap;

use crate::data::{load_stock_list, load_ths_concepts_list};

pub mod concept_stock_pick;
pub mod data_download;
pub mod data_import;
pub mod data_viewer;
pub mod expression_stock_pick;
pub mod ranking_compute;
pub mod stock_pick;
pub mod strategy_manage;

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
