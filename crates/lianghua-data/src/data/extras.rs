use std::collections::{HashMap, HashSet};

use crate::data::{RowData, load_stock_list};

pub fn load_st_list(source_dir: &str) -> Result<HashSet<String>, String> {
    let rows = load_stock_list(source_dir)?;
    let mut st_list = HashSet::new();
    for columns in rows {
        let ts_code = columns
            .first()
            .ok_or_else(|| "stock_list.csv格式错误: 缺少ts_code列".to_string())?;
        let name = columns
            .get(2)
            .ok_or_else(|| "stock_list.csv格式错误: 缺少name列".to_string())?;

        if name.to_ascii_uppercase().contains("ST") {
            st_list.insert(ts_code.trim().to_string());
        }
    }
    Ok(st_list)
}

pub fn calc_zhang_pct(ts_code: &str, is_st: bool) -> f64 {
    let ts = ts_code.trim().to_ascii_uppercase();
    let (core, suffix) = ts.split_once('.').unwrap_or((ts.as_str(), ""));

    if is_st {
        0.045
    } else if suffix == "BJ" {
        0.295
    } else if core.starts_with("30") || core.starts_with("68") {
        0.195
    } else {
        0.095
    }
}

pub fn inject_constant_num_fields(
    row_data: &mut RowData,
    fields: &[(&str, Option<f64>)],
) -> Result<(), String> {
    let len = row_data.trade_dates.len();
    for (key, value) in fields {
        row_data.cols.insert((*key).to_string(), vec![*value; len]);
    }
    row_data.validate()
}

pub fn inject_latest_num_fields(
    row_data: &mut RowData,
    fields: &[(&str, Option<f64>)],
) -> Result<(), String> {
    let len = row_data.trade_dates.len();
    for (key, value) in fields {
        let mut series = vec![None; len];
        if let Some(last) = series.last_mut() {
            *last = *value;
        }
        row_data.cols.insert((*key).to_string(), series);
    }
    row_data.validate()
}

pub fn inject_stock_extra_fields(
    row_data: &mut RowData,
    ts_code: &str,
    is_st: bool,
    fallback_total_share: Option<f64>,
) -> Result<(), String> {
    inject_constant_num_fields(row_data, &[("ZHANG", Some(calc_zhang_pct(ts_code, is_st)))])?;

    let len = row_data.trade_dates.len();
    let close_series = row_data.cols.get("C");
    let total_share_series = row_data.cols.get("TOTAL_SHARE");
    let total_mv_yi_series = (0..len)
        .map(|index| {
            let close = close_series
                .and_then(|series| series.get(index).copied().flatten())
                .filter(|value| value.is_finite() && *value > 0.0)?;
            let total_share = total_share_series
                .and_then(|series| series.get(index).copied().flatten())
                .or(fallback_total_share)
                .filter(|value| value.is_finite() && *value > 0.0)?;
            Some(total_share * close / 1e4)
        })
        .collect::<Vec<_>>();

    row_data
        .cols
        .insert("TOTAL_MV_YI".to_string(), total_mv_yi_series);
    row_data.validate()
}

pub fn load_total_share_map(source_dir: &str) -> Result<HashMap<String, f64>, String> {
    let rows = load_stock_list(source_dir)?;
    let mut out = HashMap::with_capacity(rows.len());
    for columns in rows {
        let Some(ts_code) = columns
            .first()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let Some(raw_total_share) = columns
            .get(7)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let Ok(total_share) = raw_total_share.parse::<f64>() else {
            continue;
        };
        if total_share > 0.0 && total_share.is_finite() {
            out.insert(ts_code.to_string(), total_share);
        }
    }
    Ok(out)
}

