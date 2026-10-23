use std::collections::{HashMap, HashSet};

use crate::data::{RowData, load_stock_list};
use lianghua_core::utils::utils::board_category;

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
    _fallback_total_share: Option<f64>,
) -> Result<(), String> {
    let board_id = match board_category(ts_code, None) {
        "主板" => Some(1.0),
        "创业板" => Some(2.0),
        "科创板" => Some(3.0),
        "北交所" => Some(4.0),
        _ => None,
    };
    inject_constant_num_fields(
        row_data,
        &[
            ("ZHANG", Some(calc_zhang_pct(ts_code, is_st))),
            ("BOARD_ID", board_id),
        ],
    )?;

    row_data
        .cols
        .entry("TOTAL_MV_YI".to_string())
        .or_insert_with(|| vec![None; row_data.trade_dates.len()]);
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

/// 成交额为千元，换手率为百分数；结果为按成交均价推算的流通市值（亿）。
pub fn estimate_market_cap(amount: Option<f64>, turnover: Option<f64>) -> Option<f64> {
    amount
        .zip(turnover)
        .filter(|(amount, turnover)| {
            amount.is_finite() && *amount > 0.0 && turnover.is_finite() && *turnover > 0.0
        })
        .map(|(amount, turnover)| amount / turnover / 1000.0)
        .filter(|value| value.is_finite() && *value > 0.0)
}

pub fn estimated_market_cap_sql(conn: &duckdb::Connection) -> Result<String, String> {
    let mut stmt = conn
        .prepare("SELECT name FROM pragma_table_info('stock_data')")
        .map_err(|e| format!("读取推测市值字段失败:{e}"))?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    if !columns.iter().any(|name| name.eq_ignore_ascii_case("tor")) {
        return Ok("NULL::DOUBLE".to_string());
    }
    Ok("CASE WHEN TRY_CAST(amount AS DOUBLE) > 0 AND isfinite(TRY_CAST(amount AS DOUBLE)) AND TRY_CAST(tor AS DOUBLE) > 0 AND isfinite(TRY_CAST(tor AS DOUBLE)) AND isfinite(TRY_CAST(amount AS DOUBLE) / TRY_CAST(tor AS DOUBLE) / 1000.0) THEN TRY_CAST(amount AS DOUBLE) / TRY_CAST(tor AS DOUBLE) / 1000.0 END".to_string())
}

pub fn load_estimated_market_cap_series(
    conn: &duckdb::Connection,
    ts_codes: &[String],
    adj_type: &str,
    start_date: &str,
    end_date: &str,
) -> Result<HashMap<String, HashMap<String, f64>>, String> {
    if ts_codes.is_empty() {
        return Ok(HashMap::new());
    }
    let placeholders = std::iter::repeat_n("?", ts_codes.len())
        .collect::<Vec<_>>()
        .join(",");
    let market_cap_sql = estimated_market_cap_sql(conn)?;
    let sql = format!(
        "SELECT ts_code, trade_date, {market_cap_sql} FROM stock_data WHERE adj_type = ? AND trade_date >= ? AND trade_date <= ? AND ts_code IN ({placeholders})"
    );
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("预编译推测市值序列失败:{e}"))?;
    let mut rows = stmt
        .query(duckdb::params_from_iter(
            [adj_type, start_date, end_date]
                .into_iter()
                .chain(ts_codes.iter().map(String::as_str)),
        ))
        .map_err(|e| format!("查询推测市值序列失败:{e}"))?;
    let mut out: HashMap<String, HashMap<String, f64>> = HashMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取推测市值序列失败:{e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| e.to_string())?;
        let trade_date: String = row.get(1).map_err(|e| e.to_string())?;
        let value: Option<f64> = row.get(2).map_err(|e| e.to_string())?;
        if let Some(value) = value.filter(|v| v.is_finite() && *v > 0.0) {
            out.entry(ts_code).or_default().insert(trade_date, value);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estimated_market_cap_uses_daily_amount_and_turnover() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE stock_data(ts_code VARCHAR, trade_date VARCHAR, adj_type VARCHAR, close DOUBLE, amount DOUBLE, tor DOUBLE);
            INSERT INTO stock_data VALUES
            ('A','20240101','qfq',10,1000000,2),
            ('A','20240102','qfq',99,600000,3),
            ('A','20240103','qfq',99,600000,0),
            ('A','20240104','qfq',99,600000,NULL),
            ('A','20240105','qfq',99,-1,1),
            ('A','20240106','qfq',99,'Infinity',1),
            ('A','20240101','hfq',100,1000000,2);").unwrap();
        let codes = vec!["A".to_string()];
        let series =
            load_estimated_market_cap_series(&conn, &codes, "qfq", "20240101", "20240106").unwrap();
        assert_eq!(series["A"].len(), 2);
        assert_eq!(series["A"]["20240101"], 500.0);
        assert_eq!(series["A"]["20240102"], 200.0);
        assert_eq!(
            load_estimated_market_cap_series(&conn, &codes, "hfq", "20240101", "20240101").unwrap()
                ["A"]["20240101"],
            500.0
        );
        for (amount, tor) in [
            (None, Some(1.0)),
            (Some(1.0), None),
            (Some(1.0), Some(0.0)),
            (Some(-1.0), Some(1.0)),
            (Some(f64::INFINITY), Some(1.0)),
            (Some(1.0), Some(f64::NAN)),
        ] {
            assert_eq!(estimate_market_cap(amount, tor), None);
        }
        assert_eq!(estimate_market_cap(Some(1000000.0), Some(2.0)), Some(500.0));
        let reader = crate::data::DataReader::new_inner(
            "",
            Some(&HashSet::from(["C".to_string()])),
            Some(conn),
        )
        .unwrap();
        let expected = vec![Some(500.0), Some(200.0), None, None, None, None];
        assert_eq!(
            reader
                .load_one("A", "qfq", "20240101", "20240106")
                .unwrap()
                .cols["TOTAL_MV_YI"],
            expected
        );
        assert_eq!(
            reader
                .load_one_tail_rows("A", "qfq", "20240106", 6)
                .unwrap()
                .cols["TOTAL_MV_YI"],
            expected
        );
        assert_eq!(
            reader
                .load_batch(&codes, "qfq", "20240101", "20240106")
                .unwrap()["A"]
                .cols["TOTAL_MV_YI"],
            expected
        );
        reader
            .conn
            .execute_batch("ALTER TABLE stock_data DROP COLUMN tor")
            .unwrap();
        assert!(
            load_estimated_market_cap_series(&reader.conn, &codes, "qfq", "20240101", "20240106")
                .unwrap()
                .is_empty()
        );
    }
}
