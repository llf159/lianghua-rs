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
    _fallback_total_share: Option<f64>,
) -> Result<(), String> {
    inject_constant_num_fields(row_data, &[("ZHANG", Some(calc_zhang_pct(ts_code, is_st)))])?;

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

pub fn load_estimated_market_cap_per_price_map(
    conn: &duckdb::Connection,
    source_dir: &str,
    adj_type: &str,
) -> Result<HashMap<String, f64>, String> {
    if !crate::data::stock_list_path(source_dir).exists() {
        return Ok(HashMap::new());
    }
    let mut anchors = Vec::new();
    for columns in load_stock_list(source_dir)? {
        let Some(ts_code) = columns.first().map(|v| v.trim()).filter(|v| !v.is_empty()) else {
            continue;
        };
        let Some(market_cap) = columns
            .get(9)
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v > 0.0)
        else {
            continue;
        };
        let date = columns
            .get(6)
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
            .unwrap_or("99999999");
        anchors.push(format!(
            "('{}', '{}', {})",
            ts_code.replace('\'', "''"),
            date.replace('\'', "''"),
            market_cap / 1e4
        ));
    }
    if anchors.is_empty() {
        return Ok(HashMap::new());
    }
    let sql = format!(
        r#"
        WITH anchors(ts_code, trade_date, market_cap) AS (VALUES {}),
        prices AS (
            SELECT ts_code, trade_date, TRY_CAST(close AS DOUBLE) AS price
            FROM stock_data WHERE adj_type = ?
        )
        SELECT a.ts_code, a.market_cap / p.price
        FROM anchors a ASOF LEFT JOIN prices p
          ON a.ts_code = p.ts_code AND a.trade_date >= p.trade_date
        WHERE p.price > 0 AND isfinite(p.price)
    "#,
        anchors.join(",")
    );
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("预编译推测市值锚点失败:{e}"))?;
    let mut rows = stmt
        .query(duckdb::params![adj_type])
        .map_err(|e| format!("查询推测市值锚点失败:{e}"))?;
    let mut out = HashMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取推测市值锚点失败:{e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| e.to_string())?;
        let value: f64 = row.get(1).map_err(|e| e.to_string())?;
        if value.is_finite() && value > 0.0 {
            out.insert(ts_code, value);
        }
    }
    Ok(out)
}

pub fn load_estimated_market_cap_series(
    conn: &duckdb::Connection,
    per_price: &HashMap<String, f64>,
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
    let sql = format!(
        "SELECT ts_code, trade_date, TRY_CAST(close AS DOUBLE) FROM stock_data WHERE adj_type = ? AND trade_date >= ? AND trade_date <= ? AND ts_code IN ({placeholders})"
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
        let price: Option<f64> = row.get(2).map_err(|e| e.to_string())?;
        if let Some(value) = price
            .zip(per_price.get(&ts_code))
            .map(|(p, m)| p * m)
            .filter(|v| v.is_finite() && *v > 0.0)
        {
            out.entry(ts_code).or_default().insert(trade_date, value);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estimated_market_cap_uses_snapshot_price_in_each_adjustment_basis() {
        let dir = std::env::temp_dir().join(format!(
            "lianghua_cap_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let source = dir.to_str().unwrap();
        std::fs::write(crate::data::stock_list_path(source), "ts_code,symbol,name,area,industry,list_date,trade_date,total_share,float_share,total_mv,circ_mv\n000001.SZ,,A,,,20200101,20240103,999999,0,2000000,0\n000002.SZ,,B,,,20200101,20240103,10000,0,,0\n").unwrap();
        let conn = duckdb::Connection::open(crate::data::source_db_path(source)).unwrap();
        conn.execute_batch("CREATE TABLE stock_data(ts_code VARCHAR,trade_date VARCHAR,adj_type VARCHAR,close DOUBLE); INSERT INTO stock_data VALUES ('000001.SZ','20240102','qfq',5),('000001.SZ','20240103','qfq',10),('000001.SZ','20240104','qfq',20),('000001.SZ','20240102','hfq',50),('000001.SZ','20240103','hfq',100),('000002.SZ','20240102','qfq',10);").unwrap();
        let codes = vec!["000001.SZ".to_string(), "000002.SZ".to_string()];
        for adj in ["qfq", "hfq"] {
            let per_price = load_estimated_market_cap_per_price_map(&conn, source, adj).unwrap();
            let series = load_estimated_market_cap_series(
                &conn, &per_price, &codes, adj, "20240102", "20240103",
            )
            .unwrap();
            assert_eq!(series["000001.SZ"]["20240102"], 100.0);
            assert_eq!(series["000001.SZ"]["20240103"], 200.0);
            assert!(!series.contains_key("000002.SZ"));
        }
        let reader = crate::data::DataReader::new_with_runtime_keys(
            source,
            &HashSet::from(["C".to_string()]),
        )
        .unwrap();
        let mut one = reader
            .load_one("000001.SZ", "qfq", "20240102", "20240102")
            .unwrap();
        inject_stock_extra_fields(&mut one, "000001.SZ", false, Some(999999.0)).unwrap();
        assert_eq!(one.cols["TOTAL_MV_YI"], vec![Some(100.0)]);
        let tail = reader
            .load_one_tail_rows("000001.SZ", "hfq", "20240103", 2)
            .unwrap();
        assert_eq!(tail.cols["TOTAL_MV_YI"], vec![Some(100.0), Some(200.0)]);
        let batch = reader
            .load_batch(&codes, "qfq", "20240102", "20240103")
            .unwrap();
        assert_eq!(
            batch["000001.SZ"].cols["TOTAL_MV_YI"],
            vec![Some(100.0), Some(200.0)]
        );
        assert_eq!(batch["000002.SZ"].cols["TOTAL_MV_YI"], vec![None]);
        drop(reader);
        drop(conn);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
