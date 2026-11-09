use std::collections::HashMap;

use duckdb::params;
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Serialize)]
pub struct DetailCompanyDataset {
    pub key: String,
    pub rows: Vec<Value>,
    pub error: Option<String>,
}

/// Company downloads are independent of the quote/result databases. Missing files or
/// tables are ordinary empty states; a failed dataset must not hide the other ones.
pub fn get_stock_detail_company(source_path: String, ts_code: String) -> Vec<DetailCompanyDataset> {
    let mut datasets = Vec::new();
    for (file, tables) in [
        (
            "company_fundamentals.db",
            &[
                "fina_indicator",
                "income",
                "balancesheet",
                "cashflow",
                "top10_holders",
                "top10_floatholders",
            ][..],
        ),
        ("shareholder_counts.db", &["stk_holdernumber"][..]),
    ] {
        let path = std::path::Path::new(&source_path).join(file);
        if !path.is_file() {
            continue;
        }
        let conn = match lianghua_data::data::open_db_readonly(path) {
            Ok(conn) => conn,
            Err(error) => {
                for table in tables {
                    datasets.push(DetailCompanyDataset {
                        key: table.to_string(),
                        rows: Vec::new(),
                        error: Some(format!("读取公司数据库失败: {error}")),
                    });
                }
                continue;
            }
        };
        for table in tables {
            let mut dataset = DetailCompanyDataset {
                key: table.to_string(),
                rows: Vec::new(),
                error: None,
            };
            let result = (|| -> Result<(), String> {
                let mut stmt = conn.prepare("SELECT column_name FROM duckdb_columns() WHERE table_name = ? AND schema_name = 'main'").map_err(|e| e.to_string())?;
                let columns = stmt
                    .query_map([table], |row| row.get::<_, String>(0))
                    .map_err(|e| e.to_string())?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| e.to_string())?;
                if columns.is_empty() {
                    return Ok(());
                }
                let mut sql =
                    format!("SELECT CAST(to_json(t) AS VARCHAR) FROM {table} t WHERE ts_code = ?");
                if columns.iter().any(|c| c == "report_type") {
                    sql.push_str(" AND CAST(report_type AS VARCHAR) = '1'");
                }
                sql.push_str(" ORDER BY end_date DESC NULLS LAST");
                if columns.iter().any(|c| c == "ann_date") {
                    sql.push_str(", ann_date DESC NULLS LAST");
                }
                if columns.iter().any(|c| c == "update_flag") {
                    sql.push_str(", update_flag DESC NULLS LAST");
                }
                // Read report groups rather than applying LIMIT 10 before deduplicating:
                // an updated shareholder announcement can repeat every holder.
                let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
                let mut rows = stmt
                    .query(params![lianghua_app_shared::canonical_ts_code(&ts_code)])
                    .map_err(|e| e.to_string())?;
                let mut seen = HashMap::new();
                let mut snapshot = None;
                while let Some(row) = rows.next().map_err(|e| e.to_string())? {
                    let value: Value =
                        serde_json::from_str(&row.get::<_, String>(0).map_err(|e| e.to_string())?)
                            .map_err(|e| e.to_string())?;
                    let period = value["end_date"].as_str().unwrap_or("").to_owned();
                    if table.starts_with("top10_") {
                        let group = (
                            period.clone(),
                            value["ann_date"].as_str().unwrap_or("").to_owned(),
                        );
                        if snapshot.as_ref().is_some_and(|s| s != &group) {
                            break;
                        }
                        snapshot = Some(group);
                        if seen.insert(value["holder_name"].to_string(), ()).is_some() {
                            continue;
                        }
                    } else if seen.insert(period, ()).is_some() {
                        continue;
                    }
                    dataset.rows.push(value);
                    if dataset.rows.len() >= if table.starts_with("top10_") { 10 } else { 8 } {
                        break;
                    }
                }
                if table.starts_with("top10_") {
                    dataset.rows.sort_by(|a, b| {
                        b["hold_amount"]
                            .as_f64()
                            .partial_cmp(&a["hold_amount"].as_f64())
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
                }
                Ok(())
            })();
            dataset.error = result.err();
            datasets.push(dataset);
        }
    }
    datasets
}
