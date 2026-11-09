use std::collections::HashMap;

use duckdb::{params, types::ValueRef};
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
                let mut stmt = conn.prepare("SELECT column_name FROM duckdb_columns() WHERE table_name = ? AND schema_name = 'main' ORDER BY column_index").map_err(|e| e.to_string())?;
                let columns = stmt
                    .query_map([table], |row| row.get::<_, String>(0))
                    .map_err(|e| e.to_string())?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| e.to_string())?;
                if columns.is_empty() {
                    return Ok(());
                }
                let mut sql = format!("SELECT * FROM {table} WHERE ts_code = ?");
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
                    // CompanyDataWriter stores VARCHAR/DOUBLE columns. Serialize here so
                    // Android does not need DuckDB's dynamically installed JSON extension.
                    let mut value = serde_json::Map::new();
                    for (index, column) in columns.iter().enumerate() {
                        let field = match row.get_ref(index).map_err(|e| e.to_string())? {
                            ValueRef::Null => Value::Null,
                            ValueRef::Text(text) => Value::String(
                                std::str::from_utf8(text)
                                    .map_err(|e| e.to_string())?
                                    .to_owned(),
                            ),
                            ValueRef::Double(number) => serde_json::Number::from_f64(number)
                                .map(Value::Number)
                                .unwrap_or(Value::Null),
                            other => {
                                return Err(format!("公司数据字段 {column} 类型不支持: {other:?}"));
                            }
                        };
                        value.insert(column.clone(), field);
                    }
                    let value = Value::Object(value);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn company_details_preserve_values_and_report_selection() {
        let directory = tempfile::tempdir().unwrap();
        let conn =
            duckdb::Connection::open(directory.path().join("company_fundamentals.db")).unwrap();
        conn.execute_batch(
            "CREATE TABLE income (ts_code VARCHAR, end_date VARCHAR, ann_date VARCHAR,
                report_type VARCHAR, update_flag VARCHAR, revenue DOUBLE, remark VARCHAR);
             INSERT INTO income VALUES
                ('000001.SZ', '20260930', '20261030', '1', '0', 1.5, '旧版'),
                ('000001.SZ', '20260930', '20261030', '1', '1', 2.5, '修订\"版'),
                ('000001.SZ', '20260930', '20261031', '2', '1', 9, '非合并'),
                ('000001.SZ', '20260630', NULL, '1', NULL, NULL, NULL),
                ('600519.SH', '20260930', '20261030', '1', '1', 99, '其他股票');
             CREATE TABLE top10_holders (ts_code VARCHAR, end_date VARCHAR,
                ann_date VARCHAR, holder_name VARCHAR, hold_amount DOUBLE);
             INSERT INTO top10_holders VALUES
                ('000001.SZ', '20260930', '20261030', '股东甲', 10),
                ('000001.SZ', '20260930', '20261030', '股东甲', 10),
                ('000001.SZ', '20260930', '20261030', '股东乙', 20),
                ('000001.SZ', '20260930', '20261029', '旧股东', 100);",
        )
        .unwrap();
        drop(conn);

        let datasets = get_stock_detail_company(
            directory.path().to_str().unwrap().to_owned(),
            "000001.SZ".to_owned(),
        );
        assert!(
            datasets.iter().all(|dataset| dataset.error.is_none()),
            "{datasets:?}"
        );
        let income = &datasets
            .iter()
            .find(|dataset| dataset.key == "income")
            .unwrap()
            .rows;
        assert_eq!(income.len(), 2);
        assert_eq!(income[0]["revenue"], 2.5);
        assert_eq!(income[0]["remark"], "修订\"版");
        assert_eq!(income[0]["end_date"], "20260930");
        assert_eq!(income[1]["revenue"], Value::Null);
        assert_eq!(income[1]["remark"], Value::Null);
        let holders = &datasets
            .iter()
            .find(|dataset| dataset.key == "top10_holders")
            .unwrap()
            .rows;
        assert_eq!(holders.len(), 2);
        assert_eq!(holders[0]["holder_name"], "股东乙");
        assert_eq!(holders[1]["holder_name"], "股东甲");
        assert!(
            datasets
                .iter()
                .find(|dataset| dataset.key == "cashflow")
                .unwrap()
                .rows
                .is_empty()
        );
    }
}
