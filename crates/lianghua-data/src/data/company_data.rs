use std::{collections::HashMap, fs, path::Path};

use duckdb::{Connection, appender_params_from_iter, params, types::Value};

/// A successful query window is replaced and recorded in the same transaction.
pub fn open_company_db(source_dir: &str) -> Result<Connection, String> {
    fs::create_dir_all(source_dir).map_err(|e| format!("创建公司数据目录失败: {e}"))?;
    let conn = Connection::open(Path::new(source_dir).join("company_fundamentals.db"))
        .map_err(|e| format!("打开公司数据库失败: {e}"))?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS company_sync_log (
        api_name VARCHAR, ts_code VARCHAR, start_date VARCHAR, end_date VARCHAR,
        row_count BIGINT, synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (api_name, ts_code, start_date, end_date))",
    )
    .map_err(|e| format!("初始化公司下载记录失败: {e}"))?;
    Ok(conn)
}

#[derive(Clone)]
pub struct CompanyDownloadWindow {
    pub api: String,
    pub code: String,
    pub date_field: String,
    pub start: String,
    pub end: String,
    pub fields: Vec<String>,
    pub items: Vec<Vec<serde_json::Value>>,
}

/// Commit the successful windows in one download batch, including their completion records.
pub fn replace_company_batch(
    conn: &mut Connection,
    windows: &[CompanyDownloadWindow],
) -> Result<(), String> {
    if windows.is_empty() {
        return Ok(());
    }
    lianghua_model::task::checkpoint()?;
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    for window in windows {
        lianghua_model::task::checkpoint()?;
        let api = window.api.as_str();
        let code = window.code.as_str();
        let date_field = window.date_field.as_str();
        let start = window.start.as_str();
        let end = window.end.as_str();
        let fields = &window.fields;
        let items = &window.items;
        // Identifiers come from the API, never interpolate unchecked response strings into SQL.
        for name in std::iter::once(api)
            .chain(std::iter::once(date_field))
            .chain(fields.iter().map(String::as_str))
        {
            if name.is_empty()
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
            {
                return Err(format!("公司数据字段名无效: {name}"));
            }
        }
        let code_index = fields
            .iter()
            .position(|f| f == "ts_code")
            .ok_or("公司数据缺少 ts_code")?;
        let date_index = fields
            .iter()
            .position(|f| f == date_field)
            .ok_or_else(|| format!("{api} 缺少 {date_field}"))?;
        for row in items {
            if row.len() != fields.len()
                || row[code_index].as_str() != Some(code)
                || row[date_index]
                    .as_str()
                    .is_none_or(|d| d < start || d > end)
            {
                return Err(format!(
                    "{api} 返回字段数、股票或日期范围不匹配，保留旧数据"
                ));
            }
        }
        let mut existing = HashMap::<String, String>::new();
        {
            let mut stmt = tx
                .prepare("SELECT column_name, data_type FROM duckdb_columns() WHERE table_name = ?")
                .map_err(|e| e.to_string())?;
            let rows = stmt
                .query_map([api], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                })
                .map_err(|e| e.to_string())?;
            for row in rows {
                let (name, ty) = row.map_err(|e| e.to_string())?;
                existing.insert(name, ty);
            }
        }
        let types: Vec<&str> = fields
            .iter()
            .enumerate()
            .map(|(i, field)| {
                if existing.get(field).is_some_and(|ty| ty == "VARCHAR")
                    || field.ends_with("date")
                    || matches!(
                        field.as_str(),
                        "ts_code"
                            | "report_type"
                            | "comp_type"
                            | "end_type"
                            | "update_flag"
                            | "type"
                            | "summary"
                            | "change_reason"
                            | "perf_summary"
                            | "remark"
                            | "curr_type"
                            | "bz_item"
                            | "bz_code"
                            | "div_proc"
                            | "holder_name"
                            | "holder_type"
                            | "in_de"
                            | "share_type"
                            | "cur_stage"
                            | "name"
                            | "title"
                    )
                    || items.iter().any(|row| row[i].is_string())
                {
                    "VARCHAR"
                } else {
                    "DOUBLE"
                }
            })
            .collect();
        if existing.is_empty() {
            let columns = fields
                .iter()
                .zip(&types)
                .map(|(name, ty)| format!("\"{name}\" {ty}"))
                .collect::<Vec<_>>()
                .join(",");
            tx.execute_batch(&format!("CREATE TABLE \"{api}\" ({columns}); CREATE INDEX \"idx_{api}_code_date\" ON \"{api}\" (ts_code, \"{date_field}\")"))
            .map_err(|e| format!("创建 {api} 表失败: {e}"))?;
        } else {
            for (name, ty) in fields.iter().zip(&types) {
                if !existing.contains_key(name) {
                    tx.execute_batch(&format!("ALTER TABLE \"{api}\" ADD COLUMN \"{name}\" {ty}"))
                        .map_err(|e| e.to_string())?;
                } else if existing[name] != *ty {
                    tx.execute_batch(&format!(
                        "ALTER TABLE \"{api}\" ALTER COLUMN \"{name}\" TYPE {ty}"
                    ))
                    .map_err(|e| e.to_string())?;
                }
            }
        }
        tx.execute(
            &format!(
                "DELETE FROM \"{api}\" WHERE ts_code = ? AND \"{date_field}\" BETWEEN ? AND ?"
            ),
            params![code, start, end],
        )
        .map_err(|e| format!("替换 {api} 旧数据失败: {e}"))?;
        {
            let names = fields.iter().map(String::as_str).collect::<Vec<_>>();
            let mut appender = tx
                .appender_with_columns(api, &names)
                .map_err(|e| e.to_string())?;
            for row in items {
                let values = row
                    .iter()
                    .zip(&types)
                    .map(|(v, ty)| {
                        if v.is_null() {
                            Ok(Value::Null)
                        } else if *ty == "VARCHAR" {
                            Ok(Value::Text(
                                v.as_str()
                                    .map(str::to_owned)
                                    .unwrap_or_else(|| v.to_string()),
                            ))
                        } else {
                            v.as_f64()
                                .filter(|n| n.is_finite())
                                .map(Value::Double)
                                .ok_or_else(|| format!("{api} 数值无效: {v}"))
                        }
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                appender
                    .append_row(appender_params_from_iter(values))
                    .map_err(|e| format!("追加 {api} 数据失败: {e}"))?;
            }
            appender.flush().map_err(|e| e.to_string())?;
        }
        tx.execute("INSERT INTO company_sync_log VALUES (?, ?, ?, ?, ?, now())
        ON CONFLICT (api_name, ts_code, start_date, end_date) DO UPDATE SET row_count = excluded.row_count, synced_at = now()",
        params![api, code, start, end, items.len() as i64]).map_err(|e| e.to_string())?;
    }
    lianghua_model::task::checkpoint()?;
    tx.commit()
        .map_err(|e| format!("提交公司数据批次失败: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn company_batches_replace_atomically_preserve_variants_and_nulls() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE company_sync_log (api_name VARCHAR, ts_code VARCHAR, start_date VARCHAR, end_date VARCHAR, row_count BIGINT, synced_at TIMESTAMP, PRIMARY KEY(api_name,ts_code,start_date,end_date))").unwrap();
        let window = CompanyDownloadWindow {
            api: "income".into(),
            code: "600519.SH".into(),
            date_field: "ann_date".into(),
            start: "20240101".into(),
            end: "20241231".into(),
            fields: [
                "ts_code",
                "ann_date",
                "end_date",
                "report_type",
                "revenue",
                "note",
            ]
            .map(str::to_owned)
            .to_vec(),
            items: Vec::new(),
        };
        for amount in [100.0, 200.0] {
            replace_company_batch(
                &mut conn,
                &[CompanyDownloadWindow {
                    items: vec![
                        vec![
                            json!("600519.SH"),
                            json!("20240401"),
                            json!("20231231"),
                            json!("1"),
                            json!(amount),
                            json!(null),
                        ],
                        vec![
                            json!("600519.SH"),
                            json!("20240401"),
                            json!("20231231"),
                            json!("4"),
                            json!(null),
                            json!(null),
                        ],
                    ],
                    ..window.clone()
                }],
            )
            .unwrap();
        }
        assert_eq!(
            conn.query_row(
                "SELECT count(*), max(revenue), count(revenue) FROM income",
                [],
                |r| Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, f64>(1)?,
                    r.get::<_, i64>(2)?
                ))
            )
            .unwrap(),
            (2, 200.0, 1)
        );
        // A failure in the second window rolls back the earlier window in the same batch.
        assert!(
            replace_company_batch(
                &mut conn,
                &[
                    window.clone(),
                    CompanyDownloadWindow {
                        items: vec![vec![
                            json!("600519.SH"),
                            json!("20240401"),
                            json!("20231231"),
                            json!("1"),
                            json!({"bad":true}),
                            json!(null)
                        ]],
                        ..window.clone()
                    }
                ]
            )
            .is_err()
        );
        assert_eq!(
            conn.query_row("SELECT count(*) FROM income", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert_eq!(
            conn.query_row("SELECT row_count FROM company_sync_log", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            2
        );
        replace_company_batch(&mut conn, &[window]).unwrap();
        assert_eq!(
            conn.query_row("SELECT count(*) FROM income", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
        let empty = CompanyDownloadWindow {
            api: "forecast".into(),
            code: "000001.SZ".into(),
            date_field: "ann_date".into(),
            start: "20230101".into(),
            end: "20241231".into(),
            fields: ["ts_code", "ann_date", "type"].map(str::to_owned).to_vec(),
            items: Vec::new(),
        };
        replace_company_batch(
            &mut conn,
            &[
                empty.clone(),
                CompanyDownloadWindow {
                    code: "600519.SH".into(),
                    items: vec![vec![json!("600519.SH"), json!("20240101"), json!("预增")]],
                    ..empty
                },
            ],
        )
        .unwrap();
        assert_eq!(
            conn.query_row("SELECT type FROM forecast", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "预增"
        );
    }
}
