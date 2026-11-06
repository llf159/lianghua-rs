use std::{collections::HashMap, fs, path::Path, sync::Arc};

use duckdb::{
    Connection,
    arrow::{
        array::{ArrayRef, Float64Builder, StringBuilder},
        record_batch::RecordBatch,
    },
    params,
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CompanyDbKind {
    Fundamentals,
    ShareholderCounts,
}

impl CompanyDbKind {
    pub fn file_name(self) -> &'static str {
        match self {
            Self::Fundamentals => "company_fundamentals.db",
            Self::ShareholderCounts => "shareholder_counts.db",
        }
    }
}

/// A successful query window is replaced and recorded in the same transaction.
pub fn open_company_db(source_dir: &str, kind: CompanyDbKind) -> Result<Connection, String> {
    fs::create_dir_all(source_dir).map_err(|e| format!("创建公司数据目录失败: {e}"))?;
    let conn = Connection::open(Path::new(source_dir).join(kind.file_name()))
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

/// Schema cache belongs to this exclusive writer and never outlives its connection borrow.
pub struct CompanyDataWriter<'a> {
    conn: &'a mut Connection,
    schemas: HashMap<String, HashMap<String, String>>,
}

impl<'a> CompanyDataWriter<'a> {
    pub fn new(conn: &'a mut Connection) -> Self {
        Self {
            conn,
            schemas: HashMap::new(),
        }
    }

    /// Commit one stock's downloaded windows and completion records atomically.
    pub fn replace_stock(&mut self, windows: &[CompanyDownloadWindow]) -> Result<(), String> {
        let result = (|| {
            if windows.is_empty() {
                return Ok(());
            }
            let code = windows[0].code.as_str();
            if windows.iter().any(|window| window.code != code) {
                return Err("单股事务只能包含同一只股票的下载窗口".into());
            }
            lianghua_model::task::checkpoint()?;
            let tx = self.conn.transaction().map_err(|e| e.to_string())?;
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
                if !self.schemas.contains_key(api) {
                    let mut existing = HashMap::<String, String>::new();
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
                    self.schemas.insert(api.to_owned(), existing);
                }
                let existing = self.schemas.get_mut(api).expect("schema was loaded");
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
                } else if fields
                    .iter()
                    .zip(&types)
                    .any(|(name, ty)| existing.get(name).is_none_or(|current| current != ty))
                {
                    // DuckDB's index dependency blocks ALTER even for other columns.
                    tx.execute_batch(&format!("DROP INDEX IF EXISTS \"idx_{api}_code_date\""))
                        .map_err(|e| e.to_string())?;
                    for (name, ty) in fields.iter().zip(&types) {
                        if !existing.contains_key(name) {
                            tx.execute_batch(&format!(
                                "ALTER TABLE \"{api}\" ADD COLUMN \"{name}\" {ty}"
                            ))
                            .map_err(|e| e.to_string())?;
                        } else if existing[name] != *ty {
                            tx.execute_batch(&format!(
                                "ALTER TABLE \"{api}\" ALTER COLUMN \"{name}\" TYPE {ty}"
                            ))
                            .map_err(|e| e.to_string())?;
                        }
                    }
                    tx.execute_batch(&format!("CREATE INDEX \"idx_{api}_code_date\" ON \"{api}\" (ts_code, \"{date_field}\")"))
                        .map_err(|e| e.to_string())?;
                }
                for (field, ty) in fields.iter().zip(&types) {
                    if existing.get(field).is_none_or(|current| current != ty) {
                        existing.insert(field.clone(), (*ty).to_owned());
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
                    // Bound column buffers even when a split response covers a long history.
                    for rows in items.chunks(8192) {
                        lianghua_model::task::checkpoint()?;
                        let mut columns = Vec::with_capacity(fields.len());
                        for (i, (field, ty)) in fields.iter().zip(&types).enumerate() {
                            let column: ArrayRef = if *ty == "VARCHAR" {
                                let mut values = StringBuilder::new();
                                for row in rows {
                                    let value = &row[i];
                                    if value.is_null() {
                                        values.append_null();
                                    } else if let Some(text) = value.as_str() {
                                        values.append_value(text);
                                    } else {
                                        values.append_value(value.to_string());
                                    }
                                }
                                Arc::new(values.finish())
                            } else {
                                let mut values = Float64Builder::with_capacity(rows.len());
                                for row in rows {
                                    let value = &row[i];
                                    if value.is_null() {
                                        values.append_null();
                                    } else {
                                        let number = value
                                            .as_f64()
                                            .filter(|n| n.is_finite())
                                            .ok_or_else(|| format!("{api} 数值无效: {value}"))?;
                                        values.append_value(number);
                                    }
                                }
                                Arc::new(values.finish())
                            };
                            columns.push((field.as_str(), column));
                        }
                        let batch = RecordBatch::try_from_iter(columns)
                            .map_err(|e| format!("构造 {api} Arrow 批次失败: {e}"))?;
                        appender
                            .append_record_batch(batch)
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
                .map_err(|e| format!("提交股票数据事务失败: {e}"))
        })();
        // DDL may have rolled back; reload schemas on the next stock after any failure.
        if result.is_err() {
            self.schemas.clear();
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn company_stock_transaction_replaces_atomically_and_preserves_variants_and_nulls() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE company_sync_log (api_name VARCHAR, ts_code VARCHAR, start_date VARCHAR, end_date VARCHAR, row_count BIGINT, synced_at TIMESTAMP, PRIMARY KEY(api_name,ts_code,start_date,end_date))").unwrap();
        let mut writer = CompanyDataWriter::new(&mut conn);
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
            writer
                .replace_stock(&[CompanyDownloadWindow {
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
                }])
                .unwrap();
        }
        assert_eq!(
            writer
                .conn
                .query_row(
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
            writer
                .replace_stock(&[
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
                ])
                .is_err()
        );
        assert_eq!(
            writer
                .conn
                .query_row("SELECT count(*) FROM income", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert_eq!(
            writer
                .conn
                .query_row("SELECT row_count FROM company_sync_log", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            2
        );
        writer.replace_stock(&[window]).unwrap();
        assert_eq!(
            writer
                .conn
                .query_row("SELECT count(*) FROM income", [], |r| r.get::<_, i64>(0))
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
        writer.replace_stock(&[empty.clone()]).unwrap();
        writer
            .replace_stock(&[CompanyDownloadWindow {
                code: "600519.SH".into(),
                items: vec![vec![json!("600519.SH"), json!("20240101"), json!("预增")]],
                ..empty
            }])
            .unwrap();
        assert_eq!(
            writer
                .conn
                .query_row("SELECT type FROM forecast", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "预增"
        );
    }

    #[test]
    fn company_arrow_chunks_preserve_column_order_and_reload_rolled_back_schema() {
        let directory = std::env::temp_dir().join(format!("company-arrow-{}", std::process::id()));
        let mut conn =
            open_company_db(directory.to_str().unwrap(), CompanyDbKind::Fundamentals).unwrap();
        let mut writer = CompanyDataWriter::new(&mut conn);
        let window = CompanyDownloadWindow {
            api: "daily_basic".into(),
            code: "600519.SH".into(),
            date_field: "trade_date".into(),
            start: "20240101".into(),
            end: "20241231".into(),
            fields: ["ts_code", "trade_date", "total_share"]
                .map(str::to_owned)
                .to_vec(),
            items: (0..8193)
                .map(|i| {
                    vec![
                        json!("600519.SH"),
                        json!("20240101"),
                        if i == 8192 {
                            json!(null)
                        } else {
                            json!(i as f64)
                        },
                    ]
                })
                .collect(),
        };
        writer.replace_stock(&[window.clone()]).unwrap();
        assert_eq!(
            writer
                .conn
                .query_row(
                    "SELECT count(*), count(total_share), sum(total_share) FROM daily_basic",
                    [],
                    |r| Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, f64>(2)?
                    ))
                )
                .unwrap(),
            (8193, 8192, 33550336.0)
        );
        // Upgrade a numeric column to text and add a column, then fail after the first
        // Arrow chunk. Both DDL and already-appended rows must roll back.
        let mut changed = CompanyDownloadWindow {
            fields: ["new_value", "total_share", "trade_date", "ts_code"]
                .map(str::to_owned)
                .to_vec(),
            items: (0..8193)
                .map(|i| {
                    vec![
                        if i == 8192 {
                            json!({"bad":true})
                        } else {
                            json!(1.0)
                        },
                        json!("100"),
                        json!("20240101"),
                        json!("600519.SH"),
                    ]
                })
                .collect(),
            ..window.clone()
        };
        assert!(writer.replace_stock(&[changed.clone()]).is_err());
        assert!(writer.schemas.is_empty());
        assert_eq!(
            writer
                .conn
                .query_row("SELECT count(*) FROM daily_basic", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            8193
        );
        assert_eq!(writer.conn.query_row("SELECT data_type FROM duckdb_columns() WHERE table_name = 'daily_basic' AND column_name = 'total_share'", [], |r| r.get::<_, String>(0)).unwrap(), "DOUBLE");
        changed.items = vec![vec![
            json!(null),
            json!("100"),
            json!("20240101"),
            json!("600519.SH"),
        ]];
        writer.replace_stock(&[changed]).unwrap();
        assert_eq!(
            writer
                .conn
                .query_row(
                    "SELECT total_share, new_value FROM daily_basic",
                    [],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<f64>>(1)?))
                )
                .unwrap(),
            ("100".into(), None)
        );
        // Reuse the committed cache for a subsequent numeric response; VARCHAR persists.
        writer
            .replace_stock(&[CompanyDownloadWindow {
                items: vec![vec![json!("600519.SH"), json!("20240101"), json!(200.0)]],
                ..window
            }])
            .unwrap();
        assert_eq!(
            writer
                .conn
                .query_row("SELECT total_share FROM daily_basic", [], |r| r
                    .get::<_, String>(0))
                .unwrap(),
            "200.0"
        );
        drop(writer);
        drop(conn);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
