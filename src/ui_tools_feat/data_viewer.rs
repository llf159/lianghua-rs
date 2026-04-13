use std::path::Path;

use duckdb::Connection;
use serde::Serialize;

use crate::data::{
    concept_performance_db_path, load_stock_list, result_db_path, source_db_path, stock_list_path,
    ths_concepts_path, trade_calendar_path,
};

use super::data_import::{resolve_source_root, validate_target_relative_path};

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedSourceDbPreviewRow {
    pub ts_code: String,
    pub trade_date: String,
    pub adj_type: String,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: Option<f64>,
    pub pre_close: Option<f64>,
    pub pct_chg: Option<f64>,
    pub vol: Option<f64>,
    pub amount: Option<f64>,
    pub tor: Option<f64>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedSourceDbPreviewResult {
    pub source_path: String,
    pub db_path: String,
    pub row_count: u64,
    pub matched_rows: u64,
    pub min_trade_date: Option<String>,
    pub max_trade_date: Option<String>,
    pub rows: Vec<ManagedSourceDbPreviewRow>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedSourceDatasetPreviewResult {
    pub source_path: String,
    pub target_path: String,
    pub dataset_id: String,
    pub dataset_label: String,
    pub row_count: u64,
    pub matched_rows: u64,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

#[derive(Clone, Serialize)]
pub struct StockLookupRow {
    pub ts_code: String,
    pub name: String,
    pub cnspell: Option<String>,
}

fn normalize_preview_trade_date(value: Option<String>) -> Result<Option<String>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.len() != 8 || !trimmed.chars().all(|ch| ch.is_ascii_digit()) {
        return Err("交易日格式无效，应为 YYYYMMDD".to_string());
    }
    Ok(Some(trimmed.to_string()))
}

fn normalize_preview_ts_code(value: Option<String>) -> Result<Option<String>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if !trimmed
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '.' || ch == '_' || ch == '-')
    {
        return Err("股票代码格式无效".to_string());
    }
    Ok(Some(trimmed.to_ascii_uppercase()))
}

fn quote_sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn load_relation_columns(conn: &Connection, relation_sql: &str) -> Result<Vec<String>, String> {
    let describe_sql = format!("DESCRIBE SELECT * FROM {relation_sql}");
    let mut stmt = conn
        .prepare(&describe_sql)
        .map_err(|error| format!("读取数据集列结构失败: {error}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|error| format!("查询数据集列结构失败: {error}"))?;
    let mut columns = Vec::with_capacity(32);

    while let Some(row) = rows
        .next()
        .map_err(|error| format!("读取数据集列失败: {error}"))?
    {
        let column_name: String = row
            .get(0)
            .map_err(|error| format!("读取数据集列名失败: {error}"))?;
        columns.push(column_name);
    }

    Ok(columns)
}

fn build_csv_relation_sql(path: &Path) -> Result<String, String> {
    let path_str = path
        .to_str()
        .ok_or_else(|| "CSV 路径不是有效 UTF-8".to_string())?;
    Ok(format!(
        "read_csv_auto({}, header = true, all_varchar = true)",
        quote_sql_string(path_str)
    ))
}

pub fn list_stock_lookup_rows(source_path: &str) -> Result<Vec<StockLookupRow>, String> {
    let trimmed = source_path.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    let rows = load_stock_list(trimmed)?;
    let mut out = Vec::with_capacity(rows.len());

    for cols in rows {
        let Some(ts_code) = cols.first() else {
            continue;
        };
        let Some(name) = cols.get(2) else {
            continue;
        };
        let cnspell = cols
            .get(13)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty());

        let ts_code = ts_code.trim();
        let name = name.trim();
        if ts_code.is_empty() || name.is_empty() {
            continue;
        }

        out.push(StockLookupRow {
            ts_code: ts_code.to_string(),
            name: name.to_string(),
            cnspell: cnspell.map(|value| value.to_string()),
        });
    }

    Ok(out)
}

pub fn preview_managed_source_dataset(
    app_data_root: &Path,
    source_dir: String,
    dataset_id: String,
    trade_date: Option<String>,
    ts_code: Option<String>,
    limit: usize,
) -> Result<ManagedSourceDatasetPreviewResult, String> {
    validate_target_relative_path(&source_dir)?;
    let source_path = resolve_source_root(app_data_root, &source_dir)?;
    let source_path_str = source_path
        .to_str()
        .ok_or_else(|| "当前应用数据路径不是有效 UTF-8".to_string())?;

    let normalized_trade_date = normalize_preview_trade_date(trade_date)?;
    let normalized_ts_code = normalize_preview_ts_code(ts_code)?;
    let normalized_dataset_id = dataset_id.trim();
    if normalized_dataset_id.is_empty() {
        return Err("dataset_id 不能为空".to_string());
    }

    let mut filter_trade_column = None;
    let mut filter_ts_code_column = None;
    let dataset_label;
    let target_path;
    let relation_sql;
    let all_columns;
    let selected_columns;
    let order_by_sql;

    match normalized_dataset_id {
        "stock-data-base" | "stock-data-indicators" => {
            let db_path = source_db_path(source_path_str);
            if !db_path.exists() {
                return Err(format!("原始行情库不存在: {}", db_path.display()));
            }
            let db_path_str = db_path
                .to_str()
                .ok_or_else(|| "stock_data.db 路径不是有效 UTF-8".to_string())?;
            let conn = Connection::open(db_path_str)
                .map_err(|error| format!("打开 stock_data.db 失败: {error}"))?;
            let relation = quote_ident("stock_data");
            let columns = load_relation_columns(&conn, &relation)?;
            let base_columns = [
                "ts_code",
                "trade_date",
                "adj_type",
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "change",
                "pct_chg",
                "vol",
                "amount",
                "tor",
            ];
            let mut selected = base_columns
                .iter()
                .filter(|column| columns.iter().any(|item| item.eq_ignore_ascii_case(column)))
                .map(|column| (*column).to_string())
                .collect::<Vec<_>>();
            for column in &columns {
                if base_columns
                    .iter()
                    .any(|item| item.eq_ignore_ascii_case(column))
                {
                    continue;
                }
                selected.push(column.clone());
            }
            filter_trade_column = Some("trade_date");
            filter_ts_code_column = Some("ts_code");
            dataset_label = "原始行情库";
            target_path = db_path.display().to_string();
            relation_sql = relation;
            all_columns = columns;
            selected_columns = selected;
            order_by_sql = "trade_date DESC, ts_code ASC";
        }
        "score-summary" => {
            let db_path = result_db_path(source_path_str);
            if !db_path.exists() {
                return Err(format!("结果库不存在: {}", db_path.display()));
            }
            let db_path_str = db_path
                .to_str()
                .ok_or_else(|| "scoring_result.db 路径不是有效 UTF-8".to_string())?;
            let conn = Connection::open(db_path_str)
                .map_err(|error| format!("打开 scoring_result.db 失败: {error}"))?;
            let relation = quote_ident("score_summary");
            let columns = load_relation_columns(&conn, &relation)?;
            filter_trade_column = Some("trade_date");
            filter_ts_code_column = Some("ts_code");
            dataset_label = "结果库 score_summary";
            target_path = db_path.display().to_string();
            relation_sql = relation;
            selected_columns = columns.clone();
            all_columns = columns;
            order_by_sql = "trade_date DESC, ts_code ASC";
        }
        "rule-details" => {
            let db_path = result_db_path(source_path_str);
            if !db_path.exists() {
                return Err(format!("结果库不存在: {}", db_path.display()));
            }
            let db_path_str = db_path
                .to_str()
                .ok_or_else(|| "scoring_result.db 路径不是有效 UTF-8".to_string())?;
            let conn = Connection::open(db_path_str)
                .map_err(|error| format!("打开 scoring_result.db 失败: {error}"))?;
            let relation = quote_ident("rule_details");
            let columns = load_relation_columns(&conn, &relation)?;
            filter_trade_column = Some("trade_date");
            filter_ts_code_column = Some("ts_code");
            dataset_label = "结果库 rule_details";
            target_path = db_path.display().to_string();
            relation_sql = relation;
            selected_columns = columns.clone();
            all_columns = columns;
            order_by_sql = "trade_date DESC, ts_code ASC, rule_name ASC";
        }
        "scene-details" => {
            let db_path = result_db_path(source_path_str);
            if !db_path.exists() {
                return Err(format!("结果库不存在: {}", db_path.display()));
            }
            let db_path_str = db_path
                .to_str()
                .ok_or_else(|| "scoring_result.db 路径不是有效 UTF-8".to_string())?;
            let conn = Connection::open(db_path_str)
                .map_err(|error| format!("打开 scoring_result.db 失败: {error}"))?;
            let relation = quote_ident("scene_details");
            let columns = load_relation_columns(&conn, &relation)?;
            filter_trade_column = Some("trade_date");
            filter_ts_code_column = Some("ts_code");
            dataset_label = "结果库 scene_details";
            target_path = db_path.display().to_string();
            relation_sql = relation;
            selected_columns = columns.clone();
            all_columns = columns;
            order_by_sql = "trade_date DESC, ts_code ASC, scene_name ASC";
        }
        "concept-performance" => {
            let db_path = concept_performance_db_path(source_path_str);
            if !db_path.exists() {
                return Err(format!("概念表现库不存在: {}", db_path.display()));
            }
            let db_path_str = db_path
                .to_str()
                .ok_or_else(|| "concept_performance.db 路径不是有效 UTF-8".to_string())?;
            let conn = Connection::open(db_path_str)
                .map_err(|error| format!("打开 concept_performance.db 失败: {error}"))?;
            let relation = quote_ident("concept_performance");
            let columns = load_relation_columns(&conn, &relation)?;
            filter_trade_column = Some("trade_date");
            dataset_label = "概念表现库";
            target_path = db_path.display().to_string();
            relation_sql = relation;
            selected_columns = columns.clone();
            all_columns = columns;
            order_by_sql = "trade_date DESC, performance_type ASC, concept ASC";
        }
        "stock-list-csv" => {
            let csv_path = stock_list_path(source_path_str);
            if !csv_path.exists() {
                return Err(format!("stock_list.csv 不存在: {}", csv_path.display()));
            }
            let conn = Connection::open_in_memory()
                .map_err(|error| format!("打开内存查询连接失败: {error}"))?;
            let relation = build_csv_relation_sql(&csv_path)?;
            let columns = load_relation_columns(&conn, &relation)?;
            filter_trade_column = Some("trade_date");
            filter_ts_code_column = Some("ts_code");
            dataset_label = "股票列表 CSV";
            target_path = csv_path.display().to_string();
            relation_sql = relation;
            selected_columns = columns.clone();
            all_columns = columns;
            order_by_sql = "trade_date DESC, ts_code ASC";
        }
        "trade-calendar-csv" => {
            let csv_path = trade_calendar_path(source_path_str);
            if !csv_path.exists() {
                return Err(format!("trade_calendar.csv 不存在: {}", csv_path.display()));
            }
            let conn = Connection::open_in_memory()
                .map_err(|error| format!("打开内存查询连接失败: {error}"))?;
            let relation = build_csv_relation_sql(&csv_path)?;
            let columns = load_relation_columns(&conn, &relation)?;
            filter_trade_column = Some("cal_date");
            dataset_label = "交易日历 CSV";
            target_path = csv_path.display().to_string();
            relation_sql = relation;
            selected_columns = columns.clone();
            all_columns = columns;
            order_by_sql = "cal_date DESC";
        }
        "stock-concepts-csv" => {
            let csv_path = ths_concepts_path(source_path_str);
            if !csv_path.exists() {
                return Err(format!("stock_concepts.csv 不存在: {}", csv_path.display()));
            }
            let conn = Connection::open_in_memory()
                .map_err(|error| format!("打开内存查询连接失败: {error}"))?;
            let relation = build_csv_relation_sql(&csv_path)?;
            let columns = load_relation_columns(&conn, &relation)?;
            filter_ts_code_column = Some("ts_code");
            dataset_label = "同花顺概念 CSV";
            target_path = csv_path.display().to_string();
            relation_sql = relation;
            selected_columns = columns.clone();
            all_columns = columns;
            order_by_sql = "ts_code ASC";
        }
        _ => return Err(format!("不支持的数据集: {normalized_dataset_id}")),
    }

    if selected_columns.is_empty() {
        return Err(format!("数据集 {normalized_dataset_id} 没有可展示列"));
    }

    let uses_memory_conn = normalized_dataset_id.ends_with("-csv");
    let conn = if uses_memory_conn {
        Connection::open_in_memory().map_err(|error| format!("打开内存查询连接失败: {error}"))?
    } else if normalized_dataset_id == "concept-performance" {
        let db_path = concept_performance_db_path(source_path_str);
        let db_path_str = db_path
            .to_str()
            .ok_or_else(|| "concept_performance.db 路径不是有效 UTF-8".to_string())?;
        Connection::open(db_path_str)
            .map_err(|error| format!("打开 concept_performance.db 失败: {error}"))?
    } else if normalized_dataset_id.starts_with("stock-data-") {
        let db_path = source_db_path(source_path_str);
        let db_path_str = db_path
            .to_str()
            .ok_or_else(|| "stock_data.db 路径不是有效 UTF-8".to_string())?;
        Connection::open(db_path_str)
            .map_err(|error| format!("打开 stock_data.db 失败: {error}"))?
    } else {
        let db_path = result_db_path(source_path_str);
        let db_path_str = db_path
            .to_str()
            .ok_or_else(|| "scoring_result.db 路径不是有效 UTF-8".to_string())?;
        Connection::open(db_path_str)
            .map_err(|error| format!("打开 scoring_result.db 失败: {error}"))?
    };

    let mut where_clauses = Vec::with_capacity(2);
    if let (Some(column), Some(value)) = (filter_trade_column, normalized_trade_date.as_deref()) {
        if all_columns
            .iter()
            .any(|item| item.eq_ignore_ascii_case(column))
        {
            where_clauses.push(format!(
                "{} = {}",
                quote_ident(column),
                quote_sql_string(value)
            ));
        }
    }
    if let (Some(column), Some(value)) = (filter_ts_code_column, normalized_ts_code.as_deref()) {
        if all_columns
            .iter()
            .any(|item| item.eq_ignore_ascii_case(column))
        {
            where_clauses.push(format!(
                "{} = {}",
                quote_ident(column),
                quote_sql_string(value)
            ));
        }
    }
    let where_sql = if where_clauses.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_clauses.join(" AND "))
    };

    let row_count_i64 = conn
        .query_row(&format!("SELECT COUNT(*) FROM {relation_sql}"), [], |row| {
            row.get::<_, i64>(0)
        })
        .map_err(|error| format!("读取数据集总行数失败: {error}"))?;
    let matched_rows_i64 = conn
        .query_row(
            &format!("SELECT COUNT(*) FROM {relation_sql}{where_sql}"),
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|error| format!("读取数据集筛选行数失败: {error}"))?;

    let select_sql = selected_columns
        .iter()
        .map(|column| {
            format!(
                "COALESCE(CAST({} AS VARCHAR), '') AS {}",
                quote_ident(column),
                quote_ident(column)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let preview_sql = format!(
        "SELECT {select_sql} FROM {relation_sql}{where_sql} ORDER BY {order_by_sql} LIMIT {limit}"
    );
    let mut stmt = conn
        .prepare(&preview_sql)
        .map_err(|error| format!("准备数据集预览查询失败: {error}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|error| format!("执行数据集预览查询失败: {error}"))?;
    let mut preview_rows = Vec::with_capacity(limit);

    while let Some(row) = rows
        .next()
        .map_err(|error| format!("读取数据集预览行失败: {error}"))?
    {
        let mut values = Vec::with_capacity(selected_columns.len());
        for index in 0..selected_columns.len() {
            let value: Option<String> = row
                .get(index)
                .map_err(|error| format!("读取预览字段失败: {error}"))?;
            values.push(value.unwrap_or_default());
        }
        preview_rows.push(values);
    }

    Ok(ManagedSourceDatasetPreviewResult {
        source_path: source_path.display().to_string(),
        target_path,
        dataset_id: normalized_dataset_id.to_string(),
        dataset_label: dataset_label.to_string(),
        row_count: row_count_i64.max(0) as u64,
        matched_rows: matched_rows_i64.max(0) as u64,
        columns: selected_columns,
        rows: preview_rows,
    })
}

pub fn preview_managed_source_stock_data(
    app_data_root: &Path,
    source_dir: String,
    trade_date: Option<String>,
    ts_code: Option<String>,
    limit: usize,
) -> Result<ManagedSourceDbPreviewResult, String> {
    validate_target_relative_path(&source_dir)?;
    let source_path = resolve_source_root(app_data_root, &source_dir)?;
    let source_path_str = source_path
        .to_str()
        .ok_or_else(|| "当前应用数据路径不是有效 UTF-8".to_string())?;
    let db_path = source_db_path(source_path_str);
    if !db_path.exists() {
        return Err(format!("原始行情库不存在: {}", db_path.display()));
    }

    let db_path_str = db_path
        .to_str()
        .ok_or_else(|| "stock_data.db 路径不是有效 UTF-8".to_string())?;
    let conn = Connection::open(db_path_str)
        .map_err(|error| format!("打开 stock_data.db 失败: {error}"))?;
    let table_exists = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'stock_data'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|error| format!("检查 stock_data 表结构失败: {error}"))?;
    if table_exists <= 0 {
        return Err("stock_data 表不存在".to_string());
    }

    let normalized_trade_date = normalize_preview_trade_date(trade_date)?;
    let normalized_ts_code = normalize_preview_ts_code(ts_code)?;
    let mut where_clauses = Vec::with_capacity(2);
    if let Some(value) = normalized_trade_date.as_deref() {
        where_clauses.push(format!("trade_date = {}", quote_sql_string(value)));
    }
    if let Some(value) = normalized_ts_code.as_deref() {
        where_clauses.push(format!("ts_code = {}", quote_sql_string(value)));
    }
    let where_sql = if where_clauses.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_clauses.join(" AND "))
    };

    let row_count_i64 = conn
        .query_row("SELECT COUNT(*) FROM stock_data", [], |row| {
            row.get::<_, i64>(0)
        })
        .map_err(|error| format!("读取 stock_data 总行数失败: {error}"))?;

    let summary_sql =
        format!("SELECT COUNT(*), MIN(trade_date), MAX(trade_date) FROM stock_data{where_sql}");
    let (matched_rows_i64, min_trade_date, max_trade_date) = conn
        .query_row(&summary_sql, [], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })
        .map_err(|error| format!("读取预览范围失败: {error}"))?;

    let preview_sql = format!(
        "SELECT ts_code, trade_date, adj_type, \
            CAST(open AS DOUBLE), CAST(high AS DOUBLE), CAST(low AS DOUBLE), \
            CAST(close AS DOUBLE), CAST(pre_close AS DOUBLE), CAST(pct_chg AS DOUBLE), \
            CAST(vol AS DOUBLE), CAST(amount AS DOUBLE), CAST(tor AS DOUBLE) \
        FROM stock_data{where_sql} ORDER BY trade_date DESC, ts_code ASC LIMIT {limit}"
    );
    let mut stmt = conn
        .prepare(&preview_sql)
        .map_err(|error| format!("准备预览查询失败: {error}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|error| format!("执行预览查询失败: {error}"))?;
    let mut preview_rows = Vec::with_capacity(limit);

    while let Some(row) = rows
        .next()
        .map_err(|error| format!("读取预览行失败: {error}"))?
    {
        preview_rows.push(ManagedSourceDbPreviewRow {
            ts_code: row
                .get(0)
                .map_err(|error| format!("读取 ts_code 失败: {error}"))?,
            trade_date: row
                .get(1)
                .map_err(|error| format!("读取 trade_date 失败: {error}"))?,
            adj_type: row
                .get(2)
                .map_err(|error| format!("读取 adj_type 失败: {error}"))?,
            open: row
                .get(3)
                .map_err(|error| format!("读取 open 失败: {error}"))?,
            high: row
                .get(4)
                .map_err(|error| format!("读取 high 失败: {error}"))?,
            low: row
                .get(5)
                .map_err(|error| format!("读取 low 失败: {error}"))?,
            close: row
                .get(6)
                .map_err(|error| format!("读取 close 失败: {error}"))?,
            pre_close: row
                .get(7)
                .map_err(|error| format!("读取 pre_close 失败: {error}"))?,
            pct_chg: row
                .get(8)
                .map_err(|error| format!("读取 pct_chg 失败: {error}"))?,
            vol: row
                .get(9)
                .map_err(|error| format!("读取 vol 失败: {error}"))?,
            amount: row
                .get(10)
                .map_err(|error| format!("读取 amount 失败: {error}"))?,
            tor: row
                .get(11)
                .map_err(|error| format!("读取 tor 失败: {error}"))?,
        });
    }

    Ok(ManagedSourceDbPreviewResult {
        source_path: source_path.display().to_string(),
        db_path: db_path.display().to_string(),
        row_count: row_count_i64.max(0) as u64,
        matched_rows: matched_rows_i64.max(0) as u64,
        min_trade_date,
        max_trade_date,
        rows: preview_rows,
    })
}
