use std::collections::{HashMap, HashSet};
use std::path::Path;

use duckdb::Connection;
use serde::Serialize;

use crate::data::{
    concept_performance_db_path, cyq_chen_db_path, cyq_db_path, dragon_tiger_db_path,
    load_stock_list, result_db_path, source_db_path, stock_list_path, ths_concepts_path,
    trade_calendar_path,
};

use super::import::{resolve_source_root, validate_target_relative_path};

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

    let mut panel_fields = Vec::new();
    let mut panel_signature = String::new();
    let mut filter_trade_column = None;
    let mut filter_ts_code_column = None;
    let dataset_label;
    let target_path;
    let relation_sql;
    let all_columns;
    let selected_columns;
    let order_by_sql;
    let conn;

    match normalized_dataset_id {
        "stock-data-base" | "stock-data-indicators" => {
            let db_path = source_db_path(source_path_str);
            if !db_path.exists() {
                return Err(format!("原始行情库不存在: {}", db_path.display()));
            }
            let db_path_str = db_path
                .to_str()
                .ok_or_else(|| "stock_data.db 路径不是有效 UTF-8".to_string())?;
            conn = crate::data::open_db_readonly(db_path_str)
                .map_err(|error| format!("打开 stock_data.db 失败: {error}"))?;
            let relation = quote_ident("stock_data");
            let mut columns = load_relation_columns(&conn, &relation)?;
            let definitions = crate::data::panel::definitions(source_path_str)?;
            if crate::data::panel::has_values(&conn)? {
                panel_signature = crate::data::panel::config_signature(&definitions)?;
                panel_fields = definitions
                    .into_iter()
                    .filter(|field| field.scope != crate::data::IndicatorScope::Stock)
                    .map(|field| (field.name, field.scope))
                    .collect();
                let names = panel_fields
                    .iter()
                    .map(|(name, _)| name.to_ascii_uppercase())
                    .collect::<HashSet<_>>();
                columns.retain(|name| !names.contains(&name.to_ascii_uppercase()));
                columns.extend(panel_fields.iter().map(|(name, _)| name.clone()));
            }
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
        "dragon-tiger-top-list" | "dragon-tiger-top-inst" => {
            let db_path = dragon_tiger_db_path(source_path_str);
            if !db_path.exists() {
                return Err(format!("龙虎榜库不存在: {}", db_path.display()));
            }
            let db_path_str = db_path
                .to_str()
                .ok_or_else(|| "dragon_tiger.db 路径不是有效 UTF-8".to_string())?;
            conn = crate::data::open_db_readonly(db_path_str)
                .map_err(|error| format!("打开 dragon_tiger.db 失败: {error}"))?;
            let (table_name, label, order_by) = if normalized_dataset_id.ends_with("top-list") {
                (
                    "top_list",
                    "龙虎榜每日明细",
                    "trade_date DESC, ts_code ASC, net_amount DESC NULLS LAST",
                )
            } else {
                (
                    "top_inst",
                    "龙虎榜席位明细",
                    "trade_date DESC, ts_code ASC, net_buy DESC NULLS LAST, exalter ASC",
                )
            };
            let relation = quote_ident(table_name);
            let columns = load_relation_columns(&conn, &relation)?;
            filter_trade_column = Some("trade_date");
            filter_ts_code_column = Some("ts_code");
            dataset_label = label;
            target_path = db_path.display().to_string();
            relation_sql = relation;
            selected_columns = columns.clone();
            all_columns = columns;
            order_by_sql = order_by;
        }
        "strategy-trigger-similarity-rank"
        | "strategy-trigger-similarity-meta"
        | "strategy-trigger-similarity-summary" => {
            let db_path = crate::data::similarity_db_path(source_path_str);
            if !db_path.exists() {
                return Err(format!("结果库不存在: {}", db_path.display()));
            }
            let db_path_str = db_path
                .to_str()
                .ok_or_else(|| "strategy_similarity.db 路径不是有效 UTF-8".to_string())?;
            conn = crate::data::open_db_readonly(db_path_str)
                .map_err(|error| format!("打开 strategy_similarity.db 失败: {error}"))?;
            let (table_name, label, order_by) = if normalized_dataset_id.ends_with("meta") {
                (
                    "strategy_trigger_similarity_rank_meta",
                    "走势相似排名元数据",
                    "trade_date DESC, config_key ASC, generated_at_epoch_seconds DESC",
                )
            } else if normalized_dataset_id.ends_with("summary") {
                (
                    "strategy_trigger_similarity_summary",
                    "走势相似排名汇总",
                    "trade_date DESC, rank NULLS LAST, ts_code ASC",
                )
            } else {
                (
                    "strategy_trigger_similarity_rank",
                    "走势相似排名明细",
                    "trade_date DESC, config_key ASC, rank NULLS LAST, ts_code ASC",
                )
            };
            let relation = quote_ident(table_name);
            let columns = load_relation_columns(&conn, &relation)?;
            filter_trade_column = Some("trade_date");
            filter_ts_code_column = Some("ts_code");
            dataset_label = label;
            target_path = db_path.display().to_string();
            relation_sql = relation;
            selected_columns = columns.clone();
            all_columns = columns;
            order_by_sql = order_by;
        }
        "score-summary" => {
            let db_path = result_db_path(source_path_str);
            if !db_path.exists() {
                return Err(format!("结果库不存在: {}", db_path.display()));
            }
            let db_path_str = db_path
                .to_str()
                .ok_or_else(|| "scoring_result.db 路径不是有效 UTF-8".to_string())?;
            conn = crate::data::open_db_readonly(db_path_str)
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
            conn = crate::data::open_db_readonly(db_path_str)
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
            conn = crate::data::open_db_readonly(db_path_str)
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
            conn = crate::data::open_db_readonly(db_path_str)
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
        "cyq-snapshot" => {
            let db_path = cyq_db_path(source_path_str);
            if !db_path.exists() {
                return Err(format!("筹码库不存在: {}", db_path.display()));
            }
            let db_path_str = db_path
                .to_str()
                .ok_or_else(|| "cyq.db 路径不是有效 UTF-8".to_string())?;
            conn = crate::data::open_db_readonly(db_path_str)
                .map_err(|error| format!("打开 cyq.db 失败: {error}"))?;
            let relation = quote_ident("cyq_snapshot");
            let columns = load_relation_columns(&conn, &relation)?;
            filter_trade_column = Some("trade_date");
            filter_ts_code_column = Some("ts_code");
            dataset_label = "筹码库摘要";
            target_path = db_path.display().to_string();
            relation_sql = relation;
            selected_columns = columns.clone();
            all_columns = columns;
            order_by_sql = "trade_date DESC, ts_code ASC";
        }
        "cyq-bins" => {
            let db_path = cyq_db_path(source_path_str);
            if !db_path.exists() {
                return Err(format!("筹码库不存在: {}", db_path.display()));
            }
            let db_path_str = db_path
                .to_str()
                .ok_or_else(|| "cyq.db 路径不是有效 UTF-8".to_string())?;
            conn = crate::data::open_db_readonly(db_path_str)
                .map_err(|error| format!("打开 cyq.db 失败: {error}"))?;
            let relation = quote_ident("cyq_bin");
            let columns = load_relation_columns(&conn, &relation)?;
            filter_trade_column = Some("trade_date");
            filter_ts_code_column = Some("ts_code");
            dataset_label = "筹码库分桶";
            target_path = db_path.display().to_string();
            relation_sql = relation;
            selected_columns = columns.clone();
            all_columns = columns;
            order_by_sql = "trade_date DESC, ts_code ASC, bin_index ASC";
        }
        "cyq-chen-snapshot" => {
            let db_path = cyq_chen_db_path(source_path_str);
            if !db_path.exists() {
                return Err(format!("新筹码库不存在: {}", db_path.display()));
            }
            let db_path_str = db_path
                .to_str()
                .ok_or_else(|| "cyq_chen.db 路径不是有效 UTF-8".to_string())?;
            conn = crate::data::open_db_readonly(db_path_str)
                .map_err(|error| format!("打开 cyq_chen.db 失败: {error}"))?;
            let relation = quote_ident("cyq_chen_snapshot");
            let columns = load_relation_columns(&conn, &relation)?;
            filter_trade_column = Some("trade_date");
            filter_ts_code_column = Some("ts_code");
            dataset_label = "新筹码库摘要";
            target_path = db_path.display().to_string();
            relation_sql = relation;
            selected_columns = columns.clone();
            all_columns = columns;
            order_by_sql = "trade_date DESC, ts_code ASC";
        }
        "cyq-chen-bins" => {
            let db_path = cyq_chen_db_path(source_path_str);
            if !db_path.exists() {
                return Err(format!("新筹码库不存在: {}", db_path.display()));
            }
            let db_path_str = db_path
                .to_str()
                .ok_or_else(|| "cyq_chen.db 路径不是有效 UTF-8".to_string())?;
            conn = crate::data::open_db_readonly(db_path_str)
                .map_err(|error| format!("打开 cyq_chen.db 失败: {error}"))?;
            let relation = quote_ident("cyq_chen_bin");
            let columns = load_relation_columns(&conn, &relation)?;
            filter_trade_column = Some("trade_date");
            filter_ts_code_column = Some("ts_code");
            dataset_label = "新筹码库分桶";
            target_path = db_path.display().to_string();
            relation_sql = relation;
            selected_columns = columns.clone();
            all_columns = columns;
            order_by_sql = "trade_date DESC, ts_code ASC, bin_index ASC";
        }
        "stock-list-csv" => {
            let csv_path = stock_list_path(source_path_str);
            if !csv_path.exists() {
                return Err(format!("stock_list.csv 不存在: {}", csv_path.display()));
            }
            conn = Connection::open_in_memory()
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
            conn = Connection::open_in_memory()
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
            conn = Connection::open_in_memory()
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

    let transaction = conn
        .unchecked_transaction()
        .map_err(|error| format!("开始预览只读事务失败: {error}"))?;
    let row_count_i64 = conn
        .query_row(&format!("SELECT COUNT(*) FROM {relation_sql}"), [], |row| {
            row.get::<_, i64>(0)
        })
        .map_err(|error| format!("读取数据集总行数失败: {error}"))?;
    let matched_rows_i64 = if where_sql.is_empty() {
        row_count_i64
    } else {
        conn.query_row(
            &format!("SELECT COUNT(*) FROM {relation_sql}{where_sql}"),
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|error| format!("读取数据集筛选行数失败: {error}"))?
    };

    // Only physical columns participate in the preview scan. Panel values are
    // fetched for the returned keys below, never joined to the full history.
    let physical_column_count = selected_columns.len() - panel_fields.len();
    let select_sql = selected_columns[..physical_column_count]
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
    let output_order_sql = order_by_sql
        .split(", ")
        .map(|column| format!("preview.{column}"))
        .collect::<Vec<_>>()
        .join(", ");
    // An IN / semi-join plan can scan wide history again. Exact contiguous
    // rowid ranges let each scan skip unrelated storage, without one query per row.
    let preview_sql = if normalized_dataset_id.starts_with("stock-data-") {
        let mut key_stmt = conn
            .prepare(&format!(
                "SELECT rowid FROM {relation_sql}{where_sql} ORDER BY {order_by_sql} LIMIT {limit}"
            ))
            .map_err(|error| format!("准备预览行号查询失败: {error}"))?;
        let mut ids = key_stmt
            .query_map([], |row| row.get::<_, i64>(0))
            .map_err(|error| format!("执行预览行号查询失败: {error}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("读取预览行号失败: {error}"))?;
        ids.sort_unstable();
        let mut ranges = Vec::new();
        let mut remaining = ids.as_slice();
        while let Some((&start, rest)) = remaining.split_first() {
            let mut end = start;
            remaining = rest;
            while remaining.first() == Some(&(end + 1)) {
                end += 1;
                remaining = &remaining[1..];
            }
            ranges.push(format!(
                "SELECT * FROM {relation_sql} WHERE rowid BETWEEN {start} AND {end}"
            ));
        }
        let relation = if ranges.is_empty() {
            format!("SELECT * FROM {relation_sql} WHERE false")
        } else {
            ranges.join(" UNION ALL ")
        };
        format!("SELECT {select_sql} FROM ({relation}) preview ORDER BY {output_order_sql}")
    } else {
        format!(
            "SELECT {select_sql} FROM (SELECT * FROM {relation_sql}{where_sql} ORDER BY {order_by_sql} LIMIT {limit}) preview ORDER BY {output_order_sql}"
        )
    };
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
        for index in 0..physical_column_count {
            let value: Option<String> = row
                .get(index)
                .map_err(|error| format!("读取预览字段失败: {error}"))?;
            values.push(value.unwrap_or_default());
        }
        values.resize(selected_columns.len(), String::new());
        preview_rows.push(values);
    }

    if !panel_fields.is_empty() && !preview_rows.is_empty() {
        let code_index = selected_columns
            .iter()
            .position(|name| name.eq_ignore_ascii_case("ts_code"))
            .ok_or("原始行情库缺少 ts_code 列")?;
        let date_index = selected_columns
            .iter()
            .position(|name| name.eq_ignore_ascii_case("trade_date"))
            .ok_or("原始行情库缺少 trade_date 列")?;
        let mut keys = HashSet::new();
        for row in &preview_rows {
            for (_, scope) in &panel_fields {
                let code = if *scope == crate::data::IndicatorScope::Market {
                    ""
                } else {
                    &row[code_index]
                };
                keys.insert(format!(
                    "({}, {})",
                    quote_sql_string(code),
                    quote_sql_string(&row[date_index])
                ));
            }
        }
        let names = panel_fields
            .iter()
            .map(|(name, _)| quote_sql_string(name))
            .collect::<Vec<_>>()
            .join(",");
        let keys = keys.into_iter().collect::<Vec<_>>().join(",");
        let mut panel_stmt = conn
            .prepare(&format!(
                "SELECT v.name, v.ts_code, v.trade_date, COALESCE(CAST(v.value AS VARCHAR), '') \
             FROM indicator_panel_values v \
             JOIN (VALUES {keys}) AS k(ts_code, trade_date) \
               ON v.ts_code = k.ts_code AND v.trade_date = k.trade_date \
             JOIN indicator_panel_state m ON m.id = 1 AND m.version = 1 AND m.signature = ? \
             WHERE v.name IN ({names}) AND (m.dirty_from IS NULL OR v.trade_date < m.dirty_from)"
            ))
            .map_err(|error| format!("准备预览指标查询失败: {error}"))?;
        let mut panel_rows = panel_stmt
            .query([&panel_signature])
            .map_err(|error| format!("执行预览指标查询失败: {error}"))?;
        let mut values = HashMap::new();
        while let Some(row) = panel_rows
            .next()
            .map_err(|error| format!("读取预览指标失败: {error}"))?
        {
            let key = (
                row.get::<_, String>(0),
                row.get::<_, String>(1),
                row.get::<_, String>(2),
            );
            let key = match key {
                (Ok(name), Ok(code), Ok(date)) => (name, code, date),
                _ => return Err("读取预览指标标识失败".to_string()),
            };
            let value = row
                .get::<_, String>(3)
                .map_err(|error| format!("读取预览指标值失败: {error}"))?;
            values.insert(key, value);
        }
        for row in &mut preview_rows {
            for (index, (name, scope)) in panel_fields.iter().enumerate() {
                let code = if *scope == crate::data::IndicatorScope::Market {
                    ""
                } else {
                    &row[code_index]
                };
                if let Some(value) =
                    values.get(&(name.clone(), code.to_string(), row[date_index].clone()))
                {
                    row[physical_column_count + index] = value.clone();
                }
            }
        }
    }

    transaction
        .commit()
        .map_err(|error| format!("结束预览只读事务失败: {error}"))?;

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
    let conn = crate::data::open_db_readonly(db_path_str)
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

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use crate::data::dragon_tiger_data::open_dragon_tiger_db;

    use super::preview_managed_source_dataset;

    #[test]
    fn previews_stock_rows_before_loading_panel_values() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("lianghua-stock-preview-{nanos}"));
        let source = root.join("source");
        fs::create_dir_all(&source).unwrap();
        let conn = duckdb::Connection::open(source.join("stock_data.db")).unwrap();
        conn.execute_batch(
            "CREATE TABLE stock_data(ts_code VARCHAR, trade_date VARCHAR, adj_type VARCHAR, close DOUBLE, MY_RANK DOUBLE);
             INSERT INTO stock_data VALUES
                 ('000001.SZ', '20260809', 'qfq', 10, 999),
                 ('000001.SZ', '20260808', 'qfq', 9, 999),
                 ('000001.SZ', '20260808', 'raw', 8, 999),
                 ('000002.SZ', '20260808', 'qfq', NULL, 999);"
        ).unwrap();
        drop(conn);
        let plain = preview_managed_source_dataset(
            &root,
            "source".into(),
            "stock-data-base".into(),
            None,
            None,
            2,
        )
        .unwrap();
        assert_eq!(
            (plain.row_count, plain.matched_rows, plain.rows.len()),
            (4, 4, 2)
        );
        assert_eq!(plain.rows[0][1], "20260809");

        fs::write(
            source.join("ind.toml"),
            r#"version = 1
[[ind]]
name="MY_MARKET"
scope="market"
expr="CS_MEAN(C/C)"
prec=2
[[ind]]
name="MY_RANK"
scope="cross_section"
expr="CS_RANK(C/C)"
prec=2
"#,
        )
        .unwrap();
        let definitions = crate::data::panel::definitions(source.to_str().unwrap()).unwrap();
        let signature = crate::data::panel::config_signature(&definitions).unwrap();
        let conn = duckdb::Connection::open(source.join("stock_data.db")).unwrap();
        conn.execute_batch(
            "CREATE TABLE indicator_panel_state(id INTEGER, version INTEGER, signature VARCHAR, dirty_from VARCHAR);
             CREATE TABLE indicator_panel_values(name VARCHAR, ts_code VARCHAR, trade_date VARCHAR, value DOUBLE,
                 PRIMARY KEY(name, ts_code, trade_date));
             INSERT INTO indicator_panel_values VALUES
                 ('MY_MARKET', '', '20260808', 12.5),
                 ('MY_MARKET', '', '20260809', 50),
                 ('MY_RANK', '000001.SZ', '20260808', 0.5),
                 ('MY_RANK', '000002.SZ', '20260808', NULL);"
        ).unwrap();
        conn.execute(
            "INSERT INTO indicator_panel_state VALUES (1, 1, ?, '20260809')",
            [&signature],
        )
        .unwrap();
        drop(conn);

        for (date, code, matched) in [
            (None, None, 4),
            (Some("20260808"), Some("000001.SZ"), 2),
            (Some("20260101"), None, 0),
        ] {
            let preview = preview_managed_source_dataset(
                &root,
                "source".into(),
                "stock-data-base".into(),
                date.map(str::to_string),
                code.map(str::to_string),
                100,
            )
            .unwrap();
            assert_eq!((preview.row_count, preview.matched_rows), (4, matched));
            assert_eq!(preview.rows.len() as u64, matched);
            let market = preview
                .columns
                .iter()
                .position(|name| name == "MY_MARKET")
                .unwrap();
            let rank = preview
                .columns
                .iter()
                .position(|name| name == "MY_RANK")
                .unwrap();
            assert_eq!(
                preview
                    .columns
                    .iter()
                    .filter(|name| *name == "MY_RANK")
                    .count(),
                1
            );
            for row in preview.rows {
                if row[1] == "20260809" {
                    assert_eq!(row[market], "");
                    assert_eq!(row[rank], "");
                } else {
                    assert_eq!(row[market], "12.5");
                    assert_eq!(row[rank], if row[0] == "000001.SZ" { "0.5" } else { "" });
                }
            }
        }
        let conn = duckdb::Connection::open(source.join("stock_data.db")).unwrap();
        conn.execute("UPDATE indicator_panel_state SET signature = 'stale'", [])
            .unwrap();
        drop(conn);
        let stale = preview_managed_source_dataset(
            &root,
            "source".into(),
            "stock-data-indicators".into(),
            None,
            None,
            100,
        )
        .unwrap();
        assert!(
            stale
                .rows
                .iter()
                .all(|row| row[row.len() - 2..].iter().all(String::is_empty))
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn previews_dragon_tiger_business_tables() {
        let app_data_root = (|| -> PathBuf {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time")
                .as_nanos();
            std::env::temp_dir().join(format!("lianghua-data-viewer-{nanos}"))
        })();
        let source_path = app_data_root.join("source");
        let source_path_str = source_path.to_str().expect("utf8 source path");
        let conn = open_dragon_tiger_db(source_path_str).expect("open dragon tiger db");
        conn.execute(
            "INSERT INTO top_list (trade_date, ts_code, name, reason) VALUES (?, ?, ?, ?)",
            ["20260808", "000001.SZ", "平安银行", "测试原因"],
        )
        .expect("insert top list");
        conn.execute(
            "INSERT INTO top_inst (trade_date, ts_code, exalter, side, reason) VALUES (?, ?, ?, ?, ?)",
            ["20260808", "000001.SZ", "测试席位", "0", "测试原因"],
        )
        .expect("insert top inst");
        drop(conn);

        let top_list = preview_managed_source_dataset(
            &app_data_root,
            "source".to_string(),
            "dragon-tiger-top-list".to_string(),
            Some("20260808".to_string()),
            Some("000001.SZ".to_string()),
            100,
        )
        .expect("preview top list");
        assert_eq!(top_list.dataset_label, "龙虎榜每日明细");
        assert_eq!(top_list.row_count, 1);
        assert_eq!(top_list.matched_rows, 1);
        assert_eq!(top_list.rows.len(), 1);

        let top_inst = preview_managed_source_dataset(
            &app_data_root,
            "source".to_string(),
            "dragon-tiger-top-inst".to_string(),
            Some("20260808".to_string()),
            Some("000001.SZ".to_string()),
            100,
        )
        .expect("preview top inst");
        assert_eq!(top_inst.dataset_label, "龙虎榜席位明细");
        assert_eq!(top_inst.row_count, 1);
        assert_eq!(top_inst.matched_rows, 1);
        assert_eq!(top_inst.rows.len(), 1);

        fs::remove_dir_all(app_data_root).ok();
    }
}

