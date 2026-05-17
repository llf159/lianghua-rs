use std::collections::HashMap;

use duckdb::{Connection, params};
use serde::Serialize;

use crate::{
    data::{result_db_path, source_db_path},
    ui_tools_feat::{
        build_concepts_map, build_name_map, build_total_mv_map, filter_mv, resolve_trade_date,
    },
    utils::utils::board_category,
};

const DEFAULT_ADJ_TYPE: &str = "qfq";
const BOARD_ST: &str = "ST";

#[derive(Debug, Serialize, Clone)]
pub struct OverviewRow {
    pub ts_code: String,
    pub trade_date: Option<String>,
    pub ref_date: Option<String>,
    pub total_score: Option<f64>,
    pub tiebreak_j: Option<f64>,
    pub rank: Option<i64>,
    pub ref_rank: Option<i64>,
    pub post_rank_return_pct: Option<f64>,
    pub name: String,
    pub board: String,
    pub total_mv_yi: Option<f64>,
    pub concept: String,
}

#[derive(Debug, Serialize)]
pub struct OverviewPageData {
    pub rows: Vec<OverviewRow>,
    pub rank_date_options: Option<Vec<String>>,
    pub resolved_rank_date: Option<String>,
    pub resolved_ref_date: Option<String>,
}

fn open_result_conn(source_path: &str) -> Result<Connection, String> {
    let result_db = result_db_path(source_path);
    let result_db_str = result_db
        .to_str()
        .ok_or_else(|| "结果库路径不是有效UTF-8".to_string())?;
    Connection::open(result_db_str).map_err(|e| format!("打开结果库失败: {e}"))
}

fn open_source_conn(source_path: &str) -> Result<Connection, String> {
    let source_db = source_db_path(source_path);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "原始库路径不是有效UTF-8".to_string())?;
    Connection::open(source_db_str).map_err(|e| format!("打开原始库失败: {e}"))
}

fn table_exists(conn: &Connection, table_name: &str) -> Result<bool, String> {
    let count = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| format!("检查排名表结构失败: {e}"))?;
    Ok(count > 0)
}

fn query_rank_trade_date_options_from_conn(conn: &Connection) -> Result<Vec<String>, String> {
    if !table_exists(conn, "score_summary")? {
        return Ok(Vec::new());
    }

    let mut stmt = conn
        .prepare(
            r#"
            SELECT DISTINCT trade_date
            FROM score_summary
            ORDER BY trade_date DESC
            "#,
        )
        .map_err(|e| format!("预编译日期列表失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询日期列表失败: {e}"))?;
    let mut out = Vec::new();

    while let Some(row) = rows.next().map_err(|e| format!("读取日期列表失败: {e}"))? {
        let trade_date: String = row.get(0).map_err(|e| format!("读取日期字段失败: {e}"))?;
        if !trade_date.trim().is_empty() {
            out.push(trade_date);
        }
    }

    Ok(out)
}

fn query_rank_map(
    conn: &Connection,
    trade_date: &str,
) -> Result<HashMap<String, Option<i64>>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT ts_code, rank
            FROM score_summary
            WHERE trade_date = ?
            "#,
        )
        .map_err(|e| format!("预编译参考日排名失败: {e}"))?;
    let mut rows = stmt
        .query(params![trade_date])
        .map_err(|e| format!("查询参考日排名失败: {e}"))?;

    let mut out = HashMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取参考日排名失败: {e}"))?
    {
        let ts_code: String = row
            .get(0)
            .map_err(|e| format!("读取参考日排名代码失败: {e}"))?;
        let rank: Option<i64> = row
            .get(1)
            .map_err(|e| format!("读取参考日排名字段失败: {e}"))?;
        out.insert(ts_code, rank);
    }

    Ok(out)
}

fn query_tiebreak_j_map(
    source_conn: &Connection,
    trade_date: &str,
) -> Result<HashMap<String, Option<f64>>, String> {
    let mut stmt = source_conn
        .prepare(
            r#"
            SELECT ts_code, TRY_CAST(j AS DOUBLE)
            FROM stock_data
            WHERE trade_date = ? AND adj_type = ?
            "#,
        )
        .map_err(|e| format!("预编译同分排序J失败: {e}"))?;
    let mut rows = stmt
        .query(params![trade_date, DEFAULT_ADJ_TYPE])
        .map_err(|e| format!("查询同分排序J失败: {e}"))?;

    let mut out = HashMap::new();
    while let Some(row) = rows.next().map_err(|e| format!("读取同分排序J失败: {e}"))? {
        let ts_code: String = row
            .get(0)
            .map_err(|e| format!("读取同分排序J代码失败: {e}"))?;
        let j_value: Option<f64> = row
            .get(1)
            .map_err(|e| format!("读取同分排序J字段失败: {e}"))?;
        out.insert(ts_code, j_value);
    }

    Ok(out)
}

fn quote_sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn selected_ts_codes_cte(ts_codes: &[String]) -> String {
    let values = ts_codes
        .iter()
        .map(|ts_code| format!("({})", quote_sql_string(ts_code)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("selected_ts_codes(ts_code) AS (VALUES {values})")
}

fn query_post_rank_return_pct_map(
    source_conn: &Connection,
    trade_date: &str,
    ts_codes: &[String],
) -> Result<HashMap<String, Option<f64>>, String> {
    if ts_codes.is_empty() {
        return Ok(HashMap::new());
    }

    let selected_cte = selected_ts_codes_cte(ts_codes);
    let sql = format!(
        r#"
        WITH
            {selected_cte},
            next_rows AS (
                SELECT ts_code, next_open
                FROM (
                    SELECT
                        s.ts_code,
                        TRY_CAST(s.open AS DOUBLE) AS next_open,
                        ROW_NUMBER() OVER (
                            PARTITION BY s.ts_code
                            ORDER BY s.trade_date ASC
                        ) AS row_num
                    FROM stock_data AS s
                    INNER JOIN selected_ts_codes AS selected
                        ON selected.ts_code = s.ts_code
                    WHERE s.adj_type = ? AND s.trade_date > ?
                )
                WHERE row_num = 1
            ),
            latest_rows AS (
                SELECT ts_code, latest_close
                FROM (
                    SELECT
                        s.ts_code,
                        TRY_CAST(s.close AS DOUBLE) AS latest_close,
                        ROW_NUMBER() OVER (
                            PARTITION BY s.ts_code
                            ORDER BY s.trade_date DESC
                        ) AS row_num
                    FROM stock_data AS s
                    INNER JOIN selected_ts_codes AS selected
                        ON selected.ts_code = s.ts_code
                    WHERE s.adj_type = ?
                )
                WHERE row_num = 1
            )
        SELECT
            selected.ts_code,
            next_rows.next_open,
            latest_rows.latest_close
        FROM selected_ts_codes AS selected
        LEFT JOIN next_rows ON next_rows.ts_code = selected.ts_code
        LEFT JOIN latest_rows ON latest_rows.ts_code = selected.ts_code
        "#
    );

    let mut stmt = source_conn
        .prepare(&sql)
        .map_err(|e| format!("预编译排名后涨幅失败: {e}"))?;
    let mut rows = stmt
        .query(params![DEFAULT_ADJ_TYPE, trade_date, DEFAULT_ADJ_TYPE])
        .map_err(|e| format!("查询排名后涨幅失败: {e}"))?;

    let mut out = HashMap::with_capacity(ts_codes.len());
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取排名后涨幅失败: {e}"))?
    {
        let ts_code: String = row
            .get(0)
            .map_err(|e| format!("读取排名后涨幅代码失败: {e}"))?;
        let next_open: Option<f64> = row
            .get(1)
            .map_err(|e| format!("读取次日开盘价字段失败: {e}"))?;
        let latest_close: Option<f64> = row
            .get(2)
            .map_err(|e| format!("读取最新收盘价字段失败: {e}"))?;

        let post_rank_return_pct = match (next_open, latest_close) {
            (Some(open), Some(close)) if open > 0.0 => Some((close / open - 1.0) * 100.0),
            _ => None,
        };
        out.insert(ts_code, post_rank_return_pct);
    }

    Ok(out)
}

pub fn get_rank_trade_date_options(source_path: String) -> Result<Vec<String>, String> {
    if !result_db_path(&source_path).exists() {
        return Ok(Vec::new());
    }

    let conn = open_result_conn(&source_path)?;
    query_rank_trade_date_options_from_conn(&conn)
}

pub fn get_rank_overview(
    source_path: String,
    trade_date: Option<String>,
    limit: Option<u32>,
    board: Option<String>,
    exclude_st_board: Option<bool>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
) -> Result<Vec<OverviewRow>, String> {
    if let (Some(min_v), Some(max_v)) = (total_mv_min, total_mv_max) {
        if min_v > max_v {
            return Err("总市值最小值不能大于最大值".to_string());
        }
    }

    let conn = open_result_conn(&source_path)?;
    let effective_trade_date = resolve_trade_date(&conn, trade_date)?;

    let name_map = build_name_map(&source_path)?;
    let total_mv_map = build_total_mv_map(&source_path)?;
    let concepts_map = build_concepts_map(&source_path)?;

    let sql = r#"
    SELECT
        ts_code,
        trade_date,
        total_score,
        rank
    FROM score_summary
    WHERE trade_date = ?
    ORDER BY COALESCE(rank, 999999) ASC, total_score DESC, ts_code ASC
    "#;

    let mut stmt = conn.prepare(sql).map_err(|e| format!("预编译失败: {e}"))?;
    let mut rows = stmt
        .query(params![effective_trade_date])
        .map_err(|e| format!("查询失败: {e}"))?;

    let board_filter = board
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty() && value != "全部");
    let exclude_st_board = exclude_st_board.unwrap_or(false);

    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(|e| format!("读行失败: {e}"))? {
        let ts_code: String = row.get(0).map_err(|e| format!("读取 ts_code 失败: {e}"))?;
        let board_value =
            board_category(&ts_code, name_map.get(&ts_code).map(|value| value.as_str()))
                .to_string();

        if exclude_st_board && board_value == BOARD_ST {
            continue;
        }

        if let Some(ref board_value_filter) = board_filter {
            if &board_value != board_value_filter {
                continue;
            }
        }

        let total_mv_yi = total_mv_map.get(&ts_code).copied();
        if !filter_mv(&total_mv_map, &ts_code, total_mv_min, total_mv_max) {
            continue;
        }

        out.push(OverviewRow {
            ts_code: ts_code.clone(),
            trade_date: Some(
                row.get(1)
                    .map_err(|e| format!("读取 trade_date 失败: {e}"))?,
            ),
            ref_date: None,
            total_score: Some(
                row.get(2)
                    .map_err(|e| format!("读取 total_score 失败: {e}"))?,
            ),
            tiebreak_j: None,
            rank: row.get(3).map_err(|e| format!("读取 rank 失败: {e}"))?,
            ref_rank: None,
            post_rank_return_pct: None,
            name: name_map.get(&ts_code).cloned().unwrap_or_default(),
            board: board_value,
            total_mv_yi,
            concept: concepts_map.get(&ts_code).cloned().unwrap_or_default(),
        });

        if let Some(limit_value) = limit {
            if limit_value > 0 && out.len() >= limit_value as usize {
                break;
            }
        }
    }

    Ok(out)
}

pub fn get_rank_overview_page(
    source_path: String,
    rank_date: Option<String>,
    ref_date: Option<String>,
    limit: Option<u32>,
    board: Option<String>,
    exclude_st_board: Option<bool>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
) -> Result<OverviewPageData, String> {
    let result_conn = open_result_conn(&source_path)?;
    let effective_rank_date = resolve_trade_date(&result_conn, rank_date)?;
    let effective_ref_date = resolve_trade_date(&result_conn, ref_date)?;
    let rank_date_options = query_rank_trade_date_options_from_conn(&result_conn)?;

    let mut rows = get_rank_overview(
        source_path.clone(),
        Some(effective_rank_date.clone()),
        limit,
        board,
        exclude_st_board,
        total_mv_min,
        total_mv_max,
    )?;

    let source_conn = open_source_conn(&source_path)?;
    let ts_codes = rows
        .iter()
        .map(|row| row.ts_code.clone())
        .collect::<Vec<_>>();
    let ref_rank_map = query_rank_map(&result_conn, &effective_ref_date)?;
    let tiebreak_j_map = query_tiebreak_j_map(&source_conn, &effective_rank_date)?;
    let post_rank_return_pct_map =
        query_post_rank_return_pct_map(&source_conn, &effective_rank_date, &ts_codes)?;

    for row in &mut rows {
        row.ref_date = Some(effective_ref_date.clone());
        row.ref_rank = ref_rank_map.get(&row.ts_code).copied().flatten();
        row.tiebreak_j = tiebreak_j_map.get(&row.ts_code).copied().flatten();
        row.post_rank_return_pct = post_rank_return_pct_map
            .get(&row.ts_code)
            .copied()
            .flatten();
    }

    Ok(OverviewPageData {
        rows,
        rank_date_options: Some(rank_date_options),
        resolved_rank_date: Some(effective_rank_date),
        resolved_ref_date: Some(effective_ref_date),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_result_conn() -> Connection {
        let conn = Connection::open_in_memory().expect("in-memory result db should open");
        conn.execute_batch(
            r#"
            CREATE TABLE score_summary (
                ts_code TEXT,
                trade_date TEXT,
                total_score DOUBLE,
                rank BIGINT
            );
            INSERT INTO score_summary VALUES
                ('000001.SZ', '20240103', 90.0, 1),
                ('000002.SZ', '20240103', 80.0, 2),
                ('000001.SZ', '20240101', 70.0, 4),
                ('000002.SZ', '20240101', 60.0, 8);
            "#,
        )
        .expect("result fixture should be created");
        conn
    }

    fn build_source_conn() -> Connection {
        let conn = Connection::open_in_memory().expect("in-memory source db should open");
        conn.execute_batch(
            r#"
            CREATE TABLE stock_data (
                ts_code TEXT,
                trade_date TEXT,
                adj_type TEXT,
                open DOUBLE,
                close DOUBLE,
                j DOUBLE
            );
            INSERT INTO stock_data VALUES
                ('000001.SZ', '20240103', 'qfq', 10.0, 11.0, 33.0),
                ('000001.SZ', '20240104', 'qfq', 12.0, 18.0, 34.0),
                ('000001.SZ', '20240105', 'qfq', 20.0, 24.0, 35.0),
                ('000002.SZ', '20240103', 'qfq', 5.0, 6.0, 22.0),
                ('000002.SZ', '20240104', 'qfq', 0.0, 8.0, 23.0),
                ('000001.SZ', '20240105', 'hfq', 200.0, 240.0, 350.0);
            "#,
        )
        .expect("source fixture should be created");
        conn
    }

    #[test]
    fn overview_page_batch_queries_return_supplement_fields() {
        let result_conn = build_result_conn();
        let source_conn = build_source_conn();

        let rank_map = query_rank_map(&result_conn, "20240101").expect("rank map");
        assert_eq!(rank_map.get("000001.SZ").copied().flatten(), Some(4));
        assert_eq!(rank_map.get("000002.SZ").copied().flatten(), Some(8));

        let j_map = query_tiebreak_j_map(&source_conn, "20240103").expect("j map");
        assert_eq!(j_map.get("000001.SZ").copied().flatten(), Some(33.0));
        assert_eq!(j_map.get("000002.SZ").copied().flatten(), Some(22.0));

        let returns = query_post_rank_return_pct_map(
            &source_conn,
            "20240103",
            &[
                "000001.SZ".to_string(),
                "000002.SZ".to_string(),
                "000003.SZ".to_string(),
            ],
        )
        .expect("return map");

        let first_return = returns
            .get("000001.SZ")
            .copied()
            .flatten()
            .expect("first stock should have return");
        assert!((first_return - 100.0).abs() < 1e-9);
        assert_eq!(returns.get("000002.SZ").copied().flatten(), None);
        assert_eq!(returns.get("000003.SZ").copied().flatten(), None);
    }
}
