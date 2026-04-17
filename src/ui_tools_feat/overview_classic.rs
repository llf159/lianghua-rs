use duckdb::{params, Connection};
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

fn query_rank_trade_date_options_from_conn(conn: &Connection) -> Result<Vec<String>, String> {
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

fn query_optional_rank(
    conn: &Connection,
    trade_date: &str,
    ts_code: &str,
) -> Result<Option<i64>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT rank
            FROM score_summary
            WHERE trade_date = ? AND ts_code = ?
            LIMIT 1
            "#,
        )
        .map_err(|e| format!("预编译参考日排名失败: {e}"))?;
    let mut rows = stmt
        .query(params![trade_date, ts_code])
        .map_err(|e| format!("查询参考日排名失败: {e}"))?;

    if let Some(row) = rows
        .next()
        .map_err(|e| format!("读取参考日排名失败: {e}"))?
    {
        let rank: Option<i64> = row
            .get(0)
            .map_err(|e| format!("读取参考日排名字段失败: {e}"))?;
        Ok(rank)
    } else {
        Ok(None)
    }
}

fn query_optional_j(
    source_conn: &Connection,
    trade_date: &str,
    ts_code: &str,
) -> Result<Option<f64>, String> {
    let mut stmt = source_conn
        .prepare(
            r#"
            SELECT TRY_CAST(j AS DOUBLE)
            FROM stock_data
            WHERE ts_code = ? AND trade_date = ? AND adj_type = ?
            LIMIT 1
            "#,
        )
        .map_err(|e| format!("预编译同分排序J失败: {e}"))?;
    let mut rows = stmt
        .query(params![ts_code, trade_date, DEFAULT_ADJ_TYPE])
        .map_err(|e| format!("查询同分排序J失败: {e}"))?;

    if let Some(row) = rows.next().map_err(|e| format!("读取同分排序J失败: {e}"))? {
        let j_value: Option<f64> = row
            .get(0)
            .map_err(|e| format!("读取同分排序J字段失败: {e}"))?;
        Ok(j_value)
    } else {
        Ok(None)
    }
}

fn query_optional_next_open(
    source_conn: &Connection,
    trade_date: &str,
    ts_code: &str,
) -> Result<Option<f64>, String> {
    let mut stmt = source_conn
        .prepare(
            r#"
            SELECT TRY_CAST(open AS DOUBLE)
            FROM stock_data
            WHERE ts_code = ? AND adj_type = ? AND trade_date > ?
            ORDER BY trade_date ASC
            LIMIT 1
            "#,
        )
        .map_err(|e| format!("预编译次日开盘价失败: {e}"))?;
    let mut rows = stmt
        .query(params![ts_code, DEFAULT_ADJ_TYPE, trade_date])
        .map_err(|e| format!("查询次日开盘价失败: {e}"))?;

    if let Some(row) = rows
        .next()
        .map_err(|e| format!("读取次日开盘价失败: {e}"))?
    {
        let open_value: Option<f64> = row
            .get(0)
            .map_err(|e| format!("读取次日开盘价字段失败: {e}"))?;
        Ok(open_value)
    } else {
        Ok(None)
    }
}

fn query_optional_latest_close(
    source_conn: &Connection,
    ts_code: &str,
) -> Result<Option<f64>, String> {
    let mut stmt = source_conn
        .prepare(
            r#"
            SELECT TRY_CAST(close AS DOUBLE)
            FROM stock_data
            WHERE ts_code = ? AND adj_type = ?
            ORDER BY trade_date DESC
            LIMIT 1
            "#,
        )
        .map_err(|e| format!("预编译最新收盘价失败: {e}"))?;
    let mut rows = stmt
        .query(params![ts_code, DEFAULT_ADJ_TYPE])
        .map_err(|e| format!("查询最新收盘价失败: {e}"))?;

    if let Some(row) = rows
        .next()
        .map_err(|e| format!("读取最新收盘价失败: {e}"))?
    {
        let close_value: Option<f64> = row
            .get(0)
            .map_err(|e| format!("读取最新收盘价字段失败: {e}"))?;
        Ok(close_value)
    } else {
        Ok(None)
    }
}

fn calc_post_rank_return_pct(
    source_conn: &Connection,
    trade_date: &str,
    ts_code: &str,
) -> Result<Option<f64>, String> {
    let Some(next_open) = query_optional_next_open(source_conn, trade_date, ts_code)? else {
        return Ok(None);
    };
    if next_open <= 0.0 {
        return Ok(None);
    }

    let Some(latest_close) = query_optional_latest_close(source_conn, ts_code)? else {
        return Ok(None);
    };

    Ok(Some((latest_close / next_open - 1.0) * 100.0))
}

pub fn get_rank_trade_date_options(source_path: String) -> Result<Vec<String>, String> {
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
    for row in &mut rows {
        row.ref_date = Some(effective_ref_date.clone());
        row.ref_rank = query_optional_rank(&result_conn, &effective_ref_date, &row.ts_code)?;
        row.tiebreak_j = query_optional_j(&source_conn, &effective_rank_date, &row.ts_code)?;
        row.post_rank_return_pct =
            calc_post_rank_return_pct(&source_conn, &effective_rank_date, &row.ts_code)?;
    }

    Ok(OverviewPageData {
        rows,
        rank_date_options: Some(rank_date_options),
        resolved_rank_date: Some(effective_rank_date),
        resolved_ref_date: Some(effective_ref_date),
    })
}
