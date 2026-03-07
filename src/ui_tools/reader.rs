use std::collections::HashMap;

use duckdb::{Connection, params};
use serde::Serialize;

use crate::utils::utils::{
    board_category, load_stock_list, load_ths_concepts_list, result_db_path,
};

#[derive(Debug, Serialize)]
pub struct OverviewRow {
    pub ts_code: String,
    pub name: String,
    pub trade_date: String,
    pub total_score: f64,
    pub rank: Option<i64>,
    pub board: String,
    pub total_mv_yi: Option<f64>,
    pub concept: String,
}

fn build_total_mv_map(source_dir: &str) -> Result<HashMap<String, f64>, String> {
    let stock_list = load_stock_list(source_dir)?;
    let mut out = HashMap::with_capacity(stock_list.len());
    for cols in stock_list {
        let Some(ts_code) = cols.first() else {
            continue;
        };
        let Some(total_mv_raw) = cols.get(9) else {
            continue;
        };
        let Ok(total_mv) = total_mv_raw.trim().parse::<f64>() else {
            continue;
        };
        // total_mv 单位是万元，这里统一换算到“亿”
        out.insert(ts_code.trim().to_string(), total_mv / 1e4);
    }
    Ok(out)
}

fn filter_mv(
    total_mv_map: &HashMap<String, f64>,
    ts_code: &str,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
) -> bool {
    if total_mv_max.is_none() && total_mv_min.is_none() {
        return true;
    }
    let Some(total_mv) = total_mv_map.get(ts_code).copied() else {
        return false;
    };

    if let Some(min_v) = total_mv_min {
        if total_mv < min_v {
            return false;
        }
    }

    if let Some(max_v) = total_mv_max {
        if total_mv > max_v {
            return false;
        }
    }

    true
}

fn build_concepts_map(source_dir: &str) -> Result<HashMap<String, String>, String> {
    let concepts_list = load_ths_concepts_list(source_dir)?;
    let mut out = HashMap::new();
    for cols in concepts_list {
        let Some(ts_code) = cols.first() else {
            continue;
        };
        let Some(concept) = cols.get(2) else {
            continue;
        };
        out.insert(ts_code.to_string(), concept.to_string());
    }
    Ok(out)
}

// fn concept_query(ts_code: &str, concepts_map: &HashMap<String, String>) -> String {
//     concepts_map.get(ts_code).cloned().unwrap_or_default()
// }

fn build_name_map(source_dir: &str) -> Result<HashMap<String, String>, String> {
    let stock_list = load_stock_list(source_dir)?;
    let mut out = HashMap::with_capacity(stock_list.len());

    for cols in stock_list {
        let Some(ts_code) = cols.first() else {
            continue;
        };
        let Some(name) = cols.get(2) else {
            continue;
        };

        out.insert(ts_code.trim().to_string(), name.trim().to_string());
    }

    Ok(out)
}

fn resolve_trade_date(conn: &Connection, trade_date: Option<String>) -> Result<String, String> {
    if let Some(d) = trade_date {
        let d = d.trim().to_string();
        if !d.is_empty() {
            return Ok(d);
        }
    }

    let mut stmt = conn
        .prepare("SELECT MAX(trade_date) FROM score_summary")
        .map_err(|e| format!("查询最新交易日预编译失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询最新交易日失败: {e}"))?;

    if let Some(row) = rows
        .next()
        .map_err(|e| format!("读取最新交易日结果失败: {e}"))?
    {
        let d: Option<String> = row
            .get(0)
            .map_err(|e| format!("读取最新交易日字段失败: {e}"))?;
        if let Some(v) = d {
            if !v.trim().is_empty() {
                return Ok(v);
            }
        }
    }
    Err("score_summary 没有可用交易日".to_string())
}

pub fn get_rank_overview(
    source_path: String,
    trade_date: Option<String>,
    limit: Option<u32>,
    board: Option<String>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
) -> Result<Vec<OverviewRow>, String> {
    if let (Some(min_v), Some(max_v)) = (total_mv_min, total_mv_max) {
        if min_v > max_v {
            return Err("总市值最小值不能大于最大值".to_string());
        }
    }

    let result_db = result_db_path(&source_path);
    let result_db_str = result_db
        .to_str()
        .ok_or_else(|| "结果库路径不是有效UTF-8".to_string())?;
    let conn = Connection::open(result_db_str).map_err(|e| format!("打开结果库失败: {e}"))?;
    let effective_trade_date = resolve_trade_date(&conn, trade_date)?;

    let name_map = build_name_map(&source_path)?;
    // 无论是否启用筛选，都返回总市值列，前端可直接展示
    let total_mv_map = build_total_mv_map(&source_path)?;
    let concepts_map = build_concepts_map(&source_path)?;

    let sql = r#"
    SELECT
        ts_code,
        trade_date,
        total_score,
        "rank"
    FROM score_summary
    WHERE trade_date = ?
    ORDER BY COALESCE(rank, 999999) ASC, total_score DESC, ts_code ASC
  "#;

    let mut stmt = conn.prepare(sql).map_err(|e| format!("预编译失败: {e}"))?;
    let mut rows = stmt
        .query(params![effective_trade_date])
        .map_err(|e| format!("查询失败: {e}"))?;

    let board_filter = board
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty() && v != "全部");

    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(|e| format!("读行失败: {e}"))? {
        let ts_code: String = row.get(0).map_err(|e| format!("读取 ts_code 失败: {e}"))?;
        let name = name_map.get(&ts_code).cloned().unwrap_or_default();
        let board_val = board_category(&ts_code).to_string();

        if let Some(ref bf) = board_filter {
            if &board_val != bf {
                continue;
            }
        }

        let total_mv_yi = total_mv_map.get(&ts_code).copied();
        if !filter_mv(&total_mv_map, &ts_code, total_mv_min, total_mv_max) {
            continue;
        }

        let concept = concepts_map.get(&ts_code).cloned().unwrap_or_default();

        out.push(OverviewRow {
            ts_code,
            name,
            trade_date: row
                .get(1)
                .map_err(|e| format!("读取 trade_date 失败: {e}"))?,
            total_score: row
                .get(2)
                .map_err(|e| format!("读取 total_score 失败: {e}"))?,
            rank: row.get(3).map_err(|e| format!("读取 rank 失败: {e}"))?,
            board: board_val,
            total_mv_yi,
            concept,
        });
    }

    if let Some(n) = limit {
        if n > 0 && out.len() > n as usize {
            out.truncate(n as usize);
        }
    }

    Ok(out)
}
