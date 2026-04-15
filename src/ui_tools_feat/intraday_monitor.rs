use duckdb::{Connection, params};
use serde::{Deserialize, Serialize};

use crate::{
    data::result_db_path,
    ui_tools_feat::{
        build_concepts_map, build_latest_vol_map, build_name_map, build_total_mv_map, filter_mv,
        realtime::fetch_realtime_quote_map,
    },
    utils::utils::board_category,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IntradayMonitorRow {
    pub ts_code: String,
    pub trade_date: Option<String>,
    pub scene_name: String,
    pub direction: Option<String>,
    pub scene_score: Option<f64>,
    pub risk_score: Option<f64>,
    pub confirm_strength: Option<f64>,
    pub risk_intensity: Option<f64>,
    pub scene_status: Option<String>,
    pub rank: Option<i64>,
    pub name: String,
    pub board: String,
    pub total_mv_yi: Option<f64>,
    pub concept: String,
    pub realtime_price: Option<f64>,
    pub realtime_change_pct: Option<f64>,
    pub realtime_change_open_pct: Option<f64>,
    pub realtime_vol_ratio: Option<f64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IntradayMonitorPageData {
    pub rows: Vec<IntradayMonitorRow>,
    pub rank_date_options: Option<Vec<String>>,
    pub resolved_rank_date: Option<String>,
    pub refreshed_at: Option<String>,
}

fn open_result_conn(source_path: &str) -> Result<Connection, String> {
    let result_db = result_db_path(source_path);
    let result_db_str = result_db
        .to_str()
        .ok_or_else(|| "结果库路径不是有效UTF-8".to_string())?;
    Connection::open(result_db_str).map_err(|e| format!("打开结果库失败: {e}"))
}

fn resolve_trade_date(conn: &Connection, trade_date: Option<String>) -> Result<String, String> {
    if let Some(d) = trade_date {
        let d = d.trim().to_string();
        if !d.is_empty() {
            return Ok(d);
        }
    }

    let mut stmt = conn
        .prepare("SELECT MAX(trade_date) FROM scene_details")
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
    Err("scene_details 没有可用交易日".to_string())
}

fn query_rank_trade_date_options_from_conn(conn: &Connection) -> Result<Vec<String>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT DISTINCT trade_date
            FROM scene_details
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

pub fn refresh_intraday_monitor_realtime(
    source_path: &str,
    rows: Vec<IntradayMonitorRow>,
) -> Result<IntradayMonitorPageData, String> {
    if rows.is_empty() {
        return Ok(IntradayMonitorPageData {
            rows,
            rank_date_options: None,
            resolved_rank_date: None,
            refreshed_at: None,
        });
    }

    let ts_codes = rows
        .iter()
        .map(|item| item.ts_code.clone())
        .collect::<Vec<_>>();
    let latest_vol_map = build_latest_vol_map(source_path, &ts_codes).unwrap_or_default();
    let (quote_map, fetch_meta) = fetch_realtime_quote_map(&ts_codes)?;

    let mut next_rows = rows;
    for row in &mut next_rows {
        if let Some(quote) = quote_map.get(&row.ts_code) {
            row.realtime_price = Some(quote.price);
            row.realtime_change_pct = quote.change_pct;
            row.realtime_change_open_pct = if quote.open > 0.0 {
                Some((quote.price / quote.open - 1.0) * 100.0)
            } else {
                None
            };
            row.realtime_vol_ratio = latest_vol_map
                .get(&row.ts_code)
                .copied()
                .filter(|value| *value > 0.0)
                .map(|value| quote.vol / value);
        } else {
            row.realtime_price = None;
            row.realtime_change_pct = None;
            row.realtime_change_open_pct = None;
            row.realtime_vol_ratio = None;
        }
    }

    Ok(IntradayMonitorPageData {
        rows: next_rows,
        rank_date_options: None,
        resolved_rank_date: None,
        refreshed_at: fetch_meta.refreshed_at,
    })
}

pub fn get_intraday_monitor_page(
    source_path: &str,
    rank_date: Option<String>,
    scene_name: Option<String>,
    limit: Option<u32>,
    board: Option<String>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
) -> Result<IntradayMonitorPageData, String> {
    if let (Some(min_v), Some(max_v)) = (total_mv_min, total_mv_max) {
        if min_v > max_v {
            return Err("总市值最小值不能大于最大值".to_string());
        }
    }

    let conn = open_result_conn(source_path)?;
    let effective_rank_date = resolve_trade_date(&conn, rank_date)?;
    let rank_date_options = query_rank_trade_date_options_from_conn(&conn)?;
    let scene_filter = scene_name
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    let name_map = build_name_map(source_path)?;
    let total_mv_map = build_total_mv_map(source_path)?;
    let concepts_map = build_concepts_map(source_path)?;

    let board_filter = board
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty() && value != "全部");

    let mut stmt = conn
        .prepare(
            r#"
            SELECT
                ts_code,
                trade_date,
                scene_name,
                direction,
                stage_score,
                risk_score,
                confirm_strength,
                risk_intensity,
                stage,
                scene_rank
            FROM scene_details
            WHERE trade_date = ?
            ORDER BY
                scene_name ASC,
                COALESCE(scene_rank, 999999) ASC,
                COALESCE(confirm_strength, 0.0) DESC,
                ts_code ASC
            "#,
        )
        .map_err(|e| format!("预编译失败: {e}"))?;
    let mut db_rows = stmt
        .query(params![effective_rank_date])
        .map_err(|e| format!("查询失败: {e}"))?;

    let per_scene_limit = limit.filter(|value| *value > 0).map(|value| value as usize);
    let mut base_rows = Vec::new();
    let mut per_scene_count = std::collections::HashMap::<String, usize>::new();
    while let Some(row) = db_rows.next().map_err(|e| format!("读行失败: {e}"))? {
        let ts_code: String = row.get(0).map_err(|e| format!("读取 ts_code 失败: {e}"))?;
        let this_scene_name: String = row
            .get(2)
            .map_err(|e| format!("读取 scene_name 失败: {e}"))?;

        if let Some(ref wanted_scene_name) = scene_filter {
            if &this_scene_name != wanted_scene_name {
                continue;
            }
        }

        let board_value =
            board_category(&ts_code, name_map.get(&ts_code).map(|value| value.as_str()))
                .to_string();

        if let Some(ref board_value_filter) = board_filter {
            if &board_value != board_value_filter {
                continue;
            }
        }

        if !filter_mv(&total_mv_map, &ts_code, total_mv_min, total_mv_max) {
            continue;
        }

        if let Some(limit_value) = per_scene_limit {
            let next_count = per_scene_count.get(&this_scene_name).copied().unwrap_or(0);
            if next_count >= limit_value {
                continue;
            }
            per_scene_count.insert(this_scene_name.clone(), next_count + 1);
        }

        base_rows.push(IntradayMonitorRow {
            ts_code: ts_code.clone(),
            trade_date: Some(
                row.get(1)
                    .map_err(|e| format!("读取 trade_date 失败: {e}"))?,
            ),
            scene_name: this_scene_name,
            direction: row
                .get(3)
                .map_err(|e| format!("读取 direction 失败: {e}"))?,
            scene_score: row
                .get(4)
                .map_err(|e| format!("读取 scene_score 失败: {e}"))?,
            risk_score: row
                .get(5)
                .map_err(|e| format!("读取 risk_score 失败: {e}"))?,
            confirm_strength: row
                .get(6)
                .map_err(|e| format!("读取 confirm_strength 失败: {e}"))?,
            risk_intensity: row
                .get(7)
                .map_err(|e| format!("读取 risk_intensity 失败: {e}"))?,
            scene_status: row
                .get::<_, Option<String>>(8)
                .map_err(|e| format!("读取 scene_status 失败: {e}"))?,
            rank: row.get(9).map_err(|e| format!("读取 rank 失败: {e}"))?,
            name: name_map.get(&ts_code).cloned().unwrap_or_default(),
            board: board_value,
            total_mv_yi: total_mv_map.get(&ts_code).copied(),
            concept: concepts_map.get(&ts_code).cloned().unwrap_or_default(),
            realtime_price: None,
            realtime_change_pct: None,
            realtime_change_open_pct: None,
            realtime_vol_ratio: None,
        });
    }

    Ok(IntradayMonitorPageData {
        rows: base_rows,
        rank_date_options: Some(rank_date_options),
        resolved_rank_date: Some(effective_rank_date),
        refreshed_at: None,
    })
}
