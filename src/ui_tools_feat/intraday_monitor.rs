use duckdb::{params, Connection};
use serde::{Deserialize, Serialize};

use crate::{
    data::result_db_path,
    ui_tools_feat::{
        build_concepts_map, build_latest_vol_map, build_name_map, build_total_mv_map, filter_mv,
        realtime::fetch_realtime_quote_map,
    },
    utils::utils::board_category,
};

const BOARD_ST: &str = "ST";

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IntradayMonitorRow {
    pub rank_mode: String,
    pub ts_code: String,
    pub trade_date: Option<String>,
    pub scene_name: String,
    pub direction: Option<String>,
    pub total_score: Option<f64>,
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
    pub scene_options: Option<Vec<String>>,
    pub refreshed_at: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IntradayRankMode {
    Total,
    Scene,
}

impl IntradayRankMode {
    fn parse(raw: Option<&str>) -> Result<Self, String> {
        let normalized = raw
            .map(|value| value.trim().to_ascii_lowercase())
            .unwrap_or_else(|| "total".to_string());
        match normalized.as_str() {
            "" | "total" | "summary" | "overall" => Ok(Self::Total),
            "scene" => Ok(Self::Scene),
            _ => Err("rank_mode 仅支持 total 或 scene".to_string()),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Total => "total",
            Self::Scene => "scene",
        }
    }

    fn table_name(self) -> &'static str {
        match self {
            Self::Total => "score_summary",
            Self::Scene => "scene_details",
        }
    }
}

fn open_result_conn(source_path: &str) -> Result<Connection, String> {
    let result_db = result_db_path(source_path);
    let result_db_str = result_db
        .to_str()
        .ok_or_else(|| "结果库路径不是有效UTF-8".to_string())?;
    Connection::open(result_db_str).map_err(|e| format!("打开结果库失败: {e}"))
}

fn resolve_trade_date(
    conn: &Connection,
    trade_date: Option<String>,
    rank_mode: IntradayRankMode,
) -> Result<String, String> {
    if let Some(d) = trade_date {
        let d = d.trim().to_string();
        if !d.is_empty() {
            return Ok(d);
        }
    }

    let table_name = rank_mode.table_name();
    let sql = format!("SELECT MAX(trade_date) FROM {table_name}");
    let mut stmt = conn
        .prepare(&sql)
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
    Err(format!("{} 没有可用交易日", table_name))
}

fn query_rank_trade_date_options_from_conn(
    conn: &Connection,
    rank_mode: IntradayRankMode,
) -> Result<Vec<String>, String> {
    let table_name = rank_mode.table_name();
    let sql = format!(
        r#"
            SELECT DISTINCT trade_date
            FROM {table_name}
            ORDER BY trade_date DESC
            "#
    );
    let mut stmt = conn
        .prepare(&sql)
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

fn query_scene_options_from_conn(conn: &Connection) -> Result<Vec<String>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT DISTINCT scene_name
            FROM scene_details
            WHERE scene_name IS NOT NULL
            ORDER BY scene_name ASC
            "#,
        )
        .map_err(|e| format!("预编译场景列表失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询场景列表失败: {e}"))?;
    let mut out = Vec::new();

    while let Some(row) = rows.next().map_err(|e| format!("读取场景列表失败: {e}"))? {
        let scene_name: String = row.get(0).map_err(|e| format!("读取场景名称失败: {e}"))?;
        let scene_name = scene_name.trim();
        if !scene_name.is_empty() {
            out.push(scene_name.to_string());
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
            scene_options: None,
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
        scene_options: None,
        refreshed_at: fetch_meta.refreshed_at,
    })
}

pub fn get_intraday_monitor_page(
    source_path: &str,
    rank_mode: Option<String>,
    rank_date: Option<String>,
    scene_name: Option<String>,
    limit: Option<u32>,
    board: Option<String>,
    exclude_st_board: Option<bool>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
) -> Result<IntradayMonitorPageData, String> {
    if let (Some(min_v), Some(max_v)) = (total_mv_min, total_mv_max) {
        if min_v > max_v {
            return Err("总市值最小值不能大于最大值".to_string());
        }
    }

    let conn = open_result_conn(source_path)?;
    let rank_mode = IntradayRankMode::parse(rank_mode.as_deref())?;
    let effective_rank_date = resolve_trade_date(&conn, rank_date, rank_mode)?;
    let rank_date_options = query_rank_trade_date_options_from_conn(&conn, rank_mode)?;
    let scene_options = if rank_mode == IntradayRankMode::Scene {
        Some(query_scene_options_from_conn(&conn)?)
    } else {
        None
    };
    let scene_filter = if rank_mode == IntradayRankMode::Scene {
        scene_name
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    } else {
        None
    };
    if let (IntradayRankMode::Scene, Some(wanted_scene_name), Some(scene_names)) =
        (rank_mode, scene_filter.as_ref(), scene_options.as_ref())
    {
        if !scene_names.iter().any(|item| item == wanted_scene_name) {
            return Err(format!("场景不存在: {wanted_scene_name}"));
        }
    }

    let name_map = build_name_map(source_path)?;
    let total_mv_map = build_total_mv_map(source_path)?;
    let concepts_map = build_concepts_map(source_path)?;

    let board_filter = board
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty() && value != "全部");
    let exclude_st_board = exclude_st_board.unwrap_or(false);

    let limit = limit.filter(|value| *value > 0).map(|value| value as usize);
    let mut base_rows = Vec::new();

    match rank_mode {
        IntradayRankMode::Total => {
            let mut stmt = conn
                .prepare(
                    r#"
                    SELECT
                        ts_code,
                        trade_date,
                        total_score,
                        rank
                    FROM score_summary
                    WHERE trade_date = ?
                    ORDER BY
                        COALESCE(rank, 999999) ASC,
                        total_score DESC,
                        ts_code ASC
                    "#,
                )
                .map_err(|e| format!("预编译总榜查询失败: {e}"))?;
            let mut db_rows = stmt
                .query(params![effective_rank_date])
                .map_err(|e| format!("查询总榜失败: {e}"))?;

            while let Some(row) = db_rows.next().map_err(|e| format!("读取总榜失败: {e}"))? {
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
                if !filter_mv(&total_mv_map, &ts_code, total_mv_min, total_mv_max) {
                    continue;
                }

                base_rows.push(IntradayMonitorRow {
                    rank_mode: rank_mode.as_str().to_string(),
                    ts_code: ts_code.clone(),
                    trade_date: Some(
                        row.get(1)
                            .map_err(|e| format!("读取 trade_date 失败: {e}"))?,
                    ),
                    scene_name: "总榜".to_string(),
                    direction: None,
                    total_score: row
                        .get(2)
                        .map_err(|e| format!("读取 total_score 失败: {e}"))?,
                    scene_score: None,
                    risk_score: None,
                    confirm_strength: None,
                    risk_intensity: None,
                    scene_status: None,
                    rank: row.get(3).map_err(|e| format!("读取 rank 失败: {e}"))?,
                    name: name_map.get(&ts_code).cloned().unwrap_or_default(),
                    board: board_value,
                    total_mv_yi: total_mv_map.get(&ts_code).copied(),
                    concept: concepts_map.get(&ts_code).cloned().unwrap_or_default(),
                    realtime_price: None,
                    realtime_change_pct: None,
                    realtime_change_open_pct: None,
                    realtime_vol_ratio: None,
                });

                if let Some(limit_value) = limit {
                    if base_rows.len() >= limit_value {
                        break;
                    }
                }
            }
        }
        IntradayRankMode::Scene => {
            let mut per_scene_count = std::collections::HashMap::<String, usize>::new();
            let (sql, query_scene_name): (&str, Option<String>) = if scene_filter.is_some() {
                (
                    r#"
                    SELECT
                        d.ts_code,
                        d.trade_date,
                        d.scene_name,
                        d.direction,
                        d.stage_score,
                        d.risk_score,
                        d.confirm_strength,
                        d.risk_intensity,
                        d.stage,
                        d.scene_rank,
                        s.total_score
                    FROM scene_details AS d
                    LEFT JOIN score_summary AS s
                      ON d.ts_code = s.ts_code
                     AND d.trade_date = s.trade_date
                    WHERE d.trade_date = ?
                      AND d.scene_name = ?
                    ORDER BY
                        COALESCE(d.scene_rank, 999999) ASC,
                        COALESCE(d.confirm_strength, 0.0) DESC,
                        COALESCE(s.total_score, -1e18) DESC,
                        d.ts_code ASC
                    "#,
                    scene_filter.clone(),
                )
            } else {
                (
                    r#"
                    SELECT
                        d.ts_code,
                        d.trade_date,
                        d.scene_name,
                        d.direction,
                        d.stage_score,
                        d.risk_score,
                        d.confirm_strength,
                        d.risk_intensity,
                        d.stage,
                        d.scene_rank,
                        s.total_score
                    FROM scene_details AS d
                    LEFT JOIN score_summary AS s
                      ON d.ts_code = s.ts_code
                     AND d.trade_date = s.trade_date
                    WHERE d.trade_date = ?
                    ORDER BY
                        COALESCE(d.scene_rank, 999999) ASC,
                        d.scene_name ASC,
                        COALESCE(d.confirm_strength, 0.0) DESC,
                        COALESCE(s.total_score, -1e18) DESC,
                        d.ts_code ASC
                    "#,
                    None,
                )
            };

            let mut stmt = conn
                .prepare(sql)
                .map_err(|e| format!("预编译场景榜查询失败: {e}"))?;
            let mut db_rows = if let Some(scene_name) = query_scene_name {
                stmt.query(params![effective_rank_date, scene_name])
            } else {
                stmt.query(params![effective_rank_date])
            }
            .map_err(|e| format!("查询场景榜失败: {e}"))?;

            while let Some(row) = db_rows.next().map_err(|e| format!("读取场景榜失败: {e}"))?
            {
                let ts_code: String = row.get(0).map_err(|e| format!("读取 ts_code 失败: {e}"))?;
                let this_scene_name: String = row
                    .get(2)
                    .map_err(|e| format!("读取 scene_name 失败: {e}"))?;

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

                if !filter_mv(&total_mv_map, &ts_code, total_mv_min, total_mv_max) {
                    continue;
                }

                base_rows.push(IntradayMonitorRow {
                    rank_mode: rank_mode.as_str().to_string(),
                    ts_code: ts_code.clone(),
                    trade_date: Some(
                        row.get(1)
                            .map_err(|e| format!("读取 trade_date 失败: {e}"))?,
                    ),
                    scene_name: this_scene_name.clone(),
                    direction: row
                        .get(3)
                        .map_err(|e| format!("读取 direction 失败: {e}"))?,
                    total_score: row
                        .get(10)
                        .map_err(|e| format!("读取 total_score 失败: {e}"))?,
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

                if let Some(limit_value) = limit {
                    if scene_filter.is_some() {
                        if base_rows.len() >= limit_value {
                            break;
                        }
                    } else {
                        let next_count =
                            per_scene_count.get(&this_scene_name).copied().unwrap_or(0);
                        if next_count >= limit_value {
                            let _ = base_rows.pop();
                            continue;
                        }
                        per_scene_count.insert(this_scene_name, next_count + 1);
                    }
                }
            }
        }
    }

    Ok(IntradayMonitorPageData {
        rows: base_rows,
        rank_date_options: Some(rank_date_options),
        resolved_rank_date: Some(effective_rank_date),
        scene_options,
        refreshed_at: None,
    })
}
