use chrono::{Local, Timelike};
use duckdb::{Connection, params_from_iter};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::{
    data::{load_trade_date_list, result_db_path, source_db_path},
    realtime::{RealtimeFetchMeta, fetch_realtime_quote_map},
};
use lianghua_app_shared::{
    build_concepts_map, build_latest_vol_map, build_name_map, resolve_trade_date,
};

use super::scene_stage::{level as scene_stage_level, threshold as parse_scene_stage_threshold};

pub use lianghua_app_shared::{normalize_trade_date, normalize_ts_code};

const DEFAULT_ADJ_TYPE: &str = "qfq";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WatchObserveStoredRow {
    pub ts_code: String,
    pub name: String,
    #[serde(default, alias = "addedDate")]
    pub watch_date: String,
    pub tag: String,
    pub concept: String,
    #[serde(default, alias = "tradeDate")]
    pub marked_date: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WatchObserveRow {
    pub ts_code: String,
    pub name: String,
    pub latest_close: Option<f64>,
    pub latest_change_pct: Option<f64>,
    pub volume_ratio: Option<f64>,
    pub return_3d_pct: Option<f64>,
    pub watch_date: String,
    pub post_watch_return_pct: Option<f64>,
    pub today_rank: Option<i64>,
    pub scene_marker: Option<String>,
    pub tag: String,
    pub concept: String,
    pub marked_date: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WatchObserveSnapshotData {
    pub mode: String,
    pub rows: Vec<WatchObserveRow>,
    pub refreshed_at: Option<String>,
    pub reference_trade_date: Option<String>,
    pub requested_count: usize,
    pub effective_count: usize,
    pub fetched_count: usize,
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct LatestSnapshot {
    latest_close: Option<f64>,
    latest_change_pct: Option<f64>,
    realtime_3d_base_close: Option<f64>,
    daily_3d_base_close: Option<f64>,
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

pub fn resolve_watch_date_for_clock(
    trade_dates: &[String],
    today: &str,
    current_hhmm: u32,
) -> Result<String, String> {
    let today = normalize_trade_date(today).ok_or_else(|| format!("系统日期无效: {today}"))?;
    let mut normalized_dates: Vec<String> = trade_dates
        .iter()
        .filter_map(|value| normalize_trade_date(value))
        .collect();
    normalized_dates.sort();
    normalized_dates.dedup();

    if current_hhmm >= 930 && normalized_dates.binary_search(&today).is_ok() {
        return Ok(today);
    }

    normalized_dates
        .into_iter()
        .rev()
        .find(|value| value < &today)
        .ok_or_else(|| format!("交易日历中找不到 {today} 开盘前的上一交易日"))
}

pub fn resolve_current_watch_date(source_path: &str) -> Result<String, String> {
    let trade_dates = load_trade_date_list(source_path)?;
    let now = Local::now();
    resolve_watch_date_for_clock(
        &trade_dates,
        &now.format("%Y%m%d").to_string(),
        now.hour() * 100 + now.minute(),
    )
}

fn query_rank_map(
    conn: &Connection,
    trade_date: &str,
    ts_codes: &[String],
) -> Result<HashMap<String, i64>, String> {
    if ts_codes.is_empty() {
        return Ok(HashMap::new());
    }
    let placeholders = std::iter::repeat_n("?", ts_codes.len())
        .collect::<Vec<_>>()
        .join(", ");
    let mut stmt = conn
        .prepare(&format!(
            r#"
            SELECT ts_code, rank
            FROM score_summary
            WHERE trade_date = ? AND ts_code IN ({placeholders})
            "#
        ))
        .map_err(|e| format!("预编译自选排名失败: {e}"))?;
    let mut query_params = Vec::with_capacity(ts_codes.len() + 1);
    query_params.push(trade_date.to_string());
    query_params.extend(ts_codes.iter().cloned());
    let mut rows = stmt
        .query(params_from_iter(query_params.iter()))
        .map_err(|e| format!("查询自选排名失败: {e}"))?;
    let mut out = HashMap::with_capacity(ts_codes.len());
    while let Some(row) = rows.next().map_err(|e| format!("读取自选排名失败: {e}"))? {
        let ts_code: String = row
            .get(0)
            .map_err(|e| format!("读取自选排名代码失败: {e}"))?;
        let rank: Option<i64> = row
            .get(1)
            .map_err(|e| format!("读取自选排名字段失败: {e}"))?;
        if let Some(rank) = rank {
            out.insert(ts_code, rank);
        }
    }
    Ok(out)
}

fn query_scene_marker_map(
    conn: &Connection,
    trade_date: &str,
    ts_codes: &[String],
    scene_stage_threshold: Option<&str>,
) -> Result<HashMap<String, String>, String> {
    if ts_codes.is_empty() {
        return Ok(HashMap::new());
    }
    let threshold_level = parse_scene_stage_threshold(scene_stage_threshold);
    let placeholders = std::iter::repeat_n("?", ts_codes.len())
        .collect::<Vec<_>>()
        .join(", ");
    let mut stmt = conn
        .prepare(&format!(
            r#"
            SELECT ts_code, scene_name, scene_rank, stage
            FROM scene_details
              WHERE trade_date = ?
              AND ts_code IN ({placeholders})
              AND scene_name IS NOT NULL
              AND TRIM(scene_name) <> ''
            ORDER BY ts_code, COALESCE(scene_rank, 999999) ASC, scene_name ASC
            "#
        ))
        .map_err(|e| format!("预编译自选场景排名失败: {e}"))?;
    let mut query_params = Vec::with_capacity(ts_codes.len() + 1);
    query_params.push(trade_date.to_string());
    query_params.extend(ts_codes.iter().cloned());
    let mut rows = stmt
        .query(params_from_iter(query_params.iter()))
        .map_err(|e| format!("查询自选场景排名失败: {e}"))?;
    let mut out = HashMap::with_capacity(ts_codes.len());
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取自选场景排名失败: {e}"))?
    {
        let ts_code: String = row
            .get(0)
            .map_err(|e| format!("读取自选场景代码失败: {e}"))?;
        if out.contains_key(&ts_code) {
            continue;
        }
        let scene_name: String = row
            .get(1)
            .map_err(|e| format!("读取自选场景名称失败: {e}"))?;
        let scene_rank: Option<i64> = row
            .get(2)
            .map_err(|e| format!("读取自选场景排名字段失败: {e}"))?;
        let stage: Option<String> = row
            .get(3)
            .map_err(|e| format!("读取自选场景等级失败: {e}"))?;
        if scene_stage_level(stage.as_deref()) < threshold_level {
            continue;
        }
        out.insert(
            ts_code,
            match scene_rank {
                Some(rank) => format!("{} #{}", scene_name.trim(), rank),
                None => scene_name.trim().to_string(),
            },
        );
    }
    Ok(out)
}

fn query_latest_snapshot_map(
    source_conn: &Connection,
    ts_codes: &[String],
) -> Result<HashMap<String, LatestSnapshot>, String> {
    if ts_codes.is_empty() {
        return Ok(HashMap::new());
    }
    let placeholders = std::iter::repeat_n("?", ts_codes.len())
        .collect::<Vec<_>>()
        .join(", ");
    let mut stmt = source_conn
        .prepare(&format!(
            r#"
            SELECT
                ts_code,
                MAX(CASE WHEN row_num = 1 THEN close_value END),
                MAX(CASE WHEN row_num = 2 THEN close_value END),
                MAX(CASE WHEN row_num = 3 THEN close_value END),
                MAX(CASE WHEN row_num = 4 THEN close_value END)
            FROM (
                SELECT
                    ts_code,
                    TRY_CAST(close AS DOUBLE) AS close_value,
                    ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS row_num
                FROM stock_data
                WHERE adj_type = ? AND ts_code IN ({placeholders})
            ) ranked
            WHERE row_num <= 4
            GROUP BY ts_code
            "#
        ))
        .map_err(|e| format!("预编译自选最新快照失败: {e}"))?;
    let mut query_params = Vec::with_capacity(ts_codes.len() + 1);
    query_params.push(DEFAULT_ADJ_TYPE.to_string());
    query_params.extend(ts_codes.iter().cloned());
    let mut rows = stmt
        .query(params_from_iter(query_params.iter()))
        .map_err(|e| format!("查询自选最新快照失败: {e}"))?;
    let mut out = HashMap::with_capacity(ts_codes.len());
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取自选最新快照失败: {e}"))?
    {
        let ts_code: String = row
            .get(0)
            .map_err(|e| format!("读取自选最新快照代码失败: {e}"))?;
        let latest_close: Option<f64> =
            row.get(1).map_err(|e| format!("读取自选最新价失败: {e}"))?;
        let previous_close: Option<f64> =
            row.get(2).map_err(|e| format!("读取自选前收盘失败: {e}"))?;
        out.insert(
            ts_code,
            LatestSnapshot {
                latest_close,
                latest_change_pct: match (latest_close, previous_close) {
                    (Some(latest), Some(previous)) if previous > 0.0 => {
                        Some((latest / previous - 1.0) * 100.0)
                    }
                    _ => None,
                },
                realtime_3d_base_close: row
                    .get(3)
                    .map_err(|e| format!("读取自选实时三日基准失败: {e}"))?,
                daily_3d_base_close: row
                    .get(4)
                    .map_err(|e| format!("读取自选日线三日基准失败: {e}"))?,
            },
        );
    }
    Ok(out)
}

fn calc_return_pct(price: Option<f64>, base: Option<f64>) -> Option<f64> {
    match (price, base) {
        (Some(price), Some(base)) if price.is_finite() && base.is_finite() && base > 0.0 => {
            Some((price / base - 1.0) * 100.0)
        }
        _ => None,
    }
}

fn query_post_watch_open_map(
    source_conn: &Connection,
    observe_dates: &HashMap<String, String>,
) -> Result<HashMap<String, f64>, String> {
    if observe_dates.is_empty() {
        return Ok(HashMap::new());
    }
    let requested_rows = std::iter::repeat_n("(?, ?)", observe_dates.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
        WITH requested(ts_code, observe_date) AS (VALUES {requested_rows})
        SELECT
            requested.ts_code,
            ARG_MIN(TRY_CAST(stock_data.open AS DOUBLE), stock_data.trade_date)
        FROM requested
        LEFT JOIN stock_data
          ON stock_data.ts_code = requested.ts_code
         AND stock_data.adj_type = ?
         AND stock_data.trade_date > requested.observe_date
        GROUP BY requested.ts_code
        "#
    );
    let mut query_params = Vec::with_capacity(observe_dates.len() * 2 + 1);
    for (ts_code, observe_date) in observe_dates {
        query_params.push(ts_code.clone());
        query_params.push(observe_date.clone());
    }
    query_params.push(DEFAULT_ADJ_TYPE.to_string());
    let mut stmt = source_conn
        .prepare(&sql)
        .map_err(|e| format!("预编译自选次日开盘价失败: {e}"))?;
    let mut rows = stmt
        .query(params_from_iter(query_params.iter()))
        .map_err(|e| format!("查询自选次日开盘价失败: {e}"))?;
    let mut out = HashMap::with_capacity(observe_dates.len());
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取自选次日开盘价失败: {e}"))?
    {
        let ts_code: String = row
            .get(0)
            .map_err(|e| format!("读取自选次日开盘价代码失败: {e}"))?;
        let next_open: Option<f64> = row
            .get(1)
            .map_err(|e| format!("读取自选次日开盘价字段失败: {e}"))?;
        if let Some(next_open) = next_open.filter(|value| *value > 0.0) {
            out.insert(ts_code, next_open);
        }
    }
    Ok(out)
}

pub fn hydrate_watch_observe_rows(
    source_path: Option<&str>,
    stored_rows: &[WatchObserveStoredRow],
    reference_trade_date: Option<String>,
    scene_stage_threshold: Option<String>,
) -> Result<Vec<WatchObserveRow>, String> {
    let Some(source_path) = source_path.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(stored_rows
            .iter()
            .map(|row| WatchObserveRow {
                ts_code: row.ts_code.clone(),
                name: row.name.clone(),
                latest_close: None,
                latest_change_pct: None,
                volume_ratio: None,
                return_3d_pct: None,
                watch_date: row.watch_date.clone(),
                post_watch_return_pct: None,
                today_rank: None,
                scene_marker: None,
                tag: row.tag.clone(),
                concept: row.concept.clone(),
                marked_date: row.marked_date.clone(),
            })
            .collect());
    };

    let name_map = build_name_map(source_path).unwrap_or_default();
    let concepts_map = build_concepts_map(source_path).unwrap_or_default();
    let source_conn = open_source_conn(source_path).ok();
    let result_conn = open_result_conn(source_path).ok();
    let resolved_rank_trade_date = match (result_conn.as_ref(), reference_trade_date) {
        (Some(conn), trade_date) => Some(resolve_trade_date(conn, trade_date)?),
        (None, trade_date) => trade_date,
    };
    let ts_codes = stored_rows
        .iter()
        .map(|row| row.ts_code.clone())
        .collect::<Vec<_>>();
    let observe_dates = stored_rows
        .iter()
        .filter_map(|row| {
            row.marked_date
                .as_deref()
                .and_then(normalize_trade_date)
                .or_else(|| normalize_trade_date(&row.watch_date))
                .map(|trade_date| (row.ts_code.clone(), trade_date))
        })
        .collect::<HashMap<_, _>>();
    let latest_snapshot_map = source_conn
        .as_ref()
        .and_then(|conn| query_latest_snapshot_map(conn, &ts_codes).ok())
        .unwrap_or_default();
    let post_watch_open_map = match source_conn.as_ref() {
        Some(conn) => query_post_watch_open_map(conn, &observe_dates)?,
        None => HashMap::new(),
    };
    let today_rank_map = match (result_conn.as_ref(), resolved_rank_trade_date.as_deref()) {
        (Some(conn), Some(trade_date)) => query_rank_map(conn, trade_date, &ts_codes)?,
        _ => HashMap::new(),
    };
    let scene_marker_map = match (result_conn.as_ref(), resolved_rank_trade_date.as_deref()) {
        (Some(conn), Some(trade_date)) => query_scene_marker_map(
            conn,
            trade_date,
            &ts_codes,
            scene_stage_threshold.as_deref(),
        )
        .unwrap_or_default(),
        _ => HashMap::new(),
    };

    let mut out = Vec::with_capacity(stored_rows.len());
    for row in stored_rows {
        let name = name_map
            .get(&row.ts_code)
            .cloned()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| row.name.clone());
        let concept = concepts_map
            .get(&row.ts_code)
            .cloned()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| row.concept.clone());
        let latest_snapshot = latest_snapshot_map
            .get(&row.ts_code)
            .copied()
            .unwrap_or_default();
        let latest_close = latest_snapshot.latest_close;
        let latest_change_pct = latest_snapshot.latest_change_pct;
        let volume_ratio = None;
        let return_3d_pct = calc_return_pct(latest_close, latest_snapshot.daily_3d_base_close);
        let today_rank = today_rank_map.get(&row.ts_code).copied();
        let scene_marker = scene_marker_map.get(&row.ts_code).cloned();
        let post_watch_return_pct = post_watch_open_map
            .get(&row.ts_code)
            .and_then(|next_open| calc_return_pct(latest_close, Some(*next_open)));

        out.push(WatchObserveRow {
            ts_code: row.ts_code.clone(),
            name,
            latest_close,
            latest_change_pct,
            volume_ratio,
            return_3d_pct,
            watch_date: row.watch_date.clone(),
            post_watch_return_pct,
            today_rank,
            scene_marker,
            tag: row.tag.clone(),
            concept,
            marked_date: row.marked_date.clone(),
        });
    }

    Ok(out)
}

pub fn refresh_watch_observe_rows(
    source_path: Option<&str>,
    stored_rows: &[WatchObserveStoredRow],
    reference_trade_date: Option<String>,
    scene_stage_threshold: Option<String>,
) -> Result<WatchObserveSnapshotData, String> {
    let ts_codes: Vec<String> = stored_rows.iter().map(|row| row.ts_code.clone()).collect();
    let (quote_map, fetch_meta) = fetch_realtime_quote_map(&ts_codes)?;
    build_watch_observe_snapshot_data(
        source_path,
        stored_rows,
        reference_trade_date,
        scene_stage_threshold,
        quote_map,
        fetch_meta,
    )
}

pub fn build_watch_observe_snapshot_data(
    source_path: Option<&str>,
    stored_rows: &[WatchObserveStoredRow],
    reference_trade_date: Option<String>,
    scene_stage_threshold: Option<String>,
    quote_map: HashMap<String, crate::crawler::SinaQuote>,
    fetch_meta: RealtimeFetchMeta,
) -> Result<WatchObserveSnapshotData, String> {
    let name_map = source_path
        .map(build_name_map)
        .transpose()?
        .unwrap_or_default();
    let concepts_map = source_path
        .map(build_concepts_map)
        .transpose()?
        .unwrap_or_default();
    let source_conn = source_path.and_then(|path| open_source_conn(path).ok());
    let result_conn = source_path.and_then(|path| open_result_conn(path).ok());
    let ts_codes: Vec<String> = stored_rows.iter().map(|row| row.ts_code.clone()).collect();
    let latest_vol_map = source_path
        .and_then(|path| build_latest_vol_map(path, &ts_codes).ok())
        .unwrap_or_default();
    let resolved_reference_trade_date = match (result_conn.as_ref(), reference_trade_date) {
        (Some(conn), trade_date) => Some(resolve_trade_date(conn, trade_date)?),
        (None, trade_date) => trade_date.and_then(|value| normalize_trade_date(&value)),
    };
    let observe_dates = stored_rows
        .iter()
        .filter_map(|row| {
            row.marked_date
                .as_deref()
                .and_then(normalize_trade_date)
                .or_else(|| normalize_trade_date(&row.watch_date))
                .map(|trade_date| (row.ts_code.clone(), trade_date))
        })
        .collect::<HashMap<_, _>>();
    let latest_snapshot_map = source_conn
        .as_ref()
        .and_then(|conn| query_latest_snapshot_map(conn, &ts_codes).ok())
        .unwrap_or_default();
    let post_watch_open_map = match source_conn.as_ref() {
        Some(conn) => query_post_watch_open_map(conn, &observe_dates)?,
        None => HashMap::new(),
    };
    let today_rank_map = match (
        result_conn.as_ref(),
        resolved_reference_trade_date.as_deref(),
    ) {
        (Some(conn), Some(trade_date)) => query_rank_map(conn, trade_date, &ts_codes)?,
        _ => HashMap::new(),
    };
    let scene_marker_map = match (
        result_conn.as_ref(),
        resolved_reference_trade_date.as_deref(),
    ) {
        (Some(conn), Some(trade_date)) => query_scene_marker_map(
            conn,
            trade_date,
            &ts_codes,
            scene_stage_threshold.as_deref(),
        )
        .unwrap_or_default(),
        _ => HashMap::new(),
    };

    let mut out = Vec::with_capacity(stored_rows.len());
    for row in stored_rows {
        let name = name_map
            .get(&row.ts_code)
            .cloned()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| row.name.clone());
        let concept = concepts_map
            .get(&row.ts_code)
            .cloned()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| row.concept.clone());
        let quote = quote_map.get(&row.ts_code);
        let fallback_snapshot = latest_snapshot_map
            .get(&row.ts_code)
            .copied()
            .unwrap_or_default();
        let latest_close = quote
            .map(|item| item.price)
            .or(fallback_snapshot.latest_close);
        let latest_change_pct = quote
            .and_then(|item| item.change_pct)
            .or(fallback_snapshot.latest_change_pct);
        let volume_ratio = match (
            quote.map(|item| item.vol),
            latest_vol_map.get(&row.ts_code).copied(),
        ) {
            (Some(current_vol), Some(previous_vol)) if previous_vol > 0.0 => {
                Some(current_vol / previous_vol)
            }
            _ => None,
        };
        let return_3d_pct = if quote.is_some() {
            calc_return_pct(latest_close, fallback_snapshot.realtime_3d_base_close)
        } else {
            calc_return_pct(latest_close, fallback_snapshot.daily_3d_base_close)
        };
        let post_watch_return_pct = post_watch_open_map
            .get(&row.ts_code)
            .and_then(|next_open| calc_return_pct(latest_close, Some(*next_open)));
        let today_rank = today_rank_map.get(&row.ts_code).copied();
        let scene_marker = scene_marker_map.get(&row.ts_code).cloned();

        out.push(WatchObserveRow {
            ts_code: row.ts_code.clone(),
            name,
            latest_close,
            latest_change_pct,
            volume_ratio,
            return_3d_pct,
            watch_date: row.watch_date.clone(),
            post_watch_return_pct,
            today_rank,
            scene_marker,
            tag: row.tag.clone(),
            concept,
            marked_date: row.marked_date.clone(),
        });
    }

    Ok(WatchObserveSnapshotData {
        mode: "realtime".to_string(),
        rows: out,
        refreshed_at: fetch_meta.refreshed_at,
        reference_trade_date: resolved_reference_trade_date,
        requested_count: fetch_meta.requested_count,
        effective_count: fetch_meta.effective_count,
        fetched_count: fetch_meta.fetched_count,
        truncated: fetch_meta.truncated,
    })
}

#[cfg(test)]
mod tests {
    use duckdb::{Connection, params};

    use super::{
        calc_return_pct, query_latest_snapshot_map, query_post_watch_open_map,
        query_scene_marker_map, resolve_watch_date_for_clock,
    };

    fn dates() -> Vec<String> {
        ["20260727", "20260728", "20260729", "20260731"]
            .into_iter()
            .map(str::to_string)
            .collect()
    }

    #[test]
    fn watch_date_stays_on_previous_trade_day_before_open() {
        assert_eq!(
            resolve_watch_date_for_clock(&dates(), "20260729", 929).unwrap(),
            "20260728"
        );
    }

    #[test]
    fn watch_date_switches_to_today_at_open() {
        assert_eq!(
            resolve_watch_date_for_clock(&dates(), "20260729", 930).unwrap(),
            "20260729"
        );
    }

    #[test]
    fn watch_date_stays_on_today_after_close() {
        assert_eq!(
            resolve_watch_date_for_clock(&dates(), "20260729", 1700).unwrap(),
            "20260729"
        );
    }

    #[test]
    fn watch_date_stays_on_last_trade_day_during_market_break() {
        assert_eq!(
            resolve_watch_date_for_clock(&dates(), "20260730", 1200).unwrap(),
            "20260729"
        );
    }

    #[test]
    fn scene_marker_uses_best_rank_meeting_monitor_threshold() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute(
            "CREATE TABLE scene_details (
                trade_date VARCHAR,
                ts_code VARCHAR,
                scene_name VARCHAR,
                scene_rank BIGINT,
                stage VARCHAR
            )",
            [],
        )
        .unwrap();
        for (scene_name, scene_rank, stage) in [
            ("观察场景", 1_i64, "observe"),
            ("触发场景", 2_i64, "trigger"),
            ("确认场景", 5_i64, "confirm"),
        ] {
            conn.execute(
                "INSERT INTO scene_details VALUES (?, ?, ?, ?, ?)",
                params!["20260729", "000001.SZ", scene_name, scene_rank, stage],
            )
            .unwrap();
        }

        assert_eq!(
            query_scene_marker_map(
                &conn,
                "20260729",
                &["000001.SZ".to_string()],
                Some("trigger")
            )
            .unwrap()
            .get("000001.SZ")
            .map(String::as_str),
            Some("触发场景 #2")
        );
        assert_eq!(
            query_scene_marker_map(
                &conn,
                "20260729",
                &["000001.SZ".to_string()],
                Some("observe")
            )
            .unwrap()
            .get("000001.SZ")
            .map(String::as_str),
            Some("观察场景 #1")
        );
        assert_eq!(
            query_scene_marker_map(
                &conn,
                "20260729",
                &["000001.SZ".to_string()],
                Some("confirm")
            )
            .unwrap()
            .get("000001.SZ")
            .map(String::as_str),
            Some("确认场景 #5")
        );
    }

    #[test]
    fn three_day_return_uses_daily_and_realtime_baselines() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute(
            "CREATE TABLE stock_data (
                ts_code VARCHAR,
                adj_type VARCHAR,
                trade_date VARCHAR,
                close DOUBLE,
                open DOUBLE
            )",
            [],
        )
        .unwrap();
        for (trade_date, close) in [
            ("20260724", 10.0_f64),
            ("20260727", 11.0_f64),
            ("20260728", 12.0_f64),
            ("20260729", 13.0_f64),
        ] {
            conn.execute(
                "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?)",
                params!["000001.SZ", "qfq", trade_date, close, close - 0.5],
            )
            .unwrap();
        }

        let snapshots = query_latest_snapshot_map(&conn, &["000001.SZ".to_string()]).unwrap();
        let snapshot = snapshots.get("000001.SZ").unwrap();
        let daily_return =
            calc_return_pct(snapshot.latest_close, snapshot.daily_3d_base_close).unwrap();
        assert!((daily_return - 30.0).abs() < 0.000_001);
        let realtime_return = calc_return_pct(Some(14.0), snapshot.realtime_3d_base_close).unwrap();
        assert!((realtime_return - 27.272_727).abs() < 0.000_001);

        let next_opens = query_post_watch_open_map(
            &conn,
            &[("000001.SZ".to_string(), "20260727".to_string())]
                .into_iter()
                .collect(),
        )
        .unwrap();
        assert_eq!(next_opens.get("000001.SZ"), Some(&11.5));
    }
}
