use chrono::{Local, Timelike};
use duckdb::{Connection, params};
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
        .map_err(|e| format!("预编译自选排名失败: {e}"))?;
    let mut rows = stmt
        .query(params![trade_date, ts_code])
        .map_err(|e| format!("查询自选排名失败: {e}"))?;

    if let Some(row) = rows.next().map_err(|e| format!("读取自选排名失败: {e}"))? {
        let rank: Option<i64> = row
            .get(0)
            .map_err(|e| format!("读取自选排名字段失败: {e}"))?;
        Ok(rank)
    } else {
        Ok(None)
    }
}

fn query_optional_scene_marker(
    conn: &Connection,
    trade_date: &str,
    ts_code: &str,
    scene_stage_threshold: Option<&str>,
) -> Result<Option<String>, String> {
    let threshold_level = parse_scene_stage_threshold(scene_stage_threshold);
    let mut stmt = conn
        .prepare(
            r#"
            SELECT scene_name, scene_rank, stage
            FROM scene_details
            WHERE trade_date = ?
              AND ts_code = ?
              AND scene_name IS NOT NULL
              AND TRIM(scene_name) <> ''
            ORDER BY COALESCE(scene_rank, 999999) ASC, scene_name ASC
            "#,
        )
        .map_err(|e| format!("预编译自选场景排名失败: {e}"))?;
    let mut rows = stmt
        .query(params![trade_date, ts_code])
        .map_err(|e| format!("查询自选场景排名失败: {e}"))?;

    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取自选场景排名失败: {e}"))?
    {
        let scene_name: String = row
            .get(0)
            .map_err(|e| format!("读取自选场景名称失败: {e}"))?;
        let scene_rank: Option<i64> = row
            .get(1)
            .map_err(|e| format!("读取自选场景排名字段失败: {e}"))?;
        let stage: Option<String> = row
            .get(2)
            .map_err(|e| format!("读取自选场景等级失败: {e}"))?;
        if scene_stage_level(stage.as_deref()) < threshold_level {
            continue;
        }

        return Ok(Some(match scene_rank {
            Some(rank) => format!("{} #{}", scene_name.trim(), rank),
            None => scene_name.trim().to_string(),
        }));
    }

    Ok(None)
}

fn query_latest_snapshot(
    source_conn: &Connection,
    ts_code: &str,
) -> Result<LatestSnapshot, String> {
    let mut stmt = source_conn
        .prepare(
            r#"
            SELECT TRY_CAST(close AS DOUBLE)
            FROM stock_data
            WHERE ts_code = ? AND adj_type = ?
            ORDER BY trade_date DESC
            LIMIT 4
            "#,
        )
        .map_err(|e| format!("预编译自选最新快照失败: {e}"))?;
    let mut rows = stmt
        .query(params![ts_code, DEFAULT_ADJ_TYPE])
        .map_err(|e| format!("查询自选最新快照失败: {e}"))?;

    let mut closes = Vec::with_capacity(4);
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取自选最新快照失败: {e}"))?
    {
        let value: Option<f64> = row
            .get(0)
            .map_err(|e| format!("读取自选最新快照字段失败: {e}"))?;
        closes.push(value);
    }

    let latest_close = closes.first().copied().flatten();
    let previous_close = closes.get(1).copied().flatten();
    let latest_change_pct = match (latest_close, previous_close) {
        (Some(latest), Some(previous)) if previous > 0.0 => Some((latest / previous - 1.0) * 100.0),
        _ => None,
    };

    Ok(LatestSnapshot {
        latest_close,
        latest_change_pct,
        realtime_3d_base_close: closes.get(2).copied().flatten(),
        daily_3d_base_close: closes.get(3).copied().flatten(),
    })
}

fn calc_return_pct(price: Option<f64>, base: Option<f64>) -> Option<f64> {
    match (price, base) {
        (Some(price), Some(base)) if price.is_finite() && base.is_finite() && base > 0.0 => {
            Some((price / base - 1.0) * 100.0)
        }
        _ => None,
    }
}

fn calc_post_watch_return_pct(
    source_conn: &Connection,
    trade_date: &str,
    ts_code: &str,
    latest_price_override: Option<f64>,
) -> Result<Option<f64>, String> {
    let Some(next_open) = (|source_conn: &Connection,
                            trade_date: &str,
                            ts_code: &str|
     -> Result<Option<f64>, String> {
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
            .map_err(|e| format!("预编译自选次日开盘价失败: {e}"))?;
        let mut rows = stmt
            .query(params![ts_code, DEFAULT_ADJ_TYPE, trade_date])
            .map_err(|e| format!("查询自选次日开盘价失败: {e}"))?;

        if let Some(row) = rows
            .next()
            .map_err(|e| format!("读取自选次日开盘价失败: {e}"))?
        {
            let open_value: Option<f64> = row
                .get(0)
                .map_err(|e| format!("读取自选次日开盘价字段失败: {e}"))?;
            Ok(open_value)
        } else {
            Ok(None)
        }
    })(source_conn, trade_date, ts_code)?
    else {
        return Ok(None);
    };
    if next_open <= 0.0 {
        return Ok(None);
    }

    let Some(latest_close) = latest_price_override.or((|source_conn: &Connection,
                                                        ts_code: &str|
     -> Result<Option<f64>, String> {
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
            .map_err(|e| format!("预编译自选最新收盘价失败: {e}"))?;
        let mut rows = stmt
            .query(params![ts_code, DEFAULT_ADJ_TYPE])
            .map_err(|e| format!("查询自选最新收盘价失败: {e}"))?;

        if let Some(row) = rows
            .next()
            .map_err(|e| format!("读取自选最新收盘价失败: {e}"))?
        {
            let close_value: Option<f64> = row
                .get(0)
                .map_err(|e| format!("读取自选最新收盘价字段失败: {e}"))?;
            Ok(close_value)
        } else {
            Ok(None)
        }
    })(source_conn, ts_code)?) else {
        return Ok(None);
    };

    Ok(Some((latest_close / next_open - 1.0) * 100.0))
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
        let latest_snapshot = source_conn
            .as_ref()
            .and_then(|conn| query_latest_snapshot(conn, &row.ts_code).ok())
            .unwrap_or_default();
        let latest_close = latest_snapshot.latest_close;
        let latest_change_pct = latest_snapshot.latest_change_pct;
        let volume_ratio = None;
        let return_3d_pct = calc_return_pct(latest_close, latest_snapshot.daily_3d_base_close);
        let today_rank = match (result_conn.as_ref(), resolved_rank_trade_date.as_deref()) {
            (Some(conn), Some(trade_date)) => query_optional_rank(conn, trade_date, &row.ts_code)?,
            _ => None,
        };
        let scene_marker = match (result_conn.as_ref(), resolved_rank_trade_date.as_deref()) {
            (Some(conn), Some(trade_date)) => query_optional_scene_marker(
                conn,
                trade_date,
                &row.ts_code,
                scene_stage_threshold.as_deref(),
            )
            .unwrap_or_default(),
            _ => None,
        };
        let observe_trade_date = row
            .marked_date
            .as_deref()
            .and_then(normalize_trade_date)
            .or_else(|| normalize_trade_date(&row.watch_date));
        let post_watch_return_pct = match (source_conn.as_ref(), observe_trade_date.as_deref()) {
            (Some(conn), Some(trade_date)) => {
                calc_post_watch_return_pct(conn, trade_date, &row.ts_code, None)?
            }
            _ => None,
        };

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
        let fallback_snapshot = source_conn
            .as_ref()
            .and_then(|conn| query_latest_snapshot(conn, &row.ts_code).ok())
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
        let observe_trade_date = row
            .marked_date
            .as_deref()
            .and_then(normalize_trade_date)
            .or_else(|| normalize_trade_date(&row.watch_date));
        let post_watch_return_pct = match (source_conn.as_ref(), observe_trade_date.as_deref()) {
            (Some(conn), Some(trade_date)) => {
                calc_post_watch_return_pct(conn, trade_date, &row.ts_code, latest_close)?
            }
            _ => None,
        };
        let today_rank = match (
            result_conn.as_ref(),
            resolved_reference_trade_date.as_deref(),
        ) {
            (Some(conn), Some(trade_date)) => query_optional_rank(conn, trade_date, &row.ts_code)?,
            _ => None,
        };
        let scene_marker = match (
            result_conn.as_ref(),
            resolved_reference_trade_date.as_deref(),
        ) {
            (Some(conn), Some(trade_date)) => query_optional_scene_marker(
                conn,
                trade_date,
                &row.ts_code,
                scene_stage_threshold.as_deref(),
            )
            .unwrap_or_default(),
            _ => None,
        };

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
        calc_return_pct, query_latest_snapshot, query_optional_scene_marker,
        resolve_watch_date_for_clock,
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
            query_optional_scene_marker(&conn, "20260729", "000001.SZ", Some("trigger"))
                .unwrap()
                .as_deref(),
            Some("触发场景 #2")
        );
        assert_eq!(
            query_optional_scene_marker(&conn, "20260729", "000001.SZ", Some("observe"))
                .unwrap()
                .as_deref(),
            Some("观察场景 #1")
        );
        assert_eq!(
            query_optional_scene_marker(&conn, "20260729", "000001.SZ", Some("confirm"))
                .unwrap()
                .as_deref(),
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
                close DOUBLE
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
                "INSERT INTO stock_data VALUES (?, ?, ?, ?)",
                params!["000001.SZ", "qfq", trade_date, close],
            )
            .unwrap();
        }

        let snapshot = query_latest_snapshot(&conn, "000001.SZ").unwrap();
        let daily_return =
            calc_return_pct(snapshot.latest_close, snapshot.daily_3d_base_close).unwrap();
        assert!((daily_return - 30.0).abs() < 0.000_001);
        let realtime_return = calc_return_pct(Some(14.0), snapshot.realtime_3d_base_close).unwrap();
        assert!((realtime_return - 27.272_727).abs() < 0.000_001);
    }
}
