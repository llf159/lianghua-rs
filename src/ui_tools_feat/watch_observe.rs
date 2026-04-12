use duckdb::{Connection, params};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::{
    data::{result_db_path, source_db_path},
    ui_tools_feat::{
        build_concepts_map, build_latest_vol_map, build_name_map,
        realtime::{RealtimeFetchMeta, fetch_realtime_quote_map},
        resolve_trade_date,
    },
};

const DEFAULT_ADJ_TYPE: &str = "qfq";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WatchObserveStoredRow {
    pub ts_code: String,
    pub name: String,
    pub added_date: String,
    pub tag: String,
    pub concept: String,
    pub trade_date: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WatchObserveRow {
    pub ts_code: String,
    pub name: String,
    pub latest_close: Option<f64>,
    pub latest_change_pct: Option<f64>,
    pub volume_ratio: Option<f64>,
    pub added_date: String,
    pub post_watch_return_pct: Option<f64>,
    pub today_rank: Option<i64>,
    pub tag: String,
    pub concept: String,
    pub trade_date: Option<String>,
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

pub fn normalize_ts_code(raw: &str) -> Option<String> {
    let trimmed = raw.trim().to_ascii_uppercase();
    if trimmed.is_empty() {
        return None;
    }

    if trimmed.contains('.') {
        return Some(trimmed);
    }

    let digits: String = trimmed.chars().filter(|ch| ch.is_ascii_digit()).collect();
    if digits.len() != 6 {
        return None;
    }

    let suffix = if digits.starts_with("30") || digits.starts_with("00") {
        ".SZ"
    } else if digits.starts_with("60") || digits.starts_with("68") {
        ".SH"
    } else {
        ".BJ"
    };

    Some(format!("{digits}{suffix}"))
}

pub fn normalize_trade_date(raw: &str) -> Option<String> {
    let digits: String = raw
        .trim()
        .chars()
        .filter(|ch| ch.is_ascii_digit())
        .collect();
    if digits.len() == 8 {
        Some(digits)
    } else {
        None
    }
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
}

fn query_latest_snapshot(
    source_conn: &Connection,
    ts_code: &str,
) -> Result<(Option<f64>, Option<f64>), String> {
    let mut stmt = source_conn
        .prepare(
            r#"
            SELECT TRY_CAST(close AS DOUBLE)
            FROM stock_data
            WHERE ts_code = ? AND adj_type = ?
            ORDER BY trade_date DESC
            LIMIT 2
            "#,
        )
        .map_err(|e| format!("预编译自选最新快照失败: {e}"))?;
    let mut rows = stmt
        .query(params![ts_code, DEFAULT_ADJ_TYPE])
        .map_err(|e| format!("查询自选最新快照失败: {e}"))?;

    let mut closes = Vec::with_capacity(2);
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

    Ok((latest_close, latest_change_pct))
}

fn calc_post_watch_return_pct(
    source_conn: &Connection,
    trade_date: &str,
    ts_code: &str,
    latest_price_override: Option<f64>,
) -> Result<Option<f64>, String> {
    let Some(next_open) = query_optional_next_open(source_conn, trade_date, ts_code)? else {
        return Ok(None);
    };
    if next_open <= 0.0 {
        return Ok(None);
    }

    let Some(latest_close) =
        latest_price_override.or(query_optional_latest_close(source_conn, ts_code)?)
    else {
        return Ok(None);
    };

    Ok(Some((latest_close / next_open - 1.0) * 100.0))
}

pub fn hydrate_watch_observe_rows(
    source_path: Option<&str>,
    stored_rows: &[WatchObserveStoredRow],
    reference_trade_date: Option<String>,
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
                added_date: row.added_date.clone(),
                post_watch_return_pct: None,
                today_rank: None,
                tag: row.tag.clone(),
                concept: row.concept.clone(),
                trade_date: row.trade_date.clone(),
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
        let (latest_close, latest_change_pct) = source_conn
            .as_ref()
            .and_then(|conn| query_latest_snapshot(conn, &row.ts_code).ok())
            .unwrap_or((None, None));
        let volume_ratio = None;
        let today_rank = match (result_conn.as_ref(), resolved_rank_trade_date.as_deref()) {
            (Some(conn), Some(trade_date)) => query_optional_rank(conn, trade_date, &row.ts_code)?,
            _ => None,
        };
        let observe_trade_date = row
            .trade_date
            .as_deref()
            .and_then(normalize_trade_date)
            .or_else(|| normalize_trade_date(&row.added_date));
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
            added_date: row.added_date.clone(),
            post_watch_return_pct,
            today_rank,
            tag: row.tag.clone(),
            concept,
            trade_date: row.trade_date.clone(),
        });
    }

    Ok(out)
}

pub fn refresh_watch_observe_rows(
    source_path: Option<&str>,
    stored_rows: &[WatchObserveStoredRow],
    reference_trade_date: Option<String>,
) -> Result<WatchObserveSnapshotData, String> {
    let ts_codes: Vec<String> = stored_rows.iter().map(|row| row.ts_code.clone()).collect();
    let (quote_map, fetch_meta) = fetch_realtime_quote_map(&ts_codes)?;
    build_watch_observe_snapshot_data(
        source_path,
        stored_rows,
        reference_trade_date,
        quote_map,
        fetch_meta,
    )
}

pub fn build_watch_observe_snapshot_data(
    source_path: Option<&str>,
    stored_rows: &[WatchObserveStoredRow],
    reference_trade_date: Option<String>,
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
            .unwrap_or((None, None));
        let latest_close = quote.map(|item| item.price).or(fallback_snapshot.0);
        let latest_change_pct = quote
            .and_then(|item| item.change_pct)
            .or(fallback_snapshot.1);
        let volume_ratio = match (
            quote.map(|item| item.vol),
            latest_vol_map.get(&row.ts_code).copied(),
        ) {
            (Some(current_vol), Some(previous_vol)) if previous_vol > 0.0 => {
                Some(current_vol / previous_vol)
            }
            _ => None,
        };
        let observe_trade_date = row
            .trade_date
            .as_deref()
            .and_then(normalize_trade_date)
            .or_else(|| normalize_trade_date(&row.added_date));
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

        out.push(WatchObserveRow {
            ts_code: row.ts_code.clone(),
            name,
            latest_close,
            latest_change_pct,
            volume_ratio,
            added_date: row.added_date.clone(),
            post_watch_return_pct,
            today_rank,
            tag: row.tag.clone(),
            concept,
            trade_date: row.trade_date.clone(),
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
