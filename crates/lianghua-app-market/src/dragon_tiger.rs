use std::path::Path;

use duckdb::{Connection, params};
use serde::Serialize;

use crate::data::dragon_tiger_db_path;
use lianghua_app_shared::normalize_trade_date;

#[derive(Debug, Clone, Default, Serialize)]
pub struct DragonTigerMarketSummary {
    pub top_list_rows: usize,
    pub stock_count: usize,
    pub top_inst_rows: usize,
    pub total_l_buy: f64,
    pub total_l_sell: f64,
    pub total_net_amount: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DragonTigerTopListItem {
    pub trade_date: String,
    pub ts_code: String,
    pub name: String,
    pub close: Option<f64>,
    pub pct_change: Option<f64>,
    pub turnover_rate: Option<f64>,
    pub amount: Option<f64>,
    pub l_sell: Option<f64>,
    pub l_buy: Option<f64>,
    pub l_amount: Option<f64>,
    pub net_amount: Option<f64>,
    pub net_rate: Option<f64>,
    pub amount_rate: Option<f64>,
    pub float_values: Option<f64>,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DragonTigerTopInstItem {
    pub trade_date: String,
    pub ts_code: String,
    pub exalter: String,
    pub buy: Option<f64>,
    pub buy_rate: Option<f64>,
    pub sell: Option<f64>,
    pub sell_rate: Option<f64>,
    pub net_buy: Option<f64>,
    pub side: String,
    pub reason: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DragonTigerMarketData {
    pub db_exists: bool,
    pub latest_sync_trade_date: Option<String>,
    pub resolved_trade_date: Option<String>,
    pub available_trade_dates: Vec<String>,
    pub summary: DragonTigerMarketSummary,
    pub top_list: Vec<DragonTigerTopListItem>,
    pub top_inst: Vec<DragonTigerTopInstItem>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DragonTigerStockDetailData {
    pub ts_code: String,
    pub name: String,
    pub resolved_trade_date: Option<String>,
    pub current_list: Vec<DragonTigerTopListItem>,
    pub seats: Vec<DragonTigerTopInstItem>,
    pub history: Vec<DragonTigerTopListItem>,
    pub history_trade_count: usize,
    pub history_record_count: usize,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DragonTigerSeatStatisticsSummary {
    pub appearance_count: usize,
    pub trade_date_count: usize,
    pub stock_count: usize,
    pub buy_count: usize,
    pub sell_count: usize,
    pub total_buy: f64,
    pub total_sell: f64,
    pub total_net_buy: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DragonTigerSeatStatisticsRow {
    pub trade_date: String,
    pub ts_code: String,
    pub name: String,
    pub buy: Option<f64>,
    pub sell: Option<f64>,
    pub net_buy: Option<f64>,
    pub side: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DragonTigerSeatFavoriteStock {
    pub ts_code: String,
    pub name: String,
    pub appearance_count: usize,
    pub total_buy: f64,
    pub total_sell: f64,
    pub total_net_buy: f64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DragonTigerSeatStatisticsData {
    pub exalter: String,
    pub summary: DragonTigerSeatStatisticsSummary,
    pub favorite_stocks: Vec<DragonTigerSeatFavoriteStock>,
    pub recent_records: Vec<DragonTigerSeatStatisticsRow>,
}

fn load_optional_max_date(conn: &Connection, sql: &str) -> Result<Option<String>, String> {
    conn.query_row(sql, [], |row| row.get(0))
        .map_err(|error| format!("读取龙虎榜日期失败: {error}"))
}

fn resolve_trade_date(
    conn: &Connection,
    reference_trade_date: Option<String>,
) -> Result<Option<String>, String> {
    let Some(reference_trade_date) = reference_trade_date else {
        return load_optional_max_date(conn, "SELECT MAX(trade_date) FROM dragon_tiger_sync_log");
    };
    let normalized = normalize_trade_date(&reference_trade_date)
        .ok_or_else(|| "龙虎榜参考日格式无效，请使用 YYYY-MM-DD 或 YYYYMMDD".to_string())?;
    conn.query_row(
        "SELECT MAX(trade_date) FROM dragon_tiger_sync_log WHERE trade_date <= ?",
        [normalized],
        |row| row.get(0),
    )
    .map_err(|error| format!("解析龙虎榜参考交易日失败: {error}"))
}

pub fn get_dragon_tiger_market_data(
    source_dir: String,
    reference_trade_date: Option<String>,
) -> Result<DragonTigerMarketData, String> {
    let db_path = dragon_tiger_db_path(source_dir.trim());
    if !Path::new(&db_path).exists() {
        return Ok(DragonTigerMarketData::default());
    }

    let conn = Connection::open(&db_path).map_err(|error| {
        format!(
            "打开龙虎榜数据库失败: path={}, err={error}",
            db_path.display()
        )
    })?;
    let latest_sync_trade_date =
        load_optional_max_date(&conn, "SELECT MAX(trade_date) FROM dragon_tiger_sync_log")?;
    let resolved_trade_date = resolve_trade_date(&conn, reference_trade_date)?;
    let available_trade_dates = (|conn: &Connection| -> Result<Vec<String>, String> {
        let mut stmt = conn
            .prepare(
                "SELECT trade_date
             FROM dragon_tiger_sync_log
             ORDER BY trade_date DESC
             LIMIT 250",
            )
            .map_err(|error| format!("预编译龙虎榜日期列表查询失败: {error}"))?;
        let rows = stmt
            .query_map([], |row| row.get(0))
            .map_err(|error| format!("查询龙虎榜日期列表失败: {error}"))?;
        rows.collect::<Result<Vec<String>, _>>()
            .map_err(|error| format!("读取龙虎榜日期列表失败: {error}"))
    })(&conn)?;
    let Some(trade_date) = resolved_trade_date.as_deref() else {
        return Ok(DragonTigerMarketData {
            db_exists: true,
            latest_sync_trade_date,
            available_trade_dates,
            ..DragonTigerMarketData::default()
        });
    };

    let top_list =
        (|conn: &Connection, trade_date: &str| -> Result<Vec<DragonTigerTopListItem>, String> {
            let mut stmt = conn
                .prepare(
                    r#"
            SELECT
                trade_date, ts_code, name, close, pct_change, turnover_rate,
                amount, l_sell, l_buy, l_amount, net_amount, net_rate,
                amount_rate, float_values, reason
            FROM top_list
            WHERE trade_date = ?
            ORDER BY net_amount DESC NULLS LAST, ts_code, reason
            "#,
                )
                .map_err(|error| format!("预编译龙虎榜每日明细查询失败: {error}"))?;
            let rows = stmt
                .query_map([trade_date], |row| {
                    Ok(DragonTigerTopListItem {
                        trade_date: row.get(0)?,
                        ts_code: row.get(1)?,
                        name: row.get(2)?,
                        close: row.get(3)?,
                        pct_change: row.get(4)?,
                        turnover_rate: row.get(5)?,
                        amount: row.get(6)?,
                        l_sell: row.get(7)?,
                        l_buy: row.get(8)?,
                        l_amount: row.get(9)?,
                        net_amount: row.get(10)?,
                        net_rate: row.get(11)?,
                        amount_rate: row.get(12)?,
                        float_values: row.get(13)?,
                        reason: row.get(14)?,
                    })
                })
                .map_err(|error| format!("查询龙虎榜每日明细失败: {error}"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|error| format!("读取龙虎榜每日明细失败: {error}"))
        })(&conn, trade_date)?;
    let top_inst =
        (|conn: &Connection, trade_date: &str| -> Result<Vec<DragonTigerTopInstItem>, String> {
            let mut stmt = conn
                .prepare(
                    r#"
            SELECT
                trade_date, ts_code, exalter, buy, buy_rate,
                sell, sell_rate, net_buy, side, reason
            FROM top_inst
            WHERE trade_date = ?
            ORDER BY net_buy DESC NULLS LAST, ts_code, side, exalter
            "#,
                )
                .map_err(|error| format!("预编译龙虎榜席位明细查询失败: {error}"))?;
            let rows = stmt
                .query_map([trade_date], |row| {
                    Ok(DragonTigerTopInstItem {
                        trade_date: row.get(0)?,
                        ts_code: row.get(1)?,
                        exalter: row.get(2)?,
                        buy: row.get(3)?,
                        buy_rate: row.get(4)?,
                        sell: row.get(5)?,
                        sell_rate: row.get(6)?,
                        net_buy: row.get(7)?,
                        side: row.get(8)?,
                        reason: row.get(9)?,
                    })
                })
                .map_err(|error| format!("查询龙虎榜席位明细失败: {error}"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|error| format!("读取龙虎榜席位明细失败: {error}"))
        })(&conn, trade_date)?;
    let mut stock_codes = std::collections::HashSet::new();
    let mut summary = DragonTigerMarketSummary {
        top_list_rows: top_list.len(),
        top_inst_rows: top_inst.len(),
        ..DragonTigerMarketSummary::default()
    };
    for item in &top_list {
        stock_codes.insert(item.ts_code.as_str());
        summary.total_l_buy += item.l_buy.unwrap_or(0.0);
        summary.total_l_sell += item.l_sell.unwrap_or(0.0);
        summary.total_net_amount += item.net_amount.unwrap_or(0.0);
    }
    summary.stock_count = stock_codes.len();

    Ok(DragonTigerMarketData {
        db_exists: true,
        latest_sync_trade_date,
        resolved_trade_date,
        available_trade_dates,
        summary,
        top_list,
        top_inst,
    })
}

pub fn get_dragon_tiger_stock_detail(
    source_dir: String,
    ts_code: String,
    trade_date: String,
) -> Result<DragonTigerStockDetailData, String> {
    let trimmed_code = ts_code.trim().to_uppercase();
    if trimmed_code.is_empty() {
        return Err("个股代码不能为空".to_string());
    }
    let normalized_date = normalize_trade_date(&trade_date)
        .ok_or_else(|| "龙虎榜交易日格式无效，请使用 YYYY-MM-DD 或 YYYYMMDD".to_string())?;
    let db_path = dragon_tiger_db_path(source_dir.trim());
    if !Path::new(&db_path).exists() {
        return Err("尚未发现 dragon_tiger.db，请先同步龙虎榜数据。".to_string());
    }

    let conn = Connection::open(&db_path).map_err(|error| {
        format!(
            "打开龙虎榜数据库失败: path={}, err={error}",
            db_path.display()
        )
    })?;
    let current_list = (|conn: &Connection,
                         trade_date: &str,
                         ts_code: &str|
     -> Result<Vec<DragonTigerTopListItem>, String> {
        let mut stmt = conn
            .prepare(
                r#"
            SELECT
                trade_date, ts_code, name, close, pct_change, turnover_rate,
                amount, l_sell, l_buy, l_amount, net_amount, net_rate,
                amount_rate, float_values, reason
            FROM top_list
            WHERE trade_date = ? AND ts_code = ?
            ORDER BY net_amount DESC NULLS LAST, reason
            "#,
            )
            .map_err(|error| format!("预编译个股龙虎榜当日明细查询失败: {error}"))?;
        let rows = stmt
            .query_map(params![trade_date, ts_code], |row| {
                Ok(DragonTigerTopListItem {
                    trade_date: row.get(0)?,
                    ts_code: row.get(1)?,
                    name: row.get(2)?,
                    close: row.get(3)?,
                    pct_change: row.get(4)?,
                    turnover_rate: row.get(5)?,
                    amount: row.get(6)?,
                    l_sell: row.get(7)?,
                    l_buy: row.get(8)?,
                    l_amount: row.get(9)?,
                    net_amount: row.get(10)?,
                    net_rate: row.get(11)?,
                    amount_rate: row.get(12)?,
                    float_values: row.get(13)?,
                    reason: row.get(14)?,
                })
            })
            .map_err(|error| format!("查询个股龙虎榜当日明细失败: {error}"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("读取个股龙虎榜当日明细失败: {error}"))
    })(&conn, &normalized_date, &trimmed_code)?;
    if current_list.is_empty() {
        return Err(format!(
            "未找到 {trimmed_code} 在 {normalized_date} 的龙虎榜记录"
        ));
    }
    let seats = (|conn: &Connection,
                  trade_date: &str,
                  ts_code: &str|
     -> Result<Vec<DragonTigerTopInstItem>, String> {
        let mut stmt = conn
            .prepare(
                r#"
            SELECT
                trade_date, ts_code, exalter, buy, buy_rate,
                sell, sell_rate, net_buy, side, reason
            FROM top_inst
            WHERE trade_date = ? AND ts_code = ?
            ORDER BY reason, side, net_buy DESC NULLS LAST, exalter
            "#,
            )
            .map_err(|error| format!("预编译个股龙虎榜席位查询失败: {error}"))?;
        let rows = stmt
            .query_map(params![trade_date, ts_code], |row| {
                Ok(DragonTigerTopInstItem {
                    trade_date: row.get(0)?,
                    ts_code: row.get(1)?,
                    exalter: row.get(2)?,
                    buy: row.get(3)?,
                    buy_rate: row.get(4)?,
                    sell: row.get(5)?,
                    sell_rate: row.get(6)?,
                    net_buy: row.get(7)?,
                    side: row.get(8)?,
                    reason: row.get(9)?,
                })
            })
            .map_err(|error| format!("查询个股龙虎榜席位失败: {error}"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("读取个股龙虎榜席位失败: {error}"))
    })(&conn, &normalized_date, &trimmed_code)?;
    let history = (|conn: &Connection,
                    trade_date: &str,
                    ts_code: &str|
     -> Result<Vec<DragonTigerTopListItem>, String> {
        let mut stmt = conn
            .prepare(
                r#"
            SELECT
                trade_date, ts_code, name, close, pct_change, turnover_rate,
                amount, l_sell, l_buy, l_amount, net_amount, net_rate,
                amount_rate, float_values, reason
            FROM top_list
            WHERE ts_code = ? AND trade_date < ?
            ORDER BY trade_date DESC, net_amount DESC NULLS LAST, reason
            LIMIT 100
            "#,
            )
            .map_err(|error| format!("预编译个股历史上榜查询失败: {error}"))?;
        let rows = stmt
            .query_map(params![ts_code, trade_date], |row| {
                Ok(DragonTigerTopListItem {
                    trade_date: row.get(0)?,
                    ts_code: row.get(1)?,
                    name: row.get(2)?,
                    close: row.get(3)?,
                    pct_change: row.get(4)?,
                    turnover_rate: row.get(5)?,
                    amount: row.get(6)?,
                    l_sell: row.get(7)?,
                    l_buy: row.get(8)?,
                    l_amount: row.get(9)?,
                    net_amount: row.get(10)?,
                    net_rate: row.get(11)?,
                    amount_rate: row.get(12)?,
                    float_values: row.get(13)?,
                    reason: row.get(14)?,
                })
            })
            .map_err(|error| format!("查询个股历史上榜失败: {error}"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("读取个股历史上榜失败: {error}"))
    })(&conn, &normalized_date, &trimmed_code)?;
    let (history_trade_count, history_record_count) =
        (|conn: &Connection, trade_date: &str, ts_code: &str| -> Result<(usize, usize), String> {
            let (trade_count, record_count): (i64, i64) = conn
                .query_row(
                    "SELECT COUNT(DISTINCT trade_date), COUNT(*)
             FROM top_list
             WHERE ts_code = ? AND trade_date < ?",
                    params![ts_code, trade_date],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(|error| format!("查询个股历史上榜次数失败: {error}"))?;
            let trade_count = usize::try_from(trade_count)
                .map_err(|error| format!("解析个股历史上榜交易日数失败: {error}"))?;
            let record_count = usize::try_from(record_count)
                .map_err(|error| format!("解析个股历史上榜记录数失败: {error}"))?;
            Ok((trade_count, record_count))
        })(&conn, &normalized_date, &trimmed_code)?;
    let name = current_list
        .first()
        .map(|item| item.name.clone())
        .unwrap_or_default();

    Ok(DragonTigerStockDetailData {
        ts_code: trimmed_code,
        name,
        resolved_trade_date: Some(normalized_date),
        current_list,
        seats,
        history,
        history_trade_count,
        history_record_count,
    })
}

pub fn get_dragon_tiger_seat_statistics(
    source_dir: String,
    exalter: String,
) -> Result<DragonTigerSeatStatisticsData, String> {
    let trimmed_exalter = exalter.trim().to_string();
    if trimmed_exalter.is_empty() {
        return Err("营业部/机构名称不能为空".to_string());
    }
    let db_path = dragon_tiger_db_path(source_dir.trim());
    if !Path::new(&db_path).exists() {
        return Err("尚未发现 dragon_tiger.db，请先同步龙虎榜数据。".to_string());
    }

    let conn = Connection::open(&db_path).map_err(|error| {
        format!(
            "打开龙虎榜数据库失败: path={}, err={error}",
            db_path.display()
        )
    })?;

    let summary = conn
        .query_row(
            r#"
            SELECT
                COUNT(*),
                COUNT(DISTINCT trade_date),
                COUNT(DISTINCT ts_code),
                COUNT(*) FILTER (WHERE side = '0'),
                COUNT(*) FILTER (WHERE side = '1'),
                SUM(CASE WHEN side = '0' THEN COALESCE(buy, 0) ELSE 0 END),
                SUM(CASE WHEN side = '1' THEN COALESCE(sell, 0) ELSE 0 END)
            FROM top_inst
            WHERE exalter = ?
            "#,
            [&trimmed_exalter],
            |row| {
                let appearance_count: i64 = row.get(0)?;
                let trade_date_count: i64 = row.get(1)?;
                let stock_count: i64 = row.get(2)?;
                let buy_count: i64 = row.get(3)?;
                let sell_count: i64 = row.get(4)?;
                let total_buy: Option<f64> = row.get(5)?;
                let total_sell: Option<f64> = row.get(6)?;
                let total_buy = total_buy.unwrap_or(0.0);
                let total_sell = total_sell.unwrap_or(0.0);
                Ok(DragonTigerSeatStatisticsSummary {
                    appearance_count: appearance_count.max(0) as usize,
                    trade_date_count: trade_date_count.max(0) as usize,
                    stock_count: stock_count.max(0) as usize,
                    buy_count: buy_count.max(0) as usize,
                    sell_count: sell_count.max(0) as usize,
                    total_buy,
                    total_sell,
                    total_net_buy: total_buy - total_sell,
                })
            },
        )
        .map_err(|error| format!("查询营业部/机构汇总统计失败: {error}"))?;

    let mut favorite_stmt = conn
        .prepare(
            r#"
            SELECT
                ti.ts_code,
                COALESCE(MAX(tl.name), ti.ts_code),
                COUNT(DISTINCT ti.trade_date || '|' || ti.reason) AS appearance_count,
                SUM(CASE WHEN ti.side = '0' THEN COALESCE(ti.buy, 0) ELSE 0 END) AS total_buy,
                SUM(CASE WHEN ti.side = '1' THEN COALESCE(ti.sell, 0) ELSE 0 END) AS total_sell
            FROM top_inst ti
            LEFT JOIN top_list tl
              ON tl.trade_date = ti.trade_date
             AND tl.ts_code = ti.ts_code
             AND tl.reason = ti.reason
            WHERE ti.exalter = ?
            GROUP BY ti.ts_code
            ORDER BY appearance_count DESC, total_buy - total_sell DESC, ti.ts_code
            LIMIT 10
            "#,
        )
        .map_err(|error| format!("预编译营业部/机构偏好股票查询失败: {error}"))?;
    let favorite_rows = favorite_stmt
        .query_map([&trimmed_exalter], |row| {
            let appearance_count: i64 = row.get(2)?;
            let total_buy: f64 = row.get(3)?;
            let total_sell: f64 = row.get(4)?;
            Ok(DragonTigerSeatFavoriteStock {
                ts_code: row.get(0)?,
                name: row.get(1)?,
                appearance_count: appearance_count.max(0) as usize,
                total_buy,
                total_sell,
                total_net_buy: total_buy - total_sell,
            })
        })
        .map_err(|error| format!("查询营业部/机构偏好股票失败: {error}"))?;
    let favorite_stocks = favorite_rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("读取营业部/机构偏好股票失败: {error}"))?;

    let mut recent_stmt = conn
        .prepare(
            r#"
            SELECT
                ti.trade_date,
                ti.ts_code,
                COALESCE(tl.name, ti.ts_code),
                ti.buy,
                ti.sell,
                ti.net_buy,
                ti.side,
                ti.reason
            FROM top_inst ti
            LEFT JOIN top_list tl
              ON tl.trade_date = ti.trade_date
             AND tl.ts_code = ti.ts_code
             AND tl.reason = ti.reason
            WHERE ti.exalter = ?
            ORDER BY ti.trade_date DESC, ti.net_buy DESC NULLS LAST, ti.ts_code, ti.side
            LIMIT 200
            "#,
        )
        .map_err(|error| format!("预编译营业部/机构近期记录查询失败: {error}"))?;
    let recent_rows = recent_stmt
        .query_map([&trimmed_exalter], |row| {
            Ok(DragonTigerSeatStatisticsRow {
                trade_date: row.get(0)?,
                ts_code: row.get(1)?,
                name: row.get(2)?,
                buy: row.get(3)?,
                sell: row.get(4)?,
                net_buy: row.get(5)?,
                side: row.get(6)?,
                reason: row.get(7)?,
            })
        })
        .map_err(|error| format!("查询营业部/机构近期记录失败: {error}"))?;
    let recent_records = recent_rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("读取营业部/机构近期记录失败: {error}"))?;

    Ok(DragonTigerSeatStatisticsData {
        exalter: trimmed_exalter,
        summary,
        favorite_stocks,
        recent_records,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use crate::{
        data::dragon_tiger_data::{open_dragon_tiger_db, replace_dragon_tiger_trade_date},
        download::{TopInstRow, TopListRow},
    };

    use super::*;

    #[test]
    fn market_data_resolves_latest_date_not_after_reference() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let source_dir = std::env::temp_dir().join(format!("lianghua_dragon_market_{nanos}"));
        let source_path = source_dir.to_string_lossy().into_owned();
        let mut conn = open_dragon_tiger_db(&source_path).expect("open db");

        for trade_date in ["20260723", "20260724"] {
            replace_dragon_tiger_trade_date(
                &mut conn,
                trade_date,
                &[TopListRow {
                    trade_date: trade_date.to_string(),
                    ts_code: "000011.SZ".to_string(),
                    name: "深物业A".to_string(),
                    close: Some(8.26),
                    pct_change: Some(9.98),
                    turnover_rate: Some(4.76),
                    amount: Some(1000.0),
                    l_sell: Some(200.0),
                    l_buy: Some(500.0),
                    l_amount: Some(700.0),
                    net_amount: Some(300.0),
                    net_rate: Some(30.0),
                    amount_rate: Some(70.0),
                    float_values: Some(10_000.0),
                    reason: "测试上榜理由".to_string(),
                }],
                &[
                    TopInstRow {
                        trade_date: trade_date.to_string(),
                        ts_code: "000011.SZ".to_string(),
                        exalter: "测试营业部".to_string(),
                        buy: Some(500.0),
                        buy_rate: Some(50.0),
                        sell: Some(200.0),
                        sell_rate: Some(20.0),
                        net_buy: Some(300.0),
                        side: "0".to_string(),
                        reason: "测试上榜理由".to_string(),
                    },
                    TopInstRow {
                        trade_date: trade_date.to_string(),
                        ts_code: "000011.SZ".to_string(),
                        exalter: "测试营业部".to_string(),
                        buy: Some(100.0),
                        buy_rate: Some(10.0),
                        sell: Some(200.0),
                        sell_rate: Some(20.0),
                        net_buy: Some(-100.0),
                        side: "1".to_string(),
                        reason: "测试上榜理由".to_string(),
                    },
                ],
            )
            .expect("write date");
        }
        drop(conn);

        let data =
            get_dragon_tiger_market_data(source_path.clone(), Some("2026-07-23".to_string()))
                .expect("query market data");
        assert!(data.db_exists);
        assert_eq!(data.latest_sync_trade_date.as_deref(), Some("20260724"));
        assert_eq!(data.resolved_trade_date.as_deref(), Some("20260723"));
        assert_eq!(data.summary.stock_count, 1);
        assert_eq!(data.summary.top_list_rows, 1);
        assert_eq!(data.summary.top_inst_rows, 2);
        assert_eq!(data.summary.total_net_amount, 300.0);

        let detail = get_dragon_tiger_stock_detail(
            source_path.clone(),
            "000011.SZ".to_string(),
            "2026-07-24".to_string(),
        )
        .expect("query stock detail");
        assert_eq!(detail.name, "深物业A");
        assert_eq!(detail.resolved_trade_date.as_deref(), Some("20260724"));
        assert_eq!(detail.current_list.len(), 1);
        assert_eq!(detail.seats.len(), 2);
        assert_eq!(detail.history.len(), 1);
        assert_eq!(detail.history_trade_count, 1);
        assert_eq!(detail.history_record_count, 1);

        let seat_statistics =
            get_dragon_tiger_seat_statistics(source_path.clone(), "测试营业部".to_string())
                .expect("query seat statistics");
        assert_eq!(seat_statistics.summary.appearance_count, 4);
        assert_eq!(seat_statistics.summary.trade_date_count, 2);
        assert_eq!(seat_statistics.summary.stock_count, 1);
        assert_eq!(seat_statistics.summary.buy_count, 2);
        assert_eq!(seat_statistics.summary.sell_count, 2);
        assert_eq!(seat_statistics.summary.total_buy, 1000.0);
        assert_eq!(seat_statistics.summary.total_sell, 400.0);
        assert_eq!(seat_statistics.summary.total_net_buy, 600.0);
        assert_eq!(seat_statistics.favorite_stocks.len(), 1);
        assert_eq!(seat_statistics.favorite_stocks[0].appearance_count, 2);
        assert_eq!(seat_statistics.recent_records.len(), 4);

        fs::remove_dir_all(source_dir).ok();
    }
}
