//! Trade-date normalization and score-result date resolution.

use duckdb::Connection;

pub fn normalize_trade_date(raw: &str) -> Option<String> {
    let digits: String = raw
        .trim()
        .chars()
        .filter(|ch| ch.is_ascii_digit())
        .collect();
    (digits.len() == 8).then_some(digits)
}

pub fn resolve_trade_date(conn: &Connection, trade_date: Option<String>) -> Result<String, String> {
    if let Some(date) = trade_date {
        let date = date.trim().to_string();
        if !date.is_empty() {
            return Ok(date);
        }
    }

    let mut stmt = conn
        .prepare("SELECT MAX(trade_date) FROM score_summary")
        .map_err(|error| format!("查询最新交易日预编译失败: {error}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|error| format!("查询最新交易日失败: {error}"))?;

    if let Some(row) = rows
        .next()
        .map_err(|error| format!("读取最新交易日结果失败: {error}"))?
    {
        let date: Option<String> = row
            .get(0)
            .map_err(|error| format!("读取最新交易日字段失败: {error}"))?;
        if let Some(date) = date.filter(|value| !value.trim().is_empty()) {
            return Ok(date);
        }
    }

    Err("score_summary 没有可用交易日".to_string())
}
