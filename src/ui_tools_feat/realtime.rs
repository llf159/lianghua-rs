use std::{collections::HashMap, time::Duration};

use reqwest::blocking::Client;
use serde::Serialize;

use crate::{
    crawler::{
        SinaQuote, TencentQuote, fetch_sina_quotes, fetch_sina_quotes_async,
        fetch_sina_quotes_parallel, fetch_tencent_quotes, fetch_tencent_quotes_async,
        fetch_tencent_quotes_parallel,
    },
    data::load_stock_list,
};

pub const REALTIME_BATCH_CAP: usize = 50;
pub const TENCENT_REALTIME_BATCH_CAP: usize = 60;
const REALTIME_CONNECT_TIMEOUT_SECS: u64 = 6;
const REALTIME_REQUEST_TIMEOUT_SECS: u64 = 12;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RealtimeFetchMeta {
    pub requested_count: usize,
    pub effective_count: usize,
    pub fetched_count: usize,
    pub truncated: bool,
    pub refreshed_at: Option<String>,
    pub quote_trade_date: Option<String>,
    pub quote_time: Option<String>,
}

pub fn normalize_quote_trade_date(raw: &str) -> Option<String> {
    let digits: String = raw.chars().filter(|ch| ch.is_ascii_digit()).collect();
    if digits.len() == 8 {
        Some(digits)
    } else {
        None
    }
}

pub fn normalize_quote_time(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    let parts = trimmed.split(':').collect::<Vec<_>>();
    if parts.len() == 2 || parts.len() == 3 {
        let hour = parts[0];
        let minute = parts[1];
        let second = if parts.len() == 3 { parts[2] } else { "00" };
        if [hour, minute, second]
            .iter()
            .all(|part| part.len() == 2 && part.chars().all(|ch| ch.is_ascii_digit()))
        {
            return Some(format!("{hour}:{minute}:{second}"));
        }
    }

    let digits: String = trimmed.chars().filter(|ch| ch.is_ascii_digit()).collect();
    if digits.len() == 6 {
        return Some(format!(
            "{}:{}:{}",
            &digits[0..2],
            &digits[2..4],
            &digits[4..6]
        ));
    }
    if digits.len() == 4 {
        return Some(format!("{}:{}:00", &digits[0..2], &digits[2..4]));
    }

    None
}

pub fn build_refreshed_at(date: &str, time: &str) -> Option<String> {
    let date = normalize_quote_trade_date(date)?;
    match normalize_quote_time(time) {
        Some(time) => Some(format!("{date} {time}")),
        None => Some(date),
    }
}

fn build_realtime_fetch_meta(
    ts_codes: &[String],
    quote_map: &HashMap<String, SinaQuote>,
) -> RealtimeFetchMeta {
    let quotes: Vec<&SinaQuote> = quote_map.values().collect();
    let refreshed_at = quotes
        .iter()
        .find_map(|quote| build_refreshed_at(&quote.date, &quote.time));
    let quote_trade_date = quotes
        .iter()
        .find_map(|quote| normalize_quote_trade_date(&quote.date));
    let quote_time = quotes
        .iter()
        .find_map(|quote| normalize_quote_time(&quote.time));

    RealtimeFetchMeta {
        requested_count: ts_codes.len(),
        effective_count: ts_codes.len(),
        fetched_count: quote_map.len(),
        truncated: false,
        refreshed_at,
        quote_trade_date,
        quote_time,
    }
}

fn build_tencent_realtime_fetch_meta(
    ts_codes: &[String],
    quote_map: &HashMap<String, TencentQuote>,
) -> RealtimeFetchMeta {
    let quotes: Vec<&TencentQuote> = quote_map.values().collect();
    let refreshed_at = quotes
        .iter()
        .find_map(|quote| build_refreshed_at(&quote.date, &quote.time));
    let quote_trade_date = quotes
        .iter()
        .find_map(|quote| normalize_quote_trade_date(&quote.date));
    let quote_time = quotes
        .iter()
        .find_map(|quote| normalize_quote_time(&quote.time));

    RealtimeFetchMeta {
        requested_count: ts_codes.len(),
        effective_count: ts_codes.len(),
        fetched_count: quote_map.len(),
        truncated: false,
        refreshed_at,
        quote_trade_date,
        quote_time,
    }
}

fn build_realtime_quote_map_result(
    ts_codes: &[String],
    mut quotes: Vec<SinaQuote>,
) -> (HashMap<String, SinaQuote>, Vec<String>) {
    let mut quote_map = HashMap::with_capacity(quotes.len());
    for quote in quotes.drain(..) {
        quote_map.insert(quote.ts_code.clone(), quote);
    }

    let missing_codes = if quote_map.len() < ts_codes.len() {
        ts_codes
            .iter()
            .filter(|ts_code| !quote_map.contains_key(ts_code.as_str()))
            .cloned()
            .collect()
    } else {
        Vec::new()
    };

    (quote_map, missing_codes)
}

fn build_tencent_realtime_quote_map_result(
    ts_codes: &[String],
    mut quotes: Vec<TencentQuote>,
) -> (HashMap<String, TencentQuote>, Vec<String>) {
    let mut quote_map = HashMap::with_capacity(quotes.len());
    for quote in quotes.drain(..) {
        quote_map.insert(quote.ts_code.clone(), quote);
    }

    let missing_codes = if quote_map.len() < ts_codes.len() {
        ts_codes
            .iter()
            .filter(|ts_code| !quote_map.contains_key(ts_code.as_str()))
            .cloned()
            .collect()
    } else {
        Vec::new()
    };

    (quote_map, missing_codes)
}

fn load_all_market_ts_codes(source_path: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    for row in load_stock_list(source_path)? {
        let Some(ts_code) = row.first().map(|value| value.trim()) else {
            continue;
        };
        if !ts_code.is_empty() {
            out.push(ts_code.to_string());
        }
    }
    Ok(out)
}

pub fn fetch_realtime_quote_map(
    ts_codes: &[String],
) -> Result<(HashMap<String, SinaQuote>, RealtimeFetchMeta), String> {
    let requested_count = ts_codes.len();
    let effective_count = requested_count;
    let truncated = false;

    if effective_count == 0 {
        return Ok((
            HashMap::new(),
            RealtimeFetchMeta {
                requested_count,
                effective_count,
                fetched_count: 0,
                truncated,
                refreshed_at: None,
                quote_trade_date: None,
                quote_time: None,
            },
        ));
    }

    let http = Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_secs(REALTIME_CONNECT_TIMEOUT_SECS))
        .timeout(Duration::from_secs(REALTIME_REQUEST_TIMEOUT_SECS))
        .build()
        .map_err(|e| format!("创建实时行情客户端失败: {e}"))?;
    let quotes = fetch_sina_quotes(&http, ts_codes, REALTIME_BATCH_CAP)?;
    let (mut quote_map, missing_codes) = build_realtime_quote_map_result(ts_codes, quotes);

    for ts_code in &missing_codes {
        let retry_quotes = fetch_sina_quotes(&http, std::slice::from_ref(ts_code), 1)?;
        for quote in retry_quotes {
            quote_map.insert(quote.ts_code.clone(), quote);
        }
    }
    let fetch_meta = build_realtime_fetch_meta(ts_codes, &quote_map);

    Ok((quote_map, fetch_meta))
}

pub fn fetch_tencent_realtime_quote_map(
    ts_codes: &[String],
) -> Result<(HashMap<String, TencentQuote>, RealtimeFetchMeta), String> {
    let requested_count = ts_codes.len();
    let effective_count = requested_count;
    let truncated = false;

    if effective_count == 0 {
        return Ok((
            HashMap::new(),
            RealtimeFetchMeta {
                requested_count,
                effective_count,
                fetched_count: 0,
                truncated,
                refreshed_at: None,
                quote_trade_date: None,
                quote_time: None,
            },
        ));
    }

    let http = Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_secs(REALTIME_CONNECT_TIMEOUT_SECS))
        .timeout(Duration::from_secs(REALTIME_REQUEST_TIMEOUT_SECS))
        .build()
        .map_err(|e| format!("创建腾讯实时行情客户端失败: {e}"))?;
    let quotes = fetch_tencent_quotes(&http, ts_codes, TENCENT_REALTIME_BATCH_CAP)?;
    let (mut quote_map, missing_codes) = build_tencent_realtime_quote_map_result(ts_codes, quotes);

    for ts_code in &missing_codes {
        let retry_quotes = fetch_tencent_quotes(&http, std::slice::from_ref(ts_code), 1)?;
        for quote in retry_quotes {
            quote_map.insert(quote.ts_code.clone(), quote);
        }
    }
    let fetch_meta = build_tencent_realtime_fetch_meta(ts_codes, &quote_map);

    Ok((quote_map, fetch_meta))
}

pub fn fetch_all_market_realtime_quote_map(
    source_path: &str,
) -> Result<(HashMap<String, SinaQuote>, RealtimeFetchMeta), String> {
    let ts_codes = load_all_market_ts_codes(source_path)?;
    fetch_all_market_realtime_quote_map_for_codes(&ts_codes)
}

pub fn fetch_all_market_realtime_quote_map_for_codes(
    ts_codes: &[String],
) -> Result<(HashMap<String, SinaQuote>, RealtimeFetchMeta), String> {
    let requested_count = ts_codes.len();
    let effective_count = requested_count;
    let truncated = false;

    if effective_count == 0 {
        return Ok((
            HashMap::new(),
            RealtimeFetchMeta {
                requested_count,
                effective_count,
                fetched_count: 0,
                truncated,
                refreshed_at: None,
                quote_trade_date: None,
                quote_time: None,
            },
        ));
    }

    let http = Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_secs(REALTIME_CONNECT_TIMEOUT_SECS))
        .timeout(Duration::from_secs(REALTIME_REQUEST_TIMEOUT_SECS))
        .build()
        .map_err(|e| format!("创建全市场实时行情客户端失败: {e}"))?;
    let quotes = fetch_sina_quotes_parallel(&http, ts_codes, REALTIME_BATCH_CAP)?;
    let (mut quote_map, missing_codes) = build_realtime_quote_map_result(ts_codes, quotes);

    for ts_code in &missing_codes {
        let retry_quotes = fetch_sina_quotes(&http, std::slice::from_ref(ts_code), 1)?;
        for quote in retry_quotes {
            quote_map.insert(quote.ts_code.clone(), quote);
        }
    }
    let fetch_meta = build_realtime_fetch_meta(&ts_codes, &quote_map);

    Ok((quote_map, fetch_meta))
}

pub fn fetch_all_market_tencent_realtime_quote_map(
    source_path: &str,
) -> Result<(HashMap<String, TencentQuote>, RealtimeFetchMeta), String> {
    let ts_codes = load_all_market_ts_codes(source_path)?;
    fetch_all_market_tencent_realtime_quote_map_for_codes(&ts_codes)
}

pub fn fetch_all_market_tencent_realtime_quote_map_for_codes(
    ts_codes: &[String],
) -> Result<(HashMap<String, TencentQuote>, RealtimeFetchMeta), String> {
    let requested_count = ts_codes.len();
    let effective_count = requested_count;
    let truncated = false;

    if effective_count == 0 {
        return Ok((
            HashMap::new(),
            RealtimeFetchMeta {
                requested_count,
                effective_count,
                fetched_count: 0,
                truncated,
                refreshed_at: None,
                quote_trade_date: None,
                quote_time: None,
            },
        ));
    }

    let http = Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_secs(REALTIME_CONNECT_TIMEOUT_SECS))
        .timeout(Duration::from_secs(REALTIME_REQUEST_TIMEOUT_SECS))
        .build()
        .map_err(|e| format!("创建腾讯全市场实时行情客户端失败: {e}"))?;
    let quotes = fetch_tencent_quotes_parallel(&http, ts_codes, TENCENT_REALTIME_BATCH_CAP)?;
    let (mut quote_map, missing_codes) = build_tencent_realtime_quote_map_result(ts_codes, quotes);

    for ts_code in &missing_codes {
        let retry_quotes = fetch_tencent_quotes(&http, std::slice::from_ref(ts_code), 1)?;
        for quote in retry_quotes {
            quote_map.insert(quote.ts_code.clone(), quote);
        }
    }
    let fetch_meta = build_tencent_realtime_fetch_meta(ts_codes, &quote_map);

    Ok((quote_map, fetch_meta))
}

pub async fn fetch_realtime_quote_map_async(
    ts_codes: &[String],
) -> Result<(HashMap<String, SinaQuote>, RealtimeFetchMeta), String> {
    let requested_count = ts_codes.len();
    let effective_count = requested_count;
    let truncated = false;

    if effective_count == 0 {
        return Ok((
            HashMap::new(),
            RealtimeFetchMeta {
                requested_count,
                effective_count,
                fetched_count: 0,
                truncated,
                refreshed_at: None,
                quote_trade_date: None,
                quote_time: None,
            },
        ));
    }

    let http = reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_secs(REALTIME_CONNECT_TIMEOUT_SECS))
        .timeout(Duration::from_secs(REALTIME_REQUEST_TIMEOUT_SECS))
        .build()
        .map_err(|e| format!("创建实时行情客户端失败: {e}"))?;
    let quotes = fetch_sina_quotes_async(&http, ts_codes, REALTIME_BATCH_CAP).await?;
    let (mut quote_map, missing_codes) = build_realtime_quote_map_result(ts_codes, quotes);

    for ts_code in &missing_codes {
        let retry_quotes = fetch_sina_quotes_async(&http, std::slice::from_ref(ts_code), 1).await?;
        for quote in retry_quotes {
            quote_map.insert(quote.ts_code.clone(), quote);
        }
    }
    let fetch_meta = build_realtime_fetch_meta(ts_codes, &quote_map);

    Ok((quote_map, fetch_meta))
}

pub async fn fetch_tencent_realtime_quote_map_async(
    ts_codes: &[String],
) -> Result<(HashMap<String, TencentQuote>, RealtimeFetchMeta), String> {
    let requested_count = ts_codes.len();
    let effective_count = requested_count;
    let truncated = false;

    if effective_count == 0 {
        return Ok((
            HashMap::new(),
            RealtimeFetchMeta {
                requested_count,
                effective_count,
                fetched_count: 0,
                truncated,
                refreshed_at: None,
                quote_trade_date: None,
                quote_time: None,
            },
        ));
    }

    let http = reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_secs(REALTIME_CONNECT_TIMEOUT_SECS))
        .timeout(Duration::from_secs(REALTIME_REQUEST_TIMEOUT_SECS))
        .build()
        .map_err(|e| format!("创建腾讯实时行情客户端失败: {e}"))?;
    let quotes = fetch_tencent_quotes_async(&http, ts_codes, TENCENT_REALTIME_BATCH_CAP).await?;
    let (mut quote_map, missing_codes) = build_tencent_realtime_quote_map_result(ts_codes, quotes);

    for ts_code in &missing_codes {
        let retry_quotes =
            fetch_tencent_quotes_async(&http, std::slice::from_ref(ts_code), 1).await?;
        for quote in retry_quotes {
            quote_map.insert(quote.ts_code.clone(), quote);
        }
    }
    let fetch_meta = build_tencent_realtime_fetch_meta(ts_codes, &quote_map);

    Ok((quote_map, fetch_meta))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_quote(ts_code: &str, date: &str, time: &str) -> SinaQuote {
        SinaQuote {
            date: date.to_string(),
            time: time.to_string(),
            ts_code: ts_code.to_string(),
            name: String::new(),
            open: 10.0,
            high: 10.5,
            low: 9.8,
            pre_close: 9.9,
            price: 10.2,
            vol: 1000.0,
            amount: 10000.0,
            change_pct: Some(3.03),
        }
    }

    fn sample_tencent_quote(ts_code: &str, date: &str, time: &str) -> TencentQuote {
        TencentQuote {
            date: date.to_string(),
            time: time.to_string(),
            ts_code: ts_code.to_string(),
            name: String::new(),
            open: 10.0,
            high: 10.5,
            low: 9.8,
            pre_close: 9.9,
            price: 10.2,
            vol: 1000.0,
            amount: 10000.0,
            change_pct: Some(3.03),
            volume_ratio: Some(1.2),
            avg_price: Some(10.1),
        }
    }

    #[test]
    fn normalize_quote_time_accepts_common_sina_formats() {
        assert_eq!(
            normalize_quote_time("09:31:05").as_deref(),
            Some("09:31:05")
        );
        assert_eq!(normalize_quote_time("09:31").as_deref(), Some("09:31:00"));
        assert_eq!(normalize_quote_time("093105").as_deref(), Some("09:31:05"));
        assert_eq!(normalize_quote_time("0931").as_deref(), Some("09:31:00"));
        assert_eq!(normalize_quote_time("").as_deref(), None);
    }

    #[test]
    fn realtime_fetch_meta_uses_available_quote_time() {
        let ts_codes = vec!["000001.SZ".to_string(), "000002.SZ".to_string()];
        let mut quote_map = HashMap::new();
        quote_map.insert("000001.SZ".to_string(), sample_quote("000001.SZ", "", ""));
        quote_map.insert(
            "000002.SZ".to_string(),
            sample_quote("000002.SZ", "2024-06-03", "093105"),
        );

        let meta = build_realtime_fetch_meta(&ts_codes, &quote_map);

        assert_eq!(meta.refreshed_at.as_deref(), Some("20240603 09:31:05"));
        assert_eq!(meta.quote_trade_date.as_deref(), Some("20240603"));
        assert_eq!(meta.quote_time.as_deref(), Some("09:31:05"));
    }

    #[test]
    fn tencent_realtime_fetch_meta_uses_available_quote_time() {
        let ts_codes = vec!["000001.SZ".to_string(), "000002.SZ".to_string()];
        let mut quote_map = HashMap::new();
        quote_map.insert(
            "000001.SZ".to_string(),
            sample_tencent_quote("000001.SZ", "", ""),
        );
        quote_map.insert(
            "000002.SZ".to_string(),
            sample_tencent_quote("000002.SZ", "20240603", "093105"),
        );

        let meta = build_tencent_realtime_fetch_meta(&ts_codes, &quote_map);

        assert_eq!(meta.refreshed_at.as_deref(), Some("20240603 09:31:05"));
        assert_eq!(meta.quote_trade_date.as_deref(), Some("20240603"));
        assert_eq!(meta.quote_time.as_deref(), Some("09:31:05"));
    }

    #[test]
    fn tencent_realtime_quote_map_result_reports_missing_codes() {
        let ts_codes = vec!["000001.SZ".to_string(), "000002.SZ".to_string()];
        let quotes = vec![sample_tencent_quote("000001.SZ", "20240603", "09:31:05")];

        let (quote_map, missing_codes) = build_tencent_realtime_quote_map_result(&ts_codes, quotes);

        assert!(quote_map.contains_key("000001.SZ"));
        assert_eq!(missing_codes, vec!["000002.SZ"]);
    }
}
