use std::{collections::HashMap, time::Duration};

use reqwest::blocking::Client;
use serde::Serialize;

use crate::{
    crawler::{SinaQuote, fetch_sina_quotes, fetch_sina_quotes_async, fetch_sina_quotes_parallel},
    data::load_stock_list,
};

pub const REALTIME_BATCH_CAP: usize = 50;
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

pub fn build_refreshed_at(date: &str, time: &str) -> Option<String> {
    let date = normalize_quote_trade_date(date)?;
    let time = time.trim();
    if time.is_empty() {
        return Some(date);
    }
    Some(format!("{date} {time}"))
}

fn build_realtime_fetch_meta(
    ts_codes: &[String],
    quote_map: &HashMap<String, SinaQuote>,
) -> RealtimeFetchMeta {
    let quotes: Vec<&SinaQuote> = quote_map.values().collect();
    let refreshed_at = quotes
        .first()
        .and_then(|quote| build_refreshed_at(&quote.date, &quote.time));
    let quote_trade_date = quotes
        .first()
        .and_then(|quote| normalize_quote_trade_date(&quote.date));
    let quote_time = quotes.first().map(|quote| quote.time.clone());

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
