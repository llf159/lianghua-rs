use std::{collections::HashMap, time::Duration};

use reqwest::blocking::Client;
use serde::Serialize;

use crate::crawler::{SinaQuote, fetch_sina_quotes, fetch_sina_quotes_async};

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
        .connect_timeout(Duration::from_secs(REALTIME_CONNECT_TIMEOUT_SECS))
        .timeout(Duration::from_secs(REALTIME_REQUEST_TIMEOUT_SECS))
        .build()
        .map_err(|e| format!("创建实时行情客户端失败: {e}"))?;
    let mut quotes = fetch_sina_quotes(&http, ts_codes, REALTIME_BATCH_CAP)?;
    let mut quote_map = HashMap::with_capacity(quotes.len());
    for quote in quotes.drain(..) {
        quote_map.insert(quote.ts_code.clone(), quote);
    }

    if quote_map.len() < effective_count {
        let missing_codes: Vec<String> = ts_codes
            .iter()
            .filter(|ts_code| !quote_map.contains_key(ts_code.as_str()))
            .cloned()
            .collect();

        for ts_code in &missing_codes {
            let retry_quotes = fetch_sina_quotes(&http, std::slice::from_ref(ts_code), 1)?;
            for quote in retry_quotes {
                quote_map.insert(quote.ts_code.clone(), quote);
            }
        }
    }

    let quotes: Vec<&SinaQuote> = quote_map.values().collect();
    let refreshed_at = quotes
        .first()
        .and_then(|quote| build_refreshed_at(&quote.date, &quote.time));
    let quote_trade_date = quotes
        .first()
        .and_then(|quote| normalize_quote_trade_date(&quote.date));
    let quote_time = quotes.first().map(|quote| quote.time.clone());
    let fetched_count = quote_map.len();

    Ok((
        quote_map,
        RealtimeFetchMeta {
            requested_count,
            effective_count,
            fetched_count,
            truncated,
            refreshed_at,
            quote_trade_date,
            quote_time,
        },
    ))
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
        .connect_timeout(Duration::from_secs(REALTIME_CONNECT_TIMEOUT_SECS))
        .timeout(Duration::from_secs(REALTIME_REQUEST_TIMEOUT_SECS))
        .build()
        .map_err(|e| format!("创建实时行情客户端失败: {e}"))?;
    let mut quotes = fetch_sina_quotes_async(&http, ts_codes, REALTIME_BATCH_CAP).await?;
    let mut quote_map = HashMap::with_capacity(quotes.len());
    for quote in quotes.drain(..) {
        quote_map.insert(quote.ts_code.clone(), quote);
    }

    if quote_map.len() < effective_count {
        let missing_codes: Vec<String> = ts_codes
            .iter()
            .filter(|ts_code| !quote_map.contains_key(ts_code.as_str()))
            .cloned()
            .collect();

        for ts_code in &missing_codes {
            let retry_quotes =
                fetch_sina_quotes_async(&http, std::slice::from_ref(ts_code), 1).await?;
            for quote in retry_quotes {
                quote_map.insert(quote.ts_code.clone(), quote);
            }
        }
    }

    let quotes: Vec<&SinaQuote> = quote_map.values().collect();
    let refreshed_at = quotes
        .first()
        .and_then(|quote| build_refreshed_at(&quote.date, &quote.time));
    let quote_trade_date = quotes
        .first()
        .and_then(|quote| normalize_quote_trade_date(&quote.date));
    let quote_time = quotes.first().map(|quote| quote.time.clone());
    let fetched_count = quote_map.len();

    Ok((
        quote_map,
        RealtimeFetchMeta {
            requested_count,
            effective_count,
            fetched_count,
            truncated,
            refreshed_at,
            quote_trade_date,
            quote_time,
        },
    ))
}
