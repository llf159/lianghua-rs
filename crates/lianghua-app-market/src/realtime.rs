use std::{
    collections::{HashMap, HashSet},
    sync::{
        OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use rayon::prelude::*;

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

fn build_latest_quote_time_meta<'a>(
    quotes: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> (Option<String>, Option<String>, Option<String>) {
    let latest = quotes
        .into_iter()
        .filter_map(|(date, time)| {
            let date = normalize_quote_trade_date(date)?;
            let time = normalize_quote_time(time);
            Some((date, time))
        })
        .max_by(|(left_date, left_time), (right_date, right_time)| {
            left_date.cmp(right_date).then_with(|| {
                left_time
                    .as_deref()
                    .unwrap_or("00:00:00")
                    .cmp(right_time.as_deref().unwrap_or("00:00:00"))
            })
        });

    match latest {
        Some((date, time)) => {
            let refreshed_at = match &time {
                Some(time) => format!("{date} {time}"),
                None => date.clone(),
            };
            (Some(refreshed_at), Some(date), time)
        }
        None => (None, None, None),
    }
}

fn build_realtime_fetch_meta(
    ts_codes: &[String],
    quote_map: &HashMap<String, SinaQuote>,
) -> RealtimeFetchMeta {
    let (refreshed_at, quote_trade_date, quote_time) = build_latest_quote_time_meta(
        quote_map
            .values()
            .map(|quote| (quote.date.as_str(), quote.time.as_str())),
    );

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
    let (refreshed_at, quote_trade_date, quote_time) = build_latest_quote_time_meta(
        quote_map
            .values()
            .map(|quote| (quote.date.as_str(), quote.time.as_str())),
    );

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
    if quote_map.is_empty() {
        return Err("腾讯全市场行情返回空数据，已停止刷新并保留页面上一轮行情".to_string());
    }
    let fetch_meta = build_tencent_realtime_fetch_meta(ts_codes, &quote_map);

    Ok((quote_map, fetch_meta))
}

pub fn fetch_realtime_quote_map_with_provider(
    ts_codes: &[String],
    provider: Option<&str>,
) -> Result<(HashMap<String, SinaQuote>, RealtimeFetchMeta), String> {
    match provider
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_else(|| "tencent".to_string())
        .as_str()
    {
        "" | "tencent" | "qq" | "gtimg" => {
            let (quote_map, fetch_meta) = fetch_tencent_realtime_quote_map(ts_codes)?;
            Ok((
                quote_map
                    .into_iter()
                    .map(|(ts_code, quote)| (ts_code, quote.into_sina_quote()))
                    .collect(),
                fetch_meta,
            ))
        }
        "sina" | "sinajs" => fetch_realtime_quote_map(ts_codes),
        _ => Err("实时行情源仅支持 sina 或 tencent".to_string()),
    }
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
    if quote_map.is_empty() {
        return Err("新浪全市场行情返回空数据，已停止刷新并保留页面上一轮行情".to_string());
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

// Both parsers already normalize stock volume to hands and amount to yuan.
fn sina_monitor_quote(quote: SinaQuote) -> TencentQuote {
    let avg_price = (quote.vol.is_finite()
        && quote.vol > 0.0
        && quote.amount.is_finite()
        && quote.amount > 0.0)
        .then(|| quote.amount / (quote.vol * 100.0))
        .filter(|value| value.is_finite() && *value > 0.0);
    TencentQuote {
        date: quote.date,
        time: quote.time,
        ts_code: quote.ts_code,
        name: quote.name,
        open: quote.open,
        high: quote.high,
        low: quote.low,
        pre_close: quote.pre_close,
        price: quote.price,
        vol: quote.vol,
        amount: quote.amount,
        change_pct: quote.change_pct,
        volume_ratio: None,
        avg_price,
    }
}

fn fetch_monitor_batch(
    http: &Client,
    codes: &[String],
    tencent: bool,
) -> Result<Vec<TencentQuote>, String> {
    if tencent {
        fetch_tencent_quotes(http, codes, TENCENT_REALTIME_BATCH_CAP)
    } else {
        fetch_sina_quotes(http, codes, REALTIME_BATCH_CAP)
            .map(|quotes| quotes.into_iter().map(sina_monitor_quote).collect())
    }
}

pub fn fetch_all_market_mixed_realtime_quote_map_for_codes(
    ts_codes: &[String],
) -> Result<(HashMap<String, TencentQuote>, RealtimeFetchMeta), String> {
    static NETWORK: OnceLock<Result<(rayon::ThreadPool, Client), String>> = OnceLock::new();
    let (pool, http) = NETWORK
        .get_or_init(|| {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(16)
                .thread_name(|index| format!("monitor-quote-{index}"))
                .build()
                .map_err(|error| format!("创建双源行情线程池失败: {error}"))?;
            let http = Client::builder()
                .no_proxy()
                .connect_timeout(Duration::from_secs(REALTIME_CONNECT_TIMEOUT_SECS))
                .timeout(Duration::from_secs(REALTIME_REQUEST_TIMEOUT_SECS))
                .build()
                .map_err(|error| format!("创建双源行情客户端失败: {error}"))?;
            Ok((pool, http))
        })
        .as_ref()
        .map_err(Clone::clone)?;
    let failed_sources = [AtomicBool::new(false), AtomicBool::new(false)];
    let results = pool.install(|| {
        ts_codes
            .par_chunks(REALTIME_BATCH_CAP)
            .enumerate()
            .map(|(index, codes)| {
                if failed_sources
                    .iter()
                    .all(|failed| failed.load(Ordering::Relaxed))
                {
                    return Err("两个行情源本轮均请求失败，保留上一轮行情".into());
                }
                let mut tencent = index % 2 == 0;
                if failed_sources[usize::from(tencent)].load(Ordering::Relaxed) {
                    tencent = !tencent;
                }
                let (mut quotes, primary_failed) = match fetch_monitor_batch(http, codes, tencent) {
                    Ok(quotes) => (quotes, false),
                    Err(_) => {
                        failed_sources[usize::from(tencent)].store(true, Ordering::Relaxed);
                        (Vec::new(), true)
                    }
                };
                let received: HashSet<_> =
                    quotes.iter().map(|quote| quote.ts_code.as_str()).collect();
                let missing = codes
                    .iter()
                    .filter(|code| !received.contains(code.as_str()))
                    .cloned()
                    .collect::<Vec<_>>();
                if !missing.is_empty()
                    && !failed_sources[usize::from(!tencent)].load(Ordering::Relaxed)
                {
                    match fetch_monitor_batch(http, &missing, !tencent) {
                        Ok(fallback) => quotes.extend(fallback),
                        Err(error) if primary_failed => {
                            return Err(format!("双源行情请求均失败: {error}"));
                        }
                        Err(_) => {
                            failed_sources[usize::from(!tencent)].store(true, Ordering::Relaxed);
                        }
                    }
                }
                if primary_failed && quotes.is_empty() {
                    return Err("两个行情源本轮均请求失败，保留上一轮行情".into());
                }
                Ok(quotes)
            })
            .collect::<Result<Vec<_>, String>>()
    })?;
    let (quote_map, _) =
        build_tencent_realtime_quote_map_result(ts_codes, results.into_iter().flatten().collect());
    if !ts_codes.is_empty() && quote_map.is_empty() {
        return Err("双源行情返回空数据，保留上一轮行情".into());
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
    fn sina_monitor_conversion_keeps_hand_units_and_calculates_average_price() {
        let mut quote = sample_quote("000001.SZ", "2026-11-02", "10:30:00");
        quote.vol = 1200.0;
        quote.amount = 1_200_000.0;
        let converted = super::sina_monitor_quote(quote.clone());
        assert_eq!(converted.vol, quote.vol);
        assert_eq!(converted.avg_price, Some(10.0));
        assert_eq!(converted.volume_ratio, None);
        assert_eq!(converted.date, quote.date);
        assert_eq!(
            crate::volume_ratio::current_volume_ratio(&quote, 4800.0, 1.0),
            Some(1.0)
        );
        quote.vol = 0.0;
        assert_eq!(super::sina_monitor_quote(quote).avg_price, None);
    }

    #[test]
    fn local_volume_ratio_rejects_invalid_input_and_non_trading_time() {
        let mut quote = sample_quote("000001.SZ", "2026-11-02", "13:30:00");
        quote.vol = 3000.0;
        assert_eq!(
            crate::volume_ratio::current_volume_ratio(&quote, 4800.0, 1.0),
            Some(1.0)
        );
        for time in ["09:29:59", "12:00:00", "15:00:01"] {
            quote.time = time.into();
            assert_eq!(
                crate::volume_ratio::current_volume_ratio(&quote, 4800.0, 1.0),
                None
            );
        }
        quote.time = "10:30:00".into();
        for average in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert_eq!(
                crate::volume_ratio::current_volume_ratio(&quote, average, 1.0),
                None
            );
        }
        quote.vol = f64::NAN;
        assert_eq!(
            crate::volume_ratio::current_volume_ratio(&quote, 4800.0, 1.0),
            None
        );
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
        let ts_codes = vec![
            "000001.SZ".to_string(),
            "000002.SZ".to_string(),
            "000003.SZ".to_string(),
        ];
        let mut quote_map = HashMap::new();
        quote_map.insert("000001.SZ".to_string(), sample_quote("000001.SZ", "", ""));
        quote_map.insert(
            "000002.SZ".to_string(),
            sample_quote("000002.SZ", "2024-06-03", "093105"),
        );
        quote_map.insert(
            "000003.SZ".to_string(),
            sample_quote("000003.SZ", "2024-06-03", "09:34:08"),
        );

        let meta = build_realtime_fetch_meta(&ts_codes, &quote_map);

        assert_eq!(meta.refreshed_at.as_deref(), Some("20240603 09:34:08"));
        assert_eq!(meta.quote_trade_date.as_deref(), Some("20240603"));
        assert_eq!(meta.quote_time.as_deref(), Some("09:34:08"));
    }

    #[test]
    fn tencent_realtime_fetch_meta_uses_available_quote_time() {
        let ts_codes = vec![
            "000001.SZ".to_string(),
            "000002.SZ".to_string(),
            "000003.SZ".to_string(),
        ];
        let mut quote_map = HashMap::new();
        quote_map.insert(
            "000001.SZ".to_string(),
            sample_tencent_quote("000001.SZ", "", ""),
        );
        quote_map.insert(
            "000002.SZ".to_string(),
            sample_tencent_quote("000002.SZ", "20240603", "093105"),
        );
        quote_map.insert(
            "000003.SZ".to_string(),
            sample_tencent_quote("000003.SZ", "20240603", "09:34:08"),
        );

        let meta = build_tencent_realtime_fetch_meta(&ts_codes, &quote_map);

        assert_eq!(meta.refreshed_at.as_deref(), Some("20240603 09:34:08"));
        assert_eq!(meta.quote_trade_date.as_deref(), Some("20240603"));
        assert_eq!(meta.quote_time.as_deref(), Some("09:34:08"));
    }

    #[test]
    fn latest_quote_time_meta_prefers_latest_date_and_time() {
        let (refreshed_at, quote_trade_date, quote_time) = build_latest_quote_time_meta([
            ("20240603", "09:31:05"),
            ("20240603", "09:35:01"),
            ("20240604", "09:29:59"),
        ]);

        assert_eq!(refreshed_at.as_deref(), Some("20240604 09:29:59"));
        assert_eq!(quote_trade_date.as_deref(), Some("20240604"));
        assert_eq!(quote_time.as_deref(), Some("09:29:59"));
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
