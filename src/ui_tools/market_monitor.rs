use serde::Serialize;

use crate::ui_tools::{
    overview::{OverviewRow, get_rank_overview},
    realtime::{RealtimeFetchMeta, fetch_realtime_quote_map},
};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketMonitorRow {
    pub ts_code: String,
    pub name: String,
    pub reference_trade_date: Option<String>,
    pub reference_rank: Option<i64>,
    pub total_score: Option<f64>,
    pub latest_price: Option<f64>,
    pub latest_change_pct: Option<f64>,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub concept: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketMonitorPageData {
    pub rows: Vec<MarketMonitorRow>,
    pub requested_count: usize,
    pub effective_count: usize,
    pub fetched_count: usize,
    pub truncated: bool,
    pub refreshed_at: Option<String>,
    pub reference_trade_date: Option<String>,
}

pub fn build_market_monitor_rows(
    overview_rows: Vec<OverviewRow>,
    quote_map: &std::collections::HashMap<String, crate::crawler::SinaQuote>,
) -> Vec<MarketMonitorRow> {
    overview_rows
        .into_iter()
        .map(|row| {
            let quote = quote_map.get(&row.ts_code);
            MarketMonitorRow {
                ts_code: row.ts_code,
                name: row.name,
                reference_trade_date: row.trade_date,
                reference_rank: row.rank,
                total_score: row.total_score,
                latest_price: quote.map(|item| item.price),
                latest_change_pct: quote.and_then(|item| item.change_pct),
                open: quote.map(|item| item.open),
                high: quote.map(|item| item.high),
                low: quote.map(|item| item.low),
                concept: row.concept,
            }
        })
        .collect()
}

pub fn build_market_monitor_page_from_rows(
    overview_rows: Vec<OverviewRow>,
    quote_map: std::collections::HashMap<String, crate::crawler::SinaQuote>,
    fetch_meta: RealtimeFetchMeta,
) -> MarketMonitorPageData {
    let resolved_reference_trade_date =
        overview_rows.first().and_then(|row| row.trade_date.clone());
    let rows = build_market_monitor_rows(overview_rows, &quote_map);

    MarketMonitorPageData {
        rows,
        requested_count: fetch_meta.requested_count,
        effective_count: fetch_meta.effective_count,
        fetched_count: fetch_meta.fetched_count,
        truncated: fetch_meta.truncated,
        refreshed_at: fetch_meta.refreshed_at,
        reference_trade_date: resolved_reference_trade_date,
    }
}

pub fn get_market_monitor_page(
    source_path: String,
    reference_trade_date: Option<String>,
    top_limit: Option<u32>,
) -> Result<MarketMonitorPageData, String> {
    let limit = top_limit.unwrap_or(20).max(1);
    let overview_rows = get_rank_overview(
        source_path,
        reference_trade_date,
        Some(limit),
        None,
        None,
        None,
    )?;
    let ts_codes: Vec<String> = overview_rows
        .iter()
        .map(|row| row.ts_code.clone())
        .collect();
    let (quote_map, fetch_meta) = fetch_realtime_quote_map(&ts_codes)?;
    let resolved_reference_trade_date =
        overview_rows.first().and_then(|row| row.trade_date.clone());

    Ok(MarketMonitorPageData {
        rows: build_market_monitor_rows(overview_rows, &quote_map),
        requested_count: fetch_meta.requested_count,
        effective_count: fetch_meta.effective_count,
        fetched_count: fetch_meta.fetched_count,
        truncated: fetch_meta.truncated,
        refreshed_at: fetch_meta.refreshed_at,
        reference_trade_date: resolved_reference_trade_date,
    })
}
