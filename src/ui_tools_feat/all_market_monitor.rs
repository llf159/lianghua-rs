use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
};

use duckdb::{Connection, params};
use serde::Serialize;

use crate::{
    crawler::{SinaQuote, default_realtime_index_ts_codes},
    data::{load_stock_list, result_db_path, source_db_path},
    ui_tools_feat::{
        build_concepts_map,
        realtime::{
            RealtimeFetchMeta, fetch_all_market_realtime_quote_map_for_codes,
            fetch_all_market_tencent_realtime_quote_map_for_codes,
        },
    },
    utils::utils::board_category,
};

#[derive(Debug, Clone)]
struct StockMeta {
    ts_code: String,
    name: String,
    board: String,
    total_mv_yi: Option<f64>,
    concept: String,
}

#[derive(Debug, Clone)]
struct SourceMetaCacheEntry {
    stocks: Vec<StockMeta>,
}

#[derive(Debug, Clone)]
struct RankContext {
    rank: Option<i64>,
    best_rank_3d: Option<i64>,
    best_rank_5d: Option<i64>,
    total_score: Option<f64>,
}

#[derive(Debug, Clone)]
struct RankCacheEntry {
    rank_date: String,
    ranks: HashMap<String, RankContext>,
}

#[derive(Debug, Clone)]
struct SceneMarkerCandidate {
    scene_name: String,
    scene_rank: Option<i64>,
    stage_level: i32,
}

#[derive(Debug, Clone)]
struct SceneMarkerCacheEntry {
    rank_date: String,
    candidates: HashMap<String, Vec<SceneMarkerCandidate>>,
}

#[derive(Debug, Clone, Copy)]
struct Return5dContext {
    latest_close: Option<f64>,
    realtime_base_close: Option<f64>,
    daily_base_close: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct AllMarketMonitorRow {
    pub ts_code: String,
    pub name: String,
    pub board: String,
    pub concept: String,
    pub rank: Option<i64>,
    pub best_rank_3d: Option<i64>,
    pub best_rank_5d: Option<i64>,
    pub total_score: Option<f64>,
    pub realtime_trade_date: Option<String>,
    pub realtime_price: Option<f64>,
    pub realtime_open: Option<f64>,
    pub realtime_high: Option<f64>,
    pub realtime_low: Option<f64>,
    pub realtime_pre_close: Option<f64>,
    pub realtime_avg_price: Option<f64>,
    pub realtime_change_pct: Option<f64>,
    pub realtime_change_open_pct: Option<f64>,
    pub realtime_vol: Option<f64>,
    pub realtime_amount: Option<f64>,
    pub realtime_vol_ratio: Option<f64>,
    pub return_5d_pct: Option<f64>,
    pub scene_marker: Option<String>,
    pub total_mv_yi: Option<f64>,
    pub refreshed_at: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct AllMarketIndexRow {
    pub ts_code: String,
    pub name: String,
    pub realtime_trade_date: Option<String>,
    pub realtime_price: Option<f64>,
    pub realtime_change_pct: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct AllMarketMonitorSnapshotData {
    pub rows: Vec<AllMarketMonitorRow>,
    pub index_rows: Vec<AllMarketIndexRow>,
    pub refreshed_at: Option<String>,
    pub rank_date: Option<String>,
    pub requested_count: usize,
    pub fetched_count: usize,
}

static SOURCE_META_CACHE: OnceLock<Mutex<HashMap<String, SourceMetaCacheEntry>>> = OnceLock::new();
static RANK_CONTEXT_CACHE: OnceLock<Mutex<HashMap<String, RankCacheEntry>>> = OnceLock::new();
static SCENE_MARKER_CACHE: OnceLock<Mutex<HashMap<String, SceneMarkerCacheEntry>>> =
    OnceLock::new();
static RETURN_5D_CONTEXT_CACHE: OnceLock<Mutex<HashMap<String, HashMap<String, Return5dContext>>>> =
    OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RealtimeQuoteProvider {
    Sina,
    Tencent,
}

impl RealtimeQuoteProvider {
    fn parse(raw: Option<&str>) -> Result<Self, String> {
        let normalized = raw
            .map(|value| value.trim().to_ascii_lowercase())
            .unwrap_or_else(|| "sina".to_string());
        match normalized.as_str() {
            "" | "sina" | "sinajs" => Ok(Self::Sina),
            "tencent" | "qq" | "gtimg" => Ok(Self::Tencent),
            _ => Err("实时行情源仅支持 sina 或 tencent".to_string()),
        }
    }

    fn opposite(self) -> Self {
        match self {
            Self::Sina => Self::Tencent,
            Self::Tencent => Self::Sina,
        }
    }
}

fn source_meta_cache() -> &'static Mutex<HashMap<String, SourceMetaCacheEntry>> {
    SOURCE_META_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn rank_context_cache() -> &'static Mutex<HashMap<String, RankCacheEntry>> {
    RANK_CONTEXT_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn scene_marker_cache() -> &'static Mutex<HashMap<String, SceneMarkerCacheEntry>> {
    SCENE_MARKER_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn return_5d_context_cache() -> &'static Mutex<HashMap<String, HashMap<String, Return5dContext>>> {
    RETURN_5D_CONTEXT_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn parse_total_mv_yi(raw: Option<&String>) -> Option<f64> {
    raw?.trim()
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
        .map(|value| value / 1e4)
}

fn load_source_meta(source_path: &str) -> Result<SourceMetaCacheEntry, String> {
    let mut stocks = Vec::new();
    let concepts_map = build_concepts_map(source_path).unwrap_or_default();
    for cols in load_stock_list(source_path)? {
        let Some(ts_code) = cols.first().map(|value| value.trim()) else {
            continue;
        };
        if ts_code.is_empty() {
            continue;
        }
        let name = cols.get(2).map(|value| value.trim()).unwrap_or_default();
        stocks.push(StockMeta {
            ts_code: ts_code.to_string(),
            name: name.to_string(),
            board: board_category(ts_code, Some(name)).to_string(),
            total_mv_yi: parse_total_mv_yi(cols.get(9)),
            concept: concepts_map.get(ts_code).cloned().unwrap_or_default(),
        });
    }
    Ok(SourceMetaCacheEntry { stocks })
}

fn cached_source_meta(source_path: &str) -> Result<SourceMetaCacheEntry, String> {
    if let Some(entry) = source_meta_cache()
        .lock()
        .map_err(|_| "股票基础信息缓存锁已损坏".to_string())?
        .get(source_path)
        .cloned()
    {
        return Ok(entry);
    }

    let entry = load_source_meta(source_path)?;
    source_meta_cache()
        .lock()
        .map_err(|_| "股票基础信息缓存锁已损坏".to_string())?
        .insert(source_path.to_string(), entry.clone());
    Ok(entry)
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

fn query_latest_rank_date(conn: &Connection) -> Result<String, String> {
    let mut stmt = conn
        .prepare("SELECT MAX(trade_date) FROM score_summary")
        .map_err(|e| format!("预编译最新总榜日期失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询最新总榜日期失败: {e}"))?;

    if let Some(row) = rows
        .next()
        .map_err(|e| format!("读取最新总榜日期失败: {e}"))?
    {
        let trade_date: Option<String> = row
            .get(0)
            .map_err(|e| format!("读取最新总榜日期字段失败: {e}"))?;
        if let Some(value) = trade_date.filter(|value| !value.trim().is_empty()) {
            return Ok(value);
        }
    }

    Err("score_summary 没有可用交易日".to_string())
}

fn load_rank_context(
    conn: &Connection,
    rank_date: &str,
) -> Result<HashMap<String, RankContext>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            WITH recent_dates AS (
                SELECT
                    trade_date,
                    ROW_NUMBER() OVER (ORDER BY trade_date DESC) AS date_rank
                FROM (
                    SELECT trade_date
                    FROM score_summary
                    GROUP BY trade_date
                    ORDER BY trade_date DESC
                    LIMIT 5
                ) dates
            )
            SELECT
                s.ts_code,
                MAX(CASE WHEN s.trade_date = ? THEN s.rank ELSE NULL END) AS current_rank,
                MIN(CASE WHEN d.date_rank <= 3 AND s.rank IS NOT NULL THEN s.rank ELSE NULL END) AS best_rank_3d,
                MIN(CASE WHEN d.date_rank <= 5 AND s.rank IS NOT NULL THEN s.rank ELSE NULL END) AS best_rank_5d,
                MAX(CASE WHEN s.trade_date = ? THEN s.total_score ELSE NULL END) AS current_total_score
            FROM score_summary s
            INNER JOIN recent_dates d ON s.trade_date = d.trade_date
            GROUP BY s.ts_code
            "#,
        )
        .map_err(|e| format!("预编译全市场总榜排名失败: {e}"))?;
    let mut rows = stmt
        .query(params![rank_date, rank_date])
        .map_err(|e| format!("查询全市场总榜排名失败: {e}"))?;
    let mut out = HashMap::new();

    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取全市场总榜排名失败: {e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取排名代码失败: {e}"))?;
        if ts_code.trim().is_empty() {
            continue;
        }
        out.insert(
            ts_code,
            RankContext {
                rank: row.get(1).map_err(|e| format!("读取排名失败: {e}"))?,
                best_rank_3d: row
                    .get(2)
                    .map_err(|e| format!("读取三日最优排名失败: {e}"))?,
                best_rank_5d: row
                    .get(3)
                    .map_err(|e| format!("读取五日最优排名失败: {e}"))?,
                total_score: row.get(4).map_err(|e| format!("读取总分失败: {e}"))?,
            },
        );
    }

    Ok(out)
}

fn cached_rank_context(
    source_path: &str,
    conn: &Connection,
    rank_date: &str,
) -> Result<HashMap<String, RankContext>, String> {
    if let Some(entry) = rank_context_cache()
        .lock()
        .map_err(|_| "排名缓存锁已损坏".to_string())?
        .get(source_path)
        .filter(|entry| entry.rank_date == rank_date)
        .cloned()
    {
        return Ok(entry.ranks);
    }

    let ranks = load_rank_context(conn, rank_date)?;
    rank_context_cache()
        .lock()
        .map_err(|_| "排名缓存锁已损坏".to_string())?
        .insert(
            source_path.to_string(),
            RankCacheEntry {
                rank_date: rank_date.to_string(),
                ranks: ranks.clone(),
            },
        );
    Ok(ranks)
}

fn scene_stage_level(raw: Option<&str>) -> i32 {
    match raw.map(|value| value.trim().to_ascii_lowercase()).as_deref() {
        Some("confirm") => 3,
        Some("trigger") => 2,
        Some("observe") => 1,
        Some("fail") => 0,
        _ => -1,
    }
}

fn parse_scene_stage_threshold(raw: Option<&str>) -> i32 {
    match raw.map(|value| value.trim().to_ascii_lowercase()).as_deref() {
        Some("confirm") => 3,
        Some("observe") => 1,
        Some("fail") => 0,
        Some("trigger") | Some("") | None => 2,
        _ => 2,
    }
}

fn load_scene_marker_candidates(
    conn: &Connection,
    rank_date: &str,
) -> Result<HashMap<String, Vec<SceneMarkerCandidate>>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT
                ts_code,
                scene_name,
                stage,
                scene_rank
            FROM scene_details
            WHERE trade_date = ?
              AND scene_name IS NOT NULL
              AND TRIM(scene_name) <> ''
            ORDER BY
                ts_code ASC,
                COALESCE(scene_rank, 999999) ASC,
                scene_name ASC
            "#,
        )
        .map_err(|e| format!("预编译全市场场景标记失败: {e}"))?;
    let mut rows = stmt
        .query(params![rank_date])
        .map_err(|e| format!("查询全市场场景标记失败: {e}"))?;
    let mut out = HashMap::<String, Vec<SceneMarkerCandidate>>::new();

    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取全市场场景标记失败: {e}"))?
    {
        let ts_code: String = row
            .get(0)
            .map_err(|e| format!("读取场景标记代码失败: {e}"))?;
        let scene_name: String = row
            .get(1)
            .map_err(|e| format!("读取场景标记名称失败: {e}"))?;
        let stage: Option<String> = row
            .get(2)
            .map_err(|e| format!("读取场景标记等级失败: {e}"))?;
        let scene_rank: Option<i64> = row
            .get(3)
            .map_err(|e| format!("读取场景标记排名失败: {e}"))?;
        if ts_code.trim().is_empty() || scene_name.trim().is_empty() {
            continue;
        }
        out.entry(ts_code).or_default().push(SceneMarkerCandidate {
            scene_name: scene_name.trim().to_string(),
            scene_rank,
            stage_level: scene_stage_level(stage.as_deref()),
        });
    }

    Ok(out)
}

fn cached_scene_marker_candidates(
    source_path: &str,
    conn: &Connection,
    rank_date: &str,
) -> Result<HashMap<String, Vec<SceneMarkerCandidate>>, String> {
    if let Some(entry) = scene_marker_cache()
        .lock()
        .map_err(|_| "场景标记缓存锁已损坏".to_string())?
        .get(source_path)
        .filter(|entry| entry.rank_date == rank_date)
        .cloned()
    {
        return Ok(entry.candidates);
    }

    let candidates = load_scene_marker_candidates(conn, rank_date)?;
    scene_marker_cache()
        .lock()
        .map_err(|_| "场景标记缓存锁已损坏".to_string())?
        .insert(
            source_path.to_string(),
            SceneMarkerCacheEntry {
                rank_date: rank_date.to_string(),
                candidates: candidates.clone(),
            },
        );
    Ok(candidates)
}

fn build_scene_marker_map(
    candidates: &HashMap<String, Vec<SceneMarkerCandidate>>,
    threshold_level: i32,
) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for (ts_code, items) in candidates {
        let best = items
            .iter()
            .filter(|item| item.stage_level >= threshold_level)
            .min_by(|left, right| {
                left.scene_rank
                    .unwrap_or(i64::MAX)
                    .cmp(&right.scene_rank.unwrap_or(i64::MAX))
                    .then_with(|| left.scene_name.cmp(&right.scene_name))
            });
        if let Some(item) = best {
            let marker = match item.scene_rank {
                Some(rank) => format!("{} #{}", item.scene_name, rank),
                None => item.scene_name.clone(),
            };
            out.insert(ts_code.clone(), marker);
        }
    }
    out
}

fn load_return_5d_context_map(
    conn: &Connection,
    ts_codes: &[String],
) -> Result<HashMap<String, Return5dContext>, String> {
    if ts_codes.is_empty() {
        return Ok(HashMap::new());
    }

    let placeholders = std::iter::repeat_n("?", ts_codes.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
        SELECT
            ts_code,
            MAX(CASE WHEN row_num = 1 THEN close_value ELSE NULL END) AS latest_close,
            MAX(CASE WHEN row_num = 5 THEN close_value ELSE NULL END) AS realtime_base_close,
            MAX(CASE WHEN row_num = 6 THEN close_value ELSE NULL END) AS daily_base_close
        FROM (
            SELECT
                ts_code,
                TRY_CAST(close AS DOUBLE) AS close_value,
                ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS row_num
            FROM stock_data
            WHERE adj_type = ? AND ts_code IN ({placeholders})
        ) ranked
        WHERE row_num IN (1, 5, 6)
        GROUP BY ts_code
        "#
    );

    let mut params = Vec::with_capacity(ts_codes.len() + 1);
    params.push("qfq".to_string());
    params.extend(ts_codes.iter().cloned());

    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("预编译五日涨幅查询失败: {e}"))?;
    let mut rows = stmt
        .query(duckdb::params_from_iter(params.iter()))
        .map_err(|e| format!("查询五日涨幅失败: {e}"))?;
    let mut out = HashMap::new();

    while let Some(row) = rows.next().map_err(|e| format!("读取五日涨幅失败: {e}"))? {
        let ts_code: String = row
            .get(0)
            .map_err(|e| format!("读取五日涨幅代码失败: {e}"))?;
        out.insert(
            ts_code,
            Return5dContext {
                latest_close: row
                    .get(1)
                    .map_err(|e| format!("读取五日涨幅最新收盘失败: {e}"))?,
                realtime_base_close: row
                    .get(2)
                    .map_err(|e| format!("读取五日涨幅实时基准失败: {e}"))?,
                daily_base_close: row
                    .get(3)
                    .map_err(|e| format!("读取五日涨幅日线基准失败: {e}"))?,
            },
        );
    }

    Ok(out)
}

fn cached_return_5d_context_map(
    source_path: &str,
    conn: &Connection,
    ts_codes: &[String],
) -> Result<HashMap<String, Return5dContext>, String> {
    if let Some(entry) = return_5d_context_cache()
        .lock()
        .map_err(|_| "五日涨幅缓存锁已损坏".to_string())?
        .get(source_path)
        .cloned()
    {
        return Ok(entry);
    }

    let entry = load_return_5d_context_map(conn, ts_codes)?;
    return_5d_context_cache()
        .lock()
        .map_err(|_| "五日涨幅缓存锁已损坏".to_string())?
        .insert(source_path.to_string(), entry.clone());
    Ok(entry)
}

fn calc_return_pct(price: Option<f64>, base: Option<f64>) -> Option<f64> {
    match (price, base) {
        (Some(price), Some(base)) if price.is_finite() && base.is_finite() && base > 0.0 => {
            Some((price / base - 1.0) * 100.0)
        }
        _ => None,
    }
}

fn normalize_quote_trade_date(raw: &str) -> Option<String> {
    let digits: String = raw.chars().filter(|ch| ch.is_ascii_digit()).collect();
    if digits.len() == 8 {
        Some(digits)
    } else {
        None
    }
}

fn quote_change_open_pct(quote: &SinaQuote) -> Option<f64> {
    if quote.open > 0.0 {
        Some((quote.price / quote.open - 1.0) * 100.0)
    } else {
        None
    }
}

fn build_rows(
    meta: &SourceMetaCacheEntry,
    ranks: &HashMap<String, RankContext>,
    quotes: &HashMap<String, SinaQuote>,
    volume_ratio_map: &HashMap<String, f64>,
    avg_price_map: &HashMap<String, f64>,
    return_5d_map: &HashMap<String, Return5dContext>,
    scene_marker_map: &HashMap<String, String>,
    fetch_meta: &RealtimeFetchMeta,
) -> Vec<AllMarketMonitorRow> {
    meta.stocks
        .iter()
        .map(|stock| {
            let quote = quotes.get(&stock.ts_code);
            let rank = ranks.get(&stock.ts_code);
            let return_5d = return_5d_map.get(&stock.ts_code);
            let return_5d_pct = if let Some(quote) = quote {
                calc_return_pct(
                    Some(quote.price),
                    return_5d.and_then(|item| item.realtime_base_close),
                )
            } else {
                calc_return_pct(
                    return_5d.and_then(|item| item.latest_close),
                    return_5d.and_then(|item| item.daily_base_close),
                )
            };
            AllMarketMonitorRow {
                ts_code: stock.ts_code.clone(),
                name: quote
                    .map(|item| item.name.trim())
                    .filter(|value| !value.is_empty())
                    .unwrap_or(stock.name.as_str())
                    .to_string(),
                board: stock.board.clone(),
                concept: stock.concept.clone(),
                rank: rank.and_then(|item| item.rank),
                best_rank_3d: rank.and_then(|item| item.best_rank_3d),
                best_rank_5d: rank.and_then(|item| item.best_rank_5d),
                total_score: rank.and_then(|item| item.total_score),
                realtime_trade_date: quote.and_then(|item| normalize_quote_trade_date(&item.date)),
                realtime_price: quote.map(|item| item.price),
                realtime_open: quote.map(|item| item.open),
                realtime_high: quote.map(|item| item.high),
                realtime_low: quote.map(|item| item.low),
                realtime_pre_close: quote.map(|item| item.pre_close),
                realtime_avg_price: avg_price_map.get(&stock.ts_code).copied(),
                realtime_change_pct: quote.and_then(|item| item.change_pct),
                realtime_change_open_pct: quote.and_then(quote_change_open_pct),
                realtime_vol: quote.map(|item| item.vol),
                realtime_amount: quote.map(|item| item.amount),
                realtime_vol_ratio: volume_ratio_map.get(&stock.ts_code).copied(),
                return_5d_pct,
                scene_marker: scene_marker_map.get(&stock.ts_code).cloned(),
                total_mv_yi: stock.total_mv_yi,
                refreshed_at: fetch_meta.refreshed_at.clone(),
            }
        })
        .collect()
}

fn build_index_rows(quotes: &HashMap<String, SinaQuote>) -> Vec<AllMarketIndexRow> {
    default_realtime_index_ts_codes()
        .into_iter()
        .map(|ts_code| {
            let quote = quotes.get(&ts_code);
            AllMarketIndexRow {
                ts_code: ts_code.clone(),
                name: quote
                    .map(|item| item.name.trim())
                    .filter(|value| !value.is_empty())
                    .unwrap_or(ts_code.as_str())
                    .to_string(),
                realtime_trade_date: quote.and_then(|item| normalize_quote_trade_date(&item.date)),
                realtime_price: quote.map(|item| item.price),
                realtime_change_pct: quote.and_then(|item| item.change_pct),
            }
        })
        .collect()
}

fn fetch_index_rows(provider: RealtimeQuoteProvider) -> Vec<AllMarketIndexRow> {
    let index_ts_codes = default_realtime_index_ts_codes();
    let quote_result = match provider {
        RealtimeQuoteProvider::Sina => {
            fetch_all_market_realtime_quote_map_for_codes(&index_ts_codes).map(|(quotes, _)| quotes)
        }
        RealtimeQuoteProvider::Tencent => fetch_all_market_tencent_realtime_quote_map_for_codes(
            &index_ts_codes,
        )
        .map(|(quotes, _)| {
            quotes
                .into_iter()
                .map(|(ts_code, quote)| (ts_code, quote.into_sina_quote()))
                .collect::<HashMap<_, _>>()
        }),
    };

    quote_result
        .map(|quotes| build_index_rows(&quotes))
        .unwrap_or_else(|_| Vec::new())
}

pub fn get_all_market_monitor_snapshot(
    source_path: &str,
    realtime_provider: Option<String>,
    scene_stage_threshold: Option<String>,
) -> Result<AllMarketMonitorSnapshotData, String> {
    let meta = cached_source_meta(source_path)?;
    let ts_codes = meta
        .stocks
        .iter()
        .map(|item| item.ts_code.clone())
        .collect::<Vec<_>>();
    let provider = RealtimeQuoteProvider::parse(realtime_provider.as_deref())?;
    let (quotes, volume_ratio_map, avg_price_map, fetch_meta) = match provider {
        RealtimeQuoteProvider::Sina => {
            let (quotes, fetch_meta) = fetch_all_market_realtime_quote_map_for_codes(&ts_codes)?;
            (quotes, HashMap::new(), HashMap::new(), fetch_meta)
        }
        RealtimeQuoteProvider::Tencent => {
            let (tencent_quotes, fetch_meta) =
                fetch_all_market_tencent_realtime_quote_map_for_codes(&ts_codes)?;
            let volume_ratio_map = tencent_quotes
                .iter()
                .filter_map(|(ts_code, quote)| {
                    quote.volume_ratio.map(|value| (ts_code.clone(), value))
                })
                .collect::<HashMap<_, _>>();
            let avg_price_map = tencent_quotes
                .iter()
                .filter_map(|(ts_code, quote)| {
                    quote.avg_price.map(|value| (ts_code.clone(), value))
                })
                .collect::<HashMap<_, _>>();
            let quotes = tencent_quotes
                .into_iter()
                .map(|(ts_code, quote)| (ts_code, quote.into_sina_quote()))
                .collect::<HashMap<_, _>>();
            (quotes, volume_ratio_map, avg_price_map, fetch_meta)
        }
    };

    let conn = open_result_conn(source_path)?;
    let rank_date = query_latest_rank_date(&conn)?;
    let ranks = cached_rank_context(source_path, &conn, &rank_date)?;
    let scene_marker_candidates =
        cached_scene_marker_candidates(source_path, &conn, &rank_date).unwrap_or_default();
    let scene_marker_map = build_scene_marker_map(
        &scene_marker_candidates,
        parse_scene_stage_threshold(scene_stage_threshold.as_deref()),
    );
    let return_5d_map = open_source_conn(source_path)
        .and_then(|conn| cached_return_5d_context_map(source_path, &conn, &ts_codes))
        .unwrap_or_default();
    let rows = build_rows(
        &meta,
        &ranks,
        &quotes,
        &volume_ratio_map,
        &avg_price_map,
        &return_5d_map,
        &scene_marker_map,
        &fetch_meta,
    );
    let index_rows = fetch_index_rows(provider.opposite());

    Ok(AllMarketMonitorSnapshotData {
        rows,
        index_rows,
        refreshed_at: fetch_meta.refreshed_at,
        rank_date: Some(rank_date),
        requested_count: fetch_meta.requested_count,
        fetched_count: fetch_meta.fetched_count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_meta() -> SourceMetaCacheEntry {
        SourceMetaCacheEntry {
            stocks: vec![
                StockMeta {
                    ts_code: "000001.SZ".to_string(),
                    name: "平安银行".to_string(),
                    board: "主板".to_string(),
                    total_mv_yi: Some(100.0),
                    concept: "银行;互联金融".to_string(),
                },
                StockMeta {
                    ts_code: "300001.SZ".to_string(),
                    name: "特锐德".to_string(),
                    board: "创业/科创".to_string(),
                    total_mv_yi: None,
                    concept: String::new(),
                },
            ],
        }
    }

    fn sample_fetch_meta() -> RealtimeFetchMeta {
        RealtimeFetchMeta {
            requested_count: 2,
            effective_count: 2,
            fetched_count: 1,
            truncated: false,
            refreshed_at: Some("20240603 09:31:00".to_string()),
            quote_trade_date: Some("20240603".to_string()),
            quote_time: Some("09:31:00".to_string()),
        }
    }

    #[test]
    fn build_rows_keeps_unranked_and_unquoted_stocks() {
        let mut ranks = HashMap::new();
        ranks.insert(
            "000001.SZ".to_string(),
            RankContext {
                rank: Some(12),
                best_rank_3d: Some(5),
                best_rank_5d: Some(3),
                total_score: Some(88.5),
            },
        );
        let mut quotes = HashMap::new();
        quotes.insert(
            "000001.SZ".to_string(),
            SinaQuote {
                date: "2024-06-03".to_string(),
                time: "09:31:00".to_string(),
                ts_code: "000001.SZ".to_string(),
                name: "平安银行".to_string(),
                open: 10.0,
                high: 10.2,
                low: 9.9,
                pre_close: 9.8,
                price: 10.1,
                vol: 1000.0,
                amount: 10_000.0,
                change_pct: Some(1.02),
            },
        );

        let rows = build_rows(
            &sample_meta(),
            &ranks,
            &quotes,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &sample_fetch_meta(),
        );

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].rank, Some(12));
        assert_eq!(rows[0].best_rank_3d, Some(5));
        assert_eq!(rows[0].best_rank_5d, Some(3));
        assert_eq!(rows[0].total_score, Some(88.5));
        assert_eq!(rows[0].concept, "银行;互联金融");
        assert_eq!(rows[0].realtime_trade_date.as_deref(), Some("20240603"));
        let change_open_pct = rows[0]
            .realtime_change_open_pct
            .expect("open change should exist");
        assert!((change_open_pct - 1.0).abs() < 1e-9);
        assert_eq!(rows[1].rank, None);
        assert_eq!(rows[1].realtime_price, None);
    }
}
