use crate::{
    daily_baseline::{self, DailyBaseline},
    open_monitor_result_conn as open_result_conn,
};
use std::{
    collections::{HashMap, HashSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    sync::{Arc, Mutex, OnceLock},
};

use chrono::{FixedOffset, NaiveDateTime, Timelike, Utc};
use duckdb::{Connection, params};
use rayon::prelude::*;
use serde::Serialize;

use crate::{
    crawler::{SinaQuote, default_realtime_index_ts_codes},
    data::{
        DataReader, RowData, ind_toml_path, load_stock_list, load_trade_date_list, result_db_path,
        runtime::row_into_rt, source_db_path,
    },
    download::ind_calc::{
        IndsCache, cache_ind_build, calc_inds_with_cache_lossy, warmup_ind_estimate,
    },
    expr::{
        eval::{Runtime, Value},
        parser::{Expr, Stmt, Stmts},
    },
    scoring::tools::{
        CyqChenFieldInjector, SimilarityRankFieldInjector, inject_empty_optional_cyq_chen_fields,
        inject_latest_num_fields, inject_stock_extra_fields, load_total_share_map, rt_max_len,
    },
    utils::utils::board_category,
};
use crate::{
    intraday_monitor::{
        CompiledIntradayMonitorTemplate, DEFAULT_ADJ_TYPE, IntradayMonitorTemplate,
        add_indicator_input_runtime_keys, build_quote_only_runtime_row_data,
        collect_intraday_template_cyq_chen_keys, collect_intraday_template_runtime_keys,
        compile_intraday_templates, merge_realtime_quote_into_row_data, normalize_runtime_row_data,
    },
    realtime::{
        RealtimeFetchMeta, fetch_all_market_realtime_quote_map_for_codes,
        fetch_all_market_tencent_realtime_quote_map_for_codes, normalize_quote_time,
    },
};
use lianghua_app_expression::{
    RT_AVERAGE_PRICE, RT_FALL_FROM_HIGH_PCT, RT_OPEN_CHANGE_PCT, RT_OPENING_VOLUME_RATIO_HIGH,
    RT_VOLUME_RATIO,
};
use lianghua_app_shared::build_concepts_map;

use super::scene_stage::{level as scene_stage_level, threshold as parse_scene_stage_threshold};

#[derive(Debug, Clone)]
struct StockMeta {
    ts_code: String,
    name: String,
    board: String,
    industry: String,
    total_mv_yi: Option<f64>,
    concept: String,
}

#[derive(Debug, Clone)]
struct SourceMetaCacheEntry {
    stocks: Vec<StockMeta>,
    warning: Option<String>,
}

#[derive(Debug, Clone)]
struct RankContext {
    rank: Option<i64>,
    best_rank_3d: Option<i64>,
    similarity_rank: Option<i64>,
    total_score: Option<f64>,
}

#[derive(Debug, Clone)]
struct RankCacheEntry {
    rank_date: String,
    ranks: Arc<HashMap<String, RankContext>>,
    scene_candidates: Arc<HashMap<String, Vec<SceneMarkerCandidate>>>,
}

#[derive(Debug, Clone)]
struct SceneMarkerCandidate {
    scene_name: String,
    scene_rank: Option<i64>,
    stage_level: i32,
}

#[derive(Debug, Clone, Serialize)]
pub struct AllMarketTemplateHit {
    pub template_id: String,
    pub template_name: String,
}

#[derive(Debug, Serialize)]
pub struct AllMarketMonitorRow {
    pub ts_code: String,
    pub name: String,
    pub board: String,
    pub industry: String,
    pub concept: String,
    pub rank: Option<i64>,
    pub best_rank_3d: Option<i64>,
    pub similarity_rank: Option<i64>,
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
    pub realtime_vol_ratio_high: Option<f64>,
    pub realtime_vol_ratio_opening_high: Option<f64>,
    pub return_5d_pct: Option<f64>,
    pub other_sort_value: Option<f64>,
    pub scene_marker: Option<String>,
    pub template_hits: Option<Vec<AllMarketTemplateHit>>,
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
    pub realtime_amount: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct AllMarketMonitorSnapshotData {
    pub rows: Vec<AllMarketMonitorRow>,
    pub index_rows: Vec<AllMarketIndexRow>,
    pub data_version: Option<String>,
    pub has_new_data: bool,
    pub market_active: bool,
    pub market_status_message: Option<String>,
    pub refreshed_at: Option<String>,
    pub rank_date: Option<String>,
    pub requested_count: usize,
    pub fetched_count: usize,
    pub template_warning_message: Option<String>,
    pub volume_ratio_status_message: Option<String>,
}

pub(crate) fn check_market_activity(
    now: NaiveDateTime,
    quotes: &HashMap<String, SinaQuote>,
    calendar_trading_day: impl FnOnce() -> Option<bool>,
) -> (bool, Option<String>) {
    let seconds = now.time().num_seconds_from_midnight();
    let session_start = if (9 * 3600 + 30 * 60..11 * 3600 + 30 * 60).contains(&seconds) {
        9 * 3600 + 30 * 60
    } else if (13 * 3600..15 * 3600).contains(&seconds) {
        13 * 3600
    } else {
        return (
            false,
            Some("非连续交易时段，已刷新一次并暂停爬虫".to_string()),
        );
    };
    let mut available = 0;
    let mut fresh = 0;
    let mut freshness_by_timestamp = HashMap::new();
    for quote in quotes
        .values()
        .filter(|quote| quote.price.is_finite() && quote.price > 0.0)
    {
        available += 1;
        let is_fresh = freshness_by_timestamp
            .entry((quote.date.as_str(), quote.time.as_str()))
            .or_insert_with(|| {
                let timestamp = normalize_quote_trade_date(&quote.date)
                    .zip(normalize_quote_time(&quote.time))
                    .and_then(|(date, time)| {
                        NaiveDateTime::parse_from_str(&format!("{date} {time}"), "%Y%m%d %H:%M:%S")
                            .ok()
                    });
                timestamp.is_some_and(|timestamp| {
                    let age = (now - timestamp).num_seconds();
                    timestamp.date() == now.date()
                        && timestamp.time().num_seconds_from_midnight() >= session_start
                        && (0..=180).contains(&age)
                })
            });
        fresh += usize::from(*is_fresh);
    }
    if available > 0 && fresh * 2 > available {
        // 当天多数有效行情能纠正缺失、过期或误标休市的本地交易日历。
        return (true, None);
    }
    let reason = if calendar_trading_day() == Some(false) {
        "本地日历未标为交易日，且行情未确认当前正在交易"
    } else {
        "行情日期或更新时间未确认当前正在交易"
    };
    (false, Some(format!("{reason}，已刷新一次并暂停爬虫")))
}

static SOURCE_META_CACHE: OnceLock<Mutex<HashMap<String, Arc<SourceMetaCacheEntry>>>> =
    OnceLock::new();
static RANK_CONTEXT_CACHE: OnceLock<Mutex<HashMap<String, RankCacheEntry>>> = OnceLock::new();
static TEMPLATE_RUNTIME_CACHE: OnceLock<
    Mutex<HashMap<String, Arc<AllMarketTemplateRuntimeCacheEntry>>>,
> = OnceLock::new();
static OTHER_SORT_DAILY_VALUE_CACHE: OnceLock<
    Mutex<HashMap<String, Arc<HashMap<String, Option<f64>>>>>,
> = OnceLock::new();

#[derive(Debug, Clone, Copy, Default)]
struct TemplateRankScoreInfo {
    rank: Option<f64>,
    score: Option<f64>,
}

struct AllMarketTemplateRuntimeCacheEntry {
    cache_key: String,
    cache_prefix: String,
    template_order: Vec<String>,
    compiled_templates: HashMap<String, CompiledIntradayMonitorTemplate>,
    base_rows: HashMap<String, RowData>,
    indicator_cache: Vec<IndsCache>,
    total_share_map: HashMap<String, f64>,
    cyq_chen_runtime_keys: HashSet<String>,
    warning_messages: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TemplateRuntimeDataMode {
    QuoteOnly,
    History,
}

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

fn source_meta_cache() -> &'static Mutex<HashMap<String, Arc<SourceMetaCacheEntry>>> {
    SOURCE_META_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn rank_context_cache() -> &'static Mutex<HashMap<String, RankCacheEntry>> {
    RANK_CONTEXT_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn load_rank_context(conn: &Connection) -> Result<RankCacheEntry, String> {
    let similarity_board = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_catalog='similarity_db' AND table_schema='main' AND table_name='strategy_trigger_similarity_summary')",
        [], |row| row.get::<_, bool>(0),
    ).unwrap_or(false);
    let latest_sim_cte = if similarity_board {
        r#",
        latest_sim AS (
            SELECT ts_code, rank AS similarity_rank
            FROM similarity_db.main.strategy_trigger_similarity_summary
            WHERE trade_date = (SELECT MAX(trade_date) FROM similarity_db.main.strategy_trigger_similarity_summary)
                AND rank IS NOT NULL
        )"#
    } else {
        ""
    };
    let (ts_code_column, sim_column, sim_join) = if similarity_board {
        (
            "COALESCE(agg.ts_code, sim.ts_code)",
            "sim.similarity_rank",
            "FULL OUTER JOIN latest_sim sim ON sim.ts_code = agg.ts_code",
        )
    } else {
        ("agg.ts_code", "CAST(NULL AS BIGINT)", "")
    };
    let sql = format!(
        r#"
        WITH recent_dates AS (
            SELECT DISTINCT trade_date FROM score_summary
            ORDER BY trade_date DESC LIMIT 3
        ), latest AS (
            SELECT MAX(trade_date) AS rd FROM recent_dates
        ), agg AS (
            SELECT s.ts_code,
                MAX(CASE WHEN s.trade_date = latest.rd THEN s.rank END) AS current_rank,
                MIN(s.rank) AS best_rank_3d,
                MAX(CASE WHEN s.trade_date = latest.rd THEN s.total_score END) AS current_total_score
            FROM score_summary s
            INNER JOIN recent_dates d ON s.trade_date = d.trade_date
            CROSS JOIN latest
            GROUP BY s.ts_code
        ){latest_sim_cte}
        SELECT latest.rd, {ts_code_column}, agg.current_rank, agg.best_rank_3d,
            agg.current_total_score, {sim_column},
            CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR), CAST(NULL AS BIGINT)
        FROM agg {sim_join} CROSS JOIN latest
        UNION ALL
        SELECT latest.rd, ts_code, NULL, NULL, NULL, NULL, scene_name, stage, scene_rank
        FROM scene_details CROSS JOIN latest
        WHERE trade_date = latest.rd AND scene_name IS NOT NULL AND TRIM(scene_name) <> ''
        "#,
    );
    // 两类记录直接拼接，避免同股多条场景重复展开排名。
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("预编译监控排名与场景失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询监控排名与场景失败: {e}"))?;
    let mut rank_date = String::new();
    let mut ranks = HashMap::new();
    let mut scene_candidates = HashMap::<String, Vec<SceneMarkerCandidate>>::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取监控排名与场景失败: {e}"))?
    {
        let ts_code: String = row.get(1).map_err(|e| format!("读取排名代码失败: {e}"))?;
        if ts_code.trim().is_empty() {
            continue;
        }
        if rank_date.is_empty() {
            rank_date = row
                .get::<_, Option<String>>(0)
                .map_err(|e| format!("读取最新总榜日期失败: {e}"))?
                .unwrap_or_default();
        }
        let scene_name: Option<String> = row
            .get(6)
            .map_err(|e| format!("读取场景标记名称失败: {e}"))?;
        if let Some(scene_name) = scene_name {
            let stage: Option<String> = row
                .get(7)
                .map_err(|e| format!("读取场景标记等级失败: {e}"))?;
            scene_candidates
                .entry(ts_code)
                .or_default()
                .push(SceneMarkerCandidate {
                    scene_name: scene_name.trim().to_string(),
                    scene_rank: row
                        .get(8)
                        .map_err(|e| format!("读取场景标记排名失败: {e}"))?,
                    stage_level: scene_stage_level(stage.as_deref()),
                });
        } else {
            ranks.insert(
                ts_code,
                RankContext {
                    rank: row.get(2).map_err(|e| format!("读取排名失败: {e}"))?,
                    best_rank_3d: row
                        .get(3)
                        .map_err(|e| format!("读取三日最优排名失败: {e}"))?,
                    total_score: row.get(4).map_err(|e| format!("读取总分失败: {e}"))?,
                    similarity_rank: row.get(5).map_err(|e| format!("读取相似排名失败: {e}"))?,
                },
            );
        }
    }
    Ok(RankCacheEntry {
        rank_date,
        ranks: Arc::new(ranks),
        scene_candidates: Arc::new(scene_candidates),
    })
}

fn template_runtime_cache()
-> &'static Mutex<HashMap<String, Arc<AllMarketTemplateRuntimeCacheEntry>>> {
    TEMPLATE_RUNTIME_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn other_sort_daily_value_cache()
-> &'static Mutex<HashMap<String, Arc<HashMap<String, Option<f64>>>>> {
    OTHER_SORT_DAILY_VALUE_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
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

fn freeze_runtime_series(rt: &mut Runtime) {
    for value in rt.vars.values_mut() {
        if let Value::NumSeries(series) = value {
            let owned = std::mem::take(series);
            *value = Value::SharedNumSeries(Arc::new(owned));
        }
    }
}

fn hash_to_hex<T: Hash>(value: &T) -> String {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn expr_uses_ident(expr: &Expr, targets: &HashSet<&str>) -> bool {
    match expr {
        Expr::Ident(name) => targets.contains(name.to_ascii_uppercase().as_str()),
        Expr::Call { args, .. } => args.iter().any(|arg| expr_uses_ident(arg, targets)),
        Expr::Unary { rhs, .. } => expr_uses_ident(rhs, targets),
        Expr::Binary { lhs, rhs, .. } => {
            expr_uses_ident(lhs, targets) || expr_uses_ident(rhs, targets)
        }
        Expr::Number(_) => false,
    }
}

fn program_uses_ident(program: &Stmts, targets: &HashSet<&str>) -> bool {
    program.item.iter().any(|stmt| match stmt {
        Stmt::Expr(expr) => expr_uses_ident(expr, targets),
        Stmt::Assign { value, .. } => expr_uses_ident(value, targets),
    })
}

fn inject_template_rank_score_series(
    row_data: &mut RowData,
    rank_score_map: Option<&HashMap<String, TemplateRankScoreInfo>>,
) -> Result<(), String> {
    let len = row_data.trade_dates.len();
    let mut rank_series = vec![None; len];
    let mut score_series = vec![None; len];

    if let Some(rank_score_map) = rank_score_map {
        for (index, trade_date) in row_data.trade_dates.iter().enumerate() {
            if let Some(values) = rank_score_map.get(trade_date) {
                rank_series[index] = values.rank;
                score_series[index] = values.score;
            }
        }
    }

    row_data.cols.insert("RANK".to_string(), rank_series);
    row_data.cols.insert("SCORE".to_string(), score_series);
    row_data.validate()
}

fn clear_latest_template_rank_score(row_data: &mut RowData) {
    if row_data.trade_dates.is_empty() {
        return;
    }
    let latest_index = row_data.trade_dates.len() - 1;
    for key in ["RANK", "SCORE"] {
        if let Some(series) = row_data.cols.get_mut(key) {
            if let Some(value) = series.get_mut(latest_index) {
                *value = None;
            }
        }
    }
}

fn build_template_warning_message(messages: Vec<String>) -> Option<String> {
    let normalized = messages
        .into_iter()
        .map(|message| message.trim().to_string())
        .filter(|message| !message.is_empty())
        .collect::<Vec<_>>();

    match normalized.as_slice() {
        [] => None,
        [first] => Some(format!("模板计算异常: {first}")),
        [first, ..] => Some(format!(
            "模板计算异常共 {} 条，首条: {}",
            normalized.len(),
            first
        )),
    }
}

fn cached_template_runtime(
    source_path: &str,
    stocks: &[StockMeta],
    templates: &[IntradayMonitorTemplate],
    data_mode: TemplateRuntimeDataMode,
) -> Result<Arc<AllMarketTemplateRuntimeCacheEntry>, String> {
    let lookup_prefix = format!(
        "{}|tpl:{}|mode:{:?}|",
        source_path,
        hash_to_hex(
            &templates
                .iter()
                .map(|item| (item.id.trim(), item.name.trim(), item.expression.trim(),))
                .collect::<Vec<_>>()
        ),
        data_mode,
    );
    let source_stamps = [
        source_db_path(source_path),
        result_db_path(source_path),
        ind_toml_path(source_path),
    ]
    .map(|path| {
        std::fs::metadata(path)
            .ok()
            .map(|metadata| (metadata.modified().ok(), metadata.len()))
    });
    let stock_fingerprint = hash_to_hex(
        &stocks
            .iter()
            .map(|stock| (stock.ts_code.as_str(), stock.board.as_str()))
            .collect::<Vec<_>>(),
    );
    let today = Utc::now()
        .with_timezone(&FixedOffset::east_opt(8 * 3600).unwrap())
        .date_naive();
    let lookup_key = format!(
        "{lookup_prefix}stocks:{stock_fingerprint}|source:{}|date:{today}",
        hash_to_hex(&source_stamps)
    );
    // 命中时连编译、字段依赖分析和指标预热估算也一并跳过。
    if let Some(entry) = template_runtime_cache()
        .lock()
        .map_err(|_| "模板预热缓存锁已损坏".to_string())?
        .get(&lookup_key)
        .cloned()
    {
        return Ok(entry);
    }

    let compiled_templates = compile_intraday_templates(templates);
    let template_order = (|templates: &[IntradayMonitorTemplate]| -> Vec<String> {
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for template in templates {
            let id = template.id.trim();
            if id.is_empty() || !seen.insert(id.to_string()) {
                continue;
            }
            out.push(id.to_string());
        }
        out
    })(templates);
    let ready_programs = compiled_templates
        .values()
        .filter_map(|item| match item {
            CompiledIntradayMonitorTemplate::Ready(template) => Some(&template.ast),
            CompiledIntradayMonitorTemplate::Invalid { .. } => None,
        })
        .collect::<Vec<_>>();
    let mut required_runtime_keys = collect_intraday_template_runtime_keys(&ready_programs);
    let cyq_chen_runtime_keys = collect_intraday_template_cyq_chen_keys(&ready_programs);
    let rank_score_keys = HashSet::from(["RANK", "SCORE"]);
    let uses_rank_score = ready_programs
        .iter()
        .any(|program| program_uses_ident(program, &rank_score_keys));
    let similarity_rank_keys = HashSet::from(["S_RANK"]);
    let uses_similarity_rank = ready_programs
        .iter()
        .any(|program| program_uses_ident(program, &similarity_rank_keys));
    let template_warmup_need = compiled_templates
        .values()
        .filter_map(|item| match item {
            CompiledIntradayMonitorTemplate::Ready(template) => Some(template.warmup_need),
            CompiledIntradayMonitorTemplate::Invalid { .. } => None,
        })
        .max()
        .unwrap_or(0);
    let indicator_cache = (|indicator_cache: Vec<IndsCache>,
                            required_runtime_keys: &HashSet<String>|
     -> Vec<IndsCache> {
        if required_runtime_keys.is_empty() {
            return Vec::new();
        }
        indicator_cache
            .into_iter()
            .filter(|item| required_runtime_keys.contains(item.name.trim()))
            .collect()
    })(
        cache_ind_build(source_path).unwrap_or_default(),
        &required_runtime_keys,
    );
    let indicator_warmup_need = if indicator_cache.is_empty() {
        0
    } else {
        add_indicator_input_runtime_keys(&mut required_runtime_keys);
        warmup_ind_estimate(source_path).unwrap_or(0)
    };
    let needs_history = data_mode == TemplateRuntimeDataMode::History
        || template_warmup_need > 0
        || indicator_warmup_need > 0
        || uses_rank_score
        || uses_similarity_rank
        || !cyq_chen_runtime_keys.is_empty();
    let need_rows = template_warmup_need.max(indicator_warmup_need).max(1);
    let ts_codes = stocks
        .iter()
        .map(|item| item.ts_code.clone())
        .collect::<Vec<_>>();

    let total_share_map = load_total_share_map(source_path).unwrap_or_default();
    let warning_messages = template_order
        .iter()
        .filter_map(|template_id| match compiled_templates.get(template_id) {
            Some(CompiledIntradayMonitorTemplate::Invalid { name, message }) => {
                Some(format!("{name}: {message}"))
            }
            _ => None,
        })
        .collect::<Vec<_>>();

    let has_ready_template = compiled_templates
        .values()
        .any(|item| matches!(item, CompiledIntradayMonitorTemplate::Ready(_)));
    if !has_ready_template || !needs_history {
        let entry = Arc::new(AllMarketTemplateRuntimeCacheEntry {
            cache_key: lookup_key.clone(),
            cache_prefix: lookup_prefix.clone(),
            template_order,
            compiled_templates,
            base_rows: HashMap::new(),
            indicator_cache,
            total_share_map,
            cyq_chen_runtime_keys,
            warning_messages,
        });
        let mut cache = template_runtime_cache()
            .lock()
            .map_err(|_| "模板预热缓存锁已损坏".to_string())?;
        cache.retain(|key, _| !key.starts_with(&lookup_prefix));
        cache.insert(lookup_key, entry.clone());
        return Ok(entry);
    }

    let reader = DataReader::new_with_runtime_keys(source_path, &required_runtime_keys)
        .map_err(|e| format!("模板预热初始化日K读取器失败: {e}"))?;
    let (start_date, end_date) =
        (|conn: &Connection, need_rows: usize| -> Result<(String, String), String> {
            let mut stmt = conn
                .prepare(
                    r#"
            SELECT trade_date
            FROM (
                SELECT DISTINCT trade_date
                FROM stock_data
                WHERE adj_type = ?
                ORDER BY trade_date DESC
                LIMIT ?
            ) recent_dates
            ORDER BY trade_date ASC
            "#,
                )
                .map_err(|e| format!("预编译模板预热交易日窗口失败: {e}"))?;
            let mut rows = stmt
                .query(params![DEFAULT_ADJ_TYPE, need_rows.max(1) as i64])
                .map_err(|e| format!("查询模板预热交易日窗口失败: {e}"))?;
            let mut dates = Vec::new();

            while let Some(row) = rows
                .next()
                .map_err(|e| format!("读取模板预热交易日窗口失败: {e}"))?
            {
                let trade_date: String = row
                    .get(0)
                    .map_err(|e| format!("读取模板预热交易日失败: {e}"))?;
                if !trade_date.trim().is_empty() {
                    dates.push(trade_date);
                }
            }

            let start_date = dates
                .first()
                .cloned()
                .ok_or_else(|| "stock_data 没有可用于模板预热的交易日".to_string())?;
            let end_date = dates
                .last()
                .cloned()
                .ok_or_else(|| "stock_data 没有可用于模板预热的交易日".to_string())?;
            Ok((start_date, end_date))
        })(&reader.conn, need_rows)?;
    let rank_score_map = if uses_rank_score {
        (|conn: &Connection,
          start_date: &str,
          end_date: &str|
         -> Result<HashMap<String, HashMap<String, TemplateRankScoreInfo>>, String> {
            let mut stmt = conn
                .prepare(
                    r#"
            SELECT ts_code, trade_date, rank, total_score
            FROM score_summary
            WHERE trade_date >= ?
              AND trade_date <= ?
            "#,
                )
                .map_err(|e| format!("预编译全市场模板总榜序列失败: {e}"))?;
            let mut rows = stmt
                .query(params![start_date, end_date])
                .map_err(|e| format!("查询全市场模板总榜序列失败: {e}"))?;
            let mut out = HashMap::<String, HashMap<String, TemplateRankScoreInfo>>::new();

            while let Some(row) = rows
                .next()
                .map_err(|e| format!("读取全市场模板总榜序列失败: {e}"))?
            {
                let ts_code: String = row
                    .get(0)
                    .map_err(|e| format!("读取全市场模板总榜代码失败: {e}"))?;
                let trade_date: String = row
                    .get(1)
                    .map_err(|e| format!("读取全市场模板总榜日期失败: {e}"))?;
                let rank = row
                    .get::<_, Option<i64>>(2)
                    .map_err(|e| format!("读取全市场模板总榜排名失败: {e}"))?
                    .map(|value| value as f64);
                let score = row
                    .get::<_, Option<f64>>(3)
                    .map_err(|e| format!("读取全市场模板总榜得分失败: {e}"))?;
                if ts_code.trim().is_empty() || trade_date.trim().is_empty() {
                    continue;
                }
                out.entry(ts_code)
                    .or_default()
                    .insert(trade_date, TemplateRankScoreInfo { rank, score });
            }

            Ok(out)
        })(&open_result_conn(source_path)?, &start_date, &end_date)
        .unwrap_or_default()
    } else {
        HashMap::new()
    };
    let raw_rows = reader.load_batch(&ts_codes, DEFAULT_ADJ_TYPE, &start_date, &end_date)?;
    let stock_board_map = stocks
        .iter()
        .map(|item| (item.ts_code.as_str(), item.board.as_str()))
        .collect::<HashMap<_, _>>();
    let cyq_injector = CyqChenFieldInjector::new(source_path, &cyq_chen_runtime_keys);
    let similarity_rank_injector =
        SimilarityRankFieldInjector::new(source_path, uses_similarity_rank);
    let mut base_rows = HashMap::with_capacity(raw_rows.len());

    for (ts_code, row_data) in raw_rows {
        let mut row_data = normalize_runtime_row_data(row_data)?;
        let is_st = stock_board_map
            .get(ts_code.as_str())
            .map(|board| board.trim() == "ST")
            .unwrap_or(false);
        inject_stock_extra_fields(
            &mut row_data,
            &ts_code,
            is_st,
            total_share_map.get(&ts_code).copied(),
        )?;
        inject_template_rank_score_series(&mut row_data, rank_score_map.get(&ts_code))?;
        if uses_similarity_rank {
            similarity_rank_injector.inject(&mut row_data, &ts_code)?;
        }
        let _ = cyq_injector.inject(&mut row_data, &ts_code);
        row_data.validate()?;
        base_rows.insert(ts_code, row_data);
    }

    let entry = Arc::new(AllMarketTemplateRuntimeCacheEntry {
        cache_key: lookup_key.clone(),
        cache_prefix: lookup_prefix.clone(),
        template_order,
        compiled_templates,
        base_rows,
        indicator_cache,
        total_share_map,
        cyq_chen_runtime_keys,
        warning_messages,
    });
    let mut cache = template_runtime_cache()
        .lock()
        .map_err(|_| "模板预热缓存锁已损坏".to_string())?;
    cache.retain(|key, _| !key.starts_with(&lookup_prefix));
    cache.insert(lookup_key, entry.clone());
    Ok(entry)
}

fn build_template_runtime_row_data(
    entry: &AllMarketTemplateRuntimeCacheEntry,
    row: &AllMarketMonitorRow,
    quote: &SinaQuote,
) -> Result<RowData, String> {
    let trade_date = if let Some(value) = row
        .realtime_trade_date
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        value.to_string()
    } else {
        normalize_quote_trade_date(&quote.date)
            .ok_or_else(|| format!("{} 缺少实时行情日期", row.ts_code))?
    };

    let mut row_data = if let Some(base_row) = entry.base_rows.get(&row.ts_code) {
        let mut row_data = base_row.clone();
        merge_realtime_quote_into_row_data(&mut row_data, quote, &trade_date)?;
        clear_latest_template_rank_score(&mut row_data);
        row_data
    } else {
        let mut row_data = build_quote_only_runtime_row_data(quote, &trade_date)?;
        inject_template_rank_score_series(&mut row_data, None)?;
        inject_empty_optional_cyq_chen_fields(&mut row_data, &entry.cyq_chen_runtime_keys)?;
        row_data
    };

    inject_stock_extra_fields(
        &mut row_data,
        &row.ts_code,
        row.board.trim() == "ST",
        entry.total_share_map.get(&row.ts_code).copied(),
    )?;
    inject_latest_num_fields(
        &mut row_data,
        &[
            (RT_OPEN_CHANGE_PCT, row.realtime_change_open_pct),
            (
                RT_FALL_FROM_HIGH_PCT,
                (|quote: &SinaQuote| -> Option<f64> {
                    if quote.high > 0.0 {
                        Some(((quote.high - quote.price) / quote.high).max(0.0) * 100.0)
                    } else {
                        None
                    }
                })(quote),
            ),
            (RT_VOLUME_RATIO, row.realtime_vol_ratio),
            (
                RT_OPENING_VOLUME_RATIO_HIGH,
                row.realtime_vol_ratio_opening_high,
            ),
            (RT_AVERAGE_PRICE, row.realtime_avg_price),
            ("S_RANK", row.similarity_rank.map(|value| value as f64)),
        ],
    )?;

    if !entry.indicator_cache.is_empty() {
        for (name, series) in calc_inds_with_cache_lossy(&entry.indicator_cache, &row_data) {
            row_data.cols.insert(name, series);
        }
    }

    row_data.validate()?;
    Ok(row_data)
}

fn eval_other_sort_value(
    runtime: &Runtime,
    ast: &crate::expr::parser::Stmts,
) -> Result<Option<f64>, String> {
    let mut rt = runtime.clone();
    let value = rt
        .eval_program(ast)
        .map_err(|error| format!("表达式计算错误:{}", error.msg))?;
    let len = rt_max_len(&rt);
    (|value: &Value, len: usize| -> Result<Option<f64>, String> {
        match value {
            Value::Num(value) => Ok(value.is_finite().then_some(*value)),
            Value::Bool(value) => Ok(Some(if *value { 1.0 } else { 0.0 })),
            Value::NumSeries(_) | Value::SharedNumSeries(_) => {
                let series = Value::as_num_series(value, len)
                    .map_err(|error| format!("表达式返回值非数值:{}", error.msg))?;
                Ok(series
                    .last()
                    .copied()
                    .flatten()
                    .filter(|value| value.is_finite()))
            }
            Value::BoolSeries(_) => {
                let series = Value::as_bool_series(value, len)
                    .map_err(|error| format!("表达式返回值非布尔:{}", error.msg))?;
                Ok(series.last().map(|value| if *value { 1.0 } else { 0.0 }))
            }
        }
    })(&value, len)
}

fn build_other_sort_template(expression: &str) -> IntradayMonitorTemplate {
    IntradayMonitorTemplate {
        id: "__all_market_other_sort__".to_string(),
        name: "其他排序".to_string(),
        expression: expression.trim().to_string(),
    }
}

fn build_other_sort_warning(messages: Vec<String>) -> Option<String> {
    let normalized = messages
        .into_iter()
        .map(|message| message.trim().to_string())
        .filter(|message| !message.is_empty())
        .collect::<Vec<_>>();

    match normalized.as_slice() {
        [] => None,
        [first] => Some(format!("其他排序异常: {first}")),
        [first, ..] => Some(format!(
            "其他排序异常共 {} 条，首条: {}",
            normalized.len(),
            first
        )),
    }
}

fn build_rows(
    meta: &SourceMetaCacheEntry,
    ranks: &HashMap<String, RankContext>,
    quotes: &HashMap<String, SinaQuote>,
    volume_ratio_map: &HashMap<String, f64>,
    avg_price_map: &HashMap<String, f64>,
    return_5d_map: &HashMap<String, DailyBaseline>,
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
                industry: stock.industry.clone(),
                concept: stock.concept.clone(),
                rank: rank.and_then(|item| item.rank),
                best_rank_3d: rank.and_then(|item| item.best_rank_3d),
                similarity_rank: rank.and_then(|item| item.similarity_rank),
                total_score: rank.and_then(|item| item.total_score),
                realtime_trade_date: quote.and_then(|item| normalize_quote_trade_date(&item.date)),
                realtime_price: quote.map(|item| item.price),
                realtime_open: quote.map(|item| item.open),
                realtime_high: quote.map(|item| item.high),
                realtime_low: quote.map(|item| item.low),
                realtime_pre_close: quote.map(|item| item.pre_close),
                realtime_avg_price: avg_price_map.get(&stock.ts_code).copied(),
                realtime_change_pct: quote.and_then(|item| item.change_pct),
                realtime_change_open_pct: quote.and_then(|quote: &SinaQuote| -> Option<f64> {
                    if quote.pre_close > 0.0 && quote.open.is_finite() && quote.price.is_finite() {
                        Some((quote.price - quote.open) / quote.pre_close * 100.0)
                    } else {
                        None
                    }
                }),
                realtime_vol: quote.map(|item| item.vol),
                realtime_amount: quote.map(|item| item.amount),
                realtime_vol_ratio: volume_ratio_map.get(&stock.ts_code).copied(),
                realtime_vol_ratio_high: None,
                realtime_vol_ratio_opening_high: None,
                return_5d_pct,
                other_sort_value: None,
                scene_marker: scene_marker_map.get(&stock.ts_code).cloned(),
                template_hits: None,
                total_mv_yi: stock.total_mv_yi,
                refreshed_at: fetch_meta.refreshed_at.clone(),
            }
        })
        .collect()
}

fn has_new_realtime_data(last_data_version: Option<&str>, data_version: Option<&str>) -> bool {
    let last_data_version = last_data_version
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let data_version = data_version
        .map(str::trim)
        .filter(|value| !value.is_empty());

    match (last_data_version, data_version) {
        (_, None) => false,
        (None, Some(_)) => true,
        (Some(last), Some(current)) => current > last,
    }
}

pub fn get_all_market_monitor_snapshot(
    source_path: &str,
    realtime_provider: Option<String>,
    scene_stage_threshold: Option<String>,
    template_enabled: Option<bool>,
    templates: Option<Vec<IntradayMonitorTemplate>>,
    ts_codes: Option<Vec<String>>,
    other_sort_expression: Option<String>,
    other_sort_use_realtime: Option<bool>,
    last_data_version: Option<String>,
    volume_ratio_fetch_enabled: Option<bool>,
) -> Result<AllMarketMonitorSnapshotData, String> {
    let mut meta = (|source_path: &str| -> Result<Arc<SourceMetaCacheEntry>, String> {
        if let Some(entry) = source_meta_cache()
            .lock()
            .map_err(|_| "股票基础信息缓存锁已损坏".to_string())?
            .get(source_path)
            .cloned()
        {
            return Ok(entry);
        }

        let entry = (|source_path: &str| -> Result<SourceMetaCacheEntry, String> {
            let mut stocks = Vec::new();
            let (concepts_map, warning) = match build_concepts_map(source_path) {
                Ok(concepts) => (concepts, None),
                Err(error) => (
                    HashMap::new(),
                    Some(format!("读取概念资料失败，概念榜暂不可用: {error}")),
                ),
            };
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
                    industry: cols
                        .get(4)
                        .map(|value| value.trim())
                        .unwrap_or_default()
                        .to_string(),
                    total_mv_yi: (|raw: Option<&String>| -> Option<f64> {
                        raw?.trim()
                            .parse::<f64>()
                            .ok()
                            .filter(|value| value.is_finite())
                            .map(|value| value / 1e4)
                    })(cols.get(9)),
                    concept: concepts_map.get(ts_code).cloned().unwrap_or_default(),
                });
            }
            Ok(SourceMetaCacheEntry { stocks, warning })
        })(source_path)?;
        let entry = Arc::new(entry);
        if entry.warning.is_none() {
            source_meta_cache()
                .lock()
                .map_err(|_| "股票基础信息缓存锁已损坏".to_string())?
                .insert(source_path.to_string(), entry.clone());
        }
        Ok(entry)
    })(source_path)?;
    if let Some(ref codes) = ts_codes {
        let code_set: HashSet<&str> = codes.iter().map(|s| s.as_str()).collect();
        Arc::make_mut(&mut meta)
            .stocks
            .retain(|s| code_set.contains(s.ts_code.as_str()));
    }
    let ts_codes = meta
        .stocks
        .iter()
        .map(|item| item.ts_code.clone())
        .collect::<Vec<_>>();
    let provider = RealtimeQuoteProvider::parse(realtime_provider.as_deref())?;
    let (quotes, mut volume_ratio_map, avg_price_map, fetch_meta) = match provider {
        RealtimeQuoteProvider::Sina => {
            let (quotes, fetch_meta) = fetch_all_market_realtime_quote_map_for_codes(&ts_codes)?;
            (quotes, HashMap::new(), HashMap::new(), fetch_meta)
        }
        RealtimeQuoteProvider::Tencent => {
            let (tencent_quotes, fetch_meta) =
                fetch_all_market_tencent_realtime_quote_map_for_codes(&ts_codes)?;
            let mut volume_ratio_map = HashMap::with_capacity(tencent_quotes.len());
            let mut avg_price_map = HashMap::with_capacity(tencent_quotes.len());
            let mut quotes = HashMap::with_capacity(tencent_quotes.len());
            for (ts_code, quote) in tencent_quotes {
                if let Some(value) = quote.volume_ratio {
                    volume_ratio_map.insert(ts_code.clone(), value);
                }
                if let Some(value) = quote.avg_price {
                    avg_price_map.insert(ts_code.clone(), value);
                }
                quotes.insert(ts_code, quote.into_sina_quote());
            }
            (quotes, volume_ratio_map, avg_price_map, fetch_meta)
        }
    };

    let now = Utc::now()
        .with_timezone(&FixedOffset::east_opt(8 * 3600).expect("valid China offset"))
        .naive_local();
    let (market_active, market_status_message) = check_market_activity(now, &quotes, || {
        let today = now.format("%Y%m%d").to_string();
        load_trade_date_list(source_path).ok().map(|dates| {
            dates
                .iter()
                .any(|date| normalize_quote_trade_date(date).as_deref() == Some(today.as_str()))
        })
    });
    // 手动补齐可以在休市后拉取行情所带日期；分时响应仍严格校验日期。
    let volume_ratios = if (market_active || volume_ratio_fetch_enabled.unwrap_or(false))
        && matches!(provider, RealtimeQuoteProvider::Tencent)
    {
        fetch_meta
            .quote_trade_date
            .as_deref()
            .map(|date| super::volume_ratio::snapshot(source_path, &quotes, date))
            .transpose()?
    } else {
        None
    };
    if let Some(snapshot) = &volume_ratios {
        for (code, value) in &snapshot.values {
            volume_ratio_map.insert(code.clone(), value.current);
        }
    }
    let data_version = fetch_meta.refreshed_at.as_ref().map(|version| {
        format!(
            "{version}|volume-ratio:{}",
            volume_ratios
                .as_ref()
                .map_or(0, |snapshot| snapshot.revision)
        )
    });
    let mut volume_ratio_status_message = volume_ratios
        .as_ref()
        .and_then(|snapshot| snapshot.status.clone());
    if let Some(warning) = &meta.warning {
        volume_ratio_status_message = Some(match volume_ratio_status_message {
            Some(message) => format!("{message}；{warning}"),
            None => warning.clone(),
        });
    }
    if !has_new_realtime_data(last_data_version.as_deref(), data_version.as_deref()) {
        return Ok(AllMarketMonitorSnapshotData {
            rows: Vec::new(),
            index_rows: Vec::new(),
            data_version,
            has_new_data: false,
            market_active,
            market_status_message,
            refreshed_at: fetch_meta.refreshed_at,
            rank_date: None,
            requested_count: fetch_meta.requested_count,
            fetched_count: fetch_meta.fetched_count,
            template_warning_message: None,
            volume_ratio_status_message,
        });
    }

    let rank_context = (|| -> Result<RankCacheEntry, String> {
        let mut cache = rank_context_cache()
            .lock()
            .map_err(|_| "排名与场景缓存锁已损坏".to_string())?;
        if let Some(entry) = cache.get(source_path) {
            return Ok(entry.clone());
        }
        let entry = load_rank_context(&open_result_conn(source_path)?)?;
        if !entry.rank_date.is_empty() {
            cache.insert(source_path.to_string(), entry.clone());
        }
        Ok(entry)
    })()?;
    let scene_marker_map = (|candidates: &HashMap<String, Vec<SceneMarkerCandidate>>,
                             threshold_level: i32|
     -> HashMap<String, String> {
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
    })(
        &rank_context.scene_candidates,
        parse_scene_stage_threshold(scene_stage_threshold.as_deref()),
    );
    let daily_baselines = daily_baseline::load(
        source_path,
        fetch_meta
            .quote_trade_date
            .as_deref()
            .unwrap_or(&now.format("%Y%m%d").to_string()),
    );
    let daily_baselines = match daily_baselines {
        Ok(baselines) => baselines,
        Err(error) => {
            let warning = format!("读取本地日线基准失败，五日涨幅暂不可用: {error}");
            volume_ratio_status_message = Some(match volume_ratio_status_message {
                Some(message) => format!("{message}；{warning}"),
                None => warning,
            });
            Default::default()
        }
    };
    let mut rows = build_rows(
        &meta,
        &rank_context.ranks,
        &quotes,
        &volume_ratio_map,
        &avg_price_map,
        &daily_baselines.rows,
        &scene_marker_map,
        &fetch_meta,
    );
    if let Some(snapshot) = &volume_ratios {
        for row in &mut rows {
            if let Some(value) = snapshot.values.get(&row.ts_code) {
                row.realtime_vol_ratio_high = Some(value.high);
                row.realtime_vol_ratio_opening_high = value.opening_high;
            }
        }
    }
    let template_warning_message = if template_enabled.unwrap_or(false) {
        (|source_path: &str,
          stocks: &[StockMeta],
          rows: &mut [AllMarketMonitorRow],
          quotes: &HashMap<String, SinaQuote>,
          templates: &[IntradayMonitorTemplate]|
         -> Option<String> {
            for row in rows.iter_mut() {
                row.template_hits = Some(Vec::new());
            }

            if templates.is_empty() {
                return None;
            }

            let entry = match cached_template_runtime(
                source_path,
                stocks,
                templates,
                TemplateRuntimeDataMode::QuoteOnly,
            ) {
                Ok(entry) => entry,
                Err(err) => {
                    return Some(format!("模板计算异常: runtime 初始化失败: {err}"));
                }
            };

            let mut warning_messages = entry.warning_messages.clone();

            let ready_templates = entry
                .template_order
                .iter()
                .filter_map(|id| match entry.compiled_templates.get(id) {
                    Some(CompiledIntradayMonitorTemplate::Ready(template)) => Some((id, template)),
                    _ => None,
                })
                .collect::<Vec<_>>();
            if ready_templates.is_empty() {
                return build_template_warning_message(warning_messages);
            }

            // 按股票完成构建和全部模板求值，工作线程处理完就释放 runtime。
            let eval_warnings = rows
                .par_iter_mut()
                .filter_map(|row| {
                    let quote = quotes.get(&row.ts_code)?;
                    let row_data = match build_template_runtime_row_data(&entry, row, quote) {
                        Ok(data) => data,
                        Err(error) => return Some(vec![format!("{}: {}", row.ts_code, error)]),
                    };
                    let series_len = row_data.trade_dates.len();
                    let mut runtime = match row_into_rt(row_data) {
                        Ok(runtime) => runtime,
                        Err(error) => {
                            return Some(vec![format!(
                                "{}: runtime 构建失败: {}",
                                row.ts_code, error
                            )]);
                        }
                    };
                    freeze_runtime_series(&mut runtime);
                    let mut warnings = Vec::new();
                    for (id, template) in &ready_templates {
                        let hit = (|| -> Result<bool, String> {
                            if let Some(hit) = runtime
                                .eval_program_bool_at(&template.ast, series_len.saturating_sub(1))
                                .map_err(|error| format!("表达式计算错误:{}", error.msg))?
                            {
                                return Ok(hit);
                            }
                            // 含函数的程序仍按完整序列求值，各模板赋值互相隔离。
                            let mut rt = runtime.clone();
                            let value = rt
                                .eval_program(&template.ast)
                                .map_err(|error| format!("表达式计算错误:{}", error.msg))?;
                            let len = rt_max_len(&rt);
                            let series = Value::as_bool_series(&value, len)
                                .map_err(|error| format!("表达式返回值非布尔:{}", error.msg))?;
                            Ok(series.last().copied().unwrap_or(false))
                        })();
                        match hit {
                            Ok(true) => {
                                row.template_hits
                                    .as_mut()
                                    .unwrap()
                                    .push(AllMarketTemplateHit {
                                        template_id: (*id).clone(),
                                        template_name: template.name.clone(),
                                    })
                            }
                            Ok(false) => {}
                            Err(error) => warnings
                                .push(format!("{}: {} · {}", row.ts_code, template.name, error)),
                        }
                    }
                    Some(warnings)
                })
                .collect::<Vec<_>>();
            warning_messages.extend(eval_warnings.into_iter().flatten());

            build_template_warning_message(warning_messages)
        })(
            source_path,
            &meta.stocks,
            &mut rows,
            &quotes,
            templates.as_deref().unwrap_or(&[]),
        )
    } else {
        None
    };
    let other_sort_warning_message = (|source_path: &str,
                                       stocks: &[StockMeta],
                                       rows: &mut [AllMarketMonitorRow],
                                       quotes: &HashMap<String, SinaQuote>,
                                       expression: Option<&str>,
                                       use_realtime: bool|
     -> Option<String> {
        let expression = expression
            .map(str::trim)
            .filter(|value| !value.is_empty())?;
        if !use_realtime {
            return match (|source_path: &str,
                           stocks: &[StockMeta],
                           expression: &str|
             -> Result<
                (Arc<HashMap<String, Option<f64>>>, Option<String>),
                String,
            > {
                let template = build_other_sort_template(expression);
                let templates = vec![template.clone()];
                let entry = cached_template_runtime(
                    source_path,
                    stocks,
                    &templates,
                    TemplateRuntimeDataMode::History,
                )?;
                let lookup_key = format!("{}|other-sort-daily", entry.cache_key);
                if let Some(values) = other_sort_daily_value_cache()
                    .lock()
                    .map_err(|_| "其他排序日K缓存锁已损坏".to_string())?
                    .get(&lookup_key)
                    .cloned()
                {
                    return Ok((
                        values,
                        build_other_sort_warning(entry.warning_messages.clone()),
                    ));
                }

                let Some(CompiledIntradayMonitorTemplate::Ready(compiled)) =
                    entry.compiled_templates.get(&template.id)
                else {
                    return Ok((
                        Arc::new(HashMap::new()),
                        build_other_sort_warning(entry.warning_messages.clone()),
                    ));
                };

                let eval_results = stocks
                    .par_iter()
                    .filter_map(|stock| {
                        let base_row = entry.base_rows.get(&stock.ts_code)?;
                        let mut row_data = base_row.clone();
                        if !entry.indicator_cache.is_empty() {
                            for (name, series) in
                                calc_inds_with_cache_lossy(&entry.indicator_cache, &row_data)
                            {
                                row_data.cols.insert(name, series);
                            }
                        }
                        let mut runtime =
                            match row_data.validate().and_then(|_| row_into_rt(row_data)) {
                                Ok(runtime) => runtime,
                                Err(error) => {
                                    return Some(Err(format!(
                                        "{}: runtime 构建失败: {}",
                                        stock.ts_code, error
                                    )));
                                }
                            };
                        freeze_runtime_series(&mut runtime);
                        Some(
                            eval_other_sort_value(&runtime, &compiled.ast)
                                .map(|value| (stock.ts_code.clone(), value))
                                .map_err(|error| format!("{}: {}", stock.ts_code, error)),
                        )
                    })
                    .collect::<Vec<_>>();

                let mut values = HashMap::new();
                let mut warnings = entry.warning_messages.clone();
                for result in eval_results {
                    match result {
                        Ok((ts_code, value)) => {
                            values.insert(ts_code, value);
                        }
                        Err(error) => warnings.push(error),
                    }
                }

                let values = Arc::new(values);
                let mut cache = other_sort_daily_value_cache()
                    .lock()
                    .map_err(|_| "其他排序日K缓存锁已损坏".to_string())?;
                cache.retain(|key, _| !key.starts_with(&entry.cache_prefix));
                cache.insert(lookup_key, values.clone());
                Ok((values, build_other_sort_warning(warnings)))
            })(source_path, stocks, expression)
            {
                Ok((values, warning)) => {
                    for row in rows.iter_mut() {
                        row.other_sort_value = values.get(&row.ts_code).copied().flatten();
                    }
                    warning
                }
                Err(error) => Some(format!("其他排序异常: {error}")),
            };
        }

        let template = build_other_sort_template(expression);
        let templates = vec![template.clone()];
        let entry = match cached_template_runtime(
            source_path,
            stocks,
            &templates,
            TemplateRuntimeDataMode::QuoteOnly,
        ) {
            Ok(entry) => entry,
            Err(error) => return Some(format!("其他排序异常: runtime 初始化失败: {error}")),
        };

        let Some(CompiledIntradayMonitorTemplate::Ready(compiled)) =
            entry.compiled_templates.get(&template.id)
        else {
            return build_other_sort_warning(entry.warning_messages.clone());
        };

        let build_results = rows
            .par_iter()
            .enumerate()
            .filter_map(|(row_index, row)| {
                let quote = quotes.get(&row.ts_code)?;
                let row_data = match build_template_runtime_row_data(&entry, row, quote) {
                    Ok(row_data) => row_data,
                    Err(error) => return Some(Err(format!("{}: {}", row.ts_code, error))),
                };
                let mut runtime = match row_into_rt(row_data) {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        return Some(Err(format!("{}: runtime 构建失败: {}", row.ts_code, error)));
                    }
                };
                freeze_runtime_series(&mut runtime);
                Some(
                    eval_other_sort_value(&runtime, &compiled.ast)
                        .map(|value| (row_index, value))
                        .map_err(|error| format!("{}: {}", row.ts_code, error)),
                )
            })
            .collect::<Vec<_>>();

        let mut warnings = entry.warning_messages.clone();
        for result in build_results {
            match result {
                Ok((row_index, value)) => rows[row_index].other_sort_value = value,
                Err(error) => warnings.push(error),
            }
        }

        build_other_sort_warning(warnings)
    })(
        source_path,
        &meta.stocks,
        &mut rows,
        &quotes,
        other_sort_expression.as_deref(),
        other_sort_use_realtime.unwrap_or(true),
    );
    let template_warning_message = match (template_warning_message, other_sort_warning_message) {
        (Some(left), Some(right)) => Some(format!("{left}；{right}")),
        (Some(message), None) | (None, Some(message)) => Some(message),
        (None, None) => None,
    };
    let index_rows = (|provider: RealtimeQuoteProvider| -> Vec<AllMarketIndexRow> {
        let index_ts_codes = default_realtime_index_ts_codes();
        let quote_result = match provider {
            RealtimeQuoteProvider::Sina => {
                fetch_all_market_realtime_quote_map_for_codes(&index_ts_codes)
                    .map(|(quotes, _)| quotes)
            }
            RealtimeQuoteProvider::Tencent => {
                fetch_all_market_tencent_realtime_quote_map_for_codes(&index_ts_codes).map(
                    |(quotes, _)| {
                        quotes
                            .into_iter()
                            .map(|(ts_code, quote)| (ts_code, quote.into_sina_quote()))
                            .collect::<HashMap<_, _>>()
                    },
                )
            }
        };

        quote_result
            .map(|quotes| {
                (|quotes: &HashMap<String, SinaQuote>| -> Vec<AllMarketIndexRow> {
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
                                realtime_trade_date: quote
                                    .and_then(|item| normalize_quote_trade_date(&item.date)),
                                realtime_price: quote.map(|item| item.price),
                                realtime_change_pct: quote.and_then(|item| item.change_pct),
                                realtime_amount: quote
                                    .map(|item| item.amount)
                                    .filter(|value| value.is_finite() && *value >= 0.0),
                            }
                        })
                        .collect()
                })(&quotes)
            })
            .unwrap_or_else(|_| Vec::new())
    })(provider.opposite());

    Ok(AllMarketMonitorSnapshotData {
        rows,
        index_rows,
        data_version,
        has_new_data: true,
        market_active,
        market_status_message,
        refreshed_at: fetch_meta.refreshed_at,
        rank_date: (!rank_context.rank_date.is_empty()).then_some(rank_context.rank_date),
        requested_count: fetch_meta.requested_count,
        fetched_count: fetch_meta.fetched_count,
        template_warning_message,
        volume_ratio_status_message,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        expr::parser::{Parser, lex_all},
        intraday_monitor::compile_intraday_templates,
    };

    fn parse_program(expr: &str) -> Stmts {
        let mut parser = Parser::new(lex_all(expr));
        parser.parse_main().expect("expression should parse")
    }

    #[test]
    fn combined_ranks_and_scenes_preserve_dates_and_independent_records() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE score_summary(trade_date VARCHAR, ts_code VARCHAR, rank BIGINT, total_score DOUBLE);
            CREATE TABLE scene_details(trade_date VARCHAR, ts_code VARCHAR, scene_name VARCHAR,
                stage VARCHAR, scene_rank BIGINT);
            INSERT INTO score_summary VALUES
                ('20261009','a',1,999), ('20261010','a',10,100),
                ('20261011','a',30,110), ('20261012','a',50,120),
                ('20261009','old',2,200), ('20261011','previous',8,80);
            INSERT INTO scene_details VALUES
                ('20261012','a',' observe ','observe',2),
                ('20261012','a','confirmed','confirm',9),
                ('20261012','scene_only','triggered','trigger',3),
                ('20261011','a','old_scene','confirm',1),
                ('20261012','a','  ','confirm',1);",
        ).unwrap();
        for similarity_board in [false, true] {
            if similarity_board {
                conn.execute_batch(
                    "ATTACH ':memory:' AS similarity_db;
                    CREATE TABLE similarity_db.strategy_trigger_similarity_summary
                        (trade_date VARCHAR, ts_code VARCHAR, rank BIGINT);
                    INSERT INTO similarity_db.strategy_trigger_similarity_summary VALUES
                        ('20261012','a',99), ('20261013','a',7),
                        ('20261013','similarity_only',1);",
                )
                .unwrap();
            }
            let context = load_rank_context(&conn).unwrap();
            assert_eq!(context.rank_date, "20261012");
            assert_eq!(context.ranks["a"].rank, Some(50));
            assert_eq!(context.ranks["a"].best_rank_3d, Some(10));
            assert_eq!(context.ranks["a"].total_score, Some(120.0));
            assert_eq!(
                context.ranks["a"].similarity_rank,
                similarity_board.then_some(7)
            );
            assert_eq!(context.ranks["previous"].rank, None);
            assert_eq!(context.ranks["previous"].best_rank_3d, Some(8));
            assert!(!context.ranks.contains_key("old"));
            assert!(!context.ranks.contains_key("scene_only"));
            assert_eq!(context.scene_candidates["a"].len(), 2);
            let observed = context.scene_candidates["a"]
                .iter()
                .find(|candidate| candidate.scene_name == "observe")
                .unwrap();
            assert_eq!(observed.stage_level, 1);
            assert_eq!(observed.scene_rank, Some(2));
            assert_eq!(
                context.scene_candidates["scene_only"][0].scene_name,
                "triggered"
            );
            if similarity_board {
                assert_eq!(context.ranks["similarity_only"].rank, None);
                assert_eq!(context.ranks["similarity_only"].similarity_rank, Some(1));
            }
        }
        conn.execute_batch("DELETE FROM score_summary; DELETE FROM scene_details;")
            .unwrap();
        let context = load_rank_context(&conn).unwrap();
        assert!(context.rank_date.is_empty());
        assert!(context.scene_candidates.is_empty());
        assert_eq!(context.ranks["a"].similarity_rank, Some(7));
    }

    #[test]
    fn market_activity_requires_session_and_fresh_quote_majority() {
        let quote = SinaQuote {
            date: "20261009".to_string(),
            time: "10:00:00".to_string(),
            ts_code: "000001.SZ".to_string(),
            name: "test".to_string(),
            open: 10.0,
            high: 10.0,
            low: 10.0,
            pre_close: 10.0,
            price: 10.0,
            vol: 100.0,
            amount: 1000.0,
            change_pct: Some(0.0),
        };
        for (now, date, time, calendar, expected) in [
            (
                "20261009 10:01:00",
                "20261009",
                "10:00:00",
                Some(true),
                true,
            ),
            (
                "20261009 10:01:00",
                "20261009",
                "10:00:00",
                Some(false),
                true,
            ),
            ("20261009 10:01:00", "20261009", "10:00:00", None, true),
            (
                "20261009 09:29:59",
                "20261009",
                "09:29:59",
                Some(true),
                false,
            ),
            (
                "20261009 09:30:00",
                "20261009",
                "09:30:00",
                Some(true),
                true,
            ),
            (
                "20261009 11:30:00",
                "20261009",
                "11:30:00",
                Some(true),
                false,
            ),
            (
                "20261009 13:00:00",
                "20261009",
                "11:30:00",
                Some(true),
                false,
            ),
            (
                "20261009 13:00:00",
                "20261009",
                "13:00:00",
                Some(true),
                true,
            ),
            (
                "20261009 15:00:00",
                "20261009",
                "15:00:00",
                Some(true),
                false,
            ),
            (
                "20261010 10:01:00",
                "20261009",
                "10:00:00",
                Some(true),
                false,
            ),
            (
                "20261009 10:04:00",
                "20261009",
                "10:00:00",
                Some(true),
                false,
            ),
            (
                "20261009 10:01:00",
                "20261009",
                "10:02:00",
                Some(true),
                false,
            ),
        ] {
            let now = NaiveDateTime::parse_from_str(now, "%Y%m%d %H:%M:%S").unwrap();
            let mut quote = quote.clone();
            quote.date = date.to_string();
            quote.time = time.to_string();
            let quotes = HashMap::from([(quote.ts_code.clone(), quote)]);
            assert_eq!(
                check_market_activity(now, &quotes, || calendar).0,
                expected,
                "{now}"
            );
        }
        let now = NaiveDateTime::parse_from_str("20261009 10:01:00", "%Y%m%d %H:%M:%S").unwrap();
        assert!(!check_market_activity(now, &HashMap::new(), || Some(true)).0);
        let mut quotes = HashMap::from([(quote.ts_code.clone(), quote.clone())]);
        let mut stale = quote.clone();
        stale.date = "20261008".to_string();
        quotes.insert("stale".to_string(), stale);
        assert!(!check_market_activity(now, &quotes, || Some(true)).0);
        quotes.insert("fresh".to_string(), quote);
        assert!(check_market_activity(now, &quotes, || Some(false)).0);
        assert!(
            check_market_activity(now, &quotes, || panic!(
                "fresh quotes must not read calendar"
            ))
            .0
        );
        let closed = NaiveDateTime::parse_from_str("20261009 15:00:00", "%Y%m%d %H:%M:%S").unwrap();
        assert!(
            !check_market_activity(closed, &quotes, || panic!(
                "closed session must not read calendar"
            ))
            .0
        );
    }

    #[test]
    fn history_dependency_detection_catches_rank_case_insensitively() {
        let program = parse_program("rank <= 100 AND RT_OP > 1");
        let rank_score_keys = HashSet::from(["RANK", "SCORE"]);
        assert!(program_uses_ident(&program, &rank_score_keys));
    }

    #[test]
    fn compiled_intraday_template_reports_ref_warmup() {
        let templates = vec![IntradayMonitorTemplate {
            id: "sort".to_string(),
            name: "其他排序".to_string(),
            expression: "REF(C, 1)".to_string(),
        }];
        let compiled = compile_intraday_templates(&templates);
        let Some(CompiledIntradayMonitorTemplate::Ready(template)) = compiled.get("sort") else {
            panic!("template should compile");
        };
        assert_eq!(template.warmup_need, 1);
    }

    #[test]
    fn clear_latest_template_rank_score_keeps_realtime_tail_empty() {
        let mut row_data = RowData {
            trade_dates: vec!["20240603".to_string(), "20240604".to_string()],
            cols: HashMap::from([
                ("RANK".to_string(), vec![Some(12.0), Some(8.0)]),
                ("SCORE".to_string(), vec![Some(88.0), Some(91.0)]),
            ]),
        };
        clear_latest_template_rank_score(&mut row_data);
        assert_eq!(row_data.cols["RANK"], vec![Some(12.0), None]);
        assert_eq!(row_data.cols["SCORE"], vec![Some(88.0), None]);
    }

    #[test]
    fn realtime_data_only_refreshes_when_version_changes() {
        assert!(!has_new_realtime_data(
            Some("20240603 09:31:00"),
            Some("20240603 09:31:00"),
        ));
        assert!(!has_new_realtime_data(
            Some("20240603 09:31:00"),
            Some("20240603 09:30:59"),
        ));
        assert!(has_new_realtime_data(
            Some("20240603 09:31:00"),
            Some("20240603 09:31:01"),
        ));
        assert!(has_new_realtime_data(None, Some("20240603 09:31:00"),));
        assert!(!has_new_realtime_data(Some("20240603 09:31:00"), None));
    }

    #[test]
    fn build_rows_keeps_unranked_and_unquoted_stocks() {
        let mut ranks = HashMap::new();
        ranks.insert(
            "000001.SZ".to_string(),
            RankContext {
                rank: Some(12),
                best_rank_3d: Some(5),
                similarity_rank: Some(3),
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

        let mut rows = build_rows(
            &(|| -> SourceMetaCacheEntry {
                SourceMetaCacheEntry {
                    stocks: vec![
                        StockMeta {
                            ts_code: "000001.SZ".to_string(),
                            name: "平安银行".to_string(),
                            board: "主板".to_string(),
                            industry: "银行".to_string(),
                            total_mv_yi: Some(100.0),
                            concept: "银行;互联金融".to_string(),
                        },
                        StockMeta {
                            ts_code: "300001.SZ".to_string(),
                            name: "特锐德".to_string(),
                            board: "创业板".to_string(),
                            industry: "电气设备".to_string(),
                            total_mv_yi: None,
                            concept: String::new(),
                        },
                    ],
                    warning: None,
                }
            })(),
            &ranks,
            &quotes,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &(|| -> RealtimeFetchMeta {
                RealtimeFetchMeta {
                    requested_count: 2,
                    effective_count: 2,
                    fetched_count: 1,
                    truncated: false,
                    refreshed_at: Some("20240603 09:31:00".to_string()),
                    quote_trade_date: Some("20240603".to_string()),
                    quote_time: Some("09:31:00".to_string()),
                }
            })(),
        );

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].rank, Some(12));
        assert_eq!(rows[0].best_rank_3d, Some(5));
        assert_eq!(rows[0].similarity_rank, Some(3));
        assert_eq!(rows[0].total_score, Some(88.5));
        assert_eq!(rows[0].concept, "银行;互联金融");
        assert_eq!(rows[0].realtime_trade_date.as_deref(), Some("20240603"));
        let change_open_pct = rows[0]
            .realtime_change_open_pct
            .expect("open change should exist");
        assert!((change_open_pct - 1.0204081632653061).abs() < 1e-9);
        assert_eq!(rows[1].rank, None);
        assert_eq!(rows[1].realtime_price, None);

        let entry = AllMarketTemplateRuntimeCacheEntry {
            cache_key: String::new(),
            cache_prefix: String::new(),
            template_order: Vec::new(),
            compiled_templates: HashMap::new(),
            base_rows: HashMap::from([(
                rows[0].ts_code.clone(),
                RowData {
                    trade_dates: vec!["20240531".to_string()],
                    cols: HashMap::new(),
                },
            )]),
            indicator_cache: Vec::new(),
            total_share_map: HashMap::new(),
            cyq_chen_runtime_keys: HashSet::new(),
            warning_messages: Vec::new(),
        };
        let program = parse_program("RT_VR > RT_VR5H");
        rows[0].realtime_vol_ratio = Some(3.0);
        for (opening_high, expected) in [(Some(2.0), true), (Some(4.0), false), (None, false)] {
            rows[0].realtime_vol_ratio_opening_high = opening_high;
            let row_data =
                build_template_runtime_row_data(&entry, &rows[0], &quotes["000001.SZ"]).unwrap();
            assert_eq!(
                row_data.cols[RT_OPENING_VOLUME_RATIO_HIGH],
                vec![None, opening_high]
            );
            let mut runtime = row_into_rt(row_data).unwrap();
            let value = runtime.eval_program(&program).unwrap();
            assert_eq!(
                Value::as_bool_series(&value, 2).unwrap(),
                vec![false, expected]
            );
        }
    }

    #[test]
    fn template_runtime_plan_is_reused_and_invalidated_by_indicator_edits() {
        let directory = std::env::temp_dir().join(format!(
            "lianghua-template-cache-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let source_path = directory.to_str().unwrap();
        let templates = vec![IntradayMonitorTemplate {
            id: "rt".to_string(),
            name: "实时条件".to_string(),
            expression: "RT_OP > 1 AND RT_VR > 2".to_string(),
        }];
        let first = cached_template_runtime(
            source_path,
            &[],
            &templates,
            TemplateRuntimeDataMode::QuoteOnly,
        )
        .unwrap();
        let second = cached_template_runtime(
            source_path,
            &[],
            &templates,
            TemplateRuntimeDataMode::QuoteOnly,
        )
        .unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        std::fs::write(ind_toml_path(source_path), "version = 1\n").unwrap();
        let revised = cached_template_runtime(
            source_path,
            &[],
            &templates,
            TemplateRuntimeDataMode::QuoteOnly,
        )
        .unwrap();
        assert!(!Arc::ptr_eq(&first, &revised));
        assert_ne!(first.cache_key, revised.cache_key);
        let cache = template_runtime_cache().lock().unwrap();
        assert_eq!(
            cache
                .keys()
                .filter(|key| key.starts_with(&first.cache_prefix))
                .count(),
            1
        );
        drop(cache);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
