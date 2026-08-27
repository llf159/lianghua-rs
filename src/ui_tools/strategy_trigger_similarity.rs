use std::collections::{HashMap, HashSet};

use duckdb::{AccessMode, Config, Connection, params, params_from_iter};
use rayon::prelude::*;
use serde::Serialize;

use crate::{
    data::{result_db_path, source_db_path},
    download::runner::INDEX_TS_CODES,
};

use super::{
    build_concepts_map, build_industry_map, build_name_map, normalize_trade_date,
    resolve_trade_date,
};

pub mod ranking;

const DEFAULT_WINDOW_TRADE_DAYS: usize = 20;
const DEFAULT_POOL_SEGMENTS: usize = 5;
const DEFAULT_OUTCOME_TRADE_DAYS: usize = 5;
const DEFAULT_LIMIT: usize = 30;
const MAX_POOL_SEGMENTS: usize = 12;
const MAX_INDICATOR_COLUMNS: usize = 24;
const MAX_CANDIDATE_ANCHORS: usize = 50_000;
// 候选池优先覆盖近期事件，同时保留跨期分散样本；不再用只含触发摘要的代理分数
// 预判包含量价、指标和市场环境的最终相似度。
const RECENT_CANDIDATE_ANCHORS: usize = 40_000;
const HISTORY_DIVERSITY_ANCHORS: usize = MAX_CANDIDATE_ANCHORS - RECENT_CANDIDATE_ANCHORS;
// 评级样本独立于页面展示条数，并限制重叠事件对有效样本量的虚增。
const RATING_SAMPLE_LIMIT: usize = 30;
const RATING_MAX_PER_OUTCOME_WINDOW: usize = 3;
// 大块读取减少 DuckDB 对 rule_details/stock_data 的重复扫描；池化后每块内存仍可控。
const ANCHOR_CHUNK_SIZE: usize = 8_192;
const SHRINKAGE_STRENGTH: f64 = 8.0;
const EPS: f64 = 1e-12;
// 两阶段候选池上进行跨期样本外复核后的精排权重；四项之和必须为 1。
const TRIGGER_SIMILARITY_WEIGHT: f64 = 0.35;
const PRICE_VOLUME_SIMILARITY_WEIGHT: f64 = 0.30;
const INDICATOR_SIMILARITY_WEIGHT: f64 = 0.15;
const MARKET_SIMILARITY_WEIGHT: f64 = 0.20;
const DEFAULT_BENCHMARK_INDEX_CODE: &str = "000001.SH";
const KERNEL_NAMES: [&str; 6] = [
    "均匀核",
    "短期指数核",
    "中期指数核",
    "近期增强核",
    "趋势对比核",
    "拐点核",
];

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyTriggerSimilarityTarget {
    pub ts_code: String,
    pub name: Option<String>,
    pub industry: Option<String>,
    pub concept: Option<String>,
    pub start_trade_date: String,
    pub end_trade_date: String,
    pub trigger_count: usize,
    pub rule_names: Vec<String>,
    pub pooled_feature_dimension: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyTriggerSimilarityOutcomeSummary {
    pub sample_count: usize,
    pub effective_sample_count: f64,
    pub weighted_return_pct: Option<f64>,
    pub weighted_excess_return_pct: Option<f64>,
    pub shrunk_excess_return_pct: Option<f64>,
    pub weighted_positive_rate: Option<f64>,
    pub weighted_median_excess_return_pct: Option<f64>,
    pub winsorized_excess_return_pct: Option<f64>,
    pub weighted_excess_positive_rate: Option<f64>,
    pub weighted_mfe_pct: Option<f64>,
    pub weighted_mae_pct: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyTriggerSimilarityRow {
    pub ts_code: String,
    pub name: Option<String>,
    pub industry: Option<String>,
    pub concept: Option<String>,
    pub candidate_start_trade_date: String,
    pub candidate_end_trade_date: String,
    pub outcome_start_trade_date: String,
    pub outcome_end_trade_date: String,
    pub similarity_score: f64,
    pub trigger_similarity: f64,
    pub price_volume_similarity: Option<f64>,
    pub indicator_similarity: Option<f64>,
    pub market_similarity: Option<f64>,
    pub matched_rule_count: usize,
    pub matched_rule_names: Vec<String>,
    pub candidate_trigger_count: usize,
    pub forward_return_pct: f64,
    pub forward_excess_return_pct: Option<f64>,
    pub mfe_pct: f64,
    pub mae_pct: f64,
    pub total_score: Option<f64>,
    pub rank: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyTriggerSimilarityPageData {
    pub resolved_trade_date: String,
    pub resolved_ts_code: String,
    pub window_trade_days: usize,
    pub pool_segments: usize,
    pub outcome_trade_days: usize,
    pub historical_cutoff_date: String,
    pub benchmark_index_code: String,
    pub kernel_names: Vec<String>,
    pub indicator_columns: Vec<String>,
    pub candidate_universe_count: usize,
    pub candidate_anchor_count: usize,
    pub evaluated_anchor_count: usize,
    pub candidate_pool_truncated: bool,
    pub target: StrategyTriggerSimilarityTarget,
    pub outcome_summary: StrategyTriggerSimilarityOutcomeSummary,
    pub items: Vec<StrategyTriggerSimilarityRow>,
}

#[derive(Debug, Clone)]
struct Anchor {
    id: usize,
    ts_code: String,
    start_trade_date: String,
    end_trade_date: String,
}

#[derive(Debug, Clone)]
struct RuleEvent {
    rule_name: String,
    trade_date: String,
    score: f64,
}

#[derive(Debug, Clone)]
struct MarketObservation {
    trade_date: String,
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
    pct_chg: Option<f64>,
    vol: Option<f64>,
    amount: Option<f64>,
    turnover: Option<f64>,
    net_flow: Option<f64>,
    indicators: Vec<Option<f64>>,
}

#[derive(Debug, Clone)]
struct FutureObservation {
    trade_date: String,
    open: f64,
    close: f64,
    high: f64,
    low: f64,
}

#[derive(Debug, Clone)]
struct Outcome {
    start_trade_date: String,
    end_trade_date: String,
    return_pct: f64,
    excess_return_pct: Option<f64>,
    mfe_pct: f64,
    mae_pct: f64,
}

#[derive(Debug, Clone, Copy)]
struct BenchmarkObservation {
    open: f64,
    close: f64,
}

#[derive(Debug, Clone)]
struct EventFingerprint {
    trigger: Vec<f64>,
    price_volume: Vec<Option<Vec<f64>>>,
    indicators: Vec<Option<Vec<f64>>>,
    market: Vec<Option<Vec<f64>>>,
}

impl EventFingerprint {
    fn dimension(&self) -> usize {
        self.trigger.len()
            + self
                .price_volume
                .iter()
                .chain(self.indicators.iter())
                .chain(self.market.iter())
                .filter_map(|channel| channel.as_ref().map(Vec::len))
                .sum::<usize>()
    }
}

#[derive(Debug, Clone)]
struct EventSample {
    anchor: Anchor,
    fingerprint: EventFingerprint,
    matched_rule_names: Vec<String>,
    trigger_count: usize,
    outcome: Option<Outcome>,
    total_score: Option<f64>,
    rank: Option<i64>,
}

#[derive(Debug, Clone)]
struct MarketSchema {
    columns: HashMap<String, String>,
    indicator_columns: Vec<String>,
}

#[derive(Debug, Default, Clone)]
struct MarketEnvironment {
    by_date: HashMap<String, Vec<Option<f64>>>,
    channel_count: usize,
}

fn normalize_ts_code(ts_code: &str) -> String {
    let normalized = ts_code.trim().to_ascii_uppercase();
    if normalized.contains('.') {
        return normalized;
    }
    if normalized.starts_with("30") || normalized.starts_with("00") {
        format!("{normalized}.SZ")
    } else if normalized.starts_with("60") || normalized.starts_with("68") {
        format!("{normalized}.SH")
    } else {
        format!("{normalized}.BJ")
    }
}

fn resolve_benchmark_index_code(value: Option<&str>) -> Result<String, String> {
    let code = value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(DEFAULT_BENCHMARK_INDEX_CODE)
        .to_ascii_uppercase();
    if INDEX_TS_CODES.contains(&code.as_str()) {
        Ok(code)
    } else {
        Err(format!(
            "不支持的评级基准指数: {code}，可选值为 {}",
            INDEX_TS_CODES.join("、")
        ))
    }
}

pub fn list_strategy_trigger_similarity_benchmark_index_codes() -> Vec<String> {
    INDEX_TS_CODES
        .iter()
        .map(|code| (*code).to_string())
        .collect()
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn open_result_conn(source_path: &str) -> Result<Connection, String> {
    let result_db = result_db_path(source_path);
    let result_db_str = result_db
        .to_str()
        .ok_or_else(|| "结果库路径不是有效UTF-8".to_string())?;
    let config = Config::default()
        .access_mode(AccessMode::ReadOnly)
        .map_err(|e| format!("配置结果库只读模式失败: {e}"))?;
    let conn = Connection::open_with_flags(result_db_str, config)
        .map_err(|e| format!("打开结果库失败: {e}"))?;
    let market_db = source_db_path(source_path);
    let market_db_str = market_db
        .to_str()
        .ok_or_else(|| "行情库路径不是有效UTF-8".to_string())?;
    conn.execute(
        &format!(
            "ATTACH {} AS trigger_market_db (READ_ONLY)",
            sql_string_literal(market_db_str)
        ),
        [],
    )
    .map_err(|e| format!("挂载行情库失败: {e}"))?;
    Ok(conn)
}

fn resolve_existing_trade_date(
    conn: &Connection,
    trade_date: Option<String>,
) -> Result<String, String> {
    let requested = match trade_date.as_deref().and_then(normalize_trade_date) {
        Some(normalized) => Some(normalized),
        None => trade_date,
    };
    let requested = resolve_trade_date(conn, requested)?;
    conn.query_row(
        "SELECT MAX(trade_date) FROM score_summary WHERE trade_date <= ?",
        params![requested],
        |row| row.get::<_, Option<String>>(0),
    )
    .map_err(|e| format!("解析策略事件参考日失败: {e}"))?
    .ok_or_else(|| "score_summary 没有可用交易日".to_string())
}

fn load_all_trade_dates(conn: &Connection) -> Result<Vec<String>, String> {
    let mut stmt = conn
        .prepare("SELECT DISTINCT trade_date FROM score_summary ORDER BY trade_date")
        .map_err(|e| format!("预编译交易日查询失败: {e}"))?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|e| format!("查询交易日失败: {e}"))?;
    let dates = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("读取交易日失败: {e}"))?;
    if dates.is_empty() {
        Err("score_summary 没有可用交易日".to_string())
    } else {
        Ok(dates)
    }
}

fn load_market_schema(conn: &Connection) -> Result<MarketSchema, String> {
    let mut stmt = conn
        .prepare("DESCRIBE SELECT * FROM trigger_market_db.stock_data")
        .map_err(|e| format!("预编译行情字段查询失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询行情字段失败: {e}"))?;
    let mut columns = HashMap::new();
    let mut typed_columns = Vec::new();
    while let Some(row) = rows.next().map_err(|e| format!("读取行情字段失败: {e}"))? {
        let name: String = row.get(0).map_err(|e| format!("读取行情字段名失败: {e}"))?;
        let data_type: String = row
            .get(1)
            .map_err(|e| format!("读取行情字段类型失败: {e}"))?;
        columns.insert(name.to_ascii_lowercase(), name.clone());
        typed_columns.push((name, data_type));
    }
    if columns.is_empty() {
        return Err("行情库不存在 stock_data 表或无法读取字段".to_string());
    }

    let base_columns = HashSet::from([
        "ts_code",
        "trade_date",
        "adj_type",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "change",
        "pct_chg",
        "vol",
        "amount",
        "tor",
        "b_sm_v",
        "s_sm_v",
        "b_md_v",
        "s_md_v",
        "b_lg_v",
        "s_lg_v",
        "b_elg_v",
        "s_elg_v",
        "net_mf_v",
    ]);
    let mut indicator_columns = typed_columns
        .into_iter()
        .filter(|(name, ty)| {
            !base_columns.contains(name.to_ascii_lowercase().as_str())
                && ["INT", "DOUBLE", "FLOAT", "REAL", "DECIMAL", "HUGEINT"]
                    .iter()
                    .any(|marker| ty.to_ascii_uppercase().contains(marker))
        })
        .map(|(name, _)| name)
        .collect::<Vec<_>>();
    indicator_columns.sort();
    indicator_columns.truncate(MAX_INDICATOR_COLUMNS);
    Ok(MarketSchema {
        columns,
        indicator_columns,
    })
}

fn column_expr(schema: &MarketSchema, logical_name: &str, alias: &str) -> String {
    schema
        .columns
        .get(&logical_name.to_ascii_lowercase())
        .map(|actual| format!("TRY_CAST({alias}.{} AS DOUBLE)", quote_ident(actual)))
        .unwrap_or_else(|| "CAST(NULL AS DOUBLE)".to_string())
}

fn load_target_rule_events(
    conn: &Connection,
    ts_code: &str,
    start_date: &str,
    end_date: &str,
) -> Result<Vec<RuleEvent>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT rule_name, trade_date, TRY_CAST(rule_score AS DOUBLE)
            FROM rule_details
            WHERE ts_code = ? AND trade_date >= ? AND trade_date <= ?
              AND TRY_CAST(rule_score AS DOUBLE) IS NOT NULL
              AND ABS(TRY_CAST(rule_score AS DOUBLE)) > ?
            ORDER BY trade_date, rule_name
            "#,
        )
        .map_err(|e| format!("预编译目标触发查询失败: {e}"))?;
    let mut rows = stmt
        .query(params![ts_code, start_date, end_date, EPS])
        .map_err(|e| format!("查询目标触发失败: {e}"))?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(|e| format!("读取目标触发失败: {e}"))? {
        out.push(RuleEvent {
            rule_name: row.get(0).map_err(|e| format!("读取规则名失败: {e}"))?,
            trade_date: row.get(1).map_err(|e| format!("读取触发日失败: {e}"))?,
            score: row.get(2).map_err(|e| format!("读取规则分数失败: {e}"))?,
        });
    }
    Ok(out)
}

fn distinct_rule_names(events: &[RuleEvent]) -> Vec<String> {
    let mut names = events
        .iter()
        .map(|event| event.rule_name.clone())
        .collect::<Vec<_>>();
    names.sort();
    names.dedup();
    names
}

fn load_candidate_anchors(
    conn: &Connection,
    target_rule_names: &[String],
    earliest_date: &str,
    cutoff_date: &str,
    all_trade_dates: &[String],
    window_trade_days: usize,
) -> Result<(Vec<Anchor>, usize), String> {
    if target_rule_names.is_empty() {
        return Ok((Vec::new(), 0));
    }
    let target_rule_literals = target_rule_names
        .iter()
        .map(|name| sql_string_literal(name))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
        WITH candidates AS (
            SELECT DISTINCT ts_code, trade_date
            FROM rule_details
            WHERE trade_date >= {earliest_date} AND trade_date <= {cutoff_date}
              AND rule_name IN ({target_rule_literals})
              AND TRY_CAST(rule_score AS DOUBLE) IS NOT NULL
              AND ABS(TRY_CAST(rule_score AS DOUBLE)) > {EPS}
        ),
        counted AS (
            SELECT *, COUNT(*) OVER () AS candidate_total
            FROM candidates
        ),
        recent_history AS MATERIALIZED (
            SELECT * FROM counted
            ORDER BY trade_date DESC, hash(ts_code, trade_date)
            LIMIT {recent_limit}
        ),
        diverse_history AS (
            SELECT c.* FROM counted c
            WHERE NOT EXISTS (
                SELECT 1 FROM recent_history t
                WHERE t.ts_code = c.ts_code AND t.trade_date = c.trade_date
            )
            ORDER BY hash(c.ts_code, c.trade_date)
            LIMIT {diversity_limit}
        )
        SELECT ts_code, trade_date, candidate_total FROM recent_history
        UNION ALL
        SELECT ts_code, trade_date, candidate_total FROM diverse_history
        "#,
        cutoff_date = sql_string_literal(cutoff_date),
        earliest_date = sql_string_literal(earliest_date),
        recent_limit = RECENT_CANDIDATE_ANCHORS,
        diversity_limit = HISTORY_DIVERSITY_ANCHORS,
    );
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("预编译历史事件锚点查询失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询历史事件锚点失败: {e}"))?;
    let date_index = all_trade_dates
        .iter()
        .enumerate()
        .map(|(index, date)| (date.as_str(), index))
        .collect::<HashMap<_, _>>();
    let mut anchors = Vec::new();
    let mut candidate_universe_count = 0;
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取历史事件锚点失败: {e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取锚点代码失败: {e}"))?;
        let end_trade_date: String = row.get(1).map_err(|e| format!("读取锚点日期失败: {e}"))?;
        let total: i64 = row
            .get(2)
            .map_err(|e| format!("读取候选全集数量失败: {e}"))?;
        candidate_universe_count = total.max(0) as usize;
        let Some(end_index) = date_index.get(end_trade_date.as_str()).copied() else {
            continue;
        };
        let start_index = (end_index + 1).saturating_sub(window_trade_days);
        anchors.push(Anchor {
            id: anchors.len(),
            ts_code,
            start_trade_date: all_trade_dates[start_index].clone(),
            end_trade_date,
        });
    }
    Ok((anchors, candidate_universe_count))
}

fn anchors_values_sql(anchors: &[Anchor]) -> String {
    anchors
        .iter()
        .map(|anchor| {
            format!(
                "({}, {}, {}, {})",
                anchor.id,
                sql_string_literal(&anchor.ts_code),
                sql_string_literal(&anchor.start_trade_date),
                sql_string_literal(&anchor.end_trade_date)
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn load_market_rows(
    conn: &Connection,
    anchors: &[Anchor],
    schema: &MarketSchema,
) -> Result<HashMap<usize, Vec<MarketObservation>>, String> {
    if anchors.is_empty() {
        return Ok(HashMap::new());
    }
    let indicator_sql = schema
        .indicator_columns
        .iter()
        .map(|name| format!(", TRY_CAST(s.{} AS DOUBLE)", quote_ident(name)))
        .collect::<String>();
    let sql = format!(
        r#"
        WITH anchors(anchor_id, ts_code, start_date, end_date) AS (VALUES {})
        SELECT a.anchor_id, s.trade_date,
               {}, {}, {}, {}, {}, {}, {}, {}, {} {}
        FROM anchors a
        JOIN trigger_market_db.stock_data s
          ON s.ts_code = a.ts_code AND s.trade_date >= a.start_date
         AND s.trade_date <= a.end_date AND s.adj_type = 'qfq'
        ORDER BY a.anchor_id, s.trade_date
        "#,
        anchors_values_sql(anchors),
        column_expr(schema, "open", "s"),
        column_expr(schema, "high", "s"),
        column_expr(schema, "low", "s"),
        column_expr(schema, "close", "s"),
        column_expr(schema, "pct_chg", "s"),
        column_expr(schema, "vol", "s"),
        column_expr(schema, "amount", "s"),
        column_expr(schema, "tor", "s"),
        column_expr(schema, "net_mf_v", "s"),
        indicator_sql,
    );
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("预编译事件量价窗口查询失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询事件量价窗口失败: {e}"))?;
    let mut out = HashMap::<usize, Vec<MarketObservation>>::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取事件量价窗口失败: {e}"))?
    {
        let anchor_id: i64 = row.get(0).map_err(|e| format!("读取事件编号失败: {e}"))?;
        let indicator_count = schema.indicator_columns.len();
        let mut indicators = Vec::with_capacity(indicator_count);
        for index in 0..indicator_count {
            indicators.push(
                row.get::<_, Option<f64>>(11 + index)
                    .map_err(|e| format!("读取指标窗口失败: {e}"))?,
            );
        }
        out.entry(anchor_id.max(0) as usize)
            .or_default()
            .push(MarketObservation {
                trade_date: row.get(1).map_err(|e| format!("读取行情日期失败: {e}"))?,
                open: row.get(2).map_err(|e| format!("读取开盘价失败: {e}"))?,
                high: row.get(3).map_err(|e| format!("读取最高价失败: {e}"))?,
                low: row.get(4).map_err(|e| format!("读取最低价失败: {e}"))?,
                close: row.get(5).map_err(|e| format!("读取收盘价失败: {e}"))?,
                pct_chg: row.get(6).map_err(|e| format!("读取涨跌幅失败: {e}"))?,
                vol: row.get(7).map_err(|e| format!("读取成交量失败: {e}"))?,
                amount: row.get(8).map_err(|e| format!("读取成交额失败: {e}"))?,
                turnover: row.get(9).map_err(|e| format!("读取换手率失败: {e}"))?,
                net_flow: row.get(10).map_err(|e| format!("读取净流量失败: {e}"))?,
                indicators,
            });
    }
    Ok(out)
}

fn load_rule_rows(
    conn: &Connection,
    anchors: &[Anchor],
) -> Result<HashMap<usize, Vec<RuleEvent>>, String> {
    if anchors.is_empty() {
        return Ok(HashMap::new());
    }
    let sql = format!(
        r#"
        WITH anchors(anchor_id, ts_code, start_date, end_date) AS (VALUES {})
        SELECT a.anchor_id, d.rule_name, d.trade_date, TRY_CAST(d.rule_score AS DOUBLE)
        FROM anchors a JOIN rule_details d ON d.ts_code = a.ts_code
         AND d.trade_date >= a.start_date AND d.trade_date <= a.end_date
        WHERE TRY_CAST(d.rule_score AS DOUBLE) IS NOT NULL
          AND ABS(TRY_CAST(d.rule_score AS DOUBLE)) > {EPS}
        ORDER BY a.anchor_id, d.trade_date, d.rule_name
        "#,
        anchors_values_sql(anchors)
    );
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("预编译事件规则窗口查询失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询事件规则窗口失败: {e}"))?;
    let mut out = HashMap::<usize, Vec<RuleEvent>>::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取事件规则窗口失败: {e}"))?
    {
        let anchor_id: i64 = row.get(0).map_err(|e| format!("读取事件编号失败: {e}"))?;
        out.entry(anchor_id.max(0) as usize)
            .or_default()
            .push(RuleEvent {
                rule_name: row.get(1).map_err(|e| format!("读取规则名失败: {e}"))?,
                trade_date: row.get(2).map_err(|e| format!("读取触发日期失败: {e}"))?,
                score: row.get(3).map_err(|e| format!("读取规则分数失败: {e}"))?,
            });
    }
    Ok(out)
}

fn load_future_rows(
    conn: &Connection,
    anchors: &[Anchor],
    outcome_trade_days: usize,
    target_trade_date: &str,
) -> Result<HashMap<usize, Vec<FutureObservation>>, String> {
    if anchors.is_empty() {
        return Ok(HashMap::new());
    }
    let sql = format!(
        r#"
        WITH anchors(anchor_id, ts_code, start_date, end_date) AS (VALUES {}),
        future AS (
            SELECT a.anchor_id, s.trade_date, TRY_CAST(s.open AS DOUBLE) AS open_value,
                   TRY_CAST(s.close AS DOUBLE) AS close_value,
                   TRY_CAST(s.high AS DOUBLE) AS high_value,
                   TRY_CAST(s.low AS DOUBLE) AS low_value,
                   ROW_NUMBER() OVER (PARTITION BY a.anchor_id ORDER BY s.trade_date) rn
            FROM anchors a JOIN trigger_market_db.stock_data s ON s.ts_code = a.ts_code
             AND s.trade_date > a.end_date AND s.trade_date <= {} AND s.adj_type = 'qfq'
        )
        SELECT anchor_id, trade_date, open_value, close_value, high_value, low_value
        FROM future WHERE rn <= {}
        ORDER BY anchor_id, trade_date
        "#,
        anchors_values_sql(anchors),
        sql_string_literal(target_trade_date),
        outcome_trade_days,
    );
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("预编译事件后验查询失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询事件后验失败: {e}"))?;
    let mut out = HashMap::<usize, Vec<FutureObservation>>::new();
    while let Some(row) = rows.next().map_err(|e| format!("读取事件后验失败: {e}"))? {
        let anchor_id: i64 = row.get(0).map_err(|e| format!("读取事件编号失败: {e}"))?;
        let open: Option<f64> = row.get(2).map_err(|e| format!("读取后验开盘价失败: {e}"))?;
        let close: Option<f64> = row.get(3).map_err(|e| format!("读取后验收盘价失败: {e}"))?;
        let high: Option<f64> = row.get(4).map_err(|e| format!("读取后验最高价失败: {e}"))?;
        let low: Option<f64> = row.get(5).map_err(|e| format!("读取后验最低价失败: {e}"))?;
        let (Some(open), Some(close), Some(high), Some(low)) = (open, close, high, low) else {
            continue;
        };
        if !open.is_finite() || !close.is_finite() || !high.is_finite() || !low.is_finite() {
            continue;
        }
        out.entry(anchor_id.max(0) as usize)
            .or_default()
            .push(FutureObservation {
                trade_date: row.get(1).map_err(|e| format!("读取后验日期失败: {e}"))?,
                open,
                close,
                high,
                low,
            });
    }
    Ok(out)
}

fn load_summary_rows(
    conn: &Connection,
    anchors: &[Anchor],
) -> Result<HashMap<usize, (Option<f64>, Option<i64>)>, String> {
    if anchors.is_empty() {
        return Ok(HashMap::new());
    }
    let sql = format!(
        r#"
        WITH anchors(anchor_id, ts_code, start_date, end_date) AS (VALUES {})
        SELECT a.anchor_id, TRY_CAST(s.total_score AS DOUBLE), s.rank
        FROM anchors a LEFT JOIN score_summary s
          ON s.ts_code = a.ts_code AND s.trade_date = a.end_date
        "#,
        anchors_values_sql(anchors)
    );
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("预编译事件榜单查询失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询事件榜单失败: {e}"))?;
    let mut out = HashMap::new();
    while let Some(row) = rows.next().map_err(|e| format!("读取事件榜单失败: {e}"))? {
        let anchor_id: i64 = row.get(0).map_err(|e| format!("读取事件编号失败: {e}"))?;
        out.insert(
            anchor_id.max(0) as usize,
            (
                row.get(1).map_err(|e| format!("读取总分失败: {e}"))?,
                row.get(2).map_err(|e| format!("读取排名失败: {e}"))?,
            ),
        );
    }
    Ok(out)
}

fn load_market_environment(
    conn: &Connection,
    start_date: &str,
    end_date: &str,
    schema: &MarketSchema,
) -> Result<MarketEnvironment, String> {
    let amount_expr = column_expr(schema, "amount", "s");
    let turnover_expr = column_expr(schema, "tor", "s");
    let sql = format!(
        r#"
        SELECT s.trade_date, AVG(TRY_CAST(s.pct_chg AS DOUBLE)),
               MEDIAN(TRY_CAST(s.pct_chg AS DOUBLE)), AVG(ABS(TRY_CAST(s.pct_chg AS DOUBLE))),
               AVG(CASE WHEN TRY_CAST(s.pct_chg AS DOUBLE) > 0 THEN 1.0 ELSE 0.0 END),
               AVG(CASE WHEN TRY_CAST(s.pct_chg AS DOUBLE) >= 5 THEN 1.0 ELSE 0.0 END),
               LN(1 + COALESCE(SUM({amount_expr}), 0)), AVG({turnover_expr})
        FROM trigger_market_db.stock_data s
        WHERE s.adj_type = 'qfq' AND s.trade_date >= ? AND s.trade_date <= ?
        GROUP BY s.trade_date ORDER BY s.trade_date
        "#
    );
    let mut environment = MarketEnvironment {
        by_date: HashMap::new(),
        channel_count: 7 + INDEX_TS_CODES.len(),
    };
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("预编译市场宽度查询失败: {e}"))?;
    let mut rows = stmt
        .query(params![start_date, end_date])
        .map_err(|e| format!("查询市场宽度失败: {e}"))?;
    while let Some(row) = rows.next().map_err(|e| format!("读取市场宽度失败: {e}"))? {
        let date: String = row.get(0).map_err(|e| format!("读取市场日期失败: {e}"))?;
        let mut values = Vec::with_capacity(environment.channel_count);
        for index in 1..=7 {
            values.push(
                row.get::<_, Option<f64>>(index)
                    .map_err(|e| format!("读取市场宽度字段失败: {e}"))?,
            );
        }
        values.resize(environment.channel_count, None);
        environment.by_date.insert(date, values);
    }

    let placeholders = std::iter::repeat_n("?", INDEX_TS_CODES.len())
        .collect::<Vec<_>>()
        .join(", ");
    let index_sql = format!(
        "SELECT ts_code, trade_date, TRY_CAST(pct_chg AS DOUBLE) FROM trigger_market_db.stock_data \
         WHERE adj_type = 'ind' AND trade_date >= ? AND trade_date <= ? AND ts_code IN ({placeholders})"
    );
    let mut values = vec![start_date.to_string(), end_date.to_string()];
    values.extend(INDEX_TS_CODES.iter().map(|value| value.to_string()));
    let mut stmt = conn
        .prepare(&index_sql)
        .map_err(|e| format!("预编译宽基环境查询失败: {e}"))?;
    let mut rows = stmt
        .query(params_from_iter(values.iter()))
        .map_err(|e| format!("查询宽基环境失败: {e}"))?;
    while let Some(row) = rows.next().map_err(|e| format!("读取宽基环境失败: {e}"))? {
        let code: String = row.get(0).map_err(|e| format!("读取宽基代码失败: {e}"))?;
        let date: String = row.get(1).map_err(|e| format!("读取宽基日期失败: {e}"))?;
        let value: Option<f64> = row.get(2).map_err(|e| format!("读取宽基涨跌失败: {e}"))?;
        if let Some(index) = INDEX_TS_CODES
            .iter()
            .position(|candidate| *candidate == code)
        {
            environment
                .by_date
                .entry(date)
                .or_insert_with(|| vec![None; environment.channel_count])[7 + index] = value;
        }
    }
    Ok(environment)
}

fn pool_series(values: &[Option<f64>], segments: usize) -> Vec<f64> {
    let len = values.len();
    (0..segments)
        .map(|segment| {
            let start = segment * len / segments;
            let end = (segment + 1) * len / segments;
            let finite = values[start..end]
                .iter()
                .filter_map(|value| value.filter(|number| number.is_finite()))
                .collect::<Vec<_>>();
            if finite.is_empty() {
                0.0
            } else {
                finite.iter().sum::<f64>() / finite.len() as f64
            }
        })
        .collect()
}

fn weighted_projection(values: &[Option<f64>], weights: &[f64]) -> f64 {
    let mut weighted = 0.0;
    let mut weight_sum = 0.0;
    for (value, weight) in values.iter().zip(weights) {
        if let Some(value) = value.filter(|number| number.is_finite()) {
            weighted += value * weight;
            weight_sum += weight.abs();
        }
    }
    if weight_sum <= EPS {
        0.0
    } else {
        weighted / weight_sum
    }
}

fn kernel_responses(values: &[Option<f64>]) -> Vec<f64> {
    let len = values.len();
    let ages = (0..len).map(|index| len - 1 - index).collect::<Vec<_>>();
    let uniform = vec![1.0; len];
    let short_exp = ages
        .iter()
        .map(|age| 0.55_f64.powi(*age as i32))
        .collect::<Vec<_>>();
    let medium_exp = ages
        .iter()
        .map(|age| 0.82_f64.powi(*age as i32))
        .collect::<Vec<_>>();
    let recent_linear = (1..=len).map(|value| value as f64).collect::<Vec<_>>();
    let midpoint = len.saturating_sub(1) as f64 / 2.0;
    let trend = (0..len)
        .map(|index| index as f64 - midpoint)
        .collect::<Vec<_>>();
    let mut turning = vec![0.0; len];
    if len >= 3 {
        turning[len - 3] = 1.0;
        turning[len - 2] = -2.0;
        turning[len - 1] = 1.0;
    } else if len >= 2 {
        turning[len - 2] = -1.0;
        turning[len - 1] = 1.0;
    }
    [
        uniform,
        short_exp,
        medium_exp,
        recent_linear,
        trend,
        turning,
    ]
    .iter()
    .map(|weights| weighted_projection(values, weights))
    .collect()
}

fn temporal_signature(
    values: &[Option<f64>],
    segments: usize,
    standardize: bool,
) -> Option<Vec<f64>> {
    let finite = values
        .iter()
        .filter_map(|value| value.filter(|number| number.is_finite()))
        .collect::<Vec<_>>();
    if finite.len() < 2 {
        return None;
    }
    let transformed = if standardize {
        let mean = finite.iter().sum::<f64>() / finite.len() as f64;
        let variance = finite
            .iter()
            .map(|value| (value - mean).powi(2))
            .sum::<f64>()
            / finite.len() as f64;
        let std = variance.sqrt();
        if std <= EPS {
            return None;
        }
        values
            .iter()
            .map(|value| value.map(|number| (number - mean) / std))
            .collect::<Vec<_>>()
    } else {
        values.to_vec()
    };
    let mut signature = pool_series(&transformed, segments);
    signature.extend(kernel_responses(&transformed));
    // 固定偏置使余弦距离同时感知响应幅度；否则成比例的高/低波动窗口会完全同向。
    signature.push(1.0);
    Some(signature)
}

fn build_trigger_fingerprint(
    events: &[RuleEvent],
    target_rule_names: &[String],
    window_dates: &[String],
    segments: usize,
) -> (Vec<f64>, Vec<String>) {
    let date_index = window_dates
        .iter()
        .enumerate()
        .map(|(i, date)| (date.as_str(), i))
        .collect::<HashMap<_, _>>();
    let signature_len = segments + KERNEL_NAMES.len() + 1;
    let mut out = Vec::new();
    let mut matched = Vec::new();
    for rule_name in target_rule_names {
        let mut series = vec![Some(0.0); window_dates.len()];
        let mut hit = false;
        for event in events.iter().filter(|event| event.rule_name == *rule_name) {
            if let Some(index) = date_index.get(event.trade_date.as_str()).copied() {
                series[index] = Some(series[index].unwrap_or(0.0) + event.score);
                hit = true;
            }
        }
        if hit {
            matched.push(rule_name.clone());
        }
        out.extend(
            temporal_signature(&series, segments, false)
                .unwrap_or_else(|| vec![0.0; signature_len]),
        );
    }
    let mut total_count = vec![Some(0.0); window_dates.len()];
    let mut total_score = vec![Some(0.0); window_dates.len()];
    for event in events {
        if let Some(index) = date_index.get(event.trade_date.as_str()).copied() {
            total_count[index] = Some(total_count[index].unwrap_or(0.0) + 1.0);
            total_score[index] = Some(total_score[index].unwrap_or(0.0) + event.score);
        }
    }
    out.extend(
        temporal_signature(&total_count, segments, false)
            .unwrap_or_else(|| vec![0.0; signature_len]),
    );
    out.extend(
        temporal_signature(&total_score, segments, false)
            .unwrap_or_else(|| vec![0.0; signature_len]),
    );
    (out, matched)
}

fn build_price_volume_channels(
    rows: &[MarketObservation],
    segments: usize,
) -> Vec<Option<Vec<f64>>> {
    let mut channels = vec![Vec::new(); 9];
    for row in rows {
        channels[0].push(row.close.filter(|v| *v > 0.0).map(f64::ln));
        channels[1].push(row.pct_chg);
        channels[2].push(match (row.open, row.close) {
            (Some(o), Some(c)) if o.abs() > EPS => Some((c / o - 1.0) * 100.0),
            _ => None,
        });
        channels[3].push(match (row.low, row.high) {
            (Some(l), Some(h)) if l.abs() > EPS => Some((h / l - 1.0) * 100.0),
            _ => None,
        });
        channels[4].push(match (row.low, row.high, row.close) {
            (Some(l), Some(h), Some(c)) if (h - l).abs() > EPS => Some((c - l) / (h - l)),
            _ => None,
        });
        channels[5].push(row.vol.filter(|v| *v >= 0.0).map(|v| (1.0 + v).ln()));
        channels[6].push(row.amount.filter(|v| *v >= 0.0).map(|v| (1.0 + v).ln()));
        channels[7].push(row.turnover);
        channels[8].push(match (row.net_flow, row.vol) {
            (Some(f), Some(v)) if v.abs() > EPS => Some(f / v),
            _ => None,
        });
    }
    // 价格水平、成交量和成交额只比较相对形态；收益、振幅、位置、换手和资金流
    // 保留原始状态，以免把牛熊方向与风险强度标准化掉。
    let standardize = [true, false, false, false, false, true, true, false, false];
    channels
        .iter()
        .zip(standardize)
        .map(|(series, standardize)| temporal_signature(series, segments, standardize))
        .collect()
}

fn build_indicator_channels(
    rows: &[MarketObservation],
    count: usize,
    segments: usize,
) -> Vec<Option<Vec<f64>>> {
    (0..count)
        .map(|index| {
            let series = rows
                .iter()
                .map(|row| row.indicators.get(index).copied().flatten())
                .collect::<Vec<_>>();
            temporal_signature(&series, segments, false)
        })
        .collect()
}

fn build_environment_channels(
    environment: &MarketEnvironment,
    window_dates: &[String],
    segments: usize,
) -> Vec<Option<Vec<f64>>> {
    (0..environment.channel_count)
        .map(|index| {
            let series = window_dates
                .iter()
                .map(|date| {
                    environment
                        .by_date
                        .get(date)
                        .and_then(|v| v.get(index))
                        .copied()
                        .flatten()
                })
                .collect::<Vec<_>>();
            temporal_signature(&series, segments, true)
        })
        .collect()
}

fn build_environment_fingerprint_map(
    environment: &MarketEnvironment,
    all_trade_dates: &[String],
    window_trade_days: usize,
    segments: usize,
) -> HashMap<String, Vec<Option<Vec<f64>>>> {
    all_trade_dates
        .iter()
        .enumerate()
        .map(|(end_index, end_date)| {
            let start_index = (end_index + 1).saturating_sub(window_trade_days);
            (
                end_date.clone(),
                build_environment_channels(
                    environment,
                    &all_trade_dates[start_index..=end_index],
                    segments,
                ),
            )
        })
        .collect()
}

fn cosine_similarity(left: &[f64], right: &[f64]) -> f64 {
    if left.len() != right.len() || left.is_empty() {
        return 0.0;
    }
    let dot = left.iter().zip(right).map(|(a, b)| a * b).sum::<f64>();
    let left_norm = left.iter().map(|v| v * v).sum::<f64>().sqrt();
    let right_norm = right.iter().map(|v| v * v).sum::<f64>().sqrt();
    if left_norm <= EPS && right_norm <= EPS {
        100.0
    } else if left_norm <= EPS || right_norm <= EPS {
        0.0
    } else {
        (50.0 * (1.0 + dot / (left_norm * right_norm))).clamp(0.0, 100.0)
    }
}

fn channel_similarity(target: &[Option<Vec<f64>>], candidate: &[Option<Vec<f64>>]) -> Option<f64> {
    let scores = target
        .iter()
        .zip(candidate)
        .filter_map(|(a, b)| match (a, b) {
            (Some(a), Some(b)) => Some(cosine_similarity(a, b)),
            _ => None,
        })
        .collect::<Vec<_>>();
    (!scores.is_empty()).then(|| scores.iter().sum::<f64>() / scores.len() as f64)
}

fn final_similarity(
    trigger: f64,
    price: Option<f64>,
    indicator: Option<f64>,
    market: Option<f64>,
) -> f64 {
    let mut total = trigger * TRIGGER_SIMILARITY_WEIGHT;
    let mut weight = TRIGGER_SIMILARITY_WEIGHT;
    for (score, w) in [
        (price, PRICE_VOLUME_SIMILARITY_WEIGHT),
        (indicator, INDICATOR_SIMILARITY_WEIGHT),
        (market, MARKET_SIMILARITY_WEIGHT),
    ] {
        if let Some(score) = score {
            total += score * w;
            weight += w;
        }
    }
    total / weight
}

fn window_dates_for_anchor<'a>(anchor: &Anchor, dates: &'a [String]) -> &'a [String] {
    let start = dates.binary_search(&anchor.start_trade_date).unwrap_or(0);
    let end = dates
        .binary_search(&anchor.end_trade_date)
        .map(|i| i + 1)
        .unwrap_or(dates.len());
    &dates[start.min(end)..end]
}

fn build_outcome(
    _market_rows: &[MarketObservation],
    future_rows: &[FutureObservation],
    horizon: usize,
    benchmark_rows: &HashMap<String, BenchmarkObservation>,
) -> Option<Outcome> {
    if future_rows.len() != horizon {
        return None;
    }
    let first = future_rows.first()?;
    let entry = (first.open.abs() > EPS).then_some(first.open)?;
    let last = future_rows.last()?;
    let return_pct = (last.close / entry - 1.0) * 100.0;
    let mfe_pct = future_rows
        .iter()
        .map(|r| (r.high / entry - 1.0) * 100.0)
        .fold(f64::NEG_INFINITY, f64::max);
    let mae_pct = future_rows
        .iter()
        .map(|r| (r.low / entry - 1.0) * 100.0)
        .fold(f64::INFINITY, f64::min);
    let excess_return_pct = match (
        benchmark_rows.get(&first.trade_date),
        benchmark_rows.get(&last.trade_date),
    ) {
        (Some(start), Some(end)) if start.open.abs() > EPS => {
            Some(return_pct - (end.close / start.open - 1.0) * 100.0)
        }
        _ => None,
    };
    Some(Outcome {
        start_trade_date: first.trade_date.clone(),
        end_trade_date: last.trade_date.clone(),
        return_pct,
        excess_return_pct,
        mfe_pct,
        mae_pct,
    })
}

fn load_benchmark_rows(
    conn: &Connection,
    start: &str,
    end: &str,
    benchmark_index_code: &str,
) -> Result<HashMap<String, BenchmarkObservation>, String> {
    let mut stmt = conn
        .prepare(
            "SELECT trade_date, TRY_CAST(open AS DOUBLE), TRY_CAST(close AS DOUBLE) \
             FROM trigger_market_db.stock_data \
             WHERE adj_type='ind' AND ts_code=? AND trade_date>=? AND trade_date<=?",
        )
        .map_err(|e| format!("预编译基准指数行情查询失败: {e}"))?;
    let mut rows = stmt
        .query(params![benchmark_index_code, start, end])
        .map_err(|e| format!("查询基准指数行情失败: {e}"))?;
    let mut out = HashMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取基准指数行情失败: {e}"))?
    {
        let date: String = row.get(0).map_err(|e| format!("读取指数日期失败: {e}"))?;
        let open: Option<f64> = row.get(1).map_err(|e| format!("读取指数开盘价失败: {e}"))?;
        let close: Option<f64> = row.get(2).map_err(|e| format!("读取指数收盘价失败: {e}"))?;
        if let (Some(open), Some(close)) = (
            open.filter(|value| value.is_finite()),
            close.filter(|value| value.is_finite()),
        ) {
            out.insert(date, BenchmarkObservation { open, close });
        }
    }
    Ok(out)
}

#[derive(Clone, Copy)]
struct SampleBuildContext<'a> {
    schema: &'a MarketSchema,
    target_rule_names: &'a [String],
    all_trade_dates: &'a [String],
    environment_fingerprints: &'a HashMap<String, Vec<Option<Vec<f64>>>>,
    benchmark_rows: &'a HashMap<String, BenchmarkObservation>,
    pool_segments: usize,
    outcome_trade_days: usize,
    target_trade_date: &'a str,
    include_outcome: bool,
    include_summaries: bool,
}

fn build_samples_for_chunk(
    conn: &Connection,
    anchors: &[Anchor],
    context: &SampleBuildContext<'_>,
) -> Result<Vec<EventSample>, String> {
    let market_by_anchor = load_market_rows(conn, anchors, context.schema)?;
    let rules_by_anchor = load_rule_rows(conn, anchors)?;
    let summaries = if context.include_summaries {
        load_summary_rows(conn, anchors)?
    } else {
        HashMap::new()
    };
    let future_by_anchor = if context.include_outcome {
        load_future_rows(
            conn,
            anchors,
            context.outcome_trade_days,
            context.target_trade_date,
        )?
    } else {
        HashMap::new()
    };
    let samples = anchors
        .par_iter()
        .filter_map(|anchor| {
            let market_rows = market_by_anchor.get(&anchor.id)?;
            if market_rows.len() < 3
                || market_rows.last().map(|r| r.trade_date.as_str())
                    != Some(anchor.end_trade_date.as_str())
            {
                return None;
            }
            let rules = rules_by_anchor
                .get(&anchor.id)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let window_dates = window_dates_for_anchor(anchor, context.all_trade_dates);
            let (trigger, matched_rule_names) = build_trigger_fingerprint(
                rules,
                context.target_rule_names,
                window_dates,
                context.pool_segments,
            );
            let fingerprint = EventFingerprint {
                trigger,
                price_volume: build_price_volume_channels(market_rows, context.pool_segments),
                indicators: build_indicator_channels(
                    market_rows,
                    context.schema.indicator_columns.len(),
                    context.pool_segments,
                ),
                market: context
                    .environment_fingerprints
                    .get(&anchor.end_trade_date)
                    .cloned()
                    .unwrap_or_default(),
            };
            let outcome = if context.include_outcome {
                build_outcome(
                    market_rows,
                    future_by_anchor
                        .get(&anchor.id)
                        .map(Vec::as_slice)
                        .unwrap_or(&[]),
                    context.outcome_trade_days,
                    context.benchmark_rows,
                )
            } else {
                None
            };
            if context.include_outcome && outcome.is_none() {
                return None;
            }
            let (total_score, rank) = summaries.get(&anchor.id).copied().unwrap_or((None, None));
            Some(EventSample {
                anchor: anchor.clone(),
                fingerprint,
                matched_rule_names,
                trigger_count: rules.len(),
                outcome,
                total_score,
                rank,
            })
        })
        .collect();
    Ok(samples)
}

fn weighted_quantile(values: &[(f64, f64)], quantile: f64) -> Option<f64> {
    let mut finite = values
        .iter()
        .copied()
        .filter(|(value, weight)| value.is_finite() && weight.is_finite() && *weight > EPS)
        .collect::<Vec<_>>();
    if finite.is_empty() {
        return None;
    }
    finite.sort_by(|a, b| a.0.total_cmp(&b.0));
    let total_weight = finite.iter().map(|(_, weight)| weight).sum::<f64>();
    let target_weight = quantile.clamp(0.0, 1.0) * total_weight;
    let mut cumulative_weight = 0.0;
    for (value, weight) in &finite {
        cumulative_weight += weight;
        if cumulative_weight + EPS >= target_weight {
            return Some(*value);
        }
    }
    finite.last().map(|(value, _)| *value)
}

fn weighted_winsorized_mean(values: &[(f64, f64)], tail_fraction: f64) -> Option<f64> {
    let lower = weighted_quantile(values, tail_fraction)?;
    let upper = weighted_quantile(values, 1.0 - tail_fraction)?;
    let (weighted_sum, weight_sum) = values
        .iter()
        .filter(|(value, weight)| value.is_finite() && weight.is_finite() && *weight > EPS)
        .fold((0.0, 0.0), |(sum, total), (value, weight)| {
            (sum + value.clamp(lower, upper) * weight, total + weight)
        });
    (weight_sum > EPS).then_some(weighted_sum / weight_sum)
}

fn summarize_outcomes(
    items: &[StrategyTriggerSimilarityRow],
) -> StrategyTriggerSimilarityOutcomeSummary {
    let weighted = items
        .iter()
        .map(|item| (item, (item.similarity_score / 100.0).powi(2)))
        .filter(|(_, w)| *w > EPS)
        .collect::<Vec<_>>();
    let weight_sum = weighted.iter().map(|(_, w)| w).sum::<f64>();
    let weight_sq_sum = weighted.iter().map(|(_, w)| w * w).sum::<f64>();
    if weight_sum <= EPS {
        return StrategyTriggerSimilarityOutcomeSummary {
            sample_count: 0,
            effective_sample_count: 0.0,
            weighted_return_pct: None,
            weighted_excess_return_pct: None,
            shrunk_excess_return_pct: None,
            weighted_positive_rate: None,
            weighted_median_excess_return_pct: None,
            winsorized_excess_return_pct: None,
            weighted_excess_positive_rate: None,
            weighted_mfe_pct: None,
            weighted_mae_pct: None,
        };
    }
    let average = |f: fn(&StrategyTriggerSimilarityRow) -> f64| {
        weighted.iter().map(|(i, w)| f(i) * w).sum::<f64>() / weight_sum
    };
    let excess = weighted
        .iter()
        .filter_map(|(i, w)| i.forward_excess_return_pct.map(|v| (v, *w)))
        .collect::<Vec<_>>();
    let excess_weight = excess.iter().map(|(_, w)| w).sum::<f64>();
    let weighted_excess = (excess_weight > EPS)
        .then(|| excess.iter().map(|(v, w)| v * w).sum::<f64>() / excess_weight);
    let weighted_median_excess = weighted_quantile(&excess, 0.5);
    let winsorized_excess = weighted_winsorized_mean(&excess, 0.1);
    let weighted_excess_positive_rate = (excess_weight > EPS).then(|| {
        excess
            .iter()
            .map(|(value, weight)| (*value > 0.0) as u8 as f64 * weight)
            .sum::<f64>()
            / excess_weight
            * 100.0
    });
    let effective = weight_sum * weight_sum / weight_sq_sum.max(EPS);
    StrategyTriggerSimilarityOutcomeSummary {
        sample_count: weighted.len(),
        effective_sample_count: effective,
        weighted_return_pct: Some(average(|i| i.forward_return_pct)),
        weighted_excess_return_pct: weighted_excess,
        shrunk_excess_return_pct: weighted_excess
            .map(|v| v * effective / (effective + SHRINKAGE_STRENGTH)),
        weighted_positive_rate: Some(
            average(|i| (i.forward_return_pct > 0.0) as u8 as f64) * 100.0,
        ),
        weighted_median_excess_return_pct: weighted_median_excess,
        winsorized_excess_return_pct: winsorized_excess,
        weighted_excess_positive_rate,
        weighted_mfe_pct: Some(average(|i| i.mfe_pct)),
        weighted_mae_pct: Some(average(|i| i.mae_pct)),
    }
}

fn build_rating_sample(
    sorted_items: &[StrategyTriggerSimilarityRow],
    all_trade_dates: &[String],
    window_trade_days: usize,
    outcome_trade_days: usize,
) -> Vec<StrategyTriggerSimilarityRow> {
    let date_indices = all_trade_dates
        .iter()
        .enumerate()
        .map(|(index, date)| (date.as_str(), index))
        .collect::<HashMap<_, _>>();
    let same_stock_exclusion = window_trade_days.max(outcome_trade_days).max(1);
    let outcome_exclusion = outcome_trade_days.max(1);
    let mut selected = Vec::with_capacity(RATING_SAMPLE_LIMIT);
    let mut selected_end_indices = Vec::<usize>::with_capacity(RATING_SAMPLE_LIMIT);
    let mut selected_by_stock = HashMap::<&str, Vec<usize>>::new();

    for item in sorted_items {
        if item.forward_excess_return_pct.is_none() {
            continue;
        }
        let Some(end_index) = date_indices
            .get(item.candidate_end_trade_date.as_str())
            .copied()
        else {
            continue;
        };
        let nearby_outcome_count = selected_end_indices
            .iter()
            .filter(|selected_index| selected_index.abs_diff(end_index) < outcome_exclusion)
            .count();
        if nearby_outcome_count >= RATING_MAX_PER_OUTCOME_WINDOW {
            continue;
        }
        if selected_by_stock
            .get(item.ts_code.as_str())
            .is_some_and(|indices| {
                indices
                    .iter()
                    .any(|selected_index| selected_index.abs_diff(end_index) < same_stock_exclusion)
            })
        {
            continue;
        }
        selected_end_indices.push(end_index);
        selected_by_stock
            .entry(item.ts_code.as_str())
            .or_default()
            .push(end_index);
        selected.push(item.clone());
        if selected.len() >= RATING_SAMPLE_LIMIT {
            break;
        }
    }
    selected
}

#[allow(clippy::too_many_arguments)]
pub fn get_strategy_trigger_similarity_page(
    source_path: String,
    trade_date: Option<String>,
    ts_code: String,
    window_trade_days: Option<u32>,
    pool_segments: Option<u32>,
    outcome_trade_days: Option<u32>,
    benchmark_index_code: Option<String>,
    limit: Option<u32>,
) -> Result<StrategyTriggerSimilarityPageData, String> {
    let source_path = source_path.trim().to_string();
    if source_path.is_empty() {
        return Err("source_path 不能为空".to_string());
    }
    let conn = open_result_conn(&source_path)?;
    let resolved_trade_date = resolve_existing_trade_date(&conn, trade_date)?;
    let resolved_ts_code = normalize_ts_code(&ts_code);
    let benchmark_index_code = resolve_benchmark_index_code(benchmark_index_code.as_deref())?;
    let window_trade_days = window_trade_days
        .map(|v| v as usize)
        .filter(|v| *v >= 3)
        .unwrap_or(DEFAULT_WINDOW_TRADE_DAYS);
    let pool_segments = pool_segments
        .map(|v| v as usize)
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_POOL_SEGMENTS)
        .min(MAX_POOL_SEGMENTS)
        .min(window_trade_days);
    let outcome_trade_days = outcome_trade_days
        .map(|v| v as usize)
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_OUTCOME_TRADE_DAYS);
    let limit = limit
        .map(|v| v as usize)
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_LIMIT);

    let all_trade_dates = load_all_trade_dates(&conn)?;
    let target_end_index = all_trade_dates
        .binary_search(&resolved_trade_date)
        .map_err(|_| format!("参考日不在评分交易日中: {resolved_trade_date}"))?;
    if target_end_index < outcome_trade_days {
        return Err("参考日前没有足够历史区间构建完整后验样本".to_string());
    }
    let target_start_index = (target_end_index + 1).saturating_sub(window_trade_days);
    let target_start_date = all_trade_dates[target_start_index].clone();
    let historical_cutoff_date = all_trade_dates[target_end_index - outcome_trade_days].clone();
    let target_events = load_target_rule_events(
        &conn,
        &resolved_ts_code,
        &target_start_date,
        &resolved_trade_date,
    )?;
    let target_rule_names = distinct_rule_names(&target_events);
    let schema = load_market_schema(&conn)?;
    let first_date = all_trade_dates
        .first()
        .map(String::as_str)
        .unwrap_or(&target_start_date);
    let environment = load_market_environment(&conn, first_date, &resolved_trade_date, &schema)?;
    let environment_fingerprints = build_environment_fingerprint_map(
        &environment,
        &all_trade_dates,
        window_trade_days,
        pool_segments,
    );
    let benchmark_rows = load_benchmark_rows(
        &conn,
        first_date,
        &resolved_trade_date,
        &benchmark_index_code,
    )?;
    let target_anchor = Anchor {
        id: 0,
        ts_code: resolved_ts_code.clone(),
        start_trade_date: target_start_date.clone(),
        end_trade_date: resolved_trade_date.clone(),
    };
    let target_context = SampleBuildContext {
        schema: &schema,
        target_rule_names: &target_rule_names,
        all_trade_dates: &all_trade_dates,
        environment_fingerprints: &environment_fingerprints,
        benchmark_rows: &benchmark_rows,
        pool_segments,
        outcome_trade_days,
        target_trade_date: &resolved_trade_date,
        include_outcome: false,
        include_summaries: false,
    };
    let target_sample =
        build_samples_for_chunk(&conn, std::slice::from_ref(&target_anchor), &target_context)?
            .into_iter()
            .next()
            .ok_or_else(|| {
                format!("{resolved_ts_code} 在 {resolved_trade_date} 没有完整量价窗口")
            })?;

    let earliest_candidate_date = all_trade_dates
        .get(window_trade_days.saturating_sub(1))
        .map(String::as_str)
        .unwrap_or(&all_trade_dates[0]);
    let (candidate_anchors, candidate_universe_count) = load_candidate_anchors(
        &conn,
        &target_rule_names,
        earliest_candidate_date,
        &historical_cutoff_date,
        &all_trade_dates,
        window_trade_days,
    )?;
    let candidate_anchor_count = candidate_anchors.len();
    let candidate_pool_truncated = candidate_universe_count > candidate_anchor_count;
    let candidate_context = SampleBuildContext {
        include_outcome: true,
        include_summaries: true,
        ..target_context
    };
    let name_map = build_name_map(&source_path).unwrap_or_default();
    let industry_map = build_industry_map(&source_path).unwrap_or_default();
    let concept_map = build_concepts_map(&source_path).unwrap_or_default();
    let mut items = Vec::new();
    let mut evaluated_anchor_count = 0;
    for chunk in candidate_anchors.chunks(ANCHOR_CHUNK_SIZE) {
        let samples = build_samples_for_chunk(&conn, chunk, &candidate_context)?;
        evaluated_anchor_count += samples.len();
        for sample in samples {
            let trigger_similarity = cosine_similarity(
                &target_sample.fingerprint.trigger,
                &sample.fingerprint.trigger,
            );
            let price_volume_similarity = channel_similarity(
                &target_sample.fingerprint.price_volume,
                &sample.fingerprint.price_volume,
            );
            let indicator_similarity = channel_similarity(
                &target_sample.fingerprint.indicators,
                &sample.fingerprint.indicators,
            );
            let market_similarity = channel_similarity(
                &target_sample.fingerprint.market,
                &sample.fingerprint.market,
            );
            let similarity_score = final_similarity(
                trigger_similarity,
                price_volume_similarity,
                indicator_similarity,
                market_similarity,
            );
            let Some(outcome) = sample.outcome else {
                continue;
            };
            items.push(StrategyTriggerSimilarityRow {
                name: name_map.get(&sample.anchor.ts_code).cloned(),
                industry: industry_map.get(&sample.anchor.ts_code).cloned(),
                concept: concept_map.get(&sample.anchor.ts_code).cloned(),
                ts_code: sample.anchor.ts_code,
                candidate_start_trade_date: sample.anchor.start_trade_date,
                candidate_end_trade_date: sample.anchor.end_trade_date,
                outcome_start_trade_date: outcome.start_trade_date,
                outcome_end_trade_date: outcome.end_trade_date,
                similarity_score,
                trigger_similarity,
                price_volume_similarity,
                indicator_similarity,
                market_similarity,
                matched_rule_count: sample.matched_rule_names.len(),
                matched_rule_names: sample.matched_rule_names,
                candidate_trigger_count: sample.trigger_count,
                forward_return_pct: outcome.return_pct,
                forward_excess_return_pct: outcome.excess_return_pct,
                mfe_pct: outcome.mfe_pct,
                mae_pct: outcome.mae_pct,
                total_score: sample.total_score,
                rank: sample.rank,
            });
        }
    }
    items.sort_by(|a, b| {
        b.similarity_score
            .total_cmp(&a.similarity_score)
            .then_with(|| b.trigger_similarity.total_cmp(&a.trigger_similarity))
            .then_with(|| b.candidate_end_trade_date.cmp(&a.candidate_end_trade_date))
            .then_with(|| a.ts_code.cmp(&b.ts_code))
    });
    let rating_sample = build_rating_sample(
        &items,
        &all_trade_dates,
        window_trade_days,
        outcome_trade_days,
    );
    let outcome_summary = summarize_outcomes(&rating_sample);
    items.truncate(limit);
    let target_dimension = target_sample.fingerprint.dimension();
    Ok(StrategyTriggerSimilarityPageData {
        resolved_trade_date: resolved_trade_date.clone(),
        resolved_ts_code: resolved_ts_code.clone(),
        window_trade_days: target_end_index + 1 - target_start_index,
        pool_segments,
        outcome_trade_days,
        historical_cutoff_date,
        benchmark_index_code,
        kernel_names: KERNEL_NAMES.iter().map(|v| v.to_string()).collect(),
        indicator_columns: schema.indicator_columns,
        candidate_universe_count,
        candidate_anchor_count,
        evaluated_anchor_count,
        candidate_pool_truncated,
        target: StrategyTriggerSimilarityTarget {
            ts_code: resolved_ts_code.clone(),
            name: name_map.get(&resolved_ts_code).cloned(),
            industry: industry_map.get(&resolved_ts_code).cloned(),
            concept: concept_map.get(&resolved_ts_code).cloned(),
            start_trade_date: target_start_date,
            end_trade_date: resolved_trade_date,
            trigger_count: target_events.len(),
            rule_names: target_rule_names,
            pooled_feature_dimension: target_dimension,
        },
        outcome_summary,
        items,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        BenchmarkObservation, FutureObservation, INDEX_TS_CODES, INDICATOR_SIMILARITY_WEIGHT,
        KERNEL_NAMES, MARKET_SIMILARITY_WEIGHT, PRICE_VOLUME_SIMILARITY_WEIGHT,
        StrategyTriggerSimilarityRow, TRIGGER_SIMILARITY_WEIGHT, build_outcome,
        build_rating_sample, cosine_similarity, final_similarity,
        list_strategy_trigger_similarity_benchmark_index_codes, resolve_benchmark_index_code,
        temporal_signature, weighted_quantile, weighted_winsorized_mean,
    };

    fn similarity_row(ts_code: &str, end_trade_date: &str) -> StrategyTriggerSimilarityRow {
        StrategyTriggerSimilarityRow {
            ts_code: ts_code.to_string(),
            name: None,
            industry: None,
            concept: None,
            candidate_start_trade_date: end_trade_date.to_string(),
            candidate_end_trade_date: end_trade_date.to_string(),
            outcome_start_trade_date: end_trade_date.to_string(),
            outcome_end_trade_date: end_trade_date.to_string(),
            similarity_score: 90.0,
            trigger_similarity: 90.0,
            price_volume_similarity: Some(90.0),
            indicator_similarity: Some(90.0),
            market_similarity: Some(90.0),
            matched_rule_count: 1,
            matched_rule_names: vec!["test".to_string()],
            candidate_trigger_count: 1,
            forward_return_pct: 1.0,
            forward_excess_return_pct: Some(0.5),
            mfe_pct: 2.0,
            mae_pct: -1.0,
            total_score: Some(1.0),
            rank: Some(1),
        }
    }

    #[test]
    fn temporal_signature_contains_pool_and_multiple_kernels() {
        let values = (1..=20).map(|value| Some(value as f64)).collect::<Vec<_>>();
        let signature = temporal_signature(&values, 5, true).expect("signature");
        assert_eq!(signature.len(), 5 + KERNEL_NAMES.len() + 1);
    }

    #[test]
    fn cosine_similarity_is_percentage_scaled() {
        assert!((cosine_similarity(&[1.0, 0.0], &[1.0, 0.0]) - 100.0).abs() < 1e-9);
        assert!(cosine_similarity(&[1.0, 0.0], &[-1.0, 0.0]).abs() < 1e-9);
        assert!((cosine_similarity(&[1.0, 0.0], &[0.0, 1.0]) - 50.0).abs() < 1e-9);
    }

    #[test]
    fn calibrated_similarity_weights_sum_to_one_and_renormalize_missing_channels() {
        let weight_sum = TRIGGER_SIMILARITY_WEIGHT
            + PRICE_VOLUME_SIMILARITY_WEIGHT
            + INDICATOR_SIMILARITY_WEIGHT
            + MARKET_SIMILARITY_WEIGHT;
        assert!((weight_sum - 1.0).abs() < 1e-12);
        let expected = 100.0 * TRIGGER_SIMILARITY_WEIGHT
            / (TRIGGER_SIMILARITY_WEIGHT + PRICE_VOLUME_SIMILARITY_WEIGHT);
        assert!((final_similarity(100.0, Some(0.0), None, None) - expected).abs() < 1e-9);
    }

    #[test]
    fn robust_outcome_statistics_resist_large_positive_outlier() {
        let values = (0..9)
            .map(|value| (value as f64, 1.0))
            .chain(std::iter::once((100.0, 1.0)))
            .collect::<Vec<_>>();
        assert_eq!(weighted_quantile(&values, 0.5), Some(4.0));
        assert_eq!(weighted_quantile(&values, 0.9), Some(8.0));
        let winsorized = weighted_winsorized_mean(&values, 0.1).expect("winsorized mean");
        assert!((winsorized - 4.4).abs() < 1e-12);
    }

    #[test]
    fn benchmark_index_is_configurable_but_validated() {
        assert_eq!(
            list_strategy_trigger_similarity_benchmark_index_codes(),
            INDEX_TS_CODES.map(str::to_string)
        );
        for code in INDEX_TS_CODES {
            assert_eq!(
                resolve_benchmark_index_code(Some(code)).as_deref(),
                Ok(code)
            );
        }
        assert!(resolve_benchmark_index_code(Some("invalid")).is_err());
    }

    #[test]
    fn outcome_uses_next_day_open_for_stock_and_benchmark() {
        let future = vec![
            FutureObservation {
                trade_date: "20240102".to_string(),
                open: 100.0,
                close: 105.0,
                high: 110.0,
                low: 95.0,
            },
            FutureObservation {
                trade_date: "20240103".to_string(),
                open: 106.0,
                close: 121.0,
                high: 125.0,
                low: 104.0,
            },
        ];
        let benchmark = std::collections::HashMap::from([
            (
                "20240102".to_string(),
                BenchmarkObservation {
                    open: 100.0,
                    close: 103.0,
                },
            ),
            (
                "20240103".to_string(),
                BenchmarkObservation {
                    open: 104.0,
                    close: 110.0,
                },
            ),
        ]);
        let outcome = build_outcome(&[], &future, 2, &benchmark).expect("outcome");
        assert_eq!(outcome.start_trade_date, "20240102");
        assert!((outcome.return_pct - 21.0).abs() < 1e-9);
        assert!((outcome.excess_return_pct.expect("excess") - 11.0).abs() < 1e-9);
        assert!((outcome.mfe_pct - 25.0).abs() < 1e-9);
        assert!((outcome.mae_pct + 5.0).abs() < 1e-9);
    }

    #[test]
    fn rating_sample_rejects_overlapping_stock_and_market_windows() {
        let dates = (1..=12)
            .map(|day| format!("202401{day:02}"))
            .collect::<Vec<_>>();
        let rows = vec![
            similarity_row("A", "20240105"),
            similarity_row("A", "20240106"),
            similarity_row("B", "20240105"),
            similarity_row("C", "20240105"),
            similarity_row("D", "20240105"),
            similarity_row("E", "20240110"),
        ];
        let selected = build_rating_sample(&rows, &dates, 5, 3);
        assert_eq!(
            selected
                .iter()
                .map(|item| item.ts_code.as_str())
                .collect::<Vec<_>>(),
            vec!["A", "B", "C", "E"]
        );
    }
}
