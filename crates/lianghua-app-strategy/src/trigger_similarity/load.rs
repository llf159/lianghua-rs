use crate::trigger_similarity::{
    Anchor, EPS, FutureObservation, MarketEnvironment, MarketObservation, MarketSchema,
    RuleCatalog, RuleEvent,
};

use crate::data::result_db_path;
use crate::data::source_db_path;
use crate::download::runner::INDEX_TS_CODES;
use duckdb::AccessMode;
use duckdb::Config;
use duckdb::Connection;
use duckdb::params;
use duckdb::params_from_iter;
use lianghua_app_shared::normalize_trade_date;
use lianghua_app_shared::resolve_trade_date;
use std::collections::HashMap;
use std::collections::HashSet;
pub(super) fn resolve_benchmark_index_code(value: Option<&str>) -> Result<String, String> {
    let code = value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("000001.SH")
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

pub(super) fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub(super) fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

pub(super) fn open_result_conn(source_path: &str) -> Result<Connection, String> {
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

pub(super) fn resolve_existing_trade_date(
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

pub(super) fn load_all_trade_dates(conn: &Connection) -> Result<Vec<String>, String> {
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

pub(super) fn load_market_schema(conn: &Connection) -> Result<MarketSchema, String> {
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
    Ok(MarketSchema {
        columns,
        indicator_columns,
    })
}

pub(super) fn column_expr(schema: &MarketSchema, logical_name: &str, alias: &str) -> String {
    schema
        .columns
        .get(&logical_name.to_ascii_lowercase())
        .map(|actual| format!("TRY_CAST({alias}.{} AS DOUBLE)", quote_ident(actual)))
        .unwrap_or_else(|| "CAST(NULL AS DOUBLE)".to_string())
}

pub(super) fn anchors_values_sql(anchors: &[Anchor]) -> String {
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

pub(super) fn load_market_rows(
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

pub(super) fn load_rule_rows(
    conn: &Connection,
    anchors: &[Anchor],
    rule_catalog: &mut RuleCatalog,
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
                rule_id: rule_catalog
                    .intern(row.get(1).map_err(|e| format!("读取规则名失败: {e}"))?),
                trade_date: row.get(2).map_err(|e| format!("读取触发日期失败: {e}"))?,
                score: row.get(3).map_err(|e| format!("读取规则分数失败: {e}"))?,
            });
    }
    Ok(out)
}

pub(super) fn load_future_rows(
    conn: &Connection,
    anchors: &[Anchor],
    outcome_trade_days: usize,
    target_trade_date: &str,
) -> Result<HashMap<usize, Vec<FutureObservation>>, String> {
    if anchors.is_empty() {
        return Ok(HashMap::new());
    }
    let mut stock_codes = HashSet::with_capacity(anchors.len());
    let has_repeated_stock = anchors
        .iter()
        .any(|anchor| !stock_codes.insert(anchor.ts_code.as_str()));
    let sql = if has_repeated_stock {
        format!(
            r#"
        WITH anchors(anchor_id, ts_code, start_date, end_date) AS (VALUES {}),
        stock_scope AS (
            SELECT ts_code, MIN(end_date) AS first_end_date FROM anchors GROUP BY ts_code
        ),
        numbered AS MATERIALIZED (
            SELECT s.ts_code, s.trade_date,
                   TRY_CAST(s.open AS DOUBLE) AS open_value,
                   TRY_CAST(s.close AS DOUBLE) AS close_value,
                   TRY_CAST(s.high AS DOUBLE) AS high_value,
                   TRY_CAST(s.low AS DOUBLE) AS low_value,
                   ROW_NUMBER() OVER (PARTITION BY s.ts_code ORDER BY s.trade_date) AS rn
            FROM trigger_market_db.stock_data s JOIN stock_scope scope ON s.ts_code=scope.ts_code
            WHERE s.trade_date > scope.first_end_date AND s.trade_date <= {} AND s.adj_type='qfq'
        ),
        anchor_positions AS (
            SELECT a.anchor_id, a.ts_code, COALESCE(n.rn, 0) AS end_rn
            FROM anchors a ASOF LEFT JOIN numbered n
              ON a.ts_code=n.ts_code AND a.end_date >= n.trade_date
        )
        SELECT a.anchor_id, n.trade_date, n.open_value, n.close_value, n.high_value, n.low_value
        FROM anchor_positions a JOIN numbered n ON n.ts_code=a.ts_code
          AND n.rn > a.end_rn AND n.rn <= a.end_rn + {}
        ORDER BY a.anchor_id, n.trade_date
        "#,
            anchors_values_sql(anchors),
            sql_string_literal(target_trade_date),
            outcome_trade_days,
        )
    } else {
        format!(
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
        )
    };
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

pub(super) fn load_summary_rows(
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

pub(super) fn load_market_environment(
    conn: &Connection,
    start_date: &str,
    end_date: &str,
    schema: &MarketSchema,
) -> Result<MarketEnvironment, String> {
    let index_codes = load_market_environment_index_codes(conn)?;
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
        channel_count: 7 + index_codes.len(),
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

    if index_codes.is_empty() {
        return Ok(environment);
    }

    let placeholders = std::iter::repeat_n("?", index_codes.len())
        .collect::<Vec<_>>()
        .join(", ");
    let index_sql = format!(
        "SELECT UPPER(TRIM(ts_code)), trade_date, TRY_CAST(pct_chg AS DOUBLE) \
         FROM trigger_market_db.stock_data \
         WHERE adj_type = 'ind' AND trade_date >= ? AND trade_date <= ? \
           AND UPPER(TRIM(ts_code)) IN ({placeholders})"
    );
    let mut values = vec![start_date.to_string(), end_date.to_string()];
    values.extend(index_codes.iter().cloned());
    let index_positions = index_codes
        .iter()
        .enumerate()
        .map(|(index, code)| (code.as_str(), index))
        .collect::<HashMap<_, _>>();
    let mut stmt = conn
        .prepare(&index_sql)
        .map_err(|e| format!("预编译指数市场环境查询失败: {e}"))?;
    let mut rows = stmt
        .query(params_from_iter(values.iter()))
        .map_err(|e| format!("查询指数市场环境失败: {e}"))?;
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取指数市场环境失败: {e}"))?
    {
        let code: String = row.get(0).map_err(|e| format!("读取指数代码失败: {e}"))?;
        let date: String = row.get(1).map_err(|e| format!("读取指数日期失败: {e}"))?;
        let value: Option<f64> = row.get(2).map_err(|e| format!("读取指数涨跌失败: {e}"))?;
        if let Some(index) = index_positions.get(code.as_str()).copied() {
            environment
                .by_date
                .entry(date)
                .or_insert_with(|| vec![None; environment.channel_count])[7 + index] = value;
        }
    }
    Ok(environment)
}

pub(super) fn load_market_environment_index_codes(
    conn: &Connection,
) -> Result<Vec<String>, String> {
    let mut stmt = conn
        .prepare(
            "SELECT DISTINCT UPPER(TRIM(ts_code)) AS ts_code \
             FROM trigger_market_db.stock_data \
             WHERE adj_type='ind' AND ts_code IS NOT NULL AND TRIM(ts_code)<>'' \
             ORDER BY ts_code",
        )
        .map_err(|e| format!("预编译市场环境指数列表查询失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询市场环境指数列表失败: {e}"))?;
    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取市场环境指数列表失败: {e}"))?
    {
        let code: String = row
            .get(0)
            .map_err(|e| format!("读取市场环境指数代码失败: {e}"))?;
        if !is_beijing_exchange_index_code(&code) {
            out.push(code);
        }
    }
    Ok(out)
}

pub(super) fn is_beijing_exchange_index_code(code: &str) -> bool {
    let normalized = code.trim().to_ascii_uppercase();
    normalized.ends_with(".BJ") || normalized.starts_with("899")
}

#[cfg(test)]
mod tests {
    use crate::download::runner::INDEX_TS_CODES;
    use crate::trigger_similarity::load::is_beijing_exchange_index_code;
    use crate::trigger_similarity::load::list_strategy_trigger_similarity_benchmark_index_codes;
    use crate::trigger_similarity::load::load_market_environment_index_codes;
    use crate::trigger_similarity::load::load_market_schema;
    use crate::trigger_similarity::load::resolve_benchmark_index_code;
    use duckdb::Connection;

    #[test]
    fn rule_ids_and_idf_weights_are_shared_across_batches() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE rule_details(ts_code VARCHAR, trade_date VARCHAR, rule_name VARCHAR, rule_score DOUBLE);
            INSERT INTO rule_details VALUES
            ('A', '20240101', '规则甲', 1.0), ('A', '20240102', '规则甲', 2.0),
            ('B', '20240102', '规则乙', 1.0), ('A', '20240103', '当日新规则', 1.0);").unwrap();
        let mut catalog = crate::trigger_similarity::RuleCatalog::default();
        let target = crate::trigger_similarity::Anchor {
            id: 0,
            ts_code: "A".into(),
            start_trade_date: "20240103".into(),
            end_trade_date: "20240103".into(),
        };
        let current =
            crate::trigger_similarity::load::load_rule_rows(&conn, &[target], &mut catalog)
                .unwrap();
        let current_id = current[&0][0].rule_id;
        let weights = crate::trigger_similarity::fingerprint::load_rule_idf_weights(
            &conn,
            "20240101",
            "20240102",
            &mut catalog,
        )
        .unwrap();
        let historical = crate::trigger_similarity::Anchor {
            id: 1,
            ts_code: "A".into(),
            start_trade_date: "20240101".into(),
            end_trade_date: "20240102".into(),
        };
        let batch = crate::trigger_similarity::load::load_rule_rows(
            &conn,
            &[historical.clone()],
            &mut catalog,
        )
        .unwrap();
        let repeated =
            crate::trigger_similarity::load::load_rule_rows(&conn, &[historical], &mut catalog)
                .unwrap();
        let id = batch[&1][0].rule_id;
        assert_eq!(batch[&1][1].rule_id, id);
        assert_eq!(repeated[&1][0].rule_id, id);
        assert_eq!(catalog.names[id], "规则甲");
        assert_eq!(catalog.names[current_id], "当日新规则");
        assert_eq!(
            crate::trigger_similarity::fingerprint::rule_weight(&weights, current_id),
            1.0
        );
        assert_eq!(
            crate::trigger_similarity::fingerprint::rule_weight(&weights, id),
            (1.0_f64 + 3.0 / 2.0).ln().clamp(1.0, 6.0)
        );
        let late_id = catalog.intern("后续批次新规则".into());
        assert_eq!(
            crate::trigger_similarity::fingerprint::rule_weight(&weights, late_id),
            1.0
        );
        assert_eq!(catalog.intern("规则甲".into()), id);
    }

    #[test]
    fn bounded_future_query_matches_original_row_number_semantics() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("ATTACH ':memory:' AS trigger_market_db;
            CREATE TABLE trigger_market_db.stock_data(ts_code VARCHAR, trade_date VARCHAR, adj_type VARCHAR, open DOUBLE, close DOUBLE, high DOUBLE, low DOUBLE);
            INSERT INTO trigger_market_db.stock_data VALUES
            ('A', '20240102', 'qfq', 10, 11, 12, 9),
            ('A', '20240104', 'qfq', NULL, 11, 12, 9),
            ('A', '20240107', 'qfq', 10, 11, 12, 9),
            ('A', '20240108', 'qfq', 10, 'NaN', 12, 9),
            ('A', '20240109', 'qfq', 10, 11, 'Infinity', 9),
            ('A', '20240112', 'qfq', 11, 12, 13, 10),
            ('A', '20240107', 'hfq', 90, 91, 92, 89),
            ('B', '20240103', 'qfq', 20, 21, 22, 19),
            ('B', '20240109', 'qfq', 21, 22, 23, 20);").unwrap();
        let anchors = [
            ("A", "20240101"),
            ("A", "20240102"),
            ("A", "20240105"),
            ("A", "20240107"),
            ("A", "20240112"),
            ("B", "20240102"),
            ("B", "20240104"),
            ("MISSING", "20240101"),
        ]
        .into_iter()
        .enumerate()
        .map(|(id, (code, date))| crate::trigger_similarity::Anchor {
            id,
            ts_code: code.into(),
            start_trade_date: "20240101".into(),
            end_trade_date: date.into(),
        })
        .collect::<Vec<_>>();
        let unique_anchors = vec![anchors[0].clone(), anchors[5].clone(), anchors[7].clone()];
        for anchors in [&anchors, &unique_anchors] {
            for target_date in ["20240109", "20240120"] {
                for horizon in [0, 1, 3, 5] {
                    let actual = crate::trigger_similarity::load::load_future_rows(
                        &conn,
                        &anchors,
                        horizon,
                        target_date,
                    )
                    .unwrap();
                    let sql = format!(
                        r#"
                    WITH anchors(anchor_id, ts_code, start_date, end_date) AS (VALUES {}),
                    future AS (
                        SELECT a.anchor_id, s.trade_date, s.open, s.close, s.high, s.low,
                               ROW_NUMBER() OVER (PARTITION BY a.anchor_id ORDER BY s.trade_date) rn
                        FROM anchors a JOIN trigger_market_db.stock_data s ON s.ts_code=a.ts_code
                          AND s.trade_date>a.end_date AND s.trade_date<={} AND s.adj_type='qfq'
                    )
                    SELECT anchor_id, trade_date, open, close, high, low FROM future WHERE rn<={}
                    ORDER BY anchor_id, trade_date
                "#,
                        crate::trigger_similarity::load::anchors_values_sql(&anchors),
                        crate::trigger_similarity::load::sql_string_literal(target_date),
                        horizon
                    );
                    let mut stmt = conn.prepare(&sql).unwrap();
                    let mut rows = stmt.query([]).unwrap();
                    let mut expected = Vec::new();
                    while let Some(row) = rows.next().unwrap() {
                        let values = [
                            row.get::<_, Option<f64>>(2).unwrap(),
                            row.get(3).unwrap(),
                            row.get(4).unwrap(),
                            row.get(5).unwrap(),
                        ];
                        if let [Some(open), Some(close), Some(high), Some(low)] = values {
                            if values.iter().all(|value| value.unwrap().is_finite()) {
                                expected.push((
                                    row.get::<_, usize>(0).unwrap(),
                                    row.get::<_, String>(1).unwrap(),
                                    open,
                                    close,
                                    high,
                                    low,
                                ));
                            }
                        }
                    }
                    let mut actual = actual
                        .into_iter()
                        .flat_map(|(id, rows)| {
                            rows.into_iter().map(move |row| {
                                (id, row.trade_date, row.open, row.close, row.high, row.low)
                            })
                        })
                        .collect::<Vec<_>>();
                    actual.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
                    assert_eq!(actual, expected, "target={target_date}, horizon={horizon}");
                }
            }
        }
        assert!(
            crate::trigger_similarity::load::load_future_rows(&conn, &[], 3, "20240120")
                .unwrap()
                .is_empty()
        );
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
    fn market_environment_uses_every_non_beijing_exchange_index() {
        assert!(is_beijing_exchange_index_code("899050.BJ"));
        assert!(is_beijing_exchange_index_code("899050"));
        assert!(!is_beijing_exchange_index_code("000001.SH"));

        let conn = Connection::open_in_memory().expect("open memory db");
        conn.execute_batch(
            "ATTACH ':memory:' AS trigger_market_db; \
             CREATE TABLE trigger_market_db.stock_data \
               (ts_code VARCHAR, trade_date VARCHAR, adj_type VARCHAR); \
             INSERT INTO trigger_market_db.stock_data VALUES \
               ('399001.SZ', '20240102', 'ind'), \
               ('000001.SH', '20240102', 'ind'), \
               ('000510.SH', '20240102', 'ind'), \
               ('899050.BJ', '20240102', 'ind'), \
               ('899999', '20240102', 'ind'), \
               ('000001.SZ', '20240102', 'qfq');",
        )
        .expect("seed index rows");

        assert_eq!(
            load_market_environment_index_codes(&conn).expect("load environment indexes"),
            vec!["000001.SH", "000510.SH", "399001.SZ"]
        );
    }

    #[test]
    fn market_schema_keeps_all_numeric_indicator_columns() {
        let conn = Connection::open_in_memory().expect("open in-memory DuckDB");
        let indicator_columns = (0..30)
            .map(|index| format!("indicator_{index:02} DOUBLE"))
            .collect::<Vec<_>>()
            .join(", ");
        conn.execute_batch(&format!(
            "ATTACH ':memory:' AS trigger_market_db; \
             CREATE TABLE trigger_market_db.stock_data (\
                 ts_code VARCHAR, trade_date VARCHAR, adj_type VARCHAR, {indicator_columns});"
        ))
        .expect("create market schema");
        let schema = load_market_schema(&conn).expect("load market schema");
        assert_eq!(schema.indicator_columns.len(), 30);
        assert_eq!(
            schema.indicator_columns.first().map(String::as_str),
            Some("indicator_00")
        );
        assert_eq!(
            schema.indicator_columns.last().map(String::as_str),
            Some("indicator_29")
        );
    }
}
