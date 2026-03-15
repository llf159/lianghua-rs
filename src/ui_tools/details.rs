use std::{collections::HashMap, fs, path::Path};

use duckdb::{Connection, params};
use serde::Serialize;

use crate::{
    data::{RuleTag, ScoreConfig},
    data::{result_db_path, score_rule_path, source_db_path},
    ui_tools::{
        build_circ_mv_map, build_concepts_map, build_name_map, build_total_mv_map,
        resolve_trade_date,
    },
};

const DEFAULT_ADJ_TYPE: &str = "qfq";
const DEFAULT_ROW_WEIGHTS: [u32; 4] = [46, 18, 18, 18];

#[derive(Debug, Serialize)]
pub struct DetailOverview {
    pub ts_code: String,
    pub name: Option<String>,
    pub trade_date: Option<String>,
    pub total_score: Option<f64>,
    pub rank: Option<i64>,
    pub total: Option<i64>,
    pub total_mv_yi: Option<f64>,
    pub circ_mv_yi: Option<f64>,
    pub concept: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DetailPrevRankRow {
    pub trade_date: String,
    pub rank: Option<i64>,
    pub total: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct DetailKlineRow {
    pub trade_date: String,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: Option<f64>,
    pub vol: Option<f64>,
    pub amount: Option<f64>,
    pub tor: Option<f64>,
    pub brick: Option<f64>,
    pub j: Option<f64>,
    pub duokong_short: Option<f64>,
    pub duokong_long: Option<f64>,
    pub bupiao_short: Option<f64>,
    pub bupiao_long: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct DetailKlinePanel {
    pub key: String,
    pub label: String,
    pub kind: Option<String>,
    pub series_keys: Option<Vec<String>>,
    pub row_weight: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct DetailKlinePayload {
    pub items: Option<Vec<DetailKlineRow>>,
    pub panels: Option<Vec<DetailKlinePanel>>,
    pub default_window: Option<u32>,
    pub chart_height: Option<u32>,
    pub row_weights: Option<Vec<u32>>,
    pub watermark_name: Option<String>,
    pub watermark_code: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DetailStrategyTriggerRow {
    pub rule_name: String,
    pub rule_score: Option<f64>,
    pub hit_date: Option<String>,
    pub lag: Option<i64>,
    pub explain: Option<String>,
    pub tag: Option<String>,
    pub when: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DetailStrategyPayload {
    pub triggered: Option<Vec<DetailStrategyTriggerRow>>,
    pub untriggered: Option<Vec<DetailStrategyTriggerRow>>,
}

#[derive(Debug, Serialize)]
pub struct StockDetailPageData {
    pub resolved_trade_date: Option<String>,
    pub resolved_ts_code: Option<String>,
    pub overview: Option<DetailOverview>,
    pub prev_ranks: Option<Vec<DetailPrevRankRow>>,
    pub kline: Option<DetailKlinePayload>,
    pub strategy_triggers: Option<DetailStrategyPayload>,
}

#[derive(Debug)]
struct RuleMeta {
    rule_name: String,
    explain: String,
    tag: String,
    when: String,
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

fn split_ts_code(ts_code: &str) -> String {
    ts_code.split('.').next().unwrap_or(ts_code).to_string()
}

fn query_total_for_trade_date(conn: &Connection, trade_date: &str) -> Result<Option<i64>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT COUNT(*)
            FROM score_summary
            WHERE trade_date = ?
            "#,
        )
        .map_err(|e| format!("预编译总样本数失败: {e}"))?;
    let mut rows = stmt
        .query(params![trade_date])
        .map_err(|e| format!("查询总样本数失败: {e}"))?;

    if let Some(row) = rows.next().map_err(|e| format!("读取总样本数失败: {e}"))? {
        let total: Option<i64> = row
            .get(0)
            .map_err(|e| format!("读取总样本数字段失败: {e}"))?;
        Ok(total)
    } else {
        Ok(None)
    }
}

fn query_detail_overview(
    conn: &Connection,
    source_path: &str,
    effective_trade_date: &str,
    ts_code: &str,
) -> Result<DetailOverview, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT total_score, rank
            FROM score_summary
            WHERE trade_date = ? AND ts_code = ?
            LIMIT 1
            "#,
        )
        .map_err(|e| format!("预编译详情总览失败: {e}"))?;
    let mut rows = stmt
        .query(params![effective_trade_date, ts_code])
        .map_err(|e| format!("查询详情总览失败: {e}"))?;

    let Some(row) = rows.next().map_err(|e| format!("读取详情总览失败: {e}"))? else {
        return Err(format!(
            "未找到 {} 在 {} 的排名结果",
            ts_code, effective_trade_date
        ));
    };

    let total = query_total_for_trade_date(conn, effective_trade_date)?;
    let name_map = build_name_map(source_path)?;
    let total_mv_map = build_total_mv_map(source_path)?;
    let circ_mv_map = build_circ_mv_map(source_path)?;
    let concept_map = build_concepts_map(source_path)?;

    Ok(DetailOverview {
        ts_code: ts_code.to_string(),
        name: name_map.get(ts_code).cloned(),
        trade_date: Some(effective_trade_date.to_string()),
        total_score: row
            .get(0)
            .map_err(|e| format!("读取详情 total_score 失败: {e}"))?,
        rank: row.get(1).map_err(|e| format!("读取详情 rank 失败: {e}"))?,
        total,
        total_mv_yi: total_mv_map.get(ts_code).copied(),
        circ_mv_yi: circ_mv_map.get(ts_code).copied(),
        concept: concept_map.get(ts_code).cloned(),
    })
}

fn query_rank_history(conn: &Connection, ts_code: &str) -> Result<Vec<DetailPrevRankRow>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT
                s.trade_date,
                s.rank,
                totals.total
            FROM score_summary AS s
            LEFT JOIN (
                SELECT trade_date, COUNT(*) AS total
                FROM score_summary
                GROUP BY trade_date
            ) AS totals
              ON totals.trade_date = s.trade_date
            WHERE s.ts_code = ?
            ORDER BY s.trade_date DESC
            "#,
        )
        .map_err(|e| format!("预编译排名历史失败: {e}"))?;
    let mut rows = stmt
        .query(params![ts_code])
        .map_err(|e| format!("查询排名历史失败: {e}"))?;

    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(|e| format!("读取排名历史失败: {e}"))? {
        out.push(DetailPrevRankRow {
            trade_date: row
                .get(0)
                .map_err(|e| format!("读取排名历史日期失败: {e}"))?,
            rank: row.get(1).map_err(|e| format!("读取排名历史值失败: {e}"))?,
            total: row
                .get(2)
                .map_err(|e| format!("读取排名历史总数失败: {e}"))?,
        });
    }

    Ok(out)
}

fn default_kline_panels() -> Vec<DetailKlinePanel> {
    vec![
        DetailKlinePanel {
            key: "price".to_string(),
            label: "主K".to_string(),
            kind: Some("candles".to_string()),
            series_keys: Some(vec![
                "open".to_string(),
                "high".to_string(),
                "low".to_string(),
                "close".to_string(),
                "duokong_short".to_string(),
                "duokong_long".to_string(),
            ]),
            row_weight: Some(DEFAULT_ROW_WEIGHTS[0]),
        },
        DetailKlinePanel {
            key: "indicator".to_string(),
            label: "指标".to_string(),
            kind: Some("line".to_string()),
            series_keys: Some(vec![
                "j".to_string(),
                "bupiao_long".to_string(),
                "bupiao_short".to_string(),
            ]),
            row_weight: Some(DEFAULT_ROW_WEIGHTS[1]),
        },
        DetailKlinePanel {
            key: "volume".to_string(),
            label: "量能".to_string(),
            kind: Some("bar".to_string()),
            series_keys: Some(vec!["vol".to_string()]),
            row_weight: Some(DEFAULT_ROW_WEIGHTS[2]),
        },
        DetailKlinePanel {
            key: "brick".to_string(),
            label: "砖型图".to_string(),
            kind: Some("brick".to_string()),
            series_keys: Some(vec!["brick".to_string()]),
            row_weight: Some(DEFAULT_ROW_WEIGHTS[3]),
        },
    ]
}

fn query_kline(
    source_conn: &Connection,
    ts_code: &str,
    default_window_days: usize,
    watermark_name: Option<String>,
) -> Result<DetailKlinePayload, String> {
    let mut stmt = source_conn
        .prepare(
            r#"
            SELECT
                trade_date,
                TRY_CAST(open AS DOUBLE) AS open,
                TRY_CAST(high AS DOUBLE) AS high,
                TRY_CAST(low AS DOUBLE) AS low,
                TRY_CAST(close AS DOUBLE) AS close,
                TRY_CAST(vol AS DOUBLE) AS vol,
                TRY_CAST(amount AS DOUBLE) AS amount,
                TRY_CAST(tor AS DOUBLE) AS tor,
                TRY_CAST(brick AS DOUBLE) AS brick,
                TRY_CAST(j AS DOUBLE) AS j,
                TRY_CAST(duokong_short AS DOUBLE) AS duokong_short,
                TRY_CAST(duokong_long AS DOUBLE) AS duokong_long,
                TRY_CAST(bupiao_short AS DOUBLE) AS bupiao_short,
                TRY_CAST(bupiao_long AS DOUBLE) AS bupiao_long
            FROM stock_data
            WHERE ts_code = ? AND adj_type = ?
            ORDER BY trade_date ASC
            "#,
        )
        .map_err(|e| format!("预编译K线查询失败: {e}"))?;
    let mut rows = stmt
        .query(params![ts_code, DEFAULT_ADJ_TYPE])
        .map_err(|e| format!("查询K线数据失败: {e}"))?;

    let mut items = Vec::new();
    while let Some(row) = rows.next().map_err(|e| format!("读取K线数据失败: {e}"))? {
        items.push(DetailKlineRow {
            trade_date: row.get(0).map_err(|e| format!("读取K线日期失败: {e}"))?,
            open: row.get(1).map_err(|e| format!("读取 open 失败: {e}"))?,
            high: row.get(2).map_err(|e| format!("读取 high 失败: {e}"))?,
            low: row.get(3).map_err(|e| format!("读取 low 失败: {e}"))?,
            close: row.get(4).map_err(|e| format!("读取 close 失败: {e}"))?,
            vol: row.get(5).map_err(|e| format!("读取 vol 失败: {e}"))?,
            amount: row.get(6).map_err(|e| format!("读取 amount 失败: {e}"))?,
            tor: row.get(7).map_err(|e| format!("读取 tor 失败: {e}"))?,
            brick: row.get(8).map_err(|e| format!("读取 brick 失败: {e}"))?,
            j: row.get(9).map_err(|e| format!("读取 j 失败: {e}"))?,
            duokong_short: row
                .get(10)
                .map_err(|e| format!("读取 duokong_short 失败: {e}"))?,
            duokong_long: row
                .get(11)
                .map_err(|e| format!("读取 duokong_long 失败: {e}"))?,
            bupiao_short: row
                .get(12)
                .map_err(|e| format!("读取 bupiao_short 失败: {e}"))?,
            bupiao_long: row
                .get(13)
                .map_err(|e| format!("读取 bupiao_long 失败: {e}"))?,
        });
    }

    Ok(DetailKlinePayload {
        items: Some(items),
        panels: Some(default_kline_panels()),
        default_window: Some(default_window_days as u32),
        chart_height: Some(820),
        row_weights: Some(DEFAULT_ROW_WEIGHTS.to_vec()),
        watermark_name,
        watermark_code: Some(split_ts_code(ts_code)),
    })
}

fn load_rule_meta_list(source_path: &str) -> Result<Vec<RuleMeta>, String> {
    let imported_rule_path = score_rule_path(source_path);
    let project_rule_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("source")
        .join("score_rule.toml");
    let rule_path = if imported_rule_path.exists() {
        imported_rule_path
    } else {
        project_rule_path
    };
    let text = fs::read_to_string(&rule_path)
        .map_err(|e| format!("读取规则文件失败: path={}, err={e}", rule_path.display()))?;
    let config: ScoreConfig =
        toml::from_str(&text).map_err(|e| format!("解析规则文件失败: {e}"))?;

    Ok(config
        .rule
        .into_iter()
        .map(|rule| RuleMeta {
            rule_name: rule.name,
            explain: rule.explain,
            tag: tag_label(rule.tag).to_string(),
            when: rule.when,
        })
        .collect())
}

fn tag_label(tag: RuleTag) -> &'static str {
    match tag {
        RuleTag::Normal => "普通",
        RuleTag::Opportunity => "机会",
    }
}

fn load_current_rule_score_map(
    conn: &Connection,
    ts_code: &str,
    effective_trade_date: &str,
) -> Result<HashMap<String, f64>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT rule_name, rule_score
            FROM score_details
            WHERE ts_code = ? AND trade_date = ?
            "#,
        )
        .map_err(|e| format!("预编译当前策略得分失败: {e}"))?;
    let mut rows = stmt
        .query(params![ts_code, effective_trade_date])
        .map_err(|e| format!("查询当前策略得分失败: {e}"))?;

    let mut out = HashMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取当前策略得分失败: {e}"))?
    {
        let rule_name: String = row
            .get(0)
            .map_err(|e| format!("读取 rule_name 失败: {e}"))?;
        let rule_score: Option<f64> = row
            .get(1)
            .map_err(|e| format!("读取 rule_score 失败: {e}"))?;
        out.insert(rule_name, rule_score.unwrap_or(0.0));
    }

    Ok(out)
}

fn load_latest_hit_date_map(
    conn: &Connection,
    ts_code: &str,
    effective_trade_date: &str,
) -> Result<HashMap<String, String>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT rule_name, MAX(trade_date) AS hit_date
            FROM score_details
            WHERE ts_code = ? AND trade_date <= ? AND rule_score != 0
            GROUP BY rule_name
            "#,
        )
        .map_err(|e| format!("预编译最近命中日期失败: {e}"))?;
    let mut rows = stmt
        .query(params![ts_code, effective_trade_date])
        .map_err(|e| format!("查询最近命中日期失败: {e}"))?;

    let mut out = HashMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取最近命中日期失败: {e}"))?
    {
        let rule_name: String = row
            .get(0)
            .map_err(|e| format!("读取 rule_name 失败: {e}"))?;
        let hit_date: String = row.get(1).map_err(|e| format!("读取 hit_date 失败: {e}"))?;
        out.insert(rule_name, hit_date);
    }

    Ok(out)
}

fn build_trade_day_index_map(conn: &Connection) -> Result<HashMap<String, usize>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT DISTINCT trade_date
            FROM score_summary
            ORDER BY trade_date ASC
            "#,
        )
        .map_err(|e| format!("预编译交易日索引失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询交易日索引失败: {e}"))?;

    let mut out = HashMap::new();
    let mut index = 0usize;
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取交易日索引失败: {e}"))?
    {
        let trade_date: String = row.get(0).map_err(|e| format!("读取交易日字段失败: {e}"))?;
        out.insert(trade_date, index);
        index += 1;
    }

    Ok(out)
}

fn calc_lag(
    trade_day_index_map: &HashMap<String, usize>,
    effective_trade_date: &str,
    hit_date: Option<&String>,
) -> Option<i64> {
    let hit_date = hit_date?;
    let current_index = trade_day_index_map.get(effective_trade_date)?;
    let hit_index = trade_day_index_map.get(hit_date)?;
    Some((*current_index as i64) - (*hit_index as i64))
}

fn query_strategy_triggers(
    conn: &Connection,
    source_path: &str,
    ts_code: &str,
    effective_trade_date: &str,
) -> Result<DetailStrategyPayload, String> {
    let rule_meta_list = load_rule_meta_list(source_path)?;
    let current_score_map = load_current_rule_score_map(conn, ts_code, effective_trade_date)?;
    let latest_hit_date_map = load_latest_hit_date_map(conn, ts_code, effective_trade_date)?;
    let trade_day_index_map = build_trade_day_index_map(conn)?;

    let mut triggered = Vec::new();
    let mut untriggered = Vec::new();

    for meta in rule_meta_list {
        let current_score = current_score_map
            .get(&meta.rule_name)
            .copied()
            .unwrap_or(0.0);
        let hit_date = latest_hit_date_map.get(&meta.rule_name).cloned();
        let row = DetailStrategyTriggerRow {
            rule_name: meta.rule_name.clone(),
            rule_score: Some(current_score),
            hit_date: hit_date.clone(),
            lag: calc_lag(
                &trade_day_index_map,
                effective_trade_date,
                hit_date.as_ref(),
            ),
            explain: Some(meta.explain),
            tag: Some(meta.tag),
            when: Some(meta.when),
        };

        if current_score != 0.0 {
            triggered.push(row);
        } else {
            untriggered.push(row);
        }
    }

    Ok(DetailStrategyPayload {
        triggered: Some(triggered),
        untriggered: Some(untriggered),
    })
}

pub fn get_stock_detail_page(
    source_path: String,
    trade_date: Option<String>,
    ts_code: String,
    chart_window_days: Option<u32>,
    _prev_rank_days: Option<u32>,
) -> Result<StockDetailPageData, String> {
    let normalized_ts_code = normalize_ts_code(&ts_code);
    let result_conn = open_result_conn(&source_path)?;
    let effective_trade_date = resolve_trade_date(&result_conn, trade_date)?;
    let source_conn = open_source_conn(&source_path)?;

    let overview = query_detail_overview(
        &result_conn,
        &source_path,
        &effective_trade_date,
        &normalized_ts_code,
    )?;
    let prev_ranks = query_rank_history(&result_conn, &normalized_ts_code)?;
    let kline = query_kline(
        &source_conn,
        &normalized_ts_code,
        chart_window_days.unwrap_or(280) as usize,
        overview.name.clone(),
    )?;
    let strategy_triggers = query_strategy_triggers(
        &result_conn,
        &source_path,
        &normalized_ts_code,
        &effective_trade_date,
    )?;

    Ok(StockDetailPageData {
        resolved_trade_date: Some(effective_trade_date),
        resolved_ts_code: Some(normalized_ts_code),
        overview: Some(overview),
        prev_ranks: Some(prev_ranks),
        kline: Some(kline),
        strategy_triggers: Some(strategy_triggers),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_kline_panels_overlay_duokong_on_price_panel() {
        let panels = default_kline_panels();

        let price_panel = panels
            .iter()
            .find(|panel| panel.key == "price")
            .expect("missing price panel");
        let price_series = price_panel
            .series_keys
            .as_ref()
            .expect("price panel missing series keys");

        assert_eq!(
            price_series,
            &vec![
                "open".to_string(),
                "high".to_string(),
                "low".to_string(),
                "close".to_string(),
                "duokong_short".to_string(),
                "duokong_long".to_string(),
            ]
        );

        let indicator_panel = panels
            .iter()
            .find(|panel| panel.key == "indicator")
            .expect("missing indicator panel");
        let indicator_series = indicator_panel
            .series_keys
            .as_ref()
            .expect("indicator panel missing series keys");

        assert!(!indicator_series.iter().any(|key| key == "duokong_short"));
        assert!(!indicator_series.iter().any(|key| key == "duokong_long"));
    }
}
