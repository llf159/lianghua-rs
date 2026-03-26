use std::collections::HashMap;

use duckdb::{Connection, params};
use serde::Serialize;

use crate::{
    data::{ScopeWay, ScoreRule, result_db_path},
    ui_tools::{build_concepts_map, build_name_map},
};

const TOP_RANK_THRESHOLD: i64 = 100;

#[derive(Debug, Clone)]
struct RuleMeta {
    trigger_mode: String,
    is_each: bool,
    points: f64,
}

#[derive(Debug, Serialize)]
pub struct StrategyHeatmapCell {
    pub trade_date: String,
    pub day_level: Option<f64>,
    pub avg_level: Option<f64>,
    pub delta_level: Option<f64>,
    pub above_avg: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct StrategyOverviewPayload {
    pub items: Option<Vec<StrategyHeatmapCell>>,
    pub latest_trade_date: Option<String>,
    pub average_level: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyDailyRow {
    pub trade_date: String,
    pub rule_name: String,
    pub trigger_mode: Option<String>,
    pub sample_count: Option<i64>,
    pub trigger_count: Option<i64>,
    pub coverage: Option<f64>,
    pub contribution_score: Option<f64>,
    pub contribution_per_trigger: Option<f64>,
    pub median_trigger_count: Option<f64>,
    pub top100_trigger_count: Option<i64>,
    pub best_rank: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct StrategyChartPoint {
    pub trade_date: String,
    pub trigger_count: Option<i64>,
    pub top100_trigger_count: Option<i64>,
    pub coverage: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct StrategyChartPayload {
    pub items: Option<Vec<StrategyChartPoint>>,
}

#[derive(Debug, Serialize)]
pub struct TriggeredStockRow {
    pub rank: Option<i64>,
    pub ts_code: String,
    pub name: Option<String>,
    pub total_score: Option<f64>,
    pub rule_score: Option<f64>,
    pub concept: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct StrategyStatisticsPageData {
    pub overview: Option<StrategyOverviewPayload>,
    pub detail_rows: Option<Vec<StrategyDailyRow>>,
    pub strategy_options: Option<Vec<String>>,
    pub resolved_strategy_name: Option<String>,
    pub analysis_trade_date_options: Option<Vec<String>>,
    pub resolved_analysis_trade_date: Option<String>,
    pub chart: Option<StrategyChartPayload>,
    pub triggered_stocks: Option<Vec<TriggeredStockRow>>,
}

#[derive(Debug, Serialize)]
pub struct StrategyStatisticsDetailData {
    pub strategy_name: String,
    pub analysis_trade_date_options: Vec<String>,
    pub resolved_analysis_trade_date: Option<String>,
    pub selected_daily_row: Option<StrategyDailyRow>,
    pub chart: Option<StrategyChartPayload>,
    pub triggered_stocks: Vec<TriggeredStockRow>,
}

fn open_result_conn(source_path: &str) -> Result<Connection, String> {
    let result_db = result_db_path(source_path);
    let result_db_str = result_db
        .to_str()
        .ok_or_else(|| "结果库路径不是有效UTF-8".to_string())?;
    Connection::open(result_db_str).map_err(|e| format!("打开结果库失败: {e}"))
}

fn scope_way_label(scope_way: ScopeWay) -> String {
    match scope_way {
        ScopeWay::Any => "any".to_string(),
        ScopeWay::Last => "last".to_string(),
        ScopeWay::Each => "each".to_string(),
        ScopeWay::Recent => "recent".to_string(),
        ScopeWay::Consec(n) => format!("consec>={n}"),
    }
}

fn load_rule_meta(source_path: &str) -> Result<(Vec<String>, HashMap<String, RuleMeta>), String> {
    let rules = ScoreRule::load_rules(source_path)?;
    let mut order = Vec::with_capacity(rules.len());
    let mut meta_map = HashMap::with_capacity(rules.len());

    for rule in rules {
        order.push(rule.name.clone());
        meta_map.insert(
            rule.name,
            RuleMeta {
                trigger_mode: scope_way_label(rule.scope_way),
                is_each: matches!(rule.scope_way, ScopeWay::Each),
                points: rule.points,
            },
        );
    }

    Ok((order, meta_map))
}

fn query_overview(conn: &Connection) -> Result<StrategyOverviewPayload, String> {
    let sql = r#"
        WITH per_stock_day AS (
            SELECT
                trade_date,
                ts_code,
                SUM(CASE WHEN rule_score != 0 THEN 1 ELSE 0 END) AS hit_rule_count
            FROM score_details
            GROUP BY 1, 2
        ),
        daily_level AS (
            SELECT
                trade_date,
                AVG(hit_rule_count) AS day_level
            FROM per_stock_day
            GROUP BY 1
        ),
        overall_level AS (
            SELECT AVG(hit_rule_count) AS avg_level
            FROM per_stock_day
        )
        SELECT
            d.trade_date,
            d.day_level,
            o.avg_level,
            d.day_level - o.avg_level AS delta_level,
            CASE
                WHEN d.day_level IS NULL OR o.avg_level IS NULL THEN NULL
                ELSE d.day_level > o.avg_level
            END AS above_avg
        FROM daily_level AS d
        CROSS JOIN overall_level AS o
        ORDER BY d.trade_date ASC
    "#;

    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| format!("预编译总体统计 SQL 失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("执行总体统计 SQL 失败: {e}"))?;

    let mut items = Vec::new();
    let mut latest_trade_date = None;
    let mut average_level = None;

    while let Some(row) = rows.next().map_err(|e| format!("读取总体统计失败: {e}"))? {
        let trade_date: String = row.get(0).map_err(|e| format!("读取交易日失败: {e}"))?;
        let avg_level: Option<f64> = row.get(2).map_err(|e| format!("读取平均水平失败: {e}"))?;

        latest_trade_date = Some(trade_date.clone());
        average_level = avg_level;
        items.push(StrategyHeatmapCell {
            trade_date,
            day_level: row.get(1).map_err(|e| format!("读取当日水平失败: {e}"))?,
            avg_level,
            delta_level: row.get(3).map_err(|e| format!("读取差值失败: {e}"))?,
            above_avg: row.get(4).map_err(|e| format!("读取强弱标记失败: {e}"))?,
        });
    }

    Ok(StrategyOverviewPayload {
        items: Some(items),
        latest_trade_date,
        average_level,
    })
}

fn query_each_rule_medians(
    conn: &Connection,
    meta_map: &HashMap<String, RuleMeta>,
) -> Result<HashMap<(String, String), f64>, String> {
    let mut out = HashMap::new();

    for (rule_name, meta) in meta_map {
        if !meta.is_each || meta.points == 0.0 {
            continue;
        }

        let mut stmt = conn
            .prepare(
                r#"
                SELECT
                    trade_date,
                    QUANTILE_CONT(ABS(rule_score / ?), 0.5) AS median_trigger_count
                FROM score_details
                WHERE rule_name = ?
                  AND rule_score != 0
                GROUP BY 1
                ORDER BY 1 ASC
                "#,
            )
            .map_err(|e| format!("预编译 EACH 中位触发次数 SQL 失败: {e}"))?;
        let mut rows = stmt
            .query(params![meta.points, rule_name])
            .map_err(|e| format!("执行 EACH 中位触发次数 SQL 失败: {e}"))?;

        while let Some(row) = rows
            .next()
            .map_err(|e| format!("读取 EACH 中位触发次数失败: {e}"))?
        {
            let trade_date: String = row.get(0).map_err(|e| format!("读取交易日失败: {e}"))?;
            let median: Option<f64> = row
                .get(1)
                .map_err(|e| format!("读取中位触发次数失败: {e}"))?;
            if let Some(value) = median {
                out.insert((trade_date, rule_name.clone()), value);
            }
        }
    }

    Ok(out)
}

fn query_daily_rows(
    conn: &Connection,
    meta_map: &HashMap<String, RuleMeta>,
) -> Result<Vec<StrategyDailyRow>, String> {
    let each_medians = query_each_rule_medians(conn, meta_map)?;
    let sql = r#"
        WITH daily_rank_bounds AS (
            SELECT
                trade_date,
                MAX(rank) AS max_rank
            FROM score_summary
            GROUP BY 1
        )
        SELECT
            d.trade_date,
            d.rule_name,
            COUNT(*) AS sample_count,
            SUM(CASE WHEN d.rule_score != 0 THEN 1 ELSE 0 END) AS trigger_count,
            AVG(CASE WHEN d.rule_score != 0 THEN 1.0 ELSE 0.0 END) AS coverage,
            SUM(
                CASE
                    WHEN d.rule_score > 0
                      AND s.rank IS NOT NULL
                      AND b.max_rank IS NOT NULL
                      AND b.max_rank > 0
                    THEN d.rule_score * CAST((b.max_rank + 1 - s.rank) AS DOUBLE) / CAST(b.max_rank AS DOUBLE)
                    ELSE 0
                END
            ) AS contribution_score,
            SUM(CASE WHEN d.rule_score != 0 AND s.rank <= ? THEN 1 ELSE 0 END) AS top100_trigger_count,
            MIN(CASE WHEN d.rule_score != 0 THEN s.rank END) AS best_rank
        FROM score_details AS d
        LEFT JOIN score_summary AS s
          ON s.ts_code = d.ts_code
         AND s.trade_date = d.trade_date
        LEFT JOIN daily_rank_bounds AS b
          ON b.trade_date = d.trade_date
        GROUP BY 1, 2
        ORDER BY d.trade_date ASC, d.rule_name ASC
    "#;

    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| format!("预编译日度策略统计 SQL 失败: {e}"))?;
    let mut rows = stmt
        .query(params![TOP_RANK_THRESHOLD])
        .map_err(|e| format!("执行日度策略统计 SQL 失败: {e}"))?;

    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取日度策略统计失败: {e}"))?
    {
        let trade_date: String = row.get(0).map_err(|e| format!("读取交易日失败: {e}"))?;
        let rule_name: String = row.get(1).map_err(|e| format!("读取策略名失败: {e}"))?;
        let meta = meta_map.get(&rule_name);
        let trigger_count: Option<i64> =
            row.get(3).map_err(|e| format!("读取触发次数失败: {e}"))?;
        let contribution_score: Option<f64> =
            row.get(5).map_err(|e| format!("读取策略贡献度失败: {e}"))?;
        let contribution_per_trigger = match (contribution_score, trigger_count) {
            (Some(score), Some(count)) if count > 0 => Some(score / count as f64),
            _ => None,
        };

        out.push(StrategyDailyRow {
            median_trigger_count: each_medians
                .get(&(trade_date.clone(), rule_name.clone()))
                .copied(),
            trade_date,
            rule_name,
            trigger_mode: meta.map(|v| v.trigger_mode.clone()),
            sample_count: row.get(2).map_err(|e| format!("读取样本数失败: {e}"))?,
            trigger_count,
            coverage: row.get(4).map_err(|e| format!("读取覆盖率失败: {e}"))?,
            contribution_score,
            contribution_per_trigger,
            top100_trigger_count: row
                .get(6)
                .map_err(|e| format!("读取前100触发次数失败: {e}"))?,
            best_rank: row.get(7).map_err(|e| format!("读取最优排名失败: {e}"))?,
        });
    }

    Ok(out)
}

fn resolve_strategy_name(requested: Option<String>, strategy_options: &[String]) -> Option<String> {
    let requested = requested
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    if let Some(name) = requested {
        if strategy_options.iter().any(|item| item == &name) {
            return Some(name);
        }
    }
    None
}

fn resolve_analysis_trade_date(
    requested: Option<String>,
    trade_date_options: &[String],
) -> Option<String> {
    let requested = requested
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    if let Some(trade_date) = requested {
        if trade_date_options.iter().any(|item| item == &trade_date) {
            return Some(trade_date);
        }
    }
    trade_date_options.first().cloned()
}

fn build_chart(strategy_rows: &[StrategyDailyRow]) -> StrategyChartPayload {
    let items = strategy_rows
        .iter()
        .map(|row| StrategyChartPoint {
            trade_date: row.trade_date.clone(),
            trigger_count: row.trigger_count,
            top100_trigger_count: row.top100_trigger_count,
            coverage: row.coverage,
        })
        .collect();

    StrategyChartPayload { items: Some(items) }
}

fn query_triggered_stocks(
    conn: &Connection,
    source_path: &str,
    rule_name: &str,
    trade_date: &str,
) -> Result<Vec<TriggeredStockRow>, String> {
    let name_map = build_name_map(source_path).unwrap_or_default();
    let concept_map = build_concepts_map(source_path).unwrap_or_default();
    let mut stmt = conn
        .prepare(
            r#"
            SELECT
                s.rank,
                d.ts_code,
                s.total_score,
                d.rule_score
            FROM score_details AS d
            LEFT JOIN score_summary AS s
              ON s.ts_code = d.ts_code
             AND s.trade_date = d.trade_date
            WHERE d.trade_date = ?
              AND d.rule_name = ?
              AND d.rule_score != 0
            ORDER BY s.rank ASC NULLS LAST, d.ts_code ASC
            "#,
        )
        .map_err(|e| format!("预编译触发股票 SQL 失败: {e}"))?;
    let mut rows = stmt
        .query(params![trade_date, rule_name])
        .map_err(|e| format!("执行触发股票 SQL 失败: {e}"))?;

    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(|e| format!("读取触发股票失败: {e}"))? {
        let ts_code: String = row.get(1).map_err(|e| format!("读取股票代码失败: {e}"))?;
        out.push(TriggeredStockRow {
            rank: row.get(0).map_err(|e| format!("读取排名失败: {e}"))?,
            total_score: row.get(2).map_err(|e| format!("读取总分失败: {e}"))?,
            rule_score: row.get(3).map_err(|e| format!("读取策略得分失败: {e}"))?,
            name: name_map.get(&ts_code).cloned(),
            concept: concept_map.get(&ts_code).cloned(),
            ts_code,
        });
    }

    Ok(out)
}

pub fn get_strategy_triggered_stocks(
    source_path: String,
    strategy_name: String,
    analysis_trade_date: String,
) -> Result<Vec<TriggeredStockRow>, String> {
    let strategy_name = strategy_name.trim();
    let analysis_trade_date = analysis_trade_date.trim();
    if strategy_name.is_empty() || analysis_trade_date.is_empty() {
        return Ok(Vec::new());
    }

    let conn = open_result_conn(&source_path)?;
    query_triggered_stocks(&conn, &source_path, strategy_name, analysis_trade_date)
}

pub fn get_strategy_statistics_detail(
    source_path: String,
    strategy_name: String,
    analysis_trade_date: Option<String>,
) -> Result<StrategyStatisticsDetailData, String> {
    let strategy_name = strategy_name.trim().to_string();
    if strategy_name.is_empty() {
        return Err("策略名不能为空".to_string());
    }

    let conn = open_result_conn(&source_path)?;
    let (_, meta_map) = load_rule_meta(&source_path)?;
    let detail_rows_all = query_daily_rows(&conn, &meta_map)?;
    let strategy_rows = detail_rows_all
        .iter()
        .filter(|row| row.rule_name == strategy_name)
        .cloned()
        .collect::<Vec<_>>();

    let mut analysis_trade_date_options = strategy_rows
        .iter()
        .filter(|row| row.trigger_count.unwrap_or(0) > 0)
        .map(|row| row.trade_date.clone())
        .collect::<Vec<_>>();
    analysis_trade_date_options.sort();
    analysis_trade_date_options.dedup();
    analysis_trade_date_options.reverse();
    if analysis_trade_date_options.is_empty() {
        analysis_trade_date_options = strategy_rows
            .iter()
            .map(|row| row.trade_date.clone())
            .collect::<Vec<_>>();
        analysis_trade_date_options.sort();
        analysis_trade_date_options.dedup();
        analysis_trade_date_options.reverse();
    }
    let resolved_analysis_trade_date =
        resolve_analysis_trade_date(analysis_trade_date, &analysis_trade_date_options);
    let selected_daily_row = resolved_analysis_trade_date
        .as_ref()
        .and_then(|trade_date| {
            strategy_rows
                .iter()
                .find(|row| row.trade_date == *trade_date)
                .cloned()
        });
    let triggered_stocks = if let Some(trade_date) = resolved_analysis_trade_date.as_deref() {
        query_triggered_stocks(&conn, &source_path, &strategy_name, trade_date)?
    } else {
        Vec::new()
    };

    Ok(StrategyStatisticsDetailData {
        strategy_name,
        analysis_trade_date_options,
        resolved_analysis_trade_date,
        selected_daily_row,
        chart: Some(build_chart(&strategy_rows)),
        triggered_stocks,
    })
}

pub fn get_strategy_statistics_page(
    source_path: String,
    strategy_name: Option<String>,
    analysis_trade_date: Option<String>,
) -> Result<StrategyStatisticsPageData, String> {
    let conn = open_result_conn(&source_path)?;
    let overview = query_overview(&conn)?;
    let (strategy_options, meta_map) = load_rule_meta(&source_path)?;
    let detail_rows_all = query_daily_rows(&conn, &meta_map)?;

    let resolved_strategy_name = resolve_strategy_name(strategy_name, &strategy_options);

    let strategy_rows: Vec<StrategyDailyRow> =
        if let Some(selected_name) = resolved_strategy_name.as_ref() {
            detail_rows_all
                .iter()
                .filter(|row| row.rule_name == *selected_name)
                .cloned()
                .collect()
        } else {
            Vec::new()
        };

    let mut analysis_trade_date_options: Vec<String> = detail_rows_all
        .iter()
        .filter(|row| row.trigger_count.unwrap_or(0) > 0)
        .map(|row| row.trade_date.clone())
        .collect();
    analysis_trade_date_options.sort();
    analysis_trade_date_options.dedup();
    analysis_trade_date_options.reverse();

    if analysis_trade_date_options.is_empty() {
        analysis_trade_date_options = detail_rows_all
            .iter()
            .map(|row| row.trade_date.clone())
            .collect();
        analysis_trade_date_options.sort();
        analysis_trade_date_options.dedup();
        analysis_trade_date_options.reverse();
    }

    let resolved_analysis_trade_date =
        resolve_analysis_trade_date(analysis_trade_date, &analysis_trade_date_options);

    let triggered_stocks = if let (Some(rule_name), Some(trade_date)) = (
        resolved_strategy_name.as_deref(),
        resolved_analysis_trade_date.as_deref(),
    ) {
        query_triggered_stocks(&conn, &source_path, rule_name, trade_date)?
    } else {
        Vec::new()
    };

    let mut detail_rows = detail_rows_all;
    detail_rows.sort_by(|a, b| {
        b.trade_date
            .cmp(&a.trade_date)
            .then_with(|| {
                b.trigger_count
                    .unwrap_or(0)
                    .cmp(&a.trigger_count.unwrap_or(0))
            })
            .then_with(|| a.rule_name.cmp(&b.rule_name))
    });

    Ok(StrategyStatisticsPageData {
        overview: Some(overview),
        detail_rows: Some(detail_rows),
        strategy_options: Some(strategy_options),
        resolved_strategy_name,
        analysis_trade_date_options: Some(analysis_trade_date_options),
        resolved_analysis_trade_date,
        chart: Some(build_chart(&strategy_rows)),
        triggered_stocks: Some(triggered_stocks),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    };

    use duckdb::Connection;

    use crate::data::{result_db_path, scoring_data::init_result_db};

    fn unique_temp_dir() -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock drift")
            .as_nanos();
        std::env::temp_dir().join(format!("lianghua_rs_strategy_stats_{stamp}"))
    }

    fn write_fixture_files(source_dir: &Path) {
        fs::create_dir_all(source_dir).expect("create source dir");
        fs::write(
            source_dir.join("stock_list.csv"),
            concat!(
                "ts_code,symbol,name,c3,c4,c5,c6,c7,c8,total_mv,circ_mv\n",
                "000001.SZ,000001,平安银行,x,x,x,x,x,x,100000,50000\n",
                "000002.SZ,000002,万科A,x,x,x,x,x,x,80000,40000\n",
                "000003.SZ,000003,测试股,x,x,x,x,x,x,50000,20000\n"
            ),
        )
        .expect("write stock_list.csv");
        fs::write(
            source_dir.join("stock_concepts.csv"),
            concat!(
                "ts_code,name,concept\n",
                "000001.SZ,平安银行,金融\n",
                "000002.SZ,万科A,地产\n",
                "000003.SZ,测试股,芯片\n"
            ),
        )
        .expect("write stock_concepts.csv");
        fs::write(
            source_dir.join("score_rule.toml"),
            concat!(
                "version = 1\n\n",
                "[[rule]]\n",
                "name = \"R_LAST\"\n",
                "scope_windows = 3\n",
                "scope_way = \"LAST\"\n",
                "when = \"C > O\"\n",
                "points = 1.0\n",
                "explain = \"last\"\n\n",
                "[[rule]]\n",
                "name = \"R_EACH\"\n",
                "scope_windows = 3\n",
                "scope_way = \"EACH\"\n",
                "when = \"C > O\"\n",
                "points = 2.0\n",
                "explain = \"each\"\n"
            ),
        )
        .expect("write score_rule.toml");
    }

    fn write_fixture_db(source_dir: &Path) {
        let db_path = result_db_path(source_dir.to_str().expect("utf8 path"));
        init_result_db(&db_path).expect("init result db");
        let conn = Connection::open(&db_path).expect("open result db");

        let summary_rows = [
            ("000001.SZ", "20240101", 90.0, 1_i64),
            ("000002.SZ", "20240101", 80.0, 50_i64),
            ("000003.SZ", "20240101", 60.0, 150_i64),
            ("000001.SZ", "20240102", 85.0, 2_i64),
            ("000002.SZ", "20240102", 88.0, 40_i64),
            ("000003.SZ", "20240102", 62.0, 120_i64),
        ];
        for (ts_code, trade_date, total_score, rank) in summary_rows {
            conn.execute(
                "INSERT INTO score_summary (ts_code, trade_date, total_score, rank) VALUES (?, ?, ?, ?)",
                params![ts_code, trade_date, total_score, rank],
            )
            .expect("insert score_summary");
        }

        let detail_rows = [
            ("000001.SZ", "20240101", "R_LAST", 1.0),
            ("000002.SZ", "20240101", "R_LAST", 0.0),
            ("000003.SZ", "20240101", "R_LAST", 1.0),
            ("000001.SZ", "20240102", "R_LAST", 0.0),
            ("000002.SZ", "20240102", "R_LAST", 1.0),
            ("000003.SZ", "20240102", "R_LAST", 0.0),
            ("000001.SZ", "20240101", "R_EACH", 4.0),
            ("000002.SZ", "20240101", "R_EACH", 0.0),
            ("000003.SZ", "20240101", "R_EACH", 2.0),
            ("000001.SZ", "20240102", "R_EACH", 6.0),
            ("000002.SZ", "20240102", "R_EACH", 2.0),
            ("000003.SZ", "20240102", "R_EACH", 0.0),
        ];
        for (ts_code, trade_date, rule_name, rule_score) in detail_rows {
            conn.execute(
                "INSERT INTO score_details (ts_code, trade_date, rule_name, rule_score) VALUES (?, ?, ?, ?)",
                params![ts_code, trade_date, rule_name, rule_score],
            )
            .expect("insert score_details");
        }
    }

    #[test]
    fn strategy_statistics_page_builds_overview_chart_and_triggered_stocks() {
        let source_dir = unique_temp_dir();
        write_fixture_files(&source_dir);
        write_fixture_db(&source_dir);

        let page = get_strategy_statistics_page(
            source_dir.to_str().expect("utf8 path").to_string(),
            Some("R_EACH".to_string()),
            Some("20240102".to_string()),
        )
        .expect("build strategy statistics page");

        let overview = page.overview.expect("overview");
        let overview_items = overview.items.expect("overview items");
        assert_eq!(overview_items.len(), 2);
        assert_eq!(overview.latest_trade_date.as_deref(), Some("20240102"));
        assert_eq!(overview_items[0].trade_date, "20240101");
        assert_eq!(overview_items[0].above_avg, Some(true));
        assert_eq!(overview_items[1].above_avg, Some(false));

        let chart = page.chart.expect("chart");
        let chart_items = chart.items.expect("chart items");
        assert_eq!(chart_items.len(), 2);
        assert_eq!(chart_items[0].trade_date, "20240101");
        assert_eq!(chart_items[0].trigger_count, Some(2));
        assert_eq!(chart_items[0].top100_trigger_count, Some(1));
        assert_eq!(chart_items[1].top100_trigger_count, Some(2));

        let detail_rows = page.detail_rows.expect("detail rows");
        let latest_each_row = detail_rows
            .iter()
            .find(|row| row.trade_date == "20240102" && row.rule_name == "R_EACH")
            .expect("latest each row");
        assert_eq!(latest_each_row.trigger_mode.as_deref(), Some("each"));
        assert_eq!(latest_each_row.trigger_count, Some(2));
        assert_eq!(latest_each_row.top100_trigger_count, Some(2));
        assert_eq!(latest_each_row.best_rank, Some(2));
        assert_eq!(latest_each_row.median_trigger_count, Some(2.0));
        let contribution_score = latest_each_row
            .contribution_score
            .expect("latest each contribution score");
        assert!((contribution_score - 7.3).abs() < 1e-9);
        let contribution_per_trigger = latest_each_row
            .contribution_per_trigger
            .expect("latest each contribution per trigger");
        assert!((contribution_per_trigger - 3.65).abs() < 1e-9);

        assert_eq!(page.resolved_strategy_name.as_deref(), Some("R_EACH"));
        assert_eq!(
            page.resolved_analysis_trade_date.as_deref(),
            Some("20240102")
        );

        let triggered_stocks = page.triggered_stocks.expect("triggered stocks");
        assert_eq!(triggered_stocks.len(), 2);
        assert_eq!(triggered_stocks[0].ts_code, "000001.SZ");
        assert_eq!(triggered_stocks[0].rank, Some(2));
        assert_eq!(triggered_stocks[0].name.as_deref(), Some("平安银行"));
        assert_eq!(triggered_stocks[1].ts_code, "000002.SZ");

        let _ = fs::remove_dir_all(source_dir);
    }

    #[test]
    fn strategy_statistics_page_does_not_default_to_first_strategy() {
        let source_dir = unique_temp_dir();
        write_fixture_files(&source_dir);
        write_fixture_db(&source_dir);

        let page = get_strategy_statistics_page(
            source_dir.to_str().expect("utf8 path").to_string(),
            None,
            Some("20240102".to_string()),
        )
        .expect("build strategy statistics page");

        assert_eq!(page.resolved_strategy_name, None);
        assert_eq!(
            page.resolved_analysis_trade_date.as_deref(),
            Some("20240102")
        );
        assert_eq!(
            page.analysis_trade_date_options,
            Some(vec!["20240102".to_string(), "20240101".to_string()])
        );
        assert_eq!(
            page.chart.expect("chart").items.expect("chart items").len(),
            0
        );
        assert_eq!(page.triggered_stocks.expect("triggered stocks").len(), 0);

        let _ = fs::remove_dir_all(source_dir);
    }

    #[test]
    fn get_strategy_triggered_stocks_queries_single_strategy_date_only() {
        let source_dir = unique_temp_dir();
        write_fixture_files(&source_dir);
        write_fixture_db(&source_dir);

        let triggered_stocks = get_strategy_triggered_stocks(
            source_dir.to_str().expect("utf8 path").to_string(),
            "R_LAST".to_string(),
            "20240101".to_string(),
        )
        .expect("query triggered stocks");

        assert_eq!(triggered_stocks.len(), 2);
        assert_eq!(triggered_stocks[0].ts_code, "000001.SZ");
        assert_eq!(triggered_stocks[0].rank, Some(1));
        assert_eq!(triggered_stocks[1].ts_code, "000003.SZ");
        assert_eq!(triggered_stocks[1].rank, Some(150));

        let _ = fs::remove_dir_all(source_dir);
    }
}
