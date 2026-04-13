use std::collections::{HashMap, HashSet};

use duckdb::{params, Connection};
use serde::Serialize;

use crate::{
    data::{
        concept_performance_db_path, load_stock_list, load_ths_concepts_list, result_db_path,
        source_db_path, ScopeWay, ScoreRule, ScoreScene,
    },
    simulate::scene::{
        SceneLayerConfig, SceneLayerFromDbInput, calc_scene_layer_metrics_from_db,
    },
    ui_tools_feat::{build_concepts_map, build_name_map},
    utils::utils::board_category,
};

const TOP_RANK_THRESHOLD: i64 = 100;

#[derive(Debug, Clone)]
struct RuleMeta {
    trigger_mode: String,
    is_each: bool,
    points: f64,
}

#[derive(Debug, Clone, Default)]
struct RuleDayAgg {
    trigger_count: i64,
    contribution_score: f64,
    top100_trigger_count: i64,
    best_rank: Option<i64>,
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

#[derive(Debug, Serialize)]
pub struct SceneLayerStateAvgResidualReturn {
    pub scene_state: String,
    pub avg_residual_return: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct SceneLayerPointPayload {
    pub trade_date: String,
    pub state_avg_residual_returns: Vec<SceneLayerStateAvgResidualReturn>,
    pub top_bottom_spread: Option<f64>,
    pub ic: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct SceneLayerSceneSummary {
    pub scene_name: String,
    pub point_count: usize,
    pub spread_mean: Option<f64>,
    pub ic_mean: Option<f64>,
    pub ic_std: Option<f64>,
    pub icir: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct SceneLayerBacktestData {
    pub scene_name: String,
    pub stock_adj_type: String,
    pub index_ts_code: String,
    pub index_beta: f64,
    pub concept_beta: f64,
    pub board_beta: f64,
    pub start_date: String,
    pub end_date: String,
    pub min_samples_per_scene_day: usize,
    pub backtest_period: usize,
    pub points: Vec<SceneLayerPointPayload>,
    pub spread_mean: Option<f64>,
    pub ic_mean: Option<f64>,
    pub ic_std: Option<f64>,
    pub icir: Option<f64>,
    pub is_all_scenes: bool,
    pub all_scene_summaries: Vec<SceneLayerSceneSummary>,
}

#[derive(Debug, Serialize)]
pub struct SceneLayerBacktestDefaultsData {
    pub scene_options: Vec<String>,
    pub resolved_scene_name: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct MarketRankItem {
    pub name: String,
    pub value: f64,
}

#[derive(Debug, Serialize)]
pub struct MarketAnalysisSnapshot {
    pub trade_date: Option<String>,
    pub concept_top: Vec<MarketRankItem>,
    pub industry_top: Vec<MarketRankItem>,
    pub gain_top: Vec<MarketRankItem>,
}

#[derive(Debug, Serialize)]
pub struct MarketAnalysisData {
    pub lookback_period: usize,
    pub latest_trade_date: Option<String>,
    pub resolved_reference_trade_date: Option<String>,
    pub board_options: Vec<String>,
    pub resolved_board: Option<String>,
    pub interval: MarketAnalysisSnapshot,
    pub daily: MarketAnalysisSnapshot,
}

#[derive(Debug, Serialize)]
pub struct MarketContributorItem {
    pub ts_code: String,
    pub name: Option<String>,
    pub industry: Option<String>,
    pub contribution_pct: f64,
}

#[derive(Debug, Serialize)]
pub struct MarketContributionData {
    pub scope: String,
    pub kind: String,
    pub name: String,
    pub trade_date: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub lookback_period: usize,
    pub contributors: Vec<MarketContributorItem>,
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

fn load_scene_options(source_path: &str) -> Result<Vec<String>, String> {
    let scenes = ScoreScene::load_scenes(source_path)?;
    Ok(scenes.into_iter().map(|scene| scene.name).collect())
}

fn query_overview(conn: &Connection) -> Result<StrategyOverviewPayload, String> {
    let sql = r#"
        WITH per_stock_day AS (
            SELECT
                trade_date,
                ts_code,
                COUNT(*) AS hit_rule_count
            FROM rule_details
            WHERE rule_score IS NOT NULL
              AND ABS(rule_score) > 1e-12
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
                FROM rule_details
                WHERE rule_name = ?
                  AND rule_score IS NOT NULL
                  AND ABS(rule_score) > 1e-12
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
    rule_order: &[String],
    meta_map: &HashMap<String, RuleMeta>,
) -> Result<Vec<StrategyDailyRow>, String> {
    let each_medians = query_each_rule_medians(conn, meta_map)?;
    let mut sample_stmt = conn
        .prepare(
            r#"
        SELECT
            trade_date,
            COUNT(*) AS sample_count
        FROM score_summary
        GROUP BY 1
        ORDER BY 1 ASC
        "#,
        )
        .map_err(|e| format!("预编译日度样本数 SQL 失败: {e}"))?;
    let mut sample_rows = sample_stmt
        .query([])
        .map_err(|e| format!("执行日度样本数 SQL 失败: {e}"))?;

    let mut daily_samples = Vec::new();
    while let Some(row) = sample_rows
        .next()
        .map_err(|e| format!("读取日度样本数失败: {e}"))?
    {
        let trade_date: String = row.get(0).map_err(|e| format!("读取交易日失败: {e}"))?;
        let sample_count: i64 = row.get(1).map_err(|e| format!("读取样本数失败: {e}"))?;
        daily_samples.push((trade_date, sample_count));
    }

    let sql = r#"
        WITH daily_rank_bounds AS (
            SELECT
                trade_date,
                MAX(rank) AS max_rank
            FROM score_summary
            GROUP BY 1
        ),
        triggered_rule_rows AS (
            SELECT *
            FROM rule_details
            WHERE rule_score IS NOT NULL
              AND ABS(rule_score) > 1e-12
        )
        SELECT
            d.trade_date,
            d.rule_name,
            COUNT(*) AS trigger_count,
            SUM(
                CASE
                    WHEN s.rank IS NOT NULL
                      AND b.max_rank IS NOT NULL
                      AND b.max_rank > 0
                    THEN d.rule_score * CAST((b.max_rank + 1 - s.rank) AS DOUBLE) / CAST(b.max_rank AS DOUBLE)
                    ELSE 0
                END
            ) AS contribution_score,
            SUM(CASE WHEN s.rank <= ? THEN 1 ELSE 0 END) AS top100_trigger_count,
            MIN(s.rank) AS best_rank
        FROM triggered_rule_rows AS d
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
    let mut daily_agg_map: HashMap<(String, String), RuleDayAgg> = HashMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取日度策略统计失败: {e}"))?
    {
        let trade_date: String = row.get(0).map_err(|e| format!("读取交易日失败: {e}"))?;
        let rule_name: String = row.get(1).map_err(|e| format!("读取策略名失败: {e}"))?;
        daily_agg_map.insert(
            (trade_date, rule_name),
            RuleDayAgg {
                trigger_count: row.get(2).map_err(|e| format!("读取触发次数失败: {e}"))?,
                contribution_score: row
                    .get::<usize, Option<f64>>(3)
                    .map_err(|e| format!("读取策略贡献度失败: {e}"))?
                    .unwrap_or(0.0),
                top100_trigger_count: row
                    .get::<usize, Option<i64>>(4)
                    .map_err(|e| format!("读取前100触发次数失败: {e}"))?
                    .unwrap_or(0),
                best_rank: row.get(5).map_err(|e| format!("读取最优排名失败: {e}"))?,
            },
        );
    }

    for (trade_date, sample_count) in daily_samples {
        for rule_name in rule_order {
            let agg = daily_agg_map
                .get(&(trade_date.clone(), rule_name.clone()))
                .cloned()
                .unwrap_or_default();
            let meta = meta_map.get(rule_name);
            let contribution_score = if agg.trigger_count > 0 {
                Some(agg.contribution_score)
            } else {
                None
            };
            let contribution_per_trigger =
                contribution_score.map(|score| score / agg.trigger_count as f64);
            let coverage = if sample_count > 0 {
                Some(agg.trigger_count as f64 / sample_count as f64)
            } else {
                None
            };

            out.push(StrategyDailyRow {
                median_trigger_count: each_medians
                    .get(&(trade_date.clone(), rule_name.clone()))
                    .copied(),
                trade_date: trade_date.clone(),
                rule_name: rule_name.clone(),
                trigger_mode: meta.map(|v| v.trigger_mode.clone()),
                sample_count: Some(sample_count),
                trigger_count: Some(agg.trigger_count),
                coverage,
                contribution_score,
                contribution_per_trigger,
                top100_trigger_count: Some(agg.top100_trigger_count),
                best_rank: agg.best_rank,
            });
        }
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
            FROM rule_details AS d
            LEFT JOIN score_summary AS s
              ON s.ts_code = d.ts_code
             AND s.trade_date = d.trade_date
            WHERE d.trade_date = ?
              AND d.rule_name = ?
              AND d.rule_score IS NOT NULL
              AND ABS(d.rule_score) > 1e-12
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
    let (rule_order, meta_map) = load_rule_meta(&source_path)?;
    let detail_rows_all = query_daily_rows(&conn, &rule_order, &meta_map)?;
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
    let selected_daily_row = resolved_analysis_trade_date.as_ref().and_then(|trade_date| {
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
    let detail_rows_all = query_daily_rows(&conn, &strategy_options, &meta_map)?;

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
            .then_with(|| b.trigger_count.unwrap_or(0).cmp(&a.trigger_count.unwrap_or(0)))
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

fn load_stock_name_map(source_path: &str) -> Result<HashMap<String, String>, String> {
    let rows = load_stock_list(source_path)?;
    let mut out = HashMap::with_capacity(rows.len());

    for cols in rows {
        let Some(ts_code) = cols.first().map(|value| value.trim()) else {
            continue;
        };
        let Some(name_raw) = cols.get(2).map(|value| value.trim()) else {
            continue;
        };
        if ts_code.is_empty() || name_raw.is_empty() {
            continue;
        }

        out.insert(ts_code.to_string(), name_raw.to_string());
    }

    Ok(out)
}

fn split_board_tags(board_raw: &str) -> Vec<String> {
    board_raw
        .split(|ch| matches!(ch, ',' | ';' | '，' | '；' | '|' | '、' | '/' | '\n' | '\r'))
        .map(|part| part.trim())
        .filter(|part| !part.is_empty())
        .map(|part| part.to_string())
        .collect()
}

fn build_board_maps(
    source_path: &str,
) -> Result<(Vec<String>, HashMap<String, Vec<String>>), String> {
    let stock_rows = load_stock_list(source_path)?;
    let mut ts_board_map: HashMap<String, Vec<String>> = HashMap::with_capacity(stock_rows.len());
    let mut board_set: HashSet<String> = HashSet::new();

    for cols in stock_rows {
        let Some(ts_code_raw) = cols.first().map(|value| value.trim()) else {
            continue;
        };
        let ts_code = ts_code_raw.to_ascii_uppercase();
        let stock_name = cols.get(1).map(|value| value.trim()).filter(|value| !value.is_empty());

        let mut board_list = Vec::new();
        let category_board = board_category(&ts_code, stock_name).to_string();
        board_set.insert(category_board.clone());
        board_list.push(category_board);

        if let Some(board_raw) = cols.get(4).map(|value| value.trim()) {
            if !board_raw.is_empty() {
                let detail_boards = split_board_tags(board_raw);
                for board in detail_boards {
                    if board_list.iter().any(|item| item == &board) {
                        continue;
                    }
                    board_set.insert(board.clone());
                    board_list.push(board);
                }
            }
        }

        ts_board_map.insert(ts_code, board_list);
    }

    let mut board_options = board_set.into_iter().collect::<Vec<_>>();
    board_options.sort();

    Ok((board_options, ts_board_map))
}

fn resolve_board_filter(requested: Option<String>, board_options: &[String]) -> Option<String> {
    let requested = requested
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if let Some(board) = requested {
        if board_options.iter().any(|item| item == &board) {
            return Some(board);
        }
    }
    None
}

fn match_board_filter(board_list: &[String], selected_board: Option<&str>) -> bool {
    let Some(selected_board) = selected_board else {
        return true;
    };
    board_list.iter().any(|board| board == selected_board)
}

pub fn get_market_analysis(
    source_path: String,
    lookback_period: Option<usize>,
    reference_trade_date: Option<String>,
    board: Option<String>,
) -> Result<MarketAnalysisData, String> {
    let lookback_period = lookback_period.unwrap_or(20).max(1);

    let source_db = source_db_path(&source_path);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "原始库路径不是有效UTF-8".to_string())?;
    let source_conn = Connection::open(source_db_str).map_err(|e| format!("打开原始库失败: {e}"))?;

    let latest_trade_date: Option<String> = source_conn
        .query_row(
            "SELECT MAX(trade_date) FROM stock_data WHERE adj_type = 'qfq'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| format!("查询最新交易日失败: {e}"))?;

    let resolved_reference_trade_date = reference_trade_date
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| latest_trade_date.clone());

    let (board_options, ts_board_map) = build_board_maps(&source_path)?;
    let resolved_board = resolve_board_filter(board, &board_options);

    let Some(ref_date) = resolved_reference_trade_date.clone() else {
        return Ok(MarketAnalysisData {
            lookback_period,
            latest_trade_date,
            resolved_reference_trade_date: None,
            board_options,
            resolved_board,
            interval: MarketAnalysisSnapshot {
                trade_date: None,
                concept_top: Vec::new(),
                industry_top: Vec::new(),
                gain_top: Vec::new(),
            },
            daily: MarketAnalysisSnapshot {
                trade_date: None,
                concept_top: Vec::new(),
                industry_top: Vec::new(),
                gain_top: Vec::new(),
            },
        });
    };

    let mut date_stmt = source_conn
        .prepare(
            r#"
            SELECT trade_date
            FROM (
                SELECT DISTINCT trade_date
                FROM stock_data
                WHERE adj_type = 'qfq'
                  AND trade_date <= ?
                ORDER BY trade_date DESC
                LIMIT ?
            ) AS t
            ORDER BY trade_date ASC
            "#,
        )
        .map_err(|e| format!("预编译市场分析区间日期 SQL 失败: {e}"))?;
    let mut date_rows = date_stmt
        .query(params![&ref_date, lookback_period as i64])
        .map_err(|e| format!("执行市场分析区间日期 SQL 失败: {e}"))?;
    let mut dates = Vec::new();
    while let Some(row) = date_rows
        .next()
        .map_err(|e| format!("读取市场分析区间日期失败: {e}"))?
    {
        let trade_date: String = row.get(0).map_err(|e| format!("读取交易日失败: {e}"))?;
        dates.push(trade_date);
    }

    if dates.is_empty() {
        return Ok(MarketAnalysisData {
            lookback_period,
            latest_trade_date,
            resolved_reference_trade_date: Some(ref_date.clone()),
            board_options,
            resolved_board,
            interval: MarketAnalysisSnapshot {
                trade_date: None,
                concept_top: Vec::new(),
                industry_top: Vec::new(),
                gain_top: Vec::new(),
            },
            daily: MarketAnalysisSnapshot {
                trade_date: Some(ref_date),
                concept_top: Vec::new(),
                industry_top: Vec::new(),
                gain_top: Vec::new(),
            },
        });
    }

    let interval_start = dates.first().cloned().unwrap_or_else(|| ref_date.clone());
    let interval_end = dates.last().cloned().unwrap_or_else(|| ref_date.clone());

    let concept_db = concept_performance_db_path(&source_path);
    let concept_db_str = concept_db
        .to_str()
        .ok_or_else(|| "概念表现库路径不是有效UTF-8".to_string())?;
    let concept_conn = Connection::open(concept_db_str).map_err(|e| format!("打开概念表现库失败: {e}"))?;
    let has_performance_type = concept_conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('concept_performance') WHERE name = 'performance_type'",
            [],
            |row| row.get::<usize, i64>(0),
        )
        .map_err(|e| format!("检查 concept_performance 结构失败: {e}"))?
        > 0;

    let concept_interval_sql = if has_performance_type {
        r#"
        SELECT concept, AVG(TRY_CAST(performance_pct AS DOUBLE)) AS avg_pct
        FROM concept_performance
        WHERE performance_type = 'concept'
          AND trade_date >= ?
          AND trade_date <= ?
        GROUP BY 1
        ORDER BY avg_pct DESC NULLS LAST, concept ASC
        LIMIT ?
        "#
    } else {
        r#"
        SELECT concept, AVG(TRY_CAST(performance_pct AS DOUBLE)) AS avg_pct
        FROM concept_performance
        WHERE trade_date >= ?
          AND trade_date <= ?
        GROUP BY 1
        ORDER BY avg_pct DESC NULLS LAST, concept ASC
        LIMIT ?
        "#
    };

    let mut concept_interval_stmt = concept_conn
        .prepare(concept_interval_sql)
        .map_err(|e| format!("预编译概念区间榜 SQL 失败: {e}"))?;
    let mut concept_interval_rows = concept_interval_stmt
        .query(params![&interval_start, &interval_end, 20_i64])
        .map_err(|e| format!("执行概念区间榜 SQL 失败: {e}"))?;
    let mut interval_concept_top = Vec::new();
    while let Some(row) = concept_interval_rows
        .next()
        .map_err(|e| format!("读取概念区间榜失败: {e}"))?
    {
        let name: String = row.get(0).map_err(|e| format!("读取概念名失败: {e}"))?;
        let value: Option<f64> = row.get(1).map_err(|e| format!("读取概念值失败: {e}"))?;
        if let Some(value) = value.filter(|v| v.is_finite()) {
            interval_concept_top.push(MarketRankItem { name, value });
        }
    }


    let mut interval_board_stmt = source_conn
        .prepare(
            r#"
            SELECT ts_code, AVG(TRY_CAST(pct_chg AS DOUBLE)) AS avg_pct
            FROM stock_data
            WHERE adj_type = 'qfq'
              AND trade_date >= ?
              AND trade_date <= ?
            GROUP BY 1
            "#,
        )
        .map_err(|e| format!("预编译板块区间榜 SQL 失败: {e}"))?;
    let mut interval_board_rows = interval_board_stmt
        .query(params![&interval_start, &interval_end])
        .map_err(|e| format!("执行板块区间榜 SQL 失败: {e}"))?;
    let mut interval_board_acc: HashMap<String, (f64, usize)> = HashMap::new();
    while let Some(row) = interval_board_rows
        .next()
        .map_err(|e| format!("读取板块区间榜失败: {e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取代码失败: {e}"))?;
        let avg_pct: Option<f64> = row.get(1).map_err(|e| format!("读取板块值失败: {e}"))?;
        let Some(avg_pct) = avg_pct.filter(|v| v.is_finite()) else {
            continue;
        };
        let ts_code = ts_code.to_ascii_uppercase();
        let Some(board_list) = ts_board_map.get(&ts_code) else {
            continue;
        };
        for board in board_list {
            let entry = interval_board_acc.entry(board.clone()).or_insert((0.0, 0));
            entry.0 += avg_pct;
            entry.1 += 1;
        }
    }
    let mut interval_board_top = interval_board_acc
        .into_iter()
        .filter_map(|(name, (sum, cnt))| {
            if cnt == 0 {
                return None;
            }
            let value = sum / cnt as f64;
            if !value.is_finite() {
                return None;
            }
            Some(MarketRankItem { name, value })
        })
        .collect::<Vec<_>>();
    interval_board_top.sort_by(|a, b| {
        b.value
            .partial_cmp(&a.value)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.name.cmp(&b.name))
    });
    interval_board_top.truncate(20);

    let stock_name_map = load_stock_name_map(&source_path)?;

    let mut interval_gain_stmt = source_conn
        .prepare(
            r#"
            SELECT ts_code, AVG(TRY_CAST(pct_chg AS DOUBLE)) AS avg_pct
            FROM stock_data
            WHERE adj_type = 'qfq'
              AND trade_date >= ?
              AND trade_date <= ?
            GROUP BY 1
            ORDER BY avg_pct DESC NULLS LAST, ts_code ASC
            "#,
        )
        .map_err(|e| format!("预编译涨幅区间榜 SQL 失败: {e}"))?;
    let mut interval_gain_rows = interval_gain_stmt
        .query(params![&interval_start, &interval_end])
        .map_err(|e| format!("执行涨幅区间榜 SQL 失败: {e}"))?;
    let mut interval_gain_top = Vec::new();
    while let Some(row) = interval_gain_rows
        .next()
        .map_err(|e| format!("读取涨幅区间榜失败: {e}"))?
    {
        if interval_gain_top.len() >= 20 {
            break;
        }
        let ts_code: String = row.get(0).map_err(|e| format!("读取代码失败: {e}"))?;
        let value: Option<f64> = row.get(1).map_err(|e| format!("读取涨幅值失败: {e}"))?;
        let Some(value) = value.filter(|v| v.is_finite()) else {
            continue;
        };
        let ts_code = ts_code.to_ascii_uppercase();
        let Some(board_list) = ts_board_map.get(&ts_code) else {
            continue;
        };
        if !match_board_filter(board_list, resolved_board.as_deref()) {
            continue;
        }

        let name = stock_name_map
            .get(&ts_code)
            .cloned()
            .unwrap_or(ts_code.clone());
        interval_gain_top.push(MarketRankItem {
            name: format!("{} ({})", name, ts_code),
            value,
        });
    }

    let daily_concept_sql = if has_performance_type {
        r#"
        SELECT concept, TRY_CAST(performance_pct AS DOUBLE)
        FROM concept_performance
        WHERE performance_type = 'concept'
          AND trade_date = ?
        ORDER BY TRY_CAST(performance_pct AS DOUBLE) DESC NULLS LAST, concept ASC
        LIMIT ?
        "#
    } else {
        r#"
        SELECT concept, TRY_CAST(performance_pct AS DOUBLE)
        FROM concept_performance
        WHERE trade_date = ?
        ORDER BY TRY_CAST(performance_pct AS DOUBLE) DESC NULLS LAST, concept ASC
        LIMIT ?
        "#
    };

    let mut daily_concept_stmt = concept_conn
        .prepare(daily_concept_sql)
        .map_err(|e| format!("预编译概念当日榜 SQL 失败: {e}"))?;
    let mut daily_concept_rows = daily_concept_stmt
        .query(params![&ref_date, 20_i64])
        .map_err(|e| format!("执行概念当日榜 SQL 失败: {e}"))?;
    let mut daily_concept_top = Vec::new();
    while let Some(row) = daily_concept_rows
        .next()
        .map_err(|e| format!("读取概念当日榜失败: {e}"))?
    {
        let name: String = row.get(0).map_err(|e| format!("读取概念名失败: {e}"))?;
        let value: Option<f64> = row.get(1).map_err(|e| format!("读取概念值失败: {e}"))?;
        if let Some(value) = value.filter(|v| v.is_finite()) {
            daily_concept_top.push(MarketRankItem { name, value });
        }
    }

    let mut daily_board_stmt = source_conn
        .prepare(
            r#"
            SELECT ts_code, TRY_CAST(pct_chg AS DOUBLE) AS pct
            FROM stock_data
            WHERE adj_type = 'qfq'
              AND trade_date = ?
            "#,
        )
        .map_err(|e| format!("预编译板块当日榜 SQL 失败: {e}"))?;
    let mut daily_board_rows = daily_board_stmt
        .query(params![&ref_date])
        .map_err(|e| format!("执行板块当日榜 SQL 失败: {e}"))?;
    let mut daily_board_acc: HashMap<String, (f64, usize)> = HashMap::new();
    while let Some(row) = daily_board_rows
        .next()
        .map_err(|e| format!("读取板块当日榜失败: {e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取代码失败: {e}"))?;
        let pct: Option<f64> = row.get(1).map_err(|e| format!("读取板块值失败: {e}"))?;
        let Some(pct) = pct.filter(|v| v.is_finite()) else {
            continue;
        };
        let ts_code = ts_code.to_ascii_uppercase();
        let Some(board_list) = ts_board_map.get(&ts_code) else {
            continue;
        };
        for board in board_list {
            let entry = daily_board_acc.entry(board.clone()).or_insert((0.0, 0));
            entry.0 += pct;
            entry.1 += 1;
        }
    }
    let mut daily_board_top = daily_board_acc
        .into_iter()
        .filter_map(|(name, (sum, cnt))| {
            if cnt == 0 {
                return None;
            }
            let value = sum / cnt as f64;
            if !value.is_finite() {
                return None;
            }
            Some(MarketRankItem { name, value })
        })
        .collect::<Vec<_>>();
    daily_board_top.sort_by(|a, b| {
        b.value
            .partial_cmp(&a.value)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.name.cmp(&b.name))
    });
    daily_board_top.truncate(20);

    let mut daily_gain_stmt = source_conn
        .prepare(
            r#"
            SELECT ts_code, TRY_CAST(pct_chg AS DOUBLE)
            FROM stock_data
            WHERE adj_type = 'qfq'
              AND trade_date = ?
            ORDER BY TRY_CAST(pct_chg AS DOUBLE) DESC NULLS LAST, ts_code ASC
            "#,
        )
        .map_err(|e| format!("预编译涨幅当日榜 SQL 失败: {e}"))?;
    let mut daily_gain_rows = daily_gain_stmt
        .query(params![&ref_date])
        .map_err(|e| format!("执行涨幅当日榜 SQL 失败: {e}"))?;
    let mut daily_gain_top = Vec::new();
    while let Some(row) = daily_gain_rows
        .next()
        .map_err(|e| format!("读取涨幅当日榜失败: {e}"))?
    {
        if daily_gain_top.len() >= 20 {
            break;
        }
        let ts_code: String = row.get(0).map_err(|e| format!("读取代码失败: {e}"))?;
        let value: Option<f64> = row.get(1).map_err(|e| format!("读取涨幅值失败: {e}"))?;
        let Some(value) = value.filter(|v| v.is_finite()) else {
            continue;
        };
        let ts_code = ts_code.to_ascii_uppercase();
        let Some(board_list) = ts_board_map.get(&ts_code) else {
            continue;
        };
        if !match_board_filter(board_list, resolved_board.as_deref()) {
            continue;
        }

        let name = stock_name_map
            .get(&ts_code)
            .cloned()
            .unwrap_or(ts_code.clone());
        daily_gain_top.push(MarketRankItem {
            name: format!("{} ({})", name, ts_code),
            value,
        });
    }

    Ok(MarketAnalysisData {
        lookback_period,
        latest_trade_date,
        resolved_reference_trade_date: Some(ref_date.clone()),
        board_options,
        resolved_board,
        interval: MarketAnalysisSnapshot {
            trade_date: Some(format!("{}~{}", interval_start, interval_end)),
            concept_top: interval_concept_top,
            industry_top: interval_board_top,
            gain_top: interval_gain_top,
        },
        daily: MarketAnalysisSnapshot {
            trade_date: Some(ref_date),
            concept_top: daily_concept_top,
            industry_top: daily_board_top,
            gain_top: daily_gain_top,
        },
    })
}

pub fn get_market_contribution(
    source_path: String,
    scope: String,
    kind: String,
    name: String,
    lookback_period: Option<usize>,
    reference_trade_date: Option<String>,
) -> Result<MarketContributionData, String> {
    let scope = scope.trim().to_ascii_lowercase();
    let kind = kind.trim().to_ascii_lowercase();
    let target_name = name.trim().to_string();
    if !matches!(scope.as_str(), "interval" | "daily") {
        return Err("scope 仅支持 interval/daily".to_string());
    }
    let kind = match kind.as_str() {
        "concept" => "concept".to_string(),
        "industry" | "board" | "market" => "industry".to_string(),
        _ => return Err("kind 仅支持 concept/industry".to_string()),
    };
    if target_name.is_empty() {
        return Err("名称不能为空".to_string());
    }

    let lookback_period = lookback_period.unwrap_or(20).max(1);
    let source_db = source_db_path(&source_path);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "原始库路径不是有效UTF-8".to_string())?;
    let source_conn = Connection::open(source_db_str).map_err(|e| format!("打开原始库失败: {e}"))?;

    let latest_trade_date: Option<String> = source_conn
        .query_row(
            "SELECT MAX(trade_date) FROM stock_data WHERE adj_type = 'qfq'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| format!("查询最新交易日失败: {e}"))?;
    let ref_date = reference_trade_date
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or(latest_trade_date)
        .ok_or_else(|| "缺少有效参考日".to_string())?;

    let mut date_stmt = source_conn
        .prepare(
            r#"
            SELECT trade_date
            FROM (
                SELECT DISTINCT trade_date
                FROM stock_data
                WHERE adj_type = 'qfq'
                  AND trade_date <= ?
                ORDER BY trade_date DESC
                LIMIT ?
            ) AS t
            ORDER BY trade_date ASC
            "#,
        )
        .map_err(|e| format!("预编译市场贡献区间日期 SQL 失败: {e}"))?;
    let mut date_rows = date_stmt
        .query(params![&ref_date, lookback_period as i64])
        .map_err(|e| format!("执行市场贡献区间日期 SQL 失败: {e}"))?;
    let mut dates = Vec::new();
    while let Some(row) = date_rows
        .next()
        .map_err(|e| format!("读取市场贡献区间日期失败: {e}"))?
    {
        let trade_date: String = row.get(0).map_err(|e| format!("读取交易日失败: {e}"))?;
        dates.push(trade_date);
    }

    if dates.is_empty() {
        return Ok(MarketContributionData {
            scope,
            kind,
            name: target_name,
            trade_date: Some(ref_date),
            start_date: None,
            end_date: None,
            lookback_period,
            contributors: Vec::new(),
        });
    }

    let interval_start = dates.first().cloned();
    let interval_end = dates.last().cloned();

    let stock_rows = load_stock_list(&source_path)?;
    let mut ts_name_map: HashMap<String, String> = HashMap::with_capacity(stock_rows.len());
    let mut ts_board_map: HashMap<String, String> = HashMap::with_capacity(stock_rows.len());
    let mut target_codes: HashSet<String> = HashSet::new();

    for cols in stock_rows {
        let Some(ts_code) = cols.first().map(|value| value.trim()) else {
            continue;
        };
        if ts_code.is_empty() {
            continue;
        }

        let stock_name = cols
            .get(2)
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        if let Some(stock_name) = stock_name {
            ts_name_map.insert(ts_code.to_string(), stock_name);
        }

        let board_name = cols
            .get(4)
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        if let Some(board_name) = board_name.clone() {
            ts_board_map.insert(ts_code.to_string(), board_name.clone());
        }

        if kind == "industry" {
            let is_match = board_name
                .as_deref()
                .map(|value| {
                    value
                        .split(|ch| {
                            matches!(ch, ',' | ';' | '，' | '；' | '|' | '、' | '/' | '\n' | '\r')
                        })
                        .map(|part| part.trim())
                        .any(|part| !part.is_empty() && part == target_name)
                })
                .unwrap_or(false);
            if is_match {
                target_codes.insert(ts_code.to_string());
            }
        }
    }

    if kind == "concept" {
        let concept_rows = load_ths_concepts_list(&source_path)?;
        for cols in concept_rows {
            let Some(ts_code) = cols.first().map(|value| value.trim()) else {
                continue;
            };
            let Some(concept_raw) = cols.get(2).map(|value| value.trim()) else {
                continue;
            };
            if ts_code.is_empty() || concept_raw.is_empty() {
                continue;
            }
            let is_match = concept_raw
                .split(|ch| {
                    matches!(ch, ',' | ';' | '，' | '；' | '|' | '、' | '/' | '\n' | '\r')
                })
                .map(|part| part.trim())
                .any(|part| !part.is_empty() && part == target_name);
            if is_match {
                target_codes.insert(ts_code.to_string());
            }
        }
    }

    if target_codes.is_empty() {
        return Ok(MarketContributionData {
            scope,
            kind,
            name: target_name,
            trade_date: Some(ref_date),
            start_date: interval_start,
            end_date: interval_end,
            lookback_period,
            contributors: Vec::new(),
        });
    }

    let mut contributors = Vec::new();
    if scope == "daily" {
        let mut stmt = source_conn
            .prepare(
                r#"
                SELECT ts_code, TRY_CAST(pct_chg AS DOUBLE) AS pct
                FROM stock_data
                WHERE adj_type = 'qfq'
                  AND trade_date = ?
                "#,
            )
            .map_err(|e| format!("预编译市场贡献当日 SQL 失败: {e}"))?;
        let mut rows = stmt
            .query(params![&ref_date])
            .map_err(|e| format!("执行市场贡献当日 SQL 失败: {e}"))?;

        while let Some(row) = rows
            .next()
            .map_err(|e| format!("读取市场贡献当日数据失败: {e}"))?
        {
            let ts_code: String = row.get(0).map_err(|e| format!("读取代码失败: {e}"))?;
            if !target_codes.contains(&ts_code) {
                continue;
            }
            let pct: Option<f64> = row.get(1).map_err(|e| format!("读取涨幅失败: {e}"))?;
            let Some(contribution_pct) = pct.filter(|v| v.is_finite()) else {
                continue;
            };
            contributors.push(MarketContributorItem {
                ts_code: ts_code.clone(),
                name: ts_name_map.get(&ts_code).cloned(),
                industry: ts_board_map.get(&ts_code).cloned(),
                contribution_pct,
            });
        }
    } else {
        let start = interval_start.clone().unwrap_or_else(|| ref_date.clone());
        let end = interval_end.clone().unwrap_or_else(|| ref_date.clone());
        let mut stmt = source_conn
            .prepare(
                r#"
                SELECT ts_code, AVG(TRY_CAST(pct_chg AS DOUBLE)) AS avg_pct
                FROM stock_data
                WHERE adj_type = 'qfq'
                  AND trade_date >= ?
                  AND trade_date <= ?
                GROUP BY 1
                "#,
            )
            .map_err(|e| format!("预编译市场贡献区间 SQL 失败: {e}"))?;
        let mut rows = stmt
            .query(params![&start, &end])
            .map_err(|e| format!("执行市场贡献区间 SQL 失败: {e}"))?;

        while let Some(row) = rows
            .next()
            .map_err(|e| format!("读取市场贡献区间数据失败: {e}"))?
        {
            let ts_code: String = row.get(0).map_err(|e| format!("读取代码失败: {e}"))?;
            if !target_codes.contains(&ts_code) {
                continue;
            }
            let pct: Option<f64> = row.get(1).map_err(|e| format!("读取涨幅失败: {e}"))?;
            let Some(contribution_pct) = pct.filter(|v| v.is_finite()) else {
                continue;
            };
            contributors.push(MarketContributorItem {
                ts_code: ts_code.clone(),
                name: ts_name_map.get(&ts_code).cloned(),
                industry: ts_board_map.get(&ts_code).cloned(),
                contribution_pct,
            });
        }
    }

    contributors.sort_by(|a, b| {
        b.contribution_pct
            .partial_cmp(&a.contribution_pct)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.ts_code.cmp(&b.ts_code))
    });
    contributors.truncate(100);

    Ok(MarketContributionData {
        scope,
        kind,
        name: target_name,
        trade_date: Some(ref_date),
        start_date: interval_start,
        end_date: interval_end,
        lookback_period,
        contributors,
    })
}

pub fn get_scene_layer_backtest_defaults(
    source_path: String,
) -> Result<SceneLayerBacktestDefaultsData, String> {
    let scene_options = load_scene_options(&source_path)?;

    let conn = open_result_conn(&source_path)?;
    let mut stmt = conn
        .prepare(
            r#"
            SELECT
                MIN(trade_date) AS min_trade_date,
                MAX(trade_date) AS max_trade_date
            FROM scene_details
            "#,
        )
        .map_err(|e| format!("预编译 scene_details 日期区间 SQL 失败: {e}"))?;

    let mut rows = stmt
        .query([])
        .map_err(|e| format!("执行 scene_details 日期区间 SQL 失败: {e}"))?;

    let (start_date, end_date) = if let Some(row) = rows
        .next()
        .map_err(|e| format!("读取 scene_details 日期区间失败: {e}"))?
    {
        let min_trade_date: Option<String> = row
            .get(0)
            .map_err(|e| format!("读取最小交易日失败: {e}"))?;
        let max_trade_date: Option<String> = row
            .get(1)
            .map_err(|e| format!("读取最大交易日失败: {e}"))?;
        (min_trade_date, max_trade_date)
    } else {
        (None, None)
    };

    Ok(SceneLayerBacktestDefaultsData {
        resolved_scene_name: scene_options.first().cloned(),
        scene_options,
        start_date,
        end_date,
    })
}

pub fn run_scene_layer_backtest(
    source_path: String,
    scene_name: String,
    stock_adj_type: Option<String>,
    index_ts_code: String,
    index_beta: Option<f64>,
    concept_beta: Option<f64>,
    board_beta: Option<f64>,
    start_date: String,
    end_date: String,
    min_samples_per_scene_day: Option<usize>,
    backtest_period: Option<usize>,
) -> Result<SceneLayerBacktestData, String> {
    let source_db = source_db_path(&source_path);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "原始库路径不是有效UTF-8".to_string())?;
    let source_conn =
        Connection::open(source_db_str).map_err(|e| format!("打开原始库失败: {e}"))?;

    let scene_name = scene_name.trim().to_string();
    let stock_adj_type = stock_adj_type
        .unwrap_or_else(|| "qfq".to_string())
        .trim()
        .to_string();
    let index_ts_code = index_ts_code.trim().to_string();
    let index_beta = index_beta.unwrap_or(0.5);
    let concept_beta = concept_beta.unwrap_or(0.2);
    let board_beta = board_beta.unwrap_or(0.0);
    let start_date = start_date.trim().to_string();
    let end_date = end_date.trim().to_string();
    let min_samples_per_scene_day = min_samples_per_scene_day.unwrap_or(5);
    let backtest_period = backtest_period.unwrap_or(1);

    let is_all_scenes = scene_name == "__ALL__";

    if is_all_scenes {
        let scene_options = load_scene_options(&source_path)?;
        let mut all_scene_summaries = Vec::new();

        for one_scene_name in scene_options {
            let input = SceneLayerFromDbInput {
                scene_name: one_scene_name.clone(),
                stock_adj_type: stock_adj_type.clone(),
                index_ts_code: index_ts_code.clone(),
                index_beta,
                concept_beta,
                board_beta,
                start_date: start_date.clone(),
                end_date: end_date.clone(),
                layer_config: SceneLayerConfig {
                    min_samples_per_day: min_samples_per_scene_day,
                    backtest_period,
                },
            };

            let metrics = calc_scene_layer_metrics_from_db(&source_conn, &source_path, &input)?;
            all_scene_summaries.push(SceneLayerSceneSummary {
                scene_name: one_scene_name,
                point_count: metrics.points.len(),
                spread_mean: metrics.spread_mean,
                ic_mean: metrics.ic_mean,
                ic_std: metrics.ic_std,
                icir: metrics.icir,
            });
        }

        all_scene_summaries.sort_by(|a, b| {
            b.spread_mean
                .unwrap_or(f64::NEG_INFINITY)
                .partial_cmp(&a.spread_mean.unwrap_or(f64::NEG_INFINITY))
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| b.point_count.cmp(&a.point_count))
                .then_with(|| a.scene_name.cmp(&b.scene_name))
        });

        return Ok(SceneLayerBacktestData {
            scene_name: "__ALL__".to_string(),
            stock_adj_type,
            index_ts_code,
            index_beta,
            concept_beta,
            board_beta,
            start_date,
            end_date,
            min_samples_per_scene_day,
            backtest_period,
            points: Vec::new(),
            spread_mean: None,
            ic_mean: None,
            ic_std: None,
            icir: None,
            is_all_scenes: true,
            all_scene_summaries,
        });
    }

    let input = SceneLayerFromDbInput {
        scene_name,
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        board_beta,
        start_date,
        end_date,
        layer_config: SceneLayerConfig {
            min_samples_per_day: min_samples_per_scene_day,
            backtest_period,
        },
    };

    let metrics = calc_scene_layer_metrics_from_db(&source_conn, &source_path, &input)?;

    Ok(SceneLayerBacktestData {
        scene_name: input.scene_name,
        stock_adj_type: input.stock_adj_type,
        index_ts_code: input.index_ts_code,
        index_beta: input.index_beta,
        concept_beta: input.concept_beta,
        board_beta: input.board_beta,
        start_date: input.start_date,
        end_date: input.end_date,
        min_samples_per_scene_day: input.layer_config.min_samples_per_day,
        backtest_period: input.layer_config.backtest_period,
        points: metrics
            .points
            .into_iter()
            .map(|point| SceneLayerPointPayload {
                trade_date: point.trade_date,
                state_avg_residual_returns: point
                    .state_avg_residual_returns
                    .into_iter()
                    .map(|(scene_state, avg_residual_return)| {
                        SceneLayerStateAvgResidualReturn {
                            scene_state,
                            avg_residual_return: Some(avg_residual_return),
                        }
                    })
                    .collect(),
                top_bottom_spread: point.top_bottom_spread,
                ic: point.ic,
            })
            .collect(),
        spread_mean: metrics.spread_mean,
        ic_mean: metrics.ic_mean,
        ic_std: metrics.ic_std,
        icir: metrics.icir,
        is_all_scenes: false,
        all_scene_summaries: Vec::new(),
    })
}
