//! 策略与场景统计页面：日度行、热力图、图表与触发个股明细。

use crate::data::{ScoreRule, ScoreScene};
use crate::statistics::common::{RuleDayAgg, RuleMeta, load_rule_meta, open_result_conn};
use duckdb::{Connection, params};
use lianghua_app_shared::{build_concepts_map, build_name_map};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
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
pub struct SceneStageRow {
    pub stage: String,
    pub sample_count: i64,
    pub stage_ratio_in_scene: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct SceneContributionSummary {
    pub scene_covered_count: i64,
    pub scene_total_sample_count: i64,
    pub scene_coverage_ratio: Option<f64>,
    pub scene_rule_contribution_score: Option<f64>,
    pub all_rule_contribution_score: Option<f64>,
    pub scene_rule_contribution_ratio: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct SceneStatisticsPageData {
    pub scene_options: Option<Vec<String>>,
    pub resolved_scene_name: Option<String>,
    pub analysis_trade_date_options: Option<Vec<String>>,
    pub resolved_analysis_trade_date: Option<String>,
    pub stage_rows: Option<Vec<SceneStageRow>>,
    pub summary: Option<SceneContributionSummary>,
}

pub(in crate::statistics) fn load_scene_options(source_path: &str) -> Result<Vec<String>, String> {
    let scenes = ScoreScene::load_scenes(source_path)?;
    Ok(scenes.into_iter().map(|scene| scene.name).collect())
}

pub(in crate::statistics) fn query_daily_rows(
    conn: &Connection,
    rule_order: &[String],
    meta_map: &HashMap<String, RuleMeta>,
) -> Result<Vec<StrategyDailyRow>, String> {
    let each_medians = (|conn: &Connection,
                         meta_map: &HashMap<String, RuleMeta>|
     -> Result<HashMap<(String, String), f64>, String> {
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
    })(conn, meta_map)?;
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
        .query(params![(100)])
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

pub(in crate::statistics) fn resolve_analysis_trade_date(
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

pub(in crate::statistics) fn build_chart(
    strategy_rows: &[StrategyDailyRow],
) -> StrategyChartPayload {
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

pub(in crate::statistics) fn query_triggered_stocks(
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
    let overview = (|conn: &Connection| -> Result<StrategyOverviewPayload, String> {
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
            let avg_level: Option<f64> =
                row.get(2).map_err(|e| format!("读取平均水平失败: {e}"))?;

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
    })(&conn)?;
    let (strategy_options, meta_map) = load_rule_meta(&source_path)?;
    let detail_rows_all = query_daily_rows(&conn, &strategy_options, &meta_map)?;

    let resolved_strategy_name =
        (|requested: Option<String>, strategy_options: &[String]| -> Option<String> {
            let requested = requested
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty());
            if let Some(name) = requested {
                if strategy_options.iter().any(|item| item == &name) {
                    return Some(name);
                }
            }
            None
        })(strategy_name, &strategy_options);

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

pub fn get_scene_statistics_page(
    source_path: String,
    scene_name: Option<String>,
    analysis_trade_date: Option<String>,
) -> Result<SceneStatisticsPageData, String> {
    let conn = open_result_conn(&source_path)?;
    let scene_options = load_scene_options(&source_path)?;
    let resolved_scene_name =
        (|requested: Option<String>, scene_options: &[String]| -> Option<String> {
            let requested = requested
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty());
            if let Some(scene_name) = requested {
                if scene_options.iter().any(|item| item == &scene_name) {
                    return Some(scene_name);
                }
            }
            scene_options.first().cloned()
        })(scene_name, &scene_options);
    let analysis_trade_date_options = (|conn: &Connection| -> Result<Vec<String>, String> {
        let mut stmt = conn
            .prepare(
                r#"
            SELECT DISTINCT trade_date
            FROM scene_details
            ORDER BY trade_date DESC
            "#,
            )
            .map_err(|e| format!("预编译 scene 交易日 SQL 失败: {e}"))?;
        let mut rows = stmt
            .query([])
            .map_err(|e| format!("执行 scene 交易日 SQL 失败: {e}"))?;

        let mut out = Vec::new();
        while let Some(row) = rows
            .next()
            .map_err(|e| format!("读取 scene 交易日失败: {e}"))?
        {
            let trade_date: String = row.get(0).map_err(|e| format!("读取交易日字段失败: {e}"))?;
            if !trade_date.trim().is_empty() {
                out.push(trade_date);
            }
        }

        Ok(out)
    })(&conn)?;
    let resolved_analysis_trade_date =
        resolve_analysis_trade_date(analysis_trade_date, &analysis_trade_date_options);

    let mut stage_rows = Vec::new();
    let mut summary = None;

    if let (Some(selected_scene_name), Some(selected_trade_date)) = (
        resolved_scene_name.as_deref(),
        resolved_analysis_trade_date.as_deref(),
    ) {
        let (next_stage_rows, total_sample_count, covered_count) =
            (|conn: &Connection,
              scene_name: &str,
              trade_date: &str|
             -> Result<(Vec<SceneStageRow>, i64, i64), String> {
                let mut stmt = conn
                    .prepare(
                        r#"
            SELECT
                COALESCE(NULLIF(stage, ''), 'none') AS stage,
                COUNT(*) AS sample_count
            FROM scene_details
            WHERE trade_date = ?
              AND scene_name = ?
            GROUP BY 1
            "#,
                    )
                    .map_err(|e| format!("预编译 scene 阶段统计 SQL 失败: {e}"))?;
                let mut rows = stmt
                    .query(params![trade_date, scene_name])
                    .map_err(|e| format!("执行 scene 阶段统计 SQL 失败: {e}"))?;

                let mut stage_count_map: HashMap<String, i64> = HashMap::new();
                while let Some(row) = rows
                    .next()
                    .map_err(|e| format!("读取 scene 阶段统计失败: {e}"))?
                {
                    let stage: String = row.get(0).map_err(|e| format!("读取阶段字段失败: {e}"))?;
                    let sample_count: i64 =
                        row.get(1).map_err(|e| format!("读取阶段数量失败: {e}"))?;
                    let normalized_stage = stage.trim().to_ascii_lowercase();
                    stage_count_map.insert(normalized_stage, sample_count);
                }

                let total_sample_count: i64 = stage_count_map.values().sum();
                let none_count = stage_count_map.get("none").copied().unwrap_or(0);
                let covered_count = (total_sample_count - none_count).max(0);

                let mut rows_out = Vec::new();
                let stage_order = ["trigger", "confirm", "observe", "fail", "none"];

                for stage in stage_order {
                    let sample_count = stage_count_map.remove(stage).unwrap_or(0);
                    rows_out.push(SceneStageRow {
                        stage: stage.to_string(),
                        sample_count,
                        stage_ratio_in_scene: if total_sample_count > 0 {
                            Some(sample_count as f64 / total_sample_count as f64)
                        } else {
                            None
                        },
                    });
                }

                let mut remain_stages = stage_count_map.into_iter().collect::<Vec<_>>();
                remain_stages.sort_by(|a, b| a.0.cmp(&b.0));
                for (stage, sample_count) in remain_stages {
                    rows_out.push(SceneStageRow {
                        stage,
                        sample_count,
                        stage_ratio_in_scene: if total_sample_count > 0 {
                            Some(sample_count as f64 / total_sample_count as f64)
                        } else {
                            None
                        },
                    });
                }

                Ok((rows_out, total_sample_count, covered_count))
            })(&conn, selected_scene_name, selected_trade_date)?;
        stage_rows = next_stage_rows;

        let scene_rule_name_sets =
            (|source_path: &str| -> Result<HashMap<String, HashSet<String>>, String> {
                let rules = ScoreRule::load_rules(source_path)?;
                let mut out: HashMap<String, HashSet<String>> = HashMap::new();

                for rule in rules {
                    out.entry(rule.scene_name).or_default().insert(rule.name);
                }

                Ok(out)
            })(&source_path)?;
        let contribution_by_rule = (|conn: &Connection,
                                     trade_date: &str|
         -> Result<HashMap<String, f64>, String> {
            let sql = r#"
        WITH daily_rank_bounds AS (
            SELECT
                trade_date,
                MAX(rank) AS max_rank
            FROM score_summary
            WHERE trade_date = ?
            GROUP BY 1
        ),
        triggered_rule_rows AS (
            SELECT *
            FROM rule_details
            WHERE trade_date = ?
              AND rule_score IS NOT NULL
              AND ABS(rule_score) > 1e-12
        )
        SELECT
            d.rule_name,
            SUM(
                CASE
                    WHEN s.rank IS NOT NULL
                      AND b.max_rank IS NOT NULL
                      AND b.max_rank > 0
                    THEN d.rule_score * CAST((b.max_rank + 1 - s.rank) AS DOUBLE) / CAST(b.max_rank AS DOUBLE)
                    ELSE 0
                END
            ) AS contribution_score
        FROM triggered_rule_rows AS d
        LEFT JOIN score_summary AS s
          ON s.ts_code = d.ts_code
         AND s.trade_date = d.trade_date
        LEFT JOIN daily_rank_bounds AS b
          ON b.trade_date = d.trade_date
        GROUP BY 1
    "#;

            let mut stmt = conn
                .prepare(sql)
                .map_err(|e| format!("预编译 scene 规则贡献度 SQL 失败: {e}"))?;
            let mut rows = stmt
                .query(params![trade_date, trade_date])
                .map_err(|e| format!("执行 scene 规则贡献度 SQL 失败: {e}"))?;

            let mut out = HashMap::new();
            while let Some(row) = rows
                .next()
                .map_err(|e| format!("读取 scene 规则贡献度失败: {e}"))?
            {
                let rule_name: String = row.get(0).map_err(|e| format!("读取规则名失败: {e}"))?;
                let contribution_score = row
                    .get::<usize, Option<f64>>(1)
                    .map_err(|e| format!("读取规则贡献度失败: {e}"))?
                    .unwrap_or(0.0);
                out.insert(rule_name, contribution_score);
            }

            Ok(out)
        })(&conn, selected_trade_date)?;
        summary = Some((|scene_total_sample_count: i64,
                         scene_covered_count: i64,
                         scene_rule_names: Option<&HashSet<String>>,
                         contribution_by_rule: &HashMap<String, f64>|
         -> SceneContributionSummary {
            let scene_rule_contribution_score = scene_rule_names.map(|rule_names| {
                contribution_by_rule
                    .iter()
                    .filter(|(rule_name, _)| rule_names.contains(*rule_name))
                    .map(|(_, score)| *score)
                    .sum::<f64>()
            });
            let all_rule_contribution_score = if contribution_by_rule.is_empty() {
                None
            } else {
                Some(contribution_by_rule.values().sum::<f64>())
            };
            let scene_rule_contribution_ratio =
                match (scene_rule_contribution_score, all_rule_contribution_score) {
                    (Some(scene_score), Some(all_score)) if all_score.abs() > 1e-12 => {
                        Some(scene_score / all_score)
                    }
                    _ => None,
                };

            SceneContributionSummary {
                scene_covered_count,
                scene_total_sample_count,
                scene_coverage_ratio: if scene_total_sample_count > 0 {
                    Some(scene_covered_count as f64 / scene_total_sample_count as f64)
                } else {
                    None
                },
                scene_rule_contribution_score,
                all_rule_contribution_score,
                scene_rule_contribution_ratio,
            }
        })(
            total_sample_count,
            covered_count,
            scene_rule_name_sets.get(selected_scene_name),
            &contribution_by_rule,
        ));
    }

    Ok(SceneStatisticsPageData {
        scene_options: Some(scene_options),
        resolved_scene_name,
        analysis_trade_date_options: Some(analysis_trade_date_options),
        resolved_analysis_trade_date,
        stage_rows: Some(stage_rows),
        summary,
    })
}
