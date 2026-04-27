use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
};

use duckdb::{Connection, params};
use serde::Serialize;

use crate::{
    data::{RowData, ScoreConfig},
    data::{cyq_db_path, result_db_path, score_rule_path, source_db_path},
    download::ind_calc::{cache_ind_build, calc_inds_with_cache},
    scoring::tools::{inject_stock_extra_fields, load_st_list},
    ui_tools_feat::{
        build_area_map, build_circ_mv_map, build_concepts_map, build_industry_map,
        build_most_related_concept_map, build_name_map, build_total_mv_map,
        chart_indicator::{
            ChartMarkerPosition, ChartMarkerShape, ChartPanelKind, ChartPanelRole, ChartSeriesKind,
            ChartTooltipFormat, CompiledChartIndicatorConfig, execute_chart_indicator_config,
            load_compiled_chart_indicator_config,
        },
        realtime::{RealtimeFetchMeta, fetch_realtime_quote_map, normalize_quote_trade_date},
        stock_similarity::{StockSimilarityPageData, get_stock_similarity_page_with_conn},
    },
    utils::utils::board_category,
};

const DEFAULT_ADJ_TYPE: &str = "qfq";

#[derive(Debug, Serialize)]
pub struct DetailOverview {
    pub ts_code: String,
    pub name: Option<String>,
    pub board: Option<String>,
    pub area: Option<String>,
    pub industry: Option<String>,
    pub trade_date: Option<String>,
    pub total_score: Option<f64>,
    pub rank: Option<i64>,
    pub total: Option<i64>,
    pub total_mv_yi: Option<f64>,
    pub circ_mv_yi: Option<f64>,
    pub most_related_concept: Option<String>,
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
    pub is_realtime: Option<bool>,
    pub realtime_color_hint: Option<String>,
    #[serde(flatten)]
    pub indicators: HashMap<String, serde_json::Value>,
    #[serde(skip)]
    runtime_values: HashMap<String, Option<f64>>,
}

#[derive(Debug, Serialize)]
pub struct DetailKlinePanel {
    pub key: String,
    pub label: String,
    pub role: Option<String>,
    pub kind: Option<String>,
    pub series: Option<Vec<DetailKlineSeries>>,
    pub markers: Option<Vec<DetailKlineMarker>>,
    pub tooltips: Option<Vec<DetailKlineTooltip>>,
}

#[derive(Debug, Serialize)]
pub struct DetailKlineSeries {
    pub key: String,
    pub label: Option<String>,
    pub kind: String,
    pub color: Option<String>,
    pub color_when: Option<Vec<DetailKlineColorRule>>,
    pub line_width: Option<f64>,
    pub opacity: Option<f64>,
    pub base_value: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct DetailKlineMarker {
    pub key: String,
    pub label: Option<String>,
    pub when_key: String,
    pub y_key: Option<String>,
    pub position: Option<String>,
    pub shape: Option<String>,
    pub color: Option<String>,
    pub text: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DetailKlineTooltip {
    pub key: String,
    pub label: Option<String>,
    pub value_key: String,
    pub format: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DetailKlineColorRule {
    pub when_key: String,
    pub color: String,
}

#[derive(Debug, Serialize)]
pub struct DetailKlinePayload {
    pub items: Option<Vec<DetailKlineRow>>,
    pub panels: Option<Vec<DetailKlinePanel>>,
    pub default_window: Option<u32>,
    pub chart_height: Option<u32>,
    pub watermark_name: Option<String>,
    pub watermark_code: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DetailStrategyTriggerRow {
    pub rule_name: String,
    pub scene_name: Option<String>,
    pub rule_score: Option<f64>,
    pub is_triggered: Option<bool>,
    pub hit_date: Option<String>,
    pub lag: Option<i64>,
    pub explain: Option<String>,
    pub when: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DetailStrategyPayload {
    pub triggered: Option<Vec<DetailStrategyTriggerRow>>,
    pub untriggered: Option<Vec<DetailStrategyTriggerRow>>,
}

#[derive(Debug, Serialize)]
pub struct DetailSceneTriggerRow {
    pub scene_name: String,
    pub direction: Option<String>,
    pub stage: Option<String>,
    pub stage_score: Option<f64>,
    pub risk_score: Option<f64>,
    pub confirm_strength: Option<f64>,
    pub risk_intensity: Option<f64>,
    pub scene_rank: Option<i64>,
    pub hit_date: Option<String>,
    pub lag: Option<i64>,
    pub observe_threshold: Option<f64>,
    pub trigger_threshold: Option<f64>,
    pub confirm_threshold: Option<f64>,
    pub fail_threshold: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct DetailScenePayload {
    pub triggered: Option<Vec<DetailSceneTriggerRow>>,
    pub untriggered: Option<Vec<DetailSceneTriggerRow>>,
}

#[derive(Debug, Serialize)]
pub struct StockDetailPageData {
    pub resolved_trade_date: Option<String>,
    pub resolved_ts_code: Option<String>,
    pub overview: Option<DetailOverview>,
    pub prev_ranks: Option<Vec<DetailPrevRankRow>>,
    pub stock_similarity: Option<StockSimilarityPageData>,
    pub stock_similarity_error: Option<String>,
    pub kline: Option<DetailKlinePayload>,
    pub strategy_triggers: Option<DetailStrategyPayload>,
    pub strategy_scenes: Option<DetailScenePayload>,
}

#[derive(Debug, Serialize)]
pub struct StockDetailStrategySnapshotData {
    pub resolved_trade_date: Option<String>,
    pub resolved_ts_code: Option<String>,
    pub strategy_triggers: Option<DetailStrategyPayload>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StockDetailRealtimeData {
    pub ts_code: String,
    pub refreshed_at: Option<String>,
    pub quote_trade_date: Option<String>,
    pub quote_time: Option<String>,
    pub has_database_trade_date: bool,
    pub kline: DetailKlinePayload,
}

#[derive(Debug, Serialize, Clone)]
pub struct DetailCyqBin {
    pub price: f64,
    pub price_low: f64,
    pub price_high: f64,
    pub chip: f64,
    pub chip_pct: f64,
}

#[derive(Debug, Serialize, Clone)]
pub struct DetailCyqSnapshot {
    pub trade_date: String,
    pub close: f64,
    pub bins: Vec<DetailCyqBin>,
}

#[derive(Debug, Serialize, Clone)]
pub struct StockDetailCyqData {
    pub resolved_ts_code: String,
    pub factor: Option<u32>,
    pub snapshots: Vec<DetailCyqSnapshot>,
}

#[derive(Debug)]
struct RuleMeta {
    rule_name: String,
    scene_name: String,
    explain: String,
    when: String,
}

#[derive(Debug)]
struct SceneMeta {
    scene_name: String,
    direction: String,
    observe_threshold: f64,
    trigger_threshold: f64,
    confirm_threshold: f64,
    fail_threshold: f64,
}

#[derive(Debug, Clone, Copy)]
struct CurrentRuleState {
    rule_score: f64,
    is_triggered: bool,
}

#[derive(Debug, Clone)]
struct CurrentSceneState {
    direction: Option<String>,
    stage: Option<String>,
    stage_score: f64,
    risk_score: f64,
    confirm_strength: f64,
    risk_intensity: f64,
    scene_rank: Option<i64>,
    is_triggered: bool,
}

#[derive(Debug, Default)]
struct DetailTriggerSnapshot {
    current_rule_state_map: HashMap<String, CurrentRuleState>,
    latest_rule_hit_date_map: HashMap<String, String>,
    current_scene_state_map: HashMap<String, CurrentSceneState>,
    latest_scene_hit_date_map: HashMap<String, String>,
    trade_day_index_map: HashMap<String, usize>,
}

fn resolve_trade_date(conn: &Connection, trade_date: Option<String>) -> Result<String, String> {
    if let Some(d) = trade_date {
        let d = d.trim().to_string();
        if !d.is_empty() {
            return Ok(d);
        }
    }

    let mut stmt = conn
        .prepare("SELECT MAX(trade_date) FROM score_summary")
        .map_err(|e| format!("查询最新交易日预编译失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询最新交易日失败: {e}"))?;

    if let Some(row) = rows
        .next()
        .map_err(|e| format!("读取最新交易日结果失败: {e}"))?
    {
        let d: Option<String> = row
            .get(0)
            .map_err(|e| format!("读取最新交易日字段失败: {e}"))?;
        if let Some(v) = d {
            if !v.trim().is_empty() {
                return Ok(v);
            }
        }
    }
    Err("score_summary 没有可用交易日".to_string())
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

fn open_cyq_conn(source_path: &str) -> Result<Option<Connection>, String> {
    let cyq_db = cyq_db_path(source_path);
    if !cyq_db.exists() {
        return Ok(None);
    }
    let cyq_db_str = cyq_db
        .to_str()
        .ok_or_else(|| "筹码库路径不是有效UTF-8".to_string())?;
    Connection::open(cyq_db_str)
        .map(Some)
        .map_err(|e| format!("打开筹码库失败: {e}"))
}

fn cyq_table_exists(conn: &Connection, table_name: &str) -> Result<bool, String> {
    let count = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| format!("检查筹码表结构失败: {e}"))?;
    Ok(count > 0)
}

fn query_stock_detail_cyq(source_path: &str, ts_code: &str) -> Result<StockDetailCyqData, String> {
    let normalized_ts_code = normalize_ts_code(ts_code);
    let Some(conn) = open_cyq_conn(source_path)? else {
        return Ok(StockDetailCyqData {
            resolved_ts_code: normalized_ts_code,
            factor: None,
            snapshots: Vec::new(),
        });
    };
    if !cyq_table_exists(&conn, "cyq_snapshot")? || !cyq_table_exists(&conn, "cyq_bin")? {
        return Ok(StockDetailCyqData {
            resolved_ts_code: normalized_ts_code,
            factor: None,
            snapshots: Vec::new(),
        });
    }

    let mut factor_stmt = conn
        .prepare("SELECT MAX(factor) FROM cyq_snapshot WHERE ts_code = ? AND adj_type = ?")
        .map_err(|e| format!("预编译筹码分桶查询失败: {e}"))?;
    let mut factor_rows = factor_stmt
        .query(params![&normalized_ts_code, DEFAULT_ADJ_TYPE])
        .map_err(|e| format!("查询筹码分桶失败: {e}"))?;
    let factor = if let Some(row) = factor_rows
        .next()
        .map_err(|e| format!("读取筹码分桶失败: {e}"))?
    {
        let value: Option<i64> = row.get(0).map_err(|e| format!("读取筹码分桶值失败: {e}"))?;
        value.and_then(|item| u32::try_from(item.max(0)).ok())
    } else {
        None
    };

    let mut snapshot_stmt = conn
        .prepare(
            r#"
            SELECT trade_date, close
            FROM cyq_snapshot
            WHERE ts_code = ? AND adj_type = ?
            ORDER BY trade_date ASC
            "#,
        )
        .map_err(|e| format!("预编译筹码摘要查询失败: {e}"))?;
    let mut snapshot_rows = snapshot_stmt
        .query(params![&normalized_ts_code, DEFAULT_ADJ_TYPE])
        .map_err(|e| format!("查询筹码摘要失败: {e}"))?;

    let mut snapshots = Vec::new();
    while let Some(row) = snapshot_rows
        .next()
        .map_err(|e| format!("读取筹码摘要失败: {e}"))?
    {
        let trade_date: String = row
            .get(0)
            .map_err(|e| format!("读取筹码摘要日期失败: {e}"))?;
        let close: Option<f64> = row
            .get(1)
            .map_err(|e| format!("读取筹码摘要收盘价失败: {e}"))?;
        snapshots.push(DetailCyqSnapshot {
            trade_date,
            close: close.unwrap_or(0.0),
            bins: Vec::new(),
        });
    }

    if snapshots.is_empty() {
        return Ok(StockDetailCyqData {
            resolved_ts_code: normalized_ts_code,
            factor,
            snapshots,
        });
    }

    let mut snapshot_index_by_trade_date = HashMap::with_capacity(snapshots.len());
    for (index, snapshot) in snapshots.iter().enumerate() {
        snapshot_index_by_trade_date.insert(snapshot.trade_date.clone(), index);
    }

    let mut bin_stmt = conn
        .prepare(
            r#"
            SELECT trade_date, price, price_low, price_high, chip, chip_pct
            FROM cyq_bin
            WHERE ts_code = ? AND adj_type = ?
            ORDER BY trade_date ASC, bin_index ASC
            "#,
        )
        .map_err(|e| format!("预编译筹码分布查询失败: {e}"))?;
    let mut bin_rows = bin_stmt
        .query(params![&normalized_ts_code, DEFAULT_ADJ_TYPE])
        .map_err(|e| format!("查询筹码分布失败: {e}"))?;

    while let Some(row) = bin_rows
        .next()
        .map_err(|e| format!("读取筹码分布失败: {e}"))?
    {
        let trade_date: String = row
            .get(0)
            .map_err(|e| format!("读取筹码分布日期失败: {e}"))?;
        let Some(snapshot_index) = snapshot_index_by_trade_date.get(&trade_date).copied() else {
            continue;
        };
        snapshots[snapshot_index].bins.push(DetailCyqBin {
            price: row.get(1).map_err(|e| format!("读取筹码价格失败: {e}"))?,
            price_low: row
                .get(2)
                .map_err(|e| format!("读取筹码价格下沿失败: {e}"))?,
            price_high: row
                .get(3)
                .map_err(|e| format!("读取筹码价格上沿失败: {e}"))?,
            chip: row.get(4).map_err(|e| format!("读取筹码值失败: {e}"))?,
            chip_pct: row.get(5).map_err(|e| format!("读取筹码占比失败: {e}"))?,
        });
    }

    Ok(StockDetailCyqData {
        resolved_ts_code: normalized_ts_code,
        factor,
        snapshots,
    })
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
    let area_map = build_area_map(source_path)?;
    let industry_map = build_industry_map(source_path)?;
    let total_mv_map = build_total_mv_map(source_path)?;
    let circ_mv_map = build_circ_mv_map(source_path)?;
    let concept_map = build_concepts_map(source_path)?;
    let most_related_concept_map = build_most_related_concept_map(source_path)?;

    Ok(DetailOverview {
        ts_code: ts_code.to_string(),
        name: name_map.get(ts_code).cloned(),
        board: Some(
            board_category(ts_code, name_map.get(ts_code).map(|value| value.as_str())).to_string(),
        ),
        area: area_map.get(ts_code).cloned(),
        industry: industry_map.get(ts_code).cloned(),
        trade_date: Some(effective_trade_date.to_string()),
        total_score: row
            .get(0)
            .map_err(|e| format!("读取详情 total_score 失败: {e}"))?,
        rank: row.get(1).map_err(|e| format!("读取详情 rank 失败: {e}"))?,
        total,
        total_mv_yi: total_mv_map.get(ts_code).copied(),
        circ_mv_yi: circ_mv_map.get(ts_code).copied(),
        most_related_concept: most_related_concept_map.get(ts_code).cloned(),
        concept: concept_map.get(ts_code).cloned(),
    })
}

fn query_rank_history(
    conn: &Connection,
    ts_code: &str,
    limit: Option<usize>,
) -> Result<Vec<DetailPrevRankRow>, String> {
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

    if let Some(limit) = limit {
        if out.len() > limit {
            out.truncate(limit);
        }
    }

    Ok(out)
}

fn query_latest_kline_trade_date(
    source_conn: &Connection,
    ts_code: &str,
) -> Result<String, String> {
    let mut stmt = source_conn
        .prepare(
            r#"
            SELECT MAX(trade_date)
            FROM stock_data
            WHERE ts_code = ? AND adj_type = ?
            "#,
        )
        .map_err(|e| format!("预编译最新K线日期失败: {e}"))?;
    let mut rows = stmt
        .query(params![ts_code, DEFAULT_ADJ_TYPE])
        .map_err(|e| format!("查询最新K线日期失败: {e}"))?;

    if let Some(row) = rows
        .next()
        .map_err(|e| format!("读取最新K线日期失败: {e}"))?
    {
        let trade_date: Option<String> = row
            .get(0)
            .map_err(|e| format!("读取最新K线日期字段失败: {e}"))?;
        if let Some(value) = trade_date {
            if !value.trim().is_empty() {
                return Ok(value);
            }
        }
    }

    Err(format!("{ts_code} 没有可用K线日期"))
}

fn build_basic_detail_overview(
    source_path: &str,
    effective_trade_date: &str,
    ts_code: &str,
) -> DetailOverview {
    let name_map = build_name_map(source_path).unwrap_or_default();
    let area_map = build_area_map(source_path).unwrap_or_default();
    let industry_map = build_industry_map(source_path).unwrap_or_default();
    let total_mv_map = build_total_mv_map(source_path).unwrap_or_default();
    let circ_mv_map = build_circ_mv_map(source_path).unwrap_or_default();
    let concept_map = build_concepts_map(source_path).unwrap_or_default();
    let most_related_concept_map = build_most_related_concept_map(source_path).unwrap_or_default();

    DetailOverview {
        ts_code: ts_code.to_string(),
        name: name_map.get(ts_code).cloned(),
        board: Some(
            board_category(ts_code, name_map.get(ts_code).map(|value| value.as_str())).to_string(),
        ),
        area: area_map.get(ts_code).cloned(),
        industry: industry_map.get(ts_code).cloned(),
        trade_date: Some(effective_trade_date.to_string()),
        total_score: None,
        rank: None,
        total: None,
        total_mv_yi: total_mv_map.get(ts_code).copied(),
        circ_mv_yi: circ_mv_map.get(ts_code).copied(),
        most_related_concept: most_related_concept_map.get(ts_code).cloned(),
        concept: concept_map.get(ts_code).cloned(),
    }
}

#[cfg(test)]
fn default_kline_panels() -> Vec<DetailKlinePanel> {
    let config = crate::ui_tools_feat::chart_indicator::load_chart_indicator_config("")
        .unwrap_or_else(|_| {
            crate::ui_tools_feat::chart_indicator::default_chart_indicator_config()
        });
    let compiled =
        crate::ui_tools_feat::chart_indicator::compile_chart_indicator_config(&config, None)
            .expect("default chart indicator config should compile");
    detail_kline_panels_from_compiled(&compiled)
}

fn detail_kline_panels_from_compiled(
    compiled: &CompiledChartIndicatorConfig,
) -> Vec<DetailKlinePanel> {
    compiled
        .panels
        .iter()
        .map(|panel| {
            let series = panel
                .series
                .iter()
                .map(|series| DetailKlineSeries {
                    key: series.key.clone(),
                    label: series.render.label.clone(),
                    kind: chart_series_kind_name(series.render.kind).to_string(),
                    color: series.render.color.clone(),
                    color_when: if series.color_rules.is_empty() {
                        None
                    } else {
                        Some(
                            series
                                .color_rules
                                .iter()
                                .map(|rule| DetailKlineColorRule {
                                    when_key: rule.when_key.clone(),
                                    color: rule.color.clone(),
                                })
                                .collect(),
                        )
                    },
                    line_width: series.render.line_width,
                    opacity: series.render.opacity,
                    base_value: series.render.base_value,
                })
                .collect::<Vec<_>>();
            let markers = panel
                .markers
                .iter()
                .map(|marker| DetailKlineMarker {
                    key: marker.key.clone(),
                    label: marker.render.label.clone(),
                    when_key: marker.when_key.clone(),
                    y_key: marker.y_key.clone(),
                    position: marker
                        .render
                        .position
                        .map(chart_marker_position_name)
                        .map(str::to_string),
                    shape: marker
                        .render
                        .shape
                        .map(chart_marker_shape_name)
                        .map(str::to_string),
                    color: marker.render.color.clone(),
                    text: marker.render.text.clone(),
                })
                .collect::<Vec<_>>();
            DetailKlinePanel {
                key: panel.key.clone(),
                label: panel.label.clone(),
                role: Some(chart_panel_role_name(panel.role).to_string()),
                kind: Some(chart_panel_kind_name(panel.kind).to_string()),
                series: Some(series),
                markers: Some(markers),
                tooltips: Some(
                    panel
                        .tooltips
                        .iter()
                        .map(|tooltip| DetailKlineTooltip {
                            key: tooltip.key.clone(),
                            label: tooltip.render.label.clone(),
                            value_key: tooltip.value_key.clone(),
                            format: tooltip
                                .render
                                .format
                                .map(chart_tooltip_format_name)
                                .map(str::to_string),
                        })
                        .collect(),
                ),
            }
        })
        .collect()
}

fn chart_panel_role_name(role: ChartPanelRole) -> &'static str {
    match role {
        ChartPanelRole::Main => "main",
        ChartPanelRole::Sub => "sub",
    }
}

fn chart_panel_kind_name(kind: ChartPanelKind) -> &'static str {
    match kind {
        ChartPanelKind::Candles => "candles",
        ChartPanelKind::Line => "line",
        ChartPanelKind::Bar => "bar",
        ChartPanelKind::Brick => "brick",
    }
}

fn chart_series_kind_name(kind: ChartSeriesKind) -> &'static str {
    match kind {
        ChartSeriesKind::Line => "line",
        ChartSeriesKind::Bar => "bar",
        ChartSeriesKind::Histogram => "histogram",
        ChartSeriesKind::Area => "area",
        ChartSeriesKind::Band => "band",
        ChartSeriesKind::Brick => "brick",
    }
}

fn chart_marker_position_name(position: ChartMarkerPosition) -> &'static str {
    match position {
        ChartMarkerPosition::Above => "above",
        ChartMarkerPosition::Below => "below",
        ChartMarkerPosition::Value => "value",
    }
}

fn chart_marker_shape_name(shape: ChartMarkerShape) -> &'static str {
    match shape {
        ChartMarkerShape::Dot => "dot",
        ChartMarkerShape::TriangleUp => "triangle_up",
        ChartMarkerShape::TriangleDown => "triangle_down",
        ChartMarkerShape::Flag => "flag",
    }
}

fn chart_tooltip_format_name(format: ChartTooltipFormat) -> &'static str {
    match format {
        ChartTooltipFormat::Number => "number",
        ChartTooltipFormat::Percent => "percent",
        ChartTooltipFormat::Ratio => "ratio",
    }
}

fn load_stock_data_columns(source_conn: &Connection) -> Result<HashSet<String>, String> {
    let mut stmt = source_conn
        .prepare("DESCRIBE stock_data")
        .map_err(|e| format!("预编译 stock_data 列查询失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询 stock_data 列失败: {e}"))?;
    let mut columns = HashSet::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取 stock_data 列失败: {e}"))?
    {
        let name: String = row
            .get(0)
            .map_err(|e| format!("读取 stock_data 列名失败: {e}"))?;
        columns.insert(name);
    }
    Ok(columns)
}

fn build_case_insensitive_column_lookup(columns: &HashSet<String>) -> HashMap<String, String> {
    columns
        .iter()
        .map(|column| (column.to_ascii_lowercase(), column.clone()))
        .collect()
}

fn resolve_kline_base_columns(
    column_lookup: &HashMap<String, String>,
) -> Result<Vec<(String, String)>, String> {
    let pairs = [
        ("open", "O"),
        ("high", "H"),
        ("low", "L"),
        ("close", "C"),
        ("vol", "V"),
        ("amount", "AMOUNT"),
        ("pre_close", "PRE_CLOSE"),
        ("change", "CHANGE"),
        ("pct_chg", "PCT_CHG"),
        ("tor", "TOR"),
    ];

    pairs
        .into_iter()
        .map(|(db_col, runtime_key)| {
            let Some(actual) = column_lookup.get(db_col).cloned() else {
                return Err(format!("stock_data 缺少基础列: {db_col}"));
            };
            Ok((actual, runtime_key.to_string()))
        })
        .collect()
}

fn quote_sql_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn apply_chart_indicator_execution(
    items: &mut [DetailKlineRow],
    values: HashMap<String, Vec<serde_json::Value>>,
) {
    for (key, series) in values {
        if is_fixed_kline_output_key(&key) {
            continue;
        }
        for (item, value) in items.iter_mut().zip(series) {
            item.indicators.insert(key.clone(), value);
        }
    }
}

fn is_fixed_kline_output_key(key: &str) -> bool {
    matches!(
        key.trim().to_ascii_lowercase().as_str(),
        "trade_date"
            | "open"
            | "high"
            | "low"
            | "close"
            | "vol"
            | "amount"
            | "tor"
            | "is_realtime"
            | "realtime_color_hint"
    )
}

fn inject_chart_indicator_extra_runtime_fields(
    row_data: &mut RowData,
    source_path: &str,
    ts_code: &str,
) -> Result<(), String> {
    let st_list = load_st_list(source_path).unwrap_or_default();
    let fallback_total_mv_yi = build_total_mv_map(source_path)
        .ok()
        .and_then(|map| map.get(ts_code).copied());
    inject_stock_extra_fields(
        row_data,
        ts_code,
        st_list.contains(ts_code),
        fallback_total_mv_yi,
    )?;
    inject_chart_indicator_rank_series(row_data, source_path, ts_code)
}

fn inject_chart_indicator_rank_series(
    row_data: &mut RowData,
    source_path: &str,
    ts_code: &str,
) -> Result<(), String> {
    let len = row_data.trade_dates.len();
    let mut rank_series = vec![None; len];
    if len == 0 {
        row_data.cols.insert("RANK".to_string(), rank_series);
        return row_data.validate();
    }

    let Some(start_date) = row_data.trade_dates.first() else {
        row_data.cols.insert("RANK".to_string(), rank_series);
        return row_data.validate();
    };
    let Some(end_date) = row_data.trade_dates.last() else {
        row_data.cols.insert("RANK".to_string(), rank_series);
        return row_data.validate();
    };
    let rank_map = load_chart_indicator_rank_series_map(source_path, ts_code, start_date, end_date);
    for (index, trade_date) in row_data.trade_dates.iter().enumerate() {
        rank_series[index] = rank_map.get(trade_date).copied().flatten();
    }

    row_data.cols.insert("RANK".to_string(), rank_series);
    row_data.validate()
}

fn load_chart_indicator_rank_series_map(
    source_path: &str,
    ts_code: &str,
    start_date: &str,
    end_date: &str,
) -> HashMap<String, Option<f64>> {
    let result_db = result_db_path(source_path);
    if !result_db.exists() {
        return HashMap::new();
    }

    let Some(result_db_str) = result_db.to_str() else {
        return HashMap::new();
    };
    let Ok(conn) = Connection::open(result_db_str) else {
        return HashMap::new();
    };
    let Ok(mut stmt) = conn.prepare(
        r#"
        SELECT trade_date, rank
        FROM score_summary
        WHERE ts_code = ? AND trade_date >= ? AND trade_date <= ?
        "#,
    ) else {
        return HashMap::new();
    };
    let Ok(mut rows) = stmt.query(params![ts_code, start_date, end_date]) else {
        return HashMap::new();
    };

    let mut out = HashMap::new();
    while let Ok(Some(row)) = rows.next() {
        let Ok(trade_date) = row.get::<_, String>(0) else {
            continue;
        };
        let rank = row
            .get::<_, Option<i64>>(1)
            .ok()
            .flatten()
            .map(|value| value as f64);
        out.insert(trade_date, rank);
    }
    out
}

fn query_kline(
    source_conn: &Connection,
    source_path: &str,
    ts_code: &str,
    default_window_days: usize,
    watermark_name: Option<String>,
) -> Result<DetailKlinePayload, String> {
    let db_columns = load_stock_data_columns(source_conn)?;
    let compiled = load_compiled_chart_indicator_config(source_path, Some(&db_columns))?;
    let panels = detail_kline_panels_from_compiled(&compiled);

    let column_lookup = build_case_insensitive_column_lookup(&db_columns);
    let base_columns = resolve_kline_base_columns(&column_lookup)?;
    let dependency_columns = compiled
        .database_indicator_columns
        .iter()
        .filter(|db_col| {
            !base_columns
                .iter()
                .any(|(base_db_col, _)| base_db_col.eq_ignore_ascii_case(db_col))
        })
        .cloned()
        .collect::<Vec<_>>();
    let mut select_cols = vec!["trade_date".to_string()];
    for (db_col, _) in &base_columns {
        select_cols.push(format!(
            "TRY_CAST({} AS DOUBLE) AS {}",
            quote_sql_ident(db_col),
            quote_sql_ident(db_col)
        ));
    }
    for db_col in &dependency_columns {
        select_cols.push(format!(
            "TRY_CAST({} AS DOUBLE) AS {}",
            quote_sql_ident(db_col),
            quote_sql_ident(db_col)
        ));
    }
    let query_sql = format!(
        r#"
            SELECT
                {}
            FROM stock_data
            WHERE ts_code = ? AND adj_type = ?
            ORDER BY trade_date ASC
            "#,
        select_cols.join(",\n")
    );

    let mut stmt = source_conn
        .prepare(&query_sql)
        .map_err(|e| format!("预编译K线查询失败: {e}"))?;
    let mut rows = stmt
        .query(params![ts_code, DEFAULT_ADJ_TYPE])
        .map_err(|e| format!("查询K线数据失败: {e}"))?;

    let mut items = Vec::new();
    let mut row_data = RowData {
        trade_dates: Vec::new(),
        cols: HashMap::new(),
    };
    for (_, runtime_key) in &base_columns {
        row_data.cols.insert(runtime_key.clone(), Vec::new());
    }
    for db_col in &dependency_columns {
        row_data
            .cols
            .entry(db_col.to_ascii_uppercase())
            .or_default();
    }

    while let Some(row) = rows.next().map_err(|e| format!("读取K线数据失败: {e}"))? {
        let trade_date: String = row.get(0).map_err(|e| format!("读取K线日期失败: {e}"))?;
        row_data.trade_dates.push(trade_date.clone());
        let mut open = None;
        let mut high = None;
        let mut low = None;
        let mut close = None;
        let mut vol = None;
        let mut amount = None;
        let mut tor = None;
        for (index, (_, runtime_key)) in base_columns.iter().enumerate() {
            let value: Option<f64> = row
                .get(index + 1)
                .map_err(|e| format!("读取 {runtime_key} 失败: {e}"))?;
            match runtime_key.as_str() {
                "O" => open = value,
                "H" => high = value,
                "L" => low = value,
                "C" => close = value,
                "V" => vol = value,
                "AMOUNT" => amount = value,
                "TOR" => tor = value,
                _ => {}
            }
            row_data
                .cols
                .get_mut(runtime_key)
                .expect("base runtime key should exist")
                .push(value);
        }
        let dependency_start = 1 + base_columns.len();
        let mut runtime_values = HashMap::new();
        for (index, db_col) in dependency_columns.iter().enumerate() {
            let value: Option<f64> = row
                .get(dependency_start + index)
                .map_err(|e| format!("读取 {db_col} 失败: {e}"))?;
            row_data
                .cols
                .entry(db_col.to_ascii_uppercase())
                .or_default()
                .push(value);
            runtime_values.insert(db_col.to_ascii_uppercase(), value);
        }

        items.push(DetailKlineRow {
            trade_date,
            open,
            high,
            low,
            close,
            vol,
            amount,
            tor,
            is_realtime: None,
            realtime_color_hint: None,
            indicators: HashMap::new(),
            runtime_values,
        });
    }
    inject_chart_indicator_extra_runtime_fields(&mut row_data, source_path, ts_code)?;
    let execution = execute_chart_indicator_config(&compiled, row_data)?;
    apply_chart_indicator_execution(&mut items, execution.values);

    Ok(DetailKlinePayload {
        items: Some(items),
        panels: Some(panels),
        default_window: Some(default_window_days as u32),
        chart_height: Some(820),
        watermark_name,
        watermark_code: Some(split_ts_code(ts_code)),
    })
}

fn build_chart_indicator_row_data_from_items(
    items: &[DetailKlineRow],
    source_path: &str,
    ts_code: &str,
    realtime_pre_close: Option<f64>,
    compiled: &CompiledChartIndicatorConfig,
) -> Result<RowData, String> {
    if items.is_empty() {
        return Err("详情图指标计算失败: items为空".to_string());
    }

    let mut trade_dates = Vec::with_capacity(items.len());
    let mut cols = HashMap::with_capacity(10 + compiled.database_indicator_columns.len());
    for key in [
        "O",
        "H",
        "L",
        "C",
        "V",
        "AMOUNT",
        "PRE_CLOSE",
        "CHANGE",
        "PCT_CHG",
        "TURNOVER_RATE",
    ] {
        cols.insert(key.to_string(), Vec::with_capacity(items.len()));
    }
    for db_col in &compiled.database_indicator_columns {
        cols.entry(db_col.to_ascii_uppercase())
            .or_insert_with(|| Vec::with_capacity(items.len()));
    }

    let mut prev_close = None;
    let last_index = items.len().saturating_sub(1);
    for (index, item) in items.iter().enumerate() {
        trade_dates.push(item.trade_date.clone());

        let pre_close = if index == last_index && item.is_realtime == Some(true) {
            realtime_pre_close.or(prev_close)
        } else {
            prev_close
        };
        let change = match (item.close, pre_close) {
            (Some(close), Some(previous)) => Some(close - previous),
            _ => None,
        };
        let pct_chg = match (change, pre_close) {
            (Some(change_value), Some(previous)) if previous.abs() > f64::EPSILON => {
                Some(change_value / previous * 100.0)
            }
            _ => None,
        };

        cols.get_mut("O").expect("O should exist").push(item.open);
        cols.get_mut("H").expect("H should exist").push(item.high);
        cols.get_mut("L").expect("L should exist").push(item.low);
        cols.get_mut("C").expect("C should exist").push(item.close);
        cols.get_mut("V").expect("V should exist").push(item.vol);
        cols.get_mut("AMOUNT")
            .expect("AMOUNT should exist")
            .push(item.amount);
        cols.get_mut("PRE_CLOSE")
            .expect("PRE_CLOSE should exist")
            .push(pre_close);
        cols.get_mut("CHANGE")
            .expect("CHANGE should exist")
            .push(change);
        cols.get_mut("PCT_CHG")
            .expect("PCT_CHG should exist")
            .push(pct_chg);
        cols.get_mut("TURNOVER_RATE")
            .expect("TURNOVER_RATE should exist")
            .push(item.tor);
        for db_col in &compiled.database_indicator_columns {
            let normalized = db_col.to_ascii_uppercase();
            let value = item
                .runtime_values
                .get(&normalized)
                .copied()
                .flatten()
                .or_else(|| indicator_value_by_normalized_key(item, &normalized));
            cols.entry(normalized).or_default().push(value);
        }

        prev_close = item.close;
    }

    let mut row_data = RowData { trade_dates, cols };
    inject_chart_indicator_extra_runtime_fields(&mut row_data, source_path, ts_code)?;
    Ok(row_data)
}

fn indicator_value_by_normalized_key(item: &DetailKlineRow, normalized_key: &str) -> Option<f64> {
    item.indicators.iter().find_map(|(key, value)| {
        if key.to_ascii_uppercase() == normalized_key {
            value.as_f64()
        } else {
            None
        }
    })
}

fn rerun_realtime_chart_indicators(
    source_conn: &Connection,
    source_path: &str,
    ts_code: &str,
    items: &mut [DetailKlineRow],
    realtime_pre_close: f64,
) -> Result<(), String> {
    if items.is_empty() || items.last().and_then(|row| row.is_realtime) != Some(true) {
        return Ok(());
    }

    let db_columns = load_stock_data_columns(source_conn)?;
    let compiled = load_compiled_chart_indicator_config(source_path, Some(&db_columns))?;
    let mut row_data = build_chart_indicator_row_data_from_items(
        items,
        source_path,
        ts_code,
        Some(realtime_pre_close),
        &compiled,
    )?;
    let ind_cache = cache_ind_build(source_path)?;
    if !ind_cache.is_empty() {
        for (name, series) in calc_inds_with_cache(&ind_cache, row_data.clone())? {
            row_data.cols.insert(name, series);
        }
    }
    let execution = execute_chart_indicator_config(&compiled, row_data)?;

    for item in items.iter_mut() {
        for panel in &compiled.panels {
            for series in &panel.series {
                item.indicators.remove(&series.key);
                for rule in &series.color_rules {
                    item.indicators.remove(&rule.when_key);
                }
            }
            for marker in &panel.markers {
                item.indicators.remove(&marker.when_key);
            }
            for tooltip in &panel.tooltips {
                item.indicators.remove(&tooltip.value_key);
            }
        }
    }
    apply_chart_indicator_execution(items, execution.values);
    Ok(())
}

fn build_realtime_kline_row(quote: &crate::crawler::SinaQuote) -> Option<DetailKlineRow> {
    let trade_date = normalize_quote_trade_date(&quote.date)?;
    let realtime_color_hint = match quote.change_pct {
        Some(value) if value > 0.0 => Some("up".to_string()),
        Some(value) if value < 0.0 => Some("down".to_string()),
        _ => Some("flat".to_string()),
    };

    Some(DetailKlineRow {
        trade_date,
        open: Some(quote.open),
        high: Some(quote.high),
        low: Some(quote.low),
        close: Some(quote.price),
        vol: Some(quote.vol),
        amount: Some(quote.amount),
        tor: None,
        is_realtime: Some(true),
        realtime_color_hint,
        indicators: HashMap::new(),
        runtime_values: HashMap::new(),
    })
}

fn merge_realtime_kline(
    mut kline: DetailKlinePayload,
    quote: &crate::crawler::SinaQuote,
) -> (DetailKlinePayload, bool) {
    let Some(items) = kline.items.as_mut() else {
        return (kline, false);
    };
    let Some(mut realtime_row) = build_realtime_kline_row(quote) else {
        return (kline, false);
    };

    if let Some(last_row) = items.last_mut() {
        if last_row.trade_date == realtime_row.trade_date {
            last_row.open = realtime_row.open;
            last_row.high = realtime_row.high;
            last_row.low = realtime_row.low;
            last_row.close = realtime_row.close;
            last_row.vol = realtime_row.vol;
            last_row.amount = realtime_row.amount;
            last_row.is_realtime = realtime_row.is_realtime;
            realtime_row.realtime_color_hint = None;
            last_row.realtime_color_hint = realtime_row.realtime_color_hint;
            return (kline, true);
        }
    }

    items.push(realtime_row);
    (kline, false)
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
            scene_name: rule.scene_name,
            explain: rule.explain,
            when: rule.when,
        })
        .collect())
}

fn load_scene_meta_list(source_path: &str) -> Result<Vec<SceneMeta>, String> {
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
        .scene
        .into_iter()
        .map(|scene| SceneMeta {
            scene_name: scene.name,
            direction: scene.direction.as_str().to_string(),
            observe_threshold: scene.observe_threshold,
            trigger_threshold: scene.trigger_threshold,
            confirm_threshold: scene.confirm_threshold,
            fail_threshold: scene.fail_threshold,
        })
        .collect())
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

fn load_detail_trigger_snapshot(
    conn: &Connection,
    ts_code: &str,
    effective_trade_date: &str,
) -> Result<DetailTriggerSnapshot, String> {
    let mut stmt = conn
        .prepare(
            r#"
            WITH current_rule AS (
                SELECT rule_name, rule_score
                FROM rule_details
                WHERE ts_code = ? AND trade_date = ?
            ),
            current_scene AS (
                SELECT scene_name, direction, stage, stage_score, risk_score, confirm_strength, risk_intensity, scene_rank
                FROM scene_details
                WHERE ts_code = ? AND trade_date = ?
            )
            SELECT
                'rule' AS item_type,
                cr.rule_name AS item_name,
                cr.rule_score,
                CAST(NULL AS VARCHAR) AS direction,
                CAST(NULL AS VARCHAR) AS stage,
                CAST(NULL AS DOUBLE) AS stage_score,
                CAST(NULL AS DOUBLE) AS risk_score,
                CAST(NULL AS DOUBLE) AS confirm_strength,
                CAST(NULL AS DOUBLE) AS risk_intensity,
                CAST(NULL AS BIGINT) AS scene_rank,
                CAST(NULL AS VARCHAR) AS hit_date
            FROM current_rule AS cr
            UNION ALL
            SELECT
                'scene' AS item_type,
                cs.scene_name AS item_name,
                CAST(NULL AS DOUBLE) AS rule_score,
                cs.direction,
                cs.stage,
                cs.stage_score,
                cs.risk_score,
                cs.confirm_strength,
                cs.risk_intensity,
                cs.scene_rank,
                CAST(NULL AS VARCHAR) AS hit_date
            FROM current_scene AS cs
            "#,
        )
        .map_err(|e| format!("预编译当前策略/场景合并查询失败: {e}"))?;
    let mut rows = stmt
        .query(params![
            ts_code,
            effective_trade_date,
            ts_code,
            effective_trade_date
        ])
        .map_err(|e| format!("执行当前策略/场景合并查询失败: {e}"))?;

    let mut snapshot = DetailTriggerSnapshot {
        trade_day_index_map: build_trade_day_index_map(conn)?,
        ..DetailTriggerSnapshot::default()
    };

    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取当前策略/场景合并查询失败: {e}"))?
    {
        let item_type: String = row
            .get(0)
            .map_err(|e| format!("读取 item_type 失败: {e}"))?;
        let item_name: String = row
            .get(1)
            .map_err(|e| format!("读取 item_name 失败: {e}"))?;

        match item_type.as_str() {
            "rule" => {
                let rule_score: Option<f64> = row
                    .get(2)
                    .map_err(|e| format!("读取 rule_score 失败: {e}"))?;
                if let Some(score) = rule_score {
                    snapshot.current_rule_state_map.insert(
                        item_name.clone(),
                        CurrentRuleState {
                            rule_score: score,
                            is_triggered: true,
                        },
                    );
                }
            }
            "scene" => {
                let direction: Option<String> = row
                    .get(3)
                    .map_err(|e| format!("读取 direction 失败: {e}"))?;
                let stage: Option<String> =
                    row.get(4).map_err(|e| format!("读取 stage 失败: {e}"))?;
                let stage_score: Option<f64> = row
                    .get(5)
                    .map_err(|e| format!("读取 stage_score 失败: {e}"))?;
                let risk_score: Option<f64> = row
                    .get(6)
                    .map_err(|e| format!("读取 risk_score 失败: {e}"))?;
                let confirm_strength: Option<f64> = row
                    .get(7)
                    .map_err(|e| format!("读取 confirm_strength 失败: {e}"))?;
                let risk_intensity: Option<f64> = row
                    .get(8)
                    .map_err(|e| format!("读取 risk_intensity 失败: {e}"))?;
                let scene_rank: Option<i64> = row
                    .get(9)
                    .map_err(|e| format!("读取 scene_rank 失败: {e}"))?;
                if direction.is_some()
                    || stage.is_some()
                    || stage_score.is_some()
                    || risk_score.is_some()
                    || confirm_strength.is_some()
                    || risk_intensity.is_some()
                    || scene_rank.is_some()
                {
                    snapshot.current_scene_state_map.insert(
                        item_name.clone(),
                        CurrentSceneState {
                            direction,
                            stage,
                            stage_score: stage_score.unwrap_or(0.0),
                            risk_score: risk_score.unwrap_or(0.0),
                            confirm_strength: confirm_strength.unwrap_or(0.0),
                            risk_intensity: risk_intensity.unwrap_or(0.0),
                            scene_rank,
                            is_triggered: true,
                        },
                    );
                }
            }
            _ => {}
        }
    }

    snapshot.latest_rule_hit_date_map =
        load_latest_rule_hit_date_map(conn, ts_code, effective_trade_date)?;
    snapshot.latest_scene_hit_date_map =
        load_latest_scene_hit_date_map(conn, ts_code, effective_trade_date)?;

    Ok(snapshot)
}

fn load_latest_rule_hit_date_map(
    conn: &Connection,
    ts_code: &str,
    effective_trade_date: &str,
) -> Result<HashMap<String, String>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT rule_name, MAX(trade_date) AS hit_date
            FROM rule_details
            WHERE ts_code = ? AND trade_date <= ?
            GROUP BY rule_name
            "#,
        )
        .map_err(|e| format!("预编译最近规则命中日期失败: {e}"))?;
    let mut rows = stmt
        .query(params![ts_code, effective_trade_date])
        .map_err(|e| format!("查询最近规则命中日期失败: {e}"))?;

    let mut out = HashMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取最近规则命中日期失败: {e}"))?
    {
        let rule_name: String = row
            .get(0)
            .map_err(|e| format!("读取 rule_name 失败: {e}"))?;
        let hit_date: String = row.get(1).map_err(|e| format!("读取 hit_date 失败: {e}"))?;
        out.insert(rule_name, hit_date);
    }

    Ok(out)
}

fn load_latest_scene_hit_date_map(
    conn: &Connection,
    ts_code: &str,
    effective_trade_date: &str,
) -> Result<HashMap<String, String>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT scene_name, MAX(trade_date) AS hit_date
            FROM scene_details
            WHERE ts_code = ? AND trade_date <= ?
            GROUP BY scene_name
            "#,
        )
        .map_err(|e| format!("预编译最近场景命中日期失败: {e}"))?;
    let mut rows = stmt
        .query(params![ts_code, effective_trade_date])
        .map_err(|e| format!("查询最近场景命中日期失败: {e}"))?;

    let mut out = HashMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取最近场景命中日期失败: {e}"))?
    {
        let scene_name: String = row
            .get(0)
            .map_err(|e| format!("读取 scene_name 失败: {e}"))?;
        let hit_date: String = row.get(1).map_err(|e| format!("读取 hit_date 失败: {e}"))?;
        out.insert(scene_name, hit_date);
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

fn build_strategy_triggers(
    source_path: &str,
    effective_trade_date: &str,
    snapshot: &DetailTriggerSnapshot,
) -> Result<DetailStrategyPayload, String> {
    let rule_meta_list = load_rule_meta_list(source_path)?;

    let mut triggered = Vec::new();
    let mut untriggered = Vec::new();

    for meta in rule_meta_list {
        let current_state = snapshot
            .current_rule_state_map
            .get(&meta.rule_name)
            .copied()
            .unwrap_or(CurrentRuleState {
                rule_score: 0.0,
                is_triggered: false,
            });
        let hit_date = snapshot
            .latest_rule_hit_date_map
            .get(&meta.rule_name)
            .cloned();
        let row = DetailStrategyTriggerRow {
            rule_name: meta.rule_name.clone(),
            scene_name: Some(meta.scene_name.clone()),
            rule_score: Some(current_state.rule_score),
            is_triggered: Some(current_state.is_triggered),
            hit_date: hit_date.clone(),
            lag: calc_lag(
                &snapshot.trade_day_index_map,
                effective_trade_date,
                hit_date.as_ref(),
            ),
            explain: Some(meta.explain),
            when: Some(meta.when),
        };

        if current_state.is_triggered {
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

fn build_scene_triggers(
    source_path: &str,
    effective_trade_date: &str,
    snapshot: &DetailTriggerSnapshot,
) -> Result<DetailScenePayload, String> {
    let scene_meta_list = load_scene_meta_list(source_path)?;

    let mut triggered = Vec::new();
    let mut untriggered = Vec::new();

    for meta in scene_meta_list {
        let current_state = snapshot
            .current_scene_state_map
            .get(&meta.scene_name)
            .cloned()
            .unwrap_or(CurrentSceneState {
                direction: None,
                stage: None,
                stage_score: 0.0,
                risk_score: 0.0,
                confirm_strength: 0.0,
                risk_intensity: 0.0,
                scene_rank: None,
                is_triggered: false,
            });
        let hit_date = snapshot
            .latest_scene_hit_date_map
            .get(&meta.scene_name)
            .cloned();
        let row = DetailSceneTriggerRow {
            scene_name: meta.scene_name.clone(),
            direction: current_state
                .direction
                .clone()
                .or_else(|| Some(meta.direction.clone())),
            stage: current_state.stage.clone(),
            stage_score: Some(current_state.stage_score),
            risk_score: Some(current_state.risk_score),
            confirm_strength: Some(current_state.confirm_strength),
            risk_intensity: Some(current_state.risk_intensity),
            scene_rank: current_state.scene_rank,
            hit_date: hit_date.clone(),
            lag: calc_lag(
                &snapshot.trade_day_index_map,
                effective_trade_date,
                hit_date.as_ref(),
            ),
            observe_threshold: Some(meta.observe_threshold),
            trigger_threshold: Some(meta.trigger_threshold),
            confirm_threshold: Some(meta.confirm_threshold),
            fail_threshold: Some(meta.fail_threshold),
        };

        if current_state.is_triggered {
            triggered.push(row);
        } else {
            untriggered.push(row);
        }
    }

    Ok(DetailScenePayload {
        triggered: Some(triggered),
        untriggered: Some(untriggered),
    })
}

pub fn get_stock_detail_page(
    source_path: String,
    trade_date: Option<String>,
    ts_code: String,
    chart_window_days: Option<u32>,
    prev_rank_days: Option<u32>,
) -> Result<StockDetailPageData, String> {
    let normalized_ts_code = normalize_ts_code(&ts_code);
    let source_conn = open_source_conn(&source_path)?;
    let result_conn = if result_db_path(&source_path).exists() {
        open_result_conn(&source_path).ok()
    } else {
        None
    };
    let requested_trade_date = trade_date
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let effective_trade_date = if let Some(value) = requested_trade_date {
        value
    } else if let Some(conn) = result_conn.as_ref() {
        resolve_trade_date(conn, None)
            .or_else(|_| query_latest_kline_trade_date(&source_conn, &normalized_ts_code))?
    } else {
        query_latest_kline_trade_date(&source_conn, &normalized_ts_code)?
    };

    let overview = match result_conn.as_ref() {
        Some(conn) => query_detail_overview(
            conn,
            &source_path,
            &effective_trade_date,
            &normalized_ts_code,
        )
        .unwrap_or_else(|_| {
            build_basic_detail_overview(&source_path, &effective_trade_date, &normalized_ts_code)
        }),
        None => {
            build_basic_detail_overview(&source_path, &effective_trade_date, &normalized_ts_code)
        }
    };

    let prev_ranks = match result_conn.as_ref() {
        Some(conn) => query_rank_history(
            conn,
            &normalized_ts_code,
            prev_rank_days
                .map(|value| value as usize)
                .filter(|value| *value > 0),
        )
        .unwrap_or_default(),
        None => Vec::new(),
    };
    let (stock_similarity, stock_similarity_error) = match result_conn.as_ref() {
        Some(conn) => match get_stock_similarity_page_with_conn(
            conn,
            &source_path,
            &effective_trade_date,
            &normalized_ts_code,
            Some(12),
        ) {
            Ok(data) => (Some(data), None),
            Err(error) => (None, Some(error)),
        },
        None => (None, None),
    };
    let kline = query_kline(
        &source_conn,
        &source_path,
        &normalized_ts_code,
        chart_window_days.unwrap_or(280) as usize,
        overview.name.clone(),
    )?;
    let trigger_snapshot = result_conn
        .as_ref()
        .and_then(|conn| {
            load_detail_trigger_snapshot(conn, &normalized_ts_code, &effective_trade_date).ok()
        })
        .unwrap_or_default();
    let strategy_triggers =
        build_strategy_triggers(&source_path, &effective_trade_date, &trigger_snapshot)?;
    let strategy_scenes =
        build_scene_triggers(&source_path, &effective_trade_date, &trigger_snapshot)?;

    Ok(StockDetailPageData {
        resolved_trade_date: Some(effective_trade_date),
        resolved_ts_code: Some(normalized_ts_code),
        overview: Some(overview),
        prev_ranks: Some(prev_ranks),
        stock_similarity,
        stock_similarity_error,
        kline: Some(kline),
        strategy_triggers: Some(strategy_triggers),
        strategy_scenes: Some(strategy_scenes),
    })
}

pub fn get_stock_detail_strategy_snapshot(
    source_path: String,
    trade_date: Option<String>,
    ts_code: String,
) -> Result<StockDetailStrategySnapshotData, String> {
    let normalized_ts_code = normalize_ts_code(&ts_code);
    let result_conn = open_result_conn(&source_path)?;
    let effective_trade_date = resolve_trade_date(&result_conn, trade_date)?;
    let trigger_snapshot =
        load_detail_trigger_snapshot(&result_conn, &normalized_ts_code, &effective_trade_date)?;
    let strategy_triggers =
        build_strategy_triggers(&source_path, &effective_trade_date, &trigger_snapshot)?;

    Ok(StockDetailStrategySnapshotData {
        resolved_trade_date: Some(effective_trade_date),
        resolved_ts_code: Some(normalized_ts_code),
        strategy_triggers: Some(strategy_triggers),
    })
}

pub fn get_stock_detail_cyq(
    source_path: String,
    ts_code: String,
) -> Result<StockDetailCyqData, String> {
    if source_path.trim().is_empty() {
        return Err("数据目录为空，请先到数据管理页确认当前目录".to_string());
    }
    if ts_code.trim().is_empty() {
        return Err("股票代码不能为空".to_string());
    }
    query_stock_detail_cyq(&source_path, &ts_code)
}

pub fn get_stock_detail_realtime(
    source_path: String,
    ts_code: String,
    chart_window_days: Option<u32>,
) -> Result<StockDetailRealtimeData, String> {
    let normalized_ts_code = normalize_ts_code(&ts_code);
    let (quote_map, fetch_meta) =
        fetch_realtime_quote_map(std::slice::from_ref(&normalized_ts_code))?;
    build_stock_detail_realtime_from_quote_map(
        source_path,
        normalized_ts_code,
        chart_window_days,
        quote_map,
        fetch_meta,
    )
}

pub fn build_stock_detail_realtime_from_quote_map(
    source_path: String,
    normalized_ts_code: String,
    chart_window_days: Option<u32>,
    quote_map: HashMap<String, crate::crawler::SinaQuote>,
    fetch_meta: RealtimeFetchMeta,
) -> Result<StockDetailRealtimeData, String> {
    let source_conn = open_source_conn(&source_path)?;
    let name_map = build_name_map(&source_path).unwrap_or_default();
    let watermark_name = name_map.get(&normalized_ts_code).cloned();
    let kline = query_kline(
        &source_conn,
        &source_path,
        &normalized_ts_code,
        chart_window_days.unwrap_or(280) as usize,
        watermark_name,
    )?;
    let quote = quote_map
        .get(&normalized_ts_code)
        .ok_or_else(|| format!("未获取到 {} 的实时行情", normalized_ts_code))?;
    let (mut kline, has_database_trade_date) = merge_realtime_kline(kline, quote);
    if let Some(items) = kline.items.as_mut() {
        rerun_realtime_chart_indicators(
            &source_conn,
            &source_path,
            &normalized_ts_code,
            items,
            quote.pre_close,
        )?;
    }

    Ok(StockDetailRealtimeData {
        ts_code: normalized_ts_code,
        refreshed_at: fetch_meta.refreshed_at,
        quote_trade_date: fetch_meta.quote_trade_date,
        quote_time: fetch_meta.quote_time,
        has_database_trade_date,
        kline,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use duckdb::Connection;

    use super::*;

    #[test]
    fn default_kline_panels_only_contains_price_panel() {
        let panels = default_kline_panels();
        assert_eq!(panels.len(), 1);

        let price_panel = panels
            .iter()
            .find(|panel| panel.key == "price")
            .expect("missing price panel");
        assert_eq!(price_panel.role.as_deref(), Some("main"));
        assert!(
            price_panel
                .series
                .as_ref()
                .is_some_and(|series| series.is_empty())
        );
    }

    #[test]
    fn query_kline_default_config_runs_chart_indicator_runtime() {
        let conn = build_test_stock_data_conn();
        let source_path = unique_temp_dir("details_chart_default");
        fs::create_dir_all(&source_path).expect("temp dir should be created");

        let payload = query_kline(
            &conn,
            source_path.to_str().expect("temp path should be utf8"),
            "000001.SZ",
            280,
            Some("测试股".to_string()),
        )
        .expect("kline should query");

        let items = payload.items.expect("items should exist");
        assert_eq!(items.len(), 2);
        assert!(items.iter().all(|item| item.indicators.is_empty()));

        let panels = payload.panels.expect("panels should exist");
        assert_eq!(panels.len(), 1);
        assert_eq!(panels[0].key, "price");
        assert!(
            panels[0]
                .series
                .as_ref()
                .is_some_and(|series| series.is_empty())
        );

        fs::remove_dir_all(source_path).ok();
    }

    #[test]
    fn query_kline_external_config_overrides_default_chart_indicators() {
        let conn = build_test_stock_data_conn();
        let source_path = unique_temp_dir("details_chart_external");
        fs::create_dir_all(&source_path).expect("temp dir should be created");
        write_ma2_chart_config(&source_path);

        let payload = query_kline(
            &conn,
            source_path.to_str().expect("temp path should be utf8"),
            "000001.SZ",
            280,
            None,
        )
        .expect("kline should query");

        let items = payload.items.expect("items should exist");
        assert_eq!(
            items[0].indicators.get("ma2"),
            Some(&serde_json::Value::Null)
        );
        assert_eq!(
            items[1].indicators.get("ma2"),
            Some(&serde_json::json!(10.5))
        );

        let panels = payload.panels.expect("panels should exist");
        assert_eq!(panels.len(), 1);
        assert_eq!(
            panels[0].series.as_ref().map(|series| series
                .iter()
                .map(|row| row.key.as_str())
                .collect::<Vec<_>>()),
            Some(vec!["ma2"])
        );

        fs::remove_dir_all(source_path).ok();
    }

    #[test]
    fn realtime_chart_indicators_rerun_same_runtime_after_append() {
        let conn = build_test_stock_data_conn();
        let source_path = unique_temp_dir("details_chart_realtime");
        fs::create_dir_all(&source_path).expect("temp dir should be created");
        write_ma2_chart_config(&source_path);

        let payload = query_kline(
            &conn,
            source_path.to_str().expect("temp path should be utf8"),
            "000001.SZ",
            280,
            None,
        )
        .expect("kline should query");
        let quote = crate::crawler::SinaQuote {
            date: "2024-01-03".to_string(),
            time: "10:00:00".to_string(),
            ts_code: "000001.SZ".to_string(),
            name: "测试股".to_string(),
            open: 11.0,
            high: 13.2,
            low: 10.8,
            pre_close: 11.0,
            price: 13.0,
            vol: 150.0,
            amount: 1500.0,
            change_pct: Some(18.18),
        };
        let (mut payload, has_database_trade_date) = merge_realtime_kline(payload, &quote);
        assert!(!has_database_trade_date);
        let items = payload.items.as_mut().expect("items should exist");

        rerun_realtime_chart_indicators(
            &conn,
            source_path.to_str().expect("temp path should be utf8"),
            "000001.SZ",
            items,
            quote.pre_close,
        )
        .expect("realtime chart indicators should rerun");

        let latest = items.last().expect("latest row should exist");
        assert_eq!(latest.trade_date, "20240103");
        assert_eq!(latest.indicators.get("ma2"), Some(&serde_json::json!(12.0)));

        fs::remove_dir_all(source_path).ok();
    }

    #[test]
    fn realtime_chart_indicators_can_use_ind_toml_outputs() {
        let conn = build_test_stock_data_conn();
        let source_path = unique_temp_dir("details_chart_realtime_ind_toml");
        fs::create_dir_all(&source_path).expect("temp dir should be created");
        write_ind_j_config(&source_path);
        write_j_chart_config(&source_path);

        let payload = query_kline(
            &conn,
            source_path.to_str().expect("temp path should be utf8"),
            "000001.SZ",
            280,
            None,
        )
        .expect("kline should query");
        let quote = crate::crawler::SinaQuote {
            date: "2024-01-03".to_string(),
            time: "10:00:00".to_string(),
            ts_code: "000001.SZ".to_string(),
            name: "测试股".to_string(),
            open: 11.0,
            high: 13.2,
            low: 10.8,
            pre_close: 11.0,
            price: 13.0,
            vol: 150.0,
            amount: 1500.0,
            change_pct: Some(18.18),
        };
        let (mut payload, has_database_trade_date) = merge_realtime_kline(payload, &quote);
        assert!(!has_database_trade_date);
        let items = payload.items.as_mut().expect("items should exist");

        rerun_realtime_chart_indicators(
            &conn,
            source_path.to_str().expect("temp path should be utf8"),
            "000001.SZ",
            items,
            quote.pre_close,
        )
        .expect("realtime chart indicators should rerun");

        let latest = items.last().expect("latest row should exist");
        assert_eq!(latest.trade_date, "20240103");
        assert_eq!(
            latest.indicators.get("j_line"),
            Some(&serde_json::json!(12.0))
        );

        fs::remove_dir_all(source_path).ok();
    }

    fn write_ma2_chart_config(source_path: &std::path::Path) {
        fs::write(
            source_path.join("chart_indicators.toml"),
            r##"
version = 1

[[panel]]
key = "price"
label = "Price"
role = "main"
kind = "candles"

[[panel.series]]
key = "ma2"
label = "MA2"
expr = "MA(C, 2)"
kind = "line"
"##,
        )
        .expect("config should be written");
    }

    fn write_ind_j_config(source_path: &std::path::Path) {
        fs::write(
            source_path.join("ind.toml"),
            r#"
version = 1

[[ind]]
name = "J"
expr = "MA(C, 2)"
prec = 2
"#,
        )
        .expect("ind config should be written");
    }

    fn write_j_chart_config(source_path: &std::path::Path) {
        fs::write(
            source_path.join("chart_indicators.toml"),
            r##"
version = 1

[[panel]]
key = "price"
label = "Price"
role = "main"
kind = "candles"

[[panel.series]]
key = "j_line"
label = "J"
expr = "J"
kind = "line"
"##,
        )
        .expect("config should be written");
    }

    fn build_test_stock_data_conn() -> Connection {
        let conn = Connection::open_in_memory().expect("in-memory db should open");
        conn.execute_batch(
            r#"
            CREATE TABLE stock_data (
                ts_code TEXT,
                adj_type TEXT,
                trade_date TEXT,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                vol DOUBLE,
                amount DOUBLE,
                pre_close DOUBLE,
                change DOUBLE,
                pct_chg DOUBLE,
                tor DOUBLE,
                brick DOUBLE,
                j DOUBLE,
                duokong_short DOUBLE,
                duokong_long DOUBLE,
                bupiao_short DOUBLE,
                bupiao_long DOUBLE,
                VOL_SIGMA DOUBLE
            );
            INSERT INTO stock_data VALUES
                ('000001.SZ', 'qfq', '20240101', 9.0, 10.5, 8.8, 10.0, 100.0, 1000.0, 9.5, 0.5, 5.0, 1.1, 1.0, 20.0, 9.5, 9.0, 8.5, 8.0, 0.5),
                ('000001.SZ', 'qfq', '20240102', 10.0, 11.5, 9.8, 11.0, 120.0, 1200.0, 10.0, 1.0, 10.0, 1.2, 2.0, 30.0, 10.5, 9.8, 8.8, 8.2, 0.7);
            "#,
        )
        .expect("test table should be created");
        conn
    }

    fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}_{nanos}"))
    }
}
