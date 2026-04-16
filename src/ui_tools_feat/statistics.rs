use std::collections::{HashMap, HashSet};

use duckdb::{Connection, params};
use rand::{Rng, SeedableRng, rngs::StdRng};
use serde::{Deserialize, Serialize};

use crate::{
    data::scoring_data::row_into_rt,
    data::{
        DataReader, RowData, RuleStage, RuleTag, ScopeWay, ScoreRule, ScoreScene,
        concept_performance_db_path, load_stock_list, load_ths_concepts_list, result_db_path,
        source_db_path,
    },
    expr::{
        lexer::TokenKind,
        parser::{Expr, Parser, Stmt, Stmts, lex_all},
    },
    scoring::tools::{calc_zhang_pct, load_st_list},
    scoring::{CachedRule, evaluate_cached_rule_scores},
    simulate::{
        rule::{
            RuleLayerConfig, RuleLayerFromDbInput, calc_all_rule_layer_metrics_from_db,
            calc_rule_layer_metrics_from_db,
            calc_rule_layer_metrics_with_samples_from_triggered_scores,
        },
        scene::{
            SceneLayerConfig, SceneLayerFromDbInput, calc_all_scene_layer_metrics_from_db,
            calc_scene_layer_metrics_from_db,
        },
    },
    ui_tools_feat::{build_concepts_map, build_name_map},
    utils::utils::board_category,
    utils::utils::{eval_binary_for_warmup, impl_expr_warmup},
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
    pub industry_beta: f64,
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
pub struct RuleLayerPointPayload {
    pub trade_date: String,
    pub sample_count: usize,
    pub avg_rule_score: Option<f64>,
    pub avg_residual_return: Option<f64>,
    pub top_bottom_spread: Option<f64>,
    pub ic: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct RuleLayerRuleSummary {
    pub rule_name: String,
    pub point_count: usize,
    pub avg_residual_mean: Option<f64>,
    pub spread_mean: Option<f64>,
    pub ic_mean: Option<f64>,
    pub ic_std: Option<f64>,
    pub icir: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct RuleLayerBacktestData {
    pub rule_name: String,
    pub stock_adj_type: String,
    pub index_ts_code: String,
    pub index_beta: f64,
    pub concept_beta: f64,
    pub industry_beta: f64,
    pub start_date: String,
    pub end_date: String,
    pub min_samples_per_rule_day: usize,
    pub backtest_period: usize,
    pub points: Vec<RuleLayerPointPayload>,
    pub avg_residual_mean: Option<f64>,
    pub spread_mean: Option<f64>,
    pub ic_mean: Option<f64>,
    pub ic_std: Option<f64>,
    pub icir: Option<f64>,
    pub is_all_rules: bool,
    pub all_rule_summaries: Vec<RuleLayerRuleSummary>,
}

#[derive(Debug, Serialize)]
pub struct RuleLayerBacktestDefaultsData {
    pub rule_options: Vec<String>,
    pub resolved_rule_name: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
}

const VALIDATION_EPS: f64 = 1e-12;
const VALIDATION_MAX_COMBINATIONS: usize = 256;
const VALIDATION_DEFAULT_SAMPLE_LIMIT_PER_GROUP: usize = 30;
const VALIDATION_MAX_SAMPLE_LIMIT_PER_GROUP: usize = 200;
const VALIDATION_RANDOM_SAMPLE_SEED: u64 = 0x9E37_79B9_7F4A_7C15;

#[derive(Debug, Clone, Deserialize)]
pub struct RuleValidationUnknownConfig {
    pub name: String,
    pub start: f64,
    pub end: f64,
    pub step: f64,
}

#[derive(Debug, Serialize)]
pub struct RuleValidationUnknownValue {
    pub name: String,
    pub value: f64,
}

#[derive(Debug, Serialize)]
pub struct RuleValidationSimilarityRow {
    pub rule_name: String,
    pub explain: Option<String>,
    pub overlap_samples: usize,
    pub overlap_rate_vs_validation: Option<f64>,
    pub overlap_rate_vs_existing: Option<f64>,
    pub overlap_lift: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct RuleValidationSampleStats {
    pub positive_count: usize,
    pub negative_count: usize,
    pub random_count: usize,
    pub total_samples: usize,
}

#[derive(Debug, Serialize)]
pub struct RuleValidationSampleRow {
    pub ts_code: String,
    pub name: Option<String>,
    pub trade_date: String,
    pub rule_score: f64,
    pub residual_return: f64,
}

#[derive(Debug, Serialize)]
pub struct RuleValidationSampleGroups {
    pub positive: Vec<RuleValidationSampleRow>,
    pub negative: Vec<RuleValidationSampleRow>,
    pub random: Vec<RuleValidationSampleRow>,
}

#[derive(Debug, Serialize)]
pub struct RuleValidationComboResult {
    pub combo_key: String,
    pub combo_label: String,
    pub formula: String,
    pub unknown_values: Vec<RuleValidationUnknownValue>,
    pub trigger_samples: usize,
    pub triggered_days: usize,
    pub avg_daily_trigger: f64,
    pub sample_stats: RuleValidationSampleStats,
    pub sample_groups: RuleValidationSampleGroups,
    pub backtest: RuleLayerBacktestData,
    pub similarity_rows: Vec<RuleValidationSimilarityRow>,
}

#[derive(Debug, Serialize)]
pub struct RuleExpressionValidationData {
    pub import_rule_name: String,
    pub import_rule_explain: String,
    pub scope_way: String,
    pub scope_windows: usize,
    pub sample_limit_per_group: usize,
    pub combo_results: Vec<RuleValidationComboResult>,
    pub best_combo_key: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuleExpressionValidationManualStrategy {
    pub name: Option<String>,
    pub scene_name: Option<String>,
    pub stage: Option<String>,
    pub scope_way: Option<String>,
    pub scope_windows: Option<usize>,
    pub when: Option<String>,
    pub points: Option<f64>,
    pub dist_points: Option<Vec<crate::data::DistPoint>>,
    pub explain: Option<String>,
    pub tag: Option<String>,
}

#[derive(Debug)]
struct ValidationVariant {
    combo_key: String,
    combo_label: String,
    formula: String,
    unknown_values: Vec<RuleValidationUnknownValue>,
}

#[derive(Debug, Clone)]
struct ValidationSeedRule {
    rule_name: String,
    rule_explain: String,
    scope_way: ScopeWay,
    scope_windows: usize,
    formula: String,
    points: f64,
    dist_points: Option<Vec<crate::data::DistPoint>>,
    tag: RuleTag,
    exclude_rule_name: Option<String>,
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

fn parse_scope_way_input(scope_way_raw: &str) -> Result<ScopeWay, String> {
    let normalized = scope_way_raw.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "ANY" => Ok(ScopeWay::Any),
        "LAST" => Ok(ScopeWay::Last),
        "EACH" => Ok(ScopeWay::Each),
        "RECENT" => Ok(ScopeWay::Recent),
        value => {
            let Some(num) = value.strip_prefix("CONSEC>=") else {
                return Err(format!(
                    "scope_way 不支持: {scope_way_raw}，仅支持 ANY/LAST/EACH/RECENT/CONSEC>=N"
                ));
            };
            let threshold = num
                .parse::<usize>()
                .map_err(|_| format!("scope_way 连续阈值非法: {scope_way_raw}"))?;
            if threshold == 0 {
                return Err("scope_way 连续阈值必须 >= 1".to_string());
            }
            Ok(ScopeWay::Consec(threshold))
        }
    }
}

fn parse_rule_stage_input(stage_raw: &str) -> Result<RuleStage, String> {
    match stage_raw.trim().to_ascii_lowercase().as_str() {
        "base" => Ok(RuleStage::Base),
        "trigger" => Ok(RuleStage::Trigger),
        "confirm" => Ok(RuleStage::Confirm),
        "risk" => Ok(RuleStage::Risk),
        "fail" => Ok(RuleStage::Fail),
        _ => Err(format!(
            "stage 不支持: {stage_raw}，仅支持 base/trigger/confirm/risk/fail"
        )),
    }
}

fn parse_rule_tag_input(tag_raw: &str) -> Result<RuleTag, String> {
    match tag_raw.trim().to_ascii_lowercase().as_str() {
        "" | "normal" => Ok(RuleTag::Normal),
        "opportunity" => Ok(RuleTag::Opportunity),
        "rare" => Ok(RuleTag::Rare),
        _ => Err(format!(
            "tag 不支持: {tag_raw}，仅支持 normal/opportunity/rare"
        )),
    }
}

fn read_non_empty_owned(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn resolve_validation_seed_rule(
    import_rule_name_raw: &str,
    manual_strategy: Option<&RuleExpressionValidationManualStrategy>,
    when: Option<&str>,
    scope_way: Option<&str>,
    scope_windows: Option<usize>,
    all_rules: &[ScoreRule],
) -> Result<ValidationSeedRule, String> {
    let import_rule_name = import_rule_name_raw.trim();
    let import_rule = if import_rule_name.is_empty() {
        None
    } else {
        all_rules
            .iter()
            .find(|rule| rule.name.trim() == import_rule_name)
            .cloned()
    };

    let top_formula = read_non_empty_owned(when);
    let top_scope_way = read_non_empty_owned(scope_way);

    let manual_name =
        manual_strategy.and_then(|strategy| read_non_empty_owned(strategy.name.as_deref()));
    let manual_formula =
        manual_strategy.and_then(|strategy| read_non_empty_owned(strategy.when.as_deref()));
    let manual_explain =
        manual_strategy.and_then(|strategy| read_non_empty_owned(strategy.explain.as_deref()));
    let manual_scope_windows = manual_strategy.and_then(|strategy| strategy.scope_windows);
    let manual_points_raw = manual_strategy.and_then(|strategy| strategy.points);
    let manual_points = manual_points_raw.filter(|value| value.is_finite());
    if manual_points_raw.is_some() && manual_points.is_none() {
        return Err("手动策略 points 非法".to_string());
    }
    let manual_dist_points = manual_strategy
        .and_then(|strategy| strategy.dist_points.clone())
        .and_then(|items| if items.is_empty() { None } else { Some(items) });

    let manual_scope_way = match manual_strategy
        .and_then(|strategy| read_non_empty_owned(strategy.scope_way.as_deref()))
    {
        Some(raw) => Some(parse_scope_way_input(&raw)?),
        None => None,
    };

    let manual_tag = match manual_strategy.and_then(|strategy| strategy.tag.as_deref()) {
        Some(raw) if !raw.trim().is_empty() => Some(parse_rule_tag_input(raw)?),
        _ => None,
    };

    if let Some(stage_raw) = manual_strategy.and_then(|strategy| strategy.stage.as_deref()) {
        if !stage_raw.trim().is_empty() {
            let _ = parse_rule_stage_input(stage_raw)?;
        }
    }

    let has_manual_override = manual_name.is_some()
        || manual_formula.is_some()
        || manual_scope_way.is_some()
        || manual_scope_windows.is_some()
        || manual_points.is_some()
        || manual_dist_points.is_some()
        || manual_explain.is_some()
        || manual_tag.is_some()
        || manual_strategy
            .and_then(|strategy| strategy.scene_name.as_deref())
            .map(str::trim)
            .is_some_and(|value| !value.is_empty())
        || manual_strategy
            .and_then(|strategy| strategy.stage.as_deref())
            .map(str::trim)
            .is_some_and(|value| !value.is_empty());

    if !import_rule_name.is_empty() && import_rule.is_none() && !has_manual_override {
        return Err(format!("未找到策略: {import_rule_name}"));
    }

    let formula = top_formula
        .or(manual_formula)
        .or_else(|| {
            import_rule
                .as_ref()
                .map(|rule| rule.when.trim().to_string())
        })
        .ok_or_else(|| "表达式不能为空".to_string())?;

    let resolved_scope_way = if let Some(raw) = top_scope_way {
        parse_scope_way_input(&raw)?
    } else if let Some(value) = manual_scope_way {
        value
    } else if let Some(rule) = import_rule.as_ref() {
        rule.scope_way
    } else {
        ScopeWay::Any
    };

    let resolved_scope_windows = scope_windows
        .or(manual_scope_windows)
        .or_else(|| import_rule.as_ref().map(|rule| rule.scope_windows))
        .unwrap_or(1)
        .max(1);

    if let ScopeWay::Consec(threshold) = resolved_scope_way {
        if resolved_scope_windows < threshold {
            return Err(format!(
                "scope_windows({resolved_scope_windows}) 不能小于 CONSEC 阈值 {threshold}"
            ));
        }
    }

    let rule_name = manual_name
        .or_else(|| {
            import_rule
                .as_ref()
                .map(|rule| rule.name.trim().to_string())
        })
        .or_else(|| read_non_empty_owned(Some(import_rule_name)))
        .unwrap_or_else(|| "manual_validation_rule".to_string());

    let rule_explain = manual_explain
        .or_else(|| {
            import_rule
                .as_ref()
                .map(|rule| rule.explain.trim().to_string())
        })
        .unwrap_or_else(|| format!("表达式验证策略: {rule_name}"));

    let points = manual_points
        .or_else(|| import_rule.as_ref().map(|rule| rule.points))
        .unwrap_or(1.0);
    if !points.is_finite() {
        return Err("策略 points 非法".to_string());
    }

    let dist_points = manual_dist_points.or_else(|| {
        import_rule
            .as_ref()
            .and_then(|rule| rule.dist_points.clone())
    });

    let tag = manual_tag
        .or_else(|| import_rule.as_ref().map(|rule| rule.tag))
        .unwrap_or(RuleTag::Normal);

    let exclude_rule_name = if let Some(rule) = import_rule.as_ref() {
        Some(rule.name.clone())
    } else if all_rules.iter().any(|rule| rule.name.trim() == rule_name) {
        Some(rule_name.clone())
    } else {
        None
    };

    Ok(ValidationSeedRule {
        rule_name,
        rule_explain,
        scope_way: resolved_scope_way,
        scope_windows: resolved_scope_windows,
        formula,
        points,
        dist_points,
        tag,
        exclude_rule_name,
    })
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

fn load_scene_rule_name_sets(
    source_path: &str,
) -> Result<HashMap<String, HashSet<String>>, String> {
    let rules = ScoreRule::load_rules(source_path)?;
    let mut out: HashMap<String, HashSet<String>> = HashMap::new();

    for rule in rules {
        out.entry(rule.scene_name).or_default().insert(rule.name);
    }

    Ok(out)
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

fn resolve_scene_name(requested: Option<String>, scene_options: &[String]) -> Option<String> {
    let requested = requested
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if let Some(scene_name) = requested {
        if scene_options.iter().any(|item| item == &scene_name) {
            return Some(scene_name);
        }
    }
    scene_options.first().cloned()
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

fn query_scene_trade_date_options(conn: &Connection) -> Result<Vec<String>, String> {
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
}

fn query_scene_stage_rows(
    conn: &Connection,
    scene_name: &str,
    trade_date: &str,
) -> Result<(Vec<SceneStageRow>, i64, i64), String> {
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
        let sample_count: i64 = row.get(1).map_err(|e| format!("读取阶段数量失败: {e}"))?;
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
}

fn query_rule_contribution_by_date(
    conn: &Connection,
    trade_date: &str,
) -> Result<HashMap<String, f64>, String> {
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
}

fn build_scene_contribution_summary(
    scene_total_sample_count: i64,
    scene_covered_count: i64,
    scene_rule_names: Option<&HashSet<String>>,
    contribution_by_rule: &HashMap<String, f64>,
) -> SceneContributionSummary {
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
    let resolved_scene_name = resolve_scene_name(scene_name, &scene_options);
    let analysis_trade_date_options = query_scene_trade_date_options(&conn)?;
    let resolved_analysis_trade_date =
        resolve_analysis_trade_date(analysis_trade_date, &analysis_trade_date_options);

    let mut stage_rows = Vec::new();
    let mut summary = None;

    if let (Some(selected_scene_name), Some(selected_trade_date)) = (
        resolved_scene_name.as_deref(),
        resolved_analysis_trade_date.as_deref(),
    ) {
        let (next_stage_rows, total_sample_count, covered_count) =
            query_scene_stage_rows(&conn, selected_scene_name, selected_trade_date)?;
        stage_rows = next_stage_rows;

        let scene_rule_name_sets = load_scene_rule_name_sets(&source_path)?;
        let contribution_by_rule = query_rule_contribution_by_date(&conn, selected_trade_date)?;
        summary = Some(build_scene_contribution_summary(
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

fn format_validation_number(value: f64) -> String {
    let rounded = value.round();
    if (value - rounded).abs() < 1e-9 {
        format!("{rounded:.0}")
    } else {
        let mut text = format!("{value:.6}");
        while text.contains('.') && text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.pop();
        }
        text
    }
}

fn expand_unknown_config(config: &RuleValidationUnknownConfig) -> Result<Vec<f64>, String> {
    let name = config.name.trim();
    if name.is_empty() {
        return Err("未知数名称不能为空".to_string());
    }
    if !config.start.is_finite() || !config.end.is_finite() || !config.step.is_finite() {
        return Err(format!("未知数 {name} 存在非法数值"));
    }
    if config.step <= 0.0 {
        return Err(format!("未知数 {name} 的 step 必须 > 0"));
    }
    if config.end < config.start {
        return Err(format!("未知数 {name} 的 end 不能小于 start"));
    }

    let mut values = Vec::new();
    let mut current = config.start;
    let mut guard = 0usize;
    while current <= config.end + config.step * 1e-9 {
        values.push(current.min(config.end));
        current += config.step;
        guard += 1;
        if guard > VALIDATION_MAX_COMBINATIONS * 8 {
            return Err(format!(
                "未知数 {name} 的取值数量过多，请增大 step 或缩小范围"
            ));
        }
    }
    if values.is_empty() {
        values.push(config.start);
    }
    Ok(values)
}

fn replace_validation_unknowns(formula: &str, assignments: &[(String, f64)]) -> String {
    if assignments.is_empty() {
        return formula.to_string();
    }

    let replace_map = assignments
        .iter()
        .map(|(name, value)| (name.as_str(), format_validation_number(*value)))
        .collect::<HashMap<_, _>>();

    let tokens = lex_all(formula);
    let mut out = String::with_capacity(formula.len() + assignments.len() * 4);
    let mut cursor = 0usize;

    for token in tokens {
        if token.start > cursor {
            out.push_str(&formula[cursor..token.start]);
        }
        match token.kind {
            TokenKind::Ident(name) => {
                if let Some(replacement) = replace_map.get(name.as_str()) {
                    out.push_str(replacement);
                } else {
                    out.push_str(&formula[token.start..token.end]);
                }
            }
            TokenKind::Eof => {}
            _ => out.push_str(&formula[token.start..token.end]),
        }
        cursor = token.end;
    }

    if cursor < formula.len() {
        out.push_str(&formula[cursor..]);
    }

    out
}

fn build_validation_variants(
    formula: &str,
    unknown_configs: &[RuleValidationUnknownConfig],
) -> Result<Vec<ValidationVariant>, String> {
    let formula = formula.trim();
    if formula.is_empty() {
        return Err("表达式不能为空".to_string());
    }

    let mut unknown_groups = Vec::<(String, Vec<f64>)>::new();
    let mut total_combinations = 1usize;
    let mut seen = HashSet::new();

    for config in unknown_configs {
        let name = config.name.trim();
        if name.is_empty() {
            continue;
        }
        if !seen.insert(name.to_string()) {
            return Err(format!("未知数名称重复: {name}"));
        }

        let values = expand_unknown_config(config)?;
        total_combinations = total_combinations.saturating_mul(values.len().max(1));
        if total_combinations > VALIDATION_MAX_COMBINATIONS {
            return Err(format!(
                "未知数组合过多({total_combinations})，当前上限为 {VALIDATION_MAX_COMBINATIONS}"
            ));
        }

        unknown_groups.push((name.to_string(), values));
    }

    let mut out = Vec::new();
    let mut assignments = Vec::<(String, f64)>::new();

    fn walk_variants(
        index: usize,
        unknown_groups: &[(String, Vec<f64>)],
        assignments: &mut Vec<(String, f64)>,
        formula: &str,
        out: &mut Vec<ValidationVariant>,
    ) {
        if index >= unknown_groups.len() {
            let mut sorted = assignments.clone();
            sorted.sort_by(|left, right| {
                right
                    .0
                    .len()
                    .cmp(&left.0.len())
                    .then_with(|| left.0.cmp(&right.0))
            });
            let unknown_values = sorted
                .iter()
                .map(|(name, value)| RuleValidationUnknownValue {
                    name: name.clone(),
                    value: *value,
                })
                .collect::<Vec<_>>();
            let replaced_formula = replace_validation_unknowns(formula, &sorted);
            let combo_key = format!("validation_combo_{:03}", out.len() + 1);
            let combo_label = if unknown_values.is_empty() {
                "默认参数".to_string()
            } else {
                unknown_values
                    .iter()
                    .map(|item| format!("{}={}", item.name, format_validation_number(item.value)))
                    .collect::<Vec<_>>()
                    .join(", ")
            };

            out.push(ValidationVariant {
                combo_key,
                combo_label,
                formula: replaced_formula,
                unknown_values,
            });
            return;
        }

        let (name, values) = &unknown_groups[index];
        for value in values {
            assignments.push((name.clone(), *value));
            walk_variants(index + 1, unknown_groups, assignments, formula, out);
            assignments.pop();
        }
    }

    walk_variants(0, &unknown_groups, &mut assignments, formula, &mut out);

    if out.is_empty() {
        out.push(ValidationVariant {
            combo_key: "validation_combo_001".to_string(),
            combo_label: "默认参数".to_string(),
            formula: formula.to_string(),
            unknown_values: Vec::new(),
        });
    }

    Ok(out)
}

fn estimate_rule_warmup(
    stmts: &Stmts,
    scope_way: ScopeWay,
    scope_windows: usize,
) -> Result<usize, String> {
    let mut locals = std::collections::HashMap::new();
    let mut consts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut expr_need = 0usize;

    for stmt in stmts.item.clone() {
        match stmt {
            Stmt::Assign { name, value } => match value {
                Expr::Number(v) => {
                    if v < 0.0 {
                        return Err("表达式常量赋值结果不能为负数".to_string());
                    }
                    consts.insert(name, v as usize);
                }
                Expr::Binary { op, lhs, rhs } => {
                    if let Some(out) = eval_binary_for_warmup(&op, &lhs, &rhs, &consts)? {
                        consts.insert(name, out as usize);
                    } else {
                        let need =
                            impl_expr_warmup(Expr::Binary { op, lhs, rhs }, &locals, &consts)?;
                        locals.insert(name, need);
                    }
                }
                other => {
                    let need = impl_expr_warmup(other, &locals, &consts)?;
                    locals.insert(name, need);
                }
            },
            Stmt::Expr(expr) => {
                expr_need = expr_need.max(impl_expr_warmup(expr, &locals, &consts)?);
            }
        }
    }

    let scope_extra = match scope_way {
        ScopeWay::Last => 0,
        ScopeWay::Any | ScopeWay::Each | ScopeWay::Recent => scope_windows.saturating_sub(1),
        ScopeWay::Consec(threshold) => scope_windows
            .saturating_sub(1)
            .max(threshold.saturating_sub(1)),
    };

    Ok(expr_need + scope_extra)
}

fn fill_validation_extra_fields(
    row_data: &mut RowData,
    ts_code: &str,
    is_st: bool,
) -> Result<(), String> {
    let zhang = calc_zhang_pct(ts_code, is_st);
    let zhang_series = vec![Some(zhang); row_data.trade_dates.len()];
    row_data.cols.insert("ZHANG".to_string(), zhang_series);
    row_data.validate()
}

fn build_validation_cached_rule(
    rule_name: String,
    scope_way: ScopeWay,
    scope_windows: usize,
    points: f64,
    dist_points: Option<Vec<crate::data::DistPoint>>,
    tag: crate::data::RuleTag,
    formula: &str,
) -> Result<CachedRule, String> {
    let tokens = lex_all(formula);
    let mut parser = Parser::new(tokens);
    let stmts = parser
        .parse_main()
        .map_err(|e| format!("表达式解析错误在{}:{}", e.idx, e.msg))?;

    Ok(CachedRule {
        name: rule_name,
        scope_windows,
        scope_way,
        points,
        dist_points,
        tag,
        when_src: formula.to_string(),
        when_ast: stmts,
    })
}

fn build_validation_triggered_scores(
    source_path: &str,
    stock_adj_type: &str,
    start_date: &str,
    end_date: &str,
    cached_rule: &CachedRule,
) -> Result<
    (
        HashMap<String, HashMap<String, f64>>,
        HashSet<(String, String)>,
    ),
    String,
> {
    let reader = DataReader::new(source_path)?;
    let ts_codes = reader.list_ts_code(stock_adj_type, start_date, end_date)?;
    let st_list = load_st_list(source_path)?;
    let warmup_need = estimate_rule_warmup(
        &cached_rule.when_ast,
        cached_rule.scope_way,
        cached_rule.scope_windows,
    )?;
    let need_rows = (warmup_need + cached_rule.scope_windows).max(1);

    let mut triggered_score_map: HashMap<String, HashMap<String, f64>> = HashMap::new();
    let mut hit_pairs: HashSet<(String, String)> = HashSet::new();

    for ts_code in ts_codes {
        let mut row_data =
            reader.load_one_tail_rows(&ts_code, stock_adj_type, end_date, need_rows)?;
        fill_validation_extra_fields(&mut row_data, &ts_code, st_list.contains(&ts_code))?;

        let trade_dates = row_data.trade_dates.clone();
        if trade_dates.is_empty() {
            continue;
        }

        let mut rt = row_into_rt(row_data)?;
        let (scores, triggered_flags) = evaluate_cached_rule_scores(cached_rule, &mut rt)?;

        let keep_from = trade_dates
            .binary_search_by(|date| date.as_str().cmp(start_date))
            .unwrap_or_else(|index| index);
        let min_len = usize::min(
            trade_dates.len(),
            usize::min(scores.len(), triggered_flags.len()),
        );
        if keep_from >= min_len {
            continue;
        }

        for index in keep_from..min_len {
            let Some(score) = normalize_validation_trigger_score(
                scores[index],
                triggered_flags[index],
                cached_rule.points,
            ) else {
                continue;
            };
            let trade_date = trade_dates[index].clone();
            triggered_score_map
                .entry(ts_code.clone())
                .or_default()
                .insert(trade_date.clone(), score);
            hit_pairs.insert((ts_code.clone(), trade_date));
        }
    }

    Ok((triggered_score_map, hit_pairs))
}

fn normalize_validation_trigger_score(
    score: f64,
    triggered: bool,
    rule_points: f64,
) -> Option<f64> {
    if !score.is_finite() {
        return None;
    }
    if score.abs() > VALIDATION_EPS {
        return Some(score);
    }
    if !triggered {
        return None;
    }

    if rule_points.is_finite() && rule_points.abs() > VALIDATION_EPS {
        return Some(rule_points.signum());
    }
    Some(1.0)
}

fn build_rule_backtest_payload(
    combo_key: &str,
    params: &RuleLayerBacktestRunParams,
    metrics: crate::simulate::rule::RuleLayerMetrics,
) -> RuleLayerBacktestData {
    RuleLayerBacktestData {
        rule_name: combo_key.to_string(),
        stock_adj_type: params.stock_adj_type.clone(),
        index_ts_code: params.index_ts_code.clone(),
        index_beta: params.index_beta,
        concept_beta: params.concept_beta,
        industry_beta: params.industry_beta,
        start_date: params.start_date.clone(),
        end_date: params.end_date.clone(),
        min_samples_per_rule_day: params.min_samples_per_day,
        backtest_period: params.backtest_period,
        points: metrics
            .points
            .into_iter()
            .map(|point| RuleLayerPointPayload {
                trade_date: point.trade_date,
                sample_count: point.sample_count,
                avg_rule_score: point.avg_rule_score,
                avg_residual_return: point.avg_residual_return,
                top_bottom_spread: point.top_bottom_spread,
                ic: point.ic,
            })
            .collect(),
        avg_residual_mean: metrics.avg_residual_mean,
        spread_mean: metrics.spread_mean,
        ic_mean: metrics.ic_mean,
        ic_std: metrics.ic_std,
        icir: metrics.icir,
        is_all_rules: false,
        all_rule_summaries: Vec::new(),
    }
}

fn load_validation_similarity_rows(
    result_conn: &Connection,
    start_date: &str,
    end_date: &str,
    hit_pairs: &HashSet<(String, String)>,
    exclude_rule_name: Option<&str>,
    explain_map: &HashMap<String, String>,
) -> Result<Vec<RuleValidationSimilarityRow>, String> {
    let combo_hit_count = hit_pairs.len() as f64;
    if combo_hit_count <= 0.0 {
        return Ok(Vec::new());
    }

    let excluded_rule_name = exclude_rule_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    let total_samples = result_conn
        .query_row(
            r#"
            SELECT COUNT(*)
            FROM score_summary
            WHERE trade_date >= ?
              AND trade_date <= ?
            "#,
            params![start_date, end_date],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| format!("读取验证样本总数失败: {e}"))?
        .max(0) as f64;

    let mut stmt = result_conn
        .prepare(
            r#"
            SELECT
                rule_name,
                ts_code,
                trade_date
            FROM rule_details
            WHERE trade_date >= ?
              AND trade_date <= ?
              AND TRY_CAST(rule_score AS DOUBLE) IS NOT NULL
              AND ABS(TRY_CAST(rule_score AS DOUBLE)) > 1e-12
            "#,
        )
        .map_err(|e| format!("预编译触发相似度查询失败: {e}"))?;
    let mut rows = stmt
        .query(params![start_date, end_date])
        .map_err(|e| format!("查询触发相似度失败: {e}"))?;

    let mut existing_hit_count = HashMap::<String, usize>::new();
    let mut overlap_hit_count = HashMap::<String, usize>::new();

    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取触发相似度失败: {e}"))?
    {
        let rule_name: String = row.get(0).map_err(|e| format!("读取规则名失败: {e}"))?;
        if excluded_rule_name
            .as_deref()
            .is_some_and(|excluded| rule_name == excluded)
        {
            continue;
        }
        let ts_code: String = row.get(1).map_err(|e| format!("读取代码失败: {e}"))?;
        let trade_date: String = row.get(2).map_err(|e| format!("读取交易日失败: {e}"))?;

        *existing_hit_count.entry(rule_name.clone()).or_default() += 1;
        if hit_pairs.contains(&(ts_code, trade_date)) {
            *overlap_hit_count.entry(rule_name).or_default() += 1;
        }
    }

    let mut out = overlap_hit_count
        .into_iter()
        .filter_map(|(rule_name, overlap_samples)| {
            if overlap_samples == 0 {
                return None;
            }
            let existing_count = existing_hit_count.get(&rule_name).copied().unwrap_or(0) as f64;
            let overlap_rate_vs_validation = Some(overlap_samples as f64 / combo_hit_count);
            let overlap_rate_vs_existing = if existing_count > 0.0 {
                Some(overlap_samples as f64 / existing_count)
            } else {
                None
            };
            let overlap_lift = if total_samples > 0.0 && existing_count > 0.0 {
                Some(overlap_samples as f64 * total_samples / (combo_hit_count * existing_count))
            } else {
                None
            };

            Some(RuleValidationSimilarityRow {
                rule_name: rule_name.clone(),
                explain: explain_map.get(&rule_name).cloned(),
                overlap_samples,
                overlap_rate_vs_validation,
                overlap_rate_vs_existing,
                overlap_lift,
            })
        })
        .collect::<Vec<_>>();

    out.sort_by(|left, right| {
        right
            .overlap_samples
            .cmp(&left.overlap_samples)
            .then_with(|| left.rule_name.cmp(&right.rule_name))
    });
    out.truncate(20);
    Ok(out)
}

fn compare_option_f64_desc(left: Option<f64>, right: Option<f64>) -> std::cmp::Ordering {
    match (left, right) {
        (Some(l), Some(r)) => r.partial_cmp(&l).unwrap_or(std::cmp::Ordering::Equal),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

#[derive(Debug, Clone)]
struct ValidationSampleRawRow {
    ts_code: String,
    trade_date: String,
    rule_score: f64,
    residual_return: f64,
}

fn build_validation_sample_groups(
    samples: &[ValidationSampleRawRow],
    sample_limit_per_group: usize,
    stock_name_map: &HashMap<String, String>,
) -> (RuleValidationSampleStats, RuleValidationSampleGroups) {
    let mut positive = Vec::new();
    let mut negative = Vec::new();

    for row in samples {
        if row.residual_return > 0.0 {
            positive.push(row.clone());
        } else if row.residual_return < 0.0 {
            negative.push(row.clone());
        }
    }

    positive.sort_by(|left, right| {
        right
            .residual_return
            .partial_cmp(&left.residual_return)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.trade_date.cmp(&right.trade_date))
            .then_with(|| left.ts_code.cmp(&right.ts_code))
    });
    negative.sort_by(|left, right| {
        left.residual_return
            .partial_cmp(&right.residual_return)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.trade_date.cmp(&right.trade_date))
            .then_with(|| left.ts_code.cmp(&right.ts_code))
    });

    let mut rng = StdRng::seed_from_u64(VALIDATION_RANDOM_SAMPLE_SEED);
    let mut random_pool = samples.to_vec();
    random_pool.sort_by(|left, right| {
        left.trade_date
            .cmp(&right.trade_date)
            .then_with(|| left.ts_code.cmp(&right.ts_code))
    });
    if random_pool.len() > 1 {
        for index in (1..random_pool.len()).rev() {
            let swap_index = rng.random_range(0..=index);
            random_pool.swap(index, swap_index);
        }
    }

    let to_payload = |rows: Vec<ValidationSampleRawRow>| {
        rows.into_iter()
            .take(sample_limit_per_group)
            .map(|row| RuleValidationSampleRow {
                name: stock_name_map.get(&row.ts_code).cloned(),
                ts_code: row.ts_code,
                trade_date: row.trade_date,
                rule_score: row.rule_score,
                residual_return: row.residual_return,
            })
            .collect::<Vec<_>>()
    };

    let stats = RuleValidationSampleStats {
        positive_count: positive.len(),
        negative_count: negative.len(),
        random_count: random_pool.len(),
        total_samples: samples.len(),
    };

    let groups = RuleValidationSampleGroups {
        positive: to_payload(positive),
        negative: to_payload(negative),
        random: to_payload(random_pool),
    };

    (stats, groups)
}

pub fn run_rule_expression_validation(
    source_path: String,
    import_rule_name: String,
    when: Option<String>,
    scope_way: Option<String>,
    scope_windows: Option<usize>,
    stock_adj_type: Option<String>,
    index_ts_code: String,
    index_beta: Option<f64>,
    concept_beta: Option<f64>,
    industry_beta: Option<f64>,
    start_date: String,
    end_date: String,
    min_samples_per_rule_day: Option<usize>,
    backtest_period: Option<usize>,
    manual_strategy: Option<RuleExpressionValidationManualStrategy>,
    unknown_configs: Option<Vec<RuleValidationUnknownConfig>>,
    sample_limit_per_group: Option<usize>,
) -> Result<RuleExpressionValidationData, String> {
    let source_path = source_path.trim().to_string();
    if source_path.is_empty() {
        return Err("数据目录不能为空".to_string());
    }

    let import_rule_name = import_rule_name.trim().to_string();
    let all_rules = ScoreRule::load_rules(&source_path)?;
    let seed_rule = resolve_validation_seed_rule(
        &import_rule_name,
        manual_strategy.as_ref(),
        when.as_deref(),
        scope_way.as_deref(),
        scope_windows,
        &all_rules,
    )?;

    let params = RuleLayerBacktestRunParams {
        stock_adj_type: stock_adj_type
            .unwrap_or_else(|| "qfq".to_string())
            .trim()
            .to_string(),
        index_ts_code: index_ts_code.trim().to_string(),
        index_beta: index_beta.unwrap_or(0.5),
        concept_beta: concept_beta.unwrap_or(0.2),
        industry_beta: industry_beta.unwrap_or(0.0),
        start_date: start_date.trim().to_string(),
        end_date: end_date.trim().to_string(),
        min_samples_per_day: min_samples_per_rule_day.unwrap_or(5).max(1),
        backtest_period: backtest_period.unwrap_or(1).max(1),
    };

    let variants =
        build_validation_variants(&seed_rule.formula, &unknown_configs.unwrap_or_default())?;
    let sample_limit_per_group = sample_limit_per_group
        .unwrap_or(VALIDATION_DEFAULT_SAMPLE_LIMIT_PER_GROUP)
        .clamp(1, VALIDATION_MAX_SAMPLE_LIMIT_PER_GROUP);
    let source_db = source_db_path(&source_path);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "原始库路径不是有效UTF-8".to_string())?;
    let source_conn =
        Connection::open(source_db_str).map_err(|e| format!("打开原始库失败: {e}"))?;
    let result_conn = open_result_conn(&source_path)?;
    let explain_map = all_rules
        .iter()
        .map(|rule| (rule.name.clone(), rule.explain.clone()))
        .collect::<HashMap<_, _>>();
    let stock_name_map = load_stock_name_map(&source_path)?;

    let mut combo_results = Vec::with_capacity(variants.len());
    for variant in variants {
        let cached_rule = build_validation_cached_rule(
            variant.combo_key.clone(),
            seed_rule.scope_way,
            seed_rule.scope_windows,
            seed_rule.points,
            seed_rule.dist_points.clone(),
            seed_rule.tag,
            &variant.formula,
        )?;

        let (triggered_score_map, hit_pairs) = build_validation_triggered_scores(
            &source_path,
            &params.stock_adj_type,
            &params.start_date,
            &params.end_date,
            &cached_rule,
        )?;
        let metrics_with_samples = calc_rule_layer_metrics_with_samples_from_triggered_scores(
            &source_conn,
            &source_path,
            &triggered_score_map,
            &params.stock_adj_type,
            &params.index_ts_code,
            params.index_beta,
            params.concept_beta,
            params.industry_beta,
            &params.start_date,
            &params.end_date,
            &RuleLayerConfig {
                min_samples_per_day: params.min_samples_per_day,
                backtest_period: params.backtest_period,
            },
        )?;
        let triggered_sample_rows = metrics_with_samples
            .samples
            .into_iter()
            .filter_map(|sample| {
                if !hit_pairs.contains(&(sample.ts_code.clone(), sample.trade_date.clone())) {
                    return None;
                }
                Some(ValidationSampleRawRow {
                    ts_code: sample.ts_code,
                    trade_date: sample.trade_date,
                    rule_score: sample.rule_score,
                    residual_return: sample.residual_return,
                })
            })
            .collect::<Vec<_>>();

        let (sample_stats, sample_groups) = build_validation_sample_groups(
            &triggered_sample_rows,
            sample_limit_per_group,
            &stock_name_map,
        );
        let metrics = metrics_with_samples.metrics;
        let backtest = build_rule_backtest_payload(&variant.combo_key, &params, metrics);
        let similarity_rows = load_validation_similarity_rows(
            &result_conn,
            &params.start_date,
            &params.end_date,
            &hit_pairs,
            seed_rule.exclude_rule_name.as_deref(),
            &explain_map,
        )?;
        let triggered_days = hit_pairs
            .iter()
            .map(|(_, trade_date)| trade_date.clone())
            .collect::<HashSet<_>>()
            .len();
        let trigger_samples = hit_pairs.len();

        combo_results.push(RuleValidationComboResult {
            combo_key: variant.combo_key,
            combo_label: variant.combo_label,
            formula: variant.formula,
            unknown_values: variant.unknown_values,
            trigger_samples,
            triggered_days,
            avg_daily_trigger: if triggered_days > 0 {
                trigger_samples as f64 / triggered_days as f64
            } else {
                0.0
            },
            sample_stats,
            sample_groups,
            backtest,
            similarity_rows,
        });
    }

    combo_results.sort_by(|left, right| {
        compare_option_f64_desc(left.backtest.spread_mean, right.backtest.spread_mean)
            .then_with(|| compare_option_f64_desc(left.backtest.icir, right.backtest.icir))
            .then_with(|| right.trigger_samples.cmp(&left.trigger_samples))
            .then_with(|| left.combo_key.cmp(&right.combo_key))
    });

    let best_combo_key = combo_results.first().map(|item| item.combo_key.clone());

    Ok(RuleExpressionValidationData {
        import_rule_name: seed_rule.rule_name,
        import_rule_explain: seed_rule.rule_explain,
        scope_way: scope_way_label(seed_rule.scope_way),
        scope_windows: seed_rule.scope_windows,
        sample_limit_per_group,
        combo_results,
        best_combo_key,
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
        let stock_name = cols
            .get(1)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty());

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
    let source_conn =
        Connection::open(source_db_str).map_err(|e| format!("打开原始库失败: {e}"))?;

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
    let concept_conn =
        Connection::open(concept_db_str).map_err(|e| format!("打开概念表现库失败: {e}"))?;
    let concept_interval_sql = r#"
        SELECT concept, AVG(TRY_CAST(performance_pct AS DOUBLE)) AS avg_pct
        FROM concept_performance
        WHERE performance_type = 'concept'
          AND trade_date >= ?
          AND trade_date <= ?
        GROUP BY 1
        ORDER BY avg_pct DESC NULLS LAST, concept ASC
        LIMIT ?
        "#;

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

    let daily_concept_sql = r#"
        SELECT concept, TRY_CAST(performance_pct AS DOUBLE)
        FROM concept_performance
        WHERE performance_type = 'concept'
          AND trade_date = ?
        ORDER BY TRY_CAST(performance_pct AS DOUBLE) DESC NULLS LAST, concept ASC
        LIMIT ?
        "#;

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
    let source_conn =
        Connection::open(source_db_str).map_err(|e| format!("打开原始库失败: {e}"))?;

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
                .split(|ch| matches!(ch, ',' | ';' | '，' | '；' | '|' | '、' | '/' | '\n' | '\r'))
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
        let min_trade_date: Option<String> =
            row.get(0).map_err(|e| format!("读取最小交易日失败: {e}"))?;
        let max_trade_date: Option<String> =
            row.get(1).map_err(|e| format!("读取最大交易日失败: {e}"))?;
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

pub fn get_rule_layer_backtest_defaults(
    source_path: String,
) -> Result<RuleLayerBacktestDefaultsData, String> {
    let (rule_options, _) = load_rule_meta(&source_path)?;

    let conn = open_result_conn(&source_path)?;
    let mut stmt = conn
        .prepare(
            r#"
            SELECT
                MIN(trade_date) AS min_trade_date,
                MAX(trade_date) AS max_trade_date
            FROM rule_details
            "#,
        )
        .map_err(|e| format!("预编译 rule_details 日期区间 SQL 失败: {e}"))?;

    let mut rows = stmt
        .query([])
        .map_err(|e| format!("执行 rule_details 日期区间 SQL 失败: {e}"))?;

    let (start_date, end_date) = if let Some(row) = rows
        .next()
        .map_err(|e| format!("读取 rule_details 日期区间失败: {e}"))?
    {
        let min_trade_date: Option<String> =
            row.get(0).map_err(|e| format!("读取最小交易日失败: {e}"))?;
        let max_trade_date: Option<String> =
            row.get(1).map_err(|e| format!("读取最大交易日失败: {e}"))?;
        (min_trade_date, max_trade_date)
    } else {
        (None, None)
    };

    Ok(RuleLayerBacktestDefaultsData {
        resolved_rule_name: rule_options.first().cloned(),
        rule_options,
        start_date,
        end_date,
    })
}

#[derive(Debug, Clone)]
struct SceneLayerBacktestRunParams {
    stock_adj_type: String,
    index_ts_code: String,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: String,
    end_date: String,
    min_samples_per_day: usize,
    backtest_period: usize,
}

#[derive(Debug, Clone)]
struct RuleLayerBacktestRunParams {
    stock_adj_type: String,
    index_ts_code: String,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: String,
    end_date: String,
    min_samples_per_day: usize,
    backtest_period: usize,
}

fn run_scene_layer_backtest_core(
    source_conn: &Connection,
    source_path: &str,
    scene_name: Option<&str>,
    params: &SceneLayerBacktestRunParams,
) -> Result<SceneLayerBacktestData, String> {
    let layer_config = SceneLayerConfig {
        min_samples_per_day: params.min_samples_per_day,
        backtest_period: params.backtest_period,
    };

    if let Some(scene_name) = scene_name {
        let scene_name = scene_name.trim();
        if scene_name.is_empty() {
            return Err("scene_name不能为空".to_string());
        }

        let input = SceneLayerFromDbInput {
            scene_name: scene_name.to_string(),
            stock_adj_type: params.stock_adj_type.clone(),
            index_ts_code: params.index_ts_code.clone(),
            index_beta: params.index_beta,
            concept_beta: params.concept_beta,
            industry_beta: params.industry_beta,
            start_date: params.start_date.clone(),
            end_date: params.end_date.clone(),
            layer_config,
        };

        let metrics = calc_scene_layer_metrics_from_db(source_conn, source_path, &input)?;

        return Ok(SceneLayerBacktestData {
            scene_name: input.scene_name,
            stock_adj_type: input.stock_adj_type,
            index_ts_code: input.index_ts_code,
            index_beta: input.index_beta,
            concept_beta: input.concept_beta,
            industry_beta: input.industry_beta,
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
        });
    }

    let scene_options = load_scene_options(source_path)?;
    let all_metrics = calc_all_scene_layer_metrics_from_db(
        source_conn,
        source_path,
        &scene_options,
        &params.stock_adj_type,
        &params.index_ts_code,
        params.index_beta,
        params.concept_beta,
        params.industry_beta,
        &params.start_date,
        &params.end_date,
        &layer_config,
    )?;
    let mut all_scene_summaries = Vec::with_capacity(all_metrics.len());

    for (one_scene_name, metrics) in all_metrics {
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

    Ok(SceneLayerBacktestData {
        scene_name: String::new(),
        stock_adj_type: params.stock_adj_type.clone(),
        index_ts_code: params.index_ts_code.clone(),
        index_beta: params.index_beta,
        concept_beta: params.concept_beta,
        industry_beta: params.industry_beta,
        start_date: params.start_date.clone(),
        end_date: params.end_date.clone(),
        min_samples_per_scene_day: params.min_samples_per_day,
        backtest_period: params.backtest_period,
        points: Vec::new(),
        spread_mean: None,
        ic_mean: None,
        ic_std: None,
        icir: None,
        is_all_scenes: true,
        all_scene_summaries,
    })
}

fn run_rule_layer_backtest_core(
    source_conn: &Connection,
    source_path: &str,
    rule_name: Option<&str>,
    params: &RuleLayerBacktestRunParams,
) -> Result<RuleLayerBacktestData, String> {
    let layer_config = RuleLayerConfig {
        min_samples_per_day: params.min_samples_per_day,
        backtest_period: params.backtest_period,
    };

    if let Some(rule_name) = rule_name {
        let rule_name = rule_name.trim();
        if rule_name.is_empty() {
            return Err("rule_name不能为空".to_string());
        }

        let input = RuleLayerFromDbInput {
            rule_name: rule_name.to_string(),
            stock_adj_type: params.stock_adj_type.clone(),
            index_ts_code: params.index_ts_code.clone(),
            index_beta: params.index_beta,
            concept_beta: params.concept_beta,
            industry_beta: params.industry_beta,
            start_date: params.start_date.clone(),
            end_date: params.end_date.clone(),
            layer_config,
        };

        let metrics = calc_rule_layer_metrics_from_db(source_conn, source_path, &input)?;

        return Ok(RuleLayerBacktestData {
            rule_name: input.rule_name,
            stock_adj_type: input.stock_adj_type,
            index_ts_code: input.index_ts_code,
            index_beta: input.index_beta,
            concept_beta: input.concept_beta,
            industry_beta: input.industry_beta,
            start_date: input.start_date,
            end_date: input.end_date,
            min_samples_per_rule_day: input.layer_config.min_samples_per_day,
            backtest_period: input.layer_config.backtest_period,
            points: metrics
                .points
                .into_iter()
                .map(|point| RuleLayerPointPayload {
                    trade_date: point.trade_date,
                    sample_count: point.sample_count,
                    avg_rule_score: point.avg_rule_score,
                    avg_residual_return: point.avg_residual_return,
                    top_bottom_spread: point.top_bottom_spread,
                    ic: point.ic,
                })
                .collect(),
            avg_residual_mean: metrics.avg_residual_mean,
            spread_mean: metrics.spread_mean,
            ic_mean: metrics.ic_mean,
            ic_std: metrics.ic_std,
            icir: metrics.icir,
            is_all_rules: false,
            all_rule_summaries: Vec::new(),
        });
    }

    let (rule_options, _) = load_rule_meta(source_path)?;
    let all_metrics = calc_all_rule_layer_metrics_from_db(
        source_conn,
        source_path,
        &rule_options,
        &params.stock_adj_type,
        &params.index_ts_code,
        params.index_beta,
        params.concept_beta,
        params.industry_beta,
        &params.start_date,
        &params.end_date,
        &layer_config,
    )?;
    let mut all_rule_summaries = Vec::with_capacity(all_metrics.len());

    for (one_rule_name, metrics) in all_metrics {
        all_rule_summaries.push(RuleLayerRuleSummary {
            rule_name: one_rule_name,
            point_count: metrics.points.len(),
            avg_residual_mean: metrics.avg_residual_mean,
            spread_mean: metrics.spread_mean,
            ic_mean: metrics.ic_mean,
            ic_std: metrics.ic_std,
            icir: metrics.icir,
        });
    }

    all_rule_summaries.sort_by(|a, b| {
        b.spread_mean
            .unwrap_or(f64::NEG_INFINITY)
            .partial_cmp(&a.spread_mean.unwrap_or(f64::NEG_INFINITY))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.point_count.cmp(&a.point_count))
            .then_with(|| a.rule_name.cmp(&b.rule_name))
    });

    Ok(RuleLayerBacktestData {
        rule_name: String::new(),
        stock_adj_type: params.stock_adj_type.clone(),
        index_ts_code: params.index_ts_code.clone(),
        index_beta: params.index_beta,
        concept_beta: params.concept_beta,
        industry_beta: params.industry_beta,
        start_date: params.start_date.clone(),
        end_date: params.end_date.clone(),
        min_samples_per_rule_day: params.min_samples_per_day,
        backtest_period: params.backtest_period,
        points: Vec::new(),
        avg_residual_mean: None,
        spread_mean: None,
        ic_mean: None,
        ic_std: None,
        icir: None,
        is_all_rules: true,
        all_rule_summaries,
    })
}

pub fn run_scene_layer_backtest(
    source_path: String,
    stock_adj_type: Option<String>,
    index_ts_code: String,
    index_beta: Option<f64>,
    concept_beta: Option<f64>,
    industry_beta: Option<f64>,
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

    let params = SceneLayerBacktestRunParams {
        stock_adj_type: stock_adj_type
            .unwrap_or_else(|| "qfq".to_string())
            .trim()
            .to_string(),
        index_ts_code: index_ts_code.trim().to_string(),
        index_beta: index_beta.unwrap_or(0.5),
        concept_beta: concept_beta.unwrap_or(0.2),
        industry_beta: industry_beta.unwrap_or(0.0),
        start_date: start_date.trim().to_string(),
        end_date: end_date.trim().to_string(),
        min_samples_per_day: min_samples_per_scene_day.unwrap_or(5),
        backtest_period: backtest_period.unwrap_or(1),
    };

    // 当前入口固定全量；后续如需恢复单场景，仅需传入 Some(scene_name)。
    run_scene_layer_backtest_core(&source_conn, &source_path, None, &params)
}

pub fn run_rule_layer_backtest(
    source_path: String,
    stock_adj_type: Option<String>,
    index_ts_code: String,
    index_beta: Option<f64>,
    concept_beta: Option<f64>,
    industry_beta: Option<f64>,
    start_date: String,
    end_date: String,
    min_samples_per_rule_day: Option<usize>,
    backtest_period: Option<usize>,
) -> Result<RuleLayerBacktestData, String> {
    let source_db = source_db_path(&source_path);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "原始库路径不是有效UTF-8".to_string())?;
    let source_conn =
        Connection::open(source_db_str).map_err(|e| format!("打开原始库失败: {e}"))?;

    let params = RuleLayerBacktestRunParams {
        stock_adj_type: stock_adj_type
            .unwrap_or_else(|| "qfq".to_string())
            .trim()
            .to_string(),
        index_ts_code: index_ts_code.trim().to_string(),
        index_beta: index_beta.unwrap_or(0.5),
        concept_beta: concept_beta.unwrap_or(0.2),
        industry_beta: industry_beta.unwrap_or(0.0),
        start_date: start_date.trim().to_string(),
        end_date: end_date.trim().to_string(),
        min_samples_per_day: min_samples_per_rule_day.unwrap_or(5),
        backtest_period: backtest_period.unwrap_or(1),
    };

    // 当前入口固定全量；后续如需恢复单策略，仅需传入 Some(rule_name)。
    run_rule_layer_backtest_core(&source_conn, &source_path, None, &params)
}
