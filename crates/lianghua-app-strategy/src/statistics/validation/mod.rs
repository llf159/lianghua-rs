pub(super) mod samples;
pub(super) mod scores;
pub(super) mod similarity;
pub(super) mod walk_forward;

use crate::data::DataReader;
use crate::data::RuleKind;
use crate::data::RuleStage;
use crate::data::RuleTag;
use crate::data::ScopeWay;
use crate::data::ScoreRule;
use crate::data::result_db_path;
use crate::data::source_db_path;
use crate::expr::lexer::TokenKind;
use crate::expr::parser::lex_all;
use crate::scoring::CachedRule;
use crate::scoring::tools::calc_query_need_rows;
use crate::scoring::tools::calc_query_start_date;
use crate::scoring::tools::load_st_list;
use crate::simulate::DEFAULT_BACKTEST_MIN_LISTED_TRADE_DAYS;
use crate::simulate::rule::DEFAULT_RULE_WITH_SAMPLES_PARALLEL_BATCH_SIZE;
use crate::simulate::rule::RuleLayerConfig;
use crate::simulate::rule::RuleLayerRuntimeCache;
use crate::simulate::rule::build_rule_layer_runtime_cache_from_stock_data_with_ts_filter;
use crate::simulate::rule::calc_rule_layer_metrics_with_samples_from_cache;
use crate::simulate::rule::visit_triggered_rule_samples_from_cache;
use crate::statistics::backtest::{RuleLayerBacktestData, RuleLayerBacktestRunParams};
use crate::statistics::common::{open_result_conn, parse_scope_way_input, scope_way_label};
use crate::statistics::universe::{
    ValidationSampleStockMeta, build_backtest_stock_filter, load_validation_sample_stock_meta_map,
    ts_code_allowed_by_filter,
};
use crate::statistics::validation::samples::ValidationSampleAccumulator;
use crate::statistics::validation::scores::{
    build_rule_backtest_payload, build_validation_cached_rule,
    build_validation_return_distribution, build_validation_score_layer_details,
    build_validation_triggered_scores_for_combos, collect_rule_validation_runtime_keys,
    collect_validation_assigned_names, estimate_rule_warmup, format_validation_number,
};
use crate::statistics::validation::similarity::{
    ValidationExistingRuleScoreIndex, ValidationUniverseIndex,
    build_validation_existing_rule_daily_returns, build_validation_expression_similarity_rows,
    build_validation_universe_index, load_validation_existing_rule_score_index,
    validation_pair_hash,
};
use crate::statistics::validation::walk_forward::{
    ValidationFoldPlan, build_validation_fold_plan, build_validation_incremental,
    build_validation_walk_forward, sort_validation_points, validation_axis_direction_sign,
};
use duckdb::Connection;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
pub(super) const VALIDATION_EPS: f64 = 1e-12;
pub(super) const VALIDATION_MAX_COMBINATIONS: usize = 256;
#[derive(Debug, Clone, Deserialize)]
pub struct RuleValidationUnknownConfig {
    pub name: String,
    pub start: f64,
    pub end: f64,
    pub step: f64,
}

#[derive(Debug, Clone, Serialize)]
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
    pub jaccard: Option<f64>,
    pub phi: Option<f64>,
    pub score_pearson: Option<f64>,
    pub return_pearson: Option<f64>,
    pub shared_return_days: usize,
}

#[derive(Debug, Serialize)]
pub struct RuleValidationSampleStats {
    pub positive_count: usize,
    pub negative_count: usize,
    pub random_count: usize,
    pub total_samples: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct RuleValidationTriggerCountStats {
    pub trigger_count: usize,
    pub positive_count: usize,
    pub negative_count: usize,
    pub random_count: usize,
    pub total_samples: usize,
}

#[derive(Debug, Serialize)]
pub struct RuleValidationSampleRow {
    pub ts_code: String,
    pub name: Option<String>,
    pub board: String,
    pub volatility_group: String,
    pub trade_date: String,
    pub trigger_count: usize,
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
pub struct RuleValidationReturnDistributionBucket {
    pub bucket_label: String,
    pub sample_count: usize,
    pub sample_ratio: Option<f64>,
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
    pub trigger_count_stats: Vec<RuleValidationTriggerCountStats>,
    pub sample_groups: RuleValidationSampleGroups,
    pub return_distribution: Vec<RuleValidationReturnDistributionBucket>,
    pub backtest: RuleLayerBacktestData,
    pub daily_metrics: Vec<RuleValidationDailyMetric>,
    pub walk_forward: RuleValidationWalkForwardData,
    pub incremental: RuleValidationIncrementalData,
    pub similarity_rows: Vec<RuleValidationSimilarityRow>,
}

#[derive(Debug, Serialize)]
pub struct RuleExpressionValidationData {
    pub import_rule_name: String,
    pub import_rule_explain: String,
    pub scope_way: String,
    pub scope_windows: usize,
    pub sample_limit_per_group: usize,
    pub walk_forward_folds: usize,
    pub core_rule_names: Vec<String>,
    pub combo_results: Vec<RuleValidationComboResult>,
}

#[derive(Debug, Serialize)]
pub struct ValidationCoreRuleOption {
    pub name: String,
    pub trigger_count: usize,
    pub valid_trigger_count: usize,
}

pub fn get_validation_core_rule_options(
    source_path: &str,
) -> Result<Vec<ValidationCoreRuleOption>, String> {
    if !result_db_path(source_path).exists() {
        return Ok(Vec::new());
    }
    let mut options = Vec::new();
    let Ok(conn) = open_result_conn(source_path) else {
        return Ok(options);
    };
    let query_result = (|conn: &duckdb::Connection| -> Result<(), String> {
        let mut stmt = conn
            .prepare(
                r#"
                SELECT
                    rule_name,
                    COUNT(*),
                    SUM(
                        CASE
                            WHEN ABS(TRY_CAST(rule_score AS DOUBLE)) > ? THEN 1
                            ELSE 0
                        END
                    )
                FROM rule_details
                WHERE rule_name IS NOT NULL AND TRIM(rule_name) <> ''
                GROUP BY rule_name
                ORDER BY rule_name
                "#,
            )
            .map_err(|error| format!("预编译核心策略可用性查询失败: {error}"))?;
        let mut rows = stmt
            .query(duckdb::params![VALIDATION_EPS])
            .map_err(|error| format!("查询核心策略可用性失败: {error}"))?;
        while let Some(row) = rows
            .next()
            .map_err(|error| format!("读取核心策略可用性失败: {error}"))?
        {
            let name: String = row.get(0).map_err(|e| format!("读取规则名失败: {e}"))?;
            let trigger_count: i64 = row.get(1).map_err(|e| format!("读取触发数失败: {e}"))?;
            let valid_trigger_count: i64 =
                row.get(2).map_err(|e| format!("读取有效触发数失败: {e}"))?;
            options.push(ValidationCoreRuleOption {
                name,
                trigger_count: trigger_count.max(0) as usize,
                valid_trigger_count: valid_trigger_count.max(0) as usize,
            });
        }
        Ok(())
    })(&conn);
    if let Err(error) = query_result {
        eprintln!("读取核心策略可用性失败: {error}");
        return Ok(Vec::new());
    }
    Ok(options)
}

#[derive(Debug, Serialize)]
pub struct RuleValidationWalkForwardFold {
    pub fold_index: usize,
    pub status: String,
    pub train_start_date: String,
    pub train_end_date: String,
    pub test_start_date: String,
    pub test_end_date: String,
    pub test_day_count: usize,
    pub test_sample_count: usize,
    pub ic_mean: Option<f64>,
    pub ic_t_value: Option<f64>,
    pub avg_residual_return: Option<f64>,
    pub spread_mean: Option<f64>,
}

#[derive(Debug, Default, Serialize)]
pub struct RuleValidationWalkForwardData {
    pub fold_count: usize,
    pub purge_days: usize,
    pub folds: Vec<RuleValidationWalkForwardFold>,
    pub ic_positive_folds: usize,
    pub residual_positive_folds: usize,
    pub spread_positive_folds: usize,
}

#[derive(Debug, Serialize)]
pub struct RuleValidationIncrementalFold {
    pub fold_index: usize,
    pub status: String,
    pub train_start_date: String,
    pub train_end_date: String,
    pub test_start_date: String,
    pub test_end_date: String,
    pub train_day_count: usize,
    pub test_day_count: usize,
    pub incremental_mean: Option<f64>,
    pub incremental_hac_t: Option<f64>,
    pub positive_day_ratio: Option<f64>,
}

#[derive(Debug, Default, Serialize)]
pub struct RuleValidationIncrementalData {
    pub core_rule_names: Vec<String>,
    pub purge_days: usize,
    pub folds: Vec<RuleValidationIncrementalFold>,
    pub positive_folds: usize,
}

#[derive(Debug, Serialize)]
pub struct RuleValidationDailyMetric {
    pub trade_date: String,
    pub ic: Option<f64>,
    pub avg_residual_return: Option<f64>,
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
pub(super) struct ValidationVariant {
    pub(in crate::statistics) combo_key: String,
    pub(in crate::statistics) combo_label: String,
    pub(in crate::statistics) formula: String,
    pub(in crate::statistics) unknown_values: Vec<RuleValidationUnknownValue>,
}

pub(super) type ValidationTriggeredScoreMap = HashMap<String, HashMap<String, f64>>;

pub(super) struct PreparedValidationCombo {
    pub(in crate::statistics) variant: ValidationVariant,
    pub(in crate::statistics) cached_rule: CachedRule,
    pub(in crate::statistics) assigned_names: Vec<String>,
}

pub(super) struct ValidationExecutionPlan {
    combos: Vec<PreparedValidationCombo>,
    need_rows: usize,
    query_start_date: String,
}

pub(super) struct ValidationTsCodeEvaluation {
    ts_code: String,
    combo_hits: Vec<(usize, HashMap<String, f64>)>,
}

#[derive(Debug, Clone)]
pub(super) struct ValidationSeedRule {
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

pub(super) fn read_non_empty_owned(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
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
    min_listed_trade_days: Option<usize>,
    backtest_period: Option<usize>,
    manual_strategy: Option<RuleExpressionValidationManualStrategy>,
    unknown_configs: Option<Vec<RuleValidationUnknownConfig>>,
    sample_limit_per_group: Option<usize>,
    board: Option<String>,
    exclude_st_board: Option<bool>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
    walk_forward_folds: Option<usize>,
    core_rule_names: Option<Vec<String>>,
) -> Result<RuleExpressionValidationData, String> {
    let source_path = source_path.trim().to_string();
    if source_path.is_empty() {
        return Err("数据目录不能为空".to_string());
    }

    let import_rule_name = import_rule_name.trim().to_string();
    let all_rules = ScoreRule::load_rules(&source_path)?;
    let seed_rule = (|import_rule_name_raw: &str,
                      manual_strategy: Option<&RuleExpressionValidationManualStrategy>,
                      when: Option<&str>,
                      scope_way: Option<&str>,
                      scope_windows: Option<usize>,
                      all_rules: &[ScoreRule]|
     -> Result<ValidationSeedRule, String> {
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
        let manual_points = (|raw: Option<f64>| -> Result<f64, String> {
            match raw {
                Some(value) if !value.is_finite() => Err("手动策略 points 非法".to_string()),
                Some(value) if value < 0.0 => Ok(-1.0),
                Some(_) | None => Ok(1.0),
            }
        })(manual_strategy.and_then(|strategy| strategy.points))?;
        let manual_dist_points = manual_strategy
            .and_then(|strategy| strategy.dist_points.clone())
            .and_then(|items| if items.is_empty() { None } else { Some(items) });

        if import_rule
            .as_ref()
            .is_some_and(|rule| rule.kind == RuleKind::Combination)
            && top_formula.is_none()
            && manual_formula.is_none()
        {
            return Err("组合策略不能导入到单表达式验证，请直接在“策略回测”中验证".to_string());
        }

        let manual_scope_way = match manual_strategy
            .and_then(|strategy| read_non_empty_owned(strategy.scope_way.as_deref()))
        {
            Some(raw) => Some(parse_scope_way_input(&raw)?),
            None => None,
        };

        let manual_tag = match manual_strategy.and_then(|strategy| strategy.tag.as_deref()) {
            Some(raw) if !raw.trim().is_empty() => {
                Some((|tag_raw: &str| -> Result<RuleTag, String> {
                    match tag_raw.trim().to_ascii_lowercase().as_str() {
                        "" | "normal" => Ok(RuleTag::Normal),
                        "opportunity" => Ok(RuleTag::Opportunity),
                        "rare" => Ok(RuleTag::Rare),
                        _ => Err(format!(
                            "tag 不支持: {tag_raw}，仅支持 normal/opportunity/rare"
                        )),
                    }
                })(raw)?)
            }
            _ => None,
        };

        if let Some(stage_raw) = manual_strategy.and_then(|strategy| strategy.stage.as_deref()) {
            if !stage_raw.trim().is_empty() {
                let _ = (|stage_raw: &str| -> Result<RuleStage, String> {
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
                })(stage_raw)?;
            }
        }

        let has_manual_override = manual_name.is_some()
            || manual_formula.is_some()
            || manual_scope_way.is_some()
            || manual_scope_windows.is_some()
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

        let points = manual_points;
        if !points.is_finite() {
            return Err("策略 points 非法".to_string());
        }

        let dist_points = manual_dist_points;

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
    })(
        &import_rule_name,
        manual_strategy.as_ref(),
        when.as_deref(),
        scope_way.as_deref(),
        scope_windows,
        &all_rules,
    )?;
    let start_date = start_date.trim().to_string();
    let end_date = end_date.trim().to_string();
    let variants = (|formula: &str,
                     unknown_configs: &[RuleValidationUnknownConfig]|
     -> Result<Vec<ValidationVariant>, String> {
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

            let values = (|config: &RuleValidationUnknownConfig| -> Result<Vec<f64>, String> {
                let name = config.name.trim();
                if name.is_empty() {
                    return Err("未知数名称不能为空".to_string());
                }
                if !config.start.is_finite() || !config.end.is_finite() || !config.step.is_finite()
                {
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
            })(config)?;
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
                let replaced_formula = (|formula: &str, assignments: &[(String, f64)]| -> String {
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
                })(formula, &sorted);
                let combo_key = format!("validation_combo_{:03}", out.len() + 1);
                let combo_label = if unknown_values.is_empty() {
                    "默认参数".to_string()
                } else {
                    unknown_values
                        .iter()
                        .map(|item| {
                            format!("{}={}", item.name, format_validation_number(item.value))
                        })
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
    })(&seed_rule.formula, &unknown_configs.unwrap_or_default())?;
    let execution_plan = (|source_path: &str,
                           start_date: &str,
                           end_date: &str,
                           seed_rule: &ValidationSeedRule,
                           variants: Vec<ValidationVariant>|
     -> Result<ValidationExecutionPlan, String> {
        let mut max_warmup_need = 0usize;
        let mut combos = Vec::with_capacity(variants.len());

        for variant in variants {
            let combo = (|seed_rule: &ValidationSeedRule,
                          variant: ValidationVariant|
             -> Result<PreparedValidationCombo, String> {
                let cached_rule = build_validation_cached_rule(
                    variant.combo_key.clone(),
                    seed_rule.scope_way,
                    seed_rule.scope_windows,
                    seed_rule.points,
                    seed_rule.dist_points.clone(),
                    seed_rule.tag,
                    &variant.formula,
                )?;
                let assigned_names = collect_validation_assigned_names(&cached_rule.when_ast);

                Ok(PreparedValidationCombo {
                    variant,
                    cached_rule,
                    assigned_names,
                })
            })(seed_rule, variant)?;
            max_warmup_need = max_warmup_need.max(estimate_rule_warmup(
                &combo.cached_rule.when_ast,
                combo.cached_rule.scope_way,
                combo.cached_rule.scope_windows,
            )?);
            combos.push(combo);
        }

        let need_rows = calc_query_need_rows(source_path, max_warmup_need, start_date, end_date)?;
        let query_start_date = calc_query_start_date(source_path, max_warmup_need, start_date)?;
        Ok(ValidationExecutionPlan {
            combos,
            need_rows,
            query_start_date,
        })
    })(&source_path, &start_date, &end_date, &seed_rule, variants)?;

    let (resolved_board, exclude_st_board, total_mv_min, total_mv_max, allowed_ts_codes) =
        build_backtest_stock_filter(
            &source_path,
            board,
            exclude_st_board,
            total_mv_min,
            total_mv_max,
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
        start_date,
        end_date,
        min_samples_per_day: min_samples_per_rule_day.unwrap_or(5).max(1),
        min_listed_trade_days: min_listed_trade_days
            .unwrap_or(DEFAULT_BACKTEST_MIN_LISTED_TRADE_DAYS),
        backtest_period: backtest_period.unwrap_or(1).max(1),
        parallel_batch_size: DEFAULT_RULE_WITH_SAMPLES_PARALLEL_BATCH_SIZE,
        resolved_board,
        exclude_st_board,
        total_mv_min,
        total_mv_max,
        allowed_ts_codes,
    };

    let sample_limit_per_group = sample_limit_per_group.unwrap_or(30).clamp(1, 200);
    let source_db = source_db_path(&source_path);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "原始库路径不是有效UTF-8".to_string())?;
    let source_conn =
        Connection::open(source_db_str).map_err(|e| format!("打开原始库失败: {e}"))?;
    let layer_config = RuleLayerConfig {
        min_samples_per_day: params.min_samples_per_day,
        backtest_period: params.backtest_period,
        min_listed_trade_days: params.min_listed_trade_days,
    };
    let runtime_cache = Arc::new(
        build_rule_layer_runtime_cache_from_stock_data_with_ts_filter(
            &source_conn,
            &source_path,
            &params.stock_adj_type,
            &params.index_ts_code,
            params.index_beta,
            params.concept_beta,
            params.industry_beta,
            &params.start_date,
            &params.end_date,
            &layer_config,
            params.allowed_ts_codes.as_ref(),
        )?,
    );
    let validation_required_runtime_keys =
        collect_rule_validation_runtime_keys(&execution_plan.combos);
    let validation_reader =
        DataReader::new_with_runtime_keys(&source_path, &validation_required_runtime_keys)?;
    let mut validation_ts_codes = validation_reader.list_ts_code(
        &params.stock_adj_type,
        &params.start_date,
        &params.end_date,
    )?;
    if let Some(allowed_ts_codes) = params.allowed_ts_codes.as_ref() {
        validation_ts_codes
            .retain(|ts_code| ts_code_allowed_by_filter(Some(allowed_ts_codes), ts_code));
    }
    let st_list = load_st_list(&source_path)?;
    let explain_map = all_rules
        .iter()
        .map(|rule| (rule.name.clone(), rule.explain.clone()))
        .collect::<HashMap<_, _>>();
    let stock_meta_map = load_validation_sample_stock_meta_map(&source_path)?;
    let universe_index = build_validation_universe_index(
        &calc_rule_layer_metrics_with_samples_from_cache(
            runtime_cache.as_ref(),
            &ValidationTriggeredScoreMap::new(),
            &layer_config,
        )?
        .samples,
    );
    let existing_rule_score_index = load_validation_existing_rule_score_index(
        &source_path,
        &params.start_date,
        &params.end_date,
        &universe_index,
    );
    let existing_rule_daily_returns = build_validation_existing_rule_daily_returns(
        &universe_index,
        &existing_rule_score_index,
        params.min_samples_per_day,
    );
    let requested_core_rule_names = core_rule_names.unwrap_or_default();
    if requested_core_rule_names
        .iter()
        .filter(|name| !name.trim().is_empty())
        .count()
        > 8
    {
        return Err("核心策略最多选择 8 个".to_string());
    }
    let core_series = existing_rule_score_index
        .rule_names
        .iter()
        .enumerate()
        .filter(|(_, rule_name)| {
            requested_core_rule_names
                .iter()
                .any(|name| name.trim() == rule_name.as_str())
        })
        .map(|(rule_index, rule_name)| {
            (rule_name.clone(), &existing_rule_daily_returns[rule_index])
        })
        .collect::<Vec<_>>();
    let missing_core_rule_names = requested_core_rule_names
        .iter()
        .map(|name| name.trim())
        .filter(|name| !name.is_empty())
        .filter(|name| {
            !existing_rule_score_index
                .rule_names
                .iter()
                .any(|rule_name| rule_name.as_str() == *name)
        })
        .collect::<Vec<_>>();
    if !missing_core_rule_names.is_empty() {
        let describe = |name: &str| -> String {
            if let Some(count) = existing_rule_score_index.universe_trigger_counts.get(name) {
                format!("{name}（本次样本内触发 {count} 次，但分数全部为 0）")
            } else if let Some(count) = existing_rule_score_index.range_trigger_counts.get(name) {
                format!("{name}（区间内触发 {count} 次，但都不在本次股票池样本内）")
            } else {
                format!("{name}（结果库区间内没有触发记录）")
            }
        };
        let zero_scored = missing_core_rule_names.iter().any(|name| {
            existing_rule_score_index
                .universe_trigger_counts
                .contains_key(*name)
        });
        let hint = if zero_scored {
            "核心策略在结果库里分数被归零或未重算（排名计算用的策略版本与当前策略文件可能不一致），请重算该区间评分，或改选其他核心策略"
        } else if existing_rule_score_index.rule_names.is_empty()
            && existing_rule_score_index.range_trigger_counts.is_empty()
        {
            "结果库中没有可用的策略评分，请先执行排名计算（Ranking Compute）"
        } else {
            "请先在排名计算中生成这些策略的评分，或改选其他核心策略"
        };
        return Err(format!(
            "核心策略在结果库中不可用作增量 predictor: {}；{hint}",
            missing_core_rule_names
                .iter()
                .map(|name| describe(name))
                .collect::<Vec<_>>()
                .join("；")
        ));
    }
    let resolved_walk_forward_folds = walk_forward_folds.unwrap_or(4).clamp(1, 8);
    let mut validation_calendar = runtime_cache
        .trade_dates()
        .map(str::to_string)
        .collect::<Vec<_>>();
    validation_calendar.sort_unstable();
    validation_calendar.dedup();
    let validation_purge_days = params.backtest_period;
    let validation_hac_lag = params.backtest_period.saturating_sub(1);
    let validation_fold_plan = build_validation_fold_plan(
        validation_calendar.len(),
        resolved_walk_forward_folds,
        validation_purge_days,
    );
    let mut combo_results = Vec::with_capacity(execution_plan.combos.len());
    for combo_chunk in execution_plan.combos.chunks(16) {
        let combo_triggered_maps = build_validation_triggered_scores_for_combos(
            &source_path,
            &params.stock_adj_type,
            &execution_plan.query_start_date,
            &params.start_date,
            &params.end_date,
            execution_plan.need_rows,
            &validation_ts_codes,
            &st_list,
            combo_chunk,
        )?;

        for (combo, triggered_score_map) in combo_chunk.iter().zip(combo_triggered_maps.into_iter())
        {
            combo_results.push(
                (|params: &RuleLayerBacktestRunParams,
                  seed_rule: &ValidationSeedRule,
                  combo: &PreparedValidationCombo,
                  triggered_score_map: ValidationTriggeredScoreMap,
                  runtime_cache: &RuleLayerRuntimeCache,
                  layer_config: &RuleLayerConfig,
                  universe_index: &ValidationUniverseIndex,
                  existing_rule_score_index: &ValidationExistingRuleScoreIndex,
                  existing_rule_daily_returns: &[HashMap<String, f64>],
                  core_series: &[(String, &HashMap<String, f64>)],
                  calendar: &[String],
                  fold_plan: &[ValidationFoldPlan],
                  purge_days: usize,
                  hac_lag: usize,
                  explain_map: &HashMap<String, String>,
                  stock_meta_map: &HashMap<String, ValidationSampleStockMeta>,
                  sample_limit_per_group: usize|
                 -> Result<RuleValidationComboResult, String> {
                    let metrics_with_samples = calc_rule_layer_metrics_with_samples_from_cache(
                        runtime_cache,
                        &triggered_score_map,
                        layer_config,
                    )?;
                    let validation_layer_details = build_validation_score_layer_details(
                        &metrics_with_samples.samples,
                        layer_config.min_samples_per_day,
                    );
                    let return_distribution =
                        build_validation_return_distribution(&metrics_with_samples.samples);
                    let mut sample_accumulator = ValidationSampleAccumulator::new(
                        sample_limit_per_group,
                        stock_meta_map,
                        None,
                        matches!(seed_rule.scope_way, ScopeWay::Each),
                        seed_rule.points,
                        seed_rule.dist_points.is_some(),
                    );
                    visit_triggered_rule_samples_from_cache(
                        runtime_cache,
                        &triggered_score_map,
                        |sample| {
                            sample_accumulator.push(sample);
                            Ok(())
                        },
                    )?;
                    let (
                        trigger_samples,
                        triggered_days,
                        sample_stats,
                        trigger_count_stats,
                        sample_groups,
                        _,
                    ) = sample_accumulator.into_parts();
                    let axis = sort_validation_points(&metrics_with_samples.metrics.points);
                    let direction_sign = validation_axis_direction_sign(&axis);
                    let points_by_date = axis
                        .iter()
                        .map(|point| (point.trade_date.as_str(), *point))
                        .collect::<HashMap<_, _>>();
                    let candidate_daily_returns = axis
                        .iter()
                        .filter_map(|point| {
                            point
                                .score_weighted_residual_return
                                .filter(|value| value.is_finite())
                                .map(|value| (point.trade_date.clone(), value))
                        })
                        .collect::<HashMap<_, _>>();
                    let mut candidate_scores = HashMap::<u64, f64>::new();
                    let mut day_trigger_counts = HashMap::<String, usize>::new();
                    for (ts_code, scores_by_date) in &triggered_score_map {
                        for (trade_date, score) in scores_by_date {
                            if !score.is_finite() {
                                continue;
                            }
                            let pair_hash = validation_pair_hash(ts_code, trade_date);
                            if universe_index.index_by_pair.contains_key(&pair_hash) {
                                candidate_scores.insert(pair_hash, *score);
                                *day_trigger_counts.entry(trade_date.clone()).or_default() += 1;
                            }
                        }
                    }
                    let similarity_rows = build_validation_expression_similarity_rows(
                        universe_index,
                        existing_rule_score_index,
                        existing_rule_daily_returns,
                        &candidate_scores,
                        &candidate_daily_returns,
                        seed_rule.exclude_rule_name.as_deref(),
                        explain_map,
                    );
                    let walk_forward = build_validation_walk_forward(
                        calendar,
                        fold_plan,
                        params.backtest_period,
                        direction_sign,
                        &points_by_date,
                        &day_trigger_counts,
                    );
                    let incremental = build_validation_incremental(
                        calendar,
                        fold_plan,
                        purge_days,
                        hac_lag,
                        &candidate_daily_returns,
                        core_series,
                    );
                    let daily_metrics = axis
                        .iter()
                        .map(|point| RuleValidationDailyMetric {
                            trade_date: point.trade_date.clone(),
                            ic: point.ic,
                            avg_residual_return: point
                                .avg_excess_residual_return
                                .map(|value| value * direction_sign),
                        })
                        .collect();
                    let backtest = build_rule_backtest_payload(
                        &combo.variant.combo_key,
                        params,
                        metrics_with_samples.metrics,
                        Some(validation_layer_details),
                    );

                    Ok(RuleValidationComboResult {
                        combo_key: combo.variant.combo_key.clone(),
                        combo_label: combo.variant.combo_label.clone(),
                        formula: combo.variant.formula.clone(),
                        unknown_values: combo.variant.unknown_values.clone(),
                        trigger_samples,
                        triggered_days,
                        avg_daily_trigger: if triggered_days > 0 {
                            trigger_samples as f64 / triggered_days as f64
                        } else {
                            0.0
                        },
                        sample_stats,
                        trigger_count_stats,
                        sample_groups,
                        return_distribution,
                        backtest,
                        daily_metrics,
                        walk_forward,
                        incremental,
                        similarity_rows,
                    })
                })(
                    &params,
                    &seed_rule,
                    combo,
                    triggered_score_map,
                    runtime_cache.as_ref(),
                    &layer_config,
                    &universe_index,
                    &existing_rule_score_index,
                    &existing_rule_daily_returns,
                    &core_series,
                    &validation_calendar,
                    &validation_fold_plan,
                    validation_purge_days,
                    validation_hac_lag,
                    &explain_map,
                    &stock_meta_map,
                    sample_limit_per_group,
                )?,
            );
        }
    }

    Ok(RuleExpressionValidationData {
        import_rule_name: seed_rule.rule_name,
        import_rule_explain: seed_rule.rule_explain,
        scope_way: scope_way_label(seed_rule.scope_way),
        scope_windows: seed_rule.scope_windows,
        sample_limit_per_group,
        walk_forward_folds: resolved_walk_forward_folds,
        core_rule_names: core_series
            .iter()
            .map(|(rule_name, _)| rule_name.clone())
            .collect(),
        combo_results,
    })
}

#[cfg(test)]
mod tests {
    use crate::statistics::test_support::*;
    use std::fs::{create_dir_all, write};
    use std::path::PathBuf;

    #[test]
    fn rule_expression_validation_reports_bad_expression_before_stock_filter() {
        let source_dir = temp_source_dir();
        let source_dir_str = source_dir.to_str().expect("utf8 source dir");
        create_dir_all(source_dir_str).expect("create source dir");
        write(
            PathBuf::from(source_dir_str).join("score_rule.toml"),
            r#"
    version = 1

    [[scene]]
    name = "趋势启动"
    direction = "long"
    observe_threshold = 1.0
    trigger_threshold = 2.0
    confirm_threshold = 3.0
    fail_threshold = 1.0

    [[rule]]
    name = "有效策略"
    scene = "趋势启动"
    stage = "base"
    scope_windows = 1
    scope_way = "LAST"
    when = "C > O"
    points = 1.0
    explain = "test"
    "#,
        )
        .expect("write score_rule.toml");

        let error = super::run_rule_expression_validation(
            source_dir_str.to_string(),
            String::new(),
            Some("MA(C,".to_string()),
            Some("LAST".to_string()),
            Some(1),
            Some("qfq".to_string()),
            "000001.SH".to_string(),
            Some(0.5),
            Some(0.2),
            Some(0.0),
            "20240102".to_string(),
            "20240104".to_string(),
            Some(1),
            Some(0),
            Some(1),
            None,
            None,
            Some(1),
            Some("主板".to_string()),
            Some(false),
            None,
            None,
            Some(4),
            None,
        )
        .expect_err("bad expression should fail before stock filtering");

        assert!(error.contains("表达式解析错误"), "{error}");
        assert!(!error.contains("stock_list.csv"), "{error}");
    }

    #[test]
    fn rule_expression_validation_returns_every_combo_in_enumeration_order() {
        let source_dir = temp_source_dir();
        let source_dir_str = source_dir.to_str().expect("utf8 source dir");
        prepare_validation_source_files(source_dir_str);
        write(
            PathBuf::from(source_dir_str).join("stock_concepts.csv"),
            "ts_code,c1,c2,c3,concept\n",
        )
        .expect("write stock_concepts.csv");
        write(
            PathBuf::from(source_dir_str).join("score_rule.toml"),
            r#"
    version = 1

    [[scene]]
    name = "趋势启动"
    direction = "long"
    observe_threshold = 1.0
    trigger_threshold = 2.0
    confirm_threshold = 3.0
    fail_threshold = 1.0

    [[rule]]
    name = "有效策略"
    scene = "趋势启动"
    stage = "base"
    scope_windows = 1
    scope_way = "LAST"
    when = "C > O"
    points = 1.0
    explain = "test"
    "#,
        )
        .expect("write score_rule.toml");

        let data = super::run_rule_expression_validation(
            source_dir_str.to_string(),
            String::new(),
            Some("C > REF(C, N)".to_string()),
            Some("LAST".to_string()),
            Some(1),
            Some("qfq".to_string()),
            "000001.SH".to_string(),
            Some(0.5),
            Some(0.2),
            Some(0.0),
            "20240102".to_string(),
            "20240104".to_string(),
            Some(1),
            Some(0),
            Some(1),
            None,
            Some(vec![
                crate::statistics::validation::RuleValidationUnknownConfig {
                    name: "N".to_string(),
                    start: 1.0,
                    end: 2.0,
                    step: 1.0,
                },
            ]),
            Some(1),
            None,
            Some(false),
            None,
            None,
            Some(2),
            None,
        )
        .expect("rule expression validation");

        assert_eq!(data.combo_results.len(), 2);
        assert_eq!(data.core_rule_names, Vec::<String>::new());
        for (index, combo) in data.combo_results.iter().enumerate() {
            assert_eq!(combo.unknown_values.len(), 1);
            assert_eq!(combo.unknown_values[0].value, 1.0 + index as f64);
            assert!(combo.incremental.folds.is_empty());
        }

        let missing_core_error = super::run_rule_expression_validation(
            source_dir_str.to_string(),
            String::new(),
            Some("C > O".to_string()),
            Some("LAST".to_string()),
            Some(1),
            Some("qfq".to_string()),
            "000001.SH".to_string(),
            Some(0.5),
            Some(0.2),
            Some(0.0),
            "20240102".to_string(),
            "20240104".to_string(),
            Some(1),
            Some(0),
            Some(1),
            None,
            None,
            Some(1),
            None,
            Some(false),
            None,
            None,
            Some(2),
            Some(vec!["不存在的核心策略".to_string()]),
        )
        .expect_err("missing core strategy should fail");
        assert!(
            missing_core_error.contains("不存在的核心策略")
                && missing_core_error.contains("结果库区间内没有触发记录"),
            "{missing_core_error}"
        );
        let _ = std::fs::remove_dir_all(&source_dir);
    }
}
