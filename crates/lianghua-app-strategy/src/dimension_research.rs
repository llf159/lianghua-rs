use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use duckdb::{AccessMode, Config, Connection, params, params_from_iter};
use lianghua_backtest::simulate::{
    dimension::{
        SignalPairMoments, calc_distance_correlation, calc_linear_orthogonal_diagnostics,
        calc_signal_pair_metrics,
    },
    fp_utils::{calc_newey_west_t_value, mean, pearson_corr, spearman_corr},
    rule::{RuleLayerConfig, calc_all_rule_layer_metrics_from_db},
};
use serde::Serialize;

use crate::data::{load_trade_date_list, result_db_path, source_db_path};

const MAX_RESEARCH_STRATEGY_COUNT: usize = 20;
const DEFAULT_NONLINEAR_SAMPLE_LIMIT: usize = 512;
const MAX_NONLINEAR_SAMPLE_LIMIT: usize = 1024;
const DEFAULT_RIDGE_LAMBDA: f64 = 1e-6;
const DEFAULT_HOLDING_PERIOD: usize = 5;
const MAX_HOLDING_PERIOD: usize = 60;
const RETURN_MIN_SAMPLES_PER_DAY: usize = 5;
const RETURN_MIN_LISTED_TRADE_DAYS: usize = 60;
const RETURN_RIDGE_LAMBDA: f64 = 0.1;
const RETURN_MIN_TRAIN_SAMPLES: usize = 30;
const RETURN_MIN_TRAIN_SAMPLES_PER_PREDICTOR: usize = 5;
const RETURN_MIN_TEST_SAMPLES: usize = 20;
const OOS_TRAIN_RATIO: f64 = 0.7;
const RETURN_STOCK_ADJ_TYPE: &str = "qfq";
const RETURN_INDEX_TS_CODES: [&str; 2] = ["000300.SH", "399300.SZ"];
const RETURN_INDEX_BETA: f64 = 0.5;
const RETURN_CONCEPT_BETA: f64 = 0.2;
const RETURN_INDUSTRY_BETA: f64 = 0.0;
const VARIANCE_EPS: f64 = 1e-12;
const STYLE_DIMENSIONS: [(&str, &str); 8] = [
    ("direction_reaction", "方向反应"),
    ("entry_shape", "入场形态"),
    ("time_scale", "时间尺度"),
    ("price_position", "价格位置"),
    ("volatility_jump", "波动与跳跃"),
    ("liquidity", "量能与流动性"),
    ("market_regime", "市场状态依赖"),
    ("return_shape", "收益形态"),
];

#[derive(Debug, Clone, Serialize)]
pub struct StrategyDimensionRuleOption {
    pub rule_name: String,
    pub trigger_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyDimensionResearchDefaultsData {
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub rule_options: Vec<StrategyDimensionRuleOption>,
    pub max_strategy_count: usize,
    pub default_nonlinear_sample_limit: usize,
    pub max_nonlinear_sample_limit: usize,
    pub default_ridge_lambda: f64,
    pub default_holding_period: usize,
    pub max_holding_period: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyDimensionRuleSummary {
    pub rule_name: String,
    pub trigger_count: usize,
    pub coverage: Option<f64>,
    pub score_mean_with_zeros: Option<f64>,
    pub score_std_with_zeros: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyDimensionPairMetrics {
    pub left_rule_name: String,
    pub right_rule_name: String,
    pub joint_trigger_count: usize,
    pub union_trigger_count: usize,
    pub jaccard: Option<f64>,
    pub phi: Option<f64>,
    pub score_pearson_with_zeros: Option<f64>,
    pub score_spearman_daily_mean: Option<f64>,
    pub distance_correlation_daily_mean: Option<f64>,
    pub nonlinear_sample_count: usize,
    pub return_pearson: Option<f64>,
    pub return_shared_day_count: usize,
    pub style_distance: Option<f64>,
    pub style_shared_dimension_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyDimensionReturnSummary {
    pub rule_name: String,
    pub valid_day_count: usize,
    pub train_day_count: usize,
    pub test_day_count: usize,
    pub avg_residual_return: Option<f64>,
    pub hac_t_value: Option<f64>,
    pub train_avg_residual_return: Option<f64>,
    pub test_avg_residual_return: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyDimensionReturnIncrement {
    pub rule_name: String,
    pub basis_coefficients: Vec<StrategyDimensionBasisCoefficient>,
    pub train_sample_count: usize,
    pub test_sample_count: usize,
    pub test_incremental_mean: Option<f64>,
    pub test_incremental_hac_t_value: Option<f64>,
    pub test_incremental_positive_ratio: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyDimensionStyleValue {
    pub key: String,
    pub label: String,
    pub value: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyDimensionStyleExposure {
    pub rule_name: String,
    pub sample_count: usize,
    pub dimensions: Vec<StrategyDimensionStyleValue>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyDimensionBasisCoefficient {
    pub rule_name: String,
    pub coefficient: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyDimensionOrthogonalDiagnostic {
    pub rule_name: String,
    pub basis_coefficients: Vec<StrategyDimensionBasisCoefficient>,
    pub explained_variance_ratio: Option<f64>,
    pub residual_variance_ratio: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyDimensionResearchData {
    pub start_date: String,
    pub end_date: String,
    pub universe_sample_count: usize,
    pub nonlinear_sample_limit: usize,
    pub ridge_lambda: f64,
    pub score_missing_value: f64,
    pub nonlinear_sample_scope: String,
    pub orthogonal_order_sensitive: bool,
    pub strategies: Vec<StrategyDimensionRuleSummary>,
    pub pair_metrics: Vec<StrategyDimensionPairMetrics>,
    pub orthogonal_diagnostics: Vec<StrategyDimensionOrthogonalDiagnostic>,
    pub holding_period: usize,
    pub return_min_samples_per_day: usize,
    pub return_min_listed_trade_days: usize,
    pub return_ridge_lambda: f64,
    pub return_min_train_samples: usize,
    pub return_min_train_samples_per_predictor: usize,
    pub return_min_test_samples: usize,
    pub return_stock_adj_type: String,
    pub return_index_ts_code: String,
    pub return_index_beta: f64,
    pub return_concept_beta: f64,
    pub return_industry_beta: f64,
    pub oos_train_ratio: f64,
    pub oos_test_start_date: Option<String>,
    pub return_summaries: Vec<StrategyDimensionReturnSummary>,
    pub return_increments: Vec<StrategyDimensionReturnIncrement>,
    pub style_exposures: Vec<StrategyDimensionStyleExposure>,
    pub pending_layers: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
struct RuleMoments {
    trigger_count: usize,
    score_sum: f64,
    score_square_sum: f64,
}

pub fn get_strategy_dimension_research_defaults(
    source_path: String,
) -> Result<StrategyDimensionResearchDefaultsData, String> {
    let connection = open_result_database(&source_path)?;
    let (start_date, end_date) = connection
        .query_row(
            "SELECT MIN(trade_date), MAX(trade_date) FROM score_summary",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|error| format!("读取结果库日期范围失败:{error}"))?;
    let mut statement = connection
        .prepare(
            "SELECT rule_name, COUNT(*) AS trigger_count
             FROM rule_details
             WHERE rule_name IS NOT NULL AND TRIM(rule_name) <> ''
             GROUP BY rule_name
             ORDER BY rule_name",
        )
        .map_err(|error| format!("准备结果库规则列表失败:{error}"))?;
    let mut rows = statement
        .query([])
        .map_err(|error| format!("查询结果库规则列表失败:{error}"))?;
    let mut rule_options = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|error| format!("读取结果库规则列表失败:{error}"))?
    {
        rule_options.push(StrategyDimensionRuleOption {
            rule_name: row
                .get(0)
                .map_err(|error| format!("读取规则名称失败:{error}"))?,
            trigger_count: read_usize(row.get::<_, i64>(1), "规则触发数")?,
        });
    }

    Ok(StrategyDimensionResearchDefaultsData {
        start_date,
        end_date,
        rule_options,
        max_strategy_count: MAX_RESEARCH_STRATEGY_COUNT,
        default_nonlinear_sample_limit: DEFAULT_NONLINEAR_SAMPLE_LIMIT,
        max_nonlinear_sample_limit: MAX_NONLINEAR_SAMPLE_LIMIT,
        default_ridge_lambda: DEFAULT_RIDGE_LAMBDA,
        default_holding_period: DEFAULT_HOLDING_PERIOD,
        max_holding_period: MAX_HOLDING_PERIOD,
    })
}

pub fn run_strategy_dimension_research(
    source_path: String,
    start_date: String,
    end_date: String,
    rule_names: Vec<String>,
    nonlinear_sample_limit: Option<usize>,
    ridge_lambda: Option<f64>,
    holding_period: Option<usize>,
) -> Result<StrategyDimensionResearchData, String> {
    validate_date_range(&start_date, &end_date)?;
    let rule_names = normalize_rule_names(rule_names)?;
    let nonlinear_sample_limit = nonlinear_sample_limit.unwrap_or(DEFAULT_NONLINEAR_SAMPLE_LIMIT);
    if !(3..=MAX_NONLINEAR_SAMPLE_LIMIT).contains(&nonlinear_sample_limit) {
        return Err(format!(
            "非线性样本上限必须在3..={MAX_NONLINEAR_SAMPLE_LIMIT}之间"
        ));
    }
    let ridge_lambda = ridge_lambda.unwrap_or(DEFAULT_RIDGE_LAMBDA);
    if !ridge_lambda.is_finite() || ridge_lambda < 0.0 {
        return Err("岭正则系数必须是有限的非负数".to_string());
    }
    let holding_period = holding_period.unwrap_or(DEFAULT_HOLDING_PERIOD);
    if !(1..=MAX_HOLDING_PERIOD).contains(&holding_period) {
        return Err(format!("持有交易日必须在1..={MAX_HOLDING_PERIOD}之间"));
    }

    let connection = open_result_database(&source_path)?;
    let universe_sample_count = read_usize(
        connection.query_row(
            "SELECT COUNT(*) FROM score_summary WHERE trade_date >= ? AND trade_date <= ?",
            params![&start_date, &end_date],
            |row| row.get::<_, i64>(0),
        ),
        "结果库评分样本数",
    )?;
    if universe_sample_count < 2 {
        return Err("所选日期区间的结果库评分样本不足2条".to_string());
    }

    let moments_by_rule = load_rule_moments(&connection, &start_date, &end_date, &rule_names)?;
    let missing_rules = rule_names
        .iter()
        .filter(|rule_name| !moments_by_rule.contains_key(*rule_name))
        .cloned()
        .collect::<Vec<_>>();
    if !missing_rules.is_empty() {
        return Err(format!(
            "所选区间没有以下规则的有效触发分数:{}",
            missing_rules.join("、")
        ));
    }
    if let Some((rule_name, moments)) = moments_by_rule
        .iter()
        .find(|(_, moments)| moments.trigger_count > universe_sample_count)
    {
        return Err(format!(
            "结果库口径不一致:规则{rule_name}触发数{}超过评分宇宙样本数{universe_sample_count}",
            moments.trigger_count
        ));
    }

    let strategies = rule_names
        .iter()
        .map(|rule_name| {
            let moments = moments_by_rule[rule_name];
            let sample_count = universe_sample_count as f64;
            let mean = moments.score_sum / sample_count;
            let variance = (moments.score_square_sum / sample_count - mean * mean).max(0.0);
            StrategyDimensionRuleSummary {
                rule_name: rule_name.clone(),
                trigger_count: moments.trigger_count,
                coverage: Some(moments.trigger_count as f64 / sample_count),
                score_mean_with_zeros: Some(mean),
                score_std_with_zeros: (variance > VARIANCE_EPS).then_some(variance.sqrt()),
            }
        })
        .collect::<Vec<_>>();

    let mut correlation_matrix = vec![vec![0.0; rule_names.len()]; rule_names.len()];
    for (index, strategy) in strategies.iter().enumerate() {
        if strategy.score_std_with_zeros.is_some() {
            correlation_matrix[index][index] = 1.0;
        }
    }

    let return_layer = load_return_layer(
        &source_path,
        &start_date,
        &end_date,
        &rule_names,
        holding_period,
    )?;
    let mut pair_metrics = Vec::with_capacity(rule_names.len() * (rule_names.len() - 1) / 2);
    let pair_cross_moments =
        load_pair_cross_moments(&connection, &start_date, &end_date, &rule_names)?;
    let (daily_score_means, nonlinear_sample_count) = load_daily_score_means(
        &connection,
        &start_date,
        &end_date,
        &rule_names,
        nonlinear_sample_limit,
    )?;

    for left_index in 0..rule_names.len() {
        for right_index in (left_index + 1)..rule_names.len() {
            let left_rule_name = &rule_names[left_index];
            let right_rule_name = &rule_names[right_index];
            let pair_key = if left_rule_name < right_rule_name {
                (left_rule_name.clone(), right_rule_name.clone())
            } else {
                (right_rule_name.clone(), left_rule_name.clone())
            };
            let (joint_trigger_count, cross_sum) = pair_cross_moments
                .get(&pair_key)
                .copied()
                .unwrap_or((0, 0.0));
            let left_moments = moments_by_rule[left_rule_name];
            let right_moments = moments_by_rule[right_rule_name];
            if joint_trigger_count > left_moments.trigger_count
                || joint_trigger_count > right_moments.trigger_count
            {
                return Err(format!(
                    "结果库口径不一致:{left_rule_name}/{right_rule_name}共同触发数超过单规则触发数"
                ));
            }
            let metrics = calc_signal_pair_metrics(SignalPairMoments {
                universe_count: universe_sample_count,
                left_trigger_count: left_moments.trigger_count,
                right_trigger_count: right_moments.trigger_count,
                joint_trigger_count,
                left_sum: left_moments.score_sum,
                right_sum: right_moments.score_sum,
                left_square_sum: left_moments.score_square_sum,
                right_square_sum: right_moments.score_square_sum,
                cross_sum,
            });
            if let Some(value) = metrics.score_pearson {
                correlation_matrix[left_index][right_index] = value;
                correlation_matrix[right_index][left_index] = value;
            }

            let left_daily_means = &daily_score_means[left_rule_name];
            let right_daily_means = &daily_score_means[right_rule_name];
            pair_metrics.push(StrategyDimensionPairMetrics {
                left_rule_name: left_rule_name.clone(),
                right_rule_name: right_rule_name.clone(),
                joint_trigger_count,
                union_trigger_count: metrics.union_trigger_count,
                jaccard: metrics.jaccard,
                phi: metrics.phi,
                score_pearson_with_zeros: metrics.score_pearson,
                score_spearman_daily_mean: spearman_corr(left_daily_means, right_daily_means),
                distance_correlation_daily_mean: calc_distance_correlation(
                    left_daily_means,
                    right_daily_means,
                ),
                nonlinear_sample_count,
                return_pearson: return_layer
                    .pair_correlations
                    .get(&pair_key)
                    .and_then(|value| value.0),
                return_shared_day_count: return_layer
                    .pair_correlations
                    .get(&pair_key)
                    .map(|value| value.1)
                    .unwrap_or(0),
                style_distance: return_layer
                    .style_distances
                    .get(&pair_key)
                    .and_then(|value| value.0),
                style_shared_dimension_count: return_layer
                    .style_distances
                    .get(&pair_key)
                    .map(|value| value.1)
                    .unwrap_or(0),
            });
        }
    }

    let orthogonal = calc_linear_orthogonal_diagnostics(&correlation_matrix, ridge_lambda)?;
    let orthogonal_diagnostics = orthogonal
        .into_iter()
        .enumerate()
        .map(
            |(target_index, diagnostic)| StrategyDimensionOrthogonalDiagnostic {
                rule_name: rule_names[target_index].clone(),
                basis_coefficients: diagnostic
                    .basis_coefficients
                    .into_iter()
                    .enumerate()
                    .map(
                        |(basis_index, coefficient)| StrategyDimensionBasisCoefficient {
                            rule_name: rule_names[basis_index].clone(),
                            coefficient,
                        },
                    )
                    .collect(),
                explained_variance_ratio: diagnostic.explained_variance_ratio,
                residual_variance_ratio: diagnostic.residual_variance_ratio,
            },
        )
        .collect();

    Ok(StrategyDimensionResearchData {
        start_date,
        end_date,
        universe_sample_count,
        nonlinear_sample_limit,
        ridge_lambda,
        score_missing_value: 0.0,
        nonlinear_sample_scope: "daily_cross_section_score_mean".to_string(),
        orthogonal_order_sensitive: true,
        strategies,
        pair_metrics,
        orthogonal_diagnostics,
        holding_period,
        return_min_samples_per_day: RETURN_MIN_SAMPLES_PER_DAY,
        return_min_listed_trade_days: RETURN_MIN_LISTED_TRADE_DAYS,
        return_ridge_lambda: RETURN_RIDGE_LAMBDA,
        return_min_train_samples: RETURN_MIN_TRAIN_SAMPLES,
        return_min_train_samples_per_predictor: RETURN_MIN_TRAIN_SAMPLES_PER_PREDICTOR,
        return_min_test_samples: RETURN_MIN_TEST_SAMPLES,
        return_stock_adj_type: RETURN_STOCK_ADJ_TYPE.to_string(),
        return_index_ts_code: return_layer.index_ts_code,
        return_index_beta: RETURN_INDEX_BETA,
        return_concept_beta: RETURN_CONCEPT_BETA,
        return_industry_beta: RETURN_INDUSTRY_BETA,
        oos_train_ratio: OOS_TRAIN_RATIO,
        oos_test_start_date: return_layer.oos_test_start_date,
        return_summaries: return_layer.summaries,
        return_increments: return_layer.increments,
        style_exposures: return_layer.style_exposures,
        pending_layers: Vec::new(),
    })
}

struct ReturnLayerData {
    index_ts_code: String,
    pair_correlations: HashMap<(String, String), (Option<f64>, usize)>,
    oos_test_start_date: Option<String>,
    summaries: Vec<StrategyDimensionReturnSummary>,
    increments: Vec<StrategyDimensionReturnIncrement>,
    style_exposures: Vec<StrategyDimensionStyleExposure>,
    style_distances: HashMap<(String, String), (Option<f64>, usize)>,
}

fn load_return_layer(
    source_path: &str,
    start_date: &str,
    end_date: &str,
    rule_names: &[String],
    holding_period: usize,
) -> Result<ReturnLayerData, String> {
    let database_path = source_db_path(source_path);
    if !database_path.is_file() {
        return Err(format!("原始行情库不存在:{}", database_path.display()));
    }
    let database_path_text = database_path
        .to_str()
        .ok_or_else(|| "原始行情库路径不是有效UTF-8".to_string())?;
    let config = Config::default()
        .access_mode(AccessMode::ReadOnly)
        .map_err(|error| format!("配置原始行情库只读模式失败:{error}"))?;
    let source_connection = Connection::open_with_flags(database_path_text, config)
        .map_err(|error| format!("打开原始行情库失败:{database_path_text}:{error}"))?;
    let layer_config = RuleLayerConfig {
        min_samples_per_day: RETURN_MIN_SAMPLES_PER_DAY,
        backtest_period: holding_period,
        min_listed_trade_days: RETURN_MIN_LISTED_TRADE_DAYS,
    };
    let index_ts_code = source_connection
        .query_row(
            "SELECT ts_code
             FROM stock_data
             WHERE adj_type = 'ind' AND trade_date >= ? AND trade_date <= ?
               AND ts_code IN ('000300.SH', '399300.SZ')
               AND TRY_CAST(pct_chg AS DOUBLE) IS NOT NULL
             GROUP BY ts_code
             ORDER BY COUNT(*) DESC,
                      CASE ts_code WHEN '000300.SH' THEN 0 ELSE 1 END
             LIMIT 1",
            params![start_date, end_date],
            |row| row.get::<_, String>(0),
        )
        .map_err(|error| {
            format!(
                "研究区间缺少沪深300指数行情({}):{error}",
                RETURN_INDEX_TS_CODES.join("或")
            )
        })?;
    let metrics = calc_all_rule_layer_metrics_from_db(
        &source_connection,
        source_path,
        rule_names,
        RETURN_STOCK_ADJ_TYPE,
        &index_ts_code,
        RETURN_INDEX_BETA,
        RETURN_CONCEPT_BETA,
        RETURN_INDUSTRY_BETA,
        start_date,
        end_date,
        &layer_config,
    )?;

    let returns_by_rule = metrics
        .into_iter()
        .map(|(rule_name, metrics)| {
            let series = metrics
                .points
                .into_iter()
                .filter_map(|point| {
                    point
                        .score_weighted_residual_return
                        .filter(|value| value.is_finite())
                        .map(|value| (point.trade_date, value))
                })
                .collect::<BTreeMap<_, _>>();
            (rule_name, series)
        })
        .collect::<HashMap<_, _>>();
    let all_dates = returns_by_rule
        .values()
        .flat_map(|series| series.keys().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let test_start_index = if all_dates.len() >= 2 {
        ((all_dates.len() as f64 * OOS_TRAIN_RATIO).floor() as usize).clamp(1, all_dates.len() - 1)
    } else {
        all_dates.len()
    };
    let oos_test_start_date = all_dates.get(test_start_index).cloned();

    let mut pair_correlations = HashMap::new();
    for left_index in 0..rule_names.len() {
        for right_index in (left_index + 1)..rule_names.len() {
            let left_name = &rule_names[left_index];
            let right_name = &rule_names[right_index];
            let left_series = &returns_by_rule[left_name];
            let right_series = &returns_by_rule[right_name];
            let mut left_values = Vec::new();
            let mut right_values = Vec::new();
            for (date, left_value) in left_series {
                if let Some(right_value) = right_series.get(date) {
                    left_values.push(*left_value);
                    right_values.push(*right_value);
                }
            }
            let key = if left_name < right_name {
                (left_name.clone(), right_name.clone())
            } else {
                (right_name.clone(), left_name.clone())
            };
            pair_correlations.insert(
                key,
                (pearson_corr(&left_values, &right_values), left_values.len()),
            );
        }
    }

    let mut summaries = Vec::with_capacity(rule_names.len());
    for rule_name in rule_names {
        let series = &returns_by_rule[rule_name];
        let all_values = series.values().copied().collect::<Vec<_>>();
        let (train_values, test_values): (Vec<_>, Vec<_>) = series.iter().partition(|(date, _)| {
            oos_test_start_date
                .as_ref()
                .is_none_or(|test_start| *date < test_start)
        });
        let train_values = train_values
            .into_iter()
            .map(|(_, value)| *value)
            .collect::<Vec<_>>();
        let test_values = test_values
            .into_iter()
            .map(|(_, value)| *value)
            .collect::<Vec<_>>();
        summaries.push(StrategyDimensionReturnSummary {
            rule_name: rule_name.clone(),
            valid_day_count: all_values.len(),
            train_day_count: train_values.len(),
            test_day_count: test_values.len(),
            avg_residual_return: mean(&all_values),
            hac_t_value: calc_newey_west_t_value(&all_values, holding_period - 1),
            train_avg_residual_return: mean(&train_values),
            test_avg_residual_return: mean(&test_values),
        });
    }

    let mut increments = Vec::with_capacity(rule_names.len());
    for target_index in 0..rule_names.len() {
        let relevant_names = &rule_names[..=target_index];
        let mut train_rows = Vec::new();
        let mut test_rows = Vec::new();
        for date in &all_dates {
            let Some(row) = relevant_names
                .iter()
                .map(|rule_name| returns_by_rule[rule_name].get(date).copied())
                .collect::<Option<Vec<_>>>()
            else {
                continue;
            };
            if oos_test_start_date
                .as_ref()
                .is_some_and(|test_start| date >= test_start)
            {
                test_rows.push(row);
            } else {
                train_rows.push(row);
            }
        }

        let column_count = relevant_names.len();
        let mut means = vec![0.0; column_count];
        let mut standard_deviations = vec![0.0; column_count];
        for column in 0..column_count {
            let values = train_rows.iter().map(|row| row[column]).collect::<Vec<_>>();
            if let Some(value) = mean(&values) {
                means[column] = value;
                standard_deviations[column] = (values
                    .iter()
                    .map(|item| (item - value) * (item - value))
                    .sum::<f64>()
                    / values.len() as f64)
                    .sqrt();
            }
        }

        let mut raw_coefficients = Vec::new();
        let mut regression_available = target_index == 0;
        let minimum_train_samples =
            RETURN_MIN_TRAIN_SAMPLES.max(target_index * RETURN_MIN_TRAIN_SAMPLES_PER_PREDICTOR);
        if target_index > 0
            && train_rows.len() >= minimum_train_samples
            && test_rows.len() >= RETURN_MIN_TEST_SAMPLES
            && standard_deviations[target_index] > VARIANCE_EPS
        {
            let mut correlation_matrix = vec![vec![0.0; column_count]; column_count];
            for row in 0..column_count {
                correlation_matrix[row][row] =
                    (standard_deviations[row] > VARIANCE_EPS) as u8 as f64;
                for column in (row + 1)..column_count {
                    let left = train_rows.iter().map(|item| item[row]).collect::<Vec<_>>();
                    let right = train_rows
                        .iter()
                        .map(|item| item[column])
                        .collect::<Vec<_>>();
                    let correlation = pearson_corr(&left, &right).unwrap_or(0.0);
                    correlation_matrix[row][column] = correlation;
                    correlation_matrix[column][row] = correlation;
                }
            }
            let diagnostic =
                calc_linear_orthogonal_diagnostics(&correlation_matrix, RETURN_RIDGE_LAMBDA)?
                    .pop()
                    .expect("非空相关矩阵必须返回目标诊断");
            if diagnostic.residual_variance_ratio.is_some() {
                regression_available = true;
                raw_coefficients = diagnostic
                    .basis_coefficients
                    .into_iter()
                    .enumerate()
                    .map(|(index, coefficient)| {
                        if standard_deviations[index] > VARIANCE_EPS {
                            coefficient * standard_deviations[target_index]
                                / standard_deviations[index]
                        } else {
                            0.0
                        }
                    })
                    .collect();
            }
        }

        let test_residuals = if regression_available {
            test_rows
                .iter()
                .map(|row| {
                    let fitted = raw_coefficients
                        .iter()
                        .enumerate()
                        .map(|(index, coefficient)| coefficient * row[index])
                        .sum::<f64>();
                    row[target_index] - fitted
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let positive_count = test_residuals.iter().filter(|value| **value > 0.0).count();
        increments.push(StrategyDimensionReturnIncrement {
            rule_name: rule_names[target_index].clone(),
            basis_coefficients: raw_coefficients
                .into_iter()
                .enumerate()
                .map(|(index, coefficient)| StrategyDimensionBasisCoefficient {
                    rule_name: rule_names[index].clone(),
                    coefficient,
                })
                .collect(),
            train_sample_count: train_rows.len(),
            test_sample_count: test_rows.len(),
            test_incremental_mean: mean(&test_residuals),
            test_incremental_hac_t_value: calc_newey_west_t_value(
                &test_residuals,
                holding_period - 1,
            ),
            test_incremental_positive_ratio: (!test_residuals.is_empty())
                .then_some(positive_count as f64 / test_residuals.len() as f64),
        });
    }

    let (style_exposures, style_distances) = load_style_layer(
        &source_connection,
        source_path,
        start_date,
        end_date,
        rule_names,
        &returns_by_rule,
        &index_ts_code,
    )?;

    Ok(ReturnLayerData {
        index_ts_code,
        pair_correlations,
        oos_test_start_date,
        summaries,
        increments,
        style_exposures,
        style_distances,
    })
}

fn load_style_layer(
    source_connection: &Connection,
    source_path: &str,
    start_date: &str,
    end_date: &str,
    rule_names: &[String],
    returns_by_rule: &HashMap<String, BTreeMap<String, f64>>,
    index_ts_code: &str,
) -> Result<
    (
        Vec<StrategyDimensionStyleExposure>,
        HashMap<(String, String), (Option<f64>, usize)>,
    ),
    String,
> {
    let result_path = result_db_path(source_path);
    let result_path = result_path
        .to_str()
        .ok_or_else(|| "结果库路径不是有效UTF-8".to_string())?
        .replace('\'', "''");
    source_connection
        .execute_batch(&format!(
            "ATTACH '{result_path}' AS dimension_result (READ_ONLY)"
        ))
        .map_err(|error| format!("挂载结果库以计算风格暴露失败:{error}"))?;

    let trade_dates = load_trade_date_list(source_path)?;
    let start_index = trade_dates.partition_point(|date| date.as_str() < start_date);
    let warmup_start_date = trade_dates
        .get(start_index.saturating_sub(60))
        .map(String::as_str)
        .unwrap_or(start_date);
    let placeholders = std::iter::repeat_n("?", rule_names.len())
        .collect::<Vec<_>>()
        .join(",");
    let feature_sql = format!(
        "WITH history AS (
             SELECT ts_code, trade_date,
                    TRY_CAST(close AS DOUBLE) AS close_price,
                    TRY_CAST(high AS DOUBLE) AS high_price,
                    TRY_CAST(low AS DOUBLE) AS low_price,
                    ABS(TRY_CAST(pct_chg AS DOUBLE)) AS abs_pct,
                    TRY_CAST(amount AS DOUBLE) AS amount_value,
                    LAG(TRY_CAST(close AS DOUBLE), 5) OVER stock_window AS close_lag_5,
                    MAX(TRY_CAST(high AS DOUBLE)) OVER (
                        PARTITION BY ts_code ORDER BY trade_date
                        ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                    ) AS prior_high_20,
                    MIN(TRY_CAST(low AS DOUBLE)) OVER (
                        PARTITION BY ts_code ORDER BY trade_date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) AS low_60,
                    MAX(TRY_CAST(high AS DOUBLE)) OVER (
                        PARTITION BY ts_code ORDER BY trade_date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) AS high_60,
                    AVG(ABS(TRY_CAST(pct_chg AS DOUBLE))) OVER (
                        PARTITION BY ts_code ORDER BY trade_date
                        ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                    ) AS prior_abs_pct_20
             FROM stock_data
             WHERE adj_type = '{RETURN_STOCK_ADJ_TYPE}'
               AND trade_date >= ? AND trade_date <= ?
             WINDOW stock_window AS (PARTITION BY ts_code ORDER BY trade_date)
         ), features AS (
             SELECT history.ts_code, history.trade_date,
                    close_price / close_lag_5 - 1.0 AS direction_raw,
                    close_price / prior_high_20 - 1.0 AS entry_raw,
                    (close_price - low_60) / NULLIF(high_60 - low_60, 0.0) AS position_raw,
                    abs_pct / NULLIF(prior_abs_pct_20, 0.0) AS volatility_raw,
                    LN(amount_value) AS liquidity_raw
             FROM history
             INNER JOIN dimension_result.main.score_summary AS summary
               ON summary.ts_code = history.ts_code
              AND summary.trade_date = history.trade_date
             WHERE history.trade_date >= ? AND history.trade_date <= ?
               AND close_price > 0.0 AND close_lag_5 > 0.0 AND prior_high_20 > 0.0
               AND high_60 > low_60 AND prior_abs_pct_20 > 0.0 AND amount_value > 0.0
         ), ranked AS (
             SELECT ts_code, trade_date,
                    2.0 * PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY direction_raw) - 1.0 AS direction_exposure,
                    2.0 * PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY entry_raw) - 1.0 AS entry_exposure,
                    2.0 * PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY position_raw) - 1.0 AS position_exposure,
                    2.0 * PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY volatility_raw) - 1.0 AS volatility_exposure,
                    2.0 * PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY liquidity_raw) - 1.0 AS liquidity_exposure
             FROM features
         )
         , daily_exposure AS (
             SELECT details.rule_name, ranked.trade_date, COUNT(*) AS sample_count,
                    SUM(details.rule_score * direction_exposure)
                        / NULLIF(SUM(ABS(details.rule_score)), 0.0) AS direction_exposure,
                    SUM(details.rule_score * entry_exposure)
                        / NULLIF(SUM(ABS(details.rule_score)), 0.0) AS entry_exposure,
                    SUM(details.rule_score * position_exposure)
                        / NULLIF(SUM(ABS(details.rule_score)), 0.0) AS position_exposure,
                    SUM(details.rule_score * volatility_exposure)
                        / NULLIF(SUM(ABS(details.rule_score)), 0.0) AS volatility_exposure,
                    SUM(details.rule_score * liquidity_exposure)
                        / NULLIF(SUM(ABS(details.rule_score)), 0.0) AS liquidity_exposure
             FROM ranked
             INNER JOIN dimension_result.main.rule_details AS details
               ON details.ts_code = ranked.ts_code AND details.trade_date = ranked.trade_date
             WHERE details.rule_name IN ({placeholders})
               AND isfinite(details.rule_score) AND ABS(details.rule_score) > {VARIANCE_EPS}
             GROUP BY details.rule_name, ranked.trade_date
         )
         SELECT rule_name, SUM(sample_count),
                AVG(direction_exposure), AVG(entry_exposure), AVG(position_exposure),
                AVG(volatility_exposure), AVG(liquidity_exposure)
         FROM daily_exposure GROUP BY rule_name"
    );
    let mut feature_params = vec![
        warmup_start_date.to_string(),
        end_date.to_string(),
        start_date.to_string(),
        end_date.to_string(),
    ];
    feature_params.extend(rule_names.iter().cloned());
    let mut style_values = rule_names
        .iter()
        .map(|rule_name| (rule_name.clone(), ([None; STYLE_DIMENSIONS.len()], 0usize)))
        .collect::<HashMap<_, _>>();
    let mut statement = source_connection
        .prepare(&feature_sql)
        .map_err(|error| format!("准备八维风格特征查询失败:{error}"))?;
    let mut rows = statement
        .query(params_from_iter(feature_params.iter()))
        .map_err(|error| format!("查询八维风格特征失败:{error}"))?;
    while let Some(row) = rows
        .next()
        .map_err(|error| format!("读取八维风格特征失败:{error}"))?
    {
        let rule_name: String = row
            .get(0)
            .map_err(|error| format!("读取风格规则名失败:{error}"))?;
        if let Some((values, sample_count)) = style_values.get_mut(&rule_name) {
            *sample_count = read_usize(row.get::<_, i64>(1), "风格样本数")?;
            values[0] = row
                .get(2)
                .map_err(|error| format!("读取方向暴露失败:{error}"))?;
            values[1] = row
                .get(3)
                .map_err(|error| format!("读取入场暴露失败:{error}"))?;
            values[3] = row
                .get(4)
                .map_err(|error| format!("读取位置暴露失败:{error}"))?;
            values[4] = row
                .get(5)
                .map_err(|error| format!("读取波动暴露失败:{error}"))?;
            values[5] = row
                .get(6)
                .map_err(|error| format!("读取流动性暴露失败:{error}"))?;
        }
    }
    drop(rows);
    drop(statement);

    let persistence_sql = format!(
        "WITH dates AS (
             SELECT trade_date, DENSE_RANK() OVER (ORDER BY trade_date) AS date_index
             FROM (SELECT DISTINCT trade_date FROM dimension_result.main.score_summary
                   WHERE trade_date >= ? AND trade_date <= ?)
         ), universe AS (
             SELECT summary.ts_code, summary.trade_date, dates.date_index
             FROM dimension_result.main.score_summary AS summary
             INNER JOIN dates USING (trade_date)
         ), transitions AS (
             SELECT current.ts_code, current.trade_date, previous.trade_date AS previous_date
             FROM universe AS current
             INNER JOIN universe AS previous
               ON previous.ts_code = current.ts_code
              AND previous.date_index = current.date_index - 1
         ), selected_rules AS (
             SELECT DISTINCT rule_name FROM dimension_result.main.rule_details
             WHERE rule_name IN ({placeholders})
         )
         SELECT selected_rules.rule_name,
                CORR(COALESCE(previous_score.rule_score, 0.0),
                     COALESCE(current_score.rule_score, 0.0))
         FROM selected_rules CROSS JOIN transitions
         LEFT JOIN dimension_result.main.rule_details AS previous_score
           ON previous_score.rule_name = selected_rules.rule_name
          AND previous_score.ts_code = transitions.ts_code
          AND previous_score.trade_date = transitions.previous_date
         LEFT JOIN dimension_result.main.rule_details AS current_score
           ON current_score.rule_name = selected_rules.rule_name
          AND current_score.ts_code = transitions.ts_code
          AND current_score.trade_date = transitions.trade_date
         GROUP BY selected_rules.rule_name"
    );
    let mut persistence_params = vec![start_date.to_string(), end_date.to_string()];
    persistence_params.extend(rule_names.iter().cloned());
    let mut statement = source_connection
        .prepare(&persistence_sql)
        .map_err(|error| format!("准备策略持续性查询失败:{error}"))?;
    let mut rows = statement
        .query(params_from_iter(persistence_params.iter()))
        .map_err(|error| format!("查询策略持续性失败:{error}"))?;
    while let Some(row) = rows
        .next()
        .map_err(|error| format!("读取策略持续性失败:{error}"))?
    {
        let rule_name: String = row
            .get(0)
            .map_err(|error| format!("读取持续性规则名失败:{error}"))?;
        if let Some((values, _)) = style_values.get_mut(&rule_name) {
            values[2] = row
                .get::<_, Option<f64>>(1)
                .map_err(|error| format!("读取时间尺度暴露失败:{error}"))?
                .map(|value| value.clamp(-1.0, 1.0));
        }
    }
    drop(rows);
    drop(statement);

    let market_sql = format!(
        "WITH market AS (
             SELECT trade_date, TRY_CAST(pct_chg AS DOUBLE) AS market_return
             FROM stock_data
             WHERE ts_code = ? AND adj_type = 'ind' AND trade_date >= ? AND trade_date <= ?
         ), universe AS (
             SELECT trade_date, COUNT(*) AS universe_count
             FROM dimension_result.main.score_summary
             WHERE trade_date >= ? AND trade_date <= ? GROUP BY trade_date
         ), selected_rules AS (
             SELECT DISTINCT rule_name FROM dimension_result.main.rule_details
             WHERE rule_name IN ({placeholders})
         ), daily_rule AS (
             SELECT rule_name, trade_date, SUM(rule_score) AS score_sum
             FROM dimension_result.main.rule_details
             WHERE rule_name IN ({placeholders}) AND trade_date >= ? AND trade_date <= ?
             GROUP BY rule_name, trade_date
         )
         SELECT selected_rules.rule_name,
                CORR(COALESCE(daily_rule.score_sum, 0)::DOUBLE / universe.universe_count,
                     market.market_return)
         FROM selected_rules CROSS JOIN market
         INNER JOIN universe USING (trade_date)
         LEFT JOIN daily_rule ON daily_rule.rule_name = selected_rules.rule_name
                             AND daily_rule.trade_date = market.trade_date
         WHERE isfinite(market.market_return)
         GROUP BY selected_rules.rule_name"
    );
    let mut market_params = vec![
        index_ts_code.to_string(),
        start_date.to_string(),
        end_date.to_string(),
        start_date.to_string(),
        end_date.to_string(),
    ];
    market_params.extend(rule_names.iter().cloned());
    market_params.extend(rule_names.iter().cloned());
    market_params.push(start_date.to_string());
    market_params.push(end_date.to_string());
    let mut statement = source_connection
        .prepare(&market_sql)
        .map_err(|error| format!("准备市场状态依赖查询失败:{error}"))?;
    let mut rows = statement
        .query(params_from_iter(market_params.iter()))
        .map_err(|error| format!("查询市场状态依赖失败:{error}"))?;
    while let Some(row) = rows
        .next()
        .map_err(|error| format!("读取市场状态依赖失败:{error}"))?
    {
        let rule_name: String = row
            .get(0)
            .map_err(|error| format!("读取市场依赖规则名失败:{error}"))?;
        if let Some((values, _)) = style_values.get_mut(&rule_name) {
            values[6] = row
                .get::<_, Option<f64>>(1)
                .map_err(|error| format!("读取市场状态依赖失败:{error}"))?
                .map(|value| value.clamp(-1.0, 1.0));
        }
    }

    for rule_name in rule_names {
        let mut positive = 0.0;
        let mut negative = 0.0;
        for value in returns_by_rule[rule_name].values() {
            if *value > 0.0 {
                positive += value;
            } else if *value < 0.0 {
                negative += value.abs();
            }
        }
        if positive + negative > VARIANCE_EPS {
            style_values
                .get_mut(rule_name)
                .expect("规则风格槽必须存在")
                .0[7] = Some(((positive - negative) / (positive + negative)).clamp(-1.0, 1.0));
        }
    }

    let style_exposures = rule_names
        .iter()
        .map(|rule_name| {
            let (values, sample_count) = style_values[rule_name];
            StrategyDimensionStyleExposure {
                rule_name: rule_name.clone(),
                sample_count,
                dimensions: STYLE_DIMENSIONS
                    .iter()
                    .enumerate()
                    .map(|(index, (key, label))| StrategyDimensionStyleValue {
                        key: (*key).to_string(),
                        label: (*label).to_string(),
                        value: values[index],
                    })
                    .collect(),
            }
        })
        .collect::<Vec<_>>();
    let exposure_by_rule = style_exposures
        .iter()
        .map(|exposure| (exposure.rule_name.as_str(), exposure))
        .collect::<HashMap<_, _>>();
    let mut style_distances = HashMap::new();
    for left_index in 0..rule_names.len() {
        for right_index in (left_index + 1)..rule_names.len() {
            let left_name = &rule_names[left_index];
            let right_name = &rule_names[right_index];
            let left = exposure_by_rule[left_name.as_str()];
            let right = exposure_by_rule[right_name.as_str()];
            let squared_differences = left
                .dimensions
                .iter()
                .zip(right.dimensions.iter())
                .filter_map(|(left, right)| left.value.zip(right.value))
                .map(|(left, right)| ((left - right) / 2.0).powi(2))
                .collect::<Vec<_>>();
            let shared_count = squared_differences.len();
            let distance = (!squared_differences.is_empty()).then(|| {
                (squared_differences.iter().sum::<f64>() / shared_count as f64)
                    .sqrt()
                    .clamp(0.0, 1.0)
            });
            let key = if left_name < right_name {
                (left_name.clone(), right_name.clone())
            } else {
                (right_name.clone(), left_name.clone())
            };
            style_distances.insert(key, (distance, shared_count));
        }
    }
    Ok((style_exposures, style_distances))
}

fn open_result_database(source_path: &str) -> Result<Connection, String> {
    let database_path = result_db_path(source_path);
    if !database_path.is_file() {
        return Err(format!("结果库不存在:{}", database_path.display()));
    }
    let database_path = database_path
        .to_str()
        .ok_or_else(|| "结果库路径不是有效UTF-8".to_string())?;
    let config = Config::default()
        .access_mode(AccessMode::ReadOnly)
        .map_err(|error| format!("配置结果库只读模式失败:{error}"))?;
    Connection::open_with_flags(database_path, config)
        .map_err(|error| format!("打开结果库失败:{database_path}:{error}"))
}

fn normalize_rule_names(rule_names: Vec<String>) -> Result<Vec<String>, String> {
    let mut seen = HashSet::new();
    let mut normalized = Vec::new();
    for rule_name in rule_names {
        let rule_name = rule_name.trim();
        if !rule_name.is_empty() && seen.insert(rule_name.to_string()) {
            normalized.push(rule_name.to_string());
        }
    }
    if normalized.len() < 2 {
        return Err("相关性研究至少需要选择2个不同规则".to_string());
    }
    if normalized.len() > MAX_RESEARCH_STRATEGY_COUNT {
        return Err(format!(
            "单次相关性研究最多选择{MAX_RESEARCH_STRATEGY_COUNT}个规则"
        ));
    }
    Ok(normalized)
}

fn validate_date_range(start_date: &str, end_date: &str) -> Result<(), String> {
    if ![start_date, end_date]
        .iter()
        .all(|value| value.len() == 8 && value.bytes().all(|byte| byte.is_ascii_digit()))
    {
        return Err("开始和结束日期必须使用YYYYMMDD格式".to_string());
    }
    if start_date > end_date {
        return Err("开始日期不能晚于结束日期".to_string());
    }
    Ok(())
}

fn load_rule_moments(
    connection: &Connection,
    start_date: &str,
    end_date: &str,
    rule_names: &[String],
) -> Result<HashMap<String, RuleMoments>, String> {
    let placeholders = std::iter::repeat_n("?", rule_names.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT rule_name, COUNT(*), SUM(rule_score), SUM(rule_score * rule_score)
         FROM rule_details
         WHERE rule_name IN ({placeholders})
           AND trade_date >= ? AND trade_date <= ? AND isfinite(rule_score)
         GROUP BY rule_name"
    );
    let mut query_params = rule_names.to_vec();
    query_params.push(start_date.to_string());
    query_params.push(end_date.to_string());
    let mut statement = connection
        .prepare(&sql)
        .map_err(|error| format!("准备规则统计查询失败:{error}"))?;
    let mut rows = statement
        .query(params_from_iter(query_params.iter()))
        .map_err(|error| format!("查询规则统计失败:{error}"))?;
    let mut moments_by_rule = HashMap::with_capacity(rule_names.len());
    while let Some(row) = rows
        .next()
        .map_err(|error| format!("读取规则统计失败:{error}"))?
    {
        let rule_name = row
            .get(0)
            .map_err(|error| format!("读取规则统计名称失败:{error}"))?;
        moments_by_rule.insert(
            rule_name,
            RuleMoments {
                trigger_count: read_usize(row.get::<_, i64>(1), "规则触发数")?,
                score_sum: row
                    .get(2)
                    .map_err(|error| format!("读取规则分数和失败:{error}"))?,
                score_square_sum: row
                    .get(3)
                    .map_err(|error| format!("读取规则分数平方和失败:{error}"))?,
            },
        );
    }
    Ok(moments_by_rule)
}

fn load_pair_cross_moments(
    connection: &Connection,
    start_date: &str,
    end_date: &str,
    rule_names: &[String],
) -> Result<HashMap<(String, String), (usize, f64)>, String> {
    let placeholders = std::iter::repeat_n("?", rule_names.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "WITH selected AS (
             SELECT ts_code, trade_date, rule_name, rule_score
             FROM rule_details
             WHERE rule_name IN ({placeholders})
               AND trade_date >= ? AND trade_date <= ? AND isfinite(rule_score)
         )
         SELECT left_rule.rule_name, right_rule.rule_name, COUNT(*),
                SUM(left_rule.rule_score * right_rule.rule_score)
         FROM selected AS left_rule
         INNER JOIN selected AS right_rule
           ON left_rule.ts_code = right_rule.ts_code
          AND left_rule.trade_date = right_rule.trade_date
          AND left_rule.rule_name < right_rule.rule_name
         GROUP BY left_rule.rule_name, right_rule.rule_name"
    );
    let mut query_params = rule_names.to_vec();
    query_params.push(start_date.to_string());
    query_params.push(end_date.to_string());
    let mut statement = connection
        .prepare(&sql)
        .map_err(|error| format!("准备批量规则交集查询失败:{error}"))?;
    let mut rows = statement
        .query(params_from_iter(query_params.iter()))
        .map_err(|error| format!("批量查询规则交集失败:{error}"))?;
    let mut pair_moments = HashMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|error| format!("读取批量规则交集失败:{error}"))?
    {
        let left_rule_name = row
            .get(0)
            .map_err(|error| format!("读取左规则名称失败:{error}"))?;
        let right_rule_name = row
            .get(1)
            .map_err(|error| format!("读取右规则名称失败:{error}"))?;
        pair_moments.insert(
            (left_rule_name, right_rule_name),
            (
                read_usize(row.get::<_, i64>(2), "共同触发数")?,
                row.get(3)
                    .map_err(|error| format!("读取规则分数乘积和失败:{error}"))?,
            ),
        );
    }
    Ok(pair_moments)
}

fn load_daily_score_means(
    connection: &Connection,
    start_date: &str,
    end_date: &str,
    rule_names: &[String],
    sample_limit: usize,
) -> Result<(HashMap<String, Vec<f64>>, usize), String> {
    let sampled_date_sql = format!(
        "SELECT trade_date, COUNT(*)
         FROM score_summary
         WHERE trade_date >= ? AND trade_date <= ?
         GROUP BY trade_date
         ORDER BY hash(trade_date), trade_date
         LIMIT {sample_limit}"
    );
    let mut date_statement = connection
        .prepare(&sampled_date_sql)
        .map_err(|error| format!("准备非线性日期抽样失败:{error}"))?;
    let mut date_rows = date_statement
        .query(params![start_date, end_date])
        .map_err(|error| format!("查询非线性日期样本失败:{error}"))?;
    let mut sampled_dates = HashMap::with_capacity(sample_limit);
    while let Some(row) = date_rows
        .next()
        .map_err(|error| format!("读取非线性日期样本失败:{error}"))?
    {
        let trade_date: String = row
            .get(0)
            .map_err(|error| format!("读取非线性样本日期失败:{error}"))?;
        let universe_count = read_usize(row.get::<_, i64>(1), "单日评分样本数")?;
        if universe_count > 0 {
            sampled_dates.insert(trade_date, (sampled_dates.len(), universe_count));
        }
    }

    let placeholders = std::iter::repeat_n("?", rule_names.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT rule_name, trade_date, SUM(rule_score)
         FROM rule_details
         WHERE rule_name IN ({placeholders})
           AND trade_date >= ? AND trade_date <= ? AND isfinite(rule_score)
         GROUP BY rule_name, trade_date"
    );
    let mut query_params = rule_names.to_vec();
    query_params.push(start_date.to_string());
    query_params.push(end_date.to_string());
    let mut statement = connection
        .prepare(&sql)
        .map_err(|error| format!("准备日度规则强度查询失败:{error}"))?;
    let mut rows = statement
        .query(params_from_iter(query_params.iter()))
        .map_err(|error| format!("查询日度规则强度失败:{error}"))?;
    let mut daily_score_means = rule_names
        .iter()
        .map(|rule_name| (rule_name.clone(), vec![0.0; sampled_dates.len()]))
        .collect::<HashMap<_, _>>();
    while let Some(row) = rows
        .next()
        .map_err(|error| format!("读取日度规则强度失败:{error}"))?
    {
        let rule_name: String = row
            .get(0)
            .map_err(|error| format!("读取日度规则名称失败:{error}"))?;
        let trade_date: String = row
            .get(1)
            .map_err(|error| format!("读取日度规则日期失败:{error}"))?;
        let Some((date_index, universe_count)) = sampled_dates.get(&trade_date).copied() else {
            continue;
        };
        let score_sum: f64 = row
            .get(2)
            .map_err(|error| format!("读取日度规则分数和失败:{error}"))?;
        daily_score_means
            .get_mut(&rule_name)
            .ok_or_else(|| format!("日度聚合返回了未选择的规则:{rule_name}"))?[date_index] =
            score_sum / universe_count as f64;
    }
    Ok((daily_score_means, sampled_dates.len()))
}

fn read_usize(value: Result<i64, duckdb::Error>, label: &str) -> Result<usize, String> {
    let value = value.map_err(|error| format!("读取{label}失败:{error}"))?;
    usize::try_from(value).map_err(|_| format!("{label}超出有效范围:{value}"))
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use duckdb::Connection;

    use super::{normalize_rule_names, run_strategy_dimension_research, validate_date_range};
    use crate::data::{result_db_path, scoring_store::init_result_db, source_db_path};

    #[test]
    fn research_input_keeps_rule_order_and_removes_duplicates() {
        let names = normalize_rule_names(vec![" A ".to_string(), "B".to_string(), "A".to_string()])
            .unwrap();
        assert_eq!(names, vec!["A", "B"]);
    }

    #[test]
    fn research_date_range_rejects_non_compact_dates() {
        assert!(validate_date_range("2024-01-01", "20240131").is_err());
        assert!(validate_date_range("20240201", "20240131").is_err());
    }

    #[test]
    fn result_database_drives_signal_and_orthogonal_metrics() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let source_dir = std::env::temp_dir().join(format!(
            "lianghua-dimension-research-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir_all(&source_dir).unwrap();
        fs::write(
            source_dir.join("stock_list.csv"),
            "ts_code,list_date,industry\n",
        )
        .unwrap();
        fs::write(
            source_dir.join("stock_concepts.csv"),
            "ts_code,c1,c2,c3,concept\n",
        )
        .unwrap();
        let trade_calendar = std::iter::once("cal_date".to_string())
            .chain((1..=60).map(|index| format!("202300{index:02}")))
            .chain(["20240102".to_string(), "20240103".to_string()])
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(
            source_dir.join("trade_calendar.csv"),
            format!("{trade_calendar}\n"),
        )
        .unwrap();
        let source_connection =
            Connection::open(source_db_path(source_dir.to_str().unwrap())).unwrap();
        source_connection
            .execute_batch(
                "CREATE TABLE stock_data (
                    ts_code VARCHAR,
                    trade_date VARCHAR,
                    adj_type VARCHAR,
                    pct_chg DOUBLE,
                    open DOUBLE,
                    close DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    amount DOUBLE
                 );
                 INSERT INTO stock_data
                 SELECT '000001.SZ', printf('202300%02d', value), 'qfq', 0.2,
                        9.0 + value * 0.01, 9.0 + value * 0.01,
                        9.1 + value * 0.01, 8.9 + value * 0.01, 1000.0 + value
                 FROM range(1, 61) AS days(value);
                 INSERT INTO stock_data
                 SELECT '000002.SZ', printf('202300%02d', value), 'qfq', -0.1,
                        21.0 - value * 0.01, 21.0 - value * 0.01,
                        21.1 - value * 0.01, 20.9 - value * 0.01, 2000.0 - value
                 FROM range(1, 61) AS days(value);
                 INSERT INTO stock_data VALUES
                    ('000001.SZ', '20240102', 'qfq', 0.0, 10.0, 10.0, 10.1, 9.9, 1000.0),
                    ('000001.SZ', '20240103', 'qfq', 1.0, 10.0, 10.1, 10.2, 9.9, 1100.0),
                    ('000002.SZ', '20240102', 'qfq', 0.0, 20.0, 20.0, 20.2, 19.8, 2000.0),
                    ('000002.SZ', '20240103', 'qfq', -1.0, 20.0, 19.8, 20.1, 19.7, 1900.0),
                    ('399300.SZ', '20240102', 'ind', 0.0, 100.0, 100.0, 100.0, 100.0, 0.0),
                    ('399300.SZ', '20240103', 'ind', 0.0, 100.0, 100.0, 100.0, 100.0, 0.0);",
            )
            .unwrap();
        drop(source_connection);
        let database_path = result_db_path(source_dir.to_str().unwrap());
        init_result_db(&database_path).unwrap();
        let connection = Connection::open(&database_path).unwrap();
        connection
            .execute_batch(
                "INSERT INTO score_summary VALUES
                    ('000001.SZ', '20240102', 1.0, 1),
                    ('000002.SZ', '20240102', 1.0, 2),
                    ('000001.SZ', '20240103', 1.0, 1),
                    ('000002.SZ', '20240103', 1.0, 2);
                 INSERT INTO rule_details VALUES
                    ('000001.SZ', '20240102', 'A', 1.0),
                    ('000002.SZ', '20240102', 'B', 1.0),
                    ('000001.SZ', '20240103', 'A', 2.0),
                    ('000001.SZ', '20240103', 'B', 2.0);",
            )
            .unwrap();
        drop(connection);

        let result = run_strategy_dimension_research(
            source_dir.to_string_lossy().into_owned(),
            "20240102".to_string(),
            "20240103".to_string(),
            vec!["A".to_string(), "B".to_string()],
            Some(10),
            None,
            None,
        )
        .unwrap();
        assert_eq!(result.universe_sample_count, 4);
        assert_eq!(result.pair_metrics[0].joint_trigger_count, 1);
        assert_eq!(result.pair_metrics[0].union_trigger_count, 3);
        assert_eq!(result.pair_metrics[0].jaccard, Some(1.0 / 3.0));
        assert_eq!(result.pair_metrics[0].nonlinear_sample_count, 2);
        assert!((result.pair_metrics[0].score_spearman_daily_mean.unwrap() - 1.0).abs() < 1e-12);
        assert_eq!(result.orthogonal_diagnostics.len(), 2);
        assert_eq!(result.return_index_ts_code, "399300.SZ");
        assert_eq!(result.style_exposures.len(), 2);
        assert!(result.style_exposures[0].dimensions[0].value.is_some());
        assert!(result.pair_metrics[0].style_distance.is_some());
        assert!(result.pair_metrics[0].style_shared_dimension_count >= 5);
        assert!(result.pending_layers.is_empty());

        std::fs::remove_dir_all(source_dir).unwrap();
    }
}
