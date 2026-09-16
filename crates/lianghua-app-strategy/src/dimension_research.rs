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

use crate::data::{result_db_path, source_db_path};

const MAX_RESEARCH_STRATEGY_COUNT: usize = 20;
const DEFAULT_NONLINEAR_SAMPLE_LIMIT: usize = 512;
const MAX_NONLINEAR_SAMPLE_LIMIT: usize = 1024;
const DEFAULT_RIDGE_LAMBDA: f64 = 1e-6;
const DEFAULT_HOLDING_PERIOD: usize = 5;
const MAX_HOLDING_PERIOD: usize = 60;
const RETURN_MIN_SAMPLES_PER_DAY: usize = 5;
const RETURN_MIN_LISTED_TRADE_DAYS: usize = 60;
const OOS_TRAIN_RATIO: f64 = 0.7;
const RETURN_STOCK_ADJ_TYPE: &str = "qfq";
const RETURN_INDEX_TS_CODE: &str = "000300.SH";
const RETURN_INDEX_BETA: f64 = 0.5;
const RETURN_CONCEPT_BETA: f64 = 0.2;
const RETURN_INDUSTRY_BETA: f64 = 0.0;
const VARIANCE_EPS: f64 = 1e-12;

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
    pub return_stock_adj_type: String,
    pub return_index_ts_code: String,
    pub return_index_beta: f64,
    pub return_concept_beta: f64,
    pub return_industry_beta: f64,
    pub oos_train_ratio: f64,
    pub oos_test_start_date: Option<String>,
    pub return_summaries: Vec<StrategyDimensionReturnSummary>,
    pub return_increments: Vec<StrategyDimensionReturnIncrement>,
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
        ridge_lambda,
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
        return_stock_adj_type: RETURN_STOCK_ADJ_TYPE.to_string(),
        return_index_ts_code: RETURN_INDEX_TS_CODE.to_string(),
        return_index_beta: RETURN_INDEX_BETA,
        return_concept_beta: RETURN_CONCEPT_BETA,
        return_industry_beta: RETURN_INDUSTRY_BETA,
        oos_train_ratio: OOS_TRAIN_RATIO,
        oos_test_start_date: return_layer.oos_test_start_date,
        return_summaries: return_layer.summaries,
        return_increments: return_layer.increments,
        pending_layers: vec!["结果库尚未物化八维量价风格暴露，当前不输出风格距离".to_string()],
    })
}

struct ReturnLayerData {
    pair_correlations: HashMap<(String, String), (Option<f64>, usize)>,
    oos_test_start_date: Option<String>,
    summaries: Vec<StrategyDimensionReturnSummary>,
    increments: Vec<StrategyDimensionReturnIncrement>,
}

fn load_return_layer(
    source_path: &str,
    start_date: &str,
    end_date: &str,
    rule_names: &[String],
    holding_period: usize,
    ridge_lambda: f64,
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
    let metrics = calc_all_rule_layer_metrics_from_db(
        &source_connection,
        source_path,
        rule_names,
        RETURN_STOCK_ADJ_TYPE,
        RETURN_INDEX_TS_CODE,
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
                        .avg_residual_return
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
        if target_index > 0
            && train_rows.len() >= 3
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
            let diagnostic = calc_linear_orthogonal_diagnostics(&correlation_matrix, ridge_lambda)?
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

    Ok(ReturnLayerData {
        pair_correlations,
        oos_test_start_date,
        summaries,
        increments,
    })
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
        fs::write(
            source_dir.join("trade_calendar.csv"),
            "cal_date\n20240102\n20240103\n",
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
                    close DOUBLE
                 );
                 INSERT INTO stock_data VALUES
                    ('000001.SZ', '20240102', 'qfq', 0.0, 10.0, 10.0),
                    ('000001.SZ', '20240103', 'qfq', 1.0, 10.0, 10.1),
                    ('000002.SZ', '20240102', 'qfq', 0.0, 20.0, 20.0),
                    ('000002.SZ', '20240103', 'qfq', -1.0, 20.0, 19.8),
                    ('000300.SH', '20240102', 'ind', 0.0, 100.0, 100.0),
                    ('000300.SH', '20240103', 'ind', 0.0, 100.0, 100.0);",
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

        std::fs::remove_dir_all(source_dir).unwrap();
    }
}
