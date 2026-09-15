use std::collections::{HashMap, HashSet};

use duckdb::{AccessMode, Config, Connection, params, params_from_iter};
use lianghua_backtest::simulate::{
    dimension::{
        SignalPairMoments, calc_distance_correlation, calc_linear_orthogonal_diagnostics,
        calc_signal_pair_metrics,
    },
    fp_utils::spearman_corr,
};
use serde::Serialize;

use crate::data::result_db_path;

const MAX_RESEARCH_STRATEGY_COUNT: usize = 20;
const DEFAULT_NONLINEAR_SAMPLE_LIMIT: usize = 512;
const MAX_NONLINEAR_SAMPLE_LIMIT: usize = 1024;
const DEFAULT_RIDGE_LAMBDA: f64 = 1e-6;
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
    })
}

pub fn run_strategy_dimension_research(
    source_path: String,
    start_date: String,
    end_date: String,
    rule_names: Vec<String>,
    nonlinear_sample_limit: Option<usize>,
    ridge_lambda: Option<f64>,
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
        pending_layers: vec![
            "结果库尚未物化八维量价风格暴露，当前不输出风格距离".to_string(),
            "结果库尚未物化策略持仓收益路径，当前不输出收益相关与样本外增量".to_string(),
        ],
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
    use std::time::{SystemTime, UNIX_EPOCH};

    use duckdb::Connection;

    use super::{normalize_rule_names, run_strategy_dimension_research, validate_date_range};
    use crate::data::{result_db_path, scoring_store::init_result_db};

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
