use crate::scoring_model::CompactRuleScore;
use crate::scoring_model::ScoreBatch;
#[cfg(test)]
use crate::scoring_model::ScoreDetails;
use crate::scoring_model::ScoreSummary;
use crate::simulate::fp_utils::calc_newey_west_standard_error;
use crate::simulate::fp_utils::calc_newey_west_t_value;
use crate::simulate::fp_utils::calc_profit_loss_sums;
use crate::simulate::rule::RuleLayerMetricsWithValidation;
use crate::simulate::rule::RuleLayerSamplePointRef;
use crate::statistics::backtest::{
    RULE_BACKTEST_EPS, RuleDecayValidation, RuleLayerBacktestRunParams, RuleLayerRuleSummary,
    RulePortfolioDailyValue,
};
use crate::statistics::common::{RuleDayAgg, RuleMeta, open_result_conn};
use crate::statistics::universe::{ValidationSampleStockMeta, ts_code_allowed_by_filter};
use crate::statistics::validation::samples::ValidationSampleAccumulator;
use crate::statistics::validation::scores::{
    build_rule_backtest_payload, build_validation_return_distribution_from_counts,
    build_validation_score_layer_details_from_daily_layers, mean_f64, sample_std_f64,
};
use crate::statistics::validation::similarity::{
    ValidationSimilarityCache, build_validation_similarity_rows_from_overlap,
};
use crate::statistics::validation::walk_forward::{
    build_validation_fold_plan, build_validation_walk_forward, sort_validation_points,
    validation_axis_direction_sign,
};
use crate::statistics::validation::{
    RuleValidationComboResult, RuleValidationDailyMetric, RuleValidationIncrementalData,
};
use duckdb::params;
use duckdb::params_from_iter;
use std::collections::HashMap;
use std::collections::HashSet;
#[derive(Debug, Clone, Default)]
pub(in crate::statistics) struct RuleContributionAverages {
    pub(in crate::statistics) avg_contribution_score: Option<f64>,
    pub(in crate::statistics) avg_contribution_per_trigger: Option<f64>,
}

#[derive(Debug, Clone, Default)]
pub(in crate::statistics) struct RuleContributionAccumulator {
    pub(in crate::statistics) contribution_sum: f64,
    pub(in crate::statistics) contribution_days: usize,
    pub(in crate::statistics) trigger_count: i64,
}

pub(in crate::statistics) fn build_rule_contribution_averages(
    source_path: &str,
    rule_options: &[String],
    start_date: &str,
    end_date: &str,
) -> Result<HashMap<String, RuleContributionAverages>, String> {
    if rule_options.is_empty() {
        return Ok(HashMap::new());
    }

    let result_conn = open_result_conn(source_path)?;
    let placeholders = std::iter::repeat_n("?", rule_options.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
        WITH daily_rank_bounds AS (
            SELECT trade_date, MAX(rank) AS max_rank
            FROM score_summary
            WHERE trade_date >= ?
              AND trade_date <= ?
            GROUP BY trade_date
        ),
        triggered_rule_rows AS (
            SELECT
                rule_name,
                ts_code,
                trade_date,
                TRY_CAST(rule_score AS DOUBLE) AS rule_score
            FROM rule_details
            WHERE trade_date >= ?
              AND trade_date <= ?
              AND rule_name IN ({placeholders})
              AND TRY_CAST(rule_score AS DOUBLE) IS NOT NULL
              AND ABS(TRY_CAST(rule_score AS DOUBLE)) > 1e-12
        )
        SELECT
            d.rule_name,
            SUM(
                CASE
                    WHEN s.rank IS NOT NULL
                      AND b.max_rank IS NOT NULL
                      AND b.max_rank > 0
                    THEN d.rule_score * CAST((b.max_rank + 1 - s.rank) AS DOUBLE)
                         / CAST(b.max_rank AS DOUBLE)
                    ELSE 0
                END
            ) AS contribution_sum,
            COUNT(DISTINCT d.trade_date) AS contribution_days,
            COUNT(*) AS trigger_count
        FROM triggered_rule_rows AS d
        LEFT JOIN score_summary AS s
          ON s.ts_code = d.ts_code
         AND s.trade_date = d.trade_date
        LEFT JOIN daily_rank_bounds AS b
          ON b.trade_date = d.trade_date
        GROUP BY d.rule_name
        "#
    );
    let mut stmt = result_conn
        .prepare(&sql)
        .map_err(|e| format!("预编译策略回测贡献度查询失败: {e}"))?;
    let query_params = [start_date, end_date, start_date, end_date]
        .into_iter()
        .chain(rule_options.iter().map(String::as_str));
    let mut rows = stmt
        .query(params_from_iter(query_params))
        .map_err(|e| format!("查询策略回测贡献度失败: {e}"))?;
    let mut out = HashMap::with_capacity(rule_options.len());

    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取策略回测贡献度失败: {e}"))?
    {
        let rule_name: String = row.get(0).map_err(|e| format!("读取策略名失败: {e}"))?;
        let contribution_sum = row
            .get::<usize, Option<f64>>(1)
            .map_err(|e| format!("读取策略贡献度失败: {e}"))?
            .unwrap_or(0.0);
        let contribution_days: i64 = row.get(2).map_err(|e| format!("读取贡献天数失败: {e}"))?;
        let trigger_count: i64 = row.get(3).map_err(|e| format!("读取触发次数失败: {e}"))?;
        out.insert(
            rule_name,
            RuleContributionAverages {
                avg_contribution_score: (contribution_days > 0)
                    .then_some(contribution_sum / contribution_days as f64),
                avg_contribution_per_trigger: (trigger_count > 0)
                    .then_some(contribution_sum / trigger_count as f64),
            },
        );
    }

    Ok(out)
}

pub(in crate::statistics) fn load_daily_max_rank(
    source_path: &str,
    start_date: &str,
    end_date: &str,
    allowed_ts_codes: Option<&HashSet<String>>,
) -> Result<HashMap<String, i64>, String> {
    let result_conn = open_result_conn(source_path)?;
    let mut stmt = result_conn
        .prepare(
            r#"
            SELECT ts_code, trade_date, rank
            FROM score_summary
            WHERE trade_date >= ?
              AND trade_date <= ?
              AND rank IS NOT NULL
              AND rank > 0
            "#,
        )
        .map_err(|e| format!("预编译策略回测每日排名上限查询失败: {e}"))?;
    let mut rows = stmt
        .query(params![start_date, end_date])
        .map_err(|e| format!("查询策略回测每日排名上限失败: {e}"))?;
    let mut daily_max_rank: HashMap<String, i64> = HashMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取策略回测每日排名上限失败: {e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取总榜代码失败: {e}"))?;
        if !ts_code_allowed_by_filter(allowed_ts_codes, &ts_code) {
            continue;
        }
        let trade_date: String = row.get(1).map_err(|e| format!("读取总榜日期失败: {e}"))?;
        let rank: i64 = row.get(2).map_err(|e| format!("读取总榜排名失败: {e}"))?;
        daily_max_rank
            .entry(trade_date)
            .and_modify(|max_rank| *max_rank = (*max_rank).max(rank))
            .or_insert(rank);
    }
    Ok(daily_max_rank)
}

pub(in crate::statistics) fn build_one_rule_contribution_average(
    source_path: &str,
    rule_name: &str,
    start_date: &str,
    end_date: &str,
    allowed_ts_codes: Option<&HashSet<String>>,
    daily_max_rank: &HashMap<String, i64>,
) -> Result<RuleContributionAverages, String> {
    let result_conn = open_result_conn(source_path)?;
    let mut stmt = result_conn
        .prepare(
            r#"
            SELECT
                d.ts_code,
                d.trade_date,
                TRY_CAST(d.rule_score AS DOUBLE),
                s.rank
            FROM rule_details AS d
            LEFT JOIN score_summary AS s
              ON s.ts_code = d.ts_code
             AND s.trade_date = d.trade_date
            WHERE d.rule_name = ?
              AND d.trade_date >= ?
              AND d.trade_date <= ?
              AND TRY_CAST(d.rule_score AS DOUBLE) IS NOT NULL
              AND ABS(TRY_CAST(d.rule_score AS DOUBLE)) > 1e-12
            "#,
        )
        .map_err(|e| format!("预编译单策略贡献度查询失败: {e}"))?;
    let mut rows = stmt
        .query(params![rule_name, start_date, end_date])
        .map_err(|e| format!("查询单策略贡献度失败: {e}"))?;
    let mut daily_aggregates: HashMap<String, RuleDayAgg> = HashMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取单策略贡献度失败: {e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取规则代码失败: {e}"))?;
        if !ts_code_allowed_by_filter(allowed_ts_codes, &ts_code) {
            continue;
        }
        let trade_date: String = row.get(1).map_err(|e| format!("读取规则日期失败: {e}"))?;
        let rule_score: f64 = row.get(2).map_err(|e| format!("读取规则分数失败: {e}"))?;
        if !rule_score.is_finite() {
            continue;
        }
        let aggregate = daily_aggregates.entry(trade_date.clone()).or_default();
        aggregate.trigger_count += 1;
        let (Some(rank), Some(max_rank)) = (
            row.get::<usize, Option<i64>>(3)
                .map_err(|e| format!("读取总榜排名失败: {e}"))?
                .filter(|rank| *rank > 0),
            daily_max_rank.get(&trade_date),
        ) else {
            continue;
        };
        if *max_rank > 0 {
            aggregate.contribution_score +=
                rule_score * (*max_rank + 1 - rank) as f64 / *max_rank as f64;
        }
    }

    let mut accumulator = RuleContributionAccumulator::default();
    for aggregate in daily_aggregates.into_values() {
        if aggregate.trigger_count <= 0 {
            continue;
        }
        accumulator.contribution_sum += aggregate.contribution_score;
        accumulator.contribution_days += 1;
        accumulator.trigger_count += aggregate.trigger_count;
    }
    Ok(RuleContributionAverages {
        avg_contribution_score: (accumulator.contribution_days > 0)
            .then_some(accumulator.contribution_sum / accumulator.contribution_days as f64),
        avg_contribution_per_trigger: (accumulator.trigger_count > 0)
            .then_some(accumulator.contribution_sum / accumulator.trigger_count as f64),
    })
}

pub(in crate::statistics) fn filter_score_summary_rows_by_ts_codes(
    mut rows: Vec<ScoreSummary>,
    allowed_ts_codes: Option<&HashSet<String>>,
) -> Vec<ScoreSummary> {
    if allowed_ts_codes.is_some() {
        rows.retain(|row| ts_code_allowed_by_filter(allowed_ts_codes, &row.ts_code));
    }
    rows
}

pub(in crate::statistics) fn load_score_summary_rows_from_db(
    source_path: &str,
    start_date: &str,
    end_date: &str,
    allowed_ts_codes: Option<&HashSet<String>>,
) -> Result<Vec<ScoreSummary>, String> {
    let result_conn = open_result_conn(source_path)?;

    let mut summary_stmt = result_conn
        .prepare(
            r#"
            SELECT ts_code, trade_date, total_score, rank
            FROM score_summary
            WHERE trade_date >= ?
              AND trade_date <= ?
            ORDER BY trade_date ASC, ts_code ASC
            "#,
        )
        .map_err(|e| format!("预编译策略回测总榜原始行失败: {e}"))?;
    let mut summary_rows = summary_stmt
        .query(params![start_date, end_date])
        .map_err(|e| format!("查询策略回测总榜原始行失败: {e}"))?;
    let mut summaries = Vec::new();
    while let Some(row) = summary_rows
        .next()
        .map_err(|e| format!("读取策略回测总榜原始行失败: {e}"))?
    {
        let item = ScoreSummary {
            ts_code: row.get(0).map_err(|e| format!("读取总榜代码失败: {e}"))?,
            trade_date: row.get(1).map_err(|e| format!("读取总榜日期失败: {e}"))?,
            total_score: row.get(2).map_err(|e| format!("读取总榜分数失败: {e}"))?,
            rank: row.get(3).map_err(|e| format!("读取总榜排名失败: {e}"))?,
        };
        if ts_code_allowed_by_filter(allowed_ts_codes, &item.ts_code) {
            summaries.push(item);
        }
    }

    Ok(summaries)
}

#[cfg(test)]
pub(in crate::statistics) fn build_rule_contribution_averages_from_rows(
    summary_rows: &[ScoreSummary],
    detail_rows: &[ScoreDetails],
    start_date: &str,
    end_date: &str,
) -> HashMap<String, RuleContributionAverages> {
    let mut daily_max_rank: HashMap<String, i64> = HashMap::new();
    let mut rank_by_sample: HashMap<(String, String), i64> = HashMap::new();

    for row in summary_rows {
        if row.trade_date.as_str() < start_date || row.trade_date.as_str() > end_date {
            continue;
        }
        let Some(rank) = row.rank.filter(|value| *value > 0) else {
            continue;
        };

        daily_max_rank
            .entry(row.trade_date.clone())
            .and_modify(|max_rank| *max_rank = (*max_rank).max(rank))
            .or_insert(rank);
        rank_by_sample.insert((row.ts_code.clone(), row.trade_date.clone()), rank);
    }

    let mut daily_agg_map: HashMap<(String, String), RuleDayAgg> = HashMap::new();
    for row in detail_rows {
        if row.trade_date.as_str() < start_date || row.trade_date.as_str() > end_date {
            continue;
        }
        if !row.rule_score.is_finite() || row.rule_score.abs() <= RULE_BACKTEST_EPS {
            continue;
        }

        let agg = daily_agg_map
            .entry((row.trade_date.clone(), row.rule_name.clone()))
            .or_default();
        agg.trigger_count += 1;

        let Some(rank) = rank_by_sample.get(&(row.ts_code.clone(), row.trade_date.clone())) else {
            continue;
        };
        let Some(max_rank) = daily_max_rank.get(&row.trade_date) else {
            continue;
        };
        if *max_rank <= 0 {
            continue;
        }

        agg.contribution_score +=
            row.rule_score * (*max_rank + 1 - *rank) as f64 / *max_rank as f64;
    }

    let mut acc_map: HashMap<String, RuleContributionAccumulator> = HashMap::new();
    for ((_trade_date, rule_name), agg) in daily_agg_map {
        if agg.trigger_count <= 0 {
            continue;
        }
        let acc = acc_map.entry(rule_name).or_default();
        acc.contribution_sum += agg.contribution_score;
        acc.contribution_days += 1;
        acc.trigger_count += agg.trigger_count.max(0);
    }

    (|acc_map : HashMap < String , RuleContributionAccumulator >| -> HashMap < String , RuleContributionAverages > {
    acc_map
        .into_iter()
        .map(|(rule_name, acc)| {
            let avg_contribution_score = if acc.contribution_days > 0 {
                Some(acc.contribution_sum / acc.contribution_days as f64)
            } else {
                None
            };
            let avg_contribution_per_trigger = if acc.trigger_count > 0 {
                Some(acc.contribution_sum / acc.trigger_count as f64)
            } else {
                None
            };

            (
                rule_name,
                RuleContributionAverages {
                    avg_contribution_score,
                    avg_contribution_per_trigger,
                },
            )
        })
        .collect()
})(acc_map)
}

pub(in crate::statistics) fn build_rule_contribution_averages_from_compact_rows(
    summary_rows: &[ScoreSummary],
    detail_rows: &[CompactRuleScore],
    scoring_rule_names: &[String],
    start_date: &str,
    end_date: &str,
) -> HashMap<String, RuleContributionAverages> {
    let mut daily_max_rank: HashMap<&str, i64> = HashMap::new();
    for row in summary_rows {
        if row.trade_date.as_str() < start_date || row.trade_date.as_str() > end_date {
            continue;
        }
        if let Some(rank) = row.rank.filter(|value| *value > 0) {
            daily_max_rank
                .entry(row.trade_date.as_str())
                .and_modify(|max_rank| *max_rank = (*max_rank).max(rank))
                .or_insert(rank);
        }
    }

    let mut daily_agg_map: HashMap<(&str, u32), RuleDayAgg> = HashMap::new();
    for row in detail_rows {
        if !row.rule_score.is_finite() || row.rule_score.abs() <= RULE_BACKTEST_EPS {
            continue;
        }
        let Some(summary) = summary_rows.get(row.summary_index as usize) else {
            continue;
        };
        if summary.trade_date.as_str() < start_date || summary.trade_date.as_str() > end_date {
            continue;
        }
        let agg = daily_agg_map
            .entry((summary.trade_date.as_str(), row.rule_id))
            .or_default();
        agg.trigger_count += 1;
        let (Some(rank), Some(max_rank)) = (
            summary.rank.filter(|value| *value > 0),
            daily_max_rank.get(summary.trade_date.as_str()),
        ) else {
            continue;
        };
        agg.contribution_score += row.rule_score * (*max_rank + 1 - rank) as f64 / *max_rank as f64;
    }

    let mut acc_by_rule: HashMap<u32, RuleContributionAccumulator> = HashMap::new();
    for ((_trade_date, rule_id), agg) in daily_agg_map {
        if agg.trigger_count <= 0 {
            continue;
        }
        let acc = acc_by_rule.entry(rule_id).or_default();
        acc.contribution_sum += agg.contribution_score;
        acc.contribution_days += 1;
        acc.trigger_count += agg.trigger_count;
    }
    acc_by_rule
        .into_iter()
        .filter_map(|(rule_id, acc)| {
            let rule_name = scoring_rule_names.get(rule_id as usize)?.clone();
            Some((
                rule_name,
                RuleContributionAverages {
                    avg_contribution_score: (acc.contribution_days > 0)
                        .then_some(acc.contribution_sum / acc.contribution_days as f64),
                    avg_contribution_per_trigger: (acc.trigger_count > 0)
                        .then_some(acc.contribution_sum / acc.trigger_count as f64),
                },
            ))
        })
        .collect()
}

pub(in crate::statistics) fn filter_compact_rule_score_batch(
    batch: &mut ScoreBatch,
    allowed_ts_codes: Option<&HashSet<String>>,
) {
    let Some(allowed_ts_codes) = allowed_ts_codes else {
        return;
    };
    let mut remapped_summary_ids = vec![u32::MAX; batch.summary_rows.len()];
    let mut old_index = 0usize;
    let mut new_index = 0u32;
    batch.summary_rows.retain(|row| {
        let keep = ts_code_allowed_by_filter(Some(allowed_ts_codes), &row.ts_code);
        if keep {
            remapped_summary_ids[old_index] = new_index;
            new_index = new_index.saturating_add(1);
        }
        old_index += 1;
        keep
    });
    batch.compact_rule_rows.retain_mut(|row| {
        let Some(&new_summary_id) = remapped_summary_ids.get(row.summary_index as usize) else {
            return false;
        };
        if new_summary_id == u32::MAX {
            return false;
        }
        row.summary_index = new_summary_id;
        true
    });
}

pub(in crate::statistics) fn weighted_rule_summary_metric(
    summaries: &[RuleLayerRuleSummary],
    value: impl Fn(&RuleLayerRuleSummary) -> Option<f64>,
) -> Option<f64> {
    let mut weighted_sum = 0.0;
    let mut total_weight = 0usize;

    for summary in summaries {
        if summary.point_count == 0 {
            continue;
        }
        let Some(metric_value) = value(summary) else {
            continue;
        };
        if !metric_value.is_finite() {
            continue;
        }

        weighted_sum += metric_value * summary.point_count as f64;
        total_weight += summary.point_count;
    }

    if total_weight == 0 {
        None
    } else {
        Some(weighted_sum / total_weight as f64)
    }
}

pub(in crate::statistics) fn aggregate_all_rule_summary_metrics(
    summaries: &[RuleLayerRuleSummary],
    backtest_period: usize,
) -> (
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
) {
    let mut daily_values = HashMap::<String, (Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>)>::new();
    for value in summaries
        .iter()
        .flat_map(|summary| summary.portfolio_daily_values.iter())
    {
        let entry = daily_values.entry(value.trade_date.clone()).or_default();
        if let Some(metric) = value
            .avg_residual_return
            .filter(|metric| metric.is_finite())
        {
            entry.0.push(metric);
        }
        if let Some(metric) = value
            .avg_excess_residual_return
            .filter(|metric| metric.is_finite())
        {
            entry.1.push(metric);
        }
        if let Some(metric) = value.top_bottom_spread.filter(|metric| metric.is_finite()) {
            entry.2.push(metric);
        }
        if let Some(metric) = value.ic.filter(|metric| metric.is_finite()) {
            entry.3.push(metric);
        }
    }
    let mut daily_values = daily_values.into_iter().collect::<Vec<_>>();
    daily_values.sort_by(|left, right| left.0.cmp(&right.0));
    let avg_residual_values = daily_values
        .iter()
        .filter_map(|(_, values)| mean_f64(&values.0))
        .collect::<Vec<_>>();
    let avg_excess_residual_values = daily_values
        .iter()
        .filter_map(|(_, values)| mean_f64(&values.1))
        .collect::<Vec<_>>();
    let spread_values = daily_values
        .iter()
        .filter_map(|(_, values)| mean_f64(&values.2))
        .collect::<Vec<_>>();
    let ic_values = daily_values
        .iter()
        .filter_map(|(_, values)| mean_f64(&values.3))
        .collect::<Vec<_>>();

    let avg_residual_mean = mean_f64(&avg_residual_values);
    let avg_excess_residual_mean = mean_f64(&avg_excess_residual_values);
    let avg_er_change = (|summaries: &[RuleLayerRuleSummary]| -> Option<f64> {
        let mut weighted_sum = 0.0;
        let mut total_weight = 0usize;

        for summary in summaries {
            let Some(avg_er_change) = summary.avg_er_change.filter(|value| value.is_finite())
            else {
                continue;
            };
            if summary.er_change_sample_count == 0 {
                continue;
            }
            weighted_sum += avg_er_change * summary.er_change_sample_count as f64;
            total_weight += summary.er_change_sample_count;
        }

        if total_weight == 0 {
            None
        } else {
            Some(weighted_sum / total_weight as f64)
        }
    })(summaries);
    let profit_loss_ratio = calc_profit_loss_sums(&avg_residual_values).ratio();
    let spread_mean = mean_f64(&spread_values);
    let ic_mean = mean_f64(&ic_values);
    let ic_std = sample_std_f64(&ic_values);
    let icir = match (ic_mean, ic_std) {
        (Some(mean), Some(std)) if std.abs() >= RULE_BACKTEST_EPS => Some(mean / std),
        _ => None,
    };
    let ic_t_value = calc_newey_west_t_value(&ic_values, backtest_period.saturating_sub(1));

    (
        avg_residual_mean,
        avg_excess_residual_mean,
        avg_er_change,
        profit_loss_ratio,
        spread_mean,
        ic_mean,
        ic_std,
        icir,
        ic_t_value,
    )
}

pub(in crate::statistics) fn build_decay_validations_from_daily_values(
    mut daily_values: Vec<(String, f64)>,
    backtest_period: usize,
) -> Vec<RuleDecayValidation> {
    daily_values.retain(|(_, value)| value.is_finite());
    daily_values.sort_by(|left, right| left.0.cmp(&right.0));

    ([20, 40, 60])
        .into_iter()
        .map(|window_days| {
            let recent_day_count = daily_values.len().min(window_days);
            let recent_start_index = daily_values.len().saturating_sub(recent_day_count);
            let prior_day_count = recent_start_index;
            let recent_start_date = daily_values
                .get(recent_start_index)
                .map(|(trade_date, _)| trade_date.clone());
            let recent_end_date = daily_values
                .last()
                .map(|(trade_date, _)| trade_date.clone());

            if recent_day_count < window_days || prior_day_count < (10) {
                return RuleDecayValidation {
                    window_days,
                    recent_start_date,
                    recent_end_date,
                    recent_day_count,
                    prior_day_count,
                    recent_directional_excess_mean: None,
                    prior_directional_excess_mean: None,
                    decay_change: None,
                    decay_t_value: None,
                    status: "insufficient".to_string(),
                    status_label: "样本不足".to_string(),
                };
            }

            let prior = daily_values[..recent_start_index]
                .iter()
                .map(|(_, value)| *value)
                .collect::<Vec<_>>();
            let recent = daily_values[recent_start_index..]
                .iter()
                .map(|(_, value)| *value)
                .collect::<Vec<_>>();
            let recent_mean = mean_f64(&recent).unwrap_or_default();
            let prior_mean = mean_f64(&prior).unwrap_or_default();
            let change = recent_mean - prior_mean;
            let t_value = (|recent: &[f64], prior: &[f64], change: f64| -> Option<f64> {
                let lag = backtest_period.saturating_sub(1);
                let recent_se = calc_newey_west_standard_error(recent, lag)?;
                let prior_se = calc_newey_west_standard_error(prior, lag)?;
                let standard_error = (recent_se * recent_se + prior_se * prior_se).sqrt();
                if !standard_error.is_finite() || standard_error <= RULE_BACKTEST_EPS {
                    None
                } else {
                    Some(change / standard_error)
                }
            })(&recent, &prior, change);
            let (status, status_label) = (|recent_mean: f64,
                                           change: f64,
                                           t_value: Option<f64>|
             -> (&'static str, &'static str) {
                if change < 0.0 && t_value.is_some_and(|value| value <= -2.0) {
                    ("significant_decay", "显著衰减")
                } else if change < 0.0 && recent_mean < 0.0 {
                    ("decay", "衰减")
                } else if change < 0.0 {
                    ("weakening", "走弱")
                } else if recent_mean < 0.0 {
                    ("weak", "近期偏弱")
                } else if change > 0.0 {
                    ("improving", "改善")
                } else {
                    ("stable", "稳定")
                }
            })(recent_mean, change, t_value);

            RuleDecayValidation {
                window_days,
                recent_start_date,
                recent_end_date,
                recent_day_count,
                prior_day_count,
                recent_directional_excess_mean: Some(recent_mean),
                prior_directional_excess_mean: Some(prior_mean),
                decay_change: Some(change),
                decay_t_value: t_value,
                status: status.to_string(),
                status_label: status_label.to_string(),
            }
        })
        .collect()
}

pub(in crate::statistics) fn build_rule_directional_excess_daily_values(
    points: &[crate::simulate::rule::RuleLayerPoint],
) -> Vec<(String, f64)> {
    let direction_score_sum = points
        .iter()
        .filter_map(|point| point.avg_rule_score.filter(|value| value.is_finite()))
        .sum::<f64>();
    let direction_sign = if direction_score_sum < 0.0 { -1.0 } else { 1.0 };
    points
        .iter()
        .filter_map(|point| {
            point
                .avg_excess_residual_return
                .filter(|value| value.is_finite())
                .map(|value| (point.trade_date.clone(), value * direction_sign))
        })
        .collect()
}

pub(in crate::statistics) fn build_rule_decay_validations(
    points: &[crate::simulate::rule::RuleLayerPoint],
    backtest_period: usize,
) -> Vec<RuleDecayValidation> {
    build_decay_validations_from_daily_values(
        build_rule_directional_excess_daily_values(points),
        backtest_period,
    )
}

pub(in crate::statistics) fn build_rule_basket_decay_from_daily_groups<'a>(
    daily_groups: impl IntoIterator<Item = &'a [(String, f64)]>,
    backtest_period: usize,
) -> Vec<RuleDecayValidation> {
    let mut daily_aggregates = HashMap::<String, (f64, usize)>::new();
    for (trade_date, value) in daily_groups.into_iter().flatten() {
        if !value.is_finite() {
            continue;
        }
        let aggregate = daily_aggregates.entry(trade_date.clone()).or_default();
        aggregate.0 += *value;
        aggregate.1 += 1;
    }
    build_decay_validations_from_daily_values(
        daily_aggregates
            .into_iter()
            .filter_map(|(trade_date, (sum, count))| {
                (count > 0).then_some((trade_date, sum / count as f64))
            })
            .collect(),
        backtest_period,
    )
}

pub(in crate::statistics) fn build_all_rule_decay_validations(
    summaries: &[RuleLayerRuleSummary],
    backtest_period: usize,
) -> Vec<RuleDecayValidation> {
    build_rule_basket_decay_from_daily_groups(
        summaries
            .iter()
            .map(|summary| summary.decay_daily_values.as_slice()),
        backtest_period,
    )
}

pub(in crate::statistics) fn build_one_rule_backtest_summary_and_detail(
    one_rule_name: &str,
    validation: RuleLayerMetricsWithValidation,
    rule_meta_map: &HashMap<String, RuleMeta>,
    contribution_averages: &HashMap<String, RuleContributionAverages>,
    explain_map: &HashMap<String, String>,
    params: &RuleLayerBacktestRunParams,
    similarity_cache: &ValidationSimilarityCache,
    stock_meta_map: &HashMap<String, ValidationSampleStockMeta>,
) -> (RuleLayerRuleSummary, Option<RuleValidationComboResult>) {
    let metrics = &validation.metrics;
    let contribution_average = contribution_averages
        .get(one_rule_name)
        .cloned()
        .unwrap_or_default();
    let decay_daily_values = build_rule_directional_excess_daily_values(&metrics.points);
    let portfolio_daily_values = metrics
        .points
        .iter()
        .map(|point| RulePortfolioDailyValue {
            trade_date: point.trade_date.clone(),
            avg_residual_return: point.avg_residual_return,
            avg_excess_residual_return: point.avg_excess_residual_return,
            top_bottom_spread: point.top_bottom_spread,
            ic: point.ic,
        })
        .collect();
    let decay_validations = build_decay_validations_from_daily_values(
        decay_daily_values.clone(),
        params.backtest_period,
    );
    let summary = RuleLayerRuleSummary {
        rule_name: one_rule_name.to_string(),
        point_count: metrics.points.len(),
        avg_residual_mean: metrics.avg_residual_mean,
        avg_excess_residual_mean: metrics.avg_excess_residual_mean,
        avg_er_change: metrics.avg_er_change,
        er_change_sample_count: metrics.er_change_sample_count,
        profit_loss_ratio: metrics.profit_loss_ratio,
        spread_mean: None,
        avg_contribution_score: contribution_average.avg_contribution_score,
        avg_contribution_per_trigger: contribution_average.avg_contribution_per_trigger,
        ic_mean: metrics.ic_mean,
        ic_std: metrics.ic_std,
        icir: metrics.icir,
        ic_t_value: metrics.ic_t_value,
        decay_validations,
        decay_daily_values,
        portfolio_daily_values,
    };
    let detail = rule_meta_map.get(one_rule_name).map(|rule_meta| {
        (|params: &RuleLayerBacktestRunParams,
          rule_name: &str,
          rule_meta: &RuleMeta,
          validation: RuleLayerMetricsWithValidation,
          similarity_cache: &ValidationSimilarityCache,
          explain_map: &HashMap<String, String>,
          stock_meta_map: &HashMap<String, ValidationSampleStockMeta>|
         -> RuleValidationComboResult {
            let RuleLayerMetricsWithValidation {
                metrics,
                triggered_samples,
                daily_score_layers,
                return_distribution_counts,
            } = validation;
            let validation_layer_details = build_validation_score_layer_details_from_daily_layers(
                daily_score_layers,
                params.min_samples_per_day,
            );
            let return_distribution =
                build_validation_return_distribution_from_counts(return_distribution_counts);
            let mut sample_accumulator = ValidationSampleAccumulator::new(
                5,
                stock_meta_map,
                Some(similarity_cache),
                rule_meta.is_each,
                rule_meta.points,
                false,
            );
            for sample in &triggered_samples {
                if sample.rule_score.abs() <= RULE_BACKTEST_EPS {
                    continue;
                }
                sample_accumulator.push(RuleLayerSamplePointRef {
                    ts_code: &sample.ts_code,
                    trade_date: &sample.trade_date,
                    rule_score: sample.rule_score,
                    residual_return: sample.residual_return,
                });
            }

            let (
                trigger_samples,
                triggered_days,
                sample_stats,
                trigger_count_stats,
                sample_groups,
                overlap_hit_count,
            ) = sample_accumulator.into_parts();
            let walk_forward_axis = sort_validation_points(&metrics.points);
            let walk_forward_calendar = walk_forward_axis
                .iter()
                .map(|point| point.trade_date.clone())
                .collect::<Vec<_>>();
            let walk_forward_folds =
                build_validation_fold_plan(walk_forward_calendar.len(), 4, params.backtest_period);
            let mut day_trigger_counts = HashMap::<String, usize>::new();
            for sample in &triggered_samples {
                if sample.rule_score.abs() > RULE_BACKTEST_EPS {
                    *day_trigger_counts
                        .entry(sample.trade_date.clone())
                        .or_default() += 1;
                }
            }
            let direction_sign = validation_axis_direction_sign(&walk_forward_axis);
            let points_by_date = walk_forward_axis
                .iter()
                .map(|point| (point.trade_date.as_str(), *point))
                .collect::<HashMap<_, _>>();
            let walk_forward = build_validation_walk_forward(
                &walk_forward_calendar,
                &walk_forward_folds,
                params.backtest_period,
                direction_sign,
                &points_by_date,
                &day_trigger_counts,
            );
            let daily_metrics = walk_forward_axis
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
                rule_name,
                params,
                metrics,
                Some(validation_layer_details),
            );
            let similarity_rows = build_validation_similarity_rows_from_overlap(
                similarity_cache,
                trigger_samples,
                overlap_hit_count,
                Some(rule_name),
                explain_map,
            );

            RuleValidationComboResult {
                combo_key: rule_name.to_string(),
                combo_label: rule_name.to_string(),
                formula: rule_meta.when.clone(),
                unknown_values: Vec::new(),
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
                incremental: RuleValidationIncrementalData::default(),
                similarity_rows,
            }
        })(
            params,
            one_rule_name,
            rule_meta,
            validation,
            similarity_cache,
            explain_map,
            stock_meta_map,
        )
    });

    (summary, detail)
}

pub(in crate::statistics) fn split_and_sort_rule_backtest_summaries_and_details(
    items: Vec<(RuleLayerRuleSummary, Option<RuleValidationComboResult>)>,
) -> (Vec<RuleLayerRuleSummary>, Vec<RuleValidationComboResult>) {
    let mut all_rule_summaries = Vec::with_capacity(items.len());
    let mut rule_validation_details = Vec::new();

    for (summary, detail) in items {
        all_rule_summaries.push(summary);
        if let Some(detail) = detail {
            rule_validation_details.push(detail);
        }
    }

    all_rule_summaries.sort_by(|a, b| {
        b.profit_loss_ratio
            .unwrap_or(f64::NEG_INFINITY)
            .partial_cmp(&a.profit_loss_ratio.unwrap_or(f64::NEG_INFINITY))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.point_count.cmp(&a.point_count))
            .then_with(|| a.rule_name.cmp(&b.rule_name))
    });

    (all_rule_summaries, rule_validation_details)
}

#[cfg(test)]
mod tests {
    use crate::data::result_db_path;
    use crate::scoring_model::ScoreDetails;
    use crate::scoring_model::ScoreSummary;
    use crate::simulate::rule::RuleLayerPoint;
    use crate::statistics::backtest::RuleLayerRuleSummary;
    use crate::statistics::backtest::RulePortfolioDailyValue;
    use crate::statistics::backtest::summary::aggregate_all_rule_summary_metrics;
    use crate::statistics::backtest::summary::build_one_rule_contribution_average;
    use crate::statistics::backtest::summary::build_rule_basket_decay_from_daily_groups;
    use crate::statistics::backtest::summary::build_rule_contribution_averages;
    use crate::statistics::backtest::summary::build_rule_contribution_averages_from_rows;
    use crate::statistics::backtest::summary::build_rule_decay_validations;
    use crate::statistics::backtest::summary::load_daily_max_rank;
    use crate::statistics::test_support::*;
    use crate::statistics::validation::VALIDATION_EPS;
    use duckdb::Connection;
    use std::fs::create_dir_all;

    #[test]
    fn transient_rule_contribution_averages_match_rank_weight_formula() {
        let summary_rows = vec![
            ScoreSummary {
                ts_code: "000001.SZ".to_string(),
                trade_date: "20240102".to_string(),
                total_score: 10.0,
                rank: Some(1),
            },
            ScoreSummary {
                ts_code: "000002.SZ".to_string(),
                trade_date: "20240102".to_string(),
                total_score: 5.0,
                rank: Some(2),
            },
            ScoreSummary {
                ts_code: "000001.SZ".to_string(),
                trade_date: "20240103".to_string(),
                total_score: 3.0,
                rank: Some(2),
            },
            ScoreSummary {
                ts_code: "000002.SZ".to_string(),
                trade_date: "20240103".to_string(),
                total_score: 9.0,
                rank: Some(1),
            },
        ];
        let detail_rows = vec![
            ScoreDetails {
                ts_code: "000001.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_name: "规则A".to_string(),
                rule_score: 2.0,
            },
            ScoreDetails {
                ts_code: "000002.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_name: "规则A".to_string(),
                rule_score: 1.0,
            },
            ScoreDetails {
                ts_code: "000001.SZ".to_string(),
                trade_date: "20240103".to_string(),
                rule_name: "规则A".to_string(),
                rule_score: -2.0,
            },
            ScoreDetails {
                ts_code: "000002.SZ".to_string(),
                trade_date: "20240103".to_string(),
                rule_name: "规则B".to_string(),
                rule_score: 3.0,
            },
        ];

        let averages = build_rule_contribution_averages_from_rows(
            &summary_rows,
            &detail_rows,
            "20240102",
            "20240103",
        );

        let rule_a = averages.get("规则A").expect("rule A averages");
        assert_eq!(rule_a.avg_contribution_score, Some(0.75));
        assert_eq!(rule_a.avg_contribution_per_trigger, Some(0.5));

        let rule_b = averages.get("规则B").expect("rule B averages");
        assert_eq!(rule_b.avg_contribution_score, Some(3.0));
        assert_eq!(rule_b.avg_contribution_per_trigger, Some(3.0));
    }

    #[test]
    fn persisted_rule_contribution_sql_matches_row_formula() {
        let source_dir = temp_source_dir();
        let source_dir_str = source_dir.to_str().expect("utf8 source dir");
        create_dir_all(source_dir_str).expect("create source dir");
        let result_conn = Connection::open(result_db_path(source_dir_str)).expect("open result db");
        result_conn
            .execute_batch(
                r#"
                    CREATE TABLE score_summary (
                        ts_code VARCHAR,
                        trade_date VARCHAR,
                        total_score DOUBLE,
                        rank BIGINT
                    );
                    INSERT INTO score_summary VALUES
                        ('000001.SZ', '20240102', 10.0, 1),
                        ('000002.SZ', '20240102', 5.0, 2),
                        ('000001.SZ', '20240103', 3.0, 2),
                        ('000002.SZ', '20240103', 9.0, 1);

                    CREATE TABLE rule_details (
                        ts_code VARCHAR,
                        trade_date VARCHAR,
                        rule_name VARCHAR,
                        rule_score DOUBLE
                    );
                    INSERT INTO rule_details VALUES
                        ('000001.SZ', '20240102', '规则A', 2.0),
                        ('000002.SZ', '20240102', '规则A', 1.0),
                        ('000001.SZ', '20240103', '规则A', -2.0),
                        ('000002.SZ', '20240103', '规则B', 3.0),
                        ('000001.SZ', '20240103', '未请求规则', 100.0);
                    "#,
            )
            .expect("prepare contribution rows");
        drop(result_conn);

        let averages = build_rule_contribution_averages(
            source_dir_str,
            &["规则A".to_string(), "规则B".to_string()],
            "20240102",
            "20240103",
        )
        .expect("query contribution averages");

        let rule_a = averages.get("规则A").expect("rule A averages");
        assert_eq!(rule_a.avg_contribution_score, Some(0.75));
        assert_eq!(rule_a.avg_contribution_per_trigger, Some(0.5));

        let rule_b = averages.get("规则B").expect("rule B averages");
        assert_eq!(rule_b.avg_contribution_score, Some(3.0));
        assert_eq!(rule_b.avg_contribution_per_trigger, Some(3.0));
        assert!(!averages.contains_key("未请求规则"));

        let daily_max_rank = load_daily_max_rank(source_dir_str, "20240102", "20240103", None)
            .expect("load daily max rank");
        let streamed_rule_a = build_one_rule_contribution_average(
            source_dir_str,
            "规则A",
            "20240102",
            "20240103",
            None,
            &daily_max_rank,
        )
        .expect("query one rule contribution average");
        assert_eq!(
            streamed_rule_a.avg_contribution_score,
            rule_a.avg_contribution_score
        );
        assert_eq!(
            streamed_rule_a.avg_contribution_per_trigger,
            rule_a.avg_contribution_per_trigger
        );
    }

    #[test]
    fn all_rule_summary_recomputes_metrics_from_daily_portfolio() {
        let make_summary =
            |rule_name: &str, residuals: [f64; 3], ics: [f64; 3]| RuleLayerRuleSummary {
                rule_name: rule_name.to_string(),
                point_count: 3,
                avg_residual_mean: None,
                avg_excess_residual_mean: None,
                avg_er_change: None,
                er_change_sample_count: 0,
                profit_loss_ratio: None,
                spread_mean: None,
                avg_contribution_score: None,
                avg_contribution_per_trigger: None,
                ic_mean: None,
                ic_std: None,
                icir: None,
                ic_t_value: None,
                decay_validations: Vec::new(),
                decay_daily_values: Vec::new(),
                portfolio_daily_values: residuals
                    .into_iter()
                    .zip(ics)
                    .enumerate()
                    .map(|(index, (residual, ic))| RulePortfolioDailyValue {
                        trade_date: format!("2024010{}", index + 1),
                        avg_residual_return: Some(residual),
                        avg_excess_residual_return: Some(residual),
                        top_bottom_spread: None,
                        ic: Some(ic),
                    })
                    .collect(),
            };
        let summaries = [
            make_summary("a", [4.0, -2.0, 4.0], [1.0, 1.0, -1.0]),
            make_summary("b", [0.0, -2.0, 0.0], [-1.0, 1.0, 1.0]),
        ];

        let (mean, _, _, profit_factor, _, _, ic_std, _, _) =
            aggregate_all_rule_summary_metrics(&summaries, 1);

        assert!((mean.expect("portfolio mean") - 2.0 / 3.0).abs() < VALIDATION_EPS);
        assert!((profit_factor.expect("portfolio profit factor") - 2.0).abs() < VALIDATION_EPS);
        assert!((ic_std.expect("portfolio IC std") - 3.0_f64.sqrt() / 3.0).abs() < VALIDATION_EPS);
    }

    fn decay_test_point(index: usize, score: f64, excess: f64) -> RuleLayerPoint {
        RuleLayerPoint {
            trade_date: format!("{index:08}"),
            sample_count: 10,
            avg_rule_score: Some(score),
            avg_residual_return: Some(excess),
            avg_excess_residual_return: Some(excess),
            score_weighted_residual_return: Some(excess),
            top_bottom_spread: None,
            ic: None,
        }
    }

    #[test]
    fn rule_decay_validation_detects_recent_positive_rule_decay() {
        let points = (0..80)
            .map(|index| {
                let excess = if index < 60 {
                    0.20 + (index % 2) as f64 * 0.02
                } else {
                    -0.50 + (index % 2) as f64 * 0.02
                };
                decay_test_point(index, 1.0, excess)
            })
            .collect::<Vec<_>>();

        let validations = build_rule_decay_validations(&points, 1);
        let recent_20 = validations
            .iter()
            .find(|item| item.window_days == 20)
            .expect("20-day validation");

        assert_eq!(recent_20.status, "significant_decay");
        assert_eq!(recent_20.recent_day_count, 20);
        assert_eq!(recent_20.prior_day_count, 60);
        assert!(
            recent_20
                .recent_directional_excess_mean
                .is_some_and(|value| value < 0.0)
        );
        assert!(recent_20.decay_change.is_some_and(|value| value < -0.6));
        assert!(recent_20.decay_t_value.is_some_and(|value| value < -2.0));
    }

    #[test]
    fn rule_decay_validation_normalizes_negative_rule_direction() {
        let points = (0..80)
            .map(|index| {
                let excess = if index < 60 {
                    -0.30 - (index % 2) as f64 * 0.02
                } else {
                    0.20 - (index % 2) as f64 * 0.02
                };
                decay_test_point(index, -1.0, excess)
            })
            .collect::<Vec<_>>();

        let validations = build_rule_decay_validations(&points, 1);
        let recent_20 = validations
            .iter()
            .find(|item| item.window_days == 20)
            .expect("20-day validation");

        assert_eq!(recent_20.status, "significant_decay");
        assert!(
            recent_20
                .prior_directional_excess_mean
                .is_some_and(|value| value > 0.0)
        );
        assert!(
            recent_20
                .recent_directional_excess_mean
                .is_some_and(|value| value < 0.0)
        );
        assert!(recent_20.decay_change.is_some_and(|value| value < 0.0));
    }

    #[test]
    fn rule_decay_validation_marks_short_history_as_insufficient() {
        let points = (0..25)
            .map(|index| decay_test_point(index, 1.0, 0.10))
            .collect::<Vec<_>>();

        let validations = build_rule_decay_validations(&points, 1);

        assert_eq!(validations.len(), 3);
        assert!(validations.iter().all(|item| item.status == "insufficient"));
        let recent_20 = validations
            .iter()
            .find(|item| item.window_days == 20)
            .expect("20-day validation");
        assert_eq!(recent_20.recent_day_count, 20);
        assert_eq!(recent_20.prior_day_count, 5);
        assert_eq!(recent_20.decay_change, None);
    }

    #[test]
    fn all_rule_basket_decay_averages_directional_strategy_days() {
        let first = (0..80)
            .map(|index| {
                let value = if index < 60 {
                    0.20 + (index % 2) as f64 * 0.02
                } else {
                    -0.30 + (index % 2) as f64 * 0.02
                };
                (format!("{index:08}"), value)
            })
            .collect::<Vec<_>>();
        let second = (0..80)
            .map(|index| {
                let value = if index < 60 {
                    0.40 + (index % 2) as f64 * 0.02
                } else {
                    -0.10 + (index % 2) as f64 * 0.02
                };
                (format!("{index:08}"), value)
            })
            .collect::<Vec<_>>();

        let validations =
            build_rule_basket_decay_from_daily_groups([first.as_slice(), second.as_slice()], 1);
        let recent_20 = validations
            .iter()
            .find(|item| item.window_days == 20)
            .expect("20-day basket validation");

        assert_eq!(recent_20.status, "significant_decay");
        assert_eq!(recent_20.recent_day_count, 20);
        assert_eq!(recent_20.prior_day_count, 60);
        assert!(
            recent_20
                .recent_directional_excess_mean
                .is_some_and(|value| value < -0.18)
        );
        assert!(recent_20.decay_change.is_some_and(|value| value < -0.49));
    }
}
