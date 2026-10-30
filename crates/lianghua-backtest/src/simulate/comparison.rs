use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use crate::scoring_model::ScoreSummary;
use crate::simulate::{BacktestSampleEligibility, rule::RuleLayerSamplePoint};

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct RankComparisonSummary {
    pub period: String,
    pub dimension: String,
    pub bucket: String,
    pub daily_differences: Vec<(String, Option<f64>)>,
    pub statistics: Option<crate::simulate::research_statistics::ResearchSeriesStatistics>,
    pub pool_kind: String,
    pub top_k: usize,
    pub common_ranking_days: usize,
    pub calendar_days: Option<usize>,
    pub quarter_status: String,
    pub paired_return_days: usize,
    pub baseline_members: usize,
    pub baseline_scored_members: usize,
    pub baseline_labels: usize,
    pub candidate_members: usize,
    pub candidate_scored_members: usize,
    pub candidate_labels: usize,
    pub unknown_eligibility: usize,
    pub added: usize,
    pub removed: usize,
    pub added_labels: usize,
    pub removed_labels: usize,
    pub added_return_mean: Option<f64>,
    pub removed_return_mean: Option<f64>,
    pub baseline_paired_daily_mean: Option<f64>,
    pub candidate_paired_daily_mean: Option<f64>,
    pub paired_daily_difference: Option<f64>,
    pub common_score_label_members: usize,
    pub paired_ic_days: usize,
    pub baseline_ic_mean: Option<f64>,
    pub candidate_ic_mean: Option<f64>,
    pub ic_difference: Option<f64>,
    pub baseline_icir: Option<f64>,
    pub candidate_icir: Option<f64>,
    pub icir_difference: Option<f64>,
    pub paired_spread_days: usize,
    pub baseline_spread_mean: Option<f64>,
    pub candidate_spread_mean: Option<f64>,
    pub spread_difference: Option<f64>,
    pub net_contribution_days: usize,
    pub net_contribution_mean: Option<f64>,
}

#[derive(Default)]
struct RankComparisonAccumulator {
    common_days: usize,
    daily_differences: Vec<(String, Option<f64>)>,
    baseline_members: usize,
    baseline_scored_members: usize,
    baseline_labels: usize,
    candidate_members: usize,
    candidate_scored_members: usize,
    candidate_labels: usize,
    unknown_eligibility: usize,
    added: usize,
    removed: usize,
    added_sum: f64,
    removed_sum: f64,
    added_labels: usize,
    removed_labels: usize,
    baseline_daily: Vec<f64>,
    candidate_daily: Vec<f64>,
    net_daily: Vec<f64>,
    common_score_label_members: usize,
    baseline_ic: Vec<f64>,
    candidate_ic: Vec<f64>,
    baseline_spread: Vec<f64>,
    candidate_spread: Vec<f64>,
}

pub fn compare_ranked_samples(
    baseline: &[ScoreSummary],
    candidate: &[ScoreSummary],
    labels: &[RuleLayerSamplePoint],
    eligibility: &BacktestSampleEligibility,
    report_start: &str,
    member_filter: Option<&dyn Fn(&str, &str) -> bool>,
) -> Vec<RankComparisonSummary> {
    let mut baseline_days: BTreeMap<&str, Vec<&ScoreSummary>> = BTreeMap::new();
    let mut candidate_days: BTreeMap<&str, Vec<&ScoreSummary>> = BTreeMap::new();
    for (rows, days) in [
        (baseline, &mut baseline_days),
        (candidate, &mut candidate_days),
    ] {
        for row in rows {
            if row.rank.is_some_and(|rank| rank > 0) && row.total_score.is_finite() {
                days.entry(row.trade_date.as_str()).or_default().push(row);
            }
        }
    }
    let outcomes = labels
        .iter()
        .filter(|row| row.residual_return.is_finite())
        .map(|row| {
            (
                (row.trade_date.as_str(), row.ts_code.as_str()),
                row.residual_return,
            )
        })
        .collect::<HashMap<_, _>>();
    let mut aggregates = BTreeMap::<(String, usize, String), RankComparisonAccumulator>::new();
    let dates = baseline_days
        .keys()
        .chain(candidate_days.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    let mut baseline_history: VecDeque<(&str, Vec<HashSet<&str>>, HashSet<&str>)> = VecDeque::new();
    let mut candidate_history: VecDeque<(&str, Vec<HashSet<&str>>, HashSet<&str>)> =
        VecDeque::new();
    for date in dates {
        let baseline_rows = baseline_days.get(date);
        let candidate_rows = candidate_days.get(date);
        let mut base = baseline_rows
            .into_iter()
            .flat_map(|rows| rows.iter().copied())
            .filter(|row| eligibility.sample_status(&row.ts_code, date) == Some(true))
            .collect::<Vec<_>>();
        let mut cand = candidate_rows
            .into_iter()
            .flat_map(|rows| rows.iter().copied())
            .filter(|row| eligibility.sample_status(&row.ts_code, date) == Some(true))
            .collect::<Vec<_>>();
        base.sort_unstable_by(|a, b| a.rank.cmp(&b.rank).then_with(|| a.ts_code.cmp(&b.ts_code)));
        cand.sort_unstable_by(|a, b| a.rank.cmp(&b.rank).then_with(|| a.ts_code.cmp(&b.ts_code)));
        if let Some(rows) = baseline_rows {
            let unknown = rows
                .iter()
                .copied()
                .filter(|row| eligibility.sample_status(&row.ts_code, date).is_none())
                .map(|row| row.ts_code.as_str())
                .collect();
            baseline_history.push_back((
                date,
                [100usize, 500, 1000]
                    .into_iter()
                    .map(|k| {
                        base.iter()
                            .copied()
                            .take(k)
                            .map(|row| row.ts_code.as_str())
                            .collect()
                    })
                    .collect(),
                unknown,
            ));
            if baseline_history.len() > 3 {
                baseline_history.pop_front();
            }
        }
        if let Some(rows) = candidate_rows {
            let unknown = rows
                .iter()
                .copied()
                .filter(|row| eligibility.sample_status(&row.ts_code, date).is_none())
                .map(|row| row.ts_code.as_str())
                .collect();
            candidate_history.push_back((
                date,
                [100usize, 500, 1000]
                    .into_iter()
                    .map(|k| {
                        cand.iter()
                            .copied()
                            .take(k)
                            .map(|row| row.ts_code.as_str())
                            .collect()
                    })
                    .collect(),
                unknown,
            ));
            if candidate_history.len() > 3 {
                candidate_history.pop_front();
            }
        }
        if date < report_start {
            continue;
        }
        let (Some(baseline_rows), Some(candidate_rows)) = (baseline_rows, candidate_rows) else {
            continue;
        };
        let unknown = baseline_rows
            .iter()
            .chain(candidate_rows.iter())
            .filter(|row| eligibility.sample_status(&row.ts_code, date).is_none())
            .map(|row| row.ts_code.as_str())
            .collect::<HashSet<_>>()
            .len();
        let pool_unknown = baseline_history
            .iter()
            .chain(candidate_history.iter())
            .flat_map(|(day, _, codes)| codes.iter().map(move |code| (*day, *code)))
            .collect::<HashSet<_>>()
            .len();
        let quarter = date
            .get(4..6)
            .and_then(|month| month.parse::<u8>().ok())
            .filter(|month| (1..=12).contains(month))
            .and_then(|month| {
                date.get(..4)
                    .map(|year| format!("{year} Q{}", (month - 1) / 3 + 1))
            });
        let baseline_by_code = base
            .iter()
            .map(|row| (row.ts_code.as_str(), *row))
            .collect::<HashMap<_, _>>();
        let candidate_by_code = cand
            .iter()
            .map(|row| (row.ts_code.as_str(), *row))
            .collect::<HashMap<_, _>>();
        for (pool_kind, top_k) in [
            ("daily", 0usize),
            ("daily", 100),
            ("daily", 500),
            ("daily", 1000),
            ("three_day", 100),
            ("three_day", 500),
            ("three_day", 1000),
        ] {
            let limit = if top_k == 0 { usize::MAX } else { top_k };
            let (mut left, mut right): (HashSet<&str>, HashSet<&str>) = if pool_kind == "three_day"
            {
                if baseline_history.len() < 3 || candidate_history.len() < 3 {
                    continue;
                }
                let slot = match top_k {
                    100 => 0,
                    500 => 1,
                    _ => 2,
                };
                (
                    baseline_history
                        .iter()
                        .flat_map(|day| day.1[slot].iter().copied())
                        .collect(),
                    candidate_history
                        .iter()
                        .flat_map(|day| day.1[slot].iter().copied())
                        .collect(),
                )
            } else {
                (
                    base.iter()
                        .take(limit)
                        .map(|row| row.ts_code.as_str())
                        .collect(),
                    cand.iter()
                        .take(limit)
                        .map(|row| row.ts_code.as_str())
                        .collect(),
                )
            };
            let fixed_denominator = left.len();
            let equal_original_seats = left.len() == right.len();
            if let Some(filter) = member_filter {
                left.retain(|code| filter(date, code));
                right.retain(|code| filter(date, code));
            }
            let mut left_ordered = left.iter().copied().collect::<Vec<_>>();
            let mut right_ordered = right.iter().copied().collect::<Vec<_>>();
            left_ordered.sort_unstable();
            right_ordered.sort_unstable();
            let base_values = left_ordered
                .iter()
                .filter_map(|code| outcomes.get(&(date, *code)).copied())
                .collect::<Vec<_>>();
            let candidate_values = right_ordered
                .iter()
                .filter_map(|code| outcomes.get(&(date, *code)).copied())
                .collect::<Vec<_>>();
            let added = right_ordered
                .iter()
                .copied()
                .filter(|code| !left.contains(code))
                .collect::<Vec<_>>();
            let removed = left_ordered
                .iter()
                .copied()
                .filter(|code| !right.contains(code))
                .collect::<Vec<_>>();
            let added_values = added
                .iter()
                .filter_map(|code| outcomes.get(&(date, *code)).copied())
                .collect::<Vec<_>>();
            let removed_values = removed
                .iter()
                .filter_map(|code| outcomes.get(&(date, *code)).copied())
                .collect::<Vec<_>>();
            // 固定基线成员，并只对两侧分数和标签共同有效的股票做横截面比较。
            let common = left_ordered
                .iter()
                .filter_map(|code| {
                    let baseline_row = baseline_by_code.get(code)?;
                    let candidate_row = candidate_by_code.get(code)?;
                    let outcome = outcomes.get(&(date, *code))?;
                    Some((*baseline_row, *candidate_row, *outcome))
                })
                .collect::<Vec<_>>();
            let baseline_scores = common
                .iter()
                .map(|item| item.0.total_score)
                .collect::<Vec<_>>();
            let candidate_scores = common
                .iter()
                .map(|item| item.1.total_score)
                .collect::<Vec<_>>();
            let returns = common.iter().map(|item| item.2).collect::<Vec<_>>();
            let paired_ic = crate::simulate::rank::spearman_corr(&baseline_scores, &returns).zip(
                crate::simulate::rank::spearman_corr(&candidate_scores, &returns),
            );
            let spreads = if common.len() >= 5 {
                let mut baseline_order = common.clone();
                let mut candidate_order = common.clone();
                baseline_order.sort_unstable_by(|a, b| {
                    a.0.rank
                        .cmp(&b.0.rank)
                        .then_with(|| a.0.ts_code.cmp(&b.0.ts_code))
                });
                candidate_order.sort_unstable_by(|a, b| {
                    a.1.rank
                        .cmp(&b.1.rank)
                        .then_with(|| a.1.ts_code.cmp(&b.1.ts_code))
                });
                let n = common.len() / 5;
                let spread = |items: &[(&ScoreSummary, &ScoreSummary, f64)]| {
                    (items.iter().take(n).map(|item| item.2).sum::<f64>()
                        - items.iter().rev().take(n).map(|item| item.2).sum::<f64>())
                        / n as f64
                };
                Some((spread(&baseline_order), spread(&candidate_order)))
            } else {
                None
            };
            for period in std::iter::once("全部".to_string()).chain(quarter.iter().cloned()) {
                let agg = aggregates
                    .entry((pool_kind.to_string(), top_k, period))
                    .or_default();
                agg.common_days += 1;
                agg.daily_differences.push((
                    date.to_string(),
                    if base_values.is_empty() || candidate_values.is_empty() {
                        None
                    } else {
                        Some(
                            candidate_values.iter().sum::<f64>() / candidate_values.len() as f64
                                - base_values.iter().sum::<f64>() / base_values.len() as f64,
                        )
                    },
                ));
                agg.baseline_members += left.len();
                agg.baseline_scored_members += left
                    .iter()
                    .filter(|code| baseline_by_code.contains_key(**code))
                    .count();
                agg.baseline_labels += base_values.len();
                agg.candidate_members += right.len();
                agg.candidate_scored_members += right
                    .iter()
                    .filter(|code| candidate_by_code.contains_key(**code))
                    .count();
                agg.candidate_labels += candidate_values.len();
                agg.unknown_eligibility += if pool_kind == "three_day" {
                    pool_unknown
                } else {
                    unknown
                };
                agg.common_score_label_members += common.len();
                if let Some((base_ic, cand_ic)) = paired_ic {
                    agg.baseline_ic.push(base_ic);
                    agg.candidate_ic.push(cand_ic);
                }
                if let Some((base_spread, cand_spread)) = spreads {
                    agg.baseline_spread.push(base_spread);
                    agg.candidate_spread.push(cand_spread);
                }
                agg.added += added.len();
                agg.removed += removed.len();
                agg.added_sum += added_values.iter().sum::<f64>();
                agg.removed_sum += removed_values.iter().sum::<f64>();
                agg.added_labels += added_values.len();
                agg.removed_labels += removed_values.len();
                if !base_values.is_empty() && !candidate_values.is_empty() {
                    agg.baseline_daily
                        .push(base_values.iter().sum::<f64>() / base_values.len() as f64);
                    agg.candidate_daily
                        .push(candidate_values.iter().sum::<f64>() / candidate_values.len() as f64);
                }
                if fixed_denominator > 0
                    && equal_original_seats
                    && added.len() == added_values.len()
                    && removed.len() == removed_values.len()
                {
                    agg.net_daily.push(
                        (added_values.iter().sum::<f64>() - removed_values.iter().sum::<f64>())
                            / fixed_denominator as f64,
                    );
                }
            }
        }
    }
    aggregates
        .into_iter()
        .map(|((pool_kind, top_k, period), agg)| {
            let mean = |values: &[f64]| {
                (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
            };
            let baseline_mean = mean(&agg.baseline_daily);
            let candidate_mean = mean(&agg.candidate_daily);
            let baseline_ic = mean(&agg.baseline_ic);
            let candidate_ic = mean(&agg.candidate_ic);
            let baseline_spread = mean(&agg.baseline_spread);
            let candidate_spread = mean(&agg.candidate_spread);
            let baseline_icir = baseline_ic
                .zip(crate::simulate::rank::sample_std(&agg.baseline_ic))
                .and_then(|(value, deviation)| (deviation > 1e-12).then_some(value / deviation));
            let candidate_icir = candidate_ic
                .zip(crate::simulate::rank::sample_std(&agg.candidate_ic))
                .and_then(|(value, deviation)| (deviation > 1e-12).then_some(value / deviation));
            RankComparisonSummary {
                dimension: "all".into(),
                bucket: "全部".into(),
                daily_differences: agg.daily_differences,
                statistics: None,
                period,
                pool_kind,
                top_k,
                common_ranking_days: agg.common_days,
                calendar_days: None,
                quarter_status: "unknown".to_string(),
                paired_return_days: agg.baseline_daily.len(),
                baseline_members: agg.baseline_members,
                baseline_scored_members: agg.baseline_scored_members,
                baseline_labels: agg.baseline_labels,
                candidate_members: agg.candidate_members,
                candidate_scored_members: agg.candidate_scored_members,
                candidate_labels: agg.candidate_labels,
                unknown_eligibility: agg.unknown_eligibility,
                added: agg.added,
                removed: agg.removed,
                added_labels: agg.added_labels,
                removed_labels: agg.removed_labels,
                added_return_mean: (agg.added_labels > 0)
                    .then(|| agg.added_sum / agg.added_labels as f64),
                removed_return_mean: (agg.removed_labels > 0)
                    .then(|| agg.removed_sum / agg.removed_labels as f64),
                baseline_paired_daily_mean: baseline_mean,
                candidate_paired_daily_mean: candidate_mean,
                paired_daily_difference: baseline_mean
                    .zip(candidate_mean)
                    .map(|(left, right)| right - left),
                common_score_label_members: agg.common_score_label_members,
                paired_ic_days: agg.baseline_ic.len(),
                baseline_ic_mean: baseline_ic,
                candidate_ic_mean: candidate_ic,
                ic_difference: baseline_ic
                    .zip(candidate_ic)
                    .map(|(base, cand)| cand - base),
                baseline_icir,
                candidate_icir,
                icir_difference: baseline_icir
                    .zip(candidate_icir)
                    .map(|(base, cand)| cand - base),
                paired_spread_days: agg.baseline_spread.len(),
                baseline_spread_mean: baseline_spread,
                candidate_spread_mean: candidate_spread,
                spread_difference: baseline_spread
                    .zip(candidate_spread)
                    .map(|(base, cand)| cand - base),
                net_contribution_days: agg.net_daily.len(),
                net_contribution_mean: mean(&agg.net_daily),
            }
        })
        .collect()
}

pub struct RankComparisonComputed {
    pub summaries: Vec<RankComparisonSummary>,
    pub labels: Vec<RuleLayerSamplePoint>,
    pub label_windows: Vec<(String, String, u32, u32)>,
}

/// 对两个已重算排名共享同一份后验缓存；不修改评分结果库。
pub fn compare_rankings_from_score_rows(
    source_conn: &duckdb::Connection,
    source_dir: &str,
    input: &crate::simulate::rank::RankLayerFromDbInput,
    baseline: &[ScoreSummary],
    candidate: &[ScoreSummary],
) -> Result<RankComparisonComputed, String> {
    if input.layer_config.target.total_mv_min.is_some()
        || input.layer_config.target.total_mv_max.is_some()
    {
        return Err("排名对照尚不支持事前市值过滤".to_string());
    }
    let config = crate::simulate::rule::RuleLayerConfig {
        target: input.layer_config.target.clone(),
        min_samples_per_day: input.layer_config.min_samples_per_day,
        backtest_period: input.layer_config.backtest_period,
        min_listed_trade_days: input.layer_config.min_listed_trade_days,
    };
    let mut universe = Vec::with_capacity(baseline.len() + candidate.len());
    universe.extend_from_slice(baseline);
    universe.extend_from_slice(candidate);
    let cache = crate::simulate::rule::build_rule_layer_runtime_cache_from_summary_rows(
        source_conn,
        source_dir,
        &universe,
        &input.stock_adj_type,
        &input.index_ts_code,
        input.index_beta,
        input.concept_beta,
        input.industry_beta,
        &input.start_date,
        &input.end_date,
        &config,
    )?;
    drop(universe);
    let labels = crate::simulate::rule::collect_all_rule_samples_from_cache(
        &cache,
        &HashMap::new(),
        &config,
    )?;
    let mut label_windows = Vec::with_capacity(labels.len());
    cache.visit_label_windows(|code, day, entry, end, _| {
        label_windows.push((code.to_string(), day.to_string(), entry, end));
        Ok(())
    })?;
    drop(cache);
    let eligibility = crate::simulate::build_backtest_sample_eligibility(
        source_dir,
        config.min_listed_trade_days,
    )?;
    let mut summaries = compare_ranked_samples(
        baseline,
        candidate,
        &labels,
        &eligibility,
        &input.start_date,
        None,
    );
    if let Ok(calendar) = crate::data::load_trade_date_list(source_dir) {
        complete_rank_comparison_calendar(
            &mut summaries,
            &calendar,
            &input.start_date,
            &input.end_date,
        );
    }
    Ok(RankComparisonComputed {
        summaries,
        labels,
        label_windows,
    })
}

/// Add empty quarters and calendar coverage for every saved slice without selecting new members.
pub fn complete_rank_comparison_calendar(
    summaries: &mut Vec<RankComparisonSummary>,
    calendar: &[String],
    start_date: &str,
    end_date: &str,
) {
    let mut days = calendar
        .iter()
        .map(String::as_str)
        .filter(|day| day.len() == 8 && day.bytes().all(|byte| byte.is_ascii_digit()))
        .collect::<Vec<_>>();
    days.sort_unstable();
    days.dedup();
    let periods = days
        .iter()
        .filter(|day| **day >= start_date && **day <= end_date)
        .filter_map(|day| {
            let month = day.get(4..6)?.parse::<u8>().ok()?;
            (1..=12)
                .contains(&month)
                .then(|| format!("{} Q{}", &day[..4], (month - 1) / 3 + 1))
        })
        .chain(std::iter::once("全部".to_string()))
        .collect::<BTreeSet<_>>();
    let mut slices = summaries
        .iter()
        .map(|item| (item.dimension.clone(), item.bucket.clone()))
        .collect::<BTreeSet<_>>();
    if slices.is_empty() {
        slices.insert(("all".to_string(), "全部".to_string()));
    }
    for (dimension, bucket) in slices {
        for period in &periods {
            for (pool_kind, top_k) in [
                ("daily", 0),
                ("daily", 100),
                ("daily", 500),
                ("daily", 1000),
                ("three_day", 100),
                ("three_day", 500),
                ("three_day", 1000),
            ] {
                if !summaries.iter().any(|item| {
                    item.dimension == dimension
                        && item.bucket == bucket
                        && item.period == *period
                        && item.pool_kind == pool_kind
                        && item.top_k == top_k
                }) {
                    summaries.push(RankComparisonSummary {
                        dimension: dimension.clone(),
                        bucket: bucket.clone(),
                        period: period.clone(),
                        pool_kind: pool_kind.to_string(),
                        top_k,
                        ..Default::default()
                    });
                }
            }
        }
    }
    summaries.sort_by(|a, b| {
        (&a.dimension, &a.bucket, &a.pool_kind, a.top_k, &a.period).cmp(&(
            &b.dimension,
            &b.bucket,
            &b.pool_kind,
            b.top_k,
            &b.period,
        ))
    });
    for summary in summaries {
        let (count, status) = crate::simulate::rank::quarter_coverage(
            &summary.period,
            summary.common_ranking_days,
            &days,
            start_date,
            end_date,
        );
        summary.calendar_days = count;
        summary.quarter_status = status.to_string();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paired_rankings_keep_fixed_members_and_missing_changes_unknown() {
        let rows = |codes: &[&str]| {
            codes
                .iter()
                .enumerate()
                .map(|(i, code)| ScoreSummary {
                    ts_code: (*code).into(),
                    trade_date: "20260105".into(),
                    total_score: 10.0 - i as f64,
                    rank: Some(i as i64 + 1),
                })
                .collect::<Vec<_>>()
        };
        let baseline = rows(&["A", "B"]);
        let candidate = rows(&["A", "C"]);
        let labels = [("A", 1.0), ("C", 3.0)]
            .into_iter()
            .map(|(code, value)| RuleLayerSamplePoint {
                ts_code: code.into(),
                trade_date: "20260105".into(),
                rule_score: 0.0,
                residual_return: value,
                er_change: 0.0,
            })
            .collect::<Vec<_>>();
        let eligibility = BacktestSampleEligibility::default();
        let comparisons = compare_ranked_samples(
            &baseline,
            &candidate,
            &labels,
            &eligibility,
            "20260105",
            None,
        );
        let item = comparisons
            .iter()
            .find(|item| item.period == "2026 Q1" && item.top_k == 100)
            .unwrap();
        assert_eq!(
            (
                item.baseline_members,
                item.baseline_labels,
                item.candidate_members,
                item.candidate_labels
            ),
            (2, 1, 2, 2)
        );
        assert_eq!(
            (
                item.added,
                item.removed,
                item.added_labels,
                item.removed_labels
            ),
            (1, 1, 1, 0)
        );
        assert_eq!(item.paired_daily_difference, Some(1.0));
        assert_eq!(item.net_contribution_mean, None);
        let equal = compare_ranked_samples(
            &baseline,
            &baseline,
            &labels,
            &eligibility,
            "20260105",
            None,
        );
        let item = equal
            .iter()
            .find(|item| item.period == "全部" && item.top_k == 100)
            .unwrap();
        assert_eq!(item.paired_daily_difference, Some(0.0));
        assert_eq!(item.net_contribution_mean, Some(0.0));

        let five = rows(&["A", "B", "C", "D", "E"]);
        let five_labels = [("A", 5.0), ("B", 4.0), ("C", 3.0), ("D", 2.0), ("E", 1.0)]
            .into_iter()
            .map(|(code, value)| RuleLayerSamplePoint {
                ts_code: code.into(),
                trade_date: "20260105".into(),
                rule_score: 0.0,
                residual_return: value,
                er_change: 0.0,
            })
            .collect::<Vec<_>>();
        let equal =
            compare_ranked_samples(&five, &five, &five_labels, &eligibility, "20260105", None);
        let item = equal
            .iter()
            .find(|item| item.period == "全部" && item.top_k == 0)
            .unwrap();
        assert_eq!(item.paired_ic_days, 1);
        assert!((item.baseline_ic_mean.unwrap() - 1.0).abs() < 1e-12);
        assert_eq!(item.ic_difference, Some(0.0));
        assert_eq!(item.baseline_spread_mean, Some(4.0));
        assert_eq!(item.spread_difference, Some(0.0));

        let history = [("A", "20260105"), ("B", "20260106"), ("C", "20260107")]
            .into_iter()
            .map(|(code, date)| ScoreSummary {
                ts_code: code.into(),
                trade_date: date.into(),
                total_score: 1.0,
                rank: Some(1),
            })
            .collect::<Vec<_>>();
        let final_label = vec![RuleLayerSamplePoint {
            ts_code: "C".into(),
            trade_date: "20260107".into(),
            rule_score: 0.0,
            residual_return: 2.0,
            er_change: 0.0,
        }];
        let pooled = compare_ranked_samples(
            &history,
            &history,
            &final_label,
            &eligibility,
            "20260107",
            None,
        );
        let item = pooled
            .iter()
            .find(|item| {
                item.pool_kind == "three_day" && item.period == "全部" && item.top_k == 100
            })
            .unwrap();
        assert_eq!(
            (
                item.common_ranking_days,
                item.baseline_members,
                item.baseline_labels
            ),
            (1, 3, 1)
        );
        assert_eq!(item.paired_daily_difference, Some(0.0));
        let mut sliced = comparisons;
        for summary in &mut sliced {
            summary.dimension = "board".into();
            summary.bucket = "主板".into();
        }
        let calendar = ["20251231", "20260105", "20260401", "20260701"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        complete_rank_comparison_calendar(&mut sliced, &calendar, "20260105", "20260401");
        let missing = sliced
            .iter()
            .find(|row| row.period == "2026 Q2" && row.pool_kind == "daily" && row.top_k == 100)
            .unwrap();
        assert_eq!(
            (
                missing.common_ranking_days,
                missing.baseline_members,
                missing.calendar_days
            ),
            (0, 0, Some(1))
        );
        assert_eq!(missing.quarter_status, "missing_rank_days");
        assert!(
            sliced
                .iter()
                .all(|row| row.dimension == "board" && row.bucket == "主板")
        );
    }
}
