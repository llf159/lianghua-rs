use crate::trigger_similarity::channel::{cosine_similarity_with_norms, vector_norm};
use crate::trigger_similarity::{
    EPS, KERNEL_NAMES, RuleCatalog, RuleEvent, RuleTriggerHit, TRIGGER_AGGREGATE_RHYTHM_WEIGHT,
    TRIGGER_RULE_SET_WEIGHT, TRIGGER_RULE_TIMING_WEIGHT, TRIGGER_TIME_DECAY_DAYS,
    TriggerFingerprint,
};

use duckdb::Connection;
use duckdb::params;
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
pub(super) fn pool_series(values: &[Option<f64>], segments: usize) -> Vec<f64> {
    let len = values.len();
    (0..segments)
        .map(|segment| {
            let start = segment * len / segments;
            let end = (segment + 1) * len / segments;
            let (sum, count) = values[start..end]
                .iter()
                .filter_map(|value| value.filter(|number| number.is_finite()))
                .fold((0.0, 0_usize), |(sum, count), value| {
                    (sum + value, count + 1)
                });
            if count == 0 { 0.0 } else { sum / count as f64 }
        })
        .collect()
}

pub(super) fn weighted_projection(values: &[Option<f64>], weights: &[f64]) -> f64 {
    let mut weighted = 0.0;
    let mut weight_sum = 0.0;
    for (value, weight) in values.iter().zip(weights) {
        if let Some(value) = value.filter(|number| number.is_finite()) {
            weighted += value * weight;
            weight_sum += weight.abs();
        }
    }
    if weight_sum <= EPS {
        0.0
    } else {
        weighted / weight_sum
    }
}

pub(super) struct TemporalKernelWeights {
    uniform: Vec<f64>,
    short_exp: Vec<f64>,
    medium_exp: Vec<f64>,
    recent_linear: Vec<f64>,
    turning: Vec<f64>,
}

thread_local! {
    static TEMPORAL_KERNEL_CACHE: RefCell<HashMap<usize, Arc<TemporalKernelWeights>>> =
        RefCell::new(HashMap::new());
}

pub(super) fn kernel_responses(values: &[Option<f64>]) -> Vec<f64> {
    let weights = (|len: usize| -> Arc<TemporalKernelWeights> {
        TEMPORAL_KERNEL_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            Arc::clone(cache.entry(len).or_insert_with(|| {
                let uniform = vec![1.0; len];
                let short_exp = (0..len)
                    .map(|index| {
                        let age = len - 1 - index;
                        if age < (10) {
                            0.5_f64.powf(age as f64 / (5.0))
                        } else {
                            0.0
                        }
                    })
                    .collect::<Vec<_>>();
                let medium_exp = (0..len)
                    .map(|index| {
                        let age = len - 1 - index;
                        if age < (30) {
                            0.5_f64.powf(age as f64 / (10.0))
                        } else {
                            0.0
                        }
                    })
                    .collect::<Vec<_>>();
                let recent_linear = (1..=len).map(|value| value as f64).collect::<Vec<_>>();
                let mut turning = vec![0.0; len];
                if len >= 3 {
                    turning[len - 3] = 1.0;
                    turning[len - 2] = -2.0;
                    turning[len - 1] = 1.0;
                } else if len >= 2 {
                    turning[len - 2] = -1.0;
                    turning[len - 1] = 1.0;
                }
                Arc::new(TemporalKernelWeights {
                    uniform,
                    short_exp,
                    medium_exp,
                    recent_linear,
                    turning,
                })
            }))
        })
    })(values.len());
    let stage_levels = pool_series(values, 3);
    let front_to_middle = stage_levels[1] - stage_levels[0];
    let middle_to_back = stage_levels[2] - stage_levels[1];
    let front_to_back = stage_levels[2] - stage_levels[0];
    vec![
        weighted_projection(values, &weights.uniform),
        weighted_projection(values, &weights.short_exp),
        weighted_projection(values, &weights.medium_exp),
        weighted_projection(values, &weights.recent_linear),
        front_to_middle,
        middle_to_back,
        front_to_back,
        weighted_projection(values, &weights.turning),
    ]
}

pub(super) fn temporal_signature(
    values: &[Option<f64>],
    segments: usize,
    standardize: bool,
) -> Option<Vec<f64>> {
    let finite_count = values
        .iter()
        .filter_map(|value| value.filter(|number| number.is_finite()))
        .count();
    if finite_count < 2 {
        return None;
    }
    let transformed = if standardize {
        let mean = values
            .iter()
            .filter_map(|value| value.filter(|number| number.is_finite()))
            .sum::<f64>()
            / finite_count as f64;
        let variance = values
            .iter()
            .filter_map(|value| value.filter(|number| number.is_finite()))
            .map(|value| (value - mean).powi(2))
            .sum::<f64>()
            / finite_count as f64;
        let std = variance.sqrt();
        if std <= EPS {
            return None;
        }
        Cow::Owned(
            values
                .iter()
                .map(|value| value.map(|number| (number - mean) / std))
                .collect::<Vec<_>>(),
        )
    } else {
        Cow::Borrowed(values)
    };
    let mut signature = pool_series(&transformed, segments);
    signature.extend(kernel_responses(&transformed));
    signature.push(1.0);
    Some(signature)
}

pub(super) fn build_trigger_fingerprint(
    events: &[RuleEvent],
    window_dates: &[String],
    segments: usize,
) -> TriggerFingerprint {
    let date_index = window_dates
        .iter()
        .enumerate()
        .map(|(i, date)| (date.as_str(), i))
        .collect::<HashMap<_, _>>();
    let mut by_rule = HashMap::<usize, Vec<RuleTriggerHit>>::new();
    let mut total_count = vec![Some(0.0); window_dates.len()];
    let mut total_score = vec![Some(0.0); window_dates.len()];
    for event in events {
        if let Some(index) = date_index.get(event.trade_date.as_str()).copied() {
            let hit = RuleTriggerHit {
                day_index: index,
                score: event.score,
            };
            if let Some(hits) = by_rule.get_mut(&event.rule_id) {
                hits.push(hit);
            } else {
                by_rule.insert(event.rule_id, vec![hit]);
            }
        }
    }
    for hits in by_rule.values_mut() {
        hits.sort_unstable_by_key(|hit| hit.day_index);
        let mut merged = Vec::<RuleTriggerHit>::with_capacity(hits.len());
        for hit in hits.drain(..) {
            if let Some(previous) = merged.last_mut()
                && previous.day_index == hit.day_index
            {
                previous.score += hit.score;
            } else {
                merged.push(hit);
            }
        }
        *hits = merged;
        for hit in hits.iter() {
            total_count[hit.day_index] = Some(total_count[hit.day_index].unwrap_or(0.0) + 1.0);
            total_score[hit.day_index] =
                Some(total_score[hit.day_index].unwrap_or(0.0) + hit.score);
        }
    }
    let signature_len = segments + KERNEL_NAMES.len() + 1;
    let total_count = temporal_signature(&total_count, segments, false)
        .unwrap_or_else(|| vec![0.0; signature_len]);
    let total_score = temporal_signature(&total_score, segments, false)
        .unwrap_or_else(|| vec![0.0; signature_len]);
    TriggerFingerprint {
        by_rule,
        total_count_norm: vector_norm(&total_count),
        total_score_norm: vector_norm(&total_score),
        total_count,
        total_score,
    }
}

pub(super) fn load_rule_idf_weights(
    conn: &Connection,
    start_date: &str,
    end_date: &str,
    rule_catalog: &mut RuleCatalog,
) -> Result<Vec<f64>, String> {
    let mut stmt = conn
        .prepare(
            "SELECT rule_name, COUNT(*) \
             FROM rule_details \
             WHERE trade_date>=? AND trade_date<=? \
               AND TRY_CAST(rule_score AS DOUBLE) IS NOT NULL \
               AND ABS(TRY_CAST(rule_score AS DOUBLE))>? \
             GROUP BY rule_name",
        )
        .map_err(|e| format!("预编译规则稀有度查询失败: {e}"))?;
    let mut rows = stmt
        .query(params![start_date, end_date, EPS])
        .map_err(|e| format!("查询规则稀有度失败: {e}"))?;
    let mut counts = Vec::<(String, f64)>::new();
    let mut total = 0.0;
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取规则稀有度失败: {e}"))?
    {
        let name: String = row.get(0).map_err(|e| format!("读取规则名称失败: {e}"))?;
        let count = row
            .get::<_, i64>(1)
            .map_err(|e| format!("读取规则触发次数失败: {e}"))?
            .max(0) as f64;
        if count > 0.0 {
            total += count;
            counts.push((name, count));
        }
    }
    let mut weights = vec![1.0; rule_catalog.names.len()];
    for (name, count) in counts {
        let id = rule_catalog.intern(name);
        weights.resize(rule_catalog.names.len(), 1.0);
        weights[id] = (1.0 + total / count).ln().clamp(1.0, 6.0);
    }
    Ok(weights)
}

pub(super) fn rule_weight(rule_weights: &[f64], rule_id: usize) -> f64 {
    rule_weights
        .get(rule_id)
        .copied()
        .filter(|weight| weight.is_finite() && *weight > 0.0)
        .unwrap_or(1.0)
}

pub(super) fn trigger_rule_weight_sum(
    fingerprint: &TriggerFingerprint,
    rule_weights: &[f64],
) -> f64 {
    fingerprint
        .by_rule
        .keys()
        .map(|name| rule_weight(rule_weights, *name))
        .sum()
}

#[cfg(test)]
pub(super) fn weighted_rule_set_similarity(
    target: &TriggerFingerprint,
    candidate: &TriggerFingerprint,
    rule_weights: &[f64],
) -> f64 {
    weighted_rule_set_similarity_with_masses(
        target,
        candidate,
        rule_weights,
        trigger_rule_weight_sum(target, rule_weights),
        trigger_rule_weight_sum(candidate, rule_weights),
    )
}

pub(super) fn weighted_rule_set_similarity_with_masses(
    target: &TriggerFingerprint,
    candidate: &TriggerFingerprint,
    rule_weights: &[f64],
    target_weight: f64,
    candidate_weight: f64,
) -> f64 {
    if target_weight <= EPS && candidate_weight <= EPS {
        return 1.0;
    }
    let intersection_weight = target
        .by_rule
        .keys()
        .filter(|name| candidate.by_rule.contains_key(*name))
        .map(|name| rule_weight(rule_weights, *name))
        .sum::<f64>();
    weighted_rule_set_similarity_from_masses(target_weight, candidate_weight, intersection_weight)
}

pub(super) fn weighted_rule_set_similarity_from_masses(
    target_weight: f64,
    candidate_weight: f64,
    intersection_weight: f64,
) -> f64 {
    let union_weight = target_weight + candidate_weight - intersection_weight;
    if union_weight <= EPS {
        0.0
    } else {
        (intersection_weight / union_weight).clamp(0.0, 1.0)
    }
}

pub(super) fn trigger_time_decay_scores() -> &'static [f64] {
    static SCORES: OnceLock<Vec<f64>> = OnceLock::new();
    SCORES.get_or_init(|| {
        (0..=(512))
            .map(|gap| (-(gap as f64) / TRIGGER_TIME_DECAY_DAYS).exp())
            .collect()
    })
}

thread_local! {
    static TRIGGER_MATCH_DP: RefCell<Vec<f64>> = const { RefCell::new(Vec::new()) };
}

pub(super) fn weighted_rule_timing_similarity(
    target: &TriggerFingerprint,
    candidate: &TriggerFingerprint,
    rule_weights: &[f64],
) -> f64 {
    weighted_rule_timing_similarity_with_minimum(target, candidate, rule_weights, f64::NEG_INFINITY)
        .unwrap_or(0.0)
}

pub(super) fn weighted_rule_timing_similarity_with_minimum(
    target: &TriggerFingerprint,
    candidate: &TriggerFingerprint,
    rule_weights: &[f64],
    minimum_similarity: f64,
) -> Option<f64> {
    let mut total_weight = 0.0;
    let mut remaining_upper_weight = 0.0;
    for (name, target_hits) in &target.by_rule {
        let Some(candidate_hits) = candidate.by_rule.get(name) else {
            continue;
        };
        let weight = rule_weight(rule_weights, *name);
        total_weight += weight;
        if minimum_similarity.is_finite() {
            remaining_upper_weight += weight * target_hits.len().min(candidate_hits.len()) as f64
                / target_hits.len().max(candidate_hits.len()).max(1) as f64;
        }
    }
    weighted_rule_timing_similarity_with_masses(
        target,
        candidate,
        rule_weights,
        minimum_similarity,
        total_weight,
        remaining_upper_weight,
    )
}

pub(super) fn weighted_rule_timing_similarity_with_masses(
    target: &TriggerFingerprint,
    candidate: &TriggerFingerprint,
    rule_weights: &[f64],
    minimum_similarity: f64,
    total_weight: f64,
    mut remaining_upper_weight: f64,
) -> Option<f64> {
    if total_weight <= EPS {
        return (0.0 + EPS >= minimum_similarity).then_some(0.0);
    }
    if minimum_similarity.is_finite() {
        if remaining_upper_weight / total_weight + EPS < minimum_similarity {
            return None;
        }
    } else {
        remaining_upper_weight = total_weight;
    }
    TRIGGER_MATCH_DP.with(|scratch| {
        let time_decay_scores = trigger_time_decay_scores();
        let mut dp = scratch.borrow_mut();
        let match_score = |left_hit: &RuleTriggerHit, right_hit: &RuleTriggerHit| {
            let day_gap = left_hit.day_index.abs_diff(right_hit.day_index);
            let time_score = time_decay_scores
                .get(day_gap)
                .copied()
                .unwrap_or_else(|| (-(day_gap as f64) / TRIGGER_TIME_DECAY_DAYS).exp());
            let denominator = left_hit.score.abs() + right_hit.score.abs();
            let intensity_score = if denominator <= EPS {
                1.0
            } else {
                (1.0 - (left_hit.score - right_hit.score).abs() / denominator).clamp(0.0, 1.0)
            };
            time_score * intensity_score
        };
        let mut weighted_sum = 0.0;
        for (name, target_hits) in &target.by_rule {
            let Some(candidate_hits) = candidate.by_rule.get(name) else {
                continue;
            };
            let weight = rule_weight(rule_weights, *name);
            let count_upper = if minimum_similarity.is_finite() {
                target_hits.len().min(candidate_hits.len()) as f64
                    / target_hits.len().max(candidate_hits.len()).max(1) as f64
            } else {
                1.0
            };
            let count_upper_weight = weight * count_upper;
            let remaining_after_rule = (remaining_upper_weight - count_upper_weight).max(0.0);
            let date_upper = if minimum_similarity.is_finite() {
                let directional_upper = |left: &[RuleTriggerHit], right: &[RuleTriggerHit]| {
                    let mut right_index = 0;
                    let mut sum = 0.0;
                    for left_hit in left {
                        while right_index + 1 < right.len()
                            && left_hit
                                .day_index
                                .abs_diff(right[right_index + 1].day_index)
                                <= left_hit.day_index.abs_diff(right[right_index].day_index)
                        {
                            right_index += 1;
                        }
                        let gap = left_hit.day_index.abs_diff(right[right_index].day_index);
                        sum += time_decay_scores
                            .get(gap)
                            .copied()
                            .unwrap_or_else(|| (-(gap as f64) / TRIGGER_TIME_DECAY_DAYS).exp());
                    }
                    sum
                };
                (directional_upper(target_hits, candidate_hits)
                    .min(directional_upper(candidate_hits, target_hits))
                    / target_hits.len().max(candidate_hits.len()).max(1) as f64)
                    .min(count_upper)
            } else {
                1.0
            };
            if (weighted_sum + date_upper * weight + remaining_after_rule) / total_weight + EPS
                < minimum_similarity
            {
                return None;
            }
            let timing_score = if !target_hits.is_empty()
                && target_hits.len() == candidate_hits.len()
                && target_hits.iter().zip(candidate_hits).all(|(left, right)| {
                    left.day_index == right.day_index
                        && left.score.is_finite()
                        && left.score == right.score
                }) {
                1.0
            } else if target_hits.len() == 1 {
                candidate_hits
                    .iter()
                    .map(|hit| match_score(&target_hits[0], hit))
                    .fold(0.0, f64::max)
                    / candidate_hits.len() as f64
            } else if candidate_hits.len() == 1 {
                target_hits
                    .iter()
                    .map(|hit| match_score(hit, &candidate_hits[0]))
                    .fold(0.0, f64::max)
                    / target_hits.len() as f64
            } else {
                dp.resize(candidate_hits.len() + 1, 0.0);
                dp.fill(0.0);
                for (left_index, left_hit) in target_hits.iter().enumerate() {
                    let mut diagonal = 0.0;
                    for (right_index, right_hit) in candidate_hits.iter().enumerate() {
                        let column = right_index + 1;
                        let previous_row = dp[column];
                        let matched = diagonal + match_score(left_hit, right_hit);
                        dp[column] = dp[column].max(dp[column - 1]).max(matched);
                        diagonal = previous_row;
                    }
                    let timing_upper = (dp[candidate_hits.len()]
                        + (target_hits.len() - left_index - 1) as f64)
                        .min(target_hits.len().min(candidate_hits.len()) as f64)
                        / target_hits.len().max(candidate_hits.len()) as f64;
                    let timing_upper = timing_upper.min(date_upper);
                    let upper_bound = (weighted_sum + timing_upper * weight + remaining_after_rule)
                        / total_weight;
                    if upper_bound + EPS < minimum_similarity {
                        return None;
                    }
                }
                dp[candidate_hits.len()] / target_hits.len().max(candidate_hits.len()) as f64
            };
            weighted_sum += timing_score * weight;
            remaining_upper_weight = remaining_after_rule;
            let upper_bound = (weighted_sum + remaining_after_rule) / total_weight;
            if upper_bound + EPS < minimum_similarity {
                return None;
            }
        }
        Some((weighted_sum / total_weight).clamp(0.0, 1.0))
    })
}

#[cfg(test)]
pub(super) fn trigger_fingerprint_similarity(
    target: &TriggerFingerprint,
    candidate: &TriggerFingerprint,
    rule_weights: &[f64],
) -> f64 {
    trigger_fingerprint_similarity_with_masses(
        target,
        candidate,
        rule_weights,
        trigger_rule_weight_sum(target, rule_weights),
        trigger_rule_weight_sum(candidate, rule_weights),
    )
}

pub(super) fn trigger_fingerprint_similarity_with_masses(
    target: &TriggerFingerprint,
    candidate: &TriggerFingerprint,
    rule_weights: &[f64],
    target_rule_weight: f64,
    candidate_rule_weight: f64,
) -> f64 {
    let rule_set = weighted_rule_set_similarity_with_masses(
        target,
        candidate,
        rule_weights,
        target_rule_weight,
        candidate_rule_weight,
    );
    let timing = weighted_rule_timing_similarity(target, candidate, rule_weights);
    let aggregate = trigger_aggregate_similarity(target, candidate);
    combine_trigger_similarity(rule_set, timing, aggregate)
}

pub(super) fn trigger_aggregate_similarity(
    target: &TriggerFingerprint,
    candidate: &TriggerFingerprint,
) -> f64 {
    (cosine_similarity_with_norms(
        &target.total_count,
        &candidate.total_count,
        target.total_count_norm,
        candidate.total_count_norm,
    ) + cosine_similarity_with_norms(
        &target.total_score,
        &candidate.total_score,
        target.total_score_norm,
        candidate.total_score_norm,
    )) / 200.0
}

pub(super) fn combine_trigger_similarity(rule_set: f64, timing: f64, aggregate: f64) -> f64 {
    100.0
        * (rule_set * TRIGGER_RULE_SET_WEIGHT
            + timing * TRIGGER_RULE_TIMING_WEIGHT
            + aggregate * TRIGGER_AGGREGATE_RHYTHM_WEIGHT)
}

#[cfg(test)]
mod tests {
    use crate::trigger_similarity::KERNEL_NAMES;
    use crate::trigger_similarity::RuleEvent;
    use crate::trigger_similarity::fingerprint::build_trigger_fingerprint;
    use crate::trigger_similarity::fingerprint::kernel_responses;
    use crate::trigger_similarity::fingerprint::temporal_signature;
    use crate::trigger_similarity::fingerprint::trigger_fingerprint_similarity;
    use crate::trigger_similarity::fingerprint::trigger_time_decay_scores;
    use crate::trigger_similarity::fingerprint::weighted_rule_set_similarity;
    use crate::trigger_similarity::fingerprint::weighted_rule_timing_similarity;
    use crate::trigger_similarity::fingerprint::weighted_rule_timing_similarity_with_minimum;

    #[test]
    fn temporal_signature_contains_pool_and_multiple_kernels() {
        let values = (1..=20).map(|value| Some(value as f64)).collect::<Vec<_>>();
        let signature = temporal_signature(&values, 5, true).expect("signature");
        assert_eq!(signature.len(), 5 + KERNEL_NAMES.len() + 1);
    }

    #[test]
    fn trigger_similarity_penalizes_extra_rules_and_time_misalignment() {
        let dates = (1..=10)
            .map(|day| format!("202401{day:02}"))
            .collect::<Vec<_>>();
        let event = |name: &str, date: &str, score: f64| RuleEvent {
            rule_id: (name.as_bytes()[0] - b'A') as usize,
            trade_date: date.to_string(),
            score,
        };
        let target_events = vec![
            event("A", "20240110", 1.0),
            event("A", "20240110", 1.0),
            event("B", "20240105", -3.0),
        ];
        let target = build_trigger_fingerprint(&target_events, &dates, 3);
        assert_eq!(target.by_rule[&0].len(), 1);
        assert_eq!(target.by_rule[&0][0].score, 2.0);
        let identical = build_trigger_fingerprint(
            &[event("A", "20240110", 2.0), event("B", "20240105", -3.0)],
            &dates,
            3,
        );
        let with_extra = build_trigger_fingerprint(
            &[
                event("A", "20240110", 2.0),
                event("B", "20240105", -3.0),
                event("C", "20240108", 1.0),
            ],
            &dates,
            3,
        );
        let shifted = build_trigger_fingerprint(
            &[event("A", "20240107", 2.0), event("B", "20240102", -3.0)],
            &dates,
            3,
        );
        let weights = [2.0, 1.0, 1.0];

        assert!(
            (trigger_fingerprint_similarity(&target, &identical, &weights) - 100.0).abs() < 1e-9
        );
        assert!((weighted_rule_set_similarity(&target, &with_extra, &weights) - 0.75).abs() < 1e-9);
        assert!(trigger_fingerprint_similarity(&target, &with_extra, &weights) < 100.0);
        assert!(
            trigger_fingerprint_similarity(&target, &shifted, &weights)
                < trigger_fingerprint_similarity(&target, &identical, &weights)
        );

        let exact_timing = weighted_rule_timing_similarity(&target, &shifted, &weights);
        let retained =
            weighted_rule_timing_similarity_with_minimum(&target, &shifted, &weights, exact_timing)
                .expect("候选达到精确下限时不能被剪枝");
        assert!((retained - exact_timing).abs() < 1e-12);
        assert!(
            weighted_rule_timing_similarity_with_minimum(
                &target,
                &shifted,
                &weights,
                exact_timing + 1e-6,
            )
            .is_none()
        );
    }

    #[test]
    fn dense_timing_fast_paths_preserve_scores_and_cutoffs() {
        let dates = (0..120).map(|day| day.to_string()).collect::<Vec<_>>();
        let events = dates
            .iter()
            .map(|date| RuleEvent {
                rule_id: 0,
                trade_date: date.clone(),
                score: 1.0,
            })
            .collect::<Vec<_>>();
        let target = build_trigger_fingerprint(&events, &dates, 3);
        let weights = [];
        crate::trigger_similarity::fingerprint::TRIGGER_MATCH_DP
            .with(|scratch| scratch.borrow_mut().clear());
        assert_eq!(
            weighted_rule_timing_similarity_with_minimum(&target, &target, &weights, 1.0),
            Some(1.0)
        );
        crate::trigger_similarity::fingerprint::TRIGGER_MATCH_DP
            .with(|scratch| assert!(scratch.borrow().is_empty()));
        assert_eq!(
            weighted_rule_timing_similarity_with_minimum(&target, &target, &weights, 1.01),
            None
        );
        let opposite_events = events
            .into_iter()
            .map(|mut event| {
                event.score = -1.0;
                event
            })
            .collect::<Vec<_>>();
        let opposite = build_trigger_fingerprint(&opposite_events, &dates, 3);
        assert_eq!(
            weighted_rule_timing_similarity_with_minimum(&target, &opposite, &weights, 0.9),
            None
        );
        assert_eq!(
            weighted_rule_timing_similarity_with_minimum(&target, &opposite, &weights, 0.0),
            Some(0.0)
        );
    }

    #[test]
    fn timing_count_bounds_preserve_exact_scores_at_cutoff() {
        let dates = (0..48).map(|day| day.to_string()).collect::<Vec<_>>();
        let weights = [1.3, 5.7];
        for seed in 0..24 {
            let mut events = [Vec::new(), Vec::new()];
            for (side, rows) in events.iter_mut().enumerate() {
                for (day, date) in dates.iter().enumerate() {
                    for rule in 0..2 {
                        if (day + seed + rule * 3) % (2 + (seed + side * 3 + rule) % 7) == 0 {
                            rows.push(RuleEvent {
                                rule_id: rule,
                                trade_date: date.clone(),
                                score: ((day + seed + side + rule) % 5) as f64 - 2.0,
                            });
                        }
                    }
                }
            }
            let target = build_trigger_fingerprint(&events[0], &dates, 3);
            let candidate = build_trigger_fingerprint(&events[1], &dates, 3);
            let exact = weighted_rule_timing_similarity(&target, &candidate, &weights);
            let mut intersection_weight = 0.0;
            let mut timing_upper_weight = 0.0;
            for (id, hits) in &target.by_rule {
                if let Some(other) = candidate.by_rule.get(id) {
                    let weight = crate::trigger_similarity::fingerprint::rule_weight(&weights, *id);
                    intersection_weight += weight;
                    timing_upper_weight += weight
                        * (hits.len().min(other.len()) as f64 / hits.len().max(other.len()) as f64);
                }
            }
            for cutoff in [
                f64::NEG_INFINITY,
                0.0,
                exact - 1e-6,
                exact,
                exact + 1e-6,
                0.9,
            ] {
                let retained = weighted_rule_timing_similarity_with_minimum(
                    &target, &candidate, &weights, cutoff,
                );
                let cached = crate::trigger_similarity::fingerprint::weighted_rule_timing_similarity_with_masses(
                    &target,
                    &candidate,
                    &weights,
                    cutoff,
                    intersection_weight,
                    timing_upper_weight,
                );
                assert_eq!(
                    cached, retained,
                    "cached bounds: seed={seed}, cutoff={cutoff}"
                );
                if exact + crate::trigger_similarity::EPS >= cutoff {
                    assert_eq!(retained, Some(exact), "seed={seed}, cutoff={cutoff}");
                } else {
                    assert_eq!(retained, None, "seed={seed}, cutoff={cutoff}");
                }
            }
        }
        let dense = build_trigger_fingerprint(
            &dates
                .iter()
                .map(|date| RuleEvent {
                    rule_id: 0,
                    trade_date: date.clone(),
                    score: 1.0,
                })
                .collect::<Vec<_>>(),
            &dates,
            3,
        );
        let mut sparse = dense.clone();
        sparse.by_rule.get_mut(&0).unwrap().truncate(2);
        crate::trigger_similarity::fingerprint::TRIGGER_MATCH_DP
            .with(|scratch| scratch.borrow_mut().clear());
        assert_eq!(
            weighted_rule_timing_similarity_with_minimum(&dense, &sparse, &weights, 0.5),
            None
        );
        crate::trigger_similarity::fingerprint::TRIGGER_MATCH_DP
            .with(|scratch| assert!(scratch.borrow().is_empty()));
    }

    #[test]
    fn single_hit_timing_fast_paths_preserve_one_to_one_matching() {
        let dates = (1..=5)
            .map(|day| format!("202401{day:02}"))
            .collect::<Vec<_>>();
        let event = |date: &str| RuleEvent {
            rule_id: 0,
            trade_date: date.to_string(),
            score: 1.0,
        };
        let single = build_trigger_fingerprint(&[event("20240101")], &dates, 2);
        let repeated =
            build_trigger_fingerprint(&[event("20240101"), event("20240104")], &dates, 2);
        let weights = [1.0];

        assert!(
            (weighted_rule_timing_similarity(&single, &repeated, &weights) - 0.5).abs() < 1e-12
        );
        assert!(
            (weighted_rule_timing_similarity(&repeated, &single, &weights) - 0.5).abs() < 1e-12
        );
    }

    #[test]
    fn cached_trigger_time_decay_is_bit_exact() {
        for (gap, cached) in trigger_time_decay_scores().iter().copied().enumerate() {
            let direct = (-(gap as f64) / crate::trigger_similarity::TRIGGER_TIME_DECAY_DAYS).exp();
            assert_eq!(cached.to_bits(), direct.to_bits());
        }
    }

    #[test]
    fn kernels_use_bounded_horizons_and_three_equal_position_stages() {
        let stages = [1.0, 1.0, 1.0, 3.0, 3.0, 3.0, 6.0, 6.0, 6.0]
            .into_iter()
            .map(Some)
            .collect::<Vec<_>>();
        let responses = kernel_responses(&stages);
        assert_eq!(responses.len(), KERNEL_NAMES.len());
        assert!((responses[4] - 2.0).abs() < 1e-12);
        assert!((responses[5] - 3.0).abs() < 1e-12);
        assert!((responses[6] - 5.0).abs() < 1e-12);

        let mut outside_horizons = vec![Some(0.0); 40];
        outside_horizons[0] = Some(100.0);
        let responses = kernel_responses(&outside_horizons);
        assert_eq!(responses[1], 0.0);
        assert_eq!(responses[2], 0.0);
    }
}
