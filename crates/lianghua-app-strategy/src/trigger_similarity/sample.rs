use crate::trigger_similarity::channel::{
    build_channel_fingerprint, build_indicator_channels, build_price_volume_channels,
    market_category_features,
};
use crate::trigger_similarity::fingerprint::build_trigger_fingerprint;
use crate::trigger_similarity::load::{
    load_future_rows, load_market_rows, load_rule_rows, load_summary_rows,
};
use crate::trigger_similarity::{
    Anchor, BenchmarkObservation, ChannelFingerprint, EPS, EventFingerprint, EventSample,
    FutureObservation, MarketObservation, MarketSchema, Outcome, RATING_MAX_PER_OUTCOME_WINDOW,
    RATING_SAMPLE_LIMIT, RuleCatalog, RuleEvent, SHRINKAGE_STRENGTH,
    StrategyTriggerSimilarityOutcomeSummary, StrategyTriggerSimilarityRow,
};

// 见父模块 mod.rs

use duckdb::Connection;
use duckdb::params;
use rayon::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
pub(super) fn window_dates_for_anchor<'a>(anchor: &Anchor, dates: &'a [String]) -> &'a [String] {
    let start = dates.binary_search(&anchor.start_trade_date).unwrap_or(0);
    let end = dates
        .binary_search(&anchor.end_trade_date)
        .map(|i| i + 1)
        .unwrap_or(dates.len());
    &dates[start.min(end)..end]
}

pub(super) fn build_outcome(
    _market_rows: &[MarketObservation],
    future_rows: &[FutureObservation],
    horizon: usize,
    benchmark_rows: &HashMap<String, BenchmarkObservation>,
) -> Option<Outcome> {
    if future_rows.len() != horizon {
        return None;
    }
    let first = future_rows.first()?;
    let entry = (first.open.abs() > EPS).then_some(first.open)?;
    let last = future_rows.last()?;
    let return_pct = (last.close / entry - 1.0) * 100.0;
    let mfe_pct = future_rows
        .iter()
        .map(|r| (r.high / entry - 1.0) * 100.0)
        .fold(f64::NEG_INFINITY, f64::max);
    let mae_pct = future_rows
        .iter()
        .map(|r| (r.low / entry - 1.0) * 100.0)
        .fold(f64::INFINITY, f64::min);
    let excess_return_pct = match (
        benchmark_rows.get(&first.trade_date),
        benchmark_rows.get(&last.trade_date),
    ) {
        (Some(start), Some(end)) if start.open.abs() > EPS => {
            Some(return_pct - (end.close / start.open - 1.0) * 100.0)
        }
        _ => None,
    };
    Some(Outcome {
        start_trade_date: first.trade_date.clone(),
        end_trade_date: last.trade_date.clone(),
        return_pct,
        excess_return_pct,
        mfe_pct,
        mae_pct,
    })
}

pub(super) fn load_benchmark_rows(
    conn: &Connection,
    start: &str,
    end: &str,
    benchmark_index_code: &str,
) -> Result<HashMap<String, BenchmarkObservation>, String> {
    let mut stmt = conn
        .prepare(
            "SELECT trade_date, TRY_CAST(open AS DOUBLE), TRY_CAST(close AS DOUBLE) \
             FROM trigger_market_db.stock_data \
             WHERE adj_type='ind' AND ts_code=? AND trade_date>=? AND trade_date<=?",
        )
        .map_err(|e| format!("预编译基准指数行情查询失败: {e}"))?;
    let mut rows = stmt
        .query(params![benchmark_index_code, start, end])
        .map_err(|e| format!("查询基准指数行情失败: {e}"))?;
    let mut out = HashMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取基准指数行情失败: {e}"))?
    {
        let date: String = row.get(0).map_err(|e| format!("读取指数日期失败: {e}"))?;
        let open: Option<f64> = row.get(1).map_err(|e| format!("读取指数开盘价失败: {e}"))?;
        let close: Option<f64> = row.get(2).map_err(|e| format!("读取指数收盘价失败: {e}"))?;
        if let (Some(open), Some(close)) = (
            open.filter(|value| value.is_finite()),
            close.filter(|value| value.is_finite()),
        ) {
            out.insert(date, BenchmarkObservation { open, close });
        }
    }
    Ok(out)
}

#[derive(Clone, Copy)]
pub(super) struct SampleBuildContext<'a> {
    pub(super) schema: &'a MarketSchema,
    pub(super) all_trade_dates: &'a [String],
    pub(super) environment_fingerprints: &'a HashMap<String, Arc<ChannelFingerprint>>,
    pub(super) benchmark_rows: &'a HashMap<String, BenchmarkObservation>,
    pub(super) total_mv_map: &'a HashMap<String, f64>,
    pub(super) name_map: &'a HashMap<String, String>,
    pub(super) pool_segments: usize,
    pub(super) outcome_trade_days: usize,
    pub(super) target_trade_date: &'a str,
    pub(super) include_outcome: bool,
    pub(super) include_summaries: bool,
}

pub(super) fn build_samples_for_chunk(
    conn: &Connection,
    anchors: Vec<Anchor>,
    context: &SampleBuildContext<'_>,
    rule_catalog: &mut RuleCatalog,
    preloaded_rules: Option<HashMap<usize, Vec<RuleEvent>>>,
) -> Result<Vec<EventSample>, String> {
    let market_by_anchor = load_market_rows(conn, &anchors, context.schema)?;
    let rules_by_anchor = match preloaded_rules {
        Some(rules) => rules,
        None => load_rule_rows(conn, &anchors, rule_catalog)?,
    };
    let summaries = if context.include_summaries {
        load_summary_rows(conn, &anchors)?
    } else {
        HashMap::new()
    };
    let future_by_anchor = if context.include_outcome {
        load_future_rows(
            conn,
            &anchors,
            context.outcome_trade_days,
            context.target_trade_date,
        )?
    } else {
        HashMap::new()
    };
    let samples = anchors
        .into_par_iter()
        .filter_map(|anchor| {
            let market_rows = market_by_anchor.get(&anchor.id)?;
            if market_rows.len() < 3
                || market_rows.last().map(|r| r.trade_date.as_str())
                    != Some(anchor.end_trade_date.as_str())
            {
                return None;
            }
            let rules = rules_by_anchor
                .get(&anchor.id)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let window_dates = window_dates_for_anchor(&anchor, context.all_trade_dates);
            let trigger = build_trigger_fingerprint(rules, window_dates, context.pool_segments);
            let fingerprint = EventFingerprint {
                trigger,
                price_volume: build_channel_fingerprint(build_price_volume_channels(
                    market_rows,
                    context.pool_segments,
                    context.total_mv_map.get(&anchor.ts_code).copied(),
                    market_category_features(
                        &anchor.ts_code,
                        context.name_map.get(&anchor.ts_code).map(String::as_str),
                    ),
                )),
                indicators: build_channel_fingerprint(build_indicator_channels(
                    market_rows,
                    context.schema.indicator_columns.len(),
                    context.pool_segments,
                )),
                market: context
                    .environment_fingerprints
                    .get(&anchor.end_trade_date)
                    .cloned()
                    .unwrap_or_else(|| -> Arc<ChannelFingerprint> {
                        static EMPTY: OnceLock<Arc<ChannelFingerprint>> = OnceLock::new();
                        Arc::clone(
                            EMPTY.get_or_init(|| Arc::new(build_channel_fingerprint(Vec::new()))),
                        )
                    }),
            };
            let outcome = if context.include_outcome {
                build_outcome(
                    market_rows,
                    future_by_anchor
                        .get(&anchor.id)
                        .map(Vec::as_slice)
                        .unwrap_or(&[]),
                    context.outcome_trade_days,
                    context.benchmark_rows,
                )
            } else {
                None
            };
            if context.include_outcome && outcome.is_none() {
                return None;
            }
            let (total_score, rank) = summaries.get(&anchor.id).copied().unwrap_or((None, None));
            Some(EventSample {
                fingerprint,
                trigger_count: rules.len(),
                outcome,
                total_score,
                rank,
                anchor,
            })
        })
        .collect();
    Ok(samples)
}

pub(super) fn weighted_quantile(values: &[(f64, f64)], quantile: f64) -> Option<f64> {
    let mut finite = values
        .iter()
        .copied()
        .filter(|(value, weight)| value.is_finite() && weight.is_finite() && *weight > EPS)
        .collect::<Vec<_>>();
    if finite.is_empty() {
        return None;
    }
    finite.sort_by(|a, b| a.0.total_cmp(&b.0));
    let total_weight = finite.iter().map(|(_, weight)| weight).sum::<f64>();
    let target_weight = quantile.clamp(0.0, 1.0) * total_weight;
    let mut cumulative_weight = 0.0;
    for (value, weight) in &finite {
        cumulative_weight += weight;
        if cumulative_weight + EPS >= target_weight {
            return Some(*value);
        }
    }
    finite.last().map(|(value, _)| *value)
}

pub(super) fn weighted_winsorized_mean(values: &[(f64, f64)], tail_fraction: f64) -> Option<f64> {
    let lower = weighted_quantile(values, tail_fraction)?;
    let upper = weighted_quantile(values, 1.0 - tail_fraction)?;
    let (weighted_sum, weight_sum) = values
        .iter()
        .filter(|(value, weight)| value.is_finite() && weight.is_finite() && *weight > EPS)
        .fold((0.0, 0.0), |(sum, total), (value, weight)| {
            (sum + value.clamp(lower, upper) * weight, total + weight)
        });
    (weight_sum > EPS).then_some(weighted_sum / weight_sum)
}

#[derive(Clone, Copy)]
pub(super) struct OutcomeSummarySample {
    pub(super) similarity_score: f64,
    pub(super) return_pct: f64,
    pub(super) excess_return_pct: Option<f64>,
    pub(super) mfe_pct: f64,
    pub(super) mae_pct: f64,
}

pub(super) fn summarize_outcomes(
    items: impl IntoIterator<Item = OutcomeSummarySample>,
) -> StrategyTriggerSimilarityOutcomeSummary {
    let weighted = items
        .into_iter()
        .map(|item| (item, (item.similarity_score / 100.0).powi(2)))
        .filter(|(_, w)| *w > EPS)
        .collect::<Vec<_>>();
    let weight_sum = weighted.iter().map(|(_, w)| w).sum::<f64>();
    let weight_sq_sum = weighted.iter().map(|(_, w)| w * w).sum::<f64>();
    if weight_sum <= EPS {
        return StrategyTriggerSimilarityOutcomeSummary {
            sample_count: 0,
            effective_sample_count: 0.0,
            weighted_return_pct: None,
            weighted_excess_return_pct: None,
            shrunk_excess_return_pct: None,
            weighted_positive_rate: None,
            weighted_median_excess_return_pct: None,
            winsorized_excess_return_pct: None,
            weighted_excess_positive_rate: None,
            weighted_mfe_pct: None,
            weighted_mae_pct: None,
        };
    }
    let average = |f: fn(&OutcomeSummarySample) -> f64| {
        weighted.iter().map(|(i, w)| f(i) * w).sum::<f64>() / weight_sum
    };
    let excess = weighted
        .iter()
        .filter_map(|(i, w)| i.excess_return_pct.map(|v| (v, *w)))
        .collect::<Vec<_>>();
    let excess_weight = excess.iter().map(|(_, w)| w).sum::<f64>();
    let weighted_excess = (excess_weight > EPS)
        .then(|| excess.iter().map(|(v, w)| v * w).sum::<f64>() / excess_weight);
    let weighted_median_excess = weighted_quantile(&excess, 0.5);
    let winsorized_excess = weighted_winsorized_mean(&excess, 0.1);
    let weighted_excess_positive_rate = (excess_weight > EPS).then(|| {
        excess
            .iter()
            .map(|(value, weight)| (*value > 0.0) as u8 as f64 * weight)
            .sum::<f64>()
            / excess_weight
            * 100.0
    });
    let effective = weight_sum * weight_sum / weight_sq_sum.max(EPS);
    StrategyTriggerSimilarityOutcomeSummary {
        sample_count: weighted.len(),
        effective_sample_count: effective,
        weighted_return_pct: Some(average(|i| i.return_pct)),
        weighted_excess_return_pct: weighted_excess,
        shrunk_excess_return_pct: weighted_excess
            .map(|v| v * effective / (effective + SHRINKAGE_STRENGTH)),
        weighted_positive_rate: Some(average(|i| (i.return_pct > 0.0) as u8 as f64) * 100.0),
        weighted_median_excess_return_pct: weighted_median_excess,
        winsorized_excess_return_pct: winsorized_excess,
        weighted_excess_positive_rate,
        weighted_mfe_pct: Some(average(|i| i.mfe_pct)),
        weighted_mae_pct: Some(average(|i| i.mae_pct)),
    }
}

pub(super) fn build_rating_sample(
    sorted_items: &[StrategyTriggerSimilarityRow],
    all_trade_dates: &[String],
    window_trade_days: usize,
    outcome_trade_days: usize,
) -> Vec<StrategyTriggerSimilarityRow> {
    let date_indices = all_trade_dates
        .iter()
        .enumerate()
        .map(|(index, date)| (date.as_str(), index))
        .collect::<HashMap<_, _>>();
    let same_stock_exclusion = window_trade_days.max(outcome_trade_days).max(1);
    let outcome_exclusion = outcome_trade_days.max(1);
    let mut selected = Vec::with_capacity(RATING_SAMPLE_LIMIT);
    let mut selected_end_indices = Vec::<usize>::with_capacity(RATING_SAMPLE_LIMIT);
    let mut selected_by_stock = HashMap::<&str, Vec<usize>>::new();

    for item in sorted_items {
        if item.forward_excess_return_pct.is_none() {
            continue;
        }
        let Some(end_index) = date_indices
            .get(item.candidate_end_trade_date.as_str())
            .copied()
        else {
            continue;
        };
        let nearby_outcome_count = selected_end_indices
            .iter()
            .filter(|selected_index| selected_index.abs_diff(end_index) < outcome_exclusion)
            .count();
        if nearby_outcome_count >= RATING_MAX_PER_OUTCOME_WINDOW {
            continue;
        }
        if selected_by_stock
            .get(item.ts_code.as_str())
            .is_some_and(|indices| {
                indices
                    .iter()
                    .any(|selected_index| selected_index.abs_diff(end_index) < same_stock_exclusion)
            })
        {
            continue;
        }
        selected_end_indices.push(end_index);
        selected_by_stock
            .entry(item.ts_code.as_str())
            .or_default()
            .push(end_index);
        selected.push(item.clone());
        if selected.len() >= RATING_SAMPLE_LIMIT {
            break;
        }
    }
    selected
}

#[cfg(test)]
mod tests {
    use crate::trigger_similarity::BenchmarkObservation;
    use crate::trigger_similarity::FutureObservation;
    use crate::trigger_similarity::sample::build_outcome;
    use crate::trigger_similarity::sample::build_rating_sample;
    use crate::trigger_similarity::sample::weighted_quantile;
    use crate::trigger_similarity::sample::weighted_winsorized_mean;
    use crate::trigger_similarity::test_support::*;

    #[test]
    fn robust_outcome_statistics_resist_large_positive_outlier() {
        let values = (0..9)
            .map(|value| (value as f64, 1.0))
            .chain(std::iter::once((100.0, 1.0)))
            .collect::<Vec<_>>();
        assert_eq!(weighted_quantile(&values, 0.5), Some(4.0));
        assert_eq!(weighted_quantile(&values, 0.9), Some(8.0));
        let winsorized = weighted_winsorized_mean(&values, 0.1).expect("winsorized mean");
        assert!((winsorized - 4.4).abs() < 1e-12);
    }

    #[test]
    fn outcome_uses_next_day_open_for_stock_and_benchmark() {
        let future = vec![
            FutureObservation {
                trade_date: "20240102".to_string(),
                open: 100.0,
                close: 105.0,
                high: 110.0,
                low: 95.0,
            },
            FutureObservation {
                trade_date: "20240103".to_string(),
                open: 106.0,
                close: 121.0,
                high: 125.0,
                low: 104.0,
            },
        ];
        let benchmark = std::collections::HashMap::from([
            (
                "20240102".to_string(),
                BenchmarkObservation {
                    open: 100.0,
                    close: 103.0,
                },
            ),
            (
                "20240103".to_string(),
                BenchmarkObservation {
                    open: 104.0,
                    close: 110.0,
                },
            ),
        ]);
        let outcome = build_outcome(&[], &future, 2, &benchmark).expect("outcome");
        assert_eq!(outcome.start_trade_date, "20240102");
        assert!((outcome.return_pct - 21.0).abs() < 1e-9);
        assert!((outcome.excess_return_pct.expect("excess") - 11.0).abs() < 1e-9);
        assert!((outcome.mfe_pct - 25.0).abs() < 1e-9);
        assert!((outcome.mae_pct + 5.0).abs() < 1e-9);
    }

    #[test]
    fn rating_sample_rejects_overlapping_stock_and_market_windows() {
        let dates = (1..=12)
            .map(|day| format!("202401{day:02}"))
            .collect::<Vec<_>>();
        let rows = vec![
            similarity_row("A", "20240105"),
            similarity_row("A", "20240106"),
            similarity_row("B", "20240105"),
            similarity_row("C", "20240105"),
            similarity_row("D", "20240105"),
            similarity_row("E", "20240110"),
        ];
        let selected = build_rating_sample(&rows, &dates, 5, 3);
        assert_eq!(
            selected
                .iter()
                .map(|item| item.ts_code.as_str())
                .collect::<Vec<_>>(),
            vec!["A", "B", "C", "E"]
        );
    }
}
