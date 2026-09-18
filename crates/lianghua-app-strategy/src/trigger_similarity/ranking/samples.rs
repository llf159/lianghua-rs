use crate::trigger_similarity::*;

// 见父模块 mod.rs

use super::*;
use crate::trigger_similarity::channel::build_channel_fingerprint;
use crate::trigger_similarity::channel::build_indicator_channels;
use crate::trigger_similarity::channel::build_price_volume_channels;
use crate::trigger_similarity::channel::cached_channel_similarity;
use crate::trigger_similarity::channel::market_category_features;
use crate::trigger_similarity::fingerprint::build_trigger_fingerprint;
use crate::trigger_similarity::load::load_future_rows;
use crate::trigger_similarity::load::load_market_rows;
use crate::trigger_similarity::load::load_rule_rows;
use crate::trigger_similarity::load::load_summary_rows;
use crate::trigger_similarity::sample::build_outcome;
use crate::trigger_similarity::sample::window_dates_for_anchor;
use duckdb::Connection;
use duckdb::params;
use rayon::prelude::*;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
#[allow(clippy::too_many_arguments)]
pub(in crate::trigger_similarity) fn build_ranking_samples_for_chunk(
    conn: &Connection,
    anchors: Vec<Anchor>,
    schema: &MarketSchema,
    all_trade_dates: &[String],
    environment_fingerprints: &HashMap<String, Arc<ChannelFingerprint>>,
    benchmark_rows: &HashMap<String, BenchmarkObservation>,
    total_mv_map: &HashMap<String, f64>,
    name_map: &HashMap<String, String>,
    pool_segments: usize,
    outcome_trade_days: usize,
    target_trade_date: &str,
    include_outcome: bool,
    progress: Option<(&str, usize, usize)>,
    rule_catalog: &mut RuleCatalog,
) -> Result<Vec<RankingSample>, String> {
    if let Some((phase, completed, total)) = progress {
        set_ranking_progress(phase, "正在读取本批股票的行情和指标窗口", completed, total);
    }
    let market_by_anchor = load_market_rows(conn, &anchors, schema)?;
    if let Some((phase, completed, total)) = progress {
        set_ranking_progress(phase, "正在读取本批股票的策略触发窗口", completed, total);
    }
    let rules_by_anchor = load_rule_rows(conn, &anchors, rule_catalog)?;
    // 历史模板不使用原始评分和排名，只有当日目标需要展示这两个字段。
    let summaries = if include_outcome {
        HashMap::new()
    } else {
        if let Some((phase, completed, total)) = progress {
            set_ranking_progress(phase, "正在读取本批股票的评分摘要", completed, total);
        }
        load_summary_rows(conn, &anchors)?
    };
    let future_by_anchor = if include_outcome {
        if let Some((phase, completed, total)) = progress {
            set_ranking_progress(phase, "正在读取本批历史模板的后验行情", completed, total);
        }
        load_future_rows(conn, &anchors, outcome_trade_days, target_trade_date)?
    } else {
        HashMap::new()
    };
    if let Some((phase, completed, total)) = progress {
        set_ranking_progress(phase, "正在构建本批股票的指纹", completed, total);
    }
    Ok(anchors
        .into_par_iter()
        .filter_map(|anchor| {
            let market_rows = market_by_anchor.get(&anchor.id)?;
            if market_rows.len() < 3
                || market_rows.last().map(|row| row.trade_date.as_str())
                    != Some(anchor.end_trade_date.as_str())
            {
                return None;
            }
            let rules = rules_by_anchor
                .get(&anchor.id)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let window_dates = window_dates_for_anchor(&anchor, all_trade_dates);
            let outcome = if include_outcome {
                build_outcome(
                    market_rows,
                    future_by_anchor
                        .get(&anchor.id)
                        .map(Vec::as_slice)
                        .unwrap_or(&[]),
                    outcome_trade_days,
                    benchmark_rows,
                )
            } else {
                None
            };
            if include_outcome && outcome.is_none() {
                return None;
            }
            let trigger = (|events: &[RuleEvent],
                            window_dates: &[String],
                            segments: usize|
             -> TriggerFingerprint {
                build_trigger_fingerprint(events, window_dates, segments)
            })(rules, window_dates, pool_segments);
            if trigger.by_rule.is_empty() {
                return None;
            }
            let (total_score, original_rank) =
                summaries.get(&anchor.id).copied().unwrap_or((None, None));
            Some(RankingSample {
                fingerprint: RankingFingerprint {
                    trigger,
                    price_volume: build_channel_fingerprint(build_price_volume_channels(
                        market_rows,
                        pool_segments,
                        total_mv_map.get(&anchor.ts_code).copied(),
                        market_category_features(
                            &anchor.ts_code,
                            name_map.get(&anchor.ts_code).map(String::as_str),
                        ),
                    )),
                    indicators: build_channel_fingerprint(build_indicator_channels(
                        market_rows,
                        schema.indicator_columns.len(),
                        pool_segments,
                    )),
                    market: environment_fingerprints
                        .get(&anchor.end_trade_date)
                        .cloned()
                        .unwrap_or_else(|| -> Arc<ChannelFingerprint> {
                            static EMPTY: OnceLock<Arc<ChannelFingerprint>> = OnceLock::new();
                            Arc::clone(
                                EMPTY.get_or_init(|| {
                                    Arc::new(build_channel_fingerprint(Vec::new()))
                                }),
                            )
                        }),
                },
                trigger_count: rules.len(),
                outcome,
                total_score,
                original_rank,
                template_quality_score: None,
                template_class: 0,
                anchor,
            })
        })
        .collect())
}

#[allow(clippy::too_many_arguments)]
pub(in crate::trigger_similarity) fn load_outcome_selected_anchors(
    conn: &Connection,
    earliest_date: &str,
    cutoff_date: &str,
    target_date: &str,
    all_trade_dates: &[String],
    window_trade_days: usize,
    outcome_trade_days: usize,
    benchmark_rows: &HashMap<String, BenchmarkObservation>,
    environment_fingerprints: &HashMap<String, Arc<ChannelFingerprint>>,
    target_market: Option<&ChannelFingerprint>,
    sample_gap_trade_days: usize,
) -> Result<(Vec<OutcomeSelectedAnchor>, usize), String> {
    let date_index = all_trade_dates
        .iter()
        .enumerate()
        .map(|(index, date)| (date.as_str(), index))
        .collect::<HashMap<_, _>>();
    let earliest_index = date_index
        .get(earliest_date)
        .copied()
        .ok_or_else(|| format!("历史起始日不在评分交易日中: {earliest_date}"))?;
    let cutoff_index = date_index
        .get(cutoff_date)
        .copied()
        .ok_or_else(|| format!("历史截止日不在评分交易日中: {cutoff_date}"))?;
    let target_index = date_index
        .get(target_date)
        .copied()
        .ok_or_else(|| format!("参考日不在评分交易日中: {target_date}"))?;
    let sample_cutoff_index = cutoff_index.min(target_index.saturating_sub(sample_gap_trade_days));

    let mut scored_dates = HashMap::<String, Vec<bool>>::new();
    let mut score_stmt = conn
        .prepare(
            "SELECT ts_code, trade_date FROM score_summary \
             WHERE trade_date>=? AND trade_date<=?",
        )
        .map_err(|e| format!("预编译评分日期扫描失败: {e}"))?;
    let mut score_rows = score_stmt
        .query(params![earliest_date, cutoff_date])
        .map_err(|e| format!("查询评分日期扫描失败: {e}"))?;
    while let Some(row) = score_rows
        .next()
        .map_err(|e| format!("读取评分日期扫描失败: {e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取评分股票失败: {e}"))?;
        let trade_date: String = row.get(1).map_err(|e| format!("读取评分日期失败: {e}"))?;
        if let Some(index) = date_index.get(trade_date.as_str()) {
            scored_dates
                .entry(ts_code)
                .or_insert_with(|| vec![false; all_trade_dates.len()])[*index] = true;
        }
    }
    drop(score_rows);
    drop(score_stmt);

    let sql = r#"
        SELECT s.ts_code, s.trade_date,
               TRY_CAST(s.open AS DOUBLE), TRY_CAST(s.high AS DOUBLE),
               TRY_CAST(s.low AS DOUBLE), TRY_CAST(s.close AS DOUBLE),
               TRY_CAST(s.pct_chg AS DOUBLE)
        FROM trigger_market_db.stock_data s
        WHERE s.adj_type='qfq' AND s.trade_date>=? AND s.trade_date<=?
    "#;
    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| format!("预编译线性未来表现扫描失败: {e}"))?;
    let mut rows = stmt
        .query(params![earliest_date, target_date])
        .map_err(|e| format!("查询线性未来表现扫描失败: {e}"))?;
    let mut stock_codes = Vec::<String>::new();
    let mut labels = Vec::<RawOutcomeLabel>::new();
    let mut paths = HashMap::<String, Vec<OutcomePathRow>>::new();

    let flush_stock = |ts_code: &str,
                       path: &mut Vec<OutcomePathRow>,
                       stock_codes: &mut Vec<String>,
                       labels: &mut Vec<RawOutcomeLabel>| {
        if ts_code.is_empty() || path.len() <= outcome_trade_days {
            path.clear();
            return;
        }
        let stock_index = stock_codes.len();
        stock_codes.push(ts_code.to_string());
        path.sort_unstable_by_key(|row| row.date_index);
        let Some(stock_scored_dates) = scored_dates.get(ts_code) else {
            path.clear();
            return;
        };
        for anchor_position in 0..path.len().saturating_sub(outcome_trade_days) {
            let anchor = &path[anchor_position];
            if anchor.date_index < earliest_index || anchor.date_index > sample_cutoff_index {
                continue;
            }
            if !stock_scored_dates[anchor.date_index] {
                continue;
            }
            let future = &path[anchor_position + 1..=anchor_position + outcome_trade_days];
            if future.iter().any(|row| row.close.is_none()) {
                continue;
            }
            let Some(entry) = future[0]
                .open
                .filter(|value| value.is_finite() && value.abs() > EPS)
            else {
                continue;
            };
            let Some(exit) = future
                .last()
                .and_then(|row| row.close)
                .filter(|v| v.is_finite())
            else {
                continue;
            };
            let future_high = future
                .iter()
                .filter_map(|row| row.high.filter(|value| value.is_finite()))
                .fold(f64::NEG_INFINITY, f64::max);
            let future_low = future
                .iter()
                .filter_map(|row| row.low.filter(|value| value.is_finite()))
                .fold(f64::INFINITY, f64::min);
            if !future_high.is_finite() || !future_low.is_finite() {
                continue;
            }
            let Some(benchmark_start) = benchmark_rows.get(&all_trade_dates[future[0].date_index])
            else {
                continue;
            };
            let Some(benchmark_end) =
                benchmark_rows.get(&all_trade_dates[future.last().unwrap().date_index])
            else {
                continue;
            };
            if benchmark_start.open.abs() <= EPS {
                continue;
            }
            let return_pct = (exit / entry - 1.0) * 100.0;
            labels.push(RawOutcomeLabel {
                stock_index,
                date_index: anchor.date_index,
                excess_return_pct: return_pct
                    - (benchmark_end.close / benchmark_start.open - 1.0) * 100.0,
                mfe_pct: (future_high / entry - 1.0) * 100.0,
                mae_pct: (future_low / entry - 1.0) * 100.0,
                persistence: future
                    .iter()
                    .filter(|row| row.pct_chg.is_some_and(|value| value > 0.0))
                    .count() as f64
                    / outcome_trade_days as f64,
            });
        }
        path.clear();
    };

    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取线性未来表现失败: {e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取股票代码失败: {e}"))?;
        let trade_date: String = row.get(1).map_err(|e| format!("读取行情日期失败: {e}"))?;
        let Some(row_date_index) = date_index.get(trade_date.as_str()).copied() else {
            continue;
        };
        paths.entry(ts_code).or_default().push(OutcomePathRow {
            date_index: row_date_index,
            open: row.get(2).map_err(|e| format!("读取开盘价失败: {e}"))?,
            high: row.get(3).map_err(|e| format!("读取最高价失败: {e}"))?,
            low: row.get(4).map_err(|e| format!("读取最低价失败: {e}"))?,
            close: row.get(5).map_err(|e| format!("读取收盘价失败: {e}"))?,
            pct_chg: row.get(6).map_err(|e| format!("读取涨跌幅失败: {e}"))?,
        });
    }
    drop(rows);
    drop(stmt);
    for (ts_code, mut path) in paths {
        flush_stock(&ts_code, &mut path, &mut stock_codes, &mut labels);
    }

    let mut labels_by_date = vec![Vec::<usize>::new(); all_trade_dates.len()];
    for (label_index, label) in labels.iter().enumerate() {
        labels_by_date[label.date_index].push(label_index);
    }
    let quality_pairs = labels_by_date
        .par_iter()
        .flat_map_iter(|indices| {
            if indices.is_empty() {
                return Vec::new();
            }
            let mut ranks = vec![[0.0; 4]; indices.len()];
            for component in 0..4 {
                let mut ordered = indices
                    .iter()
                    .enumerate()
                    .map(|(local_index, label_index)| {
                        let label = labels[*label_index];
                        let value = match component {
                            0 => label.excess_return_pct,
                            1 => label.mfe_pct,
                            2 => label.mae_pct,
                            _ => label.persistence,
                        };
                        (local_index, value)
                    })
                    .collect::<Vec<_>>();
                ordered.sort_unstable_by(|left, right| left.1.total_cmp(&right.1));
                let denominator = ordered.len().saturating_sub(1) as f64;
                let mut position = 0;
                while position < ordered.len() {
                    let mut end = position + 1;
                    while end < ordered.len()
                        && ordered[end].1.total_cmp(&ordered[position].1) == Ordering::Equal
                    {
                        end += 1;
                    }
                    let percent_rank = if denominator <= 0.0 {
                        0.0
                    } else {
                        position as f64 / denominator
                    };
                    for &(local_index, _) in &ordered[position..end] {
                        ranks[local_index][component] = percent_rank;
                    }
                    position = end;
                }
            }
            indices
                .iter()
                .enumerate()
                .map(|(local_index, label_index)| {
                    let rank = ranks[local_index];
                    (
                        *label_index,
                        0.45 * rank[0] + 0.25 * rank[1] + 0.20 * rank[2] + 0.10 * rank[3],
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut quality = vec![0.0; labels.len()];
    for (label_index, score) in quality_pairs {
        quality[label_index] = score;
    }

    let mut trigger_dates = HashMap::<String, Vec<bool>>::new();
    let mut trigger_stmt = conn
        .prepare(
            "SELECT ts_code, trade_date FROM rule_details \
             WHERE trade_date>=? AND trade_date<=? \
               AND TRY_CAST(rule_score AS DOUBLE) IS NOT NULL \
               AND ABS(TRY_CAST(rule_score AS DOUBLE)) > ?",
        )
        .map_err(|e| format!("预编译策略触发线性扫描失败: {e}"))?;
    let mut trigger_rows = trigger_stmt
        .query(params![&all_trade_dates[0], cutoff_date, EPS])
        .map_err(|e| format!("查询策略触发线性扫描失败: {e}"))?;
    while let Some(row) = trigger_rows
        .next()
        .map_err(|e| format!("读取策略触发线性扫描失败: {e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取触发股票失败: {e}"))?;
        let trade_date: String = row.get(1).map_err(|e| format!("读取触发日期失败: {e}"))?;
        if let Some(index) = date_index.get(trade_date.as_str()) {
            trigger_dates
                .entry(ts_code)
                .or_insert_with(|| vec![false; all_trade_dates.len()])[*index] = true;
        }
    }

    #[derive(Clone, Copy)]
    struct SelectedLabel {
        stock_index: usize,
        date_index: usize,
        quality_score: f64,
        quality_class: i8,
    }
    let mut selected = Vec::<SelectedLabel>::new();
    let mut previous_stock = usize::MAX;
    let mut previous_quality = None;
    for (label_index, label) in labels.iter().enumerate() {
        if label.stock_index != previous_stock {
            previous_stock = label.stock_index;
            previous_quality = None;
        }
        let score = quality[label_index];
        let quality_class = if score >= SUCCESS_QUALITY_THRESHOLD
            && previous_quality.unwrap_or(0.0) < SUCCESS_QUALITY_THRESHOLD
        {
            1
        } else if score <= FAILURE_QUALITY_THRESHOLD
            && previous_quality.unwrap_or(1.0) > FAILURE_QUALITY_THRESHOLD
        {
            -1
        } else {
            0
        };
        previous_quality = Some(score);
        if quality_class == 0 {
            continue;
        }
        let stock_code = &stock_codes[label.stock_index];
        let window_start = (label.date_index + 1).saturating_sub(window_trade_days);
        let has_trigger = trigger_dates.get(stock_code).is_some_and(|dates| {
            dates[window_start..=label.date_index]
                .iter()
                .any(|triggered| *triggered)
        });
        if has_trigger {
            selected.push(SelectedLabel {
                stock_index: label.stock_index,
                date_index: label.date_index,
                quality_score: score,
                quality_class,
            });
        }
    }
    let universe_count = selected.len();

    fn stable_label_hash(stock: &str, date_index: usize) -> u64 {
        let mut hash = 0xcbf29ce484222325_u64;
        for byte in stock.bytes().chain(date_index.to_le_bytes()) {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }
    // 市场相似度只取决于锚点日期，按交易日预计算一次，避免排序比较器反复做高维点积。
    let market_similarity_by_date = target_market.map(|target_market| {
        let mut scores = vec![f64::NEG_INFINITY; all_trade_dates.len()];
        let limit = (cutoff_index + 1).min(all_trade_dates.len());
        for (index, score) in scores.iter_mut().enumerate().take(limit) {
            *score = environment_fingerprints
                .get(&all_trade_dates[index])
                .and_then(|candidate| cached_channel_similarity(target_market, candidate))
                .unwrap_or(f64::NEG_INFINITY);
        }
        scores
    });
    let market_similarity_of = |row: &SelectedLabel| -> f64 {
        market_similarity_by_date
            .as_ref()
            .map(|scores| scores[row.date_index])
            .unwrap_or(0.0)
    };
    let mut chosen = Vec::<SelectedLabel>::new();
    for quality_class in [1_i8, -1_i8] {
        let mut class_rows = selected
            .iter()
            .copied()
            .filter(|row| row.quality_class == quality_class)
            .collect::<Vec<_>>();
        class_rows.sort_unstable_by(|left, right| {
            right.date_index.cmp(&left.date_index).then_with(|| {
                stable_label_hash(&stock_codes[left.stock_index], left.date_index).cmp(
                    &stable_label_hash(&stock_codes[right.stock_index], right.date_index),
                )
            })
        });
        let recent_limit = RECENT_CANDIDATE_ANCHORS / 2;
        let diverse_limit = HISTORY_DIVERSITY_ANCHORS / 2;
        let remaining = class_rows.split_off(class_rows.len().min(recent_limit));
        chosen.extend(class_rows);

        // 历史分散样本按时间分桶，每段市场阶段内先按与目标市场的环境相似度排序，
        // 再各桶轮流取，避免市场环境通道又只命中离当前最近的事件。
        let bucket_span = (sample_cutoff_index + 1).max(1);
        let mut buckets = vec![Vec::<SelectedLabel>::new(); MARKET_HISTORY_BUCKETS];
        for row in remaining {
            let bucket = (row.date_index * MARKET_HISTORY_BUCKETS / bucket_span)
                .min(MARKET_HISTORY_BUCKETS - 1);
            buckets[bucket].push(row);
        }
        for bucket in &mut buckets {
            bucket.sort_unstable_by(|left, right| {
                market_similarity_of(right)
                    .total_cmp(&market_similarity_of(left))
                    .then_with(|| {
                        stable_label_hash(&stock_codes[left.stock_index], left.date_index).cmp(
                            &stable_label_hash(&stock_codes[right.stock_index], right.date_index),
                        )
                    })
            });
        }
        let mut cursors = vec![0_usize; buckets.len()];
        let mut picked = 0_usize;
        while picked < diverse_limit {
            let mut progressed = false;
            for (bucket_index, bucket) in buckets.iter().enumerate() {
                if picked >= diverse_limit {
                    break;
                }
                if let Some(row) = bucket.get(cursors[bucket_index]) {
                    chosen.push(*row);
                    cursors[bucket_index] += 1;
                    picked += 1;
                    progressed = true;
                }
            }
            if !progressed {
                break;
            }
        }
    }

    chosen.sort_unstable_by(|left, right| {
        right
            .date_index
            .cmp(&left.date_index)
            .then_with(|| left.stock_index.cmp(&right.stock_index))
    });
    let anchors = chosen
        .into_iter()
        .enumerate()
        .map(|(id, selected)| {
            let start_index = (selected.date_index + 1).saturating_sub(window_trade_days);
            OutcomeSelectedAnchor {
                anchor: Anchor {
                    id,
                    ts_code: stock_codes[selected.stock_index].clone(),
                    start_trade_date: all_trade_dates[start_index].clone(),
                    end_trade_date: all_trade_dates[selected.date_index].clone(),
                },
                quality_score: selected.quality_score,
                quality_class: selected.quality_class,
            }
        })
        .collect();
    Ok((anchors, universe_count))
}

pub(super) fn assign_ranks(rows: &mut [StrategyTriggerRankingRow]) {
    rows.sort_by(
        |left, right| match (left.prediction_signal, right.prediction_signal) {
            (Some(left_signal), Some(right_signal)) => right_signal
                .total_cmp(&left_signal)
                .then_with(|| {
                    right
                        .excess_positive_rate
                        .unwrap_or(f64::NEG_INFINITY)
                        .total_cmp(&left.excess_positive_rate.unwrap_or(f64::NEG_INFINITY))
                })
                .then_with(|| {
                    right
                        .best_similarity
                        .unwrap_or(f64::NEG_INFINITY)
                        .total_cmp(&left.best_similarity.unwrap_or(f64::NEG_INFINITY))
                })
                .then_with(|| left.ts_code.cmp(&right.ts_code)),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => left.ts_code.cmp(&right.ts_code),
        },
    );
    let ranked_count = rows
        .iter()
        .filter(|row| row.prediction_signal.is_some())
        .count();
    for (index, row) in rows.iter_mut().take(ranked_count).enumerate() {
        row.rank = Some(index + 1);
        row.ranking_score = Some(if ranked_count <= 1 {
            100.0
        } else {
            100.0 * (ranked_count - 1 - index) as f64 / (ranked_count - 1) as f64
        });
    }
}

#[cfg(test)]
mod tests {
    use crate::trigger_similarity::MIN_SAMPLE_GAP_TRADE_DAYS;
    use crate::trigger_similarity::load::load_all_trade_dates;
    use crate::trigger_similarity::load::load_market_schema;
    use crate::trigger_similarity::load::open_result_conn;
    use crate::trigger_similarity::ranking::StrategyTriggerRankingRow;
    use crate::trigger_similarity::ranking::samples::assign_ranks;
    use crate::trigger_similarity::ranking::samples::build_ranking_samples_for_chunk;
    use crate::trigger_similarity::ranking::samples::load_outcome_selected_anchors;
    use crate::trigger_similarity::sample::load_benchmark_rows;
    use std::collections::HashMap;

    fn empty_row(code: &str, signal: Option<f64>) -> StrategyTriggerRankingRow {
        StrategyTriggerRankingRow {
            rank: None,
            ts_code: code.to_string(),
            name: None,
            industry: None,
            concept: None,
            board: None,
            original_score: None,
            original_rank: None,
            best_rank_3d: None,
            ranking_score: None,
            prediction_signal: signal,
            confidence: 0.0,
            sample_count: 0,
            effective_sample_count: 0.0,
            expected_return_pct: None,
            expected_excess_return_pct: None,
            shrunk_excess_return_pct: None,
            excess_positive_rate: None,
            expected_mfe_pct: None,
            expected_mae_pct: None,
            average_similarity: None,
            best_similarity: None,
            trigger_count: 0,
            total_mv_yi: None,
            top_matches: Vec::new(),
        }
    }

    #[test]
    fn sample_builders_skip_unused_summary_and_preloaded_rule_queries() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch("ATTACH ':memory:' AS trigger_market_db;
            CREATE TABLE trigger_market_db.stock_data(ts_code VARCHAR, trade_date VARCHAR, adj_type VARCHAR, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE);
            INSERT INTO trigger_market_db.stock_data VALUES
            ('A', '20240101', 'qfq', 10, 12, 9, 11),
            ('A', '20240102', 'qfq', 11, 13, 10, 12),
            ('A', '20240103', 'qfq', 12, 14, 11, 13),
            ('A', '20240104', 'qfq', 13, 15, 12, 14);
            CREATE TABLE rule_details(ts_code VARCHAR, trade_date VARCHAR, rule_name VARCHAR, rule_score DOUBLE);
            INSERT INTO rule_details VALUES ('A', '20240103', '启动', 1.0);").unwrap();
        let schema = load_market_schema(&conn).unwrap();
        let dates = (1..=4)
            .map(|day| format!("202401{day:02}"))
            .collect::<Vec<_>>();
        let anchor = crate::trigger_similarity::Anchor {
            id: 7,
            ts_code: "A".into(),
            start_trade_date: dates[0].clone(),
            end_trade_date: dates[2].clone(),
        };
        let benchmark = HashMap::from([(
            dates[3].clone(),
            crate::trigger_similarity::BenchmarkObservation {
                open: 10.0,
                close: 11.0,
            },
        )]);
        let environment = HashMap::new();
        let names = HashMap::new();
        let market_caps = HashMap::new();
        let mut catalog = crate::trigger_similarity::RuleCatalog::default();
        // 不创建 score_summary：历史模板若仍查询摘要，这里会直接失败。
        let historical = build_ranking_samples_for_chunk(
            &conn,
            vec![anchor.clone()],
            &schema,
            &dates,
            &environment,
            &benchmark,
            &market_caps,
            &names,
            2,
            1,
            &dates[3],
            true,
            None,
            &mut catalog,
        )
        .unwrap();
        assert_eq!(historical.len(), 1);
        assert!(historical[0].total_score.is_none());
        assert!(historical[0].original_rank.is_none());
        assert!(historical[0].outcome.is_some());
        let rules = crate::trigger_similarity::load::load_rule_rows(
            &conn,
            std::slice::from_ref(&anchor),
            &mut catalog,
        )
        .unwrap();
        let context = crate::trigger_similarity::sample::SampleBuildContext {
            schema: &schema,
            all_trade_dates: &dates,
            environment_fingerprints: &environment,
            benchmark_rows: &benchmark,
            total_mv_map: &market_caps,
            name_map: &names,
            pool_segments: 2,
            outcome_trade_days: 1,
            target_trade_date: &dates[3],
            include_outcome: false,
            include_summaries: false,
        };
        let original = crate::trigger_similarity::sample::build_samples_for_chunk(
            &conn,
            vec![anchor.clone()],
            &context,
            &mut catalog,
            None,
        )
        .unwrap();
        conn.execute_batch("DROP TABLE rule_details").unwrap();
        let reused = crate::trigger_similarity::sample::build_samples_for_chunk(
            &conn,
            vec![anchor],
            &context,
            &mut catalog,
            Some(rules),
        )
        .unwrap();
        assert_eq!(reused.len(), 1);
        assert_eq!(reused[0].trigger_count, original[0].trigger_count);
        assert_eq!(
            reused[0].fingerprint.dimension(),
            original[0].fingerprint.dimension()
        );
        assert!(
            (crate::trigger_similarity::fingerprint::trigger_fingerprint_similarity(
                &reused[0].fingerprint.trigger,
                &original[0].fingerprint.trigger,
                &[]
            ) - 100.0)
                .abs()
                < 1e-9
        );
    }

    #[test]
    fn rank_assignment_puts_unrated_rows_last() {
        let mut rows = vec![
            empty_row("B", None),
            empty_row("A", Some(2.0)),
            empty_row("C", Some(1.0)),
        ];
        assign_ranks(&mut rows);
        assert_eq!(
            rows.iter()
                .map(|row| row.ts_code.as_str())
                .collect::<Vec<_>>(),
            vec!["A", "C", "B"]
        );
        assert_eq!(rows[0].rank, Some(1));
        assert_eq!(rows[2].rank, None);
    }

    #[test]
    #[ignore = "requires LIANGHUA_BENCH_DATA_DIR and a real dataset"]
    fn benchmark_real_outcome_anchor_scan() {
        let source_path = std::env::var("LIANGHUA_BENCH_DATA_DIR")
            .expect("set LIANGHUA_BENCH_DATA_DIR to a real source directory");
        let conn = open_result_conn(&source_path).expect("open real source databases");
        let all_trade_dates = load_all_trade_dates(&conn).expect("load scoring calendar");
        let target_date = all_trade_dates.last().expect("target date").clone();
        let horizon = 5;
        let cutoff_index = all_trade_dates.len() - 1 - horizon;
        let earliest_date = all_trade_dates[19].clone();
        let cutoff_date = all_trade_dates[cutoff_index].clone();
        let benchmark_rows = load_benchmark_rows(&conn, &earliest_date, &target_date, "000001.SH")
            .expect("load benchmark");
        let started = std::time::Instant::now();
        let (anchors, universe_count) = load_outcome_selected_anchors(
            &conn,
            &earliest_date,
            &cutoff_date,
            &target_date,
            &all_trade_dates,
            20,
            horizon,
            &benchmark_rows,
            &HashMap::new(),
            None,
            MIN_SAMPLE_GAP_TRADE_DAYS,
        )
        .expect("scan real outcome labels");
        eprintln!(
            "real outcome scan: elapsed={:?}, selected={}, universe={}",
            started.elapsed(),
            anchors.len(),
            universe_count
        );
        assert!(!anchors.is_empty());
    }
}
