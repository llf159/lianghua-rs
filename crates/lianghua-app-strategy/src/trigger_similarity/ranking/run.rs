use crate::trigger_similarity::ranking::samples::{
    assign_ranks, build_ranking_samples_for_chunk, load_outcome_selected_anchors,
};
use crate::trigger_similarity::ranking::store::{
    config_key, ensure_ranking_tables, load_active_config_record, load_data_signature,
    parse_config_key, stable_content_signature,
};
use crate::trigger_similarity::*;

// 见父模块 mod.rs

use super::*;
use crate::data::ind_toml_path;
use crate::data::result_db_path;
use crate::data::score_rule_path;
use crate::trigger_similarity::channel::build_environment_fingerprint_map;
use crate::trigger_similarity::channel::cached_channel_similarity;
use crate::trigger_similarity::channel::final_similarity;
use crate::trigger_similarity::channel::share_environment_fingerprints;
use crate::trigger_similarity::fingerprint::combine_trigger_similarity;
use crate::trigger_similarity::fingerprint::load_rule_idf_weights;
use crate::trigger_similarity::fingerprint::rule_weight;
use crate::trigger_similarity::fingerprint::trigger_aggregate_similarity;
use crate::trigger_similarity::fingerprint::trigger_rule_weight_sum;
use crate::trigger_similarity::fingerprint::weighted_rule_set_similarity_from_masses;
use crate::trigger_similarity::fingerprint::weighted_rule_timing_similarity_with_masses;
use crate::trigger_similarity::load::load_all_trade_dates;
use crate::trigger_similarity::load::load_market_environment;
use crate::trigger_similarity::load::load_market_schema;
use crate::trigger_similarity::load::open_result_conn;
use crate::trigger_similarity::load::resolve_benchmark_index_code;
use crate::trigger_similarity::load::resolve_existing_trade_date;
use crate::trigger_similarity::sample::OutcomeSummarySample;
use crate::trigger_similarity::sample::load_benchmark_rows;
use crate::trigger_similarity::sample::summarize_outcomes;
use duckdb::Connection;
use duckdb::params;
use lianghua_app_shared::build_concepts_map;
use lianghua_app_shared::build_industry_map;
use lianghua_app_shared::build_name_map;
use lianghua_app_shared::build_total_mv_map;
use rayon::prelude::*;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::time::Instant;
#[allow(clippy::too_many_arguments)]
pub fn run_strategy_trigger_similarity_ranking(
    source_path: String,
    trade_date: Option<String>,
    window_trade_days: Option<u32>,
    pool_segments: Option<u32>,
    outcome_trade_days: Option<u32>,
    benchmark_index_code: Option<String>,
    limit: Option<u32>,
    board: Option<String>,
    exclude_st_board: Option<bool>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
    sample_gap_trade_days: Option<u32>,
) -> Result<StrategyTriggerRankingPageData, String> {
    let _guard = RANKING_COMPUTE_LOCK
        .get_or_init(|| Mutex::new(()))
        .try_lock()
        .map_err(|_| "全市场相似排行榜正在计算，请等待当前任务完成".to_string())?;
    let started = Instant::now();
    *RANKING_PROGRESS
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("相似排行榜进度锁被污染") = None;
    set_ranking_progress("prepare", "读取行情、策略触发和市场环境", 0, 0);
    let source_path = source_path.trim().to_string();
    if source_path.is_empty() {
        return Err("source_path 不能为空".to_string());
    }
    let conn = open_result_conn(&source_path)?;
    let resolved_trade_date = resolve_existing_trade_date(&conn, trade_date)?;
    let active_config = get_strategy_trigger_similarity_active_config(&conn)?;
    let window_trade_days = window_trade_days
        .map(|v| v as usize)
        .filter(|v| *v >= 3)
        .or_else(|| {
            active_config
                .as_ref()
                .map(|config| config.window_trade_days)
        })
        .unwrap_or(DEFAULT_WINDOW_TRADE_DAYS);
    let pool_segments = pool_segments
        .map(|v| v as usize)
        .filter(|v| *v > 0)
        .or_else(|| active_config.as_ref().map(|config| config.pool_segments))
        .unwrap_or(DEFAULT_POOL_SEGMENTS)
        .min(MAX_POOL_SEGMENTS)
        .min(window_trade_days);
    let outcome_trade_days = outcome_trade_days
        .map(|v| v as usize)
        .filter(|v| *v > 0)
        .or_else(|| {
            active_config
                .as_ref()
                .map(|config| config.outcome_trade_days)
        })
        .unwrap_or(DEFAULT_OUTCOME_TRADE_DAYS);
    let sample_gap_trade_days = sample_gap_trade_days
        .map(|v| v as usize)
        .or_else(|| {
            active_config
                .as_ref()
                .map(|config| config.sample_gap_trade_days)
        })
        .unwrap_or(MIN_SAMPLE_GAP_TRADE_DAYS);
    let benchmark_index_code =
        resolve_benchmark_index_code(benchmark_index_code.as_deref().or_else(|| {
            active_config
                .as_ref()
                .map(|config| config.benchmark_index_code.as_str())
        }))?;
    let initial_signature = load_data_signature(&conn, &source_path, &resolved_trade_date)?;
    let mut timings = Vec::new();

    let phase = Instant::now();
    let all_trade_dates = load_all_trade_dates(&conn)?;
    let target_index = all_trade_dates
        .binary_search(&resolved_trade_date)
        .map_err(|_| format!("参考日不在评分交易日中: {resolved_trade_date}"))?;
    let candidate_gap_trade_days = outcome_trade_days.max(sample_gap_trade_days);
    if target_index < candidate_gap_trade_days {
        return Err(format!(
            "参考日前不足 {candidate_gap_trade_days} 个交易日，无法隔离最近样本"
        ));
    }
    let target_start_index = (target_index + 1).saturating_sub(window_trade_days);
    let target_start_date = all_trade_dates[target_start_index].clone();
    let historical_cutoff_date = all_trade_dates[target_index - candidate_gap_trade_days].clone();
    let earliest_candidate_date = all_trade_dates
        .get(window_trade_days.saturating_sub(1))
        .map(String::as_str)
        .unwrap_or(&all_trade_dates[0]);
    let schema = load_market_schema(&conn)?;
    let first_date = all_trade_dates
        .first()
        .map(String::as_str)
        .unwrap_or(&target_start_date);
    let environment = load_market_environment(&conn, first_date, &resolved_trade_date, &schema)?;
    let environment_fingerprints =
        share_environment_fingerprints(build_environment_fingerprint_map(
            &environment,
            &all_trade_dates,
            window_trade_days,
            pool_segments,
        ));
    let benchmark_rows = load_benchmark_rows(
        &conn,
        first_date,
        &resolved_trade_date,
        &benchmark_index_code,
    )?;
    let total_mv_map = build_total_mv_map(&source_path).unwrap_or_default();
    let name_map = build_name_map(&source_path).unwrap_or_default();
    timings.push(StrategyTriggerRankingTiming {
        label: "市场环境与基准".to_string(),
        elapsed_ms: elapsed_ms(phase),
    });

    let phase = Instant::now();
    set_ranking_progress("select-templates", "正在扫描历史表现并筛选模板", 0, 0);
    let (selected_anchors, candidate_universe_count) = load_outcome_selected_anchors(
        &conn,
        earliest_candidate_date,
        &historical_cutoff_date,
        &resolved_trade_date,
        &all_trade_dates,
        window_trade_days,
        outcome_trade_days,
        &benchmark_rows,
        &environment_fingerprints,
        environment_fingerprints
            .get(&resolved_trade_date)
            .map(Arc::as_ref),
        sample_gap_trade_days,
    )?;
    let selected_quality = selected_anchors
        .iter()
        .map(|selected| {
            (
                selected.anchor.id,
                (selected.quality_score, selected.quality_class),
            )
        })
        .collect::<HashMap<_, _>>();
    let candidate_anchor_count = selected_anchors.len();
    let candidate_anchors = selected_anchors
        .into_iter()
        .map(|selected| selected.anchor)
        .collect::<Vec<_>>();
    let mut rule_catalog = RuleCatalog::default();
    let mut candidates = Vec::with_capacity(candidate_anchor_count);
    let mut candidate_anchor_iter = candidate_anchors.into_iter();
    set_ranking_progress(
        "candidate-fingerprints",
        "正在计算历史模板指纹",
        0,
        candidate_anchor_count,
    );
    let mut candidate_completed = 0;
    loop {
        let chunk = candidate_anchor_iter
            .by_ref()
            .take(ANCHOR_CHUNK_SIZE)
            .collect::<Vec<_>>();
        if chunk.is_empty() {
            break;
        }
        let chunk_len = chunk.len();
        let mut chunk_samples = build_ranking_samples_for_chunk(
            &conn,
            chunk,
            &schema,
            &all_trade_dates,
            &environment_fingerprints,
            &benchmark_rows,
            &total_mv_map,
            &name_map,
            pool_segments,
            outcome_trade_days,
            &resolved_trade_date,
            true,
            Some((
                "candidate-fingerprints",
                candidate_completed,
                candidate_anchor_count,
            )),
            &mut rule_catalog,
        )?;
        for sample in &mut chunk_samples {
            if let Some((quality_score, quality_class)) = selected_quality.get(&sample.anchor.id) {
                sample.template_quality_score = Some(*quality_score);
                sample.template_class = *quality_class;
            }
        }
        candidates.extend(chunk_samples);
        candidate_completed += chunk_len;
        set_ranking_progress(
            "candidate-fingerprints",
            "正在计算历史模板指纹",
            candidate_completed.min(candidate_anchor_count),
            candidate_anchor_count,
        );
    }
    let evaluated_anchor_count = candidates.len();
    timings.push(StrategyTriggerRankingTiming {
        label: "表现反推启动模板与指纹".to_string(),
        elapsed_ms: elapsed_ms(phase),
    });

    let phase = Instant::now();
    let target_anchors = (|conn: &Connection,
                           target_date: &str,
                           start_date: &str|
     -> Result<Vec<Anchor>, String> {
        let mut stmt = conn
        .prepare(
            "SELECT ts_code FROM score_summary WHERE trade_date = ? ORDER BY rank NULLS LAST, ts_code",
        )
        .map_err(|e| format!("预编译当日股票池失败: {e}"))?;
        let rows = stmt
            .query_map(params![target_date], |row| row.get::<_, String>(0))
            .map_err(|e| format!("查询当日股票池失败: {e}"))?;
        rows.enumerate()
            .map(|(id, row)| {
                row.map(|ts_code| Anchor {
                    id,
                    ts_code,
                    start_trade_date: start_date.to_string(),
                    end_trade_date: target_date.to_string(),
                })
                .map_err(|e| format!("读取当日股票池失败: {e}"))
            })
            .collect()
    })(&conn, &resolved_trade_date, &target_start_date)?;
    let target_anchor_count = target_anchors.len();
    let mut targets = Vec::with_capacity(target_anchor_count);
    let mut target_anchor_iter = target_anchors.into_iter();
    set_ranking_progress(
        "target-fingerprints",
        "正在计算当日股票指纹",
        0,
        target_anchor_count,
    );
    let mut target_completed = 0;
    loop {
        let chunk = target_anchor_iter
            .by_ref()
            .take(ANCHOR_CHUNK_SIZE)
            .collect::<Vec<_>>();
        if chunk.is_empty() {
            break;
        }
        let chunk_len = chunk.len();
        targets.extend(build_ranking_samples_for_chunk(
            &conn,
            chunk,
            &schema,
            &all_trade_dates,
            &environment_fingerprints,
            &benchmark_rows,
            &total_mv_map,
            &name_map,
            pool_segments,
            outcome_trade_days,
            &resolved_trade_date,
            false,
            Some(("target-fingerprints", target_completed, target_anchor_count)),
            &mut rule_catalog,
        )?);
        target_completed += chunk_len;
        set_ranking_progress(
            "target-fingerprints",
            "正在计算当日股票指纹",
            target_completed.min(target_anchor_count),
            target_anchor_count,
        );
    }
    timings.push(StrategyTriggerRankingTiming {
        label: "当日全市场指纹".to_string(),
        elapsed_ms: elapsed_ms(phase),
    });

    let phase = Instant::now();
    set_ranking_progress("candidate-index", "正在建立历史模板的规则倒排索引", 0, 0);
    let mut candidate_by_rule = vec![Vec::<CandidateRulePosting>::new(); rule_catalog.names.len()];
    for (index, candidate) in candidates.iter().enumerate() {
        for (rule_name, hits) in &candidate.fingerprint.trigger.by_rule {
            candidate_by_rule[*rule_name].push(CandidateRulePosting {
                candidate_index: index,
                hit_count: hits.len(),
            });
        }
    }
    let industry_map = build_industry_map(&source_path).unwrap_or_default();
    let concept_map = build_concepts_map(&source_path).unwrap_or_default();
    set_ranking_progress("candidate-index", "正在读取并计算规则权重", 0, 0);
    let rule_weights = load_rule_idf_weights(
        &conn,
        earliest_candidate_date,
        &historical_cutoff_date,
        &mut rule_catalog,
    )?;
    let candidate_rule_weight_sums = candidates
        .par_iter()
        .map(|candidate| trigger_rule_weight_sum(&candidate.fingerprint.trigger, &rule_weights))
        .collect::<Vec<_>>();

    timings.push(StrategyTriggerRankingTiming {
        label: "规则倒排索引与权重".to_string(),
        elapsed_ms: elapsed_ms(phase),
    });

    let phase = Instant::now();
    let ranking_completed = AtomicUsize::new(0);
    let prune_counters = RankingPruneCounters::default();
    log::info!(
        "相似榜精排开始: stocks={}, candidates={}, window_days={}, indicator_columns={}",
        targets.len(),
        candidates.len(),
        window_trade_days,
        schema.indicator_columns.len(),
    );
    set_ranking_progress("ranking", "正在计算候选市场相似度", 0, targets.len());
    let candidate_market_similarities =
        (|targets: &[RankingSample], candidates: &[RankingSample]| -> Vec<Option<f64>> {
            let Some(target) = targets.first() else {
                return vec![None; candidates.len()];
            };
            debug_assert!(targets.iter().all(|other| {
                other.anchor.end_trade_date == target.anchor.end_trade_date
                    && Arc::ptr_eq(&other.fingerprint.market, &target.fingerprint.market)
            }));

            // 市场指纹只由交易日决定。同一批目标共用参考日，因此每个候选交易日只需
            // 计算一次市场相似度，再展开成候选下标数组供精排直接索引。
            let mut similarity_by_date = HashMap::<&str, Option<f64>>::new();
            for candidate in candidates {
                similarity_by_date
                    .entry(candidate.anchor.end_trade_date.as_str())
                    .or_insert_with(|| {
                        cached_channel_similarity(
                            &target.fingerprint.market,
                            &candidate.fingerprint.market,
                        )
                    });
            }
            candidates
                .iter()
                .map(|candidate| {
                    similarity_by_date
                        .get(candidate.anchor.end_trade_date.as_str())
                        .copied()
                        .flatten()
                })
                .collect()
        })(&targets, &candidates);
    let mut ranking_rows = targets
        .par_iter()
        .map(|target| {
            let target_started = Instant::now();
            let row = (|target: &RankingSample,
                        candidates: &[RankingSample],
                        candidate_market_similarities: &[Option<f64>],
                        candidate_rule_weight_sums: &[f64],
                        candidate_by_rule: &[Vec<CandidateRulePosting>],
                        all_trade_dates: &[String],
                        window_trade_days: usize,
                        outcome_trade_days: usize,
                        name_map: &HashMap<String, String>,
                        industry_map: &HashMap<String, String>,
                        concept_map: &HashMap<String, String>,
                        rule_weights: &[f64]|
             -> (StrategyTriggerRankingRow, RankingPruneStats) {
                RANKING_TARGET_SCRATCH.with(|scratch| {
                    (|target: &RankingSample,
                      candidates: &[RankingSample],
                      candidate_market_similarities: &[Option<f64>],
                      candidate_rule_weight_sums: &[f64],
                      candidate_by_rule: &[Vec<CandidateRulePosting>],
                      all_trade_dates: &[String],
                      window_trade_days: usize,
                      outcome_trade_days: usize,
                      name_map: &HashMap<String, String>,
                      industry_map: &HashMap<String, String>,
                      concept_map: &HashMap<String, String>,
                      rule_weights: &[f64],
                      scratch: &mut RankingTargetScratch|
                     -> (StrategyTriggerRankingRow, RankingPruneStats) {
                        let target_rule_weight_sum =
                            trigger_rule_weight_sum(&target.fingerprint.trigger, rule_weights);
                        let per_class_limit = (256) / 2;
                        // 线程内复用候选标记、规则交集权重和精排堆。代数标记让每个目标只写入
                        // 实际命中的候选，不再全量清零 candidates.len() 个浮点数。
                        scratch.prepare(candidates.len(), per_class_limit);
                        for (rule_name, target_hits) in &target.fingerprint.trigger.by_rule {
                            let Some(indices) = candidate_by_rule.get(*rule_name) else {
                                continue;
                            };
                            let weight = rule_weight(rule_weights, *rule_name);
                            for posting in indices {
                                let smaller = target_hits.len().min(posting.hit_count) as f64;
                                let larger = target_hits.len().max(posting.hit_count) as f64;
                                scratch.add_candidate_rule_weight(
                                    posting.candidate_index,
                                    weight,
                                    smaller / larger,
                                );
                            }
                        }

                        // 触发时序的单规则上界不会超过 min(m,n)/max(m,n)。再合入已经
                        // 缓存的真实市场分作为最终分上界，并把成功、失败模板分桶，尽早
                        // 分别填满两个 Top-K 堆，不改变最终的精确结果。
                        for position in 0..scratch.candidate_indices.len() {
                            let candidate_index = scratch.candidate_indices[position];
                            let candidate = &candidates[candidate_index];
                            if candidate.template_class == 0
                                || candidate.anchor.ts_code == target.anchor.ts_code
                            {
                                continue;
                            }
                            let intersection_weight =
                                scratch.candidate_intersection_weights[candidate_index];
                            let timing_upper = if intersection_weight <= EPS {
                                0.0
                            } else {
                                (scratch.candidate_timing_upper_weights[candidate_index]
                                    / intersection_weight)
                                    .clamp(0.0, 1.0)
                            };
                            let rule_set = weighted_rule_set_similarity_from_masses(
                                target_rule_weight_sum,
                                candidate_rule_weight_sums[candidate_index],
                                intersection_weight,
                            );
                            scratch.candidate_rule_set_similarities[candidate_index] = rule_set;
                            let aggregate = trigger_aggregate_similarity(
                                &target.fingerprint.trigger,
                                &candidate.fingerprint.trigger,
                            );
                            scratch.candidate_aggregate_similarities[candidate_index] = aggregate;
                            let trigger_upper =
                                combine_trigger_similarity(rule_set, timing_upper, aggregate);
                            scratch.candidate_trigger_upper_bounds[candidate_index] = trigger_upper;
                            let price_available = target.fingerprint.price_volume.has_vectors
                                && candidate.fingerprint.price_volume.has_vectors;
                            let indicator_available = target.fingerprint.indicators.has_vectors
                                && candidate.fingerprint.indicators.has_vectors;
                            let market_available = target.fingerprint.market.has_vectors
                                && candidate.fingerprint.market.has_vectors;
                            let total_weight = TRIGGER_SIMILARITY_WEIGHT
                                + if price_available {
                                    PRICE_VOLUME_SIMILARITY_WEIGHT
                                } else {
                                    0.0
                                }
                                + if indicator_available {
                                    INDICATOR_SIMILARITY_WEIGHT
                                } else {
                                    0.0
                                }
                                + if market_available {
                                    MARKET_SIMILARITY_WEIGHT
                                } else {
                                    0.0
                                };
                            let remaining_weight = total_weight - TRIGGER_SIMILARITY_WEIGHT;
                            let market_similarity = market_available
                                .then(|| candidate_market_similarities[candidate_index])
                                .flatten();
                            let market_upper_score =
                                market_similarity.map_or(remaining_weight * 100.0, |score| {
                                    score * MARKET_SIMILARITY_WEIGHT
                                        + (remaining_weight - MARKET_SIMILARITY_WEIGHT) * 100.0
                                });
                            let final_upper = (trigger_upper * TRIGGER_SIMILARITY_WEIGHT
                                + market_upper_score)
                                / total_weight;
                            let buckets = if candidate.template_class > 0 {
                                &mut scratch.success_candidate_upper_buckets
                            } else {
                                &mut scratch.failure_candidate_upper_buckets
                            };
                            buckets[final_upper.clamp(0.0, 100.0).floor() as usize]
                                .push(candidate_index);
                        }
                        scratch.candidate_indices.clear();
                        for bucket_index in (0..=100).rev() {
                            scratch.candidate_indices.append(
                                &mut scratch.success_candidate_upper_buckets[bucket_index],
                            );
                            scratch.candidate_indices.append(
                                &mut scratch.failure_candidate_upper_buckets[bucket_index],
                            );
                        }
                        scratch.prune_stats.overlap_candidates = scratch.candidate_indices.len();

                        for candidate_position in 0..scratch.candidate_indices.len() {
                            let candidate_index = scratch.candidate_indices[candidate_position];
                            let candidate = &candidates[candidate_index];
                            // Leave-one-stock-out：同一股票的滚动窗口会共享真实 K 线、触发和静态
                            // 特征，不能作为自己的历史近邻，否则会形成股票身份与窗口重叠泄漏。
                            let price_available = target.fingerprint.price_volume.has_vectors
                                && candidate.fingerprint.price_volume.has_vectors;
                            let indicator_available = target.fingerprint.indicators.has_vectors
                                && candidate.fingerprint.indicators.has_vectors;
                            let market_available = target.fingerprint.market.has_vectors
                                && candidate.fingerprint.market.has_vectors;
                            let total_weight = TRIGGER_SIMILARITY_WEIGHT
                                + if price_available {
                                    PRICE_VOLUME_SIMILARITY_WEIGHT
                                } else {
                                    0.0
                                }
                                + if indicator_available {
                                    INDICATOR_SIMILARITY_WEIGHT
                                } else {
                                    0.0
                                }
                                + if market_available {
                                    MARKET_SIMILARITY_WEIGHT
                                } else {
                                    0.0
                                };
                            let cutoff = (|template_class: i8,
                                           success_heap: &BinaryHeap<Reverse<ScoredCandidate>>,
                                           failure_heap: &BinaryHeap<Reverse<ScoredCandidate>>,
                                           limit: usize|
                             -> Option<f64> {
                                let heap = if template_class > 0 {
                                    success_heap
                                } else if template_class < 0 {
                                    failure_heap
                                } else {
                                    return Some(f64::INFINITY);
                                };
                                (heap.len() >= limit).then(|| {
                                    heap.peek().map_or(f64::NEG_INFINITY, |row| row.0.score)
                                })
                            })(
                                candidate.template_class,
                                &scratch.success_heap,
                                &scratch.failure_heap,
                                per_class_limit,
                            );
                            let rule_set =
                                scratch.candidate_rule_set_similarities[candidate_index];
                            let aggregate =
                                scratch.candidate_aggregate_similarities[candidate_index];
                            let trigger_upper_bound =
                                scratch.candidate_trigger_upper_bounds[candidate_index];
                            // 市场分已按候选日期缓存，先用它收紧上界，避免为市场环境
                            // 不匹配且不可能入堆的候选计算量价和指标点积。没有共同有效
                            // 市场向量时仍保留原来的宽松上界，不把缺失市场分视为零分。
                            let market_similarity = market_available
                                .then(|| candidate_market_similarities[candidate_index])
                                .flatten();
                            let remaining_weight = total_weight - TRIGGER_SIMILARITY_WEIGHT;
                            let market_upper_score =
                                market_similarity.map_or(remaining_weight * 100.0, |score| {
                                    score * MARKET_SIMILARITY_WEIGHT
                                        + (remaining_weight - MARKET_SIMILARITY_WEIGHT) * 100.0
                                });
                            if can_prune_exact_candidate(
                                (trigger_upper_bound * TRIGGER_SIMILARITY_WEIGHT
                                    + market_upper_score)
                                    / total_weight,
                                cutoff,
                            ) {
                                scratch.prune_stats.market_pruned += 1;
                                continue;
                            }
                            // 先计算线性点积通道，再由真实通道分反推时序项必须达到的最低分。
                            // 只有仍可能进入堆的候选才执行昂贵的触发序列 DP。
                            let mut channel_weighted_score = 0.0;
                            let mut remaining_weight = total_weight - TRIGGER_SIMILARITY_WEIGHT;

                            let price_volume_similarity = price_available
                                .then(|| {
                                    scratch.prune_stats.price_evaluated += 1;
                                    cached_channel_similarity(
                                        &target.fingerprint.price_volume,
                                        &candidate.fingerprint.price_volume,
                                    )
                                })
                                .flatten();
                            if let Some(score) = price_volume_similarity {
                                channel_weighted_score += score * PRICE_VOLUME_SIMILARITY_WEIGHT;
                                remaining_weight -= PRICE_VOLUME_SIMILARITY_WEIGHT;
                            }
                            // 保持量价、市场、指标的浮点累加顺序，与原评分结果一致。
                            if let Some(score) = market_similarity {
                                channel_weighted_score += score * MARKET_SIMILARITY_WEIGHT;
                                remaining_weight -= MARKET_SIMILARITY_WEIGHT;
                            }
                            if can_prune_exact_candidate(
                                (trigger_upper_bound * TRIGGER_SIMILARITY_WEIGHT
                                    + channel_weighted_score
                                    + remaining_weight * 100.0)
                                    / total_weight,
                                cutoff,
                            ) {
                                scratch.prune_stats.price_pruned += 1;
                                continue;
                            }

                            let indicator_similarity = indicator_available
                                .then(|| {
                                    scratch.prune_stats.indicator_evaluated += 1;
                                    cached_channel_similarity(
                                        &target.fingerprint.indicators,
                                        &candidate.fingerprint.indicators,
                                    )
                                })
                                .flatten();
                            if let Some(score) = indicator_similarity {
                                channel_weighted_score += score * INDICATOR_SIMILARITY_WEIGHT;
                                remaining_weight -= INDICATOR_SIMILARITY_WEIGHT;
                            }
                            if can_prune_exact_candidate(
                                (trigger_upper_bound * TRIGGER_SIMILARITY_WEIGHT
                                    + channel_weighted_score
                                    + remaining_weight * 100.0)
                                    / total_weight,
                                cutoff,
                            ) {
                                scratch.prune_stats.indicator_pruned += 1;
                                continue;
                            }

                            let minimum_timing =
                                cutoff.map_or(f64::NEG_INFINITY, |minimum_score| {
                                    let minimum_trigger = (minimum_score * total_weight
                                        - channel_weighted_score)
                                        / TRIGGER_SIMILARITY_WEIGHT;
                                    (minimum_trigger / 100.0
                                        - rule_set * TRIGGER_RULE_SET_WEIGHT
                                        - aggregate * TRIGGER_AGGREGATE_RHYTHM_WEIGHT)
                                        / TRIGGER_RULE_TIMING_WEIGHT
                                });
                            scratch.prune_stats.timing_evaluated += 1;
                            let Some(timing) = weighted_rule_timing_similarity_with_masses(
                                &target.fingerprint.trigger,
                                &candidate.fingerprint.trigger,
                                rule_weights,
                                minimum_timing,
                                scratch.candidate_intersection_weights[candidate_index],
                                scratch.candidate_timing_upper_weights[candidate_index],
                            ) else {
                                scratch.prune_stats.timing_pruned += 1;
                                continue;
                            };
                            let trigger_similarity =
                                combine_trigger_similarity(rule_set, timing, aggregate);
                            let scored = ScoredCandidate {
                                score: final_similarity(
                                    trigger_similarity,
                                    price_volume_similarity,
                                    indicator_similarity,
                                    market_similarity,
                                ),
                                candidate_index,
                            };
                            scratch.prune_stats.scored += 1;
                            if candidate.template_class > 0 {
                                push_top_candidate(
                                    &mut scratch.success_heap,
                                    scored,
                                    per_class_limit,
                                );
                            } else if candidate.template_class < 0 {
                                push_top_candidate(
                                    &mut scratch.failure_heap,
                                    scored,
                                    per_class_limit,
                                );
                            }
                        }

                        let mut scored_candidates = scratch
                            .success_heap
                            .drain()
                            .chain(scratch.failure_heap.drain())
                            .map(|item| item.0)
                            .collect::<Vec<_>>();
                        scored_candidates.sort_by(|left, right| right.cmp(left));
                        let rating_candidates = (|sorted_candidates: &[ScoredCandidate],
                                                  candidates: &[RankingSample],
                                                  all_trade_dates: &[String],
                                                  window_trade_days: usize,
                                                  outcome_trade_days: usize|
                         -> Vec<ScoredCandidate> {
                            let same_stock_exclusion =
                                window_trade_days.max(outcome_trade_days).max(1);
                            let outcome_exclusion = outcome_trade_days.max(1);
                            let mut selected = Vec::with_capacity(RATING_SAMPLE_LIMIT);
                            let mut selected_end_indices =
                                Vec::<usize>::with_capacity(RATING_SAMPLE_LIMIT);
                            let mut selected_by_stock = HashMap::<&str, Vec<usize>>::new();

                            for scored in sorted_candidates {
                                let candidate = &candidates[scored.candidate_index];
                                if candidate
                                    .outcome
                                    .as_ref()
                                    .and_then(|outcome| outcome.excess_return_pct)
                                    .is_none()
                                {
                                    continue;
                                }
                                let Ok(end_index) =
                                    all_trade_dates.binary_search(&candidate.anchor.end_trade_date)
                                else {
                                    continue;
                                };
                                let nearby_outcome_count = selected_end_indices
                                    .iter()
                                    .filter(|selected_index| {
                                        selected_index.abs_diff(end_index) < outcome_exclusion
                                    })
                                    .count();
                                if nearby_outcome_count >= RATING_MAX_PER_OUTCOME_WINDOW {
                                    continue;
                                }
                                if selected_by_stock
                                    .get(candidate.anchor.ts_code.as_str())
                                    .is_some_and(|indices| {
                                        indices.iter().any(|selected_index| {
                                            selected_index.abs_diff(end_index)
                                                < same_stock_exclusion
                                        })
                                    })
                                {
                                    continue;
                                }
                                selected_end_indices.push(end_index);
                                selected_by_stock
                                    .entry(candidate.anchor.ts_code.as_str())
                                    .or_default()
                                    .push(end_index);
                                selected.push(*scored);
                                if selected.len() >= RATING_SAMPLE_LIMIT {
                                    break;
                                }
                            }
                            selected
                        })(
                            &scored_candidates,
                            candidates,
                            all_trade_dates,
                            window_trade_days,
                            outcome_trade_days,
                        );
                        let summary = summarize_outcomes(rating_candidates.iter().filter_map(
                            |scored| {
                                let outcome = candidates[scored.candidate_index].outcome.as_ref()?;
                                Some(OutcomeSummarySample {
                                    similarity_score: scored.score,
                                    return_pct: outcome.return_pct,
                                    excess_return_pct: outcome.excess_return_pct,
                                    mfe_pct: outcome.mfe_pct,
                                    mae_pct: outcome.mae_pct,
                                })
                            },
                        ));
                        let confidence = (summary.effective_sample_count
                            / (summary.effective_sample_count + SHRINKAGE_STRENGTH))
                            .max(0.0)
                            .sqrt();
                        let (quality_weighted_sum, quality_weight_sum) = rating_candidates
                            .iter()
                            .filter_map(|scored| {
                                let quality =
                                    candidates[scored.candidate_index].template_quality_score?;
                                let weight = (scored.score / 100.0).powi(2);
                                (weight > EPS).then_some((quality * weight, weight))
                            })
                            .fold((0.0, 0.0), |(value_sum, weight_sum), (value, weight)| {
                                (value_sum + value, weight_sum + weight)
                            });
                        let predicted_quality = (quality_weight_sum > EPS)
                            .then_some(quality_weighted_sum / quality_weight_sum);
                        let prediction_signal = (summary.effective_sample_count
                            >= SHRINKAGE_STRENGTH)
                            .then(|| {
                                predicted_quality.map(|quality| (quality - 0.5) * 2.0 * confidence)
                            })
                            .flatten();
                        let average_similarity = (!rating_candidates.is_empty()).then(|| {
                            rating_candidates
                                .iter()
                                .map(|row| row.score)
                                .sum::<f64>()
                                / rating_candidates.len() as f64
                        });
                        let mut display_candidates = Vec::with_capacity(5);
                        for template_class in [1_i8, -1_i8] {
                            if let Some(scored) = rating_candidates.iter().find(|scored| {
                                candidates[scored.candidate_index].template_class == template_class
                            }) {
                                display_candidates.push(scored);
                            }
                        }
                        for scored in &rating_candidates {
                            if display_candidates
                                .iter()
                                .any(|selected| selected.candidate_index == scored.candidate_index)
                            {
                                continue;
                            }
                            display_candidates.push(scored);
                            if display_candidates.len() >= 5 {
                                break;
                            }
                        }
                        let top_matches = display_candidates
                            .into_iter()
                            .filter_map(|scored| {
                                let candidate = &candidates[scored.candidate_index];
                                let outcome = candidate.outcome.as_ref()?;
                                Some(StrategyTriggerRankingMatch {
                                    ts_code: candidate.anchor.ts_code.clone(),
                                    name: name_map.get(&candidate.anchor.ts_code).cloned(),
                                    candidate_start_trade_date: candidate
                                        .anchor
                                        .start_trade_date
                                        .clone(),
                                    candidate_end_trade_date: candidate.anchor.end_trade_date.clone(),
                                    outcome_start_trade_date: outcome.start_trade_date.clone(),
                                    outcome_end_trade_date: outcome.end_trade_date.clone(),
                                    template_class: candidate.template_class,
                                    similarity_score: scored.score,
                                    forward_excess_return_pct: outcome.excess_return_pct,
                                    mfe_pct: outcome.mfe_pct,
                                    mae_pct: outcome.mae_pct,
                                })
                            })
                            .collect();
                        let row = StrategyTriggerRankingRow {
                            rank: None,
                            ts_code: target.anchor.ts_code.clone(),
                            name: name_map.get(&target.anchor.ts_code).cloned(),
                            industry: industry_map.get(&target.anchor.ts_code).cloned(),
                            concept: concept_map.get(&target.anchor.ts_code).cloned(),
                            board: None,
                            total_mv_yi: None,
                            original_score: target.total_score,
                            original_rank: target.original_rank,
                            best_rank_3d: None,
                            ranking_score: None,
                            prediction_signal,
                            confidence,
                            sample_count: summary.sample_count,
                            effective_sample_count: summary.effective_sample_count,
                            expected_return_pct: summary.weighted_return_pct,
                            expected_excess_return_pct: summary.weighted_excess_return_pct,
                            shrunk_excess_return_pct: summary.shrunk_excess_return_pct,
                            excess_positive_rate: summary.weighted_excess_positive_rate,
                            expected_mfe_pct: summary.weighted_mfe_pct,
                            expected_mae_pct: summary.weighted_mae_pct,
                            average_similarity,
                            best_similarity: rating_candidates.first().map(|row| row.score),
                            trigger_count: target.trigger_count,
                            top_matches,
                        };
                        (row, scratch.prune_stats)
                    })(
                        target,
                        candidates,
                        candidate_market_similarities,
                        candidate_rule_weight_sums,
                        candidate_by_rule,
                        all_trade_dates,
                        window_trade_days,
                        outcome_trade_days,
                        name_map,
                        industry_map,
                        concept_map,
                        rule_weights,
                        &mut scratch.borrow_mut(),
                    )
                })
            })(
                target,
                &candidates,
                &candidate_market_similarities,
                &candidate_rule_weight_sums,
                &candidate_by_rule,
                &all_trade_dates,
                window_trade_days,
                outcome_trade_days,
                &name_map,
                &industry_map,
                &concept_map,
                &rule_weights,
            );
            let (row, prune_stats) = row;
            for (counter, value) in [
                (&prune_counters.overlap_candidates, prune_stats.overlap_candidates),
                (&prune_counters.market_pruned, prune_stats.market_pruned),
                (&prune_counters.price_evaluated, prune_stats.price_evaluated),
                (&prune_counters.price_pruned, prune_stats.price_pruned),
                (
                    &prune_counters.indicator_evaluated,
                    prune_stats.indicator_evaluated,
                ),
                (&prune_counters.indicator_pruned, prune_stats.indicator_pruned),
                (&prune_counters.timing_evaluated, prune_stats.timing_evaluated),
                (&prune_counters.timing_pruned, prune_stats.timing_pruned),
                (&prune_counters.scored, prune_stats.scored),
            ] {
                counter.fetch_add(value, AtomicOrdering::Relaxed);
            }
            let completed = ranking_completed.fetch_add(1, AtomicOrdering::Relaxed) + 1;
            if target_started.elapsed().as_secs() >= 5 {
                log::info!(
                    "相似榜慢股票: stock={}, elapsed_ms={}, triggers={}, candidate_pool={}, completed={}/{}",
                    target.anchor.ts_code,
                    target_started.elapsed().as_millis(),
                    target.trigger_count,
                    candidates.len(),
                    completed,
                    targets.len(),
                );
            }
            set_ranking_progress(
                "ranking",
                &format!("已完成 {} 的近邻精排", target.anchor.ts_code),
                completed,
                targets.len(),
            );
            row
        })
        .collect::<Vec<_>>();
    log::info!(
        "相似榜精排剪枝统计: overlap={}, market_pruned={}, price_evaluated={}, price_pruned={}, indicator_evaluated={}, indicator_pruned={}, timing_evaluated={}, timing_pruned={}, scored={}",
        prune_counters
            .overlap_candidates
            .load(AtomicOrdering::Relaxed),
        prune_counters.market_pruned.load(AtomicOrdering::Relaxed),
        prune_counters.price_evaluated.load(AtomicOrdering::Relaxed),
        prune_counters.price_pruned.load(AtomicOrdering::Relaxed),
        prune_counters
            .indicator_evaluated
            .load(AtomicOrdering::Relaxed),
        prune_counters
            .indicator_pruned
            .load(AtomicOrdering::Relaxed),
        prune_counters
            .timing_evaluated
            .load(AtomicOrdering::Relaxed),
        prune_counters.timing_pruned.load(AtomicOrdering::Relaxed),
        prune_counters.scored.load(AtomicOrdering::Relaxed),
    );
    assign_ranks(&mut ranking_rows);
    timings.push(StrategyTriggerRankingTiming {
        label: "全市场近邻精排与后验聚合".to_string(),
        elapsed_ms: elapsed_ms(phase),
    });

    set_ranking_progress("validate", "正在校验计算期间的数据一致性", 0, 0);
    let final_signature = load_data_signature(&conn, &source_path, &resolved_trade_date)?;
    if final_signature != initial_signature {
        return Err(
            "计算期间行情或策略触发数据库发生更新，已放弃提交旧排行榜，请重新计算".to_string(),
        );
    }
    let scope_signature = (|source_path: &str,
                            indicator_columns: &[String]|
     -> Result<String, String> {
        let strategy_signature = stable_content_signature(&score_rule_path(source_path))?;
        let indicator_signature = stable_content_signature(&ind_toml_path(source_path))?;
        Ok(format!(
            "{SEMANTIC_DEFINITION_SIGNATURE_PREFIX}features=market-cap+main-star-growth-bse-st|strategy={strategy_signature}|indicator={indicator_signature}|columns={}",
            indicator_columns.join("\u{1f}")
        ))
    })(&source_path, &schema.indicator_columns)?;
    drop(conn);
    let before_write_elapsed = elapsed_ms(started);
    let phase = Instant::now();
    set_ranking_progress("write", "正在写入走势相似排行榜", 0, 0);
    let key = config_key(
        window_trade_days,
        pool_segments,
        outcome_trade_days,
        sample_gap_trade_days,
        &benchmark_index_code,
    );
    (|source_path: &str,
      trade_date: &str,
      config_key: &str,
      signature: &str,
      scope_signature: &str,
      historical_cutoff_date: &str,
      rows: &[StrategyTriggerRankingRow],
      universe_count: usize,
      candidate_universe_count: usize,
      candidate_anchor_count: usize,
      evaluated_anchor_count: usize,
      total_elapsed_ms: u64,
      timings: &[StrategyTriggerRankingTiming]|
     -> Result<(), String> {
        let (
            window_trade_days,
            pool_segments,
            outcome_trade_days,
            sample_gap_trade_days,
            benchmark_index_code,
        ) = parse_config_key(config_key)
            .ok_or_else(|| format!("无法解析走势相似排行配置: {config_key}"))?;
        let result_path = result_db_path(source_path);
        let mut conn = Connection::open(&result_path)
            .map_err(|e| format!("打开结果库写入相似排行榜失败: {e}"))?;
        ensure_ranking_tables(&conn)?;
        let tx = conn
            .transaction()
            .map_err(|e| format!("创建相似排行榜事务失败: {e}"))?;
        let previous_active = load_active_config_record(&tx)?;
        // 配置、策略或指标定义变化会改变汇总口径；每天重算复权行情、指标值或评分结果
        // 不改变口径，因此保留历史快照。
        if let Some(previous) = previous_active.as_ref() {
            let semantics_changed = previous.config_key != config_key
                || (previous
                    .scope_signature
                    .starts_with(SEMANTIC_DEFINITION_SIGNATURE_PREFIX)
                    && previous.scope_signature != scope_signature);
            if semantics_changed {
                tx.execute(
                    "DELETE FROM strategy_trigger_similarity_rank WHERE config_key=?",
                    params![previous.config_key],
                )
                .map_err(|e| format!("策略或指标变化后清理相似排行失败: {e}"))?;
                tx.execute(
                    "DELETE FROM strategy_trigger_similarity_rank_meta WHERE config_key=?",
                    params![previous.config_key],
                )
                .map_err(|e| format!("策略或指标变化后清理相似排行元数据失败: {e}"))?;
                tx.execute("DELETE FROM strategy_trigger_similarity_summary", [])
                    .map_err(|e| format!("相似榜口径变化后清理历史汇总失败: {e}"))?;
            }
        }
        // 生产库只允许存在当前生效配置；配置切换本身触发旧配置清理。
        tx.execute(
            "DELETE FROM strategy_trigger_similarity_rank WHERE config_key<>?",
            params![config_key],
        )
        .map_err(|e| format!("清理非生效相似排行配置失败: {e}"))?;
        tx.execute(
            "DELETE FROM strategy_trigger_similarity_rank_meta WHERE config_key<>?",
            params![config_key],
        )
        .map_err(|e| format!("清理非生效相似排行配置元数据失败: {e}"))?;
        tx.execute(
            "DELETE FROM strategy_trigger_similarity_rank WHERE trade_date=? AND config_key=?",
            params![trade_date, config_key],
        )
        .map_err(|e| format!("清理旧相似排行失败: {e}"))?;
        tx.execute(
            "DELETE FROM strategy_trigger_similarity_rank_meta WHERE trade_date=? AND config_key=?",
            params![trade_date, config_key],
        )
        .map_err(|e| format!("清理旧相似排行元数据失败: {e}"))?;
        tx.execute(
            "DELETE FROM strategy_trigger_similarity_summary WHERE trade_date=?",
            params![trade_date],
        )
        .map_err(|e| format!("清理旧相似排行汇总失败: {e}"))?;
        {
            let mut insert = tx
                .prepare(
                    r#"
                INSERT INTO strategy_trigger_similarity_rank VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                "#,
                )
                .map_err(|e| format!("预编译相似排行写入失败: {e}"))?;
            for row in rows {
                let top_matches_json = serde_json::to_string(&row.top_matches)
                    .map_err(|e| format!("序列化相似事件失败: {e}"))?;
                insert
                    .execute(params![
                        trade_date,
                        config_key,
                        row.rank.map(|value| value as i64),
                        row.ts_code,
                        row.name,
                        row.industry,
                        row.concept,
                        row.original_score,
                        row.original_rank,
                        row.ranking_score,
                        row.prediction_signal,
                        row.confidence,
                        row.sample_count as i64,
                        row.effective_sample_count,
                        row.expected_return_pct,
                        row.expected_excess_return_pct,
                        row.shrunk_excess_return_pct,
                        row.excess_positive_rate,
                        row.expected_mfe_pct,
                        row.expected_mae_pct,
                        row.average_similarity,
                        row.best_similarity,
                        row.trigger_count as i64,
                        top_matches_json,
                    ])
                    .map_err(|e| format!("写入相似排行失败 {}: {e}", row.ts_code))?;
            }
        }
        tx.execute(
            "INSERT INTO strategy_trigger_similarity_summary (trade_date, ts_code, rank)
             SELECT trade_date, ts_code, rank FROM strategy_trigger_similarity_rank
             WHERE trade_date=? AND config_key=?",
            params![trade_date, config_key],
        )
        .map_err(|e| format!("写入相似排行汇总失败: {e}"))?;
        let timings_json =
            serde_json::to_string(timings).map_err(|e| format!("序列化相似排行计时失败: {e}"))?;
        tx.execute(
        "INSERT INTO strategy_trigger_similarity_rank_meta VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            trade_date,
            config_key,
            signature,
            now_epoch_seconds(),
            historical_cutoff_date,
            universe_count as i64,
            rows.iter().filter(|row| row.rank.is_some()).count() as i64,
            candidate_universe_count as i64,
            candidate_anchor_count as i64,
            evaluated_anchor_count as i64,
            total_elapsed_ms as i64,
            timings_json,
        ],
    )
    .map_err(|e| format!("写入相似排行元数据失败: {e}"))?;
        tx.execute(
            "DELETE FROM strategy_trigger_similarity_active_config WHERE id=1",
            [],
        )
        .map_err(|e| format!("清理旧相似排行生效配置失败: {e}"))?;
        tx.execute(
            r#"
        INSERT INTO strategy_trigger_similarity_active_config (
            id, config_key, algorithm_version, window_trade_days, pool_segments,
            outcome_trade_days, sample_gap_trade_days, benchmark_index_code, scope_trade_date,
            scope_signature, updated_at_epoch_seconds
        ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
            params![
                config_key,
                ALGORITHM_VERSION,
                window_trade_days as i64,
                pool_segments as i64,
                outcome_trade_days as i64,
                sample_gap_trade_days as i64,
                benchmark_index_code,
                trade_date,
                scope_signature,
                now_epoch_seconds()
            ],
        )
        .map_err(|e| format!("写入相似排行生效配置失败: {e}"))?;
        tx.commit().map_err(|e| format!("提交相似排行榜失败: {e}"))
    })(
        &source_path,
        &resolved_trade_date,
        &key,
        &initial_signature,
        &scope_signature,
        &historical_cutoff_date,
        &ranking_rows,
        target_anchor_count,
        candidate_universe_count,
        candidate_anchor_count,
        evaluated_anchor_count,
        before_write_elapsed,
        &timings,
    )?;
    timings.push(StrategyTriggerRankingTiming {
        label: "原子写入排行榜".to_string(),
        elapsed_ms: elapsed_ms(phase),
    });
    set_ranking_progress("read-result", "正在读取排行榜结果", 0, 0);

    let page = get_strategy_trigger_similarity_ranking_page(
        source_path,
        Some(resolved_trade_date),
        Some(window_trade_days as u32),
        Some(pool_segments as u32),
        Some(outcome_trade_days as u32),
        Some(benchmark_index_code),
        limit,
        board,
        exclude_st_board,
        total_mv_min,
        total_mv_max,
        None,
        Some(sample_gap_trade_days as u32),
    )?;
    set_ranking_progress("completed", "走势相似排行榜计算完成", 1, 1);
    Ok(page)
}

#[cfg(test)]
mod tests {
    use crate::trigger_similarity::ranking::page::get_strategy_trigger_similarity_ranking_page;
    use crate::trigger_similarity::ranking::run::run_strategy_trigger_similarity_ranking;
    use crate::trigger_similarity::ranking::store::get_strategy_trigger_similarity_active_config;
    use duckdb::Connection;
    use duckdb::params;
    use std::fs;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    #[test]
    #[ignore = "requires LIANGHUA_BENCH_DATA_DIR and writes the real ranking tables"]
    fn benchmark_real_full_ranking() {
        let source_path = std::env::var("LIANGHUA_BENCH_DATA_DIR")
            .expect("set LIANGHUA_BENCH_DATA_DIR to a real source directory");
        let started = std::time::Instant::now();
        let page = run_strategy_trigger_similarity_ranking(
            source_path,
            None,
            Some(20),
            Some(5),
            Some(5),
            Some("000001.SH".to_string()),
            Some(100),
            None,
            None,
            None,
            None,
            None,
        )
        .expect("compute real full-market ranking");
        eprintln!(
            "real full ranking: elapsed={:?}, universe={}, ranked={}, candidates={}/{}, timings={:?}",
            started.elapsed(),
            page.universe_count,
            page.ranked_count,
            page.evaluated_anchor_count,
            page.candidate_anchor_count,
            page.timings
        );
        assert!(page.is_fresh);
    }

    #[test]
    fn ranking_round_trip_writes_and_revalidates_data_signature() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let source_dir = std::env::temp_dir().join(format!(
            "lianghua-strategy-sim-rank-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&source_dir).expect("create test source directory");
        fs::write(source_dir.join("score_rule.toml"), "version = 1\n")
            .expect("write strategy definition");
        fs::write(source_dir.join("ind.toml"), "[[indicator]]\nname = 'J'\n")
            .expect("write indicator definition");
        let market_path = source_dir.join("stock_data.db");
        let result_path = source_dir.join("scoring_result.db");
        let market = Connection::open(&market_path).expect("open market db");
        market
            .execute_batch(
                r#"
                CREATE TABLE stock_data (
                    ts_code VARCHAR, trade_date VARCHAR, adj_type VARCHAR,
                    open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
                    pct_chg DOUBLE, vol DOUBLE, amount DOUBLE, tor DOUBLE, net_mf_v DOUBLE
                );
                "#,
            )
            .expect("create market table");
        let stocks = [
            "TARGET.SZ",
            "C0.SZ",
            "C1.SZ",
            "C2.SZ",
            "C3.SZ",
            "C4.SZ",
            "C5.SZ",
        ];
        {
            let mut insert = market
                .prepare("INSERT INTO stock_data VALUES (?, ?, 'qfq', ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                .expect("prepare market rows");
            for (stock_index, stock) in stocks.iter().enumerate() {
                for day in 1..=10 {
                    let base = 10.0 + stock_index as f64 + day as f64 * 0.1;
                    insert
                        .execute(params![
                            stock,
                            format!("202401{day:02}"),
                            base,
                            base * 1.03,
                            base * 0.98,
                            base * 1.01,
                            day as f64 * 0.1,
                            1_000.0 + day as f64,
                            10_000.0 + day as f64,
                            2.0,
                            10.0,
                        ])
                        .expect("insert market row");
                }
            }
            let mut insert_index = market
                .prepare("INSERT INTO stock_data VALUES ('000001.SH', ?, 'ind', ?, ?, ?, ?, ?, 0, 0, 0, 0)")
                .expect("prepare benchmark rows");
            for day in 1..=10 {
                let base = 3_000.0 + day as f64;
                insert_index
                    .execute(params![
                        format!("202401{day:02}"),
                        base,
                        base,
                        base,
                        base,
                        0.1
                    ])
                    .expect("insert benchmark row");
            }
        }
        drop(market);

        let result = Connection::open(&result_path).expect("open result db");
        result
            .execute_batch(
                r#"
                CREATE TABLE score_summary (
                    ts_code VARCHAR, trade_date VARCHAR, total_score DOUBLE, rank BIGINT
                );
                CREATE TABLE rule_details (
                    ts_code VARCHAR, trade_date VARCHAR, rule_name VARCHAR, rule_score DOUBLE
                );
                "#,
            )
            .expect("create result tables");
        {
            let mut insert_score = result
                .prepare("INSERT INTO score_summary VALUES (?, ?, ?, ?)")
                .expect("prepare score rows");
            for day in 1..=10 {
                for (index, stock) in stocks.iter().enumerate() {
                    insert_score
                        .execute(params![
                            stock,
                            format!("202401{day:02}"),
                            10.0 - index as f64,
                            index as i64 + 1
                        ])
                        .expect("insert score row");
                }
            }
            let mut insert_rule = result
                .prepare("INSERT INTO rule_details VALUES (?, ?, '启动规则', 1.0)")
                .expect("prepare rule rows");
            for (index, stock) in stocks.iter().skip(1).enumerate() {
                insert_rule
                    .execute(params![stock, format!("202401{:02}", index + 2)])
                    .expect("insert historical trigger");
            }
            insert_rule
                .execute(params!["TARGET.SZ", "20240110"])
                .expect("insert target trigger");
        }
        drop(result);

        let source_path = source_dir.to_string_lossy().to_string();
        run_strategy_trigger_similarity_ranking(
            source_path.clone(),
            Some("20240109".to_string()),
            Some(3),
            Some(2),
            Some(2),
            Some("000001.SH".to_string()),
            Some(100),
            None,
            None,
            None,
            None,
            Some(0),
        )
        .expect("compute first historical ranking snapshot");
        let market = Connection::open(source_dir.join("stock_data.db"))
            .expect("reopen market db for daily qfq rebuild simulation");
        market
            .execute(
                "UPDATE stock_data SET close=close+0.01 WHERE ts_code='C0.SZ' AND trade_date='20240102' AND adj_type='qfq'",
                [],
            )
            .expect("simulate recalculated historical qfq value");
        drop(market);
        let result = Connection::open(source_dir.join("scoring_result.db"))
            .expect("reopen result db for daily scoring rebuild simulation");
        result
            .execute(
                "UPDATE score_summary SET total_score=total_score+0.01 WHERE ts_code='C0.SZ' AND trade_date='20240102'",
                [],
            )
            .expect("simulate recalculated historical score");
        result
            .execute(
                "UPDATE rule_details SET rule_score=rule_score+0.01 WHERE ts_code='C0.SZ' AND trade_date='20240102'",
                [],
            )
            .expect("simulate recalculated historical rule result");
        drop(result);
        let computed = run_strategy_trigger_similarity_ranking(
            source_path.clone(),
            Some("20240110".to_string()),
            None,
            None,
            None,
            None,
            Some(100),
            None,
            None,
            None,
            None,
            Some(0),
        )
        .expect("compute ranking");
        assert!(computed.is_fresh);
        assert_eq!(computed.universe_count, stocks.len());
        assert!(computed.evaluated_anchor_count > 0);
        assert!(computed.items.iter().any(|row| row.ts_code == "TARGET.SZ"));
        let result = Connection::open(source_dir.join("scoring_result.db"))
            .expect("reopen result db for active config assertions");
        let summary_difference: i64 = result.query_row(
            "SELECT COUNT(*) FROM (
                (SELECT trade_date, ts_code, rank FROM strategy_trigger_similarity_summary
                 EXCEPT ALL SELECT trade_date, ts_code, rank FROM strategy_trigger_similarity_rank)
                UNION ALL
                (SELECT trade_date, ts_code, rank FROM strategy_trigger_similarity_rank
                 EXCEPT ALL SELECT trade_date, ts_code, rank FROM strategy_trigger_similarity_summary)
             ) differences", [], |row| row.get(0),
        ).unwrap();
        assert_eq!(
            summary_difference, 0,
            "汇总应保留全部日期和 NULL 排名，且无重复行"
        );
        let active = get_strategy_trigger_similarity_active_config(&result)
            .expect("read active config")
            .expect("active config should exist");
        assert_eq!(active.window_trade_days, 3);
        assert_eq!(active.pool_segments, 2);
        assert_eq!(active.outcome_trade_days, 2);
        assert_eq!(active.benchmark_index_code, "000001.SH");
        assert_eq!(
            result
                .query_row(
                    "SELECT COUNT(DISTINCT trade_date) FROM strategy_trigger_similarity_rank_meta",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("count retained snapshots"),
            2,
            "daily qfq and scoring rebuilds must not delete older snapshots"
        );
        assert_eq!(
            result
                .query_row(
                    "SELECT COUNT(DISTINCT trade_date) FROM strategy_trigger_similarity_summary",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("count retained summary snapshots"),
            2
        );
        drop(result);

        let reread = get_strategy_trigger_similarity_ranking_page(
            source_path.clone(),
            Some("20240110".to_string()),
            Some(3),
            Some(2),
            Some(2),
            Some("000001.SH".to_string()),
            Some(100),
            None,
            None,
            None,
            None,
            None,
            Some(0),
        )
        .expect("read ranking");
        assert!(reread.is_fresh);
        assert_eq!(reread.items.len(), computed.items.len());

        let searched = get_strategy_trigger_similarity_ranking_page(
            source_path.clone(),
            Some("20240110".to_string()),
            Some(3),
            Some(2),
            Some(2),
            Some("000001.SH".to_string()),
            Some(100),
            None,
            None,
            None,
            None,
            Some("TARGET.SZ".to_string()),
            Some(0),
        )
        .expect("filter ranking by stock code");
        assert_eq!(searched.items.len(), 1);
        assert_eq!(searched.items[0].ts_code, "TARGET.SZ");

        let result = Connection::open(source_dir.join("scoring_result.db"))
            .expect("reopen result db for mutation");
        result
            .execute(
                "UPDATE rule_details SET rule_score=2.0 WHERE ts_code='TARGET.SZ' AND trade_date='20240110'",
                [],
            )
            .expect("mutate strategy trigger data");
        drop(result);
        let stale = get_strategy_trigger_similarity_ranking_page(
            source_dir.to_string_lossy().to_string(),
            Some("20240110".to_string()),
            Some(3),
            Some(2),
            Some(2),
            Some("000001.SH".to_string()),
            Some(100),
            None,
            None,
            None,
            None,
            None,
            Some(0),
        )
        .expect("revalidate changed ranking");
        assert!(!stale.is_fresh);
        assert!(stale.items.is_empty());

        fs::write(source_dir.join("score_rule.toml"), "version = 2\n")
            .expect("change strategy definition");

        run_strategy_trigger_similarity_ranking(
            source_dir.to_string_lossy().to_string(),
            Some("20240110".to_string()),
            None,
            None,
            None,
            None,
            Some(100),
            None,
            None,
            None,
            None,
            Some(0),
        )
        .expect("recompute after strategy change");
        let result = Connection::open(source_dir.join("scoring_result.db"))
            .expect("reopen result db after strategy change");
        assert_eq!(
            result
                .query_row(
                    "SELECT COUNT(DISTINCT trade_date) FROM strategy_trigger_similarity_rank_meta",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("count snapshots after strategy change"),
            1,
            "strategy changes must clear old snapshots before writing the replacement"
        );
        assert_eq!(
            result
                .query_row(
                    "SELECT COUNT(DISTINCT trade_date) FROM strategy_trigger_similarity_summary",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("count summary snapshots after strategy change"),
            1
        );
        drop(result);

        run_strategy_trigger_similarity_ranking(
            source_dir.to_string_lossy().to_string(),
            Some("20240110".to_string()),
            Some(3),
            Some(3),
            Some(2),
            Some("000001.SH".to_string()),
            Some(100),
            None,
            None,
            None,
            None,
            Some(0),
        )
        .expect("switch active pool configuration");
        let result = Connection::open(source_dir.join("scoring_result.db"))
            .expect("reopen result db after config switch");
        let (config_count, active_pool): (i64, i64) = result
            .query_row(
                "SELECT (SELECT COUNT(DISTINCT config_key) FROM strategy_trigger_similarity_rank_meta), pool_segments \
                 FROM strategy_trigger_similarity_active_config WHERE id=1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("read switched active config");
        assert_eq!(config_count, 1);
        assert_eq!(active_pool, 3);
        drop(result);

        fs::remove_dir_all(&source_dir).expect("remove test source directory");
    }
}
