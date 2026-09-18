mod page;
mod run;
pub(crate) mod samples;
mod store;
#[cfg(test)]
mod test_support;

pub use page::get_strategy_trigger_similarity_ranking_page;
pub use run::run_strategy_trigger_similarity_ranking;
pub use store::get_strategy_trigger_similarity_active_config;

use crate::trigger_similarity::*;

use serde::Deserialize;
use serde::Serialize;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicUsize;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
pub(super) const ALGORITHM_VERSION: &str = "outcome-reverse-startup-ranking-v11";
pub(super) const SUCCESS_QUALITY_THRESHOLD: f64 = 0.80;
pub(super) const FAILURE_QUALITY_THRESHOLD: f64 = 0.20;
pub(super) const MARKET_HISTORY_BUCKETS: usize = 8;
pub(super) const SEMANTIC_DEFINITION_SIGNATURE_PREFIX: &str = "definitions-v1|";

#[derive(Debug, Clone)]
pub(super) struct OutcomePathRow {
    date_index: usize,
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
    pct_chg: Option<f64>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RawOutcomeLabel {
    stock_index: usize,
    date_index: usize,
    excess_return_pct: f64,
    mfe_pct: f64,
    mae_pct: f64,
    persistence: f64,
}

pub(super) static RANKING_COMPUTE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
pub(super) static RANKING_PROGRESS: OnceLock<Mutex<Option<StrategyTriggerRankingProgress>>> =
    OnceLock::new();

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyTriggerRankingProgress {
    pub phase: String,
    pub message: String,
    pub completed: usize,
    pub total: usize,
    pub started_at_epoch_seconds: i64,
    pub phase_started_at_epoch_seconds: i64,
}

pub(super) fn set_ranking_progress(phase: &str, message: &str, completed: usize, total: usize) {
    let mut progress = RANKING_PROGRESS
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("相似排行榜进度锁被污染");
    if progress.as_ref().is_some_and(|item| {
        item.phase == phase && item.total == total && completed < item.completed
    }) {
        return;
    }
    let now = now_epoch_seconds();
    let phase_changed = progress.as_ref().is_none_or(|item| item.phase != phase);
    let started_at_epoch_seconds = progress
        .as_ref()
        .map(|item| item.started_at_epoch_seconds)
        .unwrap_or(now);
    let phase_started_at_epoch_seconds = progress
        .as_ref()
        .filter(|item| item.phase == phase)
        .map(|item| item.phase_started_at_epoch_seconds)
        .unwrap_or(now);
    *progress = Some(StrategyTriggerRankingProgress {
        phase: phase.to_string(),
        message: message.to_string(),
        completed,
        total,
        started_at_epoch_seconds,
        phase_started_at_epoch_seconds,
    });
    drop(progress);
    if phase_changed {
        log::info!("相似榜进入阶段 {phase}: {message}, completed={completed}, total={total}");
    }
}

pub fn get_strategy_trigger_similarity_ranking_progress() -> Option<StrategyTriggerRankingProgress>
{
    RANKING_PROGRESS
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("相似排行榜进度锁被污染")
        .clone()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyTriggerRankingMatch {
    pub ts_code: String,
    pub name: Option<String>,
    pub candidate_start_trade_date: String,
    pub candidate_end_trade_date: String,
    pub outcome_start_trade_date: String,
    pub outcome_end_trade_date: String,
    pub template_class: i8,
    pub similarity_score: f64,
    pub forward_excess_return_pct: Option<f64>,
    pub mfe_pct: f64,
    pub mae_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyTriggerRankingRow {
    pub rank: Option<usize>,
    pub ts_code: String,
    pub name: Option<String>,
    pub industry: Option<String>,
    pub concept: Option<String>,
    pub board: Option<String>,
    pub original_score: Option<f64>,
    pub original_rank: Option<i64>,
    pub best_rank_3d: Option<i64>,
    pub ranking_score: Option<f64>,
    pub prediction_signal: Option<f64>,
    pub confidence: f64,
    pub sample_count: usize,
    pub effective_sample_count: f64,
    pub expected_return_pct: Option<f64>,
    pub expected_excess_return_pct: Option<f64>,
    pub shrunk_excess_return_pct: Option<f64>,
    pub excess_positive_rate: Option<f64>,
    pub expected_mfe_pct: Option<f64>,
    pub expected_mae_pct: Option<f64>,
    pub average_similarity: Option<f64>,
    pub best_similarity: Option<f64>,
    pub trigger_count: usize,
    pub total_mv_yi: Option<f64>,
    pub top_matches: Vec<StrategyTriggerRankingMatch>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyTriggerRankingTiming {
    pub label: String,
    pub elapsed_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyTriggerSimilarityActiveConfig {
    pub algorithm_version: String,
    pub window_trade_days: usize,
    pub pool_segments: usize,
    pub outcome_trade_days: usize,
    pub sample_gap_trade_days: usize,
    pub benchmark_index_code: String,
}

#[derive(Debug, Clone)]
pub(super) struct ActiveConfigRecord {
    config: StrategyTriggerSimilarityActiveConfig,
    config_key: String,
    scope_signature: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyTriggerRankingPageData {
    pub resolved_trade_date: String,
    pub historical_cutoff_date: String,
    pub window_trade_days: usize,
    pub pool_segments: usize,
    pub outcome_trade_days: usize,
    pub benchmark_index_code: String,
    pub algorithm_version: String,
    pub data_signature: String,
    pub generated_at_epoch_seconds: Option<i64>,
    pub is_fresh: bool,
    pub stale_reason: Option<String>,
    pub universe_count: usize,
    pub ranked_count: usize,
    pub candidate_universe_count: usize,
    pub candidate_anchor_count: usize,
    pub evaluated_anchor_count: usize,
    pub elapsed_ms: Option<u64>,
    pub timings: Vec<StrategyTriggerRankingTiming>,
    pub items: Vec<StrategyTriggerRankingRow>,
}

#[derive(Debug, Clone)]
pub(super) struct RankingFingerprint {
    trigger: TriggerFingerprint,
    price_volume: ChannelFingerprint,
    indicators: ChannelFingerprint,
    market: Arc<ChannelFingerprint>,
}

#[derive(Debug, Clone)]
pub(super) struct RankingSample {
    anchor: Anchor,
    fingerprint: RankingFingerprint,
    trigger_count: usize,
    outcome: Option<Outcome>,
    total_score: Option<f64>,
    original_rank: Option<i64>,
    template_quality_score: Option<f64>,
    template_class: i8,
}

#[derive(Debug, Clone)]
pub(super) struct OutcomeSelectedAnchor {
    pub(in crate::trigger_similarity) anchor: Anchor,
    pub(in crate::trigger_similarity) quality_score: f64,
    pub(in crate::trigger_similarity) quality_class: i8,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ScoredCandidate {
    score: f64,
    candidate_index: usize,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct CandidateRulePosting {
    candidate_index: usize,
    hit_count: usize,
}

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct RankingPruneStats {
    overlap_candidates: usize,
    market_pruned: usize,
    price_evaluated: usize,
    price_pruned: usize,
    indicator_evaluated: usize,
    indicator_pruned: usize,
    timing_evaluated: usize,
    timing_pruned: usize,
    scored: usize,
}

#[derive(Default)]
pub(super) struct RankingPruneCounters {
    overlap_candidates: AtomicUsize,
    market_pruned: AtomicUsize,
    price_evaluated: AtomicUsize,
    price_pruned: AtomicUsize,
    indicator_evaluated: AtomicUsize,
    indicator_pruned: AtomicUsize,
    timing_evaluated: AtomicUsize,
    timing_pruned: AtomicUsize,
    scored: AtomicUsize,
}

#[derive(Default)]
pub(super) struct RankingTargetScratch {
    candidate_intersection_weights: Vec<f64>,
    candidate_timing_upper_weights: Vec<f64>,
    candidate_rule_set_similarities: Vec<f64>,
    candidate_trigger_upper_bounds: Vec<f64>,
    candidate_aggregate_similarities: Vec<f64>,
    candidate_generations: Vec<u32>,
    generation: u32,
    candidate_indices: Vec<usize>,
    success_candidate_upper_buckets: Vec<Vec<usize>>,
    failure_candidate_upper_buckets: Vec<Vec<usize>>,
    success_heap: BinaryHeap<Reverse<ScoredCandidate>>,
    failure_heap: BinaryHeap<Reverse<ScoredCandidate>>,
    prune_stats: RankingPruneStats,
}

impl RankingTargetScratch {
    fn prepare(&mut self, candidate_count: usize, per_class_limit: usize) {
        self.candidate_intersection_weights
            .resize(candidate_count, 0.0);
        self.candidate_timing_upper_weights
            .resize(candidate_count, 0.0);
        self.candidate_rule_set_similarities
            .resize(candidate_count, 0.0);
        self.candidate_trigger_upper_bounds
            .resize(candidate_count, 0.0);
        self.candidate_aggregate_similarities
            .resize(candidate_count, 0.0);
        self.candidate_generations.resize(candidate_count, 0);
        self.generation = self.generation.wrapping_add(1);
        if self.generation == 0 {
            self.candidate_generations.fill(0);
            self.generation = 1;
        }
        self.candidate_indices.clear();
        for buckets in [
            &mut self.success_candidate_upper_buckets,
            &mut self.failure_candidate_upper_buckets,
        ] {
            if buckets.is_empty() {
                buckets.resize_with(101, Vec::new);
            } else {
                for bucket in buckets {
                    bucket.clear();
                }
            }
        }
        self.prune_stats = RankingPruneStats::default();
        self.success_heap.clear();
        self.failure_heap.clear();
        let required_capacity = per_class_limit + 1;
        if self.success_heap.capacity() < required_capacity {
            self.success_heap.reserve(required_capacity);
        }
        if self.failure_heap.capacity() < required_capacity {
            self.failure_heap.reserve(required_capacity);
        }
    }

    fn add_candidate_rule_weight(
        &mut self,
        candidate_index: usize,
        weight: f64,
        timing_upper: f64,
    ) {
        if self.candidate_generations[candidate_index] != self.generation {
            self.candidate_generations[candidate_index] = self.generation;
            self.candidate_intersection_weights[candidate_index] = weight;
            self.candidate_timing_upper_weights[candidate_index] = weight * timing_upper;
            self.candidate_indices.push(candidate_index);
        } else {
            self.candidate_intersection_weights[candidate_index] += weight;
            self.candidate_timing_upper_weights[candidate_index] += weight * timing_upper;
        }
    }
}

thread_local! {
    static RANKING_TARGET_SCRATCH: RefCell<RankingTargetScratch> =
        RefCell::new(RankingTargetScratch::default());
}

impl PartialEq for ScoredCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.score.total_cmp(&other.score) == Ordering::Equal
            && self.candidate_index == other.candidate_index
    }
}

impl Eq for ScoredCandidate {}

impl PartialOrd for ScoredCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.candidate_index.cmp(&other.candidate_index))
    }
}

pub(super) fn push_top_candidate(
    heap: &mut BinaryHeap<Reverse<ScoredCandidate>>,
    scored: ScoredCandidate,
    limit: usize,
) {
    if heap.len() < limit {
        heap.push(Reverse(scored));
    } else if heap.peek().is_some_and(|minimum| scored > minimum.0) {
        heap.pop();
        heap.push(Reverse(scored));
    }
}

pub(super) fn can_prune_exact_candidate(upper_bound: f64, cutoff: Option<f64>) -> bool {
    cutoff.is_some_and(|minimum| upper_bound + EPS < minimum)
}

#[derive(Debug, Clone)]
pub(super) struct RankingMeta {
    data_signature: String,
    generated_at_epoch_seconds: i64,
    historical_cutoff_date: String,
    universe_count: usize,
    ranked_count: usize,
    candidate_universe_count: usize,
    candidate_anchor_count: usize,
    evaluated_anchor_count: usize,
    elapsed_ms: u64,
    timings: Vec<StrategyTriggerRankingTiming>,
}

pub(super) fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

pub(super) fn now_epoch_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .min(i64::MAX as u64) as i64
}

#[cfg(test)]
mod tests {
    use crate::trigger_similarity::ranking::RankingTargetScratch;

    #[test]
    fn ranking_progress_preserves_phase_time_and_rejects_stale_counts() {
        let _compute = crate::trigger_similarity::ranking::RANKING_COMPUTE_LOCK
            .get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .unwrap();
        *crate::trigger_similarity::ranking::RANKING_PROGRESS
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .unwrap() = Some(
            crate::trigger_similarity::ranking::StrategyTriggerRankingProgress {
                phase: "ranking".into(),
                message: "精排".into(),
                completed: 32,
                total: 64,
                started_at_epoch_seconds: 1,
                phase_started_at_epoch_seconds: 2,
            },
        );
        crate::trigger_similarity::ranking::set_ranking_progress("ranking", "精排", 16, 64);
        let progress =
            crate::trigger_similarity::ranking::get_strategy_trigger_similarity_ranking_progress()
                .unwrap();
        assert_eq!(progress.completed, 32);
        assert_eq!(progress.phase_started_at_epoch_seconds, 2);
        crate::trigger_similarity::ranking::set_ranking_progress("ranking", "精排", 64, 64);
        let progress =
            crate::trigger_similarity::ranking::get_strategy_trigger_similarity_ranking_progress()
                .unwrap();
        assert_eq!(progress.completed, 64);
        assert_eq!(progress.phase_started_at_epoch_seconds, 2);
        crate::trigger_similarity::ranking::set_ranking_progress("write", "写库", 0, 0);
        let progress =
            crate::trigger_similarity::ranking::get_strategy_trigger_similarity_ranking_progress()
                .unwrap();
        assert_eq!(progress.phase, "write");
        assert_eq!(progress.completed, 0);
        assert_eq!(progress.started_at_epoch_seconds, 1);
        assert!(progress.phase_started_at_epoch_seconds > 2);
        *crate::trigger_similarity::ranking::RANKING_PROGRESS
            .get()
            .unwrap()
            .lock()
            .unwrap() = None;
    }

    #[test]
    fn target_scratch_reuses_storage_without_leaking_rule_weights() {
        let mut scratch = RankingTargetScratch::default();
        scratch.prepare(8, 4);
        scratch.add_candidate_rule_weight(3, 1.5, 0.5);
        scratch.add_candidate_rule_weight(3, 0.5, 1.0);
        assert_eq!(scratch.candidate_indices, vec![3]);
        assert_eq!(scratch.candidate_intersection_weights[3], 2.0);
        assert_eq!(scratch.candidate_timing_upper_weights[3], 1.25);

        let weights_capacity = scratch.candidate_intersection_weights.capacity();
        let indices_capacity = scratch.candidate_indices.capacity();
        scratch.prepare(8, 4);
        scratch.add_candidate_rule_weight(3, 4.0, 0.25);
        assert_eq!(scratch.candidate_indices, vec![3]);
        assert_eq!(scratch.candidate_intersection_weights[3], 4.0);
        assert_eq!(scratch.candidate_timing_upper_weights[3], 1.0);
        assert_eq!(
            scratch.candidate_intersection_weights.capacity(),
            weights_capacity
        );
        assert_eq!(scratch.candidate_indices.capacity(), indices_capacity);
    }
}
