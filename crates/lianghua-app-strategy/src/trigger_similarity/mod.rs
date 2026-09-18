pub mod ranking;

mod channel;
mod fingerprint;
mod load;
mod page;
mod sample;
#[cfg(test)]
mod test_support;

pub use load::list_strategy_trigger_similarity_benchmark_index_codes;
pub use page::get_strategy_trigger_similarity_page;

use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
pub(super) const DEFAULT_WINDOW_TRADE_DAYS: usize = 20;
pub(super) const DEFAULT_POOL_SEGMENTS: usize = 5;
pub(super) const DEFAULT_OUTCOME_TRADE_DAYS: usize = 5;
pub(super) const DEFAULT_LIMIT: usize = 30;
pub(super) const MAX_POOL_SEGMENTS: usize = 12;
pub(super) const MIN_SAMPLE_GAP_TRADE_DAYS: usize = 90;
pub(super) const RECENT_CANDIDATE_ANCHORS: usize = 15_000;
pub(super) const HISTORY_DIVERSITY_ANCHORS: usize = (50_000) - RECENT_CANDIDATE_ANCHORS;
pub(super) const RATING_SAMPLE_LIMIT: usize = 30;
pub(super) const RATING_MAX_PER_OUTCOME_WINDOW: usize = 3;
pub(super) const ANCHOR_CHUNK_SIZE: usize = 8_192;
pub(super) const SHRINKAGE_STRENGTH: f64 = 8.0;
pub(super) const EPS: f64 = 1e-12;
pub(super) const TRIGGER_SIMILARITY_WEIGHT: f64 = 0.40;
pub(super) const PRICE_VOLUME_SIMILARITY_WEIGHT: f64 = 0.20;
pub(super) const INDICATOR_SIMILARITY_WEIGHT: f64 = 0.15;
pub(super) const MARKET_SIMILARITY_WEIGHT: f64 = 0.25;
pub(super) const TRIGGER_RULE_SET_WEIGHT: f64 = 0.45;
pub(super) const TRIGGER_RULE_TIMING_WEIGHT: f64 = 0.35;
pub(super) const TRIGGER_AGGREGATE_RHYTHM_WEIGHT: f64 = 0.20;
pub(super) const TRIGGER_TIME_DECAY_DAYS: f64 = 3.0;
pub(super) const KERNEL_NAMES: [&str; 8] = [
    "均匀核",
    "短期指数核（5-10日）",
    "中期指数核（10-30日）",
    "近期增强核",
    "前中趋势核",
    "中后趋势核",
    "前后趋势核",
    "拐点核",
];

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyTriggerSimilarityTarget {
    pub ts_code: String,
    pub name: Option<String>,
    pub industry: Option<String>,
    pub concept: Option<String>,
    pub start_trade_date: String,
    pub end_trade_date: String,
    pub trigger_count: usize,
    pub rule_names: Vec<String>,
    pub pooled_feature_dimension: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyTriggerSimilarityOutcomeSummary {
    pub sample_count: usize,
    pub effective_sample_count: f64,
    pub weighted_return_pct: Option<f64>,
    pub weighted_excess_return_pct: Option<f64>,
    pub shrunk_excess_return_pct: Option<f64>,
    pub weighted_positive_rate: Option<f64>,
    pub weighted_median_excess_return_pct: Option<f64>,
    pub winsorized_excess_return_pct: Option<f64>,
    pub weighted_excess_positive_rate: Option<f64>,
    pub weighted_mfe_pct: Option<f64>,
    pub weighted_mae_pct: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyTriggerSimilarityRow {
    pub ts_code: String,
    pub name: Option<String>,
    pub industry: Option<String>,
    pub concept: Option<String>,
    pub candidate_start_trade_date: String,
    pub candidate_end_trade_date: String,
    pub outcome_start_trade_date: String,
    pub outcome_end_trade_date: String,
    pub similarity_score: f64,
    pub trigger_similarity: f64,
    pub price_volume_similarity: Option<f64>,
    pub indicator_similarity: Option<f64>,
    pub market_similarity: Option<f64>,
    pub matched_rule_count: usize,
    pub matched_rule_names: Vec<String>,
    pub candidate_trigger_count: usize,
    pub forward_return_pct: f64,
    pub forward_excess_return_pct: Option<f64>,
    pub mfe_pct: f64,
    pub mae_pct: f64,
    pub total_score: Option<f64>,
    pub rank: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyTriggerSimilarityPageData {
    pub resolved_trade_date: String,
    pub resolved_ts_code: String,
    pub window_trade_days: usize,
    pub pool_segments: usize,
    pub outcome_trade_days: usize,
    pub historical_cutoff_date: String,
    pub benchmark_index_code: String,
    pub kernel_names: Vec<String>,
    pub indicator_columns: Vec<String>,
    pub candidate_universe_count: usize,
    pub candidate_anchor_count: usize,
    pub evaluated_anchor_count: usize,
    pub candidate_pool_truncated: bool,
    pub target: StrategyTriggerSimilarityTarget,
    pub outcome_summary: StrategyTriggerSimilarityOutcomeSummary,
    pub items: Vec<StrategyTriggerSimilarityRow>,
}

#[derive(Debug, Clone)]
pub(super) struct Anchor {
    id: usize,
    ts_code: String,
    start_trade_date: String,
    end_trade_date: String,
}

#[derive(Default)]
pub(super) struct RuleCatalog {
    ids: HashMap<String, usize>,
    names: Vec<String>,
}

impl RuleCatalog {
    fn intern(&mut self, name: String) -> usize {
        if let Some(&id) = self.ids.get(&name) {
            return id;
        }
        let id = self.names.len();
        self.names.push(name.clone());
        self.ids.insert(name, id);
        id
    }
}

#[derive(Debug, Clone)]
pub(super) struct RuleEvent {
    rule_id: usize,
    trade_date: String,
    score: f64,
}

#[derive(Debug, Clone)]
pub(super) struct MarketObservation {
    trade_date: String,
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
    pct_chg: Option<f64>,
    vol: Option<f64>,
    amount: Option<f64>,
    turnover: Option<f64>,
    net_flow: Option<f64>,
    indicators: Vec<Option<f64>>,
}

#[derive(Debug, Clone)]
pub(super) struct FutureObservation {
    trade_date: String,
    open: f64,
    close: f64,
    high: f64,
    low: f64,
}

#[derive(Debug, Clone)]
pub(super) struct Outcome {
    start_trade_date: String,
    end_trade_date: String,
    return_pct: f64,
    excess_return_pct: Option<f64>,
    mfe_pct: f64,
    mae_pct: f64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct BenchmarkObservation {
    open: f64,
    close: f64,
}

#[derive(Debug, Clone)]
pub(super) struct ChannelFingerprint {
    channel_offsets: Vec<usize>,
    normalized_values: Vec<f64>,
    channel_states: Vec<u8>,
    dimension: usize,
    has_vectors: bool,
    all_channels_nonzero: bool,
}

#[derive(Debug, Clone)]
pub(super) struct EventFingerprint {
    trigger: TriggerFingerprint,
    price_volume: ChannelFingerprint,
    indicators: ChannelFingerprint,
    market: Arc<ChannelFingerprint>,
}

impl EventFingerprint {
    fn dimension(&self) -> usize {
        self.trigger.dimension()
            + self.price_volume.dimension
            + self.indicators.dimension
            + self.market.dimension
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RuleTriggerHit {
    day_index: usize,
    score: f64,
}

#[derive(Debug, Clone)]
pub(super) struct TriggerFingerprint {
    by_rule: HashMap<usize, Vec<RuleTriggerHit>>,
    total_count: Vec<f64>,
    total_score: Vec<f64>,
    total_count_norm: f64,
    total_score_norm: f64,
}

impl TriggerFingerprint {
    fn dimension(&self) -> usize {
        self.total_count.len()
            + self.total_score.len()
            + self
                .by_rule
                .values()
                .map(|hits| hits.len() * 2)
                .sum::<usize>()
    }
}

#[derive(Debug, Clone)]
pub(super) struct EventSample {
    anchor: Anchor,
    fingerprint: EventFingerprint,
    trigger_count: usize,
    outcome: Option<Outcome>,
    total_score: Option<f64>,
    rank: Option<i64>,
}

#[derive(Debug, Clone)]
pub(super) struct MarketSchema {
    columns: HashMap<String, String>,
    indicator_columns: Vec<String>,
}

#[derive(Debug, Default, Clone)]
pub(super) struct MarketEnvironment {
    by_date: HashMap<String, Vec<Option<f64>>>,
    channel_count: usize,
}
