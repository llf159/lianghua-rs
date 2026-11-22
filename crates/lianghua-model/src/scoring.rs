use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TieBreakWay {
    TsCode,
    KdjJ,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SceneResolvedStage {
    Observe,
    Trigger,
    Confirm,
    Fail,
}

impl SceneResolvedStage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Observe => "observe",
            Self::Trigger => "trigger",
            Self::Confirm => "confirm",
            Self::Fail => "fail",
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct ScoreSummary {
    pub ts_code: String,
    pub trade_date: String,
    pub total_score: f64,
    pub rank: Option<i64>,
}

#[derive(Debug, Default, Clone)]
pub struct ScoreDetails {
    pub ts_code: String,
    pub trade_date: String,
    pub rule_name: String,
    pub rule_score: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CompactRuleScore {
    pub summary_index: u32,
    pub rule_id: u32,
    pub rule_score: f64,
}

#[derive(Debug, Default, Clone)]
pub struct SceneDetails {
    pub ts_code: String,
    pub trade_date: String,
    pub scene_name: String,
    pub direction: String,
    pub stage: Option<String>,
    pub stage_score: f64,
    pub risk_score: f64,
    pub confirm_strength: f64,
    pub risk_intensity: f64,
    pub total_score: f64,
    pub scene_rank: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct SceneBacktestRow {
    pub ts_code: Arc<str>,
    pub trade_date: Arc<str>,
    pub scene_name: Arc<str>,
    pub stage: Option<SceneResolvedStage>,
}

#[derive(Debug, Clone)]
pub struct TrackingScoreState {
    pub ts_code: String,
    pub trade_date: String,
    pub config_hash: String,
    pub input_stamp: String,
    pub state_json: String,
}

#[derive(Debug, Default)]
pub struct ScoreBatch {
    pub summary_rows: Vec<ScoreSummary>,
    pub detail_rows: Vec<ScoreDetails>,
    pub compact_rule_rows: Vec<CompactRuleScore>,
    pub scene_rows: Vec<SceneDetails>,
    pub scene_backtest_rows: Vec<SceneBacktestRow>,
    pub tracking_states: Vec<TrackingScoreState>,
}

impl ScoreBatch {
    pub fn memory_bytes(&self) -> usize {
        self.summary_rows.capacity() * std::mem::size_of::<ScoreSummary>()
            + self.detail_rows.capacity() * std::mem::size_of::<ScoreDetails>()
            + self.compact_rule_rows.capacity() * std::mem::size_of::<CompactRuleScore>()
            + self.scene_rows.capacity() * std::mem::size_of::<SceneDetails>()
            + self.tracking_states.capacity() * std::mem::size_of::<TrackingScoreState>()
            + self
                .summary_rows
                .iter()
                .map(|r| r.ts_code.capacity() + r.trade_date.capacity())
                .sum::<usize>()
            + self
                .detail_rows
                .iter()
                .map(|r| r.ts_code.capacity() + r.trade_date.capacity() + r.rule_name.capacity())
                .sum::<usize>()
            + self
                .scene_rows
                .iter()
                .map(|r| {
                    r.ts_code.capacity()
                        + r.trade_date.capacity()
                        + r.scene_name.capacity()
                        + r.direction.capacity()
                        + r.stage.as_ref().map_or(0, String::capacity)
                })
                .sum::<usize>()
            + self
                .tracking_states
                .iter()
                .map(|r| {
                    r.ts_code.capacity()
                        + r.trade_date.capacity()
                        + r.config_hash.capacity()
                        + r.input_stamp.capacity()
                        + r.state_json.capacity()
                })
                .sum::<usize>()
    }
    pub fn extend(&mut self, other: ScoreBatch) {
        let summary_offset = self.summary_rows.len() as u32;
        self.summary_rows.extend(other.summary_rows);
        self.detail_rows.extend(other.detail_rows);
        self.compact_rule_rows
            .extend(other.compact_rule_rows.into_iter().map(|mut row| {
                row.summary_index = row.summary_index.saturating_add(summary_offset);
                row
            }));
        self.scene_rows.extend(other.scene_rows);
        self.scene_backtest_rows.extend(other.scene_backtest_rows);
        self.tracking_states.extend(other.tracking_states);
    }
}

#[derive(Debug)]
pub enum ScoreWriteMessage {
    Begin { resume_key: String, resume: bool },
    Finish,
    StrategySnapshot(String),
    TrackingCalendar(String),
    Batch(ScoreBatch),
    BudgetedBatch(ScoreBatch, Arc<crate::resource::BytePermit>),
    Abort(String),
}

#[derive(Debug, Default, Clone)]
pub struct ScoreWriteProfile {
    pub total_ms: u64,
    pub drop_indexes_ms: u64,
    pub attach_source_db_ms: Option<u64>,
    pub delete_range_ms: u64,
    pub receive_and_append_batches_ms: u64,
    pub scene_rank_ms: u64,
    pub summary_rank_ms: u64,
    pub checkpoint_ms: u64,
    pub commit_ms: u64,
    pub detach_source_db_ms: Option<u64>,
    pub recreate_indexes_ms: u64,
    pub batch_count: usize,
}

#[derive(Debug, Default, Clone)]
pub struct RankTiebreakProfile {
    pub total_ms: u64,
    pub attach_source_db_ms: Option<u64>,
    pub update_rank_ms: u64,
    pub detach_source_db_ms: Option<u64>,
}
