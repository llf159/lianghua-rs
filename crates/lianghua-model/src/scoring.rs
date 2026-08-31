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

#[derive(Debug, Default)]
pub struct ScoreBatch {
    pub summary_rows: Vec<ScoreSummary>,
    pub detail_rows: Vec<ScoreDetails>,
    pub scene_rows: Vec<SceneDetails>,
    pub scene_backtest_rows: Vec<SceneBacktestRow>,
}

impl ScoreBatch {
    pub fn extend(&mut self, other: ScoreBatch) {
        self.summary_rows.extend(other.summary_rows);
        self.detail_rows.extend(other.detail_rows);
        self.scene_rows.extend(other.scene_rows);
        self.scene_backtest_rows.extend(other.scene_backtest_rows);
    }
}

#[derive(Debug)]
pub enum ScoreWriteMessage {
    Batch(ScoreBatch),
    Abort(String),
}

#[derive(Debug, Default, Clone)]
pub struct ScoreWriteProfile {
    pub total_ms: u64,
    pub drop_indexes_ms: u64,
    pub attach_source_db_ms: Option<u64>,
    pub delete_range_ms: u64,
    pub receive_and_append_batches_ms: u64,
    pub summary_rank_ms: u64,
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
