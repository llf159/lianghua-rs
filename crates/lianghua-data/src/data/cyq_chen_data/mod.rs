mod compute;
pub use compute::{compute_cyq_chen_stock_group_batch, resolve_cyq_chen_load_start_date};
mod maintain;
mod store;
#[cfg(test)]
mod test_support;

pub use maintain::{
    maintain_cyq_chen_incremental_if_db_exists, rebuild_cyq_chen_all,
    rebuild_cyq_chen_all_if_db_exists, rebuild_cyq_chen_all_with_progress,
    repair_cyq_chen_stocks_if_db_exists,
};
pub use store::{init_cyq_chen_db, query_cyq_chen_strategy_maintenance_status};

use crate::data::cyq_chen::ChenChipBin;
use crate::data::cyq_chen::ChenChipSnapshot;
use duckdb::arrow::record_batch::RecordBatch;
use std::sync::Arc;
pub(super) const CYQ_CHEN_SNAPSHOT_TABLE: &str = "cyq_chen_snapshot";
pub(super) const CYQ_CHEN_BIN_TABLE: &str = "cyq_chen_bin";
pub(super) const CYQ_CHEN_META_TABLE: &str = "cyq_chen_meta";
pub(super) const DEFAULT_ADJ_TYPE: &str = "qfq";
pub(super) const CYQ_CHEN_GROUP_SIZE: usize = 8;
pub(super) const CYQ_CHEN_QUEUE_BOUND: usize = 2;
pub(super) const CYQ_CHEN_FLUSH_BATCH_SIZE: usize = 32;
pub(super) const CYQ_CHEN_SCHEMA_VERSION: &str = "5";

#[derive(Debug, Clone, PartialEq)]
pub struct CyqChenRebuildSummary {
    pub snapshot_rows: usize,
    pub bin_rows: usize,
    pub warmup_days: usize,
    pub bucket_pct: f64,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CyqChenStrategyMaintenanceStatus {
    pub db_exists: bool,
    pub has_data: bool,
    pub strategy_changed: bool,
    pub detail: String,
}

#[derive(Debug)]
pub struct ComputedCyqChenStock {
    pub ts_code: String,
    pub snapshots: Vec<ChenChipSnapshot>,
    replace_history: bool,
    feature_state: Option<crate::data::cyq_chen::ChenChipFeatureState>,
}

pub struct CyqChenInitialState {
    state_trade_date: String,
    feature_state: crate::data::cyq_chen::ChenChipFeatureState,
    bins: Vec<ChenChipBin>,
    main_ratio_history: Vec<Arc<Vec<Option<f64>>>>,
}

pub struct CyqChenCheckpointRecovery<'a> {
    source_dir: &'a str,
    first_date: &'a std::sync::OnceLock<Result<String, String>>,
    report: &'a dyn Fn(&str, &str),
}

pub(super) fn decode_cyq_chen_checkpoint(
    trade_date: &str,
    bins_json: &str,
    feature_json: Option<&str>,
) -> Result<
    (
        Vec<ChenChipBin>,
        crate::data::cyq_chen::ChenChipFeatureState,
    ),
    String,
> {
    let bins: Vec<ChenChipBin> =
        serde_json::from_str(bins_json).map_err(|e| format!("新筹码分桶检查点损坏: {e}"))?;
    let state: crate::data::cyq_chen::ChenChipFeatureState =
        serde_json::from_str(feature_json.ok_or_else(|| "缺少筹码特征检查点".to_string())?)
            .map_err(|e| format!("筹码特征检查点损坏: {e}"))?;
    state.validate(trade_date, &bins)?;
    Ok((bins, state))
}

#[derive(Debug, Default)]
pub struct CyqChenWriteBatch {
    pub stocks: Vec<ComputedCyqChenStock>,
}

#[derive(Debug)]
pub(super) struct PreparedCyqChenWriteBatch {
    stock_count: usize,
    replace_ts_codes: Vec<String>,
    snapshot_rows: usize,
    bin_rows: usize,
    checkpoints: Vec<(String, String, String, Option<String>)>,
    snapshot_batch: Option<RecordBatch>,
    bin_batch: Option<RecordBatch>,
}

#[derive(Debug)]
pub(super) enum CyqChenWriteMessage {
    Batch(CyqChenWriteBatch),
    PreparedBatch(PreparedCyqChenWriteBatch),
    Abort(String),
    Finish,
}
