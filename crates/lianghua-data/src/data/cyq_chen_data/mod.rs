//! 筹码分布（陈氏）快照库：建库、增量维护、重建与修复。
//!
//! - `store`：库结构、索引与元数据
//! - `compute`：批量计算与写入通道
//! - `maintain`：增量维护、全量重建与单股修复

mod compute;
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
use std::sync::Arc;
pub(super) const CYQ_CHEN_SNAPSHOT_TABLE: &str = "cyq_chen_snapshot";
pub(super) const CYQ_CHEN_BIN_TABLE: &str = "cyq_chen_bin";
pub(super) const CYQ_CHEN_META_TABLE: &str = "cyq_chen_meta";
pub(super) const DEFAULT_ADJ_TYPE: &str = "qfq";
pub(super) const CYQ_CHEN_GROUP_SIZE: usize = 8;
pub(super) const CYQ_CHEN_QUEUE_BOUND: usize = 2;
pub(super) const CYQ_CHEN_FLUSH_BATCH_SIZE: usize = 32;
pub(super) const CYQ_CHEN_SCHEMA_VERSION: &str = "4";

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
pub(super) struct ComputedCyqChenStock {
    ts_code: String,
    snapshots: Vec<ChenChipSnapshot>,
}

pub(super) struct CyqChenInitialState {
    state_trade_date: String,
    bins: Vec<ChenChipBin>,
    main_ratio_history: Vec<Arc<Vec<Option<f64>>>>,
}

#[derive(Debug, Default)]
pub(super) struct CyqChenWriteBatch {
    stocks: Vec<ComputedCyqChenStock>,
}

#[derive(Debug)]
pub(super) enum CyqChenWriteMessage {
    Batch(CyqChenWriteBatch),
    Abort(String),
}
