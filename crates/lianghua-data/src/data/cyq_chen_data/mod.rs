mod compute;
pub use compute::{compute_cyq_chen_stock_group_batch, resolve_cyq_chen_load_start_date};
mod maintain;
mod maintenance_stage;
mod prepare;
mod store;
#[cfg(test)]
mod test_support;

pub use maintain::{
    maintain_cyq_chen_incremental_if_db_exists, rebuild_cyq_chen_all,
    rebuild_cyq_chen_all_if_db_exists, rebuild_cyq_chen_all_with_progress,
    repair_cyq_chen_stocks_if_db_exists,
};
pub use store::{
    init_cyq_chen_db, query_cyq_chen_rebuild_errors, query_cyq_chen_rebuild_status,
    query_cyq_chen_strategy_maintenance_status,
};

use crate::data::cyq_chen::ChenChipBin;
use crate::data::cyq_chen::ChenChipSnapshot;
use duckdb::arrow::record_batch::RecordBatch;
use std::path::Path;
use std::sync::Arc;
pub(super) const CYQ_CHEN_SNAPSHOT_TABLE: &str = "cyq_chen_snapshot";
pub(super) const CYQ_CHEN_BIN_TABLE: &str = "cyq_chen_bin";
pub(super) const CYQ_CHEN_META_TABLE: &str = "cyq_chen_meta";
pub(super) const DEFAULT_ADJ_TYPE: &str = "qfq";
pub(super) const CYQ_CHEN_QUEUE_BOUND: usize = 2;
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
    pub feature_changed: bool,
    pub detail: String,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CyqChenRebuildStatus {
    pub start_date: String,
    pub end_date: String,
    pub warmup_days: usize,
    pub bucket_pct: f64,
    pub completed_stocks: usize,
    pub total_stocks: usize,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CyqChenRebuildError {
    pub ts_code: String,
    pub error: String,
}

pub fn remove_cyq_chen_storage(source_dir: &str, remove_official: bool) -> Result<(), String> {
    let source_dir = Path::new(source_dir);
    let _lock = crate::data::cyq_chen_temp::lock_rebuild_directory(source_dir)?
        .ok_or_else(|| "新筹码计算正在运行，不能清理或删除筹码库。".to_string())?;
    crate::data::cyq_chen_temp::remove_stale_rebuilds(source_dir)?;
    store::remove_cyq_chen_db_artifacts(&source_dir.join(".cyq_chen.db.resume.tmp"))?;
    store::remove_cyq_chen_db_artifacts(&source_dir.join(".cyq_chen.db.maintenance.tmp"))?;
    if remove_official {
        store::remove_cyq_chen_db_artifacts(&source_dir.join("cyq_chen.db"))?;
    }
    Ok(())
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

#[derive(Debug, Clone)]
pub(super) struct PreparedCyqChenWriteBatch {
    stock_count: usize,
    completed_ts_codes: Vec<String>,
    replace_ts_codes: Vec<String>,
    errors: Vec<CyqChenRebuildError>,
    snapshot_rows: usize,
    bin_rows: usize,
    checkpoints: Vec<(String, String, String, Option<String>)>,
    snapshot_batch: Option<RecordBatch>,
    bin_batch: Option<RecordBatch>,
}

impl PreparedCyqChenWriteBatch {
    pub(super) fn with_error(ts_code: &str, error: String) -> Self {
        Self {
            stock_count: 0,
            completed_ts_codes: Vec::new(),
            replace_ts_codes: Vec::new(),
            errors: vec![CyqChenRebuildError {
                ts_code: ts_code.to_string(),
                error,
            }],
            snapshot_rows: 0,
            bin_rows: 0,
            checkpoints: Vec::new(),
            snapshot_batch: None,
            bin_batch: None,
        }
    }
}

#[derive(Debug)]
pub(super) enum CyqChenWriteMessage {
    // 正式路径在计算线程预构造批次；测试保留原始批次以验证写入侧失败处理。
    #[cfg(test)]
    Batch(CyqChenWriteBatch),
    PreparedBatch(PreparedCyqChenWriteBatch),
    Abort(String),
    Finish,
}
