//! Application use cases grouped by business capability.
//!
//! New code should use the nested modules (`chart`, `data`, `market`,
//! `strategy`, and `shared`). The root-level module re-exports below preserve
//! the historical API consumed by the Tauri adapter.

pub mod chart;
pub mod data;
pub mod expression;
pub mod facade;
pub mod market;
pub mod shared;
pub mod strategy;

// Shared application support kept at the root for source compatibility.
pub use shared::*;

// Historical flat module paths.
pub use chart::{indicator as chart_indicator, indicator_settings as chart_indicator_settings};
pub use data::{download as data_download, import as data_import, viewer as data_viewer};
pub use facade::{concept_stock_pick, cyq_chen, details, expression_stock_pick};
pub use market::{all_market_monitor, dragon_tiger, intraday_monitor, realtime, watch_observe};
pub use strategy::{
    convolution_rank, manage as strategy_manage, overview, overview_classic,
    paper_validation as strategy_paper_validation, ranking_compute, statistics, stock_pick,
    stock_similarity, trigger_similarity as strategy_trigger_similarity,
};
