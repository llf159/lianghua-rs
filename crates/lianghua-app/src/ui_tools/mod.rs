//! Application use cases grouped by business capability.
//!
//! New code should use the nested modules (`chart`, `data`, `market`,
//! `strategy`, and `shared`). The root-level module re-exports below preserve
//! the historical API consumed by the Tauri adapter.

pub mod chart {
    pub use lianghua_app_chart::{indicator, indicator_settings};
}

pub mod data {
    pub use lianghua_app_data::{download, import, viewer};
}

pub mod expression {
    pub use lianghua_app_expression::*;
}

pub mod facade {
    pub use lianghua_app_facade::{concept_stock_pick, cyq_chen, details, expression_stock_pick};
}

pub mod market {
    pub use lianghua_app_market::{
        all_market_monitor, dragon_tiger, intraday_monitor, realtime, watch_observe,
    };
}

pub mod shared {
    pub use lianghua_app_shared::*;
}

pub mod strategy {
    pub use lianghua_app_strategy::{
        convolution_rank, manage, overview, overview_classic, paper_validation, ranking_compute,
        statistics, stock_pick, stock_similarity, trigger_similarity,
    };
}

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
