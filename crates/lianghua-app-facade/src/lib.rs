//! Page-level use cases that compose multiple application capabilities.

pub use lianghua_core::utils;
pub use lianghua_engine::{data, download, scoring};
pub use lianghua_provider::crawler;

pub mod cyq_chen;
pub mod details;

pub mod concept_stock_pick {
    pub use lianghua_app_strategy::stock_pick::{StockPickResultData, run_concept_stock_pick};
}

pub mod expression_stock_pick {
    pub use lianghua_app_strategy::stock_pick::{
        ExpressionStockPickTemplateValidationData, StockPickResultData, get_stock_pick_options,
        run_expression_stock_pick, validate_expression_stock_pick_template_expression,
    };
}
