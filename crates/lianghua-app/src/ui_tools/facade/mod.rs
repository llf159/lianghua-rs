//! Compatibility facades for stock-picking entry points.

pub mod cyq_chen;
pub mod details;

pub mod concept_stock_pick {
    pub use crate::ui_tools::strategy::stock_pick::{StockPickResultData, run_concept_stock_pick};
}

pub mod expression_stock_pick {
    pub use crate::ui_tools::strategy::stock_pick::{
        ExpressionStockPickTemplateValidationData, StockPickResultData, get_stock_pick_options,
        run_expression_stock_pick, validate_expression_stock_pick_template_expression,
    };
}
