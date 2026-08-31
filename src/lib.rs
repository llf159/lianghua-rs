//! Compatibility facade for the split backend workspace.
//!
//! New code should depend on the narrowest `lianghua-*` crate. Existing
//! consumers can continue using the historical `lianghua_rs::*` paths.

pub use lianghua_app::ui_tools;
pub use lianghua_backtest::simulate;
pub use lianghua_core::{expr, utils};
pub use lianghua_engine::{data, download, scoring};
pub use lianghua_provider::crawler;
