//! Compatibility aggregation for the backend capability crates.
//!
//! Implementation belongs in the narrow `lianghua-data`,
//! `lianghua-download`, or `lianghua-scoring` crate.

pub use lianghua_core::{expr, utils};
pub use lianghua_download::download;
pub use lianghua_provider::crawler;
pub use lianghua_scoring::scoring;

pub mod data {
    pub use lianghua_data::data::*;
    pub use lianghua_download::download::simulate;
    pub use lianghua_scoring::scoring::scoring_data;
}
