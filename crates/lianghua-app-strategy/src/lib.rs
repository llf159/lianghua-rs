//! Strategy authoring, ranking, validation, and analysis workflows.

use lianghua_backtest::simulate;
use lianghua_core::{expr, utils};
use lianghua_data::data;
use lianghua_download::download;
use lianghua_scoring::scoring;

pub mod convolution_rank;
pub mod manage;
pub mod overview;
pub mod overview_classic;
pub mod paper_validation;
pub mod ranking_compute;
pub mod statistics;
pub mod stock_pick;
pub mod stock_similarity;
pub mod trigger_similarity;
