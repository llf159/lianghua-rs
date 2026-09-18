//! Live-market observation and market-data presentation workflows.

use lianghua_core::{expr, utils};
use lianghua_data::data;
use lianghua_download::download;
use lianghua_provider::crawler;
use lianghua_scoring::scoring;

pub mod all_market_monitor;
pub mod dragon_tiger;
pub mod intraday_monitor;
pub mod realtime;
pub mod watch_observe;

mod scene_stage;
