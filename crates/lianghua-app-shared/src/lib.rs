//! Shared application-layer primitives without page ownership.

use lianghua_data::data;

mod date;
mod stock_metadata;
mod symbol;

pub use date::{normalize_trade_date, resolve_trade_date};
pub use stock_metadata::{
    build_area_map, build_circ_mv_map, build_concepts_map, build_industry_map,
    build_latest_vol_map, build_most_related_concept_map, build_name_map, build_total_mv_map,
    filter_mv,
};
pub use symbol::{canonical_ts_code, normalize_ts_code};
