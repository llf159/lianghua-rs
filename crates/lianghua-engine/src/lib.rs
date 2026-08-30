//! Storage, ingestion, indicator calculation, and scoring engine.
//!
//! The re-exports preserve the former `crate::expr`, `crate::utils`, and
//! `crate::crawler` paths while keeping the physical dependency graph acyclic.

pub use lianghua_core::{expr, utils};
pub use lianghua_provider::crawler;

pub mod data;
pub mod download;
pub mod scoring;
