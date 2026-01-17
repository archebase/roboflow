//! High-level writers for creating format files.
//!
//! This module provides format-specific writers for:
//! - Creating new MCAP files
//! - Creating new BAG files
//! - Writing encoded messages

pub mod bag;
pub mod mcap;

// Re-exports for convenience
pub use bag::{BagMessage, BagWriter};
pub use mcap::ParallelMcapWriter;
