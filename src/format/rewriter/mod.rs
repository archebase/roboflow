//! File rewriting with transformations.
//!
//! This module provides format-specific rewriters for:
//! - Reading files from one format
//! - Applying transformations (topic rename, type rename, etc.)
//! - Writing to a new file

pub mod bag;
pub mod mcap;
pub mod mcap_engine;

// Re-exports for convenience
pub use bag::BagRewriter;
pub use mcap::McapRewriter;
pub use mcap_engine::{McapRewriteEngine, McapRewriteStats};
