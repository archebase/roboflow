//! Format-specific implementations for robotics data files.
//!
//! This module provides low-level format-specific readers and writers for:
//! - **MCAP**: `mcap` - Modern robotics data format
//! - **BAG**: `bag` - ROS1 bag format
//!
//! ## Architecture
//!
//! This is the **consolidated format layer**. All format-specific code that
//! was previously split between `io::formats` and `format` is now here.
//!
//! The KPS dataset format is at `crate::io::formats::kps`.
//!
//! ## Modules
//!
//! - `mcap` - MCAP format readers and writers
//! - `bag` - ROS1 bag format readers and writers

pub mod bag;
pub mod mcap;

// Re-exports for convenience
pub use bag::{BagFormat, ParallelBagReader, SequentialBagReader};
pub use mcap::{McapFormat, ParallelMcapReader, SequentialMcapReader};
