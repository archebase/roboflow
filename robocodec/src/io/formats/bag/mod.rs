// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! BAG format implementation.
//!
//! This module provides a complete ROS1 bag reader/writer implementation with:
//! - Parallel chunk-based reading for optimal performance
//! - Sequential reading
//! - Custom writer with manual chunk control for parallel compression

// Parallel reader implementation
pub mod parallel;

// Parser utilities
pub mod parser;

// Sequential reader implementation
pub mod sequential;

// Writer implementation
pub mod writer;

// Re-exports
pub use parallel::{BagFormat, ParallelBagReader};
pub use sequential::{BagSequentialFormat, SequentialBagRawIter, SequentialBagReader};
pub use writer::{BagMessage, BagWriter};
