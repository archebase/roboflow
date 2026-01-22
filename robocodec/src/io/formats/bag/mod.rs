// Copyright (c) 2026 ArcheBase
// Roboflow is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//     http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

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
