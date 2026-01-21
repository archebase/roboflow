// Copyright (c) 2026 ArcheBase
// Roboflow is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//     http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

//! Format-specific rewriters for robotics data transformation.
//!
//! This module provides rewriter implementations for different formats:
//! - [`facade`] - Unified facade with auto-detection
//! - [`engine`] - Shared rewrite engine logic
//! - [`mcap`] - MCAP format rewriter
//! - [`bag`] - ROS1 bag format rewriter

pub mod bag;
pub mod engine;
pub mod facade;
pub mod mcap;

// Re-export unified facade types
pub use facade::{detect_format, FormatRewriter, RewriteOptions, RewriteStats, RoboRewriter};

// Re-export shared types
pub use engine::{McapRewriteEngine, McapRewriteStats};
pub use mcap::McapRewriter;

// Note: BagRewriter is not re-exported at module level to avoid name collision
// Use it as: rewriter::bag::BagRewriter
