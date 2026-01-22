// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

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
