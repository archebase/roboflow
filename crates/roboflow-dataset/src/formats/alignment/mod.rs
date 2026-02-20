// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Frame alignment module.
//!
//! This module provides frame alignment functionality for synchronizing
//! messages from different topics to aligned output frames.

pub mod buffer;
pub mod completion;
pub mod config;
pub mod stats;

// Re-export commonly used types
pub use buffer::{FrameAlignmentBuffer, PartialFrame, TimestampedMessage};
pub use completion::FrameCompletionCriteria;
pub use config::StreamingConfig;
pub use stats::AlignmentStats;
