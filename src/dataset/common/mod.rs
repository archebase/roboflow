// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Common dataset writing utilities.
//!
//! This module contains shared components used by different dataset formats
//! (KPS, LeRobot, etc.) to avoid code duplication.
//!
//! # Key Exports
//!
//! - [`DatasetWriter`] - Core trait for all dataset writers
//! - [`AlignedFrame`] - Universal aligned frame data structure
//! - [`ImageData`], [`AudioData`] - Shared multimedia types
//! - [`WriterStats`] - Common statistics structure
//! - [`ProgressSender`] - Channel-based progress reporting

pub mod base;
pub mod parquet_base;
pub mod progress;
pub mod video;

// Re-export core types (shared across all formats)
pub use base::{
    AlignedFrame, AudioData, DatasetWriter, DatasetWriterError, ImageData, WriterStats,
};

// Re-export parquet utilities
pub use parquet_base::{calculate_stats, FeatureStats, ParquetWriterBase};

// Re-export progress utilities
pub use progress::{ProgressReceiver, ProgressSender, ProgressUpdate};
