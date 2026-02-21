// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Core types and traits for the roboflow-pipeline crate.
//!
//! This module provides the foundational abstractions that enable
//! format-agnostic dataset writing. The key components are:
//!
//! # Core Types
//!
//! - [`frame::AlignedFrame`] - Universal data transfer object for aligned sensor data
//! - [`frame::ImageData`] - Image data with metadata
//! - [`frame::AudioData`] - Audio data with metadata
//! - [`stats::WriterStats`] - Statistics about write operations
//!
//! # Core Traits
//!
//! - [`traits::FormatWriter`] - Main trait for writing frames to any format
//! - [`traits::EpisodeManager`] - Trait for episode lifecycle management
//! - [`traits::VideoPathScheme`] - Trait for format-specific video path generation
//!
//! # Registry
//!
//! - [`registry::FormatRegistry`] - Dynamic format discovery and creation
//!
//! # Example
//!
//! ```rust,ignore
//! use roboflow_dataset::core::{FormatWriter, AlignedFrame, EpisodeManager};
//!
//! fn process_dataset<W: FormatWriter>(writer: &mut W, frames: &[AlignedFrame]) -> Result<()> {
//!     writer.start_episode(None)?;
//!     writer.write_batch(frames)?;
//!     writer.finish_episode()?;
//!     let stats = writer.finalize()?;
//!     println!("Wrote {} frames", stats.frames_written);
//!     Ok(())
//! }
//! ```

pub mod error;
pub mod frame;
pub mod registry;
pub mod stats;
pub mod traits;

// Re-export commonly used types
pub use error::{DatasetWriterError, PipelineError, Result, VideoError};
pub use frame::{AlignedFrame, AudioData, CameraInfo, DatasetFrame, ImageData, UploadState};
pub use registry::{FormatDescriptor, FormatRegistry, register_format};
pub use stats::{EpisodeStats, ProgressStats, WriterStats};
pub use traits::{EpisodeManager, FormatContext, FormatFactory, FormatWriter, VideoPathScheme};
