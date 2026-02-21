// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Core traits for format-agnostic dataset writing.
//!
//! This module defines the core abstractions that allow the pipeline to work
//! with multiple dataset formats without format-specific coupling.
//!
//! # Design Philosophy
//!
//! 1. **FormatWriter** - The main trait for writing frames to any format
//! 2. **EpisodeManager** - Trait for episode lifecycle management
//! 3. **VideoPathScheme** - Trait for format-specific video path generation
//!
//! These traits are designed to eliminate the need for `downcast_mut` calls
//! in the pipeline by providing all necessary operations through trait methods.

use super::stats::EpisodeStats;
use std::any::Any;
use std::path::PathBuf;

// Re-export types from formats/common for use in traits
pub use crate::formats::common::{AlignedFrame, WriterStats};
pub use roboflow_core::Result;

/// Core trait for writing frames to dataset formats.
///
/// This trait extends the basic writer functionality with episode management
/// and format introspection, eliminating the need for downcasting.
///
/// # Lifecycle
///
/// 1. `start_episode()` - Begin a new episode (optional, for multi-episode formats)
/// 2. `write_frame()` / `write_batch()` - Write frames to the current episode
/// 3. `finish_episode()` - Complete the current episode
/// 4. `finalize()` - Finalize the entire dataset
///
/// # Example
///
/// ```rust,ignore
/// use roboflow_dataset::core::{FormatWriter, AlignedFrame};
///
/// fn process_episode<W: FormatWriter>(writer: &mut W, frames: &[AlignedFrame]) -> Result<()> {
///     writer.start_episode(None)?;
///     writer.write_batch(frames)?;
///     writer.finish_episode()?;
///     Ok(())
/// }
/// ```
pub trait FormatWriter: Send + Sync + Any {
    /// Write a single aligned frame to the dataset.
    ///
    /// This method is called for each frame in the output, in order.
    /// Implementations may buffer data for performance.
    fn write_frame(&mut self, frame: &AlignedFrame) -> Result<()>;

    /// Write multiple frames in a batch.
    ///
    /// Default implementation calls `write_frame` for each frame.
    /// Implementations may override this for better performance.
    fn write_batch(&mut self, frames: &[AlignedFrame]) -> Result<()> {
        for frame in frames {
            self.write_frame(frame)?;
        }
        Ok(())
    }

    /// Finalize the dataset and write metadata files.
    ///
    /// Called after all frames have been written. Writes metadata
    /// files (info.json, episodes, etc.) and returns statistics.
    fn finalize(&mut self) -> Result<WriterStats>;

    /// Get the number of frames written so far.
    fn frame_count(&self) -> usize;

    // === Episode Management ===

    /// Start a new episode.
    ///
    /// # Arguments
    ///
    /// * `task_index` - Optional task index for this episode
    ///
    /// # Returns
    ///
    /// The episode index that was started.
    ///
    /// # Note
    ///
    /// For formats that don't support episodes, this is a no-op
    /// and returns 0. If `task_index` is provided but not supported,
    /// a debug log message is emitted.
    fn start_episode(&mut self, task_index: Option<usize>) -> Result<usize> {
        if task_index.is_some() && !self.supports_episodes() {
            tracing::debug!(
                format = self.format_name(),
                task_index = ?task_index,
                "task_index provided but format does not support episodes"
            );
        }
        Ok(0)
    }

    /// Finish the current episode and write its data.
    ///
    /// # Returns
    ///
    /// Statistics about the completed episode.
    ///
    /// # Note
    ///
    /// For formats that don't support episodes, this is a no-op.
    fn finish_episode(&mut self) -> Result<EpisodeStats> {
        Ok(EpisodeStats::default())
    }

    /// Get the current episode index (if supported).
    ///
    /// Returns `None` for formats that don't track episodes.
    fn episode_index(&self) -> Option<usize> {
        None
    }

    /// Check if this format supports episode management.
    fn supports_episodes(&self) -> bool {
        false
    }

    // === Format Information ===

    /// Get the format name (e.g., "LeRobot", "HDF5").
    fn format_name(&self) -> &'static str;

    /// Get the format version string.
    fn format_version(&self) -> &'static str {
        "unknown"
    }

    // === Video Handling ===

    /// Check if this format handles video encoding internally.
    ///
    /// If `true`, the pipeline will delegate video encoding to the writer.
    /// If `false`, the pipeline will handle video encoding separately.
    fn handles_video(&self) -> bool {
        true
    }

    /// Get the video path scheme for this format.
    ///
    /// Returns `None` if the format doesn't use video or uses default paths.
    fn video_path_scheme(&self) -> Option<Box<dyn VideoPathScheme>> {
        None
    }

    // === Downcasting Support ===

    /// Return `self` as `&dyn Any` for downcasting.
    fn as_any(&self) -> &dyn Any;

    /// Return `self` as `&mut dyn Any` for mutable downcasting.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Trait for managing episode lifecycle.
///
/// This trait provides a unified interface for episode management
/// that can be implemented by format writers or used as a trait object.
///
/// # Implementation
///
/// A blanket implementation is provided for any type that implements
/// `FormatWriter`, so you typically don't need to implement this directly.
pub trait EpisodeManager: Send {
    /// Start a new episode.
    ///
    /// # Arguments
    ///
    /// * `task_index` - Optional task index for this episode
    ///
    /// # Returns
    ///
    /// The episode index that was started.
    fn start_episode(&mut self, task_index: Option<usize>) -> Result<usize>;

    /// Finish the current episode.
    ///
    /// # Returns
    ///
    /// Statistics about the completed episode.
    fn finish_episode(&mut self) -> Result<EpisodeStats>;

    /// Get the current episode index.
    fn current_episode(&self) -> Option<usize>;

    /// Get the number of episodes completed.
    fn episodes_completed(&self) -> usize;
}

/// Blanket implementation of EpisodeManager for FormatWriter.
impl<T: FormatWriter> EpisodeManager for T {
    fn start_episode(&mut self, task_index: Option<usize>) -> Result<usize> {
        FormatWriter::start_episode(self, task_index)
    }

    fn finish_episode(&mut self) -> Result<EpisodeStats> {
        FormatWriter::finish_episode(self)
    }

    fn current_episode(&self) -> Option<usize> {
        self.episode_index()
    }

    fn episodes_completed(&self) -> usize {
        // Default implementation - formats can override
        self.episode_index().map(|i| i + 1).unwrap_or(0)
    }
}

/// Trait for format-specific video path generation.
///
/// Different formats use different directory structures for video files.
/// This trait abstracts that logic to allow the video encoder to work
/// with any format.
///
/// # Example Formats
///
/// - **LeRobot v2.1**: `videos/chunk-{chunk:03d}/{camera}/episode_{episode:06d}.mp4`
/// - **RLDS**: `episode_{episode}/videos/{camera}.mp4`
/// - **HDF5**: Videos stored internally (no file paths needed)
pub trait VideoPathScheme: Send + Sync + std::fmt::Debug {
    /// Generate the video path for a specific episode and camera.
    ///
    /// # Arguments
    ///
    /// * `episode` - Episode index
    /// * `camera` - Camera name/identifier
    /// * `chunk` - Chunk index (for chunked formats like LeRobot)
    ///
    /// # Returns
    ///
    /// Relative path to the video file (e.g., `videos/chunk-000/cam_left/episode_000000.mp4`)
    fn video_path(&self, episode: usize, camera: &str, chunk: usize) -> PathBuf;

    /// Generate the chunk directory path.
    ///
    /// # Arguments
    ///
    /// * `chunk` - Chunk index
    ///
    /// # Returns
    ///
    /// Relative path to the chunk directory
    fn chunk_dir(&self, chunk: usize) -> PathBuf;

    /// Parse episode index from a video file path.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to parse
    ///
    /// # Returns
    ///
    /// The episode index if the path matches the format, or `None`.
    fn parse_episode(&self, path: &std::path::Path) -> Option<usize>;

    /// Get the file extension for video files.
    fn video_extension(&self) -> &str {
        "mp4"
    }

    /// Get a human-readable name for this scheme.
    fn scheme_name(&self) -> &'static str;
}

/// Trait for creating format writers.
///
/// This factory trait allows formats to be registered and created
/// dynamically based on configuration.
pub trait FormatFactory: Send + Sync {
    /// The format name this factory creates.
    fn format_name(&self) -> &'static str;

    /// Create a new writer instance.
    ///
    /// # Arguments
    ///
    /// * `config` - Format-specific configuration as JSON
    /// * `context` - Creation context with storage, etc.
    fn create_writer(
        &self,
        config: &serde_json::Value,
        context: &FormatContext,
    ) -> Result<Box<dyn FormatWriter>>;

    /// Check if this format is available (feature flags, dependencies, etc.).
    fn is_available(&self) -> bool {
        true
    }

    /// Get a description of the format for documentation.
    fn description(&self) -> &'static str;
}

/// Context provided when creating a format writer.
///
/// Contains all the resources and configuration needed to create
/// a writer instance.
#[derive(Clone)]
pub struct FormatContext {
    /// Output storage URL (e.g., "s3://bucket/prefix" or "file:///output").
    pub output_url: String,

    /// Optional storage backend (if pre-configured).
    pub storage: Option<std::sync::Arc<dyn roboflow_storage::Storage>>,

    /// Base path within the storage.
    pub base_path: PathBuf,

    /// Number of worker threads for parallel operations.
    pub num_workers: usize,
}

impl Default for FormatContext {
    fn default() -> Self {
        Self {
            output_url: String::new(),
            storage: None,
            base_path: PathBuf::new(),
            num_workers: 4,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_context_default() {
        let ctx = FormatContext::default();
        assert!(ctx.output_url.is_empty());
        assert!(ctx.storage.is_none());
        assert!(ctx.base_path.as_os_str().is_empty());
        assert_eq!(ctx.num_workers, 4);
    }
}
