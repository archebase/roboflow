// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! LeRobot writer trait.
//!
//! This module defines the [`LerobotWriterTrait`] which extends the common
//! [`DatasetWriter`] trait with LeRobot-specific functionality.

use crate::common::{AlignedFrame, DatasetWriter, WriterStats};
use crate::lerobot::config::LerobotConfig;
use roboflow_core::Result;

/// LeRobot v2.1 writer trait.
///
/// This trait extends the generic [`DatasetWriter`] with LeRobot-specific
/// methods for episode management and task registration.
///
/// # Relationship to DatasetWriter
///
/// `LerobotWriterTrait` is LeRobot-specific (uses `LerobotConfig`) while
/// [`DatasetWriter`] is format-agnostic. This trait provides a more ergonomic
/// API for LeRobot-specific use cases.
///
/// # Example
///
/// ```rust,ignore
/// use roboflow::dataset::lerobot::{LerobotWriterTrait, LerobotWriter, LerobotConfig};
///
/// let config = LerobotConfig::from_file("config.toml")?;
/// let mut writer = LerobotWriter::create("/output", config)?;
/// writer.initialize_with_config(&config)?;
///
/// writer.start_episode(Some(0));
/// for frame in frames {
///     writer.write_frame(&frame)?;
/// }
/// writer.finish_episode(Some(0))?;
///
/// let stats = writer.finalize_with_config(&config)?;
/// ```
pub trait LerobotWriterTrait: DatasetWriter {
    /// Initialize the writer with LeRobot configuration.
    ///
    /// This is a no-op in the new API since configuration is provided
    /// via the builder. Kept for backward compatibility.
    #[deprecated(
        since = "0.3.0",
        note = "Configuration is now provided via the builder pattern. Use LerobotWriter::builder().config(cfg).build() instead."
    )]
    fn initialize_with_config(&mut self, _config: &LerobotConfig) -> Result<()> {
        Ok(())
    }

    /// Start a new episode.
    ///
    /// # Arguments
    ///
    /// * `task_index` - Optional task index for this episode
    fn start_episode(&mut self, task_index: Option<usize>);

    /// Finish the current episode and write its data.
    ///
    /// # Arguments
    ///
    /// * `task_index` - Optional task index for this episode
    fn finish_episode(&mut self, task_index: Option<usize>) -> Result<()>;

    /// Register a task and return its index.
    ///
    /// # Arguments
    ///
    /// * `task` - Task description string
    ///
    /// # Returns
    ///
    /// The task index for use in episode data
    fn register_task(&mut self, task: String) -> usize;

    /// Add a frame to the current episode.
    ///
    /// This is a convenience method that works with LeRobot's frame structure.
    ///
    /// # Arguments
    ///
    /// * `frame` - Aligned frame data to add
    fn add_frame(&mut self, frame: &AlignedFrame) -> Result<()> {
        self.write_frame(frame)
    }

    /// Add image data for a camera frame.
    ///
    /// # Arguments
    ///
    /// * `camera` - Camera name (e.g., "cam_high")
    /// * `data` - Image data
    fn add_image(&mut self, camera: String, data: crate::common::ImageData);

    /// Finalize the dataset and write metadata files.
    ///
    /// This is a convenience method that calls [`DatasetWriter::finalize`].
    ///
    /// # Returns
    ///
    /// Statistics about the write operation
    fn finalize_with_config(&mut self) -> Result<WriterStats> {
        self.finalize()
    }

    /// Get reference to metadata collector.
    ///
    /// This provides access to the collected metadata for inspection.
    fn metadata(&self) -> &crate::lerobot::metadata::MetadataCollector;

    /// Get total frames written so far.
    fn frame_count(&self) -> usize;
}

/// Conversion from [`AlignedFrame`] to LeRobot's frame representation.
///
/// This trait provides conversion methods for transforming the generic
/// [`AlignedFrame`] into LeRobot-specific frame data.
pub trait FromAlignedFrame {
    /// Convert from an aligned frame.
    ///
    /// # Arguments
    ///
    /// * `frame` - Source aligned frame
    /// * `episode_index` - Episode index for the new frame
    ///
    /// # Returns
    ///
    /// A LeRobot frame with converted data
    fn from_aligned_frame(frame: &AlignedFrame, episode_index: usize) -> Self;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trait_exists() {
        // This test just verifies the trait compiles
        fn _accepts_trait<T: LerobotWriterTrait>(_: &T) {}
        // If this compiles, the trait exists
    }
}
