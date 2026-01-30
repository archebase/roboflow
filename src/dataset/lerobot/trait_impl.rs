// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! LeRobot writer trait.
//!
//! This module defines the [`LerobotWriterTrait`] which extends the common
//! [`DatasetWriter`] trait with LeRobot-specific functionality.

use crate::core::Result;
use crate::dataset::common::{AlignedFrame, DatasetWriter, WriterStats};
use crate::dataset::lerobot::config::LerobotConfig;

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
    /// This is a convenience method that calls [`DatasetWriter::initialize`]
    /// with the proper type casting.
    fn initialize_with_config(&mut self, config: &LerobotConfig) -> Result<()> {
        self.initialize(config)
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
    fn add_image(&mut self, camera: String, data: crate::dataset::common::ImageData);

    /// Finalize the dataset and write metadata files.
    ///
    /// This is a convenience method that calls [`DatasetWriter::finalize`]
    /// with the proper type casting.
    ///
    /// # Arguments
    ///
    /// * `config` - LeRobot configuration
    ///
    /// # Returns
    ///
    /// Statistics about the write operation
    fn finalize_with_config(&mut self, config: &LerobotConfig) -> Result<WriterStats> {
        self.finalize(config)
    }

    /// Get reference to metadata collector.
    ///
    /// This provides access to the collected metadata for inspection.
    fn metadata(&self) -> &crate::dataset::lerobot::metadata::MetadataCollector;

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
    fn from_aligned_frame(
        frame: &AlignedFrame,
        episode_index: usize,
    ) -> Self;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trait_exists() {
        // This test just verifies the trait compiles
        fn accepts_trait<T: LerobotWriterTrait>(_: &T) {}
        // If this compiles, the trait exists
        assert!(true);
    }
}
