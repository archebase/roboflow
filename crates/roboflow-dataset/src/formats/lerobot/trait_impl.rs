// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! LeRobot writer trait.
//!
//! This module defines the [`LerobotWriterTrait`] which extends the common
//! [`DatasetWriter`] trait with LeRobot-specific functionality.

use crate::formats::common::{AlignedFrame, DatasetWriter, WriterStats};
use crate::formats::lerobot::config::LerobotConfig;
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
    fn add_image(&mut self, camera: String, data: crate::formats::common::ImageData);

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
    fn metadata(&self) -> &crate::formats::lerobot::metadata::MetadataCollector;

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
    use crate::formats::common::DatasetWriter;
    use crate::formats::common::config::DatasetBaseConfig;
    use crate::formats::lerobot::config::{
        DatasetConfig, FlushingConfig, LerobotConfig, StreamingConfig, VideoConfig,
    };
    use crate::formats::lerobot::metadata::MetadataCollector;

    #[derive(Default)]
    struct DummyWriter {
        frames: usize,
        metadata: MetadataCollector,
    }

    impl DatasetWriter for DummyWriter {
        fn write_frame(&mut self, _frame: &AlignedFrame) -> Result<()> {
            self.frames += 1;
            Ok(())
        }

        fn finalize(&mut self) -> Result<WriterStats> {
            Ok(WriterStats {
                frames_written: self.frames,
                ..WriterStats::default()
            })
        }

        fn frame_count(&self) -> usize {
            self.frames
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    impl LerobotWriterTrait for DummyWriter {
        fn start_episode(&mut self, _task_index: Option<usize>) {}

        fn finish_episode(&mut self, _task_index: Option<usize>) -> Result<()> {
            Ok(())
        }

        fn register_task(&mut self, task: String) -> usize {
            self.metadata.register_task(task)
        }

        fn add_image(&mut self, _camera: String, _data: crate::formats::common::ImageData) {}

        fn metadata(&self) -> &crate::formats::lerobot::metadata::MetadataCollector {
            &self.metadata
        }

        fn frame_count(&self) -> usize {
            self.frames
        }
    }

    fn make_frame(index: usize) -> AlignedFrame {
        let mut frame = AlignedFrame::new(index, (index as u64) * 1_000_000);
        frame.add_state("observation.state".to_string(), vec![1.0, 2.0]);
        frame
    }

    fn test_config() -> LerobotConfig {
        LerobotConfig {
            dataset: DatasetConfig {
                base: DatasetBaseConfig {
                    name: "trait_impl_tests".to_string(),
                    fps: 30,
                    robot_type: None,
                },
                env_type: None,
            },
            mappings: Vec::new(),
            video: VideoConfig::default(),
            annotation_file: None,
            flushing: FlushingConfig::default(),
            streaming: StreamingConfig::default(),
        }
    }

    #[test]
    fn test_trait_exists() {
        // This test just verifies the trait compiles
        fn _accepts_trait<T: LerobotWriterTrait>(_: &T) {}
        // If this compiles, the trait exists
    }

    #[allow(deprecated)]
    #[test]
    fn test_default_methods_delegate_correctly() {
        let mut writer = DummyWriter::default();
        let config = test_config();

        writer
            .initialize_with_config(&config)
            .expect("initialize should be no-op");

        writer
            .add_frame(&make_frame(0))
            .expect("add frame should write");
        writer
            .add_frame(&make_frame(1))
            .expect("add frame should write");
        assert_eq!(DatasetWriter::frame_count(&writer), 2);
        assert_eq!(LerobotWriterTrait::frame_count(&writer), 2);

        let stats = writer
            .finalize_with_config()
            .expect("finalize should delegate");
        assert_eq!(stats.frames_written, 2);
    }

    #[test]
    fn test_register_task_and_metadata_access() {
        let mut writer = DummyWriter::default();
        let t0 = writer.register_task("pick".to_string());
        let t1 = writer.register_task("pick".to_string());
        let t2 = writer.register_task("place".to_string());

        assert_eq!(t0, t1);
        assert_ne!(t0, t2);
        assert_eq!(writer.metadata().tasks.len(), 2);
    }
}
