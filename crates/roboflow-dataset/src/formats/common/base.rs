// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Base types and traits for dataset writers.
//!
//! This module provides shared types that are used across different
//! dataset formats (KPS, LeRobot, etc.). This promotes code reuse
//! while allowing each format to have its own specific behavior.
//!
//! # Architecture
//!
//! - [`DatasetWriter`] - Core trait that all dataset writers implement
//! - [`AlignedFrame`] - Universal data transfer object for aligned sensor data
//! - [`WriterStats`] - Common statistics structure
//!
//! # Re-exports
//!
//! This module re-exports the following types from [`roboflow_media`] for convenience:
//! - [`ImageData`] - Image data with metadata
//! - [`AudioData`] - Audio data with metadata
//! - [`CameraInfo`] - Camera calibration information

use roboflow_core::Result;
use roboflow_media::{AudioData, CameraInfo, ImageData};
use std::collections::HashMap;
use std::sync::Arc;

/// Aligned frame data ready for writing to dataset formats.
///
/// This is the universal data transfer object for all dataset writers.
/// It represents a single frame in the output dataset, with all
/// observations and actions aligned to the target timestamp.
///
/// # Design
///
/// - Uses feature-based naming (e.g., `"observation.camera_0"`, `"action.joint.position"`)
/// - Type-specific storage (images, states, actions, audio, timestamps)
/// - Timestamp-aligned: all data synchronized to `timestamp`
///
/// # Example
///
/// ```ignore
/// use roboflow_dataset::formats::common::{AlignedFrame, ImageData};
///
/// let mut frame = AlignedFrame::new(0, 1_000_000_000); // frame 0, 1 second
/// frame.add_state("observation.joint.position".to_string(), vec![0.0, 1.0, 2.0]);
/// let data = vec![0u8; 640 * 480 * 3];
/// frame.add_image("observation.camera_0".to_string(), ImageData::new(640, 480, data));
/// ```
#[derive(Debug, Clone)]
pub struct AlignedFrame {
    /// Frame index in the episode.
    pub frame_index: usize,

    /// Target timestamp for this frame (nanoseconds).
    pub timestamp: u64,

    /// Image observations by feature name (e.g., "observation.camera_0").
    /// Uses Arc for zero-copy sharing when the same image is referenced multiple times.
    pub images: HashMap<String, Arc<ImageData>>,

    /// State observations by feature name.
    pub states: HashMap<String, Vec<f32>>,

    /// Action data by feature name.
    pub actions: HashMap<String, Vec<f32>>,

    /// Additional timestamp data.
    pub timestamps: HashMap<String, u64>,

    /// Audio data by feature name.
    pub audio: HashMap<String, AudioData>,
}

impl AlignedFrame {
    /// Create a new aligned frame.
    pub fn new(frame_index: usize, timestamp: u64) -> Self {
        Self {
            frame_index,
            timestamp,
            images: HashMap::new(),
            states: HashMap::new(),
            actions: HashMap::new(),
            timestamps: HashMap::new(),
            audio: HashMap::new(),
        }
    }

    /// Add an image observation.
    pub fn add_image(&mut self, feature: String, data: ImageData) {
        self.images.insert(feature, Arc::new(data));
    }

    /// Add an image observation from Arc (zero-copy if already Arc-wrapped).
    pub fn add_image_arc(&mut self, feature: String, data: Arc<ImageData>) {
        self.images.insert(feature, data);
    }

    /// Add a state observation.
    pub fn add_state(&mut self, feature: String, values: Vec<f32>) {
        self.states.insert(feature, values);
    }

    /// Add action data.
    pub fn add_action(&mut self, feature: String, values: Vec<f32>) {
        self.actions.insert(feature, values);
    }

    /// Add timestamp data.
    pub fn add_timestamp(&mut self, feature: String, timestamp: u64) {
        self.timestamps.insert(feature, timestamp);
    }

    /// Add audio data.
    pub fn add_audio(&mut self, feature: String, data: AudioData) {
        self.audio.insert(feature, data);
    }

    /// Check if the frame has any data.
    pub fn is_empty(&self) -> bool {
        self.images.is_empty()
            && self.states.is_empty()
            && self.actions.is_empty()
            && self.audio.is_empty()
    }

    /// Get the timestamp in seconds (for formats that use f64 timestamps).
    pub fn timestamp_sec(&self) -> f64 {
        self.timestamp as f64 / 1_000_000_000.0
    }
}

/// Core dataset writer trait.
///
/// This is the unified trait that all dataset writers (KPS, LeRobot, etc.) must implement.
/// No separate format-specific traits are needed - implement this directly.
///
/// # Design
///
/// - **Format-agnostic**: Works with any dataset format
/// - **Streaming**: Writes frames incrementally, not all-at-once
/// - **Aligned data**: Assumes frames are already time-aligned
///
/// # Lifecycle
///
/// 1. `initialize()` - One-time setup before any frames
/// 2. `write_frame()` - Called for each frame in order
/// 3. `finalize()` - Writes metadata and returns statistics
///
/// # Example
///
/// ```rust,ignore
/// use roboflow::dataset::common::{DatasetWriter, AlignedFrame};
///
/// // Writers are created via type-safe builders
/// let mut writer = LerobotWriter::builder()
///     .output_dir("/output")
///     .config(config)
///     .build()?;
///
/// for frame in frames {
///     writer.write_frame(&frame)?;
/// }
/// let stats = writer.finalize()?;
/// ```
pub trait DatasetWriter: Send + std::any::Any {
    /// Write a single aligned frame to the dataset.
    ///
    /// This method is called for each frame in the output, in order.
    /// Implementations may buffer data for performance.
    ///
    /// # Arguments
    ///
    /// * `frame` - Aligned frame data with synchronized observations
    fn write_frame(&mut self, frame: &AlignedFrame) -> Result<()>;

    /// Write multiple frames in a batch.
    ///
    /// Default implementation calls `write_frame` for each frame.
    /// Implementations may override this for better performance.
    ///
    /// # Arguments
    ///
    /// * `frames` - Slice of aligned frames to write
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
    ///
    /// # Returns
    ///
    /// Statistics about the write operation (frames, bytes, duration)
    fn finalize(&mut self) -> Result<WriterStats>;

    /// Get the number of frames written so far.
    fn frame_count(&self) -> usize;

    /// Get the current episode index (if supported by the writer).
    ///
    /// Returns the episode index for writers that support multi-episode datasets
    /// (e.g., LeRobotWriter). Returns None for single-episode writers or formats
    /// that don't track episodes.
    ///
    /// This is used for checkpointing to resume from the correct episode.
    fn episode_index(&self) -> Option<usize> {
        None
    }

    /// Return `self` as `&dyn Any` for downcasting.
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Error type for dataset writer operations.
///
/// This is a general error type that can be used by any dataset format.
/// Format-specific writers may add their own error variants.
#[derive(Debug, thiserror::Error)]
pub enum DatasetWriterError {
    /// I/O error during write operation.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// HDF5 library error.
    #[error("HDF5 error: {0}")]
    Hdf5(String),

    /// Parquet encoding error.
    #[error("Parquet error: {0}")]
    Parquet(String),

    /// Video/image encoding error.
    #[error("Encoding error: {0}")]
    Encoding(String),

    /// Invalid or malformed message data.
    #[error("Invalid message data: {0}")]
    InvalidData(String),

    /// Required channel/topic not found.
    #[error("Channel not found: {0}")]
    ChannelNotFound(String),

    /// Feature not mapped in configuration.
    #[error("Feature not mapped: {0}")]
    FeatureNotMapped(String),

    /// Writer was used before initialization.
    #[error("Writer not initialized")]
    NotInitialized,

    /// Writer was used after finalization.
    #[error("Writer already finalized")]
    AlreadyFinalized,
}

/// Statistics from a dataset writer operation.
///
/// Returned after finalization to provide metrics about the write operation.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct WriterStats {
    /// Total number of frames written.
    pub frames_written: usize,

    /// Number of images encoded/written.
    pub images_encoded: usize,

    /// Number of state/action records written.
    pub state_records: usize,

    /// Size of output data in bytes.
    pub output_bytes: u64,

    /// Processing duration in seconds.
    pub duration_sec: f64,
}

impl WriterStats {
    /// Create new empty stats.
    pub fn new() -> Self {
        Self::default()
    }

    /// Calculate throughput in frames per second.
    pub fn fps(&self) -> f64 {
        if self.duration_sec > 0.0 {
            self.frames_written as f64 / self.duration_sec
        } else {
            0.0
        }
    }

    /// Calculate throughput in MB/s.
    pub fn mb_per_sec(&self) -> f64 {
        if self.duration_sec > 0.0 {
            (self.output_bytes as f64 / (1024.0 * 1024.0)) / self.duration_sec
        } else {
            0.0
        }
    }
}

/// A frame of data ready to be written to a dataset.
///
/// This is the primary input type for all writers, providing a unified
/// interface regardless of the output format (LeRobot, KPS, Zarr, etc.).
#[derive(Debug, Clone)]
pub struct DatasetFrame {
    /// Frame index within episode
    pub frame_index: usize,
    /// Episode index
    pub episode_index: usize,
    /// Timestamp (seconds)
    pub timestamp: f64,
    /// Observation state (e.g., joint positions)
    pub observation_state: Option<Vec<f32>>,
    /// Action data (e.g., commands sent to robot)
    pub action: Option<Vec<f32>>,
    /// Task index (for multi-task datasets)
    pub task_index: Option<usize>,
    /// Image data by feature name -> ImageData
    pub images: HashMap<String, ImageData>,
    /// Camera calibration info by camera name
    pub camera_info: HashMap<String, CameraInfo>,
    /// Additional data fields
    pub additional_data: HashMap<String, Vec<f32>>,
}

impl DatasetFrame {
    /// Create a new dataset frame.
    pub fn new(frame_index: usize, episode_index: usize, timestamp: f64) -> Self {
        Self {
            frame_index,
            episode_index,
            timestamp,
            observation_state: None,
            action: None,
            task_index: None,
            images: HashMap::new(),
            camera_info: HashMap::new(),
            additional_data: HashMap::new(),
        }
    }

    /// Add an image to the frame.
    pub fn with_image(mut self, name: impl Into<String>, image: ImageData) -> Self {
        self.images.insert(name.into(), image);
        self
    }

    /// Add observation state to the frame.
    pub fn with_observation_state(mut self, state: Vec<f32>) -> Self {
        self.observation_state = Some(state);
        self
    }

    /// Add action data to the frame.
    pub fn with_action(mut self, action: Vec<f32>) -> Self {
        self.action = Some(action);
        self
    }

    /// Add camera calibration info to the frame.
    pub fn with_camera_info(mut self, camera_name: impl Into<String>, info: CameraInfo) -> Self {
        self.camera_info.insert(camera_name.into(), info);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use roboflow_media::ImageDataError;

    #[test]
    fn test_image_data_new() {
        let data = vec![0u8; 640 * 480 * 3];
        let img = ImageData::new(640, 480, data);

        assert_eq!(img.width, 640);
        assert_eq!(img.height, 480);
        assert_eq!(img.pixel_count(), 640 * 480);
        assert!(img.is_rgb());
        assert!(!img.is_encoded);
    }

    #[test]
    fn test_image_data_encoded() {
        let data = vec![0u8; 1000]; // Smaller than RGB size
        let img = ImageData::encoded(640, 480, data);

        assert!(img.is_encoded);
        assert!(!img.is_rgb());
    }

    #[test]
    fn test_writer_stats() {
        let mut stats = WriterStats::new();
        stats.frames_written = 100;
        stats.duration_sec = 2.0;

        assert_eq!(stats.fps(), 50.0);
    }

    #[test]
    fn test_audio_data() {
        let samples = vec![1.0, 2.0, 3.0, 4.0];
        let audio = AudioData::new(samples, 16000, 1, 0);

        assert_eq!(audio.frames(), 4);
        assert!((audio.duration() - 0.00025).abs() < 0.0001);
    }

    #[test]
    fn test_image_data_validate() {
        // Valid RGB data
        let data = vec![0u8; 640 * 480 * 3];
        let img = ImageData::new(640, 480, data);
        assert!(img.validate());

        // Invalid RGB data (wrong size)
        let data = vec![0u8; 1000];
        let img = ImageData::new(640, 480, data);
        assert!(!img.validate());

        // Encoded data always validates
        let data = vec![0u8; 1000];
        let img = ImageData::encoded(640, 480, data);
        assert!(img.validate());
    }

    #[test]
    fn test_audio_data_validate() {
        // Valid audio data
        let samples = vec![1.0, 2.0, 3.0, 4.0];
        let audio = AudioData::new(samples, 16000, 2, 0);
        assert!(audio.validate());

        // Invalid: zero sample_rate
        let samples = vec![1.0, 2.0, 3.0, 4.0];
        let audio = AudioData::new(samples, 0, 2, 0);
        assert!(!audio.validate());

        // Invalid: zero channels
        let samples = vec![1.0, 2.0, 3.0, 4.0];
        let audio = AudioData::new(samples, 16000, 0, 0);
        assert!(!audio.validate());

        // Invalid: samples not divisible by channels
        let samples = vec![1.0, 2.0, 3.0]; // 3 samples, 2 channels = incomplete frame
        let audio = AudioData::new(samples, 16000, 2, 0);
        assert!(!audio.validate());
    }

    #[test]
    fn test_audio_data_zero_channels_duration() {
        // Test that duration() handles zero channels gracefully
        let samples = vec![1.0, 2.0, 3.0, 4.0];
        let audio = AudioData::new(samples, 16000, 0, 0);
        assert_eq!(audio.duration(), 0.0);
        assert_eq!(audio.frames(), 0);
    }

    #[test]
    fn test_aligned_frame_new() {
        let frame = AlignedFrame::new(42, 1_000_000_000);
        assert_eq!(frame.frame_index, 42);
        assert_eq!(frame.timestamp, 1_000_000_000);
        assert!(frame.images.is_empty());
        assert!(frame.states.is_empty());
        assert!(frame.actions.is_empty());
    }

    #[test]
    fn test_aligned_frame_is_empty() {
        let frame = AlignedFrame::new(0, 0);
        assert!(frame.is_empty());

        let mut frame = AlignedFrame::new(0, 0);
        frame.add_state("test".to_string(), vec![1.0]);
        assert!(!frame.is_empty());
    }

    #[test]
    fn test_aligned_frame_timestamp_sec() {
        let frame = AlignedFrame::new(0, 500_000_000); // 0.5 seconds
        assert!((frame.timestamp_sec() - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_aligned_frame_add_methods() {
        let mut frame = AlignedFrame::new(0, 0);

        frame.add_state("state".to_string(), vec![1.0, 2.0]);
        assert_eq!(frame.states.get("state"), Some(&vec![1.0, 2.0]));

        frame.add_action("action".to_string(), vec![0.5]);
        assert_eq!(frame.actions.get("action"), Some(&vec![0.5]));

        frame.add_timestamp("ts".to_string(), 123);
        assert_eq!(frame.timestamps.get("ts"), Some(&123));

        frame.add_audio("audio".to_string(), AudioData::new(vec![], 16000, 1, 0));
        assert!(frame.audio.contains_key("audio"));
    }

    #[test]
    fn test_aligned_frame_add_image() {
        let mut frame = AlignedFrame::new(0, 0);
        let img = ImageData::new(640, 480, vec![0u8; 640 * 480 * 3]);
        frame.add_image("camera".to_string(), img);

        assert!(frame.images.contains_key("camera"));
        assert!(!frame.is_empty());
    }

    #[test]
    fn test_aligned_frame_add_image_arc() {
        let mut frame = AlignedFrame::new(0, 0);
        let img = Arc::new(ImageData::new(640, 480, vec![0u8; 640 * 480 * 3]));
        frame.add_image_arc("camera".to_string(), img.clone());

        assert!(frame.images.contains_key("camera"));
    }

    #[test]
    fn test_writer_stats_default() {
        let stats = WriterStats::default();
        assert_eq!(stats.frames_written, 0);
        assert_eq!(stats.images_encoded, 0);
        assert_eq!(stats.state_records, 0);
        assert_eq!(stats.output_bytes, 0);
        assert_eq!(stats.duration_sec, 0.0);
    }

    #[test]
    fn test_writer_stats_mb_per_sec() {
        let stats = WriterStats {
            output_bytes: 10 * 1024 * 1024, // 10 MB
            duration_sec: 2.0,
            ..Default::default()
        };

        assert_eq!(stats.mb_per_sec(), 5.0);
    }

    #[test]
    fn test_image_data_depth() {
        let img = ImageData::depth(640, 480, vec![0u8; 640 * 480 * 2]);
        assert!(img.is_depth);
        assert!(!img.is_encoded);
    }

    #[test]
    fn test_image_data_with_timestamp() {
        let img = ImageData::with_timestamp(640, 480, vec![0u8; 640 * 480 * 3], 123456);
        assert_eq!(img.original_timestamp, 123456);
    }

    #[test]
    fn test_image_data_new_rgb_success() {
        let result = ImageData::new_rgb(2, 2, vec![0u8; 12]);
        assert!(result.is_ok());
        let img = result.unwrap();
        assert_eq!(img.width, 2);
        assert_eq!(img.height, 2);
    }

    #[test]
    fn test_image_data_new_rgb_error() {
        let result = ImageData::new_rgb(2, 2, vec![0u8; 6]); // Wrong size
        assert!(result.is_err());
        if let Err(ImageDataError::SizeMismatch {
            width,
            height,
            expected_size,
            actual_size,
        }) = result
        {
            assert_eq!(width, 2);
            assert_eq!(height, 2);
            assert_eq!(expected_size, 12);
            assert_eq!(actual_size, 6);
        }
    }

    #[test]
    fn test_audio_data_stereo() {
        let samples = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]; // 3 stereo frames
        let audio = AudioData::new(samples, 44100, 2, 0);
        assert_eq!(audio.frames(), 3);
        assert!((audio.duration() - (3.0 / 44100.0)).abs() < 0.0001);
    }

    #[test]
    fn test_dataset_writer_error_display() {
        let err = DatasetWriterError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file not found",
        ));
        assert!(err.to_string().contains("I/O error"));

        let err = DatasetWriterError::Encoding("test".to_string());
        assert!(err.to_string().contains("Encoding error"));

        let err = DatasetWriterError::NotInitialized;
        assert!(err.to_string().contains("not initialized"));
    }

    #[test]
    fn test_aligned_frame_clone() {
        let mut frame = AlignedFrame::new(0, 100);
        frame.add_state("test".to_string(), vec![1.0]);
        let cloned = frame.clone();
        assert_eq!(frame.frame_index, cloned.frame_index);
        assert_eq!(frame.timestamp, cloned.timestamp);
    }
}
