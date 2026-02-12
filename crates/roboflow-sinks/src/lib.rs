//! roboflow-sinks: Sink trait and implementations for writing robotics datasets

#![warn(missing_docs)]
#![warn(unused_crate_dependencies)]

mod config;
mod convert;
mod error;
mod lerobot_factory;
mod registry;

// Sink implementations
pub mod lerobot;

// Re-export factory for external use (e.g., TaskExecutor in roboflow-distributed)
pub use lerobot_factory::{LerobotWriterConfig, LerobotWriterResult, create_lerobot_writer};

pub use config::{SinkConfig, SinkType};
pub use error::{SinkError, SinkResult};
pub use registry::{create_sink, global_registry, has_sink, register_sink, registered_sinks};

// Re-export ImageFormat from roboflow_dataset (canonical location)
pub use roboflow_dataset::image::ImageFormat;

use async_trait::async_trait;
use std::collections::HashMap;

/// Camera calibration information extracted from sensor_msgs/CameraInfo.
///
/// Contains intrinsic parameters needed for camera calibration in dataset formats.
#[derive(Debug, Clone)]
pub struct CameraInfo {
    /// Camera name/identifier
    pub camera_name: String,
    /// Image width
    pub width: u32,
    /// Image height
    pub height: u32,
    /// K matrix (3x3 row-major): [fx, 0, cx, 0, fy, cy, 0, 0, 1]
    pub k: [f64; 9],
    /// D vector (distortion coefficients): [k1, k2, t1, t2, k3]
    pub d: Vec<f64>,
    /// R matrix (3x3 row-major rectification matrix)
    pub r: Option<[f64; 9]>,
    /// P matrix (3x4 row-major projection matrix)
    pub p: Option<[f64; 12]>,
    /// Distortion model name (e.g., "plumb_bob", "rational_polynomial")
    pub distortion_model: String,
}

/// A frame of data ready to be written to a dataset.
///
/// This is the primary input type for all sinks, providing a unified
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
    /// Image data by feature name -> (width, height, data)
    pub images: HashMap<String, ImageData>,
    /// Camera calibration info by camera name
    pub camera_info: HashMap<String, CameraInfo>,
    /// Additional data fields
    pub additional_data: HashMap<String, Vec<f32>>,
}

/// Image data with dimensions.
#[derive(Debug, Clone)]
pub struct ImageData {
    /// Width in pixels
    pub width: u32,
    /// Height in pixels
    pub height: u32,
    /// Raw image data (e.g., RGB, JPEG)
    pub data: Vec<u8>,
    /// Image format (e.g., "rgb8", "jpeg")
    pub format: ImageFormat,
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

/// Statistics from sink operations.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SinkStats {
    /// Total frames written
    pub frames_written: usize,
    /// Total episodes written
    pub episodes_written: usize,
    /// Processing time in seconds
    pub duration_sec: f64,
    /// Total data size in bytes (if known)
    pub total_bytes: Option<u64>,
    /// Additional sink-specific metrics
    pub metrics: HashMap<String, serde_json::Value>,
}

impl SinkStats {
    /// Create new sink stats.
    pub fn new() -> Self {
        Self {
            frames_written: 0,
            episodes_written: 0,
            duration_sec: 0.0,
            total_bytes: None,
            metrics: HashMap::new(),
        }
    }

    /// Add a metric.
    pub fn with_metric(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.metrics.insert(key.into(), value);
        self
    }
}

impl Default for SinkStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Checkpoint data for resumable writes.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SinkCheckpoint {
    /// Last frame index written
    pub last_frame_index: usize,
    /// Last episode index written
    pub last_episode_index: usize,
    /// Checkpoint timestamp
    pub checkpoint_time: i64,
    /// Additional checkpoint data
    pub data: HashMap<String, serde_json::Value>,
}

impl SinkCheckpoint {
    /// Create a new checkpoint.
    pub fn new(frame_index: usize, episode_index: usize) -> Self {
        Self {
            last_frame_index: frame_index,
            last_episode_index: episode_index,
            checkpoint_time: chrono::Utc::now().timestamp(),
            data: HashMap::new(),
        }
    }
}

/// Trait for writing robotics datasets to various formats.
///
/// Sinks provide a unified interface for writing data to different
/// file formats and storage systems. All sinks are async and support
/// streaming writes for memory efficiency.
///
/// # Example
///
/// ```rust,no_run
/// use roboflow_sinks::{Sink, SinkConfig, create_sink, DatasetFrame};
///
/// async fn write_to_lerobot() -> roboflow_sinks::SinkResult<()> {
///     let config = SinkConfig::lerobot("/path/to/output");
///     let mut sink = create_sink(&config)?;
///
///     sink.initialize(&config).await?;
///
///     let frame = DatasetFrame::new(0, 0, 0.0);
///     sink.write_frame(frame).await?;
///
///     let stats = sink.finalize().await?;
///     println!("Wrote {} frames", stats.frames_written);
///
///     Ok(())
/// }
/// ```
#[async_trait]
pub trait Sink: Send + Sync + 'static {
    /// Initialize the sink with the given configuration.
    ///
    /// This method is called once before any other operations. It should
    /// create the output directory/file, write metadata, and prepare for writing.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for this sink
    async fn initialize(&mut self, config: &SinkConfig) -> SinkResult<()>;

    /// Write a frame to the sink.
    ///
    /// Frames should be written in order (by frame_index, then episode_index).
    /// The sink may buffer frames for efficiency.
    ///
    /// # Arguments
    ///
    /// * `frame` - Frame to write
    async fn write_frame(&mut self, frame: DatasetFrame) -> SinkResult<()>;

    /// Flush any buffered data.
    ///
    /// This ensures all buffered data is written to storage.
    async fn flush(&mut self) -> SinkResult<()>;

    /// Finalize the sink and return statistics.
    ///
    /// This should flush any buffered data, close files, and return
    /// statistics about the write operation.
    async fn finalize(&mut self) -> SinkResult<SinkStats>;

    /// Get a checkpoint for the current write position.
    ///
    /// This can be used to resume writes after interruption.
    async fn checkpoint(&self) -> SinkResult<SinkCheckpoint>;

    /// Restore from a checkpoint.
    ///
    /// # Arguments
    ///
    /// * `checkpoint` - Checkpoint to restore from
    async fn restore(&mut self, checkpoint: &SinkCheckpoint) -> SinkResult<()> {
        let _ = checkpoint;
        Err(SinkError::RestoreNotSupported)
    }

    /// Check if the sink supports checkpointing.
    fn supports_checkpointing(&self) -> bool {
        false
    }

    /// Clone the sink.
    ///
    /// This is used when multiple writers need to share the same sink configuration.
    /// Not all sinks support cloning.
    fn box_clone(&self) -> SinkResult<Box<dyn Sink>> {
        Err(SinkError::CloneNotSupported)
    }
}

/// Factory function for creating sinks.
///
/// Each sink implementation should register a factory function
/// that creates a new instance of that sink.
pub type SinkFactory = Box<dyn Fn() -> Box<dyn Sink> + Send + Sync>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dataset_frame() {
        let frame = DatasetFrame::new(0, 0, 0.0)
            .with_observation_state(vec![1.0, 2.0, 3.0])
            .with_action(vec![0.5]);

        assert_eq!(frame.frame_index, 0);
        assert_eq!(frame.observation_state, Some(vec![1.0, 2.0, 3.0]));
        assert_eq!(frame.action, Some(vec![0.5]));
        assert!(frame.camera_info.is_empty());
    }

    #[test]
    fn test_sink_stats() {
        let stats = SinkStats::new().with_metric("test_metric", serde_json::json!(42));

        assert_eq!(stats.frames_written, 0);
        assert_eq!(
            stats.metrics.get("test_metric"),
            Some(&serde_json::json!(42))
        );
    }

    #[test]
    fn test_sink_checkpoint() {
        let checkpoint = SinkCheckpoint::new(10, 2);

        assert_eq!(checkpoint.last_frame_index, 10);
        assert_eq!(checkpoint.last_episode_index, 2);
    }
}
