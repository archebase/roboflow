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
    fn test_dataset_frame_with_images() {
        let image = ImageData {
            width: 640,
            height: 480,
            data: vec![0u8; 640 * 480 * 3],
            format: ImageFormat::Rgb8,
        };

        let frame = DatasetFrame::new(0, 0, 0.0).with_image("camera_left", image);

        assert_eq!(frame.images.len(), 1);
        assert!(frame.images.contains_key("camera_left"));
        let img = &frame.images["camera_left"];
        assert_eq!(img.width, 640);
        assert_eq!(img.height, 480);
        assert_eq!(img.format, ImageFormat::Rgb8);
    }

    #[test]
    fn test_dataset_frame_with_camera_info() {
        let camera_info = CameraInfo {
            camera_name: "left_camera".to_string(),
            width: 640,
            height: 480,
            k: [500.0, 0.0, 320.0, 0.0, 500.0, 240.0, 0.0, 0.0, 1.0],
            d: vec![0.1, 0.2, 0.0, 0.0, 0.0],
            r: None,
            p: None,
            distortion_model: "plumb_bob".to_string(),
        };

        let frame =
            DatasetFrame::new(0, 0, 0.0).with_camera_info("left_camera", camera_info.clone());

        assert_eq!(frame.camera_info.len(), 1);
        let info = &frame.camera_info["left_camera"];
        assert_eq!(info.width, 640);
        assert_eq!(info.distortion_model, "plumb_bob");
    }

    #[test]
    fn test_dataset_frame_with_task_index() {
        let mut frame = DatasetFrame::new(5, 1, 1.5);
        frame.task_index = Some(3);
        frame
            .additional_data
            .insert("custom".to_string(), vec![1.0]);

        assert_eq!(frame.frame_index, 5);
        assert_eq!(frame.episode_index, 1);
        assert_eq!(frame.timestamp, 1.5);
        assert_eq!(frame.task_index, Some(3));
        assert_eq!(frame.additional_data.get("custom"), Some(&vec![1.0]));
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
    fn test_sink_stats_default() {
        let stats = SinkStats::default();
        assert_eq!(stats.frames_written, 0);
        assert_eq!(stats.episodes_written, 0);
        assert_eq!(stats.duration_sec, 0.0);
        assert!(stats.total_bytes.is_none());
    }

    #[test]
    fn test_sink_stats_multiple_metrics() {
        let stats = SinkStats::new()
            .with_metric("frames_per_sec", serde_json::json!(30.0))
            .with_metric("bytes_written", serde_json::json!(1024))
            .with_metric("compression_ratio", serde_json::json!(0.85));

        assert_eq!(stats.metrics.len(), 3);
        assert_eq!(
            stats.metrics.get("frames_per_sec"),
            Some(&serde_json::json!(30.0))
        );
        assert_eq!(
            stats.metrics.get("bytes_written"),
            Some(&serde_json::json!(1024))
        );
    }

    #[test]
    fn test_sink_checkpoint() {
        let checkpoint = SinkCheckpoint::new(10, 2);

        assert_eq!(checkpoint.last_frame_index, 10);
        assert_eq!(checkpoint.last_episode_index, 2);
        assert!(checkpoint.data.is_empty());
    }

    #[test]
    fn test_sink_checkpoint_timestamp() {
        let before = chrono::Utc::now().timestamp();
        let checkpoint = SinkCheckpoint::new(0, 0);
        let after = chrono::Utc::now().timestamp();

        assert!(checkpoint.checkpoint_time >= before);
        assert!(checkpoint.checkpoint_time <= after);
    }

    #[test]
    fn test_camera_info_fields() {
        let info = CameraInfo {
            camera_name: "test_cam".to_string(),
            width: 1920,
            height: 1080,
            k: [1000.0, 0.0, 960.0, 0.0, 1000.0, 540.0, 0.0, 0.0, 1.0],
            d: vec![0.1, 0.2, 0.01, 0.02, 0.0],
            r: Some([1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0]),
            p: Some([
                1000.0, 0.0, 960.0, 0.0, 0.0, 1000.0, 540.0, 0.0, 0.0, 0.0, 1.0, 0.0,
            ]),
            distortion_model: "rational_polynomial".to_string(),
        };

        assert_eq!(info.width, 1920);
        assert_eq!(info.height, 1080);
        assert!(info.r.is_some());
        assert!(info.p.is_some());
        assert_eq!(info.d.len(), 5);
    }

    #[test]
    fn test_image_data_jpeg() {
        let image = ImageData {
            width: 640,
            height: 480,
            data: vec![0xFF, 0xD8, 0xFF], // JPEG header bytes
            format: ImageFormat::Jpeg,
        };

        assert_eq!(image.format, ImageFormat::Jpeg);
        assert_eq!(image.data.len(), 3);
    }

    #[test]
    fn test_dataset_frame_multiple_images() {
        let img1 = ImageData {
            width: 100,
            height: 100,
            data: vec![0u8; 30000],
            format: ImageFormat::Rgb8,
        };
        let img2 = ImageData {
            width: 200,
            height: 150,
            data: vec![0u8; 90000],
            format: ImageFormat::Rgb8,
        };

        let frame = DatasetFrame::new(0, 0, 0.0)
            .with_image("left", img1)
            .with_image("right", img2);

        assert_eq!(frame.images.len(), 2);
        assert!(frame.images.contains_key("left"));
        assert!(frame.images.contains_key("right"));
    }

    #[test]
    fn test_camera_info_with_projection_matrix() {
        // Test camera info with full P matrix (3x4 projection)
        let info = CameraInfo {
            camera_name: "stereo_left".to_string(),
            width: 1280,
            height: 720,
            k: [1000.0, 0.0, 640.0, 0.0, 1000.0, 360.0, 0.0, 0.0, 1.0],
            d: vec![0.0, 0.0, 0.0, 0.0, 0.0], // No distortion
            r: Some([1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0]),
            p: Some([
                1000.0, 0.0, 640.0, -100.0, // Baseline offset for stereo
                0.0, 1000.0, 360.0, 0.0, 0.0, 0.0, 1.0, 0.0,
            ]),
            distortion_model: "plumb_bob".to_string(),
        };

        assert!(info.r.is_some());
        assert!(info.p.is_some());
        let p = info.p.unwrap();
        assert_eq!(p.len(), 12); // 3x4 matrix
        assert_eq!(p[3], -100.0); // Baseline offset
    }

    #[test]
    fn test_sink_stats_serialization() {
        let stats = SinkStats::new()
            .with_metric("frames_per_sec", serde_json::json!(30.5))
            .with_metric("total_time", serde_json::json!(10.0));

        // Test serialization
        let json = serde_json::to_string(&stats).expect("Failed to serialize");
        assert!(json.contains("frames_written"));
        assert!(json.contains("frames_per_sec"));

        // Test deserialization
        let decoded: SinkStats = serde_json::from_str(&json).expect("Failed to deserialize");
        assert_eq!(decoded.frames_written, 0);
        assert_eq!(decoded.metrics.len(), 2);
    }

    #[test]
    fn test_sink_checkpoint_with_data() {
        let mut checkpoint = SinkCheckpoint::new(100, 5);
        checkpoint
            .data
            .insert("last_camera".to_string(), serde_json::json!("left"));
        checkpoint
            .data
            .insert("pending_uploads".to_string(), serde_json::json!(3));

        assert_eq!(checkpoint.last_frame_index, 100);
        assert_eq!(checkpoint.last_episode_index, 5);
        assert_eq!(checkpoint.data.len(), 2);
        assert_eq!(
            checkpoint.data.get("last_camera"),
            Some(&serde_json::json!("left"))
        );
        assert_eq!(
            checkpoint.data.get("pending_uploads"),
            Some(&serde_json::json!(3))
        );
    }

    #[test]
    fn test_dataset_frame_builder_chain() {
        // Test that builder methods can be chained
        let frame = DatasetFrame::new(10, 2, 1.5)
            .with_observation_state(vec![0.0, 1.0, 2.0])
            .with_action(vec![0.5, -0.5])
            .with_camera_info(
                "cam",
                CameraInfo {
                    camera_name: "cam".to_string(),
                    width: 640,
                    height: 480,
                    k: [1.0; 9],
                    d: vec![],
                    r: None,
                    p: None,
                    distortion_model: "none".to_string(),
                },
            );

        assert_eq!(frame.frame_index, 10);
        assert_eq!(frame.episode_index, 2);
        assert_eq!(frame.timestamp, 1.5);
        assert!(frame.observation_state.is_some());
        assert!(frame.action.is_some());
        assert!(frame.camera_info.contains_key("cam"));
    }
}
