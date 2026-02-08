// Types for the streaming dataset pipeline

use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::common::AlignedFrame;

/// Re-export robocodec's CodecValue for convenience
pub use robocodec::CodecValue;

/// A decoded message from the input file.
///
/// This wraps robocodec's TimestampedDecodedMessage for pipeline processing.
/// We use robocodec's streaming API directly: `RoboReader::open(path)?.decoded()`
/// which returns a lazy iterator of TimestampedDecodedMessage.
#[derive(Debug, Clone)]
pub struct DecodedMessage {
    /// Channel/topic name
    pub topic: String,
    /// Message type name
    pub message_type: String,
    /// Log timestamp (nanoseconds)
    pub log_time: u64,
    /// Sequence number
    pub sequence: Option<u64>,
    /// Decoded message data (using robocodec's CodecValue directly)
    pub data: CodecValue,
}

/// A frame ready for transformation.
#[derive(Debug, Clone)]
pub struct TransformableFrame {
    /// Frame index
    pub frame_index: usize,
    /// Timestamp (nanoseconds)
    pub timestamp: u64,
    /// Aligned data from multiple topics
    pub aligned_data: AlignedFrame,
}

/// A frame ready for dataset writing.
#[derive(Debug, Clone)]
pub struct DatasetFrame {
    /// Frame index within episode
    pub frame_index: usize,
    /// Episode index
    pub episode_index: usize,
    /// Timestamp (seconds)
    pub timestamp: f64,
    /// Observation state
    pub observation_state: Option<Vec<f32>>,
    /// Action data
    pub action: Option<Vec<f32>>,
    /// Task index
    pub task_index: Option<usize>,
    /// Image data by feature name -> (width, height, data)
    pub images: HashMap<String, (u32, u32, Vec<u8>)>,
}

impl DatasetFrame {
    /// Create a new dataset frame from aligned data
    pub fn from_aligned(
        frame_index: usize,
        episode_index: usize,
        timestamp_ns: u64,
        aligned: AlignedFrame,
    ) -> Self {
        let timestamp_sec = timestamp_ns as f64 / 1_000_000_000.0;

        // Convert images
        let images = aligned
            .images
            .into_iter()
            .map(|(k, v)| (k, (v.width, v.height, v.data)))
            .collect();

        Self {
            frame_index,
            episode_index,
            timestamp: timestamp_sec,
            observation_state: aligned.states.get("observation.state").cloned(),
            action: aligned.actions.get("action").cloned(),
            task_index: None,
            images,
        }
    }
}

/// Parquet row data ready for writing.
#[derive(Debug, Clone)]
pub struct ParquetRow {
    /// Episode index
    pub episode_index: usize,
    /// Frame index
    pub frame_index: usize,
    /// Timestamp (seconds)
    pub timestamp: f64,
    /// Observation state
    pub observation_state: Option<Vec<f32>>,
    /// Action
    pub action: Option<Vec<f32>>,
    /// Task index
    pub task_index: Option<usize>,
}

/// Encoded video file ready for upload.
#[derive(Debug, Clone)]
pub struct EncodedVideo {
    /// Episode index
    pub episode_index: usize,
    /// Camera/feature name
    pub camera_name: String,
    /// Local path to encoded MP4
    pub local_path: PathBuf,
    /// File size in bytes
    pub size: u64,
    /// Duration in seconds
    pub duration: f64,
}

/// Statistics for a pipeline stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageStats {
    /// Stage name
    pub stage: String,
    /// Number of items processed
    pub items_processed: usize,
    /// Number of items produced
    pub items_produced: usize,
    /// Processing time in seconds
    pub duration_sec: f64,
    /// Peak memory usage in MB (if tracked)
    pub peak_memory_mb: Option<f64>,
    /// Additional stage-specific metrics
    pub metrics: HashMap<String, serde_json::Value>,
}

impl StageStats {
    /// Create new stage stats
    pub fn new(stage: String) -> Self {
        Self {
            stage,
            items_processed: 0,
            items_produced: 0,
            duration_sec: 0.0,
            peak_memory_mb: None,
            metrics: HashMap::new(),
        }
    }

    /// Add a metric
    pub fn with_metric(
        mut self,
        key: impl Into<String>,
        value: impl Into<serde_json::Value>,
    ) -> Self {
        self.metrics.insert(key.into(), value.into());
        self
    }
}

/// Final pipeline report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineReport {
    /// Total frames written
    pub frames_written: usize,
    /// Total messages processed
    pub messages_processed: usize,
    /// Total duration in seconds
    pub duration_sec: f64,
    /// Throughput in frames per second
    pub throughput_fps: f64,
    /// Per-stage statistics
    pub stage_stats: Vec<StageStats>,
    /// Peak memory usage in MB
    pub peak_memory_mb: Option<f64>,
}

impl PipelineReport {
    /// Create a new empty report
    pub fn new() -> Self {
        Self {
            frames_written: 0,
            messages_processed: 0,
            duration_sec: 0.0,
            throughput_fps: 0.0,
            stage_stats: Vec::new(),
            peak_memory_mb: None,
        }
    }
}

impl Default for PipelineReport {
    fn default() -> Self {
        Self::new()
    }
}

/// Error type for pipeline operations.
#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    /// Stage initialization error
    #[error("Stage {stage} initialization failed: {reason}")]
    InitFailed { stage: String, reason: String },

    /// Stage execution error
    #[error("Stage {stage} execution failed: {reason}")]
    ExecutionFailed { stage: String, reason: String },

    /// Channel communication error
    #[error("Channel error between {from} and {to}: {reason}")]
    ChannelError {
        from: String,
        to: String,
        reason: String,
    },

    /// Timeout error
    #[error("Operation timed out after {timeout_sec}s")]
    Timeout { timeout_sec: u64 },

    /// Cancellation error
    #[error("Pipeline cancelled")]
    Cancelled,

    /// Other error
    #[error("Pipeline error: {0}")]
    Other(String),
}

impl From<PipelineError> for roboflow_core::RoboflowError {
    fn from(err: PipelineError) -> Self {
        roboflow_core::RoboflowError::other(err.to_string())
    }
}

/// Result type for pipeline operations.
pub type PipelineResult<T> = std::result::Result<T, PipelineError>;
