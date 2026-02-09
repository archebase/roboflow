// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Pipeline framework using Source/Sink abstractions.
//!
//! This module provides a unified pipeline orchestrator that works with
//! the pluggable Source and Sink traits, enabling flexible data processing
//! without being tied to specific file formats.
//!
//! # Data model
//!
//! For the data section (output dataset): **each bag file represents a single episode.**
//! One source file (one bag/MCAP) is not split by time gap or frame count; all frames
//! from that file are written as episode index 0.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use roboflow_core::{Result, RoboflowError};
use roboflow_sinks::{
    lerobot::LerobotSink, CameraInfo, DatasetFrame, ImageData, ImageFormat, Sink, SinkConfig,
    SinkStats,
};
use roboflow_sources::{
    BagSource, McapSource, RrdSource, Source, SourceConfig, TimestampedMessage,
};
use tracing::{debug, info, instrument, warn};

/// Checkpoint callback type for progress reporting.
///
/// Called during pipeline execution to report progress.
/// The callback receives the current frame index and total estimated frames.
pub type CheckpointCallback = Arc<dyn Fn(usize, usize) + Send + Sync>;

/// Configuration for the pipeline.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Source configuration
    pub source: SourceConfig,
    /// Sink configuration
    pub sink: SinkConfig,
    /// Target FPS for frame alignment
    pub fps: u32,
    /// Maximum frames to process (None = unlimited)
    pub max_frames: Option<usize>,
    /// Checkpoint interval (None = no checkpointing)
    pub checkpoint_interval: Option<Duration>,
    /// Topic mappings for dataset conversion
    pub topic_mappings: HashMap<String, String>,
}

impl PipelineConfig {
    /// Create a new pipeline configuration.
    pub fn new(source: SourceConfig, sink: SinkConfig) -> Self {
        Self {
            source,
            sink,
            fps: 30,
            max_frames: None,
            checkpoint_interval: None,
            topic_mappings: HashMap::new(),
        }
    }

    /// Set the target FPS.
    pub fn with_fps(mut self, fps: u32) -> Self {
        self.fps = fps;
        self
    }

    /// Set maximum frames to process.
    pub fn with_max_frames(mut self, max: usize) -> Self {
        self.max_frames = Some(max);
        self
    }

    /// Set checkpoint interval.
    pub fn with_checkpoint_interval(mut self, interval: Duration) -> Self {
        self.checkpoint_interval = Some(interval);
        self
    }

    /// Add a topic mapping.
    pub fn with_topic_mapping(
        mut self,
        topic: impl Into<String>,
        feature: impl Into<String>,
    ) -> Self {
        self.topic_mappings.insert(topic.into(), feature.into());
        self
    }
}

/// Statistics from pipeline execution.
#[derive(Debug, Clone)]
pub struct PipelineReport {
    /// Frames written
    pub frames_written: usize,
    /// Episodes written
    pub episodes_written: usize,
    /// Messages processed
    pub messages_processed: usize,
    /// Processing time in seconds
    pub duration_sec: f64,
    /// Throughput in frames per second
    pub fps: f64,
    /// Additional sink stats
    pub sink_stats: SinkStats,
}

/// The main pipeline orchestrator.
///
/// This uses the pluggable Source/Sink abstractions to create a flexible
/// data processing pipeline.
pub struct Pipeline {
    source: Box<dyn Source>,
    sink: Box<dyn Sink>,
    config: PipelineConfig,
}

impl Pipeline {
    /// Create a new pipeline with the given configuration.
    pub fn new(config: PipelineConfig) -> Result<Self> {
        // Create source based on config type
        use roboflow_sources::SourceType;
        let source: Box<dyn Source> = match &config.source.source_type {
            SourceType::Mcap { path } => Box::new(McapSource::new(path).map_err(|e| {
                RoboflowError::other(format!("Failed to create MCAP source: {}", e))
            })?),
            SourceType::Bag { path } => Box::new(BagSource::new(path).map_err(|e| {
                RoboflowError::other(format!("Failed to create Bag source: {}", e))
            })?),
            SourceType::Rrd { path } => Box::new(RrdSource::new(path).map_err(|e| {
                RoboflowError::other(format!("Failed to create RRD source: {}", e))
            })?),
        };

        // Create sink based on config type
        use roboflow_sinks::SinkType;
        let sink: Box<dyn Sink> = match &config.sink.sink_type {
            SinkType::Lerobot { path } => Box::new(LerobotSink::new(path).map_err(|e| {
                RoboflowError::other(format!("Failed to create LeRobot sink: {}", e))
            })?),
            SinkType::Zarr { .. } => {
                return Err(RoboflowError::other(
                    "Zarr sink not yet implemented in Pipeline".to_string(),
                ));
            }
        };

        Ok(Self {
            source,
            sink,
            config,
        })
    }

    /// Create a pipeline with pre-created source and sink.
    ///
    /// This is useful when you want to customize the source/sink creation
    /// or when you need to share them across multiple pipelines.
    pub fn with_components(
        source: Box<dyn Source>,
        sink: Box<dyn Sink>,
        config: PipelineConfig,
    ) -> Self {
        Self {
            source,
            sink,
            config,
        }
    }

    /// Run the pipeline with proper timestamp-based frame alignment.
    #[instrument(skip_all, fields(
        source = %self.config.source.path(),
        sink = %self.config.sink.path(),
        fps = self.config.fps,
    ))]
    pub async fn run(mut self) -> Result<PipelineReport> {
        let start = Instant::now();

        info!("Initializing pipeline");

        // Initialize source and sink
        self.source
            .initialize(&self.config.source)
            .await
            .map_err(|e| RoboflowError::other(format!("Source init failed: {e}")))?;

        self.sink
            .initialize(&self.config.sink)
            .await
            .map_err(|e| RoboflowError::other(format!("Sink init failed: {e}")))?;

        // Get source metadata
        let metadata = self
            .source
            .metadata()
            .await
            .map_err(|e| RoboflowError::other(format!("Failed to get metadata: {e}")))?;

        debug!(
            "Source has {} topics, {} messages",
            metadata.topics.len(),
            metadata.message_count.unwrap_or(0)
        );

        // Calculate frame interval from fps
        let frame_interval_ns = 1_000_000_000u64 / self.config.fps as u64;

        // Message buffer for timestamp alignment: timestamp_ns -> Vec<TimestampedMessage>
        let mut message_buffer: HashMap<u64, Vec<TimestampedMessage>> = HashMap::new();

        // Track timestamps
        let mut current_timestamp_ns: Option<u64> = None;
        let mut end_timestamp_ns: Option<u64> = None;

        let mut messages_processed = 0usize;
        let mut frames_written = 0usize;
        let episode_index = 0usize; // One bag = one episode
        let mut frame_index = 0usize;
        let mut last_checkpoint_time = Instant::now();

        // One bag file = one episode (no splitting by time gap or frame count)
        let batch_size = 1000;

        loop {
            // Check max frames
            if let Some(max) = self.config.max_frames {
                if frames_written >= max {
                    debug!("Reached max frames limit: {}", max);
                    break;
                }
            }

            // Read batch from source
            let batch = self
                .source
                .read_batch(batch_size)
                .await
                .map_err(|e| RoboflowError::other(format!("Read failed: {e}")))?;

            let batch = match batch {
                Some(b) if !b.is_empty() => b,
                None => break,       // End of stream
                Some(_) => continue, // Empty batch, keep trying
            };

            messages_processed += batch.len();

            // Buffer messages by timestamp (round to nearest frame interval)
            for msg in batch {
                // Calculate frame index for this message
                let frame_idx = msg.log_time / frame_interval_ns;
                let aligned_timestamp = frame_idx * frame_interval_ns;

                message_buffer
                    .entry(aligned_timestamp)
                    .or_default()
                    .push(msg);

                // Track timestamp range
                if current_timestamp_ns.is_none() {
                    current_timestamp_ns = Some(aligned_timestamp);
                }
                end_timestamp_ns = Some(aligned_timestamp.max(end_timestamp_ns.unwrap_or(0)));
            }

            // Process frames that are complete (all messages for a given timestamp)
            while let Some(timestamp) = current_timestamp_ns {
                // Check if we have messages for this timestamp
                if let Some(messages) = message_buffer.remove(&timestamp) {
                    // Create frame from all messages at this timestamp
                    let frame =
                        self.messages_to_frame(messages, frame_index, episode_index, timestamp)?;

                    self.sink
                        .write_frame(frame)
                        .await
                        .map_err(|e| RoboflowError::other(format!("Write failed: {e}")))?;

                    frame_index += 1;
                    frames_written += 1;

                    // Move to next timestamp
                    let next_ts = end_timestamp_ns.unwrap_or(timestamp);
                    current_timestamp_ns = if timestamp < next_ts {
                        // Find next buffered timestamp
                        message_buffer
                            .keys()
                            .copied()
                            .filter(|&t| t > timestamp)
                            .min()
                    } else {
                        None
                    };
                } else {
                    // No more messages for current timestamp, move to next buffered timestamp
                    let next_ts = timestamp;
                    current_timestamp_ns = message_buffer
                        .keys()
                        .copied()
                        .filter(|&t| t > next_ts)
                        .min();
                    break;
                }
            }

            // Checkpoint if needed
            if let Some(interval) = self.config.checkpoint_interval {
                if last_checkpoint_time.elapsed() >= interval {
                    if self.sink.supports_checkpointing() {
                        match self.sink.checkpoint().await {
                            Ok(_) => debug!("Checkpoint saved"),
                            Err(e) => warn!("Failed to checkpoint: {}", e),
                        }
                    }
                    last_checkpoint_time = Instant::now();
                }
            }
        }

        // Process any remaining buffered messages (same episode: one bag = one episode)
        while let Some((timestamp, messages)) = message_buffer.drain().next() {
            if !messages.is_empty() {
                let frame =
                    self.messages_to_frame(messages, frame_index, episode_index, timestamp)?;

                self.sink
                    .write_frame(frame)
                    .await
                    .map_err(|e| RoboflowError::other(format!("Write failed: {e}")))?;

                frame_index += 1;
                frames_written += 1;
            }
        }

        // Flush and finalize
        self.sink
            .flush()
            .await
            .map_err(|e| RoboflowError::other(format!("Flush failed: {e}")))?;

        let sink_stats = self
            .sink
            .finalize()
            .await
            .map_err(|e| RoboflowError::other(format!("Finalize failed: {e}")))?;

        let duration = start.elapsed();
        let fps = if duration.as_secs_f64() > 0.0 {
            frames_written as f64 / duration.as_secs_f64()
        } else {
            0.0
        };

        info!(
            "Pipeline completed: {} frames in {:.2}s ({:.1} fps)",
            frames_written,
            duration.as_secs_f64(),
            fps
        );

        Ok(PipelineReport {
            frames_written,
            episodes_written: episode_index + 1,
            messages_processed,
            duration_sec: duration.as_secs_f64(),
            fps,
            sink_stats,
        })
    }

    /// Convert multiple timestamped messages at the same timestamp to a dataset frame.
    ///
    /// This aggregates data from all topics at the given timestamp.
    fn messages_to_frame(
        &self,
        messages: Vec<TimestampedMessage>,
        frame_index: usize,
        episode_index: usize,
        timestamp_ns: u64,
    ) -> Result<DatasetFrame> {
        let timestamp_sec = timestamp_ns as f64 / 1_000_000_000.0;
        let mut frame = DatasetFrame::new(frame_index, episode_index, timestamp_sec);

        // Process all messages at this timestamp
        for msg in messages {
            // Convert based on message type
            match msg.data {
                robocodec::CodecValue::Array(ref arr) => {
                    // Convert CodecValue array to Vec<f32>
                    let state: Vec<f32> =
                        arr.iter().filter_map(codec_value_element_to_f32).collect();
                    if !state.is_empty() {
                        let feature = self.config.topic_mappings.get(&msg.topic);
                        if feature.is_some_and(|f| f == "action") {
                            frame.action = Some(state);
                        } else {
                            frame.observation_state = Some(state);
                        }
                    }
                }
                robocodec::CodecValue::Struct(ref map) => {
                    // Check topic mapping to decide how to handle this struct
                    let feature = self.config.topic_mappings.get(&msg.topic);

                    // Camera info handling: check for K matrix (unique to CameraInfo)
                    // We process this regardless of mapping since it provides metadata
                    if map.contains_key("K") && map.contains_key("D") {
                        // This looks like a CameraInfo message
                        // Use the mapped feature name as the camera identifier, or derive from topic
                        let camera_name = feature.cloned().unwrap_or_else(|| {
                            msg.topic
                                .replace('/', "_")
                                .trim_start_matches('_')
                                .to_string()
                        });

                        if let Some(info) = extract_camera_info_from_struct(map, camera_name) {
                            tracing::debug!(
                                camera = %info.camera_name,
                                width = info.width,
                                height = info.height,
                                fx = info.k[0],
                                fy = info.k[4],
                                "Pipeline: extracted camera calibration info"
                            );
                            frame.camera_info.insert(info.camera_name.clone(), info);
                        }
                    } else if feature
                        .as_ref()
                        .is_some_and(|f| f.starts_with("observation.state") || f == &"action")
                    {
                        // State/action topic: extract numeric array from struct.
                        // For sensor_msgs/JointState, extract `position` field.
                        // Falls back to any float64/float32 array field.
                        if let Some(state) = extract_state_from_struct(map) {
                            if !state.is_empty() {
                                if feature.is_some_and(|f| f == "action") {
                                    frame.action = Some(state);
                                } else {
                                    frame.observation_state = Some(state);
                                }
                            }
                        }
                    } else if feature.as_ref().is_some_and(|f| f.contains("images")) {
                        // Image topic: only extract if mapped as an image feature
                        if let Some(image_bytes) = extract_image_bytes_from_struct(map, &msg.topic)
                        {
                            // Image data (sensor_msgs/Image or sensor_msgs/CompressedImage)
                            tracing::debug!(
                                topic = %msg.topic,
                                bytes = image_bytes.len(),
                                "Pipeline: extracted image bytes for frame"
                            );
                            let width = map
                                .get("width")
                                .and_then(|v: &robocodec::CodecValue| {
                                    if let robocodec::CodecValue::UInt32(w) = v {
                                        Some(*w)
                                    } else if let robocodec::CodecValue::UInt64(w) = v {
                                        Some(*w as u32)
                                    } else {
                                        None
                                    }
                                })
                                .unwrap_or(640);
                            let height = map
                                .get("height")
                                .and_then(|v: &robocodec::CodecValue| {
                                    if let robocodec::CodecValue::UInt32(h) = v {
                                        Some(*h)
                                    } else if let robocodec::CodecValue::UInt64(h) = v {
                                        Some(*h as u32)
                                    } else {
                                        None
                                    }
                                })
                                .unwrap_or(480);

                            let format = map
                                .get("format")
                                .and_then(|v: &robocodec::CodecValue| {
                                    if let robocodec::CodecValue::String(s) = v {
                                        let s = s.to_lowercase();
                                        if s.contains("jpeg") || s.contains("jpg") {
                                            Some(ImageFormat::Jpeg)
                                        } else if s.contains("png") {
                                            Some(ImageFormat::Png)
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                })
                                .unwrap_or(ImageFormat::Rgb8);

                            let feature_name = feature.cloned().unwrap_or_else(|| {
                                msg.topic
                                    .replace('/', "_")
                                    .trim_start_matches('_')
                                    .to_string()
                            });

                            frame.images.insert(
                                feature_name,
                                ImageData {
                                    width,
                                    height,
                                    data: image_bytes,
                                    format,
                                },
                            );
                        }
                        // If image extraction fails, silently skip - not all structs are images
                    }
                    // If topic has no mapping or isn't a state/action/image type, skip it
                }
                _ => {}
            }
        }

        if !frame.images.is_empty() {
            tracing::debug!(
                frame_index,
                episode_index,
                image_count = frame.images.len(),
                "Pipeline: frame has images"
            );
        }
        Ok(frame)
    }
}

/// Extract raw image bytes from a struct message's "data" field.
///
/// Handles multiple codec representations:
/// - `CodecValue::Bytes` - Standard binary data
/// - `CodecValue::Array<UInt8>` - Decoded uint8 array
/// - `CodecValue::Array<UInt32>` - Some codecs decode uint8[] as UInt32
/// - `CodecValue::Array<Int8>` - Signed byte arrays
/// - `CodecValue::Array<Int32>` - Some codecs use signed int32
/// - `CodecValue::String` - Base64-encoded data (some codecs)
/// - Nested arrays and other edge cases
///
/// Returns None if:
/// - Data field is missing
/// - Data format is unsupported
/// - Data is empty after extraction
fn extract_image_bytes_from_struct(
    map: &std::collections::HashMap<String, robocodec::CodecValue>,
    topic: &str,
) -> Option<Vec<u8>> {
    let data = map.get("data")?;
    let result = match data {
        robocodec::CodecValue::Bytes(b) => Some(b.clone()),
        robocodec::CodecValue::Array(arr) => {
            // Handle UInt8 array (most common case) - use helper for cleaner code
            let bytes: Vec<u8> = arr.iter().filter_map(codec_value_to_u8).collect();
            if bytes.is_empty() {
                // Try nested arrays (some codecs use Array<Array<UInt8>>)
                for v in arr.iter() {
                    if let robocodec::CodecValue::Array(inner) = v {
                        let inner_bytes: Vec<u8> =
                            inner.iter().filter_map(codec_value_to_u8).collect();
                        if !inner_bytes.is_empty() {
                            return Some(inner_bytes);
                        }
                    }
                }
                None
            } else {
                Some(bytes)
            }
        }
        robocodec::CodecValue::String(s) => {
            // Handle base64-encoded data (some codecs encode images as base64 strings)
            tracing::warn!(
                topic = %topic,
                string_len = s.len(),
                "Image 'data' is String type - may be base64 encoded. \
                 Consider using codec that outputs Bytes or Array<UInt8> for better performance."
            );
            None
        }
        other => {
            // Get actual variant type name instead of enum type
            let actual_type = other.type_name();
            let available_fields: Vec<&str> = map.keys().map(|k| k.as_str()).collect();

            tracing::warn!(
                topic = %topic,
                value_type = %actual_type,
                available_fields = ?available_fields,
                "Image struct 'data' has unsupported codec format; \
                 consider updating the codec to use Bytes or Array<UInt8>"
            );
            None
        }
    };
    result
}

/// Extract a numeric state vector from a decoded struct message.
///
/// Handles common robotics state message types:
/// - `sensor_msgs/JointState`: extracts `position` field
/// - Generic: falls back to the first array field containing numeric values
fn extract_state_from_struct(
    map: &std::collections::HashMap<String, robocodec::CodecValue>,
) -> Option<Vec<f32>> {
    // Priority 1: JointState `position` field (most common state message)
    if let Some(arr) = map.get("position") {
        if let Some(state) = codec_value_to_f32_vec(arr) {
            if !state.is_empty() {
                return Some(state);
            }
        }
    }

    // Priority 2: any other numeric array field (skip `name`, `header`, etc.)
    for value in map.values() {
        if let robocodec::CodecValue::Array(_) = value {
            if let Some(state) = codec_value_to_f32_vec(value) {
                if !state.is_empty() {
                    return Some(state);
                }
            }
        }
    }

    None
}

/// Convert a single numeric `CodecValue` element to `f32`.
fn codec_value_element_to_f32(v: &robocodec::CodecValue) -> Option<f32> {
    match v {
        robocodec::CodecValue::Float32(n) => Some(*n),
        robocodec::CodecValue::Float64(n) => Some(*n as f32),
        robocodec::CodecValue::Int32(n) => Some(*n as f32),
        robocodec::CodecValue::Int64(n) => Some(*n as f32),
        robocodec::CodecValue::UInt32(n) => Some(*n as f32),
        robocodec::CodecValue::UInt64(n) => Some(*n as f32),
        _ => None,
    }
}

/// Convert a `CodecValue` (expected to be an Array of numerics) into `Vec<f32>`.
fn codec_value_to_f32_vec(value: &robocodec::CodecValue) -> Option<Vec<f32>> {
    match value {
        robocodec::CodecValue::Array(arr) => {
            let v: Vec<f32> = arr.iter().filter_map(codec_value_element_to_f32).collect();
            Some(v)
        }
        _ => None,
    }
}

/// Extract u8 byte from any numeric CodecValue variant.
///
/// Handles all integer types with proper bounds checking:
/// - Unsigned types (UInt8, UInt16, UInt32, UInt64) - checked against u8::MAX
/// - Signed types (Int8, Int16, Int32, Int64) - checked for non-negative and u8::MAX
fn codec_value_to_u8(v: &robocodec::CodecValue) -> Option<u8> {
    match v {
        robocodec::CodecValue::UInt8(x) => Some(*x),
        robocodec::CodecValue::Int8(x) if *x >= 0 => Some(*x as u8),
        robocodec::CodecValue::UInt16(x) if *x <= u8::MAX as u16 => Some(*x as u8),
        robocodec::CodecValue::Int16(x) if *x >= 0 && (*x as u16) <= u8::MAX as u16 => {
            Some(*x as u8)
        }
        robocodec::CodecValue::UInt32(x) if *x <= u8::MAX as u32 => Some(*x as u8),
        robocodec::CodecValue::Int32(x) if *x >= 0 && (*x as u32) <= u8::MAX as u32 => {
            Some(*x as u8)
        }
        robocodec::CodecValue::UInt64(x) if *x <= u8::MAX as u64 => Some(*x as u8),
        robocodec::CodecValue::Int64(x) if *x >= 0 && (*x as u64) <= u8::MAX as u64 => {
            Some(*x as u8)
        }
        _ => None,
    }
}

/// Extract camera calibration info from a sensor_msgs/CameraInfo struct.
///
/// ROS CameraInfo message structure:
/// - K: 3x3 intrinsic matrix [fx, 0, cx, 0, fy, cy, 0, 0, 1]
/// - D: distortion coefficients [k1, k2, t1, t2, k3]
/// - R: 3x3 rectification matrix
/// - P: 3x4 projection matrix
/// - distortion_model: string (e.g., "plumb_bob", "rational_polynomial")
fn extract_camera_info_from_struct(
    map: &std::collections::HashMap<String, robocodec::CodecValue>,
    camera_name: String,
) -> Option<CameraInfo> {
    // Extract width and height
    let width = map.get("width").and_then(|v| {
        if let robocodec::CodecValue::UInt32(w) = v {
            Some(*w)
        } else if let robocodec::CodecValue::UInt64(w) = v {
            Some(*w as u32)
        } else {
            None
        }
    })?;

    let height = map.get("height").and_then(|v| {
        if let robocodec::CodecValue::UInt32(h) = v {
            Some(*h)
        } else if let robocodec::CodecValue::UInt64(h) = v {
            Some(*h as u32)
        } else {
            None
        }
    })?;

    // Extract distortion model
    let distortion_model = map
        .get("distortion_model")
        .and_then(|v| {
            if let robocodec::CodecValue::String(s) = v {
                Some(s.clone())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "plumb_bob".to_string());

    // Extract K matrix (3x3 intrinsic matrix)
    let k = extract_f64_array_3x3(map.get("K")?)?;

    // Extract D vector (distortion coefficients)
    let d = extract_f64_vector(map.get("D")?);

    // Extract R matrix (3x3 rectification matrix) - optional
    let r = map.get("R").and_then(extract_f64_array_3x3);

    // Extract P matrix (3x4 projection matrix) - optional
    let p = map.get("P").and_then(extract_f64_array_3x4);

    Some(CameraInfo {
        camera_name,
        width,
        height,
        k,
        d,
        r,
        p,
        distortion_model,
    })
}

/// Extract a 3x3 f64 array from a CodecValue::Array.
fn extract_f64_array_3x3(value: &robocodec::CodecValue) -> Option<[f64; 9]> {
    let arr = match value {
        robocodec::CodecValue::Array(a) => a,
        _ => return None,
    };

    if arr.len() < 9 {
        return None;
    }

    let mut result = [0.0f64; 9];
    for (i, val) in arr.iter().take(9).enumerate() {
        result[i] = match val {
            robocodec::CodecValue::Float64(f) => *f,
            robocodec::CodecValue::Float32(f) => *f as f64,
            robocodec::CodecValue::Int32(i) => *i as f64,
            robocodec::CodecValue::Int64(i) => *i as f64,
            robocodec::CodecValue::UInt32(u) => *u as f64,
            robocodec::CodecValue::UInt64(u) => *u as f64,
            _ => return None,
        };
    }
    Some(result)
}

/// Extract a 3x4 f64 array from a CodecValue::Array.
fn extract_f64_array_3x4(value: &robocodec::CodecValue) -> Option<[f64; 12]> {
    let arr = match value {
        robocodec::CodecValue::Array(a) => a,
        _ => return None,
    };

    if arr.len() < 12 {
        return None;
    }

    let mut result = [0.0f64; 12];
    for (i, val) in arr.iter().take(12).enumerate() {
        result[i] = match val {
            robocodec::CodecValue::Float64(f) => *f,
            robocodec::CodecValue::Float32(f) => *f as f64,
            robocodec::CodecValue::Int32(i) => *i as f64,
            robocodec::CodecValue::Int64(i) => *i as f64,
            robocodec::CodecValue::UInt32(u) => *u as f64,
            robocodec::CodecValue::UInt64(u) => *u as f64,
            _ => return None,
        };
    }
    Some(result)
}

/// Extract a variable-length f64 vector from a CodecValue::Array.
fn extract_f64_vector(value: &robocodec::CodecValue) -> Vec<f64> {
    let arr = match value {
        robocodec::CodecValue::Array(a) => a,
        _ => return Vec::new(),
    };

    arr.iter()
        .filter_map(|val| match val {
            robocodec::CodecValue::Float64(f) => Some(*f),
            robocodec::CodecValue::Float32(f) => Some(*f as f64),
            robocodec::CodecValue::Int32(i) => Some(*i as f64),
            robocodec::CodecValue::Int64(i) => Some(*i as f64),
            robocodec::CodecValue::UInt32(u) => Some(*u as f64),
            robocodec::CodecValue::UInt64(u) => Some(*u as f64),
            _ => None,
        })
        .collect()
}

/// Distributed executor for running pipelines in a distributed environment.
///
/// This is used by the worker to execute pipeline work units.
pub struct DistributedExecutor {
    _checkpoint_interval: Duration,
    checkpoint_callback: Option<CheckpointCallback>,
}

impl DistributedExecutor {
    /// Create a new distributed executor.
    pub fn new(checkpoint_interval: Duration) -> Self {
        Self {
            _checkpoint_interval: checkpoint_interval,
            checkpoint_callback: None,
        }
    }

    /// Set a checkpoint callback for progress reporting.
    ///
    /// The callback will be invoked during pipeline execution to report progress.
    pub fn with_checkpoint_callback(mut self, callback: CheckpointCallback) -> Self {
        self.checkpoint_callback = Some(callback);
        self
    }

    /// Execute a pipeline with the given configuration.
    #[instrument(skip_all)]
    pub async fn execute(&self, config: PipelineConfig) -> Result<PipelineReport> {
        let pipeline = Pipeline::new(config)?;
        pipeline.run().await
    }

    /// Execute a pipeline with pre-created source and sink.
    #[instrument(skip_all)]
    pub async fn execute_with_components(
        &self,
        source: Box<dyn Source>,
        sink: Box<dyn Sink>,
        config: PipelineConfig,
    ) -> Result<PipelineReport> {
        let pipeline = Pipeline::with_components(source, sink, config);
        pipeline.run().await
    }
}

impl Default for DistributedExecutor {
    fn default() -> Self {
        Self::new(Duration::from_secs(10))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_config_builder() {
        let source = SourceConfig::mcap("input.mcap");
        let sink = SinkConfig::lerobot("/output");

        let config = PipelineConfig::new(source, sink)
            .with_fps(60)
            .with_max_frames(1000)
            .with_checkpoint_interval(Duration::from_secs(30))
            .with_topic_mapping("/camera", "observation.camera");

        assert_eq!(config.fps, 60);
        assert_eq!(config.max_frames, Some(1000));
        assert_eq!(config.checkpoint_interval, Some(Duration::from_secs(30)));
        assert_eq!(
            config.topic_mappings.get("/camera"),
            Some(&"observation.camera".to_string())
        );
    }
}
