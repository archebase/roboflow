// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Unified pipeline executor for dataset writing.
//!
//! This module provides a streamlined pipeline orchestration that works
//! directly with `TimestampedMessage` from sources and `DatasetWriter`
//! for output. It replaces the multi-layer abstraction of
//! `roboflow-pipeline/framework.rs` + `roboflow-sinks` with a single,
//! focused executor.
//!
//! # Architecture
//!
//! ```text
//! Source (MCAP) -> PipelineExecutor -> DatasetWriter
//!   TimestampedMsg    Frame alignment    (LeRobotWriter)
//!                     Episode tracking
//!                     Message aggregation
//! ```

use std::collections::HashMap;
use std::time::{Duration, Instant};

use roboflow_core::{Result, RoboflowError};
use roboflow_sources::TimestampedMessage;
use tracing::{debug, info, instrument, warn};

use crate::common::base::{AlignedFrame, DatasetWriter, ImageData};
use crate::streaming::config::StreamingConfig;

/// Configuration for the pipeline executor.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Streaming configuration for frame alignment
    pub streaming: StreamingConfig,
    /// Maximum frames to process (None = unlimited)
    pub max_frames: Option<usize>,
    /// Checkpoint interval (None = no checkpointing)
    pub checkpoint_interval: Option<Duration>,
    /// Topic mappings for dataset conversion (topic -> feature name)
    pub topic_mappings: HashMap<String, String>,
}

impl PipelineConfig {
    /// Create a new pipeline configuration.
    pub fn new(streaming: StreamingConfig) -> Self {
        Self {
            streaming,
            max_frames: None,
            checkpoint_interval: None,
            topic_mappings: HashMap::new(),
        }
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

    /// Add multiple topic mappings at once.
    pub fn with_topic_mappings(mut self, mappings: HashMap<String, String>) -> Self {
        self.topic_mappings = mappings;
        self
    }
}

/// Statistics from pipeline execution.
#[derive(Debug, Clone)]
pub struct PipelineStats {
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
}

/// Unified pipeline executor for dataset writing.
///
/// This executor processes `TimestampedMessage` directly and uses
/// `StreamingConfig` for frame alignment, producing `AlignedFrame`
/// for the `DatasetWriter`.
///
/// # Example
///
/// ```rust,ignore
/// use roboflow_dataset::{PipelineExecutor, PipelineConfig};
/// use roboflow_dataset::lerobot::LerobotWriter;
/// use roboflow_dataset::streaming::config::StreamingConfig;
///
/// let streaming_config = StreamingConfig::with_fps(30);
/// let pipeline_config = PipelineConfig::new(streaming_config);
///
/// let writer = LerobotWriter::new_local("/output", lerobot_config)?;
/// let mut executor = PipelineExecutor::new(writer, pipeline_config);
///
/// // Process messages from source
/// for msg in source {
///     executor.process_message(msg)?;
/// }
///
/// let stats = executor.finalize()?;
/// ```
pub struct PipelineExecutor<W: DatasetWriter> {
    writer: W,
    config: PipelineConfig,
    stats: ExecutorStats,
    state: ExecutorState,
}

#[derive(Debug, Default)]
struct ExecutorStats {
    messages_processed: usize,
    frames_written: usize,
    episodes_written: usize,
}

#[derive(Debug)]
struct ExecutorState {
    /// Message buffer: timestamp_ns -> Vec<TimestampedMessage>
    message_buffer: HashMap<u64, Vec<TimestampedMessage>>,
    /// Current timestamp being processed
    current_timestamp_ns: Option<u64>,
    /// End timestamp of buffered data
    end_timestamp_ns: Option<u64>,
    /// Current episode index
    episode_index: usize,
    /// Current frame index within episode
    frame_index: usize,
    /// Start time
    start_time: Instant,
}

impl<W: DatasetWriter> PipelineExecutor<W> {
    /// Create a new pipeline executor.
    pub fn new(writer: W, config: PipelineConfig) -> Self {
        Self {
            writer,
            config,
            stats: ExecutorStats::default(),
            state: ExecutorState {
                message_buffer: HashMap::new(),
                current_timestamp_ns: None,
                end_timestamp_ns: None,
                episode_index: 0,
                frame_index: 0,
                start_time: Instant::now(),
            },
        }
    }

    /// Process a single timestamped message.
    ///
    /// Messages are buffered by timestamp and processed in order.
    /// When a frame is complete (all messages for that timestamp),
    /// it is written to the underlying writer.
    #[instrument(skip_all, fields(
        topic = %msg.topic,
        log_time = msg.log_time,
    ))]
    pub fn process_message(&mut self, msg: TimestampedMessage) -> Result<()> {
        self.stats.messages_processed += 1;

        // Check max frames limit
        if let Some(max) = self.config.max_frames
            && self.stats.frames_written >= max
        {
            return Ok(());
        }

        // Calculate frame index for this message
        let frame_interval_ns = self.config.streaming.frame_interval_ns();
        let frame_idx = msg.log_time / frame_interval_ns;
        let aligned_timestamp = frame_idx * frame_interval_ns;

        // Buffer message by timestamp
        self.state
            .message_buffer
            .entry(aligned_timestamp)
            .or_default()
            .push(msg);

        // Track timestamp range
        if self.state.current_timestamp_ns.is_none() {
            self.state.current_timestamp_ns = Some(aligned_timestamp);
        }
        self.state.end_timestamp_ns =
            Some(aligned_timestamp.max(self.state.end_timestamp_ns.unwrap_or(0)));

        // Process complete frames
        self.process_complete_frames()?;

        Ok(())
    }

    /// Process any remaining buffered messages and finalize the output.
    ///
    /// This must be called after all messages have been processed.
    /// It flushes remaining buffered frames and calls the underlying
    /// writer's finalize method.
    #[instrument(skip_all)]
    pub fn finalize(mut self) -> Result<PipelineStats> {
        info!(
            messages = self.stats.messages_processed,
            buffered_frames = self.state.message_buffer.len(),
            "Finalizing pipeline"
        );

        // Process any remaining buffered messages
        self.flush_remaining_frames()?;

        // Finalize the writer
        self.writer
            .finalize()
            .map_err(|e| RoboflowError::other(format!("Writer finalize failed: {}", e)))?;

        let duration = self.state.start_time.elapsed();
        let fps = if duration.as_secs_f64() > 0.0 {
            self.stats.frames_written as f64 / duration.as_secs_f64()
        } else {
            0.0
        };

        info!(
            frames = self.stats.frames_written,
            episodes = self.stats.episodes_written,
            messages = self.stats.messages_processed,
            duration_sec = duration.as_secs_f64(),
            fps,
            "Pipeline completed"
        );

        Ok(PipelineStats {
            frames_written: self.stats.frames_written,
            episodes_written: self.stats.episodes_written,
            messages_processed: self.stats.messages_processed,
            duration_sec: duration.as_secs_f64(),
            fps,
        })
    }

    /// Get mutable reference to the underlying writer.
    ///
    /// This allows direct access to writer methods like
    /// `set_camera_intrinsics` that may need to be called
    /// during processing.
    pub fn writer_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    /// Get reference to the underlying writer.
    pub fn writer(&self) -> &W {
        &self.writer
    }

    /// Get the current frame count.
    pub fn frame_count(&self) -> usize {
        self.stats.frames_written
    }

    /// Get the current episode index.
    pub fn episode_index(&self) -> usize {
        self.state.episode_index
    }

    /// Process complete frames from the buffer.
    fn process_complete_frames(&mut self) -> Result<()> {
        let frame_interval_ns = self.config.streaming.frame_interval_ns();
        let completion_window = self.config.streaming.completion_window_ns();

        while let Some(timestamp) = self.state.current_timestamp_ns {
            // Check if we have messages for this timestamp
            if let Some(messages) = self.state.message_buffer.remove(&timestamp) {
                // Create frame from all messages at this timestamp
                match self.messages_to_frame(messages, timestamp) {
                    Ok(Some(frame)) => {
                        self.write_frame(frame)?;
                    }
                    Ok(None) => {
                        // Frame was empty (no relevant data), skip it
                    }
                    Err(e) => {
                        warn!(timestamp, error = %e, "Failed to create frame, skipping");
                    }
                }

                // Move to next timestamp
                let _next_ts = self
                    .state
                    .end_timestamp_ns
                    .unwrap_or(timestamp)
                    .saturating_add(frame_interval_ns);

                // Find next buffered timestamp that's within completion window
                self.state.current_timestamp_ns = self
                    .state
                    .message_buffer
                    .keys()
                    .copied()
                    .filter(|&t: &u64| {
                        t >= timestamp && t.saturating_sub(timestamp) <= completion_window
                    })
                    .min();

                // If no more frames in window, advance to the next buffered timestamp
                if self.state.current_timestamp_ns.is_none() {
                    self.state.current_timestamp_ns = self
                        .state
                        .message_buffer
                        .keys()
                        .copied()
                        .filter(|&t: &u64| t > timestamp)
                        .min();
                }
            } else {
                // No messages for current timestamp, move to next
                self.state.current_timestamp_ns = self
                    .state
                    .message_buffer
                    .keys()
                    .copied()
                    .filter(|&t: &u64| t > timestamp)
                    .min();
                break;
            }
        }

        Ok(())
    }

    /// Flush any remaining frames from the buffer.
    fn flush_remaining_frames(&mut self) -> Result<()> {
        // Collect all remaining messages to avoid borrow checker issues
        let remaining: Vec<_> = self.state.message_buffer.drain().collect();

        for (timestamp, messages) in remaining {
            if !messages.is_empty() {
                match self.messages_to_frame(messages, timestamp) {
                    Ok(Some(frame)) => {
                        self.write_frame(frame)?;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        warn!(timestamp, error = %e, "Failed to create frame during flush");
                    }
                }
            }
        }
        Ok(())
    }

    /// Write a frame to the underlying writer.
    fn write_frame(&mut self, frame: AlignedFrame) -> Result<()> {
        self.writer
            .write_frame(&frame)
            .map_err(|e| RoboflowError::other(format!("Write frame failed: {}", e)))?;
        self.stats.frames_written += 1;
        self.state.frame_index += 1;
        Ok(())
    }

    /// Convert multiple timestamped messages to an aligned frame.
    ///
    /// Returns None if the frame has no relevant data (no images or states).
    fn messages_to_frame(
        &self,
        messages: Vec<TimestampedMessage>,
        timestamp_ns: u64,
    ) -> Result<Option<AlignedFrame>> {
        let mut frame = AlignedFrame::new(self.state.frame_index, timestamp_ns);

        for msg in messages {
            self.process_message_for_frame(&mut frame, &msg)?;
        }

        // Only return the frame if it has some data
        if frame.is_empty() {
            Ok(None)
        } else {
            Ok(Some(frame))
        }
    }

    /// Process a single message and add its data to the frame.
    fn process_message_for_frame(
        &self,
        frame: &mut AlignedFrame,
        msg: &TimestampedMessage,
    ) -> Result<()> {
        // Get the feature name for this topic
        let feature_name = self
            .config
            .topic_mappings
            .get(&msg.topic)
            .cloned()
            .unwrap_or_else(|| {
                // Default: convert topic to feature name
                msg.topic
                    .replace('/', ".")
                    .trim_start_matches('.')
                    .to_string()
            });

        match &msg.data {
            robocodec::CodecValue::Array(arr) => {
                // Convert array of numerics to state vector
                let state: Vec<f32> = arr
                    .iter()
                    .filter_map(|v| match v {
                        robocodec::CodecValue::Float32(n) => Some(*n),
                        robocodec::CodecValue::Float64(n) => Some(*n as f32),
                        robocodec::CodecValue::Int32(n) => Some(*n as f32),
                        robocodec::CodecValue::Int64(n) => Some(*n as f32),
                        robocodec::CodecValue::UInt32(n) => Some(*n as f32),
                        robocodec::CodecValue::UInt64(n) => Some(*n as f32),
                        _ => None,
                    })
                    .collect();

                if !state.is_empty() {
                    // Determine if this is an action or state
                    if feature_name == "action" || feature_name.contains(".action") {
                        frame.add_action(feature_name, state);
                    } else {
                        frame.add_state(feature_name, state);
                    }
                }
            }
            robocodec::CodecValue::Struct(map) => {
                // Check for CameraInfo (has K and D matrices)
                if map.contains_key("K") && map.contains_key("D") {
                    // Camera info - this is metadata, not frame data
                    // It will be handled separately by the writer
                    debug!(
                        topic = %msg.topic,
                        feature = %feature_name,
                        "Detected camera calibration message"
                    );
                    return Ok(());
                }

                // Check for image data (has width, height, data fields)
                if let (Some(width), Some(height), Some(image_bytes)) = (
                    map.get("width").and_then(extract_u32),
                    map.get("height").and_then(extract_u32),
                    extract_image_bytes(map),
                ) {
                    let image_data = ImageData::new_rgb(width, height, image_bytes)
                        .map_err(|e| RoboflowError::other(format!("Invalid image data: {}", e)))?;
                    frame.add_image(feature_name, image_data);
                    return Ok(());
                }

                // Check for state data in struct (e.g., JointState position field)
                if let Some(robocodec::CodecValue::Array(position_arr)) = map.get("position") {
                    let state: Vec<f32> = position_arr
                        .iter()
                        .filter_map(|v| match v {
                            robocodec::CodecValue::Float32(n) => Some(*n),
                            robocodec::CodecValue::Float64(n) => Some(*n as f32),
                            robocodec::CodecValue::Int32(n) => Some(*n as f32),
                            robocodec::CodecValue::Int64(n) => Some(*n as f32),
                            robocodec::CodecValue::UInt32(n) => Some(*n as f32),
                            robocodec::CodecValue::UInt64(n) => Some(*n as f32),
                            _ => None,
                        })
                        .collect();

                    if !state.is_empty() {
                        if feature_name == "action" || feature_name.contains(".action") {
                            frame.add_action(feature_name, state);
                        } else {
                            frame.add_state(feature_name, state);
                        }
                        return Ok(());
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }
}

/// Extract u32 from a CodecValue.
fn extract_u32(value: &robocodec::CodecValue) -> Option<u32> {
    match value {
        robocodec::CodecValue::UInt32(n) => Some(*n),
        robocodec::CodecValue::UInt64(n) if *n <= u32::MAX as u64 => Some(*n as u32),
        robocodec::CodecValue::Int32(n) if *n >= 0 => Some(*n as u32),
        robocodec::CodecValue::Int64(n) if *n >= 0 && *n <= u32::MAX as i64 => Some(*n as u32),
        _ => None,
    }
}

/// Extract image bytes from a struct message.
fn extract_image_bytes(map: &HashMap<String, robocodec::CodecValue>) -> Option<Vec<u8>> {
    let data = map.get("data")?;

    match data {
        robocodec::CodecValue::Bytes(b) => Some(b.clone()),
        robocodec::CodecValue::Array(arr) => {
            // Handle UInt8 array
            let bytes: Vec<u8> = arr
                .iter()
                .filter_map(|v| match v {
                    robocodec::CodecValue::UInt8(b) => Some(*b),
                    robocodec::CodecValue::Int8(b) if *b >= 0 => Some(*b as u8),
                    robocodec::CodecValue::UInt16(b) if *b <= u8::MAX as u16 => Some(*b as u8),
                    robocodec::CodecValue::Int16(b) if *b >= 0 && (*b as u16) <= u8::MAX as u16 => {
                        Some(*b as u8)
                    }
                    robocodec::CodecValue::UInt32(b) if *b <= u8::MAX as u32 => Some(*b as u8),
                    robocodec::CodecValue::Int32(b) if *b >= 0 && (*b as u32) <= u8::MAX as u32 => {
                        Some(*b as u8)
                    }
                    robocodec::CodecValue::UInt64(b) if *b <= u8::MAX as u64 => Some(*b as u8),
                    robocodec::CodecValue::Int64(b) if *b >= 0 && (*b as u64) <= u8::MAX as u64 => {
                        Some(*b as u8)
                    }
                    _ => None,
                })
                .collect();

            if bytes.is_empty() {
                // Try nested arrays
                for v in arr.iter() {
                    if let robocodec::CodecValue::Array(inner) = v {
                        let inner_bytes: Vec<u8> = inner
                            .iter()
                            .filter_map(|v| match v {
                                robocodec::CodecValue::UInt8(b) => Some(*b),
                                robocodec::CodecValue::Int8(b) if *b >= 0 => Some(*b as u8),
                                _ => None,
                            })
                            .collect();
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
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_config_builder() {
        let streaming = StreamingConfig::with_fps(60);
        let config = PipelineConfig::new(streaming)
            .with_max_frames(1000)
            .with_checkpoint_interval(Duration::from_secs(30))
            .with_topic_mapping("/camera", "observation.camera");

        assert_eq!(config.streaming.fps, 60);
        assert_eq!(config.max_frames, Some(1000));
        assert_eq!(config.checkpoint_interval, Some(Duration::from_secs(30)));
        assert_eq!(
            config.topic_mappings.get("/camera"),
            Some(&"observation.camera".to_string())
        );
    }

    #[test]
    fn test_extract_u32() {
        use robocodec::CodecValue;

        assert_eq!(extract_u32(&CodecValue::UInt32(42)), Some(42));
        assert_eq!(extract_u32(&CodecValue::UInt64(42)), Some(42));
        assert_eq!(extract_u32(&CodecValue::Int32(42)), Some(42));
        assert_eq!(extract_u32(&CodecValue::Int64(42)), Some(42));
        assert_eq!(extract_u32(&CodecValue::UInt32(u32::MAX)), Some(u32::MAX));
        assert_eq!(
            extract_u32(&CodecValue::UInt64(u32::MAX as u64)),
            Some(u32::MAX)
        );
        assert_eq!(extract_u32(&CodecValue::Int32(-1)), None);
        assert_eq!(extract_u32(&CodecValue::UInt64(u32::MAX as u64 + 1)), None);
    }

    #[test]
    fn test_extract_image_bytes() {
        use robocodec::CodecValue;

        let mut map = HashMap::new();
        map.insert("data".to_string(), CodecValue::Bytes(vec![1, 2, 3, 4]));

        assert_eq!(extract_image_bytes(&map), Some(vec![1, 2, 3, 4]));
    }

    #[test]
    fn test_extract_image_bytes_from_array() {
        use robocodec::CodecValue;

        let mut map = HashMap::new();
        let data: Vec<CodecValue> = vec![1, 2, 3, 4]
            .into_iter()
            .map(CodecValue::UInt8)
            .collect();
        map.insert("data".to_string(), CodecValue::Array(data));

        assert_eq!(extract_image_bytes(&map), Some(vec![1, 2, 3, 4]));
    }
}
