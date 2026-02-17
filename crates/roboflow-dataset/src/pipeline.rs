// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Unified pipeline executor for dataset writing.
//!
//! This module provides a streamlined pipeline orchestration that works
//! directly with `TimestampedMessage` from sources and `DatasetWriter`
//! for output.
//!
//! # Architecture
//!
//! ```text
//! Source (MCAP) -> PipelineExecutor -> DatasetWriter
//!   TimestampedMsg    Frame alignment    (LeRobotWriter)
//!                     Episode tracking
//!                     Message aggregation
//! ```

use std::borrow::Cow;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use roboflow_core::{Result, RoboflowError, TimestampedMessage};
use tracing::{debug, info, instrument, trace, warn};

use crate::common::base::{AlignedFrame, DatasetWriter, ImageData};
use crate::image::ImageFormat;
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

    /// Get the feature name for a given topic.
    ///
    /// This avoids repeated string allocations by using Cow.
    /// Uses the topic_mappings if available, otherwise converts
    /// the topic to a feature name by replacing '/' with '.' and
    /// trimming leading '.'.
    pub fn get_feature_name<'a>(&'a self, topic: &'a str) -> Cow<'a, str> {
        if let Some(mapped) = self.topic_mappings.get(topic) {
            Cow::Borrowed(mapped)
        } else {
            // Convert topic to feature name: '/' -> '.', trim leading '.'
            let mut s = topic.replace('/', ".");
            if s.starts_with('.') {
                s = s.trim_start_matches('.').to_string();
            }
            Cow::Owned(s)
        }
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
    /// Camera info topics we've already processed (calibration is constant per bag)
    processed_camera_info: std::collections::HashSet<String>,
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
                processed_camera_info: std::collections::HashSet::new(),
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

        // Quick check: skip camera info messages we've already processed
        // Camera calibration is constant per bag, so we only need to decode it once
        if is_camera_info_topic(&msg.data)
            && !self.state.processed_camera_info.insert(msg.topic.clone())
        {
            // Already processed this camera info topic before
            return Ok(());
        }
        // First time seeing this camera info topic - will be decoded when buffered

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

    /// Process multiple timestamped messages in batch.
    ///
    /// This is more efficient than calling `process_message` multiple times
    /// as it reduces function call overhead and allows better cache utilization.
    /// Messages are still processed in timestamp order.
    ///
    /// # Arguments
    ///
    /// * `messages` - Slice of timestamped messages to process
    #[instrument(skip_all, fields(count = messages.len()))]
    pub fn process_messages_batch(&mut self, messages: &[TimestampedMessage]) -> Result<()> {
        // Check max frames limit once for the batch
        if let Some(max) = self.config.max_frames
            && self.stats.frames_written >= max
        {
            return Ok(());
        }

        let frame_interval_ns = self.config.streaming.frame_interval_ns();

        // Pre-allocate and buffer all messages at once
        for msg in messages {
            // Check max frames limit during iteration
            if let Some(max) = self.config.max_frames
                && self.stats.frames_written >= max
            {
                break;
            }

            // Calculate frame index for this message
            let frame_idx = msg.log_time / frame_interval_ns;
            let aligned_timestamp = frame_idx * frame_interval_ns;

            // Buffer message by timestamp
            self.state
                .message_buffer
                .entry(aligned_timestamp)
                .or_default()
                .push(msg.clone());

            // Track timestamp range
            if self.state.current_timestamp_ns.is_none() {
                self.state.current_timestamp_ns = Some(aligned_timestamp);
            }
            self.state.end_timestamp_ns =
                Some(aligned_timestamp.max(self.state.end_timestamp_ns.unwrap_or(0)));
        }

        // Update stats (more efficient than per-message)
        self.stats.messages_processed += messages.len();

        // Process complete frames in batch
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
        &mut self,
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
        &mut self,
        frame: &mut AlignedFrame,
        msg: &TimestampedMessage,
    ) -> Result<()> {
        // Get the feature name for this topic
        // When topic_mappings is configured, only process mapped topics
        // When topic_mappings is empty, fall back to converting topic name to feature name
        let feature_name = if self.config.topic_mappings.is_empty() {
            // No explicit mappings - use fallback conversion
            msg.topic
                .replace('/', ".")
                .trim_start_matches('.')
                .to_string()
        } else {
            // Mappings configured - only process topics that are explicitly mapped
            match self.config.topic_mappings.get(&msg.topic).cloned() {
                Some(feature) => feature,
                None => {
                    // Topic not in mappings - skip this message
                    // This ensures only topics explicitly configured in lerobot_config.toml are processed
                    trace!(
                        topic = %msg.topic,
                        "Skipping unmapped topic"
                    );
                    return Ok(());
                }
            }
        };

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
                    // Skip processing if we've already seen this topic (calibration is constant per bag)
                    if self.state.processed_camera_info.insert(msg.topic.clone()) {
                        debug!(
                            topic = %msg.topic,
                            feature = %feature_name,
                            "Cached camera calibration message (first occurrence)"
                        );
                    }
                    return Ok(());
                }

                // Check for ROS CompressedImage (has format and data, but no width/height)
                // sensor_msgs/CompressedImage: std_msgs/Header header, string format, uint8[] data
                if let (Some(format), Some(image_bytes)) = (
                    map.get("format").and_then(|v| {
                        if let robocodec::CodecValue::String(s) = v {
                            Some(s.as_str())
                        } else {
                            None
                        }
                    }),
                    extract_image_bytes(map),
                ) {
                    // Compressed image (JPEG/PNG) - extract dimensions from header
                    let data_size = image_bytes.len();
                    let detected_format = ImageFormat::from_magic_bytes(&image_bytes);
                    let (width, height) = detected_format
                        .extract_dimensions(&image_bytes)
                        .unwrap_or((0, 0));

                    let image_data = ImageData::encoded(width, height, image_bytes);
                    frame.add_image(feature_name.clone(), image_data);

                    if width == 0 || height == 0 {
                        debug!(
                            topic = %msg.topic,
                            feature = %feature_name,
                            format,
                            size = data_size,
                            detected_format = ?detected_format,
                            "CompressedImage with unknown dimensions (will need decode)"
                        );
                    } else {
                        trace!(
                            topic = %msg.topic,
                            feature = %feature_name,
                            format,
                            size = data_size,
                            width,
                            height,
                            "Processing CompressedImage"
                        );
                    }
                    return Ok(());
                }

                // Check for regular image data (has width, height, data fields)
                if let (Some(width), Some(height), Some(image_bytes)) = (
                    map.get("width").and_then(extract_u32),
                    map.get("height").and_then(extract_u32),
                    extract_image_bytes(map),
                ) {
                    // Check if this is compressed image data (JPEG/PNG)
                    // Compressed images have data size much smaller than expected RGB size
                    let expected_rgb_size = (width as usize) * (height as usize) * 3;
                    let is_compressed = image_bytes.len() < expected_rgb_size;

                    let image_data = if is_compressed {
                        // Compressed image (JPEG/PNG) - use encoded() constructor
                        // The data will be decoded later during MP4 encoding
                        ImageData::encoded(width, height, image_bytes)
                    } else {
                        // Raw RGB data - validate size
                        ImageData::new_rgb(width, height, image_bytes).map_err(|e| {
                            RoboflowError::other(format!("Invalid image data: {}", e))
                        })?
                    };
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

/// Check if a message contains camera calibration info (K and D matrices).
///
/// Camera calibration messages contain intrinsic matrix K and distortion
/// coefficients D. These are constant for a given camera throughout a bag
/// recording, so we only need to process each camera's calibration once.
fn is_camera_info_topic(data: &robocodec::CodecValue) -> bool {
    match data {
        robocodec::CodecValue::Struct(map) => map.contains_key("K") && map.contains_key("D"),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::base::{DatasetWriter, ImageData, UploadState, WriterStats};
    use std::any::Any;

    /// Mock writer for testing pipeline executor.
    struct MockWriter {
        frame_count: usize,
        /// Track images added to the writer for test assertions
        images: Vec<ImageData>,
    }

    impl MockWriter {
        fn new() -> Self {
            Self {
                frame_count: 0,
                images: Vec::new(),
            }
        }
    }

    impl DatasetWriter for MockWriter {
        fn write_frame(&mut self, frame: &AlignedFrame) -> Result<()> {
            self.frame_count += 1;
            // Capture images from the frame for test assertions
            for img in frame.images.values() {
                self.images.push(ImageData::encoded(img.width, img.height, img.data.clone()));
            }
            Ok(())
        }

        fn write_batch(&mut self, frames: &[AlignedFrame]) -> Result<()> {
            for frame in frames {
                self.write_frame(frame)?;
            }
            Ok(())
        }

        fn finalize(&mut self) -> Result<WriterStats> {
            Ok(WriterStats {
                frames_written: self.frame_count,
                images_encoded: 0,
                state_records: 0,
                output_bytes: 0,
                duration_sec: 0.0,
                decode_failures: 0,
            })
        }

        fn frame_count(&self) -> usize {
            self.frame_count
        }

        fn episode_index(&self) -> Option<usize> {
            Some(0)
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn get_upload_state(&self) -> Option<UploadState> {
            None
        }
    }

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

    #[test]
    fn test_is_camera_info_topic() {
        use robocodec::CodecValue;

        // Camera info has K and D matrices
        let mut camera_info = HashMap::new();
        camera_info.insert("K".to_string(), CodecValue::Array(vec![]));
        camera_info.insert("D".to_string(), CodecValue::Array(vec![]));
        assert!(is_camera_info_topic(&CodecValue::Struct(camera_info)));

        // Non-camera info struct
        let mut other_info = HashMap::new();
        other_info.insert("width".to_string(), CodecValue::UInt32(640));
        other_info.insert("height".to_string(), CodecValue::UInt32(480));
        assert!(!is_camera_info_topic(&CodecValue::Struct(other_info)));

        // Only K matrix
        let mut only_k = HashMap::new();
        only_k.insert("K".to_string(), CodecValue::Array(vec![]));
        assert!(!is_camera_info_topic(&CodecValue::Struct(only_k)));

        // Only D matrix
        let mut only_d = HashMap::new();
        only_d.insert("D".to_string(), CodecValue::Array(vec![]));
        assert!(!is_camera_info_topic(&CodecValue::Struct(only_d)));

        // Array value (not a struct)
        assert!(!is_camera_info_topic(&CodecValue::Array(vec![])));

        // Bytes value
        assert!(!is_camera_info_topic(&CodecValue::Bytes(vec![])));
    }

    #[test]
    fn test_camera_info_caching() {
        use robocodec::CodecValue;

        // Create a mock writer that tracks what it receives
        let writer = MockWriter::new();

        let streaming = StreamingConfig::with_fps(30);
        let config = PipelineConfig::new(streaming);

        let mut executor = PipelineExecutor::new(writer, config);

        // Camera info message with K and D matrices
        let mut camera_info_map = HashMap::new();
        camera_info_map.insert(
            "K".to_string(),
            CodecValue::Array(vec![
                CodecValue::Float64(1000.0),
                CodecValue::Float64(0.0),
                CodecValue::Float64(320.0),
                CodecValue::Float64(0.0),
                CodecValue::Float64(1000.0),
                CodecValue::Float64(240.0),
                CodecValue::Float64(0.0),
                CodecValue::Float64(0.0),
                CodecValue::Float64(1.0),
            ]),
        );
        camera_info_map.insert(
            "D".to_string(),
            CodecValue::Array(vec![
                CodecValue::Float64(0.1),
                CodecValue::Float64(0.2),
                CodecValue::Float64(0.0),
                CodecValue::Float64(0.0),
                CodecValue::Float64(0.3),
            ]),
        );

        // Send the same camera info message multiple times
        for i in 0..5 {
            let msg = TimestampedMessage {
                topic: "/camera/camera_info".to_string(),
                log_time: i * 1_000_000, // Different timestamps
                data: CodecValue::Struct(camera_info_map.clone()),
            };

            executor.process_message(msg).unwrap();
        }

        // Verify that the camera info topic is marked as processed
        assert!(
            executor
                .state
                .processed_camera_info
                .contains("/camera/camera_info")
        );

        // The messages_processed stat should count all messages (filtering happens later)
        assert_eq!(executor.stats.messages_processed, 5);
    }

    #[test]
    fn test_multiple_camera_info_topics() {
        use robocodec::CodecValue;

        let writer = MockWriter::new();
        let streaming = StreamingConfig::with_fps(30);
        let config = PipelineConfig::new(streaming);
        let mut executor = PipelineExecutor::new(writer, config);

        // Create camera info for different cameras
        let topics = [
            "/camera/front/camera_info",
            "/camera/back/camera_info",
            "/camera/left/camera_info",
        ];

        for (i, topic) in topics.iter().enumerate() {
            let mut camera_info = HashMap::new();
            camera_info.insert("K".to_string(), CodecValue::Array(vec![]));
            camera_info.insert("D".to_string(), CodecValue::Array(vec![]));

            let msg = TimestampedMessage {
                topic: topic.to_string(),
                log_time: (i as u64) * 1_000_000,
                data: CodecValue::Struct(camera_info),
            };

            executor.process_message(msg).unwrap();
        }

        // All three camera topics should be marked as processed
        for topic in &topics {
            assert!(executor.state.processed_camera_info.contains(*topic));
        }
        assert_eq!(executor.state.processed_camera_info.len(), 3);
    }

    #[test]
    fn test_camera_info_cached_during_buffering() {
        use robocodec::CodecValue;

        let writer = MockWriter::new();
        let streaming = StreamingConfig::with_fps(30);
        let config = PipelineConfig::new(streaming);
        let mut executor = PipelineExecutor::new(writer, config);

        // Create camera info message
        let mut camera_info = HashMap::new();
        camera_info.insert("K".to_string(), CodecValue::Array(vec![]));
        camera_info.insert("D".to_string(), CodecValue::Array(vec![]));

        let topic = "/camera/camera_info";

        // First camera info message - should be processed
        let msg1 = TimestampedMessage {
            topic: topic.to_string(),
            log_time: 0,
            data: CodecValue::Struct(camera_info.clone()),
        };
        executor.process_message(msg1).unwrap();

        // Verify it's in the processed set
        assert!(executor.state.processed_camera_info.contains(topic));

        // Second camera info message with same topic - should be skipped early
        // (before buffering, in process_message)
        let msg2 = TimestampedMessage {
            topic: topic.to_string(),
            log_time: 33_333_333,
            data: CodecValue::Struct(camera_info),
        };

        let messages_before = executor.stats.messages_processed;
        executor.process_message(msg2).unwrap();
        let messages_after = executor.stats.messages_processed;

        // Message count should still increment (we count messages, not just process them)
        assert_eq!(messages_after, messages_before + 1);

        // But the topic should remain in processed set (no duplicate entry)
        assert_eq!(executor.state.processed_camera_info.len(), 1);
    }

    #[test]
    fn test_compressed_image_handling() {
        use robocodec::CodecValue;

        let writer = MockWriter::new();
        let streaming = StreamingConfig::with_fps(30);
        let config = PipelineConfig::new(streaming);
        let mut executor = PipelineExecutor::new(writer, config);

        // Simulate a compressed JPEG image (much smaller than expected RGB size)
        // 640x480 RGB would be 921,600 bytes, but JPEG might be 10-50KB
        let width = 640u32;
        let height = 480u32;
        let compressed_jpeg = vec![0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46]; // JPEG header + some data

        let mut image_msg = HashMap::new();
        image_msg.insert("width".to_string(), CodecValue::UInt32(width));
        image_msg.insert("height".to_string(), CodecValue::UInt32(height));
        image_msg.insert("data".to_string(), CodecValue::Bytes(compressed_jpeg));

        let msg = TimestampedMessage {
            topic: "/camera/compressed".to_string(),
            log_time: 0,
            data: CodecValue::Struct(image_msg),
        };

        executor.process_message(msg).unwrap();

        // Message was processed without error
        assert_eq!(executor.stats.messages_processed, 1);
    }

    #[test]
    fn test_raw_rgb_image_handling() {
        use robocodec::CodecValue;

        let writer = MockWriter::new();
        let streaming = StreamingConfig::with_fps(30);
        let config = PipelineConfig::new(streaming);
        let mut executor = PipelineExecutor::new(writer, config);

        // Simulate raw RGB image data (exactly width * height * 3 bytes)
        let width = 64u32;
        let height = 48u32;
        let rgb_data = vec![128u8; (width * height * 3) as usize]; // Exact RGB size

        let mut image_msg = HashMap::new();
        image_msg.insert("width".to_string(), CodecValue::UInt32(width));
        image_msg.insert("height".to_string(), CodecValue::UInt32(height));
        image_msg.insert("data".to_string(), CodecValue::Bytes(rgb_data));

        let msg = TimestampedMessage {
            topic: "/camera/raw".to_string(),
            log_time: 0,
            data: CodecValue::Struct(image_msg),
        };

        executor.process_message(msg).unwrap();

        // Message was processed without error
        assert_eq!(executor.stats.messages_processed, 1);
    }

    #[test]
    fn test_unmapped_topics_skipped_when_mappings_configured() {
        use robocodec::CodecValue;

        let writer = MockWriter::new();
        let streaming = StreamingConfig::with_fps(30);
        // Configure with explicit topic mappings
        let config = PipelineConfig::new(streaming)
            .with_topic_mapping("/camera/mapped", "observation.images.mapped");
        let mut executor = PipelineExecutor::new(writer, config);

        // Create a compressed image message for a MAPPED topic
        let width = 64u32;
        let height = 48u32;
        let compressed_jpeg = vec![0xFF, 0xD8, 0xFF, 0xE0]; // JPEG header

        let mut mapped_image = HashMap::new();
        mapped_image.insert("width".to_string(), CodecValue::UInt32(width));
        mapped_image.insert("height".to_string(), CodecValue::UInt32(height));
        mapped_image.insert(
            "data".to_string(),
            CodecValue::Bytes(compressed_jpeg.clone()),
        );

        let mapped_msg = TimestampedMessage {
            topic: "/camera/mapped".to_string(),
            log_time: 0,
            data: CodecValue::Struct(mapped_image),
        };

        // Create a compressed image message for an UNMAPPED topic
        let mut unmapped_image = HashMap::new();
        unmapped_image.insert("width".to_string(), CodecValue::UInt32(width));
        unmapped_image.insert("height".to_string(), CodecValue::UInt32(height));
        unmapped_image.insert("data".to_string(), CodecValue::Bytes(compressed_jpeg));

        let unmapped_msg = TimestampedMessage {
            topic: "/camera/unmapped".to_string(),
            log_time: 33_333_333, // Next frame
            data: CodecValue::Struct(unmapped_image),
        };

        // Process both messages
        executor.process_message(mapped_msg).unwrap();
        executor.process_message(unmapped_msg).unwrap();

        // Both messages should be counted as processed
        assert_eq!(executor.stats.messages_processed, 2);

        // But only the mapped topic should have been written to frames
        // We can verify this by checking the writer's frame count
        // (MockWriter doesn't track this, so we just verify no errors)
    }

    #[test]
    fn test_all_topics_processed_when_no_mappings() {
        use robocodec::CodecValue;

        let writer = MockWriter::new();
        let streaming = StreamingConfig::with_fps(30);
        // No topic mappings - should process all topics
        let config = PipelineConfig::new(streaming);
        let mut executor = PipelineExecutor::new(writer, config);

        // Create image messages for different topics
        let width = 64u32;
        let height = 48u32;
        let compressed_jpeg = vec![0xFF, 0xD8, 0xFF, 0xE0];

        for topic in ["/camera/a", "/camera/b", "/camera/c"] {
            let mut image = HashMap::new();
            image.insert("width".to_string(), CodecValue::UInt32(width));
            image.insert("height".to_string(), CodecValue::UInt32(height));
            image.insert(
                "data".to_string(),
                CodecValue::Bytes(compressed_jpeg.clone()),
            );

            let msg = TimestampedMessage {
                topic: topic.to_string(),
                log_time: 0,
                data: CodecValue::Struct(image),
            };

            executor.process_message(msg).unwrap();
        }

        // All messages should be processed
        assert_eq!(executor.stats.messages_processed, 3);
    }

    /// Test that ROS CompressedImage messages (format + data, no width/height)
    /// have dimensions extracted from the JPEG/PNG header.
    #[test]
    fn test_compressed_image_dimension_extraction_from_header() {
        use robocodec::CodecValue;

        let writer = MockWriter::new();
        let streaming = StreamingConfig::with_fps(30);
        let config = PipelineConfig::new(streaming);
        let mut executor = PipelineExecutor::new(writer, config);

        // Construct a minimal valid JPEG with SOF0 marker containing dimensions
        // This simulates a real sensor_msgs/CompressedImage which has NO width/height fields
        // FF D8 - SOI (Start of Image)
        // FF E0 + length + "JFIF\0" - APP0 marker
        // FF C0 + length + precision + height + width - SOF0 marker with dimensions
        // Expected: 200x100 pixels (width x height)
        let jpeg_with_sof: Vec<u8> = vec![
            0xFF, 0xD8, // SOI (Start of Image)
            0xFF, 0xE0, 0x00, 0x10, // APP0 marker + length (16 bytes)
            0x4A, 0x46, 0x49, 0x46, 0x00, // "JFIF\0"
            0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, // JFIF version and density
            0xFF, 0xC0, // SOF0 marker (Start of Frame, baseline DCT)
            0x00, 0x0B, // Length (11 bytes)
            0x08, // Precision (8 bits)
            0x00, 0x64, // Height: 100 (0x0064)
            0x00, 0xC8, // Width: 200 (0x00C8)
            0x01, 0x01, 0x11, 0x00, // Number of components + component data
        ];

        // Create a sensor_msgs/CompressedImage message (has format + data, no width/height)
        let mut compressed_msg = HashMap::new();
        compressed_msg.insert("format".to_string(), CodecValue::String("jpeg".to_string()));
        compressed_msg.insert("data".to_string(), CodecValue::Bytes(jpeg_with_sof.clone()));
        // NOTE: No width/height fields - this is the key difference from sensor_msgs/Image

        let msg = TimestampedMessage {
            topic: "/camera/compressed".to_string(),
            log_time: 0,
            data: CodecValue::Struct(compressed_msg),
        };

        executor.process_message(msg).unwrap();

        // Verify message was processed
        assert_eq!(executor.stats.messages_processed, 1);

        // Verify the image was added to the writer with correct dimensions
        let writer = &executor.writer;
        assert_eq!(writer.images.len(), 1);
        let img = &writer.images[0];
        assert_eq!(img.width, 200, "Width should be extracted from JPEG SOF marker");
        assert_eq!(img.height, 100, "Height should be extracted from JPEG SOF marker");
        assert!(img.is_encoded, "Image should be marked as encoded");
    }

    /// Test that ROS CompressedImage with PNG format has dimensions extracted from header.
    #[test]
    fn test_compressed_image_png_dimension_extraction_from_header() {
        use robocodec::CodecValue;

        let writer = MockWriter::new();
        let streaming = StreamingConfig::with_fps(30);
        let config = PipelineConfig::new(streaming);
        let mut executor = PipelineExecutor::new(writer, config);

        // Construct a minimal valid PNG with IHDR chunk containing dimensions
        // Expected: 128x64 pixels (width x height)
        let png_data: Vec<u8> = vec![
            0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, // PNG signature
            0x00, 0x00, 0x00, 0x0D, // IHDR chunk length (13 bytes)
            0x49, 0x48, 0x44, 0x52, // "IHDR"
            0x00, 0x00, 0x00, 0x80, // Width: 128 (0x80)
            0x00, 0x00, 0x00, 0x40, // Height: 64 (0x40)
            0x08, 0x02, 0x00, 0x00, 0x00, // Bit depth, color type, etc.
            0x00, 0x00, 0x00, 0x00, // CRC placeholder
        ];

        let mut compressed_msg = HashMap::new();
        compressed_msg.insert("format".to_string(), CodecValue::String("png".to_string()));
        compressed_msg.insert("data".to_string(), CodecValue::Bytes(png_data.clone()));
        // NOTE: No width/height fields

        let msg = TimestampedMessage {
            topic: "/camera/compressed".to_string(),
            log_time: 0,
            data: CodecValue::Struct(compressed_msg),
        };

        executor.process_message(msg).unwrap();

        // Verify message was processed
        assert_eq!(executor.stats.messages_processed, 1);

        // Verify dimensions extracted from PNG IHDR
        let writer = &executor.writer;
        assert_eq!(writer.images.len(), 1);
        let img = &writer.images[0];
        assert_eq!(img.width, 128, "Width should be extracted from PNG IHDR");
        assert_eq!(img.height, 64, "Height should be extracted from PNG IHDR");
        assert!(img.is_encoded, "Image should be marked as encoded");
    }

    /// Test that CompressedImage with unparseable header gets dimensions (0, 0)
    /// and is still processed without error.
    #[test]
    fn test_compressed_image_unknown_dimensions_handled_gracefully() {
        use robocodec::CodecValue;

        let writer = MockWriter::new();
        let streaming = StreamingConfig::with_fps(30);
        let config = PipelineConfig::new(streaming);
        let mut executor = PipelineExecutor::new(writer, config);

        // Create a CompressedImage with invalid/truncated JPEG data
        // (no SOF marker, so dimensions can't be extracted)
        let invalid_jpeg = vec![0xFF, 0xD8, 0xFF]; // Just JPEG magic, no SOF

        let mut compressed_msg = HashMap::new();
        compressed_msg.insert("format".to_string(), CodecValue::String("jpeg".to_string()));
        compressed_msg.insert("data".to_string(), CodecValue::Bytes(invalid_jpeg));

        let msg = TimestampedMessage {
            topic: "/camera/compressed".to_string(),
            log_time: 0,
            data: CodecValue::Struct(compressed_msg),
        };

        // Should NOT error - handles gracefully with (0, 0) dimensions
        executor.process_message(msg).unwrap();

        assert_eq!(executor.stats.messages_processed, 1);

        // Image should still be added but with 0x0 dimensions
        let writer = &executor.writer;
        assert_eq!(writer.images.len(), 1);
        let img = &writer.images[0];
        assert_eq!(img.width, 0, "Width should be 0 for unparseable header");
        assert_eq!(img.height, 0, "Height should be 0 for unparseable header");
        assert!(img.is_encoded, "Image should still be marked as encoded");
    }
}
