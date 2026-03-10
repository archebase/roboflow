// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Unified dataset pipeline executor using the policy pattern.
//!
//! This module provides [`DatasetPipelineExecutor`] which combines frame alignment,
//! dataset writing, and execution policy into a unified interface that works
//! directly with [`AlignedFrame`] and [`FormatWriter`].
//!
//! # Architecture
//!
//! ```text
//! Source (TimestampedMessage)
//!        ↓
//! DatasetPipelineExecutor (buffer by timestamp, apply policy)
//!        ↓
//! FormatWriter (LerobotWriter, etc.)
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use roboflow_dataset::formats::{DatasetPipelineExecutor, DatasetPipelineConfig};
//! use roboflow_dataset::formats::lerobot::LerobotWriter;
//! use roboflow_executor::SequentialPolicy;
//!
//! let writer = LerobotWriter::new_local("./output", config)?;
//! let config = DatasetPipelineConfig::with_fps(30);
//! let mut executor = DatasetPipelineExecutor::new(writer, config, SequentialPolicy);
//!
//! // Process messages
//! for msg in messages {
//!     executor.process_message(msg)?;
//! }
//!
//! let stats = executor.finalize()?;
//! ```

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Instant;

use robocodec::CodecValue;
use roboflow_core::{Result, TimestampedMessage};
use roboflow_media::image::ImageFormat;
use tracing::{debug, info, trace, warn};

use roboflow_dataset::core::traits::{AlignedFrame, FormatWriter};
use roboflow_dataset::formats::alignment::config::StreamingConfig;
use roboflow_dataset::formats::common::{ImageData, ImageRef, extract_image_bytes, extract_u32};
use roboflow_dataset::formats::lerobot::writer::{BackgroundVideoEncoder, EncodeRequest};

/// Re-export execution policy types from roboflow_executor.
pub use roboflow_executor::{ExecutionPolicy, ParallelPolicy, SequentialPolicy};

/// Statistics from dataset pipeline execution.
#[derive(Debug, Clone, Default)]
pub struct DatasetPipelineStats {
    /// Total frames written
    pub frames_written: usize,
    /// Total episodes written
    pub episodes_written: usize,
    /// Total messages processed
    pub messages_processed: usize,
    /// Processing time in seconds
    pub duration_sec: f64,
    /// Throughput in frames per second
    pub fps: f64,
    /// Name of the execution policy used
    pub policy_name: &'static str,
    /// State/action feature dimensions (feature_name -> dim)
    pub state_dims: HashMap<String, usize>,
    /// Image feature shapes (feature_name -> (height, width))
    pub image_shapes: HashMap<String, (usize, usize)>,
    /// Per-feature statistics (serialized JSON values for cross-crate compatibility)
    pub feature_stats: HashMap<String, serde_json::Value>,
    /// Total images encoded via fast-path
    pub images_encoded: usize,
    /// Frames skipped during encoding
    pub skipped_frames: usize,
    /// Total output bytes from video encoding
    pub encode_output_bytes: u64,
}

/// Configuration for the image fast-path.
///
/// When enabled, image messages are routed directly from message processing
/// to the `BackgroundVideoEncoder`, bypassing the frame alignment buffer and
/// writer. Only lightweight `ImageRef` (width/height) passes through alignment
/// for parquet path column generation.
#[derive(Debug, Clone)]
pub struct ImageFastPathConfig {
    /// Whether the fast-path is enabled.
    pub enabled: bool,
    /// Output directory for video files.
    pub output_dir: PathBuf,
    /// Chunk index for LeRobot v2.1 path scheme.
    pub chunk_index: u32,
    /// Episode index for file naming.
    pub episode_index: usize,
    /// Topics classified as image topics (MappingType::Image).
    pub image_topics: HashSet<String>,
}

/// Episode management strategy.
#[derive(Debug, Clone, Default)]
pub enum EpisodeStrategy {
    /// Single episode for entire stream
    #[default]
    Single,
    /// Split episodes when timestamp gap exceeds threshold
    GapBased {
        /// Gap threshold in nanoseconds
        threshold_ns: u64,
    },
    /// Fixed number of frames per episode
    FrameCount {
        /// Maximum frames per episode
        max_frames: usize,
    },
}

/// Configuration for the dataset pipeline executor.
#[derive(Debug, Clone)]
pub struct DatasetPipelineConfig {
    /// Streaming/alignment configuration
    pub streaming: StreamingConfig,
    /// Maximum frames to process (None = unlimited)
    pub max_frames: Option<usize>,
    /// Episode management strategy
    pub episode_strategy: EpisodeStrategy,
    /// Topic to feature mappings
    pub topic_mappings: HashMap<String, String>,
    /// Image fast-path configuration (None = images go through writer)
    pub image_fast_path: Option<ImageFastPathConfig>,
}

impl DatasetPipelineConfig {
    /// Create a new config with the specified frame rate.
    pub fn with_fps(fps: u32) -> Self {
        Self {
            streaming: StreamingConfig::with_fps(fps),
            max_frames: None,
            episode_strategy: EpisodeStrategy::default(),
            topic_mappings: HashMap::new(),
            image_fast_path: None,
        }
    }

    /// Set maximum frames to process.
    pub fn with_max_frames(mut self, max: usize) -> Self {
        self.max_frames = Some(max);
        self
    }

    /// Set episode management strategy.
    pub fn with_episode_strategy(mut self, strategy: EpisodeStrategy) -> Self {
        self.episode_strategy = strategy;
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

    /// Set all topic mappings at once.
    pub fn with_topic_mappings(mut self, mappings: HashMap<String, String>) -> Self {
        self.topic_mappings = mappings;
        self
    }

    /// Set image fast-path configuration.
    pub fn with_image_fast_path(mut self, config: ImageFastPathConfig) -> Self {
        self.image_fast_path = Some(config);
        self
    }

    /// Get the feature name for a topic.
    pub fn get_feature_name<'a>(&'a self, topic: &'a str) -> Cow<'a, str> {
        if let Some(mapped) = self.topic_mappings.get(topic) {
            Cow::Borrowed(mapped)
        } else {
            let mut s = topic.replace('/', ".");
            if s.starts_with('.') {
                s = s.trim_start_matches('.').to_string();
            }
            Cow::Owned(s)
        }
    }
}

impl Default for DatasetPipelineConfig {
    fn default() -> Self {
        Self::with_fps(30)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ImageEncodingHint {
    Encoded,
    Raw,
    Unknown,
}

fn extract_bool(value: &CodecValue) -> Option<bool> {
    match value {
        CodecValue::Bool(v) => Some(*v),
        _ => None,
    }
}

fn detect_image_encoding_hint(
    map: &HashMap<String, CodecValue>,
    image_bytes: &[u8],
) -> ImageEncodingHint {
    if let Some(is_encoded) = map.get("is_encoded").and_then(extract_bool) {
        return if is_encoded {
            ImageEncodingHint::Encoded
        } else {
            ImageEncodingHint::Raw
        };
    }

    if let Some(CodecValue::String(format_str)) = map.get("format") {
        let format = ImageFormat::from_ros_format(format_str);
        if format.is_encoded() {
            return ImageEncodingHint::Encoded;
        }
        if format != ImageFormat::Unknown {
            return ImageEncodingHint::Raw;
        }
    }

    if ImageFormat::from_magic_bytes(image_bytes).is_encoded() {
        return ImageEncodingHint::Encoded;
    }

    ImageEncodingHint::Unknown
}

fn resolve_encoded_dimensions(
    width: Option<u32>,
    height: Option<u32>,
    image_bytes: &[u8],
) -> (u32, u32) {
    match (width, height) {
        (Some(w), Some(h)) if w > 0 && h > 0 => (w, h),
        _ => {
            let detected_format = ImageFormat::from_magic_bytes(image_bytes);
            detected_format
                .extract_dimensions(image_bytes)
                .unwrap_or((0, 0))
        }
    }
}

/// Internal executor state.
struct ExecutorState {
    /// Message buffer keyed by aligned timestamp
    message_buffer: HashMap<u64, Vec<TimestampedMessage>>,
    /// Current timestamp being processed
    current_timestamp_ns: Option<u64>,
    /// End timestamp seen
    end_timestamp_ns: Option<u64>,
    /// Last timestamp seen (for gap detection)
    last_timestamp_ns: Option<u64>,
    /// Current frame index
    frame_index: usize,
    /// Current episode index
    episode_index: usize,
    /// Frames in current episode
    frames_in_episode: usize,
    /// Whether an episode has been started
    episode_started: bool,
    /// Camera info topics already processed
    processed_camera_info: std::collections::HashSet<String>,
    /// Start time
    start_time: Instant,
}

impl ExecutorState {
    fn new() -> Self {
        Self {
            message_buffer: HashMap::new(),
            current_timestamp_ns: None,
            end_timestamp_ns: None,
            last_timestamp_ns: None,
            frame_index: 0,
            episode_index: 0,
            frames_in_episode: 0,
            episode_started: false,
            processed_camera_info: std::collections::HashSet::new(),
            start_time: Instant::now(),
        }
    }
}

/// Unified dataset pipeline executor with pluggable execution policy.
///
/// This executor combines:
/// - Frame alignment (internal buffering by timestamp)
/// - Dataset writing (via `FormatWriter`)
/// - Execution policy (sequential or parallel via `ExecutionPolicy`)
///
/// The policy determines how frames are processed after alignment.
/// For most dataset writing, sequential policy is appropriate since
/// writing is typically I/O bound. Parallel policy can be useful
/// for CPU-intensive transformations before writing.
pub struct DatasetPipelineExecutor<W: FormatWriter, P: ExecutionPolicy> {
    /// The format writer
    writer: W,
    /// Configuration
    config: DatasetPipelineConfig,
    /// Execution policy
    policy: P,
    /// Statistics
    stats: DatasetPipelineStats,
    /// Internal state
    state: ExecutorState,
    /// Background video encoder for image fast-path
    background_encoder: Option<BackgroundVideoEncoder>,
}

impl<W: FormatWriter> DatasetPipelineExecutor<W, SequentialPolicy> {
    /// Create a new executor with sequential execution policy.
    pub fn sequential(writer: W, config: DatasetPipelineConfig) -> Self {
        Self::new(writer, config, SequentialPolicy)
    }
}

impl<W: FormatWriter> DatasetPipelineExecutor<W, ParallelPolicy> {
    /// Create a new executor with parallel execution policy.
    pub fn parallel(writer: W, config: DatasetPipelineConfig, num_threads: usize) -> Self {
        Self::new(writer, config, ParallelPolicy::new(num_threads))
    }
}

impl<W: FormatWriter, P: ExecutionPolicy> DatasetPipelineExecutor<W, P> {
    /// Create a new dataset pipeline executor.
    ///
    /// # Arguments
    ///
    /// * `writer` - The format writer implementation
    /// * `config` - Executor configuration
    /// * `policy` - Execution policy (sequential or parallel)
    pub fn new(writer: W, config: DatasetPipelineConfig, policy: P) -> Self {
        // Create background encoder if image fast-path is configured
        let background_encoder = config
            .image_fast_path
            .as_ref()
            .filter(|fp| fp.enabled)
            .and_then(|fp| {
                use roboflow_media::video::VideoEncoderConfig;

                let video_config = VideoEncoderConfig::default();
                match BackgroundVideoEncoder::new(
                    video_config,
                    fp.output_dir.clone(),
                    fp.chunk_index,
                    fp.episode_index,
                ) {
                    Ok(encoder) => {
                        info!(
                            output_dir = %fp.output_dir.display(),
                            chunk_index = fp.chunk_index,
                            episode_index = fp.episode_index,
                            image_topics = fp.image_topics.len(),
                            "Image fast-path encoder initialized"
                        );
                        Some(encoder)
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to create background encoder, falling back to writer path");
                        None
                    }
                }
            });

        Self {
            writer,
            config,
            policy,
            stats: DatasetPipelineStats::default(),
            state: ExecutorState::new(),
            background_encoder,
        }
    }

    /// Process a single timestamped message.
    ///
    /// Messages are buffered and aligned by timestamp. Complete frames
    /// are written to the underlying writer.
    pub fn process_message(&mut self, msg: TimestampedMessage) -> Result<()> {
        self.stats.messages_processed += 1;

        // Check max frames limit
        if let Some(max) = self.config.max_frames
            && self.stats.frames_written >= max
        {
            return Ok(());
        }

        // Auto-start episode 0 on first message
        if !self.state.episode_started {
            self.start_episode(0)?;
        }

        // Check for episode boundary based on gap
        if let EpisodeStrategy::GapBased { threshold_ns } = &self.config.episode_strategy
            && let Some(last_ts) = self.state.last_timestamp_ns
            && msg.log_time > last_ts
            && msg.log_time - last_ts > *threshold_ns
        {
            self.finish_episode()?;
            self.start_episode(self.state.episode_index + 1)?;
        }

        // Check for episode boundary based on frame count
        if let EpisodeStrategy::FrameCount { max_frames } = &self.config.episode_strategy
            && self.state.frames_in_episode >= *max_frames
        {
            self.finish_episode()?;
            self.start_episode(self.state.episode_index + 1)?;
        }

        self.state.last_timestamp_ns = Some(msg.log_time);

        // Quick check: skip camera info messages we've already processed
        let is_camera_info = self.is_camera_info_message(&msg.data);
        if is_camera_info && !self.state.processed_camera_info.insert(msg.topic.clone()) {
            return Ok(());
        }

        let frame_interval_ns = self.config.streaming.frame_interval_ns();
        let frame_idx = msg.log_time / frame_interval_ns;
        let aligned_timestamp = frame_idx * frame_interval_ns;

        self.state
            .message_buffer
            .entry(aligned_timestamp)
            .or_default()
            .push(msg);

        if self.state.current_timestamp_ns.is_none() {
            self.state.current_timestamp_ns = Some(aligned_timestamp);
        }
        self.state.end_timestamp_ns =
            Some(aligned_timestamp.max(self.state.end_timestamp_ns.unwrap_or(0)));

        self.process_complete_frames()?;

        Ok(())
    }

    /// Process multiple messages in batch.
    pub fn process_messages(&mut self, messages: Vec<TimestampedMessage>) -> Result<()> {
        for msg in messages {
            self.process_message(msg)?;
        }
        Ok(())
    }

    /// Check if a message is a CameraInfo message.
    fn is_camera_info_message(&self, data: &CodecValue) -> bool {
        if let CodecValue::Struct(map) = data {
            map.contains_key("K") && map.contains_key("D")
        } else {
            false
        }
    }

    /// Process completed frames from the buffer.
    fn process_complete_frames(&mut self) -> Result<()> {
        let _frame_interval_ns = self.config.streaming.frame_interval_ns();
        let completion_window = self.config.streaming.completion_window_ns();

        while let Some(timestamp) = self.state.current_timestamp_ns {
            if let Some(messages) = self.state.message_buffer.remove(&timestamp) {
                match self.messages_to_frame(messages, timestamp) {
                    Ok(Some(frame)) => {
                        self.write_frame(frame)?;
                    }
                    Ok(None) => {
                        // Frame was empty, skip it
                    }
                    Err(e) => {
                        warn!(timestamp, error = %e, "Failed to create frame, skipping");
                    }
                }

                // Find next buffered timestamp within completion window
                self.state.current_timestamp_ns = self
                    .state
                    .message_buffer
                    .keys()
                    .copied()
                    .filter(|&t| t >= timestamp && t.saturating_sub(timestamp) <= completion_window)
                    .min();

                // If no more frames in window, advance to next buffered timestamp
                if self.state.current_timestamp_ns.is_none() {
                    self.state.current_timestamp_ns = self
                        .state
                        .message_buffer
                        .keys()
                        .copied()
                        .filter(|&t| t > timestamp)
                        .min();
                }
            } else {
                self.state.current_timestamp_ns = self
                    .state
                    .message_buffer
                    .keys()
                    .copied()
                    .filter(|&t| t > timestamp)
                    .min();
                break;
            }
        }

        Ok(())
    }

    /// Convert messages to an aligned frame.
    fn messages_to_frame(
        &mut self,
        messages: Vec<TimestampedMessage>,
        timestamp_ns: u64,
    ) -> Result<Option<AlignedFrame>> {
        let mut frame = AlignedFrame::new(self.state.frame_index, timestamp_ns);

        for msg in messages {
            self.process_message_for_frame(&mut frame, &msg)?;
        }

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
        let feature_name = if self.config.topic_mappings.is_empty() {
            msg.topic
                .replace('/', ".")
                .trim_start_matches('.')
                .to_string()
        } else {
            match self.config.topic_mappings.get(&msg.topic).cloned() {
                Some(feature) => feature,
                None => {
                    trace!(topic = %msg.topic, "Skipping unmapped topic");
                    return Ok(());
                }
            }
        };

        match &msg.data {
            CodecValue::Array(arr) => {
                let state: Vec<f32> = arr
                    .iter()
                    .filter_map(|v| match v {
                        CodecValue::Float32(n) => Some(*n),
                        CodecValue::Float64(n) => Some(*n as f32),
                        CodecValue::Int32(n) => Some(*n as f32),
                        CodecValue::Int64(n) => Some(*n as f32),
                        CodecValue::UInt32(n) => Some(*n as f32),
                        CodecValue::UInt64(n) => Some(*n as f32),
                        _ => None,
                    })
                    .collect();

                if !state.is_empty() {
                    if feature_name == "action" || feature_name.contains(".action") {
                        frame.add_action(feature_name, state);
                    } else {
                        frame.add_state(feature_name, state);
                    }
                }
            }
            CodecValue::Struct(map) => {
                // Skip CameraInfo (handled elsewhere)
                if map.contains_key("K") && map.contains_key("D") {
                    return Ok(());
                }

                if let Some(image_bytes) = extract_image_bytes(map) {
                    let width = map.get("width").and_then(extract_u32);
                    let height = map.get("height").and_then(extract_u32);

                    match detect_image_encoding_hint(map, &image_bytes) {
                        ImageEncodingHint::Encoded => {
                            let (resolved_width, resolved_height) =
                                resolve_encoded_dimensions(width, height, &image_bytes);
                            let image_data =
                                ImageData::encoded(resolved_width, resolved_height, image_bytes);

                            // Fast-path: send to background encoder, add only ImageRef to frame
                            if let Some(ref encoder) = self.background_encoder {
                                frame.add_image_ref(
                                    feature_name.clone(),
                                    ImageRef {
                                        width: resolved_width,
                                        height: resolved_height,
                                    },
                                );
                                encoder.send(EncodeRequest {
                                    camera: feature_name,
                                    images: vec![image_data],
                                })?;
                            } else {
                                frame.add_image_ref(
                                    feature_name,
                                    ImageRef {
                                        width: resolved_width,
                                        height: resolved_height,
                                    },
                                );
                            }
                            return Ok(());
                        }
                        ImageEncodingHint::Raw | ImageEncodingHint::Unknown => {
                            if let (Some(width), Some(height)) = (width, height) {
                                let image_data = ImageData::new_rgb(width, height, image_bytes)
                                    .map_err(|e| {
                                        roboflow_core::RoboflowError::other(format!(
                                            "Invalid image data: {}",
                                            e
                                        ))
                                    })?;

                                // Fast-path: send to background encoder, add only ImageRef to frame
                                if let Some(ref encoder) = self.background_encoder {
                                    frame.add_image_ref(
                                        feature_name.clone(),
                                        ImageRef { width, height },
                                    );
                                    encoder.send(EncodeRequest {
                                        camera: feature_name,
                                        images: vec![image_data],
                                    })?;
                                } else {
                                    frame.add_image_ref(feature_name, ImageRef { width, height });
                                }
                                return Ok(());
                            }
                        }
                    }
                }

                // Check for state data in struct
                if let Some(CodecValue::Array(position_arr)) = map.get("position") {
                    let state: Vec<f32> = position_arr
                        .iter()
                        .filter_map(|v| match v {
                            CodecValue::Float32(n) => Some(*n),
                            CodecValue::Float64(n) => Some(*n as f32),
                            CodecValue::Int32(n) => Some(*n as f32),
                            CodecValue::Int64(n) => Some(*n as f32),
                            CodecValue::UInt32(n) => Some(*n as f32),
                            CodecValue::UInt64(n) => Some(*n as f32),
                            _ => None,
                        })
                        .collect();

                    if !state.is_empty() {
                        if feature_name == "action" || feature_name.contains(".action") {
                            frame.add_action(feature_name, state);
                        } else {
                            frame.add_state(feature_name, state);
                        }
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }

    /// Write a frame to the writer.
    fn write_frame(&mut self, frame: AlignedFrame) -> Result<()> {
        debug!(
            frame_index = frame.frame_index,
            timestamp = frame.timestamp,
            images = frame.image_refs.len(),
            states = frame.states.len(),
            actions = frame.actions.len(),
            "Writing frame"
        );
        self.writer.write_frame(&frame)?;
        self.stats.frames_written += 1;
        self.state.frame_index += 1;

        if !frame.image_refs.is_empty() {
            self.state.frames_in_episode += 1;
        }

        Ok(())
    }

    /// Start a new episode.
    fn start_episode(&mut self, index: usize) -> Result<()> {
        if self.state.episode_started && self.state.episode_index != index {
            self.finish_episode()?;
        }

        self.state.episode_index = index;
        self.state.frames_in_episode = 0;
        self.state.episode_started = true;
        self.writer.start_episode(Some(index))?;

        debug!(episode_index = index, "Started episode");
        Ok(())
    }

    /// Finish the current episode.
    fn finish_episode(&mut self) -> Result<()> {
        if !self.state.episode_started {
            return Ok(());
        }

        self.writer.finish_episode()?;
        self.stats.episodes_written += 1;
        self.state.episode_started = false;

        debug!(
            episode_index = self.state.episode_index,
            frames = self.state.frames_in_episode,
            "Finished episode"
        );
        Ok(())
    }

    /// Finalize the executor and return statistics.
    pub fn finalize(mut self) -> Result<DatasetPipelineStats> {
        info!(
            messages = self.stats.messages_processed,
            frames_written = self.stats.frames_written,
            "Finalizing dataset pipeline"
        );

        // Flush remaining frames
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

        // Finish current episode
        if self.state.episode_started {
            self.finish_episode()?;
        }

        // Extract metadata from writer before finalize (if it's a LerobotWriter)
        if let Some(lerobot_writer) =
            self.writer
                .as_any()
                .downcast_ref::<roboflow_dataset::formats::lerobot::writer::LerobotWriter>()
        {
            let metadata = lerobot_writer.metadata();
            self.stats.state_dims = metadata.state_dims.clone();
            self.stats.image_shapes = metadata.image_shapes.clone();

            // Extract feature stats from the last episode (most recent stats)
            if let Some(last_stats) = metadata.episode_stats.last()
                && let Some(map) = last_stats.stats.as_object()
            {
                for (key, value) in map {
                    self.stats.feature_stats.insert(key.clone(), value.clone());
                }
            }
        }

        // Finish background encoder (if fast-path was active)
        if let Some(encoder) = self.background_encoder.take() {
            let result = encoder.finish()?;
            self.stats.images_encoded = result.stats.images_encoded;
            self.stats.skipped_frames = result.stats.skipped_frames;
            self.stats.encode_output_bytes = result.stats.output_bytes;
        }

        // Finalize writer
        let writer_stats = self.writer.finalize()?;
        self.stats.frames_written = writer_stats.frames_written;

        // Calculate final stats
        self.stats.duration_sec = self.state.start_time.elapsed().as_secs_f64();
        if self.stats.duration_sec > 0.0 {
            self.stats.fps = self.stats.frames_written as f64 / self.stats.duration_sec;
        }
        self.stats.policy_name = self.policy.name();

        info!(
            frames = self.stats.frames_written,
            episodes = self.stats.episodes_written,
            messages = self.stats.messages_processed,
            duration_sec = self.stats.duration_sec,
            fps = self.stats.fps,
            policy = self.stats.policy_name,
            "Dataset pipeline completed"
        );

        Ok(self.stats)
    }

    /// Get mutable reference to the writer.
    pub fn writer_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    /// Get reference to the writer.
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

    /// Get the policy name.
    pub fn policy_name(&self) -> &'static str {
        self.policy.name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use robocodec::CodecValue;
    use roboflow_dataset::core::stats::EpisodeStats;
    use roboflow_dataset::core::traits::WriterStats;
    use std::any::Any;

    /// Mock writer for testing.
    struct MockWriter {
        frame_count: usize,
        episodes_started: usize,
        episodes_finished: usize,
    }

    impl MockWriter {
        fn new() -> Self {
            Self {
                frame_count: 0,
                episodes_started: 0,
                episodes_finished: 0,
            }
        }
    }

    impl FormatWriter for MockWriter {
        fn write_frame(&mut self, _frame: &AlignedFrame) -> Result<()> {
            self.frame_count += 1;
            Ok(())
        }

        fn finalize(&mut self) -> Result<WriterStats> {
            Ok(WriterStats {
                frames_written: self.frame_count,
                images_encoded: 0,
                state_records: 0,
                output_bytes: 0,
                duration_sec: 0.0,
            })
        }

        fn frame_count(&self) -> usize {
            self.frame_count
        }

        fn start_episode(&mut self, _task_index: Option<usize>) -> Result<usize> {
            self.episodes_started += 1;
            Ok(self.episodes_started - 1)
        }

        fn finish_episode(&mut self) -> Result<EpisodeStats> {
            self.episodes_finished += 1;
            Ok(EpisodeStats::default())
        }

        fn supports_episodes(&self) -> bool {
            true
        }

        fn format_name(&self) -> &'static str {
            "mock"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    #[test]
    fn test_sequential_executor() {
        let writer = MockWriter::new();
        let config = DatasetPipelineConfig::with_fps(30);
        let executor = DatasetPipelineExecutor::sequential(writer, config);
        assert_eq!(executor.policy_name(), "sequential");
    }

    #[test]
    fn test_parallel_executor() {
        let writer = MockWriter::new();
        let config = DatasetPipelineConfig::with_fps(30);
        let executor = DatasetPipelineExecutor::parallel(writer, config, 2);
        assert_eq!(executor.policy_name(), "parallel");
    }

    #[test]
    fn test_config_builder() {
        let config = DatasetPipelineConfig::with_fps(60)
            .with_max_frames(1000)
            .with_topic_mapping("/camera", "observation.images.camera");

        assert_eq!(config.streaming.fps, 60);
        assert_eq!(config.max_frames, Some(1000));
        assert_eq!(
            config.topic_mappings.get("/camera"),
            Some(&"observation.images.camera".to_string())
        );
    }

    #[test]
    fn test_episode_strategy_default() {
        let strategy = EpisodeStrategy::default();
        assert!(matches!(strategy, EpisodeStrategy::Single));
    }

    fn make_executor_for_tests() -> DatasetPipelineExecutor<MockWriter, SequentialPolicy> {
        let writer = MockWriter::new();
        let config = DatasetPipelineConfig::with_fps(30);
        DatasetPipelineExecutor::sequential(writer, config)
    }

    #[test]
    fn test_struct_image_is_encoded_authoritative_accepts_zero_dims() {
        let executor = make_executor_for_tests();
        let mut frame = AlignedFrame::new(0, 0);

        let mut image_map = HashMap::new();
        image_map.insert("width".to_string(), CodecValue::UInt32(0));
        image_map.insert("height".to_string(), CodecValue::UInt32(0));
        image_map.insert("is_encoded".to_string(), CodecValue::Bool(true));
        image_map.insert(
            "data".to_string(),
            CodecValue::Bytes(vec![0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10]),
        );

        let msg = TimestampedMessage {
            topic: "/camera/image".to_string(),
            log_time: 0,
            data: CodecValue::Struct(image_map),
        };

        executor
            .process_message_for_frame(&mut frame, &msg)
            .expect("encoded message should be accepted");

        let _image_ref = frame
            .image_refs
            .get("camera.image")
            .expect("expected image ref in frame");
        // Previously asserted is_encoded; ImageRef only stores dimensions.
        // The key assertion is that the image was accepted despite zero dims.
    }

    #[test]
    fn test_struct_image_raw_rgb_size_mismatch_fails() {
        let executor = make_executor_for_tests();
        let mut frame = AlignedFrame::new(0, 0);

        let mut image_map = HashMap::new();
        image_map.insert("width".to_string(), CodecValue::UInt32(2));
        image_map.insert("height".to_string(), CodecValue::UInt32(2));
        image_map.insert("data".to_string(), CodecValue::Bytes(vec![42; 10]));

        let msg = TimestampedMessage {
            topic: "/camera/image".to_string(),
            log_time: 0,
            data: CodecValue::Struct(image_map),
        };

        let err = executor
            .process_message_for_frame(&mut frame, &msg)
            .expect_err("mismatched RGB size should fail");
        assert!(err.to_string().contains("Invalid image data"));
    }

    #[test]
    fn test_struct_image_format_hint_jpeg_treated_as_encoded_without_dims() {
        let executor = make_executor_for_tests();
        let mut frame = AlignedFrame::new(0, 0);

        let mut image_map = HashMap::new();
        image_map.insert("format".to_string(), CodecValue::String("jpeg".to_string()));
        image_map.insert(
            "data".to_string(),
            CodecValue::Bytes(vec![0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10]),
        );

        let msg = TimestampedMessage {
            topic: "/camera/image".to_string(),
            log_time: 0,
            data: CodecValue::Struct(image_map),
        };

        executor
            .process_message_for_frame(&mut frame, &msg)
            .expect("jpeg format hint should be treated as encoded");

        let _image_ref = frame
            .image_refs
            .get("camera.image")
            .expect("expected image ref in frame");
        // Previously asserted is_encoded; ImageRef only stores dimensions.
        // The key assertion is that JPEG format hint was treated as encoded.
    }
}
