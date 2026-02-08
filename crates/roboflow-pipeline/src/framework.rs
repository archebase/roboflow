// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Pipeline framework using Source/Sink abstractions.
//!
//! This module provides a unified pipeline orchestrator that works with
//! the pluggable Source and Sink traits, enabling flexible data processing
//! without being tied to specific file formats.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use roboflow_core::{Result, RoboflowError};
use roboflow_sinks::{
    kps::KpsSink, lerobot::LerobotSink, DatasetFrame, ImageData, ImageFormat, Sink, SinkConfig,
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
        let sink: Box<dyn Sink> =
            match &config.sink.sink_type {
                SinkType::Lerobot { path } => Box::new(LerobotSink::new(path).map_err(|e| {
                    RoboflowError::other(format!("Failed to create LeRobot sink: {}", e))
                })?),
                SinkType::Kps { path } => Box::new(KpsSink::new(path).map_err(|e| {
                    RoboflowError::other(format!("Failed to create KPS sink: {}", e))
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
        let mut episode_index = 0usize;
        let mut frame_index = 0usize;
        let mut last_checkpoint_time = Instant::now();

        // Episode detection: gap in timestamps (in nanoseconds)
        // If gap > 1 second, consider it a new episode
        let episode_gap_ns = 1_000_000_000u64;

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
                    // Check for episode gap
                    if timestamp > end_timestamp_ns.unwrap_or(0) + episode_gap_ns && frame_index > 0
                    {
                        // New episode
                        episode_index += 1;
                        frame_index = 0;
                    }

                    // Create frame from all messages at this timestamp
                    let frame =
                        self.messages_to_frame(messages, frame_index, episode_index, timestamp)?;

                    self.sink
                        .write_frame(frame)
                        .await
                        .map_err(|e| RoboflowError::other(format!("Write failed: {e}")))?;

                    frame_index += 1;
                    frames_written += 1;

                    // Simple episode boundary: every 1000 frames
                    if frame_index >= 1000 {
                        frame_index = 0;
                        episode_index += 1;
                    }

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

        // Process any remaining buffered messages
        while let Some((timestamp, messages)) = message_buffer.drain().next() {
            if !messages.is_empty() {
                // Check for episode gap
                if timestamp > end_timestamp_ns.unwrap_or(0) + episode_gap_ns && frame_index > 0 {
                    episode_index += 1;
                    frame_index = 0;
                }

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
                robocodec::CodecValue::Array(arr) => {
                    // Convert CodecValue array to Vec<f32>
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
                        frame.observation_state = Some(state);
                    }
                }
                robocodec::CodecValue::Struct(map) => {
                    // Look for image data
                    if let Some(robocodec::CodecValue::Bytes(data)) = map.get("data") {
                        // Extract image dimensions if available
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

                        let feature_name = self
                            .config
                            .topic_mappings
                            .get(&msg.topic)
                            .cloned()
                            .unwrap_or_else(|| {
                                // Generate feature name from topic
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
                                data: data.clone(),
                                format: ImageFormat::Rgb8,
                            },
                        );
                    }
                }
                _ => {}
            }
        }

        Ok(frame)
    }
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
