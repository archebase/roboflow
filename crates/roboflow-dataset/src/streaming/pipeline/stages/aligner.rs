// Frame aligner stage - align messages by timestamp

use std::collections::HashSet;
use std::thread::{self, JoinHandle};
use std::time::Instant;

use crossbeam_channel::{Receiver, Sender};

use crate::streaming::alignment::{FrameAlignmentBuffer, TimestampedMessage};
use crate::streaming::pipeline::types::{
    DecodedMessage, PipelineError, PipelineResult, TransformableFrame,
};
use crate::streaming::pipeline::{PipelineConfig, StageStats};

/// Statistics from the frame aligner stage.
#[derive(Debug, Clone)]
pub struct AlignerStats {
    /// Total messages processed
    pub messages_processed: usize,
    /// Frames aligned
    pub frames_aligned: usize,
    /// Frames force-completed
    pub force_completed: usize,
    /// Peak buffer size
    pub peak_buffer_size: usize,
    /// Processing time in seconds
    pub duration_sec: f64,
}

/// The frame aligner stage.
///
/// Receives decoded messages and aligns them into frames by timestamp.
pub struct FrameAlignerStage {
    config: crate::streaming::pipeline::AlignerConfig,
    input_rx: Receiver<DecodedMessage>,
    output_tx: Sender<TransformableFrame>,
    /// Topic mappings (topic -> feature name)
    topic_mappings: std::collections::HashMap<String, String>,
}

impl FrameAlignerStage {
    /// Create a new frame aligner stage.
    pub fn new(
        config: crate::streaming::pipeline::AlignerConfig,
        input_rx: Receiver<DecodedMessage>,
        output_tx: Sender<TransformableFrame>,
    ) -> Self {
        Self {
            config,
            input_rx,
            output_tx,
            topic_mappings: std::collections::HashMap::new(),
        }
    }

    /// Set topic mappings.
    pub fn with_mappings(mut self, mappings: std::collections::HashMap<String, String>) -> Self {
        self.topic_mappings = mappings;
        self
    }

    /// Create from pipeline config.
    pub fn from_config(
        config: &PipelineConfig,
        input_rx: Receiver<DecodedMessage>,
        output_tx: Sender<TransformableFrame>,
    ) -> Self {
        let mut topic_mappings = std::collections::HashMap::new();

        // Build mappings from LeRobot config
        for mapping in &config.lerobot_config.mappings {
            topic_mappings.insert(mapping.topic.clone(), mapping.feature.clone());
        }

        Self::new(config.aligner.clone(), input_rx, output_tx).with_mappings(topic_mappings)
    }

    /// Spawn the aligner in a thread.
    pub fn spawn(self) -> JoinHandle<PipelineResult<(AlignerStats, StageStats)>> {
        thread::spawn(move || {
            let name = "FrameAligner";
            tracing::debug!(
                fps = self.config.fps,
                window_frames = self.config.completion_window_frames,
                "{name} starting"
            );

            let start = Instant::now();
            let result = self.run_internal();
            let duration = start.elapsed();

            match &result {
                Ok((aligner_stats, _stage_stats)) => {
                    tracing::debug!(
                        duration_sec = duration.as_secs_f64(),
                        messages = aligner_stats.messages_processed,
                        frames = aligner_stats.frames_aligned,
                        force_completed = aligner_stats.force_completed,
                        peak_buffer = aligner_stats.peak_buffer_size,
                        "{name} completed"
                    );
                }
                Err(e) => {
                    tracing::error!(error = %e, "{name} failed");
                }
            }

            result
        })
    }

    fn run_internal(&self) -> PipelineResult<(AlignerStats, StageStats)> {
        use crate::streaming::StreamingConfig;

        // Build streaming config from aligner config
        let stream_config = StreamingConfig::with_fps(self.config.fps)
            .with_completion_window(self.config.completion_window_frames)
            .with_max_buffered_frames(self.config.max_buffered_frames)
            .with_max_memory_mb(self.config.max_buffered_memory_mb);

        // Create frame alignment buffer
        let mut aligner = FrameAlignmentBuffer::new(stream_config.clone());
        let mut next_frame_index = 0usize;

        let mut messages_processed = 0usize;
        let mut frames_aligned = 0usize;
        let mut peak_buffer_size = 0usize;
        #[allow(unused_assignments)]
        let mut force_completed = 0usize;

        // Track seen topics for warning
        let mut seen_topics: HashSet<String> = HashSet::new();

        loop {
            match self.input_rx.recv() {
                Ok(decoded) => {
                    messages_processed += 1;

                    // Warn about unmapped topics once
                    if !self.topic_mappings.contains_key(&decoded.topic)
                        && seen_topics.insert(decoded.topic.clone())
                    {
                        tracing::warn!(
                            topic = %decoded.topic,
                            "Message from unmapped topic will be ignored"
                        );
                        continue;
                    }

                    // Convert to TimestampedMessage
                    // decoded.data is CodecValue::Struct(HashMap<String, CodecValue>)
                    // Extract the HashMap for TimestampedMessage
                    use robocodec::CodecValue;
                    let message_map = match decoded.data {
                        CodecValue::Struct(map) => map,
                        other => {
                            tracing::warn!(
                                topic = %decoded.topic,
                                data_type = ?std::mem::discriminant(&other),
                                "Message data is not a Struct, skipping"
                            );
                            continue;
                        }
                    };

                    let timestamped = TimestampedMessage {
                        log_time: decoded.log_time,
                        message: message_map,
                    };

                    // Get feature name for this topic
                    if let Some(feature_name) = self.topic_mappings.get(&decoded.topic) {
                        // Process through aligner
                        let completed_frames = aligner.process_message(&timestamped, feature_name);

                        // Track buffer size
                        peak_buffer_size = peak_buffer_size.max(aligner.len());

                        // Send completed frames
                        for frame in completed_frames {
                            let transformable = TransformableFrame {
                                frame_index: next_frame_index,
                                timestamp: frame.timestamp,
                                aligned_data: frame,
                            };

                            self.output_tx.send(transformable).map_err(|e| {
                                PipelineError::ChannelError {
                                    from: "Aligner".to_string(),
                                    to: "Transformer".to_string(),
                                    reason: e.to_string(),
                                }
                            })?;

                            frames_aligned += 1;
                            next_frame_index += 1;
                        }
                    }

                    // Progress logging
                    if messages_processed.is_multiple_of(10000) {
                        tracing::debug!(
                            messages = messages_processed,
                            frames = frames_aligned,
                            buffer = aligner.len(),
                            "Aligner progress"
                        );
                    }
                }
                Err(_) => {
                    // Channel closed - flush remaining frames
                    let remaining = aligner.flush();
                    force_completed = remaining.len();

                    for frame in remaining {
                        let transformable = TransformableFrame {
                            frame_index: next_frame_index,
                            timestamp: frame.timestamp,
                            aligned_data: frame,
                        };

                        self.output_tx.send(transformable).map_err(|e| {
                            PipelineError::ChannelError {
                                from: "Aligner".to_string(),
                                to: "Transformer".to_string(),
                                reason: e.to_string(),
                            }
                        })?;

                        frames_aligned += 1;
                        next_frame_index += 1;
                    }
                    break;
                }
            }
        }

        Ok((
            AlignerStats {
                messages_processed,
                frames_aligned,
                force_completed,
                peak_buffer_size,
                duration_sec: 0.0, // Set by caller
            },
            StageStats {
                stage: "FrameAligner".to_string(),
                items_processed: messages_processed,
                items_produced: frames_aligned,
                duration_sec: 0.0, // Set by caller
                peak_memory_mb: None,
                metrics: [
                    (
                        "force_completed".to_string(),
                        serde_json::json!(force_completed),
                    ),
                    (
                        "peak_buffer_size".to_string(),
                        serde_json::json!(peak_buffer_size),
                    ),
                ]
                .into_iter()
                .collect(),
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_aligner_config_default() {
        let config = crate::streaming::pipeline::AlignerConfig::default();
        assert_eq!(config.fps, 30);
        assert_eq!(config.completion_window_frames, 3);
    }

    #[test]
    fn test_aligner_completion_window_ns() {
        let config = crate::streaming::pipeline::AlignerConfig::default();
        // 30 fps = 33.33ms per frame, 3 frames = 100ms
        assert_eq!(config.completion_window_ns(), 100_000_000);
    }
}
