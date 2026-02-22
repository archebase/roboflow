// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Frame alignment module.
//!
//! This module provides frame alignment functionality for synchronizing
//! messages from different topics to aligned output frames.
//!
//! # Components
//!
//! - [`FrameAligner`] - High-level API for frame alignment with pluggable processing
//! - [`FrameAlignmentBuffer`] - Low-level buffer for frame alignment
//! - [`FrameProcessor`] - Trait for custom frame processing
//! - [`FrameCompletionCriteria`] - Criteria for frame completion
//! - [`AlignmentStats`] - Statistics tracking
//!
//! # Example
//!
//! ```rust,ignore
//! use roboflow_dataset::formats::alignment::{
//!     FrameAligner, FrameProcessor, PassthroughProcessor, StreamingConfig,
//! };
//!
//! // Create a frame aligner with default (passthrough) processor
//! let config = StreamingConfig::with_fps(30);
//! let processor = PassthroughProcessor::new();
//! let mut aligner = FrameAligner::new(config, Box::new(processor));
//!
//! // Add messages and get completed frames
//! let frames = aligner.add_messages(messages)?;
//!
//! // Flush any remaining frames
//! let remaining = aligner.flush()?;
//! ```

use std::collections::HashMap;

use roboflow_core::Result;

use crate::formats::common::AlignedFrame;

pub mod buffer;
pub mod completion;
pub mod config;
pub mod processor;
pub mod stats;

// Re-export commonly used types
pub use buffer::{FrameAlignmentBuffer, PartialFrame, TimestampedMessage};
pub use completion::FrameCompletionCriteria;
pub use config::StreamingConfig;
pub use processor::{FrameProcessor, PassthroughProcessor, ProcessedFrame, ProcessorStats};
pub use stats::AlignmentStats;

/// High-level frame aligner with pluggable processing.
///
/// This struct provides a clean API for frame alignment that combines:
/// - A [`FrameAlignmentBuffer`] for buffering and aligning messages
/// - A [`FrameProcessor`] for custom frame processing
///
/// # Example
///
/// ```rust,ignore
/// use roboflow_dataset::formats::alignment::{
///     FrameAligner, PassthroughProcessor, StreamingConfig,
/// };
///
/// let config = StreamingConfig::with_fps(30);
/// let mut aligner = FrameAligner::new(config, Box::new(PassthroughProcessor::new()));
///
/// // Process messages
/// for msg in messages {
///     let frames = aligner.process_message(&msg, &feature_name)?;
///     for frame in frames {
///         // Handle completed frame
///     }
/// }
///
/// // Flush remaining frames
/// let final_frames = aligner.flush()?;
/// let stats = aligner.finalize()?;
/// ```
pub struct FrameAligner {
    /// The underlying buffer for frame alignment
    buffer: FrameAlignmentBuffer,
    /// The processor for custom frame handling
    processor: Box<dyn FrameProcessor>,
    /// Statistics from alignment
    alignment_stats: AlignmentStats,
    /// Statistics from processing
    processor_stats: ProcessorStats,
    /// Topic to feature name mappings
    topic_mappings: HashMap<String, String>,
}

impl FrameAligner {
    /// Create a new frame aligner with the given configuration and processor.
    ///
    /// # Arguments
    ///
    /// * `config` - Streaming configuration for frame alignment
    /// * `processor` - Custom frame processor (use [`PassthroughProcessor`] for no-op)
    pub fn new(config: StreamingConfig, processor: Box<dyn FrameProcessor>) -> Self {
        let buffer = FrameAlignmentBuffer::new(config);
        Self {
            buffer,
            processor,
            alignment_stats: AlignmentStats::new(),
            processor_stats: ProcessorStats::new(),
            topic_mappings: HashMap::new(),
        }
    }

    /// Create a new frame aligner with custom completion criteria.
    ///
    /// # Arguments
    ///
    /// * `config` - Streaming configuration for frame alignment
    /// * `criteria` - Custom completion criteria
    /// * `processor` - Custom frame processor
    pub fn with_completion_criteria(
        config: StreamingConfig,
        criteria: FrameCompletionCriteria,
        processor: Box<dyn FrameProcessor>,
    ) -> Self {
        let buffer = FrameAlignmentBuffer::with_completion_criteria(config, criteria);
        Self {
            buffer,
            processor,
            alignment_stats: AlignmentStats::new(),
            processor_stats: ProcessorStats::new(),
            topic_mappings: HashMap::new(),
        }
    }

    /// Set topic to feature name mappings.
    ///
    /// This allows the aligner to map incoming topic names to feature names.
    pub fn with_topic_mappings(mut self, mappings: HashMap<String, String>) -> Self {
        self.topic_mappings = mappings;
        self
    }

    /// Add a single topic mapping.
    pub fn with_topic_mapping(mut self, topic: impl Into<String>, feature: impl Into<String>) -> Self {
        self.topic_mappings.insert(topic.into(), feature.into());
        self
    }

    /// Get the feature name for a topic.
    ///
    /// Returns the mapped name if configured, otherwise converts the topic
    /// to a feature name by replacing '/' with '.' and trimming leading '.'.
    pub fn get_feature_name<'a>(&'a self, topic: &'a str) -> std::borrow::Cow<'a, str> {
        if let Some(mapped) = self.topic_mappings.get(topic) {
            std::borrow::Cow::Borrowed(mapped)
        } else {
            let mut s = topic.replace('/', ".");
            if s.starts_with('.') {
                s = s.trim_start_matches('.').to_string();
            }
            std::borrow::Cow::Owned(s)
        }
    }

    /// Process a single timestamped message.
    ///
    /// This method:
    /// 1. Aligns the message to a frame boundary
    /// 2. Checks for completed frames
    /// 3. Processes completed frames through the processor
    ///
    /// Returns any frames that are ready for writing.
    pub fn process_message(
        &mut self,
        msg: &TimestampedMessage,
        feature_name: &str,
    ) -> Result<Vec<ProcessedFrame>> {
        let completed = self.buffer.process_message(msg, feature_name);
        self.process_completed_frames(completed)
    }

    /// Add multiple messages with their feature names and return completed frames.
    ///
    /// This is a convenience method for processing multiple messages at once.
    /// Each message must be paired with its feature name.
    pub fn add_messages_with_features(
        &mut self,
        messages: Vec<(String, TimestampedMessage)>,
    ) -> Result<Vec<ProcessedFrame>> {
        let mut all_processed = Vec::new();

        for (feature_name, msg) in messages {
            let completed = self.buffer.process_message(&msg, &feature_name);
            let processed = self.process_completed_frames(completed)?;
            all_processed.extend(processed);
        }

        Ok(all_processed)
    }

    /// Process completed frames through the processor.
    fn process_completed_frames(
        &mut self,
        frames: Vec<AlignedFrame>,
    ) -> Result<Vec<ProcessedFrame>> {
        let mut processed = Vec::with_capacity(frames.len());

        for frame in frames {
            match self.processor.process(frame) {
                Ok(p) => {
                    if p.should_write {
                        self.processor_stats.record_success();
                    } else {
                        self.processor_stats.record_skip();
                    }
                    processed.push(p);
                }
                Err(e) => {
                    self.processor_stats.record_error();
                    tracing::warn!(error = %e, "Frame processing failed");
                }
            }
        }

        Ok(processed)
    }

    /// Flush all remaining frames (end of stream).
    ///
    /// This should be called when all messages have been processed
    /// to get any remaining buffered frames.
    pub fn flush(&mut self) -> Result<Vec<ProcessedFrame>> {
        let completed = self.buffer.flush();
        self.process_completed_frames(completed)
    }

    /// Finalize the aligner and return processing statistics.
    ///
    /// This calls `finalize` on the processor and returns the combined stats.
    pub fn finalize(&mut self) -> Result<AlignerStats> {
        let processor_stats = self.processor.finalize()?;
        Ok(AlignerStats {
            alignment: self.alignment_stats.clone(),
            processing: processor_stats,
        })
    }

    /// Get a reference to the alignment statistics.
    pub fn stats(&self) -> &AlignmentStats {
        self.buffer.stats()
    }

    /// Get the number of frames currently in the buffer.
    pub fn buffer_len(&self) -> usize {
        self.buffer.len()
    }

    /// Check if the buffer is empty.
    pub fn is_buffer_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Estimate memory usage in bytes.
    pub fn estimated_memory_bytes(&self) -> usize {
        self.buffer.estimated_memory_bytes()
    }
}

/// Combined statistics from alignment and processing.
#[derive(Debug, Clone)]
pub struct AlignerStats {
    /// Alignment statistics
    pub alignment: AlignmentStats,
    /// Processing statistics
    pub processing: ProcessorStats,
}

impl AlignerStats {
    /// Create new stats.
    pub fn new() -> Self {
        Self {
            alignment: AlignmentStats::new(),
            processing: ProcessorStats::new(),
        }
    }
}

impl Default for AlignerStats {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_aligner_new() {
        let config = StreamingConfig::with_fps(30);
        let processor = Box::new(PassthroughProcessor::new());
        let aligner = FrameAligner::new(config, processor);

        assert!(aligner.is_buffer_empty());
        assert_eq!(aligner.buffer_len(), 0);
    }

    #[test]
    fn test_frame_aligner_get_feature_name() {
        let config = StreamingConfig::with_fps(30);
        let processor = Box::new(PassthroughProcessor::new());
        let aligner = FrameAligner::new(config, processor);

        // Test direct mapping
        let feature = aligner.get_feature_name("/camera/image_raw");
        assert_eq!(feature, "camera.image_raw");

        // Test with custom mapping
        let aligner = FrameAligner::new(StreamingConfig::with_fps(30), Box::new(PassthroughProcessor::new()))
            .with_topic_mapping("/camera/image_raw", "observation.images.cam_0");

        let feature = aligner.get_feature_name("/camera/image_raw");
        assert_eq!(feature, "observation.images.cam_0");
    }

    #[test]
    fn test_frame_aligner_flush_empty() {
        let config = StreamingConfig::with_fps(30);
        let processor = Box::new(PassthroughProcessor::new());
        let mut aligner = FrameAligner::new(config, processor);

        let frames = aligner.flush().unwrap();
        assert!(frames.is_empty());
    }

    #[test]
    fn test_aligner_stats() {
        let stats = AlignerStats::new();
        assert_eq!(stats.alignment.frames_processed, 0);
        assert_eq!(stats.processing.frames_processed, 0);
    }
}
