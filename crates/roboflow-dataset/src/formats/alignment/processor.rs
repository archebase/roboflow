// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Frame processor trait for pluggable frame processing.
//!
//! This module provides the [`FrameProcessor`] trait which allows custom
//! processing of aligned frames before they are written to the dataset.

use roboflow_core::Result;

use crate::formats::common::AlignedFrame;

/// Statistics from frame processing.
#[derive(Debug, Clone, Default)]
pub struct ProcessorStats {
    /// Number of frames processed
    pub frames_processed: usize,
    /// Number of frames that had errors
    pub frames_errored: usize,
    /// Number of frames skipped (e.g., empty frames)
    pub frames_skipped: usize,
    /// Total processing time in milliseconds
    pub total_processing_time_ms: f64,
}

impl ProcessorStats {
    /// Create new empty stats.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a successfully processed frame.
    pub fn record_success(&mut self) {
        self.frames_processed += 1;
    }

    /// Record a frame that had an error.
    pub fn record_error(&mut self) {
        self.frames_errored += 1;
    }

    /// Record a skipped frame.
    pub fn record_skip(&mut self) {
        self.frames_skipped += 1;
    }

    /// Add processing time.
    pub fn add_processing_time(&mut self, time_ms: f64) {
        self.total_processing_time_ms += time_ms;
    }

    /// Get total frames (processed + errored + skipped).
    pub fn total_frames(&self) -> usize {
        self.frames_processed + self.frames_errored + self.frames_skipped
    }

    /// Get success rate (0.0 - 1.0).
    pub fn success_rate(&self) -> f64 {
        let total = self.total_frames();
        if total > 0 {
            self.frames_processed as f64 / total as f64
        } else {
            1.0
        }
    }
}

/// A processed frame ready for writing.
///
/// This is the output of a [`FrameProcessor`] and contains
/// the frame data along with any additional metadata.
#[derive(Debug, Clone)]
pub struct ProcessedFrame {
    /// The aligned frame data
    pub frame: AlignedFrame,
    /// Optional task index for multi-task datasets
    pub task_index: Option<usize>,
    /// Whether this frame should be written
    pub should_write: bool,
}

impl ProcessedFrame {
    /// Create a new processed frame.
    pub fn new(frame: AlignedFrame) -> Self {
        Self {
            frame,
            task_index: None,
            should_write: true,
        }
    }

    /// Set the task index.
    pub fn with_task_index(mut self, index: usize) -> Self {
        self.task_index = Some(index);
        self
    }

    /// Mark this frame as not to be written (e.g., filtered out).
    pub fn skip(mut self) -> Self {
        self.should_write = false;
        self
    }
}

/// Trait for processing aligned frames.
///
/// Implement this trait to customize frame processing, such as:
/// - Filtering frames based on content
/// - Transforming frame data
/// - Adding metadata
///
/// # Example
///
/// ```rust,ignore
/// use roboflow_dataset::formats::alignment::{FrameProcessor, ProcessedFrame, ProcessorStats};
/// use roboflow_core::Result;
///
/// struct MyProcessor;
///
/// impl FrameProcessor for MyProcessor {
///     fn process(&mut self, frame: AlignedFrame) -> Result<ProcessedFrame> {
///         // Custom processing logic
///         Ok(ProcessedFrame::new(frame))
///     }
///
///     fn finalize(&mut self) -> Result<ProcessorStats> {
///         Ok(ProcessorStats::default())
///     }
/// }
/// ```
pub trait FrameProcessor: Send + Sync {
    /// Process a single aligned frame.
    ///
    /// Returns a [`ProcessedFrame`] which may be modified or filtered.
    fn process(&mut self, frame: AlignedFrame) -> Result<ProcessedFrame>;

    /// Finalize processing and return statistics.
    ///
    /// This is called when all frames have been processed.
    fn finalize(&mut self) -> Result<ProcessorStats>;
}

/// A no-op frame processor that passes frames through unchanged.
///
/// This is useful when you want to use the frame aligner without
/// any custom processing.
#[derive(Debug, Clone, Default)]
pub struct PassthroughProcessor {
    stats: ProcessorStats,
}

impl PassthroughProcessor {
    /// Create a new passthrough processor.
    pub fn new() -> Self {
        Self::default()
    }
}

impl FrameProcessor for PassthroughProcessor {
    fn process(&mut self, frame: AlignedFrame) -> Result<ProcessedFrame> {
        self.stats.record_success();
        Ok(ProcessedFrame::new(frame))
    }

    fn finalize(&mut self) -> Result<ProcessorStats> {
        Ok(std::mem::take(&mut self.stats))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_processor_stats() {
        let mut stats = ProcessorStats::new();
        stats.record_success();
        stats.record_success();
        stats.record_error();
        stats.record_skip();

        assert_eq!(stats.frames_processed, 2);
        assert_eq!(stats.frames_errored, 1);
        assert_eq!(stats.frames_skipped, 1);
        assert_eq!(stats.total_frames(), 4);
        assert!((stats.success_rate() - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_processor_stats_empty() {
        let stats = ProcessorStats::new();
        assert_eq!(stats.total_frames(), 0);
        assert_eq!(stats.success_rate(), 1.0);
    }

    #[test]
    fn test_processed_frame() {
        let frame = AlignedFrame::new(0, 1000);
        let processed = ProcessedFrame::new(frame.clone())
            .with_task_index(42)
            .skip();

        assert_eq!(processed.task_index, Some(42));
        assert!(!processed.should_write);
    }

    #[test]
    fn test_passthrough_processor() {
        let mut processor = PassthroughProcessor::new();
        let frame = AlignedFrame::new(0, 1000);

        let result = processor.process(frame).unwrap();
        assert!(result.should_write);
        assert!(result.task_index.is_none());

        let stats = processor.finalize().unwrap();
        assert_eq!(stats.frames_processed, 1);
    }
}
