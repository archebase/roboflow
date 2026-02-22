// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Unified pipeline executor with pluggable execution policy.
//!
//! This module provides [`PipelineExecutor`] which combines frame alignment,
//! processing, and execution policy into a unified interface.

use std::collections::HashMap;
use std::time::Instant;

use roboflow_core::Result;

use crate::policy::ExecutionPolicy;

/// Statistics from pipeline execution.
#[derive(Debug, Clone, Default)]
pub struct PipelineExecutorStats {
    /// Total frames processed
    pub frames_processed: usize,
    /// Total frames written
    pub frames_written: usize,
    /// Frames that had errors
    pub frames_errored: usize,
    /// Processing time in seconds
    pub duration_sec: f64,
    /// Frames per second throughput
    pub fps: f64,
}

/// A frame ready for processing.
///
/// This represents an aligned frame that needs to be processed
/// by a frame processor.
#[derive(Debug, Clone)]
pub struct FrameForProcessing {
    /// Frame index
    pub index: usize,
    /// Frame timestamp (nanoseconds)
    pub timestamp: u64,
    /// Episode index
    pub episode_index: usize,
    /// Raw frame data (generic, to be defined by processor)
    pub data: Vec<u8>,
}

/// A processed frame ready for output.
///
/// This is the result of processing a frame through a [`FrameProcessor`].
#[derive(Debug, Clone)]
pub struct ProcessedFrameOutput {
    /// Original frame index
    pub index: usize,
    /// Whether the frame should be written
    pub should_write: bool,
    /// Processed data (format-dependent)
    pub data: Vec<u8>,
}

/// Trait for processing frames.
///
/// Implement this trait to define how frames are processed
/// (e.g., writing to a dataset format).
pub trait FrameProcessor: Send + Sync {
    /// Process a single frame.
    fn process(&mut self, frame: FrameForProcessing) -> Result<ProcessedFrameOutput>;

    /// Start a new episode.
    fn start_episode(&mut self, episode_index: usize) -> Result<()>;

    /// Finish the current episode.
    fn finish_episode(&mut self) -> Result<()>;

    /// Finalize processing and return final stats.
    fn finalize(&mut self) -> Result<PipelineExecutorStats>;
}

/// Trait for transforming frames (CPU-bound work that can be parallelized).
///
/// This is separate from [`FrameProcessor`] because transformations
/// (like image decoding) can be done in parallel, while writing
/// is typically sequential.
pub trait FrameTransformer: Send + Sync + Clone {
    /// Transform a frame's data.
    fn transform(&self, frame: &FrameForProcessing) -> Result<Vec<u8>>;
}

/// A no-op frame transformer.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoOpTransformer;

impl FrameTransformer for NoOpTransformer {
    fn transform(&self, _frame: &FrameForProcessing) -> Result<Vec<u8>> {
        Ok(vec![])
    }
}

/// Configuration for the unified pipeline executor.
#[derive(Debug, Clone)]
pub struct PipelineExecutorConfig {
    /// Frame interval in nanoseconds
    pub frame_interval_ns: u64,
    /// Maximum frames to process (None = unlimited)
    pub max_frames: Option<usize>,
    /// Episode management strategy
    pub episode_strategy: EpisodeStrategy,
    /// Topic to feature mappings
    pub topic_mappings: HashMap<String, String>,
}

impl Default for PipelineExecutorConfig {
    fn default() -> Self {
        Self {
            frame_interval_ns: 33_333_333, // ~30 FPS
            max_frames: None,
            episode_strategy: EpisodeStrategy::Single,
            topic_mappings: HashMap::new(),
        }
    }
}

impl PipelineExecutorConfig {
    /// Create a new config with the specified frame rate.
    pub fn with_fps(fps: u32) -> Self {
        Self {
            frame_interval_ns: 1_000_000_000u64 / fps as u64,
            ..Default::default()
        }
    }
}

/// Episode management strategy.
#[derive(Debug, Clone, Default)]
pub enum EpisodeStrategy {
    /// Single episode for entire stream
    #[default]
    Single,
    /// Split episodes when timestamp gap exceeds threshold
    GapBased { threshold_ns: u64 },
    /// Fixed number of frames per episode
    FrameCount { max_frames: usize },
}

/// Unified pipeline executor with pluggable execution policy.
///
/// This executor combines:
/// - Frame transformation (via [`FrameTransformer`], can be parallelized)
/// - Frame processing (via [`FrameProcessor`], typically sequential)
/// - Execution policy (sequential or parallel via [`ExecutionPolicy`])
///
/// # Execution Model
///
/// 1. Frames are received in batches
/// 2. Frame transformations (if any) are applied using the execution policy
///    (parallel or sequential)
/// 3. Transformed frames are processed by the frame processor (sequential)
///
/// # Example
///
/// ```rust,ignore
/// use roboflow_executor::policy::SequentialPolicy;
/// use roboflow_executor::PipelineExecutor;
///
/// let processor = MyProcessor::new();
/// let config = PipelineExecutorConfig::with_fps(30);
/// let mut executor = PipelineExecutor::new(processor, config, SequentialPolicy);
///
/// // Process frames
/// executor.process_batch(frames)?;
///
/// // Finalize
/// let stats = executor.finalize()?;
/// ```
pub struct PipelineExecutor<P: ExecutionPolicy, T: FrameTransformer = NoOpTransformer> {
    /// Frame processor (handles writing)
    processor: Box<dyn FrameProcessor>,
    /// Frame transformer (CPU-bound work, can be parallelized)
    transformer: T,
    /// Configuration
    config: PipelineExecutorConfig,
    /// Execution policy
    policy: P,
    /// Statistics
    stats: PipelineExecutorStats,
    /// Current episode index
    episode_index: usize,
    /// Whether an episode has been started
    episode_started: bool,
    /// Start time
    start_time: Instant,
}

impl<P: ExecutionPolicy> PipelineExecutor<P, NoOpTransformer> {
    /// Create a new pipeline executor without frame transformation.
    ///
    /// # Arguments
    ///
    /// * `processor` - Frame processor implementation
    /// * `config` - Executor configuration
    /// * `policy` - Execution policy (sequential or parallel)
    pub fn new(
        processor: Box<dyn FrameProcessor>,
        config: PipelineExecutorConfig,
        policy: P,
    ) -> Self {
        Self {
            processor,
            transformer: NoOpTransformer,
            config,
            policy,
            stats: PipelineExecutorStats::default(),
            episode_index: 0,
            episode_started: false,
            start_time: Instant::now(),
        }
    }
}

impl<P: ExecutionPolicy, T: FrameTransformer> PipelineExecutor<P, T> {
    /// Create a new pipeline executor with frame transformation.
    ///
    /// # Arguments
    ///
    /// * `processor` - Frame processor implementation
    /// * `transformer` - Frame transformer for CPU-bound work
    /// * `config` - Executor configuration
    /// * `policy` - Execution policy (sequential or parallel)
    pub fn with_transformer(
        processor: Box<dyn FrameProcessor>,
        transformer: T,
        config: PipelineExecutorConfig,
        policy: P,
    ) -> Self {
        Self {
            processor,
            transformer,
            config,
            policy,
            stats: PipelineExecutorStats::default(),
            episode_index: 0,
            episode_started: false,
            start_time: Instant::now(),
        }
    }

    /// Process a batch of frames.
    ///
    /// Frame transformations are applied using the execution policy,
    /// then processed sequentially by the frame processor.
    pub fn process_batch(&mut self, frames: Vec<FrameForProcessing>) -> Result<()> {
        // Ensure episode is started
        if !self.episode_started {
            self.start_episode(0)?;
        }

        // Check max frames limit
        if let Some(max) = self.config.max_frames
            && self.stats.frames_processed >= max
        {
            return Ok(());
        }

        // Transform frames using the policy (can be parallel)
        let transformed: Vec<(FrameForProcessing, Result<Vec<u8>>)> = self
            .policy
            .execute_batch(frames, |frame| {
                let transformed_data = self.transformer.transform(&frame);
                (frame, transformed_data)
            });

        // Process transformed frames sequentially
        for (mut frame, transform_result) in transformed {
            self.stats.frames_processed += 1;

            match transform_result {
                Ok(data) => {
                    frame.data = data;

                    match self.processor.process(frame) {
                        Ok(output) => {
                            if output.should_write {
                                self.stats.frames_written += 1;
                            }
                        }
                        Err(e) => {
                            self.stats.frames_errored += 1;
                            tracing::warn!(error = %e, "Frame processing failed");
                        }
                    }
                }
                Err(e) => {
                    self.stats.frames_errored += 1;
                    tracing::warn!(error = %e, "Frame transformation failed");
                }
            }
        }

        Ok(())
    }

    /// Start a new episode.
    pub fn start_episode(&mut self, index: usize) -> Result<()> {
        if self.episode_started && self.episode_index != index {
            self.processor.finish_episode()?;
        }

        self.episode_index = index;
        self.episode_started = true;
        self.processor.start_episode(index)?;

        tracing::info!(episode_index = index, "Started episode");
        Ok(())
    }

    /// Finish the current episode.
    pub fn finish_episode(&mut self) -> Result<()> {
        if self.episode_started {
            self.processor.finish_episode()?;
            self.episode_started = false;
            tracing::info!(episode_index = self.episode_index, "Finished episode");
        }
        Ok(())
    }

    /// Finalize the executor and return statistics.
    pub fn finalize(&mut self) -> Result<PipelineExecutorStats> {
        // Finish current episode
        if self.episode_started
            && let Err(e) = self.processor.finish_episode()
        {
            tracing::warn!(error = %e, "Failed to finish episode during finalize");
        }

        // Get stats from processor
        let mut final_stats = self.processor.finalize()?;

        // Add our stats
        final_stats.frames_processed = self.stats.frames_processed;
        final_stats.frames_written = self.stats.frames_written;
        final_stats.frames_errored = self.stats.frames_errored;
        final_stats.duration_sec = self.start_time.elapsed().as_secs_f64();

        if final_stats.duration_sec > 0.0 {
            final_stats.fps = final_stats.frames_processed as f64 / final_stats.duration_sec;
        }

        tracing::info!(
            frames_processed = final_stats.frames_processed,
            frames_written = final_stats.frames_written,
            frames_errored = final_stats.frames_errored,
            duration_sec = final_stats.duration_sec,
            fps = final_stats.fps,
            policy = %self.policy.name(),
            "Pipeline finalized"
        );

        Ok(final_stats)
    }

    /// Get the current frame count.
    pub fn frame_count(&self) -> usize {
        self.stats.frames_processed
    }

    /// Get the current episode index.
    pub fn episode_index(&self) -> usize {
        self.episode_index
    }

    /// Get the policy name.
    pub fn policy_name(&self) -> &'static str {
        self.policy.name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestProcessor {
        frames_processed: usize,
        episodes_started: usize,
        episodes_finished: usize,
    }

    impl TestProcessor {
        fn new() -> Self {
            Self {
                frames_processed: 0,
                episodes_started: 0,
                episodes_finished: 0,
            }
        }
    }

    impl FrameProcessor for TestProcessor {
        fn process(&mut self, _frame: FrameForProcessing) -> Result<ProcessedFrameOutput> {
            self.frames_processed += 1;
            Ok(ProcessedFrameOutput {
                index: self.frames_processed - 1,
                should_write: true,
                data: vec![],
            })
        }

        fn start_episode(&mut self, _episode_index: usize) -> Result<()> {
            self.episodes_started += 1;
            Ok(())
        }

        fn finish_episode(&mut self) -> Result<()> {
            self.episodes_finished += 1;
            Ok(())
        }

        fn finalize(&mut self) -> Result<PipelineExecutorStats> {
            Ok(PipelineExecutorStats::default())
        }
    }

    #[test]
    fn test_pipeline_executor_sequential() {
        let processor = Box::new(TestProcessor::new());
        let config = PipelineExecutorConfig::with_fps(30);
        let mut executor = PipelineExecutor::new(processor, config, crate::policy::SequentialPolicy);

        let frames = vec![
            FrameForProcessing {
                index: 0,
                timestamp: 0,
                episode_index: 0,
                data: vec![],
            },
            FrameForProcessing {
                index: 1,
                timestamp: 33_333_333,
                episode_index: 0,
                data: vec![],
            },
        ];

        executor.process_batch(frames).unwrap();
        let stats = executor.finalize().unwrap();

        assert_eq!(stats.frames_processed, 2);
        assert_eq!(stats.frames_written, 2);
    }

    #[test]
    fn test_pipeline_executor_parallel() {
        let processor = Box::new(TestProcessor::new());
        let config = PipelineExecutorConfig::with_fps(30);
        let mut executor = PipelineExecutor::new(
            processor,
            config,
            crate::policy::ParallelPolicy::new(2),
        );

        let frames: Vec<FrameForProcessing> = (0..10)
            .map(|i| FrameForProcessing {
                index: i,
                timestamp: i as u64 * 33_333_333,
                episode_index: 0,
                data: vec![],
            })
            .collect();

        executor.process_batch(frames).unwrap();
        let stats = executor.finalize().unwrap();

        assert_eq!(stats.frames_processed, 10);
        assert_eq!(stats.frames_written, 10);
    }

    #[test]
    fn test_pipeline_executor_episode_management() {
        let processor = Box::new(TestProcessor::new());
        let config = PipelineExecutorConfig::default();
        let mut executor = PipelineExecutor::new(processor, config, crate::policy::SequentialPolicy);

        executor.start_episode(0).unwrap();
        executor.start_episode(1).unwrap();
        executor.finish_episode().unwrap();

        let stats = executor.finalize().unwrap();
        assert_eq!(stats.frames_processed, 0);
    }

    #[test]
    fn test_pipeline_config_with_fps() {
        let config = PipelineExecutorConfig::with_fps(30);
        assert_eq!(config.frame_interval_ns, 33_333_333);

        let config = PipelineExecutorConfig::with_fps(60);
        assert_eq!(config.frame_interval_ns, 16_666_666);
    }
}
