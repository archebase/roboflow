// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Testable pipeline runner for executing conversion pipelines.
//!
//! This module extracts the core pipeline execution logic from TaskExecutor
//! to make it testable and benchmarkable without TiKV/job registry dependencies.

use std::time::{Duration, Instant};

use roboflow_core::{Result, RoboflowError, TimestampedMessage};
use roboflow_dataset::{DatasetWriter, PipelineExecutor};
use roboflow_sources::{Source, SourceConfig};
use tokio_util::sync::CancellationToken;

/// Statistics from a pipeline run.
#[derive(Debug, Clone)]
pub struct PipelineRunStats {
    /// Total frames written
    pub frames_written: usize,
    /// Total messages processed
    pub messages_processed: usize,
    /// Total duration from start to finish
    pub total_duration: Duration,
    /// Time spent reading from source
    pub read_time: Duration,
    /// Time spent processing messages (includes encoding)
    pub process_time: Duration,
    /// Time spent finalizing (includes video encoding flush)
    pub finalize_time: Duration,
}

impl PipelineRunStats {
    /// Calculate throughput in frames per second
    pub fn fps(&self) -> f64 {
        if self.total_duration.as_secs_f64() > 0.0 {
            self.frames_written as f64 / self.total_duration.as_secs_f64()
        } else {
            0.0
        }
    }

    /// Calculate the percentage of time spent reading
    pub fn read_percentage(&self) -> f64 {
        if self.total_duration.as_secs_f64() > 0.0 {
            self.read_time.as_secs_f64() / self.total_duration.as_secs_f64() * 100.0
        } else {
            0.0
        }
    }

    /// Calculate the percentage of time spent processing
    pub fn process_percentage(&self) -> f64 {
        if self.total_duration.as_secs_f64() > 0.0 {
            self.process_time.as_secs_f64() / self.total_duration.as_secs_f64() * 100.0
        } else {
            0.0
        }
    }
}

/// A testable pipeline runner that executes conversion pipelines.
///
/// This struct extracts the core pipeline execution logic from TaskExecutor,
/// making it usable in tests and benchmarks without TiKV or job registry
/// dependencies.
///
/// # Example
///
/// ```ignore
/// use roboflow_distributed::worker::PipelineRunner;
/// use roboflow_dataset::{PipelineConfig, PipelineExecutor};
/// use roboflow_dataset::lerobot::LerobotWriter;
///
/// let runner = PipelineRunner::new();
/// let writer = LerobotWriter::new_local(output_dir, config)?;
/// let executor = PipelineExecutor::new(writer, pipeline_config);
///
/// let stats = runner.run(&mut source, executor, &source_config, None).await?;
/// println!("Processed {} frames in {:?}", stats.frames_written, stats.total_duration);
/// ```
pub struct PipelineRunner {
    batch_size: usize,
}

impl Default for PipelineRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineRunner {
    /// Create a new pipeline runner with default settings.
    pub fn new() -> Self {
        Self { batch_size: 1000 }
    }

    /// Set the batch size for reading messages.
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Run the pipeline with the given source and executor.
    ///
    /// This is the core pipeline execution loop extracted from TaskExecutor.
    /// It reads messages from the source, processes them through the executor,
    /// and returns detailed timing statistics.
    ///
    /// # Arguments
    ///
    /// * `source` - The data source to read from
    /// * `executor` - The pipeline executor with writer
    /// * `source_config` - Configuration for the source
    /// * `cancel_token` - Optional cancellation token
    ///
    /// # Returns
    ///
    /// Pipeline run statistics including timing breakdowns
    pub async fn run<W: DatasetWriter>(
        &self,
        source: &mut dyn Source,
        mut executor: PipelineExecutor<W>,
        source_config: &SourceConfig,
        cancel_token: Option<CancellationToken>,
    ) -> Result<PipelineRunStats> {
        let start_time = Instant::now();
        let mut read_time = Duration::ZERO;
        let mut process_time = Duration::ZERO;

        // Initialize source
        source.initialize(source_config).await.map_err(|e| {
            RoboflowError::other(format!("Source initialization failed: {}", e))
        })?;

        // Process messages
        loop {
            // Check cancellation
            if let Some(ref token) = cancel_token {
                if token.is_cancelled() {
                    return Err(RoboflowError::other("Cancelled".to_string()));
                }
            }

            // Read batch
            let read_start = Instant::now();
            let batch_result = source.read_batch(self.batch_size).await;
            read_time += read_start.elapsed();

            match batch_result {
                Ok(Some(messages)) if !messages.is_empty() => {
                    // Process batch
                    let process_start = Instant::now();
                    for msg in messages {
                        executor.process_message(msg)?;
                    }
                    process_time += process_start.elapsed();
                }
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(e) => {
                    return Err(RoboflowError::other(format!("Source read failed: {}", e)));
                }
            }
        }

        // Finalize
        let finalize_start = Instant::now();
        let pipeline_stats = executor.finalize()?;
        let finalize_time = finalize_start.elapsed();

        let total_duration = start_time.elapsed();

        Ok(PipelineRunStats {
            frames_written: pipeline_stats.frames_written,
            messages_processed: pipeline_stats.messages_processed,
            total_duration,
            read_time,
            process_time,
            finalize_time,
        })
    }

    /// Run the pipeline with a source that produces pre-loaded messages.
    ///
    /// This is useful for benchmarks where you want to measure encoding time
    /// without I/O overhead from reading files.
    ///
    /// # Arguments
    ///
    /// * `messages` - Pre-loaded messages to process
    /// * `executor` - The pipeline executor with writer
    /// * `cancel_token` - Optional cancellation token
    ///
    /// # Returns
    ///
    /// Pipeline run statistics
    pub fn run_with_messages<W: DatasetWriter>(
        &self,
        messages: Vec<TimestampedMessage>,
        mut executor: PipelineExecutor<W>,
        cancel_token: Option<CancellationToken>,
    ) -> Result<PipelineRunStats> {
        let start_time = Instant::now();
        let mut process_time = Duration::ZERO;

        // Process all messages
        for chunk in messages.chunks(self.batch_size) {
            // Check cancellation
            if let Some(ref token) = cancel_token {
                if token.is_cancelled() {
                    return Err(RoboflowError::other("Cancelled".to_string()));
                }
            }

            // Process batch
            let process_start = Instant::now();
            for msg in chunk {
                executor.process_message(msg.clone())?;
            }
            process_time += process_start.elapsed();
        }

        // Finalize
        let finalize_start = Instant::now();
        let pipeline_stats = executor.finalize()?;
        let finalize_time = finalize_start.elapsed();

        let total_duration = start_time.elapsed();

        Ok(PipelineRunStats {
            frames_written: pipeline_stats.frames_written,
            messages_processed: pipeline_stats.messages_processed,
            total_duration,
            read_time: Duration::ZERO, // No I/O when using pre-loaded messages
            process_time,
            finalize_time,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use roboflow_core::CodecValue;

    fn create_test_messages(count: usize) -> Vec<TimestampedMessage> {
        (0..count)
            .map(|i| TimestampedMessage {
                topic: "/test/topic".to_string(),
                log_time: i as u64 * 1_000_000_000, // 1 second intervals
                data: CodecValue::String(format!("message_{}", i)),
            })
            .collect()
    }

    #[test]
    fn test_pipeline_stats_calculations() {
        let stats = PipelineRunStats {
            frames_written: 100,
            messages_processed: 1000,
            total_duration: Duration::from_secs(10),
            read_time: Duration::from_secs(3),
            process_time: Duration::from_secs(5),
            finalize_time: Duration::from_secs(2),
        };

        assert_eq!(stats.fps(), 10.0);
        assert_eq!(stats.read_percentage(), 30.0);
        assert_eq!(stats.process_percentage(), 50.0);
    }

    #[test]
    fn test_pipeline_stats_zero_duration() {
        let stats = PipelineRunStats {
            frames_written: 0,
            messages_processed: 0,
            total_duration: Duration::ZERO,
            read_time: Duration::ZERO,
            process_time: Duration::ZERO,
            finalize_time: Duration::ZERO,
        };

        assert_eq!(stats.fps(), 0.0);
        assert_eq!(stats.read_percentage(), 0.0);
        assert_eq!(stats.process_percentage(), 0.0);
    }
}
