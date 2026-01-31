// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Pipeline orchestrator - coordinates all pipeline stages.
//!
//! The orchestrator is responsible for:
//! - Creating channels for stage communication
//! - Spawning worker threads for each stage
//! - Coordinating graceful shutdown
//! - Collecting and reporting metrics

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tracing::{debug, info, instrument, warn};

use crate::stages::compression::{CompressionStage, CompressionStageConfig};
use crate::stages::reader::{ReaderStage, ReaderStageConfig, ReaderStats};
use crate::stages::transform::{TransformStage, TransformStageConfig};
use crate::stages::writer::{WriterStage, WriterStageConfig};
use robocodec::io::detection::detect_format;
use robocodec::io::metadata::{ChannelInfo as IoChannelInfo, FileFormat};
use robocodec::io::traits::{FormatReader, MessageChunkData};
use robocodec::transform::{ChannelInfo, MultiTransform};
use roboflow_core::{Result, RoboflowError};

/// Statistics from parallel reading (simplified version).
#[derive(Debug, Clone, Default)]
pub struct ParallelReaderStats {
    pub messages_read: u64,
    pub chunks_built: usize,
    pub total_bytes: u64,
    pub read_time_sec: f64,
    pub decompress_time_sec: f64,
    pub deserialize_time_sec: f64,
}

/// Simplified parallel reader configuration.
#[derive(Debug, Clone, Default)]
pub struct ParallelReaderConfig {
    pub num_threads: Option<usize>,
}

/// Convert ReaderStats to ParallelReaderStats for unified error handling.
fn reader_result_to_stats(result: Result<ReaderStats>) -> Result<ParallelReaderStats> {
    result.map(|stats| ParallelReaderStats {
        messages_read: stats.messages_read,
        chunks_built: stats.chunks_built as usize,
        total_bytes: stats.total_bytes,
        read_time_sec: 0.0,
        decompress_time_sec: 0.0,
        deserialize_time_sec: 0.0,
    })
}

/// Default channel capacity for backpressure handling.
///
/// This value balances memory usage with throughput. A smaller capacity
/// reduces memory buffering but may increase contention between stages.
///
/// With 16MB chunks:
/// - Capacity 16: ~320 MB total memory (256MB raw + 64MB compressed)
/// - Capacity 512: ~10 GB total memory (8GB raw + 2GB compressed)
const DEFAULT_CHANNEL_CAPACITY: usize = 16;

/// Configuration for the async pipeline.
#[derive(Debug)]
pub struct PipelineConfig {
    /// Input file path
    pub input_path: PathBuf,
    /// Output file path
    pub output_path: PathBuf,
    /// Reader stage configuration
    pub reader_config: ReaderStageConfig,
    /// Compression stage configuration
    pub compression_config: CompressionStageConfig,
    /// Writer stage configuration
    pub writer_config: WriterStageConfig,
    /// Parallel reader configuration (reserved for future use)
    pub parallel_reader_config: ParallelReaderConfig,
    /// Optional transform pipeline for schema/topic transformations
    pub transform_pipeline: Option<MultiTransform>,
    /// Channel capacity for inter-stage communication
    pub channel_capacity: usize,
}

// Manual Clone implementation since MultiTransform isn't cloneable
impl Clone for PipelineConfig {
    fn clone(&self) -> Self {
        // Note: transform_pipeline is not cloned (would be None in cloned config)
        Self {
            input_path: self.input_path.clone(),
            output_path: self.output_path.clone(),
            reader_config: self.reader_config.clone(),
            compression_config: self.compression_config.clone(),
            writer_config: self.writer_config.clone(),
            parallel_reader_config: self.parallel_reader_config.clone(),
            transform_pipeline: None, // Cannot clone MultiTransform
            channel_capacity: self.channel_capacity,
        }
    }
}

impl PipelineConfig {
    /// Create a new pipeline config.
    pub fn new<P: AsRef<Path>>(input_path: P, output_path: P) -> Self {
        Self {
            input_path: input_path.as_ref().to_path_buf(),
            output_path: output_path.as_ref().to_path_buf(),
            reader_config: ReaderStageConfig::default(),
            compression_config: CompressionStageConfig::default(),
            writer_config: WriterStageConfig::default(),
            parallel_reader_config: ParallelReaderConfig::default(),
            transform_pipeline: None,
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        }
    }

    /// Set the chunk size for the reader stage.
    pub fn with_chunk_size(mut self, size: usize) -> Self {
        self.reader_config.target_chunk_size = size;
        self.compression_config.target_chunk_size = size;
        self
    }

    /// Set the compression level.
    pub fn with_compression_level(mut self, level: i32) -> Self {
        self.compression_config.compression_level = level;
        self
    }

    /// Set the number of compression threads.
    pub fn with_threads(mut self, threads: usize) -> Self {
        self.compression_config.num_threads = threads;
        self
    }

    /// Set the compression backend.
    #[cfg(all(feature = "gpu", target_os = "macos"))]
    pub fn with_compression_backend(
        mut self,
        backend: crate::stages::compression::CompressionBackend,
    ) -> Self {
        self.compression_config.backend = backend;
        self
    }

    /// Set the channel capacity for inter-stage communication.
    ///
    /// Smaller values reduce memory usage but may reduce throughput.
    /// Larger values improve throughput but increase memory buffering.
    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }
}

/// Async zero-copy pipeline with clean architecture.
///
/// This pipeline uses separate stages for reading, compression, and writing,
/// connected by bounded channels for backpressure handling.
pub struct AsyncPipeline {
    config: PipelineConfig,
}

impl AsyncPipeline {
    /// Create a new async pipeline.
    pub fn new<P: AsRef<Path>>(input_path: P, output_path: P) -> Result<Self> {
        let config = PipelineConfig::new(input_path, output_path);
        Self::with_config(config)
    }

    /// Create a new async pipeline with explicit configuration.
    pub fn with_config(mut config: PipelineConfig) -> Result<Self> {
        // Validate input file exists
        if !config.input_path.exists() {
            return Err(RoboflowError::parse(
                "AsyncPipeline",
                format!("Input file not found: {}", config.input_path.display()),
            ));
        }

        // Auto-calculate optimal WindowLog from CPU cache
        if let Some(calculated_window_log) = Self::calculate_optimal_window_log(&config.input_path)?
        {
            config.compression_config.window_log = Some(calculated_window_log);
        }

        Ok(Self { config })
    }

    /// Calculate optimal WindowLog based on CPU L3 cache size.
    ///
    /// The optimal WindowLog is determined by CPU cache, not chunk size.
    /// - x86_64: Use CPUID to detect L3 cache size
    /// - aarch64 (Apple Silicon): 25 (32MB) is optimal
    /// - Other: 23 (8MB) safe default
    #[cfg(all(target_arch = "x86_64", feature = "cpuid"))]
    fn calculate_optimal_window_log(_input_path: &Path) -> Result<Option<u32>> {
        use raw_cpuid::CpuId;
        let cpuid = CpuId::new();

        // Attempt to get L3 cache size
        if let Some(cparams) = cpuid.get_cache_parameters() {
            for cache in cparams {
                // Level 3 Cache
                if cache.level() == 3 {
                    let cache_size_bytes =
                        cache.sets() * cache.associativity() * cache.coherency_line_size();
                    // Use half of L3 cache as Window size (to leave room for other data)
                    let window_size = cache_size_bytes / 2;
                    let window_log = (window_size as f64).log2().floor() as u32;
                    // Cap at 26 (64 MB) which is optimal before cache thrashing
                    let window_log = window_log.clamp(10, 26);
                    info!(
                        "Detected L3 cache: {} MB, using WindowLog: {} ({} MB)",
                        cache_size_bytes / 1024 / 1024,
                        window_log,
                        2u64.pow(window_log) / 1024 / 1024
                    );
                    return Ok(Some(window_log));
                }
            }
        }
        // Fallback if CPUID fails
        info!("Could not detect L3 cache, using default WindowLog: 26 (64 MB)");
        Ok(Some(26))
    }

    #[cfg(all(target_arch = "aarch64", target_vendor = "apple"))]
    fn calculate_optimal_window_log(_input_path: &Path) -> Result<Option<u32>> {
        // Apple Silicon (M1/M2/M3) has unified memory architecture
        // Optimal is 24-25 based on benchmarks
        info!("Apple Silicon detected, using WindowLog: 25 (32 MB)");
        Ok(Some(25))
    }

    #[cfg(all(target_arch = "aarch64", not(target_vendor = "apple")))]
    fn calculate_optimal_window_log(_input_path: &Path) -> Result<Option<u32>> {
        // Generic ARM - conservative default
        info!("ARM detected, using WindowLog: 24 (16 MB)");
        Ok(Some(24))
    }

    #[cfg(all(target_arch = "x86_64", not(feature = "cpuid")))]
    fn calculate_optimal_window_log(_input_path: &Path) -> Result<Option<u32>> {
        info!("x86_64 without cpuid feature, using WindowLog: 26 (64 MB)");
        Ok(Some(26))
    }

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    fn calculate_optimal_window_log(_input_path: &Path) -> Result<Option<u32>> {
        // Other architectures - safe default
        info!("Unknown architecture, using WindowLog: 23 (8 MB)");
        Ok(Some(23))
    }

    /// Run the pipeline to completion.
    #[instrument(skip_all, fields(
        input = %self.config.input_path.display(),
        output = %self.config.output_path.display(),
        compression_level = self.config.compression_config.compression_level,
        threads = self.config.compression_config.num_threads,
        has_transform = self.config.transform_pipeline.is_some(),
    ))]
    pub fn run(self) -> Result<PipelineReport> {
        let start = Instant::now();

        let has_transform = self.config.transform_pipeline.is_some();

        info!(
            input = %self.config.input_path.display(),
            output = %self.config.output_path.display(),
            compression_level = self.config.compression_config.compression_level,
            threads = self.config.compression_config.num_threads,
            chunk_size_mb = self.config.reader_config.target_chunk_size / (1024 * 1024),
            has_transform,
            "Starting async zero-copy pipeline"
        );

        // Get input file size
        let input_size = std::fs::metadata(&self.config.input_path)
            .map(|m| m.len())
            .unwrap_or(0);
        debug!(
            input_size_mb = input_size as f64 / (1024.0 * 1024.0),
            "Input file size"
        );

        // Detect file format
        let format = detect_format(&self.config.input_path)?;

        // Open reader to get channel info (needed for all code paths)
        let (channels, original_channel_count): (
            std::collections::HashMap<u16, IoChannelInfo>,
            usize,
        ) = match format {
            FileFormat::Mcap => {
                use robocodec::mcap::McapFormat;
                let reader = McapFormat::open(&self.config.input_path)?;
                let count = reader.channels().len();
                (reader.channels().clone(), count)
            }
            FileFormat::Bag => {
                use robocodec::bag::BagFormat;
                let reader = BagFormat::open(&self.config.input_path)?;
                let count = reader.channels().len();
                (reader.channels().clone(), count)
            }
            FileFormat::Unknown => {
                return Err(RoboflowError::parse(
                    "AsyncPipeline",
                    format!("Unknown file format: {}", self.config.input_path.display()),
                ));
            }
        };

        info!(channel_count = original_channel_count, "Opened input file");

        // Build channel info map for transform stage
        let channel_info_map: std::collections::HashMap<u16, ChannelInfo> = channels
            .iter()
            .map(|(id, ch)| {
                (
                    *id,
                    ChannelInfo {
                        id: ch.id,
                        topic: ch.topic.clone(),
                        message_type: ch.message_type.clone(),
                        encoding: ch.encoding.clone(),
                        schema: ch.schema.clone(),
                        schema_encoding: ch.schema_encoding.clone(),
                    },
                )
            })
            .collect();

        // Create channels for stage communication
        // If transform is enabled: reader -> transform -> compression -> writer
        // Otherwise: reader -> compression -> writer
        let capacity = self.config.channel_capacity;
        let (reader_to_transform, transform_receiver) = if has_transform {
            let (s, r) = crossbeam_channel::bounded::<MessageChunkData>(capacity);
            (Some(s), Some(r))
        } else {
            (None, None)
        };

        let (chunks_to_compress, chunks_receiver) = if has_transform {
            // Transform sends to compression
            crossbeam_channel::bounded::<MessageChunkData>(capacity)
        } else {
            // Reader sends to compression
            crossbeam_channel::bounded::<MessageChunkData>(capacity)
        };

        let (chunks_to_write, write_receiver) = crossbeam_channel::bounded(capacity);

        // Spawn writer stage with channel info for writing schemas/channels
        let writer_stage = WriterStage::new(
            self.config.writer_config.clone(),
            write_receiver,
            self.config.output_path.clone(),
            channels.clone(), // Pass channel info so writer can write schemas/channels first
        );
        let writer_handle = writer_stage.spawn()?;

        // Spawn compression stage
        let compression_stage = CompressionStage::new(
            self.config.compression_config.clone(),
            chunks_receiver,
            chunks_to_write.clone(),
        );
        let compression_handle = compression_stage.spawn()?;

        // Spawn transform stage if enabled
        // Returns the transform handle and the reader sender channel
        let (transform_handle, reader_sender) = if has_transform {
            let reader_channel = reader_to_transform.unwrap();
            let transform_stage = TransformStage::new(
                TransformStageConfig {
                    enabled: true,
                    verbose: false,
                },
                self.config.transform_pipeline,
                channel_info_map,
                transform_receiver.unwrap(),
                chunks_to_compress,
            );
            (Some(transform_stage.spawn()?), reader_channel)
        } else {
            (None, chunks_to_compress)
        };

        // Run reader with auto-detect for MCAP files
        //
        // Reading strategy:
        // Use sequential reader (parallel reader refactoring pending)
        // - MCAP files: Sequential reading
        // - Bag files: Sequential reading (no chunk offset information available)
        let reader_start = Instant::now();
        let reader_result = {
            debug!("Using sequential reader");
            let reader_stage = ReaderStage::new(
                self.config.reader_config.clone(),
                &self.config.input_path,
                channels.clone(),
                format,
                reader_sender,
            );
            reader_result_to_stats(reader_stage.run())
        };
        let reader_duration = reader_start.elapsed();

        // Wait for transform stage if enabled
        let transform_result = if let Some(handle) = transform_handle {
            Some(handle.join().map_err(|_| {
                RoboflowError::encode("AsyncPipeline", "Transform thread panicked".to_string())
            })?)
        } else {
            None
        };

        // Wait for compression stage to complete
        let compression_result = compression_handle.join().map_err(|_| {
            RoboflowError::encode("AsyncPipeline", "Compression thread panicked".to_string())
        })?;

        // Drop the chunks_to_write sender to signal writer to finish
        drop(chunks_to_write);

        // Wait for writer to complete
        let writer_result = writer_handle.join().map_err(|_| {
            RoboflowError::encode("AsyncPipeline", "Writer thread panicked".to_string())
        })?;

        // Check all results for errors
        let reader_stats = reader_result?;
        if let Some(Ok(transform_output)) = transform_result {
            info!(
                transformed_channels = transform_output.transformed_channels.len(),
                chunks_transformed = transform_output.chunks_received,
                "Transform stage completed"
            );
        } else if let Some(Err(e)) = transform_result {
            return Err(e);
        }
        compression_result?;
        let writer_stats = writer_result?;

        let duration = start.elapsed();

        // Log stage timing breakdown
        debug!(
            reader_duration_sec = reader_duration.as_secs_f64(),
            compression_writer_duration_sec = (duration - reader_duration).as_secs_f64(),
            "Stage timing breakdown"
        );

        // Get output file size
        let output_size = std::fs::metadata(&self.config.output_path)
            .map(|m| m.len())
            .unwrap_or(0);

        let compression_ratio = if input_size > 0 {
            output_size as f64 / input_size as f64
        } else {
            1.0
        };

        let throughput_mb_s = if duration.as_secs_f64() > 0.0 {
            (input_size as f64 / (1024.0 * 1024.0)) / duration.as_secs_f64()
        } else {
            0.0
        };

        info!(
            duration_sec = duration.as_secs_f64(),
            throughput_mb_s = throughput_mb_s,
            compression_ratio = compression_ratio,
            output_size_mb = output_size as f64 / (1024.0 * 1024.0),
            "Pipeline complete"
        );

        Ok(PipelineReport {
            input_file: self.config.input_path.display().to_string(),
            output_file: self.config.output_path.display().to_string(),
            input_size_bytes: input_size,
            output_size_bytes: output_size,
            duration,
            average_throughput_mb_s: throughput_mb_s,
            compression_ratio,
            threads_used: self.config.compression_config.num_threads,
            message_count: reader_stats.messages_read,
            data_bytes: reader_stats.total_bytes,
            chunks_written: writer_stats.chunks_written,
        })
    }
}

/// Performance report from a pipeline run.
#[derive(Debug, Clone)]
pub struct PipelineReport {
    /// Input file path
    pub input_file: String,
    /// Output file path
    pub output_file: String,
    /// Input file size in bytes
    pub input_size_bytes: u64,
    /// Output file size in bytes
    pub output_size_bytes: u64,
    /// Duration of the conversion
    pub duration: Duration,
    /// Average throughput in MB/s
    pub average_throughput_mb_s: f64,
    /// Compression ratio (output / input)
    pub compression_ratio: f64,
    /// Number of compression threads used
    pub threads_used: usize,
    /// Number of messages processed
    pub message_count: u64,
    /// Number of data bytes processed
    pub data_bytes: u64,
    /// Number of chunks written
    pub chunks_written: u64,
}

/// Builder for creating an async pipeline.
#[derive(Debug, Default)]
pub struct PipelineBuilder {
    input_path: Option<PathBuf>,
    output_path: Option<PathBuf>,
    chunk_size: Option<usize>,
    compression_level: Option<i32>,
    threads: Option<usize>,
    transform_pipeline: Option<MultiTransform>,
}

impl Clone for PipelineBuilder {
    fn clone(&self) -> Self {
        Self {
            input_path: self.input_path.clone(),
            output_path: self.output_path.clone(),
            chunk_size: self.chunk_size,
            compression_level: self.compression_level,
            threads: self.threads,
            transform_pipeline: None, // Cannot clone MultiTransform
        }
    }
}

impl PipelineBuilder {
    /// Create a new pipeline builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the input file path.
    pub fn input_path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.input_path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Set the output file path.
    pub fn output_path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.output_path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Set the target chunk size.
    pub fn chunk_size(mut self, size: usize) -> Self {
        self.chunk_size = Some(size);
        self
    }

    /// Set the compression level.
    pub fn compression_level(mut self, level: i32) -> Self {
        self.compression_level = Some(level);
        self
    }

    /// Set the number of compression threads.
    pub fn threads(mut self, threads: usize) -> Self {
        self.threads = Some(threads);
        self
    }

    /// Set the transform pipeline for schema/topic transformations.
    pub fn transform_pipeline(mut self, pipeline: MultiTransform) -> Self {
        self.transform_pipeline = Some(pipeline);
        self
    }

    /// Use high-throughput preset.
    pub fn high_throughput(self) -> Self {
        self.chunk_size(16 * 1024 * 1024).compression_level(1)
    }

    /// Use balanced storage preset.
    pub fn balanced_storage(self) -> Self {
        self.chunk_size(16 * 1024 * 1024).compression_level(3)
    }

    /// Use maximum throughput preset.
    pub fn max_throughput(self) -> Self {
        self.chunk_size(32 * 1024 * 1024).compression_level(1)
    }

    /// Build the pipeline.
    pub fn build(self) -> Result<AsyncPipeline> {
        let input_path = self
            .input_path
            .ok_or_else(|| RoboflowError::parse("PipelineBuilder", "Input path not set"))?;
        let output_path = self
            .output_path
            .ok_or_else(|| RoboflowError::parse("PipelineBuilder", "Output path not set"))?;

        let mut config = PipelineConfig::new(input_path, output_path);

        if let Some(chunk_size) = self.chunk_size {
            config = config.with_chunk_size(chunk_size);
        }
        if let Some(level) = self.compression_level {
            config = config.with_compression_level(level);
        }
        if let Some(threads) = self.threads {
            config = config.with_threads(threads);
        }

        config.transform_pipeline = self.transform_pipeline;

        AsyncPipeline::with_config(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_builder_default() {
        let builder = PipelineBuilder::new();
        assert!(builder.input_path.is_none());
        assert!(builder.output_path.is_none());
    }

    #[test]
    fn test_pipeline_builder_chaining() {
        let builder = PipelineBuilder::new()
            .input_path("/test/input.bag")
            .output_path("/test/output.mcap")
            .compression_level(5)
            .threads(8)
            .chunk_size(16 * 1024 * 1024);

        assert_eq!(builder.input_path, Some(PathBuf::from("/test/input.bag")));
        assert_eq!(
            builder.output_path,
            Some(PathBuf::from("/test/output.mcap"))
        );
        assert_eq!(builder.compression_level, Some(5));
    }
}
