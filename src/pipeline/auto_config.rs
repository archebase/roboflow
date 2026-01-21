//! Automatic pipeline configuration with hardware-aware tuning.
//!
//! This module provides intelligent auto-configuration for robocodec pipelines
//! based on detected hardware capabilities and performance targets.

use crate::pipeline::hardware::HardwareInfo;
use std::path::{Path, PathBuf};
use tracing::{debug, info};

/// Performance mode for the pipeline.
///
/// Controls the trade-off between throughput, latency, and memory usage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PerformanceMode {
    /// **Throughput** - Aggressive tuning for maximum throughput on beefy machines.
    ///
    /// Uses larger batches, more threads, and higher buffer counts to maximize
    /// data processing speed. Best for:
    /// - Server-grade hardware with 16+ cores
    /// - Batch processing of large files
    /// - When throughput matters more than memory usage
    Throughput,

    /// **Balanced** - Middle ground between throughput and resource usage.
    ///
    /// Default mode that works well for most systems.
    #[default]
    Balanced,

    /// **MemoryEfficient** - Conserve memory at the cost of some throughput.
    ///
    /// Uses smaller batches and fewer buffers. Best for:
    /// - Systems with limited memory
    /// - Running alongside other memory-intensive workloads
    MemoryEfficient,
}

impl PerformanceMode {
    /// Get the ZSTD compression level for this performance mode.
    pub fn compression_level(&self) -> i32 {
        match self {
            PerformanceMode::Throughput => 1,      // Fastest
            PerformanceMode::Balanced => 3,        // Good balance
            PerformanceMode::MemoryEfficient => 3, // Same as balanced
        }
    }

    /// Batch size multiplier relative to suggested size.
    pub fn batch_multiplier(&self) -> f64 {
        match self {
            PerformanceMode::Throughput => 2.0,      // 2x batch size
            PerformanceMode::Balanced => 1.0,        // 1x batch size
            PerformanceMode::MemoryEfficient => 0.5, // 0.5x batch size
        }
    }

    /// Channel capacity multiplier.
    pub fn channel_multiplier(&self) -> f64 {
        match self {
            PerformanceMode::Throughput => 2.0,
            PerformanceMode::Balanced => 1.0,
            PerformanceMode::MemoryEfficient => 0.5,
        }
    }

    /// Whether to reserve CPU cores for other stages.
    pub fn reserve_cores(&self) -> usize {
        match self {
            PerformanceMode::Throughput => 4, // Reserve for other stages
            PerformanceMode::Balanced => 2,
            PerformanceMode::MemoryEfficient => 1,
        }
    }
}

/// Automatic pipeline configuration.
///
/// This struct holds configuration values that can be either auto-detected
/// or manually overridden by the user.
#[derive(Debug, Clone)]
pub struct PipelineAutoConfig {
    /// Detected hardware information.
    pub hardware: HardwareInfo,
    /// Performance mode for tuning.
    pub mode: PerformanceMode,
    /// Compression threads (None = auto-detect).
    pub compression_threads: Option<usize>,
    /// Batch/chunk size in bytes (None = auto-detect).
    pub batch_size_bytes: Option<usize>,
    /// Channel capacity for inter-stage communication (None = auto-detect).
    pub channel_capacity: Option<usize>,
    /// Parser threads (None = auto-detect).
    pub parser_threads: Option<usize>,
    /// Batcher threads (None = auto-detect).
    pub batcher_threads: Option<usize>,
    /// Transform threads (None = auto-detect).
    pub transform_threads: Option<usize>,
    /// Packetizer threads (None = auto-detect).
    pub packetizer_threads: Option<usize>,
    /// ZSTD compression level (None = use mode default).
    pub compression_level: Option<i32>,
    /// Prefetch block size (None = auto-detect).
    pub prefetch_block_size: Option<usize>,
    /// Writer buffer size (None = auto-detect).
    pub writer_buffer_size: Option<usize>,
}

impl PipelineAutoConfig {
    /// Create a new auto-config with the given performance mode.
    ///
    /// All values are auto-detected based on hardware.
    pub fn auto(mode: PerformanceMode) -> Self {
        let hardware = HardwareInfo::detect();

        info!(
            mode = ?mode,
            cpu_cores = hardware.cpu_cores,
            memory_gb = hardware.total_memory_gb(),
            l3_cache_mb = hardware.l3_cache_mb(),
            "Creating auto-config"
        );

        Self {
            hardware,
            mode,
            compression_threads: None,
            batch_size_bytes: None,
            channel_capacity: None,
            parser_threads: None,
            batcher_threads: None,
            transform_threads: None,
            packetizer_threads: None,
            compression_level: None,
            prefetch_block_size: None,
            writer_buffer_size: None,
        }
    }

    /// Create a new auto-config in Throughput mode (aggressive tuning).
    pub fn throughput() -> Self {
        Self::auto(PerformanceMode::Throughput)
    }

    /// Create a new auto-config in Balanced mode.
    pub fn balanced() -> Self {
        Self::auto(PerformanceMode::Balanced)
    }

    /// Create a new auto-config in MemoryEfficient mode.
    pub fn memory_efficient() -> Self {
        Self::auto(PerformanceMode::MemoryEfficient)
    }

    /// Override the compression thread count.
    pub fn with_compression_threads(mut self, threads: usize) -> Self {
        self.compression_threads = Some(threads);
        self
    }

    /// Override the batch size.
    pub fn with_batch_size(mut self, bytes: usize) -> Self {
        self.batch_size_bytes = Some(bytes);
        self
    }

    /// Override the channel capacity.
    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = Some(capacity);
        self
    }

    /// Override the parser thread count.
    pub fn with_parser_threads(mut self, threads: usize) -> Self {
        self.parser_threads = Some(threads);
        self
    }

    /// Override the batcher thread count.
    pub fn with_batcher_threads(mut self, threads: usize) -> Self {
        self.batcher_threads = Some(threads);
        self
    }

    /// Override the transform thread count.
    pub fn with_transform_threads(mut self, threads: usize) -> Self {
        self.transform_threads = Some(threads);
        self
    }

    /// Override the packetizer thread count.
    pub fn with_packetizer_threads(mut self, threads: usize) -> Self {
        self.packetizer_threads = Some(threads);
        self
    }

    /// Override the compression level.
    pub fn with_compression_level(mut self, level: i32) -> Self {
        self.compression_level = Some(level);
        self
    }

    /// Override the prefetch block size.
    pub fn with_prefetch_block_size(mut self, bytes: usize) -> Self {
        self.prefetch_block_size = Some(bytes);
        self
    }

    /// Override the writer buffer size.
    pub fn with_writer_buffer_size(mut self, bytes: usize) -> Self {
        self.writer_buffer_size = Some(bytes);
        self
    }

    // ========================================================================
    // Computed values (resolves auto-detection with overrides)
    // ========================================================================

    /// Get the effective compression thread count.
    pub fn effective_compression_threads(&self) -> usize {
        let result = self.compression_threads.unwrap_or_else(|| {
            let reserve = self.mode.reserve_cores();
            (self.hardware.cpu_cores.saturating_sub(reserve)).max(2)
        });

        debug!(
            compression_threads = result,
            cpu_cores = self.hardware.cpu_cores,
            reserved = self.mode.reserve_cores(),
            "Effective compression threads"
        );

        result
    }

    /// Get the effective batch size.
    pub fn effective_batch_size(&self) -> usize {
        self.batch_size_bytes.unwrap_or_else(|| {
            let suggested = self.hardware.suggested_batch_size();
            let multiplier = self.mode.batch_multiplier();
            ((suggested as f64) * multiplier) as usize
        })
    }

    /// Get the effective channel capacity.
    pub fn effective_channel_capacity(&self) -> usize {
        self.channel_capacity.unwrap_or_else(|| {
            let suggested = self.hardware.suggested_channel_capacity();
            let multiplier = self.mode.channel_multiplier();
            ((suggested as f64) * multiplier) as usize
        })
    }

    /// Get the effective parser thread count.
    pub fn effective_parser_threads(&self) -> usize {
        self.parser_threads
            .unwrap_or_else(|| self.hardware.suggested_stage_threads())
    }

    /// Get the effective batcher thread count.
    pub fn effective_batcher_threads(&self) -> usize {
        self.batcher_threads
            .unwrap_or_else(|| self.hardware.suggested_stage_threads())
    }

    /// Get the effective transform thread count.
    pub fn effective_transform_threads(&self) -> usize {
        self.transform_threads
            .unwrap_or_else(|| self.hardware.suggested_stage_threads())
    }

    /// Get the effective packetizer thread count.
    pub fn effective_packetizer_threads(&self) -> usize {
        self.packetizer_threads
            .unwrap_or_else(|| self.hardware.suggested_stage_threads())
    }

    /// Get the effective compression level.
    pub fn effective_compression_level(&self) -> i32 {
        self.compression_level
            .unwrap_or_else(|| self.mode.compression_level())
    }

    /// Get the effective prefetch block size (scales with batch size).
    pub fn effective_prefetch_block_size(&self) -> usize {
        self.prefetch_block_size.unwrap_or_else(|| {
            let batch_size = self.effective_batch_size();
            // Prefetch block size is 1/4 of batch size, minimum 1MB
            (batch_size / 4).max(1024 * 1024)
        })
    }

    /// Get the effective writer buffer size.
    pub fn effective_writer_buffer_size(&self) -> usize {
        self.writer_buffer_size.unwrap_or({
            match self.mode {
                PerformanceMode::Throughput => 16 * 1024 * 1024, // 16MB
                PerformanceMode::Balanced => 8 * 1024 * 1024,    // 8MB
                PerformanceMode::MemoryEfficient => 4 * 1024 * 1024, // 4MB
            }
        })
    }

    /// Create a HyperPipelineConfig from this auto-config.
    pub fn to_hyper_config(
        &self,
        input_path: impl AsRef<Path>,
        output_path: impl AsRef<Path>,
    ) -> HyperPipelineConfigBuilder {
        HyperPipelineConfigBuilder::from_auto_config(self, input_path, output_path)
    }

    /// Print configuration summary (useful for debugging).
    pub fn summarize(&self) -> String {
        format!(
            "=== Pipeline Auto-Config ===\n\
             Mode: {:?}\n\
             Hardware: {} cores, {:.1} GB RAM{}\n\
             --- Effective Values ---\n\
             Compression threads: {}\n\
             Batch size: {:.1} MB\n\
             Channel capacity: {}\n\
             Parser threads: {}\n\
             Batcher threads: {}\n\
             Transform threads: {}\n\
             Packetizer threads: {}\n\
             Compression level: {}\n\
             Prefetch block size: {:.1} MB\n\
             Writer buffer: {:.1} MB",
            self.mode,
            self.hardware.cpu_cores,
            self.hardware.total_memory_gb(),
            self.hardware
                .l3_cache_mb()
                .map(|mb| format!(", {:.0} MB L3", mb))
                .unwrap_or_default(),
            self.effective_compression_threads(),
            self.effective_batch_size() as f64 / (1024.0 * 1024.0),
            self.effective_channel_capacity(),
            self.effective_parser_threads(),
            self.effective_batcher_threads(),
            self.effective_transform_threads(),
            self.effective_packetizer_threads(),
            self.effective_compression_level(),
            self.effective_prefetch_block_size() as f64 / (1024.0 * 1024.0),
            self.effective_writer_buffer_size() as f64 / (1024.0 * 1024.0),
        )
    }
}

impl Default for PipelineAutoConfig {
    fn default() -> Self {
        Self::balanced()
    }
}

/// Builder for creating HyperPipelineConfig from PipelineAutoConfig.
pub struct HyperPipelineConfigBuilder {
    /// Input file path.
    pub input_path: PathBuf,
    /// Output file path.
    pub output_path: PathBuf,
    /// Prefetch block size.
    pub prefetch_block_size: usize,
    /// Parser threads.
    pub parser_threads: usize,
    /// Batcher config.
    pub batcher_threads: usize,
    pub batch_size: usize,
    /// Transform threads.
    pub transform_threads: usize,
    /// Compression config.
    pub compression_threads: usize,
    pub compression_level: i32,
    /// Packetizer threads.
    pub packetizer_threads: usize,
    /// Writer buffer size.
    pub writer_buffer_size: usize,
    /// Channel capacity.
    pub channel_capacity: usize,
}

impl HyperPipelineConfigBuilder {
    fn from_auto_config(
        config: &PipelineAutoConfig,
        input_path: impl AsRef<Path>,
        output_path: impl AsRef<Path>,
    ) -> Self {
        Self {
            input_path: input_path.as_ref().to_path_buf(),
            output_path: output_path.as_ref().to_path_buf(),
            prefetch_block_size: config.effective_prefetch_block_size(),
            parser_threads: config.effective_parser_threads(),
            batcher_threads: config.effective_batcher_threads(),
            batch_size: config.effective_batch_size(),
            transform_threads: config.effective_transform_threads(),
            compression_threads: config.effective_compression_threads(),
            compression_level: config.effective_compression_level(),
            packetizer_threads: config.effective_packetizer_threads(),
            writer_buffer_size: config.effective_writer_buffer_size(),
            channel_capacity: config.effective_channel_capacity(),
        }
    }

    /// Build the actual HyperPipelineConfig.
    pub fn build(self) -> crate::pipeline::hyper::HyperPipelineConfig {
        use crate::pipeline::hyper::config::{
            BatcherConfig, CompressionConfig, PacketizerConfig, ParserConfig, PrefetcherConfig,
            TransformConfig, WriterConfig,
        };

        info!(
            input = %self.input_path.display(),
            output = %self.output_path.display(),
            compression_threads = self.compression_threads,
            batch_size_mb = self.batch_size / (1024 * 1024),
            channel_capacity = self.channel_capacity,
            "Building HyperPipelineConfig from auto-config"
        );

        crate::pipeline::hyper::HyperPipelineConfig {
            input_path: self.input_path,
            output_path: self.output_path,
            prefetcher: PrefetcherConfig {
                block_size: self.prefetch_block_size,
                prefetch_ahead: 4,
                platform_hints: crate::pipeline::hyper::config::PlatformHints::auto(),
            },
            parser: ParserConfig {
                num_threads: self.parser_threads,
                buffer_pool: crate::pipeline::types::buffer_pool::BufferPool::new(),
            },
            batcher: BatcherConfig {
                target_size: self.batch_size,
                max_messages: 250_000,
                num_threads: self.batcher_threads,
            },
            transform: TransformConfig {
                enabled: true,
                num_threads: self.transform_threads,
            },
            compression: CompressionConfig {
                num_threads: self.compression_threads,
                compression_level: self.compression_level,
                window_log: None, // Will be auto-detected by orchestrator
                buffer_pool: crate::pipeline::types::buffer_pool::BufferPool::new(),
            },
            packetizer: PacketizerConfig {
                enable_crc: true,
                num_threads: self.packetizer_threads,
            },
            writer: WriterConfig {
                buffer_size: self.writer_buffer_size,
                flush_interval: 4,
            },
            channel_capacity: self.channel_capacity,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auto_config_throughput() {
        let config = PipelineAutoConfig::throughput();
        assert_eq!(config.mode, PerformanceMode::Throughput);
        assert!(config.effective_compression_threads() >= 2);
    }

    #[test]
    fn test_auto_config_balanced() {
        let config = PipelineAutoConfig::balanced();
        assert_eq!(config.mode, PerformanceMode::Balanced);
        assert!(config.effective_compression_threads() >= 2);
    }

    #[test]
    fn test_auto_config_memory_efficient() {
        let config = PipelineAutoConfig::memory_efficient();
        assert_eq!(config.mode, PerformanceMode::MemoryEfficient);
        assert!(config.effective_compression_threads() >= 2);
    }

    #[test]
    fn test_override_compression_threads() {
        let config = PipelineAutoConfig::throughput().with_compression_threads(4);
        assert_eq!(config.effective_compression_threads(), 4);
    }

    #[test]
    fn test_override_batch_size() {
        let config = PipelineAutoConfig::throughput().with_batch_size(32 * 1024 * 1024);
        assert_eq!(config.effective_batch_size(), 32 * 1024 * 1024);
    }

    #[test]
    fn test_throughput_has_larger_batches() {
        let throughput = PipelineAutoConfig::throughput();
        let balanced = PipelineAutoConfig::balanced();
        let memory_eff = PipelineAutoConfig::memory_efficient();

        assert!(throughput.effective_batch_size() >= balanced.effective_batch_size());
        assert!(balanced.effective_batch_size() >= memory_eff.effective_batch_size());
    }

    #[test]
    fn test_compression_levels() {
        assert_eq!(PerformanceMode::Throughput.compression_level(), 1);
        assert_eq!(PerformanceMode::Balanced.compression_level(), 3);
        assert_eq!(PerformanceMode::MemoryEfficient.compression_level(), 3);
    }

    #[test]
    fn test_summarize() {
        let config = PipelineAutoConfig::throughput();
        let summary = config.summarize();
        assert!(summary.contains("Throughput"));
        assert!(summary.contains("cores"));
    }

    #[test]
    fn test_default() {
        let config = PipelineAutoConfig::default();
        assert_eq!(config.mode, PerformanceMode::Balanced);
    }
}
