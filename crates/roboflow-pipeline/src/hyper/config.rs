// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Configuration for the 7-stage hyper-pipeline.

use std::path::{Path, PathBuf};

use crate::types::buffer_pool::BufferPool;
use roboflow_core::Result;

/// Default channel capacity for inter-stage communication.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 16;

/// Default prefetch block size (4MB).
pub const DEFAULT_PREFETCH_BLOCK_SIZE: usize = 4 * 1024 * 1024;

/// Default batch target size (16MB).
pub const DEFAULT_BATCH_TARGET_SIZE: usize = 16 * 1024 * 1024;

/// Configuration for the hyper-pipeline.
#[derive(Debug)]
pub struct HyperPipelineConfig {
    /// Input file path
    pub input_path: PathBuf,
    /// Output file path
    pub output_path: PathBuf,
    /// Prefetcher configuration
    pub prefetcher: PrefetcherConfig,
    /// Parser configuration
    pub parser: ParserConfig,
    /// Batcher configuration
    pub batcher: BatcherConfig,
    /// Transform configuration
    pub transform: TransformConfig,
    /// Compression configuration
    pub compression: CompressionConfig,
    /// Packetizer configuration
    pub packetizer: PacketizerConfig,
    /// Writer configuration
    pub writer: WriterConfig,
    /// Channel capacities
    pub channel_capacity: usize,
}

impl HyperPipelineConfig {
    /// Create a new configuration with default settings.
    pub fn new<P: AsRef<Path>>(input_path: P, output_path: P) -> Self {
        Self {
            input_path: input_path.as_ref().to_path_buf(),
            output_path: output_path.as_ref().to_path_buf(),
            prefetcher: PrefetcherConfig::default(),
            parser: ParserConfig::default(),
            batcher: BatcherConfig::default(),
            transform: TransformConfig::default(),
            compression: CompressionConfig::default(),
            packetizer: PacketizerConfig::default(),
            writer: WriterConfig::default(),
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        }
    }

    /// Create a builder for fluent configuration.
    pub fn builder() -> HyperPipelineBuilder {
        HyperPipelineBuilder::new()
    }
}

/// Stage 1: Prefetcher configuration.
#[derive(Debug, Clone)]
pub struct PrefetcherConfig {
    /// Block size for prefetching (default: 4MB)
    pub block_size: usize,
    /// Number of blocks to prefetch ahead
    pub prefetch_ahead: usize,
    /// Platform-specific I/O hints
    pub platform_hints: PlatformHints,
}

impl Default for PrefetcherConfig {
    fn default() -> Self {
        Self {
            block_size: DEFAULT_PREFETCH_BLOCK_SIZE,
            prefetch_ahead: 4,
            platform_hints: PlatformHints::auto(),
        }
    }
}

/// Platform-specific I/O optimization hints.
#[derive(Debug, Clone)]
pub enum PlatformHints {
    /// macOS: Use madvise with SEQUENTIAL and WILLNEED
    #[cfg(target_os = "macos")]
    Madvise {
        /// Hint sequential access pattern
        sequential: bool,
        /// Prefetch pages (MADV_WILLNEED)
        willneed: bool,
    },
    /// Linux: Use io_uring for async I/O
    #[cfg(target_os = "linux")]
    IoUring {
        /// Queue depth for io_uring
        queue_depth: u32,
    },
    /// Fallback: Use posix_fadvise (Linux) or basic mmap
    Fadvise {
        /// Hint sequential access
        sequential: bool,
    },
    /// No platform-specific optimizations
    None,
}

impl PlatformHints {
    /// Auto-detect best platform hints.
    pub fn auto() -> Self {
        #[cfg(target_os = "macos")]
        {
            PlatformHints::Madvise {
                sequential: true,
                willneed: true,
            }
        }
        #[cfg(target_os = "linux")]
        {
            // Default to fadvise; io_uring requires feature flag
            PlatformHints::Fadvise { sequential: true }
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            PlatformHints::None
        }
    }
}

/// Stage 2: Parser configuration.
#[derive(Debug, Clone)]
pub struct ParserConfig {
    /// Number of parser threads (default: 2)
    pub num_threads: usize,
    /// Buffer pool for decompression
    pub buffer_pool: BufferPool,
}

impl Default for ParserConfig {
    fn default() -> Self {
        Self {
            num_threads: 2,
            buffer_pool: BufferPool::new(),
        }
    }
}

/// Stage 3: Batcher configuration.
#[derive(Debug, Clone)]
pub struct BatcherConfig {
    /// Target batch size in bytes (default: 16MB)
    pub target_size: usize,
    /// Maximum messages per batch
    pub max_messages: usize,
    /// Number of batcher threads
    pub num_threads: usize,
}

impl Default for BatcherConfig {
    fn default() -> Self {
        Self {
            target_size: DEFAULT_BATCH_TARGET_SIZE,
            max_messages: 250_000,
            num_threads: 2,
        }
    }
}

/// Stage 4: Transform configuration.
#[derive(Debug, Clone)]
pub struct TransformConfig {
    /// Enable transform stage (default: true, but pass-through)
    pub enabled: bool,
    /// Number of transform threads (default: 2)
    pub num_threads: usize,
}

impl Default for TransformConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            num_threads: 2,
        }
    }
}

/// Stage 5: Compression configuration.
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    /// Number of compression threads (default: num_cpus)
    pub num_threads: usize,
    /// ZSTD compression level (default: 3)
    pub compression_level: i32,
    /// ZSTD window log (None = auto-detect)
    pub window_log: Option<u32>,
    /// Buffer pool for compression output
    pub buffer_pool: BufferPool,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            num_threads: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(8),
            compression_level: 3,
            window_log: None,
            buffer_pool: BufferPool::new(),
        }
    }
}

/// Stage 6: Packetizer configuration.
#[derive(Debug, Clone)]
pub struct PacketizerConfig {
    /// Enable CRC32 checksum (default: true)
    pub enable_crc: bool,
    /// Number of packetizer threads
    pub num_threads: usize,
}

impl Default for PacketizerConfig {
    fn default() -> Self {
        Self {
            enable_crc: true,
            num_threads: 2,
        }
    }
}

/// Stage 7: Writer configuration.
#[derive(Debug, Clone)]
pub struct WriterConfig {
    /// Write buffer size (default: 8MB)
    pub buffer_size: usize,
    /// Flush interval (chunks between flushes)
    pub flush_interval: u64,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            buffer_size: 8 * 1024 * 1024,
            flush_interval: 4,
        }
    }
}

/// Builder for HyperPipelineConfig.
#[derive(Debug, Default)]
pub struct HyperPipelineBuilder {
    input_path: Option<PathBuf>,
    output_path: Option<PathBuf>,
    prefetcher: Option<PrefetcherConfig>,
    parser: Option<ParserConfig>,
    batcher: Option<BatcherConfig>,
    transform: Option<TransformConfig>,
    compression: Option<CompressionConfig>,
    packetizer: Option<PacketizerConfig>,
    writer: Option<WriterConfig>,
    channel_capacity: Option<usize>,
}

impl HyperPipelineBuilder {
    /// Create a new builder.
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

    /// Set the prefetcher configuration.
    pub fn prefetcher(mut self, config: PrefetcherConfig) -> Self {
        self.prefetcher = Some(config);
        self
    }

    /// Set the parser configuration.
    pub fn parser(mut self, config: ParserConfig) -> Self {
        self.parser = Some(config);
        self
    }

    /// Set the batcher configuration.
    pub fn batcher(mut self, config: BatcherConfig) -> Self {
        self.batcher = Some(config);
        self
    }

    /// Set the transform configuration.
    pub fn transform(mut self, config: TransformConfig) -> Self {
        self.transform = Some(config);
        self
    }

    /// Set the compression configuration.
    pub fn compression(mut self, config: CompressionConfig) -> Self {
        self.compression = Some(config);
        self
    }

    /// Set the packetizer configuration.
    pub fn packetizer(mut self, config: PacketizerConfig) -> Self {
        self.packetizer = Some(config);
        self
    }

    /// Set the writer configuration.
    pub fn writer(mut self, config: WriterConfig) -> Self {
        self.writer = Some(config);
        self
    }

    /// Set channel capacity for all inter-stage channels.
    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = Some(capacity);
        self
    }

    /// Set compression level.
    pub fn compression_level(mut self, level: i32) -> Self {
        let mut config = self.compression.unwrap_or_default();
        config.compression_level = level;
        self.compression = Some(config);
        self
    }

    /// Set number of compression threads.
    pub fn compression_threads(mut self, threads: usize) -> Self {
        let mut config = self.compression.unwrap_or_default();
        config.num_threads = threads;
        self.compression = Some(config);
        self
    }

    /// Enable or disable CRC32.
    pub fn enable_crc(mut self, enable: bool) -> Self {
        let mut config = self.packetizer.unwrap_or_default();
        config.enable_crc = enable;
        self.packetizer = Some(config);
        self
    }

    /// Use high-throughput preset (compression level 1, larger batches).
    pub fn high_throughput(mut self) -> Self {
        let mut compression = self.compression.unwrap_or_default();
        compression.compression_level = 1;
        self.compression = Some(compression);

        let mut batcher = self.batcher.unwrap_or_default();
        batcher.target_size = 32 * 1024 * 1024; // 32MB batches
        self.batcher = Some(batcher);

        self
    }

    /// Use balanced preset (default settings).
    pub fn balanced(self) -> Self {
        // Defaults are already balanced
        self
    }

    /// Use maximum compression preset (level 9).
    pub fn max_compression(mut self) -> Self {
        let mut compression = self.compression.unwrap_or_default();
        compression.compression_level = 9;
        self.compression = Some(compression);
        self
    }

    /// Build the configuration.
    pub fn build(self) -> Result<HyperPipelineConfig> {
        use roboflow_core::RoboflowError;

        let input_path = self
            .input_path
            .ok_or_else(|| RoboflowError::parse("HyperPipelineBuilder", "Input path not set"))?;

        let output_path = self
            .output_path
            .ok_or_else(|| RoboflowError::parse("HyperPipelineBuilder", "Output path not set"))?;

        Ok(HyperPipelineConfig {
            input_path,
            output_path,
            prefetcher: self.prefetcher.unwrap_or_default(),
            parser: self.parser.unwrap_or_default(),
            batcher: self.batcher.unwrap_or_default(),
            transform: self.transform.unwrap_or_default(),
            compression: self.compression.unwrap_or_default(),
            packetizer: self.packetizer.unwrap_or_default(),
            writer: self.writer.unwrap_or_default(),
            channel_capacity: self.channel_capacity.unwrap_or(DEFAULT_CHANNEL_CAPACITY),
        })
    }
}

impl HyperPipelineConfig {
    /// Create a HyperPipelineConfig from auto-detected hardware configuration.
    ///
    /// This is a convenience method that uses hardware-aware defaults.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use roboflow::pipeline::hyper::HyperPipelineConfig;
    /// use roboflow::pipeline::PerformanceMode;
    ///
    /// let config = HyperPipelineConfig::auto(
    ///     PerformanceMode::Throughput,
    ///     "input.bag",
    ///     "output.mcap",
    /// );
    /// ```
    pub fn auto(
        mode: crate::auto_config::PerformanceMode,
        input_path: impl AsRef<Path>,
        output_path: impl AsRef<Path>,
    ) -> Self {
        use crate::auto_config::PipelineAutoConfig;

        let auto_config = PipelineAutoConfig::auto(mode);
        auto_config.to_hyper_config(input_path, output_path).build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = HyperPipelineConfig::new("input.bag", "output.mcap");
        assert_eq!(config.prefetcher.block_size, DEFAULT_PREFETCH_BLOCK_SIZE);
        assert_eq!(config.batcher.target_size, DEFAULT_BATCH_TARGET_SIZE);
        assert_eq!(config.compression.compression_level, 3);
        assert!(config.packetizer.enable_crc);
    }

    #[test]
    fn test_builder_high_throughput() {
        let config = HyperPipelineConfig::builder()
            .input_path("input.bag")
            .output_path("output.mcap")
            .high_throughput()
            .build()
            .unwrap();

        assert_eq!(config.compression.compression_level, 1);
        assert_eq!(config.batcher.target_size, 32 * 1024 * 1024);
    }

    #[test]
    fn test_builder_missing_input() {
        let result = HyperPipelineConfig::builder()
            .output_path("output.mcap")
            .build();

        assert!(result.is_err());
    }
}
