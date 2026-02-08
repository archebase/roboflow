// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Pipeline configuration with auto-tuning parameters.

/// Target throughput for the pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum CompressionTarget {
    /// Real-time processing (< 100ms latency)
    Realtime,
    /// Interactive processing (100-500ms latency)
    Interactive,
    /// Batch processing (maximum throughput)
    #[default]
    Batch,
    /// Maximum compression (archival)
    Archive,
}

impl CompressionTarget {
    pub fn default_compression_level(&self) -> i32 {
        match self {
            CompressionTarget::Realtime => 1,
            CompressionTarget::Interactive => 3,
            CompressionTarget::Batch => 9,
            CompressionTarget::Archive => 15,
        }
    }

    pub fn default_target_throughput_mb_s(&self) -> f64 {
        match self {
            CompressionTarget::Realtime => 50.0,
            CompressionTarget::Interactive => 200.0,
            CompressionTarget::Batch => 1000.0,
            CompressionTarget::Archive => 100.0,
        }
    }
}

/// Compression level for ZSTD.
pub type CompressionLevel = i32;

/// Default compression level for throughput.
pub const DEFAULT_COMPRESSION_LEVEL: CompressionLevel = 3;

/// High compression level for better ratio.
pub const HIGH_COMPRESSION_LEVEL: CompressionLevel = 9;

/// Low compression level for maximum speed.
pub const LOW_COMPRESSION_LEVEL: CompressionLevel = 1;

/// Unified compression configuration with auto-tuning support.
///
/// This is the single source of truth for compression settings across
/// the pipeline crate, used by both the parallel compressor and the
/// hyper-pipeline compression stage.
#[derive(Debug, Clone, Copy)]
pub struct CompressionConfig {
    /// Enable multi-threaded compression (default: true)
    pub enabled: bool,
    /// Number of compression threads (0 = auto-detect)
    pub threads: usize,
    /// Target chunk size in bytes (default: 8MB)
    pub chunk_size: usize,
    /// ZSTD compression level (0-22, default 3)
    pub compression_level: i32,
    /// Maximum memory to use for buffers in bytes (0 = auto/unlimited)
    pub max_memory_bytes: usize,
    /// ZSTD window log (None = auto-detect).
    /// Controls max window size: 2^window_log bytes.
    /// Set based on chunk size to reduce cache thrashing.
    /// For example: 22 = 4MB, 23 = 8MB, 24 = 16MB.
    pub window_log: Option<u32>,
}

/// Default chunk size: 8MB.
const DEFAULT_CHUNK_SIZE: usize = 8 * 1024 * 1024;

impl CompressionConfig {
    /// Auto-detect optimal compression settings based on system capabilities.
    ///
    /// Performance notes:
    /// - Multi-threaded ZSTD provides 2-5x speedup over single-threaded
    /// - Chunk size should be 8MB per thread for optimal throughput
    /// - Compression level 3 provides good balance between speed and ratio
    pub fn auto_detect() -> Self {
        // Detect CPU cores
        let num_cpus = crate::hardware::detect_cpu_count() as usize;

        // Use all available CPUs for maximum throughput
        let threads = num_cpus;

        // Calculate chunk size: 8MB per thread for optimal multi-threaded compression
        // This gives ZSTD enough data to distribute work across threads efficiently
        let chunk_size = DEFAULT_CHUNK_SIZE * threads;

        Self {
            enabled: true,
            threads,
            chunk_size,
            compression_level: DEFAULT_COMPRESSION_LEVEL,
            max_memory_bytes: 0,
            window_log: None,
        }
    }

    /// Create a new compression config with the given level and thread count.
    pub fn new(level: CompressionLevel, threads: usize) -> Self {
        Self {
            compression_level: level,
            threads,
            ..Self::auto_detect()
        }
    }

    /// Create configuration optimized for a specific data size.
    ///
    /// # Thresholds
    /// - < 100MB: Single-threaded (overhead not worth it)
    /// - 100MB - 1GB: 2-4 threads
    /// - > 1GB: Auto-detect based on system
    pub fn for_data_size(total_bytes: u64) -> Self {
        const GPU_THRESHOLD: u64 = 100 * 1024 * 1024; // 100MB

        if total_bytes < GPU_THRESHOLD {
            // Small files: disable multi-threading
            Self {
                enabled: false,
                threads: 0,
                chunk_size: DEFAULT_CHUNK_SIZE,
                compression_level: DEFAULT_COMPRESSION_LEVEL,
                max_memory_bytes: 0,
                window_log: None,
            }
        } else {
            // Large files: enable auto-detection
            Self::auto_detect()
        }
    }

    /// Create configuration for a specific compression target.
    pub fn for_target(target: CompressionTarget) -> Self {
        let mut config = Self::auto_detect();
        config.compression_level = target.default_compression_level();
        config
    }

    /// Disable compression (for debugging or embedded systems).
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            threads: 0,
            chunk_size: 0,
            compression_level: 0,
            max_memory_bytes: 0,
            window_log: None,
        }
    }

    /// Maximum throughput configuration.
    /// Uses level 1 compression (fastest) with all CPU cores.
    pub fn max_throughput() -> Self {
        Self {
            compression_level: LOW_COMPRESSION_LEVEL,
            ..Self::auto_detect()
        }
    }

    /// High throughput configuration.
    pub fn high_throughput() -> Self {
        Self {
            compression_level: LOW_COMPRESSION_LEVEL,
            ..Self::auto_detect()
        }
    }

    /// Balanced configuration.
    pub fn balanced() -> Self {
        Self::default()
    }

    /// High compression configuration.
    pub fn high_compression() -> Self {
        Self {
            compression_level: HIGH_COMPRESSION_LEVEL,
            ..Self::auto_detect()
        }
    }

    /// Get estimated memory usage for this configuration.
    pub fn estimated_memory_bytes(&self) -> usize {
        // Each thread uses ~100MB for compression buffers
        // Plus chunk buffer
        let thread_memory = self.threads * 100 * 1024 * 1024;
        let chunk_memory = if self.chunk_size > 0 {
            self.chunk_size
        } else {
            DEFAULT_CHUNK_SIZE
        };
        thread_memory + chunk_memory
    }
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self::auto_detect()
    }
}
