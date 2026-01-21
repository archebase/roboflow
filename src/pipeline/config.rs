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
    pub fn default_compression_level(&self) -> u32 {
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

/// Compression configuration with auto-tuning support.
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    /// Enable multi-threaded compression
    pub enabled: bool,
    /// Number of compression threads (0 = auto-detect)
    pub threads: u32,
    /// Target chunk size in bytes (None = mcap default)
    pub chunk_size: Option<u64>,
    /// ZSTD compression level (0-22, 0 = default)
    pub compression_level: u32,
    /// Maximum memory to use for buffers (bytes). None = auto-detect
    pub max_memory_bytes: Option<usize>,
}

impl CompressionConfig {
    /// Auto-detect optimal compression settings based on system capabilities.
    ///
    /// Performance notes:
    /// - Multi-threaded ZSTD provides 2-5x speedup over single-threaded
    /// - Chunk size should be 8MB per thread for optimal throughput
    /// - Compression level 3 provides good balance between speed and ratio
    pub fn auto_detect() -> Self {
        // Detect CPU cores
        let num_cpus = crate::pipeline::hardware::detect_cpu_count();

        // Use all available CPUs for maximum throughput
        let threads = num_cpus;

        // Calculate chunk size: 8MB per thread for optimal multi-threaded compression
        // This gives ZSTD enough data to distribute work across threads efficiently
        let chunk_size = 8 * 1024 * 1024 * threads as u64;

        Self {
            enabled: true,
            threads,
            chunk_size: Some(chunk_size),
            compression_level: 3,
            max_memory_bytes: None,
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
                chunk_size: None,
                compression_level: 3,
                max_memory_bytes: None,
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
            chunk_size: None,
            compression_level: 0,
            max_memory_bytes: None,
        }
    }

    /// Get estimated memory usage for this configuration.
    pub fn estimated_memory_bytes(&self) -> usize {
        // Each thread uses ~100MB for compression buffers
        // Plus chunk buffer
        let thread_memory = (self.threads as usize) * 100 * 1024 * 1024;
        let chunk_memory = self.chunk_size.unwrap_or(8 * 1024 * 1024) as usize;
        thread_memory + chunk_memory
    }
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self::auto_detect()
    }
}
