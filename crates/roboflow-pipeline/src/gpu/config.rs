// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! GPU compression configuration.

use super::{BackendType, GpuResult};

/// Configuration for GPU-accelerated compression.
#[derive(Debug, Clone)]
pub struct GpuCompressionConfig {
    /// Which backend to use
    pub backend: BackendType,
    /// Compression level (0-22, where 0 is default)
    pub compression_level: u32,
    /// Number of CPU threads to use for fallback or CPU backend
    pub cpu_threads: u32,
    /// GPU device ID to use (0 = default device)
    pub gpu_device: Option<u32>,
    /// Maximum chunk size for GPU compression (bytes)
    /// Larger chunks provide better GPU utilization but use more memory
    pub max_chunk_size: usize,
    /// Enable automatic fallback to CPU if GPU is unavailable
    pub auto_fallback: bool,
}

impl Default for GpuCompressionConfig {
    fn default() -> Self {
        Self {
            backend: BackendType::Auto,
            compression_level: 3,
            cpu_threads: crate::hardware::detect_cpu_count(),
            gpu_device: None,
            max_chunk_size: 256 * 1024 * 1024, // 256MB default
            auto_fallback: true,
        }
    }
}

impl GpuCompressionConfig {
    /// Create a new GPU compression config with optimal settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the compression backend.
    pub fn with_backend(mut self, backend: BackendType) -> Self {
        self.backend = backend;
        self
    }

    /// Set the compression level.
    pub fn with_compression_level(mut self, level: u32) -> Self {
        self.compression_level = level.clamp(0, 22);
        self
    }

    /// Set the number of CPU threads for fallback.
    pub fn with_cpu_threads(mut self, threads: u32) -> Self {
        self.cpu_threads = threads.max(1);
        self
    }

    /// Set the GPU device ID.
    pub fn with_gpu_device(mut self, device: u32) -> Self {
        self.gpu_device = Some(device);
        self
    }

    /// Set the maximum chunk size for GPU compression.
    pub fn with_max_chunk_size(mut self, size: usize) -> Self {
        self.max_chunk_size = size;
        self
    }

    /// Enable or disable automatic CPU fallback.
    pub fn with_auto_fallback(mut self, enabled: bool) -> Self {
        self.auto_fallback = enabled;
        self
    }

    /// Validate the configuration.
    pub fn validate(&self) -> GpuResult<()> {
        if self.compression_level > 22 {
            return Err(super::GpuCompressionError::CompressionFailed(
                "Compression level must be 0-22".to_string(),
            ));
        }

        if self.max_chunk_size < 1024 {
            return Err(super::GpuCompressionError::CompressionFailed(
                "Max chunk size must be at least 1KB".to_string(),
            ));
        }

        Ok(())
    }

    /// Create a configuration optimized for maximum throughput.
    pub fn max_throughput() -> Self {
        Self {
            backend: BackendType::Auto,
            compression_level: 3, // Lower level for speed
            cpu_threads: crate::hardware::detect_cpu_count(),
            gpu_device: None,
            max_chunk_size: 512 * 1024 * 1024, // 512MB chunks for GPU
            auto_fallback: true,
        }
    }

    /// Create a configuration optimized for maximum compression.
    pub fn max_compression() -> Self {
        Self {
            backend: BackendType::Auto,
            compression_level: 19, // High compression level
            cpu_threads: crate::hardware::detect_cpu_count(),
            gpu_device: None,
            max_chunk_size: 128 * 1024 * 1024, // Smaller chunks for better compression
            auto_fallback: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = GpuCompressionConfig::default();
        assert!(matches!(config.backend, BackendType::Auto));
        assert_eq!(config.compression_level, 3);
        assert!(config.auto_fallback);
    }

    #[test]
    fn test_config_builder() {
        let config = GpuCompressionConfig::new()
            .with_compression_level(10)
            .with_cpu_threads(4)
            .with_max_chunk_size(1024 * 1024);

        assert_eq!(config.compression_level, 10);
        assert_eq!(config.cpu_threads, 4);
        assert_eq!(config.max_chunk_size, 1024 * 1024);
    }

    #[test]
    fn test_config_validation() {
        let mut config = GpuCompressionConfig::new();
        assert!(config.validate().is_ok());

        config.compression_level = 30;
        assert!(config.validate().is_err());

        config.compression_level = 15;
        config.max_chunk_size = 512;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_max_throughput_config() {
        let config = GpuCompressionConfig::max_throughput();
        assert_eq!(config.compression_level, 3);
        assert_eq!(config.max_chunk_size, 512 * 1024 * 1024);
    }

    #[test]
    fn test_max_compression_config() {
        let config = GpuCompressionConfig::max_compression();
        assert_eq!(config.compression_level, 19);
        assert_eq!(config.max_chunk_size, 128 * 1024 * 1024);
    }
}
