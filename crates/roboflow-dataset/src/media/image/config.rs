// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Image decoder configuration.
//!
//! Provides configuration for image decoding with auto-detection
//! and fallback behavior.

use super::memory::MemoryStrategy;

/// Image decoder backend type selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DecoderBackendType {
    /// Auto-detect and use best available backend
    #[default]
    Auto,
    /// Force CPU decoding
    Cpu,
    /// Force GPU decoding (nvJPEG on Linux)
    Gpu,
    /// Force Apple hardware-accelerated decoding
    Apple,
}

/// Configuration for image decoding.
///
/// This configuration follows the builder pattern for easy construction.
#[derive(Debug, Clone)]
pub struct ImageDecoderConfig {
    /// Which decoder backend to use
    pub backend: DecoderBackendType,

    /// Memory allocation strategy for decoded images
    pub memory_strategy: MemoryStrategy,

    /// GPU device ID to use (0 = default device)
    pub gpu_device: Option<u32>,

    /// Enable automatic CPU fallback when GPU is unavailable
    pub auto_fallback: bool,

    /// Maximum image dimensions (security limit to prevent OOM)
    pub max_width: u32,
    /// Maximum image height in pixels.
    pub max_height: u32,

    /// Number of threads for CPU decoder
    pub cpu_threads: usize,
}

impl Default for ImageDecoderConfig {
    fn default() -> Self {
        Self {
            backend: DecoderBackendType::Auto,
            memory_strategy: MemoryStrategy::PageAligned,
            gpu_device: None,
            auto_fallback: true,
            max_width: 7680, // 8K resolution
            max_height: 4320,
            cpu_threads: rayon::current_num_threads().max(1),
        }
    }
}

impl ImageDecoderConfig {
    /// Create a new image decoder config with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the decoder backend.
    pub fn with_backend(mut self, backend: DecoderBackendType) -> Self {
        self.backend = backend;
        self
    }

    /// Set the memory allocation strategy.
    pub fn with_memory_strategy(mut self, strategy: MemoryStrategy) -> Self {
        self.memory_strategy = strategy;
        self
    }

    /// Set the GPU device ID.
    pub fn with_gpu_device(mut self, device: u32) -> Self {
        self.gpu_device = Some(device);
        self
    }

    /// Enable or disable automatic CPU fallback.
    pub fn with_auto_fallback(mut self, enabled: bool) -> Self {
        self.auto_fallback = enabled;
        self
    }

    /// Set maximum image width (security limit).
    pub fn with_max_width(mut self, width: u32) -> Self {
        self.max_width = width;
        self
    }

    /// Set maximum image height (security limit).
    pub fn with_max_height(mut self, height: u32) -> Self {
        self.max_height = height;
        self
    }

    /// Set the number of CPU threads for decoding.
    pub fn with_cpu_threads(mut self, threads: usize) -> Self {
        self.cpu_threads = threads.max(1);
        self
    }

    /// Validate the configuration.
    ///
    /// Returns an error if the configuration is invalid.
    pub fn validate(&self) -> Result<()> {
        if self.max_width == 0 || self.max_height == 0 {
            return Err(super::ImageError::InvalidData(
                "Invalid dimensions: max_width and max_height must be positive".to_string(),
            ));
        }

        if self.max_width > 16384 || self.max_height > 16384 {
            // 16K is a reasonable upper limit for robotics images
            return Err(super::ImageError::InvalidData(
                "Invalid dimensions: max_width and max_height must be <= 16384".to_string(),
            ));
        }

        if self.cpu_threads == 0 {
            return Err(super::ImageError::InvalidData(
                "CPU threads must be at least 1".to_string(),
            ));
        }

        Ok(())
    }

    /// Create a configuration optimized for maximum throughput.
    ///
    /// This prioritizes GPU decoding with page-aligned memory
    /// and higher CPU thread counts for parallel processing.
    pub fn max_throughput() -> Self {
        Self {
            backend: DecoderBackendType::Auto,
            memory_strategy: MemoryStrategy::PageAligned,
            gpu_device: None,
            auto_fallback: true,
            max_width: 7680,
            max_height: 4320,
            cpu_threads: rayon::current_num_threads().max(1),
        }
    }

    /// Create a configuration optimized for minimal memory usage.
    ///
    /// This uses heap allocation and single-threaded CPU decoding
    /// to minimize memory footprint.
    pub fn minimal_memory() -> Self {
        Self {
            backend: DecoderBackendType::Cpu,
            memory_strategy: MemoryStrategy::Heap,
            gpu_device: None,
            auto_fallback: false,
            max_width: 1920,
            max_height: 1080,
            cpu_threads: 1,
        }
    }

    /// Create a configuration for GPU-only decoding (no fallback).
    ///
    /// This will error if GPU decoding is unavailable.
    pub fn gpu_only() -> Self {
        Self {
            backend: DecoderBackendType::Gpu,
            memory_strategy: MemoryStrategy::PageAligned,
            gpu_device: None,
            auto_fallback: false,
            max_width: 7680,
            max_height: 4320,
            cpu_threads: 1,
        }
    }
}

/// Convenience result type for validation.
pub type Result<T> = std::result::Result<T, super::ImageError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ImageDecoderConfig::default();
        assert!(matches!(config.backend, DecoderBackendType::Auto));
        assert!(config.auto_fallback);
        assert!(config.cpu_threads > 0);
    }

    #[test]
    fn test_config_builder() {
        let config = ImageDecoderConfig::new()
            .with_backend(DecoderBackendType::Gpu)
            .with_max_width(1920)
            .with_max_height(1080)
            .with_cpu_threads(4);

        assert_eq!(config.max_width, 1920);
        assert_eq!(config.max_height, 1080);
        assert_eq!(config.cpu_threads, 4);
    }

    #[test]
    fn test_config_validation() {
        let config = ImageDecoderConfig::new();
        assert!(config.validate().is_ok());

        // Invalid dimensions
        let mut invalid = ImageDecoderConfig::new();
        invalid.max_width = 0;
        assert!(invalid.validate().is_err());

        // Invalid threads
        let mut invalid = ImageDecoderConfig::new();
        invalid.cpu_threads = 0;
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn test_max_throughput_config() {
        let config = ImageDecoderConfig::max_throughput();
        assert!(config.validate().is_ok());
        assert_eq!(config.memory_strategy, MemoryStrategy::PageAligned);
    }

    #[test]
    fn test_minimal_memory_config() {
        let config = ImageDecoderConfig::minimal_memory();
        assert!(config.validate().is_ok());
        assert_eq!(config.memory_strategy, MemoryStrategy::Heap);
        assert_eq!(config.cpu_threads, 1);
    }

    #[test]
    fn test_gpu_only_config() {
        let config = ImageDecoderConfig::gpu_only();
        assert!(config.validate().is_ok());
        assert!(!config.auto_fallback);
    }
}
