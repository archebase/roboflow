//! GPU-accelerated compression support.
//!
//! This module provides an abstraction for GPU-accelerated compression
//! with platform-agnostic backend support and automatic CPU fallback.
//!
//! # Experimental
//!
//! This module is **experimental** and may change significantly in future releases.
//! GPU compression requires the `gpu` feature flag and compatible hardware.
//!
//! # Supported Backends
//!
//! - **nvcomp** (NVIDIA CUDA): Requires NVIDIA GPU with CUDA support (Linux)
//! - **Apple libcompression**: Hardware-accelerated compression on Apple Silicon (macOS)
//! - **CPU Fallback**: Automatically used when GPU is unavailable
//!
//! # Example
//!
//! ```no_run
//! use robocodec::pipeline::gpu::{GpuCompressionConfig, GpuCompressorFactory};
//!
//! let config = GpuCompressionConfig::default();
//! let compressor = GpuCompressorFactory::create(&config)?;
//!
//! // Compress data
//! let compressed = compressor.compress(&data)?;
//! ```

#[cfg(all(feature = "gpu", not(target_arch = "wasm32")))]
pub use backend::{CompressorBackend, CompressorType};

#[cfg(all(feature = "gpu", not(target_arch = "wasm32")))]
pub use config::GpuCompressionConfig;

#[cfg(all(feature = "gpu", not(target_arch = "wasm32")))]
pub use factory::GpuCompressorFactory;

/// Error types for GPU compression operations.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum GpuCompressionError {
    /// GPU device not found
    DeviceNotFound,
    /// CUDA initialization failed
    CudaInitFailed(String),
    /// nvCOMP library not found
    NvcompNotFound,
    /// Insufficient GPU memory
    InsufficientMemory { required: usize, available: usize },
    /// Compression operation failed
    CompressionFailed(String),
    /// GPU operation error
    GpuError(String),
    /// Fallback to CPU compression
    CpuFallback,
}

impl std::fmt::Display for GpuCompressionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GpuCompressionError::DeviceNotFound => write!(f, "GPU device not found"),
            GpuCompressionError::CudaInitFailed(msg) => {
                write!(f, "CUDA initialization failed: {}", msg)
            }
            GpuCompressionError::NvcompNotFound => write!(f, "nvCOMP library not found"),
            GpuCompressionError::InsufficientMemory {
                required,
                available,
            } => {
                write!(
                    f,
                    "Insufficient GPU memory: required {} MB, available {} MB",
                    required / (1024 * 1024),
                    available / (1024 * 1024)
                )
            }
            GpuCompressionError::CompressionFailed(msg) => write!(f, "Compression failed: {}", msg),
            GpuCompressionError::GpuError(msg) => write!(f, "GPU error: {}", msg),
            GpuCompressionError::CpuFallback => write!(f, "Falling back to CPU compression"),
        }
    }
}

impl std::error::Error for GpuCompressionError {}

/// Result type for GPU compression operations.
pub type GpuResult<T> = std::result::Result<T, GpuCompressionError>;

/// Compression backend type selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BackendType {
    /// Auto-detect and use best available backend
    Auto,
    /// Force CPU compression (multi-threaded ZSTD)
    Cpu,
    /// Force NVIDIA GPU compression via nvcomp
    #[cfg(feature = "gpu")]
    NvComp,
    /// Force Apple libcompression (macOS only, hardware-accelerated)
    Apple,
}

impl Default for BackendType {
    fn default() -> Self {
        Self::Auto
    }
}

#[cfg(all(feature = "gpu", not(target_arch = "wasm32")))]
mod backend;
#[cfg(all(feature = "gpu", not(target_arch = "wasm32")))]
mod config;
#[cfg(all(feature = "gpu", not(target_arch = "wasm32")))]
mod factory;

// nvcomp backend (conditional compilation)
// Only compiled on Linux x86_64/aarch64 with nvCOMP available
#[cfg(all(
    feature = "gpu",
    not(target_arch = "wasm32"),
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
pub mod nvcomp;

// Stub nvcomp module for non-Linux platforms (for compilation only)
#[cfg(all(
    feature = "gpu",
    not(target_arch = "wasm32"),
    not(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))
))]
pub mod nvcomp {
    //! Stub nvcomp module for non-Linux platforms.
    //!
    //! GPU compression is only supported on Linux x86_64/aarch64 with CUDA.
    //! This stub allows compilation on other platforms for development purposes.

    use super::{
        backend::{
            ChunkToCompress, CompressedChunk, CompressorBackend, CompressorType, CpuCompressor,
        },
        GpuCompressionError,
    };

    /// Stub compressor that falls back to CPU compression.
    pub struct NvComCompressor {
        cpu_compressor: CpuCompressor,
    }

    impl NvComCompressor {
        /// Try to create a new nvCOMP compressor (falls back to CPU on non-Linux).
        pub fn try_new(
            compression_level: u32,
            _device_id: u32,
            _max_chunk_size: usize,
        ) -> Result<Self, GpuCompressionError> {
            eprintln!("GPU compression not supported on this platform. Using CPU compression.");
            Ok(Self {
                cpu_compressor: CpuCompressor::new(compression_level, 8),
            })
        }

        /// Check if nvCOMP is available (always false on non-Linux).
        pub fn is_available() -> bool {
            false
        }

        /// Get device info (returns empty list on non-Linux).
        pub fn device_info() -> Vec<super::factory::GpuDeviceInfo> {
            Vec::new()
        }
    }

    impl CompressorBackend for NvComCompressor {
        fn compress_chunk(&self, chunk: &ChunkToCompress) -> super::GpuResult<CompressedChunk> {
            self.cpu_compressor.compress_chunk(chunk)
        }

        fn compress_parallel(
            &self,
            chunks: &[ChunkToCompress],
        ) -> super::GpuResult<Vec<CompressedChunk>> {
            self.cpu_compressor.compress_parallel(chunks)
        }

        fn compressor_type(&self) -> CompressorType {
            // Report CPU type since this stub uses CPU compression internally
            CompressorType::Cpu
        }

        fn compression_level(&self) -> u32 {
            self.cpu_compressor.compression_level()
        }

        fn estimate_memory(&self, data_size: usize) -> usize {
            self.cpu_compressor.estimate_memory(data_size)
        }

        fn is_available(&self) -> bool {
            true
        }
    }
}
