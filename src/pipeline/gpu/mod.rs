// Copyright (c) 2026 ArcheBase
// Roboflow is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//     http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

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
//! use crate::pipeline::gpu::{GpuCompressionConfig, GpuCompressorFactory};
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum BackendType {
    /// Auto-detect and use best available backend
    #[default]
    Auto,
    /// Force CPU compression (multi-threaded ZSTD)
    Cpu,
    /// Force NVIDIA GPU compression via nvcomp
    #[cfg(feature = "gpu")]
    NvComp,
    /// Force Apple libcompression (macOS only, hardware-accelerated)
    Apple,
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

// Apple libcompression backend (macOS only)
#[cfg(all(feature = "gpu", not(target_arch = "wasm32"), target_os = "macos"))]
pub mod apple {
    //! Apple libcompression backend for hardware-accelerated compression on macOS.

    use super::{
        backend::{
            ChunkToCompress, CompressedChunk, CompressorBackend, CompressorType, CpuCompressor,
        },
        GpuCompressionError,
    };

    /// Compression algorithm for Apple libcompression.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum AppleCompressionAlgorithm {
        /// Automatic selection based on CPU capabilities
        Auto,
        /// LZ4 (fast compression)
        Lz4,
        /// ZLIB (moderate compression)
        Zlib,
        /// LZFSE (Apple's optimized format)
        Lzfse,
    }

    /// Apple hardware-accelerated compressor using libcompression.
    pub struct AppleCompressor {
        cpu_compressor: CpuCompressor,
        algorithm: AppleCompressionAlgorithm,
    }

    impl AppleCompressor {
        /// Try to create a new Apple compressor.
        pub fn try_new(
            compression_level: u32,
            cpu_threads: usize,
            algorithm: AppleCompressionAlgorithm,
        ) -> Result<Self, GpuCompressionError> {
            // For now, use CPU compression as a fallback
            // TODO: Integrate with actual libcompression API
            eprintln!("Apple compression backend using CPU implementation");
            Ok(Self {
                cpu_compressor: CpuCompressor::new(compression_level, cpu_threads as u32),
                algorithm,
            })
        }

        /// Get the compression algorithm.
        pub fn algorithm(&self) -> AppleCompressionAlgorithm {
            self.algorithm
        }
    }

    impl CompressorBackend for AppleCompressor {
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

// Stub apple module for non-macOS platforms
#[cfg(all(feature = "gpu", not(target_arch = "wasm32"), not(target_os = "macos")))]
pub mod apple {
    //! Stub apple module for non-macOS platforms.

    use super::{
        backend::{
            ChunkToCompress, CompressedChunk, CompressorBackend, CompressorType, CpuCompressor,
        },
        GpuCompressionError,
    };

    /// Compression algorithm placeholder.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum AppleCompressionAlgorithm {
        Auto,
    }

    /// Stub compressor.
    pub struct AppleCompressor {
        cpu_compressor: CpuCompressor,
    }

    impl AppleCompressor {
        /// Try to create a new Apple compressor (returns error on non-macOS).
        pub fn try_new(
            compression_level: u32,
            cpu_threads: usize,
            _algorithm: AppleCompressionAlgorithm,
        ) -> Result<Self, GpuCompressionError> {
            Ok(Self {
                cpu_compressor: CpuCompressor::new(compression_level, cpu_threads as u32),
            })
        }
    }

    impl CompressorBackend for AppleCompressor {
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
            CompressorType::Cpu
        }

        fn compression_level(&self) -> u32 {
            self.cpu_compressor.compression_level()
        }

        fn estimate_memory(&self, data_size: usize) -> usize {
            self.cpu_compressor.estimate_memory(data_size)
        }

        fn is_available(&self) -> bool {
            false
        }
    }
}
