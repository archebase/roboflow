// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Compression backend abstraction.
//!
//! Provides a platform-agnostic trait for compression backends,
//! allowing GPU and CPU implementations to be used interchangeably.

use super::{GpuCompressionError, GpuResult};
use roboflow_core::RoboflowError;

// Re-export chunk types from compress module to avoid duplication
pub use crate::compression::{ChunkToCompress, CompressedDataChunk as CompressedChunk};

/// Compression backend type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CompressorType {
    /// CPU-based compression (multi-threaded ZSTD)
    Cpu,
    /// GPU-based compression (nvCOMP)
    Gpu,
    /// Apple Silicon hardware-accelerated compression (libcompression)
    Apple,
}

/// Trait for compression backends.
///
/// This trait provides a unified interface for both CPU and GPU
/// compression implementations, enabling seamless fallback and
/// platform-agnostic code.
pub trait CompressorBackend: Send + Sync {
    /// Compress a single chunk of data.
    ///
    /// # Arguments
    ///
    /// * `chunk` - The data chunk to compress
    ///
    /// # Returns
    ///
    /// Compressed data with metadata
    fn compress_chunk(&self, chunk: &ChunkToCompress) -> GpuResult<CompressedChunk>;

    /// Compress multiple chunks in parallel.
    ///
    /// # Arguments
    ///
    /// * `chunks` - Slice of chunks to compress
    ///
    /// # Returns
    ///
    /// Vector of compressed chunks
    fn compress_parallel(&self, chunks: &[ChunkToCompress]) -> GpuResult<Vec<CompressedChunk>> {
        // Default implementation processes chunks sequentially
        chunks
            .iter()
            .map(|chunk| self.compress_chunk(chunk))
            .collect()
    }

    /// Get the compressor type.
    fn compressor_type(&self) -> CompressorType;

    /// Get the compression level (0-22 for ZSTD).
    fn compression_level(&self) -> u32;

    /// Estimate memory usage for compression.
    ///
    /// # Arguments
    ///
    /// * `data_size` - Size of data to be compressed in bytes
    ///
    /// # Returns
    ///
    /// Estimated memory requirement in bytes
    fn estimate_memory(&self, data_size: usize) -> usize;

    /// Check if the compressor is available and ready.
    fn is_available(&self) -> bool {
        true
    }
}

/// CPU compression backend using multi-threaded ZSTD.
///
/// Delegates to [`crate::compression::CompressionPool`] for the actual
/// compression work, keeping this type as a thin adapter that implements
/// the [`CompressorBackend`] trait.
pub struct CpuCompressor {
    pool: crate::compression::CompressionPool,
    compression_level: u32,
    threads: u32,
}

impl CpuCompressor {
    /// Create a new CPU compressor with the given settings.
    pub fn new(compression_level: u32, threads: u32) -> Self {
        use crate::config::CompressionConfig;

        let config = CompressionConfig {
            enabled: true,
            threads: threads as usize,
            compression_level: compression_level as i32,
            ..CompressionConfig::default()
        };

        Self {
            pool: crate::compression::CompressionPool::from_config(config),
            compression_level,
            threads,
        }
    }

    /// Create a CPU compressor with default settings.
    pub fn default_config() -> Self {
        Self::new(3, crate::hardware::detect_cpu_count())
    }
}

impl CompressorBackend for CpuCompressor {
    fn compress_chunk(&self, chunk: &ChunkToCompress) -> GpuResult<CompressedChunk> {
        self.pool
            .compress_chunk(chunk)
            .map_err(|e| GpuCompressionError::CompressionFailed(e.to_string()))
    }

    fn compress_parallel(&self, chunks: &[ChunkToCompress]) -> GpuResult<Vec<CompressedChunk>> {
        self.pool
            .compress_parallel(chunks)
            .map_err(|e| GpuCompressionError::CompressionFailed(e.to_string()))
    }

    fn compressor_type(&self) -> CompressorType {
        CompressorType::Cpu
    }

    fn compression_level(&self) -> u32 {
        self.compression_level
    }

    fn estimate_memory(&self, data_size: usize) -> usize {
        // CPU ZSTD uses approximately 3-4x the data size for compression window
        // Plus thread-local buffers
        let per_thread_memory = data_size * 4;
        per_thread_memory * self.threads as usize
    }

    fn is_available(&self) -> bool {
        true // CPU compression is always available
    }
}

/// Convert GpuCompressionError to RoboflowError.
impl From<GpuCompressionError> for RoboflowError {
    fn from(err: GpuCompressionError) -> Self {
        RoboflowError::encode("GpuCompressor", format!("{}", err))
    }
}
