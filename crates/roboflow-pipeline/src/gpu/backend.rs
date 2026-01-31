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
pub struct CpuCompressor {
    compression_level: u32,
    threads: u32,
}

impl CpuCompressor {
    /// Create a new CPU compressor with the given settings.
    pub fn new(compression_level: u32, threads: u32) -> Self {
        Self {
            compression_level,
            threads,
        }
    }

    /// Create a CPU compressor with default settings.
    #[allow(dead_code)]
    pub fn default_config() -> Self {
        Self {
            compression_level: 3,
            threads: crate::hardware::detect_cpu_count(),
        }
    }
}

impl CompressorBackend for CpuCompressor {
    fn compress_chunk(&self, chunk: &ChunkToCompress) -> GpuResult<CompressedChunk> {
        let mut compressor =
            zstd::bulk::Compressor::new(self.compression_level as i32).map_err(|e| {
                GpuCompressionError::CompressionFailed(format!(
                    "Failed to create CPU compressor: {}",
                    e
                ))
            })?;

        let compressed = compressor.compress(&chunk.data).map_err(|e| {
            GpuCompressionError::CompressionFailed(format!("CPU compression failed: {}", e))
        })?;

        Ok(CompressedChunk {
            sequence: chunk.sequence,
            channel_id: chunk.channel_id,
            compressed_data: compressed.to_vec(),
            original_size: chunk.data.len(),
        })
    }

    fn compress_parallel(&self, chunks: &[ChunkToCompress]) -> GpuResult<Vec<CompressedChunk>> {
        use rayon::prelude::*;

        if chunks.is_empty() {
            return Ok(Vec::new());
        }

        let compression_level = self.compression_level as i32;

        // Process chunks in parallel using rayon
        let results: Result<Vec<_>, _> = chunks
            .par_iter()
            .map(|chunk| {
                let mut compressor =
                    zstd::bulk::Compressor::new(compression_level).map_err(|e| {
                        GpuCompressionError::CompressionFailed(format!(
                            "Failed to create compressor: {}",
                            e
                        ))
                    })?;

                let compressed = compressor.compress(&chunk.data).map_err(|e| {
                    GpuCompressionError::CompressionFailed(format!("Compression failed: {}", e))
                })?;

                Ok(CompressedChunk {
                    sequence: chunk.sequence,
                    channel_id: chunk.channel_id,
                    compressed_data: compressed.to_vec(),
                    original_size: chunk.data.len(),
                })
            })
            .collect();

        results
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
