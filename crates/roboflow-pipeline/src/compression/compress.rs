// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Compression pool with multi-threaded ZSTD compression.
//!
//! This module also provides shared low-level compression utilities
//! ([`compress_data`], [`create_zstd_compressor`], [`compress_with`])
//! used by all compression backends across the pipeline crate.

use rayon::prelude::*;

use crate::config::CompressionConfig;
use roboflow_core::{Result, RoboflowError};

// ---------------------------------------------------------------------------
// Shared low-level ZSTD compression utilities
// ---------------------------------------------------------------------------

/// Create a new ZSTD bulk compressor with the given compression level.
///
/// This centralises the compressor creation + error mapping pattern so that
/// every call-site in the crate uses a consistent error message.
pub fn create_zstd_compressor(level: i32) -> Result<zstd::bulk::Compressor<'static>> {
    zstd::bulk::Compressor::new(level)
        .map_err(|e| RoboflowError::encode("zstd", format!("Failed to create compressor: {e}")))
}

/// Compress `data` using an **existing** ZSTD compressor.
///
/// Use this when you keep a long-lived compressor (e.g. one per worker
/// thread) and want to avoid re-creating it on every call.
pub fn compress_with(compressor: &mut zstd::bulk::Compressor<'_>, data: &[u8]) -> Result<Vec<u8>> {
    compressor
        .compress(data)
        .map_err(|e| RoboflowError::encode("zstd", format!("Compression failed: {e}")))
}

/// Compress `data` with ZSTD at the given compression level.
///
/// This is a convenience wrapper that creates a one-shot compressor
/// internally.  For repeated compression prefer [`create_zstd_compressor`]
/// + [`compress_with`] to amortise compressor creation.
pub fn compress_data(data: &[u8], level: i32) -> Result<Vec<u8>> {
    let mut compressor = create_zstd_compressor(level)?;
    compress_with(&mut compressor, data)
}

/// Chunk of data to be compressed.
#[derive(Debug, Clone)]
pub struct ChunkToCompress {
    pub sequence: u64,
    pub channel_id: u16,
    pub data: Vec<u8>,
}

/// Compressed chunk ready for writing (internal to compression module).
#[derive(Debug, Clone)]
pub struct CompressedDataChunk {
    pub sequence: u64,
    pub channel_id: u16,
    pub compressed_data: Vec<u8>,
    pub original_size: usize,
}

/// Parallel compression pool.
pub struct CompressionPool {
    config: CompressionConfig,
}

impl CompressionPool {
    /// Create a new compression pool with the given configuration.
    pub fn new(config: CompressionConfig) -> Result<Self> {
        Ok(Self { config })
    }

    /// Create from compression config.
    pub fn from_config(config: CompressionConfig) -> Self {
        Self { config }
    }

    /// Compress chunks in parallel using thread-local compressors.
    pub fn compress_parallel(
        &self,
        chunks: &[ChunkToCompress],
    ) -> Result<Vec<CompressedDataChunk>> {
        if chunks.is_empty() {
            return Ok(Vec::new());
        }

        let compression_enabled = self.config.enabled;
        let compression_level = self.config.compression_level;

        // Process chunks in parallel using rayon
        let results: Result<Vec<_>> = chunks
            .par_iter()
            .map(|chunk| {
                if !compression_enabled {
                    return Ok(CompressedDataChunk {
                        sequence: chunk.sequence,
                        channel_id: chunk.channel_id,
                        compressed_data: chunk.data.clone(),
                        original_size: chunk.data.len(),
                    });
                }

                let compressed = compress_data(&chunk.data, compression_level)?;

                Ok(CompressedDataChunk {
                    sequence: chunk.sequence,
                    channel_id: chunk.channel_id,
                    compressed_data: compressed,
                    original_size: chunk.data.len(),
                })
            })
            .collect();

        results
    }

    /// Compress a single chunk.
    pub fn compress_chunk(&self, chunk: &ChunkToCompress) -> Result<CompressedDataChunk> {
        if !self.config.enabled {
            return Ok(CompressedDataChunk {
                sequence: chunk.sequence,
                channel_id: chunk.channel_id,
                compressed_data: chunk.data.clone(),
                original_size: chunk.data.len(),
            });
        }

        let compressed = compress_data(&chunk.data, self.config.compression_level)?;

        Ok(CompressedDataChunk {
            sequence: chunk.sequence,
            channel_id: chunk.channel_id,
            compressed_data: compressed,
            original_size: chunk.data.len(),
        })
    }

    /// Get the compression config.
    pub fn config(&self) -> &CompressionConfig {
        &self.config
    }
}

impl Default for CompressionPool {
    fn default() -> Self {
        Self::from_config(CompressionConfig::default())
    }
}
