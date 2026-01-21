// Copyright (c) 2026 ArcheBase
// Roboflow is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//     http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

//! Compression pool with multi-threaded ZSTD compression.

use rayon::prelude::*;

use crate::core::{Result, RoboflowError};
use crate::pipeline::config::CompressionConfig;

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
        let compression_level = self.config.compression_level as i32;

        // Process chunks in parallel using rayon
        // Each thread creates its own compressor
        let results: Result<Vec<_>> = chunks
            .par_iter() // Parallel iteration
            .map(|chunk| {
                if !compression_enabled {
                    // No compression, just copy data
                    return Ok(CompressedDataChunk {
                        sequence: chunk.sequence,
                        channel_id: chunk.channel_id,
                        compressed_data: chunk.data.clone(),
                        original_size: chunk.data.len(),
                    });
                }

                // Create a compressor for this thread
                let mut compressor =
                    zstd::bulk::Compressor::new(compression_level).map_err(|e| {
                        RoboflowError::encode(
                            "CompressionPool",
                            format!("Failed to create compressor: {e}"),
                        )
                    })?;

                // Compress using ZSTD
                let compressed = compressor.compress(&chunk.data).map_err(|e| {
                    RoboflowError::encode("CompressionPool", format!("Compression failed: {e}"))
                })?;

                Ok(CompressedDataChunk {
                    sequence: chunk.sequence,
                    channel_id: chunk.channel_id,
                    compressed_data: compressed.to_vec(),
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

        let mut compressor = zstd::bulk::Compressor::new(self.config.compression_level as i32)
            .map_err(|e| {
                RoboflowError::encode(
                    "CompressionPool",
                    format!("Failed to create compressor: {e}"),
                )
            })?;

        let compressed = compressor.compress(&chunk.data).map_err(|e| {
            RoboflowError::encode("CompressionPool", format!("Compression failed: {e}"))
        })?;

        Ok(CompressedDataChunk {
            sequence: chunk.sequence,
            channel_id: chunk.channel_id,
            compressed_data: compressed.to_vec(),
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
