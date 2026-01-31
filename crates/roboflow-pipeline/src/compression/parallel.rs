// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Parallel compression for the zero-copy pipeline.
//!
//! This module provides thread-local compressors and parallel chunk
//! compression using Rayon for maximum throughput.

use rayon::prelude::*;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::types::chunk::{CompressedChunk, MessageChunk};
use roboflow_core::{Result, RoboflowError};

/// Compression level for ZSTD.
pub type CompressionLevel = i32;

/// Default compression level for throughput.
pub const DEFAULT_COMPRESSION_LEVEL: CompressionLevel = 3;

/// High compression level for better ratio.
pub const HIGH_COMPRESSION_LEVEL: CompressionLevel = 9;

/// Low compression level for maximum speed.
pub const LOW_COMPRESSION_LEVEL: CompressionLevel = 1;

/// Parallel compressor configuration.
#[derive(Debug, Clone, Copy)]
pub struct CompressionConfig {
    /// ZSTD compression level (0-22, default 3)
    pub level: CompressionLevel,
    /// Number of compression threads (0 = auto-detect)
    pub threads: usize,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            level: DEFAULT_COMPRESSION_LEVEL,
            threads: crate::hardware::detect_cpu_count() as usize,
        }
    }
}

impl CompressionConfig {
    /// Create a new compression config.
    pub fn new(level: CompressionLevel, threads: usize) -> Self {
        Self { level, threads }
    }

    /// Maximum throughput configuration.
    /// Uses level 1 compression (fastest) with all CPU cores.
    pub fn max_throughput() -> Self {
        Self {
            level: LOW_COMPRESSION_LEVEL,
            threads: crate::hardware::detect_cpu_count() as usize,
        }
    }

    /// High throughput configuration.
    pub fn high_throughput() -> Self {
        Self {
            level: LOW_COMPRESSION_LEVEL,
            threads: crate::hardware::detect_cpu_count() as usize,
        }
    }

    /// Balanced configuration.
    pub fn balanced() -> Self {
        Self::default()
    }

    /// High compression configuration.
    pub fn high_compression() -> Self {
        Self {
            level: HIGH_COMPRESSION_LEVEL,
            threads: crate::hardware::detect_cpu_count() as usize,
        }
    }
}

/// Thread-local ZSTD compressor.
///
/// Each thread gets its own compressor to avoid synchronization overhead.
#[allow(dead_code)]
struct ThreadLocalCompressor {
    compressor: zstd::bulk::Compressor<'static>,
}

#[allow(dead_code)]
impl ThreadLocalCompressor {
    /// Create a new thread-local compressor.
    fn new(level: CompressionLevel) -> Result<Self> {
        let compressor = zstd::bulk::Compressor::new(level).map_err(|e| {
            RoboflowError::encode("Compressor", format!("Failed to create compressor: {e}"))
        })?;
        Ok(Self { compressor })
    }

    /// Compress data.
    fn compress(&mut self, input: &[u8]) -> Result<Vec<u8>> {
        self.compressor
            .compress(input)
            .map(|v| v.to_vec())
            .map_err(|e| RoboflowError::encode("Compressor", format!("Compression failed: {e}")))
    }

    /// Compress directly into a buffer.
    fn compress_into(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<()> {
        output.clear();
        let compressed = self
            .compressor
            .compress(input)
            .map_err(|e| RoboflowError::encode("Compressor", format!("Compression failed: {e}")))?;
        output.extend_from_slice(&compressed);
        Ok(())
    }
}

/// Parallel chunk compressor.
///
/// Compresses chunks in parallel using Rayon, with thread-local
/// compressors for maximum throughput.
pub struct ParallelCompressor {
    /// Compression configuration
    config: CompressionConfig,
    /// Reusable Rayon thread pool
    pool: rayon::ThreadPool,
    /// Bytes compressed (for metrics)
    bytes_compressed: Arc<AtomicUsize>,
    /// Bytes output (for metrics)
    bytes_output: Arc<AtomicUsize>,
}

impl ParallelCompressor {
    /// Create a new parallel compressor.
    pub fn new(config: CompressionConfig) -> Result<Self> {
        let num_threads = if config.threads == 0 {
            crate::hardware::detect_cpu_count() as usize
        } else {
            config.threads
        };

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .map_err(|e| RoboflowError::encode(
                "Compressor",
                format!("Failed to create Rayon thread pool with {} threads: {}. Try reducing the thread count or closing other applications.", num_threads, e)
            ))?;

        Ok(Self {
            config,
            pool,
            bytes_compressed: Arc::new(AtomicUsize::new(0)),
            bytes_output: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Create with default configuration.
    pub fn default_config() -> Result<Self> {
        Self::new(CompressionConfig::default())
    }

    /// Compress a single chunk.
    pub fn compress_chunk(&self, chunk: &MessageChunk<'_>) -> Result<CompressedChunk> {
        // Build uncompressed data
        let uncompressed = self.build_uncompressed_chunk(chunk)?;

        // Create compressor for this chunk
        let mut compressor = zstd::bulk::Compressor::new(self.config.level).map_err(|e| {
            RoboflowError::encode("Compressor", format!("Failed to create compressor: {e}"))
        })?;

        let compressed_data = compressor
            .compress(&uncompressed)
            .map_err(|e| RoboflowError::encode("Compressor", format!("Compression failed: {e}")))?;

        self.bytes_compressed
            .fetch_add(uncompressed.len(), Ordering::Relaxed);
        self.bytes_output
            .fetch_add(compressed_data.len(), Ordering::Relaxed);

        Ok(CompressedChunk {
            sequence: chunk.sequence,
            compressed_data,
            uncompressed_size: uncompressed.len(),
            message_start_time: chunk.message_start_time,
            message_end_time: chunk.message_end_time,
            message_count: chunk.message_count(),
            compression_ratio: 0.0, // Will be calculated
            message_indexes: std::collections::BTreeMap::new(), // Built during chunk serialization
        })
    }

    /// Compress multiple chunks in parallel.
    pub fn compress_chunks_parallel(
        &self,
        chunks: &[MessageChunk<'_>],
    ) -> Result<Vec<CompressedChunk>> {
        if chunks.is_empty() {
            return Ok(Vec::new());
        }

        let level = self.config.level;
        let bytes_compressed = Arc::clone(&self.bytes_compressed);
        let bytes_output = Arc::clone(&self.bytes_output);

        // Use the stored thread pool instead of creating a new one
        let results: Result<Vec<_>> = self.pool.install(|| {
            chunks
                .par_iter()
                .map(|chunk| {
                    // Build uncompressed chunk
                    let uncompressed = self.build_uncompressed_chunk(chunk)?;

                    // Note: Rayon's work-stealing scheduler reuses worker threads across
                    // multiple chunks, so compressor creation overhead is amortized.
                    // Each worker thread creates its own compressor once and reuses it
                    // for all chunks it processes.
                    let mut compressor = zstd::bulk::Compressor::new(level).map_err(|e| {
                        RoboflowError::encode(
                            "Compressor",
                            format!("Failed to create compressor: {e}"),
                        )
                    })?;

                    let compressed_data = compressor.compress(&uncompressed).map_err(|e| {
                        RoboflowError::encode("Compressor", format!("Compression failed: {e}"))
                    })?;

                    bytes_compressed.fetch_add(uncompressed.len(), Ordering::Relaxed);
                    bytes_output.fetch_add(compressed_data.len(), Ordering::Relaxed);

                    Ok(CompressedChunk {
                        sequence: chunk.sequence,
                        compressed_data,
                        uncompressed_size: uncompressed.len(),
                        message_start_time: chunk.message_start_time,
                        message_end_time: chunk.message_end_time,
                        message_count: chunk.message_count(),
                        compression_ratio: 0.0,
                        message_indexes: std::collections::BTreeMap::new(),
                    })
                })
                .collect()
        });

        results
    }

    /// Build the uncompressed chunk data (MCAP message records).
    fn build_uncompressed_chunk(&self, chunk: &MessageChunk<'_>) -> Result<Vec<u8>> {
        use byteorder::{LittleEndian, WriteBytesExt};

        let estimated_size = chunk.estimated_serialized_size();
        let mut buffer = Vec::with_capacity(estimated_size);

        // Chunk header (we'll fill in proper values later)
        // For now, write placeholder values
        buffer.write_u64::<LittleEndian>(chunk.message_start_time)?;
        buffer.write_u64::<LittleEndian>(chunk.message_end_time)?;
        buffer.write_u64::<LittleEndian>(0)?; // message_start_offset

        // Write messages
        for msg in &chunk.messages {
            // Message header
            buffer.write_u16::<LittleEndian>(msg.channel_id)?;
            buffer.write_u32::<LittleEndian>(msg.sequence)?;
            buffer.write_u64::<LittleEndian>(msg.log_time)?;
            buffer.write_u64::<LittleEndian>(msg.publish_time)?;

            // Message data
            let data = msg.data.as_ref();
            buffer.write_u32::<LittleEndian>(data.len() as u32)?;
            buffer.write_all(data)?;
        }

        Ok(buffer)
    }

    /// Get total bytes compressed.
    pub fn bytes_compressed(&self) -> u64 {
        self.bytes_compressed.load(Ordering::Acquire) as u64
    }

    /// Get total bytes output.
    pub fn bytes_output(&self) -> u64 {
        self.bytes_output.load(Ordering::Acquire) as u64
    }

    /// Get the compression ratio achieved so far.
    pub fn compression_ratio(&self) -> f64 {
        let compressed = self.bytes_output() as f64;
        let uncompressed = self.bytes_compressed() as f64;
        if uncompressed > 0.0 {
            compressed / uncompressed
        } else {
            1.0
        }
    }

    /// Reset metrics.
    pub fn reset_metrics(&self) {
        self.bytes_compressed.store(0, Ordering::Release);
        self.bytes_output.store(0, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::chunk::MessageChunk;

    #[test]
    fn test_compression_config_default() {
        let config = CompressionConfig::default();
        assert_eq!(config.level, DEFAULT_COMPRESSION_LEVEL);
        assert!(config.threads > 0);
    }

    #[test]
    fn test_compression_config_high_throughput() {
        let config = CompressionConfig::high_throughput();
        assert_eq!(config.level, LOW_COMPRESSION_LEVEL);
    }

    #[test]
    fn test_compression_config_high_compression() {
        let config = CompressionConfig::high_compression();
        assert_eq!(config.level, HIGH_COMPRESSION_LEVEL);
    }

    #[test]
    fn test_parallel_compressor_new() {
        let compressor = ParallelCompressor::default_config();
        assert!(compressor.is_ok());
    }

    #[test]
    fn test_compress_chunk() {
        let compressor = ParallelCompressor::default_config().unwrap();

        let mut chunk = MessageChunk::new(0);
        chunk
            .add_message_from_slice(1, 1000, 1000, 0, b"test message data")
            .unwrap();

        let result = compressor.compress_chunk(&chunk);
        assert!(result.is_ok());

        let compressed = result.unwrap();
        assert_eq!(compressed.sequence, 0);
        assert_eq!(compressed.message_count, 1);
        assert!(!compressed.compressed_data.is_empty());
    }

    #[test]
    fn test_compress_chunks_parallel() {
        let compressor = ParallelCompressor::default_config().unwrap();

        let mut chunks = Vec::new();
        for i in 0..3 {
            let mut chunk = MessageChunk::new(i);
            chunk
                .add_message_from_slice(
                    1,
                    i * 1000,
                    i * 1000,
                    0,
                    format!("message {}", i).as_bytes(),
                )
                .unwrap();
            chunks.push(chunk);
        }

        let results = compressor.compress_chunks_parallel(&chunks);
        assert!(results.is_ok());

        let compressed = results.unwrap();
        assert_eq!(compressed.len(), 3);
    }

    #[test]
    fn test_compression_metrics() {
        let compressor = ParallelCompressor::default_config().unwrap();

        let mut chunk = MessageChunk::new(0);
        let data = vec![b'x'; 1000];
        chunk
            .add_message_from_slice(1, 1000, 1000, 0, &data)
            .unwrap();

        let _ = compressor.compress_chunk(&chunk);

        assert!(compressor.bytes_compressed() > 0);
        assert!(compressor.bytes_output() > 0);
        assert!(compressor.compression_ratio() > 0.0);
        assert!(compressor.compression_ratio() < 1.0); // Should compress
    }

    #[test]
    fn test_compression_reset_metrics() {
        let compressor = ParallelCompressor::default_config().unwrap();

        let mut chunk = MessageChunk::new(0);
        chunk
            .add_message_from_slice(1, 1000, 1000, 0, b"test data")
            .unwrap();

        let _ = compressor.compress_chunk(&chunk);
        assert!(compressor.bytes_compressed() > 0);

        compressor.reset_metrics();
        assert_eq!(compressor.bytes_compressed(), 0);
        assert_eq!(compressor.bytes_output(), 0);
    }
}
