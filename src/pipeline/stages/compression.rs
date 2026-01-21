//! Compression stage - compresses chunks in parallel.
//!
//! The compression stage is responsible for:
//! - Receiving chunks from the reader stage
//! - Spawning multiple worker threads for parallel compression
//! - Sending compressed chunks to the writer stage
//! - Managing thread-local compressors

use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use byteorder::{LittleEndian, WriteBytesExt};
use crossbeam_channel::{Receiver, Sender};

use crate::core::{Result, RoboflowError};
use crate::pipeline::types::buffer_pool::{BufferPool, PooledBuffer};
use robocodec::io::traits::MessageChunkData;
use robocodec::types::chunk::CompressedChunk;

/// Compressed chunk with pooled buffer support.
///
/// The compressed_data is a PooledBuffer that automatically returns
/// itself to the buffer pool when dropped, eliminating deallocation overhead.
pub struct PooledCompressedChunk {
    /// Chunk sequence number
    pub sequence: u64,
    /// Compressed data in a pooled buffer (returns to pool when dropped)
    pub compressed_data: PooledBuffer,
    /// Uncompressed size
    pub uncompressed_size: usize,
    /// Message start time (earliest log_time)
    pub message_start_time: u64,
    /// Message end time (latest log_time)
    pub message_end_time: u64,
    /// Number of messages in this chunk
    pub message_count: usize,
    /// Compression ratio (compressed / uncompressed)
    pub compression_ratio: f64,
}

impl PooledCompressedChunk {
    /// Convert to a regular CompressedChunk by cloning the data.
    ///
    /// Note: This allocates a new Vec, so use sparingly.
    /// Ideally, the writer should accept PooledCompressedChunk directly.
    pub fn to_compressed_chunk(&self) -> CompressedChunk {
        CompressedChunk {
            sequence: self.sequence,
            compressed_data: self.compressed_data.as_ref().to_vec(),
            uncompressed_size: self.uncompressed_size,
            message_start_time: self.message_start_time,
            message_end_time: self.message_end_time,
            message_count: self.message_count,
            compression_ratio: self.compression_ratio,
            message_indexes: std::collections::BTreeMap::new(), // Not used in pooled path
        }
    }
}

/// Compression backend selection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CompressionBackend {
    /// Software ZSTD (default, cross-platform)
    #[default]
    Zstd,
}

/// Configuration for the compression stage.
#[derive(Debug, Clone)]
pub struct CompressionStageConfig {
    /// Number of compression threads
    pub num_threads: usize,
    /// ZSTD compression level
    pub compression_level: i32,
    /// ZSTD window log (2^window_log = max window size).
    /// None uses Zstd default (typically 27 = 128MB).
    /// Set based on your chunk size to reduce cache thrashing.
    /// For example: 22 = 4MB, 23 = 8MB, 24 = 16MB.
    pub window_log: Option<u32>,
    /// Target chunk size (for building uncompressed data)
    pub target_chunk_size: usize,
    /// Compression backend to use
    pub backend: CompressionBackend,
    /// Buffer pool for reusing compression output buffers
    pub buffer_pool: BufferPool,
}

impl Default for CompressionStageConfig {
    fn default() -> Self {
        Self {
            num_threads: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(8),
            compression_level: 3,
            window_log: None, // Use Zstd default
            target_chunk_size: 16 * 1024 * 1024,
            backend: CompressionBackend::default(),
            buffer_pool: BufferPool::new(),
        }
    }
}

/// Compression stage - compresses chunks in parallel.
///
/// This stage spawns multiple worker threads that each pull chunks from
/// the input channel and compress them independently, achieving maximum
/// CPU utilization through work sharing.
pub struct CompressionStage {
    /// Compression configuration
    config: CompressionStageConfig,
    /// Channel for receiving chunks from reader
    chunks_receiver: Receiver<MessageChunkData>,
    /// Channel for sending compressed chunks to writer
    chunks_sender: Sender<CompressedChunk>,
    /// Statistics
    stats: Arc<CompressionStats>,
}

/// Statistics from the compression stage.
#[derive(Debug, Default)]
struct CompressionStats {
    /// Chunks received
    chunks_received: AtomicU64,
    /// Chunks compressed
    chunks_compressed: AtomicU64,
    /// Uncompressed bytes
    uncompressed_bytes: AtomicU64,
    /// Compressed bytes
    compressed_bytes: AtomicU64,
}

impl CompressionStage {
    /// Create a new compression stage.
    pub fn new(
        config: CompressionStageConfig,
        chunks_receiver: Receiver<MessageChunkData>,
        chunks_sender: Sender<CompressedChunk>,
    ) -> Self {
        Self {
            config,
            chunks_receiver,
            chunks_sender,
            stats: Arc::new(CompressionStats::default()),
        }
    }

    /// Spawn the compression stage in a new thread.
    pub fn spawn(self) -> Result<std::thread::JoinHandle<Result<()>>> {
        let handle = thread::spawn(move || self.run());
        Ok(handle)
    }

    /// Run the compression stage.
    ///
    /// This method spawns multiple worker threads that each pull chunks
    /// from the channel and compress them in parallel.
    fn run(self) -> Result<()> {
        println!(
            "Starting compression stage with {} worker threads...",
            self.config.num_threads
        );

        let start = Instant::now();

        // Clone the Arc'd stats for sharing across workers
        let stats = Arc::clone(&self.stats);
        // Clone the buffer pool for sharing across workers
        let buffer_pool = self.config.buffer_pool.clone();

        // Spawn multiple compression workers
        let mut worker_handles = Vec::new();
        for worker_id in 0..self.config.num_threads {
            let receiver = self.chunks_receiver.clone();
            let sender = self.chunks_sender.clone();
            let stats = Arc::clone(&stats);
            let compression_level = self.config.compression_level;
            let backend = self.config.backend;
            let buffer_pool = buffer_pool.clone();

            let handle = thread::spawn(move || {
                Self::compression_worker(
                    worker_id,
                    receiver,
                    sender,
                    stats,
                    compression_level,
                    self.config.window_log,
                    backend,
                    buffer_pool,
                )
            });

            worker_handles.push(handle);
        }

        // Drop the original sender/receiver - workers own them now
        drop(self.chunks_sender);
        drop(self.chunks_receiver);

        // Wait for all workers to complete
        let mut worker_errors = Vec::new();
        for handle in worker_handles {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => worker_errors.push(e.to_string()),
                Err(_) => worker_errors.push("Compression worker panicked".to_string()),
            }
        }

        if !worker_errors.is_empty() {
            return Err(RoboflowError::encode(
                "CompressionStage",
                format!("Worker errors: {}", worker_errors.join(", ")),
            ));
        }

        let duration = start.elapsed();

        let chunks_compressed = stats.chunks_compressed.load(Ordering::Relaxed);
        let uncompressed = stats.uncompressed_bytes.load(Ordering::Relaxed);
        let compressed = stats.compressed_bytes.load(Ordering::Relaxed);

        println!(
            "Compression stage complete: {} chunks, {:.2} MB → {:.2} MB ({:.2}x ratio) in {:.2}s",
            chunks_compressed,
            uncompressed as f64 / (1024.0 * 1024.0),
            compressed as f64 / (1024.0 * 1024.0),
            if uncompressed > 0 {
                compressed as f64 / uncompressed as f64
            } else {
                1.0
            },
            duration.as_secs_f64()
        );

        Ok(())
    }

    /// Compression worker - pulls chunks from channel and compresses them.
    #[allow(clippy::too_many_arguments)]
    fn compression_worker(
        worker_id: usize,
        receiver: Receiver<MessageChunkData>,
        sender: Sender<CompressedChunk>,
        stats: Arc<CompressionStats>,
        compression_level: i32,
        window_log: Option<u32>,
        _backend: CompressionBackend,
        buffer_pool: BufferPool,
    ) -> Result<()> {
        // Create thread-local compressor based on backend
        let mut zstd_compressor = zstd::bulk::Compressor::new(compression_level).map_err(|e| {
            RoboflowError::encode(
                "CompressionStage",
                format!("Failed to create ZSTD compressor: {e}"),
            )
        })?;

        // Set window log if specified (reduces cache thrashing for smaller chunks)
        if let Some(wlog) = window_log {
            // Zstd's window log parameter controls the maximum history size
            // Setting this to match your chunk size keeps the compression context in L3 cache
            if let Err(e) =
                zstd_compressor.set_parameter(zstd::stream::raw::CParameter::WindowLog(wlog))
            {
                tracing::debug!("Failed to set WindowLog to {}: {}", wlog, e);
            } else {
                tracing::debug!("Worker {} using WindowLog={}", worker_id, wlog);
            }
        }

        // Buffer reuse strategy:
        // 1. Keep a cached buffer that we reuse across iterations
        // 2. After compression, swap with zstd's output (keeps capacity)
        // 3. Take ownership of the compressed buffer for sending to writer
        // 4. The old cached buffer becomes our new cached buffer for next iteration
        // This eliminates the 10% deallocation overhead from constantly dropping Vecs
        let mut uncompressed_buffer: Vec<u8> = Vec::with_capacity(32 * 1024 * 1024);
        let mut cached_buffer: Vec<u8> = Vec::with_capacity(16 * 1024 * 1024);
        let mut message_indexes: std::collections::BTreeMap<
            u16,
            Vec<crate::pipeline::types::chunk::MessageIndexEntry>,
        > = std::collections::BTreeMap::new();

        while let Ok(chunk) = receiver.recv() {
            stats.chunks_received.fetch_add(1, Ordering::Relaxed);

            let sequence = chunk.sequence;

            // Build uncompressed data into reused buffer, also capturing message indexes
            uncompressed_buffer.clear();
            Self::build_uncompressed_chunk_into_buffer(
                &chunk,
                &mut uncompressed_buffer,
                &mut message_indexes,
            )?;

            // Compress using ZSTD backend
            let compressed_data = {
                // Compress - zstd allocates a new Vec
                let mut compressed =
                    zstd_compressor
                        .compress(&uncompressed_buffer)
                        .map_err(|e| {
                            RoboflowError::encode(
                                "CompressionStage",
                                format!("ZSTD compression failed: {e}"),
                            )
                        })?;

                // Swap our cached buffer with the newly allocated compressed buffer
                // After swap: cached_buffer has compressed data, compressed has old capacity
                std::mem::swap(&mut cached_buffer, &mut compressed);

                // Return the old buffer (now in 'compressed') to the global pool
                // This allows other workers to reuse this capacity
                // Only return buffers with meaningful capacity
                if compressed.capacity() >= 1024 {
                    buffer_pool.return_buffer(compressed);
                }
                // else: drop small buffer, let it deallocate

                // Take the data out of cached_buffer without cloning!
                // mem::take replaces cached_buffer with an empty Vec (same capacity)
                // This is a zero-cost move - no allocation, no copy
                std::mem::take(&mut cached_buffer)
            };

            // Update stats
            stats
                .uncompressed_bytes
                .fetch_add(uncompressed_buffer.len() as u64, Ordering::Relaxed);
            stats
                .compressed_bytes
                .fetch_add(compressed_data.len() as u64, Ordering::Relaxed);

            // Calculate compression ratio
            let compression_ratio = if !uncompressed_buffer.is_empty() {
                compressed_data.len() as f64 / uncompressed_buffer.len() as f64
            } else {
                1.0
            };

            let compressed_chunk = CompressedChunk {
                sequence,
                compressed_data,
                uncompressed_size: uncompressed_buffer.len(),
                message_start_time: chunk.message_start_time,
                message_end_time: chunk.message_end_time,
                message_count: chunk.message_count(),
                compression_ratio,
                message_indexes: message_indexes.clone(),
            };

            // Send to writer (blocks if channel is full)
            if sender.send(compressed_chunk).is_err() {
                return Err(RoboflowError::encode(
                    "CompressionStage",
                    format!("Worker {} failed to send compressed chunk", worker_id),
                ));
            }

            stats.chunks_compressed.fetch_add(1, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Build the uncompressed chunk data (MCAP message records) - worker version.
    ///
    /// Each message is written as a proper MCAP message record:
    /// - opcode: 0x05 (1 byte)
    /// - record_length: u64 (the length of the fields that follow)
    /// - channel_id: u16
    /// - sequence: u32
    /// - log_time: u64
    /// - publish_time: u64
    /// - data: bytes[]
    ///
    /// Also builds message indexes for each channel, tracking (log_time, offset) pairs.
    fn build_uncompressed_chunk_into_buffer(
        chunk: &MessageChunkData,
        buffer: &mut Vec<u8>,
        message_indexes: &mut std::collections::BTreeMap<
            u16,
            Vec<crate::pipeline::types::chunk::MessageIndexEntry>,
        >,
    ) -> Result<()> {
        use robocodec::types::chunk::MessageIndexEntry;
        const OP_MESSAGE: u8 = 0x05;

        let total_size = chunk.total_data_size();
        let estimated_size = total_size + (chunk.messages.len() * (2 + 4 + 8 + 8 + 8)); // headers per message
        if buffer.capacity() < estimated_size {
            buffer.reserve(estimated_size - buffer.capacity());
        }

        // Clear previous indexes
        message_indexes.clear();

        // Write messages as proper MCAP message records
        for msg in &chunk.messages {
            let data = &msg.data;

            // Record the offset BEFORE writing this message (offset within uncompressed chunk)
            let offset = buffer.len() as u64;

            // Add to message index for this channel
            message_indexes
                .entry(msg.channel_id)
                .or_default()
                .push(MessageIndexEntry {
                    log_time: msg.log_time,
                    offset,
                });

            // Message record: opcode + record_length + channel_id + sequence + log_time + publish_time + data
            buffer.push(OP_MESSAGE);

            // Record length = 2 (channel_id) + 4 (sequence) + 8 (log_time) + 8 (publish_time) + data.len()
            let record_len: u64 = 2 + 4 + 8 + 8 + data.len() as u64;
            buffer.write_u64::<LittleEndian>(record_len)?;

            buffer.write_u16::<LittleEndian>(msg.channel_id)?;
            buffer.write_u32::<LittleEndian>(msg.sequence.unwrap_or(0) as u32)?;
            buffer.write_u64::<LittleEndian>(msg.log_time)?;
            buffer.write_u64::<LittleEndian>(msg.publish_time)?;
            buffer.write_all(data)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_config_default() {
        let config = CompressionStageConfig::default();
        assert!(config.num_threads > 0);
        assert_eq!(config.compression_level, 3);
        assert_eq!(config.target_chunk_size, 16 * 1024 * 1024);
    }
}
