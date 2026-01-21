//! Stage 2+3: Parser/Slicer + Batcher
//!
//! This stage combines parsing and batching for efficiency:
//! - Decompresses chunks (zstd/lz4)
//! - Parses MCAP message records
//! - Allocates messages into arena
//! - Batches messages into chunks for compression

use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use byteorder::{LittleEndian, ReadBytesExt};
use crossbeam_channel::{Receiver, Sender};
use tracing::{debug, info, instrument, warn};

use crate::core::{CodecError, Result};
use crate::pipeline::hyper::types::{BlockType, CompressionType, ParserStats, PrefetchedBlock};
use crate::pipeline::types::arena_pool::global_pool;
use crate::pipeline::types::buffer_pool::BufferPool;
use crate::pipeline::types::chunk::MessageChunk;

/// Configuration for the parser/slicer stage.
#[derive(Debug, Clone)]
pub struct ParserSlicerConfig {
    /// Number of worker threads
    pub num_workers: usize,
    /// Target chunk size for batching (bytes)
    pub target_chunk_size: usize,
    /// Maximum messages per chunk
    pub max_messages_per_chunk: usize,
    /// Buffer pool for decompression
    pub buffer_pool: BufferPool,
}

impl Default for ParserSlicerConfig {
    fn default() -> Self {
        Self {
            num_workers: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
            target_chunk_size: 16 * 1024 * 1024, // 16MB
            max_messages_per_chunk: 250_000,
            buffer_pool: BufferPool::new(),
        }
    }
}

/// Stage 2+3: Parser/Slicer with integrated batching.
pub struct ParserSlicerStage {
    config: ParserSlicerConfig,
    receiver: Receiver<PrefetchedBlock>,
    sender: Sender<MessageChunk<'static>>,
    stats: Arc<ParserSlicerStats>,
}

#[derive(Debug, Default)]
struct ParserSlicerStats {
    blocks_processed: AtomicU64,
    messages_parsed: AtomicU64,
    chunks_produced: AtomicU64,
    decompress_bytes: AtomicU64,
    /// Global sequence counter for unique chunk IDs across all workers
    next_sequence: AtomicU64,
}

impl ParserSlicerStage {
    /// Create a new parser/slicer stage.
    pub fn new(
        config: ParserSlicerConfig,
        receiver: Receiver<PrefetchedBlock>,
        sender: Sender<MessageChunk<'static>>,
    ) -> Self {
        Self {
            config,
            receiver,
            sender,
            stats: Arc::new(ParserSlicerStats::default()),
        }
    }

    /// Spawn the stage in a new thread.
    pub fn spawn(self) -> Result<thread::JoinHandle<Result<ParserStats>>> {
        let handle = thread::spawn(move || self.run());
        Ok(handle)
    }

    /// Run the parser/slicer stage.
    #[instrument(skip_all)]
    fn run(self) -> Result<ParserStats> {
        info!(
            workers = self.config.num_workers,
            "Starting parser/slicer stage"
        );

        let start = Instant::now();

        // Spawn worker threads
        let mut worker_handles = Vec::new();

        for worker_id in 0..self.config.num_workers {
            let receiver = self.receiver.clone();
            let sender = self.sender.clone();
            let stats = Arc::clone(&self.stats);
            let target_chunk_size = self.config.target_chunk_size;
            let max_messages = self.config.max_messages_per_chunk;
            let buffer_pool = self.config.buffer_pool.clone();

            let handle = thread::spawn(move || {
                Self::worker(
                    worker_id,
                    receiver,
                    sender,
                    stats,
                    target_chunk_size,
                    max_messages,
                    buffer_pool,
                )
            });

            worker_handles.push(handle);
        }

        // Drop our references so workers own the channels
        drop(self.receiver);
        drop(self.sender);

        // Wait for workers
        let mut worker_errors = Vec::new();
        for handle in worker_handles {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => worker_errors.push(e.to_string()),
                Err(_) => worker_errors.push("Parser worker panicked".to_string()),
            }
        }

        if !worker_errors.is_empty() {
            return Err(CodecError::encode(
                "ParserSlicer",
                format!("Worker errors: {}", worker_errors.join(", ")),
            ));
        }

        let duration = start.elapsed();
        let stats = ParserStats {
            blocks_processed: self.stats.blocks_processed.load(Ordering::Relaxed),
            messages_parsed: self.stats.messages_parsed.load(Ordering::Relaxed),
            chunks_produced: self.stats.chunks_produced.load(Ordering::Relaxed),
            decompress_time_sec: 0.0, // Aggregate from workers if needed
            parse_time_sec: duration.as_secs_f64(),
        };

        info!(
            blocks = stats.blocks_processed,
            messages = stats.messages_parsed,
            chunks = stats.chunks_produced,
            duration_sec = stats.parse_time_sec,
            "Parser/slicer stage complete"
        );

        Ok(stats)
    }

    /// Worker thread function.
    fn worker(
        worker_id: usize,
        receiver: Receiver<PrefetchedBlock>,
        sender: Sender<MessageChunk<'static>>,
        stats: Arc<ParserSlicerStats>,
        target_chunk_size: usize,
        max_messages: usize,
        buffer_pool: BufferPool,
    ) -> Result<()> {
        debug!(worker_id, "Parser worker started");

        // Thread-local decompressor
        let mut zstd_decompressor = zstd::bulk::Decompressor::new().map_err(|e| {
            CodecError::encode(
                "ParserSlicer",
                format!("Failed to create decompressor: {e}"),
            )
        })?;

        // Current chunk being built
        let mut current_chunk: Option<MessageChunk<'static>> = None;
        let mut current_size: usize = 0;

        while let Ok(block) = receiver.recv() {
            stats.blocks_processed.fetch_add(1, Ordering::Relaxed);

            // Process based on block type
            match block.block_type {
                BlockType::McapChunk { compression, .. } => {
                    debug!(
                        sequence = block.sequence,
                        compression = ?compression,
                        data_len = block.data.len(),
                        "Processing McapChunk"
                    );

                    // Decompress if needed
                    let decompressed = match Self::decompress_block(
                        &block,
                        compression,
                        &mut zstd_decompressor,
                        &buffer_pool,
                    ) {
                        Ok(data) => {
                            debug!(
                                sequence = block.sequence,
                                decompressed_len = data.len(),
                                "Decompression successful"
                            );
                            data
                        }
                        Err(e) => {
                            warn!(
                                sequence = block.sequence,
                                error = %e,
                                "Decompression failed"
                            );
                            return Err(e);
                        }
                    };

                    stats
                        .decompress_bytes
                        .fetch_add(decompressed.len() as u64, Ordering::Relaxed);

                    // Parse messages from decompressed data
                    let messages = Self::parse_mcap_messages(&decompressed)?;

                    stats
                        .messages_parsed
                        .fetch_add(messages.len() as u64, Ordering::Relaxed);

                    // Add messages to current chunk
                    for (channel_id, log_time, publish_time, msg_seq, data) in messages {
                        // Ensure we have a chunk
                        if current_chunk.is_none() {
                            // Get globally unique sequence number
                            let sequence = stats.next_sequence.fetch_add(1, Ordering::SeqCst);
                            let arena = global_pool().get();
                            current_chunk = Some(MessageChunk::with_pooled_arena(sequence, arena));
                            current_size = 0;
                        }

                        let chunk = current_chunk.as_mut().unwrap();

                        // Add message to chunk
                        chunk
                            .add_message_from_slice(
                                channel_id,
                                log_time,
                                publish_time,
                                msg_seq,
                                &data,
                            )
                            .map_err(|e| {
                                CodecError::encode(
                                    "ParserSlicer",
                                    format!("Arena allocation failed: {e}"),
                                )
                            })?;

                        current_size += data.len() + 26; // message overhead

                        // Check if chunk is full
                        if current_size >= target_chunk_size
                            || chunk.message_count() >= max_messages
                        {
                            let full_chunk = current_chunk.take().unwrap();
                            sender.send(full_chunk).map_err(|_| {
                                CodecError::encode("ParserSlicer", "Channel closed")
                            })?;
                            stats.chunks_produced.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                BlockType::McapMetadata => {
                    // Skip metadata blocks - handled separately
                    debug!("Skipping metadata block");
                }
                BlockType::BagChunk { .. } => {
                    // Parse bag chunk (different format)
                    // For now, use existing bag parsing logic
                    warn!("Bag chunk parsing not yet implemented in hyper-pipeline");
                }
                BlockType::Unknown => {
                    debug!("Skipping unknown block type");
                }
            }
        }

        // Send any remaining chunk
        if let Some(chunk) = current_chunk.take() {
            if chunk.message_count() > 0 {
                sender
                    .send(chunk)
                    .map_err(|_| CodecError::encode("ParserSlicer", "Channel closed"))?;
                stats.chunks_produced.fetch_add(1, Ordering::Relaxed);
            }
        }

        debug!(worker_id, "Parser worker finished");
        Ok(())
    }

    /// Decompress a block if needed.
    fn decompress_block(
        block: &PrefetchedBlock,
        compression: CompressionType,
        zstd_decompressor: &mut zstd::bulk::Decompressor,
        _buffer_pool: &BufferPool,
    ) -> Result<Vec<u8>> {
        // Find compressed data within the chunk record
        // Chunk format: opcode(1) + record_len(8) + headers(32) + compression_str + compressed_data
        let data = &block.data[..];

        if data.len() < 9 {
            return Err(CodecError::parse("ParserSlicer", "Block too short"));
        }

        // Skip opcode and record length
        let header_start = 9;

        // Parse chunk header to find compressed data
        let mut cursor = Cursor::new(&data[header_start..]);

        let _msg_start_time = cursor.read_u64::<LittleEndian>().unwrap_or(0);
        let _msg_end_time = cursor.read_u64::<LittleEndian>().unwrap_or(0);
        let uncompressed_size = cursor.read_u64::<LittleEndian>().unwrap_or(0) as usize;
        let _uncompressed_crc = cursor.read_u32::<LittleEndian>().unwrap_or(0);
        let compression_len = cursor.read_u32::<LittleEndian>().unwrap_or(0) as usize;

        // Skip compression string
        // Offset: opcode(1) + record_len(8) + chunk_header(32) + compression_string + records_size(8)
        // MCAP Chunk format has a records_size field before the actual compressed records
        let compressed_data_offset = header_start + 32 + compression_len + 8;

        debug!(
            data_len = data.len(),
            header_start,
            compression_len,
            compressed_data_offset,
            uncompressed_size,
            "Decompress block offsets"
        );

        if compressed_data_offset >= data.len() {
            return Err(CodecError::parse("ParserSlicer", "Invalid chunk structure"));
        }

        let compressed_data = &data[compressed_data_offset..];

        debug!(
            compressed_data_len = compressed_data.len(),
            first_bytes = ?&compressed_data[..8.min(compressed_data.len())],
            "Compressed data"
        );

        match compression {
            CompressionType::Zstd => zstd_decompressor
                .decompress(compressed_data, uncompressed_size)
                .map_err(|e| {
                    CodecError::encode("ParserSlicer", format!("ZSTD decompression failed: {e}"))
                }),
            CompressionType::Lz4 => lz4_flex::decompress(compressed_data, uncompressed_size)
                .map_err(|e| {
                    CodecError::encode("ParserSlicer", format!("LZ4 decompression failed: {e}"))
                }),
            CompressionType::None => Ok(compressed_data.to_vec()),
        }
    }

    /// Parse MCAP message records from decompressed chunk data.
    #[allow(clippy::type_complexity)]
    fn parse_mcap_messages(data: &[u8]) -> Result<Vec<(u16, u64, u64, u32, Vec<u8>)>> {
        const OP_MESSAGE: u8 = 0x05;

        let mut messages = Vec::new();
        let mut cursor = Cursor::new(data);

        while (cursor.position() as usize) + 9 < data.len() {
            let opcode = cursor.read_u8().map_err(|e| {
                CodecError::parse("ParserSlicer", format!("Failed to read opcode: {e}"))
            })?;

            let record_len = cursor.read_u64::<LittleEndian>().map_err(|e| {
                CodecError::parse("ParserSlicer", format!("Failed to read record length: {e}"))
            })? as usize;

            if opcode != OP_MESSAGE {
                // Skip non-message records
                let pos = cursor.position() as usize;
                if pos + record_len > data.len() {
                    break;
                }
                cursor.set_position((pos + record_len) as u64);
                continue;
            }

            // Parse message record
            // channel_id (2) + sequence (4) + log_time (8) + publish_time (8) + data
            if record_len < 22 {
                break;
            }

            let channel_id = cursor.read_u16::<LittleEndian>().map_err(|e| {
                CodecError::parse("ParserSlicer", format!("Failed to read channel_id: {e}"))
            })?;

            let sequence = cursor.read_u32::<LittleEndian>().map_err(|e| {
                CodecError::parse("ParserSlicer", format!("Failed to read sequence: {e}"))
            })?;

            let log_time = cursor.read_u64::<LittleEndian>().map_err(|e| {
                CodecError::parse("ParserSlicer", format!("Failed to read log_time: {e}"))
            })?;

            let publish_time = cursor.read_u64::<LittleEndian>().map_err(|e| {
                CodecError::parse("ParserSlicer", format!("Failed to read publish_time: {e}"))
            })?;

            let data_len = record_len - 22;
            let pos = cursor.position() as usize;

            if pos + data_len > data.len() {
                break;
            }

            let msg_data = data[pos..pos + data_len].to_vec();
            cursor.set_position((pos + data_len) as u64);

            messages.push((channel_id, log_time, publish_time, sequence, msg_data));
        }

        Ok(messages)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parser_config_default() {
        let config = ParserSlicerConfig::default();
        assert!(config.num_workers > 0);
        assert_eq!(config.target_chunk_size, 16 * 1024 * 1024);
    }

    #[test]
    fn test_parse_empty_data() {
        let result = ParserSlicerStage::parse_mcap_messages(&[]);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }
}
