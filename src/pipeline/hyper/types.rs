// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Type definitions for 7-stage hyper-pipeline transitions.
//!
//! Each stage produces a specific output type consumed by the next stage:
//!
//! ```text
//! Prefetcher → PrefetchedBlock
//! Parser     → ParsedChunk
//! Batcher    → BatchedChunk (reuses MessageChunk)
//! Transform  → TransformedChunk (reuses MessageChunk)
//! Compressor → CompressedChunk (existing type)
//! Packetizer → PacketizedChunk
//! Writer     → (file output)
//! ```

use std::sync::Arc;

use crate::pipeline::types::chunk::{
    CompressedChunk, MessageChunk, MessageIndexEntry as ChunkMessageIndexEntry,
};
use robocodec::types::arena::ArenaSlice;
use robocodec::types::arena_pool::PooledArena;

// ============================================================================
// Stage 1 → Stage 2: Prefetched memory blocks
// ============================================================================

/// A prefetched block of file data ready for parsing.
///
/// The prefetcher reads file data using platform-specific optimizations
/// (madvise on macOS, io_uring on Linux) and sends blocks for parsing.
#[derive(Debug)]
pub struct PrefetchedBlock {
    /// Block sequence number (for ordering)
    pub sequence: u64,
    /// Start offset in file
    pub offset: u64,
    /// File data (shared ownership for zero-copy)
    pub data: Arc<[u8]>,
    /// Block type hint from file structure
    pub block_type: BlockType,
    /// Estimated decompressed size (for pre-allocation)
    pub estimated_uncompressed_size: usize,
    /// Source file path (used for BAG files where we need to re-open with rosbag crate)
    pub source_path: Option<String>,
}

/// Type of block detected during prefetch scanning.
#[derive(Debug, Clone, Copy)]
pub enum BlockType {
    /// MCAP chunk record (compressed messages)
    McapChunk {
        /// Size of compressed data
        compressed_size: usize,
        /// Compression algorithm
        compression: CompressionType,
    },
    /// MCAP metadata (schema, channel definitions)
    McapMetadata,
    /// ROS bag chunk
    BagChunk {
        /// Number of connections in chunk
        connection_count: u32,
    },
    /// Unknown block type
    Unknown,
}

/// Compression algorithm for MCAP chunks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    /// No compression
    None,
    /// ZSTD compression
    Zstd,
    /// LZ4 compression
    Lz4,
}

// SAFETY: PrefetchedBlock is safe to send between threads because:
// - Arc<[u8]> is Send + Sync
// - All other fields are primitive types
unsafe impl Send for PrefetchedBlock {}
unsafe impl Sync for PrefetchedBlock {}

// ============================================================================
// Stage 2 → Stage 3: Parsed messages
// ============================================================================

/// A chunk of parsed messages ready for batching.
///
/// Messages are allocated in the arena for zero-copy processing.
pub struct ParsedChunk<'arena> {
    /// Chunk sequence number
    pub sequence: u64,
    /// Arena owning all message data
    pub arena: PooledArena,
    /// Parsed messages
    pub messages: Vec<ParsedMessage<'arena>>,
    /// Source block offset (for error reporting)
    pub source_offset: u64,
}

impl std::fmt::Debug for ParsedChunk<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParsedChunk")
            .field("sequence", &self.sequence)
            .field("messages_count", &self.messages.len())
            .field("source_offset", &self.source_offset)
            .finish_non_exhaustive()
    }
}

/// A single parsed message.
#[derive(Debug, Clone, Copy)]
pub struct ParsedMessage<'arena> {
    /// Channel ID
    pub channel_id: u16,
    /// Log timestamp (nanoseconds since epoch)
    pub log_time: u64,
    /// Publish timestamp (nanoseconds since epoch)
    pub publish_time: u64,
    /// Message sequence number
    pub sequence: u32,
    /// Message data (zero-copy arena reference)
    pub data: ArenaSlice<'arena>,
}

// ============================================================================
// Stage 3 → Stage 4: Batched chunks (reuse MessageChunk)
// ============================================================================

/// Batched chunk ready for transform stage.
///
/// This is a type alias to the existing MessageChunk, which already
/// implements the arena-based zero-copy message storage we need.
pub type BatchedChunk<'arena> = MessageChunk<'arena>;

// ============================================================================
// Stage 4 → Stage 5: Transformed chunks (reuse MessageChunk)
// ============================================================================

/// Transformed chunk ready for compression.
///
/// Since we're not modifying message data (to preserve Foxglove compatibility),
/// this is the same as BatchedChunk.
pub type TransformedChunk<'arena> = MessageChunk<'arena>;

// ============================================================================
// Stage 5 → Stage 6: Compressed data (reuse CompressedChunk)
// ============================================================================

/// Compressed chunk from the compression stage.
///
/// Reuses the existing CompressedChunk type.
pub type CompressedData = CompressedChunk;

// ============================================================================
// Stage 6 → Stage 7: Packetized with CRC
// ============================================================================

/// A compressed chunk with CRC32 checksum for data integrity.
///
/// The CRC is computed over the compressed data and stored in the
/// MCAP chunk record for validation during reading.
#[derive(Debug, Clone)]
pub struct PacketizedChunk {
    /// Chunk sequence number (for ordering)
    pub sequence: u64,
    /// Compressed data
    pub compressed_data: Vec<u8>,
    /// CRC32 checksum of compressed data
    pub crc32: u32,
    /// Uncompressed size (for MCAP header)
    pub uncompressed_size: usize,
    /// Message start time (earliest log_time)
    pub message_start_time: u64,
    /// Message end time (latest log_time)
    pub message_end_time: u64,
    /// Number of messages in this chunk
    pub message_count: usize,
    /// Compression ratio (compressed / uncompressed)
    pub compression_ratio: f64,
    /// Message indexes by channel ID
    pub message_indexes: std::collections::BTreeMap<u16, Vec<MessageIndexEntry>>,
}

/// Message index entry for MCAP MessageIndex records.
#[derive(Debug, Clone)]
pub struct MessageIndexEntry {
    /// Message log time
    pub log_time: u64,
    /// Offset within chunk data
    pub offset: u64,
}

impl PacketizedChunk {
    /// Convert to CompressedChunk for writer compatibility.
    ///
    /// Note: This drops the CRC32 field since the existing writer
    /// doesn't use it. The CRC is written separately in the MCAP chunk record.
    pub fn into_compressed_chunk(self) -> CompressedChunk {
        // Convert our MessageIndexEntry to chunk::MessageIndexEntry
        let message_indexes = self
            .message_indexes
            .into_iter()
            .map(|(channel_id, entries)| {
                let converted: Vec<ChunkMessageIndexEntry> = entries
                    .into_iter()
                    .map(|e| ChunkMessageIndexEntry {
                        log_time: e.log_time,
                        offset: e.offset,
                    })
                    .collect();
                (channel_id, converted)
            })
            .collect();

        CompressedChunk {
            sequence: self.sequence,
            compressed_data: self.compressed_data,
            uncompressed_size: self.uncompressed_size,
            message_start_time: self.message_start_time,
            message_end_time: self.message_end_time,
            message_count: self.message_count,
            compression_ratio: self.compression_ratio,
            message_indexes,
        }
    }
}

impl From<PacketizedChunk> for CompressedChunk {
    fn from(packet: PacketizedChunk) -> Self {
        packet.into_compressed_chunk()
    }
}

// ============================================================================
// Stage Statistics
// ============================================================================

/// Statistics from the prefetcher stage.
#[derive(Debug, Default, Clone)]
pub struct PrefetcherStats {
    /// Number of blocks prefetched
    pub blocks_prefetched: u64,
    /// Total bytes prefetched
    pub bytes_prefetched: u64,
    /// Time spent in I/O operations (seconds)
    pub io_time_sec: f64,
}

/// Statistics from the parser/slicer stage.
#[derive(Debug, Default, Clone)]
pub struct ParserStats {
    /// Number of blocks processed
    pub blocks_processed: u64,
    /// Number of messages parsed
    pub messages_parsed: u64,
    /// Number of chunks produced
    pub chunks_produced: u64,
    /// Time spent decompressing (seconds)
    pub decompress_time_sec: f64,
    /// Time spent parsing (seconds)
    pub parse_time_sec: f64,
}

/// Statistics from the batcher/router stage.
#[derive(Debug, Default, Clone)]
pub struct BatcherStats {
    /// Number of messages received
    pub messages_received: u64,
    /// Number of batches created
    pub batches_created: u64,
    /// Average batch size (messages)
    pub avg_batch_size: f64,
}

/// Statistics from the CRC/packetizer stage.
#[derive(Debug, Default, Clone)]
pub struct PacketizerStats {
    /// Number of chunks processed
    pub chunks_processed: u64,
    /// Total bytes checksummed
    pub bytes_checksummed: u64,
    /// Time spent computing CRC (seconds)
    pub crc_time_sec: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefetched_block_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<PrefetchedBlock>();
    }

    #[test]
    fn test_compression_type_equality() {
        assert_eq!(CompressionType::Zstd, CompressionType::Zstd);
        assert_ne!(CompressionType::Zstd, CompressionType::Lz4);
    }

    #[test]
    fn test_packetized_chunk_conversion() {
        let packet = PacketizedChunk {
            sequence: 1,
            compressed_data: vec![1, 2, 3],
            crc32: 0x12345678,
            uncompressed_size: 100,
            message_start_time: 1000,
            message_end_time: 2000,
            message_count: 10,
            compression_ratio: 0.03,
            message_indexes: std::collections::BTreeMap::new(),
        };

        let compressed: CompressedChunk = packet.into();
        assert_eq!(compressed.sequence, 1);
        assert_eq!(compressed.compressed_data, vec![1, 2, 3]);
        assert_eq!(compressed.uncompressed_size, 100);
    }
}
