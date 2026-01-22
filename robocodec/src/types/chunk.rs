// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Chunk data structures for zero-copy pipeline processing.
//!
//! This module defines the core data structures used throughout the
//! zero-copy pipeline: messages, chunks, and compressed chunks.

use crate::types::arena::ArenaSlice;
use crate::types::arena_pool::PooledArena;

/// Zero-copy message reference.
///
/// Messages reference data stored in an arena, eliminating the need
/// to copy message data during pipeline processing.
#[derive(Debug, Clone, Copy)]
pub struct ArenaMessage<'arena> {
    /// Channel ID this message belongs to
    pub channel_id: u16,
    /// Log timestamp (nanoseconds since epoch)
    pub log_time: u64,
    /// Publish timestamp (nanoseconds since epoch)
    pub publish_time: u64,
    /// Message sequence number within the channel
    pub sequence: u32,
    /// Message data (zero-copy reference into arena)
    pub data: ArenaSlice<'arena>,
}

impl<'arena> ArenaMessage<'arena> {
    /// Create a new arena message.
    #[inline]
    pub fn new(
        channel_id: u16,
        log_time: u64,
        publish_time: u64,
        sequence: u32,
        data: ArenaSlice<'arena>,
    ) -> Self {
        Self {
            channel_id,
            log_time,
            publish_time,
            sequence,
            data,
        }
    }

    /// Get the size of this message's data.
    #[inline]
    pub fn data_len(&self) -> usize {
        self.data.len()
    }

    /// Get a reference to the message data.
    #[inline]
    pub fn data(&self) -> &[u8] {
        self.data.as_ref()
    }
}

/// A chunk of messages ready for compression.
///
/// Chunks are built by accumulating messages until a size threshold
/// is reached, then compressed and written to the output file.
///
/// # Safety
///
/// MessageChunk is Send because it owns its arena and all message data.
/// It can be safely moved between threads for parallel compression.
///
/// MessageChunk is Sync because it owns all its data and can be safely
/// shared across threads for reading (needed for parallel compression).
unsafe impl<'arena> Send for MessageChunk<'arena> {}
unsafe impl<'arena> Sync for MessageChunk<'arena> {}

pub struct MessageChunk<'arena> {
    /// Arena that owns all message data in this chunk
    /// When using pooled arena, this points to the arena inside pooled_arena.
    ///
    /// # Safety
    ///
    /// The arena pointer is valid for the lifetime of this MessageChunk.
    /// When using a pooled arena, the pooled_arena field keeps it alive.
    /// When not using a pooled arena, the arena is leaked and dropped manually.
    ///
    /// This is a mutable pointer because arena allocations require mutable access.
    /// The arena uses atomic operations internally for thread safety.
    pub arena: *mut crate::types::arena::MessageArena,
    /// Optional pooled arena wrapper (for returning arena to pool on drop)
    pooled_arena: Option<PooledArena>,
    /// Messages in this chunk
    pub messages: Vec<ArenaMessage<'arena>>,
    /// Chunk sequence number for ordering
    pub sequence: u64,
    /// Message start time (earliest log_time in chunk)
    pub message_start_time: u64,
    /// Message end time (latest log_time in chunk)
    pub message_end_time: u64,
}

impl<'arena> MessageChunk<'arena> {
    /// Create a new empty chunk.
    pub fn new(sequence: u64) -> Self {
        // Create a boxed arena and leak it to get a stable pointer
        let arena = Box::new(crate::types::arena::MessageArena::new());
        let arena_ptr = Box::leak(arena) as *mut crate::types::arena::MessageArena;

        Self {
            arena: arena_ptr,
            pooled_arena: None,
            messages: Vec::new(),
            sequence,
            message_start_time: u64::MAX,
            message_end_time: 0,
        }
    }

    /// Create a new chunk with pre-allocated capacity.
    pub fn with_capacity(sequence: u64, capacity: usize) -> Self {
        let arena = Box::new(crate::types::arena::MessageArena::new());
        let arena_ptr = Box::leak(arena) as *mut crate::types::arena::MessageArena;

        Self {
            arena: arena_ptr,
            pooled_arena: None,
            messages: Vec::with_capacity(capacity),
            sequence,
            message_start_time: u64::MAX,
            message_end_time: 0,
        }
    }

    /// Create a new empty chunk using an arena from the pool.
    ///
    /// This is the preferred constructor as it reuses arenas across chunks,
    /// significantly reducing allocation/deallocation overhead.
    pub fn with_pooled_arena(sequence: u64, mut pooled_arena: PooledArena) -> Self {
        let arena_ptr = pooled_arena.arena_mut() as *mut crate::types::arena::MessageArena;

        Self {
            arena: arena_ptr,
            pooled_arena: Some(pooled_arena),
            messages: Vec::new(),
            sequence,
            message_start_time: u64::MAX,
            message_end_time: 0,
        }
    }

    /// Create a new chunk with capacity using an arena from the pool.
    pub fn with_pooled_arena_and_capacity(
        sequence: u64,
        mut pooled_arena: PooledArena,
        capacity: usize,
    ) -> Self {
        let arena_ptr = pooled_arena.arena_mut() as *mut crate::types::arena::MessageArena;

        Self {
            arena: arena_ptr,
            pooled_arena: Some(pooled_arena),
            messages: Vec::with_capacity(capacity),
            sequence,
            message_start_time: u64::MAX,
            message_end_time: 0,
        }
    }

    /// Add a message to this chunk.
    pub fn add_message(&mut self, msg: ArenaMessage<'arena>) {
        // Update time bounds
        self.message_start_time = self.message_start_time.min(msg.log_time);
        self.message_end_time = self.message_end_time.max(msg.log_time);

        self.messages.push(msg);
    }

    /// Add a message to this chunk by copying data into the arena.
    ///
    /// This is a convenience method that allocates the message data in the arena
    /// and adds the message in one step, avoiding multiple mutable borrows.
    ///
    /// # Errors
    ///
    /// Returns an error if arena allocation fails (out of memory).
    ///
    /// # Safety
    ///
    /// This method uses unsafe code to extend the arena slice lifetime.
    /// The safety is ensured because:
    /// 1. The arena is owned by self
    /// 2. The messages vector is also owned by self
    /// 3. The arena will not be dropped before the messages
    pub fn add_message_from_slice(
        &mut self,
        channel_id: u16,
        log_time: u64,
        publish_time: u64,
        sequence: u32,
        data: &[u8],
    ) -> Result<(), std::io::Error> {
        // Update time bounds
        self.message_start_time = self.message_start_time.min(log_time);
        self.message_end_time = self.message_end_time.max(log_time);

        // Allocate in arena
        let arena_slice = unsafe { &mut *self.arena }.allocate_slice(data)?;

        // SAFETY: We're extending the lifetime from the anonymous borrow
        // to the 'arena lifetime. This is safe because:
        // 1. self.arena is owned by self and won't be dropped before self.messages
        // 2. The ArenaSlice only contains a pointer into the arena
        // 3. The messages are stored in self.messages, which is also owned by self
        let extended_slice: ArenaSlice<'arena> = unsafe {
            // Transmute the lifetime - this is safe because the arena outlives the messages
            std::mem::transmute(arena_slice)
        };

        self.messages.push(ArenaMessage::new(
            channel_id,
            log_time,
            publish_time,
            sequence,
            extended_slice,
        ));
        Ok(())
    }

    /// Get the total uncompressed size of all message data in this chunk.
    pub fn total_data_size(&self) -> usize {
        self.messages.iter().map(|m| m.data_len()).sum()
    }

    /// Get the total estimated serialized size of this chunk.
    ///
    /// This includes chunk header (24 bytes) and message headers (26 bytes each) plus data size.
    pub fn estimated_serialized_size(&self) -> usize {
        // Chunk header: message_start_time (8) + message_end_time (8) + message_start_offset (8)
        const CHUNK_HEADER_SIZE: usize = 24;
        // Message header: channel_id (2) + sequence (4) + log_time (8) + publish_time (8) + data_len (4)
        const MESSAGE_HEADER_SIZE: usize = 26;
        CHUNK_HEADER_SIZE + self.messages.len() * MESSAGE_HEADER_SIZE + self.total_data_size()
    }

    /// Get the number of messages in this chunk.
    pub fn message_count(&self) -> usize {
        self.messages.len()
    }

    /// Check if this chunk is empty.
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    /// Split this chunk into two chunks at the given message index.
    ///
    /// Returns a new chunk containing messages after the split point.
    /// This chunk retains messages up to and including the split point.
    pub fn split_at(&mut self, index: usize) -> Self {
        let tail = self.messages.split_off(index);
        let mut new_chunk = Self::with_capacity(self.sequence + 1, tail.len());

        // Recalculate time bounds for this chunk
        self.message_start_time = u64::MAX;
        self.message_end_time = 0;
        for msg in &self.messages {
            self.message_start_time = self.message_start_time.min(msg.log_time);
            self.message_end_time = self.message_end_time.max(msg.log_time);
        }

        // Set up the new chunk with tail messages
        new_chunk.message_start_time = u64::MAX;
        new_chunk.message_end_time = 0;
        for msg in tail {
            new_chunk.message_start_time = new_chunk.message_start_time.min(msg.log_time);
            new_chunk.message_end_time = new_chunk.message_end_time.max(msg.log_time);
            new_chunk.messages.push(msg);
        }

        new_chunk
    }
}

impl<'arena> Drop for MessageChunk<'arena> {
    fn drop(&mut self) {
        // If we have a pooled arena, dropping it will return it to the pool
        // The arena pointer points to data owned by pooled_arena, so we
        // don't need to free it separately.
        if self.pooled_arena.is_some() {
            // pooled_arena is dropped here, which returns arena to pool
        } else {
            // No pool - we need to free the arena
            // Reconstruct the Box to drop it properly
            unsafe {
                let _ = Box::from_raw(self.arena);
            }
        }
    }
}

/// Message index entry for MCAP MessageIndex records.
///
/// Each entry records the log_time and offset of a message within the
/// uncompressed chunk data, enabling time-based random access.
#[derive(Debug, Clone)]
pub struct MessageIndexEntry {
    /// Message log time (nanoseconds)
    pub log_time: u64,
    /// Offset within the uncompressed chunk data
    pub offset: u64,
}

/// A compressed chunk ready for writing.
///
/// This represents a chunk that has been compressed and is ready
/// to be serialized to the MCAP file.
#[derive(Debug, Clone)]
pub struct CompressedChunk {
    /// Chunk sequence number
    pub sequence: u64,
    /// Compressed data (includes MCAP chunk header + message records)
    pub compressed_data: Vec<u8>,
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
    /// Message indexes by channel ID for MCAP MessageIndex records.
    /// Maps channel_id -> list of (log_time, offset) entries.
    pub message_indexes: std::collections::BTreeMap<u16, Vec<MessageIndexEntry>>,
}

impl CompressedChunk {
    /// Calculate the compression ratio.
    pub fn calculate_compression_ratio(&self) -> f64 {
        if self.uncompressed_size > 0 {
            self.compressed_data.len() as f64 / self.uncompressed_size as f64
        } else {
            1.0
        }
    }

    /// Get the compressed size in bytes.
    pub fn compressed_size(&self) -> usize {
        self.compressed_data.len()
    }
}

/// Configuration for chunk building.
#[derive(Debug, Clone)]
pub struct ChunkConfig {
    /// Target chunk size in bytes (uncompressed)
    pub target_chunk_size: usize,
    /// Maximum messages per chunk
    pub max_messages: usize,
    /// Timeout for chunk accumulation (milliseconds)
    pub timeout_ms: u64,
}

impl Default for ChunkConfig {
    fn default() -> Self {
        Self {
            target_chunk_size: 8 * 1024 * 1024, // 8MB default
            max_messages: 100_000,
            timeout_ms: 100,
        }
    }
}

impl ChunkConfig {
    /// Create a chunk config for maximum throughput.
    /// Uses larger chunks (32MB) to reduce compression overhead.
    pub fn max_throughput() -> Self {
        Self {
            target_chunk_size: 32 * 1024 * 1024, // 32MB chunks
            max_messages: 500_000,
            timeout_ms: 500,
        }
    }

    /// Create a chunk config optimized for throughput with good compression.
    /// Uses 16MB chunks with balanced compression level.
    pub fn high_throughput() -> Self {
        Self {
            target_chunk_size: 16 * 1024 * 1024, // 16MB chunks
            max_messages: 250_000,
            timeout_ms: 200,
        }
    }

    /// Create a chunk config optimized for low latency.
    pub fn low_latency() -> Self {
        Self {
            target_chunk_size: 1024 * 1024, // 1MB chunks
            max_messages: 10_000,
            timeout_ms: 50,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_chunk_new() {
        let chunk = MessageChunk::new(0);
        assert!(chunk.is_empty());
        assert_eq!(chunk.message_count(), 0);
        assert_eq!(chunk.sequence, 0);
    }

    #[test]
    fn test_message_chunk_add_message() {
        let mut chunk = MessageChunk::new(0);
        chunk
            .add_message_from_slice(1, 1000, 1000, 0, b"test data")
            .unwrap();

        assert_eq!(chunk.message_count(), 1);
        assert!(!chunk.is_empty());
        assert_eq!(chunk.message_start_time, 1000);
        assert_eq!(chunk.message_end_time, 1000);
    }

    #[test]
    fn test_message_chunk_time_bounds() {
        let mut chunk = MessageChunk::new(0);

        chunk
            .add_message_from_slice(1, 5000, 5000, 0, b"data1")
            .unwrap();
        chunk
            .add_message_from_slice(1, 1000, 1000, 1, b"data2")
            .unwrap();
        chunk
            .add_message_from_slice(1, 8000, 8000, 2, b"data3")
            .unwrap();

        assert_eq!(chunk.message_start_time, 1000);
        assert_eq!(chunk.message_end_time, 8000);
    }

    #[test]
    fn test_message_chunk_split() {
        let mut chunk = MessageChunk::with_capacity(0, 5);

        for i in 0..5 {
            chunk
                .add_message_from_slice(1, i as u64 * 1000, i as u64 * 1000, i as u32, &[i as u8])
                .unwrap();
        }

        assert_eq!(chunk.message_count(), 5);

        let tail = chunk.split_at(2);
        assert_eq!(chunk.message_count(), 2);
        assert_eq!(tail.message_count(), 3);
        assert_eq!(chunk.sequence, 0);
        assert_eq!(tail.sequence, 1);
    }

    #[test]
    fn test_message_chunk_total_data_size() {
        let mut chunk = MessageChunk::new(0);

        chunk.add_message_from_slice(1, 0, 0, 0, b"hello").unwrap();
        chunk.add_message_from_slice(1, 0, 0, 1, b"world").unwrap();

        assert_eq!(chunk.total_data_size(), 10);
    }

    #[test]
    fn test_compressed_chunk_compression_ratio() {
        let chunk = CompressedChunk {
            sequence: 0,
            compressed_data: vec![0u8; 100], // 100 bytes compressed
            uncompressed_size: 1000,         // 1000 bytes uncompressed
            message_start_time: 0,
            message_end_time: 1000,
            message_count: 10,
            compression_ratio: 0.0,
            message_indexes: std::collections::BTreeMap::new(),
        };

        assert_eq!(chunk.compressed_size(), 100);
        assert_eq!(chunk.calculate_compression_ratio(), 0.1);
    }

    #[test]
    fn test_chunk_config_default() {
        let config = ChunkConfig::default();
        assert_eq!(config.target_chunk_size, 8 * 1024 * 1024);
        assert_eq!(config.max_messages, 100_000);
        assert_eq!(config.timeout_ms, 100);
    }

    #[test]
    fn test_chunk_config_high_throughput() {
        let config = ChunkConfig::high_throughput();
        assert_eq!(config.target_chunk_size, 16 * 1024 * 1024);
        assert_eq!(config.max_messages, 250_000);
        assert_eq!(config.timeout_ms, 200);
    }

    #[test]
    fn test_chunk_config_low_latency() {
        let config = ChunkConfig::low_latency();
        assert_eq!(config.target_chunk_size, 1024 * 1024);
        assert_eq!(config.max_messages, 10_000);
        assert_eq!(config.timeout_ms, 50);
    }
}
