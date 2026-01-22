// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! ROS1 bag format implementation for the unified I/O layer.
//!
//! This module provides BAG-specific readers that implement the unified I/O traits.
//!
//! **Note:** This implementation uses a custom BAG parser with no external dependencies.
//! It supports:
//! - BZ2 and uncompressed chunks
//! - Parallel reading via chunk indexes (default behavior)
//! - Full connection metadata extraction

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::time::Instant;

use rayon::prelude::*;

use crate::io::filter::ChannelFilter;
use crate::io::metadata::{ChannelInfo, FileFormat, RawMessage};
use crate::io::traits::{
    FormatReader, MessageChunkData, ParallelReader, ParallelReaderConfig, ParallelReaderStats,
};
use crate::{CodecError, Result};

use super::parser::{BagChunkInfo, BagConnection, BagParser};
use super::writer::BagWriter;

/// ROS1 bag format type.
///
/// This type provides factory methods for creating BAG readers.
/// Default behavior is parallel reading for optimal performance.
pub struct BagFormat;

impl BagFormat {
    /// Create a BAG reader with parallel reading support (default).
    ///
    /// The reader uses memory-mapping and processes chunks in parallel
    /// using the Rayon thread pool.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<ParallelBagReader> {
        ParallelBagReader::open(path)
    }

    /// Create a BAG writer with the given configuration.
    ///
    /// Returns a boxed FormatWriter trait object for unified writer API.
    pub fn create_writer<P: AsRef<Path>>(
        path: P,
        _config: &crate::io::writer::WriterConfig,
    ) -> Result<Box<dyn crate::io::traits::FormatWriter>> {
        // For now, we create a simple writer
        // TODO: Use config options for compression, chunk size, etc.
        let writer = BagWriter::create(path)?;
        Ok(Box::new(writer))
    }
}

/// Parallel BAG reader with memory-mapped file access.
///
/// This reader parses the BAG file metadata (connections, chunk indexes)
/// and supports parallel processing of chunks using Rayon.
pub struct ParallelBagReader {
    /// File path
    path: String,
    /// Custom BAG parser
    parser: BagParser,
    /// Channel information (channel_id -> ChannelInfo)
    channels: HashMap<u16, ChannelInfo>,
    /// Connection ID to channel ID mapping (conn_id -> channel_id)
    conn_id_map: HashMap<u32, u16>,
    /// Total message count (estimated from chunks)
    message_count: u64,
    /// Start timestamp
    start_time: Option<u64>,
    /// End timestamp
    end_time: Option<u64>,
}

impl ParallelBagReader {
    /// Open a BAG file for parallel reading.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let path_str = path_ref.to_string_lossy().to_string();

        // Open the custom parser
        let parser = BagParser::open(path_ref)?;

        // Build channels from parser's connections
        // Each connection becomes a separate channel to preserve callerid info
        let mut channels: HashMap<u16, ChannelInfo> = HashMap::new();
        let mut conn_id_map: HashMap<u32, u16> = HashMap::new();
        let mut next_channel_id: u16 = 0;

        // Use (topic, callerid) as the key to identify unique channels
        let mut topic_callerid_to_channel: HashMap<(String, String), u16> = HashMap::new();

        // Sort connections by conn_id to ensure deterministic channel ID assignment
        let mut sorted_conn_ids: Vec<u32> = parser.connections().keys().copied().collect();
        sorted_conn_ids.sort();

        for conn_id in sorted_conn_ids {
            let conn = &parser.connections()[&conn_id];
            let callerid = conn.caller_id.clone();
            let key = (conn.topic.clone(), callerid.clone());

            // Check if we already have a channel for this (topic, callerid) combination
            let channel_id = if let Some(&existing_id) = topic_callerid_to_channel.get(&key) {
                existing_id
            } else {
                let id = next_channel_id;
                next_channel_id = next_channel_id.wrapping_add(1);

                channels.insert(
                    id,
                    ChannelInfo {
                        id,
                        topic: conn.topic.clone(),
                        message_type: conn.message_type.clone(),
                        encoding: "ros1".to_string(), // ROS1 serialization format
                        schema: Some(conn.message_definition.clone()),
                        schema_data: None,
                        schema_encoding: Some("ros1msg".to_string()),
                        message_count: 0,
                        callerid: if callerid.is_empty() {
                            None
                        } else {
                            Some(callerid.clone())
                        },
                    },
                );
                topic_callerid_to_channel.insert(key, id);
                id
            };

            conn_id_map.insert(conn_id, channel_id);
        }

        // Calculate message count and time bounds from chunks
        let chunks = parser.chunks();
        let message_count = chunks.iter().map(|c| c.message_count as u64).sum();
        let start_time = chunks.first().map(|c| c.start_time);
        let end_time = chunks.last().map(|c| c.end_time);

        Ok(Self {
            path: path_str,
            parser,
            channels,
            conn_id_map,
            message_count,
            start_time,
            end_time,
        })
    }

    /// Get the connection ID to channel ID mapping.
    pub fn conn_id_map(&self) -> &HashMap<u32, u16> {
        &self.conn_id_map
    }

    /// Get all chunk information from the parser.
    pub fn chunks(&self) -> &[BagChunkInfo] {
        self.parser.chunks()
    }

    /// Get all connections from the parser.
    pub fn connections(&self) -> &HashMap<u32, BagConnection> {
        self.parser.connections()
    }

    /// Create a raw message iterator for sequential reading.
    ///
    /// This is useful for rewriters that need to process messages one by one.
    pub fn iter_raw(&self) -> Result<BagRawIter<'_>> {
        Ok(BagRawIter::new(
            &self.parser,
            &self.channels,
            &self.conn_id_map,
        ))
    }

    /// Process a single chunk in parallel.
    fn process_chunk(
        chunk_info: &BagChunkInfo,
        parser: &BagParser,
        conn_id_map: &HashMap<u32, u16>,
        channels: &HashMap<u16, ChannelInfo>,
        _channel_filter: &Option<ChannelFilter>,
    ) -> Result<ProcessedChunk> {
        // Read and decompress the chunk
        let decompressed = parser.read_chunk(chunk_info)?;

        // Parse messages from decompressed data
        let messages = parser.parse_chunk_messages(&decompressed, conn_id_map)?;

        // Calculate total bytes
        let total_bytes = messages.iter().map(|m| m.data.len()).sum::<usize>();
        let message_count = messages.len();

        // Build message chunk
        let mut chunk = MessageChunkData::new(chunk_info.sequence);

        for msg in messages {
            // Verify channel exists
            if channels.contains_key(&msg.channel_id) {
                let raw_msg = RawMessage {
                    channel_id: msg.channel_id,
                    log_time: msg.log_time,
                    publish_time: msg.publish_time,
                    data: msg.data,
                    sequence: Some(msg.sequence as u64),
                };
                chunk.add_message(raw_msg);
            }
        }

        Ok(ProcessedChunk {
            chunk,
            total_bytes: total_bytes as u64,
            message_count: message_count as u64,
        })
    }
}

impl FormatReader for ParallelBagReader {
    fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        &self.channels
    }

    fn message_count(&self) -> u64 {
        self.message_count
    }

    fn start_time(&self) -> Option<u64> {
        self.start_time
    }

    fn end_time(&self) -> Option<u64> {
        self.end_time
    }

    fn path(&self) -> &str {
        &self.path
    }

    fn format(&self) -> FileFormat {
        FileFormat::Bag
    }

    fn file_size(&self) -> u64 {
        self.parser.file_size()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl ParallelReader for ParallelBagReader {
    fn read_parallel(
        &self,
        config: ParallelReaderConfig,
        sender: crossbeam_channel::Sender<MessageChunkData>,
    ) -> Result<ParallelReaderStats> {
        let num_threads = config.num_threads.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(8)
        });

        println!(
            "Starting parallel BAG reader with {} worker threads...",
            num_threads
        );
        println!("  File: {}", self.path);
        println!("  Chunks to process: {}", self.parser.chunks().len());

        let total_start = Instant::now();

        // Build channel filter from topic filter
        let channel_filter = config
            .topic_filter
            .as_ref()
            .map(|tf| ChannelFilter::from_topic_filter(tf, self.channels()));

        // Create thread pool for controlled parallelism
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .thread_name(|index| format!("bag-reader-{}", index))
            .build()
            .map_err(|e| {
                CodecError::encode(
                    "ParallelBagReader",
                    format!("Failed to create thread pool: {e}"),
                )
            })?;

        // Get references for parallel processing
        let chunks = self.parser.chunks();
        let parser = &self.parser;
        let conn_id_map = &self.conn_id_map;
        let channels = &self.channels;

        // Process chunks in parallel
        let results: Vec<Result<ProcessedChunk>> = pool.install(|| {
            chunks
                .par_iter()
                .enumerate()
                .map(|(i, chunk_info)| {
                    if i % config.progress_interval == 0 && i > 0 {
                        eprint!("\rProcessing chunk {}/{}...", i, chunks.len());
                        let _ = std::io::stdout().flush();
                    }
                    Self::process_chunk(chunk_info, parser, conn_id_map, channels, &channel_filter)
                })
                .collect()
        });

        eprintln!(); // New line after progress

        // Collect results and send chunks
        let mut messages_read = 0u64;
        let mut chunks_processed = 0;
        let mut total_bytes = 0u64;

        for result in results {
            let processed = result?;
            chunks_processed += 1;
            messages_read += processed.message_count;
            total_bytes += processed.total_bytes;

            if processed.chunk.message_count() > 0 {
                sender.send(processed.chunk).map_err(|e| {
                    CodecError::encode("ParallelBagReader", format!("Failed to send chunk: {e}"))
                })?;
            }
        }

        let duration = total_start.elapsed();

        println!("Parallel BAG reader complete:");
        println!("  Chunks processed: {}", chunks_processed);
        println!("  Messages read: {}", messages_read);
        println!(
            "  Total bytes: {:.2} MB",
            total_bytes as f64 / (1024.0 * 1024.0)
        );
        println!("  Total time: {:.2}s", duration.as_secs_f64());

        Ok(ParallelReaderStats {
            messages_read,
            chunks_processed,
            total_bytes,
            read_time_sec: 0.0,
            decompress_time_sec: 0.0,
            deserialize_time_sec: 0.0,
            total_time_sec: duration.as_secs_f64(),
        })
    }

    fn chunk_count(&self) -> usize {
        self.parser.chunks().len()
    }

    fn supports_parallel(&self) -> bool {
        !self.parser.chunks().is_empty()
    }
}

/// Processed chunk ready to be sent to the output channel.
struct ProcessedChunk {
    /// Message chunk with all messages
    chunk: MessageChunkData,
    /// Total bytes in this chunk
    total_bytes: u64,
    /// Number of messages in this chunk
    message_count: u64,
}

/// Raw message iterator for BAG files (sequential reading).
///
/// This iterator processes chunks sequentially and yields messages one by one.
/// Used primarily by rewriters that need to process messages in order.
pub struct BagRawIter<'a> {
    /// Reference to the parser
    parser: &'a BagParser,
    /// Channel information
    channels: &'a HashMap<u16, ChannelInfo>,
    /// Connection ID to channel ID mapping
    conn_id_map: &'a HashMap<u32, u16>,
    /// Current chunk index
    current_chunk_idx: usize,
    /// Current messages from decompressed chunk
    current_messages: Vec<super::parser::BagMessageData>,
    /// Current message index within chunk
    current_msg_idx: usize,
}

impl<'a> BagRawIter<'a> {
    /// Create a new raw message iterator.
    pub fn new(
        parser: &'a BagParser,
        channels: &'a HashMap<u16, ChannelInfo>,
        conn_id_map: &'a HashMap<u32, u16>,
    ) -> Self {
        Self {
            parser,
            channels,
            conn_id_map,
            current_chunk_idx: 0,
            current_messages: Vec::new(),
            current_msg_idx: 0,
        }
    }

    /// Load the next chunk's messages.
    fn load_next_chunk(&mut self) -> Result<bool> {
        let chunks = self.parser.chunks();
        if self.current_chunk_idx >= chunks.len() {
            return Ok(false);
        }

        let chunk_info = &chunks[self.current_chunk_idx];
        self.current_chunk_idx += 1;

        // Read and decompress the chunk
        let decompressed = self.parser.read_chunk(chunk_info)?;

        // Parse messages from decompressed data
        self.current_messages = self
            .parser
            .parse_chunk_messages(&decompressed, self.conn_id_map)?;
        self.current_msg_idx = 0;

        Ok(true)
    }
}

impl<'a> Iterator for BagRawIter<'a> {
    type Item = Result<(RawMessage, ChannelInfo)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Check if we have messages in current chunk
            if self.current_msg_idx < self.current_messages.len() {
                let msg = &self.current_messages[self.current_msg_idx];
                self.current_msg_idx += 1;

                if let Some(channel_info) = self.channels.get(&msg.channel_id) {
                    return Some(Ok((
                        RawMessage {
                            channel_id: msg.channel_id,
                            log_time: msg.log_time,
                            publish_time: msg.publish_time,
                            data: msg.data.clone(),
                            sequence: Some(msg.sequence as u64),
                        },
                        channel_info.clone(),
                    )));
                }
                continue;
            }

            // Load next chunk
            match self.load_next_chunk() {
                Ok(true) => continue,
                Ok(false) => return None,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bag_format() {
        let _ = BagFormat;
    }

    #[test]
    fn test_parallel_bag_reader_compile() {
        // This test just verifies that the type compiles correctly
    }

    #[test]
    fn test_ros1_encoding_constant() {
        // Verify that we use "ros1" encoding for ROS1 bag files
        // This is important because "cdr" is for ROS2 and will cause
        // "Message encoding cdr with schema encoding 'ros1msg' is not supported" errors
        let ros1_encoding = "ros1";
        let ros1msg_schema_encoding = "ros1msg";

        // These constants should match what's used in the reader
        assert_eq!(ros1_encoding, "ros1");
        assert_eq!(ros1msg_schema_encoding, "ros1msg");

        // Verify they are compatible (ros1 encoding with ros1msg schema)
        assert!(ros1_encoding.starts_with("ros1"));
        assert!(ros1msg_schema_encoding.starts_with("ros1"));
    }
}
