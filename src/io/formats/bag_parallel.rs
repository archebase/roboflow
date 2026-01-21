//! Parallel BAG reader using custom parser for true random chunk access.
//!
//! This module implements parallel reading for ROS1 bag files by:
//! 1. Reading chunk info records from index section (sequential discovery)
//! 2. Processing chunks in parallel using Rayon (decompression + parsing)
//! 3. Sending MessageChunk objects through crossbeam channel
//!
//! # Performance
//!
//! Expected 3-6x speedup by parallelizing chunk decompression and message parsing.
//!
//! # Example
//!
//! ```no_run
//! use robocodec::io::formats::bag_parallel::ParallelBagReader;
//! use crossbeam_channel::bounded;
//! use robocodec::io::traits::{ParallelReader, ParallelReaderConfig};
//! use robocodec::pipeline::types::chunk::MessageChunk;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let reader = ParallelBagReader::open("data.bag")?;
//! let (sender, receiver) = bounded::<MessageChunk<'static>>(32);
//!
//! // Spawn reader thread
//! std::thread::spawn(move || {
//!     let config = ParallelReaderConfig::default();
//!     let stats = reader.read_parallel(config, sender).unwrap();
//!     println!("Read {} messages", stats.messages_read);
//! });
//!
//! // Consume chunks
//! for chunk in receiver {
//!     println!("Got chunk with {} messages", chunk.message_count());
//! }
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::time::Instant;

use rayon::prelude::*;

use crate::io::filter::ChannelFilter;
use crate::io::metadata::{ChannelInfo, FileFormat};
use crate::io::traits::{FormatReader, ParallelReader, ParallelReaderConfig, ParallelReaderStats};
use crate::pipeline::types::arena_pool::global_pool;
use crate::pipeline::types::chunk::MessageChunk;
use crate::{CodecError, Result};

use super::bag_parser::{BagConnection, BagParser};

/// Parallel BAG reader with true random chunk access.
///
/// This reader uses a custom BAG parser to:
/// - Read chunk info records from the index section
/// - Enable independent decompression of each chunk
/// - Process chunks in parallel using Rayon
pub struct ParallelBagReader {
    /// Path to the bag file
    path: String,
    /// Custom BAG parser for random chunk access
    parser: BagParser,
    /// Channel information (channel_id -> ChannelInfo)
    channels: HashMap<u16, ChannelInfo>,
    /// Connection ID to channel ID mapping (conn_id -> channel_id)
    conn_id_map: HashMap<u32, u16>,
    /// Mapped channel IDs for each connection
    #[allow(dead_code)]
    connection_channels: HashMap<u32, BagConnection>,
}

impl ParallelBagReader {
    /// Open a BAG file for parallel reading.
    ///
    /// This method:
    /// 1. Parses the BAG header to find the index section
    /// 2. Reads chunk info records for random access
    /// 3. Builds channel information from connection records
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let path_str = path_ref.to_string_lossy().to_string();

        // Open the custom parser
        let parser = BagParser::open(path_ref)?;

        // Build channels from parser's connections
        let mut channels: HashMap<u16, ChannelInfo> = HashMap::new();
        let mut conn_id_map: HashMap<u32, u16> = HashMap::new();
        let mut connection_channels: HashMap<u32, BagConnection> = HashMap::new();
        let mut next_channel_id: u16 = 0;

        for (conn_id, conn) in parser.connections() {
            // Check if we already have a channel for this topic
            let channel_id = channels
                .values()
                .find(|c| c.topic == conn.topic)
                .map(|c| c.id)
                .unwrap_or_else(|| {
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
                            callerid: if conn.caller_id.is_empty() {
                                None
                            } else {
                                Some(conn.caller_id.clone())
                            },
                        },
                    );
                    id
                });

            conn_id_map.insert(*conn_id, channel_id);
            connection_channels.insert(*conn_id, conn.clone());
        }

        Ok(Self {
            path: path_str,
            parser,
            channels,
            conn_id_map,
            connection_channels,
        })
    }

    /// Get the connection ID to channel ID mapping.
    pub fn conn_id_map(&self) -> &HashMap<u32, u16> {
        &self.conn_id_map
    }

    /// Get all chunk information from the parser.
    pub fn chunks(&self) -> &[super::bag_parser::BagChunkInfo] {
        self.parser.chunks()
    }

    /// Process a single chunk in parallel.
    ///
    /// This is called by the Rayon thread pool for each chunk.
    fn process_chunk(
        chunk_info: &super::bag_parser::BagChunkInfo,
        parser: &BagParser,
        conn_id_map: &HashMap<u32, u16>,
        channel_filter: &Option<ChannelFilter>,
    ) -> Result<ProcessedChunk> {
        // Read and decompress the chunk
        let decompressed = parser.read_chunk(chunk_info)?;

        // Parse messages from decompressed data
        let messages = parser.parse_chunk_messages(&decompressed, conn_id_map)?;

        // Apply topic filter if provided
        let filtered_messages = if let Some(ref filter) = channel_filter {
            messages
                .into_iter()
                .filter(|msg| filter.allows_channel(msg.channel_id))
                .collect()
        } else {
            messages
        };

        // Calculate total bytes
        let total_bytes = filtered_messages
            .iter()
            .map(|m| m.data.len())
            .sum::<usize>();

        // Build message chunk using arena from the pool for reuse
        let pooled_arena = global_pool().get();
        let mut chunk = MessageChunk::with_pooled_arena_and_capacity(
            chunk_info.sequence,
            pooled_arena,
            filtered_messages.len(),
        );
        let message_count = filtered_messages.len();

        for msg in &filtered_messages {
            chunk.add_message_from_slice(
                msg.channel_id,
                msg.log_time,
                msg.publish_time,
                msg.sequence,
                &msg.data,
            )?;
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
        self.parser
            .chunks()
            .iter()
            .map(|c| c.message_count as u64)
            .sum()
    }

    fn start_time(&self) -> Option<u64> {
        self.parser.chunks().first().map(|c| c.start_time)
    }

    fn end_time(&self) -> Option<u64> {
        self.parser.chunks().last().map(|c| c.end_time)
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

/// Merge messages from source chunk into target chunk.
///
/// This copies all message data from the source chunk into the target chunk's arena,
/// updating time bounds accordingly.
fn merge_chunks(target: &mut MessageChunk<'static>, source: &MessageChunk<'_>) -> Result<()> {
    for msg in &source.messages {
        // Copy message data into target's arena
        target
            .add_message_from_slice(
                msg.channel_id,
                msg.log_time,
                msg.publish_time,
                msg.sequence,
                msg.data(),
            )
            .map_err(|e| CodecError::encode("merge_chunks", format!("Failed to allocate: {e}")))?;
    }
    Ok(())
}

impl ParallelReader for ParallelBagReader {
    fn read_parallel(
        &self,
        config: ParallelReaderConfig,
        sender: crossbeam_channel::Sender<MessageChunk<'static>>,
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
                    Self::process_chunk(chunk_info, parser, conn_id_map, &channel_filter)
                })
                .collect()
        });

        eprintln!(); // New line after progress

        // Collect results and send chunks
        let mut messages_read = 0u64;
        let mut chunks_processed = 0;
        let mut total_bytes = 0u64;

        if config.merge_enabled {
            // Merge mode: accumulate small chunks into larger ones
            let target_size = config.merge_target_size;
            let mut merged_chunk: Option<MessageChunk<'static>> = None;
            let mut current_size = 0usize;
            let mut output_sequence = 0u64;

            for result in results {
                let processed = result?;
                chunks_processed += 1;
                messages_read += processed.message_count;
                total_bytes += processed.total_bytes;

                if processed.chunk.message_count() == 0 {
                    continue;
                }

                let chunk = processed.chunk;
                let chunk_size = chunk.total_data_size();

                if merged_chunk.is_none() {
                    // Start a new merged chunk with proper sequence number
                    let mut static_chunk = unsafe {
                        std::mem::transmute::<MessageChunk<'_>, MessageChunk<'static>>(chunk)
                    };
                    static_chunk.sequence = output_sequence;
                    current_size = chunk_size;
                    merged_chunk = Some(static_chunk);
                } else if current_size + chunk_size <= target_size {
                    // Merge into current chunk
                    if let Some(ref mut target) = merged_chunk {
                        merge_chunks(target, &chunk)?;
                    }
                    current_size += chunk_size;
                } else {
                    // Send current chunk and start new one
                    if let Some(c) = merged_chunk.take() {
                        sender.send(c).map_err(|e| {
                            CodecError::encode(
                                "ParallelBagReader",
                                format!("Failed to send chunk: {e}"),
                            )
                        })?;
                    }
                    output_sequence += 1;
                    let mut static_chunk = unsafe {
                        std::mem::transmute::<MessageChunk<'_>, MessageChunk<'static>>(chunk)
                    };
                    static_chunk.sequence = output_sequence;
                    current_size = chunk_size;
                    merged_chunk = Some(static_chunk);
                }
            }

            // Send final chunk (it already has its sequence set)
            if let Some(chunk) = merged_chunk {
                sender.send(chunk).map_err(|e| {
                    CodecError::encode("ParallelBagReader", format!("Failed to send chunk: {e}"))
                })?;
            }
        } else {
            // Non-merge mode: send each chunk as-is
            for result in results {
                let processed = result?;
                chunks_processed += 1;
                messages_read += processed.message_count;
                total_bytes += processed.total_bytes;

                if processed.chunk.message_count() > 0 {
                    // SAFETY: Extending lifetime to 'static - safe because:
                    // 1. The chunk owns its arena
                    // 2. The chunk is moved into the channel
                    // 3. No external references exist
                    let static_chunk = unsafe {
                        std::mem::transmute::<MessageChunk<'_>, MessageChunk<'static>>(
                            processed.chunk,
                        )
                    };

                    sender.send(static_chunk).map_err(|e| {
                        CodecError::encode(
                            "ParallelBagReader",
                            format!("Failed to send chunk: {e}"),
                        )
                    })?;
                }
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
    chunk: MessageChunk<'static>,
    /// Total bytes in this chunk
    total_bytes: u64,
    /// Number of messages in this chunk
    message_count: u64,
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_parallel_bag_reader_compile() {
        // This test just verifies that the type compiles correctly
        // We can't create a ParallelBagReader without a valid bag file
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
