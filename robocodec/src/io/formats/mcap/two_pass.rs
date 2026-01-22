// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Two-pass MCAP reader for files without summary section.
//!
//! This module implements parallel reading for MCAP files that lack a summary section:
//! 1. Pass 1 (Sequential): Scan the data section to build chunk index
//! 2. Pass 2 (Parallel): Process chunks concurrently using the built index
//!
//! # When to Use
//!
//! This reader is designed for large MCAP files (>1GB) without a summary section.
//! For smaller files or files with a summary, use the sequential reader or
//! the parallel reader with summary support instead.
//!
//! # Performance Characteristics
//!
//! - **Discovery pass**: O(n) sequential scan, reads only record headers
//! - **Processing pass**: O(n/k) parallel processing where k = thread count
//! - Expected 2-4x speedup for files >1GB despite the two-pass overhead

use std::collections::HashMap;
use std::fs::File;
use std::io::{Cursor, Read, Write};
use std::path::Path;
use std::time::Instant;

use byteorder::{LittleEndian, ReadBytesExt};
use rayon::prelude::*;

use crate::io::filter::ChannelFilter;
use crate::io::metadata::{ChannelInfo, FileFormat, RawMessage};
use crate::io::traits::{
    FormatReader, MessageChunkData, ParallelReader, ParallelReaderConfig, ParallelReaderStats,
};
use crate::{CodecError, Result};

/// Chunk index information built during the discovery pass.
#[derive(Debug, Clone)]
struct ChunkIndex {
    /// Chunk sequence number
    sequence: usize,
    /// Offset of the chunk record in the file
    #[allow(dead_code)]
    chunk_offset: u64,
    /// Offset of the compressed data within the chunk
    data_offset: u64,
    /// Size of compressed data
    compressed_size: u64,
    /// Uncompressed size (from chunk header)
    uncompressed_size: u64,
    /// Compression format
    compression: String,
    /// Message start time
    message_start_time: u64,
    /// Message end time
    message_end_time: u64,
    /// Number of messages in this chunk (estimated)
    message_count: u64,
}

/// Channel information discovered during the scan.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct DiscoveredChannel {
    /// Channel ID
    id: u16,
    /// Topic name
    topic: String,
    /// Message type
    message_type: String,
    /// Encoding
    encoding: String,
    /// Schema ID
    schema_id: u16,
}

/// Two-pass MCAP reader for files without summary.
///
/// This reader performs a sequential discovery pass to build chunk indexes,
/// then processes chunks in parallel.
pub struct TwoPassMcapReader {
    /// Path to the MCAP file
    path: String,
    /// Memory-mapped file for random access
    mmap: memmap2::Mmap,
    /// Channel information
    channels: HashMap<u16, ChannelInfo>,
    /// Chunk indexes built during discovery pass
    chunk_indexes: Vec<ChunkIndex>,
    /// File size
    file_size: u64,
}

impl TwoPassMcapReader {
    /// Open an MCAP file for two-pass parallel reading.
    ///
    /// This method:
    /// 1. Opens and memory-maps the file
    /// 2. Scans the file to build chunk indexes (discovery pass)
    /// 3. Extracts channel information
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let path_str = path_ref.to_string_lossy().to_string();

        let file = File::open(path_ref).map_err(|e| {
            CodecError::encode("TwoPassMcapReader", format!("Failed to open file: {e}"))
        })?;

        let file_size = file
            .metadata()
            .map_err(|e| {
                CodecError::encode("TwoPassMcapReader", format!("Failed to get metadata: {e}"))
            })?
            .len();

        let mmap = unsafe { memmap2::Mmap::map(&file) }.map_err(|e| {
            CodecError::encode("TwoPassMcapReader", format!("Failed to mmap file: {e}"))
        })?;

        println!("Starting two-pass MCAP reader discovery...");
        let discovery_start = Instant::now();

        // Discovery pass: scan file to build chunk indexes
        let (chunk_indexes, channels) = Self::discover_chunks(&mmap)?;

        let discovery_duration = discovery_start.elapsed();
        println!(
            "Discovery complete: found {} chunks in {:.2}s",
            chunk_indexes.len(),
            discovery_duration.as_secs_f64()
        );

        Ok(Self {
            path: path_str,
            mmap,
            channels,
            chunk_indexes,
            file_size,
        })
    }

    /// Discovery pass: scan the file to build chunk indexes.
    ///
    /// This pass reads record headers and extracts:
    /// - Chunk records with their offsets and sizes
    /// - Channel records for metadata
    /// - Schema records for type information
    fn discover_chunks(mmap: &[u8]) -> Result<(Vec<ChunkIndex>, HashMap<u16, ChannelInfo>)> {
        let mut cursor = Cursor::new(mmap);
        let mut chunk_indexes = Vec::new();
        let mut channels = HashMap::new();
        // schemas: id -> (name, encoding, data)
        let mut schemas: HashMap<u16, (String, String, Vec<u8>)> = HashMap::new();

        // Skip MCAP header (magic + header record)
        cursor.set_position(8); // Skip magic

        // Read header record if present
        if cursor.position() < mmap.len() as u64 {
            let op = cursor.read_u8().unwrap_or(0);
            if op == 0x01 {
                // Header record
                let len = cursor.read_u64::<LittleEndian>().unwrap_or(0);
                cursor.set_position(cursor.position() + len);
            }
        }

        // Scan through records
        while cursor.position() < mmap.len() as u64 - 9 {
            let record_start = cursor.position();

            let op = match cursor.read_u8() {
                Ok(o) => o,
                Err(_) => break,
            };

            let record_len = match cursor.read_u64::<LittleEndian>() {
                Ok(l) => l,
                Err(_) => break,
            };

            let record_end = record_start + 9 + record_len;

            // Check if record exceeds file
            if record_end > mmap.len() as u64 {
                break;
            }

            match op {
                // Chunk (0x06) - This contains the message data
                // MCAP Chunk format:
                //   message_start_time: u64
                //   message_end_time: u64
                //   uncompressed_size: u64
                //   uncompressed_crc: u32
                //   compression: u32 length + string bytes
                //   compressed_size: u64
                //   compressed_data: [u8; compressed_size]
                0x06 => {
                    let message_start_time = cursor.read_u64::<LittleEndian>().map_err(|e| {
                        CodecError::encode(
                            "TwoPassMcapReader",
                            format!("Failed to read chunk start time: {e}"),
                        )
                    })?;

                    let message_end_time = cursor.read_u64::<LittleEndian>().map_err(|e| {
                        CodecError::encode(
                            "TwoPassMcapReader",
                            format!("Failed to read chunk end time: {e}"),
                        )
                    })?;

                    let uncompressed_size = cursor.read_u64::<LittleEndian>().map_err(|e| {
                        CodecError::encode(
                            "TwoPassMcapReader",
                            format!("Failed to read uncompressed size: {e}"),
                        )
                    })?;

                    let _uncompressed_crc = cursor.read_u32::<LittleEndian>().map_err(|e| {
                        CodecError::encode(
                            "TwoPassMcapReader",
                            format!("Failed to read uncompressed crc: {e}"),
                        )
                    })?;

                    // Read compression info (length-prefixed string with u32 length)
                    let compression_len = cursor.read_u32::<LittleEndian>().map_err(|e| {
                        CodecError::encode(
                            "TwoPassMcapReader",
                            format!("Failed to read compression length: {e}"),
                        )
                    })? as usize;

                    let mut compression_bytes = vec![0u8; compression_len.min(64)];
                    if compression_len > 0 && compression_len <= 64 {
                        cursor.read_exact(&mut compression_bytes)?;
                    } else if compression_len > 64 {
                        cursor.set_position(cursor.position() + compression_len as u64);
                    }
                    let compression = String::from_utf8_lossy(&compression_bytes).to_string();

                    let compressed_size = cursor.read_u64::<LittleEndian>().map_err(|e| {
                        CodecError::encode(
                            "TwoPassMcapReader",
                            format!("Failed to read compressed size: {e}"),
                        )
                    })?;

                    // The data starts right after the chunk header
                    let data_offset = cursor.position();

                    // Estimate message count (will be refined during processing)
                    let message_count = uncompressed_size / 100; // Rough estimate

                    chunk_indexes.push(ChunkIndex {
                        sequence: chunk_indexes.len(),
                        chunk_offset: record_start,
                        data_offset,
                        compressed_size,
                        uncompressed_size,
                        compression,
                        message_start_time,
                        message_end_time,
                        message_count,
                    });

                    // Skip to next record
                    cursor.set_position(record_end);
                }
                // Message (0x05) - Skip (will be processed in parallel pass)
                0x05 => {
                    cursor.set_position(record_end);
                }
                // Schema (0x03) - Store for channel info
                0x03 => {
                    let schema_id = cursor.read_u16::<LittleEndian>().map_err(|e| {
                        CodecError::encode(
                            "TwoPassMcapReader",
                            format!("Failed to read schema id: {e}"),
                        )
                    })?;

                    // Read name (length-prefixed string, u32 + bytes)
                    let name_len = cursor.read_u32::<LittleEndian>()? as usize;
                    let mut name_bytes = vec![0u8; name_len.min(1024)];
                    if name_len > 0 && name_len <= 1024 {
                        cursor.read_exact(&mut name_bytes)?;
                    } else if name_len > 1024 {
                        cursor.set_position(cursor.position() + name_len as u64);
                    }
                    let name = String::from_utf8_lossy(&name_bytes).to_string();

                    // Read encoding (length-prefixed string, u32 + bytes)
                    let encoding_len = cursor.read_u32::<LittleEndian>()? as usize;
                    let mut encoding_bytes = vec![0u8; encoding_len.min(64)];
                    if encoding_len > 0 && encoding_len <= 64 {
                        cursor.read_exact(&mut encoding_bytes)?;
                    } else if encoding_len > 64 {
                        cursor.set_position(cursor.position() + encoding_len as u64);
                    }
                    let encoding = String::from_utf8_lossy(&encoding_bytes).to_string();

                    // Read data (length-prefixed bytes, u32 + bytes)
                    let data_len = cursor.read_u32::<LittleEndian>()? as usize;
                    let data_start = cursor.position() as usize;
                    let data_end = data_start + data_len;

                    if data_end <= mmap.len() {
                        let schema_data = mmap[data_start..data_end].to_vec();
                        schemas.insert(schema_id, (name, encoding, schema_data));
                    }

                    cursor.set_position(record_end);
                }
                // Channel (0x04) - Build channel info
                0x04 => {
                    let channel_id = cursor.read_u16::<LittleEndian>().map_err(|e| {
                        CodecError::encode(
                            "TwoPassMcapReader",
                            format!("Failed to read channel id: {e}"),
                        )
                    })?;

                    let schema_id = cursor.read_u16::<LittleEndian>()?;

                    // Read topic (length-prefixed string, u32 + bytes)
                    let topic_len = cursor.read_u32::<LittleEndian>()? as usize;
                    let mut topic_bytes = vec![0u8; topic_len.min(1024)];
                    if topic_len > 0 && topic_len <= 1024 {
                        cursor.read_exact(&mut topic_bytes)?;
                    } else if topic_len > 1024 {
                        cursor.set_position(cursor.position() + topic_len as u64);
                    }
                    let topic = String::from_utf8_lossy(&topic_bytes).to_string();

                    // Read message_encoding (length-prefixed string, u32 + bytes)
                    let encoding_len = cursor.read_u32::<LittleEndian>()? as usize;
                    let mut encoding_bytes = vec![0u8; encoding_len.min(64)];
                    if encoding_len > 0 && encoding_len <= 64 {
                        cursor.read_exact(&mut encoding_bytes)?;
                    } else if encoding_len > 64 {
                        cursor.set_position(cursor.position() + encoding_len as u64);
                    }
                    let encoding = String::from_utf8_lossy(&encoding_bytes).to_string();

                    // Get schema info if available
                    let (message_type, schema_encoding, schema_data) = schemas
                        .get(&schema_id)
                        .map(|(name, enc, data)| (name.clone(), enc.clone(), data.clone()))
                        .unwrap_or_else(|| ("unknown".to_string(), encoding.clone(), Vec::new()));

                    let schema_text = if !schema_data.is_empty() {
                        Some(String::from_utf8_lossy(&schema_data).to_string())
                    } else {
                        None
                    };

                    channels.insert(
                        channel_id,
                        ChannelInfo {
                            id: channel_id,
                            topic,
                            message_type,
                            encoding,
                            schema: schema_text,
                            schema_data: if schema_data.is_empty() {
                                None
                            } else {
                                Some(schema_data)
                            },
                            schema_encoding: Some(schema_encoding),
                            message_count: 0,
                            callerid: None,
                        },
                    );

                    cursor.set_position(record_end);
                }
                // Attachment (0x09), AttachmentIndex (0x0A), Statistics (0x0B) - Skip
                0x09..=0x0B => {
                    cursor.set_position(record_end);
                }
                // ChunkIndex (0x08), MessageIndex (0x07) - Skip (only in summary)
                0x07 | 0x08 => {
                    cursor.set_position(record_end);
                }
                // Footer (0x02) or DataEnd (0x0F) - End of data section
                0x02 | 0x0F => {
                    break;
                }
                // Unknown record - Skip
                _ => {
                    cursor.set_position(record_end);
                }
            }
        }

        Ok((chunk_indexes, channels))
    }

    /// Process a single chunk in parallel.
    fn process_chunk(
        chunk_index: &ChunkIndex,
        mmap: &[u8],
        _channels: &HashMap<u16, ChannelInfo>,
        _channel_filter: &Option<ChannelFilter>,
    ) -> Result<ProcessedChunk> {
        let data_start = chunk_index.data_offset as usize;
        let data_end = data_start + chunk_index.compressed_size as usize;

        if data_end > mmap.len() {
            return Err(CodecError::encode(
                "TwoPassMcapReader",
                format!(
                    "Chunk {} data exceeds file bounds: {}..{} > {}",
                    chunk_index.sequence,
                    data_start,
                    data_end,
                    mmap.len()
                ),
            ));
        }

        let compressed_data = &mmap[data_start..data_end];

        // Decompress
        let uncompressed_data =
            if chunk_index.compression == "zstd" || chunk_index.compression == "zst" {
                zstd::bulk::decompress(compressed_data, chunk_index.uncompressed_size as usize)
                    .map_err(|e| {
                        CodecError::encode(
                            "TwoPassMcapReader",
                            format!("Zstd decompression failed: {e}"),
                        )
                    })?
            } else if chunk_index.compression == "lz4" {
                lz4_flex::decompress(compressed_data, chunk_index.uncompressed_size as usize)
                    .map_err(|e| {
                        CodecError::encode(
                            "TwoPassMcapReader",
                            format!("LZ4 decompression failed: {e}"),
                        )
                    })?
            } else if chunk_index.compression.is_empty() || chunk_index.compression == "none" {
                compressed_data.to_vec()
            } else {
                return Err(CodecError::unsupported(format!(
                    "Unsupported compression: {}",
                    chunk_index.compression
                )));
            };

        // Parse records from uncompressed chunk data
        // Chunk data contains: Schema records, Channel records, Message records
        let mut cursor = Cursor::new(&uncompressed_data);
        let mut messages = Vec::new();
        let mut total_bytes = 0u64;

        while cursor.position() < uncompressed_data.len() as u64 {
            // Read record header: opcode (1 byte) + length (8 bytes)
            let op = match cursor.read_u8() {
                Ok(o) => o,
                Err(_) => break,
            };

            let record_len = match cursor.read_u64::<LittleEndian>() {
                Ok(l) => l,
                Err(_) => break,
            };

            let record_start = cursor.position();
            let record_end = record_start + record_len;

            if record_end > uncompressed_data.len() as u64 {
                break;
            }

            match op {
                // Message (0x05)
                0x05 => {
                    let channel_id = cursor.read_u16::<LittleEndian>().map_err(|e| {
                        CodecError::encode(
                            "TwoPassMcapReader",
                            format!("Failed to read channel_id: {e}"),
                        )
                    })?;

                    let sequence = cursor.read_u32::<LittleEndian>().map_err(|e| {
                        CodecError::encode(
                            "TwoPassMcapReader",
                            format!("Failed to read sequence: {e}"),
                        )
                    })?;

                    let log_time = cursor.read_u64::<LittleEndian>().map_err(|e| {
                        CodecError::encode(
                            "TwoPassMcapReader",
                            format!("Failed to read log_time: {e}"),
                        )
                    })?;

                    let publish_time = cursor.read_u64::<LittleEndian>().map_err(|e| {
                        CodecError::encode(
                            "TwoPassMcapReader",
                            format!("Failed to read publish_time: {e}"),
                        )
                    })?;

                    // Data is the rest of the record
                    let data_len = (record_len - 22) as usize; // 2+4+8+8 = 22 bytes header
                    let data_start_pos = cursor.position() as usize;
                    let data_end_pos = data_start_pos + data_len;

                    if data_end_pos > uncompressed_data.len() {
                        break;
                    }

                    let data = uncompressed_data[data_start_pos..data_end_pos].to_vec();
                    total_bytes += data_len as u64;

                    messages.push(RawMessageData {
                        channel_id,
                        log_time,
                        publish_time,
                        sequence,
                        data,
                    });
                }
                // Schema (0x03), Channel (0x04) - Skip, already parsed in discovery
                0x03 | 0x04 => {}
                _ => {}
            }

            cursor.set_position(record_end);
        }

        // Build message chunk
        let mut chunk = MessageChunkData::new(chunk_index.sequence as u64);

        for msg in messages {
            let raw_msg = RawMessage {
                channel_id: msg.channel_id,
                log_time: msg.log_time,
                publish_time: msg.publish_time,
                data: msg.data,
                sequence: Some(msg.sequence as u64),
            };
            chunk.add_message(raw_msg);
        }

        let message_count = chunk.message_count();

        Ok(ProcessedChunk {
            chunk,
            total_bytes,
            message_count: message_count as u64,
        })
    }
}

impl FormatReader for TwoPassMcapReader {
    fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        &self.channels
    }

    fn message_count(&self) -> u64 {
        self.chunk_indexes.iter().map(|c| c.message_count).sum()
    }

    fn start_time(&self) -> Option<u64> {
        self.chunk_indexes.first().map(|c| c.message_start_time)
    }

    fn end_time(&self) -> Option<u64> {
        self.chunk_indexes.last().map(|c| c.message_end_time)
    }

    fn path(&self) -> &str {
        &self.path
    }

    fn format(&self) -> FileFormat {
        FileFormat::Mcap
    }

    fn file_size(&self) -> u64 {
        self.file_size
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl ParallelReader for TwoPassMcapReader {
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
            "Starting two-pass MCAP parallel reader with {} worker threads...",
            num_threads
        );
        println!("  File: {}", self.path);
        println!("  Chunks to process: {}", self.chunk_indexes.len());

        let total_start = Instant::now();

        // Build channel filter from topic filter
        let channel_filter = config
            .topic_filter
            .as_ref()
            .map(|tf| ChannelFilter::from_topic_filter(tf, self.channels()));

        // Create thread pool for controlled parallelism
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .thread_name(|index| format!("mcap-two-pass-{}", index))
            .build()
            .map_err(|e| {
                CodecError::encode(
                    "TwoPassMcapReader",
                    format!("Failed to create thread pool: {e}"),
                )
            })?;

        // Get references for parallel processing
        let chunk_indexes = &self.chunk_indexes;
        let mmap = &self.mmap;
        let channels = &self.channels;

        // Process chunks in parallel
        let results: Vec<Result<ProcessedChunk>> = pool.install(|| {
            chunk_indexes
                .par_iter()
                .enumerate()
                .map(|(i, chunk_index)| {
                    if i % config.progress_interval == 0 && i > 0 {
                        eprint!("\rProcessing chunk {}/{}...", i, chunk_indexes.len());
                        let _ = std::io::stdout().flush();
                    }
                    Self::process_chunk(chunk_index, mmap, channels, &channel_filter)
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
                    CodecError::encode("TwoPassMcapReader", format!("Failed to send chunk: {e}"))
                })?;
            }
        }

        let duration = total_start.elapsed();

        println!("Two-pass MCAP reader complete:");
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
        self.chunk_indexes.len()
    }

    fn supports_parallel(&self) -> bool {
        !self.chunk_indexes.is_empty()
    }
}

/// Raw message data extracted from a chunk.
#[derive(Debug)]
struct RawMessageData {
    channel_id: u16,
    log_time: u64,
    publish_time: u64,
    sequence: u32,
    data: Vec<u8>,
}

/// Processed chunk ready to be sent to the output channel.
struct ProcessedChunk {
    chunk: MessageChunkData,
    total_bytes: u64,
    message_count: u64,
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_two_pass_mcap_reader_compile() {
        // This test just verifies that the type compiles correctly
        // We can't create a TwoPassMcapReader without a valid MCAP file
    }
}
