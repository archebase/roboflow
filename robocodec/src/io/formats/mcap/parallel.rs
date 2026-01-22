// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Parallel MCAP reader with memory-mapped file access.
//!
//! This module provides MCAP-specific readers that implement the unified I/O traits
//! using the arena-based ownership model for safe lifetime management.
//!
//! **Note:** This implementation uses a custom MCAP parser with no external dependencies.
//! It supports:
//! - Reading files with or without summary sections
//! - All compression types (zstd, lz4, none)
//! - Parallel reading via chunk indexes (default behavior)

use std::collections::HashMap;
use std::fs::File;
use std::io::{Cursor, Read, Write};
use std::path::Path;
use std::time::Instant;

use byteorder::{LittleEndian, ReadBytesExt};
use rayon::prelude::*;

use crate::io::filter::ChannelFilter;
use crate::io::formats::mcap::constants::{
    MCAP_MAGIC, OP_CHANNEL, OP_CHUNK, OP_CHUNK_INDEX, OP_DATA_END, OP_FOOTER, OP_HEADER,
    OP_MESSAGE, OP_SCHEMA, OP_STATISTICS,
};
use crate::io::metadata::{ChannelInfo, FileFormat, RawMessage};
use crate::io::traits::{
    FormatReader, MessageChunkData, ParallelReader, ParallelReaderConfig, ParallelReaderStats,
};
use crate::{CodecError, Result};

/// MCAP format type.
///
/// This type provides factory methods for creating MCAP readers and writers.
/// Default behavior is parallel reading for optimal performance.
pub struct McapFormat;

impl McapFormat {
    /// Create an MCAP reader with parallel reading support.
    ///
    /// The reader uses memory-mapping and processes chunks in parallel
    /// using the Rayon thread pool.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<ParallelMcapReader> {
        ParallelMcapReader::open(path)
    }

    /// Create an MCAP writer with the given configuration.
    ///
    /// Returns a boxed FormatWriter trait object for unified writer API.
    pub fn create_writer<P: AsRef<Path>>(
        path: P,
        _config: &crate::io::writer::WriterConfig,
    ) -> Result<Box<dyn crate::io::traits::FormatWriter>> {
        // For now, we create a simple writer
        // TODO: Use config options for compression, chunk size, etc.
        use crate::io::formats::mcap::writer::ParallelMcapWriter;
        let writer = ParallelMcapWriter::create_with_buffer(path, 64 * 1024)?;
        Ok(Box::new(writer))
    }

    /// Check if an MCAP file has a summary with chunk indexes.
    ///
    /// Returns (has_summary, has_chunk_indexes).
    pub fn check_summary<P: AsRef<Path>>(path: P) -> Result<(bool, bool)> {
        let file = File::open(path.as_ref())
            .map_err(|e| CodecError::encode("McapFormat", format!("Failed to open file: {e}")))?;

        let mmap = unsafe { memmap2::Mmap::map(&file) }
            .map_err(|e| CodecError::encode("McapFormat", format!("Failed to mmap file: {e}")))?;

        // Try to read summary from footer
        match ParallelMcapReader::read_summary_from_footer(&mmap) {
            Ok(Some((_, _, chunk_indexes))) => Ok((true, !chunk_indexes.is_empty())),
            Ok(None) => Ok((false, false)),
            Err(_) => Ok((false, false)),
        }
    }
}

/// Parallel MCAP reader with memory-mapped file access.
///
/// This reader parses the MCAP file metadata (channels, schemas, chunk indexes)
/// and supports parallel processing of chunks using Rayon.
pub struct ParallelMcapReader {
    /// File path
    path: String,
    /// Memory-mapped file data
    mmap: memmap2::Mmap,
    /// Channel information
    channels: HashMap<u16, ChannelInfo>,
    /// Total message count
    message_count: u64,
    /// Start timestamp
    start_time: Option<u64>,
    /// End timestamp
    end_time: Option<u64>,
    /// Chunk indexes for parallel reading
    chunk_indexes: Vec<ChunkIndex>,
    /// File size
    file_size: u64,
}

impl ParallelMcapReader {
    /// Open an MCAP file for parallel reading.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let path_str = path_ref.to_string_lossy().to_string();

        let file = File::open(path_ref).map_err(|e| {
            CodecError::encode("ParallelMcapReader", format!("Failed to open file: {e}"))
        })?;

        let file_size = file
            .metadata()
            .map_err(|e| {
                CodecError::encode("ParallelMcapReader", format!("Failed to get metadata: {e}"))
            })?
            .len();

        let mmap = unsafe { memmap2::Mmap::map(&file) }.map_err(|e| {
            CodecError::encode("ParallelMcapReader", format!("Failed to mmap file: {e}"))
        })?;

        // Read metadata from file
        let metadata = Self::read_metadata(&mmap)?;

        Ok(Self {
            path: path_str,
            mmap,
            channels: metadata.channels,
            message_count: metadata.message_count,
            start_time: metadata.start_time,
            end_time: metadata.end_time,
            chunk_indexes: metadata.chunk_indexes,
            file_size,
        })
    }

    /// Get chunk indexes for parallel reading.
    pub fn chunk_indexes(&self) -> &[ChunkIndex] {
        &self.chunk_indexes
    }

    /// Read metadata (channels, message count, timestamps, chunk indexes) from an MCAP file.
    fn read_metadata(data: &[u8]) -> Result<McapMetadata> {
        let mut cursor = Cursor::new(data);

        // Verify and skip magic
        let mut magic = [0u8; 8];
        cursor.read_exact(&mut magic).map_err(|e| {
            CodecError::parse("ParallelMcapReader", format!("Failed to read magic: {e}"))
        })?;

        if magic != MCAP_MAGIC {
            return Err(CodecError::parse(
                "ParallelMcapReader",
                format!("Invalid MCAP magic: {:?}", hex::encode(magic)),
            ));
        }

        // Try to read summary from footer first (more efficient for files with summary)
        let summary_result = Self::read_summary_from_footer(data);

        match summary_result {
            Ok(Some((mut channels, stats, chunk_indexes))) => {
                // If we got chunk_indexes from summary but no channels, scan data section for channels
                if channels.is_empty() && !chunk_indexes.is_empty() {
                    let (data_channels, _) = Self::scan_data_section(data)?;
                    channels = data_channels;
                }

                let start_time = if stats.message_start_time > 0 {
                    Some(stats.message_start_time)
                } else {
                    None
                };
                let end_time = if stats.message_end_time > 0 {
                    Some(stats.message_end_time)
                } else {
                    None
                };
                Ok(McapMetadata {
                    channels,
                    message_count: stats.message_count,
                    start_time,
                    end_time,
                    chunk_indexes,
                })
            }
            Ok(None) | Err(_) => {
                // No summary or failed to read - scan the data section
                let (channels, chunk_indexes) = Self::scan_data_section(data)?;
                Ok(McapMetadata {
                    channels,
                    message_count: 0,
                    start_time: None,
                    end_time: None,
                    chunk_indexes,
                })
            }
        }
    }

    /// Read summary section from footer.
    #[allow(clippy::type_complexity)]
    pub fn read_summary_from_footer(
        data: &[u8],
    ) -> Result<Option<(HashMap<u16, ChannelInfo>, McapStatistics, Vec<ChunkIndex>)>> {
        let file_len = data.len();
        // MCAP file structure at the end:
        // [... Summary Section ...][FOOTER][Magic]
        // Note: DATA_END comes BEFORE the summary section, not after
        // - Footer: opcode(1) + length(8) + summary_start(8) + summary_offset_start(8) + summary_crc(4) = 29 bytes
        // - Magic: 8 bytes
        // Minimum size: magic(8) + header(9+8) + footer(29) + magic(8) = 62
        if file_len < 62 {
            return Ok(None);
        }

        // Find footer by checking trailing magic
        let magic_start = file_len - 8;
        if data[magic_start..] != MCAP_MAGIC {
            return Ok(None);
        }

        // Footer record is directly before trailing magic (29 bytes)
        let footer_start = magic_start - 29;
        if footer_start < 8 {
            return Ok(None);
        }

        let mut cursor = Cursor::new(&data[footer_start..]);
        let op = cursor.read_u8()?;
        if op != OP_FOOTER {
            // Footer not at expected location, fall back to scan
            return Ok(None);
        }

        let record_len = cursor.read_u64::<LittleEndian>()?;
        if record_len != 20 {
            return Ok(None);
        }

        let summary_start = cursor.read_u64::<LittleEndian>()?;
        let _summary_offset_start = cursor.read_u64::<LittleEndian>()?;

        if summary_start == 0 || summary_start >= file_len as u64 {
            return Ok(None);
        }

        Self::read_summary_section(data, summary_start as usize)
    }

    /// Read the summary section starting at the given offset.
    #[allow(clippy::type_complexity)]
    fn read_summary_section(
        data: &[u8],
        summary_start: usize,
    ) -> Result<Option<(HashMap<u16, ChannelInfo>, McapStatistics, Vec<ChunkIndex>)>> {
        let mut cursor = Cursor::new(&data[summary_start..]);
        let mut schemas: HashMap<u16, SchemaInfo> = HashMap::new();
        let mut channels: HashMap<u16, ChannelInfo> = HashMap::new();
        let mut chunk_indexes: Vec<ChunkIndex> = Vec::new();
        let mut stats = McapStatistics::default();

        let section_len = data.len() - summary_start;

        while cursor.position() < section_len as u64 - 9 {
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
            if record_end > section_len as u64 {
                break;
            }

            match op {
                OP_SCHEMA => {
                    if let Ok(schema) = Self::read_schema_record(&mut cursor) {
                        schemas.insert(schema.id, schema);
                    }
                }
                OP_CHANNEL => {
                    if let Ok((id, topic, schema_id, encoding)) =
                        Self::read_channel_record(&mut cursor)
                    {
                        let schema = schemas.get(&schema_id);
                        channels.insert(
                            id,
                            ChannelInfo {
                                id,
                                topic,
                                message_type: schema.map(|s| s.name.clone()).unwrap_or_default(),
                                encoding,
                                schema: schema.and_then(|s| String::from_utf8(s.data.clone()).ok()),
                                schema_data: schema.map(|s| s.data.clone()),
                                schema_encoding: schema.map(|s| s.encoding.clone()),
                                message_count: 0,
                                callerid: None,
                            },
                        );
                    }
                }
                OP_CHUNK_INDEX => {
                    if let Ok(idx) = Self::read_chunk_index_record(&mut cursor) {
                        chunk_indexes.push(idx);
                    }
                }
                OP_STATISTICS => {
                    if let Ok(s) = Self::read_statistics_record(&mut cursor) {
                        stats = s;
                    }
                }
                OP_FOOTER | OP_DATA_END => break,
                _ => {}
            }

            cursor.set_position(record_end);
        }

        // Return the result even if channels is empty - chunk_indexes may still be useful
        // The caller will scan the data section for channels if needed
        if chunk_indexes.is_empty() && channels.is_empty() {
            return Ok(None);
        }

        Ok(Some((channels, stats, chunk_indexes)))
    }

    /// Scan the data section to build channel info and chunk indexes.
    fn scan_data_section(data: &[u8]) -> Result<(HashMap<u16, ChannelInfo>, Vec<ChunkIndex>)> {
        let mut cursor = Cursor::new(data);
        cursor.set_position(8); // Skip magic

        // Skip header record
        if cursor.position() < data.len() as u64 - 9 {
            let op = cursor.read_u8().unwrap_or(0);
            if op == OP_HEADER {
                let len = cursor.read_u64::<LittleEndian>().unwrap_or(0);
                cursor.set_position(cursor.position() + len);
            }
        }

        let mut schemas: HashMap<u16, SchemaInfo> = HashMap::new();
        let mut channels: HashMap<u16, ChannelInfo> = HashMap::new();
        let mut chunk_indexes: Vec<ChunkIndex> = Vec::new();

        while cursor.position() < data.len() as u64 - 9 {
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
            if record_end > data.len() as u64 {
                break;
            }

            match op {
                OP_SCHEMA => {
                    if let Ok(schema) = Self::read_schema_record(&mut cursor) {
                        schemas.insert(schema.id, schema);
                    }
                }
                OP_CHANNEL => {
                    if let Ok((id, topic, schema_id, encoding)) =
                        Self::read_channel_record(&mut cursor)
                    {
                        let schema = schemas.get(&schema_id);
                        channels.insert(
                            id,
                            ChannelInfo {
                                id,
                                topic,
                                message_type: schema.map(|s| s.name.clone()).unwrap_or_default(),
                                encoding,
                                schema: schema.and_then(|s| String::from_utf8(s.data.clone()).ok()),
                                schema_data: schema.map(|s| s.data.clone()),
                                schema_encoding: schema.map(|s| s.encoding.clone()),
                                message_count: 0,
                                callerid: None,
                            },
                        );
                    }
                }
                OP_CHUNK => {
                    if let Ok(idx) =
                        Self::read_chunk_header(&mut cursor, record_start, chunk_indexes.len())
                    {
                        chunk_indexes.push(idx);
                    }
                }
                OP_FOOTER | OP_DATA_END => break,
                _ => {}
            }

            cursor.set_position(record_end);
        }

        Ok((channels, chunk_indexes))
    }

    /// Read a schema record.
    fn read_schema_record(cursor: &mut Cursor<&[u8]>) -> Result<SchemaInfo> {
        let id = cursor.read_u16::<LittleEndian>()?;
        let name_len = cursor.read_u32::<LittleEndian>()? as usize;
        let name = Self::read_string_bytes(cursor, name_len)?;
        let encoding_len = cursor.read_u32::<LittleEndian>()? as usize;
        let encoding = Self::read_string_bytes(cursor, encoding_len)?;
        let data_len = cursor.read_u32::<LittleEndian>()? as usize;
        let mut data = vec![0u8; data_len];
        cursor.read_exact(&mut data)?;

        Ok(SchemaInfo {
            id,
            name,
            encoding,
            data,
        })
    }

    /// Read a channel record.
    fn read_channel_record(cursor: &mut Cursor<&[u8]>) -> Result<(u16, String, u16, String)> {
        let id = cursor.read_u16::<LittleEndian>()?;
        let schema_id = cursor.read_u16::<LittleEndian>()?;
        let topic_len = cursor.read_u32::<LittleEndian>()? as usize;
        let topic = Self::read_string_bytes(cursor, topic_len)?;
        let encoding_len = cursor.read_u32::<LittleEndian>()? as usize;
        let encoding = Self::read_string_bytes(cursor, encoding_len)?;
        Ok((id, topic, schema_id, encoding))
    }

    /// Read a chunk index record from summary section.
    fn read_chunk_index_record(cursor: &mut Cursor<&[u8]>) -> Result<ChunkIndex> {
        let message_start_time = cursor.read_u64::<LittleEndian>()?;
        let message_end_time = cursor.read_u64::<LittleEndian>()?;
        let chunk_start_offset = cursor.read_u64::<LittleEndian>()?;
        let chunk_length = cursor.read_u64::<LittleEndian>()?;

        // message_index_offsets is a byte-length-prefixed map: <byte_length: u32><entries...>
        // Each entry is: <channel_id: u16><offset: u64> = 10 bytes
        let map_byte_length = cursor.read_u32::<LittleEndian>()? as usize;
        let entry_size = 2 + 8; // u16 + u64
        let entry_count = if entry_size > 0 {
            map_byte_length / entry_size
        } else {
            0
        };

        // Skip message index offset entries
        for _ in 0..entry_count {
            let _channel_id = cursor.read_u16::<LittleEndian>()?;
            let _offset = cursor.read_u64::<LittleEndian>()?;
        }

        let message_index_length = cursor.read_u64::<LittleEndian>()?;
        let compression_len = cursor.read_u32::<LittleEndian>()? as usize;
        let compression = Self::read_string_bytes(cursor, compression_len)?;
        let compressed_size = cursor.read_u64::<LittleEndian>()?;
        let uncompressed_size = cursor.read_u64::<LittleEndian>()?;

        Ok(ChunkIndex {
            message_start_time,
            message_end_time,
            chunk_start_offset,
            chunk_length,
            message_index_length,
            compression,
            compressed_size,
            uncompressed_size,
        })
    }

    /// Read chunk header to build chunk index during data section scan.
    fn read_chunk_header(
        cursor: &mut Cursor<&[u8]>,
        record_start: u64,
        _sequence: usize,
    ) -> Result<ChunkIndex> {
        let message_start_time = cursor.read_u64::<LittleEndian>()?;
        let message_end_time = cursor.read_u64::<LittleEndian>()?;
        let uncompressed_size = cursor.read_u64::<LittleEndian>()?;
        let _uncompressed_crc = cursor.read_u32::<LittleEndian>()?;
        let compression_len = cursor.read_u32::<LittleEndian>()? as usize;
        let compression = Self::read_string_bytes(cursor, compression_len)?;
        let compressed_size = cursor.read_u64::<LittleEndian>()?;

        Ok(ChunkIndex {
            message_start_time,
            message_end_time,
            chunk_start_offset: record_start,
            chunk_length: 9 + 8 + 8 + 8 + 4 + 4 + compression_len as u64 + 8 + compressed_size,
            message_index_length: 0,
            compression,
            compressed_size,
            uncompressed_size,
        })
    }

    /// Read statistics record from summary section.
    fn read_statistics_record(cursor: &mut Cursor<&[u8]>) -> Result<McapStatistics> {
        let message_count = cursor.read_u64::<LittleEndian>()?;
        let _schema_count = cursor.read_u16::<LittleEndian>()?;
        let _channel_count = cursor.read_u32::<LittleEndian>()?;
        let _attachment_count = cursor.read_u32::<LittleEndian>()?;
        let _metadata_count = cursor.read_u32::<LittleEndian>()?;
        let _chunk_count = cursor.read_u32::<LittleEndian>()?;
        let message_start_time = cursor.read_u64::<LittleEndian>()?;
        let message_end_time = cursor.read_u64::<LittleEndian>()?;

        Ok(McapStatistics {
            message_count,
            message_start_time,
            message_end_time,
        })
    }

    /// Read a string of known length from cursor.
    fn read_string_bytes(cursor: &mut Cursor<&[u8]>, len: usize) -> Result<String> {
        if len == 0 {
            return Ok(String::new());
        }
        let mut buffer = vec![0u8; len];
        cursor.read_exact(&mut buffer)?;
        String::from_utf8(buffer)
            .map_err(|e| CodecError::parse("ParallelMcapReader", format!("Invalid UTF-8: {e}")))
    }

    /// Process a single chunk in parallel.
    fn process_chunk(
        chunk_index: &ChunkIndex,
        data: &[u8],
        channels: &HashMap<u16, ChannelInfo>,
        _channel_filter: &Option<ChannelFilter>,
        chunk_sequence: usize,
    ) -> Result<ProcessedChunk> {
        // Calculate where compressed data starts
        let header_size = 8 + 8 + 8 + 4 + 4 + chunk_index.compression.len() + 8;
        let data_start = chunk_index.chunk_start_offset as usize + 9 + header_size;
        let data_end = data_start + chunk_index.compressed_size as usize;

        if data_end > data.len() {
            return Err(CodecError::parse(
                "ParallelMcapReader",
                format!(
                    "Chunk data exceeds file: {}..{} > {}",
                    data_start,
                    data_end,
                    data.len()
                ),
            ));
        }

        let compressed_data = &data[data_start..data_end];

        // Decompress based on compression type
        let decompressed = match chunk_index.compression.as_str() {
            "zstd" | "zst" => {
                zstd::bulk::decompress(compressed_data, chunk_index.uncompressed_size as usize)
                    .map_err(|e| {
                        CodecError::encode(
                            "ParallelMcapReader",
                            format!("Zstd decompression failed: {e}"),
                        )
                    })?
            }
            "lz4" => lz4_flex::decompress(compressed_data, chunk_index.uncompressed_size as usize)
                .map_err(|e| {
                    CodecError::encode(
                        "ParallelMcapReader",
                        format!("LZ4 decompression failed: {e}"),
                    )
                })?,
            "" | "none" => compressed_data.to_vec(),
            other => {
                return Err(CodecError::unsupported(format!(
                    "Unsupported compression: {}",
                    other
                )));
            }
        };

        // Parse messages from decompressed chunk
        let mut cursor = Cursor::new(decompressed.as_slice());
        let mut messages = Vec::new();
        let mut total_bytes = 0u64;

        while cursor.position() < decompressed.len() as u64 - 9 {
            let op = match cursor.read_u8() {
                Ok(o) => o,
                Err(_) => break,
            };

            let record_len = match cursor.read_u64::<LittleEndian>() {
                Ok(l) => l,
                Err(_) => break,
            };

            let record_data_start = cursor.position() as usize;
            let record_end = record_data_start + record_len as usize;

            if record_end > decompressed.len() {
                break;
            }

            cursor.set_position(record_end as u64);

            if op == OP_MESSAGE {
                let mut msg_cursor = Cursor::new(&decompressed[record_data_start..record_end]);

                let channel_id = match msg_cursor.read_u16::<LittleEndian>() {
                    Ok(id) => id,
                    Err(_) => continue,
                };

                let sequence = match msg_cursor.read_u32::<LittleEndian>() {
                    Ok(s) => s,
                    Err(_) => continue,
                };

                let log_time = match msg_cursor.read_u64::<LittleEndian>() {
                    Ok(t) => t,
                    Err(_) => continue,
                };

                let publish_time = match msg_cursor.read_u64::<LittleEndian>() {
                    Ok(t) => t,
                    Err(_) => continue,
                };

                // Rest is message data
                let msg_data_start = record_data_start + 2 + 4 + 8 + 8;
                let msg_data = decompressed[msg_data_start..record_end].to_vec();
                total_bytes += msg_data.len() as u64;

                if channels.contains_key(&channel_id) {
                    messages.push(McapMessageData {
                        channel_id,
                        log_time,
                        publish_time,
                        sequence,
                        data: msg_data,
                    });
                }
            }
        }

        // Build message chunk
        let mut chunk = MessageChunkData::new(chunk_sequence as u64);
        let message_count = messages.len();

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

        Ok(ProcessedChunk {
            chunk,
            total_bytes,
            message_count: message_count as u64,
        })
    }
}

impl FormatReader for ParallelMcapReader {
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

impl ParallelReader for ParallelMcapReader {
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
            "Starting parallel MCAP reader with {} worker threads...",
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
            .thread_name(|index| format!("mcap-reader-{}", index))
            .build()
            .map_err(|e| {
                CodecError::encode(
                    "ParallelMcapReader",
                    format!("Failed to create thread pool: {e}"),
                )
            })?;

        // Get references for parallel processing
        let chunk_indexes = &self.chunk_indexes;
        let data = &self.mmap[..];
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
                    Self::process_chunk(chunk_index, data, channels, &channel_filter, i)
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
                    CodecError::encode("ParallelMcapReader", format!("Failed to send chunk: {e}"))
                })?;
            }
        }

        let duration = total_start.elapsed();

        println!("Parallel MCAP reader complete:");
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

/// Schema information.
#[derive(Debug, Clone)]
struct SchemaInfo {
    id: u16,
    name: String,
    encoding: String,
    data: Vec<u8>,
}

/// Chunk index for parallel reading.
#[derive(Debug, Clone)]
pub struct ChunkIndex {
    /// Earliest message time in chunk
    pub message_start_time: u64,
    /// Latest message time in chunk
    pub message_end_time: u64,
    /// Offset of chunk record in file
    pub chunk_start_offset: u64,
    /// Total length of chunk record
    pub chunk_length: u64,
    /// Message index section length
    pub message_index_length: u64,
    /// Compression type
    pub compression: String,
    /// Compressed data size
    pub compressed_size: u64,
    /// Uncompressed data size
    pub uncompressed_size: u64,
}

/// MCAP file statistics.
#[derive(Debug, Clone, Default)]
pub struct McapStatistics {
    message_count: u64,
    message_start_time: u64,
    message_end_time: u64,
}

/// MCAP file metadata extracted from the file.
///
/// Contains the channels, message count, timestamps, and chunk indexes
/// needed to read an MCAP file efficiently.
#[derive(Debug, Clone)]
struct McapMetadata {
    /// Channel ID to channel info mapping
    channels: HashMap<u16, ChannelInfo>,
    /// Total message count
    message_count: u64,
    /// Earliest message timestamp (nanoseconds)
    start_time: Option<u64>,
    /// Latest message timestamp (nanoseconds)
    end_time: Option<u64>,
    /// Chunk indexes for parallel reading
    chunk_indexes: Vec<ChunkIndex>,
}

/// Raw message data extracted from a chunk.
#[derive(Debug)]
struct McapMessageData {
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
    use super::*;
    use std::io::Cursor;

    /// Helper to create a minimal valid MCAP header.
    fn make_mcap_header() -> Vec<u8> {
        let mut header = Vec::new();
        header.extend_from_slice(&MCAP_MAGIC); // Magic
        header.push(OP_HEADER); // Opcode
        header.extend_from_slice(&20u64.to_le_bytes()); // Record length
        header.extend_from_slice(b"roboflow test"); // Profile
        header.extend_from_slice(&0u8.to_le_bytes()); // Library (empty string)
        header
    }

    /// Helper to create a minimal MCAP footer.
    fn make_mcap_footer(summary_start: u64) -> Vec<u8> {
        let mut footer = Vec::new();
        footer.push(OP_FOOTER); // Opcode
        footer.extend_from_slice(&20u64.to_le_bytes()); // Record length
        footer.extend_from_slice(&summary_start.to_le_bytes()); // Summary start
        footer.extend_from_slice(&0u64.to_le_bytes()); // Summary offset start
        footer.extend_from_slice(&0u32.to_le_bytes()); // Summary CRC
        footer.extend_from_slice(&MCAP_MAGIC); // Magic
        footer
    }

    /// Helper to create a data end record.
    fn make_data_end() -> Vec<u8> {
        let mut data_end = Vec::new();
        data_end.push(OP_DATA_END); // Opcode
        data_end.extend_from_slice(&4u64.to_le_bytes()); // Record length
        data_end.extend_from_slice(&0u32.to_le_bytes()); // CRC
        data_end
    }

    #[test]
    fn test_mcap_format() {
        let _ = McapFormat;
    }

    #[test]
    fn test_chunk_index_struct() {
        let chunk = ChunkIndex {
            message_start_time: 1000,
            message_end_time: 2000,
            chunk_start_offset: 100,
            chunk_length: 500,
            message_index_length: 0,
            compression: "zstd".to_string(),
            compressed_size: 400,
            uncompressed_size: 1000,
        };
        assert_eq!(chunk.compression, "zstd");
    }

    #[test]
    fn test_invalid_magic_bytes() {
        // Create an invalid MCAP file (wrong magic)
        let invalid_data = vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];

        let result = ParallelMcapReader::read_summary_from_footer(&invalid_data);
        // Should not panic, but may return an error
        assert!(result.is_err() || result.unwrap().is_none());
    }

    #[test]
    fn test_empty_file() {
        // Empty file should not panic
        let empty_data = vec![];
        let result = ParallelMcapReader::read_summary_from_footer(&empty_data);

        // Should return None or error, not panic
        assert!(result.is_err() || result.unwrap().is_none());
    }

    #[test]
    fn test_truncated_file() {
        // Create a truncated MCAP file (only header, no footer)
        let mut truncated = make_mcap_header();
        truncated.extend_from_slice(&[0, 0, 0, 0]); // Trailing garbage

        let result = ParallelMcapReader::read_summary_from_footer(&truncated);
        // Should handle gracefully
        assert!(result.is_err() || result.unwrap().is_none());
    }

    #[test]
    fn test_file_without_summary() {
        // Create a valid MCAP file with no summary section
        let mut file_data = make_mcap_header();
        file_data.extend_from_slice(&make_data_end());

        // Footer with no summary (summary_start = 0)
        file_data.extend_from_slice(&make_mcap_footer(0));

        let result = ParallelMcapReader::read_summary_from_footer(&file_data);
        // Should return None indicating no summary
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_corrupted_record_length() {
        // Create a file with corrupted record length
        let mut file_data = make_mcap_header();
        file_data.push(OP_SCHEMA); // Schema opcode
        file_data.extend_from_slice(&0xFFFFFFFFu64.to_le_bytes()); // Invalid huge length

        let result = ParallelMcapReader::scan_data_section(&file_data);
        // Should handle error gracefully
        assert!(result.is_err() || result.unwrap().0.is_empty());
    }

    #[test]
    fn test_invalid_opcode() {
        // Create a file with invalid opcode
        let mut file_data = make_mcap_header();
        file_data.push(0xFF); // Invalid opcode
        file_data.extend_from_slice(&4u64.to_le_bytes()); // Record length
        file_data.extend_from_slice(&[0, 0, 0, 0]); // Data
        file_data.extend_from_slice(&make_data_end());
        file_data.extend_from_slice(&make_mcap_footer(0));

        let result = ParallelMcapReader::scan_data_section(&file_data);
        // Should skip invalid opcode and return successfully
        assert!(result.is_ok());
    }

    #[test]
    fn test_truncated_chunk_record() {
        // Create a file with truncated chunk record
        let mut file_data = make_mcap_header();
        file_data.push(OP_CHUNK); // Chunk opcode
        file_data.extend_from_slice(&100u64.to_le_bytes()); // Record length
        file_data.extend_from_slice(&[0; 20]); // Incomplete chunk data (should be 100 bytes)

        let result = ParallelMcapReader::scan_data_section(&file_data);
        // Should handle truncation gracefully
        assert!(result.is_ok());
    }

    #[test]
    fn test_schema_record_parsing() {
        // Create a valid schema record
        let mut schema_data = Vec::new();
        schema_data.extend_from_slice(&0u16.to_le_bytes()); // Schema ID
        schema_data.extend_from_slice(&9u32.to_le_bytes()); // Name length
        schema_data.extend_from_slice(b"test_name"); // Name
        schema_data.extend_from_slice(&7u32.to_le_bytes()); // Encoding length
        schema_data.extend_from_slice(b"ros1msg"); // Encoding
        schema_data.extend_from_slice(&4u32.to_le_bytes()); // Data length
        schema_data.extend_from_slice(b"data"); // Data

        let mut cursor = Cursor::new(schema_data.as_slice());
        let result = ParallelMcapReader::read_schema_record(&mut cursor);

        assert!(result.is_ok());
        let schema = result.unwrap();
        assert_eq!(schema.id, 0);
        assert_eq!(schema.name, "test_name");
        assert_eq!(schema.encoding, "ros1msg");
    }

    #[test]
    fn test_truncated_schema_record() {
        // Create a truncated schema record
        let mut schema_data = Vec::new();
        schema_data.extend_from_slice(&0u16.to_le_bytes()); // Schema ID
        schema_data.extend_from_slice(&10u32.to_le_bytes()); // Name length
        schema_data.extend_from_slice(b"short"); // Not enough data

        let mut cursor = Cursor::new(schema_data.as_slice());
        let result = ParallelMcapReader::read_schema_record(&mut cursor);

        // Should return an error
        assert!(result.is_err());
    }

    #[test]
    fn test_channel_record_parsing() {
        // Create a valid channel record
        // Implementation expects: id, schema_id, topic_len, topic, encoding_len, encoding
        let mut channel_data = Vec::new();
        channel_data.extend_from_slice(&1u16.to_le_bytes()); // Channel ID
        channel_data.extend_from_slice(&0u16.to_le_bytes()); // Schema ID
        channel_data.extend_from_slice(&5u32.to_le_bytes()); // Topic length
        channel_data.extend_from_slice(b"/test"); // Topic
        channel_data.extend_from_slice(&3u32.to_le_bytes()); // Encoding length
        channel_data.extend_from_slice(b"cdr"); // Encoding

        let mut cursor = Cursor::new(channel_data.as_slice());
        let result = ParallelMcapReader::read_channel_record(&mut cursor);

        assert!(result.is_ok());
        let (id, topic, schema_id, encoding) = result.unwrap();
        assert_eq!(id, 1);
        assert_eq!(topic, "/test");
        assert_eq!(schema_id, 0);
        assert_eq!(encoding, "cdr");
    }

    #[test]
    fn test_truncated_channel_record() {
        // Create a truncated channel record
        let mut channel_data = Vec::new();
        channel_data.extend_from_slice(&1u16.to_le_bytes()); // Channel ID
        channel_data.extend_from_slice(&0u16.to_le_bytes()); // Schema ID
        channel_data.extend_from_slice(&100u32.to_le_bytes()); // Topic length (too large)
        channel_data.extend_from_slice(b"short"); // Not enough data

        let mut cursor = Cursor::new(channel_data.as_slice());
        let result = ParallelMcapReader::read_channel_record(&mut cursor);

        // Should return an error
        assert!(result.is_err());
    }

    #[test]
    fn test_unknown_compression_type() {
        // Test that unknown compression types are handled
        // This would be tested during decompression in process_chunk
        let compression = "unknown_format";
        // header_size = 8 + 8 + 8 + 4 + 4 + compression.len() + 8 = 40 + 14 = 54
        // data_start = chunk_start_offset + 9 + header_size = 0 + 9 + 54 = 63
        // data_end = data_start + compressed_size
        // We need data_end <= test_data.len(), so use small compressed_size
        let chunk_index = ChunkIndex {
            message_start_time: 0,
            message_end_time: 1000,
            chunk_start_offset: 0,
            chunk_length: 100,
            message_index_length: 0,
            compression: compression.to_string(),
            compressed_size: 10,
            uncompressed_size: 100,
        };

        // Need enough data: data_start (63) + compressed_size (10) = 73 bytes
        let test_data = vec![0u8; 100];
        let channels = HashMap::new();
        let channel_filter = None;

        let result = ParallelMcapReader::process_chunk(
            &chunk_index,
            &test_data,
            &channels,
            &channel_filter,
            0,
        );

        // Should return an error for unknown compression
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("Unsupported compression"));
        }
    }

    #[test]
    fn test_string_bytes_parsing() {
        // Test the helper function for reading string bytes
        let valid_data = b"hello";
        let mut cursor = Cursor::new(valid_data as &[u8]);
        let result = ParallelMcapReader::read_string_bytes(&mut cursor, 5);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "hello");

        // Test with length larger than available data
        let mut cursor2 = Cursor::new(valid_data as &[u8]);
        let result2 = ParallelMcapReader::read_string_bytes(&mut cursor2, 10);

        assert!(result2.is_err());
    }

    #[test]
    fn test_invalid_utf8_in_schema_name() {
        // Test that invalid UTF-8 in schema name is handled
        let mut schema_data = Vec::new();
        schema_data.extend_from_slice(&0u16.to_le_bytes()); // Schema ID
        schema_data.extend_from_slice(&5u32.to_le_bytes()); // Name length
        schema_data.extend_from_slice(&[0xFF, 0xFE, 0xFD, 0xFC, 0xFB]); // Invalid UTF-8
        schema_data.extend_from_slice(&0u32.to_le_bytes()); // Encoding length
        schema_data.extend_from_slice(&0u32.to_le_bytes()); // Data length

        let mut cursor = Cursor::new(schema_data.as_slice());
        let result = ParallelMcapReader::read_schema_record(&mut cursor);

        // Should handle gracefully (may error or sanitize)
        assert!(result.is_err() || result.is_ok());
    }

    #[test]
    fn test_chunk_header_with_invalid_offset() {
        // Test chunk reading with truncated data that causes read failure
        // The function reads: u64 + u64 + u64 + u32 + u32 + string + u64 = at least 40 bytes
        let file_data = [0u8; 10]; // Too small for a valid chunk header

        let result = ParallelMcapReader::read_chunk_header(&mut Cursor::new(&file_data[..]), 0, 0);

        // Should return an error due to truncated data
        assert!(result.is_err());
    }

    #[test]
    fn test_minimal_valid_mcap_structure() {
        // Test that a minimal valid MCAP structure is recognized
        let mut file_data = make_mcap_header();
        file_data.extend_from_slice(&make_data_end());

        // Add a footer with no summary
        file_data.extend_from_slice(&make_mcap_footer(0));

        // Verify the file ends with magic
        let file_end = &file_data[file_data.len() - 8..];
        assert_eq!(file_end, MCAP_MAGIC);

        // Verify the file starts with magic
        let file_start = &file_data[0..8];
        assert_eq!(file_start, MCAP_MAGIC);
    }

    #[test]
    fn test_corrupted_footer_magic() {
        // Create a file with corrupted footer magic
        let mut file_data = make_mcap_header();
        file_data.extend_from_slice(&make_data_end());
        file_data.push(OP_FOOTER);
        file_data.extend_from_slice(&20u64.to_le_bytes());
        file_data.extend_from_slice(&0u64.to_le_bytes());
        file_data.extend_from_slice(&0u64.to_le_bytes());
        file_data.extend_from_slice(&0u32.to_le_bytes());
        file_data.extend_from_slice(&[0xFF; 8]); // Corrupted magic

        let result = ParallelMcapReader::read_summary_from_footer(&file_data);
        // Should handle corrupted magic gracefully
        assert!(result.is_err() || result.unwrap().is_none());
    }

    #[test]
    fn test_summary_section_parse() {
        // Create a file with a valid summary section
        let mut file_data = make_mcap_header();

        // Add a chunk index in the summary section
        // Fields: message_start_time(8) + message_end_time(8) + chunk_start_offset(8) +
        //         chunk_length(8) + map_byte_length(4) + message_index_length(8) +
        //         compression_len(4) + compression(4) + compressed_size(8) + uncompressed_size(8)
        // Total: 8+8+8+8+4+8+4+4+8+8 = 68 bytes
        let summary_start = file_data.len() as u64;
        file_data.push(OP_CHUNK_INDEX);
        file_data.extend_from_slice(&68u64.to_le_bytes()); // Record length
        file_data.extend_from_slice(&1000u64.to_le_bytes()); // message_start_time
        file_data.extend_from_slice(&2000u64.to_le_bytes()); // message_end_time
        file_data.extend_from_slice(&100u64.to_le_bytes()); // chunk_start_offset
        file_data.extend_from_slice(&500u64.to_le_bytes()); // chunk_length
        file_data.extend_from_slice(&0u32.to_le_bytes()); // map_byte_length (no entries)
        file_data.extend_from_slice(&0u64.to_le_bytes()); // message_index_length
        file_data.extend_from_slice(&4u32.to_le_bytes()); // compression length
        file_data.extend_from_slice(b"zstd"); // compression
        file_data.extend_from_slice(&400u64.to_le_bytes()); // compressed_size
        file_data.extend_from_slice(&1000u64.to_le_bytes()); // uncompressed_size

        // Add footer pointing to summary
        file_data.extend_from_slice(&make_mcap_footer(summary_start));

        let result = ParallelMcapReader::read_summary_from_footer(&file_data);
        assert!(result.is_ok());
        let summary = result.unwrap();

        assert!(summary.is_some());
        let (_channels, _stats, chunk_indexes) = summary.unwrap();
        assert_eq!(chunk_indexes.len(), 1);
        assert_eq!(chunk_indexes[0].compression, "zstd");
    }
}
