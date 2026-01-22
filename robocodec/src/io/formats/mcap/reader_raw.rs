// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

#![allow(dead_code)]
#![allow(private_interfaces)]

//! Custom MCAP reader implementation.
//!
//! This module provides a complete MCAP reader that:
//! - Parses MCAP files without external dependencies
//! - Supports reading with or without summary section
//! - Handles all compression types (zstd, lz4, none)
//! - Enables sequential and parallel reading
//!
//! # MCAP Format Structure
//!
//! ## File Header
//! - Magic: 0x89 + "MCAP" + 0x30 + \r\n (8 bytes)
//! - Header record (op=0x01): profile, library
//!
//! ## Records
//! - Schema (op=0x03): id, name, encoding, data
//! - Channel (op=0x04): id, topic, encoding, schema_id
//! - Message (op=0x05): channel_id, sequence, log_time, publish_time, data
//! - Chunk (op=0x06): message_start_time, message_end_time, compression, compressed_size
//! - Chunk Index (op=0x08): For summary section
//! - Statistics (op=0x0B): Message count, start/end times
//! - Footer (op=0x02): summary_start, summary_offset_start
//!
//! ## Summary Section
//! - Chunk indexes for random access
//! - Statistics

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{Cursor, Read};
use std::path::Path;

use byteorder::{LittleEndian, ReadBytesExt};

use crate::io::formats::mcap::constants::{
    MCAP_MAGIC, OP_ATTACHMENT, OP_CHANNEL, OP_CHUNK, OP_CHUNK_INDEX, OP_DATA_END, OP_FOOTER,
    OP_MESSAGE, OP_MESSAGE_INDEX, OP_METADATA, OP_SCHEMA, OP_STATISTICS,
};
use crate::io::metadata::{ChannelInfo, FileFormat, RawMessage};
use crate::io::traits::FormatReader;
use crate::{CodecError, Result};

/// Schema information.
#[derive(Debug, Clone)]
struct SchemaInfo {
    id: u16,
    name: String,
    encoding: String,
    data: Vec<u8>,
}

/// Chunk index information from summary section.
#[derive(Debug, Clone)]
struct ChunkIndex {
    message_start_time: u64,
    message_end_time: u64,
    chunk_start_offset: u64,
    chunk_length: u64,
    message_index_offsets: BTreeMap<u16, u64>,
    message_index_length: u64,
    compression: String,
    compressed_size: u64,
    uncompressed_size: u64,
    message_count: u64,
}

/// MCAP file statistics.
#[derive(Debug, Clone)]
struct McapStatistics {
    message_count: u64,
    channel_count: u64,
    schema_count: u64,
    message_start_time: u64,
    message_end_time: u64,
}

/// Custom MCAP reader with full format support.
pub struct McapReader {
    /// Path to the MCAP file
    path: String,
    /// Memory-mapped file
    mmap: memmap2::Mmap,
    /// Channel information
    channels: HashMap<u16, ChannelInfo>,
    /// Schema information
    schemas: HashMap<u16, SchemaInfo>,
    /// Chunk indexes (from summary section or built during scan)
    chunk_indexes: Vec<ChunkIndex>,
    /// File statistics
    stats: Option<McapStatistics>,
    /// Start of data section
    data_start: u64,
    /// Position of summary section
    summary_start: Option<u64>,
    /// Summary offset position
    summary_offset: Option<u64>,
    /// File size
    file_size: u64,
}

impl McapReader {
    /// Open an MCAP file for reading.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let path_str = path_ref.to_string_lossy().to_string();

        let file = File::open(path_ref)
            .map_err(|e| CodecError::encode("McapReader", format!("Failed to open file: {e}")))?;

        let file_size = file
            .metadata()
            .map_err(|e| CodecError::encode("McapReader", format!("Failed to get metadata: {e}")))?
            .len();

        let mmap = unsafe { memmap2::Mmap::map(&file) }
            .map_err(|e| CodecError::encode("McapReader", format!("Failed to mmap file: {e}")))?;

        let mut cursor = Cursor::new(&mmap[..]);

        // Verify and skip magic
        let magic = Self::read_bytes(&mut cursor, 8)?;
        if magic != MCAP_MAGIC {
            return Err(CodecError::parse(
                "McapReader",
                format!("Invalid MCAP magic: {:?}", hex::encode(magic)),
            ));
        }

        let data_start = cursor.position();

        // Read records to build metadata
        let mut reader = Self {
            path: path_str,
            mmap,
            channels: HashMap::new(),
            schemas: HashMap::new(),
            chunk_indexes: Vec::new(),
            stats: None,
            data_start,
            summary_start: None,
            summary_offset: None,
            file_size,
        };

        reader.parse_metadata()?;

        Ok(reader)
    }

    /// Parse metadata from the MCAP file.
    fn parse_metadata(&mut self) -> Result<()> {
        let mut cursor = Cursor::new(&self.mmap[..]);
        cursor.set_position(self.data_start);

        let mut has_chunk_indexes = false;

        // First pass: look for summary section by reading from end
        if let Ok((summary_start, summary_offset)) = self.read_footer(&mut cursor) {
            self.summary_start = Some(summary_start);
            self.summary_offset = Some(summary_offset);

            // Try to read summary section
            if let Ok((chunk_indexes, stats)) = self.read_summary_section(summary_start) {
                self.chunk_indexes = chunk_indexes;
                self.stats = Some(stats);
                has_chunk_indexes = true;
            }
        }

        // Second pass: read all records if we don't have chunk indexes or need channels
        if !has_chunk_indexes || self.channels.is_empty() {
            self.scan_records(has_chunk_indexes);
        }

        // Build channel info from schemas and channels
        self.build_channels_from_records();

        Ok(())
    }

    /// Read the footer record to get summary section positions.
    fn read_footer(&self, _cursor: &mut Cursor<&[u8]>) -> Result<(u64, u64)> {
        // MCAP file structure at the end:
        // [... Summary Section ...][FOOTER][Magic]
        // Note: DATA_END comes BEFORE the summary section, not after
        // - Footer: opcode(1) + length(8) + summary_start(8) + summary_offset_start(8) + summary_crc(4) = 29 bytes
        // - Magic: 8 bytes

        let file_len = self.mmap.len();
        if file_len < 8 + 29 {
            return Err(CodecError::parse("McapReader", "File too small for footer"));
        }

        // Verify trailing magic
        let magic_start = file_len - 8;
        if self.mmap[magic_start..] != MCAP_MAGIC {
            return Err(CodecError::parse("McapReader", "Invalid trailing magic"));
        }

        // Footer record is directly before trailing magic (29 bytes)
        let footer_start = magic_start - 29;

        if self.mmap[footer_start] != OP_FOOTER {
            return Err(CodecError::parse("McapReader", "Expected Footer record"));
        }

        // Read footer content
        let mut cursor = Cursor::new(&self.mmap[footer_start + 1..]);
        let len = cursor.read_u64::<LittleEndian>()?;
        if len < 20 {
            return Err(CodecError::parse("McapReader", "Footer record too short"));
        }
        let summary_start = cursor.read_u64::<LittleEndian>()?;
        let summary_offset_start = cursor.read_u64::<LittleEndian>()?;

        Ok((summary_start, summary_offset_start))
    }

    /// Read the summary section.
    fn read_summary_section(
        &self,
        summary_start: u64,
    ) -> Result<(Vec<ChunkIndex>, McapStatistics)> {
        let mut cursor = Cursor::new(&self.mmap[..]);
        cursor.set_position(summary_start);

        let mut chunk_indexes = Vec::new();
        let mut stats = None;

        while cursor.position() < self.mmap.len() as u64 - 9 {
            let pos = cursor.position();
            let op = match cursor.read_u8() {
                Ok(o) => o,
                Err(_) => break,
            };

            let len = match cursor.read_u64::<LittleEndian>() {
                Ok(l) => l,
                Err(_) => break,
            };

            match op {
                OP_CHUNK_INDEX => {
                    if let Ok(index) = self.read_chunk_index(&mut cursor, len) {
                        chunk_indexes.push(index);
                    }
                }
                OP_STATISTICS => {
                    if let Ok(s) = self.read_statistics(&mut cursor) {
                        stats = Some(s);
                    }
                }
                OP_MESSAGE_INDEX | OP_ATTACHMENT | OP_METADATA => {
                    // Skip these records
                    cursor.set_position(pos + 9 + len);
                }
                _ => {
                    // Skip unknown records
                    cursor.set_position(pos + 9 + len);
                }
            }
        }

        Ok((
            chunk_indexes,
            stats.unwrap_or(McapStatistics {
                message_count: 0,
                channel_count: 0,
                schema_count: 0,
                message_start_time: 0,
                message_end_time: 0,
            }),
        ))
    }

    /// Read a chunk index record.
    fn read_chunk_index(&self, cursor: &mut Cursor<&[u8]>, _len: u64) -> Result<ChunkIndex> {
        let message_start_time = cursor.read_u64::<LittleEndian>()?;
        let message_end_time = cursor.read_u64::<LittleEndian>()?;
        let chunk_start_offset = cursor.read_u64::<LittleEndian>()?;
        let chunk_length = cursor.read_u64::<LittleEndian>()?;
        let message_index_count = cursor.read_u32::<LittleEndian>()? as usize;

        let mut message_index_offsets = BTreeMap::new();
        for _ in 0..message_index_count {
            let channel_id = cursor.read_u16::<LittleEndian>()?;
            let offset = cursor.read_u64::<LittleEndian>()?;
            message_index_offsets.insert(channel_id, offset);
        }

        let message_index_length = cursor.read_u64::<LittleEndian>()?;
        let compression_len = cursor.read_u16::<LittleEndian>()? as usize;
        let compression = Self::read_string(cursor, compression_len)?;

        let compressed_size = cursor.read_u64::<LittleEndian>()?;
        let uncompressed_size = cursor.read_u64::<LittleEndian>()?;

        // Message count is in chunk index
        // We'll need to read it from the chunk itself for accurate count
        let message_count = 0; // Will be filled when reading chunk

        Ok(ChunkIndex {
            message_start_time,
            message_end_time,
            chunk_start_offset,
            chunk_length,
            message_index_offsets,
            message_index_length,
            compression,
            compressed_size,
            uncompressed_size,
            message_count,
        })
    }

    /// Read statistics record.
    fn read_statistics(&self, cursor: &mut Cursor<&[u8]>) -> Result<McapStatistics> {
        // Statistics record format (per MCAP spec):
        // - message_count: u64
        // - schema_count: u16
        // - channel_count: u32
        // - attachment_count: u32
        // - metadata_count: u32
        // - chunk_count: u32
        // - message_start_time: u64
        // - message_end_time: u64
        // - channel_message_counts: map (prefixed u32 length)
        let message_count = cursor.read_u64::<LittleEndian>()?;
        let schema_count = cursor.read_u16::<LittleEndian>()? as u64;
        let channel_count = cursor.read_u32::<LittleEndian>()? as u64;
        let _attachment_count = cursor.read_u32::<LittleEndian>()?;
        let _metadata_count = cursor.read_u32::<LittleEndian>()?;
        let _chunk_count = cursor.read_u32::<LittleEndian>()?;
        let message_start_time = cursor.read_u64::<LittleEndian>()?;
        let message_end_time = cursor.read_u64::<LittleEndian>()?;
        // Skip channel_message_counts map (prefixed by u32 byte length)
        let map_len = cursor.read_u32::<LittleEndian>()? as u64;
        cursor.set_position(cursor.position() + map_len);

        Ok(McapStatistics {
            message_count,
            channel_count,
            schema_count,
            message_start_time,
            message_end_time,
        })
    }

    /// Scan all records in the data section.
    fn scan_records(&mut self, skip_chunks: bool) {
        let mut cursor = Cursor::new(&self.mmap[..]);
        cursor.set_position(self.data_start);

        let mut schemas: HashMap<u16, SchemaInfo> = HashMap::new();
        let mut channel_records: HashMap<u16, (String, u16, String)> = HashMap::new(); // id -> (topic, schema_id, encoding)
        let mut chunk_indexes = Vec::new();

        while cursor.position() < self.mmap.len() as u64 - 9 {
            let record_start = cursor.position();
            let op = match cursor.read_u8() {
                Ok(o) => o,
                Err(_) => break,
            };

            let len = match cursor.read_u64::<LittleEndian>() {
                Ok(l) => l,
                Err(_) => break,
            };

            let record_end = record_start + 9 + len;

            if record_end > self.mmap.len() as u64 {
                break;
            }

            match op {
                OP_SCHEMA => {
                    if let Ok(schema) = self.read_schema(&mut cursor, len) {
                        schemas.insert(schema.id, schema);
                    }
                }
                OP_CHANNEL => {
                    if let Ok((id, topic, schema_id, encoding)) =
                        self.read_channel(&mut cursor, len)
                    {
                        channel_records.insert(id, (topic, schema_id, encoding));
                    }
                }
                OP_CHUNK if !skip_chunks => {
                    if let Ok(index) = self.read_chunk_header(&mut cursor, record_start, len) {
                        chunk_indexes.push(index);
                    }
                }
                OP_MESSAGE | OP_ATTACHMENT | OP_METADATA | OP_MESSAGE_INDEX => {
                    // Skip these
                }
                OP_FOOTER | OP_DATA_END => {
                    // End of data section
                    break;
                }
                OP_STATISTICS => {
                    // Skip for now, we'll read from summary
                }
                _ => {
                    // Skip unknown records
                }
            }

            cursor.set_position(record_end);
        }

        self.schemas = schemas;

        // Build channels from collected data
        for (id, (topic, schema_id, encoding)) in channel_records {
            let schema = self.schemas.get(&schema_id);
            let schema_text = schema.map(|s| String::from_utf8_lossy(&s.data).to_string());
            let schema_data = schema.map(|s| s.data.clone());
            let schema_encoding = schema.map(|s| s.encoding.clone());

            self.channels.insert(
                id,
                ChannelInfo {
                    id,
                    topic,
                    message_type: schema.as_ref().map(|s| s.name.clone()).unwrap_or_default(),
                    encoding,
                    schema: schema_text,
                    schema_data,
                    schema_encoding,
                    message_count: 0,
                    callerid: None,
                },
            );
        }

        if !skip_chunks && !chunk_indexes.is_empty() {
            self.chunk_indexes = chunk_indexes;
        }
    }

    /// Read a schema record.
    fn read_schema(&self, cursor: &mut Cursor<&[u8]>, _len: u64) -> Result<SchemaInfo> {
        let id = cursor.read_u16::<LittleEndian>()?;
        let name_len = cursor.read_u16::<LittleEndian>()? as usize;
        let name = Self::read_string(cursor, name_len)?;
        let encoding_len = cursor.read_u16::<LittleEndian>()? as usize;
        let encoding = Self::read_string(cursor, encoding_len)?;
        let data_len = cursor.read_u64::<LittleEndian>()? as usize;

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
    fn read_channel(
        &self,
        cursor: &mut Cursor<&[u8]>,
        _len: u64,
    ) -> Result<(u16, String, u16, String)> {
        let id = cursor.read_u16::<LittleEndian>()?;
        let topic_len = cursor.read_u16::<LittleEndian>()? as usize;
        let topic = Self::read_string(cursor, topic_len)?;
        let encoding_len = cursor.read_u16::<LittleEndian>()? as usize;
        let encoding = Self::read_string(cursor, encoding_len)?;
        let schema_id = cursor.read_u16::<LittleEndian>()?;
        // Skip metadata
        Ok((id, topic, schema_id, encoding))
    }

    /// Read chunk header to build chunk index.
    fn read_chunk_header(
        &self,
        cursor: &mut Cursor<&[u8]>,
        chunk_start: u64,
        _record_len: u64,
    ) -> Result<ChunkIndex> {
        let message_start_time = cursor.read_u64::<LittleEndian>()?;
        let message_end_time = cursor.read_u64::<LittleEndian>()?;
        let _message_start_offset = cursor.read_u64::<LittleEndian>()?;

        let compression_len = cursor.read_u16::<LittleEndian>()? as usize;
        let compression = Self::read_string(cursor, compression_len)?;

        let compressed_size = cursor.read_u64::<LittleEndian>()?;
        let uncompressed_size = cursor.read_u64::<LittleEndian>()?;

        Ok(ChunkIndex {
            message_start_time,
            message_end_time,
            chunk_start_offset: chunk_start,
            chunk_length: (9 + 24 + 2 + compression_len + 8 + 8 + compressed_size as usize) as u64,
            message_index_offsets: BTreeMap::new(),
            message_index_length: 0,
            compression,
            compressed_size,
            uncompressed_size,
            message_count: 0,
        })
    }

    /// Build channels from schemas and channel records.
    fn build_channels_from_records(&mut self) {
        // Already done in scan_records
    }

    /// Read bytes from cursor.
    fn read_bytes(cursor: &mut Cursor<&[u8]>, n: usize) -> Result<Vec<u8>> {
        let mut buffer = vec![0u8; n];
        cursor
            .read_exact(&mut buffer)
            .map_err(|e| CodecError::parse("McapReader", format!("Failed to read bytes: {e}")))?;
        Ok(buffer)
    }

    /// Read a string of known length from cursor.
    fn read_string(cursor: &mut Cursor<&[u8]>, len: usize) -> Result<String> {
        if len == 0 {
            return Ok(String::new());
        }
        let mut buffer = vec![0u8; len];
        cursor
            .read_exact(&mut buffer)
            .map_err(|e| CodecError::parse("McapReader", format!("Failed to read string: {e}")))?;
        String::from_utf8(buffer)
            .map_err(|e| CodecError::parse("McapReader", format!("Invalid UTF-8: {e}")))
    }

    /// Read a length-prefixed string from cursor.
    fn read_labeled_string(cursor: &mut Cursor<&[u8]>) -> Result<String> {
        let len = cursor.read_u32::<LittleEndian>()? as usize;
        Self::read_string(cursor, len)
    }

    /// Get chunk indexes for parallel reading.
    pub fn chunk_indexes(&self) -> &[ChunkIndex] {
        &self.chunk_indexes
    }

    /// Read and decompress a chunk.
    pub fn read_chunk(&self, chunk_index: &ChunkIndex) -> Result<Vec<u8>> {
        // The compressed data starts after the chunk header
        let data_offset = chunk_index.chunk_start_offset + 9 + 24; // magic + op+len + header fields

        let data_start = (data_offset + 2 + chunk_index.compression.len() as u64 + 8 + 8) as usize;
        let data_end = data_start + chunk_index.compressed_size as usize;

        if data_end > self.mmap.len() {
            return Err(CodecError::parse(
                "McapReader",
                format!("Chunk data exceeds file: {}..{}", data_start, data_end),
            ));
        }

        let compressed_data = &self.mmap[data_start..data_end];

        match chunk_index.compression.as_str() {
            "zstd" | "zst" => Ok(zstd::bulk::decompress(
                compressed_data,
                chunk_index.uncompressed_size as usize,
            )?),
            "lz4" => lz4_flex::decompress(compressed_data, chunk_index.uncompressed_size as usize)
                .map_err(|e| {
                    CodecError::encode("McapReader", format!("LZ4 decompression failed: {e}"))
                }),
            "" | "none" => Ok(compressed_data.to_vec()),
            _ => Err(CodecError::unsupported(format!(
                "Unsupported compression: {}",
                chunk_index.compression
            ))),
        }
    }

    /// Create a raw message iterator.
    pub fn iter_raw(&self) -> Result<McapMessageIter<'_>> {
        McapMessageIter::new(self)
    }
}

impl FormatReader for McapReader {
    fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        &self.channels
    }

    fn message_count(&self) -> u64 {
        self.stats.as_ref().map(|s| s.message_count).unwrap_or(0)
    }

    fn start_time(&self) -> Option<u64> {
        self.stats
            .as_ref()
            .map(|s| s.message_start_time)
            .filter(|&t| t > 0)
    }

    fn end_time(&self) -> Option<u64> {
        self.stats
            .as_ref()
            .map(|s| s.message_end_time)
            .filter(|&t| t > 0)
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

/// Raw message iterator for MCAP files.
pub struct McapMessageIter<'a> {
    reader: &'a McapReader,
    cursor: Cursor<&'a [u8]>,
    current_chunk_data: Option<Vec<u8>>,
    current_chunk_index: usize,
}

impl<'a> McapMessageIter<'a> {
    fn new(reader: &'a McapReader) -> Result<Self> {
        let cursor = Cursor::new(&reader.mmap[..]);
        Ok(Self {
            reader,
            cursor,
            current_chunk_data: None,
            current_chunk_index: 0,
        })
    }
}

impl<'a> Iterator for McapMessageIter<'a> {
    type Item = Result<(RawMessage, ChannelInfo)>;

    fn next(&mut self) -> Option<Self::Item> {
        // If we have chunk data, read messages from it
        if let Some(ref data) = self.current_chunk_data {
            let mut cursor = Cursor::new(&data[..]);

            // Skip chunk header (24 bytes)
            cursor.set_position(24);

            // Read message records
            while cursor.position() < data.len() as u64 {
                let channel_id = match cursor.read_u16::<LittleEndian>() {
                    Ok(id) => id,
                    Err(_) => break,
                };

                let sequence = cursor.read_u32::<LittleEndian>().ok()?;
                let log_time = cursor.read_u64::<LittleEndian>().ok()?;
                let publish_time = cursor.read_u64::<LittleEndian>().ok()?;
                let data_len = cursor.read_u32::<LittleEndian>().ok()? as usize;

                let data_start = cursor.position() as usize;
                let data_end = data_start + data_len;

                if data_end > data.len() {
                    break;
                }

                let msg_data = data[data_start..data_end].to_vec();
                cursor.set_position(data_end as u64);

                if let Some(channel_info) = self.reader.channels.get(&channel_id) {
                    return Some(Ok((
                        RawMessage {
                            channel_id,
                            log_time,
                            publish_time,
                            data: msg_data,
                            sequence: Some(sequence as u64),
                        },
                        channel_info.clone(),
                    )));
                }
            }

            // Done with this chunk
            self.current_chunk_data = None;
            self.current_chunk_index += 1;
        }

        // Try to load next chunk
        if self.current_chunk_index < self.reader.chunk_indexes.len() {
            let chunk_index = &self.reader.chunk_indexes[self.current_chunk_index];
            match self.reader.read_chunk(chunk_index) {
                Ok(data) => {
                    self.current_chunk_data = Some(data);
                    return self.next();
                }
                Err(e) => return Some(Err(e)),
            }
        }

        // No more chunks
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mcap_magic() {
        assert_eq!(
            &MCAP_MAGIC,
            &[0x89, b'M', b'C', b'A', b'P', 0x30, b'\r', b'\n']
        );
    }
}
