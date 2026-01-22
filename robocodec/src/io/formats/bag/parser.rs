// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Custom ROS1 bag parser for parallel chunk reading.
//!
//! This module implements a fast BAG parser that enables true parallel
//! chunk reading by:
//! 1. Reading the bag header to locate index section
//! 2. Reading chunk info records from index section for random access
//! 3. Enabling independent decompression and parsing per worker thread
//!
//! # BAG Format Structure (Version 2.0)
//!
//! ## File Header
//! - Magic: "#ROSBAG V2.0\n" (13 bytes)
//! - Followed by bag header record in standard record format
//!
//! ## Record Format
//! All records follow: `<header_len: u32><header><data_len: u32><data>`
//! where header contains `<field_len: u32><field_name>=<field_value>` pairs
//!
//! ## Op Codes
//! - 0x02: Message data
//! - 0x03: Bag header
//! - 0x04: Index data
//! - 0x05: Chunk
//! - 0x06: Chunk info
//! - 0x07: Connection

use std::collections::HashMap;
use std::fs::File;
use std::io::{Cursor, Read, Seek};
use std::path::Path;

use byteorder::{LittleEndian, ReadBytesExt};

use crate::{CodecError, Result};

/// BAG op codes
const OP_MSG_DATA: u8 = 0x02;
const OP_BAG_HEADER: u8 = 0x03;
#[allow(dead_code)] // Reserved for future index parsing
const OP_INDEX_DATA: u8 = 0x04;
const OP_CHUNK: u8 = 0x05;
const OP_CHUNK_INFO: u8 = 0x06;
const OP_CONNECTION: u8 = 0x07;

/// BAG file header information.
#[derive(Debug, Clone)]
pub struct BagHeader {
    /// Version string (e.g., "2.0")
    pub version: String,
    /// Position of index section in file
    pub index_pos: u64,
    /// Number of connections in the file
    pub conn_count: u32,
    /// Number of chunks in the file
    pub chunk_count: u32,
}

/// BAG chunk information for random access.
#[derive(Debug, Clone)]
pub struct BagChunkInfo {
    /// Chunk sequence number
    pub sequence: u64,
    /// Offset of chunk record in file (position of header_len)
    pub chunk_pos: u64,
    /// Start time of messages in this chunk
    pub start_time: u64,
    /// End time of messages in this chunk
    pub end_time: u64,
    /// Number of messages in this chunk
    pub message_count: u32,
    /// Compression format ("none", "bz2", "lz4")
    pub compression: String,
    /// Uncompressed data size
    pub uncompressed_size: u32,
}

/// BAG connection information.
#[derive(Debug, Clone)]
pub struct BagConnection {
    /// Connection ID
    pub conn_id: u32,
    /// Topic name
    pub topic: String,
    /// Message type
    pub message_type: String,
    /// MD5 sum of message definition
    pub md5sum: String,
    /// Message definition (IDL-like text)
    pub message_definition: String,
    /// Caller ID (publishing node)
    pub caller_id: String,
}

/// Custom BAG parser for parallel reading.
///
/// This parser enables true parallel chunk reading by:
/// 1. Reading the bag header to find the index section
/// 2. Reading chunk info records for random access
/// 3. Supporting independent decompression of chunks
pub struct BagParser {
    /// Path to the bag file
    path: String,
    /// File header information
    header: BagHeader,
    /// Chunk information for random access
    chunks: Vec<BagChunkInfo>,
    /// Connection information
    connections: HashMap<u32, BagConnection>,
    /// Memory-mapped file for random access
    mmap: memmap2::Mmap,
    /// File size
    file_size: u64,
}

/// Parsed fields from a BAG record header
#[derive(Debug, Default)]
struct RecordHeader {
    op: Option<u8>,
    conn: Option<u32>,
    time: Option<u64>,
    topic: Option<String>,
    md5sum: Option<String>,
    message_type: Option<String>,
    message_definition: Option<String>,
    callerid: Option<String>,
    latching: Option<String>,
    index_pos: Option<u64>,
    conn_count: Option<u32>,
    chunk_count: Option<u32>,
    chunk_pos: Option<u64>,
    start_time: Option<u64>,
    end_time: Option<u64>,
    compression: Option<String>,
    size: Option<u32>,
    ver: Option<u32>,
    count: Option<u32>,
}

impl BagParser {
    /// BAG magic string
    const MAGIC: &[u8] = b"#ROSBAG V";

    /// Open a BAG file and parse metadata for parallel reading.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let path_str = path_ref.to_string_lossy().to_string();

        let file = File::open(&path_str).map_err(|e| {
            CodecError::encode("BagParser::open", format!("Failed to open file: {e}"))
        })?;

        let file_size = file
            .metadata()
            .map_err(|e| {
                CodecError::encode("BagParser::open", format!("Failed to get metadata: {e}"))
            })?
            .len();

        let mmap = unsafe { memmap2::Mmap::map(&file) }.map_err(|e| {
            CodecError::encode("BagParser::open", format!("Failed to mmap file: {e}"))
        })?;

        let mut cursor = Cursor::new(&mmap[..]);

        // Parse magic and version
        let version = Self::parse_magic(&mut cursor)?;

        // Parse the bag header record
        let header = Self::parse_bag_header_record(&mut cursor, version)?;

        // Read index section for chunk info and connections
        let (chunks, connections) = if header.index_pos > 0 && header.index_pos < mmap.len() as u64
        {
            Self::parse_index_section(&mmap, &header)?
        } else {
            // No index section - scan through file for connections and chunks
            Self::scan_file_for_metadata(&mmap)?
        };

        Ok(Self {
            path: path_str,
            header,
            chunks,
            connections,
            mmap,
            file_size,
        })
    }

    /// Parse the BAG magic string and return version.
    fn parse_magic<R: Read>(reader: &mut R) -> Result<String> {
        let mut magic = [0u8; 9];
        reader.read_exact(&mut magic).map_err(|e| {
            CodecError::parse(
                "BagParser::parse_magic",
                format!("Failed to read magic: {e}"),
            )
        })?;

        if magic != Self::MAGIC {
            return Err(CodecError::parse(
                "BagParser::parse_magic",
                format!("Invalid BAG magic: {:?}", String::from_utf8_lossy(&magic)),
            ));
        }

        // Read version (e.g., "2.0\n")
        let mut version_buf = [0u8; 4];
        reader.read_exact(&mut version_buf).map_err(|e| {
            CodecError::parse(
                "BagParser::parse_magic",
                format!("Failed to read version: {e}"),
            )
        })?;

        let version = String::from_utf8_lossy(&version_buf).trim().to_string();

        Ok(version)
    }

    /// Parse the bag header record (first record after magic).
    fn parse_bag_header_record<R: Read + Seek>(
        reader: &mut R,
        version: String,
    ) -> Result<BagHeader> {
        let (header_fields, _data) = Self::read_record(reader)?;

        if header_fields.op != Some(OP_BAG_HEADER) {
            return Err(CodecError::parse(
                "BagParser::parse_bag_header",
                format!(
                    "Expected bag header record (op=0x03), got op={:?}",
                    header_fields.op
                ),
            ));
        }

        Ok(BagHeader {
            version,
            index_pos: header_fields.index_pos.unwrap_or(0),
            conn_count: header_fields.conn_count.unwrap_or(0),
            chunk_count: header_fields.chunk_count.unwrap_or(0),
        })
    }

    /// Read a single BAG record: `<header_len: u32><header><data_len: u32><data>`
    fn read_record<R: Read>(reader: &mut R) -> Result<(RecordHeader, Vec<u8>)> {
        // Read header length
        let header_len = reader.read_u32::<LittleEndian>().map_err(|e| {
            CodecError::parse(
                "BagParser::read_record",
                format!("Failed to read header_len: {e}"),
            )
        })?;

        // Read header bytes
        let mut header_bytes = vec![0u8; header_len as usize];
        reader.read_exact(&mut header_bytes).map_err(|e| {
            CodecError::parse(
                "BagParser::read_record",
                format!("Failed to read header: {e}"),
            )
        })?;

        // Parse header fields
        let header_fields = Self::parse_record_header(&header_bytes)?;

        // Read data length
        let data_len = reader.read_u32::<LittleEndian>().map_err(|e| {
            CodecError::parse(
                "BagParser::read_record",
                format!("Failed to read data_len: {e}"),
            )
        })?;

        // Read data bytes
        let mut data = vec![0u8; data_len as usize];
        reader.read_exact(&mut data).map_err(|e| {
            CodecError::parse(
                "BagParser::read_record",
                format!("Failed to read data: {e}"),
            )
        })?;

        Ok((header_fields, data))
    }

    /// Parse header bytes into named fields.
    /// Format: sequence of `<field_len: u32><field_name>=<field_value>`
    fn parse_record_header(header_bytes: &[u8]) -> Result<RecordHeader> {
        let mut cursor = Cursor::new(header_bytes);
        let mut fields = RecordHeader::default();

        while (cursor.position() as usize) < header_bytes.len() {
            // Read field length
            let field_len = match cursor.read_u32::<LittleEndian>() {
                Ok(len) => len as usize,
                Err(_) => break,
            };

            if field_len == 0 {
                continue;
            }

            // Read field bytes
            let mut field_bytes = vec![0u8; field_len];
            if cursor.read_exact(&mut field_bytes).is_err() {
                break;
            }

            // Find the '=' separator
            if let Some(eq_pos) = field_bytes.iter().position(|&b| b == b'=') {
                let name = &field_bytes[..eq_pos];
                let value = &field_bytes[eq_pos + 1..];

                Self::parse_field(&mut fields, name, value);
            }
        }

        Ok(fields)
    }

    /// Parse a single field from name and value bytes.
    fn parse_field(fields: &mut RecordHeader, name: &[u8], value: &[u8]) {
        match name {
            b"op" if value.len() == 1 => {
                fields.op = Some(value[0]);
            }
            b"conn" if value.len() >= 4 => {
                fields.conn = Some(u32::from_le_bytes([value[0], value[1], value[2], value[3]]));
            }
            b"time" if value.len() >= 8 => {
                // ROS time: sec (4 bytes) + nsec (4 bytes)
                let sec = u32::from_le_bytes([value[0], value[1], value[2], value[3]]) as u64;
                let nsec = u32::from_le_bytes([value[4], value[5], value[6], value[7]]) as u64;
                fields.time = Some(sec * 1_000_000_000 + nsec);
            }
            b"topic" => {
                fields.topic = Some(String::from_utf8_lossy(value).to_string());
            }
            b"md5sum" => {
                fields.md5sum = Some(String::from_utf8_lossy(value).to_string());
            }
            b"type" => {
                fields.message_type = Some(String::from_utf8_lossy(value).to_string());
            }
            b"message_definition" => {
                fields.message_definition = Some(String::from_utf8_lossy(value).to_string());
            }
            b"callerid" => {
                fields.callerid = Some(String::from_utf8_lossy(value).to_string());
            }
            b"latching" => {
                fields.latching = Some(String::from_utf8_lossy(value).to_string());
            }
            b"index_pos" if value.len() >= 8 => {
                fields.index_pos = Some(u64::from_le_bytes([
                    value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
                ]));
            }
            b"conn_count" if value.len() >= 4 => {
                fields.conn_count =
                    Some(u32::from_le_bytes([value[0], value[1], value[2], value[3]]));
            }
            b"chunk_count" if value.len() >= 4 => {
                fields.chunk_count =
                    Some(u32::from_le_bytes([value[0], value[1], value[2], value[3]]));
            }
            b"chunk_pos" if value.len() >= 8 => {
                fields.chunk_pos = Some(u64::from_le_bytes([
                    value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
                ]));
            }
            b"start_time" if value.len() >= 8 => {
                let sec = u32::from_le_bytes([value[0], value[1], value[2], value[3]]) as u64;
                let nsec = u32::from_le_bytes([value[4], value[5], value[6], value[7]]) as u64;
                fields.start_time = Some(sec * 1_000_000_000 + nsec);
            }
            b"end_time" if value.len() >= 8 => {
                let sec = u32::from_le_bytes([value[0], value[1], value[2], value[3]]) as u64;
                let nsec = u32::from_le_bytes([value[4], value[5], value[6], value[7]]) as u64;
                fields.end_time = Some(sec * 1_000_000_000 + nsec);
            }
            b"compression" => {
                fields.compression = Some(String::from_utf8_lossy(value).to_string());
            }
            b"size" if value.len() >= 4 => {
                fields.size = Some(u32::from_le_bytes([value[0], value[1], value[2], value[3]]));
            }
            b"ver" if value.len() >= 4 => {
                fields.ver = Some(u32::from_le_bytes([value[0], value[1], value[2], value[3]]));
            }
            b"count" if value.len() >= 4 => {
                fields.count = Some(u32::from_le_bytes([value[0], value[1], value[2], value[3]]));
            }
            _ => {
                // Ignore unknown fields
            }
        }
    }

    /// Parse the index section to get chunk info and connections.
    fn parse_index_section(
        mmap: &[u8],
        header: &BagHeader,
    ) -> Result<(Vec<BagChunkInfo>, HashMap<u32, BagConnection>)> {
        let mut cursor = Cursor::new(mmap);
        cursor.set_position(header.index_pos);

        let mut chunks = Vec::new();
        let mut connections = HashMap::new();
        let mut chunk_sequence: u64 = 0;

        while (cursor.position() as usize) < mmap.len() {
            let (header_fields, data) = match Self::read_record(&mut cursor) {
                Ok(r) => r,
                Err(_) => break,
            };

            match header_fields.op {
                Some(OP_CONNECTION) => {
                    // Connection data section also contains field=value pairs
                    let data_fields = Self::parse_record_header(&data).unwrap_or_default();
                    if let Some(conn) = Self::connection_from_fields(&header_fields, &data_fields) {
                        connections.insert(conn.conn_id, conn);
                    }
                }
                Some(OP_CHUNK_INFO) => {
                    if let Some(chunk_info) =
                        Self::chunk_info_from_fields(&header_fields, &data, chunk_sequence)
                    {
                        chunks.push(chunk_info);
                        chunk_sequence += 1;
                    }
                }
                _ => {
                    // Ignore other record types in index section
                }
            }
        }

        Ok((chunks, connections))
    }

    /// Create a BagConnection from parsed header and data fields.
    fn connection_from_fields(
        header_fields: &RecordHeader,
        data_fields: &RecordHeader,
    ) -> Option<BagConnection> {
        Some(BagConnection {
            conn_id: header_fields.conn?,
            topic: header_fields.topic.clone()?,
            // type, md5sum, message_definition come from the data section
            message_type: data_fields.message_type.clone()?,
            md5sum: data_fields.md5sum.clone().unwrap_or_default(),
            message_definition: data_fields.message_definition.clone().unwrap_or_default(),
            caller_id: data_fields.callerid.clone().unwrap_or_default(),
        })
    }

    /// Create a BagChunkInfo from parsed header fields and data.
    fn chunk_info_from_fields(
        fields: &RecordHeader,
        data: &[u8],
        sequence: u64,
    ) -> Option<BagChunkInfo> {
        // Parse message count from data section
        // Format: ver (u32), then for each connection: conn (u32), count (u32)
        let mut message_count: u32 = 0;

        if data.len() >= 4 {
            let mut cursor = Cursor::new(data);
            // Skip version
            let _ = cursor.read_u32::<LittleEndian>();

            // Read connection counts
            while (cursor.position() as usize) + 8 <= data.len() {
                let _ = cursor.read_u32::<LittleEndian>(); // conn_id
                if let Ok(count) = cursor.read_u32::<LittleEndian>() {
                    message_count = message_count.saturating_add(count);
                }
            }
        }

        Some(BagChunkInfo {
            sequence,
            chunk_pos: fields.chunk_pos?,
            start_time: fields.start_time.unwrap_or(0),
            end_time: fields.end_time.unwrap_or(0),
            message_count,
            compression: String::new(), // Will be read from chunk header
            uncompressed_size: 0,       // Will be read from chunk header
        })
    }

    /// Scan file for metadata when no index section is available.
    fn scan_file_for_metadata(
        mmap: &[u8],
    ) -> Result<(Vec<BagChunkInfo>, HashMap<u32, BagConnection>)> {
        let mut cursor = Cursor::new(mmap);

        // Skip magic (13 bytes: "#ROSBAG V2.0\n")
        cursor.set_position(13);

        // Skip bag header record
        let _ = Self::read_record(&mut cursor)?;

        let mut chunks = Vec::new();
        let mut connections = HashMap::new();
        let mut chunk_sequence: u64 = 0;

        while (cursor.position() as usize) < mmap.len() {
            let record_start = cursor.position();

            let (header_fields, data) = match Self::read_record(&mut cursor) {
                Ok(r) => r,
                Err(_) => break,
            };

            match header_fields.op {
                Some(OP_CONNECTION) => {
                    // Connection data section also contains field=value pairs
                    let data_fields = Self::parse_record_header(&data).unwrap_or_default();
                    if let Some(conn) = Self::connection_from_fields(&header_fields, &data_fields) {
                        connections.insert(conn.conn_id, conn);
                    }
                }
                Some(OP_CHUNK) => {
                    // Record chunk info from the chunk header
                    // When scanning without index, we don't know message count upfront
                    chunks.push(BagChunkInfo {
                        sequence: chunk_sequence,
                        chunk_pos: record_start,
                        start_time: 0,
                        end_time: 0,
                        message_count: 0, // Unknown without index
                        compression: header_fields
                            .compression
                            .clone()
                            .unwrap_or_else(|| "none".to_string()),
                        uncompressed_size: header_fields.size.unwrap_or(data.len() as u32),
                    });
                    chunk_sequence += 1;
                }
                _ => {}
            }
        }

        Ok((chunks, connections))
    }

    /// Get chunk information for random access.
    pub fn chunks(&self) -> &[BagChunkInfo] {
        &self.chunks
    }

    /// Get connections.
    pub fn connections(&self) -> &HashMap<u32, BagConnection> {
        &self.connections
    }

    /// Get the file size.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Get the file path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Get header info.
    pub fn header(&self) -> &BagHeader {
        &self.header
    }

    /// Read and decompress a single chunk.
    pub fn read_chunk(&self, chunk_info: &BagChunkInfo) -> Result<Vec<u8>> {
        let mut cursor = Cursor::new(&self.mmap[..]);
        cursor.set_position(chunk_info.chunk_pos);

        let (header_fields, data) = Self::read_record(&mut cursor)?;

        if header_fields.op != Some(OP_CHUNK) {
            return Err(CodecError::parse(
                "BagParser::read_chunk",
                format!(
                    "Expected chunk record (op=0x05), got op={:?}",
                    header_fields.op
                ),
            ));
        }

        let compression = header_fields.compression.as_deref().unwrap_or("none");

        match compression {
            "none" => Ok(data),
            "bz2" => {
                use bzip2::read::BzDecoder;
                let mut decoder = BzDecoder::new(&data[..]);
                let mut decompressed = Vec::new();
                if let Some(size) = header_fields.size {
                    decompressed.reserve(size as usize);
                }
                decoder.read_to_end(&mut decompressed).map_err(|e| {
                    CodecError::encode(
                        "BagParser::read_chunk",
                        format!("BZ2 decompression failed: {e}"),
                    )
                })?;
                Ok(decompressed)
            }
            "lz4" => {
                use lz4_flex::decompress_size_prepended;
                // LZ4 in rosbag has a size prepended
                let decompressed = decompress_size_prepended(&data).map_err(|e| {
                    CodecError::encode(
                        "BagParser::read_chunk",
                        format!("LZ4 decompression failed: {e}"),
                    )
                })?;
                Ok(decompressed)
            }
            _ => Err(CodecError::unsupported(format!(
                "Unsupported compression format: {}",
                compression
            ))),
        }
    }

    /// Parse message data records from decompressed chunk data.
    pub fn parse_chunk_messages(
        &self,
        decompressed_data: &[u8],
        conn_id_map: &HashMap<u32, u16>,
    ) -> Result<Vec<BagMessageData>> {
        let mut cursor = Cursor::new(decompressed_data);
        let mut messages = Vec::new();

        while (cursor.position() as usize) < decompressed_data.len() {
            let (header_fields, data) = match Self::read_record(&mut cursor) {
                Ok(r) => r,
                Err(_) => break,
            };

            match header_fields.op {
                Some(OP_MSG_DATA) => {
                    let conn_id = match header_fields.conn {
                        Some(id) => id,
                        None => continue,
                    };

                    // Map connection ID to channel ID
                    let channel_id = match conn_id_map.get(&conn_id) {
                        Some(&id) => id,
                        None => continue,
                    };

                    let time = header_fields.time.unwrap_or(0);

                    messages.push(BagMessageData {
                        channel_id,
                        log_time: time,
                        publish_time: time,
                        sequence: 0,
                        data,
                    });
                }
                Some(OP_CONNECTION) => {
                    // Skip connection records inside chunks
                }
                _ => {
                    // Skip other record types
                }
            }
        }

        Ok(messages)
    }
}

/// Raw message data extracted from a BAG chunk.
#[derive(Debug)]
pub struct BagMessageData {
    /// Channel ID (internal, after mapping)
    pub channel_id: u16,
    /// Log timestamp
    pub log_time: u64,
    /// Publish timestamp
    pub publish_time: u64,
    /// Sequence number (0 for BAG)
    pub sequence: u32,
    /// Message data (raw CDR bytes)
    pub data: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_record_header() {
        // Build a header with op=0x02 and conn=1
        let mut header_bytes = Vec::new();
        // Field 1: op=0x02
        header_bytes.extend(&3u32.to_le_bytes()); // field_len = 3 ("op" + "=" + value)
        header_bytes.extend(b"op=");
        header_bytes.push(0x02);
        // Note: We've already consumed 4 bytes for field_len, and 3 bytes for "op=\x02"
        // but field_len=3, so we need to adjust. Let's rebuild:

        let mut header_bytes = Vec::new();
        // Field 1: op=\x02 (field_len = 4: "op" + "=" + 1 byte value)
        header_bytes.extend(&4u32.to_le_bytes()); // field_len
        header_bytes.extend(b"op=\x02");

        // Field 2: conn=\x01\x00\x00\x00 (field_len = 9: "conn" + "=" + 4 bytes)
        header_bytes.extend(&9u32.to_le_bytes()); // field_len
        header_bytes.extend(b"conn=");
        header_bytes.extend(&1u32.to_le_bytes());

        let fields = BagParser::parse_record_header(&header_bytes).unwrap();
        assert_eq!(fields.op, Some(0x02));
        assert_eq!(fields.conn, Some(1));
    }

    #[test]
    fn test_parse_time_field() {
        let mut header_bytes = Vec::new();
        // time field: sec=1234567890, nsec=123456789
        // field_len = 4 ("time") + 1 ("=") + 8 (sec + nsec) = 13
        header_bytes.extend(&13u32.to_le_bytes());
        header_bytes.extend(b"time=");
        header_bytes.extend(&1234567890u32.to_le_bytes()); // sec
        header_bytes.extend(&123456789u32.to_le_bytes()); // nsec

        let fields = BagParser::parse_record_header(&header_bytes).unwrap();
        let expected_time = 1234567890u64 * 1_000_000_000 + 123456789u64;
        assert_eq!(fields.time, Some(expected_time));
    }
}
