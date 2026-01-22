// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! ROS1 bag file writer implementation.
//!
//! This module provides functionality to write ROS1 bag files.
//! Based on the rosbag_direct_write C++ implementation.
//!
//! # ROS1 Bag Format Overview
//!
//! A ROS1 bag file has the following structure:
//! 1. Version line: `#ROSBAG V2.0\n`
//! 2. File header record (4096 bytes, padded)
//! 3. Chunks containing:
//!    - Chunk header
//!    - Connection records (metadata for each topic)
//!    - Message data records
//!    - Index data records (for each connection in the chunk)
//! 4. Connection records (summary at end)
//! 5. Chunk info records (summary at end)
//!
//! # Example
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use roboflow::format::writer::{BagWriter, BagMessage};
//!
//! let mut writer = BagWriter::create("output.bag")?;
//!
//! // Add connections (one per topic)
//! let schema = "std_msgs/String#ROS1";
//! writer.add_connection(0, "/camera/image", "sensor_msgs/Image", schema)?;
//!
//! // Write messages
//! let timestamp_ns = 1234567890;
//! let data = vec![0u8; 10];
//! writer.write_message(&BagMessage::new(0, timestamp_ns, data))?;
//!
//! // Finalize the bag file
//! writer.finish()?;
//! # Ok(())
//! # }
//! ```

use std::any::Any;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::Path;

use crate::io::metadata::RawMessage;
use crate::io::traits::FormatWriter;
use crate::{CodecError, Result};

/// ROS bag version string
const VERSION: &str = "2.0";

/// Operation codes for different record types
const OP_BAG_HEADER: u8 = 0x03;
const OP_CHUNK: u8 = 0x05;
const OP_CONNECTION: u8 = 0x07;
const OP_MSG_DATA: u8 = 0x02;
const OP_INDEX_DATA: u8 = 0x04;
const OP_CHUNK_INFO: u8 = 0x06;

/// Index data version
const INDEX_VERSION: u32 = 1;

/// Chunk info version
const CHUNK_INFO_VERSION: u32 = 1;

/// Default chunk threshold (768KB)
const DEFAULT_CHUNK_THRESHOLD: usize = 768 * 1024;

/// A message to be written to a bag file.
///
/// # Fields
///
/// - `conn_id`: Must correspond to a connection added via `add_connection`.
///   Values are sequential starting from 0, matching the order of
///   `add_connection` calls.
/// - `time_ns`: Timestamp in nanoseconds since the Unix epoch
///   (1970-01-01 00:00:00 UTC).
/// - `data`: Raw ROS1 serialized message bytes. This should be the
///   pre-serialized message data.
#[derive(Debug, Clone)]
pub struct BagMessage {
    /// Connection ID (must match order of add_connection calls, starting from 0)
    pub conn_id: u16,
    /// Timestamp in nanoseconds since Unix epoch
    pub time_ns: u64,
    /// Raw message data (ROS1 serialized bytes)
    pub data: Vec<u8>,
}

impl BagMessage {
    /// Create a new BagMessage from raw data.
    ///
    /// Use this when you have raw message data from another bag file.
    pub fn from_raw(conn_id: u16, time_ns: u64, data: Vec<u8>) -> Self {
        Self {
            conn_id,
            time_ns,
            data,
        }
    }

    /// Create a new BagMessage.
    pub fn new(conn_id: u16, time_ns: u64, data: Vec<u8>) -> Self {
        Self {
            conn_id,
            time_ns,
            data,
        }
    }
}

/// Connection info for a topic
#[derive(Debug, Clone)]
struct ConnectionInfo {
    id: u32,
    topic: String,
    datatype: String,
    md5sum: String,
    message_definition: String,
    callerid: Option<String>,
    latching: bool,
}

/// Index entry for message lookup
#[derive(Debug, Clone, Copy)]
struct IndexEntry {
    /// Timestamp (sec, nsec)
    time: (u32, u32),
    /// Offset within the chunk
    offset: u32,
}

/// Chunk info for the bag summary
#[derive(Debug, Clone)]
struct ChunkInfo {
    /// Position of the chunk in the file
    pos: u64,
    /// Start time (sec, nsec)
    start_time: (u32, u32),
    /// End time (sec, nsec)
    end_time: (u32, u32),
    /// Connection count per connection ID
    connection_counts: HashMap<u32, u32>,
}

/// ROS1 bag file writer.
///
/// # Important
///
/// You must call [`finish()`](BagWriter::finish) to properly finalize the bag file.
/// Dropping the writer without calling `finish()` will produce an incomplete
/// bag file (only the header will be written) and a warning will be printed.
pub struct BagWriter {
    /// File writer
    writer: BufWriter<File>,
    /// File path (kept for potential future use/debugging)
    #[allow(dead_code)]
    path: String,
    /// Is the file open
    is_open: bool,

    /// Mapping from topic to connection ID
    topic_connection_ids: HashMap<String, u32>,
    /// All connections
    connections: HashMap<u32, ConnectionInfo>,
    /// All chunk infos
    chunk_infos: Vec<ChunkInfo>,

    /// Current chunk buffer
    chunk_buffer: Vec<u8>,
    /// Position of current chunk header in the buffer
    current_chunk_position: usize,
    /// Current chunk info
    current_chunk_info: Option<ChunkInfo>,
    /// Current chunk indexes per connection
    current_chunk_indexes: HashMap<u32, Vec<IndexEntry>>,
    /// All connection indexes
    connection_indexes: HashMap<u32, Vec<IndexEntry>>,

    /// Chunk size threshold
    chunk_threshold: usize,
    /// Next connection ID
    next_conn_id: u32,
    /// Total bytes written to file
    file_pos: u64,
    /// Connections written to current chunk
    connections_written_to_chunk: HashSet<u32>,
}

impl BagWriter {
    /// Create a new bag file for writing.
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        let file = File::create(&path)
            .map_err(|e| CodecError::encode("BagWriter", format!("Failed to create file: {e}")))?;

        let mut writer = BufWriter::new(file);

        // Write version and file header
        let mut start_buffer = Vec::new();
        Self::write_file_header_record(&mut start_buffer, 0, 0, 0);
        writer
            .write_all(&start_buffer)
            .map_err(|e| CodecError::encode("BagWriter", format!("Failed to write header: {e}")))?;

        let file_pos = start_buffer.len() as u64;

        Ok(Self {
            writer,
            path: path_str,
            is_open: true,
            topic_connection_ids: HashMap::new(),
            connections: HashMap::new(),
            chunk_infos: Vec::new(),
            chunk_buffer: Vec::new(),
            current_chunk_position: 0,
            current_chunk_info: None,
            current_chunk_indexes: HashMap::new(),
            connection_indexes: HashMap::new(),
            chunk_threshold: DEFAULT_CHUNK_THRESHOLD,
            next_conn_id: 0,
            file_pos,
            connections_written_to_chunk: HashSet::new(),
        })
    }

    /// Add a connection to the bag file.
    ///
    /// This is a convenience method that uses an empty callerid.
    /// For preserving callerid information, use `add_connection_with_callerid()` instead.
    ///
    /// If a connection with the same topic and empty callerid already exists,
    /// this method returns Ok without creating a duplicate.
    ///
    /// # Arguments
    ///
    /// * `_channel_id` - Reserved for future use (connections are assigned sequential IDs internally)
    /// * `topic` - Topic name (e.g., "/chatter", "/tf")
    /// * `message_type` - Message type (e.g., "std_msgs/String", "tf2_msgs/TFMessage")
    /// * `message_definition` - Message definition schema
    pub fn add_connection(
        &mut self,
        _channel_id: u16,
        topic: &str,
        message_type: &str,
        message_definition: &str,
    ) -> Result<()> {
        // Check for duplicate topic with empty callerid (idempotent behavior)
        if let Some(&existing_conn_id) = self.topic_connection_ids.get(topic) {
            if let Some(existing_conn) = self.connections.get(&existing_conn_id) {
                if existing_conn.callerid.as_ref().is_none_or(|s| s.is_empty()) {
                    // Same topic with empty callerid already exists - skip duplicate
                    return Ok(());
                }
            }
        }
        self.add_connection_with_callerid(_channel_id, topic, message_type, message_definition, "")
    }

    /// Add a connection to the bag file with the specified callerid.
    ///
    /// In ROS1, multiple nodes can publish to the same topic with different callerids.
    /// This method preserves that information by storing the callerid for each connection.
    ///
    /// # Arguments
    ///
    /// * `channel_id` - Reserved for future use (connections are assigned sequential IDs internally)
    /// * `topic` - Topic name (e.g., "/tf", "/scan")
    /// * `message_type` - Message type (e.g., "tf2_msgs/TFMessage")
    /// * `message_definition` - Message definition schema
    /// * `callerid` - The node publishing to this topic (e.g., "/tf_publisher")
    pub fn add_connection_with_callerid(
        &mut self,
        _channel_id: u16,
        topic: &str,
        message_type: &str,
        message_definition: &str,
        callerid: &str,
    ) -> Result<()> {
        if !self.is_open {
            return Err(CodecError::encode(
                "BagWriter",
                "Cannot add connection to closed bag",
            ));
        }

        // Generate MD5 sum - zero MD5 is a known limitation.
        // Real ROS bags compute MD5 of the message definition for type safety.
        // A zero MD5 means type checking is disabled and readers will accept
        // any message data. This implementation does not compute MD5 hashes;
        // use with caution in production systems that require type safety.
        let md5sum = "00000000000000000000000000000000".to_string();

        let conn_id = self.next_conn_id;
        self.next_conn_id += 1;

        let conn_info = ConnectionInfo {
            id: conn_id,
            topic: topic.to_string(),
            datatype: message_type.to_string(),
            md5sum,
            message_definition: message_definition.to_string(),
            callerid: Some(callerid.to_string()),
            latching: false,
        };

        self.topic_connection_ids.insert(topic.to_string(), conn_id);
        self.connections.insert(conn_id, conn_info);

        Ok(())
    }

    /// Write a message to the bag file.
    pub fn write_message(&mut self, msg: &BagMessage) -> Result<()> {
        if !self.is_open {
            return Err(CodecError::encode(
                "BagWriter",
                "Cannot write to closed bag",
            ));
        }

        // Validate that the message's conn_id maps to a registered connection
        // Connection IDs must be sequential and match the order of add_connection calls
        let conn_id = self.find_connection_for_channel(msg.conn_id)?;

        // Convert nanoseconds to (sec, nsec)
        let time = ns_to_time(msg.time_ns);

        // Start a chunk if none is in progress
        if self.current_chunk_info.is_none() {
            self.start_chunk(time);
        }

        // Update chunk start/end times
        if let Some(ref mut chunk_info) = self.current_chunk_info {
            if time_less_than(time, chunk_info.start_time) {
                chunk_info.start_time = time;
            }
            if time_less_than(chunk_info.end_time, time) {
                chunk_info.end_time = time;
            }
        }

        // Write connection record to chunk if not already written
        if !self.connections_written_to_chunk.contains(&conn_id) {
            if let Some(conn_info) = self.connections.get(&conn_id) {
                Self::write_connection_record_to_buffer(&mut self.chunk_buffer, conn_info);
                self.connections_written_to_chunk.insert(conn_id);
            }
        }

        // Calculate message offset within the chunk data (for index lookups)
        let offset = self.chunk_buffer.len() - self.current_chunk_position;

        // Write message data record header
        let _header_len =
            Self::write_data_message_record_header(&mut self.chunk_buffer, conn_id, time);

        // Write message data length
        write_u32(&mut self.chunk_buffer, msg.data.len() as u32);

        // Write message data
        self.chunk_buffer.extend_from_slice(&msg.data);

        // Create index entry
        let index_entry = IndexEntry {
            time,
            offset: offset as u32,
        };

        // Add to chunk indexes
        self.current_chunk_indexes
            .entry(conn_id)
            .or_default()
            .push(index_entry);

        // Add to connection indexes
        self.connection_indexes
            .entry(conn_id)
            .or_default()
            .push(index_entry);

        // Update connection count in chunk info
        if let Some(ref mut chunk_info) = self.current_chunk_info {
            *chunk_info.connection_counts.entry(conn_id).or_default() += 1;
        }

        // Check if we should finish the chunk
        let chunk_size = self.chunk_buffer.len() - self.current_chunk_position;
        if chunk_size >= self.chunk_threshold {
            self.finish_chunk()?;
        }

        Ok(())
    }

    /// Validate connection ID and return the internal connection ID.
    ///
    /// This performs a simple bounds check assuming connection IDs are assigned
    /// sequentially starting from 0. The message's conn_id must be less than
    /// the number of connections added via `add_connection`.
    fn find_connection_for_channel(&self, conn_id: u16) -> Result<u32> {
        if (conn_id as u32) < self.next_conn_id {
            Ok(conn_id as u32)
        } else {
            Err(CodecError::encode(
                "BagWriter",
                format!(
                    "No connection found for conn_id {conn_id} (only {0} connections added)",
                    self.next_conn_id
                ),
            ))
        }
    }

    /// Start a new chunk.
    fn start_chunk(&mut self, time: (u32, u32)) {
        self.current_chunk_position = self.chunk_buffer.len();

        let chunk_pos = self.file_pos + self.chunk_buffer.len() as u64;

        self.current_chunk_info = Some(ChunkInfo {
            pos: chunk_pos,
            start_time: time,
            end_time: time,
            connection_counts: HashMap::new(),
        });

        // Write placeholder chunk header (will be overwritten later)
        Self::write_chunk_header(&mut self.chunk_buffer, 0, 0);

        self.connections_written_to_chunk.clear();
    }

    /// Finish the current chunk and write it to file.
    fn finish_chunk(&mut self) -> Result<()> {
        if self.current_chunk_info.is_none() {
            return Ok(());
        }

        // Calculate chunk data size (from after chunk header to current position)
        let chunk_header_len = Self::chunk_header_length();
        let chunk_data_len =
            self.chunk_buffer.len() - self.current_chunk_position - chunk_header_len;

        // Write index records for this chunk
        Self::write_index_records(&mut self.chunk_buffer, &self.current_chunk_indexes);

        // Update the chunk header with correct size
        let mut header_buffer = Vec::new();
        Self::write_chunk_header(
            &mut header_buffer,
            chunk_data_len as u32,
            chunk_data_len as u32,
        );

        // Replace the placeholder header
        let header_start = self.current_chunk_position;
        let header_end = header_start + chunk_header_len;
        self.chunk_buffer[header_start..header_end].copy_from_slice(&header_buffer);

        // Write chunk buffer to file
        self.writer
            .write_all(&self.chunk_buffer)
            .map_err(|e| CodecError::encode("BagWriter", format!("Failed to write chunk: {e}")))?;
        self.file_pos += self.chunk_buffer.len() as u64;

        // Save chunk info
        if let Some(chunk_info) = self.current_chunk_info.take() {
            self.chunk_infos.push(chunk_info);
        }

        // Clear buffers
        self.chunk_buffer.clear();
        self.current_chunk_indexes.clear();
        self.current_chunk_position = 0;

        Ok(())
    }
    /// Internal finalize logic that doesn't consume self.
    fn finish_internal(&mut self) -> Result<()> {
        if !self.is_open {
            return Err(CodecError::encode("BagWriter", "Bag already closed"));
        }

        // Finish any open chunk
        self.finish_chunk()?;

        // Get position of index data
        let index_data_position = self.file_pos;

        // Write connection records (summary)
        let mut stop_buffer = Vec::new();
        Self::write_connection_records(&mut stop_buffer, &self.connections);

        // Write chunk info records
        Self::write_chunk_info_records(&mut stop_buffer, &self.chunk_infos);

        self.writer
            .write_all(&stop_buffer)
            .map_err(|e| CodecError::encode("BagWriter", format!("Failed to write index: {e}")))?;

        // Update file header with correct counts
        let mut file_header_buffer = Vec::new();
        Self::write_file_header_record(
            &mut file_header_buffer,
            self.connections.len() as u32,
            self.chunk_infos.len() as u32,
            index_data_position,
        );

        // Seek to beginning and rewrite header
        self.writer
            .seek(SeekFrom::Start(0))
            .map_err(|e| CodecError::encode("BagWriter", format!("Failed to seek: {e}")))?;
        self.writer.write_all(&file_header_buffer).map_err(|e| {
            CodecError::encode("BagWriter", format!("Failed to update header: {e}"))
        })?;

        self.writer
            .flush()
            .map_err(|e| CodecError::encode("BagWriter", format!("Failed to flush: {e}")))?;

        self.is_open = false;

        Ok(())
    }

    /// Finalize the bag file and write index data.
    pub fn finish(mut self) -> Result<()> {
        self.finish_internal()
    }

    // =========================================================================
    // Helper functions for writing records
    // =========================================================================

    /// Write the version string.
    fn write_version(buffer: &mut Vec<u8>) {
        let version_line = format!("#ROSBAG V{VERSION}\n");
        buffer.extend_from_slice(version_line.as_bytes());
    }

    /// Write a header as key=value pairs.
    fn write_header(buffer: &mut Vec<u8>, fields: &BTreeMap<String, Vec<u8>>) -> u32 {
        let mut header_data = Vec::new();

        for (key, value) in fields {
            // field_len (4 bytes) + key + '=' + value
            let field_len = key.len() + 1 + value.len();
            write_u32(&mut header_data, field_len as u32);
            header_data.extend_from_slice(key.as_bytes());
            header_data.push(b'=');
            header_data.extend_from_slice(value);
        }

        let header_len = header_data.len() as u32;
        write_u32(buffer, header_len);
        buffer.extend(header_data);

        header_len
    }

    /// Write file header record (padded to 4096 bytes).
    fn write_file_header_record(
        buffer: &mut Vec<u8>,
        connection_count: u32,
        chunk_count: u32,
        index_data_position: u64,
    ) {
        Self::write_version(buffer);
        let version_len = buffer.len();

        let mut fields = BTreeMap::new();
        fields.insert("op".to_string(), vec![OP_BAG_HEADER]);
        fields.insert("index_pos".to_string(), u64_to_bytes(index_data_position));
        fields.insert("conn_count".to_string(), u32_to_bytes(connection_count));
        fields.insert("chunk_count".to_string(), u32_to_bytes(chunk_count));

        let header_len = Self::write_header(buffer, &fields);

        // Calculate padding to fill to 4096 bytes
        // 4096 - version_len - 4 (header_len) - header_len - 4 (data_len) = data_len
        let used = version_len + 4 + header_len as usize;
        let data_len = 4096 - used - 4;

        write_u32(buffer, data_len as u32);

        // Pad with spaces
        buffer.resize(buffer.len() + data_len, b' ');
    }

    /// Write a chunk header.
    fn write_chunk_header(buffer: &mut Vec<u8>, compressed_size: u32, uncompressed_size: u32) {
        let mut fields = BTreeMap::new();
        fields.insert("op".to_string(), vec![OP_CHUNK]);
        fields.insert("compression".to_string(), b"none".to_vec());
        fields.insert("size".to_string(), u32_to_bytes(uncompressed_size));

        Self::write_header(buffer, &fields);
        write_u32(buffer, compressed_size);
    }

    /// Get the length of a chunk header.
    ///
    /// This dynamically calculates the header length by writing to a temporary
    /// buffer. The header has variable-length fields, so we cannot use a
    /// constant value. This is called during chunk finalization to calculate
    /// the offset to the chunk data.
    fn chunk_header_length() -> usize {
        let mut temp = Vec::new();
        Self::write_chunk_header(&mut temp, 0, 0);
        temp.len()
    }

    /// Write connection record to buffer.
    fn write_connection_record_to_buffer(buffer: &mut Vec<u8>, conn: &ConnectionInfo) {
        // Connection header
        let mut fields = BTreeMap::new();
        fields.insert("op".to_string(), vec![OP_CONNECTION]);
        fields.insert("conn".to_string(), u32_to_bytes(conn.id));
        fields.insert("topic".to_string(), conn.topic.as_bytes().to_vec());

        Self::write_header(buffer, &fields);

        // Connection data (nested header with type info)
        let mut data_fields = BTreeMap::new();
        data_fields.insert("type".to_string(), conn.datatype.as_bytes().to_vec());
        data_fields.insert("md5sum".to_string(), conn.md5sum.as_bytes().to_vec());
        data_fields.insert(
            "message_definition".to_string(),
            conn.message_definition.as_bytes().to_vec(),
        );
        if let Some(ref callerid) = conn.callerid {
            // Ensure callerid has leading slash like the original ROS bag format
            let callerid_with_slash = if callerid.starts_with('/') {
                callerid.clone()
            } else {
                format!("/{callerid}")
            };
            data_fields.insert(
                "callerid".to_string(),
                callerid_with_slash.as_bytes().to_vec(),
            );
        }
        // Always include latching field (required by rosbag parsers)
        data_fields.insert(
            "latching".to_string(),
            if conn.latching {
                b"1".to_vec()
            } else {
                b"0".to_vec()
            },
        );

        Self::write_header(buffer, &data_fields);
    }

    /// Write connection records (summary at end of file).
    fn write_connection_records(buffer: &mut Vec<u8>, connections: &HashMap<u32, ConnectionInfo>) {
        // Sort by ID for deterministic output
        let mut ids: Vec<_> = connections.keys().collect();
        ids.sort();

        for id in ids {
            if let Some(conn) = connections.get(id) {
                Self::write_connection_record_to_buffer(buffer, conn);
            }
        }
    }

    /// Write message data record header.
    fn write_data_message_record_header(
        buffer: &mut Vec<u8>,
        conn_id: u32,
        time: (u32, u32),
    ) -> usize {
        let mut fields = BTreeMap::new();
        fields.insert("op".to_string(), vec![OP_MSG_DATA]);
        fields.insert("conn".to_string(), u32_to_bytes(conn_id));
        fields.insert("time".to_string(), time_to_bytes(time));

        let header_len = Self::write_header(buffer, &fields);
        header_len as usize
    }

    /// Write index records for a chunk.
    fn write_index_records(buffer: &mut Vec<u8>, indexes: &HashMap<u32, Vec<IndexEntry>>) {
        // Sort by connection ID for deterministic output
        let mut ids: Vec<_> = indexes.keys().collect();
        ids.sort();

        for conn_id in ids {
            if let Some(entries) = indexes.get(conn_id) {
                // Index header
                let mut fields = BTreeMap::new();
                fields.insert("op".to_string(), vec![OP_INDEX_DATA]);
                fields.insert("conn".to_string(), u32_to_bytes(*conn_id));
                fields.insert("ver".to_string(), u32_to_bytes(INDEX_VERSION));
                fields.insert("count".to_string(), u32_to_bytes(entries.len() as u32));

                Self::write_header(buffer, &fields);

                // Index data: pairs of (time, offset)
                let data_len = entries.len() * 12; // 8 bytes time + 4 bytes offset
                write_u32(buffer, data_len as u32);

                for entry in entries {
                    write_u32(buffer, entry.time.0);
                    write_u32(buffer, entry.time.1);
                    write_u32(buffer, entry.offset);
                }
            }
        }
    }

    /// Write chunk info records.
    fn write_chunk_info_records(buffer: &mut Vec<u8>, chunk_infos: &[ChunkInfo]) {
        for chunk_info in chunk_infos {
            let mut fields = BTreeMap::new();
            fields.insert("op".to_string(), vec![OP_CHUNK_INFO]);
            fields.insert("ver".to_string(), u32_to_bytes(CHUNK_INFO_VERSION));
            fields.insert("chunk_pos".to_string(), u64_to_bytes(chunk_info.pos));
            fields.insert(
                "start_time".to_string(),
                time_to_bytes(chunk_info.start_time),
            );
            fields.insert("end_time".to_string(), time_to_bytes(chunk_info.end_time));
            fields.insert(
                "count".to_string(),
                u32_to_bytes(chunk_info.connection_counts.len() as u32),
            );

            Self::write_header(buffer, &fields);

            // Write connection counts data
            let data_len = chunk_info.connection_counts.len() * 8;
            write_u32(buffer, data_len as u32);

            // Sort by connection ID for deterministic output
            let mut ids: Vec<_> = chunk_info.connection_counts.keys().collect();
            ids.sort();

            for conn_id in ids {
                if let Some(&count) = chunk_info.connection_counts.get(conn_id) {
                    write_u32(buffer, *conn_id);
                    write_u32(buffer, count);
                }
            }
        }
    }
}

impl Drop for BagWriter {
    fn drop(&mut self) {
        if self.is_open {
            // Cannot call finish() from drop because it consumes self
            // User should call finish() explicitly
            eprintln!("Warning: BagWriter dropped without calling finish()");
        }
    }
}

// =============================================================================
// Helper functions
// =============================================================================

/// Write u32 in little-endian format.
fn write_u32(buffer: &mut Vec<u8>, value: u32) {
    buffer.extend_from_slice(&value.to_le_bytes());
}

/// Convert u32 to little-endian bytes.
fn u32_to_bytes(value: u32) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

/// Convert u64 to little-endian bytes.
fn u64_to_bytes(value: u64) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

/// Convert (sec, nsec) time to little-endian bytes.
fn time_to_bytes(time: (u32, u32)) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(8);
    bytes.extend_from_slice(&time.0.to_le_bytes());
    bytes.extend_from_slice(&time.1.to_le_bytes());
    bytes
}

/// Convert nanoseconds to (sec, nsec) tuple.
fn ns_to_time(ns: u64) -> (u32, u32) {
    let sec = (ns / 1_000_000_000) as u32;
    let nsec = (ns % 1_000_000_000) as u32;
    (sec, nsec)
}

/// Compare two times.
fn time_less_than(a: (u32, u32), b: (u32, u32)) -> bool {
    if a.0 != b.0 {
        a.0 < b.0
    } else {
        a.1 < b.1
    }
}

impl FormatWriter for BagWriter {
    fn path(&self) -> &str {
        &self.path
    }

    fn add_channel(
        &mut self,
        topic: &str,
        message_type: &str,
        _encoding: &str,
        schema: Option<&str>,
    ) -> Result<u16> {
        let message_definition = schema.unwrap_or("");
        self.add_connection(0, topic, message_type, message_definition)?;
        // Return the connection ID as u16 (BAG uses u32 for conn IDs)
        Ok(self.topic_connection_ids.get(topic).copied().unwrap_or(0) as u16)
    }

    fn write(&mut self, message: &RawMessage) -> Result<()> {
        let bag_msg =
            BagMessage::from_raw(message.channel_id, message.log_time, message.data.clone());
        self.write_message(&bag_msg)
    }

    fn write_batch(&mut self, messages: &[RawMessage]) -> Result<()> {
        for msg in messages {
            self.write(msg)?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.finish_internal()
    }

    fn message_count(&self) -> u64 {
        self.chunk_infos
            .iter()
            .map(|c| c.connection_counts.values().map(|&v| v as u64).sum::<u64>())
            .sum()
    }

    fn channel_count(&self) -> usize {
        self.connections.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ns_to_time() {
        assert_eq!(ns_to_time(0), (0, 0));
        assert_eq!(ns_to_time(1_000_000_000), (1, 0));
        assert_eq!(ns_to_time(1_500_000_000), (1, 500_000_000));
        assert_eq!(ns_to_time(1_999_999_999), (1, 999_999_999));
    }

    #[test]
    fn test_time_less_than() {
        assert!(time_less_than((0, 0), (1, 0)));
        assert!(time_less_than((1, 0), (1, 1)));
        assert!(!time_less_than((1, 0), (0, 0)));
        assert!(!time_less_than((1, 1), (1, 0)));
    }

    #[test]
    fn test_write_u32() {
        let mut buffer = Vec::new();
        write_u32(&mut buffer, 0x12345678);
        assert_eq!(buffer, vec![0x78, 0x56, 0x34, 0x12]);
    }

    #[test]
    fn test_file_header_is_4096_bytes() {
        let mut buffer = Vec::new();
        BagWriter::write_file_header_record(&mut buffer, 0, 0, 0);
        assert_eq!(buffer.len(), 4096);
    }
}
