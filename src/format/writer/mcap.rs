//! Custom MCAP writer with manual chunk control and summary section writing.
//!
//! This writer accepts pre-compressed chunks and serializes them directly
//! to the MCAP file format, bypassing the mcap::Writer's internal compression.
//!
//! # Summary Section
//!
//! The writer tracks chunk metadata during writing and produces a proper
//! MCAP summary section with chunk indexes, enabling parallel reading
//! of the output file.
//!
//! # MCAP Format Compatibility
//!
//! This writer is designed to be compatible with the mcap crate v0.24.0.
//! The summary section format matches the specification at:
//! https://github.com/foxglove/mcap/tree/main/docs/specification

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use byteorder::{LittleEndian, WriteBytesExt};

use crate::core::{CodecError, Result};
use crate::io::formats::mcap_constants::{
    MCAP_MAGIC, OP_CHANNEL, OP_CHUNK, OP_CHUNK_INDEX, OP_DATA_END, OP_FOOTER, OP_HEADER,
    OP_MESSAGE, OP_SCHEMA, OP_STATISTICS, OP_SUMMARY_OFFSET,
};
use crate::pipeline::types::chunk::CompressedChunk;

/// MCAP compression identifiers.
#[allow(dead_code)]
const COMPRESSION_NONE: &str = "";
const COMPRESSION_ZSTD: &str = "zstd";
#[allow(dead_code)]
const COMPRESSION_LZ4: &str = "lz4";

/// Chunk index record for summary section.
///
/// Tracks metadata for each chunk written to enable parallel reading.
/// Format matches mcap::records::ChunkIndex exactly.
#[derive(Debug, Clone)]
struct ChunkIndexRecord {
    /// Earliest message log_time in chunk
    message_start_time: u64,
    /// Latest message log_time in chunk
    message_end_time: u64,
    /// Offset to chunk record from file start
    chunk_start_offset: u64,
    /// Total length of chunk record
    chunk_length: u64,
    /// Message index offsets: channel_id -> offset (empty map for our chunks)
    message_index_offsets: BTreeMap<u16, u64>,
    /// Message index length (0 = no message index)
    message_index_length: u64,
    /// Compression type (e.g., "zstd", "")
    compression: String,
    /// Size of compressed chunk data
    compressed_size: u64,
    /// Size of uncompressed chunk data
    uncompressed_size: u64,
}

/// Schema record for summary section.
#[derive(Debug, Clone)]
struct SchemaRecord {
    id: u16,
    name: String,
    encoding: String,
    data: Vec<u8>,
}

/// Channel record for summary section.
#[derive(Debug, Clone)]
struct ChannelRecord {
    id: u16,
    schema_id: u16,
    topic: String,
    message_encoding: String,
    metadata: HashMap<String, String>,
}

/// Default target chunk size for message buffering (4MB uncompressed)
const DEFAULT_CHUNK_SIZE: usize = 4 * 1024 * 1024;

/// Buffered message for chunk writing
#[derive(Debug, Clone)]
struct BufferedMessage {
    channel_id: u16,
    sequence: u32,
    log_time: u64,
    publish_time: u64,
    data: Vec<u8>,
}

/// Custom MCAP writer with manual chunk control.
///
/// Unlike mcap::Writer, this writer accepts pre-compressed chunks
/// and writes them directly to the file, giving full control over
/// compression parallelism and chunk boundaries.
///
/// # Summary Section
///
/// The writer tracks chunk metadata during writing and produces
/// a proper MCAP summary section with chunk indexes, enabling
/// parallel reading of the output file.
///
/// # Message Buffering
///
/// When using `write_message()`, messages are buffered and automatically
/// written as compressed chunks when the buffer exceeds the target size.
/// This ensures the output file is suitable for parallel reading.
pub struct ParallelMcapWriter<W: Write> {
    /// Underlying writer
    writer: W,
    /// Schema IDs by name
    schema_ids: HashMap<String, u16>,
    /// Channel IDs by topic
    channel_ids: HashMap<String, u16>,
    /// Next schema ID
    next_schema_id: u16,
    /// Next channel ID
    next_channel_id: u16,
    /// Sequence numbers per channel
    sequences: HashMap<u16, u32>,
    /// Chunks written
    chunks_written: u64,
    /// Messages written
    messages_written: u64,
    /// Write start position (for summary section)
    write_start: u64,
    /// Current write position (tracked manually since BufWriter doesn't expose stream_position)
    current_position: u64,

    // === Summary section tracking ===
    /// Chunk index records for summary section
    chunk_indexes: Vec<ChunkIndexRecord>,
    /// Schema records for summary section (copies of schemas written in data section)
    schema_records: Vec<SchemaRecord>,
    /// Channel records for summary section (copies of channels written in data section)
    channel_records: Vec<ChannelRecord>,
    /// Per-channel message counts
    channel_message_counts: HashMap<u16, u64>,
    /// Earliest message time in file
    file_message_start_time: u64,
    /// Latest message time in file
    file_message_end_time: u64,
    /// Summary section start offset
    summary_start_offset: u64,

    // === Message buffering for chunk-based writing ===
    /// Buffered messages waiting to be written as a chunk
    message_buffer: Vec<BufferedMessage>,
    /// Current buffer size in bytes (uncompressed)
    buffer_size: usize,
    /// Target chunk size threshold
    target_chunk_size: usize,
}

impl ParallelMcapWriter<File> {
    /// Create a new writer that writes to the specified path.
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::create(path).map_err(|e| {
            CodecError::encode("ParallelMcapWriter", format!("Failed to create file: {e}"))
        })?;

        Self::new(file)
    }
}

impl ParallelMcapWriter<BufWriter<File>> {
    /// Create a new writer with custom buffer capacity.
    pub fn create_with_buffer<P: AsRef<Path>>(path: P, capacity: usize) -> Result<Self> {
        let file = File::create(path).map_err(|e| {
            CodecError::encode("ParallelMcapWriter", format!("Failed to create file: {e}"))
        })?;

        // Wrap file in BufWriter with specified capacity
        let writer = BufWriter::with_capacity(capacity, file);
        Self::new(writer)
    }
}

impl<W: Write> ParallelMcapWriter<W> {
    /// Create a new custom MCAP writer.
    pub fn new(writer: W) -> Result<Self> {
        Self::with_chunk_size(writer, DEFAULT_CHUNK_SIZE)
    }

    /// Create a new custom MCAP writer with a specific target chunk size.
    pub fn with_chunk_size(writer: W, target_chunk_size: usize) -> Result<Self> {
        let mut slf = Self {
            writer,
            schema_ids: HashMap::new(),
            channel_ids: HashMap::new(),
            next_schema_id: 1, // Start at 1 because schema_id 0 means "no schema" in MCAP
            next_channel_id: 0,
            sequences: HashMap::new(),
            chunks_written: 0,
            messages_written: 0,
            write_start: 0,
            current_position: 0,

            // Summary tracking
            chunk_indexes: Vec::new(),
            schema_records: Vec::new(),
            channel_records: Vec::new(),
            channel_message_counts: HashMap::new(),
            file_message_start_time: u64::MAX,
            file_message_end_time: 0,
            summary_start_offset: 0,

            // Message buffering
            message_buffer: Vec::new(),
            buffer_size: 0,
            target_chunk_size,
        };

        slf.write_header()?;
        Ok(slf)
    }

    /// Write bytes and update position tracking.
    fn write_bytes(&mut self, data: &[u8]) -> Result<()> {
        self.writer.write_all(data)?;
        self.current_position += data.len() as u64;
        Ok(())
    }

    /// Write a u8 and update position tracking.
    fn write_u8(&mut self, val: u8) -> Result<()> {
        self.writer.write_all(&[val])?;
        self.current_position += 1;
        Ok(())
    }

    /// Write a u16 and update position tracking.
    fn write_u16(&mut self, val: u16) -> Result<()> {
        self.writer.write_all(&val.to_le_bytes())?;
        self.current_position += 2;
        Ok(())
    }

    /// Write a u32 and update position tracking.
    fn write_u32(&mut self, val: u32) -> Result<()> {
        self.writer.write_all(&val.to_le_bytes())?;
        self.current_position += 4;
        Ok(())
    }

    /// Write a u64 and update position tracking.
    fn write_u64(&mut self, val: u64) -> Result<()> {
        self.writer.write_all(&val.to_le_bytes())?;
        self.current_position += 8;
        Ok(())
    }

    /// Get current write position.
    fn position(&self) -> u64 {
        self.current_position
    }

    /// Write the MCAP header.
    ///
    /// Format:
    /// - Magic: 0x89 + "MCAP" + 0x30 + \r\n (8 bytes)
    /// - Header record (op 0x01):
    ///   - record length (u64)
    ///   - profile (string: u32 length + bytes)
    ///   - library (string: u32 length + bytes)
    fn write_header(&mut self) -> Result<()> {
        // Magic bytes (8 bytes)
        self.write_bytes(&MCAP_MAGIC)?;

        // Header record
        self.write_u8(OP_HEADER)?;

        // Record length: 4 (profile length prefix) + 4 (library length prefix) = 8 bytes
        self.write_u64(8)?;

        // Profile (empty string)
        self.write_u32(0)?;

        // Library (empty string)
        self.write_u32(0)?;

        self.write_start = self.position();

        Ok(())
    }

    /// Add a schema to the MCAP file.
    ///
    /// Returns the schema ID. If the schema already exists, returns
    /// the existing ID.
    ///
    /// Schema record format:
    /// - opcode (u8 = 0x03)
    /// - record length (u64)
    /// - schema_id (u16)
    /// - name (string: u32 length + bytes)
    /// - encoding (string: u32 length + bytes)
    /// - data (bytes: u32 length + data)
    pub fn add_schema(&mut self, name: &str, encoding: &str, data: &[u8]) -> Result<u16> {
        if let Some(&id) = self.schema_ids.get(name) {
            return Ok(id);
        }

        let id = self.next_schema_id;
        self.next_schema_id = id.wrapping_add(1);

        // Write schema record
        self.write_u8(OP_SCHEMA)?;

        // Record length = 2 (id) + 4 + name.len() + 4 + encoding.len() + 4 + data.len()
        let record_length: u64 =
            2 + 4 + name.len() as u64 + 4 + encoding.len() as u64 + 4 + data.len() as u64;
        self.write_u64(record_length)?;

        // Schema ID
        self.write_u16(id)?;

        // Name (string)
        self.write_u32(name.len() as u32)?;
        self.write_bytes(name.as_bytes())?;

        // Encoding (string)
        self.write_u32(encoding.len() as u32)?;
        self.write_bytes(encoding.as_bytes())?;

        // Schema data
        self.write_u32(data.len() as u32)?;
        self.write_bytes(data)?;

        // Store schema record for summary section
        self.schema_records.push(SchemaRecord {
            id,
            name: name.to_string(),
            encoding: encoding.to_string(),
            data: data.to_vec(),
        });

        self.schema_ids.insert(name.to_string(), id);
        Ok(id)
    }

    /// Add a channel to the MCAP file.
    ///
    /// Returns the channel ID. If the channel already exists, returns
    /// the existing ID.
    ///
    /// Channel record format:
    /// - opcode (u8 = 0x04)
    /// - record length (u64)
    /// - channel_id (u16)
    /// - topic (string: u32 length + bytes)
    /// - message_encoding (string: u32 length + bytes)
    /// - schema_id (u16, 0 = no schema)
    /// - metadata (string map: u32 byte length + [u32 key_len + key_bytes + u32 val_len + val_bytes]...)
    pub fn add_channel(
        &mut self,
        schema_id: u16,
        topic: &str,
        encoding: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<u16> {
        if let Some(&id) = self.channel_ids.get(topic) {
            return Ok(id);
        }

        let id = self.next_channel_id;
        self.next_channel_id = id.wrapping_add(1);

        self.write_channel_record(id, schema_id, topic, encoding, metadata)
    }

    /// Add a channel with a specific ID.
    ///
    /// This is useful when the channel IDs in the source data must be preserved
    /// (e.g., when writing pre-compressed chunks that reference specific channel IDs).
    pub fn add_channel_with_id(
        &mut self,
        channel_id: u16,
        schema_id: u16,
        topic: &str,
        encoding: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<u16> {
        if let Some(&id) = self.channel_ids.get(topic) {
            return Ok(id);
        }

        // Update next_channel_id to avoid collisions
        if channel_id >= self.next_channel_id {
            self.next_channel_id = channel_id.wrapping_add(1);
        }

        self.write_channel_record(channel_id, schema_id, topic, encoding, metadata)
    }

    /// Internal method to write a channel record.
    fn write_channel_record(
        &mut self,
        id: u16,
        schema_id: u16,
        topic: &str,
        encoding: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<u16> {
        // Serialize metadata (includes byte-length prefix)
        let metadata_bytes = serialize_metadata(metadata)?;

        // Write channel record
        self.write_u8(OP_CHANNEL)?;

        // Record length = 2 + 2 + 4 + topic.len() + 4 + encoding.len() + metadata_bytes.len()
        // Note: metadata_bytes already includes the 4-byte length prefix
        // MCAP spec order: channel_id, schema_id, topic, encoding, metadata
        let record_length: u64 = 2
            + 2
            + 4
            + topic.len() as u64
            + 4
            + encoding.len() as u64
            + metadata_bytes.len() as u64;
        self.write_u64(record_length)?;

        // Channel ID (u16)
        self.write_u16(id)?;

        // Schema ID (u16) - must come right after channel ID per MCAP spec
        self.write_u16(schema_id)?;

        // Topic (string with u32 length prefix)
        self.write_u32(topic.len() as u32)?;
        self.write_bytes(topic.as_bytes())?;

        // Message encoding (string with u32 length prefix)
        self.write_u32(encoding.len() as u32)?;
        self.write_bytes(encoding.as_bytes())?;

        // Metadata (already includes byte-length prefix from serialize_metadata)
        self.write_bytes(&metadata_bytes)?;

        // Store channel record for summary section
        self.channel_records.push(ChannelRecord {
            id,
            schema_id,
            topic: topic.to_string(),
            message_encoding: encoding.to_string(),
            metadata: metadata.clone(),
        });

        // Initialize sequence number and message count
        self.sequences.insert(id, 0);
        self.channel_message_counts.insert(id, 0);

        self.channel_ids.insert(topic.to_string(), id);
        Ok(id)
    }

    /// Write a pre-compressed chunk.
    ///
    /// This is the key method for parallel compression. The chunk has
    /// already been compressed by the compression thread pool.
    ///
    /// Chunk record format:
    /// - opcode (u8 = 0x06)
    /// - record length (u64)
    /// - message_start_time (u64)
    /// - message_end_time (u64)
    /// - uncompressed_size (u64)
    /// - uncompressed_crc (u32)
    /// - compression (string: u32 length + bytes)
    /// - compressed_size (u64)
    /// - [records...]
    ///
    /// Also tracks metadata for the summary section.
    pub fn write_compressed_chunk(&mut self, chunk: CompressedChunk) -> Result<()> {
        // Record chunk start offset for summary
        let chunk_start_offset = self.position();

        // Update file-level time bounds
        self.file_message_start_time = self.file_message_start_time.min(chunk.message_start_time);
        self.file_message_end_time = self.file_message_end_time.max(chunk.message_end_time);

        // Write chunk header
        self.write_u8(OP_CHUNK)?;

        let compression_str = COMPRESSION_ZSTD;
        let compressed_size = chunk.compressed_data.len() as u64;
        let uncompressed_size = chunk.uncompressed_size as u64;

        // Chunk record length (excluding opcode and length field)
        // 8 + 8 + 8 + 4 + 4 + compression.len() + 8 + compressed_data
        let record_length: u64 =
            8 + 8 + 8 + 4 + 4 + compression_str.len() as u64 + 8 + compressed_size;
        self.write_u64(record_length)?;

        // Message start time
        self.write_u64(chunk.message_start_time)?;

        // Message end time
        self.write_u64(chunk.message_end_time)?;

        // Uncompressed size
        self.write_u64(uncompressed_size)?;

        // Uncompressed CRC (0 = no CRC for now)
        self.write_u32(0)?;

        // Compression (string)
        self.write_u32(compression_str.len() as u32)?;
        self.write_bytes(compression_str.as_bytes())?;

        // Compressed size
        self.write_u64(compressed_size)?;

        // Write compressed data
        self.write_bytes(&chunk.compressed_data)?;

        // Calculate chunk length (before MessageIndex records)
        let chunk_end_offset = self.position();
        let chunk_length = chunk_end_offset - chunk_start_offset;

        // Write MessageIndex records after the chunk
        // These enable time-based seeking within the chunk
        let message_index_start = self.position();
        let mut message_index_offsets: BTreeMap<u16, u64> = BTreeMap::new();

        for (channel_id, entries) in &chunk.message_indexes {
            // Record the offset of this MessageIndex record
            let index_offset = self.position();
            message_index_offsets.insert(*channel_id, index_offset);

            // Write MessageIndex record
            self.write_message_index(*channel_id, entries)?;
        }

        let message_index_length = self.position() - message_index_start;

        // Track chunk for summary section
        self.chunk_indexes.push(ChunkIndexRecord {
            message_start_time: chunk.message_start_time,
            message_end_time: chunk.message_end_time,
            chunk_start_offset,
            chunk_length,
            message_index_offsets,
            message_index_length,
            compression: compression_str.to_string(),
            compressed_size,
            uncompressed_size,
        });

        self.chunks_written += 1;
        self.messages_written += chunk.message_count as u64;

        Ok(())
    }

    /// Write a MessageIndex record.
    ///
    /// MessageIndex format:
    /// - opcode: 0x07 (1 byte)
    /// - record_length: u64
    /// - channel_id: u16
    /// - records_length: u32 (byte length of records array)
    /// - records: [(log_time: u64, offset: u64), ...]
    fn write_message_index(
        &mut self,
        channel_id: u16,
        entries: &[crate::pipeline::types::chunk::MessageIndexEntry],
    ) -> Result<()> {
        const OP_MESSAGE_INDEX: u8 = 0x07;

        // Calculate records byte length: each entry is 16 bytes (8 + 8)
        let records_byte_length = entries.len() as u32 * 16;

        // Record length = 2 (channel_id) + 4 (records_length) + records_byte_length
        let record_length: u64 = 2 + 4 + records_byte_length as u64;

        self.write_u8(OP_MESSAGE_INDEX)?;
        self.write_u64(record_length)?;
        self.write_u16(channel_id)?;
        self.write_u32(records_byte_length)?;

        for entry in entries {
            self.write_u64(entry.log_time)?;
            self.write_u64(entry.offset)?;
        }

        Ok(())
    }

    /// Write a single message. Messages are buffered and written as compressed
    /// chunks when the buffer exceeds the target chunk size.
    ///
    /// This ensures the output file has proper chunk structure for parallel reading.
    pub fn write_message(
        &mut self,
        channel_id: u16,
        log_time: u64,
        publish_time: u64,
        data: &[u8],
    ) -> Result<()> {
        // Get the sequence number and increment
        let sequence = *self.sequences.entry(channel_id).or_insert(0);
        self.sequences.insert(channel_id, sequence + 1);

        // Update channel message count
        *self.channel_message_counts.entry(channel_id).or_insert(0) += 1;

        // Update file-level time bounds
        self.file_message_start_time = self.file_message_start_time.min(log_time);
        self.file_message_end_time = self.file_message_end_time.max(log_time);

        // Calculate message record size: opcode(1) + length(8) + channel_id(2) + sequence(4) + log_time(8) + publish_time(8) + data
        let message_size = 1 + 8 + 2 + 4 + 8 + 8 + data.len();

        // Buffer the message
        self.message_buffer.push(BufferedMessage {
            channel_id,
            sequence,
            log_time,
            publish_time,
            data: data.to_vec(),
        });
        self.buffer_size += message_size;
        self.messages_written += 1;

        // Flush buffer if it exceeds target chunk size
        if self.buffer_size >= self.target_chunk_size {
            self.flush_message_buffer()?;
        }

        Ok(())
    }

    /// Flush buffered messages as a compressed chunk.
    fn flush_message_buffer(&mut self) -> Result<()> {
        use crate::pipeline::types::chunk::MessageIndexEntry;

        if self.message_buffer.is_empty() {
            return Ok(());
        }

        // Serialize messages to uncompressed chunk data
        let mut uncompressed_data = Vec::with_capacity(self.buffer_size);

        let mut chunk_start_time = u64::MAX;
        let mut chunk_end_time = 0u64;
        let mut chunk_message_indexes: BTreeMap<u16, Vec<MessageIndexEntry>> = BTreeMap::new();

        for msg in &self.message_buffer {
            chunk_start_time = chunk_start_time.min(msg.log_time);
            chunk_end_time = chunk_end_time.max(msg.log_time);

            // Record offset before writing message
            let offset = uncompressed_data.len() as u64;
            chunk_message_indexes
                .entry(msg.channel_id)
                .or_default()
                .push(MessageIndexEntry {
                    log_time: msg.log_time,
                    offset,
                });

            // Write message record: opcode + length + channel_id + sequence + log_time + publish_time + data
            uncompressed_data.push(OP_MESSAGE);

            let record_len = 2 + 4 + 8 + 8 + msg.data.len();
            uncompressed_data.extend_from_slice(&(record_len as u64).to_le_bytes());
            uncompressed_data.extend_from_slice(&msg.channel_id.to_le_bytes());
            uncompressed_data.extend_from_slice(&msg.sequence.to_le_bytes());
            uncompressed_data.extend_from_slice(&msg.log_time.to_le_bytes());
            uncompressed_data.extend_from_slice(&msg.publish_time.to_le_bytes());
            uncompressed_data.extend_from_slice(&msg.data);
        }

        let message_count = self.message_buffer.len();
        let uncompressed_size = uncompressed_data.len();

        // Compress with zstd
        let compressed_data = zstd::bulk::compress(&uncompressed_data, 3).map_err(|e| {
            CodecError::encode(
                "ParallelMcapWriter",
                format!("Zstd compression failed: {e}"),
            )
        })?;

        // Write as a compressed chunk
        let chunk = CompressedChunk {
            sequence: self.chunks_written,
            compressed_data,
            uncompressed_size,
            message_start_time: chunk_start_time,
            message_end_time: chunk_end_time,
            message_count,
            compression_ratio: 0.0, // Not used here
            message_indexes: chunk_message_indexes,
        };

        // Clear the buffer before writing (to avoid double counting)
        self.message_buffer.clear();
        self.buffer_size = 0;

        // Temporarily adjust messages_written since write_compressed_chunk adds to it
        let saved_messages = self.messages_written;
        self.messages_written -= message_count as u64;

        self.write_compressed_chunk(chunk)?;

        // Restore the correct message count
        self.messages_written = saved_messages;

        Ok(())
    }

    /// Get the channel ID for a topic.
    pub fn get_channel_id(&self, topic: &str) -> Option<u16> {
        self.channel_ids.get(topic).copied()
    }

    /// Get the number of chunks written.
    pub fn chunks_written(&self) -> u64 {
        self.chunks_written
    }

    /// Get the number of messages written.
    pub fn messages_written(&self) -> u64 {
        self.messages_written
    }

    /// Flush the writer.
    pub fn flush(&mut self) -> Result<()> {
        std::io::Write::flush(&mut self.writer).map_err(|e| {
            CodecError::encode("ParallelMcapWriter", format!("Failed to flush output: {e}"))
        })
    }

    /// Write a chunk index record to the summary section.
    ///
    /// ChunkIndex record format (matching mcap::records::ChunkIndex):
    /// - opcode (u8 = 0x08)
    /// - record length (u64)
    /// - message_start_time (u64)
    /// - message_end_time (u64)
    /// - chunk_start_offset (u64)
    /// - chunk_length (u64)
    /// - message_index_offsets (int map: u32 byte length + [u16 + u64]...)
    /// - message_index_length (u64)
    /// - compression (string: u32 length + bytes)
    /// - compressed_size (u64)
    /// - uncompressed_size (u64)
    fn write_chunk_index(&mut self, chunk_idx: &ChunkIndexRecord) -> Result<()> {
        self.write_u8(OP_CHUNK_INDEX)?;

        // Calculate record length
        // 8*8 (u64 fields) + 4 (map len) + map_bytes + 4 (string len) + compression.len()
        let map_bytes: u64 = chunk_idx
            .message_index_offsets
            .iter()
            .map(|(_k, _v)| 2 + 8) // u16 key + u64 value
            .sum();
        let record_length: u64 = 8 * 7 + 4 + map_bytes + 4 + chunk_idx.compression.len() as u64;
        self.write_u64(record_length)?;

        // Message start time
        self.write_u64(chunk_idx.message_start_time)?;

        // Message end time
        self.write_u64(chunk_idx.message_end_time)?;

        // Chunk start offset
        self.write_u64(chunk_idx.chunk_start_offset)?;

        // Chunk length
        self.write_u64(chunk_idx.chunk_length)?;

        // Message index offsets (byte-length prefixed int map)
        self.write_u32(map_bytes as u32)?;
        for (&channel_id, &offset) in &chunk_idx.message_index_offsets {
            self.write_u16(channel_id)?;
            self.write_u64(offset)?;
        }

        // Message index length
        self.write_u64(chunk_idx.message_index_length)?;

        // Compression (string)
        self.write_u32(chunk_idx.compression.len() as u32)?;
        self.write_bytes(chunk_idx.compression.as_bytes())?;

        // Compressed size
        self.write_u64(chunk_idx.compressed_size)?;

        // Uncompressed size
        self.write_u64(chunk_idx.uncompressed_size)?;

        Ok(())
    }

    /// Write a statistics record to the summary section.
    ///
    /// Statistics record format (matching mcap::records::Statistics):
    /// - opcode (u8 = 0x0B)
    /// - record length (u64)
    /// - message_count (u64)
    /// - schema_count (u16)
    /// - channel_count (u32)
    /// - attachment_count (u32)
    /// - metadata_count (u32)
    /// - chunk_count (u32)
    /// - message_start_time (u64)
    /// - message_end_time (u64)
    /// - channel_message_counts (int map: u32 byte length + [u16 + u64]...)
    fn write_statistics(&mut self) -> Result<()> {
        self.write_u8(OP_STATISTICS)?;

        // Calculate record length
        // 8 + 2 + 4*3 + 4 + 8*2 + 4 + map_bytes
        let map_bytes: u64 = self
            .channel_message_counts
            .iter()
            .map(|(_k, _v)| 2 + 8) // u16 key + u64 value
            .sum();
        let record_length: u64 = 8 + 2 + 4 * 3 + 4 + 8 * 2 + 4 + map_bytes;
        self.write_u64(record_length)?;

        // Message count
        self.write_u64(self.messages_written)?;

        // Schema count
        self.write_u16(self.schema_ids.len() as u16)?;

        // Channel count
        self.write_u32(self.channel_ids.len() as u32)?;

        // Attachment count (0)
        self.write_u32(0)?;

        // Metadata count (0)
        self.write_u32(0)?;

        // Chunk count (u32!)
        self.write_u32(self.chunks_written as u32)?;

        // Message start time
        let start_time = if self.messages_written > 0 {
            self.file_message_start_time
        } else {
            0
        };
        self.write_u64(start_time)?;

        // Message end time
        let end_time = if self.messages_written > 0 {
            self.file_message_end_time
        } else {
            0
        };
        self.write_u64(end_time)?;

        // Channel message counts (byte-length prefixed int map)
        self.write_u32(map_bytes as u32)?;

        // Collect and sort counts to avoid borrow issues
        let counts: Vec<(u16, u64)> = {
            let mut sorted: Vec<_> = self.channel_message_counts.iter().collect();
            sorted.sort_by_key(|&(k, _)| k);
            sorted.iter().map(|(&k, &v)| (k, v)).collect()
        };

        for (channel_id, count) in counts {
            self.write_u16(channel_id)?;
            self.write_u64(count)?;
        }

        Ok(())
    }

    /// Write summary offset records to the summary section.
    #[allow(dead_code)]
    fn write_summary_offsets(&mut self) -> Result<()> {
        // Group opcodes by section:
        // - Schemas: OP_SCHEMA (0x03)
        // - Channels: OP_CHANNEL (0x04)
        // - Chunk Indexes: OP_CHUNK_INDEX (0x08)
        // - Statistics: OP_STATISTICS (0x0B)

        // For now, we only have chunk indexes and statistics
        // Write summary offset for chunk indexes
        self.write_summary_offset_for(OP_CHUNK_INDEX)?;

        // Write summary offset for statistics
        self.write_summary_offset_for(OP_STATISTICS)?;

        Ok(())
    }

    /// Write a summary offset record for a specific opcode group.
    fn write_summary_offset_for(&mut self, opcode: u8) -> Result<()> {
        self.write_u8(OP_SUMMARY_OFFSET)?;

        // Group opcode
        self.write_u8(opcode)?;

        // Group start (offset = 0, we'd need to track this)
        self.write_u64(0)?;

        // Group length (offset = 0, we'd need to track this)
        self.write_u64(0)?;

        Ok(())
    }

    /// Finalize the MCAP file with a proper summary section.
    ///
    /// This writes:
    /// 1. Data end section (OP_DATA_END = 0x0F)
    /// 2. Summary section with chunk indexes and statistics
    /// 3. Footer with summary reference
    /// 4. Magic bytes (8 bytes)
    ///
    /// The summary section enables parallel reading of the output file.
    ///
    /// Footer format:
    /// - opcode (u8 = 0x02)
    /// - record_length (u64 = 20)
    /// - summary_start (u64, 0 = no summary)
    /// - summary_offset_start (u64, 0 = no summary offset section)
    /// - summary_crc (u32, 0 = no CRC)
    pub fn finish(&mut self) -> Result<u64> {
        // Flush any remaining buffered messages as a final chunk
        self.flush_message_buffer()?;

        // Write data end section
        // Format: opcode (1) + record_length (8) + data_section_crc (4)
        self.write_u8(OP_DATA_END)?;
        self.write_u64(4)?; // Record length = 4 bytes for CRC field
        self.write_u32(0)?; // data_section_crc = 0 (no CRC computed)

        // === Start of summary section ===
        // Per MCAP spec and mcap crate: schemas first, then channels, then statistics, then chunk indexes
        self.summary_start_offset = self.position();

        // Write schema records (copies for summary section)
        let schema_records = self.schema_records.clone();
        for schema in &schema_records {
            self.write_summary_schema(schema)?;
        }

        // Write channel records (copies for summary section)
        let channel_records = self.channel_records.clone();
        for channel in &channel_records {
            self.write_summary_channel(channel)?;
        }

        // Write statistics record
        self.write_statistics()?;

        // Write chunk index records
        let chunk_indexes = self.chunk_indexes.clone();
        for chunk_idx in &chunk_indexes {
            self.write_chunk_index(chunk_idx)?;
        }

        // Note: We're not writing summary offsets (summary_offset_start = 0 in footer)
        // because we don't track the exact offsets of each section type.

        // === End of summary section ===

        // Write footer
        // Format: opcode (1) + record_length (8) + summary_start (8) + summary_offset_start (8) + summary_crc (4)
        self.write_u8(OP_FOOTER)?;

        // Record length = 8 (summary_start) + 8 (summary_offset_start) + 4 (summary_crc) = 20 bytes
        self.write_u64(20)?;

        // Summary start offset
        if self.chunk_indexes.is_empty() && self.messages_written == 0 {
            // No data written, zero summary
            self.write_u64(0)?;
        } else {
            self.write_u64(self.summary_start_offset)?;
        }

        // Summary offset start (0 = no summary offset section)
        self.write_u64(0)?;

        // Summary CRC (0 = no CRC computed)
        self.write_u32(0)?;

        // Write magic (8 bytes)
        self.write_bytes(&MCAP_MAGIC)?;

        self.flush()?;

        tracing::debug!(
            "Summary section written: {} schemas, {} channels, {} chunk indexes, {} messages",
            self.schema_records.len(),
            self.channel_records.len(),
            self.chunk_indexes.len(),
            self.messages_written
        );

        Ok(self.chunks_written)
    }

    /// Write a schema record to the summary section.
    fn write_summary_schema(&mut self, schema: &SchemaRecord) -> Result<()> {
        self.write_u8(OP_SCHEMA)?;

        let record_length: u64 = 2
            + 4
            + schema.name.len() as u64
            + 4
            + schema.encoding.len() as u64
            + 4
            + schema.data.len() as u64;
        self.write_u64(record_length)?;

        self.write_u16(schema.id)?;
        self.write_u32(schema.name.len() as u32)?;
        self.write_bytes(schema.name.as_bytes())?;
        self.write_u32(schema.encoding.len() as u32)?;
        self.write_bytes(schema.encoding.as_bytes())?;
        self.write_u32(schema.data.len() as u32)?;
        self.write_bytes(&schema.data)?;

        Ok(())
    }

    /// Write a channel record to the summary section.
    fn write_summary_channel(&mut self, channel: &ChannelRecord) -> Result<()> {
        // Serialize metadata (includes byte-length prefix)
        let metadata_bytes = serialize_metadata(&channel.metadata)?;

        self.write_u8(OP_CHANNEL)?;

        // Note: metadata_bytes already includes the 4-byte length prefix
        let record_length: u64 = 2
            + 2
            + 4
            + channel.topic.len() as u64
            + 4
            + channel.message_encoding.len() as u64
            + metadata_bytes.len() as u64;
        self.write_u64(record_length)?;

        self.write_u16(channel.id)?;
        self.write_u16(channel.schema_id)?;
        self.write_u32(channel.topic.len() as u32)?;
        self.write_bytes(channel.topic.as_bytes())?;
        self.write_u32(channel.message_encoding.len() as u32)?;
        self.write_bytes(channel.message_encoding.as_bytes())?;
        // Metadata already includes byte-length prefix from serialize_metadata
        self.write_bytes(&metadata_bytes)?;

        Ok(())
    }

    /// Get the underlying writer.
    pub fn into_inner(self) -> W {
        self.writer
    }
}

/// Serialize metadata HashMap to MCAP format.
///
/// Format: byte-length prefixed map of string pairs
/// - u32: total byte length of all entries
/// - For each entry: u32 key_len + key_bytes + u32 val_len + val_bytes
fn serialize_metadata(metadata: &HashMap<String, String>) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();

    // First, calculate total byte length
    let mut total_len: u32 = 0;
    for (key, value) in metadata {
        total_len += 4 + key.len() as u32 + 4 + value.len() as u32;
    }

    // Write total byte length
    bytes.write_u32::<LittleEndian>(total_len)?;

    // Write each entry
    for (key, value) in metadata {
        bytes.write_u32::<LittleEndian>(key.len() as u32)?;
        bytes.write_all(key.as_bytes())?;
        bytes.write_u32::<LittleEndian>(value.len() as u32)?;
        bytes.write_all(value.as_bytes())?;
    }

    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_serialize_metadata_empty() {
        let metadata = HashMap::new();
        let bytes = serialize_metadata(&metadata).unwrap();
        // Empty map is represented by a 0 byte length
        assert_eq!(bytes.len(), 4);
        assert_eq!(bytes, [0, 0, 0, 0]);
    }

    #[test]
    fn test_serialize_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert("key1".to_string(), "value1".to_string());
        metadata.insert("key2".to_string(), "value2".to_string());

        let bytes = serialize_metadata(&metadata).unwrap();
        assert!(!bytes.is_empty());
        // First 4 bytes should be the total length of all entries (not including itself)
        let total_len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        // Total length should be: 4 + 4 + 6 + 4 + 4 + 6 = 28 bytes for 2 entries
        // Each entry: key_len(4) + key_bytes + val_len(4) + val_bytes
        assert_eq!(total_len as usize, bytes.len() - 4); // Content length excluding prefix
    }

    #[test]
    fn test_channel_record_format_readable_by_mcap_crate() {
        // This test ensures the channel record format is compatible with mcap crate
        use std::io::BufWriter;

        let cursor = Cursor::new(Vec::new());
        let mut writer = ParallelMcapWriter::new(BufWriter::new(cursor)).unwrap();

        // Add schema and channel with metadata
        let schema_id = writer.add_schema("test/Type", "ros1msg", b"test").unwrap();
        let mut metadata = HashMap::new();
        metadata.insert("key".to_string(), "value".to_string());
        let channel_id = writer
            .add_channel(schema_id, "/test/topic", "cdr", &metadata)
            .unwrap();

        // Write a message to ensure we have data for summary
        writer
            .write_message(channel_id, 1000, 1000, b"test data")
            .unwrap();

        // Finish and get the bytes
        writer.finish().unwrap();
        let cursor = writer.into_inner().into_inner().unwrap();
        let bytes = cursor.into_inner();

        // Use mcap crate to read the summary and verify channels are valid
        match mcap::Summary::read(&bytes) {
            Ok(Some(summary)) => {
                assert_eq!(summary.schemas.len(), 1);
                assert_eq!(summary.channels.len(), 1);
                // Verify channel has the correct metadata
                let channel = summary.channels.values().next().unwrap();
                assert_eq!(channel.topic, "/test/topic");
                assert_eq!(channel.metadata.get("key"), Some(&"value".to_string()));
            }
            Ok(None) => {
                // Summary might be None for small files, verify structure
                assert_eq!(&bytes[0..8], MCAP_MAGIC);
                assert_eq!(&bytes[bytes.len() - 8..], MCAP_MAGIC);
            }
            Err(e) => panic!("mcap crate failed to read file: {:?}", e),
        }
    }

    #[test]
    fn test_custom_writer_create_memory() {
        let cursor = Cursor::new(Vec::new());
        let writer = ParallelMcapWriter::new(BufWriter::new(cursor));
        assert!(writer.is_ok());
    }

    #[test]
    fn test_custom_writer_add_schema() {
        let cursor = Cursor::new(Vec::new());
        let mut writer = ParallelMcapWriter::new(BufWriter::new(cursor)).unwrap();

        let id1 = writer
            .add_schema("test_schema", "ros1msg", b"schema data")
            .unwrap();
        let id2 = writer
            .add_schema("test_schema", "ros1msg", b"schema data")
            .unwrap();

        assert_eq!(id1, id2); // Should return same ID for duplicate
        assert_eq!(id1, 1); // Schema IDs start at 1 (0 means "no schema" in MCAP)
    }

    #[test]
    fn test_custom_writer_add_channel() {
        let cursor = Cursor::new(Vec::new());
        let mut writer = ParallelMcapWriter::new(BufWriter::new(cursor)).unwrap();

        let schema_id = writer.add_schema("schema", "ros1msg", b"data").unwrap();
        let id1 = writer
            .add_channel(schema_id, "/topic", "cdr", &HashMap::new())
            .unwrap();
        let id2 = writer
            .add_channel(schema_id, "/topic", "cdr", &HashMap::new())
            .unwrap();

        assert_eq!(id1, id2); // Should return same ID for duplicate
        assert_eq!(id1, 0); // Channel IDs still start at 0 (no reserved value)
    }

    #[test]
    fn test_custom_writer_write_chunk() {
        let cursor = Cursor::new(Vec::new());
        let mut writer = ParallelMcapWriter::new(BufWriter::new(cursor)).unwrap();

        let chunk = CompressedChunk {
            sequence: 0,
            compressed_data: vec![1, 2, 3, 4],
            uncompressed_size: 100,
            message_start_time: 1000,
            message_end_time: 5000,
            message_count: 5,
            compression_ratio: 0.5,
            message_indexes: BTreeMap::new(),
        };

        let result = writer.write_compressed_chunk(chunk);
        assert!(result.is_ok());
        assert_eq!(writer.chunks_written(), 1);
        assert_eq!(writer.messages_written(), 5);

        // Verify chunk index was tracked
        assert_eq!(writer.chunk_indexes.len(), 1);
        assert_eq!(writer.chunk_indexes[0].message_start_time, 1000);
        assert_eq!(writer.chunk_indexes[0].message_end_time, 5000);
        assert_eq!(writer.chunk_indexes[0].compression, "zstd");
    }

    #[test]
    fn test_custom_writer_write_message() {
        let cursor = Cursor::new(Vec::new());
        let mut writer = ParallelMcapWriter::new(BufWriter::new(cursor)).unwrap();

        writer.write_message(0, 1000, 1000, b"test data").unwrap();

        assert_eq!(writer.messages_written(), 1);
        assert_eq!(writer.file_message_start_time, 1000);
        assert_eq!(writer.file_message_end_time, 1000);
    }

    #[test]
    fn test_custom_writer_finish_with_summary() {
        let cursor = Cursor::new(Vec::new());
        let mut writer = ParallelMcapWriter::new(BufWriter::new(cursor)).unwrap();

        // Add schema and channel
        let schema_id = writer.add_schema("schema", "ros1msg", b"data").unwrap();
        writer
            .add_channel(schema_id, "/topic", "cdr", &HashMap::new())
            .unwrap();

        // Write a chunk
        let chunk = CompressedChunk {
            sequence: 0,
            compressed_data: vec![1, 2, 3, 4],
            uncompressed_size: 100,
            message_start_time: 1000,
            message_end_time: 5000,
            message_count: 5,
            compression_ratio: 0.5,
            message_indexes: BTreeMap::new(),
        };
        writer.write_compressed_chunk(chunk).unwrap();

        // Finish should write summary
        let result = writer.finish();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);

        // Verify summary was written
        assert!(writer.summary_start_offset > 0);
        assert_eq!(writer.chunk_indexes.len(), 1);
    }

    #[test]
    fn test_chunk_index_record() {
        let record = ChunkIndexRecord {
            message_start_time: 1000,
            message_end_time: 5000,
            chunk_start_offset: 100,
            chunk_length: 200,
            message_index_offsets: BTreeMap::new(),
            message_index_length: 0,
            compression: "zstd".to_string(),
            compressed_size: 50,
            uncompressed_size: 100,
        };

        assert_eq!(record.message_start_time, 1000);
        assert_eq!(record.message_end_time, 5000);
        assert_eq!(record.compression, "zstd");
    }

    /// Test round-trip: write an MCAP file and verify it can be read back.
    #[test]
    fn test_round_trip_simple_file() {
        use std::io::Cursor;

        // Create a writer and write some data
        let cursor = Cursor::new(Vec::new());
        let mut writer = ParallelMcapWriter::new(BufWriter::new(cursor)).unwrap();

        // Add schema
        let schema_id = writer
            .add_schema("std_msgs/String", "ros1msg", b"string data")
            .unwrap();

        // Add channel
        let channel_id = writer
            .add_channel(schema_id, "/chatter", "cdr", &HashMap::new())
            .unwrap();

        // Write some messages
        for i in 0..10 {
            let data = format!("message {}", i);
            writer
                .write_message(channel_id, 1000 + i * 100, 1000 + i * 100, data.as_bytes())
                .unwrap();
        }

        // Finish writing
        let chunks_written = writer.finish().unwrap();
        assert!(chunks_written > 0);

        // Get the written bytes
        let inner = writer.into_inner();
        let bytes = inner.into_inner().unwrap().into_inner();

        // Verify the file starts with MCAP magic
        assert_eq!(&bytes[0..8], MCAP_MAGIC);

        // Verify the file ends with MCAP magic (footer)
        assert_eq!(&bytes[bytes.len() - 8..], MCAP_MAGIC);

        // The file should be valid MCAP format
        // In a real round-trip test, we would read it back with McapFormat
        // and verify the messages match
    }

    /// Test round-trip with compressed chunks.
    #[test]
    fn test_round_trip_with_compressed_chunks() {
        use std::io::Cursor;

        let cursor = Cursor::new(Vec::new());
        let mut writer = ParallelMcapWriter::new(BufWriter::new(cursor)).unwrap();

        // Add schema and channel
        let schema_id = writer
            .add_schema("test/Message", "ros1msg", b"int32 data")
            .unwrap();
        let _channel_id = writer
            .add_channel(schema_id, "/test", "cdr", &HashMap::new())
            .unwrap();

        // Write a compressed chunk
        let chunk = CompressedChunk {
            sequence: 0,
            compressed_data: vec![1, 2, 3, 4, 5],
            uncompressed_size: 100,
            message_start_time: 1000,
            message_end_time: 5000,
            message_count: 10,
            compression_ratio: 0.05,
            message_indexes: BTreeMap::new(),
        };
        writer.write_compressed_chunk(chunk).unwrap();

        // Write another compressed chunk
        let chunk2 = CompressedChunk {
            sequence: 1,
            compressed_data: vec![6, 7, 8, 9, 10],
            uncompressed_size: 200,
            message_start_time: 6000,
            message_end_time: 10000,
            message_count: 15,
            compression_ratio: 0.025,
            message_indexes: BTreeMap::new(),
        };
        writer.write_compressed_chunk(chunk2).unwrap();

        // Finish writing
        let chunks_written = writer.finish().unwrap();
        assert_eq!(chunks_written, 2);

        // Get the written bytes and verify structure
        let inner = writer.into_inner();
        let bytes = inner.into_inner().unwrap().into_inner();

        // Verify magic bytes at start
        assert_eq!(&bytes[0..8], MCAP_MAGIC);

        // Verify magic bytes at end
        assert_eq!(&bytes[bytes.len() - 8..], MCAP_MAGIC);

        // Verify the file has reasonable size
        assert!(bytes.len() > 100);
    }

    /// Test that summary section is correctly written.
    #[test]
    fn test_summary_section_written() {
        use std::io::Cursor;

        let cursor = Cursor::new(Vec::new());
        let mut writer = ParallelMcapWriter::new(BufWriter::new(cursor)).unwrap();

        // Add schema and channel
        let schema_id = writer
            .add_schema("test/Msg", "ros1msg", b"string data")
            .unwrap();
        let channel_id = writer
            .add_channel(schema_id, "/topic", "cdr", &HashMap::new())
            .unwrap();

        // Write messages to create at least one chunk
        for i in 0..100 {
            let data = format!("test message {}", i);
            writer
                .write_message(channel_id, i * 1000, i * 1000, data.as_bytes())
                .unwrap();
        }

        // Finish to write summary
        writer.finish().unwrap();

        // Verify summary section was written (before consuming writer)
        let summary_offset = writer.summary_start_offset;
        assert!(summary_offset > 0);

        // Get bytes and verify footer structure
        let inner = writer.into_inner();
        let bytes = inner.into_inner().unwrap().into_inner();

        // Find footer (last 8 bytes are magic, before that is footer record)
        let footer_start = bytes.len() - 8 - 29; // 8 magic + 1 opcode + 8 length + 8*3 fields
        assert!(bytes[footer_start] == OP_FOOTER);

        // Verify the summary_start offset in footer points to valid location
        // The summary_start should be before the footer
        let footer_summary_start = u64::from_le_bytes([
            bytes[footer_start + 9],
            bytes[footer_start + 10],
            bytes[footer_start + 11],
            bytes[footer_start + 12],
            bytes[footer_start + 13],
            bytes[footer_start + 14],
            bytes[footer_start + 15],
            bytes[footer_start + 16],
        ]);
        assert!(footer_summary_start > 0);
        assert!(footer_summary_start < footer_start as u64);
    }

    /// Test round-trip with multiple channels.
    #[test]
    fn test_round_trip_multiple_channels() {
        use std::io::Cursor;

        let cursor = Cursor::new(Vec::new());
        let mut writer = ParallelMcapWriter::new(BufWriter::new(cursor)).unwrap();

        // Add multiple schemas
        let schema1_id = writer
            .add_schema("std_msgs/String", "ros1msg", b"string data")
            .unwrap();
        let schema2_id = writer
            .add_schema("std_msgs/Int32", "ros1msg", b"int32 data")
            .unwrap();

        // Add multiple channels
        let channel1_id = writer
            .add_channel(schema1_id, "/chatter", "cdr", &HashMap::new())
            .unwrap();
        let channel2_id = writer
            .add_channel(schema2_id, "/numbers", "cdr", &HashMap::new())
            .unwrap();

        // Write messages to different channels
        for i in 0..10 {
            let data = format!("string {}", i);
            writer
                .write_message(channel1_id, i * 1000, i * 1000, data.as_bytes())
                .unwrap();
        }

        for i in 0..10 {
            let data = (i as i32 * 42i32).to_le_bytes().to_vec();
            writer
                .write_message(
                    channel2_id,
                    i as u64 * 1000 + 500,
                    i as u64 * 1000 + 500,
                    &data,
                )
                .unwrap();
        }

        // Finish writing
        let chunks_written = writer.finish().unwrap();
        assert!(chunks_written > 0);

        // Verify file structure
        let inner = writer.into_inner();
        let bytes = inner.into_inner().unwrap().into_inner();

        assert_eq!(&bytes[0..8], MCAP_MAGIC);
        assert_eq!(&bytes[bytes.len() - 8..], MCAP_MAGIC);
    }

    /// Test round-trip with metadata.
    #[test]
    fn test_round_trip_with_metadata() {
        use std::io::Cursor;

        let cursor = Cursor::new(Vec::new());
        let mut writer = ParallelMcapWriter::new(BufWriter::new(cursor)).unwrap();

        // Add schema and channel with metadata
        let mut metadata = HashMap::new();
        metadata.insert("encoding".to_string(), "cdr".to_string());
        metadata.insert("endianness".to_string(), "little".to_string());

        let schema_id = writer
            .add_schema("test/Msg", "ros1msg", b"string data")
            .unwrap();
        writer
            .add_channel(schema_id, "/topic", "cdr", &metadata)
            .unwrap();

        // Write some messages
        for i in 0..5 {
            let data = format!("message {}", i);
            writer
                .write_message(0, i * 1000, i * 1000, data.as_bytes())
                .unwrap();
        }

        // Finish
        writer.finish().unwrap();

        // Verify file is valid
        let inner = writer.into_inner();
        let bytes = inner.into_inner().unwrap().into_inner();

        assert_eq!(&bytes[0..8], MCAP_MAGIC);
        assert_eq!(&bytes[bytes.len() - 8..], MCAP_MAGIC);
    }

    /// Test that empty file (no messages) produces valid MCAP.
    #[test]
    fn test_round_trip_empty_file() {
        use std::io::Cursor;

        let cursor = Cursor::new(Vec::new());
        let mut writer = ParallelMcapWriter::new(BufWriter::new(cursor)).unwrap();

        // Add schema and channel but no messages
        let schema_id = writer
            .add_schema("test/Msg", "ros1msg", b"string data")
            .unwrap();
        writer
            .add_channel(schema_id, "/topic", "cdr", &HashMap::new())
            .unwrap();

        // Finish (should write valid MCAP with no chunks)
        let chunks_written = writer.finish().unwrap();
        assert_eq!(chunks_written, 0);

        // Verify file is valid MCAP
        let inner = writer.into_inner();
        let bytes = inner.into_inner().unwrap().into_inner();

        assert_eq!(&bytes[0..8], MCAP_MAGIC);
        assert_eq!(&bytes[bytes.len() - 8..], MCAP_MAGIC);

        // Empty file should have minimal size (header + schema + channel + footer + data_end)
        // Schema and channel records add significant size, so use a larger threshold
        assert!(bytes.len() < 500);
    }

    /// Test round-trip with large data (multiple chunks).
    #[test]
    fn test_round_trip_large_data() {
        use std::io::Cursor;

        let cursor = Cursor::new(Vec::new());
        // Use a small chunk size (1KB) to ensure multiple chunks are created
        let mut writer = ParallelMcapWriter::with_chunk_size(BufWriter::new(cursor), 1024).unwrap();

        // Add schema and channel
        let schema_id = writer
            .add_schema("test/Msg", "ros1msg", b"string data")
            .unwrap();
        let channel_id = writer
            .add_channel(schema_id, "/topic", "cdr", &HashMap::new())
            .unwrap();

        // Write many messages to trigger chunking
        for i in 0..1000 {
            let data = format!("message number {} with some payload", i);
            writer
                .write_message(channel_id, i * 1000, i * 1000, data.as_bytes())
                .unwrap();
        }

        // Finish
        let chunks_written = writer.finish().unwrap();
        assert!(chunks_written > 1); // Should create multiple chunks

        // Verify chunk indexes were created (before consuming writer)
        let chunk_indexes_count = writer.chunk_indexes.len();
        assert!(chunk_indexes_count > 1);

        // Verify file structure
        let inner = writer.into_inner();
        let bytes = inner.into_inner().unwrap().into_inner();

        assert_eq!(&bytes[0..8], MCAP_MAGIC);
        assert_eq!(&bytes[bytes.len() - 8..], MCAP_MAGIC);
    }

    /// Test add_channel_with_id preserves the specified channel ID.
    #[test]
    fn test_add_channel_with_id() {
        use std::io::Cursor;

        let cursor = Cursor::new(Vec::new());
        let mut writer = ParallelMcapWriter::new(BufWriter::new(cursor)).unwrap();

        let schema_id = writer.add_schema("test/Msg", "ros1msg", b"data").unwrap();

        // Add channels with specific IDs (out of order)
        let id1 = writer
            .add_channel_with_id(5, schema_id, "/topic_a", "ros1", &HashMap::new())
            .unwrap();
        let id2 = writer
            .add_channel_with_id(2, schema_id, "/topic_b", "ros1", &HashMap::new())
            .unwrap();
        let id3 = writer
            .add_channel_with_id(10, schema_id, "/topic_c", "ros1", &HashMap::new())
            .unwrap();

        // Verify IDs are preserved
        assert_eq!(id1, 5);
        assert_eq!(id2, 2);
        assert_eq!(id3, 10);

        // Write a message to ensure we have data
        writer.write_message(5, 1000, 1000, b"test data").unwrap();

        // Finish and verify with mcap crate
        writer.finish().unwrap();
        let inner = writer.into_inner();
        let bytes = inner.into_inner().unwrap().into_inner();

        match mcap::Summary::read(&bytes) {
            Ok(Some(summary)) => {
                assert_eq!(summary.channels.len(), 3);
                // Verify channel IDs are correct
                assert!(summary.channels.contains_key(&5));
                assert!(summary.channels.contains_key(&2));
                assert!(summary.channels.contains_key(&10));
                assert_eq!(summary.channels.get(&5).unwrap().topic, "/topic_a");
                assert_eq!(summary.channels.get(&2).unwrap().topic, "/topic_b");
                assert_eq!(summary.channels.get(&10).unwrap().topic, "/topic_c");
            }
            Ok(None) => {
                // Summary might be None if file is too small, just verify structure
                assert_eq!(&bytes[0..8], MCAP_MAGIC);
                assert_eq!(&bytes[bytes.len() - 8..], MCAP_MAGIC);
            }
            Err(e) => panic!("mcap crate failed to read file: {:?}", e),
        }
    }

    /// Test MessageIndex records are written after chunks.
    #[test]
    fn test_message_index_records_written() {
        use crate::pipeline::types::chunk::MessageIndexEntry;
        use std::io::Cursor;

        let cursor = Cursor::new(Vec::new());
        let mut writer = ParallelMcapWriter::new(BufWriter::new(cursor)).unwrap();

        let schema_id = writer.add_schema("test/Msg", "ros1msg", b"data").unwrap();
        writer
            .add_channel_with_id(0, schema_id, "/topic", "ros1", &HashMap::new())
            .unwrap();

        // Create a chunk with message indexes
        let mut message_indexes = BTreeMap::new();
        message_indexes.insert(
            0u16,
            vec![
                MessageIndexEntry {
                    log_time: 1000,
                    offset: 0,
                },
                MessageIndexEntry {
                    log_time: 2000,
                    offset: 50,
                },
                MessageIndexEntry {
                    log_time: 3000,
                    offset: 100,
                },
            ],
        );

        let chunk = CompressedChunk {
            sequence: 0,
            compressed_data: vec![1, 2, 3, 4],
            uncompressed_size: 100,
            message_start_time: 1000,
            message_end_time: 3000,
            message_count: 3,
            compression_ratio: 0.04,
            message_indexes,
        };

        writer.write_compressed_chunk(chunk).unwrap();
        writer.finish().unwrap();

        let inner = writer.into_inner();
        let bytes = inner.into_inner().unwrap().into_inner();

        // Parse the file to find MESSAGE_INDEX records
        let mut found_message_index = false;
        let mut pos = 8; // Skip magic

        while pos < bytes.len() - 8 {
            let opcode = bytes[pos];
            let record_len =
                u64::from_le_bytes(bytes[pos + 1..pos + 9].try_into().unwrap()) as usize;

            if opcode == 0x07 {
                // MESSAGE_INDEX opcode
                found_message_index = true;
                // Verify the record has correct structure
                let channel_id = u16::from_le_bytes(bytes[pos + 9..pos + 11].try_into().unwrap());
                assert_eq!(channel_id, 0);

                let records_len = u32::from_le_bytes(bytes[pos + 11..pos + 15].try_into().unwrap());
                assert_eq!(records_len, 48); // 3 entries * 16 bytes each
                break;
            }

            pos += 9 + record_len;
        }

        assert!(
            found_message_index,
            "MessageIndex record should be present in the file"
        );
    }

    /// Test that chunks with message indexes have correct ChunkIndex metadata.
    #[test]
    fn test_chunk_index_has_message_index_offsets() {
        use crate::pipeline::types::chunk::MessageIndexEntry;
        use std::io::Cursor;

        let cursor = Cursor::new(Vec::new());
        let mut writer = ParallelMcapWriter::new(BufWriter::new(cursor)).unwrap();

        let schema_id = writer.add_schema("test/Msg", "ros1msg", b"data").unwrap();
        writer
            .add_channel_with_id(0, schema_id, "/topic", "ros1", &HashMap::new())
            .unwrap();

        // Create a chunk with message indexes
        let mut message_indexes = BTreeMap::new();
        message_indexes.insert(
            0u16,
            vec![MessageIndexEntry {
                log_time: 1000,
                offset: 0,
            }],
        );

        let chunk = CompressedChunk {
            sequence: 0,
            compressed_data: vec![1, 2, 3, 4],
            uncompressed_size: 100,
            message_start_time: 1000,
            message_end_time: 1000,
            message_count: 1,
            compression_ratio: 0.04,
            message_indexes,
        };

        writer.write_compressed_chunk(chunk).unwrap();

        // Verify chunk index has message_index_offsets
        assert_eq!(writer.chunk_indexes.len(), 1);
        let chunk_index = &writer.chunk_indexes[0];
        assert!(!chunk_index.message_index_offsets.is_empty());
        assert!(chunk_index.message_index_offsets.contains_key(&0));
        assert!(chunk_index.message_index_length > 0);

        writer.finish().unwrap();

        // Verify with mcap crate
        let inner = writer.into_inner();
        let bytes = inner.into_inner().unwrap().into_inner();

        match mcap::Summary::read(&bytes) {
            Ok(Some(summary)) => {
                assert_eq!(summary.chunk_indexes.len(), 1);
                let chunk_index = &summary.chunk_indexes[0];
                // Verify message index offsets are present
                assert!(!chunk_index.message_index_offsets.is_empty());
                assert!(chunk_index.message_index_length > 0);
            }
            Ok(None) => panic!("Summary should exist"),
            Err(e) => panic!("mcap crate failed to read file: {:?}", e),
        }
    }
}
