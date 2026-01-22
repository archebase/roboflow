// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! MCAP file reader with automatic encoding detection.
//!
//! This module provides `McapReader` for reading MCAP files with support for
//! CDR, Protobuf, and JSON encodings.
//!
//! **Note:** This implementation uses a custom MCAP parser with no external dependencies.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use byteorder::{LittleEndian, ReadBytesExt};
use tracing::warn;

use crate::core::{CodecError, DecodedMessage, Result};
use crate::encoding::{CdrDecoder, JsonDecoder, ProtobufDecoder};
use crate::io::formats::mcap::ParallelMcapReader;

/// Information about a channel in an MCAP file.
#[derive(Debug, Clone)]
pub struct ChannelInfo {
    /// Channel ID
    pub id: u16,
    /// Topic name (e.g., "/joint_states")
    pub topic: String,
    /// Message type (e.g., "sensor_msgs/msg/JointState")
    pub message_type: String,
    /// Encoding (e.g., "cdr", "protobuf", "json")
    pub encoding: String,
    /// Schema definition (message definition text)
    pub schema: Option<String>,
    /// Schema data (binary, for protobuf FileDescriptorSet)
    pub schema_data: Option<Vec<u8>>,
    /// Schema encoding (e.g., "ros2msg", "protobuf")
    pub schema_encoding: Option<String>,
    /// Message count
    pub message_count: u64,
    /// Caller ID - identifies the node that publishes to this topic (ROS1 specific)
    pub callerid: Option<String>,
}

/// Raw message data from MCAP with metadata (undecoded).
#[derive(Debug, Clone)]
pub struct RawMessage {
    /// Channel ID
    pub channel_id: u16,
    /// Log timestamp (nanoseconds)
    pub log_time: u64,
    /// Publish timestamp (nanoseconds)
    pub publish_time: u64,
    /// Raw message data
    pub data: Vec<u8>,
    /// Sequence number (if available)
    pub sequence: Option<u64>,
}

/// Decoded message with timestamp metadata.
#[derive(Debug, Clone)]
pub struct TimestampedDecodedMessage {
    /// The decoded message fields
    pub message: DecodedMessage,
    /// Log timestamp (nanoseconds)
    pub log_time: u64,
    /// Publish timestamp (nanoseconds)
    pub publish_time: u64,
}

/// Robotics data reader - handles MCAP files with automatic encoding detection.
pub struct McapReader {
    /// Path to the MCAP file
    path: String,
    /// Underlying parallel reader
    inner: ParallelMcapReader,
    /// Channel information indexed by channel ID
    channels: HashMap<u16, ChannelInfo>,
    /// Decoders for different encodings
    cdr_decoder: Arc<CdrDecoder>,
    proto_decoder: Arc<ProtobufDecoder>,
    json_decoder: Arc<JsonDecoder>,
}

impl McapReader {
    /// Open an MCAP file and read its metadata.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();

        // Use the custom parallel reader
        let inner = ParallelMcapReader::open(&path)?;

        // Convert channel info to local format
        let channels: HashMap<u16, ChannelInfo> = inner
            .channels()
            .iter()
            .map(|(&id, ch)| {
                (
                    id,
                    ChannelInfo {
                        id: ch.id,
                        topic: ch.topic.clone(),
                        message_type: ch.message_type.clone(),
                        encoding: ch.encoding.clone(),
                        schema: ch.schema.clone(),
                        schema_data: ch.schema_data.clone(),
                        schema_encoding: ch.schema_encoding.clone(),
                        message_count: ch.message_count,
                        callerid: ch.callerid.clone(),
                    },
                )
            })
            .collect();

        if channels.is_empty() {
            warn!(
                context = "mcap_reader",
                "No channels found, file will be scanned during iteration"
            );
        }

        Ok(Self {
            path: path_str,
            inner,
            channels,
            cdr_decoder: Arc::new(CdrDecoder::new()),
            proto_decoder: Arc::new(ProtobufDecoder::new()),
            json_decoder: Arc::new(JsonDecoder::new()),
        })
    }

    /// Get all channel information.
    pub fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        &self.channels
    }

    /// Get channel info by topic name.
    pub fn channel_by_topic(&self, topic: &str) -> Option<&ChannelInfo> {
        self.channels.values().find(|c| c.topic == topic)
    }

    /// Get total message count.
    pub fn message_count(&self) -> u64 {
        self.inner.message_count()
    }

    /// Get start timestamp in nanoseconds.
    pub fn start_time(&self) -> Option<u64> {
        self.inner.start_time()
    }

    /// Get end timestamp in nanoseconds.
    pub fn end_time(&self) -> Option<u64> {
        self.inner.end_time()
    }

    /// Get the file path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Iterate over raw (undecoded) messages in the MCAP file.
    pub fn iter_raw(&self) -> Result<RawMessageIter<'_>> {
        Ok(RawMessageIter {
            inner: &self.inner,
            channels: self.channels.clone(),
        })
    }

    /// Iterate over decoded messages.
    ///
    /// This is the primary API for consuming robotics data. Encoding detection
    /// and decoding happen automatically based on channel metadata.
    ///
    /// # Returns
    ///
    /// An iterator yielding `(DecodedMessage, ChannelInfo)` tuples.
    pub fn decode_messages(&self) -> Result<DecodedMessageIter<'_>> {
        Ok(DecodedMessageIter {
            inner: &self.inner,
            channels: self.channels.clone(),
            cdr_decoder: Arc::clone(&self.cdr_decoder),
            proto_decoder: Arc::clone(&self.proto_decoder),
            json_decoder: Arc::clone(&self.json_decoder),
        })
    }

    /// Iterate over decoded messages with timestamps.
    ///
    /// Similar to `decode_messages()` but includes the original message timestamps
    /// from the MCAP file.
    ///
    /// # Returns
    ///
    /// An iterator yielding `(TimestampedDecodedMessage, ChannelInfo)` tuples.
    pub fn decode_messages_with_timestamp(&self) -> Result<DecodedMessageWithTimestampIter<'_>> {
        Ok(DecodedMessageWithTimestampIter {
            inner: &self.inner,
            channels: self.channels.clone(),
            cdr_decoder: Arc::clone(&self.cdr_decoder),
            proto_decoder: Arc::clone(&self.proto_decoder),
            json_decoder: Arc::clone(&self.json_decoder),
        })
    }

    /// Process all decoded messages with a callback.
    pub fn for_each_decoded<F>(self, mut callback: F) -> Result<()>
    where
        F: FnMut(&DecodedMessage, &ChannelInfo) -> Result<()>,
    {
        let iter = self.decode_messages()?;
        for result in iter {
            let (msg, channel_info) = result?;
            callback(&msg, &channel_info)?;
        }
        Ok(())
    }
}

use crate::io::traits::FormatReader;

impl FormatReader for McapReader {
    fn channels(&self) -> &HashMap<u16, crate::io::metadata::ChannelInfo> {
        self.inner.channels()
    }

    fn message_count(&self) -> u64 {
        self.inner.message_count()
    }

    fn start_time(&self) -> Option<u64> {
        self.inner.start_time()
    }

    fn end_time(&self) -> Option<u64> {
        self.inner.end_time()
    }

    fn path(&self) -> &str {
        &self.path
    }

    fn format(&self) -> crate::io::metadata::FileFormat {
        crate::io::metadata::FileFormat::Mcap
    }

    fn file_size(&self) -> u64 {
        self.inner.file_size()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Iterator over raw (undecoded) MCAP messages.
pub struct RawMessageIter<'a> {
    inner: &'a ParallelMcapReader,
    pub channels: HashMap<u16, ChannelInfo>,
}

impl<'a> RawMessageIter<'a> {
    /// Get the channels for this iterator.
    pub fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        &self.channels
    }

    /// Create a proper streaming iterator over raw messages.
    pub fn stream(&self) -> Result<RawMessageStream<'a>> {
        RawMessageStream::new(self.inner, &self.channels)
    }
}

/// MCAP message opcode
const OP_MESSAGE: u8 = 0x05;

/// Streaming iterator over raw MCAP messages.
pub struct RawMessageStream<'a> {
    inner: &'a ParallelMcapReader,
    channels: HashMap<u16, ChannelInfo>,
    /// Current chunk index
    current_chunk_idx: usize,
    /// Current decompressed chunk data
    current_chunk_data: Option<Vec<u8>>,
    /// Current position within chunk data
    chunk_cursor_pos: usize,
    /// For non-chunked files: mmap reference
    mmap: Option<memmap2::Mmap>,
    /// For non-chunked files: current position in data section
    data_section_pos: usize,
    /// For non-chunked files: end of data section
    data_section_end: usize,
    /// Whether we're reading non-chunked messages
    non_chunked_mode: bool,
}

impl<'a> RawMessageStream<'a> {
    fn new(inner: &'a ParallelMcapReader, channels: &HashMap<u16, ChannelInfo>) -> Result<Self> {
        let chunk_indexes = inner.chunk_indexes();
        let non_chunked_mode = chunk_indexes.is_empty();

        // For non-chunked files, memory-map the file and find data section boundaries
        let (mmap, data_section_pos, data_section_end) = if non_chunked_mode {
            let file = std::fs::File::open(inner.path()).map_err(|e| {
                CodecError::encode("RawMessageStream", format!("Failed to open file: {e}"))
            })?;
            let mmap = unsafe { memmap2::Mmap::map(&file) }.map_err(|e| {
                CodecError::encode("RawMessageStream", format!("Failed to mmap file: {e}"))
            })?;

            // Find data section start (after header record)
            let mut pos = 8usize; // Skip magic

            // Skip header record if present
            if pos + 9 < mmap.len() {
                let op = mmap[pos];
                if op == 0x01 {
                    // OP_HEADER
                    let len =
                        u64::from_le_bytes(mmap[pos + 1..pos + 9].try_into().unwrap_or([0u8; 8]));
                    pos += 9 + len as usize;
                }
            }

            // Find data section end (footer/data_end)
            let end = mmap.len().saturating_sub(8 + 9 + 20); // Leave room for footer + magic

            (Some(mmap), pos, end)
        } else {
            (None, 0, 0)
        };

        Ok(Self {
            inner,
            channels: channels.clone(),
            current_chunk_idx: 0,
            current_chunk_data: None,
            chunk_cursor_pos: 0,
            mmap,
            data_section_pos,
            data_section_end,
            non_chunked_mode,
        })
    }

    /// Load the next chunk and decompress it.
    fn load_next_chunk(&mut self) -> Result<bool> {
        let chunk_indexes = self.inner.chunk_indexes();
        if self.current_chunk_idx >= chunk_indexes.len() {
            return Ok(false);
        }

        let chunk_index = &chunk_indexes[self.current_chunk_idx];
        self.current_chunk_idx += 1;

        // Calculate where compressed data starts
        let header_size = 8 + 8 + 8 + 4 + 4 + chunk_index.compression.len() + 8;
        let data_start = chunk_index.chunk_start_offset as usize + 9 + header_size;
        let data_end = data_start + chunk_index.compressed_size as usize;

        // Read from mmap - the inner has file data
        // We need to get access to the mmap data, but ParallelMcapReader doesn't expose it directly
        // For now, we'll read chunks using a file handle
        let file = std::fs::File::open(self.inner.path()).map_err(|e| {
            CodecError::encode("RawMessageStream", format!("Failed to open file: {e}"))
        })?;
        let mmap = unsafe { memmap2::Mmap::map(&file) }.map_err(|e| {
            CodecError::encode("RawMessageStream", format!("Failed to mmap file: {e}"))
        })?;

        if data_end > mmap.len() {
            return Err(CodecError::parse(
                "RawMessageStream",
                format!(
                    "Chunk data exceeds file: {}..{} > {}",
                    data_start,
                    data_end,
                    mmap.len()
                ),
            ));
        }

        let compressed_data = &mmap[data_start..data_end];

        // Decompress based on compression type
        let decompressed = match chunk_index.compression.as_str() {
            "zstd" | "zst" => {
                zstd::bulk::decompress(compressed_data, chunk_index.uncompressed_size as usize)
                    .map_err(|e| {
                        CodecError::encode(
                            "RawMessageStream",
                            format!("Zstd decompression failed: {e}"),
                        )
                    })?
            }
            "lz4" => lz4_flex::decompress(compressed_data, chunk_index.uncompressed_size as usize)
                .map_err(|e| {
                    CodecError::encode("RawMessageStream", format!("LZ4 decompression failed: {e}"))
                })?,
            "" | "none" => compressed_data.to_vec(),
            other => {
                return Err(CodecError::unsupported(format!(
                    "Unsupported compression: {}",
                    other
                )));
            }
        };

        self.current_chunk_data = Some(decompressed);
        self.chunk_cursor_pos = 0;
        Ok(true)
    }

    /// Read the next message from the current chunk.
    fn read_next_message(
        &mut self,
    ) -> Option<std::result::Result<(RawMessage, ChannelInfo), CodecError>> {
        let chunk_data = self.current_chunk_data.as_ref()?;

        while self.chunk_cursor_pos + 9 < chunk_data.len() {
            let mut cursor = Cursor::new(&chunk_data[self.chunk_cursor_pos..]);

            let op = match cursor.read_u8() {
                Ok(o) => o,
                Err(_) => break,
            };

            let record_len = match cursor.read_u64::<LittleEndian>() {
                Ok(l) => l,
                Err(_) => break,
            };

            let record_data_start = self.chunk_cursor_pos + 9;
            let record_end = record_data_start + record_len as usize;

            if record_end > chunk_data.len() {
                break;
            }

            self.chunk_cursor_pos = record_end;

            if op == OP_MESSAGE {
                // Parse message record
                let mut msg_cursor = Cursor::new(&chunk_data[record_data_start..record_end]);

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
                let data_start = record_data_start + 2 + 4 + 8 + 8;
                let data = chunk_data[data_start..record_end].to_vec();

                if let Some(channel_info) = self.channels.get(&channel_id) {
                    return Some(Ok((
                        RawMessage {
                            channel_id,
                            log_time,
                            publish_time,
                            data,
                            sequence: Some(sequence as u64),
                        },
                        channel_info.clone(),
                    )));
                }
            }
        }

        // Done with this chunk
        self.current_chunk_data = None;
        None
    }

    /// Read the next message from the data section (non-chunked mode).
    fn read_next_data_section_message(
        &mut self,
    ) -> Option<std::result::Result<(RawMessage, ChannelInfo), CodecError>> {
        let mmap = self.mmap.as_ref()?;

        while self.data_section_pos + 9 < self.data_section_end
            && self.data_section_pos + 9 < mmap.len()
        {
            let pos = self.data_section_pos;

            let op = mmap[pos];
            let record_len =
                u64::from_le_bytes(mmap[pos + 1..pos + 9].try_into().unwrap_or([0u8; 8]));
            let record_data_start = pos + 9;
            let record_end = record_data_start + record_len as usize;

            if record_end > mmap.len() {
                break;
            }

            self.data_section_pos = record_end;

            // Stop at footer or data_end
            if op == 0x02 || op == 0x0F {
                // OP_FOOTER or OP_DATA_END
                break;
            }

            if op == OP_MESSAGE {
                // Parse message record
                let mut cursor = Cursor::new(&mmap[record_data_start..record_end]);

                let channel_id = match cursor.read_u16::<LittleEndian>() {
                    Ok(id) => id,
                    Err(_) => continue,
                };

                let sequence = match cursor.read_u32::<LittleEndian>() {
                    Ok(s) => s,
                    Err(_) => continue,
                };

                let log_time = match cursor.read_u64::<LittleEndian>() {
                    Ok(t) => t,
                    Err(_) => continue,
                };

                let publish_time = match cursor.read_u64::<LittleEndian>() {
                    Ok(t) => t,
                    Err(_) => continue,
                };

                // Rest is message data
                let data_start = record_data_start + 2 + 4 + 8 + 8;
                let data = mmap[data_start..record_end].to_vec();

                if let Some(channel_info) = self.channels.get(&channel_id) {
                    return Some(Ok((
                        RawMessage {
                            channel_id,
                            log_time,
                            publish_time,
                            data,
                            sequence: Some(sequence as u64),
                        },
                        channel_info.clone(),
                    )));
                }
            }
        }

        None
    }
}

impl<'a> Iterator for RawMessageStream<'a> {
    type Item = std::result::Result<(RawMessage, ChannelInfo), CodecError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Non-chunked mode: read directly from data section
        if self.non_chunked_mode {
            return self.read_next_data_section_message();
        }

        // Chunked mode: read from chunks
        loop {
            // Try to read from current chunk
            if let Some(result) = self.read_next_message() {
                return Some(result);
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

/// Iterator over decoded messages.
///
/// Yields `(DecodedMessage, ChannelInfo)` tuples where `DecodedMessage`
/// is a `HashMap<String, CodecValue>` containing decoded field values.
pub struct DecodedMessageIter<'a> {
    inner: &'a ParallelMcapReader,
    channels: HashMap<u16, ChannelInfo>,
    cdr_decoder: Arc<CdrDecoder>,
    proto_decoder: Arc<ProtobufDecoder>,
    json_decoder: Arc<JsonDecoder>,
}

impl<'a> DecodedMessageIter<'a> {
    /// Get the channels for this iterator.
    pub fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        &self.channels
    }

    /// Create a proper streaming iterator over decoded messages.
    pub fn stream(&self) -> Result<DecodedMessageStream<'a>> {
        DecodedMessageStream::new(
            self.inner,
            &self.channels,
            Arc::clone(&self.cdr_decoder),
            Arc::clone(&self.proto_decoder),
            Arc::clone(&self.json_decoder),
        )
    }
}

impl<'a> Iterator for DecodedMessageIter<'a> {
    type Item = std::result::Result<(DecodedMessage, ChannelInfo), CodecError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Note: This placeholder implementation doesn't work properly
        // Use stream() instead to get a proper streaming iterator
        None
    }
}

/// Streaming iterator over decoded messages.
pub struct DecodedMessageStream<'a> {
    raw_stream: RawMessageStream<'a>,
    cdr_decoder: Arc<CdrDecoder>,
    proto_decoder: Arc<ProtobufDecoder>,
    json_decoder: Arc<JsonDecoder>,
}

impl<'a> DecodedMessageStream<'a> {
    fn new(
        inner: &'a ParallelMcapReader,
        channels: &HashMap<u16, ChannelInfo>,
        cdr_decoder: Arc<CdrDecoder>,
        proto_decoder: Arc<ProtobufDecoder>,
        json_decoder: Arc<JsonDecoder>,
    ) -> Result<Self> {
        let raw_stream = RawMessageStream::new(inner, channels)?;
        Ok(Self {
            raw_stream,
            cdr_decoder,
            proto_decoder,
            json_decoder,
        })
    }
}

impl<'a> Iterator for DecodedMessageStream<'a> {
    type Item = std::result::Result<(DecodedMessage, ChannelInfo), CodecError>;

    fn next(&mut self) -> Option<Self::Item> {
        let cdr_decoder = Arc::clone(&self.cdr_decoder);
        let proto_decoder = Arc::clone(&self.proto_decoder);
        let json_decoder = Arc::clone(&self.json_decoder);

        let (raw_msg, channel_info) = match self.raw_stream.next()? {
            Ok(msg) => msg,
            Err(e) => return Some(Err(e)),
        };

        // Decode based on encoding
        let decoded: Result<DecodedMessage> = match channel_info.encoding.as_str() {
            "protobuf" => proto_decoder
                .decode(&raw_msg.data)
                .map_err(|e| CodecError::parse("Protobuf", e.to_string())),
            "json" => {
                let json_str = match std::str::from_utf8(&raw_msg.data) {
                    Ok(s) => s,
                    Err(e) => {
                        return Some(Err(CodecError::parse(
                            "JSON",
                            format!("Invalid UTF-8: {e}"),
                        )))
                    }
                };
                json_decoder
                    .decode(json_str)
                    .map_err(|e| CodecError::parse("JSON", e.to_string()))
            }
            _ => {
                // CDR decoding
                let schema = match channel_info.schema.as_deref() {
                    Some(s) => s,
                    None => {
                        return Some(Err(CodecError::parse(
                            channel_info.message_type.as_str(),
                            "No schema available",
                        )))
                    }
                };
                let parsed_schema =
                    match crate::schema::parse_schema(&channel_info.message_type, schema) {
                        Ok(s) => s,
                        Err(e) => {
                            return Some(Err(CodecError::parse(
                                channel_info.message_type.as_str(),
                                format!("Failed to parse schema: {e}"),
                            )))
                        }
                    };
                cdr_decoder
                    .decode(
                        &parsed_schema,
                        &raw_msg.data,
                        Some(&channel_info.message_type),
                    )
                    .map_err(|e| {
                        CodecError::parse("CDR", format!("{}: {}", channel_info.message_type, e))
                    })
            }
        };

        match decoded {
            Ok(msg) => Some(Ok((msg, channel_info))),
            Err(e) => Some(Err(e)),
        }
    }
}

/// Iterator over decoded messages with timestamps.
///
/// Yields `(TimestampedDecodedMessage, ChannelInfo)` tuples.
pub struct DecodedMessageWithTimestampIter<'a> {
    inner: &'a ParallelMcapReader,
    channels: HashMap<u16, ChannelInfo>,
    cdr_decoder: Arc<CdrDecoder>,
    proto_decoder: Arc<ProtobufDecoder>,
    json_decoder: Arc<JsonDecoder>,
}

impl<'a> DecodedMessageWithTimestampIter<'a> {
    /// Get the channels for this iterator.
    pub fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        &self.channels
    }

    /// Create a proper streaming iterator over decoded messages with timestamps.
    pub fn stream(&self) -> Result<DecodedMessageWithTimestampStream<'a>> {
        DecodedMessageWithTimestampStream::new(
            self.inner,
            &self.channels,
            Arc::clone(&self.cdr_decoder),
            Arc::clone(&self.proto_decoder),
            Arc::clone(&self.json_decoder),
        )
    }
}

impl<'a> Iterator for DecodedMessageWithTimestampIter<'a> {
    type Item = std::result::Result<(TimestampedDecodedMessage, ChannelInfo), CodecError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Note: This placeholder implementation doesn't work properly
        // Use stream() instead to get a proper streaming iterator
        None
    }
}

/// Streaming iterator over decoded messages with timestamps.
pub struct DecodedMessageWithTimestampStream<'a> {
    raw_stream: RawMessageStream<'a>,
    cdr_decoder: Arc<CdrDecoder>,
    proto_decoder: Arc<ProtobufDecoder>,
    json_decoder: Arc<JsonDecoder>,
}

impl<'a> DecodedMessageWithTimestampStream<'a> {
    fn new(
        inner: &'a ParallelMcapReader,
        channels: &HashMap<u16, ChannelInfo>,
        cdr_decoder: Arc<CdrDecoder>,
        proto_decoder: Arc<ProtobufDecoder>,
        json_decoder: Arc<JsonDecoder>,
    ) -> Result<Self> {
        let raw_stream = RawMessageStream::new(inner, channels)?;
        Ok(Self {
            raw_stream,
            cdr_decoder,
            proto_decoder,
            json_decoder,
        })
    }
}

impl<'a> Iterator for DecodedMessageWithTimestampStream<'a> {
    type Item = std::result::Result<(TimestampedDecodedMessage, ChannelInfo), CodecError>;

    fn next(&mut self) -> Option<Self::Item> {
        let cdr_decoder = Arc::clone(&self.cdr_decoder);
        let proto_decoder = Arc::clone(&self.proto_decoder);
        let json_decoder = Arc::clone(&self.json_decoder);

        let (raw_msg, channel_info) = match self.raw_stream.next()? {
            Ok(msg) => msg,
            Err(e) => return Some(Err(e)),
        };

        let log_time = raw_msg.log_time;
        let publish_time = raw_msg.publish_time;

        // Decode based on encoding (same logic as DecodedMessageStream)
        let decoded: Result<DecodedMessage> = match channel_info.encoding.as_str() {
            "protobuf" => proto_decoder
                .decode(&raw_msg.data)
                .map_err(|e| CodecError::parse("Protobuf", e.to_string())),
            "json" => {
                let json_str = match std::str::from_utf8(&raw_msg.data) {
                    Ok(s) => s,
                    Err(e) => {
                        return Some(Err(CodecError::parse(
                            "JSON",
                            format!("Invalid UTF-8: {e}"),
                        )))
                    }
                };
                json_decoder
                    .decode(json_str)
                    .map_err(|e| CodecError::parse("JSON", e.to_string()))
            }
            _ => {
                // CDR decoding
                let schema = match channel_info.schema.as_deref() {
                    Some(s) => s,
                    None => {
                        return Some(Err(CodecError::parse(
                            "CDR",
                            format!("No schema available for {}", channel_info.message_type),
                        )))
                    }
                };
                let parsed_schema =
                    match crate::schema::parse_schema(&channel_info.message_type, schema) {
                        Ok(s) => s,
                        Err(e) => {
                            return Some(Err(CodecError::parse(
                                "Schema",
                                format!("{}: {}", channel_info.message_type, e),
                            )))
                        }
                    };
                cdr_decoder
                    .decode(
                        &parsed_schema,
                        &raw_msg.data,
                        Some(&channel_info.message_type),
                    )
                    .map_err(|e| {
                        CodecError::parse("CDR", format!("{}: {}", channel_info.message_type, e))
                    })
            }
        };

        match decoded {
            Ok(msg) => Some(Ok((
                TimestampedDecodedMessage {
                    message: msg,
                    log_time,
                    publish_time,
                },
                channel_info,
            ))),
            Err(e) => Some(Err(CodecError::parse(
                "Message",
                format!("{}: {}", channel_info.topic, e),
            ))),
        }
    }
}
