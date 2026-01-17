//! Format reader abstraction and implementations.
//!
//! This module provides a unified interface for reading different robotics data formats
//! (MCAP, ROS1 bag, etc.) through the `FormatReader` trait.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use tracing::warn;

use crate::format::mcap::{ChannelInfo, RawMessage};
use crate::{CdrDecoder, CodecError, DecodedMessage, JsonDecoder, ProtobufDecoder, Result};
use rosbag::record_types::MessageData;

/// Trait for reading robotics data from different file formats.
///
/// This trait abstracts over format-specific readers (MCAP, ROS1 bag, etc.)
/// to provide a unified API.
pub trait FormatReader: Send + Sync {
    /// Get all channel information.
    fn channels(&self) -> &HashMap<u16, ChannelInfo>;

    /// Get channel info by topic name. Returns the first match.
    ///
    /// Note: In ROS1 bag files, multiple connections can have the same topic name
    /// with different callerids. This method returns only the first match.
    /// Use `channels_by_topic()` to get all channels for a topic.
    fn channel_by_topic(&self, topic: &str) -> Option<&ChannelInfo> {
        self.channels().values().find(|c| c.topic == topic)
    }

    /// Get all channels with the given topic name.
    ///
    /// This is useful for ROS1 bag files where multiple connections can have
    /// the same topic name with different callerids (e.g., `/tf` from different publishers).
    fn channels_by_topic(&self, topic: &str) -> Vec<&ChannelInfo> {
        self.channels()
            .values()
            .filter(|c| c.topic == topic)
            .collect()
    }

    /// Get total message count.
    fn message_count(&self) -> u64;

    /// Get start timestamp in nanoseconds.
    fn start_time(&self) -> Option<u64>;

    /// Get end timestamp in nanoseconds.
    fn end_time(&self) -> Option<u64>;

    /// Get the file path.
    fn path(&self) -> &str;

    /// Create a decoded message stream for this reader.
    fn decode_messages(&self) -> Result<Box<dyn DecodedMessageStream>>;

    /// Get as `Any` for downcasting to concrete types.
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Streaming iterator over decoded messages.
///
/// This trait allows format-specific implementations to provide
/// streaming access to decoded messages.
pub trait DecodedMessageStream:
    Iterator<Item = Result<(DecodedMessage, ChannelInfo)>> + Send
{
}

// Blanket implementation for any type that matches the requirements
impl<T> DecodedMessageStream for T where
    T: Iterator<Item = Result<(DecodedMessage, ChannelInfo)>> + Send
{
}

// =============================================================================
// MCAP Format Reader
// =============================================================================

/// MCAP format reader implementation.
pub struct McapFormatReader {
    /// Path to the MCAP file
    path: String,
    /// Channel information indexed by channel ID
    channels: HashMap<u16, ChannelInfo>,
    /// Total message count
    message_count: u64,
    /// Start timestamp (nanoseconds)
    pub start_time: Option<u64>,
    /// End timestamp (nanoseconds)
    pub end_time: Option<u64>,
}

impl McapFormatReader {
    /// Open an MCAP file and read its metadata.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        let file = std::fs::File::open(&path).map_err(|e| {
            CodecError::encode("McapFormatReader", format!("Failed to open file: {e}"))
        })?;

        let mapped = unsafe { memmap2::Mmap::map(&file) }.map_err(|e| {
            CodecError::encode("McapFormatReader", format!("Failed to mmap file: {e}"))
        })?;

        // Try to read summary, but handle files without summary gracefully
        let summary_result = mcap::Summary::read(&mapped);

        let summary_opt: Option<mcap::Summary> = match summary_result {
            Ok(Some(summary)) => Some(summary),
            Ok(None) => {
                warn!(context = "mcap_reader", "MCAP file has no summary section");
                None
            }
            Err(e) => {
                warn!(
                    context = "mcap_reader",
                    error = %e,
                    "Failed to read summary, attempting to read without summary"
                );
                None
            }
        };

        let mut channels = HashMap::new();
        let mut message_counts: HashMap<u16, u64> = HashMap::new();

        if let Some(summary) = &summary_opt {
            // Count messages per channel
            if let Some(stats) = &summary.stats {
                for (channel_id, count) in &stats.channel_message_counts {
                    message_counts.insert(*channel_id, *count);
                }
            }

            // Build channel info
            for (id, channel) in &summary.channels {
                let schema = channel
                    .schema
                    .as_ref()
                    .and_then(|s| summary.schemas.get(&s.id));

                let schema_text = schema.and_then(|s| String::from_utf8(s.data.to_vec()).ok());
                let schema_data = schema.map(|s| s.data.to_vec());
                let schema_encoding = schema.map(|s| s.encoding.clone());

                channels.insert(
                    *id,
                    ChannelInfo {
                        id: *id,
                        topic: channel.topic.clone(),
                        message_type: channel
                            .schema
                            .as_ref()
                            .map(|s| s.name.clone())
                            .unwrap_or_default(),
                        encoding: channel.message_encoding.clone(),
                        schema: schema_text,
                        schema_data,
                        schema_encoding,
                        message_count: *message_counts.get(id).unwrap_or(&0),
                        callerid: None, // MCAP doesn't have callerid (ROS1-specific)
                    },
                );
            }
        } else {
            warn!(
                context = "mcap_reader",
                "No summary found, file will be scanned during iteration"
            );
        }

        let (start_time, end_time, message_count) = match &summary_opt {
            Some(summary) => match &summary.stats {
                Some(stats) => (
                    Some(stats.message_start_time),
                    Some(stats.message_end_time),
                    stats.message_count,
                ),
                None => (None, None, 0),
            },
            None => (None, None, 0),
        };

        Ok(Self {
            path: path_str,
            channels,
            message_count,
            start_time,
            end_time,
        })
    }
}

impl FormatReader for McapFormatReader {
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

    fn decode_messages(&self) -> Result<Box<dyn DecodedMessageStream>> {
        let file = std::fs::File::open(&self.path).map_err(|e| {
            CodecError::encode("McapFormatReader", format!("Failed to open file: {e}"))
        })?;

        let mapped = unsafe { memmap2::Mmap::map(&file) }.map_err(|e| {
            CodecError::encode("McapFormatReader", format!("Failed to mmap file: {e}"))
        })?;

        Ok(Box::new(McapDecodedMessageStream::new(
            mapped,
            &self.channels,
        )?))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Streaming iterator over decoded MCAP messages.
pub struct McapDecodedMessageStream {
    channels: HashMap<u16, ChannelInfo>,
    cdr_decoder: std::sync::Arc<CdrDecoder>,
    proto_decoder: std::sync::Arc<ProtobufDecoder>,
    json_decoder: std::sync::Arc<JsonDecoder>,
    stream: Option<mcap::MessageStream<'static>>,
    _mmap_owner: Option<std::mem::ManuallyDrop<memmap2::Mmap>>,
    /// Runtime validity flag for the transmuted stream data.
    /// Set to false when _mmap_owner is dropped.
    _mmap_valid: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Count of messages that were skipped due to errors
    pub skipped_count: u64,
}

impl McapDecodedMessageStream {
    fn new(mmap: memmap2::Mmap, channels: &HashMap<u16, ChannelInfo>) -> Result<Self> {
        // SAFETY: Lifetime extension for self-referential struct pattern
        //
        // We're creating a self-referential struct where:
        // - `_mmap_owner` owns the mmap data (lifetime 'a)
        // - `stream` contains references to that data (borrowed with lifetime 'a)
        // - We need to store both in the same struct
        //
        // The Arc dance:
        // 1. Wrap mmap in Arc - this gives us a stable address
        // 2. Create MessageStream borrowing from the Arc (lifetime tied to Arc)
        // 3. Transmute MessageStream<'a> to MessageStream<'static> to satisfy compiler
        // 4. Extract the mmap back from Arc using Arc::into_raw + ptr::read
        // 5. Store both the transmuted stream and the mmap owner
        //
        // Why this is sound:
        // - The stream's borrowed data outlives the stream itself (_mmap_owner guarantees this)
        // - _mmap_valid flag is checked before each stream access (runtime validation)
        // - Drop implementation sets _mmap_valid to false before dropping mmap
        // - No other code has access to the mmap (exclusive ownership via Arc::into_raw)
        //
        // The 'static lifetime is a lie to the compiler - the actual lifetime is tied
        // to _mmap_owner. Runtime validation via _mmap_valid catches misuse.
        let mmap_ptr = std::sync::Arc::new(mmap);
        let mmap_valid = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let stream = unsafe {
            // Create a stream that extends the lifetime
            let stream = mcap::MessageStream::new(&mmap_ptr).map_err(|e| {
                CodecError::encode("McapFormatReader", format!("Failed to create stream: {e}"))
            })?;
            std::mem::transmute::<mcap::MessageStream<'_>, mcap::MessageStream<'static>>(stream)
        };

        Ok(Self {
            channels: channels.clone(),
            cdr_decoder: std::sync::Arc::new(CdrDecoder::new()),
            proto_decoder: std::sync::Arc::new(ProtobufDecoder::new()),
            json_decoder: std::sync::Arc::new(JsonDecoder::new()),
            stream: Some(stream),
            _mmap_owner: Some(std::mem::ManuallyDrop::new(
                // SAFETY: Converting Arc<Mmap> back to Mmap via raw pointer
                //
                // Arc::into_raw consumes the Arc and returns a raw pointer, leaking
                // the reference count. We then use ptr::read to copy the Mmap out.
                //
                // Why this is sound:
                // - We have exclusive ownership of the Arc (strong_count == 1)
                // - Arc::into_raw transfers ownership without dropping
                // - ptr::read performs a bitwise copy of Mmap (which is safe as Mmap
                //   is just a wrapper around a pointer/length)
                // - We store the copied Mmap in _mmap_owner to ensure it's dropped later
                // - The original Arc reference count is deliberately "forgotten" - we
                //   don't call Arc::from_raw because we already extracted the Mmap
                //
                // This effectively "unwraps" the Arc to get the original Mmap back.
                // Since we created the Arc with new(mmap) and had exclusive ownership,
                // no other references exist.
                unsafe { std::ptr::read(Arc::into_raw(mmap_ptr)) },
            )),
            _mmap_valid: mmap_valid,
            skipped_count: 0,
        })
    }
}

impl Drop for McapDecodedMessageStream {
    fn drop(&mut self) {
        // Mark the mmap data as invalid before dropping
        self._mmap_valid
            .store(false, std::sync::atomic::Ordering::Release);
        // Properly drop the mmap
        if let Some(mmap) = self._mmap_owner.take() {
            std::mem::ManuallyDrop::into_inner(mmap);
        }
    }
}

impl Iterator for McapDecodedMessageStream {
    type Item = Result<(DecodedMessage, ChannelInfo)>;

    fn next(&mut self) -> Option<Self::Item> {
        // Runtime validation: ensure the mmap data is still valid
        if !self._mmap_valid.load(std::sync::atomic::Ordering::Acquire) {
            return Some(Err(CodecError::encode(
                "McapDecodedMessageStream",
                "Invariant violation: mmap data was dropped while stream was still active",
            )));
        }

        let stream = match &mut self.stream {
            Some(s) => s,
            None => return None,
        };

        let cdr_decoder = std::sync::Arc::clone(&self.cdr_decoder);
        let proto_decoder = std::sync::Arc::clone(&self.proto_decoder);
        let json_decoder = std::sync::Arc::clone(&self.json_decoder);

        for result in stream {
            let message = match result {
                Ok(m) => m,
                Err(e) => {
                    return Some(Err(CodecError::encode(
                        "McapFormatReader",
                        format!("Read error: {e}"),
                    )))
                }
            };

            if let Some(channel_info) = self.channels.get(&message.channel.id) {
                // Decode based on encoding
                let decoded: Result<DecodedMessage> = match channel_info.encoding.as_str() {
                    "protobuf" => proto_decoder
                        .decode(&message.data)
                        .map_err(|e| CodecError::parse("Protobuf", e.to_string())),
                    "json" => {
                        let json_str = match std::str::from_utf8(&message.data) {
                            Ok(s) => s,
                            Err(e) => {
                                warn!(
                                    context = "json_decode",
                                    error = %e,
                                    channel_id = message.channel.id,
                                    "Invalid UTF-8 in JSON message"
                                );
                                self.skipped_count += 1;
                                continue;
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
                                warn!(
                                    context = "cdr_decode",
                                    message_type = %channel_info.message_type,
                                    "No schema available"
                                );
                                self.skipped_count += 1;
                                continue;
                            }
                        };

                        // Check if this is a ROS1 message (has ros1msg schema encoding)
                        let schema_encoding = channel_info.schema_encoding.as_deref().unwrap_or("");
                        let is_ros1 = schema_encoding.contains("ros1");

                        let parsed_schema =
                            match crate::schema::parser::parse_schema_with_encoding_str(
                                &channel_info.message_type,
                                schema,
                                schema_encoding,
                            ) {
                                Ok(s) => s,
                                Err(e) => {
                                    warn!(
                                        context = "schema_parse",
                                        message_type = %channel_info.message_type,
                                        error = %e,
                                        "Failed to parse schema"
                                    );
                                    self.skipped_count += 1;
                                    continue;
                                }
                            };

                        // Use ROS1 decoder if the schema encoding indicates ROS1
                        if is_ros1 {
                            cdr_decoder
                                .decode_ros1_with_header(
                                    &parsed_schema,
                                    &message.data,
                                    Some(&channel_info.message_type),
                                )
                                .map_err(|e| {
                                    CodecError::parse(
                                        "CDR",
                                        format!("{}: {}", channel_info.message_type, e),
                                    )
                                })
                        } else {
                            cdr_decoder
                                .decode(
                                    &parsed_schema,
                                    &message.data,
                                    Some(&channel_info.message_type),
                                )
                                .map_err(|e| {
                                    CodecError::parse(
                                        "CDR",
                                        format!("{}: {}", channel_info.message_type, e),
                                    )
                                })
                        }
                    }
                };

                match decoded {
                    Ok(msg) => return Some(Ok((msg, channel_info.clone()))),
                    Err(e) => {
                        warn!(
                            context = "message_decode",
                            topic = %channel_info.topic,
                            error = %e,
                            "Failed to decode message"
                        );
                        self.skipped_count += 1;
                        continue;
                    }
                }
            }
        }

        None
    }
}

// =============================================================================
// ROS1 Bag Format Reader
// =============================================================================

/// ROS1 bag format reader implementation.
pub struct BagFormatReader {
    /// Path to the bag file
    path: String,
    /// Channel information indexed by channel ID
    channels: HashMap<u16, ChannelInfo>,
    /// Connection ID mapping (bag conn_id -> internal u16 ID)
    conn_id_map: HashMap<u32, u16>,
    /// Total message count
    message_count: u64,
    /// Start timestamp (nanoseconds)
    pub start_time: Option<u64>,
    /// End timestamp (nanoseconds)
    pub end_time: Option<u64>,
}

impl BagFormatReader {
    /// Open a ROS1 bag file and read its metadata.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        let bag = rosbag::RosBag::new(&path).map_err(|e| {
            CodecError::encode("BagFormatReader", format!("Failed to open bag: {e}"))
        })?;

        let mut channels = HashMap::new();
        let mut conn_id_map = HashMap::new();
        let mut next_channel_id: u16 = 0;

        // Extract connection records from both index and chunk sections
        let mut connections_seen: std::collections::HashSet<u32> = std::collections::HashSet::new();

        // First, collect connections from index section
        let mut _index_conn_count = 0;
        for record in bag.index_records() {
            let record = record.map_err(|e| {
                CodecError::encode("BagFormatReader", format!("Failed to read index: {e}"))
            })?;
            if let rosbag::IndexRecord::Connection(conn) = record {
                if connections_seen.insert(conn.id) {
                    _index_conn_count += 1;
                    let channel_id = next_channel_id;
                    next_channel_id = next_channel_id.wrapping_add(1);

                    channels.insert(
                        channel_id,
                        ChannelInfo {
                            id: channel_id,
                            topic: conn.topic.to_string(),
                            message_type: conn.tp.to_string(),
                            encoding: "cdr".to_string(),
                            schema: Some(conn.message_definition.to_string()),
                            schema_data: None,
                            schema_encoding: Some("ros1msg".to_string()),
                            message_count: 0,
                            callerid: if conn.caller_id.is_empty() {
                                None
                            } else {
                                Some(conn.caller_id.to_string())
                            },
                        },
                    );
                    conn_id_map.insert(conn.id, channel_id);
                }
            }
        }

        // Also check chunk section for any connections not in index
        for record in bag.chunk_records() {
            let record = record.map_err(|e| {
                CodecError::encode("BagFormatReader", format!("Failed to read chunk: {e}"))
            })?;
            if let rosbag::ChunkRecord::Chunk(chunk) = record {
                for msg_result in chunk.messages() {
                    let msg_result = msg_result.map_err(|e| {
                        CodecError::encode(
                            "BagFormatReader",
                            format!("Failed to read message: {e}"),
                        )
                    })?;
                    if let rosbag::MessageRecord::Connection(conn) = msg_result {
                        if connections_seen.insert(conn.id) {
                            let channel_id = next_channel_id;
                            next_channel_id = next_channel_id.wrapping_add(1);

                            channels.insert(
                                channel_id,
                                ChannelInfo {
                                    id: channel_id,
                                    topic: conn.topic.to_string(),
                                    message_type: conn.tp.to_string(),
                                    encoding: "cdr".to_string(),
                                    schema: Some(conn.message_definition.to_string()),
                                    schema_data: None,
                                    schema_encoding: Some("ros1msg".to_string()),
                                    message_count: 0,
                                    callerid: if conn.caller_id.is_empty() {
                                        None
                                    } else {
                                        Some(conn.caller_id.to_string())
                                    },
                                },
                            );
                            conn_id_map.insert(conn.id, channel_id);
                        }
                    }
                }
            }
        }

        Ok(Self {
            path: path_str,
            channels,
            conn_id_map,
            message_count: 0,
            start_time: None,
            end_time: None,
        })
    }
}

impl BagFormatReader {
    /// Get the connection ID to channel ID mapping.
    ///
    /// This maps ROS1 bag connection IDs (u32) to channel IDs (u16).
    pub fn conn_id_map(&self) -> &HashMap<u32, u16> {
        &self.conn_id_map
    }
}

impl FormatReader for BagFormatReader {
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

    fn decode_messages(&self) -> Result<Box<dyn DecodedMessageStream>> {
        let bag = rosbag::RosBag::new(&self.path).map_err(|e| {
            CodecError::encode("BagFormatReader", format!("Failed to open bag: {e}"))
        })?;

        Ok(Box::new(BagDecodedMessageStream {
            channels: self.channels.clone(),
            conn_id_map: self.conn_id_map.clone(),
            cdr_decoder: std::sync::Arc::new(CdrDecoder::new()),
            bag,
            chunk_records: Vec::new(),
            current_messages: None,
            current_index: 0,
            chunk_index: 0,
            _bag_valid: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
            skipped_count: 0,
        }))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Streaming iterator over decoded ROS1 bag messages.
///
/// Note: Due to lifetime constraints of the rosbag crate, this collects
/// chunk records upfront. For large bags, a different approach may be needed.
pub struct BagDecodedMessageStream {
    channels: HashMap<u16, ChannelInfo>,
    conn_id_map: HashMap<u32, u16>,
    cdr_decoder: std::sync::Arc<CdrDecoder>,
    bag: rosbag::RosBag,
    chunk_records: Vec<Vec<MessageData<'static>>>,
    current_messages: Option<Vec<MessageData<'static>>>,
    current_index: usize,
    chunk_index: usize,
    /// Runtime validity flag for the transmuted message data.
    /// Set to false when the bag is dropped.
    _bag_valid: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Count of messages that were skipped due to errors
    pub skipped_count: u64,
}

impl BagDecodedMessageStream {
    /// Load the next chunk's messages.
    fn load_next_chunk(&mut self) -> Result<bool> {
        // Collect all chunk records on first access
        if self.chunk_index == 0 && self.chunk_records.is_empty() {
            for record in self.bag.chunk_records() {
                let record = record.map_err(|e| {
                    CodecError::encode("BagFormatReader", format!("Failed to read record: {e}"))
                })?;

                if let rosbag::ChunkRecord::Chunk(chunk) = record {
                    let mut messages = Vec::new();
                    for msg_result in chunk.messages() {
                        let msg_result = msg_result.map_err(|e| {
                            CodecError::encode(
                                "BagFormatReader",
                                format!("Failed to read message: {e}"),
                            )
                        })?;
                        if let rosbag::MessageRecord::MessageData(msg) = msg_result {
                            // SAFETY: Lifetime extension for MessageData
                            //
                            // MessageData<'_> borrows from the Chunk, which borrows from the RosBag.
                            // The BagDecodedMessageStream owns `self.bag: RosBag`, so the bag data
                            // outlives the stream.
                            //
                            // We transmute MessageData<'a> to MessageData<'static> to store it
                            // in chunk_records. This is sound because:
                            // 1. self.bag owns the underlying data (moved into the struct)
                            // 2. chunk_records is only accessed while self.bag is alive
                            // 3. _bag_valid flag provides runtime validation (checked in next())
                            // 4. Drop impl sets _bag_valid to false before dropping bag
                            //
                            // The 'static lifetime is a lie - actual lifetime is tied to self.bag.
                            // This self-referential pattern is necessary because rosbag::RosBag
                            // doesn't provide a way to iterate with owned data.
                            let extended = unsafe {
                                std::mem::transmute::<MessageData<'_>, MessageData<'static>>(msg)
                            };
                            messages.push(extended);
                        }
                    }
                    if !messages.is_empty() {
                        self.chunk_records.push(messages);
                    }
                }
            }
        }

        if self.chunk_index >= self.chunk_records.len() {
            return Ok(false);
        }

        self.current_messages = Some(self.chunk_records[self.chunk_index].clone());
        self.current_index = 0;
        self.chunk_index += 1;
        Ok(true)
    }
}

impl Drop for BagDecodedMessageStream {
    fn drop(&mut self) {
        // Mark the bag data as invalid before dropping
        self._bag_valid
            .store(false, std::sync::atomic::Ordering::Release);
    }
}

impl Iterator for BagDecodedMessageStream {
    type Item = Result<(DecodedMessage, ChannelInfo)>;

    fn next(&mut self) -> Option<Self::Item> {
        // Runtime validation: ensure the bag data is still valid
        if !self._bag_valid.load(std::sync::atomic::Ordering::Acquire) {
            return Some(Err(CodecError::encode(
                "BagDecodedMessageStream",
                "Invariant violation: bag data was dropped while stream was still active",
            )));
        }

        loop {
            // Load current messages if needed
            if self.current_messages.is_none() {
                match self.load_next_chunk() {
                    Ok(true) => continue,
                    Ok(false) => return None,
                    Err(e) => return Some(Err(e)),
                }
            }

            let messages = self.current_messages.as_ref().unwrap();
            if self.current_index >= messages.len() {
                self.current_messages = None;
                continue;
            }

            let msg_data = &messages[self.current_index];
            self.current_index += 1;

            // Map connection ID to channel ID
            let channel_id = match self.conn_id_map.get(&msg_data.conn_id) {
                Some(&id) => id,
                None => {
                    warn!(
                        context = "bag_stream",
                        conn_id = msg_data.conn_id,
                        "Unknown connection ID"
                    );
                    self.skipped_count += 1;
                    continue;
                }
            };

            let channel_info = match self.channels.get(&channel_id) {
                Some(info) => info,
                None => {
                    warn!(context = "bag_stream", channel_id, "Channel ID not found");
                    self.skipped_count += 1;
                    continue;
                }
            };

            // Decode CDR data
            let schema = match channel_info.schema.as_deref() {
                Some(s) => s,
                None => {
                    warn!(
                        context = "bag_stream",
                        message_type = %channel_info.message_type,
                        "No schema available"
                    );
                    self.skipped_count += 1;
                    continue;
                }
            };

            let parsed_schema = match crate::schema::parser::parse_schema_with_encoding_str(
                &channel_info.message_type,
                schema,
                channel_info.schema_encoding.as_deref().unwrap_or("ros1msg"),
            ) {
                Ok(s) => s,
                Err(e) => {
                    warn!(
                        context = "bag_stream",
                        message_type = %channel_info.message_type,
                        error = %e,
                        "Failed to parse schema"
                    );
                    self.skipped_count += 1;
                    continue;
                }
            };

            // Timestamp is already in nanoseconds
            let _time_ns = msg_data.time;

            // Choose decoder based on encoding
            // - "cdr" uses standard CDR decoding (for files we wrote)
            // - "ros1msg" uses ROS1-specific decoding with header skip
            let decoded = if channel_info.encoding == "cdr" {
                self.cdr_decoder.decode(
                    &parsed_schema,
                    msg_data.data,
                    Some(&channel_info.message_type),
                )
            } else {
                // ROS1 bags have a 12-byte record header followed by CDR data
                self.cdr_decoder.decode_ros1_with_header(
                    &parsed_schema,
                    msg_data.data,
                    Some(&channel_info.message_type),
                )
            };

            match decoded {
                Ok(decoded) => return Some(Ok((decoded, channel_info.clone()))),
                Err(e) => {
                    warn!(
                        context = "bag_stream",
                        topic = %channel_info.topic,
                        error = %e,
                        "Failed to decode message"
                    );
                    self.skipped_count += 1;
                    continue;
                }
            }
        }
    }
}

// =============================================================================
// Bag Raw Message Iterator
// =============================================================================

/// Raw message iterator for ROS1 bag files.
///
/// Similar to MCAP's RawMessageIter, provides access to undecoded message data.
pub struct BagRawMessageIter {
    /// Path to the bag file
    pub path: String,
    /// Channel information indexed by channel ID
    pub channels: HashMap<u16, ChannelInfo>,
    /// Connection ID to channel ID mapping
    conn_id_map: HashMap<u32, u16>,
}

impl BagRawMessageIter {
    /// Create a new raw message iterator for a bag file.
    pub fn new(
        path: String,
        channels: HashMap<u16, ChannelInfo>,
        conn_id_map: HashMap<u32, u16>,
    ) -> Self {
        Self {
            path,
            channels,
            conn_id_map,
        }
    }

    /// Get the channels for this iterator.
    pub fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        &self.channels
    }

    /// Create a streaming iterator over raw messages.
    pub fn into_stream(&self) -> Result<BagRawMessageStream> {
        BagRawMessageStream::new(&self.path, &self.channels, &self.conn_id_map)
    }
}

/// Streaming iterator over raw ROS1 bag messages.
///
/// Yields `(RawMessage, ChannelInfo)` tuples where RawMessage contains
/// the undecoded message data (with ROS record headers intact).
pub struct BagRawMessageStream {
    bag: rosbag::RosBag,
    channels: HashMap<u16, ChannelInfo>,
    conn_id_map: HashMap<u32, u16>,
    chunk_records: Vec<Vec<MessageData<'static>>>,
    current_messages: Option<Vec<MessageData<'static>>>,
    current_index: usize,
    chunk_index: usize,
}

impl BagRawMessageStream {
    /// Create a new raw message stream.
    fn new(
        path: &str,
        channels: &HashMap<u16, ChannelInfo>,
        conn_id_map: &HashMap<u32, u16>,
    ) -> Result<Self> {
        let bag = rosbag::RosBag::new(Path::new(path)).map_err(|e| {
            CodecError::encode("BagRawMessageStream", format!("Failed to open bag: {e}"))
        })?;

        Ok(Self {
            bag,
            channels: channels.clone(),
            conn_id_map: conn_id_map.clone(),
            chunk_records: Vec::new(),
            current_messages: None,
            current_index: 0,
            chunk_index: 0,
        })
    }

    /// Load the next chunk's messages.
    fn load_next_chunk(&mut self) -> Result<bool> {
        // Collect all chunk records on first access
        if self.chunk_index == 0 && self.chunk_records.is_empty() {
            for record in self.bag.chunk_records() {
                let record = record.map_err(|e| {
                    CodecError::encode("BagRawMessageStream", format!("Failed to read record: {e}"))
                })?;

                if let rosbag::ChunkRecord::Chunk(chunk) = record {
                    let mut messages = Vec::new();
                    for msg_result in chunk.messages() {
                        let msg_result = msg_result.map_err(|e| {
                            CodecError::encode(
                                "BagRawMessageStream",
                                format!("Failed to read message: {e}"),
                            )
                        })?;
                        if let rosbag::MessageRecord::MessageData(msg) = msg_result {
                            // SAFETY: Lifetime extension for MessageData
                            //
                            // MessageData<'_> borrows from the Chunk, which borrows from the RosBag.
                            // The BagRawMessageStream owns `self.bag: RosBag`, so the bag data
                            // outlives the stream.
                            //
                            // We transmute MessageData<'a> to MessageData<'static> to store it
                            // in chunk_records. This is sound because:
                            // 1. self.bag owns the underlying data (moved into the struct)
                            // 2. chunk_records is only accessed while self.bag is alive
                            // 3. The stream is never used after bag is dropped (Drop order guarantees this)
                            //
                            // The 'static lifetime is a lie - actual lifetime is tied to self.bag.
                            // This self-referential pattern is necessary because rosbag::RosBag
                            // doesn't provide a way to iterate with owned data.
                            let extended = unsafe {
                                std::mem::transmute::<MessageData<'_>, MessageData<'static>>(msg)
                            };
                            messages.push(extended);
                        }
                    }
                    if !messages.is_empty() {
                        self.chunk_records.push(messages);
                    }
                }
            }
        }

        if self.chunk_index >= self.chunk_records.len() {
            return Ok(false);
        }

        self.current_messages = Some(self.chunk_records[self.chunk_index].clone());
        self.current_index = 0;
        self.chunk_index += 1;
        Ok(true)
    }
}

impl Iterator for BagRawMessageStream {
    type Item = Result<(RawMessage, ChannelInfo)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Load current messages if needed
            if self.current_messages.is_none() {
                match self.load_next_chunk() {
                    Ok(true) => continue,
                    Ok(false) => return None,
                    Err(e) => return Some(Err(e)),
                }
            }

            let messages = self.current_messages.as_ref().unwrap();
            if self.current_index >= messages.len() {
                self.current_messages = None;
                continue;
            }

            let msg_data = &messages[self.current_index];
            self.current_index += 1;

            // Get channel ID from connection ID
            let channel_id = match self.conn_id_map.get(&msg_data.conn_id) {
                Some(&id) => id,
                None => {
                    warn!(
                        conn_id = msg_data.conn_id,
                        "Connection ID not found in map, skipping message"
                    );
                    continue;
                }
            };

            // Get channel info
            let channel_info = match self.channels.get(&channel_id) {
                Some(info) => info.clone(),
                None => {
                    warn!(
                        channel_id,
                        "Channel info not found for channel_id, skipping message"
                    );
                    continue;
                }
            };

            // Return raw message with full data (including ROS headers)
            // ROS1 bag message data includes:
            // - 4 bytes: sequence number
            // - 8 bytes: timestamp
            // - 4 bytes: CDR header
            // - Remaining bytes: actual CDR data
            //
            // NOTE: We use to_vec() to copy the data because the slice comes from
            // transmuted lifetime extension.
            let data: Vec<u8> = msg_data.data.to_vec();
            return Some(Ok((
                RawMessage {
                    channel_id,
                    log_time: msg_data.time,
                    publish_time: msg_data.time,
                    data,
                    sequence: None,
                },
                channel_info,
            )));
        }
    }
}

// =============================================================================
// Unified Raw Message Iterator
// =============================================================================

/// Unified raw message iterator that works for both MCAP and BAG formats.
pub enum RoboCodecRawMessageIter {
    /// MCAP format iterator
    Mcap(crate::format::mcap::RawMessageIter),
    /// ROS1 bag format iterator
    Bag(BagRawMessageIter),
}

impl RoboCodecRawMessageIter {
    /// Get the channels for this iterator.
    pub fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        match self {
            Self::Mcap(iter) => iter.channels(),
            Self::Bag(iter) => iter.channels(),
        }
    }

    /// Convert this iterator into a stream.
    pub fn into_stream(self) -> Result<RoboCodecRawMessageStream<'static>> {
        match self {
            Self::Mcap(_iter) => {
                // For MCAP, we need to use the stream from the iterator
                // But the iterator doesn't have access to the mapped data
                // This is a limitation - users should use the specific reader's stream methods
                Err(CodecError::encode(
                    "RoboCodecRawMessageIter",
                    "into_stream() not supported for MCAP through unified interface. Use iter_raw_mcap() directly.",
                ))
            }
            Self::Bag(iter) => Ok(RoboCodecRawMessageStream::Bag(iter.into_stream()?)),
        }
    }
}

/// Unified raw message stream that works for both MCAP and BAG formats.
///
/// NOTE: The streaming variant has lifetime issues and is currently not implemented.
/// Use the `RoboCodecRawMessageIter` directly instead.
pub enum RoboCodecRawMessageStream<'a> {
    /// MCAP format stream (owns the mmap data)
    Mcap(Box<crate::format::mcap::RawMessageStream<'a>>),
    /// ROS1 bag format stream
    Bag(BagRawMessageStream),
}

impl<'a> Iterator for RoboCodecRawMessageStream<'a> {
    type Item = Result<(crate::format::mcap::RawMessage, ChannelInfo)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Mcap(stream) => stream.next(),
            Self::Bag(stream) => stream.next(),
        }
    }
}

// =============================================================================
// RoboReader Facade
// =============================================================================

/// Unified robotics data reader.
///
/// `RoboReader` provides a single API for reading robotics data from different
/// file formats (MCAP, ROS1 bag, etc.). Format is detected from file extension.
pub struct RoboReader {
    inner: Box<dyn FormatReader>,
}

impl RoboReader {
    /// Open a robotics data file, detecting format from extension.
    ///
    /// Supported formats:
    /// - `.mcap` - MCAP files
    /// - `.bag` - ROS1 bag files
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let extension = path_ref
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();

        let inner: Box<dyn FormatReader> = match extension.as_str() {
            "mcap" => Box::new(McapFormatReader::open(path_ref)?),
            "bag" => Box::new(BagFormatReader::open(path_ref)?),
            _ => {
                return Err(CodecError::encode(
                    "RoboReader",
                    format!("Unknown file format: '.{extension}'. Supported: .mcap, .bag",),
                ))
            }
        };

        Ok(Self { inner })
    }

    /// Get all channel information.
    pub fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        self.inner.channels()
    }

    /// Get channel info by topic name. Returns the first match.
    ///
    /// Note: In ROS1 bag files, multiple connections can have the same topic name
    /// with different callerids. This method returns only the first match.
    /// Use `channels_by_topic()` to get all channels for a topic.
    pub fn channel_by_topic(&self, topic: &str) -> Option<&ChannelInfo> {
        self.inner.channel_by_topic(topic)
    }

    /// Get all channels with the given topic name.
    ///
    /// This is useful for ROS1 bag files where multiple connections can have
    /// the same topic name with different callerids (e.g., `/tf` from different publishers).
    pub fn channels_by_topic(&self, topic: &str) -> Vec<&ChannelInfo> {
        self.inner.channels_by_topic(topic)
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
        self.inner.path()
    }

    /// Iterate over decoded messages.
    ///
    /// This is the primary API for consuming robotics data. Encoding detection
    /// and decoding happen automatically based on channel metadata.
    ///
    /// # Returns
    ///
    /// An iterator yielding `(DecodedMessage, ChannelInfo)` tuples.
    pub fn decode_messages(&self) -> Result<Box<dyn DecodedMessageStream>> {
        self.inner.decode_messages()
    }

    /// Iterate over raw (undecoded) messages for MCAP files.
    ///
    /// This method only works for MCAP files. Returns an error for other formats.
    pub fn iter_raw_mcap(&self) -> Result<crate::format::mcap::RawMessageIter> {
        let path = self.path();
        let extension = std::path::Path::new(path)
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();

        if extension != "mcap" {
            return Err(CodecError::encode(
                "RoboReader",
                format!("iter_raw_mcap only supports MCAP files, got: .{extension}"),
            ));
        }

        // Reopen as MCAP reader to get raw iterator
        let mcap_reader = McapFormatReader::open(path)?;
        let file = std::fs::File::open(path)
            .map_err(|e| CodecError::encode("RoboReader", format!("Failed to open file: {e}")))?;

        let mapped = unsafe { memmap2::Mmap::map(&file) }
            .map_err(|e| CodecError::encode("RoboReader", format!("Failed to mmap file: {e}")))?;

        Ok(crate::format::mcap::RawMessageIter {
            _mapped: mapped,
            channels: mcap_reader.channels.clone(),
        })
    }

    /// Iterate over raw (undecoded) messages for ROS1 bag files.
    ///
    /// This method only works for ROS1 bag files. Returns an error for other formats.
    pub fn iter_raw_bag(&self) -> Result<BagRawMessageIter> {
        let path = self.path();
        let extension = std::path::Path::new(path)
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();

        if extension != "bag" {
            return Err(CodecError::encode(
                "RoboReader",
                format!("iter_raw_bag only supports ROS1 bag files, got: .{extension}"),
            ));
        }

        // Try to downcast to BagFormatReader
        let bag_reader = self.inner.as_any().downcast_ref::<BagFormatReader>();
        let (channels, conn_id_map) = match bag_reader {
            Some(r) => (r.channels.clone(), r.conn_id_map.clone()),
            None => {
                // Reopen the bag file
                let r = BagFormatReader::open(std::path::Path::new(path))?;
                (r.channels.clone(), r.conn_id_map.clone())
            }
        };

        Ok(BagRawMessageIter::new(
            path.to_string(),
            channels,
            conn_id_map,
        ))
    }

    /// Iterate over raw (undecoded) messages for any supported format.
    ///
    /// This is the unified API that works for both MCAP and ROS1 bag files.
    /// Format is automatically detected from the file extension.
    ///
    /// # Returns
    ///
    /// A `RoboCodecRawMessageIter` that can be converted to a stream via `into_stream()`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let reader = RoboReader::open("data.mcap")?;
    /// let iter = reader.iter_raw()?;
    /// let stream = iter.into_stream()?;
    /// for result in stream {
    ///     let (msg, channel_info) = result?;
    ///     // Process raw message...
    /// }
    /// ```
    pub fn iter_raw(&self) -> Result<RoboCodecRawMessageIter> {
        let path = self.path();
        let extension = std::path::Path::new(path)
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();

        match extension.as_str() {
            "mcap" => Ok(RoboCodecRawMessageIter::Mcap(self.iter_raw_mcap()?)),
            "bag" => Ok(RoboCodecRawMessageIter::Bag(self.iter_raw_bag()?)),
            _ => Err(CodecError::encode(
                "RoboReader",
                format!(
                    "Unsupported file format for iter_raw: '.{extension}'. Supported: .mcap, .bag"
                ),
            )),
        }
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
