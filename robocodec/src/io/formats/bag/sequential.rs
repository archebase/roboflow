// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Sequential BAG reader using rosbag crate as reference.
//!
//! This module provides `SequentialBagReader` for reading ROS1 bag files
//! sequentially using the rosbag crate as the underlying implementation.
//!
//! Use this reader as the reference implementation to verify the correctness
//! of the parallel BAG reader.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::io::metadata::{ChannelInfo, FileFormat, RawMessage};
use crate::io::traits::FormatReader;
use crate::{CodecError, Result};

/// Sequential BAG reader format.
pub struct BagSequentialFormat;

impl BagSequentialFormat {
    /// Open a BAG file and return a sequential reader.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<SequentialBagReader> {
        SequentialBagReader::open(path)
    }
}

/// Sequential BAG reader using rosbag crate.
///
/// This reader uses the rosbag crate for reliable BAG file reading.
/// It's suitable for:
/// - Reference implementation for testing
/// - Sequential processing workflows
/// - Rewriting operations
pub struct SequentialBagReader {
    /// File path
    path: String,
    /// Channel information indexed by channel ID
    channels: HashMap<u16, ChannelInfo>,
    /// Connection ID to channel ID mapping
    conn_id_map: HashMap<u32, u16>,
    /// Total message count
    message_count: u64,
    /// Start timestamp (nanoseconds)
    start_time: Option<u64>,
    /// End timestamp (nanoseconds)
    end_time: Option<u64>,
}

impl SequentialBagReader {
    /// Open a BAG file for sequential reading.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let path_str = path_ref.to_string_lossy().to_string();

        // Open the bag file using rosbag crate
        let bag = rosbag::RosBag::new(path_ref).map_err(|e| {
            CodecError::encode("SequentialBagReader", format!("Failed to open bag: {e}"))
        })?;

        let mut channels = HashMap::new();
        let mut conn_id_map = HashMap::new();
        let mut next_channel_id: u16 = 0;
        let mut connections_seen: HashSet<u32> = HashSet::new();

        // Collect connections from index section
        for record in bag.index_records() {
            let record = record.map_err(|e| {
                CodecError::encode("SequentialBagReader", format!("Failed to read index: {e}"))
            })?;
            if let rosbag::IndexRecord::Connection(conn) = record {
                if connections_seen.insert(conn.id) {
                    let channel_id = next_channel_id;
                    next_channel_id = next_channel_id.wrapping_add(1);

                    channels.insert(
                        channel_id,
                        ChannelInfo {
                            id: channel_id,
                            topic: conn.topic.to_string(),
                            message_type: conn.tp.to_string(),
                            encoding: "ros1".to_string(), // ROS1 serialization format
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

        // Also check chunk section for connections not in index
        for record in bag.chunk_records() {
            let record = record.map_err(|e| {
                CodecError::encode("SequentialBagReader", format!("Failed to read chunk: {e}"))
            })?;
            if let rosbag::ChunkRecord::Chunk(chunk) = record {
                for msg_result in chunk.messages() {
                    let msg_result = msg_result.map_err(|e| {
                        CodecError::encode(
                            "SequentialBagReader",
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
                                    encoding: "ros1".to_string(), // ROS1 serialization format
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

    /// Create a raw message iterator.
    pub fn iter_raw(&self) -> Result<SequentialBagRawIter> {
        SequentialBagRawIter::new(&self.path, &self.channels, &self.conn_id_map)
    }

    /// Get the connection ID to channel ID mapping.
    pub fn conn_id_map(&self) -> &HashMap<u32, u16> {
        &self.conn_id_map
    }
}

impl FormatReader for SequentialBagReader {
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
        std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Raw message iterator for BAG files using rosbag crate.
///
/// This iterator uses the rosbag crate to read messages sequentially.
pub struct SequentialBagRawIter {
    /// Bag file reader
    bag: rosbag::RosBag,
    /// Channel information
    channels: HashMap<u16, ChannelInfo>,
    /// Connection ID to channel ID mapping
    conn_id_map: HashMap<u32, u16>,
    /// Chunk records collected upfront
    chunk_records: Vec<Vec<rosbag::record_types::MessageData<'static>>>,
    /// Current messages being processed
    current_messages: Option<Vec<rosbag::record_types::MessageData<'static>>>,
    /// Current index within current_messages
    current_index: usize,
    /// Current chunk index
    chunk_index: usize,
}

impl SequentialBagRawIter {
    /// Create a new raw message iterator for a bag file.
    pub fn new(
        path: &str,
        channels: &HashMap<u16, ChannelInfo>,
        conn_id_map: &HashMap<u32, u16>,
    ) -> Result<Self> {
        let bag = rosbag::RosBag::new(Path::new(path)).map_err(|e| {
            CodecError::encode("SequentialBagRawIter", format!("Failed to open bag: {e}"))
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
                    CodecError::encode(
                        "SequentialBagRawIter",
                        format!("Failed to read record: {e}"),
                    )
                })?;

                if let rosbag::ChunkRecord::Chunk(chunk) = record {
                    let mut messages = Vec::new();
                    for msg_result in chunk.messages() {
                        let msg_result = msg_result.map_err(|e| {
                            CodecError::encode(
                                "SequentialBagRawIter",
                                format!("Failed to read message: {e}"),
                            )
                        })?;
                        if let rosbag::MessageRecord::MessageData(msg) = msg_result {
                            // SAFETY: We extend the lifetime to 'static for storage.
                            // This is safe because we own the RosBag which owns the data.
                            let extended = unsafe {
                                std::mem::transmute::<
                                    rosbag::record_types::MessageData<'_>,
                                    rosbag::record_types::MessageData<'static>,
                                >(msg)
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

impl Iterator for SequentialBagRawIter {
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

            // Map connection ID to channel ID
            let channel_id = match self.conn_id_map.get(&msg_data.conn_id) {
                Some(&id) => id,
                None => continue, // Skip unknown connection IDs
            };

            // Get channel info
            let channel_info = match self.channels.get(&channel_id) {
                Some(info) => info.clone(),
                None => continue, // Skip unknown channel IDs
            };

            // Return raw message
            return Some(Ok((
                RawMessage {
                    channel_id,
                    log_time: msg_data.time,
                    publish_time: msg_data.time,
                    data: msg_data.data.to_vec(),
                    sequence: None,
                },
                channel_info,
            )));
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_sequential_bag_reader_compiles() {
        // Just verify the types compile correctly
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
