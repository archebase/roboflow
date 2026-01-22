// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Shared metadata types for all robotics data formats.
//!
//! This module provides unified types for representing metadata that
//! is common across different formats (MCAP, ROS1 bag, etc.).

use std::collections::HashMap;

/// Information about a channel/topic in a robotics data file.
///
/// A channel (also called a "topic" in ROS terminology) represents
/// a named stream of messages of a specific type.
#[derive(Debug, Clone, PartialEq)]
pub struct ChannelInfo {
    /// Unique channel ID within the file
    pub id: u16,
    /// Topic name (e.g., "/joint_states", "/tf")
    pub topic: String,
    /// Message type name (e.g., "sensor_msgs/msg/JointState", "tf2_msgs/TFMessage")
    pub message_type: String,
    /// Encoding format (e.g., "cdr", "protobuf", "json")
    pub encoding: String,
    /// Schema definition (message definition text for ROS messages)
    pub schema: Option<String>,
    /// Schema binary data (e.g., protobuf FileDescriptorSet)
    pub schema_data: Option<Vec<u8>>,
    /// Schema encoding (e.g., "ros2msg", "protobuf", "ros1msg")
    pub schema_encoding: Option<String>,
    /// Number of messages in this channel (0 if unknown)
    pub message_count: u64,
    /// Caller ID - identifies the publishing node (ROS1 specific)
    pub callerid: Option<String>,
}

impl ChannelInfo {
    /// Create a new ChannelInfo.
    pub fn new(id: u16, topic: impl Into<String>, message_type: impl Into<String>) -> Self {
        Self {
            id,
            topic: topic.into(),
            message_type: message_type.into(),
            encoding: String::new(),
            schema: None,
            schema_data: None,
            schema_encoding: None,
            message_count: 0,
            callerid: None,
        }
    }

    /// Set the topic.
    pub fn with_topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = topic.into();
        self
    }

    /// Set the encoding.
    pub fn with_encoding(mut self, encoding: impl Into<String>) -> Self {
        self.encoding = encoding.into();
        self
    }

    /// Set the schema.
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Set the schema data.
    pub fn with_schema_data(mut self, data: Vec<u8>, encoding: impl Into<String>) -> Self {
        self.schema_data = Some(data);
        self.schema_encoding = Some(encoding.into());
        self
    }

    /// Set the message count.
    pub fn with_message_count(mut self, count: u64) -> Self {
        self.message_count = count;
        self
    }

    /// Set the caller ID.
    pub fn with_callerid(mut self, callerid: impl Into<String>) -> Self {
        self.callerid = Some(callerid.into());
        self
    }
}

/// Raw message data with metadata (undecoded).
///
/// This represents a message before decoding, containing only the raw bytes
/// and timing information.
#[derive(Debug, Clone, PartialEq)]
pub struct RawMessage {
    /// Channel ID this message belongs to
    pub channel_id: u16,
    /// Log timestamp (nanoseconds since Unix epoch)
    pub log_time: u64,
    /// Publish timestamp (nanoseconds since Unix epoch)
    pub publish_time: u64,
    /// Raw message data bytes
    pub data: Vec<u8>,
    /// Sequence number (if available from the format)
    pub sequence: Option<u64>,
}

impl RawMessage {
    /// Create a new RawMessage.
    pub fn new(channel_id: u16, log_time: u64, publish_time: u64, data: Vec<u8>) -> Self {
        Self {
            channel_id,
            log_time,
            publish_time,
            data,
            sequence: None,
        }
    }

    /// Set the sequence number.
    pub fn with_sequence(mut self, sequence: u64) -> Self {
        self.sequence = Some(sequence);
        self
    }

    /// Get the data length.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the message has no data.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

/// Metadata about a single message.
///
/// Lightweight version of RawMessage for references into arena data.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MessageMetadata {
    /// Channel ID this message belongs to
    pub channel_id: u16,
    /// Log timestamp (nanoseconds)
    pub log_time: u64,
    /// Publish timestamp (nanoseconds)
    pub publish_time: u64,
    /// Offset of the message data in the file
    pub data_offset: u64,
    /// Length of the message data
    pub data_len: u32,
    /// Sequence number (if available)
    pub sequence: Option<u64>,
}

impl MessageMetadata {
    /// Create a new MessageMetadata.
    pub fn new(
        channel_id: u16,
        log_time: u64,
        publish_time: u64,
        data_offset: u64,
        data_len: u32,
    ) -> Self {
        Self {
            channel_id,
            log_time,
            publish_time,
            data_offset,
            data_len,
            sequence: None,
        }
    }

    /// Get the data range as a tuple.
    pub fn data_range(&self) -> (u64, u64) {
        (self.data_offset, self.data_offset + self.data_len as u64)
    }

    /// Check if the data range is valid for a given file size.
    pub fn is_valid_for_size(&self, file_size: u64) -> bool {
        let (start, end) = self.data_range();
        start < end && end <= file_size
    }
}

/// Information about a robotics data file.
///
/// Provides metadata about the file regardless of its format.
#[derive(Debug, Clone)]
pub struct FileInfo {
    /// File path
    pub path: String,
    /// Detected format
    pub format: FileFormat,
    /// File size in bytes
    pub size: u64,
    /// All channels in the file
    pub channels: HashMap<u16, ChannelInfo>,
    /// Total message count (0 if unknown)
    pub message_count: u64,
    /// Start timestamp (nanoseconds, 0 if unknown)
    pub start_time: u64,
    /// End timestamp (nanoseconds, 0 if unknown)
    pub end_time: u64,
    /// Duration in nanoseconds (0 if unknown)
    pub duration: u64,
}

impl FileInfo {
    /// Create a new FileInfo.
    pub fn new(path: impl Into<String>, format: FileFormat) -> Self {
        Self {
            path: path.into(),
            format,
            size: 0,
            channels: HashMap::new(),
            message_count: 0,
            start_time: 0,
            end_time: 0,
            duration: 0,
        }
    }

    /// Check if the file has a specific topic.
    pub fn has_topic(&self, topic: &str) -> bool {
        self.channels.values().any(|c| c.topic == topic)
    }

    /// Get all channels for a specific topic.
    pub fn channels_for_topic(&self, topic: &str) -> Vec<&ChannelInfo> {
        self.channels
            .values()
            .filter(|c| c.topic == topic)
            .collect()
    }

    /// Get the total number of topics.
    pub fn topic_count(&self) -> usize {
        use std::collections::HashSet;
        self.channels
            .values()
            .map(|c| c.topic.as_str())
            .collect::<HashSet<_>>()
            .len()
    }
}

/// Detected file format.
///
/// Used by the format detection system to identify the type of
/// robotics data file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    /// MCAP format
    Mcap,
    /// ROS1 bag format
    Bag,
    /// Unknown format
    Unknown,
}

impl FileFormat {
    /// Get the file extension for this format.
    pub fn extension(&self) -> &'static str {
        match self {
            FileFormat::Mcap => "mcap",
            FileFormat::Bag => "bag",
            FileFormat::Unknown => "",
        }
    }

    /// Get the default MIME type for this format.
    pub fn mime_type(&self) -> &'static str {
        match self {
            FileFormat::Mcap => "application/x-mcap",
            FileFormat::Bag => "application/x-rosbag",
            FileFormat::Unknown => "application/octet-stream",
        }
    }
}

impl std::fmt::Display for FileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileFormat::Mcap => write!(f, "MCAP"),
            FileFormat::Bag => write!(f, "ROS1 Bag"),
            FileFormat::Unknown => write!(f, "Unknown"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_info_builder() {
        let info = ChannelInfo::new(1, "/test", "std_msgs/String")
            .with_encoding("json")
            .with_schema("string data")
            .with_message_count(100);

        assert_eq!(info.id, 1);
        assert_eq!(info.topic, "/test");
        assert_eq!(info.message_type, "std_msgs/String");
        assert_eq!(info.encoding, "json");
        assert_eq!(info.schema, Some("string data".to_string()));
        assert_eq!(info.message_count, 100);
    }

    #[test]
    fn test_raw_message() {
        let msg = RawMessage::new(1, 1000, 900, b"test data".to_vec()).with_sequence(5);

        assert_eq!(msg.channel_id, 1);
        assert_eq!(msg.log_time, 1000);
        assert_eq!(msg.publish_time, 900);
        assert_eq!(msg.data, b"test data");
        assert_eq!(msg.sequence, Some(5));
        assert_eq!(msg.len(), 9);
    }

    #[test]
    fn test_message_metadata() {
        let meta = MessageMetadata::new(1, 1000, 900, 100, 50);
        assert_eq!(meta.channel_id, 1);
        assert_eq!(meta.data_range(), (100, 150));
        assert!(meta.is_valid_for_size(200));
        assert!(!meta.is_valid_for_size(120));
    }

    #[test]
    fn test_file_info() {
        let mut info = FileInfo::new("test.mcap", FileFormat::Mcap);
        info.size = 1000;
        info.message_count = 500;

        let ch = ChannelInfo::new(1, "/ch1", "std_msgs/String");
        info.channels.insert(1, ch.clone());
        info.channels.insert(
            2,
            ChannelInfo::new(2, "/ch2", "std_msgs/String").with_topic("/ch1"),
        ); // Same topic, different channel

        assert!(info.has_topic("/ch1"));
        assert_eq!(info.channels_for_topic("/ch1").len(), 2);
        assert_eq!(info.topic_count(), 1); // Only one unique topic
    }

    #[test]
    fn test_file_format() {
        assert_eq!(FileFormat::Mcap.extension(), "mcap");
        assert_eq!(FileFormat::Bag.mime_type(), "application/x-rosbag");
        assert_eq!(format!("{}", FileFormat::Mcap), "MCAP");
    }
}
