//! Sequential MCAP reader using mcap 0.24 as reference.
//!
//! This module provides `SequentialMcapReader` for reading MCAP files
//! sequentially using the mcap crate as the underlying implementation.

use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

use crate::{CodecError, Result};

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
    /// Message count
    pub message_count: u64,
    /// Schema encoding (e.g., "ros2msg", "protobuf")
    pub schema_encoding: Option<String>,
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
    /// Sequence number
    pub sequence: u32,
}

/// Sequential MCAP reader using mcap 0.24 as the underlying implementation.
pub struct SequentialMcapReader {
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

impl SequentialMcapReader {
    /// Open an MCAP file and read its metadata.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();

        // Use mcap 0.24 to read the file
        let file = File::open(&path_str).map_err(|e| {
            CodecError::encode("SequentialMcapReader", format!("Failed to open file: {e}"))
        })?;

        let mapped = unsafe { memmap2::Mmap::map(&file) }.map_err(|e| {
            CodecError::encode("SequentialMcapReader", format!("Failed to mmap file: {e}"))
        })?;

        // Try to read summary from mcap 0.24
        let summary_opt = match mcap::Summary::read(&mapped) {
            Ok(Some(summary)) => Some(summary),
            Ok(None) => {
                tracing::warn!("MCAP file has no summary section");
                None
            }
            Err(e) => {
                tracing::warn!(
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
                        schema_encoding,
                        message_count: *message_counts.get(id).unwrap_or(&0),
                    },
                );
            }
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

    /// Get all channel information.
    pub fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        &self.channels
    }

    /// Get total message count.
    pub fn message_count(&self) -> u64 {
        self.message_count
    }

    /// Get start timestamp in nanoseconds.
    pub fn start_time(&self) -> Option<u64> {
        self.start_time
    }

    /// Get end timestamp in nanoseconds.
    pub fn end_time(&self) -> Option<u64> {
        self.end_time
    }

    /// Get the file path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Iterate over raw (undecoded) messages in the MCAP file.
    pub fn iter_raw(&self) -> Result<SequentialRawIter> {
        let file = File::open(&self.path).map_err(|e| {
            CodecError::encode("SequentialMcapReader", format!("Failed to open file: {e}"))
        })?;

        let mapped = unsafe { memmap2::Mmap::map(&file) }.map_err(|e| {
            CodecError::encode("SequentialMcapReader", format!("Failed to mmap file: {e}"))
        })?;

        Ok(SequentialRawIter {
            _mapped: mapped,
            channels: self.channels.clone(),
        })
    }
}

/// Iterator over raw (undecoded) MCAP messages.
pub struct SequentialRawIter {
    _mapped: memmap2::Mmap,
    channels: HashMap<u16, ChannelInfo>,
}

impl SequentialRawIter {
    /// Get the channels for this iterator.
    pub fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        &self.channels
    }

    /// Create a proper streaming iterator over raw messages.
    pub fn into_stream(&self) -> Result<SequentialRawStream<'_>> {
        SequentialRawStream::new(&self._mapped, &self.channels)
    }
}

/// Streaming iterator over raw MCAP messages using mcap 0.24.
pub struct SequentialRawStream<'a> {
    channels: HashMap<u16, ChannelInfo>,
    stream: mcap::MessageStream<'a>,
}

impl<'a> SequentialRawStream<'a> {
    fn new(mmap: &'a memmap2::Mmap, channels: &HashMap<u16, ChannelInfo>) -> Result<Self> {
        let stream = mcap::MessageStream::new(mmap).map_err(|e| {
            CodecError::encode(
                "SequentialMcapReader",
                format!("Failed to create stream: {e}"),
            )
        })?;
        Ok(Self {
            channels: channels.clone(),
            stream,
        })
    }
}

impl<'a> Iterator for SequentialRawStream<'a> {
    type Item = std::result::Result<(RawMessage, ChannelInfo), CodecError>;

    fn next(&mut self) -> Option<Self::Item> {
        for result in &mut self.stream {
            let message = match result {
                Ok(m) => m,
                Err(e) => {
                    return Some(Err(CodecError::encode(
                        "SequentialMcapReader",
                        format!("Read error: {e}"),
                    )))
                }
            };

            if let Some(channel_info) = self.channels.get(&message.channel.id) {
                return Some(Ok((
                    RawMessage {
                        channel_id: message.channel.id,
                        log_time: message.log_time,
                        publish_time: message.publish_time,
                        data: message.data.to_vec(),
                        sequence: message.sequence,
                    },
                    channel_info.clone(),
                )));
            }
        }

        None
    }
}
