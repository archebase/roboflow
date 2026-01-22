// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Sequential MCAP reader using the mcap crate.
//!
//! This module provides a simple sequential reader that uses the mcap crate
//! for reliable MCAP file reading. It's suitable for:
//! - Files without summary sections
//! - Sequential processing workflows
//! - Rewriting operations
//!
//! For parallel reading with chunk-based processing, use `ParallelMcapReader`.

use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

use tracing::warn;

use crate::io::metadata::{ChannelInfo, FileFormat, RawMessage};
use crate::io::traits::FormatReader;
use crate::{CodecError, Result};

/// Sequential MCAP reader using the mcap crate.
///
/// This reader uses memory-mapping and the mcap crate's MessageStream
/// for sequential message iteration. It's reliable and works with
/// all valid MCAP files, including those without summary sections.
pub struct SequentialMcapReader {
    /// File path
    path: String,
    /// Memory-mapped file
    mmap: memmap2::Mmap,
    /// Channel information indexed by channel ID
    channels: HashMap<u16, ChannelInfo>,
    /// Total message count (from summary, 0 if no summary)
    message_count: u64,
    /// Start timestamp (nanoseconds)
    start_time: Option<u64>,
    /// End timestamp (nanoseconds)
    end_time: Option<u64>,
    /// File size
    file_size: u64,
}

impl SequentialMcapReader {
    /// Open an MCAP file for sequential reading.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let path_str = path_ref.to_string_lossy().to_string();

        let file = File::open(path_ref).map_err(|e| {
            CodecError::encode("SequentialMcapReader", format!("Failed to open file: {e}"))
        })?;

        let file_size = file
            .metadata()
            .map_err(|e| {
                CodecError::encode(
                    "SequentialMcapReader",
                    format!("Failed to get metadata: {e}"),
                )
            })?
            .len();

        let mmap = unsafe { memmap2::Mmap::map(&file) }.map_err(|e| {
            CodecError::encode("SequentialMcapReader", format!("Failed to mmap file: {e}"))
        })?;

        // Try to read summary for metadata
        let summary_result = mcap::Summary::read(&mmap);

        let (channels, message_count, start_time, end_time) = match summary_result {
            Ok(Some(summary)) => {
                let mut channels = HashMap::new();
                let mut message_counts: HashMap<u16, u64> = HashMap::new();

                // Count messages per channel from stats
                if let Some(stats) = &summary.stats {
                    for (channel_id, count) in &stats.channel_message_counts {
                        message_counts.insert(*channel_id, *count);
                    }
                }

                // Build channel info from summary
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
                            callerid: None,
                        },
                    );
                }

                let (start, end, count) = match &summary.stats {
                    Some(stats) => (
                        Some(stats.message_start_time),
                        Some(stats.message_end_time),
                        stats.message_count,
                    ),
                    None => (None, None, 0),
                };

                (channels, count, start, end)
            }
            Ok(None) => {
                warn!(
                    context = "SequentialMcapReader",
                    "MCAP file has no summary section, scanning for channels"
                );
                // Scan for channels by iterating through messages
                let channels = Self::scan_channels(&mmap)?;
                (channels, 0, None, None)
            }
            Err(e) => {
                warn!(
                    context = "SequentialMcapReader",
                    error = %e,
                    "Failed to read summary, scanning for channels"
                );
                let channels = Self::scan_channels(&mmap)?;
                (channels, 0, None, None)
            }
        };

        Ok(Self {
            path: path_str,
            mmap,
            channels,
            message_count,
            start_time,
            end_time,
            file_size,
        })
    }

    /// Scan the file to build channel information when no summary is available.
    fn scan_channels(mmap: &memmap2::Mmap) -> Result<HashMap<u16, ChannelInfo>> {
        let mut channels = HashMap::new();

        let stream = mcap::MessageStream::new(mmap).map_err(|e| {
            CodecError::encode(
                "SequentialMcapReader",
                format!("Failed to create message stream: {e}"),
            )
        })?;

        for result in stream {
            let message = match result {
                Ok(m) => m,
                Err(e) => {
                    warn!(
                        context = "SequentialMcapReader",
                        error = %e,
                        "Error reading message during channel scan"
                    );
                    continue;
                }
            };

            let channel_id = message.channel.id;
            if let std::collections::hash_map::Entry::Vacant(e) = channels.entry(channel_id) {
                let schema = message.channel.schema.as_ref();
                let schema_text = schema.and_then(|s| String::from_utf8(s.data.to_vec()).ok());
                let schema_data = schema.map(|s| s.data.to_vec());
                let schema_encoding = schema.map(|s| s.encoding.clone());

                e.insert(ChannelInfo {
                    id: channel_id,
                    topic: message.channel.topic.clone(),
                    message_type: schema.map(|s| s.name.clone()).unwrap_or_default(),
                    encoding: message.channel.message_encoding.clone(),
                    schema: schema_text,
                    schema_data,
                    schema_encoding,
                    message_count: 0,
                    callerid: None,
                });
            }
        }

        Ok(channels)
    }

    /// Create a raw message iterator for sequential reading.
    pub fn iter_raw(&self) -> Result<SequentialRawIter<'_>> {
        SequentialRawIter::new(&self.mmap, &self.channels)
    }

    /// Get the memory-mapped data.
    pub fn mmap(&self) -> &memmap2::Mmap {
        &self.mmap
    }
}

impl FormatReader for SequentialMcapReader {
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

/// Sequential raw message iterator using the mcap crate.
pub struct SequentialRawIter<'a> {
    channels: HashMap<u16, ChannelInfo>,
    stream: mcap::MessageStream<'a>,
}

impl<'a> SequentialRawIter<'a> {
    fn new(mmap: &'a memmap2::Mmap, channels: &HashMap<u16, ChannelInfo>) -> Result<Self> {
        let stream = mcap::MessageStream::new(mmap).map_err(|e| {
            CodecError::encode(
                "SequentialRawIter",
                format!("Failed to create message stream: {e}"),
            )
        })?;

        Ok(Self {
            channels: channels.clone(),
            stream,
        })
    }

    /// Get the channels.
    pub fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        &self.channels
    }
}

impl<'a> Iterator for SequentialRawIter<'a> {
    type Item = Result<(RawMessage, ChannelInfo)>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(result) = self.stream.next() {
            let message = match result {
                Ok(m) => m,
                Err(e) => {
                    return Some(Err(CodecError::encode(
                        "SequentialRawIter",
                        format!("Read error: {e}"),
                    )))
                }
            };

            let channel_id = message.channel.id;

            // Get or create channel info
            let channel_info = if let Some(info) = self.channels.get(&channel_id) {
                info.clone()
            } else {
                // Channel not in our map, create one from the message
                let schema = message.channel.schema.as_ref();
                ChannelInfo {
                    id: channel_id,
                    topic: message.channel.topic.clone(),
                    message_type: schema.map(|s| s.name.clone()).unwrap_or_default(),
                    encoding: message.channel.message_encoding.clone(),
                    schema: schema.and_then(|s| String::from_utf8(s.data.to_vec()).ok()),
                    schema_data: schema.map(|s| s.data.to_vec()),
                    schema_encoding: schema.map(|s| s.encoding.clone()),
                    message_count: 0,
                    callerid: None,
                }
            };

            return Some(Ok((
                RawMessage {
                    channel_id,
                    log_time: message.log_time,
                    publish_time: message.publish_time,
                    data: message.data.to_vec(),
                    sequence: Some(message.sequence as u64),
                },
                channel_info,
            )));
        }

        None
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_sequential_reader_compiles() {
        // Just verify the types compile correctly
    }
}
