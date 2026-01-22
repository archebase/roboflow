// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Core traits for unified I/O operations.
//!
//! This module defines the foundational traits that all format-specific
//! readers and writers must implement. These traits provide a consistent
//! API across all supported formats (MCAP, ROS1 bag, etc.).

use std::any::Any;
use std::collections::HashMap;

use crate::{DecodedMessage, Result};

use super::metadata::{ChannelInfo, FileInfo, RawMessage};

// Re-export filter types
use super::filter::TopicFilter;

/// Trait for reading robotics data from different file formats.
///
/// This trait abstracts over format-specific readers to provide a unified API.
/// All readers must implement this trait to be compatible with the unified I/O layer.
///
/// # Example
///
/// ```no_run
/// use robocodec::io::traits::FormatReader;
///
/// fn process_reader(reader: &dyn FormatReader) {
///     println!("Channels: {}", reader.channels().len());
///     println!("Messages: {}", reader.message_count());
/// }
/// ```
pub trait FormatReader: Send + Sync {
    /// Get all channel information.
    ///
    /// Returns a map of channel ID to channel info.
    fn channels(&self) -> &HashMap<u16, ChannelInfo>;

    /// Get channel info by topic name.
    ///
    /// Returns the first matching channel. In ROS1 bag files, multiple
    /// connections can have the same topic name with different callerids.
    fn channel_by_topic(&self, topic: &str) -> Option<&ChannelInfo> {
        self.channels().values().find(|c| c.topic == topic)
    }

    /// Get all channels with the given topic name.
    fn channels_by_topic(&self, topic: &str) -> Vec<&ChannelInfo> {
        self.channels()
            .values()
            .filter(|c| c.topic == topic)
            .collect()
    }

    /// Get the total message count.
    ///
    /// Returns 0 if the count is unknown (e.g., for files without summary).
    fn message_count(&self) -> u64;

    /// Get the start timestamp in nanoseconds.
    fn start_time(&self) -> Option<u64>;

    /// Get the end timestamp in nanoseconds.
    fn end_time(&self) -> Option<u64>;

    /// Get the file path.
    fn path(&self) -> &str;

    /// Get file information metadata.
    fn file_info(&self) -> FileInfo {
        FileInfo {
            path: self.path().to_string(),
            format: self.format(),
            size: self.file_size(),
            channels: self.channels().clone(),
            message_count: self.message_count(),
            start_time: self.start_time().unwrap_or(0),
            end_time: self.end_time().unwrap_or(0),
            duration: self.duration(),
        }
    }

    /// Get the file format.
    fn format(&self) -> crate::io::metadata::FileFormat;

    /// Get the file size in bytes.
    fn file_size(&self) -> u64;

    /// Get the duration in nanoseconds.
    fn duration(&self) -> u64 {
        match (self.start_time(), self.end_time()) {
            (Some(s), Some(e)) if e > s => e - s,
            _ => 0,
        }
    }

    /// Downcast to `Any` for accessing format-specific functionality.
    fn as_any(&self) -> &dyn Any;

    /// Downcast mutably to `Any` for accessing format-specific functionality.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Streaming iterator over raw messages.
///
/// This trait provides an iterator interface for reading raw messages
/// from a file. The iterator owns its data and is `Send`, allowing it
/// to be moved across threads.
pub trait RawMessageStream: Iterator<Item = Result<RawMessage>> + Send {}

// Blanket implementation for any matching type
impl<T> RawMessageStream for T where T: Iterator<Item = Result<RawMessage>> + Send {}

/// Streaming iterator over decoded messages.
///
/// This trait provides an iterator interface for reading decoded messages
/// from a file. Messages are decoded using the appropriate decoder for
/// their encoding type (CDR, Protobuf, JSON, etc.).
pub trait DecodedMessageStream:
    Iterator<Item = Result<(DecodedMessage, ChannelInfo)>> + Send
{
}

// Blanket implementation for any matching type
impl<T> DecodedMessageStream for T where
    T: Iterator<Item = Result<(DecodedMessage, ChannelInfo)>> + Send
{
}

/// Trait for writing robotics data to different file formats.
///
/// This trait abstracts over format-specific writers to provide a unified API.
///
/// # Example
///
/// ```no_run
/// use robocodec::io::traits::FormatWriter;
/// use robocodec::io::metadata::RawMessage;
///
/// fn write_messages<W: FormatWriter>(writer: &mut W, messages: &[RawMessage]) {
///     for msg in messages {
///         writer.write(msg).unwrap();
///     }
///     writer.finish().unwrap();
/// }
/// ```
pub trait FormatWriter: Send {
    /// Get the output file path.
    fn path(&self) -> &str;

    /// Add a channel/topic to the file.
    ///
    /// Returns the assigned channel ID.
    fn add_channel(
        &mut self,
        topic: &str,
        message_type: &str,
        encoding: &str,
        schema: Option<&str>,
    ) -> Result<u16>;

    /// Write a raw message to the file.
    ///
    /// The message must reference a channel that was previously added
    /// via `add_channel`.
    fn write(&mut self, message: &RawMessage) -> Result<()>;

    /// Write multiple messages in batch.
    ///
    /// Default implementation calls `write` for each message.
    /// Format-specific implementations may override this for better performance.
    fn write_batch(&mut self, messages: &[RawMessage]) -> Result<()> {
        for msg in messages {
            self.write(msg)?;
        }
        Ok(())
    }

    /// Finalize and close the file.
    ///
    /// This must be called to ensure all data is flushed and the
    /// file is properly closed with necessary footer sections.
    fn finish(&mut self) -> Result<()>;

    /// Get the number of messages written so far.
    fn message_count(&self) -> u64;

    /// Get the number of channels added so far.
    fn channel_count(&self) -> usize;

    /// Downcast to `Any` for accessing format-specific functionality.
    fn as_any(&self) -> &dyn Any;

    /// Downcast mutably to `Any` for accessing format-specific functionality.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Builder for creating format-specific readers.
///
/// This trait allows format-specific readers to expose a builder pattern
/// for configuration.
pub trait FormatReaderBuilder: Default {
    type Reader: FormatReader;

    /// Create a new builder with default settings.
    fn new() -> Self {
        Self::default()
    }

    /// Build the reader from the given path.
    fn build<P: AsRef<std::path::Path>>(self, path: P) -> Result<Self::Reader>;
}

/// Builder for creating format-specific writers.
///
/// This trait allows format-specific writers to expose a builder pattern
/// for configuration.
pub trait FormatWriterBuilder: Default {
    type Writer: FormatWriter;

    /// Create a new builder with default settings.
    fn new() -> Self {
        Self::default()
    }

    /// Set the output path.
    fn with_path<P: AsRef<std::path::Path>>(self, path: P) -> Self;

    /// Set the compression level (if supported).
    fn with_compression(self, level: i32) -> Self;

    /// Set the chunk size (if supported).
    fn with_chunk_size(self, size: usize) -> Self;

    /// Build the writer.
    fn build(self) -> Result<Self::Writer>;
}

/// Configuration for parallel reading.
#[derive(Debug, Clone)]
pub struct ParallelReaderConfig {
    /// Number of worker threads (None = auto-detect CPU count)
    pub num_threads: Option<usize>,
    /// Topic/channel filter (None = read all topics)
    pub topic_filter: Option<TopicFilter>,
    /// Backpressure via bounded channel capacity
    pub channel_capacity: Option<usize>,
    /// Progress reporting interval (number of chunks between updates)
    pub progress_interval: usize,
    /// Enable merging of small chunks into larger ones.
    /// This reduces compression overhead and improves throughput,
    /// especially for files with many small chunks (e.g., BAG files).
    /// Default: true.
    pub merge_enabled: bool,
    /// Target size for merged chunks in bytes.
    /// Only used when merge_enabled is true.
    /// Default: 16MB.
    pub merge_target_size: usize,
}

impl Default for ParallelReaderConfig {
    fn default() -> Self {
        Self {
            num_threads: None,
            topic_filter: None,
            channel_capacity: Some(32),
            progress_interval: 10,
            merge_enabled: true,
            merge_target_size: 16 * 1024 * 1024, // 16MB
        }
    }
}

impl ParallelReaderConfig {
    /// Set the number of worker threads.
    pub fn with_threads(mut self, num_threads: usize) -> Self {
        self.num_threads = Some(num_threads);
        self
    }

    /// Set the topic filter.
    pub fn with_topic_filter(mut self, filter: TopicFilter) -> Self {
        self.topic_filter = Some(filter);
        self
    }

    /// Set the channel capacity for backpressure.
    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = Some(capacity);
        self
    }

    /// Set the progress reporting interval.
    pub fn with_progress_interval(mut self, interval: usize) -> Self {
        self.progress_interval = interval;
        self
    }

    /// Set whether chunk merging is enabled.
    ///
    /// When enabled, small chunks are merged into larger chunks to reduce
    /// compression overhead and improve throughput.
    pub fn with_merge_enabled(mut self, enabled: bool) -> Self {
        self.merge_enabled = enabled;
        self
    }

    /// Set the target size for merged chunks in bytes.
    ///
    /// Only used when merge_enabled is true. Chunks will be merged
    /// until they reach approximately this size.
    pub fn with_merge_target_size(mut self, size: usize) -> Self {
        self.merge_target_size = size;
        self
    }
}

/// Statistics from parallel reading.
#[derive(Debug, Clone)]
pub struct ParallelReaderStats {
    /// Total messages read
    pub messages_read: u64,
    /// Number of chunks processed
    pub chunks_processed: usize,
    /// Total data bytes processed
    pub total_bytes: u64,
    /// Time spent reading chunks (seconds)
    pub read_time_sec: f64,
    /// Time spent decompressing (seconds)
    pub decompress_time_sec: f64,
    /// Time spent deserializing messages (seconds)
    pub deserialize_time_sec: f64,
    /// Total time for parallel read (seconds)
    pub total_time_sec: f64,
}

impl Default for ParallelReaderStats {
    fn default() -> Self {
        Self {
            messages_read: 0,
            chunks_processed: 0,
            total_bytes: 0,
            read_time_sec: 0.0,
            decompress_time_sec: 0.0,
            deserialize_time_sec: 0.0,
            total_time_sec: 0.0,
        }
    }
}

/// A message chunk with raw message data.
///
/// This type is used to pass messages from parallel readers to the pipeline.
/// It contains all messages from a single file chunk, along with metadata.
#[derive(Debug)]
pub struct MessageChunkData {
    /// Chunk sequence number
    pub sequence: u64,
    /// Messages in this chunk
    pub messages: Vec<RawMessage>,
    /// Message start time (earliest log_time in chunk)
    pub message_start_time: u64,
    /// Message end time (latest log_time in chunk)
    pub message_end_time: u64,
}

impl MessageChunkData {
    /// Create a new empty message chunk.
    pub fn new(sequence: u64) -> Self {
        Self {
            sequence,
            messages: Vec::new(),
            message_start_time: u64::MAX,
            message_end_time: 0,
        }
    }

    /// Add a message to this chunk.
    pub fn add_message(&mut self, msg: RawMessage) {
        self.message_start_time = self.message_start_time.min(msg.log_time);
        self.message_end_time = self.message_end_time.max(msg.log_time);
        self.messages.push(msg);
    }

    /// Get the number of messages in this chunk.
    pub fn message_count(&self) -> usize {
        self.messages.len()
    }

    /// Check if this chunk is empty.
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    /// Get the total size of all message data in this chunk.
    pub fn total_data_size(&self) -> usize {
        self.messages.iter().map(|m| m.data.len()).sum()
    }
}

/// Parallel reader capability for high-performance chunk-based reading.
///
/// This trait extends FormatReader with parallel reading capabilities for
/// formats that support chunk-based access (MCAP, ROS1 bag, etc.).
///
/// # Two-Phase Pattern
///
/// All parallel readers follow a two-phase pattern:
/// 1. **Discovery Phase** (Sequential): Read metadata to enable parallel access
///    - MCAP with summary: Read summary section at end of file
///    - MCAP without summary: Scan file to build chunk index (>1GB only)
///    - BAG: Read chunk info records from index section
/// 2. **Processing Phase** (Parallel): Process chunks concurrently
///    - Use Rayon thread pool to decompress and parse chunks
///    - Send results through crossbeam channel
///
/// # Example
///
/// ```no_run
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use robocodec::io::traits::ParallelReader;
/// use crossbeam_channel::bounded;
///
/// // Assume you have a reader that implements ParallelReader
/// // let reader = ...;
/// // let (sender, receiver) = bounded(32);
/// //
/// // std::thread::spawn(move || {
/// //     let config = robocodec::io::traits::ParallelReaderConfig::default();
/// //     let stats = reader.read_parallel(config, sender).unwrap();
/// //     println!("Read {} messages", stats.messages_read);
/// // });
/// //
/// // for chunk in receiver {
/// //     // Process chunk...
/// // }
/// # Ok(())
/// # }
/// ```
pub trait ParallelReader: FormatReader {
    /// Read chunks in parallel and send to output channel.
    ///
    /// This method processes chunks concurrently using a Rayon thread pool
    /// and sends MessageChunkData objects through the provided channel. The channel
    /// provides backpressure to prevent memory overload.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for parallel reading (threads, filtering, etc.)
    /// * `sender` - Crossbeam channel for sending processed chunks
    ///
    /// # Returns
    ///
    /// Statistics about the parallel read operation (message count, timing, etc.)
    fn read_parallel(
        &self,
        config: ParallelReaderConfig,
        sender: crossbeam_channel::Sender<MessageChunkData>,
    ) -> Result<ParallelReaderStats>;

    /// Get the number of chunks in the file.
    ///
    /// Returns 0 if the file doesn't support chunk-based reading.
    fn chunk_count(&self) -> usize;

    /// Check if this file can be read in parallel.
    ///
    /// Returns true if the file has the necessary metadata for parallel access
    /// (summary section, chunk info records, etc.).
    fn supports_parallel(&self) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_by_topic() {
        let mut channels = HashMap::new();
        channels.insert(1, ChannelInfo::new(1, "/ch1", "type1"));
        channels.insert(2, ChannelInfo::new(2, "/ch2", "type2"));
        channels.insert(3, ChannelInfo::new(3, "/ch1", "type3")); // Same topic

        struct TestReader {
            channels: HashMap<u16, ChannelInfo>,
        }

        impl FormatReader for TestReader {
            fn channels(&self) -> &HashMap<u16, ChannelInfo> {
                &self.channels
            }

            fn message_count(&self) -> u64 {
                0
            }

            fn start_time(&self) -> Option<u64> {
                None
            }

            fn end_time(&self) -> Option<u64> {
                None
            }

            fn path(&self) -> &str {
                "test"
            }

            fn format(&self) -> crate::io::metadata::FileFormat {
                crate::io::metadata::FileFormat::Unknown
            }

            fn file_size(&self) -> u64 {
                0
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn as_any_mut(&mut self) -> &mut dyn Any {
                self
            }
        }

        let reader = TestReader { channels };

        assert!(reader.channel_by_topic("/ch1").is_some());
        assert!(reader.channel_by_topic("/ch2").is_some());
        assert!(reader.channel_by_topic("/ch3").is_none());

        let ch1_channels = reader.channels_by_topic("/ch1");
        assert_eq!(ch1_channels.len(), 2);
    }
}
