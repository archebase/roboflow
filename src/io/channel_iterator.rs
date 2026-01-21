//! Iterator wrapper for crossbeam channels.
//!
//! This module provides ergonomic iterator APIs over crossbeam channels,
//! enabling pull-based consumption of push-based parallel reading results.

use std::time::Duration;

use crate::pipeline::types::chunk::MessageChunk;
use crate::{CodecError, Result};

/// Iterator over MessageChunks from a channel receiver.
///
/// This provides an ergonomic pull-based API over the push-based channel
/// used by parallel readers. It supports both blocking and timeout-based
/// iteration.
///
/// # Example
///
/// ```no_run
/// use robocodec::io::channel_iterator::ChannelChunkIterator;
/// use crossbeam_channel::bounded;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let (sender, receiver) = bounded(32);
/// // ... parallel reader sends chunks to sender ...
///
/// let iter = ChannelChunkIterator::new(receiver);
/// for result in iter {
///     let chunk = result?;
///     // Process chunk...
/// }
/// # Ok(())
/// # }
/// ```
pub struct ChannelChunkIterator {
    receiver: crossbeam_channel::Receiver<MessageChunk<'static>>,
    timeout: Option<Duration>,
}

impl ChannelChunkIterator {
    /// Create a new iterator with no timeout (blocks indefinitely).
    pub fn new(receiver: crossbeam_channel::Receiver<MessageChunk<'static>>) -> Self {
        Self {
            receiver,
            timeout: None,
        }
    }

    /// Create a new iterator with a timeout.
    ///
    /// # Arguments
    ///
    /// * `receiver` - The channel receiver
    /// * `timeout` - Maximum time to wait for each chunk
    pub fn with_timeout(
        receiver: crossbeam_channel::Receiver<MessageChunk<'static>>,
        timeout: Duration,
    ) -> Self {
        Self {
            receiver,
            timeout: Some(timeout),
        }
    }

    /// Try to receive the next chunk without blocking.
    ///
    /// Returns:
    /// - `Ok(Some(chunk))` - A chunk was available
    /// - `Ok(None)` - No chunk available (channel empty)
    /// - `Err(e)` - Channel disconnected or other error
    pub fn try_next(&mut self) -> Result<Option<MessageChunk<'static>>> {
        match self.receiver.try_recv() {
            Ok(chunk) => Ok(Some(chunk)),
            Err(crossbeam_channel::TryRecvError::Empty) => Ok(None),
            Err(crossbeam_channel::TryRecvError::Disconnected) => Err(CodecError::encode(
                "ChannelChunkIterator",
                "Channel disconnected",
            )),
        }
    }
}

impl Iterator for ChannelChunkIterator {
    type Item = Result<MessageChunk<'static>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.timeout {
            Some(timeout) => match self.receiver.recv_timeout(timeout) {
                Ok(chunk) => Some(Ok(chunk)),
                // Both Timeout and Disconnected mean end of iteration
                Err(_) => None,
            },
            None => match self.receiver.recv() {
                Ok(chunk) => Some(Ok(chunk)),
                Err(_) => None, // Channel closed
            },
        }
    }
}

/// Builder for creating parallel reader with iterator output.
///
/// This builder provides a convenient API for parallel reading that returns
/// an iterator over the processed chunks, hiding the channel/thread management.
///
/// # Example
///
/// ```no_run
/// use robocodec::io::channel_iterator::ParallelReaderIteratorBuilder;
/// use robocodec::io::filter::TopicFilter;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let (mut iter, _stats) = ParallelReaderIteratorBuilder::new()
///     .with_threads(8)
///     .with_topic_filter(TopicFilter::include(vec![String::from("/camera")]))
///     .build("data.mcap")?;
///
/// for result in iter {
///     let chunk = result?;
///     println!("Got chunk with {} messages", chunk.message_count());
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Default)]
pub struct ParallelReaderIteratorBuilder {
    config: crate::io::traits::ParallelReaderConfig,
    thread_name: Option<String>,
}

impl ParallelReaderIteratorBuilder {
    /// Create a new builder with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the number of worker threads.
    pub fn with_threads(mut self, num_threads: usize) -> Self {
        self.config.num_threads = Some(num_threads);
        self
    }

    /// Set the topic filter.
    pub fn with_topic_filter(mut self, filter: crate::io::filter::TopicFilter) -> Self {
        self.config.topic_filter = Some(filter);
        self
    }

    /// Set the channel capacity for backpressure.
    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.config.channel_capacity = Some(capacity);
        self
    }

    /// Set the thread name for debugging.
    pub fn with_thread_name(mut self, name: String) -> Self {
        self.thread_name = Some(name);
        self
    }

    /// Build the parallel reader and return an iterator + join handle.
    ///
    /// This spawns a reader thread that processes chunks in parallel and
    /// sends them to a channel. The returned iterator consumes the channel,
    /// and the join handle returns the statistics when complete.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the file to read
    ///
    /// # Returns
    ///
    /// A tuple of (iterator, join_handle) where:
    /// - iterator: `ChannelChunkIterator` for consuming chunks
    /// - join_handle: `JoinHandle<Result<ParallelReaderStats>>` for getting stats
    pub fn build<P: AsRef<std::path::Path>>(
        self,
        path: P,
    ) -> Result<(
        ChannelChunkIterator,
        std::thread::JoinHandle<Result<crate::io::traits::ParallelReaderStats>>,
    )> {
        let path_ref = path.as_ref();
        let path_str = path_ref.to_string_lossy().to_string();

        // Detect format
        let format = crate::io::detection::detect_format(path_ref)?;

        // Create bounded channel for backpressure
        let capacity = self.config.channel_capacity.unwrap_or(32);
        let (sender, receiver) = crossbeam_channel::bounded(capacity);

        // Clone config for the thread
        let config = self.config.clone();
        let thread_name = self
            .thread_name
            .unwrap_or_else(|| format!("parallel-reader-{}", path_str));

        // Spawn reader thread
        let handle = std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                // Create appropriate parallel reader based on format
                let result: Result<Box<dyn crate::io::traits::ParallelReader>> = match format {
                    crate::io::metadata::FileFormat::Mcap => {
                        // For MCAP files, always use two-pass reader
                        // This handles files without summary section
                        crate::io::formats::mcap_two_pass::TwoPassMcapReader::open(&path_str)
                            .map(|r| Box::new(r) as Box<dyn crate::io::traits::ParallelReader>)
                    }
                    crate::io::metadata::FileFormat::Bag => {
                        crate::io::formats::bag_parallel::ParallelBagReader::open(&path_str)
                            .map(|r| Box::new(r) as Box<dyn crate::io::traits::ParallelReader>)
                    }
                    crate::io::metadata::FileFormat::Unknown => {
                        Err(crate::CodecError::unsupported(format!(
                            "Unknown file format: {}",
                            path_str
                        )))
                    }
                };

                // Execute parallel read
                match result {
                    Ok(reader) => reader.read_parallel(config, sender),
                    Err(e) => Err(e),
                }
            })
            .map_err(|e| {
                crate::CodecError::encode(
                    "ParallelReaderIteratorBuilder",
                    format!("Failed to spawn thread: {e}"),
                )
            })?;

        Ok((ChannelChunkIterator::new(receiver), handle))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_channel_chunk_iterator_empty() {
        let (sender, receiver) = crossbeam_channel::unbounded();
        drop(sender); // Drop sender so channel closes immediately
        let mut iter = ChannelChunkIterator::new(receiver);

        // Channel is empty and closed, should return None immediately
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_channel_chunk_iterator_try_next() {
        let (sender, receiver) = crossbeam_channel::unbounded();

        // Send a chunk
        let chunk = MessageChunk::with_capacity(0, 0);
        sender.send(chunk).unwrap();
        // Don't drop sender yet - keep it alive so channel stays open

        let mut iter = ChannelChunkIterator::new(receiver);

        // First call returns the chunk
        let result = iter.try_next().unwrap();
        assert!(result.is_some());

        // Second call returns None (channel empty but still open)
        assert!(iter.try_next().unwrap().is_none());

        // Third call still returns None (channel empty)
        assert!(iter.try_next().unwrap().is_none());
    }

    #[test]
    fn test_channel_chunk_iterator_timeout() {
        // Create a bounded channel and drop sender so channel is closed
        let (sender, receiver) = crossbeam_channel::bounded::<MessageChunk<'static>>(1);
        drop(sender);

        let mut iter = ChannelChunkIterator::with_timeout(receiver, Duration::from_millis(10));

        // Channel is closed, should return None immediately (disconnected = None)
        let result = iter.next();
        // When disconnected, we return None (not an error)
        assert!(result.is_none());
    }

    #[test]
    fn test_channel_chunk_iterator_with_data() {
        let (sender, receiver) = crossbeam_channel::bounded(1);
        let chunk = MessageChunk::with_capacity(0, 0);
        sender.send(chunk).unwrap();
        drop(sender); // Close after sending

        let mut iter = ChannelChunkIterator::new(receiver);

        // Chunk is available, should return immediately
        assert!(iter.next().is_some());

        // No more chunks - closed channel returns None
        assert!(iter.next().is_none());
    }
}
