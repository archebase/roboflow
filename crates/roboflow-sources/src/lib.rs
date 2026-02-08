//! roboflow-sources: Source trait and implementations for reading robotics data

#![warn(missing_docs)]
#![warn(unused_crate_dependencies)]

mod bag;
mod config;
mod error;
pub mod mcap;
mod metadata;
mod registry;

pub use bag::BagSource;
pub use config::{SourceConfig, SourceType};
pub use error::{SourceError, SourceResult};
pub use mcap::McapSource;
pub use metadata::{SourceMetadata, TopicMetadata};
pub use registry::{SourceRegistry, create_source, global_registry, register_source};

use async_trait::async_trait;
use robocodec::CodecValue;

/// A decoded message from a source.
///
/// This is the primary output type for all sources, providing a unified
/// interface regardless of the underlying file format (MCAP, Bag, HDF5, etc.).
#[derive(Debug, Clone)]
pub struct TimestampedMessage {
    /// Channel/topic name
    pub topic: String,
    /// Log timestamp (nanoseconds)
    pub log_time: u64,
    /// Decoded message data
    pub data: CodecValue,
}

/// Trait for reading robotics data from various sources.
///
/// Sources provide a unified interface for reading data from different
/// file formats and storage systems. All sources are async and support
/// streaming reads for memory efficiency.
///
/// # Example
///
/// ```rust,no_run
/// use roboflow_sources::{Source, SourceConfig, SourceRegistry};
///
/// async fn read_from_mcap() -> roboflow_sources::SourceResult<()> {
///     let config = SourceConfig::mcap("path/to/data.mcap");
///     let registry = SourceRegistry::new();
///     let mut source = registry.create(&config)?;
///
///     let metadata = source.initialize(&config).await?;
///     println!("Source has {} topics", metadata.topics.len());
///
///     while let Some(batch) = source.read_batch(100).await? {
///         for msg in batch {
///             println!("Got message from {}", msg.topic);
///         }
///     }
///
///     Ok(())
/// }
/// ```
#[async_trait]
pub trait Source: Send + Sync + 'static {
    /// Initialize the source with the given configuration.
    ///
    /// This method is called once before any other operations. It should
    /// open the file/connection, read metadata, and prepare for reading.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for this source
    ///
    /// # Returns
    ///
    /// Metadata about the source, including available topics and message types.
    async fn initialize(&mut self, config: &SourceConfig) -> SourceResult<SourceMetadata>;

    /// Read a batch of messages from the source.
    ///
    /// This method should return messages in chronological order when possible.
    /// The returned `Option` indicates whether more messages are available:
    /// - `Some(Ok(batch))` - A batch of messages (may be empty if no new messages)
    /// - `Some(Err(e))` - An error occurred
    /// - `None` - End of stream, no more messages available
    ///
    /// # Arguments
    ///
    /// * `size` - Maximum number of messages to return (may return fewer)
    ///
    /// # Returns
    ///
    /// A batch of messages, or None if end of stream is reached.
    async fn read_batch(&mut self, size: usize) -> SourceResult<Option<Vec<TimestampedMessage>>>;

    /// Seek to a specific timestamp.
    ///
    /// Not all sources support seeking. Sources that don't support seeking
    /// should return `SourceError::SeekNotSupported`.
    ///
    /// # Arguments
    ///
    /// * `_timestamp` - Target timestamp in nanoseconds
    ///
    /// # Returns
    ///
    /// Ok(()) if seek succeeded, or an error
    async fn seek(&mut self, _timestamp: u64) -> SourceResult<()> {
        Err(SourceError::SeekNotSupported)
    }

    /// Get metadata about the source.
    ///
    /// This should return the same information that was returned from
    /// `initialize()`, but can be called multiple times.
    ///
    /// # Returns
    ///
    /// The source metadata
    async fn metadata(&self) -> SourceResult<SourceMetadata>;

    /// Get the current position in the stream.
    ///
    /// # Returns
    ///
    /// The current timestamp in nanoseconds, if available
    async fn position(&self) -> SourceResult<Option<u64>> {
        Ok(None)
    }

    /// Check if the source supports seeking.
    ///
    /// # Returns
    ///
    /// true if `seek()` is supported
    fn supports_seeking(&self) -> bool {
        false
    }

    /// Clone the source.
    ///
    /// This is used when multiple readers need to access the same source.
    /// Not all sources support cloning.
    ///
    /// # Returns
    ///
    /// A cloned source, or an error if cloning is not supported
    fn box_clone(&self) -> SourceResult<Box<dyn Source>> {
        Err(SourceError::CloneNotSupported)
    }
}

// Blanket impl for all Box<dyn Source>
impl Clone for Box<dyn Source> {
    fn clone(&self) -> Self {
        self.box_clone().expect("Clone failed")
    }
}

/// Factory function for creating sources.
///
/// Each source implementation should register a factory function
/// that creates a new instance of that source.
pub type SourceFactory = Box<dyn Fn() -> Box<dyn Source> + Send + Sync>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamped_message() {
        let msg = TimestampedMessage {
            topic: "/test/topic".to_string(),
            log_time: 1234567890,
            data: CodecValue::String("hello".to_string()),
        };

        assert_eq!(msg.topic, "/test/topic");
        assert_eq!(msg.log_time, 1234567890);
    }
}
