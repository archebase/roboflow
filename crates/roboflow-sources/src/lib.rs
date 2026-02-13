//! roboflow-sources: Source trait and implementations for reading robotics data

#![warn(missing_docs)]
#![warn(unused_crate_dependencies)]

mod bag;
mod config;
mod decode;
mod error;
pub mod mcap;
mod metadata;
mod registry;
mod rrd;
mod s3_prefix;

pub use bag::BagSource;
pub use config::{SourceConfig, SourceType};
pub use error::{SourceError, SourceResult};
pub use mcap::McapSource;
pub use metadata::{SourceMetadata, TopicMetadata};
pub use registry::{
    create_source, global_registry, has_source, register_source, registered_sources,
};
pub use rrd::RrdSource;
pub use s3_prefix::S3PrefixSource;

// Re-export TimestampedMessage from roboflow-core for backward compatibility
pub use roboflow_core::TimestampedMessage;

use async_trait::async_trait;

/// Register all built-in source types with the global registry.
///
/// This function should be called once at program startup to ensure
/// all source types are available for dynamic creation.
pub fn register_builtin_sources() {
    use crate::{BagSource, McapSource, RrdSource, S3PrefixSource, Source};

    // Register Bag source
    // Note: The placeholder path is safe because Source::new() never fails for empty paths
    register_source(
        "bag",
        Box::new(|| {
            Box::new(BagSource::new("").expect("empty path is valid for placeholder"))
                as Box<dyn Source>
        }),
    );

    // Register MCAP source
    register_source(
        "mcap",
        Box::new(|| {
            Box::new(McapSource::new("").expect("empty path is valid for placeholder"))
                as Box<dyn Source>
        }),
    );

    // Register RRD source
    register_source(
        "rrd",
        Box::new(|| {
            Box::new(RrdSource::new("").expect("empty path is valid for placeholder"))
                as Box<dyn Source>
        }),
    );

    // Register S3 prefix source
    register_source(
        "s3-prefix",
        Box::new(|| {
            Box::new(
                S3PrefixSource::new("s3://placeholder/")
                    .expect("placeholder URL is valid for factory"),
            ) as Box<dyn Source>
        }),
    );
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
/// use roboflow_sources::{Source, SourceConfig, create_source};
///
/// async fn read_from_mcap() -> roboflow_sources::SourceResult<()> {
///     let config = SourceConfig::mcap("path/to/data.mcap");
///     let mut source = create_source(&config)?;
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
    use robocodec::CodecValue;

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

    #[test]
    fn test_timestamped_message_with_bytes() {
        let msg = TimestampedMessage {
            topic: "/camera/image".to_string(),
            log_time: 9876543210,
            data: CodecValue::Bytes(vec![0xFF, 0xD8, 0xFF, 0xE0]), // JPEG header
        };

        assert_eq!(msg.topic, "/camera/image");
        assert_eq!(msg.log_time, 9876543210);
        if let CodecValue::Bytes(data) = &msg.data {
            assert_eq!(data.len(), 4);
        } else {
            panic!("Expected Bytes variant");
        }
    }

    #[test]
    fn test_timestamped_message_empty_data() {
        // Test with empty bytes to verify message creation
        let msg = TimestampedMessage {
            topic: "/empty/topic".to_string(),
            log_time: 0,
            data: CodecValue::Bytes(vec![]),
        };

        assert_eq!(msg.topic, "/empty/topic");
        assert_eq!(msg.log_time, 0);
        if let CodecValue::Bytes(data) = &msg.data {
            assert!(data.is_empty());
        } else {
            panic!("Expected Bytes variant");
        }
    }

    #[test]
    fn test_source_factory_type() {
        // Test that SourceFactory is a valid type alias
        fn _check_factory_type(_factory: SourceFactory) {}
        // This compiles if the type alias is correct
    }
}
