pub mod bag;
pub mod config;
pub mod decode;
pub mod error;
pub mod mcap;
pub mod metadata;
pub mod registry;
pub mod rrd;
pub mod s3_prefix;

pub use bag::{BagSource, BagSourceBatched, BagSourceBlocking};
pub use config::{SourceConfig, SourceType};
pub use error::{SourceError, SourceResult};
pub use mcap::McapSource;
pub use metadata::{SourceMetadata, TopicMetadata};
pub use registry::{
    create_source, global_registry, has_source, register_source, registered_sources,
};
pub use rrd::RrdSource;
pub use s3_prefix::S3PrefixSource;
pub use roboflow_core::TimestampedMessage;

use async_trait::async_trait;

pub fn register_builtin_sources() {
    use crate::sources::{BagSource, McapSource, RrdSource, S3PrefixSource, Source};

    register_source(
        "bag",
        Box::new(|| {
            Box::new(BagSource::new("").expect("empty path is valid for placeholder"))
                as Box<dyn Source>
        }),
    );

    register_source(
        "mcap",
        Box::new(|| {
            Box::new(McapSource::new("").expect("empty path is valid for placeholder"))
                as Box<dyn Source>
        }),
    );

    register_source(
        "rrd",
        Box::new(|| {
            Box::new(RrdSource::new("").expect("empty path is valid for placeholder"))
                as Box<dyn Source>
        }),
    );

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

#[async_trait]
pub trait Source: Send + Sync + 'static {
    async fn initialize(&mut self, config: &SourceConfig) -> SourceResult<SourceMetadata>;
    async fn read_batch(&mut self, size: usize) -> SourceResult<Option<Vec<TimestampedMessage>>>;
    async fn seek(&mut self, _timestamp: u64) -> SourceResult<()> {
        Err(SourceError::SeekNotSupported)
    }
    async fn metadata(&self) -> SourceResult<SourceMetadata>;
    async fn position(&self) -> SourceResult<Option<u64>> {
        Ok(None)
    }
    fn supports_seeking(&self) -> bool {
        false
    }
    fn box_clone(&self) -> SourceResult<Box<dyn Source>> {
        Err(SourceError::CloneNotSupported)
    }
}

impl Clone for Box<dyn Source> {
    fn clone(&self) -> Self {
        self.box_clone().expect("Clone failed")
    }
}

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
}
