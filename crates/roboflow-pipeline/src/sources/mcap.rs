// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! MCAP source implementation.
//!
//! Supports both local files and S3/OSS URLs via robocodec's native streaming.
//! Uses a background decoder thread with a bounded channel for backpressure.

use crate::sources::decode;
use crate::sources::{
    Source, SourceConfig, SourceError, SourceMetadata, SourceResult, TimestampedMessage,
};
use std::thread;

/// MCAP source reader.
///
/// Reads robotics data from MCAP files. Supports local files and S3/OSS URLs.
pub struct McapSource {
    path: String,
    metadata: Option<SourceMetadata>,
    receiver: Option<tokio::sync::mpsc::Receiver<TimestampedMessage>>,
    decoder_handle: Option<thread::JoinHandle<Result<usize, String>>>,
    finished: bool,
}

impl McapSource {
    /// Create a new MCAP source from a file path or URL.
    pub fn new(path: impl Into<String>) -> SourceResult<Self> {
        let path = path.into();
        Ok(Self {
            path,
            metadata: None,
            receiver: None,
            decoder_handle: None,
            finished: false,
        })
    }

    /// Create a new MCAP source from a SourceConfig.
    pub fn from_config(config: &SourceConfig) -> SourceResult<Self> {
        match &config.source_type {
            crate::SourceType::Mcap { path } => Self::new(path),
            _ => Err(SourceError::InvalidConfig(
                "Invalid config for McapSource".to_string(),
            )),
        }
    }

    fn is_cloud_url(&self) -> bool {
        self.path.starts_with("s3://") || self.path.starts_with("oss://")
    }

    fn check_decoder_result(&mut self) -> SourceResult<()> {
        if let Some(handle) = self.decoder_handle.take() {
            match handle.join() {
                Ok(Ok(count)) => {
                    tracing::debug!(messages = count, "MCAP decoder completed");
                    Ok(())
                }
                Ok(Err(e)) => Err(SourceError::ReadFailed(format!("Decoder error: {e}"))),
                Err(_) => Err(SourceError::ReadFailed(
                    "Decoder thread panicked".to_string(),
                )),
            }
        } else {
            Ok(())
        }
    }
}

#[async_trait::async_trait]
impl Source for McapSource {
    async fn initialize(&mut self, config: &SourceConfig) -> SourceResult<SourceMetadata> {
        // Update path from config if provided
        if let crate::SourceType::Mcap { path } = &config.source_type {
            self.path = path.clone();
        }

        let is_cloud = self.is_cloud_url();
        let (metadata, rx, handle) = decode::initialize_threaded_source(
            &self.path,
            is_cloud,
            "mcap-decoder",
            move |path, meta_tx, msg_tx| {
                if is_cloud {
                    decode::decode_s3_mcap(&path, meta_tx, msg_tx)
                } else {
                    decode::decode_local(&path, "mcap", meta_tx, msg_tx)
                }
            },
        )
        .await?;

        self.metadata = Some(metadata.clone());
        self.receiver = Some(rx);
        self.decoder_handle = Some(handle);

        tracing::info!(
            path = %self.path,
            topics = metadata.topics.len(),
            messages = ?metadata.message_count,
            "MCAP source initialized"
        );

        Ok(metadata)
    }

    async fn read_batch(
        &mut self,
        batch_size: usize,
    ) -> SourceResult<Option<Vec<TimestampedMessage>>> {
        if self.finished {
            return Ok(None);
        }

        let receiver = self.receiver.as_mut().ok_or_else(|| {
            SourceError::ReadFailed("Source not initialized - call initialize() first".to_string())
        })?;

        let mut batch = Vec::with_capacity(batch_size.min(1024));

        match receiver.recv().await {
            Some(msg) => batch.push(msg),
            None => {
                self.finished = true;
                self.check_decoder_result()?;
                return Ok(None);
            }
        }

        while batch.len() < batch_size {
            match receiver.try_recv() {
                Ok(msg) => batch.push(msg),
                Err(_) => break,
            }
        }

        Ok(Some(batch))
    }

    async fn seek(&mut self, _timestamp: u64) -> SourceResult<()> {
        Err(SourceError::SeekNotSupported)
    }

    async fn metadata(&self) -> SourceResult<SourceMetadata> {
        self.metadata
            .clone()
            .ok_or_else(|| SourceError::ReadFailed("Source not initialized".to_string()))
    }

    fn supports_seeking(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mcap_source_creation() {
        let source = McapSource::new("test.mcap");
        assert!(source.is_ok());
        let source = source.unwrap();
        assert_eq!(source.path, "test.mcap");
        assert!(!source.is_cloud_url());
    }

    #[test]
    fn test_mcap_source_from_config() {
        let config = SourceConfig::mcap("test.mcap");
        let source = McapSource::from_config(&config);
        assert!(source.is_ok());
    }

    #[test]
    fn test_mcap_source_invalid_config() {
        let config = SourceConfig::bag("test.bag");
        let source = McapSource::from_config(&config);
        assert!(source.is_err());
    }

    #[test]
    fn test_cloud_url_detection() {
        assert!(
            McapSource::new("s3://bucket/file.mcap")
                .unwrap()
                .is_cloud_url()
        );
        assert!(
            McapSource::new("oss://bucket/file.mcap")
                .unwrap()
                .is_cloud_url()
        );
        assert!(
            !McapSource::new("/path/to/file.mcap")
                .unwrap()
                .is_cloud_url()
        );
    }
}
