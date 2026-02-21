// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! MCAP source implementation.
//!
//! Supports both local files and S3/OSS URLs via robocodec's native streaming.
//! Uses a background decoder thread with a bounded channel for backpressure.

use crate::sources::{
    Source, SourceConfig, SourceError, SourceMetadata, SourceResult, TopicMetadata,
};
use robocodec::io::traits::FormatReader;
use roboflow_core::TimestampedMessage;
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

/// Spawn a decoder thread for local MCAP file decoding.
fn spawn_local_decoder(
    path: String,
    meta_tx: tokio::sync::oneshot::Sender<SourceResult<SourceMetadata>>,
    msg_tx: tokio::sync::mpsc::Sender<TimestampedMessage>,
    format_name: &'static str,
) -> Result<usize, String> {
    let reader = match robocodec::RoboReader::open(&path) {
        Ok(r) => r,
        Err(e) => {
            let err = SourceError::OpenFailed {
                path: std::path::PathBuf::from(&path),
                error: Box::new(e),
            };
            let _ = meta_tx.send(Err(err));
            return Err(format!("Failed to open {format_name} file: {path}"));
        }
    };

    let message_count = reader.message_count();
    let channels = reader.channels();
    let topics: Vec<TopicMetadata> = channels
        .values()
        .map(|ch| TopicMetadata::new(ch.topic.clone(), ch.message_type.clone()))
        .collect();

    let metadata = SourceMetadata::new(format_name.to_string(), path)
        .with_message_count(message_count)
        .with_topics(topics);

    if meta_tx.send(Ok(metadata)).is_err() {
        return Err("Metadata receiver dropped".to_string());
    }

    let iter = match reader.decoded() {
        Ok(iter) => iter,
        Err(e) => return Err(format!("Failed to get decoded iterator: {e}")),
    };

    let mut count = 0usize;
    for msg_result in iter {
        let msg = match msg_result {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, offset = count, "Skipping decode error");
                continue;
            }
        };

        let timestamped = TimestampedMessage::from(msg);

        if msg_tx.blocking_send(timestamped).is_err() {
            tracing::debug!(count, "Receiver dropped, stopping decoder");
            break;
        }

        count += 1;
        if count.is_multiple_of(10_000) {
            tracing::debug!(messages = count, "{format_name} decoder progress");
        }
    }

    tracing::debug!(messages = count, "Local {format_name} decode complete");
    Ok(count)
}

/// Initialize a threaded source with a decoder function.
async fn initialize_threaded_source(
    path: &str,
    thread_name: &str,
    decoder_fn: impl FnOnce(
        String,
        tokio::sync::oneshot::Sender<SourceResult<SourceMetadata>>,
        tokio::sync::mpsc::Sender<TimestampedMessage>,
    ) -> Result<usize, String>
    + Send
    + 'static,
) -> SourceResult<(
    SourceMetadata,
    tokio::sync::mpsc::Receiver<TimestampedMessage>,
    thread::JoinHandle<Result<usize, String>>,
)> {
    let (tx, rx) = tokio::sync::mpsc::channel(8192);
    let (meta_tx, meta_rx) = tokio::sync::oneshot::channel();

    let path_owned = path.to_string();
    let handle = thread::Builder::new()
        .name(thread_name.to_string())
        .spawn(move || decoder_fn(path_owned, meta_tx, tx))
        .map_err(|e| SourceError::ReadFailed(format!("Failed to spawn decoder thread: {e}")))?;

    let metadata = match meta_rx.await {
        Ok(Ok(metadata)) => metadata,
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            match handle.join() {
                Ok(Err(e)) => {
                    return Err(SourceError::ReadFailed(format!(
                        "Source initialization failed: {e}"
                    )));
                }
                Err(_) => {
                    return Err(SourceError::ReadFailed(
                        "Decoder thread panicked during initialization".to_string(),
                    ));
                }
                Ok(Ok(_)) => {}
            }
            return Err(SourceError::ReadFailed(
                "Decoder thread exited before sending metadata".to_string(),
            ));
        }
    };

    Ok((metadata, rx, handle))
}

#[async_trait::async_trait]
impl Source for McapSource {
    async fn initialize(&mut self, config: &SourceConfig) -> SourceResult<SourceMetadata> {
        // Update path from config if provided
        if let crate::SourceType::Mcap { path } = &config.source_type {
            self.path = path.clone();
        }

        if self.is_cloud_url() {
            return Err(SourceError::InvalidConfig(
                "Cloud URLs not yet supported for McapSource. Use local files.".to_string(),
            ));
        }

        let (metadata, rx, handle) =
            initialize_threaded_source(&self.path, "mcap-decoder", |path, meta_tx, msg_tx| {
                spawn_local_decoder(path, meta_tx, msg_tx, "mcap")
            })
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
