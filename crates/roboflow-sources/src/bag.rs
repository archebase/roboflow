// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! ROS Bag source implementation.
//!
//! Supports both local files and S3/OSS URLs via robocodec's native streaming.
//! Uses a background decoder thread with a bounded channel for backpressure.

use crate::decode;
use crate::{Source, SourceConfig, SourceError, SourceMetadata, SourceResult, TimestampedMessage};
use std::thread;

/// ROS Bag source reader.
///
/// Reads robotics data from ROS bag files. Supports local files and S3/OSS URLs.
pub struct BagSource {
    path: String,
    metadata: Option<SourceMetadata>,
    receiver: Option<tokio::sync::mpsc::Receiver<TimestampedMessage>>,
    decoder_handle: Option<thread::JoinHandle<Result<usize, String>>>,
    finished: bool,
}

impl BagSource {
    /// Create a new Bag source from a file path or URL.
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

    /// Create a new Bag source from a SourceConfig.
    pub fn from_config(config: &SourceConfig) -> SourceResult<Self> {
        match &config.source_type {
            crate::SourceType::Bag { path } => Self::new(path),
            _ => Err(SourceError::InvalidConfig(
                "Invalid config for BagSource".to_string(),
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
                    tracing::debug!(messages = count, "Bag decoder completed");
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
impl Source for BagSource {
    async fn initialize(&mut self, config: &SourceConfig) -> SourceResult<SourceMetadata> {
        // Update path from config if provided
        if let crate::SourceType::Bag { path } = &config.source_type {
            self.path = path.clone();
        }

        let is_cloud = self.is_cloud_url();
        let (metadata, rx, handle) = decode::initialize_threaded_source(
            &self.path,
            is_cloud,
            "bag-decoder",
            move |path, meta_tx, msg_tx| {
                if is_cloud {
                    decode::decode_s3_bag(&path, meta_tx, msg_tx)
                } else {
                    decode::decode_local(&path, "bag", meta_tx, msg_tx)
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
            "Bag source initialized"
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

pub struct BagSourceBatched {
    path: String,
    metadata: Option<SourceMetadata>,
    receiver: Option<tokio::sync::mpsc::Receiver<Vec<TimestampedMessage>>>,
    decoder_handle: Option<thread::JoinHandle<Result<usize, String>>>,
    finished: bool,
    current_batch: Vec<TimestampedMessage>,
    batch_size: usize,
}

impl BagSourceBatched {
    pub fn new(path: impl Into<String>, batch_size: usize) -> SourceResult<Self> {
        let path = path.into();
        Ok(Self {
            path,
            metadata: None,
            receiver: None,
            decoder_handle: None,
            finished: false,
            current_batch: Vec::new(),
            batch_size,
        })
    }

    pub fn from_config(config: &SourceConfig, batch_size: usize) -> SourceResult<Self> {
        match &config.source_type {
            crate::SourceType::Bag { path } => Self::new(path, batch_size),
            _ => Err(SourceError::InvalidConfig(
                "Invalid config for BagSource".to_string(),
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
                    tracing::debug!(messages = count, "Bag decoder completed");
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
impl Source for BagSourceBatched {
    async fn initialize(&mut self, config: &SourceConfig) -> SourceResult<SourceMetadata> {
        if let crate::SourceType::Bag { path } = &config.source_type {
            self.path = path.clone();
        }

        let is_cloud = self.is_cloud_url();
        let batch_size = self.batch_size;
        
        if is_cloud {
            return Err(SourceError::InvalidConfig(
                "Batched mode not supported for cloud URLs yet".to_string(),
            ));
        }

        let (metadata, rx, handle) = decode::initialize_threaded_source_batched(
            &self.path,
            is_cloud,
            "bag-decoder-batched",
            batch_size,
            move |path, meta_tx, batch_tx, batch_size| {
                decode::decode_local_batched(&path, "bag", meta_tx, batch_tx, batch_size)
            },
        )
        .await?;

        self.metadata = Some(metadata.clone());
        self.receiver = Some(rx);
        self.decoder_handle = Some(handle);

        tracing::info!(
            path = %self.path,
            topics = metadata.topics.len(),
            batch_size = self.batch_size,
            "Bag source (batched) initialized"
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

        if !self.current_batch.is_empty() {
            let result = if self.current_batch.len() <= batch_size {
                let batch = std::mem::take(&mut self.current_batch);
                Some(batch)
            } else {
                let remaining = self.current_batch.split_off(batch_size);
                let result = std::mem::take(&mut self.current_batch);
                self.current_batch = remaining;
                Some(result)
            };
            return Ok(result);
        }

        let receiver = self.receiver.as_mut().ok_or_else(|| {
            SourceError::ReadFailed("Source not initialized - call initialize() first".to_string())
        })?;

        match receiver.recv().await {
            Some(mut batch) => {
                if batch.len() <= batch_size {
                    Ok(Some(batch))
                } else {
                    let remaining = batch.split_off(batch_size);
                    self.current_batch = remaining;
                    Ok(Some(batch))
                }
            }
            None => {
                self.finished = true;
                self.check_decoder_result()?;
                Ok(None)
            }
        }
    }

    async fn seek(&mut self, _timestamp: u64) -> SourceResult<()> {
        Err(SourceError::SeekNotSupported)
    }

    async fn metadata(&self) -> SourceResult<SourceMetadata> {
        self.metadata
            .clone()
            .ok_or_else(|| SourceError::ReadFailed("Source not initialized".to_string()))
    }

    async fn position(&self) -> SourceResult<Option<u64>> {
        Ok(None)
    }

    fn supports_seeking(&self) -> bool {
        false
    }
}

pub struct BagSourceBlocking {
    path: String,
    metadata: Option<SourceMetadata>,
    receiver: Option<crossbeam_channel::Receiver<Vec<TimestampedMessage>>>,
    decoder_handle: Option<thread::JoinHandle<Result<usize, String>>>,
    finished: bool,
    current_batch: Vec<TimestampedMessage>,
    batch_size: usize,
}

impl BagSourceBlocking {
    pub fn new(path: impl Into<String>, batch_size: usize) -> SourceResult<Self> {
        let path = path.into();
        Ok(Self {
            path,
            metadata: None,
            receiver: None,
            decoder_handle: None,
            finished: false,
            current_batch: Vec::new(),
            batch_size,
        })
    }

    pub fn from_config(config: &SourceConfig, batch_size: usize) -> SourceResult<Self> {
        match &config.source_type {
            crate::SourceType::Bag { path } => Self::new(path, batch_size),
            _ => Err(SourceError::InvalidConfig(
                "Invalid config for BagSource".to_string(),
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
                    tracing::debug!(messages = count, "Bag decoder completed");
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
impl Source for BagSourceBlocking {
    async fn initialize(&mut self, config: &SourceConfig) -> SourceResult<SourceMetadata> {
        if let crate::SourceType::Bag { path } = &config.source_type {
            self.path = path.clone();
        }

        let is_cloud = self.is_cloud_url();
        let batch_size = self.batch_size;

        if is_cloud {
            return Err(SourceError::InvalidConfig(
                "Blocking mode not supported for cloud URLs".to_string(),
            ));
        }

        let (metadata, rx, handle): (SourceMetadata, crossbeam_channel::Receiver<Vec<TimestampedMessage>>, _) = decode::initialize_threaded_source_blocking(
            &self.path,
            is_cloud,
            "bag-decoder-blocking",
            move |path, meta_tx, batch_tx| {
                decode::decode_local_blocking(&path, "bag", meta_tx, batch_tx, batch_size)
            },
        ).await?;

        self.metadata = Some(metadata.clone());
        self.receiver = Some(rx);
        self.decoder_handle = Some(handle);

        tracing::info!(
            path = %self.path,
            topics = metadata.topics.len(),
            batch_size = self.batch_size,
            "Bag source (blocking) initialized"
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

        if !self.current_batch.is_empty() {
            let result = if self.current_batch.len() <= batch_size {
                let batch = std::mem::take(&mut self.current_batch);
                Some(batch)
            } else {
                let remaining = self.current_batch.split_off(batch_size);
                let result = std::mem::take(&mut self.current_batch);
                self.current_batch = remaining;
                Some(result)
            };
            return Ok(result);
        }

        let receiver = self.receiver.as_mut().ok_or_else(|| {
            SourceError::ReadFailed("Source not initialized - call initialize() first".to_string())
        })?;

        match receiver.recv() {
            Ok(mut batch) => {
                if batch.len() <= batch_size {
                    Ok(Some(batch))
                } else {
                    let remaining = batch.split_off(batch_size);
                    self.current_batch = remaining;
                    Ok(Some(batch))
                }
            }
            Err(_) => {
                self.finished = true;
                self.check_decoder_result()?;
                Ok(None)
            }
        }
    }

    async fn seek(&mut self, _timestamp: u64) -> SourceResult<()> {
        Err(SourceError::SeekNotSupported)
    }

    async fn metadata(&self) -> SourceResult<SourceMetadata> {
        self.metadata
            .clone()
            .ok_or_else(|| SourceError::ReadFailed("Source not initialized".to_string()))
    }

    async fn position(&self) -> SourceResult<Option<u64>> {
        Ok(None)
    }

    fn supports_seeking(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bag_source_creation() {
        let source = BagSource::new("test.bag");
        assert!(source.is_ok());
        let source = source.unwrap();
        assert_eq!(source.path, "test.bag");
        assert!(!source.is_cloud_url());
    }

    #[test]
    fn test_bag_source_from_config() {
        let config = SourceConfig::bag("test.bag");
        let source = BagSource::from_config(&config);
        assert!(source.is_ok());
    }

    #[test]
    fn test_bag_source_invalid_config() {
        let config = SourceConfig::mcap("test.mcap");
        let source = BagSource::from_config(&config);
        assert!(source.is_err());
    }

    #[test]
    fn test_cloud_url_detection() {
        assert!(
            BagSource::new("s3://bucket/file.bag")
                .unwrap()
                .is_cloud_url()
        );
        assert!(
            BagSource::new("oss://bucket/file.bag")
                .unwrap()
                .is_cloud_url()
        );
        assert!(!BagSource::new("/path/to/file.bag").unwrap().is_cloud_url());
        assert!(!BagSource::new("file.bag").unwrap().is_cloud_url());
    }
}
