// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! ROS Bag source implementation.
//!
//! Supports both local files and S3/OSS URLs via robocodec's native streaming.
//! Uses a background decoder thread with a bounded channel for backpressure.

use crate::sources::{
    Source, SourceConfig, SourceError, SourceMetadata, SourceResult, TopicMetadata,
};
use robocodec::io::traits::FormatReader;
use roboflow_core::TimestampedMessage;
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

    #[cfg(test)]
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

/// Spawn a decoder thread for local file decoding.
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
impl Source for BagSource {
    async fn initialize(&mut self, config: &SourceConfig) -> SourceResult<SourceMetadata> {
        // Update path from config if provided
        if let crate::SourceType::Bag { path } = &config.source_type {
            self.path = path.clone();
        }

        let (metadata, rx, handle) =
            initialize_threaded_source(&self.path, "bag-decoder", |path, meta_tx, msg_tx| {
                spawn_local_decoder(path, meta_tx, msg_tx, "bag")
            })
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

/// Batched ROS bag source that decodes messages in background thread.
///
/// This source uses a dedicated decoder thread to read and parse bag files,
/// delivering messages in batches for efficient processing.
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
    /// Create a new batched bag source with the given path and batch size.
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

    /// Create a batched bag source from a source configuration.
    pub fn from_config(config: &SourceConfig, batch_size: usize) -> SourceResult<Self> {
        match &config.source_type {
            crate::SourceType::Bag { path } => Self::new(path, batch_size),
            _ => Err(SourceError::InvalidConfig(
                "Invalid config for BagSource".to_string(),
            )),
        }
    }

    #[cfg(test)]
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

/// Spawn a batched decoder thread for local file decoding.
fn spawn_local_decoder_batched(
    path: String,
    meta_tx: tokio::sync::oneshot::Sender<SourceResult<SourceMetadata>>,
    batch_tx: tokio::sync::mpsc::Sender<Vec<TimestampedMessage>>,
    batch_size: usize,
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
    let mut batch = Vec::with_capacity(batch_size);

    for msg_result in iter {
        let msg = match msg_result {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, offset = count, "Skipping decode error");
                continue;
            }
        };

        batch.push(TimestampedMessage::from(msg));

        if batch.len() >= batch_size {
            let batch_to_send = std::mem::replace(&mut batch, Vec::with_capacity(batch_size));
            if batch_tx.blocking_send(batch_to_send).is_err() {
                tracing::debug!(count, "Receiver dropped, stopping decoder");
                break;
            }
        }

        count += 1;
        if count.is_multiple_of(10_000) {
            tracing::debug!(messages = count, "{format_name} decoder progress");
        }
    }

    // Send remaining messages in partial batch
    if !batch.is_empty() {
        let _ = batch_tx.blocking_send(batch);
    }

    tracing::debug!(
        messages = count,
        "Local {format_name} batched decode complete"
    );
    Ok(count)
}

/// Initialize a threaded source with batched output.
async fn initialize_threaded_source_batched(
    path: &str,
    thread_name: &str,
    batch_size: usize,
    decoder_fn: impl FnOnce(
        String,
        tokio::sync::oneshot::Sender<SourceResult<SourceMetadata>>,
        tokio::sync::mpsc::Sender<Vec<TimestampedMessage>>,
        usize,
    ) -> Result<usize, String>
    + Send
    + 'static,
) -> SourceResult<(
    SourceMetadata,
    tokio::sync::mpsc::Receiver<Vec<TimestampedMessage>>,
    thread::JoinHandle<Result<usize, String>>,
)> {
    let (tx, rx) = tokio::sync::mpsc::channel(1024);
    let (meta_tx, meta_rx) = tokio::sync::oneshot::channel();

    let path_owned = path.to_string();
    let handle = thread::Builder::new()
        .name(thread_name.to_string())
        .spawn(move || decoder_fn(path_owned, meta_tx, tx, batch_size))
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
impl Source for BagSourceBatched {
    async fn initialize(&mut self, config: &SourceConfig) -> SourceResult<SourceMetadata> {
        if let crate::SourceType::Bag { path } = &config.source_type {
            self.path = path.clone();
        }

        let batch_size = self.batch_size;
        let (metadata, rx, handle) = initialize_threaded_source_batched(
            &self.path,
            "bag-decoder-batched",
            batch_size,
            |path, meta_tx, batch_tx, batch_size| {
                spawn_local_decoder_batched(path, meta_tx, batch_tx, batch_size, "bag")
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

/// Blocking ROS bag source for synchronous decoding.
///
/// Similar to `BagSourceBatched` but uses blocking channels instead of async.
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
    /// Create a new blocking bag source with the given path and batch size.
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

    /// Create a blocking bag source from a source configuration.
    pub fn from_config(config: &SourceConfig, batch_size: usize) -> SourceResult<Self> {
        match &config.source_type {
            crate::SourceType::Bag { path } => Self::new(path, batch_size),
            _ => Err(SourceError::InvalidConfig(
                "Invalid config for BagSource".to_string(),
            )),
        }
    }

    #[cfg(test)]
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

/// Spawn a blocking decoder thread for local file decoding.
fn spawn_local_decoder_blocking(
    path: String,
    meta_tx: tokio::sync::oneshot::Sender<SourceResult<SourceMetadata>>,
    batch_tx: crossbeam_channel::Sender<Vec<TimestampedMessage>>,
    batch_size: usize,
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
    let mut batch = Vec::with_capacity(batch_size);

    for msg_result in iter {
        let msg = match msg_result {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, offset = count, "Skipping decode error");
                continue;
            }
        };

        batch.push(TimestampedMessage::from(msg));

        if batch.len() >= batch_size {
            let batch_to_send = std::mem::replace(&mut batch, Vec::with_capacity(batch_size));
            if batch_tx.send(batch_to_send).is_err() {
                tracing::debug!(count, "Receiver dropped, stopping decoder");
                break;
            }
        }

        count += 1;
        if count.is_multiple_of(10_000) {
            tracing::debug!(messages = count, "{format_name} decoder progress");
        }
    }

    if !batch.is_empty() {
        let _ = batch_tx.send(batch);
    }

    tracing::debug!(
        messages = count,
        "Local {format_name} blocking decode complete"
    );
    Ok(count)
}

#[async_trait::async_trait]
impl Source for BagSourceBlocking {
    async fn initialize(&mut self, config: &SourceConfig) -> SourceResult<SourceMetadata> {
        if let crate::SourceType::Bag { path } = &config.source_type {
            self.path = path.clone();
        }

        let batch_size = self.batch_size;
        let (tx, rx) = crossbeam_channel::bounded(16);
        let (meta_tx, meta_rx) = tokio::sync::oneshot::channel();

        let path_owned = self.path.clone();
        let handle = thread::Builder::new()
            .name("bag-decoder-blocking".to_string())
            .spawn(move || spawn_local_decoder_blocking(path_owned, meta_tx, tx, batch_size, "bag"))
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

    #[test]
    fn test_bag_source_batched_creation() {
        let source = BagSourceBatched::new("test.bag", 100);
        assert!(source.is_ok());
        let source = source.unwrap();
        assert_eq!(source.path, "test.bag");
        assert_eq!(source.batch_size, 100);
        assert!(!source.is_cloud_url());
    }

    #[test]
    fn test_bag_source_batched_from_config() {
        let config = SourceConfig::bag("test.bag");
        let source = BagSourceBatched::from_config(&config, 256);
        assert!(source.is_ok());
        let source = source.unwrap();
        assert_eq!(source.batch_size, 256);
    }

    #[test]
    fn test_bag_source_batched_invalid_config() {
        let config = SourceConfig::mcap("test.mcap");
        let source = BagSourceBatched::from_config(&config, 100);
        assert!(source.is_err());
    }

    #[test]
    fn test_bag_source_batched_cloud_url() {
        let source = BagSourceBatched::new("s3://bucket/file.bag", 100).unwrap();
        assert!(source.is_cloud_url());
    }

    #[test]
    fn test_bag_source_batched_various_batch_sizes() {
        for size in [1, 10, 100, 1000, 10000] {
            let source = BagSourceBatched::new("test.bag", size).unwrap();
            assert_eq!(source.batch_size, size);
        }
    }

    #[test]
    fn test_bag_source_blocking_creation() {
        let source = BagSourceBlocking::new("test.bag", 100);
        assert!(source.is_ok());
        let source = source.unwrap();
        assert_eq!(source.path, "test.bag");
        assert_eq!(source.batch_size, 100);
        assert!(!source.is_cloud_url());
    }

    #[test]
    fn test_bag_source_blocking_from_config() {
        let config = SourceConfig::bag("test.bag");
        let source = BagSourceBlocking::from_config(&config, 512);
        assert!(source.is_ok());
        let source = source.unwrap();
        assert_eq!(source.batch_size, 512);
    }

    #[test]
    fn test_bag_source_blocking_invalid_config() {
        let config = SourceConfig::mcap("test.mcap");
        let source = BagSourceBlocking::from_config(&config, 100);
        assert!(source.is_err());
    }

    #[test]
    fn test_bag_source_blocking_cloud_url() {
        let source = BagSourceBlocking::new("oss://bucket/file.bag", 100).unwrap();
        assert!(source.is_cloud_url());
    }

    #[test]
    fn test_bag_source_blocking_various_batch_sizes() {
        for size in [1, 50, 500, 5000] {
            let source = BagSourceBlocking::new("test.bag", size).unwrap();
            assert_eq!(source.batch_size, size);
        }
    }

    #[test]
    fn test_bag_source_initial_state() {
        let source = BagSource::new("test.bag").unwrap();
        assert!(source.metadata.is_none());
        assert!(source.receiver.is_none());
        assert!(source.decoder_handle.is_none());
        assert!(!source.finished);
    }

    #[test]
    fn test_bag_source_batched_initial_state() {
        let source = BagSourceBatched::new("test.bag", 100).unwrap();
        assert!(source.metadata.is_none());
        assert!(source.receiver.is_none());
        assert!(source.decoder_handle.is_none());
        assert!(!source.finished);
        assert!(source.current_batch.is_empty());
    }

    #[test]
    fn test_bag_source_blocking_initial_state() {
        let source = BagSourceBlocking::new("test.bag", 100).unwrap();
        assert!(source.metadata.is_none());
        assert!(source.receiver.is_none());
        assert!(source.decoder_handle.is_none());
        assert!(!source.finished);
        assert!(source.current_batch.is_empty());
    }

    #[test]
    fn test_bag_source_supports_seeking() {
        let source = BagSource::new("test.bag").unwrap();
        assert!(!source.supports_seeking());
    }

    #[test]
    fn test_bag_source_batched_supports_seeking() {
        let source = BagSourceBatched::new("test.bag", 100).unwrap();
        assert!(!source.supports_seeking());
    }

    #[test]
    fn test_bag_source_blocking_supports_seeking() {
        let source = BagSourceBlocking::new("test.bag", 100).unwrap();
        assert!(!source.supports_seeking());
    }

    #[test]
    fn test_bag_source_empty_path() {
        let source = BagSource::new("");
        assert!(source.is_ok());
        let source = source.unwrap();
        assert_eq!(source.path, "");
        assert!(!source.is_cloud_url());
    }

    #[test]
    fn test_bag_source_path_with_spaces() {
        let source = BagSource::new("/path/to/my file.bag");
        assert!(source.is_ok());
        let source = source.unwrap();
        assert_eq!(source.path, "/path/to/my file.bag");
    }

    #[test]
    fn test_bag_source_relative_path() {
        let source = BagSource::new("./data/test.bag").unwrap();
        assert_eq!(source.path, "./data/test.bag");
        assert!(!source.is_cloud_url());
    }

    #[test]
    fn test_bag_source_windows_path() {
        let source = BagSource::new("C:\\Users\\test\\data.bag").unwrap();
        assert_eq!(source.path, "C:\\Users\\test\\data.bag");
        assert!(!source.is_cloud_url());
    }
}

#[cfg(test)]
mod s3_url_tests {
    //! Tests verifying S3/OSS URLs are accepted (not rejected).
    //! These tests verify that the artificial "Cloud URLs not yet supported"
    //! restriction has been removed.

    use super::*;

    #[test]
    fn test_bag_source_accepts_s3_url() {
        let source = BagSource::new("s3://bucket/file.bag");
        assert!(source.is_ok(), "BagSource should accept S3 URLs");
        let source = source.unwrap();
        assert!(source.is_cloud_url());
    }

    #[test]
    fn test_bag_source_batched_accepts_s3_url() {
        let source = BagSourceBatched::new("s3://bucket/file.bag", 100);
        assert!(source.is_ok(), "BagSourceBatched should accept S3 URLs");
    }

    #[test]
    fn test_bag_source_blocking_accepts_s3_url() {
        let source = BagSourceBlocking::new("s3://bucket/file.bag", 100);
        assert!(source.is_ok(), "BagSourceBlocking should accept S3 URLs");
    }
}
