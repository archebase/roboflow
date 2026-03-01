// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! ROS Bag source implementation.
//!
//! Uses robocodec's StreamingRoboReader for fast frame-aligned decoding.
//! Supports both local files and S3/OSS URLs via robocodec's built-in streaming.

use crate::formats::common::AlignedFrame;
use crate::sources::s3_env::maybe_apply_s3_env_for_url;
use crate::sources::{
    Source, SourceConfig, SourceError, SourceMetadata, SourceResult, TopicMetadata,
};
use robocodec::io::streaming::{FrameAlignmentConfig, StreamConfig, StreamingRoboReader};
use robocodec::io::traits::FormatReader;
use roboflow_core::TimestampedMessage;
use roboflow_media::ImageData;
use roboflow_storage::StorageFactory;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::Path;
use std::path::PathBuf;

/// RAII guard for removing temporary files on scope exit.
struct TempFileGuard {
    path: PathBuf,
}

impl TempFileGuard {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_file(&self.path) {
            tracing::warn!(
                path = %self.path.display(),
                error = %e,
                "Failed to remove temporary downloaded bag file"
            );
        }
    }
}

/// Download a file from S3/OSS to a temporary local file (for legacy batched/blocking decoders).
fn download_to_temp(url: &str) -> Result<PathBuf, String> {
    tracing::info!(url = %url, "Downloading S3/OSS file to temp location");

    let factory = StorageFactory::from_env();
    let storage = factory
        .create(url)
        .map_err(|e| format!("Failed to create storage for {url}: {e}"))?;

    let parsed_url: roboflow_storage::StorageUrl = url
        .parse()
        .map_err(|e| format!("Failed to parse URL {url}: {e}"))?;

    let object_path = match &parsed_url {
        roboflow_storage::StorageUrl::S3 { key, .. } => Path::new(key),
        roboflow_storage::StorageUrl::Local { path } => path.as_path(),
    };

    let temp_filename = format!("roboflow_bag_{}.bag", uuid::Uuid::new_v4().simple());
    let temp_path = std::env::temp_dir().join(&temp_filename);

    let mut reader = storage
        .reader(object_path)
        .map_err(|e| format!("Failed to open reader for {url}: {e}"))?;

    let mut temp_file = std::fs::File::create(&temp_path)
        .map_err(|e| format!("Failed to create temp file {}: {e}", temp_path.display()))?;

    // Use 64KB buffer for better throughput on large files
    let mut buffer = vec![0u8; 65536];
    let mut total_bytes = 0u64;

    loop {
        let bytes_read = reader
            .read(&mut buffer)
            .map_err(|e| format!("Failed to read from {url}: {e}"))?;

        if bytes_read == 0 {
            break;
        }

        temp_file
            .write_all(&buffer[..bytes_read])
            .map_err(|e| format!("Failed to write to temp file: {e}"))?;

        total_bytes += bytes_read as u64;
    }

    temp_file
        .flush()
        .map_err(|e| format!("Failed to flush: {e}"))?;

    #[cfg(not(windows))]
    {
        temp_file
            .sync_all()
            .map_err(|e| format!("Failed to sync: {e}"))?;
    }

    tracing::info!(url = %url, bytes = total_bytes, "Download complete");
    Ok(temp_path)
}

/// ROS Bag source reader.
///
/// Reads robotics data from ROS bag files using robocodec's StreamingRoboReader.
/// Supports local files and S3/OSS URLs via robocodec's built-in streaming.
pub struct BagSource {
    path: String,
    metadata: Option<SourceMetadata>,
    receiver: Option<tokio::sync::mpsc::Receiver<AlignedFrame>>,
    decoder_handle: Option<tokio::task::JoinHandle<Result<usize, String>>>,
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

    async fn check_decoder_result(&mut self) -> SourceResult<()> {
        if let Some(handle) = self.decoder_handle.take() {
            match handle.await {
                Ok(Ok(count)) => {
                    tracing::debug!(messages = count, "Bag decoder completed");
                    Ok(())
                }
                Ok(Err(e)) => Err(SourceError::ReadFailed(format!("Decoder error: {e}"))),
                Err(_) => Err(SourceError::ReadFailed("Decoder task panicked".to_string())),
            }
        } else {
            Ok(())
        }
    }
}

/// Convert roboflow's AlignedFrame to a vector of TimestampedMessages.
///
/// This bridges the frame-aligned output from StreamingRoboReader back to
/// the message-based Source trait interface.
fn convert_aligned_frame_to_messages(frame: AlignedFrame) -> Vec<TimestampedMessage> {
    use robocodec::CodecValue;

    let mut messages = Vec::new();

    // Convert images to TimestampedMessage
    for (feature_name, image_data) in frame.images {
        let image_data = match std::sync::Arc::try_unwrap(image_data) {
            Ok(image_data) => image_data,
            Err(shared) => (*shared).clone(),
        };

        let ImageData {
            width,
            height,
            data,
            is_encoded,
            is_depth,
            original_timestamp,
            ..
        } = image_data;

        let mut fields = HashMap::new();
        fields.insert("width".to_string(), CodecValue::UInt32(width));
        fields.insert("height".to_string(), CodecValue::UInt32(height));
        fields.insert("data".to_string(), CodecValue::Bytes(data));
        fields.insert("is_encoded".to_string(), CodecValue::Bool(is_encoded));
        fields.insert("is_depth".to_string(), CodecValue::Bool(is_depth));
        fields.insert(
            "original_timestamp".to_string(),
            CodecValue::UInt64(original_timestamp),
        );

        let data = CodecValue::Struct(fields);

        messages.push(TimestampedMessage {
            topic: feature_name,
            log_time: frame.timestamp,
            data,
        });
    }

    // Convert states to TimestampedMessage
    for (feature_name, state_data) in frame.states {
        let data = CodecValue::Array(state_data.into_iter().map(CodecValue::Float32).collect());
        messages.push(TimestampedMessage {
            topic: feature_name,
            log_time: frame.timestamp,
            data,
        });
    }

    messages
}

/// Convert robocodec's AlignedFrame to roboflow's AlignedFrame.
fn convert_codec_frame_to_roboflow_frame(
    codec_frame: robocodec::io::streaming::AlignedFrame,
) -> AlignedFrame {
    let mut frame = AlignedFrame::new(codec_frame.frame_index, codec_frame.timestamp);

    // Convert images
    for (name, img) in codec_frame.images {
        let image_data = ImageData::encoded(img.width, img.height, img.data);
        frame.add_image(name, image_data);
    }

    // Convert states
    for (name, state) in codec_frame.states {
        frame.add_state(name, state);
    }

    frame
}

fn build_frame_config(
    fps: u32,
    image_topics: &[String],
    state_topics: &[String],
) -> FrameAlignmentConfig {
    let mut frame_config = FrameAlignmentConfig::new(fps).with_max_latency(50_000_000);

    if image_topics.is_empty() {
        frame_config = frame_config.with_image_topic("/cam_l/color/image_raw/compressed");
    } else {
        for topic in image_topics {
            frame_config = frame_config.with_image_topic(topic.clone());
        }
    }

    if state_topics.is_empty() {
        frame_config = frame_config.with_state_topic("/kuavo_arm_traj");
    } else {
        for topic in state_topics {
            frame_config = frame_config.with_state_topic(topic.clone());
        }
    }

    frame_config
}

/// Spawn a decoder thread using robocodec's StreamingRoboReader with frame alignment.
fn spawn_local_decoder(
    path: String,
    meta_tx: tokio::sync::oneshot::Sender<SourceResult<SourceMetadata>>,
    frame_tx: tokio::sync::mpsc::Sender<AlignedFrame>,
    format_name: &'static str,
    image_topics: Vec<String>,
    state_topics: Vec<String>,
    fps: u32,
) -> Result<usize, String> {
    tracing::info!(
        path = %path,
        format = format_name,
        "spawn_local_decoder: starting with StreamingRoboReader"
    );

    // Apply S3 environment variables for cloud streaming
    maybe_apply_s3_env_for_url(&path);

    // Create tokio runtime for async streaming reader
    tracing::info!("Creating tokio runtime for decoder");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("Failed to create runtime: {e}"))?;
    tracing::info!("Runtime created successfully");

    let reader = rt.block_on(async {
        let config = StreamConfig::new();
        tracing::info!(path = %path, "Opening StreamingRoboReader...");
        let reader = StreamingRoboReader::open(&path, config)
            .await
            .map_err(|e| format!("Failed to open: {e}"))?;
        tracing::info!("StreamingRoboReader opened successfully");

        // Get metadata from reader
        let message_count = reader.message_count();
        let channels = reader.channels();
        let topics: Vec<TopicMetadata> = channels
            .values()
            .map(|ch| TopicMetadata::new(ch.topic.clone(), ch.message_type.clone()))
            .collect();
        let topics_len = topics.len();

        let metadata = SourceMetadata::new(format_name.to_string(), path.clone())
            .with_message_count(message_count)
            .with_topics(topics);

        tracing::info!(
            "Metadata extracted: {} topics, {} messages",
            topics_len,
            message_count
        );
        tracing::info!("Sending metadata to channel...");
        if meta_tx.send(Ok(metadata)).is_err() {
            return Err("Metadata receiver dropped".to_string());
        }
        tracing::info!("Metadata sent successfully");

        Ok(reader)
    })?;

    drop(rt);

    tracing::debug!(path = %path, "Metadata sent, starting frame processing");

    let frame_config = build_frame_config(fps, &image_topics, &state_topics);

    let mut frame_count = 0usize;

    reader
        .process_frames(frame_config, |codec_frame| {
            // Convert robocodec frame to roboflow frame
            let frame = convert_codec_frame_to_roboflow_frame(codec_frame);

            if frame_tx.blocking_send(frame).is_err() {
                return Err(robocodec::CodecError::Other("Channel closed".into()));
            }

            frame_count += 1;
            if frame_count.is_multiple_of(1000) {
                tracing::debug!(frames = frame_count, "{format_name} decoder progress");
            }

            Ok(())
        })
        .map_err(|e| format!("Frame processing error: {e}"))?;

    tracing::debug!(frames = frame_count, "Local {format_name} decode complete");
    Ok(frame_count)
}

/// Initialize a threaded source with a decoder function using tokio::task::spawn_blocking.
///
/// Uses robocodec's StreamingRoboReader for frame-aligned decoding.
async fn initialize_threaded_source(
    path: &str,
    _thread_name: &str,
    decoder_fn: impl FnOnce(
        String,
        tokio::sync::oneshot::Sender<SourceResult<SourceMetadata>>,
        tokio::sync::mpsc::Sender<AlignedFrame>,
    ) -> Result<usize, String>
    + Send
    + 'static,
) -> SourceResult<(
    SourceMetadata,
    tokio::sync::mpsc::Receiver<AlignedFrame>,
    tokio::task::JoinHandle<Result<usize, String>>,
)> {
    let (tx, rx) = tokio::sync::mpsc::channel(1024);
    let (meta_tx, meta_rx) = tokio::sync::oneshot::channel();

    let path_owned = path.to_string();
    let handle = tokio::task::spawn_blocking(move || decoder_fn(path_owned, meta_tx, tx));

    let metadata = match meta_rx.await {
        Ok(Ok(metadata)) => metadata,
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            match handle.await {
                Ok(Err(e)) => {
                    return Err(SourceError::ReadFailed(format!(
                        "Source initialization failed: {e}"
                    )));
                }
                Err(_) => {
                    return Err(SourceError::ReadFailed(
                        "Decoder task panicked during initialization".to_string(),
                    ));
                }
                Ok(Ok(_)) => {}
            }
            return Err(SourceError::ReadFailed(
                "Decoder task exited before sending metadata".to_string(),
            ));
        }
    };

    Ok((metadata, rx, handle))
}

/// Initialize a streaming source using a dedicated blocking decoder task.
async fn initialize_streaming_source(
    path: &str,
    image_topics: Vec<String>,
    state_topics: Vec<String>,
    fps: u32,
) -> SourceResult<(
    SourceMetadata,
    tokio::sync::mpsc::Receiver<AlignedFrame>,
    tokio::task::JoinHandle<Result<usize, String>>,
)> {
    let (tx, rx) = tokio::sync::mpsc::channel(1024);
    let (meta_tx, meta_rx) = tokio::sync::oneshot::channel();

    let path_owned = path.to_string();
    let handle = tokio::task::spawn_blocking(move || {
        spawn_streaming_decoder(
            path_owned,
            meta_tx,
            tx,
            "bag",
            image_topics,
            state_topics,
            fps,
        )
    });

    let metadata = match meta_rx.await {
        Ok(Ok(metadata)) => metadata,
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            match handle.await {
                Ok(Err(e)) => {
                    return Err(SourceError::ReadFailed(format!(
                        "Source initialization failed: {e}"
                    )));
                }
                Err(_) => {
                    return Err(SourceError::ReadFailed(
                        "Decoder task panicked during initialization".to_string(),
                    ));
                }
                Ok(Ok(_)) => {}
            }
            return Err(SourceError::ReadFailed(
                "Decoder task exited before sending metadata".to_string(),
            ));
        }
    };

    Ok((metadata, rx, handle))
}

/// Spawn a streaming decoder in a blocking task.
fn spawn_streaming_decoder(
    path: String,
    meta_tx: tokio::sync::oneshot::Sender<SourceResult<SourceMetadata>>,
    frame_tx: tokio::sync::mpsc::Sender<AlignedFrame>,
    format_name: &'static str,
    image_topics: Vec<String>,
    state_topics: Vec<String>,
    fps: u32,
) -> Result<usize, String> {
    tracing::info!(
        path = %path,
        format = format_name,
        "spawn_streaming_decoder: starting"
    );

    maybe_apply_s3_env_for_url(&path);

    tracing::info!("Creating tokio runtime for streaming decoder");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("Failed to create runtime: {e}"))?;

    let config = StreamConfig::new();
    tracing::info!("Opening StreamingRoboReader...");
    let reader = rt.block_on(async {
        let reader = StreamingRoboReader::open(&path, config)
            .await
            .map_err(|e| format!("Failed to open: {e}"))?;
        tracing::info!("StreamingRoboReader opened successfully");

        // Get metadata from reader
        let message_count = reader.message_count();
        let channels = reader.channels();
        let topics: Vec<TopicMetadata> = channels
            .values()
            .map(|ch| TopicMetadata::new(ch.topic.clone(), ch.message_type.clone()))
            .collect();
        let topics_len = topics.len();

        let metadata = SourceMetadata::new(format_name.to_string(), path.clone())
            .with_message_count(message_count)
            .with_topics(topics);

        if (path.starts_with("s3://") || path.starts_with("oss://")) && message_count == 0 {
            tracing::info!(
                topics = topics_len,
                messages = message_count,
                "Metadata extracted; message count may be unavailable until streaming progresses"
            );
        } else {
            tracing::info!(
                "Metadata extracted: {} topics, {} messages",
                topics_len,
                message_count
            );
        }

        if meta_tx.send(Ok(metadata)).is_err() {
            return Err("Metadata receiver dropped".to_string());
        }

        Ok(reader)
    })?;

    drop(rt);

    tracing::info!("Metadata sent, starting frame processing");

    let frame_config = build_frame_config(fps, &image_topics, &state_topics);

    let mut frame_count = 0usize;

    reader
        .process_frames(frame_config, |codec_frame| {
            let frame = convert_codec_frame_to_roboflow_frame(codec_frame);

            if frame_tx.blocking_send(frame).is_err() {
                return Err(robocodec::CodecError::Other("Channel closed".into()));
            }

            frame_count += 1;
            if frame_count.is_multiple_of(1000) {
                tracing::debug!(frames = frame_count, "{} decoder progress", format_name);
            }

            Ok(())
        })
        .map_err(|e| format!("Frame processing error: {e}"))?;

    tracing::info!(frames = frame_count, "Streaming decode complete");
    Ok(frame_count)
}

#[async_trait::async_trait]
impl Source for BagSource {
    async fn initialize(&mut self, config: &SourceConfig) -> SourceResult<SourceMetadata> {
        // Update path from config if provided
        if let crate::SourceType::Bag { path } = &config.source_type {
            self.path = path.clone();
        }

        maybe_apply_s3_env_for_url(&self.path);

        // Extract topics from config options
        let image_topics: Vec<String> = config
            .get_option::<Vec<String>>("image_topics")
            .unwrap_or_default();
        let state_topics: Vec<String> = config
            .get_option::<Vec<String>>("state_topics")
            .unwrap_or_default();
        let fps: u32 = config.get_option::<u32>("fps").unwrap_or(30);

        tracing::debug!(
            path = %self.path,
            image_topics = ?image_topics,
            state_topics = ?state_topics,
            fps = fps,
            "Initializing bag source with configured topics"
        );

        let (metadata, rx, handle) = if self.path.starts_with("s3://")
            || self.path.starts_with("oss://")
        {
            // Use streaming decoder path for cloud URLs
            initialize_streaming_source(&self.path, image_topics, state_topics, fps).await?
        } else {
            // Use threaded source for local files
            initialize_threaded_source(&self.path, "bag-decoder", move |path, meta_tx, msg_tx| {
                spawn_local_decoder(
                    path,
                    meta_tx,
                    msg_tx,
                    "bag",
                    image_topics,
                    state_topics,
                    fps,
                )
            })
            .await?
        };

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

        // Receive AlignedFrame and convert to TimestampedMessage
        match receiver.recv().await {
            Some(frame) => {
                let messages = convert_aligned_frame_to_messages(frame);
                batch.extend(messages);
            }
            None => {
                self.finished = true;
                self.check_decoder_result().await?;
                return Ok(None);
            }
        }

        // Try to fill the batch with more frames
        while batch.len() < batch_size {
            match receiver.try_recv() {
                Ok(frame) => {
                    let messages = convert_aligned_frame_to_messages(frame);
                    batch.extend(messages);
                }
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
    decoder_handle: Option<tokio::task::JoinHandle<Result<usize, String>>>,
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

    async fn check_decoder_result(&mut self) -> SourceResult<()> {
        if let Some(handle) = self.decoder_handle.take() {
            match handle.await {
                Ok(Ok(count)) => {
                    tracing::debug!(messages = count, "Bag decoder completed");
                    Ok(())
                }
                Ok(Err(e)) => Err(SourceError::ReadFailed(format!("Decoder error: {e}"))),
                Err(_) => Err(SourceError::ReadFailed("Decoder task panicked".to_string())),
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
    // For S3/OSS URLs, download to temp file first
    let mut temp_file_guard = None;
    let local_path = if path.starts_with("s3://") || path.starts_with("oss://") {
        match download_to_temp(&path) {
            Ok(temp_path) => {
                let guard = TempFileGuard::new(temp_path);
                let path_string = guard.path().to_string_lossy().to_string();
                temp_file_guard = Some(guard);
                path_string
            }
            Err(e) => {
                let err = SourceError::OpenFailed {
                    path: std::path::PathBuf::from(&path),
                    error: Box::new(std::io::Error::other(e.clone())),
                };
                let _ = meta_tx.send(Err(err));
                return Err(format!(
                    "Failed to download {format_name} file from {path}: {e}"
                ));
            }
        }
    } else {
        path.clone()
    };

    let _temp_file_guard = temp_file_guard;

    let reader_result = robocodec::RoboReader::open(&local_path);

    let reader = match reader_result {
        Ok(r) => r,
        Err(e) => {
            let err = SourceError::OpenFailed {
                path: std::path::PathBuf::from(&local_path),
                error: Box::new(e),
            };
            let _ = meta_tx.send(Err(err));
            return Err(format!("Failed to open {format_name} file: {local_path}"));
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

/// Initialize a threaded source with batched output using tokio::task::spawn_blocking.
async fn initialize_threaded_source_batched(
    path: &str,
    _thread_name: &str,
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
    tokio::task::JoinHandle<Result<usize, String>>,
)> {
    let (tx, rx) = tokio::sync::mpsc::channel(1024);
    let (meta_tx, meta_rx) = tokio::sync::oneshot::channel();

    let path_owned = path.to_string();
    let handle =
        tokio::task::spawn_blocking(move || decoder_fn(path_owned, meta_tx, tx, batch_size));

    let metadata = match meta_rx.await {
        Ok(Ok(metadata)) => metadata,
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            match handle.await {
                Ok(Err(e)) => {
                    return Err(SourceError::ReadFailed(format!(
                        "Source initialization failed: {e}"
                    )));
                }
                Err(_) => {
                    return Err(SourceError::ReadFailed(
                        "Decoder task panicked during initialization".to_string(),
                    ));
                }
                Ok(Ok(_)) => {}
            }
            return Err(SourceError::ReadFailed(
                "Decoder task exited before sending metadata".to_string(),
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

        maybe_apply_s3_env_for_url(&self.path);

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
                self.check_decoder_result().await?;
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
    decoder_handle: Option<tokio::task::JoinHandle<Result<usize, String>>>,
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

    async fn check_decoder_result(&mut self) -> SourceResult<()> {
        if let Some(handle) = self.decoder_handle.take() {
            match handle.await {
                Ok(Ok(count)) => {
                    tracing::debug!(messages = count, "Bag decoder completed");
                    Ok(())
                }
                Ok(Err(e)) => Err(SourceError::ReadFailed(format!("Decoder error: {e}"))),
                Err(_) => Err(SourceError::ReadFailed("Decoder task panicked".to_string())),
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
    // For S3/OSS URLs, download to temp file first
    let mut temp_file_guard = None;
    let local_path = if path.starts_with("s3://") || path.starts_with("oss://") {
        match download_to_temp(&path) {
            Ok(temp_path) => {
                let guard = TempFileGuard::new(temp_path);
                let path_string = guard.path().to_string_lossy().to_string();
                temp_file_guard = Some(guard);
                path_string
            }
            Err(e) => {
                let err = SourceError::OpenFailed {
                    path: std::path::PathBuf::from(&path),
                    error: Box::new(std::io::Error::other(e.clone())),
                };
                let _ = meta_tx.send(Err(err));
                return Err(format!(
                    "Failed to download {format_name} file from {path}: {e}"
                ));
            }
        }
    } else {
        path.clone()
    };

    let _temp_file_guard = temp_file_guard;

    let reader_result = robocodec::RoboReader::open(&local_path);

    let reader = match reader_result {
        Ok(r) => r,
        Err(e) => {
            let err = SourceError::OpenFailed {
                path: std::path::PathBuf::from(&local_path),
                error: Box::new(e),
            };
            let _ = meta_tx.send(Err(err));
            return Err(format!("Failed to open {format_name} file: {local_path}"));
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

        maybe_apply_s3_env_for_url(&self.path);

        let batch_size = self.batch_size;
        let (tx, rx) = crossbeam_channel::bounded(16);
        let (meta_tx, meta_rx) = tokio::sync::oneshot::channel();

        let path_owned = self.path.clone();
        let handle = tokio::task::spawn_blocking(move || {
            spawn_local_decoder_blocking(path_owned, meta_tx, tx, batch_size, "bag")
        });

        let metadata = match meta_rx.await {
            Ok(Ok(metadata)) => metadata,
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                match handle.await {
                    Ok(Err(e)) => {
                        return Err(SourceError::ReadFailed(format!(
                            "Source initialization failed: {e}"
                        )));
                    }
                    Err(_) => {
                        return Err(SourceError::ReadFailed(
                            "Decoder task panicked during initialization".to_string(),
                        ));
                    }
                    Ok(Ok(_)) => {}
                }
                return Err(SourceError::ReadFailed(
                    "Decoder task exited before sending metadata".to_string(),
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
                self.check_decoder_result().await?;
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
