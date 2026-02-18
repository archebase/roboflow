// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Shared decode helpers for Source implementations.
//!
//! Contains the background decoder thread logic for local files (format-agnostic
//! via `RoboReader`) and S3/OSS streaming (format-specific parsers). Both MCAP
//! and Bag sources delegate to these shared helpers.

use crate::{SourceError, SourceMetadata, SourceResult, TimestampedMessage, TopicMetadata};
use std::collections::HashMap;

// =============================================================================
// Local file decoder (format-agnostic — RoboReader auto-detects bag vs mcap)
// =============================================================================

/// Decode a local file using RoboReader's lazy streaming iterator.
///
/// Works for both MCAP and Bag files — `RoboReader::open()` auto-detects the format.
/// Sends metadata via `meta_tx`, then streams decoded messages via `msg_tx`.
pub(crate) fn decode_local(
    path: &str,
    format_name: &str,
    meta_tx: tokio::sync::oneshot::Sender<SourceResult<SourceMetadata>>,
    msg_tx: tokio::sync::mpsc::Sender<TimestampedMessage>,
) -> Result<usize, String> {
    use robocodec::io::traits::FormatReader;

    let reader = match robocodec::RoboReader::open(path) {
        Ok(r) => r,
        Err(e) => {
            let err = SourceError::OpenFailed {
                path: path.into(),
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

    let metadata = SourceMetadata::new(format_name.to_string(), path.to_string())
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

        let timestamped = TimestampedMessage {
            topic: msg.channel.topic.clone(),
            log_time: msg.log_time.unwrap_or(0),
            data: robocodec::CodecValue::Struct(msg.message),
        };

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

pub(crate) fn decode_local_batched(
    path: &str,
    format_name: &str,
    meta_tx: tokio::sync::oneshot::Sender<SourceResult<SourceMetadata>>,
    batch_tx: tokio::sync::mpsc::Sender<Vec<TimestampedMessage>>,
    batch_size: usize,
) -> Result<usize, String> {
    use robocodec::io::traits::FormatReader;

    let reader = match robocodec::RoboReader::open(path) {
        Ok(r) => r,
        Err(e) => {
            let err = SourceError::OpenFailed {
                path: path.into(),
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

    let metadata = SourceMetadata::new(format_name.to_string(), path.to_string())
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

        let timestamped = TimestampedMessage {
            topic: msg.channel.topic.clone(),
            log_time: msg.log_time.unwrap_or(0),
            data: robocodec::CodecValue::Struct(msg.message),
        };

        batch.push(timestamped);

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

pub(crate) fn decode_local_blocking(
    path: &str,
    format_name: &str,
    meta_tx: tokio::sync::oneshot::Sender<SourceResult<SourceMetadata>>,
    batch_tx: crossbeam_channel::Sender<Vec<TimestampedMessage>>,
    batch_size: usize,
) -> Result<usize, String> {
    use robocodec::io::traits::FormatReader;

    let reader = match robocodec::RoboReader::open(path) {
        Ok(r) => r,
        Err(e) => {
            let err = SourceError::OpenFailed {
                path: path.into(),
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

    let metadata = SourceMetadata::new(format_name.to_string(), path.to_string())
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

        let timestamped = TimestampedMessage {
            topic: msg.channel.topic.clone(),
            log_time: msg.log_time.unwrap_or(0),
            data: robocodec::CodecValue::Struct(msg.message),
        };

        batch.push(timestamped);

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

// =============================================================================
// S3/OSS streaming decoders (format-specific)
// =============================================================================

/// Decode a bag file from S3/OSS using chunk-based streaming.
pub(crate) fn decode_s3_bag(
    url: &str,
    meta_tx: tokio::sync::oneshot::Sender<SourceResult<SourceMetadata>>,
    msg_tx: tokio::sync::mpsc::Sender<TimestampedMessage>,
) -> Result<usize, String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("Failed to create async runtime: {e}"))?;

    rt.block_on(decode_s3_bag_async(url, meta_tx, msg_tx))
}

/// Decode an MCAP file from S3/OSS using chunk-based streaming.
pub(crate) fn decode_s3_mcap(
    url: &str,
    meta_tx: tokio::sync::oneshot::Sender<SourceResult<SourceMetadata>>,
    msg_tx: tokio::sync::mpsc::Sender<TimestampedMessage>,
) -> Result<usize, String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("Failed to create async runtime: {e}"))?;

    rt.block_on(decode_s3_mcap_async(url, meta_tx, msg_tx))
}

// -- Bag S3 async impl -------------------------------------------------------

async fn decode_s3_bag_async(
    url: &str,
    meta_tx: tokio::sync::oneshot::Sender<SourceResult<SourceMetadata>>,
    msg_tx: tokio::sync::mpsc::Sender<TimestampedMessage>,
) -> Result<usize, String> {
    use robocodec::FormatReader as _;
    use robocodec::encoding::CodecFactory;
    use robocodec::io::formats::bag::stream::StreamingBagParser;
    use robocodec::io::s3::{S3Client, S3Reader};

    let location = parse_cloud_url(url).map_err(|e| format!("Failed to parse URL '{url}': {e}"))?;
    let config = build_s3_config().map_err(|e| format!("Failed to build S3 config: {e}"))?;

    let reader = S3Reader::open_with_config(location.clone(), config.clone())
        .await
        .map_err(|e| format!("Failed to open S3 reader for '{url}': {e}"))?;

    let channels = reader.channels().clone();
    let file_size = reader.file_size();

    let topics: Vec<TopicMetadata> = channels
        .values()
        .map(|ch| TopicMetadata::new(ch.topic.clone(), ch.message_type.clone()))
        .collect();
    let metadata = SourceMetadata::new("bag".to_string(), url.to_string()).with_topics(topics);

    tracing::info!(url = %url, channels = channels.len(), file_size, "S3 bag reader initialized");

    if meta_tx.send(Ok(metadata)).is_err() {
        return Err("Metadata receiver dropped".to_string());
    }

    let client = S3Client::new(config).map_err(|e| format!("S3 client error: {e}"))?;
    let codec_factory = CodecFactory::new();
    let mut schema_cache = build_schema_cache(&channels, &codec_factory);

    let chunk_size: u64 = 10 * 1024 * 1024;
    let mut offset = 0u64;
    let mut count = 0usize;
    let mut parser = StreamingBagParser::new();

    while offset < file_size {
        let fetch_size = chunk_size.min(file_size - offset);
        let chunk = client
            .fetch_range(&location, offset, fetch_size)
            .await
            .map_err(|e| format!("S3 fetch failed at offset {offset}: {e}"))?;

        if chunk.is_empty() {
            break;
        }
        offset += chunk.len() as u64;

        let records = parser
            .parse_chunk(&chunk)
            .map_err(|e| format!("BAG parse error: {e}"))?;

        let bag_channels = parser.channels();

        // Dynamically update schema_cache for newly discovered channels.
        // The initial header scan (1MB) may not discover all connection records;
        // additional connections are found inside compressed chunks during streaming.
        for (ch_id, ch_info) in &bag_channels {
            if !schema_cache.contains_key(ch_id)
                && let Some(schema) = build_schema_for_channel(ch_info, &codec_factory)
            {
                tracing::debug!(
                    channel_id = ch_id,
                    topic = %ch_info.topic,
                    msg_type = %ch_info.message_type,
                    "Schema cache updated for newly discovered channel"
                );
                schema_cache.insert(*ch_id, schema);
            }
        }

        for record in records {
            let channel_id = record.conn_id as u16;
            let channel_info = bag_channels
                .get(&channel_id)
                .or_else(|| channels.get(&channel_id));
            let Some(channel_info) = channel_info else {
                continue;
            };

            let decoded = match decode_raw_message(
                &record.data,
                channel_info,
                &schema_cache,
                &codec_factory,
                record.log_time,
            ) {
                Ok(msg) => msg,
                Err(e) => {
                    tracing::warn!(topic = %channel_info.topic, error = %e, "Skipping decode error");
                    continue;
                }
            };

            if msg_tx.send(decoded).await.is_err() {
                return Ok(count);
            }

            count += 1;
            if count.is_multiple_of(10_000) {
                tracing::debug!(
                    messages = count,
                    offset,
                    file_size,
                    "S3 bag decoder progress"
                );
            }
        }
    }

    tracing::info!(messages = count, "S3 bag decode complete");
    Ok(count)
}

// -- MCAP S3 async impl ------------------------------------------------------

async fn decode_s3_mcap_async(
    url: &str,
    meta_tx: tokio::sync::oneshot::Sender<SourceResult<SourceMetadata>>,
    msg_tx: tokio::sync::mpsc::Sender<TimestampedMessage>,
) -> Result<usize, String> {
    use robocodec::FormatReader as _;
    use robocodec::encoding::CodecFactory;
    use robocodec::io::formats::mcap::streaming::McapS3Adapter;
    use robocodec::io::s3::{S3Client, S3Reader};

    let location = parse_cloud_url(url).map_err(|e| format!("Failed to parse URL '{url}': {e}"))?;
    let config = build_s3_config().map_err(|e| format!("Failed to build S3 config: {e}"))?;

    let reader = S3Reader::open_with_config(location.clone(), config.clone())
        .await
        .map_err(|e| format!("Failed to open S3 reader for '{url}': {e}"))?;

    let channels = reader.channels().clone();
    let file_size = reader.file_size();

    let topics: Vec<TopicMetadata> = channels
        .values()
        .map(|ch| TopicMetadata::new(ch.topic.clone(), ch.message_type.clone()))
        .collect();
    let metadata = SourceMetadata::new("mcap".to_string(), url.to_string()).with_topics(topics);

    tracing::info!(url = %url, channels = channels.len(), file_size, "S3 MCAP reader initialized");

    if meta_tx.send(Ok(metadata)).is_err() {
        return Err("Metadata receiver dropped".to_string());
    }

    let client = S3Client::new(config).map_err(|e| format!("S3 client error: {e}"))?;
    let codec_factory = CodecFactory::new();
    let schema_cache = build_schema_cache(&channels, &codec_factory);

    let chunk_size: u64 = 10 * 1024 * 1024;
    let mut offset = 0u64;
    let mut count = 0usize;
    let mut adapter = McapS3Adapter::new();

    while offset < file_size {
        let fetch_size = chunk_size.min(file_size - offset);
        let chunk = client
            .fetch_range(&location, offset, fetch_size)
            .await
            .map_err(|e| format!("S3 fetch failed at offset {offset}: {e}"))?;

        if chunk.is_empty() {
            break;
        }
        offset += chunk.len() as u64;

        let records = adapter
            .process_chunk(&chunk)
            .map_err(|e| format!("MCAP parse error: {e}"))?;

        for record in records {
            let channel_id = record.channel_id;
            let Some(channel_info) = channels.get(&channel_id) else {
                continue;
            };

            let decoded = match decode_raw_message(
                &record.data,
                channel_info,
                &schema_cache,
                &codec_factory,
                record.log_time,
            ) {
                Ok(msg) => msg,
                Err(e) => {
                    tracing::warn!(topic = %channel_info.topic, error = %e, "Skipping decode error");
                    continue;
                }
            };

            if msg_tx.send(decoded).await.is_err() {
                return Ok(count);
            }

            count += 1;
            if count.is_multiple_of(10_000) {
                tracing::debug!(
                    messages = count,
                    offset,
                    file_size,
                    "S3 MCAP decoder progress"
                );
            }
        }
    }

    tracing::info!(messages = count, "S3 MCAP decode complete");
    Ok(count)
}

// =============================================================================
// S3/Cloud helpers
// =============================================================================

/// Parse a cloud URL (s3:// or oss://) into an S3Location.
pub(crate) fn parse_cloud_url(url: &str) -> SourceResult<robocodec::io::s3::S3Location> {
    let s3_url = if let Some(rest) = url.strip_prefix("oss://") {
        let endpoint = std::env::var("OSS_ENDPOINT")
            .unwrap_or_else(|_| "https://oss-cn-hangzhou.aliyuncs.com".to_string());
        format!("s3://{}?endpoint={}", rest, endpoint)
    } else if !url.contains("endpoint=") {
        if let Ok(endpoint) = std::env::var("AWS_ENDPOINT_URL") {
            if url.contains('?') {
                format!("{}&endpoint={}", url, endpoint)
            } else {
                format!("{}?endpoint={}", url, endpoint)
            }
        } else {
            url.to_string()
        }
    } else {
        url.to_string()
    };

    robocodec::io::s3::S3Location::from_s3_url(&s3_url).map_err(|e| SourceError::OpenFailed {
        path: url.into(),
        error: Box::new(e),
    })
}

/// Build S3ReaderConfig from environment variables.
pub(crate) fn build_s3_config() -> SourceResult<robocodec::io::s3::S3ReaderConfig> {
    use robocodec::io::s3::{AwsCredentials, S3ReaderConfig};

    let credentials = AwsCredentials::from_env().or_else(|| {
        let access_key = std::env::var("OSS_ACCESS_KEY_ID").ok()?;
        let secret_key = std::env::var("OSS_ACCESS_KEY_SECRET").ok()?;
        AwsCredentials::new(access_key, secret_key)
    });

    let mut config = S3ReaderConfig::default();
    if let Some(creds) = credentials {
        config = config.with_credentials(Some(creds));
    }
    Ok(config)
}

/// Build schema metadata cache from channel info.
pub(crate) fn build_schema_cache(
    channels: &HashMap<u16, robocodec::ChannelInfo>,
    factory: &robocodec::encoding::CodecFactory,
) -> HashMap<u16, robocodec::encoding::SchemaMetadata> {
    use robocodec::core::Encoding;
    use robocodec::encoding::SchemaMetadata;

    let mut cache = HashMap::new();
    for (&id, ch) in channels {
        let encoding = factory.detect_encoding(&ch.encoding, ch.schema_encoding.as_deref());
        let schema = match encoding {
            Encoding::Cdr => {
                // ROS1 bags: decoder must use decode_headerless_ros1 (no CDR header, packed layout).
                // If the reader set encoding to "ros1" but did not set schema_encoding, default to
                // "ros1msg" so the codec takes the ROS1 path and avoids wrong-byte-offset errors.
                let schema_encoding = ch.schema_encoding.clone().or_else(|| {
                    if ch.encoding.to_lowercase().contains("ros1") {
                        Some("ros1msg".to_string())
                    } else {
                        None
                    }
                });
                SchemaMetadata::cdr_with_encoding(
                    ch.message_type.clone(),
                    ch.schema.clone().unwrap_or_default(),
                    schema_encoding,
                )
            }
            Encoding::Protobuf => SchemaMetadata::protobuf(
                ch.message_type.clone(),
                ch.schema_data.clone().unwrap_or_default(),
            ),
            Encoding::Json => SchemaMetadata::json(
                ch.message_type.clone(),
                ch.schema.clone().unwrap_or_default(),
            ),
        };
        cache.insert(id, schema);
    }
    cache
}

/// Build schema metadata for a single channel.
///
/// Used to dynamically update the schema cache when new channels are discovered
/// during streaming (channels not found in the initial header scan).
fn build_schema_for_channel(
    ch: &robocodec::ChannelInfo,
    factory: &robocodec::encoding::CodecFactory,
) -> Option<robocodec::encoding::SchemaMetadata> {
    use robocodec::core::Encoding;
    use robocodec::encoding::SchemaMetadata;

    let encoding = factory.detect_encoding(&ch.encoding, ch.schema_encoding.as_deref());
    let schema = match encoding {
        Encoding::Cdr => {
            let schema_encoding = ch.schema_encoding.clone().or_else(|| {
                if ch.encoding.to_lowercase().contains("ros1") {
                    Some("ros1msg".to_string())
                } else {
                    None
                }
            });
            SchemaMetadata::cdr_with_encoding(
                ch.message_type.clone(),
                ch.schema.clone().unwrap_or_default(),
                schema_encoding,
            )
        }
        Encoding::Protobuf => SchemaMetadata::protobuf(
            ch.message_type.clone(),
            ch.schema_data.clone().unwrap_or_default(),
        ),
        Encoding::Json => SchemaMetadata::json(
            ch.message_type.clone(),
            ch.schema.clone().unwrap_or_default(),
        ),
    };
    Some(schema)
}

/// Decode raw message bytes into a TimestampedMessage.
pub(crate) fn decode_raw_message(
    data: &[u8],
    channel_info: &robocodec::ChannelInfo,
    schema_cache: &HashMap<u16, robocodec::encoding::SchemaMetadata>,
    factory: &robocodec::encoding::CodecFactory,
    log_time: u64,
) -> Result<TimestampedMessage, String> {
    let schema = schema_cache.get(&channel_info.id).ok_or_else(|| {
        format!(
            "No schema for channel {} (topic: {})",
            channel_info.id, channel_info.topic
        )
    })?;

    let encoding = schema.encoding();
    let codec = factory.get_codec(encoding).map_err(|e| {
        format!(
            "No codec for encoding {:?} (topic: {}): {}",
            encoding, channel_info.topic, e
        )
    })?;

    let decoded_fields = codec.decode_dynamic(data, schema).map_err(|e| {
        format!(
            "Decode failed for topic {} (type: {}): {}",
            channel_info.topic, channel_info.message_type, e
        )
    })?;

    Ok(TimestampedMessage {
        topic: channel_info.topic.clone(),
        log_time,
        data: robocodec::CodecValue::Struct(decoded_fields),
    })
}

// =============================================================================
// Shared Source initialization helper
// =============================================================================

/// Initialize a source that uses a background decoder thread + channel pattern.
///
/// Spawns a named decoder thread, waits for metadata, and returns the receiver
/// and handle. Used by both `BagSource` and `McapSource`.
pub(crate) async fn initialize_threaded_source(
    path: &str,
    is_cloud: bool,
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
    std::thread::JoinHandle<Result<usize, String>>,
)> {
    let (tx, rx) = tokio::sync::mpsc::channel(8192);
    let (meta_tx, meta_rx) = tokio::sync::oneshot::channel();

    let path_owned = path.to_string();
    let handle = std::thread::Builder::new()
        .name(thread_name.to_string())
        .spawn(move || decoder_fn(path_owned, meta_tx, tx))
        .map_err(|e| SourceError::ReadFailed(format!("Failed to spawn decoder thread: {e}")))?;

    let metadata = match meta_rx.await {
        Ok(Ok(metadata)) => metadata,
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            // meta_tx dropped — get actual error from thread join
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

    let _ = is_cloud;
    Ok((metadata, rx, handle))
}

pub(crate) async fn initialize_threaded_source_batched(
    path: &str,
    is_cloud: bool,
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
    std::thread::JoinHandle<Result<usize, String>>,
)> {
    let (tx, rx) = tokio::sync::mpsc::channel(1024);
    let (meta_tx, meta_rx) = tokio::sync::oneshot::channel();

    let path_owned = path.to_string();
    let handle = std::thread::Builder::new()
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

    let _ = is_cloud;
    Ok((metadata, rx, handle))
}

pub(crate) async fn initialize_threaded_source_blocking(
    path: &str,
    is_cloud: bool,
    thread_name: &str,
    decoder_fn: impl FnOnce(
        String,
        tokio::sync::oneshot::Sender<SourceResult<SourceMetadata>>,
        crossbeam_channel::Sender<Vec<TimestampedMessage>>,
    ) -> Result<usize, String>
    + Send
    + 'static,
) -> SourceResult<(
    SourceMetadata,
    crossbeam_channel::Receiver<Vec<TimestampedMessage>>,
    std::thread::JoinHandle<Result<usize, String>>,
)> {
    let (tx, rx) = crossbeam_channel::bounded(16);
    let (meta_tx, meta_rx) = tokio::sync::oneshot::channel();

    let path_owned = path.to_string();
    let handle = std::thread::Builder::new()
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

    let _ = is_cloud;
    Ok((metadata, rx, handle))
}
