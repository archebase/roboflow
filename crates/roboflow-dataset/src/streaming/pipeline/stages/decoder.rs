// Decoder stage - wraps robocodec's streaming decoder
//
// Supports two input modes:
// - LocalFile: uses RoboReader::open() for local files
// - S3Url: uses robocodec's S3Client + format-specific streaming parsers
//   for direct S3/OSS streaming without temp files

use std::collections::HashMap;
use std::thread::{self, JoinHandle};
use std::time::Instant;

use crossbeam_channel::Sender;

use crate::streaming::pipeline::types::{DecodedMessage, PipelineError, PipelineResult};

/// Statistics from the decoder stage.
#[derive(Debug, Clone)]
pub struct DecoderStats {
    /// Total messages decoded
    pub messages_decoded: usize,
    /// Processing time in seconds
    pub duration_sec: f64,
}

/// Input source for the decoder stage.
#[derive(Debug, Clone)]
pub enum InputSource {
    /// Local file path - uses RoboReader::open()
    LocalFile(std::path::PathBuf),
    /// S3/OSS URL - uses robocodec S3Reader for direct streaming.
    ///
    /// Supports both `s3://bucket/key` and `oss://bucket/key` URLs.
    /// For OSS, set `OSS_ENDPOINT` environment variable.
    /// Credentials are read from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
    /// (or `OSS_ACCESS_KEY_ID` / `OSS_ACCESS_KEY_SECRET`).
    S3Url(String),
}

/// The decoder stage.
///
/// This stage wraps robocodec's streaming decoder with two input modes:
/// - For local files: `RoboReader::open()` with its `decoded()` lazy iterator
/// - For S3/OSS URLs: direct HTTP range-request streaming via `S3Client` +
///   format-specific parsers, eliminating temp file downloads entirely
pub struct DecoderStage {
    /// Input source (local file or S3 URL)
    input_source: InputSource,
    /// Output channel for decoded messages
    output_tx: Sender<DecodedMessage>,
}

impl DecoderStage {
    /// Create a new decoder stage.
    pub fn new(input_source: InputSource, output_tx: Sender<DecodedMessage>) -> Self {
        Self {
            input_source,
            output_tx,
        }
    }

    /// Create a new decoder stage from a local file path (convenience method).
    pub fn from_path(input_path: std::path::PathBuf, output_tx: Sender<DecodedMessage>) -> Self {
        Self::new(InputSource::LocalFile(input_path), output_tx)
    }

    /// Spawn the decoder in a thread.
    pub fn spawn(self) -> JoinHandle<PipelineResult<DecoderStats>> {
        thread::spawn(move || {
            let name = "Decoder";
            let input_label = match &self.input_source {
                InputSource::LocalFile(p) => p.display().to_string(),
                InputSource::S3Url(url) => url.clone(),
            };
            tracing::debug!(input = %input_label, "{name} starting");

            let start = Instant::now();
            let result = match &self.input_source {
                InputSource::LocalFile(_) => self.run_local(),
                InputSource::S3Url(_) => self.run_s3_streaming(),
            };
            let duration = start.elapsed();

            match &result {
                Ok(stats) => {
                    tracing::debug!(
                        duration_sec = duration.as_secs_f64(),
                        messages = stats.messages_decoded,
                        "{name} completed"
                    );
                }
                Err(e) => {
                    tracing::error!(error = %e, "{name} failed");
                }
            }

            result.map(|mut stats| {
                stats.duration_sec = duration.as_secs_f64();
                stats
            })
        })
    }

    /// Run the decoder using RoboReader for local files.
    fn run_local(&self) -> PipelineResult<DecoderStats> {
        use robocodec::RoboReader;

        let input_path = match &self.input_source {
            InputSource::LocalFile(p) => p,
            _ => unreachable!("run_local called with non-local input"),
        };

        let path_str = input_path
            .to_str()
            .ok_or_else(|| PipelineError::ExecutionFailed {
                stage: "Decoder".to_string(),
                reason: "Invalid UTF-8 path".to_string(),
            })?;

        // Open robocodec reader - this handles file I/O optimization internally
        let reader = RoboReader::open(path_str).map_err(|e| PipelineError::ExecutionFailed {
            stage: "Decoder".to_string(),
            reason: format!("Failed to open input: {e}"),
        })?;

        let mut messages_decoded = 0usize;

        // Use robocodec's streaming iterator - decoded() returns a lazy iterator
        // Messages are decoded on-demand, not loaded all at once
        // msg.message is HashMap<String, robocodec::CodecValue>
        for msg_result in reader
            .decoded()
            .map_err(|e| PipelineError::ExecutionFailed {
                stage: "Decoder".to_string(),
                reason: format!("Failed to get decoded iterator: {e}"),
            })?
        {
            let msg = msg_result.map_err(|e| PipelineError::ExecutionFailed {
                stage: "Decoder".to_string(),
                reason: format!("Decode error: {e}"),
            })?;

            // Convert TimestampedDecodedMessage to our DecodedMessage
            // msg.message is HashMap<String, robocodec::CodecValue>
            let decoded = DecodedMessage {
                topic: msg.channel.topic.clone(),
                message_type: msg.channel.message_type.clone(),
                log_time: msg.log_time.unwrap_or(0),
                sequence: msg.sequence,
                data: robocodec::CodecValue::Struct(msg.message),
            };

            self.output_tx
                .send(decoded)
                .map_err(|e| PipelineError::ChannelError {
                    from: "Decoder".to_string(),
                    to: "Aligner".to_string(),
                    reason: e.to_string(),
                })?;

            messages_decoded += 1;

            if messages_decoded.is_multiple_of(10000) {
                tracing::debug!(messages = messages_decoded, "Decoder progress");
            }
        }

        Ok(DecoderStats {
            messages_decoded,
            duration_sec: 0.0,
        })
    }

    /// Run the decoder using S3 streaming for cloud inputs.
    ///
    /// Uses robocodec's S3Reader for initialization (two-tier header scan for
    /// channel discovery), then streams chunks via S3Client + format-specific
    /// parsers to preserve message timing metadata (log_time, sequence).
    fn run_s3_streaming(&self) -> PipelineResult<DecoderStats> {
        use robocodec::FormatReader as _;
        use robocodec::encoding::CodecFactory;
        use robocodec::io::s3::{S3Client, S3Reader};

        let url = match &self.input_source {
            InputSource::S3Url(u) => u.as_str(),
            _ => unreachable!("run_s3_streaming called with non-S3 input"),
        };

        let location = parse_cloud_url_to_s3_location(url)?;
        let config = build_s3_reader_config()?;

        // Create a tokio runtime for async S3 operations
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| PipelineError::ExecutionFailed {
                stage: "Decoder".to_string(),
                reason: format!("Failed to create async runtime: {e}"),
            })?;

        rt.block_on(async {
            // Phase 1: Use S3Reader for initialization (two-tier header scan)
            let reader = S3Reader::open_with_config(location.clone(), config.clone())
                .await
                .map_err(|e| PipelineError::ExecutionFailed {
                    stage: "Decoder".to_string(),
                    reason: format!("Failed to open S3 reader: {e}"),
                })?;

            let channels = reader.channels().clone();
            let file_size = reader.file_size();
            let format = reader.format();

            tracing::info!(
                url = %url,
                format = ?format,
                channels = channels.len(),
                file_size,
                "S3 reader initialized, streaming messages"
            );

            // Phase 2: Create our own S3Client for chunk-level streaming
            // (so we can preserve log_time from message records)
            let client = S3Client::new(config).map_err(|e| PipelineError::ExecutionFailed {
                stage: "Decoder".to_string(),
                reason: format!("Failed to create S3 client: {e}"),
            })?;

            // Phase 3: Build schema metadata cache and codec factory
            let codec_factory = CodecFactory::new();
            let schema_cache = build_schema_cache(&channels, &codec_factory);

            // Phase 4: Stream chunks and decode messages with timestamps
            let chunk_size: u64 = 10 * 1024 * 1024; // 10MB chunks
            let mut offset = 0u64;
            let mut messages_decoded = 0usize;

            match format {
                robocodec::io::metadata::FileFormat::Mcap => {
                    use robocodec::io::formats::mcap::streaming::McapS3Adapter;
                    let mut adapter = McapS3Adapter::new();

                    while offset < file_size {
                        let fetch_size = chunk_size.min(file_size - offset);
                        let chunk = client
                            .fetch_range(&location, offset, fetch_size)
                            .await
                            .map_err(|e| PipelineError::ExecutionFailed {
                                stage: "Decoder".to_string(),
                                reason: format!("S3 fetch failed at offset {offset}: {e}"),
                            })?;

                        if chunk.is_empty() {
                            break;
                        }
                        offset += chunk.len() as u64;

                        let records = adapter.process_chunk(&chunk).map_err(|e| {
                            PipelineError::ExecutionFailed {
                                stage: "Decoder".to_string(),
                                reason: format!("MCAP parse error: {e}"),
                            }
                        })?;

                        for record in records {
                            let channel_id = record.channel_id;
                            let Some(channel_info) = channels.get(&channel_id) else {
                                continue;
                            };

                            let decoded = decode_raw_message(
                                &record.data,
                                channel_info,
                                &schema_cache,
                                &codec_factory,
                                record.log_time,
                                Some(record.sequence),
                            )?;

                            self.output_tx.send(decoded).map_err(|e| {
                                PipelineError::ChannelError {
                                    from: "Decoder".to_string(),
                                    to: "Aligner".to_string(),
                                    reason: e.to_string(),
                                }
                            })?;

                            messages_decoded += 1;
                            if messages_decoded.is_multiple_of(10000) {
                                tracing::debug!(
                                    messages = messages_decoded,
                                    offset,
                                    "Decoder S3 progress"
                                );
                            }
                        }
                    }
                }
                robocodec::io::metadata::FileFormat::Bag => {
                    use robocodec::io::formats::bag::stream::StreamingBagParser;
                    let mut parser = StreamingBagParser::new();

                    while offset < file_size {
                        let fetch_size = chunk_size.min(file_size - offset);
                        let chunk = client
                            .fetch_range(&location, offset, fetch_size)
                            .await
                            .map_err(|e| PipelineError::ExecutionFailed {
                                stage: "Decoder".to_string(),
                                reason: format!("S3 fetch failed at offset {offset}: {e}"),
                            })?;

                        if chunk.is_empty() {
                            break;
                        }
                        offset += chunk.len() as u64;

                        let records = parser.parse_chunk(&chunk).map_err(|e| {
                            PipelineError::ExecutionFailed {
                                stage: "Decoder".to_string(),
                                reason: format!("BAG parse error: {e}"),
                            }
                        })?;

                        // BAG uses conn_id to map to channels; update channel map
                        // from parser's discovered channels
                        let bag_channels = parser.channels();

                        for record in records {
                            let channel_id = record.conn_id as u16;
                            let channel_info = bag_channels
                                .get(&channel_id)
                                .or_else(|| channels.get(&channel_id));
                            let Some(channel_info) = channel_info else {
                                continue;
                            };

                            let decoded = decode_raw_message(
                                &record.data,
                                channel_info,
                                &schema_cache,
                                &codec_factory,
                                record.log_time,
                                None,
                            )?;

                            self.output_tx.send(decoded).map_err(|e| {
                                PipelineError::ChannelError {
                                    from: "Decoder".to_string(),
                                    to: "Aligner".to_string(),
                                    reason: e.to_string(),
                                }
                            })?;

                            messages_decoded += 1;
                            if messages_decoded.is_multiple_of(10000) {
                                tracing::debug!(
                                    messages = messages_decoded,
                                    offset,
                                    "Decoder S3 progress"
                                );
                            }
                        }
                    }
                }
                other => {
                    return Err(PipelineError::ExecutionFailed {
                        stage: "Decoder".to_string(),
                        reason: format!("S3 streaming not supported for format: {other:?}"),
                    });
                }
            }

            tracing::info!(messages = messages_decoded, "S3 streaming decode complete");

            Ok(DecoderStats {
                messages_decoded,
                duration_sec: 0.0,
            })
        })
    }
}

// =========================================================================
// S3 streaming helpers
// =========================================================================

/// Parse a cloud URL (s3:// or oss://) into an S3Location.
///
/// For OSS URLs, converts to s3:// with endpoint from `OSS_ENDPOINT` env var.
/// For S3 URLs, checks `AWS_ENDPOINT_URL` env var for S3-compatible services (e.g. MinIO).
pub(crate) fn parse_cloud_url_to_s3_location(
    url: &str,
) -> PipelineResult<robocodec::io::s3::S3Location> {
    let s3_url = if let Some(rest) = url.strip_prefix("oss://") {
        let endpoint = std::env::var("OSS_ENDPOINT")
            .unwrap_or_else(|_| "https://oss-cn-hangzhou.aliyuncs.com".to_string());
        format!("s3://{}?endpoint={}", rest, endpoint)
    } else if !url.contains("endpoint=") {
        // For s3:// URLs without an explicit endpoint, check AWS_ENDPOINT_URL
        // (standard env var for S3-compatible services like MinIO)
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

    robocodec::io::s3::S3Location::from_s3_url(&s3_url).map_err(|e| {
        PipelineError::ExecutionFailed {
            stage: "Decoder".to_string(),
            reason: format!("Failed to parse S3 URL '{}': {}", url, e),
        }
    })
}

/// Build S3ReaderConfig from environment variables.
///
/// Checks both AWS and OSS credential env vars for compatibility.
pub(crate) fn build_s3_reader_config() -> PipelineResult<robocodec::io::s3::S3ReaderConfig> {
    use robocodec::io::s3::{AwsCredentials, S3ReaderConfig};

    // Try AWS credentials first, fall back to OSS credentials
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

/// Build a schema metadata cache from channel info, keyed by channel ID.
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

/// Decode raw message bytes using the codec factory and channel metadata.
pub(crate) fn decode_raw_message(
    data: &[u8],
    channel_info: &robocodec::ChannelInfo,
    schema_cache: &HashMap<u16, robocodec::encoding::SchemaMetadata>,
    factory: &robocodec::encoding::CodecFactory,
    log_time: u64,
    sequence: Option<u64>,
) -> PipelineResult<DecodedMessage> {
    let schema =
        schema_cache
            .get(&channel_info.id)
            .ok_or_else(|| PipelineError::ExecutionFailed {
                stage: "Decoder".to_string(),
                reason: format!(
                    "No schema for channel {} (topic: {})",
                    channel_info.id, channel_info.topic
                ),
            })?;

    let encoding = schema.encoding();
    let codec = factory
        .get_codec(encoding)
        .map_err(|e| PipelineError::ExecutionFailed {
            stage: "Decoder".to_string(),
            reason: format!(
                "No codec for encoding {:?} (topic: {}): {}",
                encoding, channel_info.topic, e
            ),
        })?;

    let decoded_fields =
        codec
            .decode_dynamic(data, schema)
            .map_err(|e| PipelineError::ExecutionFailed {
                stage: "Decoder".to_string(),
                reason: format!(
                    "Decode failed for topic {} (type: {}): {}",
                    channel_info.topic, channel_info.message_type, e
                ),
            })?;

    Ok(DecodedMessage {
        topic: channel_info.topic.clone(),
        message_type: channel_info.message_type.clone(),
        log_time,
        sequence,
        data: robocodec::CodecValue::Struct(decoded_fields),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decoder_stage_creation_local() {
        use crossbeam_channel::bounded;
        let (tx, _rx) = bounded(10);
        let stage = DecoderStage::from_path(std::path::PathBuf::from("test.bag"), tx);
        assert!(matches!(stage.input_source, InputSource::LocalFile(_)));
    }

    #[test]
    fn test_decoder_stage_creation_s3() {
        use crossbeam_channel::bounded;
        let (tx, _rx) = bounded(10);
        let stage = DecoderStage::new(InputSource::S3Url("s3://bucket/file.mcap".to_string()), tx);
        assert!(matches!(stage.input_source, InputSource::S3Url(_)));
    }

    #[test]
    fn test_parse_s3_url() {
        let location = parse_cloud_url_to_s3_location("s3://my-bucket/path/to/file.mcap").unwrap();
        assert_eq!(location.bucket(), "my-bucket");
        assert_eq!(location.key(), "path/to/file.mcap");
    }

    #[test]
    fn test_parse_oss_url() {
        // Set OSS_ENDPOINT for the test
        // SAFETY: This test does not run in parallel with other tests that
        // depend on the OSS_ENDPOINT env var.
        unsafe {
            std::env::set_var("OSS_ENDPOINT", "https://oss-cn-hangzhou.aliyuncs.com");
        }
        let location = parse_cloud_url_to_s3_location("oss://my-bucket/path/to/file.bag").unwrap();
        assert_eq!(location.bucket(), "my-bucket");
        assert_eq!(location.key(), "path/to/file.bag");
        assert_eq!(
            location.endpoint(),
            Some("https://oss-cn-hangzhou.aliyuncs.com")
        );
        unsafe {
            std::env::remove_var("OSS_ENDPOINT");
        }
    }

    #[test]
    fn test_build_schema_cache() {
        let factory = robocodec::encoding::CodecFactory::new();
        let mut channels = HashMap::new();
        let mut ch = robocodec::ChannelInfo::new(1, "/test", "test_msgs/Msg");
        ch.encoding = "cdr".to_string();
        ch.schema = Some("int32 value".to_string());
        ch.schema_encoding = Some("ros2msg".to_string());
        channels.insert(1, ch);

        let cache = build_schema_cache(&channels, &factory);
        assert_eq!(cache.len(), 1);
        assert!(cache.contains_key(&1));
    }
}
