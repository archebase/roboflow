// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Streaming dataset converter with bounded memory footprint.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use tracing::{debug, info, instrument, warn};

use crate::DatasetFormat;
use crate::common::DatasetWriter;
use crate::streaming::{
    BackpressureHandler, FrameAlignmentBuffer, StreamingConfig, StreamingStats, TempFileManager,
};
use robocodec::RoboReader;
use roboflow_core::Result;
use roboflow_storage::{LocalStorage, Storage};

/// Progress callback for checkpoint saving during conversion.
///
/// This trait allows the caller to receive progress updates during
/// streaming conversion, enabling periodic checkpoint saves for
/// fault-tolerant distributed processing.
pub trait ProgressCallback: Send + Sync {
    /// Called after each frame is written.
    ///
    /// Parameters:
    /// - `frames_written`: Total number of frames written so far
    /// - `messages_processed`: Total number of messages processed
    /// - `writer`: Reference to the writer (for getting episode index, etc.)
    ///
    /// Returns an error if the callback fails (will abort conversion).
    fn on_frame_written(
        &self,
        frames_written: u64,
        messages_processed: u64,
        writer: &dyn std::any::Any,
    ) -> std::result::Result<(), String>;
}

/// A no-op callback for when checkpointing is not needed.
pub struct NoOpCallback;

impl ProgressCallback for NoOpCallback {
    fn on_frame_written(
        &self,
        _frames_written: u64,
        _messages_processed: u64,
        _writer: &dyn std::any::Any,
    ) -> std::result::Result<(), String> {
        std::result::Result::Ok(())
    }
}

/// Streaming dataset converter.
///
/// Converts input files (MCAP/Bag) directly to dataset formats using
/// a streaming architecture with bounded memory footprint.
///
/// # Storage Support
///
/// The converter supports both local and cloud storage backends:
/// - **Input storage**: Downloads cloud files to temp directory before processing
/// - **Output storage**: Writes output files directly to the configured backend
///
/// # Deprecation Notice
///
/// **This type is deprecated**. Please migrate to the new pipeline-v2 API:
///
/// ```rust,no_run
/// // Old (deprecated)
/// let converter = StreamingDatasetConverter::new_lerobot(output_dir, config)?;
/// let stats = converter.convert(input_file)?;
///
/// // New (recommended)
/// let source = roboflow_sources::SourceConfig::mcap(input_file);
/// let sink = roboflow_sinks::SinkConfig::lerobot(output_dir);
/// let stats = roboflow_pipeline::Pipeline::run(source, sink).await?;
/// ```
///
/// The new API provides:
/// - Better separation of concerns (Source/Sink abstraction)
/// - Easier to extend with new formats
/// - More flexible pipeline configuration
/// - Better testability
#[deprecated(
    since = "0.2.0",
    note = "Use the pipeline-v2 API (Source/Sink traits) instead"
)]
pub struct StreamingDatasetConverter {
    /// Output directory (local buffer for temporary files)
    output_dir: PathBuf,

    /// Dataset format
    format: DatasetFormat,

    /// Configuration for KPS format
    kps_config: Option<crate::kps::config::KpsConfig>,

    /// Configuration for LeRobot format
    lerobot_config: Option<crate::lerobot::config::LerobotConfig>,

    /// Streaming configuration
    config: StreamingConfig,

    /// Input storage backend for reading input files
    input_storage: Option<Arc<dyn Storage>>,

    /// Output storage backend for writing output files
    output_storage: Option<Arc<dyn Storage>>,

    /// Output prefix within storage (e.g., "datasets/my_dataset")
    output_prefix: Option<String>,

    /// Optional progress callback for checkpointing
    progress_callback: Option<Arc<dyn ProgressCallback>>,
}

#[allow(deprecated)]
impl StreamingDatasetConverter {
    /// Create a new streaming converter for KPS format.
    pub fn new_kps<P: AsRef<Path>>(
        output_dir: P,
        kps_config: crate::kps::config::KpsConfig,
        config: StreamingConfig,
    ) -> Result<Self> {
        let config = config.resolve_decoder();
        Ok(Self {
            output_dir: output_dir.as_ref().to_path_buf(),
            format: DatasetFormat::Kps,
            kps_config: Some(kps_config),
            lerobot_config: None,
            config,
            input_storage: None,
            output_storage: None,
            output_prefix: None,
            progress_callback: None,
        })
    }

    /// Create a new streaming converter for KPS format with storage backends.
    pub fn new_kps_with_storage<P: AsRef<Path>>(
        output_dir: P,
        kps_config: crate::kps::config::KpsConfig,
        config: StreamingConfig,
        input_storage: Option<Arc<dyn Storage>>,
        output_storage: Option<Arc<dyn Storage>>,
    ) -> Result<Self> {
        let config = config.resolve_decoder();
        Ok(Self {
            output_dir: output_dir.as_ref().to_path_buf(),
            format: DatasetFormat::Kps,
            kps_config: Some(kps_config),
            lerobot_config: None,
            config,
            input_storage,
            output_storage,
            output_prefix: None,
            progress_callback: None,
        })
    }

    /// Create a new streaming converter for LeRobot format.
    pub fn new_lerobot<P: AsRef<Path>>(
        output_dir: P,
        lerobot_config: crate::lerobot::config::LerobotConfig,
    ) -> Result<Self> {
        let fps = lerobot_config.dataset.fps;
        // Require observation.state for LeRobot datasets; resolve_decoder so one decoder is shared by all alignment buffers
        let config = StreamingConfig::with_fps(fps)
            .require_feature("observation.state")
            .resolve_decoder();
        Ok(Self {
            output_dir: output_dir.as_ref().to_path_buf(),
            format: DatasetFormat::Lerobot,
            kps_config: None,
            lerobot_config: Some(lerobot_config),
            config,
            input_storage: None,
            output_storage: None,
            output_prefix: None,
            progress_callback: None,
        })
    }

    /// Create a new streaming converter for LeRobot format with storage backends.
    pub fn new_lerobot_with_storage<P: AsRef<Path>>(
        output_dir: P,
        lerobot_config: crate::lerobot::config::LerobotConfig,
        input_storage: Option<Arc<dyn Storage>>,
        output_storage: Option<Arc<dyn Storage>>,
    ) -> Result<Self> {
        let fps = lerobot_config.dataset.fps;
        // Require observation.state for LeRobot datasets; resolve_decoder so one decoder is shared
        let config = StreamingConfig::with_fps(fps)
            .require_feature("observation.state")
            .resolve_decoder();
        Ok(Self {
            output_dir: output_dir.as_ref().to_path_buf(),
            format: DatasetFormat::Lerobot,
            kps_config: None,
            lerobot_config: Some(lerobot_config),
            config,
            input_storage,
            output_storage,
            output_prefix: None,
            progress_callback: None,
        })
    }

    /// Set the input storage backend.
    pub fn with_input_storage(mut self, storage: Arc<dyn Storage>) -> Self {
        self.input_storage = Some(storage);
        self
    }

    /// Set the output storage backend.
    pub fn with_output_storage(mut self, storage: Arc<dyn Storage>) -> Self {
        self.output_storage = Some(storage);
        self
    }

    /// Set the output prefix within storage.
    ///
    /// This is the path prefix within the storage backend where output files will be written.
    /// For example, with prefix "datasets/my_dataset", files will be written to:
    /// - "datasets/my_dataset/data/chunk-000/episode_000000.parquet"
    /// - "datasets/my_dataset/videos/chunk-000/..."
    pub fn with_output_prefix(mut self, prefix: String) -> Self {
        self.output_prefix = Some(prefix);
        self
    }

    /// Set the progress callback for checkpointing.
    pub fn with_progress_callback(mut self, callback: Arc<dyn ProgressCallback>) -> Self {
        self.progress_callback = Some(callback);
        self
    }

    /// Set the completion window (in frames).
    pub fn with_completion_window(mut self, frames: usize) -> Self {
        self.config.completion_window_frames = frames;
        self
    }

    /// Set the maximum buffered frames.
    pub fn with_max_buffered_frames(mut self, max: usize) -> Self {
        self.config.max_buffered_frames = max;
        self
    }

    /// Set the maximum buffered memory (in MB).
    pub fn with_max_memory_mb(mut self, mb: usize) -> Self {
        self.config.max_buffered_memory_mb = mb;
        self
    }

    /// Extract the object key from a cloud storage URL.
    ///
    /// Convert input file to dataset format.
    ///
    /// For cloud URLs (s3://, oss://), uses robocodec's S3 streaming to read
    /// messages directly from cloud storage via HTTP range requests -- no temp
    /// files are created.  For local files, uses RoboReader as before.
    #[instrument(skip_all, fields(
        input = %input_path.as_ref().display(),
        output = %self.output_dir.display(),
        format = ?self.format,
    ))]
    pub fn convert<P: AsRef<Path>>(self, input_path: P) -> Result<StreamingStats> {
        let input_path = input_path.as_ref();

        info!(
            input = %input_path.display(),
            output = %self.output_dir.display(),
            format = ?self.format,
            "Starting streaming dataset conversion"
        );

        // Detect if input_path is a cloud storage URL (s3:// or oss://)
        let input_path_str = input_path.to_string_lossy();
        let is_cloud_url =
            input_path_str.starts_with("s3://") || input_path_str.starts_with("oss://");

        if is_cloud_url {
            // Direct S3 streaming path -- no temp files
            self.convert_from_s3(&input_path_str)
        } else {
            // Local file path -- use RoboReader
            self.convert_from_local(input_path)
        }
    }

    /// Convert from a local file using RoboReader.
    fn convert_from_local(self, input_path: &Path) -> Result<StreamingStats> {
        let start_time = Instant::now();

        // Resolve input storage
        let input_storage = if let Some(storage) = &self.input_storage {
            storage.clone()
        } else {
            Arc::new(LocalStorage::new(
                input_path.parent().unwrap_or(Path::new(".")),
            )) as Arc<dyn Storage>
        };

        let temp_dir = self
            .config
            .temp_dir
            .clone()
            .unwrap_or_else(std::env::temp_dir);

        // For local storage, pass just the filename (not full path)
        let storage_path = if input_storage.as_any().is::<LocalStorage>() {
            input_path.file_name().unwrap_or(input_path.as_os_str())
        } else {
            input_path.as_os_str()
        };
        let storage_path = Path::new(storage_path);

        let _temp_manager = match TempFileManager::new(input_storage, storage_path, &temp_dir) {
            Ok(manager) => manager,
            Err(e) => {
                return Err(roboflow_core::RoboflowError::other(format!(
                    "Failed to prepare input file: {}",
                    e
                )));
            }
        };

        let process_path = _temp_manager.path();

        info!(
            input = %input_path.display(),
            process_path = %process_path.display(),
            is_temp = _temp_manager.is_temp(),
            "Processing input file (local)"
        );

        let mut writer = self.create_writer()?;
        let mut aligner = FrameAlignmentBuffer::new(self.config.clone());
        let mut backpressure = BackpressureHandler::from_config(&self.config);
        let topic_mappings = self.build_topic_mappings()?;

        let path_str = process_path
            .to_str()
            .ok_or_else(|| roboflow_core::RoboflowError::parse("Path", "Invalid UTF-8 path"))?;
        let reader = RoboReader::open(path_str)?;

        info!(
            mappings = topic_mappings.len(),
            "Starting message processing"
        );

        let mut stats = StreamingStats::default();
        let mut unmapped_warning_shown: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        for msg_result in reader.decoded()? {
            let msg_result = msg_result?;
            stats.messages_processed += 1;

            let mapping = match topic_mappings.get(&msg_result.channel.topic) {
                Some(m) => m,
                None => {
                    if unmapped_warning_shown.insert(msg_result.channel.topic.clone()) {
                        tracing::warn!(
                            topic = %msg_result.channel.topic,
                            "Message from unmapped topic will be ignored. Add this topic to your configuration if needed."
                        );
                    }
                    aligner.stats_mut().record_unmapped_message();
                    continue;
                }
            };

            let msg = crate::streaming::alignment::TimestampedMessage {
                log_time: msg_result.log_time.unwrap_or(0),
                message: msg_result.message,
            };

            let completed_frames = aligner.process_message(&msg, &mapping.feature);
            self.write_frames(
                &completed_frames,
                &mut writer,
                &mut stats,
                &mut backpressure,
                &aligner,
                &start_time,
            )?;

            self.apply_backpressure_if_needed(
                &mut aligner,
                &mut writer,
                &mut stats,
                &mut backpressure,
            )?;

            if stats.messages_processed.is_multiple_of(1000) {
                let elapsed = start_time.elapsed().as_secs_f64();
                let throughput = stats.messages_processed as f64 / elapsed;
                info!(
                    messages = stats.messages_processed,
                    frames = stats.frames_written,
                    buffer = aligner.len(),
                    throughput = format!("{:.0} msg/s", throughput),
                    "Progress update"
                );
            }
        }

        self.finalize_conversion(aligner, writer, stats, start_time)
    }

    /// Convert from S3/OSS using direct streaming -- no temp files.
    ///
    /// Uses robocodec's S3Client + format-specific streaming parsers to stream
    /// messages directly from cloud storage via HTTP range requests, preserving
    /// message timing metadata (log_time, sequence).
    fn convert_from_s3(self, url: &str) -> Result<StreamingStats> {
        use robocodec::FormatReader as _;
        use robocodec::encoding::CodecFactory;
        use robocodec::io::s3::{S3Client, S3Reader};

        use crate::streaming::pipeline::stages::decoder::{
            build_s3_reader_config, build_schema_cache, decode_raw_message,
            parse_cloud_url_to_s3_location,
        };

        let start_time = Instant::now();

        info!(url = %url, "Starting S3 streaming conversion (no temp files)");

        let location = parse_cloud_url_to_s3_location(url).map_err(|e| {
            roboflow_core::RoboflowError::other(format!("Failed to parse S3 URL: {e}"))
        })?;
        info!(
            bucket = %location.bucket(),
            key = %location.key(),
            endpoint = ?location.endpoint(),
            region = ?location.region(),
            resolved_url = %location.url(),
            "S3 location parsed"
        );
        let config = build_s3_reader_config().map_err(|e| {
            roboflow_core::RoboflowError::other(format!("Failed to build S3 config: {e}"))
        })?;
        info!(
            has_credentials = config.credentials().is_some(),
            "S3 reader config built"
        );

        // Create a tokio runtime for async S3 operations
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                roboflow_core::RoboflowError::other(format!("Failed to create async runtime: {e}"))
            })?;

        rt.block_on(async {
            // Phase 1: S3Reader initialization (two-tier header scan for channels)
            let reader = S3Reader::open_with_config(location.clone(), config.clone())
                .await
                .map_err(|e| {
                    roboflow_core::RoboflowError::other(format!(
                        "Failed to open S3 reader for '{}': {e}",
                        url
                    ))
                })?;

            let channels = reader.channels().clone();
            let file_size = reader.file_size();
            let format = reader.format();

            info!(
                url = %url,
                format = ?format,
                channels = channels.len(),
                file_size,
                "S3 reader initialized, streaming messages"
            );

            // Phase 2: Create S3Client for chunk-level streaming with timestamps
            let client = S3Client::new(config).map_err(|e| {
                roboflow_core::RoboflowError::other(format!("Failed to create S3 client: {e}"))
            })?;

            // Phase 3: Build codec infrastructure
            let codec_factory = CodecFactory::new();
            let schema_cache = build_schema_cache(&channels, &codec_factory);
            let topic_mappings = self.build_topic_mappings()?;

            info!(
                topic_mappings = topic_mappings.len(),
                topics = ?topic_mappings.keys().collect::<Vec<_>>(),
                "Topic mappings built for S3 streaming"
            );

            let mut writer = self.create_writer()?;
            let mut aligner = FrameAlignmentBuffer::new(self.config.clone());
            let mut backpressure = BackpressureHandler::from_config(&self.config);
            let mut stats = StreamingStats::default();
            let mut unmapped_warning_shown: std::collections::HashSet<String> =
                std::collections::HashSet::new();

            // Phase 4: Stream chunks, decode, and align
            let chunk_size: u64 = 10 * 1024 * 1024; // 10MB
            let mut offset = 0u64;

            match format {
                robocodec::io::metadata::FileFormat::Mcap => {
                    use robocodec::io::formats::mcap::streaming::McapS3Adapter;
                    let mut adapter = McapS3Adapter::new();

                    while offset < file_size {
                        let fetch_size = chunk_size.min(file_size - offset);
                        let chunk = client
                            .fetch_range(&location, offset, fetch_size)
                            .await
                            .map_err(|e| {
                                roboflow_core::RoboflowError::other(format!(
                                    "S3 fetch failed at offset {offset}: {e}"
                                ))
                            })?;
                        if chunk.is_empty() {
                            break;
                        }
                        offset += chunk.len() as u64;

                        let records = match adapter.process_chunk(&chunk) {
                            Ok(r) => r,
                            Err(e) => {
                                warn!(offset, error = %e, "MCAP parse error, skipping chunk");
                                continue;
                            }
                        };

                        for record in records {
                            let Some(channel_info) = channels.get(&record.channel_id) else {
                                continue;
                            };

                            let decoded_msg = decode_raw_message(
                                &record.data,
                                channel_info,
                                &schema_cache,
                                &codec_factory,
                                record.log_time,
                                Some(record.sequence),
                            )
                            .map_err(|e| {
                                roboflow_core::RoboflowError::other(format!("Decode failed: {e}"))
                            })?;

                            stats.messages_processed += 1;
                            self.process_decoded_message(
                                &decoded_msg,
                                &topic_mappings,
                                &mut unmapped_warning_shown,
                                &mut aligner,
                                &mut writer,
                                &mut stats,
                                &mut backpressure,
                                &start_time,
                            )?;
                        }
                    }
                }
                robocodec::io::metadata::FileFormat::Bag => {
                    use robocodec::encoding::CdrDecoder;
                    use robocodec::io::formats::bag::stream::StreamingBagParser;
                    let mut parser = StreamingBagParser::new();
                    let mut total_records: u64 = 0;
                    let mut total_chunks_fetched: u64 = 0;
                    let mut channel_miss: u64 = 0;
                    // ROS1 bag messages use ROS1 serialization (not standard CDR).
                    // We need a CdrDecoder and parsed schemas for decode_headerless_ros1.
                    let ros1_decoder = CdrDecoder::new();
                    let mut ros1_schema_cache: HashMap<
                        u16,
                        robocodec::schema::MessageSchema,
                    > = HashMap::new();
                    let mut known_channel_count: usize = 0;

                    while offset < file_size {
                        let fetch_size = chunk_size.min(file_size - offset);
                        let chunk = client
                            .fetch_range(&location, offset, fetch_size)
                            .await
                            .map_err(|e| {
                                roboflow_core::RoboflowError::other(format!(
                                    "S3 fetch failed at offset {offset}: {e}"
                                ))
                            })?;
                        if chunk.is_empty() {
                            info!(offset, file_size, "Empty chunk received, stopping");
                            break;
                        }
                        offset += chunk.len() as u64;
                        total_chunks_fetched += 1;

                        let records = match parser.parse_chunk(&chunk) {
                            Ok(r) => r,
                            Err(e) => {
                                warn!(offset, error = %e, "BAG parse error, skipping chunk");
                                continue;
                            }
                        };

                        if total_chunks_fetched <= 3 || total_chunks_fetched.is_multiple_of(50) {
                            let bag_channels = parser.channels();
                            info!(
                                chunk = total_chunks_fetched,
                                offset,
                                records_in_chunk = records.len(),
                                bag_channels = bag_channels.len(),
                                total_records,
                                "BAG streaming progress"
                            );
                        }

                        let bag_channels = parser.channels();

                        // Rebuild ROS1 schema cache when new channels are discovered
                        if bag_channels.len() > known_channel_count {
                            for (&id, ch) in &bag_channels {
                                if ros1_schema_cache.contains_key(&id) {
                                    continue;
                                }
                                if let Some(schema_text) = &ch.schema {
                                    match robocodec::schema::parse_schema(
                                        &ch.message_type,
                                        schema_text,
                                    ) {
                                        Ok(parsed) => {
                                            ros1_schema_cache.insert(id, parsed);
                                        }
                                        Err(e) => {
                                            warn!(
                                                channel_id = id,
                                                topic = %ch.topic,
                                                error = %e,
                                                "Failed to parse ROS1 schema, skipping channel"
                                            );
                                        }
                                    }
                                }
                            }
                            known_channel_count = bag_channels.len();
                            debug!(
                                known_channel_count,
                                schemas = ros1_schema_cache.len(),
                                "Rebuilt ROS1 schema cache with new BAG channels"
                            );
                        }

                        for record in records {
                            total_records += 1;
                            let channel_id = record.conn_id as u16;
                            let channel_info = bag_channels
                                .get(&channel_id)
                                .or_else(|| channels.get(&channel_id));
                            let Some(channel_info) = channel_info else {
                                channel_miss += 1;
                                if channel_miss <= 5 {
                                    info!(
                                        conn_id = record.conn_id,
                                        channel_id,
                                        bag_channels = bag_channels.len(),
                                        "No channel info for record"
                                    );
                                }
                                continue;
                            };

                            // ROS1 bag messages use ROS1 serialization, not standard CDR.
                            // We must use decode_headerless_ros1 (matching ParallelBagReader).
                            let decoded_msg = decode_ros1_message(
                                &record.data,
                                channel_info,
                                &ros1_schema_cache,
                                &ros1_decoder,
                                record.log_time,
                            )
                            .map_err(|e| {
                                roboflow_core::RoboflowError::other(format!("Decode failed: {e}"))
                            })?;

                            stats.messages_processed += 1;
                            self.process_decoded_message(
                                &decoded_msg,
                                &topic_mappings,
                                &mut unmapped_warning_shown,
                                &mut aligner,
                                &mut writer,
                                &mut stats,
                                &mut backpressure,
                                &start_time,
                            )?;
                        }
                    }

                    info!(
                        total_chunks_fetched,
                        total_records,
                        channel_miss,
                        messages_processed = stats.messages_processed,
                        bag_channels = parser.channels().len(),
                        bag_channel_topics = ?parser.channels().values().map(|c| &c.topic).collect::<Vec<_>>(),
                        "BAG streaming complete"
                    );
                }
                other => {
                    return Err(roboflow_core::RoboflowError::other(format!(
                        "S3 streaming not supported for format: {other:?}"
                    )));
                }
            }

            self.finalize_conversion(aligner, writer, stats, start_time)
        })
    }

    /// Process a single decoded message through alignment + writing.
    #[allow(clippy::too_many_arguments)]
    fn process_decoded_message(
        &self,
        decoded_msg: &crate::streaming::pipeline::types::DecodedMessage,
        topic_mappings: &MappingMap,
        unmapped_warning_shown: &mut std::collections::HashSet<String>,
        aligner: &mut FrameAlignmentBuffer,
        writer: &mut Box<dyn DatasetWriter>,
        stats: &mut StreamingStats,
        backpressure: &mut BackpressureHandler,
        start_time: &Instant,
    ) -> Result<()> {
        let mapping = match topic_mappings.get(&decoded_msg.topic) {
            Some(m) => m,
            None => {
                if unmapped_warning_shown.insert(decoded_msg.topic.clone()) {
                    tracing::warn!(
                        topic = %decoded_msg.topic,
                        "Message from unmapped topic will be ignored."
                    );
                }
                aligner.stats_mut().record_unmapped_message();
                return Ok(());
            }
        };

        // Extract the decoded fields from the CodecValue::Struct wrapper
        let message = match &decoded_msg.data {
            robocodec::CodecValue::Struct(fields) => fields.clone(),
            _ => std::collections::HashMap::new(),
        };

        let msg = crate::streaming::alignment::TimestampedMessage {
            log_time: decoded_msg.log_time,
            message,
        };

        let completed_frames = aligner.process_message(&msg, &mapping.feature);
        self.write_frames(
            &completed_frames,
            writer,
            stats,
            backpressure,
            aligner,
            start_time,
        )?;

        self.apply_backpressure_if_needed(aligner, writer, stats, backpressure)?;

        if stats.messages_processed.is_multiple_of(1000) {
            let elapsed = start_time.elapsed().as_secs_f64();
            let throughput = stats.messages_processed as f64 / elapsed;
            info!(
                messages = stats.messages_processed,
                frames = stats.frames_written,
                buffer = aligner.len(),
                throughput = format!("{:.0} msg/s", throughput),
                "Progress update"
            );
        }

        Ok(())
    }

    /// Write completed frames to the writer.
    fn write_frames(
        &self,
        frames: &[crate::common::AlignedFrame],
        writer: &mut Box<dyn DatasetWriter>,
        stats: &mut StreamingStats,
        backpressure: &mut BackpressureHandler,
        aligner: &FrameAlignmentBuffer,
        _start_time: &Instant,
    ) -> Result<()> {
        for frame in frames {
            writer.write_frame(frame)?;
            stats.frames_written += 1;

            if let Some(ref callback) = self.progress_callback
                && let Err(e) = callback.on_frame_written(
                    stats.frames_written as u64,
                    stats.messages_processed as u64,
                    writer.as_any(),
                )
            {
                return Err(roboflow_core::RoboflowError::other(format!(
                    "Progress callback failed: {}",
                    e
                )));
            }

            backpressure.update_memory_estimate(aligner);
        }
        Ok(())
    }

    /// Apply backpressure if needed by flushing the alignment buffer.
    fn apply_backpressure_if_needed(
        &self,
        aligner: &mut FrameAlignmentBuffer,
        writer: &mut Box<dyn DatasetWriter>,
        stats: &mut StreamingStats,
        backpressure: &mut BackpressureHandler,
    ) -> Result<()> {
        if backpressure.should_apply_backpressure(aligner) && !backpressure.is_in_cooldown() {
            info!(
                buffer_size = aligner.len(),
                memory_mb = backpressure.memory_mb(),
                "Applying backpressure"
            );

            let force_completed = aligner.flush();
            for frame in force_completed {
                writer.write_frame(&frame)?;
                stats.frames_written += 1;
                stats.force_completed_frames += 1;

                if let Some(ref callback) = self.progress_callback
                    && let Err(e) = callback.on_frame_written(
                        stats.frames_written as u64,
                        stats.messages_processed as u64,
                        writer.as_any(),
                    )
                {
                    return Err(roboflow_core::RoboflowError::other(format!(
                        "Progress callback failed: {}",
                        e
                    )));
                }
            }

            backpressure.record_backpressure();
        }
        Ok(())
    }

    /// Finalize conversion: flush remaining frames, finalize writer, compile stats.
    fn finalize_conversion(
        &self,
        mut aligner: FrameAlignmentBuffer,
        mut writer: Box<dyn DatasetWriter>,
        mut stats: StreamingStats,
        start_time: Instant,
    ) -> Result<StreamingStats> {
        info!(
            remaining_frames = aligner.len(),
            "Flushing remaining frames"
        );

        let remaining = aligner.flush();
        for frame in remaining {
            writer.write_frame(&frame)?;
            stats.frames_written += 1;
            stats.force_completed_frames += 1;
        }

        let writer_stats = writer.finalize()?;

        stats.duration_sec = start_time.elapsed().as_secs_f64();
        stats.writer_stats = writer_stats;
        stats.avg_buffer_size = aligner.stats().peak_buffer_size as f32;
        stats.peak_memory_mb = 0.0;

        info!(
            frames_written = stats.frames_written,
            messages = stats.messages_processed,
            duration_sec = stats.duration_sec,
            throughput_fps = stats.throughput_fps(),
            "Streaming conversion complete"
        );

        Ok(stats)
    }

    /// Create the appropriate dataset writer.
    fn create_writer(&self) -> Result<Box<dyn DatasetWriter>> {
        use crate::{DatasetConfig, create_writer};

        match self.format {
            DatasetFormat::Kps => {
                let kps_config = self.kps_config.as_ref().ok_or_else(|| {
                    roboflow_core::RoboflowError::parse(
                        "StreamingConverter",
                        "KPS config required but not provided",
                    )
                })?;
                let config = DatasetConfig::Kps(kps_config.clone());
                // KPS doesn't support cloud storage yet
                create_writer(&self.output_dir, None, None, &config).map_err(|e| {
                    roboflow_core::RoboflowError::encode(
                        "StreamingConverter",
                        format!(
                            "Failed to create KPS writer at {}: {}",
                            self.output_dir.display(),
                            e
                        ),
                    )
                })
            }
            DatasetFormat::Lerobot => {
                let lerobot_config = self.lerobot_config.as_ref().ok_or_else(|| {
                    roboflow_core::RoboflowError::parse(
                        "StreamingConverter",
                        "LeRobot config required but not provided",
                    )
                })?;
                let config = DatasetConfig::Lerobot(lerobot_config.clone());
                // Use cloud storage if available
                let storage_ref = self.output_storage.as_ref();
                let prefix_ref = self.output_prefix.as_deref();
                create_writer(&self.output_dir, storage_ref, prefix_ref, &config).map_err(|e| {
                    roboflow_core::RoboflowError::encode(
                        "StreamingConverter",
                        format!(
                            "Failed to create LeRobot writer at {}: {}",
                            self.output_dir.display(),
                            e
                        ),
                    )
                })
            }
        }
    }

    /// Build topic -> feature mapping lookup.
    fn build_topic_mappings(&self) -> Result<MappingMap> {
        let mut map = HashMap::new();

        match self.format {
            DatasetFormat::Kps => {
                if let Some(config) = &self.kps_config {
                    for mapping in &config.mappings {
                        map.insert(
                            mapping.topic.clone(),
                            Mapping {
                                feature: mapping.feature.clone(),
                                _mapping_type: match mapping.mapping_type {
                                    crate::kps::MappingType::Image => "image",
                                    crate::kps::MappingType::State => "state",
                                    crate::kps::MappingType::Action => "action",
                                    _ => "state",
                                },
                            },
                        );
                    }
                }
            }
            DatasetFormat::Lerobot => {
                if let Some(config) = &self.lerobot_config {
                    for mapping in &config.mappings {
                        map.insert(
                            mapping.topic.clone(),
                            Mapping {
                                feature: mapping.feature.clone(),
                                _mapping_type: match mapping.mapping_type {
                                    crate::lerobot::config::MappingType::Image => "image",
                                    crate::lerobot::config::MappingType::State => "state",
                                    crate::lerobot::config::MappingType::Action => "action",
                                    crate::lerobot::config::MappingType::Timestamp => "timestamp",
                                    _ => "state",
                                },
                            },
                        );
                    }
                }
            }
        }

        Ok(map)
    }
}

/// Topic mapping for looking up feature names.
type MappingMap = HashMap<String, Mapping>;

/// Mapping from topic to feature.
#[derive(Debug, Clone)]
struct Mapping {
    feature: String,
    /// Data type for validation/routing (reserved for future use)
    /// Values: "image", "state", "action", "timestamp"
    _mapping_type: &'static str,
}

/// Decode a ROS1 bag message using the ROS1-specific headerless decoder.
///
/// ROS1 messages use a different serialization format from CDR (ROS2).
/// This must be used instead of `decode_raw_message` for BAG file data.
fn decode_ros1_message(
    data: &[u8],
    channel_info: &robocodec::ChannelInfo,
    schema_cache: &HashMap<u16, robocodec::schema::MessageSchema>,
    decoder: &robocodec::encoding::CdrDecoder,
    log_time: u64,
) -> Result<crate::streaming::pipeline::types::DecodedMessage> {
    let schema = schema_cache.get(&channel_info.id).ok_or_else(|| {
        roboflow_core::RoboflowError::other(format!(
            "No ROS1 schema for channel {} (topic: {})",
            channel_info.id, channel_info.topic
        ))
    })?;

    let decoded_fields = decoder
        .decode_headerless_ros1(schema, data, Some(&channel_info.message_type))
        .map_err(|e| {
            roboflow_core::RoboflowError::other(format!(
                "ROS1 decode failed for topic {} (type: {}): {}",
                channel_info.topic, channel_info.message_type, e
            ))
        })?;

    Ok(crate::streaming::pipeline::types::DecodedMessage {
        topic: channel_info.topic.clone(),
        message_type: channel_info.message_type.clone(),
        log_time,
        sequence: None,
        data: robocodec::CodecValue::Struct(decoded_fields),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn test_converter_creation() {
        // Basic test that the converter can be created
        let lerobot_config = crate::lerobot::config::LerobotConfig {
            dataset: crate::lerobot::config::DatasetConfig {
                base: crate::common::config::DatasetBaseConfig {
                    name: "test".to_string(),
                    fps: 30,
                    robot_type: None,
                },
                env_type: None,
            },
            mappings: vec![],
            video: Default::default(),
            annotation_file: None,
        };

        let converter = StreamingDatasetConverter::new_lerobot("/tmp/test", lerobot_config);

        assert!(converter.is_ok());
    }

    #[test]
    fn test_noop_callback() {
        // Test that NoOpCallback works without error
        let callback = NoOpCallback;
        assert!(callback.on_frame_written(100, 1000, &()).is_ok());
        assert!(callback.on_frame_written(200, 2000, &()).is_ok());
    }

    #[test]
    fn test_progress_callback_invocation() {
        // Test callback that counts invocations
        struct CountingCallback {
            call_count: Arc<AtomicU64>,
            last_frames: Arc<AtomicU64>,
        }

        impl ProgressCallback for CountingCallback {
            fn on_frame_written(
                &self,
                frames_written: u64,
                _messages_processed: u64,
                _writer: &dyn std::any::Any,
            ) -> std::result::Result<(), String> {
                self.call_count.fetch_add(1, Ordering::Relaxed);
                self.last_frames.store(frames_written, Ordering::Relaxed);
                std::result::Result::Ok(())
            }
        }

        let call_count = Arc::new(AtomicU64::new(0));
        let last_frames = Arc::new(AtomicU64::new(0));

        let callback = CountingCallback {
            call_count: call_count.clone(),
            last_frames: last_frames.clone(),
        };

        // Simulate callback invocations
        callback.on_frame_written(1, 10, &()).unwrap();
        callback.on_frame_written(2, 20, &()).unwrap();
        callback.on_frame_written(3, 30, &()).unwrap();

        assert_eq!(call_count.load(Ordering::Relaxed), 3);
        assert_eq!(last_frames.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn test_callback_returns_error() {
        // Test that callback errors are propagated
        struct ErrorCallback;

        impl ProgressCallback for ErrorCallback {
            fn on_frame_written(
                &self,
                _frames_written: u64,
                _messages_processed: u64,
                _writer: &dyn std::any::Any,
            ) -> std::result::Result<(), String> {
                std::result::Result::Err("test error".to_string())
            }
        }

        let callback = ErrorCallback;
        let result = callback.on_frame_written(1, 10, &());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "test error");
    }
}
