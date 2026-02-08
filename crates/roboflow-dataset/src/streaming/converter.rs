// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Streaming dataset converter with bounded memory footprint.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use tracing::{info, instrument, warn};

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

impl StreamingDatasetConverter {
    /// Create a new streaming converter for KPS format.
    pub fn new_kps<P: AsRef<Path>>(
        output_dir: P,
        kps_config: crate::kps::config::KpsConfig,
        config: StreamingConfig,
    ) -> Result<Self> {
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
        // Require observation.state for LeRobot datasets
        let config = StreamingConfig::with_fps(fps).require_feature("observation.state");
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
        // Require observation.state for LeRobot datasets
        let config = StreamingConfig::with_fps(fps).require_feature("observation.state");
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
    /// For example:
    /// - `s3://my-bucket/path/to/file.bag` → `path/to/file.bag`
    /// - `oss://my-bucket/file.bag` → `file.bag`
    ///
    /// Returns `None` if the URL is not a valid S3/OSS URL.
    fn extract_cloud_key(url: &str) -> Option<&str> {
        let rest = if let Some(r) = url.strip_prefix("s3://") {
            r
        } else if let Some(r) = url.strip_prefix("oss://") {
            r
        } else {
            return None;
        };

        // Find the first '/' to split bucket/key
        rest.find('/').map(|idx| &rest[idx + 1..])
    }

    /// Create cloud storage backend from URL for S3/OSS inputs.
    ///
    /// This is used when the converter receives an S3 or OSS URL directly
    /// (without input_storage being set by the worker).
    fn create_cloud_storage(&self, url: &str) -> Result<Arc<dyn Storage>> {
        use roboflow_storage::{OssConfig, OssStorage};
        use std::env;

        // Parse URL to get bucket from the URL
        let rest = if let Some(r) = url.strip_prefix("s3://") {
            r
        } else if let Some(r) = url.strip_prefix("oss://") {
            r
        } else {
            return Err(roboflow_core::RoboflowError::other(format!(
                "Unsupported cloud storage URL: {}",
                url
            )));
        };

        // Split bucket/key - we only need the bucket for storage creation
        let (bucket, _key) = rest.split_once('/').ok_or_else(|| {
            roboflow_core::RoboflowError::other(format!("Invalid cloud URL: {}", url))
        })?;

        // Get credentials from environment
        let access_key_id = env::var("AWS_ACCESS_KEY_ID")
            .or_else(|_| env::var("OSS_ACCESS_KEY_ID"))
            .map_err(|_| roboflow_core::RoboflowError::other(
                "Cloud storage credentials not found. Set AWS_ACCESS_KEY_ID or OSS_ACCESS_KEY_ID".to_string(),
            ))?;

        let access_key_secret = env::var("AWS_SECRET_ACCESS_KEY")
            .or_else(|_| env::var("OSS_ACCESS_KEY_SECRET"))
            .map_err(|_| roboflow_core::RoboflowError::other(
                "Cloud storage credentials not found. Set AWS_SECRET_ACCESS_KEY or OSS_ACCESS_KEY_SECRET".to_string(),
            ))?;

        // Get endpoint from environment or construct from URL
        let endpoint = env::var("AWS_ENDPOINT_URL")
            .or_else(|_| env::var("OSS_ENDPOINT"))
            .unwrap_or_else(|_| {
                // For MinIO or local testing, default to localhost
                if url.contains("127.0.0.1") || url.contains("localhost") {
                    "http://127.0.0.1:9000".to_string()
                } else {
                    "https://s3.amazonaws.com".to_string()
                }
            });

        let region = env::var("AWS_REGION").ok();

        // Create OSS config
        let mut oss_config =
            OssConfig::new(bucket, endpoint.clone(), access_key_id, access_key_secret);
        if let Some(reg) = region {
            oss_config = oss_config.with_region(reg);
        }
        // Enable HTTP if endpoint uses http://
        if endpoint.starts_with("http://") {
            oss_config = oss_config.with_allow_http(true);
        }

        // Create OssStorage
        let storage = OssStorage::with_config(oss_config.clone()).map_err(|e| {
            roboflow_core::RoboflowError::other(format!(
                "Failed to create cloud storage for bucket '{}' with endpoint '{}': {}",
                bucket,
                oss_config.endpoint_url(),
                e
            ))
        })?;

        Ok(Arc::new(storage) as Arc<dyn Storage>)
    }

    /// Convert input file to dataset format.
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

        let start_time = Instant::now();

        // Detect if input_path is a cloud storage URL (s3:// or oss://)
        let input_path_str = input_path.to_string_lossy();
        let is_cloud_url =
            input_path_str.starts_with("s3://") || input_path_str.starts_with("oss://");

        // Handle cloud input: download to temp file if needed
        let input_storage = if let Some(storage) = &self.input_storage {
            storage.clone()
        } else if is_cloud_url {
            // Create cloud storage for S3/OSS URLs
            self.create_cloud_storage(&input_path_str)?
        } else {
            // Default to LocalStorage for local files
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
        // to avoid duplication when joining with the storage root
        // For cloud storage (S3/OSS), extract just the object key from the URL
        let storage_path = if input_storage.as_any().is::<LocalStorage>() {
            input_path.file_name().unwrap_or(input_path.as_os_str())
        } else if is_cloud_url {
            // Extract just the key from s3://bucket/key or oss://bucket/key
            Self::extract_cloud_key(&input_path_str)
                .map(std::ffi::OsStr::new)
                .unwrap_or(input_path.as_os_str())
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
            "Processing input file"
        );

        // Create the dataset writer (already initialized via builder)
        let mut writer = self.create_writer()?;

        // Create alignment buffer
        let mut aligner = FrameAlignmentBuffer::new(self.config.clone());

        // Create backpressure handler
        let mut backpressure = BackpressureHandler::from_config(&self.config);

        // Build topic mappings
        let topic_mappings = self.build_topic_mappings()?;

        // Open input file
        // NOTE: RoboReader decodes BAG/MCAP files directly to TimestampedDecodedMessage.
        // There is NO intermediate MCAP conversion - neither in memory nor on disk.
        // BAG format is parsed natively, messages are decoded directly to HashMap<String, CodecValue>.
        let path_str = process_path
            .to_str()
            .ok_or_else(|| roboflow_core::RoboflowError::parse("Path", "Invalid UTF-8 path"))?;
        let reader = RoboReader::open(path_str)?;

        info!(
            mappings = topic_mappings.len(),
            "Starting message processing"
        );

        // Stream messages
        let mut stats = StreamingStats::default();
        let mut unmapped_warning_shown: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        for msg_result in reader.decoded()? {
            let msg_result = msg_result?;
            stats.messages_processed += 1;

            // Find mapping for this topic
            let mapping = match topic_mappings.get(&msg_result.channel.topic) {
                Some(m) => m,
                None => {
                    // Log warning once per unmapped topic to avoid spam
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

            // Convert to our TimestampedMessage type
            let msg = crate::streaming::alignment::TimestampedMessage {
                log_time: msg_result.log_time.unwrap_or(0),
                message: msg_result.message,
            };

            // Process message through alignment buffer
            let completed_frames = aligner.process_message(&msg, &mapping.feature);

            // Write completed frames immediately
            for frame in completed_frames {
                writer.write_frame(&frame)?;
                stats.frames_written += 1;

                // Call progress callback for checkpointing
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

                // Update memory estimate
                backpressure.update_memory_estimate(&aligner);
            }

            // Apply backpressure if needed
            if backpressure.should_apply_backpressure(&aligner) && !backpressure.is_in_cooldown() {
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

                    // Call progress callback for checkpointing
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

            // Progress reporting every 1000 messages
            if stats.messages_processed % 1000 == 0 {
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

        // Flush remaining frames
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

        // Finalize writer
        let writer_stats = writer.finalize()?;

        // Compile final statistics
        stats.duration_sec = start_time.elapsed().as_secs_f64();
        stats.writer_stats = writer_stats;
        stats.avg_buffer_size = aligner.stats().peak_buffer_size as f32;
        stats.peak_memory_mb = backpressure.memory_mb();

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
