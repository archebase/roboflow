// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! LeRobot v2.1 dataset writer.
//!
//! Writes robotics data in LeRobot v2.1 format with:
//! - Parquet files for frame data (one per episode)
//! - MP4 videos for camera observations (one per camera per episode)
//! - Complete metadata files

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::common::parquet_base::calculate_stats;
use crate::common::video::{Mp4Encoder, VideoEncoderConfig, VideoFrame, VideoFrameBuffer};
use crate::common::{AlignedFrame, DatasetWriter, ImageData, WriterStats};
use crate::kps::video_encoder::VideoEncoderError;
use crate::lerobot::config::LerobotConfig;
use crate::lerobot::metadata::MetadataCollector;
use crate::lerobot::trait_impl::{FromAlignedFrame, LerobotWriterTrait};
use crate::lerobot::video_profiles::ResolvedConfig;
use roboflow_core::Result;

/// LeRobot v2.1 dataset writer.
pub struct LerobotWriter {
    /// Storage backend for writing data (only available with cloud-storage feature)

    #[allow(dead_code)]
    storage: std::sync::Arc<dyn roboflow_storage::Storage>,

    /// Output prefix within storage (empty for local filesystem root)

    #[allow(dead_code)]
    output_prefix: String,

    /// Local buffer directory for temporary files (Parquet, video encoding)

    #[allow(dead_code)]
    local_buffer: PathBuf,

    /// Output directory (deprecated, kept for backward compatibility)
    output_dir: PathBuf,

    /// Configuration
    config: LerobotConfig,

    /// Current episode index
    episode_index: usize,

    /// Frame data for current episode
    frame_data: Vec<LerobotFrame>,

    /// Image buffers per camera
    image_buffers: HashMap<String, Vec<ImageData>>,

    /// Metadata collector
    metadata: MetadataCollector,

    /// Total frames written
    total_frames: usize,

    /// Total images encoded
    images_encoded: usize,

    /// Number of frames skipped due to dimension mismatches
    skipped_frames: usize,

    /// Whether the writer has been initialized
    initialized: bool,

    /// Start time for duration calculation
    start_time: Option<std::time::Instant>,

    /// Output bytes written
    output_bytes: u64,

    /// Number of videos that failed to encode
    failed_encodings: usize,

    /// Whether to use cloud storage (detected from storage type)

    #[allow(dead_code)]
    use_cloud_storage: bool,
}

/// Frame data for LeRobot Parquet file.
#[derive(Debug)]
pub struct LerobotFrame {
    /// Episode index
    pub episode_index: usize,

    /// Frame index within episode
    pub frame_index: usize,

    /// Global frame index
    pub index: usize,

    /// Timestamp in seconds
    pub timestamp: f64,

    /// Observation state (joint positions)
    pub observation_state: Option<Vec<f32>>,

    /// Action (target joint positions)
    pub action: Option<Vec<f32>>,

    /// Task index
    pub task_index: Option<usize>,

    /// Image frame references (camera -> (path, timestamp))
    pub image_frames: HashMap<String, (String, f64)>,
}

impl LerobotWriter {
    /// Create a new LeRobot writer.
    ///
    /// # Deprecated
    ///
    /// Use `new_local` instead for clarity. This method will be removed in a future version.
    #[allow(unused_variables)]
    #[deprecated(since = "0.2.0", note = "Use new_local() instead")]
    pub fn create(output_dir: impl AsRef<Path>, config: LerobotConfig) -> Result<Self> {
        Self::new_local(output_dir, config)
    }

    /// Create a new LeRobot writer for local filesystem output.
    ///
    /// This is the recommended constructor for local filesystem output.
    /// For cloud storage support, use the storage-aware constructors when
    /// the `cloud-storage` feature is enabled.
    ///
    /// # Arguments
    ///
    /// * `output_dir` - Output directory path
    /// * `config` - LeRobot configuration
    pub fn new_local(output_dir: impl AsRef<Path>, config: LerobotConfig) -> Result<Self> {
        let output_dir = output_dir.as_ref();

        // Create LeRobot v2.1 directory structure
        let data_dir = output_dir.join("data/chunk-000");
        let videos_dir = output_dir.join("videos/chunk-000");
        let meta_dir = output_dir.join("meta");

        fs::create_dir_all(&data_dir)?;
        fs::create_dir_all(&videos_dir)?;
        fs::create_dir_all(&meta_dir)?;

        // Create LocalStorage for backward compatibility
        let storage = std::sync::Arc::new(roboflow_storage::LocalStorage::new(output_dir));
        let local_buffer = output_dir.to_path_buf();
        let output_prefix = String::new();

        Ok(Self {
            storage,
            output_prefix,
            local_buffer,
            output_dir: output_dir.to_path_buf(),
            config,
            episode_index: 0,
            frame_data: Vec::new(),
            image_buffers: HashMap::new(),
            metadata: MetadataCollector::new(),
            total_frames: 0,
            images_encoded: 0,
            skipped_frames: 0,
            initialized: false,
            start_time: None,
            output_bytes: 0,
            failed_encodings: 0,
            use_cloud_storage: false,
        })
    }

    /// Create a new LeRobot writer with a storage backend.
    ///
    /// This constructor enables cloud storage support for writing datasets
    /// to remote storage backends (OSS, S3, etc.).
    ///
    /// # Arguments
    ///
    /// * `storage` - Storage backend for writing data
    /// * `output_prefix` - Output prefix within storage (e.g., "datasets/my_dataset")
    /// * `local_buffer` - Local buffer directory for temporary files (Parquet, video encoding)
    /// * `config` - LeRobot configuration
    ///
    /// # Example
    ///
    /// ```ignore
    /// use roboflow::storage::{Storage, StorageFactory, LocalStorage};
    /// use roboflow::dataset::lerobot::{LerobotWriter, LerobotConfig};
    /// use std::sync::Arc;
    ///
    /// // Create storage backend
    /// let factory = StorageFactory::new();
    /// let storage = factory.create("oss://my-bucket/datasets")?;
    ///
    /// // Create writer with cloud storage
    /// let writer = LerobotWriter::new(
    ///     storage,
    ///     "my_dataset".to_string(),
    ///     "/tmp/roboflow_buffer".into(),
    ///     LerobotConfig::default(),
    /// )?;
    /// ```
    pub fn new(
        storage: std::sync::Arc<dyn roboflow_storage::Storage>,
        output_prefix: String,
        local_buffer: impl AsRef<Path>,
        config: LerobotConfig,
    ) -> Result<Self> {
        let local_buffer = local_buffer.as_ref();

        // Create local buffer directory structure
        let data_dir = local_buffer.join("data/chunk-000");
        let videos_dir = local_buffer.join("videos/chunk-000");
        let meta_dir = local_buffer.join("meta");

        fs::create_dir_all(&data_dir)?;
        fs::create_dir_all(&videos_dir)?;
        fs::create_dir_all(&meta_dir)?;

        // Detect if this is cloud storage (not LocalStorage)
        use roboflow_storage::LocalStorage;
        let is_local = storage.as_any().is::<LocalStorage>();
        let use_cloud_storage = !is_local;

        // Create remote directories
        if !output_prefix.is_empty() {
            let data_prefix = format!("{}/data/chunk-000", output_prefix);
            let videos_prefix = format!("{}/videos/chunk-000", output_prefix);
            let meta_prefix = format!("{}/meta", output_prefix);

            storage
                .create_dir_all(Path::new(&data_prefix))
                .map_err(|e| {
                    roboflow_core::RoboflowError::encode(
                        "Storage",
                        format!(
                            "Failed to create remote data directory '{}': {}",
                            data_prefix, e
                        ),
                    )
                })?;
            storage
                .create_dir_all(Path::new(&videos_prefix))
                .map_err(|e| {
                    roboflow_core::RoboflowError::encode(
                        "Storage",
                        format!(
                            "Failed to create remote videos directory '{}': {}",
                            videos_prefix, e
                        ),
                    )
                })?;
            storage
                .create_dir_all(Path::new(&meta_prefix))
                .map_err(|e| {
                    roboflow_core::RoboflowError::encode(
                        "Storage",
                        format!(
                            "Failed to create remote meta directory '{}': {}",
                            meta_prefix, e
                        ),
                    )
                })?;
        }

        Ok(Self {
            storage,
            output_prefix,
            local_buffer: local_buffer.to_path_buf(),
            output_dir: local_buffer.to_path_buf(),
            config,
            episode_index: 0,
            frame_data: Vec::new(),
            image_buffers: HashMap::new(),
            metadata: MetadataCollector::new(),
            total_frames: 0,
            images_encoded: 0,
            skipped_frames: 0,
            initialized: false,
            start_time: None,
            output_bytes: 0,
            failed_encodings: 0,
            use_cloud_storage,
        })
    }

    /// Add a frame to the current episode.
    pub fn add_frame(&mut self, frame: LerobotFrame) {
        // Update metadata
        if let Some(ref state) = frame.observation_state {
            self.metadata
                .update_state_dim("observation.state".to_string(), state.len());
        }
        if let Some(ref action) = frame.action {
            self.metadata
                .update_state_dim("action".to_string(), action.len());
        }

        // Store image data for video encoding
        // Note: Image data is added separately via add_image()
        // The image_frames map is iterated during video encoding
        let _ = &frame.image_frames;

        self.frame_data.push(frame);
    }

    /// Add image data for a camera frame.
    pub fn add_image(&mut self, camera: String, data: ImageData) {
        // Update shape metadata
        self.metadata
            .update_image_shape(camera.clone(), data.width as usize, data.height as usize);

        // Buffer for video encoding
        self.image_buffers.entry(camera).or_default().push(data);
    }

    /// Start a new episode.
    pub fn start_episode(&mut self, _task_index: Option<usize>) {
        self.episode_index = self.frame_data.len();
        // Episode index continues from where we left off
    }

    /// Finish the current episode and write its data.
    pub fn finish_episode(&mut self, task_index: Option<usize>) -> Result<()> {
        if self.frame_data.is_empty() {
            return Ok(());
        }

        let tasks = task_index.map(|t| vec![t]).unwrap_or_default();

        let start = std::time::Instant::now();
        // Write Parquet file
        self.write_episode_parquet()?;
        let parquet_time = start.elapsed();

        let start = std::time::Instant::now();
        // Encode videos
        self.encode_videos()?;
        let video_time = start.elapsed();

        eprintln!(
            "[TIMING] finish_episode: parquet={:.1}ms, video={:.1}ms",
            parquet_time.as_secs_f64() * 1000.0,
            video_time.as_secs_f64() * 1000.0,
        );

        // Calculate and store episode stats
        self.calculate_episode_stats()?;

        // Update metadata
        self.metadata
            .add_episode(self.episode_index, self.frame_data.len(), tasks);

        // Update counters
        self.total_frames += self.frame_data.len();

        // Clear for next episode
        self.frame_data.clear();
        for buffer in self.image_buffers.values_mut() {
            buffer.clear();
        }

        self.episode_index += 1;

        Ok(())
    }

    /// Write current episode to Parquet file.
    fn write_episode_parquet(&mut self) -> Result<()> {
        use polars::prelude::*;
        use std::fs::File;
        use std::io::BufWriter;

        if self.frame_data.is_empty() {
            return Ok(());
        }

        let state_dim = self
            .frame_data
            .first()
            .and_then(|f| f.observation_state.as_ref())
            .map(|v| v.len())
            .ok_or_else(|| {
                roboflow_core::RoboflowError::encode(
                    "LerobotWriter",
                    "Cannot determine state dimension: first frame has no observation_state",
                )
            })?;

        let mut episode_index: Vec<i64> = Vec::new();
        let mut frame_index: Vec<i64> = Vec::new();
        let mut index: Vec<i64> = Vec::new();
        let mut timestamp: Vec<f64> = Vec::new();
        let mut observation_state: Vec<Vec<f32>> = Vec::new();
        let mut action: Vec<Vec<f32>> = Vec::new();
        let mut task_index: Vec<i64> = Vec::new();

        // Collect camera names from image_frames
        let mut cameras: Vec<String> = Vec::new();
        for frame in &self.frame_data {
            for camera in frame.image_frames.keys() {
                if !cameras.contains(camera) {
                    cameras.push(camera.clone());
                }
            }
        }

        // Image frame references per camera
        let mut image_paths: HashMap<String, Vec<String>> = HashMap::new();
        let mut image_timestamps: HashMap<String, Vec<f64>> = HashMap::new();
        for camera in &cameras {
            image_paths.insert(camera.clone(), Vec::new());
            image_timestamps.insert(camera.clone(), Vec::new());
        }

        for frame in &self.frame_data {
            episode_index.push(frame.episode_index as i64);
            frame_index.push(frame.frame_index as i64);
            index.push(frame.index as i64);
            timestamp.push(frame.timestamp);

            if let Some(ref state) = frame.observation_state {
                observation_state.push(state.clone());
            }
            if let Some(ref act) = frame.action {
                action.push(act.clone());
            }

            task_index.push(frame.task_index.map(|t| t as i64).unwrap_or(0));

            for camera in &cameras {
                if let Some((path, ts)) = frame.image_frames.get(camera) {
                    if let Some(paths) = image_paths.get_mut(camera) {
                        paths.push(path.clone());
                    }
                    if let Some(timestamps) = image_timestamps.get_mut(camera) {
                        timestamps.push(*ts);
                    }
                } else {
                    // Default path if image not available
                    let path = format!(
                        "videos/chunk-000/observation.images.{}/episode_{:06}.mp4",
                        camera, self.episode_index
                    );
                    if let Some(paths) = image_paths.get_mut(camera) {
                        paths.push(path);
                    }
                    if let Some(timestamps) = image_timestamps.get_mut(camera) {
                        timestamps.push(frame.timestamp);
                    }
                }
            }
        }

        // Build Parquet columns
        let mut series_vec = vec![
            Series::new("episode_index", episode_index),
            Series::new("frame_index", frame_index),
            Series::new("index", index),
            Series::new("timestamp", timestamp),
        ];

        // Add observation state columns
        for i in 0..state_dim {
            let col_name = format!("observation.state.{}", i);
            let values: Vec<f32> = observation_state
                .iter()
                .map(|v| v.get(i).copied().unwrap_or(0.0))
                .collect();
            series_vec.push(Series::new(&col_name, values));
        }

        // Add action columns
        for i in 0..state_dim {
            let col_name = format!("action.{}", i);
            let values: Vec<f32> = action
                .iter()
                .map(|v| v.get(i).copied().unwrap_or(0.0))
                .collect();
            series_vec.push(Series::new(&col_name, values));
        }

        // Add task_index
        series_vec.push(Series::new("task_index", task_index));

        // Add image frame references
        for camera in &cameras {
            let feature_name = format!("observation.images.{}", camera);
            if let Some(paths) = image_paths.get(camera) {
                series_vec.push(Series::new(
                    format!("{}_path", feature_name).as_str(),
                    paths.clone(),
                ));
            }
            if let Some(timestamps) = image_timestamps.get(camera) {
                series_vec.push(Series::new(
                    format!("{}_timestamp", feature_name).as_str(),
                    timestamps.clone(),
                ));
            }
        }

        // Create DataFrame and write
        let df = DataFrame::new(series_vec).map_err(|e| {
            roboflow_core::RoboflowError::parse("Parquet", format!("DataFrame error: {}", e))
        })?;

        let parquet_path = self.output_dir.join(format!(
            "data/chunk-000/episode_{:06}.parquet",
            self.episode_index
        ));

        let file = File::create(&parquet_path)?;
        let mut writer = BufWriter::new(file);

        ParquetWriter::new(&mut writer)
            .finish(&mut df.clone())
            .map_err(|e| {
                roboflow_core::RoboflowError::parse("Parquet", format!("Write error: {}", e))
            })?;

        // Track output bytes
        if let Ok(metadata) = std::fs::metadata(&parquet_path) {
            self.output_bytes += metadata.len();
        }

        tracing::info!(
            path = %parquet_path.display(),
            frames = self.frame_data.len(),
            "Wrote LeRobot v2.1 Parquet file"
        );

        // Upload to cloud storage if enabled

        {
            if self.use_cloud_storage {
                self.upload_parquet_file(&parquet_path)?;
            }
        }

        Ok(())
    }

    /// Upload a Parquet file to cloud storage.
    #[allow(dead_code)]
    fn upload_parquet_file(&self, local_path: &Path) -> Result<()> {
        use std::io::Read;

        let filename = local_path
            .file_name()
            .ok_or_else(|| roboflow_core::RoboflowError::parse("Path", "Invalid file name"))?;

        let remote_path = if self.output_prefix.is_empty() {
            Path::new("data/chunk-000").join(filename)
        } else {
            Path::new(&self.output_prefix)
                .join("data/chunk-000")
                .join(filename)
        };

        // Read local file
        let mut file = fs::File::open(local_path).map_err(|e| {
            roboflow_core::RoboflowError::encode(
                "Storage",
                format!("Failed to open parquet file: {}", e),
            )
        })?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).map_err(|e| {
            roboflow_core::RoboflowError::encode(
                "Storage",
                format!("Failed to read parquet file: {}", e),
            )
        })?;

        // Write to storage
        let mut writer = self.storage.writer(&remote_path).map_err(|e| {
            roboflow_core::RoboflowError::encode(
                "Storage",
                format!("Failed to create storage writer: {}", e),
            )
        })?;

        use std::io::Write;
        writer.write_all(&buffer).map_err(|e| {
            roboflow_core::RoboflowError::encode(
                "Storage",
                format!("Failed to write parquet to storage: {}", e),
            )
        })?;

        writer.flush().map_err(|e| {
            roboflow_core::RoboflowError::encode(
                "Storage",
                format!("Failed to flush parquet to storage: {}", e),
            )
        })?;

        tracing::info!(
            local = %local_path.display(),
            remote = %remote_path.display(),
            size = buffer.len(),
            "Uploaded Parquet file to cloud storage"
        );

        // Delete local file after successful upload
        if self.use_cloud_storage {
            if let Err(e) = fs::remove_file(local_path) {
                tracing::error!(
                    path = %local_path.display(),
                    error = %e,
                    "Failed to delete local Parquet file after upload - disk space may leak"
                );
            } else {
                tracing::debug!(path = %local_path.display(), "Deleted local Parquet file after upload");
            }
        }

        Ok(())
    }

    /// Upload a video file to cloud storage.
    #[allow(dead_code)]
    fn upload_video_file(&self, local_path: &Path, camera: &str) -> Result<()> {
        use std::io::Read;

        let filename = local_path
            .file_name()
            .ok_or_else(|| roboflow_core::RoboflowError::parse("Path", "Invalid file name"))?;

        let feature_name = format!("observation.images.{}", camera);
        let remote_path = if self.output_prefix.is_empty() {
            Path::new("videos/chunk-000")
                .join(&feature_name)
                .join(filename)
        } else {
            Path::new(&self.output_prefix)
                .join("videos/chunk-000")
                .join(&feature_name)
                .join(filename)
        };

        // Read local file
        let mut file = fs::File::open(local_path).map_err(|e| {
            roboflow_core::RoboflowError::encode(
                "Storage",
                format!("Failed to open video file: {}", e),
            )
        })?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).map_err(|e| {
            roboflow_core::RoboflowError::encode(
                "Storage",
                format!("Failed to read video file: {}", e),
            )
        })?;

        // Write to storage
        let mut writer = self.storage.writer(&remote_path).map_err(|e| {
            roboflow_core::RoboflowError::encode(
                "Storage",
                format!("Failed to create storage writer: {}", e),
            )
        })?;

        use std::io::Write;
        writer.write_all(&buffer).map_err(|e| {
            roboflow_core::RoboflowError::encode(
                "Storage",
                format!("Failed to write video to storage: {}", e),
            )
        })?;

        writer.flush().map_err(|e| {
            roboflow_core::RoboflowError::encode(
                "Storage",
                format!("Failed to flush video to storage: {}", e),
            )
        })?;

        tracing::info!(
            local = %local_path.display(),
            remote = %remote_path.display(),
            size = buffer.len(),
            camera = %camera,
            "Uploaded video file to cloud storage"
        );

        // Delete local file after successful upload
        if self.use_cloud_storage {
            if let Err(e) = fs::remove_file(local_path) {
                tracing::error!(
                    path = %local_path.display(),
                    file_size = buffer.len(),
                    error = %e,
                    "Failed to delete local video file ({:.2} MB) after upload - disk space may leak",
                    buffer.len() as f64 / (1024.0 * 1024.0)
                );
            } else {
                tracing::debug!(path = %local_path.display(), "Deleted local video file after upload");
            }
        }

        Ok(())
    }

    /// Upload multiple video files to cloud storage in parallel.
    #[allow(dead_code)]
    fn upload_videos_parallel(&self, video_files: Vec<(PathBuf, String)>) -> Result<()> {
        use rayon::prelude::*;

        let results: Vec<Result<()>> = video_files
            .par_iter()
            .map(|(path, camera)| self.upload_video_file(path, camera))
            .collect();

        // Check for any errors
        for result in results {
            result?;
        }

        Ok(())
    }

    /// Encode videos for all cameras.
    ///
    /// This function uses parallel encoding when multiple cameras are present.
    /// The degree of parallelism is controlled by the video configuration.
    fn encode_videos(&mut self) -> Result<()> {
        if self.image_buffers.is_empty() {
            return Ok(());
        }

        let total_start = std::time::Instant::now();

        // Resolve the video configuration (profiles, hardware acceleration, etc.)
        let resolved = ResolvedConfig::from_video_config(&self.config.video);
        let encoder_config = resolved.to_encoder_config(self.config.dataset.fps);

        tracing::info!(
            codec = %resolved.codec,
            crf = resolved.crf,
            preset = %resolved.preset,
            hardware_accelerated = resolved.hardware_accelerated,
            parallel_jobs = resolved.parallel_jobs,
            "Video encoding configuration"
        );

        let videos_dir = self.output_dir.join("videos/chunk-000");

        // Collect camera data for encoding
        let camera_data: Vec<(String, Vec<ImageData>)> = self
            .image_buffers
            .iter()
            .filter(|(_, images)| !images.is_empty())
            .map(|(camera, images)| (camera.clone(), images.clone()))
            .collect();

        if camera_data.is_empty() {
            return Ok(());
        }

        // Use parallel encoding only when:
        // 1. Hardware acceleration is enabled (reduces CPU contention)
        // 2. We have multiple cameras
        // 3. parallel_jobs > 1
        let use_parallel =
            resolved.hardware_accelerated && resolved.parallel_jobs > 1 && camera_data.len() > 1;

        if use_parallel {
            // Limit concurrent encodings to avoid resource contention
            let concurrent_jobs = resolved.parallel_jobs.min(camera_data.len());
            self.encode_videos_parallel(
                camera_data,
                &videos_dir,
                &encoder_config,
                concurrent_jobs,
            )?;
        } else {
            self.encode_videos_sequential(camera_data, &videos_dir, &encoder_config)?;
        }

        eprintln!(
            "[TIMING] encode_videos: total={:.1}ms",
            total_start.elapsed().as_secs_f64() * 1000.0,
        );

        Ok(())
    }

    /// Encode videos sequentially (original behavior).
    fn encode_videos_sequential(
        &mut self,
        camera_data: Vec<(String, Vec<ImageData>)>,
        videos_dir: &Path,
        encoder_config: &VideoEncoderConfig,
    ) -> Result<()> {
        let encoder = Mp4Encoder::with_config(encoder_config.clone());
        let mut buffer_time = std::time::Duration::ZERO;
        let mut encode_time = std::time::Duration::ZERO;

        // Track video files for cloud upload

        let mut video_files: Vec<(PathBuf, String)> = Vec::new();

        for (camera, images) in camera_data {
            let b_start = std::time::Instant::now();
            let buffer = self.build_frame_buffer(&images)?;
            buffer_time += b_start.elapsed();

            if !buffer.is_empty() {
                let feature_name = format!("observation.images.{}", camera);
                let camera_dir = videos_dir.join(&feature_name);
                fs::create_dir_all(&camera_dir)?;

                let video_path = camera_dir.join(format!("episode_{:06}.mp4", self.episode_index));

                let e_start = std::time::Instant::now();
                match encoder.encode_buffer(&buffer, &video_path) {
                    Ok(()) => {
                        self.images_encoded += buffer.len();
                        tracing::debug!(
                            camera = %camera,
                            frames = buffer.len(),
                            path = %video_path.display(),
                            "Encoded MP4 video"
                        );
                    }
                    Err(VideoEncoderError::FfmpegNotFound) => {
                        tracing::error!(
                            "ffmpeg not found. Please install ffmpeg to encode videos. \
                             Camera '{}' videos will not be available in the dataset.",
                            camera
                        );
                        self.failed_encodings += 1;
                        return Err(roboflow_core::RoboflowError::unsupported(
                            "Video encoding requires ffmpeg. Install ffmpeg and ensure it's in your PATH.",
                        ));
                    }
                    Err(e) => {
                        tracing::error!(
                            camera = %camera,
                            error = %e,
                            "Failed to encode video"
                        );
                        self.failed_encodings += 1;
                        return Err(roboflow_core::RoboflowError::encode(
                            "VideoEncoder",
                            format!("Failed to encode video for camera '{}': {}", camera, e),
                        ));
                    }
                }
                encode_time += e_start.elapsed();

                if let Ok(metadata) = std::fs::metadata(&video_path) {
                    self.output_bytes += metadata.len();
                }

                // Track for upload

                {
                    if self.use_cloud_storage {
                        video_files.push((video_path.clone(), camera.clone()));
                    }
                }
            }
        }

        // Upload videos to cloud storage

        {
            if self.use_cloud_storage && !video_files.is_empty() {
                self.upload_videos_parallel(video_files)?;
            }
        }

        eprintln!(
            "[TIMING] encode_videos (sequential): buffer={:.1}ms, encode={:.1}ms",
            buffer_time.as_secs_f64() * 1000.0,
            encode_time.as_secs_f64() * 1000.0,
        );

        Ok(())
    }

    /// Encode videos in parallel using rayon.
    fn encode_videos_parallel(
        &mut self,
        camera_data: Vec<(String, Vec<ImageData>)>,
        videos_dir: &Path,
        encoder_config: &VideoEncoderConfig,
        parallel_jobs: usize,
    ) -> Result<()> {
        use rayon::prelude::*;

        // Configure rayon thread pool
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(parallel_jobs)
            .build()
            .map_err(|e| roboflow_core::RoboflowError::encode("ThreadPool", e.to_string()))?;

        // Create all camera directories before parallel encoding to avoid race
        for (camera, _) in &camera_data {
            let feature_name = format!("observation.images.{}", camera);
            let camera_dir = videos_dir.join(&feature_name);
            fs::create_dir_all(&camera_dir).map_err(|e| {
                roboflow_core::RoboflowError::encode(
                    "VideoEncoder",
                    format!("Failed to create camera directory '{}': {}", camera, e),
                )
            })?;
        }

        // Shared counters for statistics (using atomic types to avoid mutex poisoning)
        let images_encoded = Arc::new(AtomicUsize::new(0usize));
        let output_bytes = Arc::new(AtomicU64::new(0u64));
        let skipped_frames = Arc::new(AtomicUsize::new(0usize));
        let failed_encodings = Arc::new(AtomicUsize::new(0usize));

        // Track video files for cloud upload

        let video_files = Arc::new(std::sync::Mutex::new(Vec::new()));

        let result: Result<Vec<()>> = pool.install(|| {
            camera_data.par_iter().map(|(camera, images)| {
                let b_start = std::time::Instant::now();
                let (buffer, skipped) = Self::build_frame_buffer_static(images)
                    .map_err(|e| {
                        // Include camera name in error for debugging
                        roboflow_core::RoboflowError::encode(
                            "VideoEncoder",
                            format!("Failed to build frame buffer for camera '{}': {}", camera, e),
                        )
                    })?;
                let _buffer_time = b_start.elapsed();

                // Track skipped frames
                if skipped > 0 {
                    skipped_frames.fetch_add(skipped, Ordering::Relaxed);
                }

                if !buffer.is_empty() {
                    let feature_name = format!("observation.images.{}", camera);
                    let camera_dir = videos_dir.join(&feature_name);
                    let video_path = camera_dir.join(format!("episode_{:06}.mp4", self.episode_index));

                    let encoder = Mp4Encoder::with_config(encoder_config.clone());
                    let e_start = std::time::Instant::now();

                    match encoder.encode_buffer(&buffer, &video_path) {
                        Ok(()) => {
                            images_encoded.fetch_add(buffer.len(), Ordering::Relaxed);
                            tracing::debug!(
                                camera = %camera,
                                frames = buffer.len(),
                                path = %video_path.display(),
                                "Encoded MP4 video"
                            );

                            // Track for upload
                            {
                                if self.use_cloud_storage {
                                    let mut files = video_files.lock().map_err(|e| {
                                        roboflow_core::RoboflowError::encode(
                                            "VideoEncoder",
                                            format!("Video files mutex poisoned: {}", e),
                                        )
                                    })?;
                                    files.push((video_path.clone(), camera.clone()));
                                }
                            }
                        }
                        Err(VideoEncoderError::FfmpegNotFound) => {
                            tracing::error!(
                                "ffmpeg not found. Please install ffmpeg to encode videos."
                            );
                            failed_encodings.fetch_add(1, Ordering::Relaxed);
                            return Err(roboflow_core::RoboflowError::unsupported(
                                "Video encoding requires ffmpeg. Install ffmpeg and ensure it's in your PATH."
                            ));
                        }
                        Err(e) => {
                            tracing::error!(
                                camera = %camera,
                                error = %e,
                                "Failed to encode video"
                            );
                            failed_encodings.fetch_add(1, Ordering::Relaxed);
                            return Err(roboflow_core::RoboflowError::encode(
                                "VideoEncoder",
                                format!("Failed to encode video for camera '{}': {}", camera, e)
                            ));
                        }
                    }
                    let _encode_time = e_start.elapsed();

                    if let Ok(metadata) = std::fs::metadata(&video_path) {
                        output_bytes.fetch_add(metadata.len(), Ordering::Relaxed);
                    }
                }

                Ok(())
            }).collect()
        });

        result?;

        // Update counters using atomic loads
        self.images_encoded += images_encoded.load(Ordering::Relaxed);
        self.output_bytes += output_bytes.load(Ordering::Relaxed);
        self.skipped_frames += skipped_frames.load(Ordering::Relaxed);
        self.failed_encodings += failed_encodings.load(Ordering::Relaxed);

        // Upload videos to cloud storage

        {
            if self.use_cloud_storage {
                let files = video_files.lock().map_err(|e| {
                    roboflow_core::RoboflowError::encode(
                        "VideoEncoder",
                        format!("Video files mutex poisoned during upload: {}", e),
                    )
                })?;
                if !files.is_empty() {
                    self.upload_videos_parallel(files.clone())?;
                }
            }
        }

        eprintln!(
            "[TIMING] encode_videos (parallel, {} jobs): {} cameras encoded",
            parallel_jobs,
            camera_data.len()
        );

        Ok(())
    }

    /// Build a frame buffer from image data.
    fn build_frame_buffer(&self, images: &[ImageData]) -> roboflow_core::Result<VideoFrameBuffer> {
        let (buffer, _) = Self::build_frame_buffer_static(images)?;
        Ok(buffer)
    }

    /// Static version of build_frame_buffer for use in parallel context.
    ///
    /// Returns (buffer, skipped_frame_count) where skipped frames are those
    /// that had dimension mismatches.
    fn build_frame_buffer_static(
        images: &[ImageData],
    ) -> roboflow_core::Result<(VideoFrameBuffer, usize)> {
        let mut buffer = VideoFrameBuffer::new();
        let mut skipped = 0usize;

        for img in images {
            if img.width > 0 && img.height > 0 {
                let rgb_data = img.data.clone();

                let video_frame = VideoFrame::new(img.width, img.height, rgb_data);
                if let Err(e) = buffer.add_frame(video_frame) {
                    // Track dimension mismatches
                    skipped += 1;
                    tracing::warn!(
                        expected_width = buffer.width.unwrap_or(0),
                        expected_height = buffer.height.unwrap_or(0),
                        actual_width = img.width,
                        actual_height = img.height,
                        error = %e,
                        "Frame dimension mismatch - skipping frame"
                    );
                }
            }
        }

        // Fail if all frames were skipped - indicates serious data corruption
        if !images.is_empty() && buffer.is_empty() {
            return Err(roboflow_core::RoboflowError::encode(
                "VideoEncoder",
                format!(
                    "All {} frames skipped due to dimension mismatches - dataset may be corrupted",
                    images.len()
                ),
            ));
        }

        Ok((buffer, skipped))
    }

    /// Calculate episode statistics.
    fn calculate_episode_stats(&mut self) -> Result<()> {
        if self.frame_data.is_empty() {
            return Ok(());
        }

        let mut stats = HashMap::new();

        // Calculate observation.state stats
        let state_values: Vec<Vec<f32>> = self
            .frame_data
            .iter()
            .filter_map(|f| f.observation_state.as_ref())
            .cloned()
            .collect();

        if let Some(feature_stats) = calculate_stats(&state_values) {
            stats.insert("observation.state".to_string(), feature_stats);
        }

        // Calculate action stats
        let action_values: Vec<Vec<f32>> = self
            .frame_data
            .iter()
            .filter_map(|f| f.action.as_ref())
            .cloned()
            .collect();

        if let Some(feature_stats) = calculate_stats(&action_values) {
            stats.insert("action".to_string(), feature_stats);
        }

        self.metadata.add_episode_stats(self.episode_index, stats);

        Ok(())
    }

    /// Finalize the dataset and write metadata files.
    pub fn finalize(mut self) -> Result<usize> {
        // Finish any remaining episode
        if !self.frame_data.is_empty() {
            self.finish_episode(None)?;
        }

        // Write metadata files

        {
            if self.use_cloud_storage {
                self.metadata.write_all_to_storage(
                    &self.storage,
                    &self.output_prefix,
                    &self.config,
                )?;
            } else {
                self.metadata.write_all(&self.output_dir, &self.config)?;
            }
        }

        {
            self.metadata.write_all(&self.output_dir, &self.config)?;
        }

        let duration = self
            .start_time
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0);

        tracing::info!(
            output_dir = %self.output_dir.display(),
            episodes = self.episode_index,
            frames = self.total_frames,
            images_encoded = self.images_encoded,
            skipped_frames = self.skipped_frames,
            output_bytes = self.output_bytes,
            duration_sec = duration,
            "Finalized LeRobot v2.1 dataset"
        );

        // Warn if there were any skipped frames or failed encodings
        if self.skipped_frames > 0 {
            tracing::warn!(
                "{} frames were skipped due to dimension mismatches",
                self.skipped_frames
            );
        }

        Ok(self.total_frames)
    }

    /// Get total frames written so far.
    pub fn frame_count(&self) -> usize {
        self.total_frames + self.frame_data.len()
    }

    /// Register a task and return its index.
    pub fn register_task(&mut self, task: String) -> usize {
        self.metadata.register_task(task)
    }

    /// Get reference to metadata collector.
    pub fn metadata(&self) -> &MetadataCollector {
        &self.metadata
    }

    /// Check if the writer has been initialized.
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Get the number of skipped frames.
    pub fn skipped_frames(&self) -> usize {
        self.skipped_frames
    }

    /// Get the number of failed video encodings.
    pub fn failed_encodings(&self) -> usize {
        self.failed_encodings
    }
}

/// Implement the core DatasetWriter trait for LerobotWriter.
impl DatasetWriter for LerobotWriter {
    fn initialize(&mut self, config: &dyn std::any::Any) -> Result<()> {
        if let Some(lerobot_config) = config.downcast_ref::<LerobotConfig>() {
            self.config = lerobot_config.clone();
        }
        self.initialized = true;
        self.start_time = Some(std::time::Instant::now());
        Ok(())
    }

    fn write_frame(&mut self, frame: &AlignedFrame) -> Result<()> {
        if !self.initialized {
            return Err(roboflow_core::RoboflowError::encode(
                "LerobotWriter",
                "Writer not initialized. Call initialize() before write_frame().",
            ));
        }

        // Convert AlignedFrame to LerobotFrame
        let lerobot_frame = LerobotFrame::from_aligned_frame(frame, self.episode_index);

        // Add the frame
        self.add_frame(lerobot_frame);

        // Add images
        for (camera, data) in &frame.images {
            self.add_image(camera.clone(), data.clone());
        }

        Ok(())
    }

    fn finalize(&mut self, config: &dyn std::any::Any) -> Result<WriterStats> {
        // Update config if provided
        if let Some(lerobot_config) = config.downcast_ref::<LerobotConfig>() {
            self.config = lerobot_config.clone();
        }

        // Finish any remaining episode
        if !self.frame_data.is_empty() {
            self.finish_episode(None)?;
        }

        // Write metadata files

        {
            if self.use_cloud_storage {
                self.metadata.write_all_to_storage(
                    &self.storage,
                    &self.output_prefix,
                    &self.config,
                )?;
            } else {
                self.metadata.write_all(&self.output_dir, &self.config)?;
            }
        }

        {
            self.metadata.write_all(&self.output_dir, &self.config)?;
        }

        let duration = self
            .start_time
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0);

        tracing::info!(
            output_dir = %self.output_dir.display(),
            episodes = self.episode_index,
            frames = self.total_frames,
            images_encoded = self.images_encoded,
            skipped_frames = self.skipped_frames,
            output_bytes = self.output_bytes,
            duration_sec = duration,
            "Finalized LeRobot v2.1 dataset"
        );

        // Warn if there were any skipped frames or failed encodings
        if self.skipped_frames > 0 {
            tracing::warn!(
                "{} frames were skipped due to dimension mismatches",
                self.skipped_frames
            );
        }

        Ok(WriterStats {
            frames_written: self.total_frames,
            images_encoded: self.images_encoded,
            state_records: self.total_frames * 2, // state + action
            output_bytes: self.output_bytes,
            duration_sec: duration,
        })
    }

    fn frame_count(&self) -> usize {
        self.total_frames + self.frame_data.len()
    }

    fn is_initialized(&self) -> bool {
        self.initialized
    }
}

/// Implement the LeRobot-specific trait for LerobotWriter.
impl LerobotWriterTrait for LerobotWriter {
    fn start_episode(&mut self, _task_index: Option<usize>) {
        self.episode_index = self.frame_data.len();
    }

    fn finish_episode(&mut self, task_index: Option<usize>) -> Result<()> {
        if self.frame_data.is_empty() {
            return Ok(());
        }

        let tasks = task_index.map(|t| vec![t]).unwrap_or_default();

        // Write Parquet file
        self.write_episode_parquet()?;

        // Encode videos
        self.encode_videos()?;

        // Calculate and store episode stats
        self.calculate_episode_stats()?;

        // Update metadata
        self.metadata
            .add_episode(self.episode_index, self.frame_data.len(), tasks);

        // Update counters
        self.total_frames += self.frame_data.len();

        // Clear for next episode
        self.frame_data.clear();
        for buffer in self.image_buffers.values_mut() {
            buffer.clear();
        }

        self.episode_index += 1;

        Ok(())
    }

    fn register_task(&mut self, task: String) -> usize {
        self.metadata.register_task(task)
    }

    fn add_frame(&mut self, frame: &AlignedFrame) -> Result<()> {
        <LerobotWriter as DatasetWriter>::write_frame(self, frame)
    }

    fn add_image(&mut self, camera: String, data: ImageData) {
        // Update shape metadata
        self.metadata
            .update_image_shape(camera.clone(), data.width as usize, data.height as usize);

        // Buffer for video encoding
        self.image_buffers.entry(camera).or_default().push(data);
    }

    fn metadata(&self) -> &MetadataCollector {
        &self.metadata
    }

    fn frame_count(&self) -> usize {
        self.total_frames + self.frame_data.len()
    }
}

/// Implement conversion from AlignedFrame to LerobotFrame.
impl FromAlignedFrame for LerobotFrame {
    fn from_aligned_frame(frame: &AlignedFrame, episode_index: usize) -> Self {
        // Extract observation state from the aligned frame
        let observation_state = frame
            .states
            .iter()
            .find(|(k, _)| k.contains("observation") || k.contains("state"))
            .map(|(_, v)| v.clone());

        // Extract action from the aligned frame
        let action = frame.actions.values().next().cloned();

        // Build image frame references
        let mut image_frames = HashMap::new();
        for camera in frame.images.keys() {
            let path = format!(
                "videos/chunk-000/observation.images.{}/episode_{:06}.mp4",
                camera, episode_index
            );
            image_frames.insert(camera.clone(), (path, frame.timestamp_sec()));
        }

        Self {
            episode_index,
            frame_index: frame.frame_index,
            index: frame.frame_index,
            timestamp: frame.timestamp_sec(),
            observation_state,
            action,
            task_index: None,
            image_frames,
        }
    }
}
