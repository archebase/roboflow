// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! LeRobot v2.1 dataset writer.
//!
//! Writes robotics data in LeRobot v2.1 format with:
//! - Parquet files for frame data (one per episode)
//! - MP4 videos for camera observations (one per camera per episode)
//! - Complete metadata files

mod encoding;
mod frame;
mod parquet;
mod stats;
mod upload;

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::common::{AlignedFrame, DatasetWriter, ImageData, WriterStats};
use crate::lerobot::config::LerobotConfig;
use crate::lerobot::metadata::MetadataCollector;
use crate::lerobot::trait_impl::{FromAlignedFrame, LerobotWriterTrait};
use crate::lerobot::video_profiles::ResolvedConfig;
use roboflow_core::Result;

pub use frame::LerobotFrame;

use encoding::{encode_videos, EncodeStats};

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

    /// Upload coordinator for cloud uploads (optional).
    upload_coordinator: Option<std::sync::Arc<crate::lerobot::upload::EpisodeUploadCoordinator>>,
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
            upload_coordinator: None,
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
                        format!("Failed to create remote data directory '{}': {}", data_prefix, e),
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
                        format!("Failed to create remote meta directory '{}': {}", meta_prefix, e),
                    )
                })?;
        }

        // Create upload coordinator for cloud storage
        let upload_coordinator = if use_cloud_storage {
            let upload_config = crate::lerobot::upload::UploadConfig {
                show_progress: false,
                ..Default::default()
            };

            match crate::lerobot::upload::EpisodeUploadCoordinator::new(
                std::sync::Arc::clone(&storage),
                upload_config,
                None,
            ) {
                Ok(coordinator) => Some(std::sync::Arc::new(coordinator)),
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "Failed to create upload coordinator, uploads will be done synchronously"
                    );
                    None
                }
            }
        } else {
            None
        };

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
            upload_coordinator,
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
    }

    /// Finish the current episode and write its data.
    pub fn finish_episode(&mut self, task_index: Option<usize>) -> Result<()> {
        if self.frame_data.is_empty() {
            return Ok(());
        }

        let tasks = task_index.map(|t| vec![t]).unwrap_or_default();

        let start = std::time::Instant::now();
        // Write Parquet file
        let (_parquet_path, _) = self.write_episode_parquet()?;
        let parquet_time = start.elapsed();

        let start = std::time::Instant::now();
        // Encode videos
        let (_video_files, encode_stats) = self.encode_videos()?;
        let video_time = start.elapsed();

        // Update statistics
        self.images_encoded += encode_stats.images_encoded;
        self.skipped_frames += encode_stats.skipped_frames;
        self.failed_encodings += encode_stats.failed_encodings;
        self.output_bytes += encode_stats.output_bytes;

        eprintln!(
            "[TIMING] finish_episode: parquet={:.1}ms, video={:.1}ms",
            parquet_time.as_secs_f64() * 1000.0,
            video_time.as_secs_f64() * 1000.0,
        );

        // Queue upload via coordinator if available (non-blocking)
        if self.upload_coordinator.is_some() {
            // Reconstruct parquet path
            let parquet_path = self.output_dir.join(format!(
                "data/chunk-000/episode_{:06}.parquet",
                self.episode_index
            ));

            // Collect video paths from image_buffers
            let video_paths: Vec<(String, PathBuf)> = self
                .image_buffers
                .keys()
                .filter(|camera| {
                    self.image_buffers
                        .get(&**camera)
                        .is_some_and(|v| !v.is_empty())
                })
                .map(|camera| {
                    let video_path = self.output_dir.join(format!(
                        "videos/chunk-000/{}/episode_{:06}.mp4",
                        camera, self.episode_index
                    ));
                    (camera.clone(), video_path)
                })
                .collect();

            if let Err(e) = self.queue_episode_upload(&parquet_path, &video_paths) {
                tracing::warn!(
                    episode = self.episode_index,
                    error = %e,
                    "Failed to queue episode upload, files will remain local"
                );
            }
        }

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
    fn write_episode_parquet(&mut self) -> Result<(PathBuf, usize)> {
        let (parquet_path, size) = parquet::write_episode_parquet(
            &self.frame_data,
            self.episode_index,
            &self.output_dir,
        )?;

        self.output_bytes += size as u64;

        // Upload to cloud storage if enabled (without upload coordinator)
        if self.use_cloud_storage && self.upload_coordinator.is_none() && !parquet_path.as_os_str().is_empty() {
            upload::upload_parquet_file(self.storage.as_ref(), &parquet_path, &self.output_prefix)?;
        }

        Ok((parquet_path, size))
    }

    /// Encode videos for all cameras.
    fn encode_videos(&mut self) -> Result<(Vec<(PathBuf, String)>, EncodeStats)> {
        if self.image_buffers.is_empty() {
            return Ok((Vec::new(), EncodeStats::default()));
        }

        let videos_dir = self.output_dir.join("videos/chunk-000");

        // Collect camera data for encoding
        let camera_data: Vec<(String, Vec<ImageData>)> = self
            .image_buffers
            .iter()
            .map(|(camera, images)| (camera.clone(), images.clone()))
            .collect();

        // Resolve the video configuration
        let resolved = ResolvedConfig::from_video_config(&self.config.video);

        let (mut video_files, encode_stats) = encode_videos(
            &camera_data,
            self.episode_index,
            &videos_dir,
            &resolved,
            self.config.dataset.fps,
            self.use_cloud_storage,
        )?;

        // Upload videos to cloud storage (without upload coordinator)
        if self.use_cloud_storage && self.upload_coordinator.is_none() && !video_files.is_empty() {
            upload::upload_videos_parallel(self.storage.as_ref(), video_files.clone())?;
            // Clear video files after upload to avoid double-upload
            video_files.clear();
        }

        Ok((video_files, encode_stats))
    }

    /// Queue episode upload via the upload coordinator (non-blocking).
    fn queue_episode_upload(
        &self,
        parquet_path: &Path,
        video_paths: &[(String, PathBuf)],
    ) -> Result<bool> {
        if let Some(coordinator) = &self.upload_coordinator {
            let episode_files = crate::lerobot::upload::EpisodeFiles {
                parquet_path: parquet_path.to_path_buf(),
                video_paths: video_paths.to_vec(),
                remote_prefix: self.output_prefix.clone(),
                episode_index: self.episode_index as u64,
            };

            coordinator.queue_episode_upload(episode_files)?;
            tracing::debug!(
                episode = self.episode_index,
                "Queued episode upload via coordinator"
            );
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Calculate episode statistics.
    fn calculate_episode_stats(&mut self) -> Result<()> {
        stats::calculate_episode_stats(&self.frame_data, self.episode_index, &mut self.metadata)
    }

    /// Finalize the dataset and write metadata files.
    pub fn finalize(mut self) -> Result<usize> {
        // Finish any remaining episode
        if !self.frame_data.is_empty() {
            self.finish_episode(None)?;
        }

        // Write metadata files
        if self.use_cloud_storage {
            self.metadata.write_all_to_storage(
                &self.storage,
                &self.output_prefix,
                &self.config,
            )?;
        }
        self.metadata.write_all(&self.output_dir, &self.config)?;

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
        if self.use_cloud_storage {
            self.metadata.write_all_to_storage(
                &self.storage,
                &self.output_prefix,
                &self.config,
            )?;
        }
        self.metadata.write_all(&self.output_dir, &self.config)?;

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

        // Flush pending uploads to cloud storage before completing
        if let Some(coordinator) = &self.upload_coordinator {
            tracing::info!("Waiting for pending cloud uploads to complete before finalize...");
            match coordinator.flush() {
                Ok(()) => {
                    tracing::info!("All cloud uploads completed successfully");
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "Some cloud uploads may not have completed before finalize. \
                         Background uploads will continue after finalize returns."
                    );
                }
            }
        }

        Ok(WriterStats {
            frames_written: self.total_frames,
            images_encoded: self.images_encoded,
            state_records: self.total_frames * 2,
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

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn episode_index(&self) -> Option<usize> {
        Some(self.episode_index)
    }

    fn get_upload_state(&self) -> Option<crate::common::base::UploadState> {
        self.upload_coordinator
            .as_ref()
            .map(|coordinator| coordinator.completed_uploads())
    }
}

/// Implement the LeRobot-specific trait for LerobotWriter.
impl LerobotWriterTrait for LerobotWriter {
    fn start_episode(&mut self, _task_index: Option<usize>) {
        self.episode_index = self.frame_data.len();
    }

    fn finish_episode(&mut self, task_index: Option<usize>) -> Result<()> {
        // Call the inherent method using fully qualified syntax to avoid recursion
        LerobotWriter::finish_episode(self, task_index)
    }

    fn register_task(&mut self, task: String) -> usize {
        self.metadata.register_task(task)
    }

    fn add_frame(&mut self, frame: &AlignedFrame) -> Result<()> {
        <LerobotWriter as DatasetWriter>::write_frame(self, frame)
    }

    fn add_image(&mut self, camera: String, data: ImageData) {
        self.add_image(camera, data);
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
                "videos/chunk-000/{}/episode_{:06}.mp4",
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
