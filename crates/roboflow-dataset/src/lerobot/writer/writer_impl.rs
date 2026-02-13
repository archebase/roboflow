// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! LerobotWriter implementation.
//!
//! This module contains the main implementation of [`LerobotWriter`] and
//! [`LerobotWriterBuilder`]. It is not part of the public API directly;
//! types are re-exported from the parent module.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::common::{
    AlignedFrame, ConcurrentEncoderConfig, ConcurrentVideoEncoder, DatasetWriter, ImageData,
    WriterStats,
};
use crate::lerobot::config::LerobotConfig;
use crate::lerobot::metadata::MetadataCollector;
use crate::lerobot::trait_impl::{FromAlignedFrame, LerobotWriterTrait};
use crate::lerobot::video_profiles::ResolvedConfig;
use roboflow_core::Result;

use super::camera::{CameraExtrinsic, CameraIntrinsic};
use super::camera_params::CameraParamsWriter;
use super::cloud_upload::CloudUploader;
use super::encoding::{EncodeStats, encode_videos};
use super::frame::LerobotFrame;
use super::stats;

/// LeRobot v2.1 dataset writer.
pub struct LerobotWriter {
    /// Storage backend for writing data
    storage: Arc<dyn roboflow_storage::Storage>,

    /// Output prefix within storage (empty for local filesystem root)
    output_prefix: String,

    /// Local buffer directory for temporary files (Parquet, video encoding)
    _local_buffer: PathBuf,

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

    /// Camera intrinsic parameters (camera_name -> intrinsic params)
    camera_intrinsics: HashMap<String, CameraIntrinsic>,

    /// Camera extrinsic parameters (camera_name -> extrinsic params)
    camera_extrinsics: HashMap<String, CameraExtrinsic>,

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
    use_cloud_storage: bool,

    /// Upload coordinator for cloud uploads (optional).
    upload_coordinator: Option<Arc<crate::lerobot::upload::EpisodeUploadCoordinator>>,

    /// Helper for cloud uploads
    cloud_uploader: CloudUploader,
}

impl LerobotWriter {
    /// Create a new LeRobot writer.
    ///
    /// # Deprecated
    ///
    /// Use `new_local` instead for clarity. This method will be removed in a future version.
    #[deprecated(since = "0.2.0", note = "Use new_local() instead")]
    pub fn create(output_dir: impl AsRef<Path>, config: LerobotConfig) -> Result<Self> {
        Self::new_local(output_dir, config)
    }

    /// Create a new LeRobot writer for local filesystem output.
    ///
    /// This is the recommended constructor for local filesystem output.
    /// For cloud storage support, use the `new_with_storage` constructor.
    ///
    /// # Arguments
    ///
    /// * `output_dir` - Output directory path
    /// * `config` - LeRobot configuration
    pub fn new_local(output_dir: impl AsRef<Path>, config: LerobotConfig) -> Result<Self> {
        let output_dir = output_dir.as_ref();

        // Validate that output_dir is not a cloud storage URL
        let output_dir_str = output_dir.as_os_str().to_string_lossy();
        if output_dir_str.starts_with("s3://")
            || output_dir_str.starts_with("oss://")
            || output_dir_str.starts_with("S3://")
            || output_dir_str.starts_with("OSS://")
        {
            return Err(roboflow_core::RoboflowError::parse(
                "LerobotWriter",
                format!(
                    "output_dir appears to be a cloud storage URL ('{}'). For cloud storage, use the storage-aware constructor:\n\n\
                               LerobotWriter::new(\n\n\
                                   storage,\n\n\
                                   prefix.to_string(),\n\n\
                                   local_buffer,\n\n\
                                   config,\n\n\
                               )\n\n\
                               Or use the builder:\n\n\
                               LerobotWriter::builder()\n\n\
                                   .storage(storage)\n\n\
                                   .output_prefix(\"datasets\")\n\n\
                                   .local_buffer(\"/tmp/roboflow_buffer\")\n\n\
                                   .config(config)",
                    output_dir_str
                ),
            ));
        }

        // Create LeRobot v2.1 directory structure
        let data_dir = output_dir.join("data/chunk-000");
        let videos_dir = output_dir.join("videos/chunk-000");
        let meta_dir = output_dir.join("meta");
        let params_dir = output_dir.join("parameters");

        fs::create_dir_all(&data_dir)?;
        fs::create_dir_all(&videos_dir)?;
        fs::create_dir_all(&meta_dir)?;
        fs::create_dir_all(&params_dir)?;

        // Create LocalStorage for backward compatibility
        let storage: Arc<dyn roboflow_storage::Storage> =
            Arc::new(roboflow_storage::LocalStorage::new(output_dir));
        let local_buffer = output_dir.to_path_buf();
        let output_prefix = String::new();
        let cloud_uploader = CloudUploader::new(Arc::clone(&storage), output_prefix.clone());

        Ok(Self {
            storage,
            output_prefix,
            _local_buffer: local_buffer,
            output_dir: output_dir.to_path_buf(),
            config,
            episode_index: 0,
            frame_data: Vec::new(),
            image_buffers: HashMap::new(),
            metadata: MetadataCollector::new(),
            camera_intrinsics: HashMap::new(),
            camera_extrinsics: HashMap::new(),
            total_frames: 0,
            images_encoded: 0,
            skipped_frames: 0,
            initialized: true, // new_local creates a fully initialized writer
            start_time: None,
            output_bytes: 0,
            failed_encodings: 0,
            use_cloud_storage: false,
            upload_coordinator: None,
            cloud_uploader,
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
        storage: Arc<dyn roboflow_storage::Storage>,
        output_prefix: String,
        local_buffer: impl AsRef<Path>,
        config: LerobotConfig,
    ) -> Result<Self> {
        let local_buffer = local_buffer.as_ref();

        // Create local buffer directory structure
        let data_dir = local_buffer.join("data/chunk-000");
        let videos_dir = local_buffer.join("videos/chunk-000");
        let meta_dir = local_buffer.join("meta");
        let params_dir = local_buffer.join("parameters");

        fs::create_dir_all(&data_dir)?;
        fs::create_dir_all(&videos_dir)?;
        fs::create_dir_all(&meta_dir)?;
        fs::create_dir_all(&params_dir)?;

        // Detect if this is cloud storage (not LocalStorage)
        use roboflow_storage::LocalStorage;
        let is_local = storage.as_any().is::<LocalStorage>();
        let use_cloud_storage = !is_local;

        tracing::info!(
            is_local,
            use_cloud_storage,
            "Cloud storage detection result"
        );

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

        // Create upload coordinator for cloud storage
        let upload_coordinator = if use_cloud_storage {
            tracing::info!("Creating upload coordinator for cloud storage...");
            let upload_config = crate::lerobot::upload::UploadConfig {
                show_progress: false,
                ..Default::default()
            };

            match crate::lerobot::upload::EpisodeUploadCoordinator::new(
                Arc::clone(&storage),
                upload_config,
                None,
            ) {
                Ok(coordinator) => {
                    tracing::info!("Upload coordinator created successfully");
                    Some(Arc::new(coordinator))
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "Failed to create upload coordinator, uploads will be done synchronously"
                    );
                    None
                }
            }
        } else {
            tracing::info!("Not creating upload coordinator (use_cloud_storage=false)");
            None
        };

        let cloud_uploader = CloudUploader::new(Arc::clone(&storage), output_prefix.clone());

        Ok(Self {
            storage,
            output_prefix,
            _local_buffer: local_buffer.to_path_buf(),
            output_dir: local_buffer.to_path_buf(),
            config,
            episode_index: 0,
            frame_data: Vec::new(),
            image_buffers: HashMap::new(),
            metadata: MetadataCollector::new(),
            camera_intrinsics: HashMap::new(),
            camera_extrinsics: HashMap::new(),
            total_frames: 0,
            images_encoded: 0,
            skipped_frames: 0,
            initialized: true, // new() creates a fully initialized writer
            start_time: None,
            output_bytes: 0,
            failed_encodings: 0,
            use_cloud_storage,
            upload_coordinator: upload_coordinator.clone(),
            cloud_uploader,
        })
    }

    /// Log the upload coordinator state for debugging
    pub fn log_upload_state(&self) {
        tracing::info!(
            use_cloud_storage = self.use_cloud_storage,
            has_upload_coordinator = self.upload_coordinator.is_some(),
            "LerobotWriter upload state"
        );
    }

    /// Add a frame to the current episode.
    /// Note: This does NOT trigger incremental flushing to avoid flushing before images are added.
    /// The flush check is deferred until after all images for a frame are added (in write_frame).
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
    /// Note: This does NOT trigger incremental flushing to avoid mid-frame flushes.
    /// The flush check is deferred until after all images for a frame are added.
    pub fn add_image(&mut self, camera: String, data: ImageData) {
        // Update shape metadata
        self.metadata
            .update_image_shape(camera.clone(), data.width as usize, data.height as usize);

        // Buffer for video encoding
        self.image_buffers.entry(camera).or_default().push(data);
    }

    /// Add image data from Arc (zero-copy if already Arc-wrapped).
    pub fn add_image_arc(&mut self, camera: String, data: Arc<ImageData>) {
        // Update shape metadata
        let inner = &*data;
        self.metadata.update_image_shape(
            camera.clone(),
            inner.width as usize,
            inner.height as usize,
        );

        // Buffer for video encoding - try to unwrap if uniquely owned
        self.image_buffers
            .entry(camera)
            .or_default()
            .push(Arc::try_unwrap(data).unwrap_or_else(|arc| (*arc).clone()));
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
        let (video_files, encode_stats) = self.encode_videos()?;
        let video_time = start.elapsed();

        // Update statistics
        self.images_encoded += encode_stats.images_encoded;
        self.skipped_frames += encode_stats.skipped_frames;
        self.failed_encodings += encode_stats.failed_encodings;
        self.output_bytes += encode_stats.output_bytes;

        tracing::debug!(
            parquet_ms = parquet_time.as_secs_f64() * 1000.0,
            video_ms = video_time.as_secs_f64() * 1000.0,
            "finish_episode timing"
        );

        // Queue upload via coordinator if available (non-blocking)
        tracing::debug!(
            has_upload_coordinator = self.upload_coordinator.is_some(),
            use_cloud_storage = self.use_cloud_storage,
            episode_index = self.episode_index,
            "Checking upload coordinator availability"
        );
        if self.upload_coordinator.is_some() {
            tracing::info!(
                episode = self.episode_index,
                "Upload coordinator available, queuing episode upload..."
            );
            // Reconstruct parquet path
            let parquet_path = self.output_dir.join(format!(
                "data/chunk-000/episode_{:06}.parquet",
                self.episode_index
            ));

            // Check if parquet file exists
            let parquet_exists = parquet_path.exists();
            tracing::info!(
                episode = self.episode_index,
                parquet_path = %parquet_path.display(),
                parquet_exists,
                "Parquet file existence check"
            );

            // Use video_files returned by encode_videos (contains (camera, PathBuf) tuples)
            // When use_cloud_storage is true with S3Storage:
            //   - encode_videos_with_coordinator uploads videos directly to S3 and returns empty video_files
            //   - Only the parquet file needs to be uploaded
            // When use_cloud_storage is false:
            //   - video_files is empty (no upload needed)
            let video_paths_for_upload: Vec<(String, PathBuf)> = if self.use_cloud_storage {
                // Use the video_files returned by encode_videos
                video_files
                    .into_iter()
                    .map(|(path, camera)| (camera, path))
                    .collect()
            } else {
                // Local storage: no upload coordinator should be used
                return Err(roboflow_core::RoboflowError::other(
                    "Upload coordinator should not be used with local storage (use_cloud_storage=false)",
                ));
            };

            tracing::info!(
                episode = self.episode_index,
                video_count = video_paths_for_upload.len(),
                "Calling queue_episode_upload"
            );

            match self.queue_episode_upload(&parquet_path, &video_paths_for_upload) {
                Ok(_) => {
                    tracing::info!(
                        episode = self.episode_index,
                        video_count = video_paths_for_upload.len(),
                        output_prefix = %self.output_prefix,
                        "Queued episode for upload via coordinator"
                    );
                }
                Err(e) => {
                    let hint = if e.to_string().contains("disconnected") {
                        " (channel disconnected — coordinator may have been shut down, e.g. job cancelled)"
                    } else {
                        ""
                    };
                    tracing::error!(
                        episode = self.episode_index,
                        error = %e,
                        "Failed to queue episode upload, files will remain local{}",
                        hint
                    );
                    // Fallback: upload this episode synchronously so data still reaches cloud
                    if self.use_cloud_storage {
                        if parquet_path.exists() {
                            if let Err(upload_e) = self.cloud_uploader.upload_parquet(&parquet_path)
                            {
                                tracing::error!(
                                    episode = self.episode_index,
                                    error = %upload_e,
                                    "Fallback Parquet upload failed"
                                );
                            } else {
                                tracing::info!(
                                    episode = self.episode_index,
                                    "Uploaded episode Parquet via fallback (coordinator unavailable)"
                                );
                            }
                        }
                        for (camera, path) in &video_paths_for_upload {
                            if path.exists() {
                                if let Err(upload_e) =
                                    self.cloud_uploader.upload_video(path, camera)
                                {
                                    tracing::error!(
                                        episode = self.episode_index,
                                        camera = %camera,
                                        error = %upload_e,
                                        "Fallback video upload failed"
                                    );
                                } else {
                                    tracing::debug!(
                                        episode = self.episode_index,
                                        camera = %camera,
                                        "Uploaded episode video via fallback"
                                    );
                                }
                            }
                        }
                    }
                }
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

    /// Estimate current memory usage in bytes.
    fn estimate_memory_bytes(&self) -> usize {
        let mut total = 0usize;

        // Frame data overhead
        total += self.frame_data.len() * 512;

        // Image data
        for images in self.image_buffers.values() {
            for img in images {
                total += img.data.len();
            }
        }

        total
    }

    /// Flush current chunk to disk (incremental flushing).
    fn flush_chunk(&mut self) -> Result<()> {
        if self.frame_data.is_empty() && self.image_buffers.is_empty() {
            return Ok(());
        }

        let frame_count = self.frame_data.len();
        let memory_bytes = self.estimate_memory_bytes();

        tracing::info!(
            frames = frame_count,
            memory_mb = memory_bytes / (1024 * 1024),
            cameras = self.image_buffers.len(),
            "Flushing chunk for memory management"
        );

        // Write parquet for this chunk
        let _parquet_path = self.write_episode_parquet()?;

        // Encode videos for this chunk
        let (video_files, encode_stats) = self.encode_videos()?;

        // Update statistics (important: track encode stats from incremental flushes)
        self.images_encoded += encode_stats.images_encoded;
        self.skipped_frames += encode_stats.skipped_frames;
        self.failed_encodings += encode_stats.failed_encodings;
        self.output_bytes += encode_stats.output_bytes;
        self.total_frames += frame_count;

        // Queue uploads if coordinator available
        if self.upload_coordinator.is_some() && !video_files.is_empty() {
            let parquet_path = self.output_dir.join(format!(
                "data/chunk-000/episode_{:06}.parquet",
                self.episode_index
            ));
            let video_paths: Vec<(String, PathBuf)> = video_files
                .into_iter()
                .map(|(path, camera)| (camera, path))
                .collect();
            let _ = self.queue_episode_upload(&parquet_path, &video_paths);
        }

        // Clear buffers
        self.frame_data.clear();
        for buffer in self.image_buffers.values_mut() {
            buffer.clear();
        }

        tracing::debug!("Chunk flushed, buffers cleared - ready for more frames");

        Ok(())
    }

    /// Write current episode to Parquet file.
    fn write_episode_parquet(&mut self) -> Result<(PathBuf, usize)> {
        let (parquet_path, size) = super::parquet::write_episode_parquet(
            &self.frame_data,
            self.episode_index,
            &self.output_dir,
        )?;

        self.output_bytes += size as u64;

        // Upload to cloud storage if enabled (without upload coordinator)
        if self.use_cloud_storage
            && self.upload_coordinator.is_none()
            && !parquet_path.as_os_str().is_empty()
        {
            self.cloud_uploader.upload_parquet(&parquet_path)?;
        }

        Ok((parquet_path, size))
    }

    /// Encode videos for all cameras.
    fn encode_videos(&mut self) -> Result<(Vec<(PathBuf, String)>, EncodeStats)> {
        if self.image_buffers.is_empty() {
            tracing::debug!(
                episode_index = self.episode_index,
                "Video skip: image_buffers empty (no add_image calls for this episode)"
            );
            return Ok((Vec::new(), EncodeStats::default()));
        }
        let total_images: usize = self.image_buffers.values().map(|v| v.len()).sum();
        tracing::debug!(
            episode_index = self.episode_index,
            cameras = self.image_buffers.len(),
            total_frames = total_images,
            "Encoding videos"
        );

        let videos_dir = self.output_dir.join("videos/chunk-000");

        // Collect camera data for encoding
        let camera_data: Vec<(String, Vec<ImageData>)> = self
            .image_buffers
            .iter()
            .map(|(camera, images)| (camera.clone(), images.clone()))
            .collect();

        // Resolve the video configuration
        let resolved = ResolvedConfig::from_video_config(&self.config.video);

        // Use streaming coordinator for cloud storage (OSS/S3)
        // For local storage, use batch encoding
        let (mut video_files, encode_stats) = if self.use_cloud_storage
            && self
                .storage
                .as_any()
                .downcast_ref::<roboflow_storage::S3Storage>()
                .is_some()
        {
            tracing::info!(
                episode_index = self.episode_index,
                "Using streaming coordinator for direct S3/OSS upload"
            );
            self.encode_videos_with_coordinator()?
        } else {
            // Batch encoding with intermediate files
            encode_videos(
                &camera_data,
                self.episode_index,
                &videos_dir,
                &resolved,
                self.config.dataset.fps,
                self.use_cloud_storage,
            )?
        };

        // Upload videos to cloud storage (without upload coordinator)
        if self.use_cloud_storage && self.upload_coordinator.is_none() && !video_files.is_empty() {
            self.cloud_uploader.upload_videos_parallel(&video_files)?;
            // Clear video files after upload to avoid double-upload
            video_files.clear();
        }

        Ok((video_files, encode_stats))
    }

    /// Encode videos using the concurrent video encoder for multi-camera parallel encoding.
    ///
    /// This method provides better performance for multi-camera setups by using
    /// dedicated encoder threads for each camera with concurrent S3/OSS upload.
    ///
    /// # Returns
    ///
    /// A tuple of (video_files, encode_stats) where video_files contains
    /// (path, camera) tuples and encode_stats contains encoding statistics.
    fn encode_videos_with_coordinator(&mut self) -> Result<(Vec<(PathBuf, String)>, EncodeStats)> {
        if self.image_buffers.is_empty() {
            tracing::debug!(
                episode_index = self.episode_index,
                "Video skip: image_buffers empty"
            );
            return Ok((Vec::new(), EncodeStats::default()));
        }

        let total_images: usize = self.image_buffers.values().map(|v| v.len()).sum();
        tracing::info!(
            episode_index = self.episode_index,
            cameras = self.image_buffers.len(),
            total_frames = total_images,
            "Encoding videos with concurrent encoder"
        );

        // Get the S3 storage backend
        let s3_storage = self
            .storage
            .as_any()
            .downcast_ref::<roboflow_storage::S3Storage>()
            .ok_or_else(|| {
                roboflow_core::RoboflowError::encode(
                    "LerobotWriter",
                    "S3 storage not available for concurrent encoder",
                )
            })?;

        // Get S3 config for the encoder
        let s3_config = s3_storage.config().clone();

        // Resolve video configuration
        let resolved = ResolvedConfig::from_video_config(&self.config.video);

        // Use relative path prefix (not full S3 URL) - the storage backend already knows the bucket
        let key_prefix = self.output_prefix.trim_end_matches('/').to_string();

        // Create concurrent encoder configuration
        // Note: chunk_index is currently 0 (single chunk). Future: compute from episode_index.
        let encoder_config = ConcurrentEncoderConfig {
            key_prefix,
            chunk_index: 0,
            episode_index: self.episode_index as u32,
            chunk_size: 256 * 1024, // 256KB chunks for streaming
            video_config: resolved.to_encoder_config(self.config.dataset.fps),
            frame_channel_capacity: self.config.streaming.ring_buffer_size,
            s3_config,
        };

        // Create concurrent encoder
        let mut encoder = ConcurrentVideoEncoder::new(encoder_config)?;

        // Add all frames from all cameras
        let mut skipped_frames = 0;
        for (camera, images) in &self.image_buffers {
            for image in images {
                if let Err(e) = encoder.add_frame(camera, image.clone()) {
                    tracing::debug!(
                        camera = %camera,
                        error = %e,
                        "Failed to add frame to encoder"
                    );
                    skipped_frames += 1;
                }
            }
        }

        // Finalize and get results
        let results = encoder.finalize()?;

        let camera_count = results.len();
        let images_encoded: usize = results.iter().map(|r| r.frames_encoded).sum();

        // When using ConcurrentVideoEncoder, videos are already uploaded to S3 directly.
        // Return empty video_files so the upload coordinator won't try to upload non-existent local files.
        let video_files: Vec<(PathBuf, String)> = Vec::new();

        let encode_stats = EncodeStats {
            images_encoded,
            skipped_frames,
            failed_encodings: 0,
            decode_failures: 0,
            output_bytes: 0,
        };

        tracing::info!(
            episode_index = self.episode_index,
            cameras = camera_count,
            images_encoded = encode_stats.images_encoded,
            "Completed encoding with concurrent encoder (videos already uploaded to S3)"
        );

        Ok((video_files, encode_stats))
    }

    /// Queue episode upload via the upload coordinator (non-blocking).
    fn queue_episode_upload(
        &self,
        parquet_path: &Path,
        video_paths: &[(String, PathBuf)],
    ) -> Result<bool> {
        tracing::info!(
            episode = self.episode_index,
            parquet_path = %parquet_path.display(),
            video_count = video_paths.len(),
            "queue_episode_upload: called with coordinator"
        );
        if let Some(coordinator) = &self.upload_coordinator {
            let episode_files = crate::lerobot::upload::EpisodeFiles {
                parquet_path: parquet_path.to_path_buf(),
                video_paths: video_paths.to_vec(),
                remote_prefix: self.output_prefix.clone(),
                episode_index: self.episode_index as u64,
            };

            tracing::info!(
                episode = self.episode_index,
                "queue_episode_upload: calling coordinator.queue_episode_upload"
            );
            match coordinator.queue_episode_upload(episode_files) {
                Ok(_) => {
                    tracing::info!(
                        episode = self.episode_index,
                        "queue_episode_upload: coordinator.queue_episode_upload returned Ok"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        episode = self.episode_index,
                        error = %e,
                        "queue_episode_upload: coordinator.queue_episode_upload returned Err"
                    );
                    return Err(e);
                }
            }
            tracing::debug!(
                episode = self.episode_index,
                "Queued episode upload via coordinator"
            );
            Ok(true)
        } else {
            tracing::warn!(
                episode = self.episode_index,
                "queue_episode_upload: no coordinator available"
            );
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
            self.metadata
                .write_all_to_storage(&self.storage, &self.output_prefix, &self.config)?;
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

    /// Set camera intrinsic parameters.
    pub fn set_camera_intrinsics(&mut self, camera: String, intrinsic: CameraIntrinsic) {
        self.camera_intrinsics.insert(camera, intrinsic);
    }

    /// Set camera extrinsic parameters.
    pub fn set_camera_extrinsics(&mut self, camera: String, extrinsic: CameraExtrinsic) {
        self.camera_extrinsics.insert(camera, extrinsic);
    }

    /// Write camera parameters to the parameters directory.
    fn write_camera_parameters(&self) -> Result<()> {
        let writer = CameraParamsWriter::new(&self.camera_intrinsics, &self.camera_extrinsics);
        writer.write(&self.output_dir)
    }

    // ========================================================================
    // Builder support
    // ========================================================================

    /// Create a builder for configuring a LeRobot writer.
    pub fn builder() -> super::builder::LerobotWriterBuilder {
        super::builder::LerobotWriterBuilder::new()
    }

    /// Internal constructor used by the builder.
    pub(super) fn new_internal(
        storage: Arc<dyn roboflow_storage::Storage>,
        output_prefix: String,
        local_buffer: PathBuf,
        config: LerobotConfig,
        use_cloud_storage: bool,
    ) -> Result<Self> {
        let local_buffer_path = local_buffer.clone();

        // Create local buffer directory structure
        let data_dir = local_buffer.join("data/chunk-000");
        let videos_dir = local_buffer.join("videos/chunk-000");
        let meta_dir = local_buffer.join("meta");
        let params_dir = local_buffer.join("parameters");

        fs::create_dir_all(&data_dir)?;
        fs::create_dir_all(&videos_dir)?;
        fs::create_dir_all(&meta_dir)?;
        fs::create_dir_all(&params_dir)?;

        // Detect if this is cloud storage
        use roboflow_storage::LocalStorage;
        let is_local = storage.as_any().is::<LocalStorage>();

        // Create upload coordinator for cloud storage
        let upload_coordinator = if use_cloud_storage && !is_local {
            let upload_config = crate::lerobot::upload::UploadConfig {
                show_progress: false,
                ..Default::default()
            };

            match crate::lerobot::upload::EpisodeUploadCoordinator::new(
                Arc::clone(&storage),
                upload_config,
                None,
            ) {
                Ok(coordinator) => Some(Arc::new(coordinator)),
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

        let cloud_uploader = CloudUploader::new(Arc::clone(&storage), output_prefix.clone());

        Ok(Self {
            storage,
            output_prefix,
            _local_buffer: local_buffer_path,
            output_dir: local_buffer,
            config,
            episode_index: 0,
            frame_data: Vec::new(),
            image_buffers: HashMap::new(),
            metadata: MetadataCollector::new(),
            camera_intrinsics: HashMap::new(),
            camera_extrinsics: HashMap::new(),
            total_frames: 0,
            images_encoded: 0,
            skipped_frames: 0,
            initialized: true,
            start_time: Some(std::time::Instant::now()),
            output_bytes: 0,
            failed_encodings: 0,
            use_cloud_storage,
            upload_coordinator,
            cloud_uploader,
        })
    }
}

// ============================================================================
// Trait implementations
// ============================================================================

/// Implement the core DatasetWriter trait for LerobotWriter.
impl DatasetWriter for LerobotWriter {
    fn write_frame(&mut self, frame: &AlignedFrame) -> Result<()> {
        if !self.initialized {
            return Err(roboflow_core::RoboflowError::encode(
                "LerobotWriter",
                "Writer not initialized. Use builder().build() to create an initialized writer.",
            ));
        }

        // Convert AlignedFrame to LerobotFrame
        let lerobot_frame = LerobotFrame::from_aligned_frame(frame, self.episode_index);

        // Add the frame
        self.add_frame(lerobot_frame);

        // Add all images for this frame BEFORE checking flush
        // This prevents mid-frame flushes that would lose other cameras' data
        for (camera, data) in &frame.images {
            self.add_image_arc(camera.clone(), data.clone());
        }

        // NOW check if we should flush (after all images for this frame are added)
        let memory_bytes = self.estimate_memory_bytes();
        if self
            .config
            .flushing
            .should_flush(self.frame_data.len(), memory_bytes)
            && let Err(e) = self.flush_chunk()
        {
            tracing::error!(
                error = %e,
                "Failed to flush chunk, continuing (memory may increase)"
            );
        }

        Ok(())
    }

    fn finalize(&mut self) -> Result<WriterStats> {
        // Finish any remaining episode
        if !self.frame_data.is_empty() {
            self.finish_episode(None)?;
        }

        // Write camera parameters
        self.write_camera_parameters()?;

        // Write metadata files
        if self.use_cloud_storage {
            self.metadata
                .write_all_to_storage(&self.storage, &self.output_prefix, &self.config)?;
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

        // Flush pending uploads to cloud storage; fail finalize if uploads don't complete or any failed
        if let Some(coordinator) = &self.upload_coordinator {
            let stats_before = coordinator.stats();
            tracing::info!(
                pending = stats_before.pending_count,
                in_progress = stats_before.in_progress_count,
                "Waiting for pending cloud uploads to complete before finalize..."
            );
            coordinator.flush().map_err(|e| {
                roboflow_core::RoboflowError::other(format!(
                    "Cloud upload flush failed: {e}. Not all data/video may have been written to sink."
                ))
            })?;
            let stats = coordinator.stats();
            if stats.failed_count > 0 {
                return Err(roboflow_core::RoboflowError::other(format!(
                    "{} cloud upload(s) failed. Data/video may be incomplete in sink.",
                    stats.failed_count
                )));
            }
            tracing::info!(
                files_uploaded = stats.total_files,
                total_bytes = stats.total_bytes,
                "All cloud uploads completed successfully"
            );
        }

        Ok(WriterStats {
            frames_written: self.total_frames,
            images_encoded: self.images_encoded,
            state_records: self.total_frames * 2,
            output_bytes: self.output_bytes,
            duration_sec: duration,
            decode_failures: self.failed_encodings,
        })
    }

    fn frame_count(&self) -> usize {
        self.total_frames + self.frame_data.len()
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
