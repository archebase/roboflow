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

use crate::formats::common::{
    AlignedFrame, ConcurrentEncoderConfig, ConcurrentVideoEncoder, DatasetWriter, ImageData,
    WriterStats,
};
use crate::formats::lerobot::config::LerobotConfig;
use crate::formats::lerobot::metadata::MetadataCollector;
use crate::formats::lerobot::trait_impl::{FromAlignedFrame, LerobotWriterTrait};
use crate::formats::lerobot::video_profiles::ResolvedConfig;
use roboflow_core::Result;

use super::camera::{CameraExtrinsic, CameraIntrinsic};
use super::camera_params::CameraParamsWriter;
use super::cloud_upload::CloudUploader;
use super::encoding::{EncodeStats, encode_videos};
use super::frame::LerobotFrame;
use super::stats;

/// Default episodes per chunk for LeRobot v2.1 format.
/// This matches LeRobot's default of 500 episodes per chunk.
pub const DEFAULT_EPISODES_PER_CHUNK: u32 = 500;

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

    /// Current episode index (logical episode, one per bag file)
    episode_index: usize,

    /// Current segment index within the episode (increments on memory flush)
    segment_index: u32,

    /// Unique session ID for temp segment paths
    session_id: String,

    /// Pending segment paths per camera, to be merged on finalize
    /// Maps camera_name -> list of segment paths in temp storage
    pending_segments: HashMap<String, Vec<PathBuf>>,

    /// Number of episodes per chunk for LeRobot v2.1 format.
    /// Episodes 0 to episodes_per_chunk-1 go to chunk-000,
    /// episodes episodes_per_chunk to 2*episodes_per_chunk-1 go to chunk-001, etc.
    episodes_per_chunk: u32,

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
    upload_coordinator: Option<Arc<crate::formats::lerobot::upload::EpisodeUploadCoordinator>>,

    /// Helper for cloud uploads
    cloud_uploader: CloudUploader,

    /// Images added since last flush (for correct flush triggering)
    images_since_flush: usize,
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

        let mut config = config;
        if config.flushing.max_frames_per_chunk > 0 {
            tracing::info!(
                max_frames_per_chunk = config.flushing.max_frames_per_chunk,
                "Disabling video segmentation for local storage"
            );
            config.flushing.max_frames_per_chunk = 0;
        }

        Ok(Self {
            storage,
            output_prefix,
            _local_buffer: local_buffer,
            output_dir: output_dir.to_path_buf(),
            config,
            episode_index: 0,
            segment_index: 0,
            session_id: uuid::Uuid::new_v4().to_string(),
            pending_segments: HashMap::new(),
            episodes_per_chunk: DEFAULT_EPISODES_PER_CHUNK,
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
            images_since_flush: 0,
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
            let upload_config = crate::formats::lerobot::upload::UploadConfig {
                show_progress: false,
                ..Default::default()
            };

            match crate::formats::lerobot::upload::EpisodeUploadCoordinator::new(
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
            segment_index: 0,
            session_id: uuid::Uuid::new_v4().to_string(),
            pending_segments: HashMap::new(),
            episodes_per_chunk: DEFAULT_EPISODES_PER_CHUNK,
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
            images_since_flush: 0,
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

    /// Calculate the chunk index for the current episode.
    ///
    /// Uses `episode_index / episodes_per_chunk` to determine which chunk
    /// the current episode belongs to.
    #[inline]
    fn chunk_index(&self) -> u32 {
        (self.episode_index / self.episodes_per_chunk as usize) as u32
    }

    /// Get the chunk directory name (e.g., "chunk-000", "chunk-001").
    #[inline]
    fn chunk_dir_name(&self) -> String {
        format!("chunk-{:03}", self.chunk_index())
    }

    /// Get the data directory path for the current chunk.
    ///
    /// Returns path like: `{output_dir}/data/chunk-000`
    fn data_chunk_dir(&self) -> PathBuf {
        self.output_dir.join("data").join(self.chunk_dir_name())
    }

    /// Get the videos directory path for the current chunk.
    ///
    /// Returns path like: `{output_dir}/videos/chunk-000`
    fn videos_chunk_dir(&self) -> PathBuf {
        self.output_dir.join("videos").join(self.chunk_dir_name())
    }

    /// Ensure chunk directories exist (locally and remotely if using cloud storage).
    fn ensure_chunk_dirs(&self) -> Result<()> {
        // Create local directories
        fs::create_dir_all(self.data_chunk_dir())?;
        fs::create_dir_all(self.videos_chunk_dir())?;

        // Create remote directories if using cloud storage
        if self.use_cloud_storage && !self.output_prefix.is_empty() {
            let data_prefix = format!("{}/data/{}", self.output_prefix, self.chunk_dir_name());
            let videos_prefix = format!("{}/videos/{}", self.output_prefix, self.chunk_dir_name());

            self.storage
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
            self.storage
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
        }

        Ok(())
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
        self.images_since_flush += 1;
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
        self.images_since_flush += 1;
    }

    /// Start a new episode.
    ///
    /// This ensures chunk directories exist for the current episode.
    /// In distributed mode, `set_episode_index()` should be called before this
    /// to set the externally-allocated episode index.
    pub fn start_episode(&mut self, _task_index: Option<usize>) -> Result<()> {
        // Ensure chunk directories exist for this episode
        self.ensure_chunk_dirs()?;

        // Reset episode state (frame_data is cleared in finish_episode)
        self.start_time = Some(std::time::Instant::now());

        Ok(())
    }

    /// Set the episode index for distributed processing.
    ///
    /// In distributed mode, episode indices are allocated centrally by
    /// an `EpisodeAllocator`. This method allows setting the externally
    /// allocated episode index before processing begins.
    ///
    /// # Arguments
    ///
    /// * `index` - The allocated episode index (global across the batch)
    ///
    /// # Example
    ///
    /// ```ignore
    /// // In a distributed worker
    /// let allocation = episode_allocator.allocate().await?;
    /// writer.set_episode_index(allocation.episode_index as usize);
    /// writer.start_episode(Some(task_index))?;
    /// ```
    pub fn set_episode_index(&mut self, index: usize) {
        self.episode_index = index;
    }

    /// Set the number of episodes per chunk.
    ///
    /// This determines how episodes are grouped into chunks.
    /// Default is 500 (matching LeRobot's convention).
    ///
    /// # Arguments
    ///
    /// * `count` - Number of episodes per chunk (e.g., 500)
    pub fn set_episodes_per_chunk(&mut self, count: u32) {
        self.episodes_per_chunk = count;
    }

    /// Get the current episode index.
    pub fn get_episode_index(&self) -> usize {
        self.episode_index
    }

    /// Get the current chunk index.
    pub fn get_chunk_index(&self) -> u32 {
        self.chunk_index()
    }

    /// Get the episodes per chunk configuration.
    pub fn get_episodes_per_chunk(&self) -> u32 {
        self.episodes_per_chunk
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
                        self.upload_fallback_sync(&parquet_path, &video_paths_for_upload);
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

        // Clear for next segment/episode
        self.frame_data.clear();
        for buffer in self.image_buffers.values_mut() {
            buffer.clear();
        }

        // Increment segment index for next flush
        // episode_index is NOT incremented here - it only changes when
        // a new source file (bag/mcap) is started, which is handled
        // by start_episode() method
        self.segment_index += 1;

        Ok(())
    }

    /// Flush video buffers to temp segment files.
    ///
    /// This method is called when memory threshold is hit during processing.
    /// It encodes the current video buffers to temporary segment files in cloud
    /// storage, without writing parquet or clearing frame data.
    ///
    /// The parquet file is written once on finalize with all accumulated frame data.
    fn flush_video_segment(&mut self) -> Result<()> {
        // Skip if no cameras have any frames
        if self.image_buffers.values().all(|v| v.is_empty()) {
            return Ok(());
        }

        let total_images: usize = self.image_buffers.values().map(|v| v.len()).sum();
        tracing::info!(
            episode_index = self.episode_index,
            segment_index = self.segment_index,
            cameras = self.image_buffers.len(),
            total_frames = total_images,
            "Flushing video segment to temporary storage"
        );

        // Encode videos to temp segment paths
        let (video_files, encode_stats) = if self.upload_coordinator.is_some() {
            self.encode_videos_with_coordinator()?
        } else {
            self.encode_videos()?
        };

        // Update statistics
        self.images_encoded += encode_stats.images_encoded;
        self.skipped_frames += encode_stats.skipped_frames;
        self.failed_encodings += encode_stats.failed_encodings;
        self.output_bytes += encode_stats.output_bytes;

        // For non-cloud storage, move encoded videos to temp paths too
        // This ensures consistent behavior across storage backends
        if !video_files.is_empty() && !self.use_cloud_storage {
            // Videos were encoded locally - move them to temp segment paths
            let temp_prefix = format!("temp/{}/episode_{:06}", self.session_id, self.episode_index);

            for (local_path, camera) in video_files {
                // Build temp segment path
                let temp_path = PathBuf::from(format!(
                    "{}/{}/segment_{:04}.mp4",
                    temp_prefix, camera, self.segment_index
                ));

                // Track for later merging
                self.pending_segments
                    .entry(camera.clone())
                    .or_default()
                    .push(temp_path.clone());

                // Move file to temp location (or copy if cross-device)
                if let Some(parent) = temp_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::rename(&local_path, &temp_path).or_else(|_| {
                    // Fallback to copy + remove if rename fails (cross-device)
                    fs::copy(&local_path, &temp_path)?;
                    fs::remove_file(&local_path)?;
                    Ok::<_, std::io::Error>(())
                })?;

                tracing::debug!(
                    camera = %camera,
                    segment_index = self.segment_index,
                    temp_path = %temp_path.display(),
                    "Moved local video to temp segment path"
                );
            }
        }

        // Clear only image buffers - keep frame_data for parquet write on finalize
        for buffer in self.image_buffers.values_mut() {
            buffer.clear();
        }
        self.images_since_flush = 0;

        // Increment segment index for next flush
        self.segment_index += 1;

        tracing::info!(
            episode_index = self.episode_index,
            segment_index = self.segment_index - 1,
            images_encoded = encode_stats.images_encoded,
            "Video segment flushed to temporary storage"
        );

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

    /// Write current episode to Parquet file.
    fn write_episode_parquet(&mut self) -> Result<(PathBuf, usize)> {
        // Ensure chunk directory exists
        fs::create_dir_all(self.data_chunk_dir())?;

        let (parquet_path, size) = super::parquet::write_episode_parquet_with_chunk(
            &self.frame_data,
            self.episode_index,
            self.chunk_index(),
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
            chunk_index = self.chunk_index(),
            cameras = self.image_buffers.len(),
            total_frames = total_images,
            "Encoding videos"
        );

        // Ensure videos chunk directory exists
        fs::create_dir_all(self.videos_chunk_dir())?;

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
                chunk_index = self.chunk_index(),
                "Using streaming coordinator for direct S3/OSS upload"
            );
            self.encode_videos_with_coordinator()?
        } else {
            // Batch encoding with intermediate files
            encode_videos(
                &camera_data,
                self.episode_index,
                &self.videos_chunk_dir(),
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
    /// Videos are uploaded to temp segment paths and tracked for later merging.
    ///
    /// # Returns
    ///
    /// A tuple of (video_files, encode_stats) where video_files contains
    /// (path, camera) tuples and encode_stats contains encoding statistics.
    fn encode_videos_with_coordinator(&mut self) -> Result<(Vec<(PathBuf, String)>, EncodeStats)> {
        // Skip if no cameras have any frames
        if self.image_buffers.values().all(|v| v.is_empty()) {
            tracing::debug!(
                episode_index = self.episode_index,
                segment_index = self.segment_index,
                "Video skip: no frames in image_buffers"
            );
            return Ok((Vec::new(), EncodeStats::default()));
        }

        let total_images: usize = self.image_buffers.values().map(|v| v.len()).sum();
        tracing::info!(
            episode_index = self.episode_index,
            segment_index = self.segment_index,
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

        // Use temp path for segment uploads
        // Format: temp/{session_id}/episode_{episode:06}/{camera}/segment_{segment:04}.mp4
        let key_prefix = format!("temp/{}/episode_{:06}", self.session_id, self.episode_index);

        // Create concurrent encoder configuration
        // Note: We use segment_index in place of episode_index for the filename
        let encoder_config = ConcurrentEncoderConfig {
            key_prefix,
            chunk_index: self.chunk_index(),
            episode_index: self.segment_index, // Use segment_index for filename
            chunk_size: 256 * 1024,            // 256KB chunks for streaming
            video_config: resolved.to_encoder_config(self.config.dataset.fps),
            frame_channel_capacity: self.config.streaming.ring_buffer_size,
            s3_config,
            use_parallel_pipeline: false,
            path_scheme: None,
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

        // Track uploaded segment paths for later merging
        for result in &results {
            let segment_path = PathBuf::from(&result.url);
            self.pending_segments
                .entry(result.camera.clone())
                .or_default()
                .push(segment_path);
        }

        // When using ConcurrentVideoEncoder, videos are already uploaded to S3 directly.
        // Return empty video_files so the upload coordinator won't try to upload non-existent local files.
        let video_files: Vec<(PathBuf, String)> = Vec::new();

        let encode_stats = EncodeStats {
            images_encoded,
            skipped_frames,
            failed_encodings: 0,
            output_bytes: 0,
        };

        tracing::info!(
            episode_index = self.episode_index,
            segment_index = self.segment_index,
            cameras = camera_count,
            images_encoded = encode_stats.images_encoded,
            "Completed encoding with concurrent encoder (videos uploaded to temp segment path)"
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
            let episode_files = crate::formats::lerobot::upload::EpisodeFiles {
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

    /// Upload episode files synchronously as fallback when coordinator fails.
    fn upload_fallback_sync(&self, parquet_path: &Path, video_paths: &[(String, PathBuf)]) {
        // Upload parquet file
        if parquet_path.exists() {
            match self.cloud_uploader.upload_parquet(parquet_path) {
                Ok(()) => {
                    tracing::info!(
                        episode = self.episode_index,
                        "Uploaded episode Parquet via fallback (coordinator unavailable)"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        episode = self.episode_index,
                        error = %e,
                        "Fallback Parquet upload failed"
                    );
                }
            }
        }

        // Upload video files
        for (camera, path) in video_paths {
            if path.exists() {
                match self.cloud_uploader.upload_video(path, camera) {
                    Ok(()) => {
                        tracing::debug!(
                            episode = self.episode_index,
                            camera = %camera,
                            "Uploaded episode video via fallback"
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            episode = self.episode_index,
                            camera = %camera,
                            error = %e,
                            "Fallback video upload failed"
                        );
                    }
                }
            }
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

        // Merge all pending segments into final episode files
        self.merge_pending_segments()?;

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

    /// Merge all pending video segments into final episode files.
    ///
    /// This method is called during finalization to compose all temporary
    /// segment files (created during memory-bounded processing) into the
    /// final episode MP4 files in the LeRobot v2.1 directory structure.
    fn merge_pending_segments(&mut self) -> Result<()> {
        if self.pending_segments.is_empty() {
            tracing::debug!("No pending segments to merge");
            return Ok(());
        }

        let chunk_index = self.chunk_index();
        let episode_index = self.episode_index;

        tracing::info!(
            session_id = %self.session_id,
            episode_index,
            chunk_index,
            cameras = self.pending_segments.len(),
            "Merging video segments into final episode files"
        );

        // Track total merged files for logging
        let mut total_merged = 0;
        let mut total_segments = 0;

        // For each camera, compose all segments into the final episode file
        for (camera, segments) in &self.pending_segments {
            if segments.is_empty() {
                continue;
            }

            total_segments += segments.len();

            // Construct final path following LeRobot v2.1 format:
            // {output_prefix}/videos/chunk-{chunk:03}/{camera}/episode_{episode:06}.mp4
            let final_path = if self.output_prefix.is_empty() {
                PathBuf::from(format!(
                    "videos/chunk-{:03}/{}/episode_{:06}.mp4",
                    chunk_index, camera, episode_index
                ))
            } else {
                PathBuf::from(format!(
                    "{}/videos/chunk-{:03}/{}/episode_{:06}.mp4",
                    self.output_prefix, chunk_index, camera, episode_index
                ))
            };

            // Log the merge operation
            tracing::debug!(
                camera = %camera,
                segment_count = segments.len(),
                final_path = %final_path.display(),
                "Composing segments into final video"
            );

            // Prepare source paths as slice of references
            let source_refs: Vec<&Path> = segments.iter().map(|p| p.as_path()).collect();

            // Compose all segments into the final file
            self.storage
                .compose_objects(&source_refs, &final_path)
                .map_err(|e| {
                    roboflow_core::RoboflowError::encode(
                        "LerobotWriter",
                        format!(
                            "Failed to compose {} segments for camera {}: {}",
                            segments.len(),
                            camera,
                            e
                        ),
                    )
                })?;

            total_merged += 1;

            tracing::info!(
                camera = %camera,
                segments_merged = segments.len(),
                final_path = %final_path.display(),
                "Merged video segments into final episode file"
            );
        }

        // Clean up temp directory for this session
        let temp_prefix = PathBuf::from(format!("temp/{}", self.session_id));
        match self.storage.delete_prefix(&temp_prefix) {
            Ok(deleted_count) => {
                tracing::debug!(
                    temp_prefix = %temp_prefix.display(),
                    deleted_count,
                    "Cleaned up temporary segment files"
                );
            }
            Err(e) => {
                // Log warning but don't fail - temp files can be cleaned up later
                tracing::warn!(
                    temp_prefix = %temp_prefix.display(),
                    error = %e,
                    "Failed to clean up temporary segment files (non-critical)"
                );
            }
        }

        tracing::info!(
            total_merged,
            total_segments,
            "Completed merging all video segments"
        );

        // Clear pending segments since they're now merged
        self.pending_segments.clear();

        Ok(())
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
            let upload_config = crate::formats::lerobot::upload::UploadConfig {
                show_progress: false,
                ..Default::default()
            };

            match crate::formats::lerobot::upload::EpisodeUploadCoordinator::new(
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
            segment_index: 0,
            session_id: uuid::Uuid::new_v4().to_string(),
            pending_segments: HashMap::new(),
            episodes_per_chunk: DEFAULT_EPISODES_PER_CHUNK,
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
            images_since_flush: 0,
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

        // Check if we should flush for memory management.
        // We flush video segments (not full episodes) to temporary storage.
        // The parquet file is written once on finalize with all accumulated frame data.
        // IMPORTANT: Use images_since_flush (not frame_data.len()) to avoid triggering
        // flush on every frame after the threshold is reached. frame_data is cumulative
        // and never cleared, while images_since_flush is reset after each flush.
        let memory_bytes = self.estimate_memory_bytes();
        if self
            .config
            .flushing
            .should_flush(self.images_since_flush, memory_bytes)
        {
            tracing::info!(
                images_since_flush = self.images_since_flush,
                total_frames = self.frame_data.len(),
                memory_mb = memory_bytes / (1024 * 1024),
                episode_index = self.episode_index,
                segment_index = self.segment_index,
                "Memory threshold reached, flushing video segment"
            );
            if let Err(e) = self.flush_video_segment() {
                tracing::error!(
                    error = %e,
                    "Failed to flush video segment for memory management, continuing (memory may increase)"
                );
            }
        }

        Ok(())
    }

    fn finalize(&mut self) -> Result<WriterStats> {
        // Flush any remaining video buffers
        if !self.image_buffers.values().all(|v| v.is_empty()) {
            self.flush_video_segment()?;
        }

        // Write parquet file with ALL accumulated frame data
        if !self.frame_data.is_empty() {
            self.write_episode_parquet()?;
            // Update metadata with episode info
            self.metadata
                .add_episode(self.episode_index, self.frame_data.len(), vec![]);
            self.total_frames += self.frame_data.len();

            // Calculate episode statistics for episodes_stats.jsonl
            self.calculate_episode_stats()?;
        }

        // Merge all video segments into final episode files
        self.merge_pending_segments()?;

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

    fn get_upload_state(&self) -> Option<crate::formats::common::base::UploadState> {
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

impl super::EpisodeWriter for LerobotWriter {
    fn set_episode_index(&mut self, index: usize) {
        self.episode_index = index;
    }

    fn get_episode_index(&self) -> usize {
        self.episode_index
    }

    fn set_episodes_per_chunk(&mut self, count: u32) {
        self.episodes_per_chunk = count;
    }

    fn get_chunk_index(&self) -> u32 {
        (self.episode_index / self.episodes_per_chunk as usize) as u32
    }

    fn get_episodes_per_chunk(&self) -> u32 {
        self.episodes_per_chunk
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::common::{AlignedFrame, DatasetWriter, ImageData};
    use crate::formats::lerobot::config::{
        DatasetConfig, FlushingConfig, LerobotConfig, StreamingConfig, VideoConfig,
    };

    /// Build a minimal LerobotConfig with a custom FlushingConfig.
    fn test_config(flushing: FlushingConfig) -> LerobotConfig {
        use crate::formats::common::config::DatasetBaseConfig;
        LerobotConfig {
            dataset: DatasetConfig {
                base: DatasetBaseConfig {
                    name: "test".to_string(),
                    fps: 30,
                    robot_type: None,
                },
                env_type: None,
            },
            mappings: Vec::new(),
            video: VideoConfig::default(),
            annotation_file: None,
            flushing,
            streaming: StreamingConfig::default(),
        }
    }

    /// Create a small test frame with a tiny image.
    fn make_frame(index: usize) -> AlignedFrame {
        let mut frame = AlignedFrame::new(index, (index as u64) * 33_333_333); // ~30fps
        frame.add_state("observation.state".to_string(), vec![index as f32; 6]);
        frame.add_action("action".to_string(), vec![index as f32; 6]);
        // 64x48 RGB image — small enough for fast encoding
        let pixels = 64 * 48 * 3;
        frame.add_image(
            "observation.images.cam".to_string(),
            ImageData::new(64, 48, vec![128u8; pixels]),
        );
        frame
    }

    #[test]
    fn test_memory_flush_creates_single_episode() {
        // This test verifies the new segment-based approach:
        // - Memory flushes create video segments (not new episodes)
        // - All frames from a single source end up in a single parquet file
        // - Video segments are merged on finalize
        let tmp = tempfile::tempdir().unwrap();

        let flushing = FlushingConfig {
            max_frames_per_chunk: 5,
            max_memory_bytes: 0, // disable memory-based flushing
            incremental_video_encoding: true,
        };
        let config = test_config(flushing);

        let mut writer = LerobotWriter::new_local(tmp.path(), config).unwrap();

        // Write 12 frames. With the new segment-based approach:
        // - Memory flush (based on max_frames_per_chunk) creates video segments
        // - All frame data is accumulated until finalize
        // - Finalize writes a single parquet file with all frames
        for i in 0..12 {
            writer.write_frame(&make_frame(i)).unwrap();
        }

        let stats = <LerobotWriter as DatasetWriter>::finalize(&mut writer).unwrap();

        // ---- Verify total frame count ----
        assert_eq!(stats.frames_written, 12, "Expected 12 total frames written");

        // ---- Verify that only ONE parquet file exists (single episode) ----
        let parquet_dir = tmp.path().join("data/chunk-000");
        let parquet_0 = parquet_dir.join("episode_000000.parquet");
        let parquet_1 = parquet_dir.join("episode_000001.parquet");
        let parquet_2 = parquet_dir.join("episode_000002.parquet");

        assert!(
            parquet_0.exists(),
            "episode_000000.parquet should exist: {:?}",
            parquet_0
        );
        // With segment-based approach, only ONE parquet file should exist
        assert!(
            !parquet_1.exists(),
            "episode_000001.parquet should NOT exist (all frames are in episode_000000): {:?}",
            parquet_1
        );
        assert!(
            !parquet_2.exists(),
            "episode_000002.parquet should NOT exist (all frames are in episode_000000): {:?}",
            parquet_2
        );
    }

    #[test]
    fn test_video_segment_merge_on_finalize() {
        // This test verifies that video segments are properly merged on finalize:
        // - Memory flushes create temporary video segments
        // - Finalize merges all segments into a single video file
        // - Temporary files are cleaned up
        let tmp = tempfile::tempdir().unwrap();

        // Use small memory limit to trigger flushes
        let flushing = FlushingConfig {
            max_frames_per_chunk: 3, // Flush every 3 frames
            max_memory_bytes: 0,     // disable memory-based flushing
            incremental_video_encoding: true,
        };
        let config = test_config(flushing);

        let mut writer = LerobotWriter::new_local(tmp.path(), config).unwrap();

        // Write 9 frames - should trigger 3 flushes (frames 0-2, 3-5, 6-8)
        for i in 0..9 {
            writer.write_frame(&make_frame(i)).unwrap();
        }

        let stats = <LerobotWriter as DatasetWriter>::finalize(&mut writer).unwrap();

        // Verify all frames were written
        assert_eq!(stats.frames_written, 9, "Expected 9 total frames written");

        // Verify a single parquet file exists
        let parquet_dir = tmp.path().join("data/chunk-000");
        let parquet_0 = parquet_dir.join("episode_000000.parquet");
        assert!(
            parquet_0.exists(),
            "episode_000000.parquet should exist: {:?}",
            parquet_0
        );

        // Verify no second episode
        let parquet_1 = parquet_dir.join("episode_000001.parquet");
        assert!(
            !parquet_1.exists(),
            "episode_000001.parquet should NOT exist: {:?}",
            parquet_1
        );

        // Verify the merged video file exists
        let video_dir = tmp.path().join("videos/chunk-000/observation.images.cam");
        let video_0 = video_dir.join("episode_000000.mp4");
        assert!(
            video_0.exists(),
            "Merged video file should exist: {:?}",
            video_0
        );

        // Verify temp directory is cleaned up
        let temp_dir = tmp.path().join("temp");
        assert!(
            !temp_dir.exists()
                || temp_dir
                    .read_dir()
                    .map(|mut d| d.next().is_none())
                    .unwrap_or(true),
            "Temp directory should be cleaned up after merge: {:?}",
            temp_dir
        );
    }
}
