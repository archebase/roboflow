// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! LeRobot v2.1 dataset writer.
//!
//! Writes robotics data in LeRobot v2.1 format with:
//! - Parquet files for frame data (one per episode)
//! - MP4 videos for camera observations (one per camera per episode)
//! - Camera parameters (intrinsic/extrinsic) in `parameters/` directory
//! - Complete metadata files

mod encoding;
mod frame;
mod parquet;
mod stats;

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
use serde::{Deserialize, Serialize};

pub use frame::LerobotFrame;

use encoding::{EncodeStats, encode_videos};

/// Camera intrinsic parameters in LeRobot format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CameraIntrinsic {
    /// Focal length x (pixels)
    pub fx: f64,
    /// Focal length y (pixels)
    pub fy: f64,
    /// Principal point x (pixels)
    pub ppx: f64,
    /// Principal point y (pixels)
    pub ppy: f64,
    /// Distortion model name
    pub distortion_model: String,
    /// k1 distortion coefficient
    pub k1: f64,
    /// k2 distortion coefficient
    pub k2: f64,
    /// k3 distortion coefficient
    pub k3: f64,
    /// p1 distortion coefficient
    pub p1: f64,
    /// p2 distortion coefficient
    pub p2: f64,
}

/// Camera extrinsic parameters in LeRobot format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CameraExtrinsic {
    /// Extrinsic data wrapper (matches LeRobot format)
    pub extrinsic: ExtrinsicData,
}

/// The actual extrinsic data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtrinsicData {
    /// 3x3 rotation matrix (row-major)
    pub rotation_matrix: Vec<Vec<f64>>,
    /// Translation vector [x, y, z]
    pub translation_vector: Vec<f64>,
}

impl CameraExtrinsic {
    /// Create extrinsic from rotation matrix and translation.
    pub fn new(rotation_matrix: [[f64; 3]; 3], translation: [f64; 3]) -> Self {
        Self {
            extrinsic: ExtrinsicData {
                rotation_matrix: vec![
                    rotation_matrix[0].to_vec(),
                    rotation_matrix[1].to_vec(),
                    rotation_matrix[2].to_vec(),
                ],
                translation_vector: translation.to_vec(),
            },
        }
    }

    /// Create extrinsic from flat arrays.
    pub fn from_arrays(rotation_matrix: [f64; 9], translation: [f64; 3]) -> Self {
        Self {
            extrinsic: ExtrinsicData {
                rotation_matrix: vec![
                    vec![rotation_matrix[0], rotation_matrix[1], rotation_matrix[2]],
                    vec![rotation_matrix[3], rotation_matrix[4], rotation_matrix[5]],
                    vec![rotation_matrix[6], rotation_matrix[7], rotation_matrix[8]],
                ],
                translation_vector: translation.to_vec(),
            },
        }
    }
}

/// LeRobot v2.1 dataset writer.
pub struct LerobotWriter {
    /// Storage backend for writing data (only available with cloud-storage feature)
    storage: std::sync::Arc<dyn roboflow_storage::Storage>,

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
    upload_coordinator: Option<std::sync::Arc<crate::lerobot::upload::EpisodeUploadCoordinator>>,
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
    /// For cloud storage support, use the storage-aware constructors when
    /// the `cloud-storage` feature is enabled.
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
        let storage = std::sync::Arc::new(roboflow_storage::LocalStorage::new(output_dir));
        let local_buffer = output_dir.to_path_buf();
        let output_prefix = String::new();

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
                std::sync::Arc::clone(&storage),
                upload_config,
                None,
            ) {
                Ok(coordinator) => {
                    tracing::info!("Upload coordinator created successfully");
                    Some(std::sync::Arc::new(coordinator))
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
                            if let Err(upload_e) = self.upload_parquet_file(&parquet_path) {
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
                                if let Err(upload_e) = self.upload_video_file(path, camera) {
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
        let (parquet_path, size) =
            parquet::write_episode_parquet(&self.frame_data, self.episode_index, &self.output_dir)?;

        self.output_bytes += size as u64;

        // Upload to cloud storage if enabled (without upload coordinator)
        if self.use_cloud_storage
            && self.upload_coordinator.is_none()
            && !parquet_path.as_os_str().is_empty()
        {
            self.upload_parquet_file(&parquet_path)?;
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
            self.upload_videos_parallel(&video_files)?;
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

        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|e| roboflow_core::RoboflowError::other(format!("No tokio runtime: {}", e)))?;

        // Resolve video configuration
        let resolved = ResolvedConfig::from_video_config(&self.config.video);

        // Build S3 URL prefix
        let bucket = s3_storage.bucket();
        let s3_prefix = if self.output_prefix.is_empty() {
            format!("s3://{}", bucket)
        } else {
            format!(
                "s3://{}/{}",
                bucket,
                self.output_prefix.trim_end_matches('/')
            )
        };

        // Create concurrent encoder configuration
        let encoder_config = ConcurrentEncoderConfig {
            s3_prefix,
            frames_per_fragment: 300, // 10 seconds @ 30fps
            temp_dir: self._local_buffer.clone(),
            video_config: resolved.to_encoder_config(self.config.dataset.fps),
            frame_channel_capacity: self.config.streaming.ring_buffer_size,
        };

        // Create concurrent encoder - pass the storage Arc directly
        let mut encoder = ConcurrentVideoEncoder::new(
            encoder_config,
            self.storage.clone(),
            runtime,
        )?;

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
        if self.camera_intrinsics.is_empty() && self.camera_extrinsics.is_empty() {
            return Ok(());
        }

        let params_dir = self.output_dir.join("parameters");

        // Write intrinsics
        for (camera, intrinsic) in &self.camera_intrinsics {
            let filename = format!("{}_intrinsic.json", camera);
            let filepath = params_dir.join(&filename);

            let json = serde_json::to_string_pretty(intrinsic).map_err(|e| {
                roboflow_core::RoboflowError::encode(
                    "CameraParameters",
                    format!("Failed to serialize intrinsic params for {}: {}", camera, e),
                )
            })?;

            fs::write(&filepath, json).map_err(|e| {
                roboflow_core::RoboflowError::encode(
                    "CameraParameters",
                    format!("Failed to write intrinsic params for {}: {}", filename, e),
                )
            })?;

            tracing::debug!(
                camera = %camera,
                file = %filename,
                "Wrote camera intrinsics"
            );
        }

        // Write extrinsics
        for (camera, extrinsic) in &self.camera_extrinsics {
            let filename = format!("{}_extrinsic.json", camera);
            let filepath = params_dir.join(&filename);

            let json = serde_json::to_string_pretty(extrinsic).map_err(|e| {
                roboflow_core::RoboflowError::encode(
                    "CameraParameters",
                    format!("Failed to serialize extrinsic params for {}: {}", camera, e),
                )
            })?;

            fs::write(&filepath, json).map_err(|e| {
                roboflow_core::RoboflowError::encode(
                    "CameraParameters",
                    format!("Failed to write extrinsic params for {}: {}", filename, e),
                )
            })?;

            tracing::debug!(
                camera = %camera,
                file = %filename,
                "Wrote camera extrinsics"
            );
        }

        Ok(())
    }
}

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

/// Builder for creating [`LerobotWriter`] instances.
///
/// # Example
///
/// ```ignore
/// use roboflow::dataset::lerobot::{LerobotWriter, LerobotConfig};
///
/// let config = LerobotConfig::default();
/// let writer = LerobotWriter::builder()
///     .output_dir("/output")
///     .config(config)
///     .build()?;
/// ```
pub struct LerobotWriterBuilder {
    output_dir: Option<PathBuf>,
    storage: Option<std::sync::Arc<dyn roboflow_storage::Storage>>,
    output_prefix: Option<String>,
    local_buffer: Option<PathBuf>,
    config: Option<LerobotConfig>,
}

impl Default for LerobotWriterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl LerobotWriterBuilder {
    /// Create a new builder with default settings.
    pub fn new() -> Self {
        Self {
            output_dir: None,
            storage: None,
            output_prefix: None,
            local_buffer: None,
            config: None,
        }
    }

    /// Set the output directory for local filesystem output.
    ///
    /// When this is set without `storage`, the writer uses LocalStorage.
    pub fn output_dir(mut self, path: impl AsRef<Path>) -> Self {
        self.output_dir = Some(path.as_ref().to_path_buf());
        self
    }

    /// Set the storage backend for cloud storage support.
    ///
    /// When set, the writer will use this storage backend for writing data.
    /// The `output_dir` is still used as a local buffer for temporary files.
    pub fn storage(mut self, storage: std::sync::Arc<dyn roboflow_storage::Storage>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Set the output prefix within storage.
    ///
    /// This is used as a prefix for all files written to cloud storage.
    /// For example, "datasets/my_dataset" would result in files at
    /// "datasets/my_dataset/data/chunk-000/...".
    pub fn output_prefix(mut self, prefix: String) -> Self {
        self.output_prefix = Some(prefix);
        self
    }

    /// Set the local buffer directory for temporary files.
    ///
    /// This is where Parquet files and videos are created before being
    /// uploaded to cloud storage (if a storage backend is configured).
    pub fn local_buffer(mut self, path: impl AsRef<Path>) -> Self {
        self.local_buffer = Some(path.as_ref().to_path_buf());
        self
    }

    /// Set the LeRobot configuration.
    pub fn config(mut self, config: LerobotConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Build the writer.
    ///
    /// # Errors
    ///
    /// Returns an error if required fields are not set.
    pub fn build(self) -> Result<LerobotWriter> {
        let config = self.config.ok_or_else(|| {
            roboflow_core::RoboflowError::parse("LerobotWriterBuilder", "config is required")
        })?;

        // Determine if we're using cloud storage
        let use_cloud_storage = self.storage.is_some();

        let (storage, output_prefix, local_buffer, _output_dir) = if let Some(storage) =
            self.storage
        {
            let local_buffer = self.local_buffer.ok_or_else(|| {
                roboflow_core::RoboflowError::parse(
                    "LerobotWriterBuilder",
                    "local_buffer is required when using cloud storage",
                )
            })?;
            let output_dir = local_buffer.clone();
            let output_prefix = self.output_prefix.unwrap_or_default();
            (storage, output_prefix, local_buffer, output_dir)
        } else {
            // Local storage mode
            let output_dir = self.output_dir.ok_or_else(|| {
                roboflow_core::RoboflowError::parse(
                    "LerobotWriterBuilder",
                    "output_dir is required (or use storage() for cloud storage)",
                )
            })?;

            // Validate output_dir is not a cloud storage URL
            let output_dir_str = output_dir.to_string_lossy();
            let lower = output_dir_str.to_lowercase();
            if lower.starts_with("s3://") || lower.starts_with("oss://") {
                return Err(roboflow_core::RoboflowError::parse(
                    "LerobotWriterBuilder",
                    "output_dir cannot be a cloud storage URL (s3:// or oss://). Use storage() method with local_buffer() instead.",
                ));
            }

            // Validate that output_dir is not a cloud storage URL
            let output_dir_str = output_dir.as_os_str().to_string_lossy();
            if output_dir_str.starts_with("s3://")
                || output_dir_str.starts_with("oss://")
                || output_dir_str.starts_with("S3://")
                || output_dir_str.starts_with("OSS://")
            {
                return Err(roboflow_core::RoboflowError::parse(
                    "LerobotWriterBuilder",
                    format!(
                        "output_dir appears to be a cloud storage URL ('{}'). For cloud storage, use the storage() method with StorageFactory instead.\n\n\
                             Example:\n\n\
                               let storage = StorageFactory::new().create(\"{}\")?;\n\n\
                               LerobotWriter::builder()\n\n\
                                   .storage(storage)\n\n\
                                   .output_prefix(\"datasets\")\n\n\
                                   .local_buffer(\"/tmp/roboflow_buffer\")\n\n\
                                   .config(config)",
                        output_dir_str, output_dir_str
                    ),
                ));
            }
            let storage =
                std::sync::Arc::new(roboflow_storage::LocalStorage::new(&output_dir)) as _;
            let local_buffer = output_dir.clone();
            let output_prefix = self.output_prefix.unwrap_or_default();
            (storage, output_prefix, local_buffer, output_dir)
        };

        LerobotWriter::new_internal(
            storage,
            output_prefix,
            local_buffer,
            config,
            use_cloud_storage,
        )
    }
}

impl LerobotWriter {
    /// Create a builder for configuring a LeRobot writer.
    pub fn builder() -> LerobotWriterBuilder {
        LerobotWriterBuilder::new()
    }

    /// Internal constructor used by the builder.
    fn new_internal(
        storage: std::sync::Arc<dyn roboflow_storage::Storage>,
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
        })
    }

    /// Upload a Parquet file to cloud storage.
    fn upload_parquet_file(&self, local_path: &Path) -> Result<()> {
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

        self.storage
            .upload_file(local_path, &remote_path)
            .map_err(|e| roboflow_core::RoboflowError::encode("Storage", format!("Upload failed: {}", e)))?;

        tracing::info!(
            local = %local_path.display(),
            remote = %remote_path.display(),
            "Uploaded Parquet file to cloud storage"
        );

        // Clean up local file after successful upload
        if let Err(e) = fs::remove_file(local_path) {
            tracing::error!(
                path = %local_path.display(),
                error = %e,
                "Failed to delete local file after upload - disk space may leak"
            );
        }

        Ok(())
    }

    /// Upload a video file to cloud storage.
    fn upload_video_file(&self, local_path: &Path, camera: &str) -> Result<()> {
        let filename = local_path
            .file_name()
            .ok_or_else(|| roboflow_core::RoboflowError::parse("Path", "Invalid file name"))?;

        let remote_path = if self.output_prefix.is_empty() {
            Path::new("videos/chunk-000").join(camera).join(filename)
        } else {
            Path::new(&self.output_prefix)
                .join("videos/chunk-000")
                .join(camera)
                .join(filename)
        };

        self.storage
            .upload_file(local_path, &remote_path)
            .map_err(|e| roboflow_core::RoboflowError::encode("Storage", format!("Upload failed: {}", e)))?;

        tracing::info!(
            local = %local_path.display(),
            remote = %remote_path.display(),
            camera = %camera,
            "Uploaded video file to cloud storage"
        );

        // Clean up local file after successful upload
        if let Err(e) = fs::remove_file(local_path) {
            tracing::error!(
                path = %local_path.display(),
                error = %e,
                "Failed to delete local file after upload - disk space may leak"
            );
        }

        Ok(())
    }

    /// Upload multiple video files to cloud storage in parallel.
    fn upload_videos_parallel(&self, video_files: &[(PathBuf, String)]) -> Result<()> {
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
}
