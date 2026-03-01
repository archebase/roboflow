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

use crate::formats::common::{AlignedFrame, DatasetWriter, ImageData, WriterStats};
use crate::formats::lerobot::config::LerobotConfig;
use crate::formats::lerobot::metadata::MetadataCollector;
use crate::formats::lerobot::trait_impl::{FromAlignedFrame, LerobotWriterTrait};
use crate::formats::lerobot::video_profiles::resolve_video_config;
use roboflow_core::Result;
use roboflow_media::video::{
    EncodeStats, RsmpegVideoComposer, VideoComposer, build_frame_buffer_static, encode_videos,
};

use super::frame::LerobotFrame;
use super::stats;
use super::{CameraExtrinsic, CameraIntrinsic, CameraParamsWriter};

/// Default episodes per chunk for LeRobot v2.1 format.
/// This matches LeRobot's default of 500 episodes per chunk.
pub const DEFAULT_EPISODES_PER_CHUNK: u32 = 500;

/// LeRobot v2.1 dataset writer.
pub struct LerobotWriter {
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

        let local_buffer = output_dir.to_path_buf();
        let output_prefix = String::new();

        let mut config = config;
        if config.flushing.max_frames_per_chunk > 0 {
            tracing::info!(
                max_frames_per_chunk = config.flushing.max_frames_per_chunk,
                "Disabling video segmentation for local storage"
            );
            config.flushing.max_frames_per_chunk = 0;
        }

        Ok(Self {
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
    #[deprecated(
        since = "0.2.0",
        note = "Cloud storage is no longer supported. Use `new_local()` instead. \
                The executor handles cloud uploads after local conversion."
    )]
    pub fn new(
        _storage: Arc<dyn roboflow_storage::Storage>,
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

        Ok(Self {
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
            images_since_flush: 0,
        })
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

    /// Ensure chunk directories exist.
    fn ensure_chunk_dirs(&self) -> Result<()> {
        // Create local directories
        fs::create_dir_all(self.data_chunk_dir())?;
        fs::create_dir_all(self.videos_chunk_dir())?;

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
        let (_video_files, encode_stats) = self.encode_videos()?;
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
    /// It encodes the current video buffers to temporary segment files,
    /// without writing parquet or clearing frame data.
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

        // Collect camera data for encoding
        let camera_data: Vec<(String, Vec<ImageData>)> = self
            .image_buffers
            .iter()
            .map(|(camera, images)| (camera.clone(), images.clone()))
            .collect();

        // Resolve video configuration
        let resolved = resolve_video_config(&self.config.video);
        let encoder_config = resolved.to_encoder_config(self.config.dataset.fps);

        // Create temp directory for segments
        let temp_base = self.output_dir.join("temp").join(&self.session_id);
        let segment_dir = temp_base.join(format!("segment_{:04}", self.segment_index));
        fs::create_dir_all(&segment_dir)?;

        // Encode each camera to a temp segment file
        let mut encode_stats = EncodeStats::default();
        let mut segment_paths: Vec<(PathBuf, String)> = Vec::new();

        for (camera, images) in &camera_data {
            if images.is_empty() {
                continue;
            }

            // Build frame buffer
            let (buffer, skipped) = build_frame_buffer_static(images)?;
            encode_stats.skipped_frames += skipped;

            if buffer.is_empty() {
                continue;
            }

            // Create temp segment path for this camera
            let camera_dir = segment_dir.join(camera);
            fs::create_dir_all(&camera_dir)?;
            let segment_path = camera_dir.join("segment.mp4");

            // Use unified VideoEncoder to create a valid MP4 file
            use roboflow_media::video::{OutputConfig, VideoEncoder};

            let output = OutputConfig::file(&segment_path);
            match VideoEncoder::new(encoder_config.clone(), output) {
                Ok(mut encoder) => {
                    let mut encode_error = None;
                    for frame in &buffer.frames {
                        if let Err(e) =
                            encoder.encode_frame(frame.data(), frame.width, frame.height)
                        {
                            encode_error = Some(e);
                            break;
                        }
                    }

                    if let Some(e) = encode_error {
                        tracing::error!(
                            camera = %camera,
                            error = %e,
                            "Failed to encode frame"
                        );
                        encode_stats.failed_encodings += 1;
                    } else {
                        match encoder.finalize() {
                            Ok(result) => {
                                encode_stats.images_encoded += buffer.len();
                                encode_stats.output_bytes += result.bytes_written;

                                segment_paths.push((segment_path.clone(), camera.clone()));

                                tracing::debug!(
                                    camera = %camera,
                                    segment_index = self.segment_index,
                                    frames = buffer.len(),
                                    path = %segment_path.display(),
                                    "Encoded video segment"
                                );
                            }
                            Err(e) => {
                                tracing::error!(
                                    camera = %camera,
                                    error = %e,
                                    "Failed to finalize video encoder"
                                );
                                encode_stats.failed_encodings += 1;
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(
                        camera = %camera,
                        error = %e,
                        "Failed to create video encoder"
                    );
                    encode_stats.failed_encodings += 1;
                }
            }
        }

        // Update statistics
        self.images_encoded += encode_stats.images_encoded;
        self.skipped_frames += encode_stats.skipped_frames;
        self.failed_encodings += encode_stats.failed_encodings;
        self.output_bytes += encode_stats.output_bytes;

        // Track segment paths for later merging
        for (segment_path, camera) in segment_paths {
            self.pending_segments
                .entry(camera.clone())
                .or_default()
                .push(segment_path.clone());

            tracing::debug!(
                camera = %camera,
                segment_index = self.segment_index,
                segment_path = %segment_path.display(),
                "Tracked video segment for later merging"
            );
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
        let resolved = resolve_video_config(&self.config.video);

        // Batch encoding with intermediate files
        let (video_files, encode_stats) = encode_videos(
            &camera_data,
            self.episode_index,
            &self.videos_chunk_dir(),
            &resolved,
            self.config.dataset.fps,
            false, // local-only
        )?;

        Ok((video_files, encode_stats))
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

        if self.config.streaming.finalize_metadata_in_coordinator {
            tracing::info!(
                output_dir = %self.output_dir.display(),
                "Skipping local metadata write; coordinator finalizes metadata"
            );
        } else {
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

    /// Finalize the dataset and upload all files to cloud storage.
    ///
    /// This method:
    /// 1. Calls `finalize()` to complete local processing
    /// 2. Uploads all files from the local output directory to a cloud storage staging path
    /// 3. Returns the total frames and the list of uploaded files
    ///
    /// # Arguments
    ///
    /// * `storage` - The storage backend to upload to
    /// * `staging_prefix` - Destination prefix in storage for uploaded files
    ///
    /// # Returns
    ///
    /// A tuple of (total_frames, uploaded_files_metadata)
    pub fn finalize_with_upload<S>(
        self,
        storage: &S,
        staging_prefix: &std::path::Path,
    ) -> Result<(usize, Vec<roboflow_storage::ObjectMetadata>)>
    where
        S: roboflow_storage::Storage + Clone + Send + 'static,
    {
        // Get output_dir before finalize consumes self
        let output_dir = self.output_dir.clone();

        // Step 1: Call finalize() to complete local processing
        let total_frames = self.finalize()?;

        // Step 2: Upload all files from the local output directory
        // Use Arc to wrap the storage reference for trait object compatibility
        let storage_arc = std::sync::Arc::new(storage.clone());
        let uploaded = roboflow_storage::upload::upload_directory_recursive(
            storage_arc,
            &output_dir,
            staging_prefix,
        )
        .map_err(|e| roboflow_core::RoboflowError::storage("upload", e.to_string(), false))?;

        tracing::info!(
            output_dir = %output_dir.display(),
            staging_prefix = %staging_prefix.display(),
            file_count = uploaded.len(),
            total_frames,
            "Uploaded dataset to cloud storage"
        );

        // Step 3: Return the frame count and upload metadata
        Ok((total_frames, uploaded))
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

            // Copy all segments to temp files for composition
            let temp_dir = tempfile::tempdir().map_err(|e| {
                roboflow_core::RoboflowError::io(format!("Failed to create temp dir: {}", e))
            })?;
            let mut temp_segments: Vec<PathBuf> = Vec::new();
            for (i, segment) in segments.iter().enumerate() {
                let temp_path = temp_dir.path().join(format!("segment_{}.mp4", i));
                fs::copy(segment, &temp_path).map_err(|e| {
                    roboflow_core::RoboflowError::io(format!(
                        "Failed to copy segment {}: {}",
                        segment.display(),
                        e
                    ))
                })?;
                temp_segments.push(temp_path);
            }

            // Compose segments locally
            let composed_path = temp_dir.path().join("composed.mp4");
            let source_refs: Vec<&Path> = temp_segments.iter().map(|p| p.as_path()).collect();
            let composer = RsmpegVideoComposer::new();
            composer
                .compose(&source_refs, &composed_path)
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

            // Copy the composed file to final destination (local file operation)
            let final_full_path = self.output_dir.join(&final_path);
            if let Some(parent) = final_full_path.parent() {
                fs::create_dir_all(parent).map_err(|e| {
                    roboflow_core::RoboflowError::io(format!(
                        "Failed to create video directory {}: {}",
                        parent.display(),
                        e
                    ))
                })?;
            }
            fs::copy(&composed_path, &final_full_path).map_err(|e| {
                roboflow_core::RoboflowError::io(format!(
                    "Failed to copy composed video to {}: {}",
                    final_full_path.display(),
                    e
                ))
            })?;

            total_merged += 1;

            tracing::info!(
                camera = %camera,
                segments_merged = segments.len(),
                final_path = %final_path.display(),
                "Merged video segments into final episode file"
            );
        }

        // Clean up temp directory for this session (local file operation)
        let temp_dir = self.output_dir.join("temp").join(&self.session_id);
        if temp_dir.exists() {
            match fs::remove_dir_all(&temp_dir) {
                Ok(()) => {
                    tracing::debug!(
                        temp_dir = %temp_dir.display(),
                        "Cleaned up temporary segment files"
                    );
                }
                Err(e) => {
                    // Log warning but don't fail - temp files can be cleaned up later
                    tracing::warn!(
                        temp_dir = %temp_dir.display(),
                        error = %e,
                        "Failed to clean up temporary segment files (non-critical)"
                    );
                }
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
        _storage: Arc<dyn roboflow_storage::Storage>,
        output_prefix: String,
        local_buffer: PathBuf,
        config: LerobotConfig,
        _use_cloud_storage: bool,
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

        Ok(Self {
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

        if self.config.streaming.finalize_metadata_in_coordinator {
            tracing::info!(
                output_dir = %self.output_dir.display(),
                "Skipping local metadata write; coordinator finalizes metadata"
            );
        } else {
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
    use crate::formats::lerobot::writer::EpisodeWriter;
    use crate::formats::lerobot::writer::{CameraExtrinsic, CameraIntrinsic};
    use roboflow_storage::LocalStorage;

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

    #[test]
    fn test_new_local_rejects_cloud_output_urls() {
        let cfg = test_config(FlushingConfig::default());

        assert!(LerobotWriter::new_local("s3://bucket/path", cfg.clone()).is_err());
        assert!(LerobotWriter::new_local("oss://bucket/path", cfg.clone()).is_err());
        assert!(LerobotWriter::new_local("S3://bucket/path", cfg.clone()).is_err());
        assert!(LerobotWriter::new_local("OSS://bucket/path", cfg).is_err());
    }

    #[allow(deprecated)]
    #[test]
    fn test_deprecated_constructors_and_internal_constructor() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = test_config(FlushingConfig::default());

        let via_create = LerobotWriter::create(tmp.path(), cfg.clone()).unwrap();
        assert!(via_create.is_initialized());

        let storage = Arc::new(LocalStorage::new(tmp.path())) as Arc<dyn roboflow_storage::Storage>;
        let via_new = LerobotWriter::new(
            storage.clone(),
            "prefix".to_string(),
            tmp.path(),
            cfg.clone(),
        )
        .unwrap();
        assert!(via_new.is_initialized());

        let internal = LerobotWriter::new_internal(
            storage,
            "prefix2".to_string(),
            tmp.path().join("buf"),
            cfg,
            false,
        )
        .unwrap();
        assert!(internal.is_initialized());
    }

    #[test]
    fn test_chunk_accessors_and_episode_writer_trait_methods() {
        let tmp = tempfile::tempdir().unwrap();
        let mut writer =
            LerobotWriter::new_local(tmp.path(), test_config(FlushingConfig::default())).unwrap();

        writer.set_episodes_per_chunk(10);
        writer.set_episode_index(25);
        assert_eq!(writer.get_episodes_per_chunk(), 10);
        assert_eq!(writer.get_episode_index(), 25);
        assert_eq!(writer.get_chunk_index(), 2);

        <LerobotWriter as EpisodeWriter>::set_episodes_per_chunk(&mut writer, 7);
        <LerobotWriter as EpisodeWriter>::set_episode_index(&mut writer, 15);
        assert_eq!(
            <LerobotWriter as EpisodeWriter>::get_episodes_per_chunk(&writer),
            7
        );
        assert_eq!(
            <LerobotWriter as EpisodeWriter>::get_episode_index(&writer),
            15
        );
        assert_eq!(
            <LerobotWriter as EpisodeWriter>::get_chunk_index(&writer),
            2
        );

        writer.start_episode(None).unwrap();
        assert!(tmp.path().join("data/chunk-002").exists());
        assert!(tmp.path().join("videos/chunk-002").exists());
    }

    #[test]
    fn test_write_frame_requires_initialized_and_empty_helpers() {
        let tmp = tempfile::tempdir().unwrap();
        let mut writer =
            LerobotWriter::new_local(tmp.path(), test_config(FlushingConfig::default())).unwrap();

        writer.initialized = false;
        assert!(writer.write_frame(&make_frame(0)).is_err());

        writer.initialized = true;
        let (files, stats) = writer.encode_videos().unwrap();
        assert!(files.is_empty());
        assert_eq!(stats.images_encoded, 0);

        writer.flush_video_segment().unwrap();
        assert_eq!(writer.skipped_frames(), 0);
        assert_eq!(writer.failed_encodings(), 0);
    }

    #[test]
    fn test_camera_params_register_task_and_finalize_no_frames() {
        let tmp = tempfile::tempdir().unwrap();
        let mut writer =
            LerobotWriter::new_local(tmp.path(), test_config(FlushingConfig::default())).unwrap();

        let t0 = writer.register_task("pick".to_string());
        let t1 = writer.register_task("pick".to_string());
        assert_eq!(t0, t1);
        assert_eq!(writer.metadata().tasks.len(), 1);

        writer.set_camera_intrinsics(
            "cam_a".to_string(),
            CameraIntrinsic {
                fx: 1.0,
                fy: 1.0,
                ppx: 0.0,
                ppy: 0.0,
                distortion_model: "none".to_string(),
                k1: 0.0,
                k2: 0.0,
                k3: 0.0,
                p1: 0.0,
                p2: 0.0,
            },
        );
        writer.set_camera_extrinsics(
            "cam_a".to_string(),
            CameraExtrinsic::new(
                [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]],
                [0.0, 0.0, 0.0],
            ),
        );

        let stats = <LerobotWriter as DatasetWriter>::finalize(&mut writer).unwrap();
        assert_eq!(stats.frames_written, 0);
        assert!(tmp.path().join("parameters/cam_a_intrinsic.json").exists());
        assert!(tmp.path().join("parameters/cam_a_extrinsic.json").exists());
        assert!(tmp.path().join("meta/info.json").exists());
    }

    #[test]
    fn test_finalize_skips_local_metadata_when_coordinator_enabled() {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = test_config(FlushingConfig::default());
        config.streaming.finalize_metadata_in_coordinator = true;

        let mut writer = LerobotWriter::new_local(tmp.path(), config).unwrap();
        let stats = <LerobotWriter as DatasetWriter>::finalize(&mut writer).unwrap();

        assert_eq!(stats.frames_written, 0);
        assert!(!tmp.path().join("meta/info.json").exists());
        assert!(!tmp.path().join("meta/episodes.jsonl").exists());
        assert!(!tmp.path().join("meta/episodes_stats.jsonl").exists());
    }

    #[test]
    fn test_from_aligned_frame_conversion_paths() {
        let mut frame = AlignedFrame::new(3, 1_500_000_000);
        frame.add_state("robot_observation".to_string(), vec![1.0, 2.0]);
        frame.add_action("action".to_string(), vec![0.5, 0.2]);
        frame.add_image(
            "observation.images.front".to_string(),
            ImageData::new(8, 8, vec![0; 8 * 8 * 3]),
        );

        let converted = LerobotFrame::from_aligned_frame(&frame, 12);
        assert_eq!(converted.episode_index, 12);
        assert_eq!(converted.frame_index, 3);
        assert!(converted.observation_state.is_some());
        assert!(converted.action.is_some());
        assert!(
            converted
                .image_frames
                .contains_key("observation.images.front")
        );
    }

    #[test]
    fn test_finalize_with_upload() {
        use roboflow_storage::mock::MockStorage;

        let tmp = tempfile::tempdir().unwrap();
        let storage = MockStorage::new();

        let mut writer =
            LerobotWriter::new_local(tmp.path(), test_config(FlushingConfig::default())).unwrap();

        // Write 3 frames
        for i in 0..3 {
            writer.write_frame(&make_frame(i)).unwrap();
        }

        // Finalize with upload
        let staging_prefix = std::path::Path::new("datasets/test_episode");
        let (total_frames, uploaded) = writer
            .finalize_with_upload(&storage, staging_prefix)
            .unwrap();

        // Verify frame count
        assert_eq!(total_frames, 3, "Expected 3 total frames");

        // Verify files were uploaded
        assert!(!uploaded.is_empty(), "Expected uploaded files");

        // Check that parquet file was uploaded
        let parquet_uploaded = uploaded
            .iter()
            .any(|meta| meta.path.contains("episode_000000.parquet"));
        assert!(
            parquet_uploaded,
            "Expected parquet file to be uploaded: {:?}",
            uploaded
        );

        // Check that video file was uploaded
        let video_uploaded = uploaded
            .iter()
            .any(|meta| meta.path.contains("episode_000000.mp4"));
        assert!(
            video_uploaded,
            "Expected video file to be uploaded: {:?}",
            uploaded
        );

        // Verify metadata files were uploaded
        let info_uploaded = uploaded.iter().any(|meta| meta.path.contains("info.json"));
        assert!(
            info_uploaded,
            "Expected info.json to be uploaded: {:?}",
            uploaded
        );

        // Verify all uploaded paths contain the staging prefix
        for meta in &uploaded {
            assert!(
                meta.path.starts_with("datasets/test_episode"),
                "Uploaded path should start with staging prefix: {}",
                meta.path
            );
        }
    }
}
