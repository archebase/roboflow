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
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};

use crate::formats::common::{AlignedFrame, DatasetWriter, ImageRef, WriterStats};
use crate::formats::lerobot::config::LerobotConfig;
use crate::formats::lerobot::metadata::MetadataCollector;
use crate::formats::lerobot::trait_impl::{FromAlignedFrame, LerobotWriterTrait};
use polars::prelude::{DataFrame, ParquetReader, ParquetWriter, SerReader};
use roboflow_core::Result;

use super::frame::LerobotFrame;
use super::stats;
use super::{CameraExtrinsic, CameraIntrinsic, CameraParamsWriter};

/// Default episodes per chunk for LeRobot v2.1 format.
/// This matches LeRobot's default of 500 episodes per chunk.
pub const DEFAULT_EPISODES_PER_CHUNK: u32 = 500;

/// LeRobot v2.1 dataset writer.
pub struct LerobotWriter {
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

    /// Pending parquet segment paths (streaming writes), merged on finalize.
    pending_parquet_segments: Vec<PathBuf>,

    /// Parquet segment sequence counter.
    parquet_segment_index: u32,

    /// Number of episodes per chunk for LeRobot v2.1 format.
    /// Episodes 0 to episodes_per_chunk-1 go to chunk-000,
    /// episodes episodes_per_chunk to 2*episodes_per_chunk-1 go to chunk-001, etc.
    episodes_per_chunk: u32,

    /// Frame data for current episode
    frame_data: Vec<LerobotFrame>,

    /// Metadata collector
    metadata: MetadataCollector,

    /// Camera intrinsic parameters (camera_name -> intrinsic params)
    camera_intrinsics: HashMap<String, CameraIntrinsic>,

    /// Camera extrinsic parameters (camera_name -> extrinsic params)
    camera_extrinsics: HashMap<String, CameraExtrinsic>,

    /// Total frames written
    total_frames: usize,

    /// Total frames at the start of the current episode (for per-episode counting)
    episode_start_frames: usize,

    /// Whether the writer has been initialized
    initialized: bool,

    /// Start time for duration calculation
    start_time: Option<std::time::Instant>,

    /// Output bytes written
    output_bytes: u64,
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

        Ok(Self {
            output_dir: output_dir.to_path_buf(),
            config,
            episode_index: 0,
            segment_index: 0,
            session_id: uuid::Uuid::new_v4().to_string(),
            pending_parquet_segments: Vec::new(),
            parquet_segment_index: 0,
            episodes_per_chunk: DEFAULT_EPISODES_PER_CHUNK,
            frame_data: Vec::new(),
            metadata: MetadataCollector::new(),
            camera_intrinsics: HashMap::new(),
            camera_extrinsics: HashMap::new(),
            total_frames: 0,
            episode_start_frames: 0,
            initialized: true, // new_local creates a fully initialized writer
            start_time: None,
            output_bytes: 0,
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
        _storage: std::sync::Arc<dyn roboflow_storage::Storage>,
        _output_prefix: String,
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
            output_dir: local_buffer.to_path_buf(),
            config,
            episode_index: 0,
            segment_index: 0,
            session_id: uuid::Uuid::new_v4().to_string(),
            pending_parquet_segments: Vec::new(),
            parquet_segment_index: 0,
            episodes_per_chunk: DEFAULT_EPISODES_PER_CHUNK,
            frame_data: Vec::new(),
            metadata: MetadataCollector::new(),
            camera_intrinsics: HashMap::new(),
            camera_extrinsics: HashMap::new(),
            total_frames: 0,
            episode_start_frames: 0,
            initialized: true, // new() creates a fully initialized writer
            start_time: None,
            output_bytes: 0,
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

    /// Add an image reference to metadata tracking.
    ///
    /// Records the image dimensions for metadata without storing pixel data.
    /// Pixel data is routed directly to the encoder by the executor.
    pub fn add_image_ref(&mut self, camera: String, image_ref: ImageRef) {
        self.metadata.update_image_shape(
            camera,
            image_ref.width as usize,
            image_ref.height as usize,
        );
    }

    /// Start a new episode.
    ///
    /// This ensures chunk directories exist for the current episode.
    /// In distributed mode, `set_episode_index()` should be called before this
    /// to set the externally-allocated episode index.
    pub fn start_episode(&mut self, _task_index: Option<usize>) -> Result<()> {
        // Ensure chunk directories exist for this episode
        self.ensure_chunk_dirs()?;

        // Record frame count at episode start for per-episode counting
        self.episode_start_frames = self.total_frames;

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
        if self.config.flushing.incremental_video_encoding {
            // Flush remaining parquet data.
            if !self.frame_data.is_empty() {
                self.flush_parquet_segment()?;
            }

            self.merge_pending_parquet_segments()?;

            // Per-episode frame count: total_frames accumulated since start_episode()
            let episode_frames = self.total_frames - self.episode_start_frames;
            let tasks = task_index.map(|t| vec![t]).unwrap_or_default();
            self.metadata
                .add_episode(self.episode_index, episode_frames, tasks);

            return Ok(());
        }

        if self.frame_data.is_empty() {
            return Ok(());
        }

        let tasks = task_index.map(|t| vec![t]).unwrap_or_default();

        // Write Parquet file
        let (_parquet_path, _) = self.write_episode_parquet()?;

        // Calculate and store episode stats
        self.calculate_episode_stats()?;

        // Update metadata
        self.metadata
            .add_episode(self.episode_index, self.frame_data.len(), tasks);

        // Update counters
        self.total_frames += self.frame_data.len();

        // Clear for next segment/episode
        self.frame_data.clear();

        // Increment segment index for next flush
        self.segment_index += 1;

        Ok(())
    }

    /// Flush current frame buffer as a parquet segment to temporary storage.
    fn flush_parquet_segment(&mut self) -> Result<()> {
        if self.frame_data.is_empty() {
            return Ok(());
        }

        let temp_base = self.output_dir.join("temp").join(&self.session_id);
        let parquet_segment_dir = temp_base.join("parquet_segments");
        fs::create_dir_all(&parquet_segment_dir)?;
        fs::create_dir_all(parquet_segment_dir.join("data").join(self.chunk_dir_name()))?;

        let segment_path =
            parquet_segment_dir.join(format!("segment_{:06}.parquet", self.parquet_segment_index));

        let (tmp_path, _size) = super::parquet::write_episode_parquet_with_chunk(
            &self.frame_data,
            self.episode_index,
            self.chunk_index(),
            &parquet_segment_dir,
        )?;

        // Rename generated episode parquet to ordered segment path.
        fs::rename(&tmp_path, &segment_path).map_err(|e| {
            roboflow_core::RoboflowError::io(format!(
                "Failed to rename parquet segment {} -> {}: {}",
                tmp_path.display(),
                segment_path.display(),
                e
            ))
        })?;

        self.pending_parquet_segments.push(segment_path);
        self.total_frames += self.frame_data.len();
        self.frame_data.clear();
        self.parquet_segment_index += 1;

        Ok(())
    }

    /// Merge all parquet segments into final episode parquet path.
    fn merge_pending_parquet_segments(&mut self) -> Result<()> {
        if self.pending_parquet_segments.is_empty() {
            return Ok(());
        }

        fs::create_dir_all(self.data_chunk_dir())?;
        let final_path = self
            .data_chunk_dir()
            .join(format!("episode_{:06}.parquet", self.episode_index));

        if self.pending_parquet_segments.len() == 1 {
            let src = &self.pending_parquet_segments[0];
            fs::copy(src, &final_path).map_err(|e| {
                roboflow_core::RoboflowError::io(format!(
                    "Failed to copy parquet segment {} -> {}: {}",
                    src.display(),
                    final_path.display(),
                    e
                ))
            })?;
        } else {
            let mut dataframes = Vec::<DataFrame>::new();
            for segment in &self.pending_parquet_segments {
                let file = File::open(segment)?;
                let df = ParquetReader::new(file).finish().map_err(|e| {
                    roboflow_core::RoboflowError::parse(
                        "Parquet",
                        format!(
                            "Failed to read parquet segment {}: {}",
                            segment.display(),
                            e
                        ),
                    )
                })?;
                dataframes.push(df);
            }

            let mut merged = polars::functions::concat_df_diagonal(&dataframes).map_err(|e| {
                roboflow_core::RoboflowError::parse(
                    "Parquet",
                    format!("Failed to concat parquet segments: {}", e),
                )
            })?;

            let file = File::create(&final_path)?;
            let mut writer = BufWriter::new(file);
            ParquetWriter::new(&mut writer)
                .finish(&mut merged)
                .map_err(|e| {
                    roboflow_core::RoboflowError::parse(
                        "Parquet",
                        format!("Failed to write merged parquet: {}", e),
                    )
                })?;
        }

        self.output_bytes += fs::metadata(&final_path).map(|m| m.len()).unwrap_or(0);

        self.pending_parquet_segments.clear();
        Ok(())
    }

    /// Estimate current memory usage in bytes.
    fn estimate_memory_bytes(&self) -> usize {
        // Frame data overhead only (image data is now routed to encoder directly)
        self.frame_data.len() * 512
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
            output_bytes = self.output_bytes,
            duration_sec = duration,
            "Finalized LeRobot v2.1 dataset (parquet only)"
        );

        // Clean up temp directory (parquet segments, etc.)
        let temp_dir = self.output_dir.join("temp");
        if temp_dir.exists() {
            let _ = fs::remove_dir_all(&temp_dir);
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
        _storage: std::sync::Arc<dyn roboflow_storage::Storage>,
        _output_prefix: String,
        local_buffer: PathBuf,
        config: LerobotConfig,
        _use_cloud_storage: bool,
    ) -> Result<Self> {
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
            output_dir: local_buffer,
            config,
            episode_index: 0,
            segment_index: 0,
            session_id: uuid::Uuid::new_v4().to_string(),
            pending_parquet_segments: Vec::new(),
            parquet_segment_index: 0,
            episodes_per_chunk: DEFAULT_EPISODES_PER_CHUNK,
            frame_data: Vec::new(),
            metadata: MetadataCollector::new(),
            camera_intrinsics: HashMap::new(),
            camera_extrinsics: HashMap::new(),
            total_frames: 0,
            episode_start_frames: 0,
            initialized: true,
            start_time: Some(std::time::Instant::now()),
            output_bytes: 0,
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

        // Record image metadata for parquet path generation
        for (camera, image_ref) in &frame.image_refs {
            self.add_image_ref(camera.clone(), *image_ref);
        }

        // In streaming pipeline mode, flush parquet segments when row buffering
        // reaches its limit.
        if self.config.flushing.incremental_video_encoding {
            let memory_bytes = self.estimate_memory_bytes();
            let should_flush_parquet = (self.config.flushing.max_frames_per_chunk > 0
                && self.frame_data.len() >= self.config.flushing.max_frames_per_chunk)
                || (self.config.flushing.max_memory_bytes > 0
                    && memory_bytes >= self.config.flushing.max_memory_bytes);

            if should_flush_parquet {
                self.flush_parquet_segment()?;
            }
        }

        Ok(())
    }

    fn finalize(&mut self) -> Result<WriterStats> {
        if self.config.flushing.incremental_video_encoding {
            if !self.frame_data.is_empty() {
                self.flush_parquet_segment()?;
            }

            self.merge_pending_parquet_segments()?;

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
                output_bytes = self.output_bytes,
                duration_sec = duration,
                "Finalized LeRobot v2.1 dataset (parquet only)"
            );

            // Clean up temp directory (parquet segments, etc.)
            let temp_dir = self.output_dir.join("temp");
            if temp_dir.exists() {
                let _ = fs::remove_dir_all(&temp_dir);
            }

            return Ok(WriterStats {
                frames_written: self.total_frames,
                images_encoded: 0,
                state_records: self.total_frames,
                duration_sec: duration,
                output_bytes: self.output_bytes,
            });
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
            output_bytes = self.output_bytes,
            duration_sec = duration,
            "Finalized LeRobot v2.1 dataset (parquet only)"
        );

        Ok(WriterStats {
            frames_written: self.total_frames,
            images_encoded: 0,
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

    fn add_image_ref(&mut self, camera: String, image_ref: ImageRef) {
        self.add_image_ref(camera, image_ref);
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
        for camera in frame.image_refs.keys() {
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
    use crate::formats::common::{AlignedFrame, DatasetWriter, ImageRef};
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

    /// Create a small test frame with an image reference.
    fn make_frame(index: usize) -> AlignedFrame {
        let mut frame = AlignedFrame::new(index, (index as u64) * 33_333_333); // ~30fps
        frame.add_state("observation.state".to_string(), vec![index as f32; 6]);
        frame.add_action("action".to_string(), vec![index as f32; 6]);
        // Image reference (dimensions only, pixel data routed to encoder separately)
        frame.add_image_ref(
            "observation.images.cam".to_string(),
            ImageRef {
                width: 64,
                height: 48,
            },
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
        // This test verifies that parquet segments are properly merged on finalize:
        // - Memory flushes create temporary parquet segments
        // - Finalize merges all segments into a single parquet file
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
    fn test_streaming_mode_flushes_parquet_segments_during_processing() {
        let tmp = tempfile::tempdir().unwrap();

        let flushing = FlushingConfig {
            max_frames_per_chunk: 0,
            max_memory_bytes: 1,
            incremental_video_encoding: true,
        };
        let mut config = test_config(flushing);
        config.streaming.finalize_metadata_in_coordinator = true;

        let mut writer = LerobotWriter::new_local(tmp.path(), config).unwrap();

        for i in 0..12 {
            writer.write_frame(&make_frame(i)).unwrap();
        }

        // In streaming mode, flush should keep frame_data bounded and persisted as segments.
        assert_eq!(writer.frame_data.len(), 0);
        assert!(!writer.pending_parquet_segments.is_empty());

        let stats = <LerobotWriter as DatasetWriter>::finalize(&mut writer).unwrap();
        assert_eq!(stats.frames_written, 12);

        let parquet = tmp.path().join("data/chunk-000/episode_000000.parquet");
        assert!(parquet.exists(), "final merged parquet should exist");

        let file = std::fs::File::open(&parquet).unwrap();
        let df = ParquetReader::new(file).finish().unwrap();
        assert_eq!(df.height(), 12);
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
        use std::sync::Arc;
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
    fn test_write_frame_requires_initialized() {
        let tmp = tempfile::tempdir().unwrap();
        let mut writer =
            LerobotWriter::new_local(tmp.path(), test_config(FlushingConfig::default())).unwrap();

        writer.initialized = false;
        assert!(writer.write_frame(&make_frame(0)).is_err());

        writer.initialized = true;
        assert!(writer.write_frame(&make_frame(0)).is_ok());
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
        frame.add_image_ref(
            "observation.images.front".to_string(),
            ImageRef {
                width: 8,
                height: 8,
            },
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

        // Note: Video files are no longer produced by the writer — video encoding
        // is handled by the executor's image fast-path. Only parquet and metadata
        // files are uploaded here.

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
