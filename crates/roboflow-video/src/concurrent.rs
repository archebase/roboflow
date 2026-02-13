// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Concurrent video encoder for multi-camera streaming.
//!
//! This module provides `ConcurrentVideoEncoder` which orchestrates
//! video encoding for multiple cameras with concurrent upload.
//!
//! # Design
//!
//! - Per-camera pipeline threads (frame buffering + encoding + upload)
//! - Each pipeline manages its own `MultipartUpload` directly
//! - Backpressure via bounded channels
//! - Clean abort on errors
//!
//! # Example
//!
//! ```ignore
//! use roboflow_video::{ConcurrentVideoEncoder, ConcurrentEncoderConfig};
//! use roboflow_storage::S3Storage;
//! use std::sync::Arc;
//!
//! let config = ConcurrentEncoderConfig {
//!     key_prefix: "dataset/episode_001".to_string(),
//!     ..Default::default()
//! };
//!
//! let storage = Arc::new(S3Storage::from_env("my-bucket")?);
//! let runtime = tokio::runtime::Handle::current();
//!
//! let mut encoder = ConcurrentVideoEncoder::new(config, storage, runtime)?;
//!
//! // Add frames for different cameras
//! encoder.add_frame("cam0", image_data)?;
//! encoder.add_frame("cam1", image_data)?;
//!
//! // Finalize and get results
//! let results = encoder.finalize()?;
//! for result in results {
//!     println!("{}: {} frames -> {}", result.camera, result.frames_encoded, result.url);
//! }
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crossbeam_channel::{Receiver, Sender};
use roboflow_core::{Result, RoboflowError};
use roboflow_storage::{MultipartUpload, S3Storage, Storage, StorageStreamingExt};
use tokio::runtime::Handle;

use crate::ImageData;
use crate::config::VideoEncoderConfig;
use crate::fragment::{FragmentEncoder, FragmentEncoderConfig, FragmentInfo};
use crate::frame::VideoFrame;

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for concurrent video encoder.
#[derive(Debug, Clone)]
pub struct ConcurrentEncoderConfig {
    /// Key prefix for output videos within the storage bucket.
    /// This should be a relative path (e.g., "dataset/episode_001"), not a full URL.
    pub key_prefix: String,
    /// Chunk index for video organization (0 = chunk-000).
    /// Videos will be saved as "{prefix}/videos/chunk-{chunk:03d}/{camera}/episode_{episode:06d}.mp4".
    pub chunk_index: u32,
    /// Episode index for video filename (0 = episode_000000).
    pub episode_index: u32,
    /// Frames per fragment (affects memory usage and upload frequency).
    pub frames_per_fragment: usize,
    /// Temp directory for fragment files.
    pub temp_dir: PathBuf,
    /// Video encoder configuration.
    pub video_config: VideoEncoderConfig,
    /// Frame channel capacity (backpressure threshold).
    pub frame_channel_capacity: usize,
}

impl Default for ConcurrentEncoderConfig {
    fn default() -> Self {
        Self {
            key_prefix: String::new(),
            chunk_index: 0,
            episode_index: 0,
            frames_per_fragment: 300,
            temp_dir: std::env::temp_dir(),
            video_config: VideoEncoderConfig::default(),
            frame_channel_capacity: 64,
        }
    }
}

// =============================================================================
// Result Types
// =============================================================================

/// Result from the concurrent encoder.
#[derive(Debug, Clone)]
pub struct ConcurrentEncoderResult {
    /// Camera name.
    pub camera: String,
    /// Destination URL.
    pub url: String,
    /// Frames encoded.
    pub frames_encoded: usize,
    /// Frames skipped.
    pub frames_skipped: usize,
}

/// Result from camera pipeline finalization.
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct CameraPipelineResult {
    /// Camera name.
    camera: String,
    /// Total frames encoded.
    frames_encoded: usize,
    /// Total fragments created.
    fragments_created: usize,
    /// Frames skipped (decode failures, dimension mismatches).
    frames_skipped: usize,
}

// =============================================================================
// Upload Commands
// =============================================================================

/// Command for upload threads.
#[allow(dead_code)]
#[derive(Debug)]
enum UploadCommand {
    /// Upload a fragment.
    UploadFragment {
        camera: String,
        fragment: FragmentInfo,
    },
    /// Finish upload for a camera.
    Finish { camera: String },
    /// Abort all uploads.
    AbortAll,
}

// =============================================================================
// Pipeline Commands
// =============================================================================

/// Command for the camera pipeline thread.
#[derive(Debug)]
enum PipelineCommand {
    /// Add a frame to the pipeline.
    AddFrame { image: ImageData },
    /// Flush remaining frames and finish.
    Flush,
    /// Shutdown immediately (abort).
    Shutdown,
}

// =============================================================================
// Camera Pipeline
// =============================================================================

/// Per-camera encoding pipeline.
///
/// Runs in its own thread, receiving frames, buffering, encoding fragments,
/// and sending them to the uploader.
struct CameraPipeline {
    /// Camera name.
    camera: String,
    /// Command receiver.
    cmd_rx: Receiver<PipelineCommand>,
    /// Upload command sender.
    upload_tx: Sender<UploadCommand>,
    /// Fragment encoder.
    encoder: FragmentEncoder,
    /// Frame buffer.
    frame_buffer: Vec<VideoFrame>,
    /// Max frames per fragment.
    frames_per_fragment: usize,
    /// Video dimensions (set from first frame).
    width: u32,
    height: u32,
    /// Statistics.
    frames_encoded: usize,
    frames_skipped: usize,
    fragments_created: usize,
}

impl CameraPipeline {
    /// Run the pipeline thread.
    fn run(mut self) -> Result<CameraPipelineResult> {
        // Extract fields needed for communication before the loop
        let camera = self.camera.clone();
        let upload_tx = self.upload_tx.clone();

        // Use recv() in a loop to avoid borrow conflicts with &mut self
        // Note: Cannot use while let because Shutdown case needs to return Err
        #[allow(clippy::while_let_loop)]
        loop {
            match self.cmd_rx.recv() {
                Ok(cmd) => match cmd {
                    PipelineCommand::AddFrame { image } => {
                        self.handle_frame(image);
                    }
                    PipelineCommand::Flush => {
                        break;
                    }
                    PipelineCommand::Shutdown => {
                        // Signal uploader to abort
                        let _ = upload_tx.send(UploadCommand::AbortAll);
                        return Err(RoboflowError::encode(
                            "CameraPipeline",
                            format!("Camera {} shutdown requested", camera),
                        ));
                    }
                },
                Err(_) => {
                    // Channel closed, exit gracefully
                    break;
                }
            }
        }

        // Flush remaining frames
        self.flush_remaining(&upload_tx)?;

        // Signal uploader that this camera is done
        upload_tx
            .send(UploadCommand::Finish {
                camera: camera.clone(),
            })
            .map_err(|e| {
                RoboflowError::encode(
                    "CameraPipeline",
                    format!("Failed to send finish command: {}", e),
                )
            })?;

        // Log summary
        if self.frames_skipped > 0 {
            tracing::warn!(
                camera = %camera,
                frames_encoded = self.frames_encoded,
                frames_skipped = self.frames_skipped,
                "Camera pipeline completed with skipped frames"
            );
        } else {
            tracing::info!(
                camera = %camera,
                frames_encoded = self.frames_encoded,
                fragments = self.fragments_created,
                "Camera pipeline completed"
            );
        }

        Ok(CameraPipelineResult {
            camera,
            frames_encoded: self.frames_encoded,
            fragments_created: self.fragments_created,
            frames_skipped: self.frames_skipped,
        })
    }

    fn handle_frame(&mut self, image: ImageData) {
        // Skip images with zero dimensions
        if image.width == 0 || image.height == 0 {
            tracing::debug!(camera = %self.camera, "Skipping frame with zero dimensions");
            self.frames_skipped += 1;
            return;
        }

        // Set dimensions from first frame
        if self.width == 0 {
            self.width = image.width;
            self.height = image.height;
        }

        // Validate dimensions
        if image.width != self.width || image.height != self.height {
            tracing::debug!(
                camera = %self.camera,
                expected = format!("{}x{}", self.width, self.height),
                actual = format!("{}x{}", image.width, image.height),
                "Skipping frame due to dimension mismatch"
            );
            self.frames_skipped += 1;
            return;
        }

        // Decode image to RGB
        let rgb_data = match decode_to_rgb(&image) {
            Some(data) => data.2,
            None => {
                tracing::debug!(camera = %self.camera, "Failed to decode frame");
                self.frames_skipped += 1;
                return;
            }
        };

        // Create video frame and add to buffer
        let frame = VideoFrame::new(self.width, self.height, rgb_data);
        self.frame_buffer.push(frame);
        self.frames_encoded += 1;

        // Encode fragment when buffer is full
        if self.frame_buffer.len() >= self.frames_per_fragment {
            let frames = std::mem::take(&mut self.frame_buffer);
            match self.encoder.encode(frames) {
                Ok(fragment) => {
                    if let Err(e) = self.upload_tx.send(UploadCommand::UploadFragment {
                        camera: self.camera.clone(),
                        fragment,
                    }) {
                        tracing::error!(camera = %self.camera, error = %e, "Failed to send fragment");
                    }
                    self.fragments_created += 1;
                }
                Err(e) => {
                    tracing::error!(camera = %self.camera, error = %e, "Failed to encode fragment");
                }
            }
        }
    }

    fn flush_remaining(&mut self, upload_tx: &Sender<UploadCommand>) -> Result<()> {
        if !self.frame_buffer.is_empty() {
            let frames = std::mem::take(&mut self.frame_buffer);
            let fragment = self.encoder.encode(frames)?;
            upload_tx
                .send(UploadCommand::UploadFragment {
                    camera: self.camera.clone(),
                    fragment,
                })
                .map_err(|e| {
                    RoboflowError::encode(
                        "CameraPipeline",
                        format!("Failed to send fragment: {}", e),
                    )
                })?;
            self.fragments_created += 1;
        }
        Ok(())
    }
}

// =============================================================================
// Pipeline Handle
// =============================================================================

/// Handle for a running camera pipeline thread.
struct CameraPipelineHandle {
    /// Camera name.
    #[allow(dead_code)]
    camera: String,
    /// Command sender.
    cmd_tx: Sender<PipelineCommand>,
    /// Pipeline thread join handle.
    thread_handle: Option<std::thread::JoinHandle<Result<CameraPipelineResult>>>,
    /// Upload thread join handle.
    upload_thread_handle: Option<std::thread::JoinHandle<()>>,
}

impl CameraPipelineHandle {
    /// Send a frame to the pipeline.
    fn add_frame(&self, image: ImageData) -> Result<()> {
        self.cmd_tx
            .send(PipelineCommand::AddFrame { image })
            .map_err(|e| {
                RoboflowError::encode(
                    "CameraPipelineHandle",
                    format!("Failed to send frame: {}", e),
                )
            })
    }

    /// Signal the pipeline to flush and finish.
    fn flush(&self) -> Result<()> {
        self.cmd_tx.send(PipelineCommand::Flush).map_err(|e| {
            RoboflowError::encode(
                "CameraPipelineHandle",
                format!("Failed to send flush: {}", e),
            )
        })
    }

    /// Signal the pipeline to shutdown immediately.
    fn shutdown(&self) -> Result<()> {
        self.cmd_tx.send(PipelineCommand::Shutdown).map_err(|e| {
            RoboflowError::encode(
                "CameraPipelineHandle",
                format!("Failed to send shutdown: {}", e),
            )
        })
    }

    /// Wait for the pipeline to finish and get the result.
    fn join(mut self) -> Result<CameraPipelineResult> {
        // First wait for the pipeline thread
        let handle = self.thread_handle.take();
        let result = if let Some(handle) = handle {
            handle.join().map_err(|e| {
                RoboflowError::other(format!("Camera pipeline thread panicked: {:?}", e))
            })?
        } else {
            Err(RoboflowError::other("Pipeline thread already joined"))
        }?;

        // Then wait for the upload thread to complete
        if let Some(upload_handle) = self.upload_thread_handle.take()
            && let Err(e) = upload_handle.join()
        {
            tracing::warn!("Upload thread panicked: {:?}", e);
        }

        Ok(result)
    }
}

/// Decode image data to RGB format.
///
/// Handles both raw RGB and encoded (JPEG/PNG) data.
fn decode_to_rgb(image: &ImageData) -> Option<(u32, u32, Vec<u8>)> {
    if image.width == 0 || image.height == 0 {
        return None;
    }

    if image.is_encoded {
        // Decode JPEG/PNG to RGB
        let img = image::load_from_memory(&image.data).ok()?;
        let rgb = img.to_rgb8();
        Some((rgb.width(), rgb.height(), rgb.into_raw()))
    } else {
        // Already RGB
        Some((image.width, image.height, image.data.clone()))
    }
}

// =============================================================================
// Concurrent Video Encoder
// =============================================================================

/// Concurrent video encoder for multiple cameras.
///
/// This orchestrates per-camera encoding pipelines. Each pipeline
/// manages its own upload using the `MultipartUpload` trait.
#[allow(dead_code)]
pub struct ConcurrentVideoEncoder {
    /// Per-camera pipeline handles.
    pipelines: HashMap<String, CameraPipelineHandle>,
    /// Storage backend for uploads.
    storage: Arc<dyn Storage>,
    /// Tokio runtime.
    runtime: Handle,
    /// Configuration.
    config: ConcurrentEncoderConfig,
    /// Whether the encoder has been finalized.
    finalized: bool,
    /// Destination URLs per camera.
    dest_urls: HashMap<String, String>,
}

impl ConcurrentVideoEncoder {
    /// Create a new concurrent video encoder.
    ///
    /// # Arguments
    ///
    /// * `config` - Encoder configuration
    /// * `storage` - Storage backend (must be S3Storage for cloud uploads)
    /// * `runtime` - Tokio runtime handle for async operations
    pub fn new(
        config: ConcurrentEncoderConfig,
        storage: Arc<dyn Storage>,
        runtime: Handle,
    ) -> Result<Self> {
        // Ensure temp directory exists
        std::fs::create_dir_all(&config.temp_dir).map_err(|e| {
            RoboflowError::encode(
                "ConcurrentVideoEncoder",
                format!("Failed to create temp directory: {}", e),
            )
        })?;

        Ok(Self {
            pipelines: HashMap::new(),
            storage,
            runtime,
            config,
            finalized: false,
            dest_urls: HashMap::new(),
        })
    }

    /// Build the destination key for a camera in LeRobot v2.1 format.
    /// Format: {prefix}/videos/chunk-{chunk:03}/{camera}/episode_{episode:06}.mp4
    fn build_dest_url(&self, camera: &str) -> String {
        let prefix = self.config.key_prefix.trim_end_matches('/');
        format!(
            "{}/videos/chunk-{:03}/{}/episode_{:06}.mp4",
            prefix, self.config.chunk_index, camera, self.config.episode_index
        )
    }

    /// Ensure a pipeline exists for the given camera.
    fn ensure_pipeline(&mut self, camera: &str) -> Result<()> {
        if self.pipelines.contains_key(camera) {
            return Ok(());
        }

        // Build destination URL
        let dest_url = self.build_dest_url(camera);
        self.dest_urls.insert(camera.to_string(), dest_url.clone());

        // Create upload channel for this camera
        let (upload_tx, upload_rx) = crossbeam_channel::unbounded();

        // Spawn upload handler thread
        let dest_url_clone = dest_url.clone();
        let storage_clone = self.storage.clone();
        let camera_clone = camera.to_string();

        let upload_thread_handle = std::thread::Builder::new()
            .name(format!("upload-{}", camera))
            .spawn(move || {
                Self::upload_thread(camera_clone, dest_url_clone, storage_clone, upload_rx)
            })
            .map_err(|e| RoboflowError::other(format!("Failed to spawn upload thread: {}", e)))?;

        // Create pipeline config
        let encoder_config = FragmentEncoderConfig {
            video: self.config.video_config.clone(),
            temp_dir: self.config.temp_dir.clone(),
            max_frames_per_fragment: self.config.frames_per_fragment,
            camera_id: camera.to_string(),
        };

        let encoder = FragmentEncoder::new(encoder_config)?;

        // Create command channel
        let (cmd_tx, cmd_rx) = crossbeam_channel::bounded(self.config.frame_channel_capacity);

        let camera_clone = camera.to_string();
        let pipeline = CameraPipeline {
            camera: camera_clone,
            cmd_rx,
            upload_tx,
            encoder,
            frame_buffer: Vec::with_capacity(self.config.frames_per_fragment),
            frames_per_fragment: self.config.frames_per_fragment,
            width: 0,
            height: 0,
            frames_encoded: 0,
            frames_skipped: 0,
            fragments_created: 0,
        };

        // Spawn pipeline thread
        let thread_name = format!("camera-pipeline-{}", camera);
        let handle = std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || pipeline.run())
            .map_err(|e| {
                RoboflowError::other(format!("Failed to spawn camera pipeline thread: {}", e))
            })?;

        self.pipelines.insert(
            camera.to_string(),
            CameraPipelineHandle {
                camera: camera.to_string(),
                cmd_tx,
                thread_handle: Some(handle),
                upload_thread_handle: Some(upload_thread_handle),
            },
        );

        Ok(())
    }

    /// Upload thread for a single camera.
    fn upload_thread(
        camera: String,
        dest_url: String,
        storage: Arc<dyn Storage>,
        upload_rx: Receiver<UploadCommand>,
    ) {
        // Downcast to S3Storage to access put_multipart_stream
        let s3_storage = match storage.as_any().downcast_ref::<S3Storage>() {
            Some(s) => s,
            None => {
                tracing::error!(camera = %camera, "Storage is not S3Storage, cannot upload");
                return;
            }
        };

        let mut upload: Option<Box<dyn MultipartUpload>> = None;
        let mut _bytes_uploaded: u64 = 0;
        let mut fragments_count: usize = 0;

        for cmd in upload_rx {
            match cmd {
                UploadCommand::UploadFragment { fragment, .. } => {
                    // Create upload if not exists
                    if upload.is_none() {
                        match s3_storage.put_multipart_stream(Path::new(&dest_url)) {
                            Ok(u) => upload = Some(u),
                            Err(e) => {
                                tracing::error!(camera = %camera, error = %e, "Failed to create multipart upload");
                                continue;
                            }
                        }
                    }

                    if let Some(ref mut u) = upload {
                        match fragment.read_data() {
                            Ok(data) => {
                                if let Err(e) = u.write(&data) {
                                    tracing::error!(camera = %camera, error = %e, "Failed to write fragment");
                                    continue;
                                }
                                _bytes_uploaded += data.len() as u64;
                                fragments_count += 1;
                            }
                            Err(e) => {
                                tracing::error!(camera = %camera, error = %e, "Failed to read fragment data");
                            }
                        }
                    }
                    // FragmentInfo is dropped here, cleaning up temp file
                }
                UploadCommand::Finish { .. } => {
                    if let Some(u) = upload.take() {
                        match u.finish() {
                            Ok(stats) => {
                                tracing::info!(
                                    camera = %camera,
                                    bytes = stats.bytes_uploaded,
                                    fragments = fragments_count,
                                    "Upload completed"
                                );
                            }
                            Err(e) => {
                                tracing::error!(camera = %camera, error = %e, "Failed to finish upload");
                            }
                        }
                    }
                    break;
                }
                UploadCommand::AbortAll => {
                    if let Some(u) = upload.take() {
                        let _ = u.abort();
                    }
                    tracing::warn!(camera = %camera, "Upload aborted");
                    break;
                }
            }
        }
    }

    /// Add a frame for a specific camera.
    ///
    /// This will create a new pipeline for the camera if it doesn't exist.
    ///
    /// # Arguments
    ///
    /// * `camera` - Camera name (e.g., "cam_0", "left", "right")
    /// * `image` - Image data to encode
    pub fn add_frame(&mut self, camera: &str, image: ImageData) -> Result<()> {
        if self.finalized {
            return Err(RoboflowError::other(
                "Cannot add frame to finalized encoder".to_string(),
            ));
        }

        self.ensure_pipeline(camera)?;

        let pipeline = self.pipelines.get(camera).ok_or_else(|| {
            RoboflowError::other(format!("Pipeline not found for camera: {}", camera))
        })?;

        pipeline.add_frame(image)
    }

    /// Finalize encoding and wait for all uploads to complete.
    ///
    /// This signals all pipelines to flush remaining frames,
    /// waits for them to finish, then waits for all uploads to complete.
    ///
    /// # Returns
    ///
    /// List of encoder results per camera.
    pub fn finalize(mut self) -> Result<Vec<ConcurrentEncoderResult>> {
        self.finalized = true;

        // Signal all pipelines to flush
        for (camera, pipeline) in &self.pipelines {
            if let Err(e) = pipeline.flush() {
                tracing::error!(camera = %camera, error = %e, "Failed to flush pipeline");
            }
        }

        // Wait for all pipelines to finish and collect results
        let mut results = Vec::new();
        for (camera, pipeline) in self.pipelines.drain() {
            let dest_url = self.dest_urls.get(&camera).cloned().unwrap_or_default();

            match pipeline.join() {
                Ok(pipeline_result) => {
                    results.push(ConcurrentEncoderResult {
                        camera: pipeline_result.camera,
                        url: dest_url,
                        frames_encoded: pipeline_result.frames_encoded,
                        frames_skipped: pipeline_result.frames_skipped,
                    });
                }
                Err(e) => {
                    tracing::error!(camera = %camera, error = %e, "Pipeline failed");
                    results.push(ConcurrentEncoderResult {
                        camera,
                        url: dest_url,
                        frames_encoded: 0,
                        frames_skipped: 0,
                    });
                }
            }
        }

        // Log summary
        let total_frames: usize = results.iter().map(|r| r.frames_encoded).sum();
        tracing::info!(
            cameras = results.len(),
            total_frames,
            "Concurrent encoding completed"
        );

        Ok(results)
    }

    /// Abort all encoding and uploads.
    pub fn abort(mut self) -> Result<()> {
        self.finalized = true;

        // Signal all pipelines to shutdown
        for (camera, pipeline) in &self.pipelines {
            if let Err(e) = pipeline.shutdown() {
                tracing::warn!(camera = %camera, error = %e, "Failed to shutdown pipeline");
            }
        }

        tracing::warn!("Concurrent encoding aborted");

        Ok(())
    }

    /// Get the list of active cameras.
    pub fn cameras(&self) -> Vec<&str> {
        self.pipelines.keys().map(|s| s.as_str()).collect()
    }

    /// Check if the encoder has been finalized.
    pub fn is_finalized(&self) -> bool {
        self.finalized
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = ConcurrentEncoderConfig::default();
        assert_eq!(config.frames_per_fragment, 300);
        assert_eq!(config.frame_channel_capacity, 64);
    }

    // =============================================================================
    // ConcurrentEncoderConfig Tests
    // =============================================================================

    #[test]
    fn test_config_default_key_prefix() {
        let config = ConcurrentEncoderConfig::default();
        assert!(config.key_prefix.is_empty());
    }

    #[test]
    fn test_config_default_indices() {
        let config = ConcurrentEncoderConfig::default();
        assert_eq!(config.chunk_index, 0);
        assert_eq!(config.episode_index, 0);
    }

    #[test]
    fn test_config_default_video_config() {
        let config = ConcurrentEncoderConfig::default();
        assert_eq!(config.video_config.fps, 30);
        assert_eq!(config.video_config.codec, "libx264");
    }

    #[test]
    fn test_config_custom_values() {
        let config = ConcurrentEncoderConfig {
            key_prefix: "dataset/episode_042".to_string(),
            chunk_index: 1,
            episode_index: 42,
            frames_per_fragment: 150,
            temp_dir: PathBuf::from("/tmp/test"),
            video_config: VideoEncoderConfig::default().with_fps(60),
            frame_channel_capacity: 128,
        };

        assert_eq!(config.key_prefix, "dataset/episode_042");
        assert_eq!(config.chunk_index, 1);
        assert_eq!(config.episode_index, 42);
        assert_eq!(config.frames_per_fragment, 150);
        assert_eq!(config.frame_channel_capacity, 128);
        assert_eq!(config.video_config.fps, 60);
    }

    #[test]
    fn test_config_clone() {
        let config = ConcurrentEncoderConfig {
            key_prefix: "test".to_string(),
            ..Default::default()
        };
        let cloned = config.clone();
        assert_eq!(config.key_prefix, cloned.key_prefix);
        assert_eq!(config.frames_per_fragment, cloned.frames_per_fragment);
    }

    // =============================================================================
    // ConcurrentEncoderResult Tests
    // =============================================================================

    #[test]
    fn test_result_fields() {
        let result = ConcurrentEncoderResult {
            camera: "cam_left".to_string(),
            url: "s3://bucket/videos/cam_left.mp4".to_string(),
            frames_encoded: 1500,
            frames_skipped: 5,
        };

        assert_eq!(result.camera, "cam_left");
        assert_eq!(result.url, "s3://bucket/videos/cam_left.mp4");
        assert_eq!(result.frames_encoded, 1500);
        assert_eq!(result.frames_skipped, 5);
    }

    #[test]
    fn test_result_clone() {
        let result = ConcurrentEncoderResult {
            camera: "cam_right".to_string(),
            url: "test_url".to_string(),
            frames_encoded: 100,
            frames_skipped: 2,
        };
        let cloned = result.clone();
        assert_eq!(result.camera, cloned.camera);
        assert_eq!(result.frames_encoded, cloned.frames_encoded);
    }

    #[test]
    fn test_result_debug() {
        let result = ConcurrentEncoderResult {
            camera: "cam_0".to_string(),
            url: "url".to_string(),
            frames_encoded: 0,
            frames_skipped: 0,
        };
        let debug_str = format!("{:?}", result);
        assert!(debug_str.contains("cam_0"));
        assert!(debug_str.contains("frames_encoded"));
    }

    // =============================================================================
    // PipelineCommand Tests
    // =============================================================================

    #[test]
    fn test_pipeline_command_debug() {
        let cmd = PipelineCommand::Flush;
        let debug_str = format!("{:?}", cmd);
        assert!(debug_str.contains("Flush"));
    }

    // =============================================================================
    // UploadCommand Tests
    // =============================================================================

    #[test]
    fn test_upload_command_abort_debug() {
        let cmd = UploadCommand::AbortAll;
        let debug_str = format!("{:?}", cmd);
        assert!(debug_str.contains("AbortAll"));
    }

    #[test]
    fn test_upload_command_finish_debug() {
        let cmd = UploadCommand::Finish {
            camera: "test".to_string(),
        };
        let debug_str = format!("{:?}", cmd);
        assert!(debug_str.contains("Finish"));
        assert!(debug_str.contains("test"));
    }

    // =============================================================================
    // decode_to_rgb Tests
    // =============================================================================

    #[test]
    fn test_decode_to_rgb_zero_dimensions() {
        let image = ImageData {
            data: vec![0u8; 100],
            width: 0,
            height: 100,
            is_encoded: false,
        };
        assert!(decode_to_rgb(&image).is_none());

        let image2 = ImageData {
            data: vec![0u8; 100],
            width: 100,
            height: 0,
            is_encoded: false,
        };
        assert!(decode_to_rgb(&image2).is_none());
    }

    #[test]
    fn test_decode_to_rgb_raw_rgb() {
        // 2x2 RGB image = 12 bytes
        let image = ImageData {
            data: vec![255u8; 12],
            width: 2,
            height: 2,
            is_encoded: false,
        };
        let result = decode_to_rgb(&image);
        assert!(result.is_some());
        let (w, h, data) = result.unwrap();
        assert_eq!(w, 2);
        assert_eq!(h, 2);
        assert_eq!(data.len(), 12);
    }

    // =============================================================================
    // CameraPipelineResult Tests
    // =============================================================================

    #[test]
    fn test_camera_pipeline_result_fields() {
        let result = CameraPipelineResult {
            camera: "cam_test".to_string(),
            frames_encoded: 500,
            fragments_created: 2,
            frames_skipped: 10,
        };

        assert_eq!(result.camera, "cam_test");
        assert_eq!(result.frames_encoded, 500);
        assert_eq!(result.fragments_created, 2);
        assert_eq!(result.frames_skipped, 10);
    }
}
