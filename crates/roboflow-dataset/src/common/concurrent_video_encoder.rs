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
//! use roboflow_dataset::common::concurrent_video_encoder::{
//!     ConcurrentVideoEncoder, ConcurrentEncoderConfig,
//! };
//!
//! let config = ConcurrentEncoderConfig {
//!     s3_prefix: "s3://bucket/videos".to_string(),
//!     ..Default::default()
//! };
//!
//! let mut encoder = ConcurrentVideoEncoder::new(
//!     config,
//!     storage,
//!     runtime,
//! )?;
//!
//! // Add frames for different cameras
//! encoder.add_frame("cam0", image1)?;
//! encoder.add_frame("cam1", image2)?;
//!
//! // Finalize and get results
//! let results = encoder.finalize()?;
//! for result in results {
//!     println!("{}: {} frames -> {}", result.camera, result.frames_encoded, result.url);
//! }
//! ```

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use roboflow_core::{Result, RoboflowError};
use roboflow_storage::{MultipartUpload, S3Storage, Storage, StorageStreamingExt};
use tokio::runtime::Handle;

use crate::common::ImageData;
use crate::common::camera_pipeline::{
    CameraPipelineConfig, CameraPipelineHandle, spawn_camera_pipeline,
};
use crate::common::video::VideoEncoderConfig;

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

/// Concurrent video encoder for multiple cameras.
///
/// This orchestrates per-camera encoding pipelines. Each pipeline
/// manages its own upload using the `MultipartUpload` trait.
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

        // Create pipeline config
        let pipeline_config = CameraPipelineConfig {
            camera: camera.to_string(),
            frames_per_fragment: self.config.frames_per_fragment,
            temp_dir: self.config.temp_dir.clone(),
            video_config: self.config.video_config.clone(),
        };

        // Create upload channel for this camera
        let (upload_tx, upload_rx) = crossbeam_channel::unbounded();

        // For now, spawn a simple upload handler thread for this camera
        let dest_url_clone = dest_url.clone();
        let storage_clone = self.storage.clone();
        let runtime_clone = self.runtime.clone();
        let camera_clone = camera.to_string();

        std::thread::Builder::new()
            .name(format!("upload-{}", camera))
            .spawn(move || {
                Self::upload_thread(
                    camera_clone,
                    dest_url_clone,
                    storage_clone,
                    runtime_clone,
                    upload_rx,
                )
            })
            .map_err(|e| RoboflowError::other(format!("Failed to spawn upload thread: {}", e)))?;

        // Spawn pipeline
        let handle = spawn_camera_pipeline(pipeline_config, upload_tx)?;

        self.pipelines.insert(camera.to_string(), handle);

        Ok(())
    }

    /// Upload thread for a single camera.
    fn upload_thread(
        camera: String,
        dest_url: String,
        storage: Arc<dyn Storage>,
        _runtime: Handle,
        upload_rx: crossbeam_channel::Receiver<crate::common::fragment_uploader::UploadCommand>,
    ) {
        use crate::common::fragment_uploader::UploadCommand;
        use std::path::Path;

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
}
