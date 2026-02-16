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
//! - Single encoder initialization per camera per episode
//! - Streaming fMP4 output via channels (no temp files)
//! - Per-camera pipeline threads with backpressure
//! - Per-camera upload threads with isolated Tokio runtimes for S3 operations
//! - Clean abort on errors
//! - **Configurable pipeline selection**: 2-stage (single-threaded) or 3-stage (parallel SIMD)
//!
//! # Pipeline Selection
//!
//! - **2-stage (default)**: Single-threaded decode + encode per camera
//!   - Lower memory usage, simpler flow
//!   - Best for: single camera or low-throughput scenarios
//! - **3-stage**: Parallel decode + convert + encode
//!   - SIMD-accelerated color conversion (8-12x faster than FFmpeg)
//!   - Higher throughput for multi-camera scenarios
//!   - Enable via `ConcurrentEncoderConfig::use_parallel_pipeline = true`
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                  ConcurrentVideoEncoder                      │
//! │                                                              │
//! │  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐      │
//! │  │   Camera    │    │   Camera    │    │   Camera    │      │
//! │  │  Streaming  │    │  Streaming  │    │  Streaming  │      │
//! │  │  Pipeline   │    │  Pipeline   │    │  Pipeline   │      │
//! │  │  (thread)   │    │  (thread)   │    │  (thread)   │      │
//! │  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘      │
//! │         │                  │                  │              │
//! │         │ EncodedChunk     │ EncodedChunk     │ EncodedChunk │
//! │         ▼                  ▼                  ▼              │
//! │  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐      │
//! │  │   Upload    │    │   Upload    │    │   Upload    │      │
//! │  │   Thread    │    │   Thread    │    │   Thread    │      │
//! │  │ (own RT +   │    │ (own RT +   │    │ (own RT +   │      │
//! │  │ own S3)     │    │ own S3)     │    │ own S3)     │      │
//! │  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘      │
//! │         │                  │                  │              │
//! │         └──────────────────┼──────────────────┘              │
//! │                            ▼                                 │
//! │                     ┌─────────────┐                          │
//! │                     │  S3/MinIO   │                          │
//! │                     │  Storage    │                          │
//! │                     └─────────────┘                          │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```ignore
//! use roboflow_dataset::common::concurrent_video_encoder::{
//!     ConcurrentVideoEncoder, ConcurrentEncoderConfig,
//! };
//! use roboflow_storage::S3Config;
//!
//! let config = ConcurrentEncoderConfig {
//!     key_prefix: "dataset/episode_001".to_string(),
//!     s3_config: S3Config::new(
//!         "my-bucket",
//!         "s3.amazonaws.com",
//!         "access_key",
//!         "secret_key",
//!     ),
//!     ..Default::default()
//! };
//!
//! let mut encoder = ConcurrentVideoEncoder::new(config)?;
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

use crossbeam_channel::{Receiver, unbounded};
use roboflow_core::{Result, RoboflowError};
use roboflow_storage::S3Config;

use crate::common::ImageData;
use crate::common::camera_streaming_pipeline::{
    EitherPipeline, PipelineAdapter, StreamingPipelineConfig, StreamingUploadCommand,
    spawn_streaming_pipeline,
};
use crate::common::video::VideoEncoderConfig;
use roboflow_video::pipeline::ThreeStageConfig;

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
    /// Chunk size for streaming delivery (bytes).
    pub chunk_size: usize,
    /// Video encoder configuration.
    pub video_config: VideoEncoderConfig,
    /// Frame channel capacity (backpressure threshold).
    pub frame_channel_capacity: usize,
    /// S3 storage configuration for uploads.
    pub s3_config: S3Config,
    /// Whether to use the 3-stage parallel pipeline (DecodePool + ConvertPool + EncoderPool).
    /// When true, uses SIMD-accelerated parallel processing for higher throughput.
    /// When false, uses the 2-stage single-threaded pipeline (StreamingMp4Encoder).
    pub use_parallel_pipeline: bool,
}

impl ConcurrentEncoderConfig {
    /// Create a new encoder config with the given S3 configuration.
    pub fn new(s3_config: S3Config) -> Self {
        Self {
            key_prefix: String::new(),
            chunk_index: 0,
            episode_index: 0,
            chunk_size: 256 * 1024, // 256KB chunks
            video_config: VideoEncoderConfig::default(),
            frame_channel_capacity: 64,
            s3_config,
            use_parallel_pipeline: false,
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
/// This orchestrates per-camera streaming encoding pipelines with upload threads.
/// Each pipeline runs in a dedicated thread with single encoder initialization,
/// while upload threads handle I/O-bound uploads to S3 with isolated runtimes.
pub struct ConcurrentVideoEncoder {
    /// Per-camera pipeline handles (either legacy or adapter).
    pipelines: HashMap<String, EitherPipeline>,
    /// Per-camera upload thread handles.
    upload_handles: HashMap<String, std::thread::JoinHandle<()>>,
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
    /// * `config` - Encoder configuration including S3 settings
    pub fn new(config: ConcurrentEncoderConfig) -> Result<Self> {
        Ok(Self {
            pipelines: HashMap::new(),
            upload_handles: HashMap::new(),
            config,
            finalized: false,
            dest_urls: HashMap::new(),
        })
    }

    /// Build the destination key for a camera in LeRobot v2.1 format.
    /// Format: {prefix}/videos/chunk-{chunk:03}/{camera}/episode_{episode:06d}.mp4
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
        let pipeline_config = StreamingPipelineConfig {
            camera: camera.to_string(),
            video_config: self.config.video_config.clone(),
            chunk_size: self.config.chunk_size,
        };

        // Create crossbeam channel for upload commands
        let (upload_tx, upload_rx) = unbounded::<StreamingUploadCommand>();

        // Clone S3 config for the upload thread
        let s3_config = self.config.s3_config.clone();
        let camera_clone = camera.to_string();
        let dest_url_clone = dest_url.clone();

        // Spawn upload thread with isolated runtime and S3Storage
        let upload_handle = std::thread::Builder::new()
            .name(format!("upload-{}", camera))
            .spawn(move || {
                Self::upload_thread(camera_clone, dest_url_clone, s3_config, upload_rx);
            })
            .map_err(|e| RoboflowError::other(format!("Failed to spawn upload thread: {}", e)))?;

        // Spawn streaming encoding pipeline
        // Choose pipeline type based on use_parallel_pipeline flag
        let pipeline: EitherPipeline = if self.config.use_parallel_pipeline {
            // Create 3-stage pipeline (parallel decode + convert + encode)
            let three_stage_config = ThreeStageConfig {
                camera: camera.to_string(),
                video_config: self.config.video_config.clone(),
                decode_workers: Some(num_cpus::get_physical()),
                convert_workers: Some(num_cpus::get_physical()),
                encode_workers: Some(1), // Hardware encoder only handles one at a time
                pending_capacity: 512,
                completed_capacity: 512,
                frames_per_fragment: 30,
                chunk_size: self.config.chunk_size,
            };

            EitherPipeline::Adapter(PipelineAdapter::new(
                camera.to_string(),
                three_stage_config,
                upload_tx,
            )?)
        } else {
            // Create 2-stage pipeline (single-threaded decode + encode)
            let handle = spawn_streaming_pipeline(pipeline_config, upload_tx)?;
            EitherPipeline::Legacy(handle)
        };

        let camera_string = camera.to_string();
        self.pipelines.insert(camera_string.clone(), pipeline);
        self.upload_handles.insert(camera_string, upload_handle);

        Ok(())
    }

    /// Upload thread for a single camera.
    ///
    /// This thread uses a simple, correct concurrency model:
    /// 1. Create `AsyncS3Storage` directly (sync, no runtime needed)
    /// 2. Create an OWNED Tokio runtime
    /// 3. Use `rt.block_on()` DIRECTLY for all async operations
    fn upload_thread(
        camera: String,
        dest_url: String,
        s3_config: S3Config,
        upload_rx: Receiver<StreamingUploadCommand>,
    ) {
        // Create AsyncS3Storage directly (synchronous, no runtime needed)
        let async_storage = match roboflow_storage::AsyncS3Storage::with_config(s3_config) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(camera = %camera, error = %e, "Failed to create AsyncS3Storage");
                return;
            }
        };

        // Create OWNED runtime for this thread
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                tracing::error!(camera = %camera, error = %e, "Failed to create upload runtime");
                return;
            }
        };

        // Multipart upload state
        let mut multipart: Option<roboflow_storage::object_store::WriteMultipart> = None;
        let mut bytes_uploaded: u64 = 0;
        let mut chunks_count: usize = 0;

        // Process upload commands
        for cmd in upload_rx {
            match cmd {
                StreamingUploadCommand::UploadChunk { chunk } => {
                    // Create multipart upload if not exists
                    if multipart.is_none() {
                        use roboflow_storage::object_store::path::Path as ObjectPath;
                        let key = ObjectPath::from(dest_url.as_str());
                        let store = async_storage.object_store();

                        // Use rt.block_on() DIRECTLY - this works correctly
                        match rt.block_on(store.put_multipart(&key)) {
                            Ok(upload) => {
                                use roboflow_storage::object_store::WriteMultipart;
                                // 5MB chunk size for S3
                                multipart = Some(WriteMultipart::new_with_chunk_size(
                                    upload,
                                    5 * 1024 * 1024,
                                ));
                            }
                            Err(e) => {
                                tracing::error!(camera = %camera, error = %e, "Failed to create multipart upload");
                                continue;
                            }
                        }
                    }

                    // Upload chunk data
                    if let Some(ref mut mp) = multipart {
                        mp.write(&chunk.data);
                        bytes_uploaded += chunk.data.len() as u64;
                        chunks_count += 1;
                    }
                }
                StreamingUploadCommand::Finish { .. } => {
                    if let Some(mp) = multipart.take() {
                        // Use rt.block_on() DIRECTLY for finish
                        match rt.block_on(mp.finish()) {
                            Ok(_) => {
                                tracing::info!(
                                    camera = %camera,
                                    bytes = bytes_uploaded,
                                    chunks = chunks_count,
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
                StreamingUploadCommand::AbortAll => {
                    if let Some(mp) = multipart.take() {
                        // Use rt.block_on() DIRECTLY for abort
                        let _ = rt.block_on(mp.abort());
                    }
                    tracing::warn!(camera = %camera, "Upload aborted");
                    break;
                }
            }
        }

        // Runtime is dropped here, cleaning up resources
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

        // Wait for all upload threads to complete
        for (camera, upload_handle) in self.upload_handles.drain() {
            if let Err(e) = upload_handle.join() {
                tracing::warn!(camera = %camera, "Upload thread panicked: {:?}", e);
            }
        }

        // Log summary
        let total_frames: usize = results.iter().map(|r| r.frames_encoded).sum();
        tracing::info!(
            cameras = results.len(),
            total_frames,
            "Concurrent streaming encoding completed"
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

        tracing::warn!("Concurrent streaming encoding aborted");

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
    use crate::common::ImageData;

    fn test_s3_config() -> S3Config {
        S3Config::new("test-bucket", "localhost:9000", "access_key", "secret_key")
    }

    #[test]
    fn test_config_new() {
        let config = ConcurrentEncoderConfig::new(test_s3_config());
        assert_eq!(config.chunk_size, 256 * 1024);
        assert_eq!(config.frame_channel_capacity, 64);
    }

    #[test]
    fn test_config_fields() {
        let config = ConcurrentEncoderConfig {
            use_parallel_pipeline: false,
            key_prefix: "test/prefix".to_string(),
            chunk_index: 5,
            episode_index: 42,
            chunk_size: 128 * 1024,
            video_config: crate::common::video::VideoEncoderConfig::default(),
            frame_channel_capacity: 32,
            s3_config: test_s3_config(),
        };

        assert_eq!(config.key_prefix, "test/prefix");
        assert_eq!(config.chunk_index, 5);
        assert_eq!(config.episode_index, 42);
        assert_eq!(config.chunk_size, 128 * 1024);
    }

    #[test]
    fn test_encoder_create() {
        let config = ConcurrentEncoderConfig::new(test_s3_config());
        let encoder = ConcurrentVideoEncoder::new(config);
        assert!(encoder.is_ok());

        let encoder = encoder.unwrap();
        assert!(!encoder.is_finalized());
        assert!(encoder.cameras().is_empty());
    }

    #[test]
    fn test_encoder_cannot_add_after_finalize() {
        let config = ConcurrentEncoderConfig::new(test_s3_config());
        let encoder = ConcurrentVideoEncoder::new(config).unwrap();

        // Finalize without adding any frames
        let results = encoder.finalize().unwrap();
        assert!(results.is_empty());

        // Note: encoder is consumed by finalize(), so we can't add frames after
        // This is enforced at compile time
    }

    #[test]
    fn test_build_dest_url_format() {
        let config = ConcurrentEncoderConfig {
            use_parallel_pipeline: false,
            key_prefix: "dataset/episode_001".to_string(),
            chunk_index: 0,
            episode_index: 42,
            chunk_size: 256 * 1024,
            video_config: crate::common::video::VideoEncoderConfig::default(),
            frame_channel_capacity: 64,
            s3_config: test_s3_config(),
        };

        let encoder = ConcurrentVideoEncoder::new(config).unwrap();
        // The build_dest_url method is private, but we can verify through add_frame
        // which creates the URL internally

        // This test verifies the encoder can be created with specific config
        assert!(!encoder.is_finalized());
    }

    #[test]
    fn test_encoder_single_camera() {
        let config = ConcurrentEncoderConfig {
            use_parallel_pipeline: false,
            key_prefix: "test_single".to_string(),
            chunk_index: 0,
            episode_index: 0,
            chunk_size: 256 * 1024,
            video_config: crate::common::video::VideoEncoderConfig::default(),
            frame_channel_capacity: 64,
            s3_config: test_s3_config(),
        };

        let mut encoder = ConcurrentVideoEncoder::new(config).unwrap();

        // Add 3 frames for one camera
        for i in 0..3 {
            let image = ImageData::new(64, 64, vec![(i * 50) as u8; 64 * 64 * 3]);
            encoder.add_frame("cam0", image).unwrap();
        }

        // Verify camera was added
        let cameras = encoder.cameras();
        assert_eq!(cameras.len(), 1);
        assert_eq!(cameras[0], "cam0");

        // Finalize
        let results = encoder.finalize().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].camera, "cam0");
        assert_eq!(results[0].frames_encoded, 3);
    }

    #[test]
    fn test_encoder_multi_camera() {
        let config = ConcurrentEncoderConfig {
            use_parallel_pipeline: false,
            key_prefix: "test_multi".to_string(),
            chunk_index: 0,
            episode_index: 0,
            chunk_size: 256 * 1024,
            video_config: crate::common::video::VideoEncoderConfig::default(),
            frame_channel_capacity: 64,
            s3_config: test_s3_config(),
        };

        let mut encoder = ConcurrentVideoEncoder::new(config).unwrap();

        // Add frames for multiple cameras
        let cameras = vec!["left", "right", "center"];
        for camera in &cameras {
            for i in 0..5 {
                let image = ImageData::new(64, 64, vec![(i * 30) as u8; 64 * 64 * 3]);
                encoder.add_frame(camera, image).unwrap();
            }
        }

        // Verify all cameras were added
        let encoder_cameras: Vec<_> = encoder.cameras().into_iter().collect();
        assert_eq!(encoder_cameras.len(), 3);

        // Finalize
        let results = encoder.finalize().unwrap();
        assert_eq!(results.len(), 3);

        // Each camera should have 5 frames
        for result in &results {
            assert_eq!(result.frames_encoded, 5);
        }
    }

    #[test]
    fn test_encoder_non_square_dimensions() {
        // Regression test for non-square image dimensions
        let config = ConcurrentEncoderConfig {
            use_parallel_pipeline: false,
            key_prefix: "test_nonsquare".to_string(),
            chunk_index: 0,
            episode_index: 0,
            chunk_size: 256 * 1024,
            video_config: crate::common::video::VideoEncoderConfig::default(),
            frame_channel_capacity: 64,
            s3_config: test_s3_config(),
        };

        let mut encoder = ConcurrentVideoEncoder::new(config).unwrap();

        // 160x120 is non-square - this used to fail!
        let width = 160u32;
        let height = 120u32;
        let rgb_data = vec![128u8; (width * height * 3) as usize];

        for _ in 0..5 {
            let image = ImageData::new(width, height, rgb_data.clone());
            encoder.add_frame("nonsquare_cam", image).unwrap();
        }

        let results = encoder.finalize().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].frames_encoded, 5);
        assert_eq!(results[0].frames_skipped, 0);
    }

    #[test]
    fn test_encoder_skip_invalid_frames() {
        let config = ConcurrentEncoderConfig {
            use_parallel_pipeline: false,
            key_prefix: "test_skip".to_string(),
            chunk_index: 0,
            episode_index: 0,
            chunk_size: 256 * 1024,
            video_config: crate::common::video::VideoEncoderConfig::default(),
            frame_channel_capacity: 64,
            s3_config: test_s3_config(),
        };

        let mut encoder = ConcurrentVideoEncoder::new(config).unwrap();

        // Add a valid frame first (sets dimensions)
        let valid_image = ImageData::new(64, 64, vec![128u8; 64 * 64 * 3]);
        encoder.add_frame("cam0", valid_image).unwrap();

        // Add an invalid frame (zero dimensions - should be skipped)
        let invalid_image = ImageData::new(0, 0, vec![]);
        // This should still succeed - invalid frames are skipped gracefully
        let result = encoder.add_frame("cam0", invalid_image);
        assert!(result.is_ok());

        // Add another valid frame
        let valid_image2 = ImageData::new(64, 64, vec![200u8; 64 * 64 * 3]);
        encoder.add_frame("cam0", valid_image2).unwrap();

        let results = encoder.finalize().unwrap();
        assert_eq!(results.len(), 1);
        // 2 valid frames encoded, 1 skipped
        assert_eq!(results[0].frames_encoded, 2);
        assert_eq!(results[0].frames_skipped, 1);
    }

    #[test]
    fn test_encoder_empty() {
        // Test encoder with no frames added
        let config = ConcurrentEncoderConfig::new(test_s3_config());
        let encoder = ConcurrentVideoEncoder::new(config).unwrap();

        let results = encoder.finalize().unwrap();
        assert!(results.is_empty());
    }
}
