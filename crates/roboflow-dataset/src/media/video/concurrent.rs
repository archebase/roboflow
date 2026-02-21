// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Concurrent video encoder for multi-camera streaming.
//!
//! This module provides `ConcurrentVideoEncoder` which orchestrates
//! video encoding for multiple cameras with local file output.
//!
//! # Design
//!
//! - Single encoder initialization per camera per episode
//! - Streaming fMP4 output via channels (no temp files)
//! - Per-camera pipeline threads with backpressure
//! - Per-camera file writer threads for local file I/O
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
//! │  │   File      │    │   File      │    │   File      │      │
//! │  │   Writer    │    │   Writer    │    │   Writer    │      │
//! │  │   Thread    │    │   Thread    │    │   Thread    │      │
//! │  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘      │
//! │         │                  │                  │              │
//! │         └──────────────────┼──────────────────┘              │
//! │                            ▼                                 │
//! │                     ┌─────────────┐                          │
//! │                     │   Local     │                          │
//! │                     │   Files     │                          │
//! │                     └─────────────┘                          │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```ignore
//! use roboflow_dataset::media::video::{
//!     ConcurrentVideoEncoder, ConcurrentEncoderConfig,
//! };
//! use std::path::PathBuf;
//!
//! let config = ConcurrentEncoderConfig {
//!     key_prefix: "dataset/episode_001".to_string(),
//!     output_dir: PathBuf::from("./output"),
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
//!     println!("{}: {} frames -> {}", result.camera, result.frames_encoded, result.output_path.display());
//! }
//! ```

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crossbeam_channel::{Receiver, unbounded};
use roboflow_core::{Result, RoboflowError};

use crate::core::VideoPathScheme;
use crate::formats::common::{ImageData, VideoEncoderConfig};
use crate::media::video::pipeline::VideoPipelineConfig;
use super::camera_streaming_pipeline::{
    EitherPipeline, PipelineAdapter, StreamingPipelineConfig, StreamingUploadCommand,
    spawn_streaming_pipeline,
};

/// Configuration for concurrent video encoder.
#[derive(Clone)]
pub struct ConcurrentEncoderConfig {
    /// Key prefix for output videos within the output directory.
    /// This should be a relative path (e.g., "dataset/episode_001").
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
    /// Output directory for video files.
    pub output_dir: PathBuf,
    /// Whether to use the 3-stage parallel pipeline (DecodePool + ConvertPool + EncoderPool).
    /// When true, uses SIMD-accelerated parallel processing for higher throughput.
    /// When false, uses the 2-stage single-threaded pipeline (StreamingMp4Encoder).
    pub use_parallel_pipeline: bool,
    /// Optional video path scheme for generating output paths.
    /// If None, uses the default LeRobot v2.1 format.
    pub path_scheme: Option<Arc<dyn VideoPathScheme>>,
}

impl ConcurrentEncoderConfig {
    /// Create a new encoder config with the given output directory.
    pub fn new(output_dir: PathBuf) -> Self {
        Self {
            key_prefix: String::new(),
            chunk_index: 0,
            episode_index: 0,
            chunk_size: 256 * 1024, // 256KB chunks
            video_config: VideoEncoderConfig::default(),
            frame_channel_capacity: 64,
            output_dir,
            use_parallel_pipeline: false,
            path_scheme: None,
        }
    }

    /// Set the video path scheme.
    pub fn with_path_scheme(mut self, scheme: Arc<dyn VideoPathScheme>) -> Self {
        self.path_scheme = Some(scheme);
        self
    }
}

/// Result from the concurrent encoder.
#[derive(Debug, Clone)]
pub struct ConcurrentEncoderResult {
    /// Camera name.
    pub camera: String,
    /// Output file path.
    pub output_path: PathBuf,
    /// Frames encoded.
    pub frames_encoded: usize,
    /// Frames skipped.
    pub frames_skipped: usize,
}

/// Concurrent video encoder for multiple cameras.
///
/// This orchestrates per-camera streaming encoding pipelines with file writer threads.
/// Each pipeline runs in a dedicated thread with single encoder initialization,
/// while writer threads handle I/O-bound local file writes.
pub struct ConcurrentVideoEncoder {
    /// Per-camera pipeline handles (either legacy or adapter).
    pipelines: HashMap<String, EitherPipeline>,
    /// Per-camera file writer thread handles.
    writer_handles: HashMap<String, std::thread::JoinHandle<()>>,
    /// Configuration.
    config: ConcurrentEncoderConfig,
    /// Whether the encoder has been finalized.
    finalized: bool,
    /// Output paths per camera.
    output_paths: HashMap<String, PathBuf>,
}

impl ConcurrentVideoEncoder {
    /// Create a new concurrent video encoder.
    ///
    /// # Arguments
    ///
    /// * `config` - Encoder configuration including output directory
    pub fn new(config: ConcurrentEncoderConfig) -> Result<Self> {
        Ok(Self {
            pipelines: HashMap::new(),
            writer_handles: HashMap::new(),
            config,
            finalized: false,
            output_paths: HashMap::new(),
        })
    }

    /// Build the output path for a camera using the configured path scheme.
    /// If no path scheme is configured, uses the default LeRobot v2.1 format.
    fn build_output_path(&self, camera: &str) -> PathBuf {
        let relative_path = if let Some(ref scheme) = self.config.path_scheme {
            // Use the configured path scheme
            scheme.video_path(
                self.config.episode_index as usize,
                camera,
                self.config.chunk_index as usize,
            )
        } else {
            // Default LeRobot v2.1 format
            let prefix = self.config.key_prefix.trim_end_matches('/');
            PathBuf::from(format!(
                "{}/videos/chunk-{:03}/{}/episode_{:06}.mp4",
                prefix, self.config.chunk_index, camera, self.config.episode_index
            ))
        };

        self.config.output_dir.join(relative_path)
    }

    /// Ensure a pipeline exists for the given camera.
    fn ensure_pipeline(&mut self, camera: &str) -> Result<()> {
        if self.pipelines.contains_key(camera) {
            return Ok(());
        }

        // Build output path
        let output_path = self.build_output_path(camera);
        self.output_paths
            .insert(camera.to_string(), output_path.clone());

        // Create pipeline config
        let pipeline_config = StreamingPipelineConfig {
            camera: camera.to_string(),
            video_config: self.config.video_config.clone(),
            chunk_size: self.config.chunk_size,
        };

        // Create crossbeam channel for writer commands
        let (writer_tx, writer_rx) = unbounded::<StreamingUploadCommand>();

        // Clone output path for the writer thread
        let camera_clone = camera.to_string();
        let output_path_clone = output_path.clone();

        // Spawn file writer thread
        let writer_handle = std::thread::Builder::new()
            .name(format!("file-writer-{}", camera))
            .spawn(move || {
                Self::file_writer_thread(camera_clone, output_path_clone, writer_rx);
            })
            .map_err(|e| {
                RoboflowError::other(format!("Failed to spawn file writer thread: {}", e))
            })?;

        // Spawn streaming encoding pipeline
        // Choose pipeline type based on use_parallel_pipeline flag
        let pipeline: EitherPipeline = if self.config.use_parallel_pipeline {
            // Create parallel video pipeline (decode + convert + encode)
            let video_pipeline_config = VideoPipelineConfig {
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
                video_pipeline_config,
                writer_tx,
            )?)
        } else {
            // Create 2-stage pipeline (single-threaded decode + encode)
            let handle = spawn_streaming_pipeline(pipeline_config, writer_tx)?;
            EitherPipeline::Legacy(handle)
        };

        let camera_string = camera.to_string();
        self.pipelines.insert(camera_string.clone(), pipeline);
        self.writer_handles.insert(camera_string, writer_handle);

        Ok(())
    }

    /// File writer thread for a single camera.
    ///
    /// This thread receives encoded chunks and writes them to a local file.
    fn file_writer_thread(
        camera: String,
        output_path: PathBuf,
        writer_rx: Receiver<StreamingUploadCommand>,
    ) {
        // Ensure parent directory exists
        if let Some(parent) = output_path.parent()
            && let Err(e) = std::fs::create_dir_all(parent)
        {
            tracing::error!(camera = %camera, error = %e, "Failed to create output directory");
            return;
        }

        // Output file
        let mut file: Option<std::fs::File> = None;
        let mut bytes_written: u64 = 0;
        let mut chunks_count: usize = 0;

        // Process writer commands
        for cmd in writer_rx {
            match cmd {
                StreamingUploadCommand::UploadChunk { chunk } => {
                    // Create file if not exists
                    if file.is_none() {
                        match std::fs::File::create(&output_path) {
                            Ok(f) => file = Some(f),
                            Err(e) => {
                                tracing::error!(camera = %camera, error = %e, "Failed to create output file");
                                continue;
                            }
                        }
                    }

                    // Write chunk data
                    if let Some(ref mut f) = file {
                        use std::io::Write;
                        match f.write_all(&chunk.data) {
                            Ok(_) => {
                                bytes_written += chunk.data.len() as u64;
                                chunks_count += 1;
                            }
                            Err(e) => {
                                tracing::error!(camera = %camera, error = %e, "Failed to write chunk");
                            }
                        }
                    }
                }
                StreamingUploadCommand::Finish { .. } => {
                    // Flush and close file
                    if let Some(mut f) = file.take() {
                        use std::io::Write;
                        if let Err(e) = f.flush() {
                            tracing::error!(camera = %camera, error = %e, "Failed to flush file");
                        }
                    }

                    tracing::info!(
                        camera = %camera,
                        path = %output_path.display(),
                        bytes = bytes_written,
                        chunks = chunks_count,
                        "File write completed"
                    );
                    break;
                }
                StreamingUploadCommand::AbortAll => {
                    // Delete partial file if exists
                    drop(file.take());
                    let _ = std::fs::remove_file(&output_path);
                    tracing::warn!(camera = %camera, "File write aborted");
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

    /// Finalize encoding and wait for all file writes to complete.
    ///
    /// This signals all pipelines to flush remaining frames,
    /// waits for them to finish, then waits for all file writes to complete.
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
            let output_path = self.output_paths.get(&camera).cloned().unwrap_or_default();

            match pipeline.join() {
                Ok(pipeline_result) => {
                    results.push(ConcurrentEncoderResult {
                        camera: pipeline_result.camera,
                        output_path,
                        frames_encoded: pipeline_result.frames_encoded,
                        frames_skipped: pipeline_result.frames_skipped,
                    });
                }
                Err(e) => {
                    tracing::error!(camera = %camera, error = %e, "Pipeline failed");
                    results.push(ConcurrentEncoderResult {
                        camera,
                        output_path,
                        frames_encoded: 0,
                        frames_skipped: 0,
                    });
                }
            }
        }

        // Wait for all writer threads to complete
        for (camera, writer_handle) in self.writer_handles.drain() {
            if let Err(e) = writer_handle.join() {
                tracing::warn!(camera = %camera, "File writer thread panicked: {:?}", e);
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
    use tempfile::tempdir;

    fn test_output_dir() -> PathBuf {
        tempdir().unwrap().path().to_path_buf()
    }

    #[test]
    fn test_config_new() {
        let config = ConcurrentEncoderConfig::new(test_output_dir());
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
            video_config: VideoEncoderConfig::default(),
            frame_channel_capacity: 32,
            output_dir: test_output_dir(),
            path_scheme: None,
        };

        assert_eq!(config.key_prefix, "test/prefix");
        assert_eq!(config.chunk_index, 5);
        assert_eq!(config.episode_index, 42);
        assert_eq!(config.chunk_size, 128 * 1024);
    }

    #[test]
    fn test_encoder_create() {
        let config = ConcurrentEncoderConfig::new(test_output_dir());
        let encoder = ConcurrentVideoEncoder::new(config);
        assert!(encoder.is_ok());

        let encoder = encoder.unwrap();
        assert!(!encoder.is_finalized());
        assert!(encoder.cameras().is_empty());
    }

    #[test]
    fn test_encoder_cannot_add_after_finalize() {
        let config = ConcurrentEncoderConfig::new(test_output_dir());
        let encoder = ConcurrentVideoEncoder::new(config).unwrap();

        // Finalize without adding any frames
        let results = encoder.finalize().unwrap();
        assert!(results.is_empty());

        // Note: encoder is consumed by finalize(), so we can't add frames after
        // This is enforced at compile time
    }

    #[test]
    fn test_build_output_path_format() {
        let config = ConcurrentEncoderConfig {
            use_parallel_pipeline: false,
            key_prefix: "dataset/episode_001".to_string(),
            chunk_index: 0,
            episode_index: 42,
            chunk_size: 256 * 1024,
            video_config: VideoEncoderConfig::default(),
            frame_channel_capacity: 64,
            output_dir: test_output_dir(),
            path_scheme: None,
        };

        let encoder = ConcurrentVideoEncoder::new(config).unwrap();
        // The build_output_path method is private, but we can verify through add_frame
        // which creates the path internally

        // This test verifies the encoder can be created with specific config
        assert!(!encoder.is_finalized());
    }

    #[test]
    fn test_encoder_single_camera() {
        let output_dir = test_output_dir();
        let config = ConcurrentEncoderConfig {
            use_parallel_pipeline: false,
            key_prefix: "test_single".to_string(),
            chunk_index: 0,
            episode_index: 0,
            chunk_size: 256 * 1024,
            video_config: VideoEncoderConfig::default(),
            frame_channel_capacity: 64,
            output_dir: output_dir.clone(),
            path_scheme: None,
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

        // Verify output file was created
        assert!(results[0].output_path.exists());
    }

    #[test]
    fn test_encoder_multi_camera() {
        let output_dir = test_output_dir();
        let config = ConcurrentEncoderConfig {
            use_parallel_pipeline: false,
            key_prefix: "test_multi".to_string(),
            chunk_index: 0,
            episode_index: 0,
            chunk_size: 256 * 1024,
            video_config: VideoEncoderConfig::default(),
            frame_channel_capacity: 64,
            output_dir: output_dir.clone(),
            path_scheme: None,
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
            // Verify output file was created
            assert!(result.output_path.exists());
        }
    }

    #[test]
    fn test_encoder_non_square_dimensions() {
        // Regression test for non-square image dimensions
        let output_dir = test_output_dir();
        let config = ConcurrentEncoderConfig {
            use_parallel_pipeline: false,
            key_prefix: "test_nonsquare".to_string(),
            chunk_index: 0,
            episode_index: 0,
            chunk_size: 256 * 1024,
            video_config: VideoEncoderConfig::default(),
            frame_channel_capacity: 64,
            output_dir: output_dir.clone(),
            path_scheme: None,
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
        assert!(results[0].output_path.exists());
    }

    #[test]
    fn test_encoder_skip_invalid_frames() {
        let output_dir = test_output_dir();
        let config = ConcurrentEncoderConfig {
            use_parallel_pipeline: false,
            key_prefix: "test_skip".to_string(),
            chunk_index: 0,
            episode_index: 0,
            chunk_size: 256 * 1024,
            video_config: VideoEncoderConfig::default(),
            frame_channel_capacity: 64,
            output_dir: output_dir.clone(),
            path_scheme: None,
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
        let config = ConcurrentEncoderConfig::new(test_output_dir());
        let encoder = ConcurrentVideoEncoder::new(config).unwrap();

        let results = encoder.finalize().unwrap();
        assert!(results.is_empty());
    }
}
