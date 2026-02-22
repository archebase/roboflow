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
//! - Uses `VideoEncoder` for all encoding
//! - Per-camera encoding threads
//! - Direct file output (no separate writer threads)
//! - Clean abort on errors
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                  ConcurrentVideoEncoder                      │
//! │                                                              │
//! │  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐      │
//! │  │   Camera    │    │   Camera    │    │   Camera    │      │
//! │  │  Encoding   │    │  Encoding   │    │  Encoding   │      │
//! │  │  Thread     │    │  Thread     │    │  Thread     │      │
//! │  │ (VideoEncoder)│  │ (VideoEncoder)│  │ (VideoEncoder)│    │
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

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crossbeam_channel::{Receiver, Sender, unbounded};
use roboflow_core::{Result, RoboflowError};

use super::{OutputConfig, VideoEncoder, VideoEncoderConfig};
use crate::core::VideoPathScheme;
use crate::formats::common::{ImageData, decode_to_rgb};

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
    /// Video encoder configuration.
    pub video_config: VideoEncoderConfig,
    /// Output directory for video files.
    pub output_dir: PathBuf,
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
            video_config: VideoEncoderConfig::default(),
            output_dir,
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

// =============================================================================
// Internal Commands
// =============================================================================

/// Command for encoding threads.
enum EncodeCommand {
    /// Add a frame to encode.
    Frame(ImageData),
    /// Flush and finish encoding.
    Flush,
    /// Abort immediately.
    Abort,
}

// =============================================================================
// Concurrent Video Encoder
// =============================================================================

/// Concurrent video encoder for multiple cameras.
///
/// This orchestrates per-camera encoding threads that use `VideoEncoder`
/// for local file output.
pub struct ConcurrentVideoEncoder {
    /// Per-camera command senders.
    cmd_txs: HashMap<String, Sender<EncodeCommand>>,
    /// Per-camera thread handles.
    thread_handles: HashMap<String, std::thread::JoinHandle<Result<ConcurrentEncoderResult>>>,
    /// Configuration.
    config: ConcurrentEncoderConfig,
    /// Whether the encoder has been finalized.
    finalized: bool,
}

impl ConcurrentVideoEncoder {
    /// Create a new concurrent video encoder.
    ///
    /// # Arguments
    ///
    /// * `config` - Encoder configuration including output directory
    pub fn new(config: ConcurrentEncoderConfig) -> Result<Self> {
        Ok(Self {
            cmd_txs: HashMap::new(),
            thread_handles: HashMap::new(),
            config,
            finalized: false,
        })
    }

    /// Build the output path for a camera using the configured path scheme.
    fn build_output_path(&self, camera: &str) -> PathBuf {
        let relative_path = if let Some(ref scheme) = self.config.path_scheme {
            scheme.video_path(
                self.config.episode_index as usize,
                camera,
                self.config.chunk_index as usize,
            )
        } else {
            let prefix = self.config.key_prefix.trim_end_matches('/');
            PathBuf::from(format!(
                "{}/videos/chunk-{:03}/{}/episode_{:06}.mp4",
                prefix, self.config.chunk_index, camera, self.config.episode_index
            ))
        };

        self.config.output_dir.join(relative_path)
    }

    /// Ensure an encoding thread exists for the given camera.
    fn ensure_camera(&mut self, camera: &str) -> Result<()> {
        if self.cmd_txs.contains_key(camera) {
            return Ok(());
        }

        // Build output path
        let output_path = self.build_output_path(camera);

        // Create parent directory
        if let Some(parent) = output_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                RoboflowError::other(format!(
                    "Failed to create directory '{}': {}",
                    parent.display(),
                    e
                ))
            })?;
        }

        // Create command channel
        let (cmd_tx, cmd_rx) = unbounded();

        // Clone config for the thread
        let video_config = self.config.video_config.clone();
        let camera_name = camera.to_string();
        let output_path_clone = output_path.clone();

        // Spawn encoding thread
        let handle = std::thread::Builder::new()
            .name(format!("encoder-{}", camera))
            .spawn(move || {
                Self::encoding_thread(camera_name, output_path_clone, video_config, cmd_rx)
            })
            .map_err(|e| RoboflowError::other(format!("Failed to spawn encoder thread: {}", e)))?;

        self.cmd_txs.insert(camera.to_string(), cmd_tx);
        self.thread_handles.insert(camera.to_string(), handle);

        Ok(())
    }

    /// Encoding thread for a single camera.
    fn encoding_thread(
        camera: String,
        output_path: PathBuf,
        video_config: VideoEncoderConfig,
        cmd_rx: Receiver<EncodeCommand>,
    ) -> Result<ConcurrentEncoderResult> {
        let mut frames_encoded = 0usize;
        let mut frames_skipped = 0usize;

        // Video dimensions (set from first frame)
        let mut width = 0u32;
        let mut height = 0u32;

        // Encoder (lazy initialization)
        let mut encoder: Option<VideoEncoder> = None;

        // Process commands
        while let Ok(cmd) = cmd_rx.recv() {
            match cmd {
                EncodeCommand::Frame(image) => {
                    // Skip invalid frames
                    if !image.is_encoded && (image.width == 0 || image.height == 0) {
                        frames_skipped += 1;
                        continue;
                    }

                    // Decode to RGB
                    let (decoded_w, decoded_h, rgb_data) = match decode_to_rgb(&image) {
                        Some(data) => data,
                        None => {
                            frames_skipped += 1;
                            continue;
                        }
                    };

                    // Initialize encoder on first frame
                    if width == 0 {
                        width = decoded_w;
                        height = decoded_h;

                        let output = OutputConfig::file(&output_path);
                        match VideoEncoder::new(video_config.clone(), output) {
                            Ok(enc) => {
                                encoder = Some(enc);
                                tracing::info!(
                                    camera = %camera,
                                    width = decoded_w,
                                    height = decoded_h,
                                    path = %output_path.display(),
                                    "Encoder initialized"
                                );
                            }
                            Err(e) => {
                                tracing::error!(
                                    camera = %camera,
                                    error = %e,
                                    "Failed to initialize encoder"
                                );
                                return Err(e);
                            }
                        }
                    }

                    // Validate dimensions
                    if decoded_w != width || decoded_h != height {
                        tracing::debug!(
                            camera = %camera,
                            expected = format!("{}x{}", width, height),
                            actual = format!("{}x{}", decoded_w, decoded_h),
                            "Skipping frame due to dimension mismatch"
                        );
                        frames_skipped += 1;
                        continue;
                    }

                    // Encode frame
                    if let Some(ref mut enc) = encoder {
                        match enc.encode_frame(&rgb_data, decoded_w, decoded_h) {
                            Ok(_) => {
                                frames_encoded += 1;
                            }
                            Err(e) => {
                                tracing::warn!(
                                    camera = %camera,
                                    error = %e,
                                    "Failed to encode frame, skipping"
                                );
                                frames_skipped += 1;
                            }
                        }
                    }
                }
                EncodeCommand::Flush => {
                    break;
                }
                EncodeCommand::Abort => {
                    tracing::warn!(camera = %camera, "Encoding aborted");
                    return Err(RoboflowError::other(format!(
                        "Camera {} encoding aborted",
                        camera
                    )));
                }
            }
        }

        // Finalize encoder
        if let Some(enc) = encoder {
            match enc.finalize() {
                Ok(result) => {
                    tracing::info!(
                        camera = %camera,
                        frames = frames_encoded,
                        bytes = result.bytes_written,
                        path = %output_path.display(),
                        "Encoding complete"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        camera = %camera,
                        error = %e,
                        "Failed to finalize encoder"
                    );
                    return Err(e);
                }
            }
        }

        Ok(ConcurrentEncoderResult {
            camera,
            output_path,
            frames_encoded,
            frames_skipped,
        })
    }

    /// Add a frame for a specific camera.
    ///
    /// This will create a new encoder for the camera if it doesn't exist.
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

        self.ensure_camera(camera)?;

        let cmd_tx = self
            .cmd_txs
            .get(camera)
            .ok_or_else(|| RoboflowError::other(format!("No encoder for camera: {}", camera)))?;

        cmd_tx
            .send(EncodeCommand::Frame(image))
            .map_err(|e| RoboflowError::other(format!("Failed to send frame to encoder: {}", e)))
    }

    /// Finalize encoding and wait for all encoders to complete.
    ///
    /// # Returns
    ///
    /// List of encoder results per camera.
    pub fn finalize(mut self) -> Result<Vec<ConcurrentEncoderResult>> {
        self.finalized = true;

        // Signal all encoders to flush
        for (camera, cmd_tx) in &self.cmd_txs {
            if let Err(e) = cmd_tx.send(EncodeCommand::Flush) {
                tracing::warn!(camera = %camera, error = %e, "Failed to send flush command");
            }
        }

        // Wait for all threads to complete
        let mut results = Vec::new();

        // First, build all output paths (before draining)
        let output_paths: HashMap<String, PathBuf> = self
            .thread_handles
            .keys()
            .map(|camera| (camera.clone(), self.build_output_path(camera)))
            .collect();

        for (camera, handle) in self.thread_handles.drain() {
            let output_path = output_paths.get(&camera).cloned().unwrap_or_default();
            match handle.join() {
                Ok(Ok(result)) => {
                    results.push(result);
                }
                Ok(Err(e)) => {
                    tracing::error!(camera = %camera, error = %e, "Encoding thread failed");
                    results.push(ConcurrentEncoderResult {
                        camera,
                        output_path,
                        frames_encoded: 0,
                        frames_skipped: 0,
                    });
                }
                Err(e) => {
                    tracing::error!(camera = %camera, "Encoding thread panicked: {:?}", e);
                    results.push(ConcurrentEncoderResult {
                        camera,
                        output_path,
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

    /// Abort all encoding.
    pub fn abort(mut self) -> Result<()> {
        self.finalized = true;

        // Signal all encoders to abort
        for (camera, cmd_tx) in &self.cmd_txs {
            if let Err(e) = cmd_tx.send(EncodeCommand::Abort) {
                tracing::warn!(camera = %camera, error = %e, "Failed to send abort command");
            }
        }

        tracing::warn!("Concurrent encoding aborted");

        Ok(())
    }

    /// Get the list of active cameras.
    pub fn cameras(&self) -> Vec<&str> {
        self.cmd_txs.keys().map(|s| s.as_str()).collect()
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
        assert!(config.key_prefix.is_empty());
        assert_eq!(config.chunk_index, 0);
        assert_eq!(config.episode_index, 0);
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
    fn test_encoder_single_camera() {
        let output_dir = test_output_dir();
        let config = ConcurrentEncoderConfig {
            key_prefix: "test_single".to_string(),
            chunk_index: 0,
            episode_index: 0,
            video_config: VideoEncoderConfig::default(),
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
            key_prefix: "test_multi".to_string(),
            chunk_index: 0,
            episode_index: 0,
            video_config: VideoEncoderConfig::default(),
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
            assert!(result.output_path.exists());
        }
    }

    #[test]
    fn test_encoder_skip_invalid_frames() {
        let output_dir = test_output_dir();
        let config = ConcurrentEncoderConfig {
            key_prefix: "test_skip".to_string(),
            chunk_index: 0,
            episode_index: 0,
            video_config: VideoEncoderConfig::default(),
            output_dir: output_dir.clone(),
            path_scheme: None,
        };

        let mut encoder = ConcurrentVideoEncoder::new(config).unwrap();

        // Add a valid frame first (sets dimensions)
        let valid_image = ImageData::new(64, 64, vec![128u8; 64 * 64 * 3]);
        encoder.add_frame("cam0", valid_image).unwrap();

        // Add an invalid frame (zero dimensions - should be skipped)
        let invalid_image = ImageData::new(0, 0, vec![]);
        encoder.add_frame("cam0", invalid_image).unwrap();

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

    #[test]
    fn test_encoder_cannot_add_after_finalize() {
        let config = ConcurrentEncoderConfig::new(test_output_dir());
        let encoder = ConcurrentVideoEncoder::new(config).unwrap();

        // Finalize without adding any frames
        let results = encoder.finalize().unwrap();
        assert!(results.is_empty());
        // Note: encoder is consumed by finalize(), so we can't add frames after
    }

    #[test]
    fn test_non_square_dimensions_encoding() {
        // Regression test for non-square image dimensions
        let output_dir = test_output_dir();
        let config = ConcurrentEncoderConfig {
            key_prefix: "test_nonsquare".to_string(),
            chunk_index: 0,
            episode_index: 0,
            video_config: VideoEncoderConfig::default(),
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
}
