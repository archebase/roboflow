// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Rsmpeg-based S3 streaming video encoder.
//!
//! This module provides a video encoder that:
//! - Uses native rsmpeg FFmpeg bindings (no subprocess)
//! - Encodes to an in-memory buffer
//! - Uploads directly to S3/OSS storage when complete
//!
//! This replaces the FFmpeg CLI-based S3StreamingEncoder.

use std::sync::Arc;

use roboflow_core::{Result, RoboflowError};
use roboflow_storage::object_store;
use tokio::runtime::Handle;

use crate::common::{ImageData, decode_to_rgb};
use crate::common::video::{VideoEncoderConfig, VideoFrame, VideoFrameBuffer};
use crate::common::rsmpeg_encoder::RsmpegMp4Encoder;

/// Configuration for rsmpeg S3 encoder.
#[derive(Debug, Clone, Default)]
pub struct RsmpegS3EncoderConfig {
    /// Video encoder configuration (codec, crf, preset, etc.)
    pub video: VideoEncoderConfig,
}

impl From<&VideoEncoderConfig> for RsmpegS3EncoderConfig {
    fn from(config: &VideoEncoderConfig) -> Self {
        Self {
            video: config.clone(),
        }
    }
}

/// Rsmpeg-based S3 streaming video encoder.
///
/// This encoder:
/// 1. Collects frames in memory
/// 2. Encodes to MP4 using native rsmpeg (FFmpeg bindings)
/// 3. Uploads directly to S3/OSS storage
///
/// # Example
///
/// ```ignore
/// use roboflow_dataset::common::rsmpeg_s3_encoder::RsmpegS3Encoder;
///
/// let config = RsmpegS3EncoderConfig::default();
/// let mut encoder = RsmpegS3Encoder::new(
///     "s3://bucket/videos/episode_000.mp4",
///     object_store,
///     runtime,
///     config,
/// )?;
///
/// // Add frames
/// for image in images {
///     encoder.add_frame(image)?;
/// }
///
/// // Finalize and get S3 URL
/// let (url, frames_encoded) = encoder.finalize()?;
/// ```
pub struct RsmpegS3Encoder {
    /// Object store for S3/OSS upload
    store: Arc<dyn object_store::ObjectStore>,

    /// Tokio runtime handle
    runtime: Handle,

    /// Destination path (s3://bucket/path/video.mp4)
    dest_path: String,

    /// Object key for S3/OSS (parsed from dest_path)
    key: object_store::path::Path,

    /// Encoder configuration
    config: RsmpegS3EncoderConfig,

    /// Video width
    width: u32,

    /// Video height
    height: u32,

    /// Frame buffer for collecting frames
    buffer: VideoFrameBuffer,

    /// Number of frames successfully added to buffer
    frames_encoded: usize,

    /// Number of frames skipped (decode failures, zero dimensions, etc.)
    frames_skipped: usize,

    /// Whether the encoder has been finalized
    finalized: bool,
}

impl RsmpegS3Encoder {
    /// Create a new rsmpeg S3 encoder.
    ///
    /// # Arguments
    ///
    /// * `dest_path` - Destination S3/OSS path (e.g., "s3://bucket/path/video.mp4")
    /// * `store` - Object store client
    /// * `runtime` - Tokio runtime handle
    /// * `config` - Encoder configuration
    pub fn new(
        dest_path: &str,
        store: Arc<dyn object_store::ObjectStore>,
        runtime: Handle,
        config: RsmpegS3EncoderConfig,
    ) -> Result<Self> {
        // Parse S3/OSS URL to extract the key
        let key = parse_s3_url_to_key(dest_path)?;

        Ok(Self {
            store,
            runtime,
            dest_path: dest_path.to_string(),
            key,
            config: config.clone(),
            width: 0,
            height: 0,
            buffer: VideoFrameBuffer::new(),
            frames_encoded: 0,
            frames_skipped: 0,
            finalized: false,
        })
    }

    /// Add a frame to the encoder.
    ///
    /// This method handles both raw RGB and compressed JPEG/PNG images.
    /// Compressed images are automatically decoded before encoding.
    ///
    /// # Arguments
    ///
    /// * `image` - The image data to encode
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The encoder has been finalized
    /// - The frame dimensions don't match
    /// - The image cannot be decoded (if encoded)
    pub fn add_frame(&mut self, image: &ImageData) -> Result<()> {
        if self.finalized {
            return Err(RoboflowError::encode(
                "RsmpegS3Encoder",
                "Cannot add frame to finalized encoder",
            ));
        }

        // Skip images with zero dimensions
        if image.width == 0 || image.height == 0 {
            tracing::warn!("Skipping image with zero dimensions");
            self.frames_skipped += 1;
            return Ok(());
        }

        // Set dimensions from first frame
        if self.width == 0 {
            self.width = image.width;
            self.height = image.height;
        }

        // Validate dimensions
        if image.width != self.width || image.height != self.height {
            self.frames_skipped += 1;
            tracing::warn!(
                expected_width = self.width,
                expected_height = self.height,
                actual_width = image.width,
                actual_height = image.height,
                "Skipping frame due to dimension mismatch"
            );
            return Ok(());
        }

        // Decode compressed images to RGB if needed
        let (width, height, rgb_data) = match decode_to_rgb(image) {
            Some((w, h, data)) => (w, h, data),
            None => {
                self.frames_skipped += 1;
                tracing::warn!(
                    width = image.width,
                    height = image.height,
                    data_len = image.data.len(),
                    "Failed to decode encoded image - this frame will be MISSING from output video. \
                     Check source file integrity and codec compatibility."
                );
                // Return Ok but don't encode this frame
                return Ok(());
            }
        };

        // Create video frame and add to buffer
        let video_frame = VideoFrame::new(width, height, rgb_data);
        if let Err(e) = self.buffer.add_frame(video_frame) {
            self.frames_skipped += 1;
            tracing::warn!(
                error = %e,
                "Failed to add frame to buffer - frame will be MISSING"
            );
            return Ok(());
        }

        self.frames_encoded += 1;

        Ok(())
    }

    /// Finalize encoding and upload to S3.
    ///
    /// # Returns
    ///
    /// A tuple of (url, frames_encoded) where:
    /// - `url` is the S3 URL
    /// - `frames_encoded` is the number of frames successfully encoded
    ///
    /// # Errors
    ///
    /// Returns an error if encoding or upload fails.
    pub fn finalize(mut self) -> Result<(String, usize)> {
        self.finalized = true;

        if self.buffer.is_empty() {
            // If we tried to encode frames but all failed, that's an error
            if self.frames_encoded == 0 && (self.frames_skipped > 0 || self.width > 0) {
                return Err(RoboflowError::encode(
                    "RsmpegS3Encoder",
                    format!(
                        "No frames were successfully encoded. {} frames were skipped due to decode failures or dimension mismatches.",
                        self.frames_skipped
                    ),
                ));
            }
            tracing::warn!("No frames to encode");
            return Ok((self.dest_path.clone(), 0));
        }

        // Log summary of skipped frames if any
        if self.frames_skipped > 0 {
            tracing::warn!(
                frames_encoded = self.frames_encoded,
                frames_skipped = self.frames_skipped,
                skip_rate = format!("{:.1}%",
                    (self.frames_skipped as f64 / (self.frames_encoded + self.frames_skipped) as f64) * 100.0
                ),
                "Video encoding completed with skipped frames - some frames are MISSING from output"
            );
        }

        // Create a temporary file path for encoding
        // Use process ID + random nonce to avoid collisions
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let temp_file = std::env::temp_dir().join(format!(
            "rsmpeg_encode_{}_{}.mp4",
            std::process::id(),
            nonce
        ));

        // Ensure temp file is cleaned up on all exit paths
        struct TempFileGuard<'a>(&'a std::path::Path);
        impl<'a> Drop for TempFileGuard<'a> {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(self.0);
            }
        }
        let _guard = TempFileGuard(&temp_file);

        // Encode using RsmpegMp4Encoder
        let encoder = RsmpegMp4Encoder::with_config(self.config.video.clone());

        encoder.encode_buffer(&self.buffer, &temp_file).map_err(|e| {
            RoboflowError::encode(
                "RsmpegS3Encoder",
                format!("Failed to encode video: {}", e),
            )
        })?;

        // Read the encoded file
        let encoded_data = std::fs::read(&temp_file).map_err(|e| {
            RoboflowError::encode(
                "RsmpegS3Encoder",
                format!("Failed to read encoded file: {}", e),
            )
        })?;
        let encoded_len = encoded_data.len();

        // Upload to S3 via object store
        self.runtime.block_on(async {
            self.store
                .put(&self.key, encoded_data.into())
                .await
                .map_err(|e| RoboflowError::encode("RsmpegS3Encoder", e.to_string()))
        })?;

        // Note: temp file cleanup happens automatically when _guard goes out of scope

        tracing::info!(
            bytes = encoded_len,
            frames = self.frames_encoded,
            path = %self.key,
            "Rsmpeg S3 encoding and upload completed"
        );

        Ok((self.dest_path.clone(), self.frames_encoded))
    }

    /// Get the number of frames encoded so far.
    pub fn frames_encoded(&self) -> usize {
        self.frames_encoded
    }

    /// Get the number of frames skipped (decode failures, dimension mismatches, etc.).
    pub fn frames_skipped(&self) -> usize {
        self.frames_skipped
    }

    /// Get the number of frames in the buffer.
    pub fn buffer_len(&self) -> usize {
        self.buffer.len()
    }

    /// Check if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}

/// Parse an S3/OSS URL to extract the key.
fn parse_s3_url_to_key(url: &str) -> std::result::Result<object_store::path::Path, RoboflowError> {
    let url_without_scheme = url
        .strip_prefix("s3://")
        .or_else(|| url.strip_prefix("oss://"))
        .ok_or_else(|| {
            RoboflowError::parse(
                "RsmpegS3Encoder",
                "URL must start with s3:// or oss://",
            )
        })?;

    let slash_idx = url_without_scheme.find('/').ok_or_else(|| {
        RoboflowError::parse(
            "RsmpegS3Encoder",
            "URL must contain a path after bucket",
        )
    })?;

    let key = &url_without_scheme[slash_idx + 1..];

    if !key.ends_with(".mp4") {
        return Err(RoboflowError::parse(
            "RsmpegS3Encoder",
            "Video file must have .mp4 extension",
        ));
    }

    Ok(object_store::path::Path::from(key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::runtime::Runtime;

    #[test]
    fn test_config_default() {
        let config = RsmpegS3EncoderConfig::default();
        assert_eq!(config.video.fps, 30);
    }

    #[test]
    fn test_config_from_video_config() {
        let video_config = VideoEncoderConfig {
            fps: 60,
            ..Default::default()
        };
        let config = RsmpegS3EncoderConfig::from(&video_config);
        assert_eq!(config.video.fps, 60);
    }

    #[test]
    fn test_dimension_mismatch_skips_frame() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let runtime = Runtime::new().unwrap();
        let config = RsmpegS3EncoderConfig::default();

        let mut encoder = RsmpegS3Encoder::new(
            "s3://test-bucket/videos/test.mp4",
            store,
            runtime.handle().clone(),
            config,
        ).unwrap();

        // Add first frame to set dimensions
        let rgb_data = vec![128u8; 640 * 480 * 3];
        let img1 = ImageData {
            width: 640,
            height: 480,
            data: rgb_data.clone(),
            original_timestamp: 0,
            is_encoded: false,
            is_depth: false,
        };
        encoder.add_frame(&img1).unwrap();

        // Add second frame with different dimensions - should be skipped
        let img2 = ImageData {
            width: 320,
            height: 240,
            data: rgb_data,
            original_timestamp: 1,
            is_encoded: false,
            is_depth: false,
        };
        let result = encoder.add_frame(&img2);
        assert!(result.is_ok(), "Dimension mismatch returns Ok but skips frame");
        assert_eq!(encoder.frames_skipped(), 1, "Frame should be tracked as skipped");
    }

    #[test]
    fn test_empty_finalize_returns_zero_frames() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let runtime = Runtime::new().unwrap();
        let config = RsmpegS3EncoderConfig::default();

        let encoder = RsmpegS3Encoder::new(
            "s3://test-bucket/videos/test.mp4",
            store,
            runtime.handle().clone(),
            config,
        ).unwrap();

        let (url, frames) = encoder.finalize().unwrap();
        assert_eq!(url, "s3://test-bucket/videos/test.mp4");
        assert_eq!(frames, 0);
    }

    #[test]
    fn test_all_frames_decode_failure_returns_error() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let runtime = Runtime::new().unwrap();
        let config = RsmpegS3EncoderConfig::default();

        let mut encoder = RsmpegS3Encoder::new(
            "s3://test-bucket/videos/test.mp4",
            store,
            runtime.handle().clone(),
            config,
        ).unwrap();

        // Add invalid encoded image that will fail to decode
        let invalid_jpeg = vec![0xFF, 0xD8, 0xFF]; // Too short
        let img = ImageData {
            width: 640,
            height: 480,
            data: invalid_jpeg,
            original_timestamp: 0,
            is_encoded: true,
            is_depth: false,
        };
        encoder.add_frame(&img).unwrap();

        // All frames failed, should return error
        let result = encoder.finalize();
        assert!(result.is_err(), "Should return error when all frames fail to decode");
    }

    #[test]
    fn test_zero_dimensions_skips_frame() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let runtime = Runtime::new().unwrap();
        let config = RsmpegS3EncoderConfig::default();

        let mut encoder = RsmpegS3Encoder::new(
            "s3://test-bucket/videos/test.mp4",
            store,
            runtime.handle().clone(),
            config,
        ).unwrap();

        // Add frame with zero dimensions
        let img = ImageData {
            width: 0,
            height: 480,
            data: vec![0u8; 1],
            original_timestamp: 0,
            is_encoded: false,
            is_depth: false,
        };
        encoder.add_frame(&img).unwrap();

        assert_eq!(encoder.frames_encoded(), 0);
        assert_eq!(encoder.frames_skipped(), 1);
    }
}
