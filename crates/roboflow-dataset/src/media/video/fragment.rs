// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Fragment-based video encoding for streaming uploads.
//!
//! This module provides `FragmentEncoder` which encodes a batch of frames
//! to a temporary MP4 file. The fragment can then be uploaded and cleaned up.
//!
//! # Design
//!
//! - Encodes fixed-size batches of frames to temp files
//! - Returns fragment metadata (path, size, frame count)
//! - Automatic cleanup of temp files on drop
//!
//! # Example
//!
//! ```ignore
//! let encoder = FragmentEncoder::new(config)?;
//! let fragment = encoder.encode(frames)?;
//! // Upload fragment.data or read from fragment.path
//! // Temp file is cleaned up when FragmentInfo is dropped
//! ```

use std::path::PathBuf;

use roboflow_core::{Result, RoboflowError};

use crate::media::video::config::VideoEncoderConfig;
use crate::media::video::frame::{VideoFrame, VideoFrameBuffer};
use crate::media::video::rsmpeg::PersistentEncoder as InnerEncoder;

/// Metadata about an encoded fragment.
#[derive(Debug)]
pub struct FragmentInfo {
    /// Path to the temporary file containing encoded data.
    /// The file is deleted when this struct is dropped.
    pub path: PathBuf,
    /// Number of frames in this fragment.
    pub frame_count: usize,
    /// Size of the encoded data in bytes.
    pub byte_size: u64,
    /// Duration of the fragment in milliseconds (based on FPS).
    pub duration_ms: u64,
}

impl Drop for FragmentInfo {
    fn drop(&mut self) {
        // Clean up temp file
        if let Err(e) = std::fs::remove_file(&self.path) {
            tracing::warn!(
                path = %self.path.display(),
                error = %e,
                "Failed to clean up temp fragment file"
            );
        }
    }
}

impl FragmentInfo {
    /// Read the fragment data from the temp file.
    pub fn read_data(&self) -> Result<Vec<u8>> {
        std::fs::read(&self.path).map_err(|e| {
            RoboflowError::encode(
                "FragmentInfo",
                format!("Failed to read fragment data: {}", e),
            )
        })
    }
}

/// Configuration for fragment encoding.
#[derive(Debug, Clone)]
pub struct FragmentEncoderConfig {
    /// Video encoder configuration (codec, fps, etc.)
    pub video: VideoEncoderConfig,
    /// Directory for temporary files.
    pub temp_dir: PathBuf,
    /// Maximum frames per fragment.
    pub max_frames_per_fragment: usize,
    /// Camera identifier for unique temp file names.
    pub camera_id: String,
}

impl Default for FragmentEncoderConfig {
    fn default() -> Self {
        Self {
            video: VideoEncoderConfig::default(),
            temp_dir: std::env::temp_dir(),
            max_frames_per_fragment: 300, // 10 seconds @ 30fps
            camera_id: String::new(),
        }
    }
}

/// Fragment-based video encoder.
///
/// Encodes batches of frames to temporary MP4 files for streaming upload.
pub struct FragmentEncoder {
    config: FragmentEncoderConfig,
    /// Fragment counter for unique filenames.
    fragment_counter: u64,
    /// Process ID for unique temp file names.
    pid: u32,
    /// Inner encoder for reusing codec context across fragments.
    inner: Option<InnerEncoder>,
}

impl FragmentEncoder {
    /// Create a new fragment encoder.
    pub fn new(config: FragmentEncoderConfig) -> Result<Self> {
        // Ensure temp directory exists
        std::fs::create_dir_all(&config.temp_dir).map_err(|e| {
            RoboflowError::encode(
                "FragmentEncoder",
                format!("Failed to create temp directory: {}", e),
            )
        })?;

        Ok(Self {
            config,
            fragment_counter: 0,
            pid: std::process::id(),
            inner: None,
        })
    }

    /// Create a fragment encoder with default configuration.
    pub fn with_temp_dir(temp_dir: PathBuf) -> Result<Self> {
        Self::new(FragmentEncoderConfig {
            temp_dir,
            ..Default::default()
        })
    }

    /// Encode a batch of frames to a temporary MP4 file.
    ///
    /// # Arguments
    ///
    /// * `frames` - Frames to encode (will be consumed)
    ///
    /// # Returns
    ///
    /// `FragmentInfo` containing the temp file path and metadata.
    /// The temp file is automatically cleaned up when `FragmentInfo` is dropped.
    pub fn encode(&mut self, frames: Vec<VideoFrame>) -> Result<FragmentInfo> {
        if frames.is_empty() {
            return Err(RoboflowError::encode(
                "FragmentEncoder",
                "Cannot encode empty frame batch",
            ));
        }

        // Generate unique temp file path
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        self.fragment_counter += 1;

        // Sanitize camera_id for use in filename (replace non-alphanumeric with underscore)
        let safe_camera_id: String = self
            .config
            .camera_id
            .chars()
            .map(|c| if c.is_alphanumeric() { c } else { '_' })
            .collect();

        let temp_path = self.config.temp_dir.join(format!(
            "fragment_{}_{}_{}_{}.mp4",
            self.pid, safe_camera_id, self.fragment_counter, nonce
        ));

        let frame_count = frames.len();

        // Create VideoFrameBuffer from frames
        let mut buffer = VideoFrameBuffer::new();
        for frame in frames {
            buffer.add_frame(frame).map_err(|e| {
                RoboflowError::encode(
                    "FragmentEncoder",
                    format!("Failed to add frame to buffer: {}", e),
                )
            })?;
        }

        // Encode using inner persistent encoder for codec context reuse
        // Initialize lazily on first encode call
        if self.inner.is_none() {
            self.inner = Some(InnerEncoder::new());
            tracing::debug!("Initialized fragment encoder");
        }

        let encoder = self.inner.as_mut().unwrap();
        encoder
            .encode_fragment_to_path(&buffer, &self.config.video, &temp_path)
            .map_err(|e| {
                let _ = std::fs::remove_file(&temp_path);
                RoboflowError::encode(
                    "FragmentEncoder",
                    format!("Failed to encode fragment: {:?}", e),
                )
            })?;

        // Get file size
        let metadata = std::fs::metadata(&temp_path).map_err(|e| {
            let _ = std::fs::remove_file(&temp_path);
            RoboflowError::encode(
                "FragmentEncoder",
                format!("Failed to get fragment metadata: {}", e),
            )
        })?;
        let byte_size = metadata.len();

        // Calculate duration
        let duration_ms = (frame_count as u64 * 1000) / self.config.video.fps as u64;

        tracing::debug!(
            path = %temp_path.display(),
            frames = frame_count,
            bytes = byte_size,
            duration_ms = duration_ms,
            "Fragment encoded"
        );

        Ok(FragmentInfo {
            path: temp_path,
            frame_count,
            byte_size,
            duration_ms,
        })
    }

    /// Get the maximum frames per fragment.
    pub fn max_frames(&self) -> usize {
        self.config.max_frames_per_fragment
    }

    /// Get the video encoder configuration.
    pub fn video_config(&self) -> &VideoEncoderConfig {
        &self.config.video
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fragment_encoder_config_default() {
        let config = FragmentEncoderConfig::default();
        assert_eq!(config.max_frames_per_fragment, 300);
        assert_eq!(config.video.fps, 30);
        assert_eq!(config.camera_id, "");
    }

    #[test]
    fn test_encode_empty_frames_returns_error() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut encoder = FragmentEncoder::with_temp_dir(temp_dir.path().to_path_buf()).unwrap();
        let result = encoder.encode(vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn test_encode_single_frame() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut encoder = FragmentEncoder::with_temp_dir(temp_dir.path().to_path_buf()).unwrap();

        // Create a simple test frame (64x64 RGB)
        let rgb_data = vec![128u8; 64 * 64 * 3];
        let frame = VideoFrame::new(64, 64, rgb_data);

        let fragment = encoder.encode(vec![frame]).unwrap();
        assert_eq!(fragment.frame_count, 1);
        assert!(fragment.byte_size > 0);
        assert!(fragment.path.exists());
    }

    #[test]
    fn test_fragment_cleanup_on_drop() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut encoder = FragmentEncoder::with_temp_dir(temp_dir.path().to_path_buf()).unwrap();

        let rgb_data = vec![128u8; 64 * 64 * 3];
        let frame = VideoFrame::new(64, 64, rgb_data);

        let fragment = encoder.encode(vec![frame]).unwrap();
        let path = fragment.path.clone();
        assert!(path.exists());

        // Drop the fragment
        drop(fragment);

        // Temp file should be cleaned up
        assert!(!path.exists());
    }

    /// Regression test for race condition: different camera IDs should produce
    /// different temp file paths even when encoding at the same time.
    #[test]
    fn test_different_cameras_produce_unique_filenames() {
        let temp_dir = tempfile::tempdir().unwrap();

        // Create two encoders with different camera IDs
        let config1 = FragmentEncoderConfig {
            video: VideoEncoderConfig::default(),
            temp_dir: temp_dir.path().to_path_buf(),
            max_frames_per_fragment: 300,
            camera_id: "observation.images.cam_left".to_string(),
        };
        let config2 = FragmentEncoderConfig {
            video: VideoEncoderConfig::default(),
            temp_dir: temp_dir.path().to_path_buf(),
            max_frames_per_fragment: 300,
            camera_id: "observation.images.cam_right".to_string(),
        };

        let mut encoder1 = FragmentEncoder::new(config1).unwrap();
        let mut encoder2 = FragmentEncoder::new(config2).unwrap();

        // Create test frames
        let rgb_data = vec![128u8; 64 * 64 * 3];
        let frame = VideoFrame::new(64, 64, rgb_data.clone());

        // Encode from both encoders (simulating concurrent encoding)
        let fragment1 = encoder1.encode(vec![frame]).unwrap();
        let frame2 = VideoFrame::new(64, 64, rgb_data);
        let fragment2 = encoder2.encode(vec![frame2]).unwrap();

        // The paths should be different because camera IDs are different
        assert_ne!(
            fragment1.path, fragment2.path,
            "Fragments from different cameras must have unique paths"
        );

        // Verify the camera ID is embedded in the filename
        let filename1 = fragment1.path.file_name().unwrap().to_str().unwrap();
        let filename2 = fragment2.path.file_name().unwrap().to_str().unwrap();

        assert!(
            filename1.contains("cam_left"),
            "Expected 'cam_left' in filename, got: {}",
            filename1
        );
        assert!(
            filename2.contains("cam_right"),
            "Expected 'cam_right' in filename, got: {}",
            filename2
        );

        // Both files should exist simultaneously (no overwrites)
        assert!(fragment1.path.exists());
        assert!(fragment2.path.exists());
    }
}
