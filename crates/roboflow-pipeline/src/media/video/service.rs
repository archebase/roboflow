// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Video encoder service abstraction.
//!
//! This module provides the `VideoEncoderService` trait which abstracts
//! video encoding operations with configurable path schemes.

use roboflow_core::Result;
use std::sync::Arc;

use crate::core::VideoPathScheme;
use crate::formats::common::ImageData;

/// Configuration for video encoder service.
#[derive(Debug, Clone)]
pub struct VideoServiceConfig {
    /// Video path scheme for generating output paths.
    pub path_scheme: Arc<dyn VideoPathScheme>,
    /// Chunk size for streaming delivery (bytes).
    pub chunk_size: usize,
    /// Frame channel capacity (backpressure threshold).
    pub frame_channel_capacity: usize,
    /// Whether to use parallel pipeline.
    pub use_parallel_pipeline: bool,
}

impl VideoServiceConfig {
    /// Create a new video service config with the given path scheme.
    pub fn new(path_scheme: Arc<dyn VideoPathScheme>) -> Self {
        Self {
            path_scheme,
            chunk_size: 256 * 1024, // 256KB chunks
            frame_channel_capacity: 64,
            use_parallel_pipeline: false,
        }
    }
}

/// Trait for video encoding services.
///
/// This trait abstracts the video encoding pipeline, allowing different
/// implementations for various storage backends and encoding strategies.
///
/// # Example
///
/// ```ignore
/// use roboflow_pipeline::video::{VideoEncoderService, LeRobotVideoPathScheme};
/// use std::sync::Arc;
///
/// let path_scheme = Arc::new(LeRobotVideoPathScheme::new("dataset/episode_001"));
/// let service = VideoEncoderServiceImpl::new(path_scheme);
///
/// service.add_frame("cam0", image)?;
/// let results = service.finalize()?;
/// ```
pub trait VideoEncoderService: Send + Sync {
    /// Add a frame for the specified camera.
    ///
    /// # Arguments
    ///
    /// * `camera` - Camera identifier
    /// * `image` - Image data to encode
    ///
    /// # Returns
    ///
    /// Returns Ok(()) if the frame was queued successfully.
    fn add_frame(&mut self, camera: &str, image: ImageData) -> Result<()>;

    /// Finalize encoding and return results.
    ///
    /// This method blocks until all frames are encoded and uploaded.
    /// After calling this, no more frames can be added.
    fn finalize(&mut self) -> Result<Vec<EncoderResult>>;

    /// Abort encoding and clean up resources.
    fn abort(&mut self) -> Result<()>;

    /// Get the list of cameras being encoded.
    fn cameras(&self) -> Vec<&str>;

    /// Check if encoding is finalized.
    fn is_finalized(&self) -> bool;

    /// Get the video path scheme.
    fn path_scheme(&self) -> &dyn VideoPathScheme;
}

/// Result from video encoding.
#[derive(Debug, Clone)]
pub struct EncoderResult {
    /// Camera name.
    pub camera: String,
    /// Destination URL/path.
    pub url: String,
    /// Number of frames encoded.
    pub frames_encoded: usize,
    /// Number of frames skipped (invalid).
    pub frames_skipped: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_video_service_config() {
        use crate::media::video::LeRobotVideoPathScheme;
        let scheme = Arc::new(LeRobotVideoPathScheme::new("test"));
        let config = VideoServiceConfig::new(scheme);

        assert_eq!(config.chunk_size, 256 * 1024);
        assert_eq!(config.frame_channel_capacity, 64);
        assert!(!config.use_parallel_pipeline);
    }
}
