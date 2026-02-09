// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Configuration for streaming dataset conversion.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::image::{ImageDecoderBackend, ImageDecoderConfig, ImageDecoderFactory};

/// Streaming dataset converter configuration.
#[derive(Debug, Clone)]
pub struct StreamingConfig {
    /// Target FPS for frame alignment
    pub fps: u32,

    /// Frame completion window (in frames)
    ///
    /// Messages arriving after this window (from the frame's timestamp)
    /// are considered "late" and the frame will be force-completed.
    pub completion_window_frames: usize,

    /// Maximum frames to buffer before forcing completion
    pub max_buffered_frames: usize,

    /// Maximum memory to buffer (in MB)
    pub max_buffered_memory_mb: usize,

    /// How to handle messages arriving after frame completion
    pub late_message_strategy: LateMessageStrategy,

    /// Per-feature completion requirements
    /// Keys are feature names (e.g., "observation.images.cam_high")
    pub feature_requirements: HashMap<String, FeatureRequirement>,

    /// Temporary directory for downloading cloud input files
    ///
    /// When the input storage is a cloud backend (S3/OSS), files are downloaded
    /// to this directory before processing. Defaults to `std::env::temp_dir()`.
    pub temp_dir: Option<PathBuf>,

    /// Image decoder configuration for CompressedImage messages.
    ///
    /// When set, compressed images (JPEG/PNG) will be decoded to RGB
    /// before being stored in the dataset. If None, compressed images
    /// are stored as-is.
    pub decoder_config: Option<ImageDecoderConfig>,

    /// Pre-created shared decoder (used when set; avoids creating a decoder per alignment buffer).
    /// Set by `resolve_decoder()` so the decoder is created once and reused.
    pub shared_decoder: Option<Arc<dyn ImageDecoderBackend>>,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        use crate::image::ImageDecoderConfig;

        Self {
            fps: 30,
            completion_window_frames: 5, // Wait for 5 frames (166ms at 30fps)
            max_buffered_frames: 300,    // 10 seconds at 30fps
            max_buffered_memory_mb: 500, // 500MB max buffer
            late_message_strategy: LateMessageStrategy::WarnAndDrop,
            feature_requirements: HashMap::new(),
            temp_dir: None,
            decoder_config: Some(ImageDecoderConfig::new()),
            shared_decoder: None,
        }
    }
}

/// How to handle messages arriving after frame completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LateMessageStrategy {
    /// Drop late messages silently
    Drop,

    /// Log warning but drop late messages
    WarnAndDrop,

    /// Create a new frame (can cause gaps in sequence)
    CreateNewFrame,
}

/// Feature completion requirement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeatureRequirement {
    /// Feature must be present for frame to be complete
    Required,

    /// Feature is optional (does not affect completion)
    Optional,

    /// At least N of the listed features must be present
    AtLeast { min_count: usize },
}

impl StreamingConfig {
    /// Create a new configuration with the given FPS.
    ///
    /// # Panics
    ///
    /// Panics if `fps` is 0.
    pub fn with_fps(fps: u32) -> Self {
        assert!(fps > 0, "FPS must be greater than 0, got {}", fps);
        Self {
            fps,
            ..Default::default()
        }
    }

    /// Validate the configuration.
    ///
    /// Returns an error if the configuration is invalid.
    pub fn validate(&self) -> Result<(), String> {
        if self.fps == 0 {
            return Err("FPS must be greater than 0".to_string());
        }
        if self.completion_window_frames == 0 {
            return Err("Completion window must be at least 1 frame".to_string());
        }
        if self.max_buffered_frames == 0 {
            return Err("Max buffered frames must be at least 1".to_string());
        }
        Ok(())
    }

    /// Set the completion window (in frames).
    pub fn with_completion_window(mut self, frames: usize) -> Self {
        self.completion_window_frames = frames;
        self
    }

    /// Set the maximum buffered frames.
    pub fn with_max_buffered_frames(mut self, max: usize) -> Self {
        self.max_buffered_frames = max;
        self
    }

    /// Set the maximum buffered memory (in MB).
    pub fn with_max_memory_mb(mut self, mb: usize) -> Self {
        self.max_buffered_memory_mb = mb;
        self
    }

    /// Set the late message strategy.
    pub fn with_late_message_strategy(mut self, strategy: LateMessageStrategy) -> Self {
        self.late_message_strategy = strategy;
        self
    }

    /// Add a required feature.
    pub fn require_feature(mut self, feature: impl Into<String>) -> Self {
        self.feature_requirements
            .insert(feature.into(), FeatureRequirement::Required);
        self
    }

    /// Add an optional feature.
    pub fn optional_feature(mut self, feature: impl Into<String>) -> Self {
        self.feature_requirements
            .insert(feature.into(), FeatureRequirement::Optional);
        self
    }

    /// Set the temporary directory for cloud input downloads.
    pub fn with_temp_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.temp_dir = Some(dir.into());
        self
    }

    /// Set the image decoder configuration.
    ///
    /// When configured, compressed images (JPEG/PNG) will be decoded to RGB
    /// before being stored in the dataset.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use roboflow_dataset::{StreamingConfig, image::ImageDecoderConfig};
    ///
    /// let config = StreamingConfig::with_fps(30)
    ///     .with_decoder_config(ImageDecoderConfig::max_throughput());
    /// ```
    pub fn with_decoder_config(mut self, config: ImageDecoderConfig) -> Self {
        self.decoder_config = Some(config);
        self
    }

    /// Create the image decoder once and store it as shared_decoder.
    ///
    /// Call this when building config for a converter so that alignment buffers
    /// reuse the same decoder instead of each creating their own (which would
    /// call create_decoder many times). Returns a new config with
    /// `shared_decoder` set and `decoder_config` cleared.
    pub fn resolve_decoder(mut self) -> Self {
        if let Some(ref decoder_config) = self.decoder_config {
            let mut factory = ImageDecoderFactory::new(decoder_config);
            if let Ok(decoder) = factory.create_decoder() {
                self.shared_decoder = Some(Arc::from(decoder));
                self.decoder_config = None;
            }
        }
        self
    }

    /// Calculate the completion window in nanoseconds.
    ///
    /// # Panics
    ///
    /// Panics if `fps` is 0.
    #[inline]
    pub fn completion_window_ns(&self) -> u64 {
        let frame_interval_ns = self.frame_interval_ns();
        frame_interval_ns * self.completion_window_frames as u64
    }

    /// Calculate frame interval in nanoseconds.
    ///
    /// # Panics
    ///
    /// Panics if `fps` is 0.
    #[inline]
    pub fn frame_interval_ns(&self) -> u64 {
        // Checked would return Option, but we want to fail fast with a clear message
        // The with_fps constructor validates fps > 0
        1_000_000_000 / self.fps as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = StreamingConfig::default();
        assert_eq!(config.fps, 30);
        assert_eq!(config.completion_window_frames, 5);
        assert_eq!(config.max_buffered_frames, 300);
        assert_eq!(config.max_buffered_memory_mb, 500);
    }

    #[test]
    fn test_frame_interval_calculation() {
        let config = StreamingConfig::with_fps(30);
        assert_eq!(config.frame_interval_ns(), 33_333_333);

        let config = StreamingConfig::with_fps(60);
        assert_eq!(config.frame_interval_ns(), 16_666_666);
    }

    #[test]
    fn test_completion_window_ns() {
        let config = StreamingConfig::with_fps(30).with_completion_window(5);
        // 30 FPS = 33.33ms per frame, 5 frames = ~166.7ms
        assert_eq!(config.completion_window_ns(), 166_666_665);
    }

    #[test]
    fn test_config_validation() {
        let config = StreamingConfig::with_fps(30);
        assert!(config.validate().is_ok());

        // Create a config with fps=0 (only possible through direct struct construction)
        // Note: with_fps() would panic, so we test validate() separately
        let config = StreamingConfig {
            fps: 0,
            temp_dir: None,
            decoder_config: None,
            shared_decoder: None,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_with_fps_panics_on_zero() {
        // with_fps should panic on fps=0
        let result = std::panic::catch_unwind(|| {
            StreamingConfig::with_fps(0);
        });
        assert!(result.is_err());
    }
}
