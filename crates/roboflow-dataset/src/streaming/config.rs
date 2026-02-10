// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Streaming configuration for frame alignment.

use std::collections::HashMap;

use crate::image::ImageDecoderConfig;

/// Configuration for streaming frame alignment.
#[derive(Debug, Clone)]
pub struct StreamingConfig {
    /// Frames per second for the output dataset
    pub fps: u32,

    /// Completion window in nanoseconds (how long to wait for late messages)
    pub completion_window_ns: u64,

    /// Feature requirements for frame completion
    pub feature_requirements: HashMap<String, usize>,

    /// Image decoder configuration
    pub decoder_config: Option<ImageDecoderConfig>,
}

impl StreamingConfig {
    /// Create a new streaming config with specified FPS.
    pub fn with_fps(fps: u32) -> Self {
        Self {
            fps,
            // Default completion window: 3 frames worth of data
            completion_window_ns: Self::default_completion_window(fps),
            feature_requirements: HashMap::new(),
            decoder_config: None,
        }
    }

    /// Calculate default completion window based on FPS.
    pub fn default_completion_window(fps: u32) -> u64 {
        // 3 frames at the given FPS
        let frame_interval_ns = 1_000_000_000u64 / fps as u64;
        frame_interval_ns * 3
    }

    /// Get the frame interval in nanoseconds.
    pub fn frame_interval_ns(&self) -> u64 {
        1_000_000_000u64 / self.fps as u64
    }

    /// Get the completion window in nanoseconds.
    pub fn completion_window_ns(&self) -> u64 {
        self.completion_window_ns
    }

    /// Set completion window.
    pub fn with_completion_window(mut self, window_ns: u64) -> Self {
        self.completion_window_ns = window_ns;
        self
    }

    /// Add a feature requirement.
    pub fn require_feature(mut self, feature: impl Into<String>, count: usize) -> Self {
        self.feature_requirements.insert(feature.into(), count);
        self
    }

    /// Set decoder configuration.
    pub fn with_decoder(mut self, config: ImageDecoderConfig) -> Self {
        self.decoder_config = Some(config);
        self
    }
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self::with_fps(30)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_with_fps() {
        let config = StreamingConfig::with_fps(60);
        assert_eq!(config.fps, 60);
        // 60 FPS = 16.666... ms per frame
        // 3 frames = 49,999,998ns (1_000_000_000 / 60 * 3 with integer division)
        assert_eq!(config.completion_window_ns, 49_999_998);
    }

    #[test]
    fn test_frame_interval_ns() {
        let config = StreamingConfig::with_fps(30);
        // 30 FPS = 33.333... ms per frame (integer division: 1_000_000_000 / 30)
        assert_eq!(config.frame_interval_ns(), 33_333_333);
    }

    #[test]
    fn test_completion_window_ns() {
        let config = StreamingConfig::with_fps(30);
        // 3 frames worth = 33,333,333 * 3 = 99,999,999
        assert_eq!(config.completion_window_ns(), 99_999_999);
    }

    #[test]
    fn test_with_completion_window() {
        let config = StreamingConfig::with_fps(30).with_completion_window(200_000_000);

        assert_eq!(config.completion_window_ns(), 200_000_000);
    }

    #[test]
    fn test_require_feature() {
        let config = StreamingConfig::with_fps(30).require_feature("camera_0", 1);

        assert_eq!(config.feature_requirements.len(), 1);
        assert_eq!(config.feature_requirements.get("camera_0"), Some(&1));
    }
}
