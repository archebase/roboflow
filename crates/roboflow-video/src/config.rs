// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Configuration types for video encoding.

/// Video encoder configuration.
#[derive(Debug, Clone)]
pub struct VideoEncoderConfig {
    /// Video codec (default: H.264)
    pub codec: String,

    /// Pixel format (default: yuv420p)
    pub pixel_format: String,

    /// Frame rate for output video (default: 30)
    pub fps: u32,

    /// CRF quality value (lower = better quality, 0-51, default: 23)
    pub crf: u32,

    /// Whether to use fast preset
    pub preset: String,
}

impl Default for VideoEncoderConfig {
    fn default() -> Self {
        // Use hardware encoder when available for 5-10x speedup
        let best_encoder = crate::hardware::select_best_encoder();
        Self {
            codec: best_encoder.name().to_string(),
            pixel_format: "yuv420p".to_string(),
            fps: 30,
            crf: 23,
            preset: "fast".to_string(),
        }
    }
}

impl VideoEncoderConfig {
    /// Create a config with custom FPS.
    pub fn with_fps(mut self, fps: u32) -> Self {
        self.fps = fps;
        self
    }

    /// Create a config with custom quality.
    pub fn with_quality(mut self, crf: u32) -> Self {
        self.crf = crf;
        self
    }

    /// Create a config with custom codec.
    pub fn with_codec(mut self, codec: impl Into<String>) -> Self {
        self.codec = codec.into();
        self
    }

    /// Create a config with custom preset.
    pub fn with_preset(mut self, preset: impl Into<String>) -> Self {
        self.preset = preset.into();
        self
    }
}

/// Configuration for depth MKV encoding.
#[derive(Debug, Clone)]
pub struct DepthEncoderConfig {
    /// Frames per second for the output video.
    pub fps: u32,
    /// Video codec (e.g., "ffv1" for lossless).
    pub codec: String,
    /// Encoding preset (e.g., "fast", "medium", "slow").
    pub preset: String,
}

impl Default for DepthEncoderConfig {
    fn default() -> Self {
        Self {
            fps: 30,
            codec: "ffv1".to_string(),
            preset: "fast".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encoder_config_default() {
        let config = VideoEncoderConfig::default();
        // Default codec should use best available encoder (hardware when available)
        assert!(matches!(
            config.codec.as_str(),
            "h264_videotoolbox" | "nvenc" | "libx264"
        ));
        assert_eq!(config.pixel_format, "yuv420p");
        assert_eq!(config.fps, 30);
        assert_eq!(config.crf, 23);
        assert_eq!(config.preset, "fast");
    }

    #[test]
    fn test_encoder_config_with_fps() {
        let config = VideoEncoderConfig::default().with_fps(60);
        assert_eq!(config.fps, 60);
    }

    #[test]
    fn test_encoder_config_with_quality() {
        let config = VideoEncoderConfig::default().with_quality(18);
        assert_eq!(config.crf, 18);
    }
}
