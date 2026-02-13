// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Configuration for rsmpeg encoder.

use std::ffi::CStr;

use rsmpeg::avcodec::AVCodec;

/// Configuration for rsmpeg encoder.
#[derive(Debug, Clone)]
pub struct RsmpegEncoderConfig {
    /// Video width in pixels
    pub width: u32,

    /// Video height in pixels
    pub height: u32,

    /// Frame rate (fps)
    pub fps: u32,

    /// Target bitrate (bps)
    pub bitrate: u64,

    /// Codec name (e.g., "h264_nvenc", "libx264", "hevc_nvenc")
    pub codec: String,

    /// Output pixel format ("nv12" for NVENC, "yuv420p" for libx264)
    pub pixel_format: String,

    /// CRF quality (0-51 for H.264, lower = better quality)
    pub crf: u32,

    /// Encoder preset (speed/quality tradeoff)
    pub preset: String,

    /// GOP size (keyframe interval in frames)
    pub gop_size: u32,

    /// Buffer size for accumulating encoded data before sending
    pub buffer_size: usize,

    /// Number of B-frames between I/P frames
    pub max_b_frames: u32,
}

impl Default for RsmpegEncoderConfig {
    fn default() -> Self {
        Self {
            width: 640,
            height: 480,
            fps: 30,
            bitrate: 5_000_000,           // 5 Mbps
            codec: "libx264".to_string(), // Default to CPU encoder
            pixel_format: "yuv420p".to_string(),
            crf: 23,
            preset: "medium".to_string(),
            gop_size: 30,
            buffer_size: 4 * 1024 * 1024, // 4MB buffer
            max_b_frames: 1,
        }
    }
}

impl RsmpegEncoderConfig {
    /// Create a new encoder configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set video dimensions.
    pub fn with_dimensions(mut self, width: u32, height: u32) -> Self {
        self.width = width;
        self.height = height;
        self
    }

    /// Set frame rate.
    pub fn with_fps(mut self, fps: u32) -> Self {
        self.fps = fps;
        self
    }

    /// Set bitrate.
    pub fn with_bitrate(mut self, bitrate: u64) -> Self {
        self.bitrate = bitrate;
        self
    }

    /// Set codec name.
    pub fn with_codec(mut self, codec: impl Into<String>) -> Self {
        self.codec = codec.into();
        self
    }

    /// Set pixel format.
    pub fn with_pixel_format(mut self, format: impl Into<String>) -> Self {
        self.pixel_format = format.into();
        self
    }

    /// Set CRF quality.
    pub fn with_crf(mut self, crf: u32) -> Self {
        self.crf = crf;
        self
    }

    /// Set encoder preset.
    pub fn with_preset(mut self, preset: impl Into<String>) -> Self {
        self.preset = preset.into();
        self
    }

    /// Detect and use best available codec.
    ///
    /// This attempts to find hardware encoders first (NVENC, VideoToolbox)
    /// and falls back to libx264 if unavailable.
    pub fn detect_best_codec() -> Self {
        #[cfg(target_os = "linux")]
        {
            // Try NVENC first on Linux
            if Self::is_codec_available("h264_nvenc") {
                tracing::info!("Detected NVENC encoder for hardware acceleration");
                return Self {
                    codec: "h264_nvenc".to_string(),
                    pixel_format: "nv12".to_string(),
                    preset: "p4".to_string(), // NVENC preset p1-p7 (p4 = medium)
                    ..Default::default()
                };
            }
        }

        #[cfg(target_os = "macos")]
        {
            // Try VideoToolbox on macOS
            if Self::is_codec_available("h264_videotoolbox") {
                tracing::info!("Detected VideoToolbox encoder for hardware acceleration");
                return Self {
                    codec: "h264_videotoolbox".to_string(),
                    pixel_format: "nv12".to_string(),
                    preset: "medium".to_string(),
                    ..Default::default()
                };
            }
        }

        // Default to libx264
        tracing::info!("Using libx264 CPU encoder");
        Self {
            codec: "libx264".to_string(),
            pixel_format: "yuv420p".to_string(),
            preset: "medium".to_string(),
            ..Default::default()
        }
    }

    /// Check if a codec is available by name.
    fn is_codec_available(name: &str) -> bool {
        if name == "libx264" {
            return true;
        }
        // Try to find the encoder
        let name_with_nul = format!("{}\0", name);
        let codec_name = CStr::from_bytes_with_nul(name_with_nul.as_bytes()).unwrap_or(c"libx264");
        AVCodec::find_encoder_by_name(codec_name).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = RsmpegEncoderConfig::default();
        assert_eq!(config.width, 640);
        assert_eq!(config.height, 480);
        assert_eq!(config.fps, 30);
        assert_eq!(config.codec, "libx264");
    }

    #[test]
    fn test_config_builder() {
        let config = RsmpegEncoderConfig::new()
            .with_dimensions(1280, 720)
            .with_fps(60)
            .with_bitrate(10_000_000)
            .with_codec("h264_nvenc")
            .with_crf(20);

        assert_eq!(config.width, 1280);
        assert_eq!(config.height, 720);
        assert_eq!(config.fps, 60);
        assert_eq!(config.bitrate, 10_000_000);
        assert_eq!(config.codec, "h264_nvenc");
        assert_eq!(config.crf, 20);
    }

    #[test]
    fn test_detect_best_codec() {
        let config = RsmpegEncoderConfig::detect_best_codec();
        // Should always return a valid codec
        assert!(!config.codec.is_empty());
        assert!(
            config.codec == "libx264"
                || config.codec.contains("nvenc")
                || config.codec.contains("videotoolbox")
        );
    }
}
