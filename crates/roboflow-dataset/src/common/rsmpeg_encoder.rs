// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! # Rsmpeg Native Streaming Encoder
//!
//! This module provides high-performance video encoding using native FFmpeg bindings
//! via the rsmpeg library.
//!
//! ## Note
//!
//! This is a placeholder implementation. The full rsmpeg integration requires
//! updating to the correct rsmpeg v0.18 API. For now, this module provides
//! the type definitions and configuration used by the streaming coordinator.

use std::sync::mpsc::Sender;

use roboflow_core::Result;

// =============================================================================
// Configuration
// =============================================================================

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

    /// Fragment size for fMP4 output (bytes)
    pub fragment_size: usize,

    /// Number of B-frames between I/P frames
    pub max_b_frames: u32,
}

impl Default for RsmpegEncoderConfig {
    fn default() -> Self {
        Self {
            width: 640,
            height: 480,
            fps: 30,
            bitrate: 5_000_000, // 5 Mbps
            codec: "h264_nvenc".to_string(),
            pixel_format: "nv12".to_string(),
            crf: 23,
            preset: "p4".to_string(), // NVENC preset p1-p7 (p4 = medium)
            gop_size: 30,
            fragment_size: 1024 * 1024, // 1MB fragments
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
    pub fn detect_best_codec() -> Self {
        // Try NVENC first, fall back to libx264
        // For now, use libx264 as default since NVENC detection requires runtime check
        Self {
            codec: "libx264".to_string(),
            pixel_format: "yuv420p".to_string(),
            preset: "medium".to_string(),
            ..Default::default()
        }
    }
}

// =============================================================================
// Rsmpeg Encoder
// =============================================================================

/// Rsmpeg-based video encoder for streaming output.
///
/// This encoder uses native FFmpeg bindings for maximum performance.
pub struct RsmpegEncoder {
    /// Configuration
    config: RsmpegEncoderConfig,

    /// Channel for encoded fragments
    _encoded_tx: Sender<Vec<u8>>,

    /// Frame count
    frame_count: u64,

    /// Whether finalized
    finalized: bool,
}

impl RsmpegEncoder {
    /// Create a new rsmpeg encoder.
    ///
    /// # Arguments
    ///
    /// * `config` - Encoder configuration
    /// * `encoded_tx` - Channel to send encoded fragments
    pub fn new(config: RsmpegEncoderConfig, _encoded_tx: Sender<Vec<u8>>) -> Result<Self> {
        Ok(Self {
            config,
            _encoded_tx,
            frame_count: 0,
            finalized: false,
        })
    }

    /// Get the encoder configuration.
    pub fn config(&self) -> &RsmpegEncoderConfig {
        &self.config
    }

    /// Add a frame for encoding.
    ///
    /// # Arguments
    ///
    /// * `rgb_data` - Raw RGB image data (width × height × 3 bytes)
    pub fn add_frame(&mut self, _rgb_data: &[u8]) -> Result<()> {
        if self.finalized {
            return Err(roboflow_core::RoboflowError::encode(
                "RsmpegEncoder",
                "Cannot add frame to finalized encoder",
            ));
        }

        self.frame_count += 1;
        Ok(())
    }

    /// Finalize encoding and flush remaining data.
    pub fn finalize(&mut self) -> Result<()> {
        self.finalized = true;
        Ok(())
    }
}

// =============================================================================
// Utility Functions
// =============================================================================

/// Check if rsmpeg is available.
pub fn is_rsmpeg_available() -> bool {
    true // rsmpeg is now a direct dependency
}

/// Get an error indicating rsmpeg is unavailable.
pub fn rsmpeg_unavailable_error() -> roboflow_core::RoboflowError {
    roboflow_core::RoboflowError::unsupported("rsmpeg is not available")
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = RsmpegEncoderConfig::default();
        assert_eq!(config.width, 640);
        assert_eq!(config.height, 480);
        assert_eq!(config.fps, 30);
    }

    #[test]
    fn test_config_builder() {
        let config = RsmpegEncoderConfig::new()
            .with_dimensions(1280, 720)
            .with_fps(60)
            .with_bitrate(10_000_000);

        assert_eq!(config.width, 1280);
        assert_eq!(config.height, 720);
        assert_eq!(config.fps, 60);
        assert_eq!(config.bitrate, 10_000_000);
    }
}
