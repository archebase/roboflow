// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! # Rsmpeg Native FFmpeg Bindings
//!
//! This module provides high-performance video encoding utilities using native
//! FFmpeg bindings via the rsmpeg library.
//!
//! ## Features
//!
//! - In-process FFmpeg encoding (no subprocess overhead)
//! - Hardware encoder support (NVENC, VideoToolbox) with fallback to libx264
//!
//! ## Re-exports
//!
//! This module selectively re-exports rsmpeg types for use by the unified encoder.

// Re-export rsmpeg types selectively
pub use rsmpeg::{
    avcodec::{AVCodec, AVCodecContext, AVCodecID, AVPacket},
    avformat::AVFormatContextOutput,
    avutil::{AVDictionary, AVFrame, AVRational},
    error::RsmpegError,
    swscale::SwsContext,
};

// =============================================================================
// Utility Functions
// =============================================================================

/// Check if rsmpeg is available.
pub fn is_rsmpeg_available() -> bool {
    // rsmpeg is now a direct dependency with link_system_ffmpeg
    true
}

/// Check if hardware encoding is available.
pub fn is_hardware_encoding_available() -> bool {
    #[cfg(target_os = "linux")]
    {
        // Check for NVENC (NVIDIA)
        AVCodec::find_encoder_by_name(c"h264_nvenc").is_some()
    }

    #[cfg(target_os = "macos")]
    {
        // VideoToolbox is always available on macOS
        AVCodec::find_encoder_by_name(c"h264_videotoolbox").is_some()
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        false
    }
}

/// Get the default codec name for the current platform.
pub fn default_codec_name() -> &'static str {
    #[cfg(target_os = "macos")]
    {
        if is_hardware_encoding_available() {
            "h264_videotoolbox"
        } else {
            "libx264"
        }
    }

    #[cfg(target_os = "linux")]
    {
        if is_hardware_encoding_available() {
            "h264_nvenc"
        } else {
            "libx264"
        }
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        "libx264"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_rsmpeg_available() {
        assert!(is_rsmpeg_available());
    }

    #[test]
    fn test_default_codec_name() {
        let codec = default_codec_name();
        assert!(!codec.is_empty());
    }

    #[test]
    fn test_hardware_encoding_detection() {
        // This test will pass if hardware encoding is available
        // It may fail on systems without GPU support
        let _available = is_hardware_encoding_available();
        // Just check the function doesn't crash
    }
}
