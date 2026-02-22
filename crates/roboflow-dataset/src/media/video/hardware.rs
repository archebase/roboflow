// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Hardware encoder detection utilities.
//!
//! This module provides hardware detection functions for video encoding:
//! - NVENC for NVIDIA GPUs (Linux/Windows)
//! - VideoToolbox for macOS
//!
//! Note: CLI-based encoders (Mp4Encoder, NvencEncoder, etc.) have been removed.
//! Use `RsmpegMp4Encoder` for native in-process encoding via rsmpeg, which is
//! 2-3x faster than FFmpeg CLI and doesn't require subprocess spawning.

// =============================================================================
// Hardware Detection Functions
// =============================================================================

/// Check if NVENC encoder is available.
///
/// Delegates to [`crate::media::video::hardware_config::detect_hardware_backend()`] as the
/// single source of truth for hardware detection.
pub fn check_nvenc_available() -> bool {
    matches!(
        crate::media::video::hardware_config::detect_hardware_backend(),
        crate::media::video::hardware_config::HardwareBackend::Nvenc
    )
}

/// Check if VideoToolbox encoder is available (macOS).
///
/// Delegates to [`crate::media::video::hardware_config::detect_hardware_backend()`] as the
/// single source of truth for hardware detection.
pub fn check_videotoolbox_available() -> bool {
    matches!(
        crate::media::video::hardware_config::detect_hardware_backend(),
        crate::media::video::hardware_config::HardwareBackend::VideoToolbox
    )
}

// =============================================================================
// Unified Encoder Selection
// =============================================================================

/// Encoder type for unified video encoding interface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncoderChoice {
    /// NVIDIA NVENC hardware encoder
    Nvenc,
    /// Apple VideoToolbox hardware encoder
    VideoToolbox,
    /// Rsmpeg native FFmpeg encoding
    RsmpegLibx264,
    /// FFmpeg CLI with libx264
    FfmpegLibx264,
}

impl EncoderChoice {
    /// Get the FFmpeg codec name (valid for `AVCodec::find_encoder_by_name`).
    pub fn name(&self) -> &'static str {
        match self {
            Self::Nvenc => "h264_nvenc",
            Self::VideoToolbox => "h264_videotoolbox",
            Self::RsmpegLibx264 => "libx264",
            Self::FfmpegLibx264 => "libx264",
        }
    }

    /// Get human-readable display name for diagnostics.
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Nvenc => "h264_nvenc",
            Self::VideoToolbox => "h264_videotoolbox",
            Self::RsmpegLibx264 => "libx264 (rsmpeg)",
            Self::FfmpegLibx264 => "libx264 (ffmpeg)",
        }
    }

    /// Get expected speedup factor vs FFmpeg CLI.
    pub fn speedup_factor(&self) -> f32 {
        match self {
            Self::Nvenc => 7.5,
            Self::VideoToolbox => 4.0,
            Self::RsmpegLibx264 => 2.5,
            Self::FfmpegLibx264 => 1.0,
        }
    }
}

/// Select the best available encoder.
///
/// Delegates to [`crate::media::video::hardware_config::detect_hardware_backend()`] as the
/// single source of truth for hardware detection.
pub fn select_best_encoder() -> EncoderChoice {
    use crate::media::video::hardware_config::{HardwareBackend, detect_hardware_backend};

    match detect_hardware_backend() {
        HardwareBackend::Nvenc => EncoderChoice::Nvenc,
        HardwareBackend::VideoToolbox => EncoderChoice::VideoToolbox,
        // QSV and VAAPI don't have EncoderChoice variants — fall through to software
        _ => {
            tracing::info!("Selected Rsmpeg native encoder (2-3x faster than FFmpeg CLI)");
            EncoderChoice::RsmpegLibx264
        }
    }
}

/// Check if specific encoder type is available.
pub fn is_encoder_available(encoder: EncoderChoice) -> bool {
    match encoder {
        EncoderChoice::Nvenc => check_nvenc_available(),
        EncoderChoice::VideoToolbox => check_videotoolbox_available(),
        EncoderChoice::RsmpegLibx264 => true,
        EncoderChoice::FfmpegLibx264 => std::process::Command::new("ffmpeg")
            .arg("-version")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false),
    }
}

/// Get all available encoders in priority order.
pub fn available_encoders() -> Vec<EncoderChoice> {
    let mut encoders = Vec::new();

    #[cfg(any(target_os = "linux", target_os = "windows"))]
    {
        if check_nvenc_available() {
            encoders.push(EncoderChoice::Nvenc);
        }
    }

    #[cfg(target_os = "macos")]
    {
        if check_videotoolbox_available() {
            encoders.push(EncoderChoice::VideoToolbox);
        }
    }

    encoders.push(EncoderChoice::RsmpegLibx264);

    if is_encoder_available(EncoderChoice::FfmpegLibx264) {
        encoders.push(EncoderChoice::FfmpegLibx264);
    }

    encoders
}

/// Print encoder selection diagnostics.
pub fn print_encoder_diagnostics() {
    let available = available_encoders();

    if available.is_empty() {
        tracing::info!(
            "=== Video Encoder Diagnostics ===\n⚠️  No encoders available! Please install FFmpeg."
        );
    } else {
        let encoder_list: Vec<String> = available
            .iter()
            .enumerate()
            .map(|(i, encoder)| {
                format!(
                    "  {}. {} - {}x speedup",
                    i + 1,
                    encoder.display_name(),
                    encoder.speedup_factor()
                )
            })
            .collect();

        tracing::info!(
            "=== Video Encoder Diagnostics ===\nAvailable encoders:\n{}\n\nSelected: {}",
            encoder_list.join("\n"),
            select_best_encoder().display_name()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encoder_choice_names() {
        assert_eq!(EncoderChoice::Nvenc.name(), "h264_nvenc");
        assert_eq!(EncoderChoice::VideoToolbox.name(), "h264_videotoolbox");
    }

    #[test]
    fn test_encoder_choice_speedup() {
        assert!(EncoderChoice::Nvenc.speedup_factor() > 5.0);
        assert!(EncoderChoice::VideoToolbox.speedup_factor() > 3.0);
        assert_eq!(EncoderChoice::FfmpegLibx264.speedup_factor(), 1.0);
    }

    #[test]
    fn test_select_best_encoder() {
        let encoder = select_best_encoder();
        // Should always return a valid encoder
        match encoder {
            EncoderChoice::Nvenc
            | EncoderChoice::VideoToolbox
            | EncoderChoice::RsmpegLibx264
            | EncoderChoice::FfmpegLibx264 => {}
        }
    }

    #[test]
    fn test_available_encoders() {
        let encoders = available_encoders();
        assert!(!encoders.is_empty());
        assert!(encoders.contains(&EncoderChoice::RsmpegLibx264));
    }

    // =============================================================================
    // Additional EncoderChoice Tests
    // =============================================================================

    #[test]
    fn test_encoder_choice_rsmpeg_name() {
        assert_eq!(EncoderChoice::RsmpegLibx264.name(), "libx264");
        assert_eq!(
            EncoderChoice::RsmpegLibx264.display_name(),
            "libx264 (rsmpeg)"
        );
    }

    #[test]
    fn test_encoder_choice_ffmpeg_name() {
        assert_eq!(EncoderChoice::FfmpegLibx264.name(), "libx264");
        assert_eq!(
            EncoderChoice::FfmpegLibx264.display_name(),
            "libx264 (ffmpeg)"
        );
    }

    #[test]
    fn test_encoder_choice_rsmpeg_speedup() {
        assert!(EncoderChoice::RsmpegLibx264.speedup_factor() > 1.0);
        assert!(EncoderChoice::RsmpegLibx264.speedup_factor() < 5.0);
    }

    #[test]
    fn test_encoder_choice_equality() {
        assert_eq!(EncoderChoice::Nvenc, EncoderChoice::Nvenc);
        assert_eq!(EncoderChoice::VideoToolbox, EncoderChoice::VideoToolbox);
        assert_ne!(EncoderChoice::Nvenc, EncoderChoice::VideoToolbox);
        assert_ne!(EncoderChoice::RsmpegLibx264, EncoderChoice::FfmpegLibx264);
    }

    #[test]
    fn test_encoder_choice_clone() {
        let encoder = EncoderChoice::Nvenc;
        let cloned = encoder; // Copy types don't need clone
        assert_eq!(encoder, cloned);
    }

    #[test]
    fn test_encoder_choice_debug() {
        let encoder = EncoderChoice::Nvenc;
        let debug_str = format!("{:?}", encoder);
        assert!(debug_str.contains("Nvenc"));
    }

    // =============================================================================
    // Availability Tests
    // =============================================================================

    #[test]
    fn test_is_encoder_available_rsmpeg() {
        // RsmpegLibx264 is always available
        assert!(is_encoder_available(EncoderChoice::RsmpegLibx264));
    }

    #[test]
    fn test_check_videotoolbox_non_macos() {
        // On non-macOS, videotoolbox should be false
        #[cfg(not(target_os = "macos"))]
        assert!(!check_videotoolbox_available());
    }

    #[test]
    fn test_check_videotoolbox_macos() {
        // On macOS with ffmpeg, videotoolbox should be detected
        #[cfg(target_os = "macos")]
        {
            // check_videotoolbox_available now delegates to detect_hardware_backend,
            // so it depends on ffmpeg actually listing h264_videotoolbox
            let vt = check_videotoolbox_available();
            let backend = crate::media::video::hardware_config::detect_hardware_backend();
            let expected = matches!(
                backend,
                crate::media::video::hardware_config::HardwareBackend::VideoToolbox
            );
            assert_eq!(
                vt, expected,
                "check_videotoolbox_available must agree with detect_hardware_backend"
            );
        }
    }

    // =========================================================================
    // Unified detection consistency tests
    // =========================================================================

    #[test]
    fn test_select_best_encoder_consistent_with_detect_hardware_backend() {
        use crate::media::video::hardware_config::{HardwareBackend, detect_hardware_backend};

        let backend = detect_hardware_backend();
        let encoder = select_best_encoder();

        match backend {
            HardwareBackend::Nvenc => assert_eq!(encoder, EncoderChoice::Nvenc),
            HardwareBackend::VideoToolbox => assert_eq!(encoder, EncoderChoice::VideoToolbox),
            // QSV / VAAPI / None all map to software
            _ => assert_eq!(encoder, EncoderChoice::RsmpegLibx264),
        }
    }

    #[test]
    fn test_is_encoder_available_videotoolbox_consistent() {
        let via_is_available = is_encoder_available(EncoderChoice::VideoToolbox);
        let via_check = check_videotoolbox_available();
        assert_eq!(
            via_is_available, via_check,
            "is_encoder_available and check_videotoolbox_available must agree"
        );
    }

    #[test]
    fn test_is_encoder_available_nvenc_consistent() {
        let via_is_available = is_encoder_available(EncoderChoice::Nvenc);
        let via_check = check_nvenc_available();
        assert_eq!(
            via_is_available, via_check,
            "is_encoder_available and check_nvenc_available must agree"
        );
    }

    #[test]
    fn test_check_nvenc_delegates_to_detect_hardware_backend() {
        use crate::media::video::hardware_config::{HardwareBackend, detect_hardware_backend};
        let backend = detect_hardware_backend();
        let nvenc = check_nvenc_available();
        assert_eq!(
            nvenc,
            matches!(backend, HardwareBackend::Nvenc),
            "check_nvenc_available must agree with detect_hardware_backend"
        );
    }

    #[test]
    fn test_check_videotoolbox_delegates_to_detect_hardware_backend() {
        use crate::media::video::hardware_config::{HardwareBackend, detect_hardware_backend};
        let backend = detect_hardware_backend();
        let vt = check_videotoolbox_available();
        assert_eq!(
            vt,
            matches!(backend, HardwareBackend::VideoToolbox),
            "check_videotoolbox_available must agree with detect_hardware_backend"
        );
    }

    #[test]
    fn test_available_encoders_includes_videotoolbox_when_detected() {
        let encoders = available_encoders();
        let vt_detected = check_videotoolbox_available();
        assert_eq!(
            encoders.contains(&EncoderChoice::VideoToolbox),
            vt_detected,
            "available_encoders must include VideoToolbox iff it is detected"
        );
    }

    #[test]
    fn test_available_encoders_includes_best_encoder() {
        let best = select_best_encoder();
        let encoders = available_encoders();
        assert!(
            encoders.contains(&best),
            "available_encoders must contain the best encoder"
        );
    }

    #[test]
    fn test_select_best_encoder_is_deterministic() {
        let first = select_best_encoder();
        let second = select_best_encoder();
        assert_eq!(
            first, second,
            "consecutive calls must return the same encoder"
        );
    }
}
