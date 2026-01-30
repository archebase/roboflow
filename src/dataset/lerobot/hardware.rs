// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Hardware acceleration detection and configuration for video encoding.
//!
//! This module provides automatic detection and configuration of hardware
//! video encoders available on the current platform.

use std::process::Command;

/// Hardware acceleration backend for video encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HardwareBackend {
    /// No hardware acceleration (software encoding)
    None,
    /// VideoToolbox (macOS/iOS)
    VideoToolbox,
    /// NVIDIA NVENC (Windows/Linux)
    Nvenc,
    /// Intel Quick Sync Video (Windows/Linux)
    Qsv,
    /// Video Acceleration API (Linux)
    Vaapi,
}

impl HardwareBackend {
    /// Get the ffmpeg codec name for this backend.
    pub fn codec_name(self) -> &'static str {
        match self {
            HardwareBackend::None => "libx264",
            HardwareBackend::VideoToolbox => "h264_videotoolbox",
            HardwareBackend::Nvenc => "h264_nvenc",
            HardwareBackend::Qsv => "h264_qsv",
            HardwareBackend::Vaapi => "h264_vaapi",
        }
    }

    /// Get the pixel format for this backend.
    pub fn pixel_format(self) -> &'static str {
        match self {
            HardwareBackend::None => "yuv420p",
            HardwareBackend::VideoToolbox => "yuv420p",
            HardwareBackend::Nvenc => "yuv420p",
            HardwareBackend::Qsv => "yuv420p",
            HardwareBackend::Vaapi => "yuv420p",
        }
    }

    /// Check if this is a hardware backend.
    pub fn is_hardware(self) -> bool {
        !matches!(self, HardwareBackend::None)
    }

    /// Get human-readable name.
    pub fn name(self) -> &'static str {
        match self {
            HardwareBackend::None => "Software (libx264)",
            HardwareBackend::VideoToolbox => "VideoToolbox (macOS)",
            HardwareBackend::Nvenc => "NVENC (NVIDIA)",
            HardwareBackend::Qsv => "Quick Sync (Intel)",
            HardwareBackend::Vaapi => "VAAPI (Linux)",
        }
    }
}

/// Detect the best available hardware acceleration backend.
///
/// This function checks for available hardware encoders in order of preference:
/// 1. NVENC (NVIDIA GPUs) - best performance on NVIDIA GPUs
/// 2. QSV (Intel GPUs) - good performance on Intel hardware
/// 3. VAAPI (Linux) - hardware acceleration on Linux
/// 4. Software (libx264) - fallback
///
/// Note: VideoToolbox is not auto-detected by default due to compatibility issues.
/// Users can explicitly enable it via `HardwareConfig::with_backend(HardwareBackend::VideoToolbox)`.
pub fn detect_hardware_backend() -> HardwareBackend {
    // Check NVENC (NVIDIA GPUs - cross-platform) first
    // This is more reliable than VideoToolbox and works on multiple platforms
    if check_encoder_available("h264_nvenc") {
        tracing::info!("Detected NVENC hardware acceleration");
        return HardwareBackend::Nvenc;
    }

    // Check QSV (Intel Quick Sync - Windows/Linux)
    if check_encoder_available("h264_qsv") {
        tracing::info!("Detected Intel Quick Sync hardware acceleration");
        return HardwareBackend::Qsv;
    }

    // Check VAAPI (Linux only)
    if cfg!(target_os = "linux") && check_encoder_available("h264_vaapi") {
        tracing::info!("Detected VAAPI hardware acceleration");
        return HardwareBackend::Vaapi;
    }

    // Note: We skip VideoToolbox auto-detection because:
    // 1. It often fails with error -12903 in containerized/VM environments
    // 2. It has compatibility issues with certain input formats
    // Users can explicitly enable it if needed

    tracing::info!("No hardware acceleration detected, using software encoding");
    HardwareBackend::None
}

/// Check if a specific ffmpeg encoder is available.
fn check_encoder_available(encoder: &str) -> bool {
    let result = Command::new("ffmpeg")
        .arg("-hide_banner")
        .arg("-encoders")
        .output();

    match result {
        Ok(output) => {
            let output_str = String::from_utf8_lossy(&output.stdout);
            output_str.contains(encoder)
        }
        Err(_) => false,
    }
}

/// Hardware acceleration configuration.
#[derive(Debug, Clone)]
pub struct HardwareConfig {
    /// The detected or configured backend
    pub backend: HardwareBackend,

    /// Whether to auto-detect the backend
    pub auto_detect: bool,

    /// Override codec (if set, ignores backend)
    pub codec_override: Option<String>,
}

impl Default for HardwareConfig {
    fn default() -> Self {
        Self {
            backend: detect_hardware_backend(),
            auto_detect: true,
            codec_override: None,
        }
    }
}

impl HardwareConfig {
    /// Create a new hardware config with auto-detection.
    pub fn auto_detect() -> Self {
        Self::default()
    }

    /// Create a config with a specific backend.
    pub fn with_backend(backend: HardwareBackend) -> Self {
        Self {
            backend,
            auto_detect: false,
            codec_override: None,
        }
    }

    /// Create a config with a codec override.
    pub fn with_codec(codec: String) -> Self {
        Self {
            backend: HardwareBackend::None,
            auto_detect: false,
            codec_override: Some(codec),
        }
    }

    /// Disable hardware acceleration.
    pub fn software_only() -> Self {
        Self {
            backend: HardwareBackend::None,
            auto_detect: false,
            codec_override: None,
        }
    }

    /// Get the effective codec name.
    pub fn codec(&self) -> &str {
        self.codec_override
            .as_deref()
            .unwrap_or_else(|| self.backend.codec_name())
    }

    /// Get the pixel format.
    pub fn pixel_format(&self) -> &str {
        self.backend.pixel_format()
    }

    /// Check if using hardware acceleration.
    pub fn is_hardware_accelerated(&self) -> bool {
        self.codec_override
            .as_ref()
            .map(|c| !c.starts_with("libx"))
            .unwrap_or_else(|| self.backend.is_hardware())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backend_codec_names() {
        assert_eq!(HardwareBackend::None.codec_name(), "libx264");
        assert_eq!(
            HardwareBackend::VideoToolbox.codec_name(),
            "h264_videotoolbox"
        );
        assert_eq!(HardwareBackend::Nvenc.codec_name(), "h264_nvenc");
        assert_eq!(HardwareBackend::Qsv.codec_name(), "h264_qsv");
        assert_eq!(HardwareBackend::Vaapi.codec_name(), "h264_vaapi");
    }

    #[test]
    fn test_backend_is_hardware() {
        assert!(!HardwareBackend::None.is_hardware());
        assert!(HardwareBackend::VideoToolbox.is_hardware());
        assert!(HardwareBackend::Nvenc.is_hardware());
        assert!(HardwareBackend::Qsv.is_hardware());
        assert!(HardwareBackend::Vaapi.is_hardware());
    }

    #[test]
    fn test_hardware_config_default() {
        let config = HardwareConfig::default();
        // Auto-detect will run, but we just check the struct is valid
        assert!(config.auto_detect);
    }

    #[test]
    fn test_hardware_config_with_backend() {
        let config = HardwareConfig::with_backend(HardwareBackend::Nvenc);
        assert_eq!(config.backend, HardwareBackend::Nvenc);
        assert!(!config.auto_detect);
        assert_eq!(config.codec(), "h264_nvenc");
    }

    #[test]
    fn test_hardware_config_with_codec() {
        let config = HardwareConfig::with_codec("custom_codec".to_string());
        assert_eq!(config.codec(), "custom_codec");
        assert!(config.is_hardware_accelerated());
    }

    #[test]
    fn test_hardware_config_software_only() {
        let config = HardwareConfig::software_only();
        assert_eq!(config.backend, HardwareBackend::None);
        assert!(!config.is_hardware_accelerated());
        assert_eq!(config.codec(), "libx264");
    }
}
