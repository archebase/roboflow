// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Video encoding optimization strategies.
//!
//! This module provides optimized video encoding configurations
//! for different use cases (speed vs quality vs size).

use crate::lerobot::config::VideoConfig;
use roboflow_video::HardwareConfig;

/// Video encoding preset - trades encoding speed for compression efficiency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpeedPreset {
    /// Best compression, slowest encoding (not recommended for batch processing)
    Ultra,
    /// Good compression, slow encoding
    Slow,
    /// Balanced speed/compression (default)
    Medium,
    /// Fast encoding, good compression
    Fast,
    /// Very fast encoding, lower compression
    Faster,
    /// Super fast encoding, lowest compression (recommended for speed)
    Superfast,
    /// Real-time encoding (lowest quality)
    Veryfast,
}

impl SpeedPreset {
    /// Get the ffmpeg preset name
    pub fn as_ffmpeg_preset(self) -> &'static str {
        match self {
            SpeedPreset::Ultra => "veryslow",
            SpeedPreset::Slow => "slower",
            SpeedPreset::Medium => "medium",
            SpeedPreset::Fast => "fast",
            SpeedPreset::Faster => "faster",
            SpeedPreset::Superfast => "superfast",
            SpeedPreset::Veryfast => "veryfast",
        }
    }

    /// Get recommended CRF value for this preset
    pub fn recommended_crf(self) -> u32 {
        match self {
            // Better presets can use lower CRF for same quality
            SpeedPreset::Ultra => 18,
            SpeedPreset::Slow => 19,
            SpeedPreset::Medium => 20,
            SpeedPreset::Fast => 22,
            SpeedPreset::Faster => 24,
            SpeedPreset::Superfast => 26,
            SpeedPreset::Veryfast => 28,
        }
    }
}

/// Video encoding quality tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QualityTier {
    /// Maximum quality, largest files, slowest
    High,
    /// Good quality for training, balanced size
    Medium,
    /// Compressed for storage/bandwidth
    Low,
    /// Maximum compression for prototyping
    Prototype,
}

impl QualityTier {
    /// Get recommended speed preset for this quality tier
    pub fn recommended_preset(self) -> SpeedPreset {
        match self {
            QualityTier::High => SpeedPreset::Fast,
            QualityTier::Medium => SpeedPreset::Faster,
            QualityTier::Low => SpeedPreset::Superfast,
            QualityTier::Prototype => SpeedPreset::Veryfast,
        }
    }

    /// Get recommended CRF for this quality tier
    pub fn recommended_crf(self) -> u32 {
        match self {
            QualityTier::High => 18,
            QualityTier::Medium => 23,
            QualityTier::Low => 28,
            QualityTier::Prototype => 32,
        }
    }
}

/// Optimized video encoding configuration.
#[derive(Debug, Clone)]
pub struct VideoEncodingProfile {
    /// Speed preset
    pub preset: SpeedPreset,

    /// CRF quality (0-51, lower = better, 18-28 is typical range)
    pub crf: u32,

    /// Whether to use hardware acceleration
    pub hardware_accel: bool,

    /// Number of parallel encoding jobs
    pub parallel_jobs: usize,
}

impl VideoEncodingProfile {
    /// Create a new profile with explicit settings
    pub fn new(preset: SpeedPreset, crf: u32) -> Self {
        Self {
            preset,
            crf,
            hardware_accel: false,
            parallel_jobs: 1,
        }
    }

    /// Create a profile optimized for speed
    pub fn speed() -> Self {
        Self {
            preset: SpeedPreset::Superfast,
            crf: SpeedPreset::Superfast.recommended_crf(),
            hardware_accel: false,
            parallel_jobs: 1,
        }
    }

    /// Create a profile optimized for quality
    pub fn quality() -> Self {
        Self {
            preset: SpeedPreset::Fast,
            crf: 18,
            hardware_accel: false,
            parallel_jobs: 1,
        }
    }

    /// Create a profile optimized for storage
    pub fn storage() -> Self {
        Self {
            preset: SpeedPreset::Medium,
            crf: 23,
            hardware_accel: false,
            parallel_jobs: 1,
        }
    }

    /// Create a profile for prototyping (fastest, lowest quality)
    pub fn prototype() -> Self {
        Self {
            preset: SpeedPreset::Veryfast,
            crf: 32,
            hardware_accel: false,
            parallel_jobs: 1,
        }
    }

    /// Enable hardware acceleration (if available)
    pub fn with_hardware_accel(mut self) -> Self {
        self.hardware_accel = true;
        self
    }

    /// Set number of parallel encoding jobs
    pub fn with_parallel_jobs(mut self, jobs: usize) -> Self {
        self.parallel_jobs = jobs.max(1);
        self
    }

    /// Convert to TOML configuration string
    pub fn to_toml_table(&self) -> String {
        format!(
            r#"[video]
preset = "{}"
crf = {}
"#,
            self.preset.as_ffmpeg_preset(),
            self.crf
        )
    }
}

/// Predefined encoding profiles for common use cases.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    /// Balanced speed/quality - best for most use cases
    Balanced,
    /// Maximum speed - lowest quality, largest files
    Speed,
    /// Maximum quality - slowest encoding, best compression
    Quality,
    /// Compressed for storage - medium speed, smaller files
    Storage,
    /// Fast prototyping - fastest, lowest quality
    Prototype,
}

impl Profile {
    /// Get the VideoEncodingProfile for this profile.
    pub fn to_encoding_profile(self) -> VideoEncodingProfile {
        match self {
            Profile::Balanced => VideoEncodingProfile {
                preset: SpeedPreset::Faster,
                crf: 23,
                hardware_accel: true,
                parallel_jobs: num_cpus::get(),
            },
            Profile::Speed => VideoEncodingProfile::speed()
                .with_hardware_accel()
                .with_parallel_jobs(num_cpus::get()),
            Profile::Quality => VideoEncodingProfile::quality()
                .with_hardware_accel()
                .with_parallel_jobs(num_cpus::get()),
            Profile::Storage => VideoEncodingProfile::storage()
                .with_hardware_accel()
                .with_parallel_jobs(num_cpus::get()),
            Profile::Prototype => VideoEncodingProfile::prototype(),
        }
    }

    /// Parse from string.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "balanced" => Some(Profile::Balanced),
            "speed" => Some(Profile::Speed),
            "quality" => Some(Profile::Quality),
            "storage" => Some(Profile::Storage),
            "prototype" => Some(Profile::Prototype),
            _ => None,
        }
    }
}

/// Resolved video encoding configuration.
///
/// This combines the VideoConfig from TOML with profile settings
/// and hardware acceleration detection to produce the final
/// encoding configuration.
#[derive(Debug, Clone)]
pub struct ResolvedConfig {
    /// The codec to use (e.g., "libx264", "h264_videotoolbox")
    pub codec: String,

    /// The CRF quality value
    pub crf: u32,

    /// The ffmpeg preset
    pub preset: String,

    /// The pixel format
    pub pixel_format: String,

    /// Whether hardware acceleration is enabled
    pub hardware_accelerated: bool,

    /// Number of parallel encoding jobs
    pub parallel_jobs: usize,
}

impl ResolvedConfig {
    /// Resolve the final configuration from VideoConfig.
    ///
    /// This function:
    /// 1. Checks if a profile is specified
    /// 2. Applies profile settings if present
    /// 3. Resolves hardware acceleration
    /// 4. Applies any explicit overrides from VideoConfig
    pub fn from_video_config(video_config: &VideoConfig) -> Self {
        let hardware = HardwareConfig::auto_detect();

        // Check if profile is specified
        if let Some(profile) = &video_config.profile
            && let Some(p) = Profile::parse(profile)
        {
            let profile_config = p.to_encoding_profile();

            // If codec is explicitly set (not default), use it instead of profile's codec
            let codec = if !video_config.codec.is_empty() && video_config.codec != "libx264" {
                video_config.codec.clone()
            } else if profile_config.hardware_accel {
                hardware.codec().to_string()
            } else {
                "libx264".to_string()
            };

            // For CRF: use profile default if crf is at the default value (18)
            // This allows users to override by setting a different crf
            let use_profile_crf = video_config.crf == 18; // 18 is the default in config
            let crf = if use_profile_crf {
                profile_config.crf
            } else {
                video_config.crf
            };

            // For preset: use profile default if preset is at default "fast"
            let use_profile_preset = video_config.preset == "fast";
            let preset = if use_profile_preset {
                profile_config.preset.as_ffmpeg_preset().to_string()
            } else {
                video_config.preset.clone()
            };

            return Self {
                codec,
                crf,
                preset,
                pixel_format: hardware.pixel_format().to_string(),
                hardware_accelerated: hardware.is_hardware_accelerated(),
                parallel_jobs: profile_config.parallel_jobs,
            };
        }

        // No profile or invalid profile - use explicit settings
        Self {
            codec: video_config.codec.clone(),
            crf: video_config.crf,
            preset: video_config.preset.clone(),
            pixel_format: "yuv420p".to_string(),
            hardware_accelerated: false,
            parallel_jobs: 1,
        }
    }

    /// Create a VideoEncoderConfig from this resolved config.
    pub fn to_encoder_config(&self, fps: u32) -> crate::common::video::VideoEncoderConfig {
        crate::common::video::VideoEncoderConfig {
            codec: self.codec.clone(),
            pixel_format: self.pixel_format.clone(),
            fps,
            crf: self.crf,
            preset: self.preset.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use roboflow_video::{HardwareBackend, HardwareConfig};

    #[test]
    fn test_preset_names() {
        assert_eq!(SpeedPreset::Superfast.as_ffmpeg_preset(), "superfast");
        assert_eq!(SpeedPreset::Fast.as_ffmpeg_preset(), "fast");
    }

    #[test]
    fn test_recommended_crf() {
        assert_eq!(SpeedPreset::Superfast.recommended_crf(), 26);
        assert_eq!(SpeedPreset::Fast.recommended_crf(), 22);
    }

    #[test]
    fn test_profiles() {
        let speed = VideoEncodingProfile::speed();
        assert_eq!(speed.preset, SpeedPreset::Superfast);
        assert_eq!(speed.crf, 26);

        let quality = VideoEncodingProfile::quality();
        assert_eq!(quality.preset, SpeedPreset::Fast);
        assert_eq!(quality.crf, 18);
    }

    #[test]
    fn test_profile_from_str() {
        assert_eq!(Profile::parse("speed"), Some(Profile::Speed));
        assert_eq!(Profile::parse("quality"), Some(Profile::Quality));
        assert_eq!(Profile::parse("balanced"), Some(Profile::Balanced));
        assert_eq!(Profile::parse("storage"), Some(Profile::Storage));
        assert_eq!(Profile::parse("prototype"), Some(Profile::Prototype));
        assert_eq!(Profile::parse("invalid"), None);
    }

    #[test]
    fn test_profile_from_str_case_insensitive() {
        assert_eq!(Profile::parse("SPEED"), Some(Profile::Speed));
        assert_eq!(Profile::parse("Quality"), Some(Profile::Quality));
        assert_eq!(Profile::parse("BALANCED"), Some(Profile::Balanced));
    }

    #[test]
    fn test_hardware_backend_codec_names() {
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
    fn test_hardware_backend_is_hardware() {
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

    #[test]
    fn test_video_encoding_profile_builder() {
        let profile = VideoEncodingProfile::new(SpeedPreset::Fast, 20)
            .with_hardware_accel()
            .with_parallel_jobs(4);

        assert_eq!(profile.preset, SpeedPreset::Fast);
        assert_eq!(profile.crf, 20);
        assert!(profile.hardware_accel);
        assert_eq!(profile.parallel_jobs, 4);
    }

    #[test]
    fn test_video_encoding_profile_parallel_jobs_min() {
        let profile = VideoEncodingProfile::speed().with_parallel_jobs(0);

        // Should be at least 1
        assert_eq!(profile.parallel_jobs, 1);
    }

    #[test]
    fn test_quality_tier_recommended_preset() {
        assert_eq!(QualityTier::High.recommended_preset(), SpeedPreset::Fast);
        assert_eq!(
            QualityTier::Medium.recommended_preset(),
            SpeedPreset::Faster
        );
        assert_eq!(
            QualityTier::Low.recommended_preset(),
            SpeedPreset::Superfast
        );
        assert_eq!(
            QualityTier::Prototype.recommended_preset(),
            SpeedPreset::Veryfast
        );
    }

    #[test]
    fn test_quality_tier_recommended_crf() {
        assert_eq!(QualityTier::High.recommended_crf(), 18);
        assert_eq!(QualityTier::Medium.recommended_crf(), 23);
        assert_eq!(QualityTier::Low.recommended_crf(), 28);
        assert_eq!(QualityTier::Prototype.recommended_crf(), 32);
    }
}
