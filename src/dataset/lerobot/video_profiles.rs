// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Video encoding optimization strategies.
//!
//! This module provides optimized video encoding configurations
//! for different use cases (speed vs quality vs size).

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
