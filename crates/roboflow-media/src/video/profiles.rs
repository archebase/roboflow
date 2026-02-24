// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

use super::{HardwareConfig, VideoEncoderConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpeedPreset {
    Ultra,
    Slow,
    Medium,
    Fast,
    Faster,
    Superfast,
    Veryfast,
}

impl SpeedPreset {
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

    pub fn recommended_crf(self) -> u32 {
        match self {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QualityTier {
    High,
    Medium,
    Low,
    Prototype,
}

impl QualityTier {
    pub fn recommended_preset(self) -> SpeedPreset {
        match self {
            QualityTier::High => SpeedPreset::Fast,
            QualityTier::Medium => SpeedPreset::Faster,
            QualityTier::Low => SpeedPreset::Superfast,
            QualityTier::Prototype => SpeedPreset::Veryfast,
        }
    }

    pub fn recommended_crf(self) -> u32 {
        match self {
            QualityTier::High => 18,
            QualityTier::Medium => 23,
            QualityTier::Low => 28,
            QualityTier::Prototype => 32,
        }
    }
}

#[derive(Debug, Clone)]
pub struct VideoEncodingProfile {
    pub preset: SpeedPreset,
    pub crf: u32,
    pub hardware_accel: bool,
    pub parallel_jobs: usize,
}

impl VideoEncodingProfile {
    pub fn speed() -> Self {
        Self {
            preset: SpeedPreset::Superfast,
            crf: SpeedPreset::Superfast.recommended_crf(),
            hardware_accel: false,
            parallel_jobs: 1,
        }
    }

    pub fn quality() -> Self {
        Self {
            preset: SpeedPreset::Fast,
            crf: 18,
            hardware_accel: false,
            parallel_jobs: 1,
        }
    }

    pub fn storage() -> Self {
        Self {
            preset: SpeedPreset::Medium,
            crf: 23,
            hardware_accel: false,
            parallel_jobs: 1,
        }
    }

    pub fn prototype() -> Self {
        Self {
            preset: SpeedPreset::Veryfast,
            crf: 32,
            hardware_accel: false,
            parallel_jobs: 1,
        }
    }

    pub fn with_hardware_accel(mut self) -> Self {
        self.hardware_accel = true;
        self
    }

    pub fn with_parallel_jobs(mut self, jobs: usize) -> Self {
        self.parallel_jobs = jobs.max(1);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    Balanced,
    Speed,
    Quality,
    Storage,
    Prototype,
}

impl Profile {
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
}

#[derive(Debug, Clone)]
pub struct ResolvedConfig {
    pub codec: String,
    pub crf: u32,
    pub preset: String,
    pub pixel_format: String,
    pub hardware_accelerated: bool,
    pub parallel_jobs: usize,
}

impl ResolvedConfig {
    pub fn from_video_fields(codec: &str, crf: u32, preset: &str, profile: Option<&str>) -> Self {
        let hardware = HardwareConfig::auto_detect();

        if let Some(profile_name) = profile
            && let Some(p) = Profile::parse(profile_name)
        {
            let profile_config = p.to_encoding_profile();
            let resolved_codec = if !codec.is_empty() && codec != "libx264" {
                codec.to_string()
            } else if profile_config.hardware_accel {
                hardware.codec().to_string()
            } else {
                "libx264".to_string()
            };

            let resolved_crf = if crf == 18 { profile_config.crf } else { crf };
            let resolved_preset = if preset == "fast" {
                profile_config.preset.as_ffmpeg_preset().to_string()
            } else {
                preset.to_string()
            };

            return Self {
                codec: resolved_codec,
                crf: resolved_crf,
                preset: resolved_preset,
                pixel_format: hardware.pixel_format().to_string(),
                hardware_accelerated: hardware.is_hardware_accelerated(),
                parallel_jobs: profile_config.parallel_jobs,
            };
        }

        Self {
            codec: codec.to_string(),
            crf,
            preset: preset.to_string(),
            pixel_format: "yuv420p".to_string(),
            hardware_accelerated: false,
            parallel_jobs: 1,
        }
    }

    pub fn to_encoder_config(&self, fps: u32) -> VideoEncoderConfig {
        VideoEncoderConfig {
            codec: self.codec.clone(),
            pixel_format: self.pixel_format.clone(),
            fps,
            crf: self.crf,
            preset: self.preset.clone(),
        }
    }
}
