// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

pub use crate::formats::common::LeRobotVideoPathScheme as LerobotVideoPathScheme;
use crate::formats::lerobot::config::VideoConfig;

pub use roboflow_media::video::{
    Profile, QualityTier, ResolvedConfig, SpeedPreset, VideoEncodingProfile,
};

pub fn resolve_video_config(video_config: &VideoConfig) -> ResolvedConfig {
    ResolvedConfig::from_video_fields(
        &video_config.codec,
        video_config.crf,
        &video_config.preset,
        video_config.profile.as_deref(),
    )
}
