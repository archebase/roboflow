// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! LeRobot dataset format support.
//!
//! This module provides conversion from ROS bag files to LeRobot v2.1 dataset format.
//! LeRobot is Hugging Face's robotics learning dataset format.

pub mod annotations;
pub mod config;
pub mod episode;
pub mod factory;
pub mod format_writer_impl;
pub mod metadata;
pub mod trait_impl;
pub mod upload;
pub mod video_profiles;
pub mod writer;

pub use annotations::{AnnotationData, SkillMark};
pub use config::{
    DatasetConfig, FlushingConfig, LerobotConfig, Mapping, MappingType, StreamingConfig,
    VideoConfig,
};
pub use episode::{
    CalibrationWriter, EpisodeAction, EpisodeTracker, apply_camera_calibration,
    convert_camera_calibration, convert_camera_extrinsic, convert_camera_intrinsic,
};
// Re-export hardware types from roboflow-video
pub use crate::media::video::{HardwareBackend, HardwareConfig};
pub use trait_impl::{FromAlignedFrame, LerobotWriterTrait};

pub use factory::{LerobotWriterConfig, LerobotWriterResult, create_lerobot_writer};
pub use upload::EpisodeUploadCoordinator;
pub use upload::{EpisodeFiles, UploadConfig, UploadProgress, UploadStats};
pub use video_profiles::{Profile, QualityTier, ResolvedConfig, SpeedPreset, VideoEncodingProfile};
pub use writer::{CameraExtrinsic, CameraIntrinsic, EpisodeWriter, LerobotFrame, LerobotWriter};
