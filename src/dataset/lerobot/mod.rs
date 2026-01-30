// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! LeRobot dataset format support.
//!
//! This module provides conversion from ROS bag files to LeRobot v2.1 dataset format.
//! LeRobot is Hugging Face's robotics learning dataset format.

pub mod annotations;
pub mod config;
pub mod hardware;
pub mod metadata;
pub mod trait_impl;
pub mod video_profiles;
pub mod writer;

pub use annotations::{AnnotationData, SkillMark};
pub use config::{DatasetConfig, LerobotConfig, Mapping, MappingType, VideoConfig};
pub use hardware::{HardwareBackend, HardwareConfig};
pub use trait_impl::{FromAlignedFrame, LerobotWriterTrait};
pub use video_profiles::{Profile, QualityTier, ResolvedConfig, SpeedPreset, VideoEncodingProfile};
pub use writer::{LerobotFrame, LerobotWriter};
