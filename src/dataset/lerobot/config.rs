// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! LeRobot dataset configuration.
//!
//! Configuration for converting ROS bag data to LeRobot v2.1 format.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use serde::Deserialize;

use crate::core::Result;

/// LeRobot dataset configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct LerobotConfig {
    /// Dataset metadata
    pub dataset: DatasetConfig,

    /// Topic to feature mappings
    #[serde(default)]
    pub mappings: Vec<Mapping>,

    /// Video encoding options
    #[serde(default)]
    pub video: VideoConfig,

    /// Path to JSON annotation file for episode segmentation
    #[serde(default)]
    pub annotation_file: Option<String>,
}

impl LerobotConfig {
    /// Load configuration from a TOML file.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        Self::from_toml(&content)
    }

    /// Parse configuration from a TOML string.
    pub fn from_toml(toml_str: &str) -> Result<Self> {
        let config: LerobotConfig = toml::from_str(toml_str).map_err(|e| {
            crate::RoboflowError::parse("LerobotConfig", format!("TOML parse error: {}", e))
        })?;
        Ok(config)
    }

    /// Get mappings by topic.
    pub fn mappings_by_topic(&self) -> HashMap<String, Mapping> {
        let mut map = HashMap::new();
        for mapping in &self.mappings {
            map.insert(mapping.topic.clone(), mapping.clone());
        }
        map
    }

    /// Get camera mappings (observation.images.*).
    pub fn camera_mappings(&self) -> Vec<&Mapping> {
        self.mappings
            .iter()
            .filter(|m| m.feature.starts_with("observation.images."))
            .collect()
    }

    /// Get state mappings (observation.state).
    pub fn state_mappings(&self) -> Vec<&Mapping> {
        self.mappings
            .iter()
            .filter(|m| m.feature == "observation.state")
            .collect()
    }

    /// Get action mappings.
    pub fn action_mappings(&self) -> Vec<&Mapping> {
        self.mappings
            .iter()
            .filter(|m| m.feature.starts_with("action."))
            .collect()
    }
}

/// Dataset metadata configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct DatasetConfig {
    /// Dataset name
    pub name: String,

    /// Frames per second for the dataset
    pub fps: u32,

    /// Robot type (optional, can be inferred from annotations)
    #[serde(default)]
    pub robot_type: Option<String>,

    /// Environment type (optional)
    #[serde(default)]
    pub env_type: Option<String>,
}

/// Topic to LeRobot feature mapping.
#[derive(Debug, Clone, Deserialize)]
pub struct Mapping {
    /// ROS topic name
    pub topic: String,

    /// LeRobot feature path (e.g., "observation.images.cam_high")
    pub feature: String,

    /// Mapping type
    #[serde(default)]
    pub mapping_type: MappingType,
}

/// Type of data being mapped.
#[derive(Debug, Clone, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum MappingType {
    /// Image data (camera)
    Image,
    /// State/joint data
    #[default]
    State,
    /// Action data
    Action,
    /// Timestamp data
    Timestamp,
}

/// Video encoding configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct VideoConfig {
    /// Video codec (default: libx264)
    #[serde(default = "default_codec")]
    pub codec: String,

    /// CRF quality (lower = better, 0-51, default: 18)
    #[serde(default = "default_crf")]
    pub crf: u32,

    /// Encoder preset (default: fast)
    #[serde(default = "default_preset")]
    pub preset: String,

    /// Optional profile name (speed, quality, balanced, storage, prototype)
    ///
    /// When specified, overrides codec/crf/preset with profile defaults.
    /// Explicit codec/crf/preset settings can override profile values.
    #[serde(default)]
    pub profile: Option<String>,
}

impl Default for VideoConfig {
    fn default() -> Self {
        Self {
            codec: default_codec(),
            crf: default_crf(),
            preset: default_preset(),
            profile: None,
        }
    }
}

fn default_codec() -> String {
    "libx264".to_string()
}

fn default_crf() -> u32 {
    18
}

fn default_preset() -> String {
    "fast".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_config() {
        let toml = r#"
[dataset]
name = "test_dataset"
fps = 30

[[mappings]]
topic = "/cam_h/color/image_raw/compressed"
feature = "observation.images.cam_high"
mapping_type = "image"

[[mappings]]
topic = "/kuavo_arm_traj"
feature = "observation.state"
mapping_type = "state"

[[mappings]]
topic = "/joint_cmd"
feature = "action"
mapping_type = "action"

[video]
codec = "libx264"
crf = 18
"#;

        let config: LerobotConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.dataset.name, "test_dataset");
        assert_eq!(config.dataset.fps, 30);
        assert_eq!(config.mappings.len(), 3);
        assert_eq!(config.mappings[0].feature, "observation.images.cam_high");
        assert_eq!(config.video.codec, "libx264");
        assert_eq!(config.video.crf, 18);
    }

    #[test]
    fn test_camera_mappings() {
        let toml = r#"
[dataset]
name = "test"
fps = 30

[[mappings]]
topic = "/cam_h/color"
feature = "observation.images.cam_high"
mapping_type = "image"

[[mappings]]
topic = "/joint_states"
feature = "observation.state"
"#;

        let config: LerobotConfig = toml::from_str(toml).unwrap();
        let cameras = config.camera_mappings();
        assert_eq!(cameras.len(), 1);
        assert_eq!(cameras[0].feature, "observation.images.cam_high");
    }
}
