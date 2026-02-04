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

use roboflow_core::Result;

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
            roboflow_core::RoboflowError::parse("LerobotConfig", format!("TOML parse error: {}", e))
        })?;
        config.validate()?;
        Ok(config)
    }

    /// Validate the configuration for semantic correctness.
    fn validate(&self) -> Result<()> {
        // Validate FPS > 0
        if self.dataset.fps == 0 {
            return Err(roboflow_core::RoboflowError::parse(
                "LerobotConfig",
                "dataset.fps must be greater than 0",
            ));
        }

        // Validate CRF range (0-51 for H.264)
        if self.video.crf > 51 {
            return Err(roboflow_core::RoboflowError::parse(
                "LerobotConfig",
                format!("video.crf ({}) must be in range [0-51]", self.video.crf),
            ));
        }

        // Check for duplicate topics
        use std::collections::HashSet;
        let mut topics = HashSet::new();
        for mapping in &self.mappings {
            if !topics.insert(&mapping.topic) {
                return Err(roboflow_core::RoboflowError::parse(
                    "LerobotConfig",
                    format!("Duplicate topic found: {}", mapping.topic),
                ));
            }
        }

        Ok(())
    }

    /// Get mappings by topic.
    pub fn mappings_by_topic(&self) -> HashMap<String, Mapping> {
        let mut map = HashMap::new();
        for mapping in &self.mappings {
            map.insert(mapping.topic.clone(), mapping.clone());
        }
        map
    }

    /// Get image mappings (any feature with mapping_type = "image").
    ///
    /// This is config-driven and works with any feature naming convention.
    /// Use camera_key in mappings to override the default (full feature path).
    pub fn camera_mappings(&self) -> Vec<&Mapping> {
        self.mappings
            .iter()
            .filter(|m| m.mapping_type == MappingType::Image)
            .collect()
    }

    /// Get state mappings (mapping_type = "state").
    ///
    /// This is config-driven and works with any feature naming convention.
    pub fn state_mappings(&self) -> Vec<&Mapping> {
        self.mappings
            .iter()
            .filter(|m| m.mapping_type == MappingType::State)
            .collect()
    }

    /// Get action mappings (mapping_type = "action").
    ///
    /// This is config-driven and works with any feature naming convention.
    pub fn action_mappings(&self) -> Vec<&Mapping> {
        self.mappings
            .iter()
            .filter(|m| m.mapping_type == MappingType::Action)
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

    /// Camera key for video directory naming (optional).
    ///
    /// If not specified, defaults to using the full feature path.
    /// For example, feature="observation.images.cam_high" -> camera_key="observation.images.cam_high".
    ///
    /// Use this when you want a different camera key than the full feature path.
    #[serde(default)]
    pub camera_key: Option<String>,
}

impl Mapping {
    /// Get the camera key for this mapping.
    ///
    /// Returns the explicitly configured `camera_key` if set,
    /// otherwise returns the full feature path (config-driven, works with any naming).
    ///
    /// This allows flexible feature naming (e.g., "observation.images.cam_high",
    /// "obsv.images.cam_r", "my.camera") without hard-coded prefix assumptions.
    pub fn camera_key(&self) -> String {
        self.camera_key
            .clone()
            .unwrap_or_else(|| self.feature.clone())
    }
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

    #[test]
    fn test_camera_key_derivation() {
        let toml = r#"
[dataset]
name = "test"
fps = 30

[[mappings]]
topic = "/cam_h/color"
feature = "observation.images.cam_high"
mapping_type = "image"

[[mappings]]
topic = "/cam_l/color"
feature = "observation.images.cam_left"
mapping_type = "image"
camera_key = "left_camera"

[[mappings]]
topic = "/joint_states"
feature = "observation.state"
"#;

        let config: LerobotConfig = toml::from_str(toml).unwrap();
        let cameras = config.camera_mappings();
        assert_eq!(cameras.len(), 2);

        // First camera: no explicit camera_key, so returns full feature path
        assert_eq!(cameras[0].camera_key(), "observation.images.cam_high");

        // Second camera: explicit camera_key overrides the default
        assert_eq!(cameras[1].camera_key(), "left_camera");
        assert_eq!(cameras[1].feature, "observation.images.cam_left");
    }
}
