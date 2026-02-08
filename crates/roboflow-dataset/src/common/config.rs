// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Shared configuration types for dataset formats.
//!
//! This module defines the common configuration structures used by both
//! KPS and LeRobot dataset formats, reducing code duplication while
//! maintaining full serde compatibility.
//!
//! # Types
//!
//! - [`DatasetBaseConfig`] - Common dataset metadata (name, fps, robot_type)
//! - [`Mapping`] - Topic-to-feature mapping with type information
//! - [`MappingType`] - Superset enum of all mapping types across formats

use serde::{Deserialize, Serialize};

/// Common dataset metadata configuration.
///
/// This struct holds fields shared across KPS and LeRobot dataset configs.
/// Format-specific configs embed this via `#[serde(flatten)]`.
///
/// # TOML Example
///
/// ```toml
/// [dataset]
/// name = "my_dataset"
/// fps = 30
/// robot_type = "panda"
/// ```
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DatasetBaseConfig {
    /// Dataset name.
    pub name: String,

    /// Frames per second for the dataset.
    pub fps: u32,

    /// Robot type (optional).
    #[serde(default)]
    pub robot_type: Option<String>,
}

/// Topic-to-feature mapping configuration.
///
/// Maps a ROS/MCAP topic to a dataset feature path with type information.
/// This is the unified mapping type used by both KPS and LeRobot formats.
///
/// # TOML Example
///
/// ```toml
/// [[mappings]]
/// topic = "/camera/high"
/// feature = "observation.camera_0"
/// type = "image"
/// camera_key = "cam_high"
/// ```
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Mapping {
    /// ROS/MCAP topic name or pattern.
    pub topic: String,

    /// Dataset feature path (e.g., "observation.camera_0", "action").
    pub feature: String,

    /// Mapping type (determines how the data is processed).
    #[serde(default, alias = "type")]
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
    /// otherwise returns the full feature path.
    pub fn camera_key(&self) -> String {
        self.camera_key
            .clone()
            .unwrap_or_else(|| self.feature.clone())
    }
}

/// Type of data being mapped.
///
/// This is the superset of all mapping types across KPS and LeRobot formats.
/// - Common: Image, State, Action, Timestamp
/// - KPS-specific: OtherSensor, Audio
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum MappingType {
    /// Image data (camera).
    Image,
    /// State/joint data.
    #[default]
    State,
    /// Action data.
    Action,
    /// Timestamp data.
    Timestamp,
    /// Other sensor data (IMU, force, etc.). KPS-specific.
    OtherSensor,
    /// Audio data. KPS-specific.
    Audio,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dataset_base_config_deserialize() {
        let toml_str = r#"
name = "test_dataset"
fps = 30
robot_type = "panda"
"#;
        let config: DatasetBaseConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.name, "test_dataset");
        assert_eq!(config.fps, 30);
        assert_eq!(config.robot_type, Some("panda".to_string()));
    }

    #[test]
    fn test_dataset_base_config_optional_robot_type() {
        let toml_str = r#"
name = "test"
fps = 60
"#;
        let config: DatasetBaseConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.robot_type, None);
    }

    #[test]
    fn test_mapping_deserialize_with_type_alias() {
        let toml_str = r#"
topic = "/camera/high"
feature = "observation.camera_0"
type = "image"
"#;
        let mapping: Mapping = toml::from_str(toml_str).unwrap();
        assert_eq!(mapping.topic, "/camera/high");
        assert_eq!(mapping.feature, "observation.camera_0");
        assert_eq!(mapping.mapping_type, MappingType::Image);
        assert_eq!(mapping.camera_key, None);
    }

    #[test]
    fn test_mapping_deserialize_with_mapping_type() {
        let toml_str = r#"
topic = "/joint_states"
feature = "observation.state"
mapping_type = "state"
"#;
        let mapping: Mapping = toml::from_str(toml_str).unwrap();
        assert_eq!(mapping.mapping_type, MappingType::State);
    }

    #[test]
    fn test_mapping_with_camera_key() {
        let toml_str = r#"
topic = "/cam_l/color"
feature = "observation.images.cam_left"
type = "image"
camera_key = "left_camera"
"#;
        let mapping: Mapping = toml::from_str(toml_str).unwrap();
        assert_eq!(mapping.camera_key(), "left_camera");
    }

    #[test]
    fn test_mapping_camera_key_defaults_to_feature() {
        let toml_str = r#"
topic = "/cam_h/color"
feature = "observation.images.cam_high"
type = "image"
"#;
        let mapping: Mapping = toml::from_str(toml_str).unwrap();
        assert_eq!(mapping.camera_key(), "observation.images.cam_high");
    }

    #[test]
    fn test_default_mapping_type() {
        let toml_str = r#"
topic = "/joint_states"
feature = "observation.state"
"#;
        let mapping: Mapping = toml::from_str(toml_str).unwrap();
        assert_eq!(mapping.mapping_type, MappingType::State);
    }

    #[test]
    fn test_kps_specific_mapping_types() {
        let toml_str = r#"
topic = "/imu"
feature = "observation.imu"
type = "othersensor"
"#;
        let mapping: Mapping = toml::from_str(toml_str).unwrap();
        assert_eq!(mapping.mapping_type, MappingType::OtherSensor);

        let toml_str = r#"
topic = "/audio"
feature = "observation.audio"
type = "audio"
"#;
        let mapping: Mapping = toml::from_str(toml_str).unwrap();
        assert_eq!(mapping.mapping_type, MappingType::Audio);
    }

    #[test]
    fn test_mapping_type_variants() {
        assert_eq!(MappingType::default(), MappingType::State);
    }
}
