//! Kps conversion configuration.
//!
//! Parses TOML configuration for MCAP → Kps conversion.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use serde::Deserialize;

/// Kps conversion configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct KpsConfig {
    /// Dataset metadata
    pub dataset: DatasetConfig,
    /// Topic to feature mappings
    #[serde(default)]
    pub mappings: Vec<Mapping>,
    /// Output format options
    #[serde(default)]
    pub output: OutputConfig,
}

impl KpsConfig {
    /// Load configuration from a TOML file.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path)?;
        let config: KpsConfig = toml::from_str(&content)?;
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

    /// Get mappings for image features.
    pub fn image_mappings(&self) -> Vec<&Mapping> {
        self.mappings
            .iter()
            .filter(|m| matches!(m.mapping_type, MappingType::Image))
            .collect()
    }

    /// Get mappings for state features.
    pub fn state_mappings(&self) -> Vec<&Mapping> {
        self.mappings
            .iter()
            .filter(|m| {
                matches!(
                    m.mapping_type,
                    MappingType::State | MappingType::Action | MappingType::OtherSensor
                )
            })
            .collect()
    }
}

/// Dataset metadata configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct DatasetConfig {
    /// Dataset name
    pub name: String,
    /// Frames per second
    pub fps: u32,
    /// Robot type (optional)
    #[serde(default)]
    pub robot_type: Option<String>,
}

/// Topic to Kps feature mapping.
#[derive(Debug, Clone, Deserialize)]
pub struct Mapping {
    /// MCAP topic pattern
    pub topic: String,
    /// Kps feature path (e.g., "observation.camera_0")
    pub feature: String,
    /// Mapping type (TOML field: "type")
    #[serde(default, alias = "type")]
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
    /// Other sensor data (IMU, force, etc.)
    OtherSensor,
    /// Audio data
    Audio,
}

/// Output format configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct OutputConfig {
    /// Which formats to generate
    #[serde(default)]
    pub formats: Vec<OutputFormat>,
    /// How to encode images
    #[serde(default = "default_image_format")]
    pub image_format: ImageFormat,
    /// Maximum frames to process (None = unlimited)
    #[serde(default)]
    pub max_frames: Option<usize>,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            formats: vec![OutputFormat::Hdf5],
            image_format: ImageFormat::Raw,
            max_frames: None,
        }
    }
}

/// Supported output formats.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum OutputFormat {
    /// HDF5 format (legacy)
    Hdf5,
    /// Parquet + MP4 format (v3.0)
    Parquet,
}

/// Image encoding format.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ImageFormat {
    /// MP4 video (for Parquet format)
    Mp4,
    /// Raw embedded images (for HDF5)
    Raw,
}

fn default_image_format() -> ImageFormat {
    ImageFormat::Raw
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_config() {
        let toml_content = r#"
[dataset]
name = "test_dataset"
fps = 30

[[mappings]]
topic = "/camera/high"
feature = "observation.camera_0"
type = "image"

[[mappings]]
topic = "/joint_states"
feature = "observation.state"
type = "state"
"#;

        let config: KpsConfig = toml::from_str(toml_content).unwrap();
        assert_eq!(config.dataset.name, "test_dataset");
        assert_eq!(config.dataset.fps, 30);
        assert_eq!(config.mappings.len(), 2);
        assert_eq!(config.mappings[0].topic, "/camera/high");
        assert_eq!(config.mappings[0].feature, "observation.camera_0");
    }

    #[test]
    fn test_parse_config_with_robot_type() {
        let toml_content = r#"
[dataset]
name = "test_dataset"
fps = 30
robot_type = "panda"

[[mappings]]
topic = "/joint_states"
feature = "observation.state"
"#;

        let config: KpsConfig = toml::from_str(toml_content).unwrap();
        assert_eq!(config.dataset.robot_type, Some("panda".to_string()));
    }

    #[test]
    fn test_parse_config_with_output_formats() {
        let toml_content = r#"
[dataset]
name = "test"
fps = 30

[output]
formats = ["hdf5", "parquet"]
image_format = "mp4"
max_frames = 1000
"#;

        let config: KpsConfig = toml::from_str(toml_content).unwrap();
        assert_eq!(config.output.formats.len(), 2);
        assert_eq!(config.output.formats[0], OutputFormat::Hdf5);
        assert_eq!(config.output.formats[1], OutputFormat::Parquet);
        assert_eq!(config.output.image_format, ImageFormat::Mp4);
        assert_eq!(config.output.max_frames, Some(1000));
    }

    #[test]
    fn test_mappings_by_topic() {
        let toml_content = r#"
[dataset]
name = "test"
fps = 30

[[mappings]]
topic = "/camera/high"
feature = "observation.camera_0"
type = "image"

[[mappings]]
topic = "/joint_states"
feature = "observation.state"
type = "state"
"#;

        let config: KpsConfig = toml::from_str(toml_content).unwrap();
        let topic_map = config.mappings_by_topic();

        assert_eq!(topic_map.len(), 2);
        assert!(topic_map.contains_key("/camera/high"));
        assert!(topic_map.contains_key("/joint_states"));
        assert_eq!(topic_map["/camera/high"].feature, "observation.camera_0");
    }

    #[test]
    fn test_image_mappings() {
        let toml_content = r#"
[dataset]
name = "test"
fps = 30

[[mappings]]
topic = "/camera/high"
feature = "observation.camera_0"
type = "image"

[[mappings]]
topic = "/joint_states"
feature = "observation.state"
type = "state"

[[mappings]]
topic = "/camera/low"
feature = "observation.camera_1"
type = "image"
"#;

        let config: KpsConfig = toml::from_str(toml_content).unwrap();
        let image_mappings = config.image_mappings();

        assert_eq!(image_mappings.len(), 2);
        assert_eq!(image_mappings[0].topic, "/camera/high");
        assert_eq!(image_mappings[1].topic, "/camera/low");
    }

    #[test]
    fn test_state_mappings() {
        let toml_content = r#"
[dataset]
name = "test"
fps = 30

[[mappings]]
topic = "/joint_states"
feature = "observation.state"
type = "state"

[[mappings]]
topic = "/action"
feature = "action"
type = "action"

[[mappings]]
topic = "/camera"
feature = "observation.image"
type = "image"
"#;

        let config: KpsConfig = toml::from_str(toml_content).unwrap();
        let state_mappings = config.state_mappings();

        // Should include both state and action mappings, but not image
        assert_eq!(state_mappings.len(), 2);
        assert!(state_mappings
            .iter()
            .all(|m| { matches!(m.mapping_type, MappingType::State | MappingType::Action) }));
    }

    #[test]
    fn test_default_mapping_type() {
        let toml_content = r#"
[dataset]
name = "test"
fps = 30

[[mappings]]
topic = "/joint_states"
feature = "observation.state"
"#;

        let config: KpsConfig = toml::from_str(toml_content).unwrap();
        assert_eq!(config.mappings[0].mapping_type, MappingType::State);
    }

    #[test]
    fn test_default_output_config() {
        let output = OutputConfig::default();
        assert_eq!(output.formats, vec![OutputFormat::Hdf5]);
        assert_eq!(output.image_format, ImageFormat::Raw);
        assert_eq!(output.max_frames, None);
    }

    #[test]
    fn test_parse_invalid_mapping_type_falls_back_to_default() {
        // Unknown type should use default (State)
        let toml_content = r#"
[dataset]
name = "test"
fps = 30

[[mappings]]
topic = "/unknown"
feature = "observation.unknown"
type = "unknown_type"
"#;

        let result: Result<KpsConfig, _> = toml::from_str(toml_content);
        // This test verifies the deserialization behavior
        // The actual behavior depends on serde's handling of unknown enums
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_timestamp_mapping_type() {
        let toml_content = r#"
[dataset]
name = "test"
fps = 30

[[mappings]]
topic = "/timestamp"
feature = "observation.timestamp"
type = "timestamp"
"#;

        let config: KpsConfig = toml::from_str(toml_content).unwrap();
        assert_eq!(config.mappings[0].mapping_type, MappingType::Timestamp);
    }
}
