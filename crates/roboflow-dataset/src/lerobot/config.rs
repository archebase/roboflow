// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! LeRobot dataset configuration.
//!
//! Configuration for converting ROS bag data to LeRobot v2.1 format.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use roboflow_core::{Result, Validate, validators};

// Re-export shared config types so existing imports continue to work.
pub use crate::common::config::DatasetBaseConfig;
pub use crate::common::config::Mapping;
pub use crate::common::config::MappingType;

/// LeRobot dataset configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
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

    /// Incremental flushing options for memory-bounded processing
    #[serde(default)]
    pub flushing: FlushingConfig,

    /// S3 streaming encoder options
    #[serde(default)]
    pub streaming: StreamingConfig,
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
    fn validate_config(&self) -> Result<()> {
        // Validate FPS > 0
        validators::positive(self.dataset.fps, "dataset.fps")?;

        // Validate CRF range (0-51 for H.264)
        validators::range(self.video.crf, 0, 51, "video.crf")?;

        // Validate streaming config
        validators::positive(
            self.streaming.ring_buffer_size,
            "streaming.ring_buffer_size",
        )?;

        // Validate upload part size (5MB to 5GB)
        const MIN_PART_SIZE: usize = 5 * 1024 * 1024;
        const MAX_PART_SIZE: usize = 5 * 1024 * 1024 * 1024;
        validators::range(
            self.streaming.upload_part_size,
            MIN_PART_SIZE,
            MAX_PART_SIZE,
            "streaming.upload_part_size",
        )?;

        // Check for duplicate topics
        let mut topics = HashSet::new();
        for mapping in &self.mappings {
            if !topics.insert(&mapping.topic) {
                return Err(roboflow_core::RoboflowError::parse(
                    "mappings",
                    format!("Duplicate topic found: {}", mapping.topic),
                ));
            }
        }

        Ok(())
    }
}

impl Validate for LerobotConfig {
    fn validate(&self) -> Result<()> {
        self.validate_config()
    }
}

impl LerobotConfig {
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

/// LeRobot-specific dataset metadata configuration.
///
/// Embeds [`DatasetBaseConfig`] via `#[serde(flatten)]` for the common fields
/// (`name`, `fps`, `robot_type`) and adds LeRobot-specific fields.
///
/// Field access to base fields works transparently via `Deref`:
/// ```rust,ignore
/// let config: DatasetConfig = /* ... */;
/// let name = &config.name;       // auto-derefs to base.name
/// let fps = config.fps;           // auto-derefs to base.fps
/// let env = &config.env_type;     // direct field access
/// ```
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DatasetConfig {
    /// Common dataset fields (name, fps, robot_type).
    #[serde(flatten)]
    pub base: DatasetBaseConfig,

    /// Environment type (optional, LeRobot-specific).
    #[serde(default)]
    pub env_type: Option<String>,
}

impl std::ops::Deref for DatasetConfig {
    type Target = DatasetBaseConfig;
    fn deref(&self) -> &DatasetBaseConfig {
        &self.base
    }
}

impl std::ops::DerefMut for DatasetConfig {
    fn deref_mut(&mut self) -> &mut DatasetBaseConfig {
        &mut self.base
    }
}

/// Video encoding configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
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

/// Incremental flushing configuration for memory-bounded processing.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FlushingConfig {
    /// Maximum frames per chunk before auto-flush (0 = unlimited).
    #[serde(default = "default_max_frames")]
    pub max_frames_per_chunk: usize,

    /// Maximum memory bytes per chunk before auto-flush (0 = unlimited).
    #[serde(default = "default_max_memory")]
    pub max_memory_bytes: usize,

    /// Whether to encode videos incrementally (per-chunk).
    #[serde(default = "default_incremental_encoding")]
    pub incremental_video_encoding: bool,
}

impl Default for FlushingConfig {
    fn default() -> Self {
        Self {
            max_frames_per_chunk: default_max_frames(),
            max_memory_bytes: default_max_memory(),
            incremental_video_encoding: default_incremental_encoding(),
        }
    }
}

impl FlushingConfig {
    /// Create unlimited buffering (deprecated: use bounded flushing for production).
    ///
    /// # Deprecated
    ///
    /// Unlimited buffering can cause OOM on long recordings. Use bounded defaults
    /// or configure appropriate limits for your hardware.
    #[deprecated(
        since = "0.3.0",
        note = "Use bounded flushing to avoid OOM on long recordings"
    )]
    pub fn unlimited() -> Self {
        Self {
            max_frames_per_chunk: 0,
            max_memory_bytes: 0,
            incremental_video_encoding: false,
        }
    }

    /// Check if flushing should occur based on current state.
    pub fn should_flush(&self, frame_count: usize, memory_bytes: usize) -> bool {
        if self.max_frames_per_chunk > 0 && frame_count >= self.max_frames_per_chunk {
            return true;
        }
        if self.max_memory_bytes > 0 && memory_bytes >= self.max_memory_bytes {
            return true;
        }
        false
    }

    /// Is this config actually limiting (vs unlimited)?
    pub fn is_limited(&self) -> bool {
        self.max_frames_per_chunk > 0 || self.max_memory_bytes > 0
    }
}

fn default_max_frames() -> usize {
    1000
}

fn default_max_memory() -> usize {
    2 * 1024 * 1024 * 1024 // 2GB
}

fn default_incremental_encoding() -> bool {
    true
}

/// S3 streaming encoder configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StreamingConfig {
    /// Enable S3 streaming encoder (auto-detected if not specified)
    #[serde(default)]
    pub enabled: Option<bool>,

    /// Use multi-camera streaming coordinator for better parallelization
    #[serde(default)]
    pub use_coordinator: bool,

    /// Ring buffer capacity in frames (default: 128)
    #[serde(default = "default_ring_buffer_size")]
    pub ring_buffer_size: usize,

    /// Multipart upload part size in bytes (default: 16MB)
    /// S3/OSS requires: 5MB <= part_size <= 5GB
    #[serde(default = "default_upload_part_size")]
    pub upload_part_size: usize,

    /// Timeout for frame operations in seconds (default: 5)
    #[serde(default = "default_buffer_timeout_secs")]
    pub buffer_timeout_secs: u64,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            enabled: None,
            use_coordinator: false,
            ring_buffer_size: default_ring_buffer_size(),
            upload_part_size: default_upload_part_size(),
            buffer_timeout_secs: default_buffer_timeout_secs(),
        }
    }
}

fn default_ring_buffer_size() -> usize {
    128
}

fn default_upload_part_size() -> usize {
    16 * 1024 * 1024 // 16 MB
}

fn default_buffer_timeout_secs() -> u64 {
    5
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

    // =============================================================================
    // Additional VideoConfig Tests
    // =============================================================================

    #[test]
    fn test_video_config_default() {
        let config = VideoConfig::default();
        assert_eq!(config.codec, "libx264");
        assert_eq!(config.crf, 18);
        assert_eq!(config.preset, "fast");
        assert!(config.profile.is_none());
    }

    #[test]
    fn test_video_config_with_profile() {
        let toml = r#"
[dataset]
name = "test"
fps = 30

[video]
profile = "quality"
"#;

        let config: LerobotConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.video.profile, Some("quality".to_string()));
    }

    #[test]
    fn test_video_config_custom_values() {
        let toml = r#"
[dataset]
name = "test"
fps = 30

[video]
codec = "h264_nvenc"
crf = 23
preset = "medium"
"#;

        let config: LerobotConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.video.codec, "h264_nvenc");
        assert_eq!(config.video.crf, 23);
        assert_eq!(config.video.preset, "medium");
    }

    // =============================================================================
    // FlushingConfig Tests
    // =============================================================================

    #[test]
    fn test_flushing_config_default() {
        let config = FlushingConfig::default();
        assert_eq!(config.max_frames_per_chunk, 1000);
        assert_eq!(config.max_memory_bytes, 2 * 1024 * 1024 * 1024);
        assert!(config.incremental_video_encoding);
    }

    #[test]
    fn test_flushing_config_should_flush() {
        let config = FlushingConfig::default();

        // Should not flush when under limits
        assert!(!config.should_flush(500, 1024 * 1024 * 1024));

        // Should flush when frame limit reached
        assert!(config.should_flush(1000, 1024 * 1024 * 1024));

        // Should flush when memory limit reached
        assert!(config.should_flush(500, 2 * 1024 * 1024 * 1024));
    }

    #[test]
    fn test_flushing_config_is_limited() {
        let config = FlushingConfig::default();
        assert!(config.is_limited());

        let unlimited = FlushingConfig {
            max_frames_per_chunk: 0,
            max_memory_bytes: 0,
            incremental_video_encoding: false,
        };
        assert!(!unlimited.is_limited());
    }

    #[test]
    fn test_flushing_config_custom_values() {
        let toml = r#"
[dataset]
name = "test"
fps = 30

[flushing]
max_frames_per_chunk = 500
max_memory_bytes = 1073741824
incremental_video_encoding = false
"#;

        let config: LerobotConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.flushing.max_frames_per_chunk, 500);
        assert_eq!(config.flushing.max_memory_bytes, 1073741824);
        assert!(!config.flushing.incremental_video_encoding);
    }

    // =============================================================================
    // StreamingConfig Tests
    // =============================================================================

    #[test]
    fn test_streaming_config_default() {
        let config = StreamingConfig::default();
        assert!(config.enabled.is_none());
        assert!(!config.use_coordinator);
        assert_eq!(config.ring_buffer_size, 128);
        assert_eq!(config.upload_part_size, 16 * 1024 * 1024);
        assert_eq!(config.buffer_timeout_secs, 5);
    }

    #[test]
    fn test_streaming_config_custom_values() {
        let toml = r#"
[dataset]
name = "test"
fps = 30

[streaming]
enabled = true
use_coordinator = true
ring_buffer_size = 256
upload_part_size = 33554432
buffer_timeout_secs = 10
"#;

        let config: LerobotConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.streaming.enabled, Some(true));
        assert!(config.streaming.use_coordinator);
        assert_eq!(config.streaming.ring_buffer_size, 256);
        assert_eq!(config.streaming.upload_part_size, 33554432);
        assert_eq!(config.streaming.buffer_timeout_secs, 10);
    }

    // =============================================================================
    // DatasetConfig Tests
    // =============================================================================

    #[test]
    fn test_dataset_config_deref() {
        let toml = r#"
[dataset]
name = "test_dataset"
fps = 60
robot_type = "arm"
env_type = "simulation"
"#;

        let config: LerobotConfig = toml::from_str(toml).unwrap();
        // Deref to base config fields
        assert_eq!(config.dataset.name, "test_dataset");
        assert_eq!(config.dataset.fps, 60);
        assert_eq!(config.dataset.robot_type, Some("arm".to_string()));
        // Direct field access
        assert_eq!(config.dataset.env_type, Some("simulation".to_string()));
    }

    // =============================================================================
    // Mapping Tests
    // =============================================================================

    #[test]
    fn test_state_mappings() {
        let toml = r#"
[dataset]
name = "test"
fps = 30

[[mappings]]
topic = "/joint_states"
feature = "observation.state"
mapping_type = "state"

[[mappings]]
topic = "/joint_cmd"
feature = "action"
mapping_type = "action"
"#;

        let config: LerobotConfig = toml::from_str(toml).unwrap();
        let states = config.state_mappings();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].feature, "observation.state");
    }

    #[test]
    fn test_action_mappings() {
        let toml = r#"
[dataset]
name = "test"
fps = 30

[[mappings]]
topic = "/joint_states"
feature = "observation.state"
mapping_type = "state"

[[mappings]]
topic = "/joint_cmd"
feature = "action"
mapping_type = "action"

[[mappings]]
topic = "/joint_cmd2"
feature = "action2"
mapping_type = "action"
"#;

        let config: LerobotConfig = toml::from_str(toml).unwrap();
        let actions = config.action_mappings();
        assert_eq!(actions.len(), 2);
    }

    #[test]
    fn test_mappings_by_topic() {
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
mapping_type = "state"
"#;

        let config: LerobotConfig = toml::from_str(toml).unwrap();
        let map = config.mappings_by_topic();

        assert_eq!(map.len(), 2);
        assert!(map.contains_key("/cam_h/color"));
        assert!(map.contains_key("/joint_states"));
    }

    // =============================================================================
    // Validation Tests
    // =============================================================================

    #[test]
    fn test_validate_duplicate_topic() {
        let toml = r#"
[dataset]
name = "test"
fps = 30

[[mappings]]
topic = "/same_topic"
feature = "feature1"
mapping_type = "image"

[[mappings]]
topic = "/same_topic"
feature = "feature2"
mapping_type = "state"
"#;

        let result = toml::from_str::<LerobotConfig>(toml);
        // TOML parsing may succeed, but validation should fail
        if let Ok(config) = result {
            assert!(config.validate().is_err());
        }
    }

    #[test]
    fn test_env_type_optional() {
        let toml = r#"
[dataset]
name = "test"
fps = 30
"#;

        let config: LerobotConfig = toml::from_str(toml).unwrap();
        assert!(config.dataset.env_type.is_none());
    }

    // =============================================================================
    // Annotation File Tests
    // =============================================================================

    #[test]
    fn test_annotation_file_optional() {
        let toml = r#"
[dataset]
name = "test"
fps = 30
"#;

        let config: LerobotConfig = toml::from_str(toml).unwrap();
        assert!(config.annotation_file.is_none());
    }

    #[test]
    fn test_annotation_file_specified() {
        let toml = r#"
annotation_file = "/path/to/annotations.json"

[dataset]
name = "test"
fps = 30
"#;

        let config: LerobotConfig = toml::from_str(toml).unwrap();
        assert_eq!(
            config.annotation_file,
            Some("/path/to/annotations.json".to_string())
        );
    }
}
