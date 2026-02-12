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
}
