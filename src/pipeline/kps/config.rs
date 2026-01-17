//! Configuration for Kps pipeline.

use std::collections::HashMap;

use crate::io::kps::KpsConfig;

/// Configuration for the Kps conversion pipeline.
#[derive(Debug, Clone)]
pub struct KpsPipelineConfig {
    /// Kps dataset configuration.
    pub kps_config: KpsConfig,

    /// Time alignment configuration.
    pub time_aligner: TimeAlignerConfig,

    /// Camera extraction configuration.
    pub camera_extractor: CameraExtractorConfig,

    /// Channel capacity for inter-stage communication.
    pub channel_capacity: usize,
}

impl Default for KpsPipelineConfig {
    fn default() -> Self {
        Self {
            kps_config: KpsConfig {
                dataset: crate::io::kps::DatasetConfig {
                    name: "dataset".to_string(),
                    fps: 30,
                    robot_type: None,
                },
                mappings: vec![],
                output: crate::io::kps::OutputConfig::default(),
            },
            time_aligner: TimeAlignerConfig::default(),
            camera_extractor: CameraExtractorConfig::default(),
            channel_capacity: 16,
        }
    }
}

impl KpsPipelineConfig {
    /// Create a new pipeline config from a Kps config file.
    pub fn from_kps_config(kps_config: KpsConfig) -> Self {
        let fps = kps_config.dataset.fps;
        Self {
            kps_config,
            time_aligner: TimeAlignerConfig {
                target_fps: fps,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Create a new pipeline config from a TOML file.
    pub fn from_file(
        path: impl AsRef<std::path::Path>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let kps_config = KpsConfig::from_file(path)?;
        Ok(Self::from_kps_config(kps_config))
    }

    /// Set the channel capacity.
    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    /// Enable camera parameter extraction.
    pub fn with_camera_extraction(mut self, enabled: bool) -> Self {
        self.camera_extractor.enabled = enabled;
        self
    }

    /// Set camera topics for parameter extraction.
    pub fn with_camera_topics(mut self, topics: HashMap<String, String>) -> Self {
        self.camera_extractor.camera_topics = topics;
        self
    }
}

/// Configuration for time alignment stage.
#[derive(Debug, Clone)]
pub struct TimeAlignerConfig {
    /// Target frames per second for output.
    pub target_fps: u32,

    /// Which interpolation strategy to use.
    pub strategy: crate::pipeline::kps::traits::time_alignment::TimeAlignmentStrategyType,

    /// Maximum gap for state interpolation (nanoseconds).
    pub state_interpolation_max_gap_ns: u64,

    /// Maximum distance for image synchronization (nanoseconds).
    pub image_sync_tolerance_ns: u64,
}

impl Default for TimeAlignerConfig {
    fn default() -> Self {
        Self {
            target_fps: 30,
            strategy: crate::pipeline::kps::traits::time_alignment::TimeAlignmentStrategyType::LinearInterpolation,
            state_interpolation_max_gap_ns: 100_000_000, // 100ms
            image_sync_tolerance_ns: 33_333_333u64,       // ~1 frame at 30fps
        }
    }
}

/// Configuration for camera parameter extraction.
#[derive(Debug, Clone, Default)]
pub struct CameraExtractorConfig {
    /// Whether camera parameter extraction is enabled.
    pub enabled: bool,

    /// Camera name to topic prefix mapping.
    pub camera_topics: HashMap<String, String>,

    /// Parent frame ID for extrinsic parameters.
    pub parent_frame: String,

    /// Camera info topic suffix.
    pub camera_info_suffix: String,

    /// TF topic for transforms.
    pub tf_topic: String,
}

impl CameraExtractorConfig {
    /// Create a new camera extractor config.
    pub fn new() -> Self {
        Self {
            enabled: false,
            camera_topics: HashMap::new(),
            parent_frame: "base_link".to_string(),
            camera_info_suffix: "/camera_info".to_string(),
            tf_topic: "/tf".to_string(),
        }
    }

    /// Add a camera topic mapping.
    pub fn add_camera(mut self, name: String, topic_prefix: String) -> Self {
        self.camera_topics.insert(name, topic_prefix);
        self
    }

    /// Set the parent frame.
    pub fn with_parent_frame(mut self, frame: String) -> Self {
        self.parent_frame = frame;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = KpsPipelineConfig::default();
        assert_eq!(config.channel_capacity, 16);
        assert!(!config.camera_extractor.enabled);
        assert_eq!(config.time_aligner.target_fps, 30);
    }

    #[test]
    fn test_with_channel_capacity() {
        let config = KpsPipelineConfig::default().with_channel_capacity(32);
        assert_eq!(config.channel_capacity, 32);
    }

    #[test]
    fn test_with_camera_extraction() {
        let config = KpsPipelineConfig::default().with_camera_extraction(true);
        assert!(config.camera_extractor.enabled);
    }

    #[test]
    fn test_camera_extractor_add_camera() {
        let config = CameraExtractorConfig::new()
            .add_camera("hand_high".to_string(), "/camera/high".to_string());

        assert_eq!(
            config.camera_topics.get("hand_high"),
            Some(&"/camera/high".to_string())
        );
    }
}
