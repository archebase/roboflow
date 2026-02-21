// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Unified pipeline configuration.
//!
//! Provides a single configuration structure that combines all components
//! needed for data conversion:
//! - Input source configuration
//! - Output sink configuration
//! - Dataset format configuration
//! - Processing options
//!
//! # Example
//!
//! ```toml
//! # pipeline.toml
//! [source]
//! type = "mcap"
//! path = "input.mcap"
//!
//! [sink]
//! type = "lerobot"
//! path = "output/"
//!
//! [dataset]
//! name = "my_dataset"
//! fps = 30
//!
//! [[mappings]]
//! topic = "/camera/image"
//! feature = "observation.image"
//!
//! [video]
//! codec = "h264"
//! crf = 23
//!
//! [processing]
//! workers = 4
//! ```
//!
//! # Migration
//!
//! This unified config can coexist with the existing individual configs:
//! - `LerobotConfig` for dataset-specific settings
//! - `SourceConfig` for input configuration
//! - `OutputConfig` for output configuration
//!
//! To migrate:
//! 1. Combine all configs into a single `pipeline.toml`
//! 2. Use `PipelineConfig::from_file()` instead of individual config loaders

use serde::{Deserialize, Serialize};
use std::path::Path;

use roboflow_core::{Result, Validate, validators};

// Re-export individual configs for backward compatibility
pub use roboflow_dataset::formats::config::{OutputConfig, OutputFormat};
pub use roboflow_dataset::formats::lerobot::{
    DatasetConfig, FlushingConfig, LerobotConfig, Mapping, StreamingConfig, VideoConfig,
};
pub use roboflow_dataset::sources::{SourceConfig, SourceType};

/// Unified pipeline configuration.
///
/// Combines all configuration needed for a conversion pipeline into
/// a single structure that can be loaded from a TOML file.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PipelineConfig {
    /// Input source configuration.
    pub source: SourceConfig,

    /// Output configuration.
    pub sink: OutputConfig,

    /// Dataset format configuration.
    pub dataset: DatasetConfig,

    /// Topic to feature mappings.
    #[serde(default)]
    pub mappings: Vec<Mapping>,

    /// Video encoding configuration.
    #[serde(default)]
    pub video: VideoConfig,

    /// Processing options.
    #[serde(default)]
    pub processing: ProcessingConfig,

    /// Incremental flushing options.
    #[serde(default)]
    pub flushing: FlushingConfig,

    /// S3 streaming encoder options.
    #[serde(default)]
    pub streaming: StreamingConfig,
}

impl PipelineConfig {
    /// Load configuration from a TOML file.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        Self::from_toml(&content)
    }

    /// Parse configuration from a TOML string.
    pub fn from_toml(toml_str: &str) -> Result<Self> {
        let config: PipelineConfig = toml::from_str(toml_str).map_err(|e| {
            roboflow_core::RoboflowError::parse(
                "PipelineConfig",
                format!("TOML parse error: {}", e),
            )
        })?;
        config.validate()?;
        Ok(config)
    }

    /// Create a builder for constructing a pipeline config.
    pub fn builder() -> PipelineConfigBuilder {
        PipelineConfigBuilder::default()
    }

    /// Convert to a LerobotConfig for use with existing writers.
    pub fn to_lerobot_config(&self) -> LerobotConfig {
        LerobotConfig {
            dataset: self.dataset.clone(),
            mappings: self.mappings.clone(),
            video: self.video.clone(),
            annotation_file: None,
            flushing: self.flushing.clone(),
            streaming: self.streaming.clone(),
        }
    }
}

impl Validate for PipelineConfig {
    fn validate(&self) -> Result<()> {
        // Validate dataset config
        validators::positive(self.dataset.fps, "dataset.fps")?;
        validators::not_empty_str(&self.dataset.name, "dataset.name")?;

        // Validate video config
        validators::range(self.video.crf, 0, 51, "video.crf")?;
        validators::not_empty_str(&self.video.codec, "video.codec")?;

        // Validate processing config
        if self.processing.workers > 0 {
            validators::positive(self.processing.workers, "processing.workers")?;
        }

        Ok(())
    }
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            source: SourceConfig::mcap(""),
            sink: OutputConfig::lerobot(""),
            dataset: DatasetConfig {
                base: crate::DatasetBaseConfig {
                    name: "dataset".to_string(),
                    fps: 30,
                    robot_type: None,
                },
                env_type: None,
            },
            mappings: vec![],
            video: VideoConfig::default(),
            processing: ProcessingConfig::default(),
            flushing: FlushingConfig::default(),
            streaming: StreamingConfig::default(),
        }
    }
}

/// Processing configuration options.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ProcessingConfig {
    /// Number of worker threads for parallel processing.
    #[serde(default = "default_workers")]
    pub workers: usize,

    /// Enable distributed processing mode.
    #[serde(default)]
    pub distributed: bool,

    /// Maximum frames to process (0 = unlimited).
    #[serde(default)]
    pub max_frames: usize,

    /// Batch size for message reading.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

impl Default for ProcessingConfig {
    fn default() -> Self {
        Self {
            workers: default_workers(),
            distributed: false,
            max_frames: 0,
            batch_size: default_batch_size(),
        }
    }
}

fn default_workers() -> usize {
    std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(4)
}

fn default_batch_size() -> usize {
    100
}

/// Builder for PipelineConfig.
#[derive(Debug, Default)]
pub struct PipelineConfigBuilder {
    source: Option<SourceConfig>,
    sink: Option<OutputConfig>,
    dataset: Option<DatasetConfig>,
    mappings: Vec<Mapping>,
    video: Option<VideoConfig>,
    processing: ProcessingConfig,
    flushing: Option<FlushingConfig>,
    streaming: Option<StreamingConfig>,
}

impl PipelineConfigBuilder {
    /// Set the input source configuration.
    pub fn source(mut self, config: SourceConfig) -> Self {
        self.source = Some(config);
        self
    }

    /// Set the input source from a path (auto-detects format).
    pub fn source_path(mut self, path: impl AsRef<str>) -> Self {
        self.source = Some(SourceConfig::from_url(path));
        self
    }

    /// Set the output configuration.
    pub fn sink(mut self, config: OutputConfig) -> Self {
        self.sink = Some(config);
        self
    }

    /// Set the output path (defaults to LeRobot format).
    pub fn sink_path(mut self, path: impl Into<String>) -> Self {
        self.sink = Some(OutputConfig::lerobot(path));
        self
    }

    /// Set the dataset configuration.
    pub fn dataset(mut self, config: DatasetConfig) -> Self {
        self.dataset = Some(config);
        self
    }

    /// Set the dataset name.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        let name = name.into();
        let dataset = self.dataset.get_or_insert_with(|| DatasetConfig {
            base: crate::DatasetBaseConfig {
                name: name.clone(),
                fps: 30,
                robot_type: None,
            },
            env_type: None,
        });
        dataset.base.name = name;
        self
    }

    /// Set the FPS.
    pub fn fps(mut self, fps: u32) -> Self {
        let dataset = self.dataset.get_or_insert_with(|| DatasetConfig {
            base: crate::DatasetBaseConfig {
                name: "dataset".to_string(),
                fps,
                robot_type: None,
            },
            env_type: None,
        });
        dataset.base.fps = fps;
        self
    }

    /// Add a topic mapping.
    pub fn mapping(mut self, mapping: Mapping) -> Self {
        self.mappings.push(mapping);
        self
    }

    /// Set the video configuration.
    pub fn video(mut self, config: VideoConfig) -> Self {
        self.video = Some(config);
        self
    }

    /// Set the number of workers.
    pub fn workers(mut self, workers: usize) -> Self {
        self.processing.workers = workers;
        self
    }

    /// Enable distributed processing.
    pub fn distributed(mut self, enabled: bool) -> Self {
        self.processing.distributed = enabled;
        self
    }

    /// Build the pipeline configuration.
    pub fn build(self) -> Result<PipelineConfig> {
        let config = PipelineConfig {
            source: self.source.ok_or_else(|| {
                roboflow_core::RoboflowError::parse("PipelineConfig", "source is required")
            })?,
            sink: self.sink.ok_or_else(|| {
                roboflow_core::RoboflowError::parse("PipelineConfig", "sink is required")
            })?,
            dataset: self.dataset.unwrap_or_else(|| DatasetConfig {
                base: crate::DatasetBaseConfig {
                    name: "dataset".to_string(),
                    fps: 30,
                    robot_type: None,
                },
                env_type: None,
            }),
            mappings: self.mappings,
            video: self.video.unwrap_or_default(),
            processing: self.processing,
            flushing: self.flushing.unwrap_or_default(),
            streaming: self.streaming.unwrap_or_default(),
        };
        config.validate()?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_config_builder() {
        let config = PipelineConfig::builder()
            .source_path("/path/to/input.mcap")
            .sink_path("/path/to/output/")
            .name("test_dataset")
            .fps(30)
            .workers(4)
            .build()
            .unwrap();

        assert_eq!(config.source.path(), "/path/to/input.mcap");
        assert_eq!(config.sink.path(), "/path/to/output/");
        assert_eq!(config.dataset.base.name, "test_dataset");
        assert_eq!(config.dataset.base.fps, 30);
        assert_eq!(config.processing.workers, 4);
    }

    #[test]
    fn test_pipeline_config_from_toml() {
        let toml = r#"
[source]
type = "Mcap"
path = "input.mcap"

[sink]
type = "Lerobot"
path = "output/"

[dataset]
name = "test_dataset"
fps = 30

[video]
codec = "h264"
crf = 23

[processing]
workers = 4
"#;

        let config = PipelineConfig::from_toml(toml).unwrap();
        assert_eq!(config.source.path(), "input.mcap");
        assert_eq!(config.sink.path(), "output/");
        assert_eq!(config.dataset.base.name, "test_dataset");
        assert_eq!(config.video.codec, "h264");
        assert_eq!(config.processing.workers, 4);
    }

    #[test]
    fn test_pipeline_config_to_lerobot_config() {
        let pipeline = PipelineConfig::builder()
            .source_path("input.mcap")
            .sink_path("output/")
            .name("test")
            .fps(30)
            .build()
            .unwrap();

        let lerobot = pipeline.to_lerobot_config();
        assert_eq!(lerobot.dataset.base.name, "test");
        assert_eq!(lerobot.dataset.base.fps, 30);
    }

    #[test]
    fn test_pipeline_config_default() {
        let config = PipelineConfig::default();
        assert!(config.mappings.is_empty());
        assert!(config.processing.workers > 0);
    }

    #[test]
    fn test_processing_config_default() {
        let config = ProcessingConfig::default();
        assert!(config.workers > 0);
        assert!(!config.distributed);
        assert_eq!(config.max_frames, 0);
        assert_eq!(config.batch_size, 100);
    }
}
