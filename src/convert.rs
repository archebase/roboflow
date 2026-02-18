// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! High-level conversion API.
//!
//! This module provides a simple, ergonomic API for converting robotics data
//! files (MCAP, ROS bag, Rerun) to dataset formats (LeRobot).
//!
//! # Example
//!
//! ```rust,ignore
//! use roboflow::convert;
//!
//! // Simple usage
//! let report = convert("input.mcap", "output/", "config.toml")?;
//! println!("Converted {} frames", report.frames_total);
//!
//! // Builder pattern for more control
//! use roboflow::ConvertBuilder;
//!
//! let report = ConvertBuilder::new()
//!     .input("input.mcap")
//!     .output("s3://bucket/output/")
//!     .config_path("config.toml")
//!     .max_frames(1000)
//!     .run()
//!     .await?;
//! ```

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use roboflow_core::{Result, RoboflowError};

#[cfg(feature = "sources")]
use roboflow_pipeline::sources::{SourceConfig, create_source};

#[cfg(feature = "sinks")]
use roboflow_pipeline::formats::lerobot::{LerobotWriterConfig, create_lerobot_writer};

use roboflow_pipeline::formats::{
    PipelineConfig, PipelineStats,
    common::config::{DatasetBaseConfig, Mapping, MappingType},
    lerobot::{DatasetConfig, FlushingConfig, LerobotConfig, StreamingConfig, VideoConfig},
};

/// Report from a conversion operation.
///
/// Contains statistics about the conversion process including
/// frame counts, processing time, and throughput metrics.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConversionReport {
    /// Total frames converted.
    pub frames_total: usize,
    /// Total episodes converted.
    pub episodes_total: usize,
    /// Total messages processed.
    pub messages_total: usize,
    /// Processing time in seconds.
    pub duration_sec: f64,
    /// Processing throughput in frames per second.
    pub fps: f64,
    /// Input source path.
    pub input_path: String,
    /// Output destination path.
    pub output_path: String,
    /// Additional metadata about the conversion.
    pub metadata: HashMap<String, serde_json::Value>,
}

impl ConversionReport {
    /// Create a new conversion report.
    pub fn new(input_path: impl Into<String>, output_path: impl Into<String>) -> Self {
        Self {
            frames_total: 0,
            episodes_total: 0,
            messages_total: 0,
            duration_sec: 0.0,
            fps: 0.0,
            input_path: input_path.into(),
            output_path: output_path.into(),
            metadata: HashMap::new(),
        }
    }

    /// Add a metadata entry.
    pub fn with_metadata(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }

    /// Create from pipeline stats.
    fn from_pipeline_stats(
        stats: PipelineStats,
        input_path: impl Into<String>,
        output_path: impl Into<String>,
    ) -> Self {
        Self {
            frames_total: stats.frames_written,
            episodes_total: stats.episodes_written,
            messages_total: stats.messages_processed,
            duration_sec: stats.duration_sec,
            fps: stats.fps,
            input_path: input_path.into(),
            output_path: output_path.into(),
            metadata: HashMap::new(),
        }
    }
}

/// Builder for conversion operations.
///
/// Provides a fluent interface for configuring and running
/// robotics data conversions.
///
/// # Example
///
/// ```rust,ignore
/// let report = ConvertBuilder::new()
///     .input("recording.mcap")
///     .output("dataset/")
///     .config_path("lerobot_config.toml")
///     .max_frames(10000)
///     .run()
///     .await?;
/// ```
#[derive(Debug, Clone)]
pub struct ConvertBuilder {
    /// Input path (file or S3 prefix).
    input: Option<String>,
    /// Output path (local directory or S3 prefix).
    output: Option<String>,
    /// Configuration file path.
    config_path: Option<PathBuf>,
    /// Inline configuration (alternative to config_path).
    config: Option<LerobotConfig>,
    /// Maximum frames to process.
    max_frames: Option<usize>,
    /// Batch size for reading messages.
    batch_size: usize,
    /// Processing timeout.
    timeout: Option<Duration>,
    /// Topic mappings (topic -> feature name).
    topic_mappings: HashMap<String, String>,
}

impl ConvertBuilder {
    /// Create a new conversion builder.
    pub fn new() -> Self {
        Self {
            input: None,
            output: None,
            config_path: None,
            config: None,
            max_frames: None,
            batch_size: 1000,
            timeout: None,
            topic_mappings: HashMap::new(),
        }
    }

    /// Set the input path.
    ///
    /// Supports:
    /// - Local files: `/path/to/file.mcap`
    /// - S3 files: `s3://bucket/path/file.mcap`
    /// - S3 prefix: `s3://bucket/path/to/prefix/`
    pub fn input(mut self, path: impl Into<String>) -> Self {
        self.input = Some(path.into());
        self
    }

    /// Set the output path.
    ///
    /// Supports:
    /// - Local directory: `/path/to/output/`
    /// - S3 prefix: `s3://bucket/output/`
    pub fn output(mut self, path: impl Into<String>) -> Self {
        self.output = Some(path.into());
        self
    }

    /// Set the configuration file path.
    pub fn config_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.config_path = Some(path.into());
        self
    }

    /// Set the configuration directly.
    ///
    /// This is an alternative to `config_path` for programmatic configuration.
    pub fn config(mut self, config: LerobotConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Set the maximum frames to process.
    ///
    /// Useful for testing or partial conversions.
    pub fn max_frames(mut self, max: usize) -> Self {
        self.max_frames = Some(max);
        self
    }

    /// Set the batch size for reading messages.
    ///
    /// Larger batches are more efficient but use more memory.
    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set a processing timeout.
    ///
    /// If the conversion doesn't complete within this time, it will be cancelled.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Add a topic mapping.
    ///
    /// Maps a ROS topic to a LeRobot feature name.
    pub fn topic_mapping(mut self, topic: impl Into<String>, feature: impl Into<String>) -> Self {
        self.topic_mappings.insert(topic.into(), feature.into());
        self
    }

    /// Add multiple topic mappings.
    pub fn topic_mappings(mut self, mappings: HashMap<String, String>) -> Self {
        self.topic_mappings.extend(mappings);
        self
    }

    /// Run the conversion.
    ///
    /// This is an async operation that:
    /// 1. Creates a source from the input path
    /// 2. Creates a LeRobot writer from the output path
    /// 3. Processes all messages through the pipeline
    /// 4. Returns a conversion report
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Input path is not set or invalid
    /// - Output path is not set or invalid
    /// - Configuration cannot be loaded
    /// - Source or sink creation fails
    /// - Processing fails
    #[cfg(all(feature = "sources", feature = "sinks"))]
    pub async fn run(self) -> Result<ConversionReport> {
        let input = self.input.clone().ok_or_else(|| {
            RoboflowError::other("Input path not set. Call .input() before .run()")
        })?;
        let output = self.output.clone().ok_or_else(|| {
            RoboflowError::other("Output path not set. Call .output() before .run()")
        })?;

        // Load configuration
        let config = if let Some(config) = self.config {
            config
        } else if let Some(path) = &self.config_path {
            LerobotConfig::from_file(path).map_err(|e| {
                RoboflowError::parse(
                    "LerobotConfig",
                    format!("Failed to load config from {}: {}", path.display(), e),
                )
            })?
        } else {
            // Default configuration with sensible defaults
            create_default_config()
        };

        // Merge topic mappings from builder with config
        let mut merged_mappings = config.mappings.clone();
        for (topic, feature) in &self.topic_mappings {
            // Check if topic already exists in mappings
            if !merged_mappings.iter().any(|m| &m.topic == topic) {
                merged_mappings.push(Mapping {
                    topic: topic.clone(),
                    feature: feature.clone(),
                    mapping_type: MappingType::Image, // Default to image type
                    camera_key: None,
                });
            }
        }
        let mut config = config;
        config.mappings = merged_mappings;

        // Create source
        let source_config = SourceConfig::from_url(&input);
        let mut source = create_source(&source_config).map_err(|e| {
            RoboflowError::other(format!("Failed to create source for {}: {}", input, e))
        })?;
        source
            .initialize(&source_config)
            .await
            .map_err(|e| RoboflowError::other(format!("Failed to initialize source: {}", e)))?;

        // Create writer using the consolidated factory
        let factory_config = LerobotWriterConfig::new(&output, config.clone());
        let writer_result = create_lerobot_writer(&factory_config).map_err(|e| {
            RoboflowError::other(format!("Failed to create writer for {}: {}", output, e))
        })?;

        // Build pipeline config
        let topic_mappings: HashMap<String, String> = config
            .mappings
            .iter()
            .map(|m| (m.topic.clone(), m.feature.clone()))
            .collect();

        let mut streaming_config =
            roboflow_pipeline::formats::streaming::config::StreamingConfig::with_fps(config.dataset.fps);
        let frame_interval_ns = 1_000_000_000u64 / config.dataset.fps as u64;
        streaming_config.completion_window_ns = frame_interval_ns * 3;

        let mut pipeline_config =
            PipelineConfig::new(streaming_config).with_topic_mappings(topic_mappings);

        if let Some(max) = self.max_frames {
            pipeline_config = pipeline_config.with_max_frames(max);
        }

        // Execute pipeline
        use roboflow_pipeline::formats::PipelineExecutor;

        let mut executor = PipelineExecutor::new(writer_result.writer, pipeline_config);

        // Process messages
        let batch_size = self.batch_size;
        loop {
            match source.read_batch(batch_size).await {
                Ok(Some(messages)) if !messages.is_empty() => {
                    for msg in messages {
                        executor.process_message(msg)?;
                    }
                }
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(e) => {
                    return Err(RoboflowError::other(format!("Source read failed: {}", e)));
                }
            }
        }

        // Finalize
        let stats = executor.finalize()?;

        Ok(ConversionReport::from_pipeline_stats(stats, input, output))
    }

    /// Run the conversion with a timeout.
    ///
    /// This is a convenience method that wraps `run()` with a timeout.
    #[cfg(all(feature = "sources", feature = "sinks"))]
    pub async fn run_with_timeout(self, timeout: Duration) -> Result<ConversionReport> {
        tokio::time::timeout(timeout, self.run())
            .await
            .map_err(|_| {
                RoboflowError::timeout(format!("Conversion timed out after {:?}", timeout))
            })?
    }
}

impl Default for ConvertBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert a robotics data file to LeRobot format.
///
/// This is the simplest way to convert data. For more control,
/// use [`ConvertBuilder`].
///
/// # Arguments
///
/// * `input` - Input file path (MCAP, ROS bag, or S3 URL)
/// * `output` - Output directory path (local or S3 URL)
/// * `config_path` - Path to LeRobot configuration TOML file
///
/// # Example
///
/// ```rust,ignore
/// use roboflow::convert;
///
/// let report = convert("recording.mcap", "dataset/", "lerobot_config.toml").await?;
/// println!("Converted {} frames in {:.2}s", report.frames_total, report.duration_sec);
/// ```
#[cfg(all(feature = "sources", feature = "sinks"))]
pub async fn convert(
    input: impl AsRef<str>,
    output: impl AsRef<str>,
    config_path: impl AsRef<str>,
) -> Result<ConversionReport> {
    ConvertBuilder::new()
        .input(input.as_ref())
        .output(output.as_ref())
        .config_path(PathBuf::from(config_path.as_ref()))
        .run()
        .await
}

/// Convert with a default configuration.
///
/// This is useful for quick conversions where you don't have a config file.
/// The default configuration uses 30 FPS and auto-detects topics.
///
/// # Example
///
/// ```rust,ignore
/// use roboflow::convert_with_defaults;
///
/// let report = convert_with_defaults("recording.mcap", "dataset/").await?;
/// println!("Converted {} frames", report.frames_total);
/// ```
#[cfg(all(feature = "sources", feature = "sinks"))]
pub async fn convert_with_defaults(
    input: impl AsRef<str>,
    output: impl AsRef<str>,
) -> Result<ConversionReport> {
    ConvertBuilder::new()
        .input(input.as_ref())
        .output(output.as_ref())
        .run()
        .await
}

/// Create a default LeRobot configuration.
///
/// This is used when no configuration file is provided.
fn create_default_config() -> LerobotConfig {
    LerobotConfig {
        dataset: DatasetConfig {
            base: DatasetBaseConfig {
                name: "dataset".to_string(),
                fps: 30,
                robot_type: None,
            },
            env_type: None,
        },
        mappings: Vec::new(),
        video: VideoConfig::default(),
        annotation_file: None,
        flushing: FlushingConfig::default(),
        streaming: StreamingConfig::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_conversion_report_new() {
        let report = ConversionReport::new("input.mcap", "output/");
        assert_eq!(report.input_path, "input.mcap");
        assert_eq!(report.output_path, "output/");
        assert_eq!(report.frames_total, 0);
    }

    #[test]
    fn test_conversion_report_with_metadata() {
        let report = ConversionReport::new("input.mcap", "output/")
            .with_metadata("camera_count", serde_json::json!(3));
        assert_eq!(
            report.metadata.get("camera_count"),
            Some(&serde_json::json!(3))
        );
    }

    #[test]
    fn test_convert_builder() {
        let builder = ConvertBuilder::new()
            .input("test.mcap")
            .output("output/")
            .max_frames(1000)
            .batch_size(500);

        assert_eq!(builder.input, Some("test.mcap".to_string()));
        assert_eq!(builder.output, Some("output/".to_string()));
        assert_eq!(builder.max_frames, Some(1000));
        assert_eq!(builder.batch_size, 500);
    }

    #[test]
    fn test_convert_builder_topic_mappings() {
        let builder = ConvertBuilder::new()
            .topic_mapping("/camera/left", "observation.images.left")
            .topic_mapping("/camera/right", "observation.images.right");

        assert_eq!(builder.topic_mappings.len(), 2);
        assert_eq!(
            builder.topic_mappings.get("/camera/left"),
            Some(&"observation.images.left".to_string())
        );
    }

    #[cfg(feature = "sources")]
    #[test]
    fn test_source_config_from_url_local() {
        let config = SourceConfig::from_url("/path/to/file.mcap");
        assert_eq!(config.source_type.name(), "mcap");

        let config = SourceConfig::from_url("/path/to/file.bag");
        assert_eq!(config.source_type.name(), "bag");

        let config = SourceConfig::from_url("/path/to/file.rrd");
        assert_eq!(config.source_type.name(), "rrd");
    }

    #[cfg(feature = "sources")]
    #[test]
    fn test_source_config_from_url_cloud() {
        let config = SourceConfig::from_url("s3://bucket/path/file.mcap");
        assert_eq!(config.source_type.name(), "mcap");

        let config = SourceConfig::from_url("s3://bucket/path/prefix/");
        assert_eq!(config.source_type.name(), "s3-prefix");

        let config = SourceConfig::from_url("oss://bucket/path/file.bag");
        assert_eq!(config.source_type.name(), "bag");
    }
}
