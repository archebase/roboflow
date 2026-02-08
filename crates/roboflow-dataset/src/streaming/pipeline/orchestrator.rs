// Main pipeline orchestrator

use std::path::Path;
use std::time::Instant;

use super::config::PipelineConfig;
use super::types::{PipelineError, PipelineReport};
use crate::lerobot::config::LerobotConfig;

/// The streaming dataset pipeline.
///
/// This is a 7-stage pipeline for high-throughput dataset conversion.
///
/// For now, it delegates to the existing StreamingDatasetConverter
/// while individual stages are being implemented.
pub struct StreamingDatasetPipeline {
    config: PipelineConfig,
}

impl StreamingDatasetPipeline {
    /// Create a new pipeline with the given configuration.
    pub fn new(config: PipelineConfig) -> Result<Self, PipelineError> {
        config.validate().map_err(|e| PipelineError::InitFailed {
            stage: "Pipeline".to_string(),
            reason: e,
        })?;

        Ok(Self { config })
    }

    /// Create a pipeline builder.
    pub fn builder() -> PipelineBuilder {
        PipelineBuilder::new()
    }

    /// Run the pipeline to completion.
    pub fn run(self) -> Result<PipelineReport, PipelineError> {
        let start = Instant::now();

        tracing::info!(
            input = %self.config.input_path.display(),
            episode = self.config.episode_index,
            decoder_threads = self.config.decoder.num_threads,
            encoder_threads = self.config.video_encoder.num_threads,
            "Starting StreamingDatasetPipeline"
        );

        // Check if input is a cloud URL
        let input_path_str = self.config.input_path.to_string_lossy();
        let is_cloud_input =
            input_path_str.starts_with("s3://") || input_path_str.starts_with("oss://");

        // Step 1: Prepare input file (download from cloud if needed)
        let process_path = if is_cloud_input {
            self.download_cloud_input()?
        } else {
            self.config.input_path.clone()
        };

        tracing::debug!(
            input = %process_path.display(),
            "Processing input file"
        );

        // TODO: Implement the 7-stage pipeline
        // For now, delegate to the existing StreamingDatasetConverter
        // while we build out the individual stages

        let report = self.run_with_converter(&process_path)?;

        let duration = start.elapsed();

        tracing::info!(
            duration_sec = duration.as_secs_f64(),
            frames_written = report.frames_written,
            messages_processed = report.messages_processed,
            throughput_fps = report.throughput_fps,
            "Pipeline complete"
        );

        Ok(report)
    }

    /// Download cloud input to local temp file.
    fn download_cloud_input(&self) -> Result<std::path::PathBuf, PipelineError> {
        use std::env;

        let temp_dir = env::temp_dir().join(format!("roboflow-input-{}", std::process::id()));

        std::fs::create_dir_all(&temp_dir).map_err(|e| PipelineError::InitFailed {
            stage: "Prefetcher".to_string(),
            reason: format!("failed to create temp dir: {e}"),
        })?;

        let filename =
            self.config
                .input_path
                .file_name()
                .ok_or_else(|| PipelineError::InitFailed {
                    stage: "Prefetcher".to_string(),
                    reason: "input path has no filename".to_string(),
                })?;

        let local_path = temp_dir.join(filename);

        tracing::debug!(
            cloud_url = %self.config.input_path.display(),
            local_path = %local_path.display(),
            "Downloading cloud input"
        );

        // TODO: Use streaming download
        // For now, this would delegate to the storage layer

        Ok(local_path)
    }

    /// Run using the existing converter (temporary until all stages are implemented).
    fn run_with_converter(&self, input_path: &Path) -> Result<PipelineReport, PipelineError> {
        let start = Instant::now();

        // Use the existing StreamingDatasetConverter
        let converter = crate::streaming::StreamingDatasetConverter::new_lerobot(
            // Output directory (local buffer for now)
            std::env::temp_dir().join(format!("roboflow-output-{}", std::process::id())),
            self.config.lerobot_config.clone(),
        )
        .map_err(|e| PipelineError::InitFailed {
            stage: "Converter".to_string(),
            reason: e.to_string(),
        })?;

        let stats = converter
            .convert(input_path)
            .map_err(|e| PipelineError::ExecutionFailed {
                stage: "Converter".to_string(),
                reason: e.to_string(),
            })?;

        let duration = start.elapsed();

        Ok(PipelineReport {
            frames_written: stats.frames_written,
            messages_processed: stats.messages_processed,
            duration_sec: duration.as_secs_f64(),
            throughput_fps: stats.throughput_fps(),
            stage_stats: vec![super::types::StageStats {
                stage: "Converter".to_string(),
                items_processed: stats.messages_processed,
                items_produced: stats.frames_written,
                duration_sec: duration.as_secs_f64(),
                peak_memory_mb: Some(stats.peak_memory_mb),
                metrics: [
                    (
                        "force_completed_frames".to_string(),
                        serde_json::json!(stats.force_completed_frames),
                    ),
                    (
                        "avg_buffer_size".to_string(),
                        serde_json::json!(stats.avg_buffer_size),
                    ),
                ]
                .into_iter()
                .collect(),
            }],
            peak_memory_mb: Some(stats.peak_memory_mb),
        })
    }
}

/// Builder for creating a StreamingDatasetPipeline.
pub struct PipelineBuilder {
    input_path: Option<std::path::PathBuf>,
    output_storage: Option<std::sync::Arc<dyn roboflow_storage::Storage>>,
    output_prefix: Option<String>,
    episode_index: usize,
    lerobot_config: Option<LerobotConfig>,
    channels: super::stage::ChannelConfig,
    decoder: super::config::DecoderConfig,
    aligner: super::config::AlignerConfig,
    video_encoder: super::config::VideoEncoderConfig,
}

impl PipelineBuilder {
    /// Create a new builder.
    pub fn new() -> Self {
        Self {
            input_path: None,
            output_storage: None,
            output_prefix: None,
            episode_index: 0,
            lerobot_config: None,
            channels: super::stage::ChannelConfig::default(),
            decoder: super::config::DecoderConfig::default(),
            aligner: super::config::AlignerConfig::default(),
            video_encoder: super::config::VideoEncoderConfig::default(),
        }
    }

    /// Set input path.
    pub fn input_path(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.input_path = Some(path.into());
        self
    }

    /// Set output storage.
    pub fn output_storage(
        mut self,
        storage: std::sync::Arc<dyn roboflow_storage::Storage>,
    ) -> Self {
        self.output_storage = Some(storage);
        self
    }

    /// Set output prefix.
    pub fn output_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.output_prefix = Some(prefix.into());
        self
    }

    /// Set episode index.
    pub fn episode_index(mut self, index: usize) -> Self {
        self.episode_index = index;
        self
    }

    /// Set LeRobot config.
    pub fn lerobot_config(mut self, config: LerobotConfig) -> Self {
        self.lerobot_config = Some(config);
        self
    }

    /// Use high-throughput settings.
    pub fn high_throughput(mut self) -> Self {
        self.channels = super::stage::ChannelConfig::high_throughput();
        self.decoder = super::config::DecoderConfig {
            num_threads: (num_cpus::get() / 2).max(2),
            ..Default::default()
        };
        self.video_encoder = super::config::VideoEncoderConfig {
            num_threads: (num_cpus::get() / 2).max(2),
            ..Default::default()
        };
        self
    }

    /// Build the pipeline config.
    pub fn build(self) -> Result<PipelineConfig, PipelineError> {
        let input_path = self.input_path.ok_or_else(|| PipelineError::InitFailed {
            stage: "Builder".to_string(),
            reason: "input_path is required".to_string(),
        })?;

        let lerobot_config = self
            .lerobot_config
            .ok_or_else(|| PipelineError::InitFailed {
                stage: "Builder".to_string(),
                reason: "lerobot_config is required".to_string(),
            })?;

        Ok(PipelineConfig {
            input_path,
            output_storage: self.output_storage,
            output_prefix: self.output_prefix,
            episode_index: self.episode_index,
            lerobot_config,
            channels: self.channels,
            decoder: self.decoder,
            aligner: self.aligner,
            transformer: super::config::TransformerConfig::default(),
            video_encoder: self.video_encoder,
            parquet_writer: super::config::ParquetWriterConfig::default(),
            upload: super::config::UploadConfig::default(),
        })
    }
}

impl Default for PipelineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_missing_input() {
        let dataset_config = crate::lerobot::config::DatasetConfig {
            name: "test".to_string(),
            fps: 30,
            robot_type: None,
            env_type: None,
        };
        let lerobot_config = crate::lerobot::config::LerobotConfig {
            dataset: dataset_config,
            mappings: vec![],
            video: crate::lerobot::config::VideoConfig::default(),
            annotation_file: None,
        };
        let builder = PipelineBuilder::new().lerobot_config(lerobot_config);
        assert!(builder.build().is_err());
    }

    #[test]
    fn test_builder_valid() {
        let dataset_config = crate::lerobot::config::DatasetConfig {
            name: "test".to_string(),
            fps: 30,
            robot_type: None,
            env_type: None,
        };
        let lerobot_config = crate::lerobot::config::LerobotConfig {
            dataset: dataset_config,
            mappings: vec![],
            video: crate::lerobot::config::VideoConfig::default(),
            annotation_file: None,
        };

        let builder = PipelineBuilder::new()
            .input_path("test.bag")
            .lerobot_config(lerobot_config);

        let result = builder.build();
        assert!(result.is_ok());

        let pipeline_config = result.unwrap();
        assert_eq!(
            pipeline_config.input_path,
            std::path::PathBuf::from("test.bag")
        );
        assert_eq!(pipeline_config.episode_index, 0);
    }
}
