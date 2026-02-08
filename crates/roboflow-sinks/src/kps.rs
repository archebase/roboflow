// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! KPS sink implementation.
//!
//! This sink writes robotics datasets in KPS format by delegating
//! to `roboflow_dataset::kps::StreamingParquetWriter`.

use crate::convert::dataset_frame_to_aligned;
use crate::{DatasetFrame, Sink, SinkCheckpoint, SinkConfig, SinkError, SinkResult, SinkStats};
use roboflow_dataset::kps::{KpsConfig, StreamingParquetWriter};
use std::collections::HashMap;

/// KPS dataset sink.
///
/// Writes robotics datasets in KPS (Knowledge-based Policy Sharing) format
/// using sharded Parquet files. Delegates to `StreamingParquetWriter`.
pub struct KpsSink {
    /// Output directory path
    output_path: String,
    /// The dataset writer (created during initialize)
    writer: Option<StreamingParquetWriter>,
    /// Current episode index
    current_episode: usize,
    /// Frames written counter
    frames_written: usize,
    /// Episodes completed counter
    episodes_completed: usize,
    /// Start time for duration calculation
    start_time: Option<std::time::Instant>,
}

impl KpsSink {
    /// Create a new KPS sink.
    pub fn new(path: impl Into<String>) -> SinkResult<Self> {
        Ok(Self {
            output_path: path.into(),
            writer: None,
            current_episode: 0,
            frames_written: 0,
            episodes_completed: 0,
            start_time: None,
        })
    }

    /// Create a new KPS sink from a SinkConfig.
    pub fn from_config(config: &SinkConfig) -> SinkResult<Self> {
        match &config.sink_type {
            crate::SinkType::Kps { path } => Self::new(path),
            _ => Err(SinkError::InvalidConfig(
                "Invalid config for KpsSink".to_string(),
            )),
        }
    }

    /// Extract KpsConfig from SinkConfig options, or create a minimal default.
    fn extract_kps_config(config: &SinkConfig) -> KpsConfig {
        // Try to get config from options
        if let Some(kps_config) = config.get_option::<KpsConfig>("kps_config") {
            return kps_config;
        }

        let fps = config.get_option::<u32>("fps").unwrap_or(30);
        let name = config
            .get_option::<String>("dataset_name")
            .unwrap_or_else(|| "dataset".to_string());
        let robot_type = config.get_option::<String>("robot_type");

        KpsConfig {
            dataset: roboflow_dataset::kps::DatasetConfig {
                name,
                fps,
                robot_type,
            },
            mappings: Vec::new(),
            output: roboflow_dataset::kps::OutputConfig::default(),
        }
    }

    /// Create a new writer for the given episode.
    fn create_writer_for_episode(
        output_path: &str,
        episode_id: usize,
        config: &KpsConfig,
    ) -> SinkResult<StreamingParquetWriter> {
        StreamingParquetWriter::create(output_path, episode_id, config).map_err(|e| {
            SinkError::CreateFailed {
                path: output_path.into(),
                error: Box::new(e),
            }
        })
    }
}

#[async_trait::async_trait]
impl Sink for KpsSink {
    async fn initialize(&mut self, config: &SinkConfig) -> SinkResult<()> {
        // Create output directory
        let path = std::path::Path::new(&self.output_path);
        std::fs::create_dir_all(path).map_err(|e| SinkError::CreateFailed {
            path: path.to_path_buf(),
            error: Box::new(e),
        })?;

        let kps_config = Self::extract_kps_config(config);

        tracing::info!(
            output = %self.output_path,
            fps = kps_config.dataset.fps,
            name = %kps_config.dataset.name,
            "Initializing KPS sink"
        );

        let writer = Self::create_writer_for_episode(&self.output_path, 0, &kps_config)?;
        self.writer = Some(writer);
        self.start_time = Some(std::time::Instant::now());

        Ok(())
    }

    async fn write_frame(&mut self, frame: DatasetFrame) -> SinkResult<()> {
        let writer = self.writer.as_mut().ok_or_else(|| {
            SinkError::WriteFailed("Sink not initialized. Call initialize() first.".to_string())
        })?;

        // KPS: each episode gets its own Parquet files.
        // For simplicity, we write all frames to the initial writer.
        // Multi-episode handling would require creating new writers per episode.
        if frame.episode_index != self.current_episode {
            // Finalize current writer and create new one for new episode
            use roboflow_dataset::DatasetWriter;
            let _ = writer.finalize().map_err(|e| {
                SinkError::WriteFailed(format!("Failed to finalize episode: {e}"))
            })?;
            self.episodes_completed += 1;
            self.current_episode = frame.episode_index;

            // Note: creating a new writer requires the config again.
            // For now, use builder with defaults for the new episode.
            *writer = StreamingParquetWriter::builder()
                .output_dir(&self.output_path)
                .episode_id(frame.episode_index)
                .build()
                .map_err(|e| {
                    SinkError::WriteFailed(format!("Failed to create writer for episode: {e}"))
                })?;

            tracing::debug!(
                episode = self.current_episode,
                "Started new KPS episode"
            );
        }

        let aligned = dataset_frame_to_aligned(&frame);

        use roboflow_dataset::DatasetWriter;
        writer.write_frame(&aligned).map_err(|e| {
            SinkError::WriteFailed(format!("KPS write_frame failed: {e}"))
        })?;

        self.frames_written += 1;

        Ok(())
    }

    async fn flush(&mut self) -> SinkResult<()> {
        Ok(())
    }

    async fn finalize(&mut self) -> SinkResult<SinkStats> {
        let writer = self.writer.as_mut().ok_or_else(|| {
            SinkError::WriteFailed("Sink not initialized".to_string())
        })?;

        use roboflow_dataset::DatasetWriter;
        let writer_stats = writer.finalize().map_err(|e| {
            SinkError::WriteFailed(format!("KPS finalize failed: {e}"))
        })?;

        let duration = self
            .start_time
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0);

        tracing::info!(
            frames = writer_stats.frames_written,
            images = writer_stats.images_encoded,
            episodes = self.episodes_completed + 1,
            bytes = writer_stats.output_bytes,
            duration_sec = duration,
            "KPS sink finalized"
        );

        Ok(SinkStats {
            frames_written: writer_stats.frames_written,
            episodes_written: self.episodes_completed + 1,
            duration_sec: duration,
            total_bytes: Some(writer_stats.output_bytes),
            metrics: HashMap::from([
                (
                    "images_encoded".to_string(),
                    serde_json::json!(writer_stats.images_encoded),
                ),
                (
                    "state_records".to_string(),
                    serde_json::json!(writer_stats.state_records),
                ),
            ]),
        })
    }

    async fn checkpoint(&self) -> SinkResult<SinkCheckpoint> {
        Ok(SinkCheckpoint {
            last_frame_index: self.frames_written,
            last_episode_index: self.current_episode,
            checkpoint_time: chrono::Utc::now().timestamp(),
            data: HashMap::new(),
        })
    }

    fn supports_checkpointing(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kps_sink_creation() {
        let sink = KpsSink::new("/tmp/output");
        assert!(sink.is_ok());
        let sink = sink.unwrap();
        assert_eq!(sink.output_path, "/tmp/output");
    }

    #[test]
    fn test_kps_sink_from_config() {
        let config = SinkConfig::kps("/tmp/output");
        let sink = KpsSink::from_config(&config);
        assert!(sink.is_ok());
    }

    #[test]
    fn test_kps_sink_invalid_config() {
        let config = SinkConfig::lerobot("/tmp/output");
        let sink = KpsSink::from_config(&config);
        assert!(sink.is_err());
    }

    #[test]
    fn test_extract_default_config() {
        let config = SinkConfig::kps("/tmp/output");
        let kps_config = KpsSink::extract_kps_config(&config);
        assert_eq!(kps_config.dataset.fps, 30);
        assert_eq!(kps_config.dataset.name, "dataset");
    }
}
