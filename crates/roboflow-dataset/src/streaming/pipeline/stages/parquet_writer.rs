// Parquet writer stage - delegates to existing LerobotWriter

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Instant;

use crossbeam_channel::Receiver;

use crate::common::base::{AlignedFrame, ImageData};
use crate::streaming::pipeline::types::{DatasetFrame, PipelineError, PipelineResult};
use roboflow_storage::{LocalStorage, Storage};

/// Statistics from the parquet writer stage.
#[derive(Debug, Clone)]
pub struct ParquetWriterStats {
    /// Frames processed
    pub frames_processed: usize,
    /// Rows written
    pub rows_written: usize,
    /// Parquet files created
    pub files_created: usize,
    /// Processing time in seconds
    pub duration_sec: f64,
}

/// Parquet writer stage configuration.
#[derive(Debug, Clone)]
pub struct ParquetWriterConfig {
    /// FPS for output
    pub fps: u32,
}

impl Default for ParquetWriterConfig {
    fn default() -> Self {
        Self { fps: 30 }
    }
}

/// The parquet writer stage.
///
/// Receives DatasetFrames and writes them to Parquet format.
/// Delegates to the existing LerobotWriter for compatibility.
pub struct ParquetWriterStage {
    /// Episode index (currently unused, reserved for future use)
    _episode_index: usize,
    /// Input receiver
    input_rx: Receiver<DatasetFrame>,
    /// Output directory
    output_dir: PathBuf,
    /// Storage backend
    storage: Option<Arc<dyn Storage>>,
    /// Output prefix
    output_prefix: Option<String>,
    /// Configuration
    config: ParquetWriterConfig,
}

impl ParquetWriterStage {
    /// Create a new parquet writer stage.
    pub fn new(
        _episode_index: usize,
        input_rx: Receiver<DatasetFrame>,
        output_dir: PathBuf,
        storage: Option<Arc<dyn Storage>>,
        output_prefix: Option<String>,
        config: ParquetWriterConfig,
    ) -> Self {
        Self {
            _episode_index,
            input_rx,
            output_dir,
            storage,
            output_prefix,
            config,
        }
    }

    /// Spawn the writer in a thread.
    pub fn spawn(
        self,
    ) -> JoinHandle<PipelineResult<(ParquetWriterStats, crate::streaming::pipeline::StageStats)>>
    {
        thread::spawn(move || {
            let name = "ParquetWriter";
            tracing::debug!("{name} starting");

            let start = Instant::now();
            let result = self.run_internal();
            let duration = start.elapsed();

            match &result {
                Ok((writer_stats, _stage_stats)) => {
                    tracing::debug!(
                        duration_sec = duration.as_secs_f64(),
                        frames = writer_stats.frames_processed,
                        rows = writer_stats.rows_written,
                        "{name} completed"
                    );
                }
                Err(e) => {
                    tracing::error!(error = %e, "{name} failed");
                }
            }

            result
        })
    }

    fn run_internal(
        &self,
    ) -> PipelineResult<(ParquetWriterStats, crate::streaming::pipeline::StageStats)> {
        use crate::common::DatasetWriter;
        use crate::lerobot::writer::LerobotWriter;

        // Create storage backend
        let storage = self
            .storage
            .clone()
            .unwrap_or_else(|| Arc::new(LocalStorage::new(&self.output_dir)) as Arc<dyn Storage>);

        let output_prefix = self.output_prefix.clone().unwrap_or_default();

        // Create lerobot config
        let lerobot_config = crate::lerobot::config::LerobotConfig {
            dataset: crate::lerobot::config::DatasetConfig {
                base: crate::common::config::DatasetBaseConfig {
                    name: "pipeline".to_string(),
                    fps: self.config.fps,
                    robot_type: None,
                },
                env_type: None,
            },
            mappings: vec![],
            video: crate::lerobot::config::VideoConfig::default(),
            annotation_file: None,
        };

        // Create the writer
        let mut writer =
            LerobotWriter::new(storage, output_prefix, &self.output_dir, lerobot_config).map_err(
                |e| PipelineError::ExecutionFailed {
                    stage: "ParquetWriter".to_string(),
                    reason: e.to_string(),
                },
            )?;

        let mut frames_processed = 0usize;

        loop {
            match self.input_rx.recv() {
                Ok(frame) => {
                    frames_processed += 1;

                    // Convert DatasetFrame back to AlignedFrame for writing
                    let images: HashMap<String, ImageData> = frame
                        .images
                        .iter()
                        .map(|(k, (width, height, data))| {
                            (
                                k.clone(),
                                ImageData {
                                    width: *width,
                                    height: *height,
                                    data: data.clone(),
                                    original_timestamp: (frame.timestamp * 1_000_000_000.0) as u64,
                                    is_encoded: false,
                                    is_depth: false,
                                },
                            )
                        })
                        .collect();

                    let mut states = HashMap::new();
                    if let Some(state) = frame.observation_state {
                        states.insert("observation.state".to_string(), state);
                    }
                    if let Some(action) = frame.action {
                        states.insert("action".to_string(), action);
                    }

                    let aligned_frame = AlignedFrame {
                        frame_index: frame.frame_index,
                        timestamp: (frame.timestamp * 1_000_000_000.0) as u64,
                        images,
                        states,
                        actions: HashMap::new(),
                        audio: HashMap::new(),
                        timestamps: HashMap::new(),
                    };

                    writer.write_frame(&aligned_frame).map_err(|e| {
                        PipelineError::ExecutionFailed {
                            stage: "ParquetWriter".to_string(),
                            reason: e.to_string(),
                        }
                    })?;

                    if frames_processed.is_multiple_of(1000) {
                        tracing::debug!(frames = frames_processed, "ParquetWriter progress");
                    }
                }
                Err(_) => {
                    // Channel closed - finalize writer
                    let stats = DatasetWriter::finalize(&mut writer).map_err(|e| {
                        PipelineError::ExecutionFailed {
                            stage: "ParquetWriter".to_string(),
                            reason: e.to_string(),
                        }
                    })?;

                    return Ok((
                        ParquetWriterStats {
                            frames_processed,
                            rows_written: stats.frames_written,
                            files_created: 1,
                            duration_sec: stats.duration_sec,
                        },
                        crate::streaming::pipeline::StageStats {
                            stage: "ParquetWriter".to_string(),
                            items_processed: frames_processed,
                            items_produced: stats.frames_written,
                            duration_sec: stats.duration_sec,
                            peak_memory_mb: None,
                            metrics: [(
                                "rows_written".to_string(),
                                serde_json::json!(stats.frames_written),
                            )]
                            .into_iter()
                            .collect(),
                        },
                    ));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parquet_writer_config_default() {
        let config = ParquetWriterConfig::default();
        assert_eq!(config.fps, 30);
    }
}
