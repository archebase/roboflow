// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Convert stage for processing bag files to LeRobot format.
//!
//! This stage processes input files and converts them to the
//! LeRobot v2.1 dataset format (Parquet + MP4 videos).

use roboflow_core::Result;
use roboflow_dataset::formats::lerobot::{LerobotWriterConfig, create_lerobot_writer};
use roboflow_dataset::formats::unified_executor::{DatasetPipelineConfig, DatasetPipelineExecutor};
use roboflow_dataset::formats::{
    common::DatasetBaseConfig,
    lerobot::{DatasetConfig, FlushingConfig, LerobotConfig, StreamingConfig, VideoConfig},
};
use roboflow_dataset::sources::{SourceConfig, create_source};
use roboflow_executor::object_store::{ObjectId, ObjectRef};
use roboflow_executor::stage::{PartitionId, Stage, StageId};
use roboflow_executor::task::{Task, TaskContext, TaskResult, TaskStatus};

/// Stage for converting bag files to LeRobot format.
///
/// This stage processes input files and converts them to the
/// LeRobot v2.1 dataset format (Parquet + MP4 videos) using
/// parallel processing for maximum throughput.
///
/// Each partition processes one input file independently.
pub struct ConvertStage {
    input_file: String,
    output_prefix: String,
    config_hash: String,
}

impl ConvertStage {
    /// Create a new convert stage.
    ///
    /// # Arguments
    ///
    /// * `input_file` - Input file URL to convert.
    /// * `output_prefix` - Output path prefix.
    /// * `config_hash` - Configuration hash for caching.
    pub fn new(
        input_file: impl Into<String>,
        output_prefix: impl Into<String>,
        config_hash: impl Into<String>,
    ) -> Self {
        Self {
            input_file: input_file.into(),
            output_prefix: output_prefix.into(),
            config_hash: config_hash.into(),
        }
    }
}

impl Stage for ConvertStage {
    fn id(&self) -> StageId {
        StageId(1)
    }

    fn name(&self) -> &str {
        "convert"
    }

    fn partition_count(&self) -> usize {
        1
    }

    fn dependencies(&self) -> Vec<StageId> {
        vec![StageId(0)]
    }

    fn create_task(&self, partition: PartitionId) -> Box<dyn Task> {
        Box::new(ConvertTask {
            input_file: self.input_file.clone(),
            output_prefix: self.output_prefix.clone(),
            config_hash: self.config_hash.clone(),
            partition_id: partition,
        })
    }
}

/// Task for converting a single bag file.
struct ConvertTask {
    input_file: String,
    output_prefix: String,
    #[allow(dead_code)]
    config_hash: String,
    partition_id: PartitionId,
}

#[async_trait::async_trait]
impl Task for ConvertTask {
    async fn execute(&mut self, ctx: &TaskContext) -> Result<TaskResult> {
        tracing::info!(
            task_id = ?ctx.task_id,
            partition = ?self.partition_id,
            input_file = %self.input_file,
            output_prefix = %self.output_prefix,
            "Converting bag file to LeRobot format"
        );

        // Create output directory for this partition
        let output_dir = format!("{}/episode_{:06}", self.output_prefix, self.partition_id.0);
        std::fs::create_dir_all(&output_dir).map_err(|e| {
            roboflow_core::RoboflowError::other(format!("Failed to create output dir: {}", e))
        })?;

        // Determine source type from file extension
        let source_type = if self.input_file.to_lowercase().ends_with(".mcap") {
            "mcap"
        } else if self.input_file.to_lowercase().ends_with(".bag") {
            "bag"
        } else {
            return Err(roboflow_core::RoboflowError::other(format!(
                "Unsupported file format: {}",
                self.input_file
            )));
        };

        // Create source config
        let source_config = match source_type {
            "mcap" => SourceConfig::mcap(&self.input_file),
            "bag" => SourceConfig::bag(&self.input_file),
            _ => unreachable!(),
        };

        // Create source
        let mut source = create_source(&source_config).map_err(|e| {
            roboflow_core::RoboflowError::other(format!("Failed to create source: {}", e))
        })?;

        // Initialize source
        source.initialize(&source_config).await.map_err(|e| {
            roboflow_core::RoboflowError::other(format!("Failed to initialize source: {}", e))
        })?;

        // Create a basic LeRobot config
        let lerobot_config = LerobotConfig {
            dataset: DatasetConfig {
                base: DatasetBaseConfig {
                    name: format!("episode_{:06}", self.partition_id.0),
                    fps: 30,
                    robot_type: None,
                },
                env_type: None,
            },
            mappings: vec![],
            video: VideoConfig::default(),
            annotation_file: None,
            flushing: FlushingConfig::default(),
            streaming: StreamingConfig::default(),
        };

        // Create LerobotWriter
        let writer_config = LerobotWriterConfig::new(&output_dir, lerobot_config.clone());

        let writer_result = create_lerobot_writer(&writer_config).map_err(|e| {
            roboflow_core::RoboflowError::other(format!("Failed to create writer: {}", e))
        })?;

        let writer = writer_result.writer;

        // Create pipeline config
        let pipeline_config = DatasetPipelineConfig::with_fps(lerobot_config.dataset.base.fps);

        // Create parallel pipeline executor for maximum throughput
        let num_threads = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4);
        let mut executor = DatasetPipelineExecutor::parallel(writer, pipeline_config, num_threads);

        // Process messages in batches
        loop {
            match source.read_batch(100).await {
                Ok(Some(messages)) => {
                    executor.process_messages(messages).map_err(|e| {
                        roboflow_core::RoboflowError::other(format!("Pipeline error: {}", e))
                    })?;
                }
                Ok(None) => break,
                Err(e) => {
                    return Err(roboflow_core::RoboflowError::other(format!(
                        "Source read error: {}",
                        e
                    )));
                }
            }
        }

        // Finalize and get stats
        let stats = executor.finalize().map_err(|e| {
            roboflow_core::RoboflowError::other(format!("Pipeline finalize error: {}", e))
        })?;
        let frames_written = stats.frames_written;
        let episodes_written = stats.episodes_written;

        tracing::info!(
            frames_written = frames_written,
            episodes_written = episodes_written,
            policy = stats.policy_name,
            "Conversion complete"
        );

        let _output_path = format!("{}/data", output_dir);
        let obj_ref = ObjectRef::new(ObjectId::new([2u8; 32]), 1024, ctx.task_id, vec![]);

        Ok(TaskResult {
            outputs: vec![obj_ref],
            metrics: roboflow_executor::task::TaskMetrics {
                duration_secs: stats.duration_sec,
                cpu_secs: 0.0,
                memory_peak_bytes: 0,
                bytes_read: 0,
                bytes_written: frames_written as u64 * 1024,
            },
            status: TaskStatus::Success,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_stage() {
        let stage = ConvertStage::new("/input/test.bag", "s3://bucket/output/", "config_hash_123");

        assert_eq!(stage.id(), StageId(1));
        assert_eq!(stage.name(), "convert");
        assert_eq!(stage.dependencies(), vec![StageId(0)]);
    }
}
