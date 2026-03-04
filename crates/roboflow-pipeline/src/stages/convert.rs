// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Convert stage: processes bag/MCAP files into LeRobot format.

use std::path::PathBuf;

use roboflow_core::Result;
use roboflow_dataset::formats::lerobot::config::LerobotConfig;
use roboflow_dataset::formats::lerobot::{LerobotWriterConfig, create_lerobot_writer};
use roboflow_dataset::sources::{SourceConfig, create_source};
use roboflow_executor::{PartitionId, Stage, StageId, Task, TaskContext, TaskResult};

use crate::{DatasetPipelineConfig, DatasetPipelineExecutor};

/// Pipeline stage that converts bag/MCAP files to LeRobot format.
///
/// Each partition processes one input file independently.
pub struct ConvertStage {
    id: StageId,
    output_dir: PathBuf,
    partition_count: usize,
    lerobot_config: LerobotConfig,
    /// Input file URLs, one per partition (populated from discover stage)
    input_urls: Vec<String>,
}

impl ConvertStage {
    /// Create a new convert stage.
    ///
    /// # Arguments
    ///
    /// * `id` - Stage identifier
    /// * `output_dir` - Base output directory
    /// * `input_urls` - Input file URLs from the discover stage
    /// * `lerobot_config` - LeRobot configuration for the writer
    pub fn new(
        id: StageId,
        output_dir: impl Into<PathBuf>,
        input_urls: Vec<String>,
        lerobot_config: LerobotConfig,
    ) -> Self {
        let partition_count = input_urls.len().max(1);
        Self {
            id,
            output_dir: output_dir.into(),
            partition_count,
            lerobot_config,
            input_urls,
        }
    }
}

impl Stage for ConvertStage {
    fn id(&self) -> StageId {
        self.id
    }

    fn name(&self) -> &str {
        "convert"
    }

    fn partition_count(&self) -> usize {
        self.partition_count
    }

    fn create_task(&self, partition: PartitionId) -> Box<dyn Task> {
        let input_url = self
            .input_urls
            .get(partition.0 as usize)
            .cloned()
            .unwrap_or_default();

        Box::new(ConvertTask {
            input_url,
            output_dir: self.output_dir.clone(),
            episode_index: partition.0 as usize,
            lerobot_config: self.lerobot_config.clone(),
        })
    }
}

struct ConvertTask {
    input_url: String,
    output_dir: PathBuf,
    episode_index: usize,
    lerobot_config: LerobotConfig,
}

#[async_trait::async_trait]
impl Task for ConvertTask {
    async fn execute(&mut self, ctx: &TaskContext) -> Result<TaskResult> {
        if self.input_url.is_empty() {
            tracing::warn!(
                episode_index = self.episode_index,
                "Skipping conversion: empty input URL"
            );
            return Ok(TaskResult {
                outputs: vec![],
                metrics: roboflow_executor::TaskMetrics::default(),
                status: roboflow_executor::TaskStatus::Success,
            });
        }

        let start = std::time::Instant::now();
        tracing::info!(
            input = %self.input_url,
            episode_index = self.episode_index,
            "Starting conversion"
        );

        // Create source
        let source_config = if self.input_url.ends_with(".mcap") {
            SourceConfig::mcap(&self.input_url)
        } else {
            SourceConfig::bag(&self.input_url)
        };

        let mut source = create_source(&source_config).map_err(|e| {
            roboflow_core::RoboflowError::parse("ConvertStage", format!("Source error: {}", e))
        })?;
        source.initialize(&source_config).await.map_err(|e| {
            roboflow_core::RoboflowError::parse("ConvertStage", format!("Init error: {}", e))
        })?;

        // Create episode output directory
        let episode_output = self
            .output_dir
            .join(format!("episode_{:06}", self.episode_index));
        std::fs::create_dir_all(&episode_output)?;

        // Create writer
        let writer_config = LerobotWriterConfig::new(
            episode_output.to_string_lossy().to_string(),
            self.lerobot_config.clone(),
        );

        let writer = create_lerobot_writer(&writer_config)
            .map_err(|e| roboflow_core::RoboflowError::parse("ConvertStage", e))?
            .writer;

        // Build topic mappings
        let topic_mappings: std::collections::HashMap<String, String> = self
            .lerobot_config
            .mappings
            .iter()
            .map(|m| (m.topic.clone(), m.feature.clone()))
            .collect();

        let num_threads = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4);

        let mut executor = DatasetPipelineExecutor::parallel(
            writer,
            DatasetPipelineConfig::with_fps(self.lerobot_config.dataset.fps)
                .with_topic_mappings(topic_mappings),
            num_threads,
        );

        // Process messages
        let mut total_messages: u64 = 0;
        loop {
            if ctx.is_cancelled() {
                return Ok(TaskResult {
                    outputs: vec![],
                    metrics: roboflow_executor::TaskMetrics::default(),
                    status: roboflow_executor::TaskStatus::Cancelled,
                });
            }

            match source.read_batch(100).await {
                Ok(Some(messages)) => {
                    total_messages += messages.len() as u64;
                    executor.process_messages(messages)?;
                }
                Ok(None) => break,
                Err(e) => {
                    tracing::error!(error = %e, "Read error during conversion");
                    return Err(roboflow_core::RoboflowError::parse(
                        "ConvertStage",
                        format!("Read error: {}", e),
                    ));
                }
            }
        }

        // Finalize
        let stats = executor.finalize()?;
        let duration = start.elapsed();

        tracing::info!(
            input = %self.input_url,
            episode_index = self.episode_index,
            frames = stats.frames_written,
            messages = total_messages,
            duration_sec = duration.as_secs_f64(),
            "Conversion completed"
        );

        Ok(TaskResult {
            outputs: vec![episode_output.to_string_lossy().to_string()],
            metrics: roboflow_executor::TaskMetrics {
                duration_secs: duration.as_secs_f64(),
                bytes_written: 0, // Actual bytes not tracked at this level
                ..Default::default()
            },
            status: roboflow_executor::TaskStatus::Success,
        })
    }
}
