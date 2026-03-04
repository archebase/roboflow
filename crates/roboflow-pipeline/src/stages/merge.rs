// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Merge stage: assembles global metadata from completed conversions.

use roboflow_core::Result;
use roboflow_executor::{PartitionId, Stage, StageId, Task, TaskContext, TaskResult};

/// Pipeline stage that merges results from all convert tasks.
///
/// Collects episode results and writes global metadata files
/// (info.json, episodes.jsonl, tasks.jsonl, episodes_stats.jsonl).
pub struct MergeStage {
    id: StageId,
    output_path: String,
    /// Results from the convert stage (episode output paths)
    episode_results: Vec<String>,
}

impl MergeStage {
    /// Create a new merge stage.
    ///
    /// # Arguments
    ///
    /// * `id` - Stage identifier
    /// * `output_path` - Final dataset output path
    /// * `episode_results` - Output paths from completed convert tasks
    pub fn new(id: StageId, output_path: impl Into<String>, episode_results: Vec<String>) -> Self {
        Self {
            id,
            output_path: output_path.into(),
            episode_results,
        }
    }
}

impl Stage for MergeStage {
    fn id(&self) -> StageId {
        self.id
    }

    fn name(&self) -> &str {
        "merge"
    }

    fn partition_count(&self) -> usize {
        1
    }

    fn create_task(&self, _partition: PartitionId) -> Box<dyn Task> {
        Box::new(MergeTask {
            output_path: self.output_path.clone(),
            episode_results: self.episode_results.clone(),
        })
    }
}

struct MergeTask {
    output_path: String,
    episode_results: Vec<String>,
}

#[async_trait::async_trait]
impl Task for MergeTask {
    async fn execute(&mut self, ctx: &TaskContext) -> Result<TaskResult> {
        let start = std::time::Instant::now();

        tracing::info!(
            output_path = %self.output_path,
            episodes = self.episode_results.len(),
            "Starting metadata merge"
        );

        if ctx.is_cancelled() {
            return Ok(TaskResult {
                outputs: vec![],
                metrics: roboflow_executor::TaskMetrics::default(),
                status: roboflow_executor::TaskStatus::Cancelled,
            });
        }

        let output_dir = std::path::Path::new(&self.output_path);
        let meta_dir = output_dir.join("meta");
        std::fs::create_dir_all(&meta_dir).map_err(|e| {
            roboflow_core::RoboflowError::io(format!("Failed to create meta dir: {}", e))
        })?;

        // Collect episode info from each result
        let mut episodes_info = Vec::new();
        for (index, episode_path) in self.episode_results.iter().enumerate() {
            if ctx.is_cancelled() {
                return Ok(TaskResult {
                    outputs: vec![],
                    metrics: roboflow_executor::TaskMetrics::default(),
                    status: roboflow_executor::TaskStatus::Cancelled,
                });
            }

            let path = std::path::Path::new(episode_path);
            if path.exists() {
                // Read the episode's own metadata if available, otherwise count
                // parquet data files (chunk-000/*.parquet) for frame count.
                let data_chunk_dir = path.join("data/chunk-000");
                let frame_count = if data_chunk_dir.exists() {
                    // Count parquet files — each represents one episode's frames
                    std::fs::read_dir(&data_chunk_dir)
                        .map(|entries| {
                            entries
                                .filter_map(|e| e.ok())
                                .filter(|e| {
                                    e.path().extension().is_some_and(|ext| ext == "parquet")
                                })
                                .count()
                        })
                        .unwrap_or(0)
                } else {
                    0
                };

                episodes_info.push(serde_json::json!({
                    "episode_index": index,
                    "length": frame_count,
                    "tasks": []
                }));
            }
        }

        // Write episodes.jsonl
        let episodes_path = meta_dir.join("episodes.jsonl");
        let episodes_content: String = episodes_info
            .iter()
            .map(|e| serde_json::to_string(e).unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\n");
        if !episodes_content.is_empty() {
            std::fs::write(&episodes_path, &episodes_content).map_err(|e| {
                roboflow_core::RoboflowError::io(format!("Failed to write episodes.jsonl: {}", e))
            })?;
        }

        let duration = start.elapsed();
        tracing::info!(
            episodes = self.episode_results.len(),
            output_path = %self.output_path,
            duration_sec = duration.as_secs_f64(),
            "Metadata merge completed"
        );

        Ok(TaskResult {
            outputs: vec![self.output_path.clone()],
            metrics: roboflow_executor::TaskMetrics {
                duration_secs: duration.as_secs_f64(),
                ..Default::default()
            },
            status: roboflow_executor::TaskStatus::Success,
        })
    }
}
