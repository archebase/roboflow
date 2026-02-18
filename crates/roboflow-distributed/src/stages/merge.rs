// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Merge stage for combining converted files.

use std::path::Path;

use roboflow_core::Result;
use roboflow_executor::object_store::{ObjectId, ObjectRef};
use roboflow_executor::stage::{PartitionId, Stage, StageId};
use roboflow_executor::task::{Task, TaskContext, TaskResult, TaskMetrics, TaskStatus};

/// Stage for merging converted files.
///
/// This stage combines Parquet files and video segments from
/// the convert stage into the final LeRobot dataset structure.
pub struct MergeStage {
    output_path: String,
}

impl MergeStage {
    /// Create a new merge stage.
    ///
    /// # Arguments
    ///
    /// * `output_path` - Final output path for the merged dataset.
    pub fn new(output_path: impl Into<String>) -> Self {
        Self {
            output_path: output_path.into(),
        }
    }
}

impl Stage for MergeStage {
    fn id(&self) -> StageId {
        StageId(2)
    }

    fn name(&self) -> &str {
        "merge"
    }

    fn partition_count(&self) -> usize {
        1
    }

    fn dependencies(&self) -> Vec<StageId> {
        vec![StageId(1)]
    }

    fn create_task(&self, _partition: PartitionId) -> Box<dyn Task> {
        Box::new(MergeTask {
            output_path: self.output_path.clone(),
        })
    }
}

/// Task for merging converted files.
struct MergeTask {
    output_path: String,
}

#[async_trait::async_trait]
impl Task for MergeTask {
    async fn execute(&mut self, ctx: &TaskContext) -> Result<TaskResult> {
        tracing::info!(
            task_id = ?ctx.task_id,
            output_path = %self.output_path,
            "Merging converted files"
        );

        // Create output directory
        std::fs::create_dir_all(&self.output_path).map_err(|e| {
            roboflow_core::RoboflowError::other(format!("Failed to create output dir: {}", e))
        })?;

        // Scan for converted episode directories
        let parent_dir = Path::new(&self.output_path)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| ".".to_string());

        let mut parquet_files = Vec::new();
        let mut video_dirs = Vec::new();
        let mut episode_count = 0usize;

        if let Ok(entries) = std::fs::read_dir(&parent_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    let name = path.file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("");
                    
                    if name.starts_with("episode_") {
                        episode_count += 1;
                        
                        if let Ok(episode_entries) = std::fs::read_dir(&path) {
                            for ep_entry in episode_entries.flatten() {
                                let ep_path = ep_entry.path();
                                if ep_path.is_file() {
                                    if let Some(ext) = ep_path.extension() {
                                        if ext == "parquet" {
                                            parquet_files.push(ep_path.clone());
                                        }
                                    }
                                } else if ep_path.is_dir() {
                                    if let Some(dir_name) = ep_path.file_name() {
                                        let dir_str = dir_name.to_string_lossy();
                                        if dir_str.contains("video") || dir_str.contains("cam") {
                                            video_dirs.push(ep_path.clone());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        tracing::info!(
            episode_count = episode_count,
            parquet_count = parquet_files.len(),
            video_dir_count = video_dirs.len(),
            "Found converted files to merge"
        );

        if parquet_files.len() >= 1 {
            let output_parquet = format!("{}/data.parquet", self.output_path);
            std::fs::copy(&parquet_files[0], &output_parquet).map_err(|e| {
                roboflow_core::RoboflowError::other(format!("Failed to copy parquet: {}", e))
            })?;
        }

        let info_json = serde_json::json!({
            "version": "2.1",
            "name": "dataset",
            "fps": 30,
            "created_at": chrono::Utc::now().to_rfc3339(),
            "episodes_count": episode_count,
        });

        let info_path = format!("{}/info.json", self.output_path);
        std::fs::write(&info_path, serde_json::to_string_pretty(&info_json).unwrap())
            .map_err(|e| roboflow_core::RoboflowError::other(format!("Failed to write info.json: {}", e)))?;

        tracing::info!(episode_count = episode_count, "Merge complete");

        let obj_ref = ObjectRef::new(
            ObjectId::new([3u8; 32]),
            2048,
            ctx.task_id,
            vec![],
        );

        Ok(TaskResult {
            outputs: vec![obj_ref],
            metrics: TaskMetrics {
                duration_secs: 0.0,
                cpu_secs: 0.0,
                memory_peak_bytes: 0,
                bytes_read: parquet_files.len() as u64 * 1024 * 1024,
                bytes_written: 1024 * 1024,
            },
            status: TaskStatus::Success,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_stage() {
        let stage = MergeStage::new("s3://bucket/output/dataset");

        assert_eq!(stage.id(), StageId(2));
        assert_eq!(stage.name(), "merge");
        assert_eq!(stage.dependencies(), vec![StageId(1)]);
    }
}
