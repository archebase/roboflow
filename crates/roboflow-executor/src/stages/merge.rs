// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Merge stage for combining converted files.

use roboflow_core::Result;

use crate::stage::{PartitionId, Stage, StageId};
use crate::task::{Task, TaskContext, TaskOutput, TaskResult};

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

        // Simulate merge work
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        Ok(TaskResult {
            outputs: vec![TaskOutput {
                id: self.output_path.clone(),
                size_bytes: 2048,
            }],
            metrics: Default::default(),
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
