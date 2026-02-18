// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Convert stage for processing bag files to LeRobot format.

use std::sync::Arc;

use roboflow_core::Result;

use crate::stage::{PartitionId, Stage, StageId};
use crate::task::{Task, TaskContext, TaskOutput, TaskResult};

/// Stage for converting bag files to LeRobot format.
///
/// This stage processes input files and converts them to the
/// LeRobot v2.1 dataset format (Parquet + MP4 videos).
///
/// Each partition processes one input file independently.
pub struct ConvertStage {
    output_prefix: String,
    config_hash: String,
}

impl ConvertStage {
    /// Create a new convert stage.
    ///
    /// # Arguments
    ///
    /// * `output_prefix` - Output path prefix.
    /// * `config_hash` - Configuration hash for caching.
    pub fn new(
        output_prefix: impl Into<String>,
        config_hash: impl Into<String>,
    ) -> Self {
        Self {
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
            output_prefix: self.output_prefix.clone(),
            config_hash: self.config_hash.clone(),
            partition_id: partition,
        })
    }
}

/// Task for converting a single bag file.
struct ConvertTask {
    output_prefix: String,
    config_hash: String,
    partition_id: PartitionId,
}

#[async_trait::async_trait]
impl Task for ConvertTask {
    async fn execute(&mut self, ctx: &TaskContext) -> Result<TaskResult> {
        tracing::info!(
            task_id = ?ctx.task_id,
            partition = ?self.partition_id,
            output_prefix = %self.output_prefix,
            "Converting bag file to LeRobot format"
        );

        // Simulate conversion work
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let output_path = format!("{}/episode_{:06}.parquet", 
            self.output_prefix, self.partition_id.0);

        Ok(TaskResult {
            outputs: vec![TaskOutput {
                id: output_path,
                size_bytes: 1024,
            }],
            metrics: Default::default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_stage() {
        let stage = ConvertStage::new("s3://bucket/output/", "config_hash_123");
        
        assert_eq!(stage.id(), StageId(1));
        assert_eq!(stage.name(), "convert");
        assert_eq!(stage.dependencies(), vec![StageId(0)]);
    }
}
