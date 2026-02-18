// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Transform stage for converting input files to episodes.
//!
//! This stage is format-specific and transforms bag/MCAP files into
//! episodes using the format's writer (e.g., LeRobotWriter).

use roboflow_core::Result;

use crate::object_store::{ObjectId, ObjectRef};
use crate::resource::ResourceRequest;
use crate::stage::{PartitionId, Stage, StageId};
use crate::task::{Task, TaskContext, TaskResult, TaskStatus};

/// Stage for transforming input files to episodes.
///
/// This stage is format-specific. Each partition processes one input file
/// and produces one episode.
pub struct TransformStage {
    output_prefix: String,
    partition_count: usize,
}

impl TransformStage {
    /// Create a new transform stage.
    ///
    /// # Arguments
    ///
    /// * `output_prefix` - Output path prefix.
    /// * `partition_count` - Number of partitions (one per input file).
    pub fn new(output_prefix: impl Into<String>, partition_count: usize) -> Self {
        Self {
            output_prefix: output_prefix.into(),
            partition_count,
        }
    }
}

impl Stage for TransformStage {
    fn id(&self) -> StageId {
        StageId(1)
    }

    fn name(&self) -> &str {
        "transform"
    }

    fn partition_count(&self) -> usize {
        self.partition_count
    }

    fn dependencies(&self) -> Vec<StageId> {
        vec![StageId(0)]
    }

    fn create_task(&self, partition: PartitionId) -> Box<dyn Task> {
        Box::new(TransformTask {
            output_prefix: self.output_prefix.clone(),
            partition_id: partition,
        })
    }

    fn resource_profile(&self) -> ResourceRequest {
        // Transform tasks need more resources for video encoding
        ResourceRequest::new(2.0, 4.0, 0)
    }
}

/// Task for transforming a single input file.
struct TransformTask {
    output_prefix: String,
    partition_id: PartitionId,
}

#[async_trait::async_trait]
impl Task for TransformTask {
    async fn execute(&mut self, ctx: &TaskContext) -> Result<TaskResult> {
        tracing::info!(
            task_id = ?ctx.task_id,
            partition = ?self.partition_id,
            output_prefix = %self.output_prefix,
            "Transforming input file to episode"
        );

        // Simulate transformation work
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Create object ref for episode metadata
        let obj_ref = ObjectRef::new(
            ObjectId::new([3u8; 32]),
            2048,
            ctx.task_id,
            vec![],
        );

        Ok(TaskResult {
            outputs: vec![obj_ref],
            metrics: Default::default(),
            status: TaskStatus::Success,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transform_stage() {
        let stage = TransformStage::new("s3://bucket/output/", 4);

        assert_eq!(stage.id(), StageId(1));
        assert_eq!(stage.name(), "transform");
        assert_eq!(stage.dependencies(), vec![StageId(0)]);
        assert_eq!(stage.partition_count(), 4);
    }
}
