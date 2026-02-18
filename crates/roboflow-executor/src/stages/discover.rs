// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Discover stage for finding input files.

use roboflow_core::Result;

use crate::object_store::{ObjectRef, ObjectId};
use crate::stage::{PartitionId, Stage, StageId};
use crate::task::{Task, TaskContext, TaskResult, TaskStatus};

/// Stage for discovering input files.
///
/// This stage scans a source prefix (local or cloud storage) and
/// identifies files to be processed. It produces a list of file URLs
/// as output.
///
/// # Output
///
/// A list of discovered file URLs (one per line in a text output).
pub struct DiscoverStage {
    source_prefix: String,
}

impl DiscoverStage {
    /// Create a new discover stage.
    ///
    /// # Arguments
    ///
    /// * `source_prefix` - URL prefix to scan (e.g., `s3://bucket/input/`).
    pub fn new(source_prefix: impl Into<String>) -> Self {
        Self {
            source_prefix: source_prefix.into(),
        }
    }
}

impl Stage for DiscoverStage {
    fn id(&self) -> StageId {
        StageId(0)
    }

    fn name(&self) -> &str {
        "discover"
    }

    fn partition_count(&self) -> usize {
        1
    }

    fn create_task(&self, _partition: PartitionId) -> Box<dyn Task> {
        Box::new(DiscoverTask {
            source_prefix: self.source_prefix.clone(),
        })
    }
}

/// Task for discovering input files.
struct DiscoverTask {
    source_prefix: String,
}

#[async_trait::async_trait]
impl Task for DiscoverTask {
    async fn execute(&mut self, ctx: &TaskContext) -> Result<TaskResult> {
        tracing::info!(
            source_prefix = %self.source_prefix,
            "Discovering input files"
        );

        // For now, return the source prefix as a single file
        // In a real implementation, this would scan the storage backend
        let file_list = format!(
            "{}/file1.bag\n{}/file2.bag",
            self.source_prefix, self.source_prefix
        );

        // Create a simple object ref from the file list
        let data = file_list.into_bytes();
        let obj_ref = ObjectRef::new(
            ObjectId::new([1u8; 32]), // In real impl, would hash the data
            data.len() as u64,
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
    fn test_discover_stage() {
        let stage = DiscoverStage::new("s3://bucket/input/");

        assert_eq!(stage.id(), StageId(0));
        assert_eq!(stage.name(), "discover");
        assert_eq!(stage.partition_count(), 1);
    }
}
