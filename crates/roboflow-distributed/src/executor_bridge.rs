// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Bridge between roboflow-executor and roboflow-distributed worker infrastructure.

use std::sync::Arc;

use roboflow_core::Result;
use roboflow_executor::{
    ConvertStage, DiscoverStage, MergeStage, PipelineBuilder, StageExecutor, StageId,
};

use crate::batch::WorkUnit;
use crate::episode::EpisodeAllocator;
use crate::worker::metrics::ProcessingResult;
use crate::worker::registry::JobRegistry;

/// Bridge between new StageExecutor and existing worker infrastructure.
///
/// This adapter allows the new stage-based executor to process WorkUnits
/// from the existing distributed batch system.
pub struct StageExecutorBridge {
    executor: StageExecutor,
    output_prefix: String,
    episode_allocator: Option<Arc<dyn EpisodeAllocator>>,
}

impl StageExecutorBridge {
    /// Create a new bridge.
    pub fn new(max_concurrent: usize, output_prefix: impl Into<String>) -> Self {
        Self {
            executor: StageExecutor::new(max_concurrent),
            output_prefix: output_prefix.into(),
            episode_allocator: None,
        }
    }

    /// Set the episode allocator for distributed processing.
    pub fn with_episode_allocator(mut self, allocator: Arc<dyn EpisodeAllocator>) -> Self {
        self.episode_allocator = Some(allocator);
        self
    }

    /// Execute a work unit using the stage-based pipeline.
    ///
    /// This creates a Discover → Convert → Merge pipeline for each work unit.
    pub async fn execute(
        &self,
        unit: &WorkUnit,
        _job_registry: Arc<tokio::sync::RwLock<JobRegistry>>,
    ) -> Result<ProcessingResult> {
        tracing::info!(
            unit_id = %unit.id,
            files = unit.files.len(),
            "Executing work unit with stage-based pipeline"
        );

        // Create the source prefix from the first file
        let source_prefix = unit
            .files
            .first()
            .map(|f| f.url.clone())
            .unwrap_or_else(|| "file:///tmp/input".to_string());

        // Create output path
        let output_path = format!("{}/{}", self.output_prefix, unit.id);

        // Build the pipeline: Discover → Convert → Merge
        let pipeline = PipelineBuilder::new()
            .stage(Arc::new(DiscoverStage::new(&source_prefix)))
            .stage(Arc::new(ConvertStage::new(&output_path, &unit.config_hash)))
            .stage(Arc::new(MergeStage::new(format!(
                "{}/dataset",
                output_path
            ))))
            .dependency(StageId(1), StageId(0))
            .dependency(StageId(2), StageId(1))
            .build()
            .map_err(|e| {
                roboflow_core::RoboflowError::other(format!("Pipeline build failed: {}", e))
            })?;

        // Execute the pipeline
        let result = self.executor.execute(&pipeline).await?;

        tracing::info!(
            unit_id = %unit.id,
            stages_completed = result.stages_completed,
            tasks_completed = result.tasks_completed,
            duration_secs = result.duration_secs,
            "Pipeline execution complete"
        );

        Ok(ProcessingResult::Success {
            episode_index: 0, // TODO: Get from EpisodeAllocator
            frame_count: result.tasks_completed,
            episode_stats: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::{WorkFile, WorkUnit};

    #[tokio::test]
    async fn test_bridge_execution() {
        let _ = tracing_subscriber::fmt::try_init();

        let bridge = StageExecutorBridge::new(2, "/tmp/output");
        let registry = Arc::new(tokio::sync::RwLock::new(JobRegistry::default()));

        let work_unit = WorkUnit::new(
            "test-batch".to_string(),
            vec![WorkFile::new("file:///tmp/test.bag".to_string(), 1024)],
            "/tmp/output".to_string(),
            "config_hash".to_string(),
        );

        let result = bridge.execute(&work_unit, registry).await;

        assert!(matches!(result, Ok(ProcessingResult::Success { .. })));
    }
}
