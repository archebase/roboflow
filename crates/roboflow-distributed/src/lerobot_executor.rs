// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! LeRobot executor using the stage-based executor framework.

use std::sync::Arc;

use roboflow_core::Result;
use roboflow_executor::{PipelineBuilder, StageExecutor, StageId};

use crate::stages::{ConvertStage, MergeStage};

use crate::batch::WorkUnit;
use crate::episode::EpisodeAllocator;
use crate::worker::metrics::ProcessingResult;
use crate::worker::registry::JobRegistry;

/// Executes bag/mcap files to LeRobot format using the stage-based executor framework.
///
/// This executor processes source files and converts them to LeRobot v2.1 format
/// by creating a Discover → Convert → Merge pipeline for each work unit.
/// Uses parallel processing for maximum throughput.
pub struct LeRobotExecutor {
    stage_executor: StageExecutor,
    output_prefix: String,
    episode_allocator: Option<Arc<dyn EpisodeAllocator>>,
}

impl LeRobotExecutor {
    /// Create a new LeRobot executor.
    pub fn new(max_concurrent: usize, output_prefix: impl Into<String>) -> Self {
        Self {
            stage_executor: StageExecutor::new(max_concurrent),
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
    /// This creates a Convert → Merge pipeline for each work unit.
    /// (Discovery is done at the batch level, not per-work-unit)
    pub async fn execute(
        &self,
        unit: &WorkUnit,
        _job_registry: Arc<tokio::sync::RwLock<JobRegistry>>,
    ) -> Result<ProcessingResult> {
        // Ensure sources are registered
        roboflow_pipeline::sources::register_builtin_sources();
        tracing::info!(
            unit_id = %unit.id,
            files = unit.files.len(),
            "Executing work unit with stage-based pipeline"
        );

        // Get the input file from the work unit
        let input_file =
            unit.files.first().map(|f| f.url.clone()).ok_or_else(|| {
                roboflow_core::RoboflowError::other("No input files in work unit")
            })?;

        // Create output path
        let output_path = format!("{}/{}", self.output_prefix, unit.id);

        // Build the pipeline: Convert → Merge
        // (DiscoverStage runs at batch level, not per-work-unit)
        let pipeline = PipelineBuilder::new()
            .stage(Arc::new(ConvertStage::new(
                &input_file,
                &output_path,
                &unit.config_hash,
            )))
            .stage(Arc::new(MergeStage::new(format!(
                "{}/dataset",
                output_path
            ))))
            .dependency(StageId(2), StageId(1))
            .build()
            .map_err(|e| {
                roboflow_core::RoboflowError::other(format!("Pipeline build failed: {}", e))
            })?;

        // Execute the pipeline
        let result = self.stage_executor.execute(&pipeline).await?;

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
    #[ignore = "Requires registered sources and real bag file - run manually"]
    async fn test_bridge_execution() {
        let _ = tracing_subscriber::fmt::try_init();

        let executor = LeRobotExecutor::new(2, "/tmp/output");
        let registry = Arc::new(tokio::sync::RwLock::new(JobRegistry::default()));

        let work_unit = WorkUnit::new(
            "test-batch".to_string(),
            vec![WorkFile::new("file:///tmp/test.bag".to_string(), 1024)],
            "/tmp/output".to_string(),
            "config_hash".to_string(),
        );

        let result = executor.execute(&work_unit, registry).await;

        if let Err(ref e) = result {
            eprintln!("Executor failed: {}", e);
        }

        assert!(matches!(result, Ok(ProcessingResult::Success { .. })));
    }
}
