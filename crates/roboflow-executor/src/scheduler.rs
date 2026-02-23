// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Stage scheduler for orchestrating pipeline execution.
//!
//! The StageScheduler orchestrates execution of a DAG of stages. It:
//! 1. Determines which stages are ready to execute (dependencies satisfied)
//! 2. Schedules tasks within each stage to available slots
//! 3. On failure, the entire pipeline is retried (no lineage tracking needed)

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use roboflow_core::Result;

use crate::pipeline::Pipeline;
use crate::resource::{ResourceRequest, SlotPool};
use crate::stage::{PartitionId, Stage, StageId};
use crate::task::{Task, TaskContext, TaskId, TaskResult};

/// Stage scheduler orchestrates stage execution.
pub struct StageScheduler {
    /// Slot pool for resource management.
    slot_pool: Arc<SlotPool>,
}

impl StageScheduler {
    /// Create a new stage scheduler.
    pub fn new(slot_pool: Arc<SlotPool>) -> Self {
        Self { slot_pool }
    }

    /// Execute a pipeline to completion.
    pub async fn execute_pipeline(&self, pipeline: &Pipeline) -> Result<PipelineResult> {
        let mut completed_stages = HashSet::new();
        let mut stage_outputs: HashMap<StageId, Vec<String>> = HashMap::new();

        loop {
            let ready_stages = pipeline.ready_stages(&completed_stages);
            if ready_stages.is_empty() {
                break;
            }

            // Execute ready stages in parallel
            for stage_id in ready_stages {
                if let Some(stage) = pipeline.get_stage(stage_id) {
                    let outputs = self.execute_stage(stage, &stage_outputs).await?;
                    stage_outputs.insert(stage_id, outputs);
                    completed_stages.insert(stage_id);
                }
            }
        }

        // Collect final outputs from terminal stages
        let final_outputs: Vec<String> = pipeline
            .stages()
            .keys()
            .filter(|id| {
                // Terminal stages have no dependents
                !pipeline
                    .stages()
                    .values()
                    .any(|s| s.dependencies().contains(id))
            })
            .flat_map(|id| stage_outputs.get(id).cloned().unwrap_or_default())
            .collect();

        Ok(PipelineResult {
            stage_outputs,
            final_outputs,
        })
    }

    /// Execute a single stage.
    async fn execute_stage(
        &self,
        stage: &Arc<dyn Stage>,
        _previous_outputs: &HashMap<StageId, Vec<String>>,
    ) -> Result<Vec<String>> {
        let partition_count = stage.partition_count();
        let mut tasks: Vec<Box<dyn Task>> = Vec::with_capacity(partition_count);

        // Create tasks for all partitions
        for i in 0..partition_count {
            let partition_id = PartitionId(i as u64);
            let task = stage.create_task(partition_id);
            tasks.push(task);
        }

        // Execute tasks with bounded concurrency
        let mut results: Vec<TaskResult> = Vec::with_capacity(tasks.len());

        for mut task in tasks {
            // Acquire a slot
            let slot_guard = self
                .slot_pool
                .acquire(&ResourceRequest::default(), TaskId(0))
                .await
                .ok_or_else(|| {
                    roboflow_core::RoboflowError::other("No available slots".to_string())
                })?;

            // Create task context
            let (_tx, rx) = tokio::sync::watch::channel(false);
            let ctx = TaskContext {
                task_id: TaskId(0),
                stage_id: stage.id(),
                partition: PartitionId(0),
                cancel: rx,
            };

            // Execute task
            let result = task.execute(&ctx).await?;

            // Release slot
            drop(slot_guard);

            results.push(result);
        }

        // Collect output paths
        let outputs: Vec<String> = results.into_iter().flat_map(|r| r.outputs).collect();

        Ok(outputs)
    }
}

/// Result of pipeline execution.
#[derive(Debug)]
pub struct PipelineResult {
    /// Outputs from all stages (output paths).
    pub stage_outputs: HashMap<StageId, Vec<String>>,
    /// Final outputs from terminal stages (output paths).
    pub final_outputs: Vec<String>,
}
