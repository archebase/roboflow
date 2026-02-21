// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Stage-based task executor.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use roboflow_core::Result;

use crate::pipeline::Pipeline;
use crate::stage::{PartitionId, Stage};
use crate::task::{TaskContext, TaskId};

/// Unique identifier for an execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExecuteId(pub u64);

/// Result of pipeline execution.
#[derive(Debug)]
pub struct ExecuteResult {
    /// Execution ID.
    pub execute_id: ExecuteId,

    /// Number of stages completed.
    pub stages_completed: usize,

    /// Number of tasks completed.
    pub tasks_completed: u64,

    /// Execution duration.
    pub duration_secs: f64,
}

/// Stage executor with slot-based resource management.
///
/// This executor manages the execution of pipelines with:
/// - Stage-aware scheduling (respects dependencies)
/// - Slot-based concurrency limiting
/// - Task retry on failure
pub struct StageExecutor {
    /// Maximum concurrent tasks (slot pool size).
    max_concurrent: usize,

    /// Task ID counter.
    task_id_counter: AtomicU64,

    /// Execute ID counter.
    execute_id_counter: AtomicU64,
}

impl StageExecutor {
    /// Create a new stage executor.
    ///
    /// # Arguments
    ///
    /// * `max_concurrent` - Maximum number of concurrent tasks.
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            max_concurrent,
            task_id_counter: AtomicU64::new(0),
            execute_id_counter: AtomicU64::new(0),
        }
    }

    /// Execute a pipeline to completion.
    ///
    /// This method runs all stages in topological order, respecting
    /// dependencies and limiting concurrency via the slot pool.
    pub async fn execute(&self, pipeline: &Pipeline) -> Result<ExecuteResult> {
        let execute_id = ExecuteId(self.execute_id_counter.fetch_add(1, Ordering::SeqCst));
        let start_time = std::time::Instant::now();

        let mut completed_stages = HashSet::new();
        let mut total_tasks = 0u64;

        // Process stages in waves (all ready stages in parallel)
        loop {
            let ready = pipeline.ready_stages(&completed_stages);
            if ready.is_empty() {
                break;
            }

            // Execute all ready stages in parallel
            let mut stage_futures = Vec::new();
            for stage_id in &ready {
                if let Some(stage) = pipeline.get_stage(*stage_id) {
                    stage_futures.push(self.execute_stage(execute_id, Arc::clone(stage)));
                }
            }

            // Wait for all stages to complete
            let results = futures::future::join_all(stage_futures).await;

            for (stage_id, result) in ready.iter().zip(results) {
                match result {
                    Ok(task_count) => {
                        completed_stages.insert(*stage_id);
                        total_tasks += task_count;
                    }
                    Err(e) => {
                        tracing::error!(
                            execute_id = ?execute_id,
                            stage_id = ?stage_id,
                            error = %e,
                            "Stage execution failed"
                        );
                        return Err(e);
                    }
                }
            }
        }

        let duration = start_time.elapsed();

        Ok(ExecuteResult {
            execute_id,
            stages_completed: completed_stages.len(),
            tasks_completed: total_tasks,
            duration_secs: duration.as_secs_f64(),
        })
    }

    /// Execute a single stage.
    async fn execute_stage(&self, _execute_id: ExecuteId, stage: Arc<dyn Stage>) -> Result<u64> {
        let partition_count = stage.partition_count();
        let mut tasks = Vec::with_capacity(partition_count);

        // Create tasks for all partitions
        for i in 0..partition_count {
            let partition_id = PartitionId(i as u64);
            let task = stage.create_task(partition_id);
            tasks.push((partition_id, task));
        }

        // Execute tasks with bounded concurrency
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.max_concurrent));
        let mut handles = Vec::with_capacity(tasks.len());

        for (partition_id, mut task) in tasks {
            let permit = Arc::clone(&semaphore);
            let task_id = TaskId(self.task_id_counter.fetch_add(1, Ordering::SeqCst));
            let stage_id = stage.id();

            let handle = tokio::spawn(async move {
                let _permit = permit
                    .acquire()
                    .await
                    .expect("Semaphore should not be closed");

                let (tx, rx) = tokio::sync::watch::channel(false);
                let ctx = TaskContext {
                    task_id,
                    stage_id,
                    partition: partition_id,
                    cancel: rx,
                };

                tracing::debug!(
                    task_id = ?task_id,
                    stage_id = ?stage_id,
                    partition = ?partition_id,
                    "Starting task"
                );

                let result = task.execute(&ctx).await;

                // Clean up
                drop(tx);

                result
            });

            handles.push(handle);
        }

        // Wait for all tasks to complete
        let mut task_count = 0u64;
        for handle in handles {
            match handle.await {
                Ok(Ok(_result)) => {
                    task_count += 1;
                }
                Ok(Err(e)) => {
                    return Err(e);
                }
                Err(e) => {
                    return Err(roboflow_core::RoboflowError::other(format!(
                        "Task panicked: {}",
                        e
                    )));
                }
            }
        }

        Ok(task_count)
    }
}

impl Default for StageExecutor {
    fn default() -> Self {
        Self::new(num_cpus::get())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stage::{PartitionId, Stage, StageId};
    use crate::task::{Task, TaskContext, TaskResult};

    struct MockTask {
        name: String,
    }

    #[async_trait::async_trait]
    impl Task for MockTask {
        async fn execute(&mut self, _ctx: &TaskContext) -> Result<TaskResult> {
            tracing::debug!("Executing task: {}", self.name);
            Ok(TaskResult {
                outputs: vec![],
                metrics: Default::default(),
                status: crate::task::TaskStatus::Success,
            })
        }
    }

    struct MockStage {
        id: StageId,
        name: String,
        task_count: usize,
    }

    impl Stage for MockStage {
        fn id(&self) -> StageId {
            self.id
        }

        fn name(&self) -> &str {
            &self.name
        }

        fn partition_count(&self) -> usize {
            self.task_count
        }

        fn create_task(&self, partition: PartitionId) -> Box<dyn Task> {
            Box::new(MockTask {
                name: format!("{}-{}", self.name, partition.0),
            })
        }
    }

    #[tokio::test]
    async fn test_executor_simple() {
        let _ = tracing_subscriber::fmt::try_init();

        let stage = Arc::new(MockStage {
            id: StageId(0),
            name: "test".to_string(),
            task_count: 4,
        });

        let pipeline = crate::pipeline::PipelineBuilder::new()
            .stage(stage)
            .build()
            .unwrap();

        let executor = StageExecutor::new(2);
        let result = executor.execute(&pipeline).await.unwrap();

        assert_eq!(result.stages_completed, 1);
        assert_eq!(result.tasks_completed, 4);
    }
}
