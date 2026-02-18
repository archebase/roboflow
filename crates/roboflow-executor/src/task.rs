// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Task trait for atomic work units.

use std::fmt;

use roboflow_core::Result;

/// Unique identifier for a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskId(pub u64);

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Task({})", self.0)
    }
}

/// A task is an atomic, idempotent unit of work.
///
/// Tasks are the smallest unit of execution. They are deterministic,
/// retryable, and trackable via lineage (for fault tolerance).
///
/// Inspired by Ray's task model.
///
/// # Example
///
/// ```rust,ignore
/// use roboflow_executor::{Task, TaskContext, TaskResult};
///
/// struct ConvertTask {
///     input_path: String,
/// }
///
/// #[async_trait]
/// impl Task for ConvertTask {
///     async fn execute(&mut self, ctx: &TaskContext) -> Result<TaskResult> {
///         // Convert input file to output format
///         let output = convert_file(&self.input_path).await?;
///
///         Ok(TaskResult {
///             outputs: vec![output],
///             metrics: TaskMetrics::default(),
///         })
///     }
/// }
/// ```
#[async_trait::async_trait]
pub trait Task: Send {
    /// Execute the task.
    ///
    /// This method must be idempotent - it should produce the same
    /// result when called multiple times with the same inputs.
    async fn execute(&mut self, ctx: &TaskContext) -> Result<TaskResult>;
}

/// Context provided to task execution.
///
/// Contains resources and metadata for the task.
pub struct TaskContext {
    /// Task identifier.
    pub task_id: TaskId,

    /// Stage this task belongs to.
    pub stage_id: crate::stage::StageId,

    /// Partition this task processes.
    pub partition: crate::stage::PartitionId,

    /// Cancellation token for cooperative cancellation.
    pub cancel: tokio::sync::watch::Receiver<bool>,
}

impl TaskContext {
    /// Check if cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        *self.cancel.borrow()
    }
}

/// Result of task execution.
pub struct TaskResult {
    /// Output references (object IDs or paths).
    pub outputs: Vec<TaskOutput>,

    /// Metrics collected during execution.
    pub metrics: TaskMetrics,
}

/// Output reference from a task.
#[derive(Debug, Clone)]
pub struct TaskOutput {
    /// Output identifier or path.
    pub id: String,

    /// Output size in bytes.
    pub size_bytes: u64,
}

/// Metrics collected during task execution.
#[derive(Debug, Clone, Default)]
pub struct TaskMetrics {
    /// Wall-clock execution time.
    pub duration_secs: f64,

    /// CPU time used.
    pub cpu_secs: f64,

    /// Memory peak in bytes.
    pub memory_peak_bytes: u64,

    /// Bytes read.
    pub bytes_read: u64,

    /// Bytes written.
    pub bytes_written: u64,
}
