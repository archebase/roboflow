// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Lineage tracking for fault tolerance.
//!
//! Lineage tracks task dependencies for automatic recovery on failure.
//! This is Ray's key fault tolerance mechanism adapted for our executor.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::object_store::{ObjectRef, WorkerId};
use crate::stage::StageId;
use crate::task::TaskId;

/// Lineage tracks task dependencies for recovery.
///
/// From Ray's lineage-based fault tolerance.
#[async_trait::async_trait]
pub trait Lineage: Send + Sync {
    /// Record a task's lineage info.
    async fn record(&self, task: &TaskLineage);

    /// Get all ancestors of a task.
    async fn ancestors(&self, task_id: TaskId) -> Vec<TaskId>;

    /// Check if a task can be recomputed (all inputs available).
    async fn can_recompute(&self, task_id: TaskId) -> bool;

    /// Get recompute plan for failed tasks.
    async fn recompute_plan(&self, failed: &[TaskId],
    ) -> Result<RecomputePlan, LineageError>;

    /// Recompute a lost object from lineage.
    async fn recompute_object(
        &self,
        obj: &ObjectRef,
    ) -> Result<Vec<u8>, LineageError>;
}

/// Task lineage information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskLineage {
    /// Task identifier.
    pub task_id: TaskId,
    /// Function/operation name.
    pub operation: String,
    /// Input object references.
    pub inputs: Vec<ObjectRef>,
    /// Output object references.
    pub outputs: Vec<ObjectRef>,
    /// Deterministic flag (if false, cannot recompute).
    pub deterministic: bool,
    /// Stage this task belongs to.
    pub stage_id: StageId,
    /// Worker that executed the task.
    pub worker_id: WorkerId,
    /// Timestamp when recorded.
    pub recorded_at: chrono::DateTime<chrono::Utc>,
}

impl TaskLineage {
    /// Create a new task lineage record.
    pub fn new(
        task_id: TaskId,
        operation: impl Into<String>,
        stage_id: StageId,
        worker_id: WorkerId,
    ) -> Self {
        Self {
            task_id,
            operation: operation.into(),
            inputs: Vec::new(),
            outputs: Vec::new(),
            deterministic: true,
            stage_id,
            worker_id,
            recorded_at: chrono::Utc::now(),
        }
    }

    /// Mark this task as non-deterministic (cannot be recomputed).
    pub fn non_deterministic(mut self) -> Self {
        self.deterministic = false;
        self
    }

    /// Add input objects.
    pub fn with_inputs(mut self, inputs: Vec<ObjectRef>) -> Self {
        self.inputs = inputs;
        self
    }

    /// Add output objects.
    pub fn with_outputs(mut self, outputs: Vec<ObjectRef>) -> Self {
        self.outputs = outputs;
        self
    }
}

/// Recompute plan for failed tasks.
#[derive(Debug, Clone)]
pub struct RecomputePlan {
    /// Tasks to recompute in order.
    pub tasks: Vec<TaskId>,
    /// Objects that will be produced.
    pub objects: Vec<ObjectRef>,
}

/// Error type for lineage operations.
#[derive(Debug, thiserror::Error)]
pub enum LineageError {
    #[error("Task not found: {0}")]
    TaskNotFound(TaskId),
    #[error("Object not found: {0}")]
    ObjectNotFound(String),
    #[error("Cannot recompute non-deterministic task: {0}")]
    NonDeterministic(TaskId),
    #[error("Missing input: {0}")]
    MissingInput(String),
    #[error("Cycle detected in lineage graph")]
    CycleDetected,
    #[error("Storage error: {0}")]
    Storage(String),
}

/// In-memory lineage tracker implementation.
pub struct MemoryLineage {
    /// Task lineage records.
    tasks: RwLock<HashMap<TaskId, TaskLineage>>,
    /// Object to task mapping (object owner).
    object_owners: RwLock<HashMap<String, TaskId>>,
}

impl MemoryLineage {
    /// Create a new empty lineage tracker.
    pub fn new() -> Self {
        Self {
            tasks: RwLock::new(HashMap::new()),
            object_owners: RwLock::new(HashMap::new()),
        }
    }

    /// Get task lineage by ID.
    pub async fn get_task(&self, task_id: TaskId) -> Option<TaskLineage> {
        let tasks = self.tasks.read().await;
        tasks.get(&task_id).cloned()
    }
}

impl Default for MemoryLineage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Lineage for MemoryLineage {
    async fn record(&self, task: &TaskLineage) {
        let mut tasks = self.tasks.write().await;
        tasks.insert(task.task_id, task.clone());

        // Record object ownership
        let mut owners = self.object_owners.write().await;
        for output in &task.outputs {
            owners.insert(output.id.to_string(), task.task_id);
        }
    }

    async fn ancestors(&self,
        task_id: TaskId,
    ) -> Vec<TaskId> {
        let tasks = self.tasks.read().await;
        let mut ancestors = Vec::new();
        let mut to_visit = vec![task_id];
        let mut visited = std::collections::HashSet::new();

        while let Some(current_id) = to_visit.pop() {
            if visited.contains(&current_id) {
                continue;
            }
            visited.insert(current_id);

            if let Some(task) = tasks.get(&current_id) {
                for input in &task.inputs {
                    if let Some(owner) = self.object_owners.read().await.get(&input.id.to_string()) {
                        if *owner != current_id {
                            ancestors.push(*owner);
                            to_visit.push(*owner);
                        }
                    }
                }
            }
        }

        ancestors
    }

    async fn can_recompute(&self,
        task_id: TaskId,
    ) -> bool {
        let tasks = self.tasks.read().await;

        if let Some(task) = tasks.get(&task_id) {
            if !task.deterministic {
                return false;
            }

            // Check all inputs are available (have owners)
            for input in &task.inputs {
                if !self.object_owners.read().await.contains_key(&input.id.to_string()) {
                    return false;
                }
            }

            true
        } else {
            false
        }
    }

    async fn recompute_plan(
        &self,
        failed: &[TaskId],
    ) -> Result<RecomputePlan, LineageError> {
        let mut plan = Vec::new();
        let mut to_process: Vec<TaskId> = failed.to_vec();
        let mut processed = std::collections::HashSet::new();

        while let Some(task_id) = to_process.pop() {
            if processed.contains(&task_id) {
                continue;
            }

            // Check if task can be recomputed
            if !self.can_recompute(task_id).await {
                return Err(LineageError::NonDeterministic(task_id));
            }

            // Add ancestors first (they need to be recomputed before this task)
            let ancestors = self.ancestors(task_id).await;
            for ancestor in ancestors {
                if !processed.contains(&ancestor) {
                    to_process.push(ancestor);
                }
            }

            plan.push(task_id);
            processed.insert(task_id);
        }

        // Reverse to get correct execution order (ancestors first)
        plan.reverse();

        Ok(RecomputePlan {
            tasks: plan,
            objects: Vec::new(), // Would be populated in real implementation
        })
    }

    async fn recompute_object(
        &self,
        obj: &ObjectRef,
    ) -> Result<Vec<u8>, LineageError> {
        let owners = self.object_owners.read().await;

        let task_id = owners.get(&obj.id.to_string()).copied();
        drop(owners);

        if let Some(task_id) = task_id {
            if !self.can_recompute(task_id).await {
                return Err(LineageError::NonDeterministic(task_id));
            }

            Err(LineageError::Storage(
                "Object recomputation not implemented".to_string()
            ))
        } else {
            Err(LineageError::ObjectNotFound(obj.id.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object_store::ObjectId;

    #[tokio::test]
    async fn test_lineage_record_and_retrieve() {
        let lineage = MemoryLineage::new();

        let task = TaskLineage::new(
            TaskId(1),
            "test_task",
            StageId(0),
            WorkerId(1),
        );

        lineage.record(&task).await;

        let retrieved = lineage.get_task(TaskId(1)).await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().operation, "test_task");
    }

    #[tokio::test]
    async fn test_lineage_ancestors() {
        let lineage = MemoryLineage::new();

        // Task 1 produces obj1
        let obj1 = ObjectRef::new(ObjectId::new([1u8; 32]), 100, TaskId(1), vec![]);
        let task1 = TaskLineage::new(
            TaskId(1),
            "task1",
            StageId(0),
            WorkerId(1),
        ).with_outputs(vec![obj1.clone()]);

        // Task 2 consumes obj1
        let task2 = TaskLineage::new(
            TaskId(2),
            "task2",
            StageId(1),
            WorkerId(1),
        ).with_inputs(vec![obj1]);

        lineage.record(&task1).await;
        lineage.record(&task2).await;

        let ancestors = lineage.ancestors(TaskId(2)).await;
        assert_eq!(ancestors, vec![TaskId(1)]);
    }

    #[tokio::test]
    async fn test_lineage_can_recompute() {
        let lineage = MemoryLineage::new();

        let obj1 = ObjectRef::new(ObjectId::new([1u8; 32]), 100, TaskId(1), vec![]);
        let task = TaskLineage::new(
            TaskId(1),
            "task1",
            StageId(0),
            WorkerId(1),
        ).with_outputs(vec![obj1.clone()]);

        lineage.record(&task).await;

        assert!(lineage.can_recompute(TaskId(1)).await);

        // Non-deterministic task cannot be recomputed
        let task_nd = TaskLineage::new(
            TaskId(2),
            "task2",
            StageId(0),
            WorkerId(1),
        ).non_deterministic();

        lineage.record(&task_nd).await;
        assert!(!lineage.can_recompute(TaskId(2)).await);
    }
}
