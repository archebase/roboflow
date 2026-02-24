// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Stage-based task executor for distributed data pipelines.
//!
//! This crate provides a generic execution framework inspired by Spark, Trino, and Ray:
//! - **Stage**: Execution boundary (like Spark stages)
//! - **Task**: Atomic work unit (like Ray tasks)
//! - **Pipeline**: DAG of stages
//! - **Executor**: Stage-aware scheduler with slot-based resource management
//! - **Policy**: Execution strategy (sequential or parallel)
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                         Pipeline                                │
//! │  Stage 0 (Discover) → Stage 1 (Convert) → Stage 2 (Merge)      │
//! └─────────────────────────────────────────────────────────────────┘
//!                              │
//!                              ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                       StageExecutor                             │
//! │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
//! │  │   Slot 0    │  │   Slot 1    │  │   Slot 2    │  ...        │
//! │  └─────────────┘  └─────────────┘  └─────────────┘             │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Execution Policies
//!
//! The [`policy`] module provides execution policies for batch processing:
//!
//! ```rust,ignore
//! use roboflow_executor::policy::{ExecutionPolicy, SequentialPolicy, ParallelPolicy};
//!
//! // Sequential execution
//! let seq_policy = SequentialPolicy;
//! let results = seq_policy.execute_batch(items, |x| process(x));
//!
//! // Parallel execution
//! let par_policy = ParallelPolicy::new(4);
//! let results = par_policy.execute_batch(items, |x| process(x));
//! ```
//!
//! # Data Storage
//!
//! Data is stored via `roboflow-storage::Storage` (S3, OSS, local).
//! Tasks return output paths in `TaskResult::outputs`.

pub mod executor;
pub mod pipeline;
pub mod policy;
pub mod resource;
pub mod scheduler;
pub mod stage;
pub mod task;

// Core types
pub use executor::{ExecuteResult, StageExecutor};
pub use pipeline::{Pipeline, PipelineBuilder, PipelineError};
pub use policy::{ExecutionPolicy, ParallelPolicy, SequentialPolicy};
pub use resource::{
    ResourceCapacity, ResourceRequest, Slot, SlotGuard, SlotId, SlotPool, SlotState,
};
pub use scheduler::StageScheduler;
pub use stage::{PartitionId, Stage, StageId};
pub use task::{Task, TaskContext, TaskId, TaskMetrics, TaskOutput, TaskResult, TaskStatus};

/// Re-export core types
pub use roboflow_core::Result;
