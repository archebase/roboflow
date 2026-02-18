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

pub mod executor;
pub mod pipeline;
pub mod stage;
pub mod stages;
pub mod task;

pub use executor::{ExecuteResult, StageExecutor};
pub use pipeline::{Pipeline, PipelineBuilder};
pub use stage::{PartitionId, Stage, StageId};
pub use stages::{ConvertStage, DiscoverStage, MergeStage};
pub use task::{Task, TaskContext, TaskId, TaskResult, TaskOutput, TaskMetrics};

/// Re-export core types
pub use roboflow_core::Result;
