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
pub mod format;
pub mod lineage;
pub mod object_store;
pub mod pipeline;
pub mod resource;
pub mod scheduler;
pub mod stage;
pub mod task;

// Core types
pub use executor::{ExecuteResult, StageExecutor};
pub use format::{
    ConfigError, DatasetFormat, DatasetMetadata, EpisodeMetadata, EpisodeWriter, Feature,
    FormatConfig, Frame, LeRobotV21, MetadataError, MetadataGenerator, Observation, RLDS,
    WriterError,
};
pub use lineage::{Lineage, LineageError, MemoryLineage, RecomputePlan, TaskLineage};
pub use object_store::{
    LocalObjectStore, MemoryObjectStore, ObjectId, ObjectRef, ObjectStore, ObjectStoreError,
    WorkerId,
};
pub use pipeline::{Pipeline, PipelineBuilder, PipelineError};
pub use resource::{
    ResourceCapacity, ResourceRequest, Slot, SlotGuard, SlotId, SlotPool, SlotState,
};
pub use scheduler::StageScheduler;
pub use stage::{FormatStage, PartitionId, Stage, StageId};
pub use task::{Task, TaskContext, TaskId, TaskMetrics, TaskResult, TaskStatus};

/// Re-export core types
pub use roboflow_core::Result;
