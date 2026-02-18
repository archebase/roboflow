// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Stage trait for execution boundaries.

use std::fmt;

/// Unique identifier for a stage.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct StageId(pub u64);

impl fmt::Display for StageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Stage({})", self.0)
    }
}

/// A stage represents a phase in the execution pipeline.
///
/// Stages are execution boundaries where tasks can run in parallel
/// without cross-task data exchange. Stages are separated by shuffles
/// (data redistribution).
///
/// Inspired by Spark's DAG stages and Trino's query stages.
///
/// # Example
///
/// ```rust,ignore
/// use roboflow_executor::{Stage, StageId, Task};
///
/// struct DiscoverStage {
///     source_prefix: String,
/// }
///
/// impl Stage for DiscoverStage {
///     fn id(&self) -> StageId {
///         StageId(0)
///     }
///
///     fn name(&self) -> &str {
///         "discover"
///     }
///
///     fn partition_count(&self) -> usize {
///         1 // Single discovery task
///     }
///
///     fn create_task(&self, partition: PartitionId) -> Box<dyn Task> {
///         Box::new(DiscoverTask {
///             source_prefix: self.source_prefix.clone(),
///         })
///     }
/// }
/// ```
pub trait Stage: Send + Sync {
    /// Stage identifier (unique within a pipeline).
    fn id(&self) -> StageId;

    /// Stage name for observability.
    fn name(&self) -> &str;

    /// Number of output partitions (parallelism).
    fn partition_count(&self) -> usize;

    /// Create a task for a specific partition.
    fn create_task(&self, partition: PartitionId) -> Box<dyn crate::task::Task>;

    /// Dependency stages that must complete first.
    fn dependencies(&self) -> Vec<StageId> {
        Vec::new()
    }

    /// Resource requirements for this stage.
    fn resource_profile(&self) -> crate::resource::ResourceRequest {
        crate::resource::ResourceRequest::default()
    }
}

/// Marker trait for format-specific stages.
pub trait FormatStage<F: crate::format::DatasetFormat>: Stage {
    /// Get the format name.
    fn format_name(&self) -> &'static str {
        F::NAME
    }
}

/// Partition identifier within a stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PartitionId(pub u64);

impl fmt::Display for PartitionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Partition({})", self.0)
    }
}
