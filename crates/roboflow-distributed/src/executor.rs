// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Executor trait for processing work units.
//!
//! This trait abstracts the execution of work units, allowing different
//! executors (LeRobot, TFDS, RLDS, etc.) to be used interchangeably.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::batch::WorkUnit;
use crate::worker::metrics::ProcessingResult;
use crate::worker::registry::JobRegistry;

/// Trait for executing work units.
///
/// Implementors of this trait handle the actual processing of work units,
/// such as converting bag/mcap files to various output formats.
///
/// # Example
///
/// ```rust,ignore
/// use roboflow_distributed::{Executor, WorkUnit};
///
/// struct MyExecutor;
///
/// #[async_trait::async_trait]
/// impl Executor for MyExecutor {
///     async fn execute(
///         &self,
///         work_unit: &WorkUnit,
///         job_registry: Arc<RwLock<JobRegistry>>,
///     ) -> Result<ProcessingResult, roboflow_core::RoboflowError> {
///         // Process the work unit
///         Ok(ProcessingResult::Success { ... })
///     }
/// }
/// ```
#[async_trait::async_trait]
pub trait Executor: Send + Sync {
    /// Execute a work unit.
    ///
    /// # Arguments
    ///
    /// * `work_unit` - The work unit to process
    /// * `job_registry` - Registry for tracking and canceling jobs
    ///
    /// # Returns
    ///
    /// The result of the execution
    async fn execute(
        &self,
        work_unit: &WorkUnit,
        job_registry: Arc<RwLock<JobRegistry>>,
    ) -> crate::Result<ProcessingResult>;
}

// Implement the trait for LeRobotExecutor
#[async_trait::async_trait]
impl Executor for crate::lerobot_executor::LeRobotExecutor {
    async fn execute(
        &self,
        work_unit: &WorkUnit,
        job_registry: Arc<RwLock<JobRegistry>>,
    ) -> crate::Result<ProcessingResult> {
        self.execute(work_unit, job_registry).await
    }
}

// Implement the trait for Box<dyn Executor> to allow dynamic dispatch
#[async_trait::async_trait]
impl Executor for Box<dyn Executor> {
    async fn execute(
        &self,
        work_unit: &WorkUnit,
        job_registry: Arc<RwLock<JobRegistry>>,
    ) -> crate::Result<ProcessingResult> {
        self.as_ref().execute(work_unit, job_registry).await
    }
}
