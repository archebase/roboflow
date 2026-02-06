// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Staging + Merge coordination for distributed dataset conversion.
//!
//! This module implements the merge coordination pattern for horizontally scalable
//! dataset conversion:
//!
//! # Architecture
//!
//! ```text
//! Workers → Staging Area → Merge Coordinator → Final Dataset
//!          (job-scoped)      (sequential episode_index)
//! ```
//!
//! ## Staging Phase
//!
//! - Each worker writes to its own staging path: `staging/{job_id}/`
//! - No coordination required during processing
//! - Multiple workers can process different input files in parallel
//!
//! ## Merge Phase
//!
//! - When all workers for a job are complete, merge coordinator:
//!   1. Reads all staged parquet files
//!   2. Rewrites episode_index column to be sequential
//!   3. Writes to final location with proper LeRobot v2.1 format
//!
//! ## Key Format
//!
//! - Merge state: `/roboflow/v1/merge/{job_id}`
//! - Merge locks: `/roboflow/v1/locks/merge/{job_id}`

pub mod coordinator;
pub mod executor;
pub mod schema;

pub use coordinator::{
    DEFAULT_MAX_CONCURRENT_MERGES, MergeConfig, MergeCoordinator, MergePermit, MergeResult,
    MergeSemaphore, MergeSemaphoreMetrics,
};
pub use executor::ParquetMergeExecutor;
pub use schema::{MergeState, MergeStatus};
