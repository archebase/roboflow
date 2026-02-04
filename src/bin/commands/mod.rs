// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! CLI commands for job submission and management.
//!
//! This module provides commands for:
//! - Submitting jobs to the distributed queue
//! - Querying job status
//! - Managing jobs (cancel, retry, delete)
//! - Batch job management (10,000+ file processing)

pub mod audit;
pub mod batch;
pub mod jobs;
pub mod submit;
pub mod utils;

pub use batch::run_batch_command;
pub use jobs::run_jobs_command;
pub use submit::run_submit_command;
