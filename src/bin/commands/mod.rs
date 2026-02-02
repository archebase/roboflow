// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! CLI commands for job submission and management.
//!
//! This module provides commands for:
//! - Submitting jobs to the distributed queue
//! - Querying job status
//! - Managing jobs (cancel, retry, delete)

pub mod jobs;
pub mod submit;
pub mod utils;

pub use jobs::run_jobs_command;
pub use submit::run_submit_command;
