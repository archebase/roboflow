// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Job registry for tracking active jobs and their cancellation tokens.

use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Registry for tracking active jobs and their cancellation tokens.
///
/// # Architecture
///
/// This implements a **batch cancellation monitoring** pattern that addresses
/// the scalability issues of per-job monitoring:
///
/// - **Before (Per-Job Monitoring)**: Each job spawned a monitor task → O(n) tasks
/// - **After (Batch Monitoring)**: Single monitor per worker → O(1) task
///
/// # Performance Impact
///
/// For 1000 concurrent jobs:
/// - Per-job: 1000 monitor tasks × 5s interval = 200 QPS to TiKV
/// - Batch: 1 monitor task × 5s interval = 0.2 QPS to TiKV (1000× reduction)
///
/// # Extension Points
///
/// To implement alternative monitoring strategies (e.g., push-based notifications),
/// the JobRegistry can be extended to:
/// - Support different backends (in-memory, Redis, etc.)
/// - Implement different polling strategies
/// - Add event-driven notification mechanisms
///
/// The current implementation prioritizes simplicity and immediate scalability
/// improvements over full abstraction.
#[derive(Debug, Default)]
pub struct JobRegistry {
    /// Map of job_id -> cancellation_token for active jobs.
    pub(crate) active_jobs: HashMap<String, Arc<CancellationToken>>,
}

impl JobRegistry {
    /// Register a job for cancellation monitoring.
    pub fn register(&mut self, job_id: String, token: Arc<CancellationToken>) {
        self.active_jobs.insert(job_id, token);
    }

    /// Unregister a job from cancellation monitoring.
    pub fn unregister(&mut self, job_id: &str) {
        self.active_jobs.remove(job_id);
    }

    /// Get all registered job IDs.
    pub fn job_ids(&self) -> Vec<String> {
        self.active_jobs.keys().cloned().collect()
    }

    /// Cancel a specific job by ID.
    pub fn cancel_job(&mut self, job_id: &str) {
        if let Some(token) = self.active_jobs.get(job_id) {
            token.cancel();
        }
    }
}
