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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_registry_new() {
        let registry = JobRegistry::default();
        assert!(registry.job_ids().is_empty());
    }

    #[test]
    fn test_job_registry_register() {
        let mut registry = JobRegistry::default();
        let token = Arc::new(CancellationToken::new());

        registry.register("job-1".to_string(), token);

        let job_ids = registry.job_ids();
        assert_eq!(job_ids.len(), 1);
        assert!(job_ids.contains(&"job-1".to_string()));
    }

    #[test]
    fn test_job_registry_unregister() {
        let mut registry = JobRegistry::default();
        let token = Arc::new(CancellationToken::new());

        registry.register("job-1".to_string(), token.clone());
        registry.unregister("job-1");

        assert!(registry.job_ids().is_empty());
    }

    #[test]
    fn test_job_registry_cancel_job() {
        let mut registry = JobRegistry::default();
        let token = Arc::new(CancellationToken::new());

        registry.register("job-1".to_string(), token.clone());

        assert!(!token.is_cancelled());
        registry.cancel_job("job-1");
        assert!(token.is_cancelled());
    }

    #[test]
    fn test_job_registry_cancel_nonexistent_job() {
        let mut registry = JobRegistry::default();

        // Should not panic when canceling non-existent job
        registry.cancel_job("nonexistent");
    }

    #[test]
    fn test_job_registry_multiple_jobs() {
        let mut registry = JobRegistry::default();

        let token1 = Arc::new(CancellationToken::new());
        let token2 = Arc::new(CancellationToken::new());
        let token3 = Arc::new(CancellationToken::new());

        registry.register("job-1".to_string(), token1);
        registry.register("job-2".to_string(), token2);
        registry.register("job-3".to_string(), token3);

        assert_eq!(registry.job_ids().len(), 3);

        registry.cancel_job("job-2");

        assert!(!registry.active_jobs.get("job-1").unwrap().is_cancelled());
        assert!(registry.active_jobs.get("job-2").unwrap().is_cancelled());
        assert!(!registry.active_jobs.get("job-3").unwrap().is_cancelled());
    }
}
