// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Zombie reaper for reclaiming jobs from dead workers.
//!
//! The zombie reaper periodically scans for:
//! - Stale heartbeats (workers that haven't sent a heartbeat recently)
//! - Orphaned jobs (jobs in Processing state owned by stale workers)
//!
//! When orphaned jobs are found, they are reclaimed by:
//! - Verifying the job is still in Processing state
//! - Verifying the owner's heartbeat is stale
//! - Setting the job back to Pending status with no owner
//! - Preserving the checkpoint for resume capability
//!
//! ## Design
//!
//! The reaper runs on ALL workers (no leader election) to maximize
//! fault tolerance. Multiple workers may attempt to reclaim the same
//! job, but TiKV's optimistic concurrency ensures only one succeeds.
//!
//! To prevent thundering herd, the reaper limits reclamations per
//! iteration and adds random jitter to the sleep interval.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use super::tikv::{
    TikvError,
    client::TikvClient,
    key::{HeartbeatKeys, JobKeys},
    schema::{HeartbeatRecord, JobRecord, JobStatus},
};
use tokio::sync::broadcast;
use tokio::time::sleep;

/// Default reaper interval in seconds (60 seconds).
pub const DEFAULT_REAPER_INTERVAL_SECS: u64 = 60;

/// Default stale threshold in seconds (5 minutes).
pub const DEFAULT_STALE_THRESHOLD_SECS: i64 = 300;

/// Default maximum reclamations per iteration.
pub const DEFAULT_MAX_RECLAIMS_PER_ITERATION: usize = 10;

/// Zombie reaper configuration.
#[derive(Debug, Clone)]
pub struct ReaperConfig {
    /// Interval between reaper runs.
    pub interval: Duration,

    /// Threshold for considering a heartbeat stale.
    pub stale_threshold: Duration,

    /// Maximum jobs to reclaim per iteration.
    pub max_reclaims_per_iteration: usize,

    /// Maximum heartbeats to scan per iteration.
    pub max_heartbeat_scan: u32,
}

impl Default for ReaperConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(DEFAULT_REAPER_INTERVAL_SECS),
            stale_threshold: Duration::from_secs(DEFAULT_STALE_THRESHOLD_SECS as u64),
            max_reclaims_per_iteration: DEFAULT_MAX_RECLAIMS_PER_ITERATION,
            max_heartbeat_scan: 1000,
        }
    }
}

impl ReaperConfig {
    /// Create a new reaper configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the reaper interval.
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Set the stale threshold.
    pub fn with_stale_threshold(mut self, threshold: Duration) -> Self {
        self.stale_threshold = threshold;
        self
    }

    /// Set the maximum reclamations per iteration.
    pub fn with_max_reclaims(mut self, max: usize) -> Self {
        self.max_reclaims_per_iteration = max;
        self
    }

    /// Set the maximum heartbeat scan limit.
    pub fn with_max_heartbeat_scan(mut self, max: u32) -> Self {
        self.max_heartbeat_scan = max;
        self
    }
}

/// Zombie reaper metrics.
#[derive(Debug, Default)]
pub struct ReaperMetrics {
    /// Total jobs reclaimed.
    pub jobs_reclaimed: AtomicU64,

    /// Total stale workers found.
    pub stale_workers_found: AtomicU64,

    /// Total reaper iterations.
    pub iterations_total: AtomicU64,

    /// Total reclaim attempts.
    pub reclaim_attempts: AtomicU64,

    /// Total reclaim failures.
    pub reclaim_failures: AtomicU64,

    /// Jobs skipped (already claimed by another reaper).
    pub jobs_skipped: AtomicU64,
}

impl ReaperMetrics {
    /// Create new metrics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment jobs reclaimed counter.
    pub fn inc_jobs_reclaimed(&self) {
        self.jobs_reclaimed.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment stale workers found counter.
    pub fn inc_stale_workers_found(&self, count: u64) {
        self.stale_workers_found.fetch_add(count, Ordering::Relaxed);
    }

    /// Increment iterations counter.
    pub fn inc_iterations(&self) {
        self.iterations_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment reclaim attempts.
    pub fn inc_reclaim_attempts(&self) {
        self.reclaim_attempts.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment reclaim failures.
    pub fn inc_reclaim_failures(&self) {
        self.reclaim_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment jobs skipped.
    pub fn inc_jobs_skipped(&self) {
        self.jobs_skipped.fetch_add(1, Ordering::Relaxed);
    }

    /// Get all current metric values.
    pub fn snapshot(&self) -> ReaperMetricsSnapshot {
        ReaperMetricsSnapshot {
            jobs_reclaimed: self.jobs_reclaimed.load(Ordering::Relaxed),
            stale_workers_found: self.stale_workers_found.load(Ordering::Relaxed),
            iterations_total: self.iterations_total.load(Ordering::Relaxed),
            reclaim_attempts: self.reclaim_attempts.load(Ordering::Relaxed),
            reclaim_failures: self.reclaim_failures.load(Ordering::Relaxed),
            jobs_skipped: self.jobs_skipped.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of reaper metrics.
#[derive(Debug, Clone)]
pub struct ReaperMetricsSnapshot {
    /// Total jobs reclaimed.
    pub jobs_reclaimed: u64,

    /// Total stale workers found.
    pub stale_workers_found: u64,

    /// Total reaper iterations.
    pub iterations_total: u64,

    /// Total reclaim attempts.
    pub reclaim_attempts: u64,

    /// Total reclaim failures.
    pub reclaim_failures: u64,

    /// Jobs skipped (already claimed by another reaper).
    pub jobs_skipped: u64,
}

/// Result of a job reclamation attempt.
#[derive(Debug, Clone)]
pub enum ReclaimResult {
    /// Job was successfully reclaimed.
    Reclaimed,

    /// Job was not stale (skip).
    NotStale,

    /// Job was not in Processing state (skip).
    NotProcessing,

    /// Job reclaim failed (will retry).
    Failed,

    /// Job was already reclaimed by another worker.
    Skipped,
}

/// Zombie reaper for reclaiming jobs from dead workers.
///
/// The reaper periodically scans for stale heartbeats and reclaims
/// orphaned jobs. It runs on all workers (no leader election) for
/// fault tolerance.
pub struct ZombieReaper {
    /// TiKV client for operations.
    tikv: Arc<TikvClient>,

    /// Reaper configuration.
    config: ReaperConfig,

    /// Reaper metrics.
    metrics: Arc<ReaperMetrics>,

    /// Shutdown sender.
    shutdown_tx: Option<broadcast::Sender<()>>,
}

impl ZombieReaper {
    /// Create a new zombie reaper.
    pub fn new(tikv: Arc<TikvClient>, config: ReaperConfig) -> Self {
        Self {
            tikv,
            config,
            metrics: Arc::new(ReaperMetrics::new()),
            shutdown_tx: None,
        }
    }

    /// Create a new zombie reaper with default configuration.
    pub fn with_defaults(tikv: Arc<TikvClient>) -> Self {
        Self::new(tikv, ReaperConfig::default())
    }

    /// Get a reference to the metrics.
    pub fn metrics(&self) -> &ReaperMetrics {
        &self.metrics
    }

    /// Get a reference to the configuration.
    pub fn config(&self) -> &ReaperConfig {
        &self.config
    }

    /// Find stale worker heartbeats.
    ///
    /// Scans the heartbeat namespace and returns pod IDs of workers
    /// whose heartbeats are older than the stale threshold.
    pub async fn find_stale_workers(&self) -> Result<Vec<String>, TikvError> {
        let prefix = HeartbeatKeys::prefix();
        let results = self
            .tikv
            .scan(prefix, self.config.max_heartbeat_scan)
            .await?;

        let stale_threshold = self.config.stale_threshold.as_secs() as i64;
        let mut stale_pods = Vec::new();

        for (_key, value) in results {
            if let Ok(record) = bincode::deserialize::<HeartbeatRecord>(&value)
                && record.is_stale(stale_threshold)
            {
                stale_pods.push(record.pod_id.clone());
                tracing::debug!(
                    pod_id = %record.pod_id,
                    last_seen = %record.last_heartbeat,
                    "Found stale worker"
                );
            }
        }

        self.metrics
            .inc_stale_workers_found(stale_pods.len() as u64);

        tracing::debug!(stale_count = stale_pods.len(), "Found stale workers");

        Ok(stale_pods)
    }

    /// Find orphaned jobs (jobs in Processing state owned by stale workers).
    ///
    /// Returns job records that can be reclaimed.
    pub async fn find_orphaned_jobs(&self) -> Result<Vec<JobRecord>, TikvError> {
        let stale_workers = self.find_stale_workers().await?;

        if stale_workers.is_empty() {
            tracing::debug!("No stale workers found, no orphaned jobs to reclaim");
            return Ok(Vec::new());
        }

        // Scan for jobs in Processing state
        let job_prefix = JobKeys::prefix();
        let results = self
            .tikv
            .scan(job_prefix, self.config.max_heartbeat_scan)
            .await?;

        let mut orphaned_jobs = Vec::new();

        for (_key, value) in results {
            if let Ok(job) = bincode::deserialize::<JobRecord>(&value) {
                // Check if job is in Processing state and owned by a stale worker
                if job.status == JobStatus::Processing
                    && let Some(owner) = &job.owner
                    && stale_workers.contains(owner)
                {
                    tracing::debug!(
                        job_id = %job.id,
                        owner = %owner,
                        "Found orphaned job"
                    );
                    orphaned_jobs.push(job);
                }
            }
        }

        tracing::debug!(orphaned_count = orphaned_jobs.len(), "Found orphaned jobs");

        Ok(orphaned_jobs)
    }

    /// Reclaim a single job.
    ///
    /// This uses a transaction to:
    /// 1. Read the job (verify still Processing)
    /// 2. Read the owner's heartbeat (verify stale)
    /// 3. Update job to Pending with no owner
    ///
    /// Returns the reclamation result.
    pub async fn reclaim_job(&self, job_id: &str) -> Result<ReclaimResult, TikvError> {
        self.metrics.inc_reclaim_attempts();

        let stale_threshold = self.config.stale_threshold.as_secs() as i64;

        match self.tikv.reclaim_job(job_id, stale_threshold).await {
            Ok(true) => {
                self.metrics.inc_jobs_reclaimed();
                tracing::info!(
                    job_id = %job_id,
                    "Job reclaimed successfully"
                );
                Ok(ReclaimResult::Reclaimed)
            }
            Ok(false) => {
                // Job wasn't reclaimed (not stale, not processing, or no owner)
                // This could also mean another reaper got there first
                self.metrics.inc_jobs_skipped();
                Ok(ReclaimResult::Skipped)
            }
            Err(e) if e.is_write_conflict() || e.is_retryable() => {
                // Another reaper got there first
                self.metrics.inc_jobs_skipped();
                tracing::debug!(
                    job_id = %job_id,
                    "Job reclaim failed due to conflict (likely claimed by another reaper)"
                );
                Ok(ReclaimResult::Skipped)
            }
            Err(e) => {
                self.metrics.inc_reclaim_failures();
                tracing::error!(
                    job_id = %job_id,
                    error = %e,
                    "Job reclaim failed"
                );
                Ok(ReclaimResult::Failed)
            }
        }
    }

    /// Run a single reaper iteration.
    ///
    /// Finds orphaned jobs and attempts to reclaim them up to
    /// the max_reclaims_per_iteration limit.
    pub async fn run_iteration(&self) -> Result<usize, TikvError> {
        self.metrics.inc_iterations();

        let orphaned_jobs = self.find_orphaned_jobs().await?;

        if orphaned_jobs.is_empty() {
            return Ok(0);
        }

        let mut reclaimed_count = 0;
        let max_reclaims = self.config.max_reclaims_per_iteration;

        for job in orphaned_jobs.iter().take(max_reclaims) {
            match self.reclaim_job(&job.id).await? {
                ReclaimResult::Reclaimed => {
                    reclaimed_count += 1;
                }
                ReclaimResult::NotStale | ReclaimResult::NotProcessing => {
                    // Skip these
                }
                ReclaimResult::Failed => {
                    // Log but continue with other jobs
                    tracing::warn!(
                        job_id = %job.id,
                        "Failed to reclaim job, will retry in next iteration"
                    );
                }
                ReclaimResult::Skipped => {
                    // Already claimed by another worker
                    tracing::debug!(
                        job_id = %job.id,
                        "Job already reclaimed by another worker"
                    );
                }
            }
        }

        if reclaimed_count > 0 {
            tracing::info!(
                reclaimed = reclaimed_count,
                total_orphaned = orphaned_jobs.len(),
                "Reaper iteration completed"
            );
        }

        Ok(reclaimed_count)
    }

    /// Run the reaper loop until shutdown.
    ///
    /// This is the main entry point for running the reaper.
    pub async fn run(&mut self) -> Result<(), TikvError> {
        let (_shutdown_tx, mut shutdown_rx) = broadcast::channel(1);
        self.shutdown_tx = Some(_shutdown_tx);

        tracing::info!(
            interval_secs = self.config.interval.as_secs(),
            stale_threshold_secs = self.config.stale_threshold.as_secs(),
            max_reclaims = self.config.max_reclaims_per_iteration,
            "Starting zombie reaper"
        );

        loop {
            // Run a single iteration
            match self.run_iteration().await {
                Ok(reclaimed) => {
                    if reclaimed > 0 {
                        tracing::info!(reclaimed = reclaimed, "Jobs reclaimed in this iteration");
                    }
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "Reaper iteration failed"
                    );
                }
            }

            // Wait for next interval or shutdown
            tokio::select! {
                _ = sleep(self.config.interval) => {}
                _ = shutdown_rx.recv() => {
                    tracing::info!("Reaper shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Stop the reaper.
    pub fn shutdown(&self) -> Result<(), TikvError> {
        if let Some(ref tx) = self.shutdown_tx {
            let _ = tx.send(());
            tracing::info!("Reaper shutdown signal sent");
            Ok(())
        } else {
            Err(TikvError::Other(
                "Cannot shutdown reaper: no background task running".to_string(),
            ))
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reaper_config_default() {
        let config = ReaperConfig::default();
        assert_eq!(config.interval.as_secs(), DEFAULT_REAPER_INTERVAL_SECS);
        assert_eq!(
            config.stale_threshold.as_secs(),
            DEFAULT_STALE_THRESHOLD_SECS as u64
        );
        assert_eq!(
            config.max_reclaims_per_iteration,
            DEFAULT_MAX_RECLAIMS_PER_ITERATION
        );
    }

    #[test]
    fn test_reaper_config_builder() {
        let config = ReaperConfig::new()
            .with_interval(Duration::from_secs(120))
            .with_stale_threshold(Duration::from_secs(600))
            .with_max_reclaims(20);

        assert_eq!(config.interval.as_secs(), 120);
        assert_eq!(config.stale_threshold.as_secs(), 600);
        assert_eq!(config.max_reclaims_per_iteration, 20);
    }

    #[test]
    fn test_reaper_metrics() {
        let metrics = ReaperMetrics::new();

        metrics.inc_jobs_reclaimed();
        metrics.inc_stale_workers_found(5);
        metrics.inc_iterations();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.jobs_reclaimed, 1);
        assert_eq!(snapshot.stale_workers_found, 5);
        assert_eq!(snapshot.iterations_total, 1);
    }

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_REAPER_INTERVAL_SECS, 60);
        assert_eq!(DEFAULT_STALE_THRESHOLD_SECS, 300);
        assert_eq!(DEFAULT_MAX_RECLAIMS_PER_ITERATION, 10);
    }
}
