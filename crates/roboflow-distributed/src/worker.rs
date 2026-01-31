// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Worker actor for claiming and processing jobs from TiKV queue.
//!
//! The worker implements a distributed job processing model:
//! - Claims jobs using optimistic concurrency (CAS in TiKV)
//! - Processes jobs with checkpoint support
//! - Updates job status (Complete/Failed) based on results
//! - Sends heartbeats to indicate liveness
//!
//! # Architecture
//!
//! - **Job Claiming**: Query pending jobs → CAS claim → Process → Complete/Fail
//! - **Concurrency**: Multiple workers run in parallel, each claims different jobs
//! - **Checkpoints**: Frame-level progress tracking for resume capability
//! - **Heartbeats**: Regular liveness signals with status
//!
//! # Example
//!
//! ```ignore
//! use roboflow_distributed::{Worker, WorkerConfig};
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let tikv = Arc::new(TikvClient::from_env().await?);
//!     let storage = Arc::new(StorageFactory::create_from_url("s3://bucket")?);
//!     let config = WorkerConfig::default();
//!
//!     let mut worker = Worker::new(
//!         "worker-1",
//!         tikv,
//!         storage,
//!         config,
//!     )?;
//!
//!     // Run until shutdown signal
//!     worker.run().await?;
//!
//!     Ok(())
//! }
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use super::tikv::{
    TikvError,
    client::TikvClient,
    schema::{HeartbeatRecord, JobRecord, JobStatus, WorkerStatus},
};
use roboflow_storage::Storage;
use tokio::sync::broadcast;
use tokio::time::sleep;

/// Default job poll interval in seconds.
pub const DEFAULT_POLL_INTERVAL_SECS: u64 = 5;

/// Default maximum concurrent jobs per worker.
pub const DEFAULT_MAX_CONCURRENT_JOBS: usize = 1;

/// Default maximum attempts per job.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 3;

/// Default job timeout in seconds.
pub const DEFAULT_JOB_TIMEOUT_SECS: u64 = 3600; // 1 hour

/// Default heartbeat interval in seconds.
pub const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = 30;

/// Worker configuration.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Maximum number of concurrent jobs to process.
    pub max_concurrent_jobs: usize,

    /// Interval between job polls.
    pub poll_interval: Duration,

    /// Maximum attempts per job before marking as Dead.
    pub max_attempts: u32,

    /// Timeout for individual job processing.
    pub job_timeout: Duration,

    /// Heartbeat interval.
    pub heartbeat_interval: Duration,

    /// Storage bucket/prefix for reading source files.
    pub storage_prefix: String,

    /// Storage bucket/prefix for writing output files.
    pub output_prefix: String,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_jobs: DEFAULT_MAX_CONCURRENT_JOBS,
            poll_interval: Duration::from_secs(DEFAULT_POLL_INTERVAL_SECS),
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            job_timeout: Duration::from_secs(DEFAULT_JOB_TIMEOUT_SECS),
            heartbeat_interval: Duration::from_secs(DEFAULT_HEARTBEAT_INTERVAL_SECS),
            storage_prefix: String::from("input/"),
            output_prefix: String::from("output/"),
        }
    }
}

impl WorkerConfig {
    /// Create a new worker configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the maximum concurrent jobs.
    pub fn with_max_concurrent_jobs(mut self, max: usize) -> Self {
        self.max_concurrent_jobs = max;
        self
    }

    /// Set the poll interval.
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Set the maximum attempts.
    pub fn with_max_attempts(mut self, max: u32) -> Self {
        self.max_attempts = max;
        self
    }

    /// Set the job timeout.
    pub fn with_job_timeout(mut self, timeout: Duration) -> Self {
        self.job_timeout = timeout;
        self
    }

    /// Set the heartbeat interval.
    pub fn with_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Set the storage prefix.
    pub fn with_storage_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.storage_prefix = prefix.into();
        self
    }

    /// Set the output prefix.
    pub fn with_output_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.output_prefix = prefix.into();
        self
    }
}

/// Worker metrics.
#[derive(Debug, Default)]
pub struct WorkerMetrics {
    /// Total jobs claimed.
    pub jobs_claimed: AtomicU64,

    /// Total jobs completed successfully.
    pub jobs_completed: AtomicU64,

    /// Total jobs failed.
    pub jobs_failed: AtomicU64,

    /// Total jobs marked as dead.
    pub jobs_dead: AtomicU64,

    /// Current active jobs.
    pub active_jobs: AtomicU64,

    /// Total processing errors.
    pub processing_errors: AtomicU64,

    /// Total heartbeat errors.
    pub heartbeat_errors: AtomicU64,
}

impl WorkerMetrics {
    /// Create new metrics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment jobs claimed.
    pub fn inc_jobs_claimed(&self) {
        self.jobs_claimed.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment jobs completed.
    pub fn inc_jobs_completed(&self) {
        self.jobs_completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment jobs failed.
    pub fn inc_jobs_failed(&self) {
        self.jobs_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment jobs dead.
    pub fn inc_jobs_dead(&self) {
        self.jobs_dead.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment active jobs.
    pub fn inc_active_jobs(&self) {
        self.active_jobs.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement active jobs.
    pub fn dec_active_jobs(&self) {
        self.active_jobs.fetch_sub(1, Ordering::Relaxed);
    }

    /// Increment processing errors.
    pub fn inc_processing_errors(&self) {
        self.processing_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment heartbeat errors.
    pub fn inc_heartbeat_errors(&self) {
        self.heartbeat_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Get all current metric values.
    pub fn snapshot(&self) -> WorkerMetricsSnapshot {
        WorkerMetricsSnapshot {
            jobs_claimed: self.jobs_claimed.load(Ordering::Relaxed),
            jobs_completed: self.jobs_completed.load(Ordering::Relaxed),
            jobs_failed: self.jobs_failed.load(Ordering::Relaxed),
            jobs_dead: self.jobs_dead.load(Ordering::Relaxed),
            active_jobs: self.active_jobs.load(Ordering::Relaxed),
            processing_errors: self.processing_errors.load(Ordering::Relaxed),
            heartbeat_errors: self.heartbeat_errors.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of worker metrics.
#[derive(Debug, Clone)]
pub struct WorkerMetricsSnapshot {
    /// Total jobs claimed.
    pub jobs_claimed: u64,

    /// Total jobs completed successfully.
    pub jobs_completed: u64,

    /// Total jobs failed.
    pub jobs_failed: u64,

    /// Total jobs marked as dead.
    pub jobs_dead: u64,

    /// Current active jobs.
    pub active_jobs: u64,

    /// Total processing errors.
    pub processing_errors: u64,

    /// Total heartbeat errors.
    pub heartbeat_errors: u64,
}

/// Processing result for a job.
pub enum ProcessingResult {
    /// Job completed successfully.
    Success,
    /// Job failed with retryable error.
    Failed { error: String },
}

/// Worker actor for claiming and processing jobs.
pub struct Worker {
    /// Pod ID for this worker instance.
    pod_id: String,

    /// TiKV client for job operations.
    tikv: Arc<TikvClient>,

    /// Storage backend for reading/writing files.
    ///
    /// This is stored for future use when implementing the full pipeline
    /// in issue #35. Currently unused but part of the worker's design.
    #[allow(dead_code)]
    storage: Arc<dyn Storage>,

    /// Worker configuration.
    config: WorkerConfig,

    /// Worker metrics.
    metrics: Arc<WorkerMetrics>,

    /// Shutdown sender.
    shutdown_tx: Option<broadcast::Sender<()>>,
}

impl Worker {
    /// Create a new worker.
    pub fn new(
        pod_id: impl Into<String>,
        tikv: Arc<TikvClient>,
        storage: Arc<dyn Storage>,
        config: WorkerConfig,
    ) -> Result<Self, TikvError> {
        let pod_id = pod_id.into();

        Ok(Self {
            pod_id,
            tikv,
            storage,
            config,
            metrics: Arc::new(WorkerMetrics::new()),
            shutdown_tx: None,
        })
    }

    /// Get the pod ID.
    pub fn pod_id(&self) -> &str {
        &self.pod_id
    }

    /// Get a reference to the metrics.
    pub fn metrics(&self) -> &WorkerMetrics {
        &self.metrics
    }

    /// Get a reference to the configuration.
    pub fn config(&self) -> &WorkerConfig {
        &self.config
    }

    /// Generate a unique pod ID.
    ///
    /// Uses hostname + UUID, or K8s pod name from environment if available.
    pub fn generate_pod_id() -> String {
        // Check for K8s pod name first
        if let Ok(pod_name) = std::env::var("POD_NAME") {
            return pod_name;
        }

        // Fall back to hostname + UUID
        let hostname = gethostname::gethostname()
            .to_str()
            .unwrap_or("unknown")
            .to_string();

        let uuid = uuid::Uuid::new_v4();
        format!("{}-{}", hostname, uuid)
    }

    /// Find pending jobs in TiKV.
    ///
    /// Scans the jobs namespace and returns jobs that are claimable.
    async fn find_pending_jobs(&self, limit: usize) -> Result<Vec<JobRecord>, TikvError> {
        use super::tikv::key::JobKeys;

        let prefix = JobKeys::prefix();
        let results = self.tikv.scan(prefix, limit as u32).await?;

        let mut pending_jobs = Vec::new();

        for (_key, value) in results {
            if let Ok(job) = bincode::deserialize::<JobRecord>(&value)
                && job.is_claimable()
            {
                pending_jobs.push(job);
            }
        }

        // Sort by created_at for FIFO processing
        pending_jobs.sort_by(|a, b| a.created_at.cmp(&b.created_at));

        tracing::debug!(
            pod_id = %self.pod_id,
            found = pending_jobs.len(),
            "Found pending jobs"
        );

        Ok(pending_jobs)
    }

    /// Try to claim a single job.
    async fn try_claim_job(&self, job_id: &str) -> Result<Option<JobRecord>, TikvError> {
        let claimed = self.tikv.claim_job(job_id, &self.pod_id).await?;

        if claimed {
            // Fetch the updated job record to confirm the claim
            if let Some(job) = self.tikv.get_job(job_id).await? {
                // Only increment metrics after confirming the job record exists
                self.metrics.inc_jobs_claimed();
                self.metrics.inc_active_jobs();
                tracing::info!(
                    pod_id = %self.pod_id,
                    job_id = %job_id,
                    source_key = %job.source_key,
                    "Job claimed successfully"
                );
                return Ok(Some(job));
            }
            // Job was claimed but record not found - this is unexpected
            tracing::warn!(
                pod_id = %self.pod_id,
                job_id = %job_id,
                "Job claim succeeded but record not found - may have been deleted"
            );
        }

        Ok(None)
    }

    /// Find and claim a job.
    ///
    /// Queries pending jobs and attempts to claim one using CAS.
    async fn find_and_claim_job(&self) -> Result<Option<JobRecord>, TikvError> {
        let pending = self.find_pending_jobs(100).await?;

        for job in pending {
            if let Some(claimed_job) = self.try_claim_job(&job.id).await? {
                return Ok(Some(claimed_job));
            }
            // Continue to next job if claim failed (race with another worker)
        }

        Ok(None)
    }

    /// Process a job.
    ///
    /// This is a placeholder implementation. The full pipeline processing
    /// will be implemented in a future issue (#35).
    async fn process_job(&self, job: &JobRecord) -> ProcessingResult {
        tracing::info!(
            pod_id = %self.pod_id,
            job_id = %job.id,
            source_key = %job.source_key,
            "Processing job"
        );

        // Check for existing checkpoint
        match self.tikv.get_checkpoint(&job.id).await {
            Ok(Some(checkpoint)) => {
                tracing::info!(
                    pod_id = %self.pod_id,
                    job_id = %job.id,
                    last_frame = checkpoint.last_frame,
                    total_frames = checkpoint.total_frames,
                    progress = checkpoint.progress_percent(),
                    "Resuming job from checkpoint"
                );
            }
            Ok(None) => {
                tracing::debug!(
                    pod_id = %self.pod_id,
                    job_id = %job.id,
                    "No existing checkpoint found, starting from beginning"
                );
            }
            Err(e) => {
                tracing::warn!(
                    pod_id = %self.pod_id,
                    job_id = %job.id,
                    error = %e,
                    "Failed to fetch checkpoint - starting job from beginning (progress may be lost)"
                );
            }
        }

        // TODO: Implement full pipeline processing in issue #35
        // For now, this is a placeholder that simulates successful processing
        // In the full implementation, this would:
        // 1. Load the source file from storage
        // 2. Initialize the pipeline with checkpoint if exists
        // 3. Run the pipeline
        // 4. Save checkpoints periodically
        // 5. Write output to storage

        // Simulate some processing time
        sleep(Duration::from_millis(100)).await;

        tracing::debug!(
            pod_id = %self.pod_id,
            job_id = %job.id,
            "Job processing complete (placeholder)"
        );

        ProcessingResult::Success
    }

    /// Complete a job successfully.
    async fn complete_job(&self, job_id: &str) -> Result<(), TikvError> {
        let completed = self.tikv.complete_job(job_id).await?;
        if !completed {
            tracing::warn!(
                pod_id = %self.pod_id,
                job_id = %job_id,
                "Job not in Processing state, cannot complete"
            );
            return Err(TikvError::Other(format!(
                "Job {} not in Processing state",
                job_id
            )));
        }

        // Delete checkpoint if exists
        let checkpoint_key = super::tikv::key::StateKeys::checkpoint(job_id);
        match self.tikv.delete(checkpoint_key).await {
            Ok(()) => {
                tracing::debug!(
                    pod_id = %self.pod_id,
                    job_id = %job_id,
                    "Checkpoint deleted after successful completion"
                );
            }
            Err(TikvError::KeyNotFound(_)) => {
                tracing::debug!(
                    pod_id = %self.pod_id,
                    job_id = %job_id,
                    "No checkpoint to delete (job may have been restarted)"
                );
            }
            Err(e) => {
                tracing::warn!(
                    pod_id = %self.pod_id,
                    job_id = %job_id,
                    error = %e,
                    "Failed to delete checkpoint after job completion - orphaned checkpoint remains"
                );
            }
        }

        self.metrics.inc_jobs_completed();
        self.metrics.dec_active_jobs();

        tracing::info!(
            pod_id = %self.pod_id,
            job_id = %job_id,
            "Job completed successfully"
        );

        Ok(())
    }

    /// Fail a job with an error message.
    async fn fail_job(&self, job_id: &str, error: String) -> Result<(), TikvError> {
        self.tikv.fail_job(job_id, error.clone()).await?;

        // Check if job is now dead (max attempts exceeded)
        if let Some(job_after) = self.tikv.get_job(job_id).await? {
            if job_after.status == JobStatus::Dead {
                tracing::error!(
                    pod_id = %self.pod_id,
                    job_id = %job_id,
                    attempts = job_after.attempts,
                    max_attempts = job_after.max_attempts,
                    error = %error,
                    "Job marked as Dead (max attempts exceeded)"
                );
                self.metrics.inc_jobs_dead();
            } else {
                tracing::warn!(
                    pod_id = %self.pod_id,
                    job_id = %job_id,
                    attempts = job_after.attempts,
                    error = %error,
                    "Job failed, will be retried"
                );
                self.metrics.inc_jobs_failed();
            }
        } else {
            // Job not found after fail - this is unexpected
            tracing::warn!(
                pod_id = %self.pod_id,
                job_id = %job_id,
                "Job not found after fail_job operation"
            );
        }

        self.metrics.dec_active_jobs();

        Ok(())
    }

    /// Send heartbeat to TiKV.
    ///
    /// This is a public method that can be called externally to trigger
    /// a heartbeat update. It's also called automatically by the worker's
    /// background task during normal operation.
    pub async fn send_heartbeat(&self) -> Result<(), TikvError> {
        let active = self.metrics.active_jobs.load(Ordering::Relaxed) as u32;
        let total_processed = self.metrics.jobs_completed.load(Ordering::Relaxed);

        let mut heartbeat = self
            .tikv
            .get_heartbeat(&self.pod_id)
            .await?
            .unwrap_or_else(|| HeartbeatRecord::new(self.pod_id.clone()));

        heartbeat.beat();
        heartbeat.active_jobs = active;
        heartbeat.total_processed = total_processed;
        heartbeat.status = if active > 0 {
            WorkerStatus::Busy
        } else {
            WorkerStatus::Idle
        };

        self.tikv.update_heartbeat(&self.pod_id, &heartbeat).await?;

        tracing::debug!(
            pod_id = %self.pod_id,
            active_jobs = active,
            total_processed = total_processed,
            status = ?heartbeat.status,
            "Heartbeat sent"
        );

        Ok(())
    }

    /// Run the worker loop.
    ///
    /// This will continuously:
    /// 1. Check for shutdown signal
    /// 2. Find and claim a job (if under concurrent limit)
    /// 3. Process the job
    /// 4. Complete or fail the job
    /// 5. Send periodic heartbeats
    /// 6. Repeat until shutdown
    pub async fn run(&mut self) -> Result<(), TikvError> {
        let (shutdown_tx, mut shutdown_rx) = broadcast::channel(1);
        self.shutdown_tx = Some(shutdown_tx.clone());

        // Start heartbeat task
        let tikv = self.tikv.clone();
        let pod_id_for_heartbeat = self.pod_id.clone();
        let metrics = self.metrics.clone();
        let heartbeat_interval = self.config.heartbeat_interval;
        let mut heartbeat_rx = shutdown_tx.subscribe();

        let heartbeat_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(heartbeat_interval);
            interval.tick().await; // Skip first tick

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = send_heartbeat_inner(&tikv, &pod_id_for_heartbeat, &metrics).await {
                            metrics.inc_heartbeat_errors();
                            tracing::error!(
                                pod_id = %pod_id_for_heartbeat,
                                error = %e,
                                "Failed to send heartbeat"
                            );
                        }
                    }
                    _ = heartbeat_rx.recv() => {
                        tracing::info!(
                            pod_id = %pod_id_for_heartbeat,
                            "Heartbeat task shutting down"
                        );
                        break;
                    }
                }
            }
        });

        tracing::info!(
            pod_id = %self.pod_id,
            max_concurrent_jobs = self.config.max_concurrent_jobs,
            poll_interval_secs = self.config.poll_interval.as_secs(),
            "Starting worker"
        );

        // Main loop - use tokio::select! for proper shutdown handling
        loop {
            // Check if we can claim more jobs
            let active_count = self.metrics.active_jobs.load(Ordering::Relaxed) as usize;

            if active_count < self.config.max_concurrent_jobs {
                // Try to claim and process a job
                match self.find_and_claim_job().await {
                    Ok(Some(job)) => {
                        let job_id = job.id.clone();

                        // Process the job
                        let result = self.process_job(&job).await;

                        match result {
                            ProcessingResult::Success => {
                                if let Err(e) = self.complete_job(&job_id).await {
                                    tracing::error!(
                                        pod_id = %self.pod_id,
                                        job_id = %job_id,
                                        error = %e,
                                        "Failed to complete job"
                                    );
                                    self.metrics.inc_processing_errors();
                                }
                            }
                            ProcessingResult::Failed { error } => {
                                if let Err(e) = self.fail_job(&job_id, error).await {
                                    tracing::error!(
                                        pod_id = %self.pod_id,
                                        job_id = %job_id,
                                        error = %e,
                                        "Failed to mark job as failed"
                                    );
                                    self.metrics.inc_processing_errors();
                                }
                            }
                        }
                    }
                    Ok(None) => {
                        // No jobs available - use tokio::select! to race shutdown against sleep
                        tokio::select! {
                            _ = sleep(self.config.poll_interval) => {
                                tracing::debug!(
                                    pod_id = %self.pod_id,
                                    "No jobs available, retrying"
                                );
                            }
                            _ = shutdown_rx.recv() => {
                                tracing::info!(
                                    pod_id = %self.pod_id,
                                    "Worker shutdown requested while idle"
                                );
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            pod_id = %self.pod_id,
                            error = %e,
                            "Failed to find/claim job - backing off before retry"
                        );
                        self.metrics.inc_processing_errors();
                        // Add backoff to prevent tight loop on persistent errors
                        tokio::select! {
                            _ = sleep(self.config.poll_interval) => {}
                            _ = shutdown_rx.recv() => {
                                tracing::info!(
                                    pod_id = %self.pod_id,
                                    "Worker shutdown requested during error backoff"
                                );
                                break;
                            }
                        }
                    }
                }
            } else {
                // At capacity, sleep briefly with shutdown handling
                tokio::select! {
                    _ = sleep(Duration::from_millis(100)) => {}
                    _ = shutdown_rx.recv() => {
                        tracing::info!(
                            pod_id = %self.pod_id,
                            "Worker shutdown requested while at capacity"
                        );
                        break;
                    }
                }
            }
        }

        // Wait for heartbeat task to finish gracefully
        let _ = heartbeat_handle.await;

        // Send final heartbeat with Draining status
        let mut heartbeat = self
            .tikv
            .get_heartbeat(&self.pod_id)
            .await?
            .unwrap_or_else(|| HeartbeatRecord::new(self.pod_id.clone()));
        heartbeat.beat();
        heartbeat.status = WorkerStatus::Draining;
        if let Err(e) = self.tikv.update_heartbeat(&self.pod_id, &heartbeat).await {
            tracing::error!(
                pod_id = %self.pod_id,
                error = %e,
                "Failed to send final Draining heartbeat - worker shutdown may not be visible to cluster"
            );
        }

        tracing::info!(
            pod_id = %self.pod_id,
            "Worker stopped"
        );

        Ok(())
    }

    /// Shutdown the worker gracefully.
    pub fn shutdown(&self) -> Result<(), TikvError> {
        if let Some(ref tx) = self.shutdown_tx {
            match tx.send(()) {
                Ok(_) => {
                    tracing::info!(
                        pod_id = %self.pod_id,
                        "Shutdown signal sent successfully"
                    );
                }
                Err(_) => {
                    tracing::warn!(
                        pod_id = %self.pod_id,
                        "Shutdown signal sent but no receivers - worker may not be running"
                    );
                    return Err(TikvError::Other(
                        "Cannot shutdown worker: no active receiver. Is the worker running?"
                            .to_string(),
                    ));
                }
            }
        } else {
            tracing::warn!(
                pod_id = %self.pod_id,
                "Shutdown requested but worker has no shutdown channel configured"
            );
            return Err(TikvError::Other(
                "Cannot shutdown worker: shutdown channel not initialized".to_string(),
            ));
        }
        Ok(())
    }
}

/// Helper function for sending heartbeat (used in spawned task).
async fn send_heartbeat_inner(
    tikv: &TikvClient,
    pod_id: &str,
    metrics: &WorkerMetrics,
) -> Result<(), TikvError> {
    let active = metrics.active_jobs.load(Ordering::Relaxed) as u32;
    let total_processed = metrics.jobs_completed.load(Ordering::Relaxed);

    let mut heartbeat = tikv
        .get_heartbeat(pod_id)
        .await?
        .unwrap_or_else(|| HeartbeatRecord::new(pod_id.to_string()));

    heartbeat.beat();
    heartbeat.active_jobs = active;
    heartbeat.total_processed = total_processed;
    heartbeat.status = if active > 0 {
        WorkerStatus::Busy
    } else {
        WorkerStatus::Idle
    };

    tikv.update_heartbeat(pod_id, &heartbeat).await
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_config_default() {
        let config = WorkerConfig::default();
        assert_eq!(config.max_concurrent_jobs, DEFAULT_MAX_CONCURRENT_JOBS);
        assert_eq!(config.poll_interval.as_secs(), DEFAULT_POLL_INTERVAL_SECS);
        assert_eq!(config.max_attempts, DEFAULT_MAX_ATTEMPTS);
        assert_eq!(config.job_timeout.as_secs(), DEFAULT_JOB_TIMEOUT_SECS);
        assert_eq!(
            config.heartbeat_interval.as_secs(),
            DEFAULT_HEARTBEAT_INTERVAL_SECS
        );
        assert_eq!(config.storage_prefix, "input/");
        assert_eq!(config.output_prefix, "output/");
    }

    #[test]
    fn test_worker_config_builder() {
        let config = WorkerConfig::new()
            .with_max_concurrent_jobs(5)
            .with_poll_interval(Duration::from_secs(10))
            .with_max_attempts(5)
            .with_job_timeout(Duration::from_secs(7200))
            .with_heartbeat_interval(Duration::from_secs(60))
            .with_storage_prefix("data/")
            .with_output_prefix("results/");

        assert_eq!(config.max_concurrent_jobs, 5);
        assert_eq!(config.poll_interval.as_secs(), 10);
        assert_eq!(config.max_attempts, 5);
        assert_eq!(config.job_timeout.as_secs(), 7200);
        assert_eq!(config.heartbeat_interval.as_secs(), 60);
        assert_eq!(config.storage_prefix, "data/");
        assert_eq!(config.output_prefix, "results/");
    }

    #[test]
    fn test_worker_metrics() {
        let metrics = WorkerMetrics::new();

        assert_eq!(metrics.jobs_claimed.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.active_jobs.load(Ordering::Relaxed), 0);

        metrics.inc_jobs_claimed();
        metrics.inc_active_jobs();
        metrics.inc_active_jobs();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.jobs_claimed, 1);
        assert_eq!(snapshot.active_jobs, 2);

        metrics.dec_active_jobs();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.active_jobs, 1);
    }

    #[test]
    fn test_generate_pod_id() {
        // Test that we can generate a pod ID
        let pod_id = Worker::generate_pod_id();
        assert!(!pod_id.is_empty());
        // Should contain a UUID
        assert!(pod_id.len() > 20);
    }

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_POLL_INTERVAL_SECS, 5);
        assert_eq!(DEFAULT_MAX_CONCURRENT_JOBS, 1);
        assert_eq!(DEFAULT_MAX_ATTEMPTS, 3);
        assert_eq!(DEFAULT_JOB_TIMEOUT_SECS, 3600);
        assert_eq!(DEFAULT_HEARTBEAT_INTERVAL_SECS, 30);
    }
}
