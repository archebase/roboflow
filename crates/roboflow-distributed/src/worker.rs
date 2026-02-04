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

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use super::merge::MergeCoordinator;
use super::shutdown::{ShutdownHandler, ShutdownInterrupted};
use super::tikv::{
    TikvError,
    checkpoint::{CheckpointConfig, CheckpointManager},
    client::TikvClient,
    schema::{CheckpointState, HeartbeatRecord, JobRecord, JobStatus, WorkerStatus},
};
use roboflow_storage::{Storage, StorageFactory};
use std::collections::HashMap;
use tokio::sync::{Mutex, RwLock};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use lru::LruCache;

// Dataset conversion imports
use roboflow_dataset::{
    common::DatasetWriter,
    lerobot::{LerobotConfig, VideoConfig},
    streaming::StreamingDatasetConverter,
};

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

/// Default checkpoint interval in frames.
pub const DEFAULT_CHECKPOINT_INTERVAL_FRAMES: u64 = 100;

/// Default checkpoint interval in seconds.
pub const DEFAULT_CHECKPOINT_INTERVAL_SECS: u64 = 10;

/// Default cancellation check interval in seconds.
pub const DEFAULT_CANCELLATION_CHECK_INTERVAL_SECS: u64 = 5;

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
struct JobRegistry {
    /// Map of job_id -> cancellation_token for active jobs.
    active_jobs: HashMap<String, Arc<CancellationToken>>,
}

impl JobRegistry {
    /// Register a job for cancellation monitoring.
    fn register(&mut self, job_id: String, token: Arc<CancellationToken>) {
        self.active_jobs.insert(job_id, token);
    }

    /// Unregister a job from cancellation monitoring.
    fn unregister(&mut self, job_id: &str) {
        self.active_jobs.remove(job_id);
    }

    /// Get all registered job IDs.
    fn job_ids(&self) -> Vec<String> {
        self.active_jobs.keys().cloned().collect()
    }

    /// Cancel a specific job by ID.
    fn cancel_job(&mut self, job_id: &str) {
        if let Some(token) = self.active_jobs.get(job_id) {
            token.cancel();
        }
    }
}

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

    /// Checkpoint interval in frames.
    pub checkpoint_interval_frames: u64,

    /// Checkpoint interval in seconds.
    pub checkpoint_interval_seconds: u64,

    /// Whether to use async checkpointing.
    pub checkpoint_async: bool,

    /// Storage bucket/prefix for reading source files.
    pub storage_prefix: String,

    /// Local output prefix for writing files (used when output_storage_url is not set).
    pub output_prefix: String,

    /// Cloud storage URL for output files (e.g., "s3://bucket/datasets" or "oss://bucket/datasets").
    ///
    /// When set, workers write to staging paths in cloud storage using a Staging + Merge pattern:
    /// - Staging: `{output_storage_url}/staging/{job_id}/worker_{pod_id}/`
    /// - After merge: `{output_storage_url}/{dataset_path}/`
    ///
    /// If None, output goes to local filesystem at `output_prefix`.
    pub output_storage_url: Option<String>,

    /// Number of workers expected for distributed merge coordination.
    ///
    /// Used to determine when all workers have completed staging
    /// so merge can proceed.
    pub expected_workers: usize,

    /// Final output path for the merged dataset (relative to output_storage_url).
    ///
    /// After merge completes, the final dataset will be at:
    /// `{output_storage_url}/{merge_output_path}/`
    pub merge_output_path: String,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_jobs: DEFAULT_MAX_CONCURRENT_JOBS,
            poll_interval: Duration::from_secs(DEFAULT_POLL_INTERVAL_SECS),
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            job_timeout: Duration::from_secs(DEFAULT_JOB_TIMEOUT_SECS),
            heartbeat_interval: Duration::from_secs(DEFAULT_HEARTBEAT_INTERVAL_SECS),
            checkpoint_interval_frames: DEFAULT_CHECKPOINT_INTERVAL_FRAMES,
            checkpoint_interval_seconds: DEFAULT_CHECKPOINT_INTERVAL_SECS,
            checkpoint_async: true,
            storage_prefix: String::from("input/"),
            output_prefix: String::from("output/"),
            output_storage_url: None,
            expected_workers: 1,
            merge_output_path: String::from("datasets/merged"),
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

    /// Set the cloud storage URL for output files.
    ///
    /// When set, workers write to staging paths in cloud storage using a Staging + Merge pattern.
    /// Example: "s3://my-bucket/datasets" or "oss://my-bucket/datasets"
    pub fn with_output_storage_url(mut self, url: impl Into<String>) -> Self {
        self.output_storage_url = Some(url.into());
        self
    }

    /// Set the checkpoint interval in frames.
    pub fn with_checkpoint_interval_frames(mut self, interval: u64) -> Self {
        self.checkpoint_interval_frames = interval;
        self
    }

    /// Set the checkpoint interval in seconds.
    pub fn with_checkpoint_interval_seconds(mut self, interval: u64) -> Self {
        self.checkpoint_interval_seconds = interval;
        self
    }

    /// Enable or disable async checkpointing.
    pub fn with_checkpoint_async(mut self, async_mode: bool) -> Self {
        self.checkpoint_async = async_mode;
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
    /// Job was cancelled by user request.
    Cancelled,
}

/// Progress callback for saving checkpoints during conversion.
struct WorkerCheckpointCallback {
    /// Job ID for this conversion
    job_id: String,
    /// Pod ID of the worker
    pod_id: String,
    /// Total frames (estimated)
    total_frames: u64,
    /// Reference to checkpoint manager
    checkpoint_manager: CheckpointManager,
    /// Last checkpoint frame number
    last_checkpoint_frame: Arc<std::sync::atomic::AtomicU64>,
    /// Last checkpoint time
    last_checkpoint_time: Arc<std::sync::Mutex<std::time::Instant>>,
    /// Shutdown flag for graceful interruption
    shutdown_flag: Arc<AtomicBool>,
    /// Cancellation token for job cancellation
    cancellation_token: Option<Arc<CancellationToken>>,
}

impl roboflow_dataset::streaming::converter::ProgressCallback for WorkerCheckpointCallback {
    fn on_frame_written(
        &self,
        frames_written: u64,
        messages_processed: u64,
        writer: &dyn std::any::Any,
    ) -> std::result::Result<(), String> {
        // Check for shutdown signal first
        if self.shutdown_flag.load(std::sync::atomic::Ordering::SeqCst) {
            tracing::info!(
                job_id = %self.job_id,
                frames_written = frames_written,
                "Shutdown requested, interrupting conversion at checkpoint boundary"
            );
            return Err(ShutdownInterrupted.to_string());
        }

        // Check for job cancellation via token
        if let Some(token) = &self.cancellation_token
            && token.is_cancelled()
        {
            tracing::info!(
                job_id = %self.job_id,
                frames_written = frames_written,
                "Job cancellation detected, interrupting conversion at checkpoint boundary"
            );
            return Err("Job cancelled by user request".to_string());
        }

        let last_frame = self
            .last_checkpoint_frame
            .load(std::sync::atomic::Ordering::Relaxed);
        let frames_since_last = frames_written.saturating_sub(last_frame);

        // Scope the lock tightly to avoid holding it during expensive operations
        let time_since_last = {
            let last_time = self
                .last_checkpoint_time
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            last_time.elapsed()
        };

        // Check if we should save a checkpoint
        if self
            .checkpoint_manager
            .should_checkpoint(frames_since_last, time_since_last)
        {
            // Extract episode index from writer if it's a LeRobotWriter
            use roboflow_dataset::lerobot::writer::LerobotWriter;
            let episode_idx = writer
                .downcast_ref::<LerobotWriter>()
                .and_then(|w| w.episode_index())
                .unwrap_or(0) as u64;

            // NOTE: Using messages_processed as byte_offset proxy.
            // Actual byte offset tracking requires robocodec modifications.
            // Resume works by re-reading from start and skipping messages.
            //
            // NOTE: Upload state tracking requires episode-level checkpointing.
            // Current frame-level checkpoints don't capture upload state because:
            // 1. Uploads happen after finish_episode(), not during frame processing
            // 2. The coordinator tracks completion, not in-progress multipart state
            // 3. Resume should check which episodes exist in cloud storage
            //
            // Episode-level upload state tracking is a future enhancement that would:
            // - Save episode completion to TiKV after each episode finishes
            // - Query cloud storage for completed episodes on resume
            // - Skip re-uploading episodes that already exist
            //
            // For now, the frame-level checkpoint is sufficient for resume
            // as episodes are written atomically and can be detected via
            // existence checks in the output storage.
            let checkpoint = CheckpointState {
                job_id: self.job_id.clone(),
                pod_id: self.pod_id.clone(),
                byte_offset: messages_processed,
                last_frame: frames_written,
                episode_idx,
                total_frames: self.total_frames,
                video_uploads: Vec::new(),
                parquet_upload: None,
                updated_at: chrono::Utc::now(),
                version: 1,
            };

            // Use save_async which respects checkpoint_async config:
            // - When async=true: spawns background task, non-blocking
            // - When async=false: falls back to synchronous save
            self.checkpoint_manager.save_async(checkpoint.clone());
            tracing::debug!(
                job_id = %self.job_id,
                last_frame = frames_written,
                progress = %checkpoint.progress_percent(),
                "Checkpoint save initiated"
            );
            self.last_checkpoint_frame
                .store(frames_written, std::sync::atomic::Ordering::Relaxed);
            // Re-acquire lock only for the instant update
            // Use poison recovery to handle panics gracefully
            *self
                .last_checkpoint_time
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = std::time::Instant::now();
        }

        std::result::Result::Ok(())
    }
}

/// Worker actor for claiming and processing jobs.
pub struct Worker {
    /// Pod ID for this worker instance.
    pod_id: String,

    /// TiKV client for job operations.
    tikv: Arc<TikvClient>,

    /// Checkpoint manager for progress tracking.
    checkpoint_manager: CheckpointManager,

    /// Storage backend for reading/writing files.
    ///
    /// Used by the dataset conversion pipeline to download input files
    /// and write output datasets via StreamingDatasetConverter.
    storage: Arc<dyn Storage>,

    /// Storage factory for creating storage backends from URLs.
    ///
    /// Used to create output storage from output_storage_url config.
    storage_factory: StorageFactory,

    /// Worker configuration.
    config: WorkerConfig,

    /// Worker metrics.
    metrics: Arc<WorkerMetrics>,

    /// Shutdown handler for graceful termination.
    shutdown_handler: ShutdownHandler,

    /// Cancellation token for aborting conversion tasks.
    cancellation_token: Arc<CancellationToken>,

    /// Job registry for batch cancellation monitoring.
    job_registry: Arc<RwLock<JobRegistry>>,

    /// Config cache to reduce TiKV round-trips.
    /// Maps config_hash -> LerobotConfig
    config_cache: Arc<Mutex<LruCache<String, roboflow_dataset::lerobot::LerobotConfig>>>,

    /// Merge coordinator for distributed dataset merge operations.
    merge_coordinator: MergeCoordinator,
}

impl Worker {
    pub fn new(
        pod_id: impl Into<String>,
        tikv: Arc<TikvClient>,
        storage: Arc<dyn Storage>,
        config: WorkerConfig,
    ) -> Result<Self, TikvError> {
        let pod_id = pod_id.into();

        // Create storage factory from storage URL (for creating output storage backends)
        // Use the storage_prefix as the base URL for the factory
        let storage_factory = StorageFactory::new();

        // Create checkpoint manager with config from WorkerConfig
        let checkpoint_config = CheckpointConfig {
            checkpoint_interval_frames: config.checkpoint_interval_frames,
            checkpoint_interval_seconds: config.checkpoint_interval_seconds,
            checkpoint_async: config.checkpoint_async,
        };
        let checkpoint_manager = CheckpointManager::new(tikv.clone(), checkpoint_config);

        // Create merge coordinator for distributed dataset merge operations
        use super::merge::MergeCoordinator;
        let merge_coordinator = MergeCoordinator::new(tikv.clone(), pod_id.clone());

        Ok(Self {
            pod_id,
            tikv,
            checkpoint_manager,
            storage,
            storage_factory,
            config,
            metrics: Arc::new(WorkerMetrics::new()),
            shutdown_handler: ShutdownHandler::new(),
            cancellation_token: Arc::new(CancellationToken::new()),
            job_registry: Arc::new(RwLock::new(JobRegistry::default())),
            config_cache: Arc::new(Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(100).unwrap(), // Cache up to 100 configs
            ))),
            merge_coordinator,
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
    /// This implementation integrates the LerobotWriter with the distributed worker
    /// infrastructure to process bag/MCAP files and produce LeRobot v2.1 output.
    ///
    /// # Pipeline Stages
    ///
    /// 1. **Download input** - Fetch source file from S3/OSS to local buffer
    /// 2. **Initialize converter** - Create StreamingDatasetConverter with LeRobot config
    /// 3. **Process frames** - Decode, align, and write frames through the pipeline
    /// 4. **Finalize** - Complete episode and write metadata
    /// 5. **Upload output** - Output files written to storage (via storage backend)
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
                // Note: Checkpoint-based resume will be implemented in a follow-up issue.
                // For Phase 1, we start from beginning even if checkpoint exists.
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

        // Build the full input path from source_key.
        // For cloud storage (S3/OSS), we need the full URL for the converter to download.
        // For local storage, strip storage_prefix to avoid double-prefixing with LocalStorage.
        let is_cloud_storage = job.source_bucket != "local";
        let input_path = if is_cloud_storage {
            // Build S3/OSS URL: s3://bucket/key or oss://bucket/key
            // Note: output_prefix contains the full URL scheme, extract the scheme from it
            let scheme = if job.output_prefix.starts_with("s3://") {
                "s3://"
            } else if job.output_prefix.starts_with("oss://") {
                "oss://"
            } else {
                // Default to s3 for compatibility
                "s3://"
            };
            PathBuf::from(format!(
                "{}{}/{}",
                scheme, job.source_bucket, job.source_key
            ))
        } else if let Some(prefix) = job.source_key.strip_prefix(&self.config.storage_prefix) {
            PathBuf::from(prefix)
        } else {
            PathBuf::from(&job.source_key)
        };

        // Build the output path for this job
        let output_path = self.build_output_path(job);

        // Determine output storage and prefix for staging
        // When output_storage_url is configured, use cloud storage with staging pattern
        let (output_storage, staging_prefix) =
            if let Some(storage_url) = &self.config.output_storage_url {
                // Create output storage from configured URL
                match self.storage_factory.create(storage_url) {
                    Ok(storage) => {
                        // Staging pattern: {storage_url}/staging/{job_id}/worker_{pod_id}/
                        // Each worker writes to its own subdirectory for isolation
                        let staging_prefix = format!("staging/{}/worker_{}", job.id, self.pod_id);
                        tracing::info!(
                            storage_url = %storage_url,
                            staging_prefix = %staging_prefix,
                            "Using cloud storage with staging pattern"
                        );
                        (Some(storage), Some(staging_prefix))
                    }
                    Err(e) => {
                        tracing::warn!(
                            storage_url = %storage_url,
                            error = %e,
                            "Failed to create output storage, falling back to local storage"
                        );
                        (None, None)
                    }
                }
            } else {
                (None, None)
            };

        tracing::info!(
            input = %input_path.display(),
            output = %output_path.display(),
            cloud_output = staging_prefix.is_some(),
            "Starting conversion"
        );

        // Create the LeRobot configuration
        let lerobot_config = match self.create_lerobot_config(job).await {
            Ok(config) => config,
            Err(e) => {
                let error_msg = format!("Failed to load config for job {}: {}", job.id, e);
                tracing::error!(
                    job_id = %job.id,
                    original_error = %e,
                    "Failed to load LeRobot config"
                );
                return ProcessingResult::Failed { error: error_msg };
            }
        };

        // Create streaming converter with storage backends
        // For cloud storage inputs, pass None for input_storage to let converter
        // download the file. For local storage, pass self.storage for fast path.
        let input_storage = if job.source_bucket != "local" {
            None
        } else {
            Some(self.storage.clone())
        };

        // Use cloud output storage if configured, otherwise use local storage
        let output_storage_for_converter = output_storage
            .clone()
            .or_else(|| Some(self.storage.clone()));

        let mut converter = match StreamingDatasetConverter::new_lerobot_with_storage(
            &output_path,
            lerobot_config,
            input_storage,
            output_storage_for_converter,
        ) {
            Ok(c) => c,
            Err(e) => {
                let error_msg = format!(
                    "Failed to create converter for job {} (input: {}, output: {}): {}",
                    job.id,
                    input_path.display(),
                    output_path.display(),
                    e
                );
                tracing::error!(
                    job_id = %job.id,
                    input = %input_path.display(),
                    output = %output_path.display(),
                    original_error = %e,
                    "Converter creation failed"
                );
                return ProcessingResult::Failed { error: error_msg };
            }
        };

        // Set staging prefix if using cloud storage
        if let Some(ref prefix) = staging_prefix {
            converter = converter.with_output_prefix(prefix.clone());
        }

        // Add checkpoint callback if enabled
        let job_id = job.id.clone();
        // Estimate total frames from source file size.
        // Heuristic: ~100KB per frame for typical robotics data (images + state).
        // This is approximate; actual frame count is updated as we process.
        // A more accurate estimate could be obtained by parsing the MCAP file
        // header, but this requires additional I/O and parsing complexity.
        let estimated_frame_size = 100_000; // 100KB per frame
        let total_frames = (job.source_size / estimated_frame_size).max(1);

        // Create cancellation token for this job
        let cancel_token = self.cancellation_token.child_token();
        let cancel_token_for_monitor = Arc::new(cancel_token.clone());
        let cancel_token_for_callback = Arc::new(cancel_token.clone());

        // Create progress callback with cancellation token
        let checkpoint_callback = Arc::new(WorkerCheckpointCallback {
            job_id: job_id.clone(),
            pod_id: self.pod_id.clone(),
            total_frames,
            checkpoint_manager: self.checkpoint_manager.clone(),
            last_checkpoint_frame: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            last_checkpoint_time: Arc::new(std::sync::Mutex::new(std::time::Instant::now())),
            shutdown_flag: self.shutdown_handler.flag_clone(),
            cancellation_token: Some(cancel_token_for_callback),
        });
        converter = converter.with_progress_callback(checkpoint_callback);

        // Register this job with the batch cancellation monitor
        {
            let mut registry = self.job_registry.write().await;
            registry.register(job_id.clone(), cancel_token_for_monitor);
        }
        tracing::debug!(
            job_id = %job_id,
            "Registered job with batch cancellation monitor"
        );

        // Run the conversion with a timeout to prevent indefinite hangs.
        // Note: This is a synchronous operation that may take significant time.
        // We use spawn_blocking to avoid starving the async runtime.
        // A cancellation token is used to attempt cooperative cancellation on timeout.
        use std::time::Duration;
        const CONVERSION_TIMEOUT: Duration = Duration::from_secs(3600); // 1 hour

        let job_id_clone = job_id.clone();
        let cancel_token_for_timeout = cancel_token.clone();
        let tikv_for_status_check = self.tikv.clone();
        let job_registry_for_cleanup = self.job_registry.clone();

        let conversion_task = tokio::task::spawn_blocking(move || {
            // Guard cancels the token when dropped (on task completion)
            let _guard = cancel_token.drop_guard();
            converter.convert(input_path)
        });

        let stats = match tokio::time::timeout(CONVERSION_TIMEOUT, conversion_task).await {
            Ok(Ok(Ok(stats))) => {
                // Unregister from cancellation monitor
                let mut registry = job_registry_for_cleanup.write().await;
                registry.unregister(&job_id_clone);
                stats
            }
            Ok(Ok(Err(e))) => {
                // Unregister from cancellation monitor
                let mut registry = job_registry_for_cleanup.write().await;
                registry.unregister(&job_id_clone);

                let error_msg = format!("Conversion failed for job {}: {}", job_id_clone, e);
                tracing::error!(
                    job_id = %job_id_clone,
                    original_error = %e,
                    "Job processing failed"
                );
                return ProcessingResult::Failed { error: error_msg };
            }
            Ok(Err(join_err)) => {
                // Unregister from cancellation monitor
                let mut registry = job_registry_for_cleanup.write().await;
                registry.unregister(&job_id_clone);

                // Check if this was a job cancellation (not timeout)
                if join_err.is_cancelled() {
                    // Check job status to distinguish cancellation types
                    match tikv_for_status_check.get_job(&job_id_clone).await {
                        Ok(Some(job)) if job.status == JobStatus::Cancelled => {
                            tracing::info!(
                                job_id = %job_id_clone,
                                "Job was cancelled"
                            );
                            return ProcessingResult::Cancelled;
                        }
                        _ => {
                            // Regular task cancellation or error checking status
                            let error_msg =
                                format!("Conversion task cancelled for job {}", job_id_clone);
                            tracing::error!(
                                job_id = %job_id_clone,
                                join_error = %join_err,
                                "Job processing task failed"
                            );
                            return ProcessingResult::Failed { error: error_msg };
                        }
                    }
                }

                let error_msg = format!(
                    "Conversion task panicked for job {}: {}",
                    job_id_clone, join_err
                );
                tracing::error!(
                    job_id = %job_id_clone,
                    join_error = %join_err,
                    "Job processing task failed"
                );
                return ProcessingResult::Failed { error: error_msg };
            }
            Err(_) => {
                // Unregister from cancellation monitor
                let mut registry = job_registry_for_cleanup.write().await;
                registry.unregister(&job_id_clone);

                // Timeout: request cancellation to potentially stop the blocking work
                cancel_token_for_timeout.cancel();
                let error_msg = format!(
                    "Conversion timed out after {:?} for job {}",
                    CONVERSION_TIMEOUT, job_id_clone
                );
                tracing::error!(
                    job_id = %job_id_clone,
                    timeout_secs = CONVERSION_TIMEOUT.as_secs(),
                    "Job processing timed out"
                );
                return ProcessingResult::Failed { error: error_msg };
            }
        };

        tracing::info!(
            job_id = %job_id,
            frames_written = stats.frames_written,
            messages = stats.messages_processed,
            duration_sec = stats.duration_sec,
            "Job processing complete"
        );

        // Register staging completion and try to claim merge task
        // This is only done when using cloud storage with staging pattern
        if let Some(prefix) = &staging_prefix {
            // Full staging path includes the storage URL
            let storage_url = self.config.output_storage_url.as_deref().unwrap_or("");
            let staging_path = format!("{}/{}", storage_url, prefix);

            tracing::info!(
                job_id = %job_id,
                staging_path = %staging_path,
                frame_count = stats.frames_written,
                "Registering staging completion"
            );

            // Register that this worker has completed staging
            if let Err(e) = self
                .merge_coordinator
                .register_staging_complete(
                    &job_id,
                    &self.pod_id,
                    staging_path,
                    stats.frames_written as u64,
                )
                .await
            {
                tracing::warn!(
                    job_id = %job_id,
                    error = %e,
                    "Failed to register staging completion, merge may not proceed"
                );
            } else {
                // Try to claim the merge task
                tracing::info!(
                    job_id = %job_id,
                    expected_workers = self.config.expected_workers,
                    merge_output = %self.config.merge_output_path,
                    "Attempting to claim merge task"
                );

                match self
                    .merge_coordinator
                    .try_claim_merge(
                        &job_id,
                        self.config.expected_workers,
                        self.config.merge_output_path.clone(),
                    )
                    .await
                {
                    Ok(super::merge::MergeResult::Success {
                        output_path,
                        total_frames,
                    }) => {
                        tracing::info!(
                            job_id = %job_id,
                            output_path = %output_path,
                            total_frames,
                            "Merge completed successfully"
                        );
                    }
                    Ok(super::merge::MergeResult::NotClaimed) => {
                        tracing::debug!(
                            job_id = %job_id,
                            "Merge task claimed by another worker"
                        );
                    }
                    Ok(super::merge::MergeResult::NotReady) => {
                        tracing::debug!(
                            job_id = %job_id,
                            "Merge not ready, waiting for more workers"
                        );
                    }
                    Ok(super::merge::MergeResult::Failed { error }) => {
                        tracing::error!(
                            job_id = %job_id,
                            error = %error,
                            "Merge failed"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            job_id = %job_id,
                            error = %e,
                            "Failed to claim merge task"
                        );
                    }
                }
            }
        }

        ProcessingResult::Success
    }

    /// Build the output path for a job.
    ///
    /// The output path follows the pattern: `{output_prefix}/{job_id}/`
    /// This ensures each job has a unique output directory.
    fn build_output_path(&self, job: &JobRecord) -> PathBuf {
        // Create a job-specific output directory.
        // Pattern: output_prefix/job_id/
        PathBuf::from(format!(
            "{}/{}",
            self.config.output_prefix.trim_end_matches('/'),
            job.id
        ))
    }

    /// Create a LeRobot configuration for processing a job.
    ///
    /// Loads the configuration from TiKV using the config_hash stored in the job.
    /// Uses an LRU cache to reduce TiKV round-trips for frequently used configs.
    async fn create_lerobot_config(&self, job: &JobRecord) -> Result<LerobotConfig, TikvError> {
        use roboflow_dataset::lerobot::config::DatasetConfig;

        let config_hash = &job.config_hash;

        // Skip empty hash (special case for "default" or legacy behavior)
        if config_hash.is_empty() || config_hash == "default" {
            tracing::warn!(
                pod_id = %self.pod_id,
                job_id = %job.id,
                config_hash = %config_hash,
                "Using default empty config (will produce no frames)"
            );
            return Ok(LerobotConfig {
                dataset: DatasetConfig {
                    name: format!("roboflow-episode-{}", job.id),
                    fps: 30,
                    robot_type: Some("robot".to_string()),
                    env_type: None,
                },
                mappings: Vec::new(),
                video: VideoConfig::default(),
                annotation_file: None,
            });
        }

        // Check cache first
        {
            let mut cache = self.config_cache.lock().await;
            if let Some(config) = cache.get(config_hash) {
                tracing::debug!(
                    pod_id = %self.pod_id,
                    job_id = %job.id,
                    config_hash = %config_hash,
                    "Loaded config from cache"
                );
                return Ok(config.clone());
            }
        }

        // Cache miss - fetch from TiKV
        let config = match self.tikv.get_config(config_hash).await {
            Ok(Some(record)) => {
                tracing::info!(
                    pod_id = %self.pod_id,
                    job_id = %job.id,
                    config_hash = %config_hash,
                    "Loaded config from TiKV"
                );
                LerobotConfig::from_toml(&record.content)
                    .map_err(|e| TikvError::Other(format!("Failed to parse config TOML: {}", e)))?
            }
            Ok(None) => {
                // Config not found in TiKV - this is a critical error
                tracing::error!(
                    pod_id = %self.pod_id,
                    job_id = %job.id,
                    config_hash = %config_hash,
                    "Config not found in TiKV"
                );
                return Err(TikvError::Other(format!(
                    "Config '{}' not found in TiKV for job {}",
                    config_hash, job.id
                )));
            }
            Err(e) => {
                tracing::error!(
                    pod_id = %self.pod_id,
                    job_id = %job.id,
                    config_hash = %config_hash,
                    error = %e,
                    "Failed to fetch config from TiKV"
                );
                return Err(TikvError::Other(format!(
                    "Failed to fetch config '{}' from TiKV: {}",
                    config_hash, e
                )));
            }
        };

        // Store in cache for future use
        {
            let mut cache = self.config_cache.lock().await;
            cache.put(config_hash.clone(), config.clone());
        }

        Ok(config)
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

    /// Release a job back to Pending status (for graceful shutdown).
    ///
    /// This is called when shutdown is requested during job processing.
    /// The job is returned to Pending state so another worker can pick it up,
    /// and the checkpoint is preserved for resume capability.
    async fn release_job(&self, job_id: &str) -> Result<(), TikvError> {
        // Fetch the current job record
        let Some(mut job) = self.tikv.get_job(job_id).await? else {
            tracing::warn!(
                pod_id = %self.pod_id,
                job_id = %job_id,
                "Job not found for release"
            );
            return Ok(()); // Job doesn't exist, nothing to release
        };

        // Only release if we own this job
        if job.owner.as_ref() != Some(&self.pod_id) {
            tracing::warn!(
                pod_id = %self.pod_id,
                job_id = %job_id,
                owner = ?job.owner,
                "Cannot release job: not owned by this worker"
            );
            return Ok(());
        }

        // Reset job to Pending status without incrementing attempts
        job.status = JobStatus::Pending;
        job.owner = None;
        job.updated_at = chrono::Utc::now();

        // Save the updated job record
        self.tikv.put_job(&job).await?;

        self.metrics.dec_active_jobs();

        tracing::info!(
            pod_id = %self.pod_id,
            job_id = %job_id,
            "Job released back to Pending due to shutdown"
        );

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
        // Start signal handler for SIGTERM/SIGINT
        let mut shutdown_rx = self.shutdown_handler.start_signal_handler();
        let shutdown_tx = self.shutdown_handler.sender();

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

        // Start cancellation monitor task
        // This batch-checks all active jobs for cancellation, reducing TiKV load
        let tikv_for_monitor = self.tikv.clone();
        let job_registry_for_monitor = self.job_registry.clone();
        let pod_id_for_monitor = self.pod_id.clone();
        let mut cancel_monitor_rx = shutdown_tx.subscribe();

        let _cancel_monitor_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(
                DEFAULT_CANCELLATION_CHECK_INTERVAL_SECS,
            ));
            interval.tick().await; // Skip first tick

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Get all active job IDs
                        let job_ids = {
                            let registry = job_registry_for_monitor.read().await;
                            registry.job_ids()
                        };

                        if job_ids.is_empty() {
                            continue;
                        }

                        // Batch check all jobs for cancellation
                        match tikv_for_monitor
                            .batch_get_jobs(&job_ids)
                            .await
                        {
                            Ok(jobs) => {
                                let mut registry = job_registry_for_monitor.write().await;
                                for (job_id, job) in jobs {
                                    if let Some(job) = job {
                                        if job.status == JobStatus::Cancelled {
                                            tracing::info!(
                                                pod_id = %pod_id_for_monitor,
                                                job_id = %job_id,
                                                "Job cancellation detected in batch check"
                                            );
                                            registry.cancel_job(&job_id);
                                            registry.unregister(&job_id);
                                        }
                                    } else {
                                        // Job no longer exists, unregister
                                        registry.unregister(&job_id);
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    pod_id = %pod_id_for_monitor,
                                    job_count = job_ids.len(),
                                    error = %e,
                                    "Failed to batch check job cancellation status"
                                );
                            }
                        }
                    }
                    _ = cancel_monitor_rx.recv() => {
                        tracing::info!(
                            pod_id = %pod_id_for_monitor,
                            "Cancellation monitor task shutting down"
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

                        // Check if shutdown was requested before processing
                        if self.shutdown_handler.is_requested() {
                            tracing::info!(
                                pod_id = %self.pod_id,
                                "Shutdown requested, not processing new job"
                            );
                            // Release the job back to Pending
                            if let Err(e) = self.release_job(&job_id).await {
                                tracing::error!(
                                    pod_id = %self.pod_id,
                                    job_id = %job_id,
                                    error = %e,
                                    "CRITICAL: Failed to release job during shutdown - job may be stuck in Processing state"
                                );
                            }
                            break;
                        }

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
                                // Check if this was a shutdown interrupt
                                if error.contains("Job interrupted by shutdown") {
                                    tracing::info!(
                                        pod_id = %self.pod_id,
                                        job_id = %job_id,
                                        "Job interrupted by shutdown, releasing back to Pending"
                                    );
                                    let _ = self.release_job(&job_id).await;
                                    break;
                                }

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
                            ProcessingResult::Cancelled => {
                                tracing::info!(
                                    pod_id = %self.pod_id,
                                    job_id = %job_id,
                                    "Job was cancelled by user, keeping in Cancelled state"
                                );
                                // Job is already marked as Cancelled in TiKV by the cancel command
                                // Do NOT release back to Pending - cancelled jobs should not be re-claimed
                                self.metrics.dec_active_jobs();
                                // Don't break the loop - continue processing other jobs
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

        // Delete heartbeat key to prevent false zombie detection
        let heartbeat_key = super::tikv::key::HeartbeatKeys::heartbeat(&self.pod_id);
        match self.tikv.delete(heartbeat_key).await {
            Ok(()) => {
                tracing::info!(
                    pod_id = %self.pod_id,
                    "Heartbeat key deleted on shutdown"
                );
            }
            Err(TikvError::KeyNotFound(_)) => {
                tracing::debug!(
                    pod_id = %self.pod_id,
                    "Heartbeat key not found (may have been already deleted)"
                );
            }
            Err(e) => {
                tracing::warn!(
                    pod_id = %self.pod_id,
                    error = %e,
                    "Failed to delete heartbeat key - zombie detection may trigger false positive"
                );
            }
        }

        tracing::info!(
            pod_id = %self.pod_id,
            "Worker stopped"
        );

        Ok(())
    }

    /// Shutdown the worker gracefully.
    pub fn shutdown(&self) -> Result<(), TikvError> {
        self.shutdown_handler.shutdown();
        Ok(())
    }

    /// Check if shutdown has been requested.
    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_handler.is_requested()
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
        assert_eq!(
            config.checkpoint_interval_frames,
            DEFAULT_CHECKPOINT_INTERVAL_FRAMES
        );
        assert_eq!(
            config.checkpoint_interval_seconds,
            DEFAULT_CHECKPOINT_INTERVAL_SECS
        );
        assert!(config.checkpoint_async);
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
        assert_eq!(DEFAULT_CHECKPOINT_INTERVAL_FRAMES, 100);
        assert_eq!(DEFAULT_CHECKPOINT_INTERVAL_SECS, 10);
    }
}
