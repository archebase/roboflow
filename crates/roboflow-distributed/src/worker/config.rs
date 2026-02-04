// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Worker configuration.

use std::time::Duration;

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
