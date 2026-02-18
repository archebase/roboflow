// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Worker configuration.

use std::time::Duration;

use roboflow_core::{Result, Validate, validators};

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

    /// Local output prefix for writing files (used when output_storage_url is not set).
    pub output_prefix: String,

    /// Cloud storage URL for output files (e.g., "s3://bucket/datasets").
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

    /// Number of episodes per chunk for LeRobot v2.1 format.
    ///
    /// Default is 500 (LeRobot v2.1 spec).
    /// When episode allocation is enabled, each work unit gets a unique episode index,
    /// and chunk directories are automatically created based on this value.
    pub episodes_per_chunk: u32,
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
            output_prefix: String::from("output/"),
            output_storage_url: None,
            expected_workers: 1,
            merge_output_path: String::from("datasets/merged"),
            episodes_per_chunk: crate::converter::DEFAULT_EPISODES_PER_CHUNK,
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

    /// Set the output prefix.
    pub fn with_output_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.output_prefix = prefix.into();
        self
    }

    /// Set the cloud storage URL for output files.
    ///
    /// When set, workers write to staging paths in cloud storage using a Staging + Merge pattern.
    /// Example: "s3://my-bucket/datasets"
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

    /// Set the episodes per chunk for LeRobot v2.1 format.
    ///
    /// Default is 500. This setting is used when episode allocation is enabled
    /// to determine which chunk directory each episode belongs to.
    pub fn with_episodes_per_chunk(mut self, episodes: u32) -> Self {
        self.episodes_per_chunk = episodes;
        self
    }
}

impl Validate for WorkerConfig {
    fn validate(&self) -> Result<()> {
        // Validate concurrency settings
        validators::positive(self.max_concurrent_jobs, "max_concurrent_jobs")?;
        validators::positive(self.max_attempts, "max_attempts")?;
        validators::positive(self.expected_workers, "expected_workers")?;

        // Validate intervals
        validators::positive(self.poll_interval.as_secs(), "poll_interval_secs")?;
        validators::positive(self.job_timeout.as_secs(), "job_timeout_secs")?;
        validators::positive(self.heartbeat_interval.as_secs(), "heartbeat_interval_secs")?;

        // Validate checkpoint intervals (can be 0 to disable)
        validators::non_negative(
            self.checkpoint_interval_frames,
            "checkpoint_interval_frames",
        )?;
        validators::non_negative(
            self.checkpoint_interval_seconds,
            "checkpoint_interval_seconds",
        )?;

        // Validate output paths
        validators::not_empty_str(&self.output_prefix, "output_prefix")?;
        validators::not_empty_str(&self.merge_output_path, "merge_output_path")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_config_default() {
        let config = WorkerConfig::default();

        assert_eq!(config.max_concurrent_jobs, DEFAULT_MAX_CONCURRENT_JOBS);
        assert_eq!(
            config.poll_interval,
            Duration::from_secs(DEFAULT_POLL_INTERVAL_SECS)
        );
        assert_eq!(config.max_attempts, DEFAULT_MAX_ATTEMPTS);
        assert_eq!(
            config.job_timeout,
            Duration::from_secs(DEFAULT_JOB_TIMEOUT_SECS)
        );
        assert_eq!(
            config.heartbeat_interval,
            Duration::from_secs(DEFAULT_HEARTBEAT_INTERVAL_SECS)
        );
        assert!(config.output_storage_url.is_none());
    }

    #[test]
    fn test_worker_config_builder() {
        let config = WorkerConfig::new()
            .with_max_concurrent_jobs(4)
            .with_poll_interval(Duration::from_secs(10))
            .with_max_attempts(5)
            .with_job_timeout(Duration::from_secs(7200))
            .with_heartbeat_interval(Duration::from_secs(15))
            .with_output_prefix("custom/output/")
            .with_output_storage_url("s3://my-bucket/datasets");

        assert_eq!(config.max_concurrent_jobs, 4);
        assert_eq!(config.poll_interval, Duration::from_secs(10));
        assert_eq!(config.max_attempts, 5);
        assert_eq!(config.job_timeout, Duration::from_secs(7200));
        assert_eq!(config.heartbeat_interval, Duration::from_secs(15));
        assert_eq!(config.output_prefix, "custom/output/");
        assert_eq!(
            config.output_storage_url,
            Some("s3://my-bucket/datasets".to_string())
        );
    }

    #[test]
    fn test_worker_config_validation_valid() {
        let config = WorkerConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_worker_config_validation_zero_concurrent_jobs() {
        let config = WorkerConfig::new().with_max_concurrent_jobs(0);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_worker_config_validation_zero_attempts() {
        let config = WorkerConfig::new().with_max_attempts(0);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_worker_config_checkpoint_settings() {
        let config = WorkerConfig::new()
            .with_checkpoint_interval_frames(200)
            .with_checkpoint_interval_seconds(20)
            .with_checkpoint_async(false);

        assert_eq!(config.checkpoint_interval_frames, 200);
        assert_eq!(config.checkpoint_interval_seconds, 20);
        assert!(!config.checkpoint_async);
    }

    #[test]
    fn test_worker_config_zero_checkpoint_disables() {
        // Zero checkpoint interval is valid (disables checkpointing)
        let config = WorkerConfig::new()
            .with_checkpoint_interval_frames(0)
            .with_checkpoint_interval_seconds(0);

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_worker_config_episodes_per_chunk() {
        let config = WorkerConfig::new().with_episodes_per_chunk(250);
        assert_eq!(config.episodes_per_chunk, 250);

        // Default should be 500
        let default_config = WorkerConfig::default();
        assert_eq!(default_config.episodes_per_chunk, 500);
    }
}
