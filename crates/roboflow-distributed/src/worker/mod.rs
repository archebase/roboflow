// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Worker actor for claiming and processing work units from TiKV batch queue.
//!
//! # Architecture
//!
//! The worker is now composed of two main components:
//!
//! - **Coordinator**: Handles coordination logic (claiming work, heartbeats, shutdown)
//! - **LeRobotExecutor**: Handles execution logic using the stage-based executor framework
//!
//! This separation improves testability and maintainability.

pub mod config;
pub mod coordinator;
pub mod metrics;
pub mod registry;

pub use crate::executor::Executor;
pub use crate::lerobot_executor::LeRobotExecutor;
pub use config::{
    DEFAULT_CHECKPOINT_INTERVAL_FRAMES, DEFAULT_CHECKPOINT_INTERVAL_SECS,
    DEFAULT_HEARTBEAT_INTERVAL_SECS, DEFAULT_JOB_TIMEOUT_SECS, DEFAULT_MAX_ATTEMPTS,
    DEFAULT_MAX_CONCURRENT_JOBS, DEFAULT_POLL_INTERVAL_SECS, WorkerConfig,
};
pub use coordinator::{Coordinator, send_heartbeat_inner};
pub use metrics::{ProcessingResult, WorkerMetrics, WorkerMetricsSnapshot};
pub use registry::JobRegistry;

use std::sync::Arc;

use super::tikv::{TikvError, client::TikvClient};
use tokio::sync::RwLock;

use crate::episode::EpisodeAllocator;

/// Default cancellation check interval in seconds.
pub const DEFAULT_CANCELLATION_CHECK_INTERVAL_SECS: u64 = 5;

/// Worker actor for claiming and processing work units.
pub struct Worker {
    /// Coordinator for work unit management.
    coordinator: Coordinator,
    /// Executor for processing work units.
    executor: Box<dyn Executor>,
    /// Cancellation token for graceful shutdown.
    #[allow(dead_code)]
    cancellation_token: Arc<tokio_util::sync::CancellationToken>,
}

impl Worker {
    /// Create a new worker.
    pub fn new(
        pod_id: impl Into<String>,
        tikv: Arc<TikvClient>,
        config: WorkerConfig,
    ) -> Result<Self, TikvError> {
        let pod_id = pod_id.into();
        let cancellation_token = Arc::new(tokio_util::sync::CancellationToken::new());
        let job_registry = Arc::new(RwLock::new(JobRegistry::default()));

        // Create coordinator
        let coordinator = Coordinator::new(
            pod_id.clone(),
            tikv.clone(),
            config.clone(),
            job_registry.clone(),
        )?;

        // Create executor using stage-based framework
        let executor: Box<dyn Executor> = Box::new(LeRobotExecutor::new(
            config.max_concurrent_jobs,
            config.output_prefix.clone(),
        ));

        Ok(Self {
            coordinator,
            executor,
            cancellation_token,
        })
    }

    /// Create a worker with episode allocation for distributed processing.
    ///
    /// This enables:
    /// - Centralized episode index allocation via TiKV
    /// - Automatic chunk index calculation
    /// - LeRobot v2.1 compliant output structure
    ///
    /// # Arguments
    ///
    /// * `pod_id` - Unique identifier for this worker instance
    /// * `tikv` - TiKV client for coordination
    /// * `config` - Worker configuration
    /// * `episode_allocator` - Episode allocator (e.g., TiKVEpisodeAllocator)
    ///
    /// # Example
    ///
    /// ```ignore
    /// use roboflow_distributed::{
    ///     Worker, WorkerConfig, TiKVEpisodeAllocator,
    /// };
    ///
    /// let config = WorkerConfig::new()
    ///     .with_output_prefix("s3://bucket/dataset")
    ///     .with_episodes_per_chunk(500);
    ///
    /// let allocator = Arc::new(TiKVEpisodeAllocator::new(
    ///     tikv_client,
    ///     "batch-001".to_string(),
    ///     500,
    /// ));
    ///
    /// let worker = Worker::with_episode_allocator(
    ///     "worker-1",
    ///     tikv_client,
    ///     config,
    ///     allocator,
    /// )?;
    /// ```
    pub fn with_episode_allocator(
        pod_id: impl Into<String>,
        tikv: Arc<TikvClient>,
        config: WorkerConfig,
        episode_allocator: Arc<dyn EpisodeAllocator>,
    ) -> Result<Self, TikvError> {
        let pod_id = pod_id.into();
        let cancellation_token = Arc::new(tokio_util::sync::CancellationToken::new());
        let job_registry = Arc::new(RwLock::new(JobRegistry::default()));

        // Create coordinator
        let coordinator = Coordinator::new(
            pod_id.clone(),
            tikv.clone(),
            config.clone(),
            job_registry.clone(),
        )?;

        // Create executor with episode allocator using stage-based framework
        let executor: Box<dyn Executor> = Box::new(
            LeRobotExecutor::new(config.max_concurrent_jobs, config.output_prefix.clone())
                .with_episode_allocator(episode_allocator),
        );

        Ok(Self {
            coordinator,
            executor,
            cancellation_token,
        })
    }

    /// Get the pod ID.
    pub fn pod_id(&self) -> &str {
        self.coordinator.pod_id()
    }

    /// Get a reference to the metrics.
    pub fn metrics(&self) -> &WorkerMetrics {
        self.coordinator.metrics()
    }

    /// Get a reference to the configuration.
    pub fn config(&self) -> &WorkerConfig {
        self.coordinator.config()
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

    /// Send heartbeat to TiKV.
    pub async fn send_heartbeat(&self) -> Result<(), TikvError> {
        self.coordinator.send_heartbeat().await
    }

    /// Shutdown the worker gracefully.
    pub fn shutdown(&self) -> Result<(), TikvError> {
        self.coordinator.shutdown()
    }

    /// Check if shutdown has been requested.
    pub fn is_shutdown_requested(&self) -> bool {
        self.coordinator.is_shutdown_requested()
    }

    /// Run the worker loop.
    ///
    /// This will continuously:
    /// 1. Check for shutdown signal
    /// 2. Find and claim a work unit (if under concurrent limit)
    /// 3. Process the work unit
    /// 4. Complete or fail the work unit
    /// 5. Send periodic heartbeats
    /// 6. Repeat until shutdown
    pub async fn run(&mut self) -> Result<(), TikvError> {
        self.coordinator.run(&self.executor).await
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_worker_config_default() {
        let config = WorkerConfig::default();
        assert_eq!(config.max_concurrent_jobs, 1);
        assert_eq!(config.poll_interval.as_secs(), 5);
        assert_eq!(config.max_attempts, 3);
        assert_eq!(config.job_timeout.as_secs(), 3600);
        assert_eq!(config.heartbeat_interval.as_secs(), 30);
        assert_eq!(config.checkpoint_interval_frames, 100);
        assert_eq!(config.checkpoint_interval_seconds, 10);
        assert!(config.checkpoint_async);
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
            .with_output_prefix("results/");

        assert_eq!(config.max_concurrent_jobs, 5);
        assert_eq!(config.poll_interval.as_secs(), 10);
        assert_eq!(config.max_attempts, 5);
        assert_eq!(config.job_timeout.as_secs(), 7200);
        assert_eq!(config.heartbeat_interval.as_secs(), 60);
        assert_eq!(config.output_prefix, "results/");
    }

    #[test]
    fn test_worker_metrics() {
        let metrics = WorkerMetrics::new();

        assert_eq!(
            metrics
                .jobs_claimed
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_eq!(
            metrics
                .active_jobs
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );

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
        let pod_id = Worker::generate_pod_id();
        assert!(!pod_id.is_empty());
        assert!(pod_id.len() > 20);
    }
}
