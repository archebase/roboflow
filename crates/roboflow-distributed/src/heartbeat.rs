// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Worker heartbeat management for liveness tracking.
//!
//! The heartbeat system provides:
//! - Periodic heartbeat updates to TiKV
//! - Worker status tracking (Idle, Busy, Draining, Unhealthy)
//! - Current job tracking
//! - Combined heartbeat+checkpoint transactions for efficiency
//!
//! ## Design
//!
//! Heartbeats are written to `/heartbeat/{pod_id}` in TiKV with:
//! - `pod_id`: Unique worker identifier
//! - `last_seen`: Timestamp of last heartbeat
//! - `status`: Current worker status
//! - `current_job`: Optional job ID being processed
//! - `started_at`: Worker start time
//! - `hostname`: Worker hostname for debugging

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use super::tikv::{
    TikvError,
    client::TikvClient,
    schema::{HeartbeatRecord, WorkerStatus},
};
use tokio::sync::broadcast;

/// Default heartbeat interval in seconds.
pub const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = 30;

/// Default stale threshold in seconds (5 minutes).
pub const DEFAULT_STALE_THRESHOLD_SECS: i64 = 300;

/// Heartbeat manager configuration.
#[derive(Debug, Clone)]
pub struct HeartbeatConfig {
    /// Interval between heartbeat updates.
    pub interval: Duration,

    /// Threshold for considering a heartbeat stale.
    pub stale_threshold: Duration,

    /// Current job ID being processed (optional).
    pub current_job: Arc<tokio::sync::Mutex<Option<String>>>,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(DEFAULT_HEARTBEAT_INTERVAL_SECS),
            stale_threshold: Duration::from_secs(DEFAULT_STALE_THRESHOLD_SECS as u64),
            current_job: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }
}

impl HeartbeatConfig {
    /// Create a new heartbeat configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the heartbeat interval.
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Set the stale threshold.
    pub fn with_stale_threshold(mut self, threshold: Duration) -> Self {
        self.stale_threshold = threshold;
        self
    }

    /// Set the current job tracker.
    pub fn with_current_job(mut self, tracker: Arc<tokio::sync::Mutex<Option<String>>>) -> Self {
        self.current_job = tracker;
        self
    }
}

/// Heartbeat manager metrics.
#[derive(Debug, Default)]
pub struct HeartbeatMetrics {
    /// Total heartbeat updates sent.
    pub updates_total: AtomicU64,

    /// Total heartbeat errors.
    pub errors_total: AtomicU64,

    /// Time of last successful heartbeat (Unix timestamp).
    pub last_success: AtomicU64,

    /// Age of last heartbeat in seconds (for gauge).
    pub last_age_seconds: AtomicU64,
}

impl HeartbeatMetrics {
    /// Create new metrics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment updates counter.
    pub fn inc_updates(&self) {
        self.updates_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment errors counter.
    pub fn inc_errors(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Update last success timestamp.
    pub fn update_success(&self) {
        self.last_success.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            Ordering::Relaxed,
        );
    }

    /// Get current heartbeat age in seconds.
    pub fn age_seconds(&self) -> u64 {
        self.last_age_seconds.load(Ordering::Relaxed)
    }

    /// Update heartbeat age.
    pub fn update_age(&self, age: u64) {
        self.last_age_seconds.store(age, Ordering::Relaxed);
    }

    /// Get all current metric values.
    pub fn snapshot(&self) -> HeartbeatMetricsSnapshot {
        HeartbeatMetricsSnapshot {
            updates_total: self.updates_total.load(Ordering::Relaxed),
            errors_total: self.errors_total.load(Ordering::Relaxed),
            last_success: self.last_success.load(Ordering::Relaxed),
            last_age_seconds: self.last_age_seconds.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of heartbeat metrics.
#[derive(Debug, Clone)]
pub struct HeartbeatMetricsSnapshot {
    /// Total heartbeat updates sent.
    pub updates_total: u64,

    /// Total heartbeat errors.
    pub errors_total: u64,

    /// Time of last successful heartbeat (Unix timestamp).
    pub last_success: u64,

    /// Age of last heartbeat in seconds.
    pub last_age_seconds: u64,
}

/// Heartbeat manager for worker liveness tracking.
///
/// Manages periodic heartbeat updates to TiKV and provides
/// methods for combining heartbeats with checkpoint updates.
pub struct HeartbeatManager {
    /// Pod ID for this worker instance.
    pod_id: String,

    /// TiKV client for heartbeat operations.
    tikv: Arc<TikvClient>,

    /// Heartbeat configuration.
    config: HeartbeatConfig,

    /// Heartbeat metrics.
    metrics: Arc<HeartbeatMetrics>,

    /// Shutdown sender for background task.
    shutdown_tx: Option<broadcast::Sender<()>>,

    /// Hostname for debugging.
    hostname: String,

    /// Worker start time.
    started_at: chrono::DateTime<chrono::Utc>,
}

impl HeartbeatManager {
    /// Create a new heartbeat manager.
    pub fn new(
        pod_id: impl Into<String>,
        tikv: Arc<TikvClient>,
        config: HeartbeatConfig,
    ) -> Result<Self, TikvError> {
        let pod_id = pod_id.into();
        let hostname = gethostname::gethostname()
            .to_str()
            .unwrap_or("unknown")
            .to_string();

        Ok(Self {
            pod_id,
            tikv,
            config,
            metrics: Arc::new(HeartbeatMetrics::new()),
            shutdown_tx: None,
            hostname,
            started_at: chrono::Utc::now(),
        })
    }

    /// Get the pod ID.
    pub fn pod_id(&self) -> &str {
        &self.pod_id
    }

    /// Get a reference to the metrics.
    pub fn metrics(&self) -> &HeartbeatMetrics {
        &self.metrics
    }

    /// Get a reference to the configuration.
    pub fn config(&self) -> &HeartbeatConfig {
        &self.config
    }

    /// Update the heartbeat immediately.
    ///
    /// This is a synchronous update that writes the heartbeat to TiKV.
    /// It's called automatically by the background task, but can also
    /// be called manually (e.g., after checkpoint updates).
    #[must_use = "heartbeat errors should be handled to detect TiKV issues"]
    pub async fn update_heartbeat(&self) -> Result<(), TikvError> {
        let current_job = self.config.current_job.lock().await.clone();
        let active_jobs = if current_job.is_some() { 1 } else { 0 };

        // Get existing heartbeat or create new one
        let mut heartbeat = self
            .tikv
            .get_heartbeat(&self.pod_id)
            .await?
            .unwrap_or_else(|| {
                let mut hb = HeartbeatRecord::new(self.pod_id.clone());
                hb.metadata = Some(serde_json::json!({
                    "hostname": self.hostname.clone(),
                    "started_at": self.started_at.to_rfc3339(),
                }));
                hb
            });

        // Update heartbeat fields
        heartbeat.beat();
        heartbeat.active_jobs = active_jobs as u32;
        heartbeat.status = if active_jobs > 0 {
            WorkerStatus::Busy
        } else {
            WorkerStatus::Idle
        };

        // Add current job to metadata if processing
        if let Some(job_id) = &current_job
            && let Some(ref mut metadata) = heartbeat.metadata
            && let Some(obj) = metadata.as_object_mut()
        {
            obj.insert("current_job".to_string(), serde_json::json!(job_id));
        }

        self.tikv.update_heartbeat(&self.pod_id, &heartbeat).await?;

        // Update metrics
        self.metrics.inc_updates();
        self.metrics.update_success();

        // Calculate heartbeat age
        let age = chrono::Utc::now().signed_duration_since(heartbeat.last_heartbeat);
        self.metrics.update_age(age.num_seconds().max(0) as u64);

        tracing::debug!(
            pod_id = %self.pod_id,
            status = ?heartbeat.status,
            current_job = current_job.as_deref().unwrap_or("none"),
            "Heartbeat updated"
        );

        Ok(())
    }

    /// Start the background heartbeat task.
    ///
    /// Returns a receiver that can be used to wait for shutdown.
    pub fn start_background_task(&mut self) -> broadcast::Receiver<()> {
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        self.shutdown_tx = Some(shutdown_tx.clone());

        let tikv = self.tikv.clone();
        let pod_id = self.pod_id.clone();
        let metrics = self.metrics.clone();
        let interval = self.config.interval;
        let mut rx = shutdown_tx.subscribe();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);

            // Send first heartbeat immediately, then tick to reset the interval
            if let Err(e) = update_heartbeat_inner(&tikv, &pod_id).await {
                metrics.inc_errors();
                tracing::error!(
                    pod_id = %pod_id,
                    error = %e,
                    "Failed to send initial heartbeat"
                );
            }
            ticker.tick().await; // Reset interval after immediate heartbeat

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(e) = update_heartbeat_inner(&tikv, &pod_id).await {
                            metrics.inc_errors();
                            tracing::error!(
                                pod_id = %pod_id,
                                error = %e,
                                "Failed to send heartbeat"
                            );
                        }
                    }
                    _ = rx.recv() => {
                        tracing::info!(
                            pod_id = %pod_id,
                            "Heartbeat task shutting down"
                        );
                        break;
                    }
                }
            }
        });

        shutdown_rx
    }

    /// Stop the background heartbeat task.
    pub fn shutdown(&self) -> Result<(), TikvError> {
        if let Some(ref tx) = self.shutdown_tx {
            let _ = tx.send(());
            tracing::info!(
                pod_id = %self.pod_id,
                "Heartbeat shutdown signal sent"
            );
            Ok(())
        } else {
            Err(TikvError::Other(
                "Cannot shutdown heartbeat: no background task running".to_string(),
            ))
        }
    }

    /// Delete the heartbeat key from TiKV (cleanup on shutdown).
    #[must_use = "cleanup errors should be handled to detect TiKV issues"]
    pub async fn cleanup(&self) -> Result<(), TikvError> {
        let key = super::tikv::key::HeartbeatKeys::heartbeat(&self.pod_id);
        self.tikv.delete(key).await?;

        tracing::info!(
            pod_id = %self.pod_id,
            "Heartbeat key deleted"
        );

        Ok(())
    }

    /// Send a heartbeat with a specific status.
    ///
    /// Useful for state transitions (e.g., shutdown, error).
    #[must_use = "heartbeat errors should be handled to detect TiKV issues"]
    pub async fn send_with_status(&self, status: WorkerStatus) -> Result<(), TikvError> {
        let mut heartbeat = self
            .tikv
            .get_heartbeat(&self.pod_id)
            .await?
            .unwrap_or_else(|| HeartbeatRecord::new(self.pod_id.clone()));

        heartbeat.beat();
        heartbeat.status = status;

        self.tikv.update_heartbeat(&self.pod_id, &heartbeat).await?;
        self.metrics.inc_updates();
        self.metrics.update_success();

        tracing::info!(
            pod_id = %self.pod_id,
            status = ?status,
            "Heartbeat sent with status"
        );

        Ok(())
    }
}

/// Helper function for sending heartbeat (used in spawned task).
///
/// This is a minimal "liveness" heartbeat used by start_background_task().
/// It sets status to Idle to indicate the worker is available.
/// For accurate Busy/Idle status based on active jobs, use Worker::run()
/// which manages heartbeats with proper status tracking.
async fn update_heartbeat_inner(tikv: &TikvClient, pod_id: &str) -> Result<(), TikvError> {
    let mut heartbeat = tikv
        .get_heartbeat(pod_id)
        .await?
        .unwrap_or_else(|| HeartbeatRecord::new(pod_id.to_string()));

    heartbeat.beat();
    heartbeat.status = WorkerStatus::Idle;

    tikv.update_heartbeat(pod_id, &heartbeat).await
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heartbeat_config_default() {
        let config = HeartbeatConfig::default();
        assert_eq!(config.interval.as_secs(), DEFAULT_HEARTBEAT_INTERVAL_SECS);
        assert_eq!(
            config.stale_threshold.as_secs(),
            DEFAULT_STALE_THRESHOLD_SECS as u64
        );
    }

    #[test]
    fn test_heartbeat_config_builder() {
        let tracker = Arc::new(tokio::sync::Mutex::new(None));
        let config = HeartbeatConfig::new()
            .with_interval(Duration::from_secs(60))
            .with_stale_threshold(Duration::from_secs(600))
            .with_current_job(tracker);

        assert_eq!(config.interval.as_secs(), 60);
        assert_eq!(config.stale_threshold.as_secs(), 600);
    }

    #[test]
    fn test_heartbeat_metrics() {
        let metrics = HeartbeatMetrics::new();

        assert_eq!(metrics.updates_total.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.errors_total.load(Ordering::Relaxed), 0);

        metrics.inc_updates();
        metrics.inc_errors();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.updates_total, 1);
        assert_eq!(snapshot.errors_total, 1);
    }

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_HEARTBEAT_INTERVAL_SECS, 30);
        assert_eq!(DEFAULT_STALE_THRESHOLD_SECS, 300);
    }
}
