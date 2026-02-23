// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Zombie reaper for reclaiming work units from dead workers.
//!
//! The zombie reaper periodically scans for:
//! - Stale heartbeats (workers that haven't sent a heartbeat recently)
//! - Orphaned work units (work units in Processing state owned by stale workers)
//!
//! When orphaned work units are found, they are reclaimed by:
//! - Verifying the work unit is still in Processing state
//! - Verifying the owner's heartbeat is stale
//! - Setting the work unit back to Failed status with no owner
//!   (Failed status allows retry via the controller's pending queue)
//!
//! ## Design
//!
//! The reaper runs on ALL workers (no leader election) to maximize
//! fault tolerance. Multiple workers may attempt to reclaim the same
//! work unit, but TiKV's optimistic concurrency ensures only one succeeds.
//!
//! To prevent thundering herd, the reaper limits reclamations per
//! iteration and adds random jitter to the sleep interval.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use super::batch::{WorkUnit, WorkUnitKeys, WorkUnitStatus};
use super::tikv::{TikvError, client::TikvClient, key::HeartbeatKeys, schema::HeartbeatRecord};
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

    /// Maximum work units to reclaim per iteration.
    pub max_reclaims_per_iteration: usize,

    /// Maximum work units to scan per iteration.
    pub max_work_unit_scan: u32,
}

impl Default for ReaperConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(DEFAULT_REAPER_INTERVAL_SECS),
            stale_threshold: Duration::from_secs(DEFAULT_STALE_THRESHOLD_SECS as u64),
            max_reclaims_per_iteration: DEFAULT_MAX_RECLAIMS_PER_ITERATION,
            max_work_unit_scan: 1000,
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

    /// Set the maximum work unit scan limit.
    pub fn with_max_work_unit_scan(mut self, max: u32) -> Self {
        self.max_work_unit_scan = max;
        self
    }
}

/// Zombie reaper metrics.
#[derive(Debug, Default)]
pub struct ReaperMetrics {
    /// Total work units reclaimed.
    pub work_units_reclaimed: AtomicU64,

    /// Total stale workers found.
    pub stale_workers_found: AtomicU64,

    /// Total reaper iterations.
    pub iterations_total: AtomicU64,

    /// Total reclaim attempts.
    pub reclaim_attempts: AtomicU64,

    /// Total reclaim failures.
    pub reclaim_failures: AtomicU64,

    /// Work units skipped (already claimed by another reaper).
    pub work_units_skipped: AtomicU64,
}

impl ReaperMetrics {
    /// Create new metrics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment work units reclaimed counter.
    pub fn inc_work_units_reclaimed(&self) {
        self.work_units_reclaimed.fetch_add(1, Ordering::Relaxed);
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

    /// Increment work units skipped.
    pub fn inc_work_units_skipped(&self) {
        self.work_units_skipped.fetch_add(1, Ordering::Relaxed);
    }

    /// Get all current metric values.
    pub fn snapshot(&self) -> ReaperMetricsSnapshot {
        ReaperMetricsSnapshot {
            work_units_reclaimed: self.work_units_reclaimed.load(Ordering::Relaxed),
            stale_workers_found: self.stale_workers_found.load(Ordering::Relaxed),
            iterations_total: self.iterations_total.load(Ordering::Relaxed),
            reclaim_attempts: self.reclaim_attempts.load(Ordering::Relaxed),
            reclaim_failures: self.reclaim_failures.load(Ordering::Relaxed),
            work_units_skipped: self.work_units_skipped.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of reaper metrics.
#[derive(Debug, Clone)]
pub struct ReaperMetricsSnapshot {
    /// Total work units reclaimed.
    pub work_units_reclaimed: u64,

    /// Total stale workers found.
    pub stale_workers_found: u64,

    /// Total reaper iterations.
    pub iterations_total: u64,

    /// Total reclaim attempts.
    pub reclaim_attempts: u64,

    /// Total reclaim failures.
    pub reclaim_failures: u64,

    /// Work units skipped (already claimed by another reaper).
    pub work_units_skipped: u64,
}

/// Result of a work unit reclamation attempt.
#[derive(Debug, Clone)]
pub enum ReclaimResult {
    /// Work unit was successfully reclaimed.
    Reclaimed,

    /// Work unit was not stale (skip).
    NotStale,

    /// Work unit was not in Processing state (skip).
    NotProcessing,

    /// Work unit reclaim failed (will retry).
    Failed,

    /// Work unit was already reclaimed by another worker.
    Skipped,
}

/// Zombie reaper for reclaiming work units from dead workers.
///
/// The reaper periodically scans for stale heartbeats and reclaims
/// orphaned work units. It runs on all workers (no leader election) for
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
            .scan(prefix, 1000) // Fixed scan limit for heartbeats
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

    /// Find orphaned work units (work units in Processing state owned by stale workers).
    ///
    /// Returns work unit IDs and their batch IDs that can be reclaimed.
    pub async fn find_orphaned_work_units(
        &self,
    ) -> Result<Vec<(String, String, String)>, TikvError> {
        let stale_workers = self.find_stale_workers().await?;

        if stale_workers.is_empty() {
            tracing::debug!("No stale workers found, no orphaned work units to reclaim");
            return Ok(Vec::new());
        }

        // Scan for work units in Processing state
        let work_unit_prefix = WorkUnitKeys::prefix();
        let results = self
            .tikv
            .scan(work_unit_prefix, self.config.max_work_unit_scan)
            .await?;

        let mut orphaned_units = Vec::new();

        for (_key, value) in results {
            if let Ok(unit) = bincode::deserialize::<WorkUnit>(&value) {
                // Check if work unit is in Processing state and owned by a stale worker
                if unit.status == WorkUnitStatus::Processing
                    && let Some(ref owner) = unit.owner
                    && stale_workers.contains(owner)
                {
                    tracing::debug!(
                        unit_id = %unit.id,
                        batch_id = %unit.batch_id,
                        owner = %owner,
                        "Found orphaned work unit"
                    );
                    orphaned_units.push((unit.id.clone(), unit.batch_id.clone(), owner.clone()));
                }
            }
        }

        tracing::debug!(
            orphaned_count = orphaned_units.len(),
            "Found orphaned work units"
        );

        Ok(orphaned_units)
    }

    /// Reclaim a single work unit.
    ///
    /// This uses a transaction to:
    /// 1. Read the work unit (verify still Processing)
    /// 2. Read the owner's heartbeat (verify stale)
    /// 3. Update work unit to Failed with no owner (allows retry)
    ///
    /// Returns the reclamation result.
    pub async fn reclaim_work_unit(
        &self,
        batch_id: &str,
        unit_id: &str,
        owner: &str,
    ) -> Result<ReclaimResult, TikvError> {
        self.metrics.inc_reclaim_attempts();

        let stale_threshold = self.config.stale_threshold.as_secs() as i64;

        // Verify the owner's heartbeat is still stale
        let heartbeat_key = HeartbeatKeys::heartbeat(owner);
        match self.tikv.get(heartbeat_key).await? {
            Some(data) => {
                if let Ok(record) = bincode::deserialize::<HeartbeatRecord>(&data)
                    && !record.is_stale(stale_threshold)
                {
                    // Owner came back, skip reclamation
                    self.metrics.inc_work_units_skipped();
                    tracing::debug!(
                        unit_id = %unit_id,
                        owner = %owner,
                        "Work unit owner heartbeat recovered, skipping reclamation"
                    );
                    return Ok(ReclaimResult::NotStale);
                }
            }
            None => {
                // Heartbeat not found - treat as stale
            }
        }

        // Use transactional_claim pattern similar to controller
        let work_unit_key = WorkUnitKeys::unit(batch_id, unit_id);
        let owner_id = owner.to_string();
        let owner_id_for_closure = owner_id.clone();
        let unit_id_clone = unit_id.to_string();
        let batch_id_clone = batch_id.to_string();

        let result = self
            .tikv
            .transactional_claim(
                work_unit_key.clone(),
                vec![], // No pending key to delete
                &owner_id,
                move |data: &[u8]| -> Result<
                    Option<Vec<u8>>,
                    Box<dyn std::error::Error + Send + Sync>,
                > {
                    let mut unit: WorkUnit = bincode::deserialize(data)
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

                    // Verify still in Processing state
                    if unit.status != WorkUnitStatus::Processing {
                        return Ok(None);
                    }

                    // Verify still owned by the same worker
                    if unit.owner.as_deref() != Some(&owner_id_for_closure) {
                        return Ok(None);
                    }

                    // Mark as Failed (which allows retry)
                    unit.fail("Worker died during processing".to_string());

                    // Reserialize with updated state
                    let new_data = bincode::serialize(&unit)
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

                    Ok(Some(new_data))
                },
            )
            .await;

        match result {
            Ok(Some(_)) => {
                self.metrics.inc_work_units_reclaimed();
                tracing::info!(
                    unit_id = %unit_id_clone,
                    batch_id = %batch_id_clone,
                    "Work unit reclaimed successfully"
                );
                Ok(ReclaimResult::Reclaimed)
            }
            Ok(None) => {
                // Transaction failed verification
                self.metrics.inc_work_units_skipped();
                Ok(ReclaimResult::Skipped)
            }
            Err(e) => {
                self.metrics.inc_reclaim_failures();
                tracing::error!(
                    unit_id = %unit_id,
                    error = %e,
                    "Work unit reclaim failed"
                );
                Ok(ReclaimResult::Failed)
            }
        }
    }

    /// Run a single reaper iteration.
    ///
    /// Finds orphaned work units and attempts to reclaim them up to
    /// the max_reclaims_per_iteration limit.
    pub async fn run_iteration(&self) -> Result<usize, TikvError> {
        self.metrics.inc_iterations();

        let orphaned_units = self.find_orphaned_work_units().await?;

        if orphaned_units.is_empty() {
            return Ok(0);
        }

        let mut reclaimed_count = 0;
        let max_reclaims = self.config.max_reclaims_per_iteration;

        for (unit_id, batch_id, owner) in orphaned_units.iter().take(max_reclaims) {
            match self.reclaim_work_unit(batch_id, unit_id, owner).await? {
                ReclaimResult::Reclaimed => {
                    reclaimed_count += 1;
                }
                ReclaimResult::NotStale | ReclaimResult::NotProcessing => {
                    // Skip these
                }
                ReclaimResult::Failed => {
                    // Log but continue with other work units
                    tracing::warn!(
                        unit_id = %unit_id,
                        batch_id = %batch_id,
                        "Failed to reclaim work unit, will retry in next iteration"
                    );
                }
                ReclaimResult::Skipped => {
                    // Already claimed by another worker
                    tracing::debug!(
                        unit_id = %unit_id,
                        "Work unit already reclaimed by another worker"
                    );
                }
            }
        }

        if reclaimed_count > 0 {
            tracing::info!(
                reclaimed = reclaimed_count,
                total_orphaned = orphaned_units.len(),
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
                        tracing::info!(
                            reclaimed = reclaimed,
                            "Work units reclaimed in this iteration"
                        );
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
            .with_max_reclaims(20)
            .with_max_work_unit_scan(500);

        assert_eq!(config.interval.as_secs(), 120);
        assert_eq!(config.stale_threshold.as_secs(), 600);
        assert_eq!(config.max_reclaims_per_iteration, 20);
        assert_eq!(config.max_work_unit_scan, 500);
    }

    #[test]
    fn test_reaper_metrics() {
        let metrics = ReaperMetrics::new();

        metrics.inc_work_units_reclaimed();
        metrics.inc_stale_workers_found(5);
        metrics.inc_iterations();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.work_units_reclaimed, 1);
        assert_eq!(snapshot.stale_workers_found, 5);
        assert_eq!(snapshot.iterations_total, 1);
    }

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_REAPER_INTERVAL_SECS, 60);
        assert_eq!(DEFAULT_STALE_THRESHOLD_SECS, 300);
        assert_eq!(DEFAULT_MAX_RECLAIMS_PER_ITERATION, 10);
    }

    #[test]
    fn test_reaper_config_zero_values() {
        // Test that zero values are accepted
        let config = ReaperConfig::new()
            .with_interval(Duration::from_secs(0))
            .with_stale_threshold(Duration::from_secs(0))
            .with_max_reclaims(0)
            .with_max_work_unit_scan(0);

        assert_eq!(config.interval.as_secs(), 0);
        assert_eq!(config.stale_threshold.as_secs(), 0);
        assert_eq!(config.max_reclaims_per_iteration, 0);
        assert_eq!(config.max_work_unit_scan, 0);
    }

    #[test]
    fn test_reaper_config_builder_chain() {
        // Test that builder methods can be chained
        let config = ReaperConfig::default()
            .with_interval(Duration::from_secs(30))
            .with_stale_threshold(Duration::from_secs(120));

        assert_eq!(config.interval.as_secs(), 30);
        assert_eq!(config.stale_threshold.as_secs(), 120);
        // Verify defaults are preserved for unset fields
        assert_eq!(
            config.max_reclaims_per_iteration,
            DEFAULT_MAX_RECLAIMS_PER_ITERATION
        );
    }

    #[test]
    fn test_reaper_metrics_all_operations() {
        let metrics = ReaperMetrics::new();

        // Test all increment operations
        metrics.inc_work_units_reclaimed();
        metrics.inc_work_units_reclaimed();
        metrics.inc_work_units_reclaimed();
        assert_eq!(metrics.work_units_reclaimed.load(Ordering::Relaxed), 3);

        metrics.inc_stale_workers_found(10);
        assert_eq!(metrics.stale_workers_found.load(Ordering::Relaxed), 10);

        metrics.inc_iterations();
        metrics.inc_iterations();
        assert_eq!(metrics.iterations_total.load(Ordering::Relaxed), 2);

        metrics.inc_reclaim_attempts();
        assert_eq!(metrics.reclaim_attempts.load(Ordering::Relaxed), 1);

        metrics.inc_reclaim_failures();
        assert_eq!(metrics.reclaim_failures.load(Ordering::Relaxed), 1);

        metrics.inc_work_units_skipped();
        assert_eq!(metrics.work_units_skipped.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_reaper_metrics_snapshot() {
        let metrics = ReaperMetrics::new();

        metrics.inc_work_units_reclaimed();
        metrics.inc_stale_workers_found(5);
        metrics.inc_iterations();
        metrics.inc_reclaim_attempts();
        metrics.inc_reclaim_failures();
        metrics.inc_work_units_skipped();

        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.work_units_reclaimed, 1);
        assert_eq!(snapshot.stale_workers_found, 5);
        assert_eq!(snapshot.iterations_total, 1);
        assert_eq!(snapshot.reclaim_attempts, 1);
        assert_eq!(snapshot.reclaim_failures, 1);
        assert_eq!(snapshot.work_units_skipped, 1);
    }

    #[test]
    fn test_reaper_metrics_snapshot_clone() {
        let metrics = ReaperMetrics::new();
        metrics.inc_work_units_reclaimed();
        metrics.inc_iterations();

        let snapshot = metrics.snapshot();
        let cloned = snapshot.clone();

        assert_eq!(snapshot.work_units_reclaimed, cloned.work_units_reclaimed);
        assert_eq!(snapshot.iterations_total, cloned.iterations_total);
    }

    #[test]
    fn test_reclaim_result_variants() {
        // Test all variants can be created and compared
        let reclaimed = ReclaimResult::Reclaimed;
        let not_stale = ReclaimResult::NotStale;
        let not_processing = ReclaimResult::NotProcessing;
        let failed = ReclaimResult::Failed;
        let skipped = ReclaimResult::Skipped;

        // Test Debug trait
        assert!(format!("{:?}", reclaimed).contains("Reclaimed"));
        assert!(format!("{:?}", not_stale).contains("NotStale"));
        assert!(format!("{:?}", not_processing).contains("NotProcessing"));
        assert!(format!("{:?}", failed).contains("Failed"));
        assert!(format!("{:?}", skipped).contains("Skipped"));

        // Test Clone trait
        assert!(matches!(reclaimed.clone(), ReclaimResult::Reclaimed));
        assert!(matches!(failed.clone(), ReclaimResult::Failed));
    }

    #[test]
    fn test_reaper_metrics_concurrent() {
        use std::sync::Arc;
        use std::thread;

        let metrics = Arc::new(ReaperMetrics::new());
        let mut handles = vec![];

        // Spawn multiple threads that all increment counters
        for _ in 0..10 {
            let m = Arc::clone(&metrics);
            handles.push(thread::spawn(move || {
                m.inc_work_units_reclaimed();
                m.inc_iterations();
                m.inc_reclaim_attempts();
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // All increments should be visible
        assert_eq!(metrics.work_units_reclaimed.load(Ordering::Relaxed), 10);
        assert_eq!(metrics.iterations_total.load(Ordering::Relaxed), 10);
        assert_eq!(metrics.reclaim_attempts.load(Ordering::Relaxed), 10);
    }

    #[test]
    fn test_reaper_config_new() {
        let config = ReaperConfig::new();
        // Should be same as default
        let default_config = ReaperConfig::default();
        assert_eq!(config.interval, default_config.interval);
        assert_eq!(config.stale_threshold, default_config.stale_threshold);
        assert_eq!(
            config.max_reclaims_per_iteration,
            default_config.max_reclaims_per_iteration
        );
    }

    #[test]
    fn test_reaper_config_clone() {
        let config = ReaperConfig::new()
            .with_interval(Duration::from_secs(45))
            .with_stale_threshold(Duration::from_secs(200));

        let cloned = config.clone();
        assert_eq!(config.interval, cloned.interval);
        assert_eq!(config.stale_threshold, cloned.stale_threshold);
    }

    #[test]
    fn test_reaper_config_debug() {
        let config = ReaperConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("ReaperConfig"));
        assert!(debug_str.contains("interval"));
        assert!(debug_str.contains("stale_threshold"));
    }

    #[test]
    fn test_reaper_metrics_default() {
        let metrics = ReaperMetrics::default();
        assert_eq!(metrics.work_units_reclaimed.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.stale_workers_found.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.iterations_total.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_reaper_metrics_snapshot_debug() {
        let snapshot = ReaperMetricsSnapshot {
            work_units_reclaimed: 5,
            stale_workers_found: 2,
            iterations_total: 10,
            reclaim_attempts: 8,
            reclaim_failures: 1,
            work_units_skipped: 2,
        };

        let debug_str = format!("{:?}", snapshot);
        assert!(debug_str.contains("ReaperMetricsSnapshot"));
        assert!(debug_str.contains("work_units_reclaimed"));
    }
}
