// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Metrics for distributed dataset metadata operations.
//!
//! This module provides counters and timers for tracking registry
//! operations, useful for monitoring and debugging.

use std::sync::atomic::{AtomicU64, Ordering};

/// Metrics for metadata registry operations.
#[derive(Debug)]
pub struct MetadataMetrics {
    /// Number of tasks registered.
    pub tasks_registered: AtomicU64,

    /// Number of tasks found in cache (deduplicated).
    pub tasks_deduplicated: AtomicU64,

    /// Number of features registered.
    pub features_registered: AtomicU64,

    /// Number of feature validation failures.
    pub feature_validation_failures: AtomicU64,

    /// Number of episodes registered.
    pub episodes_registered: AtomicU64,

    /// Number of TiKV read operations.
    pub tikv_reads: AtomicU64,

    /// Number of TiKV write operations.
    pub tikv_writes: AtomicU64,

    /// Number of TiKV CAS retries.
    pub tikv_cas_retries: AtomicU64,

    /// Total time spent in task registration (ms).
    pub task_registration_time_ms: AtomicU64,

    /// Total time spent in feature registration (ms).
    pub feature_registration_time_ms: AtomicU64,

    /// Total time spent in episode registration (ms).
    pub episode_registration_time_ms: AtomicU64,
}

impl Default for MetadataMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl MetadataMetrics {
    /// Create new metrics.
    pub fn new() -> Self {
        Self {
            tasks_registered: AtomicU64::new(0),
            tasks_deduplicated: AtomicU64::new(0),
            features_registered: AtomicU64::new(0),
            feature_validation_failures: AtomicU64::new(0),
            episodes_registered: AtomicU64::new(0),
            tikv_reads: AtomicU64::new(0),
            tikv_writes: AtomicU64::new(0),
            tikv_cas_retries: AtomicU64::new(0),
            task_registration_time_ms: AtomicU64::new(0),
            feature_registration_time_ms: AtomicU64::new(0),
            episode_registration_time_ms: AtomicU64::new(0),
        }
    }

    /// Increment tasks registered.
    pub fn inc_tasks_registered(&self) {
        self.tasks_registered.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment tasks deduplicated.
    pub fn inc_tasks_deduplicated(&self) {
        self.tasks_deduplicated.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment features registered.
    pub fn inc_features_registered(&self) {
        self.features_registered.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment feature validation failures.
    pub fn inc_feature_validation_failures(&self) {
        self.feature_validation_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Increment episodes registered.
    pub fn inc_episodes_registered(&self) {
        self.episodes_registered.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment TiKV reads.
    pub fn inc_tikv_reads(&self, count: u64) {
        self.tikv_reads.fetch_add(count, Ordering::Relaxed);
    }

    /// Increment TiKV writes.
    pub fn inc_tikv_writes(&self, count: u64) {
        self.tikv_writes.fetch_add(count, Ordering::Relaxed);
    }

    /// Increment TiKV CAS retries.
    pub fn inc_tikv_cas_retries(&self) {
        self.tikv_cas_retries.fetch_add(1, Ordering::Relaxed);
    }

    /// Add task registration time.
    pub fn add_task_registration_time(&self, ms: u64) {
        self.task_registration_time_ms
            .fetch_add(ms, Ordering::Relaxed);
    }

    /// Add feature registration time.
    pub fn add_feature_registration_time(&self, ms: u64) {
        self.feature_registration_time_ms
            .fetch_add(ms, Ordering::Relaxed);
    }

    /// Add episode registration time.
    pub fn add_episode_registration_time(&self, ms: u64) {
        self.episode_registration_time_ms
            .fetch_add(ms, Ordering::Relaxed);
    }

    /// Get snapshot of current metrics.
    pub fn snapshot(&self) -> MetadataMetricsSnapshot {
        MetadataMetricsSnapshot {
            tasks_registered: self.tasks_registered.load(Ordering::Relaxed),
            tasks_deduplicated: self.tasks_deduplicated.load(Ordering::Relaxed),
            features_registered: self.features_registered.load(Ordering::Relaxed),
            feature_validation_failures: self.feature_validation_failures.load(Ordering::Relaxed),
            episodes_registered: self.episodes_registered.load(Ordering::Relaxed),
            tikv_reads: self.tikv_reads.load(Ordering::Relaxed),
            tikv_writes: self.tikv_writes.load(Ordering::Relaxed),
            tikv_cas_retries: self.tikv_cas_retries.load(Ordering::Relaxed),
            task_registration_time_ms: self.task_registration_time_ms.load(Ordering::Relaxed),
            feature_registration_time_ms: self.feature_registration_time_ms.load(Ordering::Relaxed),
            episode_registration_time_ms: self.episode_registration_time_ms.load(Ordering::Relaxed),
        }
    }

    /// Reset all metrics.
    pub fn reset(&self) {
        self.tasks_registered.store(0, Ordering::Relaxed);
        self.tasks_deduplicated.store(0, Ordering::Relaxed);
        self.features_registered.store(0, Ordering::Relaxed);
        self.feature_validation_failures.store(0, Ordering::Relaxed);
        self.episodes_registered.store(0, Ordering::Relaxed);
        self.tikv_reads.store(0, Ordering::Relaxed);
        self.tikv_writes.store(0, Ordering::Relaxed);
        self.tikv_cas_retries.store(0, Ordering::Relaxed);
        self.task_registration_time_ms.store(0, Ordering::Relaxed);
        self.feature_registration_time_ms
            .store(0, Ordering::Relaxed);
        self.episode_registration_time_ms
            .store(0, Ordering::Relaxed);
    }
}

/// Snapshot of metadata metrics.
#[derive(Debug, Clone, Copy)]
pub struct MetadataMetricsSnapshot {
    pub tasks_registered: u64,
    pub tasks_deduplicated: u64,
    pub features_registered: u64,
    pub feature_validation_failures: u64,
    pub episodes_registered: u64,
    pub tikv_reads: u64,
    pub tikv_writes: u64,
    pub tikv_cas_retries: u64,
    pub task_registration_time_ms: u64,
    pub feature_registration_time_ms: u64,
    pub episode_registration_time_ms: u64,
}

impl MetadataMetricsSnapshot {
    /// Calculate average task registration time (ms).
    pub fn avg_task_registration_time_ms(&self) -> f64 {
        if self.tasks_registered > 0 {
            self.task_registration_time_ms as f64 / self.tasks_registered as f64
        } else {
            0.0
        }
    }

    /// Calculate deduplication rate.
    pub fn deduplication_rate(&self) -> f64 {
        let total = self.tasks_registered + self.tasks_deduplicated;
        if total > 0 {
            self.tasks_deduplicated as f64 / total as f64
        } else {
            0.0
        }
    }

    /// Print formatted metrics.
    pub fn print(&self) {
        println!("Metadata Registry Metrics:");
        println!("  Tasks:");
        println!("    Registered: {}", self.tasks_registered);
        println!("    Deduplicated: {}", self.tasks_deduplicated);
        println!(
            "    Deduplication rate: {:.1}%",
            self.deduplication_rate() * 100.0
        );
        println!(
            "    Avg registration time: {:.2}ms",
            self.avg_task_registration_time_ms()
        );
        println!("  Features:");
        println!("    Registered: {}", self.features_registered);
        println!(
            "    Validation failures: {}",
            self.feature_validation_failures
        );
        println!("  Episodes:");
        println!("    Registered: {}", self.episodes_registered);
        println!("  TiKV Operations:");
        println!("    Reads: {}", self.tikv_reads);
        println!("    Writes: {}", self.tikv_writes);
        println!("    CAS retries: {}", self.tikv_cas_retries);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_counters() {
        let metrics = MetadataMetrics::new();

        metrics.inc_tasks_registered();
        metrics.inc_tasks_registered();
        metrics.inc_tasks_deduplicated();
        metrics.inc_features_registered();
        metrics.inc_episodes_registered();
        metrics.inc_tikv_reads(5);
        metrics.inc_tikv_writes(3);
        metrics.inc_tikv_cas_retries();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.tasks_registered, 2);
        assert_eq!(snapshot.tasks_deduplicated, 1);
        assert_eq!(snapshot.features_registered, 1);
        assert_eq!(snapshot.episodes_registered, 1);
        assert_eq!(snapshot.tikv_reads, 5);
        assert_eq!(snapshot.tikv_writes, 3);
        assert_eq!(snapshot.tikv_cas_retries, 1);
    }

    #[test]
    fn test_deduplication_rate() {
        let metrics = MetadataMetrics::new();
        metrics.inc_tasks_registered();
        metrics.inc_tasks_registered();
        metrics.inc_tasks_deduplicated();

        let snapshot = metrics.snapshot();
        assert!((snapshot.deduplication_rate() - 0.333).abs() < 0.01);
    }

    #[test]
    fn test_avg_time() {
        let metrics = MetadataMetrics::new();
        metrics.inc_tasks_registered();
        metrics.inc_tasks_registered();
        metrics.add_task_registration_time(100);
        metrics.add_task_registration_time(200);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.avg_task_registration_time_ms(), 150.0);
    }

    #[test]
    fn test_reset() {
        let metrics = MetadataMetrics::new();
        metrics.inc_tasks_registered();
        metrics.inc_features_registered();

        metrics.reset();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.tasks_registered, 0);
        assert_eq!(snapshot.features_registered, 0);
    }
}
