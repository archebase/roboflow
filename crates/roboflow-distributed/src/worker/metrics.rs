// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Worker metrics and processing results.

use std::sync::atomic::{AtomicU64, Ordering};

/// Processing result for a job.
pub enum ProcessingResult {
    /// Job completed successfully.
    Success,
    /// Job failed with retryable error.
    Failed { error: String },
    /// Job was cancelled by user request.
    Cancelled,
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
