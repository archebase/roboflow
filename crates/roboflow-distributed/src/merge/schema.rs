// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Merge state schema for Staging + Merge coordination.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Merge state for a distributed conversion job.
///
/// Tracks the progress of merging staged outputs from multiple workers
/// into a single sequential LeRobot dataset.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MergeState {
    /// Job ID being merged.
    pub job_id: String,

    /// Current merge status.
    pub status: MergeStatus,

    /// Number of workers expected to contribute to this job.
    pub expected_workers: usize,

    /// Number of workers that have completed staging.
    pub completed_workers: usize,

    /// Staging paths from each worker.
    ///
    /// Maps worker_id -> staging_prefix (e.g., "staging/{job_id}/worker_1")
    pub staging_paths: HashMap<String, String>,

    /// Total number of frames staged across all workers.
    pub total_frames: u64,

    /// Number of frames merged so far.
    pub merged_frames: u64,

    /// Output path for the final merged dataset.
    pub output_path: String,

    /// Creation timestamp.
    pub created_at: DateTime<Utc>,

    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,

    /// Error message if merge failed.
    pub error: Option<String>,

    /// Worker ID performing the merge (None if not yet assigned).
    pub merge_worker: Option<String>,
}

/// Merge status.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MergeStatus {
    /// Merge is pending (waiting for workers to complete staging).
    Pending,

    /// Merge is in progress.
    InProgress,

    /// Merge completed successfully.
    Complete,

    /// Merge failed.
    Failed,
}

/// Result of a merge operation.
#[derive(Debug, Clone)]
pub enum MergeResult {
    /// Merge completed successfully.
    Success {
        /// Output path of the merged dataset.
        output_path: String,
        /// Total number of frames merged.
        total_frames: u64,
    },

    /// Merge failed.
    Failed {
        /// Error message.
        error: String,
    },
}

impl MergeState {
    /// Create a new merge state.
    pub fn new(job_id: String, expected_workers: usize, output_path: String) -> Self {
        let now = Utc::now();
        Self {
            job_id,
            status: MergeStatus::Pending,
            expected_workers,
            completed_workers: 0,
            staging_paths: HashMap::new(),
            total_frames: 0,
            merged_frames: 0,
            output_path,
            created_at: now,
            updated_at: now,
            error: None,
            merge_worker: None,
        }
    }

    /// Check if merge is ready to start (all workers complete).
    pub fn is_ready(&self) -> bool {
        self.completed_workers >= self.expected_workers && self.status == MergeStatus::Pending
    }

    /// Check if merge is complete.
    pub fn is_complete(&self) -> bool {
        self.status == MergeStatus::Complete
    }

    /// Check if merge failed.
    pub fn is_failed(&self) -> bool {
        self.status == MergeStatus::Failed
    }

    /// Mark a worker as complete with its staging path.
    pub fn add_worker(&mut self, worker_id: String, staging_path: String, frame_count: u64) {
        self.staging_paths.insert(worker_id, staging_path);
        self.completed_workers = self.staging_paths.len();
        self.total_frames += frame_count;
        self.updated_at = Utc::now();
    }

    /// Start the merge process.
    pub fn start_merge(&mut self, merge_worker: String) -> Result<(), String> {
        if !self.is_ready() {
            return Err("Cannot start merge: not all workers complete".to_string());
        }
        self.status = MergeStatus::InProgress;
        self.merge_worker = Some(merge_worker);
        self.updated_at = Utc::now();
        Ok(())
    }

    /// Complete the merge.
    pub fn complete(&mut self) {
        self.status = MergeStatus::Complete;
        self.updated_at = Utc::now();
    }

    /// Fail the merge with an error message.
    pub fn fail(&mut self, error: String) {
        self.status = MergeStatus::Failed;
        self.error = Some(error);
        self.updated_at = Utc::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_state_new() {
        let state = MergeState::new("job-1".to_string(), 3, "/output/dataset".to_string());

        assert_eq!(state.job_id, "job-1");
        assert_eq!(state.expected_workers, 3);
        assert_eq!(state.status, MergeStatus::Pending);
        assert_eq!(state.completed_workers, 0);
        assert!(!state.is_ready());
    }

    #[test]
    fn test_merge_state_add_workers() {
        let mut state = MergeState::new("job-1".to_string(), 2, "/output/dataset".to_string());

        state.add_worker("worker-1".to_string(), "staging/job-1/w1".to_string(), 100);
        assert_eq!(state.completed_workers, 1);
        assert_eq!(state.total_frames, 100);
        assert!(!state.is_ready());

        state.add_worker("worker-2".to_string(), "staging/job-1/w2".to_string(), 150);
        assert_eq!(state.completed_workers, 2);
        assert_eq!(state.total_frames, 250);
        assert!(state.is_ready());
    }

    #[test]
    fn test_merge_state_start() {
        let mut state = MergeState::new("job-1".to_string(), 1, "/output/dataset".to_string());

        state.add_worker("worker-1".to_string(), "staging/job-1/w1".to_string(), 100);

        let result = state.start_merge("merge-worker".to_string());
        assert!(result.is_ok());
        assert_eq!(state.status, MergeStatus::InProgress);
        assert_eq!(state.merge_worker, Some("merge-worker".to_string()));
    }

    #[test]
    fn test_merge_state_start_not_ready() {
        let mut state = MergeState::new("job-1".to_string(), 2, "/output/dataset".to_string());

        let result = state.start_merge("merge-worker".to_string());
        assert!(result.is_err());
    }
}
