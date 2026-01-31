// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Schema types for distributed coordination.
//!
//! Defines the data structures stored in TiKV for job tracking,
//! distributed locking, checkpointing, and heartbeats.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Job record stored in TiKV.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobRecord {
    /// Unique identifier (UUID).
    pub id: String,

    /// Source object key in S3/OSS.
    pub source_key: String,

    /// Source bucket name.
    pub source_bucket: String,

    /// Source file size in bytes.
    pub source_size: u64,

    /// Current job status.
    pub status: JobStatus,

    /// Owner pod ID when Processing.
    pub owner: Option<String>,

    /// Number of processing attempts.
    pub attempts: u32,

    /// Maximum allowed attempts.
    pub max_attempts: u32,

    /// Creation timestamp.
    pub created_at: DateTime<Utc>,

    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,

    /// Error message if failed.
    pub error: Option<String>,

    /// Output prefix for processed data.
    pub output_prefix: String,

    /// Hash of configuration used for this job.
    pub config_hash: String,
}

impl JobRecord {
    /// Create a new job record.
    pub fn new(
        id: String,
        source_key: String,
        source_bucket: String,
        source_size: u64,
        output_prefix: String,
        config_hash: String,
    ) -> Self {
        let now = Utc::now();
        Self {
            id,
            source_key,
            source_bucket,
            source_size,
            status: JobStatus::Pending,
            owner: None,
            attempts: 0,
            max_attempts: 3,
            created_at: now,
            updated_at: now,
            error: None,
            output_prefix,
            config_hash,
        }
    }

    /// Check if this job can be claimed.
    pub fn is_claimable(&self) -> bool {
        matches!(self.status, JobStatus::Pending | JobStatus::Failed)
            && self.attempts < self.max_attempts
    }

    /// Check if this job is terminal (completed or dead).
    pub fn is_terminal(&self) -> bool {
        matches!(self.status, JobStatus::Completed | JobStatus::Dead)
    }

    /// Mark this job as claimed by a pod.
    pub fn claim(&mut self, pod_id: String) -> Result<(), String> {
        if !self.is_claimable() {
            return Err(format!("Job is not claimable: {:?}", self.status));
        }
        self.status = JobStatus::Processing;
        self.owner = Some(pod_id);
        self.attempts += 1;
        self.updated_at = Utc::now();
        Ok(())
    }

    /// Mark this job as completed.
    pub fn complete(&mut self) {
        self.status = JobStatus::Completed;
        self.owner = None;
        self.updated_at = Utc::now();
    }

    /// Mark this job as failed.
    pub fn fail(&mut self, error: String) {
        self.status = if self.attempts >= self.max_attempts {
            JobStatus::Dead
        } else {
            JobStatus::Failed
        };
        self.owner = None;
        self.error = Some(error);
        self.updated_at = Utc::now();
    }
}

/// Job status enum.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum JobStatus {
    /// Job is pending assignment.
    Pending,

    /// Job is being processed.
    Processing,

    /// Job completed successfully.
    Completed,

    /// Job failed but may be retried.
    Failed,

    /// Job failed permanently (max attempts exceeded).
    Dead,
}

impl JobStatus {
    /// Check if this status indicates the job is actively being processed.
    pub fn is_active(&self) -> bool {
        matches!(self, JobStatus::Processing)
    }

    /// Check if this status is a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(self, JobStatus::Completed | JobStatus::Dead)
    }

    /// Check if this status indicates failure.
    pub fn is_failed(&self) -> bool {
        matches!(self, JobStatus::Failed | JobStatus::Dead)
    }
}

/// Distributed lock record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LockRecord {
    /// Resource being locked.
    pub resource: String,

    /// Owner of the lock (pod ID).
    pub owner: String,

    /// Lock version for CAS operations.
    pub version: u64,

    /// Expiration timestamp.
    pub expires_at: DateTime<Utc>,

    /// When the lock was acquired.
    pub acquired_at: DateTime<Utc>,
}

impl LockRecord {
    /// Create a new lock record.
    pub fn new(resource: String, owner: String, ttl_seconds: i64) -> Self {
        let now = Utc::now();
        Self {
            resource,
            owner,
            version: 1,
            expires_at: now + chrono::Duration::seconds(ttl_seconds),
            acquired_at: now,
        }
    }

    /// Check if this lock is expired.
    pub fn is_expired(&self) -> bool {
        Utc::now() > self.expires_at
    }

    /// Check if this lock is expired with a grace period.
    ///
    /// The grace period helps avoid race conditions during the expiration boundary.
    /// A lock is considered "not expired" if it's within the grace period, even
    /// if the expiration time has technically passed.
    pub fn is_expired_with_grace(&self, grace_seconds: i64) -> bool {
        Utc::now() > (self.expires_at - chrono::Duration::seconds(grace_seconds))
    }

    /// Extend the lock TTL.
    pub fn extend(&mut self, ttl_seconds: i64) {
        self.expires_at = Utc::now() + chrono::Duration::seconds(ttl_seconds);
        self.version += 1;
    }

    /// Verify ownership of the lock.
    pub fn is_owned_by(&self, pod_id: &str) -> bool {
        self.owner == pod_id
    }

    /// Get the fencing token (version) for this lock.
    ///
    /// Fencing tokens are used to detect and prevent split-brain scenarios
    /// where two processes believe they own the same lock. Higher tokens
    /// always win.
    pub fn fencing_token(&self) -> u64 {
        self.version
    }
}

/// Worker heartbeat record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeartbeatRecord {
    /// Pod ID of the worker.
    pub pod_id: String,

    /// Last heartbeat timestamp.
    pub last_heartbeat: DateTime<Utc>,

    /// Worker status.
    pub status: WorkerStatus,

    /// Number of jobs currently processing.
    pub active_jobs: u32,

    /// Total jobs processed by this worker.
    pub total_processed: u64,

    /// Worker capabilities.
    pub capabilities: Vec<String>,

    /// Optional worker metadata.
    pub metadata: Option<serde_json::Value>,
}

impl HeartbeatRecord {
    /// Create a new heartbeat record.
    pub fn new(pod_id: String) -> Self {
        Self {
            pod_id,
            last_heartbeat: Utc::now(),
            status: WorkerStatus::Idle,
            active_jobs: 0,
            total_processed: 0,
            capabilities: Vec::new(),
            metadata: None,
        }
    }

    /// Update the heartbeat timestamp.
    pub fn beat(&mut self) {
        self.last_heartbeat = Utc::now();
    }

    /// Check if this heartbeat is stale (older than timeout seconds).
    pub fn is_stale(&self, timeout_seconds: i64) -> bool {
        let timeout = chrono::Duration::seconds(timeout_seconds);
        Utc::now().signed_duration_since(self.last_heartbeat) > timeout
    }

    /// Increment active job count.
    pub fn increment_active(&mut self) {
        self.active_jobs += 1;
        self.status = WorkerStatus::Busy;
    }

    /// Decrement active job count.
    pub fn decrement_active(&mut self) {
        if self.active_jobs > 0 {
            self.active_jobs -= 1;
        }
        if self.active_jobs == 0 {
            self.status = WorkerStatus::Idle;
        }
    }

    /// Increment total processed count.
    pub fn increment_processed(&mut self) {
        self.total_processed += 1;
    }
}

/// Worker status enum.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum WorkerStatus {
    /// Worker is idle and available for work.
    Idle,

    /// Worker is processing jobs.
    Busy,

    /// Worker is draining (shutting down).
    Draining,

    /// Worker is unhealthy.
    Unhealthy,
}

/// Checkpoint state for frame-level progress tracking.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointState {
    /// File hash being processed.
    pub file_hash: String,

    /// Processing pod ID.
    pub pod_id: String,

    /// Last successfully processed frame index.
    pub last_frame: u64,

    /// Total frames in the file.
    pub total_frames: u64,

    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,

    /// Processing state version.
    pub version: u64,
}

impl CheckpointState {
    /// Create a new checkpoint state.
    pub fn new(file_hash: String, pod_id: String, total_frames: u64) -> Self {
        Self {
            file_hash,
            pod_id,
            last_frame: 0,
            total_frames,
            updated_at: Utc::now(),
            version: 1,
        }
    }

    /// Update the checkpoint with a new frame index.
    pub fn update(&mut self, frame: u64) -> Result<(), String> {
        if frame > self.total_frames {
            return Err(format!(
                "Frame {} exceeds total frames {}",
                frame, self.total_frames
            ));
        }
        self.last_frame = frame;
        self.updated_at = Utc::now();
        self.version += 1;
        Ok(())
    }

    /// Calculate progress as a percentage.
    pub fn progress_percent(&self) -> f64 {
        if self.total_frames == 0 {
            0.0
        } else {
            (self.last_frame as f64 / self.total_frames as f64) * 100.0
        }
    }

    /// Check if processing is complete.
    pub fn is_complete(&self) -> bool {
        self.last_frame >= self.total_frames
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_record_new() {
        let job = JobRecord::new(
            "test-id".to_string(),
            "test-key".to_string(),
            "test-bucket".to_string(),
            1024,
            "output/".to_string(),
            "config-hash".to_string(),
        );
        assert_eq!(job.status, JobStatus::Pending);
        assert_eq!(job.attempts, 0);
        assert!(job.owner.is_none());
    }

    #[test]
    fn test_job_record_claim() {
        let mut job = JobRecord::new(
            "test-id".to_string(),
            "test-key".to_string(),
            "test-bucket".to_string(),
            1024,
            "output/".to_string(),
            "config-hash".to_string(),
        );
        assert!(job.claim("pod-1".to_string()).is_ok());
        assert_eq!(job.status, JobStatus::Processing);
        assert_eq!(job.owner, Some("pod-1".to_string()));
        assert_eq!(job.attempts, 1);
    }

    #[test]
    fn test_job_record_complete() {
        let mut job = JobRecord::new(
            "test-id".to_string(),
            "test-key".to_string(),
            "test-bucket".to_string(),
            1024,
            "output/".to_string(),
            "config-hash".to_string(),
        );
        job.claim("pod-1".to_string()).unwrap();
        job.complete();
        assert_eq!(job.status, JobStatus::Completed);
        assert!(job.owner.is_none());
    }

    #[test]
    fn test_job_record_fail() {
        let mut job = JobRecord::new(
            "test-id".to_string(),
            "test-key".to_string(),
            "test-bucket".to_string(),
            1024,
            "output/".to_string(),
            "config-hash".to_string(),
        );
        job.max_attempts = 2;
        job.claim("pod-1".to_string()).unwrap();
        job.fail("test error".to_string());
        assert_eq!(job.status, JobStatus::Failed);
        assert_eq!(job.error, Some("test error".to_string()));

        // Second failure should mark as dead
        job.claim("pod-2".to_string()).unwrap();
        job.fail("test error".to_string());
        assert_eq!(job.status, JobStatus::Dead);
    }

    #[test]
    fn test_job_status() {
        assert!(JobStatus::Processing.is_active());
        assert!(!JobStatus::Pending.is_active());

        assert!(JobStatus::Completed.is_terminal());
        assert!(JobStatus::Dead.is_terminal());
        assert!(!JobStatus::Pending.is_terminal());

        assert!(JobStatus::Failed.is_failed());
        assert!(JobStatus::Dead.is_failed());
        assert!(!JobStatus::Pending.is_failed());
    }

    #[test]
    fn test_lock_record() {
        let mut lock = LockRecord::new("resource".to_string(), "pod-1".to_string(), 60);
        assert_eq!(lock.version, 1);
        assert!(!lock.is_expired());
        assert!(lock.is_owned_by("pod-1"));
        assert!(!lock.is_owned_by("pod-2"));

        lock.extend(60);
        assert_eq!(lock.version, 2);
    }

    #[test]
    fn test_lock_grace_period() {
        let lock = LockRecord::new("resource".to_string(), "pod-1".to_string(), 10); // 10 second TTL
        // Lock should NOT be expired normally
        assert!(!lock.is_expired());
        // Lock should NOT be expired even with a small grace period
        assert!(!lock.is_expired_with_grace(5));
        assert_eq!(lock.fencing_token(), 1);
    }

    #[test]
    fn test_heartbeat_record() {
        let mut heartbeat = HeartbeatRecord::new("pod-1".to_string());
        assert_eq!(heartbeat.status, WorkerStatus::Idle);
        assert_eq!(heartbeat.active_jobs, 0);

        heartbeat.increment_active();
        assert_eq!(heartbeat.status, WorkerStatus::Busy);
        assert_eq!(heartbeat.active_jobs, 1);

        heartbeat.increment_processed();
        assert_eq!(heartbeat.total_processed, 1);

        heartbeat.decrement_active();
        assert_eq!(heartbeat.status, WorkerStatus::Idle);
        assert_eq!(heartbeat.active_jobs, 0);
    }

    #[test]
    fn test_checkpoint_state() {
        let mut checkpoint = CheckpointState::new("hash".to_string(), "pod-1".to_string(), 100);
        assert_eq!(checkpoint.last_frame, 0);
        assert_eq!(checkpoint.progress_percent(), 0.0);

        checkpoint.update(50).unwrap();
        assert_eq!(checkpoint.last_frame, 50);
        assert_eq!(checkpoint.progress_percent(), 50.0);
        assert!(!checkpoint.is_complete());

        checkpoint.update(100).unwrap();
        assert!(checkpoint.is_complete());

        // Exceeding total frames should fail
        assert!(checkpoint.update(101).is_err());
    }
}
