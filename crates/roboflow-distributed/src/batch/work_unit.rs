// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Work unit schema.
//!
//! A work unit represents one or more files to be processed by a worker.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Work unit for batch processing.
///
/// Work units are claimed by workers and processed in parallel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkUnit {
    /// Unique work unit ID (UUID).
    pub id: String,

    /// Batch job ID this work unit belongs to.
    pub batch_id: String,

    /// Source files to process (can be multiple for grouped work).
    pub files: Vec<WorkFile>,

    /// Output path for this work unit.
    pub output_path: String,

    /// Configuration hash for processing.
    pub config_hash: String,

    /// Current status.
    pub status: WorkUnitStatus,

    /// Worker ID that claimed this unit (if processing).
    pub owner: Option<String>,

    /// Number of processing attempts.
    #[serde(default)]
    pub attempts: u32,

    /// Maximum allowed attempts.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,

    /// Creation timestamp.
    #[serde(default = "Utc::now")]
    pub created_at: DateTime<Utc>,

    /// Last update timestamp.
    #[serde(default = "Utc::now")]
    pub updated_at: DateTime<Utc>,

    /// Error message if failed.
    pub error: Option<String>,

    /// Priority (inherited from batch, can be overridden).
    #[serde(default)]
    pub priority: i32,

    /// Number of episodes per chunk for LeRobot v2.1 format.
    ///
    /// When set (non-zero), this work unit will be assigned a unique
    /// episode index via centralized allocation. The chunk directory
    /// is calculated as: chunk_index = episode_index / episodes_per_chunk
    ///
    /// Default is 0 (disabled - no episode allocation).
    #[serde(default)]
    pub episodes_per_chunk: u32,
}

fn default_max_attempts() -> u32 {
    3
}

/// A single file in a work unit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkFile {
    /// Source URL for the file.
    pub url: String,

    /// File size in bytes.
    pub size: u64,

    /// Optional file modification time.
    pub modified_at: Option<DateTime<Utc>>,

    /// Optional checksum for validation.
    pub checksum: Option<String>,
}

/// Work unit status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum WorkUnitStatus {
    /// Work unit is pending (not yet claimed).
    #[serde(rename = "Pending")]
    Pending,

    /// Work unit is being processed.
    #[serde(rename = "Processing")]
    Processing,

    /// Work unit completed successfully.
    #[serde(rename = "Complete")]
    Complete,

    /// Work unit failed (may be retried).
    #[serde(rename = "Failed")]
    Failed,

    /// Work unit exceeded max attempts.
    #[serde(rename = "Dead")]
    Dead,

    /// Work unit was cancelled.
    #[serde(rename = "Cancelled")]
    Cancelled,
}

impl WorkUnitStatus {
    /// Check if status is terminal.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Complete | Self::Dead | Self::Cancelled)
    }

    /// Check if work unit can be claimed.
    pub fn is_claimable(&self) -> bool {
        matches!(self, Self::Pending | Self::Failed)
    }
}

impl fmt::Display for WorkUnitStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => write!(f, "Pending"),
            Self::Processing => write!(f, "Processing"),
            Self::Complete => write!(f, "Complete"),
            Self::Failed => write!(f, "Failed"),
            Self::Dead => write!(f, "Dead"),
            Self::Cancelled => write!(f, "Cancelled"),
        }
    }
}

impl crate::state::StateLifecycle for WorkUnitStatus {
    fn is_terminal(&self) -> bool {
        matches!(self, Self::Complete | Self::Dead | Self::Cancelled)
    }

    fn is_claimable(&self) -> bool {
        matches!(self, Self::Pending | Self::Failed)
    }

    fn can_transition_to(&self, target: &Self) -> bool {
        // Self-transition is always allowed (idempotent)
        if self == target {
            return true;
        }

        match self {
            WorkUnitStatus::Pending => matches!(
                target,
                WorkUnitStatus::Processing | WorkUnitStatus::Failed | WorkUnitStatus::Cancelled
            ),
            WorkUnitStatus::Processing => matches!(
                target,
                WorkUnitStatus::Complete
                    | WorkUnitStatus::Failed
                    | WorkUnitStatus::Dead
                    | WorkUnitStatus::Cancelled
            ),
            WorkUnitStatus::Failed => {
                matches!(
                    target,
                    WorkUnitStatus::Processing | WorkUnitStatus::Cancelled
                )
            }
            // Terminal states cannot transition
            WorkUnitStatus::Complete | WorkUnitStatus::Dead | WorkUnitStatus::Cancelled => false,
        }
    }
}

impl WorkUnit {
    /// Create a new work unit.
    pub fn new(
        batch_id: String,
        files: Vec<WorkFile>,
        output_path: String,
        config_hash: String,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            batch_id,
            files,
            output_path,
            config_hash,
            status: WorkUnitStatus::Pending,
            owner: None,
            attempts: 0,
            max_attempts: default_max_attempts(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            error: None,
            priority: 0,
            episodes_per_chunk: 0,
        }
    }

    /// Create a new work unit with a specific ID.
    pub fn with_id(
        id: String,
        batch_id: String,
        files: Vec<WorkFile>,
        output_path: String,
        config_hash: String,
    ) -> Self {
        Self {
            id,
            batch_id,
            files,
            output_path,
            config_hash,
            status: WorkUnitStatus::Pending,
            owner: None,
            attempts: 0,
            max_attempts: default_max_attempts(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            error: None,
            priority: 0,
            episodes_per_chunk: 0,
        }
    }

    /// Set the episodes per chunk for LeRobot v2.1 format.
    ///
    /// When set to a non-zero value, this work unit will be assigned
    /// a unique episode index via centralized TiKV allocation.
    pub fn with_episodes_per_chunk(mut self, episodes: u32) -> Self {
        self.episodes_per_chunk = episodes;
        self
    }

    /// Check if episode allocation is enabled for this work unit.
    pub fn has_episode_allocation(&self) -> bool {
        self.episodes_per_chunk > 0
    }

    /// Try to claim this work unit.
    pub fn claim(&mut self, worker_id: String) -> Result<(), WorkUnitError> {
        if !self.is_claimable() {
            return Err(WorkUnitError::NotClaimable {
                status: self.status,
                id: self.id.clone(),
            });
        }
        if self.attempts >= self.max_attempts {
            return Err(WorkUnitError::MaxAttemptsExceeded {
                id: self.id.clone(),
                max_attempts: self.max_attempts,
            });
        }

        self.status = WorkUnitStatus::Processing;
        self.owner = Some(worker_id);
        self.attempts += 1;
        self.updated_at = Utc::now();
        Ok(())
    }

    /// Mark work unit as complete.
    pub fn complete(&mut self) {
        self.status = WorkUnitStatus::Complete;
        self.owner = None;
        self.updated_at = Utc::now();
    }

    /// Mark work unit as failed.
    pub fn fail(&mut self, error: String) {
        self.status = if self.attempts >= self.max_attempts {
            WorkUnitStatus::Dead
        } else {
            WorkUnitStatus::Failed
        };
        self.owner = None;
        self.error = Some(error);
        self.updated_at = Utc::now();
    }

    /// Mark work unit as cancelled.
    pub fn cancel(&mut self) {
        self.status = WorkUnitStatus::Cancelled;
        self.owner = None;
        self.updated_at = Utc::now();
    }

    /// Check if work unit can be claimed.
    pub fn is_claimable(&self) -> bool {
        self.status.is_claimable() && self.attempts < self.max_attempts
    }

    /// Get total size of all files in bytes.
    pub fn total_size(&self) -> u64 {
        self.files.iter().map(|f| f.size).sum()
    }

    /// Get the number of files in this work unit.
    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    /// Check if this work unit is for a single file.
    pub fn is_single_file(&self) -> bool {
        self.files.len() == 1
    }

    /// Get the primary source URL (first file).
    pub fn primary_source(&self) -> Option<&str> {
        self.files.first().map(|f| f.url.as_str())
    }

    /// Create a summary for display.
    pub fn summary(&self) -> WorkUnitSummary {
        WorkUnitSummary {
            id: self.id.clone(),
            batch_id: self.batch_id.clone(),
            status: self.status,
            file_count: self.files.len(),
            total_size: self.total_size(),
            attempts: self.attempts,
        }
    }
}

/// Summary of a work unit for display.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkUnitSummary {
    /// Unique work unit identifier.
    pub id: String,
    /// Parent batch identifier.
    pub batch_id: String,
    /// Current status of the work unit.
    pub status: WorkUnitStatus,
    /// Number of files in this work unit.
    pub file_count: usize,
    /// Total size of all files in bytes.
    pub total_size: u64,
    /// Number of processing attempts.
    pub attempts: u32,
}

/// Errors related to work units.
#[derive(Debug, thiserror::Error)]
pub enum WorkUnitError {
    /// Work unit cannot be claimed in its current state.
    #[error("work unit {id} is not claimable (status: {status:?})")]
    NotClaimable {
        /// Work unit identifier.
        id: String,
        /// Current status preventing claim.
        status: WorkUnitStatus,
    },

    /// Work unit has been retried too many times.
    #[error("work unit {id} exceeded max attempts ({max_attempts})")]
    MaxAttemptsExceeded {
        /// Work unit identifier.
        id: String,
        /// Maximum attempts allowed.
        max_attempts: u32,
    },

    /// Serialization or deserialization failed.
    #[error("work unit serialization error: {0}")]
    Serialization(String),
}

impl WorkFile {
    /// Create a new work file.
    pub fn new(url: String, size: u64) -> Self {
        Self {
            url,
            size,
            modified_at: None,
            checksum: None,
        }
    }

    /// Create a new work file with modification time.
    pub fn with_modified_time(mut self, modified_at: DateTime<Utc>) -> Self {
        self.modified_at = Some(modified_at);
        self
    }

    /// Create a new work file with checksum.
    pub fn with_checksum(mut self, checksum: String) -> Self {
        self.checksum = Some(checksum);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_work_unit_new() {
        let files = vec![WorkFile::new("s3://bucket/file.mcap".to_string(), 1024)];
        let unit = WorkUnit::new(
            "batch-123".to_string(),
            files,
            "s3://output/".to_string(),
            "config-hash".to_string(),
        );

        assert_eq!(unit.batch_id, "batch-123");
        assert_eq!(unit.files.len(), 1);
        assert_eq!(unit.status, WorkUnitStatus::Pending);
        assert!(unit.owner.is_none());
        assert_eq!(unit.attempts, 0);
    }

    #[test]
    fn test_work_unit_claim() {
        let mut unit = WorkUnit::new(
            "batch-123".to_string(),
            vec![WorkFile::new("s3://bucket/file.mcap".to_string(), 1024)],
            "s3://output/".to_string(),
            "config-hash".to_string(),
        );

        assert!(unit.claim("worker-1".to_string()).is_ok());
        assert_eq!(unit.status, WorkUnitStatus::Processing);
        assert_eq!(unit.owner, Some("worker-1".to_string()));
        assert_eq!(unit.attempts, 1);
    }

    #[test]
    fn test_work_unit_claim_fails_when_not_claimable() {
        let mut unit = WorkUnit::new(
            "batch-123".to_string(),
            vec![WorkFile::new("s3://bucket/file.mcap".to_string(), 1024)],
            "s3://output/".to_string(),
            "config-hash".to_string(),
        );

        unit.status = WorkUnitStatus::Processing;
        let result = unit.claim("worker-1".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_work_unit_complete() {
        let mut unit = WorkUnit::new(
            "batch-123".to_string(),
            vec![WorkFile::new("s3://bucket/file.mcap".to_string(), 1024)],
            "s3://output/".to_string(),
            "config-hash".to_string(),
        );

        unit.complete();
        assert_eq!(unit.status, WorkUnitStatus::Complete);
        assert!(unit.owner.is_none());
    }

    #[test]
    fn test_work_unit_fail_retryable() {
        let mut unit = WorkUnit::new(
            "batch-123".to_string(),
            vec![WorkFile::new("s3://bucket/file.mcap".to_string(), 1024)],
            "s3://output/".to_string(),
            "config-hash".to_string(),
        );

        unit.attempts = 1;
        unit.max_attempts = 3;
        unit.fail("temporary error".to_string());

        assert_eq!(unit.status, WorkUnitStatus::Failed);
        assert!(unit.error.is_some());
    }

    #[test]
    fn test_work_unit_fail_dead() {
        let mut unit = WorkUnit::new(
            "batch-123".to_string(),
            vec![WorkFile::new("s3://bucket/file.mcap".to_string(), 1024)],
            "s3://output/".to_string(),
            "config-hash".to_string(),
        );

        unit.attempts = 3;
        unit.max_attempts = 3;
        unit.fail("permanent error".to_string());

        assert_eq!(unit.status, WorkUnitStatus::Dead);
    }

    #[test]
    fn test_work_unit_cancel() {
        let mut unit = WorkUnit::new(
            "batch-123".to_string(),
            vec![WorkFile::new("s3://bucket/file.mcap".to_string(), 1024)],
            "s3://output/".to_string(),
            "config-hash".to_string(),
        );

        unit.cancel();
        assert_eq!(unit.status, WorkUnitStatus::Cancelled);
        assert!(unit.owner.is_none());
    }

    #[test]
    fn test_work_unit_is_claimable() {
        let mut unit = WorkUnit::new(
            "batch-123".to_string(),
            vec![WorkFile::new("s3://bucket/file.mcap".to_string(), 1024)],
            "s3://output/".to_string(),
            "config-hash".to_string(),
        );

        assert!(unit.is_claimable());

        unit.status = WorkUnitStatus::Processing;
        assert!(!unit.is_claimable());

        unit.status = WorkUnitStatus::Failed;
        assert!(unit.is_claimable());

        unit.status = WorkUnitStatus::Complete;
        assert!(!unit.is_claimable());
    }

    #[test]
    fn test_work_unit_status_is_terminal() {
        assert!(WorkUnitStatus::Complete.is_terminal());
        assert!(WorkUnitStatus::Dead.is_terminal());
        assert!(WorkUnitStatus::Cancelled.is_terminal());
        assert!(!WorkUnitStatus::Pending.is_terminal());
        assert!(!WorkUnitStatus::Processing.is_terminal());
        assert!(!WorkUnitStatus::Failed.is_terminal());
    }

    #[test]
    fn test_work_unit_total_size() {
        let files = vec![
            WorkFile::new("file1.mcap".to_string(), 1000),
            WorkFile::new("file2.mcap".to_string(), 2000),
            WorkFile::new("file3.mcap".to_string(), 3000),
        ];
        let unit = WorkUnit::new(
            "batch-123".to_string(),
            files,
            "s3://output/".to_string(),
            "config-hash".to_string(),
        );

        assert_eq!(unit.total_size(), 6000);
    }

    #[test]
    fn test_work_unit_file_count() {
        let files = vec![
            WorkFile::new("file1.mcap".to_string(), 1000),
            WorkFile::new("file2.mcap".to_string(), 2000),
        ];
        let unit = WorkUnit::new(
            "batch-123".to_string(),
            files,
            "s3://output/".to_string(),
            "config-hash".to_string(),
        );

        assert_eq!(unit.file_count(), 2);
        assert!(!unit.is_single_file());
    }

    #[test]
    fn test_work_unit_with_id() {
        let files = vec![WorkFile::new("s3://bucket/file.mcap".to_string(), 1024)];
        let unit = WorkUnit::with_id(
            "custom-unit-id".to_string(),
            "batch-123".to_string(),
            files,
            "s3://output/".to_string(),
            "config-hash".to_string(),
        );

        assert_eq!(unit.id, "custom-unit-id");
    }

    #[test]
    fn test_work_file_builder_methods() {
        let file = WorkFile::new("s3://bucket/file.mcap".to_string(), 1024)
            .with_modified_time(Utc::now())
            .with_checksum("abc123".to_string());

        assert_eq!(file.url, "s3://bucket/file.mcap");
        assert_eq!(file.size, 1024);
        assert!(file.modified_at.is_some());
        assert_eq!(file.checksum.as_deref(), Some("abc123"));
    }

    #[test]
    fn test_work_unit_serialization() {
        let unit = WorkUnit::new(
            "batch-123".to_string(),
            vec![WorkFile::new("s3://bucket/file.mcap".to_string(), 1024)],
            "s3://output/".to_string(),
            "config-hash".to_string(),
        );

        let serialized = bincode::serialize(&unit).unwrap();
        let deserialized: WorkUnit = bincode::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.batch_id, unit.batch_id);
        assert_eq!(deserialized.files.len(), unit.files.len());
        assert_eq!(deserialized.status, unit.status);
    }

    #[test]
    fn test_work_unit_summary() {
        let files = vec![
            WorkFile::new("file1.mcap".to_string(), 1000),
            WorkFile::new("file2.mcap".to_string(), 2000),
        ];
        let unit = WorkUnit::new(
            "batch-123".to_string(),
            files,
            "s3://output/".to_string(),
            "config-hash".to_string(),
        );

        let summary = unit.summary();
        assert_eq!(summary.file_count, 2);
        assert_eq!(summary.total_size, 3000);
    }

    #[test]
    fn test_work_unit_episodes_per_chunk() {
        let mut unit = WorkUnit::new(
            "batch-123".to_string(),
            vec![WorkFile::new("s3://bucket/file.mcap".to_string(), 1024)],
            "s3://output/".to_string(),
            "config-hash".to_string(),
        );

        // Default is 0 (disabled)
        assert_eq!(unit.episodes_per_chunk, 0);
        assert!(!unit.has_episode_allocation());

        // Set episodes per chunk
        unit = unit.with_episodes_per_chunk(500);
        assert_eq!(unit.episodes_per_chunk, 500);
        assert!(unit.has_episode_allocation());
    }

    #[test]
    fn test_work_unit_episodes_serialization() {
        let unit = WorkUnit::new(
            "batch-123".to_string(),
            vec![WorkFile::new("s3://bucket/file.mcap".to_string(), 1024)],
            "s3://output/".to_string(),
            "config-hash".to_string(),
        )
        .with_episodes_per_chunk(250);

        let serialized = bincode::serialize(&unit).unwrap();
        let deserialized: WorkUnit = bincode::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.episodes_per_chunk, 250);
        assert!(deserialized.has_episode_allocation());
    }
}
