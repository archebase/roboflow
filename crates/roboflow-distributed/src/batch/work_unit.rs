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

    /// Persisted episode index allocation for retry-safe processing.
    #[serde(default)]
    pub episode_index: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyWorkUnit {
    pub id: String,
    pub batch_id: String,
    pub files: Vec<WorkFile>,
    pub output_path: String,
    pub config_hash: String,
    pub status: WorkUnitStatus,
    pub owner: Option<String>,
    pub attempts: u32,
    pub max_attempts: u32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub error: Option<String>,
    pub priority: i32,
}

impl From<LegacyWorkUnit> for WorkUnit {
    fn from(legacy: LegacyWorkUnit) -> Self {
        Self {
            id: legacy.id,
            batch_id: legacy.batch_id,
            files: legacy.files,
            output_path: legacy.output_path,
            config_hash: legacy.config_hash,
            status: legacy.status,
            owner: legacy.owner,
            attempts: legacy.attempts,
            max_attempts: legacy.max_attempts,
            created_at: legacy.created_at,
            updated_at: legacy.updated_at,
            error: legacy.error,
            priority: legacy.priority,
            episode_index: None,
        }
    }
}

/// Deserialize a work unit with backward compatibility.
///
/// Tries the current `WorkUnit` layout first, then falls back to the legacy
/// pre-`episode_index` layout.
pub fn deserialize_work_unit_compat(data: &[u8]) -> Result<WorkUnit, bincode::Error> {
    match bincode::deserialize::<WorkUnit>(data) {
        Ok(unit) => Ok(unit),
        Err(_) => bincode::deserialize::<LegacyWorkUnit>(data).map(WorkUnit::from),
    }
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
            episode_index: None,
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
            episode_index: None,
        }
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
    fn test_deserialize_work_unit_compat_with_current_payload() {
        let mut unit = WorkUnit::new(
            "batch-123".to_string(),
            vec![WorkFile::new("s3://bucket/file.mcap".to_string(), 1024)],
            "s3://output/".to_string(),
            "config-hash".to_string(),
        );
        unit.episode_index = Some(7);

        let serialized = bincode::serialize(&unit).unwrap();
        let decoded = deserialize_work_unit_compat(&serialized).unwrap();

        assert_eq!(decoded.id, unit.id);
        assert_eq!(decoded.batch_id, unit.batch_id);
        assert_eq!(decoded.episode_index, Some(7));
    }

    #[test]
    fn test_deserialize_work_unit_compat_with_legacy_payload() {
        let unit = WorkUnit::new(
            "batch-legacy".to_string(),
            vec![WorkFile::new("s3://bucket/legacy.mcap".to_string(), 2048)],
            "s3://output/".to_string(),
            "legacy-config".to_string(),
        );

        let legacy = LegacyWorkUnit {
            id: unit.id.clone(),
            batch_id: unit.batch_id.clone(),
            files: unit.files.clone(),
            output_path: unit.output_path.clone(),
            config_hash: unit.config_hash.clone(),
            status: unit.status,
            owner: unit.owner.clone(),
            attempts: unit.attempts,
            max_attempts: unit.max_attempts,
            created_at: unit.created_at,
            updated_at: unit.updated_at,
            error: unit.error.clone(),
            priority: unit.priority,
        };

        let serialized = bincode::serialize(&legacy).unwrap();
        let decoded = deserialize_work_unit_compat(&serialized).unwrap();

        assert_eq!(decoded.id, unit.id);
        assert_eq!(decoded.batch_id, unit.batch_id);
        assert_eq!(decoded.status, unit.status);
        assert_eq!(decoded.episode_index, None);
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
    fn test_work_unit_status_display() {
        assert_eq!(format!("{}", WorkUnitStatus::Pending), "Pending");
        assert_eq!(format!("{}", WorkUnitStatus::Processing), "Processing");
        assert_eq!(format!("{}", WorkUnitStatus::Complete), "Complete");
        assert_eq!(format!("{}", WorkUnitStatus::Failed), "Failed");
        assert_eq!(format!("{}", WorkUnitStatus::Dead), "Dead");
        assert_eq!(format!("{}", WorkUnitStatus::Cancelled), "Cancelled");
    }

    #[test]
    fn test_work_unit_status_can_transition_to() {
        use crate::state::StateLifecycle;

        // Pending can transition to Processing, Failed, Cancelled
        assert!(WorkUnitStatus::Pending.can_transition_to(&WorkUnitStatus::Processing));
        assert!(WorkUnitStatus::Pending.can_transition_to(&WorkUnitStatus::Failed));
        assert!(WorkUnitStatus::Pending.can_transition_to(&WorkUnitStatus::Cancelled));
        assert!(!WorkUnitStatus::Pending.can_transition_to(&WorkUnitStatus::Complete));
        assert!(!WorkUnitStatus::Pending.can_transition_to(&WorkUnitStatus::Dead));

        // Processing can transition to Complete, Failed, Dead, Cancelled
        assert!(WorkUnitStatus::Processing.can_transition_to(&WorkUnitStatus::Complete));
        assert!(WorkUnitStatus::Processing.can_transition_to(&WorkUnitStatus::Failed));
        assert!(WorkUnitStatus::Processing.can_transition_to(&WorkUnitStatus::Dead));
        assert!(WorkUnitStatus::Processing.can_transition_to(&WorkUnitStatus::Cancelled));
        assert!(!WorkUnitStatus::Processing.can_transition_to(&WorkUnitStatus::Pending));

        // Failed can transition to Processing, Cancelled
        assert!(WorkUnitStatus::Failed.can_transition_to(&WorkUnitStatus::Processing));
        assert!(WorkUnitStatus::Failed.can_transition_to(&WorkUnitStatus::Cancelled));
        assert!(!WorkUnitStatus::Failed.can_transition_to(&WorkUnitStatus::Complete));

        // Terminal states cannot transition
        assert!(!WorkUnitStatus::Complete.can_transition_to(&WorkUnitStatus::Pending));
        assert!(!WorkUnitStatus::Dead.can_transition_to(&WorkUnitStatus::Pending));
        assert!(!WorkUnitStatus::Cancelled.can_transition_to(&WorkUnitStatus::Pending));

        // Self-transition is always allowed
        assert!(WorkUnitStatus::Pending.can_transition_to(&WorkUnitStatus::Pending));
        assert!(WorkUnitStatus::Processing.can_transition_to(&WorkUnitStatus::Processing));
        assert!(WorkUnitStatus::Complete.can_transition_to(&WorkUnitStatus::Complete));
    }

    #[test]
    fn test_work_unit_status_is_claimable() {
        assert!(WorkUnitStatus::Pending.is_claimable());
        assert!(WorkUnitStatus::Failed.is_claimable());
        assert!(!WorkUnitStatus::Processing.is_claimable());
        assert!(!WorkUnitStatus::Complete.is_claimable());
        assert!(!WorkUnitStatus::Dead.is_claimable());
        assert!(!WorkUnitStatus::Cancelled.is_claimable());
    }

    #[test]
    fn test_work_unit_claim_max_attempts_exceeded() {
        let mut unit = WorkUnit::new(
            "batch-123".to_string(),
            vec![WorkFile::new("s3://bucket/file.mcap".to_string(), 1024)],
            "s3://output/".to_string(),
            "config-hash".to_string(),
        );

        unit.attempts = 3; // Already at max
        unit.max_attempts = 3;

        // When attempts >= max_attempts, is_claimable() returns false
        // so we get NotClaimable error
        let result = unit.claim("worker-1".to_string());
        assert!(result.is_err());
        assert!(!unit.is_claimable());
    }

    #[test]
    fn test_work_unit_primary_source() {
        let files = vec![
            WorkFile::new("s3://bucket/file1.mcap".to_string(), 1000),
            WorkFile::new("s3://bucket/file2.mcap".to_string(), 2000),
        ];
        let unit = WorkUnit::new(
            "batch-123".to_string(),
            files,
            "s3://output/".to_string(),
            "config-hash".to_string(),
        );

        assert_eq!(unit.primary_source(), Some("s3://bucket/file1.mcap"));
    }

    #[test]
    fn test_work_unit_primary_source_empty() {
        let unit = WorkUnit::new(
            "batch-123".to_string(),
            vec![],
            "s3://output/".to_string(),
            "config-hash".to_string(),
        );

        assert_eq!(unit.primary_source(), None);
    }

    #[test]
    fn test_work_unit_is_single_file() {
        let single = WorkUnit::new(
            "batch-123".to_string(),
            vec![WorkFile::new("s3://bucket/file.mcap".to_string(), 1024)],
            "s3://output/".to_string(),
            "config-hash".to_string(),
        );
        assert!(single.is_single_file());

        let multiple = WorkUnit::new(
            "batch-123".to_string(),
            vec![
                WorkFile::new("s3://bucket/file1.mcap".to_string(), 1024),
                WorkFile::new("s3://bucket/file2.mcap".to_string(), 2048),
            ],
            "s3://output/".to_string(),
            "config-hash".to_string(),
        );
        assert!(!multiple.is_single_file());
    }

    #[test]
    fn test_work_unit_error_display() {
        let err1 = WorkUnitError::NotClaimable {
            id: "unit-123".to_string(),
            status: WorkUnitStatus::Processing,
        };
        assert!(err1.to_string().contains("unit-123"));
        assert!(err1.to_string().contains("not claimable"));

        let err2 = WorkUnitError::MaxAttemptsExceeded {
            id: "unit-456".to_string(),
            max_attempts: 3,
        };
        assert!(err2.to_string().contains("unit-456"));
        assert!(err2.to_string().contains("max attempts"));

        let err3 = WorkUnitError::Serialization("invalid data".to_string());
        assert!(err3.to_string().contains("invalid data"));
    }

    #[test]
    fn test_work_unit_claim_attempts_increment() {
        let mut unit = WorkUnit::new(
            "batch-123".to_string(),
            vec![WorkFile::new("s3://bucket/file.mcap".to_string(), 1024)],
            "s3://output/".to_string(),
            "config-hash".to_string(),
        );

        assert_eq!(unit.attempts, 0);

        unit.claim("worker-1".to_string()).unwrap();
        assert_eq!(unit.attempts, 1);

        unit.fail("error".to_string());
        assert_eq!(unit.status, WorkUnitStatus::Failed);

        unit.claim("worker-2".to_string()).unwrap();
        assert_eq!(unit.attempts, 2);
    }

    #[test]
    fn test_work_unit_not_claimable_after_complete() {
        let mut unit = WorkUnit::new(
            "batch-123".to_string(),
            vec![WorkFile::new("s3://bucket/file.mcap".to_string(), 1024)],
            "s3://output/".to_string(),
            "config-hash".to_string(),
        );

        unit.complete();
        let result = unit.claim("worker-1".to_string());
        assert!(result.is_err());
        match result {
            Err(WorkUnitError::NotClaimable { status, .. }) => {
                assert_eq!(status, WorkUnitStatus::Complete);
            }
            _ => panic!("Expected NotClaimable error"),
        }
    }

    #[test]
    fn test_work_unit_retry_preserves_episode_index() {
        let mut unit = WorkUnit::new(
            "batch-123".to_string(),
            vec![WorkFile::new("s3://bucket/file.mcap".to_string(), 1024)],
            "s3://output/".to_string(),
            "config-hash".to_string(),
        );
        unit.episode_index = Some(42);

        unit.claim("worker-1".to_string()).unwrap();
        unit.fail("transient error".to_string());
        unit.claim("worker-2".to_string()).unwrap();

        assert_eq!(unit.episode_index, Some(42));
        assert_eq!(unit.attempts, 2);
        assert_eq!(unit.status, WorkUnitStatus::Processing);
    }
}
