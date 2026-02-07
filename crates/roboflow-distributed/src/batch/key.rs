// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! TiKV key builders for batch jobs.
//!
//! Extends the existing key namespace for batch job coordination.

use super::status::BatchPhase;
use crate::tikv::key::KeyBuilder;

/// Batch job keys for TiKV storage.
pub struct BatchKeys;

impl BatchKeys {
    /// Create a key for a batch spec (desired state).
    ///
    /// Format: `/roboflow/v1/batch/specs/{batch_id}`
    pub fn spec(batch_id: &str) -> Vec<u8> {
        KeyBuilder::new()
            .push("batch")
            .push("specs")
            .push(batch_id)
            .build()
    }

    /// Create a key for batch status (actual state).
    ///
    /// Format: `/roboflow/v1/batch/statuses/{batch_id}`
    pub fn status(batch_id: &str) -> Vec<u8> {
        KeyBuilder::new()
            .push("batch")
            .push("statuses")
            .push(batch_id)
            .build()
    }

    /// Create a prefix for scanning all batch specs.
    ///
    /// Format: `/roboflow/v1/batch/specs/`
    pub fn specs_prefix() -> Vec<u8> {
        KeyBuilder::new().push("batch").push("specs").build()
    }

    /// Create a prefix for scanning all batch statuses.
    ///
    /// Format: `/roboflow/v1/batch/statuses/`
    pub fn statuses_prefix() -> Vec<u8> {
        KeyBuilder::new().push("batch").push("statuses").build()
    }
}

/// Work unit keys for TiKV storage.
pub struct WorkUnitKeys;

impl WorkUnitKeys {
    /// Create a key for a work unit.
    ///
    /// Format: `/roboflow/v1/batch/workunits/{batch_id}/{unit_id}`
    pub fn unit(batch_id: &str, unit_id: &str) -> Vec<u8> {
        KeyBuilder::new()
            .push("batch")
            .push("workunits")
            .push(batch_id)
            .push(unit_id)
            .build()
    }

    /// Create a prefix for scanning all work units in a batch.
    ///
    /// Format: `/roboflow/v1/batch/workunits/{batch_id}/`
    pub fn batch_prefix(batch_id: &str) -> Vec<u8> {
        KeyBuilder::new()
            .push("batch")
            .push("workunits")
            .push(batch_id)
            .build()
    }

    /// Create a prefix for all work units.
    ///
    /// Format: `/roboflow/v1/batch/workunits/`
    pub fn prefix() -> Vec<u8> {
        KeyBuilder::new().push("batch").push("workunits").build()
    }

    /// Create a key for a pending work unit index entry.
    ///
    /// Format: `/roboflow/v1/batch/pending/{unit_id}`
    pub fn pending(unit_id: &str) -> Vec<u8> {
        KeyBuilder::new()
            .push("batch")
            .push("pending")
            .push(unit_id)
            .build()
    }

    /// Create a prefix for pending work units.
    ///
    /// Format: `/roboflow/v1/batch/pending/`
    pub fn pending_prefix() -> Vec<u8> {
        KeyBuilder::new().push("batch").push("pending").build()
    }
}

/// Batch index keys for efficient querying.
///
/// These are secondary indexes for scanning jobs by phase, priority, etc.
pub struct BatchIndexKeys;

impl BatchIndexKeys {
    /// Index jobs by phase.
    ///
    /// Format: `/roboflow/v1/batch/index/phase/{phase}/{batch_id}`
    pub fn phase(phase: BatchPhase, batch_id: &str) -> Vec<u8> {
        let phase_str = match phase {
            BatchPhase::Pending => "Pending",
            BatchPhase::Discovering => "Discovering",
            BatchPhase::Running => "Running",
            BatchPhase::Merging => "Merging",
            BatchPhase::Complete => "Complete",
            BatchPhase::Failed => "Failed",
            BatchPhase::Cancelled => "Cancelled",
            BatchPhase::Suspending => "Suspending",
            BatchPhase::Suspended => "Suspended",
        };
        KeyBuilder::new()
            .push("batch")
            .push("index")
            .push("phase")
            .push(phase_str)
            .push(batch_id)
            .build()
    }

    /// Create a prefix for scanning jobs in a specific phase.
    ///
    /// Format: `/roboflow/v1/batch/index/phase/{phase}/`
    pub fn phase_prefix(phase: BatchPhase) -> Vec<u8> {
        let phase_str = match phase {
            BatchPhase::Pending => "Pending",
            BatchPhase::Discovering => "Discovering",
            BatchPhase::Running => "Running",
            BatchPhase::Merging => "Merging",
            BatchPhase::Complete => "Complete",
            BatchPhase::Failed => "Failed",
            BatchPhase::Cancelled => "Cancelled",
            BatchPhase::Suspending => "Suspending",
            BatchPhase::Suspended => "Suspended",
        };
        KeyBuilder::new()
            .push("batch")
            .push("index")
            .push("phase")
            .push(phase_str)
            .build()
    }

    /// Index jobs by priority.
    ///
    /// Format: `/roboflow/v1/batch/index/priority/{priority}/{batch_id}`
    pub fn priority(priority: i32, batch_id: &str) -> Vec<u8> {
        KeyBuilder::new()
            .push("batch")
            .push("index")
            .push("priority")
            .push(priority.to_string())
            .push(batch_id)
            .build()
    }

    /// Create a prefix for scanning jobs by priority.
    ///
    /// Format: `/roboflow/v1/batch/index/priority/{priority}/`
    pub fn priority_prefix(priority: i32) -> Vec<u8> {
        KeyBuilder::new()
            .push("batch")
            .push("index")
            .push("priority")
            .push(priority.to_string())
            .build()
    }

    /// Index jobs by submitter.
    ///
    /// Format: `/roboflow/v1/batch/index/submitter/{submitter}/{batch_id}`
    pub fn submitter(submitter: &str, batch_id: &str) -> Vec<u8> {
        KeyBuilder::new()
            .push("batch")
            .push("index")
            .push("submitter")
            .push(submitter)
            .push(batch_id)
            .build()
    }

    /// Create a prefix for scanning jobs by submitter.
    ///
    /// Format: `/roboflow/v1/batch/index/submitter/{submitter}/`
    pub fn submitter_prefix(submitter: &str) -> Vec<u8> {
        KeyBuilder::new()
            .push("batch")
            .push("index")
            .push("submitter")
            .push(submitter)
            .build()
    }

    /// Index jobs by namespace.
    ///
    /// Format: `/roboflow/v1/batch/index/namespace/{namespace}/{batch_id}`
    pub fn namespace(namespace: &str, batch_id: &str) -> Vec<u8> {
        KeyBuilder::new()
            .push("batch")
            .push("index")
            .push("namespace")
            .push(namespace)
            .push(batch_id)
            .build()
    }

    /// Create a prefix for scanning jobs by namespace.
    ///
    /// Format: `/roboflow/v1/batch/index/namespace/{namespace}/`
    pub fn namespace_prefix(namespace: &str) -> Vec<u8> {
        KeyBuilder::new()
            .push("batch")
            .push("index")
            .push("namespace")
            .push(namespace)
            .build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_keys_spec() {
        let key = BatchKeys::spec("batch-123");
        let key_str = String::from_utf8(key).unwrap();
        assert!(key_str.contains("/batch/specs/batch-123"));
    }

    #[test]
    fn test_batch_keys_status() {
        let key = BatchKeys::status("batch-123");
        let key_str = String::from_utf8(key).unwrap();
        assert!(key_str.contains("/batch/statuses/batch-123"));
    }

    #[test]
    fn test_batch_keys_specs_prefix() {
        let prefix = BatchKeys::specs_prefix();
        let prefix_str = String::from_utf8(prefix).unwrap();
        assert!(prefix_str.contains("/batch/specs"));
    }

    #[test]
    fn test_batch_keys_statuses_prefix() {
        let prefix = BatchKeys::statuses_prefix();
        let prefix_str = String::from_utf8(prefix).unwrap();
        assert!(prefix_str.contains("/batch/statuses"));
    }

    #[test]
    fn test_work_unit_keys_unit() {
        let key = WorkUnitKeys::unit("batch-123", "unit-456");
        let key_str = String::from_utf8(key).unwrap();
        assert!(key_str.contains("/batch/workunits/batch-123/unit-456"));
    }

    #[test]
    fn test_work_unit_keys_batch_prefix() {
        let prefix = WorkUnitKeys::batch_prefix("batch-123");
        let prefix_str = String::from_utf8(prefix).unwrap();
        assert!(prefix_str.contains("/batch/workunits/batch-123"));
    }

    #[test]
    fn test_work_unit_keys_prefix() {
        let prefix = WorkUnitKeys::prefix();
        let prefix_str = String::from_utf8(prefix).unwrap();
        assert!(prefix_str.contains("/batch/workunits"));
    }

    #[test]
    fn test_work_unit_keys_pending() {
        let key = WorkUnitKeys::pending("unit-456");
        let key_str = String::from_utf8(key).unwrap();
        assert!(key_str.contains("/batch/pending/unit-456"));
    }

    #[test]
    fn test_work_unit_keys_pending_prefix() {
        let prefix = WorkUnitKeys::pending_prefix();
        let prefix_str = String::from_utf8(prefix).unwrap();
        assert!(prefix_str.contains("/batch/pending"));
    }

    #[test]
    fn test_batch_index_keys_phase() {
        let key = BatchIndexKeys::phase(BatchPhase::Running, "batch-123");
        let key_str = String::from_utf8(key).unwrap();
        assert!(key_str.contains("/batch/index/phase/Running/batch-123"));
    }

    #[test]
    fn test_batch_index_keys_phase_prefix() {
        let prefix = BatchIndexKeys::phase_prefix(BatchPhase::Running);
        let prefix_str = String::from_utf8(prefix).unwrap();
        assert!(prefix_str.contains("/batch/index/phase/Running"));
    }

    #[test]
    fn test_batch_index_keys_priority() {
        let key = BatchIndexKeys::priority(10, "batch-123");
        let key_str = String::from_utf8(key).unwrap();
        assert!(key_str.contains("/batch/index/priority/10/batch-123"));
    }

    #[test]
    fn test_batch_index_keys_priority_prefix() {
        let prefix = BatchIndexKeys::priority_prefix(10);
        let prefix_str = String::from_utf8(prefix).unwrap();
        assert!(prefix_str.contains("/batch/index/priority/10"));
    }

    #[test]
    fn test_batch_index_keys_submitter() {
        let key = BatchIndexKeys::submitter("user1", "batch-123");
        let key_str = String::from_utf8(key).unwrap();
        assert!(key_str.contains("/batch/index/submitter/user1/batch-123"));
    }

    #[test]
    fn test_batch_index_keys_namespace() {
        let key = BatchIndexKeys::namespace("production", "batch-123");
        let key_str = String::from_utf8(key).unwrap();
        assert!(key_str.contains("/batch/index/namespace/production/batch-123"));
    }
}
