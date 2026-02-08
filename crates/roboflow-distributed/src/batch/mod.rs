// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Batch job processing module.
//!
//! This module provides a Kubernetes-inspired declarative batch system for
//! processing large numbers of files (10,000+) in a distributed manner.
//!
//! ## Architecture
//!
//! The batch system follows a declarative pattern where users submit a
//! `BatchSpec` (desired state) and a controller reconciles the actual state
//! to match:
//!
//! 1. **BatchSpec** - User's desired state (YAML/JSON)
//! 2. **BatchStatus** - Current actual state with phases
//! 3. **WorkUnit** - Individual work items claimed by workers
//! 4. **Controller** - Reconciliation loop that drives progress
//!
//! ## Phases
//!
//! - `Pending` - Initial state, validation
//! - `Discovering` - Scanning storage for files
//! - `Running` - Workers processing work units
//! - `Complete` - All work units finished successfully
//! - `Failed` - Backoff limit exceeded
//! - `Cancelled` - User cancelled the job
//!
//! ## Example
//!
//! ```yaml
//! # batch.yaml
//! apiVersion: roboflow/v1
//! kind: BatchJob
//! metadata:
//!   name: "bag-conversion-20250130"
//! spec:
//!   sources:
//!     - url: "s3://bucket/path/*.bag"
//!   output: "s3://bucket/output/"
//!   config: "default"
//!   parallelism: 100
//!   backoffLimit: 10
//! ```
//!
//! ```bash
//! # Submit batch
//! roboflow batch submit batch.yaml
//!
//! # Check status
//! roboflow batch status <batch-id>
//! ```

mod controller;
mod key;
mod spec;
mod status;
mod work_unit;

// Re-export public types
pub use controller::{BatchController, BatchSummary, ControllerConfig};
pub use key::{BatchIndexKeys, BatchKeys, WorkUnitKeys};
pub use spec::{
    API_VERSION, BatchJobSpec, BatchMetadata, BatchSpec, BatchSpecError, KIND_BATCH_JOB,
    PartitionStrategy, SourceUrl, WorkUnitConfig,
};
pub use status::{BatchPhase, BatchStatus, DiscoveryStatus, FailedWorkUnit};
pub use work_unit::{WorkFile, WorkUnit, WorkUnitError, WorkUnitStatus, WorkUnitSummary};

/// Create a batch ID from a spec.
///
/// Uses namespace:name format for uniqueness.
pub fn batch_id_from_spec(spec: &BatchSpec) -> String {
    format!("{}:{}", spec.metadata.namespace, spec.metadata.name)
}

/// Generate a unique batch ID.
pub fn generate_batch_id() -> String {
    format!("batch:{}", uuid::Uuid::new_v4())
}

/// Check if a batch phase is terminal.
pub fn is_phase_terminal(phase: BatchPhase) -> bool {
    phase.is_terminal()
}

/// Check if a batch phase is active.
pub fn is_phase_active(phase: BatchPhase) -> bool {
    phase.is_active()
}

/// Update the phase index in TiKV during a batch phase transition.
///
/// Writes the new phase index key first, then deletes the old one.
/// Safe under crash: stale index keys are tolerated because consumers
/// verify actual status after index lookup.
///
/// ## Future: Full Scheduler Architecture
///
/// The phase index is a stepping stone toward a full SchedulerService that would:
///
/// - **Priority queue**: In-memory priority queue with fair scheduling across
///   namespaces, avoiding starvation of low-priority batches
/// - **Admission control**: Rate-limit batch submissions, enforce quotas per
///   namespace/submitter, reject when cluster is overloaded
/// - **Push-based dispatch**: Watch TiKV via CDC (Change Data Capture) instead
///   of polling, eliminating scan intervals entirely
/// - **Preemption**: Higher-priority batches can preempt lower-priority running
///   work units (with checkpointing support)
/// - **Backpressure**: Coordinate with workers to throttle discovery when the
///   pending queue is deep, preventing memory pressure
/// - **Observability**: Expose queue depth, wait times, throughput per namespace
///   as Prometheus metrics for capacity planning
///
/// The phase index design (secondary index per phase) naturally extends to
/// support these features — priority scheduling adds a composite key
/// (phase + priority + timestamp), admission control checks index counts,
/// and CDC watches the index prefixes for changes.
pub async fn update_phase_index(
    tikv: &crate::tikv::TikvClient,
    batch_id: &str,
    old_phase: BatchPhase,
    new_phase: BatchPhase,
) -> Result<(), crate::tikv::TikvError> {
    if old_phase == new_phase {
        return Ok(());
    }

    // Write new index key first (write-new-before-delete-old pattern)
    let new_key = BatchIndexKeys::phase(new_phase, batch_id);
    tikv.put(new_key, vec![]).await?;

    // Delete old index key
    let old_key = BatchIndexKeys::phase(old_phase, batch_id);
    tikv.delete(old_key).await?;

    tracing::debug!(
        batch_id = %batch_id,
        old_phase = ?old_phase,
        new_phase = ?new_phase,
        "Phase index updated"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_id_from_spec() {
        let spec = BatchSpec::new(
            "my-batch",
            vec!["s3://bucket/*.bag".to_string()],
            "output/".to_string(),
        );
        assert_eq!(batch_id_from_spec(&spec), "jobs:my-batch");
    }

    #[test]
    fn test_generate_batch_id() {
        let id = generate_batch_id();
        assert!(id.starts_with("batch:"));
        // UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        assert!(id.len() > "batch:".len());
    }

    #[test]
    fn test_is_phase_terminal() {
        assert!(is_phase_terminal(BatchPhase::Complete));
        assert!(is_phase_terminal(BatchPhase::Failed));
        assert!(is_phase_terminal(BatchPhase::Cancelled));
        assert!(!is_phase_terminal(BatchPhase::Pending));
        assert!(!is_phase_terminal(BatchPhase::Discovering));
        assert!(!is_phase_terminal(BatchPhase::Running));
    }

    #[test]
    fn test_is_phase_active() {
        assert!(is_phase_active(BatchPhase::Discovering));
        assert!(is_phase_active(BatchPhase::Running));
        assert!(!is_phase_active(BatchPhase::Pending));
        assert!(!is_phase_active(BatchPhase::Complete));
        assert!(!is_phase_active(BatchPhase::Failed));
    }
}
