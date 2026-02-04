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
pub use controller::{
    BatchController, BatchSummary, ControllerConfig,
};
pub use spec::{
    BatchSpec, BatchMetadata, BatchJobSpec, BatchSpecError, SourceUrl,
    WorkUnitConfig, PartitionStrategy, API_VERSION, KIND_BATCH_JOB,
};
pub use status::{
    BatchStatus, BatchPhase, DiscoveryStatus, FailedWorkUnit,
};
pub use work_unit::{WorkFile, WorkUnit, WorkUnitStatus, WorkUnitError, WorkUnitSummary};
pub use key::{BatchKeys, WorkUnitKeys, BatchIndexKeys};


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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_id_from_spec() {
        let spec = BatchSpec::new("my-batch", vec!["s3://bucket/*.bag".to_string()], "output/".to_string());
        assert_eq!(batch_id_from_spec(&spec), "default:my-batch");
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
