// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! # roboflow-distributed
//!
//! Distributed coordination for roboflow.
//!
//! This crate provides coordination primitives for distributed dataset processing:
//! - **TiKV backend** - Production coordination with strong consistency
//! - **Catalog** - Metadata storage for episodes and uploads
//! - **Coordinator traits** - Unified abstraction
//!
//! ## Design Philosophy
//!
//! TiKV is the production coordination layer for distributed workloads.
//! All coordination features are **always available** (no feature flags).

pub mod catalog;
pub mod scanner;
pub mod tikv;

// Re-export public types from tikv (distributed coordination)
pub use tikv::{
    CheckpointState, CircuitBreaker, CircuitConfig, CircuitState, HeartbeatRecord, JobRecord,
    JobStatus, LockGuard, LockManager, LockManagerConfig, LockRecord, TikvClient, TikvConfig,
    TikvError, WorkerStatus,
};

// Re-export public types from catalog (metadata storage)
pub use catalog::{EpisodeMetadata, SegmentMetaData, TiKVCatalog, TiKVConfig, UploadStatus};

// Re-export public types from scanner (file discovery actor)
pub use scanner::{MetricsSnapshot, ScanStats, Scanner, ScannerConfig, ScannerMetrics};

// Re-export constants from tikv config
pub use tikv::config::{DEFAULT_CONNECTION_TIMEOUT_SECS, DEFAULT_PD_ENDPOINTS, KEY_PREFIX};

// =============================================================================
// Coordinator Traits
// =============================================================================

use std::time::Duration;

use roboflow_core::Result;

/// Coordinator trait for distributed job coordination.
pub trait Coordinator: Send + Sync {
    // Job operations
    fn claim_job(&self, file_hash: &str, pod_id: &str) -> Result<Option<JobRecord>>;
    fn complete_job(&self, file_hash: &str) -> Result<()>;
    fn fail_job(&self, file_hash: &str, error: &str) -> Result<()>;

    // Lock operations
    fn acquire_lock(&self, resource: &str, owner: &str, ttl: Duration) -> Result<bool>;
    fn release_lock(&self, resource: &str, owner: &str) -> Result<bool>;

    // Heartbeat
    fn heartbeat(&self, pod_id: &str, status: &WorkerStatus) -> Result<()>;

    // Checkpoint
    fn save_checkpoint(&self, state: &CheckpointState) -> Result<()>;
    fn load_checkpoint(&self, file_hash: &str) -> Result<Option<CheckpointState>>;
}

/// Catalog trait for episode metadata.
pub trait Catalog: Send + Sync {
    // Episode metadata
    fn save_episode(&self, metadata: &EpisodeMetadata) -> Result<()>;
    fn get_episode(&self, id: &str) -> Result<Option<EpisodeMetadata>>;
    fn list_episodes(&self, prefix: &str) -> Result<Vec<EpisodeMetadata>>;

    // Upload tracking
    fn start_upload(&self, id: &str) -> Result<()>;
    fn complete_upload(&self, id: &str) -> Result<()>;
    fn get_upload_status(&self, id: &str) -> Result<UploadStatus>;
}
