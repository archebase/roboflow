// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Distributed coordination with TiKV backend.
//!
//! This module provides distributed coordination primitives for robolow:
//! - Job queue and tracking
//! - Distributed locks
//! - Checkpoint state management
//! - Worker heartbeats
//!
//! # Feature
//!
//! This module is gated behind the `distributed` feature flag.

#[cfg(feature = "distributed")]
pub mod tikv;

// Re-exports when distributed feature is enabled
#[cfg(feature = "distributed")]
pub use tikv::{
    CheckpointState, HeartbeatRecord, JobRecord, JobStatus, LockRecord, TikvClient, TikvConfig,
    TikvError, WorkerStatus,
};

/// Default key prefix for all roboflow data in TiKV.
pub const KEY_PREFIX: &str = "/roboflow/v1/";

/// Default PD endpoints for local development.
pub const DEFAULT_PD_ENDPOINTS: &str = "127.0.0.1:2379";

/// Default connection timeout in seconds.
pub const DEFAULT_CONNECTION_TIMEOUT_SECS: u64 = 10;
