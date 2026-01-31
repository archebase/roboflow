// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Distributed coordination with TiKV backend.
//!
//! This module provides distributed coordination primitives for roboflow:
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

// Re-export circuit breaker types
#[cfg(feature = "distributed")]
pub use tikv::{CircuitBreaker, CircuitConfig, CircuitState};

// Re-export constants from config module
#[cfg(feature = "distributed")]
pub use tikv::config::{DEFAULT_CONNECTION_TIMEOUT_SECS, DEFAULT_PD_ENDPOINTS, KEY_PREFIX};
