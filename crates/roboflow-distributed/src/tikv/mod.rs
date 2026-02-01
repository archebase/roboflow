// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! TiKV client wrapper for distributed coordination.
//!
//! Provides connection pooling and basic CRUD operations for TiKV.

pub mod checkpoint;
pub mod circuit;
pub mod client;
pub mod config;
pub mod error;
pub mod key;
pub mod locks;
pub mod schema;

pub use checkpoint::{
    CheckpointConfig, CheckpointManager, DEFAULT_CHECKPOINT_INTERVAL_FRAMES,
    DEFAULT_CHECKPOINT_INTERVAL_SECS,
};
pub use circuit::{CircuitBreaker, CircuitConfig, CircuitState};
pub use client::TikvClient;
pub use config::TikvConfig;
pub use error::TikvError;
pub use locks::{LockGuard, LockManager, LockManagerConfig};
pub use schema::{
    CheckpointState, HeartbeatRecord, JobRecord, JobStatus, LockRecord, ParquetUploadState,
    UploadedPart, VideoUploadState, WorkerStatus,
};
