// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! TiKV-based catalog for distributed metadata storage.
//!
//! This module provides a TiKV-backed catalog for storing:
//! - Episode metadata
//! - Segment metadata
//! - Upload progress and state
//!
//! # Features
//!
//! - Distributed metadata storage with TiKV
//! - Crash recovery for upload operations
//! - Atomic updates with version checking
//! - Integration with the storage layer for S3/MinIO
//!
//! ## Note
//!
//! This module is always available as part of the distributed processing
//! functionality. TiKV coordination is a core feature of roboflow.

/// Configuration for TiKV catalog connection.
pub mod config;

/// TiKV client pool and connection management.
pub mod pool;

/// Key encoding and decoding for TiKV storage.
pub mod key;

/// Schema types for catalog metadata.
pub mod schema;

/// Main catalog implementation.
pub mod catalog;

// Re-exports
pub use catalog::TiKVCatalog;
pub use config::TiKVConfig;
pub use schema::{EpisodeMetadata, SegmentMetaData, UploadStatus};

/// Default PD endpoints for local development.
/// Uses host.docker.internal to work with Docker Desktop on macOS/Windows
/// and Docker with host-gateway on Linux.
pub const DEFAULT_PD_ENDPOINTS: &str = "host.docker.internal:2379";

/// Default connection timeout in seconds.
pub const DEFAULT_CONNECTION_TIMEOUT_SECS: u64 = 10;
