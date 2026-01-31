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

/// Configuration for TiKV catalog connection.
#[cfg(feature = "tikv-catalog")]
pub mod config;

/// TiKV client pool and connection management.
#[cfg(feature = "tikv-catalog")]
pub mod pool;

/// Key encoding and decoding for TiKV storage.
#[cfg(feature = "tikv-catalog")]
pub mod key;

/// Schema types for catalog metadata.
#[cfg(feature = "tikv-catalog")]
pub mod schema;

/// Main catalog implementation.
#[cfg(feature = "tikv-catalog")]
pub mod catalog;

// Re-exports when feature is enabled
#[cfg(feature = "tikv-catalog")]
pub use catalog::TiKVCatalog;
#[cfg(feature = "tikv-catalog")]
pub use config::TiKVConfig;
#[cfg(feature = "tikv-catalog")]
pub use schema::{EpisodeMetadata, SegmentMetaData, UploadStatus};

/// Default PD endpoints for local development.
pub const DEFAULT_PD_ENDPOINTS: &str = "127.0.0.1:2379";

/// Default connection timeout in seconds.
pub const DEFAULT_CONNECTION_TIMEOUT_SECS: u64 = 10;
