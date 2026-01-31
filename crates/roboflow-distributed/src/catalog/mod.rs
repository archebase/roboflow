// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! TiKV-based catalog for distributed metadata storage.
//!
//! This module provides a catalog implementation for storing and retrieving
//! episode and segment metadata, as well as tracking upload progress.

mod config;
mod key;
mod pool;
mod schema;

// Main catalog implementation
mod catalog_impl;

// Re-export main catalog implementation
pub use catalog_impl::TiKVCatalog;

// Re-export configuration types
pub use config::TiKVConfig;

// Re-export schema types
pub use schema::{EpisodeMetadata, SegmentMetaData, UploadState, UploadStatus};

// Key types are internal-only
