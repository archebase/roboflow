// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Async storage abstraction for use in async contexts.
//!
//! This module provides a clean async storage interface that doesn't require
//! creating nested Tokio runtimes. Use this trait in async code (workers, scanners)
//! instead of the sync `Storage` trait which is meant for blocking contexts.

use crate::{ObjectMetadata, StorageResult as Result};
use bytes::Bytes;
use std::path::Path;

/// Core async storage abstraction.
///
/// This trait provides an asynchronous interface for storage operations,
/// designed for use in async contexts (tokio runtimes). Unlike the sync
/// `Storage` trait, this doesn't create internal runtimes and integrates
/// cleanly with async/await.
///
/// # Design Notes
///
/// - **Async-first**: All methods are async and use `await` directly
/// - **No runtime creation**: Implementations borrow the runtime from the caller
/// - **Bytes-based**: Uses `bytes::Bytes` for zero-copy data transfer
/// - **Dyn-compatible**: Uses `&Path` for trait object support
#[async_trait::async_trait]
pub trait AsyncStorage: Send + Sync {
    /// Read all data from the given path.
    ///
    /// Returns the complete file contents as `Bytes`.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::NotFound` if the object doesn't exist.
    async fn read(&self, path: &Path) -> Result<Bytes>;

    /// Write data to the given path.
    ///
    /// Creates or replaces the object at the given path.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::PermissionDenied` if the location isn't writable.
    async fn write(&self, path: &Path, data: Bytes) -> Result<()>;

    /// Check if an object exists at the given path.
    async fn exists(&self, path: &Path) -> bool;

    /// Get the size of an object in bytes.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::NotFound` if the object doesn't exist.
    async fn size(&self, path: &Path) -> Result<u64>;

    /// Get full metadata for an object.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::NotFound` if the object doesn't exist.
    async fn metadata(&self, path: &Path) -> Result<ObjectMetadata>;

    /// List objects with the given prefix.
    ///
    /// Returns a vector of metadata for all matching objects.
    async fn list(&self, prefix: &Path) -> Result<Vec<ObjectMetadata>>;

    /// Delete an object at the given path.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::NotFound` if the object doesn't exist.
    async fn delete(&self, path: &Path) -> Result<()>;

    /// Copy an object from one path to another.
    ///
    /// Both paths must be within the same storage backend.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::NotFound` if the source doesn't exist.
    async fn copy(&self, from: &Path, to: &Path) -> Result<()>;

    /// Create a directory (no-op for cloud storage).
    async fn create_dir(&self, path: &Path) -> Result<()>;

    /// Create all directories in a path (no-op for cloud storage).
    async fn create_dir_all(&self, path: &Path) -> Result<()>;

    /// Read a specific byte range from an object.
    ///
    /// Returns the requested range as `Bytes`. The end offset is exclusive.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::NotFound` if the object doesn't exist.
    async fn read_range(&self, path: &Path, start: u64, end: Option<u64>) -> Result<Bytes>;
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    // Tests for specific implementations (AsyncOssStorage, etc.)
    // will be in their respective modules.
}
