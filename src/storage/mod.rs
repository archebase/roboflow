// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! # Storage Abstraction Layer
//!
//! This module provides a unified storage abstraction that supports multiple backends:
//! - Local filesystem (always available)
//! - Alibaba OSS (S3-compatible, requires `cloud-storage` feature)
//! - Amazon S3 (requires `cloud-storage` feature)
//!
//! ## Design Principles
//!
//! - **Synchronous API**: All operations are synchronous, blocking calls
//! - **Seek support**: Local storage supports seeking; cloud storage uses buffering
//! - **Feature-gated**: Cloud storage is opt-in via the `cloud-storage` feature
//! - **Zero-copy**: Readers return `Box<dyn Read>` for zero-copy where possible
//!
//! ## Example
//!
//! ```ignore
//! use roboflow::storage::{Storage, LocalStorage};
//! use std::io::Read;
//!
//! let storage = LocalStorage::new("/tmp")?;
//! let mut reader = storage.reader("test.txt")?;
//! let mut content = String::new();
//! reader.read_to_string(&mut content)?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

pub mod factory;
pub mod local;
pub mod url;

#[cfg(feature = "cloud-storage")]
pub mod oss;

pub use factory::{StorageConfig, StorageFactory};
pub use local::LocalStorage;
pub use url::StorageUrl;

#[cfg(feature = "cloud-storage")]
pub use oss::OssStorage;

use std::io::{Read, Write};
use std::path::Path;
use std::time::SystemTime;

// =============================================================================
// Storage Error
// =============================================================================

/// Unified error type for all storage operations.
///
/// This error type encompasses failures across all storage backends,
/// providing consistent error handling regardless of the underlying storage.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// The requested object does not exist.
    #[error("object not found: {0}")]
    NotFound(String),

    /// Permission denied for the requested operation.
    #[error("permission denied: {0}")]
    PermissionDenied(String),

    /// An object with the same name already exists (for create-exclusive operations).
    #[error("object already exists: {0}")]
    AlreadyExists(String),

    /// The provided path or URL is invalid.
    #[error("invalid path: {0}")]
    InvalidPath(String),

    /// A network error occurred during a cloud storage operation.
    #[error("network error: {0}")]
    NetworkError(String),

    /// The operation timed out.
    #[error("operation timed out: {0}")]
    Timeout(String),

    /// An underlying I/O error occurred.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// An error occurred during cloud storage operations.
    #[cfg(feature = "cloud-storage")]
    #[error("cloud storage error: {0}")]
    Cloud(String),

    /// A generic error with a message.
    #[error("{0}")]
    Other(String),
}

impl StorageError {
    /// Create a not found error for the given path.
    pub fn not_found(path: impl Into<String>) -> Self {
        Self::NotFound(path.into())
    }

    /// Create a permission denied error for the given path.
    pub fn permission_denied(path: impl Into<String>) -> Self {
        Self::PermissionDenied(path.into())
    }

    /// Create an invalid path error.
    pub fn invalid_path(path: impl Into<String>) -> Self {
        Self::InvalidPath(path.into())
    }

    /// Create a timeout error.
    pub fn timeout(operation: impl Into<String>) -> Self {
        Self::Timeout(operation.into())
    }

    /// Create a network error.
    pub fn network(msg: impl Into<String>) -> Self {
        Self::NetworkError(msg.into())
    }

    /// Create an other error with a message.
    pub fn other(msg: impl Into<String>) -> Self {
        Self::Other(msg.into())
    }

    /// Check if this error is retryable.
    ///
    /// Retryable errors include timeouts, network errors, and some cloud errors.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::NetworkError(_) | Self::Timeout(_) | Self::Cloud(_)
        )
    }
}

/// Result type for storage operations.
pub type Result<T> = std::result::Result<T, StorageError>;

// =============================================================================
// Object Metadata
// =============================================================================

/// Metadata about a storage object.
///
/// This structure provides information about objects stored in any backend,
/// including size, modification time, and optional content type.
#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    /// The full path or key of the object.
    pub path: String,

    /// Size of the object in bytes.
    pub size: u64,

    /// Last modification time, if available.
    pub last_modified: Option<SystemTime>,

    /// Content type (MIME type), if available.
    pub content_type: Option<String>,

    /// Whether this object represents a directory (for local filesystem).
    pub is_dir: bool,
}

impl ObjectMetadata {
    /// Create new object metadata.
    pub fn new(path: impl Into<String>, size: u64) -> Self {
        Self {
            path: path.into(),
            size,
            last_modified: None,
            content_type: None,
            is_dir: false,
        }
    }

    /// Create metadata for a directory.
    pub fn dir(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            size: 0,
            last_modified: None,
            content_type: None,
            is_dir: true,
        }
    }

    /// Set the last modified time.
    pub fn with_last_modified(mut self, time: SystemTime) -> Self {
        self.last_modified = Some(time);
        self
    }

    /// Set the content type.
    pub fn with_content_type(mut self, ctype: impl Into<String>) -> Self {
        self.content_type = Some(ctype.into());
        self
    }
}

// =============================================================================
// Storage Trait
// =============================================================================

/// Core storage abstraction supporting multiple backends.
///
/// This trait provides a synchronous interface for storage operations,
/// allowing code to work with local filesystem or cloud storage interchangeably.
///
/// # Design Notes
///
/// - **Synchronous**: All methods block until complete. Cloud backends handle
///   async operations internally via a blocking tokio runtime.
/// - **Boxed traits**: Returns `Box<dyn Read/Write>` to enable zero-copy
///   where supported.
/// - **No seeking**: The base trait doesn't support seeking. Use `SeekableStorage`
///   extension trait for backends that support it.
/// - **Dyn-compatible**: Uses `&Path` instead of `impl AsRef<Path>` to allow
///   trait objects (`Arc<dyn Storage>`).
pub trait Storage: Send + Sync {
    /// Open a reader for the given path.
    ///
    /// Returns a boxed Read trait object that can be used to read the object's contents.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::NotFound` if the object doesn't exist.
    fn reader(&self, path: &Path) -> Result<Box<dyn Read + Send + 'static>>;

    /// Open a writer for the given path.
    ///
    /// Returns a boxed Write trait object. Creates parent directories as needed
    /// for backends that support hierarchical storage.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::PermissionDenied` if the location isn't writable.
    fn writer(&self, path: &Path) -> Result<Box<dyn Write + Send + 'static>>;

    /// Check if an object exists at the given path.
    ///
    /// Returns `true` if the object exists, `false` otherwise.
    fn exists(&self, path: &Path) -> bool;

    /// Get the size of an object in bytes.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::NotFound` if the object doesn't exist.
    fn size(&self, path: &Path) -> Result<u64>;

    /// Get full metadata for an object.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::NotFound` if the object doesn't exist.
    fn metadata(&self, path: &Path) -> Result<ObjectMetadata>;

    /// List objects with the given prefix.
    ///
    /// Returns a vector of metadata for all matching objects.
    /// For hierarchical backends (like local filesystem), this may include
    /// directory entries.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::InvalidPath` if the prefix is invalid.
    fn list(&self, prefix: &Path) -> Result<Vec<ObjectMetadata>>;

    /// Delete an object at the given path.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::NotFound` if the object doesn't exist.
    fn delete(&self, path: &Path) -> Result<()>;

    /// Copy an object from one path to another.
    ///
    /// Both paths must be within the same storage backend.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::NotFound` if the source doesn't exist.
    fn copy(&self, from: &Path, to: &Path) -> Result<()>;

    /// Create a directory (for backends that support directories).
    ///
    /// For cloud storage, this may be a no-op since buckets are flat.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::PermissionDenied` if creation fails.
    fn create_dir(&self, path: &Path) -> Result<()>;

    /// Create a directory and all parent directories.
    ///
    /// Similar to `std::fs::create_dir_all`.
    fn create_dir_all(&self, path: &Path) -> Result<()>;
}

// =============================================================================
// Seekable Storage Extension Trait
// =============================================================================

/// Combined trait for seekable readers.
///
/// This trait combines `Read` and `Seek` for use in trait objects.
pub trait SeekRead: std::io::Read + std::io::Seek {}
impl<T: std::io::Read + std::io::Seek> SeekRead for T {}

/// Extension trait for storage backends that support seeking.
///
/// Local filesystem implements this; cloud storage typically does not
/// (except through buffering).
pub trait SeekableStorage: Storage {
    /// Open a seekable reader for the given path.
    ///
    /// Unlike `reader()`, this returns a type that implements `std::io::Seek`.
    fn seekable_reader(&self, path: &Path) -> Result<Box<dyn SeekRead + Send + 'static>>;

    /// Open a reader that can be either seekable or streaming.
    ///
    /// Returns a seekable reader if supported, otherwise falls back to
    /// a streaming reader.
    fn reader_seekable(&self, path: &Path) -> Result<Box<dyn Read + Send + 'static>> {
        // Default implementation returns the non-seekable reader
        self.reader(path)
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_error_is_retryable() {
        assert!(StorageError::network("test").is_retryable());
        assert!(StorageError::timeout("test").is_retryable());
        #[cfg(feature = "cloud-storage")]
        assert!(StorageError::Cloud("test".to_string()).is_retryable());
        assert!(!StorageError::not_found("test").is_retryable());
        assert!(!StorageError::permission_denied("test").is_retryable());
        assert!(!StorageError::invalid_path("test").is_retryable());
    }

    #[test]
    fn test_object_metadata_new() {
        let meta = ObjectMetadata::new("test.txt", 1024);
        assert_eq!(meta.path, "test.txt");
        assert_eq!(meta.size, 1024);
        assert!(!meta.is_dir);
        assert!(meta.last_modified.is_none());
        assert!(meta.content_type.is_none());
    }

    #[test]
    fn test_object_metadata_dir() {
        let meta = ObjectMetadata::dir("/tmp/test");
        assert_eq!(meta.path, "/tmp/test");
        assert!(meta.is_dir);
        assert_eq!(meta.size, 0);
    }

    #[test]
    fn test_object_metadata_builder() {
        let meta = ObjectMetadata::new("test.txt", 1024)
            .with_content_type("text/plain")
            .with_last_modified(SystemTime::now());

        assert_eq!(meta.path, "test.txt");
        assert_eq!(meta.content_type.as_deref(), Some("text/plain"));
        assert!(meta.last_modified.is_some());
    }
}
