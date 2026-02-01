// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! # roboflow-storage
//!
//! Storage abstraction layer for roboflow.
//!
//! This crate provides a unified storage abstraction that supports multiple backends:
//! - **Local filesystem** - Always available for development/testing
//! - **Alibaba OSS** - S3-compatible cloud storage (always available)
//! - **Amazon S3** - Via the same OSS backend (always available)
//!
//! ## Design Philosophy
//!
//! **S3/OSS is the production storage layer** for distributed systems.
//! Local filesystem is for development/testing only.
//! **No feature flags** - all storage backends are always available.
//!
//! ## Example
//!
//! ```ignore
//! use roboflow_storage::{Storage, LocalStorage, OssStorage};
//!
//! // Local storage (for development)
//! let local = LocalStorage::new("/tmp")?;
//!
//! // Cloud storage (for production)
//! let oss = OssStorage::new("bucket", "endpoint", "key", "secret")?;
//!
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

pub mod cached;
pub mod config_file;
pub mod factory;
pub mod local;
pub mod multipart;
pub mod oss;
pub mod retry;
pub mod streaming;
pub mod url;

// Re-export public types
pub use cached::{CacheConfig, CacheStats, CachedStorage, EvictionPolicy};
pub use config_file::{ConfigError, RoboflowConfig};
pub use factory::{StorageConfig, StorageFactory};
pub use local::LocalStorage;
pub use multipart::{
    MultipartConfig, MultipartStats, MultipartUploader, ProgressCallback, upload_multipart,
};
pub use oss::{OssConfig, OssStorage};
pub use retry::{RetryConfig, RetryingStorage, retry_with_backoff};
pub use url::StorageUrl;

// Re-export from mod.rs
pub use crate::error::{
    ObjectMetadata, SeekRead, SeekableStorage, Storage, StorageError, StorageResult,
    StreamingConfig, StreamingRead,
};

// =============================================================================
// Storage Error
// =============================================================================

use std::io::{Read, Write};
use std::path::Path;
use std::time::SystemTime;

mod error {
    use super::*;

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
    pub type StorageResult<T> = std::result::Result<T, StorageError>;

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

    // =============================================================================
    // Streaming Configuration
    // =============================================================================

    /// Configuration for streaming readers.
    ///
    /// Controls chunk size for streaming storage operations.
    ///
    /// # Note
    ///
    /// The `prefetch_count` field is reserved for future use. Background prefetch
    /// is not yet implemented - streaming readers fetch data synchronously on demand.
    #[derive(Debug, Clone)]
    pub struct StreamingConfig {
        /// Size of each chunk to fetch (default: 16MB)
        pub chunk_size: usize,

        /// Number of chunks to prefetch ahead (reserved for future use, not yet implemented)
        pub prefetch_count: usize,
    }

    impl Default for StreamingConfig {
        fn default() -> Self {
            Self {
                chunk_size: 16 * 1024 * 1024, // 16MB
                prefetch_count: 2,
            }
        }
    }

    impl StreamingConfig {
        /// Create a new streaming config with custom chunk size.
        pub fn with_chunk_size(mut self, size: usize) -> Self {
            self.chunk_size = size;
            self
        }

        /// Create a new streaming config with custom prefetch count.
        ///
        /// # Note
        ///
        /// Prefetch is not yet implemented. This setting is reserved for future use.
        pub fn with_prefetch_count(self, _count: usize) -> Self {
            // TODO: Implement prefetch and use _count
            self
        }
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
        fn reader(&self, path: &Path) -> StorageResult<Box<dyn Read + Send + 'static>>;

        /// Open a writer for the given path.
        ///
        /// Returns a boxed Write trait object. Creates parent directories as needed
        /// for backends that support hierarchical storage.
        ///
        /// # Errors
        ///
        /// Returns `StorageError::PermissionDenied` if the location isn't writable.
        fn writer(&self, path: &Path) -> StorageResult<Box<dyn Write + Send + 'static>>;

        /// Check if an object exists at the given path.
        ///
        /// Returns `true` if the object exists, `false` otherwise.
        fn exists(&self, path: &Path) -> bool;

        /// Get the size of an object in bytes.
        ///
        /// # Errors
        ///
        /// Returns `StorageError::NotFound` if the object doesn't exist.
        fn size(&self, path: &Path) -> StorageResult<u64>;

        /// Get full metadata for an object.
        ///
        /// # Errors
        ///
        /// Returns `StorageError::NotFound` if the object doesn't exist.
        fn metadata(&self, path: &Path) -> StorageResult<ObjectMetadata>;

        /// List objects with the given prefix.
        ///
        /// Returns a vector of metadata for all matching objects.
        /// For hierarchical backends (like local filesystem), this may include
        /// directory entries.
        ///
        /// # Errors
        ///
        /// Returns `StorageError::InvalidPath` if the prefix is invalid.
        fn list(&self, prefix: &Path) -> StorageResult<Vec<ObjectMetadata>>;

        /// Delete an object at the given path.
        ///
        /// # Errors
        ///
        /// Returns `StorageError::NotFound` if the object doesn't exist.
        fn delete(&self, path: &Path) -> StorageResult<()>;

        /// Copy an object from one path to another.
        ///
        /// Both paths must be within the same storage backend.
        ///
        /// # Errors
        ///
        /// Returns `StorageError::NotFound` if the source doesn't exist.
        fn copy(&self, from: &Path, to: &Path) -> StorageResult<()>;

        /// Create a directory (for backends that support directories).
        ///
        /// For cloud storage, this may be a no-op since buckets are flat.
        ///
        /// # Errors
        ///
        /// Returns `StorageError::PermissionDenied` if creation fails.
        fn create_dir(&self, path: &Path) -> StorageResult<()>;

        /// Create a directory and all parent directories.
        ///
        /// Similar to `std::fs::create_dir_all`.
        fn create_dir_all(&self, path: &Path) -> StorageResult<()>;

        /// Read a specific byte range from an object.
        ///
        /// Returns a reader for the requested range. The end offset is exclusive.
        ///
        /// # Errors
        ///
        /// Returns `StorageError::NotFound` if the object doesn't exist.
        fn read_range(
            &self,
            path: &Path,
            start: u64,
            _end: Option<u64>,
        ) -> StorageResult<Box<dyn Read + Send + 'static>> {
            // Default implementation: read entire object and skip to start
            let mut reader = self.reader(path)?;
            let mut skip_buf = vec![0u8; std::cmp::min(8192, start as usize)];
            let mut remaining = start;
            while remaining > 0 {
                let to_skip = std::cmp::min(skip_buf.len() as u64, remaining) as usize;
                reader.read_exact(&mut skip_buf[..to_skip])?;
                remaining -= to_skip as u64;
            }
            Ok(Box::new(reader))
        }

        /// Open a streaming reader with prefetch.
        ///
        /// Returns a reader that fetches data in chunks with configurable prefetch.
        ///
        /// # Errors
        ///
        /// Returns `StorageError::NotFound` if the object doesn't exist.
        fn streaming_reader(
            &self,
            _path: &Path,
            _config: StreamingConfig,
        ) -> StorageResult<Box<dyn StreamingRead + Send + 'static>> {
            // Default implementation: fall back to regular reader
            // Concrete types should override this for proper streaming support
            Err(StorageError::Other(
                "streaming_reader not implemented for this storage type".to_string(),
            ))
        }

        /// Get this storage as `Any` for downcasting.
        ///
        /// This enables checking the concrete type of a `dyn Storage` trait object,
        /// which is useful for optimizations like detecting local-only mode.
        ///
        /// # Default
        ///
        /// Returns a reference that cannot be downcast (for storage types
        /// that don't need downcasting support). Override this method to return
        /// `self` for types that support downcasting.
        fn as_any(&self) -> &dyn std::any::Any {
            &()
        }
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
        fn seekable_reader(&self, path: &Path)
        -> StorageResult<Box<dyn SeekRead + Send + 'static>>;

        /// Open a reader that can be either seekable or streaming.
        ///
        /// Returns a seekable reader if supported, otherwise falls back to
        /// a streaming reader.
        fn reader_seekable(&self, path: &Path) -> StorageResult<Box<dyn Read + Send + 'static>> {
            // Default implementation returns the non-seekable reader
            self.reader(path)
        }
    }

    // =============================================================================
    // Streaming Read Extension Trait
    // =============================================================================

    /// Combined trait for streaming readers with position tracking.
    ///
    /// This trait extends `Read` with position and seeking capabilities
    /// for streaming readers that may not support full `std::io::Seek`.
    pub trait StreamingRead: Read {
        /// Get the current read position in bytes.
        fn position(&self) -> u64;

        /// Seek to a specific byte offset.
        ///
        /// Discards any buffered data and starts fetching from the new position.
        fn seek_to(&mut self, offset: u64) -> StorageResult<()>;
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
        assert!(StorageError::Cloud("test".to_string()).is_retryable());
        assert!(!StorageError::NotFound("test".to_string()).is_retryable());
        assert!(!StorageError::PermissionDenied("test".to_string()).is_retryable());
        assert!(!StorageError::InvalidPath("test".to_string()).is_retryable());
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
