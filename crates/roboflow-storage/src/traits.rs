// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Storage trait definitions.
//!
//! This module provides the core storage abstraction traits that enable
//! working with different storage backends interchangeably.

use std::io::{Read, Write};
use std::path::Path;

use crate::error::{StorageError, StorageResult};
use crate::metadata::{ObjectMetadata, StreamingConfig};

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

    /// Upload a local file to storage efficiently.
    ///
    /// For cloud backends, this uses parallel multipart upload for large files,
    /// providing significantly better throughput than `writer()` for files over
    /// 100MB. For local storage, this is a simple file copy.
    ///
    /// # Arguments
    ///
    /// * `local_path` - Path to the local file to upload
    /// * `remote_path` - Destination path in storage
    ///
    /// # Returns
    ///
    /// Total bytes uploaded.
    fn upload_file(&self, local_path: &Path, remote_path: &Path) -> StorageResult<u64> {
        // Default implementation: read file and write via writer()
        let content = std::fs::read(local_path)?;
        let size = content.len() as u64;
        let mut writer = self.writer(remote_path)?;
        writer.write_all(&content)?;
        writer.flush()?;
        Ok(size)
    }

    /// Download a storage object to a local file efficiently.
    ///
    /// For cloud backends, this uses streaming range-request reads to avoid
    /// loading the entire object into memory. For local storage, this is a
    /// simple file copy.
    ///
    /// # Arguments
    ///
    /// * `remote_path` - Path to the object in storage
    /// * `local_path` - Destination path on local filesystem
    ///
    /// # Returns
    ///
    /// Total bytes downloaded.
    fn download_file(&self, remote_path: &Path, local_path: &Path) -> StorageResult<u64> {
        // Default implementation: read via reader() and write to file
        let mut reader = self.reader(remote_path)?;
        let file = std::fs::File::create(local_path)?;
        let mut writer = std::io::BufWriter::with_capacity(4 * 1024 * 1024, file);
        let bytes = std::io::copy(&mut reader, &mut writer)?;
        writer.flush()?;
        Ok(bytes)
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

    /// Delete all objects with a given prefix.
    ///
    /// This is useful for cleaning up temporary directories after
    /// composing segments into final files.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Path prefix to delete (e.g., "temp/session123/")
    ///
    /// # Returns
    ///
    /// The number of objects deleted.
    ///
    /// # Errors
    ///
    /// Returns an error if the delete operation fails.
    fn delete_prefix(&self, prefix: &Path) -> StorageResult<usize> {
        let _ = prefix;
        Err(StorageError::Other(
            "delete_prefix not implemented for this storage type".to_string(),
        ))
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
    fn seekable_reader(&self, path: &Path) -> StorageResult<Box<dyn SeekRead + Send + 'static>>;

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
