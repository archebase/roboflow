// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Streaming multipart upload support.
//!
//! This module provides unified streaming upload functionality across
//! all storage backends (local filesystem, S3, OSS).
//!
//! # Design
//!
//! - [`MultipartUpload`] trait for streaming upload operations
//! - [`Storage::put_multipart_stream`] method to create uploads
//! - [`S3Storage`] uses `object_store::WriteMultipart` for cloud
//! - [`LocalStorage`] buffers to a temporary file for local filesystem
//!
//! # Example
//!
//! ```ignore
//! use roboflow_storage::{Storage, MultipartUpload};
//!
//! // Create a streaming upload
//! let mut upload = storage.put_multipart_stream(Path::new("videos/output.mp4"))?;
//!
//! // Write chunks (can be called multiple times)
//! upload.write(&chunk1)?;
//! upload.write(&chunk2)?;
//!
//! // Finish and get statistics
//! let stats = upload.finish()?;
//! println!("Uploaded {} bytes", stats.bytes_uploaded);
//! ```

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::Storage;
use crate::StorageError;
use crate::StorageResult as Result;

// =============================================================================
// Multipart Upload Trait
// =============================================================================

/// Statistics from a completed multipart upload.
#[derive(Debug, Clone, PartialEq)]
pub struct UploadStats {
    /// Total bytes uploaded
    pub bytes_uploaded: u64,
    /// Number of parts uploaded (for cloud backends)
    pub parts_count: u64,
    /// Duration of the upload
    pub duration: Duration,
}

impl UploadStats {
    /// Create new upload statistics.
    pub fn new(bytes_uploaded: u64, parts_count: u64, duration: Duration) -> Self {
        Self {
            bytes_uploaded,
            parts_count,
            duration,
        }
    }

    /// Create stats with only byte count (duration and parts unknown/zero).
    pub fn bytes(bytes_uploaded: u64) -> Self {
        Self {
            bytes_uploaded,
            parts_count: 1,
            duration: Duration::ZERO,
        }
    }
}

/// Trait for streaming multipart upload operations.
///
/// This trait provides a unified interface for uploading data in chunks
/// when the total size is unknown beforehand (e.g., streaming video encoding).
///
/// # Implementations
///
/// - [`CloudMultipartUpload`] - Wraps `object_store::WriteMultipart` for S3/OSS
/// - [`LocalMultipartUpload`] - Buffers to temp file for local filesystem
pub trait MultipartUpload: Send {
    /// Write a chunk of data to the upload.
    ///
    /// This can be called multiple times with chunks of varying sizes.
    /// The implementation will buffer and upload parts as needed.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The upload has already been finished or aborted
    /// - A network error occurs (for cloud backends)
    /// - The filesystem is full (for local backend)
    fn write(&mut self, data: &[u8]) -> Result<()>;

    /// Finish the upload and return statistics.
    ///
    /// This flushes any remaining buffered data and completes the upload.
    /// After calling `finish`, the upload cannot be used further.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The upload has already been finished or aborted
    /// - Completing the upload fails (e.g., network error)
    fn finish(self: Box<Self>) -> Result<UploadStats>;

    /// Abort the upload, discarding any data.
    ///
    /// For cloud backends, this cancels the multipart upload.
    /// For local backend, this deletes the temporary file.
    ///
    /// # Errors
    ///
    /// Returns an error if aborting fails (e.g., network error).
    fn abort(self: Box<Self>) -> Result<()>;

    /// Get the total number of bytes written so far.
    fn bytes_written(&self) -> u64;
}

// =============================================================================
// Cloud Implementation (Wraps object_store::WriteMultipart)
// =============================================================================

use object_store::WriteMultipart;

/// Cloud multipart upload using `object_store::WriteMultipart`.
///
/// This is used by `S3Storage` for S3 and OSS backends.
pub struct CloudMultipartUpload {
    /// The underlying WriteMultipart from object_store
    upload: WriteMultipart,
    /// Runtime for async operations
    runtime: tokio::runtime::Handle,
    /// Total bytes written so far
    bytes_written: u64,
    /// Number of chunks written
    chunks_written: u64,
    /// Start time for duration tracking
    start_time: std::time::Instant,
    /// Whether the upload is finished
    finished: bool,
}

impl CloudMultipartUpload {
    /// Create a new cloud multipart upload.
    ///
    /// # Arguments
    ///
    /// * `upload` - The WriteMultipart from object_store
    /// * `runtime` - Tokio runtime handle for async operations
    pub fn new(upload: WriteMultipart, runtime: tokio::runtime::Handle) -> Self {
        Self {
            upload,
            runtime,
            bytes_written: 0,
            chunks_written: 0,
            start_time: std::time::Instant::now(),
            finished: false,
        }
    }
}

impl MultipartUpload for CloudMultipartUpload {
    fn write(&mut self, data: &[u8]) -> Result<()> {
        if self.finished {
            return Err(StorageError::Other(
                "Cannot write to finished upload".to_string(),
            ));
        }

        self.upload.write(data);
        self.bytes_written += data.len() as u64;
        self.chunks_written += 1;
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<UploadStats> {
        if self.finished {
            return Err(StorageError::Other("Upload already finished".to_string()));
        }
        self.finished = true;

        let duration = self.start_time.elapsed();
        let bytes = self.bytes_written;
        let chunks = self.chunks_written;

        // Take ownership of the upload and runtime
        let upload = self.upload;
        let runtime = self.runtime;

        // Complete the multipart upload (async)
        runtime.block_on(async {
            upload
                .finish()
                .await
                .map_err(|e| StorageError::Cloud(format!("Failed to complete upload: {}", e)))
        })?;

        Ok(UploadStats::new(bytes, chunks, duration))
    }

    fn abort(mut self: Box<Self>) -> Result<()> {
        if self.finished {
            return Err(StorageError::Other("Upload already finished".to_string()));
        }
        self.finished = true;

        // Take ownership of the upload and runtime
        let upload = self.upload;
        let runtime = self.runtime;

        // Abort the multipart upload (async)
        runtime.block_on(async {
            upload
                .abort()
                .await
                .map_err(|e| StorageError::Cloud(format!("Failed to abort upload: {}", e)))
        })?;

        tracing::debug!("Cloud multipart upload aborted");
        Ok(())
    }

    fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

// =============================================================================
// Local Implementation (Temp File Buffering)
// =============================================================================

/// Local filesystem multipart upload using temporary file buffering.
///
/// This is used by `LocalStorage` to simulate multipart upload behavior.
/// Data is buffered to a temporary file, then moved to the final location on finish.
pub struct LocalMultipartUpload {
    /// Buffer writer (writes to temp file)
    writer: BufWriter<File>,
    /// Target path for final location
    target_path: PathBuf,
    /// Temp file path (for cleanup on abort)
    temp_path: PathBuf,
    /// Total bytes written so far
    bytes_written: u64,
    /// Start time for duration tracking
    start_time: std::time::Instant,
    /// Whether the upload is finished
    finished: bool,
}

impl LocalMultipartUpload {
    /// Create a new local multipart upload.
    ///
    /// # Arguments
    ///
    /// * `writer` - BufWriter writing to a temp file
    /// * `temp_path` - Path to the temporary file
    /// * `target_path` - Final destination path
    pub fn new(writer: BufWriter<File>, temp_path: PathBuf, target_path: PathBuf) -> Self {
        Self {
            writer,
            target_path,
            temp_path,
            bytes_written: 0,
            start_time: std::time::Instant::now(),
            finished: false,
        }
    }
}

impl MultipartUpload for LocalMultipartUpload {
    fn write(&mut self, data: &[u8]) -> Result<()> {
        if self.finished {
            return Err(StorageError::Other(
                "Cannot write to finished upload".to_string(),
            ));
        }

        self.writer.write_all(data).map_err(StorageError::Io)?;
        self.writer.flush().map_err(StorageError::Io)?;
        self.bytes_written += data.len() as u64;
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<UploadStats> {
        if self.finished {
            return Err(StorageError::Other("Upload already finished".to_string()));
        }
        self.finished = true;

        let duration = self.start_time.elapsed();
        let bytes = self.bytes_written;

        // Extract fields before consuming self
        let target_path = self.target_path.clone();
        let temp_path = self.temp_path.clone();

        // Flush and close temp file
        let file = self
            .writer
            .into_inner()
            .map_err(|e| StorageError::Other(format!("BufWriter error: {}", e)))?;
        file.sync_all().map_err(StorageError::Io)?;

        // Ensure parent directory exists
        if let Some(parent) = target_path.parent() {
            std::fs::create_dir_all(parent).map_err(StorageError::Io)?;
        }

        // Move temp file to final location
        std::fs::rename(&temp_path, &target_path).map_err(|e| {
            // Clean up temp file on failure
            let _ = std::fs::remove_file(&temp_path);
            StorageError::Io(e)
        })?;

        tracing::debug!(
            target = %target_path.display(),
            bytes = bytes,
            "Local multipart upload completed"
        );

        Ok(UploadStats::new(bytes, 1, duration))
    }

    fn abort(mut self: Box<Self>) -> Result<()> {
        if self.finished {
            return Err(StorageError::Other("Upload already finished".to_string()));
        }
        self.finished = true;

        // Extract temp path before consuming self
        let temp_path = self.temp_path.clone();

        // Close and delete temp file
        drop(self.writer);
        std::fs::remove_file(&temp_path).map_err(StorageError::Io)?;

        tracing::debug!(
            temp = %temp_path.display(),
            "Local multipart upload aborted"
        );

        Ok(())
    }

    fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

// =============================================================================
// Storage Trait Extension
// =============================================================================

/// Extension trait for adding streaming upload to Storage.
///
/// This is implemented for all Storage types, providing a unified
/// interface for creating multipart uploads.
pub trait StorageStreamingExt: Storage {
    /// Create a streaming multipart upload.
    ///
    /// This is used for uploading data when the total size is unknown
    /// (e.g., streaming video encoding, real-time data capture).
    ///
    /// # Arguments
    ///
    /// * `path` - Destination path for the uploaded object
    ///
    /// # Returns
    ///
    /// A boxed MultipartUpload trait object for the upload.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The path is invalid
    /// - Creating the upload fails (e.g., network error for cloud)
    fn put_multipart_stream(&self, path: &Path) -> Result<Box<dyn MultipartUpload>>;
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upload_stats_new() {
        let stats = UploadStats::new(1024, 2, Duration::from_secs(5));
        assert_eq!(stats.bytes_uploaded, 1024);
        assert_eq!(stats.parts_count, 2);
        assert_eq!(stats.duration, Duration::from_secs(5));
    }

    #[test]
    fn test_upload_stats_bytes() {
        let stats = UploadStats::bytes(2048);
        assert_eq!(stats.bytes_uploaded, 2048);
        assert_eq!(stats.parts_count, 1);
        assert_eq!(stats.duration, Duration::ZERO);
    }

    // LocalMultipartUpload tests
    #[test]
    fn test_local_multipart_upload_write_and_finish() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_path = temp_dir.path().join("temp.mp4");
        let target_path = temp_dir.path().join("final.mp4");

        let file = File::create(&temp_path).unwrap();
        let writer = BufWriter::new(file);
        let mut upload: Box<dyn MultipartUpload> = Box::new(LocalMultipartUpload::new(
            writer,
            temp_path.clone(),
            target_path.clone(),
        ));

        // Write some data
        upload.write(b"hello").unwrap();
        upload.write(b" world").unwrap();
        assert_eq!(upload.bytes_written(), 11);

        // Finish
        let stats = upload.finish().unwrap();
        assert_eq!(stats.bytes_uploaded, 11);
        assert_eq!(stats.parts_count, 1);
        assert!(target_path.exists());

        // Verify content
        let content = std::fs::read_to_string(&target_path).unwrap();
        assert_eq!(content, "hello world");
    }

    #[test]
    fn test_local_multipart_upload_abort() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_path = temp_dir.path().join("temp.mp4");
        let target_path = temp_dir.path().join("final.mp4");

        let file = File::create(&temp_path).unwrap();
        let writer = BufWriter::new(file);
        let mut upload: Box<dyn MultipartUpload> = Box::new(LocalMultipartUpload::new(
            writer,
            temp_path.clone(),
            target_path.clone(),
        ));

        // Write some data then abort
        upload.write(b"test data").unwrap();
        upload.abort().unwrap();

        // Target should not exist
        assert!(!target_path.exists());
        // Temp file should be cleaned up
        assert!(!temp_path.exists());
    }

    #[test]
    fn test_local_multipart_upload_creates_parent_dir() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_path = temp_dir.path().join("temp.mp4");
        let target_path = temp_dir.path().join("nested").join("dir").join("final.mp4");

        let file = File::create(&temp_path).unwrap();
        let writer = BufWriter::new(file);
        let mut upload: Box<dyn MultipartUpload> = Box::new(LocalMultipartUpload::new(
            writer,
            temp_path,
            target_path.clone(),
        ));

        upload.write(b"data").unwrap();
        upload.finish().unwrap();

        // Parent directory should be created
        assert!(target_path.exists());
        assert!(target_path.parent().unwrap().exists());
    }
}
