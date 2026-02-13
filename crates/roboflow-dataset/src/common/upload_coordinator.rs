// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Unified upload coordinator trait for consolidating upload operations.
//!
//! This module provides the [`UploadCoordinator`] trait that unifies the various
//! upload implementations across the codebase, including:
//! - [`EpisodeUploadCoordinator`] - Parallel episode file uploads
//! - [`CloudUploader`] - Simple file uploads
//!
//! # Design
//!
//! The trait provides a common interface for:
//! - Uploading single files
//! - Uploading multiple files in parallel
//! - Tracking upload progress
//! - Finalizing uploads
//!
//! # Example
//!
//! ```ignore
//! use roboflow_dataset::common::upload_coordinator::{UploadCoordinator, UploadProgress};
//!
//! fn upload_files<C: UploadCoordinator>(coordinator: &C, files: &[(PathBuf, PathBuf)]) -> Result<()> {
//!     coordinator.upload_parallel(files)?;
//!     let progress = coordinator.progress();
//!     println!("Uploaded {} files, {} bytes", progress.files_uploaded, progress.bytes_uploaded);
//!     Ok(())
//! }
//! ```

use std::path::{Path, PathBuf};

use roboflow_core::Result;

// =============================================================================
// Upload Progress
// =============================================================================

/// Progress information for upload operations.
#[derive(Debug, Clone, Default)]
pub struct UploadProgress {
    /// Number of files successfully uploaded.
    pub files_uploaded: u64,

    /// Number of files that failed to upload.
    pub files_failed: u64,

    /// Total bytes uploaded.
    pub bytes_uploaded: u64,

    /// Number of files currently pending upload.
    pub files_pending: u64,

    /// Number of uploads currently in progress.
    pub files_in_progress: u64,
}

impl UploadProgress {
    /// Create a new empty progress tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the total number of files processed (uploaded + failed).
    pub fn total_files(&self) -> u64 {
        self.files_uploaded + self.files_failed
    }

    /// Get the success rate as a percentage (0-100).
    pub fn success_rate(&self) -> f64 {
        let total = self.total_files();
        if total == 0 {
            return 100.0;
        }
        (self.files_uploaded as f64 / total as f64) * 100.0
    }

    /// Check if there are any pending or in-progress uploads.
    pub fn is_complete(&self) -> bool {
        self.files_pending == 0 && self.files_in_progress == 0
    }
}

// =============================================================================
// Upload Coordinator Trait
// =============================================================================

/// Trait for coordinating file uploads to storage backends.
///
/// This trait provides a unified interface for uploading files, whether
/// to local filesystem, S3, OSS, or other storage backends. Implementations
/// may use different strategies (parallel workers, streaming, etc.) but
/// all provide the same high-level API.
///
/// # Thread Safety
///
/// All implementations must be `Send + Sync` to allow sharing across
/// threads for parallel upload operations.
///
/// # Implementations
///
/// - [`EpisodeUploadCoordinator`] - Parallel uploads with background workers
/// - [`CloudUploader`] - Simple sequential uploads
pub trait UploadCoordinator: Send + Sync {
    /// Upload a single file from local path to remote path.
    ///
    /// # Arguments
    ///
    /// * `local_path` - Path to the local file to upload
    /// * `remote_path` - Destination path in the storage backend
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The local file doesn't exist
    /// - The upload fails (network error, permission denied, etc.)
    fn upload(&self, local_path: &Path, remote_path: &Path) -> Result<()>;

    /// Upload multiple files in parallel.
    ///
    /// This method uploads multiple files concurrently, taking advantage
    /// of parallel I/O capabilities. The implementation determines the
    /// optimal level of parallelism.
    ///
    /// # Arguments
    ///
    /// * `items` - Slice of (local_path, remote_path) tuples
    ///
    /// # Errors
    ///
    /// Returns an error if any upload fails. Some implementations may
    /// continue uploading remaining files even after a failure, while
    /// others may stop immediately.
    fn upload_parallel(&self, items: &[(PathBuf, PathBuf)]) -> Result<()>;

    /// Get the current upload progress.
    ///
    /// Returns a snapshot of the upload progress including files
    /// uploaded, bytes transferred, and pending/in-progress counts.
    fn progress(&self) -> UploadProgress;

    /// Wait for all pending uploads to complete.
    ///
    /// This method blocks until all queued uploads have finished
    /// (either successfully or with an error).
    ///
    /// # Errors
    ///
    /// Returns an error if the wait times out or is interrupted.
    fn flush(&self) -> Result<()>;
}

// =============================================================================
// Blanket Implementation for References
// =============================================================================

impl<T: UploadCoordinator + ?Sized> UploadCoordinator for &T {
    fn upload(&self, local_path: &Path, remote_path: &Path) -> Result<()> {
        (**self).upload(local_path, remote_path)
    }

    fn upload_parallel(&self, items: &[(PathBuf, PathBuf)]) -> Result<()> {
        (**self).upload_parallel(items)
    }

    fn progress(&self) -> UploadProgress {
        (**self).progress()
    }

    fn flush(&self) -> Result<()> {
        (**self).flush()
    }
}

impl<T: UploadCoordinator + ?Sized> UploadCoordinator for Box<T> {
    fn upload(&self, local_path: &Path, remote_path: &Path) -> Result<()> {
        (**self).upload(local_path, remote_path)
    }

    fn upload_parallel(&self, items: &[(PathBuf, PathBuf)]) -> Result<()> {
        (**self).upload_parallel(items)
    }

    fn progress(&self) -> UploadProgress {
        (**self).progress()
    }

    fn flush(&self) -> Result<()> {
        (**self).flush()
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upload_progress_new() {
        let progress = UploadProgress::new();
        assert_eq!(progress.files_uploaded, 0);
        assert_eq!(progress.files_failed, 0);
        assert_eq!(progress.bytes_uploaded, 0);
    }

    #[test]
    fn test_upload_progress_total_files() {
        let mut progress = UploadProgress::new();
        progress.files_uploaded = 8;
        progress.files_failed = 2;
        assert_eq!(progress.total_files(), 10);
    }

    #[test]
    fn test_upload_progress_success_rate() {
        let mut progress = UploadProgress::new();

        // Empty progress = 100%
        assert_eq!(progress.success_rate(), 100.0);

        // 80% success rate
        progress.files_uploaded = 8;
        progress.files_failed = 2;
        assert_eq!(progress.success_rate(), 80.0);

        // 100% success rate
        progress.files_uploaded = 10;
        progress.files_failed = 0;
        assert_eq!(progress.success_rate(), 100.0);
    }

    #[test]
    fn test_upload_progress_is_complete() {
        let mut progress = UploadProgress::new();

        // Empty = complete
        assert!(progress.is_complete());

        // Pending files = not complete
        progress.files_pending = 5;
        assert!(!progress.is_complete());

        // In progress = not complete
        progress.files_pending = 0;
        progress.files_in_progress = 3;
        assert!(!progress.is_complete());

        // Both zero = complete
        progress.files_in_progress = 0;
        assert!(progress.is_complete());
    }
}
