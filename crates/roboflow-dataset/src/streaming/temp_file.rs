// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Temporary file management for streaming conversion inputs.
//!
//! When processing input files from cloud storage, we need to download them
//! to a local temporary file before processing (since `RoboReader::open()`
//! requires a local file path). This module provides RAII-based management
//! of these temporary files to ensure cleanup.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use roboflow_storage::{LocalStorage, Storage, StorageError};

/// RAII guard for temporary input files.
///
/// Manages the lifecycle of a temporary file used for processing cloud inputs.
/// The temp file is automatically cleaned up when this guard is dropped,
/// unless explicitly retained.
///
/// # Local Storage Fast Path
///
/// When the input storage is `LocalStorage`, the original path is returned
/// directly without any copying. This avoids unnecessary I/O for local files.
///
/// # Example
///
/// ```ignore
/// use roboflow_storage::{Storage, LocalStorage};
/// use roboflow::streaming::TempFileManager;
///
/// let storage = Arc::new(LocalStorage::new("/data")) as Arc<dyn Storage>;
/// let input_path = Path::new("/data/input.mcap");
/// let temp_dir = Path::new("/tmp/roboflow");
///
/// let manager = TempFileManager::new(storage, input_path, temp_dir)?;
/// let processed_path = manager.path();  // Use this for conversion
///
/// // When `manager` is dropped, the temp file is automatically cleaned up
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct TempFileManager {
    /// Path to the file for processing (original or temp)
    process_path: PathBuf,

    /// Temp file path (if created, will be cleaned up on drop)
    temp_path: Option<PathBuf>,

    /// Whether to clean up on drop
    cleanup_on_drop: bool,
}

impl TempFileManager {
    /// Create a new temp file manager for the given input.
    ///
    /// If `input_storage` is `LocalStorage`, the original path is used directly
    /// (fast path, no copying). For cloud storage, the file is downloaded to
    /// a temporary location.
    ///
    /// # Arguments
    ///
    /// * `input_storage` - Storage backend for the input file
    /// * `input_path` - Path to the input file (in the storage backend)
    /// * `temp_dir` - Directory for temporary downloads
    ///
    /// # Returns
    ///
    /// A `TempFileManager` that will clean up the temp file on drop.
    pub fn new(
        input_storage: Arc<dyn Storage>,
        input_path: &Path,
        temp_dir: &Path,
    ) -> Result<Self, StorageError> {
        // Fast path for local storage: use original path directly
        if let Some(local_storage) = input_storage.as_any().downcast_ref::<LocalStorage>() {
            let full_path = local_storage.full_path(input_path)?;
            return Ok(Self {
                process_path: full_path,
                temp_path: None,
                cleanup_on_drop: true,
            });
        }

        // Cloud storage: download to temp file using streaming reads
        // This uses storage.download_file() which for cloud backends uses
        // range-request streaming (avoids loading the entire object into memory).
        let file_name = input_path
            .file_name()
            .ok_or_else(|| StorageError::invalid_path(input_path.display().to_string()))?;
        let unique_name = format!(
            "{}_{}",
            uuid::Uuid::new_v4().simple(),
            file_name.to_string_lossy()
        );
        std::fs::create_dir_all(temp_dir).map_err(StorageError::Io)?;
        let temp_path = temp_dir.join(&unique_name);

        input_storage.download_file(input_path, &temp_path)?;

        tracing::debug!(
            input = %input_path.display(),
            temp = %temp_path.display(),
            "Downloaded cloud input to temp file via streaming reads"
        );

        Ok(Self {
            process_path: temp_path.clone(),
            temp_path: Some(temp_path),
            cleanup_on_drop: true,
        })
    }

    /// Create a temp file manager with a custom temp directory path.
    ///
    /// This is a convenience method that creates the temp directory if needed.
    pub fn with_temp_dir(
        input_storage: Arc<dyn Storage>,
        input_path: &Path,
        temp_dir: &Path,
    ) -> Result<Self, StorageError> {
        std::fs::create_dir_all(temp_dir).map_err(StorageError::Io)?;
        Self::new(input_storage, input_path, temp_dir)
    }

    /// Get the path to use for processing.
    ///
    /// This returns either the original path (for local storage) or the
    /// downloaded temp file path (for cloud storage).
    pub fn path(&self) -> &Path {
        &self.process_path
    }

    /// Check if this is a temporary file (downloaded from cloud).
    pub fn is_temp(&self) -> bool {
        self.temp_path.is_some()
    }

    /// Prevent cleanup of the temp file and return its path.
    ///
    /// This is useful for debugging when you want to inspect the temp file
    /// after processing.
    ///
    /// Returns `Some(path)` if a temp file was created (cloud storage),
    /// or `None` if using the local storage fast path (no temp file).
    pub fn retain(&mut self) -> Option<PathBuf> {
        self.cleanup_on_drop = false;
        self.temp_path.take()
    }

    /// Get the temp file path (if created).
    pub fn temp_path(&self) -> Option<&Path> {
        self.temp_path.as_deref()
    }
}

impl Drop for TempFileManager {
    fn drop(&mut self) {
        if !self.cleanup_on_drop {
            return;
        }

        if let Some(temp_path) = &self.temp_path {
            if let Err(e) = std::fs::remove_file(temp_path) {
                tracing::warn!(
                    temp = %temp_path.display(),
                    error = %e,
                    "Failed to clean up temp file"
                );
            } else {
                tracing::debug!(temp = %temp_path.display(), "Cleaned up temp file");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use roboflow_storage::LocalStorage;
    use std::fs;
    use std::io::Write;

    #[test]
    fn test_local_storage_fast_path() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(LocalStorage::new(temp_dir.path())) as Arc<dyn Storage>;

        // Create a test file
        let test_file = temp_dir.path().join("test.mcap");
        let mut file = fs::File::create(&test_file).unwrap();
        file.write_all(b"test content").unwrap();

        // Create manager with relative path
        let relative_path = Path::new("test.mcap");
        let manager =
            TempFileManager::new(storage.clone(), relative_path, temp_dir.path()).unwrap();

        // Should use original path directly (no temp file)
        assert_eq!(manager.path(), &test_file);
        assert!(!manager.is_temp());
        assert!(manager.temp_path().is_none());
    }

    #[test]
    fn test_temp_file_cleanup() {
        let input_dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(LocalStorage::new(input_dir.path())) as Arc<dyn Storage>;

        // Create a test file in a different location (simulating cloud storage)
        let test_file = input_dir.path().join("remote.mcap");
        let mut file = fs::File::create(&test_file).unwrap();
        file.write_all(b"remote content").unwrap();

        // Create temp dir for downloads
        let temp_dir = tempfile::tempdir().unwrap();

        // Since LocalStorage takes the fast path, it doesn't create a temp file
        // This test verifies the fast path behavior
        let mut manager =
            TempFileManager::new(storage, Path::new("remote.mcap"), temp_dir.path()).unwrap();

        // For LocalStorage, it should use the fast path (no temp file)
        assert!(!manager.is_temp());

        // Verify retain returns None for fast path (no temp file created)
        let retained_path = manager.retain();
        assert!(
            retained_path.is_none(),
            "retain should return None for LocalStorage"
        );
    }

    #[test]
    fn test_retain_prevents_cleanup() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(LocalStorage::new(temp_dir.path())) as Arc<dyn Storage>;

        let test_file = temp_dir.path().join("retain_test.mcap");
        let mut file = fs::File::create(&test_file).unwrap();
        file.write_all(b"retain test").unwrap();

        // Create manager and get the path
        let mut manager =
            TempFileManager::new(storage, Path::new("retain_test.mcap"), temp_dir.path()).unwrap();

        // For LocalStorage, retain returns None (no temp file created)
        let retained_path = manager.retain();
        assert!(
            retained_path.is_none(),
            "retain should return None for LocalStorage fast path"
        );
    }
}
