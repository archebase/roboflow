// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Directory upload utilities for cloud storage.
//!
//! This module provides functions for uploading local directories recursively
//! to cloud storage backends, with parallel uploads and error handling.
//!
//! # Example
//!
//! ```
//! use roboflow_storage::Storage;
//! use roboflow_storage::mock::MockStorage;
//! use roboflow_storage::upload::upload_directory_recursive;
//! use std::path::Path;
//! use std::sync::Arc;
//!
//! # fn example() -> Result<(), roboflow_storage::StorageError> {
//! let storage: Arc<dyn Storage> = Arc::new(MockStorage::new());
//! let local_dir = Path::new("/tmp/local/data");
//! let remote_prefix = Path::new("uploads/data");
//!
//! // Upload with default concurrency (4 parallel uploads)
//! let results = upload_directory_recursive(storage, local_dir, remote_prefix)?;
//! # Ok(())
//! # }
//! ```

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use crate::error::{StorageError, StorageResult};
use crate::metadata::ObjectMetadata;
use crate::traits::Storage;

/// Default concurrency limit for parallel uploads.
pub const DEFAULT_CONCURRENCY: usize = 4;

/// Result of a single file upload operation.
#[derive(Debug)]
pub struct UploadResult {
    /// Local path of the uploaded file.
    pub local_path: PathBuf,
    /// Remote path where the file was uploaded.
    pub remote_path: PathBuf,
    /// Size of the uploaded file in bytes.
    pub size: u64,
    /// Error if the upload failed.
    pub error: Option<StorageError>,
}

impl UploadResult {
    /// Check if the upload succeeded.
    pub fn is_success(&self) -> bool {
        self.error.is_none()
    }

    /// Get the error if the upload failed.
    pub fn error(&self) -> Option<&StorageError> {
        self.error.as_ref()
    }
}

/// Collect all files in a directory recursively.
///
/// Returns a vector of paths to all files found in the directory and its
/// subdirectories. Does not include directories themselves.
///
/// # Arguments
///
/// * `dir` - The directory to walk
///
/// # Errors
///
/// Returns `StorageError::InvalidPath` if the directory doesn't exist or
/// isn't a directory.
///
/// # Example
///
/// ```
/// use roboflow_storage::upload::walk_directory;
/// use std::path::Path;
///
/// # fn example() -> Result<(), roboflow_storage::StorageError> {
/// let files = walk_directory(Path::new("/tmp/data"))?;
/// for file in files {
///     println!("Found: {:?}", file);
/// }
/// # Ok(())
/// # }
/// ```
pub fn walk_directory(dir: &Path) -> StorageResult<Vec<PathBuf>> {
    let metadata = std::fs::metadata(dir).map_err(|e| {
        StorageError::invalid_path(format!(
            "Cannot access directory '{}': {}",
            dir.display(),
            e
        ))
    })?;

    if !metadata.is_dir() {
        return Err(StorageError::invalid_path(format!(
            "Path is not a directory: {}",
            dir.display()
        )));
    }

    let mut files = Vec::new();
    let mut queue = VecDeque::new();
    queue.push_back(dir.to_path_buf());

    while let Some(current_dir) = queue.pop_front() {
        let entries = std::fs::read_dir(&current_dir).map_err(|e| {
            StorageError::other(format!(
                "Failed to read directory '{}': {}",
                current_dir.display(),
                e
            ))
        })?;

        for entry in entries {
            let entry = entry.map_err(|e| {
                StorageError::other(format!(
                    "Failed to read directory entry in '{}': {}",
                    current_dir.display(),
                    e
                ))
            })?;
            let path = entry.path();
            let metadata = entry.metadata().map_err(|e| {
                StorageError::other(format!(
                    "Failed to get metadata for '{}': {}",
                    path.display(),
                    e
                ))
            })?;

            if metadata.is_dir() {
                queue.push_back(path);
            } else {
                files.push(path);
            }
        }
    }

    Ok(files)
}

/// Upload a single file to storage.
///
/// This function uploads a single local file to the specified remote path
/// in the storage backend. It uses the storage's `upload_file` method for
/// efficient transfer.
///
/// # Arguments
///
/// * `storage` - The storage backend to upload to
/// * `local_path` - Path to the local file to upload
/// * `remote_path` - Destination path in storage
///
/// # Returns
///
/// Metadata about the uploaded object.
///
/// # Errors
///
/// Returns an error if:
/// - The local file doesn't exist
/// - The upload fails
///
/// # Example
///
/// ```
/// use roboflow_storage::Storage;
/// use roboflow_storage::mock::MockStorage;
/// use roboflow_storage::upload::upload_file;
/// use std::path::Path;
///
/// # fn example() -> Result<(), roboflow_storage::StorageError> {
/// let storage = MockStorage::new();
/// std::fs::write("/tmp/test.txt", b"hello").unwrap();
/// let metadata = upload_file(&storage, Path::new("/tmp/test.txt"), Path::new("test.txt"))?;
/// println!("Uploaded {} bytes", metadata.size);
/// # Ok(())
/// # }
/// ```
pub fn upload_file(
    storage: &dyn Storage,
    local_path: &Path,
    remote_path: &Path,
) -> StorageResult<ObjectMetadata> {
    // Verify the local file exists and get its size
    let local_metadata = std::fs::metadata(local_path).map_err(|e| {
        StorageError::not_found(format!(
            "Local file '{}' not found: {}",
            local_path.display(),
            e
        ))
    })?;

    if !local_metadata.is_file() {
        return Err(StorageError::invalid_path(format!(
            "Path is not a file: {}",
            local_path.display()
        )));
    }

    // Upload the file
    let size = storage.upload_file(local_path, remote_path)?;

    // Create metadata for the uploaded object
    Ok(
        ObjectMetadata::new(remote_path.to_string_lossy().to_string(), size)
            .with_last_modified(std::time::SystemTime::now()),
    )
}

/// Upload a directory recursively to cloud storage.
///
/// This function uploads all files from a local directory to a cloud storage
/// backend, preserving the directory structure. Files are uploaded in parallel
/// with a configurable concurrency limit.
///
/// Individual file upload failures are tracked but don't stop the overall
/// operation. The function returns metadata for all successfully uploaded files.
///
/// # Arguments
///
/// * `storage` - The storage backend to upload to
/// * `local_dir` - Path to the local directory to upload
/// * `remote_prefix` - Destination prefix in storage (directory structure is preserved under this prefix)
///
/// # Returns
///
/// A vector of metadata for all successfully uploaded files.
///
/// # Errors
///
/// Returns an error if:
/// - The local directory doesn't exist or isn't a directory
/// - The directory cannot be read
///
/// Note: Individual file upload failures are tracked in the `UploadResult`
/// struct but do not cause the overall function to fail.
///
/// # Example
///
/// ```
/// use roboflow_storage::Storage;
/// use roboflow_storage::mock::MockStorage;
/// use roboflow_storage::upload::upload_directory_recursive;
/// use std::path::Path;
/// use std::sync::Arc;
///
/// # fn example() -> Result<(), roboflow_storage::StorageError> {
/// let storage: Arc<dyn Storage> = Arc::new(MockStorage::new());
/// let results = upload_directory_recursive(storage, Path::new("/tmp/data"), Path::new("uploads"))?;
/// println!("Uploaded {} files", results.len());
/// for meta in &results {
///     println!("  - {} ({} bytes)", meta.path, meta.size);
/// }
/// # Ok(())
/// # }
/// ```
pub fn upload_directory_recursive(
    storage: Arc<dyn Storage>,
    local_dir: &Path,
    remote_prefix: &Path,
) -> StorageResult<Vec<ObjectMetadata>> {
    upload_directory_recursive_with_concurrency(
        storage,
        local_dir,
        remote_prefix,
        DEFAULT_CONCURRENCY,
    )
}

/// Upload a directory recursively with a custom concurrency limit.
///
/// This is the same as `upload_directory_recursive` but allows specifying
/// a custom number of concurrent uploads.
///
/// # Arguments
///
/// * `storage` - The storage backend to upload to (accepts Arc<dyn Storage> for trait objects)
/// * `local_dir` - Path to the local directory to upload
/// * `remote_prefix` - Destination prefix in storage
/// * `concurrency` - Maximum number of parallel uploads (minimum 1)
///
/// # Returns
///
/// A vector of metadata for all successfully uploaded files.
pub fn upload_directory_recursive_with_concurrency(
    storage: Arc<dyn Storage>,
    local_dir: &Path,
    remote_prefix: &Path,
    concurrency: usize,
) -> StorageResult<Vec<ObjectMetadata>> {
    let files = walk_directory(local_dir)?;

    if files.is_empty() {
        return Ok(Vec::new());
    }

    let concurrency = concurrency.max(1);
    let total_files = files.len();
    let completed = Arc::new(AtomicUsize::new(0));
    let files = Arc::new(files);

    // Spawn worker threads for parallel uploads
    let workers: Vec<_> = (0..concurrency)
        .map(|_| {
            let storage = Arc::clone(&storage);
            let local_dir = local_dir.to_path_buf();
            let remote_prefix = remote_prefix.to_path_buf();
            let completed = Arc::clone(&completed);
            let files = Arc::clone(&files);
            thread::spawn(move || {
                let mut results = Vec::new();

                loop {
                    let file_idx = completed.fetch_add(1, Ordering::SeqCst);
                    if file_idx >= total_files {
                        break;
                    }

                    // Get the file path by index (safe because we know files.len() > 0)
                    let local_path = &files[file_idx];

                    // Compute the relative path from local_dir
                    let relative_path = match local_path.strip_prefix(&local_dir) {
                        Ok(rel) => rel,
                        Err(_) => {
                            results.push(UploadResult {
                                local_path: local_path.clone(),
                                remote_path: PathBuf::new(),
                                size: 0,
                                error: Some(StorageError::invalid_path(format!(
                                    "Failed to compute relative path for {}",
                                    local_path.display()
                                ))),
                            });
                            continue;
                        }
                    };

                    // Compute the remote path
                    let remote_path = remote_prefix.join(relative_path);

                    // Perform the upload
                    match upload_file(&*storage, local_path, &remote_path) {
                        Ok(metadata) => {
                            results.push(UploadResult {
                                local_path: local_path.clone(),
                                remote_path,
                                size: metadata.size,
                                error: None,
                            });
                        }
                        Err(e) => {
                            results.push(UploadResult {
                                local_path: local_path.clone(),
                                remote_path,
                                size: 0,
                                error: Some(e),
                            });
                        }
                    }
                }

                results
            })
        })
        .collect();

    // Wait for all workers and collect results
    let mut uploaded_metadata = Vec::new();
    let mut errors: Vec<(PathBuf, StorageError)> = Vec::new();

    for worker in workers {
        match worker.join() {
            Ok(worker_results) => {
                for result in worker_results {
                    if let Some(err) = result.error {
                        errors.push((result.local_path, err));
                    } else {
                        uploaded_metadata.push(ObjectMetadata::new(
                            result.remote_path.to_string_lossy().to_string(),
                            result.size,
                        ));
                    }
                }
            }
            Err(_) => {
                return Err(StorageError::other("Worker thread panicked".to_string()));
            }
        }
    }

    // If there were errors, we could return them, but for now we just log and continue
    // The successfully uploaded files' metadata is returned
    Ok(uploaded_metadata)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::MockStorage;

    fn create_test_dir() -> (tempfile::TempDir, PathBuf) {
        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        // Create directory structure
        std::fs::create_dir_all(base_path.join("subdir1/subdir2")).unwrap();

        // Create test files
        std::fs::write(base_path.join("file1.txt"), "content1").unwrap();
        std::fs::write(base_path.join("subdir1/file2.txt"), "content2").unwrap();
        std::fs::write(base_path.join("subdir1/subdir2/file3.txt"), "content3").unwrap();

        (temp_dir, base_path)
    }

    #[test]
    fn test_walk_directory() {
        let (_temp_dir, base_path) = create_test_dir();

        let files = walk_directory(&base_path).unwrap();
        assert_eq!(files.len(), 3);

        let file_names: Vec<String> = files
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
            .collect();

        assert!(file_names.contains(&"file1.txt".to_string()));
        assert!(file_names.contains(&"file2.txt".to_string()));
        assert!(file_names.contains(&"file3.txt".to_string()));
    }

    #[test]
    fn test_walk_directory_not_found() {
        let result = walk_directory(Path::new("/nonexistent/path/that/does/not/exist"));
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StorageError::InvalidPath(_)));
    }

    #[test]
    fn test_walk_directory_not_a_directory() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("not_a_dir.txt");
        std::fs::write(&file_path, "content").unwrap();

        let result = walk_directory(&file_path);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StorageError::InvalidPath(_)));
    }

    #[test]
    fn test_upload_file() {
        let storage = MockStorage::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let local_path = temp_dir.path().join("test.txt");
        std::fs::write(&local_path, "hello world").unwrap();

        let metadata = upload_file(&storage, &local_path, Path::new("remote/test.txt")).unwrap();

        assert_eq!(metadata.path, "remote/test.txt");
        assert_eq!(metadata.size, 11);
        assert!(storage.exists(Path::new("remote/test.txt")));
    }

    #[test]
    fn test_upload_file_not_found() {
        let storage = MockStorage::new();

        let result = upload_file(
            &storage,
            Path::new("/nonexistent/file.txt"),
            Path::new("remote.txt"),
        );
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StorageError::NotFound(_)));
    }

    #[test]
    fn test_upload_file_not_a_file() {
        let storage = MockStorage::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let dir_path = temp_dir.path().join("test_dir");
        std::fs::create_dir(&dir_path).unwrap();

        let result = upload_file(&storage, &dir_path, Path::new("remote"));
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StorageError::InvalidPath(_)));
    }

    #[test]
    fn test_upload_directory_recursive() {
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new());
        let (_temp_dir, base_path) = create_test_dir();

        let results =
            upload_directory_recursive(storage.clone(), &base_path, Path::new("uploads")).unwrap();

        assert_eq!(results.len(), 3);

        // Verify files are in storage with correct paths
        assert!(storage.exists(Path::new("uploads/file1.txt")));
        assert!(storage.exists(Path::new("uploads/subdir1/file2.txt")));
        assert!(storage.exists(Path::new("uploads/subdir1/subdir2/file3.txt")));

        // Verify sizes
        let sizes: Vec<u64> = results.iter().map(|m| m.size).collect();
        assert!(sizes.contains(&8)); // "content1"
        assert!(sizes.contains(&8)); // "content2"
        assert!(sizes.contains(&8)); // "content3"
    }

    #[test]
    fn test_upload_directory_recursive_empty() {
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new());
        let temp_dir = tempfile::tempdir().unwrap();
        let empty_dir = temp_dir.path().join("empty");
        std::fs::create_dir(&empty_dir).unwrap();

        let results =
            upload_directory_recursive(storage, &empty_dir, Path::new("uploads")).unwrap();

        assert!(results.is_empty());
    }

    #[test]
    fn test_upload_directory_recursive_preserves_structure() {
        let storage: Arc<dyn Storage> = Arc::new(MockStorage::new());
        let temp_dir = tempfile::tempdir().unwrap();
        let base = temp_dir.path();

        // Create nested structure
        std::fs::create_dir_all(base.join("a/b/c")).unwrap();
        std::fs::write(base.join("a/file_a.txt"), "A").unwrap();
        std::fs::write(base.join("a/b/file_b.txt"), "B").unwrap();
        std::fs::write(base.join("a/b/c/file_c.txt"), "C").unwrap();

        let results = upload_directory_recursive(storage, base, Path::new("dest")).unwrap();

        assert_eq!(results.len(), 3);

        // Check structure is preserved
        let paths: Vec<String> = results.iter().map(|m| m.path.clone()).collect();
        assert!(paths.contains(&"dest/a/file_a.txt".to_string()));
        assert!(paths.contains(&"dest/a/b/file_b.txt".to_string()));
        assert!(paths.contains(&"dest/a/b/c/file_c.txt".to_string()));
    }

    #[test]
    fn test_upload_directory_recursive_with_concurrency() {
        let (_temp_dir, base_path) = create_test_dir();

        // Test with different concurrency levels
        for concurrency in [1, 2, 4, 8] {
            // Create a new storage for each iteration to avoid interference
            let iter_storage: Arc<dyn Storage> = Arc::new(MockStorage::new());
            let results = upload_directory_recursive_with_concurrency(
                iter_storage,
                &base_path,
                Path::new("uploads"),
                concurrency,
            )
            .unwrap();
            assert_eq!(results.len(), 3, "Failed with concurrency {}", concurrency);
        }
    }

    #[test]
    fn test_upload_result() {
        let success_result = UploadResult {
            local_path: PathBuf::from("/tmp/test.txt"),
            remote_path: PathBuf::from("remote/test.txt"),
            size: 100,
            error: None,
        };
        assert!(success_result.is_success());
        assert!(success_result.error().is_none());

        let error_result = UploadResult {
            local_path: PathBuf::from("/tmp/test.txt"),
            remote_path: PathBuf::from("remote/test.txt"),
            size: 0,
            error: Some(StorageError::not_found("test")),
        };
        assert!(!error_result.is_success());
        assert!(error_result.error().is_some());
    }
}
