// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! File discovery functionality for storage backends.
//!
//! This module provides utilities for discovering files matching patterns
//! across both local filesystem and cloud storage backends. It supports
//! glob-style wildcards like `*.parquet`, `episode_*.mp4`, etc.
//!
//! # Example
//!
//! ```
//! use roboflow_storage::{Storage, LocalStorage, discover_files};
//! use std::path::Path;
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let storage = LocalStorage::new("/tmp/data");
//!
//! // Discover all parquet files in a directory
//! let files = discover_files(&storage, Path::new("."), "*.parquet")?;
//! for file in files {
//!     println!("Found: {} ({} bytes)", file.path, file.size);
//! }
//! # Ok(())
//! # }
//! ```

use std::path::Path;

use crate::error::{StorageError, StorageResult};
use crate::metadata::ObjectMetadata;
use crate::traits::Storage;

/// Discover files matching a pattern in the given storage.
///
/// This function works with both local storage (using glob patterns) and
/// cloud storage (using list + pattern matching). For local storage, it
/// uses the glob crate for efficient pattern matching. For cloud storage,
/// it lists objects with the given prefix and filters by pattern.
///
/// # Arguments
///
/// * `storage` - The storage backend to search
/// * `directory` - The directory/prefix to search in
/// * `pattern` - The glob pattern to match (e.g., "*.parquet", "episode_*.mp4")
///
/// # Returns
///
/// Returns a vector of `ObjectMetadata` for all matching files.
///
/// # Errors
///
/// Returns `StorageError::InvalidPath` if the pattern is invalid.
/// Returns `StorageError::Other` if listing fails.
///
/// # Examples
///
/// ```
/// use roboflow_storage::{Storage, discover_files};
/// use roboflow_storage::mock::MockStorage;
/// use std::path::Path;
///
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let storage = MockStorage::with_data(vec![
///     ("data/file1.parquet", b"content1"),
///     ("data/file2.parquet", b"content2"),
///     ("data/file3.txt", b"content3"),
/// ]);
///
/// let parquet_files = discover_files(&storage, Path::new("data"), "*.parquet")?;
/// assert_eq!(parquet_files.len(), 2);
/// # Ok(())
/// # }
/// ```
pub fn discover_files(
    storage: &dyn Storage,
    directory: &Path,
    pattern: &str,
) -> StorageResult<Vec<ObjectMetadata>> {
    // For local storage, try to use glob directly for better performance
    if let Some(local) = storage.as_any().downcast_ref::<crate::LocalStorage>() {
        return discover_local_files(local, directory, pattern);
    }

    // For other storage types, use list + filter
    discover_cloud_files(storage, directory, pattern)
}

/// Discover files in local storage using glob patterns.
///
/// This is more efficient than listing and filtering because glob
/// can traverse the filesystem directly with pattern matching.
fn discover_local_files(
    storage: &crate::LocalStorage,
    directory: &Path,
    pattern: &str,
) -> StorageResult<Vec<ObjectMetadata>> {
    let root = storage.root();
    let search_path = root.join(directory).join(pattern);
    let search_pattern = search_path.to_string_lossy();

    let mut results = Vec::new();

    match glob::glob(&search_pattern) {
        Ok(paths) => {
            for entry in paths {
                match entry {
                    Ok(path) => {
                        // Get metadata for the file
                        if let Ok(metadata) = std::fs::metadata(&path)
                            && metadata.is_file()
                        {
                            // Convert absolute path back to relative path
                            let relative_path = path
                                .strip_prefix(root)
                                .unwrap_or(&path)
                                .to_string_lossy()
                                .to_string();

                            results.push(ObjectMetadata {
                                path: relative_path,
                                size: metadata.len(),
                                last_modified: metadata.modified().ok(),
                                content_type: None,
                                is_dir: false,
                            });
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Error reading glob entry: {}", e);
                    }
                }
            }
        }
        Err(e) => {
            return Err(StorageError::invalid_path(format!(
                "Invalid glob pattern '{}': {}",
                pattern, e
            )));
        }
    }

    Ok(results)
}

/// Discover files in cloud storage using list + pattern matching.
///
/// Lists all objects with the given prefix and filters by pattern.
fn discover_cloud_files(
    storage: &dyn Storage,
    directory: &Path,
    pattern: &str,
) -> StorageResult<Vec<ObjectMetadata>> {
    // List all objects in the directory
    let all_objects = storage.list(directory)?;

    // Filter by pattern
    let mut results = Vec::new();
    for obj in all_objects {
        // Skip directories
        if obj.is_dir {
            continue;
        }

        // Extract filename from path
        let path = Path::new(&obj.path);
        let filename = path
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| obj.path.clone());

        // Check if filename matches pattern
        if matches_pattern(&filename, pattern) {
            results.push(obj);
        }
    }

    Ok(results)
}

/// Check if a filename matches a glob pattern.
///
/// Supports wildcards:
/// - `*` - matches any sequence of characters
/// - `?` - matches any single character
/// - `[abc]` - matches any character in the set
/// - `[!abc]` - matches any character not in the set
///
/// # Arguments
///
/// * `filename` - The filename to check
/// * `pattern` - The glob pattern to match against
///
/// # Returns
///
/// `true` if the filename matches the pattern, `false` otherwise.
///
/// # Examples
///
/// ```
/// use roboflow_storage::discovery::matches_pattern;
///
/// assert!(matches_pattern("file.parquet", "*.parquet"));
/// assert!(matches_pattern("episode_001.mp4", "episode_*.mp4"));
/// assert!(!matches_pattern("file.txt", "*.parquet"));
/// assert!(matches_pattern("data.csv", "*.csv"));
/// ```
pub fn matches_pattern(filename: &str, pattern: &str) -> bool {
    // Handle simple prefix/suffix patterns efficiently
    if pattern == "*" || pattern == "*.*" {
        return true;
    }

    // Use glob to match the pattern
    // We wrap the pattern in a way that makes it match the entire filename
    let full_pattern = if pattern.starts_with('*') || pattern.contains('/') {
        pattern.to_string()
    } else {
        format!("*/{}", pattern)
    };

    // Create a glob pattern and check if filename matches
    match glob::Pattern::new(&full_pattern) {
        Ok(glob_pattern) => {
            // Try matching with and without a path prefix
            if glob_pattern.matches(filename) {
                return true;
            }
            // Also try matching with a dummy path prefix
            let with_prefix = format!("./{}", filename);
            if glob_pattern.matches(&with_prefix) {
                return true;
            }
            false
        }
        Err(_) => {
            // If pattern is invalid, do simple string matching
            simple_pattern_match(filename, pattern)
        }
    }
}

/// Simple pattern matching as a fallback.
///
/// Only supports `*` wildcard at the start or end of the pattern.
fn simple_pattern_match(filename: &str, pattern: &str) -> bool {
    if let Some(middle) = pattern.strip_prefix('*').and_then(|s| s.strip_suffix('*')) {
        // *middle*
        filename.contains(middle)
    } else if let Some(suffix) = pattern.strip_prefix('*') {
        // *suffix
        filename.ends_with(suffix)
    } else if let Some(prefix) = pattern.strip_suffix('*') {
        // prefix*
        filename.starts_with(prefix)
    } else if pattern.contains('*') {
        // prefix*suffix
        let parts: Vec<&str> = pattern.split('*').collect();
        if parts.len() == 2 {
            filename.starts_with(parts[0]) && filename.ends_with(parts[1])
        } else {
            false
        }
    } else {
        // No wildcards - exact match
        filename == pattern
    }
}

/// Check if a string contains glob wildcards.
///
/// # Arguments
///
/// * `pattern` - The string to check
///
/// # Returns
///
/// `true` if the string contains any glob wildcards (`*`, `?`, `[`), `false` otherwise.
///
/// # Examples
///
/// ```
/// use roboflow_storage::discovery::is_glob_pattern;
///
/// assert!(is_glob_pattern("*.parquet"));
/// assert!(is_glob_pattern("file?.txt"));
/// assert!(is_glob_pattern("data[0-9].csv"));
/// assert!(!is_glob_pattern("file.txt"));
/// ```
pub fn is_glob_pattern(pattern: &str) -> bool {
    pattern.contains('*') || pattern.contains('?') || pattern.contains('[')
}

/// Extension trait for Storage to provide discovery methods.
///
/// This trait adds convenient discovery methods to any storage implementation.
///
/// # Example
///
/// ```
/// use roboflow_storage::{Storage, discovery::DiscoveryExt};
/// use roboflow_storage::mock::MockStorage;
/// use std::path::Path;
///
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let storage = MockStorage::with_data(vec![
///     ("data/file1.parquet", b"content1"),
///     ("data/file2.parquet", b"content2"),
/// ]);
///
/// let files = storage.discover(Path::new("data"), "*.parquet")?;
/// assert_eq!(files.len(), 2);
/// # Ok(())
/// # }
/// ```
pub trait DiscoveryExt: Storage {
    /// Discover files matching a pattern.
    ///
    /// See [`discover_files`] for details.
    fn discover(&self, directory: &Path, pattern: &str) -> StorageResult<Vec<ObjectMetadata>>
    where
        Self: Sized,
    {
        discover_files(self, directory, pattern)
    }

    /// Check if a filename would match a given pattern.
    ///
    /// See [`matches_pattern`] for details.
    fn matches_pattern(&self, filename: &str, pattern: &str) -> bool {
        crate::discovery::matches_pattern(filename, pattern)
    }
}

// Implement DiscoveryExt for all types that implement Storage
impl<T: Storage + ?Sized> DiscoveryExt for T {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::MockStorage;

    // =============================================================================
    // Pattern Matching Tests
    // =============================================================================

    #[test]
    fn test_matches_pattern_wildcard_suffix() {
        assert!(matches_pattern("file.parquet", "*.parquet"));
        assert!(matches_pattern("data.parquet", "*.parquet"));
        assert!(matches_pattern("my.file.parquet", "*.parquet"));
        assert!(!matches_pattern("file.txt", "*.parquet"));
        assert!(!matches_pattern("parquet.file", "*.parquet"));
    }

    #[test]
    fn test_matches_pattern_wildcard_prefix() {
        assert!(matches_pattern("episode_001.mp4", "episode_*.mp4"));
        assert!(matches_pattern("episode_123.mp4", "episode_*.mp4"));
        assert!(!matches_pattern("episode_001.txt", "episode_*.mp4"));
        assert!(!matches_pattern("other_001.mp4", "episode_*.mp4"));
    }

    #[test]
    fn test_matches_pattern_both_wildcards() {
        assert!(matches_pattern(
            "data_backup_2024.parquet",
            "*backup*.parquet"
        ));
        assert!(!matches_pattern("data_2024.parquet", "*backup*.parquet"));
    }

    #[test]
    fn test_matches_pattern_no_wildcards() {
        assert!(matches_pattern("file.txt", "file.txt"));
        assert!(!matches_pattern("file.txt", "other.txt"));
    }

    #[test]
    fn test_matches_pattern_wildcard_only() {
        assert!(matches_pattern("anything.txt", "*"));
        assert!(matches_pattern("", "*"));
    }

    #[test]
    fn test_is_glob_pattern() {
        assert!(is_glob_pattern("*.parquet"));
        assert!(is_glob_pattern("file?.txt"));
        assert!(is_glob_pattern("[abc].txt"));
        assert!(is_glob_pattern("data[0-9].csv"));
        assert!(!is_glob_pattern("file.txt"));
        assert!(!is_glob_pattern("data.csv"));
    }

    // =============================================================================
    // MockStorage Discovery Tests
    // =============================================================================

    #[test]
    fn test_discover_files_mock_storage() {
        let storage = MockStorage::with_data(vec![
            ("data/file1.parquet", b"content1"),
            ("data/file2.parquet", b"content2"),
            ("data/file3.txt", b"content3"),
            ("data/nested/file4.parquet", b"content4"),
        ]);

        let files = discover_files(&storage, Path::new("data"), "*.parquet").unwrap();

        // MockStorage list returns all items with the prefix, including nested
        assert_eq!(files.len(), 3);
        let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
        assert!(paths.contains(&"data/file1.parquet"));
        assert!(paths.contains(&"data/file2.parquet"));
        assert!(paths.contains(&"data/nested/file4.parquet"));
    }

    #[test]
    fn test_discover_files_no_matches() {
        let storage = MockStorage::with_data(vec![
            ("data/file1.txt", b"content1"),
            ("data/file2.txt", b"content2"),
        ]);

        let files = discover_files(&storage, Path::new("data"), "*.parquet").unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn test_discover_files_empty_storage() {
        let storage = MockStorage::new();
        let files = discover_files(&storage, Path::new("data"), "*.parquet").unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn test_discover_files_with_prefix_pattern() {
        let storage = MockStorage::with_data(vec![
            ("episodes/episode_001.mp4", b"content1"),
            ("episodes/episode_002.mp4", b"content2"),
            ("episodes/other_001.mp4", b"content3"),
        ]);

        let files = discover_files(&storage, Path::new("episodes"), "episode_*.mp4").unwrap();

        assert_eq!(files.len(), 2);
        let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
        assert!(paths.contains(&"episodes/episode_001.mp4"));
        assert!(paths.contains(&"episodes/episode_002.mp4"));
    }

    #[test]
    fn test_discover_files_skips_directories() {
        // Note: MockStorage doesn't have real directories, but we can test
        // the filtering logic by creating a custom storage that returns dirs
        let storage = MockStorage::with_data(vec![
            ("data/file1.parquet", b"content1"),
            ("data/file2.parquet", b"content2"),
        ]);

        let files = discover_files(&storage, Path::new("data"), "*.parquet").unwrap();

        // All results should be files (not directories)
        for file in &files {
            assert!(!file.is_dir);
        }
    }

    // =============================================================================
    // DiscoveryExt Trait Tests
    // =============================================================================

    #[test]
    fn test_discovery_ext() {
        let storage = MockStorage::with_data(vec![
            ("data/file1.parquet", b"content1"),
            ("data/file2.parquet", b"content2"),
        ]);

        // Test discover method from trait
        let files = storage.discover(Path::new("data"), "*.parquet").unwrap();
        assert_eq!(files.len(), 2);

        // Test matches_pattern method from trait
        assert!(storage.matches_pattern("file.parquet", "*.parquet"));
        assert!(!storage.matches_pattern("file.txt", "*.parquet"));
    }

    // =============================================================================
    // Local Storage Discovery Tests
    // =============================================================================

    #[test]
    fn test_discover_local_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = crate::LocalStorage::new(temp_dir.path());

        // Create test files
        std::fs::write(temp_dir.path().join("file1.parquet"), b"content1").unwrap();
        std::fs::write(temp_dir.path().join("file2.parquet"), b"content2").unwrap();
        std::fs::write(temp_dir.path().join("file3.txt"), b"content3").unwrap();

        // Create subdirectory with more files
        std::fs::create_dir(temp_dir.path().join("subdir")).unwrap();
        std::fs::write(
            temp_dir.path().join("subdir").join("file4.parquet"),
            b"content4",
        )
        .unwrap();

        // Test discovering parquet files in root
        // Note: *.parquet doesn't recurse into subdirectories (use **/*.parquet for that)
        let files = discover_files(&storage, Path::new("."), "*.parquet").unwrap();
        assert_eq!(files.len(), 2); // Only files in root directory

        // Verify all files are parquet
        for file in &files {
            assert!(file.path.ends_with(".parquet"));
            assert!(!file.is_dir);
        }
    }

    #[test]
    fn test_discover_local_files_in_subdirectory() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = crate::LocalStorage::new(temp_dir.path());

        // Create nested directory structure
        std::fs::create_dir_all(temp_dir.path().join("data").join("2024")).unwrap();
        std::fs::write(
            temp_dir
                .path()
                .join("data")
                .join("2024")
                .join("file1.parquet"),
            b"content1",
        )
        .unwrap();
        std::fs::write(
            temp_dir
                .path()
                .join("data")
                .join("2024")
                .join("file2.parquet"),
            b"content2",
        )
        .unwrap();

        // Discover files in subdirectory
        let files = discover_files(&storage, Path::new("data/2024"), "*.parquet").unwrap();
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn test_discover_local_files_no_matches() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = crate::LocalStorage::new(temp_dir.path());

        // Create only txt files
        std::fs::write(temp_dir.path().join("file1.txt"), b"content1").unwrap();
        std::fs::write(temp_dir.path().join("file2.txt"), b"content2").unwrap();

        // Try to discover parquet files
        let files = discover_files(&storage, Path::new("."), "*.parquet").unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn test_discover_local_files_with_prefix_pattern() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = crate::LocalStorage::new(temp_dir.path());

        // Create episode files
        std::fs::write(temp_dir.path().join("episode_001.mp4"), b"episode 1").unwrap();
        std::fs::write(temp_dir.path().join("episode_002.mp4"), b"episode 2").unwrap();
        std::fs::write(temp_dir.path().join("other_001.mp4"), b"other").unwrap();

        let files = discover_files(&storage, Path::new("."), "episode_*.mp4").unwrap();
        assert_eq!(files.len(), 2);

        let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
        assert!(paths.iter().any(|p| p.contains("episode_001.mp4")));
        assert!(paths.iter().any(|p| p.contains("episode_002.mp4")));
    }

    #[test]
    fn test_discover_returns_metadata() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = crate::LocalStorage::new(temp_dir.path());

        let content = b"test content";
        std::fs::write(temp_dir.path().join("test.parquet"), content).unwrap();

        let files = discover_files(&storage, Path::new("."), "*.parquet").unwrap();
        assert_eq!(files.len(), 1);

        let file = &files[0];
        assert!(file.path.contains("test.parquet"));
        assert_eq!(file.size, content.len() as u64);
        assert!(!file.is_dir);
        assert!(file.last_modified.is_some());
    }
}
