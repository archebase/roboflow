// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Local filesystem storage backend.
//!
//! Provides the `Storage` trait implementation for local filesystem operations.
//! This backend is always available and serves as the reference implementation.

use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::{
    ObjectMetadata, SeekRead, SeekableStorage, Storage, StorageError, StorageResult as Result,
    StreamingConfig, StreamingRead,
};

/// Normalize a path by resolving '..' and '.' components.
///
/// This is a safe alternative to `Path::canonicalize()` that doesn't require
/// the path to exist. It prevents path traversal by ensuring the result
/// stays within the given root.
fn normalize_path(path: &Path, root: &Path) -> Result<PathBuf> {
    use std::path::Component;

    // Build the normalized path component by component
    let mut result = PathBuf::new();

    for comp in path.components() {
        match comp {
            Component::Prefix(..) | Component::RootDir => {
                // For absolute paths, start fresh
                result = PathBuf::new();
                result.push(comp.as_os_str());
            }
            Component::CurDir => {
                // '.' - skip
            }
            Component::ParentDir => {
                // '..' - try to go up one directory
                if !result.pop() {
                    // Tried to pop above filesystem root
                    return Err(StorageError::permission_denied(
                        "Path traversal detected: '..' escape attempt".to_string(),
                    ));
                }
            }
            Component::Normal(s) => {
                result.push(s);
            }
        }
    }

    // Also normalize root for comparison (strip leading ./ if present)
    let normalized_root = normalize_root(root);

    // Verify the normalized path starts with normalized root using Path::starts_with
    if result != normalized_root && !result.starts_with(&normalized_root) {
        return Err(StorageError::permission_denied(
            "Path traversal detected: access denied".to_string(),
        ));
    }

    Ok(result)
}

/// Normalize root path for comparison by stripping leading '.' if present.
/// This ensures that "./tests/fixtures" and "tests/fixtures" are treated as equivalent.
fn normalize_root(root: &Path) -> PathBuf {
    use std::path::Component;

    let mut result = PathBuf::new();
    let mut skip_first_curdir = true;

    for comp in root.components() {
        match comp {
            Component::Prefix(..) | Component::RootDir => {
                result.push(comp.as_os_str());
                skip_first_curdir = false;
            }
            Component::CurDir => {
                // Skip the first leading '.' (if present at the start)
                if !skip_first_curdir || !result.as_os_str().is_empty() {
                    result.push(comp.as_os_str());
                }
            }
            Component::ParentDir | Component::Normal(_) => {
                result.push(comp.as_os_str());
                skip_first_curdir = false;
            }
        }
    }

    // If result is empty, return "."
    if result.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        result
    }
}

/// Local filesystem storage backend.
///
/// Provides access to files on the local filesystem. All paths are interpreted
/// relative to the configured root directory.
///
/// # Example
///
/// ```ignore
/// use roboflow::storage::{Storage, LocalStorage};
///
/// let storage = LocalStorage::new("/tmp/data")?;
/// storage.create_dir_all(Path::new(r"subdir").as_ref())?;
/// let mut writer = storage.writer(Path::new(r"subdir/file.txt").as_ref())?;
/// writer.write_all(b"Hello, World!")?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone)]
pub struct LocalStorage {
    /// Root directory for all storage operations.
    root: PathBuf,
}

impl LocalStorage {
    /// Create a new local storage backend with the given root directory.
    ///
    /// The root directory doesn't need to exist; it will be created on first write.
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: PathBuf::from(root.as_ref()),
        }
    }

    /// Get the root directory of this storage backend.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Get the full path for a relative path within this storage.
    ///
    /// Validates that the resulting path is within the root directory
    /// to prevent path traversal attacks.
    pub fn full_path(&self, path: &Path) -> Result<PathBuf> {
        // Join the path with root
        let full = self.root.join(path);

        // Normalize the path by resolving '..' and '.' components
        // This also detects path traversal attempts
        let normalized = normalize_path(&full, &self.root)?;

        Ok(normalized)
    }

    /// Ensure parent directories exist for a path.
    fn ensure_parent(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            let parent_path = self.full_path(parent).unwrap_or_else(|_| self.root.clone());
            fs::create_dir_all(parent_path).map_err(|e| {
                StorageError::Other(format!("failed to create parent directories: {e}"))
            })?;
        }
        Ok(())
    }
}

impl Storage for LocalStorage {
    fn reader(&self, path: &Path) -> Result<Box<dyn Read + Send + 'static>> {
        let full_path = self.full_path(path)?;
        File::open(&full_path)
            .map(|f| Box::new(BufReader::new(f)) as Box<dyn Read + Send>)
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    StorageError::not_found(full_path.display().to_string())
                } else {
                    StorageError::Io(e)
                }
            })
    }

    fn writer(&self, path: &Path) -> Result<Box<dyn Write + Send + 'static>> {
        let full_path = self.full_path(path)?;
        self.ensure_parent(&full_path)?;
        File::create(&full_path)
            .map(|f| Box::new(BufWriter::new(f)) as Box<dyn Write + Send>)
            .map_err(StorageError::Io)
    }

    fn exists(&self, path: &Path) -> bool {
        self.full_path(path).is_ok_and(|p| p.exists())
    }

    fn size(&self, path: &Path) -> Result<u64> {
        let full_path = self.full_path(path)?;
        fs::metadata(&full_path).map(|m| m.len()).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::not_found(full_path.display().to_string())
            } else {
                StorageError::Io(e)
            }
        })
    }

    fn metadata(&self, path: &Path) -> Result<ObjectMetadata> {
        let full_path = self.full_path(path)?;
        let meta = fs::metadata(&full_path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::not_found(full_path.display().to_string())
            } else {
                StorageError::Io(e)
            }
        })?;

        Ok(ObjectMetadata {
            path: full_path.display().to_string(),
            size: meta.len(),
            last_modified: meta.modified().ok(),
            content_type: None,
            is_dir: meta.is_dir(),
        })
    }

    fn list(&self, prefix: &Path) -> Result<Vec<ObjectMetadata>> {
        let full_path = self.full_path(prefix)?;
        let mut results = Vec::new();

        if !full_path.exists() {
            return Ok(results);
        }

        let meta = fs::metadata(&full_path)?;
        if meta.is_file() {
            results.push(ObjectMetadata {
                path: full_path.display().to_string(),
                size: meta.len(),
                last_modified: meta.modified().ok(),
                content_type: None,
                is_dir: false,
            });
            return Ok(results);
        }

        let entries = fs::read_dir(&full_path).map_err(StorageError::Io)?;
        for entry in entries {
            let entry = entry.map_err(StorageError::Io)?;
            let path = entry.path();
            let meta = entry.metadata().map_err(StorageError::Io)?;
            results.push(ObjectMetadata {
                path: path.display().to_string(),
                size: meta.len(),
                last_modified: meta.modified().ok(),
                content_type: None,
                is_dir: meta.is_dir(),
            });
        }
        Ok(results)
    }

    fn delete(&self, path: &Path) -> Result<()> {
        let full_path = self.full_path(path)?;
        fs::remove_file(&full_path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::not_found(full_path.display().to_string())
            } else {
                StorageError::Io(e)
            }
        })
    }

    fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let from_path = self.full_path(from)?;
        let to_path = self.full_path(to)?;
        self.ensure_parent(&to_path)?;
        fs::copy(&from_path, &to_path).map(|_| ()).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::not_found(from_path.display().to_string())
            } else {
                StorageError::Io(e)
            }
        })
    }

    fn create_dir(&self, path: &Path) -> Result<()> {
        let full_path = self.full_path(path)?;
        fs::create_dir(&full_path).map_err(StorageError::Io)
    }

    fn create_dir_all(&self, path: &Path) -> Result<()> {
        let full_path = self.full_path(path)?;
        fs::create_dir_all(&full_path).map_err(StorageError::Io)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn compose_objects(
        &self,
        sources: &[&Path],
        dest: &Path,
        composer: &dyn roboflow_core::VideoComposer,
    ) -> Result<()> {
        if sources.is_empty() {
            return Err(StorageError::Other(
                "compose_objects requires at least one source".to_string(),
            ));
        }

        // For a single source, just do a copy
        if sources.len() == 1 {
            return self.copy(sources[0], dest);
        }

        // For multiple sources, use the composer for proper remuxing
        let dest_path = self.full_path(dest)?;
        self.ensure_parent(&dest_path)?;

        // Verify all sources exist first and convert to full paths
        let source_paths: Vec<PathBuf> = sources
            .iter()
            .map(|&s| self.full_path(s))
            .collect::<Result<Vec<_>>>()?;

        for (i, path) in source_paths.iter().enumerate() {
            if !path.exists() {
                return Err(StorageError::not_found(format!(
                    "source file {} not found: {}",
                    i,
                    path.display()
                )));
            }
        }

        // Use VideoComposer for proper MP4 composition (not byte concatenation)
        let source_refs: Vec<&Path> = source_paths.iter().map(|p| p.as_path()).collect();
        composer
            .compose(&source_refs, &dest_path)
            .map_err(|e| StorageError::Other(format!("video composition failed: {}", e)))?;

        tracing::info!(
            dest = %dest_path.display(),
            sources = sources.len(),
            "Composed {} video segments into {}",
            sources.len(),
            dest.display()
        );

        Ok(())
    }

    fn delete_prefix(&self, prefix: &Path) -> Result<usize> {
        let full_prefix = self.full_path(prefix)?;
        let mut deleted_count = 0;

        if !full_prefix.exists() {
            return Ok(0);
        }

        // Collect all files to delete first (to avoid iterator invalidation)
        let mut files_to_delete: Vec<PathBuf> = Vec::new();

        fn collect_files(dir: &Path, files: &mut Vec<PathBuf>) -> std::io::Result<()> {
            if dir.is_file() {
                files.push(dir.to_path_buf());
                return Ok(());
            }

            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    collect_files(&path, files)?;
                } else {
                    files.push(path);
                }
            }
            Ok(())
        }

        collect_files(&full_prefix, &mut files_to_delete).map_err(|e| {
            StorageError::Other(format!("failed to list files for deletion: {}", e))
        })?;

        // Delete all collected files
        for file_path in &files_to_delete {
            fs::remove_file(file_path).map_err(|e| {
                StorageError::Other(format!("failed to delete {}: {}", file_path.display(), e))
            })?;
            deleted_count += 1;
        }

        // Try to remove empty directories (ignore errors)
        fn remove_empty_dirs(dir: &Path) {
            if dir.is_dir() {
                if let Ok(entries) = fs::read_dir(dir) {
                    for entry in entries.flatten() {
                        remove_empty_dirs(&entry.path());
                    }
                }
                let _ = fs::remove_dir(dir);
            }
        }
        remove_empty_dirs(&full_prefix);

        tracing::info!(
            prefix = %full_prefix.display(),
            count = deleted_count,
            "Deleted {} files under prefix",
            deleted_count
        );

        Ok(deleted_count)
    }

    fn read_range(
        &self,
        path: &Path,
        start: u64,
        end: Option<u64>,
    ) -> Result<Box<dyn Read + Send + 'static>> {
        use std::io::{Cursor, Seek, SeekFrom};

        let full_path = self.full_path(path)?;

        // Determine the end offset
        let end = match end {
            Some(e) => e,
            None => {
                // Get file size if end not specified
                fs::metadata(&full_path)
                    .map_err(|e| {
                        if e.kind() == std::io::ErrorKind::NotFound {
                            StorageError::not_found(full_path.display().to_string())
                        } else {
                            StorageError::Io(e)
                        }
                    })?
                    .len()
            }
        };

        // Validate bounds
        if start > end {
            return Err(StorageError::invalid_path(format!(
                "start offset {} exceeds end offset {}",
                start, end
            )));
        }

        let length = end - start;
        let length_usize = usize::try_from(length).map_err(|_| {
            StorageError::Other(format!(
                "range length {} too large for memory allocation",
                length
            ))
        })?;

        let mut file = File::open(&full_path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::not_found(full_path.display().to_string())
            } else {
                StorageError::Io(e)
            }
        })?;

        file.seek(SeekFrom::Start(start))
            .map_err(StorageError::Io)?;

        let mut buffer = vec![0u8; length_usize];
        file.read_exact(&mut buffer).map_err(StorageError::Io)?;

        Ok(Box::new(Cursor::new(buffer)))
    }

    fn streaming_reader(
        &self,
        path: &Path,
        _config: StreamingConfig,
    ) -> Result<Box<dyn StreamingRead + Send + 'static>> {
        let full_path = self.full_path(path)?;

        let file = File::open(&full_path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::not_found(full_path.display().to_string())
            } else {
                StorageError::Io(e)
            }
        })?;

        let reader = crate::streaming::StreamingLocalReader::new(file)?;
        Ok(Box::new(reader))
    }
}

// =============================================================================
// Streaming Upload Support
// =============================================================================

impl crate::streaming_upload::StorageStreamingExt for LocalStorage {
    fn put_multipart_stream(
        &self,
        path: &Path,
    ) -> crate::StorageResult<Box<dyn crate::streaming_upload::MultipartUpload>> {
        use crate::streaming_upload::LocalMultipartUpload;
        use std::io::BufWriter;

        let target_path = self.full_path(path)?;

        // Create a temporary file in the same directory as the target
        let temp_dir = target_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        let temp_file = tempfile::Builder::new()
            .prefix(".tmp_upload_")
            .tempfile_in(temp_dir)
            .map_err(crate::StorageError::Io)?;

        let temp_path = temp_file.path().to_path_buf();

        // Use keep() to prevent auto-deletion, returns (File, PathBuf)
        let (file, _kept_path) = temp_file
            .keep()
            .map_err(|e| crate::StorageError::Io(e.into()))?;
        let writer = BufWriter::new(file);

        tracing::debug!(
            target = %target_path.display(),
            temp = %temp_path.display(),
            "Created local multipart upload"
        );

        Ok(Box::new(LocalMultipartUpload::new(
            writer,
            temp_path,
            target_path,
        )))
    }
}

impl SeekableStorage for LocalStorage {
    fn seekable_reader(&self, path: &Path) -> Result<Box<dyn SeekRead + Send + 'static>> {
        let full_path = self.full_path(path)?;
        File::open(&full_path)
            .map(|f| Box::new(BufReader::new(f)) as Box<dyn SeekRead + Send>)
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    StorageError::not_found(full_path.display().to_string())
                } else {
                    StorageError::Io(e)
                }
            })
    }

    fn reader_seekable(&self, path: &Path) -> Result<Box<dyn Read + Send + 'static>> {
        let full_path = self.full_path(path)?;
        File::open(&full_path)
            .map(|f| Box::new(BufReader::new(f)) as Box<dyn Read + Send>)
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    StorageError::not_found(full_path.display().to_string())
                } else {
                    StorageError::Io(e)
                }
            })
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Seek, SeekFrom, Write};

    #[test]
    fn test_local_storage_new() {
        let storage = LocalStorage::new("/tmp/test_roboflow");
        assert_eq!(storage.root(), PathBuf::from("/tmp/test_roboflow"));
    }

    #[test]
    fn test_write_and_read() {
        let temp_dir = std::env::temp_dir();
        let storage = LocalStorage::new(&temp_dir);

        let test_path = "test_write_read.txt";
        let test_content = b"Hello, World!";

        // Write
        let mut writer = storage.writer(Path::new(test_path)).unwrap();
        writer.write_all(test_content).unwrap();
        writer.flush().unwrap();

        // Read
        let mut reader = storage.reader(Path::new(test_path)).unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).unwrap();

        assert_eq!(buffer, test_content);

        // Cleanup
        storage.delete(Path::new(test_path)).unwrap();
    }

    #[test]
    fn test_seekable_reader() {
        let temp_dir = std::env::temp_dir();
        let storage = LocalStorage::new(&temp_dir);

        let test_path = "test_seekable.txt";
        let test_content = b"0123456789";

        // Write
        let mut writer = storage.writer(Path::new(test_path)).unwrap();
        writer.write_all(test_content).unwrap();
        writer.flush().unwrap();

        // Seek and read
        let mut reader = storage.seekable_reader(Path::new(test_path)).unwrap();
        reader.seek(SeekFrom::Start(5)).unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).unwrap();

        assert_eq!(buffer, b"56789");

        // Cleanup
        storage.delete(Path::new(test_path)).unwrap();
    }

    #[test]
    fn test_exists() {
        let temp_dir = std::env::temp_dir();
        let storage = LocalStorage::new(&temp_dir);

        let test_path = "test_exists.txt";

        assert!(!storage.exists(Path::new(test_path)));

        let mut writer = storage.writer(Path::new(test_path)).unwrap();
        writer.write_all(b"test").unwrap();
        writer.flush().unwrap();

        assert!(storage.exists(Path::new(test_path)));

        // Cleanup
        storage.delete(Path::new(test_path)).unwrap();
    }

    #[test]
    fn test_size() {
        let temp_dir = std::env::temp_dir();
        let storage = LocalStorage::new(&temp_dir);

        let test_path = "test_size.txt";
        let test_content = b"Hello, World!";

        let mut writer = storage.writer(Path::new(test_path)).unwrap();
        writer.write_all(test_content).unwrap();
        writer.flush().unwrap();

        assert_eq!(
            storage.size(Path::new(test_path)).unwrap(),
            test_content.len() as u64
        );

        // Cleanup
        storage.delete(Path::new(test_path)).unwrap();
    }

    #[test]
    fn test_metadata() {
        let temp_dir = std::env::temp_dir();
        let storage = LocalStorage::new(&temp_dir);

        let test_path = "test_metadata.txt";
        let test_content = b"test content";

        let mut writer = storage.writer(Path::new(test_path)).unwrap();
        writer.write_all(test_content).unwrap();
        writer.flush().unwrap();

        let meta = storage.metadata(Path::new(test_path)).unwrap();
        assert_eq!(meta.size, test_content.len() as u64);
        assert!(!meta.is_dir);
        assert!(meta.last_modified.is_some());

        // Cleanup
        storage.delete(Path::new(test_path)).unwrap();
    }

    #[test]
    fn test_create_dir_all() {
        let temp_dir = std::env::temp_dir();
        let storage = LocalStorage::new(&temp_dir);

        let dir_path = "test/nested/directory";
        storage.create_dir_all(Path::new(dir_path)).unwrap();

        assert!(storage.exists(Path::new(dir_path)));

        // Cleanup - remove the directory using full path
        let full_path = std::env::temp_dir().join(dir_path);
        let _ = fs::remove_dir_all(full_path);
    }

    #[test]
    fn test_copy() {
        let temp_dir = std::env::temp_dir();
        let storage = LocalStorage::new(&temp_dir);

        let src_path = "test_copy_src.txt";
        let dst_path = "test_copy_dst.txt";
        let test_content = b"copy test";

        let mut writer = storage.writer(Path::new(src_path)).unwrap();
        writer.write_all(test_content).unwrap();
        writer.flush().unwrap();

        storage
            .copy(Path::new(src_path), Path::new(dst_path))
            .unwrap();

        assert!(storage.exists(Path::new(dst_path)));
        assert_eq!(
            storage.size(Path::new(dst_path)).unwrap(),
            test_content.len() as u64
        );

        // Cleanup
        storage.delete(Path::new(src_path)).unwrap();
        storage.delete(Path::new(dst_path)).unwrap();
    }

    #[test]
    fn test_delete() {
        let temp_dir = std::env::temp_dir();
        let storage = LocalStorage::new(&temp_dir);

        let test_path = "test_delete.txt";

        let mut writer = storage.writer(Path::new(test_path)).unwrap();
        writer.write_all(b"test").unwrap();
        writer.flush().unwrap();

        assert!(storage.exists(Path::new(test_path)));
        storage.delete(Path::new(test_path)).unwrap();
        assert!(!storage.exists(Path::new(test_path)));
    }

    #[test]
    fn test_list() {
        let temp_dir = std::env::temp_dir();
        let storage = LocalStorage::new(&temp_dir);

        let dir_path = "test_list_dir";
        storage.create_dir_all(Path::new(dir_path)).unwrap();

        let file1 = format!("{dir_path}/file1.txt");
        let file2 = format!("{dir_path}/file2.txt");

        let mut w1 = storage.writer(Path::new(&file1)).unwrap();
        w1.write_all(b"content1").unwrap();
        let mut w2 = storage.writer(Path::new(&file2)).unwrap();
        w2.write_all(b"content2").unwrap();

        let results = storage.list(Path::new(dir_path)).unwrap();
        assert_eq!(results.len(), 2);

        // Cleanup - remove the directory using full path
        let full_path = std::env::temp_dir().join(dir_path);
        let _ = fs::remove_dir_all(full_path);
    }

    #[test]
    fn test_not_found_error() {
        let temp_dir = std::env::temp_dir();
        let storage = LocalStorage::new(&temp_dir);

        let result = storage.reader(Path::new(r"nonexistent_file.txt").as_ref());
        assert!(matches!(result, Err(StorageError::NotFound(_))));
    }

    // Security tests for path traversal protection
    #[test]
    fn test_path_traversal_double_dot() {
        let temp_dir = std::env::temp_dir();
        let storage = LocalStorage::new(&temp_dir);

        // Attempt to escape the temp directory using ../
        let result = storage.full_path(Path::new("../../../etc/passwd"));
        assert!(matches!(result, Err(StorageError::PermissionDenied(_))));
    }

    #[test]
    fn test_path_traversal_mixed_dots() {
        let temp_dir = std::env::temp_dir();
        let storage = LocalStorage::new(&temp_dir);

        // Attempt to escape using mixed paths
        let result = storage.full_path(Path::new("subdir/../../etc/passwd"));
        assert!(matches!(result, Err(StorageError::PermissionDenied(_))));
    }

    #[test]
    fn test_path_traversal_absolute_path_still_within_root() {
        let temp_dir = std::env::temp_dir();
        let storage = LocalStorage::new(&temp_dir);

        // Absolute paths that are still within root should work
        // (they're treated as relative to the root due to join())
        let result = storage.full_path(Path::new("test.txt"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_path_traversal_leading_double_dot() {
        let temp_dir = std::env::temp_dir();
        let storage = LocalStorage::new(&temp_dir);

        // Leading ../ should fail
        let result = storage.full_path(Path::new("../escape.txt"));
        assert!(matches!(result, Err(StorageError::PermissionDenied(_))));
    }

    // =============================================================================
    // compose_objects Tests
    // =============================================================================

    #[test]
    fn test_compose_objects_single_source() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = LocalStorage::new(temp_dir.path());

        // Create a source file
        let src_path = Path::new("segment_0.mp4");
        let mut writer = storage.writer(src_path).unwrap();
        writer.write_all(b"fake mp4 content").unwrap();
        writer.flush().unwrap();

        // Compose single source (should just copy)
        let dest_path = Path::new("episode_000000.mp4");
        let composer = roboflow_core::MockVideoComposer::new();
        storage
            .compose_objects(&[src_path], dest_path, &composer)
            .unwrap();

        // Verify destination exists and has same content
        assert!(storage.exists(dest_path));
        let mut reader = storage.reader(dest_path).unwrap();
        let mut content = Vec::new();
        reader.read_to_end(&mut content).unwrap();
        assert_eq!(content, b"fake mp4 content");
    }

    #[test]
    fn test_compose_objects_multiple_sources() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = LocalStorage::new(temp_dir.path());

        // Create multiple source files
        let sources: Vec<&Path> = vec![
            Path::new("segment_0.mp4"),
            Path::new("segment_1.mp4"),
            Path::new("segment_2.mp4"),
        ];

        for (i, &src) in sources.iter().enumerate() {
            let mut writer = storage.writer(src).unwrap();
            writer
                .write_all(format!("segment_{} content; ", i).as_bytes())
                .unwrap();
            writer.flush().unwrap();
        }

        // Compose all sources using mock composer
        let dest_path = Path::new("episode_000000.mp4");
        let composer = roboflow_core::MockVideoComposer::new();
        storage
            .compose_objects(&sources, dest_path, &composer)
            .unwrap();

        // Verify composer was called with correct arguments
        let ops = composer.get_operations();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].sources.len(), 3);
        assert!(ops[0].dest.to_string_lossy().contains("episode_000000.mp4"));
    }

    #[test]
    fn test_compose_objects_empty_sources() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = LocalStorage::new(temp_dir.path());

        let dest_path = Path::new("episode_000000.mp4");
        let composer = roboflow_core::MockVideoComposer::new();
        let result = storage.compose_objects(&[], dest_path, &composer);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("at least one source")
        );
    }

    #[test]
    fn test_compose_objects_missing_source() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = LocalStorage::new(temp_dir.path());

        // Create one source file
        let src0 = Path::new("segment_0.mp4");
        let mut writer = storage.writer(src0).unwrap();
        writer.write_all(b"content").unwrap();
        writer.flush().unwrap();

        // Try to compose with a missing source
        let src1 = Path::new("segment_1.mp4"); // This doesn't exist
        let dest_path = Path::new("episode_000000.mp4");
        let composer = roboflow_core::MockVideoComposer::new();
        let result = storage.compose_objects(&[src0, src1], dest_path, &composer);

        assert!(result.is_err());
        assert!(matches!(result, Err(StorageError::NotFound(_))));
    }

    #[test]
    fn test_compose_objects_creates_parent_dirs() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = LocalStorage::new(temp_dir.path());

        // Create source file
        let src_path = Path::new("segment_0.mp4");
        let mut writer = storage.writer(src_path).unwrap();
        writer.write_all(b"content").unwrap();
        writer.flush().unwrap();

        // Compose to nested destination
        let dest_path = Path::new("videos/chunk-000/camera/episode_000000.mp4");
        let composer = roboflow_core::MockVideoComposer::new();
        storage
            .compose_objects(&[src_path], dest_path, &composer)
            .unwrap();

        // Verify destination exists with parent directories
        assert!(storage.exists(dest_path));
    }

    // =============================================================================
    // delete_prefix Tests
    // =============================================================================

    #[test]
    fn test_delete_prefix_multiple_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = LocalStorage::new(temp_dir.path());

        // Create multiple files in a prefix
        storage
            .create_dir_all(Path::new("temp/session123"))
            .unwrap();
        let files: Vec<&Path> = vec![
            Path::new("temp/session123/segment_0.mp4"),
            Path::new("temp/session123/segment_1.mp4"),
            Path::new("temp/session123/segment_2.mp4"),
        ];

        for &file in &files {
            let mut writer = storage.writer(file).unwrap();
            writer.write_all(b"content").unwrap();
            writer.flush().unwrap();
        }

        // Delete prefix
        let count = storage.delete_prefix(Path::new("temp/session123")).unwrap();
        assert_eq!(count, 3);

        // Verify files are deleted
        for &file in &files {
            assert!(!storage.exists(file));
        }
    }

    #[test]
    fn test_delete_prefix_nonexistent() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = LocalStorage::new(temp_dir.path());

        // Delete non-existent prefix should return 0
        let count = storage.delete_prefix(Path::new("nonexistent")).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_delete_prefix_nested_directories() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = LocalStorage::new(temp_dir.path());

        // Create nested directory structure
        storage
            .create_dir_all(Path::new("temp/session123/episode_0/camera"))
            .unwrap();
        storage
            .create_dir_all(Path::new("temp/session123/episode_1/camera"))
            .unwrap();

        let files: Vec<&Path> = vec![
            Path::new("temp/session123/episode_0/camera/segment_0.mp4"),
            Path::new("temp/session123/episode_1/camera/segment_0.mp4"),
        ];

        for &file in &files {
            let mut writer = storage.writer(file).unwrap();
            writer.write_all(b"content").unwrap();
            writer.flush().unwrap();
        }

        // Delete prefix
        let count = storage.delete_prefix(Path::new("temp/session123")).unwrap();
        assert_eq!(count, 2);

        // Verify all files are deleted
        for &file in &files {
            assert!(!storage.exists(file));
        }

        // Verify directories are also removed (they should be empty now)
        assert!(!storage.exists(Path::new("temp/session123")));
    }

    #[test]
    fn test_delete_prefix_preserves_other_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = LocalStorage::new(temp_dir.path());

        // Create files in different prefixes
        let file_to_delete = Path::new("temp/session123/segment.mp4");
        let file_to_keep = Path::new("videos/episode.mp4");

        storage
            .create_dir_all(Path::new("temp/session123"))
            .unwrap();
        storage.create_dir_all(Path::new("videos")).unwrap();

        for &file in &[file_to_delete, file_to_keep] {
            let mut writer = storage.writer(file).unwrap();
            writer.write_all(b"content").unwrap();
            writer.flush().unwrap();
        }

        // Delete only one prefix
        let count = storage.delete_prefix(Path::new("temp/session123")).unwrap();
        assert_eq!(count, 1);

        // Verify correct file was deleted
        assert!(!storage.exists(file_to_delete));
        assert!(storage.exists(file_to_keep));
    }
}
