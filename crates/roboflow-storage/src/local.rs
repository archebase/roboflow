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
    pub fn full_path(&self, path: &Path) -> PathBuf {
        self.root.join(path)
    }

    /// Ensure parent directories exist for a path.
    fn ensure_parent(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(self.full_path(parent)).map_err(|e| {
                StorageError::Other(format!("failed to create parent directories: {e}"))
            })?;
        }
        Ok(())
    }
}

impl Storage for LocalStorage {
    fn reader(&self, path: &Path) -> Result<Box<dyn Read + Send + 'static>> {
        let full_path = self.full_path(path);
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
        let full_path = self.full_path(path);
        self.ensure_parent(&full_path)?;
        File::create(&full_path)
            .map(|f| Box::new(BufWriter::new(f)) as Box<dyn Write + Send>)
            .map_err(StorageError::Io)
    }

    fn exists(&self, path: &Path) -> bool {
        self.full_path(path).exists()
    }

    fn size(&self, path: &Path) -> Result<u64> {
        let full_path = self.full_path(path);
        fs::metadata(&full_path).map(|m| m.len()).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::not_found(full_path.display().to_string())
            } else {
                StorageError::Io(e)
            }
        })
    }

    fn metadata(&self, path: &Path) -> Result<ObjectMetadata> {
        let full_path = self.full_path(path);
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
        let full_path = self.full_path(prefix);
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
        let full_path = self.full_path(path);
        fs::remove_file(&full_path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::not_found(full_path.display().to_string())
            } else {
                StorageError::Io(e)
            }
        })
    }

    fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let from_path = self.full_path(from);
        let to_path = self.full_path(to);
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
        let full_path = self.full_path(path);
        fs::create_dir(&full_path).map_err(StorageError::Io)
    }

    fn create_dir_all(&self, path: &Path) -> Result<()> {
        let full_path = self.full_path(path);
        fs::create_dir_all(&full_path).map_err(StorageError::Io)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn read_range(
        &self,
        path: &Path,
        start: u64,
        end: Option<u64>,
    ) -> Result<Box<dyn Read + Send + 'static>> {
        use std::io::{Cursor, Seek, SeekFrom};

        let full_path = self.full_path(path);

        // Get file size if end not specified
        let file_size = if end.is_some() {
            None
        } else {
            Some(
                fs::metadata(&full_path)
                    .map_err(|e| {
                        if e.kind() == std::io::ErrorKind::NotFound {
                            StorageError::not_found(full_path.display().to_string())
                        } else {
                            StorageError::Io(e)
                        }
                    })?
                    .len(),
            )
        };

        let end = end.unwrap_or_else(|| file_size.unwrap());

        // Validate bounds
        if start > end {
            return Err(StorageError::invalid_path(format!(
                "start offset {} exceeds end offset {}",
                start, end
            )));
        }

        let length = end - start;
        let length_usize =
            usize::try_from(length).map_err(|_| {
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
        let full_path = self.full_path(path);

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

impl SeekableStorage for LocalStorage {
    fn seekable_reader(&self, path: &Path) -> Result<Box<dyn SeekRead + Send + 'static>> {
        let full_path = self.full_path(path);
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
        let full_path = self.full_path(path);
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
}
