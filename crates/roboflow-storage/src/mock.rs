// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Mock storage implementation for testing.
//!
//! This module provides an in-memory storage implementation that can be used
//! in tests without requiring external services like MinIO or S3.

use std::collections::HashMap;
use std::io::{Cursor, Read, Write};
use std::path::Path;
use std::sync::{Arc, RwLock};

use crate::StreamingConfig;
use crate::error::{StorageError, StorageResult};
use crate::metadata::ObjectMetadata;
use crate::traits::{Storage, StreamingRead};

/// In-memory mock storage for testing.
///
/// Uses a `HashMap` to store objects in memory, making it suitable for
/// unit tests without external dependencies.
///
/// # Example
///
/// ```
/// use roboflow_storage::mock::MockStorage;
/// use roboflow_storage::Storage;
/// use std::path::Path;
///
/// let storage = MockStorage::new();
///
/// // Write data
/// let mut writer = storage.writer(Path::new("test.txt"))?;
/// writer.write_all(b"hello")?;
/// writer.flush()?;
///
/// // Read data
/// let mut reader = storage.reader(Path::new("test.txt"))?;
/// let mut buf = String::new();
/// // reader.read_to_string(&mut buf)?;
/// // assert_eq!(buf, "hello");
/// # Ok::<(), roboflow_storage::StorageError>(())
/// ```
#[derive(Debug, Default)]
pub struct MockStorage {
    data: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl MockStorage {
    /// Create a new empty mock storage.
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a mock storage with pre-populated data.
    pub fn with_data(items: Vec<(&str, &[u8])>) -> Self {
        let mut data = HashMap::new();
        for (path, content) in items {
            data.insert(path.to_string(), content.to_vec());
        }
        Self {
            data: Arc::new(RwLock::new(data)),
        }
    }

    /// Get the number of stored objects.
    pub fn count(&self) -> usize {
        self.data.read().expect("mock storage lock poisoned").len()
    }

    /// Check if the storage is empty.
    pub fn is_empty(&self) -> bool {
        self.data
            .read()
            .expect("mock storage lock poisoned")
            .is_empty()
    }

    /// Clear all stored objects.
    pub fn clear(&self) {
        self.data
            .write()
            .expect("mock storage lock poisoned")
            .clear();
    }

    fn path_to_key(&self, path: &Path) -> String {
        path.to_string_lossy().to_string()
    }
}

impl Storage for MockStorage {
    fn reader(&self, path: &Path) -> StorageResult<Box<dyn Read + Send + 'static>> {
        let key = self.path_to_key(path);
        let data = self.data.read().expect("mock storage lock poisoned");
        let content = data
            .get(&key)
            .ok_or_else(|| StorageError::NotFound(key.clone()))?
            .clone();
        Ok(Box::new(Cursor::new(content)))
    }

    fn writer(&self, path: &Path) -> StorageResult<Box<dyn Write + Send + 'static>> {
        let key = self.path_to_key(path);
        let data = Arc::clone(&self.data);
        Ok(Box::new(MockWriter { key, data }))
    }

    fn exists(&self, path: &Path) -> bool {
        let key = self.path_to_key(path);
        self.data
            .read()
            .expect("mock storage lock poisoned")
            .contains_key(&key)
    }

    fn size(&self, path: &Path) -> StorageResult<u64> {
        let key = self.path_to_key(path);
        let data = self.data.read().expect("mock storage lock poisoned");
        data.get(&key)
            .map(|v| v.len() as u64)
            .ok_or(StorageError::NotFound(key))
    }

    fn metadata(&self, path: &Path) -> StorageResult<ObjectMetadata> {
        let key = self.path_to_key(path);
        let data = self.data.read().expect("mock storage lock poisoned");
        data.get(&key)
            .map(|v| ObjectMetadata {
                path: key.clone(),
                size: v.len() as u64,
                last_modified: Some(std::time::SystemTime::now()),
                content_type: None,
                is_dir: false,
            })
            .ok_or(StorageError::NotFound(key))
    }

    fn list(&self, prefix: &Path) -> StorageResult<Vec<ObjectMetadata>> {
        let prefix_str = self.path_to_key(prefix);
        let data = self.data.read().expect("mock storage lock poisoned");
        let results: Vec<ObjectMetadata> = data
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix_str))
            .map(|(k, v)| ObjectMetadata {
                path: k.clone(),
                size: v.len() as u64,
                last_modified: Some(std::time::SystemTime::now()),
                content_type: None,
                is_dir: false,
            })
            .collect();
        Ok(results)
    }

    fn delete(&self, path: &Path) -> StorageResult<()> {
        let key = self.path_to_key(path);
        let mut data = self.data.write().expect("mock storage lock poisoned");
        data.remove(&key)
            .map(|_| ())
            .ok_or(StorageError::NotFound(key))
    }

    fn copy(&self, from: &Path, to: &Path) -> StorageResult<()> {
        let from_key = self.path_to_key(from);
        let to_key = self.path_to_key(to);
        let data = self.data.read().expect("mock storage lock poisoned");
        let content = data
            .get(&from_key)
            .ok_or_else(|| StorageError::NotFound(from_key.clone()))?
            .clone();
        drop(data);
        self.data
            .write()
            .expect("mock storage lock poisoned")
            .insert(to_key, content);
        Ok(())
    }

    fn create_dir(&self, _path: &Path) -> StorageResult<()> {
        // No-op for in-memory storage
        Ok(())
    }

    fn create_dir_all(&self, _path: &Path) -> StorageResult<()> {
        // No-op for in-memory storage
        Ok(())
    }

    fn streaming_reader(
        &self,
        path: &Path,
        _config: StreamingConfig,
    ) -> StorageResult<Box<dyn StreamingRead + Send + 'static>> {
        // For mock, just use regular reader wrapped
        let reader = self.reader(path)?;
        Ok(Box::new(MockStreamingReader {
            reader,
            position: 0,
        }))
    }
}

/// Writer for mock storage that stores data in the HashMap.
struct MockWriter {
    key: String,
    data: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl Write for MockWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut data = self.data.write().expect("mock storage lock poisoned");
        let entry = data.entry(self.key.clone()).or_default();
        entry.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Streaming reader wrapper for mock storage.
struct MockStreamingReader {
    reader: Box<dyn Read + Send + 'static>,
    position: u64,
}

impl Read for MockStreamingReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.reader.read(buf)?;
        self.position += n as u64;
        Ok(n)
    }
}

impl StreamingRead for MockStreamingReader {
    fn position(&self) -> u64 {
        self.position
    }

    fn seek_to(&mut self, _offset: u64) -> StorageResult<()> {
        Err(StorageError::Other(
            "seek not supported in mock streaming reader".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_storage_write_read() {
        let storage = MockStorage::new();
        let path = Path::new("test.txt");

        // Write
        let mut writer = storage.writer(path).unwrap();
        writer.write_all(b"hello world").unwrap();
        writer.flush().unwrap();

        // Read
        let mut reader = storage.reader(path).unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();
        assert_eq!(&buf, b"hello world");
    }

    #[test]
    fn test_mock_storage_exists() {
        let storage = MockStorage::new();
        let path = Path::new("test.txt");

        assert!(!storage.exists(path));

        let mut writer = storage.writer(path).unwrap();
        writer.write_all(b"data").unwrap();
        writer.flush().unwrap();

        assert!(storage.exists(path));
    }

    #[test]
    fn test_mock_storage_size() {
        let storage = MockStorage::new();
        let path = Path::new("test.txt");

        let mut writer = storage.writer(path).unwrap();
        writer.write_all(b"12345").unwrap();
        writer.flush().unwrap();

        assert_eq!(storage.size(path).unwrap(), 5);
    }

    #[test]
    fn test_mock_storage_delete() {
        let storage = MockStorage::new();
        let path = Path::new("test.txt");

        let mut writer = storage.writer(path).unwrap();
        writer.write_all(b"data").unwrap();
        writer.flush().unwrap();

        assert!(storage.exists(path));
        storage.delete(path).unwrap();
        assert!(!storage.exists(path));
    }

    #[test]
    fn test_mock_storage_copy() {
        let storage = MockStorage::new();
        let from = Path::new("from.txt");
        let to = Path::new("to.txt");

        let mut writer = storage.writer(from).unwrap();
        writer.write_all(b"copy me").unwrap();
        writer.flush().unwrap();

        storage.copy(from, to).unwrap();

        let mut reader = storage.reader(to).unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();
        assert_eq!(&buf, b"copy me");
    }

    #[test]
    fn test_mock_storage_with_data() {
        let storage = MockStorage::with_data(vec![
            ("file1.txt", b"content1".as_slice()),
            ("file2.txt", b"content2".as_slice()),
        ]);

        assert_eq!(storage.count(), 2);
        assert!(storage.exists(Path::new("file1.txt")));
        assert!(storage.exists(Path::new("file2.txt")));
    }

    #[test]
    fn test_mock_storage_list() {
        let storage = MockStorage::with_data(vec![
            ("dir/file1.txt", b"a".as_slice()),
            ("dir/file2.txt", b"b".as_slice()),
            ("other.txt", b"c".as_slice()),
        ]);

        let items = storage.list(Path::new("dir/")).unwrap();
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn test_mock_storage_not_found() {
        let storage = MockStorage::new();
        let result = storage.reader(Path::new("nonexistent.txt"));
        assert!(result.is_err());
    }
}
