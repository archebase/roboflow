// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Object store for distributed intermediate data.
//!
//! ObjectRef represents immutable, content-addressed outputs stored in a
//! distributed object store. This enables lineage tracking, automatic recovery,
//! deduplication, and reference counting for garbage collection.

use serde::{Deserialize, Serialize};
use std::fmt;

use crate::task::TaskId;

type ObjectEntry = (Vec<u8>, TaskId, std::sync::atomic::AtomicUsize);

/// Unique identifier for an object (content-addressed).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectId([u8; 32]);

impl ObjectId {
    /// Create an ObjectId from a byte array.
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Get the bytes of the ObjectId.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

/// Worker identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerId(pub u64);

impl fmt::Display for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Worker({})", self.0)
    }
}

/// Object reference for lineage tracking and distributed object store.
///
/// From Ray's object store model. Objects are immutable and content-addressed.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectRef {
    /// Object ID (content-addressed: hash of data + task inputs).
    pub id: ObjectId,
    /// Size in bytes.
    pub size: u64,
    /// Owner task (for lineage tracking).
    pub owner: TaskId,
    /// Location hints (which workers have this object).
    pub locations: Vec<WorkerId>,
}

impl ObjectRef {
    /// Create a new ObjectRef.
    pub fn new(id: ObjectId, size: u64, owner: TaskId, locations: Vec<WorkerId>) -> Self {
        Self {
            id,
            size,
            owner,
            locations,
        }
    }

    /// Compute ObjectId from data (SHA-256 hash).
    pub fn compute_id(data: &[u8]) -> ObjectId {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(data);
        ObjectId::new(hasher.finalize().into())
    }
}

impl fmt::Display for ObjectRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectRef({}, {} bytes)", self.id, self.size)
    }
}

/// Error type for object store operations.
#[derive(Debug, thiserror::Error)]
pub enum ObjectStoreError {
    #[error("Object not found: {0}")]
    NotFound(ObjectId),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Storage error: {0}")]
    Storage(String),
}

/// Result type for object store operations.
pub type ObjectStoreResult<T> = Result<T, ObjectStoreError>;

/// Object store for intermediate data between stages.
///
/// This trait abstracts over different storage backends (memory, local disk,
/// distributed storage like S3/OSS, etc.).
#[async_trait::async_trait]
pub trait ObjectStore: Send + Sync {
    /// Get object by reference.
    async fn get(&self, obj: &ObjectRef) -> ObjectStoreResult<Vec<u8>>;

    /// Put object into store, returns reference.
    async fn put(&self, data: Vec<u8>, owner: TaskId) -> ObjectStoreResult<ObjectRef>;

    /// Check if object exists.
    async fn contains(&self, obj: &ObjectRef) -> bool;

    /// Add reference (increment ref count).
    async fn add_ref(&self, obj: &ObjectRef);

    /// Remove reference (decrement ref count, may GC).
    async fn remove_ref(&self, obj: &ObjectRef);

    /// Get object size without fetching data.
    async fn get_size(&self, obj: &ObjectRef) -> ObjectStoreResult<u64>;
}

/// In-memory object store implementation.
pub struct MemoryObjectStore {
    inner: tokio::sync::RwLock<std::collections::HashMap<ObjectId, ObjectEntry>>,
}

impl MemoryObjectStore {
    /// Create a new empty memory object store.
    pub fn new() -> Self {
        Self {
            inner: tokio::sync::RwLock::new(std::collections::HashMap::new()),
        }
    }
}

impl Default for MemoryObjectStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl ObjectStore for MemoryObjectStore {
    async fn get(&self, obj: &ObjectRef) -> ObjectStoreResult<Vec<u8>> {
        let inner = self.inner.read().await;
        match inner.get(&obj.id) {
            Some((data, _, _)) => Ok(data.clone()),
            None => Err(ObjectStoreError::NotFound(obj.id)),
        }
    }

    async fn put(&self, data: Vec<u8>, owner: TaskId) -> ObjectStoreResult<ObjectRef> {
        let id = ObjectRef::compute_id(&data);
        let size = data.len() as u64;

        let mut inner = self.inner.write().await;
        inner.entry(id).or_insert_with(|| {
            (data, owner, std::sync::atomic::AtomicUsize::new(1))
        });

        Ok(ObjectRef::new(id, size, owner, vec![]))
    }

    async fn contains(&self, obj: &ObjectRef) -> bool {
        let inner = self.inner.read().await;
        inner.contains_key(&obj.id)
    }

    async fn add_ref(&self, obj: &ObjectRef) {
        let inner = self.inner.read().await;
        if let Some((_, _, ref_count)) = inner.get(&obj.id) {
            ref_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    async fn remove_ref(&self, obj: &ObjectRef) {
        let mut inner = self.inner.write().await;
        if let Some((_, _, ref_count)) = inner.get(&obj.id) {
            let count = ref_count.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            if count == 1 {
                // Last reference, remove the object
                inner.remove(&obj.id);
            }
        }
    }

    async fn get_size(&self, obj: &ObjectRef) -> ObjectStoreResult<u64> {
        let inner = self.inner.read().await;
        match inner.get(&obj.id) {
            Some((data, _, _)) => Ok(data.len() as u64),
            None => Err(ObjectStoreError::NotFound(obj.id)),
        }
    }
}

/// Local disk object store implementation.
pub struct LocalObjectStore {
    base_path: std::path::PathBuf,
}

impl LocalObjectStore {
    /// Create a new local disk object store.
    pub fn new(base_path: impl AsRef<std::path::Path>) -> Self {
        Self {
            base_path: base_path.as_ref().to_path_buf(),
        }
    }

    fn object_path(&self, id: ObjectId) -> std::path::PathBuf {
        // Use first 2 bytes of hash as subdirectory for distribution
        let hex = hex::encode(id.as_bytes());
        let prefix = &hex[..2];
        self.base_path.join(prefix).join(hex)
    }
}

#[async_trait::async_trait]
impl ObjectStore for LocalObjectStore {
    async fn get(&self, obj: &ObjectRef) -> ObjectStoreResult<Vec<u8>> {
        let path = self.object_path(obj.id);
        match tokio::fs::read(&path).await {
            Ok(data) => Ok(data),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(ObjectStoreError::NotFound(obj.id))
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn put(&self, data: Vec<u8>, owner: TaskId) -> ObjectStoreResult<ObjectRef> {
        let id = ObjectRef::compute_id(&data);
        let size = data.len() as u64;
        let path = self.object_path(id);

        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Write atomically using temp file + rename
        let temp_path = path.with_extension("tmp");
        tokio::fs::write(&temp_path, &data).await?;
        tokio::fs::rename(&temp_path, &path).await?;

        Ok(ObjectRef::new(id, size, owner, vec![]))
    }

    async fn contains(&self, obj: &ObjectRef) -> bool {
        let path = self.object_path(obj.id);
        path.exists()
    }

    async fn add_ref(&self, _obj: &ObjectRef) {
        // Local store doesn't track references explicitly
    }

    async fn remove_ref(&self, obj: &ObjectRef) {
        // For local store, we could delete the file here if tracking references
        let path = self.object_path(obj.id);
        let _ = tokio::fs::remove_file(path).await;
    }

    async fn get_size(&self, obj: &ObjectRef) -> ObjectStoreResult<u64> {
        let path = self.object_path(obj.id);
        match tokio::fs::metadata(path).await {
            Ok(meta) => Ok(meta.len()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(ObjectStoreError::NotFound(obj.id))
            }
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_object_store() {
        let store = MemoryObjectStore::new();
        let task_id = TaskId(1);
        let data = b"hello world".to_vec();

        // Put object
        let obj_ref = store.put(data.clone(), task_id).await.unwrap();
        assert_eq!(obj_ref.size, data.len() as u64);
        assert_eq!(obj_ref.owner, task_id);

        // Check contains
        assert!(store.contains(&obj_ref).await);

        // Get object
        let retrieved = store.get(&obj_ref).await.unwrap();
        assert_eq!(retrieved, data);

        // Get size
        let size = store.get_size(&obj_ref).await.unwrap();
        assert_eq!(size, data.len() as u64);

        // Remove reference
        store.remove_ref(&obj_ref).await;
        assert!(!store.contains(&obj_ref).await);
    }

    #[tokio::test]
    async fn test_object_id_computation() {
        let data1 = b"test data 1";
        let data2 = b"test data 2";

        let id1a = ObjectRef::compute_id(data1);
        let id1b = ObjectRef::compute_id(data1);
        let id2 = ObjectRef::compute_id(data2);

        // Same data = same ID
        assert_eq!(id1a, id1b);
        // Different data = different ID
        assert_ne!(id1a, id2);
    }

    #[tokio::test]
    async fn test_local_object_store() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(temp_dir.path());
        let task_id = TaskId(1);
        let data = b"hello world".to_vec();

        // Put object
        let obj_ref = store.put(data.clone(), task_id).await.unwrap();

        // Check contains
        assert!(store.contains(&obj_ref).await);

        // Get object
        let retrieved = store.get(&obj_ref).await.unwrap();
        assert_eq!(retrieved, data);

        // Get size
        let size = store.get_size(&obj_ref).await.unwrap();
        assert_eq!(size, data.len() as u64);
    }
}
