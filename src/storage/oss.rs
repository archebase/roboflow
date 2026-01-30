// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Alibaba OSS and S3-compatible storage backend.
//!
//! This module provides cloud storage support using the `object_store` crate.
//! It supports Alibaba OSS (S3-compatible) and Amazon S3.

use std::io::{Read, Write};
use std::path::Path;

use super::{Result, Storage, StorageError};

/// Alibaba OSS / S3-compatible storage backend.
///
/// Provides access to objects stored in Alibaba OSS or any S3-compatible service.
/// This requires the `cloud-storage` feature to be enabled.
///
/// # Example
///
/// ```ignore
/// use roboflow::storage::{Storage, OssStorage};
///
/// let storage = OssStorage::new(
///     "my-bucket",
///     "oss-cn-hangzhou.aliyuncs.com",
///     "access-key-id",
///     "access-key-secret"
/// )?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone)]
pub struct OssStorage {
    /// Bucket name
    bucket: String,
    /// OSS endpoint (e.g., oss-cn-hangzhou.aliyuncs.com)
    endpoint: String,
    /// Access key ID
    access_key_id: String,
    /// Access key secret
    access_key_secret: String,
    /// Optional prefix for all keys
    prefix: Option<String>,
}

impl OssStorage {
    /// Create a new OSS storage backend.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket name
    /// * `endpoint` - The OSS endpoint (e.g., oss-cn-hangzhou.aliyuncs.com)
    /// * `access_key_id` - The access key ID
    /// * `access_key_secret` - The access key secret
    pub fn new(
        bucket: impl Into<String>,
        endpoint: impl Into<String>,
        access_key_id: impl Into<String>,
        access_key_secret: impl Into<String>,
    ) -> Result<Self> {
        Ok(Self {
            bucket: bucket.into(),
            endpoint: endpoint.into(),
            access_key_id: access_key_id.into(),
            access_key_secret: access_key_secret.into(),
            prefix: None,
        })
    }

    /// Set a prefix for all operations.
    ///
    /// This is useful for isolating different environments or datasets
    /// within the same bucket.
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    /// Get the full key for a path, including prefix if set.
    fn full_key(&self, path: &Path) -> String {
        let path_str = path.to_string_lossy();
        match &self.prefix {
            Some(prefix) => format!("{}/{}", prefix.trim_end_matches('/'), path_str),
            None => path_str.to_string(),
        }
    }
}

impl Storage for OssStorage {
    fn reader(&self, path: impl AsRef<Path>) -> Result<Box<dyn Read + Send + 'static>> {
        let _key = self.full_key(path.as_ref());
        // TODO: Implement using object_store
        // For now, return an error
        Err(StorageError::Other(
            "OSS storage not yet implemented - use LocalStorage".to_string(),
        ))
    }

    fn writer(&self, path: impl AsRef<Path>) -> Result<Box<dyn Write + Send + 'static>> {
        let _key = self.full_key(path.as_ref());
        // TODO: Implement using object_store
        // For now, return an error
        Err(StorageError::Other(
            "OSS storage not yet implemented - use LocalStorage".to_string(),
        ))
    }

    fn exists(&self, path: impl AsRef<Path>) -> bool {
        let _key = self.full_key(path.as_ref());
        // TODO: Implement using object_store
        false
    }

    fn size(&self, path: impl AsRef<Path>) -> Result<u64> {
        let _key = self.full_key(path.as_ref());
        // TODO: Implement using object_store
        Err(StorageError::Other(
            "OSS storage not yet implemented".to_string(),
        ))
    }

    fn metadata(&self, path: impl AsRef<Path>) -> Result<super::ObjectMetadata> {
        let _key = self.full_key(path.as_ref());
        // TODO: Implement using object_store
        Err(StorageError::Other(
            "OSS storage not yet implemented".to_string(),
        ))
    }

    fn list(&self, prefix: impl AsRef<Path>) -> Result<Vec<super::ObjectMetadata>> {
        let _key = self.full_key(prefix.as_ref());
        // TODO: Implement using object_store
        Ok(Vec::new())
    }

    fn delete(&self, path: impl AsRef<Path>) -> Result<()> {
        let _key = self.full_key(path.as_ref());
        // TODO: Implement using object_store
        Err(StorageError::Other(
            "OSS storage not yet implemented".to_string(),
        ))
    }

    fn copy(&self, from: impl AsRef<Path>, to: impl AsRef<Path>) -> Result<()> {
        let _from_key = self.full_key(from.as_ref());
        let _to_key = self.full_key(to.as_ref());
        // TODO: Implement using object_store
        Err(StorageError::Other(
            "OSS storage not yet implemented".to_string(),
        ))
    }

    fn create_dir(&self, _path: impl AsRef<Path>) -> Result<()> {
        // OSS doesn't have directories - this is a no-op
        Ok(())
    }

    fn create_dir_all(&self, _path: impl AsRef<Path>) -> Result<()> {
        // OSS doesn't have directories - this is a no-op
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oss_storage_new() {
        let storage = OssStorage::new("my-bucket", "oss-cn-hangzhou.aliyuncs.com", "key", "secret");
        assert!(storage.is_ok());
        let storage = storage.unwrap();
        assert_eq!(storage.bucket, "my-bucket");
        assert_eq!(storage.endpoint, "oss-cn-hangzhou.aliyuncs.com");
    }

    #[test]
    fn test_oss_storage_with_prefix() {
        let storage = OssStorage::new("my-bucket", "endpoint", "key", "secret")
            .unwrap()
            .with_prefix("data/test");
        assert_eq!(storage.prefix, Some("data/test".to_string()));
    }

    #[test]
    fn test_full_key_without_prefix() {
        let storage = OssStorage::new("bucket", "endpoint", "key", "secret").unwrap();
        assert_eq!(storage.full_key(Path::new("test.txt")), "test.txt");
        assert_eq!(storage.full_key(Path::new("data/test.txt")), "data/test.txt");
    }

    #[test]
    fn test_full_key_with_prefix() {
        let storage = OssStorage::new("bucket", "endpoint", "key", "secret")
            .unwrap()
            .with_prefix("datasets");
        assert_eq!(storage.full_key(Path::new("test.txt")), "datasets/test.txt");
        assert_eq!(
            storage.full_key(Path::new("data/test.txt")),
            "datasets/data/test.txt"
        );
    }

    #[test]
    fn test_full_key_with_trailing_slash_prefix() {
        let storage = OssStorage::new("bucket", "endpoint", "key", "secret")
            .unwrap()
            .with_prefix("datasets/");
        assert_eq!(storage.full_key(Path::new("test.txt")), "datasets/test.txt");
    }
}
