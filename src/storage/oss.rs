// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Alibaba OSS and S3-compatible storage backend.
//!
//! This module provides cloud storage support using the `object_store` crate.
//! It supports Alibaba OSS (S3-compatible) and Amazon S3.

use std::io::{Cursor, Read, Write};
use std::path::Path;
use std::sync::Arc;

use super::{ObjectMetadata, Result, Storage, StorageError};

/// Configuration for Alibaba OSS / S3-compatible storage.
#[derive(Debug, Clone)]
pub struct OssConfig {
    /// Bucket name
    pub bucket: String,
    /// OSS endpoint (e.g., oss-cn-hangzhou.aliyuncs.com)
    pub endpoint: String,
    /// Access key ID
    pub access_key_id: String,
    /// Access key secret
    pub access_key_secret: String,
    /// Region (optional, for AWS S3 compatibility)
    pub region: Option<String>,
    /// Optional prefix for all keys
    pub prefix: Option<String>,
    /// Whether to use internal endpoint (for Alibaba Cloud internal network)
    pub use_internal_endpoint: bool,
}

impl OssConfig {
    /// Create a new OSS configuration.
    pub fn new(
        bucket: impl Into<String>,
        endpoint: impl Into<String>,
        access_key_id: impl Into<String>,
        access_key_secret: impl Into<String>,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            endpoint: endpoint.into(),
            access_key_id: access_key_id.into(),
            access_key_secret: access_key_secret.into(),
            region: None,
            prefix: None,
            use_internal_endpoint: false,
        }
    }

    /// Set a prefix for all operations.
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    /// Set the region.
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Enable internal endpoint usage.
    pub fn with_internal_endpoint(mut self) -> Self {
        self.use_internal_endpoint = true;
        self
    }

    /// Get the full key for a path, including prefix if set.
    pub fn full_key(&self, path: &Path) -> String {
        let path_str = path.to_string_lossy();
        match &self.prefix {
            Some(prefix) => format!("{}/{}", prefix.trim_end_matches('/'), path_str),
            None => path_str.to_string(),
        }
    }

    /// Build the endpoint URL for S3-compatible API.
    pub fn endpoint_url(&self) -> String {
        if self.use_internal_endpoint {
            // Internal endpoint format: https://oss-cn-hangzhou-internal.aliyuncs.com
            let base = &self.endpoint;
            if base.contains("://") {
                base.to_string()
            } else {
                format!("https://{}", base)
            }
        } else {
            let base = &self.endpoint;
            if base.contains("://") {
                base.to_string()
            } else {
                format!("https://{}", base)
            }
        }
    }
}

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
pub struct OssStorage {
    /// The underlying object_store client
    store: Arc<dyn object_store::ObjectStore>,
    /// Tokio runtime for blocking operations
    runtime: tokio::runtime::Runtime,
    /// Configuration
    config: OssConfig,
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
        let config = OssConfig::new(bucket, endpoint, access_key_id, access_key_secret);
        Self::with_config(config)
    }

    /// Create a new OSS storage backend with configuration.
    pub fn with_config(config: OssConfig) -> Result<Self> {
        // Create a tokio runtime for blocking operations
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| StorageError::Other(format!("Failed to create tokio runtime: {}", e)))?;

        // Build S3-compatible configuration
        let builder = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(&config.bucket)
            .with_access_key_id(&config.access_key_id)
            .with_secret_access_key(&config.access_key_secret)
            .with_endpoint(config.endpoint_url())
            .with_region(
                config
                    .region
                    .as_deref()
                    .unwrap_or("default"),
            )
            // Allow HTTP for testing
            .with_allow_http(true);

        // Build the object_store client (synchronous)
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(
            builder.build().map_err(|e| StorageError::Cloud(format!("Failed to create OSS client: {}", e)))?,
        );

        Ok(Self {
            store,
            runtime,
            config,
        })
    }

    /// Get the full key for a path, including prefix if set.
    fn full_key(&self, path: &Path) -> String {
        self.config.full_key(path)
    }

    /// Convert object_store path to String key
    fn path_to_key(&self, path: &Path) -> object_store::path::Path {
        object_store::path::Path::from(self.full_key(path))
    }

    /// Convert object_store metadata to our metadata type
    fn convert_metadata(&self, meta: &object_store::ObjectMeta) -> ObjectMetadata {
        // Convert chrono DateTime to SystemTime
        fn chrono_to_system_time(dt: &chrono::DateTime<chrono::Utc>) -> std::time::SystemTime {
            let timestamp = dt.timestamp();
            std::time::SystemTime::UNIX_EPOCH
                .checked_add(std::time::Duration::from_secs(timestamp as u64))
                .unwrap_or(std::time::SystemTime::now())
        }

        let last_modified = Some(chrono_to_system_time(&meta.last_modified));

        ObjectMetadata {
            path: meta.location.to_string(),
            size: meta.size as u64,
            last_modified,
            content_type: None, // object_store doesn't provide content_type in ObjectMeta
            is_dir: false,
        }
    }
}

impl Storage for OssStorage {
    fn reader(&self, path: &Path) -> Result<Box<dyn Read + Send + 'static>> {
        let key = self.path_to_key(path);
        let store = self.store.clone();

        // Use runtime to get the object
        let get_result = self
            .runtime
            .block_on(async {
                store.get(&key).await.map_err(|e| match e {
                    object_store::Error::NotFound { .. } => {
                        StorageError::not_found(path.display().to_string())
                    }
                    _ => StorageError::Cloud(e.to_string()),
                })
            })?;

        // Get bytes from the result (this is async)
        let bytes = self
            .runtime
            .block_on(async {
                get_result
                    .bytes()
                    .await
                    .map_err(|e| StorageError::Cloud(format!("Failed to read bytes: {}", e)))
            })?
            .to_vec();

        Ok(Box::new(Cursor::new(bytes)))
    }

    fn writer(&self, path: &Path) -> Result<Box<dyn Write + Send + 'static>> {
        Ok(Box::new(OssWriter::new(
            self.store.clone(),
            self.runtime.handle().clone(),
            self.path_to_key(path),
        )))
    }

    fn exists(&self, path: &Path) -> bool {
        let key = self.path_to_key(path);
        let store = self.store.clone();

        self.runtime
            .block_on(async { store.head(&key).await })
            .is_ok()
    }

    fn size(&self, path: &Path) -> Result<u64> {
        let key = self.path_to_key(path);
        let store = self.store.clone();

        let meta = self
            .runtime
            .block_on(async {
                store.head(&key).await.map_err(|e| match e {
                    object_store::Error::NotFound { .. } => {
                        StorageError::not_found(path.display().to_string())
                    }
                    _ => StorageError::Cloud(e.to_string()),
                })
            })?;

        Ok(meta.size as u64)
    }

    fn metadata(&self, path: &Path) -> Result<ObjectMetadata> {
        let key = self.path_to_key(path);
        let store = self.store.clone();

        let meta = self
            .runtime
            .block_on(async {
                store.head(&key).await.map_err(|e| match e {
                    object_store::Error::NotFound { .. } => {
                        StorageError::not_found(path.display().to_string())
                    }
                    _ => StorageError::Cloud(e.to_string()),
                })
            })?;

        Ok(self.convert_metadata(&meta))
    }

    fn list(&self, prefix: &Path) -> Result<Vec<ObjectMetadata>> {
        let key = self.path_to_key(prefix);
        let store = self.store.clone();

        // Use async list_with_delimiter which is simpler
        let result = self.runtime.block_on(async {
            let list_result = store.list_with_delimiter(Some(&key)).await.map_err(|e| {
                StorageError::Cloud(format!("Failed to list objects: {}", e))
            })?;

            let mut metas = Vec::new();

            // Helper function to convert DateTime
            fn chrono_to_system_time(dt: chrono::DateTime<chrono::Utc>) -> std::time::SystemTime {
                let timestamp = dt.timestamp();
                std::time::SystemTime::UNIX_EPOCH
                    .checked_add(std::time::Duration::from_secs(timestamp as u64))
                    .unwrap_or(std::time::SystemTime::now())
            }

            // Process objects
            for obj in list_result.objects {
                let last_modified = Some(chrono_to_system_time(obj.last_modified));

                metas.push(ObjectMetadata {
                    path: obj.location.to_string(),
                    size: obj.size as u64,
                    last_modified,
                    content_type: None,
                    is_dir: false,
                });
            }

            // Process common prefixes (directories)
            for prefix in list_result.common_prefixes {
                metas.push(ObjectMetadata {
                    path: prefix.as_ref().to_string(),
                    size: 0,
                    last_modified: None,
                    content_type: None,
                    is_dir: true,
                });
            }

            Ok::<Vec<ObjectMetadata>, StorageError>(metas)
        })?;

        Ok(result)
    }

    fn delete(&self, path: &Path) -> Result<()> {
        let key = self.path_to_key(path);
        let store = self.store.clone();

        self.runtime
            .block_on(async {
                store.delete(&key).await.map_err(|e| match e {
                    object_store::Error::NotFound { .. } => {
                        StorageError::not_found(path.display().to_string())
                    }
                    _ => StorageError::Cloud(e.to_string()),
                })
            })?;

        Ok(())
    }

    fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let from_key = self.path_to_key(from);
        let to_key = self.path_to_key(to);
        let store = self.store.clone();

        self.runtime.block_on(async {
            store.copy(&from_key, &to_key).await.map_err(|e| match e {
                object_store::Error::NotFound { .. } => {
                    StorageError::not_found(from.display().to_string())
                }
                _ => StorageError::Cloud(e.to_string()),
            })
        })?;

        Ok(())
    }

    fn create_dir(&self, _path: &Path) -> Result<()> {
        // OSS doesn't have directories - this is a no-op
        Ok(())
    }

    fn create_dir_all(&self, _path: &Path) -> Result<()> {
        // OSS doesn't have directories - this is a no-op
        Ok(())
    }
}

impl std::fmt::Debug for OssStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OssStorage")
            .field("bucket", &self.config.bucket)
            .field("endpoint", &self.config.endpoint)
            .field("prefix", &self.config.prefix)
            .finish()
    }
}

/// A writer that buffers data and uploads to OSS on flush/drop.
struct OssWriter {
    /// Buffer for data to be uploaded
    buffer: Vec<u8>,
    /// The object_store client
    store: Arc<dyn object_store::ObjectStore>,
    /// Tokio runtime handle for async operations
    runtime: tokio::runtime::Handle,
    /// The key to write to
    key: object_store::path::Path,
    /// Whether data has been uploaded
    uploaded: bool,
    /// Maximum buffer size before automatic upload
    max_buffer_size: usize,
}

impl OssWriter {
    /// Create a new OSS writer.
    fn new(
        store: Arc<dyn object_store::ObjectStore>,
        runtime: tokio::runtime::Handle,
        key: object_store::path::Path,
    ) -> Self {
        Self {
            buffer: Vec::new(),
            store,
            runtime,
            key,
            uploaded: false,
            max_buffer_size: 100 * 1024 * 1024, // 100 MB default
        }
    }

    /// Set the maximum buffer size.
    #[allow(dead_code)]
    fn with_max_buffer_size(mut self, size: usize) -> Self {
        self.max_buffer_size = size;
        self
    }

    /// Upload the buffer to OSS.
    fn upload(&mut self) -> Result<()> {
        if self.uploaded {
            return Ok(());
        }

        let buffer_bytes = std::mem::take(&mut self.buffer);
        let bytes = bytes::Bytes::from(buffer_bytes);
        let payload = object_store::PutPayload::from_bytes(bytes);
        let key = self.key.clone();
        let store = self.store.clone();

        self.runtime
            .block_on(async {
                store.put(&key, payload).await.map_err(|e| {
                    StorageError::Cloud(format!("Failed to upload to OSS: {}", e))
                })
            })?;

        self.uploaded = true;
        Ok(())
    }
}

impl Write for OssWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = buf.len();
        self.buffer.extend_from_slice(buf);

        // Auto-upload if buffer exceeds max size
        if self.buffer.len() > self.max_buffer_size {
            self.upload().map_err(|e| std::io::Error::other(format!("Upload failed: {}", e)))?;
        }

        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.upload().map_err(|e| std::io::Error::other(format!("Flush failed: {}", e)))
    }
}

impl Drop for OssWriter {
    fn drop(&mut self) {
        // Try to upload on drop if not already uploaded
        if !self.uploaded && !self.buffer.is_empty() && let Err(e) = self.upload() {
            tracing::error!("Failed to upload OSS data on drop: {}", e);
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oss_config_new() {
        let config = OssConfig::new("my-bucket", "oss-cn-hangzhou.aliyuncs.com", "key", "secret");
        assert_eq!(config.bucket, "my-bucket");
        assert_eq!(config.endpoint, "oss-cn-hangzhou.aliyuncs.com");
        assert_eq!(config.access_key_id, "key");
        assert_eq!(config.access_key_secret, "secret");
        assert!(config.prefix.is_none());
        assert!(!config.use_internal_endpoint);
    }

    #[test]
    fn test_oss_config_with_prefix() {
        let config = OssConfig::new("bucket", "endpoint", "key", "secret")
            .with_prefix("data/test")
            .with_region("cn-hangzhou");

        assert_eq!(config.prefix, Some("data/test".to_string()));
        assert_eq!(config.region, Some("cn-hangzhou".to_string()));
    }

    #[test]
    fn test_oss_config_full_key_without_prefix() {
        let config = OssConfig::new("bucket", "endpoint", "key", "secret");
        assert_eq!(config.full_key(Path::new("test.txt")), "test.txt");
        assert_eq!(
            config.full_key(Path::new("data/test.txt")),
            "data/test.txt"
        );
    }

    #[test]
    fn test_oss_config_full_key_with_prefix() {
        let config = OssConfig::new("bucket", "endpoint", "key", "secret")
            .with_prefix("datasets");
        assert_eq!(config.full_key(Path::new("test.txt")), "datasets/test.txt");
        assert_eq!(
            config.full_key(Path::new("data/test.txt")),
            "datasets/data/test.txt"
        );
    }

    #[test]
    fn test_oss_config_full_key_with_trailing_slash_prefix() {
        let config = OssConfig::new("bucket", "endpoint", "key", "secret")
            .with_prefix("datasets/");
        assert_eq!(config.full_key(Path::new("test.txt")), "datasets/test.txt");
    }

    #[test]
    fn test_oss_config_endpoint_url() {
        let config = OssConfig::new("bucket", "oss-cn-hangzhou.aliyuncs.com", "key", "secret");
        assert_eq!(
            config.endpoint_url(),
            "https://oss-cn-hangzhou.aliyuncs.com"
        );
    }

    #[test]
    fn test_oss_config_endpoint_url_already_https() {
        let config = OssConfig::new("bucket", "https://custom.endpoint.com", "key", "secret");
        assert_eq!(config.endpoint_url(), "https://custom.endpoint.com");
    }
}
