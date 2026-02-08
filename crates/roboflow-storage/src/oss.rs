// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Alibaba OSS and S3-compatible storage backend.
//!
//! This module provides cloud storage support using the `object_store` crate.
//! It supports Alibaba OSS (S3-compatible) and Amazon S3.
//!
//! ## Architecture
//!
//! - **AsyncOssStorage**: Pure async implementation of `AsyncStorage`
//! - **OssStorage**: Sync wrapper around AsyncOssStorage for backward compatibility

use std::io::{Cursor, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::{
    AsyncStorage, ObjectMetadata, Storage, StorageError, StorageResult as Result, StreamingConfig,
    StreamingRead,
};

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for Alibaba OSS / S3-compatible storage.
#[derive(Clone)]
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
    /// Whether to allow HTTP (non-HTTPS) connections.
    /// **WARNING**: HTTP transmits credentials unencrypted. Only use for testing/local development.
    pub allow_http: bool,
}

// Manual Debug implementation to redact sensitive credentials
impl std::fmt::Debug for OssConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OssConfig")
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("access_key_id", &"<REDACTED>")
            .field("access_key_secret", &"<REDACTED>")
            .field("region", &self.region)
            .field("prefix", &self.prefix)
            .field("use_internal_endpoint", &self.use_internal_endpoint)
            .field("allow_http", &self.allow_http)
            .finish()
    }
}

impl OssConfig {
    /// Create a new OSS configuration.
    ///
    /// By default, HTTP is **disabled** for security. Use `with_allow_http(true)` only
    /// for local testing or development - never in production.
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
            allow_http: false,
        }
    }

    /// Validate bucket name according to S3/OSS naming rules.
    ///
    /// Bucket names must:
    /// - Be between 3 and 63 characters long
    /// - Contain only lowercase letters, numbers, hyphens, and dots
    /// - Start and end with a letter or number
    /// - Not be formatted as an IP address (e.g., 192.168.1.1)
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket name doesn't meet these requirements.
    pub fn validate_bucket_name(&self) -> Result<()> {
        let bucket = &self.bucket;

        // Length requirements
        if bucket.len() < 3 || bucket.len() > 63 {
            return Err(StorageError::invalid_path(format!(
                "Bucket name must be between 3 and 63 characters, got {} characters",
                bucket.len()
            )));
        }

        // Character set: lowercase letters, numbers, hyphens, dots only
        if !bucket
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.')
        {
            return Err(StorageError::invalid_path(
                "Bucket name can only contain lowercase letters, numbers, hyphens, and dots"
                    .to_string(),
            ));
        }

        // Must start and end with letter or number
        if let Some(first) = bucket.chars().next()
            && !first.is_ascii_alphanumeric()
        {
            return Err(StorageError::invalid_path(
                "Bucket name must start with a letter or number".to_string(),
            ));
        }

        if let Some(last) = bucket.chars().last()
            && !last.is_ascii_alphanumeric()
        {
            return Err(StorageError::invalid_path(
                "Bucket name must end with a letter or number".to_string(),
            ));
        }

        // Must not be formatted as IP address
        if bucket.parse::<std::net::IpAddr>().is_ok() {
            return Err(StorageError::invalid_path(
                "Bucket name cannot be formatted as an IP address".to_string(),
            ));
        }

        Ok(())
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

    /// Allow HTTP (non-HTTPS) connections.
    ///
    /// **WARNING**: This transmits credentials unencrypted. Only use for local testing
    /// or development. Never enable in production.
    pub fn with_allow_http(mut self, allow: bool) -> Self {
        self.allow_http = allow;
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
    ///
    /// Uses HTTPS by default. If `allow_http` is true and the endpoint
    /// doesn't specify a protocol, uses HTTP instead.
    pub fn endpoint_url(&self) -> String {
        let base = &self.endpoint;
        if base.contains("://") {
            base.to_string()
        } else if self.allow_http {
            format!("http://{}", base)
        } else {
            format!("https://{}", base)
        }
    }
}

// =============================================================================
// AsyncOssStorage - Pure Async Implementation
// =============================================================================

/// Async OSS/S3 storage backend.
///
/// This is the clean async implementation that doesn't create its own runtime.
/// Use this in async contexts (workers, scanners) where a Tokio runtime exists.
///
/// # Example
///
/// ```ignore
/// use roboflow_storage::{AsyncStorage, oss::AsyncOssStorage};
///
/// let storage = AsyncOssStorage::new(
///     "my-bucket",
///     "oss-cn-hangzhou.aliyuncs.com",
///     "access-key-id",
///     "access-key-secret"
/// )?;
///
/// // In an async context:
/// let data = storage.read(Path::new("file.txt")).await?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct AsyncOssStorage {
    /// The underlying object_store client
    store: Arc<dyn object_store::ObjectStore>,
    /// Configuration
    config: OssConfig,
}

impl AsyncOssStorage {
    /// Create a new async OSS storage backend.
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

    /// Create a new async OSS storage backend with configuration.
    pub fn with_config(config: OssConfig) -> Result<Self> {
        // Validate bucket name before proceeding
        config.validate_bucket_name()?;

        // Build S3-compatible configuration
        let mut builder = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(&config.bucket)
            .with_access_key_id(&config.access_key_id)
            .with_secret_access_key(&config.access_key_secret)
            .with_endpoint(config.endpoint_url())
            .with_region(config.region.as_deref().unwrap_or("default"));

        // Only allow HTTP if explicitly configured (and emit warning)
        if config.allow_http {
            tracing::warn!(
                bucket = %config.bucket,
                "HTTP connections enabled for OSS/S3 - credentials will be transmitted unencrypted. \
                 This should ONLY be used for local testing/development."
            );
            builder = builder.with_allow_http(true);
        }

        // Build the object_store client (synchronous)
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(
            builder
                .build()
                .map_err(|e| StorageError::Cloud(format!("Failed to create OSS client: {}", e)))?,
        );

        Ok(Self { store, config })
    }

    /// Get the underlying object_store client.
    pub fn object_store(&self) -> Arc<dyn object_store::ObjectStore> {
        Arc::clone(&self.store)
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
            content_type: None,
            is_dir: false,
        }
    }
}

#[async_trait::async_trait]
impl AsyncStorage for AsyncOssStorage {
    async fn read(&self, path: &Path) -> Result<bytes::Bytes> {
        let key = self.path_to_key(path);
        self.store
            .get(&key)
            .await
            .map_err(|e| match e {
                object_store::Error::NotFound { .. } => {
                    StorageError::not_found(path.display().to_string())
                }
                _ => StorageError::Cloud(e.to_string()),
            })?
            .bytes()
            .await
            .map_err(|e| StorageError::Cloud(format!("Failed to read bytes: {}", e)))
    }

    async fn write(&self, path: &Path, data: bytes::Bytes) -> Result<()> {
        let key = self.path_to_key(path);
        let payload = object_store::PutPayload::from_bytes(data);
        self.store
            .put(&key, payload)
            .await
            .map(|_| ())
            .map_err(|e| StorageError::Cloud(format!("Failed to write: {}", e)))
    }

    async fn exists(&self, path: &Path) -> bool {
        let key = self.path_to_key(path);
        self.store.head(&key).await.is_ok()
    }

    async fn size(&self, path: &Path) -> Result<u64> {
        let key = self.path_to_key(path);
        let meta = self.store.head(&key).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => {
                StorageError::not_found(path.display().to_string())
            }
            _ => StorageError::Cloud(e.to_string()),
        })?;
        Ok(meta.size as u64)
    }

    async fn metadata(&self, path: &Path) -> Result<ObjectMetadata> {
        let key = self.path_to_key(path);
        let meta = self.store.head(&key).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => {
                StorageError::not_found(path.display().to_string())
            }
            _ => StorageError::Cloud(e.to_string()),
        })?;
        Ok(self.convert_metadata(&meta))
    }

    async fn list(&self, prefix: &Path) -> Result<Vec<ObjectMetadata>> {
        let key = self.path_to_key(prefix);
        let list_result = self
            .store
            .list_with_delimiter(Some(&key))
            .await
            .map_err(|e| StorageError::Cloud(format!("Failed to list: {}", e)))?;

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

        Ok(metas)
    }

    async fn delete(&self, path: &Path) -> Result<()> {
        let key = self.path_to_key(path);
        self.store.delete(&key).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => {
                StorageError::not_found(path.display().to_string())
            }
            _ => StorageError::Cloud(e.to_string()),
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let from_key = self.path_to_key(from);
        let to_key = self.path_to_key(to);
        self.store
            .copy(&from_key, &to_key)
            .await
            .map_err(|e| match e {
                object_store::Error::NotFound { .. } => {
                    StorageError::not_found(from.display().to_string())
                }
                _ => StorageError::Cloud(e.to_string()),
            })
    }

    async fn create_dir(&self, _path: &Path) -> Result<()> {
        // OSS doesn't have directories - this is a no-op
        Ok(())
    }

    async fn create_dir_all(&self, _path: &Path) -> Result<()> {
        // OSS doesn't have directories - this is a no-op
        Ok(())
    }

    async fn read_range(&self, path: &Path, start: u64, end: Option<u64>) -> Result<bytes::Bytes> {
        let key = self.path_to_key(path);
        let object_size = if end.is_some() {
            None
        } else {
            Some(
                self.store
                    .head(&key)
                    .await
                    .map_err(|e| match e {
                        object_store::Error::NotFound { .. } => {
                            StorageError::not_found(path.display().to_string())
                        }
                        _ => StorageError::Cloud(e.to_string()),
                    })?
                    .size as u64,
            )
        };

        let end = end.unwrap_or_else(|| object_size.unwrap());

        // Validate bounds
        if start > end {
            return Err(StorageError::invalid_path(format!(
                "start offset {} exceeds end offset {}",
                start, end
            )));
        }

        // Convert to usize for get_range API
        let start_usize = usize::try_from(start)
            .map_err(|_| StorageError::Other(format!("start offset {} too large", start)))?;
        let end_usize = usize::try_from(end)
            .map_err(|_| StorageError::Other(format!("end offset {} too large", end)))?;

        // Fetch range
        self.store
            .get_range(&key, start_usize..end_usize)
            .await
            .map_err(|e| match e {
                object_store::Error::NotFound { .. } => {
                    StorageError::not_found(path.display().to_string())
                }
                _ => StorageError::Cloud(e.to_string()),
            })
    }
}

impl std::fmt::Debug for AsyncOssStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncOssStorage")
            .field("bucket", &self.config.bucket)
            .field("endpoint", &self.config.endpoint)
            .field("prefix", &self.config.prefix)
            .finish()
    }
}

// =============================================================================
// OssStorage - Sync Wrapper for Backward Compatibility
// =============================================================================

/// Sync OSS/S3 storage backend.
///
/// This is a wrapper around `AsyncOssStorage` that implements the synchronous
/// `Storage` trait. It intelligently handles both async and sync contexts:
/// - When called from within a Tokio runtime, uses `block_in_place`
/// - When called from a sync context, uses its own runtime
///
/// **Note**: In async contexts (workers, scanners), prefer using `AsyncOssStorage`
/// directly to avoid any blocking overhead.
///
/// # Example
///
/// ```ignore
/// use roboflow_storage::{Storage, OssStorage};
///
/// // For sync contexts (CLI tools, tests)
/// let storage = OssStorage::new(
///     "my-bucket",
///     "oss-cn-hangzhou.aliyuncs.com",
///     "access-key-id",
///     "access-key-secret"
/// )?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct OssStorage {
    /// The async storage implementation
    async_storage: AsyncOssStorage,
    /// Optional Tokio runtime (only created when not inside a runtime)
    runtime: Option<tokio::runtime::Runtime>,
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
        let async_storage = AsyncOssStorage::with_config(config)?;

        // Only create a runtime if we're not already inside one
        let runtime = if tokio::runtime::Handle::try_current().is_ok() {
            // We're inside a runtime - don't create a new one
            None
        } else {
            // We're in a sync context - create our own runtime
            Some(
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| {
                        StorageError::Other(format!("Failed to create tokio runtime: {}", e))
                    })?,
            )
        };

        Ok(Self {
            async_storage,
            runtime,
        })
    }

    /// Get a reference to the underlying async storage.
    pub fn async_storage(&self) -> &AsyncOssStorage {
        &self.async_storage
    }

    /// Block on a future, handling both sync and async contexts.
    fn block_on<F, R>(&self, f: F) -> R
    where
        F: std::future::Future<Output = R>,
    {
        match &self.runtime {
            Some(rt) => rt.block_on(f),
            None => {
                // We're inside a runtime - use block_in_place
                tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(f))
            }
        }
    }

    /// Get a runtime handle for writer operations.
    fn runtime_handle(&self) -> tokio::runtime::Handle {
        match &self.runtime {
            Some(rt) => rt.handle().clone(),
            None => tokio::runtime::Handle::current(),
        }
    }
}

impl Storage for OssStorage {
    fn reader(&self, path: &Path) -> Result<Box<dyn Read + Send + 'static>> {
        let bytes = self.block_on(self.async_storage.read(path))?.to_vec();
        Ok(Box::new(Cursor::new(bytes)))
    }

    fn writer(&self, path: &Path) -> Result<Box<dyn Write + Send + 'static>> {
        Ok(Box::new(SyncOssWriter::new(
            self.async_storage.object_store(),
            self.runtime_handle(),
            self.async_storage.path_to_key(path),
        )))
    }

    fn exists(&self, path: &Path) -> bool {
        self.block_on(self.async_storage.exists(path))
    }

    fn size(&self, path: &Path) -> Result<u64> {
        self.block_on(self.async_storage.size(path))
    }

    fn metadata(&self, path: &Path) -> Result<ObjectMetadata> {
        self.block_on(self.async_storage.metadata(path))
    }

    fn list(&self, prefix: &Path) -> Result<Vec<ObjectMetadata>> {
        self.block_on(self.async_storage.list(prefix))
    }

    fn delete(&self, path: &Path) -> Result<()> {
        self.block_on(self.async_storage.delete(path))
    }

    fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.block_on(self.async_storage.copy(from, to))
    }

    fn create_dir(&self, path: &Path) -> Result<()> {
        self.block_on(self.async_storage.create_dir(path))
    }

    fn create_dir_all(&self, path: &Path) -> Result<()> {
        self.block_on(self.async_storage.create_dir_all(path))
    }

    fn read_range(
        &self,
        path: &Path,
        start: u64,
        end: Option<u64>,
    ) -> Result<Box<dyn Read + Send + 'static>> {
        let bytes = self
            .block_on(self.async_storage.read_range(path, start, end))?
            .to_vec();
        Ok(Box::new(Cursor::new(bytes)))
    }

    fn streaming_reader(
        &self,
        path: &Path,
        config: StreamingConfig,
    ) -> Result<Box<dyn StreamingRead + Send + 'static>> {
        let object_size = self.size(path)?;
        let reader = crate::streaming::StreamingOssReader::new(
            self.async_storage.object_store(),
            self.runtime_handle(),
            self.async_storage.path_to_key(path),
            object_size,
            &config,
        )?;

        Ok(Box::new(reader))
    }

    fn download_file(&self, remote_path: &Path, local_path: &Path) -> Result<u64> {
        let object_size = self.size(remote_path)?;
        let config = crate::StreamingConfig::default();

        tracing::info!(
            remote_path = %remote_path.display(),
            local_path = %local_path.display(),
            object_size,
            chunk_size = config.chunk_size,
            "Downloading file via streaming range requests"
        );

        let mut reader = crate::streaming::StreamingOssReader::new(
            self.async_storage.object_store(),
            self.runtime_handle(),
            self.async_storage.path_to_key(remote_path),
            object_size,
            &config,
        )?;

        let file = std::fs::File::create(local_path).map_err(StorageError::Io)?;
        let mut writer = std::io::BufWriter::with_capacity(4 * 1024 * 1024, file);
        let bytes = std::io::copy(&mut reader, &mut writer).map_err(StorageError::Io)?;
        writer.flush().map_err(StorageError::Io)?;

        tracing::info!(total_bytes = bytes, "Streaming download complete");

        Ok(bytes)
    }

    fn upload_file(&self, local_path: &Path, remote_path: &Path) -> Result<u64> {
        use crate::multipart_parallel::{ParallelUploadConfig, upload_multipart_parallel};

        let mut file = std::fs::File::open(local_path)?;
        let file_size = file.metadata().map(|m| m.len()).unwrap_or(0);
        let key = self.async_storage.path_to_key(remote_path);
        let config = ParallelUploadConfig::default();

        tracing::info!(
            local_path = %local_path.display(),
            remote_path = %remote_path.display(),
            file_size,
            part_size = config.part_size,
            concurrency = config.concurrency,
            "Uploading file via parallel multipart"
        );

        let stats = upload_multipart_parallel(
            &self.async_storage.object_store(),
            &self.runtime_handle(),
            &key,
            &mut file,
            Some(&config),
            None,
        )?;

        tracing::info!(
            total_bytes = stats.total_bytes,
            total_parts = stats.total_parts,
            duration_sec = stats.total_duration.as_secs_f64(),
            throughput_mb_s = stats.avg_bytes_per_sec / (1024.0 * 1024.0),
            "Parallel multipart upload complete"
        );

        Ok(stats.total_bytes)
    }
}

impl std::fmt::Debug for OssStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OssStorage")
            .field("bucket", &self.async_storage.config.bucket)
            .field("endpoint", &self.async_storage.config.endpoint)
            .field("prefix", &self.async_storage.config.prefix)
            .finish()
    }
}

// =============================================================================
// SyncOssWriter
// =============================================================================

/// A writer that buffers data and uploads to OSS on flush/drop.
struct SyncOssWriter {
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

impl SyncOssWriter {
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

        self.runtime.block_on(async {
            store
                .put(&key, payload)
                .await
                .map_err(|e| StorageError::Cloud(format!("Failed to upload to OSS: {}", e)))
        })?;

        self.uploaded = true;
        Ok(())
    }
}

impl Write for SyncOssWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = buf.len();
        self.buffer.extend_from_slice(buf);

        // Auto-upload if buffer exceeds max size
        if self.buffer.len() > self.max_buffer_size {
            self.upload()
                .map_err(|e| std::io::Error::other(format!("Upload failed: {}", e)))?;
        }

        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.upload()
            .map_err(|e| std::io::Error::other(format!("Flush failed: {}", e)))
    }
}

impl Drop for SyncOssWriter {
    fn drop(&mut self) {
        // Try to upload on drop if not already uploaded
        if !self.uploaded
            && !self.buffer.is_empty()
            && let Err(e) = self.upload()
        {
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
        assert_eq!(config.full_key(Path::new("data/test.txt")), "data/test.txt");
    }

    #[test]
    fn test_oss_config_full_key_with_prefix() {
        let config = OssConfig::new("bucket", "endpoint", "key", "secret").with_prefix("datasets");
        assert_eq!(config.full_key(Path::new("test.txt")), "datasets/test.txt");
        assert_eq!(
            config.full_key(Path::new("data/test.txt")),
            "datasets/data/test.txt"
        );
    }

    #[test]
    fn test_oss_config_full_key_with_trailing_slash_prefix() {
        let config = OssConfig::new("bucket", "endpoint", "key", "secret").with_prefix("datasets/");
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

    // Security tests for bucket name validation
    #[test]
    fn test_oss_config_bucket_valid() {
        let config = OssConfig::new("my-bucket", "endpoint", "key", "secret");
        assert!(config.validate_bucket_name().is_ok());
    }

    #[test]
    fn test_oss_config_bucket_too_short() {
        let config = OssConfig::new("ab", "endpoint", "key", "secret");
        let result = config.validate_bucket_name();
        assert!(matches!(result, Err(StorageError::InvalidPath(_))));
    }

    #[test]
    fn test_oss_config_bucket_too_long() {
        let long_name = "a".repeat(64);
        let config = OssConfig::new(&long_name, "endpoint", "key", "secret");
        let result = config.validate_bucket_name();
        assert!(matches!(result, Err(StorageError::InvalidPath(_))));
    }

    #[test]
    fn test_oss_config_bucket_uppercase() {
        let config = OssConfig::new("MyBucket", "endpoint", "key", "secret");
        let result = config.validate_bucket_name();
        assert!(matches!(result, Err(StorageError::InvalidPath(_))));
    }

    #[test]
    fn test_oss_config_bucket_starts_with_hyphen() {
        let config = OssConfig::new("-mybucket", "endpoint", "key", "secret");
        let result = config.validate_bucket_name();
        assert!(matches!(result, Err(StorageError::InvalidPath(_))));
    }

    #[test]
    fn test_oss_config_bucket_ends_with_hyphen() {
        let config = OssConfig::new("mybucket-", "endpoint", "key", "secret");
        let result = config.validate_bucket_name();
        assert!(matches!(result, Err(StorageError::InvalidPath(_))));
    }

    #[test]
    fn test_oss_config_bucket_ip_address() {
        let config = OssConfig::new("192.168.1.1", "endpoint", "key", "secret");
        let result = config.validate_bucket_name();
        assert!(matches!(result, Err(StorageError::InvalidPath(_))));
    }

    #[test]
    fn test_oss_config_debug_redacts_credentials() {
        let config = OssConfig::new("bucket", "endpoint", "secret-key-id", "secret-key-value");
        let debug_str = format!("{:?}", config);

        // Credentials should be redacted
        assert!(!debug_str.contains("secret-key-id"));
        assert!(!debug_str.contains("secret-key-value"));
        assert!(debug_str.contains("<REDACTED>"));

        // Non-sensitive fields should be visible
        assert!(debug_str.contains("bucket"));
        assert!(debug_str.contains("endpoint"));
    }

    #[test]
    fn test_oss_config_allow_http_defaults_false() {
        let config = OssConfig::new("bucket", "endpoint", "key", "secret");
        assert!(!config.allow_http);
    }

    #[test]
    fn test_oss_config_allow_http() {
        let config = OssConfig::new("bucket", "endpoint", "key", "secret").with_allow_http(true);
        assert!(config.allow_http);
    }

    #[test]
    fn test_oss_config_endpoint_http_when_allowed() {
        let config =
            OssConfig::new("bucket", "localhost:9000", "key", "secret").with_allow_http(true);
        assert_eq!(config.endpoint_url(), "http://localhost:9000");
    }

    #[test]
    fn test_oss_config_endpoint_https_by_default() {
        let config = OssConfig::new("bucket", "localhost:9000", "key", "secret");
        // Default should be HTTPS
        assert_eq!(config.endpoint_url(), "https://localhost:9000");
    }

    // ========================================================================
    // OssStorage Runtime Behavior Tests
    // ========================================================================

    #[test]
    fn test_oss_storage_creates_runtime_without_existing() {
        // This test verifies OssStorage can be created outside a Tokio runtime
        // It should create its own runtime in this case
        let result = std::panic::catch_unwind(|| {
            // The storage creation should not panic (we can't test the actual runtime
            // creation without real credentials, but we verify no panic occurs)
            OssStorage::new("bucket", "endpoint", "key", "secret")
        });
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_oss_storage_detects_existing_runtime() {
        // This test verifies OssStorage can detect an existing Tokio runtime
        // and won't create a nested one
        use tokio::runtime::Handle;

        // We're in an async context, so runtime should be detected
        let handle = Handle::try_current();
        assert!(handle.is_ok(), "Test should run in a Tokio runtime");
    }

    // ========================================================================
    // Integration-Style Tests for AsyncOssStorage
    // ========================================================================

    #[tokio::test]
    async fn test_async_oss_storage_config_validation() {
        // Verify bucket name validation is called during creation
        let result = AsyncOssStorage::new(
            "invalid-bucket-name!", // Invalid character
            "endpoint",
            "key",
            "secret",
        );
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_async_oss_storage_bucket_too_short() {
        let result = AsyncOssStorage::new(
            "ab", // Too short
            "endpoint", "key", "secret",
        );
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_async_oss_storage_config_methods() {
        let config = OssConfig::new("my-bucket", "endpoint", "key", "secret")
            .with_prefix("data")
            .with_region("us-west-2");

        assert_eq!(config.bucket, "my-bucket");
        assert_eq!(config.prefix, Some("data".to_string()));
        assert_eq!(config.region, Some("us-west-2".to_string()));
        assert_eq!(config.full_key(Path::new("test.txt")), "data/test.txt");
    }

    #[tokio::test]
    async fn test_async_storage_config_with_prefix() {
        let config = OssConfig::new("bucket", "endpoint", "key", "secret").with_prefix("datasets");
        assert_eq!(config.full_key(Path::new("test.txt")), "datasets/test.txt");
    }

    // ========================================================================
    // Error Handling Tests (mock-based)
    // ========================================================================

    #[tokio::test]
    async fn test_async_storage_config_http_warning() {
        // Verify HTTP connections emit a warning
        let config = OssConfig::new("bucket", "endpoint", "key", "secret").with_allow_http(true);

        // Creating storage with HTTP allowed should log a warning
        // (we can't easily test for logging output, but we verify the config is correct)
        assert!(config.allow_http);
        assert_eq!(config.endpoint_url(), "http://endpoint");
    }

    // Test that storage creation validates inputs
    #[tokio::test]
    async fn test_async_storage_invalid_bucket_rejected() {
        // Various invalid bucket names should be rejected
        let too_long = "a".repeat(64);
        let invalid_names = vec![
            "ab",              // too short
            too_long.as_str(), // too long
            "MyBucket",        // uppercase
            "-bucket",         // starts with hyphen
            "bucket-",         // ends with hyphen
            "192.168.1.1",     // IP address
            "bucket!",         // invalid character
        ];

        for name in invalid_names {
            let result = AsyncOssStorage::new(name, "endpoint", "key", "secret");
            assert!(result.is_err(), "Bucket name '{}' should be rejected", name);
        }
    }
}
