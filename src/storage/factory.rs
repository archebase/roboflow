// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Storage factory for creating storage backends from URLs.
//!
//! Provides automatic backend selection and credential loading.

use std::sync::Arc;

use super::{
    url::StorageUrl, LocalStorage, OssStorage, Result, SeekableStorage, Storage, StorageError,
};

/// Configuration for storage backend instantiation.
///
/// This struct holds credentials and settings for cloud storage backends.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// OSS access key ID.
    pub oss_access_key_id: Option<String>,
    /// OSS access key secret.
    pub oss_access_key_secret: Option<String>,
    /// OSS endpoint (e.g., oss-cn-hangzhou.aliyuncs.com).
    pub oss_endpoint: Option<String>,
    /// AWS S3 access key ID (fallback).
    pub aws_access_key_id: Option<String>,
    /// AWS S3 secret access key (fallback).
    pub aws_secret_access_key: Option<String>,
    /// Default region for S3.
    pub aws_region: Option<String>,
    /// Local buffer directory for cloud operations.
    pub local_buffer_dir: Option<String>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            oss_access_key_id: None,
            oss_access_key_secret: None,
            oss_endpoint: None,
            aws_access_key_id: None,
            aws_secret_access_key: None,
            aws_region: None,
            local_buffer_dir: None,
        }
    }
}

impl StorageConfig {
    /// Create a new empty storage config.
    pub fn new() -> Self {
        Self::default()
    }

    /// Load configuration from environment variables.
    ///
    /// Checks the following environment variables:
    /// - `OSS_ACCESS_KEY_ID` / `ALIYUN_ACCESS_KEY_ID` / `ALIYUN_OSS_ACCESS_KEY_ID`
    /// - `OSS_ACCESS_KEY_SECRET` / `ALIYUN_ACCESS_KEY_SECRET` / `ALIYUN_OSS_ACCESS_KEY_SECRET`
    /// - `OSS_ENDPOINT` / `ALIYUN_OSS_ENDPOINT`
    /// - `AWS_ACCESS_KEY_ID` (fallback for OSS)
    /// - `AWS_SECRET_ACCESS_KEY` (fallback for OSS)
    /// - `AWS_REGION` (for S3)
    ///
    /// OSS-specific variables take precedence over AWS variables.
    pub fn from_env() -> Self {
        let oss_access_key_id = std::env::var("OSS_ACCESS_KEY_ID")
            .or_else(|_| std::env::var("ALIYUN_ACCESS_KEY_ID"))
            .or_else(|_| std::env::var("ALIYUN_OSS_ACCESS_KEY_ID"))
            .ok();

        let oss_access_key_secret = std::env::var("OSS_ACCESS_KEY_SECRET")
            .or_else(|_| std::env::var("ALIYUN_ACCESS_KEY_SECRET"))
            .or_else(|_| std::env::var("ALIYUN_OSS_ACCESS_KEY_SECRET"))
            .ok();

        let oss_endpoint = std::env::var("OSS_ENDPOINT")
            .or_else(|_| std::env::var("ALIYUN_OSS_ENDPOINT"))
            .ok();

        let aws_access_key_id = std::env::var("AWS_ACCESS_KEY_ID").ok();
        let aws_secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok();
        let aws_region = std::env::var("AWS_REGION").ok();

        Self {
            oss_access_key_id,
            oss_access_key_secret,
            oss_endpoint,
            aws_access_key_id,
            aws_secret_access_key,
            aws_region,
            local_buffer_dir: std::env::var("ROBOFLOW_BUFFER_DIR").ok(),
        }
    }

    /// Set OSS credentials.
    pub fn with_oss_credentials(
        mut self,
        access_key_id: impl Into<String>,
        access_key_secret: impl Into<String>,
    ) -> Self {
        self.oss_access_key_id = Some(access_key_id.into());
        self.oss_access_key_secret = Some(access_key_secret.into());
        self
    }

    /// Set OSS endpoint.
    pub fn with_oss_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.oss_endpoint = Some(endpoint.into());
        self
    }

    /// Set AWS S3 credentials.
    pub fn with_aws_credentials(
        mut self,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Self {
        self.aws_access_key_id = Some(access_key_id.into());
        self.aws_secret_access_key = Some(secret_access_key.into());
        self
    }

    /// Set AWS region.
    pub fn with_aws_region(mut self, region: impl Into<String>) -> Self {
        self.aws_region = Some(region.into());
        self
    }

    /// Set local buffer directory.
    pub fn with_local_buffer_dir(mut self, dir: impl Into<String>) -> Self {
        self.local_buffer_dir = Some(dir.into());
        self
    }

    /// Get OSS access key ID, with AWS fallback.
    pub fn get_oss_key_id(&self) -> Option<&str> {
        self.oss_access_key_id
            .as_deref()
            .or(self.aws_access_key_id.as_deref())
    }

    /// Get OSS access key secret, with AWS fallback.
    pub fn get_oss_key_secret(&self) -> Option<&str> {
        self.oss_access_key_secret
            .as_deref()
            .or(self.aws_secret_access_key.as_deref())
    }

    /// Check if OSS credentials are configured.
    pub fn has_oss_credentials(&self) -> bool {
        self.get_oss_key_id().is_some() && self.get_oss_key_secret().is_some()
    }
}

/// Factory for creating storage backends from URLs.
///
/// This struct provides a unified interface for creating storage backends
/// based on URL schemes, with automatic credential loading from configuration.
pub struct StorageFactory {
    config: StorageConfig,
}

impl Default for StorageFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageFactory {
    /// Create a new factory with default (empty) configuration.
    pub fn new() -> Self {
        Self {
            config: StorageConfig::default(),
        }
    }

    /// Create a new factory with configuration loaded from environment.
    pub fn from_env() -> Self {
        Self {
            config: StorageConfig::from_env(),
        }
    }

    /// Create a new factory with custom configuration.
    pub fn with_config(config: StorageConfig) -> Self {
        Self { config }
    }

    /// Get a reference to the configuration.
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    /// Create a storage backend for the given URL.
    ///
    /// The URL scheme determines the backend:
    /// - `file://` or no scheme → `LocalStorage`
    /// - `s3://` → Error (not yet implemented)
    /// - `oss://` → `OssStorage` (stub)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The URL scheme is not recognized
    /// - Required credentials are missing for cloud storage
    /// - The URL is malformed
    pub fn create(&self, url: &str) -> Result<Arc<dyn Storage>> {
        let parsed_url: StorageUrl = url.parse()?;

        self.create_from_url(&parsed_url)
    }

    /// Create a storage backend from a parsed URL.
    pub fn create_from_url(&self, url: &StorageUrl) -> Result<Arc<dyn Storage>> {
        match url {
            StorageUrl::Local { path } => {
                // Use the parent directory as root for files, or the path itself for directories
                let root = if path.to_string_lossy().contains('.') {
                    // Looks like a file - use parent directory
                    path.parent()
                        .map(|p| p.to_path_buf())
                        .unwrap_or_else(|| PathBuf::from("."))
                } else {
                    // Use the path as root
                    path.clone()
                };
                Ok(Arc::new(LocalStorage::new(root)))
            }
            StorageUrl::S3 { bucket, endpoint, region, .. } => {
                #[cfg(feature = "cloud-storage")]
                {
                    // S3 is not yet fully implemented - return an error
                    Err(StorageError::other(format!(
                        "S3 storage not yet implemented (bucket: {})", bucket
                    )))
                }
                #[cfg(not(feature = "cloud-storage"))]
                {
                    Err(StorageError::other(
                        "cloud-storage feature is not enabled".to_string(),
                    ))
                }
            }
            StorageUrl::Oss { bucket, endpoint, .. } => {
                #[cfg(feature = "cloud-storage")]
                {
                    let key_id = self
                        .config
                        .get_oss_key_id()
                        .ok_or_else(|| {
                            StorageError::other(
                                "OSS credentials not found. Set OSS_ACCESS_KEY_ID and OSS_ACCESS_KEY_SECRET environment variables.".to_string(),
                            )
                        })?
                        .to_string();

                    let key_secret = self
                        .config
                        .get_oss_key_secret()
                        .ok_or_else(|| {
                            StorageError::other(
                                "OSS credentials not found. Set OSS_ACCESS_KEY_ID and OSS_ACCESS_KEY_SECRET environment variables.".to_string(),
                            )
                        })?
                        .to_string();

                    let ep = endpoint
                        .clone()
                        .or_else(|| self.config.oss_endpoint.clone())
                        .ok_or_else(|| {
                            StorageError::other(
                                "OSS endpoint not specified. Use ?endpoint= in URL or set OSS_ENDPOINT environment variable.".to_string(),
                            )
                        })?;

                    Ok(Arc::new(OssStorage::new(bucket, ep, key_id, key_secret)?))
                }
                #[cfg(not(feature = "cloud-storage"))]
                {
                    Err(StorageError::other(
                        "cloud-storage feature is not enabled".to_string(),
                    ))
                }
            }
        }
    }

    /// Create a seekable storage backend (only for local filesystem).
    ///
    /// For cloud storage URLs, returns the regular storage backend
    /// since seeking is not natively supported.
    pub fn create_seekable(&self, url: &str) -> Result<Arc<dyn SeekableStorage>> {
        let parsed_url: StorageUrl = url.parse()?;

        match &parsed_url {
            StorageUrl::Local { .. } => {
                // This will fail at runtime since Arc<dyn Storage> can't be downcast
                // Users should call create() and handle seekable separately
                Err(StorageError::other(
                    "Use create() and check for SeekableStorage support".to_string(),
                ))
            }
            _ => Err(StorageError::other(
                "Seekable storage only supported for local filesystem".to_string(),
            )),
        }
    }
}

// Need to import PathBuf for LocalStorage root handling
use std::path::PathBuf;

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_config_default() {
        let config = StorageConfig::default();
        assert!(config.oss_access_key_id.is_none());
        assert!(config.oss_access_key_secret.is_none());
    }

    #[test]
    fn test_storage_config_builder() {
        let config = StorageConfig::new()
            .with_oss_credentials("key_id", "key_secret")
            .with_oss_endpoint("oss-cn-hangzhou.aliyuncs.com")
            .with_aws_region("us-west-2");

        assert_eq!(config.oss_access_key_id.as_deref(), Some("key_id"));
        assert_eq!(config.oss_access_key_secret.as_deref(), Some("key_secret"));
        assert_eq!(config.oss_endpoint.as_deref(), Some("oss-cn-hangzhou.aliyuncs.com"));
        assert_eq!(config.aws_region.as_deref(), Some("us-west-2"));
    }

    #[test]
    fn test_storage_config_aws_fallback() {
        let config = StorageConfig::new()
            .with_aws_credentials("aws_key", "aws_secret");

        assert_eq!(config.get_oss_key_id(), Some("aws_key"));
        assert_eq!(config.get_oss_key_secret(), Some("aws_secret"));
        assert!(config.has_oss_credentials());
    }

    #[test]
    fn test_storage_factory_default() {
        let factory = StorageFactory::new();
        assert!(factory.create("/tmp/file.txt").is_ok());
    }

    #[test]
    fn test_storage_factory_create_local() {
        let factory = StorageFactory::new();
        let storage = factory.create("file:///tmp/file.txt").unwrap();
        // We can't directly check the type, but we can verify it works
        use std::path::Path;
        assert!(storage.exists(Path::new("/tmp")));
    }

    #[test]
    fn test_storage_factory_create_local_no_scheme() {
        let factory = StorageFactory::new();
        let storage = factory.create("/tmp/test.txt").unwrap();
        // Storage should be created successfully
        assert!(Arc::strong_count(&storage) == 1);
    }

    #[test]
    fn test_storage_factory_oss_missing_credentials() {
        let factory = StorageFactory::new();
        let result = factory.create("oss://bucket/file.txt");
        assert!(matches!(
            result,
            Err(StorageError::Other(_)) | Err(StorageError::Cloud(_))
        ));
    }

    #[test]
    fn test_storage_factory_oss_with_endpoint_in_url() {
        let factory = StorageFactory::with_config(
            StorageConfig::new().with_oss_credentials("key", "secret")
        );

        // Endpoint in URL should work
        let result =
            factory.create("oss://bucket/file.txt?endpoint=oss-cn-hangzhou.aliyuncs.com");
        assert!(result.is_ok());
    }

    #[test]
    fn test_storage_factory_invalid_url() {
        let factory = StorageFactory::new();
        let result = factory.create("ftp://server/file.txt");
        assert!(matches!(result, Err(StorageError::InvalidPath(_))));
    }
}
