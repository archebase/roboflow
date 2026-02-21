// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Storage retry logic with exponential backoff.
//!
//! This module provides retry mechanisms for storage operations,
//! helping to handle transient network failures and rate limiting.
//!
//! The retry configuration and core logic are re-exported from `roboflow_core::retry`,
//! while this module provides the `RetryingStorage` wrapper specific to storage operations.
//!
//! # Example
//!
//! ```ignore
//! use roboflow::storage::{Storage, S3Storage};
//! use roboflow::storage::retry::{RetryConfig, RetryingStorage};
//!
//! let storage = S3Storage::new(...)?;
//! let retrying_storage = RetryingStorage::new(storage, RetryConfig::default());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::{ObjectMetadata, Storage, StorageError, StorageResult as Result};

// Re-export retry types from roboflow_core
pub use roboflow_core::retry::{RetryConfig, retry_with_backoff};

// Implement IsRetryableRef for StorageError so it can be used with retry_with_backoff
impl roboflow_core::retry::IsRetryableRef for StorageError {
    fn is_retryable_ref(&self) -> bool {
        self.is_retryable()
    }
}

/// A storage wrapper that adds retry logic to all operations.
///
/// This wraps any `Storage` implementation and automatically retries
/// operations that fail with retryable errors (network errors, timeouts, etc.).
///
/// # Example
///
/// ```ignore
/// use roboflow::storage::{Storage, LocalStorage, retry::{RetryConfig, RetryingStorage}};
///
/// let storage = LocalStorage::new("/tmp")?;
/// let retrying = RetryingStorage::new(storage, RetryConfig::default());
/// // Now all operations through `retrying` will have retry logic
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct RetryingStorage {
    /// The underlying storage implementation
    inner: Arc<dyn Storage>,
    /// Retry configuration
    config: RetryConfig,
}

impl RetryingStorage {
    /// Create a new retrying storage wrapper.
    pub fn new(inner: Arc<dyn Storage>, config: RetryConfig) -> Self {
        Self { inner, config }
    }

    /// Create a new retrying storage wrapper with default configuration.
    pub fn with_default(inner: Arc<dyn Storage>) -> Self {
        Self::new(inner, RetryConfig::default())
    }

    /// Get a reference to the retry configuration.
    pub fn config(&self) -> &RetryConfig {
        &self.config
    }

    /// Get a reference to the inner storage.
    pub fn inner(&self) -> &Arc<dyn Storage> {
        &self.inner
    }
}

impl Storage for RetryingStorage {
    fn reader(&self, path: &Path) -> Result<Box<dyn Read + Send + 'static>> {
        let path = path.to_owned();
        let inner = self.inner.clone();

        retry_with_backoff(&self.config, "reader", || inner.reader(&path))
    }

    fn writer(&self, path: &Path) -> Result<Box<dyn Write + Send + 'static>> {
        // Writer doesn't retry well because partial writes may have occurred
        // We'll just delegate directly without retry
        self.inner.writer(path)
    }

    fn exists(&self, path: &Path) -> bool {
        // exists() returns bool, so we can't use the standard retry logic
        // We'll just delegate directly
        self.inner.exists(path)
    }

    fn size(&self, path: &Path) -> Result<u64> {
        let path = path.to_owned();
        let inner = self.inner.clone();

        retry_with_backoff(&self.config, "size", || inner.size(&path))
    }

    fn metadata(&self, path: &Path) -> Result<ObjectMetadata> {
        let path = path.to_owned();
        let inner = self.inner.clone();

        retry_with_backoff(&self.config, "metadata", || inner.metadata(&path))
    }

    fn list(&self, prefix: &Path) -> Result<Vec<ObjectMetadata>> {
        let prefix = prefix.to_owned();
        let inner = self.inner.clone();

        retry_with_backoff(&self.config, "list", || inner.list(&prefix))
    }

    fn delete(&self, path: &Path) -> Result<()> {
        let path = path.to_owned();
        let inner = self.inner.clone();

        retry_with_backoff(&self.config, "delete", || inner.delete(&path))
    }

    fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let from = from.to_owned();
        let to = to.to_owned();
        let inner = self.inner.clone();

        retry_with_backoff(&self.config, "copy", || inner.copy(&from, &to))
    }

    fn create_dir(&self, path: &Path) -> Result<()> {
        let path = path.to_owned();
        let inner = self.inner.clone();

        retry_with_backoff(&self.config, "create_dir", || inner.create_dir(&path))
    }

    fn create_dir_all(&self, path: &Path) -> Result<()> {
        let path = path.to_owned();
        let inner = self.inner.clone();

        retry_with_backoff(&self.config, "create_dir_all", || {
            inner.create_dir_all(&path)
        })
    }
}

impl std::fmt::Debug for RetryingStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetryingStorage")
            .field("config", &self.config)
            .finish()
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use roboflow_core::retry::IsRetryableRef;

    // Use StorageError from parent module
    use super::super::StorageError;

    #[test]
    fn test_retry_config_reexport() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 5);
    }

    #[test]
    fn test_retry_config_builder() {
        let config = RetryConfig::new()
            .with_max_retries(10)
            .with_initial_backoff_ms(50)
            .with_max_backoff_ms(10000)
            .with_backoff_multiplier(3.0)
            .with_jitter(false);

        assert_eq!(config.max_retries, 10);
        assert_eq!(config.initial_backoff_ms, 50);
        assert_eq!(config.max_backoff_ms, 10000);
        assert_eq!(config.backoff_multiplier, 3.0);
        assert!(!config.jitter_enabled);
    }

    #[test]
    fn test_storage_error_is_retryable_ref() {
        // Network errors should be retryable
        let err = StorageError::NetworkError("temporary failure".to_string());
        assert!(err.is_retryable_ref());

        // Timeout errors should be retryable
        let err = StorageError::Timeout("connection timeout".to_string());
        assert!(err.is_retryable_ref());

        // Cloud errors should be retryable
        let err = StorageError::Cloud("service unavailable".to_string());
        assert!(err.is_retryable_ref());

        // NotFound should NOT be retryable
        let err = StorageError::NotFound("missing".to_string());
        assert!(!err.is_retryable_ref());

        // PermissionDenied should NOT be retryable
        let err = StorageError::PermissionDenied("access denied".to_string());
        assert!(!err.is_retryable_ref());
    }
}
