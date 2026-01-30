// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Retry logic with exponential backoff for storage operations.
//!
//! This module provides retry mechanisms for cloud storage operations,
//! helping to handle transient network failures and rate limiting.
//!
//! # Example
//!
//! ```ignore
//! use roboflow::storage::{Storage, OssStorage};
//! use roboflow::storage::retry::{RetryConfig, RetryingStorage};
//!
//! let storage = OssStorage::new(...)?;
//! let retrying_storage = RetryingStorage::new(storage, RetryConfig::default());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use super::{ObjectMetadata, Result, Storage};

// Import StorageError for tests
#[cfg(test)]
use super::StorageError;

/// Configuration for retry behavior.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts.
    pub max_retries: u32,
    /// Initial backoff duration in milliseconds.
    pub initial_backoff_ms: u64,
    /// Maximum backoff duration in milliseconds.
    pub max_backoff_ms: u64,
    /// Multiplier for exponential backoff (e.g., 2.0 doubles each time).
    pub backoff_multiplier: f64,
    /// Whether to add jitter to prevent thundering herd.
    pub jitter_enabled: bool,
    /// Jitter percentage (0.0 to 1.0, e.g., 0.15 = ±15%).
    pub jitter_factor: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 5,
            initial_backoff_ms: 100,
            max_backoff_ms: 30000,
            backoff_multiplier: 2.0,
            jitter_enabled: true,
            jitter_factor: 0.15,
        }
    }
}

impl RetryConfig {
    /// Create a new retry configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the maximum number of retry attempts.
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Set the initial backoff duration in milliseconds.
    pub fn with_initial_backoff_ms(mut self, ms: u64) -> Self {
        self.initial_backoff_ms = ms;
        self
    }

    /// Set the maximum backoff duration in milliseconds.
    pub fn with_max_backoff_ms(mut self, ms: u64) -> Self {
        self.max_backoff_ms = ms;
        self
    }

    /// Set the backoff multiplier.
    pub fn with_backoff_multiplier(mut self, multiplier: f64) -> Self {
        self.backoff_multiplier = multiplier;
        self
    }

    /// Enable or disable jitter.
    pub fn with_jitter(mut self, enabled: bool) -> Self {
        self.jitter_enabled = enabled;
        self
    }

    /// Set the jitter factor.
    pub fn with_jitter_factor(mut self, factor: f64) -> Self {
        self.jitter_factor = factor.clamp(0.0, 1.0);
        self
    }

    /// Calculate the backoff duration for a given attempt.
    fn backoff_duration(&self, attempt: u32) -> Duration {
        let base_ms = self.initial_backoff_ms as f64 * self.backoff_multiplier.powi(attempt as i32);
        let clamped_ms = base_ms.clamp(0.0, self.max_backoff_ms as f64) as u64;

        if self.jitter_enabled {
            let jitter_range = (clamped_ms as f64 * self.jitter_factor) as u64;
            let jitter = if jitter_range > 0 {
                // Use a simple random jitter based on system time
                let nanos = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(0);
                ((nanos % (2 * jitter_range)) as i64) - jitter_range as i64
            } else {
                0
            };
            Duration::from_millis(clamped_ms.saturating_add_signed(jitter))
        } else {
            Duration::from_millis(clamped_ms)
        }
    }
}

/// Execute a function with retry logic.
///
/// # Arguments
///
/// * `config` - Retry configuration
/// * `operation_name` - Name of the operation for logging
/// * `f` - Function to execute (should return a `Result<T>`)
///
/// # Returns
///
/// Returns `Ok(T)` on success or the last error if all retries fail.
pub fn retry_with_backoff<T, F>(config: &RetryConfig, operation_name: &str, mut f: F) -> Result<T>
where
    F: FnMut() -> Result<T>,
{
    let mut last_error = None;

    for attempt in 0..=config.max_retries {
        match f() {
            Ok(result) => {
                if attempt > 0 {
                    tracing::info!("{} succeeded after {} retries", operation_name, attempt);
                }
                return Ok(result);
            }
            Err(err) => {
                last_error = Some(err);

                // Don't retry if the error is not retryable
                if !last_error.as_ref().unwrap().is_retryable() {
                    tracing::debug!(
                        "{} failed with non-retryable error: {}",
                        operation_name,
                        last_error.as_ref().unwrap()
                    );
                    return Err(last_error.unwrap());
                }

                // If this isn't the last attempt, wait and retry
                if attempt < config.max_retries {
                    let backoff = config.backoff_duration(attempt);
                    tracing::warn!(
                        "{} failed (attempt {}/{}), retrying after {:?}: {}",
                        operation_name,
                        attempt + 1,
                        config.max_retries + 1,
                        backoff,
                        last_error.as_ref().unwrap()
                    );
                    std::thread::sleep(backoff);
                }
            }
        }
    }

    tracing::error!(
        "{} failed after {} attempts",
        operation_name,
        config.max_retries + 1
    );
    Err(last_error.unwrap())
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

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.initial_backoff_ms, 100);
        assert_eq!(config.max_backoff_ms, 30000);
        assert_eq!(config.backoff_multiplier, 2.0);
        assert!(config.jitter_enabled);
        assert_eq!(config.jitter_factor, 0.15);
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
    fn test_retry_config_with_jitter_factor() {
        let config = RetryConfig::new().with_jitter_factor(0.25);

        assert_eq!(config.jitter_factor, 0.25);
    }

    #[test]
    fn test_retry_config_jitter_factor_clamping() {
        // Too high should be clamped to 1.0
        let config = RetryConfig::new().with_jitter_factor(1.5);

        assert_eq!(config.jitter_factor, 1.0);

        // Negative should be clamped to 0.0
        let config = RetryConfig::new().with_jitter_factor(-0.5);

        assert_eq!(config.jitter_factor, 0.0);
    }

    #[test]
    fn test_backoff_duration_exponential() {
        let config = RetryConfig::new().with_jitter(false);

        let d0 = config.backoff_duration(0);
        let d1 = config.backoff_duration(1);
        let d2 = config.backoff_duration(2);

        assert_eq!(d0, Duration::from_millis(100));
        assert_eq!(d1, Duration::from_millis(200));
        assert_eq!(d2, Duration::from_millis(400));
    }

    #[test]
    fn test_backoff_duration_max_clamp() {
        let config = RetryConfig::new()
            .with_max_backoff_ms(250)
            .with_jitter(false);

        let d10 = config.backoff_duration(10);
        // Even at attempt 10, should be clamped to max
        assert_eq!(d10, Duration::from_millis(250));
    }

    #[test]
    fn test_backoff_duration_with_jitter() {
        let config = RetryConfig::new().with_jitter(true).with_jitter_factor(0.5);

        let d0 = config.backoff_duration(0);
        // With 50% jitter, should be between 50ms and 150ms
        assert!(d0 >= Duration::from_millis(50));
        assert!(d0 <= Duration::from_millis(150));
    }

    #[test]
    fn test_retry_with_backoff_success_on_first_try() {
        let config = RetryConfig::new().with_max_retries(3);
        let mut attempts = 0;

        let result = retry_with_backoff(&config, "test", || {
            attempts += 1;
            Ok::<_, StorageError>(42)
        });

        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts, 1);
    }

    #[test]
    fn test_retry_with_backoff_success_after_retry() {
        let config = RetryConfig::new().with_max_retries(3);
        let mut attempts = 0;

        let result = retry_with_backoff(&config, "test", || {
            attempts += 1;
            if attempts < 3 {
                Err(StorageError::network("temporary failure"))
            } else {
                Ok::<_, StorageError>(42)
            }
        });

        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts, 3);
    }

    #[test]
    fn test_retry_with_backoff_non_retryable_error_fails_immediately() {
        let config = RetryConfig::new().with_max_retries(3);
        let mut attempts = 0;

        let result = retry_with_backoff(&config, "test", || {
            attempts += 1;
            Err::<u32, _>(StorageError::not_found("missing"))
        });

        assert!(result.is_err());
        assert_eq!(attempts, 1); // Should fail immediately
    }

    #[test]
    fn test_retry_with_backoff_exhausted_retries() {
        let config = RetryConfig::new().with_max_retries(2);
        let mut attempts = 0;

        let result: Result<u32> = retry_with_backoff(&config, "test", || {
            attempts += 1;
            Err(StorageError::network("persistent failure"))
        });

        assert!(result.is_err());
        assert_eq!(attempts, 3); // Initial + 2 retries
    }
}
