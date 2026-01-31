// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Distributed lock manager with TTL support.
//!
//! Provides a high-level abstraction for distributed locking using TiKV.
//! Features include:
//!
//! - **RAII-style lock guards**: Automatic release on drop
//! - **Blocking acquisition**: Retry with exponential backoff
//! - **Optional auto-renewal**: Background thread extends TTL
//! - **Fencing tokens**: Version-based split-brain prevention via `LockGuard::fencing_token()`
//!
//! # Usage
//!
//! ```rust,ignore
//! use roboflow_distributed::tikv::{LockManager, TikvClient};
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = TikvClient::from_env().await?;
//!     let manager = LockManager::new(client, "my-pod-id");
//!
//!     // Try to acquire a lock
//!     if let Some(guard) = manager.try_acquire("my-resource", Duration::from_secs(60)).await? {
//!         // Lock acquired, do work
//!         // Lock automatically released when guard drops
//!     } else {
//!         // Lock held by someone else
//!     }
//!
//!     // Blocking acquire with timeout
//!     let guard = manager.acquire_blocking("my-resource", Duration::from_secs(60), Duration::from_secs(10)).await?;
//!
//!     Ok(())
//! }
//! ```

use std::sync::Arc;
use std::time::Duration;

use super::client::TikvClient;
use super::error::{Result, TikvError};
use super::schema::LockRecord;
use tokio::sync::Mutex;
use tokio::time::{interval, sleep};

/// Default lock TTL in seconds.
pub const DEFAULT_LOCK_TTL_SECS: u64 = 300; // 5 minutes

/// Default renewal interval in seconds.
pub const DEFAULT_RENEWAL_INTERVAL_SECS: u64 = 120; // 2 minutes

/// Default backoff base delay in milliseconds.
pub const DEFAULT_BACKOFF_BASE_DELAY_MS: u64 = 50;

/// Maximum retry attempts for blocking acquire.
pub const DEFAULT_MAX_ACQUIRE_ATTEMPTS: u32 = 20;

/// Configuration for the lock manager.
#[derive(Debug, Clone)]
pub struct LockManagerConfig {
    /// Default TTL for locks.
    pub default_ttl: Duration,

    /// Interval for auto-renewal.
    pub renewal_interval: Duration,

    /// Base delay for exponential backoff.
    pub backoff_base_delay: Duration,

    /// Maximum attempts for blocking acquire.
    pub max_acquire_attempts: u32,
}

impl Default for LockManagerConfig {
    fn default() -> Self {
        Self {
            default_ttl: Duration::from_secs(DEFAULT_LOCK_TTL_SECS),
            renewal_interval: Duration::from_secs(DEFAULT_RENEWAL_INTERVAL_SECS),
            backoff_base_delay: Duration::from_millis(DEFAULT_BACKOFF_BASE_DELAY_MS),
            max_acquire_attempts: DEFAULT_MAX_ACQUIRE_ATTEMPTS,
        }
    }
}

impl LockManagerConfig {
    /// Set a custom default TTL.
    pub fn with_default_ttl(mut self, ttl: Duration) -> Self {
        self.default_ttl = ttl;
        self
    }

    /// Set a custom renewal interval.
    pub fn with_renewal_interval(mut self, interval: Duration) -> Self {
        self.renewal_interval = interval;
        self
    }

    /// Set a custom backoff base delay.
    pub fn with_backoff_delay(mut self, delay: Duration) -> Self {
        self.backoff_base_delay = delay;
        self
    }

    /// Set maximum acquire attempts.
    pub fn with_max_acquire_attempts(mut self, attempts: u32) -> Self {
        self.max_acquire_attempts = attempts;
        self
    }
}

/// Distributed lock manager with TTL support.
///
/// Provides high-level locking operations on top of TiKV, including
/// RAII-style lock guards and optional auto-renewal.
#[derive(Clone)]
pub struct LockManager {
    /// TiKV client for distributed operations.
    client: Arc<TikvClient>,

    /// Owner identifier (e.g., pod ID).
    owner: String,

    /// Lock manager configuration.
    config: LockManagerConfig,
}

impl LockManager {
    /// Create a new lock manager.
    pub fn new(client: Arc<TikvClient>, owner: impl Into<String>) -> Self {
        Self {
            client,
            owner: owner.into(),
            config: LockManagerConfig::default(),
        }
    }

    /// Create a new lock manager with custom configuration.
    pub fn with_config(
        client: Arc<TikvClient>,
        owner: impl Into<String>,
        config: LockManagerConfig,
    ) -> Self {
        Self {
            client,
            owner: owner.into(),
            config,
        }
    }

    /// Get the owner identifier.
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// Get a reference to the configuration.
    pub fn config(&self) -> &LockManagerConfig {
        &self.config
    }

    /// Try to acquire a lock without blocking.
    ///
    /// Returns `Ok(Some(guard))` if the lock was acquired,
    /// `Ok(None)` if the lock is held by someone else, or
    /// `Err` if there was a connection error.
    ///
    /// # Arguments
    ///
    /// * `resource` - The resource key to lock
    /// * `ttl` - Time-to-live for the lock
    pub async fn try_acquire(&self, resource: &str, ttl: Duration) -> Result<Option<LockGuard>> {
        let ttl_secs = ttl.as_secs().try_into().unwrap_or(i64::MAX);
        let acquired = self
            .client
            .acquire_lock(resource, &self.owner, ttl_secs)
            .await?;

        if acquired {
            let guard = LockGuard::new(
                self.client.clone(),
                resource.to_string(),
                self.owner.clone(),
                ttl_secs,
                self.config.renewal_interval,
                false, // No auto-renewal for try_acquire
            )
            .await;
            Ok(Some(guard))
        } else {
            Ok(None)
        }
    }

    /// Try to acquire a lock with default TTL.
    pub async fn try_acquire_default(&self, resource: &str) -> Result<Option<LockGuard>> {
        self.try_acquire(resource, self.config.default_ttl).await
    }

    /// Acquire a lock, blocking with exponential backoff until acquired or timeout.
    ///
    /// # Arguments
    ///
    /// * `resource` - The resource key to lock
    /// * `ttl` - Time-to-live for the lock
    /// * `timeout` - Maximum time to wait for lock acquisition
    ///
    /// # Errors
    ///
    /// Returns `Err` if:
    /// - Timeout is reached before acquiring the lock
    /// - A connection error occurs
    pub async fn acquire_blocking(
        &self,
        resource: &str,
        ttl: Duration,
        timeout: Duration,
    ) -> Result<LockGuard> {
        let ttl_secs = ttl.as_secs().try_into().unwrap_or(i64::MAX);
        let started = tokio::time::Instant::now();
        let mut attempt = 0u32;

        loop {
            // Check timeout first
            if started.elapsed() >= timeout {
                return Err(TikvError::LockAcquisitionFailed(format!(
                    "Timeout after {timeout:?} waiting for lock '{resource}'"
                )));
            }

            // Try to acquire
            match self
                .client
                .acquire_lock(resource, &self.owner, ttl_secs)
                .await?
            {
                true => {
                    let guard = LockGuard::new(
                        self.client.clone(),
                        resource.to_string(),
                        self.owner.clone(),
                        ttl_secs,
                        self.config.renewal_interval,
                        false, // No auto-renewal for blocking acquire by default
                    )
                    .await;
                    tracing::info!(
                        resource = %resource,
                        attempts = attempt + 1,
                        elapsed = ?started.elapsed(),
                        "Lock acquired after blocking"
                    );
                    return Ok(guard);
                }
                false => {
                    attempt += 1;
                    if attempt >= self.config.max_acquire_attempts {
                        return Err(TikvError::LockAcquisitionFailed(format!(
                            "Max attempts ({}) reached waiting for lock '{resource}'",
                            self.config.max_acquire_attempts
                        )));
                    }

                    // Exponential backoff with jitter
                    let delay =
                        self.config.backoff_base_delay * 2_u32.pow(attempt.saturating_sub(1));
                    let delay = std::cmp::min(delay, Duration::from_secs(5)); // Cap at 5 seconds
                    // Jitter: up to 25% of delay, minimum 1ms to avoid panic on small delays
                    let jitter_ms = u64::try_from(delay.as_millis() / 4).unwrap_or(0).max(1);
                    let jitter = Duration::from_millis(fastrand::u64(0..jitter_ms));
                    tracing::debug!(
                        resource = %resource,
                        attempt = attempt,
                        delay = ?delay,
                        "Lock busy, retrying with backoff"
                    );
                    sleep(delay + jitter).await;
                }
            }
        }
    }

    /// Acquire a lock with default TTL and timeout.
    pub async fn acquire_blocking_default(&self, resource: &str) -> Result<LockGuard> {
        self.acquire_blocking(resource, self.config.default_ttl, Duration::from_secs(30))
            .await
    }

    /// Acquire a lock with auto-renewal.
    ///
    /// The lock will be automatically renewed in the background at the configured interval.
    /// Renewal stops when the guard is dropped.
    ///
    /// # Arguments
    ///
    /// * `resource` - The resource key to lock
    /// * `ttl` - Time-to-live for the lock (also used for renewal)
    pub async fn acquire_with_renewal(&self, resource: &str, ttl: Duration) -> Result<LockGuard> {
        let ttl_secs = ttl.as_secs().try_into().unwrap_or(i64::MAX);
        let acquired = self
            .client
            .acquire_lock(resource, &self.owner, ttl_secs)
            .await?;

        if !acquired {
            return Err(TikvError::LockAcquisitionFailed(format!(
                "Failed to acquire lock '{resource}' for renewal"
            )));
        }

        let guard = LockGuard::new(
            self.client.clone(),
            resource.to_string(),
            self.owner.clone(),
            ttl_secs,
            self.config.renewal_interval,
            true, // Enable auto-renewal
        )
        .await;
        Ok(guard)
    }

    /// Acquire a lock with auto-renewal using default TTL.
    pub async fn acquire_with_renewal_default(&self, resource: &str) -> Result<LockGuard> {
        self.acquire_with_renewal(resource, self.config.default_ttl)
            .await
    }

    /// Check if a lock is currently held (regardless of owner).
    pub async fn is_locked(&self, resource: &str) -> Result<bool> {
        let key = super::key::LockKeys::lock(resource);
        let data = self.client.get(key).await?;

        if let Some(bytes) = data {
            let lock: LockRecord = bincode::deserialize(&bytes)
                .map_err(|e| TikvError::Deserialization(e.to_string()))?;
            Ok(!lock.is_expired())
        } else {
            Ok(false)
        }
    }

    /// Get the owner of a lock, if it exists and is not expired.
    pub async fn get_owner(&self, resource: &str) -> Result<Option<String>> {
        let key = super::key::LockKeys::lock(resource);
        let data = self.client.get(key).await?;

        if let Some(bytes) = data {
            let lock: LockRecord = bincode::deserialize(&bytes)
                .map_err(|e| TikvError::Deserialization(e.to_string()))?;
            if lock.is_expired() {
                Ok(None)
            } else {
                Ok(Some(lock.owner))
            }
        } else {
            Ok(None)
        }
    }

    /// Check if a lock is expired (exists but TTL has passed).
    pub async fn is_expired(&self, resource: &str) -> Result<bool> {
        let key = super::key::LockKeys::lock(resource);
        let data = self.client.get(key).await?;

        if let Some(bytes) = data {
            let lock: LockRecord = bincode::deserialize(&bytes)
                .map_err(|e| TikvError::Deserialization(e.to_string()))?;
            Ok(lock.is_expired())
        } else {
            Ok(false) // No lock means not "expired" (never existed)
        }
    }

    /// Release a lock explicitly (without a guard).
    ///
    /// This is useful for releasing locks acquired elsewhere or for
    /// emergency cleanup. Returns `Ok(false)` if the lock doesn't exist
    /// or is owned by someone else.
    pub async fn release(&self, resource: &str) -> Result<bool> {
        self.client.release_lock(resource, &self.owner).await
    }

    /// Extend the TTL of a lock we own.
    ///
    /// Returns `Ok(true)` if extended, `Ok(false)` if we don't own the lock.
    pub async fn renew(&self, resource: &str, ttl: Duration) -> Result<bool> {
        let ttl_secs = ttl.as_secs().try_into().unwrap_or(i64::MAX);
        let acquired = self
            .client
            .acquire_lock(resource, &self.owner, ttl_secs)
            .await?;
        Ok(acquired)
    }

    /// Steal an expired lock.
    ///
    /// This will only succeed if the lock is expired. Returns `Ok(true)` if stolen.
    pub async fn steal(&self, resource: &str, ttl: Duration) -> Result<bool> {
        let key = super::key::LockKeys::lock(resource);
        let data = self.client.get(key).await?;

        let can_steal = if let Some(bytes) = data {
            let lock: LockRecord = bincode::deserialize(&bytes)
                .map_err(|e| TikvError::Deserialization(e.to_string()))?;
            lock.is_expired()
        } else {
            true // No lock exists, can acquire
        };

        if can_steal {
            let ttl_secs = ttl.as_secs().try_into().unwrap_or(i64::MAX);
            self.client
                .acquire_lock(resource, &self.owner, ttl_secs)
                .await
        } else {
            Ok(false)
        }
    }
}

/// RAII-style lock guard.
///
/// Automatically releases the lock when dropped.
/// Supports optional auto-renewal via background task.
///
/// # Drop Behavior
///
/// When the guard is dropped, it attempts to release the lock asynchronously:
/// - If a tokio runtime is available, a cleanup task is spawned
/// - If no runtime is available, the lock will leak until TTL expires
///
/// For critical cleanup, call `release()` explicitly before dropping.
pub struct LockGuard {
    /// TiKV client for releasing the lock.
    client: Arc<TikvClient>,

    /// Resource being locked.
    resource: String,

    /// Owner of the lock.
    owner: String,

    /// Lock TTL in seconds (for renewal).
    ttl_secs: i64,

    /// Whether this guard has been released (atomic for lock-free access in Drop).
    released: Arc<std::sync::atomic::AtomicBool>,

    /// Handle for the renewal task.
    renewal_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,

    /// Renewal interval for auto-renewal (moved into renewal task).
    #[allow(dead_code)]
    renewal_interval: Duration,
}

impl LockGuard {
    /// Create a new lock guard.
    async fn new(
        client: Arc<TikvClient>,
        resource: String,
        owner: String,
        ttl_secs: i64,
        renewal_interval: Duration,
        auto_renew: bool,
    ) -> Self {
        let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let renewal_handle = Arc::new(Mutex::new(None));

        if auto_renew {
            let client_clone = client.clone();
            let resource_clone = resource.clone();
            let owner_clone = owner.clone();
            let released_clone = released.clone();

            let handle = tokio::spawn(async move {
                let mut ticker = interval(renewal_interval);
                ticker.tick().await; // Skip first tick

                loop {
                    ticker.tick().await;

                    // Check if already released using atomic load
                    if released_clone.load(std::sync::atomic::Ordering::Acquire) {
                        break;
                    }

                    // Attempt to renew
                    let renewed = client_clone
                        .acquire_lock(&resource_clone, &owner_clone, ttl_secs)
                        .await;

                    match renewed {
                        Ok(true) => {
                            tracing::debug!(
                                resource = %resource_clone,
                                "Lock auto-renewed"
                            );
                        }
                        Ok(false) => {
                            tracing::warn!(
                                resource = %resource_clone,
                                "Lock renewal failed - no longer owner"
                            );
                            break;
                        }
                        Err(e) => {
                            tracing::error!(
                                resource = %resource_clone,
                                error = %e,
                                "Lock renewal error"
                            );
                            // Continue retrying on transient errors
                        }
                    }
                }
            });

            *renewal_handle.lock().await = Some(handle);
        }

        Self {
            client,
            resource,
            owner,
            ttl_secs,
            released,
            renewal_handle,
            renewal_interval,
        }
    }

    /// Get the resource being locked.
    pub fn resource(&self) -> &str {
        &self.resource
    }

    /// Get the owner of the lock.
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// Check if the lock is still valid (not released).
    pub fn is_valid(&self) -> bool {
        !self.released.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Explicitly release the lock before the guard drops.
    ///
    /// Returns `Ok(true)` if released, `Ok(false)` if already released
    /// or ownership lost.
    pub async fn release(self) -> Result<bool> {
        // Mark as released FIRST to prevent drop from doing cleanup
        self.released
            .store(true, std::sync::atomic::Ordering::Release);
        self.do_release().await
    }

    /// Extend the lock TTL.
    pub async fn renew(&self) -> Result<bool> {
        let renewed = self
            .client
            .acquire_lock(&self.resource, &self.owner, self.ttl_secs)
            .await?;
        Ok(renewed)
    }

    /// Get the current fencing token for this lock.
    ///
    /// Fencing tokens are monotonically increasing version numbers that
    /// help detect split-brain scenarios where multiple processes believe
    /// they own the same lock. Higher tokens always win.
    ///
    /// Returns `None` if the lock no longer exists or has been released.
    pub async fn fencing_token(&self) -> Result<Option<u64>> {
        let key = super::key::LockKeys::lock(&self.resource);
        let data = self.client.get(key).await?;

        if let Some(bytes) = data {
            let lock: LockRecord = bincode::deserialize(&bytes)
                .map_err(|e| TikvError::Deserialization(e.to_string()))?;
            // Only return the token if we still own the lock
            if lock.is_owned_by(&self.owner) && !lock.is_expired() {
                Ok(Some(lock.fencing_token()))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Internal release implementation.
    async fn do_release(&self) -> Result<bool> {
        // Stop renewal task first
        let mut handle_guard = self.renewal_handle.lock().await;
        if let Some(handle) = handle_guard.take() {
            handle.abort();
        }
        drop(handle_guard);

        // Release the lock
        let released = self
            .client
            .release_lock(&self.resource, &self.owner)
            .await?;

        if released {
            tracing::debug!(
                resource = %self.resource,
                owner = %self.owner,
                "Lock guard released"
            );
        }

        Ok(released)
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        // Mark as released atomically to stop renewal task
        let already_released = self
            .released
            .swap(true, std::sync::atomic::Ordering::AcqRel);

        if already_released {
            // Already explicitly released, nothing to do
            return;
        }

        // Abort renewal task if possible (best-effort without blocking)
        if let Ok(mut handle_guard) = self.renewal_handle.try_lock()
            && let Some(renewal_task) = handle_guard.take()
        {
            renewal_task.abort();
        }

        let resource = self.resource.clone();
        let owner = self.owner.clone();

        // Use tokio spawn if runtime is available for async cleanup
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let client = self.client.clone();

            // Spawn cleanup task - note: this is fire-and-forget and may
            // not complete if the runtime is shutting down.
            handle.spawn(async move {
                if let Err(e) = client.release_lock(&resource, &owner).await {
                    tracing::warn!(
                        resource = %resource,
                        owner = %owner,
                        error = %e,
                        "Failed to release lock on drop"
                    );
                }
            });
        } else {
            // No runtime available - lock will leak until TTL expires
            tracing::warn!(
                resource = %resource,
                owner = %owner,
                "Lock guard dropped without tokio runtime - lock will leak until TTL expires"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = LockManagerConfig::default();
        assert_eq!(
            config.default_ttl,
            Duration::from_secs(DEFAULT_LOCK_TTL_SECS)
        );
        assert_eq!(
            config.renewal_interval,
            Duration::from_secs(DEFAULT_RENEWAL_INTERVAL_SECS)
        );
    }

    #[test]
    fn test_config_builders() {
        let config = LockManagerConfig::default()
            .with_default_ttl(Duration::from_secs(120))
            .with_renewal_interval(Duration::from_secs(60))
            .with_backoff_delay(Duration::from_millis(100))
            .with_max_acquire_attempts(50);

        assert_eq!(config.default_ttl, Duration::from_secs(120));
        assert_eq!(config.renewal_interval, Duration::from_secs(60));
        assert_eq!(config.backoff_base_delay, Duration::from_millis(100));
        assert_eq!(config.max_acquire_attempts, 50);
    }

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_LOCK_TTL_SECS, 300);
        assert_eq!(DEFAULT_RENEWAL_INTERVAL_SECS, 120);
        assert_eq!(DEFAULT_BACKOFF_BASE_DELAY_MS, 50);
        assert_eq!(DEFAULT_MAX_ACQUIRE_ATTEMPTS, 20);
    }
}
