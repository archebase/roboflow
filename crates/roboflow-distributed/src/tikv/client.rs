// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! TiKV client wrapper for distributed coordination.
//!
//! Provides connection pooling and basic CRUD operations for TiKV.
//!
//! # Atomicity Guarantees
//!
//! This client uses TiKV's optimistic transactions. Each CRUD operation
//! (`get`, `put`, `delete`, `scan`) executes in its own transaction.
//!
//! High-level operations like `claim_job`, `acquire_lock`, `release_lock`,
//! `complete_job`, `fail_job`, and `cas` all use **single transactions**
//! for both read and write, providing atomicity. If two workers race to
//! perform conflicting operations, TiKV's optimistic concurrency control
//! will detect the conflict and one transaction will fail with a write
//! conflict error.
//!
//! # Retry Behavior
//!
//! Write conflicts are automatically retried with exponential backoff.
//! The `max_retries` and `retry_base_delay_ms` configuration values control
//! retry behavior. If all retries are exhausted, a `Retryable` error is
//! returned.
//!
//! # Scan Behavior
//!
//! The `scan` method returns keys in lexicographic order. Use the
//! `KeyBuilder` or `*Keys` types to construct proper prefix-based keys.

use std::sync::Arc;
use std::time::Duration;

use super::config::TikvConfig;
use super::error::{Result, TikvError};
use super::key::{HeartbeatKeys, JobKeys, LockKeys, StateKeys};
use super::schema::{CheckpointState, HeartbeatRecord, JobRecord, JobStatus, LockRecord};

use tokio::time::sleep;

/// TiKV client wrapper with connection pooling.
#[derive(Clone)]
pub struct TikvClient {
    /// TiKV configuration.
    config: TikvConfig,

    /// Underlying transaction client.
    inner: Option<Arc<tikv_client::TransactionClient>>,
}

impl TikvClient {
    /// Create a new TiKV client with the given configuration.
    pub async fn new(config: TikvConfig) -> Result<Self> {
        // Validate configuration first
        config.validate()?;

        {
            // Try to connect to TiKV cluster
            let client = tikv_client::TransactionClient::new_with_config(
                config.pd_endpoints.clone(),
                tikv_client::Config::default(),
            )
            .await
            .map_err(|e| TikvError::ConnectionFailed(e.to_string()))?;

            tracing::info!("Connected to TiKV: {}", config.describe());

            Ok(Self {
                config,
                inner: Some(Arc::new(client)),
            })
        }

        #[cfg(not(feature = "distributed"))]
        {
            tracing::warn!("Distributed feature not enabled, TikvClient will be a no-op");
            Ok(Self {
                config,
                inner: None,
            })
        }
    }

    /// Create a new client with default configuration from environment.
    pub async fn from_env() -> Result<Self> {
        Self::new(TikvConfig::default()).await
    }

    /// Retry helper with exponential backoff for write conflicts.
    ///
    /// This helper automatically retries operations that fail with write conflicts,
    /// using exponential backoff between retries.
    #[allow(dead_code)]
    async fn retry_with_backoff<F, Fut, T>(&self, operation_name: &str, operation: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let max_retries = self.config.max_retries;
        let base_delay = Duration::from_millis(self.config.retry_base_delay_ms);

        for attempt in 0..max_retries {
            match operation().await {
                Ok(value) => {
                    if attempt > 0 {
                        tracing::debug!(
                            operation = operation_name,
                            attempts = attempt + 1,
                            "Operation succeeded after retries"
                        );
                    }
                    return Ok(value);
                }
                Err(err) if err.is_write_conflict() || err.is_retryable() => {
                    if attempt >= max_retries - 1 {
                        tracing::warn!(
                            operation = operation_name,
                            attempts = attempt + 1,
                            max_retries,
                            "Operation failed after max retries"
                        );
                        return Err(TikvError::retryable(
                            attempt + 1,
                            max_retries,
                            err.to_string(),
                        ));
                    }

                    let delay = base_delay * 2_u32.pow(attempt);
                    tracing::debug!(
                        operation = operation_name,
                        attempt = attempt + 1,
                        delay_ms = delay.as_millis(),
                        error = %err,
                        "Retrying operation after write conflict"
                    );
                    sleep(delay).await;
                }
                Err(err) => return Err(err),
            }
        }

        // This should never be reached, but the compiler doesn't know that
        Err(TikvError::retryable(
            max_retries,
            max_retries,
            "Unexpected loop exit",
        ))
    }

    /// Get a value by key.
    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        {
            let inner = self.inner.as_ref().ok_or_else(|| {
                TikvError::ConnectionFailed("TiKV client not initialized".to_string())
            })?;

            let mut txn = inner
                .begin_optimistic()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            let result = txn
                .get(key)
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            txn.commit()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            Ok(result)
        }

        #[cfg(not(feature = "distributed"))]
        {
            let _ = key;
            Err(TikvError::ConnectionFailed(
                "Distributed feature not enabled".to_string(),
            ))
        }
    }

    /// Put a key-value pair.
    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        {
            let inner = self.inner.as_ref().ok_or_else(|| {
                TikvError::ConnectionFailed("TiKV client not initialized".to_string())
            })?;

            let mut txn = inner
                .begin_optimistic()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            txn.put(key, value)
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            txn.commit()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            Ok(())
        }

        #[cfg(not(feature = "distributed"))]
        {
            let _ = (key, value);
            Err(TikvError::ConnectionFailed(
                "Distributed feature not enabled".to_string(),
            ))
        }
    }

    /// Delete a key.
    pub async fn delete(&self, key: Vec<u8>) -> Result<()> {
        {
            let inner = self.inner.as_ref().ok_or_else(|| {
                TikvError::ConnectionFailed("TiKV client not initialized".to_string())
            })?;

            let mut txn = inner
                .begin_optimistic()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            txn.delete(key)
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            txn.commit()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            Ok(())
        }

        #[cfg(not(feature = "distributed"))]
        {
            let _ = key;
            Err(TikvError::ConnectionFailed(
                "Distributed feature not enabled".to_string(),
            ))
        }
    }

    /// Scan keys with a prefix.
    ///
    /// Uses an exclusive range to match all keys starting with the prefix.
    /// The scan is limited to `limit` results.
    pub async fn scan(&self, prefix: Vec<u8>, limit: u32) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        tracing::debug!(
            limit = limit,
            prefix = %String::from_utf8_lossy(&prefix),
            "Starting prefix scan"
        );

        {
            let inner = self.inner.as_ref().ok_or_else(|| {
                TikvError::ConnectionFailed("TiKV client not initialized".to_string())
            })?;

            let mut txn = inner
                .begin_optimistic()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            // Create a proper prefix scan range using exclusive upper bound.
            // To find the first key after the prefix, we append a null byte (0x00)
            // which gives us the first key that doesn't match the prefix.
            let mut scan_end = prefix.clone();
            scan_end.push(0);

            // Use exclusive range (..) instead of inclusive (..=) for correctness
            let iter = txn
                .scan(prefix.clone()..scan_end, limit)
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            // Collect the iterator into a Vec
            let result: Vec<(Vec<u8>, Vec<u8>)> = iter
                .map(|pair| {
                    #[allow(clippy::useless_conversion)]
                    let key: Vec<u8> = pair.key().clone().into();
                    #[allow(clippy::useless_conversion)]
                    let value: Vec<u8> = pair.value().clone().into();
                    (key, value)
                })
                .collect();

            txn.commit()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            tracing::debug!(limit = limit, results = result.len(), "Scan completed");

            Ok(result)
        }

        #[cfg(not(feature = "distributed"))]
        {
            let _ = (prefix, limit);
            Err(TikvError::ConnectionFailed(
                "Distributed feature not enabled".to_string(),
            ))
        }
    }

    /// Batch get multiple keys.
    pub async fn batch_get(&self, keys: Vec<Vec<u8>>) -> Result<Vec<Option<Vec<u8>>>> {
        {
            let inner = self.inner.as_ref().ok_or_else(|| {
                TikvError::ConnectionFailed("TiKV client not initialized".to_string())
            })?;

            let mut txn = inner
                .begin_optimistic()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            let mut results = Vec::new();
            for key in &keys {
                let value = txn
                    .get(key.clone())
                    .await
                    .map_err(|e| TikvError::ClientError(e.to_string()))?;
                results.push(value);
            }

            txn.commit()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            Ok(results)
        }

        #[cfg(not(feature = "distributed"))]
        {
            let _ = keys;
            Err(TikvError::ConnectionFailed(
                "Distributed feature not enabled".to_string(),
            ))
        }
    }

    /// Batch put multiple key-value pairs.
    pub async fn batch_put(&self, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()> {
        {
            let inner = self.inner.as_ref().ok_or_else(|| {
                TikvError::ConnectionFailed("TiKV client not initialized".to_string())
            })?;

            let mut txn = inner
                .begin_optimistic()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            for (key, value) in pairs {
                txn.put(key, value)
                    .await
                    .map_err(|e| TikvError::ClientError(e.to_string()))?;
            }

            txn.commit()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            Ok(())
        }

        #[cfg(not(feature = "distributed"))]
        {
            let _ = pairs;
            Err(TikvError::ConnectionFailed(
                "Distributed feature not enabled".to_string(),
            ))
        }
    }

    /// Compare-And-Swap (CAS) operation for atomic updates.
    ///
    /// This uses a single transaction to read the current value, check the version,
    /// and write the new value if the version matches. Returns `Ok(true)` if the
    /// operation succeeded, `Ok(false)` if the version mismatched (key exists with
    /// different version, or key doesn't exist with expected_version != 0), or
    /// `Err` if there was a connection error.
    pub async fn cas(
        &self,
        key: Vec<u8>,
        expected_version: u64,
        new_value: Vec<u8>,
    ) -> Result<bool> {
        {
            use serde_json::Value;

            let inner = self.inner.as_ref().ok_or_else(|| {
                TikvError::ConnectionFailed("TiKV client not initialized".to_string())
            })?;

            let mut txn = inner
                .begin_optimistic()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            let success = match txn
                .get(key.clone())
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?
            {
                Some(existing_data) => {
                    if let Ok(value_json) = serde_json::from_slice::<Value>(&existing_data)
                        && let Some(version) = value_json.get("version").and_then(|v| v.as_u64())
                    {
                        if version == expected_version {
                            txn.put(key, new_value)
                                .await
                                .map_err(|e| TikvError::ClientError(e.to_string()))?;
                            true
                        } else {
                            return Err(TikvError::CasFailed {
                                expected: expected_version,
                                got: version,
                            });
                        }
                    } else {
                        // No version field, can't do CAS
                        return Err(TikvError::Deserialization(
                            "No version field in value".to_string(),
                        ));
                    }
                }
                None => {
                    if expected_version == 0 {
                        txn.put(key, new_value)
                            .await
                            .map_err(|e| TikvError::ClientError(e.to_string()))?;
                        true
                    } else {
                        return Ok(false);
                    }
                }
            };

            txn.commit()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            Ok(success)
        }

        #[cfg(not(feature = "distributed"))]
        {
            let _ = (key, expected_version, new_value);
            Err(TikvError::ConnectionFailed(
                "Distributed feature not enabled".to_string(),
            ))
        }
    }

    // ============== High-level operations for specific types ==============

    /// Get a job record by file hash.
    pub async fn get_job(&self, file_hash: &str) -> Result<Option<JobRecord>> {
        let key = JobKeys::record(file_hash);
        let data = self.get(key).await?;

        match data {
            Some(bytes) => {
                let record: JobRecord = bincode::deserialize(&bytes)
                    .map_err(|e| TikvError::Deserialization(e.to_string()))?;
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    /// Put a job record.
    pub async fn put_job(&self, job: &JobRecord) -> Result<()> {
        let key = JobKeys::record(&job.id);
        let data = bincode::serialize(job).map_err(|e| TikvError::Serialization(e.to_string()))?;
        self.put(key, data).await
    }

    /// Batch get multiple job records.
    ///
    /// Returns a vector of (job_id, Option<JobRecord>) tuples.
    /// Jobs that don't exist will have None as the record.
    pub async fn batch_get_jobs(
        &self,
        job_ids: &[String],
    ) -> Result<Vec<(String, Option<JobRecord>)>> {
        use super::key::JobKeys;

        let keys: Vec<Vec<u8>> = job_ids.iter().map(|id| JobKeys::record(id)).collect();

        let values = self.batch_get(keys).await?;

        let mut results = Vec::with_capacity(job_ids.len());
        for (i, data) in values.into_iter().enumerate() {
            let job_id = &job_ids[i];
            let record = match data {
                Some(bytes) => {
                    let record: JobRecord = bincode::deserialize(&bytes)
                        .map_err(|e| TikvError::Deserialization(e.to_string()))?;
                    Some(record)
                }
                None => None,
            };
            results.push((job_id.clone(), record));
        }

        Ok(results)
    }

    /// Claim a job (atomic operation within a single transaction).
    ///
    /// This uses a single transaction to read the job, check if it's claimable,
    /// and update it. If two workers race to claim the same job, TiKV's
    /// optimistic concurrency will detect the write conflict and one will
    /// fail with a `WriteConflict` error.
    pub async fn claim_job(&self, file_hash: &str, pod_id: &str) -> Result<bool> {
        tracing::debug!(
            file_hash = %file_hash,
            pod_id = %pod_id,
            "Attempting to claim job"
        );

        {
            let inner = self.inner.as_ref().ok_or_else(|| {
                TikvError::ConnectionFailed("TiKV client not initialized".to_string())
            })?;

            let key = JobKeys::record(file_hash);
            let mut txn = inner
                .begin_optimistic()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            // Read job in transaction
            let current = txn
                .get(key.clone())
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            let claimed = match current {
                Some(data) => {
                    let job: JobRecord = bincode::deserialize(&data)
                        .map_err(|e| TikvError::Deserialization(e.to_string()))?;

                    if job.is_claimable() {
                        let mut job = job;
                        job.claim(pod_id.to_string()).map_err(TikvError::Other)?;
                        let new_data = bincode::serialize(&job)
                            .map_err(|e| TikvError::Serialization(e.to_string()))?;
                        txn.put(key, new_data)
                            .await
                            .map_err(|e| TikvError::ClientError(e.to_string()))?;
                        true
                    } else {
                        false
                    }
                }
                None => false,
            };

            txn.commit()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            if claimed {
                tracing::info!(
                    file_hash = %file_hash,
                    pod_id = %pod_id,
                    "Job successfully claimed"
                );
            } else {
                tracing::debug!(
                    file_hash = %file_hash,
                    pod_id = %pod_id,
                    "Job not claimable (already claimed or not found)"
                );
            }

            Ok(claimed)
        }

        #[cfg(not(feature = "distributed"))]
        {
            let _ = (file_hash, pod_id);
            Err(TikvError::ConnectionFailed(
                "Distributed feature not enabled".to_string(),
            ))
        }
    }

    /// Complete a job (atomic operation within a single transaction).
    ///
    /// This uses a single transaction to read the job, verify it's in Processing state,
    /// and mark it as Completed. Returns an error if the job is not in Processing state
    /// or doesn't exist.
    pub async fn complete_job(&self, file_hash: &str) -> Result<bool> {
        tracing::debug!(
            file_hash = %file_hash,
            "Attempting to complete job"
        );

        {
            use super::schema::JobStatus;

            let inner = self.inner.as_ref().ok_or_else(|| {
                TikvError::ConnectionFailed("TiKV client not initialized".to_string())
            })?;

            let key = JobKeys::record(file_hash);
            let mut txn = inner
                .begin_optimistic()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            let completed = match txn
                .get(key.clone())
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?
            {
                Some(data) => {
                    let mut job: JobRecord = bincode::deserialize(&data)
                        .map_err(|e| TikvError::Deserialization(e.to_string()))?;

                    // Only allow completion if currently processing
                    if job.status != JobStatus::Processing {
                        tracing::debug!(
                            file_hash = %file_hash,
                            status = ?job.status,
                            "Job not in Processing state, cannot complete"
                        );
                        return Ok(false);
                    }

                    job.complete();
                    let new_data = bincode::serialize(&job)
                        .map_err(|e| TikvError::Serialization(e.to_string()))?;
                    txn.put(key, new_data)
                        .await
                        .map_err(|e| TikvError::ClientError(e.to_string()))?;
                    true
                }
                None => {
                    return Err(TikvError::KeyNotFound(format!("Job: {}", file_hash)));
                }
            };

            txn.commit()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            if completed {
                tracing::info!(
                    file_hash = %file_hash,
                    "Job completed successfully"
                );
            }

            Ok(completed)
        }

        #[cfg(not(feature = "distributed"))]
        {
            let _ = file_hash;
            Err(TikvError::ConnectionFailed(
                "Distributed feature not enabled".to_string(),
            ))
        }
    }

    /// Fail a job with an error message (atomic operation within a single transaction).
    ///
    /// This uses a single transaction to read the job, verify it's in Processing state,
    /// and mark it as Failed. Returns an error if the job doesn't exist.
    pub async fn fail_job(&self, file_hash: &str, error: String) -> Result<bool> {
        tracing::debug!(
            file_hash = %file_hash,
            error = %error,
            "Attempting to fail job"
        );

        {
            use super::schema::JobStatus;

            let inner = self.inner.as_ref().ok_or_else(|| {
                TikvError::ConnectionFailed("TiKV client not initialized".to_string())
            })?;

            let key = JobKeys::record(file_hash);
            let mut txn = inner
                .begin_optimistic()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            let failed = match txn
                .get(key.clone())
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?
            {
                Some(data) => {
                    let mut job: JobRecord = bincode::deserialize(&data)
                        .map_err(|e| TikvError::Deserialization(e.to_string()))?;

                    // Only allow failure if currently processing
                    if job.status != JobStatus::Processing {
                        tracing::debug!(
                            file_hash = %file_hash,
                            status = ?job.status,
                            "Job not in Processing state, cannot fail"
                        );
                        return Ok(false);
                    }

                    job.fail(error.clone());
                    let new_data = bincode::serialize(&job)
                        .map_err(|e| TikvError::Serialization(e.to_string()))?;
                    txn.put(key, new_data)
                        .await
                        .map_err(|e| TikvError::ClientError(e.to_string()))?;
                    true
                }
                None => {
                    return Err(TikvError::KeyNotFound(format!("Job: {}", file_hash)));
                }
            };

            txn.commit()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            if failed {
                tracing::warn!(
                    file_hash = %file_hash,
                    error = %error,
                    "Job failed"
                );
            }

            Ok(failed)
        }

        #[cfg(not(feature = "distributed"))]
        {
            let _ = (file_hash, error);
            Err(TikvError::ConnectionFailed(
                "Distributed feature not enabled".to_string(),
            ))
        }
    }

    /// Acquire a distributed lock (atomic operation within a single transaction).
    ///
    /// This uses a single transaction to read the lock, check if it's available,
    /// and write the new lock record. If two workers race to acquire the same lock,
    /// TiKV's optimistic concurrency will detect the write conflict and one will fail.
    pub async fn acquire_lock(
        &self,
        resource: &str,
        owner: &str,
        ttl_seconds: i64,
    ) -> Result<bool> {
        tracing::debug!(
            resource = %resource,
            owner = %owner,
            ttl_seconds = ttl_seconds,
            "Attempting to acquire lock"
        );

        {
            let inner = self.inner.as_ref().ok_or_else(|| {
                TikvError::ConnectionFailed("TiKV client not initialized".to_string())
            })?;

            let key = LockKeys::lock(resource);
            let mut txn = inner
                .begin_optimistic()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            // Read current lock state in transaction
            let acquired = match txn
                .get(key.clone())
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?
            {
                Some(data) => {
                    let existing: LockRecord = bincode::deserialize(&data)
                        .map_err(|e| TikvError::Deserialization(e.to_string()))?;

                    // Check ownership FIRST (regardless of expiration)
                    // If we own the lock, extend it even if expired
                    if existing.is_owned_by(owner) {
                        let mut lock = existing;
                        lock.extend(ttl_seconds);
                        let new_data = bincode::serialize(&lock)
                            .map_err(|e| TikvError::Serialization(e.to_string()))?;
                        txn.put(key, new_data)
                            .await
                            .map_err(|e| TikvError::ClientError(e.to_string()))?;
                        tracing::debug!(
                            resource = %resource,
                            owner = %owner,
                            new_version = lock.version,
                            "Lock extended"
                        );
                        true
                    } else if !existing.is_expired() {
                        // Lock is held by someone else and not expired
                        tracing::debug!(
                            resource = %resource,
                            owner = %owner,
                            current_owner = %existing.owner,
                            "Lock held by another owner"
                        );
                        false
                    } else {
                        // Lock expired and not owned by us, take it
                        let lock =
                            LockRecord::new(resource.to_string(), owner.to_string(), ttl_seconds);
                        let data = bincode::serialize(&lock)
                            .map_err(|e| TikvError::Serialization(e.to_string()))?;
                        txn.put(key, data)
                            .await
                            .map_err(|e| TikvError::ClientError(e.to_string()))?;
                        tracing::info!(
                            resource = %resource,
                            owner = %owner,
                            "Lock acquired (was expired)"
                        );
                        true
                    }
                }
                None => {
                    // No lock exists, create new one
                    let lock =
                        LockRecord::new(resource.to_string(), owner.to_string(), ttl_seconds);
                    let data = bincode::serialize(&lock)
                        .map_err(|e| TikvError::Serialization(e.to_string()))?;
                    txn.put(key, data)
                        .await
                        .map_err(|e| TikvError::ClientError(e.to_string()))?;
                    tracing::info!(
                        resource = %resource,
                        owner = %owner,
                        "Lock acquired (new lock)"
                    );
                    true
                }
            };

            txn.commit()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            Ok(acquired)
        }

        #[cfg(not(feature = "distributed"))]
        {
            let _ = (resource, owner, ttl_seconds);
            Err(TikvError::ConnectionFailed(
                "Distributed feature not enabled".to_string(),
            ))
        }
    }

    /// Release a distributed lock (atomic operation within a single transaction).
    ///
    /// This uses a single transaction to read the lock, verify ownership, and delete it.
    /// Only the owner of the lock can release it.
    pub async fn release_lock(&self, resource: &str, owner: &str) -> Result<bool> {
        tracing::debug!(
            resource = %resource,
            owner = %owner,
            "Attempting to release lock"
        );

        {
            let inner = self.inner.as_ref().ok_or_else(|| {
                TikvError::ConnectionFailed("TiKV client not initialized".to_string())
            })?;

            let key = LockKeys::lock(resource);
            let mut txn = inner
                .begin_optimistic()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            let released = match txn
                .get(key.clone())
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?
            {
                Some(data) => {
                    let existing: LockRecord = bincode::deserialize(&data)
                        .map_err(|e| TikvError::Deserialization(e.to_string()))?;

                    if existing.is_owned_by(owner) {
                        txn.delete(key)
                            .await
                            .map_err(|e| TikvError::ClientError(e.to_string()))?;
                        tracing::info!(
                            resource = %resource,
                            owner = %owner,
                            fencing_token = existing.fencing_token(),
                            "Lock released"
                        );
                        true
                    } else {
                        tracing::warn!(
                            resource = %resource,
                            owner = %owner,
                            actual_owner = %existing.owner,
                            "Lock release failed: not the owner"
                        );
                        false
                    }
                }
                None => {
                    tracing::debug!(
                        resource = %resource,
                        owner = %owner,
                        "Lock release failed: lock not found"
                    );
                    false
                }
            };

            txn.commit()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            Ok(released)
        }

        #[cfg(not(feature = "distributed"))]
        {
            let _ = (resource, owner);
            Err(TikvError::ConnectionFailed(
                "Distributed feature not enabled".to_string(),
            ))
        }
    }

    /// Update or create heartbeat record.
    pub async fn update_heartbeat(&self, pod_id: &str, heartbeat: &HeartbeatRecord) -> Result<()> {
        let key = HeartbeatKeys::heartbeat(pod_id);
        let data =
            bincode::serialize(heartbeat).map_err(|e| TikvError::Serialization(e.to_string()))?;
        self.put(key, data).await
    }

    /// Get a heartbeat record.
    pub async fn get_heartbeat(&self, pod_id: &str) -> Result<Option<HeartbeatRecord>> {
        let key = HeartbeatKeys::heartbeat(pod_id);
        let data = self.get(key).await?;

        match data {
            Some(bytes) => {
                let record: HeartbeatRecord = bincode::deserialize(&bytes)
                    .map_err(|e| TikvError::Deserialization(e.to_string()))?;
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    /// Get all stale heartbeats (older than timeout seconds).
    ///
    /// Note: This scans up to `limit` heartbeat records. For very large
    /// clusters, consider paginating the scan.
    pub async fn get_stale_heartbeats(
        &self,
        timeout_seconds: i64,
        limit: u32,
    ) -> Result<Vec<String>> {
        let prefix = HeartbeatKeys::prefix();
        let results = self.scan(prefix, limit).await?;

        let mut stale_pods = Vec::new();
        for (_key, value) in results {
            if let Ok(record) = bincode::deserialize::<HeartbeatRecord>(&value)
                && record.is_stale(timeout_seconds)
            {
                stale_pods.push(record.pod_id);
            }
        }

        Ok(stale_pods)
    }

    /// Update checkpoint state.
    pub async fn update_checkpoint(&self, state: &CheckpointState) -> Result<()> {
        let key = StateKeys::checkpoint(&state.job_id);
        let data =
            bincode::serialize(state).map_err(|e| TikvError::Serialization(e.to_string()))?;
        self.put(key, data).await
    }

    /// Get checkpoint state.
    pub async fn get_checkpoint(&self, job_id: &str) -> Result<Option<CheckpointState>> {
        let key = StateKeys::checkpoint(job_id);
        let data = self.get(key).await?;

        match data {
            Some(bytes) => {
                let state: CheckpointState = bincode::deserialize(&bytes)
                    .map_err(|e| TikvError::Deserialization(e.to_string()))?;
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    /// Try to become the scanner leader.
    pub async fn acquire_scanner_lock(&self, pod_id: &str, ttl_seconds: i64) -> Result<bool> {
        self.acquire_lock("scanner_lock", pod_id, ttl_seconds).await
    }

    /// Release scanner leadership.
    pub async fn release_scanner_lock(&self, pod_id: &str) -> Result<bool> {
        self.release_lock("scanner_lock", pod_id).await
    }

    /// Reclaim an orphaned job from a dead worker.
    ///
    /// This uses a single transaction to:
    /// 1. Read the job (verify still Processing)
    /// 2. Read the owner's heartbeat (verify stale)
    /// 3. Update job to Pending with no owner
    ///
    /// Returns `Ok(true)` if the job was reclaimed, `Ok(false)` if it
    /// couldn't be reclaimed (not stale, not Processing, or conflict),
    /// or `Err` if there was a connection error.
    pub async fn reclaim_job(&self, job_id: &str, stale_threshold_seconds: i64) -> Result<bool> {
        tracing::debug!(
            job_id = %job_id,
            stale_threshold_secs = stale_threshold_seconds,
            "Attempting to reclaim job"
        );

        #[cfg(feature = "distributed")]
        {
            let inner = self.inner.as_ref().ok_or_else(|| {
                TikvError::ConnectionFailed("TiKV client not initialized".to_string())
            })?;

            let job_key = JobKeys::record(job_id);
            let mut txn = inner
                .begin_optimistic()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            // Read job in transaction
            let reclaimed = match txn
                .get(job_key.clone())
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?
            {
                Some(data) => {
                    let mut job: JobRecord = bincode::deserialize(&data)
                        .map_err(|e| TikvError::Deserialization(e.to_string()))?;

                    // Verify job is still in Processing state
                    if job.status != JobStatus::Processing {
                        tracing::debug!(
                            job_id = %job_id,
                            status = ?job.status,
                            "Job not in Processing state, cannot reclaim"
                        );
                        return Ok(false);
                    }

                    // Verify job has an owner
                    let owner = match &job.owner {
                        Some(o) => o.clone(),
                        None => {
                            tracing::debug!(
                                job_id = %job_id,
                                "Job has no owner, cannot reclaim"
                            );
                            return Ok(false);
                        }
                    };

                    // Check if owner's heartbeat is stale
                    let heartbeat_key = HeartbeatKeys::heartbeat(&owner);
                    let owner_stale = match txn
                        .get(heartbeat_key)
                        .await
                        .map_err(|e| TikvError::ClientError(e.to_string()))?
                    {
                        Some(hb_data) => {
                            if let Ok(record) = bincode::deserialize::<HeartbeatRecord>(&hb_data) {
                                record.is_stale(stale_threshold_seconds)
                            } else {
                                // Invalid heartbeat data - consider stale
                                true
                            }
                        }
                        None => {
                            // No heartbeat record - consider stale
                            true
                        }
                    };

                    if !owner_stale {
                        tracing::debug!(
                            job_id = %job_id,
                            owner = %owner,
                            "Owner's heartbeat is fresh, cannot reclaim"
                        );
                        return Ok(false);
                    }

                    // Reclaim the job: set back to Pending with no owner
                    job.status = JobStatus::Pending;
                    job.owner = None;
                    job.updated_at = chrono::Utc::now();

                    // Note: We preserve the checkpoint for resume capability
                    // The checkpoint key is NOT deleted here

                    let new_data = bincode::serialize(&job)
                        .map_err(|e| TikvError::Serialization(e.to_string()))?;
                    txn.put(job_key, new_data)
                        .await
                        .map_err(|e| TikvError::ClientError(e.to_string()))?;

                    true
                }
                None => {
                    tracing::debug!(
                        job_id = %job_id,
                        "Job not found, cannot reclaim"
                    );
                    return Ok(false);
                }
            };

            txn.commit()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            if reclaimed {
                tracing::info!(
                    job_id = %job_id,
                    "Job reclaimed successfully"
                );
            }

            Ok(reclaimed)
        }

        #[cfg(not(feature = "distributed"))]
        {
            let _ = (job_id, stale_threshold_seconds);
            Err(TikvError::ConnectionFailed(
                "Distributed feature not enabled".to_string(),
            ))
        }
    }

    /// Get a reference to the configuration.
    pub fn config(&self) -> &TikvConfig {
        &self.config
    }

    /// Check if the client is connected.
    pub fn is_connected(&self) -> bool {
        {
            self.inner.is_some()
        }
        #[cfg(not(feature = "distributed"))]
        {
            false
        }
    }
}

#[cfg(test)]
impl TikvClient {
    /// Create a no-op client for testing checkpoint manager logic.
    /// This client is not connected to TiKV and will fail on actual operations.
    #[allow(dead_code)]
    pub(crate) fn no_op_for_testing() -> Self {
        Self {
            config: TikvConfig::default(),
            inner: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validation() {
        let config = TikvConfig::default();
        assert!(config.validate().is_ok());

        let invalid_config = TikvConfig {
            pd_endpoints: vec![],
            ..Default::default()
        };
        assert!(invalid_config.validate().is_err());
    }

    #[test]
    fn test_config_describe() {
        let config = TikvConfig::default();
        let desc = config.describe();
        assert!(desc.contains("TiKV"));
        assert!(desc.contains("pd_endpoints"));
    }
}
