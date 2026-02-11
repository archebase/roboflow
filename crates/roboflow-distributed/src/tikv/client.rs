// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! TiKV client wrapper for distributed coordination.
//!
//! Provides connection pooling and basic CRUD operations for TiKV.
//!
//! # MVCC & TSO Awareness
//!
//! TiKV uses MVCC (Multi-Version Concurrency Control) with a Timestamp
//! Oracle (TSO) from PD. Every transaction gets a `start_ts` that determines
//! its snapshot. The PD client **batches** TSO allocations for efficiency,
//! which means `begin_optimistic()` may return a transaction whose `start_ts`
//! predates recently committed writes — causing **stale reads**.
//!
//! To avoid this, we use three strategies:
//!
//! - **Read-only operations** (`get`, `scan`, `batch_get`): Use
//!   `current_timestamp()` + `snapshot()` to obtain a guaranteed-fresh TSO
//!   directly from PD, bypassing the batched cache.
//!
//! - **Read-then-write operations** (`transactional_claim`, `cas`,
//!   `acquire_lock`, `release_lock`): Use **pessimistic transactions**
//!   (`begin_pessimistic()`) which acquire row locks on read and always
//!   see the latest committed state.
//!
//! - **Write-only operations** (`put`, `delete`, `batch_put`): Use
//!   optimistic transactions — no read means no stale-snapshot risk.
//!
//! # Scan Behavior
//!
//! The `scan` method returns keys in lexicographic order. Use the
//! `KeyBuilder` or `*Keys` types to construct proper prefix-based keys.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::sleep;

use super::config::TikvConfig;
use super::error::{Result, TikvError};
use super::key::{ConfigKeys, HeartbeatKeys, LockKeys, StateKeys};
use super::schema::{CheckpointState, ConfigRecord, HeartbeatRecord, LockRecord};

/// TiKV client wrapper with connection pooling and circuit breaker.
#[derive(Clone)]
pub struct TikvClient {
    /// TiKV configuration.
    config: TikvConfig,

    /// Underlying transaction client.
    inner: Option<Arc<tikv_client::TransactionClient>>,

    /// Circuit breaker for fault tolerance.
    circuit_breaker: Arc<super::circuit::CircuitBreaker>,
}

impl TikvClient {
    /// Create a new TiKV client with the given configuration.
    pub async fn new(config: TikvConfig) -> Result<Self> {
        // Validate configuration first
        config.validate()?;

        // Create circuit breaker with default configuration
        let circuit_breaker = Arc::new(super::circuit::CircuitBreaker::new());

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
            circuit_breaker,
        })
    }

    /// Create a new client with default configuration from environment.
    pub async fn from_env() -> Result<Self> {
        Self::new(TikvConfig::default()).await
    }

    /// Get the circuit breaker state (for monitoring).
    pub fn circuit_state(&self) -> super::circuit::CircuitState {
        self.circuit_breaker.state()
    }

    /// Get the circuit breaker failure count (for monitoring).
    pub fn circuit_failure_count(&self) -> u64 {
        self.circuit_breaker.failure_count()
    }

    /// Get a value by key.
    ///
    /// Uses a fresh TSO snapshot to guarantee visibility of all committed writes.
    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        // Check circuit breaker state before attempting operation
        if !self.circuit_breaker.is_call_permitted() {
            return Err(TikvError::CircuitOpen {
                failures: self.circuit_breaker.failure_count() as u32,
            });
        }

        {
            let inner = self.inner.as_ref().ok_or_else(|| {
                TikvError::ConnectionFailed("TiKV client not initialized".to_string())
            })?;

            // Get a fresh timestamp directly from PD (bypasses TSO batch cache)
            let ts = inner.current_timestamp().await.map_err(|e| {
                self.circuit_breaker.record_failure();
                TikvError::ClientError(e.to_string())
            })?;

            // Snapshot is read-only; use Warn drop-check to avoid panic on drop
            let mut snap = inner.snapshot(
                ts,
                tikv_client::TransactionOptions::new_optimistic()
                    .drop_check(tikv_client::CheckLevel::Warn),
            );

            let result = snap.get(key).await.map_err(|e| {
                self.circuit_breaker.record_failure();
                TikvError::ClientError(e.to_string())
            })?;

            // Record success for circuit breaker
            self.circuit_breaker.record_success();
            Ok(result)
        }
    }

    /// Put a key-value pair.
    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // Check circuit breaker state before attempting operation
        if !self.circuit_breaker.is_call_permitted() {
            return Err(TikvError::CircuitOpen {
                failures: self.circuit_breaker.failure_count() as u32,
            });
        }

        {
            let inner = self.inner.as_ref().ok_or_else(|| {
                TikvError::ConnectionFailed("TiKV client not initialized".to_string())
            })?;

            let mut txn = inner.begin_optimistic().await.map_err(|e| {
                self.circuit_breaker.record_failure();
                TikvError::ClientError(e.to_string())
            })?;

            txn.put(key, value).await.map_err(|e| {
                self.circuit_breaker.record_failure();
                TikvError::ClientError(e.to_string())
            })?;

            txn.commit().await.map_err(|e| {
                self.circuit_breaker.record_failure();
                TikvError::ClientError(e.to_string())
            })?;

            self.circuit_breaker.record_success();
            Ok(())
        }
    }

    /// Delete a key.
    pub async fn delete(&self, key: Vec<u8>) -> Result<()> {
        // Check circuit breaker state before attempting operation
        if !self.circuit_breaker.is_call_permitted() {
            return Err(TikvError::CircuitOpen {
                failures: self.circuit_breaker.failure_count() as u32,
            });
        }

        {
            let inner = self.inner.as_ref().ok_or_else(|| {
                TikvError::ConnectionFailed("TiKV client not initialized".to_string())
            })?;

            let mut txn = inner.begin_optimistic().await.map_err(|e| {
                self.circuit_breaker.record_failure();
                TikvError::ClientError(e.to_string())
            })?;

            txn.delete(key).await.map_err(|e| {
                self.circuit_breaker.record_failure();
                TikvError::ClientError(e.to_string())
            })?;

            txn.commit().await.map_err(|e| {
                self.circuit_breaker.record_failure();
                TikvError::ClientError(e.to_string())
            })?;

            self.circuit_breaker.record_success();
            Ok(())
        }
    }

    /// Scan keys with a prefix.
    ///
    /// Uses a fresh TSO snapshot to guarantee visibility of all committed writes.
    /// Returns keys in lexicographic order, limited to `limit` results.
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

            // Get a fresh timestamp directly from PD (bypasses TSO batch cache)
            let ts = inner
                .current_timestamp()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            // Snapshot is read-only; use Warn drop-check to avoid panic on drop
            let mut snap = inner.snapshot(
                ts,
                tikv_client::TransactionOptions::new_optimistic()
                    .drop_check(tikv_client::CheckLevel::Warn),
            );

            // Create a proper prefix scan range using exclusive upper bound.
            // We append 0xFF to ensure the scan range includes all keys with the prefix.
            let mut scan_end = prefix.clone();
            scan_end.push(0xFF);

            let iter = snap
                .scan(prefix.clone()..scan_end, limit)
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            // Collect the iterator into a Vec
            #[allow(clippy::useless_conversion)]
            let result: Vec<(Vec<u8>, Vec<u8>)> = iter
                .map(|pair| {
                    #[allow(clippy::useless_conversion)]
                    let key: Vec<u8> = pair.key().clone().into();
                    #[allow(clippy::useless_conversion)]
                    let value: Vec<u8> = pair.value().clone().into();
                    (key, value)
                })
                .collect();

            tracing::debug!(limit = limit, results = result.len(), "Scan completed");

            Ok(result)
        }
    }

    /// Batch get multiple keys.
    ///
    /// Uses a fresh TSO snapshot to guarantee visibility of all committed writes.
    pub async fn batch_get(&self, keys: Vec<Vec<u8>>) -> Result<Vec<Option<Vec<u8>>>> {
        {
            let inner = self.inner.as_ref().ok_or_else(|| {
                TikvError::ConnectionFailed("TiKV client not initialized".to_string())
            })?;

            // Get a fresh timestamp directly from PD (bypasses TSO batch cache)
            let ts = inner
                .current_timestamp()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            // Snapshot is read-only; use Warn drop-check to avoid panic on drop
            let mut snap = inner.snapshot(
                ts,
                tikv_client::TransactionOptions::new_optimistic()
                    .drop_check(tikv_client::CheckLevel::Warn),
            );

            let mut results = Vec::new();
            for key in &keys {
                let value = snap
                    .get(key.clone())
                    .await
                    .map_err(|e| TikvError::ClientError(e.to_string()))?;
                results.push(value);
            }

            Ok(results)
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
    }

    /// Compare-And-Swap (CAS) operation for atomic updates.
    ///
    /// Uses a **pessimistic transaction** to read-then-write atomically,
    /// ensuring the read always sees the latest committed state.
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
                .begin_pessimistic()
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
    }

    // ============== High-level operations for specific types ==============

    /// Transactional claim operation for work units.
    ///
    /// This atomically claims a work unit using a callback that processes
    /// the raw work unit data. The callback should return:
    /// - Some(new_data) if the work unit was successfully claimed
    /// - None if the work unit couldn't be claimed
    ///
    /// Uses a **pessimistic transaction** so the read acquires a lock and
    /// always sees the latest committed state (no stale TSO batch issue).
    pub async fn transactional_claim<F>(
        &self,
        work_unit_key: Vec<u8>,
        pending_key: Vec<u8>,
        _worker_id: &str,
        claim_processor: F,
    ) -> Result<Option<Vec<u8>>>
    where
        F: FnOnce(
            &[u8],
        )
            -> std::result::Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>>,
    {
        let inner = self.inner.as_ref().ok_or_else(|| {
            TikvError::ConnectionFailed("TiKV client not initialized".to_string())
        })?;

        let mut txn = inner
            .begin_pessimistic()
            .await
            .map_err(|e| TikvError::ClientError(e.to_string()))?;

        // Read work unit in transaction
        let claimed_data = match txn
            .get(work_unit_key.clone())
            .await
            .map_err(|e| TikvError::ClientError(e.to_string()))?
        {
            Some(data) => {
                // Call the claim processor to check and update the work unit
                match claim_processor(&data) {
                    Ok(Some(new_data)) => {
                        let data_clone = new_data.clone();
                        txn.put(work_unit_key, new_data)
                            .await
                            .map_err(|e| TikvError::ClientError(e.to_string()))?;

                        // Remove from pending queue in same transaction
                        txn.delete(pending_key)
                            .await
                            .map_err(|e| TikvError::ClientError(e.to_string()))?;

                        txn.commit()
                            .await
                            .map_err(|e| TikvError::ClientError(e.to_string()))?;

                        Some(data_clone)
                    }
                    Ok(None) => {
                        // Not claimable - commit transaction with no changes
                        txn.commit()
                            .await
                            .map_err(|e| TikvError::ClientError(e.to_string()))?;
                        None
                    }
                    Err(e) => {
                        // Claim processor failed
                        return Err(TikvError::Other(format!("Claim processor failed: {}", e)));
                    }
                }
            }
            None => {
                // Work unit doesn't exist, clean up pending index
                txn.delete(pending_key)
                    .await
                    .map_err(|e| TikvError::ClientError(e.to_string()))?;
                txn.commit()
                    .await
                    .map_err(|e| TikvError::ClientError(e.to_string()))?;
                None
            }
        };

        Ok(claimed_data)
    }

    /// Acquire a distributed lock (atomic operation within a single transaction).
    ///
    /// Uses a **pessimistic transaction** so the read acquires a row lock,
    /// preventing race conditions between concurrent lock acquisition attempts.
    ///
    /// Retries on transient errors (WriteConflict, PessimisticLock, etc.) with
    /// exponential backoff using `max_retries` and `retry_base_delay_ms`.
    pub async fn acquire_lock(
        &self,
        resource: &str,
        owner: &str,
        ttl_seconds: i64,
    ) -> Result<bool> {
        let max_retries = self.config.max_retries;
        let base_delay_ms = self.config.retry_base_delay_ms;

        for attempt in 0..=max_retries {
            match self.acquire_lock_once(resource, owner, ttl_seconds).await {
                Ok(acquired) => return Ok(acquired),
                Err(e) => {
                    if !e.is_retryable() {
                        return Err(e);
                    }
                    if attempt < max_retries {
                        let delay_ms = base_delay_ms.saturating_mul(2_u64.pow(attempt));
                        let delay = Duration::from_millis(delay_ms);
                        tracing::warn!(
                            resource = %resource,
                            attempt = attempt + 1,
                            max = max_retries + 1,
                            error = %e,
                            "Lock acquisition failed with retryable error, retrying"
                        );
                        sleep(delay).await;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
        unreachable!("retry loop always returns via Ok or Err")
    }

    /// Single attempt to acquire a lock (no retry). Used internally by `acquire_lock`.
    async fn acquire_lock_once(
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

        let inner = self.inner.as_ref().ok_or_else(|| {
            TikvError::ConnectionFailed("TiKV client not initialized".to_string())
        })?;

        let key = LockKeys::lock(resource);
        let mut txn = inner
            .begin_pessimistic()
            .await
            .map_err(|e| TikvError::ClientError(e.to_string()))?;

        // Run transactional logic; on any error we must rollback before returning
        let body_result: Result<bool> = async {
            let current = txn
                .get(key.clone())
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;
            let acquired = match current {
                Some(data) => {
                    let existing: LockRecord = bincode::deserialize(&data)
                        .map_err(|e| TikvError::Deserialization(e.to_string()))?;

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
                        tracing::debug!(
                            resource = %resource,
                            owner = %owner,
                            current_owner = %existing.owner,
                            "Lock held by another owner"
                        );
                        false
                    } else {
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
            Ok(acquired)
        }
        .await;

        match body_result {
            Ok(acquired) => {
                txn.commit()
                    .await
                    .map_err(|e| TikvError::ClientError(e.to_string()))?;
                Ok(acquired)
            }
            Err(e) => {
                let _ = txn.rollback().await;
                Err(e)
            }
        }
    }

    /// Release a distributed lock (atomic operation within a single transaction).
    ///
    /// Uses a **pessimistic transaction** to read-verify-delete atomically.
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
                .begin_pessimistic()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            let body_result: Result<bool> = async {
                let current = txn
                    .get(key.clone())
                    .await
                    .map_err(|e| TikvError::ClientError(e.to_string()))?;
                let released = match current {
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
                Ok(released)
            }
            .await;

            match body_result {
                Ok(released) => {
                    txn.commit()
                        .await
                        .map_err(|e| TikvError::ClientError(e.to_string()))?;
                    Ok(released)
                }
                Err(e) => {
                    let _ = txn.rollback().await;
                    Err(e)
                }
            }
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

    /// Store a dataset configuration in TiKV.
    pub async fn put_config(&self, config: &ConfigRecord) -> Result<()> {
        let key = ConfigKeys::config(&config.hash);
        let data =
            bincode::serialize(config).map_err(|e| TikvError::Serialization(e.to_string()))?;
        self.put(key, data).await
    }

    /// Get a dataset configuration from TiKV.
    pub async fn get_config(&self, hash: &str) -> Result<Option<ConfigRecord>> {
        let key = ConfigKeys::config(hash);
        let data = self.get(key).await?;

        match data {
            Some(bytes) => {
                let config: ConfigRecord = bincode::deserialize(&bytes)
                    .map_err(|e| TikvError::Deserialization(e.to_string()))?;
                Ok(Some(config))
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

    /// Get a reference to the configuration.
    pub fn config(&self) -> &TikvConfig {
        &self.config
    }

    /// Check if the client is connected.
    pub fn is_connected(&self) -> bool {
        self.inner.is_some()
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
