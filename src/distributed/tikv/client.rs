// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! TiKV client wrapper for distributed coordination.
//!
//! Provides connection pooling and basic CRUD operations for TiKV.

use std::sync::Arc;

use super::config::TikvConfig;
use super::error::{Result, TikvError};
use super::key::{HeartbeatKeys, JobKeys, LockKeys, StateKeys};
use super::schema::{CheckpointState, HeartbeatRecord, JobRecord, LockRecord};

/// TiKV client wrapper with connection pooling.
#[derive(Clone)]
pub struct TikvClient {
    /// TiKV configuration.
    config: TikvConfig,

    /// Underlying transaction client.
    #[cfg(feature = "distributed")]
    inner: Option<Arc<tikv_client::TransactionClient>>,
}

impl TikvClient {
    /// Create a new TiKV client with the given configuration.
    pub async fn new(config: TikvConfig) -> Result<Self> {
        // Validate configuration first
        config.validate()?;

        #[cfg(feature = "distributed")]
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

    /// Get a value by key.
    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        #[cfg(feature = "distributed")]
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
        #[cfg(feature = "distributed")]
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
        #[cfg(feature = "distributed")]
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
    pub async fn scan(&self, prefix: Vec<u8>, limit: u32) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        #[cfg(feature = "distributed")]
        {
            let inner = self.inner.as_ref().ok_or_else(|| {
                TikvError::ConnectionFailed("TiKV client not initialized".to_string())
            })?;

            let mut txn = inner
                .begin_optimistic()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            let iter = txn
                .scan(prefix.clone()..=prefix, limit)
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

            // Collect the iterator into a Vec
            let result: Vec<(Vec<u8>, Vec<u8>)> = iter
                .map(|pair| {
                    let key: Vec<u8> = pair.key().clone().into();
                    let value: Vec<u8> = pair.value().clone().into();
                    (key, value)
                })
                .collect();

            txn.commit()
                .await
                .map_err(|e| TikvError::ClientError(e.to_string()))?;

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
        #[cfg(feature = "distributed")]
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
        #[cfg(feature = "distributed")]
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
    /// Returns `Ok(true)` if the operation succeeded,
    /// `Ok(false)` if the version mismatched,
    /// or `Err` if there was a connection error.
    pub async fn cas(
        &self,
        key: Vec<u8>,
        expected_version: u64,
        new_value: Vec<u8>,
    ) -> Result<bool> {
        #[cfg(feature = "distributed")]
        {
            use serde_json::Value;

            // First, get the current value to check version
            let current = self.get(key.clone()).await?;

            if let Some(existing_data) = current {
                // Parse and check version
                if let Ok(value_json) = serde_json::from_slice::<Value>(&existing_data) {
                    if let Some(version) = value_json.get("version").and_then(|v| v.as_u64()) {
                        if version != expected_version {
                            return Err(TikvError::CasFailed {
                                expected: expected_version,
                                got: version,
                            });
                        }
                    }
                }
            }

            // Version matches or key doesn't exist, do the put
            self.put(key, new_value).await?;
            Ok(true)
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

    /// Claim a job (atomic CAS operation).
    pub async fn claim_job(&self, file_hash: &str, pod_id: &str) -> Result<bool> {
        let key = JobKeys::record(file_hash);

        // Get current job state
        let current = self.get_job(file_hash).await?;

        match current {
            Some(mut job) if job.is_claimable() => {
                job.claim(pod_id.to_string())
                    .map_err(|e| TikvError::Other(e))?;

                let data = bincode::serialize(&job)
                    .map_err(|e| TikvError::Serialization(e.to_string()))?;

                self.put(key, data).await?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Complete a job.
    pub async fn complete_job(&self, file_hash: &str) -> Result<()> {
        if let Some(mut job) = self.get_job(file_hash).await? {
            job.complete();
            self.put_job(&job).await?;
        }
        Ok(())
    }

    /// Fail a job with an error message.
    pub async fn fail_job(&self, file_hash: &str, error: String) -> Result<()> {
        if let Some(mut job) = self.get_job(file_hash).await? {
            job.fail(error);
            self.put_job(&job).await?;
        }
        Ok(())
    }

    /// Acquire a distributed lock.
    pub async fn acquire_lock(
        &self,
        resource: &str,
        owner: &str,
        ttl_seconds: i64,
    ) -> Result<bool> {
        let key = LockKeys::lock(resource);

        // Check if lock exists and is still valid
        if let Some(data) = self.get(key.clone()).await? {
            let existing: LockRecord = bincode::deserialize(&data)
                .map_err(|e| TikvError::Deserialization(e.to_string()))?;

            if !existing.is_expired() && existing.is_owned_by(owner) {
                // Already own the lock, extend it
                let mut lock = existing;
                lock.extend(ttl_seconds);
                let new_data = bincode::serialize(&lock)
                    .map_err(|e| TikvError::Serialization(e.to_string()))?;
                self.put(key, new_data).await?;
                return Ok(true);
            }

            if !existing.is_expired() {
                // Lock is held by someone else
                return Ok(false);
            }
        }

        // Create new lock
        let lock = LockRecord::new(resource.to_string(), owner.to_string(), ttl_seconds);
        let data =
            bincode::serialize(&lock).map_err(|e| TikvError::Serialization(e.to_string()))?;
        self.put(key, data).await?;
        Ok(true)
    }

    /// Release a distributed lock.
    pub async fn release_lock(&self, resource: &str, owner: &str) -> Result<bool> {
        let key = LockKeys::lock(resource);

        if let Some(data) = self.get(key.clone()).await? {
            let existing: LockRecord = bincode::deserialize(&data)
                .map_err(|e| TikvError::Deserialization(e.to_string()))?;

            if existing.is_owned_by(owner) {
                self.delete(key).await?;
                return Ok(true);
            }
        }

        Ok(false)
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
    pub async fn get_stale_heartbeats(&self, timeout_seconds: i64) -> Result<Vec<String>> {
        let prefix = HeartbeatKeys::prefix();
        let results = self.scan(prefix, 1000).await?;

        let mut stale_pods = Vec::new();
        for (_key, value) in results {
            if let Ok(record) = bincode::deserialize::<HeartbeatRecord>(&value) {
                if record.is_stale(timeout_seconds) {
                    stale_pods.push(record.pod_id);
                }
            }
        }

        Ok(stale_pods)
    }

    /// Update checkpoint state.
    pub async fn update_checkpoint(&self, state: &CheckpointState) -> Result<()> {
        let key = StateKeys::checkpoint(&state.file_hash);
        let data =
            bincode::serialize(state).map_err(|e| TikvError::Serialization(e.to_string()))?;
        self.put(key, data).await
    }

    /// Get checkpoint state.
    pub async fn get_checkpoint(&self, file_hash: &str) -> Result<Option<CheckpointState>> {
        let key = StateKeys::checkpoint(file_hash);
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

    /// Get a reference to the configuration.
    pub fn config(&self) -> &TikvConfig {
        &self.config
    }

    /// Check if the client is connected.
    pub fn is_connected(&self) -> bool {
        #[cfg(feature = "distributed")]
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
mod tests {
    use super::*;

    #[test]
    fn test_config_validation() {
        let config = TikvConfig::default();
        assert!(config.validate().is_ok());

        let mut invalid_config = TikvConfig::default();
        invalid_config.pd_endpoints = vec![];
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
