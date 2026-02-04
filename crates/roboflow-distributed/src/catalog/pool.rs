// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! TiKV client pool and connection management.

use std::sync::Arc;
use std::time::Duration;

use tikv_client::{Config as TiKVClientConfig, TransactionClient};

use super::config::TiKVConfig;
use roboflow_core::RoboflowError as Error;

/// TiKV connection pool wrapper.
///
/// Wraps the TiKV transaction client for catalog operations.
pub struct TiKVPool {
    /// The TiKV transaction client.
    client: Arc<TransactionClient>,
    /// Configuration (stored for potential reconnection scenarios).
    _config: TiKVConfig,
}

impl TiKVPool {
    /// Create a new TiKV pool with the given configuration.
    pub async fn new(config: TiKVConfig) -> Result<Self, Error> {
        // Build TiKV client configuration
        let mut tikv_config = TiKVClientConfig::default();

        // Set CA path if provided
        if let Some(ca_path) = &config.ca_path {
            tikv_config.ca_path = Some(ca_path.clone().into());
        }
        if let Some(cert_path) = &config.cert_path {
            tikv_config.cert_path = Some(cert_path.clone().into());
        }
        if let Some(key_path) = &config.key_path {
            tikv_config.key_path = Some(key_path.clone().into());
        }

        // Set timeout with clamp between 5-60 seconds
        tikv_config.timeout = Duration::from_secs(config.connection_timeout.as_secs().clamp(5, 60));

        // Create transaction client - tikv-client uses new_with_config
        let client = TransactionClient::new_with_config(config.pd_endpoints.clone(), tikv_config)
            .await
            .map_err(|e| Error::other(format!("Failed to connect to TiKV: {}", e)))?;

        tracing::info!(
            "Connected to TiKV: pd_endpoints={:?}, timeout={:?}",
            config.pd_endpoints,
            config.connection_timeout
        );

        Ok(Self {
            client: Arc::new(client),
            _config: config,
        })
    }

    /// Get a reference to the underlying TiKV client.
    pub fn client(&self) -> &Arc<TransactionClient> {
        &self.client
    }

    /// Check connection health with a ping operation.
    pub async fn ping(&self) -> Result<(), Error> {
        // Try to get a non-existent key to verify connectivity
        let mut txn = self
            .client
            .begin_optimistic()
            .await
            .map_err(|e| Error::other(format!("TiKV health check failed: {}", e)))?;

        let _ = txn
            .get(b"roboflow/health/ping".to_vec())
            .await
            .map_err(|e| Error::other(format!("TiKV health check failed: {}", e)))?;

        txn.commit()
            .await
            .map_err(|e| Error::other(format!("TiKV health check failed: {}", e)))?;

        Ok(())
    }

    /// Get a value from TiKV.
    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
        let mut txn = self
            .client
            .begin_optimistic()
            .await
            .map_err(|e| Error::other(format!("TiKV get failed: {}", e)))?;

        let result = txn
            .get(key)
            .await
            .map_err(|e| Error::other(format!("TiKV get failed: {}", e)))?;

        txn.commit()
            .await
            .map_err(|e| Error::other(format!("TiKV get failed: {}", e)))?;

        Ok(result)
    }

    /// Put a value into TiKV.
    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error> {
        let mut txn = self
            .client
            .begin_optimistic()
            .await
            .map_err(|e| Error::other(format!("TiKV put failed: {}", e)))?;

        txn.put(key, value)
            .await
            .map_err(|e| Error::other(format!("TiKV put failed: {}", e)))?;

        txn.commit()
            .await
            .map_err(|e| Error::other(format!("TiKV put failed: {}", e)))?;

        Ok(())
    }

    /// Batch put multiple key-value pairs atomically.
    pub async fn batch_put(&self, kvs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), Error> {
        let mut txn = self
            .client
            .begin_optimistic()
            .await
            .map_err(|e| Error::other(format!("TiKV begin transaction failed: {}", e)))?;

        for (key, value) in kvs {
            txn.put(key, value)
                .await
                .map_err(|e| Error::other(format!("TiKV batch put failed: {}", e)))?;
        }

        txn.commit()
            .await
            .map_err(|e| Error::other(format!("TiKV commit failed: {}", e)))?;

        Ok(())
    }

    /// Delete a key from TiKV.
    pub async fn delete(&self, key: Vec<u8>) -> Result<(), Error> {
        let mut txn = self
            .client
            .begin_optimistic()
            .await
            .map_err(|e| Error::other(format!("TiKV delete failed: {}", e)))?;

        txn.delete(key)
            .await
            .map_err(|e| Error::other(format!("TiKV delete failed: {}", e)))?;

        txn.commit()
            .await
            .map_err(|e| Error::other(format!("TiKV delete failed: {}", e)))?;

        Ok(())
    }

    /// Scan keys with a given prefix.
    pub async fn scan_prefix(&self, prefix: Vec<u8>, limit: u32) -> Result<Vec<Vec<u8>>, Error> {
        let mut txn = self
            .client
            .begin_optimistic()
            .await
            .map_err(|e| Error::other(format!("TiKV scan failed: {}", e)))?;

        // Create a proper prefix scan range using exclusive upper bound
        let mut scan_end = prefix.clone();
        scan_end.push(0);

        let iter = txn
            .scan(prefix..scan_end, limit)
            .await
            .map_err(|e| Error::other(format!("TiKV scan failed: {}", e)))?;

        // Collect the iterator - KvPair needs conversion
        let result: Vec<(Vec<u8>, Vec<u8>)> = iter
            .map(|pair| {
                let key: Vec<u8> = pair.key().clone().into();
                let value: Vec<u8> = pair.value().clone();
                (key, value)
            })
            .collect();

        txn.commit()
            .await
            .map_err(|e| Error::other(format!("TiKV scan failed: {}", e)))?;

        Ok(result.into_iter().map(|(k, _)| k).collect())
    }

    /// Scan values with a given prefix.
    pub async fn scan_prefix_values(
        &self,
        prefix: Vec<u8>,
        limit: u32,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let mut txn = self
            .client
            .begin_optimistic()
            .await
            .map_err(|e| Error::other(format!("TiKV scan values failed: {}", e)))?;

        // Create a proper prefix scan range using exclusive upper bound
        let mut scan_end = prefix.clone();
        scan_end.push(0);

        let iter = txn
            .scan(prefix..scan_end, limit)
            .await
            .map_err(|e| Error::other(format!("TiKV scan values failed: {}", e)))?;

        // Collect the iterator - KvPair needs conversion
        let result: Vec<(Vec<u8>, Vec<u8>)> = iter
            .map(|pair| {
                let key: Vec<u8> = pair.key().clone().into();
                let value: Vec<u8> = pair.value().clone();
                (key, value)
            })
            .collect();

        txn.commit()
            .await
            .map_err(|e| Error::other(format!("TiKV scan values failed: {}", e)))?;

        Ok(result.into_iter().map(|(_, v)| v).collect())
    }

    /// Put a value only if the key does not exist.
    pub async fn put_if_not_exists(&self, key: Vec<u8>, value: Vec<u8>) -> Result<bool, Error> {
        let exists = self.get(key.clone()).await?.is_some();
        if exists {
            return Ok(false);
        }

        self.put(key, value).await?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_config() {
        let config = TiKVConfig::with_pd_endpoints("127.0.0.1:2379");
        assert_eq!(config.pd_endpoints, vec!["127.0.0.1:2379"]);
    }
}
