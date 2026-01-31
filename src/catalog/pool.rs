// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! TiKV client pool and connection management.

use std::sync::Arc;
use std::time::Duration;

use tikv_client::{Config as TiKVClientConfig, TransactionClient};

use super::config::TiKVConfig;
use crate::RoboflowError as Error;

/// TiKV connection pool wrapper.
///
/// Wraps the TiKV transaction client for catalog operations.
pub struct TiKVPool {
    /// The TiKV transaction client.
    client: Arc<TransactionClient>,
    /// Configuration.
    config: TiKVConfig,
}

impl TiKVPool {
    /// Create a new TiKV pool with the given configuration.
    pub async fn new(config: TiKVConfig) -> Result<Self, Error> {
        // Build TiKV client configuration
        let mut tikv_config = TiKVClientConfig::default();

        // Set PD endpoints
        tikv_config.endpoints = config.pd_endpoints.clone();

        // Set timeout
        tikv_config.timeout = Duration::from_secs(
            config.connection_timeout.as_secs().max(5).min(60) // Clamp between 5-60 seconds
        );

        // Create transaction client
        let client = TransactionClient::new(tikv_config)
            .await
            .map_err(|e| Error::other(format!("Failed to connect to TiKV: {}", e)))?;

        tracing::info!(
            "Connected to TiKV: pd_endpoints={:?}, timeout={:?}",
            config.pd_endpoints,
            config.connection_timeout
        );

        Ok(Self {
            client: Arc::new(client),
            config,
        })
    }

    /// Get a reference to the underlying TiKV client.
    pub fn client(&self) -> &Arc<TransactionClient> {
        &self.client
    }

    /// Check connection health with a ping operation.
    pub async fn ping(&self) -> Result<(), Error> {
        // Try to get a non-existent key to verify connectivity
        let _ = self
            .client
            .get(b"roboflow/health/ping".to_vec())
            .await
            .map_err(|e| Error::other(format!("TiKV health check failed: {}", e)))?;

        Ok(())
    }

    /// Get a value from TiKV.
    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
        self.client
            .get(key)
            .await
            .map_err(|e| Error::other(format!("TiKV get failed: {}", e)))
    }

    /// Put a value into TiKV.
    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error> {
        self.client
            .put(key, value)
            .await
            .map_err(|e| Error::other(format!("TiKV put failed: {}", e)))
    }

    /// Batch put multiple key-value pairs atomically.
    pub async fn batch_put(&self, kvs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), Error> {
        let mut client = self.client
            .begin()
            .await
            .map_err(|e| Error::other(format!("TiKV begin transaction failed: {}", e)))?;

        for (key, value) in kvs {
            client
                .put(key, value)
                .map_err(|e| Error::other(format!("TiKV batch put failed: {}", e)))?;
        }

        client
            .commit()
            .await
            .map_err(|e| Error::other(format!("TiKV commit failed: {}", e)))
    }

    /// Delete a key from TiKV.
    pub async fn delete(&self, key: Vec<u8>) -> Result<(), Error> {
        self.client
            .delete(key)
            .await
            .map_err(|e| Error::other(format!("TiKV delete failed: {}", e)))
    }

    /// Scan keys with a given prefix.
    pub async fn scan_prefix(
        &self,
        prefix: Vec<u8>,
        limit: u32,
    ) -> Result<Vec<Vec<u8>>, Error> {
        self.client
            .scan(prefix, limit as usize)
            .await
            .map(|iter| iter.map(|(k, _)| k).collect())
            .map_err(|e| Error::other(format!("TiKV scan failed: {}", e)))
    }

    /// Scan values with a given prefix.
    pub async fn scan_prefix_values(
        &self,
        prefix: Vec<u8>,
        limit: u32,
    ) -> Result<Vec<Vec<u8>>, Error> {
        self.client
            .scan(prefix, limit as usize)
            .await
            .map(|iter| iter.map(|(_, v)| v).collect())
            .map_err(|e| Error::other(format!("TiKV scan values failed: {}", e)))
    }

    /// Put a value only if the key does not exist.
    pub async fn put_if_not_exists(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<bool, Error> {
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
