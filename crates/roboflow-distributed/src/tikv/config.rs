// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! TiKV configuration for distributed coordination.

use std::env;
use std::time::Duration;

use roboflow_core::validators;

use super::error::TikvError;

// Constants from parent module

/// Key prefix for all roboflow data in TiKV.
pub const KEY_PREFIX: &str = "/roboflow/v1/";
/// Default PD endpoints for local development.
pub const DEFAULT_PD_ENDPOINTS: &str = "127.0.0.1:2379";
/// Default connection timeout in seconds.
pub const DEFAULT_CONNECTION_TIMEOUT_SECS: u64 = 10;
/// Default operation timeout in seconds.
pub const DEFAULT_OPERATION_TIMEOUT_SECS: u64 = 30;
/// Default transaction timeout in seconds.
pub const DEFAULT_TRANSACTION_TIMEOUT_SECS: u64 = 60;
/// Default lock TTL in seconds.
pub const DEFAULT_LOCK_TTL_SECS: i64 = 60;
/// Default lock acquire timeout in seconds.
pub const DEFAULT_LOCK_ACQUIRE_TIMEOUT_SECS: u64 = 10;
/// Default maximum retry count.
pub const DEFAULT_MAX_RETRIES: u32 = 10;
/// Default base delay between retries in milliseconds.
pub const DEFAULT_RETRY_BASE_DELAY_MS: u64 = 50;

/// Configuration for TiKV cluster connection.
#[derive(Debug, Clone)]
pub struct TikvConfig {
    /// PD (Placement Driver) endpoints for cluster discovery.
    /// Multiple endpoints can be comma-separated for high availability.
    pub pd_endpoints: Vec<String>,

    /// Connection timeout duration.
    pub connection_timeout: Duration,

    /// Default operation timeout for individual operations.
    pub operation_timeout: Duration,

    /// Default transaction timeout.
    pub transaction_timeout: Duration,

    /// Key prefix for all operations (defaults to `/roboflow/v1/`).
    pub key_prefix: String,

    /// CA certificate path for TLS (optional).
    pub ca_path: Option<String>,

    /// Client certificate path for TLS (optional).
    pub cert_path: Option<String>,

    /// Client key path for TLS (optional).
    pub key_path: Option<String>,

    /// Default lock TTL in seconds.
    pub default_lock_ttl_secs: i64,

    /// Lock acquisition timeout in seconds.
    pub lock_acquire_timeout: Duration,

    /// Maximum retry attempts for write conflicts.
    pub max_retries: u32,

    /// Base delay for retry backoff in milliseconds.
    pub retry_base_delay_ms: u64,
}

impl Default for TikvConfig {
    fn default() -> Self {
        Self {
            pd_endpoints: Self::parse_pd_endpoints(
                &env::var("TIKV_PD_ENDPOINTS").unwrap_or_else(|_| DEFAULT_PD_ENDPOINTS.to_string()),
            ),
            connection_timeout: Duration::from_secs(
                env::var("TIKV_CONNECTION_TIMEOUT_SECS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(DEFAULT_CONNECTION_TIMEOUT_SECS),
            ),
            operation_timeout: Duration::from_secs(
                env::var("TIKV_OPERATION_TIMEOUT_SECS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(DEFAULT_OPERATION_TIMEOUT_SECS),
            ),
            transaction_timeout: Duration::from_secs(
                env::var("TIKV_TRANSACTION_TIMEOUT_SECS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(DEFAULT_TRANSACTION_TIMEOUT_SECS),
            ),
            key_prefix: KEY_PREFIX.to_string(),
            ca_path: env::var("TIKV_CA_PATH").ok(),
            cert_path: env::var("TIKV_CERT_PATH").ok(),
            key_path: env::var("TIKV_KEY_PATH").ok(),
            default_lock_ttl_secs: env::var("TIKV_LOCK_TTL_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_LOCK_TTL_SECS),
            lock_acquire_timeout: Duration::from_secs(
                env::var("TIKV_LOCK_ACQUIRE_TIMEOUT_SECS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(DEFAULT_LOCK_ACQUIRE_TIMEOUT_SECS),
            ),
            max_retries: env::var("TIKV_MAX_RETRIES")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_MAX_RETRIES),
            retry_base_delay_ms: env::var("TIKV_RETRY_BASE_DELAY_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_RETRY_BASE_DELAY_MS),
        }
    }
}

impl TikvConfig {
    /// Parse PD endpoints from a comma-separated string.
    fn parse_pd_endpoints(s: &str) -> Vec<String> {
        s.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    }

    /// Create a new configuration with custom PD endpoints.
    pub fn with_pd_endpoints(pd_endpoints: &str) -> Self {
        Self {
            pd_endpoints: Self::parse_pd_endpoints(pd_endpoints),
            ..Default::default()
        }
    }

    /// Set a custom key prefix.
    pub fn with_key_prefix(mut self, prefix: impl Into<String>) -> Self {
        let mut prefix = prefix.into();
        if !prefix.starts_with('/') {
            prefix = format!("/{}", prefix);
        }
        if !prefix.ends_with('/') {
            prefix = format!("{}/", prefix);
        }
        self.key_prefix = prefix;
        self
    }

    /// Set connection timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = timeout;
        self
    }

    /// Check if TLS is enabled.
    pub fn is_tls_enabled(&self) -> bool {
        self.ca_path.is_some() || self.cert_path.is_some() || self.key_path.is_some()
    }

    /// Build a description of the configuration for logging.
    pub fn describe(&self) -> String {
        let tls = if self.is_tls_enabled() {
            "enabled"
        } else {
            "disabled"
        };
        format!(
            "TiKV(pd_endpoints={:?}, connection_timeout={:?}, operation_timeout={:?}, \
             transaction_timeout={:?}, lock_ttl={}s, lock_acquire_timeout={:?}, \
             max_retries={}, retry_base_delay={}ms, tls={}, key_prefix={})",
            self.pd_endpoints,
            self.connection_timeout,
            self.operation_timeout,
            self.transaction_timeout,
            self.default_lock_ttl_secs,
            self.lock_acquire_timeout,
            self.max_retries,
            self.retry_base_delay_ms,
            tls,
            self.key_prefix
        )
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), TikvError> {
        // Validate PD endpoints
        validators::not_empty(&self.pd_endpoints, "pd_endpoints")
            .map_err(|e| TikvError::InvalidConfig(e.to_string()))?;

        // Validate key prefix format
        validators::starts_with(&self.key_prefix, "/", "key_prefix")
            .map_err(|e| TikvError::InvalidConfig(e.to_string()))?;

        // Validate TLS configuration consistency
        validators::paired(
            self.cert_path.as_ref(),
            self.key_path.as_ref(),
            "cert_path",
            "key_path",
        )
        .map_err(|e| TikvError::InvalidConfig(e.to_string()))?;

        // Validate timeout values
        validators::positive(self.operation_timeout.as_secs(), "operation_timeout")
            .map_err(|e| TikvError::InvalidConfig(e.to_string()))?;

        validators::positive(self.transaction_timeout.as_secs(), "transaction_timeout")
            .map_err(|e| TikvError::InvalidConfig(e.to_string()))?;

        validators::positive(self.default_lock_ttl_secs, "default_lock_ttl_secs")
            .map_err(|e| TikvError::InvalidConfig(e.to_string()))?;

        validators::positive(self.max_retries, "max_retries")
            .map_err(|e| TikvError::InvalidConfig(e.to_string()))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_pd_endpoints_single() {
        let endpoints = TikvConfig::parse_pd_endpoints("127.0.0.1:2379");
        assert_eq!(endpoints, vec!["127.0.0.1:2379"]);
    }

    #[test]
    fn test_parse_pd_endpoints_multiple() {
        let endpoints =
            TikvConfig::parse_pd_endpoints("127.0.0.1:2379,127.0.0.1:2380,127.0.0.1:2381");
        assert_eq!(
            endpoints,
            vec!["127.0.0.1:2379", "127.0.0.1:2380", "127.0.0.1:2381"]
        );
    }

    #[test]
    fn test_with_pd_endpoints() {
        let config = TikvConfig::with_pd_endpoints("192.168.1.1:2379,192.168.1.2:2379");
        assert_eq!(
            config.pd_endpoints,
            vec!["192.168.1.1:2379", "192.168.1.2:2379"]
        );
    }

    #[test]
    fn test_with_key_prefix() {
        let config = TikvConfig::default().with_key_prefix("custom/prefix");
        assert_eq!(config.key_prefix, "/custom/prefix/");
    }

    #[test]
    fn test_validate() {
        let config = TikvConfig::default();
        assert!(config.validate().is_ok());

        let config = TikvConfig {
            pd_endpoints: vec![],
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_tls_consistency() {
        // Only cert without key should fail
        let config = TikvConfig {
            cert_path: Some("/path/to/cert".to_string()),
            ..Default::default()
        };
        assert!(config.validate().is_err());

        // Both cert and key should pass
        let config = TikvConfig {
            cert_path: Some("/path/to/cert".to_string()),
            key_path: Some("/path/to/key".to_string()),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_timeouts() {
        let config = TikvConfig::default();
        assert!(config.validate().is_ok());

        // Test that zero operation_timeout fails
        let config = TikvConfig {
            operation_timeout: Duration::from_secs(0),
            ..Default::default()
        };
        assert!(config.validate().is_err());

        // Test that zero transaction_timeout fails
        let config = TikvConfig {
            transaction_timeout: Duration::from_secs(0),
            ..Default::default()
        };
        assert!(config.validate().is_err());

        // Test that non-positive lock_ttl fails
        let config = TikvConfig {
            default_lock_ttl_secs: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());

        // Test that zero max_retries fails
        let config = TikvConfig {
            max_retries: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }
}
