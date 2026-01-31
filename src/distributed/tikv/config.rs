// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! TiKV configuration for distributed coordination.

use std::env;
use std::time::Duration;

use super::error::TikvError;

// Constants from parent module
pub const KEY_PREFIX: &str = "/roboflow/v1/";
pub const DEFAULT_PD_ENDPOINTS: &str = "127.0.0.1:2379";
pub const DEFAULT_CONNECTION_TIMEOUT_SECS: u64 = 10;

/// Configuration for TiKV cluster connection.
#[derive(Debug, Clone)]
pub struct TikvConfig {
    /// PD (Placement Driver) endpoints for cluster discovery.
    /// Multiple endpoints can be comma-separated for high availability.
    pub pd_endpoints: Vec<String>,

    /// Connection timeout duration.
    pub connection_timeout: Duration,

    /// Key prefix for all operations (defaults to `/roboflow/v1/`).
    pub key_prefix: String,

    /// CA certificate path for TLS (optional).
    pub ca_path: Option<String>,

    /// Client certificate path for TLS (optional).
    pub cert_path: Option<String>,

    /// Client key path for TLS (optional).
    pub key_path: Option<String>,
}

impl Default for TikvConfig {
    fn default() -> Self {
        Self {
            pd_endpoints: Self::parse_pd_endpoints(
                &env::var("TIKV_PD_ENDPOINTS").unwrap_or_else(|_| DEFAULT_PD_ENDPOINTS.to_string()),
            ),
            connection_timeout: Duration::from_secs(
                env::var("TIKV_TIMEOUT_SECS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(DEFAULT_CONNECTION_TIMEOUT_SECS),
            ),
            key_prefix: KEY_PREFIX.to_string(),
            ca_path: env::var("TIKV_CA_PATH").ok(),
            cert_path: env::var("TIKV_CERT_PATH").ok(),
            key_path: env::var("TIKV_KEY_PATH").ok(),
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
            "TiKV(pd_endpoints={:?}, timeout={:?}, tls={}, key_prefix={})",
            self.pd_endpoints, self.connection_timeout, tls, self.key_prefix
        )
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), TikvError> {
        if self.pd_endpoints.is_empty() {
            return Err(TikvError::InvalidConfig(
                "No PD endpoints specified".to_string(),
            ));
        }
        if !self.key_prefix.starts_with('/') {
            return Err(TikvError::InvalidConfig(
                "Key prefix must start with /".to_string(),
            ));
        }
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

        let mut config = TikvConfig::default();
        config.pd_endpoints = vec![];
        assert!(config.validate().is_err());
    }
}
