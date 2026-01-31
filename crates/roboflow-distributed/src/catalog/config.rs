// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! TiKV configuration for catalog connection.

use std::env;
use std::time::Duration;

// Default constants - these should match tikv::config
const DEFAULT_PD_ENDPOINTS: &str = "127.0.0.1:2379";
const DEFAULT_CONNECTION_TIMEOUT_SECS: u64 = 5;

/// Configuration for TiKV catalog connection.
#[derive(Debug, Clone)]
pub struct TiKVConfig {
    /// PD (Placement Driver) endpoints for cluster discovery.
    /// Multiple endpoints can be comma-separated for high availability.
    pub pd_endpoints: Vec<String>,

    /// Connection timeout duration.
    pub connection_timeout: Duration,

    /// CA certificate path for TLS (optional).
    pub ca_path: Option<String>,

    /// Client certificate path for TLS (optional).
    pub cert_path: Option<String>,

    /// Client key path for TLS (optional).
    pub key_path: Option<String>,
}

impl Default for TiKVConfig {
    fn default() -> Self {
        Self {
            pd_endpoints: Self::parse_pd_endpoints(
                &env::var("ROBOFLOW_PD_ENDPOINTS")
                    .unwrap_or_else(|_| DEFAULT_PD_ENDPOINTS.to_string()),
            ),
            connection_timeout: Duration::from_secs(
                env::var("ROBOFLOW_TIKV_TIMEOUT_SECS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(DEFAULT_CONNECTION_TIMEOUT_SECS),
            ),
            ca_path: env::var("ROBOFLOW_TIKV_CA_PATH").ok(),
            cert_path: env::var("ROBOFLOW_TIKV_CERT_PATH").ok(),
            key_path: env::var("ROBOFLOW_TIKV_KEY_PATH").ok(),
        }
    }
}

impl TiKVConfig {
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
            "TiKV(pd_endpoints={:?}, timeout={:?}, tls={})",
            self.pd_endpoints, self.connection_timeout, tls
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_pd_endpoints_single() {
        let endpoints = TiKVConfig::parse_pd_endpoints("127.0.0.1:2379");
        assert_eq!(endpoints, vec!["127.0.0.1:2379"]);
    }

    #[test]
    fn test_parse_pd_endpoints_multiple() {
        let endpoints =
            TiKVConfig::parse_pd_endpoints("127.0.0.1:2379,127.0.0.1:2380,127.0.0.1:2381");
        assert_eq!(
            endpoints,
            vec!["127.0.0.1:2379", "127.0.0.1:2380", "127.0.0.1:2381"]
        );
    }

    #[test]
    fn test_with_pd_endpoints() {
        let config = TiKVConfig::with_pd_endpoints("192.168.1.1:2379,192.168.1.2:2379");
        assert_eq!(
            config.pd_endpoints,
            vec!["192.168.1.1:2379", "192.168.1.2:2379"]
        );
    }

    #[test]
    fn test_is_tls_enabled() {
        let mut config = TiKVConfig::default();
        assert!(!config.is_tls_enabled());

        config.ca_path = Some("/path/to/ca".to_string());
        assert!(config.is_tls_enabled());
    }
}
