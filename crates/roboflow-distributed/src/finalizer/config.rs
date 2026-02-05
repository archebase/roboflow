// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Finalizer configuration.

use std::time::Duration;
use tracing::warn;

/// Default poll interval for checking completed batches (seconds).
pub const DEFAULT_POLL_INTERVAL_SECS: u64 = 30;

/// Default merge operation timeout (seconds).
pub const DEFAULT_MERGE_TIMEOUT_SECS: u64 = 600;

/// Finalizer configuration.
#[derive(Debug, Clone)]
pub struct FinalizerConfig {
    /// Poll interval for checking completed batches.
    pub poll_interval: Duration,

    /// Merge operation timeout.
    pub merge_timeout: Duration,
}

impl Default for FinalizerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(DEFAULT_POLL_INTERVAL_SECS),
            merge_timeout: Duration::from_secs(DEFAULT_MERGE_TIMEOUT_SECS),
        }
    }
}

impl FinalizerConfig {
    /// Create a new finalizer configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create from environment variables.
    ///
    /// - `FINALIZER_POLL_INTERVAL_SECS`: Poll interval (default: 30)
    /// - `FINALIZER_MERGE_TIMEOUT_SECS`: Merge timeout (default: 600)
    pub fn from_env() -> Result<Self, String> {
        let poll_interval = match std::env::var("FINALIZER_POLL_INTERVAL_SECS") {
            Ok(ref s) => match s.parse::<u64>() {
                Ok(val) => val,
                Err(_) => {
                    warn!(
                        env_var = "FINALIZER_POLL_INTERVAL_SECS",
                        provided = s,
                        default = DEFAULT_POLL_INTERVAL_SECS,
                        "Invalid value for FINALIZER_POLL_INTERVAL_SECS, using default"
                    );
                    DEFAULT_POLL_INTERVAL_SECS
                }
            },
            Err(_) => DEFAULT_POLL_INTERVAL_SECS,
        };

        let merge_timeout = match std::env::var("FINALIZER_MERGE_TIMEOUT_SECS") {
            Ok(ref s) => match s.parse::<u64>() {
                Ok(val) => val,
                Err(_) => {
                    warn!(
                        env_var = "FINALIZER_MERGE_TIMEOUT_SECS",
                        provided = s,
                        default = DEFAULT_MERGE_TIMEOUT_SECS,
                        "Invalid value for FINALIZER_MERGE_TIMEOUT_SECS, using default"
                    );
                    DEFAULT_MERGE_TIMEOUT_SECS
                }
            },
            Err(_) => DEFAULT_MERGE_TIMEOUT_SECS,
        };

        Ok(Self {
            poll_interval: Duration::from_secs(poll_interval),
            merge_timeout: Duration::from_secs(merge_timeout),
        })
    }
}
