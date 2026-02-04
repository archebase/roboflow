// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Key builders for TiKV storage.
//!
//! Provides type-safe key construction for all distributed coordination keys.
//!
//! ## Key Format
//!
//! All keys follow the pattern: `/roboflow/v1/{namespace}/{key}`
//!
//! ## Namespaces
//!
//! - `jobs`: `/roboflow/v1/jobs/{file_hash}` - Job records
//! - `locks`: `/roboflow/v1/locks/{resource}` - Distributed locks
//! - `state`: `/roboflow/v1/state/{file_hash}` - Checkpoint state
//! - `heartbeat`: `/roboflow/v1/heartbeat/{pod_id}` - Worker heartbeats
//! - `configs`: `/roboflow/v1/configs/{config_hash}` - Dataset configurations
//! - `system`: `/roboflow/v1/system/scanner_lock` - Scanner leadership

use super::config::KEY_PREFIX;

/// Key builder for constructing TiKV keys.
pub struct KeyBuilder {
    parts: Vec<String>,
}

impl KeyBuilder {
    /// Create a new key builder.
    pub fn new() -> Self {
        Self { parts: Vec::new() }
    }

    /// Add a part to the key.
    pub fn push(mut self, part: impl Into<String>) -> Self {
        self.parts.push(part.into());
        self
    }

    /// Build the key as a byte vector using the default key prefix.
    pub fn build(self) -> Vec<u8> {
        self.build_with_prefix(KEY_PREFIX.trim_end_matches('/'))
    }

    /// Build the key as a byte vector with a custom prefix.
    pub fn build_with_prefix(self, prefix: &str) -> Vec<u8> {
        let mut key = prefix.to_string();
        for part in &self.parts {
            key.push('/');
            key.push_str(part);
        }
        key.into_bytes()
    }

    /// Build the key as a string for display.
    pub fn as_str(&self) -> String {
        let mut key = KEY_PREFIX.trim_end_matches('/').to_string();
        for (i, part) in self.parts.iter().enumerate() {
            if i > 0 || !key.ends_with('/') {
                key.push('/');
            }
            key.push_str(part);
        }
        key
    }
}

impl Default for KeyBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Job record keys.
pub struct JobKeys;

impl JobKeys {
    /// Create a key for a job record.
    pub fn record(file_hash: &str) -> Vec<u8> {
        KeyBuilder::new().push("jobs").push(file_hash).build()
    }

    /// Create a prefix for scanning all jobs.
    pub fn prefix() -> Vec<u8> {
        format!("{KEY_PREFIX}jobs/").into_bytes()
    }
}

/// Lock keys.
pub struct LockKeys;

impl LockKeys {
    /// Create a key for a distributed lock.
    pub fn lock(resource: &str) -> Vec<u8> {
        KeyBuilder::new().push("locks").push(resource).build()
    }

    /// Create a prefix for scanning all locks.
    pub fn prefix() -> Vec<u8> {
        format!("{KEY_PREFIX}locks/").into_bytes()
    }
}

/// Checkpoint state keys.
pub struct StateKeys;

impl StateKeys {
    /// Create a key for checkpoint state.
    pub fn checkpoint(file_hash: &str) -> Vec<u8> {
        KeyBuilder::new().push("state").push(file_hash).build()
    }

    /// Create a prefix for scanning all states.
    pub fn prefix() -> Vec<u8> {
        format!("{KEY_PREFIX}state/").into_bytes()
    }
}

/// Heartbeat keys.
pub struct HeartbeatKeys;

impl HeartbeatKeys {
    /// Create a key for a worker heartbeat.
    pub fn heartbeat(pod_id: &str) -> Vec<u8> {
        KeyBuilder::new().push("heartbeat").push(pod_id).build()
    }

    /// Create a prefix for scanning all heartbeats.
    pub fn prefix() -> Vec<u8> {
        format!("{KEY_PREFIX}heartbeat/").into_bytes()
    }
}

/// Config keys.
pub struct ConfigKeys;

impl ConfigKeys {
    /// Create a key for a configuration record.
    pub fn config(config_hash: &str) -> Vec<u8> {
        KeyBuilder::new().push("configs").push(config_hash).build()
    }

    /// Create a prefix for scanning all configs.
    pub fn prefix() -> Vec<u8> {
        format!("{KEY_PREFIX}configs/").into_bytes()
    }
}

/// System keys.
pub struct SystemKeys;

impl SystemKeys {
    /// Create a key for the scanner lock.
    pub fn scanner_lock() -> Vec<u8> {
        KeyBuilder::new()
            .push("system")
            .push("scanner_lock")
            .build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_key() {
        let key = JobKeys::record("abc123");
        let key_str = String::from_utf8(key).expect("Job keys should be valid UTF-8");
        assert_eq!(key_str, "/roboflow/v1/jobs/abc123");
    }

    #[test]
    fn test_lock_key() {
        let key = LockKeys::lock("resource_1");
        let key_str = String::from_utf8(key).expect("Lock keys should be valid UTF-8");
        assert_eq!(key_str, "/roboflow/v1/locks/resource_1");
    }

    #[test]
    fn test_state_key() {
        let key = StateKeys::checkpoint("xyz789");
        let key_str = String::from_utf8(key).expect("State keys should be valid UTF-8");
        assert_eq!(key_str, "/roboflow/v1/state/xyz789");
    }

    #[test]
    fn test_heartbeat_key() {
        let key = HeartbeatKeys::heartbeat("pod-123");
        let key_str = String::from_utf8(key).expect("Heartbeat keys should be valid UTF-8");
        assert_eq!(key_str, "/roboflow/v1/heartbeat/pod-123");
    }

    #[test]
    fn test_scanner_lock_key() {
        let key = SystemKeys::scanner_lock();
        let key_str = String::from_utf8(key).expect("System keys should be valid UTF-8");
        assert_eq!(key_str, "/roboflow/v1/system/scanner_lock");
    }
}
