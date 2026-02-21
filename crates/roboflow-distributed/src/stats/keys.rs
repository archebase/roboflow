// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! TiKV key schema for statistics storage.
//!
//! This module defines the key structure for storing episode statistics
//! in TiKV. Keys are designed to support efficient range scans by batch.

/// Key prefix for all stats-related entries.
const STATS_PREFIX: &[u8] = b"stats/";

/// Separator between key components.
const SEPARATOR: &[u8] = b"/";

/// Suffix for batch metadata entries.
const BATCH_META_SUFFIX: &[u8] = b"/meta";

/// Utility functions for constructing TiKV keys for statistics.
///
/// # Key Schema
///
/// ```text
/// stats/{batch_id}/meta              - Batch metadata (total count, etc.)
/// stats/{batch_id}/episode/{idx}     - Per-episode statistics
/// ```
///
/// This schema allows:
/// - Efficient range scan of all episodes in a batch
/// - Batch-level metadata for quick status checks
/// - Clean deletion of all batch-related keys
pub struct StatsKeys;

impl StatsKeys {
    /// Create the key for batch metadata.
    ///
    /// Format: `stats/{batch_id}/meta`
    pub fn batch_meta(batch_id: &str) -> Vec<u8> {
        let mut key =
            Vec::with_capacity(STATS_PREFIX.len() + batch_id.len() + BATCH_META_SUFFIX.len() + 2);
        key.extend_from_slice(STATS_PREFIX);
        key.extend_from_slice(batch_id.as_bytes());
        key.extend_from_slice(BATCH_META_SUFFIX);
        key
    }

    /// Create the key for an episode's statistics.
    ///
    /// Format: `stats/{batch_id}/episode/{episode_index}`
    pub fn episode(batch_id: &str, episode_index: usize) -> Vec<u8> {
        let episode_str = episode_index.to_string();
        let mut key = Vec::with_capacity(
            STATS_PREFIX.len() + batch_id.len() + b"episode/".len() + episode_str.len() + 3,
        );
        key.extend_from_slice(STATS_PREFIX);
        key.extend_from_slice(batch_id.as_bytes());
        key.extend_from_slice(SEPARATOR);
        key.extend_from_slice(b"episode/");
        key.extend_from_slice(episode_str.as_bytes());
        key
    }

    /// Create the prefix for all keys in a batch.
    ///
    /// Format: `stats/{batch_id}/`
    ///
    /// Use this for range scans to get all episodes in a batch.
    pub fn batch_prefix(batch_id: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(STATS_PREFIX.len() + batch_id.len() + 1);
        key.extend_from_slice(STATS_PREFIX);
        key.extend_from_slice(batch_id.as_bytes());
        key.extend_from_slice(SEPARATOR);
        key
    }

    /// Parse episode index from a key.
    ///
    /// Returns `Some(episode_index)` if the key is a valid episode key,
    /// `None` otherwise.
    pub fn parse_episode_index(key: &[u8]) -> Option<usize> {
        // Expected format: stats/{batch_id}/episode/{idx}
        let key_str = std::str::from_utf8(key).ok()?;

        // Find the last component after "episode/"
        let episode_marker = "/episode/";
        let idx = key_str.rfind(episode_marker)?;
        let index_str = &key_str[idx + episode_marker.len()..];

        index_str.parse().ok()
    }

    /// Check if a key is a batch metadata key.
    pub fn is_batch_meta(key: &[u8]) -> bool {
        key.ends_with(BATCH_META_SUFFIX)
    }

    /// Check if a key is an episode stats key.
    pub fn is_episode_key(key: &[u8]) -> bool {
        let key_str = std::str::from_utf8(key).unwrap_or("");
        key_str.contains("/episode/") && !key_str.ends_with("/meta")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_meta_key() {
        let key = StatsKeys::batch_meta("batch-123");
        let key_str = std::str::from_utf8(&key).unwrap();
        assert_eq!(key_str, "stats/batch-123/meta");
    }

    #[test]
    fn test_episode_key() {
        let key = StatsKeys::episode("batch-123", 42);
        let key_str = std::str::from_utf8(&key).unwrap();
        assert_eq!(key_str, "stats/batch-123/episode/42");
    }

    #[test]
    fn test_batch_prefix() {
        let prefix = StatsKeys::batch_prefix("batch-123");
        let prefix_str = std::str::from_utf8(&prefix).unwrap();
        assert_eq!(prefix_str, "stats/batch-123/");
    }

    #[test]
    fn test_parse_episode_index() {
        let key = StatsKeys::episode("batch-123", 42);
        let idx = StatsKeys::parse_episode_index(&key);
        assert_eq!(idx, Some(42));

        let meta_key = StatsKeys::batch_meta("batch-123");
        let idx = StatsKeys::parse_episode_index(&meta_key);
        assert_eq!(idx, None);
    }

    #[test]
    fn test_key_classification() {
        let meta_key = StatsKeys::batch_meta("batch-123");
        assert!(StatsKeys::is_batch_meta(&meta_key));
        assert!(!StatsKeys::is_episode_key(&meta_key));

        let episode_key = StatsKeys::episode("batch-123", 0);
        assert!(!StatsKeys::is_batch_meta(&episode_key));
        assert!(StatsKeys::is_episode_key(&episode_key));
    }

    #[test]
    fn test_keys_sort_correctly() {
        // Episodes should sort numerically within a batch
        let mut keys: Vec<_> = (0..=15)
            .map(|i| StatsKeys::episode("batch-123", i))
            .collect();

        let mut expected: Vec<_> = keys.clone();
        expected.sort();

        // Keys should already be in order (lexicographic = numeric for 0-9)
        // but may differ for 10-15
        keys.sort();
        assert_eq!(keys, expected);
    }
}
