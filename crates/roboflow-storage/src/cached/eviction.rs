// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Eviction policy and selection logic for cached storage.
//!
//! Provides cache eviction strategies for managing local storage space
//! when writing to remote storage backends.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::SystemTime;

/// Eviction policy for cached files.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EvictionPolicy {
    /// Least Recently Used - evict files with oldest access time.
    #[default]
    Lru,
    /// Least Frequently Used - evict files with lowest access count.
    Lfu,
    /// First In First Out - evict oldest cached files.
    Fifo,
}

impl std::fmt::Display for EvictionPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvictionPolicy::Lru => write!(f, "LRU"),
            EvictionPolicy::Lfu => write!(f, "LFU"),
            EvictionPolicy::Fifo => write!(f, "FIFO"),
        }
    }
}

/// Metadata for a cache entry used in eviction decisions.
///
/// This is used by the `select_eviction_candidate` helper function.
// Public API for external cache management
#[derive(Debug)]
#[allow(dead_code)]
pub struct CacheEntryMeta {
    /// Relative path within cache.
    pub path: PathBuf,
    /// File size in bytes.
    pub size: u64,
    /// Last access time (for LRU).
    pub last_accessed: SystemTime,
    /// Creation time (for FIFO).
    pub created_at: SystemTime,
    /// Access count (for LFU).
    pub access_count: u64,
}

/// Select the best candidate for eviction based on the policy.
///
/// Returns `Some((path, size))` of the entry to evict, or `None` if no
/// suitable candidate exists (e.g., all entries have pending uploads).
// Public API for external cache management
#[allow(dead_code)]
pub fn select_eviction_candidate(
    entries: &[CacheEntryMeta],
    policy: EvictionPolicy,
) -> Option<(PathBuf, u64)> {
    if entries.is_empty() {
        return None;
    }

    match policy {
        EvictionPolicy::Lru => entries
            .iter()
            .min_by_key(|e| e.last_accessed)
            .map(|e| (e.path.clone(), e.size)),
        EvictionPolicy::Lfu => entries
            .iter()
            .min_by_key(|e| e.access_count)
            .map(|e| (e.path.clone(), e.size)),
        EvictionPolicy::Fifo => entries
            .iter()
            .min_by_key(|e| e.created_at)
            .map(|e| (e.path.clone(), e.size)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eviction_policy_default() {
        let policy = EvictionPolicy::default();
        assert_eq!(policy, EvictionPolicy::Lru);
    }

    #[test]
    fn test_eviction_policy_display() {
        assert_eq!(format!("{}", EvictionPolicy::Lru), "LRU");
        assert_eq!(format!("{}", EvictionPolicy::Lfu), "LFU");
        assert_eq!(format!("{}", EvictionPolicy::Fifo), "FIFO");
    }

    #[test]
    fn test_eviction_policy_equality() {
        assert_eq!(EvictionPolicy::Lru, EvictionPolicy::Lru);
        assert_ne!(EvictionPolicy::Lru, EvictionPolicy::Lfu);
        assert_ne!(EvictionPolicy::Lfu, EvictionPolicy::Fifo);
    }

    #[test]
    fn test_eviction_policy_clone() {
        let policy = EvictionPolicy::Lfu;
        let copied = policy; // Copy is implicit for Copy types
        assert_eq!(policy, copied);
    }

    #[test]
    fn test_eviction_policy_copy() {
        let policy = EvictionPolicy::Lru;
        let copied: EvictionPolicy = policy;
        assert_eq!(policy, copied);
    }

    #[test]
    fn test_select_eviction_candidate_empty() {
        let entries: Vec<CacheEntryMeta> = vec![];
        assert!(select_eviction_candidate(&entries, EvictionPolicy::Lru).is_none());
    }

    #[test]
    fn test_select_eviction_candidate_lru() {
        let now = SystemTime::now();
        let entries = vec![
            CacheEntryMeta {
                path: PathBuf::from("old"),
                size: 100,
                last_accessed: now - std::time::Duration::from_secs(3600),
                created_at: now,
                access_count: 5,
            },
            CacheEntryMeta {
                path: PathBuf::from("new"),
                size: 200,
                last_accessed: now,
                created_at: now,
                access_count: 1,
            },
        ];

        let result = select_eviction_candidate(&entries, EvictionPolicy::Lru);
        assert_eq!(result, Some((PathBuf::from("old"), 100)));
    }

    #[test]
    fn test_select_eviction_candidate_lfu() {
        let now = SystemTime::now();
        let entries = vec![
            CacheEntryMeta {
                path: PathBuf::from("rarely_used"),
                size: 100,
                last_accessed: now,
                created_at: now,
                access_count: 1,
            },
            CacheEntryMeta {
                path: PathBuf::from("frequently_used"),
                size: 200,
                last_accessed: now,
                created_at: now,
                access_count: 100,
            },
        ];

        let result = select_eviction_candidate(&entries, EvictionPolicy::Lfu);
        assert_eq!(result, Some((PathBuf::from("rarely_used"), 100)));
    }

    #[test]
    fn test_select_eviction_candidate_fifo() {
        let now = SystemTime::now();
        let entries = vec![
            CacheEntryMeta {
                path: PathBuf::from("oldest"),
                size: 100,
                last_accessed: now,
                created_at: now - std::time::Duration::from_secs(7200),
                access_count: 10,
            },
            CacheEntryMeta {
                path: PathBuf::from("newest"),
                size: 200,
                last_accessed: now,
                created_at: now,
                access_count: 1,
            },
        ];

        let result = select_eviction_candidate(&entries, EvictionPolicy::Fifo);
        assert_eq!(result, Some((PathBuf::from("oldest"), 100)));
    }
}
