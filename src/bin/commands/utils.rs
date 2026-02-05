// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Shared utilities for CLI commands.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Compute a hash-based job ID from file key and size.
pub fn compute_file_hash(key: &str, size: u64) -> String {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    size.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_file_hash() {
        let hash1 = compute_file_hash("test-key", 1024);
        let hash2 = compute_file_hash("test-key", 1024);
        let hash3 = compute_file_hash("test-key", 2048);
        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
    }
}
