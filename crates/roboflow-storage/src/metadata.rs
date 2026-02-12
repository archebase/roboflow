// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Storage metadata types.
//!
//! This module provides metadata structures for storage objects
//! and configuration for streaming operations.

use std::time::SystemTime;

/// Metadata about a storage object.
///
/// This structure provides information about objects stored in any backend,
/// including size, modification time, and optional content type.
#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    /// The full path or key of the object.
    pub path: String,

    /// Size of the object in bytes.
    pub size: u64,

    /// Last modification time, if available.
    pub last_modified: Option<SystemTime>,

    /// Content type (MIME type), if available.
    pub content_type: Option<String>,

    /// Whether this object represents a directory (for local filesystem).
    pub is_dir: bool,
}

impl ObjectMetadata {
    /// Create new object metadata.
    pub fn new(path: impl Into<String>, size: u64) -> Self {
        Self {
            path: path.into(),
            size,
            last_modified: None,
            content_type: None,
            is_dir: false,
        }
    }

    /// Create metadata for a directory.
    pub fn dir(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            size: 0,
            last_modified: None,
            content_type: None,
            is_dir: true,
        }
    }

    /// Set the last modified time.
    pub fn with_last_modified(mut self, time: SystemTime) -> Self {
        self.last_modified = Some(time);
        self
    }

    /// Set the content type.
    pub fn with_content_type(mut self, ctype: impl Into<String>) -> Self {
        self.content_type = Some(ctype.into());
        self
    }
}

/// Configuration for streaming readers.
///
/// Controls chunk size for streaming storage operations.
///
/// # Note
///
/// The `prefetch_count` field is reserved for future use. Background prefetch
/// is not yet implemented - streaming readers fetch data synchronously on demand.
#[derive(Debug, Clone)]
pub struct StreamingConfig {
    /// Size of each chunk to fetch (default: 16MB)
    pub chunk_size: usize,

    /// Number of chunks to prefetch ahead (reserved for future use, not yet implemented)
    pub prefetch_count: usize,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            chunk_size: 16 * 1024 * 1024, // 16MB
            prefetch_count: 2,
        }
    }
}

impl StreamingConfig {
    /// Create a new streaming config with custom chunk size.
    pub fn with_chunk_size(mut self, size: usize) -> Self {
        self.chunk_size = size;
        self
    }

    /// Create a new streaming config with custom prefetch count.
    ///
    /// # Note
    ///
    /// Prefetch is a deferred optimization that would require background
    /// task coordination with the streaming reader. This setting is
    /// reserved for future use.
    pub fn with_prefetch_count(self, _count: usize) -> Self {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_metadata_new() {
        let meta = ObjectMetadata::new("test.txt", 1024);
        assert_eq!(meta.path, "test.txt");
        assert_eq!(meta.size, 1024);
        assert!(!meta.is_dir);
        assert!(meta.last_modified.is_none());
        assert!(meta.content_type.is_none());
    }

    #[test]
    fn test_object_metadata_dir() {
        let meta = ObjectMetadata::dir("/tmp/test");
        assert_eq!(meta.path, "/tmp/test");
        assert!(meta.is_dir);
        assert_eq!(meta.size, 0);
    }

    #[test]
    fn test_object_metadata_builder() {
        let meta = ObjectMetadata::new("test.txt", 1024)
            .with_content_type("text/plain")
            .with_last_modified(SystemTime::now());

        assert_eq!(meta.path, "test.txt");
        assert_eq!(meta.content_type.as_deref(), Some("text/plain"));
        assert!(meta.last_modified.is_some());
    }
}
