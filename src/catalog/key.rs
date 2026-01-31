// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Key encoding and decoding for TiKV storage.
//!
//! Keys are encoded in a hierarchical namespace format:
//! - `roboflow/episode/{episode_id}` - Episode metadata
//! - `roboflow/segment/{segment_id}` - Segment metadata
//! - `roboflow/upload/{episode_id}` - Upload status
//! - `roboflow/index/segment/{config_hash}/{segment_id}` - Config hash index

use std::str;

use serde::{Deserialize, Serialize};

/// Key namespace prefixes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyNamespace {
    /// Episode metadata
    Episode = 0x01,
    /// Segment metadata
    Segment = 0x02,
    /// Upload status tracking
    Upload = 0x03,
    /// Index entries
    Index = 0x04,
}

impl KeyNamespace {
    /// Get the prefix string for this namespace.
    pub const fn prefix(self) -> &'static str {
        match self {
            Self::Episode => "ep",
            Self::Segment => "seg",
            Self::Upload => "up",
            Self::Index => "idx",
        }
    }

    /// Get the byte prefix for this namespace.
    pub const fn byte_prefix(self) -> u8 {
        self as u8
    }
}

/// Key builder for constructing TiKV keys.
#[derive(Debug, Clone)]
pub struct KeyBuilder {
    namespace: KeyNamespace,
    parts: Vec<String>,
}

impl KeyBuilder {
    /// Create a new key builder.
    pub fn new(namespace: KeyNamespace) -> Self {
        Self {
            namespace,
            parts: Vec::new(),
        }
    }

    /// Add a part to the key.
    pub fn push(mut self, part: impl Into<String>) -> Self {
        self.parts.push(part.into());
        self
    }

    /// Build the key as a byte vector.
    pub fn build(self) -> Vec<u8> {
        let mut key = String::from("roboflow/");
        key.push_str(self.namespace.prefix());
        key.push('/');

        for (i, part) in self.parts.iter().enumerate() {
            if i > 0 {
                key.push('/');
            }
            key.push_str(part);
        }

        key.into_bytes()
    }

    /// Build the key as a string for display.
    pub fn as_str(&self) -> String {
        let mut key = String::from("roboflow/");
        key.push_str(self.namespace.prefix());
        key.push('/');

        for (i, part) in self.parts.iter().enumerate() {
            if i > 0 {
                key.push('/');
            }
            key.push_str(part);
        }

        key
    }
}

/// Key builder for episode keys.
pub struct EpisodeKey;

impl EpisodeKey {
    /// Create a key for episode metadata.
    pub fn metadata(episode_id: &str) -> Vec<u8> {
        KeyBuilder::new(KeyNamespace::Episode)
            .push(episode_id)
            .push("meta")
            .build()
    }

    /// Create a prefix for scanning all episodes.
    pub fn prefix() -> Vec<u8> {
        format!("roboflow/{}/", KeyNamespace::Episode.prefix()).into_bytes()
    }
}

/// Key builder for segment keys.
pub struct SegmentKey;

impl SegmentKey {
    /// Create a key for segment metadata.
    pub fn metadata(segment_id: &str) -> Vec<u8> {
        KeyBuilder::new(KeyNamespace::Segment)
            .push(segment_id)
            .push("meta")
            .build()
    }

    /// Create a key for config hash index.
    pub fn config_index(config_hash: &str, segment_id: &str) -> Vec<u8> {
        KeyBuilder::new(KeyNamespace::Index)
            .push("config")
            .push(config_hash)
            .push(segment_id)
            .build()
    }

    /// Create a prefix for scanning all segments.
    pub fn prefix() -> Vec<u8> {
        format!("roboflow/{}/", KeyNamespace::Segment.prefix()).into_bytes()
    }
}

/// Key builder for upload status keys.
pub struct UploadKey;

impl UploadKey {
    /// Create a key for upload status.
    pub fn status(episode_id: &str) -> Vec<u8> {
        KeyBuilder::new(KeyNamespace::Upload)
            .push(episode_id)
            .push("status")
            .build()
    }

    /// Create a prefix for scanning all uploads.
    pub fn prefix() -> Vec<u8> {
        format!("roboflow/{}/", KeyNamespace::Upload.prefix()).into_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_namespace_prefix() {
        assert_eq!(KeyNamespace::Episode.prefix(), "ep");
        assert_eq!(KeyNamespace::Segment.prefix(), "seg");
        assert_eq!(KeyNamespace::Upload.prefix(), "up");
        assert_eq!(KeyNamespace::Index.prefix(), "idx");
    }

    #[test]
    fn test_episode_key_metadata() {
        let key = EpisodeKey::metadata("episode-123");
        let key_str = String::from_utf8(key).unwrap();
        assert_eq!(key_str, "roboflow/ep/episode-123/meta");
    }

    #[test]
    fn test_segment_key_metadata() {
        let key = SegmentKey::metadata("segment-456");
        let key_str = String::from_utf8(key).unwrap();
        assert_eq!(key_str, "roboflow/seg/segment-456/meta");
    }

    #[test]
    fn test_segment_key_config_index() {
        let key = SegmentKey::config_index("hash-abc", "segment-456");
        let key_str = String::from_utf8(key).unwrap();
        assert_eq!(key_str, "roboflow/idx/config/hash-abc/segment-456");
    }

    #[test]
    fn test_upload_key_status() {
        let key = UploadKey::status("episode-123");
        let key_str = String::from_utf8(key).unwrap();
        assert_eq!(key_str, "roboflow/up/episode-123/status");
    }
}
