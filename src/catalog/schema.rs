// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Schema types for catalog metadata.

use serde::{Deserialize, Serialize};

/// Episode metadata stored in TiKV.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpisodeMetadata {
    /// Unique episode identifier.
    pub episode_id: String,

    /// Segment ID if this episode is part of a segment.
    pub segment_id: Option<String>,

    /// Dataset name.
    pub dataset_name: String,

    /// Number of frames in this episode.
    pub frame_count: u64,

    /// Total size in bytes.
    pub total_bytes: u64,

    /// Start timestamp (nanoseconds since epoch).
    pub start_ns: i64,

    /// End timestamp (nanoseconds since epoch).
    pub end_ns: i64,

    /// Creation timestamp.
    pub created_at: i64,

    /// Optional labels.
    #[serde(default)]
    pub labels: Vec<String>,

    /// Update version for optimistic locking.
    #[serde(default)]
    pub update_version: u64,
}

impl EpisodeMetadata {
    /// Create new episode metadata.
    pub fn new(
        episode_id: impl Into<String>,
        dataset_name: impl Into<String>,
        frame_count: u64,
        total_bytes: u64,
        start_ns: i64,
        end_ns: i64,
    ) -> Self {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        Self {
            episode_id: episode_id.into(),
            segment_id: None,
            dataset_name: dataset_name.into(),
            frame_count,
            total_bytes,
            start_ns,
            end_ns,
            created_at: now,
            labels: Vec::new(),
            update_version: 0,
        }
    }

    /// Get the duration in nanoseconds.
    pub fn duration_ns(&self) -> i64 {
        self.end_ns - self.start_ns
    }

    /// Get the duration in seconds.
    pub fn duration_secs(&self) -> f64 {
        self.duration_ns() as f64 / 1_000_000_000.0
    }

    /// Increment the update version.
    pub fn increment_version(&mut self) {
        self.update_version = self.update_version.wrapping_add(1);
    }

    /// Encode to bytes.
    pub fn encode(&self) -> super::Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| {
            crate::RoboflowError::other(format!("Failed to encode episode metadata: {}", e))
        })
    }

    /// Decode from bytes.
    pub fn decode(data: &[u8]) -> super::Result<Self> {
        bincode::deserialize(data).map_err(|e| {
            crate::RoboflowError::other(format!("Failed to decode episode metadata: {}", e))
        })
    }
}

/// Segment metadata for grouping episodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMetaData {
    /// Unique segment identifier.
    pub segment_id: String,

    /// Dataset name.
    pub dataset_name: String,

    /// Configuration hash for this segment.
    pub config_hash: String,

    /// Storage prefix for this segment.
    pub storage_prefix: String,

    /// Episode IDs in this segment.
    #[serde(default)]
    pub episode_ids: Vec<String>,

    /// Total frames in this segment.
    #[serde(default)]
    pub total_frames: u64,

    /// Total bytes in this segment.
    #[serde(default)]
    pub total_bytes: u64,

    /// Start timestamp (nanoseconds since epoch).
    pub start_ns: i64,

    /// End timestamp (nanoseconds since epoch).
    pub end_ns: i64,

    /// Creation timestamp.
    pub created_at: i64,

    /// Update version for optimistic locking.
    #[serde(default)]
    pub update_version: u64,
}

impl SegmentMetaData {
    /// Create new segment metadata.
    pub fn new(
        segment_id: impl Into<String>,
        dataset_name: impl Into<String>,
        config_hash: impl Into<String>,
        storage_prefix: impl Into<String>,
    ) -> Self {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        Self {
            segment_id: segment_id.into(),
            dataset_name: dataset_name.into(),
            config_hash: config_hash.into(),
            storage_prefix: storage_prefix.into(),
            episode_ids: Vec::new(),
            total_frames: 0,
            total_bytes: 0,
            start_ns: i64::MAX,
            end_ns: i64::MIN,
            created_at: now,
            update_version: 0,
        }
    }

    /// Add an episode to this segment.
    pub fn add_episode(&mut self, episode_id: impl Into<String>, start_ns: i64, end_ns: i64) {
        let episode_id = episode_id.into();
        self.episode_ids.push(episode_id.clone());
        self.start_ns = self.start_ns.min(start_ns);
        self.end_ns = self.end_ns.max(end_ns);
    }

    /// Check if an episode is in this segment.
    pub fn contains_episode(&self, episode_id: &str) -> bool {
        self.episode_ids.iter().any(|e| e == episode_id)
    }

    /// Remove an episode from this segment.
    pub fn remove_episode(&mut self, episode_id: &str) -> bool {
        if let Some(pos) = self.episode_ids.iter().position(|e| e == episode_id) {
            self.episode_ids.remove(pos);
            true
        } else {
            false
        }
    }

    /// Get the number of episodes in this segment.
    pub fn episode_count(&self) -> usize {
        self.episode_ids.len()
    }

    /// Check if this segment has capacity for more episodes.
    pub fn has_capacity(&self, max_episodes: usize) -> bool {
        self.episode_ids.len() < max_episodes
    }

    /// Get the version key for optimistic locking.
    pub fn version_key(&self) -> (u64, String) {
        (self.update_version, self.segment_id.clone())
    }

    /// Increment the update version.
    pub fn increment_version(&mut self) {
        self.update_version = self.update_version.wrapping_add(1);
    }

    /// Encode to bytes.
    pub fn encode(&self) -> super::Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| {
            crate::RoboflowError::other(format!("Failed to encode segment metadata: {}", e))
        })
    }

    /// Decode from bytes.
    pub fn decode(data: &[u8]) -> super::Result<Self> {
        bincode::deserialize(data).map_err(|e| {
            crate::RoboflowError::other(format!("Failed to decode segment metadata: {}", e))
        })
    }
}

/// Upload status tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadStatus {
    /// Episode ID being uploaded.
    pub episode_id: String,

    /// Total number of files to upload.
    pub total_files: u32,

    /// Number of files successfully uploaded.
    pub uploaded_files: u32,

    /// Number of failed uploads.
    pub failed_files: u32,

    /// Total bytes uploaded.
    pub uploaded_bytes: u64,

    /// Current status.
    pub status: UploadState,

    /// Last update timestamp.
    pub updated_at: i64,

    /// Error message if failed.
    pub error: Option<String>,
}

impl UploadStatus {
    /// Create new upload status.
    pub fn new(episode_id: impl Into<String>, total_files: u32) -> Self {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        Self {
            episode_id: episode_id.into(),
            total_files,
            uploaded_files: 0,
            failed_files: 0,
            uploaded_bytes: 0,
            status: UploadState::Pending,
            updated_at: now,
            error: None,
        }
    }

    /// Check if upload is complete.
    pub fn is_complete(&self) -> bool {
        self.uploaded_files + self.failed_files >= self.total_files
    }

    /// Check if upload is successful.
    pub fn is_successful(&self) -> bool {
        self.is_complete() && self.failed_files == 0
    }

    /// Get progress as a percentage.
    pub fn progress(&self) -> f64 {
        if self.total_files == 0 {
            return 100.0;
        }
        let completed = self.uploaded_files + self.failed_files;
        (completed as f64 / self.total_files as f64) * 100.0
    }

    /// Encode to bytes.
    pub fn encode(&self) -> super::Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| {
            crate::RoboflowError::other(format!("Failed to encode upload status: {}", e))
        })
    }

    /// Decode from bytes.
    pub fn decode(data: &[u8]) -> super::Result<Self> {
        bincode::deserialize(data).map_err(|e| {
            crate::RoboflowError::other(format!("Failed to decode upload status: {}", e))
        })
    }
}

/// Upload state enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UploadState {
    /// Upload is pending.
    Pending,
    /// Upload is in progress.
    InProgress,
    /// Upload completed successfully.
    Complete,
    /// Upload failed.
    Failed,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_episode_metadata_new() {
        let episode = EpisodeMetadata::new("ep-1", "test-dataset", 100, 1024 * 1024, 0, 1_000_000_000);
        assert_eq!(episode.episode_id, "ep-1");
        assert_eq!(episode.dataset_name, "test-dataset");
        assert_eq!(episode.frame_count, 100);
        assert_eq!(episode.total_bytes, 1024 * 1024);
        assert_eq!(episode.duration_ns(), 1_000_000_000);
    }

    #[test]
    fn test_episode_metadata_increment_version() {
        let mut episode = EpisodeMetadata::new("ep-1", "test", 100, 1024, 0, 1000);
        assert_eq!(episode.update_version, 0);
        episode.increment_version();
        assert_eq!(episode.update_version, 1);
    }

    #[test]
    fn test_segment_metadata_new() {
        let segment = SegmentMetaData::new("seg-1", "test", "hash-abc", "s3://bucket");
        assert_eq!(segment.segment_id, "seg-1");
        assert_eq!(segment.config_hash, "hash-abc");
        assert_eq!(segment.episode_count(), 0);
        assert!(segment.has_capacity(100));
    }

    #[test]
    fn test_segment_add_episode() {
        let mut segment = SegmentMetaData::new("seg-1", "test", "hash", "s3://");
        segment.add_episode("ep-1", 0, 1000);
        assert_eq!(segment.episode_count(), 1);
        assert!(segment.contains_episode("ep-1"));
        assert_eq!(segment.start_ns, 0);
        assert_eq!(segment.end_ns, 1000);
    }

    #[test]
    fn test_upload_status_new() {
        let status = UploadStatus::new("ep-1", 10);
        assert_eq!(status.episode_id, "ep-1");
        assert_eq!(status.total_files, 10);
        assert_eq!(status.progress(), 0.0);
        assert!(!status.is_complete());
    }

    #[test]
    fn test_upload_status_complete() {
        let mut status = UploadStatus::new("ep-1", 10);
        status.uploaded_files = 10;
        assert!(status.is_complete());
        assert!(status.is_successful());
        assert_eq!(status.progress(), 100.0);
    }
}
