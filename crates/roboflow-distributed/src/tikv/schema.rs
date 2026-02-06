// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Schema types for distributed coordination.
//!
//! Defines the data structures stored in TiKV for job tracking,
//! distributed locking, checkpointing, and heartbeats.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Dataset configuration stored in TiKV.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigRecord {
    /// Config hash (SHA-256 of content)
    pub hash: String,

    /// Serialized TOML configuration content
    pub content: String,

    /// Creation timestamp
    pub created_at: DateTime<Utc>,
}

impl ConfigRecord {
    /// Create a new config record from TOML content.
    pub fn new(content: String) -> Self {
        let hash = Self::compute_hash(&content);
        let now = Utc::now();
        Self {
            hash,
            content,
            created_at: now,
        }
    }

    /// Compute SHA-256 hash of config content.
    pub fn compute_hash(content: &str) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(content.as_bytes());
        format!("{:x}", hasher.finalize())
    }
}

/// Distributed lock record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LockRecord {
    /// Resource being locked.
    pub resource: String,

    /// Owner of the lock (pod ID).
    pub owner: String,

    /// Lock version for CAS operations.
    pub version: u64,

    /// Expiration timestamp.
    pub expires_at: DateTime<Utc>,

    /// When the lock was acquired.
    pub acquired_at: DateTime<Utc>,
}

impl LockRecord {
    /// Create a new lock record.
    pub fn new(resource: String, owner: String, ttl_seconds: i64) -> Self {
        let now = Utc::now();
        Self {
            resource,
            owner,
            version: 1,
            expires_at: now + chrono::Duration::seconds(ttl_seconds),
            acquired_at: now,
        }
    }

    /// Check if this lock is expired.
    pub fn is_expired(&self) -> bool {
        Utc::now() > self.expires_at
    }

    /// Check if this lock is expired with a grace period.
    ///
    /// The grace period helps avoid race conditions during the expiration boundary.
    /// A lock is considered "not expired" if it's within the grace period, even
    /// if the expiration time has technically passed.
    pub fn is_expired_with_grace(&self, grace_seconds: i64) -> bool {
        Utc::now() > (self.expires_at - chrono::Duration::seconds(grace_seconds))
    }

    /// Extend the lock TTL.
    pub fn extend(&mut self, ttl_seconds: i64) {
        self.expires_at = Utc::now() + chrono::Duration::seconds(ttl_seconds);
        self.version += 1;
    }

    /// Verify ownership of the lock.
    pub fn is_owned_by(&self, pod_id: &str) -> bool {
        self.owner == pod_id
    }

    /// Get the fencing token (version) for this lock.
    ///
    /// Fencing tokens are used to detect and prevent split-brain scenarios
    /// where two processes believe they own the same lock. Higher tokens
    /// always win.
    pub fn fencing_token(&self) -> u64 {
        self.version
    }
}

/// Worker heartbeat record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeartbeatRecord {
    /// Pod ID of the worker.
    pub pod_id: String,

    /// Last heartbeat timestamp.
    pub last_heartbeat: DateTime<Utc>,

    /// Worker status.
    pub status: WorkerStatus,

    /// Number of jobs currently processing.
    pub active_jobs: u32,

    /// Total jobs processed by this worker.
    pub total_processed: u64,

    /// Worker capabilities.
    pub capabilities: Vec<String>,

    /// Optional worker metadata.
    pub metadata: Option<serde_json::Value>,
}

impl HeartbeatRecord {
    /// Create a new heartbeat record.
    pub fn new(pod_id: String) -> Self {
        Self {
            pod_id,
            last_heartbeat: Utc::now(),
            status: WorkerStatus::Idle,
            active_jobs: 0,
            total_processed: 0,
            capabilities: Vec::new(),
            metadata: None,
        }
    }

    /// Update the heartbeat timestamp.
    pub fn beat(&mut self) {
        self.last_heartbeat = Utc::now();
    }

    /// Check if this heartbeat is stale (older than timeout seconds).
    pub fn is_stale(&self, timeout_seconds: i64) -> bool {
        let timeout = chrono::Duration::seconds(timeout_seconds);
        Utc::now().signed_duration_since(self.last_heartbeat) > timeout
    }

    /// Increment active job count.
    pub fn increment_active(&mut self) {
        self.active_jobs += 1;
        self.status = WorkerStatus::Busy;
    }

    /// Decrement active job count.
    pub fn decrement_active(&mut self) {
        if self.active_jobs > 0 {
            self.active_jobs -= 1;
        }
        if self.active_jobs == 0 {
            self.status = WorkerStatus::Idle;
        }
    }

    /// Increment total processed count.
    pub fn increment_processed(&mut self) {
        self.total_processed += 1;
    }
}

/// Worker status enum.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum WorkerStatus {
    /// Worker is idle and available for work.
    Idle,

    /// Worker is processing jobs.
    Busy,

    /// Worker is draining (shutting down).
    Draining,

    /// Worker is unhealthy.
    Unhealthy,
}

/// Checkpoint state for frame-level progress tracking with multipart resume.
///
/// This struct captures all state needed to resume processing from any point,
/// including mid-multipart uploads. Essential for Spot Instance tolerance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointState {
    /// Job ID being processed.
    pub job_id: String,

    /// Processing pod ID.
    pub pod_id: String,

    /// Position in source file (bytes).
    pub byte_offset: u64,

    /// Last successfully processed frame index.
    pub last_frame: u64,

    /// Current episode index.
    pub episode_idx: u64,

    /// Total frames in the file.
    pub total_frames: u64,

    /// Video upload states keyed by camera/topic name.
    pub video_uploads: Vec<VideoUploadState>,

    /// Parquet dataset upload state.
    pub parquet_upload: Option<ParquetUploadState>,

    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,

    /// Processing state version.
    pub version: u64,
}

/// State for a single video's multipart upload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VideoUploadState {
    /// Camera/topic identifier.
    pub camera_id: String,

    /// Multipart upload ID from S3/OSS.
    pub upload_id: String,

    /// Parts uploaded so far.
    pub parts: Vec<UploadedPart>,

    /// Offset of the last keyframe processed.
    pub last_keyframe_offset: u64,
}

/// A single uploaded part in a multipart upload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UploadedPart {
    /// Part number (1-indexed).
    pub part_num: u32,

    /// ETag returned by S3/OSS after upload.
    pub etag: String,

    /// Size of the part in bytes.
    pub size: u64,
}

/// State for parquet dataset multipart upload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParquetUploadState {
    /// Multipart upload ID from S3/OSS.
    pub upload_id: String,

    /// Parts uploaded so far.
    pub parts: Vec<UploadedPart>,

    /// Number of rows written to parquet so far.
    pub rows_written: u64,

    /// Optional serialized partial buffer state (for complex resume).
    /// Set to None for simpler "discard and re-process" behavior.
    pub buffer_state: Option<Vec<u8>>,
}

impl CheckpointState {
    /// Create a new checkpoint state.
    pub fn new(job_id: String, pod_id: String, total_frames: u64) -> Self {
        Self {
            job_id,
            pod_id,
            byte_offset: 0,
            last_frame: 0,
            episode_idx: 0,
            total_frames,
            video_uploads: Vec::new(),
            parquet_upload: None,
            updated_at: Utc::now(),
            version: 1,
        }
    }

    /// Create checkpoint with video upload tracking.
    pub fn with_videos(
        job_id: String,
        pod_id: String,
        total_frames: u64,
        camera_ids: Vec<String>,
    ) -> Self {
        let video_uploads = camera_ids
            .into_iter()
            .map(|camera_id| VideoUploadState {
                camera_id,
                upload_id: String::new(),
                parts: Vec::new(),
                last_keyframe_offset: 0,
            })
            .collect();

        Self {
            job_id,
            pod_id,
            byte_offset: 0,
            last_frame: 0,
            episode_idx: 0,
            total_frames,
            video_uploads,
            parquet_upload: None,
            updated_at: Utc::now(),
            version: 1,
        }
    }

    /// Update the checkpoint with progress.
    pub fn update(&mut self, frame: u64, byte_offset: u64) -> Result<(), String> {
        if frame > self.total_frames {
            return Err(format!(
                "Frame {} exceeds total frames {}",
                frame, self.total_frames
            ));
        }
        self.last_frame = frame;
        self.byte_offset = byte_offset;
        self.updated_at = Utc::now();
        self.version += 1;
        Ok(())
    }

    /// Update episode index.
    pub fn update_episode(&mut self, episode: u64) {
        self.episode_idx = episode;
        self.updated_at = Utc::now();
        self.version += 1;
    }

    /// Get or create video upload state for a camera.
    pub fn video_state_mut(&mut self, camera_id: &str) -> &mut VideoUploadState {
        if !self.video_uploads.iter().any(|v| v.camera_id == camera_id) {
            self.video_uploads.push(VideoUploadState {
                camera_id: camera_id.to_string(),
                upload_id: String::new(),
                parts: Vec::new(),
                last_keyframe_offset: 0,
            });
            self.version += 1;
        }
        self.video_uploads
            .iter_mut()
            .find(|v| v.camera_id == camera_id)
            .expect("camera_id just added")
    }

    /// Get video upload state for a camera (if exists).
    pub fn video_state(&self, camera_id: &str) -> Option<&VideoUploadState> {
        self.video_uploads.iter().find(|v| v.camera_id == camera_id)
    }

    /// Set parquet upload state.
    pub fn set_parquet_upload(&mut self, state: ParquetUploadState) {
        self.parquet_upload = Some(state);
        self.version += 1;
    }

    /// Calculate progress as a percentage.
    pub fn progress_percent(&self) -> f64 {
        if self.total_frames == 0 {
            0.0
        } else {
            (self.last_frame as f64 / self.total_frames as f64) * 100.0
        }
    }

    /// Check if processing is complete.
    pub fn is_complete(&self) -> bool {
        self.last_frame >= self.total_frames
    }
}

impl VideoUploadState {
    /// Create a new video upload state.
    pub fn new(camera_id: String) -> Self {
        Self {
            camera_id,
            upload_id: String::new(),
            parts: Vec::new(),
            last_keyframe_offset: 0,
        }
    }

    /// Check if upload has been initiated.
    pub fn is_active(&self) -> bool {
        !self.upload_id.is_empty()
    }

    /// Get the next part number to upload.
    pub fn next_part_number(&self) -> u32 {
        (self.parts.len() + 1) as u32
    }

    /// Add a completed part to the upload.
    pub fn add_part(&mut self, part_num: u32, etag: String, size: u64) {
        self.parts.push(UploadedPart {
            part_num,
            etag,
            size,
        });
    }

    /// Get total bytes uploaded so far.
    pub fn uploaded_bytes(&self) -> u64 {
        self.parts.iter().map(|p| p.size).sum()
    }
}

impl ParquetUploadState {
    /// Create a new parquet upload state.
    pub fn new(upload_id: String) -> Self {
        Self {
            upload_id,
            parts: Vec::new(),
            rows_written: 0,
            buffer_state: None,
        }
    }

    /// Get the next part number to upload.
    pub fn next_part_number(&self) -> u32 {
        (self.parts.len() + 1) as u32
    }

    /// Add a completed part to the upload.
    pub fn add_part(&mut self, part_num: u32, etag: String, size: u64) {
        self.parts.push(UploadedPart {
            part_num,
            etag,
            size,
        });
    }

    /// Get total bytes uploaded so far.
    pub fn uploaded_bytes(&self) -> u64 {
        self.parts.iter().map(|p| p.size).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_record() {
        let mut lock = LockRecord::new("resource".to_string(), "pod-1".to_string(), 60);
        assert_eq!(lock.version, 1);
        assert!(!lock.is_expired());
        assert!(lock.is_owned_by("pod-1"));
        assert!(!lock.is_owned_by("pod-2"));

        lock.extend(60);
        assert_eq!(lock.version, 2);
    }

    #[test]
    fn test_lock_grace_period() {
        let lock = LockRecord::new("resource".to_string(), "pod-1".to_string(), 10); // 10 second TTL
        // Lock should NOT be expired normally
        assert!(!lock.is_expired());
        // Lock should NOT be expired even with a small grace period
        assert!(!lock.is_expired_with_grace(5));
        assert_eq!(lock.fencing_token(), 1);
    }

    #[test]
    fn test_heartbeat_record() {
        let mut heartbeat = HeartbeatRecord::new("pod-1".to_string());
        assert_eq!(heartbeat.status, WorkerStatus::Idle);
        assert_eq!(heartbeat.active_jobs, 0);

        heartbeat.increment_active();
        assert_eq!(heartbeat.status, WorkerStatus::Busy);
        assert_eq!(heartbeat.active_jobs, 1);

        heartbeat.increment_processed();
        assert_eq!(heartbeat.total_processed, 1);

        heartbeat.decrement_active();
        assert_eq!(heartbeat.status, WorkerStatus::Idle);
        assert_eq!(heartbeat.active_jobs, 0);
    }

    #[test]
    fn test_checkpoint_state() {
        let mut checkpoint = CheckpointState::new("hash".to_string(), "pod-1".to_string(), 100);
        assert_eq!(checkpoint.last_frame, 0);
        assert_eq!(checkpoint.progress_percent(), 0.0);
        assert_eq!(checkpoint.byte_offset, 0);

        checkpoint.update(50, 50000).unwrap();
        assert_eq!(checkpoint.last_frame, 50);
        assert_eq!(checkpoint.byte_offset, 50000);
        assert_eq!(checkpoint.progress_percent(), 50.0);
        assert!(!checkpoint.is_complete());

        checkpoint.update(100, 100000).unwrap();
        assert!(checkpoint.is_complete());

        // Exceeding total frames should fail
        assert!(checkpoint.update(101, 101000).is_err());
    }

    #[test]
    fn test_checkpoint_state_with_videos() {
        let camera_ids = vec!["camera_1".to_string(), "camera_2".to_string()];
        let mut checkpoint = CheckpointState::with_videos(
            "job-1".to_string(),
            "pod-1".to_string(),
            1000,
            camera_ids.clone(),
        );

        assert_eq!(checkpoint.video_uploads.len(), 2);
        assert_eq!(checkpoint.video_uploads[0].camera_id, "camera_1");
        assert_eq!(checkpoint.video_uploads[1].camera_id, "camera_2");

        // Update video upload state
        let video = checkpoint.video_state_mut("camera_1");
        video.upload_id = "upload-123".to_string();
        video.add_part(1, "etag-1".to_string(), 64 * 1024 * 1024);

        assert!(video.is_active());
        assert_eq!(video.next_part_number(), 2);
        assert_eq!(video.uploaded_bytes(), 64 * 1024 * 1024);

        // Set parquet upload
        let parquet = ParquetUploadState::new("parquet-upload-456".to_string());
        checkpoint.set_parquet_upload(parquet);
        assert!(checkpoint.parquet_upload.is_some());
    }

    #[test]
    fn test_video_upload_state() {
        let mut state = VideoUploadState::new("camera_1".to_string());
        assert!(!state.is_active());
        assert_eq!(state.next_part_number(), 1);

        state.upload_id = "upload-123".to_string();
        assert!(state.is_active());

        state.add_part(1, "etag-1".to_string(), 10 * 1024 * 1024);
        state.add_part(2, "etag-2".to_string(), 10 * 1024 * 1024);

        assert_eq!(state.next_part_number(), 3);
        assert_eq!(state.uploaded_bytes(), 20 * 1024 * 1024);
    }

    #[test]
    fn test_parquet_upload_state() {
        let mut state = ParquetUploadState::new("parquet-123".to_string());
        assert_eq!(state.next_part_number(), 1);

        state.add_part(1, "etag-1".to_string(), 5 * 1024 * 1024);
        state.rows_written = 1000;

        assert_eq!(state.next_part_number(), 2);
        assert_eq!(state.rows_written, 1000);
    }

    #[test]
    fn test_checkpoint_serialization() {
        let checkpoint = CheckpointState::with_videos(
            "job-1".to_string(),
            "pod-1".to_string(),
            1000,
            vec!["camera_1".to_string()],
        );

        // Test bincode serialization
        let encoded = bincode::serialize(&checkpoint).unwrap();
        let decoded: CheckpointState = bincode::deserialize(&encoded).unwrap();
        assert_eq!(checkpoint, decoded);

        // Test JSON serialization
        let json = serde_json::to_string(&checkpoint).unwrap();
        let json_decoded: CheckpointState = serde_json::from_str(&json).unwrap();
        assert_eq!(checkpoint, json_decoded);
    }
}
