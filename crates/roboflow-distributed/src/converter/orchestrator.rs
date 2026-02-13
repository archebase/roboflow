// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! LeRobot converter orchestrator for distributed processing.
//!
//! This module provides the `LeRobotConverter` which orchestrates:
//! - Episode index allocation (via `EpisodeAllocator`)
//! - Checkpoint management for recovery
//! - LerobotWriter configuration with dynamic chunk/episode indices
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                     LeRobotConverter                        │
//! ├─────────────────────────────────────────────────────────────┤
//! │  ┌─────────────────┐    ┌─────────────────────────────┐    │
//! │  │ EpisodeAllocator│───▶│ EpisodeAllocation           │    │
//! │  │ (TiKV/Local)    │    │ - episode_index             │    │
//! │  └─────────────────┘    │ - chunk_index               │    │
//! │                         │ - chunk_offset               │    │
//! │                         └─────────────────────────────┘    │
//! │                                      │                      │
//! │                                      ▼                      │
//! │  ┌─────────────────────────────────────────────────────┐   │
//! │  │                    LerobotWriter                     │   │
//! │  │  - set_episode_index(allocation.episode_index)      │   │
//! │  │  - set_episodes_per_chunk(config.episodes_per_chunk)│   │
//! │  │  - Automatic chunk directory creation               │   │
//! │  └─────────────────────────────────────────────────────┘   │
//! │                                                             │
//! │  ┌─────────────────────────────────────────────────────┐   │
//! │  │                   CheckpointState                    │   │
//! │  │  - batch_id, episode_idx, chunk_idx                 │   │
//! │  │  - Progress tracking for spot instance recovery     │   │
//! │  └─────────────────────────────────────────────────────┘   │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Usage
//!
//! ```ignore
//! use roboflow_distributed::{LeRobotConverter, ConverterConfig, TiKVEpisodeAllocator};
//!
//! // Create with TiKV backend for distributed processing
//! let allocator = TiKVEpisodeAllocator::new(tikv_client, "my-batch".to_string(), 500);
//! let config = ConverterConfig::new("s3://bucket/dataset", 500);
//! let converter = LeRobotConverter::new(allocator, config);
//!
//! // Allocate episode for a file
//! let allocation = converter.allocate_episode().await?;
//!
//! // Configure writer with the allocated episode
//! converter.configure_writer(&mut writer, &allocation);
//! ```

use std::path::PathBuf;
use std::sync::Arc;

use roboflow_dataset::lerobot::LerobotWriter;

use crate::CheckpointState;
use crate::episode::{EpisodeAllocation, EpisodeAllocator, EpisodeAllocatorError};

/// Default number of episodes per chunk (LeRobot v2.1 spec).
pub const DEFAULT_EPISODES_PER_CHUNK: u32 = 500;

/// Configuration for the LeRobot converter.
#[derive(Debug, Clone)]
pub struct ConverterConfig {
    /// Batch ID for distributed processing.
    pub batch_id: String,

    /// Number of episodes per chunk.
    /// Default is 500 (LeRobot v2.1 spec).
    pub episodes_per_chunk: u32,

    /// Output directory/base path.
    pub output_path: PathBuf,

    /// Whether to enable checkpoint recovery.
    pub enable_checkpoints: bool,

    /// Pod ID for identifying this worker.
    pub pod_id: String,
}

impl ConverterConfig {
    /// Create a new converter configuration.
    pub fn new(output_path: impl Into<PathBuf>, episodes_per_chunk: u32) -> Self {
        Self {
            batch_id: String::new(),
            episodes_per_chunk,
            output_path: output_path.into(),
            enable_checkpoints: true,
            pod_id: default_pod_id(),
        }
    }

    /// Create configuration with batch ID for distributed processing.
    pub fn with_batch(
        batch_id: impl Into<String>,
        output_path: impl Into<PathBuf>,
        episodes_per_chunk: u32,
    ) -> Self {
        Self {
            batch_id: batch_id.into(),
            episodes_per_chunk,
            output_path: output_path.into(),
            enable_checkpoints: true,
            pod_id: default_pod_id(),
        }
    }

    /// Set the batch ID.
    pub fn batch_id(mut self, id: impl Into<String>) -> Self {
        self.batch_id = id.into();
        self
    }

    /// Set the pod ID.
    pub fn pod_id(mut self, id: impl Into<String>) -> Self {
        self.pod_id = id.into();
        self
    }

    /// Enable or disable checkpoints.
    pub fn enable_checkpoints(mut self, enabled: bool) -> Self {
        self.enable_checkpoints = enabled;
        self
    }
}

impl Default for ConverterConfig {
    fn default() -> Self {
        Self {
            batch_id: String::new(),
            episodes_per_chunk: DEFAULT_EPISODES_PER_CHUNK,
            output_path: PathBuf::from("./output"),
            enable_checkpoints: true,
            pod_id: default_pod_id(),
        }
    }
}

/// Generate a default pod ID from hostname and process ID.
fn default_pod_id() -> String {
    let hostname = gethostname::gethostname().to_string_lossy().to_string();
    let pid = std::process::id();
    format!("{}-{}", hostname, pid)
}

/// Error type for converter operations.
#[derive(Debug, thiserror::Error)]
pub enum ConverterError {
    /// Episode allocation failed.
    #[error("Episode allocation failed: {0}")]
    AllocationFailed(#[from] EpisodeAllocatorError),

    /// Checkpoint operation failed.
    #[error("Checkpoint error: {0}")]
    CheckpointError(String),

    /// Writer configuration failed.
    #[error("Writer configuration error: {0}")]
    WriterConfigError(String),

    /// Invalid state for operation.
    #[error("Invalid state: {0}")]
    InvalidState(String),
}

/// LeRobot converter orchestrator for distributed processing.
///
/// This struct coordinates:
/// 1. Episode index allocation via `EpisodeAllocator`
/// 2. LerobotWriter configuration with dynamic episode/chunk indices
/// 3. Checkpoint state management for recovery
///
/// # Thread Safety
///
/// The converter is designed to be used from a single task/thread.
/// For concurrent processing, create multiple converters with the
/// same allocator (allocators are thread-safe).
pub struct LeRobotConverter {
    /// Episode allocator (TiKV or Local).
    allocator: Arc<dyn EpisodeAllocator>,

    /// Converter configuration.
    config: ConverterConfig,

    /// Current allocation (if any).
    current_allocation: Option<EpisodeAllocation>,

    /// Current checkpoint state (if any).
    checkpoint: Option<CheckpointState>,
}

impl LeRobotConverter {
    /// Create a new LeRobot converter.
    pub fn new(allocator: Arc<dyn EpisodeAllocator>, config: ConverterConfig) -> Self {
        Self {
            allocator,
            config,
            current_allocation: None,
            checkpoint: None,
        }
    }

    /// Create a converter with a local allocator (for single-process use).
    pub fn local(config: ConverterConfig) -> Self {
        let allocator = Arc::new(crate::episode::LocalEpisodeAllocator::new(
            config.episodes_per_chunk,
        ));
        Self::new(allocator, config)
    }

    /// Get the current configuration.
    pub fn config(&self) -> &ConverterConfig {
        &self.config
    }

    /// Get the current allocation (if any).
    pub fn current_allocation(&self) -> Option<&EpisodeAllocation> {
        self.current_allocation.as_ref()
    }

    /// Allocate a new episode index.
    ///
    /// This method:
    /// 1. Calls the allocator to get the next episode index
    /// 2. Creates a checkpoint state for tracking
    /// 3. Stores the allocation for later use
    ///
    /// Returns the allocation with episode_index, chunk_index, and chunk_offset.
    pub async fn allocate_episode(
        &mut self,
    ) -> std::result::Result<EpisodeAllocation, ConverterError> {
        let allocation = self.allocator.allocate().await?;

        tracing::info!(
            batch_id = %self.config.batch_id,
            episode_index = allocation.episode_index,
            chunk_index = allocation.chunk_index,
            chunk_offset = allocation.chunk_offset,
            "Allocated episode"
        );

        // Store the allocation
        self.current_allocation = Some(allocation);

        // Initialize checkpoint state if enabled
        if self.config.enable_checkpoints {
            self.checkpoint = Some(CheckpointState::with_batch(
                allocation.episode_index.to_string(), // job_id
                self.config.batch_id.clone(),
                self.config.pod_id.clone(),
                0, // total_frames (will be updated later)
                self.config.episodes_per_chunk,
            ));
        }

        Ok(allocation)
    }

    /// Configure a LerobotWriter with the current allocation.
    ///
    /// This sets:
    /// - `episode_index` from the allocation
    /// - `episodes_per_chunk` from the config
    ///
    /// The writer will then automatically compute `chunk_index`
    /// and create the correct directory structure.
    pub fn configure_writer(
        &self,
        writer: &mut LerobotWriter,
        allocation: &EpisodeAllocation,
    ) -> std::result::Result<(), ConverterError> {
        writer.set_episode_index(allocation.episode_index as usize);
        writer.set_episodes_per_chunk(self.config.episodes_per_chunk);

        tracing::debug!(
            episode_index = allocation.episode_index,
            chunk_index = allocation.chunk_index,
            episodes_per_chunk = self.config.episodes_per_chunk,
            "Configured writer with episode allocation"
        );

        Ok(())
    }

    /// Create a checkpoint state for a file.
    ///
    /// This should be called after allocating an episode and
    /// determining the total frames in the file.
    pub fn create_checkpoint(&self, job_id: String, total_frames: u64) -> CheckpointState {
        // Note: allocation is not used here but may be useful for logging

        CheckpointState::with_batch(
            job_id,
            self.config.batch_id.clone(),
            self.config.pod_id.clone(),
            total_frames,
            self.config.episodes_per_chunk,
        )
    }

    /// Update checkpoint with progress.
    ///
    /// Returns an error if no checkpoint has been created.
    pub fn update_checkpoint(
        &mut self,
        frame: u64,
        byte_offset: u64,
    ) -> std::result::Result<(), ConverterError> {
        let checkpoint = self
            .checkpoint
            .as_mut()
            .ok_or(ConverterError::InvalidState(
                "No checkpoint to update".to_string(),
            ))?;

        checkpoint
            .update(frame, byte_offset)
            .map_err(ConverterError::CheckpointError)?;

        Ok(())
    }

    /// Update checkpoint episode index.
    ///
    /// This also updates the chunk_idx automatically.
    pub fn update_checkpoint_episode(
        &mut self,
        episode: u64,
    ) -> std::result::Result<(), ConverterError> {
        let checkpoint = self
            .checkpoint
            .as_mut()
            .ok_or(ConverterError::InvalidState(
                "No checkpoint to update".to_string(),
            ))?;

        checkpoint.update_episode(episode);
        Ok(())
    }

    /// Get the current checkpoint state.
    pub fn checkpoint(&self) -> Option<&CheckpointState> {
        self.checkpoint.as_ref()
    }

    /// Take ownership of the checkpoint state.
    ///
    /// This is useful for saving the checkpoint externally.
    pub fn take_checkpoint(&mut self) -> Option<CheckpointState> {
        self.checkpoint.take()
    }

    /// Restore checkpoint state (e.g., after recovery).
    ///
    /// This validates the checkpoint consistency and updates
    /// the internal state.
    pub fn restore_checkpoint(
        &mut self,
        checkpoint: CheckpointState,
    ) -> std::result::Result<(), ConverterError> {
        // Validate consistency
        if !checkpoint.validate_episode_consistency() {
            return Err(ConverterError::CheckpointError(
                "Checkpoint episode/chunk consistency check failed".to_string(),
            ));
        }

        // Create allocation from checkpoint
        self.current_allocation = Some(EpisodeAllocation::new(
            checkpoint.episode_idx,
            checkpoint.episodes_per_chunk,
        ));

        self.checkpoint = Some(checkpoint);

        tracing::info!(
            episode_index = self.current_allocation.as_ref().unwrap().episode_index,
            chunk_index = self.current_allocation.as_ref().unwrap().chunk_index,
            "Restored checkpoint"
        );

        Ok(())
    }

    /// Get the output path for a specific episode.
    ///
    /// Returns: `{output_path}/data/chunk-{chunk:03d}/episode_{episode:06}.parquet`
    pub fn episode_output_path(&self, allocation: &EpisodeAllocation) -> PathBuf {
        self.config
            .output_path
            .join("data")
            .join(format!("chunk-{:03}", allocation.chunk_index))
            .join(format!("episode_{:06}.parquet", allocation.episode_index))
    }

    /// Get the video output path for a specific episode and camera.
    ///
    /// Returns: `{output_path}/videos/chunk-{chunk:03d}/{camera}/episode_{episode:06}.mp4`
    pub fn video_output_path(&self, allocation: &EpisodeAllocation, camera: &str) -> PathBuf {
        self.config
            .output_path
            .join("videos")
            .join(format!("chunk-{:03}", allocation.chunk_index))
            .join(camera)
            .join(format!("episode_{:06}.mp4", allocation.episode_index))
    }

    /// Get the video output path relative to output directory.
    ///
    /// Returns: `videos/chunk-{chunk:03d}/{camera}/episode_{episode:06}.mp4`
    pub fn video_output_path_relative(
        &self,
        allocation: &EpisodeAllocation,
        camera: &str,
    ) -> String {
        format!(
            "videos/chunk-{:03}/{}/episode_{:06}.mp4",
            allocation.chunk_index, camera, allocation.episode_index
        )
    }

    /// Reset the converter for a new file.
    ///
    /// This clears the current allocation and checkpoint,
    /// but keeps the allocator for reuse.
    pub fn reset(&mut self) {
        self.current_allocation = None;
        self.checkpoint = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_converter_config_default() {
        let config = ConverterConfig::default();
        assert_eq!(config.episodes_per_chunk, DEFAULT_EPISODES_PER_CHUNK);
        assert!(config.enable_checkpoints);
        assert!(!config.pod_id.is_empty());
    }

    #[test]
    fn test_converter_config_builder() {
        let config = ConverterConfig::new("/output", 250)
            .batch_id("batch-123")
            .pod_id("pod-1")
            .enable_checkpoints(false);

        assert_eq!(config.batch_id, "batch-123");
        assert_eq!(config.episodes_per_chunk, 250);
        assert_eq!(config.pod_id, "pod-1");
        assert!(!config.enable_checkpoints);
    }

    #[test]
    fn test_converter_config_with_batch() {
        let config = ConverterConfig::with_batch("batch-456", "/data", 1000);
        assert_eq!(config.batch_id, "batch-456");
        assert_eq!(config.episodes_per_chunk, 1000);
    }

    #[tokio::test]
    async fn test_local_converter_allocate_episode() {
        let config = ConverterConfig::new("/output", 500);
        let mut converter = LeRobotConverter::local(config);

        // Allocate first episode
        let alloc1 = converter.allocate_episode().await.unwrap();
        assert_eq!(alloc1.episode_index, 0);
        assert_eq!(alloc1.chunk_index, 0);
        assert_eq!(alloc1.chunk_offset, 0);

        // Allocate second episode
        let alloc2 = converter.allocate_episode().await.unwrap();
        assert_eq!(alloc2.episode_index, 1);
        assert_eq!(alloc2.chunk_index, 0);
        assert_eq!(alloc2.chunk_offset, 1);
    }

    #[tokio::test]
    async fn test_converter_chunk_calculation() {
        let config = ConverterConfig::new("/output", 500);
        let mut converter = LeRobotConverter::local(config);

        // Episode 0-499 should be in chunk 0
        for _ in 0..500 {
            let alloc = converter.allocate_episode().await.unwrap();
            assert_eq!(alloc.chunk_index, 0);
        }

        // Episode 500 should be in chunk 1
        let alloc = converter.allocate_episode().await.unwrap();
        assert_eq!(alloc.episode_index, 500);
        assert_eq!(alloc.chunk_index, 1);
        assert_eq!(alloc.chunk_offset, 0);
    }

    #[tokio::test]
    async fn test_converter_checkpoint() {
        let config = ConverterConfig::new("/output", 500);
        let mut converter = LeRobotConverter::local(config);

        // Allocate and create checkpoint
        converter.allocate_episode().await.unwrap();
        let checkpoint = converter.create_checkpoint("job-1".to_string(), 1000);

        assert_eq!(checkpoint.job_id, "job-1");
        assert_eq!(checkpoint.total_frames, 1000);
        assert_eq!(checkpoint.episode_idx, 0);
        assert_eq!(checkpoint.chunk_idx, 0);
    }

    #[tokio::test]
    async fn test_converter_update_checkpoint() {
        let config = ConverterConfig::new("/output", 500);
        let mut converter = LeRobotConverter::local(config);

        // Allocate episode
        converter.allocate_episode().await.unwrap();

        // Create checkpoint manually for testing
        converter.checkpoint = Some(CheckpointState::new(
            "job-1".to_string(),
            "pod-1".to_string(),
            1000,
        ));

        // Update checkpoint
        converter.update_checkpoint(100, 5000).unwrap();
        converter.update_checkpoint_episode(5).unwrap();

        let checkpoint = converter.checkpoint().unwrap();
        assert_eq!(checkpoint.last_frame, 100);
        assert_eq!(checkpoint.byte_offset, 5000);
        assert_eq!(checkpoint.episode_idx, 5);
        assert_eq!(checkpoint.chunk_idx, 0); // 5 / 500 = 0
    }

    #[tokio::test]
    async fn test_converter_restore_checkpoint() {
        let config = ConverterConfig::new("/output", 500);
        let mut converter = LeRobotConverter::local(config);

        // Create a checkpoint to restore
        let mut checkpoint = CheckpointState::with_batch(
            "job-1".to_string(),
            "batch-123".to_string(),
            "pod-1".to_string(),
            1000,
            500,
        );
        checkpoint.update_episode(750);

        // Restore checkpoint
        converter.restore_checkpoint(checkpoint).unwrap();

        let allocation = converter.current_allocation().unwrap();
        assert_eq!(allocation.episode_index, 750);
        assert_eq!(allocation.chunk_index, 1); // 750 / 500 = 1
    }

    #[tokio::test]
    async fn test_converter_restore_checkpoint_inconsistent() {
        let config = ConverterConfig::new("/output", 500);
        let mut converter = LeRobotConverter::local(config);

        // Create an inconsistent checkpoint (chunk_idx doesn't match episode)
        let mut checkpoint = CheckpointState::with_batch(
            "job-1".to_string(),
            "batch-123".to_string(),
            "pod-1".to_string(),
            1000,
            500,
        );
        checkpoint.episode_idx = 750;
        checkpoint.chunk_idx = 0; // Should be 1

        // Restore should fail consistency check
        let result = converter.restore_checkpoint(checkpoint);
        assert!(result.is_err());
    }

    #[test]
    fn test_episode_output_path() {
        let config = ConverterConfig::new("/output", 500);
        let converter = LeRobotConverter::local(config);
        let alloc = EpisodeAllocation::new(0, 500);

        let path = converter.episode_output_path(&alloc);
        assert_eq!(
            path.to_string_lossy(),
            "/output/data/chunk-000/episode_000000.parquet"
        );

        let alloc2 = EpisodeAllocation::new(500, 500);
        let path2 = converter.episode_output_path(&alloc2);
        assert_eq!(
            path2.to_string_lossy(),
            "/output/data/chunk-001/episode_000500.parquet"
        );
    }

    #[test]
    fn test_video_output_path() {
        let config = ConverterConfig::new("/output", 500);
        let converter = LeRobotConverter::local(config);
        let alloc = EpisodeAllocation::new(1234, 500);

        let path = converter.video_output_path(&alloc, "cam_left");
        assert_eq!(
            path.to_string_lossy(),
            "/output/videos/chunk-002/cam_left/episode_001234.mp4"
        );

        let relative = converter.video_output_path_relative(&alloc, "cam_left");
        assert_eq!(relative, "videos/chunk-002/cam_left/episode_001234.mp4");
    }

    #[tokio::test]
    async fn test_converter_reset() {
        let config = ConverterConfig::new("/output", 500);
        let mut converter = LeRobotConverter::local(config);

        // Allocate and create state
        converter.allocate_episode().await.unwrap();
        assert!(converter.current_allocation().is_some());

        // Reset
        converter.reset();
        assert!(converter.current_allocation().is_none());
        assert!(converter.checkpoint().is_none());
    }

    #[tokio::test]
    async fn test_high_episode_index_chunk_calculation() {
        // Test for 100K episodes scenario
        let config = ConverterConfig::new("/output", 500);
        let converter = LeRobotConverter::local(config);

        // Simulate allocating episode 99,999 (last in chunk 199)
        // We'll manually create the allocation
        let alloc = EpisodeAllocation::new(99_999, 500);
        assert_eq!(alloc.chunk_index, 199); // 99999 / 500 = 199
        assert_eq!(alloc.chunk_offset, 499); // 99999 % 500 = 499

        // Verify output path
        let path = converter.episode_output_path(&alloc);
        assert!(path.to_string_lossy().contains("chunk-199"));
        assert!(path.to_string_lossy().contains("episode_099999"));
    }
}
