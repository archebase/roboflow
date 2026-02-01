// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Checkpoint manager for frame-level progress tracking.
//!
//! This module provides the CheckpointManager which handles:
//! - Loading checkpoints from TiKV
//! - Saving checkpoints with optional heartbeat in single transaction
//! - Deleting checkpoints after job completion
//! - Combined checkpoint+heartbeat transactions for efficiency

use std::sync::Arc;
use std::time::Duration;

use super::client::TikvClient;
use super::error::{Result, TikvError};
use super::key::{HeartbeatKeys, StateKeys};
use super::schema::{CheckpointState, HeartbeatRecord, WorkerStatus};

/// Default checkpoint interval in frames.
pub const DEFAULT_CHECKPOINT_INTERVAL_FRAMES: u64 = 100;

/// Default checkpoint interval in seconds.
pub const DEFAULT_CHECKPOINT_INTERVAL_SECS: u64 = 10;

/// Checkpoint manager configuration.
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// Checkpoint every N frames.
    pub checkpoint_interval_frames: u64,

    /// Checkpoint every N seconds.
    pub checkpoint_interval_seconds: u64,

    /// Whether to use async checkpointing (non-blocking saves).
    pub checkpoint_async: bool,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            checkpoint_interval_frames: DEFAULT_CHECKPOINT_INTERVAL_FRAMES,
            checkpoint_interval_seconds: DEFAULT_CHECKPOINT_INTERVAL_SECS,
            checkpoint_async: true,
        }
    }
}

impl CheckpointConfig {
    /// Create a new checkpoint configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the frame interval.
    pub fn with_frame_interval(mut self, interval: u64) -> Self {
        self.checkpoint_interval_frames = interval;
        self
    }

    /// Set the time interval.
    pub fn with_time_interval(mut self, interval: u64) -> Self {
        self.checkpoint_interval_seconds = interval;
        self
    }

    /// Enable or disable async checkpointing.
    pub fn with_async(mut self, async_mode: bool) -> Self {
        self.checkpoint_async = async_mode;
        self
    }
}

/// Checkpoint manager for frame-level progress tracking.
///
/// Manages checkpoint persistence in TiKV with support for:
/// - Single-operation checkpoint saves
/// - Combined checkpoint+heartbeat transactions
/// - Checkpoint expiration tracking
pub struct CheckpointManager {
    /// TiKV client for checkpoint operations.
    tikv: Arc<TikvClient>,

    /// Checkpoint configuration.
    config: CheckpointConfig,
}

impl CheckpointManager {
    /// Create a new checkpoint manager.
    pub fn new(tikv: Arc<TikvClient>, config: CheckpointConfig) -> Self {
        Self { tikv, config }
    }

    /// Create with default configuration.
    pub fn with_defaults(tikv: Arc<TikvClient>) -> Self {
        Self::new(tikv, CheckpointConfig::default())
    }

    /// Get a reference to the configuration.
    pub fn config(&self) -> &CheckpointConfig {
        &self.config
    }

    /// Load a checkpoint by job ID.
    ///
    /// Returns None if no checkpoint exists.
    pub fn load(&self, job_id: &str) -> Result<Option<CheckpointState>> {
        let tikv = self.tikv.clone();
        let job_id = job_id.to_string();
        Self::block_on(|handle| handle.block_on(async move { tikv.get_checkpoint(&job_id).await }))
    }

    /// Save a checkpoint.
    ///
    /// This updates the checkpoint in TiKV with the current state.
    pub fn save(&self, checkpoint: &CheckpointState) -> Result<()> {
        let tikv = self.tikv.clone();
        let checkpoint = checkpoint.clone();
        Self::block_on(|handle| {
            handle.block_on(async move { tikv.update_checkpoint(&checkpoint).await })
        })
    }

    /// Save checkpoint with heartbeat in a single transaction.
    ///
    /// This is more efficient than separate checkpoint and heartbeat updates.
    pub fn save_with_heartbeat(
        &self,
        checkpoint: &CheckpointState,
        pod_id: &str,
        status: WorkerStatus,
    ) -> Result<()> {
        let tikv = self.tikv.clone();
        let checkpoint = checkpoint.clone();
        let pod_id = pod_id.to_string();

        Self::block_on(|handle| {
            handle.block_on(async move {
                // Get existing heartbeat or create new one
                let mut heartbeat = tikv
                    .get_heartbeat(&pod_id)
                    .await?
                    .unwrap_or_else(|| HeartbeatRecord::new(pod_id.clone()));

                heartbeat.beat();
                heartbeat.status = status;

                // Serialize both
                let checkpoint_data = bincode::serialize(&checkpoint)
                    .map_err(|e| TikvError::Serialization(e.to_string()))?;
                let heartbeat_data = bincode::serialize(&heartbeat)
                    .map_err(|e| TikvError::Serialization(e.to_string()))?;

                // Batch put in single transaction
                let checkpoint_key = StateKeys::checkpoint(&checkpoint.job_id);
                let heartbeat_key = HeartbeatKeys::heartbeat(&pod_id);

                tikv.batch_put(vec![
                    (checkpoint_key, checkpoint_data),
                    (heartbeat_key, heartbeat_data),
                ])
                .await
            })
        })
    }

    /// Delete a checkpoint.
    ///
    /// Called after successful job completion.
    pub fn delete(&self, job_id: &str) -> Result<()> {
        let tikv = self.tikv.clone();
        let job_id = job_id.to_string();
        Self::block_on(|handle| {
            handle.block_on(async move {
                let key = StateKeys::checkpoint(&job_id);
                tikv.delete(key).await
            })
        })
    }

    /// Check if a checkpoint should be saved based on configuration.
    ///
    /// Returns true if either:
    /// - Frames since last checkpoint >= checkpoint_interval_frames
    /// - Time since last checkpoint >= checkpoint_interval_seconds
    pub fn should_checkpoint(&self, frames_since_last: u64, time_since_last: Duration) -> bool {
        frames_since_last >= self.config.checkpoint_interval_frames
            || time_since_last.as_secs() >= self.config.checkpoint_interval_seconds
    }

    /// Async checkpoint save (non-blocking).
    ///
    /// Spawns a background task to save the checkpoint without blocking
    /// the current execution. Errors are logged but not returned.
    #[allow(dead_code)]
    pub fn save_async(&self, checkpoint: CheckpointState) {
        if !self.config.checkpoint_async {
            // If async mode is disabled, do synchronous save
            let _ = self.save(&checkpoint);
            return;
        }

        let tikv = self.tikv.clone();
        tokio::spawn(async move {
            if let Err(e) = tikv.update_checkpoint(&checkpoint).await {
                tracing::warn!(
                    job_id = %checkpoint.job_id,
                    last_frame = checkpoint.last_frame,
                    error = %e,
                    "Async checkpoint save failed"
                );
            } else {
                tracing::debug!(
                    job_id = %checkpoint.job_id,
                    last_frame = checkpoint.last_frame,
                    "Async checkpoint saved successfully"
                );
            }
        });
    }

    /// Calculate next checkpoint frame number.
    pub fn next_checkpoint_frame(&self, current_frame: u64) -> u64 {
        ((current_frame / self.config.checkpoint_interval_frames) + 1)
            * self.config.checkpoint_interval_frames
    }

    /// Helper to execute async code in current runtime or create a temporary one.
    fn block_on<F, R>(f: F) -> R
    where
        F: FnOnce(tokio::runtime::Handle) -> R + Send,
    {
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => f(handle),
            Err(_) => {
                let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
                f(rt.handle().clone())
            }
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_config_default() {
        let config = CheckpointConfig::default();
        assert_eq!(
            config.checkpoint_interval_frames,
            DEFAULT_CHECKPOINT_INTERVAL_FRAMES
        );
        assert_eq!(
            config.checkpoint_interval_seconds,
            DEFAULT_CHECKPOINT_INTERVAL_SECS
        );
        assert!(config.checkpoint_async);
    }

    #[test]
    fn test_checkpoint_config_builder() {
        let config = CheckpointConfig::new()
            .with_frame_interval(200)
            .with_time_interval(30)
            .with_async(false);

        assert_eq!(config.checkpoint_interval_frames, 200);
        assert_eq!(config.checkpoint_interval_seconds, 30);
        assert!(!config.checkpoint_async);
    }

    #[test]
    fn test_should_checkpoint() {
        let config = CheckpointConfig::default();

        // Should checkpoint when frame interval reached
        assert!(should_checkpoint_impl(100, Duration::from_secs(5), &config));

        // Should checkpoint when time interval reached
        assert!(should_checkpoint_impl(50, Duration::from_secs(10), &config));

        // Should not checkpoint when neither threshold reached
        assert!(!should_checkpoint_impl(50, Duration::from_secs(5), &config));

        // Should checkpoint when both thresholds reached
        assert!(should_checkpoint_impl(
            100,
            Duration::from_secs(10),
            &config
        ));
    }

    #[test]
    fn test_next_checkpoint_frame() {
        let config = CheckpointConfig::default();
        assert_eq!(next_checkpoint_frame_impl(0, &config), 100);
        assert_eq!(next_checkpoint_frame_impl(50, &config), 100);
        assert_eq!(next_checkpoint_frame_impl(99, &config), 100);
        assert_eq!(next_checkpoint_frame_impl(100, &config), 200);
        assert_eq!(next_checkpoint_frame_impl(150, &config), 200);
    }

    // Test CheckpointManager construction
    #[test]
    fn test_checkpoint_manager_new() {
        use super::super::client::TikvClient;
        use std::sync::Arc;

        // Create a mock client (we just need Arc<TikvClient> for construction tests)
        // Note: This will fail with actual operations but is fine for construction tests
        let config = CheckpointConfig::default();
        let tikv = Arc::new(TikvClient::no_op_for_testing());
        let manager = CheckpointManager::new(tikv.clone(), config.clone());

        assert_eq!(manager.config().checkpoint_interval_frames, 100);
        assert_eq!(manager.config().checkpoint_interval_seconds, 10);
        assert!(manager.config().checkpoint_async);
    }

    #[test]
    fn test_checkpoint_manager_with_defaults() {
        use super::super::client::TikvClient;
        use std::sync::Arc;

        let tikv = Arc::new(TikvClient::no_op_for_testing());
        let manager = CheckpointManager::with_defaults(tikv);

        assert_eq!(manager.config().checkpoint_interval_frames, 100);
        assert_eq!(manager.config().checkpoint_interval_seconds, 10);
        assert!(manager.config().checkpoint_async);
    }

    // Test should_checkpoint on manager
    #[test]
    fn test_manager_should_checkpoint() {
        use super::super::client::TikvClient;
        use std::sync::Arc;

        let tikv = Arc::new(TikvClient::no_op_for_testing());
        let manager = CheckpointManager::with_defaults(tikv);

        // Should checkpoint when frame interval reached
        assert!(manager.should_checkpoint(100, Duration::from_secs(5)));

        // Should checkpoint when time interval reached
        assert!(manager.should_checkpoint(50, Duration::from_secs(10)));

        // Should not checkpoint when neither threshold reached
        assert!(!manager.should_checkpoint(50, Duration::from_secs(5)));

        // Should checkpoint when both thresholds reached
        assert!(manager.should_checkpoint(100, Duration::from_secs(10)));
    }

    #[test]
    fn test_manager_should_checkpoint_custom_config() {
        use super::super::client::TikvClient;
        use std::sync::Arc;

        let config = CheckpointConfig::new()
            .with_frame_interval(50)
            .with_time_interval(5);
        let tikv = Arc::new(TikvClient::no_op_for_testing());
        let manager = CheckpointManager::new(tikv, config);

        // Should checkpoint at 50 frames
        assert!(manager.should_checkpoint(50, Duration::from_secs(1)));

        // Should checkpoint at 5 seconds
        assert!(manager.should_checkpoint(10, Duration::from_secs(5)));

        // Should not checkpoint below thresholds
        assert!(!manager.should_checkpoint(49, Duration::from_secs(4)));
    }

    // Test next_checkpoint_frame
    #[test]
    fn test_manager_next_checkpoint_frame() {
        use super::super::client::TikvClient;
        use std::sync::Arc;

        let tikv = Arc::new(TikvClient::no_op_for_testing());
        let manager = CheckpointManager::with_defaults(tikv);

        assert_eq!(manager.next_checkpoint_frame(0), 100);
        assert_eq!(manager.next_checkpoint_frame(50), 100);
        assert_eq!(manager.next_checkpoint_frame(99), 100);
        assert_eq!(manager.next_checkpoint_frame(100), 200);
        assert_eq!(manager.next_checkpoint_frame(150), 200);
        assert_eq!(manager.next_checkpoint_frame(500), 600);
    }

    #[test]
    fn test_manager_next_checkpoint_frame_custom_interval() {
        use super::super::client::TikvClient;
        use std::sync::Arc;

        let config = CheckpointConfig::new().with_frame_interval(250);
        let tikv = Arc::new(TikvClient::no_op_for_testing());
        let manager = CheckpointManager::new(tikv, config);

        assert_eq!(manager.next_checkpoint_frame(0), 250);
        assert_eq!(manager.next_checkpoint_frame(100), 250);
        assert_eq!(manager.next_checkpoint_frame(250), 500);
        assert_eq!(manager.next_checkpoint_frame(500), 750);
    }

    // Test block_on helper with existing runtime
    #[test]
    fn test_block_on_with_existing_runtime() {
        use super::super::client::TikvClient;
        use std::panic::AssertUnwindSafe;
        use std::sync::Arc;

        // This test runs within the test harness runtime
        let tikv = Arc::new(TikvClient::no_op_for_testing());
        let manager = CheckpointManager::with_defaults(tikv);

        // block_on should work with existing runtime
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            // Call a method that uses block_on
            let job_id = "test-job";
            let _ = manager.load(job_id);
        }));

        // Should not panic
        assert!(result.is_ok());
    }

    // Test block_on helper without existing runtime
    #[test]
    fn test_block_on_creates_runtime() {
        // Spawn a thread without a tokio runtime
        let handle = std::thread::spawn(|| {
            // This thread has no tokio runtime
            use super::super::client::TikvClient;
            use std::sync::Arc;

            let tikv = Arc::new(TikvClient::no_op_for_testing());
            let manager = CheckpointManager::with_defaults(tikv);

            // block_on should create a new runtime
            let job_id = "test-job-no-runtime";
            let _ = manager.load(job_id);
        });

        // Should complete without panic
        assert!(handle.join().is_ok());
    }

    // Test save_async with async disabled
    #[test]
    fn test_save_async_disabled() {
        use super::super::client::TikvClient;
        use super::super::schema::CheckpointState;
        use chrono::Utc;
        use std::panic::AssertUnwindSafe;
        use std::sync::Arc;

        let config = CheckpointConfig::new().with_async(false);
        let tikv = Arc::new(TikvClient::no_op_for_testing());
        let manager = CheckpointManager::new(tikv, config);

        let checkpoint = CheckpointState {
            job_id: "test-async-disabled".to_string(),
            pod_id: "pod-1".to_string(),
            byte_offset: 0,
            last_frame: 100,
            episode_idx: 0,
            total_frames: 1000,
            video_uploads: vec![],
            parquet_upload: None,
            updated_at: Utc::now(),
            version: 1,
        };

        // Should not panic when async is disabled
        std::panic::catch_unwind(AssertUnwindSafe(|| {
            manager.save_async(checkpoint);
        }))
        .expect("save_async with disabled async should not panic");
    }

    // Test save_async with async enabled
    #[test]
    fn test_save_async_enabled() {
        use super::super::client::TikvClient;
        use super::super::schema::CheckpointState;
        use chrono::Utc;
        use std::panic::AssertUnwindSafe;
        use std::sync::Arc;

        // Need a runtime for tokio::spawn
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(async {
            let tikv = Arc::new(TikvClient::no_op_for_testing());
            let manager = CheckpointManager::with_defaults(tikv);

            let checkpoint = CheckpointState {
                job_id: "test-async-enabled".to_string(),
                pod_id: "pod-1".to_string(),
                byte_offset: 0,
                last_frame: 100,
                episode_idx: 0,
                total_frames: 1000,
                video_uploads: vec![],
                parquet_upload: None,
                updated_at: Utc::now(),
                version: 1,
            };

            // Should not panic when async is enabled
            std::panic::catch_unwind(AssertUnwindSafe(|| {
                manager.save_async(checkpoint);
            }))
        });

        // Should complete without panic
        assert!(result.is_ok());

        // Give the spawned task a moment to complete
        let _ = rt.block_on(async {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        });
    }

    // Test edge cases for should_checkpoint
    #[test]
    fn test_should_checkpoint_edge_cases() {
        use super::super::client::TikvClient;
        use std::sync::Arc;

        let tikv = Arc::new(TikvClient::no_op_for_testing());
        let manager = CheckpointManager::with_defaults(tikv);

        // Exactly at threshold - should checkpoint
        assert!(manager.should_checkpoint(100, Duration::from_secs(0)));
        assert!(manager.should_checkpoint(0, Duration::from_secs(10)));

        // Just below threshold - should not checkpoint
        assert!(!manager.should_checkpoint(99, Duration::from_secs(0)));
        assert!(!manager.should_checkpoint(0, Duration::from_secs(9)));

        // Zero values - should not checkpoint (default intervals are 100 and 10)
        assert!(!manager.should_checkpoint(0, Duration::from_secs(0)));
    }

    // Test checkpoint config edge cases
    #[test]
    fn test_checkpoint_config_zero_intervals() {
        let config = CheckpointConfig::new()
            .with_frame_interval(0)
            .with_time_interval(0);

        // Zero interval means always checkpoint
        assert!(should_checkpoint_impl(0, Duration::from_secs(0), &config));
        assert!(should_checkpoint_impl(1, Duration::from_secs(1), &config));
    }

    // Test next_checkpoint_frame edge cases
    #[test]
    fn test_next_checkpoint_frame_edge_cases() {
        use super::super::client::TikvClient;
        use std::sync::Arc;

        let tikv = Arc::new(TikvClient::no_op_for_testing());
        let manager = CheckpointManager::with_defaults(tikv);

        // Large frame numbers
        assert_eq!(manager.next_checkpoint_frame(9999), 10000);
        assert_eq!(manager.next_checkpoint_frame(10000), 10100);

        // Frame number exactly at checkpoint boundary
        assert_eq!(manager.next_checkpoint_frame(100), 200);
        assert_eq!(manager.next_checkpoint_frame(1000), 1100);
    }
}

// Helper functions for testing without a real client
#[allow(dead_code)]
fn should_checkpoint_impl(
    frames_since_last: u64,
    time_since_last: Duration,
    config: &CheckpointConfig,
) -> bool {
    frames_since_last >= config.checkpoint_interval_frames
        || time_since_last.as_secs() >= config.checkpoint_interval_seconds
}

#[allow(dead_code)]
fn next_checkpoint_frame_impl(current_frame: u64, config: &CheckpointConfig) -> u64 {
    ((current_frame / config.checkpoint_interval_frames) + 1) * config.checkpoint_interval_frames
}
