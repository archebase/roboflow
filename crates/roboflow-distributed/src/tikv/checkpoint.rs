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

impl Clone for CheckpointManager {
    fn clone(&self) -> Self {
        Self {
            tikv: self.tikv.clone(),
            config: self.config.clone(),
        }
    }
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

    /// Helper to block on an async future, handling runtime detection.
    fn block_on<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(Arc<TikvClient>) -> futures::future::BoxFuture<'static, Result<R>>
            + Send
            + 'static,
        R: Send + 'static,
    {
        let tikv = self.tikv.clone();
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| TikvError::Other(format!("Failed to create runtime: {}", e)))?;
        rt.block_on(f(tikv))
    }

    /// Load a checkpoint by job ID.
    ///
    /// Returns None if no checkpoint exists.
    pub fn load(&self, job_id: &str) -> Result<Option<CheckpointState>> {
        let job_id = job_id.to_string();
        self.block_on(|tikv| Box::pin(async move { tikv.get_checkpoint(&job_id).await }))
    }

    /// Save a checkpoint.
    ///
    /// This updates the checkpoint in TiKV with the current state.
    pub fn save(&self, checkpoint: &CheckpointState) -> Result<()> {
        let checkpoint = checkpoint.clone();
        self.block_on(|tikv| Box::pin(async move { tikv.update_checkpoint(&checkpoint).await }))
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
        let checkpoint = checkpoint.clone();
        let pod_id = pod_id.to_string();
        self.block_on(move |tikv| {
            Box::pin(async move {
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
        let job_id = job_id.to_string();
        self.block_on(|tikv| {
            Box::pin(async move {
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
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // Helper functions for testing without a real client
    fn should_checkpoint_impl(
        frames_since_last: u64,
        time_since_last: Duration,
        config: &CheckpointConfig,
    ) -> bool {
        frames_since_last >= config.checkpoint_interval_frames
            || time_since_last.as_secs() >= config.checkpoint_interval_seconds
    }

    fn next_checkpoint_frame_impl(current_frame: u64, config: &CheckpointConfig) -> u64 {
        ((current_frame / config.checkpoint_interval_frames) + 1)
            * config.checkpoint_interval_frames
    }

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
}
