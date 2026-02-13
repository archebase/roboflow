// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Checkpoint configuration for frame-level progress tracking.

use std::time::Duration;

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

    /// Check if a checkpoint should be saved based on configuration.
    ///
    /// Returns true if either:
    /// - Frames since last checkpoint >= checkpoint_interval_frames
    /// - Time since last checkpoint >= checkpoint_interval_seconds
    pub fn should_checkpoint(&self, frames_since_last: u64, time_since_last: Duration) -> bool {
        frames_since_last >= self.checkpoint_interval_frames
            || time_since_last.as_secs() >= self.checkpoint_interval_seconds
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
        assert!(config.should_checkpoint(100, Duration::from_secs(5)));

        // Should checkpoint when time interval reached
        assert!(config.should_checkpoint(50, Duration::from_secs(10)));

        // Should not checkpoint when neither threshold reached
        assert!(!config.should_checkpoint(50, Duration::from_secs(5)));

        // Should checkpoint when both thresholds reached
        assert!(config.should_checkpoint(100, Duration::from_secs(10)));
    }

    #[test]
    fn test_new_same_as_default() {
        let new_config = CheckpointConfig::new();
        let default_config = CheckpointConfig::default();
        assert_eq!(
            new_config.checkpoint_interval_frames,
            default_config.checkpoint_interval_frames
        );
        assert_eq!(
            new_config.checkpoint_interval_seconds,
            default_config.checkpoint_interval_seconds
        );
        assert_eq!(new_config.checkpoint_async, default_config.checkpoint_async);
    }

    #[test]
    fn test_should_checkpoint_zero_frames() {
        let config = CheckpointConfig::default();
        // Zero frames, but time threshold not reached
        assert!(!config.should_checkpoint(0, Duration::from_secs(5)));
    }

    #[test]
    fn test_should_checkpoint_exact_threshold() {
        let config = CheckpointConfig::new()
            .with_frame_interval(50)
            .with_time_interval(5);

        // Exactly at frame threshold
        assert!(config.should_checkpoint(50, Duration::from_secs(0)));

        // Exactly at time threshold
        assert!(config.should_checkpoint(0, Duration::from_secs(5)));
    }

    #[test]
    fn test_config_clone() {
        let config = CheckpointConfig::new()
            .with_frame_interval(150)
            .with_time_interval(20);
        let cloned = config.clone();
        assert_eq!(config.checkpoint_interval_frames, cloned.checkpoint_interval_frames);
        assert_eq!(config.checkpoint_interval_seconds, cloned.checkpoint_interval_seconds);
    }

    #[test]
    fn test_config_debug() {
        let config = CheckpointConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("CheckpointConfig"));
    }
}
