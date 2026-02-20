// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Pipeline executor shared components.
//!
//! This module provides common types and utilities used by both
//! PipelineExecutor and ParallelPipelineExecutor to avoid code duplication.

use std::collections::HashMap;
use std::time::Instant;

use roboflow_core::TimestampedMessage;

use crate::formats::common::AlignedFrame;

/// Statistics for pipeline execution.
#[derive(Debug, Default)]
pub struct ExecutorStats {
    pub messages_processed: usize,
    pub frames_written: usize,
    pub episodes_written: usize,
    pub processing_time_sec: f64,
}

/// State maintained during pipeline execution.
#[derive(Debug)]
pub struct ExecutorState {
    /// Message buffer: timestamp_ns -> Vec<TimestampedMessage>
    pub message_buffer: HashMap<u64, Vec<TimestampedMessage>>,
    /// Current timestamp being processed
    pub current_timestamp_ns: Option<u64>,
    /// End timestamp of buffered data
    pub end_timestamp_ns: Option<u64>,
    /// Current episode index
    pub episode_index: usize,
    /// Current frame index within episode
    pub frame_index: usize,
    /// Start time
    pub start_time: Instant,
    /// Camera info topics we've already processed
    pub processed_camera_info: std::collections::HashSet<String>,
    /// Whether current episode has been started
    pub current_episode_started: bool,
    /// Frames written in current episode
    pub frames_in_current_episode: usize,
    /// Last message timestamp for gap detection
    pub last_timestamp_ns: Option<u64>,
    /// Batch buffer for parallel processing (used by ParallelPipelineExecutor)
    pub pending_frames: Vec<AlignedFrame>,
    /// Batch size for parallel processing
    pub batch_size: usize,
}

impl ExecutorState {
    /// Create new executor state with default batch size
    pub fn new() -> Self {
        Self::with_batch_size(32)
    }

    /// Create new executor state with specified batch size
    pub fn with_batch_size(batch_size: usize) -> Self {
        Self {
            message_buffer: HashMap::new(),
            current_timestamp_ns: None,
            end_timestamp_ns: None,
            episode_index: 0,
            frame_index: 0,
            start_time: Instant::now(),
            processed_camera_info: std::collections::HashSet::new(),
            current_episode_started: false,
            frames_in_current_episode: 0,
            last_timestamp_ns: None,
            pending_frames: Vec::with_capacity(batch_size),
            batch_size,
        }
    }

    /// Reset state for new episode
    pub fn reset_episode(&mut self, episode_index: usize) {
        self.episode_index = episode_index;
        self.frame_index = 0;
        self.frames_in_current_episode = 0;
        self.current_episode_started = false;
        self.pending_frames.clear();
    }

    /// Check if batch is full
    pub fn is_batch_full(&self) -> bool {
        self.pending_frames.len() >= self.batch_size
    }
}

impl Default for ExecutorState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use robocodec::CodecValue;
    use roboflow_core::TimestampedMessage;

    #[test]
    fn test_executor_stats_default() {
        let stats = ExecutorStats::default();
        assert_eq!(stats.messages_processed, 0);
        assert_eq!(stats.frames_written, 0);
        assert_eq!(stats.episodes_written, 0);
        assert_eq!(stats.processing_time_sec, 0.0);
    }

    #[test]
    fn test_executor_stats_with_values() {
        let stats = ExecutorStats {
            messages_processed: 100,
            frames_written: 50,
            episodes_written: 5,
            processing_time_sec: 10.5,
        };
        assert_eq!(stats.messages_processed, 100);
        assert_eq!(stats.frames_written, 50);
        assert_eq!(stats.episodes_written, 5);
        assert_eq!(stats.processing_time_sec, 10.5);
    }

    #[test]
    fn test_executor_state_default() {
        let state = ExecutorState::default();
        assert!(state.message_buffer.is_empty());
        assert_eq!(state.current_timestamp_ns, None);
        assert_eq!(state.end_timestamp_ns, None);
        assert_eq!(state.episode_index, 0);
        assert_eq!(state.frame_index, 0);
        assert!(state.processed_camera_info.is_empty());
        assert!(!state.current_episode_started);
        assert_eq!(state.frames_in_current_episode, 0);
        assert_eq!(state.last_timestamp_ns, None);
        assert!(state.pending_frames.is_empty());
        assert_eq!(state.batch_size, 32);
    }

    #[test]
    fn test_executor_state_new() {
        let state = ExecutorState::new();
        assert_eq!(state.batch_size, 32);
        assert!(state.pending_frames.capacity() >= 32);
    }

    #[test]
    fn test_executor_state_with_batch_size() {
        let state = ExecutorState::with_batch_size(64);
        assert_eq!(state.batch_size, 64);
        assert!(state.pending_frames.capacity() >= 64);
    }

    #[test]
    fn test_executor_state_with_batch_size_zero() {
        let state = ExecutorState::with_batch_size(0);
        assert_eq!(state.batch_size, 0);
    }

    #[test]
    fn test_reset_episode() {
        let mut state = ExecutorState::new();

        // Set some values
        state.episode_index = 5;
        state.frame_index = 100;
        state.frames_in_current_episode = 50;
        state.current_episode_started = true;
        state.pending_frames.push(AlignedFrame::new(0, 0));

        // Reset to episode 10
        state.reset_episode(10);

        assert_eq!(state.episode_index, 10);
        assert_eq!(state.frame_index, 0);
        assert_eq!(state.frames_in_current_episode, 0);
        assert!(!state.current_episode_started);
        assert!(state.pending_frames.is_empty());
    }

    #[test]
    fn test_is_batch_full() {
        let mut state = ExecutorState::with_batch_size(2);

        // Empty batch is not full
        assert!(!state.is_batch_full());

        // Add one frame
        state.pending_frames.push(AlignedFrame::new(0, 0));
        assert!(!state.is_batch_full());

        // Add second frame - now full
        state.pending_frames.push(AlignedFrame::new(1, 1));
        assert!(state.is_batch_full());

        // Add third frame - still full
        state.pending_frames.push(AlignedFrame::new(2, 2));
        assert!(state.is_batch_full());
    }

    #[test]
    fn test_is_batch_full_with_zero_batch_size() {
        let state = ExecutorState::with_batch_size(0);
        // With batch_size 0, is_batch_full returns true (0 >= 0)
        assert!(state.is_batch_full());
    }

    #[test]
    fn test_message_buffer_operations() {
        let mut state = ExecutorState::new();

        let msg = TimestampedMessage {
            topic: "/test".to_string(),
            log_time: 1000,
            data: CodecValue::UInt32(42),
        };

        // Insert message into buffer
        state.message_buffer.entry(1000).or_default().push(msg);

        assert_eq!(state.message_buffer.len(), 1);
        assert!(state.message_buffer.contains_key(&1000));
    }

    #[test]
    fn test_processed_camera_info() {
        let mut state = ExecutorState::new();

        // Insert a topic
        assert!(
            state
                .processed_camera_info
                .insert("/camera/info".to_string())
        );

        // Inserting same topic again returns false
        assert!(
            !state
                .processed_camera_info
                .insert("/camera/info".to_string())
        );

        // Check it exists
        assert!(state.processed_camera_info.contains("/camera/info"));
    }

    #[test]
    fn test_executor_state_debug() {
        let state = ExecutorState::new();
        let debug_str = format!("{:?}", state);
        assert!(debug_str.contains("ExecutorState"));
    }

    #[test]
    fn test_executor_stats_debug() {
        let stats = ExecutorStats::default();
        let debug_str = format!("{:?}", stats);
        assert!(debug_str.contains("ExecutorStats"));
    }
}
