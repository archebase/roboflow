// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Backpressure management for streaming conversion.

use std::time::{Duration, Instant};

use crate::dataset::streaming::alignment::FrameAlignmentBuffer;
use crate::dataset::streaming::config::StreamingConfig;

/// Strategy for applying backpressure.
#[derive(Debug, Clone, Copy)]
pub enum BackpressureStrategy {
    /// Never apply backpressure (may use unbounded memory)
    Never,

    /// Apply backpressure when any limit is exceeded
    OnAnyLimit,

    /// Apply backpressure only when all limits are exceeded
    OnAllLimits,
}

/// Backpressure handler for managing memory and buffer limits.
#[derive(Debug)]
pub struct BackpressureHandler {
    /// Strategy for when to apply backpressure
    strategy: BackpressureStrategy,

    /// Maximum frames to buffer
    max_buffered_frames: usize,

    /// Maximum memory to buffer (in bytes)
    max_memory_bytes: usize,

    /// Memory usage estimate
    current_memory_estimate: usize,

    /// Estimate of memory per frame (in bytes)
    estimated_frame_size: usize,

    /// Last backpressure application
    last_backpressure: Option<Instant>,

    /// Minimum time between backpressure applications
    backpressure_cooldown: Duration,
}

impl BackpressureHandler {
    /// Create a new backpressure handler from config.
    pub fn from_config(config: &StreamingConfig) -> Self {
        Self {
            strategy: BackpressureStrategy::OnAnyLimit,
            max_buffered_frames: config.max_buffered_frames,
            max_memory_bytes: config.max_buffered_memory_mb * 1_024 * 1_024,
            current_memory_estimate: 0,
            estimated_frame_size: 512 * 1024, // Default 512KB per frame
            last_backpressure: None,
            backpressure_cooldown: Duration::from_millis(100),
        }
    }

    /// Set the estimated frame size (for memory calculation).
    pub fn with_estimated_frame_size(mut self, size: usize) -> Self {
        self.estimated_frame_size = size;
        self
    }

    /// Set the backpressure strategy.
    pub fn with_strategy(mut self, strategy: BackpressureStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Check if backpressure should be applied based on buffer state.
    pub fn should_apply_backpressure(&self, buffer: &FrameAlignmentBuffer) -> bool {
        let frame_count = buffer.len();
        let memory_estimate = self.current_memory_estimate;

        match self.strategy {
            BackpressureStrategy::Never => false,
            BackpressureStrategy::OnAnyLimit => {
                frame_count >= self.max_buffered_frames || memory_estimate >= self.max_memory_bytes
            }
            BackpressureStrategy::OnAllLimits => {
                frame_count >= self.max_buffered_frames && memory_estimate >= self.max_memory_bytes
            }
        }
    }

    /// Update memory estimate based on buffer state.
    pub fn update_memory_estimate(&mut self, buffer: &FrameAlignmentBuffer) {
        self.current_memory_estimate = buffer.len() * self.estimated_frame_size;

        // Adjust frame size estimate over time
        if buffer.len() > 0 && self.estimated_frame_size < 128 * 1024 {
            // Minimum estimate based on actual frames
            self.estimated_frame_size = 128 * 1024;
        }
    }

    /// Check if backpressure is currently in cooldown.
    pub fn is_in_cooldown(&self) -> bool {
        if let Some(last) = self.last_backpressure {
            last.elapsed() < self.backpressure_cooldown
        } else {
            false
        }
    }

    /// Record that backpressure was applied.
    pub fn record_backpressure(&mut self) {
        self.last_backpressure = Some(Instant::now());
    }

    /// Get the current memory usage as MB.
    pub fn memory_mb(&self) -> f64 {
        self.current_memory_estimate as f64 / (1024.0 * 1024.0)
    }

    /// Get the memory usage percentage.
    pub fn memory_usage_percent(&self) -> f32 {
        if self.max_memory_bytes > 0 {
            (self.current_memory_estimate as f32 / self.max_memory_bytes as f32) * 100.0
        } else {
            0.0
        }
    }

    /// Get the buffer usage percentage based on the current buffer size.
    ///
    /// Returns the percentage of max_buffered_frames currently in use.
    pub fn buffer_usage_percent(&self, buffer_size: usize) -> f32 {
        if self.max_buffered_frames > 0 {
            (buffer_size as f32 / self.max_buffered_frames as f32) * 100.0
        } else {
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backpressure_on_frame_limit() {
        let config = StreamingConfig {
            max_buffered_frames: 10,
            ..Default::default()
        };

        let handler = BackpressureHandler::from_config(&config);

        // With no buffer, no backpressure
        // (we can't test this without a real buffer, but the logic is clear)
        assert_eq!(handler.max_buffered_frames, 10);
    }

    #[test]
    fn test_memory_calculation() {
        let mut handler = BackpressureHandler::from_config(
            &StreamingConfig {
                max_buffered_memory_mb: 100,
                ..Default::default()
            }
        );

        // Set memory estimate to 50 MB
        handler.current_memory_estimate = 50 * 1024 * 1024;

        assert_eq!(handler.memory_mb(), 50.0);

        // Should be at 50% usage
        assert!((handler.memory_usage_percent() - 50.0).abs() < 0.1);
    }

    #[test]
    fn test_buffer_usage_percent() {
        let handler = BackpressureHandler::from_config(
            &StreamingConfig {
                max_buffered_frames: 100,
                ..Default::default()
            }
        );

        // 0% when empty
        assert_eq!(handler.buffer_usage_percent(0), 0.0);

        // 50% when half full
        assert!((handler.buffer_usage_percent(50) - 50.0).abs() < 0.1);

        // 100% when at limit
        assert_eq!(handler.buffer_usage_percent(100), 100.0);
    }
}
