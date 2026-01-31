// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Statistics and monitoring for streaming conversion.

use crate::common::WriterStats;

/// Statistics from streaming conversion.
#[derive(Debug, Clone, Default)]
pub struct StreamingStats {
    /// Total frames written
    pub frames_written: usize,

    /// Total messages processed
    pub messages_processed: usize,

    /// Messages dropped (late/unknown topic)
    pub messages_dropped: usize,

    /// Frames force-completed due to timeout
    pub force_completed_frames: usize,

    /// Average buffer size during conversion
    pub avg_buffer_size: f32,

    /// Peak memory usage (MB)
    pub peak_memory_mb: f64,

    /// Processing time (seconds)
    pub duration_sec: f64,

    /// Writer statistics
    pub writer_stats: WriterStats,
}

impl StreamingStats {
    /// Calculate throughput in frames per second.
    pub fn throughput_fps(&self) -> f64 {
        if self.duration_sec > 0.0 {
            self.frames_written as f64 / self.duration_sec
        } else {
            0.0
        }
    }

    /// Calculate average messages per second.
    pub fn message_throughput(&self) -> f64 {
        if self.duration_sec > 0.0 {
            self.messages_processed as f64 / self.duration_sec
        } else {
            0.0
        }
    }
}

/// Alignment-specific statistics.
#[derive(Debug, Clone, Default)]
pub struct AlignmentStats {
    /// Frames completed normally (all required features received)
    pub normal_completions: usize,

    /// Frames force-completed (completion window expired)
    pub force_completions: usize,

    /// Late messages received (after frame was written)
    pub late_messages: usize,

    /// Messages with unknown/unmapped topics
    pub unmapped_messages: usize,

    /// Average time frames spent in buffer (milliseconds)
    pub avg_buffer_time_ms: f64,

    /// Peak buffer size during conversion
    pub peak_buffer_size: usize,
}

impl AlignmentStats {
    /// Create a new alignment stats tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a normal completion.
    pub fn record_normal_completion(&mut self) {
        self.normal_completions += 1;
    }

    /// Record a force completion.
    pub fn record_force_completion(&mut self) {
        self.force_completions += 1;
    }

    /// Record a late message.
    pub fn record_late_message(&mut self) {
        self.late_messages += 1;
    }

    /// Record an unmapped message.
    pub fn record_unmapped_message(&mut self) {
        self.unmapped_messages += 1;
    }

    /// Update the peak buffer size.
    pub fn update_peak_buffer(&mut self, size: usize) {
        if size > self.peak_buffer_size {
            self.peak_buffer_size = size;
        }
    }

    /// Calculate the completion rate (normal / total).
    pub fn completion_rate(&self) -> f64 {
        let total = self.normal_completions + self.force_completions;
        if total > 0 {
            self.normal_completions as f64 / total as f64
        } else {
            1.0
        }
    }

    /// Get total completions (normal + force).
    pub fn total_completions(&self) -> usize {
        self.normal_completions + self.force_completions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_throughput_calculation() {
        let stats = StreamingStats {
            frames_written: 3000,
            duration_sec: 10.0,
            ..Default::default()
        };

        assert!((stats.throughput_fps() - 300.0).abs() < 0.1);
    }

    #[test]
    fn test_completion_rate() {
        let mut stats = AlignmentStats::new();
        stats.record_normal_completion();
        stats.record_normal_completion();
        stats.record_force_completion();

        // 2 normal, 1 force = 67% normal completion rate
        assert!((stats.completion_rate() - 0.667).abs() < 0.01);
    }

    #[test]
    fn test_peak_buffer_tracking() {
        let mut stats = AlignmentStats::new();

        stats.update_peak_buffer(5);
        assert_eq!(stats.peak_buffer_size, 5);

        stats.update_peak_buffer(3); // No change
        assert_eq!(stats.peak_buffer_size, 5);

        stats.update_peak_buffer(10);
        assert_eq!(stats.peak_buffer_size, 10);
    }
}
