// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Statistics tracking for frame alignment.

use std::time::Duration;

/// Statistics collected during frame alignment.
#[derive(Debug, Clone)]
pub struct AlignmentStats {
    /// Total number of frames processed
    pub frames_processed: usize,

    /// Number of frames completed normally (all required features received)
    pub normal_completions: usize,

    /// Number of frames force-completed (time window expired)
    pub force_completions: usize,

    /// Peak buffer size (maximum number of active frames)
    pub peak_buffer_size: usize,

    /// Total time spent aligning (milliseconds)
    pub total_alignment_time_ms: f64,

    /// Start time for duration tracking
    start_time: std::time::Instant,
}

impl AlignmentStats {
    /// Create new empty stats.
    pub fn new() -> Self {
        Self {
            frames_processed: 0,
            normal_completions: 0,
            force_completions: 0,
            peak_buffer_size: 0,
            total_alignment_time_ms: 0.0,
            start_time: std::time::Instant::now(),
        }
    }

    /// Record a normal frame completion.
    pub fn record_normal_completion(&mut self) {
        self.normal_completions += 1;
        self.frames_processed += 1;
    }

    /// Record a forced frame completion.
    pub fn record_force_completion(&mut self) {
        self.force_completions += 1;
        self.frames_processed += 1;
    }

    /// Update the peak buffer size.
    pub fn update_peak_buffer(&mut self, current_size: usize) {
        if current_size > self.peak_buffer_size {
            self.peak_buffer_size = current_size;
        }
    }

    /// Add alignment time.
    pub fn add_alignment_time(&mut self, duration_ms: f64) {
        self.total_alignment_time_ms += duration_ms;
    }

    /// Get the total duration since stats creation.
    pub fn duration(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Calculate frames per second.
    pub fn fps(&self) -> f64 {
        let elapsed_secs = self.duration().as_secs_f64();
        if elapsed_secs > 0.0 {
            self.frames_processed as f64 / elapsed_secs
        } else {
            0.0
        }
    }

    /// Get the completion rate (normal / total).
    pub fn completion_rate(&self) -> f64 {
        if self.frames_processed > 0 {
            self.normal_completions as f64 / self.frames_processed as f64
        } else {
            1.0
        }
    }
}

impl Default for AlignmentStats {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_new() {
        let stats = AlignmentStats::new();
        assert_eq!(stats.frames_processed, 0);
        assert_eq!(stats.peak_buffer_size, 0);
    }

    #[test]
    fn test_record_completions() {
        let mut stats = AlignmentStats::new();
        stats.record_normal_completion();
        stats.record_normal_completion();
        stats.record_force_completion();

        assert_eq!(stats.frames_processed, 3);
        assert_eq!(stats.normal_completions, 2);
        assert_eq!(stats.force_completions, 1);
    }

    #[test]
    fn test_peak_buffer() {
        let mut stats = AlignmentStats::new();
        stats.update_peak_buffer(5);
        stats.update_peak_buffer(3);
        stats.update_peak_buffer(10);

        assert_eq!(stats.peak_buffer_size, 10);
    }

    #[test]
    fn test_completion_rate() {
        let mut stats = AlignmentStats::new();
        stats.record_normal_completion();
        stats.record_force_completion();
        stats.record_normal_completion();

        // 2 normal, 1 forced = 2/3 = 0.666...
        assert!((stats.completion_rate() - 0.666).abs() < 0.01);
    }

    #[test]
    fn test_fps() {
        let mut stats = AlignmentStats::new();
        stats.record_normal_completion();
        stats.record_normal_completion();

        // Ensure some time has passed for FPS calculation
        std::thread::sleep(std::time::Duration::from_millis(10));

        // FPS should be very low but non-zero after recording frames
        let fps = stats.fps();
        assert!(
            fps > 0.0,
            "FPS should be positive after recording frames, got {}",
            fps
        );

        // With 2 frames in at least 10ms, FPS should be <= 200
        assert!(fps <= 200.0, "FPS should be reasonable, got {}", fps);
    }

    #[test]
    fn test_default_same_as_new() {
        let new_stats = AlignmentStats::new();
        let default_stats = AlignmentStats::default();
        assert_eq!(new_stats.frames_processed, default_stats.frames_processed);
        assert_eq!(
            new_stats.normal_completions,
            default_stats.normal_completions
        );
        assert_eq!(new_stats.force_completions, default_stats.force_completions);
        assert_eq!(new_stats.peak_buffer_size, default_stats.peak_buffer_size);
    }

    #[test]
    fn test_add_alignment_time() {
        let mut stats = AlignmentStats::new();
        stats.add_alignment_time(10.5);
        stats.add_alignment_time(5.5);
        assert!((stats.total_alignment_time_ms - 16.0).abs() < 0.01);
    }

    #[test]
    fn test_completion_rate_zero_frames() {
        let stats = AlignmentStats::new();
        // Completion rate should be 1.0 (100%) when no frames processed
        assert_eq!(stats.completion_rate(), 1.0);
    }

    #[test]
    fn test_duration() {
        let stats = AlignmentStats::new();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let duration = stats.duration();
        assert!(duration.as_millis() >= 10);
    }

    #[test]
    fn test_debug_impl() {
        let stats = AlignmentStats::new();
        let debug_str = format!("{:?}", stats);
        assert!(debug_str.contains("AlignmentStats"));
        assert!(debug_str.contains("frames_processed"));
    }

    #[test]
    fn test_clone() {
        let mut stats = AlignmentStats::new();
        stats.record_normal_completion();
        let cloned = stats.clone();
        assert_eq!(stats.frames_processed, cloned.frames_processed);
    }

    #[test]
    fn test_all_force_completions() {
        let mut stats = AlignmentStats::new();
        stats.record_force_completion();
        stats.record_force_completion();
        assert_eq!(stats.completion_rate(), 0.0);
    }
}
