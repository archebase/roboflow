// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Statistics types for tracking writer performance.

use std::time::Duration;

/// Statistics about a completed episode.
#[derive(Debug, Clone, Default)]
pub struct EpisodeStats {
    /// Number of frames in this episode.
    pub frames: usize,

    /// Number of images encoded.
    pub images_encoded: usize,

    /// Total bytes written.
    pub bytes_written: u64,

    /// Duration of episode processing.
    pub duration: Duration,

    /// Episode index.
    pub episode_index: usize,

    /// Task index (if applicable).
    pub task_index: Option<usize>,

    /// Video files created (camera -> file path).
    pub video_files: Vec<(String, String)>,

    /// Parquet file path (if written).
    pub parquet_path: Option<String>,
}

impl EpisodeStats {
    /// Create empty episode stats.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create stats for a specific episode.
    pub fn for_episode(episode_index: usize) -> Self {
        Self {
            episode_index,
            ..Self::default()
        }
    }

    /// Get frames per second.
    pub fn fps(&self) -> f64 {
        if self.duration.as_secs_f64() > 0.0 {
            self.frames as f64 / self.duration.as_secs_f64()
        } else {
            0.0
        }
    }

    /// Get megabytes written per second.
    pub fn mb_per_sec(&self) -> f64 {
        if self.duration.as_secs_f64() > 0.0 {
            (self.bytes_written as f64 / 1_048_576.0) / self.duration.as_secs_f64()
        } else {
            0.0
        }
    }
}

/// Statistics about a completed write operation.
#[derive(Debug, Clone, Default)]
pub struct WriterStats {
    /// Total frames written.
    pub frames_written: usize,

    /// Total images encoded.
    pub images_encoded: usize,

    /// Number of state records written.
    pub state_records: usize,

    /// Total bytes written to storage.
    pub output_bytes: u64,

    /// Total duration of the write operation.
    pub duration: Duration,

    /// Number of episodes written.
    pub episodes_written: usize,

    /// Per-episode statistics.
    pub episode_stats: Vec<EpisodeStats>,
}

impl WriterStats {
    /// Create new empty statistics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get overall frames per second.
    pub fn fps(&self) -> f64 {
        if self.duration.as_secs_f64() > 0.0 {
            self.frames_written as f64 / self.duration.as_secs_f64()
        } else {
            0.0
        }
    }

    /// Get megabytes written per second.
    pub fn mb_per_sec(&self) -> f64 {
        if self.duration.as_secs_f64() > 0.0 {
            (self.output_bytes as f64 / 1_048_576.0) / self.duration.as_secs_f64()
        } else {
            0.0
        }
    }

    /// Merge another stats into this one.
    pub fn merge(&mut self, other: &WriterStats) {
        self.frames_written += other.frames_written;
        self.images_encoded += other.images_encoded;
        self.state_records += other.state_records;
        self.output_bytes += other.output_bytes;
        self.episodes_written += other.episodes_written;
        self.episode_stats
            .extend(other.episode_stats.iter().cloned());

        // Duration is the maximum of the two
        if other.duration > self.duration {
            self.duration = other.duration;
        }
    }

    /// Add episode stats to the total.
    pub fn add_episode(&mut self, episode: EpisodeStats) {
        self.frames_written += episode.frames;
        self.images_encoded += episode.images_encoded;
        self.output_bytes += episode.bytes_written;
        self.episodes_written += 1;
        self.episode_stats.push(episode);
    }
}

/// Real-time statistics for monitoring progress.
#[derive(Debug, Clone, Default)]
pub struct ProgressStats {
    /// Current frame being processed.
    pub current_frame: usize,

    /// Total frames expected (if known).
    pub total_frames: Option<usize>,

    /// Current episode being processed.
    pub current_episode: usize,

    /// Frames processed in the current episode.
    pub episode_frames: usize,

    /// Bytes written so far.
    pub bytes_written: u64,

    /// Time elapsed since start.
    pub elapsed: Duration,

    /// Estimated time remaining.
    pub estimated_remaining: Option<Duration>,
}

impl ProgressStats {
    /// Calculate progress percentage (0.0 to 1.0).
    pub fn progress(&self) -> Option<f64> {
        self.total_frames.map(|total| {
            if total > 0 {
                self.current_frame as f64 / total as f64
            } else {
                0.0
            }
        })
    }

    /// Get current processing rate in frames per second.
    pub fn fps(&self) -> f64 {
        if self.elapsed.as_secs_f64() > 0.0 {
            self.current_frame as f64 / self.elapsed.as_secs_f64()
        } else {
            0.0
        }
    }

    /// Estimate time remaining based on current rate.
    pub fn estimate_remaining(&self) -> Option<Duration> {
        let fps = self.fps();
        if fps > 0.0 {
            self.total_frames.map(|total| {
                let remaining_frames = total.saturating_sub(self.current_frame);
                Duration::from_secs_f64(remaining_frames as f64 / fps)
            })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_episode_stats_fps() {
        let stats = EpisodeStats {
            frames: 100,
            duration: Duration::from_secs(10),
            ..Default::default()
        };
        assert!((stats.fps() - 10.0).abs() < 0.01);
    }

    #[test]
    fn test_episode_stats_zero_duration() {
        let stats = EpisodeStats {
            frames: 100,
            duration: Duration::ZERO,
            ..Default::default()
        };
        assert_eq!(stats.fps(), 0.0);
    }

    #[test]
    fn test_writer_stats_merge() {
        let mut stats1 = WriterStats {
            frames_written: 100,
            images_encoded: 50,
            state_records: 100,
            output_bytes: 1024,
            episodes_written: 1,
            duration: Duration::from_secs(10),
            episode_stats: vec![EpisodeStats::for_episode(0)],
        };

        let stats2 = WriterStats {
            frames_written: 200,
            images_encoded: 100,
            state_records: 200,
            output_bytes: 2048,
            episodes_written: 1,
            duration: Duration::from_secs(15),
            episode_stats: vec![EpisodeStats::for_episode(1)],
        };

        stats1.merge(&stats2);

        assert_eq!(stats1.frames_written, 300);
        assert_eq!(stats1.images_encoded, 150);
        assert_eq!(stats1.state_records, 300);
        assert_eq!(stats1.output_bytes, 3072);
        assert_eq!(stats1.episodes_written, 2);
        assert_eq!(stats1.episode_stats.len(), 2);
        assert_eq!(stats1.duration, Duration::from_secs(15));
    }

    #[test]
    fn test_writer_stats_add_episode() {
        let mut stats = WriterStats::new();
        let episode = EpisodeStats {
            frames: 100,
            images_encoded: 50,
            bytes_written: 1024,
            episode_index: 0,
            ..Default::default()
        };

        stats.add_episode(episode);

        assert_eq!(stats.frames_written, 100);
        assert_eq!(stats.images_encoded, 50);
        assert_eq!(stats.output_bytes, 1024);
        assert_eq!(stats.episodes_written, 1);
        assert_eq!(stats.episode_stats.len(), 1);
    }

    #[test]
    fn test_progress_stats() {
        let progress = ProgressStats {
            current_frame: 50,
            total_frames: Some(100),
            current_episode: 0,
            episode_frames: 50,
            bytes_written: 1024,
            elapsed: Duration::from_secs(5),
            estimated_remaining: None,
        };

        assert!((progress.progress().unwrap() - 0.5).abs() < 0.01);
        assert!((progress.fps() - 10.0).abs() < 0.01);
    }
}
