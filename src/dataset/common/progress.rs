// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Progress monitoring for dataset conversion.
//!
//! Provides a channel-based progress reporting system that allows
//! long-running dataset conversions to report progress to Python
//! without blocking the conversion thread.

use crate::dataset::common::WriterStats;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Progress update sent from Rust conversion thread to Python.
///
/// These updates are sent through a non-blocking channel, ensuring
/// that progress reporting doesn't slow down the conversion process.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ProgressUpdate {
    /// Conversion started
    Started {
        /// Input file being processed
        input_file: String,
        /// Estimated total frames (if known)
        estimated_frames: Option<u64>,
    },

    /// Frame processing progress
    FrameProgress {
        /// Number of frames processed so far
        frames_processed: u64,
        /// Estimated total frames
        estimated_total: u64,
        /// Processing speed in frames per second
        fps: f64,
        /// Estimated time remaining
        eta: Duration,
    },

    /// Video encoding progress for a specific camera
    VideoProgress {
        /// Camera name/feature
        camera: String,
        /// Frame number being encoded
        frame: u64,
        /// Total frames for this camera
        total: u64,
    },

    /// Parquet write progress
    ParquetProgress {
        /// Shard number being written
        shard: u32,
        /// Frames in current shard
        frames_written: u64,
    },

    /// Warning message (non-fatal issue)
    Warning {
        /// Warning category
        category: String,
        /// Warning message
        message: String,
        /// Additional context
        context: String,
    },

    /// Error occurred
    Error {
        /// Error message
        message: String,
        /// Whether the error is recoverable
        recoverable: bool,
    },

    /// Conversion completed successfully
    Completed {
        /// Final statistics
        stats: WriterStats,
    },
}

impl ProgressUpdate {
    /// Create a validated frame progress update.
    ///
    /// # Panics
    ///
    /// Panics if `fps < 0` or `frames_processed > estimated_total`.
    pub fn frame_progress(
        frames_processed: u64,
        estimated_total: u64,
        fps: f64,
        eta: Duration,
    ) -> Self {
        assert!(fps >= 0.0, "fps must be non-negative, got {}", fps);
        assert!(
            frames_processed <= estimated_total,
            "frames_processed ({}) must not exceed estimated_total ({})",
            frames_processed,
            estimated_total
        );
        ProgressUpdate::FrameProgress {
            frames_processed,
            estimated_total,
            fps,
            eta,
        }
    }

    /// Create a validated video progress update.
    ///
    /// # Panics
    ///
    /// Panics if `frame > total`.
    pub fn video_progress(camera: String, frame: u64, total: u64) -> Self {
        assert!(
            frame <= total,
            "video frame ({}) must not exceed total ({})",
            frame,
            total
        );
        ProgressUpdate::VideoProgress {
            camera,
            frame,
            total,
        }
    }

    /// Get the percentage complete (0-100) for this update.
    pub fn percent_complete(&self) -> Option<f64> {
        match self {
            ProgressUpdate::FrameProgress {
                frames_processed,
                estimated_total,
                ..
            } => {
                if *estimated_total > 0 {
                    Some((*frames_processed as f64 / *estimated_total as f64) * 100.0)
                } else {
                    None
                }
            }
            ProgressUpdate::VideoProgress { frame, total, .. } => {
                if *total > 0 {
                    Some((*frame as f64 / *total as f64) * 100.0)
                } else {
                    None
                }
            }
            ProgressUpdate::Started { .. } => Some(0.0),
            ProgressUpdate::Completed { .. } => Some(100.0),
            _ => None,
        }
    }

    /// Get the number of frames processed.
    pub fn frames_processed(&self) -> Option<u64> {
        match self {
            ProgressUpdate::FrameProgress {
                frames_processed, ..
            } => Some(*frames_processed),
            ProgressUpdate::Completed { stats } => Some(stats.frames_written as u64),
            _ => None,
        }
    }

    /// Get the estimated total frames.
    pub fn estimated_total(&self) -> Option<u64> {
        match self {
            ProgressUpdate::FrameProgress {
                estimated_total, ..
            } => Some(*estimated_total),
            ProgressUpdate::Started {
                estimated_frames, ..
            } => *estimated_frames,
            _ => None,
        }
    }

    /// Check if this is a completion update.
    pub fn is_complete(&self) -> bool {
        matches!(self, ProgressUpdate::Completed { .. })
    }

    /// Check if this is an error update.
    pub fn is_error(&self) -> bool {
        matches!(self, ProgressUpdate::Error { .. })
    }

    /// Check if this is a warning update.
    pub fn is_warning(&self) -> bool {
        matches!(self, ProgressUpdate::Warning { .. })
    }

    /// Get the variant type as a string.
    pub fn variant_type(&self) -> &'static str {
        match self {
            ProgressUpdate::Started { .. } => "started",
            ProgressUpdate::FrameProgress { .. } => "frame_progress",
            ProgressUpdate::VideoProgress { .. } => "video_progress",
            ProgressUpdate::ParquetProgress { .. } => "parquet_progress",
            ProgressUpdate::Warning { .. } => "warning",
            ProgressUpdate::Error { .. } => "error",
            ProgressUpdate::Completed { .. } => "completed",
        }
    }
}

/// Thread-safe sender for progress updates.
///
/// Uses non-blocking sends to ensure that slow Python receivers
/// don't block the conversion thread.
pub struct ProgressSender {
    sender: crossbeam_channel::Sender<ProgressUpdate>,
}

impl ProgressSender {
    /// Create a new progress sender with a bounded channel.
    ///
    /// Capacity should be large enough to buffer updates during brief
    /// receiver pauses. Default to 1000 for TB-scale conversions.
    pub fn new(capacity: usize) -> (Self, ProgressReceiver) {
        let (sender, receiver) = crossbeam_channel::bounded(capacity);
        (Self { sender }, ProgressReceiver { receiver })
    }

    /// Send a progress update (non-blocking for progress updates).
    ///
    /// Progress updates (FrameProgress, VideoProgress, ParquetProgress, Warning)
    /// are dropped if the channel is full to avoid blocking the conversion thread.
    ///
    /// Critical updates (Error, Completed) are sent with a timeout
    /// to ensure delivery.
    pub fn send(&self, update: ProgressUpdate) {
        // Check if this is a critical update without moving it
        let is_critical = matches!(
            &update,
            ProgressUpdate::Error { .. } | ProgressUpdate::Completed { .. }
        );

        if is_critical {
            use std::time::Duration;
            // For critical updates, try with increasing timeouts
            let mut sent = false;

            // Try multiple timeouts to avoid indefinite hanging
            for timeout in [100, 500, 5000].map(Duration::from_millis) {
                if self.sender.send_timeout(update.clone(), timeout).is_ok() {
                    sent = true;
                    break;
                }
            }

            if !sent {
                // Channel receiver may be dead or blocked - log but don't hang indefinitely
                eprintln!("CRITICAL: Progress channel receiver unresponsive - critical update may be lost");
                eprintln!("  Update type: {:?}", update.variant_type());
                // Don't block - the conversion should continue even if Python receiver is dead
                // The final stats will still be written to disk
            }
        } else {
            // Non-critical updates - drop if channel full
            let _ = self.sender.try_send(update);
        }
    }

    /// Send the started update.
    pub fn started(&self, input_file: String, estimated_frames: Option<u64>) {
        self.send(ProgressUpdate::Started {
            input_file,
            estimated_frames,
        });
    }

    /// Send a frame progress update.
    pub fn frame_progress(
        &self,
        frames_processed: u64,
        estimated_total: u64,
        fps: f64,
        eta: Duration,
    ) {
        self.send(ProgressUpdate::FrameProgress {
            frames_processed,
            estimated_total,
            fps,
            eta,
        });
    }

    /// Send a video progress update.
    pub fn video_progress(&self, camera: String, frame: u64, total: u64) {
        self.send(ProgressUpdate::VideoProgress {
            camera,
            frame,
            total,
        });
    }

    /// Send a warning.
    pub fn warning(&self, category: String, message: String, context: String) {
        self.send(ProgressUpdate::Warning {
            category,
            message,
            context,
        });
    }

    /// Send an error (critical - guaranteed delivery).
    pub fn error(&self, message: String, recoverable: bool) {
        self.send(ProgressUpdate::Error {
            message,
            recoverable,
        });
    }

    /// Send completion with stats (critical - guaranteed delivery).
    pub fn completed(&self, stats: WriterStats) {
        self.send(ProgressUpdate::Completed { stats });
    }
}

impl Clone for ProgressSender {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

/// Receiver for progress updates (typically held by Python).
pub struct ProgressReceiver {
    receiver: crossbeam_channel::Receiver<ProgressUpdate>,
}

impl ProgressReceiver {
    /// Try to receive a progress update without blocking.
    pub fn try_recv(&self) -> Option<ProgressUpdate> {
        self.receiver.try_recv().ok()
    }

    /// Receive a progress update, blocking until available.
    pub fn recv(&self) -> Result<ProgressUpdate, crossbeam_channel::RecvError> {
        self.receiver.recv()
    }

    /// Receive with a timeout.
    pub fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<ProgressUpdate, crossbeam_channel::RecvTimeoutError> {
        self.receiver.recv_timeout(timeout)
    }

    /// Drain all pending updates, returning the most recent one.
    ///
    /// This is useful for Python polling where we only care about
    /// the latest state rather than every intermediate update.
    pub fn latest(&self) -> Option<ProgressUpdate> {
        let mut latest = None;
        while let Ok(update) = self.receiver.try_recv() {
            latest = Some(update);
        }
        latest
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_channel() {
        let (sender, receiver) = ProgressSender::new(10);

        sender.started("test.bag".to_string(), Some(1000));

        let update = receiver.try_recv();
        assert!(update.is_some());

        if let Some(ProgressUpdate::Started { input_file, .. }) = update {
            assert_eq!(input_file, "test.bag");
        } else {
            panic!("Expected Started update");
        }
    }

    #[test]
    fn test_percent_complete() {
        let update = ProgressUpdate::FrameProgress {
            frames_processed: 500,
            estimated_total: 1000,
            fps: 30.0,
            eta: Duration::from_secs(16),
        };

        assert_eq!(update.percent_complete(), Some(50.0));
    }

    #[test]
    fn test_latest() {
        let (sender, receiver) = ProgressSender::new(10);

        sender.frame_progress(100, 1000, 30.0, Duration::from_secs(30));
        sender.frame_progress(200, 1000, 30.0, Duration::from_secs(26));
        sender.frame_progress(300, 1000, 30.0, Duration::from_secs(23));

        let latest = receiver.latest();
        assert_eq!(latest.unwrap().frames_processed(), Some(300));
    }

    #[test]
    fn test_non_blocking_send() {
        let (sender, receiver) = ProgressSender::new(1);

        // Fill the channel
        sender.started("test.bag".to_string(), None);

        // This should be dropped (channel full), not block
        sender.started("test2.bag".to_string(), None);

        // Should only have one message
        assert!(receiver.latest().is_some());
        assert!(receiver.latest().is_none());
    }
}
