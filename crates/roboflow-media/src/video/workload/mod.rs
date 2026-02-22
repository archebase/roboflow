// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Video encoding workload orchestration.
//!
//! This module provides the [`EncodingWorkload`] type for managing multiple
//! video encoding streams with configurable strategies (standard, fragment, streaming).
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────┐
//! │                    EncodingWorkload                                  │
//! ├─────────────────────────────────────────────────────────────────────┤
//! │                                                                     │
//! │  User Thread              Worker Threads                           │
//! │  ────────────             ──────────────                           │
//! │                                                                     │
//! │  submit_frame() ──────►  Frame Queue  ──────►  Encoder Pool       │
//! │                           (mpsc)              (per-stream)          │
//! │                                                                     │
//! │  finalize() ──────────►  Finalize Cmd ──────►  Stream Finalize    │
//! │                                                                     │
//! └─────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```ignore
//! use roboflow_media::video::{
//!     EncodingWorkload, WorkloadConfig, StreamConfig, StreamOutput,
//!     EncodingStrategy, FragmentTriggers, VideoEncoderConfig,
//! };
//!
//! // Create workload
//! let mut workload = EncodingWorkload::new(WorkloadConfig::default())?;
//!
//! // Add streams with different strategies
//! workload.add_stream(StreamConfig::file("main", "main.mp4")
//!     .with_strategy(EncodingStrategy::fragment_by_frames(300)))?;
//!
//! workload.add_stream(StreamConfig::file("aux", "aux.mp4"))?;  // Standard
//!
//! // Submit frames (thread-safe)
//! workload.submit_frame("main", &rgb_data, 640, 480)?;
//!
//! // Finalize all streams
//! let results = workload.finalize()?;
//! ```

mod strategy;
mod stream;

use std::collections::HashMap;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};

use roboflow_core::{Result, RoboflowError};
use tracing::{info, warn};

use crate::video::config::VideoEncoderConfig;
use crate::video::encoder::{OutputConfig, VideoEncoder};
use crate::video::fragment::{FragmentConfig, FragmentEncoder, FragmentOutputConfig};

pub use strategy::{EncodingStrategy, FragmentTriggers};
pub(crate) use stream::EncoderCommand;
pub use stream::{FrameData, StreamConfig, StreamId, StreamOutput, StreamResult};

/// Global encoder defaults for a workload.
#[derive(Debug, Clone, Default)]
pub struct EncoderDefaults {
    /// Default video encoder configuration.
    pub video_config: VideoEncoderConfig,
}

/// Configuration for an encoding workload.
#[derive(Debug, Clone, Default)]
pub struct WorkloadConfig {
    /// Global encoder defaults.
    pub defaults: EncoderDefaults,
    /// Thread pool size for encoding (None = CPU count).
    pub thread_pool_size: Option<usize>,
}

impl WorkloadConfig {
    /// Create a new workload configuration with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the thread pool size.
    pub fn with_thread_pool_size(mut self, size: usize) -> Self {
        self.thread_pool_size = Some(size);
        self
    }

    /// Set the default video encoder configuration.
    pub fn with_video_config(mut self, config: VideoEncoderConfig) -> Self {
        self.defaults.video_config = config;
        self
    }
}

/// Result from finalizing an encoding workload.
#[derive(Debug)]
pub struct WorkloadResult {
    /// Per-stream results.
    pub streams: HashMap<StreamId, StreamResult>,
    /// Total frames encoded across all streams.
    pub total_frames: u64,
    /// Total frames skipped across all streams.
    pub total_skipped: u64,
    /// Total bytes written across all streams.
    pub total_bytes: u64,
    /// Whether all streams succeeded.
    pub all_success: bool,
}

impl WorkloadResult {
    /// Check if all streams succeeded.
    pub fn is_success(&self) -> bool {
        self.all_success
    }

    /// Get a result for a specific stream.
    pub fn get(&self, id: &StreamId) -> Option<&StreamResult> {
        self.streams.get(id)
    }

    /// Get the number of streams.
    pub fn stream_count(&self) -> usize {
        self.streams.len()
    }
}

/// State for a single stream in the workload.
struct StreamState {
    /// Command sender for this stream's worker thread.
    cmd_tx: Sender<EncoderCommand>,
    /// Thread handle for joining on finalize.
    handle: Option<JoinHandle<Result<StreamResult>>>,
}

/// A video encoding workload with multiple output streams.
///
/// This is the primary entry point for parallel video encoding with
/// configurable memory strategies per stream.
///
/// # Thread Safety
///
/// `submit_frame()` can be called from any thread. Frame submission is
/// non-blocking and uses channels for communication with worker threads.
///
/// # Example
///
/// ```ignore
/// let mut workload = EncodingWorkload::new(WorkloadConfig::default())?;
/// workload.add_stream(StreamConfig::file("cam", "output.mp4"))?;
/// workload.submit_frame("cam", &rgb_data, 640, 480)?;
/// let results = workload.finalize()?;
/// ```
pub struct EncodingWorkload {
    /// Workload configuration.
    config: WorkloadConfig,
    /// Per-stream state.
    streams: HashMap<StreamId, StreamState>,
    /// Whether the workload has been finalized.
    finalized: bool,
}

impl EncodingWorkload {
    /// Create a new encoding workload.
    pub fn new(config: WorkloadConfig) -> Result<Self> {
        Ok(Self {
            config,
            streams: HashMap::new(),
            finalized: false,
        })
    }

    /// Add a stream to the workload.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A stream with the same ID already exists
    /// - The output path is invalid
    pub fn add_stream(&mut self, config: StreamConfig) -> Result<()> {
        if self.finalized {
            return Err(RoboflowError::encode(
                "EncodingWorkload",
                "Cannot add stream after finalization",
            ));
        }

        if self.streams.contains_key(&config.id) {
            return Err(RoboflowError::encode(
                "EncodingWorkload",
                format!("Stream '{}' already exists", config.id),
            ));
        }

        // Resolve video config
        let video_config = config
            .encoder_config
            .clone()
            .unwrap_or_else(|| self.config.defaults.video_config.clone());

        // Create channel for this stream
        let (cmd_tx, cmd_rx) = mpsc::channel();

        // Clone config for the thread
        let stream_id = config.id.clone();
        let stream_output = config.output.clone();
        let strategy = config.strategy.clone();

        // Spawn worker thread
        let handle = thread::Builder::new()
            .name(format!("encoder-{}", stream_id))
            .spawn(move || {
                Self::encoder_thread(stream_id, video_config, stream_output, strategy, cmd_rx)
            })
            .map_err(|e| {
                RoboflowError::encode(
                    "EncodingWorkload",
                    format!("Failed to spawn encoder thread: {}", e),
                )
            })?;

        self.streams.insert(
            config.id.clone(),
            StreamState {
                cmd_tx,
                handle: Some(handle),
            },
        );

        Ok(())
    }

    /// Submit a frame to a stream.
    ///
    /// This method is thread-safe and non-blocking.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The workload has been finalized
    /// - The stream ID doesn't exist
    /// - The channel is disconnected (worker thread died)
    pub fn submit_frame(
        &self,
        stream_id: &str,
        rgb_data: &[u8],
        width: u32,
        height: u32,
    ) -> Result<()> {
        if self.finalized {
            return Err(RoboflowError::encode(
                "EncodingWorkload",
                "Cannot submit frame after finalization",
            ));
        }

        let stream_id = StreamId::new(stream_id);
        let state = self.streams.get(&stream_id).ok_or_else(|| {
            RoboflowError::encode(
                "EncodingWorkload",
                format!("Stream '{}' not found", stream_id),
            )
        })?;

        let frame = FrameData::from_slice(rgb_data, width, height);
        state
            .cmd_tx
            .send(EncoderCommand::Frame(frame))
            .map_err(|_| {
                RoboflowError::encode(
                    "EncodingWorkload",
                    format!("Stream '{}' worker thread disconnected", stream_id),
                )
            })
    }

    /// Submit a frame using FrameData.
    pub fn submit_frame_data(&self, stream_id: &str, frame: FrameData) -> Result<()> {
        self.submit_frame(stream_id, &frame.rgb_data, frame.width, frame.height)
    }

    /// Get the number of streams in the workload.
    pub fn stream_count(&self) -> usize {
        self.streams.len()
    }

    /// Check if a stream exists.
    pub fn has_stream(&self, stream_id: &str) -> bool {
        self.streams.contains_key(&StreamId::new(stream_id))
    }

    /// Finalize all streams and get results.
    ///
    /// This method consumes the workload and returns results for all streams.
    pub fn finalize(mut self) -> Result<WorkloadResult> {
        if self.finalized {
            return Err(RoboflowError::encode(
                "EncodingWorkload",
                "Workload already finalized",
            ));
        }

        self.finalized = true;

        // Send finalize command to all streams
        let stream_ids: Vec<StreamId> = self.streams.keys().cloned().collect();

        for (id, state) in &self.streams {
            if let Err(e) = state.cmd_tx.send(EncoderCommand::Finalize) {
                warn!(stream = %id, error = %e, "Failed to send finalize command");
            }
        }

        // Collect results from all threads
        let mut results = HashMap::new();
        let mut total_frames = 0u64;
        let mut total_skipped = 0u64;
        let mut total_bytes = 0u64;
        let mut all_success = true;

        for id in stream_ids {
            if let Some(state) = self.streams.get_mut(&id)
                && let Some(handle) = state.handle.take()
            {
                match handle.join() {
                    Ok(Ok(result)) => {
                        total_frames += result.frames_encoded;
                        total_skipped += result.frames_skipped;
                        total_bytes += result.bytes_written;
                        if !result.success {
                            all_success = false;
                        }
                        info!(
                            stream = %result.id,
                            frames = result.frames_encoded,
                            skipped = result.frames_skipped,
                            bytes = result.bytes_written,
                            fragments = result.fragments,
                            success = result.success,
                            "Stream finalized"
                        );
                        results.insert(id.clone(), result);
                    }
                    Ok(Err(e)) => {
                        warn!(stream = %id, error = %e, "Stream encoding failed");
                        all_success = false;
                        results
                            .insert(id.clone(), StreamResult::failure(id.clone(), e.to_string()));
                    }
                    Err(e) => {
                        warn!(stream = %id, error = ?e, "Stream thread panicked");
                        all_success = false;
                        results.insert(
                            id.clone(),
                            StreamResult::failure(id.clone(), "Thread panicked"),
                        );
                    }
                }
            }
        }

        info!(
            streams = results.len(),
            total_frames = total_frames,
            total_skipped = total_skipped,
            total_bytes = total_bytes,
            all_success = all_success,
            "Workload finalized"
        );

        Ok(WorkloadResult {
            streams: results,
            total_frames,
            total_skipped,
            total_bytes,
            all_success,
        })
    }

    /// Encoder thread implementation.
    fn encoder_thread(
        stream_id: StreamId,
        video_config: VideoEncoderConfig,
        output: StreamOutput,
        strategy: EncodingStrategy,
        cmd_rx: Receiver<EncoderCommand>,
    ) -> Result<StreamResult> {
        let mut frames_encoded = 0u64;
        let mut frames_skipped = 0u64;

        // Create encoder based on strategy
        match (&output, &strategy) {
            (StreamOutput::File { path }, EncodingStrategy::Standard) => {
                // Standard encoder - buffer all frames
                let mut encoder =
                    VideoEncoder::new(video_config.clone(), OutputConfig::file(path))?;
                let mut dimensions: Option<(u32, u32)> = None;

                while let Ok(cmd) = cmd_rx.recv() {
                    match cmd {
                        EncoderCommand::Frame(frame) => {
                            // Validate dimensions consistency
                            if let Some((w, h)) = dimensions {
                                if frame.width != w || frame.height != h {
                                    warn!(
                                        stream = %stream_id,
                                        expected = format!("{}x{}", w, h),
                                        got = format!("{}x{}", frame.width, frame.height),
                                        "Frame dimension mismatch, skipping"
                                    );
                                    frames_skipped += 1;
                                    continue;
                                }
                            } else {
                                dimensions = Some((frame.width, frame.height));
                            }

                            match encoder.encode_frame(&frame.rgb_data, frame.width, frame.height) {
                                Ok(()) => frames_encoded += 1,
                                Err(e) => {
                                    warn!(stream = %stream_id, error = %e, "Failed to encode frame");
                                    frames_skipped += 1;
                                }
                            }
                        }
                        EncoderCommand::Finalize => break,
                    }
                }

                // Finalize
                match encoder.finalize() {
                    Ok(result) => Ok(StreamResult::success(
                        stream_id,
                        Some(path.clone()),
                        frames_encoded,
                        frames_skipped,
                        result.bytes_written,
                        1,
                    )),
                    Err(e) => Ok(StreamResult::failure(stream_id, e.to_string())),
                }
            }
            (StreamOutput::File { path }, EncodingStrategy::Fragment { triggers }) => {
                // Fragment encoder - bounded memory
                let fragment_config = FragmentConfig {
                    max_frames: triggers.frame_count,
                    max_memory_bytes: triggers.memory_bytes,
                    max_duration_secs: triggers.duration_secs,
                };

                let output_config = FragmentOutputConfig::SingleFile { path: path.clone() };

                let mut encoder =
                    FragmentEncoder::new(video_config.clone(), output_config, fragment_config)?;
                let mut dimensions: Option<(u32, u32)> = None;

                while let Ok(cmd) = cmd_rx.recv() {
                    match cmd {
                        EncoderCommand::Frame(frame) => {
                            // Validate dimensions consistency
                            if let Some((w, h)) = dimensions {
                                if frame.width != w || frame.height != h {
                                    warn!(
                                        stream = %stream_id,
                                        expected = format!("{}x{}", w, h),
                                        got = format!("{}x{}", frame.width, frame.height),
                                        "Frame dimension mismatch, skipping"
                                    );
                                    frames_skipped += 1;
                                    continue;
                                }
                            } else {
                                dimensions = Some((frame.width, frame.height));
                            }

                            match encoder.encode_frame(&frame.rgb_data, frame.width, frame.height) {
                                Ok(_) => frames_encoded += 1,
                                Err(e) => {
                                    warn!(stream = %stream_id, error = %e, "Failed to encode frame");
                                    frames_skipped += 1;
                                }
                            }
                        }
                        EncoderCommand::Finalize => break,
                    }
                }

                // Finalize
                match encoder.finalize() {
                    Ok(result) => Ok(StreamResult::success(
                        stream_id,
                        result.output_path,
                        frames_encoded,
                        frames_skipped,
                        result.bytes_written,
                        result.fragments,
                    )),
                    Err(e) => Ok(StreamResult::failure(stream_id, e.to_string())),
                }
            }
            (StreamOutput::Channel { .. }, _) => {
                // TODO: Implement streaming mode
                Ok(StreamResult::failure(
                    stream_id,
                    "Streaming mode not yet implemented",
                ))
            }
            _ => Ok(StreamResult::failure(
                stream_id,
                "Unsupported output/strategy combination",
            )),
        }
    }
}

impl std::fmt::Debug for EncodingWorkload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncodingWorkload")
            .field("stream_count", &self.streams.len())
            .field("finalized", &self.finalized)
            .field("config", &self.config)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workload_config_default() {
        let config = WorkloadConfig::default();
        assert!(config.thread_pool_size.is_none());
    }

    #[test]
    fn test_workload_config_builder() {
        let config = WorkloadConfig::new().with_thread_pool_size(4);
        assert_eq!(config.thread_pool_size, Some(4));
    }

    #[test]
    fn test_encoding_workload_create() {
        let workload = EncodingWorkload::new(WorkloadConfig::default()).unwrap();
        assert_eq!(workload.stream_count(), 0);
        assert!(!workload.has_stream("test"));
    }

    #[test]
    fn test_encoding_workload_add_stream() {
        let mut workload = EncodingWorkload::new(WorkloadConfig::default()).unwrap();
        workload
            .add_stream(StreamConfig::file("cam1", "output.mp4"))
            .unwrap();
        assert_eq!(workload.stream_count(), 1);
        assert!(workload.has_stream("cam1"));
    }

    #[test]
    fn test_encoding_workload_add_duplicate_stream() {
        let mut workload = EncodingWorkload::new(WorkloadConfig::default()).unwrap();
        workload
            .add_stream(StreamConfig::file("cam1", "output.mp4"))
            .unwrap();
        let result = workload.add_stream(StreamConfig::file("cam1", "output2.mp4"));
        assert!(result.is_err());
    }

    #[test]
    fn test_encoding_workload_submit_to_nonexistent_stream() {
        let workload = EncodingWorkload::new(WorkloadConfig::default()).unwrap();
        let rgb = vec![0u8; 64 * 64 * 3];
        let result = workload.submit_frame("nonexistent", &rgb, 64, 64);
        assert!(result.is_err());
    }

    #[test]
    fn test_encoding_workload_finalize_empty() {
        let workload = EncodingWorkload::new(WorkloadConfig::default()).unwrap();
        let result = workload.finalize().unwrap();
        assert_eq!(result.stream_count(), 0);
        assert!(result.is_success());
    }

    #[test]
    fn test_encoding_workload_single_stream_standard() {
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_dir.path().join("output.mp4");

        let mut workload = EncodingWorkload::new(WorkloadConfig::default()).unwrap();
        workload
            .add_stream(StreamConfig::file("cam1", output.clone()))
            .unwrap();

        let rgb = vec![128u8; 64 * 64 * 3];
        for _ in 0..5 {
            workload.submit_frame("cam1", &rgb, 64, 64).unwrap();
        }

        let result = workload.finalize().unwrap();
        assert!(result.is_success());
        assert_eq!(result.stream_count(), 1);

        let stream_result = result.get(&StreamId::new("cam1")).unwrap();
        assert!(stream_result.is_success());
        assert_eq!(stream_result.frames_encoded, 5);
        assert!(output.exists());
    }

    #[test]
    fn test_encoding_workload_single_stream_fragment() {
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_dir.path().join("output.mp4");

        let mut workload = EncodingWorkload::new(WorkloadConfig::default()).unwrap();
        workload
            .add_stream(
                StreamConfig::file("cam1", output.clone())
                    .with_strategy(EncodingStrategy::fragment_by_frames(3)),
            )
            .unwrap();

        let rgb = vec![128u8; 64 * 64 * 3];
        for _ in 0..10 {
            workload.submit_frame("cam1", &rgb, 64, 64).unwrap();
        }

        let result = workload.finalize().unwrap();
        assert!(result.is_success());

        let stream_result = result.get(&StreamId::new("cam1")).unwrap();
        assert!(stream_result.is_success());
        assert_eq!(stream_result.frames_encoded, 10);
        assert!(stream_result.fragments > 1); // Should have multiple fragments
        assert!(output.exists());
    }

    #[test]
    fn test_encoding_workload_multi_stream() {
        let temp_dir = tempfile::tempdir().unwrap();
        let output1 = temp_dir.path().join("cam1.mp4");
        let output2 = temp_dir.path().join("cam2.mp4");

        let mut workload = EncodingWorkload::new(WorkloadConfig::default()).unwrap();
        workload
            .add_stream(StreamConfig::file("cam1", output1.clone()))
            .unwrap();
        workload
            .add_stream(StreamConfig::file("cam2", output2.clone()))
            .unwrap();

        let rgb = vec![128u8; 64 * 64 * 3];
        for _ in 0..5 {
            workload.submit_frame("cam1", &rgb, 64, 64).unwrap();
            workload.submit_frame("cam2", &rgb, 64, 64).unwrap();
        }

        let result = workload.finalize().unwrap();
        assert!(result.is_success());
        assert_eq!(result.stream_count(), 2);
        assert_eq!(result.total_frames, 10);
        assert!(output1.exists());
        assert!(output2.exists());
    }

    #[test]
    fn test_encoding_workload_mixed_strategy() {
        let temp_dir = tempfile::tempdir().unwrap();
        let output1 = temp_dir.path().join("standard.mp4");
        let output2 = temp_dir.path().join("fragment.mp4");

        let mut workload = EncodingWorkload::new(WorkloadConfig::default()).unwrap();

        // Standard encoder
        workload
            .add_stream(StreamConfig::file("standard", output1.clone()))
            .unwrap();

        // Fragment encoder
        workload
            .add_stream(
                StreamConfig::file("fragment", output2.clone())
                    .with_strategy(EncodingStrategy::fragment_by_frames(50)),
            )
            .unwrap();

        let rgb = vec![128u8; 64 * 64 * 3];
        for _ in 0..100 {
            workload.submit_frame("standard", &rgb, 64, 64).unwrap();
            workload.submit_frame("fragment", &rgb, 64, 64).unwrap();
        }

        let result = workload.finalize().unwrap();
        assert!(result.is_success());

        let standard_result = result.get(&StreamId::new("standard")).unwrap();
        let fragment_result = result.get(&StreamId::new("fragment")).unwrap();

        assert_eq!(standard_result.frames_encoded, 100);
        assert_eq!(standard_result.fragments, 1);

        assert_eq!(fragment_result.frames_encoded, 100);
        assert!(fragment_result.fragments > 1);
    }

    #[test]
    fn test_frame_data() {
        let rgb = vec![128u8; 64 * 64 * 3];
        let frame = FrameData::new(rgb.clone(), 64, 64);
        assert!(frame.validate().is_ok());
    }
}
