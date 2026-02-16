// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Multi-encoder pool for parallel video encoding.
//!
//! This module provides a pool of encoder workers that can encode
//! video fragments in parallel, utilizing multiple CPU cores and
//! potentially multiple GPU encoders.
//!
//! # Design
//!
//! The encoder pool accepts pre-converted `VideoFrame` objects which can be:
//! - RGB24 (converted by encoder via sws_scale)
//! - NV12 (pre-converted via SIMD, 8-12x faster)
//! - YUV420P (pre-converted via SIMD)
//!
//! For best performance, use the three-stage pipeline with ConvertPool
//! to pre-convert frames before encoding.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crossbeam_channel::{Receiver, Sender, bounded};
use tracing::{debug, trace, warn};

use crate::decode::DecodedFrame;
use crate::fragment::{FragmentEncoder, FragmentEncoderConfig, FragmentInfo};
use crate::frame::VideoFrame;
use crate::simd::{ConversionStrategy, optimal_strategy};

/// Command sent to encoder workers.
#[derive(Debug)]
pub struct EncodeCommand {
    /// Sequence number for ordering.
    pub sequence: u64,
    /// Camera identifier.
    pub camera_id: String,
    /// Frames to encode (pre-converted VideoFrame).
    pub frames: Vec<VideoFrame>,
    /// Fragment index.
    pub fragment_index: u32,
}

impl EncodeCommand {
    /// Create a new encode command from pre-converted frames.
    pub fn new(
        sequence: u64,
        camera_id: String,
        frames: Vec<VideoFrame>,
        fragment_index: u32,
    ) -> Self {
        Self {
            sequence,
            camera_id,
            frames,
            fragment_index,
        }
    }

    /// Create an encode command from decoded RGB frames with inline SIMD conversion.
    ///
    /// This uses batch SIMD conversion for better cache utilization and reduced overhead.
    /// For best performance, use the three-stage pipeline with ConvertPool.
    pub fn from_decoded(
        sequence: u64,
        camera_id: String,
        decoded_frames: Vec<DecodedFrame>,
        fragment_index: u32,
    ) -> Self {
        if decoded_frames.is_empty() {
            return Self {
                sequence,
                camera_id,
                frames: Vec::new(),
                fragment_index,
            };
        }

        // All frames should have the same dimensions (validated by caller)
        let width = decoded_frames[0].width();
        let height = decoded_frames[0].height();
        let width_usize = width as usize;
        let height_usize = height as usize;

        // Collect RGB data as slices for batch processing
        let rgb_data: Vec<&[u8]> = decoded_frames.iter().map(|f| f.data()).collect();

        // Use batch SIMD conversion - better cache locality, fewer allocations
        let frames = match crate::simd::rgb_batch_to_nv12(&rgb_data, width_usize, height_usize) {
            Ok(converted) => converted
                .into_iter()
                .map(|(y_plane, uv_plane)| VideoFrame::from_nv12(width, height, y_plane, uv_plane))
                .collect(),
            Err(_) => {
                // Fallback to per-frame conversion if batch fails
                decoded_frames
                    .into_iter()
                    .map(
                        |f| match crate::simd::rgb_to_nv12(f.data(), width_usize, height_usize) {
                            Ok((y_plane, uv_plane)) => {
                                VideoFrame::from_nv12(width, height, y_plane, uv_plane)
                            }
                            Err(_) => VideoFrame::new(width, height, f.data().to_vec()),
                        },
                    )
                    .collect()
            }
        };

        Self {
            sequence,
            camera_id,
            frames,
            fragment_index,
        }
    }
}

/// Result from an encoder worker.
#[derive(Debug)]
pub struct EncodeResult {
    /// Sequence number from the original command.
    pub sequence: u64,
    /// Camera identifier.
    pub camera_id: String,
    /// Fragment index.
    pub fragment_index: u32,
    /// Encoded fragment (or error).
    pub result: io::Result<FragmentInfo>,
}

/// Configuration for the encoder pool.
#[derive(Debug, Clone)]
pub struct EncoderPoolConfig {
    /// Number of encoder workers.
    pub worker_count: usize,
    /// Channel capacity for pending encode jobs.
    pub pending_capacity: usize,
    /// Channel capacity for completed fragments.
    pub completed_capacity: usize,
    /// Fragment encoder configuration template.
    pub fragment_config: FragmentEncoderConfig,
    /// Colorspace conversion strategy (for backward compat logging).
    pub conversion_strategy: ConversionStrategy,
}

impl Default for EncoderPoolConfig {
    fn default() -> Self {
        let cpu_count = num_cpus::get();
        // VideoToolbox encoding still needs CPU for frame feeding and format conversion
        // Allocate more workers for better parallelism
        // On 10 cores: 6 encode workers (60%) + 4 decode workers (40%)
        let encode_workers = (cpu_count * 3).div_ceil(5).max(2); // ~60% of CPUs, min 2

        Self {
            worker_count: encode_workers,
            pending_capacity: 256,   // Increased for better throughput
            completed_capacity: 256, // Increased for better throughput
            fragment_config: FragmentEncoderConfig::default(),
            conversion_strategy: optimal_strategy(),
        }
    }
}

/// Statistics for the encoder pool.
#[derive(Debug, Clone, Copy, Default)]
pub struct EncoderPoolStats {
    /// Total fragments encoded.
    pub fragments_encoded: u64,
    /// Total fragments failed.
    pub fragments_failed: u64,
    /// Total frames encoded.
    pub frames_encoded: u64,
    /// Current pending queue size.
    pub pending_count: usize,
    /// Active workers.
    pub active_workers: usize,
}

/// Multi-encoder pool for parallel video encoding.
pub struct EncoderPool {
    /// Worker handles.
    workers: Vec<std::thread::JoinHandle<()>>,
    /// Channel to send encode commands.
    cmd_tx: Sender<EncodeCommand>,
    /// Channel to receive encode results.
    result_rx: Receiver<EncodeResult>,
    /// Worker count.
    worker_count: usize,
    /// Statistics (shared with workers for updates).
    stats_encoded: Arc<AtomicU64>,
    stats_failed: Arc<AtomicU64>,
    stats_frames: Arc<AtomicU64>,
    /// Active worker counter.
    active_workers: Arc<AtomicUsize>,
    /// In-flight commands being processed by workers.
    in_flight: Arc<AtomicUsize>,
}

impl EncoderPool {
    /// Create a new encoder pool.
    pub fn new(config: EncoderPoolConfig) -> io::Result<Self> {
        let (cmd_tx, cmd_rx) = bounded(config.pending_capacity);
        let (result_tx, result_rx) = bounded(config.completed_capacity);

        // Spawn workers
        let mut workers = Vec::with_capacity(config.worker_count);
        let cmd_rx = Arc::new(cmd_rx);
        let result_tx = Arc::new(result_tx);

        // Create shared stats counters
        let stats_encoded = Arc::new(AtomicU64::new(0));
        let stats_failed = Arc::new(AtomicU64::new(0));
        let stats_frames = Arc::new(AtomicU64::new(0));
        let active_workers = Arc::new(AtomicUsize::new(config.worker_count));
        let in_flight = Arc::new(AtomicUsize::new(0));

        for worker_id in 0..config.worker_count {
            let cmd_rx = Arc::clone(&cmd_rx);
            let result_tx = Arc::clone(&result_tx);
            let fragment_config = config.fragment_config.clone();
            let stats_encoded = Arc::clone(&stats_encoded);
            let stats_failed = Arc::clone(&stats_failed);
            let stats_frames = Arc::clone(&stats_frames);
            let active_workers = Arc::clone(&active_workers);
            let in_flight = Arc::clone(&in_flight);

            let handle = std::thread::Builder::new()
                .name(format!("encoder-worker-{}", worker_id))
                .spawn(move || {
                    encoder_worker_loop(
                        worker_id,
                        cmd_rx,
                        result_tx,
                        fragment_config,
                        &stats_encoded,
                        &stats_failed,
                        &stats_frames,
                        &active_workers,
                        &in_flight,
                    );
                })
                .map_err(|e| io::Error::other(e.to_string()))?;

            workers.push(handle);
        }

        // Drop original senders
        drop(cmd_rx);
        drop(result_tx);

        Ok(Self {
            workers,
            cmd_tx,
            result_rx,
            worker_count: config.worker_count,
            stats_encoded,
            stats_failed,
            stats_frames,
            active_workers,
            in_flight,
        })
    }

    /// Submit frames for encoding.
    pub fn submit(&self, cmd: EncodeCommand) -> io::Result<()> {
        self.cmd_tx
            .send(cmd)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Encoder pool shut down"))
    }

    /// Try to receive an encode result (non-blocking).
    pub fn try_recv(&self) -> Option<EncodeResult> {
        self.result_rx.try_recv().ok()
    }

    /// Receive an encode result (blocking).
    pub fn recv(&self) -> io::Result<EncodeResult> {
        self.result_rx
            .recv()
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Encoder pool shut down"))
    }

    /// Get statistics.
    pub fn stats(&self) -> EncoderPoolStats {
        EncoderPoolStats {
            fragments_encoded: self.stats_encoded.load(Ordering::Relaxed),
            fragments_failed: self.stats_failed.load(Ordering::Relaxed),
            frames_encoded: self.stats_frames.load(Ordering::Relaxed),
            pending_count: self.cmd_tx.len() + self.in_flight.load(Ordering::Relaxed),
            active_workers: self.active_workers.load(Ordering::Relaxed),
        }
    }

    /// Get the number of results waiting to be received.
    pub fn result_count(&self) -> usize {
        self.result_rx.len()
    }

    /// Shutdown the pool.
    pub fn shutdown(self) {
        drop(self.cmd_tx);
        for worker in self.workers {
            let _ = worker.join();
        }
    }

    /// Get worker count.
    pub fn worker_count(&self) -> usize {
        self.worker_count
    }
}

/// Encoder worker loop.
#[allow(clippy::too_many_arguments)]
fn encoder_worker_loop(
    worker_id: usize,
    cmd_rx: Arc<Receiver<EncodeCommand>>,
    result_tx: Arc<Sender<EncodeResult>>,
    fragment_config: FragmentEncoderConfig,
    stats_encoded: &Arc<AtomicU64>,
    stats_failed: &Arc<AtomicU64>,
    stats_frames: &Arc<AtomicU64>,
    active_workers: &Arc<AtomicUsize>,
    in_flight: &Arc<AtomicUsize>,
) {
    debug!(worker_id, "Encoder worker started");

    // Create a persistent encoder for this worker
    let mut encoder = match FragmentEncoder::new(fragment_config.clone()) {
        Ok(e) => e,
        Err(e) => {
            warn!(worker_id, error = %e, "Failed to create encoder");
            active_workers.fetch_sub(1, Ordering::Relaxed);
            return;
        }
    };

    while let Ok(cmd) = cmd_rx.recv() {
        in_flight.fetch_add(1, Ordering::Relaxed);
        let frame_count = cmd.frames.len();
        trace!(
            worker_id,
            sequence = cmd.sequence,
            camera = %cmd.camera_id,
            frames = frame_count,
            "Processing encode command"
        );

        if cmd.frames.is_empty() {
            stats_failed.fetch_add(1, Ordering::Relaxed);
            let result = EncodeResult {
                sequence: cmd.sequence,
                camera_id: cmd.camera_id,
                fragment_index: cmd.fragment_index,
                result: Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "No frames to encode",
                )),
            };
            if result_tx.send(result).is_err() {
                break;
            }
            continue;
        }

        // Encode the fragment - frames are already converted
        let encode_result = encoder.encode(cmd.frames);

        let result = match encode_result {
            Ok(fragment) => {
                stats_encoded.fetch_add(1, Ordering::Relaxed);
                stats_frames.fetch_add(frame_count as u64, Ordering::Relaxed);
                trace!(
                    worker_id,
                    sequence = cmd.sequence,
                    camera = %cmd.camera_id,
                    fragment_index = cmd.fragment_index,
                    "Fragment encoded successfully"
                );
                EncodeResult {
                    sequence: cmd.sequence,
                    camera_id: cmd.camera_id,
                    fragment_index: cmd.fragment_index,
                    result: Ok(fragment),
                }
            }
            Err(e) => {
                stats_failed.fetch_add(1, Ordering::Relaxed);
                warn!(
                    worker_id,
                    sequence = cmd.sequence,
                    camera = %cmd.camera_id,
                    error = %e,
                    "Failed to encode fragment"
                );
                EncodeResult {
                    sequence: cmd.sequence,
                    camera_id: cmd.camera_id,
                    fragment_index: cmd.fragment_index,
                    result: Err(io::Error::other(e.to_string())),
                }
            }
        };

        in_flight.fetch_sub(1, Ordering::Relaxed);
        if result_tx.send(result).is_err() {
            break;
        }
    }

    active_workers.fetch_sub(1, Ordering::Relaxed);
    debug!(worker_id, "Encoder worker exiting");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encoder_pool_config_default() {
        let config = EncoderPoolConfig::default();
        // Worker count is CPU-dependent, just check it's at least 2
        assert!(config.worker_count >= 2);
        assert!(config.pending_capacity > 0);
        assert!(config.completed_capacity > 0);
    }

    #[test]
    fn test_encoder_pool_create() {
        let config = EncoderPoolConfig {
            worker_count: 1,
            pending_capacity: 4,
            completed_capacity: 4,
            ..Default::default()
        };

        let pool = EncoderPool::new(config).expect("Failed to create pool");
        assert_eq!(pool.worker_count(), 1);

        pool.shutdown();
    }
}
