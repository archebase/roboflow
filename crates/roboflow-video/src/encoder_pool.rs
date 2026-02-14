// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Multi-encoder pool for parallel video encoding.
//!
//! This module provides a pool of encoder workers that can encode
//! video fragments in parallel, utilizing multiple CPU cores and
//! potentially multiple GPU encoders.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crossbeam_channel::{Receiver, Sender, bounded};
use tracing::{debug, trace, warn};

use crate::decode::DecodedFrame;
use crate::fragment::{FragmentEncoder, FragmentEncoderConfig, FragmentInfo};
use crate::simd::{ConversionStrategy, optimal_strategy};

/// Command sent to encoder workers.
#[derive(Debug)]
pub struct EncodeCommand {
    /// Sequence number for ordering.
    pub sequence: u64,
    /// Camera identifier.
    pub camera_id: String,
    /// Frames to encode.
    pub frames: Vec<DecodedFrame>,
    /// Fragment index.
    pub fragment_index: u32,
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
    /// Colorspace conversion strategy.
    pub conversion_strategy: ConversionStrategy,
}

impl Default for EncoderPoolConfig {
    fn default() -> Self {
        Self {
            worker_count: 2,
            pending_capacity: 32,
            completed_capacity: 32,
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
    /// Statistics.
    stats_encoded: AtomicU64,
    stats_failed: AtomicU64,
    stats_frames: AtomicU64,
    /// Active worker counter.
    active_workers: AtomicUsize,
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

        for worker_id in 0..config.worker_count {
            let cmd_rx = Arc::clone(&cmd_rx);
            let result_tx = Arc::clone(&result_tx);
            let fragment_config = config.fragment_config.clone();
            let strategy = config.conversion_strategy;

            let handle = std::thread::Builder::new()
                .name(format!("encoder-worker-{}", worker_id))
                .spawn(move || {
                    encoder_worker_loop(worker_id, cmd_rx, result_tx, fragment_config, strategy);
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
            stats_encoded: AtomicU64::new(0),
            stats_failed: AtomicU64::new(0),
            stats_frames: AtomicU64::new(0),
            active_workers: AtomicUsize::new(config.worker_count),
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
            pending_count: self.cmd_tx.len(),
            active_workers: self.active_workers.load(Ordering::Relaxed),
        }
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
fn encoder_worker_loop(
    worker_id: usize,
    cmd_rx: Arc<Receiver<EncodeCommand>>,
    result_tx: Arc<Sender<EncodeResult>>,
    fragment_config: FragmentEncoderConfig,
    strategy: ConversionStrategy,
) {
    debug!(worker_id, strategy = ?strategy, "Encoder worker started");

    // Create a persistent encoder for this worker
    let mut encoder = match FragmentEncoder::new(fragment_config.clone()) {
        Ok(e) => e,
        Err(e) => {
            warn!(worker_id, error = %e, "Failed to create encoder");
            return;
        }
    };

    while let Ok(cmd) = cmd_rx.recv() {
        trace!(
            worker_id,
            sequence = cmd.sequence,
            camera = %cmd.camera_id,
            frames = cmd.frames.len(),
            "Processing encode command"
        );

        // Convert frames to VideoFrame format
        let video_frames: Vec<crate::VideoFrame> = cmd
            .frames
            .iter()
            .map(|f| {
                // For now, pass RGB directly to encoder (the encoder handles YUV conversion internally)
                // In a future optimization, we could pre-convert to NV12/YUV420p here
                crate::VideoFrame::new(f.width, f.height, f.data.as_slice().to_vec())
            })
            .collect();

        if video_frames.is_empty() {
            let result = EncodeResult {
                sequence: cmd.sequence,
                camera_id: cmd.camera_id,
                fragment_index: cmd.fragment_index,
                result: Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "No valid frames to encode",
                )),
            };
            if result_tx.send(result).is_err() {
                break;
            }
            continue;
        }

        // Encode the fragment
        let encode_result = encoder.encode(video_frames);

        let result = match encode_result {
            Ok(fragment) => {
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

        if result_tx.send(result).is_err() {
            break;
        }
    }

    debug!(worker_id, "Encoder worker exiting");
}

/// Load balancer for distributing encode jobs across workers.
#[derive(Debug)]
pub struct LoadBalancer {
    /// Round-robin counter.
    next_worker: AtomicUsize,
    /// Number of workers.
    worker_count: usize,
}

impl LoadBalancer {
    /// Create a new load balancer.
    pub fn new(worker_count: usize) -> Self {
        Self {
            next_worker: AtomicUsize::new(0),
            worker_count,
        }
    }

    /// Get the next worker index (round-robin).
    pub fn next(&self) -> usize {
        let current = self.next_worker.fetch_add(1, Ordering::Relaxed);
        current % self.worker_count
    }
}

/// Pending job tracker for load balancing.
#[derive(Debug, Default)]
pub struct PendingTracker {
    /// Pending jobs per worker.
    pending: Vec<AtomicUsize>,
}

impl PendingTracker {
    /// Create a new pending tracker.
    pub fn new(worker_count: usize) -> Self {
        Self {
            pending: (0..worker_count).map(|_| AtomicUsize::new(0)).collect(),
        }
    }

    /// Increment pending count for a worker.
    pub fn increment(&self, worker_id: usize) {
        self.pending[worker_id].fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement pending count for a worker.
    pub fn decrement(&self, worker_id: usize) {
        self.pending[worker_id].fetch_sub(1, Ordering::Relaxed);
    }

    /// Get pending count for a worker.
    pub fn get(&self, worker_id: usize) -> usize {
        self.pending[worker_id].load(Ordering::Relaxed)
    }

    /// Get the least loaded worker.
    pub fn least_loaded(&self) -> usize {
        let mut min_idx = 0;
        let mut min_val = usize::MAX;
        for (i, counter) in self.pending.iter().enumerate() {
            let val = counter.load(Ordering::Relaxed);
            if val < min_val {
                min_val = val;
                min_idx = i;
            }
        }
        min_idx
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_balancer_round_robin() {
        let lb = LoadBalancer::new(3);
        assert_eq!(lb.next(), 0);
        assert_eq!(lb.next(), 1);
        assert_eq!(lb.next(), 2);
        assert_eq!(lb.next(), 0);
        assert_eq!(lb.next(), 1);
    }

    #[test]
    fn test_pending_tracker() {
        let tracker = PendingTracker::new(3);

        // Initially all zero
        assert_eq!(tracker.get(0), 0);
        assert_eq!(tracker.get(1), 0);
        assert_eq!(tracker.get(2), 0);

        // Increment
        tracker.increment(0);
        tracker.increment(0);
        tracker.increment(1);
        assert_eq!(tracker.get(0), 2);
        assert_eq!(tracker.get(1), 1);
        assert_eq!(tracker.get(2), 0);

        // Least loaded
        assert_eq!(tracker.least_loaded(), 2);

        // Decrement
        tracker.decrement(0);
        assert_eq!(tracker.get(0), 1);
    }

    #[test]
    fn test_encoder_pool_config_default() {
        let config = EncoderPoolConfig::default();
        assert_eq!(config.worker_count, 2);
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
