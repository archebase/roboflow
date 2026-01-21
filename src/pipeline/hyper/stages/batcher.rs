//! Stage 3: Batcher (simplified)
//!
//! This stage is largely integrated into parser_slicer for efficiency.
//! This module provides a pass-through batcher for cases where additional
//! batching control is needed.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use crossbeam_channel::{Receiver, Sender};
use tracing::{info, instrument};

use crate::core::{Result, RoboflowError};
use crate::pipeline::hyper::types::BatcherStats;
use crate::pipeline::types::chunk::MessageChunk;

/// Configuration for the batcher stage.
#[derive(Debug, Clone)]
pub struct BatcherStageConfig {
    /// Number of batcher threads
    pub num_threads: usize,
    /// Target batch size (bytes)
    pub target_size: usize,
}

impl Default for BatcherStageConfig {
    fn default() -> Self {
        Self {
            num_threads: 2,
            target_size: 16 * 1024 * 1024, // 16MB
        }
    }
}

/// Stage 3: Batcher
///
/// Pass-through batcher that can optionally merge small chunks.
pub struct BatcherStage {
    #[allow(dead_code)]
    config: BatcherStageConfig,
    receiver: Receiver<MessageChunk<'static>>,
    sender: Sender<MessageChunk<'static>>,
    stats: Arc<BatcherStageStats>,
}

#[derive(Debug, Default)]
struct BatcherStageStats {
    chunks_received: AtomicU64,
    chunks_sent: AtomicU64,
}

impl BatcherStage {
    /// Create a new batcher stage.
    pub fn new(
        config: BatcherStageConfig,
        receiver: Receiver<MessageChunk<'static>>,
        sender: Sender<MessageChunk<'static>>,
    ) -> Self {
        Self {
            config,
            receiver,
            sender,
            stats: Arc::new(BatcherStageStats::default()),
        }
    }

    /// Spawn the batcher in a new thread.
    pub fn spawn(self) -> Result<thread::JoinHandle<Result<BatcherStats>>> {
        let handle = thread::spawn(move || self.run());
        Ok(handle)
    }

    /// Run the batcher stage (pass-through mode).
    #[instrument(skip_all)]
    fn run(self) -> Result<BatcherStats> {
        info!("Starting batcher stage (pass-through)");
        let start = Instant::now();

        // Simple pass-through: forward chunks as-is
        // Batching is already done in parser_slicer
        while let Ok(chunk) = self.receiver.recv() {
            self.stats.chunks_received.fetch_add(1, Ordering::Relaxed);

            self.sender
                .send(chunk)
                .map_err(|_| RoboflowError::encode("Batcher", "Channel closed"))?;

            self.stats.chunks_sent.fetch_add(1, Ordering::Relaxed);
        }

        let duration = start.elapsed();
        let chunks_received = self.stats.chunks_received.load(Ordering::Relaxed);
        let chunks_sent = self.stats.chunks_sent.load(Ordering::Relaxed);

        let stats = BatcherStats {
            messages_received: 0, // Not tracked in pass-through mode
            batches_created: chunks_sent,
            avg_batch_size: if chunks_sent > 0 {
                chunks_received as f64 / chunks_sent as f64
            } else {
                0.0
            },
        };

        info!(
            chunks_received = chunks_received,
            chunks_sent = chunks_sent,
            duration_sec = duration.as_secs_f64(),
            "Batcher stage complete"
        );

        Ok(stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batcher_config_default() {
        let config = BatcherStageConfig::default();
        assert_eq!(config.num_threads, 2);
        assert_eq!(config.target_size, 16 * 1024 * 1024);
    }
}
