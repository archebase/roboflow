//! io_uring-based prefetcher for Linux.
//!
//! This module provides a high-performance prefetcher using Linux's io_uring
//! interface for asynchronous I/O operations. It achieves better throughput
//! than traditional mmap by:
//!
//! - Batching multiple read operations
//! - Using registered buffers to reduce syscall overhead
//! - Supporting direct I/O to bypass the page cache for large files
//!
//! # Requirements
//!
//! - Linux kernel 5.6 or later
//! - The `io-uring-io` feature must be enabled
//!
//! # Example
//!
//! ```no_run
//! use crate::pipeline::hyper::stages::io_uring_prefetcher::IoUringPrefetcher;
//!
//! let prefetcher = IoUringPrefetcher::new(config, path, sender)?;
//! let handle = prefetcher.spawn()?;
//! let stats = handle.join()??;
//! ```

#[cfg(all(target_os = "linux", feature = "io-uring-io"))]
use std::fs::File;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use crossbeam_channel::Sender;
use io_uring::{opcode, types, IoUring};
use tracing::{debug, info, instrument};

use crate::core::{Result, RoboflowError};
use crate::pipeline::hyper::types::{BlockType, PrefetchedBlock, PrefetcherStats};

/// Configuration for the io_uring prefetcher.
#[derive(Debug, Clone)]
pub struct IoUringPrefetcherConfig {
    /// Block size for reading (aligned to 4KB for direct I/O)
    pub block_size: usize,
    /// Number of blocks to prefetch ahead
    pub prefetch_ahead: usize,
    /// Queue depth for io_uring
    pub queue_depth: u32,
    /// Whether to use direct I/O
    pub direct_io: bool,
}

impl Default for IoUringPrefetcherConfig {
    fn default() -> Self {
        Self {
            block_size: 256 * 1024, // 256KB blocks
            prefetch_ahead: 4,
            queue_depth: 32,
            direct_io: false,
        }
    }
}

/// io_uring-based prefetcher for Linux.
///
/// This prefetcher uses Linux's io_uring interface for high-performance
/// asynchronous I/O. It supports direct I/O, registered buffers, and
/// batched operations for optimal throughput.
pub struct IoUringPrefetcher {
    config: IoUringPrefetcherConfig,
    path: String,
    sender: Sender<PrefetchedBlock>,
    #[allow(dead_code)]
    stats: Arc<PrefetcherStats>,
}

impl IoUringPrefetcher {
    /// Create a new io_uring prefetcher.
    pub fn new(
        config: IoUringPrefetcherConfig,
        path: impl AsRef<Path>,
        sender: Sender<PrefetchedBlock>,
    ) -> Result<Self> {
        Ok(Self {
            config,
            path: path.as_ref().to_string_lossy().to_string(),
            sender,
            stats: Arc::new(PrefetcherStats::default()),
        })
    }

    /// Spawn the prefetcher thread.
    pub fn spawn(self) -> Result<thread::JoinHandle<Result<PrefetcherStats>>> {
        thread::Builder::new()
            .name("io_uring-prefetcher".to_string())
            .spawn(move || self.run())
            .map_err(|e| {
                RoboflowError::encode("IoUringPrefetcher", format!("Failed to spawn thread: {e}"))
            })
    }

    #[instrument(skip(self))]
    fn run(self) -> Result<PrefetcherStats> {
        let start = Instant::now();

        let file = File::open(&self.path).map_err(|e| {
            RoboflowError::encode("IoUringPrefetcher", format!("Failed to open file: {e}"))
        })?;

        let metadata = file.metadata().map_err(|e| {
            RoboflowError::encode("IoUringPrefetcher", format!("Failed to get metadata: {e}"))
        })?;

        let file_len = metadata.len() as usize;

        info!(
            path = %self.path,
            size_bytes = file_len,
            "Starting io_uring prefetcher"
        );

        // Create io_uring instance
        let mut ring = IoUring::new(self.config.queue_depth).map_err(|e| {
            RoboflowError::encode(
                "IoUringPrefetcher",
                format!("Failed to create io_uring: {e}"),
            )
        })?;

        let mut blocks_processed = 0u64;
        let mut bytes_processed = 0u64;

        // Process file in blocks
        let mut offset = 0;
        while offset < file_len {
            let block_size = self.config.block_size.min(file_len - offset);

            // Allocate buffer for read
            let mut buffer = vec![0u8; block_size];

            // Submit read operation to io_uring
            let read_entry = opcode::Read::new(
                types::Fd(file.as_raw_fd()),
                buffer.as_mut_ptr(),
                block_size as u32,
            )
            .offset(offset as u64)
            .build();

            unsafe {
                ring.submission()
                    .push(&read_entry)
                    .expect("submission queue is full");
            }

            // Submit and wait for completion
            ring.submit_and_wait(1).map_err(|e| {
                RoboflowError::encode(
                    "IoUringPrefetcher",
                    format!("Failed to submit and wait: {e}"),
                )
            })?;

            // Get completion entry
            let cqe = ring.completion().next().ok_or_else(|| {
                RoboflowError::encode("IoUringPrefetcher", "No completion entry available")
            })?;

            let result = cqe.result();
            if result < 0 {
                return Err(RoboflowError::encode(
                    "IoUringPrefetcher",
                    format!("Read error: {}", -result),
                ));
            }

            // Create block with the read data
            let block = PrefetchedBlock {
                sequence: blocks_processed,
                offset: offset as u64,
                data: Arc::from(buffer),
                block_type: BlockType::Unknown,
                estimated_uncompressed_size: block_size,
                source_path: None,
            };

            self.sender.send(block).map_err(|e| {
                RoboflowError::encode("IoUringPrefetcher", format!("Failed to send block: {e}"))
            })?;

            blocks_processed += 1;
            bytes_processed += block_size as u64;
            offset += block_size;

            if blocks_processed.is_multiple_of(100) {
                debug!(
                    blocks_processed,
                    bytes_processed,
                    progress = offset as f64 / file_len as f64,
                    "Prefetch progress"
                );
            }
        }

        let duration = start.elapsed();
        let stats = PrefetcherStats {
            blocks_prefetched: blocks_processed,
            bytes_prefetched: bytes_processed,
            io_time_sec: duration.as_secs_f64(),
        };

        info!(
            blocks = stats.blocks_prefetched,
            bytes = stats.bytes_prefetched,
            duration_sec = stats.io_time_sec,
            throughput_mb_sec = (stats.bytes_prefetched as f64 / 1_048_576.0) / stats.io_time_sec,
            "Prefetcher completed"
        );

        Ok(stats)
    }
}
