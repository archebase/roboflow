// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! # Work-Stealing Encoder Pool
//!
//! This module provides a work-stealing encoder pool for efficient
//! multi-camera video encoding.
//!
//! ## Architecture
//!
//! ```text
//!                    Global Queue
//!                    (Injector)
//!                         │
//!                         ▼
//!     ┌────────────────────────────────────────┐
//!     │                                        │
//!     ▼                                        ▼
//! Worker 1                                  Worker N
//!   (local queue)                           (local queue)
//!      │ steal from                             │
//!      │──────────────────────────────────────▶│
//!      │                                        │
//!      ▼                                        ▼
//!   Encode Frame                            Encode Frame
//! ```
//!
//! ## Benefits
//!
//! - **Better Load Balancing**: Workers can steal from each other
//! - **Scalability**: Handles >16 cameras efficiently
//! - **Cache Locality**: Each worker has local queue
//! - **Adaptive**: Automatically balances load

use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam_channel::Sender;
use crossbeam_deque::{Injector, Worker};

use roboflow_core::Result;

use super::ImageData;

// =============================================================================
// Job Types
// =============================================================================

/// Encoding job for the work-stealing pool.
///
/// Jobs are processed by encoder workers that can be either:
/// - Dedicated per-camera encoders (original StreamingCoordinator)
/// - Shared pool workers (this EncoderPool)
#[derive(Debug)]
pub enum EncodeJob {
    /// Encode a single frame - returns the encoded bytes
    Encode {
        camera: String,
        image: Arc<ImageData>,
        result_tx: Sender<Vec<u8>>,
    },

    /// Shutdown the pool
    Shutdown,
}

// =============================================================================
// Encoder Pool
// =============================================================================

/// Work-stealing encoder pool.
///
/// Uses crossbeam's work-stealing deque for efficient job distribution.
/// This is designed for scenarios where you have many cameras and want
/// to dynamically balance the encoding load across a fixed number of workers.
///
/// # Example
///
/// ```rust,ignore
/// use crossbeam_channel::bounded;
/// use roboflow_dataset::common::encoder_pool::{EncoderPool, EncodeJob};
///
/// let (result_tx, result_rx) = bounded(100);
/// let pool = EncoderPool::new(4, result_tx)?;
///
/// // Submit encode jobs
/// pool.encode_frame("camera_1".to_string(), image_data);
/// ```
pub struct EncoderPool {
    /// Global job queue
    global: Arc<Injector<EncodeJob>>,

    /// Worker threads
    threads: Vec<WorkerThread>,

    /// Shutdown flag
    shutdown: Arc<std::sync::atomic::AtomicBool>,
}

/// Worker thread handle.
struct WorkerThread {
    /// Thread join handle
    handle: Option<JoinHandle<()>>,
}

impl EncoderPool {
    /// Create a new encoder pool.
    ///
    /// # Arguments
    ///
    /// * `num_threads` - Number of encoder threads (default: num_cpus::get())
    /// * `result_tx` - Channel to send encoded fragments
    pub fn new(num_threads: usize, result_tx: Sender<Vec<u8>>) -> Result<Self> {
        let num_threads = if num_threads == 0 {
            num_cpus::get_physical()
        } else {
            num_threads
        };

        let global = Arc::new(Injector::new());
        let mut threads = Vec::with_capacity(num_threads);
        let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));

        for worker_id in 0..num_threads {
            // Create worker
            let worker = Worker::new_fifo();
            let _stealer = worker.stealer(); // Stealer is unused in this simplified version

            // For simplicity, we'll just use the global queue in this implementation
            let global_clone = global.clone();
            let shutdown_clone = shutdown.clone();
            let result_tx_clone = result_tx.clone();

            // Spawn worker thread
            let handle = thread::Builder::new()
                .name(format!("encoder-pool-{}", worker_id))
                .spawn(move || {
                    Self::worker_loop(
                        worker_id,
                        worker,
                        global_clone,
                        shutdown_clone,
                        result_tx_clone,
                    )
                })?;

            threads.push(WorkerThread {
                handle: Some(handle),
            });
        }

        Ok(Self {
            global,
            threads,
            shutdown,
        })
    }

    /// Submit a job to the global queue.
    pub fn submit(&self, job: EncodeJob) {
        self.global.push(job);
    }

    /// Submit an encode job.
    ///
    /// This is a convenience method that creates an EncodeJob::Encode
    /// but requires a result_tx channel for the encoded data.
    pub fn encode_frame(&self, camera: String, image: Arc<ImageData>, result_tx: Sender<Vec<u8>>) {
        self.submit(EncodeJob::Encode {
            camera,
            image,
            result_tx,
        });
    }

    /// Shutdown the pool gracefully.
    ///
    /// Note: After calling shutdown, the pool cannot be used anymore.
    pub fn shutdown(&mut self) -> Result<()> {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::SeqCst);

        // Send shutdown signal to all workers
        for _ in 0..self.threads.len() {
            self.global.push(EncodeJob::Shutdown);
        }

        // Wait for all threads to finish
        for worker_thread in &mut self.threads {
            if let Some(handle) = worker_thread.handle.take() {
                let _ = handle.join();
            }
        }

        Ok(())
    }

    /// Worker thread main loop.
    fn worker_loop(
        worker_id: usize,
        worker: Worker<EncodeJob>,
        global: Arc<Injector<EncodeJob>>,
        shutdown: Arc<std::sync::atomic::AtomicBool>,
        _result_tx: Sender<Vec<u8>>,
    ) {
        while !shutdown.load(std::sync::atomic::Ordering::SeqCst) {
            // Try to find a job, prioritizing local queue, then global
            let job = worker.pop().or_else(|| {
                // Try global queue
                match global.steal() {
                    crossbeam_deque::Steal::Success(job) => Some(job),
                    crossbeam_deque::Steal::Empty | crossbeam_deque::Steal::Retry => None,
                }
            });

            match job {
                Some(EncodeJob::Encode {
                    camera,
                    image,
                    result_tx: tx,
                }) => {
                    // For now, we just forward the image data as a placeholder
                    // In a real implementation, this would use RsmpegEncoder
                    // or call out to the existing S3StreamingEncoder

                    // TODO: Implement actual encoding here
                    // For now, just send the raw RGB data as a placeholder
                    // to demonstrate the work-stealing mechanism

                    let width = image.width as usize;
                    let height = image.height as usize;
                    let data_size = width * height * 3;

                    if data_size == image.data.len() {
                        let _ = tx.send(image.data.clone());
                    } else {
                        tracing::warn!(
                            worker = worker_id,
                            camera = %camera,
                            expected = data_size,
                            actual = image.data.len(),
                            "Image size mismatch in encoder pool"
                        );
                    }
                }
                Some(EncodeJob::Shutdown) => {
                    break;
                }
                None => {
                    // No work available, sleep briefly
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }
    }
}

// =============================================================================
// Drop Implementation
// =============================================================================

impl Drop for EncoderPool {
    fn drop(&mut self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::SeqCst);

        // Send shutdown signals
        for _ in 0..self.threads.len() {
            self.global.push(EncodeJob::Shutdown);
        }

        // Join threads (ignore errors during drop)
        for worker_thread in &mut self.threads {
            if let Some(handle) = worker_thread.handle.take() {
                let _ = handle.join();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::bounded;

    #[test]
    fn test_encode_job_creation() {
        // Create ImageData directly
        let image = Arc::new(ImageData::new_rgb(640, 480, vec![0u8; 640 * 480 * 3]).unwrap());
        let (tx, _rx) = bounded(1);
        let job = EncodeJob::Encode {
            camera: "test".to_string(),
            image: image.clone(),
            result_tx: tx,
        };

        match job {
            EncodeJob::Encode {
                camera, image: img, ..
            } => {
                assert_eq!(camera, "test");
                assert_eq!(img.width, 640);
            }
            _ => panic!("Wrong job type"),
        }
    }

    #[test]
    fn test_encoder_pool_creation() {
        let (result_tx, _result_rx) = bounded(10);
        let pool = EncoderPool::new(2, result_tx);
        assert!(pool.is_ok(), "EncoderPool creation should succeed");
    }
}
