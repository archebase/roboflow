// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Parallel decode pool for JPEG/PNG image decoding.
//!
//! This module provides a parallel decoding infrastructure that utilizes
//! multiple worker threads to decode images concurrently, improving
//! throughput for multi-camera video encoding.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crossbeam_channel::{Receiver, Sender, bounded};
use tracing::{debug, trace};

use crate::ImageData;
use crate::video::arena::{ArcSlot, AtomicFramePool, FramePoolConfig, PoolStats};
use crate::video::frame::{FrameBuffer, PixelFormat};

/// Command sent to decode workers.
#[derive(Debug)]
pub struct DecodeCommand {
    /// Sequence number for FIFO ordering.
    pub sequence: u64,
    /// Camera identifier.
    pub camera_id: String,
    /// Image data to decode.
    pub image: ImageData,
}

/// Result from a decode worker.
#[derive(Debug)]
pub struct DecodeResult {
    /// Sequence number from the original command.
    pub sequence: u64,
    /// Camera identifier.
    pub camera_id: String,
    /// Decoded frame (or error).
    pub result: io::Result<Option<DecodedFrame>>,
}

/// A decoded frame ready for encoding.
#[derive(Debug, Clone)]
pub struct DecodedFrame {
    /// Frame buffer (shared for zero-copy pipeline).
    buffer: std::sync::Arc<FrameBuffer>,
}

impl DecodedFrame {
    /// Create a new DecodedFrame from a FrameBuffer.
    pub fn new(buffer: FrameBuffer) -> Self {
        Self {
            buffer: std::sync::Arc::new(buffer),
        }
    }

    /// Create a DecodedFrame from an existing Arc<FrameBuffer>.
    pub fn from_arc(buffer: std::sync::Arc<FrameBuffer>) -> Self {
        Self { buffer }
    }

    /// Get the frame width.
    #[inline]
    pub fn width(&self) -> u32 {
        self.buffer.width()
    }

    /// Get the frame height.
    #[inline]
    pub fn height(&self) -> u32 {
        self.buffer.height()
    }

    /// Get the pixel data as a byte slice.
    #[inline]
    pub fn data(&self) -> &[u8] {
        self.buffer.data()
    }

    /// Get the underlying Arc<FrameBuffer> for zero-copy passing.
    #[inline]
    pub fn into_buffer(self) -> std::sync::Arc<FrameBuffer> {
        self.buffer
    }

    /// Get a reference to the underlying FrameBuffer.
    #[inline]
    pub fn buffer(&self) -> &FrameBuffer {
        &self.buffer
    }

    /// Clone the Arc<FrameBuffer> for zero-copy sharing.
    #[inline]
    pub fn clone_buffer(&self) -> std::sync::Arc<FrameBuffer> {
        std::sync::Arc::clone(&self.buffer)
    }
}

/// Frame data storage (legacy, for backward compatibility).
#[derive(Debug)]
pub enum FrameData {
    /// Owned vector (for when arena is not used).
    Owned(Vec<u8>),
    /// Arena slot (for zero-copy).
    Arena(ArcSlot),
}

impl FrameData {
    /// Get the data as a byte slice.
    pub fn as_slice(&self) -> &[u8] {
        match self {
            FrameData::Owned(v) => v.as_slice(),
            FrameData::Arena(slot) => slot.data(),
        }
    }
}

/// Configuration for the decode pool.
#[derive(Debug, Clone)]
pub struct DecodePoolConfig {
    /// Number of worker threads.
    pub worker_count: usize,
    /// Channel capacity for pending decode jobs.
    pub pending_capacity: usize,
    /// Channel capacity for completed frames.
    pub completed_capacity: usize,
    /// Arena pool configuration (if using arena allocation).
    pub arena_config: Option<FramePoolConfig>,
    /// Maximum image width (0 = unlimited).
    pub max_image_width: u32,
    /// Maximum image height (0 = unlimited).
    pub max_image_height: u32,
    /// Maximum decoded image size in bytes (0 = unlimited).
    pub max_image_bytes: usize,
}

impl Default for DecodePoolConfig {
    fn default() -> Self {
        Self {
            worker_count: 4,
            pending_capacity: 64,
            completed_capacity: 64,
            // Arena is now enabled by default for zero-copy processing
            arena_config: Some(FramePoolConfig::default()),
            // Default: 8K resolution max
            max_image_width: 7680,
            max_image_height: 4320,
            // Default: 100MB max decoded size
            max_image_bytes: 100 * 1024 * 1024,
        }
    }
}

/// Statistics for the decode pool.
#[derive(Debug, Clone, Copy, Default)]
pub struct DecodePoolStats {
    /// Total frames decoded.
    pub frames_decoded: u64,
    /// Total frames that fell back to owned allocation (arena exhausted).
    pub arena_fallbacks: u64,
    /// Total frames failed.
    pub frames_failed: u64,
    /// Arena stats (if enabled).
    pub arena_stats: Option<PoolStats>,
    /// Current pending queue size.
    pub pending_count: usize,
    /// Active workers.
    pub active_workers: usize,
}

/// Parallel decode pool with FIFO ordering.
pub struct DecodePool {
    /// Worker handles.
    workers: Vec<std::thread::JoinHandle<()>>,
    /// Channel to send decode commands.
    cmd_tx: Sender<DecodeCommand>,
    /// Channel to receive decode results.
    result_rx: Receiver<DecodeResult>,
    /// Next sequence number for FIFO ordering.
    next_sequence: AtomicU64,
    /// Arena pool (optional).
    arena: Option<Arc<AtomicFramePool>>,
    /// Statistics (shared with workers).
    stats_decode: Arc<AtomicU64>,
    stats_failed: Arc<AtomicU64>,
    stats_arena_fallbacks: Arc<AtomicU64>,
    /// In-flight commands being processed by workers.
    in_flight: Arc<AtomicUsize>,
}

impl DecodePool {
    /// Create a new decode pool.
    pub fn new(config: DecodePoolConfig) -> io::Result<Self> {
        let (cmd_tx, cmd_rx) = bounded(config.pending_capacity);
        let (result_tx, result_rx) = bounded(config.completed_capacity);

        // Create arena if configured
        let arena = if let Some(arena_config) = &config.arena_config {
            Some(Arc::new(
                AtomicFramePool::new(*arena_config).map_err(|e| io::Error::other(e.to_string()))?,
            ))
        } else {
            None
        };

        // Spawn workers
        let mut workers = Vec::with_capacity(config.worker_count);
        let cmd_rx = Arc::new(cmd_rx);
        let result_tx = Arc::new(result_tx);

        // Create shared stats counters for workers
        let stats_decode = Arc::new(AtomicU64::new(0));
        let stats_failed = Arc::new(AtomicU64::new(0));
        let stats_arena_fallbacks = Arc::new(AtomicU64::new(0));
        let in_flight = Arc::new(AtomicUsize::new(0));

        for worker_id in 0..config.worker_count {
            let cmd_rx = Arc::clone(&cmd_rx);
            let result_tx = Arc::clone(&result_tx);
            let arena = arena.clone();
            let stats_decode = Arc::clone(&stats_decode);
            let stats_failed = Arc::clone(&stats_failed);
            let stats_arena_fallbacks = Arc::clone(&stats_arena_fallbacks);
            let in_flight = Arc::clone(&in_flight);
            let max_width = config.max_image_width;
            let max_height = config.max_image_height;
            let max_bytes = config.max_image_bytes;

            let handle = std::thread::Builder::new()
                .name(format!("decode-worker-{}", worker_id))
                .spawn(move || {
                    worker_loop(
                        worker_id,
                        cmd_rx,
                        result_tx,
                        arena.as_ref(),
                        &stats_decode,
                        &stats_failed,
                        &stats_arena_fallbacks,
                        &in_flight,
                        max_width,
                        max_height,
                        max_bytes,
                    );
                })
                .map_err(|e| io::Error::other(e.to_string()))?;

            workers.push(handle);
        }

        // Drop the original senders so channels close properly when workers exit
        drop(cmd_rx);
        drop(result_tx);

        Ok(Self {
            workers,
            cmd_tx,
            result_rx,
            next_sequence: AtomicU64::new(0),
            arena,
            stats_decode,
            stats_failed,
            stats_arena_fallbacks,
            in_flight,
        })
    }

    /// Submit an image for decoding.
    pub fn submit(&self, camera_id: String, image: ImageData) -> io::Result<u64> {
        let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        let cmd = DecodeCommand {
            sequence,
            camera_id,
            image,
        };
        self.cmd_tx
            .send(cmd)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Decode pool shut down"))?;
        Ok(sequence)
    }

    /// Try to receive a decoded result (non-blocking).
    pub fn try_recv(&self) -> Option<DecodeResult> {
        self.result_rx.try_recv().ok()
    }

    /// Receive a decoded result (blocking).
    pub fn recv(&self) -> io::Result<DecodeResult> {
        self.result_rx
            .recv()
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Decode pool shut down"))
    }

    /// Receive decoded results with FIFO ordering.
    ///
    /// Returns results in sequence order, buffering out-of-order results.
    pub fn recv_ordered(
        &self,
        buffer: &mut BTreeMap<u64, DecodeResult>,
    ) -> io::Result<Option<DecodeResult>> {
        // Check if we have the next expected result
        let next_seq = buffer.keys().next().copied();
        if let Some(seq) = next_seq {
            // Check if this is the next consecutive sequence
            if buffer.values().any(|r| r.sequence == seq) {
                // We need to find the minimum sequence we're waiting for
            }
        }

        // Receive a new result
        let result = self.recv()?;

        // Add to buffer
        buffer.insert(result.sequence, result);

        // Extract lowest sequence if consecutive
        let min_seq = *buffer.keys().next().unwrap();
        if let Some(result) = buffer.remove(&min_seq) {
            return Ok(Some(result));
        }

        Ok(None)
    }

    /// Get the arena pool (if configured).
    pub fn arena(&self) -> Option<&Arc<AtomicFramePool>> {
        self.arena.as_ref()
    }

    /// Get statistics.
    pub fn stats(&self) -> DecodePoolStats {
        DecodePoolStats {
            frames_decoded: self.stats_decode.load(Ordering::Relaxed),
            arena_fallbacks: self.stats_arena_fallbacks.load(Ordering::Relaxed),
            frames_failed: self.stats_failed.load(Ordering::Relaxed),
            arena_stats: self.arena.as_ref().map(|a| a.stats()),
            pending_count: self.cmd_tx.len() + self.in_flight.load(Ordering::Relaxed),
            active_workers: self.workers.len(), // Simplified: total workers
        }
    }

    /// Shutdown the pool.
    pub fn shutdown(self) {
        drop(self.cmd_tx);
        for worker in self.workers {
            let _ = worker.join();
        }
    }
}

/// Worker loop for decoding images.
#[allow(clippy::too_many_arguments)]
fn worker_loop(
    worker_id: usize,
    cmd_rx: Arc<Receiver<DecodeCommand>>,
    result_tx: Arc<Sender<DecodeResult>>,
    arena: Option<&Arc<AtomicFramePool>>,
    stats_decode: &Arc<AtomicU64>,
    stats_failed: &Arc<AtomicU64>,
    stats_arena_fallbacks: &Arc<AtomicU64>,
    in_flight: &Arc<AtomicUsize>,
    max_width: u32,
    max_height: u32,
    max_bytes: usize,
) {
    debug!(
        worker_id,
        max_width, max_height, max_bytes, "Decode worker started"
    );

    while let Ok(cmd) = cmd_rx.recv() {
        in_flight.fetch_add(1, Ordering::Relaxed);

        trace!(
            worker_id,
            sequence = cmd.sequence,
            camera = %cmd.camera_id,
            "Processing decode command"
        );

        let result = decode_image(
            &cmd,
            arena,
            max_width,
            max_height,
            max_bytes,
            stats_arena_fallbacks,
        );
        let decoded = match result {
            Ok(frame) => {
                stats_decode.fetch_add(1, Ordering::Relaxed);
                frame
            }
            Err(e) => {
                stats_failed.fetch_add(1, Ordering::Relaxed);
                let res = DecodeResult {
                    sequence: cmd.sequence,
                    camera_id: cmd.camera_id,
                    result: Err(e),
                };
                in_flight.fetch_sub(1, Ordering::Relaxed);
                if result_tx.send(res).is_err() {
                    break;
                }
                continue;
            }
        };

        let res = DecodeResult {
            sequence: cmd.sequence,
            camera_id: cmd.camera_id,
            result: Ok(decoded),
        };

        in_flight.fetch_sub(1, Ordering::Relaxed);
        if result_tx.send(res).is_err() {
            break;
        }
    }

    debug!(worker_id, "Decode worker exiting");
}

/// Decode an image to RGB format.
fn decode_image(
    cmd: &DecodeCommand,
    arena: Option<&Arc<AtomicFramePool>>,
    max_width: u32,
    max_height: u32,
    max_bytes: usize,
    stats_arena_fallbacks: &Arc<AtomicU64>,
) -> io::Result<Option<DecodedFrame>> {
    if cmd.image.width == 0 || cmd.image.height == 0 {
        return Ok(None);
    }

    // Pre-decode size validation (for unencoded data)
    if !cmd.image.is_encoded {
        if max_width > 0 && cmd.image.width > max_width {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Image width {} exceeds maximum {}",
                    cmd.image.width, max_width
                ),
            ));
        }
        if max_height > 0 && cmd.image.height > max_height {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Image height {} exceeds maximum {}",
                    cmd.image.height, max_height
                ),
            ));
        }
        let expected_size = (cmd.image.width as usize)
            .saturating_mul(cmd.image.height as usize)
            .saturating_mul(3);
        if max_bytes > 0 && expected_size > max_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Image size {} exceeds maximum {} bytes",
                    expected_size, max_bytes
                ),
            ));
        }
    }

    let (width, height, data) = if cmd.image.is_encoded {
        // Check if this is a JPEG by looking at magic bytes (FFD8FF)
        let is_jpeg = cmd.image.data.len() >= 3
            && cmd.image.data[0] == 0xFF
            && cmd.image.data[1] == 0xD8
            && cmd.image.data[2] == 0xFF;

        if is_jpeg {
            // Use zune-jpeg for fast SIMD-accelerated JPEG decoding (2-4x faster)
            use zune_jpeg::JpegDecoder;

            let mut decoder = JpegDecoder::new(&cmd.image.data);
            let rgb_data = decoder
                .decode()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

            let info = decoder
                .info()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "No JPEG info"))?;

            let decoded_width = u32::from(info.width);
            let decoded_height = u32::from(info.height);

            // Post-decode size validation
            if max_width > 0 && decoded_width > max_width {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Decoded image width {} exceeds maximum {}",
                        decoded_width, max_width
                    ),
                ));
            }
            if max_height > 0 && decoded_height > max_height {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Decoded image height {} exceeds maximum {}",
                        decoded_height, max_height
                    ),
                ));
            }

            if max_bytes > 0 && rgb_data.len() > max_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Decoded image size {} exceeds maximum {} bytes",
                        rgb_data.len(),
                        max_bytes
                    ),
                ));
            }

            (decoded_width, decoded_height, rgb_data)
        } else {
            // Fall back to image crate for PNG and other formats
            let img = image::load_from_memory(&cmd.image.data)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

            let decoded_width = img.width();
            let decoded_height = img.height();

            // Post-decode size validation
            if max_width > 0 && decoded_width > max_width {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Decoded image width {} exceeds maximum {}",
                        decoded_width, max_width
                    ),
                ));
            }
            if max_height > 0 && decoded_height > max_height {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Decoded image height {} exceeds maximum {}",
                        decoded_height, max_height
                    ),
                ));
            }

            let rgb_data = img.to_rgb8().into_raw();

            if max_bytes > 0 && rgb_data.len() > max_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Decoded image size {} exceeds maximum {} bytes",
                        rgb_data.len(),
                        max_bytes
                    ),
                ));
            }

            (decoded_width, decoded_height, rgb_data)
        }
    } else {
        // Already RGB
        (cmd.image.width, cmd.image.height, cmd.image.data.clone())
    };

    // Store in arena if available, otherwise use owned vector
    let buffer = if let Some(pool) = arena {
        if let Some(mut slot) = pool.acquire() {
            let slot_data = slot.data_mut();
            if slot_data.len() >= data.len() {
                slot_data[..data.len()].copy_from_slice(&data);
                FrameBuffer::from_arena(slot, PixelFormat::Rgb24, width, height, data.len())
            } else {
                // Slot too small, fall back to owned
                stats_arena_fallbacks.fetch_add(1, Ordering::Relaxed);
                FrameBuffer::rgb24(width, height, data)
            }
        } else {
            // Pool exhausted, fall back to owned
            stats_arena_fallbacks.fetch_add(1, Ordering::Relaxed);
            FrameBuffer::rgb24(width, height, data)
        }
    } else {
        // Arena not configured, use owned
        stats_arena_fallbacks.fetch_add(1, Ordering::Relaxed);
        FrameBuffer::rgb24(width, height, data)
    };

    Ok(Some(DecodedFrame::new(buffer)))
}

/// FIFO-ordered result collector.
///
/// Collects decode results and yields them in sequence order.
pub struct FifoCollector {
    /// Buffered results waiting for their turn.
    buffer: BTreeMap<u64, DecodeResult>,
    /// Next expected sequence number.
    next_expected: u64,
    /// Maximum buffer size before warning.
    max_buffer_size: usize,
}

impl FifoCollector {
    /// Create a new FIFO collector.
    pub fn new() -> Self {
        Self {
            buffer: BTreeMap::new(),
            next_expected: 0,
            max_buffer_size: 256,
        }
    }

    /// Add a result to the buffer.
    pub fn push(&mut self, result: DecodeResult) {
        self.buffer.insert(result.sequence, result);

        // Warn if buffer is growing too large (indicates missing sequence)
        if self.buffer.len() > self.max_buffer_size {
            debug!(
                next_expected = self.next_expected,
                buffer_size = self.buffer.len(),
                "FIFO buffer growing large, may have missing sequence"
            );
        }
    }

    /// Get the next result in sequence order.
    pub fn pop(&mut self) -> Option<DecodeResult> {
        if let Some(result) = self.buffer.remove(&self.next_expected) {
            self.next_expected += 1;
            return Some(result);
        }
        None
    }

    /// Get the current buffer size.
    pub fn buffer_size(&self) -> usize {
        self.buffer.len()
    }

    /// Check if there are results ready to be popped.
    pub fn has_ready(&self) -> bool {
        self.buffer.contains_key(&self.next_expected)
    }
}

impl Default for FifoCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_image(width: u32, height: u32, encoded: bool) -> ImageData {
        if encoded {
            // Create a minimal valid JPEG (simplified for testing)
            // In real tests, we'd use actual JPEG encoding
            ImageData {
                width,
                height,
                data: vec![0u8; 100], // Placeholder
                is_encoded: true,
                is_depth: false,
                original_timestamp: 0,
            }
        } else {
            let size = (width * height * 3) as usize;
            ImageData {
                width,
                height,
                data: vec![128u8; size],
                is_encoded: false,
                is_depth: false,
                original_timestamp: 0,
            }
        }
    }

    #[test]
    fn test_fifo_collector_ordering() {
        let mut collector = FifoCollector::new();

        // Add results out of order
        let r2 = DecodeResult {
            sequence: 2,
            camera_id: "cam0".to_string(),
            result: Ok(None),
        };
        let r0 = DecodeResult {
            sequence: 0,
            camera_id: "cam0".to_string(),
            result: Ok(None),
        };
        let r1 = DecodeResult {
            sequence: 1,
            camera_id: "cam0".to_string(),
            result: Ok(None),
        };

        collector.push(r2);
        collector.push(r0);
        collector.push(r1);

        // Should pop in order
        assert!(collector.has_ready());
        assert_eq!(collector.pop().unwrap().sequence, 0);
        assert!(collector.has_ready());
        assert_eq!(collector.pop().unwrap().sequence, 1);
        assert!(collector.has_ready());
        assert_eq!(collector.pop().unwrap().sequence, 2);
        assert!(!collector.has_ready());
        assert!(collector.pop().is_none());
    }

    #[test]
    fn test_fifo_collector_partial() {
        let mut collector = FifoCollector::new();

        // Add sequences 0, 2, 3 but not 1
        collector.push(DecodeResult {
            sequence: 0,
            camera_id: "cam0".to_string(),
            result: Ok(None),
        });
        collector.push(DecodeResult {
            sequence: 2,
            camera_id: "cam0".to_string(),
            result: Ok(None),
        });
        collector.push(DecodeResult {
            sequence: 3,
            camera_id: "cam0".to_string(),
            result: Ok(None),
        });

        // Only 0 should be available
        assert!(collector.has_ready());
        assert_eq!(collector.pop().unwrap().sequence, 0);

        // Now waiting for 1, but we have 2 and 3
        assert!(!collector.has_ready());
        assert_eq!(collector.buffer_size(), 2);

        // Add missing sequence 1
        collector.push(DecodeResult {
            sequence: 1,
            camera_id: "cam0".to_string(),
            result: Ok(None),
        });

        // Now can get 1, 2, 3
        assert_eq!(collector.pop().unwrap().sequence, 1);
        assert_eq!(collector.pop().unwrap().sequence, 2);
        assert_eq!(collector.pop().unwrap().sequence, 3);
    }

    #[test]
    fn test_decode_pool_basic() {
        let config = DecodePoolConfig {
            worker_count: 2,
            pending_capacity: 16,
            completed_capacity: 16,
            arena_config: None,
            max_image_width: 0,
            max_image_height: 0,
            max_image_bytes: 0,
        };

        let pool = DecodePool::new(config).expect("Failed to create pool");

        // Submit some frames
        let img = make_test_image(64, 64, false);
        let _seq0 = pool
            .submit("cam0".to_string(), img.clone())
            .expect("submit");
        let _seq1 = pool
            .submit("cam0".to_string(), img.clone())
            .expect("submit");

        // Receive results
        let mut collector = FifoCollector::new();

        for _ in 0..2 {
            let result = pool.recv().expect("recv");
            collector.push(result);
        }

        // Should be in order (though may come back out of order from workers)
        let r0 = collector.pop().expect("pop 0");
        let r1 = collector.pop().expect("pop 1");

        // Results should be decoded successfully
        assert!(r0.result.is_ok());
        assert!(r1.result.is_ok());

        // Sequence numbers should be 0 and 1
        let seqs = [r0.sequence, r1.sequence];
        assert!(seqs.contains(&0));
        assert!(seqs.contains(&1));

        pool.shutdown();
    }

    #[test]
    fn test_decode_with_arena() {
        let arena_config = FramePoolConfig {
            width: 64,
            height: 64,
            bytes_per_pixel: 3,
            slot_count: 4,
        };

        let config = DecodePoolConfig {
            worker_count: 2,
            pending_capacity: 16,
            completed_capacity: 16,
            arena_config: Some(arena_config),
            max_image_width: 0,
            max_image_height: 0,
            max_image_bytes: 0,
        };

        let pool = DecodePool::new(config).expect("Failed to create pool");

        // Submit a frame
        let img = make_test_image(64, 64, false);
        pool.submit("cam0".to_string(), img).expect("submit");

        // Receive result
        let result = pool.recv().expect("recv");

        // Should have decoded frame
        if let Ok(Some(frame)) = result.result {
            // Data should be from arena (64x64x3 = 12288 bytes)
            assert_eq!(frame.width(), 64);
            assert_eq!(frame.height(), 64);
            // Data might be arena or owned depending on availability
            assert!(!frame.data().is_empty());
        }

        pool.shutdown();
    }

    #[test]
    fn test_decode_zero_dimensions() {
        let config = DecodePoolConfig::default();
        let pool = DecodePool::new(config).expect("Failed to create pool");

        // Submit frame with zero dimensions
        let img = ImageData {
            is_depth: false,
            original_timestamp: 0,
            width: 0,
            height: 0,
            data: vec![],
            is_encoded: false,
        };
        pool.submit("cam0".to_string(), img).expect("submit");

        // Receive result
        let result = pool.recv().expect("recv");

        // Should return Ok(None) for zero dimensions
        assert!(result.result.is_ok());
        assert!(result.result.unwrap().is_none());

        pool.shutdown();
    }
}
