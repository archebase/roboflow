// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Lock-free ring buffer for frame streaming between capture and encode threads.
//!
//! This module provides a bounded ring buffer for passing video frames from
//! a capture thread to an encoding thread with backpressure handling.

use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::formats::common::video::VideoFrame;

/// Error type for ring buffer operations.
#[derive(Debug, Clone, PartialEq)]
pub enum RingBufferError {
    /// Buffer is full, cannot push more frames
    Full,
    /// Buffer is empty, nothing to pop
    Empty,
    /// Buffer has been closed
    Closed,
    /// Timeout waiting for space or data
    Timeout,
}

impl std::fmt::Display for RingBufferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full => write!(f, "Ring buffer is full"),
            Self::Empty => write!(f, "Ring buffer is empty"),
            Self::Closed => write!(f, "Ring buffer is closed"),
            Self::Timeout => write!(f, "Ring buffer operation timed out"),
        }
    }
}

impl std::error::Error for RingBufferError {}

/// A slot in the ring buffer that can be safely accessed from multiple threads.
struct RingBufferSlot {
    /// The frame data (using UnsafeCell for interior mutability)
    data: UnsafeCell<Option<VideoFrame>>,
}

// SAFETY: We only access the data from within the ring buffer's methods
// which use proper atomic ordering on the indices to synchronize access.
unsafe impl Send for RingBufferSlot {}
unsafe impl Sync for RingBufferSlot {}

/// A lock-free ring buffer for video frames.
///
/// This buffer provides:
/// - Bounded capacity to prevent unbounded memory growth
/// - Backpressure when full (blocking push with timeout)
/// - Thread-safe operations using atomics
/// - Efficient cache-friendly storage
///
/// # Example
///
/// ```ignore
/// use roboflow_dataset::formats::common::ring_buffer::FrameRingBuffer;
/// use roboflow_dataset::formats::common::VideoFrame;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let buffer = FrameRingBuffer::new(128);
/// let frame = VideoFrame::new(640, 480, vec![0u8; 640 * 480 * 3]);
/// buffer.try_push(frame)?;
/// let frame_out = buffer.try_pop().ok_or("No frame")?;
/// # Ok(())
/// # }
/// ```
pub struct FrameRingBuffer {
    /// Ring buffer storage
    buffer: Vec<RingBufferSlot>,

    /// Capacity (must be power of 2 for efficient masking)
    capacity: usize,

    /// Mask for efficient modulo (capacity - 1)
    mask: usize,

    /// Write index (where next frame will be written)
    write_idx: Arc<AtomicUsize>,

    /// Read index (where next frame will be read from)
    read_idx: Arc<AtomicUsize>,

    /// Whether the buffer is closed
    closed: Arc<AtomicUsize>,
}

impl FrameRingBuffer {
    /// Create a new ring buffer with the given capacity.
    ///
    /// The capacity will be rounded up to the next power of 2 for
    /// efficient indexing using bit masking.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Maximum number of frames to buffer (recommended: 64-256)
    ///
    /// # Panics
    ///
    /// Panics if capacity is 0.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use roboflow_dataset::formats::common::ring_buffer::FrameRingBuffer;
    ///
    /// let buffer = FrameRingBuffer::new(128);
    /// assert_eq!(buffer.capacity(), 128);
    /// ```
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "Ring buffer capacity must be > 0");

        // Round up to next power of 2 for efficient masking
        let capacity = capacity.next_power_of_two();
        let mask = capacity - 1;

        Self {
            buffer: (0..capacity)
                .map(|_| RingBufferSlot {
                    data: UnsafeCell::new(None),
                })
                .collect(),
            capacity,
            mask,
            write_idx: Arc::new(AtomicUsize::new(0)),
            read_idx: Arc::new(AtomicUsize::new(0)),
            closed: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Get the capacity of the buffer.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get the current number of frames in the buffer.
    #[must_use]
    pub fn len(&self) -> usize {
        let write = self.write_idx.load(Ordering::Acquire);
        let read = self.read_idx.load(Ordering::Acquire);
        write.wrapping_sub(read)
    }

    /// Check if the buffer is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Check if the buffer is full.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.len() == self.capacity
    }

    /// Close the buffer.
    ///
    /// After closing, all push operations will return `RingBufferError::Closed`.
    /// Existing frames can still be popped until the buffer is empty.
    pub fn close(&self) {
        self.closed.store(1, Ordering::Release);
    }

    /// Check if the buffer is closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire) != 0
    }

    /// Push a frame into the buffer.
    ///
    /// This method will block if the buffer is full, waiting up to the
    /// specified timeout for space to become available.
    ///
    /// # Arguments
    ///
    /// * `frame` - The video frame to push
    /// * `timeout` - Maximum time to wait if buffer is full
    ///
    /// # Errors
    ///
    /// Returns `RingBufferError::Full` if the buffer is full and timeout expires.
    /// Returns `RingBufferError::Closed` if the buffer has been closed.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use roboflow_dataset::formats::common::ring_buffer::FrameRingBuffer;
    /// use roboflow_dataset::formats::common::video::VideoFrame;
    /// use std::time::Duration;
    /// let buffer = FrameRingBuffer::new(128);
    /// let frame = VideoFrame::new(640, 480, vec![0; 640 * 480 * 3]);
    /// buffer.push_with_timeout(frame, Duration::from_millis(100))?;
    /// ```
    pub fn push_with_timeout(
        &self,
        frame: VideoFrame,
        timeout: Duration,
    ) -> Result<(), RingBufferError> {
        let start = std::time::Instant::now();

        loop {
            // Check if closed
            if self.is_closed() {
                return Err(RingBufferError::Closed);
            }

            // Try to push
            if self.try_push(frame.clone()).is_ok() {
                return Ok(());
            }

            // Check timeout
            if start.elapsed() >= timeout {
                return Err(RingBufferError::Timeout);
            }

            // Yield to reduce CPU spinning
            std::hint::spin_loop();
        }
    }

    /// Try to push a frame into the buffer without blocking.
    ///
    /// # Errors
    ///
    /// Returns `RingBufferError::Full` if the buffer is full.
    /// Returns `RingBufferError::Closed` if the buffer has been closed.
    pub fn try_push(&self, frame: VideoFrame) -> Result<(), RingBufferError> {
        if self.is_closed() {
            return Err(RingBufferError::Closed);
        }

        let write = self.write_idx.load(Ordering::Acquire);
        let read = self.read_idx.load(Ordering::Acquire);

        // Check if buffer is full
        if write.wrapping_sub(read) >= self.capacity {
            return Err(RingBufferError::Full);
        }

        // SAFETY: We have exclusive access to this slot because:
        // 1. The write index ensures only one writer at a time
        // 2. The read index ensures this slot is not being read
        let slot = unsafe { &mut *self.buffer[write & self.mask].data.get() };
        *slot = Some(frame);

        // Advance write index
        self.write_idx
            .store(write.wrapping_add(1), Ordering::Release);

        Ok(())
    }

    /// Pop a frame from the buffer.
    ///
    /// This method will block if the buffer is empty, waiting up to the
    /// specified timeout for a frame to become available.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Maximum time to wait if buffer is empty
    ///
    /// # Errors
    ///
    /// Returns `RingBufferError::Empty` if the buffer is empty and timeout expires.
    /// Returns `RingBufferError::Closed` if the buffer is closed and empty.
    pub fn pop_with_timeout(&self, timeout: Duration) -> Result<VideoFrame, RingBufferError> {
        let start = std::time::Instant::now();

        loop {
            // Check if closed and empty
            if self.is_closed() && self.is_empty() {
                return Err(RingBufferError::Closed);
            }

            // Try to pop
            if let Some(frame) = self.try_pop() {
                return Ok(frame);
            }

            // Check timeout
            if start.elapsed() >= timeout {
                return Err(RingBufferError::Timeout);
            }

            // Yield to reduce CPU spinning
            std::hint::spin_loop();
        }
    }

    /// Try to pop a frame from the buffer without blocking.
    ///
    /// Returns `None` if the buffer is empty.
    #[must_use]
    pub fn try_pop(&self) -> Option<VideoFrame> {
        let read = self.read_idx.load(Ordering::Acquire);
        let write = self.write_idx.load(Ordering::Acquire);

        // Check if buffer is empty
        if read == write {
            return None;
        }

        // SAFETY: We have exclusive access to this slot because:
        // 1. The read index ensures only one reader at a time
        // 2. The write index ensures this slot is done being written
        let slot = unsafe { &mut *self.buffer[read & self.mask].data.get() };
        let frame = slot.take();

        // Advance read index
        self.read_idx.store(read.wrapping_add(1), Ordering::Release);

        frame
    }

    /// Get a snapshot of the buffer's current state.
    #[must_use]
    pub fn snapshot(&self) -> RingBufferSnapshot {
        RingBufferSnapshot {
            capacity: self.capacity,
            len: self.len(),
            is_empty: self.is_empty(),
            is_full: self.is_full(),
            is_closed: self.is_closed(),
        }
    }
}

impl Clone for FrameRingBuffer {
    fn clone(&self) -> Self {
        // Create a new buffer sharing the same indices
        // This allows multiple threads to have references to the same buffer
        Self {
            buffer: (0..self.capacity)
                .map(|_| RingBufferSlot {
                    data: UnsafeCell::new(None),
                })
                .collect(),
            capacity: self.capacity,
            mask: self.mask,
            write_idx: Arc::clone(&self.write_idx),
            read_idx: Arc::clone(&self.read_idx),
            closed: Arc::clone(&self.closed),
        }
    }
}

/// A snapshot of the ring buffer's state.
#[derive(Debug, Clone, Copy)]
pub struct RingBufferSnapshot {
    /// Total capacity of the buffer
    pub capacity: usize,

    /// Current number of frames in the buffer
    pub len: usize,

    /// Whether the buffer is empty
    pub is_empty: bool,

    /// Whether the buffer is full
    pub is_full: bool,

    /// Whether the buffer is closed
    pub is_closed: bool,
}

impl RingBufferSnapshot {
    /// Get the buffer fill ratio (0.0 to 1.0).
    #[must_use]
    pub fn fill_ratio(&self) -> f64 {
        if self.capacity == 0 {
            0.0
        } else {
            self.len as f64 / self.capacity as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ring_buffer_creation() {
        let buffer = FrameRingBuffer::new(100);
        // Capacity is rounded up to power of 2
        assert_eq!(buffer.capacity(), 128);
        assert!(buffer.is_empty());
        assert!(!buffer.is_full());
        assert!(!buffer.is_closed());
    }

    #[test]
    fn test_ring_buffer_push_pop() {
        let buffer = FrameRingBuffer::new(4);
        let frame = VideoFrame::new(640, 480, vec![0; 640 * 480 * 3]);

        // Push and pop
        buffer.try_push(frame.clone()).unwrap();
        assert_eq!(buffer.len(), 1);

        let popped = buffer.try_pop().unwrap();
        assert_eq!(popped.width, frame.width);
        assert_eq!(popped.height, frame.height);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_ring_buffer_full() {
        let buffer = FrameRingBuffer::new(4); // Capacity = 4
        let frame = VideoFrame::new(100, 100, vec![0; 100 * 100 * 3]);

        // Fill the buffer
        for _ in 0..4 {
            buffer.try_push(frame.clone()).unwrap();
        }

        assert!(buffer.is_full());

        // Try to push when full
        let result = buffer.try_push(frame);
        assert_eq!(result, Err(RingBufferError::Full));
    }

    #[test]
    fn test_ring_buffer_empty_pop() {
        let buffer = FrameRingBuffer::new(4);

        // Pop from empty buffer
        let result = buffer.try_pop();
        assert!(result.is_none());
    }

    #[test]
    fn test_ring_buffer_close() {
        let buffer = FrameRingBuffer::new(4);
        let frame = VideoFrame::new(100, 100, vec![0; 100 * 100 * 3]);

        // Close the buffer
        buffer.close();
        assert!(buffer.is_closed());

        // Push after close
        let result = buffer.try_push(frame.clone());
        assert_eq!(result, Err(RingBufferError::Closed));

        // Pop from closed but non-empty buffer
        let buffer2 = FrameRingBuffer::new(4);
        buffer2.try_push(frame.clone()).unwrap();
        buffer2.close();
        // Can still pop existing frames
        assert!(buffer2.try_pop().is_some());
        // But now it's empty and closed
        let result = buffer2.try_pop();
        assert!(result.is_none());
    }

    #[test]
    fn test_ring_buffer_wraparound() {
        let buffer = FrameRingBuffer::new(4);
        let frame = VideoFrame::new(100, 100, vec![0; 100 * 100 * 3]);

        // Fill and drain multiple times to test wraparound
        for _ in 0..3 {
            // Fill
            for _ in 0..4 {
                buffer.try_push(frame.clone()).unwrap();
            }
            assert!(buffer.is_full());

            // Drain
            for _ in 0..4 {
                buffer.try_pop().unwrap();
            }
            assert!(buffer.is_empty());
        }
    }

    #[test]
    fn test_ring_buffer_snapshot() {
        let buffer = FrameRingBuffer::new(16);
        let frame = VideoFrame::new(100, 100, vec![0; 100 * 100 * 3]);

        // Add some frames
        for _ in 0..4 {
            buffer.try_push(frame.clone()).unwrap();
        }

        let snapshot = buffer.snapshot();
        assert_eq!(snapshot.capacity, 16);
        assert_eq!(snapshot.len, 4);
        assert!(!snapshot.is_empty);
        assert!(!snapshot.is_full);
        assert!(!snapshot.is_closed);
        assert_eq!(snapshot.fill_ratio(), 0.25);
    }

    #[test]
    fn test_ring_buffer_clone() {
        let buffer = FrameRingBuffer::new(8);
        let frame = VideoFrame::new(100, 100, vec![0; 100 * 100 * 3]);

        // Clone shares the same underlying buffer (same atomic indices)
        let buffer_clone = buffer.clone();

        buffer.try_push(frame.clone()).unwrap();

        // Both see the same length
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer_clone.len(), 1);

        // Popping from either consumes the frame
        let popped = buffer.try_pop();
        assert!(popped.is_some());
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer_clone.len(), 0);

        // The clone can no longer pop since the frame was consumed
        assert!(buffer_clone.try_pop().is_none());
    }

    // =============================================================================
    // Additional Tests
    // =============================================================================

    #[test]
    fn test_ring_buffer_capacity_power_of_two() {
        // Test various capacities are rounded to power of 2
        assert_eq!(FrameRingBuffer::new(1).capacity(), 1);
        assert_eq!(FrameRingBuffer::new(3).capacity(), 4);
        assert_eq!(FrameRingBuffer::new(7).capacity(), 8);
        assert_eq!(FrameRingBuffer::new(100).capacity(), 128);
        assert_eq!(FrameRingBuffer::new(200).capacity(), 256);
    }

    #[test]
    fn test_ring_buffer_error_display() {
        assert_eq!(format!("{}", RingBufferError::Full), "Ring buffer is full");
        assert_eq!(
            format!("{}", RingBufferError::Empty),
            "Ring buffer is empty"
        );
        assert_eq!(
            format!("{}", RingBufferError::Closed),
            "Ring buffer is closed"
        );
        assert_eq!(
            format!("{}", RingBufferError::Timeout),
            "Ring buffer operation timed out"
        );
    }

    #[test]
    fn test_ring_buffer_error_equality() {
        assert_eq!(RingBufferError::Full, RingBufferError::Full);
        assert_eq!(RingBufferError::Empty, RingBufferError::Empty);
        assert_ne!(RingBufferError::Full, RingBufferError::Empty);
        assert_ne!(RingBufferError::Closed, RingBufferError::Timeout);
    }

    #[test]
    fn test_ring_buffer_error_clone() {
        let err = RingBufferError::Full;
        let cloned = err.clone();
        assert_eq!(err, cloned);
    }

    #[test]
    fn test_ring_buffer_multi_push_pop() {
        let buffer = FrameRingBuffer::new(16);
        let frame = VideoFrame::new(100, 100, vec![0; 100 * 100 * 3]);

        // Push multiple frames
        for i in 0..8 {
            let mut f = frame.clone();
            f.width = 100 + i as u32;
            buffer.try_push(f).unwrap();
        }

        assert_eq!(buffer.len(), 8);

        // Pop frames and verify order (FIFO)
        for i in 0..8 {
            let popped = buffer.try_pop().unwrap();
            assert_eq!(popped.width, 100 + i as u32);
        }

        assert!(buffer.is_empty());
    }

    #[test]
    fn test_ring_buffer_snapshot_fill_ratio() {
        let buffer = FrameRingBuffer::new(16);
        let frame = VideoFrame::new(100, 100, vec![0; 100 * 100 * 3]);

        // Empty
        assert_eq!(buffer.snapshot().fill_ratio(), 0.0);

        // Half full
        for _ in 0..8 {
            buffer.try_push(frame.clone()).unwrap();
        }
        assert_eq!(buffer.snapshot().fill_ratio(), 0.5);

        // Full
        for _ in 0..8 {
            buffer.try_push(frame.clone()).unwrap();
        }
        assert_eq!(buffer.snapshot().fill_ratio(), 1.0);
    }

    #[test]
    fn test_ring_buffer_snapshot_debug() {
        let snapshot = RingBufferSnapshot {
            capacity: 16,
            len: 4,
            is_empty: false,
            is_full: false,
            is_closed: false,
        };
        let debug_str = format!("{:?}", snapshot);
        assert!(debug_str.contains("capacity"));
        assert!(debug_str.contains("16"));
    }

    #[test]
    fn test_ring_buffer_partial_fill() {
        let buffer = FrameRingBuffer::new(8);
        let frame = VideoFrame::new(100, 100, vec![0; 100 * 100 * 3]);

        // Push 3 frames
        for _ in 0..3 {
            buffer.try_push(frame.clone()).unwrap();
        }

        assert!(!buffer.is_empty());
        assert!(!buffer.is_full());
        assert_eq!(buffer.len(), 3);
    }

    #[test]
    fn test_ring_buffer_interleaved_push_pop() {
        let buffer = FrameRingBuffer::new(8);
        let frame = VideoFrame::new(100, 100, vec![0; 100 * 100 * 3]);

        // Interleave push and pop operations
        buffer.try_push(frame.clone()).unwrap();
        assert_eq!(buffer.len(), 1);

        buffer.try_push(frame.clone()).unwrap();
        buffer.try_pop().unwrap();
        assert_eq!(buffer.len(), 1);

        buffer.try_push(frame.clone()).unwrap();
        buffer.try_push(frame.clone()).unwrap();
        buffer.try_pop().unwrap();
        assert_eq!(buffer.len(), 2);

        // Drain
        while buffer.try_pop().is_some() {}
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_ring_buffer_large_capacity() {
        let buffer = FrameRingBuffer::new(1024);
        let frame = VideoFrame::new(100, 100, vec![0; 100 * 100 * 3]);

        // Verify capacity is power of 2
        assert_eq!(buffer.capacity(), 1024);

        // Fill half
        for _ in 0..512 {
            buffer.try_push(frame.clone()).unwrap();
        }
        assert_eq!(buffer.len(), 512);
        assert!(!buffer.is_full());
    }

    #[test]
    fn test_ring_buffer_frame_data_integrity() {
        let buffer = FrameRingBuffer::new(4);
        let original_data = vec![42u8; 100 * 100 * 3];
        let frame = VideoFrame::new(100, 100, original_data.clone());

        buffer.try_push(frame).unwrap();
        let popped = buffer.try_pop().unwrap();

        // Verify data integrity
        assert_eq!(popped.data().len(), original_data.len());
        assert!(popped.data().iter().all(|&b| b == 42));
    }

    #[test]
    #[should_panic(expected = "Ring buffer capacity must be > 0")]
    fn test_ring_buffer_zero_capacity_panics() {
        let _ = FrameRingBuffer::new(0);
    }

    #[test]
    fn test_ring_buffer_close_propagates_to_clone() {
        let buffer = FrameRingBuffer::new(8);
        let buffer_clone = buffer.clone();

        // Close original
        buffer.close();

        // Clone should also see closed state
        assert!(buffer_clone.is_closed());
    }

    #[test]
    fn test_ring_buffer_snapshot_after_close() {
        let buffer = FrameRingBuffer::new(8);
        let frame = VideoFrame::new(100, 100, vec![0; 100 * 100 * 3]);

        buffer.try_push(frame).unwrap();
        buffer.close();

        let snapshot = buffer.snapshot();
        assert!(snapshot.is_closed);
        assert_eq!(snapshot.len, 1);
    }
}
