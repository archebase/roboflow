// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Lock-free buffer pool for zero-allocation compression.
//!
//! This module provides a lock-free buffer pool using crossbeam::queue::ArrayQueue
//! that reuses buffers across compression operations, eliminating per-chunk allocations
//! and the 10% deallocation overhead from dropping Vec<u8>.

use crossbeam_queue::ArrayQueue;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Default buffer capacity (4MB)
const DEFAULT_BUFFER_CAPACITY: usize = 4 * 1024 * 1024;

/// Maximum number of buffers to keep in the pool per worker
const MAX_POOL_SIZE: usize = 4;

/// A pooled buffer that returns itself to the pool when dropped.
///
/// This is a zero-cost wrapper - the Drop implementation handles
/// returning the buffer to the pool without any runtime overhead
/// during normal use.
pub struct PooledBuffer {
    /// The buffer data
    data: Vec<u8>,
    /// Reference to the pool to return to
    pool: Arc<BufferPoolInner>,
}

impl PooledBuffer {
    /// Get a mutable reference to the buffer data.
    #[inline]
    #[allow(clippy::should_implement_trait)]
    pub fn as_mut(&mut self) -> &mut Vec<u8> {
        &mut self.data
    }

    /// Get a reference to the buffer data.
    #[inline]
    #[allow(clippy::should_implement_trait)]
    pub fn as_ref(&self) -> &[u8] {
        &self.data
    }

    /// Get the capacity of the buffer.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    /// Get the length of the buffer.
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Clear the buffer (zero-cost - just sets length to 0).
    #[inline]
    pub fn clear(&mut self) {
        self.data.clear();
    }

    /// Reserve additional capacity if needed.
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.data.reserve(additional);
    }

    /// Convert into the inner Vec, preventing return to pool.
    ///
    /// Use this when you need to transfer ownership of the buffer
    /// without returning it to the pool.
    #[inline]
    pub fn into_inner(self) -> Vec<u8> {
        // Prevent returning to pool since we're taking ownership
        let this = std::mem::ManuallyDrop::new(self);
        unsafe { std::ptr::read(&this.data) }
    }
}

impl Drop for PooledBuffer {
    #[inline]
    fn drop(&mut self) {
        // Return buffer to pool - zero-cost clear and return
        let data = std::mem::take(&mut self.data);
        self.pool.return_buffer(data);
    }
}

impl AsRef<[u8]> for PooledBuffer {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl AsMut<[u8]> for PooledBuffer {
    #[inline]
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

impl AsMut<Vec<u8>> for PooledBuffer {
    #[inline]
    fn as_mut(&mut self) -> &mut Vec<u8> {
        &mut self.data
    }
}

impl std::fmt::Debug for PooledBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledBuffer")
            .field("len", &self.data.len())
            .field("capacity", &self.data.capacity())
            .finish()
    }
}

/// Inner buffer pool state (shared via Arc).
#[derive(Debug)]
struct BufferPoolInner {
    /// Lock-free queue of available buffers
    queue: ArrayQueue<Vec<u8>>,
    /// Default buffer capacity for new allocations
    default_capacity: usize,
    /// Total number of buffer allocations (for metrics)
    total_allocations: AtomicUsize,
    /// Current pool size (for metrics)
    pool_size: AtomicUsize,
}

impl BufferPoolInner {
    /// Return a buffer to the pool.
    ///
    /// This is zero-cost when the pool is full - the buffer is simply dropped.
    #[inline]
    fn return_buffer(&self, mut buffer: Vec<u8>) {
        buffer.clear(); // Zero-cost: just sets len to 0, keeps capacity

        // Try to return to pool - if full, buffer is dropped (dealloc happens here)
        if self.queue.push(buffer).is_err() {
            // Pool full, let buffer drop (will deallocate)
            // This is fine - it means we have enough buffers in circulation
        } else {
            self.pool_size.fetch_add(1, Ordering::Release);
        }
    }

    /// Take a buffer from the pool, or allocate a new one.
    #[inline]
    fn take_buffer(&self, min_capacity: usize) -> Vec<u8> {
        // Try to get a buffer from the pool (lock-free)
        if let Some(buffer) = self.queue.pop() {
            self.pool_size.fetch_sub(1, Ordering::Acquire);
            let mut buf: Vec<u8> = buffer;

            // Check if buffer is large enough
            if buf.capacity() >= min_capacity {
                buf.clear(); // Zero-cost reset
                return buf;
            }

            // Buffer too small, reserve more space
            buf.reserve(min_capacity.saturating_sub(buf.capacity()));
            return buf;
        }

        // No available buffer, allocate new one (slow path)
        self.total_allocations.fetch_add(1, Ordering::Release);
        Vec::with_capacity(min_capacity.max(self.default_capacity))
    }

    /// Get the current pool size.
    #[inline]
    fn pool_size(&self) -> usize {
        self.pool_size.load(Ordering::Acquire)
    }

    /// Get total allocations.
    #[inline]
    fn total_allocations(&self) -> usize {
        self.total_allocations.load(Ordering::Acquire)
    }
}

/// Lock-free buffer pool for compression buffers.
///
/// Uses crossbeam::queue::ArrayQueue for zero-contention buffer reuse.
/// Each thread can acquire and return buffers without blocking.
///
/// # Example
///
/// ```no_run
/// use roboflow::pipeline::types::buffer_pool::BufferPool;
///
/// # fn main() {
/// let pool = BufferPool::with_capacity(4 * 1024 * 1024);
///
/// // In compression worker:
/// let mut output = pool.acquire(1024);
/// // use output.as_mut() to access the Vec<u8>
/// output.as_mut().extend_from_slice(&[0u8; 100]);
/// // output automatically returned to pool when dropped
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct BufferPool {
    inner: Arc<BufferPoolInner>,
}

impl BufferPool {
    /// Create a new buffer pool with the specified default buffer capacity.
    ///
    /// # Parameters
    ///
    /// - `default_capacity`: Default capacity for newly allocated buffers
    ///
    /// The pool will hold up to `MAX_POOL_SIZE` buffers per shared pool instance.
    pub fn with_capacity(default_capacity: usize) -> Self {
        Self {
            inner: Arc::new(BufferPoolInner {
                queue: ArrayQueue::new(MAX_POOL_SIZE),
                default_capacity,
                total_allocations: AtomicUsize::new(0),
                pool_size: AtomicUsize::new(0),
            }),
        }
    }

    /// Create a buffer pool with 4MB default capacity.
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_BUFFER_CAPACITY)
    }

    /// Get a buffer with at least the specified capacity.
    ///
    /// The buffer is automatically returned to the pool when dropped.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use roboflow::pipeline::types::buffer_pool::BufferPool;
    ///
    /// # fn main() {
    /// let pool = BufferPool::new();
    /// let mut buf = pool.acquire(1024);
    /// // Use as_mut() to access the inner Vec<u8>
    /// buf.as_mut().extend_from_slice(&[0u8; 100]);
    /// // buf returned to pool when it goes out of scope
    /// # }
    /// ```
    #[inline]
    pub fn acquire(&self, min_capacity: usize) -> PooledBuffer {
        let data = self.inner.take_buffer(min_capacity);
        PooledBuffer {
            data,
            pool: Arc::clone(&self.inner),
        }
    }

    /// Get a buffer with default capacity.
    #[inline]
    pub fn acquire_default(&self) -> PooledBuffer {
        self.acquire(0)
    }

    /// Get the current number of buffers in the pool.
    #[inline]
    pub fn pool_size(&self) -> usize {
        self.inner.pool_size()
    }

    /// Get the total number of buffer allocations (excluding pool reuses).
    #[inline]
    pub fn total_allocations(&self) -> usize {
        self.inner.total_allocations()
    }

    /// Pre-warm the pool with buffers.
    ///
    /// Useful for eliminating initial allocation overhead.
    pub fn warmup(&self, count: usize) {
        for _ in 0..count.min(MAX_POOL_SIZE) {
            let buffer = Vec::with_capacity(self.inner.default_capacity);
            if self.inner.queue.push(buffer).is_ok() {
                self.inner.pool_size.fetch_add(1, Ordering::Release);
            }
        }
    }

    /// Get the default buffer capacity.
    #[inline]
    pub fn default_capacity(&self) -> usize {
        self.inner.default_capacity
    }

    /// Directly return a buffer to the pool without going through PooledBuffer.
    ///
    /// This is useful when you have a Vec<u8> that you want to return to the pool
    /// without creating a PooledBuffer wrapper. The buffer will be cleared before
    /// being returned to the pool.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # fn main() {
    /// use roboflow::pipeline::types::buffer_pool::BufferPool;
    ///
    /// let buffer_pool = BufferPool::new();
    /// let mut data = vec![1, 2, 3];
    /// buffer_pool.return_buffer(data);  // data is returned to pool
    /// # }
    /// ```
    #[inline]
    pub fn return_buffer(&self, mut buffer: Vec<u8>) {
        buffer.clear();
        if self.inner.queue.push(buffer).is_ok() {
            self.inner.pool_size.fetch_add(1, Ordering::Release);
        }
        // If pool is full, buffer is dropped (deallocated)
    }
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper trait for types that can use a buffer pool.
pub trait WithBufferPool {
    /// Set the buffer pool for this type.
    fn with_buffer_pool(self, pool: BufferPool) -> Self
    where
        Self: Sized;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool_acquire() {
        let pool = BufferPool::with_capacity(1024);
        let buffer = pool.acquire(512);
        assert!(buffer.capacity() >= 512);
    }

    #[test]
    fn test_buffer_pool_reuse() {
        let pool = BufferPool::with_capacity(1024);

        // First buffer
        let capacity = {
            let buffer = pool.acquire(1024);
            buffer.capacity()
        };

        // Buffer should be returned to pool
        assert_eq!(pool.pool_size(), 1);

        // Second buffer should reuse the first one
        let buffer = pool.acquire(512);
        assert_eq!(buffer.capacity(), capacity);
        assert_eq!(pool.total_allocations(), 1); // Only one allocation
    }

    #[test]
    fn test_buffer_pool_warmup() {
        let pool = BufferPool::with_capacity(4096);
        pool.warmup(3);

        assert_eq!(pool.pool_size(), 3);

        // Should use pre-allocated buffers
        for _ in 0..3 {
            let _buffer = pool.acquire(1024);
        }

        assert_eq!(pool.total_allocations(), 0); // No new allocations
    }

    #[test]
    fn test_pooled_buffer_clear() {
        let pool = BufferPool::with_capacity(100);
        let mut buffer = pool.acquire(100);

        buffer.as_mut().extend_from_slice(&[1, 2, 3, 4, 5]);
        assert_eq!(buffer.len(), 5);

        buffer.clear();
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.capacity(), 100); // Capacity preserved
    }

    #[test]
    fn test_pooled_buffer_into_inner() {
        let pool = BufferPool::with_capacity(100);
        let buffer = pool.acquire(100);

        let vec = buffer.into_inner();
        assert!(vec.capacity() >= 100);
        // Buffer not returned to pool
        assert_eq!(pool.pool_size(), 0);
    }

    #[test]
    fn test_buffer_pool_clone() {
        let pool1 = BufferPool::with_capacity(1024);
        let pool2 = pool1.clone();

        {
            let _buffer = pool1.acquire(100);
        }

        // Both pools share the same inner state
        assert_eq!(pool2.pool_size(), 1);
    }

    #[test]
    fn test_buffer_pool_max_size() {
        let pool = BufferPool::with_capacity(1024);

        // Return more buffers than MAX_POOL_SIZE
        for _ in 0..MAX_POOL_SIZE + 2 {
            let _buffer = pool.acquire(100);
        }

        // Pool should be at most MAX_POOL_SIZE
        assert!(pool.pool_size() <= MAX_POOL_SIZE);
    }

    #[test]
    fn test_buffer_pool_concurrent() {
        use std::thread;
        let pool = Arc::new(BufferPool::with_capacity(4096));
        pool.warmup(4);

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let pool = Arc::clone(&pool);
                thread::spawn(move || {
                    for _ in 0..100 {
                        let mut buf = pool.acquire(1024);
                        buf.as_mut().push(42);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Should have done mostly pool reuses
        // 4 threads * 100 iterations = 400 acquires
        // With 4 pre-warmed buffers, most should be reuses
        println!(
            "Total allocations: {}, Pool size: {}",
            pool.total_allocations(),
            pool.pool_size()
        );
        assert!(pool.total_allocations() < 400); // Many were reuses
    }
}
