// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Frame arena allocator for zero-copy video processing.

use std::alloc::{Layout, alloc, dealloc};
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Configuration for frame pool.
#[derive(Debug, Clone, Copy)]
pub struct FramePoolConfig {
    pub width: u32,
    pub height: u32,
    pub bytes_per_pixel: u32,
    pub slot_count: u32,
}

impl Default for FramePoolConfig {
    fn default() -> Self {
        Self {
            width: 640,
            height: 480,
            bytes_per_pixel: 3,
            slot_count: 64, // Increased from 16 for high-throughput pipelines
        }
    }
}

impl FramePoolConfig {
    #[inline]
    pub fn slot_size(&self) -> usize {
        (self.width as usize) * (self.height as usize) * (self.bytes_per_pixel as usize)
    }
}

/// Errors from frame pool operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum FramePoolError {
    #[error("Invalid pool configuration: {0}")]
    InvalidConfig(String),
    #[error("Memory allocation failed: {0}")]
    AllocationFailed(String),
    #[error("Frame pool exhausted")]
    PoolExhausted,
}

/// Pre-allocated memory pool for video frames.
pub struct FramePool {
    base_ptr: *mut u8,
    layout: Layout,
    slot_size: usize,
    slot_count: u32,
    width: u32,
    height: u32,
    free_mask: AtomicU64,
    in_use: Vec<AtomicBool>,
    alloc_count: AtomicU64,
}

// SAFETY: FramePool uses atomic operations (AtomicU64, AtomicBool) for all
// concurrent access. The raw pointer (base_ptr) is managed internally with
// proper synchronization via atomic CAS on free_mask.
unsafe impl Send for FramePool {}
unsafe impl Sync for FramePool {}

impl FramePool {
    pub fn new(config: FramePoolConfig) -> Result<Self, FramePoolError> {
        if config.width == 0 || config.height == 0 {
            return Err(FramePoolError::InvalidConfig(
                "Width and height must be non-zero".into(),
            ));
        }
        if config.slot_count == 0 {
            return Err(FramePoolError::InvalidConfig(
                "Slot count must be non-zero".into(),
            ));
        }
        if config.bytes_per_pixel == 0 {
            return Err(FramePoolError::InvalidConfig(
                "Bytes per pixel must be non-zero".into(),
            ));
        }
        if config.slot_count > 64 {
            return Err(FramePoolError::InvalidConfig(
                "Slot count must be <= 64".into(),
            ));
        }

        let slot_size = config.slot_size();
        let total_size = slot_size
            .checked_mul(config.slot_count as usize)
            .ok_or_else(|| FramePoolError::InvalidConfig("Total pool size overflow".into()))?;

        let layout = Layout::from_size_align(total_size, 64)
            .map_err(|e| FramePoolError::InvalidConfig(e.to_string()))?;

        let base_ptr = if total_size == 0 {
            std::ptr::null_mut()
        } else {
            // SAFETY: Layout is valid (validated above). We allocate exactly `total_size` bytes
            // with 64-byte alignment for SIMD operations. The pointer is stored in base_ptr
            // and will be deallocated in Drop using the same layout.
            let ptr = unsafe { alloc(layout) };
            if ptr.is_null() {
                return Err(FramePoolError::AllocationFailed(
                    "Failed to allocate memory".into(),
                ));
            }
            ptr
        };

        let in_use: Vec<AtomicBool> = (0..config.slot_count)
            .map(|_| AtomicBool::new(false))
            .collect();
        // Handle edge case: 1u64 << 64 would overflow
        let free_mask = if config.slot_count == 64 {
            u64::MAX
        } else {
            (1u64 << config.slot_count) - 1
        };

        Ok(Self {
            base_ptr,
            layout,
            slot_size,
            slot_count: config.slot_count,
            width: config.width,
            height: config.height,
            free_mask: AtomicU64::new(free_mask),
            in_use,
            alloc_count: AtomicU64::new(0),
        })
    }

    /// Try to acquire a free slot using atomic CAS loop.
    /// Returns the slot index if successful, None if pool is exhausted.
    fn try_acquire_slot(&self) -> Option<u32> {
        for attempt in 0..self.slot_count {
            let mask = self.free_mask.load(Ordering::Acquire);
            let free_bit = mask.trailing_zeros();
            if free_bit >= self.slot_count {
                return None;
            }

            let slot_index = free_bit;
            let bit_mask = 1u64 << slot_index;
            let new_mask = mask & !bit_mask;

            if self
                .free_mask
                .compare_exchange_weak(mask, new_mask, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                self.in_use[slot_index as usize].store(true, Ordering::Release);
                self.alloc_count.fetch_add(1, Ordering::Relaxed);
                return Some(slot_index);
            }
            if attempt > 0 && attempt % 4 == 0 {
                std::hint::spin_loop();
            }
        }
        None
    }

    pub fn acquire(&self) -> Option<OwnedSlot<'_>> {
        let slot_index = self.try_acquire_slot()?;
        Some(OwnedSlot {
            // SAFETY: slot_index is guaranteed to be < slot_count by try_acquire_slot.
            // base_ptr is valid and aligned, and the offset calculation is within bounds.
            data_ptr: unsafe {
                NonNull::new_unchecked(self.base_ptr.add((slot_index as usize) * self.slot_size))
            },
            data_size: self.slot_size,
            index: slot_index,
            pool: self,
            width: self.width,
            height: self.height,
        })
    }

    fn release(&self, index: u32) {
        // Runtime check for safety in release builds
        if index >= self.slot_count {
            // Invalid index - this should never happen with correct RAII usage
            // Log warning in debug builds, silently return in release
            debug_assert!(false, "Invalid slot index {} >= {}", index, self.slot_count);
            return;
        }
        self.in_use[index as usize].store(false, Ordering::Release);
        self.free_mask.fetch_or(1u64 << index, Ordering::AcqRel);
    }

    #[inline]
    pub fn slot_count(&self) -> u32 {
        self.slot_count
    }
    pub fn free_count(&self) -> u32 {
        self.free_mask.load(Ordering::Acquire).count_ones()
    }
    #[inline]
    pub fn used_count(&self) -> u32 {
        self.slot_count - self.free_count()
    }
    #[inline]
    pub fn width(&self) -> u32 {
        self.width
    }
    #[inline]
    pub fn height(&self) -> u32 {
        self.height
    }
    #[inline]
    pub fn slot_size(&self) -> usize {
        self.slot_size
    }
    #[inline]
    pub fn alloc_count(&self) -> u64 {
        self.alloc_count.load(Ordering::Relaxed)
    }
}

impl Drop for FramePool {
    fn drop(&mut self) {
        if !self.base_ptr.is_null() && self.layout.size() > 0 {
            // SAFETY: base_ptr was allocated with the same layout in new(),
            // and is still valid. All slots must be released before drop
            // (enforced by RAII via OwnedSlot).
            unsafe {
                dealloc(self.base_ptr, self.layout);
            }
        }
    }
}

impl std::fmt::Debug for FramePool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FramePool")
            .field("width", &self.width)
            .field("height", &self.height)
            .field("slot_count", &self.slot_count)
            .field("slot_size", &self.slot_size)
            .field("free_count", &self.free_count())
            .field("used_count", &self.used_count())
            .finish()
    }
}

/// An owned slot from the frame pool.
pub struct OwnedSlot<'a> {
    data_ptr: NonNull<u8>,
    data_size: usize,
    index: u32,
    pool: &'a FramePool,
    width: u32,
    height: u32,
}

// SAFETY: OwnedSlot points to memory in FramePool which is Send + Sync.
// The slot is exclusively owned, so no data races are possible.
unsafe impl Send for OwnedSlot<'_> {}

impl OwnedSlot<'_> {
    #[inline]
    pub fn width(&self) -> u32 {
        self.width
    }
    #[inline]
    pub fn height(&self) -> u32 {
        self.height
    }
    #[inline]
    pub fn data(&self) -> &[u8] {
        // SAFETY: data_ptr is valid for data_size bytes, acquired from FramePool
        // which guarantees proper alignment and size.
        unsafe { std::slice::from_raw_parts(self.data_ptr.as_ptr(), self.data_size) }
    }
    #[inline]
    pub fn data_mut(&mut self) -> &mut [u8] {
        // SAFETY: data_ptr is valid for data_size bytes, acquired from FramePool.
        // We have exclusive access via &mut self, so aliasing is not possible.
        unsafe { std::slice::from_raw_parts_mut(self.data_ptr.as_ptr(), self.data_size) }
    }
    #[inline]
    pub fn index(&self) -> u32 {
        self.index
    }
    #[inline]
    pub fn expected_size(&self) -> usize {
        self.data_size
    }
}

impl Drop for OwnedSlot<'_> {
    fn drop(&mut self) {
        self.pool.release(self.index);
    }
}

impl std::fmt::Debug for OwnedSlot<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedSlot")
            .field("index", &self.index)
            .field("width", &self.width)
            .field("height", &self.height)
            .field("data_size", &self.data_size)
            .finish()
    }
}

struct SlotGuard {
    index: u32,
    pool: Arc<AtomicFramePool>,
}

impl SlotGuard {
    fn new(index: u32, pool: Arc<AtomicFramePool>) -> Self {
        Self { index, pool }
    }
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        self.pool.release(self.index);
    }
}

/// Thread-safe frame pool for use with Arc.
pub struct AtomicFramePool {
    inner: FramePool,
}

// SAFETY: AtomicFramePool wraps FramePool and uses atomic operations for slot
// acquisition/release. All mutable operations use atomic compare_exchange,
// making it safe for concurrent access from multiple threads.
unsafe impl Send for AtomicFramePool {}
unsafe impl Sync for AtomicFramePool {}

impl AtomicFramePool {
    pub fn new(config: FramePoolConfig) -> Result<Self, FramePoolError> {
        Ok(Self {
            inner: FramePool::new(config)?,
        })
    }

    pub fn acquire(self: &Arc<Self>) -> Option<ArcSlot> {
        let slot_index = self.inner.try_acquire_slot()?;
        Some(ArcSlot {
            // SAFETY: slot_index is guaranteed to be < slot_count by try_acquire_slot.
            // base_ptr is valid and aligned, and the offset calculation is within bounds.
            data_ptr: unsafe {
                NonNull::new_unchecked(
                    self.inner
                        .base_ptr
                        .add((slot_index as usize) * self.inner.slot_size),
                )
            },
            data_size: self.inner.slot_size,
            index: slot_index,
            width: self.inner.width,
            height: self.inner.height,
            _guard: SlotGuard::new(slot_index, Arc::clone(self)),
        })
    }

    fn release(&self, index: u32) {
        self.inner.release(index);
    }

    pub fn stats(&self) -> PoolStats {
        PoolStats {
            slot_count: self.inner.slot_count,
            free_count: self.inner.free_count(),
            used_count: self.inner.used_count(),
            alloc_count: self.inner.alloc_count.load(Ordering::Relaxed),
            slot_size: self.inner.slot_size,
            width: self.inner.width,
            height: self.inner.height,
        }
    }
    #[inline]
    pub fn slot_count(&self) -> u32 {
        self.inner.slot_count
    }
    #[inline]
    pub fn free_count(&self) -> u32 {
        self.inner.free_count()
    }
    #[inline]
    pub fn used_count(&self) -> u32 {
        self.inner.used_count()
    }
}

impl std::fmt::Debug for AtomicFramePool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

/// An owned slot from AtomicFramePool with static lifetime.
pub struct ArcSlot {
    data_ptr: NonNull<u8>,
    data_size: usize,
    index: u32,
    width: u32,
    height: u32,
    _guard: SlotGuard,
}

// SAFETY: ArcSlot points to memory in AtomicFramePool which is Send + Sync.
// The slot is exclusively owned via SlotGuard, so no data races are possible.
unsafe impl Send for ArcSlot {}

impl ArcSlot {
    #[inline]
    pub fn width(&self) -> u32 {
        self.width
    }
    #[inline]
    pub fn height(&self) -> u32 {
        self.height
    }
    #[inline]
    pub fn data(&self) -> &[u8] {
        // SAFETY: data_ptr is valid for data_size bytes, acquired from AtomicFramePool
        // which guarantees proper alignment and size.
        unsafe { std::slice::from_raw_parts(self.data_ptr.as_ptr(), self.data_size) }
    }
    #[inline]
    pub fn data_mut(&mut self) -> &mut [u8] {
        // SAFETY: data_ptr is valid for data_size bytes, acquired from AtomicFramePool.
        // We have exclusive access via &mut self, so aliasing is not possible.
        unsafe { std::slice::from_raw_parts_mut(self.data_ptr.as_ptr(), self.data_size) }
    }
    #[inline]
    pub fn index(&self) -> u32 {
        self.index
    }
    #[inline]
    pub fn expected_size(&self) -> usize {
        self.data_size
    }
}

impl std::fmt::Debug for ArcSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArcSlot")
            .field("index", &self.index)
            .field("width", &self.width)
            .field("height", &self.height)
            .field("data_size", &self.data_size)
            .finish()
    }
}

/// Statistics about a frame pool.
#[derive(Debug, Clone, Copy)]
pub struct PoolStats {
    pub slot_count: u32,
    pub free_count: u32,
    pub used_count: u32,
    pub alloc_count: u64,
    pub slot_size: usize,
    pub width: u32,
    pub height: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_create() {
        let pool = FramePool::new(FramePoolConfig {
            width: 64,
            height: 64,
            bytes_per_pixel: 3,
            slot_count: 4,
        })
        .unwrap();
        assert_eq!(pool.slot_count(), 4);
        assert_eq!(pool.free_count(), 4);
    }

    #[test]
    fn test_pool_acquire_release() {
        let pool = FramePool::new(FramePoolConfig {
            width: 64,
            height: 64,
            bytes_per_pixel: 3,
            slot_count: 4,
        })
        .unwrap();
        let slot = pool.acquire().expect("acquire");
        assert_eq!(pool.free_count(), 3);
        drop(slot);
        assert_eq!(pool.free_count(), 4);
    }

    #[test]
    fn test_pool_exhaustion() {
        let pool = FramePool::new(FramePoolConfig {
            width: 64,
            height: 64,
            bytes_per_pixel: 3,
            slot_count: 2,
        })
        .unwrap();
        let _s1 = pool.acquire().unwrap();
        let _s2 = pool.acquire().unwrap();
        assert!(pool.acquire().is_none());
    }

    #[test]
    fn test_atomic_pool() {
        let pool = Arc::new(
            AtomicFramePool::new(FramePoolConfig {
                width: 64,
                height: 64,
                bytes_per_pixel: 3,
                slot_count: 4,
            })
            .unwrap(),
        );
        let slot = pool.acquire().expect("acquire");
        assert_eq!(pool.free_count(), 3);
        drop(slot);
        assert_eq!(pool.free_count(), 4);
    }

    #[test]
    fn test_concurrent_acquire() {
        let pool = Arc::new(
            AtomicFramePool::new(FramePoolConfig {
                width: 64,
                height: 64,
                bytes_per_pixel: 3,
                slot_count: 8,
            })
            .unwrap(),
        );
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let p = Arc::clone(&pool);
                std::thread::spawn(move || {
                    let mut v = vec![];
                    for _ in 0..3 {
                        if let Some(s) = p.acquire() {
                            v.push(s);
                        }
                    }
                    v
                })
            })
            .collect();
        // Join all threads first, collecting results (slots held in results Vec)
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        // Now count - all slots still held, none released yet
        let total: usize = results.iter().map(|v| v.len()).sum();
        assert_eq!(total, 8);
    }
}
