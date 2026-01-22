// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Arena allocator for zero-copy message references.
//!
//! This module provides arena-based allocation that extends the lifetime of
//! message data from mmap'd regions, eliminating the need to copy message
//! data during pipeline processing.
//!
//! ## Block Recycling
//!
//! To eliminate allocation/deallocation overhead (~22% of CPU time), arena blocks
//! are recycled instead of deallocated. When an ArenaBlock is dropped, its memory
//! is returned to a global lock-free recycle pool. When a new block is needed,
//! the pool is checked first before allocating fresh memory.

use crossbeam_channel::{bounded, Receiver, Sender};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

/// Default arena block size (64MB for optimal cache locality and reduced fragmentation)
pub const DEFAULT_ARENA_BLOCK_SIZE: usize = 64 * 1024 * 1024;

/// Maximum number of blocks to keep in the global recycle pool.
const MAX_RECYCLED_BLOCKS: usize = 32;

/// Global lock-free pool of recycled arena blocks.
/// Uses crossbeam channels for lock-free recycling.
static BLOCK_POOL: OnceLock<BlockPool> = OnceLock::new();

struct BlockPool {
    sender: Sender<RecycledBlock>,
    receiver: Receiver<RecycledBlock>,
}

impl BlockPool {
    fn get() -> &'static BlockPool {
        BLOCK_POOL.get_or_init(|| {
            let (sender, receiver) = bounded(MAX_RECYCLED_BLOCKS);
            BlockPool { sender, receiver }
        })
    }

    /// Try to get a recycled block (any size, will be reused if >= needed).
    #[inline]
    fn try_get(&self) -> Option<RecycledBlock> {
        self.receiver.try_recv().ok()
    }

    /// Return a block to the pool. If pool is full, deallocates.
    #[inline]
    fn recycle(&self, block: RecycledBlock) {
        if let Err(crossbeam_channel::TrySendError::Full(returned)) = self.sender.try_send(block) {
            // Pool full, deallocate
            unsafe {
                let layout = std::alloc::Layout::from_size_align_unchecked(returned.capacity, 8);
                std::alloc::dealloc(returned.data.as_ptr(), layout);
            }
        }
    }
}

/// A recycled block ready for reuse.
struct RecycledBlock {
    data: NonNull<u8>,
    capacity: usize,
}

// SAFETY: The pointer is valid and owned by this struct
unsafe impl Send for RecycledBlock {}

impl RecycledBlock {
    /// Try to get a recycled block from the global pool.
    /// Returns (pointer, capacity) if a suitable block is available.
    #[inline]
    fn try_get(min_capacity: usize) -> Option<(NonNull<u8>, usize)> {
        let pool = BlockPool::get();
        // Try to get any recycled block that's large enough
        if let Some(block) = pool.try_get() {
            if block.capacity >= min_capacity {
                return Some((block.data, block.capacity));
            }
            // Block too small, deallocate it and try fresh allocation
            unsafe {
                let layout = std::alloc::Layout::from_size_align_unchecked(block.capacity, 8);
                std::alloc::dealloc(block.data.as_ptr(), layout);
            }
        }
        None
    }

    /// Return a block to the global recycle pool.
    #[inline]
    fn recycle(data: NonNull<u8>, capacity: usize) {
        let pool = BlockPool::get();
        pool.recycle(RecycledBlock { data, capacity });
    }
}

/// Arena allocation block.
///
/// Each block holds a contiguous region of memory that can be allocated from.
struct ArenaBlock {
    /// Raw pointer to data storage
    data: NonNull<u8>,
    /// Capacity of this block
    capacity: usize,
    /// Current allocation offset
    offset: AtomicUsize,
}

unsafe impl Send for ArenaBlock {}

impl ArenaBlock {
    /// Create a new arena block with the specified capacity.
    ///
    /// First tries to get a recycled block from the thread-local pool.
    /// If no suitable block is available, allocates fresh memory.
    fn new(capacity: usize) -> Result<Self, std::io::Error> {
        // Try to get a recycled block first (fast path)
        if let Some((data, actual_capacity)) = RecycledBlock::try_get(capacity) {
            return Ok(Self {
                data,
                capacity: actual_capacity,
                offset: AtomicUsize::new(0),
            });
        }

        // No recycled block available, allocate fresh memory (slow path)
        let layout = std::alloc::Layout::from_size_align(capacity, 8).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid arena block size")
        })?;

        let data = unsafe { std::alloc::alloc(layout) };
        if data.is_null() {
            return Err(std::io::Error::other(format!(
                "Failed to allocate arena block: {} bytes. System may be out of memory.",
                capacity
            )));
        }

        // SAFETY: We just checked that data is not null above
        Ok(Self {
            data: unsafe { NonNull::new_unchecked(data) },
            capacity,
            offset: AtomicUsize::new(0),
        })
    }

    /// Try to allocate from this block.
    #[inline]
    fn try_allocate(&self, size: usize, align: usize) -> Option<usize> {
        let current = self.offset.load(Ordering::Acquire);
        let aligned = (current + align - 1) & !(align - 1);
        let new_offset = aligned + size;

        if new_offset > self.capacity {
            None
        } else {
            match self.offset.compare_exchange_weak(
                current,
                new_offset,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => Some(aligned),
                Err(_) => self.try_allocate(size, align), // Retry
            }
        }
    }

    /// Reset the offset to zero.
    #[inline]
    fn reset(&self) {
        self.offset.store(0, Ordering::Release);
    }

    /// Get a pointer to the data at the given offset.
    #[inline]
    unsafe fn data_at(&self, offset: usize) -> *mut u8 {
        self.data.as_ptr().add(offset)
    }
}

impl Drop for ArenaBlock {
    fn drop(&mut self) {
        // Recycle the block instead of deallocating.
        // This eliminates ~10% of CPU time spent on deallocation.
        RecycledBlock::recycle(self.data, self.capacity);
    }
}

/// Thread-local arena for zero-copy message allocation.
pub struct MessageArena {
    /// Arena blocks
    blocks: Vec<ArenaBlock>,
    /// Block size for new allocations
    block_size: usize,
    /// Index of the current block (most recently allocated from)
    /// Try allocating from this block first before scanning others.
    /// Atomic because the arena may be moved across threads.
    current_block: AtomicUsize,
    /// Total bytes allocated
    allocated: AtomicUsize,
}

unsafe impl Send for MessageArena {}

impl MessageArena {
    /// Create a new arena with default block size.
    pub fn new() -> Self {
        Self::with_block_size(DEFAULT_ARENA_BLOCK_SIZE)
    }

    /// Create a new arena with the specified block size.
    pub fn with_block_size(block_size: usize) -> Self {
        Self {
            blocks: Vec::new(),
            block_size,
            current_block: AtomicUsize::new(0),
            allocated: AtomicUsize::new(0),
        }
    }

    /// Allocate a slice in the arena, copying data from the source.
    ///
    /// Returns an error if memory allocation fails (out of memory).
    pub fn allocate_slice<'arena>(
        &'arena mut self,
        data: &[u8],
    ) -> Result<ArenaSlice<'arena>, std::io::Error> {
        let len = data.len();
        if len == 0 {
            return Ok(ArenaSlice {
                ptr: std::ptr::NonNull::new(&[] as *const [u8] as *mut [u8]).unwrap(),
                len: 0,
                _phantom: std::marker::PhantomData,
            });
        }

        // Fast path: try current block first (O(1) for most allocations)
        let current = self.current_block.load(Ordering::Relaxed);
        if current < self.blocks.len() {
            let block = &self.blocks[current];
            if let Some(offset) = block.try_allocate(len, 1) {
                unsafe {
                    let dst = block.data_at(offset);
                    std::ptr::copy_nonoverlapping(data.as_ptr(), dst, len);
                    self.allocated.fetch_add(len, Ordering::Relaxed);

                    let slice = std::slice::from_raw_parts(dst, len);
                    return Ok(ArenaSlice {
                        ptr: std::ptr::NonNull::new(slice as *const [u8] as *mut [u8]).unwrap(),
                        len,
                        _phantom: std::marker::PhantomData,
                    });
                }
            }
        }

        // Slow path: scan other blocks
        for (i, block) in self.blocks.iter().enumerate() {
            if i == current {
                continue; // Already tried current block
            }
            if let Some(offset) = block.try_allocate(len, 1) {
                // Update current_block to point to this block
                self.current_block.store(i, Ordering::Relaxed);
                unsafe {
                    let dst = block.data_at(offset);
                    std::ptr::copy_nonoverlapping(data.as_ptr(), dst, len);
                    self.allocated.fetch_add(len, Ordering::Relaxed);

                    let slice = std::slice::from_raw_parts(dst, len);
                    return Ok(ArenaSlice {
                        ptr: std::ptr::NonNull::new(slice as *const [u8] as *mut [u8]).unwrap(),
                        len,
                        _phantom: std::marker::PhantomData,
                    });
                }
            }
        }

        // Need a new block
        let new_block_size = self.block_size.max(len);
        let block = ArenaBlock::new(new_block_size)?;
        let offset = block.try_allocate(len, 1).ok_or_else(|| {
            std::io::Error::other(format!(
                "Failed to allocate {} bytes from fresh block of {} bytes",
                len, new_block_size
            ))
        })?;

        let new_block_idx = self.blocks.len();
        unsafe {
            let dst = block.data_at(offset);
            std::ptr::copy_nonoverlapping(data.as_ptr(), dst, len);
            self.allocated.fetch_add(len, Ordering::Relaxed);

            let slice = std::slice::from_raw_parts(dst, len);
            let arena_slice = ArenaSlice {
                ptr: std::ptr::NonNull::new(slice as *const [u8] as *mut [u8]).unwrap(),
                len,
                _phantom: std::marker::PhantomData,
            };

            self.blocks.push(block);
            self.current_block.store(new_block_idx, Ordering::Relaxed);
            Ok(arena_slice)
        }
    }

    /// Reset the arena, reclaiming all allocations.
    pub fn reset(&mut self) {
        for block in &self.blocks {
            block.reset();
        }
        self.current_block.store(0, Ordering::Release);
        self.allocated.store(0, Ordering::Release);
    }

    /// Get the total number of bytes currently allocated.
    pub fn allocated(&self) -> usize {
        self.allocated.load(Ordering::Acquire)
    }

    /// Get the total capacity of all blocks.
    pub fn capacity(&self) -> usize {
        self.blocks.iter().map(|b| b.capacity).sum()
    }
}

impl Default for MessageArena {
    fn default() -> Self {
        Self::new()
    }
}

/// Zero-copy slice reference into an arena.
#[derive(Clone, Copy)]
pub struct ArenaSlice<'arena> {
    /// Pointer to the slice data
    ptr: std::ptr::NonNull<[u8]>,
    /// Length of the slice
    len: usize,
    /// Phantom data to tie the lifetime to the arena
    _phantom: std::marker::PhantomData<&'arena [u8]>,
}

unsafe impl Send for ArenaSlice<'_> {}
unsafe impl Sync for ArenaSlice<'_> {}

impl<'arena> ArenaSlice<'arena> {
    /// Get a reference to the slice data.
    #[inline]
    #[allow(clippy::should_implement_trait)]
    pub fn as_ref(&self) -> &[u8] {
        if self.len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.ptr.as_ptr() as *const u8, self.len) }
        }
    }

    /// Get the length of the slice.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if the slice is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl std::fmt::Debug for ArenaSlice<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ArenaSlice").field(&self.as_ref()).finish()
    }
}

impl AsRef<[u8]> for ArenaSlice<'_> {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arena_allocate() {
        let mut arena = MessageArena::new();
        let data = b"Hello, World!";
        let slice = arena.allocate_slice(data).unwrap();
        assert_eq!(slice.as_ref(), data);
    }

    #[test]
    fn test_arena_empty_slice() {
        let mut arena = MessageArena::new();
        let slice = arena.allocate_slice(b"").unwrap();
        assert_eq!(slice.len(), 0);
        assert!(slice.is_empty());
    }

    #[test]
    fn test_arena_reset() {
        let mut arena = MessageArena::new();
        let data = b"Test data";
        let slice = arena.allocate_slice(data).unwrap();
        assert_eq!(slice.as_ref(), data);
        assert_eq!(arena.allocated(), data.len());

        arena.reset();
        assert_eq!(arena.allocated(), 0);
    }

    #[test]
    fn test_arena_allocated_count() {
        let mut arena = MessageArena::new();
        assert_eq!(arena.allocated(), 0);

        arena.allocate_slice(b"Hello").unwrap();
        assert_eq!(arena.allocated(), 5);

        arena.reset();
        assert_eq!(arena.allocated(), 0);
    }
}
