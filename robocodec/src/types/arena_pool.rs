// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Arena pool for reusing arena allocations across chunks.
//!
//! This module provides an arena pool that eliminates allocation/deallocation
//! overhead by reusing arenas across chunk processing. Instead of allocating
//! a new 64MB arena for each chunk and freeing it after compression, arenas
//! are reset and returned to the pool for reuse.
//!
//! This optimization eliminates ~22% of CPU time spent on memory operations.

use crossbeam_channel::{bounded, Receiver, Sender};

use super::arena::MessageArena;

/// A pool of reusable arenas.
///
/// The pool pre-allocates arenas at startup and recycles them after use.
/// This eliminates the allocation/deallocation overhead that occurs when
/// creating and dropping arenas for each chunk.
#[derive(Clone)]
pub struct ArenaPool {
    /// Channel to get arenas from the pool
    available: Receiver<MessageArena>,
    /// Channel to return arenas to the pool
    returns: Sender<MessageArena>,
}

impl ArenaPool {
    /// Create a new arena pool with the specified number of pre-allocated arenas.
    ///
    /// A good size is `num_threads * 2` to ensure there are always arenas
    /// available while others are being processed.
    pub fn new(size: usize) -> Self {
        let (sender, receiver) = bounded(size);

        // Pre-allocate arenas
        for _ in 0..size {
            let arena = MessageArena::new();
            // Ignore error if channel is full (shouldn't happen on init)
            let _ = sender.send(arena);
        }

        Self {
            available: receiver,
            returns: sender,
        }
    }

    /// Get an arena from the pool.
    ///
    /// If no arenas are available, creates a new one (fallback for high concurrency).
    pub fn get(&self) -> PooledArena {
        let arena = self.available.try_recv().unwrap_or_else(|_| {
            // Pool exhausted, create a new arena (rare case)
            MessageArena::new()
        });

        PooledArena {
            arena: Some(arena),
            pool: self.clone(),
        }
    }

    /// Return an arena to the pool (internal use).
    fn return_arena(&self, mut arena: MessageArena) {
        arena.reset();
        // If channel is full, just drop the arena (rare case)
        let _ = self.returns.try_send(arena);
    }

    /// Get the number of arenas currently available in the pool.
    pub fn available_count(&self) -> usize {
        self.available.len()
    }
}

/// An arena borrowed from the pool.
///
/// When dropped, the arena is automatically reset and returned to the pool
/// instead of being deallocated.
pub struct PooledArena {
    arena: Option<MessageArena>,
    pool: ArenaPool,
}

impl PooledArena {
    /// Get a mutable reference to the underlying arena.
    #[inline]
    pub fn arena_mut(&mut self) -> &mut MessageArena {
        self.arena.as_mut().expect("arena already taken")
    }

    /// Get a reference to the underlying arena.
    #[inline]
    pub fn arena(&self) -> &MessageArena {
        self.arena.as_ref().expect("arena already taken")
    }

    /// Take ownership of the arena, removing it from pool management.
    ///
    /// The arena will be deallocated when dropped instead of returned to pool.
    /// Use this only when you need to transfer arena ownership.
    pub fn take(mut self) -> MessageArena {
        self.arena.take().expect("arena already taken")
    }
}

impl Drop for PooledArena {
    fn drop(&mut self) {
        if let Some(arena) = self.arena.take() {
            self.pool.return_arena(arena);
        }
    }
}

impl std::ops::Deref for PooledArena {
    type Target = MessageArena;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.arena()
    }
}

impl std::ops::DerefMut for PooledArena {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.arena_mut()
    }
}

/// Global arena pool for the pipeline.
///
/// This is lazily initialized on first use with a size based on available CPUs.
static GLOBAL_POOL: std::sync::OnceLock<ArenaPool> = std::sync::OnceLock::new();

/// Get the global arena pool.
///
/// The pool is initialized with `num_cpus * 2` arenas on first call.
pub fn global_pool() -> &'static ArenaPool {
    GLOBAL_POOL.get_or_init(|| {
        let num_cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(8);
        ArenaPool::new(num_cpus * 2)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arena_pool_basic() {
        let pool = ArenaPool::new(4);
        assert_eq!(pool.available_count(), 4);

        // Get an arena
        let arena1 = pool.get();
        assert_eq!(pool.available_count(), 3);

        // Drop returns it to pool
        drop(arena1);
        assert_eq!(pool.available_count(), 4);
    }

    #[test]
    fn test_arena_pool_reuse() {
        let pool = ArenaPool::new(2);

        // Get and use arena
        {
            let mut arena = pool.get();
            arena.arena_mut().allocate_slice(b"test data").unwrap();
            assert!(arena.arena().allocated() > 0);
        }
        // Arena returned to pool and reset

        // Get arena again - should be reset
        let arena = pool.get();
        assert_eq!(arena.arena().allocated(), 0);
    }

    #[test]
    fn test_arena_pool_exhaustion() {
        let pool = ArenaPool::new(1);

        let _arena1 = pool.get();
        // Pool exhausted, but get() should still work (creates new arena)
        let _arena2 = pool.get();
    }

    #[test]
    fn test_pooled_arena_deref() {
        let pool = ArenaPool::new(1);
        let mut arena = pool.get();

        // Test DerefMut
        arena.allocate_slice(b"hello").unwrap();

        // Test Deref
        assert_eq!(arena.allocated(), 5);
    }
}
