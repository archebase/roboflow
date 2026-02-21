// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Slot pool for resource management.
//!
//! This module provides slot-based resource management inspired by Spark's executor slots.
//! Each slot can execute one task at a time, providing predictable resource control.

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Error type for slot pool operations.
#[derive(Debug, thiserror::Error)]
pub enum SlotPoolError {
    /// No slots available (non-blocking acquire failed).
    #[error("no slots available")]
    NoSlotsAvailable,

    /// Slot pool has been closed.
    #[error("slot pool closed")]
    PoolClosed,
}

/// Result type for slot pool operations.
pub type Result<T> = std::result::Result<T, SlotPoolError>;

/// A pool of execution slots for resource management.
///
/// Each slot represents a unit of concurrent execution capacity.
/// When all slots are occupied, new tasks must wait or fail (depending on
/// the acquisition method).
///
/// # Example
///
/// ```rust,ignore
/// use roboflow_distributed::SlotPool;
///
/// #[tokio::main]
/// async fn main() {
///     // Create a pool with 4 slots
///     let pool = SlotPool::new(4);
///
///     // Acquire a slot (async, waits if none available)
///     let slot = pool.acquire().await.unwrap();
///
///     // Slot is automatically released when dropped
///     drop(slot);
/// }
/// ```
#[derive(Debug)]
pub struct SlotPool {
    /// Semaphore controlling slot availability.
    semaphore: Arc<Semaphore>,

    /// Maximum number of slots.
    max_slots: usize,
}

impl SlotPool {
    /// Create a new slot pool with the given number of slots.
    ///
    /// # Arguments
    ///
    /// * `slots` - Number of concurrent execution slots.
    pub fn new(slots: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(slots)),
            max_slots: slots,
        }
    }

    /// Acquire a slot, waiting if necessary.
    ///
    /// This method will wait until a slot becomes available.
    /// Returns a `SlotGuard` that releases the slot when dropped.
    ///
    /// # Errors
    ///
    /// Returns an error if the pool has been closed.
    pub async fn acquire(&self) -> Result<SlotGuard> {
        match self.semaphore.clone().acquire_owned().await {
            Ok(permit) => Ok(SlotGuard { permit }),
            Err(_) => Err(SlotPoolError::PoolClosed),
        }
    }

    /// Try to acquire a slot without waiting.
    ///
    /// Returns immediately with `None` if no slots are available.
    pub fn try_acquire(&self) -> Option<SlotGuard> {
        match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => Some(SlotGuard { permit }),
            Err(_) => None,
        }
    }

    /// Get the number of available slots.
    pub fn available(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Get the maximum number of slots.
    pub fn max_slots(&self) -> usize {
        self.max_slots
    }

    /// Get current utilization as a fraction (0.0 to 1.0).
    pub fn utilization(&self) -> f64 {
        let used = self.max_slots - self.available();
        used as f64 / self.max_slots as f64
    }
}

impl Clone for SlotPool {
    fn clone(&self) -> Self {
        Self {
            semaphore: Arc::clone(&self.semaphore),
            max_slots: self.max_slots,
        }
    }
}

/// Guard representing a held slot.
///
/// The slot is released when this guard is dropped.
#[derive(Debug)]
pub struct SlotGuard {
    /// The semaphore permit representing the held slot.
    #[allow(dead_code)]
    permit: OwnedSemaphorePermit,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_slot_pool_basic() {
        let pool = SlotPool::new(2);

        assert_eq!(pool.max_slots(), 2);
        assert_eq!(pool.available(), 2);
        assert_eq!(pool.utilization(), 0.0);

        // Acquire first slot
        let slot1 = pool.acquire().await.unwrap();
        assert_eq!(pool.available(), 1);
        assert_eq!(pool.utilization(), 0.5);

        // Acquire second slot
        let slot2 = pool.acquire().await.unwrap();
        assert_eq!(pool.available(), 0);
        assert_eq!(pool.utilization(), 1.0);

        // Try to acquire without waiting should fail
        assert!(pool.try_acquire().is_none());

        // Release slots
        drop(slot1);
        assert_eq!(pool.available(), 1);

        drop(slot2);
        assert_eq!(pool.available(), 2);
    }

    #[tokio::test]
    async fn test_slot_pool_concurrent() {
        let pool = SlotPool::new(2);
        let pool_clone = pool.clone();

        // Acquire both slots
        let slot1 = pool.acquire().await.unwrap();
        let slot2 = pool.acquire().await.unwrap();

        // Spawn a task that waits for a slot
        let handle = tokio::spawn(async move {
            let start = std::time::Instant::now();
            let _slot = pool_clone.acquire().await.unwrap();
            start.elapsed()
        });

        // Wait a bit to ensure the spawned task is waiting
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Release one slot
        drop(slot1);

        // The spawned task should have acquired the slot
        let elapsed = handle.await.unwrap();
        assert!(elapsed.as_millis() < 100); // Should be quick after release

        drop(slot2);
    }

    #[test]
    fn test_slot_pool_clone() {
        let pool1 = SlotPool::new(2);
        let pool2 = pool1.clone();

        // Both should share the same semaphore
        assert_eq!(pool1.available(), 2);
        assert_eq!(pool2.available(), 2);
    }
}
