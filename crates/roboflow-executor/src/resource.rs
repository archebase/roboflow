// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Resource management for slot-based task execution.
//!
//! This module provides resource-aware scheduling with slot-based resource management,
//! inspired by Spark's executor slots. Each slot can run one task at a time.

use std::fmt;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};

/// Unique identifier for a slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SlotId(pub u64);

impl fmt::Display for SlotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Slot({})", self.0)
    }
}

/// Slot state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    /// Available for task assignment.
    Free,
    /// Reserved for a specific task.
    Reserved,
    /// Currently executing a task.
    Busy,
    /// Draining (no new tasks).
    Draining,
}

/// Resource capacity of a slot.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ResourceCapacity {
    /// CPU cores available.
    pub cpu_cores: f64,
    /// Memory in GB.
    pub memory_gb: f64,
    /// GPU count.
    pub gpu_count: u32,
}

impl Default for ResourceCapacity {
    fn default() -> Self {
        Self {
            cpu_cores: 1.0,
            memory_gb: 1.0,
            gpu_count: 0,
        }
    }
}

impl ResourceCapacity {
    /// Create a new resource capacity.
    pub fn new(cpu_cores: f64, memory_gb: f64, gpu_count: u32) -> Self {
        Self {
            cpu_cores,
            memory_gb,
            gpu_count,
        }
    }

    /// Check if this capacity can satisfy a request.
    pub fn can_satisfy(&self, request: &ResourceRequest) -> bool {
        self.cpu_cores >= request.cpu_cores
            && self.memory_gb >= request.memory_gb
            && self.gpu_count >= request.gpu_count
    }
}

/// Resource requirements for a task.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ResourceRequest {
    /// CPU cores required.
    pub cpu_cores: f64,
    /// Memory in GB required.
    pub memory_gb: f64,
    /// GPU count required.
    pub gpu_count: u32,
}

impl Default for ResourceRequest {
    fn default() -> Self {
        Self {
            cpu_cores: 0.5,
            memory_gb: 0.5,
            gpu_count: 0,
        }
    }
}

impl ResourceRequest {
    /// Create a new resource request.
    pub fn new(cpu_cores: f64, memory_gb: f64, gpu_count: u32) -> Self {
        Self {
            cpu_cores,
            memory_gb,
            gpu_count,
        }
    }

    /// Create a request for CPU-only tasks.
    pub fn cpu_only(cores: f64, memory_gb: f64) -> Self {
        Self {
            cpu_cores: cores,
            memory_gb,
            gpu_count: 0,
        }
    }

    /// Create a request for GPU tasks.
    pub fn with_gpu(cores: f64, memory_gb: f64, gpus: u32) -> Self {
        Self {
            cpu_cores: cores,
            memory_gb,
            gpu_count: gpus,
        }
    }
}

/// A slot represents a resource allocation for task execution.
///
/// From Spark's slot model: each executor has N slots that can each run one task.
#[derive(Debug)]
pub struct Slot {
    /// Slot identifier.
    pub id: SlotId,
    /// Worker this slot belongs to.
    pub worker_id: WorkerId,
    /// Current state.
    pub state: SlotState,
    /// Current task (if occupied).
    pub task_id: Option<TaskId>,
    /// Resource capacity.
    pub capacity: ResourceCapacity,
}

/// Task identifier (re-exported from task module).
pub use crate::task::TaskId;

/// Worker identifier (re-exported from object_store module).
pub use crate::object_store::WorkerId;

/// Slot guard that releases the slot when dropped.
pub struct SlotGuard {
    slot_id: SlotId,
    pool: Arc<SlotPoolInner>,
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        let pool = Arc::clone(&self.pool);
        let slot_id = self.slot_id;
        // Spawn a task to release the slot asynchronously
        tokio::spawn(async move {
            pool.release(slot_id).await;
        });
    }
}

/// Inner state of the slot pool.
struct SlotPoolInner {
    slots: Mutex<Vec<Slot>>,
    semaphore: Semaphore,
}

impl SlotPoolInner {
    async fn release(&self, slot_id: SlotId) {
        let mut slots = self.slots.lock().await;
        if let Some(slot) = slots.iter_mut().find(|s| s.id == slot_id) {
            slot.state = SlotState::Free;
            slot.task_id = None;
        }
        drop(slots);
        self.semaphore.add_permits(1);
    }
}

/// Slot pool manages available slots for task execution.
pub struct SlotPool {
    inner: Arc<SlotPoolInner>,
}

impl SlotPool {
    /// Create a new slot pool with the specified number of slots.
    pub fn new(worker_id: WorkerId, num_slots: usize) -> Self {
        let slots: Vec<Slot> = (0..num_slots)
            .map(|i| Slot {
                id: SlotId(i as u64),
                worker_id,
                state: SlotState::Free,
                task_id: None,
                capacity: ResourceCapacity::default(),
            })
            .collect();

        Self {
            inner: Arc::new(SlotPoolInner {
                slots: Mutex::new(slots),
                semaphore: Semaphore::new(num_slots),
            }),
        }
    }

    /// Create a slot pool with custom capacity per slot.
    pub fn with_capacity(
        worker_id: WorkerId,
        num_slots: usize,
        capacity: ResourceCapacity,
    ) -> Self {
        let slots: Vec<Slot> = (0..num_slots)
            .map(|i| Slot {
                id: SlotId(i as u64),
                worker_id,
                state: SlotState::Free,
                task_id: None,
                capacity,
            })
            .collect();

        Self {
            inner: Arc::new(SlotPoolInner {
                slots: Mutex::new(slots),
                semaphore: Semaphore::new(num_slots),
            }),
        }
    }

    /// Acquire a slot for a task.
    ///
    /// Returns None if no slot is available that can satisfy the request.
    pub async fn acquire(&self, request: &ResourceRequest, task_id: TaskId) -> Option<SlotGuard> {
        // Acquire permit first
        let _permit = self.inner.semaphore.acquire().await.ok()?;

        let mut slots = self.inner.slots.lock().await;

        // Find a free slot that can satisfy the request
        for slot in slots.iter_mut() {
            if slot.state == SlotState::Free && slot.capacity.can_satisfy(request) {
                slot.state = SlotState::Busy;
                slot.task_id = Some(task_id);
                let slot_id = slot.id;
                drop(slots);
                drop(_permit);

                return Some(SlotGuard {
                    slot_id,
                    pool: Arc::clone(&self.inner),
                });
            }
        }

        // No suitable slot found, release the permit
        drop(slots);
        self.inner.semaphore.add_permits(1);
        None
    }

    /// Get the number of available (free) slots.
    pub async fn available(&self) -> usize {
        let slots = self.inner.slots.lock().await;
        slots.iter().filter(|s| s.state == SlotState::Free).count()
    }

    /// Get the total number of slots.
    pub fn total(&self) -> usize {
        self.inner.semaphore.available_permits()
            + self
                .inner
                .slots
                .try_lock()
                .map(|s| {
                    s.iter()
                        .filter(|slot| slot.state != SlotState::Free)
                        .count()
                })
                .unwrap_or(0)
    }

    /// Get current utilization (0.0 to 1.0).
    pub async fn utilization(&self) -> f64 {
        let slots = self.inner.slots.lock().await;
        let total = slots.len();
        let busy = slots.iter().filter(|s| s.state == SlotState::Busy).count();
        if total == 0 {
            0.0
        } else {
            busy as f64 / total as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_slot_pool_basic() {
        let pool = SlotPool::new(WorkerId(1), 4);

        assert_eq!(pool.total(), 4);
        assert_eq!(pool.available().await, 4);

        // Acquire a slot
        let guard = pool.acquire(&ResourceRequest::default(), TaskId(1)).await;
        assert!(guard.is_some());
        assert_eq!(pool.available().await, 3);

        // Drop the guard to release
        drop(guard);
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        assert_eq!(pool.available().await, 4);
    }

    #[tokio::test]
    async fn test_slot_pool_capacity_check() {
        let capacity = ResourceCapacity::new(2.0, 4.0, 0);
        let pool = SlotPool::with_capacity(WorkerId(1), 2, capacity);

        // Request that fits
        let request_ok = ResourceRequest::new(1.0, 2.0, 0);
        let guard1 = pool.acquire(&request_ok, TaskId(1)).await;
        assert!(guard1.is_some());

        // Request that exceeds capacity
        let request_too_large = ResourceRequest::new(4.0, 8.0, 0);
        let guard2 = pool.acquire(&request_too_large, TaskId(2)).await;
        assert!(guard2.is_none());
    }

    #[tokio::test]
    async fn test_slot_pool_exhaustion() {
        let pool = SlotPool::new(WorkerId(1), 2);

        let guard1 = pool.acquire(&ResourceRequest::default(), TaskId(1)).await;
        let guard2 = pool.acquire(&ResourceRequest::default(), TaskId(2)).await;
        assert!(guard1.is_some());
        assert!(guard2.is_some());

        // Third acquire should fail immediately (no slots)
        let guard3 = pool.acquire(&ResourceRequest::default(), TaskId(3)).await;
        assert!(guard3.is_none());
    }

    #[tokio::test]
    async fn test_resource_capacity_can_satisfy() {
        let capacity = ResourceCapacity::new(4.0, 16.0, 2);

        assert!(capacity.can_satisfy(&ResourceRequest::new(2.0, 8.0, 1)));
        assert!(capacity.can_satisfy(&ResourceRequest::new(4.0, 16.0, 2)));
        assert!(!capacity.can_satisfy(&ResourceRequest::new(8.0, 8.0, 1)));
        assert!(!capacity.can_satisfy(&ResourceRequest::new(2.0, 32.0, 1)));
        assert!(!capacity.can_satisfy(&ResourceRequest::new(2.0, 8.0, 4)));
    }
}
