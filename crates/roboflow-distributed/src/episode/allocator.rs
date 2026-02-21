// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Episode index allocation for distributed LeRobot dataset generation.
//!
//! This module provides atomic episode index allocation for workers converting
//! bag/MCAP files to LeRobot format. It ensures that each worker gets a unique
//! episode index and that the chunk structure is properly maintained.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
//! │   Worker 1      │     │   Worker 2      │     │   Worker N      │
//! │ allocate() ─────┼─────┼─────allocate()──┼─────┼───allocate()    │
//! └────────┬────────┘     └────────┬────────┘     └────────┬────────┘
//!          │                       │                       │
//!          └───────────────────────┼───────────────────────┘
//!                                  │
//!                    ┌─────────────▼─────────────┐
//!                    │   TiKV Atomic Counter     │
//!                    │   (CAS operation)         │
//!                    │   Key: /batch/{id}/episode│
//!                    └───────────────────────────┘
//! ```
//!
//! # Episode Index Layout
//!
//! For 100,000 episodes with 500 episodes per chunk:
//! - Episodes 0-499 → chunk-000
//! - Episodes 500-999 → chunk-001
//! - ...
//! - Episodes 99,500-99,999 → chunk-199
//!
//! # Usage
//!
//! ```ignore
//! use roboflow_distributed::episode::{TiKVEpisodeAllocator, EpisodeAllocation};
//!
//! let allocator = TiKVEpisodeAllocator::new(
//!     tikv_client,
//!     "batch-123".to_string(),
//!     500, // episodes_per_chunk
//! );
//!
//! // Allocate a single episode index
//! let allocation = allocator.allocate().await?;
//! println!("Episode {} in chunk {}", allocation.episode_index, allocation.chunk_index);
//!
//! // Allocate batch of episodes (more efficient for high throughput)
//! let allocations = allocator.allocate_batch(10).await?;
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::tikv::TikvClient;

/// Errors related to episode allocation.
#[derive(Debug, Error)]
pub enum EpisodeAllocatorError {
    /// Failed to communicate with TiKV.
    #[error("TiKV error during episode allocation: {0}")]
    TikvError(#[from] crate::tikv::TikvError),

    /// No more episode indices available (overflow).
    #[error("episode index overflow: cannot allocate beyond {max_episodes}")]
    Overflow {
        /// Maximum episodes supported.
        max_episodes: u64,
    },

    /// Allocator not initialized.
    #[error("episode allocator not initialized for batch {batch_id}")]
    NotInitialized {
        /// Batch identifier.
        batch_id: String,
    },

    /// Batch allocation would exceed limits.
    #[error(
        "batch allocation of {requested} episodes would exceed limit (current: {current}, max: {max})"
    )]
    BatchExceedsLimit {
        /// Requested number of episodes.
        requested: usize,
        /// Current allocated count.
        current: u64,
        /// Maximum allowed.
        max: u64,
    },
}

/// Result type for episode allocation operations.
pub type Result<T> = std::result::Result<T, EpisodeAllocatorError>;

/// Allocation result containing episode and chunk indices.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpisodeAllocation {
    /// The allocated episode index (global across the batch).
    pub episode_index: u64,

    /// The chunk index this episode belongs to.
    pub chunk_index: u32,

    /// Offset within the chunk (0 to episodes_per_chunk - 1).
    pub chunk_offset: u32,
}

impl EpisodeAllocation {
    /// Create a new episode allocation.
    pub fn new(episode_index: u64, episodes_per_chunk: u32) -> Self {
        let chunk_index = (episode_index / episodes_per_chunk as u64) as u32;
        let chunk_offset = (episode_index % episodes_per_chunk as u64) as u32;

        Self {
            episode_index,
            chunk_index,
            chunk_offset,
        }
    }

    /// Get the video path for this episode.
    ///
    /// Returns path like: `videos/chunk-000/camera_name/episode_000000.mp4`
    pub fn video_path(&self, camera_name: &str) -> String {
        format!(
            "videos/chunk-{:03}/{}/episode_{:06}.mp4",
            self.chunk_index, camera_name, self.episode_index
        )
    }

    /// Get the parquet path for this episode.
    ///
    /// Returns path like: `data/chunk-000/episode_000000.parquet`
    pub fn parquet_path(&self) -> String {
        format!(
            "data/chunk-{:03}/episode_{:06}.parquet",
            self.chunk_index, self.episode_index
        )
    }
}

/// Internal state stored in TiKV for episode counter.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct EpisodeCounter {
    /// Current allocated episode index (next to be allocated).
    next_episode: u64,

    /// Version for CAS operations.
    version: u64,
}

impl EpisodeCounter {
    fn new() -> Self {
        Self {
            next_episode: 0,
            version: 0,
        }
    }
}

/// Trait for episode index allocation.
///
/// Implementations must guarantee atomic allocation - no two workers
/// should ever receive the same episode index.
#[async_trait]
pub trait EpisodeAllocator: Send + Sync {
    /// Allocate a single episode index.
    ///
    /// Returns an `EpisodeAllocation` containing the episode index,
    /// chunk index, and chunk offset.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - TiKV is unavailable
    /// - Episode index overflow (exceeds u64::MAX)
    async fn allocate(&self) -> Result<EpisodeAllocation>;

    /// Allocate a batch of consecutive episode indices.
    ///
    /// This is more efficient than calling `allocate()` multiple times
    /// as it only requires one CAS operation.
    ///
    /// # Arguments
    ///
    /// * `count` - Number of episode indices to allocate.
    ///
    /// # Returns
    ///
    /// A vector of `EpisodeAllocation` in order (first allocated index first).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - TiKV is unavailable
    /// - Allocation would exceed maximum episodes
    async fn allocate_batch(&self, count: usize) -> Result<Vec<EpisodeAllocation>>;

    /// Get the current allocated count (for monitoring/health checks).
    ///
    /// This returns the number of episodes that have been allocated,
    /// not necessarily completed.
    async fn allocated_count(&self) -> Result<u64>;

    /// Get the episodes per chunk configuration.
    fn episodes_per_chunk(&self) -> u32;
}

/// TiKV-based episode allocator using atomic CAS operations.
///
/// This is the production implementation for distributed environments.
/// It uses TiKV's pessimistic transactions to ensure atomic allocation.
pub struct TiKVEpisodeAllocator {
    /// TiKV client for coordination.
    client: Arc<TikvClient>,

    /// Batch ID for key namespacing.
    batch_id: String,

    /// Number of episodes per chunk.
    episodes_per_chunk: u32,

    /// Maximum episodes allowed (for overflow protection).
    max_episodes: u64,

    /// TiKV key for the episode counter.
    counter_key: Vec<u8>,
}

impl TiKVEpisodeAllocator {
    /// Create a new TiKV-based episode allocator.
    ///
    /// # Arguments
    ///
    /// * `client` - TiKV client for coordination.
    /// * `batch_id` - Unique batch identifier.
    /// * `episodes_per_chunk` - Number of episodes per chunk (e.g., 500).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let allocator = TiKVEpisodeAllocator::new(
    ///     tikv_client,
    ///     "batch-123".to_string(),
    ///     500,
    /// );
    /// ```
    pub fn new(client: Arc<TikvClient>, batch_id: String, episodes_per_chunk: u32) -> Self {
        let counter_key = format!("/batch/{}/episode_counter", batch_id);
        let max_episodes = (u32::MAX as u64) * (episodes_per_chunk as u64);

        Self {
            client,
            batch_id,
            episodes_per_chunk,
            max_episodes,
            counter_key: counter_key.into_bytes(),
        }
    }

    /// Set a custom maximum episode count.
    ///
    /// This is useful for testing or for batches with known limits.
    pub fn with_max_episodes(mut self, max: u64) -> Self {
        self.max_episodes = max;
        self
    }

    /// Initialize the counter if it doesn't exist.
    ///
    /// This is idempotent - calling it multiple times is safe.
    async fn ensure_initialized(&self) -> Result<()> {
        let existing = self.client.get(self.counter_key.clone()).await?;

        if existing.is_none() {
            let counter = EpisodeCounter::new();
            let value = serde_json::to_vec(&counter)
                .map_err(|e| crate::tikv::TikvError::Serialization(e.to_string()))?;

            // Try to create with version 0 (expected to not exist)
            // If this fails, another worker beat us to it - that's fine
            let _ = self.client.cas(self.counter_key.clone(), 0, value).await;
        }

        Ok(())
    }

    /// Perform a single CAS allocation.
    async fn cas_allocate(&self, count: u64) -> Result<u64> {
        self.ensure_initialized().await?;

        // Read current counter
        let existing = self
            .client
            .get(self.counter_key.clone())
            .await?
            .ok_or_else(|| EpisodeAllocatorError::NotInitialized {
                batch_id: self.batch_id.clone(),
            })?;

        let mut counter: EpisodeCounter = serde_json::from_slice(&existing)
            .map_err(|e| crate::tikv::TikvError::Deserialization(e.to_string()))?;

        // Check for overflow
        let new_next = counter.next_episode.checked_add(count).ok_or({
            EpisodeAllocatorError::Overflow {
                max_episodes: self.max_episodes,
            }
        })?;

        if new_next > self.max_episodes {
            return Err(EpisodeAllocatorError::Overflow {
                max_episodes: self.max_episodes,
            });
        }

        // Prepare new counter value
        let allocated_start = counter.next_episode;
        counter.next_episode = new_next;
        counter.version += 1;

        let new_value = serde_json::to_vec(&counter)
            .map_err(|e| crate::tikv::TikvError::Serialization(e.to_string()))?;

        // Attempt CAS
        let success = self
            .client
            .cas(self.counter_key.clone(), counter.version - 1, new_value)
            .await?;

        if success {
            Ok(allocated_start)
        } else {
            // CAS failed due to concurrent modification, retry
            // In practice, we'd use a retry loop with exponential backoff
            Err(EpisodeAllocatorError::from(
                crate::tikv::TikvError::CasFailed {
                    expected: counter.version - 1,
                    got: counter.version,
                },
            ))
        }
    }
}

#[async_trait]
impl EpisodeAllocator for TiKVEpisodeAllocator {
    async fn allocate(&self) -> Result<EpisodeAllocation> {
        // Retry loop with exponential backoff
        let mut attempts = 0;
        let max_attempts = 10;

        loop {
            match self.cas_allocate(1).await {
                Ok(start) => {
                    return Ok(EpisodeAllocation::new(start, self.episodes_per_chunk));
                }
                Err(EpisodeAllocatorError::TikvError(crate::tikv::TikvError::CasFailed {
                    ..
                })) => {
                    attempts += 1;
                    if attempts >= max_attempts {
                        return Err(EpisodeAllocatorError::TikvError(
                            crate::tikv::TikvError::CasFailed {
                                expected: 0,
                                got: 0,
                            },
                        ));
                    }
                    // Exponential backoff: 1ms, 2ms, 4ms, 8ms, ...
                    let delay_ms = 1u64 << (attempts - 1).min(6);
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn allocate_batch(&self, count: usize) -> Result<Vec<EpisodeAllocation>> {
        if count == 0 {
            return Ok(Vec::new());
        }

        // Retry loop with exponential backoff
        let mut attempts = 0;
        let max_attempts = 10;

        loop {
            match self.cas_allocate(count as u64).await {
                Ok(start) => {
                    let allocations: Vec<_> = (start..start + count as u64)
                        .map(|idx| EpisodeAllocation::new(idx, self.episodes_per_chunk))
                        .collect();
                    return Ok(allocations);
                }
                Err(EpisodeAllocatorError::TikvError(crate::tikv::TikvError::CasFailed {
                    ..
                })) => {
                    attempts += 1;
                    if attempts >= max_attempts {
                        return Err(EpisodeAllocatorError::TikvError(
                            crate::tikv::TikvError::CasFailed {
                                expected: 0,
                                got: 0,
                            },
                        ));
                    }
                    let delay_ms = 1u64 << (attempts - 1).min(6);
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn allocated_count(&self) -> Result<u64> {
        let existing = self.client.get(self.counter_key.clone()).await?;

        match existing {
            Some(data) => {
                let counter: EpisodeCounter = serde_json::from_slice(&data)
                    .map_err(|e| crate::tikv::TikvError::Deserialization(e.to_string()))?;
                Ok(counter.next_episode)
            }
            None => Ok(0),
        }
    }

    fn episodes_per_chunk(&self) -> u32 {
        self.episodes_per_chunk
    }
}

/// Local (in-memory) episode allocator for testing and single-worker scenarios.
///
/// This is NOT suitable for distributed environments - use `TiKVEpisodeAllocator`
/// for production workloads.
#[derive(Debug)]
pub struct LocalEpisodeAllocator {
    /// Current episode counter.
    counter: std::sync::atomic::AtomicU64,

    /// Episodes per chunk.
    episodes_per_chunk: u32,

    /// Maximum episodes.
    max_episodes: u64,
}

impl LocalEpisodeAllocator {
    /// Create a new local episode allocator.
    pub fn new(episodes_per_chunk: u32) -> Self {
        Self {
            counter: std::sync::atomic::AtomicU64::new(0),
            episodes_per_chunk,
            max_episodes: (u32::MAX as u64) * (episodes_per_chunk as u64),
        }
    }

    /// Set a custom maximum episode count.
    pub fn with_max_episodes(mut self, max: u64) -> Self {
        self.max_episodes = max;
        self
    }
}

#[async_trait]
impl EpisodeAllocator for LocalEpisodeAllocator {
    async fn allocate(&self) -> Result<EpisodeAllocation> {
        let episode_index = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        if episode_index >= self.max_episodes {
            return Err(EpisodeAllocatorError::Overflow {
                max_episodes: self.max_episodes,
            });
        }

        Ok(EpisodeAllocation::new(
            episode_index,
            self.episodes_per_chunk,
        ))
    }

    async fn allocate_batch(&self, count: usize) -> Result<Vec<EpisodeAllocation>> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let start = self
            .counter
            .fetch_add(count as u64, std::sync::atomic::Ordering::SeqCst);

        if start + count as u64 > self.max_episodes {
            return Err(EpisodeAllocatorError::BatchExceedsLimit {
                requested: count,
                current: start,
                max: self.max_episodes,
            });
        }

        let allocations = (start..start + count as u64)
            .map(|idx| EpisodeAllocation::new(idx, self.episodes_per_chunk))
            .collect();

        Ok(allocations)
    }

    async fn allocated_count(&self) -> Result<u64> {
        Ok(self.counter.load(std::sync::atomic::Ordering::SeqCst))
    }

    fn episodes_per_chunk(&self) -> u32 {
        self.episodes_per_chunk
    }
}

/// Calculate chunk index from episode index.
///
/// This is a simple utility function for cases where you already have
/// an episode index and need to compute the chunk.
#[inline]
pub fn chunk_index_from_episode(episode_index: u64, episodes_per_chunk: u32) -> u32 {
    (episode_index / episodes_per_chunk as u64) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_episode_allocation_chunk_calculation() {
        // 500 episodes per chunk
        let episodes_per_chunk = 500u32;

        // Episode 0 → chunk 0, offset 0
        let alloc = EpisodeAllocation::new(0, episodes_per_chunk);
        assert_eq!(alloc.chunk_index, 0);
        assert_eq!(alloc.chunk_offset, 0);

        // Episode 499 → chunk 0, offset 499
        let alloc = EpisodeAllocation::new(499, episodes_per_chunk);
        assert_eq!(alloc.chunk_index, 0);
        assert_eq!(alloc.chunk_offset, 499);

        // Episode 500 → chunk 1, offset 0
        let alloc = EpisodeAllocation::new(500, episodes_per_chunk);
        assert_eq!(alloc.chunk_index, 1);
        assert_eq!(alloc.chunk_offset, 0);

        // Episode 999 → chunk 1, offset 499
        let alloc = EpisodeAllocation::new(999, episodes_per_chunk);
        assert_eq!(alloc.chunk_index, 1);
        assert_eq!(alloc.chunk_offset, 499);

        // Episode 1000 → chunk 2, offset 0
        let alloc = EpisodeAllocation::new(1000, episodes_per_chunk);
        assert_eq!(alloc.chunk_index, 2);
        assert_eq!(alloc.chunk_offset, 0);

        // Episode 99500 → chunk 199, offset 0
        let alloc = EpisodeAllocation::new(99500, episodes_per_chunk);
        assert_eq!(alloc.chunk_index, 199);
        assert_eq!(alloc.chunk_offset, 0);

        // Episode 99999 → chunk 199, offset 499
        let alloc = EpisodeAllocation::new(99999, episodes_per_chunk);
        assert_eq!(alloc.chunk_index, 199);
        assert_eq!(alloc.chunk_offset, 499);
    }

    #[test]
    fn test_episode_allocation_paths() {
        let alloc = EpisodeAllocation::new(0, 500);
        assert_eq!(
            alloc.video_path("camera_left"),
            "videos/chunk-000/camera_left/episode_000000.mp4"
        );
        assert_eq!(
            alloc.parquet_path(),
            "data/chunk-000/episode_000000.parquet"
        );

        let alloc = EpisodeAllocation::new(500, 500);
        assert_eq!(
            alloc.video_path("camera_right"),
            "videos/chunk-001/camera_right/episode_000500.mp4"
        );
        assert_eq!(
            alloc.parquet_path(),
            "data/chunk-001/episode_000500.parquet"
        );

        let alloc = EpisodeAllocation::new(99999, 500);
        assert_eq!(
            alloc.video_path("cam"),
            "videos/chunk-199/cam/episode_099999.mp4"
        );
    }

    #[test]
    fn test_chunk_index_utility() {
        assert_eq!(chunk_index_from_episode(0, 500), 0);
        assert_eq!(chunk_index_from_episode(499, 500), 0);
        assert_eq!(chunk_index_from_episode(500, 500), 1);
        assert_eq!(chunk_index_from_episode(99999, 500), 199);
    }

    #[tokio::test]
    async fn test_local_allocator_single() {
        let allocator = LocalEpisodeAllocator::new(500);

        let alloc1 = allocator.allocate().await.unwrap();
        assert_eq!(alloc1.episode_index, 0);
        assert_eq!(alloc1.chunk_index, 0);

        let alloc2 = allocator.allocate().await.unwrap();
        assert_eq!(alloc2.episode_index, 1);
        assert_eq!(alloc2.chunk_index, 0);

        let count = allocator.allocated_count().await.unwrap();
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn test_local_allocator_batch() {
        let allocator = LocalEpisodeAllocator::new(500);

        let batch = allocator.allocate_batch(5).await.unwrap();
        assert_eq!(batch.len(), 5);
        assert_eq!(batch[0].episode_index, 0);
        assert_eq!(batch[4].episode_index, 4);

        let next = allocator.allocate().await.unwrap();
        assert_eq!(next.episode_index, 5);

        let count = allocator.allocated_count().await.unwrap();
        assert_eq!(count, 6);
    }

    #[tokio::test]
    async fn test_local_allocator_overflow() {
        let allocator = LocalEpisodeAllocator::new(500).with_max_episodes(5);

        // Should succeed
        let _ = allocator.allocate_batch(5).await.unwrap();

        // Should fail
        let result = allocator.allocate().await;
        assert!(matches!(
            result,
            Err(EpisodeAllocatorError::Overflow { .. })
        ));
    }

    #[tokio::test]
    async fn test_local_allocator_concurrent() {
        use std::sync::Arc;

        let allocator = Arc::new(LocalEpisodeAllocator::new(500));
        let mut handles = Vec::new();

        // Spawn 10 concurrent tasks, each allocating 10 episodes
        for _ in 0..10 {
            let alloc = allocator.clone();
            handles.push(tokio::spawn(async move {
                let mut episodes = Vec::new();
                for _ in 0..10 {
                    episodes.push(alloc.allocate().await.unwrap());
                }
                episodes
            }));
        }

        let results: Vec<_> = futures::future::join_all(handles).await;

        // Collect all allocated episode indices
        let mut all_episodes: Vec<u64> = results
            .into_iter()
            .flat_map(|r| r.unwrap())
            .map(|a| a.episode_index)
            .collect();

        all_episodes.sort();
        all_episodes.dedup();

        // Should have exactly 100 unique episodes
        assert_eq!(all_episodes.len(), 100);
        assert_eq!(all_episodes[0], 0);
        assert_eq!(all_episodes[99], 99);
    }

    #[test]
    fn test_episode_counter_serialization() {
        let counter = EpisodeCounter::new();
        let json = serde_json::to_vec(&counter).unwrap();
        let decoded: EpisodeCounter = serde_json::from_slice(&json).unwrap();
        assert_eq!(counter.next_episode, decoded.next_episode);
        assert_eq!(counter.version, decoded.version);
    }

    #[test]
    fn test_episode_allocation_serialization() {
        let alloc = EpisodeAllocation::new(12345, 500);

        let bincode_data = bincode::serialize(&alloc).unwrap();
        let decoded: EpisodeAllocation = bincode::deserialize(&bincode_data).unwrap();
        assert_eq!(alloc, decoded);

        let json_data = serde_json::to_vec(&alloc).unwrap();
        let json_decoded: EpisodeAllocation = serde_json::from_slice(&json_data).unwrap();
        assert_eq!(alloc, json_decoded);
    }

    // =========================================================================
    // Scale Tests for 100K Episode Support
    // =========================================================================

    #[test]
    fn test_chunk_calculation_100k_episodes() {
        // Verify chunk calculation for all episode indices in a 100K dataset
        let episodes_per_chunk = 500u32;

        // Test boundary cases
        let test_cases = [
            // (episode_index, expected_chunk, expected_offset)
            (0u64, 0u32, 0u32),
            (1u64, 0u32, 1u32),
            (499u64, 0u32, 499u32), // Last episode in chunk 0
            (500u64, 1u32, 0u32),   // First episode in chunk 1
            (501u64, 1u32, 1u32),
            (999u64, 1u32, 499u32), // Last episode in chunk 1
            (1000u64, 2u32, 0u32),  // First episode in chunk 2
            (5000u64, 10u32, 0u32),
            (50000u64, 100u32, 0u32),
            (99499u64, 198u32, 499u32), // Last episode in chunk 198
            (99500u64, 199u32, 0u32),   // First episode in chunk 199
            (99999u64, 199u32, 499u32), // Last episode in chunk 199
        ];

        for (episode_idx, expected_chunk, expected_offset) in test_cases {
            let alloc = EpisodeAllocation::new(episode_idx, episodes_per_chunk);
            assert_eq!(
                alloc.chunk_index, expected_chunk,
                "Episode {} should be in chunk {}",
                episode_idx, expected_chunk
            );
            assert_eq!(
                alloc.chunk_offset, expected_offset,
                "Episode {} should have offset {}",
                episode_idx, expected_offset
            );
        }
    }

    #[test]
    fn test_chunk_count_for_100k_episodes() {
        // With 500 episodes per chunk, 100K episodes should have 200 chunks (0-199)
        let total_episodes = 100_000u64;
        let episodes_per_chunk = 500u32;

        let last_episode = total_episodes - 1;
        let last_chunk = chunk_index_from_episode(last_episode, episodes_per_chunk);

        assert_eq!(last_chunk, 199, "100K episodes should end at chunk 199");
    }

    #[test]
    fn test_episode_paths_100k_scale() {
        // Test video and parquet paths for episode indices at 100K scale
        let episodes_per_chunk = 500u32;

        // First episode
        let alloc = EpisodeAllocation::new(0, episodes_per_chunk);
        assert_eq!(
            alloc.video_path("cam"),
            "videos/chunk-000/cam/episode_000000.mp4"
        );
        assert_eq!(
            alloc.parquet_path(),
            "data/chunk-000/episode_000000.parquet"
        );

        // Episode at chunk boundary
        let alloc = EpisodeAllocation::new(50000, episodes_per_chunk);
        assert_eq!(
            alloc.video_path("cam"),
            "videos/chunk-100/cam/episode_050000.mp4"
        );
        assert_eq!(
            alloc.parquet_path(),
            "data/chunk-100/episode_050000.parquet"
        );

        // Last episode (99,999)
        let alloc = EpisodeAllocation::new(99999, episodes_per_chunk);
        assert_eq!(
            alloc.video_path("cam"),
            "videos/chunk-199/cam/episode_099999.mp4"
        );
        assert_eq!(
            alloc.parquet_path(),
            "data/chunk-199/episode_099999.parquet"
        );
    }

    #[test]
    fn test_different_episodes_per_chunk_configs() {
        // Test various episodes_per_chunk configurations

        // 250 episodes per chunk = 400 chunks for 100K episodes
        let alloc = EpisodeAllocation::new(99999, 250);
        assert_eq!(alloc.chunk_index, 399);
        assert_eq!(alloc.chunk_offset, 249);

        // 1000 episodes per chunk = 100 chunks for 100K episodes
        let alloc = EpisodeAllocation::new(99999, 1000);
        assert_eq!(alloc.chunk_index, 99);
        assert_eq!(alloc.chunk_offset, 999);

        // 100 episodes per chunk = 1000 chunks for 100K episodes
        let alloc = EpisodeAllocation::new(99999, 100);
        assert_eq!(alloc.chunk_index, 999);
        assert_eq!(alloc.chunk_offset, 99);
    }

    #[test]
    fn test_chunk_index_no_overflow() {
        // Verify no overflow issues with large episode indices
        let large_indices = [
            99999u64,  // 100K - 1
            199999u64, // 200K - 1
            499999u64, // 500K - 1
            999999u64, // 1M - 1
        ];

        for episode_idx in large_indices {
            // Should not panic with overflow
            let alloc = EpisodeAllocation::new(episode_idx, 500);
            assert!(alloc.chunk_index < u32::MAX);
            assert!(alloc.chunk_offset < 500);
        }
    }
}
