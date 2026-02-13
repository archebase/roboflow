// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Episode index allocation for distributed LeRobot dataset generation.
//!
//! This module provides the infrastructure for allocating unique episode indices
//! across distributed workers converting bag/MCAP files to LeRobot format.
//!
//! # Key Concepts
//!
//! - **Episode Index**: Global unique identifier for an episode (0 to 99,999 for 100K datasets)
//! - **Chunk Index**: Container for multiple episodes (500 episodes per chunk → 200 chunks)
//! - **Atomic Allocation**: Uses TiKV CAS operations to ensure no duplicate indices
//!
//! # Components
//!
//! - [`EpisodeAllocator`] - Trait for episode index allocation
//! - [`TiKVEpisodeAllocator`] - Production implementation using TiKV
//! - [`LocalEpisodeAllocator`] - In-memory implementation for testing
//! - [`EpisodeAllocation`] - Result containing episode and chunk indices
//!
//! # Example
//!
//! ```ignore
//! use roboflow_distributed::episode::{TiKVEpisodeAllocator, EpisodeAllocator, EpisodeAllocation};
//! use std::sync::Arc;
//!
//! async fn example(client: Arc<TikvClient>) {
//!     // Create allocator for a batch with 500 episodes per chunk
//!     let allocator = TiKVEpisodeAllocator::new(
//!         client,
//!         "batch-123".to_string(),
//!         500, // episodes_per_chunk
//!     );
//!
//!     // Allocate a single episode
//!     let allocation = allocator.allocate().await.unwrap();
//!     println!("Episode {} in chunk {}", allocation.episode_index, allocation.chunk_index);
//!
//!     // Get paths for video and parquet files
//!     let video_path = allocation.video_path("camera_left");
//!     let parquet_path = allocation.parquet_path();
//! }
//! ```

mod allocator;

pub use allocator::{
    EpisodeAllocation, EpisodeAllocator, EpisodeAllocatorError, LocalEpisodeAllocator, Result,
    TiKVEpisodeAllocator, chunk_index_from_episode,
};
