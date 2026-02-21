// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! StatsCollector trait for pluggable statistics backends.

use async_trait::async_trait;
use std::fmt::Debug;

use super::types::{BatchStatsSummary, EpisodeStats};

/// Trait for collecting and aggregating episode statistics.
///
/// This trait abstracts the storage backend for statistics collection,
/// enabling different implementations (TiKV, in-memory, etc.) while
/// maintaining a consistent interface for workers and finalizers.
///
/// # Implementation Notes
///
/// - Implementations must be thread-safe (`Send + Sync`)
/// - All methods are async to support networked backends
/// - Keys are scoped by batch_id to support concurrent batch processing
///
/// # Example
///
/// ```ignore
/// use roboflow_distributed::stats::{StatsCollector, TiKVStatsCollector, EpisodeStats};
///
/// // In worker: record stats after processing episode
/// let collector = TiKVStatsCollector::new(tikv_client);
/// collector.record_episode_stats("batch-123", episode_stats).await?;
///
/// // In finalizer: aggregate all stats
/// let summary = collector.get_batch_stats("batch-123").await?;
/// ```
#[async_trait]
pub trait StatsCollector: Debug + Send + Sync {
    /// Record statistics for a single episode.
    ///
    /// This is called by workers after processing each bag/MCAP file.
    /// The implementation should store the stats in a way that allows
    /// later aggregation by `get_batch_stats`.
    ///
    /// # Arguments
    ///
    /// * `batch_id` - Unique identifier for the processing batch
    /// * `stats` - Episode statistics to record
    ///
    /// # Errors
    ///
    /// Returns an error if the storage backend is unavailable or
    /// if the write fails.
    async fn record_episode_stats(&self, batch_id: &str, stats: EpisodeStats) -> crate::Result<()>;

    /// Retrieve aggregated statistics for a batch.
    ///
    /// This is called by the finalizer after all workers have completed.
    /// The implementation should:
    /// 1. Fetch all episode stats for the batch
    /// 2. Aggregate them into a single BatchStatsSummary
    /// 3. Calculate global statistics across all episodes
    ///
    /// # Arguments
    ///
    /// * `batch_id` - Unique identifier for the processing batch
    ///
    /// # Returns
    ///
    /// - `Some(BatchStatsSummary)` if the batch exists with stats
    /// - `None` if no stats were recorded for this batch
    ///
    /// # Errors
    ///
    /// Returns an error if the storage backend is unavailable.
    async fn get_batch_stats(&self, batch_id: &str) -> crate::Result<Option<BatchStatsSummary>>;

    /// Delete all statistics for a batch.
    ///
    /// This is called after the finalizer has successfully written
    /// the metadata to the dataset. It cleans up temporary stats data.
    ///
    /// # Arguments
    ///
    /// * `batch_id` - Unique identifier for the processing batch
    ///
    /// # Errors
    ///
    /// Returns an error if the storage backend is unavailable or
    /// if the deletion fails.
    async fn delete_batch_stats(&self, batch_id: &str) -> crate::Result<()>;

    /// Check if stats collection is healthy.
    ///
    /// This can be used for health checks and monitoring.
    /// Implementations should verify connectivity to the backend.
    ///
    /// # Returns
    ///
    /// `true` if the backend is healthy and ready to accept stats,
    /// `false` otherwise.
    async fn is_healthy(&self) -> bool;
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_trait_bounds() {
        // Verify that any StatsCollector impl must be Send + Sync
        fn assert_send_sync<T: Send + Sync>() {}
        // This test exists to ensure the trait bounds are correct
        assert_send_sync::<fn()>();
    }
}
