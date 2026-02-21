// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt;
use std::sync::RwLock;

use super::collector::StatsCollector;
use super::types::{BatchStatsSummary, EpisodeStats};

/// In-memory implementation of `StatsCollector` for testing.
///
/// This implementation stores all statistics in memory using a `HashMap`,
/// making it suitable for unit tests and local development without requiring
/// a TiKV cluster.
///
/// # Example
///
/// ```ignore
/// use roboflow_distributed::stats::{StatsCollector, InMemoryStatsCollector};
///
/// let collector = InMemoryStatsCollector::new();
///
/// // Record some episode stats
/// collector.record_episode_stats("batch-1", EpisodeStats::new(0, 100)).await?;
/// collector.record_episode_stats("batch-1", EpisodeStats::new(1, 200)).await?;
///
/// // Get aggregated stats
/// let mut summary = collector.get_batch_stats("batch-1").await?.unwrap();
/// summary.calculate_global_stats();
///
/// assert_eq!(summary.total_episodes, 2);
/// assert_eq!(summary.total_frames, 300);
/// ```
pub struct InMemoryStatsCollector {
    stats: RwLock<HashMap<String, BatchStatsSummary>>,
}

impl InMemoryStatsCollector {
    /// Create a new empty stats collector.
    pub fn new() -> Self {
        Self {
            stats: RwLock::new(HashMap::new()),
        }
    }

    /// Get all recorded operations for inspection.
    pub fn get_all_stats(&self) -> HashMap<String, BatchStatsSummary> {
        self.stats.read().unwrap().clone()
    }

    /// Clear all recorded stats.
    pub fn clear(&self) {
        self.stats.write().unwrap().clear();
    }

    /// Get the number of batches tracked.
    pub fn batch_count(&self) -> usize {
        self.stats.read().unwrap().len()
    }
}

impl Default for InMemoryStatsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for InMemoryStatsCollector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryStatsCollector")
            .field("batch_count", &self.batch_count())
            .finish()
    }
}

#[async_trait]
impl StatsCollector for InMemoryStatsCollector {
    async fn record_episode_stats(&self, batch_id: &str, stats: EpisodeStats) -> crate::Result<()> {
        let mut all_stats = self.stats.write().unwrap();
        let batch = all_stats
            .entry(batch_id.to_string())
            .or_insert_with(|| BatchStatsSummary::new(batch_id.to_string()));
        batch.add_episode(stats);
        Ok(())
    }

    async fn get_batch_stats(&self, batch_id: &str) -> crate::Result<Option<BatchStatsSummary>> {
        Ok(self.stats.read().unwrap().get(batch_id).cloned())
    }

    async fn delete_batch_stats(&self, batch_id: &str) -> crate::Result<()> {
        self.stats.write().unwrap().remove(batch_id);
        Ok(())
    }

    async fn is_healthy(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_record_and_retrieve_stats() {
        let collector = InMemoryStatsCollector::new();

        // Record some stats
        let ep1 = EpisodeStats::new(0, 100);
        let ep2 = EpisodeStats::new(1, 200);

        collector
            .record_episode_stats("batch-1", ep1)
            .await
            .unwrap();
        collector
            .record_episode_stats("batch-1", ep2)
            .await
            .unwrap();

        // Retrieve and verify
        let summary = collector.get_batch_stats("batch-1").await.unwrap().unwrap();
        assert_eq!(summary.total_episodes, 2);
        assert_eq!(summary.total_frames, 300);
    }

    #[tokio::test]
    async fn test_delete_batch_stats() {
        let collector = InMemoryStatsCollector::new();

        collector
            .record_episode_stats("batch-1", EpisodeStats::new(0, 100))
            .await
            .unwrap();
        assert!(
            collector
                .get_batch_stats("batch-1")
                .await
                .unwrap()
                .is_some()
        );

        collector.delete_batch_stats("batch-1").await.unwrap();
        assert!(
            collector
                .get_batch_stats("batch-1")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_is_healthy() {
        let collector = InMemoryStatsCollector::new();
        assert!(collector.is_healthy().await);
    }

    #[tokio::test]
    async fn test_multiple_batches() {
        let collector = InMemoryStatsCollector::new();

        collector
            .record_episode_stats("batch-1", EpisodeStats::new(0, 100))
            .await
            .unwrap();
        collector
            .record_episode_stats("batch-2", EpisodeStats::new(0, 200))
            .await
            .unwrap();

        assert_eq!(collector.batch_count(), 2);

        let batch1 = collector.get_batch_stats("batch-1").await.unwrap().unwrap();
        let batch2 = collector.get_batch_stats("batch-2").await.unwrap().unwrap();

        assert_eq!(batch1.total_frames, 100);
        assert_eq!(batch2.total_frames, 200);
    }

    #[tokio::test]
    async fn test_clear() {
        let collector = InMemoryStatsCollector::new();

        collector
            .record_episode_stats("batch-1", EpisodeStats::new(0, 100))
            .await
            .unwrap();
        assert_eq!(collector.batch_count(), 1);

        collector.clear();
        assert_eq!(collector.batch_count(), 0);
    }
}
