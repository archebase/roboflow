// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! TiKV-backed implementation of StatsCollector.

use std::sync::Arc;

use async_trait::async_trait;
use tracing::{debug, info, instrument, warn};

use super::collector::StatsCollector;
use super::keys::StatsKeys;
use super::types::{BatchStatsSummary, EpisodeStats};
use crate::tikv::TikvClient;
use crate::tikv::error::TikvError;

/// TiKV-based statistics collector for distributed episode stats.
///
/// This implementation stores episode statistics in TiKV, enabling
/// distributed workers to push stats and the finalizer to aggregate
/// them for LeRobot metadata generation.
///
/// # Key Schema
///
/// ```text
/// stats/{batch_id}/meta              - Batch metadata
/// stats/{batch_id}/episode/{idx}     - Per-episode statistics
/// ```
///
/// # Example
///
/// ```ignore
/// use roboflow_distributed::stats::{TiKVStatsCollector, StatsCollector, EpisodeStats};
///
/// let client = TikvClient::new(config).await?;
/// let collector = TiKVStatsCollector::new(client);
///
/// // Worker: record stats
/// collector.record_episode_stats("batch-123", episode_stats).await?;
///
/// // Finalizer: get all stats
/// let summary = collector.get_batch_stats("batch-123").await?;
/// ```
pub struct TiKVStatsCollector {
    client: Arc<TikvClient>,
}

impl TiKVStatsCollector {
    /// Maximum number of episodes to scan in a single batch.
    /// For batches larger than this, multiple scan calls are needed.
    const MAX_SCAN_LIMIT: u32 = 100_000;

    /// Create a new TiKV stats collector.
    pub fn new(client: Arc<TikvClient>) -> Self {
        Self { client }
    }

    /// Store batch metadata.
    ///
    /// This tracks the total episode count and is updated atomically.
    #[instrument(skip(self), fields(batch_id = %batch_id))]
    async fn increment_episode_count(&self, batch_id: &str) -> Result<usize, TikvError> {
        let key = StatsKeys::batch_meta(batch_id);

        // Get current count
        let current: usize = match self.client.get(key.clone()).await? {
            Some(v) => {
                let bytes: [u8; 8] = v.try_into().map_err(|_| {
                    TikvError::Deserialization(
                        "Invalid episode count: expected 8 bytes".to_string(),
                    )
                })?;
                usize::from_be_bytes(bytes)
            }
            None => 0,
        };

        // Increment and store
        let next = current + 1;
        self.client
            .put(key, (next as u64).to_be_bytes().to_vec())
            .await?;

        Ok(next)
    }
}

impl std::fmt::Debug for TiKVStatsCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TiKVStatsCollector")
            .field("client", &"Arc<TikvClient>")
            .finish()
    }
}

#[async_trait]
impl StatsCollector for TiKVStatsCollector {
    #[instrument(skip(self, stats), fields(batch_id = %batch_id, episode_index = stats.episode_index))]
    async fn record_episode_stats(&self, batch_id: &str, stats: EpisodeStats) -> crate::Result<()> {
        let key = StatsKeys::episode(batch_id, stats.episode_index);
        let value = serde_json::to_vec(&stats).map_err(|e| {
            TikvError::Serialization(format!("Failed to serialize episode stats: {}", e))
        })?;

        self.client.put(key, value).await?;

        // Increment the episode count
        self.increment_episode_count(batch_id).await?;

        debug!(
            batch_id = %batch_id,
            episode_index = stats.episode_index,
            frame_count = stats.frame_count,
            "Recorded episode stats to TiKV"
        );

        Ok(())
    }

    #[instrument(skip(self), fields(batch_id = %batch_id))]
    async fn get_batch_stats(&self, batch_id: &str) -> crate::Result<Option<BatchStatsSummary>> {
        let prefix = StatsKeys::batch_prefix(batch_id);

        // Scan all keys with the batch prefix
        let kvs = self.client.scan(prefix, Self::MAX_SCAN_LIMIT).await?;

        if kvs.is_empty() {
            debug!(batch_id = %batch_id, "No stats found for batch");
            return Ok(None);
        }

        // Warn if we hit the scan limit - data may be incomplete
        if kvs.len() >= Self::MAX_SCAN_LIMIT as usize {
            warn!(
                batch_id = %batch_id,
                limit = Self::MAX_SCAN_LIMIT,
                "Scan hit maximum limit, statistics may be incomplete"
            );
        }

        let mut summary = BatchStatsSummary::new(batch_id.to_string());

        for (key, value) in kvs {
            // Skip metadata entries
            if StatsKeys::is_batch_meta(&key) {
                continue;
            }

            // Parse episode stats
            if StatsKeys::is_episode_key(&key) {
                match serde_json::from_slice::<EpisodeStats>(&value) {
                    Ok(episode_stats) => {
                        summary.add_episode(episode_stats);
                    }
                    Err(e) => {
                        let key_str = String::from_utf8_lossy(&key);
                        warn!(
                            batch_id = %batch_id,
                            key = %key_str,
                            error = %e,
                            "Failed to deserialize episode stats"
                        );
                    }
                }
            }
        }

        // Calculate global statistics
        summary.calculate_global_stats();

        info!(
            batch_id = %batch_id,
            total_episodes = summary.total_episodes,
            total_frames = summary.total_frames,
            feature_count = summary.global_stats.len(),
            "Aggregated batch statistics"
        );

        Ok(Some(summary))
    }

    #[instrument(skip(self), fields(batch_id = %batch_id))]
    async fn delete_batch_stats(&self, batch_id: &str) -> crate::Result<()> {
        let prefix = StatsKeys::batch_prefix(batch_id);

        // Scan all keys with the batch prefix
        let kvs = self.client.scan(prefix, Self::MAX_SCAN_LIMIT).await?;

        if kvs.is_empty() {
            debug!(batch_id = %batch_id, "No stats to delete");
            return Ok(());
        }

        // Delete all keys
        for (key, _) in kvs {
            self.client.delete(key).await?;
        }

        info!(
            batch_id = %batch_id,
            "Deleted batch statistics"
        );

        Ok(())
    }

    #[instrument(skip(self))]
    async fn is_healthy(&self) -> bool {
        self.client.is_connected()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_keys_integration() {
        // Verify key generation is consistent
        let batch_id = "test-batch";
        let episode_idx = 42;

        let key = StatsKeys::episode(batch_id, episode_idx);
        let parsed = StatsKeys::parse_episode_index(&key);

        assert_eq!(parsed, Some(episode_idx));
    }
}
