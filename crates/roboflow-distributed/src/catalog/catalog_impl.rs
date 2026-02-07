// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Main catalog implementation.

use std::sync::Arc;

use crate::{
    catalog::config::TiKVConfig,
    catalog::key::{EpisodeKey, SegmentKey, UploadKey},
    catalog::pool::TiKVPool,
    catalog::schema::{EpisodeMetadata, SegmentMetaData, UploadStatus},
};

/// TiKV-based catalog for distributed metadata storage.
///
/// Provides methods for storing and retrieving episode and segment metadata,
/// as well as tracking upload progress.
pub struct TiKVCatalog {
    /// TiKV connection pool.
    pool: Arc<TiKVPool>,
}

impl TiKVCatalog {
    /// Create a new TiKV catalog with the given configuration.
    pub async fn new(config: TiKVConfig) -> Result<Self, roboflow_core::RoboflowError> {
        let pool = Arc::new(TiKVPool::new(config).await?);
        Ok(Self { pool })
    }

    /// Get a reference to the underlying TiKV pool.
    pub fn pool(&self) -> &Arc<TiKVPool> {
        &self.pool
    }

    // ============================================
    // Episode Operations
    // ============================================

    /// Register episode metadata in the catalog.
    pub async fn register_episode(
        &self,
        episode: EpisodeMetadata,
    ) -> Result<(), roboflow_core::RoboflowError> {
        let key = EpisodeKey::metadata(&episode.episode_id);
        let value = episode.encode()?;

        self.pool.put(key, value).await?;

        tracing::debug!("Registered episode: {}", episode.episode_id);
        Ok(())
    }

    /// Get episode metadata by ID.
    pub async fn get_episode(
        &self,
        episode_id: &str,
    ) -> Result<Option<EpisodeMetadata>, roboflow_core::RoboflowError> {
        let key = EpisodeKey::metadata(episode_id);
        match self.pool.get(key).await? {
            Some(value) => {
                let episode = EpisodeMetadata::decode(&value)?;
                Ok(Some(episode))
            }
            None => Ok(None),
        }
    }

    /// Check if an episode exists.
    pub async fn episode_exists(
        &self,
        episode_id: &str,
    ) -> Result<bool, roboflow_core::RoboflowError> {
        let key = EpisodeKey::metadata(episode_id);
        Ok(self.pool.get(key).await?.is_some())
    }

    /// Update episode metadata.
    pub async fn update_episode(
        &self,
        episode: EpisodeMetadata,
    ) -> Result<(), roboflow_core::RoboflowError> {
        let key = EpisodeKey::metadata(&episode.episode_id);
        let value = episode.encode()?;

        self.pool.put(key, value).await?;

        tracing::debug!("Updated episode: {}", episode.episode_id);
        Ok(())
    }

    /// Delete episode metadata.
    pub async fn delete_episode(
        &self,
        episode_id: &str,
    ) -> Result<(), roboflow_core::RoboflowError> {
        let key = EpisodeKey::metadata(episode_id);
        self.pool.delete(key).await?;

        tracing::debug!("Deleted episode: {}", episode_id);
        Ok(())
    }

    // ============================================
    // Segment Operations
    // ============================================

    /// Register segment metadata in the catalog.
    pub async fn register_segment(
        &self,
        segment: SegmentMetaData,
    ) -> Result<(), roboflow_core::RoboflowError> {
        let key = SegmentKey::metadata(&segment.segment_id);
        let value = segment.encode()?;

        // Also create config hash index
        let index_key = SegmentKey::config_index(&segment.config_hash, &segment.segment_id);

        // Atomic batch write
        self.pool
            .batch_put(vec![(key, value), (index_key, vec![])])
            .await?;

        tracing::debug!("Registered segment: {}", segment.segment_id);
        Ok(())
    }

    /// Get segment metadata by ID.
    pub async fn get_segment(
        &self,
        segment_id: &str,
    ) -> Result<Option<SegmentMetaData>, roboflow_core::RoboflowError> {
        let key = SegmentKey::metadata(segment_id);
        match self.pool.get(key).await? {
            Some(value) => {
                let segment = SegmentMetaData::decode(&value)?;
                Ok(Some(segment))
            }
            None => Ok(None),
        }
    }

    /// Get a segment by config hash.
    pub async fn get_segment_by_config(
        &self,
        config_hash: &str,
    ) -> Result<Option<SegmentMetaData>, roboflow_core::RoboflowError> {
        // Scan config index for this hash
        let prefix = format!("roboflow/idx/config/{}/", config_hash).into_bytes();
        let keys = self.pool.scan_prefix(prefix, 100).await?;

        for key_bytes in keys {
            let key_str = String::from_utf8_lossy(&key_bytes);
            // Key format: roboflow/idx/config/{hash}/{segment_id}
            #[expect(clippy::collapsible_if)]
            if let Some(segment_id) = key_str.split('/').nth(5) {
                if let Some(segment) = self.get_segment(segment_id).await? {
                    return Ok(Some(segment));
                }
            }
        }

        Ok(None)
    }

    /// Update segment metadata.
    pub async fn update_segment(
        &self,
        segment: SegmentMetaData,
    ) -> Result<(), roboflow_core::RoboflowError> {
        let key = SegmentKey::metadata(&segment.segment_id);
        let value = segment.encode()?;

        self.pool.put(key, value).await?;

        tracing::debug!("Updated segment: {}", segment.segment_id);
        Ok(())
    }

    /// Delete segment metadata.
    pub async fn delete_segment(
        &self,
        segment_id: &str,
    ) -> Result<(), roboflow_core::RoboflowError> {
        // First get segment for index cleanup
        if let Some(segment) = self.get_segment(segment_id).await? {
            let key = SegmentKey::metadata(segment_id);
            let index_key = SegmentKey::config_index(&segment.config_hash, segment_id);

            // Delete both (not atomic, but acceptable for cleanup)
            self.pool.delete(index_key).await?;
            self.pool.delete(key).await?;

            tracing::debug!("Deleted segment: {}", segment_id);
        }

        Ok(())
    }

    // ============================================
    // Upload Status Operations
    // ============================================

    /// Create or update upload status.
    pub async fn set_upload_status(
        &self,
        status: UploadStatus,
    ) -> Result<(), roboflow_core::RoboflowError> {
        let key = UploadKey::status(&status.episode_id);
        let value = status.encode()?;

        self.pool.put(key, value).await?;

        tracing::trace!("Set upload status: {:?}", status.episode_id);
        Ok(())
    }

    /// Get upload status for an episode.
    pub async fn get_upload_status(
        &self,
        episode_id: &str,
    ) -> Result<Option<UploadStatus>, roboflow_core::RoboflowError> {
        let key = UploadKey::status(episode_id);
        match self.pool.get(key).await? {
            Some(value) => {
                let status = UploadStatus::decode(&value)?;
                Ok(Some(status))
            }
            None => Ok(None),
        }
    }

    /// Delete upload status.
    pub async fn delete_upload_status(
        &self,
        episode_id: &str,
    ) -> Result<(), roboflow_core::RoboflowError> {
        let key = UploadKey::status(episode_id);
        self.pool.delete(key).await?;

        tracing::trace!("Deleted upload status: {}", episode_id);
        Ok(())
    }

    // ============================================
    // Health Check
    // ============================================

    /// Check if the catalog is healthy.
    pub async fn health_check(&self) -> Result<(), roboflow_core::RoboflowError> {
        self.pool.ping().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalog_config() {
        let config = TiKVConfig::with_pd_endpoints("127.0.0.1:2379");
        assert_eq!(config.pd_endpoints, vec!["127.0.0.1:2379"]);
    }
}
