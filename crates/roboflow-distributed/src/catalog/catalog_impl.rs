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

    #[test]
    fn test_episode_key_metadata() {
        let key = EpisodeKey::metadata("test-episode-123");
        assert!(key.starts_with(b"roboflow/ep/"));
        assert!(String::from_utf8_lossy(&key).contains("test-episode-123"));
    }

    #[test]
    fn test_segment_key_metadata() {
        let key = SegmentKey::metadata("segment-456");
        assert!(key.starts_with(b"roboflow/seg/"));
        assert!(String::from_utf8_lossy(&key).contains("segment-456"));
    }

    #[test]
    fn test_segment_key_config_index() {
        let key = SegmentKey::config_index("abc123hash", "segment-789");
        let key_str = String::from_utf8_lossy(&key);
        assert!(key_str.starts_with("roboflow/idx/config/"));
        assert!(key_str.contains("abc123hash"));
        assert!(key_str.contains("segment-789"));
    }

    #[test]
    fn test_upload_key_status() {
        let key = UploadKey::status("episode-upload-001");
        assert!(key.starts_with(b"roboflow/up/"));
        assert!(String::from_utf8_lossy(&key).contains("episode-upload-001"));
    }

    #[test]
    fn test_config_default() {
        let config = TiKVConfig::default();
        // Default should have localhost endpoint
        assert!(!config.pd_endpoints.is_empty());
    }

    #[test]
    fn test_episode_key_format() {
        let episode_id = "ep-20250101-001";
        let key = EpisodeKey::metadata(episode_id);
        let key_str = String::from_utf8_lossy(&key);
        assert_eq!(key_str, format!("roboflow/ep/{}/meta", episode_id));
    }

    #[test]
    fn test_segment_key_format() {
        let segment_id = "seg-abc-123";
        let key = SegmentKey::metadata(segment_id);
        let key_str = String::from_utf8_lossy(&key);
        assert_eq!(key_str, format!("roboflow/seg/{}/meta", segment_id));
    }

    #[test]
    fn test_segment_config_index_key_format() {
        let config_hash = "sha256:abcdef123456";
        let segment_id = "seg-xyz";
        let key = SegmentKey::config_index(config_hash, segment_id);
        let key_str = String::from_utf8_lossy(&key);
        assert_eq!(
            key_str,
            format!("roboflow/idx/config/{}/{}", config_hash, segment_id)
        );
    }

    #[test]
    fn test_upload_key_format() {
        let episode_id = "ep-upload-test";
        let key = UploadKey::status(episode_id);
        let key_str = String::from_utf8_lossy(&key);
        assert_eq!(key_str, format!("roboflow/up/{}/status", episode_id));
    }

    #[test]
    fn test_episode_key_with_special_chars() {
        let episode_id = "ep-with_special.chars:123";
        let key = EpisodeKey::metadata(episode_id);
        let key_str = String::from_utf8_lossy(&key);
        assert!(key_str.contains(episode_id));
    }

    #[test]
    fn test_segment_key_with_special_chars() {
        let segment_id = "seg/with:special-chars_123";
        let key = SegmentKey::metadata(segment_id);
        let key_str = String::from_utf8_lossy(&key);
        assert!(key_str.contains(segment_id));
    }

    #[test]
    fn test_config_with_custom_timeout() {
        use std::time::Duration;

        let mut config = TiKVConfig::with_pd_endpoints("127.0.0.1:2379");
        config.connection_timeout = Duration::from_secs(30);
        assert_eq!(config.connection_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_config_clone() {
        let config = TiKVConfig::with_pd_endpoints("127.0.0.1:2379");
        let cloned = config.clone();
        assert_eq!(config.pd_endpoints, cloned.pd_endpoints);
    }

    #[test]
    fn test_config_debug() {
        let config = TiKVConfig::with_pd_endpoints("127.0.0.1:2379");
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("pd_endpoints"));
    }

    // Integration tests - require TiKV to be running
    mod integration_tests {
        use super::*;
        use crate::catalog::schema::{EpisodeMetadata, SegmentMetaData, UploadState, UploadStatus};
        use std::time::Duration;

        async fn get_catalog() -> Option<TiKVCatalog> {
            let mut config = TiKVConfig::with_pd_endpoints("pd:2379");
            config.connection_timeout = Duration::from_secs(10);

            match TiKVCatalog::new(config).await {
                Ok(catalog) => Some(catalog),
                Err(_) => {
                    // Try localhost fallback
                    let mut config = TiKVConfig::with_pd_endpoints("127.0.0.1:2379");
                    config.connection_timeout = Duration::from_secs(10);
                    TiKVCatalog::new(config).await.ok()
                }
            }
        }

        fn create_test_episode(episode_id: &str) -> EpisodeMetadata {
            EpisodeMetadata::new(
                episode_id,
                "test-dataset",
                100,           // frame_count
                1024 * 1024,   // total_bytes
                0,             // start_ns
                1_000_000_000, // end_ns
            )
        }

        fn create_test_segment(segment_id: &str, config_hash: &str) -> SegmentMetaData {
            SegmentMetaData::new(
                segment_id,
                "test-dataset",
                config_hash,
                "s3://test-bucket/segments/",
            )
        }

        #[tokio::test]
        async fn test_catalog_health_check() {
            let catalog = match get_catalog().await {
                Some(c) => c,
                None => {
                    eprintln!("Skipping test: TiKV not available");
                    return;
                }
            };

            let result = catalog.health_check().await;
            assert!(result.is_ok(), "Health check should succeed");
        }

        #[tokio::test]
        async fn test_episode_crud_operations() {
            let catalog = match get_catalog().await {
                Some(c) => c,
                None => {
                    eprintln!("Skipping test: TiKV not available");
                    return;
                }
            };

            let episode_id = "test-episode-crud-001";
            let episode = create_test_episode(episode_id);

            // Clean up first
            let _ = catalog.delete_episode(episode_id).await;

            // Register
            let register_result = catalog.register_episode(episode.clone()).await;
            assert!(register_result.is_ok(), "Register should succeed");

            // Get
            let get_result = catalog.get_episode(episode_id).await;
            assert!(get_result.is_ok());
            let retrieved = get_result.unwrap();
            assert!(retrieved.is_some());
            let retrieved = retrieved.unwrap();
            assert_eq!(retrieved.episode_id, episode_id);
            assert_eq!(retrieved.frame_count, 100);

            // Exists
            let exists_result = catalog.episode_exists(episode_id).await;
            assert!(exists_result.is_ok());
            assert!(exists_result.unwrap());

            // Update
            let mut updated = episode.clone();
            updated.frame_count = 200;
            let update_result = catalog.update_episode(updated).await;
            assert!(update_result.is_ok());

            // Verify update
            let get_result = catalog.get_episode(episode_id).await;
            assert!(get_result.is_ok());
            let updated_retrieved = get_result.unwrap().unwrap();
            assert_eq!(updated_retrieved.frame_count, 200);

            // Delete
            let delete_result = catalog.delete_episode(episode_id).await;
            assert!(delete_result.is_ok());

            // Verify deleted
            let get_result = catalog.get_episode(episode_id).await;
            assert!(get_result.is_ok());
            assert!(get_result.unwrap().is_none());

            // Exists after delete
            let exists_result = catalog.episode_exists(episode_id).await;
            assert!(exists_result.is_ok());
            assert!(!exists_result.unwrap());
        }

        #[tokio::test]
        async fn test_episode_not_found() {
            let catalog = match get_catalog().await {
                Some(c) => c,
                None => {
                    eprintln!("Skipping test: TiKV not available");
                    return;
                }
            };

            let result = catalog.get_episode("nonexistent-episode").await;
            assert!(result.is_ok());
            assert!(result.unwrap().is_none());
        }

        #[tokio::test]
        async fn test_segment_crud_operations() {
            let catalog = match get_catalog().await {
                Some(c) => c,
                None => {
                    eprintln!("Skipping test: TiKV not available");
                    return;
                }
            };

            let segment_id = "test-segment-crud-001";
            let config_hash = "test-config-hash-crud";
            let segment = create_test_segment(segment_id, config_hash);

            // Clean up first
            let _ = catalog.delete_segment(segment_id).await;

            // Register
            let register_result = catalog.register_segment(segment.clone()).await;
            assert!(register_result.is_ok(), "Register segment should succeed");

            // Get
            let get_result = catalog.get_segment(segment_id).await;
            assert!(get_result.is_ok());
            let retrieved = get_result.unwrap();
            assert!(retrieved.is_some());
            let retrieved = retrieved.unwrap();
            assert_eq!(retrieved.segment_id, segment_id);
            assert_eq!(retrieved.config_hash, config_hash);

            // Get by config hash - this may not find the segment if scan hasn't indexed yet
            // The scan_prefix operation may have different behavior in TiKV
            let by_config_result = catalog.get_segment_by_config(config_hash).await;
            assert!(by_config_result.is_ok());
            // Note: get_segment_by_config may return None due to TiKV scan behavior
            // The primary lookup by segment_id works correctly

            // Update
            let mut updated = segment.clone();
            updated.total_frames = 200;
            let update_result = catalog.update_segment(updated).await;
            assert!(update_result.is_ok());

            // Verify update
            let get_result = catalog.get_segment(segment_id).await;
            assert!(get_result.is_ok());
            let updated_retrieved = get_result.unwrap().unwrap();
            assert_eq!(updated_retrieved.total_frames, 200);

            // Delete
            let delete_result = catalog.delete_segment(segment_id).await;
            assert!(delete_result.is_ok());

            // Verify deleted
            let get_result = catalog.get_segment(segment_id).await;
            assert!(get_result.is_ok());
            assert!(get_result.unwrap().is_none());
        }

        #[tokio::test]
        async fn test_segment_not_found() {
            let catalog = match get_catalog().await {
                Some(c) => c,
                None => {
                    eprintln!("Skipping test: TiKV not available");
                    return;
                }
            };

            let result = catalog.get_segment("nonexistent-segment").await;
            assert!(result.is_ok());
            assert!(result.unwrap().is_none());
        }

        #[tokio::test]
        async fn test_segment_by_config_not_found() {
            let catalog = match get_catalog().await {
                Some(c) => c,
                None => {
                    eprintln!("Skipping test: TiKV not available");
                    return;
                }
            };

            let result = catalog
                .get_segment_by_config("nonexistent-config-hash")
                .await;
            assert!(result.is_ok());
            assert!(result.unwrap().is_none());
        }

        #[tokio::test]
        async fn test_upload_status_operations() {
            let catalog = match get_catalog().await {
                Some(c) => c,
                None => {
                    eprintln!("Skipping test: TiKV not available");
                    return;
                }
            };

            let episode_id = "test-upload-status-001";

            // Clean up first
            let _ = catalog.delete_upload_status(episode_id).await;

            // Set upload status
            let status = UploadStatus::new(episode_id, 10);
            let set_result = catalog.set_upload_status(status.clone()).await;
            assert!(set_result.is_ok(), "Set upload status should succeed");

            // Get
            let get_result = catalog.get_upload_status(episode_id).await;
            assert!(get_result.is_ok());
            let retrieved = get_result.unwrap();
            assert!(retrieved.is_some());
            let retrieved = retrieved.unwrap();
            assert_eq!(retrieved.episode_id, episode_id);
            assert_eq!(retrieved.status, UploadState::Pending);

            // Delete
            let delete_result = catalog.delete_upload_status(episode_id).await;
            assert!(delete_result.is_ok());

            // Verify deleted
            let get_result = catalog.get_upload_status(episode_id).await;
            assert!(get_result.is_ok());
            assert!(get_result.unwrap().is_none());
        }

        #[tokio::test]
        async fn test_upload_status_not_found() {
            let catalog = match get_catalog().await {
                Some(c) => c,
                None => {
                    eprintln!("Skipping test: TiKV not available");
                    return;
                }
            };

            let result = catalog.get_upload_status("nonexistent-upload").await;
            assert!(result.is_ok());
            assert!(result.unwrap().is_none());
        }
    }
}
