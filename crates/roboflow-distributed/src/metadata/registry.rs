// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Dataset metadata registry backed by TiKV.

use std::collections::HashMap;
use std::sync::Arc;

use super::keys::MetadataKeys;
use super::types::{FeatureSpec, PartialEpisodeMetadata, TaskEntry};
use crate::tikv::{TikvClient, TikvError};

/// Global metadata registry backed by TiKV.
///
/// This provides distributed coordination for:
/// - Task deduplication (global task → index mapping)
/// - Feature unification (consistent specs across episodes)
/// - Partial episode metadata storage
///
/// # Usage
///
/// Workers use this registry during conversion:
/// 1. Register tasks (gets global index)
/// 2. Register features (validates consistency)
/// 3. Store episode metadata after conversion
///
/// The finalizer then aggregates all metadata from TiKV.
#[derive(Clone)]
pub struct DatasetMetadataRegistry {
    tikv: Arc<TikvClient>,
    batch_id: String,
}

impl DatasetMetadataRegistry {
    /// Create a new metadata registry.
    pub fn new(tikv: Arc<TikvClient>, batch_id: impl Into<String>) -> Self {
        Self {
            tikv,
            batch_id: batch_id.into(),
        }
    }

    /// Hash a task description for content-addressing.
    fn hash_task(task: &str) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        task.hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    }

    /// Get the batch ID.
    pub fn batch_id(&self) -> &str {
        &self.batch_id
    }

    /// Register or get existing task index (global deduplication).
    ///
    /// Tasks are content-addressed using a hash of the task description.
    /// This enables automatic deduplication across workers.
    ///
    /// # Arguments
    /// * `task` - Task description text
    ///
    /// # Returns
    /// Global task index (0, 1, 2, ...)
    pub async fn register_task(&self, task: &str) -> Result<usize, TikvError> {
        let task_hash = Self::hash_task(task);
        let key = MetadataKeys::task(&self.batch_id, &task_hash);

        // Try to get existing
        if let Some(data) = self.tikv.get(key.clone()).await? {
            let entry: TaskEntry = bincode::deserialize(&data)
                .map_err(|e| TikvError::Deserialization(e.to_string()))?;
            return Ok(entry.task_index);
        }

        // Allocate new index via CAS on counter
        let counter_key = MetadataKeys::task_counter(&self.batch_id);
        loop {
            let counter_data = self.tikv.get(counter_key.clone()).await?;
            let current: u64 = match counter_data {
                Some(d) => {
                    let (idx, _): (u64, u64) = bincode::deserialize(&d)
                        .map_err(|e| TikvError::Deserialization(e.to_string()))?;
                    idx
                }
                None => 0,
            };

            let new_index = current;
            let entry = TaskEntry {
                task_index: new_index as usize,
                task: task.to_string(),
            };

            // Atomic transaction: update counter AND store task
            let mut txn = self.tikv.begin_optimistic().await?;
            txn.put(
                counter_key.clone(),
                bincode::serialize(&(current + 1, 0u64))
                    .map_err(|e| TikvError::Serialization(e.to_string()))?,
            )
            .await
            .map_err(|e| TikvError::ClientError(e.to_string()))?;
            txn.put(
                key.clone(),
                bincode::serialize(&entry).map_err(|e| TikvError::Serialization(e.to_string()))?,
            )
            .await
            .map_err(|e| TikvError::ClientError(e.to_string()))?;

            match txn.commit().await {
                Ok(_) => return Ok(new_index as usize),
                Err(_) => continue, // Retry on conflict
            }
        }
    }

    /// Register multiple tasks in batch.
    ///
    /// More efficient than individual registrations for multiple tasks.
    pub async fn register_tasks(&self, tasks: &[String]) -> Result<Vec<usize>, TikvError> {
        let mut indices = Vec::with_capacity(tasks.len());
        for task in tasks {
            indices.push(self.register_task(task).await?);
        }
        Ok(indices)
    }

    /// Register feature spec (with validation).
    ///
    /// If a feature with this name already exists, validates that the
    /// new spec is compatible. Returns an error if specs conflict.
    ///
    /// # Arguments
    /// * `name` - Feature name (e.g., "observation.state")
    /// * `spec` - Feature specification
    pub async fn register_feature(&self, name: &str, spec: FeatureSpec) -> Result<(), TikvError> {
        let key = MetadataKeys::feature(&self.batch_id, name);

        // Check existing spec
        if let Some(data) = self.tikv.get(key.clone()).await? {
            let existing: FeatureSpec = bincode::deserialize(&data)
                .map_err(|e| TikvError::Deserialization(e.to_string()))?;

            if !existing.is_compatible(&spec) {
                return Err(TikvError::Other(format!(
                    "Feature '{}' spec mismatch: existing {:?} vs new {:?}",
                    name, existing, spec
                )));
            }
            // Existing spec is compatible, no need to update
            return Ok(());
        }

        // Store new spec
        let data =
            bincode::serialize(&spec).map_err(|e| TikvError::Serialization(e.to_string()))?;
        self.tikv.put(key, data).await?;
        Ok(())
    }

    /// Get feature spec if it exists.
    pub async fn get_feature(&self, name: &str) -> Result<Option<FeatureSpec>, TikvError> {
        let key = MetadataKeys::feature(&self.batch_id, name);

        match self.tikv.get(key).await? {
            Some(data) => {
                let spec: FeatureSpec = bincode::deserialize(&data)
                    .map_err(|e| TikvError::Deserialization(e.to_string()))?;
                Ok(Some(spec))
            }
            None => Ok(None),
        }
    }

    /// Store partial episode metadata.
    ///
    /// Workers call this after converting a bag file.
    ///
    /// # Arguments
    /// * `metadata` - Episode metadata to store
    pub async fn store_episode_metadata(
        &self,
        metadata: &PartialEpisodeMetadata,
    ) -> Result<(), TikvError> {
        let key = MetadataKeys::episode_metadata(&self.batch_id, metadata.episode_index);

        let data =
            bincode::serialize(metadata).map_err(|e| TikvError::Serialization(e.to_string()))?;

        self.tikv.put(key, data).await?;

        tracing::debug!(
            batch_id = %self.batch_id,
            episode_index = metadata.episode_index,
            "Stored episode metadata"
        );

        Ok(())
    }

    /// Get episode metadata by index.
    pub async fn get_episode_metadata(
        &self,
        episode_index: usize,
    ) -> Result<Option<PartialEpisodeMetadata>, TikvError> {
        let key = MetadataKeys::episode_metadata(&self.batch_id, episode_index);

        match self.tikv.get(key).await? {
            Some(data) => {
                let metadata: PartialEpisodeMetadata = bincode::deserialize(&data)
                    .map_err(|e| TikvError::Deserialization(e.to_string()))?;
                Ok(Some(metadata))
            }
            None => Ok(None),
        }
    }

    /// Scan all episode metadata.
    ///
    /// Returns all episodes sorted by index.
    /// Limited to 100,000 episodes per scan.
    pub async fn scan_episode_metadata(&self) -> Result<Vec<PartialEpisodeMetadata>, TikvError> {
        let prefix = MetadataKeys::episode_metadata_prefix(&self.batch_id);

        let pairs = self.tikv.scan(prefix, 100_000).await?;

        let mut episodes: Vec<PartialEpisodeMetadata> = pairs
            .into_iter()
            .filter_map(|(_, value)| bincode::deserialize(&value).ok())
            .collect();

        // Sort by episode index
        episodes.sort_by_key(|e| e.episode_index);

        Ok(episodes)
    }

    /// Scan all tasks.
    ///
    /// Returns tasks sorted by index.
    pub async fn scan_tasks(&self) -> Result<Vec<TaskEntry>, TikvError> {
        let prefix = MetadataKeys::task_prefix(&self.batch_id);

        let pairs = self.tikv.scan(prefix, 10_000).await?;

        let mut tasks: Vec<TaskEntry> = pairs
            .into_iter()
            .filter_map(|(_, value)| bincode::deserialize(&value).ok())
            .collect();

        // Sort by task index
        tasks.sort_by_key(|t| t.task_index);

        Ok(tasks)
    }

    /// Scan all feature specs.
    pub async fn scan_features(&self) -> Result<HashMap<String, FeatureSpec>, TikvError> {
        let prefix = MetadataKeys::feature_prefix(&self.batch_id);

        let pairs = self.tikv.scan(prefix, 1_000).await?;

        let mut features = HashMap::new();
        for (key, value) in pairs {
            let spec: FeatureSpec = bincode::deserialize(&value)
                .map_err(|e| TikvError::Deserialization(e.to_string()))?;

            // Extract feature name from key
            let key_str = String::from_utf8_lossy(&key);
            if let Some(name) = key_str.rsplit('/').next() {
                features.insert(name.to_string(), spec);
            }
        }

        Ok(features)
    }

    /// Delete all metadata for this batch.
    ///
    /// Call after successful finalization to clean up.
    pub async fn cleanup(&self) -> Result<(), TikvError> {
        // Scan and delete all metadata keys
        let prefixes = [
            MetadataKeys::task_prefix(&self.batch_id),
            MetadataKeys::feature_prefix(&self.batch_id),
            MetadataKeys::episode_metadata_prefix(&self.batch_id),
        ];

        for prefix in &prefixes {
            let pairs = self.tikv.scan(prefix.clone(), 100_000).await?;
            for (key, _) in pairs {
                self.tikv.delete(key).await?;
            }
        }

        // Delete counters
        let task_counter = MetadataKeys::task_counter(&self.batch_id);
        self.tikv.delete(task_counter).await?;

        tracing::info!(batch_id = %self.batch_id, "Cleaned up metadata registry");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::types::{FeatureShape, VideoInfo};
    use crate::stats::FeatureStats;

    #[test]
    fn test_task_hash_consistency() {
        let task1 = "pick up the red block";
        let task2 = "pick up the red block";
        let task3 = "pick up the blue block";

        let hash1 = DatasetMetadataRegistry::hash_task(task1);
        let hash2 = DatasetMetadataRegistry::hash_task(task2);
        let hash3 = DatasetMetadataRegistry::hash_task(task3);

        assert_eq!(hash1, hash2); // Same task = same hash
        assert_ne!(hash1, hash3); // Different task = different hash
    }

    #[test]
    fn test_task_hash_deterministic() {
        // Hash should be deterministic across multiple calls
        let task = "grasp the cylindrical object from the table";
        let hash1 = DatasetMetadataRegistry::hash_task(task);
        let hash2 = DatasetMetadataRegistry::hash_task(task);
        let hash3 = DatasetMetadataRegistry::hash_task(task);

        assert_eq!(hash1, hash2);
        assert_eq!(hash2, hash3);
    }

    #[test]
    fn test_task_hash_different_inputs() {
        // Even small changes should produce different hashes
        let tasks = [
            "pick up red block",
            "pick up blue block",
            "pickup red block",
            "pick up red  block", // double space
            "Pick up red block",  // capital P
        ];

        let hashes: std::collections::HashSet<String> = tasks
            .iter()
            .map(|t| DatasetMetadataRegistry::hash_task(t))
            .collect();

        // All hashes should be unique
        assert_eq!(hashes.len(), tasks.len());
    }

    #[test]
    fn test_feature_spec_compatibility_edge_cases() {
        // Same dtype and shape should be compatible
        let spec1 = FeatureSpec {
            dtype: "float32".to_string(),
            shape: vec![7],
            names: None,
            video_info: None,
        };

        let spec2 = FeatureSpec {
            dtype: "float32".to_string(),
            shape: vec![7],
            names: Some(vec!["x".to_string(), "y".to_string()]),
            video_info: None,
        };

        assert!(spec1.is_compatible(&spec2));

        // Different dimensions should not be compatible
        let spec3 = FeatureSpec {
            dtype: "float32".to_string(),
            shape: vec![7, 1],
            names: None,
            video_info: None,
        };
        assert!(!spec1.is_compatible(&spec3));

        // Video specs with same base shape but different video info should be compatible
        let video1 = FeatureSpec {
            dtype: "video".to_string(),
            shape: vec![480, 640, 3],
            names: None,
            video_info: Some(VideoInfo {
                codec: "libx264".to_string(),
                fps: 30,
                profile: None,
                crf: None,
            }),
        };

        let video2 = FeatureSpec {
            dtype: "video".to_string(),
            shape: vec![480, 640, 3],
            names: None,
            video_info: Some(VideoInfo {
                codec: "libx265".to_string(),
                fps: 60,
                profile: None,
                crf: None,
            }),
        };

        // Video info doesn't affect compatibility (only dtype and shape matter)
        assert!(video1.is_compatible(&video2));
    }

    #[test]
    fn test_partial_episode_metadata_with_all_fields() {
        let mut meta = PartialEpisodeMetadata::new(5);

        // Set all fields
        meta.length = 150;
        meta.tasks = vec!["task1".to_string(), "task2".to_string()];
        meta.parquet_path = "data/chunk-000/episode_000005.parquet".to_string();

        meta.feature_shapes.insert(
            "observation.state".to_string(),
            FeatureShape {
                dtype: "float32".to_string(),
                shape: vec![7],
                is_video: false,
            },
        );

        meta.feature_shapes.insert(
            "observation.images.cam_high".to_string(),
            FeatureShape {
                dtype: "video".to_string(),
                shape: vec![480, 640, 3],
                is_video: true,
            },
        );

        meta.video_paths.insert(
            "observation.images.cam_high".to_string(),
            "videos/chunk-000/observation.images.cam_high/episode_000005.mp4".to_string(),
        );

        meta.stats.insert(
            "observation.state".to_string(),
            FeatureStats {
                min: vec![-1.0; 7],
                max: vec![1.0; 7],
                mean: vec![0.0; 7],
                std: vec![0.5; 7],
            },
        );

        // Verify serialization roundtrip
        let serialized = bincode::serialize(&meta).unwrap();
        let deserialized: PartialEpisodeMetadata = bincode::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.episode_index, 5);
        assert_eq!(deserialized.length, 150);
        assert_eq!(deserialized.tasks.len(), 2);
        assert_eq!(deserialized.feature_shapes.len(), 2);
        assert_eq!(deserialized.video_paths.len(), 1);
        assert_eq!(deserialized.stats.len(), 1);
    }

    #[test]
    fn test_metadata_keys_format() {
        let batch_id = "test-batch-123";

        // Task counter key
        let counter_key = MetadataKeys::task_counter(batch_id);
        let counter_str = String::from_utf8(counter_key).unwrap();
        assert!(counter_str.contains("task_counter"));
        assert!(counter_str.contains(batch_id));

        // Task key with hash
        let task_key = MetadataKeys::task(batch_id, "abc123");
        let task_str = String::from_utf8(task_key).unwrap();
        assert!(task_str.contains("tasks"));
        assert!(task_str.contains("abc123"));

        // Feature key
        let feature_key = MetadataKeys::feature(batch_id, "observation.state");
        let feature_str = String::from_utf8(feature_key).unwrap();
        assert!(feature_str.contains("features"));
        assert!(feature_str.contains("observation.state"));

        // Episode metadata key with zero-padding
        let ep_key = MetadataKeys::episode_metadata(batch_id, 42);
        let ep_str = String::from_utf8(ep_key).unwrap();
        assert!(ep_str.contains("metadata/episode/000042"));

        // Episode 0 should be padded
        let ep0_key = MetadataKeys::episode_metadata(batch_id, 0);
        let ep0_str = String::from_utf8(ep0_key).unwrap();
        assert!(ep0_str.contains("metadata/episode/000000"));

        // Large episode number
        let ep_large_key = MetadataKeys::episode_metadata(batch_id, 999999);
        let ep_large_str = String::from_utf8(ep_large_key).unwrap();
        assert!(ep_large_str.contains("metadata/episode/999999"));
    }

    #[test]
    fn test_feature_spec_is_video_and_is_state() {
        let video_spec = FeatureSpec {
            dtype: "video".to_string(),
            shape: vec![480, 640, 3],
            names: None,
            video_info: Some(VideoInfo {
                codec: "libx264".to_string(),
                fps: 30,
                profile: None,
                crf: None,
            }),
        };

        assert!(video_spec.is_video());
        assert!(!video_spec.is_state());

        let state_spec = FeatureSpec {
            dtype: "float32".to_string(),
            shape: vec![7],
            names: None,
            video_info: None,
        };

        assert!(!state_spec.is_video());
        assert!(state_spec.is_state());

        // Multi-dimensional float is not a state
        let multi_dim = FeatureSpec {
            dtype: "float32".to_string(),
            shape: vec![10, 10],
            names: None,
            video_info: None,
        };
        assert!(!multi_dim.is_state());

        // Integer types can be states
        let int_state = FeatureSpec {
            dtype: "int64".to_string(),
            shape: vec![5],
            names: None,
            video_info: None,
        };
        assert!(int_state.is_state());
    }

    #[test]
    fn test_feature_spec_num_elements() {
        let spec1 = FeatureSpec {
            dtype: "float32".to_string(),
            shape: vec![7],
            names: None,
            video_info: None,
        };
        assert_eq!(spec1.num_elements(), 7);

        let spec2 = FeatureSpec {
            dtype: "video".to_string(),
            shape: vec![480, 640, 3],
            names: None,
            video_info: None,
        };
        assert_eq!(spec2.num_elements(), 480 * 640 * 3);

        let spec3 = FeatureSpec {
            dtype: "float32".to_string(),
            shape: vec![10, 20, 30],
            names: None,
            video_info: None,
        };
        assert_eq!(spec3.num_elements(), 10 * 20 * 30);
    }
}
