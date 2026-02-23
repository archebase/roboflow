// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Global metadata assembler for finalizing datasets.
//!
//! This module aggregates partial metadata from TiKV and writes
//! LeRobot v2.1 compatible metadata files to storage.

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use serde_json::json;

use super::registry::DatasetMetadataRegistry;
use super::types::{
    EpisodeInfo, EpisodeStatsEntry, FeatureSpec, LerobotInfo, PartialEpisodeMetadata, TaskInfo,
};
use crate::stats::BatchStatsSummary;
use crate::tikv::TikvError;
use roboflow_storage::Storage;

/// Assembles global metadata from TiKV and writes LeRobot metadata files.
///
/// This is used by the finalizer to create the dataset-level metadata
/// after all episodes have been converted.
pub struct GlobalMetadataAssembler {
    registry: DatasetMetadataRegistry,
    storage: Arc<dyn Storage>,
    batch_id: String,
    output_prefix: String,
}

impl GlobalMetadataAssembler {
    /// Create a new metadata assembler.
    pub fn new(
        registry: DatasetMetadataRegistry,
        storage: Arc<dyn Storage>,
        batch_id: impl Into<String>,
        output_prefix: impl Into<String>,
    ) -> Self {
        Self {
            registry,
            storage,
            batch_id: batch_id.into(),
            output_prefix: output_prefix.into(),
        }
    }

    /// Assemble and write all LeRobot metadata files.
    ///
    /// This is the main entry point called by the finalizer.
    ///
    /// # Arguments
    /// * `dataset_name` - Name of the dataset
    /// * `robot_type` - Optional robot type
    /// * `fps` - Frame rate
    pub async fn assemble_and_write(
        &self,
        dataset_name: impl Into<String>,
        robot_type: Option<String>,
        fps: u32,
    ) -> Result<(), MetadataAssemblyError> {
        tracing::info!(
            batch_id = %self.batch_id,
            "Starting global metadata assembly"
        );

        // 1. Collect all episode metadata from TiKV
        let episodes = self.registry.scan_episode_metadata().await.map_err(|e| {
            MetadataAssemblyError::TiKvError(format!("Failed to scan episodes: {}", e))
        })?;

        if episodes.is_empty() {
            return Err(MetadataAssemblyError::NoEpisodes);
        }

        tracing::info!(
            batch_id = %self.batch_id,
            episode_count = episodes.len(),
            "Collected episode metadata"
        );

        // 2. Build tasks list from registry
        let task_entries = self.registry.scan_tasks().await.map_err(|e| {
            MetadataAssemblyError::TiKvError(format!("Failed to scan tasks: {}", e))
        })?;

        // Convert TaskEntry to TaskInfo
        let tasks: Vec<TaskInfo> = task_entries
            .into_iter()
            .map(|t| TaskInfo {
                task_index: t.task_index,
                task: t.task,
            })
            .collect();

        tracing::info!(
            batch_id = %self.batch_id,
            task_count = tasks.len(),
            "Collected tasks"
        );

        // 3. Build unified feature specs
        let features = self.registry.scan_features().await.map_err(|e| {
            MetadataAssemblyError::TiKvError(format!("Failed to scan features: {}", e))
        })?;

        tracing::info!(
            batch_id = %self.batch_id,
            feature_count = features.len(),
            "Collected feature specs"
        );

        // 4. Aggregate statistics
        let stats_summary = self.aggregate_statistics(&episodes);

        // 5. Write LeRobot v2.1 metadata files
        self.write_info_json(&episodes, &features, dataset_name, robot_type, fps)
            .await?;
        self.write_episodes_jsonl(&episodes).await?;
        self.write_tasks_jsonl(&tasks).await?;
        self.write_episodes_stats_jsonl(&episodes, &stats_summary)
            .await?;

        tracing::info!(
            batch_id = %self.batch_id,
            output_prefix = %self.output_prefix,
            "Global metadata assembly complete"
        );

        Ok(())
    }

    /// Aggregate statistics across all episodes.
    fn aggregate_statistics(&self, episodes: &[PartialEpisodeMetadata]) -> BatchStatsSummary {
        let mut summary = BatchStatsSummary::new(self.batch_id.clone());

        for episode in episodes {
            let mut ep_stats =
                crate::stats::EpisodeStats::new(episode.episode_index, episode.length);

            for (feature_name, stats) in &episode.stats {
                ep_stats.add_feature(feature_name.clone(), stats.clone());
            }

            summary.add_episode(ep_stats);
        }

        summary.calculate_global_stats();
        summary
    }

    /// Write info.json file.
    async fn write_info_json(
        &self,
        episodes: &[PartialEpisodeMetadata],
        features: &HashMap<String, FeatureSpec>,
        dataset_name: impl Into<String>,
        robot_type: Option<String>,
        fps: u32,
    ) -> Result<(), MetadataAssemblyError> {
        let total_frames: usize = episodes.iter().map(|e| e.length).sum();

        // Build features JSON
        let mut features_json = serde_json::Map::new();

        for (name, spec) in features {
            let feature_json = if spec.is_video() {
                json!({
                    "dtype": "video",
                    "shape": spec.shape,
                    "names": vec!["height", "width", "channel"],
                })
            } else {
                json!({
                    "dtype": spec.dtype,
                    "shape": spec.shape,
                })
            };
            features_json.insert(name.clone(), feature_json);
        }

        // Add timestamp index
        features_json.insert(
            "timestamp".to_string(),
            json!({
                "dtype": "float64",
                "shape": vec![1usize],
                "names": vec!["timestamp"],
            }),
        );

        // Add episode_index
        features_json.insert(
            "episode_index".to_string(),
            json!({
                "dtype": "int64",
                "shape": vec![1usize],
            }),
        );

        // Add index (frame index)
        features_json.insert(
            "index".to_string(),
            json!({
                "dtype": "int64",
                "shape": vec![1usize],
            }),
        );

        // Add task_index
        features_json.insert(
            "task_index".to_string(),
            json!({
                "dtype": "int64",
                "shape": vec![1usize],
            }),
        );

        let info = LerobotInfo {
            name: dataset_name.into(),
            codebase_version: "v2.1".to_string(),
            robot_type,
            total_episodes: episodes.len(),
            total_frames,
            fps,
            features: serde_json::Value::Object(features_json),
        };

        let content = serde_json::to_string_pretty(&info)
            .map_err(|e| MetadataAssemblyError::Serialization(e.to_string()))?;

        let path_str = format!("{}/meta/info.json", self.output_prefix);
        let path = Path::new(&path_str);
        let mut writer = self
            .storage
            .writer(path)
            .map_err(|e| MetadataAssemblyError::Storage(e.to_string()))?;
        writer
            .write_all(content.as_bytes())
            .map_err(|e| MetadataAssemblyError::Storage(e.to_string()))?;
        writer
            .flush()
            .map_err(|e| MetadataAssemblyError::Storage(e.to_string()))?;

        tracing::debug!(path = %path.display(), "Wrote info.json");

        Ok(())
    }

    /// Write episodes.jsonl file.
    async fn write_episodes_jsonl(
        &self,
        episodes: &[PartialEpisodeMetadata],
    ) -> Result<(), MetadataAssemblyError> {
        let mut lines = Vec::new();

        // Build task index lookup
        let tasks = self.registry.scan_tasks().await.map_err(|e| {
            MetadataAssemblyError::TiKvError(format!("Failed to scan tasks: {}", e))
        })?;
        let task_map: HashMap<String, usize> =
            tasks.into_iter().map(|t| (t.task, t.task_index)).collect();

        for episode in episodes {
            // Resolve task descriptions to indices
            let task_indices: Vec<usize> = episode
                .tasks
                .iter()
                .filter_map(|t| task_map.get(t).copied())
                .collect();

            let info = EpisodeInfo {
                episode_index: episode.episode_index,
                length: episode.length,
                tasks: task_indices,
            };

            let line = serde_json::to_string(&info)
                .map_err(|e| MetadataAssemblyError::Serialization(e.to_string()))?;
            lines.push(line);
        }

        let content = lines.join("\n");
        let path_str = format!("{}/meta/episodes.jsonl", self.output_prefix);
        let path = Path::new(&path_str);

        let mut writer = self
            .storage
            .writer(path)
            .map_err(|e| MetadataAssemblyError::Storage(e.to_string()))?;
        writer
            .write_all(content.as_bytes())
            .map_err(|e| MetadataAssemblyError::Storage(e.to_string()))?;
        writer
            .flush()
            .map_err(|e| MetadataAssemblyError::Storage(e.to_string()))?;

        tracing::debug!(path = %path.display(), episodes = lines.len(), "Wrote episodes.jsonl");

        Ok(())
    }

    /// Write tasks.jsonl file.
    async fn write_tasks_jsonl(&self, tasks: &[TaskInfo]) -> Result<(), MetadataAssemblyError> {
        let mut lines = Vec::new();

        for task in tasks {
            let line = serde_json::to_string(task)
                .map_err(|e| MetadataAssemblyError::Serialization(e.to_string()))?;
            lines.push(line);
        }

        let content = lines.join("\n");
        let path_str = format!("{}/meta/tasks.jsonl", self.output_prefix);
        let path = Path::new(&path_str);

        let mut writer = self
            .storage
            .writer(path)
            .map_err(|e| MetadataAssemblyError::Storage(e.to_string()))?;
        writer
            .write_all(content.as_bytes())
            .map_err(|e| MetadataAssemblyError::Storage(e.to_string()))?;
        writer
            .flush()
            .map_err(|e| MetadataAssemblyError::Storage(e.to_string()))?;

        tracing::debug!(path = %path.display(), tasks = lines.len(), "Wrote tasks.jsonl");

        Ok(())
    }

    /// Write episodes_stats.jsonl file.
    async fn write_episodes_stats_jsonl(
        &self,
        episodes: &[PartialEpisodeMetadata],
        _summary: &BatchStatsSummary,
    ) -> Result<(), MetadataAssemblyError> {
        let mut lines = Vec::new();

        for episode in episodes {
            // Build stats JSON for this episode
            let mut stats_map = serde_json::Map::new();

            for (feature_name, stats) in &episode.stats {
                let feature_stats = json!({
                    "min": stats.min,
                    "max": stats.max,
                    "mean": stats.mean,
                    "std": stats.std,
                });
                stats_map.insert(feature_name.clone(), feature_stats);
            }

            let entry = EpisodeStatsEntry {
                episode_index: episode.episode_index,
                stats: serde_json::Value::Object(stats_map),
            };

            let line = serde_json::to_string(&entry)
                .map_err(|e| MetadataAssemblyError::Serialization(e.to_string()))?;
            lines.push(line);
        }

        let content = lines.join("\n");
        let path_str = format!("{}/meta/episodes_stats.jsonl", self.output_prefix);
        let path = Path::new(&path_str);

        let mut writer = self
            .storage
            .writer(path)
            .map_err(|e| MetadataAssemblyError::Storage(e.to_string()))?;
        writer
            .write_all(content.as_bytes())
            .map_err(|e| MetadataAssemblyError::Storage(e.to_string()))?;
        writer
            .flush()
            .map_err(|e| MetadataAssemblyError::Storage(e.to_string()))?;

        tracing::debug!(path = %path.display(), episodes = lines.len(), "Wrote episodes_stats.jsonl");

        Ok(())
    }
}

/// Errors that can occur during metadata assembly.
#[derive(Debug)]
pub enum MetadataAssemblyError {
    /// TiKV operation failed.
    TiKvError(String),

    /// Storage operation failed.
    Storage(String),

    /// Serialization failed.
    Serialization(String),

    /// No episodes found in registry.
    NoEpisodes,
}

impl std::fmt::Display for MetadataAssemblyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetadataAssemblyError::TiKvError(e) => write!(f, "TiKV error: {}", e),
            MetadataAssemblyError::Storage(e) => write!(f, "Storage error: {}", e),
            MetadataAssemblyError::Serialization(e) => write!(f, "Serialization error: {}", e),
            MetadataAssemblyError::NoEpisodes => write!(f, "No episodes found in registry"),
        }
    }
}

impl std::error::Error for MetadataAssemblyError {}

impl From<TikvError> for MetadataAssemblyError {
    fn from(e: TikvError) -> Self {
        MetadataAssemblyError::TiKvError(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::types::{FeatureShape, TaskEntry, VideoInfo};
    use crate::stats::FeatureStats;

    #[test]
    fn test_metadata_assembly_error_display() {
        let err = MetadataAssemblyError::NoEpisodes;
        assert_eq!(err.to_string(), "No episodes found in registry");

        let err = MetadataAssemblyError::Storage("connection refused".to_string());
        assert!(err.to_string().contains("Storage error"));

        let err = MetadataAssemblyError::TiKvError("connection timeout".to_string());
        assert!(err.to_string().contains("TiKV error"));

        let err = MetadataAssemblyError::Serialization("invalid json".to_string());
        assert!(err.to_string().contains("Serialization error"));
    }

    #[test]
    fn test_metadata_assembly_error_from_tikv() {
        use crate::tikv::TikvError;

        let tikv_err = TikvError::ConnectionFailed("test".to_string());
        let assembly_err: MetadataAssemblyError = tikv_err.into();

        match assembly_err {
            MetadataAssemblyError::TiKvError(_) => (), // Expected
            _ => panic!("Expected TiKvError variant"),
        }
    }

    #[test]
    fn test_lerobot_info_creation() {
        let info = LerobotInfo {
            name: "test_dataset".to_string(),
            codebase_version: "v2.1".to_string(),
            robot_type: Some("panda".to_string()),
            total_episodes: 100,
            total_frames: 10000,
            fps: 30,
            features: serde_json::json!({
                "observation.state": {
                    "dtype": "float32",
                    "shape": [7]
                }
            }),
        };

        assert_eq!(info.name, "test_dataset");
        assert_eq!(info.codebase_version, "v2.1");
        assert_eq!(info.robot_type, Some("panda".to_string()));
        assert_eq!(info.total_episodes, 100);
        assert_eq!(info.total_frames, 10000);
        assert_eq!(info.fps, 30);
    }

    #[test]
    fn test_episode_info_creation() {
        let info = EpisodeInfo {
            episode_index: 42,
            length: 150,
            tasks: vec![0, 1],
        };

        assert_eq!(info.episode_index, 42);
        assert_eq!(info.length, 150);
        assert_eq!(info.tasks, vec![0, 1]);
    }

    #[test]
    fn test_task_info_creation() {
        let info = TaskInfo {
            task_index: 5,
            task: "pick up the red block".to_string(),
        };

        assert_eq!(info.task_index, 5);
        assert_eq!(info.task, "pick up the red block");
    }

    #[test]
    fn test_partial_episode_metadata_serialization() {
        let mut meta = PartialEpisodeMetadata::new(1);
        meta.length = 100;
        meta.tasks = vec!["task1".to_string()];
        meta.parquet_path = "test.parquet".to_string();

        meta.feature_shapes.insert(
            "observation.state".to_string(),
            FeatureShape {
                dtype: "float32".to_string(),
                shape: vec![7],
                is_video: false,
            },
        );

        meta.stats.insert(
            "observation.state".to_string(),
            FeatureStats {
                min: vec![0.0; 7],
                max: vec![1.0; 7],
                mean: vec![0.5; 7],
                std: vec![0.1; 7],
            },
        );

        // Test bincode serialization (used by registry)
        let serialized = bincode::serialize(&meta).unwrap();
        let deserialized: PartialEpisodeMetadata = bincode::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.episode_index, 1);
        assert_eq!(deserialized.length, 100);
        assert_eq!(deserialized.tasks.len(), 1);
        assert_eq!(deserialized.feature_shapes.len(), 1);
        assert_eq!(deserialized.stats.len(), 1);
    }

    #[test]
    fn test_feature_spec_serialization() {
        let spec = FeatureSpec {
            dtype: "video".to_string(),
            shape: vec![480, 640, 3],
            names: Some(vec![
                "height".to_string(),
                "width".to_string(),
                "channel".to_string(),
            ]),
            video_info: Some(VideoInfo {
                codec: "libx264".to_string(),
                fps: 30,
                profile: Some("high".to_string()),
                crf: Some(23),
            }),
        };

        // Test bincode serialization
        let serialized = bincode::serialize(&spec).unwrap();
        let deserialized: FeatureSpec = bincode::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.dtype, "video");
        assert_eq!(deserialized.shape, vec![480, 640, 3]);
        assert!(deserialized.video_info.is_some());
        let video_info = deserialized.video_info.unwrap();
        assert_eq!(video_info.codec, "libx264");
        assert_eq!(video_info.fps, 30);
    }

    #[test]
    fn test_task_entry_serialization() {
        let entry = TaskEntry {
            task_index: 42,
            task: "grasp the object".to_string(),
        };

        let serialized = bincode::serialize(&entry).unwrap();
        let deserialized: TaskEntry = bincode::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.task_index, 42);
        assert_eq!(deserialized.task, "grasp the object");
    }
}
