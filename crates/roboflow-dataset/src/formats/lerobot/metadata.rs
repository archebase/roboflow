// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! LeRobot v2.1 metadata files.
//!
//! Creates the metadata files required by LeRobot v2.1 format:
//! - meta/info.json
//! - meta/episodes.jsonl
//! - meta/tasks.jsonl
//! - meta/episodes_stats.jsonl

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use serde::Serialize;
use serde_json::json;

use crate::formats::common::parquet_base::FeatureStats;
use crate::formats::lerobot::config::LerobotConfig;
use roboflow_core::Result;

use std::sync::Arc;

use roboflow_storage::Storage;

/// LeRobot v2.1 info.json structure.
#[derive(Debug, Serialize)]
pub struct LerobotInfo {
    /// Dataset name
    pub name: String,

    /// Codebase version
    pub codebase_version: String,

    /// Robot type
    pub robot_type: String,

    /// Total episodes
    pub total_episodes: usize,

    /// Total frames
    pub total_frames: usize,

    /// Frames per second
    pub fps: u32,

    /// Feature specifications
    pub features: serde_json::Value,

    /// Video info (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video: Option<VideoInfo>,
}

/// Video information.
#[derive(Debug, Serialize)]
pub struct VideoInfo {
    /// Video FPS
    pub fps: u32,

    /// Video codec
    pub codec: String,
}

/// Episode information for episodes.jsonl.
#[derive(Debug, Serialize)]
pub struct EpisodeInfo {
    /// Episode index
    pub episode_index: usize,

    /// Episode length in frames
    pub length: usize,

    /// Task indices for this episode
    pub tasks: Vec<usize>,
}

/// Task information for tasks.jsonl.
#[derive(Debug, Serialize)]
pub struct TaskInfo {
    /// Task index
    pub task_index: usize,

    /// Task description
    pub task: String,
}

/// Episode statistics for episodes_stats.jsonl.
#[derive(Debug, Serialize)]
pub struct EpisodeStats {
    /// Episode index
    pub episode_index: usize,

    /// Statistics per feature
    pub stats: serde_json::Value,
}

/// Metadata collector for LeRobot datasets.
pub struct MetadataCollector {
    /// Episode information
    pub episodes: Vec<EpisodeInfo>,

    /// Task information
    pub tasks: HashMap<String, usize>,

    /// Episode statistics
    pub episode_stats: Vec<EpisodeStats>,

    /// Image shapes (camera -> (width, height))
    pub image_shapes: HashMap<String, (usize, usize)>,

    /// State dimensions (feature -> dim)
    pub state_dims: HashMap<String, usize>,

    /// Total frame count
    pub total_frames: usize,
}

impl MetadataCollector {
    /// Create a new metadata collector.
    pub fn new() -> Self {
        Self {
            episodes: Vec::new(),
            tasks: HashMap::new(),
            episode_stats: Vec::new(),
            image_shapes: HashMap::new(),
            state_dims: HashMap::new(),
            total_frames: 0,
        }
    }

    /// Add an episode.
    pub fn add_episode(&mut self, index: usize, length: usize, tasks: Vec<usize>) {
        self.episodes.push(EpisodeInfo {
            episode_index: index,
            length,
            tasks,
        });
        self.total_frames += length;
    }

    /// Register a task and return its index.
    pub fn register_task(&mut self, task: String) -> usize {
        let len = self.tasks.len();
        *self.tasks.entry(task).or_insert(len)
    }

    /// Update image shape for a camera.
    pub fn update_image_shape(&mut self, camera: String, width: usize, height: usize) {
        self.image_shapes.insert(camera, (width, height));
    }

    /// Update state dimension for a feature.
    pub fn update_state_dim(&mut self, feature: String, dim: usize) {
        self.state_dims.insert(feature, dim);
    }

    /// Add episode statistics.
    pub fn add_episode_stats(&mut self, index: usize, stats: HashMap<String, FeatureStats>) {
        let stats_json = serde_json::to_value(stats).unwrap_or_default();
        self.episode_stats.push(EpisodeStats {
            episode_index: index,
            stats: stats_json,
        });
    }

    /// Write all metadata files to the output directory.
    ///
    /// This method writes to the local filesystem. For storage backend support,
    /// use `write_all_to_storage` instead.
    pub fn write_all(&self, output_dir: &Path, config: &LerobotConfig) -> Result<()> {
        let meta_dir = output_dir.join("meta");
        fs::create_dir_all(&meta_dir)?;

        self.write_info_json(&meta_dir, config)?;
        self.write_episodes_jsonl(&meta_dir)?;
        self.write_tasks_jsonl(&meta_dir)?;
        self.write_episodes_stats_jsonl(&meta_dir)?;

        Ok(())
    }

    /// Write all metadata files to a storage backend.
    ///
    /// This is the preferred method for cloud storage support, as it writes
    /// directly to the storage backend without requiring local filesystem access.
    ///
    /// # Arguments
    ///
    /// * `storage` - The storage backend to write to
    /// * `output_prefix` - The output prefix within storage (e.g., "datasets/my_dataset")
    /// * `config` - The LeRobot configuration
    pub fn write_all_to_storage(
        &self,
        storage: &Arc<dyn Storage>,
        output_prefix: &str,
        config: &LerobotConfig,
    ) -> Result<()> {
        let meta_prefix = if output_prefix.is_empty() {
            "meta".to_string()
        } else {
            format!("{}/meta", output_prefix)
        };

        storage
            .create_dir_all(Path::new(&meta_prefix))
            .map_err(|e| {
                roboflow_core::RoboflowError::storage(
                    "storage",
                    format!("Failed to create directory {}: {}", meta_prefix, e),
                    false,
                )
            })?;

        self.write_info_json_to_storage(storage, &meta_prefix, config)?;
        self.write_episodes_jsonl_to_storage(storage, &meta_prefix)?;
        self.write_tasks_jsonl_to_storage(storage, &meta_prefix)?;
        self.write_episodes_stats_jsonl_to_storage(storage, &meta_prefix)?;

        Ok(())
    }

    /// Write meta/info.json to storage.
    fn write_info_json(&self, meta_dir: &Path, config: &LerobotConfig) -> Result<()> {
        let mut features = serde_json::Map::new();

        // Add observation state feature
        if let Some(&dim) = self.state_dims.get("observation.state") {
            features.insert(
                "observation.state".to_string(),
                json!({
                    "dtype": "float32",
                    "shape": [dim],
                }),
            );
        }

        // Add action feature
        if let Some(&dim) = self.state_dims.get("action") {
            features.insert(
                "action".to_string(),
                json!({
                    "dtype": "float32",
                    "shape": [dim],
                }),
            );
        }

        // Add image features
        // camera key already contains the full feature path (e.g., "observation.images.cam_high")
        for (camera, (w, h)) in &self.image_shapes {
            features.insert(
                camera.clone(),
                json!({
                    "dtype": "video",
                    "shape": [*h, *w, 3],
                    "names": ["height", "width", "channel"],
                    "info": {
                        "video.fps": config.dataset.fps,
                        "video.codec": config.video.codec,
                    }
                }),
            );
        }

        let name = config.dataset.name.clone();

        let robot_type = config
            .dataset
            .robot_type
            .clone()
            .unwrap_or_else(|| "unknown".to_string());

        let info = LerobotInfo {
            name,
            codebase_version: "v2.1".to_string(),
            robot_type,
            total_episodes: self.episodes.len(),
            total_frames: self.total_frames,
            fps: config.dataset.fps,
            features: serde_json::Value::Object(features),
            video: Some(VideoInfo {
                fps: config.dataset.fps,
                codec: config.video.codec.clone(),
            }),
        };

        let info_path = meta_dir.join("info.json");
        let info_json = serde_json::to_string_pretty(&info).map_err(|e| {
            roboflow_core::RoboflowError::parse(
                "Metadata",
                format!("Failed to serialize info.json: {}", e),
            )
        })?;

        fs::write(&info_path, info_json)?;
        tracing::info!(path = %info_path.display(), "Wrote LeRobot v2.1 info.json");

        Ok(())
    }

    /// Write meta/episodes.jsonl.
    fn write_episodes_jsonl(&self, meta_dir: &Path) -> Result<()> {
        let episodes_path = meta_dir.join("episodes.jsonl");
        let mut file = File::create(&episodes_path)?;

        for episode in &self.episodes {
            let line = serde_json::to_string(episode).map_err(|e| {
                roboflow_core::RoboflowError::parse(
                    "Metadata",
                    format!("Failed to serialize episode for episodes.jsonl: {}", e),
                )
            })?;
            writeln!(file, "{}", line)?;
        }

        tracing::info!(path = %episodes_path.display(), "Wrote LeRobot v2.1 episodes.jsonl");

        Ok(())
    }

    /// Write meta/tasks.jsonl.
    fn write_tasks_jsonl(&self, meta_dir: &Path) -> Result<()> {
        if self.tasks.is_empty() {
            return Ok(());
        }

        let tasks_path = meta_dir.join("tasks.jsonl");
        let mut file = File::create(&tasks_path)?;

        // Sort by task index
        let mut tasks: Vec<_> = self.tasks.iter().collect();
        tasks.sort_by_key(|(_, idx)| **idx);

        for (task, task_index) in tasks {
            let task_info = TaskInfo {
                task_index: *task_index,
                task: task.clone(),
            };
            let line = serde_json::to_string(&task_info).map_err(|e| {
                roboflow_core::RoboflowError::parse(
                    "Metadata",
                    format!("Failed to serialize task for tasks.jsonl: {}", e),
                )
            })?;
            writeln!(file, "{}", line)?;
        }

        tracing::info!(path = %tasks_path.display(), "Wrote LeRobot v2.1 tasks.jsonl");

        Ok(())
    }

    /// Write meta/episodes_stats.jsonl.
    fn write_episodes_stats_jsonl(&self, meta_dir: &Path) -> Result<()> {
        let stats_path = meta_dir.join("episodes_stats.jsonl");
        let mut file = File::create(&stats_path)?;

        for stats in &self.episode_stats {
            let line = serde_json::to_string(stats).map_err(|e| {
                roboflow_core::RoboflowError::parse(
                    "Metadata",
                    format!(
                        "Failed to serialize episode stats for episodes_stats.jsonl: {}",
                        e
                    ),
                )
            })?;
            writeln!(file, "{}", line)?;
        }

        tracing::info!(path = %stats_path.display(), "Wrote LeRobot v2.1 episodes_stats.jsonl");

        Ok(())
    }
}

// =============================================================================
// Storage Backend Support
// =============================================================================

impl MetadataCollector {
    /// Write meta/info.json to storage.
    fn write_info_json_to_storage(
        &self,
        storage: &Arc<dyn Storage>,
        meta_dir: &str,
        config: &LerobotConfig,
    ) -> Result<()> {
        let mut features = serde_json::Map::new();

        // Add observation state feature
        if let Some(&dim) = self.state_dims.get("observation.state") {
            features.insert(
                "observation.state".to_string(),
                json!({
                    "dtype": "float32",
                    "shape": [dim],
                }),
            );
        }

        // Add action feature
        if let Some(&dim) = self.state_dims.get("action") {
            features.insert(
                "action".to_string(),
                json!({
                    "dtype": "float32",
                    "shape": [dim],
                }),
            );
        }

        // Add image features
        // camera key already contains the full feature path (e.g., "observation.images.cam_high")
        for (camera, (w, h)) in &self.image_shapes {
            features.insert(
                camera.clone(),
                json!({
                    "dtype": "video",
                    "shape": [*h, *w, 3],
                    "names": ["height", "width", "channel"],
                    "info": {
                        "video.fps": config.dataset.fps,
                        "video.codec": config.video.codec,
                    }
                }),
            );
        }

        let name = config.dataset.name.clone();

        let robot_type = config
            .dataset
            .robot_type
            .clone()
            .unwrap_or_else(|| "unknown".to_string());

        let info = LerobotInfo {
            name,
            codebase_version: "v2.1".to_string(),
            robot_type,
            total_episodes: self.episodes.len(),
            total_frames: self.total_frames,
            fps: config.dataset.fps,
            features: serde_json::Value::Object(features),
            video: Some(VideoInfo {
                fps: config.dataset.fps,
                codec: config.video.codec.clone(),
            }),
        };

        let info_json = serde_json::to_string_pretty(&info).map_err(|e| {
            roboflow_core::RoboflowError::parse(
                "Metadata",
                format!("Failed to serialize info.json: {}", e),
            )
        })?;

        let info_path = Path::new(meta_dir).join("info.json");
        let mut writer = storage.writer(&info_path).map_err(|e| {
            roboflow_core::RoboflowError::Other(format!(
                "Failed to open writer for {}: {}",
                info_path.display(),
                e
            ))
        })?;
        writer.write_all(info_json.as_bytes()).map_err(|e| {
            roboflow_core::RoboflowError::Other(format!("Failed to write info.json: {}", e))
        })?;

        tracing::info!(path = %info_path.display(), "Wrote LeRobot v2.1 info.json");

        Ok(())
    }

    /// Write meta/episodes.jsonl to storage.
    fn write_episodes_jsonl_to_storage(
        &self,
        storage: &Arc<dyn Storage>,
        meta_dir: &str,
    ) -> Result<()> {
        let episodes_path = Path::new(meta_dir).join("episodes.jsonl");
        let mut writer = storage.writer(&episodes_path).map_err(|e| {
            roboflow_core::RoboflowError::Other(format!(
                "Failed to open writer for {}: {}",
                episodes_path.display(),
                e
            ))
        })?;

        for episode in &self.episodes {
            let line = serde_json::to_string(episode).map_err(|e| {
                roboflow_core::RoboflowError::parse(
                    "Metadata",
                    format!("Failed to serialize episode for episodes.jsonl: {}", e),
                )
            })?;
            writeln!(writer, "{}", line).map_err(|e| {
                roboflow_core::RoboflowError::Other(format!(
                    "Failed to write episodes.jsonl: {}",
                    e
                ))
            })?;
        }

        tracing::info!(path = %episodes_path.display(), "Wrote LeRobot v2.1 episodes.jsonl");

        Ok(())
    }

    /// Write meta/tasks.jsonl to storage.
    fn write_tasks_jsonl_to_storage(
        &self,
        storage: &Arc<dyn Storage>,
        meta_dir: &str,
    ) -> Result<()> {
        if self.tasks.is_empty() {
            return Ok(());
        }

        let tasks_path = Path::new(meta_dir).join("tasks.jsonl");
        let mut writer = storage.writer(&tasks_path).map_err(|e| {
            roboflow_core::RoboflowError::Other(format!(
                "Failed to open writer for {}: {}",
                tasks_path.display(),
                e
            ))
        })?;

        // Sort by task index
        let mut tasks: Vec<_> = self.tasks.iter().collect();
        tasks.sort_by_key(|(_, idx)| **idx);

        for (task, task_index) in tasks {
            let task_info = TaskInfo {
                task_index: *task_index,
                task: task.clone(),
            };
            let line = serde_json::to_string(&task_info).map_err(|e| {
                roboflow_core::RoboflowError::parse(
                    "Metadata",
                    format!("Failed to serialize task for tasks.jsonl: {}", e),
                )
            })?;
            writeln!(writer, "{}", line).map_err(|e| {
                roboflow_core::RoboflowError::Other(format!("Failed to write tasks.jsonl: {}", e))
            })?;
        }

        tracing::info!(path = %tasks_path.display(), "Wrote LeRobot v2.1 tasks.jsonl");

        Ok(())
    }

    /// Write meta/episodes_stats.jsonl to storage.
    fn write_episodes_stats_jsonl_to_storage(
        &self,
        storage: &Arc<dyn Storage>,
        meta_dir: &str,
    ) -> Result<()> {
        let stats_path = Path::new(meta_dir).join("episodes_stats.jsonl");
        let mut writer = storage.writer(&stats_path).map_err(|e| {
            roboflow_core::RoboflowError::Other(format!(
                "Failed to open writer for {}: {}",
                stats_path.display(),
                e
            ))
        })?;

        for stats in &self.episode_stats {
            let line = serde_json::to_string(stats).map_err(|e| {
                roboflow_core::RoboflowError::parse(
                    "Metadata",
                    format!(
                        "Failed to serialize episode stats for episodes_stats.jsonl: {}",
                        e
                    ),
                )
            })?;
            writeln!(writer, "{}", line).map_err(|e| {
                roboflow_core::RoboflowError::Other(format!(
                    "Failed to write episodes_stats.jsonl: {}",
                    e
                ))
            })?;
        }

        tracing::info!(path = %stats_path.display(), "Wrote LeRobot v2.1 episodes_stats.jsonl");

        Ok(())
    }
}

impl Default for MetadataCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::common::config::DatasetBaseConfig;
    use crate::formats::common::parquet_base::FeatureStats;
    use crate::formats::lerobot::config::{
        DatasetConfig, FlushingConfig, LerobotConfig, StreamingConfig, VideoConfig,
    };
    use roboflow_storage::LocalStorage;
    use std::path::PathBuf;

    fn test_config(robot_type: Option<&str>) -> LerobotConfig {
        LerobotConfig {
            dataset: DatasetConfig {
                base: DatasetBaseConfig {
                    name: "dataset_for_metadata_tests".to_string(),
                    fps: 30,
                    robot_type: robot_type.map(ToString::to_string),
                },
                env_type: None,
            },
            mappings: Vec::new(),
            video: VideoConfig::default(),
            annotation_file: None,
            flushing: FlushingConfig::default(),
            streaming: StreamingConfig::default(),
        }
    }

    fn sample_stats() -> HashMap<String, FeatureStats> {
        let mut m = HashMap::new();
        m.insert(
            "observation.state".to_string(),
            FeatureStats {
                mean: vec![0.0, 1.0],
                std: vec![1.0, 2.0],
                min: vec![-1.0, -1.0],
                max: vec![2.0, 3.0],
            },
        );
        m
    }

    #[test]
    fn test_write_all_generates_expected_files_and_content() {
        let mut collector = MetadataCollector::new();
        collector.update_state_dim("observation.state".to_string(), 6);
        collector.update_state_dim("action".to_string(), 6);
        collector.update_image_shape("observation.images.cam_front".to_string(), 640, 480);
        let task_index = collector.register_task("pick and place".to_string());
        collector.add_episode(0, 10, vec![task_index]);
        collector.add_episode_stats(0, sample_stats());

        let tmp = tempfile::tempdir().expect("tempdir");
        collector
            .write_all(tmp.path(), &test_config(None))
            .expect("write all metadata");

        let meta = tmp.path().join("meta");
        assert!(meta.join("info.json").exists());
        assert!(meta.join("episodes.jsonl").exists());
        assert!(meta.join("tasks.jsonl").exists());
        assert!(meta.join("episodes_stats.jsonl").exists());

        let info_text = std::fs::read_to_string(meta.join("info.json")).expect("read info.json");
        let info: serde_json::Value = serde_json::from_str(&info_text).expect("parse info.json");
        assert_eq!(info["robot_type"], "unknown");
        assert_eq!(info["total_episodes"], 1);
        assert_eq!(info["total_frames"], 10);
        assert!(info["features"]["observation.state"].is_object());
        assert!(info["features"]["action"].is_object());
        assert!(info["features"]["observation.images.cam_front"].is_object());
    }

    #[test]
    fn test_write_all_skips_tasks_file_when_no_tasks() {
        let mut collector = MetadataCollector::new();
        collector.add_episode(0, 2, vec![]);
        collector.add_episode_stats(0, sample_stats());

        let tmp = tempfile::tempdir().expect("tempdir");
        collector
            .write_all(tmp.path(), &test_config(Some("ur5")))
            .expect("write all metadata");

        let meta = tmp.path().join("meta");
        assert!(meta.join("info.json").exists());
        assert!(meta.join("episodes.jsonl").exists());
        assert!(meta.join("episodes_stats.jsonl").exists());
        assert!(!meta.join("tasks.jsonl").exists());
    }

    #[test]
    fn test_write_all_to_storage_with_and_without_prefix() {
        let mut collector = MetadataCollector::new();
        collector.update_state_dim("observation.state".to_string(), 3);
        collector.add_episode(0, 1, vec![]);
        collector.add_episode_stats(0, sample_stats());

        let root = tempfile::tempdir().expect("tempdir");
        let storage = Arc::new(LocalStorage::new(root.path())) as Arc<dyn Storage>;

        collector
            .write_all_to_storage(&storage, "dataset_a", &test_config(Some("franka")))
            .expect("write metadata to storage with prefix");

        let prefixed_meta = PathBuf::from(root.path()).join("dataset_a/meta");
        assert!(prefixed_meta.join("info.json").exists());
        assert!(prefixed_meta.join("episodes.jsonl").exists());
        assert!(prefixed_meta.join("episodes_stats.jsonl").exists());

        collector
            .write_all_to_storage(&storage, "", &test_config(Some("franka")))
            .expect("write metadata to storage without prefix");

        let root_meta = PathBuf::from(root.path()).join("meta");
        assert!(root_meta.join("info.json").exists());
        assert!(root_meta.join("episodes.jsonl").exists());
        assert!(root_meta.join("episodes_stats.jsonl").exists());
    }

    #[test]
    fn test_write_all_to_storage_writes_tasks_and_feature_details() {
        let mut collector = MetadataCollector::new();
        collector.update_state_dim("observation.state".to_string(), 7);
        collector.update_state_dim("action".to_string(), 4);
        collector.update_image_shape("observation.images.cam_left".to_string(), 1280, 720);

        let t_pick = collector.register_task("pick".to_string());
        let t_place = collector.register_task("place".to_string());

        collector.add_episode(0, 5, vec![t_pick]);
        collector.add_episode(1, 6, vec![t_place]);
        collector.add_episode_stats(0, sample_stats());
        collector.add_episode_stats(1, sample_stats());

        let root = tempfile::tempdir().expect("tempdir");
        let storage = Arc::new(LocalStorage::new(root.path())) as Arc<dyn Storage>;

        collector
            .write_all_to_storage(&storage, "dataset_b", &test_config(Some("franka")))
            .expect("write metadata to storage");

        let meta = PathBuf::from(root.path()).join("dataset_b/meta");
        let info_text = std::fs::read_to_string(meta.join("info.json")).expect("read info.json");
        let info: serde_json::Value = serde_json::from_str(&info_text).expect("parse info.json");

        assert_eq!(info["robot_type"], "franka");
        assert_eq!(info["total_episodes"], 2);
        assert_eq!(info["total_frames"], 11);
        assert_eq!(info["features"]["observation.state"]["shape"][0], 7);
        assert_eq!(info["features"]["action"]["shape"][0], 4);
        assert_eq!(
            info["features"]["observation.images.cam_left"]["shape"][0],
            720
        );
        assert_eq!(
            info["features"]["observation.images.cam_left"]["shape"][1],
            1280
        );

        let tasks_text = std::fs::read_to_string(meta.join("tasks.jsonl")).expect("read tasks");
        let task_lines: Vec<&str> = tasks_text.lines().collect();
        assert_eq!(task_lines.len(), 2);

        let episodes_text =
            std::fs::read_to_string(meta.join("episodes.jsonl")).expect("read episodes");
        assert_eq!(episodes_text.lines().count(), 2);

        let stats_text =
            std::fs::read_to_string(meta.join("episodes_stats.jsonl")).expect("read stats");
        assert_eq!(stats_text.lines().count(), 2);
    }

    #[test]
    fn test_write_all_to_storage_uses_unknown_robot_type_by_default() {
        let mut collector = MetadataCollector::new();
        collector.add_episode(0, 1, vec![]);

        let root = tempfile::tempdir().expect("tempdir");
        let storage = Arc::new(LocalStorage::new(root.path())) as Arc<dyn Storage>;

        collector
            .write_all_to_storage(&storage, "dataset_c", &test_config(None))
            .expect("write metadata to storage");

        let info_text = std::fs::read_to_string(root.path().join("dataset_c/meta/info.json"))
            .expect("read info.json");
        let info: serde_json::Value = serde_json::from_str(&info_text).expect("parse info.json");
        assert_eq!(info["robot_type"], "unknown");
    }
}
