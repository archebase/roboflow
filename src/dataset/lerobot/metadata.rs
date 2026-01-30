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

use crate::core::Result;
use crate::dataset::common::parquet_base::FeatureStats;
use crate::dataset::lerobot::config::LerobotConfig;

/// LeRobot v2.1 info.json structure.
#[derive(Debug, Serialize)]
pub struct LerobotInfo {
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
    pub fn write_all(&self, output_dir: &Path, config: &LerobotConfig) -> Result<()> {
        let meta_dir = output_dir.join("meta");
        fs::create_dir_all(&meta_dir)?;

        self.write_info_json(&meta_dir, config)?;
        self.write_episodes_jsonl(&meta_dir)?;
        self.write_tasks_jsonl(&meta_dir)?;
        self.write_episodes_stats_jsonl(&meta_dir)?;

        Ok(())
    }

    /// Write meta/info.json.
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
        for (camera, (w, h)) in &self.image_shapes {
            features.insert(
                format!("observation.images.{}", camera),
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

        let robot_type = config
            .dataset
            .robot_type
            .clone()
            .unwrap_or_else(|| "unknown".to_string());

        let info = LerobotInfo {
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
        let info_json = serde_json::to_string_pretty(&info)
            .map_err(|e| crate::RoboflowError::parse("Metadata", format!("Failed to serialize info.json: {}", e)))?;

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
                crate::RoboflowError::parse("Metadata", format!("Failed to serialize episode for episodes.jsonl: {}", e))
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
        tasks.sort_by_key(|(_, &idx)| idx);

        for (task, task_index) in tasks {
            let task_info = TaskInfo {
                task_index: *task_index,
                task: task.clone(),
            };
            let line = serde_json::to_string(&task_info).map_err(|e| {
                crate::RoboflowError::parse("Metadata", format!("Failed to serialize task for tasks.jsonl: {}", e))
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
                crate::RoboflowError::parse("Metadata", format!("Failed to serialize episode stats for episodes_stats.jsonl: {}", e))
            })?;
            writeln!(file, "{}", line)?;
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
