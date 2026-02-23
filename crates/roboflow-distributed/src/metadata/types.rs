// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Core types for distributed dataset metadata management.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Feature specification for unified schema across episodes.
///
/// This defines the data type, shape, and additional metadata for a feature
/// (e.g., "observation.state", "observation.images.cam_high", "action").
/// Feature specs are registered in TiKV and validated for consistency.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FeatureSpec {
    /// Data type: "float32", "float64", "int32", "int64", "video", etc.
    pub dtype: String,

    /// Shape dimensions: [dim] for vectors, [H, W, 3] for images/videos
    pub shape: Vec<usize>,

    /// Optional dimension names: ["height", "width", "channel"]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub names: Option<Vec<String>>,

    /// Video-specific metadata (only for dtype == "video")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video_info: Option<VideoInfo>,
}

/// Video codec and encoding information.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VideoInfo {
    /// Video codec: "libx264", "libx265", etc.
    pub codec: String,

    /// Frame rate (fps).
    pub fps: u32,

    /// Video profile: "high", "main", "baseline", etc.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,

    /// Constant rate factor (quality setting).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub crf: Option<u32>,
}

/// Feature shape information for workers to report.
///
/// This is a simplified version used during conversion before
/// the full FeatureSpec is registered.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureShape {
    /// Data type.
    pub dtype: String,

    /// Shape dimensions.
    pub shape: Vec<usize>,

    /// Whether this feature is a video.
    pub is_video: bool,
}

impl FeatureShape {
    /// Convert to a full FeatureSpec.
    pub fn to_spec(&self) -> FeatureSpec {
        FeatureSpec {
            dtype: self.dtype.clone(),
            shape: self.shape.clone(),
            names: None,
            video_info: if self.is_video {
                Some(VideoInfo {
                    codec: "libx264".to_string(),
                    fps: 30,
                    profile: None,
                    crf: None,
                })
            } else {
                None
            },
        }
    }
}

impl FeatureSpec {
    /// Check if two specs are compatible.
    ///
    /// Features are compatible if they have the same dtype and shape.
    /// Video codec details can differ.
    pub fn is_compatible(&self, other: &FeatureSpec) -> bool {
        self.dtype == other.dtype && self.shape == other.shape
    }

    /// Get the total number of elements (product of shape).
    pub fn num_elements(&self) -> usize {
        self.shape.iter().product()
    }

    /// Check if this is a video feature.
    pub fn is_video(&self) -> bool {
        self.dtype == "video"
    }

    /// Check if this is a state/action feature (numeric vector).
    pub fn is_state(&self) -> bool {
        matches!(
            self.dtype.as_str(),
            "float32" | "float64" | "int32" | "int64"
        ) && self.shape.len() == 1
    }
}

/// Task entry in the global registry.
///
/// Tasks are content-addressed (via hash) to enable deduplication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEntry {
    /// Global task index (0, 1, 2, ...).
    pub task_index: usize,

    /// Task description text.
    pub task: String,
}

/// Partial episode metadata written by workers after conversion.
///
/// This is stored in TiKV for later aggregation by the finalizer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartialEpisodeMetadata {
    /// Global episode index (allocated from TiKV).
    pub episode_index: usize,

    /// Frame count.
    pub length: usize,

    /// Task descriptions (not yet resolved to indices).
    pub tasks: Vec<String>,

    /// Feature shapes detected during conversion.
    pub feature_shapes: HashMap<String, FeatureShape>,

    /// Relative path to Parquet file in storage.
    pub parquet_path: String,

    /// Relative paths to video files: camera_name -> path.
    pub video_paths: HashMap<String, String>,

    /// Per-feature statistics (min/max/mean/std).
    pub stats: HashMap<String, crate::stats::FeatureStats>,

    /// Timestamp when metadata was recorded.
    #[serde(default)]
    pub recorded_at: Option<i64>,
}

impl PartialEpisodeMetadata {
    /// Create new metadata with current timestamp.
    pub fn new(episode_index: usize) -> Self {
        Self {
            episode_index,
            length: 0,
            tasks: Vec::new(),
            feature_shapes: HashMap::new(),
            parquet_path: String::new(),
            video_paths: HashMap::new(),
            stats: HashMap::new(),
            recorded_at: Some(chrono::Utc::now().timestamp()),
        }
    }
}

/// Task info for tasks.jsonl output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInfo {
    pub task_index: usize,
    pub task: String,
}

/// Episode info for episodes.jsonl output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpisodeInfo {
    pub episode_index: usize,
    pub length: usize,
    pub tasks: Vec<usize>,
}

/// Episode statistics for episodes_stats.jsonl output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpisodeStatsEntry {
    pub episode_index: usize,
    pub stats: serde_json::Value,
}

/// Feature information for LeRobot info.json.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureInfo {
    pub dtype: String,
    pub shape: Vec<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub names: Option<Vec<String>>,
}

/// Video information for LeRobot info.json.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoFeatureInfo {
    pub dtype: String,
    pub shape: Vec<usize>,
    pub names: Vec<String>,
}

/// LeRobot info.json structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LerobotInfo {
    /// Dataset name.
    pub name: String,

    /// Codebase version.
    #[serde(rename = "codebase_version")]
    pub codebase_version: String,

    /// Robot type.
    #[serde(rename = "robot_type")]
    pub robot_type: Option<String>,

    /// Total number of episodes.
    #[serde(rename = "total_episodes")]
    pub total_episodes: usize,

    /// Total number of frames.
    #[serde(rename = "total_frames")]
    pub total_frames: usize,

    /// Frame rate.
    pub fps: u32,

    /// Feature specifications.
    pub features: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::FeatureStats;

    #[test]
    fn test_feature_spec_compatibility() {
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

        let spec3 = FeatureSpec {
            dtype: "float64".to_string(),
            shape: vec![7],
            names: None,
            video_info: None,
        };

        // Same dtype/shape should be compatible even with different names
        assert!(spec1.is_compatible(&spec2));
        // Different dtype should not be compatible
        assert!(!spec1.is_compatible(&spec3));
        // Different shape should not be compatible
        let spec4 = FeatureSpec {
            dtype: "float32".to_string(),
            shape: vec![14],
            names: None,
            video_info: None,
        };
        assert!(!spec1.is_compatible(&spec4));
    }

    #[test]
    fn test_feature_shape_to_spec() {
        let shape = FeatureShape {
            dtype: "video".to_string(),
            shape: vec![480, 640, 3],
            is_video: true,
        };

        let spec = shape.to_spec();
        assert_eq!(spec.dtype, "video");
        assert_eq!(spec.shape, vec![480, 640, 3]);
        assert!(spec.video_info.is_some());
        let video_info = spec.video_info.unwrap();
        assert_eq!(video_info.codec, "libx264");
        assert_eq!(video_info.fps, 30);
    }

    #[test]
    fn test_feature_shape_to_spec_non_video() {
        let shape = FeatureShape {
            dtype: "float32".to_string(),
            shape: vec![7],
            is_video: false,
        };

        let spec = shape.to_spec();
        assert_eq!(spec.dtype, "float32");
        assert_eq!(spec.shape, vec![7]);
        assert!(!spec.is_video());
        assert!(spec.video_info.is_none());
    }

    #[test]
    fn test_partial_episode_metadata() {
        let meta = PartialEpisodeMetadata::new(42);
        assert_eq!(meta.episode_index, 42);
        assert!(meta.recorded_at.is_some());
        assert_eq!(meta.length, 0);
        assert!(meta.tasks.is_empty());
        assert!(meta.feature_shapes.is_empty());
    }

    #[test]
    fn test_partial_episode_metadata_with_stats() {
        let mut meta = PartialEpisodeMetadata::new(1);
        meta.length = 100;
        meta.tasks = vec!["pick up object".to_string()];
        meta.parquet_path = "data/chunk-000/episode_000001.parquet".to_string();

        // Add feature shapes
        meta.feature_shapes.insert(
            "observation.state".to_string(),
            FeatureShape {
                dtype: "float32".to_string(),
                shape: vec![7],
                is_video: false,
            },
        );

        // Add stats
        meta.stats.insert(
            "observation.state".to_string(),
            FeatureStats {
                min: vec![0.0; 7],
                max: vec![1.0; 7],
                mean: vec![0.5; 7],
                std: vec![0.1; 7],
            },
        );

        assert_eq!(meta.episode_index, 1);
        assert_eq!(meta.length, 100);
        assert_eq!(meta.tasks.len(), 1);
        assert_eq!(meta.feature_shapes.len(), 1);
        assert_eq!(meta.stats.len(), 1);
    }

    #[test]
    fn test_feature_spec_helpers() {
        let video_spec = FeatureSpec {
            dtype: "video".to_string(),
            shape: vec![480, 640, 3],
            names: None,
            video_info: Some(VideoInfo {
                codec: "libx264".to_string(),
                fps: 30,
                profile: Some("high".to_string()),
                crf: Some(23),
            }),
        };

        assert!(video_spec.is_video());
        assert!(!video_spec.is_state());
        assert_eq!(video_spec.num_elements(), 480 * 640 * 3);

        let state_spec = FeatureSpec {
            dtype: "float32".to_string(),
            shape: vec![7],
            names: None,
            video_info: None,
        };

        assert!(!state_spec.is_video());
        assert!(state_spec.is_state());
        assert_eq!(state_spec.num_elements(), 7);
    }

    #[test]
    fn test_video_info_creation() {
        let info = VideoInfo {
            codec: "libx265".to_string(),
            fps: 60,
            profile: Some("main".to_string()),
            crf: Some(28),
        };

        assert_eq!(info.codec, "libx265");
        assert_eq!(info.fps, 60);
        assert_eq!(info.profile, Some("main".to_string()));
        assert_eq!(info.crf, Some(28));
    }

    #[test]
    fn test_lerobot_info_serialization() {
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

        let json = serde_json::to_string_pretty(&info).unwrap();
        assert!(json.contains("test_dataset"));
        assert!(json.contains("v2.1"));
        assert!(json.contains("panda"));
        assert!(json.contains("100"));
    }
}
