//! LeRobot metadata generation.
//!
//! Creates `meta/info.json` and other metadata files
//! required by the LeRobot dataset format.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use serde::Serialize;

use crate::format::lerobot::config::LeRobotConfig;

/// LeRobot info.json metadata.
#[derive(Debug, Serialize)]
pub struct LeRobotInfo {
    pub features: Features,
    pub fps: u32,
    pub codebase_version: String,
    pub total_episodes: u64,
    pub total_frames: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub robot_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video_info: Option<VideoInfo>,
}

#[derive(Debug, Serialize)]
pub struct Features {
    pub observation: HashMap<String, FeatureSpec>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub action: HashMap<String, FeatureSpec>,
}

#[derive(Debug, Serialize)]
pub struct FeatureSpec {
    pub shape: Vec<usize>,
    pub dtype: &'static str,
}

#[derive(Debug, Serialize)]
pub struct VideoInfo {
    pub video_height: usize,
    pub video_width: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video_codec: Option<String>,
}

/// Generate `meta/info.json` from configuration and extracted data.
pub fn write_info_json(
    output_dir: &Path,
    config: &LeRobotConfig,
    frame_count: u64,
    image_shapes: &HashMap<String, (usize, usize)>, // topic -> (width, height)
    state_shapes: &HashMap<String, usize>,          // topic -> dimension
) -> Result<(), Box<dyn std::error::Error>> {
    let meta_dir = output_dir.join("meta");
    fs::create_dir_all(&meta_dir)?;

    let mut features = Features {
        observation: HashMap::new(),
        action: HashMap::new(),
    };

    // Process mappings into feature specs
    for mapping in &config.mappings {
        let (shape, dtype) = match &mapping.mapping_type {
            crate::format::lerobot::config::MappingType::Image => {
                // Try to get image shape
                if let Some((w, h)) = image_shapes.get(&mapping.topic) {
                    (vec![*h, *w, 3], "uint8") // Assume RGB
                } else {
                    (vec![480, 640, 3], "uint8") // Default shape
                }
            }
            crate::format::lerobot::config::MappingType::State => {
                // Try to get state dimension
                if let Some(dim) = state_shapes.get(&mapping.topic) {
                    (vec![*dim], "float32")
                } else {
                    (vec![7], "float32") // Default DOF
                }
            }
            crate::format::lerobot::config::MappingType::Action => {
                if let Some(dim) = state_shapes.get(&mapping.topic) {
                    (vec![*dim], "float32")
                } else {
                    (vec![7], "float32")
                }
            }
            crate::format::lerobot::config::MappingType::Timestamp => (vec![1], "float64"),
        };

        // Parse feature path (e.g., "observation.camera_0")
        let parts: Vec<&str> = mapping.feature.split('.').collect();
        if parts.len() >= 2 {
            let category = parts[0];
            let name = parts[1..].join(".");

            let spec = FeatureSpec {
                shape: shape.clone(),
                dtype,
            };

            if category == "observation" {
                features.observation.insert(name, spec);
            } else if category == "action" {
                features.action.insert(name, spec);
            }
        }
    }

    // Check if we have images for video info
    let video_info = if image_shapes.is_empty() {
        None
    } else {
        // Use first image shape
        let first_shape = image_shapes.values().next();
        first_shape.map(|&(w, h)| VideoInfo {
            video_height: h,
            video_width: w,
            video_codec: Some("h264".to_string()),
        })
    };

    let info = LeRobotInfo {
        features,
        fps: config.dataset.fps,
        codebase_version: "v0.2.0".to_string(),
        total_episodes: 1, // Single episode for now
        total_frames: frame_count,
        robot_type: config.dataset.robot_type.clone(),
        video_info,
    };

    let info_path = meta_dir.join("info.json");
    let json = serde_json::to_string_pretty(&info)?;
    fs::write(&info_path, json)?;

    println!("Created: {}", info_path.display());

    Ok(())
}

/// Create episode metadata file.
pub fn write_episode_json(
    output_dir: &Path,
    episode_index: usize,
    start_time: u64,
    end_time: u64,
    frame_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let episodes_dir = output_dir.join("meta").join("episodes");
    fs::create_dir_all(&episodes_dir)?;

    #[derive(Serialize)]
    struct EpisodeInfo {
        episode_index: usize,
        start_time: f64,
        end_time: f64,
        length: usize,
    }

    let info = EpisodeInfo {
        episode_index,
        start_time: start_time as f64 / 1_000_000_000.0,
        end_time: end_time as f64 / 1_000_000_000.0,
        length: frame_count,
    };

    let episode_path = episodes_dir.join(format!("episode_{}.jsonl", episode_index));
    let json = serde_json::to_string(&info)?;
    fs::write(&episode_path, format!("{}\n", json))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_info() {
        let mut features = Features {
            observation: HashMap::new(),
            action: HashMap::new(),
        };

        features.observation.insert(
            "camera_0".to_string(),
            FeatureSpec {
                shape: vec![480, 640, 3],
                dtype: "uint8",
            },
        );

        features.action.insert(
            "position".to_string(),
            FeatureSpec {
                shape: vec![7],
                dtype: "float32",
            },
        );

        let info = LeRobotInfo {
            features,
            fps: 30,
            codebase_version: "v0.2.0".to_string(),
            total_episodes: 1,
            total_frames: 1000,
            robot_type: Some("genie_s".to_string()),
            video_info: None,
        };

        let json = serde_json::to_string_pretty(&info).unwrap();
        assert!(json.contains("observation"));
        assert!(json.contains("camera_0"));
        assert!(json.contains("\"fps\": 30"));
    }
}
