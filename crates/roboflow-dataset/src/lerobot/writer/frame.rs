// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Frame data structures for LeRobot Parquet files.

use std::collections::HashMap;

/// Frame data for LeRobot Parquet file.
#[derive(Debug)]
pub struct LerobotFrame {
    /// Episode index
    pub episode_index: usize,

    /// Frame index within episode
    pub frame_index: usize,

    /// Global frame index
    pub index: usize,

    /// Timestamp in seconds
    pub timestamp: f64,

    /// Observation state (joint positions)
    pub observation_state: Option<Vec<f32>>,

    /// Action (target joint positions)
    pub action: Option<Vec<f32>>,

    /// Task index
    pub task_index: Option<usize>,

    /// Image frame references (camera -> (path, timestamp))
    pub image_frames: HashMap<String, (String, f64)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lerobot_frame_creation() {
        let frame = LerobotFrame {
            episode_index: 0,
            frame_index: 10,
            index: 10,
            timestamp: 0.33,
            observation_state: Some(vec![1.0, 2.0, 3.0]),
            action: Some(vec![0.5, 0.5]),
            task_index: Some(0),
            image_frames: HashMap::new(),
        };

        assert_eq!(frame.episode_index, 0);
        assert_eq!(frame.frame_index, 10);
        assert_eq!(frame.index, 10);
        assert_eq!(frame.timestamp, 0.33);
        assert_eq!(frame.observation_state, Some(vec![1.0, 2.0, 3.0]));
        assert_eq!(frame.action, Some(vec![0.5, 0.5]));
        assert_eq!(frame.task_index, Some(0));
    }

    #[test]
    fn test_lerobot_frame_with_images() {
        let mut image_frames = HashMap::new();
        image_frames.insert(
            "cam_left".to_string(),
            ("videos/chunk-000/observation.images.cam_left/episode_000000.mp4".to_string(), 0.33),
        );
        image_frames.insert(
            "cam_right".to_string(),
            ("videos/chunk-000/observation.images.cam_right/episode_000000.mp4".to_string(), 0.33),
        );

        let frame = LerobotFrame {
            episode_index: 0,
            frame_index: 0,
            index: 0,
            timestamp: 0.0,
            observation_state: None,
            action: None,
            task_index: None,
            image_frames,
        };

        assert_eq!(frame.image_frames.len(), 2);
        assert!(frame.image_frames.contains_key("cam_left"));
        assert!(frame.image_frames.contains_key("cam_right"));
    }

    #[test]
    fn test_lerobot_frame_optional_fields() {
        // Frame without observation or action
        let frame = LerobotFrame {
            episode_index: 1,
            frame_index: 5,
            index: 105,
            timestamp: 0.165,
            observation_state: None,
            action: None,
            task_index: None,
            image_frames: HashMap::new(),
        };

        assert!(frame.observation_state.is_none());
        assert!(frame.action.is_none());
        assert!(frame.task_index.is_none());
    }
}
