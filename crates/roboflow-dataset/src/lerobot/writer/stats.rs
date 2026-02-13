// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Statistics calculation and tracking for LeRobot writer.

use crate::common::parquet_base::calculate_stats;
use crate::lerobot::metadata::MetadataCollector;
use std::collections::HashMap;

/// Calculate episode statistics from frame data.
pub fn calculate_episode_stats(
    frame_data: &[super::frame::LerobotFrame],
    episode_index: usize,
    metadata: &mut MetadataCollector,
) -> Result<(), roboflow_core::RoboflowError> {
    if frame_data.is_empty() {
        return Ok(());
    }

    let mut stats = HashMap::new();

    // Calculate observation.state stats
    let state_values: Vec<Vec<f32>> = frame_data
        .iter()
        .filter_map(|f| f.observation_state.as_ref())
        .cloned()
        .collect();

    if let Some(feature_stats) = calculate_stats(&state_values) {
        stats.insert("observation.state".to_string(), feature_stats);
    }

    // Calculate action stats
    let action_values: Vec<Vec<f32>> = frame_data
        .iter()
        .filter_map(|f| f.action.as_ref())
        .cloned()
        .collect();

    if let Some(feature_stats) = calculate_stats(&action_values) {
        stats.insert("action".to_string(), feature_stats);
    }

    metadata.add_episode_stats(episode_index, stats);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::frame::LerobotFrame;
    use super::*;
    use crate::lerobot::metadata::MetadataCollector;
    use std::collections::HashMap;

    fn create_test_frame(
        episode: usize,
        frame: usize,
        state: Vec<f32>,
        action: Vec<f32>,
    ) -> LerobotFrame {
        LerobotFrame {
            episode_index: episode,
            frame_index: frame,
            index: episode * 100 + frame,
            timestamp: frame as f64 * 0.033,
            observation_state: Some(state),
            action: Some(action),
            task_index: Some(0),
            image_frames: HashMap::new(),
        }
    }

    #[test]
    fn test_calculate_episode_stats_empty_frames() {
        let mut metadata = MetadataCollector::new();
        let result = calculate_episode_stats(&[], 0, &mut metadata);
        assert!(result.is_ok());
    }

    #[test]
    fn test_calculate_episode_stats_single_frame() {
        let mut metadata = MetadataCollector::new();
        let frames = vec![create_test_frame(0, 0, vec![1.0, 2.0, 3.0], vec![0.5, 0.5])];

        let result = calculate_episode_stats(&frames, 0, &mut metadata);
        assert!(result.is_ok());
    }

    #[test]
    fn test_calculate_episode_stats_multiple_frames() {
        let mut metadata = MetadataCollector::new();
        let frames = vec![
            create_test_frame(0, 0, vec![1.0, 2.0, 3.0], vec![0.0, 0.0]),
            create_test_frame(0, 1, vec![2.0, 3.0, 4.0], vec![1.0, 1.0]),
            create_test_frame(0, 2, vec![3.0, 4.0, 5.0], vec![2.0, 2.0]),
        ];

        let result = calculate_episode_stats(&frames, 0, &mut metadata);
        assert!(result.is_ok());

        // Stats should be calculated for observation.state and action
        assert_eq!(metadata.episode_stats.len(), 1);
        let episode_stats = &metadata.episode_stats[0];
        assert_eq!(episode_stats.episode_index, 0);
        // Stats are stored as JSON, verify it's not empty
        assert!(!episode_stats.stats.is_null());
    }

    #[test]
    fn test_calculate_episode_stats_missing_fields() {
        let mut metadata = MetadataCollector::new();
        let mut frame = create_test_frame(0, 0, vec![1.0, 2.0], vec![0.5]);
        frame.observation_state = None;

        let frames = vec![frame];
        let result = calculate_episode_stats(&frames, 0, &mut metadata);
        assert!(result.is_ok());

        // Should have one episode stat entry
        assert_eq!(metadata.episode_stats.len(), 1);
    }

    #[test]
    fn test_calculate_episode_stats_missing_action() {
        let mut metadata = MetadataCollector::new();
        let mut frame = create_test_frame(0, 0, vec![1.0, 2.0, 3.0], vec![0.5]);
        frame.action = None;

        let frames = vec![frame];
        let result = calculate_episode_stats(&frames, 0, &mut metadata);
        assert!(result.is_ok());

        // Should have stats for observation.state only
        assert_eq!(metadata.episode_stats.len(), 1);
    }

    #[test]
    fn test_calculate_episode_stats_high_episode_index() {
        let mut metadata = MetadataCollector::new();
        let frames = vec![create_test_frame(999, 0, vec![0.0], vec![1.0])];

        let result = calculate_episode_stats(&frames, 999, &mut metadata);
        assert!(result.is_ok());

        assert_eq!(metadata.episode_stats.len(), 1);
        assert_eq!(metadata.episode_stats[0].episode_index, 999);
    }

    #[test]
    fn test_calculate_episode_stats_mixed_frames() {
        let mut metadata = MetadataCollector::new();

        // Mix of frames with and without optional fields
        let mut frame1 = create_test_frame(0, 0, vec![1.0], vec![0.5]);
        frame1.observation_state = None;

        let frame2 = create_test_frame(0, 1, vec![2.0], vec![1.0]);
        let mut frame3 = create_test_frame(0, 2, vec![3.0], vec![1.5]);
        frame3.action = None;

        let frames = vec![frame1, frame2, frame3];
        let result = calculate_episode_stats(&frames, 0, &mut metadata);
        assert!(result.is_ok());
    }

    #[test]
    fn test_calculate_episode_stats_all_missing_state() {
        let mut metadata = MetadataCollector::new();

        // All frames without observation_state
        let mut frame1 = create_test_frame(0, 0, vec![], vec![0.5]);
        frame1.observation_state = None;

        let mut frame2 = create_test_frame(0, 1, vec![], vec![1.0]);
        frame2.observation_state = None;

        let frames = vec![frame1, frame2];
        let result = calculate_episode_stats(&frames, 0, &mut metadata);
        assert!(result.is_ok());

        // Stats should still be created (even if empty)
        assert_eq!(metadata.episode_stats.len(), 1);
    }
}
