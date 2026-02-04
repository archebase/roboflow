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
