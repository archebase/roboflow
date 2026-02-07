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
