// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Parquet file writing for LeRobot datasets.

use std::collections::HashMap;
use std::fs;
use std::io::BufWriter;
use std::path::{Path, PathBuf};

use polars::prelude::*;

use roboflow_core::Result;
use roboflow_core::RoboflowError;

use super::frame::LerobotFrame;

/// Default action dimension for robotics datasets when not inferable from state.
/// Common for dual-arm setups like BridgeData and Aloha (7 DOF per arm).
const DEFAULT_ACTION_DIMENSION: usize = 14;

/// Write current episode to Parquet file.
///
/// This function collects all frame data for the current episode and writes
/// it to a Parquet file in LeRobot v2.1 format.
///
/// This is a convenience wrapper that uses chunk index 0.
/// For distributed processing with dynamic chunk indices, use `write_episode_parquet_with_chunk`.
#[allow(dead_code)]
pub fn write_episode_parquet(
    frame_data: &[LerobotFrame],
    episode_index: usize,
    output_dir: &Path,
) -> Result<(PathBuf, usize)> {
    if frame_data.is_empty() {
        return Ok((PathBuf::new(), 0));
    }

    // Find the state dimension from the first frame that has observation_state.
    // Early frames may contain only image/tf data before state messages arrive.
    let state_dim = frame_data
        .iter()
        .find_map(|f| f.observation_state.as_ref())
        .map(|v| v.len())
        .ok_or_else(|| {
            RoboflowError::encode(
                "LerobotWriter",
                "Cannot determine state dimension: no frame has observation_state",
            )
        })?;

    let mut episode_index_vec: Vec<i64> = Vec::new();
    let mut frame_index: Vec<i64> = Vec::new();
    let mut index: Vec<i64> = Vec::new();
    let mut timestamp: Vec<f64> = Vec::new();
    let mut observation_state: Vec<Vec<f32>> = Vec::new();
    let mut action: Vec<Vec<f32>> = Vec::new();
    let mut task_index: Vec<i64> = Vec::new();

    // Collect camera names from image_frames
    let mut cameras: Vec<String> = Vec::new();
    for frame in frame_data {
        for camera in frame.image_frames.keys() {
            if !cameras.contains(camera) {
                cameras.push(camera.clone());
            }
        }
    }

    // Image frame references per camera
    let mut image_paths: HashMap<String, Vec<String>> = HashMap::new();
    let mut image_timestamps: HashMap<String, Vec<f64>> = HashMap::new();
    for camera in &cameras {
        image_paths.insert(camera.clone(), Vec::new());
        image_timestamps.insert(camera.clone(), Vec::new());
    }

    // Track last action for forward-fill
    let mut last_action: Option<Vec<f32>> = None;

    for frame in frame_data {
        // Require observation_state
        if frame.observation_state.is_none() {
            continue;
        }

        episode_index_vec.push(frame.episode_index as i64);
        frame_index.push(frame.frame_index as i64);
        index.push(frame.index as i64);
        timestamp.push(frame.timestamp);

        if let Some(ref state) = frame.observation_state {
            observation_state.push(state.clone());
        }

        // Use action if available, otherwise forward-fill from previous frame
        let act = frame.action.as_ref().or(last_action.as_ref());
        if let Some(a) = act {
            action.push(a.clone());
            last_action = Some(a.clone());
        } else if !observation_state.is_empty() {
            // No action available yet, use zeros with correct dimension
            let dim = observation_state
                .last()
                .map_or(DEFAULT_ACTION_DIMENSION, |s| {
                    s.len().min(DEFAULT_ACTION_DIMENSION)
                });
            action.push(vec![0.0; dim]);
        }

        task_index.push(frame.task_index.map(|t| t as i64).unwrap_or(0));

        for camera in &cameras {
            if let Some((path, ts)) = frame.image_frames.get(camera) {
                if let Some(paths) = image_paths.get_mut(camera) {
                    paths.push(path.clone());
                }
                if let Some(timestamps) = image_timestamps.get_mut(camera) {
                    timestamps.push(*ts);
                }
            } else {
                // Default path if image not available
                let path = format!(
                    "videos/chunk-000/{}/episode_{:06}.mp4",
                    camera, episode_index
                );
                if let Some(paths) = image_paths.get_mut(camera) {
                    paths.push(path);
                }
                if let Some(timestamps) = image_timestamps.get_mut(camera) {
                    timestamps.push(frame.timestamp);
                }
            }
        }
    }

    // Build Parquet columns
    let mut series_vec = vec![
        Series::new("episode_index", episode_index_vec),
        Series::new("frame_index", frame_index),
        Series::new("index", index),
        Series::new("timestamp", timestamp),
    ];

    // Add observation state columns
    for i in 0..state_dim {
        let col_name = format!("observation.state.{}", i);
        let values: Vec<f32> = observation_state
            .iter()
            .map(|v| v.get(i).copied().unwrap_or(0.0))
            .collect();
        series_vec.push(Series::new(&col_name, values));
    }

    // Add action columns - use action dimension from first non-empty action
    let action_dim = action
        .iter()
        .find(|v| !v.is_empty())
        .map(|v| v.len())
        .unwrap_or(14);
    for i in 0..action_dim {
        let col_name = format!("action.{}", i);
        let values: Vec<f32> = action
            .iter()
            .map(|v| v.get(i).copied().unwrap_or(0.0))
            .collect();
        series_vec.push(Series::new(&col_name, values));
    }

    // Add task_index
    series_vec.push(Series::new("task_index", task_index));

    // Add image frame references
    for camera in &cameras {
        if let Some(paths) = image_paths.get(camera) {
            series_vec.push(Series::new(
                format!("{}_path", camera).as_str(),
                paths.clone(),
            ));
        }
        if let Some(timestamps) = image_timestamps.get(camera) {
            series_vec.push(Series::new(
                format!("{}_timestamp", camera).as_str(),
                timestamps.clone(),
            ));
        }
    }

    // Create DataFrame and write
    let df = DataFrame::new(series_vec)
        .map_err(|e| RoboflowError::parse("Parquet", format!("DataFrame error: {}", e)))?;

    let parquet_path = output_dir.join(format!(
        "data/chunk-000/episode_{:06}.parquet",
        episode_index
    ));

    let file = fs::File::create(&parquet_path)?;
    let mut writer = BufWriter::new(file);

    ParquetWriter::new(&mut writer)
        .finish(&mut df.clone())
        .map_err(|e| RoboflowError::parse("Parquet", format!("Write error: {}", e)))?;

    let file_size = if let Ok(metadata) = fs::metadata(&parquet_path) {
        metadata.len()
    } else {
        0
    };

    tracing::info!(
        path = %parquet_path.display(),
        frames = frame_data.len(),
        "Wrote LeRobot v2.1 Parquet file"
    );

    Ok((parquet_path, file_size as usize))
}

/// Write current episode to Parquet file with dynamic chunk index.
///
/// This is the preferred function for distributed processing where episodes
/// are allocated dynamically and chunk index is computed from episode_index.
///
/// # Arguments
///
/// * `frame_data` - Frame data for the episode
/// * `episode_index` - Episode index (global across the batch)
/// * `chunk_index` - Chunk index (computed as episode_index / episodes_per_chunk)
/// * `output_dir` - Base output directory
pub fn write_episode_parquet_with_chunk(
    frame_data: &[LerobotFrame],
    episode_index: usize,
    chunk_index: u32,
    output_dir: &Path,
) -> Result<(PathBuf, usize)> {
    if frame_data.is_empty() {
        return Ok((PathBuf::new(), 0));
    }

    // Find the state dimension from the first frame that has observation_state.
    let state_dim = frame_data
        .iter()
        .find_map(|f| f.observation_state.as_ref())
        .map(|v| v.len())
        .ok_or_else(|| {
            RoboflowError::encode(
                "LerobotWriter",
                "Cannot determine state dimension: no frame has observation_state",
            )
        })?;

    let mut episode_index_vec: Vec<i64> = Vec::new();
    let mut frame_index: Vec<i64> = Vec::new();
    let mut index: Vec<i64> = Vec::new();
    let mut timestamp: Vec<f64> = Vec::new();
    let mut observation_state: Vec<Vec<f32>> = Vec::new();
    let mut action: Vec<Vec<f32>> = Vec::new();
    let mut task_index: Vec<i64> = Vec::new();

    // Collect camera names from image_frames
    let mut cameras: Vec<String> = Vec::new();
    for frame in frame_data {
        for camera in frame.image_frames.keys() {
            if !cameras.contains(camera) {
                cameras.push(camera.clone());
            }
        }
    }

    // Image frame references per camera
    let mut image_paths: HashMap<String, Vec<String>> = HashMap::new();
    let mut image_timestamps: HashMap<String, Vec<f64>> = HashMap::new();
    for camera in &cameras {
        image_paths.insert(camera.clone(), Vec::new());
        image_timestamps.insert(camera.clone(), Vec::new());
    }

    // Track last action for forward-fill
    let mut last_action: Option<Vec<f32>> = None;

    for frame in frame_data {
        if frame.observation_state.is_none() {
            continue;
        }

        episode_index_vec.push(frame.episode_index as i64);
        frame_index.push(frame.frame_index as i64);
        index.push(frame.index as i64);
        timestamp.push(frame.timestamp);

        if let Some(ref state) = frame.observation_state {
            observation_state.push(state.clone());
        }

        let act = frame.action.as_ref().or(last_action.as_ref());
        if let Some(a) = act {
            action.push(a.clone());
            last_action = Some(a.clone());
        } else if !observation_state.is_empty() {
            let dim = observation_state
                .last()
                .map_or(DEFAULT_ACTION_DIMENSION, |s| {
                    s.len().min(DEFAULT_ACTION_DIMENSION)
                });
            action.push(vec![0.0; dim]);
        }

        task_index.push(frame.task_index.map(|t| t as i64).unwrap_or(0));

        for camera in &cameras {
            if let Some((path, ts)) = frame.image_frames.get(camera) {
                if let Some(paths) = image_paths.get_mut(camera) {
                    paths.push(path.clone());
                }
                if let Some(timestamps) = image_timestamps.get_mut(camera) {
                    timestamps.push(*ts);
                }
            } else {
                // Default path with dynamic chunk index
                let path = format!(
                    "videos/chunk-{:03}/{}/episode_{:06}.mp4",
                    chunk_index, camera, episode_index
                );
                if let Some(paths) = image_paths.get_mut(camera) {
                    paths.push(path);
                }
                if let Some(timestamps) = image_timestamps.get_mut(camera) {
                    timestamps.push(frame.timestamp);
                }
            }
        }
    }

    // Build Parquet columns
    let mut series_vec = vec![
        Series::new("episode_index", episode_index_vec),
        Series::new("frame_index", frame_index),
        Series::new("index", index),
        Series::new("timestamp", timestamp),
    ];

    // Add observation state columns
    for i in 0..state_dim {
        let col_name = format!("observation.state.{}", i);
        let values: Vec<f32> = observation_state
            .iter()
            .map(|v| v.get(i).copied().unwrap_or(0.0))
            .collect();
        series_vec.push(Series::new(&col_name, values));
    }

    // Add action columns
    let action_dim = action
        .iter()
        .find(|v| !v.is_empty())
        .map(|v| v.len())
        .unwrap_or(14);
    for i in 0..action_dim {
        let col_name = format!("action.{}", i);
        let values: Vec<f32> = action
            .iter()
            .map(|v| v.get(i).copied().unwrap_or(0.0))
            .collect();
        series_vec.push(Series::new(&col_name, values));
    }

    // Add task_index
    series_vec.push(Series::new("task_index", task_index));

    // Add image frame references
    for camera in &cameras {
        if let Some(paths) = image_paths.get(camera) {
            series_vec.push(Series::new(
                format!("{}_path", camera).as_str(),
                paths.clone(),
            ));
        }
        if let Some(timestamps) = image_timestamps.get(camera) {
            series_vec.push(Series::new(
                format!("{}_timestamp", camera).as_str(),
                timestamps.clone(),
            ));
        }
    }

    // Create DataFrame and write
    let df = DataFrame::new(series_vec)
        .map_err(|e| RoboflowError::parse("Parquet", format!("DataFrame error: {}", e)))?;

    // Use dynamic chunk index for path
    let parquet_path = output_dir.join(format!(
        "data/chunk-{:03}/episode_{:06}.parquet",
        chunk_index, episode_index
    ));

    let file = fs::File::create(&parquet_path)?;
    let mut writer = BufWriter::new(file);

    ParquetWriter::new(&mut writer)
        .finish(&mut df.clone())
        .map_err(|e| RoboflowError::parse("Parquet", format!("Write error: {}", e)))?;

    let file_size = if let Ok(metadata) = fs::metadata(&parquet_path) {
        metadata.len()
    } else {
        0
    };

    tracing::info!(
        path = %parquet_path.display(),
        frames = frame_data.len(),
        chunk_index,
        episode_index,
        "Wrote LeRobot v2.1 Parquet file with dynamic chunk"
    );

    Ok((parquet_path, file_size as usize))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_frame(
        episode: usize,
        frame: usize,
        state: Option<Vec<f32>>,
        action: Option<Vec<f32>>,
    ) -> LerobotFrame {
        LerobotFrame {
            episode_index: episode,
            frame_index: frame,
            index: episode * 100 + frame,
            timestamp: frame as f64 / 30.0,
            observation_state: state,
            action,
            task_index: Some(0),
            image_frames: HashMap::new(),
        }
    }

    /// Create the directory structure expected by write_episode_parquet
    fn setup_output_dir(dir: &Path) {
        fs::create_dir_all(dir.join("data/chunk-000")).unwrap();
    }

    #[test]
    fn test_write_empty_frames() {
        let dir = tempdir().unwrap();
        let result = write_episode_parquet(&[], 0, dir.path());
        assert!(result.is_ok());
        let (path, size) = result.unwrap();
        assert!(path.as_os_str().is_empty());
        assert_eq!(size, 0);
    }

    #[test]
    fn test_write_requires_observation_state() {
        let dir = tempdir().unwrap();
        setup_output_dir(dir.path());
        // Frame without observation_state
        let frames = vec![create_test_frame(0, 0, None, None)];

        let result = write_episode_parquet(&frames, 0, dir.path());
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("state dimension"));
    }

    #[test]
    fn test_write_basic_episode() {
        let dir = tempdir().unwrap();
        setup_output_dir(dir.path());
        let frames = vec![
            create_test_frame(0, 0, Some(vec![0.0, 0.0, 0.0]), Some(vec![1.0, 1.0])),
            create_test_frame(0, 1, Some(vec![0.1, 0.1, 0.1]), Some(vec![1.1, 1.1])),
            create_test_frame(0, 2, Some(vec![0.2, 0.2, 0.2]), Some(vec![1.2, 1.2])),
        ];

        let result = write_episode_parquet(&frames, 0, dir.path());
        assert!(result.is_ok());

        let (path, size) = result.unwrap();
        assert!(path.to_string_lossy().contains("episode_000000.parquet"));
        assert!(size > 0);

        // Verify file exists
        assert!(path.exists());

        // Verify directory structure
        assert!(path.parent().unwrap().ends_with("chunk-000"));
    }

    #[test]
    fn test_write_with_forward_fill_action() {
        let dir = tempdir().unwrap();
        setup_output_dir(dir.path());
        // First frame has action, second doesn't (should forward-fill)
        let frames = vec![
            create_test_frame(0, 0, Some(vec![0.0, 0.0]), Some(vec![5.0, 5.0])),
            create_test_frame(0, 1, Some(vec![0.1, 0.1]), None),
            create_test_frame(0, 2, Some(vec![0.2, 0.2]), Some(vec![6.0, 6.0])),
        ];

        let result = write_episode_parquet(&frames, 0, dir.path());
        assert!(result.is_ok());
        let (path, _) = result.unwrap();
        assert!(path.exists());
    }

    #[test]
    fn test_write_with_zero_action_fallback() {
        let dir = tempdir().unwrap();
        setup_output_dir(dir.path());
        // No action in any frame (should use zeros)
        let frames = vec![
            create_test_frame(0, 0, Some(vec![0.0, 0.0, 0.0]), None),
            create_test_frame(0, 1, Some(vec![0.1, 0.1, 0.1]), None),
        ];

        let result = write_episode_parquet(&frames, 0, dir.path());
        assert!(result.is_ok());
    }

    #[test]
    fn test_write_with_camera_paths() {
        let dir = tempdir().unwrap();
        setup_output_dir(dir.path());
        let mut frame = create_test_frame(0, 0, Some(vec![0.0, 0.0]), Some(vec![1.0, 1.0]));
        frame.image_frames.insert(
            "cam_left".to_string(),
            (
                "videos/chunk-000/observation.images.cam_left/episode_000000.mp4".to_string(),
                0.0,
            ),
        );

        let frames = vec![frame];
        let result = write_episode_parquet(&frames, 0, dir.path());
        assert!(result.is_ok());
    }

    #[test]
    fn test_write_episode_with_high_index() {
        let dir = tempdir().unwrap();
        setup_output_dir(dir.path());
        let frames = vec![create_test_frame(999, 0, Some(vec![0.0]), Some(vec![1.0]))];

        let result = write_episode_parquet(&frames, 999, dir.path());
        assert!(result.is_ok());

        let (path, _) = result.unwrap();
        assert!(path.to_string_lossy().contains("episode_000999.parquet"));
    }

    #[test]
    fn test_skips_frames_without_state() {
        let dir = tempdir().unwrap();
        setup_output_dir(dir.path());
        let frames = vec![
            create_test_frame(0, 0, None, None), // Should be skipped
            create_test_frame(0, 1, Some(vec![0.0]), Some(vec![1.0])), // Valid
            create_test_frame(0, 2, Some(vec![0.1]), Some(vec![1.1])), // Valid
        ];

        let result = write_episode_parquet(&frames, 0, dir.path());
        assert!(result.is_ok());
        let (path, _) = result.unwrap();
        assert!(path.exists());
    }
}
