// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Parquet file writing for LeRobot datasets.

use std::collections::HashMap;
use std::fs;
use std::io::BufWriter;
use std::path::{Path, PathBuf};

use polars::prelude::*;

use roboflow_core::RoboflowError;
use roboflow_core::Result;

use super::frame::LerobotFrame;

/// Write current episode to Parquet file.
///
/// This function collects all frame data for the current episode and writes
/// it to a Parquet file in LeRobot v2.1 format.
pub fn write_episode_parquet(
    frame_data: &[LerobotFrame],
    episode_index: usize,
    output_dir: &Path,
) -> Result<(PathBuf, usize)> {
    if frame_data.is_empty() {
        return Ok((PathBuf::new(), 0));
    }

    let state_dim = frame_data
        .first()
        .and_then(|f| f.observation_state.as_ref())
        .map(|v| v.len())
        .ok_or_else(|| {
            RoboflowError::encode(
                "LerobotWriter",
                "Cannot determine state dimension: first frame has no observation_state",
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
            let dim = observation_state.last().map_or(14, |s| s.len().min(14));
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
    let df = DataFrame::new(series_vec).map_err(|e| {
        RoboflowError::parse("Parquet", format!("DataFrame error: {}", e))
    })?;

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
