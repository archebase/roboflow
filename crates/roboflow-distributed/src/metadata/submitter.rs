// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Worker metadata submission for distributed dataset conversion.
//!
//! This module provides integration between the conversion pipeline and the
//! distributed metadata registry. Workers use this to submit metadata after
//! converting a bag file.

use std::collections::HashMap;

use super::registry::DatasetMetadataRegistry;
use super::types::{FeatureShape, PartialEpisodeMetadata};
use crate::stats::{EpisodeStats, FeatureStats};
use crate::tikv::TikvError;

/// Submits episode metadata from workers to the distributed registry.
///
/// This is used by workers after converting a bag file to register
/// tasks, features, and episode metadata in TiKV.
///
/// # Example
///
/// ```rust,ignore
/// let submitter = MetadataSubmitter::new(registry);
///
/// // After conversion
/// submitter.submit_episode(
///     episode_index,
///     frame_count,
///     vec!["pick up object"],
///     feature_shapes,
///     parquet_path,
///     video_paths,
///     stats,
/// ).await?;
/// ```
#[derive(Clone)]
pub struct MetadataSubmitter {
    registry: DatasetMetadataRegistry,
}

impl MetadataSubmitter {
    /// Create a new metadata submitter.
    pub fn new(registry: DatasetMetadataRegistry) -> Self {
        Self { registry }
    }

    /// Submit episode metadata after conversion.
    ///
    /// This method:
    /// 1. Registers tasks (global deduplication)
    /// 2. Registers feature specs (with validation)
    /// 3. Stores partial episode metadata in TiKV
    ///
    /// # Arguments
    /// * `episode_index` - Global episode index
    /// * `frame_count` - Number of frames
    /// * `tasks` - Task descriptions
    /// * `feature_shapes` - Feature shapes detected during conversion
    /// * `parquet_path` - Relative path to Parquet file
    /// * `video_paths` - Relative paths to video files
    /// * `stats` - Per-feature statistics
    #[allow(clippy::too_many_arguments)]
    pub async fn submit_episode(
        &self,
        episode_index: usize,
        frame_count: usize,
        tasks: Vec<String>,
        feature_shapes: HashMap<String, FeatureShape>,
        parquet_path: String,
        video_paths: HashMap<String, String>,
        stats: HashMap<String, FeatureStats>,
    ) -> Result<SubmissionResult, TikvError> {
        // Register tasks and get global indices
        let mut task_indices = Vec::with_capacity(tasks.len());
        for task in &tasks {
            let idx = self.registry.register_task(task).await?;
            task_indices.push(idx);
        }

        // Register feature specs
        for (name, shape) in &feature_shapes {
            let spec = shape.to_spec();
            self.registry.register_feature(name, spec).await?;
        }

        // Build and store episode metadata
        let metadata = PartialEpisodeMetadata {
            episode_index,
            length: frame_count,
            tasks,
            feature_shapes,
            parquet_path,
            video_paths,
            stats,
            recorded_at: Some(chrono::Utc::now().timestamp()),
        };

        self.registry.store_episode_metadata(&metadata).await?;

        tracing::info!(
            episode_index = episode_index,
            task_count = task_indices.len(),
            feature_count = metadata.feature_shapes.len(),
            "Submitted episode metadata"
        );

        Ok(SubmissionResult {
            episode_index,
            task_indices,
        })
    }

    /// Submit episode with pre-computed episode stats.
    ///
    /// This is a convenience method that converts EpisodeStats to the format
    /// needed by the registry.
    pub async fn submit_episode_with_stats(
        &self,
        episode_index: usize,
        episode_stats: &EpisodeStats,
        feature_shapes: HashMap<String, FeatureShape>,
        parquet_path: String,
        video_paths: HashMap<String, String>,
    ) -> Result<SubmissionResult, TikvError> {
        // Convert tasks from indices back to strings for storage
        // (we'll resolve them to indices during final assembly)
        let tasks: Vec<String> = episode_stats
            .task_indices
            .iter()
            .map(|idx| format!("task_{}", idx))
            .collect();

        self.submit_episode(
            episode_index,
            episode_stats.frame_count,
            tasks,
            feature_shapes,
            parquet_path,
            video_paths,
            episode_stats.feature_stats.clone(),
        )
        .await
    }

    /// Get the underlying registry.
    pub fn registry(&self) -> &DatasetMetadataRegistry {
        &self.registry
    }
}

/// Result of a successful metadata submission.
#[derive(Debug, Clone)]
pub struct SubmissionResult {
    /// Episode index that was submitted.
    pub episode_index: usize,

    /// Global task indices assigned to this episode.
    pub task_indices: Vec<usize>,
}

/// Extracts feature shapes from a LerobotWriter.
///
/// This is a helper to convert writer state into FeatureShape objects
/// that can be registered with the metadata registry.
pub fn extract_feature_shapes(
    state_dims: &HashMap<String, usize>,
    image_shapes: &HashMap<String, (usize, usize)>,
) -> HashMap<String, FeatureShape> {
    let mut shapes = HashMap::new();

    // Extract state/action features
    for (name, dim) in state_dims {
        shapes.insert(
            name.clone(),
            FeatureShape {
                dtype: "float32".to_string(),
                shape: vec![*dim],
                is_video: false,
            },
        );
    }

    // Extract image/video features
    for (name, (height, width)) in image_shapes {
        shapes.insert(
            name.clone(),
            FeatureShape {
                dtype: "video".to_string(),
                shape: vec![*height, *width, 3],
                is_video: true,
            },
        );
    }

    shapes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_feature_shapes() {
        let mut state_dims = HashMap::new();
        state_dims.insert("observation.state".to_string(), 7);
        state_dims.insert("action".to_string(), 7);

        let mut image_shapes = HashMap::new();
        image_shapes.insert("observation.images.cam_high".to_string(), (480, 640));
        image_shapes.insert("observation.images.cam_left".to_string(), (480, 640));

        let shapes = extract_feature_shapes(&state_dims, &image_shapes);

        assert_eq!(shapes.len(), 4);

        // Check state feature
        let state = shapes.get("observation.state").unwrap();
        assert_eq!(state.dtype, "float32");
        assert_eq!(state.shape, vec![7]);
        assert!(!state.is_video);

        // Check video feature
        let video = shapes.get("observation.images.cam_high").unwrap();
        assert_eq!(video.dtype, "video");
        assert_eq!(video.shape, vec![480, 640, 3]);
        assert!(video.is_video);
    }

    #[test]
    fn test_submission_result() {
        let result = SubmissionResult {
            episode_index: 42,
            task_indices: vec![0, 1, 2],
        };

        assert_eq!(result.episode_index, 42);
        assert_eq!(result.task_indices, vec![0, 1, 2]);
    }
}
