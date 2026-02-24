// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Validation utilities for distributed dataset metadata.
//!
//! This module provides validation tools to ensure dataset integrity
//! and consistency across distributed workers.

use std::collections::HashMap;

use super::types::{FeatureSpec, PartialEpisodeMetadata};

/// Validation result for a dataset.
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// Whether validation passed.
    pub valid: bool,

    /// List of validation errors.
    pub errors: Vec<ValidationError>,

    /// List of validation warnings.
    pub warnings: Vec<ValidationWarning>,

    /// Summary statistics.
    pub summary: ValidationSummary,
}

impl ValidationResult {
    /// Create a new validation result.
    pub fn new() -> Self {
        Self {
            valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
            summary: ValidationSummary::default(),
        }
    }

    /// Add an error.
    pub fn add_error(&mut self, error: ValidationError) {
        self.valid = false;
        self.errors.push(error);
    }

    /// Add a warning.
    pub fn add_warning(&mut self, warning: ValidationWarning) {
        self.warnings.push(warning);
    }

    /// Merge another validation result.
    pub fn merge(&mut self, other: ValidationResult) {
        self.valid = self.valid && other.valid;
        self.errors.extend(other.errors);
        self.warnings.extend(other.warnings);
    }
}

impl Default for ValidationResult {
    fn default() -> Self {
        Self::new()
    }
}

/// Validation error.
#[derive(Debug, Clone)]
pub enum ValidationError {
    /// Missing episode metadata.
    MissingEpisode { episode_index: usize },

    /// Feature spec mismatch between episodes.
    FeatureSpecMismatch {
        feature_name: String,
        expected: Box<FeatureSpec>,
        found: Box<FeatureSpec>,
    },

    /// Missing feature in episode.
    MissingFeature {
        episode_index: usize,
        feature_name: String,
    },

    /// Inconsistent frame count.
    InconsistentFrameCount {
        episode_index: usize,
        metadata_frames: usize,
        stats_frames: usize,
    },

    /// Invalid feature shape.
    InvalidFeatureShape {
        episode_index: usize,
        feature_name: String,
        shape: Vec<usize>,
    },

    /// Missing video file.
    MissingVideoFile {
        episode_index: usize,
        camera: String,
    },

    /// Missing parquet file.
    MissingParquetFile { episode_index: usize },
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationError::MissingEpisode { episode_index } => {
                write!(f, "Missing metadata for episode {}", episode_index)
            }
            ValidationError::FeatureSpecMismatch {
                feature_name,
                expected,
                found,
            } => write!(
                f,
                "Feature spec mismatch for '{}': expected {:?}, found {:?}",
                feature_name, expected, found
            ),
            ValidationError::MissingFeature {
                episode_index,
                feature_name,
            } => write!(
                f,
                "Episode {} missing feature '{}'",
                episode_index, feature_name
            ),
            ValidationError::InconsistentFrameCount {
                episode_index,
                metadata_frames,
                stats_frames,
            } => write!(
                f,
                "Episode {} has inconsistent frame count: metadata={}, stats={}",
                episode_index, metadata_frames, stats_frames
            ),
            ValidationError::InvalidFeatureShape {
                episode_index,
                feature_name,
                shape,
            } => write!(
                f,
                "Episode {} has invalid shape for feature '{}': {:?}",
                episode_index, feature_name, shape
            ),
            ValidationError::MissingVideoFile {
                episode_index,
                camera,
            } => write!(
                f,
                "Episode {} missing video file for camera '{}'",
                episode_index, camera
            ),
            ValidationError::MissingParquetFile { episode_index } => {
                write!(f, "Episode {} missing parquet file", episode_index)
            }
        }
    }
}

/// Validation warning.
#[derive(Debug, Clone)]
pub enum ValidationWarning {
    /// Empty episode (no frames).
    EmptyEpisode { episode_index: usize },

    /// Duplicate task.
    DuplicateTask { task: String, indices: Vec<usize> },

    /// Feature not present in all episodes.
    SparseFeature {
        feature_name: String,
        present_count: usize,
        total_count: usize,
    },

    /// Missing statistics for feature.
    MissingStats {
        episode_index: usize,
        feature_name: String,
    },
}

impl std::fmt::Display for ValidationWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationWarning::EmptyEpisode { episode_index } => {
                write!(f, "Episode {} is empty (no frames)", episode_index)
            }
            ValidationWarning::DuplicateTask { task, indices } => {
                write!(
                    f,
                    "Task '{}' appears at multiple indices: {:?}",
                    task, indices
                )
            }
            ValidationWarning::SparseFeature {
                feature_name,
                present_count,
                total_count,
            } => write!(
                f,
                "Feature '{}' only present in {}/{} episodes",
                feature_name, present_count, total_count
            ),
            ValidationWarning::MissingStats {
                episode_index,
                feature_name,
            } => write!(
                f,
                "Episode {} missing stats for feature '{}'",
                episode_index, feature_name
            ),
        }
    }
}

/// Validation summary statistics.
#[derive(Debug, Clone, Default)]
pub struct ValidationSummary {
    /// Total number of episodes.
    pub total_episodes: usize,

    /// Total number of frames.
    pub total_frames: usize,

    /// Number of features.
    pub feature_count: usize,

    /// Number of tasks.
    pub task_count: usize,

    /// Number of video files.
    pub video_count: usize,
}

/// Validates a collection of episode metadata.
pub struct MetadataValidator;

impl MetadataValidator {
    /// Validate a batch of episode metadata.
    ///
    /// Checks for:
    /// - Consistent feature specs across episodes
    /// - Missing episodes (gaps in sequence)
    /// - Empty episodes
    /// - Feature presence
    pub fn validate_episodes(episodes: &[PartialEpisodeMetadata]) -> ValidationResult {
        let mut result = ValidationResult::new();

        if episodes.is_empty() {
            result.add_error(ValidationError::MissingEpisode { episode_index: 0 });
            return result;
        }

        // Sort by episode index
        let mut sorted = episodes.to_vec();
        sorted.sort_by_key(|e| e.episode_index);

        // Check for gaps
        let max_index = sorted.last().map(|e| e.episode_index).unwrap_or(0);
        let expected_count = max_index + 1;

        if sorted.len() != expected_count {
            // Find missing indices
            let present: std::collections::HashSet<_> =
                sorted.iter().map(|e| e.episode_index).collect();
            for i in 0..=max_index {
                if !present.contains(&i) {
                    result.add_error(ValidationError::MissingEpisode { episode_index: i });
                }
            }
        }

        // Collect union of all features
        let mut all_features: std::collections::HashMap<String, FeatureSpec> = HashMap::new();
        for episode in &sorted {
            for (name, shape) in &episode.feature_shapes {
                let spec = shape.to_spec();
                if let Some(existing) = all_features.get(name) {
                    if !existing.is_compatible(&spec) {
                        result.add_error(ValidationError::FeatureSpecMismatch {
                            feature_name: name.clone(),
                            expected: Box::new(existing.clone()),
                            found: Box::new(spec),
                        });
                    }
                } else {
                    all_features.insert(name.clone(), spec);
                }
            }
        }

        // Check each episode
        for episode in &sorted {
            // Check for empty episode
            if episode.length == 0 {
                result.add_warning(ValidationWarning::EmptyEpisode {
                    episode_index: episode.episode_index,
                });
            }

            // Check all features are present
            for feature_name in all_features.keys() {
                if !episode.feature_shapes.contains_key(feature_name) {
                    result.add_warning(ValidationWarning::SparseFeature {
                        feature_name: feature_name.clone(),
                        present_count: sorted
                            .iter()
                            .filter(|e| e.feature_shapes.contains_key(feature_name))
                            .count(),
                        total_count: sorted.len(),
                    });
                }
            }

            // Check for stats presence
            for feature_name in episode.feature_shapes.keys() {
                if !episode.stats.contains_key(feature_name) {
                    result.add_warning(ValidationWarning::MissingStats {
                        episode_index: episode.episode_index,
                        feature_name: feature_name.clone(),
                    });
                }
            }
        }

        // Build summary
        result.summary = ValidationSummary {
            total_episodes: sorted.len(),
            total_frames: sorted.iter().map(|e| e.length).sum(),
            feature_count: all_features.len(),
            task_count: sorted
                .iter()
                .flat_map(|e| &e.tasks)
                .collect::<std::collections::HashSet<_>>()
                .len(),
            video_count: sorted.iter().map(|e| e.video_paths.len()).sum(),
        };

        result
    }

    /// Validate tasks for duplicates.
    pub fn validate_tasks(tasks: &[(usize, String)]) -> ValidationResult {
        let mut result = ValidationResult::new();
        let mut task_to_indices: HashMap<String, Vec<usize>> = HashMap::new();

        for (index, task) in tasks {
            task_to_indices
                .entry(task.clone())
                .or_default()
                .push(*index);
        }

        for (task, indices) in task_to_indices {
            if indices.len() > 1 {
                result.add_warning(ValidationWarning::DuplicateTask { task, indices });
            }
        }

        result
    }
}

/// Dataset inspector for debugging.
pub struct DatasetInspector;

impl DatasetInspector {
    /// Print a summary of the dataset.
    pub fn print_summary(episodes: &[PartialEpisodeMetadata]) {
        let total_episodes = episodes.len();
        let total_frames: usize = episodes.iter().map(|e| e.length).sum();
        let avg_frames = if total_episodes > 0 {
            total_frames / total_episodes
        } else {
            0
        };

        println!("Dataset Summary:");
        println!("  Total episodes: {}", total_episodes);
        println!("  Total frames: {}", total_frames);
        println!("  Average frames per episode: {}", avg_frames);

        if let Some(first) = episodes.first() {
            println!(
                "  Episode index range: {} - {}",
                first.episode_index,
                episodes
                    .last()
                    .map(|e| e.episode_index)
                    .unwrap_or(first.episode_index)
            );
        }

        // Feature summary
        let mut all_features: std::collections::HashSet<String> = std::collections::HashSet::new();
        for episode in episodes {
            all_features.extend(episode.feature_shapes.keys().cloned());
        }
        println!("  Features ({}):", all_features.len());
        for feature in all_features {
            let present_count = episodes
                .iter()
                .filter(|e| e.feature_shapes.contains_key(&feature))
                .count();
            println!(
                "    {}: {}/{} episodes",
                feature, present_count, total_episodes
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::types::FeatureShape;

    fn create_test_episode(index: usize, length: usize) -> PartialEpisodeMetadata {
        PartialEpisodeMetadata {
            episode_index: index,
            length,
            tasks: vec![format!("task_{}", index)],
            feature_shapes: {
                let mut shapes = HashMap::new();
                shapes.insert(
                    "observation.state".to_string(),
                    FeatureShape {
                        dtype: "float32".to_string(),
                        shape: vec![7],
                        is_video: false,
                    },
                );
                shapes
            },
            parquet_path: format!("data/episode_{:06}.parquet", index),
            video_paths: HashMap::new(),
            stats: HashMap::new(),
            recorded_at: Some(1234567890),
        }
    }

    #[test]
    fn test_validate_episodes_empty() {
        let result = MetadataValidator::validate_episodes(&[]);
        assert!(!result.valid);
        assert_eq!(result.errors.len(), 1);
    }

    #[test]
    fn test_validate_episodes_single() {
        let episodes = vec![create_test_episode(0, 100)];
        let result = MetadataValidator::validate_episodes(&episodes);
        assert!(result.valid);
        assert_eq!(result.summary.total_episodes, 1);
        assert_eq!(result.summary.total_frames, 100);
    }

    #[test]
    fn test_validate_episodes_gap() {
        let episodes = vec![
            create_test_episode(0, 100),
            create_test_episode(2, 100), // Gap at index 1
        ];
        let result = MetadataValidator::validate_episodes(&episodes);
        assert!(!result.valid);
        assert!(
            result
                .errors
                .iter()
                .any(|e| matches!(e, ValidationError::MissingEpisode { episode_index: 1 }))
        );
    }

    #[test]
    fn test_validate_episodes_empty_episode() {
        let episodes = vec![create_test_episode(0, 0)];
        let result = MetadataValidator::validate_episodes(&episodes);
        assert!(result.valid); // Still valid, just warnings
        // Empty episode generates 2 warnings: EmptyEpisode and MissingStats (for observation.state)
        assert_eq!(result.warnings.len(), 2);
        assert!(
            result
                .warnings
                .iter()
                .any(|w| matches!(w, ValidationWarning::EmptyEpisode { .. }))
        );
        assert!(
            result
                .warnings
                .iter()
                .any(|w| matches!(w, ValidationWarning::MissingStats { .. }))
        );
    }

    #[test]
    fn test_validate_tasks_duplicates() {
        let tasks = vec![
            (0, "pick up object".to_string()),
            (1, "pick up object".to_string()),
            (2, "place object".to_string()),
        ];
        let result = MetadataValidator::validate_tasks(&tasks);
        assert!(result.valid); // Warning, not error
        assert_eq!(result.warnings.len(), 1);
        assert!(matches!(
            result.warnings[0],
            ValidationWarning::DuplicateTask { .. }
        ));
    }

    #[test]
    fn test_validation_result_merge() {
        let mut result1 = ValidationResult::new();
        result1.add_warning(ValidationWarning::EmptyEpisode { episode_index: 0 });

        let mut result2 = ValidationResult::new();
        result2.add_error(ValidationError::MissingEpisode { episode_index: 1 });

        result1.merge(result2);

        assert!(!result1.valid);
        assert_eq!(result1.errors.len(), 1);
        assert_eq!(result1.warnings.len(), 1);
    }

    #[test]
    fn test_feature_spec_mismatch() {
        let mut episode1 = create_test_episode(0, 100);
        episode1.feature_shapes.insert(
            "action".to_string(),
            FeatureShape {
                dtype: "float32".to_string(),
                shape: vec![7],
                is_video: false,
            },
        );

        let mut episode2 = create_test_episode(1, 100);
        episode2.feature_shapes.insert(
            "action".to_string(),
            FeatureShape {
                dtype: "float32".to_string(),
                shape: vec![14], // Different shape!
                is_video: false,
            },
        );

        let episodes = vec![episode1, episode2];
        let result = MetadataValidator::validate_episodes(&episodes);
        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| matches!(e,
            ValidationError::FeatureSpecMismatch { feature_name, .. } if feature_name == "action"
        )));
    }
}
