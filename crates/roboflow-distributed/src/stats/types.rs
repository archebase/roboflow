// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Statistics types for episode data collection.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Feature-level statistics compatible with LeRobot format.
///
/// Contains min, max, mean, and standard deviation for each dimension
/// of a feature (e.g., observation.state with 7 dimensions).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FeatureStats {
    /// Minimum value per dimension.
    pub min: Vec<f32>,
    /// Maximum value per dimension.
    pub max: Vec<f32>,
    /// Mean value per dimension.
    pub mean: Vec<f32>,
    /// Standard deviation per dimension.
    pub std: Vec<f32>,
}

impl FeatureStats {
    /// Create empty feature stats with given dimension.
    pub fn empty(dim: usize) -> Self {
        Self {
            min: vec![0.0; dim],
            max: vec![0.0; dim],
            mean: vec![0.0; dim],
            std: vec![0.0; dim],
        }
    }

    /// Get the dimension of this feature.
    pub fn dim(&self) -> usize {
        self.min.len()
    }
}

/// Complete statistics for a single episode.
///
/// This represents all statistics collected during processing of one
/// bag/MCAP file (one episode in LeRobot format).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpisodeStats {
    /// Episode index (global across the batch).
    pub episode_index: usize,

    /// Number of frames in this episode.
    pub frame_count: usize,

    /// Statistics per feature (e.g., "observation.state", "action").
    pub feature_stats: HashMap<String, FeatureStats>,

    /// Task indices for this episode.
    #[serde(default)]
    pub task_indices: Vec<usize>,

    /// Timestamp when stats were recorded (for debugging).
    #[serde(default)]
    pub recorded_at: Option<i64>,
}

impl EpisodeStats {
    /// Create new episode stats.
    pub fn new(episode_index: usize, frame_count: usize) -> Self {
        Self {
            episode_index,
            frame_count,
            feature_stats: HashMap::new(),
            task_indices: Vec::new(),
            recorded_at: Some(chrono::Utc::now().timestamp()),
        }
    }

    /// Add feature statistics.
    ///
    /// Note: Zero-dimensional features are rejected as they cannot be
    /// meaningfully aggregated.
    pub fn add_feature(&mut self, name: String, stats: FeatureStats) -> bool {
        if stats.dim() == 0 {
            tracing::warn!(
                feature = %name,
                episode_index = self.episode_index,
                "Rejecting zero-dimensional feature stats"
            );
            return false;
        }
        self.feature_stats.insert(name, stats);
        true
    }
}

/// Aggregated statistics for an entire batch.
///
/// Contains per-episode stats and globally aggregated statistics
/// for inclusion in LeRobot's info.json.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchStatsSummary {
    /// Batch ID.
    pub batch_id: String,

    /// Total episodes in this batch.
    pub total_episodes: usize,

    /// Total frames across all episodes.
    pub total_frames: usize,

    /// Per-episode stats indexed by episode_index.
    pub episodes: HashMap<usize, EpisodeStats>,

    /// Global aggregated stats across all episodes.
    /// Key is feature name (e.g., "observation.state").
    pub global_stats: HashMap<String, FeatureStats>,
}

impl BatchStatsSummary {
    /// Create an empty summary for a batch.
    pub fn new(batch_id: String) -> Self {
        Self {
            batch_id,
            total_episodes: 0,
            total_frames: 0,
            episodes: HashMap::new(),
            global_stats: HashMap::new(),
        }
    }

    /// Add episode stats to the summary.
    pub fn add_episode(&mut self, stats: EpisodeStats) {
        self.total_frames += stats.frame_count;
        self.total_episodes += 1;
        self.episodes.insert(stats.episode_index, stats);
    }

    /// Calculate global statistics from all episodes.
    ///
    /// Uses parallel Welford's algorithm for numerical stability,
    /// properly weighting by frame counts from each episode.
    pub fn calculate_global_stats(&mut self) {
        self.global_stats.clear();

        // Collect all feature values per feature name with their frame counts
        let mut feature_values: HashMap<String, Vec<(usize, &FeatureStats)>> = HashMap::new();

        for episode in self.episodes.values() {
            let frame_count = episode.frame_count;
            for (feature_name, stats) in &episode.feature_stats {
                feature_values
                    .entry(feature_name.clone())
                    .or_default()
                    .push((frame_count, stats));
            }
        }

        // Aggregate each feature
        for (feature_name, stats_with_counts) in feature_values {
            if let Some(aggregated) = Self::aggregate_feature_stats(&stats_with_counts) {
                self.global_stats.insert(feature_name, aggregated);
            }
        }
    }

    /// Aggregate feature stats across multiple episodes using parallel Welford's algorithm.
    ///
    /// Each episode contributes (frame_count, stats) to properly weight the aggregation.
    /// The parallel Welford algorithm combines aggregates as:
    /// - count = count_a + count_b
    /// - delta = mean_b - mean_a
    /// - mean = mean_a + delta * count_b / count
    /// - M2 = M2_a + M2_b + delta^2 * count_a * count_b / count
    fn aggregate_feature_stats(
        stats_with_counts: &[(usize, &FeatureStats)],
    ) -> Option<FeatureStats> {
        if stats_with_counts.is_empty() {
            return None;
        }

        let dim = stats_with_counts[0].1.dim();
        if dim == 0 {
            return None;
        }

        // Validate all episodes have consistent dimensions
        for (idx, (_, stats)) in stats_with_counts.iter().enumerate() {
            if stats.dim() != dim {
                tracing::warn!(
                    expected_dim = dim,
                    actual_dim = stats.dim(),
                    episode_idx = idx,
                    "Inconsistent feature dimensions, skipping aggregation"
                );
                return None;
            }
        }

        // Initialize with first episode's stats
        let (first_count, first_stats) = stats_with_counts[0];
        let mut global_min = first_stats.min.clone();
        let mut global_max = first_stats.max.clone();

        // For parallel Welford's algorithm
        let mut total_count: usize = first_count;
        let mut mean = first_stats.mean.clone();
        // Note: We approximate M2 from the episode's std since we don't have raw data
        // M2 = variance * (n-1) for sample variance, or variance * n for population
        // Using sample variance convention: M2 = std^2 * (count - 1)
        let mut m2: Vec<f32> = first_stats
            .std
            .iter()
            .map(|&s| {
                if first_count > 1 {
                    s * s * (first_count - 1) as f32
                } else {
                    0.0
                }
            })
            .collect();

        // Process remaining episodes using parallel Welford
        for (frame_count, stats) in stats_with_counts.iter().skip(1) {
            let count_b = *frame_count;
            let count_a = total_count;
            total_count = count_a + count_b;

            // Update min/max directly
            for i in 0..dim {
                global_min[i] = global_min[i].min(stats.min[i]);
                global_max[i] = global_max[i].max(stats.max[i]);
            }

            // Parallel Welford's algorithm for combining aggregates
            // M2_b from episode's std
            let m2_b: Vec<f32> = stats
                .std
                .iter()
                .map(|&s| {
                    if count_b > 1 {
                        s * s * (count_b - 1) as f32
                    } else {
                        0.0
                    }
                })
                .collect();

            for i in 0..dim {
                let delta = stats.mean[i] - mean[i];
                mean[i] += delta * count_b as f32 / total_count as f32;
                // Parallel Welford: M2 = M2_a + M2_b + delta^2 * count_a * count_b / count
                m2[i] +=
                    m2_b[i] + delta * delta * count_a as f32 * count_b as f32 / total_count as f32;
            }
        }

        // Calculate standard deviation (sample variance)
        let std: Vec<f32> = m2
            .iter()
            .map(|&m2_val| {
                if total_count > 1 {
                    (m2_val / (total_count - 1) as f32).sqrt()
                } else {
                    0.0
                }
            })
            .collect();

        Some(FeatureStats {
            min: global_min,
            max: global_max,
            mean,
            std,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_feature_stats_creation() {
        let stats = FeatureStats::empty(7);
        assert_eq!(stats.dim(), 7);
        assert_eq!(stats.min.len(), 7);
    }

    #[test]
    fn test_episode_stats_creation() {
        let mut stats = EpisodeStats::new(0, 100);
        assert_eq!(stats.episode_index, 0);
        assert_eq!(stats.frame_count, 100);
        assert!(stats.feature_stats.is_empty());

        let added = stats.add_feature("observation.state".to_string(), FeatureStats::empty(7));
        assert!(added);
        assert_eq!(stats.feature_stats.len(), 1);

        // Zero-dimensional features should be rejected
        let rejected = stats.add_feature("bad.feature".to_string(), FeatureStats::empty(0));
        assert!(!rejected);
        assert_eq!(stats.feature_stats.len(), 1); // Still just 1
    }

    #[test]
    fn test_batch_stats_summary() {
        let mut summary = BatchStatsSummary::new("batch-123".to_string());

        let mut ep1 = EpisodeStats::new(0, 100);
        ep1.add_feature(
            "observation.state".to_string(),
            FeatureStats {
                min: vec![0.0],
                max: vec![10.0],
                mean: vec![5.0],
                std: vec![1.0],
            },
        );

        let mut ep2 = EpisodeStats::new(1, 200);
        ep2.add_feature(
            "observation.state".to_string(),
            FeatureStats {
                min: vec![1.0],
                max: vec![15.0],
                mean: vec![8.0],
                std: vec![2.0],
            },
        );

        summary.add_episode(ep1);
        summary.add_episode(ep2);

        assert_eq!(summary.total_episodes, 2);
        assert_eq!(summary.total_frames, 300);

        summary.calculate_global_stats();
        assert!(summary.global_stats.contains_key("observation.state"));
    }

    #[test]
    fn test_weighted_mean_aggregation() {
        // Test that weighted mean is correctly computed
        // Episode 1: 1000 frames with mean=5.0
        // Episode 2: 10 frames with mean=10.0
        // Weighted mean should be (1000*5 + 10*10) / 1010 ≈ 5.05
        let mut summary = BatchStatsSummary::new("test-weighted".to_string());

        let mut ep1 = EpisodeStats::new(0, 1000);
        ep1.add_feature(
            "test.feature".to_string(),
            FeatureStats {
                min: vec![0.0],
                max: vec![10.0],
                mean: vec![5.0],
                std: vec![0.0], // No variance within episode for this test
            },
        );

        let mut ep2 = EpisodeStats::new(1, 10);
        ep2.add_feature(
            "test.feature".to_string(),
            FeatureStats {
                min: vec![0.0],
                max: vec![10.0],
                mean: vec![10.0],
                std: vec![0.0],
            },
        );

        summary.add_episode(ep1);
        summary.add_episode(ep2);

        summary.calculate_global_stats();

        let global = summary.global_stats.get("test.feature").unwrap();
        // Weighted mean: (1000*5 + 10*10) / 1010 = 5100 / 1010 ≈ 5.0495
        let expected_mean = (1000.0 * 5.0 + 10.0 * 10.0) / 1010.0;
        assert!(
            (global.mean[0] - expected_mean).abs() < 0.01,
            "Expected mean ~{}, got {}",
            expected_mean,
            global.mean[0]
        );
    }
}
