// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Base traits and types for Parquet-based dataset writers.
//!
//! Provides common functionality for writing datasets in Parquet format,
//! shared by KPS, LeRobot, and other formats.

use std::path::Path;

use serde::Serialize;

use crate::core::Result;

// Re-export common ImageData from base module
pub use super::base::ImageData;

/// Frame data for Parquet writing.
pub trait FrameData {
    /// Get episode index.
    fn episode_index(&self) -> usize;
    /// Get frame index within episode.
    fn frame_index(&self) -> usize;
    /// Get global frame index.
    fn index(&self) -> usize;
    /// Get timestamp in seconds.
    fn timestamp(&self) -> f64;
}

/// Base trait for Parquet dataset writers.
pub trait ParquetWriterBase {
    /// Write a batch of frames to a Parquet file.
    fn write_parquet_file(
        &mut self,
        output_path: &Path,
        frames: &[Box<dyn FrameData>],
    ) -> Result<()>;
}

/// Statistics for a feature.
#[derive(Debug, Clone, Serialize)]
pub struct FeatureStats {
    pub min: Vec<f32>,
    pub max: Vec<f32>,
    pub mean: Vec<f32>,
    pub std: Vec<f32>,
}

/// Calculate statistics for a vector of float values.
pub fn calculate_stats(values: &[Vec<f32>]) -> Option<FeatureStats> {
    if values.is_empty() || values[0].is_empty() {
        return None;
    }

    let dim = values[0].len();
    let mut min = vec![f32::INFINITY; dim];
    let mut max = vec![f32::NEG_INFINITY; dim];
    let mut sum = vec![0.0f32; dim];
    let mut sum_sq = vec![0.0f32; dim];

    for row in values {
        for (i, &val) in row.iter().enumerate() {
            if i < dim {
                min[i] = min[i].min(val);
                max[i] = max[i].max(val);
                sum[i] += val;
                sum_sq[i] += val * val;
            }
        }
    }

    let n = values.len() as f32;
    let mean: Vec<f32> = sum.iter().map(|&s| s / n).collect();
    let std: Vec<f32> = (0..dim)
        .map(|i| {
            let variance = (sum_sq[i] - (sum[i] * sum[i]) / n) / n;
            variance.sqrt().max(0.0)
        })
        .collect();

    Some(FeatureStats { min, max, mean, std })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_stats() {
        let values = vec![
            vec![1.0, 2.0, 3.0],
            vec![2.0, 3.0, 4.0],
            vec![3.0, 4.0, 5.0],
        ];

        let stats = calculate_stats(&values).unwrap();
        assert_eq!(stats.min, vec![1.0, 2.0, 3.0]);
        assert_eq!(stats.max, vec![3.0, 4.0, 5.0]);
        assert_eq!(stats.mean, vec![2.0, 3.0, 4.0]);
    }
}
