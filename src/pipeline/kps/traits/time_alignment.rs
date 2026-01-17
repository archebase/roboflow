//! Time alignment strategies for resampling robotics data to target FPS.
//!
//! This module defines the trait and implementations for different time alignment
//! strategies used when converting irregular timestamped MCAP data to fixed-FPS
//! Kps datasets.

use crate::core::Result;

/// Error type for time alignment operations.
#[derive(Debug, thiserror::Error)]
pub enum TimeAlignError {
    #[error("No data available for alignment")]
    NoData,

    #[error("Timestamps are not sorted")]
    UnsortedTimestamps,

    #[error("Gap too large for interpolation: {0}ns")]
    GapTooLarge(u64),

    #[error("Invalid target FPS: {0}")]
    InvalidFps(u32),

    #[error("Insufficient data for interpolation")]
    InsufficientData,
}

/// Strategy for aligning and resampling timestamped data to a target FPS.
///
/// Implementations define how to interpolate between samples when generating
/// frames at fixed time intervals.
pub trait TimeAlignmentStrategy: Send + Sync {
    /// Calculate target timestamps from the original data bounds.
    ///
    /// Given the original timestamp range, generates evenly spaced timestamps
    /// at the target FPS.
    fn generate_target_timestamps(
        &self,
        start_time: u64,
        end_time: u64,
        target_fps: u32,
    ) -> Result<Vec<u64>> {
        if target_fps == 0 {
            return Err(TimeAlignError::InvalidFps(target_fps).into());
        }

        let frame_duration_ns = 1_000_000_000u64 / target_fps as u64;
        let duration_ns = end_time.saturating_sub(start_time);
        let num_frames = (duration_ns / frame_duration_ns) + 1;

        let target_times: Vec<u64> = (0..num_frames)
            .map(|i| start_time.saturating_add(i * frame_duration_ns))
            .collect();

        Ok(target_times)
    }

    /// Find the source indices and weights for interpolating to a target timestamp.
    ///
    /// Returns a vector of (index, weight) pairs. The weights sum to 1.0.
    /// For nearest-neighbor, returns a single pair with weight 1.0.
    /// For linear interpolation, returns two pairs with weights based on temporal distance.
    fn interpolation_weights(
        &self,
        target_time: u64,
        source_times: &[u64],
        max_gap_ns: u64,
    ) -> Result<Vec<(usize, f64)>>;

    /// Determine if a gap is too large to interpolate across.
    fn is_gap_too_large(&self, gap_ns: u64, max_gap_ns: u64) -> bool {
        gap_ns > max_gap_ns
    }
}

/// Linear interpolation between neighboring samples.
///
/// For each target timestamp, finds the two surrounding source timestamps
/// and computes a weighted average based on temporal distance.
#[derive(Debug, Clone, Default)]
pub struct LinearInterpolation {
    /// Maximum gap (in nanoseconds) to allow interpolation.
    /// If gap exceeds this, the last value is held instead.
    pub max_gap_ns: u64,
}

impl LinearInterpolation {
    /// Create a new linear interpolation strategy.
    pub fn new() -> Self {
        Self {
            max_gap_ns: 100_000_000,
        } // Default 100ms
    }

    /// Set the maximum gap for interpolation.
    pub fn with_max_gap_ns(mut self, gap_ns: u64) -> Self {
        self.max_gap_ns = gap_ns;
        self
    }
}

impl TimeAlignmentStrategy for LinearInterpolation {
    fn interpolation_weights(
        &self,
        target_time: u64,
        source_times: &[u64],
        max_gap_ns: u64,
    ) -> Result<Vec<(usize, f64)>> {
        if source_times.is_empty() {
            return Err(TimeAlignError::NoData.into());
        }

        // Find the position where target_time would be inserted
        let pos = source_times.partition_point(|&t| t <= target_time);

        match pos {
            0 => {
                // Target is before all source times - use first
                Ok(vec![(0, 1.0)])
            }
            n if n >= source_times.len() => {
                // Target is after all source times - use last
                Ok(vec![(source_times.len() - 1, 1.0)])
            }
            _ => {
                // Target is between pos-1 and pos
                let t0 = source_times[pos - 1];
                let t1 = source_times[pos];
                let gap = t1.saturating_sub(t0);

                // Check if gap is too large
                if self.is_gap_too_large(gap, max_gap_ns) {
                    // Use nearest (closer of the two)
                    let dist_to_t0 = target_time.saturating_sub(t0);
                    let dist_to_t1 = t1.saturating_sub(target_time);
                    let idx = if dist_to_t0 <= dist_to_t1 {
                        pos - 1
                    } else {
                        pos
                    };
                    return Ok(vec![(idx, 1.0)]);
                }

                // Linear interpolation weights
                let dist_to_t0 = target_time.saturating_sub(t0);
                let total_dist = t1.saturating_sub(t0);

                if total_dist == 0 {
                    // Same timestamp - equal weight
                    Ok(vec![(pos - 1, 0.5), (pos, 0.5)])
                } else {
                    let w0 = 1.0 - (dist_to_t0 as f64 / total_dist as f64);
                    let w1 = 1.0 - w0;
                    Ok(vec![(pos - 1, w0), (pos, w1)])
                }
            }
        }
    }
}

/// Hold last known value (zero-order hold).
///
/// For each target timestamp, uses the most recent source value.
/// This is useful for discrete actions that should not be interpolated.
#[derive(Debug, Clone, Default)]
pub struct HoldLastValue {
    /// Maximum duration (in nanoseconds) to hold a value.
    /// If no data within this window, returns an error.
    pub max_hold_ns: u64,
}

impl HoldLastValue {
    /// Create a new hold-last-value strategy.
    pub fn new() -> Self {
        Self {
            max_hold_ns: 500_000_000,
        } // Default 500ms
    }

    /// Set the maximum hold duration.
    pub fn with_max_hold_ns(mut self, hold_ns: u64) -> Self {
        self.max_hold_ns = hold_ns;
        self
    }
}

impl TimeAlignmentStrategy for HoldLastValue {
    fn interpolation_weights(
        &self,
        target_time: u64,
        source_times: &[u64],
        _max_gap_ns: u64,
    ) -> Result<Vec<(usize, f64)>> {
        if source_times.is_empty() {
            return Err(TimeAlignError::NoData.into());
        }

        // Find the most recent timestamp
        let pos = source_times.partition_point(|&t| t <= target_time);

        let idx = if pos == 0 {
            0
        } else if pos >= source_times.len() {
            source_times.len() - 1
        } else {
            pos - 1
        };

        // Check if the held value is too old
        let last_time = source_times[idx];
        let age = target_time.saturating_sub(last_time);

        if age > self.max_hold_ns {
            return Err(TimeAlignError::GapTooLarge(age).into());
        }

        Ok(vec![(idx, 1.0)])
    }
}

/// Nearest neighbor selection.
///
/// For each target timestamp, selects the closest source sample in time.
#[derive(Debug, Clone, Default)]
pub struct NearestNeighbor {
    /// Maximum distance (in nanoseconds) to consider a sample valid.
    pub tolerance_ns: u64,
}

impl NearestNeighbor {
    /// Create a new nearest-neighbor strategy.
    pub fn new() -> Self {
        Self {
            tolerance_ns: 33_333_333,
        } // Default ~1 frame at 30fps
    }

    /// Set the tolerance for nearest neighbor selection.
    pub fn with_tolerance_ns(mut self, tolerance_ns: u64) -> Self {
        self.tolerance_ns = tolerance_ns;
        self
    }
}

impl TimeAlignmentStrategy for NearestNeighbor {
    fn interpolation_weights(
        &self,
        target_time: u64,
        source_times: &[u64],
        _max_gap_ns: u64,
    ) -> Result<Vec<(usize, f64)>> {
        if source_times.is_empty() {
            return Err(TimeAlignError::NoData.into());
        }

        // Find the closest timestamp
        let mut best_idx = 0;
        let mut best_dist = u64::MAX;

        for (i, &t) in source_times.iter().enumerate() {
            let dist = if t > target_time {
                t.saturating_sub(target_time)
            } else {
                target_time.saturating_sub(t)
            };

            if dist < best_dist {
                best_dist = dist;
                best_idx = i;
            }
        }

        // Check if within tolerance
        if best_dist > self.tolerance_ns {
            return Err(TimeAlignError::GapTooLarge(best_dist).into());
        }

        Ok(vec![(best_idx, 1.0)])
    }
}

/// Configuration for time alignment.
#[derive(Debug, Clone)]
pub struct TimeAlignerConfig {
    /// Target frames per second for output.
    pub target_fps: u32,

    /// Which interpolation strategy to use.
    pub strategy: TimeAlignmentStrategyType,

    /// Maximum gap for state interpolation (nanoseconds).
    pub state_interpolation_max_gap_ns: u64,

    /// Maximum distance for image synchronization (nanoseconds).
    /// Images outside this window won't be associated with a frame.
    pub image_sync_tolerance_ns: u64,
}

impl Default for TimeAlignerConfig {
    fn default() -> Self {
        Self {
            target_fps: 30,
            strategy: TimeAlignmentStrategyType::LinearInterpolation,
            state_interpolation_max_gap_ns: 100_000_000, // 100ms
            image_sync_tolerance_ns: 33_333_333,         // ~1 frame at 30fps
        }
    }
}

/// Available time alignment strategies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeAlignmentStrategyType {
    /// Linear interpolation between neighboring samples.
    LinearInterpolation,

    /// Hold last known value.
    HoldLastValue,

    /// Use nearest neighbor sample.
    NearestNeighbor,
}

impl TimeAlignmentStrategyType {
    /// Create a strategy instance from this type.
    pub fn create(&self) -> Box<dyn TimeAlignmentStrategy> {
        match self {
            Self::LinearInterpolation => Box::new(LinearInterpolation::new()),
            Self::HoldLastValue => Box::new(HoldLastValue::new()),
            Self::NearestNeighbor => Box::new(NearestNeighbor::new()),
        }
    }
}

/// Temporal buffer for storing timestamped values.
///
/// Used during time alignment to hold source data while computing
/// interpolated values at target timestamps.
#[derive(Debug, Clone)]
pub struct TemporalBuffer<T> {
    /// Timestamps for each value.
    timestamps: Vec<u64>,

    /// The values.
    values: Vec<T>,

    /// Maximum number of entries to buffer.
    max_size: usize,
}

impl<T: Clone> TemporalBuffer<T> {
    /// Create a new temporal buffer.
    pub fn new(max_size: usize) -> Self {
        Self {
            timestamps: Vec::with_capacity(max_size),
            values: Vec::with_capacity(max_size),
            max_size,
        }
    }

    /// Insert a timestamped value.
    ///
    /// Returns false if the buffer is full.
    pub fn insert(&mut self, timestamp: u64, value: T) -> bool {
        if self.timestamps.len() >= self.max_size {
            return false;
        }

        // Find insertion point to maintain sorted order
        let pos = self.timestamps.partition_point(|&t| t <= timestamp);
        self.timestamps.insert(pos, timestamp);
        self.values.insert(pos, value);
        true
    }

    /// Get all values within a time window around a target timestamp.
    pub fn get_window(&self, target_time: u64, window_ns: u64) -> Vec<(u64, &T)> {
        let start = target_time.saturating_sub(window_ns);
        let end = target_time.saturating_add(window_ns);

        self.timestamps
            .iter()
            .zip(self.values.iter())
            .filter(|(t, _)| **t >= start && **t <= end)
            .map(|(t, v)| (*t, v))
            .collect()
    }

    /// Get the most recent value before or at the target timestamp.
    pub fn get_at_or_before(&self, target_time: u64) -> Option<(u64, &T)> {
        let pos = self.timestamps.partition_point(|&t| t <= target_time);

        if pos == 0 {
            None
        } else {
            let idx = pos - 1;
            self.timestamps
                .get(idx)
                .zip(self.values.get(idx))
                .map(|(t, v)| (*t, v))
        }
    }

    /// Remove all data before the given timestamp.
    pub fn prune_before(&mut self, timestamp: u64) {
        let pos = self.timestamps.partition_point(|&t| t < timestamp);
        self.timestamps.drain(0..pos);
        self.values.drain(0..pos);
    }

    /// Clear all data.
    pub fn clear(&mut self) {
        self.timestamps.clear();
        self.values.clear();
    }

    /// Get the number of entries.
    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    /// Check if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.timestamps.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linear_interpolation_midpoint() {
        let strategy = LinearInterpolation::new();
        let source_times = vec![0, 100_000_000]; // 0ms, 100ms
        let target_time = 50_000_000; // 50ms

        let weights = strategy
            .interpolation_weights(target_time, &source_times, 200_000_000)
            .unwrap();

        assert_eq!(weights.len(), 2);
        assert!((weights[0].1 - 0.5).abs() < 0.001);
        assert!((weights[1].1 - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_linear_interpolation_before_all() {
        let strategy = LinearInterpolation::new();
        let source_times = vec![100_000_000, 200_000_000];
        let target_time = 50_000_000;

        let weights = strategy
            .interpolation_weights(target_time, &source_times, 200_000_000)
            .unwrap();

        assert_eq!(weights.len(), 1);
        assert_eq!(weights[0].0, 0);
        assert_eq!(weights[0].1, 1.0);
    }

    #[test]
    fn test_hold_last_value() {
        let strategy = HoldLastValue::new();
        let source_times = vec![0, 50_000_000, 100_000_000];
        let target_time = 75_000_000;

        let weights = strategy
            .interpolation_weights(target_time, &source_times, 0)
            .unwrap();

        assert_eq!(weights.len(), 1);
        assert_eq!(weights[0].0, 1); // Index of 50ms (last value at or before 75ms)
        assert_eq!(weights[0].1, 1.0);
    }

    #[test]
    fn test_nearest_neighbor() {
        let strategy = NearestNeighbor::new();
        let source_times = vec![0, 50_000_000, 100_000_000];
        let target_time = 65_000_000;

        let weights = strategy
            .interpolation_weights(target_time, &source_times, 0)
            .unwrap();

        assert_eq!(weights.len(), 1);
        assert_eq!(weights[0].0, 1); // Index of 50ms (closest to 65ms, distance 15ms vs 35ms)
    }

    #[test]
    fn test_generate_target_timestamps() {
        let strategy = LinearInterpolation::new();
        let target_times = strategy
            .generate_target_timestamps(0, 100_000_000, 10)
            .unwrap();

        // At 10fps, each frame is 100ms. For 100ms duration, we get 2 frames: 0ms and 100ms
        assert_eq!(target_times.len(), 2);
        assert_eq!(target_times[0], 0);
        assert_eq!(target_times[1], 100_000_000);
    }

    #[test]
    fn test_temporal_buffer() {
        let mut buffer = TemporalBuffer::<f32>::new(10);

        assert!(buffer.insert(100, 1.0));
        assert!(buffer.insert(200, 2.0));
        assert!(buffer.insert(50, 0.5)); // Should be inserted in sorted order

        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.timestamps, vec![50, 100, 200]);
        assert_eq!(buffer.values, vec![0.5, 1.0, 2.0]);

        // Get at or before
        let result = buffer.get_at_or_before(150);
        assert_eq!(result, Some((100, &1.0)));

        // Get window
        let window = buffer.get_window(100, 75);
        // Window is [25, 175], so includes 50 and 100 but not 200
        assert_eq!(window.len(), 2);
    }

    // Additional tests for comprehensive coverage

    #[test]
    fn test_linear_interpolation_exact_match() {
        let strategy = LinearInterpolation::new();
        let source_times = vec![0, 50_000_000, 100_000_000];
        let target_time = 50_000_000; // Exact match

        let weights = strategy
            .interpolation_weights(target_time, &source_times, 200_000_000)
            .unwrap();

        // When exact match, returns equal weights to adjacent elements
        assert_eq!(weights.len(), 2);
        assert_eq!(weights[0].0, 1); // Index of 50ms (pos-1)
        assert_eq!(weights[1].0, 2); // Index of 50ms (pos)
    }

    #[test]
    fn test_linear_interpolation_after_all() {
        let strategy = LinearInterpolation::new();
        let source_times = vec![0, 50_000_000, 100_000_000];
        let target_time = 150_000_000;

        let weights = strategy
            .interpolation_weights(target_time, &source_times, 200_000_000)
            .unwrap();

        assert_eq!(weights.len(), 1);
        assert_eq!(weights[0].0, 2); // Index of 100ms (last value)
        assert_eq!(weights[0].1, 1.0);
    }

    #[test]
    fn test_linear_interpolation_max_gap_exceeded() {
        // When gap exceeds max_gap, the strategy falls back to nearest neighbor
        let strategy = LinearInterpolation::new().with_max_gap_ns(30_000_000);
        let source_times = vec![0, 100_000_000]; // 100ms gap
        let target_time = 50_000_000;

        // With max_gap set to 30ms, gap of 100ms should cause nearest selection
        let weights = strategy
            .interpolation_weights(target_time, &source_times, 30_000_000)
            .unwrap();

        // Should use nearest instead of linear interpolation
        assert_eq!(weights.len(), 1);
    }

    #[test]
    fn test_hold_last_value_before_first() {
        let strategy = HoldLastValue::new();
        let source_times = vec![50_000_000, 100_000_000];
        let target_time = 25_000_000;

        let weights = strategy
            .interpolation_weights(target_time, &source_times, 0)
            .unwrap();

        assert_eq!(weights.len(), 1);
        assert_eq!(weights[0].0, 0); // Index of first value
        assert_eq!(weights[0].1, 1.0);
    }

    #[test]
    fn test_hold_last_value_after_last() {
        let strategy = HoldLastValue::new();
        let source_times = vec![50_000_000, 100_000_000];
        let target_time = 125_000_000;

        let weights = strategy
            .interpolation_weights(target_time, &source_times, 0)
            .unwrap();

        assert_eq!(weights.len(), 1);
        assert_eq!(weights[0].0, 1); // Index of last value
        assert_eq!(weights[0].1, 1.0);
    }

    #[test]
    fn test_nearest_neighbor_exact_match() {
        let strategy = NearestNeighbor::new();
        let source_times = vec![0, 50_000_000, 100_000_000];
        let target_time = 50_000_000;

        let weights = strategy
            .interpolation_weights(target_time, &source_times, 0)
            .unwrap();

        assert_eq!(weights.len(), 1);
        assert_eq!(weights[0].0, 1); // Index of 50ms
        assert_eq!(weights[0].1, 1.0);
    }

    #[test]
    fn test_nearest_neighbor_midpoint_tie() {
        let strategy = NearestNeighbor::new().with_tolerance_ns(100_000_000); // 100ms tolerance
        let source_times = vec![0, 100_000_000];
        let target_time = 50_000_000; // Exactly halfway

        let weights = strategy
            .interpolation_weights(target_time, &source_times, 0)
            .unwrap();

        // Should prefer the earlier value when equidistant (found first in iteration)
        assert_eq!(weights.len(), 1);
        assert_eq!(weights[0].0, 0); // Index of 0ms
    }

    #[test]
    fn test_generate_target_timestamps_single_frame() {
        let strategy = LinearInterpolation::new();
        let target_times = strategy
            .generate_target_timestamps(0, 33_333_333, 30) // ~1 frame at 30fps
            .unwrap();

        assert_eq!(target_times.len(), 2); // Start and end
        assert_eq!(target_times[0], 0);
        assert_eq!(target_times[1], 33_333_333);
    }

    #[test]
    fn test_generate_target_timestamps_high_fps() {
        let strategy = LinearInterpolation::new();
        let target_times = strategy
            .generate_target_timestamps(0, 1_000_000_000, 60) // 1 second at 60fps
            .unwrap();

        // 1 second / (1/60) = 60 frames, plus 1 for starting frame
        assert_eq!(target_times.len(), 61); // 0 to 60 inclusive
    }

    #[test]
    fn test_temporal_buffer_full() {
        let mut buffer = TemporalBuffer::<f32>::new(3);

        assert!(buffer.insert(100, 1.0));
        assert!(buffer.insert(200, 2.0));
        assert!(buffer.insert(300, 3.0));
        assert!(!buffer.insert(400, 4.0)); // Buffer full

        assert_eq!(buffer.len(), 3);
    }

    #[test]
    fn test_temporal_buffer_clear() {
        let mut buffer = TemporalBuffer::<f32>::new(10);

        buffer.insert(100, 1.0);
        buffer.insert(200, 2.0);
        assert_eq!(buffer.len(), 2);

        buffer.clear();
        assert_eq!(buffer.len(), 0);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_temporal_buffer_get_at_or_before_empty() {
        let buffer = TemporalBuffer::<f32>::new(10);
        assert_eq!(buffer.get_at_or_before(100), None);
    }

    #[test]
    fn test_temporal_buffer_get_at_or_before_exact() {
        let mut buffer = TemporalBuffer::<f32>::new(10);
        buffer.insert(100, 1.0);
        buffer.insert(200, 2.0);

        let result = buffer.get_at_or_before(200);
        assert_eq!(result, Some((200, &2.0)));
    }

    #[test]
    fn test_temporal_buffer_get_window_empty() {
        let buffer = TemporalBuffer::<f32>::new(10);
        let window = buffer.get_window(100, 50);
        assert_eq!(window.len(), 0);
    }

    #[test]
    fn test_temporal_buffer_get_window_partial() {
        let mut buffer = TemporalBuffer::<f32>::new(10);
        buffer.insert(100, 1.0);
        buffer.insert(200, 2.0);
        buffer.insert(300, 3.0);

        // Window that only includes the middle value
        let window = buffer.get_window(200, 25);
        assert_eq!(window.len(), 1);
        assert_eq!(window[0].0, 200);
    }

    #[test]
    fn test_time_aligner_config_default() {
        let config = TimeAlignerConfig::default();
        assert_eq!(config.target_fps, 30);
        assert_eq!(
            config.strategy,
            TimeAlignmentStrategyType::LinearInterpolation
        );
        assert_eq!(config.state_interpolation_max_gap_ns, 100_000_000);
        assert_eq!(config.image_sync_tolerance_ns, 33_333_333);
    }

    #[test]
    fn test_strategy_type_create() {
        let linear = TimeAlignmentStrategyType::LinearInterpolation.create();
        let hold = TimeAlignmentStrategyType::HoldLastValue.create();
        let nearest = TimeAlignmentStrategyType::NearestNeighbor.create();

        // Each strategy should generate timestamps
        let times = linear
            .generate_target_timestamps(0, 100_000_000, 10)
            .unwrap();
        assert!(!times.is_empty());

        let times = hold.generate_target_timestamps(0, 100_000_000, 10).unwrap();
        assert!(!times.is_empty());

        let times = nearest
            .generate_target_timestamps(0, 100_000_000, 10)
            .unwrap();
        assert!(!times.is_empty());
    }

    #[test]
    fn test_interpolation_with_negative_timestamp() {
        // Test behavior when target equals the first source timestamp
        let strategy = LinearInterpolation::new();
        let source_times = vec![0, 100_000_000];

        // Target at first timestamp - returns 2 weights with first having weight 1.0
        let weights = strategy
            .interpolation_weights(0, &source_times, 200_000_000)
            .unwrap();

        // Returns both indices but first has weight 1.0
        assert_eq!(weights.len(), 2);
        assert_eq!(weights[0].0, 0);
        assert_eq!(weights[0].1, 1.0); // First element has full weight
    }
}
