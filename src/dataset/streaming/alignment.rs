// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Frame alignment with bounded memory footprint.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Instant;

use crate::dataset::common::AlignedFrame;
use crate::dataset::streaming::completion::FrameCompletionCriteria;
use crate::dataset::streaming::config::StreamingConfig;
use crate::dataset::streaming::stats::AlignmentStats;

/// A partially complete frame waiting for more messages.
///
/// Tracks which features have been received and when the frame
/// is eligible for forced completion.
#[derive(Debug, Clone)]
pub struct PartialFrame {
    /// Frame timestamp (nanoseconds)
    pub timestamp: u64,

    /// Frame index
    pub index: usize,

    /// Aligned frame data
    pub frame: AlignedFrame,

    /// Which features have been received
    pub received_features: HashSet<String>,

    /// When this frame can be force-completed (timestamp)
    pub eligible_timestamp: u64,

    /// When this frame was first created
    pub created_at: Instant,
}

impl PartialFrame {
    /// Create a new partial frame.
    pub fn new(index: usize, timestamp: u64, eligible_timestamp: u64) -> Self {
        Self {
            timestamp,
            index,
            frame: AlignedFrame::new(index, timestamp),
            received_features: HashSet::new(),
            eligible_timestamp,
            created_at: Instant::now(),
        }
    }

    /// Add data to this frame and track the feature.
    pub fn add_feature(&mut self, feature: &str) {
        self.received_features.insert(feature.to_string());
    }

    /// Check if a specific feature has been received.
    pub fn has_feature(&self, feature: &str) -> bool {
        self.received_features.contains(feature)
    }

    /// Calculate how long this frame has been buffered (milliseconds).
    pub fn buffer_time_ms(&self) -> f64 {
        self.created_at.elapsed().as_secs_f64() * 1000.0
    }

    /// Get the number of features received.
    pub fn feature_count(&self) -> usize {
        self.received_features.len()
    }
}

/// Bounded buffer for aligning messages to frames with fixed memory footprint.
///
/// Maintains active frames being aligned and emits completed frames
/// for writing. The buffer uses a BTreeMap for automatic timestamp sorting.
pub struct FrameAlignmentBuffer {
    /// Active frames being aligned, keyed by timestamp
    active_frames: BTreeMap<u64, PartialFrame>,

    /// Configuration
    config: StreamingConfig,

    /// Completion criteria
    completion_criteria: FrameCompletionCriteria,

    /// Statistics
    stats: AlignmentStats,

    /// Next frame index to assign
    next_frame_index: usize,

    /// Current timestamp (from latest message)
    current_timestamp: u64,
}

impl FrameAlignmentBuffer {
    /// Create a new frame alignment buffer.
    pub fn new(config: StreamingConfig) -> Self {
        let completion_criteria = Self::build_completion_criteria(&config);

        Self {
            active_frames: BTreeMap::new(),
            config,
            completion_criteria,
            stats: AlignmentStats::new(),
            next_frame_index: 0,
            current_timestamp: 0,
        }
    }

    /// Create a new frame alignment buffer with custom completion criteria.
    pub fn with_completion_criteria(
        config: StreamingConfig,
        criteria: FrameCompletionCriteria,
    ) -> Self {
        Self {
            active_frames: BTreeMap::new(),
            config,
            completion_criteria: criteria,
            stats: AlignmentStats::new(),
            next_frame_index: 0,
            current_timestamp: 0,
        }
    }

    /// Process a message and return any completed frames.
    pub fn process_message(
        &mut self,
        timestamped_msg: &TimestampedMessage,
        feature_name: &str,
    ) -> Vec<AlignedFrame> {
        // Update current timestamp
        self.current_timestamp = timestamped_msg.log_time;

        // Align timestamp to frame boundary
        let aligned_ts = self.align_to_frame_boundary(timestamped_msg.log_time);

        // Get or create partial frame
        let entry = self.active_frames.entry(aligned_ts).or_insert_with(|| {
            let idx = self.next_frame_index;
            // Use checked arithmetic to detect overflow for very long recordings
            self.next_frame_index = self.next_frame_index.checked_add(1).unwrap_or_else(|| {
                tracing::error!("Frame index overflow - recording exceeds usize capacity");
                usize::MAX // Saturate at maximum value
            });
            let eligible = aligned_ts.saturating_add(self.config.completion_window_ns());
            PartialFrame::new(idx, aligned_ts, eligible)
        });

        // Add feature to the partial frame
        entry.add_feature(feature_name);

        // Extract message data and add to frame
        // Note: We do this before releasing the borrow
        Self::extract_message_to_frame_static(entry, timestamped_msg, feature_name);

        // Check for completed frames
        self.check_completions()
    }

    /// Flush all remaining frames (end of stream).
    pub fn flush(&mut self) -> Vec<AlignedFrame> {
        let mut completed = Vec::new();

        // Drain all frames from the map
        let frames: std::collections::BTreeMap<u64, PartialFrame> =
            std::mem::take(&mut self.active_frames);

        for (_ts, mut partial) in frames {
            // Update frame index to actual position
            partial.frame.frame_index = completed.len();

            // Mark as force-completed if not normally complete
            if !self
                .completion_criteria
                .is_complete(&partial.received_features)
            {
                self.stats.record_force_completion();
            } else {
                self.stats.record_normal_completion();
            }

            completed.push(partial.frame);
        }

        completed
    }

    /// Get the number of frames currently in the buffer.
    pub fn len(&self) -> usize {
        self.active_frames.len()
    }

    /// Check if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.active_frames.is_empty()
    }

    /// Get a reference to the statistics.
    pub fn stats(&self) -> &AlignmentStats {
        &self.stats
    }

    /// Get a mutable reference to the statistics.
    pub fn stats_mut(&mut self) -> &mut AlignmentStats {
        &mut self.stats
    }

    /// Estimate memory usage in bytes.
    pub fn estimated_memory_bytes(&self) -> usize {
        // Rough estimate: 1MB per frame (images dominate)
        self.active_frames.len() * 1_024 * 1_024
    }

    /// Align a timestamp to the nearest frame boundary.
    ///
    /// Uses round-half-up for consistent behavior. For example:
    /// - At 30 FPS (33,333,333 ns interval):
    ///   - 0-16,666,666 ns → frame 0
    ///   - 16,666,667-49,999,999 ns → frame 1 (rounds up at midpoint)
    ///   - 50,000,000+ ns → frame 1 (approaching next boundary)
    ///
    /// Uses saturating arithmetic to prevent overflow for very large timestamps.
    fn align_to_frame_boundary(&self, timestamp: u64) -> u64 {
        let interval = self.config.frame_interval_ns();
        // Round to nearest: (timestamp + interval/2) / interval * interval
        // Add 1 to handle the midpoint correctly (round half up)
        let half_interval = interval.saturating_add(1) / 2;
        let aligned = timestamp.saturating_add(half_interval) / interval * interval;
        aligned
    }

    /// Check for completed frames and remove them from the buffer.
    fn check_completions(&mut self) -> Vec<AlignedFrame> {
        let mut completed = Vec::new();
        let mut to_remove = Vec::new();

        for (&ts, partial) in &self.active_frames {
            // Check if frame is complete by criteria
            let is_data_complete = self
                .completion_criteria
                .is_complete(&partial.received_features);

            // Check if frame is complete by time window (eligible time has passed)
            let is_time_complete = self.current_timestamp >= partial.eligible_timestamp;

            if is_data_complete || is_time_complete {
                to_remove.push(ts);
            }
        }

        // Remove and return completed frames
        for ts in to_remove {
            if let Some(mut partial) = self.active_frames.remove(&ts) {
                // Update frame index
                partial.frame.frame_index = completed.len();

                if self
                    .completion_criteria
                    .is_complete(&partial.received_features)
                {
                    self.stats.record_normal_completion();
                } else {
                    self.stats.record_force_completion();
                }

                completed.push(partial.frame);
            }
        }

        // Update peak buffer size
        self.stats.update_peak_buffer(self.active_frames.len());

        completed
    }

    /// Build completion criteria from config.
    fn build_completion_criteria(config: &StreamingConfig) -> FrameCompletionCriteria {
        let mut criteria = FrameCompletionCriteria::new();

        for (feature, req) in &config.feature_requirements {
            criteria.features.insert(feature.clone(), *req);
        }

        // Default: require at least one data feature to avoid empty frames
        if criteria.features.is_empty() {
            criteria.min_completeness = 0.01; // Just need something
        }

        criteria
    }

    /// Extract message data and add it to the frame.
    fn extract_message_to_frame_static(
        partial: &mut PartialFrame,
        timestamped_msg: &TimestampedMessage,
        feature_name: &str,
    ) {
        use crate::dataset::common::ImageData;
        use robocodec::CodecValue;

        let msg = &timestamped_msg.message;

        // Try to extract image data
        let mut width = 0u32;
        let mut height = 0u32;
        let mut data: Option<Vec<u8>> = None;
        let mut is_encoded = false;

        for (key, value) in msg.iter() {
            match key.as_str() {
                "width" => {
                    if let CodecValue::UInt32(w) = value {
                        width = *w;
                    }
                }
                "height" => {
                    if let CodecValue::UInt32(h) = value {
                        height = *h;
                    }
                }
                "data" => {
                    if let CodecValue::Bytes(b) = value {
                        data = Some(b.clone());
                    }
                }
                "format" => {
                    if let CodecValue::String(f) = value {
                        is_encoded = f != "rgb8";
                    }
                }
                _ => {}
            }
        }

        if let Some(image_data) = data {
            partial.frame.images.insert(
                feature_name.to_string(),
                ImageData {
                    width,
                    height,
                    data: image_data,
                    original_timestamp: timestamped_msg.log_time,
                    is_encoded,
                },
            );
            return;
        }

        // Try to extract float array (for state/action)
        let mut values = Vec::new();

        // Collect all numeric values from the message
        for value in msg.values() {
            match value {
                CodecValue::Float32(n) => values.push(*n),
                CodecValue::Float64(n) => values.push(*n as f32),
                CodecValue::UInt8(n) => values.push(*n as f32),
                CodecValue::UInt16(n) => values.push(*n as f32),
                CodecValue::UInt32(n) => values.push(*n as f32),
                CodecValue::UInt64(n) => values.push(*n as f32),
                CodecValue::Int8(n) => values.push(*n as f32),
                CodecValue::Int16(n) => values.push(*n as f32),
                CodecValue::Int32(n) => values.push(*n as f32),
                CodecValue::Int64(n) => values.push(*n as f32),
                CodecValue::Array(arr) => {
                    for v in arr.iter() {
                        match v {
                            CodecValue::Float32(n) => values.push(*n),
                            CodecValue::Float64(n) => values.push(*n as f32),
                            CodecValue::UInt8(n) => values.push(*n as f32),
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }

        // Add as state or action based on feature name
        if !values.is_empty() {
            if feature_name.starts_with("action.") {
                partial
                    .frame
                    .actions
                    .insert(feature_name.to_string(), values);
            } else {
                partial
                    .frame
                    .states
                    .insert(feature_name.to_string(), values);
            }
        }
    }
}

/// A timestamped message from the source.
#[derive(Debug, Clone)]
pub struct TimestampedMessage {
    /// Log time (nanoseconds)
    pub log_time: u64,

    /// Decoded message data
    pub message: HashMap<String, robocodec::CodecValue>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_alignment() {
        let config = StreamingConfig::with_fps(30);
        let buffer = FrameAlignmentBuffer::new(config);

        // Test alignment at various timestamps
        // 30 FPS = 33,333,333 ns interval
        // Frame 0: 0 - 16,666,666 ns (rounds to 0)
        // Frame 1: 16,666,667 - 49,999,999 ns (rounds to 33,333,333)
        // Frame 2: 50,000,000 - 83,333,332 ns (rounds to 66,666,666)

        // Timestamp 0 should align to frame 0
        assert_eq!(buffer.align_to_frame_boundary(0), 0);

        // Midpoint (16,666,666) should round up to frame 1
        assert_eq!(buffer.align_to_frame_boundary(16_666_666), 33_333_333);

        // 30ms should round up to frame 1 (closer to 33.33ms than 0ms)
        assert_eq!(buffer.align_to_frame_boundary(30_000_000), 33_333_333);

        // 40ms should round to frame 1 (in the middle of frame 1's range)
        assert_eq!(buffer.align_to_frame_boundary(40_000_000), 33_333_333);

        // 50ms is at the boundary, rounds up to frame 2
        assert_eq!(buffer.align_to_frame_boundary(50_000_000), 66_666_666);
    }

    #[test]
    fn test_partial_frame() {
        let mut frame = PartialFrame::new(0, 0, 100_000_000);

        assert_eq!(frame.timestamp, 0);
        assert_eq!(frame.index, 0);
        assert_eq!(frame.eligible_timestamp, 100_000_000);
        assert_eq!(frame.feature_count(), 0);
        assert!(!frame.has_feature("test"));

        frame.add_feature("test");
        assert!(frame.has_feature("test"));
        assert_eq!(frame.feature_count(), 1);
    }

    #[test]
    fn test_buffer_estimated_memory() {
        let config = StreamingConfig::default();
        let buffer = FrameAlignmentBuffer::new(config);

        assert_eq!(buffer.estimated_memory_bytes(), 0);

        // Can't easily test adding frames without a full message setup,
        // but the logic is straightforward
    }
}
