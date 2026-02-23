// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Frame alignment with bounded memory footprint.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use robocodec::CodecValue;

use crate::formats::alignment::completion::FrameCompletionCriteria;
use crate::formats::alignment::config::StreamingConfig;
use crate::formats::alignment::stats::AlignmentStats;
use crate::formats::common::AlignedFrame;
use roboflow_media::image::{ImageDecoderFactory, ImageFormat};

/// Extracted image data from a message.
struct ExtractedImageData {
    /// Image width in pixels
    width: u32,
    /// Image height in pixels
    height: u32,
    /// Raw or encoded image bytes
    data: Vec<u8>,
    /// Whether the image is encoded (JPEG/PNG) or raw RGB
    is_encoded: bool,
}

/// Extracted state/action values from a message.
struct ExtractedValues {
    /// The numeric values extracted
    values: Vec<f32>,
    /// Whether this is an action (vs state)
    is_action: bool,
}

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
/// for writing. The buffer uses a sorted Vec for better cache locality
/// (frames typically < 1000, making binary search very efficient).
pub struct FrameAlignmentBuffer {
    /// Active frames being aligned, kept sorted by timestamp
    active_frames: Vec<PartialFrame>,

    /// Configuration
    config: StreamingConfig,

    /// Completion criteria
    completion_criteria: FrameCompletionCriteria,

    /// Statistics
    stats: AlignmentStats,

    /// Image decoder factory (optional, for decoding CompressedImage messages)
    decoder: Option<ImageDecoderFactory>,

    /// Next frame index to assign
    next_frame_index: usize,

    /// Current timestamp (from latest message)
    current_timestamp: u64,
}

impl FrameAlignmentBuffer {
    /// Create a new frame alignment buffer.
    pub fn new(config: StreamingConfig) -> Self {
        let completion_criteria = Self::build_completion_criteria(&config);
        let decoder = config.decoder_config.as_ref().map(ImageDecoderFactory::new);

        Self {
            active_frames: Vec::new(),
            config,
            completion_criteria,
            stats: AlignmentStats::new(),
            decoder,
            next_frame_index: 0,
            current_timestamp: 0,
        }
    }

    /// Create a new frame alignment buffer with custom completion criteria.
    pub fn with_completion_criteria(
        config: StreamingConfig,
        criteria: FrameCompletionCriteria,
    ) -> Self {
        let decoder = config.decoder_config.as_ref().map(ImageDecoderFactory::new);

        Self {
            active_frames: Vec::new(),
            config,
            completion_criteria: criteria,
            stats: AlignmentStats::new(),
            decoder,
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
        use roboflow_media::ImageData;

        // Update current timestamp
        self.current_timestamp = timestamped_msg.log_time;
        let msg = &timestamped_msg.message;

        // Step 1: Extract and decode image data (if any)
        let (image_info, decoded_data, final_is_encoded) =
            self.process_image_data(msg, feature_name);

        // Step 2: Extract state/action values (before mutable borrow)
        let values = Self::extract_state_action_values_static(msg, feature_name);

        // Step 3: Align timestamp to frame boundary and get/create frame
        let aligned_ts = self.align_to_frame_boundary(timestamped_msg.log_time);
        let entry = self.find_or_create_frame(aligned_ts);

        // Step 4: Add feature to the partial frame
        entry.add_feature(feature_name);

        // Step 5: Add image data to the frame (if we extracted any)
        if let Some(data) = decoded_data {
            entry.frame.images.insert(
                feature_name.to_string(),
                Arc::new(ImageData {
                    width: image_info.as_ref().map(|i| i.width).unwrap_or(0),
                    height: image_info.as_ref().map(|i| i.height).unwrap_or(0),
                    data,
                    original_timestamp: timestamped_msg.log_time,
                    is_encoded: final_is_encoded,
                    is_depth: false,
                }),
            );
        }

        // Step 6: Add state/action data
        if !values.values.is_empty() {
            if values.is_action {
                entry
                    .frame
                    .actions
                    .insert(feature_name.to_string(), values.values);
            } else {
                entry
                    .frame
                    .states
                    .insert(feature_name.to_string(), values.values);
            }
        }

        // Step 7: Check for completed frames
        self.check_completions()
    }

    /// Process image data from a message.
    ///
    /// Returns (image_info, decoded_data, is_encoded).
    fn process_image_data(
        &mut self,
        msg: &HashMap<String, CodecValue>,
        feature_name: &str,
    ) -> (Option<ExtractedImageData>, Option<Vec<u8>>, bool) {
        let Some(mut image_info) = self.extract_image_data(msg, feature_name) else {
            return (None, None, false);
        };

        let (decoded_data, final_is_encoded) =
            self.decode_image_if_needed(&mut image_info, feature_name);

        (Some(image_info), decoded_data, final_is_encoded)
    }

    /// Extract image data from a message.
    ///
    /// Returns extracted image data including width, height, raw bytes, and encoding status.
    fn extract_image_data(
        &self,
        msg: &HashMap<String, CodecValue>,
        feature_name: &str,
    ) -> Option<ExtractedImageData> {
        let mut width = 0u32;
        let mut height = 0u32;
        let mut image_data: Option<Vec<u8>> = None;
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
                    image_data = self.extract_data_field(value, feature_name);
                }
                "format" => {
                    if let CodecValue::String(f) = value {
                        is_encoded = f != "rgb8";
                        tracing::debug!(
                            feature = %feature_name,
                            format = %f,
                            is_encoded,
                            "Found image format field"
                        );
                    }
                }
                _ => {}
            }
        }

        // Log if we expected image data but didn't find it
        if (feature_name.contains("image") || feature_name.contains("cam")) && image_data.is_none()
        {
            tracing::debug!(
                feature = %feature_name,
                num_fields = msg.iter().count(),
                available_fields = ?msg.keys().cloned().collect::<Vec<_>>(),
                "Image feature but no data field found"
            );
        }

        image_data.map(|data| ExtractedImageData {
            width,
            height,
            data,
            is_encoded,
        })
    }

    /// Extract data from a 'data' field value.
    fn extract_data_field(&self, value: &CodecValue, feature_name: &str) -> Option<Vec<u8>> {
        match value {
            CodecValue::Bytes(b) => {
                tracing::debug!(
                    feature = %feature_name,
                    data_type = "Bytes",
                    data_len = b.len(),
                    data_size_mb = b.len() as f64 / (1024.0 * 1024.0),
                    "Found image data field"
                );
                Some(b.clone())
            }
            CodecValue::Array(arr) => self.extract_array_data(arr, feature_name),
            other => {
                let actual_type = other.type_name();
                tracing::warn!(
                    feature = %feature_name,
                    value_type = %actual_type,
                    "Image 'data' field found but not Bytes/Array type"
                );
                None
            }
        }
    }

    /// Extract bytes from an array data field.
    fn extract_array_data(&self, arr: &[CodecValue], feature_name: &str) -> Option<Vec<u8>> {
        // Helper to extract u8 from any numeric CodecValue
        let codec_value_to_u8 = |v: &CodecValue| -> Option<u8> {
            match v {
                CodecValue::UInt8(b) => Some(*b),
                CodecValue::Int8(b) if *b >= 0 => Some(*b as u8),
                CodecValue::UInt16(b) if *b <= u8::MAX as u16 => Some(*b as u8),
                CodecValue::Int16(b) if *b >= 0 && (*b as u16) <= u8::MAX as u16 => Some(*b as u8),
                CodecValue::UInt32(b) if *b <= u8::MAX as u32 => Some(*b as u8),
                CodecValue::Int32(b) if *b >= 0 && (*b as u32) <= u8::MAX as u32 => Some(*b as u8),
                CodecValue::UInt64(b) if *b <= u8::MAX as u64 => Some(*b as u8),
                CodecValue::Int64(b) if *b >= 0 && (*b as u64) <= u8::MAX as u64 => Some(*b as u8),
                _ => None,
            }
        };

        // Handle encoded image data stored as UInt8 array (most common)
        let bytes: Vec<u8> = arr.iter().filter_map(codec_value_to_u8).collect();
        if !bytes.is_empty() {
            tracing::debug!(
                feature = %feature_name,
                data_type = "Array<UInt8>",
                data_len = bytes.len(),
                data_size_mb = bytes.len() as f64 / (1024.0 * 1024.0),
                "Found image data field in Array format"
            );
            return Some(bytes);
        }

        // Try nested arrays (some codecs use Array<Array<UInt8>>)
        for v in arr.iter() {
            if let CodecValue::Array(inner) = v {
                let inner_bytes: Vec<u8> = inner.iter().filter_map(codec_value_to_u8).collect();
                if !inner_bytes.is_empty() {
                    tracing::debug!(
                        feature = %feature_name,
                        data_type = "Array<Array<UInt8>>",
                        "Found image data in nested Array format"
                    );
                    return Some(inner_bytes);
                }
            }
        }

        tracing::warn!(
            feature = %feature_name,
            array_len = arr.len(),
            "Image 'data' is Array but no valid UInt8 elements found"
        );
        None
    }

    /// Decode compressed image data if needed.
    ///
    /// Returns (decoded_data, is_encoded) tuple.
    fn decode_image_if_needed(
        &mut self,
        image_data: &mut ExtractedImageData,
        feature_name: &str,
    ) -> (Option<Vec<u8>>, bool) {
        // Extract dimensions from header if not provided
        if image_data.width == 0
            && image_data.height == 0
            && let Some((w, h)) = Self::extract_image_dimensions(&image_data.data)
        {
            image_data.width = w;
            image_data.height = h;
        }

        // Try decoding if we have compressed data and a decoder
        if !image_data.is_encoded {
            return (Some(image_data.data.clone()), false);
        }

        let Some(decoder) = &mut self.decoder else {
            return (Some(image_data.data.clone()), true);
        };

        let format = ImageFormat::from_magic_bytes(&image_data.data);
        if format == ImageFormat::Unknown {
            return (Some(image_data.data.clone()), true);
        }

        match decoder.get_decoder().decode(&image_data.data, format) {
            Ok(decoded) => {
                tracing::debug!(
                    width = decoded.width,
                    height = decoded.height,
                    feature = %feature_name,
                    "Decoded compressed image"
                );
                // Update dimensions from decoded image
                image_data.width = decoded.width;
                image_data.height = decoded.height;
                (Some(decoded.data), false)
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    feature = %feature_name,
                    "Failed to decode image, storing compressed"
                );
                (Some(image_data.data.clone()), true)
            }
        }
    }

    /// Extract state/action values from a message (static version for borrow safety).
    fn extract_state_action_values_static(
        msg: &HashMap<String, CodecValue>,
        feature_name: &str,
    ) -> ExtractedValues {
        let mut values = Vec::new();

        for value in msg.values() {
            Self::extract_numeric_values_static(value, &mut values);
        }

        ExtractedValues {
            is_action: feature_name.starts_with("action."),
            values,
        }
    }

    /// Extract numeric values from a CodecValue into the values vector (static version).
    fn extract_numeric_values_static(value: &CodecValue, values: &mut Vec<f32>) {
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

    /// Flush all remaining frames (end of stream).
    pub fn flush(&mut self) -> Vec<AlignedFrame> {
        let mut completed = Vec::new();

        // Drain all frames from the vec
        let frames: Vec<PartialFrame> = std::mem::take(&mut self.active_frames);

        for mut partial in frames {
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
    ///
    /// Calculates actual memory usage based on the images stored in active frames,
    /// accounting for whether images are encoded (JPEG/PNG) or decoded RGB.
    pub fn estimated_memory_bytes(&self) -> usize {
        let mut total = 0usize;

        for partial in &self.active_frames {
            // Estimate image memory usage
            for image in partial.frame.images.values() {
                if image.is_encoded {
                    // Compressed image - use actual data size
                    total += image.data.len();
                } else {
                    // RGB decoded image - width * height * 3
                    total += (image.width as usize) * (image.height as usize) * 3;
                }
            }

            // Estimate state/action memory (small contribution)
            total += partial.frame.states.len() * 100; // Rough estimate
            total += partial.frame.actions.len() * 100;
        }

        // Add overhead for the data structures themselves
        total += self.active_frames.len() * 64; // Vec overhead (much lower than BTreeMap)

        total
    }

    /// Find or create a partial frame for the given timestamp.
    ///
    /// Uses binary search since frames are kept sorted by timestamp.
    fn find_or_create_frame(&mut self, timestamp: u64) -> &mut PartialFrame {
        // Binary search for the frame
        match self
            .active_frames
            .binary_search_by_key(&timestamp, |f| f.timestamp)
        {
            Ok(idx) => {
                // Found existing frame
                &mut self.active_frames[idx]
            }
            Err(idx) => {
                // Frame not found - create new one and insert at sorted position
                let frame_idx = self.next_frame_index;
                self.next_frame_index = self.next_frame_index.checked_add(1).unwrap_or_else(|| {
                    tracing::error!("Frame index overflow - recording exceeds usize capacity");
                    usize::MAX
                });
                let eligible = timestamp.saturating_add(self.config.completion_window_ns());
                let frame = PartialFrame::new(frame_idx, timestamp, eligible);
                self.active_frames.insert(idx, frame);
                &mut self.active_frames[idx]
            }
        }
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
        timestamp.saturating_add(half_interval) / interval * interval
    }

    /// Check for completed frames and remove them from the buffer.
    fn check_completions(&mut self) -> Vec<AlignedFrame> {
        let mut completed = Vec::new();
        let mut to_remove = Vec::new();

        for (idx, partial) in self.active_frames.iter().enumerate() {
            // Check if frame is complete by criteria
            let is_data_complete = self
                .completion_criteria
                .is_complete(&partial.received_features);

            // Check if frame is complete by time window (eligible time has passed)
            let is_time_complete = self.current_timestamp >= partial.eligible_timestamp;

            if is_data_complete || is_time_complete {
                to_remove.push(idx);
            }
        }

        // Remove and return completed frames (in reverse order to preserve indices)
        for idx in to_remove.into_iter().rev() {
            let mut partial = self.active_frames.remove(idx);

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

    /// Extract image dimensions from JPEG/PNG header data.
    ///
    /// Uses `ImageFormat::extract_dimensions()` from roboflow-media.
    fn extract_image_dimensions(data: &[u8]) -> Option<(u32, u32)> {
        let format = ImageFormat::from_magic_bytes(data);
        format.extract_dimensions(data)
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

    #[test]
    fn test_partial_frame_multiple_features() {
        let mut frame = PartialFrame::new(5, 1_000_000_000, 2_000_000_000);

        frame.add_feature("observation.images.cam_0");
        frame.add_feature("observation.state");
        frame.add_feature("action");

        assert_eq!(frame.feature_count(), 3);
        assert!(frame.has_feature("observation.images.cam_0"));
        assert!(frame.has_feature("observation.state"));
        assert!(frame.has_feature("action"));
        assert!(!frame.has_feature("observation.images.cam_1"));
    }

    #[test]
    fn test_partial_frame_buffer_time() {
        let frame = PartialFrame::new(0, 0, 100_000_000);

        // Buffer time should be very small since we just created it
        let buffer_time = frame.buffer_time_ms();
        assert!(buffer_time >= 0.0);
        assert!(buffer_time < 100.0); // Should be less than 100ms
    }

    #[test]
    fn test_frame_alignment_buffer_new() {
        let config = StreamingConfig::with_fps(30);
        let buffer = FrameAlignmentBuffer::new(config);

        assert!(buffer.active_frames.is_empty());
        assert_eq!(buffer.next_frame_index, 0);
        assert_eq!(buffer.current_timestamp, 0);
    }

    #[test]
    fn test_frame_alignment_buffer_with_completion_criteria() {
        let config = StreamingConfig::with_fps(30);
        let criteria =
            FrameCompletionCriteria::new().require_feature("observation.images.cam_0", 1);

        let buffer = FrameAlignmentBuffer::with_completion_criteria(config, criteria);

        assert!(buffer.active_frames.is_empty());
    }

    #[test]
    fn test_frame_alignment_buffer_different_fps() {
        // Test with 60 FPS
        let config = StreamingConfig::with_fps(60);
        let buffer = FrameAlignmentBuffer::new(config);

        // 60 FPS = 16,666,666 ns interval
        assert_eq!(buffer.align_to_frame_boundary(0), 0);

        // Just verify the alignment produces valid results
        let result1 = buffer.align_to_frame_boundary(8_333_333);
        let result2 = buffer.align_to_frame_boundary(25_000_000);
        assert!(result1 > 0);
        assert!(result2 > result1);
    }

    #[test]
    fn test_timestamped_message_creation() {
        let mut message = HashMap::new();
        message.insert("data".to_string(), CodecValue::Bytes(vec![1, 2, 3]));

        let msg = TimestampedMessage {
            log_time: 1_000_000_000,
            message,
        };

        assert_eq!(msg.log_time, 1_000_000_000);
        assert!(msg.message.contains_key("data"));
    }

    #[test]
    fn test_partial_frame_debug() {
        let frame = PartialFrame::new(0, 0, 100_000_000);
        let debug_str = format!("{:?}", frame);

        assert!(debug_str.contains("PartialFrame"));
        assert!(debug_str.contains("timestamp"));
        assert!(debug_str.contains("index"));
    }

    #[test]
    fn test_buffer_active_frames_count() {
        let config = StreamingConfig::with_fps(30);
        let buffer = FrameAlignmentBuffer::new(config);

        assert_eq!(buffer.active_frames.len(), 0);
    }

    #[test]
    fn test_buffer_flush_empty() {
        let config = StreamingConfig::with_fps(30);
        let mut buffer = FrameAlignmentBuffer::new(config);

        let frames = buffer.flush();
        assert!(frames.is_empty());
    }

    #[test]
    fn test_buffer_stats_initial() {
        let config = StreamingConfig::with_fps(30);
        let buffer = FrameAlignmentBuffer::new(config);

        // Stats should be zero initially
        assert_eq!(buffer.stats.frames_processed, 0);
        assert_eq!(buffer.stats.normal_completions, 0);
        assert_eq!(buffer.stats.force_completions, 0);
    }

    #[test]
    fn test_align_to_frame_boundary_zero() {
        let config = StreamingConfig::with_fps(30);
        let buffer = FrameAlignmentBuffer::new(config);

        // Zero timestamp should align to zero
        assert_eq!(buffer.align_to_frame_boundary(0), 0);
    }

    #[test]
    fn test_align_to_frame_boundary_large_timestamp() {
        let config = StreamingConfig::with_fps(30);
        let buffer = FrameAlignmentBuffer::new(config);

        // Test with a large timestamp (1 second = 1_000_000_000 ns)
        // At 30 FPS, frame interval is 33,333,333 ns
        // 1 second should be frame 30 (30 * 33,333,333 = 999,999,990)
        let result = buffer.align_to_frame_boundary(1_000_000_000);
        assert!(result > 0);
    }

    #[test]
    fn test_partial_frame_clone() {
        let mut frame = PartialFrame::new(0, 0, 100_000_000);
        frame.add_feature("test");

        let cloned = frame.clone();

        assert_eq!(frame.timestamp, cloned.timestamp);
        assert_eq!(frame.index, cloned.index);
        assert_eq!(frame.feature_count(), cloned.feature_count());
    }
}
