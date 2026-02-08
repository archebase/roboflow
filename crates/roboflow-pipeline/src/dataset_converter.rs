// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Dataset converter - direct conversion to dataset formats.
//!
//! This module provides an alternative to the full pipeline for converting
//! directly to dataset formats (KPS, LeRobot) without MCAP compression.
//!
//! # Architecture
//!
//! ```text
//! Input File (MCAP/Bag) → RoboReader → DatasetWriter → Dataset Files
//!                         (decodes)
//! ```
//!
//! This bypasses the compression and MCAP writer stages for direct conversion.

use std::collections::HashMap;
use std::path::Path;

use tracing::{info, instrument};

use robocodec::CodecValue;
use robocodec::RoboReader;
use roboflow_core::{Result, RoboflowError};
use roboflow_dataset::common::config::{Mapping, MappingType};
use roboflow_dataset::common::{AlignedFrame, ImageData};
use roboflow_dataset::kps::config::KpsConfig;
use roboflow_dataset::lerobot::config::LerobotConfig;
use roboflow_dataset::{DatasetFormat, DatasetWriter, create_writer};

/// Direct dataset converter.
///
/// Converts input files (MCAP/Bag) directly to dataset formats using
/// the unified DatasetWriter interface.
pub struct DatasetConverter {
    /// Output directory
    output_dir: std::path::PathBuf,

    /// Dataset format
    format: DatasetFormat,

    /// KPS configuration (if KPS format)
    kps_config: Option<KpsConfig>,

    /// LeRobot configuration (if LeRobot format)
    lerobot_config: Option<LerobotConfig>,

    /// Target FPS for frame alignment
    fps: u32,

    /// Maximum frames to write
    max_frames: Option<usize>,
}

impl DatasetConverter {
    /// Create a new dataset converter for KPS format.
    pub fn new_kps<P: AsRef<Path>>(output_dir: P, config: KpsConfig) -> Self {
        Self {
            output_dir: output_dir.as_ref().to_path_buf(),
            format: DatasetFormat::Kps,
            kps_config: Some(config),
            lerobot_config: None,
            fps: 30, // Will be overridden from config
            max_frames: None,
        }
    }

    /// Create a new dataset converter for LeRobot format.
    pub fn new_lerobot<P: AsRef<Path>>(output_dir: P, config: LerobotConfig) -> Self {
        let fps = config.dataset.fps;
        Self {
            output_dir: output_dir.as_ref().to_path_buf(),
            format: DatasetFormat::Lerobot,
            kps_config: None,
            lerobot_config: Some(config),
            fps,
            max_frames: None,
        }
    }

    /// Set the target FPS.
    pub fn with_fps(mut self, fps: u32) -> Self {
        self.fps = fps;
        self
    }

    /// Set maximum frames to write.
    pub fn with_max_frames(mut self, max: usize) -> Self {
        self.max_frames = Some(max);
        self
    }

    /// Convert input file to dataset format.
    #[instrument(skip_all, fields(
        input = %input_path.as_ref().display(),
        output = %self.output_dir.display(),
        format = ?self.format,
    ))]
    pub fn convert<P: AsRef<Path>>(self, input_path: P) -> Result<DatasetConverterStats> {
        let input_path = input_path.as_ref();

        info!(
            input = %input_path.display(),
            output = %self.output_dir.display(),
            format = ?self.format,
            "Starting dataset conversion"
        );

        match self.format {
            DatasetFormat::Kps => self.convert_kps(input_path),
            DatasetFormat::Lerobot => self.convert_lerobot(input_path),
        }
    }

    /// Convert to KPS format.
    fn convert_kps(self, input_path: &Path) -> Result<DatasetConverterStats> {
        // Get KPS config
        let kps_config = self
            .kps_config
            .as_ref()
            .ok_or_else(|| RoboflowError::parse("DatasetConverter", "KPS config required"))?;

        let fps = kps_config.dataset.fps;

        // Create the dataset writer
        let config = roboflow_dataset::DatasetConfig::Kps(kps_config.clone());
        let writer = create_writer(&self.output_dir, None, None, &config).map_err(
            |e: roboflow_core::RoboflowError| {
                RoboflowError::encode("DatasetConverter", e.to_string())
            },
        )?;

        // Build topic -> mapping lookup
        let topic_mappings: HashMap<String, Mapping> = kps_config
            .mappings
            .iter()
            .map(|m| (m.topic.clone(), m.clone()))
            .collect();

        self.convert_common(input_path, writer, topic_mappings, fps, false)
    }

    /// Convert to LeRobot format.
    fn convert_lerobot(self, input_path: &Path) -> Result<DatasetConverterStats> {
        // Get LeRobot config
        let lerobot_config = self
            .lerobot_config
            .as_ref()
            .ok_or_else(|| RoboflowError::parse("DatasetConverter", "LeRobot config required"))?;

        let fps = lerobot_config.dataset.fps;

        // Create the dataset writer
        let config = roboflow_dataset::DatasetConfig::Lerobot(lerobot_config.clone());
        let writer = create_writer(&self.output_dir, None, None, &config).map_err(
            |e: roboflow_core::RoboflowError| {
                RoboflowError::encode("DatasetConverter", e.to_string())
            },
        )?;

        // Build topic -> mapping lookup
        let topic_mappings: HashMap<String, Mapping> = lerobot_config
            .mappings
            .iter()
            .map(|m| (m.topic.clone(), m.clone()))
            .collect();

        // LeRobot treats unrecognized mapping types (OtherSensor, Audio) as state data
        self.convert_common(input_path, writer, topic_mappings, fps, true)
    }

    /// Shared conversion loop used by both KPS and LeRobot paths.
    ///
    /// Handles reader setup, frame alignment, frame sorting/truncation,
    /// writing to the dataset writer, and finalization.
    ///
    /// # Arguments
    ///
    /// * `input_path` - Path to the input BAG/MCAP file
    /// * `writer` - Pre-configured dataset writer
    /// * `topic_mappings` - Topic-to-mapping lookup table
    /// * `fps` - Target frames per second for frame alignment
    /// * `fallback_to_state` - If `true`, unrecognized mapping types are
    ///   treated as state data (LeRobot behaviour). If `false`, they are
    ///   silently ignored (KPS behaviour).
    fn convert_common(
        &self,
        input_path: &Path,
        mut writer: Box<dyn DatasetWriter>,
        topic_mappings: HashMap<String, Mapping>,
        fps: u32,
        fallback_to_state: bool,
    ) -> Result<DatasetConverterStats> {
        // Open input file
        let path_str = input_path
            .to_str()
            .ok_or_else(|| RoboflowError::parse("Path", "Invalid UTF-8 path"))?;
        let reader = RoboReader::open(path_str)?;

        // State for building aligned frames
        let mut frame_buffer: HashMap<u64, AlignedFrame> = HashMap::new();
        let mut frame_count: usize = 0;
        let start_time = std::time::Instant::now();

        // Process decoded messages
        let frame_interval_ns = 1_000_000_000 / fps as u64;

        info!(mappings = topic_mappings.len(), "Processing messages");

        for msg_result in reader.decoded()? {
            let timestamped_msg = msg_result?;

            // Find mapping for this topic
            let mapping = match topic_mappings.get(&timestamped_msg.channel.topic) {
                Some(m) => m,
                None => continue, // Skip unmapped topics
            };

            // Align timestamp to frame boundary
            let aligned_timestamp =
                Self::align_to_frame(timestamped_msg.log_time.unwrap_or(0), frame_interval_ns);

            // Get or create frame - track new frames for max_frames limit
            let is_new = !frame_buffer.contains_key(&aligned_timestamp);
            let frame = frame_buffer.entry(aligned_timestamp).or_insert_with(|| {
                let idx = frame_count;
                if is_new {
                    frame_count += 1;
                }
                AlignedFrame::new(idx, aligned_timestamp)
            });

            // Check max frames after potentially adding a new frame
            if let Some(max) = self.max_frames {
                if frame_count > max {
                    info!("Reached max frames limit: {}", max);
                    break;
                }
            }

            // Extract and add data based on mapping type
            let msg = &timestamped_msg.message;
            match &mapping.mapping_type {
                MappingType::Image => {
                    if let Some(img) = Self::extract_image(msg) {
                        frame.add_image(
                            mapping.feature.clone(),
                            ImageData {
                                original_timestamp: timestamped_msg.log_time.unwrap_or(0),
                                ..img
                            },
                        );
                    }
                }
                MappingType::State => {
                    if let Some(values) = Self::extract_float_array(msg) {
                        frame.add_state(mapping.feature.clone(), values);
                    }
                }
                MappingType::Action => {
                    if let Some(values) = Self::extract_float_array(msg) {
                        frame.add_action(mapping.feature.clone(), values);
                    }
                }
                MappingType::Timestamp => {
                    frame.add_timestamp(
                        mapping.feature.clone(),
                        timestamped_msg.log_time.unwrap_or(0),
                    );
                }
                // OtherSensor, Audio, and any future variants:
                // LeRobot treats them as state data; KPS ignores them.
                _ => {
                    if fallback_to_state {
                        if let Some(values) = Self::extract_float_array(msg) {
                            frame.add_state(mapping.feature.clone(), values);
                        }
                    }
                }
            }
        }

        // Sort frames by timestamp and write
        let mut frames: Vec<_> = frame_buffer.into_values().collect();
        frames.sort_by_key(|f| f.timestamp);

        // Truncate to max_frames if specified
        if let Some(max) = self.max_frames {
            if frames.len() > max {
                tracing::info!(
                    original_count = frames.len(),
                    max,
                    "Truncating frames to max_frames limit"
                );
                frames.truncate(max);
            }
        }

        // Update frame indices after sorting
        for (i, frame) in frames.iter_mut().enumerate() {
            frame.frame_index = i;
        }

        info!(frames = frames.len(), "Writing frames to dataset");

        for frame in &frames {
            writer.write_frame(frame)?;
        }

        // Finalize and get stats
        let stats = writer.finalize()?;
        let duration = start_time.elapsed();

        info!(
            frames_written = frames.len(),
            duration_sec = duration.as_secs_f64(),
            "Dataset conversion complete"
        );

        Ok(DatasetConverterStats {
            frames_written: frames.len(),
            images_encoded: stats.images_encoded,
            output_bytes: stats.output_bytes,
            duration_sec: duration.as_secs_f64(),
        })
    }

    /// Align timestamp to nearest frame boundary.
    /// Rounds half-up at the midpoint.
    fn align_to_frame(timestamp: u64, interval_ns: u64) -> u64 {
        let half_interval = interval_ns / 2 + 1; // +1 to round up at exact midpoint
        ((timestamp + half_interval) / interval_ns) * interval_ns
    }

    /// Extract float array from decoded message.
    fn extract_float_array(msg: &HashMap<String, CodecValue>) -> Option<Vec<f32>> {
        let mut values = Vec::new();

        for value in msg.values() {
            match value {
                CodecValue::UInt8(n) => values.push(*n as f32),
                CodecValue::UInt16(n) => values.push(*n as f32),
                CodecValue::UInt32(n) => values.push(*n as f32),
                CodecValue::UInt64(n) => values.push(*n as f32),
                CodecValue::Int8(n) => values.push(*n as f32),
                CodecValue::Int16(n) => values.push(*n as f32),
                CodecValue::Int32(n) => values.push(*n as f32),
                CodecValue::Int64(n) => values.push(*n as f32),
                CodecValue::Float32(n) => values.push(*n),
                CodecValue::Float64(n) => values.push(*n as f32),
                CodecValue::Array(arr) => {
                    // Try to extract float values from array
                    for v in arr.iter() {
                        match v {
                            CodecValue::UInt8(n) => values.push(*n as f32),
                            CodecValue::UInt16(n) => values.push(*n as f32),
                            CodecValue::UInt32(n) => values.push(*n as f32),
                            CodecValue::Float32(n) => values.push(*n),
                            CodecValue::Float64(n) => values.push(*n as f32),
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }

        if values.is_empty() {
            None
        } else {
            Some(values)
        }
    }

    /// Extract image data from decoded message.
    fn extract_image(msg: &HashMap<String, CodecValue>) -> Option<ImageData> {
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

        let image_data = data?;

        Some(ImageData {
            width,
            height,
            data: image_data,
            original_timestamp: 0,
            is_encoded,
            is_depth: false,
        })
    }
}

/// Statistics from dataset conversion.
#[derive(Debug, Clone)]
pub struct DatasetConverterStats {
    /// Number of frames written
    pub frames_written: usize,
    /// Number of images encoded
    pub images_encoded: usize,
    /// Output size in bytes
    pub output_bytes: u64,
    /// Duration in seconds
    pub duration_sec: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_align_to_frame() {
        // 30 FPS = 33,333,333 ns interval
        let interval = 33_333_333;

        assert_eq!(DatasetConverter::align_to_frame(0, interval), 0);
        // Midpoint (16,666,666) rounds up to 33,333,333
        assert_eq!(
            DatasetConverter::align_to_frame(16_666_666, interval),
            33_333_333
        );
        // 50,000,000 is closer to 66,666,666 than 33,333,333
        assert_eq!(
            DatasetConverter::align_to_frame(50_000_000, interval),
            66_666_666
        );
        assert_eq!(
            DatasetConverter::align_to_frame(100_000_000, interval),
            99_999_999
        );
    }

    #[test]
    fn test_extract_float_array() {
        use robocodec::CodecValue;

        let mut msg = HashMap::new();
        msg.insert(
            "position".to_string(),
            CodecValue::Array(vec![
                CodecValue::Float32(1.0),
                CodecValue::Float32(2.0),
                CodecValue::Float32(3.0),
            ]),
        );

        let result = DatasetConverter::extract_float_array(&msg)
            .expect("float array extraction should succeed with valid input");
        assert_eq!(result, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_extract_image() {
        use robocodec::CodecValue;

        let mut msg = HashMap::new();
        msg.insert("width".to_string(), CodecValue::UInt32(640));
        msg.insert("height".to_string(), CodecValue::UInt32(480));
        msg.insert("data".to_string(), CodecValue::Bytes(vec![1, 2, 3, 4]));
        msg.insert("format".to_string(), CodecValue::String("rgb8".to_string()));

        let image = DatasetConverter::extract_image(&msg)
            .expect("image extraction should succeed with valid input");
        assert_eq!(image.width, 640);
        assert_eq!(image.height, 480);
        assert_eq!(image.data, vec![1, 2, 3, 4]);
        assert!(!image.is_encoded);
    }
}
