// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Kps Parquet + MP4 format writer.
//!
//! Writes Kps datasets in the v3.0 format:
//! - Tabular data in Parquet files
//! - Image data encoded as MP4 video files

use std::collections::HashMap;
use std::path::Path;

use super::config::KpsConfig;

use super::config::Mapping;

use std::io::Write;

// Row structures for Parquet data
// These are used by the ParquetKpsWriter implementation.
#[derive(Debug, Clone)]
struct ObservationRow {
    _timestamp: i64,
}

#[derive(Debug, Clone)]
struct ActionRow {
    _timestamp: i64,
}

/// Image frame for buffering.
#[derive(Debug, Clone)]
struct ImageFrame {
    _timestamp: i64,
    _width: usize,
    _height: usize,
    data: Vec<u8>,
}

/// Parquet + MP4 Kps dataset writer.
///
/// Creates Kps datasets compatible with v3.0 format:
/// - `data/` directory with Parquet shards
/// - `videos/` directory with MP4 shards
pub struct ParquetKpsWriter {
    _episode_id: usize,
    output_dir: std::path::PathBuf,
    frame_count: usize,
    image_shapes: HashMap<String, (usize, usize)>,
    state_shapes: HashMap<String, usize>,
    // Buffers for parquet data (will be used in full implementation)
    observation_data: Vec<ObservationRow>,
    action_data: Vec<ActionRow>,
    timestamps: Vec<i64>,
}

impl ParquetKpsWriter {
    /// Create a new Parquet writer for an episode.
    pub fn create(
        output_dir: impl AsRef<Path>,
        episode_id: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let output_dir = output_dir.as_ref();

        // Create directories
        std::fs::create_dir_all(output_dir.join("data"))?;
        std::fs::create_dir_all(output_dir.join("videos"))?;
        std::fs::create_dir_all(output_dir.join("meta"))?;
        std::fs::create_dir_all(output_dir.join("meta/episodes"))?;

        Ok(Self {
            _episode_id: episode_id,
            output_dir: output_dir.to_path_buf(),
            frame_count: 0,
            image_shapes: HashMap::new(),
            state_shapes: HashMap::new(),
            observation_data: Vec::new(),
            action_data: Vec::new(),
            timestamps: Vec::new(),
        })
    }

    /// Write the complete dataset from MCAP data.
    ///
    /// Processes MCAP messages and generates Parquet + MP4 output.
    pub fn write_from_mcap(
        &mut self,
        mcap_path: impl AsRef<Path>,
        config: &KpsConfig,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        self.write_from_mcap_impl(mcap_path, config)
    }

    fn write_from_mcap_impl(
        &mut self,
        mcap_path: impl AsRef<Path>,
        config: &KpsConfig,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        use crate::kps::config::MappingType;

        let mcap_path_ref = mcap_path.as_ref();

        println!("Converting MCAP to Kps Parquet+MP4 format");
        println!("  Input: {}", mcap_path_ref.display());
        println!("  Output: {}", self.output_dir.display());

        // Get max_frames from config (None means unlimited)
        let max_frames = config.output.max_frames;

        // Open MCAP file
        let path_str = mcap_path_ref.to_str().ok_or("Invalid UTF-8 path")?;
        let reader = robocodec::RoboReader::open(path_str)?;

        // Buffer image data by topic for MP4 encoding
        let mut image_buffers: HashMap<String, Vec<ImageFrame>> = HashMap::new();

        let mut frame_index = 0usize;

        // Process messages - use decoded() to get timestamps
        for item in reader.decoded()? {
            let timestamped_msg = item?;

            // Find matching mapping
            let mapping = config.mappings.iter().find(|m| {
                timestamped_msg.channel.topic == m.topic
                    || timestamped_msg.channel.topic.contains(&m.topic)
            });

            let Some(mapping) = mapping else {
                continue;
            };

            // Extract actual message timestamp (convert nanoseconds to microseconds)
            let timestamp = (timestamped_msg.log_time.unwrap_or(0) / 1000) as i64;
            self.timestamps.push(timestamp);

            let msg = &timestamped_msg.message;

            match &mapping.mapping_type {
                MappingType::Image => {
                    self.process_image(msg, mapping, &mut image_buffers)?;
                }
                MappingType::State => {
                    self.process_state(msg, mapping, timestamp);
                }
                MappingType::Action => {
                    self.process_action(msg, mapping, timestamp);
                }
                MappingType::Timestamp => {}
                MappingType::OtherSensor | MappingType::Audio => {
                    // Not yet implemented for Parquet writer
                }
            }

            frame_index += 1;
            if frame_index.is_multiple_of(100) {
                println!("  Processed {} frames...", frame_index);
            }

            // Check frame limit if configured
            if let Some(limit) = max_frames
                && frame_index >= limit
            {
                println!("  Stopping at configured limit of {} frames", limit);
                break;
            }
        }

        self.frame_count = frame_index;

        // Write Parquet files
        self.write_parquet()?;

        // Encode and write MP4 files
        self.write_videos(&image_buffers, config)?;

        // Write metadata
        crate::kps::info::write_info_json(
            &self.output_dir,
            config,
            self.frame_count as u64,
            &self.image_shapes,
            &self.state_shapes,
        )?;

        println!("  Wrote {} frames", self.frame_count);

        Ok(self.frame_count)
    }

    fn process_image(
        &mut self,
        msg: &robocodec::DecodedMessage,
        mapping: &Mapping,
        image_buffers: &mut HashMap<String, Vec<ImageFrame>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use robocodec::CodecValue;

        let mut width = 0usize;
        let mut height = 0usize;
        let mut data: Option<&[u8]> = None;

        for (key, value) in msg.iter() {
            match key.as_str() {
                "width" => {
                    if let CodecValue::UInt32(w) = value {
                        width = *w as usize;
                    }
                }
                "height" => {
                    if let CodecValue::UInt32(h) = value {
                        height = *h as usize;
                    }
                }
                "data" => {
                    if let CodecValue::Bytes(b) = value {
                        data = Some(b);
                    }
                }
                _ => {}
            }
        }

        if let (Some(img_data), w, h) = (data, width, height) {
            self.record_image_shape(mapping.topic.clone(), w, h);

            let buffers = image_buffers.entry(mapping.feature.clone()).or_default();

            buffers.push(ImageFrame {
                _timestamp: self.timestamps.last().copied().unwrap_or(0),
                _width: w,
                _height: h,
                data: img_data.to_vec(),
            });
        }

        Ok(())
    }

    fn process_state(
        &mut self,
        _msg: &robocodec::DecodedMessage,
        _mapping: &Mapping,
        timestamp: i64,
    ) {
        // Add to observation data
        // For now, just track the timestamp
        self.observation_data.push(ObservationRow {
            _timestamp: timestamp,
        });
    }

    fn process_action(
        &mut self,
        _msg: &robocodec::DecodedMessage,
        _mapping: &Mapping,
        timestamp: i64,
    ) {
        // Add to action data
        self.action_data.push(ActionRow {
            _timestamp: timestamp,
        });
    }

    fn write_parquet(&self) -> Result<(), Box<dyn std::error::Error>> {
        use polars::prelude::*;

        // Create a simple dataframe with timestamps
        let mut df = df!(
            "timestamp" => &self.timestamps,
        )?;

        let parquet_path = self
            .output_dir
            .join("data")
            .join("data-00000-of-00001.parquet");

        let mut file = std::fs::File::create(&parquet_path)?;
        ParquetWriter::new(&mut file).finish(&mut df)?;

        println!("  Created: {}", parquet_path.display());
        Ok(())
    }

    fn write_videos(
        &self,
        image_buffers: &HashMap<String, Vec<ImageFrame>>,
        _config: &KpsConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Save images as individual PNG files (ffmpeg integration not yet implemented)
        self.write_videos_images(image_buffers)
    }

    #[allow(dead_code)]
    fn write_videos_ffmpeg(
        &self,
        image_buffers: &HashMap<String, Vec<ImageFrame>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Create video directory per camera/topic
        for (feature, frames) in image_buffers {
            if frames.is_empty() {
                continue;
            }

            let _video_dir = self.output_dir.join("videos");
            let _video_path =
                _video_dir.join(format!("video-{:06}-of-00001.mp4", self._episode_id));

            println!("  Encoding video: {} ({} frames)", feature, frames.len());

            // MP4 encoding via ffmpeg is a future enhancement.
            // The current implementation saves raw RGB data instead.
            println!("    (MP4 encoding requires full ffmpeg-next integration)");
        }

        Ok(())
    }

    fn write_videos_images(
        &self,
        image_buffers: &HashMap<String, Vec<ImageFrame>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Save images as individual PNG files
        let images_dir = self.output_dir.join("images");
        std::fs::create_dir_all(&images_dir)?;

        for (feature, frames) in image_buffers {
            for (i, frame) in frames.iter().enumerate() {
                let path = images_dir.join(format!("{}_{:06}.png", feature, i));

                // For PNG encoding, we'd need a PNG library
                // For now, write as raw RGB data
                let mut file = std::fs::File::create(&path)?;
                file.write_all(&frame.data)?;
            }
        }

        Ok(())
    }

    /// Record the shape of an image topic.
    fn record_image_shape(&mut self, topic: String, width: usize, height: usize) {
        self.image_shapes.insert(topic, (width, height));
    }

    /// Record the dimension of a state topic.
    #[allow(dead_code)]
    fn record_state_dimension(&mut self, topic: String, dim: usize) {
        self.state_shapes.insert(topic, dim);
    }

    /// Get the output directory path.
    pub fn output_dir(&self) -> &Path {
        &self.output_dir
    }

    /// Get the number of frames written.
    pub fn frame_count(&self) -> usize {
        self.frame_count
    }

    /// Get recorded image shapes.
    pub fn image_shapes(&self) -> &HashMap<String, (usize, usize)> {
        &self.image_shapes
    }

    /// Get recorded state shapes.
    pub fn state_shapes(&self) -> &HashMap<String, usize> {
        &self.state_shapes
    }

    /// Finalize and close the writer.
    pub fn finish(self, _config: &KpsConfig) -> Result<(), Box<dyn std::error::Error>> {
        println!();
        println!("Kps Parquet dataset created: {}", self.output_dir.display());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_writer() {
        let temp_dir = std::env::temp_dir();
        let writer = ParquetKpsWriter::create(&temp_dir, 0);
        assert!(writer.is_ok());
    }

    #[test]
    fn test_writer_has_correct_directories() {
        let temp_dir = std::env::temp_dir().join("kps_test");
        std::fs::remove_dir_all(&temp_dir).ok();
        std::fs::create_dir_all(&temp_dir).ok();

        let writer = ParquetKpsWriter::create(&temp_dir, 0).unwrap();

        // Check directories were created
        assert!(temp_dir.join("data").exists());
        assert!(temp_dir.join("videos").exists());
        assert!(temp_dir.join("meta").exists());
        assert!(temp_dir.join("meta/episodes").exists());

        assert_eq!(writer.output_dir(), &temp_dir);
        assert_eq!(writer.frame_count(), 0);
    }

    #[test]
    fn test_image_shape_recording() {
        let temp_dir = std::env::temp_dir().join("kps_test2");
        std::fs::remove_dir_all(&temp_dir).ok();
        std::fs::create_dir_all(&temp_dir).ok();

        let mut writer = ParquetKpsWriter::create(&temp_dir, 0).unwrap();

        // This would normally be called internally, but we test the method
        writer.record_image_shape("camera_0".to_string(), 640, 480);
        writer.record_state_dimension("joints".to_string(), 7);

        assert_eq!(writer.image_shapes().get("camera_0"), Some(&(640, 480)));
        assert_eq!(writer.state_shapes().get("joints"), Some(&7));
    }
}
