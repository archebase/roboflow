//! Kps Parquet + MP4 format writer.
//!
//! Writes Kps datasets in the v3.0 format:
//! - Tabular data in Parquet files
//! - Image data encoded as MP4 video files

use std::collections::HashMap;
use std::path::Path;

use super::config::KpsConfig;

#[cfg(feature = "kps-parquet")]
use super::config::Mapping;

#[cfg(feature = "kps-parquet")]
use std::io::Write;

// Work-in-progress: These structures will be used for full Parquet implementation
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct ObservationRow {
    timestamp: i64,
    // Observations will be stored as key-value pairs
    // For Parquet, we'll serialize to Arrow format
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct ActionRow {
    timestamp: i64,
    // Action values
}

// Work-in-progress: Will be used when storing image frames
#[allow(dead_code)]
struct ImageFrame {
    timestamp: i64,
    width: usize,
    height: usize,
    data: Vec<u8>,
}

/// Parquet + MP4 Kps dataset writer.
///
/// Creates Kps datasets compatible with v3.0 format:
/// - `data/` directory with Parquet shards
/// - `videos/` directory with MP4 shards
#[allow(dead_code)]
pub struct ParquetKpsWriter {
    episode_id: usize,
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
            episode_id,
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
        #[cfg(feature = "kps-parquet")]
        {
            self.write_from_mcap_impl(mcap_path, config)
        }
        #[cfg(not(feature = "kps-parquet"))]
        {
            // Note: parameters are unused when feature is disabled
            let _ = (mcap_path, config);
            Err(anyhow::anyhow!(
                "Parquet support not enabled. Add feature 'kps-parquet' to Cargo.toml"
            )
            .into())
        }
    }

    #[cfg(feature = "kps-parquet")]
    fn write_from_mcap_impl(
        &mut self,
        mcap_path: impl AsRef<Path>,
        config: &KpsConfig,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        use crate::format::kps::config::MappingType;

        let mcap_path_ref = mcap_path.as_ref();

        println!("Converting MCAP to Kps Parquet+MP4 format");
        println!("  Input: {}", mcap_path_ref.display());
        println!("  Output: {}", self.output_dir.display());

        // Get max_frames from config (None means unlimited)
        let max_frames = config.output.max_frames;

        // Open MCAP file
        let reader = crate::RoboReader::open(mcap_path_ref)?;

        // Buffer image data by topic for MP4 encoding
        let mut image_buffers: HashMap<String, Vec<ImageFrame>> = HashMap::new();

        let mut frame_index = 0usize;

        // Process messages
        let iter = reader.decode_messages()?;
        for result in iter {
            let (msg, _channel_info) = result?;

            // Find matching mapping
            let mapping = config
                .mappings
                .iter()
                .find(|m| _channel_info.topic == m.topic || _channel_info.topic.contains(&m.topic));

            let Some(mapping) = mapping else {
                continue;
            };

            // Extract actual message timestamp from raw message
            let timestamp = raw_msg.log_time / 1000; // Convert nanoseconds to microseconds
            self.timestamps.push(timestamp);

            match &mapping.mapping_type {
                MappingType::Image => {
                    self.process_image(&msg, mapping, &mut image_buffers)?;
                }
                MappingType::State => {
                    self.process_state(&msg, mapping, timestamp);
                }
                MappingType::Action => {
                    self.process_action(&msg, mapping, timestamp);
                }
                MappingType::Timestamp => {}
            }

            frame_index += 1;
            if frame_index.is_multiple_of(100) {
                println!("  Processed {} frames...", frame_index);
            }

            // Check frame limit if configured
            if let Some(limit) = max_frames {
                if frame_index >= limit {
                    println!("  Stopping at configured limit of {} frames", limit);
                    break;
                }
            }
        }

        self.frame_count = frame_index;

        // Write Parquet files
        self.write_parquet()?;

        // Encode and write MP4 files
        self.write_videos(&image_buffers, config)?;

        // Write metadata
        crate::format::kps::info::write_info_json(
            &self.output_dir,
            config,
            self.frame_count as u64,
            &self.image_shapes,
            &self.state_shapes,
        )?;

        println!("  Wrote {} frames", self.frame_count);

        Ok(self.frame_count)
    }

    #[cfg(feature = "kps-parquet")]
    fn process_image(
        &mut self,
        msg: &crate::DecodedMessage,
        mapping: &Mapping,
        image_buffers: &mut HashMap<String, Vec<ImageFrame>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::CodecValue;

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
                timestamp: self.timestamps.last().copied().unwrap_or(0),
                width: w,
                height: h,
                data: img_data.to_vec(),
            });
        }

        Ok(())
    }

    #[cfg(feature = "kps-parquet")]
    fn process_state(&mut self, _msg: &crate::DecodedMessage, _mapping: &Mapping, timestamp: i64) {
        // Add to observation data
        // For now, just track the timestamp
        self.observation_data.push(ObservationRow { timestamp });
    }

    #[cfg(feature = "kps-parquet")]
    fn process_action(&mut self, _msg: &crate::DecodedMessage, _mapping: &Mapping, timestamp: i64) {
        // Add to action data
        self.action_data.push(ActionRow { timestamp });
    }

    #[cfg(feature = "kps-parquet")]
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

    #[cfg(feature = "kps-parquet")]
    fn write_videos(
        &self,
        image_buffers: &HashMap<String, Vec<ImageFrame>>,
        _config: &KpsConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Save images as individual PNG files (ffmpeg integration not yet implemented)
        self.write_videos_images(image_buffers)
    }

    #[cfg(feature = "kps-parquet")]
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
            let _video_path = _video_dir.join(format!("video-{:06}-of-00001.mp4", self.episode_id));

            println!("  Encoding video: {} ({} frames)", feature, frames.len());

            // Note: Actual ffmpeg encoding requires significant code
            // For now, we'll write a placeholder
            println!("    (MP4 encoding requires full ffmpeg-next integration - placeholder only)");
        }

        Ok(())
    }

    #[cfg(feature = "kps-parquet")]
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
    #[allow(dead_code)]
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
