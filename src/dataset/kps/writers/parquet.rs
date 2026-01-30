// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Streaming Parquet writer for Kps datasets.
//!
//! This writer implements the [`DatasetWriter`] trait for Parquet format,
//! supporting frame-by-frame writing for pipeline integration.

use std::collections::HashMap;
use std::path::Path;

use crate::core::Result;
use crate::dataset::common::{AlignedFrame, DatasetWriter, ImageData, WriterStats};
use crate::dataset::kps::config::KpsConfig;

/// Streaming Parquet writer for Kps datasets.
///
/// This writer supports frame-by-frame writing for pipeline integration.
/// Data is buffered in memory and flushed to Parquet files periodically.
pub struct StreamingParquetWriter {
    /// Episode ID for this writer.
    episode_id: usize,

    /// Output directory path.
    output_dir: std::path::PathBuf,

    /// Number of frames written.
    frame_count: usize,

    /// Number of images encoded.
    images_encoded: usize,

    /// Number of state records written.
    state_records: usize,

    /// Whether initialized.
    initialized: bool,

    /// Image shapes tracking.
    image_shapes: HashMap<String, (usize, usize)>,

    /// State dimensions tracking.
    state_dims: HashMap<String, usize>,

    /// Kps config.
    config: Option<KpsConfig>,

    /// Start time for duration calculation.
    start_time: Option<std::time::Instant>,

    /// Buffer for observation data.
    observation_buffer: HashMap<String, Vec<f32>>,

    /// Buffer for action data.
    action_buffer: HashMap<String, Vec<f32>>,

    /// Buffer for image data (stored as raw bytes).
    image_buffer: HashMap<String, Vec<ImageData>>,

    /// Frames per Parquet file (sharding).
    frames_per_shard: usize,

    /// Output bytes written.
    output_bytes: u64,
}

impl StreamingParquetWriter {
    /// Create a new Parquet writer for the specified output directory.
    pub fn create(
        output_dir: impl AsRef<Path>,
        episode_id: usize,
        config: &KpsConfig,
    ) -> Result<Self> {
        let output_dir = output_dir.as_ref();

        // Create directory structure for Parquet format
        let data_dir = output_dir.join("data");
        let videos_dir = output_dir.join("videos");
        let meta_dir = output_dir.join("meta");

        std::fs::create_dir_all(&data_dir)?;
        std::fs::create_dir_all(&videos_dir)?;
        std::fs::create_dir_all(&meta_dir)?;

        Ok(Self {
            episode_id,
            output_dir: output_dir.to_path_buf(),
            frame_count: 0,
            images_encoded: 0,
            state_records: 0,
            initialized: false,
            image_shapes: HashMap::new(),
            state_dims: HashMap::new(),
            config: Some(config.clone()),
            start_time: None,
            observation_buffer: HashMap::new(),
            action_buffer: HashMap::new(),
            image_buffer: HashMap::new(),
            frames_per_shard: 10000, // Default shard size
            output_bytes: 0,
        })
    }

    /// Set the number of frames per Parquet shard.
    pub fn with_frames_per_shard(mut self, frames: usize) -> Self {
        self.frames_per_shard = frames;
        self
    }

    /// Write a Parquet file from buffered data.
    #[cfg(feature = "kps-parquet")]
    fn write_parquet_shard(&mut self) -> crate::core::Result<()> {
        use polars::prelude::*;

        if self.observation_buffer.is_empty() && self.action_buffer.is_empty() {
            return Ok(());
        }

        let shard_num = self.frame_count / self.frames_per_shard;

        // Create a DataFrame from buffered observations
        let mut series_vec = Vec::new();

        for (feature, values) in &self.observation_buffer {
            let series = Series::new(feature, values.as_slice());
            series_vec.push(series);
        }

        for (feature, values) in &self.action_buffer {
            let series = Series::new(feature, values.as_slice());
            series_vec.push(series);
        }

        if !series_vec.is_empty() {
            let df = DataFrame::new(series_vec)
                .map_err(|e| crate::RoboflowError::parse("Parquet", &format!("Failed to create DataFrame: {e}")))?;

            // Write to Parquet file
            let path = self
                .output_dir
                .join(format!("data/shard_{:04}.parquet", shard_num));

            let mut file = std::fs::File::create(&path)?;

            ParquetWriter::new(&mut file)
                .finish(&mut df.clone())
                .map_err(|e| {
                    crate::RoboflowError::parse("Parquet", &format!("Failed to write Parquet file: {e}"))
                })?;

            // Track output size
            if let Ok(metadata) = std::fs::metadata(&path) {
                self.output_bytes += metadata.len();
            }
        }

        // Clear buffers
        self.observation_buffer.clear();
        self.action_buffer.clear();

        Ok(())
    }

    /// Write metadata files (info.json, episode.jsonl).
    fn write_metadata_files(&self, config: &KpsConfig) -> crate::core::Result<()> {
        use crate::dataset::kps::info;

        // Write info.json
        info::write_info_json(
            &self.output_dir,
            config,
            self.frame_count as u64,
            &self.image_shapes,
            &self.state_dims,
        )
        .map_err(|e| crate::RoboflowError::parse("Parquet", &e.to_string()))?;

        // Write episode.jsonl
        info::write_episode_json(
            &self.output_dir,
            self.episode_id,
            0,
            self.frame_count as u64 * 1_000_000_000 / config.dataset.fps as u64,
            self.frame_count,
        )
        .map_err(|e| crate::RoboflowError::parse("Parquet", &e.to_string()))?;

        Ok(())
    }

    /// Process images for video encoding.
    ///
    /// Uses ffmpeg to encode buffered images as MP4 videos.
    /// Falls back to individual PPM files if ffmpeg is not available.
    fn process_images(&mut self) -> crate::core::Result<()> {
        use crate::dataset::kps::video_encoder::{Mp4Encoder, VideoFrame, VideoFrameBuffer};

        if self.image_buffer.is_empty() {
            return Ok(());
        }

        let videos_dir = self.output_dir.join("videos");
        std::fs::create_dir_all(&videos_dir)?;

        let fps = self.config.as_ref().map(|c| c.dataset.fps).unwrap_or(30);

        // Create encoder with FPS from config
        let encoder = Mp4Encoder::with_config(
            crate::dataset::kps::video_encoder::VideoEncoderConfig::default().with_fps(fps),
        );

        // Process each camera's images
        for (feature_name, images) in self.image_buffer.drain() {
            if images.is_empty() {
                continue;
            }

            let mut buffer = VideoFrameBuffer::new();

            // Convert ImageData to VideoFrame
            for img in images {
                if img.width > 0 && img.height > 0 {
                    let video_frame = VideoFrame::new(img.width, img.height, img.data);
                    // Try to add to buffer, skip if invalid
                    if buffer.add_frame(video_frame).is_err() {
                        tracing::warn!(
                            feature = %feature_name,
                            "Skipping invalid frame (inconsistent dimensions)"
                        );
                    }
                }
            }

            if !buffer.is_empty() {
                let clean_name = Self::sanitize_feature_name(&feature_name);

                match encoder.encode_buffer_or_save_images(&buffer, &videos_dir, &clean_name) {
                    Ok(output_paths) => {
                        self.images_encoded += buffer.len();
                        tracing::debug!(
                            feature = %feature_name,
                            frames = buffer.len(),
                            output = ?output_paths,
                            "Encoded camera images"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            feature = %feature_name,
                            error = %e,
                            "Failed to encode video, images will not be saved"
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Sanitize a feature name for use as a filename.
    fn sanitize_feature_name(name: &str) -> String {
        name.replace(['.', '/'], "_")
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect()
    }
}

impl DatasetWriter for StreamingParquetWriter {
    fn initialize(&mut self, config: &dyn std::any::Any) -> crate::core::Result<()> {
        let kps_config = config
            .downcast_ref::<KpsConfig>()
            .ok_or_else(|| {
                crate::RoboflowError::parse("DatasetWriter", "Expected KpsConfig for KPS writer")
            })?;

        // Store config
        self.config = Some(kps_config.clone());

        // Initialize buffers for each mapped feature
        for mapping in &kps_config.mappings {
            let feature_name = mapping
                .feature
                .strip_prefix("observation.")
                .or_else(|| mapping.feature.strip_prefix("action."))
                .unwrap_or(&mapping.feature);

            if mapping.feature.starts_with("observation.")
                && matches!(
                    mapping.mapping_type,
                    crate::dataset::kps::MappingType::State
                )
            {
                self.observation_buffer
                    .insert(feature_name.to_string(), Vec::new());
            } else if mapping.feature.starts_with("action.") {
                self.action_buffer
                    .insert(feature_name.to_string(), Vec::new());
            }
        }

        self.initialized = true;
        self.start_time = Some(std::time::Instant::now());

        Ok(())
    }

    fn write_frame(&mut self, frame: &AlignedFrame) -> crate::core::Result<()> {
        if !self.initialized {
            return Err(crate::RoboflowError::encode(
                "DatasetWriter",
                "Writer not initialized",
            ));
        }

        // Buffer states
        for (feature, values) in &frame.states {
            let feature_name = feature.strip_prefix("observation.").unwrap_or(feature);

            // Update dimension tracking
            self.state_dims
                .insert(feature_name.to_string(), values.len());

            if let Some(buffer) = self.observation_buffer.get_mut(feature_name) {
                buffer.extend(values);
            }
        }

        // Buffer actions
        for (feature, values) in &frame.actions {
            let feature_name = feature.strip_prefix("action.").unwrap_or(feature);

            // Update dimension tracking
            self.state_dims
                .insert(feature_name.to_string(), values.len());

            if let Some(buffer) = self.action_buffer.get_mut(feature_name) {
                buffer.extend(values);
            }
        }

        // Buffer images
        for (feature, data) in &frame.images {
            let feature_name = feature.strip_prefix("observation.").unwrap_or(feature);

            // Update shape tracking
            if data.width > 0 && data.height > 0 {
                self.image_shapes.insert(
                    feature_name.to_string(),
                    (data.width as usize, data.height as usize),
                );
            }

            self.image_buffer
                .entry(feature_name.to_string())
                .or_default()
                .push(data.clone());
        }

        self.frame_count += 1;
        self.state_records += frame.states.len() + frame.actions.len();

        // Check if we should write a shard
        if self.frame_count.is_multiple_of(self.frames_per_shard) {
            #[cfg(feature = "kps-parquet")]
            {
                self.write_parquet_shard()?;
            }
            self.process_images()?;
        }

        Ok(())
    }

    fn finalize(&mut self, config: &dyn std::any::Any) -> crate::core::Result<WriterStats> {
        let kps_config = config
            .downcast_ref::<KpsConfig>()
            .ok_or_else(|| {
                crate::RoboflowError::parse("DatasetWriter", "Expected KpsConfig for KPS writer")
            })?;

        // Write final shard
        #[cfg(feature = "kps-parquet")]
        {
            if !self.observation_buffer.is_empty() || !self.action_buffer.is_empty() {
                self.write_parquet_shard()?;
            }
        }

        // Process remaining images
        self.process_images()?;

        // Write metadata files
        self.write_metadata_files(kps_config)?;

        let duration = self
            .start_time
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0);

        Ok(WriterStats {
            frames_written: self.frame_count,
            images_encoded: self.images_encoded,
            state_records: self.state_records,
            output_bytes: self.output_bytes,
            duration_sec: duration,
        })
    }

    fn frame_count(&self) -> usize {
        self.frame_count
    }

    fn is_initialized(&self) -> bool {
        self.initialized
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_writer() {
        let temp_dir = std::env::temp_dir();
        let config = KpsConfig {
            dataset: crate::dataset::kps::DatasetConfig {
                name: "test".to_string(),
                fps: 30,
                robot_type: None,
            },
            mappings: vec![],
            output: crate::dataset::kps::OutputConfig::default(),
        };

        let result = StreamingParquetWriter::create(&temp_dir, 0, &config);
        #[cfg(feature = "kps-parquet")]
        assert!(result.is_ok());
        #[cfg(not(feature = "kps-parquet"))]
        assert!(result.is_err());
    }
}
