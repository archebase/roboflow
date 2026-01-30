// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! LeRobot v2.1 dataset writer.
//!
//! Writes robotics data in LeRobot v2.1 format with:
//! - Parquet files for frame data (one per episode)
//! - MP4 videos for camera observations (one per camera per episode)
//! - Complete metadata files

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::core::Result;
use crate::dataset::common::{AlignedFrame, DatasetWriter, ImageData, WriterStats};
use crate::dataset::common::parquet_base::calculate_stats;
use crate::dataset::common::video::{Mp4Encoder, VideoEncoderConfig, VideoFrame, VideoFrameBuffer};
use crate::dataset::kps::video_encoder::VideoEncoderError;
use crate::dataset::lerobot::config::LerobotConfig;
use crate::dataset::lerobot::metadata::MetadataCollector;
use crate::dataset::lerobot::trait_impl::{FromAlignedFrame, LerobotWriterTrait};

/// LeRobot v2.1 dataset writer.
pub struct LerobotWriter {
    /// Output directory
    output_dir: std::path::PathBuf,

    /// Configuration
    config: LerobotConfig,

    /// Current episode index
    episode_index: usize,

    /// Frame data for current episode
    frame_data: Vec<LerobotFrame>,

    /// Image buffers per camera
    image_buffers: HashMap<String, Vec<ImageData>>,

    /// Metadata collector
    metadata: MetadataCollector,

    /// Total frames written
    total_frames: usize,

    /// Total images encoded
    images_encoded: usize,

    /// Number of frames skipped due to dimension mismatches
    skipped_frames: usize,

    /// Whether the writer has been initialized
    initialized: bool,

    /// Start time for duration calculation
    start_time: Option<std::time::Instant>,

    /// Output bytes written
    output_bytes: u64,

    /// Number of videos that failed to encode
    failed_encodings: usize,
}

/// Frame data for LeRobot Parquet file.
#[derive(Debug)]
pub struct LerobotFrame {
    /// Episode index
    pub episode_index: usize,

    /// Frame index within episode
    pub frame_index: usize,

    /// Global frame index
    pub index: usize,

    /// Timestamp in seconds
    pub timestamp: f64,

    /// Observation state (joint positions)
    pub observation_state: Option<Vec<f32>>,

    /// Action (target joint positions)
    pub action: Option<Vec<f32>>,

    /// Task index
    pub task_index: Option<usize>,

    /// Image frame references (camera -> (path, timestamp))
    pub image_frames: HashMap<String, (String, f64)>,
}

impl LerobotWriter {
    /// Create a new LeRobot writer.
    pub fn create(output_dir: impl AsRef<Path>, config: LerobotConfig) -> Result<Self> {
        let output_dir = output_dir.as_ref();

        // Create LeRobot v2.1 directory structure
        let data_dir = output_dir.join("data/chunk-000");
        let videos_dir = output_dir.join("videos/chunk-000");
        let meta_dir = output_dir.join("meta");

        fs::create_dir_all(&data_dir)?;
        fs::create_dir_all(&videos_dir)?;
        fs::create_dir_all(&meta_dir)?;

        Ok(Self {
            output_dir: output_dir.to_path_buf(),
            config,
            episode_index: 0,
            frame_data: Vec::new(),
            image_buffers: HashMap::new(),
            metadata: MetadataCollector::new(),
            total_frames: 0,
            images_encoded: 0,
            skipped_frames: 0,
            initialized: false,
            start_time: None,
            output_bytes: 0,
            failed_encodings: 0,
        })
    }

    /// Add a frame to the current episode.
    pub fn add_frame(&mut self, frame: LerobotFrame) {
        // Update metadata
        if let Some(ref state) = frame.observation_state {
            self.metadata.update_state_dim("observation.state".to_string(), state.len());
        }
        if let Some(ref action) = frame.action {
            self.metadata.update_state_dim("action".to_string(), action.len());
        }

        // Store image data for video encoding
        for (_camera, _data) in &frame.image_frames {
            // Image data is added separately via add_image()
        }

        self.frame_data.push(frame);
    }

    /// Add image data for a camera frame.
    pub fn add_image(&mut self, camera: String, data: ImageData) {
        // Update shape metadata
        self.metadata.update_image_shape(camera.clone(), data.width as usize, data.height as usize);

        // Buffer for video encoding
        self.image_buffers.entry(camera).or_default().push(data);
    }

    /// Start a new episode.
    pub fn start_episode(&mut self, _task_index: Option<usize>) {
        self.episode_index = self.frame_data.len();
        // Episode index continues from where we left off
    }

    /// Finish the current episode and write its data.
    pub fn finish_episode(&mut self, task_index: Option<usize>) -> Result<()> {
        if self.frame_data.is_empty() {
            return Ok(());
        }

        let tasks = task_index.map(|t| vec![t]).unwrap_or_default();

        let start = std::time::Instant::now();
        // Write Parquet file
        self.write_episode_parquet()?;
        let parquet_time = start.elapsed();

        let start = std::time::Instant::now();
        // Encode videos
        self.encode_videos()?;
        let video_time = start.elapsed();

        eprintln!(
            "[TIMING] finish_episode: parquet={:.1}ms, video={:.1}ms",
            parquet_time.as_secs_f64() * 1000.0,
            video_time.as_secs_f64() * 1000.0,
        );

        // Calculate and store episode stats
        self.calculate_episode_stats()?;

        // Update metadata
        self.metadata.add_episode(self.episode_index, self.frame_data.len(), tasks);

        // Update counters
        self.total_frames += self.frame_data.len();

        // Clear for next episode
        self.frame_data.clear();
        for buffer in self.image_buffers.values_mut() {
            buffer.clear();
        }

        self.episode_index += 1;

        Ok(())
    }

    /// Write current episode to Parquet file.
    #[cfg(feature = "kps-parquet")]
    fn write_episode_parquet(&mut self) -> Result<()> {
        use std::fs::File;
        use std::io::BufWriter;
        use polars::prelude::*;

        if self.frame_data.is_empty() {
            return Ok(());
        }

        let state_dim = self
            .frame_data
            .first()
            .and_then(|f| f.observation_state.as_ref())
            .map(|v| v.len())
            .ok_or_else(|| {
                crate::RoboflowError::encode(
                    "LerobotWriter",
                    "Cannot determine state dimension: first frame has no observation_state",
                )
            })?;

        let mut episode_index: Vec<i64> = Vec::new();
        let mut frame_index: Vec<i64> = Vec::new();
        let mut index: Vec<i64> = Vec::new();
        let mut timestamp: Vec<f64> = Vec::new();
        let mut observation_state: Vec<Vec<f32>> = Vec::new();
        let mut action: Vec<Vec<f32>> = Vec::new();
        let mut task_index: Vec<i64> = Vec::new();

        // Collect camera names from image_frames
        let mut cameras: Vec<String> = Vec::new();
        for frame in &self.frame_data {
            for camera in frame.image_frames.keys() {
                if !cameras.contains(camera) {
                    cameras.push(camera.clone());
                }
            }
        }

        // Image frame references per camera
        let mut image_paths: HashMap<String, Vec<String>> = HashMap::new();
        let mut image_timestamps: HashMap<String, Vec<f64>> = HashMap::new();
        for camera in &cameras {
            image_paths.insert(camera.clone(), Vec::new());
            image_timestamps.insert(camera.clone(), Vec::new());
        }

        for frame in &self.frame_data {
            episode_index.push(frame.episode_index as i64);
            frame_index.push(frame.frame_index as i64);
            index.push(frame.index as i64);
            timestamp.push(frame.timestamp);

            if let Some(ref state) = frame.observation_state {
                observation_state.push(state.clone());
            }
            if let Some(ref act) = frame.action {
                action.push(act.clone());
            }

            task_index.push(frame.task_index.map(|t| t as i64).unwrap_or(0));

            for camera in &cameras {
                if let Some((path, ts)) = frame.image_frames.get(camera) {
                    if let Some(paths) = image_paths.get_mut(camera) {
                        paths.push(path.clone());
                    }
                    if let Some(timestamps) = image_timestamps.get_mut(camera) {
                        timestamps.push(*ts);
                    }
                } else {
                    // Default path if image not available
                    let path = format!(
                        "videos/chunk-000/observation.images.{}/episode_{:06}.mp4",
                        camera, self.episode_index
                    );
                    if let Some(paths) = image_paths.get_mut(camera) {
                        paths.push(path);
                    }
                    if let Some(timestamps) = image_timestamps.get_mut(camera) {
                        timestamps.push(frame.timestamp);
                    }
                }
            }
        }

        // Build Parquet columns
        let mut series_vec = vec![
            Series::new("episode_index", episode_index),
            Series::new("frame_index", frame_index),
            Series::new("index", index),
            Series::new("timestamp", timestamp),
        ];

        // Add observation state columns
        for i in 0..state_dim {
            let col_name = format!("observation.state.{}", i);
            let values: Vec<f32> = observation_state.iter().map(|v| v.get(i).copied().unwrap_or(0.0)).collect();
            series_vec.push(Series::new(&col_name, values));
        }

        // Add action columns
        for i in 0..state_dim {
            let col_name = format!("action.{}", i);
            let values: Vec<f32> = action.iter().map(|v| v.get(i).copied().unwrap_or(0.0)).collect();
            series_vec.push(Series::new(&col_name, values));
        }

        // Add task_index
        series_vec.push(Series::new("task_index", task_index));

        // Add image frame references
        for camera in &cameras {
            let feature_name = format!("observation.images.{}", camera);
            if let Some(paths) = image_paths.get(camera) {
                series_vec.push(Series::new(format!("{}_path", feature_name).as_str(), paths.clone()));
            }
            if let Some(timestamps) = image_timestamps.get(camera) {
                series_vec.push(Series::new(format!("{}_timestamp", feature_name).as_str(), timestamps.clone()));
            }
        }

        // Create DataFrame and write
        let df = DataFrame::new(series_vec)
            .map_err(|e| crate::RoboflowError::parse("Parquet", &format!("DataFrame error: {}", e)))?;

        let parquet_path = self.output_dir.join(format!(
            "data/chunk-000/episode_{:06}.parquet",
            self.episode_index
        ));

        let file = File::create(&parquet_path)?;
        let mut writer = BufWriter::new(file);

        ParquetWriter::new(&mut writer)
            .finish(&mut df.clone())
            .map_err(|e| crate::RoboflowError::parse("Parquet", &format!("Write error: {}", e)))?;

        // Track output bytes
        if let Ok(metadata) = std::fs::metadata(&parquet_path) {
            self.output_bytes += metadata.len();
        }

        tracing::info!(
            path = %parquet_path.display(),
            frames = self.frame_data.len(),
            "Wrote LeRobot v2.1 Parquet file"
        );

        Ok(())
    }

    /// Write current episode to Parquet file (fallback when feature not enabled).
    #[cfg(not(feature = "kps-parquet"))]
    fn write_episode_parquet(&mut self) -> Result<()> {
        // Parquet support not enabled - return error instead of silently skipping
        return Err(crate::RoboflowError::unsupported(
            "Parquet writing requires the 'kps-parquet' feature to be enabled. \
             Add --features kps-parquet to your build command."
        ));
    }

    /// Encode videos for all cameras.
    fn encode_videos(&mut self) -> Result<()> {
        if self.image_buffers.is_empty() {
            return Ok(());
        }

        let total_start = std::time::Instant::now();
        let mut encode_time = std::time::Duration::ZERO;
        let mut buffer_time = std::time::Duration::ZERO;

        let videos_dir = self.output_dir.join("videos/chunk-000");
        let encoder_config = VideoEncoderConfig {
            codec: self.config.video.codec.clone(),
            fps: self.config.dataset.fps,
            crf: self.config.video.crf,
            preset: self.config.video.preset.clone(),
            ..Default::default()
        };

        let encoder = Mp4Encoder::with_config(encoder_config);

        for (camera, images) in self.image_buffers.iter() {
            if images.is_empty() {
                continue;
            }

            let buffer_start = std::time::Instant::now();
            let mut buffer = VideoFrameBuffer::new();

            for img in images {
                if img.width > 0 && img.height > 0 {
                    let rgb_data = if img.is_encoded {
                        // For now, assume we need to decode or use as-is
                        // In production, use image crate to decode JPEG/PNG
                        img.data.clone()
                    } else {
                        img.data.clone()
                    };

                    let video_frame = VideoFrame::new(img.width, img.height, rgb_data);
                    if let Err(e) = buffer.add_frame(video_frame) {
                        // Track skipped frames with better context
                        tracing::warn!(
                            camera = %camera,
                            expected_width = buffer.width.unwrap_or(0),
                            expected_height = buffer.height.unwrap_or(0),
                            actual_width = img.width,
                            actual_height = img.height,
                            error = %e,
                            "Skipping frame with inconsistent dimensions"
                        );
                        self.skipped_frames += 1;
                    }
                }
            }
            buffer_time += buffer_start.elapsed();

            if !buffer.is_empty() {
                let feature_name = format!("observation.images.{}", camera);
                let camera_dir = videos_dir.join(&feature_name);
                fs::create_dir_all(&camera_dir)?;

                let video_path = camera_dir.join(format!("episode_{:06}.mp4", self.episode_index));

                let encode_start = std::time::Instant::now();
                match encoder.encode_buffer(&buffer, &video_path) {
                    Ok(()) => {
                        self.images_encoded += buffer.len();
                        tracing::debug!(
                            camera = %camera,
                            frames = buffer.len(),
                            path = %video_path.display(),
                            "Encoded MP4 video"
                        );
                    }
                    Err(VideoEncoderError::FfmpegNotFound) => {
                        tracing::error!(
                            "ffmpeg not found. Please install ffmpeg to encode videos. \
                             Camera '{}' videos will not be available in the dataset.",
                            camera
                        );
                        self.failed_encodings += 1;
                        return Err(crate::RoboflowError::unsupported(
                            "Video encoding requires ffmpeg. Install ffmpeg and ensure it's in your PATH."
                        ));
                    }
                    Err(e) => {
                        tracing::error!(
                            camera = %camera,
                            error = %e,
                            "Failed to encode video"
                        );
                        self.failed_encodings += 1;
                        return Err(crate::RoboflowError::encode(
                            "VideoEncoder",
                            format!("Failed to encode video for camera '{}': {}", camera, e)
                        ));
                    }
                }
                encode_time += encode_start.elapsed();

                // Track output bytes for video file
                if let Ok(metadata) = std::fs::metadata(&video_path) {
                    self.output_bytes += metadata.len();
                }
            }
        }

        eprintln!(
            "[TIMING] encode_videos: total={:.1}ms, buffer={:.1}ms, encode={:.1}ms",
            total_start.elapsed().as_secs_f64() * 1000.0,
            buffer_time.as_secs_f64() * 1000.0,
            encode_time.as_secs_f64() * 1000.0,
        );

        Ok(())
    }

    /// Calculate episode statistics.
    fn calculate_episode_stats(&mut self) -> Result<()> {
        if self.frame_data.is_empty() {
            return Ok(());
        }

        let mut stats = HashMap::new();

        // Calculate observation.state stats
        let state_values: Vec<Vec<f32>> = self.frame_data
            .iter()
            .filter_map(|f| f.observation_state.as_ref())
            .cloned()
            .collect();

        if let Some(feature_stats) = calculate_stats(&state_values) {
            stats.insert("observation.state".to_string(), feature_stats);
        }

        // Calculate action stats
        let action_values: Vec<Vec<f32>> = self.frame_data
            .iter()
            .filter_map(|f| f.action.as_ref())
            .cloned()
            .collect();

        if let Some(feature_stats) = calculate_stats(&action_values) {
            stats.insert("action".to_string(), feature_stats);
        }

        self.metadata.add_episode_stats(self.episode_index, stats);

        Ok(())
    }

    /// Finalize the dataset and write metadata files.
    pub fn finalize(mut self) -> Result<usize> {
        // Finish any remaining episode
        if !self.frame_data.is_empty() {
            self.finish_episode(None)?;
        }

        // Write metadata files
        self.metadata.write_all(&self.output_dir, &self.config)?;

        let duration = self
            .start_time
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0);

        tracing::info!(
            output_dir = %self.output_dir.display(),
            episodes = self.episode_index,
            frames = self.total_frames,
            images_encoded = self.images_encoded,
            skipped_frames = self.skipped_frames,
            output_bytes = self.output_bytes,
            duration_sec = duration,
            "Finalized LeRobot v2.1 dataset"
        );

        // Warn if there were any skipped frames or failed encodings
        if self.skipped_frames > 0 {
            tracing::warn!(
                "{} frames were skipped due to dimension mismatches",
                self.skipped_frames
            );
        }

        Ok(self.total_frames)
    }

    /// Get total frames written so far.
    pub fn frame_count(&self) -> usize {
        self.total_frames + self.frame_data.len()
    }

    /// Register a task and return its index.
    pub fn register_task(&mut self, task: String) -> usize {
        self.metadata.register_task(task)
    }

    /// Get reference to metadata collector.
    pub fn metadata(&self) -> &MetadataCollector {
        &self.metadata
    }

    /// Check if the writer has been initialized.
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Get the number of skipped frames.
    pub fn skipped_frames(&self) -> usize {
        self.skipped_frames
    }

    /// Get the number of failed video encodings.
    pub fn failed_encodings(&self) -> usize {
        self.failed_encodings
    }
}

/// Implement the core DatasetWriter trait for LerobotWriter.
impl DatasetWriter for LerobotWriter {
    fn initialize(&mut self, config: &dyn std::any::Any) -> Result<()> {
        if let Some(lerobot_config) = config.downcast_ref::<LerobotConfig>() {
            self.config = lerobot_config.clone();
        }
        self.initialized = true;
        self.start_time = Some(std::time::Instant::now());
        Ok(())
    }

    fn write_frame(&mut self, frame: &AlignedFrame) -> Result<()> {
        if !self.initialized {
            return Err(crate::RoboflowError::encode(
                "LerobotWriter",
                "Writer not initialized. Call initialize() before write_frame().",
            ));
        }

        // Convert AlignedFrame to LerobotFrame
        let lerobot_frame = LerobotFrame::from_aligned_frame(frame, self.episode_index);

        // Add the frame
        self.add_frame(lerobot_frame);

        // Add images
        for (camera, data) in &frame.images {
            self.add_image(camera.clone(), data.clone());
        }

        Ok(())
    }

    fn finalize(&mut self, config: &dyn std::any::Any) -> Result<WriterStats> {
        // Update config if provided
        if let Some(lerobot_config) = config.downcast_ref::<LerobotConfig>() {
            self.config = lerobot_config.clone();
        }

        // Finish any remaining episode
        if !self.frame_data.is_empty() {
            self.finish_episode(None)?;
        }

        // Write metadata files
        self.metadata.write_all(&self.output_dir, &self.config)?;

        let duration = self
            .start_time
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0);

        tracing::info!(
            output_dir = %self.output_dir.display(),
            episodes = self.episode_index,
            frames = self.total_frames,
            images_encoded = self.images_encoded,
            skipped_frames = self.skipped_frames,
            output_bytes = self.output_bytes,
            duration_sec = duration,
            "Finalized LeRobot v2.1 dataset"
        );

        // Warn if there were any skipped frames or failed encodings
        if self.skipped_frames > 0 {
            tracing::warn!(
                "{} frames were skipped due to dimension mismatches",
                self.skipped_frames
            );
        }

        Ok(WriterStats {
            frames_written: self.total_frames,
            images_encoded: self.images_encoded,
            state_records: self.total_frames * 2, // state + action
            output_bytes: self.output_bytes,
            duration_sec: duration,
        })
    }

    fn frame_count(&self) -> usize {
        self.total_frames + self.frame_data.len()
    }

    fn is_initialized(&self) -> bool {
        self.initialized
    }
}

/// Implement the LeRobot-specific trait for LerobotWriter.
impl LerobotWriterTrait for LerobotWriter {
    fn start_episode(&mut self, _task_index: Option<usize>) {
        self.episode_index = self.frame_data.len();
    }

    fn finish_episode(&mut self, task_index: Option<usize>) -> Result<()> {
        if self.frame_data.is_empty() {
            return Ok(());
        }

        let tasks = task_index.map(|t| vec![t]).unwrap_or_default();

        // Write Parquet file
        self.write_episode_parquet()?;

        // Encode videos
        self.encode_videos()?;

        // Calculate and store episode stats
        self.calculate_episode_stats()?;

        // Update metadata
        self.metadata.add_episode(self.episode_index, self.frame_data.len(), tasks);

        // Update counters
        self.total_frames += self.frame_data.len();

        // Clear for next episode
        self.frame_data.clear();
        for buffer in self.image_buffers.values_mut() {
            buffer.clear();
        }

        self.episode_index += 1;

        Ok(())
    }

    fn register_task(&mut self, task: String) -> usize {
        self.metadata.register_task(task)
    }

    fn add_frame(&mut self, frame: &AlignedFrame) -> Result<()> {
        <LerobotWriter as DatasetWriter>::write_frame(self, frame)
    }

    fn add_image(&mut self, camera: String, data: ImageData) {
        // Update shape metadata
        self.metadata.update_image_shape(camera.clone(), data.width as usize, data.height as usize);

        // Buffer for video encoding
        self.image_buffers.entry(camera).or_default().push(data);
    }

    fn metadata(&self) -> &MetadataCollector {
        &self.metadata
    }

    fn frame_count(&self) -> usize {
        self.total_frames + self.frame_data.len()
    }
}

/// Implement conversion from AlignedFrame to LerobotFrame.
impl FromAlignedFrame for LerobotFrame {
    fn from_aligned_frame(frame: &AlignedFrame, episode_index: usize) -> Self {
        // Extract observation state from the aligned frame
        let observation_state = frame
            .states
            .iter()
            .find(|(k, _)| k.contains("observation") || k.contains("state"))
            .map(|(_, v)| v.clone());

        // Extract action from the aligned frame
        let action = frame
            .actions
            .values()
            .next()
            .cloned();

        // Build image frame references
        let mut image_frames = HashMap::new();
        for (camera, _data) in &frame.images {
            let path = format!(
                "videos/chunk-000/observation.images.{}/episode_{:06}.mp4",
                camera, episode_index
            );
            image_frames.insert(camera.clone(), (path, frame.timestamp_sec()));
        }

        Self {
            episode_index,
            frame_index: frame.frame_index,
            index: frame.frame_index,
            timestamp: frame.timestamp_sec(),
            observation_state,
            action,
            task_index: None,
            image_frames,
        }
    }
}
