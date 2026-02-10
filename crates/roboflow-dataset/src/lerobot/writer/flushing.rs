// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Incremental flushing for bounded memory footprint.
//!
//! This module implements chunk-based writing that flushes data incrementally
//! instead of buffering entire episodes in memory. This is critical for
//! long recordings that would otherwise exhaust memory.

use std::collections::HashMap;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use polars::prelude::*;

use roboflow_core::{Result, RoboflowError};

use super::frame::LerobotFrame;
use crate::common::ImageData;
use crate::common::video::{VideoEncoderConfig, VideoFrame};
use crate::lerobot::video_profiles::ResolvedConfig;

/// Configuration for incremental flushing.
#[derive(Debug, Clone)]
pub struct FlushingConfig {
    /// Maximum frames per chunk before auto-flush (0 = unlimited).
    pub max_frames_per_chunk: usize,

    /// Maximum memory bytes per chunk before auto-flush (0 = unlimited).
    pub max_memory_bytes: usize,

    /// Whether to encode videos incrementally (per-chunk).
    pub incremental_video_encoding: bool,
}

impl Default for FlushingConfig {
    fn default() -> Self {
        Self {
            max_frames_per_chunk: 1000,
            max_memory_bytes: 2 * 1024 * 1024 * 1024, // 2GB
            incremental_video_encoding: true,
        }
    }
}

impl FlushingConfig {
    /// Create a config with unlimited buffering (legacy behavior).
    pub fn unlimited() -> Self {
        Self {
            max_frames_per_chunk: 0,
            max_memory_bytes: 0,
            incremental_video_encoding: false,
        }
    }

    /// Create a config with frame-based limiting.
    pub fn with_max_frames(max_frames: usize) -> Self {
        Self {
            max_frames_per_chunk: max_frames,
            ..Default::default()
        }
    }

    /// Create a config with memory-based limiting.
    pub fn with_max_memory(bytes: usize) -> Self {
        Self {
            max_memory_bytes: bytes,
            ..Default::default()
        }
    }

    /// Check if flushing should occur based on current state.
    pub fn should_flush(&self, frame_count: usize, memory_bytes: usize) -> bool {
        if self.max_frames_per_chunk > 0 && frame_count >= self.max_frames_per_chunk {
            return true;
        }
        if self.max_memory_bytes > 0 && memory_bytes >= self.max_memory_bytes {
            return true;
        }
        false
    }

    /// Is this config actually limiting (vs unlimited)?
    pub fn is_limited(&self) -> bool {
        self.max_frames_per_chunk > 0 || self.max_memory_bytes > 0
    }
}

/// Statistics for chunk writing.
#[derive(Debug, Default)]
pub struct ChunkStats {
    /// Number of chunks written
    pub chunks_written: usize,
    /// Total frames written
    pub total_frames: usize,
    /// Total bytes written (videos only)
    pub total_video_bytes: u64,
    /// Total parquet bytes
    pub total_parquet_bytes: u64,
}

/// Metadata about a written chunk.
#[derive(Debug, Clone)]
pub struct ChunkMetadata {
    /// Chunk index (0-based)
    pub index: usize,
    /// Start frame index (global)
    pub start_frame: usize,
    /// End frame index (exclusive)
    pub end_frame: usize,
    /// Number of frames in this chunk
    pub frame_count: usize,
    /// Parquet file path
    pub parquet_path: PathBuf,
    /// Video files: (path, camera_name)
    pub video_files: Vec<(PathBuf, String)>,
    /// Estimated memory usage at flush time
    pub memory_bytes: usize,
}

/// Manages incremental flushing of episode data to chunks.
pub struct IncrementalFlusher {
    /// Output directory for the dataset
    output_dir: PathBuf,

    /// Episode index
    episode_index: usize,

    /// Flushing configuration
    config: FlushingConfig,

    /// Video encoding configuration
    video_config: ResolvedConfig,

    /// FPS for video encoding
    fps: u32,

    /// Whether using cloud storage (affects upload queuing)
    use_cloud_storage: bool,

    /// Current chunk index
    current_chunk: usize,

    /// Current frame buffer for this chunk
    frame_buffer: Vec<LerobotFrame>,

    /// Current image buffers per camera (camera_name -> Vec<ImageData>)
    image_buffers: HashMap<String, Vec<ImageData>>,

    /// Statistics
    stats: ChunkStats,

    /// Chunk metadata tracking
    chunk_metadata: Vec<ChunkMetadata>,
}

impl IncrementalFlusher {
    /// Create a new incremental flusher.
    pub fn new(
        output_dir: PathBuf,
        episode_index: usize,
        config: FlushingConfig,
        video_config: ResolvedConfig,
        fps: u32,
        use_cloud_storage: bool,
    ) -> Self {
        Self {
            output_dir,
            episode_index,
            config,
            video_config,
            fps,
            use_cloud_storage,
            current_chunk: 0,
            frame_buffer: Vec::new(),
            image_buffers: HashMap::new(),
            stats: ChunkStats::default(),
            chunk_metadata: Vec::new(),
        }
    }

    /// Add a frame to the buffer. Returns Some(chunk_metadata) if a flush occurred.
    pub fn add_frame(&mut self, frame: LerobotFrame) -> Result<Option<ChunkMetadata>> {
        self.frame_buffer.push(frame);
        self.stats.total_frames += 1;

        // Check if we should flush
        if self
            .config
            .should_flush(self.frame_buffer.len(), self.estimate_memory())
        {
            self.flush_chunk()
        } else {
            Ok(None)
        }
    }

    /// Add an image to a camera buffer.
    pub fn add_image(&mut self, camera: String, image: ImageData) {
        self.image_buffers.entry(camera).or_default().push(image);
    }

    /// Estimate current memory usage in bytes.
    fn estimate_memory(&self) -> usize {
        let mut total = 0usize;

        // Frame data (rough estimate)
        total += self.frame_buffer.len() * 512; // Per-frame overhead

        // Image data
        for images in self.image_buffers.values() {
            for img in images {
                total += img.data.len();
            }
        }

        total
    }

    /// Flush current chunk to disk and return metadata.
    pub fn flush_chunk(&mut self) -> Result<Option<ChunkMetadata>> {
        if self.frame_buffer.is_empty() && self.image_buffers.is_empty() {
            return Ok(None);
        }

        let start_frame = self.stats.total_frames - self.frame_buffer.len();
        let frame_count = self.frame_buffer.len();
        let memory_bytes = self.estimate_memory();

        tracing::info!(
            chunk = self.current_chunk,
            frames = frame_count,
            memory_mb = memory_bytes / (1024 * 1024),
            cameras = self.image_buffers.len(),
            "Flushing chunk"
        );

        // Create chunk directory structure
        let chunk_dir = self
            .output_dir
            .join(format!("videos/chunk-{:03}", self.current_chunk));
        fs::create_dir_all(&chunk_dir)
            .map_err(|e| RoboflowError::io(format!("Failed to create chunk directory: {}", e)))?;

        // Create data directory for parquet
        let data_dir = self.output_dir.join("data");
        fs::create_dir_all(&data_dir)
            .map_err(|e| RoboflowError::io(format!("Failed to create data directory: {}", e)))?;

        let data_chunk_dir = data_dir.join(format!("chunk-{:03}", self.current_chunk));
        fs::create_dir_all(&data_chunk_dir).map_err(|e| {
            RoboflowError::io(format!("Failed to create data chunk directory: {}", e))
        })?;

        // Write parquet for this chunk
        let parquet_path = if !self.frame_buffer.is_empty() {
            self.write_chunk_parquet(&data_chunk_dir)?
        } else {
            PathBuf::new()
        };

        // Encode videos for this chunk (if enabled)
        let video_files =
            if self.config.incremental_video_encoding && !self.image_buffers.is_empty() {
                self.encode_chunk_videos(&chunk_dir)?
            } else {
                Vec::new()
            };

        let metadata = ChunkMetadata {
            index: self.current_chunk,
            start_frame,
            end_frame: start_frame + frame_count,
            frame_count,
            parquet_path: parquet_path.clone(),
            video_files: video_files.clone(),
            memory_bytes,
        };

        self.chunk_metadata.push(metadata.clone());
        self.stats.chunks_written += 1;
        self.current_chunk += 1;

        // Clear buffers
        self.frame_buffer.clear();
        self.image_buffers.clear();

        // Track sizes
        if let Ok(meta) = fs::metadata(&parquet_path) {
            self.stats.total_parquet_bytes += meta.len();
        }
        for (path, _) in &video_files {
            if let Ok(meta) = fs::metadata(path) {
                self.stats.total_video_bytes += meta.len();
            }
        }

        Ok(Some(metadata))
    }

    /// Write parquet for current chunk.
    fn write_chunk_parquet(&self, chunk_dir: &Path) -> Result<PathBuf> {
        if self.frame_buffer.is_empty() {
            return Ok(PathBuf::new());
        }

        let frame_data = &self.frame_buffer;
        let episode_index = self.episode_index;
        let chunk_index = self.current_chunk;

        // Find state dimension
        let state_dim = frame_data
            .iter()
            .find_map(|f| f.observation_state.as_ref())
            .map(|v| v.len())
            .ok_or_else(|| {
                RoboflowError::encode(
                    "IncrementalFlusher",
                    "Cannot determine state dimension: no frame has observation_state",
                )
            })?;

        let mut episode_index_vec: Vec<i64> = Vec::new();
        let mut frame_index: Vec<i64> = Vec::new();
        let mut index: Vec<i64> = Vec::new();
        let mut timestamp: Vec<f64> = Vec::new();
        let mut observation_state: Vec<Vec<f32>> = Vec::new();
        let mut action: Vec<Vec<f32>> = Vec::new();
        let mut task_index: Vec<i64> = Vec::new();

        // Collect camera names
        let mut cameras: Vec<String> = Vec::new();
        for frame in frame_data {
            for camera in frame.image_frames.keys() {
                if !cameras.contains(camera) {
                    cameras.push(camera.clone());
                }
            }
        }

        let mut image_paths: HashMap<String, Vec<String>> = HashMap::new();
        let mut image_timestamps: HashMap<String, Vec<f64>> = HashMap::new();
        for camera in &cameras {
            image_paths.insert(camera.clone(), Vec::new());
            image_timestamps.insert(camera.clone(), Vec::new());
        }

        let mut last_action: Option<Vec<f32>> = None;

        for frame in frame_data {
            if frame.observation_state.is_none() {
                continue;
            }

            episode_index_vec.push(frame.episode_index as i64);
            frame_index.push(frame.frame_index as i64);
            index.push(frame.index as i64);
            timestamp.push(frame.timestamp);

            if let Some(ref state) = frame.observation_state {
                observation_state.push(state.clone());
            }

            let act = frame.action.as_ref().or(last_action.as_ref());
            if let Some(a) = act {
                action.push(a.clone());
                last_action = Some(a.clone());
            } else if !observation_state.is_empty() {
                let dim = observation_state.last().map_or(14, |s| s.len().min(14));
                action.push(vec![0.0; dim]);
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
                    // Reference to chunk-specific video
                    let path = format!(
                        "videos/chunk-{:03}/{}/episode_{:06}.mp4",
                        chunk_index, camera, episode_index
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

        // Build parquet columns
        let mut series_vec = vec![
            Series::new("episode_index", episode_index_vec),
            Series::new("frame_index", frame_index),
            Series::new("index", index),
            Series::new("timestamp", timestamp),
        ];

        for i in 0..state_dim {
            let col_name = format!("observation.state.{}", i);
            let values: Vec<f32> = observation_state
                .iter()
                .map(|v| v.get(i).copied().unwrap_or(0.0))
                .collect();
            series_vec.push(Series::new(&col_name, values));
        }

        let action_dim = action
            .iter()
            .find(|v| !v.is_empty())
            .map(|v| v.len())
            .unwrap_or(14);
        for i in 0..action_dim {
            let col_name = format!("action.{}", i);
            let values: Vec<f32> = action
                .iter()
                .map(|v| v.get(i).copied().unwrap_or(0.0))
                .collect();
            series_vec.push(Series::new(&col_name, values));
        }

        series_vec.push(Series::new("task_index", task_index));

        for camera in &cameras {
            if let Some(paths) = image_paths.get(camera) {
                series_vec.push(Series::new(
                    format!("{}_path", camera).as_str(),
                    paths.clone(),
                ));
            }
            if let Some(timestamps) = image_timestamps.get(camera) {
                series_vec.push(Series::new(
                    format!("{}_timestamp", camera).as_str(),
                    timestamps.clone(),
                ));
            }
        }

        let df = DataFrame::new(series_vec)
            .map_err(|e| RoboflowError::parse("Parquet", format!("DataFrame error: {}", e)))?;

        let parquet_path = chunk_dir.join(format!("episode_{:06}.parquet", episode_index));

        let file = fs::File::create(&parquet_path)?;
        let mut writer = BufWriter::new(file);

        ParquetWriter::new(&mut writer)
            .finish(&mut df.clone())
            .map_err(|e| RoboflowError::parse("Parquet", format!("Write error: {}", e)))?;

        tracing::info!(
            path = %parquet_path.display(),
            frames = frame_data.len(),
            "Wrote chunk parquet"
        );

        Ok(parquet_path)
    }

    /// Encode videos for current chunk.
    fn encode_chunk_videos(&self, chunk_dir: &Path) -> Result<Vec<(PathBuf, String)>> {
        use crate::common::video::Mp4Encoder;
        use crate::lerobot::writer::encoding::build_frame_buffer_static;

        let encoder_config = self.video_config.to_encoder_config(self.fps);
        let mut video_files = Vec::new();

        for (camera, images) in &self.image_buffers {
            if images.is_empty() {
                continue;
            }

            let camera_dir = chunk_dir.join(camera);
            fs::create_dir_all(&camera_dir)?;

            let (buffer, _skipped) = build_frame_buffer_static(images)?;
            if buffer.is_empty() {
                continue;
            }

            let video_path = camera_dir.join(format!("episode_{:06}.mp4", self.episode_index));

            let encoder = Mp4Encoder::with_config(encoder_config.clone());
            encoder.encode_buffer(&buffer, &video_path).map_err(|e| {
                RoboflowError::encode("VideoEncoder", format!("Failed to encode video: {}", e))
            })?;

            tracing::debug!(
                camera = %camera,
                frames = buffer.len(),
                path = %video_path.display(),
                "Encoded chunk video"
            );

            if self.use_cloud_storage {
                video_files.push((video_path.clone(), camera.clone()));
            }
        }

        Ok(video_files)
    }

    /// Finalize the episode, flushing any remaining data.
    pub fn finalize(mut self) -> Result<ChunkStats> {
        if !self.frame_buffer.is_empty() || !self.image_buffers.is_empty() {
            self.flush_chunk()?;
        }

        tracing::info!(
            chunks = self.stats.chunks_written,
            total_frames = self.stats.total_frames,
            video_mb = self.stats.total_video_bytes / (1024 * 1024),
            parquet_mb = self.stats.total_parquet_bytes / (1024 * 1024),
            "Episode finalized with incremental flushing"
        );

        Ok(self.stats)
    }

    /// Get current statistics.
    pub fn stats(&self) -> &ChunkStats {
        &self.stats
    }

    /// Get metadata for all written chunks.
    pub fn chunk_metadata(&self) -> &[ChunkMetadata] {
        &self.chunk_metadata
    }

    /// Check if there's any pending data to flush.
    pub fn has_pending_data(&self) -> bool {
        !self.frame_buffer.is_empty() || !self.image_buffers.is_empty()
    }
}

/// Streaming video encoder that accepts frames incrementally.
///
/// This wraps FFmpeg in a way that allows frames to be added over time
/// rather than all at once. This is useful for long recordings.
#[allow(dead_code)]
pub struct StreamingVideoEncoder {
    /// FFmpeg process handle
    ffmpeg_process: Option<std::process::Child>,

    /// Path to output video
    output_path: PathBuf,

    /// Width of video (must be consistent)
    width: u32,

    /// Height of video (must be consistent)
    height: u32,

    /// Number of frames written
    frames_written: usize,

    /// Configuration
    config: VideoEncoderConfig,

    /// Whether we've seen any frames yet
    initialized: bool,
}

#[allow(dead_code)]
impl StreamingVideoEncoder {
    /// Create a new streaming encoder.
    pub fn new(output_path: PathBuf, config: VideoEncoderConfig) -> Self {
        Self {
            ffmpeg_process: None,
            output_path,
            width: 0,
            height: 0,
            frames_written: 0,
            config,
            initialized: false,
        }
    }

    /// Add a frame to the video.
    pub fn add_frame(&mut self, frame: VideoFrame) -> Result<()> {
        if !self.initialized {
            self.initialize(&frame)?;
        } else if frame.width != self.width || frame.height != self.height {
            return Err(RoboflowError::encode(
                "StreamingVideoEncoder",
                format!(
                    "Frame dimension mismatch: expected {}x{}, got {}x{}",
                    self.width, self.height, frame.width, frame.height
                ),
            ));
        }

        // Write frame to ffmpeg stdin
        if let Some(ref mut child) = self.ffmpeg_process
            && let Some(ref mut stdin) = child.stdin
        {
            Self::write_frame_to_stdin(stdin, &frame)?;
        }

        self.frames_written += 1;
        Ok(())
    }

    /// Initialize the FFmpeg process with the first frame's dimensions.
    fn initialize(&mut self, first_frame: &VideoFrame) -> Result<()> {
        self.width = first_frame.width;
        self.height = first_frame.height;

        let ffmpeg_path = "ffmpeg";

        let child = Command::new(ffmpeg_path)
            .arg("-y")
            .arg("-f")
            .arg("image2pipe")
            .arg("-vcodec")
            .arg("ppm")
            .arg("-r")
            .arg(self.config.fps.to_string())
            .arg("-i")
            .arg("-")
            .arg("-vf")
            .arg("pad=ceil(iw/2)*2:ceil(ih/2)*2")
            .arg("-c:v")
            .arg(&self.config.codec)
            .arg("-pix_fmt")
            .arg(&self.config.pixel_format)
            .arg("-preset")
            .arg(&self.config.preset)
            .arg("-crf")
            .arg(self.config.crf.to_string())
            .arg("-movflags")
            .arg("+faststart")
            .arg(&self.output_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|_| RoboflowError::unsupported("ffmpeg not found"))?;

        self.ffmpeg_process = Some(child);
        self.initialized = true;

        // Write first frame
        if let Some(ref mut process) = self.ffmpeg_process
            && let Some(ref mut stdin) = process.stdin
        {
            Self::write_frame_to_stdin(stdin, first_frame)?;
        }

        self.frames_written = 1;
        Ok(())
    }

    /// Write a frame in PPM format to a writer.
    fn write_frame_to_stdin(writer: &mut impl Write, frame: &VideoFrame) -> Result<()> {
        writeln!(writer, "P6")?;
        writeln!(writer, "{} {}", frame.width, frame.height)?;
        writeln!(writer, "255")?;
        writer.write_all(&frame.data)?;
        Ok(())
    }

    /// Finalize the video, closing the FFmpeg process.
    pub fn finalize(mut self) -> Result<usize> {
        if let Some(mut child) = self.ffmpeg_process.take() {
            // Close stdin to signal EOF
            drop(child.stdin.take());

            let status = child.wait()?;
            if !status.success() {
                return Err(RoboflowError::encode(
                    "StreamingVideoEncoder",
                    format!("FFmpeg failed with status {:?}", status),
                ));
            }
        }

        Ok(self.frames_written)
    }

    /// Get the number of frames written so far.
    pub fn frames_written(&self) -> usize {
        self.frames_written
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flushing_config_defaults() {
        let config = FlushingConfig::default();
        assert_eq!(config.max_frames_per_chunk, 1000);
        assert_eq!(config.max_memory_bytes, 2 * 1024 * 1024 * 1024);
        assert!(config.incremental_video_encoding);
    }

    #[test]
    fn test_flushing_config_unlimited() {
        let config = FlushingConfig::unlimited();
        assert_eq!(config.max_frames_per_chunk, 0);
        assert_eq!(config.max_memory_bytes, 0);
        assert!(!config.incremental_video_encoding);
        assert!(!config.is_limited());
    }

    #[test]
    fn test_flushing_triggers() {
        let config = FlushingConfig::with_max_frames(100);

        // Should not flush yet
        assert!(!config.should_flush(50, 0));
        assert!(!config.should_flush(99, 0));

        // Should flush at limit
        assert!(config.should_flush(100, 0));
        assert!(config.should_flush(101, 0));
    }

    #[test]
    fn test_memory_based_flushing() {
        let config = FlushingConfig::with_max_memory(1024);

        assert!(!config.should_flush(0, 500));
        assert!(!config.should_flush(0, 1023));
        assert!(config.should_flush(0, 1024));
        assert!(config.should_flush(0, 2048));
    }

    #[test]
    fn test_chunk_metadata() {
        let metadata = ChunkMetadata {
            index: 0,
            start_frame: 0,
            end_frame: 1000,
            frame_count: 1000,
            parquet_path: PathBuf::from("/test/episode_000000.parquet"),
            video_files: vec![],
            memory_bytes: 512 * 1024 * 1024,
        };

        assert_eq!(metadata.index, 0);
        assert_eq!(metadata.frame_count, 1000);
    }
}
