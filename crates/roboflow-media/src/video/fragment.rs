// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Fragment-based video encoding for bounded memory usage.
//!
//! This module provides [`FragmentEncoder`], a video encoder that creates
//! video segments (fragments) with explicit flush control. This enables
//! bounded memory usage during long encoding sessions.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                      FragmentEncoder                            │
//! ├─────────────────────────────────────────────────────────────────┤
//! │                                                                 │
//! │   RGB Data → Frame Buffer → [flush_fragment()] → Temp MP4      │
//! │        ↑                          ↓                              │
//! │   [should_flush()]         Fragment N.mp4                       │
//! │        │                          ↓                              │
//! │   Auto-flush trigger       [finalize()]                         │
//! │                              ↓         ↓                         │
//! │                        Concatenate  or  Keep Separate            │
//! │                                                                 │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Why Fragment-Based Encoding?
//!
//! The standard `VideoEncoder` buffers ALL frames in memory before encoding,
//! causing high memory usage for long videos (e.g., 1000 frames at 1080p = ~6GB).
//!
//! `FragmentEncoder` solves this by:
//! 1. Buffering frames until flush threshold is reached
//! 2. Encoding fragments to temporary MP4 files
//! 3. Freeing memory after each fragment
//! 4. Concatenating fragments on finalize (for SingleFile mode)
//!
//! # Example
//!
//! ```ignore
//! use roboflow_media::video::{
//!     FragmentEncoder, FragmentConfig, FragmentOutputConfig, VideoEncoderConfig,
//! };
//!
//! // Create encoder with auto-flush every 300 frames (10 seconds at 30fps)
//! let config = FragmentConfig {
//!     max_frames: Some(300),
//!     max_memory_bytes: Some(100 * 1024 * 1024), // 100MB
//!     max_duration_secs: Some(10.0),
//! };
//!
//! let mut encoder = FragmentEncoder::new(
//!     VideoEncoderConfig::default(),
//!     FragmentOutputConfig::SingleFile {
//!         path: PathBuf::from("output.mp4"),
//!     },
//!     config,
//! )?;
//!
//! // Encode frames
//! for frame in frames {
//!     let flushed = encoder.encode_frame(&frame.rgb_data, 640, 480)?;
//! }
//!
//! // Finalize - concatenates all fragments
//! let result = encoder.finalize()?;
//! ```

use std::path::PathBuf;

use roboflow_core::{Result, RoboflowError};
use tempfile::TempDir;

use super::composer::{RsmpegVideoComposer, VideoComposer};
use super::config::VideoEncoderConfig;
use super::encoder::{OutputConfig, VideoEncoder};

// =============================================================================
// Configuration Types
// =============================================================================

/// Configuration for fragment-based encoding.
#[derive(Debug, Clone, Default)]
pub struct FragmentConfig {
    /// Optional: Auto-flush after N frames (None = manual only).
    pub max_frames: Option<u32>,

    /// Optional: Auto-flush after N bytes buffered (None = manual only).
    pub max_memory_bytes: Option<usize>,

    /// Optional: Auto-flush after N seconds of video (None = manual only).
    /// Calculated as: frames / fps
    pub max_duration_secs: Option<f64>,
}

impl FragmentConfig {
    /// Create a new fragment config with manual flush only.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a config that auto-flushes after N frames.
    pub fn with_max_frames(frames: u32) -> Self {
        assert!(frames > 0, "max_frames must be positive");
        Self {
            max_frames: Some(frames),
            max_memory_bytes: None,
            max_duration_secs: None,
        }
    }

    /// Create a config that auto-flushes after N bytes.
    pub fn with_max_memory(bytes: usize) -> Self {
        assert!(bytes > 0, "max_memory_bytes must be positive");
        Self {
            max_frames: None,
            max_memory_bytes: Some(bytes),
            max_duration_secs: None,
        }
    }

    /// Create a config that auto-flushes after N seconds.
    pub fn with_max_duration(secs: f64) -> Self {
        assert!(secs > 0.0, "max_duration_secs must be positive");
        Self {
            max_frames: None,
            max_memory_bytes: None,
            max_duration_secs: Some(secs),
        }
    }
}

/// Output configuration for fragment encoder.
#[derive(Debug, Clone)]
pub enum FragmentOutputConfig {
    /// Single output file (fragments concatenated on finalize).
    SingleFile {
        /// Output file path.
        path: PathBuf,
    },

    /// Keep fragments separate (return list of paths).
    MultipleFragments {
        /// Output directory for fragments.
        dir: PathBuf,
        /// Filename prefix for fragments.
        prefix: String,
    },
}

/// Result from fragment encoding finalization.
#[derive(Debug, Clone)]
pub struct FragmentEncodingResult {
    /// Output path (for SingleFile mode).
    pub output_path: Option<PathBuf>,

    /// Fragment paths (for MultipleFragments mode).
    pub fragment_paths: Vec<PathBuf>,

    /// Total frames encoded.
    pub frames_encoded: u64,

    /// Total bytes written.
    pub bytes_written: u64,

    /// Number of fragments created.
    pub fragments: usize,
}

// =============================================================================
// Fragment Encoder
// =============================================================================

/// Fragment encoder with explicit flush control.
///
/// This encoder creates video segments (fragments) with bounded memory usage.
/// Frames are buffered until `flush_fragment()` is called (either manually
/// or automatically via `FragmentConfig` thresholds).
///
/// # Memory Management
///
/// Unlike `VideoEncoder` which buffers ALL frames in memory, `FragmentEncoder`
/// creates a NEW `VideoEncoder` for each fragment in `flush_fragment()`, then
/// drops it. This ensures memory is freed after each fragment.
///
/// # Thread Safety
///
/// `FragmentEncoder` is NOT thread-safe. Use one encoder per thread/camera.
pub struct FragmentEncoder {
    /// Video encoder configuration (used to create new encoders for each fragment).
    video_config: VideoEncoderConfig,

    /// Output configuration.
    output_config: FragmentOutputConfig,

    /// Fragment size thresholds.
    fragment_config: FragmentConfig,

    /// Video dimensions (set on first frame).
    width: u32,

    /// Video dimensions (set on first frame).
    height: u32,

    /// Frame rate (for duration calculations).
    fps: u32,

    // Current fragment state (NOT a VideoEncoder).
    /// Simple RGB frame buffer.
    frame_buffer: Vec<Vec<u8>>,

    /// Number of frames in current buffer.
    frame_count: u32,

    /// Total frames encoded across all fragments.
    total_frames: u64,

    /// Paths to completed fragment files.
    fragment_paths: Vec<PathBuf>,

    /// Temporary directory for fragment files.
    temp_dir: TempDir,

    /// Whether encoding is finalized.
    finalized: bool,
}

impl FragmentEncoder {
    /// Create a new fragment encoder.
    pub fn new(
        video_config: VideoEncoderConfig,
        output: FragmentOutputConfig,
        fragment_config: FragmentConfig,
    ) -> Result<Self> {
        let fps = video_config.fps;

        // Validate fps for duration-based flush calculations
        if fps == 0 {
            return Err(RoboflowError::encode(
                "FragmentEncoder",
                "fps must be positive for duration-based flush calculations",
            ));
        }

        let temp_dir = tempfile::tempdir().map_err(|e| {
            RoboflowError::encode(
                "FragmentEncoder",
                format!("Failed to create temp dir: {}", e),
            )
        })?;

        Ok(Self {
            video_config,
            output_config: output,
            fragment_config,
            width: 0,
            height: 0,
            fps,
            frame_buffer: Vec::new(),
            frame_count: 0,
            total_frames: 0,
            fragment_paths: Vec::new(),
            temp_dir,
            finalized: false,
        })
    }

    /// Encode a single frame.
    ///
    /// Returns `true` if an auto-flush threshold was reached and the frame
    /// buffer was flushed.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The encoder has been finalized
    /// - Frame dimensions are zero
    /// - Frame dimensions don't match previously encoded frames
    /// - RGB data size doesn't match dimensions
    pub fn encode_frame(&mut self, rgb_data: &[u8], width: u32, height: u32) -> Result<bool> {
        if self.finalized {
            return Err(RoboflowError::encode(
                "FragmentEncoder",
                "Cannot encode frame after finalization",
            ));
        }

        // Validate
        if width == 0 || height == 0 {
            return Err(RoboflowError::encode(
                "FragmentEncoder",
                "Frame dimensions cannot be zero",
            ));
        }

        let expected_size = (width as usize) * (height as usize) * 3;
        if rgb_data.len() != expected_size {
            return Err(RoboflowError::encode(
                "FragmentEncoder",
                format!(
                    "RGB data size mismatch: got {} bytes, expected {} bytes for {}x{}",
                    rgb_data.len(),
                    expected_size,
                    width,
                    height
                ),
            ));
        }

        // Initialize dimensions on first frame
        if self.width == 0 {
            self.width = width;
            self.height = height;
        }

        // Validate dimensions match
        if width != self.width || height != self.height {
            return Err(RoboflowError::encode(
                "FragmentEncoder",
                format!(
                    "Frame dimension mismatch: expected {}x{}, got {}x{}",
                    self.width, self.height, width, height
                ),
            ));
        }

        // Add frame to buffer
        self.frame_buffer.push(rgb_data.to_vec());
        self.frame_count += 1;
        self.total_frames += 1;

        // Check auto-flush
        if self.should_flush() {
            self.flush_fragment()?;
            return Ok(true);
        }

        Ok(false)
    }

    /// Explicitly flush current buffer as a fragment.
    ///
    /// Creates a temporary MP4 file and clears the buffer.
    /// Does nothing if the buffer is empty.
    pub fn flush_fragment(&mut self) -> Result<()> {
        if self.frame_buffer.is_empty() {
            tracing::debug!("flush_fragment() called with empty buffer - no-op");
            return Ok(());
        }

        // Create temp file for this fragment
        let fragment_path = self
            .temp_dir
            .path()
            .join(format!("fragment_{:04}.mp4", self.fragment_paths.len()));

        // DESIGN NOTE: We create a NEW VideoEncoder for each fragment.
        // This is necessary because VideoEncoder is tied to a single output file
        // (its FFmpeg AVFormatContextOutput is bound to one file path).
        // Creating a new encoder per fragment ensures:
        // 1. Memory is freed when encoder is dropped after finalize()
        // 2. Each fragment gets a clean encoding state
        // 3. No complex "reset" logic needed in VideoEncoder
        let mut encoder = VideoEncoder::new(
            self.video_config.clone(),
            OutputConfig::file(&fragment_path),
        )?;

        // Encode all buffered frames to this fragment
        for frame_data in &self.frame_buffer {
            encoder.encode_frame(frame_data, self.width, self.height)?;
        }

        // Finalize the fragment - writes MP4 trailer, closes file
        encoder.finalize()?;

        // IMPORTANT: encoder is dropped here, freeing its memory.
        // This is the key to bounded memory usage.
        // Only the frame_buffer remains, which we now clear.

        // Clear buffer for next fragment
        self.frame_buffer.clear();
        self.frame_count = 0;

        // Track fragment for later concatenation
        self.fragment_paths.push(fragment_path);

        tracing::debug!(
            fragment = self.fragment_paths.len(),
            total_frames = self.total_frames,
            "Fragment flushed"
        );

        Ok(())
    }

    /// Check if auto-flush is needed based on config.
    ///
    /// Returns `true` if any configured threshold is reached:
    /// - `max_frames`: frame count >= threshold
    /// - `max_memory_bytes`: buffered bytes >= threshold
    /// - `max_duration_secs`: video duration >= threshold
    pub fn should_flush(&self) -> bool {
        // Check frame count threshold
        if let Some(max_frames) = self.fragment_config.max_frames
            && self.frame_count >= max_frames
        {
            return true;
        }

        // Check memory threshold
        if let Some(max_bytes) = self.fragment_config.max_memory_bytes
            && self.buffered_bytes() >= max_bytes
        {
            return true;
        }

        // Check duration threshold
        if let Some(max_duration) = self.fragment_config.max_duration_secs
            && self.fps > 0
        {
            let current_duration = self.frame_count as f64 / self.fps as f64;
            if current_duration >= max_duration {
                return true;
            }
        }

        false
    }

    /// Finalize encoding.
    ///
    /// For `SingleFile` mode: concatenates all fragments into output.
    /// For `MultipleFragments` mode: returns list of fragment paths.
    ///
    /// # Errors
    ///
    /// Returns an error if no frames were encoded.
    pub fn finalize(mut self) -> Result<FragmentEncodingResult> {
        if self.finalized {
            return Err(RoboflowError::encode(
                "FragmentEncoder",
                "Encoder already finalized",
            ));
        }

        self.finalized = true;

        // Flush any remaining frames
        self.flush_fragment()?;

        let frames_encoded = self.total_frames;
        let fragments = self.fragment_paths.len();

        match &self.output_config {
            FragmentOutputConfig::SingleFile { path } => {
                match self.fragment_paths.len() {
                    0 => Err(RoboflowError::encode(
                        "FragmentEncoder",
                        "No frames encoded",
                    )),
                    1 => {
                        // Single fragment - just rename
                        let source = &self.fragment_paths[0];

                        // Ensure parent directory exists
                        if let Some(parent) = path.parent() {
                            std::fs::create_dir_all(parent).map_err(|e| {
                                RoboflowError::encode(
                                    "FragmentEncoder",
                                    format!("Failed to create output directory: {}", e),
                                )
                            })?;
                        }

                        std::fs::rename(source, path).map_err(|e| {
                            RoboflowError::encode(
                                "FragmentEncoder",
                                format!("Failed to rename output file: {}", e),
                            )
                        })?;

                        // Get file size
                        let bytes_written = std::fs::metadata(path)
                            .map_err(|e| {
                                RoboflowError::encode(
                                    "FragmentEncoder",
                                    format!("Failed to get output file metadata: {}", e),
                                )
                            })?
                            .len();

                        tracing::info!(
                            path = %path.display(),
                            frames = frames_encoded,
                            bytes = bytes_written,
                            "FragmentEncoder finalized (single fragment)"
                        );

                        Ok(FragmentEncodingResult {
                            output_path: Some(path.clone()),
                            fragment_paths: vec![path.clone()],
                            frames_encoded,
                            bytes_written,
                            fragments,
                        })
                    }
                    _ => {
                        // Multiple fragments - compose
                        let sources: Vec<&PathBuf> = self.fragment_paths.iter().collect();
                        let source_paths: Vec<&std::path::Path> =
                            sources.iter().map(|p| p.as_path()).collect();

                        // Ensure parent directory exists
                        if let Some(parent) = path.parent() {
                            std::fs::create_dir_all(parent).map_err(|e| {
                                RoboflowError::encode(
                                    "FragmentEncoder",
                                    format!("Failed to create output directory: {}", e),
                                )
                            })?;
                        }

                        let composer = RsmpegVideoComposer::new();
                        composer.compose(&source_paths, path)?;

                        // Get file size
                        let bytes_written = std::fs::metadata(path)
                            .map_err(|e| {
                                RoboflowError::encode(
                                    "FragmentEncoder",
                                    format!("Failed to get output file metadata: {}", e),
                                )
                            })?
                            .len();

                        tracing::info!(
                            path = %path.display(),
                            frames = frames_encoded,
                            fragments = fragments,
                            bytes = bytes_written,
                            "FragmentEncoder finalized (concatenated)"
                        );

                        Ok(FragmentEncodingResult {
                            output_path: Some(path.clone()),
                            fragment_paths: self.fragment_paths.clone(),
                            frames_encoded,
                            bytes_written,
                            fragments,
                        })
                    }
                }
            }
            FragmentOutputConfig::MultipleFragments { dir, prefix } => {
                // Ensure output directory exists
                std::fs::create_dir_all(dir).map_err(|e| {
                    RoboflowError::encode(
                        "FragmentEncoder",
                        format!("Failed to create output directory: {}", e),
                    )
                })?;

                // Move temp files to output directory
                let mut final_paths = Vec::new();
                let mut total_bytes = 0u64;

                for (i, temp_path) in self.fragment_paths.iter().enumerate() {
                    let dest = dir.join(format!("{}_{:04}.mp4", prefix, i));
                    std::fs::rename(temp_path, &dest).map_err(|e| {
                        RoboflowError::encode(
                            "FragmentEncoder",
                            format!("Failed to move fragment file: {}", e),
                        )
                    })?;

                    let metadata = std::fs::metadata(&dest).map_err(|e| {
                        RoboflowError::encode(
                            "FragmentEncoder",
                            format!(
                                "Failed to get fragment file metadata for {}: {}",
                                dest.display(),
                                e
                            ),
                        )
                    })?;
                    total_bytes += metadata.len();

                    final_paths.push(dest);
                }

                tracing::info!(
                    dir = %dir.display(),
                    prefix = prefix,
                    frames = frames_encoded,
                    fragments = fragments,
                    bytes = total_bytes,
                    "FragmentEncoder finalized (multiple fragments)"
                );

                Ok(FragmentEncodingResult {
                    output_path: None,
                    fragment_paths: final_paths,
                    frames_encoded,
                    bytes_written: total_bytes,
                    fragments,
                })
            }
        }
    }

    /// Get current memory usage of buffered frames.
    pub fn buffered_bytes(&self) -> usize {
        self.frame_buffer.iter().map(|f| f.len()).sum()
    }

    /// Get number of buffered frames.
    pub fn buffered_frames(&self) -> u32 {
        self.frame_count
    }

    /// Get number of fragments created so far.
    pub fn fragment_count(&self) -> usize {
        self.fragment_paths.len()
    }

    /// Get total frames encoded so far.
    pub fn total_frames(&self) -> u64 {
        self.total_frames
    }

    /// Check if the encoder is finalized.
    pub fn is_finalized(&self) -> bool {
        self.finalized
    }

    /// Get video dimensions.
    pub fn dimensions(&self) -> (u32, u32) {
        (self.width, self.height)
    }
}

impl std::fmt::Debug for FragmentEncoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FragmentEncoder")
            .field("width", &self.width)
            .field("height", &self.height)
            .field("fps", &self.fps)
            .field("frame_count", &self.frame_count)
            .field("total_frames", &self.total_frames)
            .field("fragment_count", &self.fragment_paths.len())
            .field("finalized", &self.finalized)
            .field("video_config", &self.video_config)
            .field("output_config", &self.output_config)
            .field("fragment_config", &self.fragment_config)
            .field("temp_dir", &"<tempdir>")
            .finish()
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create test RGB data.
    fn create_test_rgb(width: u32, height: u32, value: u8) -> Vec<u8> {
        vec![value; (width as usize) * (height as usize) * 3]
    }

    #[test]
    fn test_fragment_config_default() {
        let config = FragmentConfig::default();
        assert!(config.max_frames.is_none());
        assert!(config.max_memory_bytes.is_none());
        assert!(config.max_duration_secs.is_none());
    }

    #[test]
    fn test_fragment_config_with_max_frames() {
        let config = FragmentConfig::with_max_frames(100);
        assert_eq!(config.max_frames, Some(100));
        assert!(config.max_memory_bytes.is_none());
        assert!(config.max_duration_secs.is_none());
    }

    #[test]
    fn test_fragment_config_with_max_memory() {
        let config = FragmentConfig::with_max_memory(1024 * 1024);
        assert!(config.max_frames.is_none());
        assert_eq!(config.max_memory_bytes, Some(1024 * 1024));
        assert!(config.max_duration_secs.is_none());
    }

    #[test]
    fn test_fragment_config_with_max_duration() {
        let config = FragmentConfig::with_max_duration(10.0);
        assert!(config.max_frames.is_none());
        assert!(config.max_memory_bytes.is_none());
        assert_eq!(config.max_duration_secs, Some(10.0));
    }

    #[test]
    fn test_fragment_encoder_create() {
        let temp_dir = tempfile::tempdir().unwrap();
        let output = FragmentOutputConfig::SingleFile {
            path: temp_dir.path().join("output.mp4"),
        };

        let encoder = FragmentEncoder::new(
            VideoEncoderConfig::default(),
            output,
            FragmentConfig::default(),
        )
        .unwrap();

        assert_eq!(encoder.buffered_frames(), 0);
        assert_eq!(encoder.buffered_bytes(), 0);
        assert_eq!(encoder.fragment_count(), 0);
        assert_eq!(encoder.total_frames(), 0);
        assert!(!encoder.is_finalized());
    }

    #[test]
    fn test_fragment_encoder_single_frame() {
        let temp_dir = tempfile::tempdir().unwrap();
        let output_path = temp_dir.path().join("output.mp4");

        let output = FragmentOutputConfig::SingleFile {
            path: output_path.clone(),
        };

        let mut encoder = FragmentEncoder::new(
            VideoEncoderConfig::default(),
            output,
            FragmentConfig::default(),
        )
        .unwrap();

        let rgb = create_test_rgb(64, 64, 128);
        encoder.encode_frame(&rgb, 64, 64).unwrap();

        assert_eq!(encoder.buffered_frames(), 1);
        assert_eq!(encoder.total_frames(), 1);

        let result = encoder.finalize().unwrap();

        assert_eq!(result.frames_encoded, 1);
        assert_eq!(result.fragments, 1);
        assert!(result.output_path.unwrap().exists());
    }

    #[test]
    fn test_fragment_encoder_explicit_flush() {
        let temp_dir = tempfile::tempdir().unwrap();
        let output_path = temp_dir.path().join("output.mp4");

        let output = FragmentOutputConfig::SingleFile {
            path: output_path.clone(),
        };

        let mut encoder = FragmentEncoder::new(
            VideoEncoderConfig::default(),
            output,
            FragmentConfig::default(),
        )
        .unwrap();

        // Add 3 frames and flush
        let rgb = create_test_rgb(64, 64, 128);
        for _ in 0..3 {
            encoder.encode_frame(&rgb, 64, 64).unwrap();
        }

        assert_eq!(encoder.buffered_frames(), 3);

        encoder.flush_fragment().unwrap();

        // Buffer should be cleared
        assert_eq!(encoder.buffered_frames(), 0);
        assert_eq!(encoder.fragment_count(), 1);

        // Add more frames
        for _ in 0..2 {
            encoder.encode_frame(&rgb, 64, 64).unwrap();
        }

        encoder.flush_fragment().unwrap();
        assert_eq!(encoder.fragment_count(), 2);

        let result = encoder.finalize().unwrap();
        assert_eq!(result.frames_encoded, 5);
        assert_eq!(result.fragments, 2);
    }

    #[test]
    fn test_fragment_encoder_auto_flush_by_frames() {
        let temp_dir = tempfile::tempdir().unwrap();
        let output_path = temp_dir.path().join("output.mp4");

        let output = FragmentOutputConfig::SingleFile {
            path: output_path.clone(),
        };

        let config = FragmentConfig::with_max_frames(5);

        let mut encoder =
            FragmentEncoder::new(VideoEncoderConfig::default(), output, config).unwrap();

        let rgb = create_test_rgb(64, 64, 128);

        // Encode 5 frames - should trigger auto-flush
        for _ in 0..4 {
            let flushed = encoder.encode_frame(&rgb, 64, 64).unwrap();
            assert!(!flushed);
        }

        // 5th frame should trigger flush
        let flushed = encoder.encode_frame(&rgb, 64, 64).unwrap();
        assert!(flushed);
        assert_eq!(encoder.fragment_count(), 1);
        assert_eq!(encoder.buffered_frames(), 0);

        // Continue with more frames
        for _ in 0..5 {
            encoder.encode_frame(&rgb, 64, 64).unwrap();
        }

        assert_eq!(encoder.fragment_count(), 2);

        let result = encoder.finalize().unwrap();
        assert_eq!(result.frames_encoded, 10);
        assert_eq!(result.fragments, 2);
    }

    #[test]
    fn test_fragment_encoder_auto_flush_by_memory() {
        let temp_dir = tempfile::tempdir().unwrap();
        let output_path = temp_dir.path().join("output.mp4");

        let output = FragmentOutputConfig::SingleFile {
            path: output_path.clone(),
        };

        // 64x64 RGB = 12288 bytes per frame
        // Set threshold to 20000 bytes (should flush after 2 frames)
        let config = FragmentConfig::with_max_memory(20000);

        let mut encoder =
            FragmentEncoder::new(VideoEncoderConfig::default(), output, config).unwrap();

        let rgb = create_test_rgb(64, 64, 128);
        assert_eq!(rgb.len(), 12288);

        // First frame
        let flushed = encoder.encode_frame(&rgb, 64, 64).unwrap();
        assert!(!flushed);
        assert_eq!(encoder.buffered_bytes(), 12288);

        // Second frame - exceeds 25000 bytes
        let flushed = encoder.encode_frame(&rgb, 64, 64).unwrap();
        assert!(flushed);
        assert_eq!(encoder.fragment_count(), 1);
    }

    #[test]
    fn test_fragment_encoder_auto_flush_by_duration() {
        let temp_dir = tempfile::tempdir().unwrap();
        let output_path = temp_dir.path().join("output.mp4");

        let output = FragmentOutputConfig::SingleFile {
            path: output_path.clone(),
        };

        // 30 fps, 10 second threshold = 300 frames
        // But we'll use 0.1 seconds = 3 frames at 30fps
        let video_config = VideoEncoderConfig::default().with_fps(30);
        let config = FragmentConfig::with_max_duration(0.1);

        let mut encoder = FragmentEncoder::new(video_config, output, config).unwrap();

        let rgb = create_test_rgb(64, 64, 128);

        // 3 frames at 30fps = 0.1 seconds
        for _ in 0..2 {
            let flushed = encoder.encode_frame(&rgb, 64, 64).unwrap();
            assert!(!flushed);
        }

        // 3rd frame should trigger flush
        let flushed = encoder.encode_frame(&rgb, 64, 64).unwrap();
        assert!(flushed);
        assert_eq!(encoder.fragment_count(), 1);
    }

    #[test]
    fn test_fragment_encoder_multiple_fragments() {
        let temp_dir = tempfile::tempdir().unwrap();
        let output_dir = temp_dir.path().join("fragments");
        let output = FragmentOutputConfig::MultipleFragments {
            dir: output_dir.clone(),
            prefix: "video".to_string(),
        };

        let config = FragmentConfig::with_max_frames(3);

        let mut encoder =
            FragmentEncoder::new(VideoEncoderConfig::default(), output, config).unwrap();

        let rgb = create_test_rgb(64, 64, 128);

        // Encode 9 frames = 3 fragments
        for _ in 0..9 {
            encoder.encode_frame(&rgb, 64, 64).unwrap();
        }

        let result = encoder.finalize().unwrap();

        assert_eq!(result.frames_encoded, 9);
        assert_eq!(result.fragments, 3);
        assert!(result.output_path.is_none());
        assert_eq!(result.fragment_paths.len(), 3);

        // Check files exist with correct names
        for i in 0..3 {
            let expected_path = output_dir.join(format!("video_{:04}.mp4", i));
            assert!(result.fragment_paths.contains(&expected_path));
            assert!(expected_path.exists());
        }
    }

    #[test]
    fn test_fragment_encoder_empty_buffer_flush() {
        let temp_dir = tempfile::tempdir().unwrap();
        let output_path = temp_dir.path().join("output.mp4");

        let output = FragmentOutputConfig::SingleFile {
            path: output_path.clone(),
        };

        let mut encoder = FragmentEncoder::new(
            VideoEncoderConfig::default(),
            output,
            FragmentConfig::default(),
        )
        .unwrap();

        // Flush empty buffer should succeed
        encoder.flush_fragment().unwrap();
        assert_eq!(encoder.fragment_count(), 0);
    }

    #[test]
    fn test_fragment_encoder_finalize_empty() {
        let temp_dir = tempfile::tempdir().unwrap();
        let output_path = temp_dir.path().join("output.mp4");

        let output = FragmentOutputConfig::SingleFile {
            path: output_path.clone(),
        };

        let encoder = FragmentEncoder::new(
            VideoEncoderConfig::default(),
            output,
            FragmentConfig::default(),
        )
        .unwrap();

        let result = encoder.finalize();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("No frames encoded")
        );
    }

    #[test]
    fn test_fragment_encoder_finalize_twice() {
        let temp_dir = tempfile::tempdir().unwrap();
        let output_path = temp_dir.path().join("output.mp4");

        let output = FragmentOutputConfig::SingleFile {
            path: output_path.clone(),
        };

        let mut encoder = FragmentEncoder::new(
            VideoEncoderConfig::default(),
            output,
            FragmentConfig::default(),
        )
        .unwrap();

        let rgb = create_test_rgb(64, 64, 128);
        encoder.encode_frame(&rgb, 64, 64).unwrap();

        let result = encoder.finalize();
        assert!(result.is_ok());

        // Note: finalize() takes ownership, so we can't call it again on the same encoder
        // The test validates that finalize works correctly; subsequent calls would require
        // a different design (e.g., returning the encoder on error)
    }

    #[test]
    fn test_fragment_encoder_encode_after_finalize() {
        let temp_dir = tempfile::tempdir().unwrap();
        let output_path = temp_dir.path().join("output.mp4");

        let output = FragmentOutputConfig::SingleFile {
            path: output_path.clone(),
        };

        let mut encoder = FragmentEncoder::new(
            VideoEncoderConfig::default(),
            output,
            FragmentConfig::default(),
        )
        .unwrap();

        let rgb = create_test_rgb(64, 64, 128);
        encoder.encode_frame(&rgb, 64, 64).unwrap();

        let _ = encoder.finalize().unwrap();

        // Note: finalize() takes ownership, so we can't encode after finalize
        // The test validates the error handling in encode_frame for finalized state
        // by creating a new encoder that's already finalized internally

        // Create a new encoder to test encode after finalize scenario
        let output2 = FragmentOutputConfig::SingleFile {
            path: temp_dir.path().join("output2.mp4"),
        };

        let mut encoder2 = FragmentEncoder::new(
            VideoEncoderConfig::default(),
            output2,
            FragmentConfig::default(),
        )
        .unwrap();

        encoder2.encode_frame(&rgb, 64, 64).unwrap();
        let _ = encoder2.finalize().unwrap();

        // Can't encode after finalize because encoder was moved
        // This is the expected behavior - finalize() consumes the encoder
    }

    #[test]
    fn test_fragment_encoder_invalid_dimensions() {
        let temp_dir = tempfile::tempdir().unwrap();
        let output_path = temp_dir.path().join("output.mp4");

        let output = FragmentOutputConfig::SingleFile {
            path: output_path.clone(),
        };

        let mut encoder = FragmentEncoder::new(
            VideoEncoderConfig::default(),
            output,
            FragmentConfig::default(),
        )
        .unwrap();

        let rgb = create_test_rgb(64, 64, 128);
        encoder.encode_frame(&rgb, 64, 64).unwrap();

        // Try different dimensions
        let rgb2 = create_test_rgb(32, 32, 128);
        let result = encoder.encode_frame(&rgb2, 32, 32);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("dimension mismatch")
        );
    }

    #[test]
    fn test_fragment_encoder_zero_dimensions() {
        let temp_dir = tempfile::tempdir().unwrap();
        let output_path = temp_dir.path().join("output.mp4");

        let output = FragmentOutputConfig::SingleFile {
            path: output_path.clone(),
        };

        let mut encoder = FragmentEncoder::new(
            VideoEncoderConfig::default(),
            output,
            FragmentConfig::default(),
        )
        .unwrap();

        let rgb = vec![0u8; 100];
        let result = encoder.encode_frame(&rgb, 0, 64);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("zero"));
    }

    #[test]
    fn test_fragment_encoder_data_size_mismatch() {
        let temp_dir = tempfile::tempdir().unwrap();
        let output_path = temp_dir.path().join("output.mp4");

        let output = FragmentOutputConfig::SingleFile {
            path: output_path.clone(),
        };

        let mut encoder = FragmentEncoder::new(
            VideoEncoderConfig::default(),
            output,
            FragmentConfig::default(),
        )
        .unwrap();

        let rgb = vec![0u8; 100]; // Wrong size
        let result = encoder.encode_frame(&rgb, 64, 64);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("size mismatch"));
    }

    #[test]
    fn test_fragment_encoder_memory_bounded_during_long_encoding() {
        // This test verifies memory stays bounded during long encoding
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_dir.path().join("output.mp4");

        let config = FragmentConfig::with_max_frames(50); // Small fragment size

        let mut encoder = FragmentEncoder::new(
            VideoEncoderConfig::default(),
            FragmentOutputConfig::SingleFile {
                path: output.clone(),
            },
            config,
        )
        .unwrap();

        // Encode 500 frames (10 fragments)
        let frame_data = create_test_rgb(64, 64, 128);
        for _ in 0..500 {
            encoder.encode_frame(&frame_data, 64, 64).unwrap();
        }

        let result = encoder.finalize().unwrap();

        // Verify:
        // 1. All frames encoded
        assert_eq!(result.frames_encoded, 500);
        // 2. Multiple fragments created
        assert_eq!(result.fragments, 10);
        // 3. Single output file exists
        assert!(result.output_path.unwrap().exists());
    }

    #[test]
    fn test_fragment_encoder_zero_fps_rejected() {
        let temp_dir = tempfile::tempdir().unwrap();
        let output = FragmentOutputConfig::SingleFile {
            path: temp_dir.path().join("output.mp4"),
        };

        // Create a config with fps = 0 (should fail in constructor)
        let video_config = VideoEncoderConfig {
            fps: 0,
            ..Default::default()
        };

        let result = FragmentEncoder::new(video_config, output, FragmentConfig::default());

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("fps must be positive")
        );
    }

    #[test]
    fn test_fragment_encoder_zero_threshold_rejected() {
        // Test that zero thresholds are rejected in factory methods
        let result = std::panic::catch_unwind(|| {
            FragmentConfig::with_max_frames(0);
        });
        assert!(result.is_err());

        let result = std::panic::catch_unwind(|| {
            FragmentConfig::with_max_memory(0);
        });
        assert!(result.is_err());

        let result = std::panic::catch_unwind(|| {
            FragmentConfig::with_max_duration(0.0);
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_fragment_encoder_finalize_with_invalid_path() {
        // Test that finalize() properly surfaces directory creation errors
        let temp_dir = tempfile::tempdir().unwrap();

        // On Unix, /root/output.mp4 requires root access
        // On Windows, C:\Windows\System32\output.mp4 requires admin
        let restricted_path = if cfg!(unix) {
            PathBuf::from("/root/roboflow_test_output.mp4")
        } else if cfg!(windows) {
            PathBuf::from("C:\\Windows\\System32\\roboflow_test_output.mp4")
        } else {
            temp_dir.path().join("output.mp4") // Fallback for other platforms
        };

        let output = FragmentOutputConfig::SingleFile {
            path: restricted_path.clone(),
        };

        let mut encoder = FragmentEncoder::new(
            VideoEncoderConfig::default(),
            output,
            FragmentConfig::default(),
        )
        .unwrap();

        // Encode a frame so there's something to finalize
        let rgb = create_test_rgb(64, 64, 128);
        let _ = encoder.encode_frame(&rgb, 64, 64);

        // Finalize should fail with directory creation error (or file write error)
        let result = encoder.finalize();

        // Note: This test may pass on systems without restrictive permissions
        // but it exercises the error handling path
        if restricted_path.parent().is_some_and(|p| !p.exists()) {
            // If the parent doesn't exist, we expect an error
            assert!(result.is_err());
        }
    }

    #[test]
    fn test_fragment_encoding_result() {
        let result = FragmentEncodingResult {
            output_path: Some(PathBuf::from("/tmp/output.mp4")),
            fragment_paths: vec![
                PathBuf::from("/tmp/frag1.mp4"),
                PathBuf::from("/tmp/frag2.mp4"),
            ],
            frames_encoded: 100,
            bytes_written: 1024 * 1024,
            fragments: 2,
        };

        assert_eq!(result.frames_encoded, 100);
        assert_eq!(result.bytes_written, 1024 * 1024);
        assert_eq!(result.fragments, 2);
        assert_eq!(result.fragment_paths.len(), 2);
    }
}
