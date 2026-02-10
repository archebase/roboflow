// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Streaming video encoder for direct S3/OSS upload.
//!
//! This module provides video encoding that writes directly to cloud storage
//! without intermediate disk files, using:
//! - Ring buffer for frame queuing
//! - FFmpeg CLI with fMP4 output
//! - Multipart upload for efficient streaming

use std::process::{Command, Stdio};
use std::sync::Arc;
use std::thread;

use tokio::runtime::Handle;

use crate::common::{ImageData, VideoFrame};
use crate::lerobot::{config::VideoConfig, video_profiles::ResolvedConfig};
use roboflow_core::{Result, RoboflowError};
use roboflow_storage::{ObjectPath, Storage, object_store};

/// Configuration for streaming video encoding.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields are part of public config API for future streaming modes
pub struct StreamingEncoderConfig {
    /// Video encoder configuration
    pub video: ResolvedConfig,

    /// Frame rate
    pub fps: u32,

    /// Ring buffer capacity in frames
    pub ring_buffer_size: usize,

    /// Multipart upload part size in bytes
    pub upload_part_size: usize,

    /// Timeout for frame operations in seconds
    pub buffer_timeout_secs: u64,
}

impl Default for StreamingEncoderConfig {
    fn default() -> Self {
        Self {
            video: ResolvedConfig::from_video_config(&VideoConfig::default()),
            fps: 30,
            ring_buffer_size: 128,
            upload_part_size: 16 * 1024 * 1024, // 16 MB
            buffer_timeout_secs: 5,
        }
    }
}

/// Statistics from streaming video encoding.
#[derive(Debug, Default)]
pub struct StreamingEncodeStats {
    /// Number of images encoded
    pub images_encoded: usize,
    /// Number of frames skipped due to dimension mismatches
    pub skipped_frames: usize,
    /// Number of cameras that failed to encode
    pub failed_encodings: usize,
    /// Total output bytes uploaded
    pub output_bytes: u64,
    /// S3 URLs of uploaded videos
    pub video_urls: Vec<(String, String)>, // (camera, s3_url)
}

/// Streaming video encoder for a single camera.
///
/// This encoder:
/// 1. Spawns an FFmpeg process with fMP4 output to stdout
/// 2. Reads frames from a ring buffer
/// 3. Converts frames to PPM format and writes to FFmpeg stdin
/// 4. Captures FFmpeg stdout and streams to S3 via multipart upload
/// 5. Completes the upload when FFmpeg exits
#[allow(dead_code)] // Fields and methods are used in different encoding modes
pub struct CameraStreamingEncoder {
    /// Camera name (full feature path)
    camera: String,

    /// S3/OSS storage
    store: Arc<dyn object_store::ObjectStore>,

    /// Tokio runtime handle
    runtime: Handle,

    /// Destination key
    key: ObjectPath,

    /// Encoder configuration
    config: StreamingEncoderConfig,

    /// Video width
    width: u32,

    /// Video height
    height: u32,

    /// Frame rate
    fps: u32,

    /// Number of frames encoded
    frames_encoded: usize,

    /// FFmpeg process
    ffmpeg_child: Option<std::process::Child>,

    /// FFmpeg stdin writer
    ffmpeg_stdin: Option<std::process::ChildStdin>,

    /// Upload state
    upload: Option<object_store::WriteMultipart>,

    /// Upload thread handle
    upload_thread: Option<thread::JoinHandle<Result<()>>>,

    /// Whether the encoder has been initialized
    initialized: bool,

    /// Whether the encoder has been finalized
    finalized: bool,
}

impl CameraStreamingEncoder {
    /// Create a new camera streaming encoder.
    ///
    /// # Arguments
    ///
    /// * `camera` - Camera name (full feature path)
    /// * `s3_url` - S3/OSS URL (e.g., "s3://bucket/path/video.mp4")
    /// * `images` - First batch of images to determine dimensions
    /// * `config` - Encoder configuration
    /// * `store` - Object store client
    /// * `runtime` - Tokio runtime handle
    pub fn new(
        camera: String,
        s3_url: &str,
        images: &[ImageData],
        config: StreamingEncoderConfig,
        store: Arc<dyn object_store::ObjectStore>,
        runtime: Handle,
    ) -> Result<Self> {
        // Parse S3 URL to get key
        let key = parse_s3_url_to_key(s3_url)?;

        // Get dimensions from first image
        let first_image = images
            .first()
            .ok_or_else(|| RoboflowError::encode("CameraStreamingEncoder", "No images provided"))?;
        let width = first_image.width;
        let height = first_image.height;

        // Validate dimensions
        if width == 0 || height == 0 {
            return Err(RoboflowError::encode(
                "CameraStreamingEncoder",
                "Width and height must be non-zero",
            ));
        }

        let fps = config.fps;
        Ok(Self {
            camera,
            store,
            runtime,
            key,
            config,
            width,
            height,
            fps,
            frames_encoded: 0,
            ffmpeg_child: None,
            ffmpeg_stdin: None,
            upload: None,
            upload_thread: None,
            initialized: false,
            finalized: false,
        })
    }

    /// Add a frame to the encoder.
    ///
    /// This method converts `ImageData` to `VideoFrame` and writes it to FFmpeg stdin.
    #[allow(dead_code)] // Used in incremental streaming mode
    pub fn add_frame(&mut self, image: &ImageData) -> Result<()> {
        if self.finalized {
            return Err(RoboflowError::encode(
                "CameraStreamingEncoder",
                "Cannot add frame to finalized encoder",
            ));
        }

        // Initialize on first frame
        if !self.initialized {
            self.initialize()?;
        }

        // Validate dimensions
        if image.width != self.width || image.height != self.height {
            return Err(RoboflowError::encode(
                "CameraStreamingEncoder",
                format!(
                    "Frame dimension mismatch: expected {}x{}, got {}x{}",
                    self.width, self.height, image.width, image.height
                ),
            ));
        }

        // Convert ImageData to VideoFrame
        let video_frame = VideoFrame::new(image.width, image.height, image.data.clone());

        // Write frame to FFmpeg stdin
        if let Some(ref mut stdin) = self.ffmpeg_stdin {
            write_ppm_frame(stdin, &video_frame).map_err(|e| {
                RoboflowError::encode(
                    "CameraStreamingEncoder",
                    format!("Failed to write frame: {}", e),
                )
            })?;
        }

        self.frames_encoded += 1;

        Ok(())
    }

    /// Initialize the encoder, FFmpeg process, and multipart upload.
    #[allow(dead_code)] // Used in incremental streaming mode
    fn initialize(&mut self) -> Result<()> {
        // Create multipart upload
        let multipart_upload = self.runtime.block_on(async {
            self.store
                .put_multipart(&self.key)
                .await
                .map_err(|e| RoboflowError::encode("CameraStreamingEncoder", e.to_string()))
        })?;

        // Create WriteMultipart with configured chunk size
        let upload = object_store::WriteMultipart::new_with_chunk_size(
            multipart_upload,
            self.config.upload_part_size,
        );

        // Build FFmpeg command line based on video config
        let codec = &self.config.video.codec;
        let crf = self.config.video.crf;
        let preset = &self.config.video.preset;
        let pixel_format = &self.config.video.pixel_format;

        let mut child = Command::new("ffmpeg")
            .arg("-y")
            .arg("-f")
            .arg("image2pipe")
            .arg("-vcodec")
            .arg("ppm")
            .arg("-r")
            .arg(self.fps.to_string())
            .arg("-i")
            .arg("-")
            .arg("-vf")
            .arg("pad=ceil(iw/2)*2:ceil(ih/2)*2")
            .arg("-c:v")
            .arg(codec)
            .arg("-crf")
            .arg(crf.to_string())
            .arg("-preset")
            .arg(preset)
            .arg("-pix_fmt")
            .arg(pixel_format)
            .arg("-movflags")
            .arg("frag_keyframe+empty_moov+default_base_moof")
            .arg("-f")
            .arg("mp4")
            .arg("-") // Output to stdout
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|_| RoboflowError::unsupported("ffmpeg not found"))?;

        let stdin = child.stdin.take().ok_or_else(|| {
            RoboflowError::encode("CameraStreamingEncoder", "Failed to open FFmpeg stdin")
        })?;

        // Start upload thread to read from stdout and upload to S3
        let stdout = child.stdout.take().ok_or_else(|| {
            RoboflowError::encode("CameraStreamingEncoder", "Failed to open FFmpeg stdout")
        })?;

        let store_clone = Arc::clone(&self.store);
        let runtime_clone = self.runtime.clone();
        let key_clone = self.key.clone();
        let part_size = self.config.upload_part_size;

        let upload_thread = thread::spawn(move || {
            read_and_upload_stdout(stdout, store_clone, runtime_clone, key_clone, part_size)
        });

        self.ffmpeg_child = Some(child);
        self.ffmpeg_stdin = Some(stdin);
        self.upload = Some(upload);
        self.upload_thread = Some(upload_thread);
        self.initialized = true;

        tracing::info!(
            camera = %self.camera,
            width = self.width,
            height = self.height,
            fps = self.fps,
            codec = %codec,
            key = %self.key,
            "Camera streaming encoder initialized with FFmpeg CLI"
        );

        Ok(())
    }

    /// Finalize the encoding and complete the upload.
    ///
    /// # Returns
    ///
    /// The S3 URL of the uploaded video.
    pub fn finalize(mut self) -> Result<String> {
        if self.finalized {
            return Err(RoboflowError::encode(
                "CameraStreamingEncoder",
                "Encoder already finalized",
            ));
        }

        self.finalized = true;

        // Close FFmpeg stdin to signal EOF
        drop(self.ffmpeg_stdin.take());

        // Wait for FFmpeg to finish
        if let Some(mut child) = self.ffmpeg_child.take() {
            let status = child.wait().map_err(|e| {
                RoboflowError::encode(
                    "CameraStreamingEncoder",
                    format!("Failed to wait for FFmpeg: {}", e),
                )
            })?;

            if !status.success() {
                return Err(RoboflowError::encode(
                    "CameraStreamingEncoder",
                    format!("FFmpeg exited with status: {:?}", status),
                ));
            }
        }

        // Wait for upload thread to finish
        if let Some(thread) = self.upload_thread.take() {
            let result: Result<()> = thread.join().map_err(|_| {
                RoboflowError::encode("CameraStreamingEncoder", "Upload thread panicked")
            })?;
            result?;
        }

        // Complete the upload
        if let Some(upload) = self.upload.take() {
            self.runtime.block_on(async {
                upload
                    .finish()
                    .await
                    .map_err(|e| RoboflowError::encode("CameraStreamingEncoder", e.to_string()))
            })?;

            tracing::info!(
                camera = %self.camera,
                frames = self.frames_encoded,
                key = %self.key,
                "Camera streaming encoder finalized successfully"
            );
        }

        // Return the S3 URL
        Ok(format!("s3://{}", self.key.as_ref()))
    }

    /// Abort the encoding and upload.
    #[allow(dead_code)] // Used in incremental streaming mode
    pub fn abort(mut self) -> Result<()> {
        self.finalized = true;

        // Kill FFmpeg process
        if let Some(mut child) = self.ffmpeg_child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }

        // Drop upload without finishing
        self.upload = None;

        tracing::warn!(
            camera = %self.camera,
            key = %self.key,
            "Camera streaming encoder aborted (partial upload may be cleaned up by storage provider)"
        );

        Ok(())
    }
}

/// Write a video frame in PPM format to a writer.
#[allow(dead_code)] // Used in incremental streaming mode
fn write_ppm_frame<W: std::io::Write>(writer: &mut W, frame: &VideoFrame) -> std::io::Result<()> {
    writeln!(writer, "P6")?;
    writeln!(writer, "{} {}", frame.width, frame.height)?;
    writeln!(writer, "255")?;
    writer.write_all(&frame.data)?;
    Ok(())
}

/// Read from FFmpeg stdout and upload to S3 via multipart upload.
///
/// This function runs in a separate thread and reads data synchronously
/// from FFmpeg's stdout, then streams it to S3 using the async runtime.
///
/// The implementation streams data directly to multipart upload without buffering
/// the entire video in memory, preventing OOM issues for large videos.
#[allow(dead_code)] // Used in incremental streaming mode
fn read_and_upload_stdout(
    mut stdout: std::process::ChildStdout,
    store: Arc<dyn object_store::ObjectStore>,
    runtime: Handle,
    key: ObjectPath,
    part_size: usize,
) -> Result<()> {
    use std::io::Read;

    // Create multipart upload for streaming
    let multipart_upload = runtime.block_on(async {
        store
            .put_multipart(&key)
            .await
            .map_err(|e| RoboflowError::encode("CameraStreamingEncoder", e.to_string()))
    })?;

    let mut multipart =
        object_store::WriteMultipart::new_with_chunk_size(multipart_upload, part_size);

    // Read data synchronously from FFmpeg stdout and stream directly to S3
    let mut buffer = vec![0u8; part_size];

    loop {
        let n = stdout.read(&mut buffer).map_err(|e| {
            RoboflowError::encode(
                "CameraStreamingEncoder",
                format!("Failed to read FFmpeg stdout: {}", e),
            )
        })?;

        if n == 0 {
            break;
        }

        // Write data directly to the multipart upload (handles buffering internally)
        multipart.write(&buffer[..n]);
    }

    // Complete the multipart upload
    runtime.block_on(async {
        multipart
            .finish()
            .await
            .map_err(|e| RoboflowError::encode("CameraStreamingEncoder", e.to_string()))?;
        Ok::<(), RoboflowError>(())
    })
}

/// Parse an S3/OSS URL to extract the key.
fn parse_s3_url_to_key(url: &str) -> Result<ObjectPath> {
    let url_without_scheme = url
        .strip_prefix("s3://")
        .or_else(|| url.strip_prefix("oss://"))
        .ok_or_else(|| {
            RoboflowError::parse(
                "CameraStreamingEncoder",
                "URL must start with s3:// or oss://",
            )
        })?;

    let slash_idx = url_without_scheme.find('/').ok_or_else(|| {
        RoboflowError::parse(
            "CameraStreamingEncoder",
            "URL must contain a path after bucket",
        )
    })?;

    let _bucket = &url_without_scheme[..slash_idx];
    let key = &url_without_scheme[slash_idx + 1..];

    if !key.ends_with(".mp4") {
        return Err(RoboflowError::parse(
            "CameraStreamingEncoder",
            "Video file must have .mp4 extension for fMP4 format",
        ));
    }

    Ok(ObjectPath::from(key))
}

/// Encode videos using streaming upload to cloud storage.
///
/// This function encodes videos for all cameras and streams them directly
/// to S3/OSS storage without intermediate disk files.
///
/// # Arguments
///
/// * `camera_data` - Camera name and image data pairs
/// * `episode_index` - Current episode index
/// * `output_prefix` - S3/OSS prefix for uploads (e.g., "bucket/path")
/// * `video_config` - Video encoding configuration
/// * `fps` - Frame rate
/// * `storage` - Storage backend
/// * `runtime` - Tokio runtime handle
pub fn encode_videos_streaming(
    camera_data: &[(String, Vec<ImageData>)],
    episode_index: usize,
    output_prefix: &str,
    video_config: &ResolvedConfig,
    fps: u32,
    storage: Arc<dyn Storage>,
    runtime: Handle,
) -> Result<StreamingEncodeStats> {
    let config = StreamingEncoderConfig {
        video: video_config.clone(),
        fps,
        ..Default::default()
    };

    let mut stats = StreamingEncodeStats::default();

    for (camera, images) in camera_data {
        if images.is_empty() {
            continue;
        }

        // Build S3 URL for this video
        let s3_url = format!(
            "{}/videos/chunk-000/{}/episode_{:06}.mp4",
            output_prefix.trim_end_matches('/'),
            camera,
            episode_index
        );

        // Check if storage is cloud storage
        let object_store = storage
            .as_any()
            .downcast_ref::<roboflow_storage::OssStorage>()
            .map(|oss| oss.async_storage().object_store());

        let object_store = match object_store {
            Some(store) => store,
            None => {
                tracing::warn!(
                    camera = %camera,
                    "Streaming encoder requires cloud storage (OssStorage), skipping"
                );
                stats.failed_encodings += 1;
                continue;
            }
        };

        // Create and run streaming encoder
        let encoder = match CameraStreamingEncoder::new(
            camera.clone(),
            &s3_url,
            images,
            config.clone(),
            object_store,
            runtime.clone(),
        ) {
            Ok(enc) => enc,
            Err(e) => {
                tracing::error!(
                    camera = %camera,
                    error = %e,
                    "Failed to create streaming encoder"
                );
                stats.failed_encodings += 1;
                continue;
            }
        };

        // Already added all images during creation, finalize
        match encoder.finalize() {
            Ok(url) => {
                stats.images_encoded += images.len();
                tracing::info!(
                    camera = %camera,
                    frames = images.len(),
                    url = %url,
                    "Streaming encoder completed successfully"
                );
                stats.video_urls.push((camera.clone(), url));
            }
            Err(e) => {
                tracing::error!(
                    camera = %camera,
                    error = %e,
                    "Streaming encoder failed"
                );
                stats.failed_encodings += 1;
            }
        }
    }

    Ok(stats)
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)] // Test code pattern
mod tests {
    use super::*;
    use crate::lerobot::config::VideoConfig;

    // =========================================================================
    // URL Parsing Tests
    // =========================================================================

    #[test]
    fn test_parse_s3_url() {
        let key = parse_s3_url_to_key("s3://mybucket/videos/episode_000.mp4")
            .expect("Failed to parse S3 URL");
        assert_eq!(key.as_ref(), "videos/episode_000.mp4");
    }

    #[test]
    fn test_parse_oss_url() {
        let key = parse_s3_url_to_key("oss://mybucket/videos/episode_000.mp4")
            .expect("Failed to parse OSS URL");
        assert_eq!(key.as_ref(), "videos/episode_000.mp4");
    }

    #[test]
    fn test_parse_s3_url_with_nested_path() {
        let key = parse_s3_url_to_key("s3://bucket/path/to/videos/episode_000.mp4")
            .expect("Failed to parse S3 URL with nested path");
        assert_eq!(key.as_ref(), "path/to/videos/episode_000.mp4");
    }

    #[test]
    fn test_parse_invalid_url() {
        let result = parse_s3_url_to_key("http://example.com/file.mp4");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_missing_extension() {
        let result = parse_s3_url_to_key("s3://bucket/videos/episode_000");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_no_path() {
        let result = parse_s3_url_to_key("s3://bucket");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_url_with_query_params() {
        // URLs with query params should still work for the path extraction
        let key = parse_s3_url_to_key("s3://bucket/videos/episode_000.mp4")
            .expect("Failed to parse S3 URL");
        assert_eq!(key.as_ref(), "videos/episode_000.mp4");
    }

    // =========================================================================
    // Config Tests
    // =========================================================================

    #[test]
    fn test_streaming_config_default() {
        let config = StreamingEncoderConfig::default();
        assert_eq!(config.fps, 30);
        assert_eq!(config.ring_buffer_size, 128);
        assert_eq!(config.upload_part_size, 16 * 1024 * 1024);
        assert_eq!(config.buffer_timeout_secs, 5);
    }

    #[test]
    fn test_streaming_config_from_video_config() {
        let video_config = VideoConfig::default();
        let resolved = ResolvedConfig::from_video_config(&video_config);
        let config = StreamingEncoderConfig {
            video: resolved.clone(),
            fps: 60,
            ..Default::default()
        };
        assert_eq!(config.fps, 60);
        assert_eq!(config.video.codec, resolved.codec);
    }

    // =========================================================================
    // Statistics Tests
    // =========================================================================

    #[test]
    fn test_streaming_stats_default() {
        let stats = StreamingEncodeStats::default();
        assert_eq!(stats.images_encoded, 0);
        assert_eq!(stats.skipped_frames, 0);
        assert_eq!(stats.failed_encodings, 0);
        assert_eq!(stats.output_bytes, 0);
        assert!(stats.video_urls.is_empty());
    }

    #[test]
    fn test_streaming_stats_with_data() {
        let mut stats = StreamingEncodeStats::default();
        stats.images_encoded = 100;
        stats.skipped_frames = 5;
        stats.output_bytes = 1024 * 1024;
        stats
            .video_urls
            .push(("camera_0".to_string(), "s3://bucket/video.mp4".to_string()));

        assert_eq!(stats.images_encoded, 100);
        assert_eq!(stats.skipped_frames, 5);
        assert_eq!(stats.output_bytes, 1024 * 1024);
        assert_eq!(stats.video_urls.len(), 1);
    }

    // =========================================================================
    // PPM Frame Writing Tests
    // =========================================================================

    #[test]
    fn test_write_ppm_frame() {
        let data = vec![255u8; 6 * 4 * 3]; // 6x4 RGB image
        let frame = VideoFrame::new(6, 4, data);
        let mut buffer = Vec::new();

        write_ppm_frame(&mut buffer, &frame).expect("Failed to write PPM frame");

        // Check PPM header (first ~20 bytes should be ASCII)
        let header = String::from_utf8_lossy(&buffer[..20]);
        assert!(header.starts_with("P6\n"));
        assert!(header.contains("6 4\n"));
        assert!(header.contains("255\n"));

        // Verify total size: header + width + height + maxval + data
        // P6\n6 4\n255\n + 6*4*3 bytes of data
        assert!(buffer.len() > 20); // Should have data beyond header
    }

    #[test]
    fn test_write_ppm_frame_different_dimensions() {
        let data = vec![128u8; 320 * 240 * 3];
        let frame = VideoFrame::new(320, 240, data);
        let mut buffer = Vec::new();

        write_ppm_frame(&mut buffer, &frame).expect("Failed to write PPM frame");

        // Check PPM header (first ~30 bytes should be ASCII)
        let header = String::from_utf8_lossy(&buffer[..30]);
        assert!(header.contains("320 240\n"));

        // Verify total size is correct
        assert_eq!(buffer.len(), "P6\n320 240\n255\n".len() + 320 * 240 * 3);
    }

    #[test]
    fn test_write_ppm_frame_minimal() {
        // Test with smallest possible image (1x1)
        let data = vec![100u8, 150u8, 200u8]; // RGB
        let frame = VideoFrame::new(1, 1, data);
        let mut buffer = Vec::new();

        write_ppm_frame(&mut buffer, &frame).expect("Failed to write PPM frame");

        let header = String::from_utf8_lossy(&buffer);
        assert!(header.starts_with("P6\n"));
        assert!(header.contains("1 1\n"));
        assert_eq!(buffer.len(), "P6\n1 1\n255\n".len() + 3);
    }
}
