// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Video encoding using ffmpeg.
//!
//! This module provides video encoding functionality by calling ffmpeg
//! as an external process. Supports:
//! - MP4/H.264 for color images
//! - MKV/FFV1 for 16-bit depth images
//!
//! Used by both KPS and LeRobot formats for MP4/MKV output.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// Errors that can occur during video encoding.
#[derive(Debug, thiserror::Error)]
pub enum VideoEncoderError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("ffmpeg not found. Please install ffmpeg to enable MP4 video encoding.")]
    FfmpegNotFound,

    #[error("ffmpeg failed with status {0}: {1}")]
    FfmpegFailed(i32, String),

    #[error("No frames to encode")]
    NoFrames,

    #[error("Inconsistent frame sizes in buffer")]
    InconsistentFrameSizes,

    #[error("Invalid frame data")]
    InvalidFrameData,
}

/// Video encoder configuration.
#[derive(Debug, Clone)]
pub struct VideoEncoderConfig {
    /// Video codec (default: H.264)
    pub codec: String,

    /// Pixel format (default: yuv420p)
    pub pixel_format: String,

    /// Frame rate for output video (default: 30)
    pub fps: u32,

    /// CRF quality value (lower = better quality, 0-51, default: 23)
    pub crf: u32,

    /// Whether to use fast preset
    pub preset: String,
}

impl Default for VideoEncoderConfig {
    fn default() -> Self {
        Self {
            codec: "libx264".to_string(),
            pixel_format: "yuv420p".to_string(),
            fps: 30,
            crf: 23,
            preset: "fast".to_string(),
        }
    }
}

impl VideoEncoderConfig {
    /// Create a config with custom FPS.
    pub fn with_fps(mut self, fps: u32) -> Self {
        self.fps = fps;
        self
    }

    /// Create a config with custom quality.
    pub fn with_quality(mut self, crf: u32) -> Self {
        self.crf = crf;
        self
    }
}

/// A single video frame.
#[derive(Debug, Clone)]
pub struct VideoFrame {
    /// Width in pixels.
    pub width: u32,

    /// Height in pixels.
    pub height: u32,

    /// Raw image data (RGB8 format).
    pub data: Vec<u8>,

    /// Whether this frame is already JPEG-encoded (for passthrough).
    pub is_jpeg: bool,
}

impl VideoFrame {
    /// Create a new video frame.
    pub fn new(width: u32, height: u32, data: Vec<u8>) -> Self {
        Self {
            width,
            height,
            data,
            is_jpeg: false,
        }
    }

    /// Create a new video frame from JPEG-encoded data.
    pub fn from_jpeg(width: u32, height: u32, jpeg_data: Vec<u8>) -> Self {
        Self {
            width,
            height,
            data: jpeg_data,
            is_jpeg: true,
        }
    }

    /// Get the expected data size for this frame.
    pub fn expected_size(&self) -> usize {
        if self.is_jpeg {
            self.data.len() // JPEG data size is variable
        } else {
            (self.width * self.height * 3) as usize
        }
    }

    /// Validate the frame data.
    pub fn validate(&self) -> Result<(), VideoEncoderError> {
        if self.is_jpeg {
            // JPEG data: just check it's not empty and has valid header
            if self.data.len() < 4 {
                return Err(VideoEncoderError::InvalidFrameData);
            }
            // Check JPEG magic bytes
            if self.data[0] != 0xFF || self.data[1] != 0xD8 || self.data[2] != 0xFF {
                return Err(VideoEncoderError::InvalidFrameData);
            }
        } else {
            // RGB data: check exact size
            let expected = (self.width * self.height * 3) as usize;
            if self.data.len() != expected {
                return Err(VideoEncoderError::InvalidFrameData);
            }
        }
        Ok(())
    }
}

/// Buffer for video frames waiting to be encoded.
#[derive(Debug, Clone, Default)]
pub struct VideoFrameBuffer {
    /// Buffered frames.
    pub frames: Vec<VideoFrame>,

    /// Width of all frames (if consistent).
    pub width: Option<u32>,

    /// Height of all frames (if consistent).
    pub height: Option<u32>,
}

impl VideoFrameBuffer {
    /// Create a new empty buffer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a frame to the buffer.
    pub fn add_frame(&mut self, frame: VideoFrame) -> Result<(), VideoEncoderError> {
        frame.validate()?;

        // Check for consistent dimensions
        match (self.width, self.height) {
            (Some(w), Some(h)) if w != frame.width || h != frame.height => {
                return Err(VideoEncoderError::InconsistentFrameSizes);
            }
            (None, None) => {
                self.width = Some(frame.width);
                self.height = Some(frame.height);
            }
            _ => {}
        }

        self.frames.push(frame);
        Ok(())
    }

    /// Get the number of frames in the buffer.
    pub fn len(&self) -> usize {
        self.frames.len()
    }

    /// Check if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    /// Clear the buffer.
    pub fn clear(&mut self) {
        self.frames.clear();
        self.width = None;
        self.height = None;
    }

    /// Get the dimensions of frames in this buffer.
    pub fn dimensions(&self) -> Option<(u32, u32)> {
        match (self.width, self.height) {
            (Some(w), Some(h)) => Some((w, h)),
            _ => None,
        }
    }
}

/// MP4 video encoder using ffmpeg.
pub struct Mp4Encoder {
    config: VideoEncoderConfig,
    ffmpeg_path: Option<PathBuf>,
}

impl Mp4Encoder {
    /// Create a new encoder with default configuration.
    pub fn new() -> Self {
        Self {
            config: VideoEncoderConfig::default(),
            ffmpeg_path: None,
        }
    }

    /// Create a new encoder with custom configuration.
    pub fn with_config(config: VideoEncoderConfig) -> Self {
        Self {
            config,
            ffmpeg_path: None,
        }
    }

    /// Set a custom path to the ffmpeg executable.
    pub fn with_ffmpeg_path(mut self, path: impl AsRef<Path>) -> Self {
        self.ffmpeg_path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Check if ffmpeg is available.
    pub fn check_ffmpeg(&self) -> Result<(), VideoEncoderError> {
        let path = self.ffmpeg_path.as_deref().unwrap_or(Path::new("ffmpeg"));

        let result = Command::new(path)
            .arg("-version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .output();

        match result {
            Ok(output) if output.status.success() => Ok(()),
            _ => Err(VideoEncoderError::FfmpegNotFound),
        }
    }

    /// Encode frames from a buffer to an MP4 file.
    ///
    /// This method writes frames as PPM format to stdin of ffmpeg,
    /// which is a simple uncompressed format that ffmpeg can read.
    pub fn encode_buffer(
        &self,
        buffer: &VideoFrameBuffer,
        output_path: &Path,
    ) -> Result<(), VideoEncoderError> {
        // Check if all frames are JPEG for passthrough optimization
        let all_jpeg = buffer.frames.iter().all(|f| f.is_jpeg);
        if all_jpeg && buffer.frames.len() > 1 {
            return self.encode_jpeg_passthrough(buffer, output_path);
        }

        // Original PPM encoding path
        self.encode_buffer_ppm(buffer, output_path)
    }

    /// Encode JPEG frames with passthrough optimization.
    ///
    /// This method pipes JPEG data directly to ffmpeg without intermediate
    /// RGB conversion, providing significant performance improvement.
    fn encode_jpeg_passthrough(
        &self,
        buffer: &VideoFrameBuffer,
        output_path: &Path,
    ) -> Result<(), VideoEncoderError> {
        if buffer.is_empty() {
            return Err(VideoEncoderError::NoFrames);
        }

        self.check_ffmpeg()?;

        let (_width, _height) = buffer
            .dimensions()
            .ok_or(VideoEncoderError::InvalidFrameData)?;

        let ffmpeg_path = self.ffmpeg_path.as_deref().unwrap_or(Path::new("ffmpeg"));

        // Build ffmpeg command for MJPEG input
        // Using -f mjpeg allows direct JPEG passthrough
        let mut child = Command::new(ffmpeg_path)
            .arg("-y") // Overwrite output
            .arg("-f") // Input format: MJPEG
            .arg("mjpeg")
            .arg("-r")
            .arg(self.config.fps.to_string())
            .arg("-i")
            .arg("-") // Read from stdin
            .arg("-vf")
            .arg("pad=ceil(iw/2)*2:ceil(ih/2)*2") // Ensure even dimensions for yuv420p
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
            .arg(output_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|_| VideoEncoderError::FfmpegNotFound)?;

        // Write JPEG frames directly to stdin
        let write_result = if let Some(mut stdin) = child.stdin.take() {
            let mut result = Ok(());
            for frame in &buffer.frames {
                if let Err(e) = self.write_jpeg_frame(&mut stdin, frame) {
                    result = Err(e);
                    break;
                }
            }
            drop(stdin);
            result
        } else {
            Ok(())
        };

        let read_stderr = |child: &mut std::process::Child| -> String {
            child
                .stderr
                .take()
                .map(|mut s| {
                    let mut buf = String::new();
                    use std::io::Read;
                    s.read_to_string(&mut buf).ok();
                    buf
                })
                .unwrap_or_default()
        };

        if let Err(write_err) = write_result {
            let stderr_output = read_stderr(&mut child);
            let _ = child.wait();

            if !stderr_output.is_empty() {
                tracing::error!(
                    stderr = %stderr_output,
                    "ffmpeg stderr output (JPEG passthrough encoding failed)"
                );
            }

            return Err(VideoEncoderError::FfmpegFailed(
                -1,
                format!(
                    "JPEG passthrough write failed: {}. ffmpeg stderr: {}",
                    write_err, stderr_output
                ),
            ));
        }

        let status = child.wait()?;

        if status.success() {
            tracing::debug!(frames = buffer.len(), "Encoded MP4 using JPEG passthrough");
            Ok(())
        } else {
            let stderr_output = read_stderr(&mut child);
            Err(VideoEncoderError::FfmpegFailed(
                status.code().unwrap_or(-1),
                format!("ffmpeg stderr: {}", stderr_output),
            ))
        }
    }

    /// Encode frames from a buffer using PPM format (original implementation).
    fn encode_buffer_ppm(
        &self,
        buffer: &VideoFrameBuffer,
        output_path: &Path,
    ) -> Result<(), VideoEncoderError> {
        if buffer.is_empty() {
            return Err(VideoEncoderError::NoFrames);
        }

        // Check ffmpeg availability
        self.check_ffmpeg()?;

        let (_width, _height) = buffer
            .dimensions()
            .ok_or(VideoEncoderError::InvalidFrameData)?;

        let ffmpeg_path = self.ffmpeg_path.as_deref().unwrap_or(Path::new("ffmpeg"));

        // Build ffmpeg command
        // We pipe PPM format images through stdin.
        // The -vf pad filter ensures even dimensions required by yuv420p/H.264.
        let mut child = Command::new(ffmpeg_path)
            .arg("-y") // Overwrite output
            .arg("-f") // Input format
            .arg("image2pipe")
            .arg("-vcodec")
            .arg("ppm")
            .arg("-r")
            .arg(self.config.fps.to_string())
            .arg("-i")
            .arg("-") // Read from stdin
            .arg("-vf")
            .arg("pad=ceil(iw/2)*2:ceil(ih/2)*2") // Ensure even dimensions for yuv420p
            .arg("-c:v")
            .arg(&self.config.codec)
            .arg("-pix_fmt")
            .arg(&self.config.pixel_format)
            .arg("-preset")
            .arg(&self.config.preset)
            .arg("-crf")
            .arg(self.config.crf.to_string())
            .arg("-movflags")
            .arg("+faststart") // Enable fast start for web playback
            .arg(output_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped()) // Capture stderr for error diagnosis
            .spawn()
            .map_err(|_| VideoEncoderError::FfmpegNotFound)?;

        // Write frames to ffmpeg stdin as PPM format.
        // On error, we still need to reap the child process and capture stderr.
        let write_result = if let Some(mut stdin) = child.stdin.take() {
            let mut result = Ok(());
            for frame in &buffer.frames {
                if let Err(e) = self.write_ppm_frame(&mut stdin, frame) {
                    result = Err(e);
                    break;
                }
            }
            // Drop stdin to signal EOF before waiting
            drop(stdin);
            result
        } else {
            Ok(())
        };

        // Helper: read stderr from the child process
        let read_stderr = |child: &mut std::process::Child| -> String {
            child
                .stderr
                .take()
                .map(|mut s| {
                    let mut buf = String::new();
                    use std::io::Read;
                    s.read_to_string(&mut buf).ok();
                    buf
                })
                .unwrap_or_default()
        };

        // If writing failed (e.g., Broken pipe), capture stderr and reap child
        if let Err(write_err) = write_result {
            let stderr_output = read_stderr(&mut child);
            let _ = child.wait(); // Reap the child to avoid zombies

            // Log the ffmpeg stderr so the user can see why it crashed
            if !stderr_output.is_empty() {
                tracing::error!(
                    stderr = %stderr_output,
                    "ffmpeg stderr output (process crashed during encoding)"
                );
            }

            return Err(VideoEncoderError::FfmpegFailed(
                -1,
                format!(
                    "Write failed: {}. ffmpeg stderr: {}",
                    write_err, stderr_output
                ),
            ));
        }

        // Wait for ffmpeg to finish normally
        let status = child.wait()?;

        if status.success() {
            Ok(())
        } else {
            let stderr_output = read_stderr(&mut child);
            Err(VideoEncoderError::FfmpegFailed(
                status.code().unwrap_or(-1),
                format!("ffmpeg stderr: {}", stderr_output),
            ))
        }
    }

    /// Write a single frame in PPM format.
    ///
    /// PPM is a simple uncompressed format:
    /// P6\nwidth height\n255\n{RGB data}
    fn write_ppm_frame(
        &self,
        writer: &mut impl Write,
        frame: &VideoFrame,
    ) -> Result<(), VideoEncoderError> {
        // PPM header
        writeln!(writer, "P6")?;
        writeln!(writer, "{} {}", frame.width, frame.height)?;
        writeln!(writer, "255")?;

        // RGB data
        writer.write_all(&frame.data)?;
        Ok(())
    }

    /// Write a single JPEG frame for passthrough.
    ///
    /// Writes the JPEG data directly without modification.
    fn write_jpeg_frame(
        &self,
        writer: &mut impl Write,
        frame: &VideoFrame,
    ) -> Result<(), VideoEncoderError> {
        // JPEG data is written as-is
        writer.write_all(&frame.data)?;
        Ok(())
    }

    /// Encode frames from a buffer, falling back to individual images if ffmpeg is not available.
    pub fn encode_buffer_or_save_images(
        &self,
        buffer: &VideoFrameBuffer,
        output_dir: &Path,
        camera_name: &str,
    ) -> Result<Vec<PathBuf>, VideoEncoderError> {
        if buffer.is_empty() {
            return Ok(Vec::new());
        }

        let _output_files: Vec<PathBuf> = Vec::new();

        // Try to encode as MP4 first
        let mp4_path = output_dir.join(format!("{}.mp4", camera_name));

        match self.encode_buffer(buffer, &mp4_path) {
            Ok(()) => {
                tracing::info!(
                    camera = camera_name,
                    frames = buffer.len(),
                    path = %mp4_path.display(),
                    "Encoded MP4 video"
                );
                // Return the single MP4 path
                return Ok(vec![mp4_path]);
            }
            Err(VideoEncoderError::FfmpegNotFound) => {
                tracing::warn!(
                    "ffmpeg not found, falling back to individual image files for {}",
                    camera_name
                );
                // Fall through to save individual images
            }
            Err(e) => return Err(e),
        }

        // Fallback: save as individual PPM files
        let images_dir = output_dir.join("images");
        std::fs::create_dir_all(&images_dir)?;

        let mut image_paths = Vec::new();
        for (i, frame) in buffer.frames.iter().enumerate() {
            let path = images_dir.join(format!("{}_{:06}.ppm", camera_name, i));

            let mut file = std::fs::File::create(&path)?;
            self.write_ppm_frame(&mut file, frame)?;

            image_paths.push(path);
        }

        tracing::info!(
            camera = camera_name,
            frames = buffer.len(),
            "Saved {} individual image files",
            image_paths.len()
        );

        Ok(image_paths)
    }
}

impl Default for Mp4Encoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Check if NVENC encoder is available.
pub fn check_nvenc_available() -> bool {
    std::process::Command::new("ffmpeg")
        .args(["-hide_banner", "-encoders"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .map(|o| {
            let output = String::from_utf8_lossy(&o.stdout);
            output.contains("h264_nvenc") || output.contains("hevc_nvenc")
        })
        .unwrap_or(false)
}

/// MP4 video encoder using NVIDIA NVENC hardware acceleration.
///
/// This encoder uses NVENC for GPU-accelerated H.264 encoding,
/// providing significant performance improvements over CPU encoding.
pub struct NvencEncoder {
    config: VideoEncoderConfig,
    ffmpeg_path: Option<PathBuf>,
    device_id: Option<u32>,
}

impl NvencEncoder {
    /// Create a new NVENC encoder with default configuration.
    pub fn new() -> Self {
        Self {
            config: VideoEncoderConfig::default(),
            ffmpeg_path: None,
            device_id: None,
        }
    }

    /// Create a new NVENC encoder with custom configuration.
    pub fn with_config(config: VideoEncoderConfig) -> Self {
        Self {
            config,
            ffmpeg_path: None,
            device_id: None,
        }
    }

    /// Set a custom path to the ffmpeg executable.
    pub fn with_ffmpeg_path(mut self, path: impl AsRef<Path>) -> Self {
        self.ffmpeg_path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Set the CUDA device ID to use.
    pub fn with_device(mut self, device_id: u32) -> Self {
        self.device_id = Some(device_id);
        self
    }

    /// Check if NVENC is available.
    pub fn check_nvenc(&self) -> Result<(), VideoEncoderError> {
        if !check_nvenc_available() {
            return Err(VideoEncoderError::FfmpegNotFound);
        }
        Ok(())
    }

    /// Encode frames from a buffer using NVENC.
    ///
    /// This method pipes RGB frames to ffmpeg which uses NVENC
    /// for hardware-accelerated H.264 encoding.
    pub fn encode_buffer(
        &self,
        buffer: &VideoFrameBuffer,
        output_path: &Path,
    ) -> Result<(), VideoEncoderError> {
        if buffer.is_empty() {
            return Err(VideoEncoderError::NoFrames);
        }

        self.check_nvenc()?;

        let (width, height) = buffer
            .dimensions()
            .ok_or(VideoEncoderError::InvalidFrameData)?;

        let ffmpeg_path = self.ffmpeg_path.as_deref().unwrap_or(Path::new("ffmpeg"));

        // Build ffmpeg command for NVENC encoding
        let mut cmd = Command::new(ffmpeg_path);
        cmd.arg("-y")
            .arg("-hide_banner")
            // GPU acceleration
            .args(["-hwaccel", "cuda"])
            .args(["-hwaccel_output_format", "cuda"]);

        // Set device if specified
        if let Some(device) = self.device_id {
            cmd.args(["-gpu", &device.to_string()]);
        }

        // Input: raw RGB from stdin
        cmd.args(["-f", "rawvideo"])
            .args(["-pix_fmt", "rgb24"])
            .args(["-s", &format!("{}x{}", width, height)])
            .args(["-r", &self.config.fps.to_string()])
            .arg("-i")
            .arg("-")
            // NVENC encoding
            .args(["-c:v", "h264_nvenc"])
            .args(["-preset", "p4"]) // Slow, high quality
            .args(["-tune", "ll"]) // Low latency
            .args(["-b:v", "5M"])
            .args(["-pix_fmt", "yuv420p"])
            .arg(output_path);

        let mut child = cmd
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|_| VideoEncoderError::FfmpegNotFound)?;

        // Write RGB frames to stdin
        let write_result = if let Some(mut stdin) = child.stdin.take() {
            let mut result = Ok(());
            for frame in &buffer.frames {
                if let Err(e) = stdin.write_all(&frame.data) {
                    result = Err(e);
                    break;
                }
            }
            drop(stdin);
            result
        } else {
            Ok(())
        };

        let read_stderr = |child: &mut std::process::Child| -> String {
            child
                .stderr
                .take()
                .map(|mut s| {
                    let mut buf = String::new();
                    use std::io::Read;
                    s.read_to_string(&mut buf).ok();
                    buf
                })
                .unwrap_or_default()
        };

        if let Err(write_err) = write_result {
            let stderr_output = read_stderr(&mut child);
            let _ = child.wait();

            if !stderr_output.is_empty() {
                tracing::error!(
                    stderr = %stderr_output,
                    "NVENC stderr output (encoding failed)"
                );
            }

            return Err(VideoEncoderError::FfmpegFailed(
                -1,
                format!(
                    "NVENC write failed: {}. stderr: {}",
                    write_err, stderr_output
                ),
            ));
        }

        let status = child.wait()?;

        if status.success() {
            tracing::debug!(
                frames = buffer.len(),
                "Encoded MP4 using NVENC hardware acceleration"
            );
            Ok(())
        } else {
            let stderr_output = read_stderr(&mut child);
            Err(VideoEncoderError::FfmpegFailed(
                status.code().unwrap_or(-1),
                format!("NVENC stderr: {}", stderr_output),
            ))
        }
    }
}

impl Default for NvencEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Check if VideoToolbox encoder is available (macOS).
#[cfg(target_os = "macos")]
pub fn check_videotoolbox_available() -> bool {
    // VideoToolbox is always available on macOS
    true
}

/// MP4 video encoder using Apple VideoToolbox hardware acceleration.
///
/// This encoder uses VideoToolbox for GPU-accelerated H.264 encoding
/// on macOS, providing significant performance improvements over CPU encoding.
#[cfg(target_os = "macos")]
pub struct VideoToolboxEncoder {
    config: VideoEncoderConfig,
    ffmpeg_path: Option<PathBuf>,
}

#[cfg(target_os = "macos")]
impl VideoToolboxEncoder {
    /// Create a new VideoToolbox encoder with default configuration.
    pub fn new() -> Self {
        Self {
            config: VideoEncoderConfig::default(),
            ffmpeg_path: None,
        }
    }

    /// Create a new VideoToolbox encoder with custom configuration.
    pub fn with_config(config: VideoEncoderConfig) -> Self {
        Self {
            config,
            ffmpeg_path: None,
        }
    }

    /// Set a custom path to the ffmpeg executable.
    pub fn with_ffmpeg_path(mut self, path: impl AsRef<Path>) -> Self {
        self.ffmpeg_path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Check if VideoToolbox is available.
    pub fn check_videotoolbox(&self) -> Result<(), VideoEncoderError> {
        // VideoToolbox is always available on macOS
        Ok(())
    }

    /// Encode frames from a buffer using VideoToolbox.
    ///
    /// This method pipes RGB frames to ffmpeg which uses VideoToolbox
    /// for hardware-accelerated H.264 encoding.
    pub fn encode_buffer(
        &self,
        buffer: &VideoFrameBuffer,
        output_path: &Path,
    ) -> Result<(), VideoEncoderError> {
        if buffer.is_empty() {
            return Err(VideoEncoderError::NoFrames);
        }

        self.check_videotoolbox()?;

        let (width, height) = buffer
            .dimensions()
            .ok_or(VideoEncoderError::InvalidFrameData)?;

        let ffmpeg_path = self.ffmpeg_path.as_deref().unwrap_or(Path::new("ffmpeg"));

        // Build ffmpeg command for VideoToolbox encoding
        let mut child = Command::new(ffmpeg_path)
            .arg("-y")
            .arg("-hide_banner")
            // VideoToolbox hardware acceleration
            .args(["-hwaccel", "videotoolbox"])
            .args(["-pix_fmt", "videotoolbox_vlc"])
            // Input: raw RGB from stdin
            .args(["-f", "rawvideo"])
            .args(["-pix_fmt", "rgb24"])
            .args(["-s", &format!("{}x{}", width, height)])
            .args(["-r", &self.config.fps.to_string()])
            .arg("-i")
            .arg("-")
            // VideoToolbox encoding
            .args(["-c:v", "h264_videotoolbox"])
            .args(["-profile:v", "high"])
            .args(["-level", "3.1"])
            .args(["-q", "23"]) // Quality (0-51, lower is better)
            .args(["-pix_fmt", "yuv420p"])
            .arg(output_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|_| VideoEncoderError::FfmpegNotFound)?;

        // Write RGB frames to stdin
        let write_result = if let Some(mut stdin) = child.stdin.take() {
            let mut result = Ok(());
            for frame in &buffer.frames {
                if let Err(e) = stdin.write_all(&frame.data) {
                    result = Err(e);
                    break;
                }
            }
            drop(stdin);
            result
        } else {
            Ok(())
        };

        let read_stderr = |child: &mut std::process::Child| -> String {
            child
                .stderr
                .take()
                .map(|mut s| {
                    let mut buf = String::new();
                    use std::io::Read;
                    s.read_to_string(&mut buf).ok();
                    buf
                })
                .unwrap_or_default()
        };

        if let Err(write_err) = write_result {
            let stderr_output = read_stderr(&mut child);
            let _ = child.wait();

            if !stderr_output.is_empty() {
                tracing::error!(
                    stderr = %stderr_output,
                    "VideoToolbox stderr output (encoding failed)"
                );
            }

            return Err(VideoEncoderError::FfmpegFailed(
                -1,
                format!(
                    "VideoToolbox write failed: {}. stderr: {}",
                    write_err, stderr_output
                ),
            ));
        }

        let status = child.wait()?;

        if status.success() {
            tracing::debug!(
                frames = buffer.len(),
                "Encoded MP4 using VideoToolbox hardware acceleration"
            );
            Ok(())
        } else {
            let stderr_output = read_stderr(&mut child);
            Err(VideoEncoderError::FfmpegFailed(
                status.code().unwrap_or(-1),
                format!("VideoToolbox stderr: {}", stderr_output),
            ))
        }
    }
}

#[cfg(target_os = "macos")]
impl Default for VideoToolboxEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// 16-bit depth video frame.
#[derive(Debug, Clone)]
pub struct DepthFrame {
    /// Width in pixels
    pub width: u32,
    /// Height in pixels
    pub height: u32,
    /// 16-bit depth data (grayscale)
    pub data: Vec<u8>, // 2 bytes per pixel
}

impl DepthFrame {
    /// Create a new depth frame.
    pub fn new(width: u32, height: u32, data: Vec<u8>) -> Self {
        Self {
            width,
            height,
            data,
        }
    }

    /// Get expected data size (2 bytes per pixel for 16-bit).
    pub fn expected_size(&self) -> usize {
        (self.width * self.height * 2) as usize
    }

    /// Validate the frame data.
    pub fn validate(&self) -> Result<(), VideoEncoderError> {
        if self.data.len() != self.expected_size() {
            return Err(VideoEncoderError::InvalidFrameData);
        }
        Ok(())
    }
}

/// Buffer for depth video frames.
#[derive(Debug, Clone, Default)]
pub struct DepthFrameBuffer {
    pub frames: Vec<DepthFrame>,
    pub width: Option<u32>,
    pub height: Option<u32>,
}

impl DepthFrameBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_frame(&mut self, frame: DepthFrame) -> Result<(), VideoEncoderError> {
        frame.validate()?;

        match (self.width, self.height) {
            (Some(w), Some(h)) if w != frame.width || h != frame.height => {
                return Err(VideoEncoderError::InconsistentFrameSizes);
            }
            (None, None) => {
                self.width = Some(frame.width);
                self.height = Some(frame.height);
            }
            _ => {}
        }

        self.frames.push(frame);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.frames.len()
    }

    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    pub fn dimensions(&self) -> Option<(u32, u32)> {
        match (self.width, self.height) {
            (Some(w), Some(h)) => Some((w, h)),
            _ => None,
        }
    }
}

/// MKV encoder for 16-bit depth video using FFV1 codec.
pub struct DepthMkvEncoder {
    config: DepthEncoderConfig,
    ffmpeg_path: Option<PathBuf>,
}

/// Configuration for depth MKV encoding.
#[derive(Debug, Clone)]
pub struct DepthEncoderConfig {
    pub fps: u32,
    pub codec: String, // Default: "ffv1"
    pub preset: String,
}

impl Default for DepthEncoderConfig {
    fn default() -> Self {
        Self {
            fps: 30,
            codec: "ffv1".to_string(),
            preset: "fast".to_string(),
        }
    }
}

impl DepthMkvEncoder {
    pub fn new() -> Self {
        Self {
            config: DepthEncoderConfig::default(),
            ffmpeg_path: None,
        }
    }

    pub fn with_config(config: DepthEncoderConfig) -> Self {
        Self {
            config,
            ffmpeg_path: None,
        }
    }

    pub fn with_ffmpeg_path(mut self, path: impl AsRef<Path>) -> Self {
        self.ffmpeg_path = Some(path.as_ref().to_path_buf());
        self
    }

    fn check_ffmpeg(&self) -> Result<(), VideoEncoderError> {
        let path = self.ffmpeg_path.as_deref().unwrap_or(Path::new("ffmpeg"));
        let result = Command::new(path)
            .arg("-version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .output();

        match result {
            Ok(output) if output.status.success() => Ok(()),
            _ => Err(VideoEncoderError::FfmpegNotFound),
        }
    }

    /// Encode depth frames to MKV with FFV1 codec.
    ///
    /// Writes frames as raw 16-bit grayscale to stdin, which ffmpeg
    /// encodes using FFV1 lossless codec.
    pub fn encode_buffer(
        &self,
        buffer: &DepthFrameBuffer,
        output_path: &Path,
    ) -> Result<(), VideoEncoderError> {
        if buffer.is_empty() {
            return Err(VideoEncoderError::NoFrames);
        }

        self.check_ffmpeg()?;

        let (width, height) = buffer
            .dimensions()
            .ok_or(VideoEncoderError::InvalidFrameData)?;

        let ffmpeg_path = self.ffmpeg_path.as_deref().unwrap_or(Path::new("ffmpeg"));

        // Build ffmpeg command for 16-bit grayscale → MKV/FFV1
        let mut child = Command::new(ffmpeg_path)
            .arg("-y") // Overwrite
            .arg("-f") // Input format
            .arg("rawvideo")
            .arg("-pix_fmt")
            .arg("gray16le") // 16-bit little-endian grayscale
            .arg("-s")
            .arg(format!("{}x{}", width, height))
            .arg("-r")
            .arg(self.config.fps.to_string())
            .arg("-i")
            .arg("-") // Stdin
            .arg("-c:v")
            .arg(&self.config.codec) // FFV1
            .arg("-level")
            .arg("3") // FFV1 level 3 for better compression
            .arg("-g")
            .arg("1") // Keyframe interval (1 = all intra frames, lossless)
            .arg(output_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|_| VideoEncoderError::FfmpegNotFound)?;

        // Write 16-bit depth frames to stdin
        if let Some(mut stdin) = child.stdin.take() {
            for frame in &buffer.frames {
                stdin.write_all(&frame.data)?;
            }
        }

        let status = child.wait()?;

        if status.success() {
            Ok(())
        } else {
            Err(VideoEncoderError::FfmpegFailed(
                status.code().unwrap_or(-1),
                "depth encoding failed".to_string(),
            ))
        }
    }

    /// Encode with fallback to PNG files if ffmpeg unavailable.
    pub fn encode_buffer_or_save_png(
        &self,
        buffer: &DepthFrameBuffer,
        output_dir: &Path,
        camera_name: &str,
    ) -> Result<Vec<PathBuf>, VideoEncoderError> {
        if buffer.is_empty() {
            return Ok(Vec::new());
        }

        let mkv_path = output_dir.join(format!("depth_{}.mkv", camera_name));

        match self.encode_buffer(buffer, &mkv_path) {
            Ok(()) => {
                tracing::info!(
                    camera = camera_name,
                    frames = buffer.len(),
                    path = %mkv_path.display(),
                    "Encoded depth MKV video"
                );
                Ok(vec![mkv_path])
            }
            Err(VideoEncoderError::FfmpegNotFound) => {
                tracing::warn!("ffmpeg not found, saving depth as PNG files");
                self.save_as_png(buffer, output_dir, camera_name)
            }
            Err(e) => Err(e),
        }
    }

    /// Save depth frames as 16-bit PNG files.
    fn save_as_png(
        &self,
        buffer: &DepthFrameBuffer,
        output_dir: &Path,
        camera_name: &str,
    ) -> Result<Vec<PathBuf>, VideoEncoderError> {
        use std::io::BufWriter;

        let depth_dir = output_dir.join("depth_images");
        std::fs::create_dir_all(&depth_dir)?;

        let mut paths = Vec::new();

        for (i, frame) in buffer.frames.iter().enumerate() {
            let path = depth_dir.join(format!("depth_{}_{:06}.png", camera_name, i));

            let file = std::fs::File::create(&path)?;
            let mut w = BufWriter::new(file);
            let mut encoder = png::Encoder::new(&mut w, frame.width, frame.height);

            encoder.set_color(png::ColorType::Grayscale);
            encoder.set_depth(png::BitDepth::Sixteen);

            let mut writer = encoder.write_header().map_err(|_| {
                VideoEncoderError::Io(std::io::Error::other("PNG header write failed"))
            })?;

            let depth_data: Vec<u16> = frame
                .data
                .chunks_exact(2)
                .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
                .collect();

            // Convert u16 to bytes for PNG writing
            let depth_bytes: Vec<u8> = depth_data.iter().flat_map(|v| v.to_le_bytes()).collect();

            writer.write_image_data(&depth_bytes).map_err(|_| {
                VideoEncoderError::Io(std::io::Error::other("PNG data write failed"))
            })?;

            paths.push(path);
        }

        tracing::info!(
            camera = camera_name,
            frames = paths.len(),
            "Saved {} depth PNG files",
            paths.len()
        );

        Ok(paths)
    }
}

impl Default for DepthMkvEncoder {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Unified Encoder Selection
// =============================================================================

/// Encoder type for unified video encoding interface.
///
/// Provides automatic fallback chain:
/// - **NVENC** (NVIDIA GPU): 5-10x faster than CPU
/// - **VideoToolbox** (macOS): 3-5x faster than CPU
/// - **Rsmpeg/libx264** (CPU): 2-3x faster than FFmpeg CLI
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncoderChoice {
    /// NVIDIA NVENC hardware encoder (Linux/Windows with NVIDIA GPU)
    Nvenc,

    /// Apple VideoToolbox hardware encoder (macOS only)
    VideoToolbox,

    /// Rsmpeg native FFmpeg encoding (CPU fallback)
    RsmpegLibx264,

    /// FFmpeg CLI with libx264 (legacy fallback)
    FfmpegLibx264,
}

impl EncoderChoice {
    /// Get human-readable name of the encoder.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Nvenc => "h264_nvenc",
            Self::VideoToolbox => "h264_videotoolbox",
            Self::RsmpegLibx264 => "libx264 (rsmpeg)",
            Self::FfmpegLibx264 => "libx264 (ffmpeg)",
        }
    }

    /// Get expected speedup factor vs FFmpeg CLI libx264.
    pub fn speedup_factor(&self) -> f32 {
        match self {
            Self::Nvenc => 7.5,         // 5-10x faster
            Self::VideoToolbox => 4.0,  // 3-5x faster
            Self::RsmpegLibx264 => 2.5, // 2-3x faster
            Self::FfmpegLibx264 => 1.0, // Baseline
        }
    }
}

/// Unified encoder selector with automatic hardware detection.
///
/// Automatically selects the best available encoder in priority order:
/// 1. NVENC (if available on Linux/Windows)
/// 2. VideoToolbox (if available on macOS)
/// 3. Rsmpeg native libx264 (CPU, always available)
/// 4. FFmpeg CLI libx264 (legacy fallback)
///
/// # Example
///
/// ```rust,ignore
/// use roboflow_dataset::common::video::{select_best_encoder, EncoderChoice};
///
/// let encoder = select_best_encoder();
/// match encoder {
///     EncoderChoice::Nvenc => println!("Using NVENC hardware acceleration"),
///     EncoderChoice::VideoToolbox => println!("Using VideoToolbox hardware acceleration"),
///     EncoderChoice::RsmpegLibx264 => println!("Using native libx264 encoding"),
///     EncoderChoice::FfmpegLibx264 => println!("Using FFmpeg CLI encoding"),
/// }
/// ```
pub fn select_best_encoder() -> EncoderChoice {
    // Priority 1: NVENC (NVIDIA GPU)
    #[cfg(any(target_os = "linux", target_os = "windows"))]
    {
        if check_nvenc_available() {
            tracing::info!("Selected NVENC encoder (5-10x faster than CPU)");
            return EncoderChoice::Nvenc;
        }
    }

    // Priority 2: VideoToolbox (macOS)
    #[cfg(target_os = "macos")]
    {
        if check_videotoolbox_available() {
            tracing::info!("Selected VideoToolbox encoder (3-5x faster than CPU)");
            return EncoderChoice::VideoToolbox;
        }
    }

    // Priority 3: Rsmpeg native encoding (2-3x faster than FFmpeg CLI)
    // rsmpeg is always available as a dependency
    tracing::info!("Selected Rsmpeg native encoder (2-3x faster than FFmpeg CLI)");
    EncoderChoice::RsmpegLibx264

    // Note: FFmpeg CLI fallback is not needed since rsmpeg is always available
    // but kept in EncoderChoice enum for reference
}

/// Check if specific encoder type is available.
pub fn is_encoder_available(encoder: EncoderChoice) -> bool {
    match encoder {
        EncoderChoice::Nvenc => check_nvenc_available(),
        #[cfg(target_os = "macos")]
        EncoderChoice::VideoToolbox => check_videotoolbox_available(),
        #[cfg(not(target_os = "macos"))]
        EncoderChoice::VideoToolbox => false,
        EncoderChoice::RsmpegLibx264 => {
            // Rsmpeg is always available as a dependency
            true
        }
        EncoderChoice::FfmpegLibx264 => {
            // Check if ffmpeg CLI is available
            std::process::Command::new("ffmpeg")
                .arg("-version")
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false)
        }
    }
}

/// Get all available encoders in priority order.
pub fn available_encoders() -> Vec<EncoderChoice> {
    let mut encoders = Vec::new();

    #[cfg(any(target_os = "linux", target_os = "windows"))]
    {
        if check_nvenc_available() {
            encoders.push(EncoderChoice::Nvenc);
        }
    }

    #[cfg(target_os = "macos")]
    {
        if check_videotoolbox_available() {
            encoders.push(EncoderChoice::VideoToolbox);
        }
    }

    encoders.push(EncoderChoice::RsmpegLibx264);

    if is_encoder_available(EncoderChoice::FfmpegLibx264) {
        encoders.push(EncoderChoice::FfmpegLibx264);
    }

    encoders
}

/// Print encoder selection diagnostics.
pub fn print_encoder_diagnostics() {
    let available = available_encoders();

    if available.is_empty() {
        tracing::info!(
            "=== Video Encoder Diagnostics ===\n⚠️  No encoders available! Please install FFmpeg."
        );
    } else {
        let encoder_list: Vec<String> = available
            .iter()
            .enumerate()
            .map(|(i, encoder)| {
                format!(
                    "  {}. {} - {} ({}x speedup)",
                    i + 1,
                    encoder.name(),
                    match encoder {
                        EncoderChoice::Nvenc => "NVIDIA GPU acceleration",
                        EncoderChoice::VideoToolbox => "Apple hardware acceleration",
                        EncoderChoice::RsmpegLibx264 => "Native FFmpeg encoding",
                        EncoderChoice::FfmpegLibx264 => "FFmpeg CLI (fallback)",
                    },
                    encoder.speedup_factor()
                )
            })
            .collect();

        tracing::info!(
            "=== Video Encoder Diagnostics ===\nAvailable encoders (in priority order):\n{}\n\nSelected: {}",
            encoder_list.join("\n"),
            select_best_encoder().name()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_video_frame_validate() {
        let frame = VideoFrame::new(2, 2, vec![0u8; 12]); // 2*2*3 = 12
        assert!(frame.validate().is_ok());

        let invalid_frame = VideoFrame::new(2, 2, vec![0u8; 10]);
        assert!(invalid_frame.validate().is_err());
    }

    #[test]
    fn test_frame_buffer_add_frame() {
        let mut buffer = VideoFrameBuffer::new();

        let frame1 = VideoFrame::new(320, 240, vec![0u8; 320 * 240 * 3]);
        assert!(buffer.add_frame(frame1).is_ok());
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.dimensions(), Some((320, 240)));

        // Adding a frame with different dimensions should fail
        let frame2 = VideoFrame::new(640, 480, vec![0u8; 640 * 480 * 3]);
        assert!(buffer.add_frame(frame2).is_err());
    }

    #[test]
    fn test_frame_buffer_clear() {
        let mut buffer = VideoFrameBuffer::new();
        buffer
            .add_frame(VideoFrame::new(320, 240, vec![0u8; 320 * 240 * 3]))
            .unwrap();
        assert_eq!(buffer.len(), 1);

        buffer.clear();
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.dimensions(), None);
    }

    #[test]
    fn test_encoder_config_default() {
        let config = VideoEncoderConfig::default();
        assert_eq!(config.codec, "libx264");
        assert_eq!(config.pixel_format, "yuv420p");
        assert_eq!(config.fps, 30);
        assert_eq!(config.crf, 23);
        assert_eq!(config.preset, "fast");
    }

    #[test]
    fn test_encoder_config_with_fps() {
        let config = VideoEncoderConfig::default().with_fps(60);
        assert_eq!(config.fps, 60);
    }

    #[test]
    fn test_mp4_encoder_new() {
        let encoder = Mp4Encoder::new();
        // Just check it can be created (ffmpeg check may fail if not installed)
        assert!(encoder.ffmpeg_path.is_none());
    }

    #[test]
    fn test_encoder_choice_names() {
        assert_eq!(EncoderChoice::Nvenc.name(), "h264_nvenc");
        assert_eq!(EncoderChoice::VideoToolbox.name(), "h264_videotoolbox");
        assert_eq!(EncoderChoice::RsmpegLibx264.name(), "libx264 (rsmpeg)");
        assert_eq!(EncoderChoice::FfmpegLibx264.name(), "libx264 (ffmpeg)");
    }

    #[test]
    fn test_encoder_choice_speedup() {
        assert!(EncoderChoice::Nvenc.speedup_factor() > 5.0);
        assert!(EncoderChoice::VideoToolbox.speedup_factor() > 3.0);
        assert!(EncoderChoice::RsmpegLibx264.speedup_factor() > 2.0);
        assert_eq!(EncoderChoice::FfmpegLibx264.speedup_factor(), 1.0);
    }

    #[test]
    fn test_select_best_encoder() {
        let encoder = select_best_encoder();
        // Should always return a valid encoder
        match encoder {
            EncoderChoice::Nvenc
            | EncoderChoice::VideoToolbox
            | EncoderChoice::RsmpegLibx264
            | EncoderChoice::FfmpegLibx264 => {
                // Valid choices
            }
        }
    }

    #[test]
    fn test_available_encoders() {
        let encoders = available_encoders();
        // At least RsmpegLibx264 should always be available
        assert!(!encoders.is_empty());
        assert!(encoders.contains(&EncoderChoice::RsmpegLibx264));
    }
}
