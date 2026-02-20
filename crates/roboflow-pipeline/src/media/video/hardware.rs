// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Hardware-accelerated video encoders.
//!
//! This module provides video encoding using FFmpeg CLI with hardware acceleration:
//! - NVENC for NVIDIA GPUs (Linux/Windows)
//! - VideoToolbox for macOS

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use crate::media::video::config::{DepthEncoderConfig, VideoEncoderConfig};
use crate::media::video::frame::{DepthFrameBuffer, VideoEncoderError, VideoFrameBuffer};

/// Check if NVENC encoder is available.
///
/// Delegates to [`crate::media::video::hardware_config::detect_hardware_backend()`] as the
/// single source of truth for hardware detection.
pub fn check_nvenc_available() -> bool {
    matches!(
        crate::media::video::hardware_config::detect_hardware_backend(),
        crate::media::video::hardware_config::HardwareBackend::Nvenc
    )
}

/// Check if VideoToolbox encoder is available (macOS).
///
/// Delegates to [`crate::media::video::hardware_config::detect_hardware_backend()`] as the
/// single source of truth for hardware detection.
pub fn check_videotoolbox_available() -> bool {
    matches!(
        crate::media::video::hardware_config::detect_hardware_backend(),
        crate::media::video::hardware_config::HardwareBackend::VideoToolbox
    )
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
    pub fn encode_buffer(
        &self,
        buffer: &VideoFrameBuffer,
        output_path: &Path,
    ) -> Result<(), VideoEncoderError> {
        // Check if all frames are JPEG for passthrough optimization
        let all_jpeg = buffer.frames.iter().all(|f| f.is_jpeg());
        if all_jpeg && buffer.frames.len() > 1 {
            return self.encode_jpeg_passthrough(buffer, output_path);
        }

        self.encode_buffer_ppm(buffer, output_path)
    }

    fn encode_jpeg_passthrough(
        &self,
        buffer: &VideoFrameBuffer,
        output_path: &Path,
    ) -> Result<(), VideoEncoderError> {
        if buffer.is_empty() {
            return Err(VideoEncoderError::NoFrames);
        }

        self.check_ffmpeg()?;

        let ffmpeg_path = self.ffmpeg_path.as_deref().unwrap_or(Path::new("ffmpeg"));

        let mut child = Command::new(ffmpeg_path)
            .arg("-y")
            .arg("-f")
            .arg("mjpeg")
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
            .arg(output_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|_| VideoEncoderError::FfmpegNotFound)?;

        let write_result = if let Some(mut stdin) = child.stdin.take() {
            let mut result = Ok(());
            for frame in &buffer.frames {
                if let Err(e) = stdin.write_all(frame.data()) {
                    result = Err(e);
                    break;
                }
            }
            drop(stdin);
            result
        } else {
            Ok(())
        };

        if let Err(write_err) = write_result {
            let _ = child.wait();
            return Err(VideoEncoderError::FfmpegFailed(
                -1,
                format!("JPEG passthrough write failed: {}", write_err),
            ));
        }

        let status = child.wait()?;
        if status.success() {
            Ok(())
        } else {
            Err(VideoEncoderError::FfmpegFailed(
                status.code().unwrap_or(-1),
                "ffmpeg encoding failed".to_string(),
            ))
        }
    }

    fn encode_buffer_ppm(
        &self,
        buffer: &VideoFrameBuffer,
        output_path: &Path,
    ) -> Result<(), VideoEncoderError> {
        if buffer.is_empty() {
            return Err(VideoEncoderError::NoFrames);
        }

        self.check_ffmpeg()?;

        let ffmpeg_path = self.ffmpeg_path.as_deref().unwrap_or(Path::new("ffmpeg"));

        let mut child = Command::new(ffmpeg_path)
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
            .arg(output_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|_| VideoEncoderError::FfmpegNotFound)?;

        let write_result = if let Some(mut stdin) = child.stdin.take() {
            let mut result = Ok(());
            for frame in &buffer.frames {
                if let Err(e) = frame.write_ppm(&mut stdin) {
                    result = Err(e);
                    break;
                }
            }
            drop(stdin);
            result
        } else {
            Ok(())
        };

        if let Err(write_err) = write_result {
            let _ = child.wait();
            return Err(VideoEncoderError::FfmpegFailed(
                -1,
                format!("Write failed: {}", write_err),
            ));
        }

        let status = child.wait()?;
        if status.success() {
            Ok(())
        } else {
            Err(VideoEncoderError::FfmpegFailed(
                status.code().unwrap_or(-1),
                "ffmpeg encoding failed".to_string(),
            ))
        }
    }
}

impl Default for Mp4Encoder {
    fn default() -> Self {
        Self::new()
    }
}

/// MP4 video encoder using NVIDIA NVENC hardware acceleration.
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

    /// Create a new NVENC encoder with the specified configuration.
    pub fn with_config(config: VideoEncoderConfig) -> Self {
        Self {
            config,
            ffmpeg_path: None,
            device_id: None,
        }
    }

    /// Set the GPU device ID for encoding.
    pub fn with_device(mut self, device_id: u32) -> Self {
        self.device_id = Some(device_id);
        self
    }

    /// Encode a buffer of video frames to an MP4 file.
    pub fn encode_buffer(
        &self,
        buffer: &VideoFrameBuffer,
        output_path: &Path,
    ) -> Result<(), VideoEncoderError> {
        if buffer.is_empty() {
            return Err(VideoEncoderError::NoFrames);
        }

        if !check_nvenc_available() {
            return Err(VideoEncoderError::FfmpegNotFound);
        }

        let (width, height) = buffer
            .dimensions()
            .ok_or_else(|| VideoEncoderError::InvalidFrameData("Invalid frame data".to_string()))?;

        let ffmpeg_path = self.ffmpeg_path.as_deref().unwrap_or(Path::new("ffmpeg"));

        let mut cmd = Command::new(ffmpeg_path);
        cmd.arg("-y")
            .arg("-hide_banner")
            .args(["-hwaccel", "cuda"])
            .args(["-hwaccel_output_format", "cuda"]);

        if let Some(device) = self.device_id {
            cmd.args(["-gpu", &device.to_string()]);
        }

        cmd.args(["-f", "rawvideo"])
            .args(["-pix_fmt", "rgb24"])
            .args(["-s", &format!("{}x{}", width, height)])
            .args(["-r", &self.config.fps.to_string()])
            .arg("-i")
            .arg("-")
            .args(["-c:v", "h264_nvenc"])
            .args(["-preset", "p4"])
            .args(["-tune", "ll"])
            .args(["-b:v", "5M"])
            .args(["-pix_fmt", "yuv420p"])
            .arg(output_path);

        let mut child = cmd
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|_| VideoEncoderError::FfmpegNotFound)?;

        let write_result = if let Some(mut stdin) = child.stdin.take() {
            let mut result = Ok(());
            for frame in &buffer.frames {
                if let Err(e) = stdin.write_all(frame.data()) {
                    result = Err(e);
                    break;
                }
            }
            drop(stdin);
            result
        } else {
            Ok(())
        };

        if let Err(write_err) = write_result {
            let _ = child.wait();
            return Err(VideoEncoderError::FfmpegFailed(
                -1,
                format!("NVENC write failed: {}", write_err),
            ));
        }

        let status = child.wait()?;
        if status.success() {
            Ok(())
        } else {
            Err(VideoEncoderError::FfmpegFailed(
                status.code().unwrap_or(-1),
                "NVENC encoding failed".to_string(),
            ))
        }
    }
}

impl Default for NvencEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// MP4 video encoder using Apple VideoToolbox hardware acceleration (macOS).
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

    /// Create a new VideoToolbox encoder with the specified configuration.
    pub fn with_config(config: VideoEncoderConfig) -> Self {
        Self {
            config,
            ffmpeg_path: None,
        }
    }

    /// Encode a buffer of video frames to an MP4 file.
    pub fn encode_buffer(
        &self,
        buffer: &VideoFrameBuffer,
        output_path: &Path,
    ) -> Result<(), VideoEncoderError> {
        if buffer.is_empty() {
            return Err(VideoEncoderError::NoFrames);
        }

        let (width, height) = buffer
            .dimensions()
            .ok_or_else(|| VideoEncoderError::InvalidFrameData("Invalid frame data".to_string()))?;

        let ffmpeg_path = self.ffmpeg_path.as_deref().unwrap_or(Path::new("ffmpeg"));

        let mut child = Command::new(ffmpeg_path)
            .arg("-y")
            .arg("-hide_banner")
            .args(["-hwaccel", "videotoolbox"])
            .args(["-pix_fmt", "videotoolbox_vlc"])
            .args(["-f", "rawvideo"])
            .args(["-pix_fmt", "rgb24"])
            .args(["-s", &format!("{}x{}", width, height)])
            .args(["-r", &self.config.fps.to_string()])
            .arg("-i")
            .arg("-")
            .args(["-c:v", "h264_videotoolbox"])
            .args(["-profile:v", "high"])
            .args(["-level", "3.1"])
            .args(["-q", "23"])
            .args(["-pix_fmt", "yuv420p"])
            .arg(output_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|_| VideoEncoderError::FfmpegNotFound)?;

        let write_result = if let Some(mut stdin) = child.stdin.take() {
            let mut result = Ok(());
            for frame in &buffer.frames {
                if let Err(e) = stdin.write_all(frame.data()) {
                    result = Err(e);
                    break;
                }
            }
            drop(stdin);
            result
        } else {
            Ok(())
        };

        if let Err(write_err) = write_result {
            let _ = child.wait();
            return Err(VideoEncoderError::FfmpegFailed(
                -1,
                format!("VideoToolbox write failed: {}", write_err),
            ));
        }

        let status = child.wait()?;
        if status.success() {
            Ok(())
        } else {
            Err(VideoEncoderError::FfmpegFailed(
                status.code().unwrap_or(-1),
                "VideoToolbox encoding failed".to_string(),
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

/// MKV encoder for 16-bit depth video using FFV1 codec.
pub struct DepthMkvEncoder {
    config: DepthEncoderConfig,
    ffmpeg_path: Option<PathBuf>,
}

impl DepthMkvEncoder {
    /// Create a new depth MKV encoder with default configuration.
    pub fn new() -> Self {
        Self {
            config: DepthEncoderConfig::default(),
            ffmpeg_path: None,
        }
    }

    /// Create a new depth MKV encoder with the specified configuration.
    pub fn with_config(config: DepthEncoderConfig) -> Self {
        Self {
            config,
            ffmpeg_path: None,
        }
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

    /// Encode a buffer of depth frames to an MKV file.
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
            .ok_or_else(|| VideoEncoderError::InvalidFrameData("Invalid frame data".to_string()))?;

        let ffmpeg_path = self.ffmpeg_path.as_deref().unwrap_or(Path::new("ffmpeg"));

        let mut child = Command::new(ffmpeg_path)
            .arg("-y")
            .arg("-f")
            .arg("rawvideo")
            .arg("-pix_fmt")
            .arg("gray16le")
            .arg("-s")
            .arg(format!("{}x{}", width, height))
            .arg("-r")
            .arg(self.config.fps.to_string())
            .arg("-i")
            .arg("-")
            .arg("-c:v")
            .arg(&self.config.codec)
            .arg("-level")
            .arg("3")
            .arg("-g")
            .arg("1")
            .arg(output_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|_| VideoEncoderError::FfmpegNotFound)?;

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

    /// Save depth frames as 16-bit PNG files (fallback if ffmpeg unavailable).
    pub fn save_as_png(
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

            let depth_bytes: Vec<u8> = depth_data.iter().flat_map(|v| v.to_le_bytes()).collect();

            writer.write_image_data(&depth_bytes).map_err(|_| {
                VideoEncoderError::Io(std::io::Error::other("PNG data write failed"))
            })?;

            paths.push(path);
        }

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncoderChoice {
    /// NVIDIA NVENC hardware encoder
    Nvenc,
    /// Apple VideoToolbox hardware encoder
    VideoToolbox,
    /// Rsmpeg native FFmpeg encoding
    RsmpegLibx264,
    /// FFmpeg CLI with libx264
    FfmpegLibx264,
}

impl EncoderChoice {
    /// Get human-readable name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Nvenc => "h264_nvenc",
            Self::VideoToolbox => "h264_videotoolbox",
            Self::RsmpegLibx264 => "libx264 (rsmpeg)",
            Self::FfmpegLibx264 => "libx264 (ffmpeg)",
        }
    }

    /// Get expected speedup factor vs FFmpeg CLI.
    pub fn speedup_factor(&self) -> f32 {
        match self {
            Self::Nvenc => 7.5,
            Self::VideoToolbox => 4.0,
            Self::RsmpegLibx264 => 2.5,
            Self::FfmpegLibx264 => 1.0,
        }
    }
}

/// Select the best available encoder.
///
/// Delegates to [`crate::media::video::hardware_config::detect_hardware_backend()`] as the
/// single source of truth for hardware detection.
pub fn select_best_encoder() -> EncoderChoice {
    use crate::media::video::hardware_config::{HardwareBackend, detect_hardware_backend};

    match detect_hardware_backend() {
        HardwareBackend::Nvenc => EncoderChoice::Nvenc,
        HardwareBackend::VideoToolbox => EncoderChoice::VideoToolbox,
        // QSV and VAAPI don't have EncoderChoice variants — fall through to software
        _ => {
            tracing::info!("Selected Rsmpeg native encoder (2-3x faster than FFmpeg CLI)");
            EncoderChoice::RsmpegLibx264
        }
    }
}

/// Check if specific encoder type is available.
pub fn is_encoder_available(encoder: EncoderChoice) -> bool {
    match encoder {
        EncoderChoice::Nvenc => check_nvenc_available(),
        EncoderChoice::VideoToolbox => check_videotoolbox_available(),
        EncoderChoice::RsmpegLibx264 => true,
        EncoderChoice::FfmpegLibx264 => std::process::Command::new("ffmpeg")
            .arg("-version")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false),
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
                    "  {}. {} - {}x speedup",
                    i + 1,
                    encoder.name(),
                    encoder.speedup_factor()
                )
            })
            .collect();

        tracing::info!(
            "=== Video Encoder Diagnostics ===\nAvailable encoders:\n{}\n\nSelected: {}",
            encoder_list.join("\n"),
            select_best_encoder().name()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encoder_choice_names() {
        assert_eq!(EncoderChoice::Nvenc.name(), "h264_nvenc");
        assert_eq!(EncoderChoice::VideoToolbox.name(), "h264_videotoolbox");
    }

    #[test]
    fn test_encoder_choice_speedup() {
        assert!(EncoderChoice::Nvenc.speedup_factor() > 5.0);
        assert!(EncoderChoice::VideoToolbox.speedup_factor() > 3.0);
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
            | EncoderChoice::FfmpegLibx264 => {}
        }
    }

    #[test]
    fn test_available_encoders() {
        let encoders = available_encoders();
        assert!(!encoders.is_empty());
        assert!(encoders.contains(&EncoderChoice::RsmpegLibx264));
    }

    // =============================================================================
    // Additional EncoderChoice Tests
    // =============================================================================

    #[test]
    fn test_encoder_choice_rsmpeg_name() {
        assert_eq!(EncoderChoice::RsmpegLibx264.name(), "libx264 (rsmpeg)");
    }

    #[test]
    fn test_encoder_choice_ffmpeg_name() {
        assert_eq!(EncoderChoice::FfmpegLibx264.name(), "libx264 (ffmpeg)");
    }

    #[test]
    fn test_encoder_choice_rsmpeg_speedup() {
        assert!(EncoderChoice::RsmpegLibx264.speedup_factor() > 1.0);
        assert!(EncoderChoice::RsmpegLibx264.speedup_factor() < 5.0);
    }

    #[test]
    fn test_encoder_choice_equality() {
        assert_eq!(EncoderChoice::Nvenc, EncoderChoice::Nvenc);
        assert_eq!(EncoderChoice::VideoToolbox, EncoderChoice::VideoToolbox);
        assert_ne!(EncoderChoice::Nvenc, EncoderChoice::VideoToolbox);
        assert_ne!(EncoderChoice::RsmpegLibx264, EncoderChoice::FfmpegLibx264);
    }

    #[test]
    fn test_encoder_choice_clone() {
        let encoder = EncoderChoice::Nvenc;
        let cloned = encoder; // Copy types don't need clone
        assert_eq!(encoder, cloned);
    }

    #[test]
    fn test_encoder_choice_debug() {
        let encoder = EncoderChoice::Nvenc;
        let debug_str = format!("{:?}", encoder);
        assert!(debug_str.contains("Nvenc"));
    }

    // =============================================================================
    // Mp4Encoder Tests
    // =============================================================================

    #[test]
    fn test_mp4_encoder_new() {
        let encoder = Mp4Encoder::new();
        assert_eq!(encoder.config.fps, 30);
        // Codec should use best available encoder
        assert!(matches!(
            encoder.config.codec.as_str(),
            "h264_videotoolbox" | "nvenc" | "libx264"
        ));
    }

    #[test]
    fn test_mp4_encoder_default() {
        let encoder = Mp4Encoder::default();
        assert_eq!(encoder.config.fps, 30);
    }

    #[test]
    fn test_mp4_encoder_with_config() {
        let config = VideoEncoderConfig::default().with_fps(60);
        let encoder = Mp4Encoder::with_config(config);
        assert_eq!(encoder.config.fps, 60);
    }

    #[test]
    fn test_mp4_encoder_custom_ffmpeg_path() {
        let encoder = Mp4Encoder::new().with_ffmpeg_path("/usr/local/bin/ffmpeg");
        assert_eq!(
            encoder.ffmpeg_path,
            Some(PathBuf::from("/usr/local/bin/ffmpeg"))
        );
    }

    // =============================================================================
    // NvencEncoder Tests
    // =============================================================================

    #[test]
    fn test_nvenc_encoder_new() {
        let encoder = NvencEncoder::new();
        assert_eq!(encoder.config.fps, 30);
        assert!(encoder.device_id.is_none());
    }

    #[test]
    fn test_nvenc_encoder_default() {
        let encoder = NvencEncoder::default();
        assert_eq!(encoder.config.fps, 30);
    }

    #[test]
    fn test_nvenc_encoder_with_config() {
        let config = VideoEncoderConfig::default().with_fps(120);
        let encoder = NvencEncoder::with_config(config);
        assert_eq!(encoder.config.fps, 120);
    }

    #[test]
    fn test_nvenc_encoder_with_device() {
        let encoder = NvencEncoder::new().with_device(1);
        assert_eq!(encoder.device_id, Some(1));
    }

    // =============================================================================
    // DepthMkvEncoder Tests
    // =============================================================================

    #[test]
    fn test_depth_mkv_encoder_new() {
        let encoder = DepthMkvEncoder::new();
        assert_eq!(encoder.config.fps, 30);
        assert_eq!(encoder.config.codec, "ffv1");
    }

    #[test]
    fn test_depth_mkv_encoder_default() {
        let encoder = DepthMkvEncoder::default();
        assert_eq!(encoder.config.fps, 30);
    }

    #[test]
    fn test_depth_mkv_encoder_with_config() {
        let config = DepthEncoderConfig {
            fps: 60,
            codec: "ffv1".to_string(),
            preset: "medium".to_string(),
        };
        let encoder = DepthMkvEncoder::with_config(config);
        assert_eq!(encoder.config.fps, 60);
        assert_eq!(encoder.config.preset, "medium");
    }

    // =============================================================================
    // Availability Tests
    // =============================================================================

    #[test]
    fn test_is_encoder_available_rsmpeg() {
        // RsmpegLibx264 is always available
        assert!(is_encoder_available(EncoderChoice::RsmpegLibx264));
    }

    #[test]
    fn test_check_videotoolbox_non_macos() {
        // On non-macOS, videotoolbox should be false
        #[cfg(not(target_os = "macos"))]
        assert!(!check_videotoolbox_available());
    }

    #[test]
    fn test_check_videotoolbox_macos() {
        // On macOS with ffmpeg, videotoolbox should be detected
        #[cfg(target_os = "macos")]
        {
            // check_videotoolbox_available now delegates to detect_hardware_backend,
            // so it depends on ffmpeg actually listing h264_videotoolbox
            let vt = check_videotoolbox_available();
            let backend = crate::media::video::hardware_config::detect_hardware_backend();
            let expected = matches!(
                backend,
                crate::media::video::hardware_config::HardwareBackend::VideoToolbox
            );
            assert_eq!(
                vt, expected,
                "check_videotoolbox_available must agree with detect_hardware_backend"
            );
        }
    }

    // =========================================================================
    // Unified detection consistency tests
    // =========================================================================

    #[test]
    fn test_select_best_encoder_consistent_with_detect_hardware_backend() {
        use crate::media::video::hardware_config::{HardwareBackend, detect_hardware_backend};

        let backend = detect_hardware_backend();
        let encoder = select_best_encoder();

        match backend {
            HardwareBackend::Nvenc => assert_eq!(encoder, EncoderChoice::Nvenc),
            HardwareBackend::VideoToolbox => assert_eq!(encoder, EncoderChoice::VideoToolbox),
            // QSV / VAAPI / None all map to software
            _ => assert_eq!(encoder, EncoderChoice::RsmpegLibx264),
        }
    }

    #[test]
    fn test_is_encoder_available_videotoolbox_consistent() {
        let via_is_available = is_encoder_available(EncoderChoice::VideoToolbox);
        let via_check = check_videotoolbox_available();
        assert_eq!(
            via_is_available, via_check,
            "is_encoder_available and check_videotoolbox_available must agree"
        );
    }

    #[test]
    fn test_is_encoder_available_nvenc_consistent() {
        let via_is_available = is_encoder_available(EncoderChoice::Nvenc);
        let via_check = check_nvenc_available();
        assert_eq!(
            via_is_available, via_check,
            "is_encoder_available and check_nvenc_available must agree"
        );
    }

    #[test]
    fn test_check_nvenc_delegates_to_detect_hardware_backend() {
        use crate::media::video::hardware_config::{HardwareBackend, detect_hardware_backend};
        let backend = detect_hardware_backend();
        let nvenc = check_nvenc_available();
        assert_eq!(
            nvenc,
            matches!(backend, HardwareBackend::Nvenc),
            "check_nvenc_available must agree with detect_hardware_backend"
        );
    }

    #[test]
    fn test_check_videotoolbox_delegates_to_detect_hardware_backend() {
        use crate::media::video::hardware_config::{HardwareBackend, detect_hardware_backend};
        let backend = detect_hardware_backend();
        let vt = check_videotoolbox_available();
        assert_eq!(
            vt,
            matches!(backend, HardwareBackend::VideoToolbox),
            "check_videotoolbox_available must agree with detect_hardware_backend"
        );
    }

    #[test]
    fn test_available_encoders_includes_videotoolbox_when_detected() {
        let encoders = available_encoders();
        let vt_detected = check_videotoolbox_available();
        assert_eq!(
            encoders.contains(&EncoderChoice::VideoToolbox),
            vt_detected,
            "available_encoders must include VideoToolbox iff it is detected"
        );
    }

    #[test]
    fn test_available_encoders_includes_best_encoder() {
        let best = select_best_encoder();
        let encoders = available_encoders();
        assert!(
            encoders.contains(&best),
            "available_encoders must contain the best encoder"
        );
    }

    #[test]
    fn test_select_best_encoder_is_deterministic() {
        let first = select_best_encoder();
        let second = select_best_encoder();
        assert_eq!(
            first, second,
            "consecutive calls must return the same encoder"
        );
    }
}
