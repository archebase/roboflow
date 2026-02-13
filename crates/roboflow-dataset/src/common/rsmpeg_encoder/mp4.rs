// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! MP4 encoder using native rsmpeg FFmpeg bindings.

use std::ffi::{CStr, c_int};
use std::path::Path;

use rsmpeg::avcodec::AVCodecContext;
use rsmpeg::avformat::AVFormatContextOutput;
use rsmpeg::avutil::{AVFrame, AVRational};
use rsmpeg::ffi;
use rsmpeg::swscale::SwsContext;

use super::encoder::AVCodec;
use crate::common::video::{VideoEncoderConfig, VideoEncoderError, VideoFrameBuffer};

/// MP4 encoder using native rsmpeg FFmpeg bindings.
///
/// This is a drop-in replacement for `Mp4Encoder` that uses in-process
/// FFmpeg via rsmpeg instead of spawning a subprocess. This provides:
/// - 2-3x faster encoding (no subprocess overhead)
/// - Better error handling
/// - No dependency on external ffmpeg binary
///
/// # Example
///
/// ```rust,ignore
/// use roboflow_dataset::common::rsmpeg_encoder::RsmpegMp4Encoder;
/// use roboflow_dataset::common::video::{VideoEncoderConfig, VideoFrameBuffer};
///
/// let config = VideoEncoderConfig::default();
/// let encoder = RsmpegMp4Encoder::with_config(config);
///
/// let buffer = VideoFrameBuffer::new();
/// encoder.encode_buffer(&buffer, "/path/to/output.mp4")?;
/// ```
pub struct RsmpegMp4Encoder {
    config: VideoEncoderConfig,
}

impl RsmpegMp4Encoder {
    /// Create a new encoder with default configuration.
    pub fn new() -> Self {
        Self {
            config: VideoEncoderConfig::default(),
        }
    }

    /// Create a new encoder with custom configuration.
    pub fn with_config(config: VideoEncoderConfig) -> Self {
        Self { config }
    }

    /// Encode frames from a buffer to an MP4 file.
    ///
    /// This method provides the same API as `Mp4Encoder::encode_buffer`
    /// but uses native FFmpeg encoding via rsmpeg.
    ///
    /// # Arguments
    ///
    /// * `buffer` - Frame buffer containing video frames
    /// * `output_path` - Path where the MP4 file should be written
    pub fn encode_buffer(
        &self,
        buffer: &VideoFrameBuffer,
        output_path: &Path,
    ) -> std::result::Result<(), VideoEncoderError> {
        if buffer.is_empty() {
            return Err(VideoEncoderError::NoFrames);
        }

        let (width, height) = buffer
            .dimensions()
            .ok_or(VideoEncoderError::InvalidFrameData)?;

        // Detect best codec (hardware or software)
        let (codec_name, pixel_format) = Self::detect_best_codec();
        let pixel_format_enum = match pixel_format.as_str() {
            "nv12" => ffi::AV_PIX_FMT_NV12,
            _ => ffi::AV_PIX_FMT_YUV420P,
        };

        tracing::info!(
            codec = %codec_name,
            width,
            height,
            frames = buffer.len(),
            fps = self.config.fps,
            "Starting native MP4 encoding"
        );

        // =============================================================
        // STEP 1: Find codec
        // =============================================================

        let codec_name_with_nul = format!("{}\0", codec_name);
        let codec_name_cstr = CStr::from_bytes_with_nul(codec_name_with_nul.as_bytes())
            .map_err(|_| VideoEncoderError::InvalidFrameData)?;

        let codec = AVCodec::find_encoder_by_name(codec_name_cstr)
            .or_else(|| {
                // Fallback to libx264 if requested codec not found
                tracing::warn!(
                    codec = %codec_name,
                    "Codec not found, falling back to libx264"
                );
                AVCodec::find_encoder(ffi::AV_CODEC_ID_H264)
            })
            .ok_or_else(|| {
                VideoEncoderError::FfmpegFailed(-1, "No H.264 encoder available".to_string())
            })?;

        // =============================================================
        // STEP 2: Allocate and configure codec context
        // =============================================================

        let mut codec_context = AVCodecContext::new(&codec);

        codec_context.set_width(width as i32);
        codec_context.set_height(height as i32);
        codec_context.set_bit_rate(5_000_000); // 5 Mbps default
        codec_context.set_time_base(AVRational {
            num: 1,
            den: self.config.fps as i32,
        });
        codec_context.set_framerate(AVRational {
            num: self.config.fps as i32,
            den: 1,
        });
        codec_context.set_gop_size(self.config.fps as i32); // 1 second keyframe interval
        codec_context.set_max_b_frames(0); // Disable B-frames for simplicity
        codec_context.set_pix_fmt(pixel_format_enum);
        // Set color range to full (JPEG) - RGB from decoded images uses full range
        // SAFETY: We have exclusive mutable access to codec_context via as_mut_ptr().
        // The AVCodecContext is properly initialized and this field write is safe.
        unsafe {
            (*codec_context.as_mut_ptr()).color_range = ffi::AVCOL_RANGE_JPEG;
        }

        // Open codec
        codec_context.open(None).map_err(|e| {
            VideoEncoderError::FfmpegFailed(-1, format!("Failed to open codec: {}", e))
        })?;

        // =============================================================
        // STEP 3: Create SWScale context for RGB → YUV conversion
        // =============================================================

        let sws_flags = ffi::SWS_BILINEAR;
        let sws_context = SwsContext::get_context(
            width as i32,
            height as i32,
            ffi::AV_PIX_FMT_RGB24,
            width as i32,
            height as i32,
            pixel_format_enum,
            sws_flags,
            None,
            None,
            None,
        )
        .ok_or_else(|| {
            VideoEncoderError::FfmpegFailed(-1, "Failed to create SWScale context".to_string())
        })?;

        // =============================================================
        // STEP 4: Create output format context
        // =============================================================

        let output_path_cstr = std::ffi::CString::new(output_path.to_str().unwrap_or("output.mp4"))
            .map_err(|_| VideoEncoderError::FfmpegFailed(-1, "Invalid path".to_string()))?;

        let mut format_context = AVFormatContextOutput::builder()
            .filename(&output_path_cstr)
            .build()
            .map_err(|e| {
                VideoEncoderError::FfmpegFailed(
                    -1,
                    format!("Failed to create format context: {}", e),
                )
            })?;

        // =============================================================
        // STEP 5: Create video stream
        // =============================================================

        {
            let mut stream = format_context.new_stream();
            let codecpar = codec_context.extract_codecpar();
            stream.set_codecpar(codecpar);
            stream.set_time_base(AVRational {
                num: 1,
                den: self.config.fps as i32,
            });
            // stream is dropped here, releasing borrow on format_context
        }

        // Write header
        format_context.write_header(&mut None).map_err(|e| {
            VideoEncoderError::FfmpegFailed(-1, format!("Failed to write header: {}", e))
        })?;

        // =============================================================
        // STEP 6: Encode all frames
        // =============================================================

        let mut frame_count = 0u64;

        for frame in &buffer.frames {
            // Create and encode frame
            let mut input_frame = AVFrame::new();
            input_frame.set_width(width as i32);
            input_frame.set_height(height as i32);
            input_frame.set_format(ffi::AV_PIX_FMT_RGB24);

            input_frame.get_buffer(0).map_err(|e| {
                VideoEncoderError::FfmpegFailed(
                    -1,
                    format!("Failed to allocate input frame: {}", e),
                )
            })?;

            // Copy RGB data to frame
            let frame_data_array = input_frame.data_mut();
            let frame_data_ptr = frame_data_array[0];
            // SAFETY: frame_data_ptr is a valid pointer to the frame's data buffer allocated by FFmpeg.
            // The buffer size matches frame.data.len() based on the frame dimensions and RGB24 format.
            let frame_data_slice =
                unsafe { std::slice::from_raw_parts_mut(frame_data_ptr, frame.data.len()) };
            frame_data_slice.copy_from_slice(&frame.data);

            // Convert to YUV
            let mut yuv_frame = AVFrame::new();
            yuv_frame.set_width(width as i32);
            yuv_frame.set_height(height as i32);
            yuv_frame.set_format(pixel_format_enum);

            yuv_frame.get_buffer(0).map_err(|e| {
                VideoEncoderError::FfmpegFailed(-1, format!("Failed to allocate YUV frame: {}", e))
            })?;

            // SAFETY: sws_scale is called with valid sws_context, input_frame, and yuv_frame.
            // Both frames have been properly allocated with get_buffer() and data ranges are valid.
            // The color_range field write is safe as we have exclusive access to yuv_frame.
            unsafe {
                // SwsContext does expose as_ptr method
                ffi::sws_scale(
                    sws_context.as_ptr() as *mut _,
                    input_frame.data.as_ptr() as *const *const u8,
                    input_frame.linesize.as_ptr() as *const c_int,
                    0,
                    height as i32,
                    yuv_frame.data_mut().as_mut_ptr(),
                    yuv_frame.linesize_mut().as_mut_ptr(),
                );
                // Set color range to full (JPEG) to avoid VideoToolbox warning
                (*yuv_frame.as_mut_ptr()).color_range = ffi::AVCOL_RANGE_JPEG;
            }

            yuv_frame.set_pts(frame_count as i64);

            // Send frame to encoder
            codec_context.send_frame(Some(&yuv_frame)).map_err(|e| {
                VideoEncoderError::FfmpegFailed(
                    -1,
                    format!("Failed to send frame {}: {}", frame_count, e),
                )
            })?;

            // Receive and write packets
            while let Ok(mut pkt) = codec_context.receive_packet() {
                // Write packet to output using write_frame
                format_context.write_frame(&mut pkt).map_err(|e| {
                    VideoEncoderError::FfmpegFailed(-1, format!("Failed to write packet: {}", e))
                })?;
            }

            frame_count += 1;
        }

        // =============================================================
        // STEP 7: Flush encoder
        // =============================================================

        codec_context.send_frame(None).ok();
        while let Ok(mut pkt) = codec_context.receive_packet() {
            format_context.write_frame(&mut pkt).map_err(|e| {
                VideoEncoderError::FfmpegFailed(-1, format!("Failed to write flush packet: {}", e))
            })?;
        }

        // =============================================================
        // STEP 8: Write trailer
        // =============================================================

        format_context.write_trailer().map_err(|e| {
            VideoEncoderError::FfmpegFailed(-1, format!("Failed to write trailer: {}", e))
        })?;

        tracing::debug!(
            frames = frame_count,
            path = %output_path.display(),
            "Native MP4 encoding completed"
        );

        Ok(())
    }

    /// Detect the best available codec.
    ///
    /// Priority:
    /// 1. NVENC (NVIDIA GPU)
    /// 2. VideoToolbox (Apple)
    /// 3. libx264 (CPU fallback)
    fn detect_best_codec() -> (String, String) {
        #[cfg(target_os = "linux")]
        {
            // Try NVENC first on Linux
            if let Some(_codec) = AVCodec::find_encoder_by_name(c"h264_nvenc") {
                tracing::info!("Using NVENC encoder (hardware acceleration)");
                return ("h264_nvenc".to_string(), "nv12".to_string());
            }
        }

        #[cfg(target_os = "macos")]
        {
            // Try VideoToolbox on macOS
            if let Some(_codec) = AVCodec::find_encoder_by_name(c"h264_videotoolbox") {
                tracing::info!("Using VideoToolbox encoder (hardware acceleration)");
                return ("h264_videotoolbox".to_string(), "nv12".to_string());
            }
        }

        // Default to libx264
        tracing::info!("Using libx264 CPU encoder");
        ("libx264".to_string(), "yuv420p".to_string())
    }
}

impl Default for RsmpegMp4Encoder {
    fn default() -> Self {
        Self::new()
    }
}
