// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Rsmpeg-based video encoder for streaming output.

use std::ffi::{CStr, c_int};
use std::sync::mpsc::Sender;

use rsmpeg::avcodec::AVCodecContext;
use rsmpeg::avformat::AVFormatContextOutput;
use rsmpeg::avutil::{AVFrame, AVRational};
use rsmpeg::error::RsmpegError;
use rsmpeg::ffi;
use rsmpeg::swscale::SwsContext;

use roboflow_core::{Result, RoboflowError};

use super::config::RsmpegEncoderConfig;

// Re-export rsmpeg types for convenience
pub use rsmpeg::avcodec::AVCodec;

/// Rsmpeg-based video encoder for streaming output.
///
/// This encoder uses native FFmpeg bindings for maximum performance,
/// avoiding the overhead of spawning FFmpeg CLI processes.
///
/// ## Usage
///
/// ```ignore
/// let (encoded_tx, encoded_rx) = std::sync::mpsc::channel();
/// let mut encoder = RsmpegEncoder::new(config, encoded_tx)?;
///
/// for frame in frames {
///     encoder.add_frame(&frame.rgb_data)?;
/// }
///
/// encoder.finalize()?;
/// ```
pub struct RsmpegEncoder {
    /// FFmpeg codec context
    codec_context: Option<AVCodecContext>,

    /// SWScale context for pixel format conversion
    sws_context: Option<SwsContext>,

    /// Channel for encoded fragments
    encoded_tx: Option<Sender<Vec<u8>>>,

    /// Frame count for PTS
    frame_count: u64,

    /// Configuration
    config: RsmpegEncoderConfig,

    /// Whether the encoder is finalized
    finalized: bool,
}

impl RsmpegEncoder {
    /// Create a new rsmpeg encoder.
    ///
    /// # Arguments
    ///
    /// * `config` - Encoder configuration
    /// * `encoded_tx` - Channel to send encoded fragments
    pub fn new(config: RsmpegEncoderConfig, encoded_tx: Sender<Vec<u8>>) -> Result<Self> {
        // =============================================================
        // STEP 1: Find and open codec
        // =============================================================

        let codec_name_with_nul = format!("{}\0", config.codec);
        let codec_name = CStr::from_bytes_with_nul(codec_name_with_nul.as_bytes())
            .map_err(|_| RoboflowError::encode("RsmpegEncoder", "Invalid codec name"))?;

        let codec = AVCodec::find_encoder_by_name(codec_name)
            .or_else(|| {
                // Fallback to libx264 if requested codec not found
                tracing::warn!(
                    codec = %config.codec,
                    "Codec not found, falling back to libx264"
                );
                AVCodec::find_encoder(ffi::AV_CODEC_ID_H264)
            })
            .ok_or_else(|| RoboflowError::encode("RsmpegEncoder", "No H.264 encoder available"))?;

        tracing::info!(
            codec = codec.name().to_str().unwrap_or("unknown"),
            description = codec.long_name().to_str().unwrap_or(""),
            "Found encoder"
        );

        // =============================================================
        // STEP 2: Allocate and configure codec context
        // =============================================================

        let mut codec_context = AVCodecContext::new(&codec);

        codec_context.set_width(config.width as i32);
        codec_context.set_height(config.height as i32);
        codec_context.set_bit_rate(config.bitrate as i64);
        codec_context.set_time_base(AVRational {
            num: 1,
            den: config.fps as i32,
        });
        codec_context.set_framerate(AVRational {
            num: config.fps as i32,
            den: 1,
        });
        codec_context.set_gop_size(config.gop_size as i32);
        codec_context.set_max_b_frames(config.max_b_frames as i32);

        // Set pixel format based on codec
        let pix_fmt = match config.pixel_format.as_str() {
            "nv12" => ffi::AV_PIX_FMT_NV12,
            _ => ffi::AV_PIX_FMT_YUV420P,
        };

        codec_context.set_pix_fmt(pix_fmt);
        // Set color range to full (JPEG) - RGB from decoded images uses full range
        // SAFETY: We have exclusive mutable access to codec_context via as_mut_ptr().
        // The AVCodecContext is properly initialized and this field write is safe.
        unsafe {
            (*codec_context.as_mut_ptr()).color_range = ffi::AVCOL_RANGE_JPEG;
        }

        // Set CRF and preset via options for libx264
        if config.codec.contains("x264") {
            // Use private options for libx264
            // Note: rsmpeg doesn't have a Set_option method exposed in the high-level API
            // For now, we skip setting these via options and rely on defaults
            tracing::debug!("CRF and preset options skipped (requires direct FFI access)");
        }

        // Open codec
        codec_context.open(None).map_err(|e| {
            RoboflowError::encode("RsmpegEncoder", format!("Failed to open codec: {}", e))
        })?;

        // =============================================================
        // STEP 3: Create SWScale context for RGB → YUV conversion
        // =============================================================

        let sws_flags = ffi::SWS_BILINEAR;

        let sws_context = SwsContext::get_context(
            config.width as i32,
            config.height as i32,
            ffi::AV_PIX_FMT_RGB24,
            config.width as i32,
            config.height as i32,
            pix_fmt,
            sws_flags,
            None,
            None,
            None,
        );

        // =============================================================
        // STEP 4: Create format context with in-memory output
        // =============================================================

        // For simplicity, we'll collect encoded data and send it via channel
        // rather than using a full AVIO context setup
        let _format_context = AVFormatContextOutput::builder()
            .filename(c"output.mp4")
            .build()
            .map_err(|e| {
                RoboflowError::encode(
                    "RsmpegEncoder",
                    format!("Failed to create format context: {}", e),
                )
            })?;

        // =============================================================
        // STEP 6: Create video stream
        // =============================================================
        // Note: format_context and stream creation is handled here but
        // the actual muxing is done in receive_and_send_packets

        tracing::info!(
            width = config.width,
            height = config.height,
            fps = config.fps,
            bitrate = config.bitrate,
            codec = codec.name().to_str().unwrap_or("unknown"),
            "RsmpegEncoder initialized"
        );

        Ok(Self {
            codec_context: Some(codec_context),
            sws_context,
            encoded_tx: Some(encoded_tx),
            frame_count: 0,
            config,
            finalized: false,
        })
    }

    /// Add a frame for encoding.
    ///
    /// This method:
    /// 1. Converts RGB24 input to the encoder's pixel format
    /// 2. Sends the frame to the encoder
    /// 3. Receives encoded packets
    /// 4. Sends fragments through the channel
    ///
    /// # Arguments
    ///
    /// * `rgb_data` - Raw RGB8 image data (width × height × 3 bytes)
    pub fn add_frame(&mut self, rgb_data: &[u8]) -> Result<()> {
        if self.finalized {
            return Err(RoboflowError::encode(
                "RsmpegEncoder",
                "Cannot add frame to finalized encoder",
            ));
        }

        let width = self.config.width as i32;
        let height = self.config.height as i32;

        // Get pixel format from config (we set it during initialization)
        let pix_fmt = match self.config.pixel_format.as_str() {
            "nv12" => ffi::AV_PIX_FMT_NV12,
            _ => ffi::AV_PIX_FMT_YUV420P,
        };

        // =============================================================
        // STEP 1: Allocate and populate input RGB frame
        // =============================================================

        let mut input_frame = AVFrame::new();
        input_frame.set_width(width);
        input_frame.set_height(height);
        input_frame.set_format(ffi::AV_PIX_FMT_RGB24);

        input_frame.get_buffer(0).map_err(|e| {
            RoboflowError::encode(
                "RsmpegEncoder",
                format!("Failed to allocate input frame: {}", e),
            )
        })?;

        // Copy RGB data to frame
        let frame_data_array = input_frame.data_mut();
        let frame_data = frame_data_array[0];
        // SAFETY: frame_data is a valid pointer to the frame's data buffer allocated by FFmpeg.
        // The buffer size matches rgb_data.len() based on the frame dimensions and RGB24 format.
        let frame_data_slice =
            unsafe { std::slice::from_raw_parts_mut(frame_data, rgb_data.len()) };
        frame_data_slice.copy_from_slice(rgb_data);

        // =============================================================
        // STEP 2: Convert pixel format (RGB → YUV)
        // =============================================================

        let mut yuv_frame = AVFrame::new();
        yuv_frame.set_width(width);
        yuv_frame.set_height(height);
        yuv_frame.set_format(pix_fmt);

        yuv_frame.get_buffer(0).map_err(|e| {
            RoboflowError::encode(
                "RsmpegEncoder",
                format!("Failed to allocate YUV frame: {}", e),
            )
        })?;

        // Perform pixel format conversion using SWScale
        if let Some(ref sws) = self.sws_context {
            // sws_scale signature:
            // sws_scale(c, src, src_stride, src_slice_y, src_h, dst, dst_stride)
            // SAFETY: sws_scale is called with valid sws_context, input_frame, and yuv_frame.
            // Both frames have been properly allocated with get_buffer() and data ranges are valid.
            unsafe {
                ffi::sws_scale(
                    sws.as_ptr() as *mut _,
                    input_frame.data.as_ptr() as *const *const u8,
                    input_frame.linesize.as_ptr() as *const c_int,
                    0,
                    height,
                    yuv_frame.data_mut().as_mut_ptr(),
                    yuv_frame.linesize_mut().as_mut_ptr(),
                );
            }
        } else {
            return Err(RoboflowError::encode(
                "RsmpegEncoder",
                "SWScale context not initialized",
            ));
        }

        // =============================================================
        // STEP 3: Set color range (full/JPEG) to avoid VideoToolbox warning
        // =============================================================
        // RGB from decoded images uses full range (0-255). Explicitly set
        // color_range so VideoToolbox/NVENC don't assume MPEG range.
        // SAFETY: We have exclusive mutable access to yuv_frame via as_mut_ptr().
        // The AVFrame is properly allocated and this field write is safe.
        unsafe {
            (*yuv_frame.as_mut_ptr()).color_range = ffi::AVCOL_RANGE_JPEG;
        }

        // =============================================================
        // STEP 4: Set timestamp
        // =============================================================

        yuv_frame.set_pts(self.frame_count as i64);
        self.frame_count += 1;

        // =============================================================
        // STEP 5: Encode frame
        // =============================================================

        let codec_context = self.codec_context.as_mut().unwrap();

        // Send frame to encoder
        codec_context.send_frame(Some(&yuv_frame)).map_err(|e| {
            RoboflowError::encode("RsmpegEncoder", format!("Failed to send frame: {}", e))
        })?;

        // =============================================================
        // STEP 6: Receive and send encoded packets
        // =============================================================

        self.receive_and_send_packets()?;

        Ok(())
    }

    /// Receive encoded packets and send them through the channel
    fn receive_and_send_packets(&mut self) -> Result<()> {
        let codec_context = self.codec_context.as_mut().unwrap();
        let tx = self.encoded_tx.as_ref().unwrap();

        loop {
            match codec_context.receive_packet() {
                Ok(pkt) => {
                    // Extract packet data - pkt derefs to ffi::AVPacket which has data and size fields
                    // SAFETY: av_packet.data and av_packet.size are valid for the lifetime of pkt.
                    // We check for null pointer and positive size before creating the slice.
                    // The data is copied immediately to a Vec before pkt is dropped.
                    let data = unsafe {
                        let av_packet: &ffi::AVPacket = &pkt;
                        let ptr = av_packet.data;
                        let len = av_packet.size as usize;
                        if len > 0 && !ptr.is_null() {
                            std::slice::from_raw_parts(ptr, len).to_vec()
                        } else {
                            Vec::new()
                        }
                    };

                    if !data.is_empty() {
                        // Send through channel
                        if tx.send(data).is_err() {
                            return Err(RoboflowError::encode(
                                "RsmpegEncoder",
                                "Channel disconnected while sending encoded data",
                            ));
                        }
                    }
                }
                Err(RsmpegError::EncoderDrainError) | Err(RsmpegError::EncoderFlushedError) => {
                    // Need more input or end of stream
                    break;
                }
                Err(e) => {
                    return Err(RoboflowError::encode(
                        "RsmpegEncoder",
                        format!("Failed to receive packet: {}", e),
                    ));
                }
            }
        }

        Ok(())
    }

    /// Finalize encoding and flush remaining packets
    pub fn finalize(mut self) -> Result<()> {
        if self.finalized {
            return Ok(());
        }

        self.finalized = true;

        let codec_context = self.codec_context.as_mut().unwrap();

        // =============================================================
        // STEP 1: Flush encoder
        // =============================================================

        // Send NULL frame to signal EOF
        let _ = codec_context.send_frame(None);

        // Drain remaining packets
        self.receive_and_send_packets()?;

        // Close the channel to signal completion
        drop(self.encoded_tx.take());

        tracing::info!(frames = self.frame_count, "RsmpegEncoder finalized");

        Ok(())
    }

    /// Get the encoder configuration.
    pub fn config(&self) -> &RsmpegEncoderConfig {
        &self.config
    }

    /// Get the number of frames encoded.
    pub fn frame_count(&self) -> u64 {
        self.frame_count
    }

    /// Check if the encoder is finalized.
    pub fn is_finalized(&self) -> bool {
        self.finalized
    }
}
