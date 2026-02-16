// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Streaming MP4 encoder with direct-to-channel output.
//!
//! This module provides `StreamingMp4Encoder` which encodes video frames
//! to fragmented MP4 (fMP4) format and outputs encoded chunks via a channel.
//!
//! # Design
//!
//! - Single FFmpeg initialization per camera per episode
//! - Custom AVIO callback writes encoded data to channel
//! - Fragmented MP4 (fMP4) format for streaming compatibility
//! - No temp files - direct to S3 multipart upload

use std::ffi::{CStr, c_int};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};

use crate::config::VideoEncoderConfig;
use crate::frame::VideoEncoderError;
use crate::rsmpeg::*;

use rsmpeg::avutil::AVMem;
use rsmpeg::ffi;

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for streaming encoder.
#[derive(Debug, Clone)]
pub struct StreamingEncoderConfig {
    /// Video width in pixels
    pub width: u32,
    /// Video height in pixels
    pub height: u32,
    /// Frame rate (fps)
    pub fps: u32,
    /// Target bitrate (bps)
    pub bitrate: u64,
    /// Codec name (e.g., "libx264", "h264_videotoolbox", "h264_nvenc")
    pub codec: String,
    /// GOP size (keyframe interval in frames)
    pub gop_size: u32,
    /// Chunk size threshold before sending to channel (bytes)
    pub chunk_size: usize,
}

impl Default for StreamingEncoderConfig {
    fn default() -> Self {
        Self {
            width: 640,
            height: 480,
            fps: 30,
            bitrate: 5_000_000,
            codec: Self::detect_best_codec(),
            gop_size: 30,
            chunk_size: 256 * 1024, // 256KB chunks
        }
    }
}

impl StreamingEncoderConfig {
    /// Create a new configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create config from VideoEncoderConfig.
    pub fn from_video_config(config: &VideoEncoderConfig) -> Self {
        Self {
            fps: config.fps,
            codec: config.codec.clone(),
            ..Default::default()
        }
    }

    /// Set video dimensions.
    pub fn with_dimensions(mut self, width: u32, height: u32) -> Self {
        self.width = width;
        self.height = height;
        self
    }

    /// Set bitrate.
    pub fn with_bitrate(mut self, bitrate: u64) -> Self {
        self.bitrate = bitrate;
        self
    }

    /// Set codec.
    pub fn with_codec(mut self, codec: impl Into<String>) -> Self {
        self.codec = codec.into();
        self
    }

    /// Detect best available codec for the platform.
    pub fn detect_best_codec() -> String {
        #[cfg(target_os = "macos")]
        {
            if AVCodec::find_encoder_by_name(c"h264_videotoolbox").is_some() {
                return "h264_videotoolbox".to_string();
            }
        }

        #[cfg(target_os = "linux")]
        {
            if AVCodec::find_encoder_by_name(c"h264_nvenc").is_some() {
                return "h264_nvenc".to_string();
            }
        }

        "libx264".to_string()
    }
}

// =============================================================================
// Encoded Chunk
// =============================================================================

/// An encoded chunk from the streaming encoder.
#[derive(Debug, Clone)]
pub struct EncodedChunk {
    /// Chunk data (fMP4 fragment)
    pub data: Vec<u8>,
}

impl EncodedChunk {
    /// Create a new encoded chunk.
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    /// Get chunk size.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if chunk is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

// =============================================================================
// AVIO Write State (Shared between encoder and callback)
// =============================================================================

/// Shared state for AVIO write callback.
struct AvioWriteState {
    /// Channel sender for encoded chunks
    tx: Sender<EncodedChunk>,
    /// Accumulation buffer
    buffer: Vec<u8>,
    /// Minimum chunk size before sending
    chunk_size: usize,
    /// Total bytes written
    bytes_written: u64,
}

impl AvioWriteState {
    fn new(tx: Sender<EncodedChunk>, chunk_size: usize) -> Self {
        Self {
            tx,
            buffer: Vec::with_capacity(chunk_size * 2),
            chunk_size,
            bytes_written: 0,
        }
    }

    /// Write data to buffer, flushing to channel when threshold reached.
    fn write(&mut self, data: &[u8]) {
        tracing::trace!(
            data_len = data.len(),
            buffer_len = self.buffer.len(),
            bytes_written = self.bytes_written,
            "AvioWriteState::write called"
        );

        self.buffer.extend_from_slice(data);
        self.bytes_written += data.len() as u64;

        // Flush when buffer exceeds threshold
        while self.buffer.len() >= self.chunk_size {
            let chunk_data: Vec<u8> = self.buffer.drain(..self.chunk_size).collect();
            tracing::trace!(
                chunk_len = chunk_data.len(),
                "AvioWriteState::write: sending chunk to channel"
            );
            if self.tx.send(EncodedChunk::new(chunk_data)).is_err() {
                tracing::warn!("Channel disconnected, dropping encoded chunk");
            }
        }

        tracing::trace!("AvioWriteState::write completed");
    }

    /// Flush remaining buffer to channel.
    fn flush(&mut self) {
        if !self.buffer.is_empty() {
            let chunk_data = std::mem::take(&mut self.buffer);
            if self.tx.send(EncodedChunk::new(chunk_data)).is_err() {
                tracing::warn!("Channel disconnected during flush");
            }
        }
    }
}

// =============================================================================
// Streaming MP4 Encoder
// =============================================================================

use rsmpeg::avformat::{
    AVFormatContextOutput, AVIOContextContainer, AVIOContextCustom, WritePacketCallback,
};
use rsmpeg::avutil::AVDictionary;

/// Streaming MP4 encoder with channel output.
///
/// This encoder:
/// 1. Initializes FFmpeg once per episode
/// 2. Uses custom AVIO to output fMP4 chunks to a channel
/// 3. No temp files - direct streaming to upload
pub struct StreamingMp4Encoder {
    /// FFmpeg codec context
    codec_context: Option<AVCodecContext>,
    /// SWScale context for pixel format conversion
    sws_context: Option<SwsContext>,
    /// Output format context
    format_context: Option<AVFormatContextOutput>,
    /// Shared AVIO write state (wrapped for callback access)
    avio_state: Arc<Mutex<AvioWriteState>>,
    /// Frame counter for PTS
    frame_count: u64,
    /// Configuration
    config: StreamingEncoderConfig,
    /// Video dimensions (set from first frame)
    width: u32,
    height: u32,
    /// Whether the encoder is finalized
    finalized: bool,
    /// Pixel format for encoding
    pix_fmt: ffi::AVPixelFormat,
}

impl StreamingMp4Encoder {
    /// Create a new streaming encoder.
    pub fn new(
        config: StreamingEncoderConfig,
        chunk_tx: Sender<EncodedChunk>,
    ) -> Result<Self, VideoEncoderError> {
        let avio_state = Arc::new(Mutex::new(AvioWriteState::new(chunk_tx, config.chunk_size)));
        Ok(Self {
            codec_context: None,
            sws_context: None,
            format_context: None,
            avio_state,
            frame_count: 0,
            config,
            width: 0,
            height: 0,
            finalized: false,
            pix_fmt: ffi::AV_PIX_FMT_YUV420P,
        })
    }

    /// Create encoder with dimensions known upfront.
    pub fn with_dimensions(
        config: StreamingEncoderConfig,
        chunk_tx: Sender<EncodedChunk>,
        width: u32,
        height: u32,
    ) -> Result<Self, VideoEncoderError> {
        let mut encoder = Self::new(config, chunk_tx)?;
        encoder.width = width;
        encoder.height = height;
        encoder.initialize_encoder()?;
        Ok(encoder)
    }

    /// Create custom AVIO context with write callback.
    fn create_custom_avio(avio_state: Arc<Mutex<AvioWriteState>>) -> AVIOContextCustom {
        tracing::debug!("create_custom_avio: allocating AVIO buffer");

        let buffer_size = 4096;
        let buffer = AVMem::new(buffer_size);

        tracing::debug!("create_custom_avio: creating write callback");

        let write_callback: WritePacketCallback =
            Box::new(move |_data: &mut Vec<u8>, buf: &[u8]| {
                tracing::trace!(buf_len = buf.len(), "AVIO write_callback: called");
                if let Ok(mut state) = avio_state.lock() {
                    state.write(buf);
                } else {
                    tracing::warn!("AVIO write_callback: failed to lock state");
                }
                buf.len() as i32
            });

        tracing::debug!("create_custom_avio: allocating AVIO context");

        AVIOContextCustom::alloc_context(buffer, true, Vec::new(), None, Some(write_callback), None)
    }

    /// Initialize the FFmpeg encoder.
    fn initialize_encoder(&mut self) -> Result<(), VideoEncoderError> {
        if self.codec_context.is_some() {
            return Ok(());
        }

        let width = self.width as i32;
        let height = self.height as i32;

        if width == 0 || height == 0 {
            return Err(VideoEncoderError::InitializationError(
                "Cannot initialize encoder with zero dimensions".to_string(),
            ));
        }

        // Find codec
        let codec_name_with_nul = format!("{}\0", self.config.codec);
        let codec_name =
            CStr::from_bytes_with_nul(codec_name_with_nul.as_bytes()).map_err(|_| {
                VideoEncoderError::InitializationError("Invalid codec name".to_string())
            })?;

        let codec = AVCodec::find_encoder_by_name(codec_name)
            .or_else(|| {
                tracing::warn!(
                    codec = %self.config.codec,
                    "Codec not found, falling back to libx264"
                );
                AVCodec::find_encoder(ffi::AV_CODEC_ID_H264)
            })
            .ok_or_else(|| {
                VideoEncoderError::InitializationError("No H.264 encoder available".to_string())
            })?;

        tracing::info!(
            codec = codec.name().to_str().unwrap_or("unknown"),
            width,
            height,
            fps = self.config.fps,
            "Initializing streaming encoder"
        );

        // Determine pixel format
        self.pix_fmt =
            if self.config.codec.contains("nvenc") || self.config.codec.contains("videotoolbox") {
                ffi::AV_PIX_FMT_NV12
            } else {
                ffi::AV_PIX_FMT_YUV420P
            };

        // Allocate codec context
        let mut codec_context = AVCodecContext::new(&codec);

        codec_context.set_width(width);
        codec_context.set_height(height);
        codec_context.set_bit_rate(self.config.bitrate as i64);
        codec_context.set_time_base(AVRational {
            num: 1,
            den: self.config.fps as i32,
        });
        codec_context.set_framerate(AVRational {
            num: self.config.fps as i32,
            den: 1,
        });
        codec_context.set_gop_size(self.config.gop_size as i32);
        codec_context.set_max_b_frames(0);
        codec_context.set_pix_fmt(self.pix_fmt);

        unsafe {
            (*codec_context.as_mut_ptr()).color_range = ffi::AVCOL_RANGE_JPEG;
        }

        codec_context.open(None).map_err(|e| {
            VideoEncoderError::InitializationError(format!("Failed to open codec: {}", e))
        })?;

        // Create SWScale context
        let sws_context = SwsContext::get_context(
            width,
            height,
            ffi::AV_PIX_FMT_RGB24,
            width,
            height,
            self.pix_fmt,
            ffi::SWS_BILINEAR,
            None,
            None,
            None,
        );

        // Create format context with custom AVIO
        let avio_context = Self::create_custom_avio(Arc::clone(&self.avio_state));

        let mut format_context = AVFormatContextOutput::builder()
            .format_name(c"mp4")
            .io_context(AVIOContextContainer::Custom(avio_context))
            .build()
            .map_err(|e| {
                VideoEncoderError::InitializationError(format!(
                    "Failed to create format context: {}",
                    e
                ))
            })?;

        // Create video stream
        {
            let mut stream = format_context.new_stream();
            let codecpar = codec_context.extract_codecpar();
            stream.set_codecpar(codecpar);
            stream.set_time_base(AVRational {
                num: 1,
                den: self.config.fps as i32,
            });
        }

        // Write header with fMP4 movflags
        let mut options = Some(AVDictionary::new(
            c"movflags",
            c"frag_keyframe+empty_moov+default_base_moof",
            0,
        ));

        format_context.write_header(&mut options).map_err(|e| {
            VideoEncoderError::InitializationError(format!("Failed to write header: {}", e))
        })?;

        tracing::info!("StreamingMp4Encoder initialized successfully");

        self.codec_context = Some(codec_context);
        self.sws_context = sws_context;
        self.format_context = Some(format_context);

        Ok(())
    }

    /// Add a frame for encoding.
    pub fn add_frame(&mut self, rgb_data: &[u8]) -> Result<(), VideoEncoderError> {
        if self.finalized {
            return Err(VideoEncoderError::FinalizedError(
                "Cannot add frame to finalized encoder".to_string(),
            ));
        }

        // Lazy initialization on first frame
        if self.codec_context.is_none() {
            let pixel_count = rgb_data.len() / 3;
            let w = (pixel_count as f64).sqrt() as u32;
            let h = pixel_count / w as usize;

            if (w as usize) * h * 3 != rgb_data.len() {
                return Err(VideoEncoderError::InvalidFrameData(format!(
                    "Invalid RGB data size: {} bytes",
                    rgb_data.len()
                )));
            }

            self.width = w;
            self.height = h as u32;
            self.initialize_encoder()?;
        }

        let width = self.width as i32;
        let height = self.height as i32;

        let expected_size = (self.width as usize) * (self.height as usize) * 3;
        if rgb_data.len() != expected_size {
            return Err(VideoEncoderError::InvalidFrameData(format!(
                "RGB data size mismatch: got {} bytes, expected {} bytes for {}x{} RGB24",
                rgb_data.len(),
                expected_size,
                self.width,
                self.height
            )));
        }

        // Allocate input RGB frame
        let mut input_frame = AVFrame::new();
        input_frame.set_width(width);
        input_frame.set_height(height);
        input_frame.set_format(ffi::AV_PIX_FMT_RGB24);

        input_frame.get_buffer(0).map_err(|e| {
            VideoEncoderError::FrameError(format!("Failed to allocate input frame: {}", e))
        })?;

        let frame_data_array = input_frame.data_mut();
        let frame_data = frame_data_array[0];

        if frame_data.is_null() {
            return Err(VideoEncoderError::FrameError(
                "Frame data pointer is null".to_string(),
            ));
        }

        unsafe {
            let frame_data_slice = std::slice::from_raw_parts_mut(frame_data, rgb_data.len());
            frame_data_slice.copy_from_slice(rgb_data);
        }

        // Allocate YUV frame
        let mut yuv_frame = AVFrame::new();
        yuv_frame.set_width(width);
        yuv_frame.set_height(height);
        yuv_frame.set_format(self.pix_fmt);

        yuv_frame.get_buffer(0).map_err(|e| {
            VideoEncoderError::FrameError(format!("Failed to allocate YUV frame: {}", e))
        })?;

        // Perform pixel format conversion
        if let Some(ref sws) = self.sws_context {
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
            return Err(VideoEncoderError::FrameError(
                "SWScale context not initialized".to_string(),
            ));
        }

        unsafe {
            (*yuv_frame.as_mut_ptr()).color_range = ffi::AVCOL_RANGE_JPEG;
        }

        yuv_frame.set_pts(self.frame_count as i64);
        self.frame_count += 1;

        // Encode frame
        let codec_context = self.codec_context.as_mut().ok_or_else(|| {
            VideoEncoderError::FrameError("Codec context not initialized".to_string())
        })?;

        codec_context
            .send_frame(Some(&yuv_frame))
            .map_err(|e| VideoEncoderError::Encoding(format!("Failed to send frame: {}", e)))?;

        self.receive_and_write_packets()?;

        Ok(())
    }

    /// Receive encoded packets and write to format context.
    fn receive_and_write_packets(&mut self) -> Result<(), VideoEncoderError> {
        let codec_context = self.codec_context.as_mut().ok_or_else(|| {
            VideoEncoderError::Encoding("Codec context not initialized".to_string())
        })?;
        let format_context = self.format_context.as_mut().ok_or_else(|| {
            VideoEncoderError::Encoding("Format context not initialized".to_string())
        })?;

        loop {
            match codec_context.receive_packet() {
                Ok(mut pkt) => {
                    format_context.write_frame(&mut pkt).map_err(|e| {
                        VideoEncoderError::Encoding(format!("Failed to write packet: {}", e))
                    })?;
                }
                Err(RsmpegError::EncoderDrainError) | Err(RsmpegError::EncoderFlushedError) => {
                    break;
                }
                Err(e) => {
                    return Err(VideoEncoderError::Encoding(format!(
                        "Failed to receive packet: {}",
                        e
                    )));
                }
            }
        }

        Ok(())
    }

    /// Finalize encoding and flush remaining data.
    pub fn finalize(mut self) -> Result<(), VideoEncoderError> {
        if self.finalized {
            return Ok(());
        }

        self.finalized = true;

        if let Some(codec_context) = self.codec_context.as_mut() {
            let _ = codec_context.send_frame(None);
            self.receive_and_write_packets()?;
        }

        if let Some(format_context) = self.format_context.as_mut() {
            format_context.write_trailer().map_err(|e| {
                VideoEncoderError::FinalizationError(format!("Failed to write trailer: {}", e))
            })?;
        }

        if let Ok(mut state) = self.avio_state.lock() {
            state.flush();
        }

        tracing::info!(
            frames = self.frame_count,
            width = self.width,
            height = self.height,
            "StreamingMp4Encoder finalized"
        );

        Ok(())
    }

    /// Get the number of frames encoded.
    pub fn frame_count(&self) -> u64 {
        self.frame_count
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

impl Drop for StreamingMp4Encoder {
    fn drop(&mut self) {
        self.format_context = None;
        self.codec_context = None;
        self.sws_context = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = StreamingEncoderConfig::default();
        assert_eq!(config.fps, 30);
        assert_eq!(config.bitrate, 5_000_000);
    }

    #[test]
    fn test_encoded_chunk() {
        let chunk = EncodedChunk::new(vec![1, 2, 3, 4]);
        assert_eq!(chunk.len(), 4);
        assert!(!chunk.is_empty());
    }
}
