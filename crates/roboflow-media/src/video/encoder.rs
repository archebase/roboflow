// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Unified video encoder with pluggable output modes.
//!
//! This module provides a single entry point for all video encoding in roboflow-dataset.
//! It consolidates the various encoder implementations into one unified architecture.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                      VideoEncoder (Single Entry)                │
//! ├─────────────────────────────────────────────────────────────────┤
//! │                                                                 │
//! │   RGB Data → SIMD Convert → H.264 Encoder → Output             │
//! │                                                                 │
//! │   Output Modes:                                                 │
//! │   - Channel: Stream fMP4 chunks to channel                     │
//! │   - File: Write regular MP4 to local file                      │
//! │                                                                 │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example - File Output
//!
//! ```ignore
//! use roboflow_dataset::media::video::{
//!     VideoEncoder, VideoEncoderConfig, OutputConfig,
//! };
//!
//! let config = VideoEncoderConfig::default();
//! let output = OutputConfig::file("/path/to/output.mp4");
//! let mut encoder = VideoEncoder::new(config, output)?;
//!
//! for frame in frames {
//!     encoder.encode_frame(&frame.rgb_data, 640, 480)?;
//! }
//!
//! let result = encoder.finalize()?;
//! ```
//!
//! # Example - Channel Output (Streaming)
//!
//! ```ignore
//! let (tx, rx) = std::sync::mpsc::channel();
//! let output = OutputConfig::channel(tx, 256 * 1024);
//! let mut encoder = VideoEncoder::new(config, output)?;
//!
//! for frame in frames {
//!     encoder.encode_frame(&frame.rgb_data, 640, 480)?;
//! }
//!
//! encoder.finalize()?;
//! // Read chunks from rx
//! ```

use std::ffi::c_int;
use std::path::PathBuf;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};

use roboflow_core::{Result, RoboflowError};
use rsmpeg::avformat::{
    AVFormatContextOutput, AVIOContextContainer, AVIOContextCustom, WritePacketCallback,
};
use rsmpeg::avutil::{AVDictionary, AVFrame, AVMem, AVRational};
use rsmpeg::ffi;

use super::codec::{self, CodecContext, CodecParams, set_full_color_range_frame};
use super::config::VideoEncoderConfig;
use super::frame::{VideoEncoderError, VideoFrame, VideoFrameBuffer};
use super::rsmpeg::{AVCodecContext, RsmpegError, SwsContext};

// =============================================================================
// Encoded Chunk
// =============================================================================

/// An encoded chunk from the streaming encoder.
#[derive(Debug, Clone)]
pub struct EncodedChunk {
    /// Chunk data (fMP4 fragment for streaming, or full MP4 for file)
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
// Output Configuration
// =============================================================================

/// Output configuration for video encoding.
#[derive(Debug)]
pub enum OutputConfig {
    /// Stream fMP4 chunks via channel.
    Channel {
        /// Channel sender for encoded chunks
        tx: Sender<EncodedChunk>,
        /// Minimum chunk size before sending to channel (bytes)
        chunk_size: usize,
    },

    /// Write regular MP4 to local file.
    File {
        /// Output file path
        path: PathBuf,
    },
}

impl OutputConfig {
    /// Create a channel output configuration.
    pub fn channel(tx: Sender<EncodedChunk>, chunk_size: usize) -> Self {
        Self::Channel { tx, chunk_size }
    }

    /// Create a file output configuration.
    pub fn file(path: impl Into<PathBuf>) -> Self {
        Self::File { path: path.into() }
    }
}

// =============================================================================
// Encoding Result
// =============================================================================

/// Result from video encoding finalization.
#[derive(Debug, Clone)]
pub struct EncodingResult {
    /// Output file path (if file output)
    pub output_path: Option<PathBuf>,
    /// Number of frames encoded
    pub frames_encoded: u64,
    /// Total bytes written
    pub bytes_written: u64,
    /// Video dimensions (width, height)
    pub dimensions: (u32, u32),
    /// Codec used
    pub codec: String,
}

// =============================================================================
// AVIO Write State (for channel output)
// =============================================================================

/// Shared state for AVIO write callback (streaming mode).
struct AvioWriteState {
    tx: Sender<EncodedChunk>,
    buffer: Vec<u8>,
    chunk_size: usize,
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

    fn write(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
        self.bytes_written += data.len() as u64;

        while self.buffer.len() >= self.chunk_size {
            let chunk_data: Vec<u8> = self.buffer.drain(..self.chunk_size).collect();
            if self.tx.send(EncodedChunk::new(chunk_data)).is_err() {
                tracing::warn!("Channel disconnected, dropping encoded chunk");
            }
        }
    }

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
// Unified Video Encoder
// =============================================================================

/// Single unified video encoder with multiple output modes.
///
/// This is the primary entry point for video encoding in roboflow-dataset.
/// It supports:
/// - Automatic hardware detection (NVENC, VideoToolbox, libx264)
/// - Channel output (streaming fMP4)
/// - File output (regular MP4)
///
/// # Thread Safety
///
/// `VideoEncoder` is NOT thread-safe. Use one encoder per thread/camera.
pub struct VideoEncoder {
    /// Encoder configuration
    config: VideoEncoderConfig,
    /// Output configuration
    output: OutputConfig,
    /// Video dimensions
    width: u32,
    height: u32,
    /// FFmpeg codec context (initialized on first frame)
    codec_context: Option<AVCodecContext>,
    /// SWScale context for pixel format conversion
    sws_context: Option<SwsContext>,
    /// Output format context (for streaming mode)
    format_context: Option<AVFormatContextOutput>,
    /// AVIO state (for streaming mode)
    avio_state: Option<Arc<Mutex<AvioWriteState>>>,
    /// Frame buffer (for file mode)
    frame_buffer: Option<VideoFrameBuffer>,
    /// Frame counter
    frame_count: u64,
    /// Total bytes written
    bytes_written: u64,
    /// Whether encoding is finalized
    finalized: bool,
    /// Pixel format for encoding
    pix_fmt: i32,
}

impl VideoEncoder {
    /// Create a new unified video encoder.
    pub fn new(config: VideoEncoderConfig, output: OutputConfig) -> Result<Self> {
        let frame_buffer = match &output {
            OutputConfig::File { .. } => Some(VideoFrameBuffer::new()),
            OutputConfig::Channel { .. } => None,
        };

        Ok(Self {
            config,
            output,
            width: 0,
            height: 0,
            codec_context: None,
            sws_context: None,
            format_context: None,
            avio_state: None,
            frame_buffer,
            frame_count: 0,
            bytes_written: 0,
            finalized: false,
            pix_fmt: ffi::AV_PIX_FMT_YUV420P,
        })
    }

    /// Create encoder with known dimensions.
    pub fn with_dimensions(
        config: VideoEncoderConfig,
        output: OutputConfig,
        width: u32,
        height: u32,
    ) -> Result<Self> {
        let mut encoder = Self::new(config, output)?;
        encoder.width = width;
        encoder.height = height;
        Ok(encoder)
    }

    /// Encode a single frame.
    pub fn encode_frame(&mut self, rgb_data: &[u8], width: u32, height: u32) -> Result<()> {
        if self.finalized {
            return Err(RoboflowError::encode(
                "VideoEncoder",
                "Cannot encode frame after finalization",
            ));
        }

        // Validate
        if width == 0 || height == 0 {
            return Err(RoboflowError::encode(
                "VideoEncoder",
                "Frame dimensions cannot be zero",
            ));
        }

        let expected_size = (width as usize) * (height as usize) * 3;
        if rgb_data.len() != expected_size {
            return Err(RoboflowError::encode(
                "VideoEncoder",
                format!(
                    "RGB data size mismatch: got {} bytes, expected {} bytes for {}x{}",
                    rgb_data.len(),
                    expected_size,
                    width,
                    height
                ),
            ));
        }

        // Initialize on first frame
        if self.width == 0 {
            self.width = width;
            self.height = height;
            self.initialize_encoder()?;
        }

        // Validate dimensions
        if width != self.width || height != self.height {
            return Err(RoboflowError::encode(
                "VideoEncoder",
                format!(
                    "Frame dimension mismatch: expected {}x{}, got {}x{}",
                    self.width, self.height, width, height
                ),
            ));
        }

        match &self.output {
            OutputConfig::Channel { .. } => {
                self.encode_frame_streaming(rgb_data)?;
            }
            OutputConfig::File { .. } => {
                self.buffer_frame(rgb_data)?;
            }
        }

        self.frame_count += 1;
        Ok(())
    }

    /// Initialize the encoder based on output mode.
    fn initialize_encoder(&mut self) -> Result<()> {
        let pix_fmt = codec::pixel_format_for_codec(&self.config.codec);
        self.pix_fmt = pix_fmt;

        let codec_ctx = CodecContext::new(
            &self.config.codec,
            pix_fmt,
            &CodecParams {
                width: self.width,
                height: self.height,
                fps: self.config.fps,
                bitrate: 5_000_000,
                gop_size: self.config.fps,
                max_b_frames: 0,
            },
        )
        .map_err(|e| RoboflowError::encode("VideoEncoder", e))?;

        self.codec_context = Some(codec_ctx.codec_ctx);
        self.sws_context = codec_ctx.sws_ctx;
        self.pix_fmt = codec_ctx.pixel_format;

        tracing::info!(
            codec = codec_ctx.codec_name,
            width = self.width,
            height = self.height,
            fps = self.config.fps,
            "VideoEncoder initialized"
        );

        // For streaming mode, also set up format context
        if let OutputConfig::Channel { tx, chunk_size } = &self.output {
            let avio_state = Arc::new(Mutex::new(AvioWriteState::new(tx.clone(), *chunk_size)));
            self.avio_state = Some(Arc::clone(&avio_state));

            let avio_context = Self::create_custom_avio(avio_state);

            let mut format_context = AVFormatContextOutput::builder()
                .format_name(c"mp4")
                .io_context(AVIOContextContainer::Custom(avio_context))
                .build()
                .map_err(|e| {
                    RoboflowError::encode(
                        "VideoEncoder",
                        format!("Failed to create format context: {}", e),
                    )
                })?;

            // Create video stream
            {
                let mut stream = format_context.new_stream();
                let codecpar = self.codec_context.as_ref().unwrap().extract_codecpar();
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
                RoboflowError::encode("VideoEncoder", format!("Failed to write header: {}", e))
            })?;

            self.format_context = Some(format_context);
        }

        Ok(())
    }

    /// Create custom AVIO context for channel output.
    fn create_custom_avio(avio_state: Arc<Mutex<AvioWriteState>>) -> AVIOContextCustom {
        let buffer = AVMem::new(4096);

        let write_callback: WritePacketCallback =
            Box::new(move |_data: &mut Vec<u8>, buf: &[u8]| {
                if let Ok(mut state) = avio_state.lock() {
                    state.write(buf);
                }
                buf.len() as i32
            });

        AVIOContextCustom::alloc_context(buffer, true, Vec::new(), None, Some(write_callback), None)
    }

    /// Encode frame for streaming output.
    fn encode_frame_streaming(&mut self, rgb_data: &[u8]) -> Result<()> {
        let width = self.width as i32;
        let height = self.height as i32;

        // Allocate input RGB frame
        let mut input_frame = AVFrame::new();
        input_frame.set_width(width);
        input_frame.set_height(height);
        input_frame.set_format(ffi::AV_PIX_FMT_RGB24);
        input_frame.get_buffer(0).map_err(|e| {
            RoboflowError::encode(
                "VideoEncoder",
                format!("Failed to allocate input frame: {}", e),
            )
        })?;

        let frame_data_array = input_frame.data_mut();
        let frame_data = frame_data_array[0];
        if frame_data.is_null() {
            return Err(RoboflowError::encode(
                "VideoEncoder",
                "Frame data pointer is null",
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
            RoboflowError::encode(
                "VideoEncoder",
                format!("Failed to allocate YUV frame: {}", e),
            )
        })?;

        // Convert pixel format
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
        }

        set_full_color_range_frame(&mut yuv_frame);
        yuv_frame.set_pts(self.frame_count as i64);

        // Encode
        let codec_context = self.codec_context.as_mut().ok_or_else(|| {
            RoboflowError::encode("VideoEncoder", "Codec context not initialized")
        })?;

        codec_context.send_frame(Some(&yuv_frame)).map_err(|e| {
            RoboflowError::encode("VideoEncoder", format!("Failed to send frame: {}", e))
        })?;

        self.receive_and_write_packets()?;

        Ok(())
    }

    /// Receive encoded packets and write to format context.
    fn receive_and_write_packets(&mut self) -> Result<()> {
        let codec_context = self.codec_context.as_mut().ok_or_else(|| {
            RoboflowError::encode("VideoEncoder", "Codec context not initialized")
        })?;
        let format_context = self.format_context.as_mut().ok_or_else(|| {
            RoboflowError::encode("VideoEncoder", "Format context not initialized")
        })?;

        loop {
            match codec_context.receive_packet() {
                Ok(mut pkt) => {
                    if pkt.duration == 0 {
                        pkt.set_duration(1);
                    }
                    format_context.write_frame(&mut pkt).map_err(|e| {
                        RoboflowError::encode(
                            "VideoEncoder",
                            format!("Failed to write packet: {}", e),
                        )
                    })?;
                }
                Err(RsmpegError::EncoderDrainError) | Err(RsmpegError::EncoderFlushedError) => {
                    break;
                }
                Err(e) => {
                    return Err(RoboflowError::encode(
                        "VideoEncoder",
                        format!("Failed to receive packet: {}", e),
                    ));
                }
            }
        }

        Ok(())
    }

    /// Buffer frame for file output (encode on finalize).
    fn buffer_frame(&mut self, rgb_data: &[u8]) -> Result<()> {
        let frame = VideoFrame::new(self.width, self.height, rgb_data.to_vec());
        self.frame_buffer
            .as_mut()
            .ok_or_else(|| RoboflowError::encode("VideoEncoder", "Frame buffer not initialized"))?
            .add_frame(frame)
            .map_err(|e| {
                RoboflowError::encode("VideoEncoder", format!("Failed to add frame: {}", e))
            })?;
        Ok(())
    }

    /// Finalize encoding and get results.
    pub fn finalize(mut self) -> Result<EncodingResult> {
        if self.finalized {
            return Err(RoboflowError::encode(
                "VideoEncoder",
                "Encoder already finalized",
            ));
        }

        self.finalized = true;

        let output_path = match &self.output {
            OutputConfig::File { path } => Some(path.clone()),
            OutputConfig::Channel { .. } => None,
        };

        match &self.output {
            OutputConfig::Channel { .. } => {
                self.finalize_streaming()?;
            }
            OutputConfig::File { path } => {
                let path = path.clone();
                self.finalize_file(&path)?;
            }
        }

        tracing::info!(
            frames = self.frame_count,
            width = self.width,
            height = self.height,
            codec = %self.config.codec,
            "VideoEncoder finalized"
        );

        Ok(EncodingResult {
            output_path,
            frames_encoded: self.frame_count,
            bytes_written: self.bytes_written,
            dimensions: (self.width, self.height),
            codec: self.config.codec.clone(),
        })
    }

    /// Finalize streaming output.
    fn finalize_streaming(&mut self) -> Result<()> {
        if let Some(codec_context) = self.codec_context.as_mut() {
            let _ = codec_context.send_frame(None);
            self.receive_and_write_packets()?;
        }

        if let Some(format_context) = self.format_context.as_mut() {
            format_context.write_trailer().map_err(|e| {
                RoboflowError::encode("VideoEncoder", format!("Failed to write trailer: {}", e))
            })?;
        }

        if let Some(avio_state) = self.avio_state.as_ref()
            && let Ok(mut state) = avio_state.lock()
        {
            state.flush();
            self.bytes_written = state.bytes_written;
        }

        Ok(())
    }

    /// Finalize file output using RsmpegMp4Encoder logic.
    fn finalize_file(&mut self, path: &PathBuf) -> Result<()> {
        let buffer = self.frame_buffer.take().unwrap_or_default();

        if buffer.is_empty() {
            tracing::debug!("No frames to encode");
            return Ok(());
        }

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                RoboflowError::encode(
                    "VideoEncoder",
                    format!("Failed to create output directory: {}", e),
                )
            })?;
        }

        let (width, height) = buffer
            .dimensions()
            .ok_or_else(|| RoboflowError::encode("VideoEncoder", "Invalid frame data"))?;

        let pix_fmt = codec::resolve_pixel_format(&self.config.pixel_format);

        // Create codec context
        let codec_ctx = CodecContext::new(
            &self.config.codec,
            pix_fmt,
            &CodecParams {
                width,
                height,
                fps: self.config.fps,
                bitrate: 5_000_000,
                gop_size: self.config.fps,
                max_b_frames: 0,
            },
        )
        .map_err(|e| RoboflowError::encode("VideoEncoder", e))?;

        let mut codec_context = codec_ctx.codec_ctx;
        let sws_context = codec_ctx.sws_ctx.ok_or_else(|| {
            RoboflowError::encode("VideoEncoder", "Failed to create SWScale context")
        })?;

        // Create output format context
        let output_path_cstr = std::ffi::CString::new(path.to_str().unwrap_or("output.mp4"))
            .map_err(|_| RoboflowError::encode("VideoEncoder", "Invalid path"))?;

        let mut format_context = AVFormatContextOutput::builder()
            .filename(&output_path_cstr)
            .build()
            .map_err(|e| {
                RoboflowError::encode(
                    "VideoEncoder",
                    format!("Failed to create format context: {}", e),
                )
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

        // Write header
        format_context.write_header(&mut None).map_err(|e| {
            RoboflowError::encode("VideoEncoder", format!("Failed to write header: {}", e))
        })?;

        // Encode all frames
        for (frame_count, frame) in buffer.frames.iter().enumerate() {
            let mut yuv_frame = AVFrame::new();
            yuv_frame.set_width(width as i32);
            yuv_frame.set_height(height as i32);
            yuv_frame.set_format(pix_fmt);

            yuv_frame.get_buffer(0).map_err(|e| {
                RoboflowError::encode(
                    "VideoEncoder",
                    format!("Failed to allocate YUV frame: {}", e),
                )
            })?;

            // Check for pre-converted NV12
            if frame.is_nv12() {
                // Direct NV12 copy
                if let Some(y_plane) = frame.y_plane() {
                    let y_size = (width as usize) * (height as usize);
                    let y_data = yuv_frame.data_mut();
                    let y_ptr = y_data[0];
                    if !y_ptr.is_null() {
                        unsafe {
                            std::ptr::copy_nonoverlapping(y_plane.as_ptr(), y_ptr, y_size);
                        }
                    }
                }
                if let Some(uv_plane) = frame.uv_plane() {
                    let uv_data = yuv_frame.data_mut();
                    let uv_ptr = uv_data[1];
                    if !uv_ptr.is_null() {
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                uv_plane.as_ptr(),
                                uv_ptr,
                                uv_plane.len(),
                            );
                        }
                    }
                }
                set_full_color_range_frame(&mut yuv_frame);
            } else {
                // RGB conversion
                let mut input_frame = AVFrame::new();
                input_frame.set_width(width as i32);
                input_frame.set_height(height as i32);
                input_frame.set_format(ffi::AV_PIX_FMT_RGB24);

                input_frame.get_buffer(0).map_err(|e| {
                    RoboflowError::encode(
                        "VideoEncoder",
                        format!("Failed to allocate input frame: {}", e),
                    )
                })?;

                let frame_data_array = input_frame.data_mut();
                let frame_data_ptr = frame_data_array[0];
                let frame_data = frame.data();
                let frame_data_slice =
                    unsafe { std::slice::from_raw_parts_mut(frame_data_ptr, frame_data.len()) };
                frame_data_slice.copy_from_slice(frame_data);

                unsafe {
                    ffi::sws_scale(
                        sws_context.as_ptr() as *mut _,
                        input_frame.data.as_ptr() as *const *const u8,
                        input_frame.linesize.as_ptr() as *const c_int,
                        0,
                        height as i32,
                        yuv_frame.data_mut().as_mut_ptr(),
                        yuv_frame.linesize_mut().as_mut_ptr(),
                    );
                    set_full_color_range_frame(&mut yuv_frame);
                }
            }

            yuv_frame.set_pts(frame_count as i64);

            codec_context.send_frame(Some(&yuv_frame)).map_err(|e| {
                RoboflowError::encode(
                    "VideoEncoder",
                    format!("Failed to send frame {}: {}", frame_count, e),
                )
            })?;

            while let Ok(mut pkt) = codec_context.receive_packet() {
                if pkt.duration == 0 {
                    pkt.set_duration(1);
                }
                format_context.write_frame(&mut pkt).map_err(|e| {
                    RoboflowError::encode("VideoEncoder", format!("Failed to write packet: {}", e))
                })?;
            }
        }

        // Flush encoder
        codec_context.send_frame(None).ok();
        while let Ok(mut pkt) = codec_context.receive_packet() {
            if pkt.duration == 0 {
                pkt.set_duration(1);
            }
            format_context.write_frame(&mut pkt).map_err(|e| {
                RoboflowError::encode(
                    "VideoEncoder",
                    format!("Failed to write flush packet: {}", e),
                )
            })?;
        }

        // Write trailer
        format_context.write_trailer().map_err(|e| {
            RoboflowError::encode("VideoEncoder", format!("Failed to write trailer: {}", e))
        })?;

        // Get file size
        if let Ok(metadata) = std::fs::metadata(path) {
            self.bytes_written = metadata.len();
        }

        Ok(())
    }

    /// Get the number of frames encoded so far.
    pub fn frame_count(&self) -> u64 {
        self.frame_count
    }

    /// Get video dimensions.
    pub fn dimensions(&self) -> (u32, u32) {
        (self.width, self.height)
    }

    /// Check if the encoder is finalized.
    pub fn is_finalized(&self) -> bool {
        self.finalized
    }

    /// Get the encoder configuration.
    pub fn config(&self) -> &VideoEncoderConfig {
        &self.config
    }

    /// Get the output configuration.
    pub fn output(&self) -> &OutputConfig {
        &self.output
    }
}

impl Drop for VideoEncoder {
    fn drop(&mut self) {
        self.format_context = None;
        self.codec_context = None;
        self.sws_context = None;
    }
}

// =============================================================================
// Conversion Helpers
// =============================================================================

impl From<VideoEncoderError> for RoboflowError {
    fn from(e: VideoEncoderError) -> Self {
        RoboflowError::encode("VideoEncoder", e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use tempfile::tempdir;

    #[test]
    fn test_output_config_file() {
        let config = OutputConfig::file("/tmp/test.mp4");
        match config {
            OutputConfig::File { path } => {
                assert_eq!(path, PathBuf::from("/tmp/test.mp4"));
            }
            _ => panic!("Expected File variant"),
        }
    }

    #[test]
    fn test_output_config_channel() {
        let (tx, _rx) = mpsc::channel();
        let config = OutputConfig::channel(tx, 256 * 1024);
        match config {
            OutputConfig::Channel { chunk_size, .. } => {
                assert_eq!(chunk_size, 256 * 1024);
            }
            _ => panic!("Expected Channel variant"),
        }
    }

    #[test]
    fn test_video_encoder_create() {
        let config = VideoEncoderConfig::default();
        let output = OutputConfig::file("/tmp/test.mp4");
        let encoder = VideoEncoder::new(config, output).unwrap();

        assert_eq!(encoder.frame_count(), 0);
        assert_eq!(encoder.dimensions(), (0, 0));
        assert!(!encoder.is_finalized());
    }

    #[test]
    fn test_video_encoder_with_dimensions() {
        let config = VideoEncoderConfig::default();
        let output = OutputConfig::file("/tmp/test.mp4");
        let encoder = VideoEncoder::with_dimensions(config, output, 640, 480).unwrap();

        assert_eq!(encoder.dimensions(), (640, 480));
    }

    #[test]
    fn test_video_encoder_validate_frame() {
        let config = VideoEncoderConfig::default();
        let output = OutputConfig::file("/tmp/test.mp4");
        let mut encoder = VideoEncoder::new(config, output).unwrap();

        // Valid frame
        let rgb_data = vec![0u8; 64 * 64 * 3];
        let result = encoder.encode_frame(&rgb_data, 64, 64);
        assert!(result.is_ok());
        assert_eq!(encoder.frame_count(), 1);
        assert_eq!(encoder.dimensions(), (64, 64));

        // Invalid dimensions
        let result = encoder.encode_frame(&rgb_data, 0, 64);
        assert!(result.is_err());

        // Size mismatch
        let small_data = vec![0u8; 100];
        let result = encoder.encode_frame(&small_data, 64, 64);
        assert!(result.is_err());

        // Dimension mismatch
        let rgb_data_128 = vec![0u8; 128 * 128 * 3];
        let result = encoder.encode_frame(&rgb_data_128, 128, 128);
        assert!(result.is_err());
    }

    #[test]
    fn test_video_encoder_finalize_empty() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("test.mp4");

        let config = VideoEncoderConfig::default();
        let output = OutputConfig::file(output_path.clone());
        let encoder = VideoEncoder::new(config, output).unwrap();

        let result = encoder.finalize().unwrap();
        assert_eq!(result.frames_encoded, 0);
        assert_eq!(result.dimensions, (0, 0));
    }

    #[test]
    fn test_video_encoder_finalize_with_frames() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("test.mp4");

        let config = VideoEncoderConfig::default();
        let output = OutputConfig::file(output_path.clone());
        let mut encoder = VideoEncoder::new(config, output).unwrap();

        // Encode frames
        let rgb_data = vec![128u8; 64 * 64 * 3];
        for _ in 0..5 {
            encoder.encode_frame(&rgb_data, 64, 64).unwrap();
        }

        let result = encoder.finalize().unwrap();
        assert_eq!(result.frames_encoded, 5);
        assert_eq!(result.dimensions, (64, 64));
        assert!(output_path.exists());
        assert!(result.bytes_written > 0);
    }

    #[test]
    fn test_video_encoder_streaming() {
        let (tx, rx) = mpsc::channel();

        let config = VideoEncoderConfig::default();
        let output = OutputConfig::channel(tx, 256 * 1024);
        let mut encoder = VideoEncoder::new(config, output).unwrap();

        // Encode frames
        let rgb_data = vec![128u8; 64 * 64 * 3];
        for _ in 0..5 {
            encoder.encode_frame(&rgb_data, 64, 64).unwrap();
        }

        let result = encoder.finalize().unwrap();
        assert_eq!(result.frames_encoded, 5);

        // Should receive chunks
        let mut chunks = 0;
        while let Ok(chunk) = rx.try_recv() {
            chunks += 1;
            assert!(!chunk.is_empty());
        }
        assert!(chunks > 0);
    }

    #[test]
    fn test_encoding_result() {
        let result = EncodingResult {
            output_path: Some(PathBuf::from("/tmp/test.mp4")),
            frames_encoded: 100,
            bytes_written: 1024 * 1024,
            dimensions: (640, 480),
            codec: "libx264".to_string(),
        };

        assert_eq!(result.frames_encoded, 100);
        assert_eq!(result.bytes_written, 1024 * 1024);
        assert_eq!(result.dimensions, (640, 480));
        assert_eq!(result.codec, "libx264");
    }

    #[test]
    fn test_encoded_chunk() {
        let chunk = EncodedChunk::new(vec![1, 2, 3, 4]);
        assert_eq!(chunk.len(), 4);
        assert!(!chunk.is_empty());

        let empty_chunk = EncodedChunk::new(vec![]);
        assert!(empty_chunk.is_empty());
    }
}
