// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Video frame types for encoding.
//!
//! This module provides frame types used by video encoders.

use std::io::Write;

use crate::video::arena::ArcSlot;

/// Errors that can occur during video encoding.
#[derive(Debug, thiserror::Error)]
pub enum VideoEncoderError {
    /// I/O error during encoding.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// FFmpeg executable not found on system PATH.
    #[error("ffmpeg not found. Please install ffmpeg to enable MP4 video encoding.")]
    FfmpegNotFound,

    /// FFmpeg process exited with non-zero status.
    #[error("ffmpeg failed with status {0}: {1}")]
    FfmpegFailed(i32, String),

    /// Attempted to encode with no frames in buffer.
    #[error("No frames to encode")]
    NoFrames,

    /// Frame dimensions don't match across buffer.
    #[error("Inconsistent frame sizes in buffer")]
    InconsistentFrameSizes,

    /// Frame data is invalid or corrupted.
    #[error("Invalid frame data: {0}")]
    InvalidFrameData(String),

    /// Generic encoding failure.
    #[error("Encoding error: {0}")]
    Encoding(String),

    /// Frame processing error.
    #[error("Frame error: {0}")]
    FrameError(String),

    /// Initialization error.
    #[error("Initialization error: {0}")]
    InitializationError(String),

    /// Finalization error.
    #[error("Finalization error: {0}")]
    FinalizationError(String),

    /// Error when encoder is already finalized.
    #[error("Encoder is finalized: {0}")]
    FinalizedError(String),
}

/// Frame data format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FrameFormat {
    /// RGB24 format (3 bytes per pixel).
    #[default]
    Rgb24,
    /// NV12 format (Y plane + interleaved UV).
    /// Y: width × height bytes
    /// UV: width/2 × height/2 × 2 bytes
    Nv12,
    /// YUV420P format (planar).
    /// Y: width × height bytes
    /// U: width/2 × height/2 bytes
    /// V: width/2 × height/2 bytes
    Yuv420p,
    /// JPEG-encoded data (passthrough mode).
    Jpeg,
}

/// Internal storage for VideoFrame.
#[derive(Debug)]
enum VideoFrameInner {
    /// Owned data (traditional path).
    Owned(Vec<u8>),
    /// Shared arena buffer (zero-copy path).
    Shared(std::sync::Arc<FrameBuffer>),
}

/// A single video frame.
#[derive(Debug)]
pub struct VideoFrame {
    /// Width in pixels.
    pub width: u32,

    /// Height in pixels.
    pub height: u32,

    /// Frame data storage.
    inner: VideoFrameInner,

    /// Frame data format.
    pub format: FrameFormat,
}

impl VideoFrame {
    /// Create a new RGB24 video frame.
    pub fn new(width: u32, height: u32, data: Vec<u8>) -> Self {
        Self {
            width,
            height,
            inner: VideoFrameInner::Owned(data),
            format: FrameFormat::Rgb24,
        }
    }

    /// Create a new NV12 video frame (pre-converted for hardware encoders).
    ///
    /// This method combines Y and UV planes into a single contiguous buffer
    /// with optimal memory copying.
    pub fn from_nv12(width: u32, height: u32, y_plane: Vec<u8>, uv_plane: Vec<u8>) -> Self {
        // Combine Y and UV planes into a single buffer
        // Layout: Y plane (width × height) followed by UV plane (width/2 × height/2 × 2)
        let total_len = y_plane.len() + uv_plane.len();
        let mut data = vec![0; total_len];

        // Copy Y plane first
        data[..y_plane.len()].copy_from_slice(&y_plane);
        // Then copy UV plane
        data[y_plane.len()..].copy_from_slice(&uv_plane);

        Self {
            width,
            height,
            inner: VideoFrameInner::Owned(data),
            format: FrameFormat::Nv12,
        }
    }

    /// Create a new YUV420P video frame (pre-converted for software encoders).
    ///
    /// This method combines Y, U, V planes into a single contiguous buffer
    /// with optimal memory copying.
    pub fn from_yuv420p(
        width: u32,
        height: u32,
        y_plane: Vec<u8>,
        u_plane: Vec<u8>,
        v_plane: Vec<u8>,
    ) -> Self {
        // Combine Y, U, V planes into a single buffer
        // Layout: Y plane (width × height) + U plane (width/2 × height/2) + V plane (width/2 × height/2)
        let y_len = y_plane.len();
        let u_len = u_plane.len();
        let v_len = v_plane.len();
        let total_len = y_len + u_len + v_len;

        let mut data = vec![0; total_len];

        // Copy planes in order: Y, then U, then V
        let mut offset = 0;
        data[offset..offset + y_len].copy_from_slice(&y_plane);
        offset += y_len;
        data[offset..offset + u_len].copy_from_slice(&u_plane);
        offset += u_len;
        data[offset..].copy_from_slice(&v_plane);

        Self {
            width,
            height,
            inner: VideoFrameInner::Owned(data),
            format: FrameFormat::Yuv420p,
        }
    }

    /// Create a new video frame from JPEG-encoded data.
    pub fn from_jpeg(width: u32, height: u32, jpeg_data: Vec<u8>) -> Self {
        Self {
            width,
            height,
            inner: VideoFrameInner::Owned(jpeg_data),
            format: FrameFormat::Jpeg,
        }
    }

    /// Create a VideoFrame from an Arc<FrameBuffer> (zero-copy).
    ///
    /// This is the preferred path for the three-stage pipeline.
    pub fn from_arc_frame_buffer(buffer: std::sync::Arc<FrameBuffer>) -> Self {
        let width = buffer.width();
        let height = buffer.height();
        let format = match buffer.format() {
            PixelFormat::Rgb24 => FrameFormat::Rgb24,
            PixelFormat::Nv12 => FrameFormat::Nv12,
            PixelFormat::Yuv420p => FrameFormat::Yuv420p,
            PixelFormat::Jpeg => FrameFormat::Jpeg,
        };
        Self {
            width,
            height,
            inner: VideoFrameInner::Shared(buffer),
            format,
        }
    }

    /// Get the raw frame data as a byte slice.
    pub fn data(&self) -> &[u8] {
        match &self.inner {
            VideoFrameInner::Owned(data) => data,
            VideoFrameInner::Shared(buffer) => buffer.data(),
        }
    }

    /// Check if this frame uses shared (zero-copy) storage.
    pub fn is_shared(&self) -> bool {
        matches!(self.inner, VideoFrameInner::Shared(_))
    }

    /// Check if this frame is in JPEG format.
    pub fn is_jpeg(&self) -> bool {
        self.format == FrameFormat::Jpeg
    }

    /// Check if this frame is pre-converted to NV12.
    pub fn is_nv12(&self) -> bool {
        self.format == FrameFormat::Nv12
    }

    /// Check if this frame is pre-converted to YUV420P.
    pub fn is_yuv420p(&self) -> bool {
        self.format == FrameFormat::Yuv420p
    }

    /// Check if this frame is RGB24.
    pub fn is_rgb24(&self) -> bool {
        self.format == FrameFormat::Rgb24
    }

    /// Get the Y plane for NV12 frames.
    pub fn y_plane(&self) -> Option<&[u8]> {
        if self.format != FrameFormat::Nv12 {
            return None;
        }
        let y_size = (self.width as usize) * (self.height as usize);
        self.data().get(..y_size)
    }

    /// Get the UV plane for NV12 frames.
    pub fn uv_plane(&self) -> Option<&[u8]> {
        if self.format != FrameFormat::Nv12 {
            return None;
        }
        let y_size = (self.width as usize) * (self.height as usize);
        self.data().get(y_size..)
    }

    /// Get the expected data size for this frame.
    pub fn expected_size(&self) -> usize {
        match self.format {
            FrameFormat::Rgb24 => (self.width * self.height * 3) as usize,
            FrameFormat::Nv12 => {
                let y_size = (self.width as usize) * (self.height as usize);
                let uv_size = (self.width as usize / 2) * (self.height as usize / 2) * 2;
                y_size + uv_size
            }
            FrameFormat::Yuv420p => {
                let y_size = (self.width as usize) * (self.height as usize);
                let uv_size = (self.width as usize / 2) * (self.height as usize / 2);
                y_size + uv_size * 2 // U + V planes
            }
            FrameFormat::Jpeg => self.data().len(), // JPEG data size is variable
        }
    }

    /// Validate the frame data.
    pub fn validate(&self) -> Result<(), VideoEncoderError> {
        let data = self.data();
        match self.format {
            FrameFormat::Jpeg => {
                // JPEG data: just check it's not empty and has valid header
                if data.len() < 4 {
                    return Err(VideoEncoderError::InvalidFrameData(
                        "Empty JPEG data".to_string(),
                    ));
                }
                // Check JPEG magic bytes
                if data[0] != 0xFF || data[1] != 0xD8 || data[2] != 0xFF {
                    return Err(VideoEncoderError::InvalidFrameData(
                        "Invalid JPEG header".to_string(),
                    ));
                }
            }
            FrameFormat::Rgb24 => {
                // RGB data: check exact size with overflow protection
                let expected = (self.width as usize)
                    .checked_mul(self.height as usize)
                    .and_then(|size| size.checked_mul(3))
                    .ok_or_else(|| {
                        VideoEncoderError::InvalidFrameData("Invalid frame data".to_string())
                    })?;
                if data.len() != expected {
                    return Err(VideoEncoderError::InvalidFrameData(
                        "Invalid frame data".to_string(),
                    ));
                }
            }
            FrameFormat::Nv12 => {
                // NV12 requires even dimensions
                if !self.width.is_multiple_of(2) || !self.height.is_multiple_of(2) {
                    return Err(VideoEncoderError::InvalidFrameData(
                        "Invalid frame data".to_string(),
                    ));
                }
                // NV12 data: check exact size with overflow protection
                let y_size = (self.width as usize)
                    .checked_mul(self.height as usize)
                    .ok_or_else(|| {
                        VideoEncoderError::InvalidFrameData("Invalid frame data".to_string())
                    })?;
                let uv_width = self.width as usize / 2;
                let uv_height = self.height as usize / 2;
                let uv_size = uv_width
                    .checked_mul(uv_height)
                    .and_then(|size| size.checked_mul(2))
                    .ok_or_else(|| {
                        VideoEncoderError::InvalidFrameData("Invalid frame data".to_string())
                    })?;
                let expected = y_size.checked_add(uv_size).ok_or_else(|| {
                    VideoEncoderError::InvalidFrameData("Invalid frame data".to_string())
                })?;
                if data.len() != expected {
                    return Err(VideoEncoderError::InvalidFrameData(
                        "Invalid frame data".to_string(),
                    ));
                }
            }
            FrameFormat::Yuv420p => {
                // YUV420P requires even dimensions
                if !self.width.is_multiple_of(2) || !self.height.is_multiple_of(2) {
                    return Err(VideoEncoderError::InvalidFrameData(
                        "Invalid frame data".to_string(),
                    ));
                }
                // YUV420P data: check exact size with overflow protection
                let y_size = (self.width as usize)
                    .checked_mul(self.height as usize)
                    .ok_or_else(|| {
                        VideoEncoderError::InvalidFrameData("Invalid frame data".to_string())
                    })?;
                let uv_width = self.width as usize / 2;
                let uv_height = self.height as usize / 2;
                let uv_size = uv_width.checked_mul(uv_height).ok_or_else(|| {
                    VideoEncoderError::InvalidFrameData("Invalid frame data".to_string())
                })?;
                let expected = y_size
                    .checked_add(uv_size)
                    .and_then(|size| size.checked_add(uv_size)) // Add U + V
                    .ok_or_else(|| {
                        VideoEncoderError::InvalidFrameData("Invalid frame data".to_string())
                    })?;
                if data.len() != expected {
                    return Err(VideoEncoderError::InvalidFrameData(
                        "Invalid frame data".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Write frame in PPM format (only for RGB24 frames).
    pub fn write_ppm(&self, writer: &mut impl Write) -> Result<(), VideoEncoderError> {
        if self.format != FrameFormat::Rgb24 {
            return Err(VideoEncoderError::InvalidFrameData(
                "Only RGB24 frames can be written as PPM".to_string(),
            ));
        }
        writeln!(writer, "P6")?;
        writeln!(writer, "{} {}", self.width, self.height)?;
        writeln!(writer, "255")?;
        writer.write_all(self.data())?;
        Ok(())
    }
}

impl Clone for VideoFrame {
    fn clone(&self) -> Self {
        // Cloning shares the buffer if it's shared, otherwise copies owned data
        match &self.inner {
            VideoFrameInner::Shared(buffer) => Self {
                width: self.width,
                height: self.height,
                inner: VideoFrameInner::Shared(std::sync::Arc::clone(buffer)),
                format: self.format,
            },
            VideoFrameInner::Owned(data) => Self {
                width: self.width,
                height: self.height,
                inner: VideoFrameInner::Owned(data.clone()),
                format: self.format,
            },
        }
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

// =============================================================================
// Unified FrameBuffer for Zero-Copy Pipeline
// =============================================================================

/// Pixel format for frame data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PixelFormat {
    /// RGB24 format (3 bytes per pixel).
    #[default]
    Rgb24,
    /// NV12 format (Y plane + interleaved UV).
    Nv12,
    /// YUV420P format (planar Y, U, V).
    Yuv420p,
    /// JPEG-encoded data (passthrough mode).
    Jpeg,
}

/// Internal storage for FrameBuffer.
#[derive(Debug)]
enum FrameBufferInner {
    /// Arena-allocated slot for zero-copy.
    Arena(ArcSlot),
    /// Owned vector (fallback when arena exhausted).
    Owned(Vec<u8>),
}

/// Unified frame buffer supporting both arena and owned allocation.
///
/// This type is used throughout the video pipeline to enable zero-copy
/// processing from decode through encode.
#[derive(Debug)]
pub struct FrameBuffer {
    /// Internal storage (arena or owned).
    inner: FrameBufferInner,
    /// Pixel format of the data.
    format: PixelFormat,
    /// Frame width in pixels.
    width: u32,
    /// Frame height in pixels.
    height: u32,
    /// Actual data length (may be less than buffer size for in-place conversion).
    len: usize,
}

impl FrameBuffer {
    /// Create a new FrameBuffer from an arena slot.
    pub fn from_arena(
        slot: ArcSlot,
        format: PixelFormat,
        width: u32,
        height: u32,
        len: usize,
    ) -> Self {
        Self {
            inner: FrameBufferInner::Arena(slot),
            format,
            width,
            height,
            len,
        }
    }

    /// Create a new FrameBuffer from owned data.
    pub fn from_owned(data: Vec<u8>, format: PixelFormat, width: u32, height: u32) -> Self {
        let len = data.len();
        Self {
            inner: FrameBufferInner::Owned(data),
            format,
            width,
            height,
            len,
        }
    }

    /// Create a new RGB24 FrameBuffer from owned data.
    pub fn rgb24(width: u32, height: u32, data: Vec<u8>) -> Self {
        Self::from_owned(data, PixelFormat::Rgb24, width, height)
    }

    /// Create a new NV12 FrameBuffer from owned data.
    pub fn nv12(width: u32, height: u32, data: Vec<u8>) -> Self {
        Self::from_owned(data, PixelFormat::Nv12, width, height)
    }

    /// Get the pixel format.
    #[inline]
    pub fn format(&self) -> PixelFormat {
        self.format
    }

    /// Get the frame width.
    #[inline]
    pub fn width(&self) -> u32 {
        self.width
    }

    /// Get the frame height.
    #[inline]
    pub fn height(&self) -> u32 {
        self.height
    }

    /// Get the data as a byte slice.
    #[inline]
    pub fn data(&self) -> &[u8] {
        match &self.inner {
            FrameBufferInner::Arena(slot) => {
                let slot_data = slot.data();
                let len = self.len.min(slot_data.len());
                &slot_data[..len]
            }
            FrameBufferInner::Owned(data) => {
                let len = self.len.min(data.len());
                &data[..len]
            }
        }
    }

    /// Get the data as a mutable byte slice.
    ///
    /// # Safety
    /// This is safe for arena slots because ArcSlot provides exclusive access.
    #[inline]
    pub fn data_mut(&mut self) -> &mut [u8] {
        let len = self.len;
        match &mut self.inner {
            FrameBufferInner::Arena(slot) => {
                let slot_data = slot.data_mut();
                let actual_len = len.min(slot_data.len());
                &mut slot_data[..actual_len]
            }
            FrameBufferInner::Owned(data) => {
                let actual_len = len.min(data.len());
                &mut data[..actual_len]
            }
        }
    }

    /// Get the total buffer capacity (may be larger than actual data).
    #[inline]
    pub fn capacity(&self) -> usize {
        match &self.inner {
            FrameBufferInner::Arena(slot) => slot.expected_size(),
            FrameBufferInner::Owned(data) => data.len(),
        }
    }

    /// Get the actual data length.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Set the pixel format (for in-place conversion).
    pub fn set_format(&mut self, format: PixelFormat) {
        self.format = format;
    }

    /// Set the data length (for in-place conversion where output is smaller).
    pub fn set_len(&mut self, len: usize) {
        self.len = len.min(self.capacity());
    }

    /// Check if this buffer uses arena allocation.
    pub fn is_arena(&self) -> bool {
        matches!(self.inner, FrameBufferInner::Arena(_))
    }

    /// Convert to a VideoFrame for encoding.
    ///
    /// This clones the data, so it should only be used when necessary.
    pub fn to_video_frame(&self) -> VideoFrame {
        let data = self.data().to_vec();
        let frame_format = match self.format {
            PixelFormat::Rgb24 => FrameFormat::Rgb24,
            PixelFormat::Nv12 => FrameFormat::Nv12,
            PixelFormat::Yuv420p => FrameFormat::Yuv420p,
            PixelFormat::Jpeg => FrameFormat::Jpeg,
        };
        VideoFrame {
            width: self.width,
            height: self.height,
            inner: VideoFrameInner::Owned(data),
            format: frame_format,
        }
    }

    /// Get the expected data size for this format and dimensions.
    pub fn expected_size(&self) -> usize {
        match self.format {
            PixelFormat::Rgb24 => (self.width as usize) * (self.height as usize) * 3,
            PixelFormat::Nv12 => {
                let y_size = (self.width as usize) * (self.height as usize);
                let uv_size = (self.width as usize / 2) * (self.height as usize / 2) * 2;
                y_size + uv_size
            }
            PixelFormat::Yuv420p => {
                let y_size = (self.width as usize) * (self.height as usize);
                let uv_size = (self.width as usize / 2) * (self.height as usize / 2);
                y_size + uv_size * 2
            }
            PixelFormat::Jpeg => self.len,
        }
    }

    /// Validate the frame data.
    pub fn validate(&self) -> Result<(), VideoEncoderError> {
        let expected = self.expected_size();
        if self.len != expected && self.format != PixelFormat::Jpeg {
            return Err(VideoEncoderError::InvalidFrameData(
                "Invalid frame buffer size".to_string(),
            ));
        }

        // For JPEG, check magic bytes
        if self.format == PixelFormat::Jpeg {
            let data = self.data();
            if data.len() < 4 {
                return Err(VideoEncoderError::InvalidFrameData(
                    "Empty JPEG data".to_string(),
                ));
            }
            if data[0] != 0xFF || data[1] != 0xD8 || data[2] != 0xFF {
                return Err(VideoEncoderError::InvalidFrameData(
                    "Invalid JPEG header".to_string(),
                ));
            }
        }

        Ok(())
    }
}

impl Clone for FrameBuffer {
    fn clone(&self) -> Self {
        // Cloning always creates an owned copy
        Self::from_owned(self.data().to_vec(), self.format, self.width, self.height)
    }
}

// SAFETY: FrameBuffer's data access is thread-safe:
// - Arena slots (ArcSlot) are designed for concurrent access with proper synchronization
// - Owned data (Vec<u8>) is only accessed through immutable borrows when shared
unsafe impl Send for FrameBuffer {}
unsafe impl Sync for FrameBuffer {}

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
            return Err(VideoEncoderError::InvalidFrameData(
                "Invalid depth frame size".to_string(),
            ));
        }
        Ok(())
    }
}

/// Buffer for depth video frames.
#[derive(Debug, Clone, Default)]
pub struct DepthFrameBuffer {
    /// The depth frames in this buffer.
    pub frames: Vec<DepthFrame>,
    /// Width of frames in pixels (None until first frame added).
    pub width: Option<u32>,
    /// Height of frames in pixels (None until first frame added).
    pub height: Option<u32>,
}

impl DepthFrameBuffer {
    /// Create a new empty depth frame buffer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a depth frame to the buffer.
    ///
    /// Returns an error if the frame dimensions don't match existing frames.
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

    /// Returns the number of frames in the buffer.
    pub fn len(&self) -> usize {
        self.frames.len()
    }

    /// Returns true if the buffer contains no frames.
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    /// Returns the frame dimensions as (width, height), or None if no frames.
    pub fn dimensions(&self) -> Option<(u32, u32)> {
        match (self.width, self.height) {
            (Some(w), Some(h)) => Some((w, h)),
            _ => None,
        }
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
    fn test_video_frame_expected_size() {
        let frame = VideoFrame::new(640, 480, vec![]);
        assert_eq!(frame.expected_size(), 640 * 480 * 3);
    }

    #[test]
    fn test_video_frame_from_jpeg() {
        // Valid JPEG magic bytes: FF D8 FF
        let jpeg_data = vec![0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10];
        let frame = VideoFrame::from_jpeg(640, 480, jpeg_data.clone());
        assert!(frame.validate().is_ok());
        assert!(frame.is_jpeg());
        assert_eq!(frame.data(), jpeg_data);
        assert_eq!(frame.expected_size(), jpeg_data.len());
    }

    #[test]
    fn test_video_frame_invalid_jpeg_too_short() {
        let jpeg_data = vec![0xFF, 0xD8]; // Only 2 bytes
        let frame = VideoFrame::from_jpeg(640, 480, jpeg_data);
        assert!(frame.validate().is_err());
    }

    #[test]
    fn test_video_frame_invalid_jpeg_magic() {
        let jpeg_data = vec![0x00, 0x00, 0x00, 0x00]; // Wrong magic bytes
        let frame = VideoFrame::from_jpeg(640, 480, jpeg_data);
        assert!(frame.validate().is_err());
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
    fn test_frame_buffer_is_empty() {
        let buffer = VideoFrameBuffer::new();
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_frame_buffer_multiple_same_size() {
        let mut buffer = VideoFrameBuffer::new();
        let size = 100 * 100 * 3;

        for _ in 0..10 {
            assert!(
                buffer
                    .add_frame(VideoFrame::new(100, 100, vec![0u8; size]))
                    .is_ok()
            );
        }
        assert_eq!(buffer.len(), 10);
        assert_eq!(buffer.dimensions(), Some((100, 100)));
    }

    #[test]
    fn test_depth_frame_validate() {
        let frame = DepthFrame::new(2, 2, vec![0u8; 8]); // 2*2*2 = 8
        assert!(frame.validate().is_ok());

        let invalid_frame = DepthFrame::new(2, 2, vec![0u8; 6]);
        assert!(invalid_frame.validate().is_err());
    }

    #[test]
    fn test_depth_frame_expected_size() {
        let frame = DepthFrame::new(640, 480, vec![]);
        assert_eq!(frame.expected_size(), 640 * 480 * 2);
    }

    #[test]
    fn test_depth_frame_buffer() {
        let mut buffer = DepthFrameBuffer::new();
        assert!(buffer.is_empty());

        let frame = DepthFrame::new(100, 100, vec![0u8; 100 * 100 * 2]);
        assert!(buffer.add_frame(frame).is_ok());
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.dimensions(), Some((100, 100)));
    }

    #[test]
    fn test_depth_frame_buffer_inconsistent_size() {
        let mut buffer = DepthFrameBuffer::new();

        let frame1 = DepthFrame::new(100, 100, vec![0u8; 100 * 100 * 2]);
        assert!(buffer.add_frame(frame1).is_ok());

        let frame2 = DepthFrame::new(200, 200, vec![0u8; 200 * 200 * 2]);
        assert!(buffer.add_frame(frame2).is_err());
    }

    #[test]
    fn test_video_frame_write_ppm() {
        let frame = VideoFrame::new(2, 2, vec![255u8; 12]);
        let mut output = Vec::new();
        assert!(frame.write_ppm(&mut output).is_ok());

        // PPM format: P6 header, dimensions, max value, then binary data
        // Header is ASCII, data is binary
        assert!(output.starts_with(b"P6\n"));
        assert!(output.len() > 12); // Has header + data
    }

    #[test]
    fn test_video_frame_write_ppm_non_rgb24() {
        // NV12 frame should fail write_ppm
        let frame = VideoFrame::from_nv12(2, 2, vec![0u8; 4], vec![0u8; 2]);
        let mut output = Vec::new();
        let result = frame.write_ppm(&mut output);
        assert!(result.is_err());
    }

    #[test]
    fn test_video_frame_from_nv12() {
        let y_plane = vec![128u8; 4]; // 2x2
        let uv_plane = vec![128u8; 2]; // 1x1x2
        let frame = VideoFrame::from_nv12(2, 2, y_plane.clone(), uv_plane.clone());

        assert_eq!(frame.width, 2);
        assert_eq!(frame.height, 2);
        assert!(frame.is_nv12());
        assert!(!frame.is_rgb24());
        assert!(!frame.is_yuv420p());
        assert!(!frame.is_jpeg());

        // Check data layout: Y plane followed by UV plane
        assert_eq!(frame.data().len(), 6);
        assert_eq!(&frame.data()[..4], y_plane.as_slice());
        assert_eq!(&frame.data()[4..], uv_plane.as_slice());
    }

    #[test]
    fn test_video_frame_from_yuv420p() {
        let y_plane = vec![128u8; 4]; // 2x2
        let u_plane = vec![128u8; 1]; // 1x1
        let v_plane = vec![128u8; 1]; // 1x1
        let frame =
            VideoFrame::from_yuv420p(2, 2, y_plane.clone(), u_plane.clone(), v_plane.clone());

        assert_eq!(frame.width, 2);
        assert_eq!(frame.height, 2);
        assert!(frame.is_yuv420p());
        assert!(!frame.is_rgb24());
        assert!(!frame.is_nv12());
        assert!(!frame.is_jpeg());

        // Check data layout: Y + U + V planes
        assert_eq!(frame.data().len(), 6);
        assert_eq!(&frame.data()[..4], y_plane.as_slice());
        assert_eq!(&frame.data()[4..5], u_plane.as_slice());
        assert_eq!(&frame.data()[5..], v_plane.as_slice());
    }

    #[test]
    fn test_video_frame_nv12_planes() {
        let y_plane = vec![16u8; 4];
        let uv_plane = vec![128u8, 128u8];
        let frame = VideoFrame::from_nv12(2, 2, y_plane.clone(), uv_plane.clone());

        assert_eq!(frame.y_plane(), Some(y_plane.as_slice()));
        assert_eq!(frame.uv_plane(), Some(uv_plane.as_slice()));
    }

    #[test]
    fn test_video_frame_nv12_planes_wrong_format() {
        // RGB24 frame should return None for y_plane/uv_plane
        let frame = VideoFrame::new(2, 2, vec![0u8; 12]);
        assert_eq!(frame.y_plane(), None);
        assert_eq!(frame.uv_plane(), None);
    }

    #[test]
    fn test_video_frame_validate_nv12() {
        // Valid NV12 (even dimensions, correct size)
        let frame = VideoFrame::from_nv12(2, 2, vec![0u8; 4], vec![0u8; 2]);
        assert!(frame.validate().is_ok());

        // Invalid: wrong size
        let bad_frame = VideoFrame {
            width: 2,
            height: 2,
            inner: VideoFrameInner::Owned(vec![0u8; 3]), // Too short
            format: FrameFormat::Nv12,
        };
        assert!(bad_frame.validate().is_err());

        // Invalid: odd dimensions
        let odd_frame = VideoFrame {
            width: 3,
            height: 3,
            inner: VideoFrameInner::Owned(vec![0u8; 16]),
            format: FrameFormat::Nv12,
        };
        assert!(odd_frame.validate().is_err());
    }

    #[test]
    fn test_video_frame_validate_yuv420p() {
        // Valid YUV420P (even dimensions, correct size)
        let frame = VideoFrame::from_yuv420p(2, 2, vec![0u8; 4], vec![0u8; 1], vec![0u8; 1]);
        assert!(frame.validate().is_ok());

        // Invalid: wrong size
        let bad_frame = VideoFrame {
            width: 2,
            height: 2,
            inner: VideoFrameInner::Owned(vec![0u8; 3]), // Too short
            format: FrameFormat::Yuv420p,
        };
        assert!(bad_frame.validate().is_err());

        // Invalid: odd dimensions
        let odd_frame = VideoFrame {
            width: 3,
            height: 3,
            inner: VideoFrameInner::Owned(vec![0u8; 16]),
            format: FrameFormat::Yuv420p,
        };
        assert!(odd_frame.validate().is_err());
    }

    #[test]
    fn test_video_frame_clone_owned() {
        let frame = VideoFrame::new(4, 4, vec![42u8; 48]);
        let cloned = frame.clone();

        assert_eq!(frame.data(), cloned.data());
        assert_eq!(frame.width, cloned.width);
        assert_eq!(frame.height, cloned.height);
        assert_eq!(frame.format, cloned.format);
    }

    #[test]
    fn test_video_frame_clone_shared() {
        let buffer = std::sync::Arc::new(FrameBuffer::rgb24(4, 4, vec![42u8; 48]));
        let frame = VideoFrame::from_arc_frame_buffer(buffer);
        assert!(frame.is_shared());

        let cloned = frame.clone();
        assert!(cloned.is_shared());
        assert_eq!(frame.data(), cloned.data());
    }

    #[test]
    fn test_video_frame_expected_size_nv12() {
        let frame = VideoFrame::from_nv12(64, 48, vec![], vec![]);
        let expected = 64 * 48 + (64 / 2) * (48 / 2) * 2; // Y + UV
        assert_eq!(frame.expected_size(), expected);
    }

    #[test]
    fn test_video_frame_expected_size_yuv420p() {
        let frame = VideoFrame::from_yuv420p(64, 48, vec![], vec![], vec![]);
        let y_size = 64 * 48;
        let uv_size = (64 / 2) * (48 / 2);
        assert_eq!(frame.expected_size(), y_size + uv_size * 2);
    }

    #[test]
    fn test_frame_buffer_rgb24_factory() {
        let buffer = FrameBuffer::rgb24(4, 4, vec![42u8; 48]);
        assert_eq!(buffer.format(), PixelFormat::Rgb24);
        assert_eq!(buffer.width(), 4);
        assert_eq!(buffer.height(), 4);
        assert_eq!(buffer.len(), 48);
        assert!(!buffer.is_arena());
    }

    #[test]
    fn test_frame_buffer_nv12_factory() {
        let buffer = FrameBuffer::nv12(4, 4, vec![42u8; 30]);
        assert_eq!(buffer.format(), PixelFormat::Nv12);
        assert_eq!(buffer.width(), 4);
        assert_eq!(buffer.height(), 4);
    }

    #[test]
    fn test_frame_buffer_data_mut() {
        let mut buffer = FrameBuffer::rgb24(2, 2, vec![0u8; 12]);
        let data = buffer.data_mut();
        data[0] = 255;
        assert_eq!(buffer.data()[0], 255);
    }

    #[test]
    fn test_frame_buffer_capacity() {
        let buffer = FrameBuffer::rgb24(2, 2, vec![0u8; 12]);
        assert_eq!(buffer.capacity(), 12);
    }

    #[test]
    fn test_frame_buffer_set_format() {
        let mut buffer = FrameBuffer::rgb24(2, 2, vec![0u8; 12]);
        assert_eq!(buffer.format(), PixelFormat::Rgb24);

        buffer.set_format(PixelFormat::Nv12);
        assert_eq!(buffer.format(), PixelFormat::Nv12);
    }

    #[test]
    fn test_frame_buffer_set_len() {
        let mut buffer = FrameBuffer::rgb24(2, 2, vec![0u8; 12]);
        assert_eq!(buffer.len(), 12);

        buffer.set_len(6);
        assert_eq!(buffer.len(), 6);
        // data() should respect the new len
        assert_eq!(buffer.data().len(), 6);

        // Setting len larger than capacity should cap at capacity
        buffer.set_len(100);
        assert_eq!(buffer.len(), 12); // capped at capacity
    }

    #[test]
    fn test_frame_buffer_is_empty_vs_not() {
        let buffer = FrameBuffer::from_owned(vec![], PixelFormat::Rgb24, 0, 0);
        assert!(buffer.is_empty());

        let buffer = FrameBuffer::rgb24(2, 2, vec![0u8; 12]);
        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_frame_buffer_expected_size() {
        // RGB24
        let buffer = FrameBuffer::rgb24(64, 48, vec![]);
        assert_eq!(buffer.expected_size(), 64 * 48 * 3);

        // NV12
        let buffer = FrameBuffer::nv12(64, 48, vec![]);
        let y_size = 64 * 48;
        let uv_size = (64 / 2) * (48 / 2) * 2;
        assert_eq!(buffer.expected_size(), y_size + uv_size);

        // YUV420P
        let buffer = FrameBuffer::from_owned(vec![], PixelFormat::Yuv420p, 64, 48);
        let y_size = 64 * 48;
        let uv_size = (64 / 2) * (48 / 2);
        assert_eq!(buffer.expected_size(), y_size + uv_size * 2);

        // JPEG - variable size, returns len
        let buffer = FrameBuffer::from_owned(vec![0u8; 100], PixelFormat::Jpeg, 64, 48);
        assert_eq!(buffer.expected_size(), 100);
    }

    #[test]
    fn test_frame_buffer_validate_rgb24() {
        // Valid
        let buffer = FrameBuffer::rgb24(2, 2, vec![0u8; 12]);
        assert!(buffer.validate().is_ok());

        // Invalid size
        let mut buffer = FrameBuffer::from_owned(vec![0u8; 10], PixelFormat::Rgb24, 2, 2);
        buffer.set_len(10); // Ensure len is set
        assert!(buffer.validate().is_err());
    }

    #[test]
    fn test_frame_buffer_validate_jpeg() {
        // Valid JPEG header
        let jpeg_data = vec![0xFF, 0xD8, 0xFF, 0xE0];
        let buffer = FrameBuffer::from_owned(jpeg_data, PixelFormat::Jpeg, 64, 48);
        assert!(buffer.validate().is_ok());

        // Invalid: too short
        let buffer = FrameBuffer::from_owned(vec![0xFF, 0xD8], PixelFormat::Jpeg, 64, 48);
        assert!(buffer.validate().is_err());

        // Invalid: wrong magic bytes
        let buffer =
            FrameBuffer::from_owned(vec![0x00, 0x00, 0x00, 0x00], PixelFormat::Jpeg, 64, 48);
        assert!(buffer.validate().is_err());
    }

    #[test]
    fn test_frame_buffer_clone() {
        let original = FrameBuffer::rgb24(2, 2, vec![42u8; 12]);
        let cloned = original.clone();

        assert_eq!(original.data(), cloned.data());
        assert_eq!(original.format(), cloned.format());
        assert_eq!(original.width(), cloned.width());
        assert_eq!(original.height(), cloned.height());
        // Clone creates owned copy, not arena
        assert!(!cloned.is_arena());
    }

    #[test]
    fn test_frame_buffer_to_video_frame() {
        let buffer = FrameBuffer::rgb24(2, 2, vec![42u8; 12]);
        let frame = buffer.to_video_frame();

        assert_eq!(frame.width, 2);
        assert_eq!(frame.height, 2);
        assert!(frame.is_rgb24());
        assert!(!frame.is_shared());
        assert_eq!(frame.data(), buffer.data());
    }

    #[test]
    fn test_video_frame_from_arc_frame_buffer() {
        let buffer = std::sync::Arc::new(FrameBuffer::nv12(4, 4, vec![128u8; 30]));
        let frame = VideoFrame::from_arc_frame_buffer(buffer);

        assert!(frame.is_shared());
        assert!(frame.is_nv12());
        assert_eq!(frame.width, 4);
        assert_eq!(frame.height, 4);

        // Test with different formats
        let yuv_buffer = std::sync::Arc::new(FrameBuffer::from_owned(
            vec![128u8; 12],
            PixelFormat::Yuv420p,
            2,
            2,
        ));
        let yuv_frame = VideoFrame::from_arc_frame_buffer(yuv_buffer);
        assert!(yuv_frame.is_yuv420p());
        assert!(yuv_frame.is_shared());

        let jpeg_buffer = std::sync::Arc::new(FrameBuffer::from_owned(
            vec![0xFF, 0xD8, 0xFF, 0xE0],
            PixelFormat::Jpeg,
            64,
            48,
        ));
        let jpeg_frame = VideoFrame::from_arc_frame_buffer(jpeg_buffer);
        assert!(jpeg_frame.is_jpeg());
        assert!(jpeg_frame.is_shared());
    }
}
