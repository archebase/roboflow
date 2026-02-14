// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Video frame types for encoding.
//!
//! This module provides frame types used by video encoders.

use std::io::Write;

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
    #[error("Invalid frame data")]
    InvalidFrameData,

    /// Generic encoding failure.
    #[error("Encoding error: {0}")]
    Encoding(String),
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
            // RGB data: check exact size with overflow protection
            let expected = (self.width as usize)
                .checked_mul(self.height as usize)
                .and_then(|size| size.checked_mul(3))
                .ok_or(VideoEncoderError::InvalidFrameData)?;
            if self.data.len() != expected {
                return Err(VideoEncoderError::InvalidFrameData);
            }
        }
        Ok(())
    }

    /// Write frame in PPM format.
    pub fn write_ppm(&self, writer: &mut impl Write) -> Result<(), VideoEncoderError> {
        writeln!(writer, "P6")?;
        writeln!(writer, "{} {}", self.width, self.height)?;
        writeln!(writer, "255")?;
        writer.write_all(&self.data)?;
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
        assert!(frame.is_jpeg);
        assert_eq!(frame.data, jpeg_data);
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
}
