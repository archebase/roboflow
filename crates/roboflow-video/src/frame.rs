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
            // RGB data: check exact size
            let expected = (self.width * self.height * 3) as usize;
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
}
