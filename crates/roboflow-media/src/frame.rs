// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Core frame data types for robotics datasets.
//!
//! This module provides fundamental data types for representing
//! image, audio, and camera calibration data used across all
//! roboflow crates.

/// Error type for image data operations.
#[derive(Debug, thiserror::Error)]
pub enum ImageDataError {
    /// Image dimensions don't match the data size.
    #[error("Size mismatch: {width}x{height} expects {expected_size} bytes, got {actual_size}")]
    SizeMismatch {
        /// Image width in pixels.
        width: u32,
        /// Image height in pixels.
        height: u32,
        /// Expected data size based on dimensions.
        expected_size: usize,
        /// Actual data size received.
        actual_size: usize,
    },
}

/// Image data with metadata.
///
/// This is a shared type used across different dataset formats.
/// It contains the raw image data along with dimensions and metadata.
#[derive(Debug, Clone)]
pub struct ImageData {
    /// Image width in pixels.
    pub width: u32,

    /// Image height in pixels.
    pub height: u32,

    /// Raw image data (RGB8 or encoded).
    pub data: Vec<u8>,

    /// Original timestamp from the message.
    pub original_timestamp: u64,

    /// Whether data is already encoded (e.g., JPEG/PNG).
    pub is_encoded: bool,

    /// Whether this is depth image data.
    pub is_depth: bool,
}

impl ImageData {
    /// Create new RGB image data with validation.
    ///
    /// Returns an error if data size doesn't match expected RGB size (width * height * 3).
    /// For encoded data (JPEG/PNG), use `encoded()` instead.
    ///
    /// # Errors
    ///
    /// Returns an error if the data size doesn't match width * height * 3.
    pub fn new_rgb(
        width: u32,
        height: u32,
        data: Vec<u8>,
    ) -> std::result::Result<Self, ImageDataError> {
        let expected_size = (width as usize) * (height as usize) * 3;
        if data.len() != expected_size {
            return Err(ImageDataError::SizeMismatch {
                width,
                height,
                expected_size,
                actual_size: data.len(),
            });
        }
        Ok(Self {
            width,
            height,
            data,
            original_timestamp: 0,
            is_encoded: false,
            is_depth: false,
        })
    }

    /// Create new image data.
    ///
    /// Validates that the data size matches expected RGB size (width * height * 3).
    /// Logs a warning if size doesn't match, but continues anyway.
    /// For encoded data (JPEG/PNG), use `encoded()` instead.
    ///
    /// For production use, prefer `new_rgb()` which returns a Result.
    pub fn new(width: u32, height: u32, data: Vec<u8>) -> Self {
        let expected_size = (width as usize) * (height as usize) * 3;
        if data.len() != expected_size {
            tracing::warn!(
                width,
                height,
                expected_size,
                actual_size = data.len(),
                "ImageData data size mismatch for RGB data"
            );
        }
        Self {
            width,
            height,
            data,
            original_timestamp: 0,
            is_encoded: false,
            is_depth: false,
        }
    }

    /// Create new image data with timestamp.
    pub fn with_timestamp(width: u32, height: u32, data: Vec<u8>, timestamp: u64) -> Self {
        let expected_size = (width as usize) * (height as usize) * 3;
        if data.len() != expected_size {
            tracing::warn!(
                width,
                height,
                expected_size,
                actual_size = data.len(),
                "ImageData data size mismatch for RGB data"
            );
        }
        Self {
            width,
            height,
            data,
            original_timestamp: timestamp,
            is_encoded: false,
            is_depth: false,
        }
    }

    /// Create new encoded image data (e.g., JPEG/PNG).
    pub fn encoded(width: u32, height: u32, data: Vec<u8>) -> Self {
        Self {
            width,
            height,
            data,
            original_timestamp: 0,
            is_encoded: true,
            is_depth: false,
        }
    }

    /// Create new depth image data.
    pub fn depth(width: u32, height: u32, data: Vec<u8>) -> Self {
        Self {
            width,
            height,
            data,
            original_timestamp: 0,
            is_encoded: false,
            is_depth: true,
        }
    }

    /// Validate image data consistency.
    ///
    /// Returns `true` if the data size matches the expected size for the given format.
    pub fn validate(&self) -> bool {
        if self.is_encoded {
            // For encoded data, we can't validate size without decoding
            true
        } else {
            self.data.len() == self.rgb_size()
        }
    }

    /// Get the number of pixels.
    pub fn pixel_count(&self) -> usize {
        (self.width as usize) * (self.height as usize)
    }

    /// Get the expected RGB data size.
    pub fn rgb_size(&self) -> usize {
        self.pixel_count() * 3
    }

    /// Check if this is valid RGB data (not encoded).
    pub fn is_rgb(&self) -> bool {
        !self.is_encoded && self.data.len() == self.rgb_size()
    }
}

/// Camera calibration information extracted from sensor_msgs/CameraInfo.
///
/// Contains intrinsic parameters needed for camera calibration in dataset formats.
#[derive(Debug, Clone)]
pub struct CameraInfo {
    /// Camera name/identifier
    pub camera_name: String,
    /// Image width
    pub width: u32,
    /// Image height
    pub height: u32,
    /// K matrix (3x3 row-major): [fx, 0, cx, 0, fy, cy, 0, 0, 1]
    pub k: [f64; 9],
    /// D vector (distortion coefficients): [k1, k2, t1, t2, k3]
    pub d: Vec<f64>,
    /// R matrix (3x3 row-major rectification matrix)
    pub r: Option<[f64; 9]>,
    /// P matrix (3x4 row-major projection matrix)
    pub p: Option<[f64; 12]>,
    /// Distortion model name (e.g., "plumb_bob", "rational_polynomial")
    pub distortion_model: String,
}

/// Audio data with metadata.
///
/// Used for datasets that include audio observations.
#[derive(Debug, Clone)]
pub struct AudioData {
    /// Audio samples (interleaved if multi-channel).
    pub samples: Vec<f32>,

    /// Sample rate in Hz.
    pub sample_rate: u32,

    /// Number of channels (1 = mono, 2 = stereo).
    pub channels: u8,

    /// Original timestamp from the message.
    pub original_timestamp: i64,
}

impl AudioData {
    /// Create new audio data.
    pub fn new(samples: Vec<f32>, sample_rate: u32, channels: u8, original_timestamp: i64) -> Self {
        if sample_rate == 0 {
            tracing::warn!("AudioData created with sample_rate=0, duration() will return 0.0");
        }
        if channels == 0 {
            tracing::warn!("AudioData created with channels=0, frames() will return 0");
        }
        if channels > 0 && !samples.len().is_multiple_of(channels as usize) {
            tracing::warn!(
                sample_count = samples.len(),
                channels,
                remainder = samples.len() % channels as usize,
                "AudioData samples not evenly divisible by channels"
            );
        }
        Self {
            samples,
            sample_rate,
            channels,
            original_timestamp,
        }
    }

    /// Validate audio data consistency.
    ///
    /// Returns `true` if the data is valid (sample_rate > 0, channels > 0,
    /// and samples evenly divisible by channels).
    pub fn validate(&self) -> bool {
        if self.sample_rate == 0 || self.channels == 0 {
            return false;
        }
        self.samples.len().is_multiple_of(self.channels as usize)
    }

    /// Get duration in seconds.
    pub fn duration(&self) -> f64 {
        if self.sample_rate > 0 && self.channels > 0 {
            self.samples.len() as f64 / (self.sample_rate as f64 * self.channels as f64)
        } else {
            0.0
        }
    }

    /// Get number of frames (samples per channel).
    pub fn frames(&self) -> usize {
        if self.channels > 0 {
            self.samples.len() / self.channels as usize
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_image_data_new() {
        let data = vec![0u8; 640 * 480 * 3];
        let img = ImageData::new(640, 480, data);

        assert_eq!(img.width, 640);
        assert_eq!(img.height, 480);
        assert_eq!(img.pixel_count(), 640 * 480);
        assert!(img.is_rgb());
        assert!(!img.is_encoded);
    }

    #[test]
    fn test_image_data_encoded() {
        let data = vec![0u8; 1000]; // Smaller than RGB size
        let img = ImageData::encoded(640, 480, data);

        assert!(img.is_encoded);
        assert!(!img.is_rgb());
    }

    #[test]
    fn test_audio_data() {
        let samples = vec![1.0, 2.0, 3.0, 4.0];
        let audio = AudioData::new(samples, 16000, 1, 0);

        assert_eq!(audio.frames(), 4);
        assert!((audio.duration() - 0.00025).abs() < 0.0001);
    }

    #[test]
    fn test_image_data_validate() {
        // Valid RGB data
        let data = vec![0u8; 640 * 480 * 3];
        let img = ImageData::new(640, 480, data);
        assert!(img.validate());

        // Invalid RGB data (wrong size)
        let data = vec![0u8; 1000];
        let img = ImageData::new(640, 480, data);
        assert!(!img.validate());

        // Encoded data always validates
        let data = vec![0u8; 1000];
        let img = ImageData::encoded(640, 480, data);
        assert!(img.validate());
    }

    #[test]
    fn test_audio_data_validate() {
        // Valid audio data
        let samples = vec![1.0, 2.0, 3.0, 4.0];
        let audio = AudioData::new(samples, 16000, 2, 0);
        assert!(audio.validate());

        // Invalid: zero sample_rate
        let samples = vec![1.0, 2.0, 3.0, 4.0];
        let audio = AudioData::new(samples, 0, 2, 0);
        assert!(!audio.validate());

        // Invalid: zero channels
        let samples = vec![1.0, 2.0, 3.0, 4.0];
        let audio = AudioData::new(samples, 16000, 0, 0);
        assert!(!audio.validate());

        // Invalid: samples not divisible by channels
        let samples = vec![1.0, 2.0, 3.0]; // 3 samples, 2 channels = incomplete frame
        let audio = AudioData::new(samples, 16000, 2, 0);
        assert!(!audio.validate());
    }

    #[test]
    fn test_audio_data_zero_channels_duration() {
        // Test that duration() handles zero channels gracefully
        let samples = vec![1.0, 2.0, 3.0, 4.0];
        let audio = AudioData::new(samples, 16000, 0, 0);
        assert_eq!(audio.duration(), 0.0);
        assert_eq!(audio.frames(), 0);
    }

    #[test]
    fn test_image_data_depth() {
        let img = ImageData::depth(640, 480, vec![0u8; 640 * 480 * 2]);
        assert!(img.is_depth);
        assert!(!img.is_encoded);
    }

    #[test]
    fn test_image_data_with_timestamp() {
        let img = ImageData::with_timestamp(640, 480, vec![0u8; 640 * 480 * 3], 123456);
        assert_eq!(img.original_timestamp, 123456);
    }

    #[test]
    fn test_image_data_new_rgb_success() {
        let result = ImageData::new_rgb(2, 2, vec![0u8; 12]);
        assert!(result.is_ok());
        let img = result.unwrap();
        assert_eq!(img.width, 2);
        assert_eq!(img.height, 2);
    }

    #[test]
    fn test_image_data_new_rgb_error() {
        let result = ImageData::new_rgb(2, 2, vec![0u8; 6]); // Wrong size
        assert!(result.is_err());
        if let Err(ImageDataError::SizeMismatch {
            width,
            height,
            expected_size,
            actual_size,
        }) = result
        {
            assert_eq!(width, 2);
            assert_eq!(height, 2);
            assert_eq!(expected_size, 12);
            assert_eq!(actual_size, 6);
        }
    }

    #[test]
    fn test_audio_data_stereo() {
        let samples = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]; // 3 stereo frames
        let audio = AudioData::new(samples, 44100, 2, 0);
        assert_eq!(audio.frames(), 3);
        assert!((audio.duration() - (3.0 / 44100.0)).abs() < 0.0001);
    }
}
