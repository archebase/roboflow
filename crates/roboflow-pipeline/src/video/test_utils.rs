// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Shared test utilities for benchmarks and examples.

use std::io::Cursor;

use crate::video::config::VideoEncoderConfig;
use crate::video::decode::DecodedFrame;
use crate::video::frame::FrameBuffer;
use crate::video::hardware::select_best_encoder;
use crate::video::hardware::EncoderChoice;
use crate::video::ImageData;

// ================================================================================================
// Helper Functions
// ================================================================================================

/// Create a minimal valid JPEG image for benchmarking.
pub fn create_synthetic_jpeg(width: u32, height: u32) -> Vec<u8> {
    let rgb_data: Vec<u8> = (0..width * height * 3)
        .map(|i| ((i * 17) % 256) as u8)
        .collect();

    let img = image::RgbImage::from_raw(width, height, rgb_data).expect("Invalid dimensions");
    let mut jpeg_buf = Cursor::new(Vec::new());
    img.write_to(&mut jpeg_buf, image::ImageFormat::Jpeg)
        .expect("JPEG encoding failed");
    jpeg_buf.into_inner()
}

/// Create a synthetic decoded frame for encoding benchmarks.
pub fn create_decoded_frame(width: u32, height: u32) -> DecodedFrame {
    let rgb_size = (width * height * 3) as usize;
    let rgb_data: Vec<u8> = (0..rgb_size).map(|i| (i % 256) as u8).collect();
    DecodedFrame::new(FrameBuffer::rgb24(width, height, rgb_data))
}

/// Create synthetic JPEG image data wrapped in ImageData.
pub fn create_encoded_image(width: u32, height: u32) -> ImageData {
    let jpeg_data = create_synthetic_jpeg(width, height);
    ImageData::encoded(width, height, jpeg_data)
}

/// Get optimal worker count based on CPU cores.
pub fn optimal_worker_count() -> usize {
    num_cpus::get()
}

/// Get hardware-accelerated video config.
pub fn hardware_video_config() -> VideoEncoderConfig {
    let best_encoder = select_best_encoder();
    let codec = match best_encoder {
        EncoderChoice::Nvenc => "h264_nvenc".to_string(),
        EncoderChoice::VideoToolbox => "h264_videotoolbox".to_string(),
        _ => "libx264".to_string(),
    };

    VideoEncoderConfig {
        codec,
        pixel_format: "yuv420p".to_string(),
        fps: 30,
        crf: 23,
        preset: "fast".to_string(),
    }
}

// ================================================================================================
// Constants
// ================================================================================================

/// Common camera names used in benchmarks.
pub const BENCHMARK_CAMERAS: &[&str] = &["cam0", "cam1", "cam2", "cam3"];

/// Common resolution configurations.
#[derive(Debug, Clone, Copy)]
pub struct Resolution {
    pub width: u32,
    pub height: u32,
    pub name: &'static str,
}

impl Resolution {
    pub const VGA: Self = Resolution {
        width: 640,
        height: 480,
        name: "640x480 (VGA)",
    };

    pub const HD: Self = Resolution {
        width: 1280,
        height: 720,
        name: "1280x720 (HD)",
    };

    pub const FHD: Self = Resolution {
        width: 1920,
        height: 1080,
        name: "1920x1080 (FHD)",
    };
}

// ================================================================================================
// Tests
// ================================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_synthetic_jpeg() {
        let jpeg = create_synthetic_jpeg(64, 64);
        assert!(jpeg.len() < 64 * 64 * 3);
        assert_eq!(jpeg[0], 0xFF);
        assert_eq!(jpeg[1], 0xD8);
    }

    #[test]
    fn test_create_decoded_frame() {
        let frame = create_decoded_frame(64, 64);
        assert_eq!(frame.width(), 64);
        assert_eq!(frame.height(), 64);
    }

    #[test]
    fn test_create_encoded_image() {
        let img = create_encoded_image(64, 64);
        assert_eq!(img.width, 64);
        assert_eq!(img.height, 64);
        assert!(img.is_encoded);
    }
}
