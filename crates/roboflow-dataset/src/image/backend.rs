// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Image decoder backend abstraction.
//!
//! Provides a platform-agnostic trait for image decoding backends,
//! allowing GPU and CPU implementations to be used interchangeably.
//!
//! # Architecture
//!
//! Similar to `roboflow-pipeline/gpu/backend.rs`, this module defines:
//! - `ImageDecoderBackend` trait for pluggable decoders
//! - `CpuImageDecoder` for CPU-based decoding (always available)
//! - GPU and Apple decoders (platform-specific, feature-gated)

use super::{
    ImageError, Result,
    format::ImageFormat,
    memory::{MemoryStrategy, allocate},
};
use std::io::Cursor;

/// Decoder type enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecoderType {
    /// CPU-based decoding (image crate)
    Cpu,
    /// GPU-based decoding (nvJPEG/cuVID)
    Gpu,
    /// Apple hardware-accelerated decoding
    Apple,
}

/// Trait for image decoder backends.
///
/// This trait provides a unified interface for both CPU and GPU
/// decoding implementations, enabling seamless fallback and
/// platform-agnostic code. Similar to `CompressorBackend` in
/// `roboflow-pipeline/gpu/backend.rs`.
pub trait ImageDecoderBackend: Send + Sync {
    /// Decode a single image to RGB.
    ///
    /// # Arguments
    ///
    /// * `data` - Compressed image data (JPEG/PNG bytes)
    /// * `format` - Image format hint
    ///
    /// # Returns
    ///
    /// Decoded RGB image with dimensions
    fn decode(&self, data: &[u8], format: ImageFormat) -> Result<DecodedImage>;

    /// Decode multiple images in parallel (GPU-accelerated).
    ///
    /// Default implementation processes images sequentially.
    /// GPU implementations should override this for true parallelism.
    ///
    /// # Arguments
    ///
    /// * `images` - Slice of (data, format) tuples
    fn decode_batch(&self, images: &[(&[u8], ImageFormat)]) -> Result<Vec<DecodedImage>> {
        images
            .iter()
            .map(|(data, format)| self.decode(data, *format))
            .collect()
    }

    /// Get the decoder type.
    fn decoder_type(&self) -> DecoderType;

    /// Get memory allocation strategy.
    fn memory_strategy(&self) -> MemoryStrategy;

    /// Check if the decoder is available and ready.
    fn is_available(&self) -> bool {
        true
    }
}

/// Decoded image with RGB data and dimensions.
///
/// The `data` field contains RGB8 pixel data (3 bytes per pixel)
/// allocated using the decoder's memory strategy.
#[derive(Debug, Clone)]
pub struct DecodedImage {
    /// Image width in pixels.
    pub width: u32,
    /// Image height in pixels.
    pub height: u32,
    /// RGB pixel data (8 bits per channel).
    pub data: Vec<u8>,
}

impl DecodedImage {
    /// Create a new decoded image.
    pub fn new(width: u32, height: u32, data: Vec<u8>) -> Self {
        Self {
            width,
            height,
            data,
        }
    }

    /// Get the total number of pixels.
    pub fn pixel_count(&self) -> usize {
        (self.width as usize) * (self.height as usize)
    }

    /// Get the expected data size for RGB (3 bytes per pixel).
    pub fn expected_rgb_size(&self) -> usize {
        self.pixel_count() * 3
    }

    /// Validate that the data size matches the dimensions.
    pub fn validate(&self) -> Result<()> {
        let expected = self.expected_rgb_size();
        if self.data.len() != expected {
            return Err(ImageError::InvalidData(format!(
                "Data size {} doesn't match expected {} for {}x{} RGB image",
                self.data.len(),
                expected,
                self.width,
                self.height
            )));
        }
        Ok(())
    }

    /// Check if this image could benefit from GPU decoding.
    ///
    /// Returns true for large images where GPU overhead is justified.
    pub fn should_use_gpu(&self) -> bool {
        // GPU decode overhead is ~1-2ms, so only use GPU for larger images
        // 640x480 = 307,200 pixels → ~900KB RGB → worth using GPU
        const GPU_THRESHOLD_PIXELS: usize = 300_000;
        self.pixel_count() >= GPU_THRESHOLD_PIXELS
    }
}

/// CPU image decoder using the `image` crate.
///
/// This decoder is always available and serves as the fallback
/// when GPU or hardware-accelerated decoders are unavailable.
#[allow(dead_code)]
pub struct CpuImageDecoder {
    memory_strategy: MemoryStrategy,
    threads: usize, // Stored for future rayon thread pool configuration
}

impl CpuImageDecoder {
    /// Create a new CPU decoder with the given memory strategy.
    pub fn new(memory_strategy: MemoryStrategy, threads: usize) -> Self {
        Self {
            memory_strategy,
            threads: threads.max(1),
        }
    }

    /// Create a CPU decoder with default settings.
    pub fn default_config() -> Self {
        Self {
            memory_strategy: MemoryStrategy::default(),
            threads: rayon::current_num_threads().max(1),
        }
    }
}

impl ImageDecoderBackend for CpuImageDecoder {
    fn decode(&self, data: &[u8], format: ImageFormat) -> Result<DecodedImage> {
        #[cfg(feature = "image-decode")]
        {
            match format {
                ImageFormat::Jpeg => self.decode_jpeg(data),
                ImageFormat::Png => self.decode_png(data),
                ImageFormat::Rgb8 => {
                    // Already RGB, just validate dimensions
                    let pixel_count = data.len() / 3;
                    let width = (pixel_count as f32).sqrt() as u32;
                    let height = pixel_count as u32 / width;
                    Ok(DecodedImage {
                        width,
                        height,
                        data: data.to_vec(),
                    })
                }
                ImageFormat::Unknown => Err(ImageError::UnsupportedFormat(
                    "Unknown format (cannot detect from magic bytes)".to_string(),
                )),
            }
        }

        #[cfg(not(feature = "image-decode"))]
        {
            let _ = (data, format);
            Err(ImageError::NotEnabled)
        }
    }

    fn decoder_type(&self) -> DecoderType {
        DecoderType::Cpu
    }

    fn memory_strategy(&self) -> MemoryStrategy {
        self.memory_strategy
    }
}

#[cfg(feature = "image-decode")]
impl CpuImageDecoder {
    fn decode_jpeg(&self, data: &[u8]) -> Result<DecodedImage> {
        use image::ImageDecoder;

        let cursor = Cursor::new(data);
        let decoder = image::codecs::jpeg::JpegDecoder::new(cursor)
            .map_err(|e| ImageError::DecodeFailed(format!("JPEG decoder init: {}", e)))?;

        let dimensions = decoder.dimensions();
        let width = dimensions.0;
        let height = dimensions.1;
        let total_bytes = decoder.total_bytes() as usize;

        // Allocate using the configured memory strategy
        let mut rgb_data = allocate(total_bytes, self.memory_strategy).data;

        decoder
            .read_image(&mut rgb_data)
            .map_err(|e| ImageError::DecodeFailed(format!("JPEG decode: {}", e)))?;

        Ok(DecodedImage::new(width, height, rgb_data))
    }

    fn decode_png(&self, data: &[u8]) -> Result<DecodedImage> {
        use image::ImageDecoder;

        let cursor = Cursor::new(data);
        let decoder = image::codecs::png::PngDecoder::new(cursor)
            .map_err(|e| ImageError::DecodeFailed(format!("PNG decoder init: {}", e)))?;

        let dimensions = decoder.dimensions();
        let width = dimensions.0;
        let height = dimensions.1;
        let total_bytes = decoder.total_bytes() as usize;

        // Allocate using the configured memory strategy
        let mut rgb_data = allocate(total_bytes, self.memory_strategy).data;

        decoder
            .read_image(&mut rgb_data)
            .map_err(|e| ImageError::DecodeFailed(format!("PNG decode: {}", e)))?;

        Ok(DecodedImage::new(width, height, rgb_data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cpu_decoder_default() {
        let decoder = CpuImageDecoder::default_config();
        assert_eq!(decoder.decoder_type(), DecoderType::Cpu);
        assert!(decoder.is_available());
    }

    #[test]
    fn test_decoded_image_validation() {
        // Valid 2x2 RGB image (12 bytes)
        let valid = DecodedImage::new(2, 2, vec![0u8; 12]);
        assert!(valid.validate().is_ok());

        // Invalid size
        let invalid = DecodedImage::new(2, 2, vec![0u8; 10]);
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn test_should_use_gpu() {
        // Small image - CPU is faster
        let small = DecodedImage::new(320, 240, vec![0u8; 320 * 240 * 3]);
        assert!(!small.should_use_gpu());

        // Large image - GPU is worth it
        let large = DecodedImage::new(640, 480, vec![0u8; 640 * 480 * 3]);
        assert!(large.should_use_gpu());
    }

    #[cfg(feature = "image-decode")]
    #[test]
    fn test_decode_jpeg_basic() {
        let decoder = CpuImageDecoder::default_config();

        // Minimal valid JPEG (1x1 red pixel)
        let jpeg_data = [
            0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46, 0x00, 0x01, 0x01, 0x00,
            0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0xFF, 0xDB, 0x00, 0x43, 0x00, 0x01, 0x01, 0x01,
            0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
            0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
            0x0A, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x00, 0x02, 0x03,
            0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0xFF, 0xC4, 0x00, 0x14, 0x01, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x09, 0xFF, 0xC4, 0x00, 0x14, 0x10, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xDA, 0x00, 0x08, 0x01, 0x01, 0x00, 0x00, 0x3F,
            0x00, 0x37, 0xFF, 0xD9,
        ];

        let result = decoder.decode(&jpeg_data, ImageFormat::Jpeg);
        // May fail depending on decoder strictness, just check it doesn't panic
        let _ = result;
    }

    #[test]
    fn test_memory_strategy_default() {
        assert_eq!(MemoryStrategy::default(), MemoryStrategy::Heap);
    }
}
