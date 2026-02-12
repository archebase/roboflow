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
pub trait ImageDecoderBackend: Send + Sync + std::fmt::Debug {
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
    ///
    /// # Panics
    ///
    /// Panics in debug mode if data size doesn't match expected size for RGB.
    pub fn new(width: u32, height: u32, data: Vec<u8>) -> Self {
        #[cfg(debug_assertions)]
        {
            let expected = (width as usize) * (height as usize) * 3;
            assert_eq!(
                data.len(),
                expected,
                "Data size {} doesn't match expected {} for {}x{} RGB image",
                data.len(),
                expected,
                width,
                height
            );
        }
        Self {
            width,
            height,
            data,
        }
    }

    /// Create a new decoded image from RGB8 data with explicit dimensions.
    ///
    /// This is the preferred way to create DecodedImage from raw RGB8 data
    /// where dimensions come from message metadata rather than being derived.
    ///
    /// # Arguments
    ///
    /// * `width` - Image width in pixels (from metadata)
    /// * `height` - Image height in pixels (from metadata)
    /// * `data` - RGB pixel data (must be width * height * 3 bytes)
    ///
    /// # Returns
    ///
    /// Returns an error if the data size doesn't match the expected size.
    pub fn from_rgb8(width: u32, height: u32, data: Vec<u8>) -> Result<Self> {
        let expected_size = (width as usize) * (height as usize) * 3;
        if data.len() != expected_size {
            return Err(ImageError::InvalidData(format!(
                "RGB8 data size {} doesn't match expected {} for {}x{} image",
                data.len(),
                expected_size,
                width,
                height
            )));
        }
        Ok(Self {
            width,
            height,
            data,
        })
    }

    /// Get the image width in pixels.
    pub fn width(&self) -> u32 {
        self.width
    }

    /// Get the image height in pixels.
    pub fn height(&self) -> u32 {
        self.height
    }

    /// Get a reference to the RGB pixel data.
    pub fn data(&self) -> &[u8] {
        &self.data
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
#[derive(Debug)]
pub struct CpuImageDecoder {
    memory_strategy: MemoryStrategy,
    _threads: usize, // Stored for future rayon thread pool configuration
}

impl CpuImageDecoder {
    /// Create a new CPU decoder with the given memory strategy.
    pub fn new(memory_strategy: MemoryStrategy, threads: usize) -> Self {
        Self {
            memory_strategy,
            _threads: threads.max(1),
        }
    }

    /// Create a CPU decoder with default settings.
    pub fn default_config() -> Self {
        Self {
            memory_strategy: MemoryStrategy::default(),
            _threads: rayon::current_num_threads().max(1),
        }
    }
}

impl ImageDecoderBackend for CpuImageDecoder {
    fn decode(&self, data: &[u8], format: ImageFormat) -> Result<DecodedImage> {
        match format {
            ImageFormat::Jpeg => self.decode_jpeg(data),
            ImageFormat::Png => self.decode_png(data),
            ImageFormat::Rgb8 | ImageFormat::Bgr8 => {
                // Already RGB/BGR, but we need explicit dimensions from metadata.
                // The previous sqrt() approach was incorrect for non-square images.
                // Return an error directing the caller to provide dimensions explicitly.
                Err(ImageError::InvalidData(
                    "RGB8/BGR8 format requires explicit width/height from message metadata. \
                     Use DecodedImage::new_with_dimensions() or extract dimensions from the ROS message.".to_string()
                ))
            }
            ImageFormat::Gray8 => {
                Err(ImageError::InvalidData(
                    "Gray8 format requires explicit width/height from message metadata. \
                     Use DecodedImage::new_with_dimensions() or extract dimensions from the ROS message.".to_string()
                ))
            }
            ImageFormat::Unknown => Err(ImageError::UnsupportedFormat(
                "Unknown format (cannot detect from magic bytes)".to_string(),
            )),
        }
    }

    fn decoder_type(&self) -> DecoderType {
        DecoderType::Cpu
    }

    fn memory_strategy(&self) -> MemoryStrategy {
        self.memory_strategy
    }
}

impl CpuImageDecoder {
    fn decode_jpeg(&self, data: &[u8]) -> Result<DecodedImage> {
        let cursor = Cursor::new(data);
        let decoder = image::codecs::jpeg::JpegDecoder::new(cursor)
            .map_err(|e| ImageError::DecodeFailed(format!("JPEG decoder init: {}", e)))?;

        self.decode_with_image_decoder(decoder, "JPEG")
    }

    fn decode_png(&self, data: &[u8]) -> Result<DecodedImage> {
        let cursor = Cursor::new(data);
        let decoder = image::codecs::png::PngDecoder::new(cursor)
            .map_err(|e| ImageError::DecodeFailed(format!("PNG decoder init: {}", e)))?;

        self.decode_with_image_decoder(decoder, "PNG")
    }

    /// Decode using any ImageDecoder and convert output to RGB8.
    ///
    /// Handles non-RGB formats (e.g. L8, L16, La8, Rgba8) by converting to RGB.
    /// This fixes panics when decoding compressedDepth (16-bit PNG) or grayscale images.
    fn decode_with_image_decoder<D>(&self, decoder: D, format_name: &str) -> Result<DecodedImage>
    where
        D: image::ImageDecoder,
    {
        let dimensions = decoder.dimensions();
        let width = dimensions.0;
        let height = dimensions.1;
        let color_type = decoder.color_type();
        let total_bytes = decoder.total_bytes() as usize;

        // Allocate for raw decode output
        let mut raw_data = allocate(total_bytes, self.memory_strategy).data;

        decoder
            .read_image(&mut raw_data)
            .map_err(|e| ImageError::DecodeFailed(format!("{} decode: {}", format_name, e)))?;

        // Convert to RGB8 if needed (handles L16, L8, La8, Rgba8, etc.)
        let rgb_data = raw_to_rgb8(width, height, &raw_data, color_type)?;

        Ok(DecodedImage::new(width, height, rgb_data))
    }
}

/// Convert raw decoded image buffer to RGB8 based on color type.
///
/// Handles formats that produce different byte layouts (e.g. 16-bit depth PNG)
/// so the pipeline can assume RGB for video encoding.
fn raw_to_rgb8(
    width: u32,
    height: u32,
    raw: &[u8],
    color_type: image::ColorType,
) -> Result<Vec<u8>> {
    use image::ColorType;

    let pixel_count = (width as usize) * (height as usize);
    let expected_rgb_size = pixel_count * 3;

    let rgb_data = match color_type {
        ColorType::Rgb8 => {
            if raw.len() != expected_rgb_size {
                return Err(ImageError::InvalidData(format!(
                    "Rgb8 data size {} doesn't match expected {} for {}x{} image",
                    raw.len(),
                    expected_rgb_size,
                    width,
                    height
                )));
            }
            raw.to_vec()
        }
        ColorType::L8 => {
            // 1 byte per pixel grayscale -> replicate to RGB
            let mut rgb = Vec::with_capacity(expected_rgb_size);
            for &g in raw {
                rgb.push(g);
                rgb.push(g);
                rgb.push(g);
            }
            rgb
        }
        ColorType::La8 => {
            // 2 bytes per pixel (L, A) -> use L for RGB
            let mut rgb = Vec::with_capacity(expected_rgb_size);
            for chunk in raw.chunks_exact(2) {
                let g = chunk[0];
                rgb.push(g);
                rgb.push(g);
                rgb.push(g);
            }
            rgb
        }
        ColorType::L16 => {
            // 2 bytes per pixel 16-bit grayscale (native endian) -> scale to 8-bit, replicate to RGB
            let mut rgb = Vec::with_capacity(expected_rgb_size);
            for chunk in raw.chunks_exact(2) {
                let v = u16::from_ne_bytes([chunk[0], chunk[1]]);
                let g = (v >> 8) as u8; // use high byte for 8-bit
                rgb.push(g);
                rgb.push(g);
                rgb.push(g);
            }
            rgb
        }
        ColorType::La16 => {
            // 4 bytes per pixel (L16, A16) -> use L high byte for RGB
            let mut rgb = Vec::with_capacity(expected_rgb_size);
            for chunk in raw.chunks_exact(4) {
                let v = u16::from_ne_bytes([chunk[0], chunk[1]]);
                let g = (v >> 8) as u8;
                rgb.push(g);
                rgb.push(g);
                rgb.push(g);
            }
            rgb
        }
        ColorType::Rgba8 => {
            // 4 bytes per pixel -> drop alpha
            let mut rgb = Vec::with_capacity(expected_rgb_size);
            for chunk in raw.chunks_exact(4) {
                rgb.push(chunk[0]);
                rgb.push(chunk[1]);
                rgb.push(chunk[2]);
            }
            rgb
        }
        ColorType::Rgb16 => {
            // 6 bytes per pixel -> scale to 8-bit
            let mut rgb = Vec::with_capacity(expected_rgb_size);
            for chunk in raw.chunks_exact(6) {
                let r = u16::from_ne_bytes([chunk[0], chunk[1]]);
                let g = u16::from_ne_bytes([chunk[2], chunk[3]]);
                let b = u16::from_ne_bytes([chunk[4], chunk[5]]);
                rgb.push((r >> 8) as u8);
                rgb.push((g >> 8) as u8);
                rgb.push((b >> 8) as u8);
            }
            rgb
        }
        ColorType::Rgba16 => {
            // 8 bytes per pixel -> scale to 8-bit, drop alpha
            let mut rgb = Vec::with_capacity(expected_rgb_size);
            for chunk in raw.chunks_exact(8) {
                let r = u16::from_ne_bytes([chunk[0], chunk[1]]);
                let g = u16::from_ne_bytes([chunk[2], chunk[3]]);
                let b = u16::from_ne_bytes([chunk[4], chunk[5]]);
                rgb.push((r >> 8) as u8);
                rgb.push((g >> 8) as u8);
                rgb.push((b >> 8) as u8);
            }
            rgb
        }
        ColorType::Rgb32F | ColorType::Rgba32F => {
            return Err(ImageError::UnsupportedFormat(format!(
                "32-bit float color type {:?} not supported for RGB conversion",
                color_type
            )));
        }
        _ => {
            return Err(ImageError::UnsupportedFormat(format!(
                "Color type {:?} not supported for RGB conversion",
                color_type
            )));
        }
    };

    debug_assert_eq!(rgb_data.len(), expected_rgb_size);
    Ok(rgb_data)
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

        // Invalid size - use from_rgb8 which returns Result instead of panicking
        let result = DecodedImage::from_rgb8(2, 2, vec![0u8; 10]);
        assert!(result.is_err());
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

    #[test]
    fn test_decoded_image_from_rgb8_valid() {
        let result = DecodedImage::from_rgb8(100, 50, vec![0u8; 100 * 50 * 3]);
        assert!(result.is_ok());
        let img = result.unwrap();
        assert_eq!(img.width, 100);
        assert_eq!(img.height, 50);
        assert_eq!(img.data.len(), 100 * 50 * 3);
    }

    #[test]
    fn test_decoded_image_from_rgb8_invalid_size() {
        // Data size doesn't match dimensions
        let result = DecodedImage::from_rgb8(100, 50, vec![0u8; 100]);
        assert!(result.is_err());
        match result {
            Err(ImageError::InvalidData(msg)) => {
                assert!(msg.contains("doesn't match expected"));
            }
            _ => panic!("Expected InvalidData error"),
        }
    }

    #[test]
    fn test_decode_jpeg_truncated() {
        let decoder = CpuImageDecoder::default_config();

        // Truncated JPEG (missing EOI marker and most of the file)
        let truncated_jpeg = [0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10];

        let result = decoder.decode(&truncated_jpeg, ImageFormat::Jpeg);
        // Should return an error, not panic
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_invalid_jpeg_magic_bytes() {
        let decoder = CpuImageDecoder::default_config();

        // Invalid JPEG data (wrong magic bytes)
        let invalid_jpeg = [0x00, 0x00, 0x00, 0x00];

        let result = decoder.decode(&invalid_jpeg, ImageFormat::Jpeg);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_unknown_format_returns_error() {
        let decoder = CpuImageDecoder::default_config();

        // Empty data with unknown format
        let result = decoder.decode(&[], ImageFormat::Unknown);
        assert!(matches!(result, Err(ImageError::UnsupportedFormat(_))));
    }

    #[test]
    fn test_decode_rgb8_requires_explicit_dimensions() {
        let decoder = CpuImageDecoder::default_config();

        // RGB8 data without explicit dimensions should fail
        let rgb_data = vec![0u8; 300]; // 10x10 RGB image

        let result = decoder.decode(&rgb_data, ImageFormat::Rgb8);
        assert!(matches!(result, Err(ImageError::InvalidData(_))));
    }

    #[test]
    fn test_decode_l16_png_converts_to_rgb() {
        // 16-bit grayscale PNG (2 bytes per pixel) - simulates compressedDepth output.
        // 2x2 L16: 4 pixels * 2 bytes = 8 bytes raw.
        // Native-endian: pixel 0 = 0x0100 (256), pixel 1 = 0x0200 (512), etc.
        let mut png_data = Vec::new();
        use image::ImageEncoder;
        let raw_l16: Vec<u8> = [0x00, 0x01, 0x00, 0x02, 0x00, 0x03, 0x00, 0x04].to_vec(); // 2x2 L16 image
        image::codecs::png::PngEncoder::new(&mut png_data)
            .write_image(&raw_l16, 2, 2, image::ExtendedColorType::L16)
            .expect("write test PNG");

        let decoder = CpuImageDecoder::default_config();
        let result = decoder.decode(&png_data, ImageFormat::Png);
        assert!(result.is_ok(), "L16 PNG should decode: {:?}", result);
        let decoded = result.unwrap();
        assert_eq!(decoded.width, 2);
        assert_eq!(decoded.height, 2);
        assert_eq!(decoded.data.len(), 12); // 2*2*3 RGB
        // L16 values 256,512,768,1024 (native endian) -> high byte 1,2,3,4 -> RGB replicated
        assert_eq!(decoded.data, vec![1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4]);
    }
}
