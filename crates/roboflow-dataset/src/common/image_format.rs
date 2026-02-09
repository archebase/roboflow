// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Image format detection and classification.
//!
//! This module provides utilities to detect image formats from raw bytes.
//! Used for optimizing the encoding pipeline by enabling JPEG passthrough
//! and other format-specific optimizations.

/// Image format category for encoding strategy selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageFormat {
    /// JPEG-encoded image (can use passthrough optimization)
    Jpeg,
    /// PNG-encoded image
    Png,
    /// Raw RGB8 data (3 bytes per pixel)
    RawRgb8,
    /// Raw BGR8 data (3 bytes per pixel)
    RawBgr8,
    /// Raw grayscale data (1 byte per pixel)
    RawGray8,
    /// Unknown format - requires decoding
    Unknown,
}

impl ImageFormat {
    /// Check if this format is already encoded (JPEG/PNG).
    pub fn is_encoded(self) -> bool {
        matches!(self, Self::Jpeg | Self::Png)
    }

    /// Check if this format can use passthrough encoding.
    pub fn supports_passthrough(self) -> bool {
        matches!(self, Self::Jpeg)
    }
}

/// Detect if image data is JPEG-encoded.
///
/// JPEG files start with the magic bytes: FF D8 FF
/// This is a quick check without full decoding.
pub fn detect_jpeg(data: &[u8]) -> bool {
    data.len() >= 4 && data[0] == 0xFF && data[1] == 0xD8 && data[2] == 0xFF
}

/// Detect if image data is PNG-encoded.
///
/// PNG files start with the magic bytes: 89 50 4E 47 (the PNG signature)
pub fn detect_png(data: &[u8]) -> bool {
    data.len() >= 8
        && data[0] == 0x89
        && data[1] == 0x50
        && data[2] == 0x4E
        && data[3] == 0x47
        && data[4] == 0x0D
        && data[5] == 0x0A
        && data[6] == 0x1A
        && data[7] == 0x0A
}

/// Detect the image format from raw bytes.
pub fn detect_image_format(data: &[u8]) -> ImageFormat {
    if detect_jpeg(data) {
        return ImageFormat::Jpeg;
    }
    if detect_png(data) {
        return ImageFormat::Png;
    }
    // For raw formats, we need additional context (width, height)
    // to distinguish between RGB8, BGR8, and Gray8
    ImageFormat::Unknown
}

/// Detect image format when dimensions are known.
///
/// This allows distinguishing between raw formats based on expected data size.
pub fn detect_image_format_with_size(data: &[u8], width: u32, height: u32) -> ImageFormat {
    // First check for encoded formats
    if detect_jpeg(data) {
        return ImageFormat::Jpeg;
    }
    if detect_png(data) {
        return ImageFormat::Png;
    }

    let pixel_count = (width * height) as usize;
    let data_len = data.len();

    // Match data size to expected sizes for different formats
    match data_len {
        len if len == pixel_count * 3 => ImageFormat::RawRgb8,
        len if len == pixel_count => ImageFormat::RawGray8,
        _ => ImageFormat::Unknown,
    }
}

/// Check if the image data is likely JPEG-encoded for passthrough.
pub fn can_passthrough(data: &[u8]) -> bool {
    detect_jpeg(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_jpeg() {
        // JPEG magic bytes: FF D8 FF
        let jpeg_header = [0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46];
        assert!(detect_jpeg(&jpeg_header));

        // Not JPEG
        let not_jpeg = [0x00, 0x00, 0x00, 0x00];
        assert!(!detect_jpeg(&not_jpeg));

        // Too short
        let too_short = [0xFF, 0xD8];
        assert!(!detect_jpeg(&too_short));
    }

    #[test]
    fn test_detect_png() {
        // PNG signature: 89 50 4E 47 0D 0A 1A 0A
        let png_header = [
            0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x01,
        ];
        assert!(detect_png(&png_header));

        // Not PNG
        let not_png = [0x00, 0x00, 0x00, 0x00];
        assert!(!detect_png(&not_png));

        // Too short
        let too_short = [0x89, 0x50, 0x4E, 0x47];
        assert!(!detect_png(&too_short));
    }

    #[test]
    fn test_detect_image_format() {
        let jpeg_header = [0xFF, 0xD8, 0xFF, 0xE0];
        assert_eq!(detect_image_format(&jpeg_header), ImageFormat::Jpeg);

        let png_header = [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
        assert_eq!(detect_image_format(&png_header), ImageFormat::Png);

        let unknown = [0x00, 0x01, 0x02, 0x03];
        assert_eq!(detect_image_format(&unknown), ImageFormat::Unknown);
    }

    #[test]
    fn test_detect_image_format_with_size() {
        // JPEG should still be detected
        let jpeg_header = [0xFF, 0xD8, 0xFF, 0xE0];
        assert_eq!(
            detect_image_format_with_size(&jpeg_header, 640, 480),
            ImageFormat::Jpeg
        );

        // Raw RGB8: 640 * 480 * 3 = 921600 bytes
        let rgb_data = vec![0u8; 640 * 480 * 3];
        assert_eq!(
            detect_image_format_with_size(&rgb_data, 640, 480),
            ImageFormat::RawRgb8
        );

        // Raw grayscale: 640 * 480 = 307200 bytes
        let gray_data = vec![0u8; 640 * 480];
        assert_eq!(
            detect_image_format_with_size(&gray_data, 640, 480),
            ImageFormat::RawGray8
        );
    }

    #[test]
    fn test_can_passthrough() {
        let jpeg_header = [0xFF, 0xD8, 0xFF, 0xE0];
        assert!(can_passthrough(&jpeg_header));

        let png_header = [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
        assert!(!can_passthrough(&png_header));

        let raw_data = [0u8; 100];
        assert!(!can_passthrough(&raw_data));
    }

    #[test]
    fn test_image_format_is_encoded() {
        assert!(ImageFormat::Jpeg.is_encoded());
        assert!(ImageFormat::Png.is_encoded());
        assert!(!ImageFormat::RawRgb8.is_encoded());
        assert!(!ImageFormat::RawGray8.is_encoded());
    }

    #[test]
    fn test_image_format_supports_passthrough() {
        assert!(ImageFormat::Jpeg.supports_passthrough());
        assert!(!ImageFormat::Png.supports_passthrough());
        assert!(!ImageFormat::RawRgb8.supports_passthrough());
    }
}
