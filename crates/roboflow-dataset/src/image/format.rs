// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Image format detection utilities.
//!
//! This module provides format detection from:
//! - Magic bytes (file headers)
//! - ROS format strings ("jpeg", "png", "rgb8", etc.)
//! - Dimension extraction from JPEG/PNG headers without full decode

/// Image format identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ImageFormat {
    /// Uncompressed RGB8 data (3 bytes per pixel)
    Rgb8,
    /// Uncompressed BGR8 data (3 bytes per pixel)
    Bgr8,
    /// Uncompressed grayscale data (1 byte per pixel)
    Gray8,
    /// JPEG compressed image
    Jpeg,
    /// PNG compressed image
    Png,
    /// Unknown format (detection failed)
    #[default]
    Unknown,
}

impl ImageFormat {
    /// Detect format from a ROS format string.
    ///
    /// ROS CompressedImage uses format strings like "jpeg", "png", "rgb8", "avi".
    pub fn from_ros_format(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "rgb8" | "rgba8" => Self::Rgb8,
            "bgr8" | "bgra8" => Self::Bgr8,
            "mono8" | "gray8" => Self::Gray8,
            "jpeg" | "jpg" | "jpe" | "jfif" => Self::Jpeg,
            "png" => Self::Png,
            // Some cameras use "avi" or "h264" but actually send JPEG
            "avi" | "h264" | "hevc" => Self::Jpeg,
            _ => Self::Unknown,
        }
    }

    /// Detect format from magic bytes (file header).
    ///
    /// - JPEG: FF D8 FF
    /// - PNG: 89 50 4E 47 ("\x89PNG")
    pub fn from_magic_bytes(data: &[u8]) -> Self {
        if data.len() < 4 {
            return Self::Unknown;
        }

        // JPEG: FF D8 FF
        if data[0] == 0xFF && data[1] == 0xD8 && data[2] == 0xFF {
            return Self::Jpeg;
        }

        // PNG: 89 50 4E 47 (..PNG)
        if data[0] == 0x89 && data[1] == 0x50 && data[2] == 0x4E && data[3] == 0x47 {
            return Self::Png;
        }

        Self::Unknown
    }

    /// Detect format combining both ROS format string and magic bytes.
    ///
    /// Magic bytes are prioritized as they're more reliable.
    pub fn detect(data: &[u8], ros_format: &str) -> Self {
        let from_magic = Self::from_magic_bytes(data);
        if from_magic != Self::Unknown {
            return from_magic;
        }
        Self::from_ros_format(ros_format)
    }

    /// Check if this format is compressed (requires decoding).
    pub fn is_compressed(&self) -> bool {
        matches!(self, Self::Jpeg | Self::Png)
    }

    /// Check if this format is already encoded (JPEG/PNG).
    pub fn is_encoded(&self) -> bool {
        matches!(self, Self::Jpeg | Self::Png)
    }

    /// Check if this format can use passthrough encoding (JPEG only).
    pub fn supports_passthrough(&self) -> bool {
        matches!(self, Self::Jpeg)
    }

    /// Extract dimensions from JPEG header without full decode.
    ///
    /// JPEG SOF (Start of Frame) markers contain dimensions:
    /// - SOF0 (0xFF 0xC0): Baseline DCT
    /// - SOF2 (0xFF 0xC2): Progressive DCT
    pub fn jpeg_dimensions(data: &[u8]) -> Option<(u32, u32)> {
        if data.len() < 4 {
            return None;
        }

        // Check JPEG magic bytes
        if data[0] != 0xFF || data[1] != 0xD8 {
            return None;
        }

        let mut i = 2;
        while i < data.len().saturating_sub(8) {
            // Find next marker (0xFF)
            if data[i] != 0xFF {
                i += 1;
                continue;
            }

            // Skip padding bytes
            while i < data.len().saturating_sub(1) && data[i + 1] == 0xFF {
                i += 1;
            }

            let marker = data[i + 1];

            // SOF0 (baseline) or SOF2 (progressive) contain dimensions
            if marker == 0xC0 || marker == 0xC2 {
                // Verify we have enough data for dimension extraction
                // Marker (2 bytes) + length (2 bytes) + precision (1 byte) + height (2 bytes) + width (2 bytes)
                if i + 10 > data.len() {
                    return None;
                }

                let height = u16::from_be_bytes([data[i + 5], data[i + 6]]) as u32;
                let width = u16::from_be_bytes([data[i + 7], data[i + 8]]) as u32;
                return Some((width, height));
            }

            // Skip to next marker
            // Length bytes (i+2, i+3) tell us how many bytes to skip
            if i + 4 > data.len() {
                break;
            }
            let length = u16::from_be_bytes([data[i + 2], data[i + 3]]) as usize;
            i += 2 + length;
        }

        None
    }

    /// Extract dimensions from PNG header without full decode.
    ///
    /// PNG IHDR (Image Header) chunk starts at byte 8 and contains dimensions.
    pub fn png_dimensions(data: &[u8]) -> Option<(u32, u32)> {
        // PNG signature: 137 80 78 71 13 10 26 10
        const PNG_SIGNATURE: [u8; 8] = [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];

        if data.len() < 24 {
            return None;
        }

        // Verify PNG signature
        if data[0..8] != PNG_SIGNATURE {
            return None;
        }

        // First chunk should be IHDR at bytes 8-11
        if &data[12..16] != b"IHDR" {
            return None;
        }

        // Width (bytes 16-19) and height (bytes 20-23) are big-endian u32
        let width = u32::from_be_bytes([data[16], data[17], data[18], data[19]]);
        let height = u32::from_be_bytes([data[20], data[21], data[22], data[23]]);

        // Validate dimensions (PNG allows up to 2^31-1)
        if width == 0 || height == 0 {
            return None;
        }

        Some((width, height))
    }

    /// Extract dimensions from header based on format.
    pub fn extract_dimensions(&self, data: &[u8]) -> Option<(u32, u32)> {
        match self {
            Self::Jpeg => Self::jpeg_dimensions(data),
            Self::Png => Self::png_dimensions(data),
            _ => None,
        }
    }
}

// =============================================================================
// Standalone detection functions for convenience
// =============================================================================

/// Detect if image data is JPEG-encoded.
///
/// JPEG files start with the magic bytes: FF D8 FF
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
    ImageFormat::Unknown
}

/// Check if the image data is likely JPEG-encoded for passthrough.
pub fn can_passthrough(data: &[u8]) -> bool {
    detect_jpeg(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_ros_format() {
        assert_eq!(ImageFormat::from_ros_format("jpeg"), ImageFormat::Jpeg);
        assert_eq!(ImageFormat::from_ros_format("JPEG"), ImageFormat::Jpeg);
        assert_eq!(ImageFormat::from_ros_format("jpg"), ImageFormat::Jpeg);
        assert_eq!(ImageFormat::from_ros_format("png"), ImageFormat::Png);
        assert_eq!(ImageFormat::from_ros_format("rgb8"), ImageFormat::Rgb8);
        assert_eq!(ImageFormat::from_ros_format("bgr8"), ImageFormat::Bgr8);
        assert_eq!(ImageFormat::from_ros_format("mono8"), ImageFormat::Gray8);
        assert_eq!(
            ImageFormat::from_ros_format("unknown"),
            ImageFormat::Unknown
        );
        assert_eq!(ImageFormat::from_ros_format("avi"), ImageFormat::Jpeg);
    }

    #[test]
    fn test_from_magic_bytes() {
        // JPEG magic bytes
        let jpeg_header = [0xFF, 0xD8, 0xFF, 0xE0];
        assert_eq!(
            ImageFormat::from_magic_bytes(&jpeg_header),
            ImageFormat::Jpeg
        );

        // PNG magic bytes
        let png_header = [0x89, 0x50, 0x4E, 0x47];
        assert_eq!(ImageFormat::from_magic_bytes(&png_header), ImageFormat::Png);

        // Unknown
        let unknown = [0x00, 0x00, 0x00, 0x00];
        assert_eq!(
            ImageFormat::from_magic_bytes(&unknown),
            ImageFormat::Unknown
        );
    }

    #[test]
    fn test_detect() {
        // JPEG with magic bytes priority
        let jpeg_data = [0xFF, 0xD8, 0xFF, 0xE0];
        assert_eq!(
            ImageFormat::detect(&jpeg_data, "unknown"),
            ImageFormat::Jpeg
        );

        // PNG with magic bytes
        let png_data = [0x89, 0x50, 0x4E, 0x47];
        assert_eq!(ImageFormat::detect(&png_data, "unknown"), ImageFormat::Png);

        // ROS format fallback when magic bytes don't match
        assert_eq!(
            ImageFormat::detect(&[0xFF, 0xD8], "jpeg"),
            ImageFormat::Jpeg
        );
    }

    #[test]
    fn test_jpeg_dimensions() {
        // Minimal JPEG with SOF0 marker
        // FF D8 FF E0 (APP0) + length + "JFIF" + ...
        // FF C0 (SOF0) + length (00 11) + precision (08) + height (00 64) + width (00 48)
        // This represents 100x72 pixels
        let jpeg_with_sof = [
            0xFF, 0xD8, // SOI (Start of Image)
            0xFF, 0xE0, 0x00, 0x10, // APP0 marker + length (16 bytes)
            0x4A, 0x46, 0x49, 0x46, 0x00, 0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00,
            0x00, // JFIF data
            0xFF, 0xC0, // SOF0 marker
            0x00, 0x11, // Length (17 bytes)
            0x08, // Precision (8 bits)
            0x00, 0x64, // Height: 100
            0x00, 0x48, // Width: 72
            // Need padding to reach the required length
            0x01, 0x01, 0x01, 0x01,
        ];

        let result = ImageFormat::jpeg_dimensions(&jpeg_with_sof);
        assert_eq!(result, Some((72, 100))); // width, height
    }

    #[test]
    fn test_png_dimensions() {
        // PNG signature + IHDR chunk with 512x900 dimensions
        let mut png_data = Vec::new();
        png_data.extend_from_slice(&[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]); // signature
        png_data.extend_from_slice(&[0x00, 0x00, 0x00, 0x0D]); // chunk length (13 bytes)
        png_data.extend_from_slice(b"IHDR"); // chunk type
        png_data.extend_from_slice(&[0x00, 0x00, 0x02, 0x00]); // width: 512 (0x0200)
        png_data.extend_from_slice(&[0x00, 0x00, 0x03, 0x84]); // height: 900 (0x0384)
        // Add remaining IHDR data (5 bytes) + CRC (4 bytes) to complete chunk
        png_data.extend_from_slice(&[0x08, 0x02, 0x00, 0x00, 0x00]); // bit depth, color type, etc.
        png_data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // CRC placeholder

        let result = ImageFormat::png_dimensions(&png_data);
        assert_eq!(result, Some((512, 900)));
    }

    #[test]
    fn test_is_compressed() {
        assert!(ImageFormat::Jpeg.is_compressed());
        assert!(ImageFormat::Png.is_compressed());
        assert!(!ImageFormat::Rgb8.is_compressed());
        assert!(!ImageFormat::Unknown.is_compressed());
    }

    #[test]
    fn test_extract_dimensions_jpeg() {
        // JPEG with 500x200 dimensions (height x width)
        let jpeg_with_sof = [
            0xFF, 0xD8, // SOI
            0xFF, 0xE0, 0x00, 0x10, // APP0
            0x4A, 0x46, 0x49, 0x46, 0x00, 0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00,
            0xFF, 0xC0, // SOF0 marker
            0x00, 0x11, // Length
            0x08, // Precision
            0x01, 0xF4, // Height: 500
            0x00, 0xC8, // Width: 200
            0x01, 0x01, 0x01, 0x01, // Padding
        ];

        let result = ImageFormat::Jpeg.extract_dimensions(&jpeg_with_sof);
        assert_eq!(result, Some((200, 500)));
    }
}
