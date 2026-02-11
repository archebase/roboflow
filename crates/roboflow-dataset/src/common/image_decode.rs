// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Image decoding utilities for compressed formats (JPEG/PNG).
//!
//! This module provides functions to decode compressed image data
//! to RGB format, with multiple fallback strategies for handling
//! various image formats and encodings.

use crate::common::ImageData;
use crate::image::{ImageFormat, decode_compressed_image};

/// JPEG magic: FF D8 FF
const JPEG_MAGIC: &[u8] = &[0xFF, 0xD8, 0xFF];
/// PNG magic: 89 50 4E 47 0D 0A 1A 0A
const PNG_MAGIC: &[u8] = &[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];

/// Decode compressed image (JPEG/PNG) to RGB.
///
/// This function handles images that have been marked as encoded
/// (e.g., from ROS compressed image topics). It tries multiple
/// strategies to handle various serialization formats.
///
/// # Strategies
///
/// 1. Direct decode of raw payload
/// 2. Skip 8-byte ROS CDR header
/// 3. Skip 4-byte header
/// 4. Find JPEG/PNG magic bytes in the data
///
/// # Arguments
///
/// * `img` - The image data to decode
///
/// # Returns
///
/// - `Some((width, height, rgb_data))` on success
/// - `None` if decode fails
///
/// # Example
///
/// ```ignore
/// use roboflow_dataset::common::{ImageData, image_decode};
///
/// let compressed = ImageData::encoded(640, 480, jpeg_data);
/// if let Some((w, h, rgb)) = image_decode::decode_image_to_rgb(&compressed) {
///     // Use rgb data
/// }
/// ```
pub fn decode_image_to_rgb(img: &ImageData) -> Option<(u32, u32, Vec<u8>)> {
    // Strategy 1: Try direct decode
    if let Some(decoded) = try_decode_payload(&img.data) {
        return Some(decoded);
    }

    // Strategy 2: Some codecs (e.g. ROS bag CDR) prefix the image with an 8-byte header
    if img.data.len() > 8
        && let Some(decoded) = try_decode_payload(&img.data[8..])
    {
        tracing::debug!(
            original_len = img.data.len(),
            "Decoded image after skipping 8-byte header"
        );
        return Some(decoded);
    }

    // Strategy 3: Try 4-byte header (some serialization formats)
    if img.data.len() > 4
        && let Some(decoded) = try_decode_payload(&img.data[4..])
    {
        tracing::debug!(
            original_len = img.data.len(),
            "Decoded image after skipping 4-byte header"
        );
        return Some(decoded);
    }

    // Strategy 4: Try to find JPEG/PNG magic bytes anywhere in the data
    let data = &img.data;
    if data.len() > 4 {
        // Find JPEG magic (FF D8 FF)
        if let Some(pos) = data
            .windows(3)
            .position(|w| w[0] == 0xFF && w[1] == 0xD8 && w[2] == 0xFF)
            && let Some(decoded) = try_decode_payload(&data[pos..])
        {
            tracing::debug!(
                skipped_bytes = pos,
                "Decoded image after finding JPEG magic bytes"
            );
            return Some(decoded);
        }
        // Find PNG magic (89 50 4E 47)
        if let Some(pos) = data
            .windows(4)
            .position(|w| w[0] == 0x89 && &w[1..4] == b"PNG")
            && let Some(decoded) = try_decode_payload(&data[pos..])
        {
            tracing::debug!(
                skipped_bytes = pos,
                "Decoded image after finding PNG magic bytes"
            );
            return Some(decoded);
        }
    }

    // All strategies failed - log detailed diagnostic info
    tracing::warn!(
        data_len = img.data.len(),
        width = img.width,
        height = img.height,
        first_bytes = if data.len() >= 8 {
            format!(
                "{:02X} {:02X} {:02X} {:02X} {:02X} {:02X} {:02X} {:02X}",
                data.first().copied().unwrap_or(0),
                data.get(1).copied().unwrap_or(0),
                data.get(2).copied().unwrap_or(0),
                data.get(3).copied().unwrap_or(0),
                data.get(4).copied().unwrap_or(0),
                data.get(5).copied().unwrap_or(0),
                data.get(6).copied().unwrap_or(0),
                data.get(7).copied().unwrap_or(0)
            )
        } else {
            "too short".to_string()
        },
        "Compressed image decode failed - data may be corrupted, truncated, or use unsupported format. \
         Consider: 1) Check source file integrity, 2) Verify codec compatibility, 3) Enable debug logging for more details"
    );

    None
}

/// Try to decode a byte slice as JPEG or PNG.
///
/// Returns (width, height, rgb_data) on success, or None if decoding fails.
fn try_decode_payload(data: &[u8]) -> Option<(u32, u32, Vec<u8>)> {
    if data.is_empty() {
        return None;
    }
    if data.starts_with(JPEG_MAGIC)
        && let Ok(decoded) = decode_compressed_image(data, ImageFormat::Jpeg)
    {
        return Some((decoded.width, decoded.height, decoded.data));
    }
    if data.starts_with(PNG_MAGIC)
        && let Ok(decoded) = decode_compressed_image(data, ImageFormat::Png)
    {
        return Some((decoded.width, decoded.height, decoded.data));
    }
    // Try both decoders when magic is missing (e.g. after skipping header)
    if let Ok(decoded) = decode_compressed_image(data, ImageFormat::Jpeg) {
        return Some((decoded.width, decoded.height, decoded.data));
    }
    if let Ok(decoded) = decode_compressed_image(data, ImageFormat::Png) {
        return Some((decoded.width, decoded.height, decoded.data));
    }
    None
}

/// Decode image data to RGB, handling both compressed and raw formats.
///
/// This is a convenience function that checks the `is_encoded` flag
/// and decodes if necessary.
///
/// # Arguments
///
/// * `img` - The image data to decode
///
/// # Returns
///
/// - `Some((width, height, rgb_data))` on success
/// - `None` if the image is encoded but decoding fails
///
/// # Example
///
/// ```ignore
/// use roboflow_dataset::common::{ImageData, image_decode};
///
/// let rgb = image_decode::decode_to_rgb(&image).unwrap();
/// ```
pub fn decode_to_rgb(img: &ImageData) -> Option<(u32, u32, Vec<u8>)> {
    if img.is_encoded {
        decode_image_to_rgb(img)
    } else {
        Some((img.width, img.height, img.data.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_to_rgb_unencoded() {
        let rgb_data = vec![128u8; 640 * 480 * 3];
        let img = ImageData {
            width: 640,
            height: 480,
            data: rgb_data.clone(),
            original_timestamp: 0,
            is_encoded: false,
            is_depth: false,
        };

        let result = decode_to_rgb(&img);
        assert!(result.is_some());
        let (w, h, data) = result.unwrap();
        assert_eq!(w, 640);
        assert_eq!(h, 480);
        assert_eq!(data.len(), 640 * 480 * 3);
    }

    #[test]
    fn test_decode_to_rgb_encoded_fails_on_invalid() {
        // Invalid JPEG - just header, too short to be valid
        let invalid_jpeg = vec![0xFF, 0xD8, 0xFF];
        let img = ImageData {
            width: 640,
            height: 480,
            data: invalid_jpeg,
            original_timestamp: 0,
            is_encoded: true,
            is_depth: false,
        };

        let result = decode_to_rgb(&img);
        // Invalid JPEG should return None
        assert!(result.is_none(), "Invalid encoded image should return None");
    }

    #[test]
    fn test_try_decode_payload_empty_returns_none() {
        let result = try_decode_payload(&[]);
        assert!(result.is_none(), "Empty payload should return None");
    }

    #[test]
    fn test_jpeg_magic_detection() {
        // Valid JPEG magic bytes
        let jpeg_magic = [0xFF, 0xD8, 0xFF];
        assert_eq!(JPEG_MAGIC, jpeg_magic);
    }

    #[test]
    fn test_png_magic_detection() {
        // Valid PNG magic bytes
        let png_magic = [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
        assert_eq!(PNG_MAGIC, png_magic);
    }
}
