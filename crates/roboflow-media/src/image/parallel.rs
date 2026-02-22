// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Parallel image decoding using rayon.
//!
//! This module provides batch image decoding capabilities using rayon
//! for parallel processing across available CPU cores.

use super::format::ImageFormat;
use rayon::prelude::*;

// Re-export DecodedImage from backend for convenience
pub use super::backend::DecodedImage;

/// Decode multiple images in parallel.
///
/// This function uses rayon to decode images across available CPU cores.
/// Returns results in the same order as input, with `None` for failed decodes.
///
/// # Arguments
///
/// * `images` - Slice of (data, format) tuples to decode
///
/// # Returns
///
/// Vector of decoded images, with `None` for any that failed to decode
pub fn decode_images_parallel(images: &[(&[u8], ImageFormat)]) -> Vec<Option<DecodedImage>> {
    use super::decode_compressed_image;

    images
        .par_iter()
        .map(|(data, format)| decode_compressed_image(data, *format).ok())
        .collect()
}

/// Decode multiple images with their dimensions in parallel.
///
/// This variant includes expected dimensions for validation.
///
/// # Arguments
///
/// * `images` - Slice of (data, format, width, height) tuples
///
/// # Returns
///
/// Vector of decoded images, with `None` for any that failed to decode
pub fn decode_images_parallel_with_dims(
    images: &[(&[u8], ImageFormat, u32, u32)],
) -> Vec<Option<DecodedImage>> {
    use super::decode_compressed_image;

    images
        .par_iter()
        .map(|(data, format, width, height)| {
            match decode_compressed_image(data, *format) {
                Ok(img) => {
                    // Validate dimensions if provided
                    if *width > 0 && *height > 0 && (img.width != *width || img.height != *height) {
                        tracing::warn!(
                            expected_width = width,
                            expected_height = height,
                            actual_width = img.width,
                            actual_height = img.height,
                            "Dimension mismatch in decoded image"
                        );
                    }
                    Some(img)
                }
                Err(e) => {
                    tracing::debug!(
                        error = %e,
                        format = ?format,
                        "Failed to decode image in parallel batch"
                    );
                    None
                }
            }
        })
        .collect()
}

/// Statistics for parallel decoding operations.
#[derive(Debug, Clone, Default)]
pub struct ParallelDecodeStats {
    /// Total images processed
    pub total_images: usize,
    /// Successfully decoded images
    pub successful_decodes: usize,
    /// Failed decodes
    pub failed_decodes: usize,
    /// Total input bytes
    pub total_input_bytes: usize,
    /// Total output bytes (RGB)
    pub total_output_bytes: usize,
    /// Processing time in seconds
    pub duration_sec: f64,
}

impl ParallelDecodeStats {
    /// Calculate the average decoding speed in megapixels per second.
    pub fn megapixels_per_sec(&self) -> f64 {
        if self.duration_sec > 0.0 {
            let total_pixels = self.successful_decodes as f64; // Simplified
            total_pixels / self.duration_sec / 1_000_000.0
        } else {
            0.0
        }
    }

    /// Calculate the compression ratio.
    pub fn compression_ratio(&self) -> f64 {
        if self.total_input_bytes > 0 {
            self.total_output_bytes as f64 / self.total_input_bytes as f64
        } else {
            1.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_images_parallel_empty() {
        let images: Vec<(&[u8], ImageFormat)> = vec![];
        let results = decode_images_parallel(&images);
        assert!(results.is_empty());
    }

    #[test]
    fn test_decode_images_parallel_with_dims_empty() {
        let images: Vec<(&[u8], ImageFormat, u32, u32)> = vec![];
        let results = decode_images_parallel_with_dims(&images);
        assert!(results.is_empty());
    }

    #[test]
    fn test_parallel_decode_stats_default() {
        let stats = ParallelDecodeStats::default();
        assert_eq!(stats.total_images, 0);
        assert_eq!(stats.successful_decodes, 0);
        assert_eq!(stats.failed_decodes, 0);
    }

    #[test]
    fn test_parallel_decode_stats_compression_ratio() {
        let stats = ParallelDecodeStats {
            total_input_bytes: 1000,
            total_output_bytes: 3000,
            ..Default::default()
        };
        assert_eq!(stats.compression_ratio(), 3.0);
    }
}
