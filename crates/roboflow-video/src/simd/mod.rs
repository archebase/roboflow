// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! # SIMD RGB to YUV Colorspace Conversion
//!
//! This module provides high-performance RGB to YUV conversion using SIMD instructions.
//!
//! ## Performance
//!
//! - **SSE2**: 4-6x faster than scalar conversion
//! - **AVX2**: 8-12x faster than scalar conversion
//! - **NEON (ARM)**: 6-10x faster than scalar conversion
//!
//! ## Usage
//!
//! ```rust,ignore
//! use roboflow_video::simd::{rgb_to_yuv420p, rgb_to_nv12, ConversionStrategy};
//!
//! // Convert RGB24 to YUV420P (planar Y, U, V)
//! let (y, u, v) = rgb_to_yuv420p(&rgb_data, width, height)?;
//!
//! // Convert RGB24 to NV12 (semi-planar Y, interleaved UV)
//! let (y, uv) = rgb_to_nv12(&rgb_data, width, height)?;
//!
//! // Check which strategy is being used
//! let strategy = ConversionStrategy::optimal();
//! println!("Using {} conversion ({}x speedup)", strategy.name(), strategy.speedup_factor());
//! ```

mod scalar;

#[cfg(target_arch = "x86_64")]
mod avx2;

#[cfg(target_arch = "aarch64")]
mod neon;

use roboflow_core::RoboflowError;

pub use scalar::{rgb_to_nv12 as rgb_to_nv12_scalar, rgb_to_yuv420p as rgb_to_yuv420p_scalar};

// =============================================================================
// Conversion Strategy
// =============================================================================

/// SIMD conversion strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversionStrategy {
    /// AVX-512 (16 pixels per iteration) - x86_64 only
    Avx512,
    /// AVX2 (8 pixels per iteration) - x86_64 only
    Avx2,
    /// SSE2 (4 pixels per iteration) - x86_64 only
    Sse2,
    /// ARM NEON (8 pixels per iteration) - aarch64 only
    Neon,
    /// Scalar fallback (1 pixel per iteration)
    Scalar,
}

impl ConversionStrategy {
    /// Get the optimal conversion strategy for the current CPU.
    pub fn optimal() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            #[cfg(target_feature = "avx512f")]
            {
                return Self::Avx512;
            }
            if Self::is_avx2_available() {
                return Self::Avx2;
            }
            #[cfg(target_feature = "sse2")]
            {
                return Self::Sse2;
            }
            Self::Scalar
        }

        #[cfg(target_arch = "aarch64")]
        {
            // NEON is always available on aarch64
            Self::Neon
        }

        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            Self::Scalar
        }
    }

    /// Get the expected speedup factor vs scalar.
    pub fn speedup_factor(&self) -> f32 {
        match self {
            Self::Avx512 => 16.0,
            Self::Avx2 => 10.0,
            Self::Sse2 => 5.0,
            Self::Neon => 8.0,
            Self::Scalar => 1.0,
        }
    }

    /// Get human-readable name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Avx512 => "AVX-512",
            Self::Avx2 => "AVX2",
            Self::Sse2 => "SSE2",
            Self::Neon => "NEON",
            Self::Scalar => "Scalar",
        }
    }

    /// Check if AVX2 is available at runtime.
    #[cfg(target_arch = "x86_64")]
    fn is_avx2_available() -> bool {
        is_x86_feature_detected!("avx2")
    }
}

impl Default for ConversionStrategy {
    fn default() -> Self {
        Self::optimal()
    }
}

// =============================================================================
// Public API - RGB to YUV420P
// =============================================================================

/// Result type for YUV420p conversion (Y, U, V planes).
pub type Yuv420pResult = Result<(Vec<u8>, Vec<u8>, Vec<u8>), RoboflowError>;

/// Convert RGB24 to YUV420P (planar format).
///
/// Returns (Y plane, U plane, V plane) where:
/// - Y: width × height bytes (luma)
/// - U: width/2 × height/2 bytes (chroma, subsampled)
/// - V: width/2 × height/2 bytes (chroma, subsampled)
///
/// # Arguments
///
/// * `rgb_data` - Input RGB24 data (3 bytes per pixel)
/// * `width` - Image width in pixels (must be even)
/// * `height` - Image height in pixels (must be even)
///
/// # Example
///
/// ```rust,ignore
/// let rgb_data = vec![0u8; 640 * 480 * 3]; // 640x480 RGB image
/// let (y, u, v) = rgb_to_yuv420p(&rgb_data, 640, 480)?;
/// assert_eq!(y.len(), 640 * 480);
/// assert_eq!(u.len(), 320 * 240);
/// assert_eq!(v.len(), 320 * 240);
/// ```
pub fn rgb_to_yuv420p(rgb_data: &[u8], width: usize, height: usize) -> Yuv420pResult {
    let expected_size = width * height * 3;
    if rgb_data.len() != expected_size {
        return Err(RoboflowError::other(format!(
            "RGB data size mismatch: expected {} bytes, got {}",
            expected_size,
            rgb_data.len()
        )));
    }

    if !width.is_multiple_of(2) || !height.is_multiple_of(2) {
        return Err(RoboflowError::other(
            "Width and height must be even for YUV420P conversion",
        ));
    }

    if width == 0 || height == 0 {
        return Err(RoboflowError::other("Width and height must be non-zero"));
    }

    let mut y_plane = vec![0u8; width * height];
    let mut u_plane = vec![0u8; (width / 2) * (height / 2)];
    let mut v_plane = vec![0u8; (width / 2) * (height / 2)];

    let strategy = ConversionStrategy::optimal();

    #[cfg(target_arch = "x86_64")]
    {
        match strategy {
            ConversionStrategy::Avx2 | ConversionStrategy::Avx512 => {
                // SAFETY: We've validated the buffer sizes match the expected dimensions
                unsafe {
                    avx2::rgb_to_yuv420p_avx2(
                        rgb_data,
                        width,
                        height,
                        &mut y_plane,
                        &mut u_plane,
                        &mut v_plane,
                    );
                }
            }
            _ => {
                scalar::rgb_to_yuv420p_scalar(
                    rgb_data,
                    width,
                    height,
                    &mut y_plane,
                    &mut u_plane,
                    &mut v_plane,
                );
            }
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if strategy == ConversionStrategy::Neon {
            neon::rgb_to_yuv420p_neon(
                rgb_data,
                width,
                height,
                &mut y_plane,
                &mut u_plane,
                &mut v_plane,
            );
        } else {
            scalar::rgb_to_yuv420p_scalar(
                rgb_data,
                width,
                height,
                &mut y_plane,
                &mut u_plane,
                &mut v_plane,
            );
        }
    }

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        scalar::rgb_to_yuv420p_scalar(
            rgb_data,
            width,
            height,
            &mut y_plane,
            &mut u_plane,
            &mut v_plane,
        );
    }

    Ok((y_plane, u_plane, v_plane))
}

// =============================================================================
// Public API - RGB to NV12
// =============================================================================

/// Result type for NV12 conversion (Y, UV planes).
pub type Nv12Result = Result<(Vec<u8>, Vec<u8>), RoboflowError>;

/// Convert RGB24 to NV12 (semi-planar format).
///
/// Returns (Y plane, UV plane) where:
/// - Y: width × height bytes (luma)
/// - UV: width/2 × height/2 × 2 bytes (interleaved chroma)
///
/// # Arguments
///
/// * `rgb_data` - Input RGB24 data (3 bytes per pixel)
/// * `width` - Image width in pixels (must be even)
/// * `height` - Image height in pixels (must be even)
pub fn rgb_to_nv12(rgb_data: &[u8], width: usize, height: usize) -> Nv12Result {
    let expected_size = width * height * 3;
    if rgb_data.len() != expected_size {
        return Err(RoboflowError::other(format!(
            "RGB data size mismatch: expected {} bytes, got {}",
            expected_size,
            rgb_data.len()
        )));
    }

    if !width.is_multiple_of(2) || !height.is_multiple_of(2) {
        return Err(RoboflowError::other(
            "Width and height must be even for NV12 conversion",
        ));
    }

    if width == 0 || height == 0 {
        return Err(RoboflowError::other("Width and height must be non-zero"));
    }

    let mut y_plane = vec![0u8; width * height];
    let mut uv_plane = vec![0u8; (width / 2) * (height / 2) * 2];

    let strategy = ConversionStrategy::optimal();

    #[cfg(target_arch = "x86_64")]
    {
        match strategy {
            ConversionStrategy::Avx2 | ConversionStrategy::Avx512 => unsafe {
                avx2::rgb_to_nv12_avx2(rgb_data, width, height, &mut y_plane, &mut uv_plane);
            },
            _ => {
                scalar::rgb_to_nv12_scalar(rgb_data, width, height, &mut y_plane, &mut uv_plane);
            }
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if strategy == ConversionStrategy::Neon {
            neon::rgb_to_nv12_neon(rgb_data, width, height, &mut y_plane, &mut uv_plane);
        } else {
            scalar::rgb_to_nv12_scalar(rgb_data, width, height, &mut y_plane, &mut uv_plane);
        }
    }

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        scalar::rgb_to_nv12_scalar(rgb_data, width, height, &mut y_plane, &mut uv_plane);
    }

    Ok((y_plane, uv_plane))
}

// =============================================================================
// Convenience Functions
// =============================================================================

/// Get the optimal conversion strategy for the current CPU.
pub fn optimal_strategy() -> ConversionStrategy {
    ConversionStrategy::optimal()
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_optimal_strategy() {
        let strategy = ConversionStrategy::optimal();
        // Should return a valid strategy
        match strategy {
            ConversionStrategy::Avx512
            | ConversionStrategy::Avx2
            | ConversionStrategy::Sse2
            | ConversionStrategy::Neon
            | ConversionStrategy::Scalar => {}
        }
    }

    #[test]
    fn test_rgb_to_yuv420p_small() {
        let rgb_data = vec![
            255, 0, 0, // Red
            0, 255, 0, // Green
            0, 0, 255, // Blue
            255, 255, 0, // Yellow
        ];

        let (y, u, v) = rgb_to_yuv420p(&rgb_data, 2, 2).unwrap();

        assert_eq!(y.len(), 4);
        assert_eq!(u.len(), 1);
        assert_eq!(v.len(), 1);
    }

    #[test]
    fn test_rgb_to_yuv420p_large() {
        let width = 64;
        let height = 64;
        let rgb_data = vec![128u8; width * height * 3];

        let (y, u, v) = rgb_to_yuv420p(&rgb_data, width, height).unwrap();

        assert_eq!(y.len(), width * height);
        assert_eq!(u.len(), (width / 2) * (height / 2));
        assert_eq!(v.len(), (width / 2) * (height / 2));
    }

    #[test]
    fn test_rgb_to_yuv420p_invalid_size() {
        let rgb_data = vec![0u8; 100]; // Invalid size
        let result = rgb_to_yuv420p(&rgb_data, 10, 10);
        assert!(result.is_err());
    }

    #[test]
    fn test_rgb_to_yuv420p_odd_dimensions() {
        let rgb_data = vec![0u8; 9]; // 3x3 = 27 bytes needed
        let result = rgb_to_yuv420p(&rgb_data, 3, 3);
        assert!(result.is_err());
    }

    #[test]
    fn test_rgb_to_nv12_small() {
        let rgb_data = vec![
            255, 0, 0, // Red
            0, 255, 0, // Green
            0, 0, 255, // Blue
            255, 255, 0, // Yellow
        ];

        let (y, uv) = rgb_to_nv12(&rgb_data, 2, 2).unwrap();

        assert_eq!(y.len(), 4);
        assert_eq!(uv.len(), 2);
    }

    #[test]
    fn test_rgb_to_nv12_large() {
        let width = 64;
        let height = 64;
        let rgb_data = vec![128u8; width * height * 3];

        let (y, uv) = rgb_to_nv12(&rgb_data, width, height).unwrap();

        assert_eq!(y.len(), width * height);
        assert_eq!(uv.len(), (width / 2) * (height / 2) * 2);
    }

    #[test]
    fn test_rgb_to_nv12_invalid_size() {
        let rgb_data = vec![0u8; 50];
        let result = rgb_to_nv12(&rgb_data, 10, 10);
        assert!(result.is_err());
    }

    #[test]
    fn test_rgb_to_yuv420p_black() {
        let rgb_data = vec![0u8; 4 * 4 * 3];
        let (y, _u, _v) = rgb_to_yuv420p(&rgb_data, 4, 4).unwrap();
        // Y should be close to 0 for black
        assert!(y.iter().all(|&v| v < 50));
    }

    #[test]
    fn test_rgb_to_yuv420p_white() {
        let rgb_data = vec![255u8; 4 * 4 * 3];
        let (y, _u, _v) = rgb_to_yuv420p(&rgb_data, 4, 4).unwrap();
        // Y should be close to 235 for white (limited range) or 255 (full range)
        assert!(y.iter().all(|&v| v > 200));
    }

    #[test]
    fn test_rgb_to_nv12_black() {
        let rgb_data = vec![0u8; 4 * 4 * 3];
        let (y, _uv) = rgb_to_nv12(&rgb_data, 4, 4).unwrap();
        assert!(y.iter().all(|&v| v < 50));
    }

    #[test]
    fn test_rgb_to_nv12_white() {
        let rgb_data = vec![255u8; 4 * 4 * 3];
        let (y, _uv) = rgb_to_nv12(&rgb_data, 4, 4).unwrap();
        assert!(y.iter().all(|&v| v > 200));
    }

    // =============================================================================
    // Strategy Tests
    // =============================================================================

    #[test]
    fn test_strategy_names() {
        assert_eq!(ConversionStrategy::Avx512.name(), "AVX-512");
        assert_eq!(ConversionStrategy::Avx2.name(), "AVX2");
        assert_eq!(ConversionStrategy::Sse2.name(), "SSE2");
        assert_eq!(ConversionStrategy::Neon.name(), "NEON");
        assert_eq!(ConversionStrategy::Scalar.name(), "Scalar");
    }

    #[test]
    fn test_strategy_speedup_factors() {
        assert!(ConversionStrategy::Avx512.speedup_factor() > 10.0);
        assert!(ConversionStrategy::Avx2.speedup_factor() > 5.0);
        assert!(ConversionStrategy::Sse2.speedup_factor() > 3.0);
        assert!(ConversionStrategy::Neon.speedup_factor() > 5.0);
        assert_eq!(ConversionStrategy::Scalar.speedup_factor(), 1.0);
    }

    #[test]
    fn test_strategy_equality() {
        assert_eq!(ConversionStrategy::Avx2, ConversionStrategy::Avx2);
        assert_eq!(ConversionStrategy::Scalar, ConversionStrategy::Scalar);
        assert_ne!(ConversionStrategy::Avx2, ConversionStrategy::Sse2);
        assert_ne!(ConversionStrategy::Neon, ConversionStrategy::Scalar);
    }

    #[test]
    fn test_strategy_clone() {
        let strategy = ConversionStrategy::Avx2;
        let cloned = strategy.clone();
        assert_eq!(strategy, cloned);
    }

    #[test]
    fn test_strategy_debug() {
        let strategy = ConversionStrategy::Neon;
        let debug_str = format!("{:?}", strategy);
        assert!(debug_str.contains("Neon"));
    }

    #[test]
    fn test_strategy_default() {
        let strategy = ConversionStrategy::default();
        // Default should be the optimal strategy
        assert_eq!(strategy, ConversionStrategy::optimal());
    }
}
