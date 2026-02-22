// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Scalar fallback for RGB to YUV colorspace conversion.
//!
//! This module provides the reference implementation used when SIMD
//! is not available or for correctness testing.

use roboflow_core::RoboflowError;

/// Result type for YUV420p conversion (Y, U, V planes).
pub type Yuv420pResult = Result<(Vec<u8>, Vec<u8>, Vec<u8>), RoboflowError>;

/// Result type for NV12 conversion (Y, UV planes).
pub type Nv12Result = Result<(Vec<u8>, Vec<u8>), RoboflowError>;

/// ITU-R BT.601 conversion coefficients (scaled by 2^8 for integer math)
/// These are for full-range YUV (0-255), not TV limited range (16-235)
const Y_R: i32 = 77; // 0.299 * 256 ≈ 77
const Y_G: i32 = 150; // 0.587 * 256 ≈ 150
const Y_B: i32 = 29; // 0.114 * 256 ≈ 29
const U_R: i32 = -43; // -0.168736 * 256 ≈ -43
const U_G: i32 = -85; // -0.331264 * 256 ≈ -85
const U_B: i32 = 128; // 0.5 * 256 ≈ 128
const V_R: i32 = 128; // 0.5 * 256 ≈ 128
const V_G: i32 = -107; // -0.418688 * 256 ≈ -107
const V_B: i32 = -21; // -0.081312 * 256 ≈ -21

/// Convert RGB24 to YUV420P (planar format) using scalar code.
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
#[inline]
pub fn rgb_to_yuv420p_scalar(
    rgb_data: &[u8],
    width: usize,
    height: usize,
    y_plane: &mut [u8],
    u_plane: &mut [u8],
    v_plane: &mut [u8],
) {
    debug_assert_eq!(rgb_data.len(), width * height * 3);
    debug_assert_eq!(y_plane.len(), width * height);
    debug_assert_eq!(u_plane.len(), (width / 2) * (height / 2));
    debug_assert_eq!(v_plane.len(), (width / 2) * (height / 2));

    // Process 2x2 blocks for chroma subsampling
    for block_y in 0..(height / 2) {
        for block_x in 0..(width / 2) {
            let mut u_sum: [i32; 4] = [0; 4];
            let mut v_sum: [i32; 4] = [0; 4];

            // Process 4 pixels in 2x2 block
            for dy in 0..2 {
                for dx in 0..2 {
                    let y = block_y * 2 + dy;
                    let x = block_x * 2 + dx;
                    let pixel_offset = (y * width + x) * 3;

                    let r = rgb_data[pixel_offset] as i32;
                    let g = rgb_data[pixel_offset + 1] as i32;
                    let b = rgb_data[pixel_offset + 2] as i32;

                    // Calculate Y (luma) - full resolution
                    let y_val = (Y_R * r + Y_G * g + Y_B * b + 128) >> 8;
                    y_plane[y * width + x] = y_val.clamp(0, 255) as u8;

                    // Collect chroma samples for 2x2 block averaging
                    let block_idx = ((y & 1) << 1) | (x & 1);
                    u_sum[block_idx] += ((U_R * r + U_G * g + U_B * b + 128) >> 8) + 128;
                    v_sum[block_idx] += ((V_R * r + V_G * g + V_B * b + 128) >> 8) + 128;
                }
            }

            // Average and store chroma
            let uv_offset = block_y * (width / 2) + block_x;
            u_plane[uv_offset] =
                ((u_sum[0] + u_sum[1] + u_sum[2] + u_sum[3]) / 4).clamp(0, 255) as u8;
            v_plane[uv_offset] =
                ((v_sum[0] + v_sum[1] + v_sum[2] + v_sum[3]) / 4).clamp(0, 255) as u8;
        }
    }
}

/// Convert RGB24 to NV12 (semi-planar format) using scalar code.
///
/// Returns (Y plane, UV plane) where:
/// - Y: width × height bytes (luma)
/// - UV: width/2 × height/2 × 2 bytes (interleaved U, V)
///
/// # Arguments
///
/// * `rgb_data` - Input RGB24 data (3 bytes per pixel)
/// * `width` - Image width in pixels (must be even)
/// * `height` - Image height in pixels (must be even)
#[inline]
pub fn rgb_to_nv12_scalar(
    rgb_data: &[u8],
    width: usize,
    height: usize,
    y_plane: &mut [u8],
    uv_plane: &mut [u8],
) {
    debug_assert_eq!(rgb_data.len(), width * height * 3);
    debug_assert_eq!(y_plane.len(), width * height);
    debug_assert_eq!(uv_plane.len(), (width / 2) * (height / 2) * 2);

    // Process 2x2 blocks for chroma subsampling
    for block_y in 0..(height / 2) {
        for block_x in 0..(width / 2) {
            let mut u_sum: [i32; 4] = [0; 4];
            let mut v_sum: [i32; 4] = [0; 4];

            // Process 4 pixels in 2x2 block
            for dy in 0..2 {
                for dx in 0..2 {
                    let y = block_y * 2 + dy;
                    let x = block_x * 2 + dx;
                    let pixel_offset = (y * width + x) * 3;

                    let r = rgb_data[pixel_offset] as i32;
                    let g = rgb_data[pixel_offset + 1] as i32;
                    let b = rgb_data[pixel_offset + 2] as i32;

                    // Calculate Y (luma) - full resolution
                    let y_val = (Y_R * r + Y_G * g + Y_B * b + 128) >> 8;
                    y_plane[y * width + x] = y_val.clamp(0, 255) as u8;

                    // Collect chroma samples for 2x2 block averaging
                    let block_idx = ((y & 1) << 1) | (x & 1);
                    u_sum[block_idx] += ((U_R * r + U_G * g + U_B * b + 128) >> 8) + 128;
                    v_sum[block_idx] += ((V_R * r + V_G * g + V_B * b + 128) >> 8) + 128;
                }
            }

            // Average and store interleaved UV
            let uv_offset = (block_y * (width / 2) + block_x) * 2;
            uv_plane[uv_offset] =
                ((u_sum[0] + u_sum[1] + u_sum[2] + u_sum[3]) / 4).clamp(0, 255) as u8;
            uv_plane[uv_offset + 1] =
                ((v_sum[0] + v_sum[1] + v_sum[2] + v_sum[3]) / 4).clamp(0, 255) as u8;
        }
    }
}

/// Convert RGB24 to YUV420P with full allocation.
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

    let mut y_plane = vec![0u8; width * height];
    let mut u_plane = vec![0u8; (width / 2) * (height / 2)];
    let mut v_plane = vec![0u8; (width / 2) * (height / 2)];

    rgb_to_yuv420p_scalar(
        rgb_data,
        width,
        height,
        &mut y_plane,
        &mut u_plane,
        &mut v_plane,
    );

    Ok((y_plane, u_plane, v_plane))
}

/// Convert RGB24 to NV12 with full allocation.
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

    let mut y_plane = vec![0u8; width * height];
    let mut uv_plane = vec![0u8; (width / 2) * (height / 2) * 2];

    rgb_to_nv12_scalar(rgb_data, width, height, &mut y_plane, &mut uv_plane);

    Ok((y_plane, uv_plane))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rgb_to_yuv420p_black() {
        let rgb = vec![0u8; 4 * 4 * 3]; // 4x4 black image
        let (y, u, v) = rgb_to_yuv420p(&rgb, 4, 4).unwrap();
        assert_eq!(y.len(), 16);
        assert_eq!(u.len(), 4);
        assert_eq!(v.len(), 4);
        // Y should be near 0 for black
        assert!(y.iter().all(|&v| v < 20));
    }

    #[test]
    fn test_rgb_to_yuv420p_white() {
        let rgb = vec![255u8; 4 * 4 * 3]; // 4x4 white image
        let (y, _u, _v) = rgb_to_yuv420p(&rgb, 4, 4).unwrap();
        // Y should be near 255 for white
        assert!(y.iter().all(|&v| v > 230));
    }

    #[test]
    fn test_rgb_to_nv12_black() {
        let rgb = vec![0u8; 4 * 4 * 3]; // 4x4 black image
        let (y, _uv) = rgb_to_nv12(&rgb, 4, 4).unwrap();
        assert_eq!(y.len(), 16);
        assert!(_uv.len() == 8);
        assert!(y.iter().all(|&v| v < 20));
    }

    #[test]
    fn test_rgb_to_nv12_white() {
        let rgb = vec![255u8; 4 * 4 * 3]; // 4x4 white image
        let (y, _uv) = rgb_to_nv12(&rgb, 4, 4).unwrap();
        assert!(y.iter().all(|&v| v > 230));
    }

    #[test]
    fn test_invalid_dimensions() {
        let rgb = vec![0u8; 3 * 3 * 3]; // 3x3 (odd dimensions)
        assert!(rgb_to_yuv420p(&rgb, 3, 3).is_err());
        assert!(rgb_to_nv12(&rgb, 3, 3).is_err());
    }

    #[test]
    fn test_size_mismatch() {
        let rgb = vec![0u8; 10]; // Wrong size
        assert!(rgb_to_yuv420p(&rgb, 4, 4).is_err());
    }
}
