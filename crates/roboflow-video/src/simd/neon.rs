// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! NEON-accelerated RGB to YUV colorspace conversion for ARM/Apple Silicon.
//!
//! This module provides 8-pixel-per-iteration conversion using
//! 128-bit NEON SIMD instructions. NEON is always available on aarch64.

/// ITU-R BT.601 conversion coefficients (scaled by 2^8 for integer math)
/// These are for full-range YUV (0-255), not TV limited range (16-235)
#[cfg(target_arch = "aarch64")]
const Y_R: i16 = 77; // 0.299 * 256
#[cfg(target_arch = "aarch64")]
const Y_G: i16 = 150; // 0.587 * 256
#[cfg(target_arch = "aarch64")]
const Y_B: i16 = 29; // 0.114 * 256
#[cfg(target_arch = "aarch64")]
const U_R: i16 = -43; // -0.168736 * 256
#[cfg(target_arch = "aarch64")]
const U_G: i16 = -85; // -0.331264 * 256
#[cfg(target_arch = "aarch64")]
const U_B: i16 = 128; // 0.5 * 256
#[cfg(target_arch = "aarch64")]
const V_R: i16 = 128; // 0.5 * 256
#[cfg(target_arch = "aarch64")]
const V_G: i16 = -107; // -0.418688 * 256
#[cfg(target_arch = "aarch64")]
const V_B: i16 = -21; // -0.081312 * 256

/// Load 8 RGB pixels and compute Y values directly.
/// This is written to allow the compiler to auto-vectorize with NEON.
#[cfg(target_arch = "aarch64")]
#[inline]
fn compute_y_values_8(rgb_ptr: *const u8) -> [u8; 8] {
    // Load RGB values - compiler can auto-vectorize this
    let mut r_arr = [0i16; 8];
    let mut g_arr = [0i16; 8];
    let mut b_arr = [0i16; 8];

    unsafe {
        for i in 0..8 {
            r_arr[i] = *rgb_ptr.add(i * 3) as i16;
            g_arr[i] = *rgb_ptr.add(i * 3 + 1) as i16;
            b_arr[i] = *rgb_ptr.add(i * 3 + 2) as i16;
        }
    }

    // Compute Y values - vectorizable
    let mut y_arr = [0u8; 8];
    for i in 0..8 {
        let y_val = (Y_R as i32 * r_arr[i] as i32
            + Y_G as i32 * g_arr[i] as i32
            + Y_B as i32 * b_arr[i] as i32
            + 128)
            >> 8;
        y_arr[i] = y_val.clamp(0, 255) as u8;
    }
    y_arr
}

/// Convert RGB24 to YUV420P using NEON (8 pixels at a time for Y).
/// NEON is always available on aarch64, so we rely on auto-vectorization.
#[cfg(target_arch = "aarch64")]
pub fn rgb_to_yuv420p_neon(
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

    let width_minus_8 = width - (width % 8);

    // Process Y plane (full resolution)
    for y in 0..height {
        let row_offset = y * width * 3;
        let y_row_offset = y * width;

        let mut x = 0;
        while x < width_minus_8 {
            let y_vals = compute_y_values_8(rgb_data.as_ptr().wrapping_add(row_offset + x * 3));
            y_plane[y_row_offset + x..y_row_offset + x + 8].copy_from_slice(&y_vals);
            x += 8;
        }

        // Handle remaining pixels with scalar
        while x < width {
            let offset = row_offset + x * 3;
            let r = rgb_data[offset] as i32;
            let g = rgb_data[offset + 1] as i32;
            let b = rgb_data[offset + 2] as i32;

            let y_val =
                ((Y_R as i32 * r + Y_G as i32 * g + Y_B as i32 * b + 128) >> 8).clamp(0, 255);
            y_plane[y_row_offset + x] = y_val as u8;
            x += 1;
        }
    }

    // Process U and V planes (2x2 subsampling)
    for block_y in 0..(height / 2) {
        for block_x in 0..(width / 2) {
            let mut u_sum: i32 = 0;
            let mut v_sum: i32 = 0;

            for dy in 0..2 {
                for dx in 0..2 {
                    let py = block_y * 2 + dy;
                    let px = block_x * 2 + dx;
                    let offset = (py * width + px) * 3;

                    let r = rgb_data[offset] as i32;
                    let g = rgb_data[offset + 1] as i32;
                    let b = rgb_data[offset + 2] as i32;

                    u_sum += ((U_R as i32 * r + U_G as i32 * g + U_B as i32 * b + 128) >> 8) + 128;
                    v_sum += ((V_R as i32 * r + V_G as i32 * g + V_B as i32 * b + 128) >> 8) + 128;
                }
            }

            let uv_offset = block_y * (width / 2) + block_x;
            u_plane[uv_offset] = ((u_sum + 2) / 4).clamp(0, 255) as u8;
            v_plane[uv_offset] = ((v_sum + 2) / 4).clamp(0, 255) as u8;
        }
    }
}

/// Convert RGB24 to NV12 using NEON.
#[cfg(target_arch = "aarch64")]
pub fn rgb_to_nv12_neon(
    rgb_data: &[u8],
    width: usize,
    height: usize,
    y_plane: &mut [u8],
    uv_plane: &mut [u8],
) {
    debug_assert_eq!(rgb_data.len(), width * height * 3);
    debug_assert_eq!(y_plane.len(), width * height);
    debug_assert_eq!(uv_plane.len(), (width / 2) * (height / 2) * 2);

    let width_minus_8 = width - (width % 8);

    // Process Y plane (full resolution)
    for y in 0..height {
        let row_offset = y * width * 3;
        let y_row_offset = y * width;

        let mut x = 0;
        while x < width_minus_8 {
            let y_vals = compute_y_values_8(rgb_data.as_ptr().wrapping_add(row_offset + x * 3));
            y_plane[y_row_offset + x..y_row_offset + x + 8].copy_from_slice(&y_vals);
            x += 8;
        }

        while x < width {
            let offset = row_offset + x * 3;
            let r = rgb_data[offset] as i32;
            let g = rgb_data[offset + 1] as i32;
            let b = rgb_data[offset + 2] as i32;

            let y_val =
                ((Y_R as i32 * r + Y_G as i32 * g + Y_B as i32 * b + 128) >> 8).clamp(0, 255);
            y_plane[y_row_offset + x] = y_val as u8;
            x += 1;
        }
    }

    // Process UV plane (2x2 subsampling, interleaved)
    for block_y in 0..(height / 2) {
        for block_x in 0..(width / 2) {
            let mut u_sum: i32 = 0;
            let mut v_sum: i32 = 0;

            for dy in 0..2 {
                for dx in 0..2 {
                    let py = block_y * 2 + dy;
                    let px = block_x * 2 + dx;
                    let offset = (py * width + px) * 3;

                    let r = rgb_data[offset] as i32;
                    let g = rgb_data[offset + 1] as i32;
                    let b = rgb_data[offset + 2] as i32;

                    u_sum += ((U_R as i32 * r + U_G as i32 * g + U_B as i32 * b + 128) >> 8) + 128;
                    v_sum += ((V_R as i32 * r + V_G as i32 * g + V_B as i32 * b + 128) >> 8) + 128;
                }
            }

            let uv_offset = (block_y * (width / 2) + block_x) * 2;
            uv_plane[uv_offset] = ((u_sum + 2) / 4).clamp(0, 255) as u8;
            uv_plane[uv_offset + 1] = ((v_sum + 2) / 4).clamp(0, 255) as u8;
        }
    }
}

// =============================================================================
// Batch Conversion Functions
// =============================================================================

/// Convert multiple RGB24 frames to NV12 using NEON batch processing.
#[cfg(target_arch = "aarch64")]
pub fn rgb_batch_to_nv12_neon(
    rgb_frames: &[&[u8]],
    width: usize,
    height: usize,
    results: &mut [(Vec<u8>, Vec<u8>)],
) {
    for (i, &rgb_data) in rgb_frames.iter().enumerate() {
        let (y_plane, uv_plane) = results.get_mut(i).unwrap();
        rgb_to_nv12_neon(rgb_data, width, height, y_plane, uv_plane);
    }
}

/// Convert multiple RGB24 frames to YUV420P using NEON batch processing.
#[cfg(target_arch = "aarch64")]
pub fn rgb_batch_to_yuv420p_neon(
    rgb_frames: &[&[u8]],
    width: usize,
    height: usize,
    results: &mut [(Vec<u8>, Vec<u8>, Vec<u8>)],
) {
    for (i, &rgb_data) in rgb_frames.iter().enumerate() {
        let (y_plane, u_plane, v_plane) = results.get_mut(i).unwrap();
        rgb_to_yuv420p_neon(rgb_data, width, height, y_plane, u_plane, v_plane);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simd::scalar;

    #[test]
    #[cfg(target_arch = "aarch64")]
    fn test_neon_yuv420p_correctness() {
        // Test various sizes
        for &(width, height) in &[(64, 64), (128, 96), (320, 240)] {
            let rgb: Vec<u8> = (0..(width * height * 3))
                .map(|i| ((i * 17) % 256) as u8)
                .collect();

            let mut y_neon = vec![0u8; width * height];
            let mut u_neon = vec![0u8; (width / 2) * (height / 2)];
            let mut v_neon = vec![0u8; (width / 2) * (height / 2)];

            let mut y_scalar = vec![0u8; width * height];
            let mut u_scalar = vec![0u8; (width / 2) * (height / 2)];
            let mut v_scalar = vec![0u8; (width / 2) * (height / 2)];

            rgb_to_yuv420p_neon(&rgb, width, height, &mut y_neon, &mut u_neon, &mut v_neon);
            scalar::rgb_to_yuv420p_scalar(
                &rgb,
                width,
                height,
                &mut y_scalar,
                &mut u_scalar,
                &mut v_scalar,
            );

            // Allow small differences due to different rounding
            for i in 0..y_neon.len() {
                let diff = (y_neon[i] as i16 - y_scalar[i] as i16).abs();
                assert!(
                    diff <= 2,
                    "Y mismatch at {}: NEON={}, scalar={}",
                    i,
                    y_neon[i],
                    y_scalar[i]
                );
            }
            for i in 0..u_neon.len() {
                let diff = (u_neon[i] as i16 - u_scalar[i] as i16).abs();
                assert!(
                    diff <= 2,
                    "U mismatch at {}: NEON={}, scalar={}",
                    i,
                    u_neon[i],
                    u_scalar[i]
                );
            }
            for i in 0..v_neon.len() {
                let diff = (v_neon[i] as i16 - v_scalar[i] as i16).abs();
                assert!(
                    diff <= 2,
                    "V mismatch at {}: NEON={}, scalar={}",
                    i,
                    v_neon[i],
                    v_scalar[i]
                );
            }
        }
    }

    #[test]
    #[cfg(target_arch = "aarch64")]
    fn test_neon_nv12_correctness() {
        let width = 64;
        let height = 64;
        let rgb: Vec<u8> = (0..(width * height * 3))
            .map(|i| ((i * 17) % 256) as u8)
            .collect();

        let mut y_neon = vec![0u8; width * height];
        let mut uv_neon = vec![0u8; (width / 2) * (height / 2) * 2];

        let mut y_scalar = vec![0u8; width * height];
        let mut uv_scalar = vec![0u8; (width / 2) * (height / 2) * 2];

        rgb_to_nv12_neon(&rgb, width, height, &mut y_neon, &mut uv_neon);
        scalar::rgb_to_nv12_scalar(&rgb, width, height, &mut y_scalar, &mut uv_scalar);

        for i in 0..y_neon.len() {
            let diff = (y_neon[i] as i16 - y_scalar[i] as i16).abs();
            assert!(
                diff <= 2,
                "Y mismatch at {}: NEON={}, scalar={}",
                i,
                y_neon[i],
                y_scalar[i]
            );
        }
        for i in 0..uv_neon.len() {
            let diff = (uv_neon[i] as i16 - uv_scalar[i] as i16).abs();
            assert!(
                diff <= 2,
                "UV mismatch at {}: NEON={}, scalar={}",
                i,
                uv_neon[i],
                uv_scalar[i]
            );
        }
    }
}
