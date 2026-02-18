// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! AVX2-accelerated RGB to YUV colorspace conversion for x86_64.
//!
//! This module provides 8-pixel-per-iteration conversion using
//! 256-bit AVX2 SIMD instructions.

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// ITU-R BT.601 conversion coefficients (scaled by 2^8 for integer math)
/// These are for full-range YUV (0-255), not TV limited range (16-235)
#[cfg(target_arch = "x86_64")]
const Y_R: i16 = 77; // 0.299 * 256
#[cfg(target_arch = "x86_64")]
const Y_G: i16 = 150; // 0.587 * 256
#[cfg(target_arch = "x86_64")]
const Y_B: i16 = 29; // 0.114 * 256
#[cfg(target_arch = "x86_64")]
const U_R: i16 = -43; // -0.168736 * 256
#[cfg(target_arch = "x86_64")]
const U_G: i16 = -85; // -0.331264 * 256
#[cfg(target_arch = "x86_64")]
const U_B: i16 = 128; // 0.5 * 256
#[cfg(target_arch = "x86_64")]
const V_R: i16 = 128; // 0.5 * 256
#[cfg(target_arch = "x86_64")]
const V_G: i16 = -107; // -0.418688 * 256
#[cfg(target_arch = "x86_64")]
const V_B: i16 = -21; // -0.081312 * 256

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn load_rgb_values(rgb_ptr: *const u8) -> (__m256i, __m256i, __m256i) {
    // Load 8 RGB pixels (24 bytes) - we need 3 loads of 8 bytes each
    // RGB layout: R0 G0 B0 R1 G1 B1 R2 G2 B2 R3 G3 B3 R4 G4 B4 R5 G5 B5 R6 G6 B6 R7 G7 B7

    // For simplicity, use scalar extraction for now and optimize later
    let mut r_arr = [0i16; 16];
    let mut g_arr = [0i16; 16];
    let mut b_arr = [0i16; 16];

    for i in 0..8 {
        r_arr[i] = *rgb_ptr.add(i * 3) as i16;
        g_arr[i] = *rgb_ptr.add(i * 3 + 1) as i16;
        b_arr[i] = *rgb_ptr.add(i * 3 + 2) as i16;
    }

    let r = _mm256_loadu_si256(r_arr.as_ptr() as *const __m256i);
    let g = _mm256_loadu_si256(g_arr.as_ptr() as *const __m256i);
    let b = _mm256_loadu_si256(b_arr.as_ptr() as *const __m256i);

    (r, g, b)
}

/// Convert 8 RGB pixels to 8 Y values using AVX2.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn rgb8_to_y_avx2(r: __m256i, g: __m256i, b: __m256i) -> __m256i {
    // Y = 0.299*R + 0.587*G + 0.114*B
    // Using 8-bit coefficients scaled by 256
    let y_r = _mm256_set1_epi16(Y_R);
    let y_g = _mm256_set1_epi16(Y_G);
    let y_b = _mm256_set1_epi16(Y_B);

    // Multiply and accumulate
    let r_contrib = _mm256_mullo_epi16(r, y_r);
    let g_contrib = _mm256_mullo_epi16(g, y_g);
    let b_contrib = _mm256_mullo_epi16(b, y_b);

    let y_sum = _mm256_add_epi16(_mm256_add_epi16(r_contrib, g_contrib), b_contrib);

    // Add rounding offset (128 = 256/2) and shift right by 8
    let rounding = _mm256_set1_epi16(128);
    let y_rounded = _mm256_add_epi16(y_sum, rounding);

    // Arithmetic shift right by 8
    _mm256_srai_epi16(y_rounded, 8)
}

/// Convert 8 RGB pixels to 8 U values using AVX2.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn rgb8_to_u_avx2(r: __m256i, g: __m256i, b: __m256i) -> __m256i {
    // U = -0.168736*R - 0.331264*G + 0.5*B + 128
    let u_r = _mm256_set1_epi16(U_R);
    let u_g = _mm256_set1_epi16(U_G);
    let u_b = _mm256_set1_epi16(U_B);
    let offset = _mm256_set1_epi16(128 * 256); // 128 scaled by 256

    let r_contrib = _mm256_mullo_epi16(r, u_r);
    let g_contrib = _mm256_mullo_epi16(g, u_g);
    let b_contrib = _mm256_mullo_epi16(b, u_b);

    let u_sum = _mm256_add_epi16(_mm256_add_epi16(r_contrib, g_contrib), b_contrib);
    let u_offset = _mm256_add_epi16(u_sum, offset);

    let rounding = _mm256_set1_epi16(128);
    let u_rounded = _mm256_add_epi16(u_offset, rounding);

    _mm256_srai_epi16(u_rounded, 8)
}

/// Convert 8 RGB pixels to 8 V values using AVX2.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn rgb8_to_v_avx2(r: __m256i, g: __m256i, b: __m256i) -> __m256i {
    // V = 0.5*R - 0.418688*G - 0.081312*B + 128
    let v_r = _mm256_set1_epi16(V_R);
    let v_g = _mm256_set1_epi16(V_G);
    let v_b = _mm256_set1_epi16(V_B);
    let offset = _mm256_set1_epi16(128 * 256);

    let r_contrib = _mm256_mullo_epi16(r, v_r);
    let g_contrib = _mm256_mullo_epi16(g, v_g);
    let b_contrib = _mm256_mullo_epi16(b, v_b);

    let v_sum = _mm256_add_epi16(_mm256_add_epi16(r_contrib, g_contrib), b_contrib);
    let v_offset = _mm256_add_epi16(v_sum, offset);

    let rounding = _mm256_set1_epi16(128);
    let v_rounded = _mm256_add_epi16(v_offset, rounding);

    _mm256_srai_epi16(v_rounded, 8)
}

/// Pack 16-bit values to 8-bit with clamping.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn pack_and_clamp_epi16(v: __m256i) -> __m128i {
    // Clamp to 0-255 range
    let zero = _mm256_setzero_si256();
    let max_val = _mm256_set1_epi16(255);

    let clamped = _mm256_min_epi16(_mm256_max_epi16(v, zero), max_val);

    // Pack 16-bit to 8-bit (takes lower 128 bits of each 128-bit lane)
    let packed = _mm256_packus_epi16(clamped, zero);

    // Extract lower 128 bits
    _mm256_castsi256_si128(packed)
}

/// Convert RGB24 to YUV420P using AVX2 (8 pixels at a time for Y).
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
pub unsafe fn rgb_to_yuv420p_avx2(
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

        // Process 8 pixels at a time
        let mut x = 0;
        while x < width_minus_8 {
            let (r, g, b) = load_rgb_values(rgb_data.as_ptr().add(row_offset + x * 3));
            let y_vals = rgb8_to_y_avx2(r, g, b);
            let y_packed = pack_and_clamp_epi16(y_vals);

            // Store 8 Y values
            _mm_storeu_si128(
                y_plane.as_mut_ptr().add(y_row_offset + x) as *mut __m128i,
                y_packed,
            );
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

/// Convert RGB24 to NV12 using AVX2.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
pub unsafe fn rgb_to_nv12_avx2(
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

    // Process Y plane (full resolution) - same as YUV420P
    for y in 0..height {
        let row_offset = y * width * 3;
        let y_row_offset = y * width;

        let mut x = 0;
        while x < width_minus_8 {
            let (r, g, b) = load_rgb_values(rgb_data.as_ptr().add(row_offset + x * 3));
            let y_vals = rgb8_to_y_avx2(r, g, b);
            let y_packed = pack_and_clamp_epi16(y_vals);
            _mm_storeu_si128(
                y_plane.as_mut_ptr().add(y_row_offset + x) as *mut __m128i,
                y_packed,
            );
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

/// Check if AVX2 is available at runtime.
#[cfg(target_arch = "x86_64")]
pub fn is_avx2_available() -> bool {
    is_x86_feature_detected!("avx2")
}

#[cfg(not(target_arch = "x86_64"))]
pub fn is_avx2_available() -> bool {
    false
}

// =============================================================================
// Batch Conversion Functions
// =============================================================================

/// Convert multiple RGB24 frames to NV12 using AVX2 batch processing.
///
/// This function processes multiple frames in sequence, with optimized cache
/// utilization by processing each frame completely before moving to the next.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
pub unsafe fn rgb_batch_to_nv12_avx2(
    rgb_frames: &[&[u8]],
    width: usize,
    height: usize,
    results: &mut [(Vec<u8>, Vec<u8>)],
) {
    for (i, &rgb_data) in rgb_frames.iter().enumerate() {
        let (y_plane, uv_plane) = results.get_mut(i).unwrap();
        rgb_to_nv12_avx2(rgb_data, width, height, y_plane, uv_plane);
    }
}

/// Convert multiple RGB24 frames to YUV420P using AVX2 batch processing.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
pub unsafe fn rgb_batch_to_yuv420p_avx2(
    rgb_frames: &[&[u8]],
    width: usize,
    height: usize,
    results: &mut [(Vec<u8>, Vec<u8>, Vec<u8>)],
) {
    for (i, &rgb_data) in rgb_frames.iter().enumerate() {
        let (y_plane, u_plane, v_plane) = results.get_mut(i).unwrap();
        rgb_to_yuv420p_avx2(rgb_data, width, height, y_plane, u_plane, v_plane);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::video::simd::scalar;

    #[test]
    #[cfg(target_arch = "x86_64")]
    fn test_avx2_yuv420p_correctness() {
        if !is_avx2_available() {
            return;
        }

        // Test various sizes
        for &(width, height) in &[(64, 64), (128, 96), (320, 240)] {
            let rgb: Vec<u8> = (0..(width * height * 3))
                .map(|i| ((i * 17) % 256) as u8)
                .collect();

            let mut y_avx = vec![0u8; width * height];
            let mut u_avx = vec![0u8; (width / 2) * (height / 2)];
            let mut v_avx = vec![0u8; (width / 2) * (height / 2)];

            let mut y_scalar = vec![0u8; width * height];
            let mut u_scalar = vec![0u8; (width / 2) * (height / 2)];
            let mut v_scalar = vec![0u8; (width / 2) * (height / 2)];

            unsafe {
                rgb_to_yuv420p_avx2(&rgb, width, height, &mut y_avx, &mut u_avx, &mut v_avx);
            }
            scalar::rgb_to_yuv420p_scalar(
                &rgb,
                width,
                height,
                &mut y_scalar,
                &mut u_scalar,
                &mut v_scalar,
            );

            // Allow small differences due to different rounding
            for i in 0..y_avx.len() {
                let diff = (y_avx[i] as i16 - y_scalar[i] as i16).abs();
                assert!(
                    diff <= 2,
                    "Y mismatch at {}: AVX2={}, scalar={}",
                    i,
                    y_avx[i],
                    y_scalar[i]
                );
            }
            for i in 0..u_avx.len() {
                let diff = (u_avx[i] as i16 - u_scalar[i] as i16).abs();
                assert!(
                    diff <= 2,
                    "U mismatch at {}: AVX2={}, scalar={}",
                    i,
                    u_avx[i],
                    u_scalar[i]
                );
            }
            for i in 0..v_avx.len() {
                let diff = (v_avx[i] as i16 - v_scalar[i] as i16).abs();
                assert!(
                    diff <= 2,
                    "V mismatch at {}: AVX2={}, scalar={}",
                    i,
                    v_avx[i],
                    v_scalar[i]
                );
            }
        }
    }

    #[test]
    #[cfg(target_arch = "x86_64")]
    fn test_avx2_nv12_correctness() {
        if !is_avx2_available() {
            return;
        }

        let width = 64;
        let height = 64;
        let rgb: Vec<u8> = (0..(width * height * 3))
            .map(|i| ((i * 17) % 256) as u8)
            .collect();

        let mut y_avx = vec![0u8; width * height];
        let mut uv_avx = vec![0u8; (width / 2) * (height / 2) * 2];

        let mut y_scalar = vec![0u8; width * height];
        let mut uv_scalar = vec![0u8; (width / 2) * (height / 2) * 2];

        unsafe {
            rgb_to_nv12_avx2(&rgb, width, height, &mut y_avx, &mut uv_avx);
        }
        scalar::rgb_to_nv12_scalar(&rgb, width, height, &mut y_scalar, &mut uv_scalar);

        for i in 0..y_avx.len() {
            let diff = (y_avx[i] as i16 - y_scalar[i] as i16).abs();
            assert!(diff <= 2, "Y mismatch at {}", i);
        }
        for i in 0..uv_avx.len() {
            let diff = (uv_avx[i] as i16 - uv_scalar[i] as i16).abs();
            assert!(diff <= 2, "UV mismatch at {}", i);
        }
    }
}
