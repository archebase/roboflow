// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Video encoding for LeRobot datasets.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use roboflow_video::RsmpegMp4Encoder;
use crate::common::video::VideoEncoderError;
use crate::common::video::{VideoEncoderConfig, VideoFrame, VideoFrameBuffer};
use crate::common::{ImageData, decode_image_to_rgb};
use crate::lerobot::video_profiles::ResolvedConfig;
use roboflow_core::Result;

/// Encode videos for all cameras.
///
/// This function uses parallel encoding when multiple cameras are present
/// and hardware acceleration is available.
pub fn encode_videos(
    image_buffers: &[(String, Vec<ImageData>)],
    episode_index: usize,
    videos_dir: &Path,
    video_config: &ResolvedConfig,
    fps: u32,
    use_cloud_storage: bool,
) -> Result<(Vec<(PathBuf, String)>, EncodeStats)> {
    if image_buffers.is_empty() {
        return Ok((Vec::new(), EncodeStats::default()));
    }

    let encoder_config = video_config.to_encoder_config(fps);

    tracing::info!(
        codec = %video_config.codec,
        crf = video_config.crf,
        preset = %video_config.preset,
        hardware_accelerated = video_config.hardware_accelerated,
        parallel_jobs = video_config.parallel_jobs,
        "Video encoding configuration"
    );

    // Filter out empty cameras
    let camera_data: Vec<(String, Vec<ImageData>)> = image_buffers
        .iter()
        .filter(|(_, images)| !images.is_empty())
        .map(|(camera, images)| (camera.clone(), images.clone()))
        .collect();

    if camera_data.is_empty() {
        return Ok((Vec::new(), EncodeStats::default()));
    }

    // Use parallel encoding only when hardware acceleration is enabled
    let use_parallel = video_config.hardware_accelerated
        && video_config.parallel_jobs > 1
        && camera_data.len() > 1;

    let result = if use_parallel {
        let concurrent_jobs = video_config.parallel_jobs.min(camera_data.len());
        encode_videos_parallel(
            camera_data,
            videos_dir,
            &encoder_config,
            episode_index,
            concurrent_jobs,
            use_cloud_storage,
        )?
    } else {
        encode_videos_sequential(
            camera_data,
            videos_dir,
            &encoder_config,
            episode_index,
            use_cloud_storage,
        )?
    };

    Ok(result)
}

/// Statistics from video encoding.
#[derive(Debug, Default)]
pub struct EncodeStats {
    /// Number of images encoded
    pub images_encoded: usize,
    /// Number of frames skipped due to dimension mismatches
    pub skipped_frames: usize,
    /// Number of videos that failed to encode
    pub failed_encodings: usize,
    /// Number of images that failed to decode (corrupted/unsupported format)
    #[allow(dead_code)]
    pub decode_failures: usize,
    /// Total output bytes
    pub output_bytes: u64,
}

/// Encode videos sequentially (original behavior).
fn encode_videos_sequential(
    camera_data: Vec<(String, Vec<ImageData>)>,
    videos_dir: &Path,
    encoder_config: &VideoEncoderConfig,
    episode_index: usize,
    use_cloud_storage: bool,
) -> Result<(Vec<(PathBuf, String)>, EncodeStats)> {
    let mut stats = EncodeStats::default();
    let mut video_files = Vec::new();

    tracing::info!("Using native rsmpeg encoder (in-process FFmpeg)");

    for (camera, images) in camera_data {
        let (buffer, skipped) = build_frame_buffer_static(&images)?;
        stats.skipped_frames += skipped;

        if !buffer.is_empty() {
            // camera key already contains the full feature path
            let camera_dir = videos_dir.join(&camera);
            fs::create_dir_all(&camera_dir)?;

            let video_path = camera_dir.join(format!("episode_{:06}.mp4", episode_index));

            // Use native rsmpeg encoder
            match RsmpegMp4Encoder::with_config(encoder_config.clone())
                .encode_buffer(&buffer, &video_path)
            {
                Ok(()) => {
                    stats.images_encoded += buffer.len();
                    tracing::debug!(
                        camera = %camera,
                        frames = buffer.len(),
                        path = %video_path.display(),
                        "Encoded MP4 video"
                    );
                }
                Err(VideoEncoderError::FfmpegNotFound) => {
                    tracing::error!(
                        "Native FFmpeg encoder failed. Camera '{}' videos will not be available.",
                        camera
                    );
                    return Err(roboflow_core::RoboflowError::unsupported(
                        "Video encoding requires FFmpeg libraries. Ensure rsmpeg can find libavcodec.",
                    ));
                }
                Err(e) => {
                    tracing::error!(
                        camera = %camera,
                        error = %e,
                        "Failed to encode video"
                    );
                    return Err(roboflow_core::RoboflowError::encode(
                        "VideoEncoder",
                        format!("Failed to encode video for camera '{}': {}", camera, e),
                    ));
                }
            }

            if let Ok(metadata) = fs::metadata(&video_path) {
                stats.output_bytes += metadata.len();
            }

            if use_cloud_storage {
                video_files.push((video_path.clone(), camera.clone()));
            }
        }
    }

    Ok((video_files, stats))
}

/// Encode videos in parallel using rayon.
fn encode_videos_parallel(
    camera_data: Vec<(String, Vec<ImageData>)>,
    videos_dir: &Path,
    encoder_config: &VideoEncoderConfig,
    episode_index: usize,
    parallel_jobs: usize,
    use_cloud_storage: bool,
) -> Result<(Vec<(PathBuf, String)>, EncodeStats)> {
    use rayon::prelude::*;

    // Configure rayon thread pool
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(parallel_jobs)
        .build()
        .map_err(|e| roboflow_core::RoboflowError::encode("ThreadPool", e.to_string()))?;

    // Create all camera directories before parallel encoding to avoid race
    for (camera, _) in &camera_data {
        let camera_dir = videos_dir.join(camera);
        fs::create_dir_all(&camera_dir).map_err(|e| {
            roboflow_core::RoboflowError::encode(
                "VideoEncoder",
                format!("Failed to create camera directory '{}': {}", camera, e),
            )
        })?;
    }

    tracing::info!("Using native rsmpeg encoder for parallel encoding");

    // Shared counters for statistics
    let images_encoded = Arc::new(AtomicUsize::new(0));
    let output_bytes = Arc::new(AtomicU64::new(0));
    let skipped_frames = Arc::new(AtomicUsize::new(0));
    let failed_encodings = Arc::new(AtomicUsize::new(0));
    let video_files = Arc::new(std::sync::Mutex::new(Vec::new()));

    let result: Result<Vec<()>> = pool.install(|| {
        camera_data.par_iter().map(|(camera, images)| {
            let (buffer, skipped) = build_frame_buffer_static(images).map_err(|e| {
                roboflow_core::RoboflowError::encode(
                    "VideoEncoder",
                    format!("Failed to build frame buffer for camera '{}': {}", camera, e),
                )
            })?;

            if skipped > 0 {
                skipped_frames.fetch_add(skipped, Ordering::Relaxed);
            }

            if !buffer.is_empty() {
                let camera_dir = videos_dir.join(camera);
                let video_path = camera_dir.join(format!("episode_{:06}.mp4", episode_index));

                // Use native rsmpeg encoder
                match RsmpegMp4Encoder::with_config(encoder_config.clone())
                    .encode_buffer(&buffer, &video_path)
                {
                    Ok(()) => {
                        images_encoded.fetch_add(buffer.len(), Ordering::Relaxed);
                        tracing::debug!(
                            camera = %camera,
                            frames = buffer.len(),
                            path = %video_path.display(),
                            "Encoded MP4 video"
                        );

                        if use_cloud_storage {
                            let mut files = video_files.lock().map_err(|e| {
                                roboflow_core::RoboflowError::encode(
                                    "VideoEncoder",
                                    format!("Video files mutex poisoned: {}", e),
                                )
                            })?;
                            files.push((video_path.clone(), camera.clone()));
                        }
                    }
                    Err(VideoEncoderError::FfmpegNotFound) => {
                        tracing::error!("Native FFmpeg encoder failed. Please ensure FFmpeg libraries are available.");
                        failed_encodings.fetch_add(1, Ordering::Relaxed);
                        return Err(roboflow_core::RoboflowError::unsupported(
                            "Video encoding requires FFmpeg libraries. Ensure rsmpeg can find libavcodec."
                        ));
                    }
                    Err(e) => {
                        tracing::error!(
                            camera = %camera,
                            error = %e,
                            "Failed to encode video"
                        );
                        failed_encodings.fetch_add(1, Ordering::Relaxed);
                        return Err(roboflow_core::RoboflowError::encode(
                            "VideoEncoder",
                            format!("Failed to encode video for camera '{}': {}", camera, e)
                        ));
                    }
                }

                if let Ok(metadata) = fs::metadata(&video_path) {
                    output_bytes.fetch_add(metadata.len(), Ordering::Relaxed);
                }
            }

            Ok(())
        }).collect()
    });

    result?;

    let stats = EncodeStats {
        images_encoded: images_encoded.load(Ordering::Relaxed),
        skipped_frames: skipped_frames.load(Ordering::Relaxed),
        failed_encodings: failed_encodings.load(Ordering::Relaxed),
        decode_failures: skipped_frames.load(Ordering::Relaxed), // Decode failures tracked as skips
        output_bytes: output_bytes.load(Ordering::Relaxed),
    };

    let files = video_files
        .lock()
        .map_err(|e| {
            roboflow_core::RoboflowError::encode(
                "VideoEncoder",
                format!("Video files mutex poisoned during upload: {}", e),
            )
        })?
        .clone();

    Ok((files, stats))
}

/// Static version of build_frame_buffer for use in parallel context.
///
/// Returns (buffer, skipped_frame_count) where skipped frames are those
/// that had dimension mismatches or failed to decode (when encoded).
/// Compressed images (JPEG/PNG) are decoded to RGB before encoding to MP4.
pub fn build_frame_buffer_static(images: &[ImageData]) -> Result<(VideoFrameBuffer, usize)> {
    let mut buffer = VideoFrameBuffer::new();
    let mut skipped = 0usize;
    let mut decode_failures = 0usize;

    for img in images {
        if img.width == 0 || img.height == 0 {
            tracing::debug!("Skipping image with zero dimensions");
            skipped += 1;
            continue;
        }

        let (width, height, rgb_data) = if img.is_encoded {
            match decode_image_to_rgb(img) {
                Some((w, h, data)) => (w, h, data),
                None => {
                    decode_failures += 1;
                    skipped += 1;
                    tracing::debug!(
                        width = img.width,
                        height = img.height,
                        data_len = img.data.len(),
                        "Skipping encoded image (decode failed)"
                    );
                    continue;
                }
            }
        } else {
            (img.width, img.height, img.data.clone())
        };

        let video_frame = VideoFrame::new(width, height, rgb_data);
        if let Err(e) = buffer.add_frame(video_frame) {
            skipped += 1;
            tracing::warn!(
                expected_width = buffer.width.unwrap_or(0),
                expected_height = buffer.height.unwrap_or(0),
                actual_width = width,
                actual_height = height,
                error = %e,
                "Frame dimension mismatch - skipping frame"
            );
        }
    }

    // When all frames were skipped, log and continue (no video for this camera, episode still succeeds)
    if !images.is_empty() && buffer.is_empty() {
        tracing::warn!(
            frame_count = images.len(),
            decode_failures,
            "All frames skipped for video (decode failed or dimension mismatch); \
             Parquet and other cameras will still be written. \
             Check image data integrity and codec compatibility."
        );
    }

    // Log decode failure summary
    if decode_failures > 0 {
        tracing::warn!(
            decode_failures,
            total_frames = images.len(),
            failure_rate = format!(
                "{:.1}%",
                (decode_failures as f64 / images.len() as f64) * 100.0
            ),
            "Image decode failures detected"
        );
    }

    Ok((buffer, skipped))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ImageData;

    /// Create a test ImageData with raw RGB pixels
    fn create_test_image(width: u32, height: u32, is_encoded: bool) -> ImageData {
        let data = if is_encoded {
            // Minimal valid JPEG header (not actually valid, just for testing)
            vec![0xFF, 0xD8, 0xFF, 0xE0]
        } else {
            // Raw RGB data
            vec![0u8; (width as usize) * (height as usize) * 3]
        };
        ImageData {
            width,
            height,
            data,
            original_timestamp: 0,
            is_encoded,
            is_depth: false,
        }
    }

    #[test]
    fn test_build_frame_buffer_empty_images() {
        let images: Vec<ImageData> = vec![];
        let (buffer, skipped) = build_frame_buffer_static(&images).unwrap();
        assert!(buffer.is_empty());
        assert_eq!(skipped, 0);
    }

    #[test]
    fn test_build_frame_buffer_zero_dimensions() {
        let images = vec![
            create_test_image(0, 100, false),
            create_test_image(100, 0, false),
            create_test_image(0, 0, false),
        ];
        let (buffer, skipped) = build_frame_buffer_static(&images).unwrap();
        assert!(buffer.is_empty());
        assert_eq!(skipped, 3);
    }

    #[test]
    fn test_build_frame_buffer_single_frame() {
        let images = vec![create_test_image(64, 48, false)];
        let (buffer, skipped) = build_frame_buffer_static(&images).unwrap();
        assert!(!buffer.is_empty());
        assert_eq!(buffer.len(), 1);
        assert_eq!(skipped, 0);
    }

    #[test]
    fn test_build_frame_buffer_multiple_frames() {
        let images = vec![
            create_test_image(64, 48, false),
            create_test_image(64, 48, false),
            create_test_image(64, 48, false),
        ];
        let (buffer, skipped) = build_frame_buffer_static(&images).unwrap();
        assert!(!buffer.is_empty());
        assert_eq!(buffer.len(), 3);
        assert_eq!(skipped, 0);
    }

    #[test]
    fn test_build_frame_buffer_dimension_mismatch() {
        // First frame sets the expected dimensions
        // Second frame has different dimensions, should be skipped
        let images = vec![
            create_test_image(64, 48, false),
            create_test_image(32, 24, false), // Different size
        ];
        let (buffer, skipped) = build_frame_buffer_static(&images).unwrap();
        assert_eq!(buffer.len(), 1); // Only first frame
        assert_eq!(skipped, 1); // Second frame skipped
    }

    #[test]
    fn test_build_frame_buffer_encoded_image_decode_failure() {
        // Encoded image with invalid JPEG data will fail to decode
        let images = vec![create_test_image(64, 48, true)];
        let (buffer, skipped) = build_frame_buffer_static(&images).unwrap();
        assert!(buffer.is_empty());
        assert_eq!(skipped, 1);
    }

    #[test]
    fn test_encode_stats_default() {
        let stats = EncodeStats::default();
        assert_eq!(stats.images_encoded, 0);
        assert_eq!(stats.skipped_frames, 0);
        assert_eq!(stats.failed_encodings, 0);
        assert_eq!(stats.decode_failures, 0);
        assert_eq!(stats.output_bytes, 0);
    }

    #[test]
    fn test_encode_videos_empty_input() {
        let image_buffers: Vec<(String, Vec<ImageData>)> = vec![];
        let video_config = create_test_video_config();
        let temp_dir = tempfile::tempdir().unwrap();

        let (videos, stats) =
            encode_videos(&image_buffers, 0, temp_dir.path(), &video_config, 30, false).unwrap();

        assert!(videos.is_empty());
        assert_eq!(stats.images_encoded, 0);
    }

    #[test]
    fn test_encode_videos_empty_camera() {
        // Camera with no images should be filtered out
        let image_buffers = vec![("camera_0".to_string(), vec![])];
        let video_config = create_test_video_config();
        let temp_dir = tempfile::tempdir().unwrap();

        let (videos, stats) =
            encode_videos(&image_buffers, 0, temp_dir.path(), &video_config, 30, false).unwrap();

        assert!(videos.is_empty());
        assert_eq!(stats.images_encoded, 0);
    }

    /// Create a minimal ResolvedConfig for testing
    fn create_test_video_config() -> ResolvedConfig {
        ResolvedConfig {
            codec: "libx264".to_string(),
            crf: 23,
            preset: "medium".to_string(),
            pixel_format: "yuv420p".to_string(),
            hardware_accelerated: false,
            parallel_jobs: 1,
        }
    }
}
