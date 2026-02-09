// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Video encoding for LeRobot datasets.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::common::ImageData;
use crate::common::video::VideoEncoderError;
use crate::common::video::{Mp4Encoder, VideoEncoderConfig, VideoFrame, VideoFrameBuffer};
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
    let encoder = Mp4Encoder::with_config(encoder_config.clone());
    let mut stats = EncodeStats::default();
    let mut video_files = Vec::new();

    for (camera, images) in camera_data {
        let (buffer, skipped) = build_frame_buffer_static(&images)?;
        stats.skipped_frames += skipped;

        if !buffer.is_empty() {
            // camera key already contains the full feature path
            let camera_dir = videos_dir.join(&camera);
            fs::create_dir_all(&camera_dir)?;

            let video_path = camera_dir.join(format!("episode_{:06}.mp4", episode_index));

            match encoder.encode_buffer(&buffer, &video_path) {
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
                        "ffmpeg not found. Please install ffmpeg to encode videos. \
                         Camera '{}' videos will not be available in the dataset.",
                        camera
                    );
                    return Err(roboflow_core::RoboflowError::unsupported(
                        "Video encoding requires ffmpeg. Install ffmpeg and ensure it's in your PATH.",
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

                let encoder = Mp4Encoder::with_config(encoder_config.clone());

                match encoder.encode_buffer(&buffer, &video_path) {
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
                        tracing::error!("ffmpeg not found. Please install ffmpeg to encode videos.");
                        failed_encodings.fetch_add(1, Ordering::Relaxed);
                        return Err(roboflow_core::RoboflowError::unsupported(
                            "Video encoding requires ffmpeg. Install ffmpeg and ensure it's in your PATH."
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

/// JPEG magic: FF D8 FF
const JPEG_MAGIC: &[u8] = &[0xFF, 0xD8, 0xFF];
/// PNG magic: 89 50 4E 47 0D 0A 1A 0A
const PNG_MAGIC: &[u8] = &[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];

/// Decode compressed image (JPEG/PNG) to RGB when `is_encoded` is true.
/// Tries raw payload first, then skips an 8-byte header if present (e.g. ROS/serialization prefix).
/// Returns None if decode fails.
fn decode_image_to_rgb(img: &ImageData) -> Option<(u32, u32, Vec<u8>)> {
    if let Some(decoded) = try_decode_payload(&img.data) {
        return Some(decoded);
    }
    // Some codecs (e.g. ROS bag CDR) prefix the image with an 8-byte header (e.g. zeros or length).
    // Try skipping the first 8 bytes and decode again.
    if img.data.len() > 8
        && let Some(decoded) = try_decode_payload(&img.data[8..])
    {
        return Some(decoded);
    }
    None
}

/// Try to decode a byte slice as JPEG or PNG. Returns (width, height, rgb_data) on success.
fn try_decode_payload(data: &[u8]) -> Option<(u32, u32, Vec<u8>)> {
    use crate::image::{ImageFormat, decode_compressed_image};

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

/// Static version of build_frame_buffer for use in parallel context.
///
/// Returns (buffer, skipped_frame_count) where skipped frames are those
/// that had dimension mismatches or failed to decode (when encoded).
/// Compressed images (JPEG/PNG) are decoded to RGB before encoding to MP4.
pub fn build_frame_buffer_static(images: &[ImageData]) -> Result<(VideoFrameBuffer, usize)> {
    let mut buffer = VideoFrameBuffer::new();
    let mut skipped = 0usize;

    for img in images {
        if img.width == 0 || img.height == 0 {
            continue;
        }

        let (width, height, rgb_data) = if img.is_encoded {
            match decode_image_to_rgb(img) {
                Some((w, h, data)) => (w, h, data),
                None => {
                    skipped += 1;
                    tracing::debug!("Skipping encoded image (decode failed)");
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
            "All frames skipped for video (decode failed or dimension mismatch); \
             check logs above for 'Compressed image decode failed' to fix. \
             Parquet and other cameras will still be written."
        );
    }

    Ok((buffer, skipped))
}
