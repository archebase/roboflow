// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use rayon::prelude::*;

use roboflow_core::{Result, RoboflowError};

use crate::ImageData;
use crate::image::decode_image_to_rgb;

use super::{
    OutputConfig, ResolvedConfig, VideoEncoder, VideoEncoderConfig, VideoFrame, VideoFrameBuffer,
};

#[derive(Debug, Default)]
pub struct EncodeStats {
    pub images_encoded: usize,
    pub skipped_frames: usize,
    pub failed_encodings: usize,
    pub output_bytes: u64,
}

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
    let camera_data: Vec<(String, Vec<ImageData>)> = image_buffers
        .iter()
        .filter(|(_, images)| !images.is_empty())
        .map(|(camera, images)| (camera.clone(), images.clone()))
        .collect();

    if camera_data.is_empty() {
        return Ok((Vec::new(), EncodeStats::default()));
    }

    let use_parallel = video_config.hardware_accelerated
        && video_config.parallel_jobs > 1
        && camera_data.len() > 1;

    if use_parallel {
        let concurrent_jobs = video_config.parallel_jobs.min(camera_data.len());
        encode_videos_parallel(
            camera_data,
            videos_dir,
            &encoder_config,
            episode_index,
            concurrent_jobs,
            use_cloud_storage,
        )
    } else {
        encode_videos_sequential(
            camera_data,
            videos_dir,
            &encoder_config,
            episode_index,
            use_cloud_storage,
        )
    }
}

fn encode_videos_sequential(
    camera_data: Vec<(String, Vec<ImageData>)>,
    videos_dir: &Path,
    encoder_config: &VideoEncoderConfig,
    episode_index: usize,
    use_cloud_storage: bool,
) -> Result<(Vec<(PathBuf, String)>, EncodeStats)> {
    let mut stats = EncodeStats::default();
    let mut video_files = Vec::new();

    for (camera, images) in camera_data {
        let (buffer, skipped) = build_frame_buffer_static(&images)?;
        stats.skipped_frames += skipped;

        if buffer.is_empty() {
            continue;
        }

        let camera_dir = videos_dir.join(&camera);
        fs::create_dir_all(&camera_dir)?;
        let video_path = camera_dir.join(format!("episode_{:06}.mp4", episode_index));

        let output = OutputConfig::file(&video_path);
        let mut encoder = VideoEncoder::new(encoder_config.clone(), output).map_err(|e| {
            RoboflowError::encode(
                "VideoEncoder",
                format!("Failed to create encoder for camera '{}': {}", camera, e),
            )
        })?;

        for frame in &buffer.frames {
            encoder
                .encode_frame(frame.data(), frame.width, frame.height)
                .map_err(|e| {
                    RoboflowError::encode(
                        "VideoEncoder",
                        format!("Failed to encode frame for camera '{}': {}", camera, e),
                    )
                })?;
        }

        let result = encoder.finalize().map_err(|e| {
            RoboflowError::encode(
                "VideoEncoder",
                format!("Failed to finalize encoder for camera '{}': {}", camera, e),
            )
        })?;

        stats.images_encoded += buffer.len();
        stats.output_bytes += result.bytes_written;

        if use_cloud_storage {
            video_files.push((video_path, camera));
        }
    }

    Ok((video_files, stats))
}

fn encode_videos_parallel(
    camera_data: Vec<(String, Vec<ImageData>)>,
    videos_dir: &Path,
    encoder_config: &VideoEncoderConfig,
    episode_index: usize,
    parallel_jobs: usize,
    use_cloud_storage: bool,
) -> Result<(Vec<(PathBuf, String)>, EncodeStats)> {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(parallel_jobs)
        .build()
        .map_err(|e| RoboflowError::encode("ThreadPool", e.to_string()))?;

    for (camera, _) in &camera_data {
        fs::create_dir_all(videos_dir.join(camera)).map_err(|e| {
            RoboflowError::encode(
                "VideoEncoder",
                format!("Failed to create camera directory '{}': {}", camera, e),
            )
        })?;
    }

    let images_encoded = Arc::new(AtomicUsize::new(0));
    let output_bytes = Arc::new(AtomicU64::new(0));
    let skipped_frames = Arc::new(AtomicUsize::new(0));
    let failed_encodings = Arc::new(AtomicUsize::new(0));
    let video_files = Arc::new(std::sync::Mutex::new(Vec::new()));

    let result: Result<Vec<()>> = pool.install(|| {
        camera_data
            .par_iter()
            .map(|(camera, images)| {
                let (buffer, skipped) = build_frame_buffer_static(images)?;
                if skipped > 0 {
                    skipped_frames.fetch_add(skipped, Ordering::Relaxed);
                }
                if buffer.is_empty() {
                    return Ok(());
                }

                let video_path = videos_dir
                    .join(camera)
                    .join(format!("episode_{:06}.mp4", episode_index));
                let output = OutputConfig::file(&video_path);
                let mut encoder =
                    VideoEncoder::new(encoder_config.clone(), output).map_err(|e| {
                        failed_encodings.fetch_add(1, Ordering::Relaxed);
                        RoboflowError::encode(
                            "VideoEncoder",
                            format!("Failed to create encoder for camera '{}': {}", camera, e),
                        )
                    })?;

                for frame in &buffer.frames {
                    if let Err(e) = encoder.encode_frame(frame.data(), frame.width, frame.height) {
                        failed_encodings.fetch_add(1, Ordering::Relaxed);
                        return Err(RoboflowError::encode(
                            "VideoEncoder",
                            format!("Failed to encode frame for camera '{}': {}", camera, e),
                        ));
                    }
                }

                let result = match encoder.finalize() {
                    Ok(r) => r,
                    Err(e) => {
                        failed_encodings.fetch_add(1, Ordering::Relaxed);
                        return Err(RoboflowError::encode(
                            "VideoEncoder",
                            format!("Failed to finalize encoder for camera '{}': {}", camera, e),
                        ));
                    }
                };

                images_encoded.fetch_add(buffer.len(), Ordering::Relaxed);
                output_bytes.fetch_add(result.bytes_written, Ordering::Relaxed);
                if use_cloud_storage {
                    let mut files = video_files.lock().map_err(|e| {
                        RoboflowError::encode(
                            "VideoEncoder",
                            format!("Video files mutex poisoned: {}", e),
                        )
                    })?;
                    files.push((video_path, camera.clone()));
                }
                Ok(())
            })
            .collect()
    });

    result?;
    let files = video_files
        .lock()
        .map_err(|e| RoboflowError::encode("VideoEncoder", format!("Mutex poisoned: {}", e)))?
        .clone();

    Ok((
        files,
        EncodeStats {
            images_encoded: images_encoded.load(Ordering::Relaxed),
            skipped_frames: skipped_frames.load(Ordering::Relaxed),
            failed_encodings: failed_encodings.load(Ordering::Relaxed),
            output_bytes: output_bytes.load(Ordering::Relaxed),
        },
    ))
}

pub fn build_frame_buffer_static(images: &[ImageData]) -> Result<(VideoFrameBuffer, usize)> {
    let encoded_count = images.iter().filter(|img| img.is_encoded).count();
    let use_parallel = encoded_count > 10 && rayon::current_num_threads() > 1;

    if use_parallel {
        let mut buffer = VideoFrameBuffer::new();
        let mut skipped = 0usize;

        let decoded: Vec<_> = images
            .par_iter()
            .map(|img| {
                if img.width == 0 || img.height == 0 {
                    return Ok(None);
                }
                if img.is_encoded {
                    match decode_image_to_rgb(img) {
                        Some((w, h, data)) => Ok(Some((w, h, data))),
                        None => Err(()),
                    }
                } else {
                    Ok(Some((img.width, img.height, img.data.clone())))
                }
            })
            .collect();

        for result in decoded {
            match result {
                Ok(Some((width, height, rgb_data))) => {
                    let video_frame = VideoFrame::new(width, height, rgb_data);
                    if buffer.add_frame(video_frame).is_err() {
                        skipped += 1;
                    }
                }
                Ok(None) | Err(()) => skipped += 1,
            }
        }
        Ok((buffer, skipped))
    } else {
        let mut buffer = VideoFrameBuffer::new();
        let mut skipped = 0usize;
        for img in images {
            if img.width == 0 || img.height == 0 {
                skipped += 1;
                continue;
            }
            let prepared = if img.is_encoded {
                decode_image_to_rgb(img)
            } else {
                Some((img.width, img.height, img.data.clone()))
            };

            match prepared {
                Some((width, height, rgb_data)) => {
                    let video_frame = VideoFrame::new(width, height, rgb_data);
                    if buffer.add_frame(video_frame).is_err() {
                        skipped += 1;
                    }
                }
                None => skipped += 1,
            }
        }
        Ok((buffer, skipped))
    }
}
