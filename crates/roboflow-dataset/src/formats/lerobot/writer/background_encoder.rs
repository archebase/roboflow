// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Background video encoder for pipelined video encoding.
//!
//! Runs a `ConcurrentVideoEncoder` on a dedicated thread, accepting
//! `EncodeRequest` batches via a channel. This allows the writer to
//! continue reading messages while video encoding proceeds in the background.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;

use roboflow_core::Result;
use roboflow_media::video::{
    ConcurrentEncoderConfig, ConcurrentVideoEncoder, EncodeStats, VideoEncoderConfig,
};

use crate::formats::common::ImageData;

/// A batch of images for a single camera to be encoded.
pub struct EncodeRequest {
    /// Camera name (e.g., "observation.images.cam_left").
    pub camera: String,
    /// Images to encode.
    pub images: Vec<ImageData>,
}

/// Result returned when the background encoder finishes.
pub struct BackgroundEncoderResult {
    /// Aggregate encoding statistics.
    pub stats: EncodeStats,
    /// Names of cameras that were encoded.
    pub cameras_encoded: Vec<String>,
}

/// Background video encoder that runs on a dedicated thread.
///
/// Images are sent via `send()` and the encoder is drained by calling `finish()`.
pub struct BackgroundVideoEncoder {
    /// Sender for encode requests.
    tx: Option<mpsc::Sender<EncodeRequest>>,
    /// Join handle for the encoder thread.
    handle: Option<thread::JoinHandle<Result<BackgroundEncoderResult>>>,
}

impl BackgroundVideoEncoder {
    /// Create a new background video encoder.
    ///
    /// # Arguments
    ///
    /// * `video_config` - Video encoder configuration (codec, quality, etc.)
    /// * `output_dir`   - Root output directory (video files are placed under `videos/chunk-NNN/`)
    /// * `chunk_index`  - Chunk index for the LeRobot v2.1 path scheme
    /// * `episode_index` - Episode index for file naming
    pub fn new(
        video_config: VideoEncoderConfig,
        output_dir: PathBuf,
        chunk_index: u32,
        episode_index: usize,
    ) -> Result<Self> {
        let (tx, rx) = mpsc::channel::<EncodeRequest>();

        let handle = thread::Builder::new()
            .name("bg-video-encoder".to_string())
            .spawn(move || {
                Self::encoder_thread(rx, video_config, output_dir, chunk_index, episode_index)
            })
            .map_err(|e| {
                roboflow_core::RoboflowError::other(format!(
                    "Failed to spawn background encoder thread: {}",
                    e
                ))
            })?;

        Ok(Self {
            tx: Some(tx),
            handle: Some(handle),
        })
    }

    /// Send an encode request to the background thread.
    pub fn send(&self, request: EncodeRequest) -> Result<()> {
        if let Some(ref tx) = self.tx {
            tx.send(request).map_err(|e| {
                roboflow_core::RoboflowError::other(format!("Failed to send encode request: {}", e))
            })
        } else {
            Err(roboflow_core::RoboflowError::other(
                "Background encoder already finished".to_string(),
            ))
        }
    }

    /// Finish encoding: close the channel, join the thread, and return results.
    pub fn finish(mut self) -> Result<BackgroundEncoderResult> {
        // Drop sender to signal the encoder thread to finish
        drop(self.tx.take());

        match self.handle.take() {
            Some(handle) => handle.join().map_err(|e| {
                roboflow_core::RoboflowError::other(format!(
                    "Background encoder thread panicked: {:?}",
                    e
                ))
            })?,
            None => Err(roboflow_core::RoboflowError::other(
                "Background encoder already finished".to_string(),
            )),
        }
    }

    /// Encoder thread: receives requests, feeds them to a ConcurrentVideoEncoder,
    /// and returns aggregate stats on completion.
    fn encoder_thread(
        rx: mpsc::Receiver<EncodeRequest>,
        video_config: VideoEncoderConfig,
        output_dir: PathBuf,
        chunk_index: u32,
        episode_index: usize,
    ) -> Result<BackgroundEncoderResult> {
        let config = ConcurrentEncoderConfig::with_video_config(video_config);
        let mut encoder = ConcurrentVideoEncoder::new(config)?;
        let mut registered_cameras = HashSet::new();

        // Process incoming requests
        while let Ok(request) = rx.recv() {
            let camera = request.camera;

            // Lazily register camera if not yet added
            if registered_cameras.insert(camera.clone()) {
                let video_path = output_dir.join(format!(
                    "videos/chunk-{:03}/{}/episode_{:06}.mp4",
                    chunk_index, camera, episode_index
                ));
                encoder.add_camera(&camera, video_path)?;
            }

            // Feed all images for this camera
            for image in request.images {
                encoder.add_frame(&camera, image)?;
            }
        }

        // Finalize all encoders
        let results = encoder.finalize()?;

        let mut stats = EncodeStats {
            images_encoded: 0,
            skipped_frames: 0,
            failed_encodings: 0,
            output_bytes: 0,
        };
        let mut cameras_encoded = Vec::new();

        for result in &results {
            stats.images_encoded += result.frames_encoded;
            stats.skipped_frames += result.frames_skipped;
            cameras_encoded.push(result.camera.clone());

            // Accumulate output bytes from the produced file
            if let Ok(meta) = std::fs::metadata(&result.output_path) {
                stats.output_bytes += meta.len();
            }
        }

        Ok(BackgroundEncoderResult {
            stats,
            cameras_encoded,
        })
    }
}
