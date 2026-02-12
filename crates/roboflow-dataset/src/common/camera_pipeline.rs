// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Per-camera video encoding pipeline.
//!
//! This module provides `CameraPipeline` which coordinates frame buffering,
//! fragment encoding, and upload for a single camera stream.
//!
//! # Design
//!
//! - Frame accumulator with backpressure (bounded buffer)
//! - Fragment encoding when buffer is full
//! - Send fragments to uploader thread
//! - Handle errors with abort

use std::path::PathBuf;

use crossbeam_channel::{Receiver, Sender};
use roboflow_core::{Result, RoboflowError};

use crate::common::fragment_encoder::{FragmentEncoder, FragmentEncoderConfig};
use crate::common::fragment_uploader::UploadCommand;
use crate::common::video::VideoFrame;
use crate::common::{ImageData, decode_to_rgb};

/// Command for the camera pipeline thread.
#[derive(Debug)]
pub enum PipelineCommand {
    /// Add a frame to the pipeline.
    AddFrame { image: ImageData },
    /// Flush remaining frames and finish.
    Flush,
    /// Shutdown immediately (abort).
    Shutdown,
}

/// Configuration for camera pipeline.
#[derive(Debug, Clone)]
pub struct CameraPipelineConfig {
    /// Camera name.
    pub camera: String,
    /// Maximum frames to buffer before encoding a fragment.
    pub frames_per_fragment: usize,
    /// Temp directory for fragment files.
    pub temp_dir: PathBuf,
    /// Video encoder configuration.
    pub video_config: crate::common::video::VideoEncoderConfig,
}

impl Default for CameraPipelineConfig {
    fn default() -> Self {
        Self {
            camera: String::new(),
            frames_per_fragment: 300, // 10 seconds @ 30fps
            temp_dir: std::env::temp_dir(),
            video_config: crate::common::video::VideoEncoderConfig::default(),
        }
    }
}

/// Result from camera pipeline finalization.
#[derive(Debug, Clone)]
pub struct CameraPipelineResult {
    /// Camera name.
    pub camera: String,
    /// Total frames encoded.
    pub frames_encoded: usize,
    /// Total fragments created.
    pub fragments_created: usize,
    /// Frames skipped (decode failures, dimension mismatches).
    pub frames_skipped: usize,
}

/// Per-camera encoding pipeline.
///
/// Runs in its own thread, receiving frames, buffering, encoding fragments,
/// and sending them to the uploader.
pub struct CameraPipeline {
    /// Camera name.
    camera: String,
    /// Command receiver.
    cmd_rx: Receiver<PipelineCommand>,
    /// Upload command sender.
    upload_tx: Sender<UploadCommand>,
    /// Fragment encoder.
    encoder: FragmentEncoder,
    /// Frame buffer.
    frame_buffer: Vec<VideoFrame>,
    /// Max frames per fragment.
    frames_per_fragment: usize,
    /// Video dimensions (set from first frame).
    width: u32,
    height: u32,
    /// Statistics.
    frames_encoded: usize,
    frames_skipped: usize,
    fragments_created: usize,
}

impl CameraPipeline {
    /// Create a new camera pipeline.
    pub fn new(
        config: CameraPipelineConfig,
        cmd_rx: Receiver<PipelineCommand>,
        upload_tx: Sender<UploadCommand>,
    ) -> Result<Self> {
        let encoder_config = FragmentEncoderConfig {
            video: config.video_config,
            temp_dir: config.temp_dir,
            max_frames_per_fragment: config.frames_per_fragment,
        };

        let encoder = FragmentEncoder::new(encoder_config)?;

        Ok(Self {
            camera: config.camera,
            cmd_rx,
            upload_tx,
            encoder,
            frame_buffer: Vec::with_capacity(config.frames_per_fragment),
            frames_per_fragment: config.frames_per_fragment,
            width: 0,
            height: 0,
            frames_encoded: 0,
            frames_skipped: 0,
            fragments_created: 0,
        })
    }

    /// Run the pipeline thread.
    ///
    /// This blocks until a Flush or Shutdown command is received.
    ///
    /// # Returns
    ///
    /// Returns `CameraPipelineResult` with encoding statistics if successful,
    /// or an error if the pipeline fails or is shutdown.
    pub fn run(self) -> Result<CameraPipelineResult> {
        // Destructure self to take ownership of fields
        let Self {
            camera,
            cmd_rx,
            upload_tx,
            mut encoder,
            mut frame_buffer,
            frames_per_fragment,
            mut width,
            mut height,
            mut frames_encoded,
            mut frames_skipped,
            mut fragments_created,
        } = self;

        for cmd in cmd_rx {
            match cmd {
                PipelineCommand::AddFrame { image } => {
                    // Handle frame inline
                    // Skip images with zero dimensions
                    if image.width == 0 || image.height == 0 {
                        tracing::debug!(camera = %camera, "Skipping frame with zero dimensions");
                        frames_skipped += 1;
                        continue;
                    }

                    // Set dimensions from first frame
                    if width == 0 {
                        width = image.width;
                        height = image.height;
                    }

                    // Validate dimensions
                    if image.width != width || image.height != height {
                        tracing::debug!(
                            camera = %camera,
                            expected = format!("{}x{}", width, height),
                            actual = format!("{}x{}", image.width, image.height),
                            "Skipping frame due to dimension mismatch"
                        );
                        frames_skipped += 1;
                        continue;
                    }

                    // Decode image to RGB
                    let (w, h, rgb_data) = match decode_to_rgb(&image) {
                        Some(data) => data,
                        None => {
                            tracing::debug!(camera = %camera, "Failed to decode frame");
                            frames_skipped += 1;
                            continue;
                        }
                    };

                    // Create video frame and add to buffer
                    let frame = VideoFrame::new(w, h, rgb_data);
                    frame_buffer.push(frame);
                    frames_encoded += 1;

                    // Encode fragment when buffer is full
                    if frame_buffer.len() >= frames_per_fragment {
                        let frames = std::mem::take(&mut frame_buffer);
                        let fragment = encoder.encode(frames)?;
                        upload_tx
                            .send(UploadCommand::UploadFragment {
                                camera: camera.clone(),
                                fragment,
                            })
                            .map_err(|e| {
                                RoboflowError::encode(
                                    "CameraPipeline",
                                    format!("Failed to send fragment to uploader: {}", e),
                                )
                            })?;
                        fragments_created += 1;
                    }
                }
                PipelineCommand::Flush => {
                    break;
                }
                PipelineCommand::Shutdown => {
                    // Signal uploader to abort
                    if let Err(e) = upload_tx.send(UploadCommand::AbortAll) {
                        tracing::warn!(camera = %camera, error = %e, "Failed to send abort command to uploader");
                    }
                    return Err(RoboflowError::encode(
                        "CameraPipeline",
                        format!("Camera {} shutdown requested", camera),
                    ));
                }
            }
        }

        // Flush remaining frames
        if !frame_buffer.is_empty() {
            let frames = std::mem::take(&mut frame_buffer);
            let fragment = encoder.encode(frames)?;
            upload_tx
                .send(UploadCommand::UploadFragment {
                    camera: camera.clone(),
                    fragment,
                })
                .map_err(|e| {
                    RoboflowError::encode(
                        "CameraPipeline",
                        format!("Failed to send fragment to uploader: {}", e),
                    )
                })?;
            fragments_created += 1;
        }

        // Signal uploader that this camera is done
        upload_tx
            .send(UploadCommand::Finish {
                camera: camera.clone(),
            })
            .map_err(|e| {
                RoboflowError::encode(
                    "CameraPipeline",
                    format!("Failed to send finish command: {}", e),
                )
            })?;

        // Log summary
        if frames_skipped > 0 {
            tracing::warn!(
                camera = %camera,
                frames_encoded,
                frames_skipped,
                "Camera pipeline completed with skipped frames"
            );
        } else {
            tracing::info!(
                camera = %camera,
                frames_encoded,
                fragments = fragments_created,
                "Camera pipeline completed"
            );
        }

        Ok(CameraPipelineResult {
            camera,
            frames_encoded,
            fragments_created,
            frames_skipped,
        })
    }
}

/// Handle for a running camera pipeline thread.
pub struct CameraPipelineHandle {
    /// Camera name.
    pub camera: String,
    /// Command sender.
    pub cmd_tx: Sender<PipelineCommand>,
    /// Thread join handle.
    pub thread_handle: Option<std::thread::JoinHandle<Result<CameraPipelineResult>>>,
}

impl CameraPipelineHandle {
    /// Send a frame to the pipeline.
    pub fn add_frame(&self, image: ImageData) -> Result<()> {
        self.cmd_tx
            .send(PipelineCommand::AddFrame { image })
            .map_err(|e| {
                RoboflowError::encode(
                    "CameraPipelineHandle",
                    format!("Failed to send frame: {}", e),
                )
            })
    }

    /// Signal the pipeline to flush and finish.
    pub fn flush(&self) -> Result<()> {
        self.cmd_tx.send(PipelineCommand::Flush).map_err(|e| {
            RoboflowError::encode(
                "CameraPipelineHandle",
                format!("Failed to send flush: {}", e),
            )
        })
    }

    /// Signal the pipeline to shutdown immediately.
    pub fn shutdown(&self) -> Result<()> {
        self.cmd_tx.send(PipelineCommand::Shutdown).map_err(|e| {
            RoboflowError::encode(
                "CameraPipelineHandle",
                format!("Failed to send shutdown: {}", e),
            )
        })
    }

    /// Wait for the pipeline to finish and get the result.
    pub fn join(mut self) -> Result<CameraPipelineResult> {
        let handle = self.thread_handle.take();
        if let Some(handle) = handle {
            handle.join().map_err(|e| {
                RoboflowError::other(format!("Camera pipeline thread panicked: {:?}", e))
            })?
        } else {
            Err(RoboflowError::other("Pipeline thread already joined"))
        }
    }
}

/// Spawn a camera pipeline in a new thread.
///
/// # Arguments
///
/// * `config` - Pipeline configuration including camera name and encoding settings
/// * `upload_tx` - Sender for upload commands to the uploader thread
///
/// # Returns
///
/// Returns a `CameraPipelineHandle` for sending commands to the pipeline.
pub fn spawn_camera_pipeline(
    config: CameraPipelineConfig,
    upload_tx: Sender<UploadCommand>,
) -> Result<CameraPipelineHandle> {
    let camera = config.camera.clone();
    let (cmd_tx, cmd_rx) = crossbeam_channel::bounded(64);

    let pipeline = CameraPipeline::new(config, cmd_rx, upload_tx)?;

    let thread_name = format!("camera-pipeline-{}", camera);
    let handle = std::thread::Builder::new()
        .name(thread_name)
        .spawn(move || pipeline.run())
        .map_err(|e| {
            RoboflowError::other(format!("Failed to spawn camera pipeline thread: {}", e))
        })?;

    Ok(CameraPipelineHandle {
        camera,
        cmd_tx,
        thread_handle: Some(handle),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_config_default() {
        let config = CameraPipelineConfig::default();
        assert_eq!(config.frames_per_fragment, 300);
        assert_eq!(config.video_config.fps, 30);
    }
}
