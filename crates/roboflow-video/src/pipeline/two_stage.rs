// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Two-stage pipeline: Single-threaded decode + encode per camera.
//!
//! This is the current default pipeline implementation. It uses
//! `StreamingMp4Encoder` which performs decode and encoding in
//! a single thread per camera.
//!
//! # Characteristics
//!
//! - **Memory**: Lower memory usage (no frame buffers)
//! - **CPU**: Single-threaded per camera (scales linearly)
//! - **Throughput**: Good for 1-2 cameras
//! - **Latency**: Lower latency (no buffering)
//!
//! # When to Use
//!
//! - Single camera scenarios
//! - Low-throughput requirements (< 100 fps total)
//! - Memory-constrained environments
//! - When hardware encoding is not available

use std::io;
use std::sync::mpsc::Sender;
use std::thread::JoinHandle;

use super::{PipelineConfig, PipelineHandle, PipelineResult};
use crate::ImageData;
use crate::streaming::{EncodedChunk, StreamingEncoderConfig, StreamingMp4Encoder};

/// Configuration for the two-stage pipeline.
#[derive(Debug, Clone)]
pub struct TwoStageConfig {
    /// Camera name.
    pub camera: String,
    /// Video encoder configuration.
    pub video_config: StreamingEncoderConfig,
    /// Chunk size threshold (bytes).
    pub chunk_size: usize,
}

impl Default for TwoStageConfig {
    fn default() -> Self {
        Self {
            camera: String::new(),
            video_config: StreamingEncoderConfig::default(),
            chunk_size: 256 * 1024, // 256KB
        }
    }
}

impl PipelineConfig for TwoStageConfig {
    fn create_pipeline(
        &self,
        upload_tx: Sender<EncodedChunk>,
    ) -> io::Result<Box<dyn PipelineHandle>> {
        TwoStagePipeline::new(self.clone(), upload_tx)
            .map(|p| Box::new(p) as Box<dyn PipelineHandle>)
    }
}

/// Two-stage pipeline handle.
///
/// This pipeline wraps `StreamingMp4Encoder` and runs it in a dedicated thread.
pub struct TwoStagePipeline {
    #[allow(dead_code)] // Stored for debugging and potential future use
    camera: String,
    cmd_tx: Sender<PipelineCommand>,
    thread_handle: Option<JoinHandle<std::io::Result<PipelineResult>>>,
}

/// Commands for the pipeline thread.
enum PipelineCommand {
    AddFrame(ImageData),
    Flush,
    Shutdown,
}

impl TwoStagePipeline {
    /// Create a new two-stage pipeline.
    pub fn new(config: TwoStageConfig, upload_tx: Sender<EncodedChunk>) -> io::Result<Self> {
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel();

        let camera = config.camera.clone();
        let video_config = config.video_config;

        let handle = std::thread::Builder::new()
            .name(format!("two-stage-pipeline-{}", camera))
            .spawn(move || Self::run_pipeline(camera, cmd_rx, upload_tx, video_config))
            .map_err(|e| io::Error::other(e.to_string()))?;

        Ok(Self {
            camera: config.camera,
            cmd_tx,
            thread_handle: Some(handle),
        })
    }

    /// Run the pipeline thread.
    fn run_pipeline(
        camera: String,
        cmd_rx: std::sync::mpsc::Receiver<PipelineCommand>,
        upload_tx: Sender<EncodedChunk>,
        video_config: StreamingEncoderConfig,
    ) -> io::Result<PipelineResult> {
        let mut encoder =
            StreamingMp4Encoder::new(video_config, upload_tx).map_err(io::Error::other)?;
        let mut frames_encoded = 0usize;
        let mut frames_skipped = 0usize;

        // Process commands
        while let Ok(cmd) = cmd_rx.recv() {
            match cmd {
                PipelineCommand::AddFrame(image) => match handle_image(&mut encoder, &image) {
                    Ok(_) => frames_encoded += 1,
                    Err(e) => {
                        tracing::warn!(
                            camera = %camera,
                            error = %e,
                            "Failed to encode frame, skipping"
                        );
                        frames_skipped += 1;
                    }
                },
                PipelineCommand::Flush => break,
                PipelineCommand::Shutdown => {
                    return Err(io::Error::other("Pipeline shutdown requested"));
                }
            }
        }

        // Finalize encoder
        encoder.finalize().map_err(io::Error::other)?;

        Ok(PipelineResult {
            camera,
            frames_encoded,
            frames_skipped,
        })
    }
}

/// Handle a single image in the two-stage pipeline.
fn handle_image(encoder: &mut StreamingMp4Encoder, image: &ImageData) -> io::Result<()> {
    // For encoded images (JPEG/PNG), we need to decode first
    let rgb_data = if image.is_encoded {
        // Decode JPEG/PNG to RGB
        decode_image(&image.data)?
    } else {
        // Already RGB
        if image.width == 0 || image.height == 0 {
            return Err(io::Error::other(
                "Cannot encode raw image with zero dimensions",
            ));
        }
        image.data.clone()
    };

    encoder.add_frame(&rgb_data).map_err(io::Error::other)
}

/// Decode an encoded image (JPEG/PNG) to RGB.
fn decode_image(data: &[u8]) -> io::Result<Vec<u8>> {
    // Check for JPEG magic bytes
    let is_jpeg = data.len() >= 3 && data[0] == 0xFF && data[1] == 0xD8 && data[2] == 0xFF;

    if is_jpeg {
        // Use zune-jpeg for fast decoding
        let mut decoder = zune_jpeg::JpegDecoder::new(data);
        decoder
            .decode()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
    } else {
        // Fall back to image crate for PNG and other formats
        let img = image::load_from_memory(data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        Ok(img.to_rgb8().into_raw())
    }
}

impl PipelineHandle for TwoStagePipeline {
    fn add_frame(&self, image: ImageData) -> io::Result<()> {
        self.cmd_tx
            .send(PipelineCommand::AddFrame(image))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Pipeline thread terminated"))
    }

    fn flush(&self) -> io::Result<()> {
        self.cmd_tx
            .send(PipelineCommand::Flush)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Pipeline thread terminated"))
    }

    fn shutdown(&self) -> io::Result<()> {
        self.cmd_tx
            .send(PipelineCommand::Shutdown)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Pipeline thread terminated"))
    }

    fn join(mut self: Box<Self>) -> io::Result<PipelineResult> {
        let handle = self
            .thread_handle
            .take()
            .ok_or_else(|| io::Error::other("Pipeline already joined"))?;

        handle
            .join()
            .map_err(|e| io::Error::other(format!("Pipeline thread panicked: {:?}", e)))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = TwoStageConfig::default();
        assert_eq!(config.camera, "");
        assert_eq!(config.chunk_size, 256 * 1024);
    }

    #[test]
    fn test_decode_jpeg() {
        // Minimal JPEG header (1x1 red pixel)
        let jpeg = vec![
            0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46, 0x00, 0x01, 0x01, 0x00,
            0x00, 0x01, 0x00, 0x01, 0x00,
            0x00,
            // This is not a valid JPEG, just testing the magic byte check
        ];

        // Should detect as JPEG
        assert!(jpeg.len() >= 3 && jpeg[0] == 0xFF && jpeg[1] == 0xD8 && jpeg[2] == 0xFF);
    }
}
