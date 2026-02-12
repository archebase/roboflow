// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Per-camera streaming video encoding pipeline.
//!
//! This module provides `CameraStreamingPipeline` which coordinates frame handling
//! and streaming encoding for a single camera, sending encoded chunks directly
//! to the upload thread without intermediate files.
//!
//! # Design
//!
//! - Single encoder initialization per camera per episode
//! - No temp files - direct streaming to upload thread
//! - Frame validation and error recovery (skip problematic frames)
//! - Backpressure via bounded channels

use crossbeam_channel::{Receiver, Sender};
use roboflow_core::{Result, RoboflowError};

use crate::common::streaming_encoder::{EncodedChunk, StreamingEncoderConfig, StreamingMp4Encoder};
use crate::common::video::VideoEncoderConfig;
use crate::common::{ImageData, decode_to_rgb};

// =============================================================================
// Commands
// =============================================================================

/// Command for the streaming pipeline thread.
#[derive(Debug)]
pub enum StreamingCommand {
    /// Add a frame to the pipeline.
    AddFrame { image: ImageData },
    /// Flush remaining frames and finish.
    Flush,
    /// Shutdown immediately (abort).
    Shutdown,
}

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for streaming pipeline.
#[derive(Debug, Clone)]
pub struct StreamingPipelineConfig {
    /// Camera name.
    pub camera: String,
    /// Video encoder configuration.
    pub video_config: VideoEncoderConfig,
    /// Chunk size for channel delivery (bytes).
    pub chunk_size: usize,
}

impl Default for StreamingPipelineConfig {
    fn default() -> Self {
        Self {
            camera: String::new(),
            video_config: VideoEncoderConfig::default(),
            chunk_size: 256 * 1024, // 256KB chunks
        }
    }
}

// =============================================================================
// Result
// =============================================================================

/// Result from streaming pipeline finalization.
#[derive(Debug, Clone)]
pub struct StreamingPipelineResult {
    /// Camera name.
    pub camera: String,
    /// Total frames encoded.
    pub frames_encoded: usize,
    /// Frames skipped (decode failures, dimension mismatches).
    pub frames_skipped: usize,
}

// =============================================================================
// Upload Command (Simplified)
// =============================================================================

/// Command for upload threads (streaming version).
#[derive(Debug)]
pub enum StreamingUploadCommand {
    /// Upload an encoded chunk.
    UploadChunk { chunk: EncodedChunk },
    /// Finish upload.
    Finish { camera: String },
    /// Abort all uploads.
    AbortAll,
}

// =============================================================================
// Camera Streaming Pipeline
// =============================================================================

/// Per-camera streaming encoding pipeline.
///
/// Runs in its own thread, receiving frames, encoding with single
/// encoder initialization, and sending encoded chunks to upload thread.
pub struct CameraStreamingPipeline {
    /// Camera name.
    camera: String,
    /// Command receiver.
    cmd_rx: Receiver<StreamingCommand>,
    /// Upload command sender.
    upload_tx: Sender<StreamingUploadCommand>,
    /// Streaming encoder (lazy initialization).
    encoder: Option<StreamingMp4Encoder>,
    /// Encoder configuration.
    config: StreamingPipelineConfig,
    /// Video dimensions (set from first frame).
    width: u32,
    height: u32,
    /// Statistics.
    frames_encoded: usize,
    frames_skipped: usize,
}

impl CameraStreamingPipeline {
    /// Create a new streaming pipeline.
    pub fn new(
        config: StreamingPipelineConfig,
        cmd_rx: Receiver<StreamingCommand>,
        upload_tx: Sender<StreamingUploadCommand>,
    ) -> Self {
        Self {
            camera: config.camera.clone(),
            cmd_rx,
            upload_tx,
            encoder: None,
            config,
            width: 0,
            height: 0,
            frames_encoded: 0,
            frames_skipped: 0,
        }
    }

    /// Run the pipeline thread.
    ///
    /// This blocks until a Flush or Shutdown command is received.
    pub fn run(mut self) -> Result<StreamingPipelineResult> {
        while let Ok(cmd) = self.cmd_rx.recv() {
            match cmd {
                StreamingCommand::AddFrame { image } => {
                    self.handle_frame(&image)?;
                }
                StreamingCommand::Flush => {
                    break;
                }
                StreamingCommand::Shutdown => {
                    self.send_abort()?;
                    return Err(RoboflowError::encode(
                        "CameraStreamingPipeline",
                        format!("Camera {} shutdown requested", self.camera),
                    ));
                }
            }
        }

        // Finalize encoder and flush remaining data
        self.finalize_encoder()?;

        // Send finish command
        self.upload_tx
            .send(StreamingUploadCommand::Finish {
                camera: self.camera.clone(),
            })
            .map_err(|e| {
                RoboflowError::encode(
                    "CameraStreamingPipeline",
                    format!("Failed to send finish command: {}", e),
                )
            })?;

        self.log_summary();

        Ok(StreamingPipelineResult {
            camera: self.camera,
            frames_encoded: self.frames_encoded,
            frames_skipped: self.frames_skipped,
        })
    }

    /// Handle a single frame.
    fn handle_frame(&mut self, image: &ImageData) -> Result<()> {
        // Skip images with zero dimensions
        if image.width == 0 || image.height == 0 {
            tracing::debug!(camera = %self.camera, "Skipping frame with zero dimensions");
            self.frames_skipped += 1;
            return Ok(());
        }

        // Set dimensions from first frame
        if self.width == 0 {
            self.width = image.width;
            self.height = image.height;
            self.initialize_encoder()?;
        }

        // Validate dimensions
        if image.width != self.width || image.height != self.height {
            tracing::debug!(
                camera = %self.camera,
                expected = format!("{}x{}", self.width, self.height),
                actual = format!("{}x{}", image.width, image.height),
                "Skipping frame due to dimension mismatch"
            );
            self.frames_skipped += 1;
            return Ok(());
        }

        // Decode image to RGB
        let (w, h, rgb_data) = match decode_to_rgb(image) {
            Some(data) => data,
            None => {
                tracing::debug!(camera = %self.camera, "Failed to decode frame");
                self.frames_skipped += 1;
                return Ok(());
            }
        };

        // Encode frame
        match self.encode_frame(&rgb_data, w, h) {
            Ok(_) => {
                self.frames_encoded += 1;
            }
            Err(e) => {
                // Error recovery: skip frame and continue
                tracing::warn!(
                    camera = %self.camera,
                    error = %e,
                    "Encoding error, skipping frame"
                );
                self.frames_skipped += 1;
            }
        }

        Ok(())
    }

    /// Initialize the streaming encoder.
    fn initialize_encoder(&mut self) -> Result<()> {
        if self.encoder.is_some() {
            return Ok(());
        }

        let (chunk_tx, chunk_rx) = std::sync::mpsc::channel();

        // Spawn a small task to forward chunks to upload thread
        let upload_tx = self.upload_tx.clone();
        let camera = self.camera.clone();
        std::thread::spawn(move || {
            for chunk in chunk_rx {
                if upload_tx
                    .send(StreamingUploadCommand::UploadChunk { chunk })
                    .is_err()
                {
                    tracing::warn!(camera = %camera, "Upload channel closed");
                    break;
                }
            }
        });

        let encoder_config = StreamingEncoderConfig::from_video_config(&self.config.video_config)
            .with_dimensions(self.width, self.height)
            .with_codec(StreamingEncoderConfig::detect_best_codec());

        let encoder = StreamingMp4Encoder::with_dimensions(
            encoder_config,
            chunk_tx,
            self.width,
            self.height,
        )?;

        tracing::info!(
            camera = %self.camera,
            width = self.width,
            height = self.height,
            "Streaming encoder initialized"
        );

        self.encoder = Some(encoder);
        Ok(())
    }

    /// Encode a single frame.
    fn encode_frame(&mut self, rgb_data: &[u8], _width: u32, _height: u32) -> Result<()> {
        let encoder = self.encoder.as_mut().ok_or_else(|| {
            RoboflowError::encode("CameraStreamingPipeline", "Encoder not initialized")
        })?;

        encoder.add_frame(rgb_data)
    }

    /// Finalize the encoder.
    fn finalize_encoder(&mut self) -> Result<()> {
        if let Some(encoder) = self.encoder.take() {
            encoder.finalize()?;
        }
        Ok(())
    }

    /// Send abort command to upload thread.
    fn send_abort(&self) -> Result<()> {
        self.upload_tx
            .send(StreamingUploadCommand::AbortAll)
            .map_err(|e| {
                RoboflowError::encode(
                    "CameraStreamingPipeline",
                    format!("Failed to send abort: {}", e),
                )
            })
    }

    /// Log pipeline summary.
    fn log_summary(&self) {
        if self.frames_skipped > 0 {
            tracing::warn!(
                camera = %self.camera,
                frames_encoded = self.frames_encoded,
                frames_skipped = self.frames_skipped,
                "Streaming pipeline completed with skipped frames"
            );
        } else {
            tracing::info!(
                camera = %self.camera,
                frames_encoded = self.frames_encoded,
                "Streaming pipeline completed"
            );
        }
    }
}

// =============================================================================
// Pipeline Handle
// =============================================================================

/// Handle for a running streaming pipeline thread.
pub struct StreamingPipelineHandle {
    /// Camera name.
    pub camera: String,
    /// Command sender.
    pub cmd_tx: Sender<StreamingCommand>,
    /// Thread join handle.
    pub thread_handle: Option<std::thread::JoinHandle<Result<StreamingPipelineResult>>>,
}

impl StreamingPipelineHandle {
    /// Send a frame to the pipeline.
    pub fn add_frame(&self, image: ImageData) -> Result<()> {
        self.cmd_tx
            .send(StreamingCommand::AddFrame { image })
            .map_err(|e| {
                RoboflowError::encode(
                    "StreamingPipelineHandle",
                    format!("Failed to send frame: {}", e),
                )
            })
    }

    /// Signal the pipeline to flush and finish.
    pub fn flush(&self) -> Result<()> {
        self.cmd_tx.send(StreamingCommand::Flush).map_err(|e| {
            RoboflowError::encode(
                "StreamingPipelineHandle",
                format!("Failed to send flush: {}", e),
            )
        })
    }

    /// Signal the pipeline to shutdown immediately.
    pub fn shutdown(&self) -> Result<()> {
        self.cmd_tx.send(StreamingCommand::Shutdown).map_err(|e| {
            RoboflowError::encode(
                "StreamingPipelineHandle",
                format!("Failed to send shutdown: {}", e),
            )
        })
    }

    /// Wait for the pipeline to finish and get the result.
    pub fn join(mut self) -> Result<StreamingPipelineResult> {
        let handle = self.thread_handle.take();
        if let Some(handle) = handle {
            handle.join().map_err(|e| {
                RoboflowError::other(format!("Streaming pipeline thread panicked: {:?}", e))
            })?
        } else {
            Err(RoboflowError::other("Pipeline thread already joined"))
        }
    }
}

// =============================================================================
// Spawn Function
// =============================================================================

/// Spawn a streaming pipeline in a new thread.
///
/// # Arguments
///
/// * `config` - Pipeline configuration including camera name and encoding settings
/// * `upload_tx` - Sender for upload commands to the upload thread
///
/// # Returns
///
/// Returns a `StreamingPipelineHandle` for sending commands to the pipeline.
pub fn spawn_streaming_pipeline(
    config: StreamingPipelineConfig,
    upload_tx: Sender<StreamingUploadCommand>,
) -> Result<StreamingPipelineHandle> {
    let camera = config.camera.clone();
    let (cmd_tx, cmd_rx) = crossbeam_channel::bounded(64);
    let pipeline = CameraStreamingPipeline::new(config, cmd_rx, upload_tx);

    let thread_name = format!("streaming-pipeline-{}", camera);
    let handle = std::thread::Builder::new()
        .name(thread_name)
        .spawn(move || pipeline.run())
        .map_err(|e| {
            RoboflowError::other(format!("Failed to spawn streaming pipeline thread: {}", e))
        })?;

    Ok(StreamingPipelineHandle {
        camera,
        cmd_tx,
        thread_handle: Some(handle),
    })
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_config_default() {
        let config = StreamingPipelineConfig::default();
        assert_eq!(config.camera, "");
        assert_eq!(config.chunk_size, 256 * 1024);
    }

    #[test]
    fn test_streaming_command_debug() {
        let cmd = StreamingCommand::Flush;
        assert!(format!("{:?}", cmd).contains("Flush"));
    }

    #[test]
    fn test_streaming_upload_command_debug() {
        let cmd = StreamingUploadCommand::Finish {
            camera: "cam0".to_string(),
        };
        assert!(format!("{:?}", cmd).contains("Finish"));
    }

    #[test]
    fn test_pipeline_result() {
        let result = StreamingPipelineResult {
            camera: "cam0".to_string(),
            frames_encoded: 100,
            frames_skipped: 5,
        };
        assert_eq!(result.camera, "cam0");
        assert_eq!(result.frames_encoded, 100);
        assert_eq!(result.frames_skipped, 5);
    }

    // =========================================================================
    // Error Recovery Tests
    // =========================================================================

    #[test]
    fn test_handle_frame_zero_dimensions() {
        let (upload_tx, upload_rx) = crossbeam_channel::unbounded();
        let (cmd_tx, cmd_rx) = crossbeam_channel::bounded(16);

        let config = StreamingPipelineConfig {
            camera: "test_cam".to_string(),
            ..Default::default()
        };

        let pipeline = CameraStreamingPipeline::new(config, cmd_rx, upload_tx);

        // Create image with zero dimensions
        let zero_dim_image = ImageData::new(0, 0, vec![]);
        assert_eq!(zero_dim_image.width, 0);
        assert_eq!(zero_dim_image.height, 0);

        // Send frame with zero dimensions
        cmd_tx
            .send(StreamingCommand::AddFrame {
                image: zero_dim_image,
            })
            .unwrap();
        cmd_tx.send(StreamingCommand::Flush).unwrap();

        // Run pipeline
        let result = pipeline.run().unwrap();

        // Frame should be skipped
        assert_eq!(result.frames_encoded, 0);
        assert_eq!(result.frames_skipped, 1);

        // Keep receiver alive
        drop(upload_rx);
    }

    #[test]
    fn test_handle_frame_dimension_mismatch() {
        let (upload_tx, upload_rx) = crossbeam_channel::unbounded();
        let (cmd_tx, cmd_rx) = crossbeam_channel::bounded(16);

        let config = StreamingPipelineConfig {
            camera: "test_cam".to_string(),
            ..Default::default()
        };

        let pipeline = CameraStreamingPipeline::new(config, cmd_rx, upload_tx);

        // First frame sets dimensions to 64x64
        let first_image = ImageData::new(64, 64, vec![128u8; 64 * 64 * 3]);
        cmd_tx
            .send(StreamingCommand::AddFrame { image: first_image })
            .unwrap();

        // Second frame has different dimensions (32x32)
        let mismatch_image = ImageData::new(32, 32, vec![128u8; 32 * 32 * 3]);
        cmd_tx
            .send(StreamingCommand::AddFrame {
                image: mismatch_image,
            })
            .unwrap();

        // Third frame matches first (64x64)
        let matching_image = ImageData::new(64, 64, vec![128u8; 64 * 64 * 3]);
        cmd_tx
            .send(StreamingCommand::AddFrame {
                image: matching_image,
            })
            .unwrap();

        cmd_tx.send(StreamingCommand::Flush).unwrap();

        let result = pipeline.run().unwrap();

        // First and third should encode, second should skip
        assert_eq!(result.frames_encoded, 2);
        assert_eq!(result.frames_skipped, 1);

        // Keep receiver alive
        drop(upload_rx);
    }

    #[test]
    fn test_shutdown_command() {
        let (upload_tx, upload_rx) = crossbeam_channel::unbounded();
        let (cmd_tx, cmd_rx) = crossbeam_channel::bounded(16);

        let config = StreamingPipelineConfig {
            camera: "test_cam".to_string(),
            ..Default::default()
        };

        let pipeline = CameraStreamingPipeline::new(config, cmd_rx, upload_tx);

        // Send a frame first
        let image = ImageData::new(64, 64, vec![128u8; 64 * 64 * 3]);
        cmd_tx.send(StreamingCommand::AddFrame { image }).unwrap();

        // Then send shutdown
        cmd_tx.send(StreamingCommand::Shutdown).unwrap();

        // Pipeline should return error on shutdown
        let result = pipeline.run();
        assert!(result.is_err());

        // Check that abort was sent to upload channel
        let received = upload_rx.try_recv();
        assert!(matches!(received, Ok(StreamingUploadCommand::AbortAll)));
    }

    #[test]
    fn test_non_square_dimensions_encoding() {
        // Regression test for the bug where non-square dimensions
        // (like 160x120) would fail due to incorrect dimension inference
        let (upload_tx, upload_rx) = crossbeam_channel::unbounded();
        let (cmd_tx, cmd_rx) = crossbeam_channel::bounded(16);

        let config = StreamingPipelineConfig {
            camera: "test_cam".to_string(),
            ..Default::default()
        };

        let pipeline = CameraStreamingPipeline::new(config, cmd_rx, upload_tx);

        // 160x120 is non-square - this used to fail!
        let width = 160u32;
        let height = 120u32;
        let rgb_data = vec![128u8; (width * height * 3) as usize];

        // Add 5 frames with non-square dimensions
        for _ in 0..5 {
            let image = ImageData::new(width, height, rgb_data.clone());
            cmd_tx.send(StreamingCommand::AddFrame { image }).unwrap();
        }

        cmd_tx.send(StreamingCommand::Flush).unwrap();

        let result = pipeline.run().unwrap();

        // All frames should encode successfully
        assert_eq!(result.frames_encoded, 5);
        assert_eq!(result.frames_skipped, 0);

        // Keep receiver alive
        drop(upload_rx);
    }

    #[test]
    fn test_empty_pipeline() {
        // Test pipeline with no frames
        let (upload_tx, upload_rx) = crossbeam_channel::unbounded();
        let (cmd_tx, cmd_rx) = crossbeam_channel::bounded(16);

        let config = StreamingPipelineConfig {
            camera: "test_cam".to_string(),
            ..Default::default()
        };

        let pipeline = CameraStreamingPipeline::new(config, cmd_rx, upload_tx);

        // Send flush immediately without any frames
        cmd_tx.send(StreamingCommand::Flush).unwrap();

        let result = pipeline.run().unwrap();

        // No frames encoded
        assert_eq!(result.frames_encoded, 0);
        assert_eq!(result.frames_skipped, 0);

        // Finish command should still be sent
        let received = upload_rx.try_recv();
        assert!(matches!(
            received,
            Ok(StreamingUploadCommand::Finish { .. })
        ));
    }

    #[test]
    fn test_channel_closed_early() {
        // Test when upload channel is closed before pipeline finishes
        let (upload_tx, upload_rx) = crossbeam_channel::unbounded();
        let (cmd_tx, cmd_rx) = crossbeam_channel::bounded(16);

        let config = StreamingPipelineConfig {
            camera: "test_cam".to_string(),
            ..Default::default()
        };

        let pipeline = CameraStreamingPipeline::new(config, cmd_rx, upload_tx);

        // Add some frames
        for _ in 0..3 {
            let image = ImageData::new(64, 64, vec![128u8; 64 * 64 * 3]);
            cmd_tx.send(StreamingCommand::AddFrame { image }).unwrap();
        }

        // Close the upload channel
        drop(upload_rx);

        // Send flush - pipeline should handle closed channel gracefully
        cmd_tx.send(StreamingCommand::Flush).unwrap();

        // Pipeline should complete (even though finish command will fail)
        let result = pipeline.run();

        // Should return error because finish command failed to send
        assert!(result.is_err());
    }
}
