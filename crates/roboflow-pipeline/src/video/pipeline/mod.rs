// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Pipeline abstraction for video encoding strategies.
//!
//! This module defines a trait-based abstraction that allows different
//! pipeline implementations (2-stage, 3-stage, future variants) to be
//! used interchangeably at runtime.
//!
//! # Architecture
//!
//! ## Pipeline Types
//!
//! - **TwoStagePipeline**: Single-threaded decode + encode per camera (current default)
//!   - Uses `StreamingMp4Encoder` directly
//!   - Lower memory usage, simpler flow
//!   - Best for: single camera or low-throughput scenarios
//!
//! - **ThreeStagePipeline**: Parallel decode + convert + encode
//!   - Uses `DecodePool` → `ConvertPool` → `EncoderPool`
//!   - SIMD color conversion (8-12x faster than FFmpeg sws_scale)
//!   - Best for: multi-camera, high-throughput scenarios
//!
//! # Usage
//!
//! ```rust,ignore
//! use crate::video::pipeline::{PipelineHandle, TwoStageConfig, ThreeStageConfig};
//! use crate::video::streaming::EncodedChunk;
//!
//! // Choose pipeline based on config
//! let pipeline: Box<dyn PipelineHandle> = if config.use_parallel {
//!     Box::new(ThreeStagePipeline::new(config)?)
//! } else {
//!     Box::new(TwoStagePipeline::new(config)?)
//! };
//!
//! // Add frames
//! pipeline.add_frame(image)?;
//!
//! // Finalize
//! let result = pipeline.join()?;
//! ```

pub mod three_stage;
pub mod two_stage;

use std::sync::mpsc::Sender;

pub use three_stage::{ThreeStageConfig, ThreeStagePipeline};
pub use two_stage::{TwoStageConfig, TwoStagePipeline};

use crate::ImageData;

/// Result from any pipeline implementation.
///
/// All pipelines must produce this standardized result.
#[derive(Debug, Clone)]
pub struct PipelineResult {
    /// Camera name.
    pub camera: String,
    /// Total frames encoded.
    pub frames_encoded: usize,
    /// Frames skipped (decode failures, dimension mismatches).
    pub frames_skipped: usize,
}

/// Handle for controlling a running pipeline.
///
/// This trait provides the interface for interacting with a pipeline
/// that is running in a separate thread.
///
/// All pipeline implementations must implement this trait to ensure
/// they can be used interchangeably by higher-level code.
pub trait PipelineHandle: Send {
    /// Add a frame to the pipeline.
    ///
    /// The frame will be processed asynchronously by the pipeline thread.
    /// Returns an error if the pipeline has already been finalized.
    fn add_frame(&self, image: ImageData) -> std::io::Result<()>;

    /// Signal the pipeline to flush and finish.
    ///
    /// This signals the pipeline to complete processing of all pending frames
    /// and finish encoding. After calling `flush()`, no more frames can be added.
    fn flush(&self) -> std::io::Result<()>;

    /// Signal the pipeline to shutdown immediately.
    ///
    /// This aborts the pipeline without completing pending work.
    /// Any unprocessed frames will be lost.
    fn shutdown(&self) -> std::io::Result<()>;

    /// Wait for the pipeline to finish and get the result.
    ///
    /// This consumes the handle and waits for the pipeline thread to complete.
    /// Returns the final statistics including frames encoded and skipped.
    fn join(self: Box<Self>) -> std::io::Result<PipelineResult>;
}

/// Configuration for creating a pipeline.
///
/// This trait allows higher-level code to create pipelines without
/// knowing the concrete implementation type.
pub trait PipelineConfig: Send + Sync {
    /// Create a new pipeline handle with the given upload channel.
    ///
    /// The upload channel receives encoded chunks that should be sent
    /// to storage (S3, local filesystem, etc.).
    ///
    /// # Arguments
    ///
    /// * `upload_tx` - Channel sender for encoded chunks
    ///
    /// # Returns
    ///
    /// A boxed pipeline handle ready to receive frames.
    fn create_pipeline(
        &self,
        upload_tx: Sender<super::streaming::EncodedChunk>,
    ) -> std::io::Result<Box<dyn PipelineHandle>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_result_creation() {
        let result = PipelineResult {
            camera: "test".to_string(),
            frames_encoded: 100,
            frames_skipped: 5,
        };
        assert_eq!(result.camera, "test");
        assert_eq!(result.frames_encoded, 100);
        assert_eq!(result.frames_skipped, 5);
    }
}
