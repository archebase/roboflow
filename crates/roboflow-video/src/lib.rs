// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Video encoding for robotics data conversion.
//!
//! This crate provides low-level video encoding primitives for the roboflow ecosystem.
//! It is the foundational crate for video processing, providing hardware acceleration,
//! SIMD color conversion, and streaming encoders.
//!
//! # Crate Separation
//!
//! - **`roboflow-video` (this crate)**: Low-level video primitives
//!   - Hardware encoders (NVENC, VideoToolbox)
//!   - SIMD color space conversion (RGB→NV12/YUV420P)
//!   - Streaming MP4 encoding
//!   - Fragment-based encoding
//! - **`roboflow-dataset`**: High-level dataset writers built on top of this crate
//!   - `ConcurrentVideoEncoder` (multi-camera orchestration)
//!   - `CameraStreamingPipeline` (dataset-specific pipelines)
//!   - LeRobot format encoding
//!
//! Dependency direction: `roboflow-dataset` → `roboflow-video` (one-way)
//!
//! # Features
//!
//! - **Hardware acceleration**: NVENC (NVIDIA), VideoToolbox (macOS)
//! - **SIMD color conversion**: 8-12x faster than FFmpeg sws_scale
//! - **Fragment-based encoding**: Stream video fragments during encoding
//! - **Streaming MP4**: Zero-temp-file encoding with chunked output
//!
//! # Three-Stage Pipeline (Available but not currently used)
//!
//! For high-throughput scenarios, a three-stage pipeline is available:
//!
//! ```text
//! ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
//! │ DecodePool  │ ──► │ ConvertPool │ ──► │ EncoderPool │
//! │ (JPEG→RGB)  │     │ (RGB→NV12)  │     │ (H.264)     │
//! └─────────────┘     └─────────────┘     └─────────────┘
//! ```
//!
//! Note: The 3-stage pipeline is exported but not currently used by `roboflow-dataset`,
//! which uses `StreamingMp4Encoder` and `RsmpegMp4Encoder` directly.
//!
//! # Example
//!
//! ```rust,ignore
//! use roboflow_video::{StreamingMp4Encoder, StreamingEncoderConfig, ImageData};
//!
//! let config = StreamingEncoderConfig::default();
//! let (chunk_tx, chunk_rx) = std::sync::mpsc::channel();
//!
//! let mut encoder = StreamingMp4Encoder::with_dimensions(
//!     config, chunk_tx, 1920, 1080
//! )?;
//!
//! // Add frames
//! encoder.add_frame(&rgb_data)?;
//!
//! // Finalize
//! encoder.finalize()?;
//! ```

pub mod arena;
pub mod config;
pub mod convert;
pub mod decode;
pub mod encoder_pool;
pub mod fragment;
pub mod frame;
pub mod hardware;
pub mod hardware_config;
pub mod pipeline;
pub mod reorder;
pub mod rsmpeg;
pub mod simd;
pub mod streaming;

// Test utilities module (always compiled, used by benches/examples)
pub mod test_utils;

// Re-export main types
pub use arena::{FramePool, FramePoolConfig};
pub use config::{DepthEncoderConfig, VideoEncoderConfig};
pub use convert::{ConvertPool, ConvertPoolConfig, TargetFormat};
pub use decode::{DecodePool, DecodePoolConfig};
pub use encoder_pool::{EncoderPool, EncoderPoolConfig};
pub use fragment::{FragmentEncoder, FragmentEncoderConfig, FragmentInfo};
pub use frame::{
    DepthFrame, DepthFrameBuffer, FrameBuffer, PixelFormat, VideoEncoderError, VideoFrame,
    VideoFrameBuffer,
};
pub use hardware::{
    DepthMkvEncoder, EncoderChoice, Mp4Encoder, NvencEncoder, VideoToolboxEncoder,
    available_encoders, check_nvenc_available, check_videotoolbox_available, is_encoder_available,
    print_encoder_diagnostics, select_best_encoder,
};
pub use hardware_config::{detect_hardware_backend, HardwareBackend, HardwareConfig};
pub use rsmpeg::{
    EncodeFrame, RsmpegEncoder, RsmpegEncoderConfig, RsmpegMp4Encoder, default_codec_name,
    is_hardware_encoding_available, is_rsmpeg_available,
};
pub use simd::{
    optimal_strategy, rgb_batch_to_nv12, rgb_batch_to_yuv420p, rgb_to_nv12, rgb_to_nv12_in_place,
    rgb_to_yuv420p, ConversionStrategy,
};
pub use streaming::{EncodedChunk, StreamingEncoderConfig, StreamingMp4Encoder};

// Pipeline abstraction (Clean Architecture for pipeline selection)
pub use pipeline::{PipelineHandle, PipelineResult, PipelineConfig, TwoStageConfig, ThreeStageConfig, TwoStagePipeline, ThreeStagePipeline};

/// Image data for video encoding.
///
/// This is a minimal version of ImageData that the video crate needs.
/// Full ImageData is in roboflow-dataset.
#[derive(Debug, Clone)]
pub struct ImageData {
    /// Image width in pixels.
    pub width: u32,
    /// Image height in pixels.
    pub height: u32,
    /// Raw image data (RGB8 or encoded).
    pub data: Vec<u8>,
    /// Whether data is already encoded (e.g., JPEG/PNG).
    pub is_encoded: bool,
}

impl ImageData {
    /// Create new image data.
    pub fn new(width: u32, height: u32, data: Vec<u8>) -> Self {
        Self {
            width,
            height,
            data,
            is_encoded: false,
        }
    }

    /// Create encoded image data.
    pub fn encoded(width: u32, height: u32, data: Vec<u8>) -> Self {
        Self {
            width,
            height,
            data,
            is_encoded: true,
        }
    }
}
