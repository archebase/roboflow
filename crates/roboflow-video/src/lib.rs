// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Video encoding for robotics data conversion.
//!
//! This crate provides video encoding functionality extracted from roboflow-dataset,
//! making it a first-class component for video processing.
//!
//! # Features
//!
//! - **Multi-camera encoding**: `ConcurrentVideoEncoder` for parallel camera streams
//! - **Hardware acceleration**: NVENC (NVIDIA), VideoToolbox (macOS)
//! - **Fragment-based encoding**: Stream video fragments during encoding
//! - **Concurrent upload**: Upload while encoding
//!
//! # Architecture
//!
//! ```text
//! ConcurrentVideoEncoder (orchestrator)
//! └── CameraPipeline (per-camera thread)
//!     └── FragmentEncoder (encodes frame batches)
//!         └── RsmpegEncoder (actual FFmpeg encoding)
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use roboflow_video::{ConcurrentVideoEncoder, ConcurrentEncoderConfig, ImageData};
//! use roboflow_storage::S3Storage;
//! use std::sync::Arc;
//!
//! let config = ConcurrentEncoderConfig {
//!     key_prefix: "dataset/episode_001".to_string(),
//!     ..Default::default()
//! };
//!
//! let storage = Arc::new(S3Storage::from_env("my-bucket")?);
//! let runtime = tokio::runtime::Handle::current();
//!
//! let mut encoder = ConcurrentVideoEncoder::new(config, storage, runtime)?;
//!
//! // Add frames for different cameras
//! let image = ImageData::new(640, 480, rgb_data);
//! encoder.add_frame("cam_0", image)?;
//!
//! // Finalize and get results
//! let results = encoder.finalize()?;
//! ```

pub mod concurrent;
pub mod config;
pub mod fragment;
pub mod frame;
pub mod hardware;
pub mod rsmpeg;
pub mod simd;

// Re-export main types
pub use concurrent::{ConcurrentEncoderConfig, ConcurrentEncoderResult, ConcurrentVideoEncoder};
pub use config::{DepthEncoderConfig, VideoEncoderConfig};
pub use fragment::{FragmentEncoder, FragmentEncoderConfig, FragmentInfo};
pub use frame::{DepthFrame, DepthFrameBuffer, VideoEncoderError, VideoFrame, VideoFrameBuffer};
pub use hardware::{
    DepthMkvEncoder, EncoderChoice, Mp4Encoder, NvencEncoder, VideoToolboxEncoder,
    available_encoders, check_nvenc_available, check_videotoolbox_available, is_encoder_available,
    print_encoder_diagnostics, select_best_encoder,
};
pub use rsmpeg::{
    EncodeFrame, RsmpegEncoder, RsmpegEncoderConfig, RsmpegMp4Encoder, default_codec_name,
    is_hardware_encoding_available, is_rsmpeg_available,
};
pub use simd::{ConversionStrategy, optimal_strategy, rgb_to_nv12, rgb_to_yuv420p};

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

/// Decode image data to RGB format.
///
/// Handles both raw RGB and encoded (JPEG/PNG) data.
pub fn decode_to_rgb(image: &ImageData) -> Option<(u32, u32, Vec<u8>)> {
    if image.width == 0 || image.height == 0 {
        return None;
    }

    if image.is_encoded {
        // Decode JPEG/PNG to RGB
        let img = image::load_from_memory(&image.data).ok()?;
        let rgb = img.to_rgb8();
        Some((rgb.width(), rgb.height(), rgb.into_raw()))
    } else {
        // Already RGB
        Some((image.width, image.height, image.data.clone()))
    }
}
