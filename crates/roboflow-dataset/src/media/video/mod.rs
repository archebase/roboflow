// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Video encoding module.
//!
//! This module provides video encoding capabilities for dataset creation.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                      Public API                                  │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  VideoEncoder - Single encoder (file or channel output)         │
//! │  ConcurrentVideoEncoder - Multi-camera orchestration            │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example - Single Video
//!
//! ```ignore
//! use roboflow_dataset::media::video::{VideoEncoder, OutputConfig, VideoEncoderConfig};
//!
//! let config = VideoEncoderConfig::default();
//! let output = OutputConfig::file("/path/to/output.mp4");
//! let mut encoder = VideoEncoder::new(config, output)?;
//!
//! for frame in frames {
//!     encoder.encode_frame(&rgb_data, 640, 480)?;
//! }
//!
//! let result = encoder.finalize()?;
//! ```
//!
//! # Example - Multi-Camera
//!
//! ```ignore
//! use roboflow_dataset::media::video::{ConcurrentVideoEncoder, ConcurrentEncoderConfig};
//! use roboflow_dataset::formats::common::{LeRobotVideoPathScheme, VideoPathScheme};
//! use std::path::PathBuf;
//!
//! let config = ConcurrentEncoderConfig::new();
//! let mut encoder = ConcurrentVideoEncoder::new(config)?;
//!
//! // Register cameras with their output paths
//! let output_dir = PathBuf::from("./output");
//! let scheme = LeRobotVideoPathScheme::new("dataset/episode_001");
//! let cam_path = output_dir.join(scheme.video_path(0, "cam_left", 0));
//! encoder.add_camera("cam_left", cam_path)?;
//!
//! encoder.add_frame("cam_left", image1)?;
//!
//! let results = encoder.finalize()?;
//! ```

// Internal modules
pub mod arena;
pub mod codec;
pub mod composer;
pub mod concurrent;
pub mod config;
pub mod convert;
pub mod decode;
pub mod encoder;
pub mod frame;
pub mod hardware;
pub mod hardware_config;
pub mod rsmpeg;
pub mod simd;
pub mod test_utils;

// Re-export core types
pub use config::{DepthEncoderConfig, VideoEncoderConfig};
pub use frame::{
    DepthFrame, DepthFrameBuffer, FrameBuffer, PixelFormat, VideoEncoderError, VideoFrame,
    VideoFrameBuffer,
};

// Re-export hardware detection
pub use hardware::{
    EncoderChoice, available_encoders, check_nvenc_available, check_videotoolbox_available,
    is_encoder_available, print_encoder_diagnostics, select_best_encoder,
};
pub use hardware_config::{HardwareBackend, HardwareConfig, detect_hardware_backend};

// Re-export codec utilities
pub use rsmpeg::{default_codec_name, is_hardware_encoding_available, is_rsmpeg_available};

// Re-export SIMD conversion
pub use simd::{
    ConversionStrategy, optimal_strategy, rgb_batch_to_nv12, rgb_batch_to_yuv420p, rgb_to_nv12,
    rgb_to_nv12_in_place, rgb_to_yuv420p,
};

// Re-export composer for video concatenation
pub use composer::{RsmpegVideoComposer, VideoComposer};

// Re-export arena types
pub use arena::{FramePool, FramePoolConfig};

// Re-export conversion pool
pub use convert::{ConvertPool, ConvertPoolConfig, TargetFormat};

// Re-export decode pool
pub use decode::{DecodePool, DecodePoolConfig};

// Re-export VideoPathScheme from core traits
pub use crate::core::VideoPathScheme;

// Re-export ImageData from formats::common for convenience
pub use crate::formats::common::ImageData;

// =============================================================================
// VIDEO ENCODER API (Canonical Public API)
// =============================================================================

/// Encoded chunk from video encoder.
pub use encoder::EncodedChunk;

/// Result from video encoding finalization.
pub use encoder::EncodingResult;

/// Output configuration for video encoder.
pub use encoder::OutputConfig;

/// Video encoder.
pub use encoder::VideoEncoder;

// =============================================================================
// MULTI-CAMERA ENCODER API
// =============================================================================

/// Configuration for concurrent encoder.
pub use concurrent::ConcurrentEncoderConfig;

/// Result from concurrent encoder.
pub use concurrent::ConcurrentEncoderResult;

/// Multi-camera concurrent video encoder.
pub use concurrent::ConcurrentVideoEncoder;
