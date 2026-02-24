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
//! │  VideoEncoder - Single stream encoding                          │
//! │  FragmentEncoder - Bounded memory encoding with flush control   │
//! │  EncodingWorkload - Multi-stream parallel encoding (unified)    │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example - Single Video (Simple)
//!
//! ```ignore
//! use roboflow_media::video::{VideoEncoder, OutputConfig, VideoEncoderConfig};
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
//! # Example - Fragment Encoding (Bounded Memory)
//!
//! ```ignore
//! use roboflow_media::video::{FragmentEncoder, FragmentConfig, FragmentOutputConfig};
//! use std::path::PathBuf;
//!
//! let config = FragmentConfig::with_max_frames(300); // Flush every 300 frames
//! let output = FragmentOutputConfig::SingleFile {
//!     path: PathBuf::from("/path/to/output.mp4"),
//! };
//! let mut encoder = FragmentEncoder::new(video_config, output, config)?;
//!
//! for frame in frames {
//!     encoder.encode_frame(&rgb_data, 640, 480)?;
//! }
//!
//! let result = encoder.finalize()?;
//! ```
//!
//! # Example - Multi-Camera (Unified API)
//!
//! ```ignore
//! use roboflow_media::video::{
//!     EncodingWorkload, WorkloadConfig, StreamConfig, EncodingStrategy,
//! };
//!
//! let mut workload = EncodingWorkload::new(WorkloadConfig::default())?;
//!
//! // Add streams with different strategies
//! workload.add_stream(StreamConfig::file("cam_left", "left.mp4")
//!     .with_strategy(EncodingStrategy::fragment_by_frames(300)))?;
//! workload.add_stream(StreamConfig::file("cam_right", "right.mp4"))?;
//!
//! // Submit frames (thread-safe)
//! workload.submit_frame("cam_left", &rgb_data, 640, 480)?;
//!
//! // Finalize all streams
//! let results = workload.finalize()?;
//! ```

// =============================================================================
// Internal modules (not exposed publicly)
// =============================================================================

#[allow(dead_code)]
mod arena;
#[allow(dead_code)]
mod codec;
#[allow(dead_code)]
mod composer;
#[allow(dead_code)]
mod concurrent;
#[allow(dead_code)]
mod config;
#[allow(dead_code, clippy::wrong_self_convention)]
mod convert;
mod dataset_encode;
#[allow(dead_code)]
mod decode;
mod encoder;
mod fragment;
#[allow(dead_code)]
mod frame;
#[allow(dead_code)]
mod hardware;
#[allow(dead_code)]
mod hardware_config;
mod profiles;
#[allow(dead_code)]
mod rsmpeg;
#[allow(dead_code)]
mod simd;
#[allow(dead_code)]
mod test_utils;
mod workload;

// =============================================================================
// Minimal Public API
// =============================================================================

// Re-export ImageData from crate root for convenience
pub use crate::ImageData;

// -----------------------------------------------------------------------------
// Core Types
// -----------------------------------------------------------------------------

/// Video encoder configuration.
pub use config::VideoEncoderConfig;

/// Pixel format for video frames.
pub use frame::PixelFormat;

/// Video frame for encoding.
pub use frame::VideoFrame;

/// Frame buffer for zero-copy processing.
pub use frame::FrameBuffer;

/// Video encoder error type.
pub use frame::VideoEncoderError;

pub use dataset_encode::{EncodeStats, build_frame_buffer_static, encode_videos};
/// Video frame buffer (alias for FrameBuffer).
pub use frame::VideoFrameBuffer;
pub use profiles::{Profile, QualityTier, ResolvedConfig, SpeedPreset, VideoEncodingProfile};

// -----------------------------------------------------------------------------
// Simple Video Encoder API
// -----------------------------------------------------------------------------

/// Video encoder for single-stream encoding.
pub use encoder::VideoEncoder;

/// Output configuration for video encoder.
pub use encoder::OutputConfig;

/// Result from video encoding finalization.
pub use encoder::EncodingResult;

/// Encoded chunk from video encoder.
pub use encoder::EncodedChunk;

// -----------------------------------------------------------------------------
// Hardware Configuration
// -----------------------------------------------------------------------------

/// Hardware encoder backend.
pub use hardware_config::HardwareBackend;

/// Hardware encoder configuration.
pub use hardware_config::HardwareConfig;

// -----------------------------------------------------------------------------
// Video Composition
// -----------------------------------------------------------------------------

/// Video composer trait.
pub use composer::VideoComposer;

/// Rsmpeg-based video composer.
pub use composer::RsmpegVideoComposer;

// -----------------------------------------------------------------------------
// SIMD Color Conversion
// -----------------------------------------------------------------------------

/// Conversion strategy for color space conversion.
pub use simd::ConversionStrategy;

/// Optimal conversion strategy.
pub use simd::optimal_strategy;

/// Convert RGB to NV12.
pub use simd::rgb_to_nv12;

/// Convert RGB to YUV420P.
pub use simd::rgb_to_yuv420p;

// -----------------------------------------------------------------------------
// Fragment Encoder API (Bounded Memory)
// -----------------------------------------------------------------------------

/// Fragment encoder with explicit flush control.
pub use fragment::FragmentEncoder;

/// Configuration for fragment-based encoding.
pub use fragment::FragmentConfig;

/// Output configuration for fragment encoder.
pub use fragment::FragmentOutputConfig;

/// Result from fragment encoding.
pub use fragment::FragmentEncodingResult;

// -----------------------------------------------------------------------------
// Concurrent Encoder API (Legacy)
// -----------------------------------------------------------------------------

/// Configuration for concurrent video encoder.
pub use concurrent::ConcurrentEncoderConfig;

/// Result from concurrent encoding.
pub use concurrent::ConcurrentEncoderResult;

/// Concurrent video encoder for multi-camera encoding.
pub use concurrent::ConcurrentVideoEncoder;

// -----------------------------------------------------------------------------
// Workload API (Unified Multi-Stream)
// -----------------------------------------------------------------------------

/// Encoding workload for multi-stream video encoding.
pub use workload::EncodingWorkload;

/// Configuration for encoding workload.
pub use workload::WorkloadConfig;

/// Result from encoding workload finalization.
pub use workload::WorkloadResult;

/// Configuration for a single stream in a workload.
pub use workload::StreamConfig;

/// Unique identifier for a stream.
pub use workload::StreamId;

/// Output destination for a stream.
pub use workload::StreamOutput;

/// Result from encoding a single stream.
pub use workload::StreamResult;

/// Frame data for submission to a stream.
pub use workload::FrameData;

/// Encoding strategy for a stream.
pub use workload::EncodingStrategy;

/// Fragment flush triggers.
pub use workload::FragmentTriggers;
