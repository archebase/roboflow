// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Video encoding utilities.
//!
//! This module re-exports types from the `roboflow_video` crate.
//! The canonical implementations are in `roboflow-video/src/hardware.rs`,
//! `roboflow-video/src/frame.rs`, and `roboflow-video/src/rsmpeg.rs`.
//!
//! # Architecture Note
//!
//! This module exists to provide a consistent API surface for dataset writers.
//! Video encoding primitives are defined in `roboflow-video` and re-exported here
//! for convenience. Dataset-specific video functionality (like camera pipelines)
//! is implemented in `common/camera_streaming_pipeline.rs`.

// Re-export all video types from roboflow-video crate (canonical location)
pub use roboflow_video::{
    DepthEncoderConfig, DepthFrame, DepthFrameBuffer, DepthMkvEncoder, EncodeFrame, EncoderChoice,
    Mp4Encoder, NvencEncoder, RsmpegEncoder, RsmpegEncoderConfig, RsmpegMp4Encoder,
    VideoEncoderConfig, VideoEncoderError, VideoFrame, VideoFrameBuffer, VideoToolboxEncoder,
    available_encoders, check_nvenc_available, check_videotoolbox_available, default_codec_name,
    is_encoder_available, is_hardware_encoding_available, is_rsmpeg_available,
    print_encoder_diagnostics, select_best_encoder,
};
