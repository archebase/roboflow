// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Video encoding utilities.
//!
//! This module re-exports types from the `roboflow_video` crate.
//! The canonical implementations are in `roboflow-video/src/hardware.rs`
//! and `roboflow-video/src/frame.rs`.

// Re-export all video types from roboflow-video crate (canonical location)
pub use roboflow_video::{
    DepthEncoderConfig, DepthFrame, DepthFrameBuffer, DepthMkvEncoder, EncoderChoice, Mp4Encoder,
    NvencEncoder, VideoEncoderConfig, VideoEncoderError, VideoFrame, VideoFrameBuffer,
    VideoToolboxEncoder, available_encoders, check_nvenc_available, check_videotoolbox_available,
    is_encoder_available, print_encoder_diagnostics, select_best_encoder,
};
