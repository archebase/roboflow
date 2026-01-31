// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Re-export video encoder from KPS for shared use.
//!
//! The video encoder is used by both KPS and LeRobot for MP4 output.

pub use crate::kps::video_encoder::{
    DepthEncoderConfig, DepthFrame, DepthFrameBuffer, DepthMkvEncoder, Mp4Encoder,
    VideoEncoderConfig, VideoFrame, VideoFrameBuffer,
};
